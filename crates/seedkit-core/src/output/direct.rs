//! # Direct Database Insertion
//!
//! Inserts generated data directly into a live database using the fastest
//! method available for each database engine. All operations are wrapped
//! in a single transaction — if any batch fails, the entire insertion is
//! rolled back, leaving the database in its original state.
//!
//! - **PostgreSQL**: Batched multi-row `INSERT` statements within a
//!   transaction. After all inserts, auto-increment sequences are
//!   synchronized via `setval(pg_get_serial_sequence(...))` so that
//!   future manual inserts don't collide with seeded IDs.
//!
//! - **MySQL**: Batched multi-row `INSERT INTO ... VALUES (...), (...)` within
//!   a transaction, with `SET FOREIGN_KEY_CHECKS = 0` to defer constraint
//!   validation until after all data is inserted.
//!
//! - **SQLite**: All inserts wrapped in a single `BEGIN ... COMMIT` transaction,
//!   which is sufficient to hit 100k+ rows/sec on SQLite.
//!
//! After all inserts complete, deferred FK updates (from cycle-breaking) are
//! applied as individual UPDATE statements within the same transaction.

use indexmap::IndexMap;

use crate::error::{Result, SeedKitError};
use crate::generate::engine::{DeferredUpdate, GeneratedData};
use crate::generate::value::Value;
use crate::schema::types::{DatabaseSchema, DatabaseType};

/// Batch size for multi-row INSERT statements.
const INSERT_BATCH_SIZE: usize = 100;

/// Progress reporting interval (rows) to avoid terminal I/O overhead.
const PROGRESS_BATCH_SIZE: usize = 100;

/// Insert generated data directly into a database.
///
/// Connects to the database using the provided URL, then inserts all generated
/// rows using the fastest available method for the database engine. All inserts
/// run inside a single transaction — if any batch fails, the entire operation
/// is rolled back so no partial data is left behind.
///
/// The `progress_callback` receives (rows_inserted_so_far, total_rows) and is
/// called every `PROGRESS_BATCH_SIZE` rows to avoid terminal I/O overhead.
pub async fn insert_direct(
    data: &GeneratedData,
    schema: &DatabaseSchema,
    db_url: &str,
    progress_callback: Option<&(dyn Fn(usize, usize) + Send + Sync)>,
) -> Result<()> {
    let total_rows: usize = data.tables.values().map(|rows| rows.len()).sum();

    match schema.database_type {
        DatabaseType::PostgreSQL => {
            insert_postgres(data, schema, db_url, total_rows, progress_callback).await
        }
        DatabaseType::MySQL => {
            insert_mysql(data, schema, db_url, total_rows, progress_callback).await
        }
        DatabaseType::SQLite => {
            insert_sqlite(data, schema, db_url, total_rows, progress_callback).await
        }
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL: transactional batched INSERT + sequence sync
// ---------------------------------------------------------------------------

async fn insert_postgres(
    data: &GeneratedData,
    schema: &DatabaseSchema,
    db_url: &str,
    total_rows: usize,
    progress_callback: Option<&(dyn Fn(usize, usize) + Send + Sync)>,
) -> Result<()> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(db_url)
        .await
        .map_err(|e| SeedKitError::Connection {
            message: "Failed to connect for direct insertion".to_string(),
            connection_hint: sanitize_url(db_url),
            source: e,
        })?;

    // Wrap all inserts in a single transaction — if any batch fails,
    // the entire insertion is rolled back (no partial data left behind).
    let mut tx = pool.begin().await.map_err(|e| SeedKitError::InsertFailed {
        table: "(session)".to_string(),
        row_index: 0,
        message: "Failed to begin transaction".to_string(),
        sql_preview: "BEGIN".to_string(),
        source: e,
    })?;

    let db_type = &DatabaseType::PostgreSQL;
    let mut rows_inserted = 0usize;

    for (table_name, rows) in &data.tables {
        if rows.is_empty() {
            continue;
        }

        let columns: Vec<&String> = rows[0].keys().collect();
        let quoted_table = quote_identifier(table_name, db_type);
        let quoted_columns: Vec<String> = columns
            .iter()
            .map(|c| quote_identifier(c, db_type))
            .collect();
        let col_list = quoted_columns.join(", ");

        for chunk in rows.chunks(INSERT_BATCH_SIZE) {
            let sql = build_batched_insert(&quoted_table, &col_list, &columns, chunk, db_type);

            sqlx::query(&sql)
                .execute(&mut *tx)
                .await
                .map_err(|e| SeedKitError::InsertFailed {
                    table: table_name.clone(),
                    row_index: rows_inserted,
                    message: "Batched INSERT failed".to_string(),
                    sql_preview: truncate_sql(&sql, 200),
                    source: e,
                })?;

            rows_inserted += chunk.len();
            report_progress(progress_callback, rows_inserted, total_rows);
        }

        // Sequence synchronization: if the table has an auto-increment PK,
        // sync the underlying sequence so future manual INSERTs by the user
        // don't collide with our seeded IDs.
        if let Some(table) = schema.tables.get(table_name) {
            if let Some(pk) = &table.primary_key {
                if pk.columns.len() == 1 {
                    if let Some(col) = table.columns.get(&pk.columns[0]) {
                        if col.is_auto_increment || col.data_type.is_serial() {
                            let pk_quoted = quote_identifier(&pk.columns[0], db_type);
                            let sync_sql = format!(
                                "SELECT setval(\
                                    pg_get_serial_sequence('{}', '{}'), \
                                    coalesce(max({}), 1), \
                                    max({}) IS NOT null\
                                ) FROM {}",
                                table_name, pk.columns[0], pk_quoted, pk_quoted, quoted_table,
                            );
                            // Best-effort: don't fail the entire insert if
                            // sequence sync fails (e.g., table uses IDENTITY
                            // instead of SERIAL and has no owned sequence).
                            let _ = sqlx::query(&sync_sql).execute(&mut *tx).await;
                        }
                    }
                }
            }
        }
    }

    // Deferred FK updates (within the same transaction)
    execute_deferred_updates_pg(&mut tx, &data.deferred_updates, data, schema).await?;

    // Commit the transaction
    tx.commit().await.map_err(|e| SeedKitError::InsertFailed {
        table: "(session)".to_string(),
        row_index: rows_inserted,
        message: "Failed to commit transaction".to_string(),
        sql_preview: "COMMIT".to_string(),
        source: e,
    })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// MySQL: transactional batched INSERT with FK checks disabled
// ---------------------------------------------------------------------------

async fn insert_mysql(
    data: &GeneratedData,
    schema: &DatabaseSchema,
    db_url: &str,
    total_rows: usize,
    progress_callback: Option<&(dyn Fn(usize, usize) + Send + Sync)>,
) -> Result<()> {
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(5)
        .connect(db_url)
        .await
        .map_err(|e| SeedKitError::Connection {
            message: "Failed to connect for direct insertion".to_string(),
            connection_hint: sanitize_url(db_url),
            source: e,
        })?;

    // Begin transaction — all inserts are atomic.
    let mut tx = pool.begin().await.map_err(|e| SeedKitError::InsertFailed {
        table: "(session)".to_string(),
        row_index: 0,
        message: "Failed to begin transaction".to_string(),
        sql_preview: "BEGIN".to_string(),
        source: e,
    })?;

    let db_type = &DatabaseType::MySQL;

    // Disable FK checks within the transaction
    sqlx::query("SET FOREIGN_KEY_CHECKS = 0")
        .execute(&mut *tx)
        .await
        .map_err(|e| SeedKitError::InsertFailed {
            table: "(session)".to_string(),
            row_index: 0,
            message: "Failed to disable FK checks".to_string(),
            sql_preview: "SET FOREIGN_KEY_CHECKS = 0".to_string(),
            source: e,
        })?;

    let mut rows_inserted = 0usize;

    for (table_name, rows) in &data.tables {
        if rows.is_empty() {
            continue;
        }

        let columns: Vec<&String> = rows[0].keys().collect();
        let quoted_table = quote_identifier(table_name, db_type);
        let quoted_columns: Vec<String> = columns
            .iter()
            .map(|c| quote_identifier(c, db_type))
            .collect();
        let col_list = quoted_columns.join(", ");

        for chunk in rows.chunks(INSERT_BATCH_SIZE) {
            let sql = build_batched_insert(&quoted_table, &col_list, &columns, chunk, db_type);

            sqlx::query(&sql)
                .execute(&mut *tx)
                .await
                .map_err(|e| SeedKitError::InsertFailed {
                    table: table_name.clone(),
                    row_index: rows_inserted,
                    message: "Batched INSERT failed".to_string(),
                    sql_preview: truncate_sql(&sql, 200),
                    source: e,
                })?;

            rows_inserted += chunk.len();
            report_progress(progress_callback, rows_inserted, total_rows);
        }
    }

    // Deferred FK updates (within the same transaction)
    execute_deferred_updates_mysql(&mut tx, &data.deferred_updates, data, schema).await?;

    // Re-enable FK checks before commit
    let _ = sqlx::query("SET FOREIGN_KEY_CHECKS = 1")
        .execute(&mut *tx)
        .await;

    // Commit the transaction
    tx.commit().await.map_err(|e| SeedKitError::InsertFailed {
        table: "(session)".to_string(),
        row_index: rows_inserted,
        message: "Failed to commit transaction".to_string(),
        sql_preview: "COMMIT".to_string(),
        source: e,
    })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// SQLite: single-transaction wrapping
// ---------------------------------------------------------------------------

async fn insert_sqlite(
    data: &GeneratedData,
    schema: &DatabaseSchema,
    db_url: &str,
    total_rows: usize,
    progress_callback: Option<&(dyn Fn(usize, usize) + Send + Sync)>,
) -> Result<()> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await
        .map_err(|e| SeedKitError::Connection {
            message: "Failed to connect for direct insertion".to_string(),
            connection_hint: sanitize_url(db_url),
            source: e,
        })?;

    let db_type = &DatabaseType::SQLite;

    // SQLite uses sqlx transactions too for atomicity.
    let mut tx = pool.begin().await.map_err(|e| SeedKitError::InsertFailed {
        table: "(session)".to_string(),
        row_index: 0,
        message: "Failed to begin transaction".to_string(),
        sql_preview: "BEGIN".to_string(),
        source: e,
    })?;

    // Disable FK enforcement within the transaction
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *tx)
        .await
        .map_err(|e| SeedKitError::InsertFailed {
            table: "(session)".to_string(),
            row_index: 0,
            message: "Failed to disable FK enforcement".to_string(),
            sql_preview: "PRAGMA foreign_keys = OFF".to_string(),
            source: e,
        })?;

    let mut rows_inserted = 0usize;

    for (table_name, rows) in &data.tables {
        if rows.is_empty() {
            continue;
        }

        let columns: Vec<&String> = rows[0].keys().collect();
        let quoted_table = quote_identifier(table_name, db_type);
        let quoted_columns: Vec<String> = columns
            .iter()
            .map(|c| quote_identifier(c, db_type))
            .collect();
        let col_list = quoted_columns.join(", ");

        for chunk in rows.chunks(INSERT_BATCH_SIZE) {
            let sql = build_batched_insert(&quoted_table, &col_list, &columns, chunk, db_type);

            sqlx::query(&sql)
                .execute(&mut *tx)
                .await
                .map_err(|e| SeedKitError::InsertFailed {
                    table: table_name.clone(),
                    row_index: rows_inserted,
                    message: "INSERT failed within transaction".to_string(),
                    sql_preview: truncate_sql(&sql, 200),
                    source: e,
                })?;

            rows_inserted += chunk.len();
            report_progress(progress_callback, rows_inserted, total_rows);
        }
    }

    // Deferred FK updates (within the same transaction)
    execute_deferred_updates_sqlite(&mut tx, &data.deferred_updates, data, schema).await?;

    // Re-enable FK enforcement before commit
    let _ = sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *tx)
        .await;

    // Commit the transaction
    tx.commit().await.map_err(|e| SeedKitError::InsertFailed {
        table: "(session)".to_string(),
        row_index: rows_inserted,
        message: "Failed to commit transaction".to_string(),
        sql_preview: "COMMIT".to_string(),
        source: e,
    })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Build a batched multi-row INSERT statement.
///
/// Produces: `INSERT INTO "table" ("col1", "col2") VALUES (v1, v2), (v3, v4)`
fn build_batched_insert(
    quoted_table: &str,
    col_list: &str,
    columns: &[&String],
    rows: &[IndexMap<String, Value>],
    db_type: &DatabaseType,
) -> String {
    let mut sql = format!("INSERT INTO {} ({}) VALUES ", quoted_table, col_list);

    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push('(');
        for (j, col) in columns.iter().enumerate() {
            if j > 0 {
                sql.push_str(", ");
            }
            let literal = row
                .get(*col)
                .map(|v| v.to_sql_literal(db_type))
                .unwrap_or_else(|| "NULL".to_string());
            sql.push_str(&literal);
        }
        sql.push(')');
    }

    sql
}

/// Build an UPDATE statement for a deferred FK update.
///
/// Requires the row's primary key value to identify which row to update.
/// Returns `None` if the table has no PK or the PK value isn't available.
fn build_deferred_update(
    update: &DeferredUpdate,
    data: &GeneratedData,
    schema: &DatabaseSchema,
    db_type: &DatabaseType,
) -> Option<String> {
    let table = schema.tables.get(&update.table_name)?;
    let pk = table.primary_key.as_ref()?;

    // Get the row that needs updating
    let rows = data.tables.get(&update.table_name)?;
    let row = rows.get(update.row_index)?;

    // Build WHERE clause from PK columns
    let mut where_parts = Vec::new();
    for pk_col in &pk.columns {
        if let Some(pk_val) = row.get(pk_col) {
            where_parts.push(format!(
                "{} = {}",
                quote_identifier(pk_col, db_type),
                pk_val.to_sql_literal(db_type),
            ));
        } else {
            // For auto-increment PKs not in the row, use the synthesized ID
            let id = update.row_index as i64 + 1;
            where_parts.push(format!("{} = {}", quote_identifier(pk_col, db_type), id,));
        }
    }

    if where_parts.is_empty() {
        return None;
    }

    Some(format!(
        "UPDATE {} SET {} = {} WHERE {}",
        quote_identifier(&update.table_name, db_type),
        quote_identifier(&update.column_name, db_type),
        update.value.to_sql_literal(db_type),
        where_parts.join(" AND "),
    ))
}

/// Execute deferred FK updates on PostgreSQL within a transaction.
async fn execute_deferred_updates_pg(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    updates: &[DeferredUpdate],
    data: &GeneratedData,
    schema: &DatabaseSchema,
) -> Result<()> {
    let db_type = &DatabaseType::PostgreSQL;
    for update in updates {
        if let Some(sql) = build_deferred_update(update, data, schema, db_type) {
            sqlx::query(&sql)
                .execute(&mut **tx)
                .await
                .map_err(|e| SeedKitError::InsertFailed {
                    table: update.table_name.clone(),
                    row_index: update.row_index,
                    message: format!(
                        "Deferred FK UPDATE failed for column {}",
                        update.column_name
                    ),
                    sql_preview: truncate_sql(&sql, 200),
                    source: e,
                })?;
        }
    }
    Ok(())
}

/// Execute deferred FK updates on MySQL within a transaction.
async fn execute_deferred_updates_mysql(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    updates: &[DeferredUpdate],
    data: &GeneratedData,
    schema: &DatabaseSchema,
) -> Result<()> {
    let db_type = &DatabaseType::MySQL;
    for update in updates {
        if let Some(sql) = build_deferred_update(update, data, schema, db_type) {
            sqlx::query(&sql)
                .execute(&mut **tx)
                .await
                .map_err(|e| SeedKitError::InsertFailed {
                    table: update.table_name.clone(),
                    row_index: update.row_index,
                    message: format!(
                        "Deferred FK UPDATE failed for column {}",
                        update.column_name
                    ),
                    sql_preview: truncate_sql(&sql, 200),
                    source: e,
                })?;
        }
    }
    Ok(())
}

/// Execute deferred FK updates on SQLite within a transaction.
async fn execute_deferred_updates_sqlite(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    updates: &[DeferredUpdate],
    data: &GeneratedData,
    schema: &DatabaseSchema,
) -> Result<()> {
    let db_type = &DatabaseType::SQLite;
    for update in updates {
        if let Some(sql) = build_deferred_update(update, data, schema, db_type) {
            sqlx::query(&sql)
                .execute(&mut **tx)
                .await
                .map_err(|e| SeedKitError::InsertFailed {
                    table: update.table_name.clone(),
                    row_index: update.row_index,
                    message: format!(
                        "Deferred FK UPDATE failed for column {}",
                        update.column_name
                    ),
                    sql_preview: truncate_sql(&sql, 200),
                    source: e,
                })?;
        }
    }
    Ok(())
}

/// Quote a SQL identifier based on database type.
fn quote_identifier(name: &str, db_type: &DatabaseType) -> String {
    match db_type {
        DatabaseType::MySQL => format!("`{}`", name),
        _ => format!("\"{}\"", name),
    }
}

/// Truncate a SQL string for error messages.
fn truncate_sql(sql: &str, max_len: usize) -> String {
    if sql.len() <= max_len {
        sql.to_string()
    } else {
        format!("{}...", &sql[..max_len])
    }
}

/// Sanitize a database URL for error messages (hide password).
///
/// Uses the `url` crate for proper RFC 3986 parsing instead of fragile
/// string slicing. Handles all edge cases: encoded characters, unusual
/// ports, query parameters, usernames with special characters, etc.
fn sanitize_url(db_url: &str) -> String {
    if let Ok(mut parsed) = url::Url::parse(db_url) {
        if parsed.password().is_some() {
            let _ = parsed.set_password(Some("****"));
        }
        return parsed.to_string();
    }
    // If URL parsing fails, return as-is (e.g., SQLite file paths)
    db_url.to_string()
}

/// Report progress in batches to avoid terminal I/O overhead.
fn report_progress(
    callback: Option<&(dyn Fn(usize, usize) + Send + Sync)>,
    current: usize,
    total: usize,
) {
    if let Some(cb) = callback {
        if current.is_multiple_of(PROGRESS_BATCH_SIZE) || current == total {
            cb(current, total);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    #[test]
    fn test_build_batched_insert() {
        let col_name = "name".to_string();
        let col_age = "age".to_string();

        let mut row1 = IndexMap::new();
        row1.insert(col_name.clone(), Value::String(Cow::Borrowed("Alice")));
        row1.insert(col_age.clone(), Value::Int(30));

        let mut row2 = IndexMap::new();
        row2.insert(col_name.clone(), Value::String(Cow::Borrowed("Bob")));
        row2.insert(col_age.clone(), Value::Int(25));

        let columns = vec![&col_name, &col_age];
        let db_type = DatabaseType::PostgreSQL;
        let rows = [row1, row2];

        let sql = build_batched_insert("\"users\"", "\"name\", \"age\"", &columns, &rows, &db_type);

        assert!(sql.starts_with("INSERT INTO \"users\" (\"name\", \"age\") VALUES "));
        assert!(sql.contains("('Alice', 30)"));
        assert!(sql.contains("('Bob', 25)"));
    }

    #[test]
    fn test_build_batched_insert_mysql_quoting() {
        let col_email = "email".to_string();

        let mut row = IndexMap::new();
        row.insert(col_email.clone(), Value::String(Cow::Borrowed("a@b.com")));

        let columns = vec![&col_email];
        let db_type = DatabaseType::MySQL;
        let rows = [row];

        let sql = build_batched_insert("`users`", "`email`", &columns, &rows, &db_type);

        assert!(sql.starts_with("INSERT INTO `users`"));
        assert!(sql.contains("('a@b.com')"));
    }

    #[test]
    fn test_quote_identifier_postgres() {
        assert_eq!(
            quote_identifier("users", &DatabaseType::PostgreSQL),
            "\"users\""
        );
    }

    #[test]
    fn test_quote_identifier_mysql() {
        assert_eq!(quote_identifier("users", &DatabaseType::MySQL), "`users`");
    }

    #[test]
    fn test_truncate_sql_short() {
        let sql = "SELECT 1";
        assert_eq!(truncate_sql(sql, 200), "SELECT 1");
    }

    #[test]
    fn test_truncate_sql_long() {
        let sql = "A".repeat(300);
        let truncated = truncate_sql(&sql, 200);
        assert_eq!(truncated.len(), 203); // 200 chars + "..."
        assert!(truncated.ends_with("..."));
    }

    // --- URL sanitization tests (using the url crate) ---

    #[test]
    fn test_sanitize_url_hides_password() {
        let url = "postgres://user:secret123@localhost:5432/mydb";
        let sanitized = sanitize_url(url);
        assert!(!sanitized.contains("secret123"));
        assert!(sanitized.contains("****"));
        assert!(sanitized.contains("user"));
        assert!(sanitized.contains("localhost"));
        assert!(sanitized.contains("mydb"));
    }

    #[test]
    fn test_sanitize_url_no_password() {
        let url = "sqlite://test.db";
        let sanitized = sanitize_url(url);
        // Should not crash or mangle the URL
        assert!(sanitized.contains("test.db"));
    }

    #[test]
    fn test_sanitize_url_encoded_password() {
        // Password with special chars: p@ss:w0rd → URL-encoded as p%40ss%3Aw0rd
        let url = "postgres://admin:p%40ss%3Aw0rd@db.example.com:5432/prod";
        let sanitized = sanitize_url(url);
        assert!(!sanitized.contains("p%40ss"));
        assert!(!sanitized.contains("p@ss"));
        assert!(sanitized.contains("****"));
        assert!(sanitized.contains("admin"));
        assert!(sanitized.contains("db.example.com"));
    }

    #[test]
    fn test_sanitize_url_mysql_with_query_params() {
        let url = "mysql://root:hunter2@localhost:3306/mydb?ssl-mode=required";
        let sanitized = sanitize_url(url);
        assert!(!sanitized.contains("hunter2"));
        assert!(sanitized.contains("****"));
        assert!(sanitized.contains("ssl-mode=required"));
    }

    #[test]
    fn test_sanitize_url_no_credentials() {
        let url = "postgres://localhost:5432/mydb";
        let sanitized = sanitize_url(url);
        assert!(!sanitized.contains("****"));
        assert!(sanitized.contains("localhost"));
    }

    #[test]
    fn test_sanitize_url_username_no_password() {
        let url = "postgres://readonly@localhost:5432/mydb";
        let sanitized = sanitize_url(url);
        assert!(!sanitized.contains("****"));
        assert!(sanitized.contains("readonly"));
    }

    // --- Deferred update tests ---

    #[test]
    fn test_build_deferred_update() {
        use crate::schema::types::{DatabaseSchema, PrimaryKey, Table};

        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());
        let mut table = Table::new("users".to_string());
        table.primary_key = Some(PrimaryKey {
            columns: vec!["id".to_string()],
            name: None,
        });
        schema.tables.insert("users".to_string(), table);

        let mut row = IndexMap::new();
        row.insert("id".to_string(), Value::Int(5));
        row.insert("name".to_string(), Value::String(Cow::Borrowed("Alice")));

        let mut tables = IndexMap::new();
        tables.insert("users".to_string(), vec![row]);

        let data = GeneratedData {
            tables,
            deferred_updates: Vec::new(),
        };

        let update = DeferredUpdate {
            table_name: "users".to_string(),
            row_index: 0,
            column_name: "manager_id".to_string(),
            value: Value::Int(3),
        };

        let sql = build_deferred_update(&update, &data, &schema, &DatabaseType::PostgreSQL);
        assert!(sql.is_some());
        let sql = sql.unwrap();
        assert!(sql.contains("UPDATE \"users\""));
        assert!(sql.contains("SET \"manager_id\" = 3"));
        assert!(sql.contains("WHERE \"id\" = 5"));
    }
}
