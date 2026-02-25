use anyhow::{Context, Result};
use comfy_table::{Cell, Table as ComfyTable};

use seedkit_core::classify::rules::classify_schema;
use seedkit_core::schema::introspect::{database_type_from_url, SchemaIntrospector};
use seedkit_core::schema::types::DatabaseType;

use crate::args::IntrospectArgs;

pub async fn run(args: &IntrospectArgs) -> Result<()> {
    let db_url = args
        .db
        .as_deref()
        .ok_or_else(|| seedkit_core::error::SeedKitError::NoDatabaseUrl)?;

    let db_type = database_type_from_url(db_url)?;

    let schema = match db_type {
        DatabaseType::PostgreSQL => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(5)
                .connect(db_url)
                .await
                .context("Failed to connect to PostgreSQL")?;
            let introspector = seedkit_core::schema::postgres::PostgresIntrospector::new(pool);
            introspector.introspect().await?
        }
        DatabaseType::MySQL => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(5)
                .connect(db_url)
                .await
                .context("Failed to connect to MySQL")?;
            let db_name = db_url
                .rsplit('/')
                .next()
                .unwrap_or("mysql")
                .split('?')
                .next()
                .unwrap_or("mysql")
                .to_string();
            let introspector = seedkit_core::schema::mysql::MySqlIntrospector::new(pool, db_name);
            introspector.introspect().await?
        }
        DatabaseType::SQLite => {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(db_url)
                .await
                .context("Failed to connect to SQLite")?;
            let introspector = seedkit_core::schema::sqlite::SqliteIntrospector::new(pool);
            introspector.introspect().await?
        }
    };

    let classifications = classify_schema(&schema);

    match args.format {
        crate::args::IntrospectFormat::Json => {
            let json = serde_json::to_string_pretty(&schema)?;
            println!("{}", json);
        }
        crate::args::IntrospectFormat::Table => {
            println!(
                "Database: {} ({})",
                schema.database_name, schema.database_type
            );
            println!(
                "Tables: {}  Columns: {}  Foreign Keys: {}",
                schema.table_count(),
                schema.column_count(),
                schema.foreign_key_count()
            );

            if !schema.enums.is_empty() {
                println!("Enums: {}", schema.enums.len());
            }
            println!();

            for (table_name, table) in &schema.tables {
                println!("━━━ {} ━━━", table_name);

                let mut t = ComfyTable::new();
                t.set_header(vec!["Column", "Type", "Nullable", "PK", "FK", "Semantic"]);

                let pk_columns: Vec<&str> = table
                    .primary_key
                    .as_ref()
                    .map(|pk| pk.columns.iter().map(|s| s.as_str()).collect())
                    .unwrap_or_default();

                for (col_name, column) in &table.columns {
                    let is_pk = pk_columns.contains(&col_name.as_str());
                    let fk_target = table.foreign_keys.iter().find_map(|fk| {
                        if fk.source_columns.contains(col_name) {
                            Some(format!("→ {}", fk.referenced_table))
                        } else {
                            None
                        }
                    });

                    let semantic = classifications
                        .get(&(table_name.clone(), col_name.clone()))
                        .map(|st| format!("{}", st))
                        .unwrap_or_default();

                    t.add_row(vec![
                        Cell::new(col_name),
                        Cell::new(column.data_type.to_string()),
                        Cell::new(if column.nullable { "YES" } else { "NO" }),
                        Cell::new(if is_pk { "PK" } else { "" }),
                        Cell::new(fk_target.as_deref().unwrap_or("")),
                        Cell::new(&semantic),
                    ]);
                }

                println!("{}", t);
                println!();
            }
        }
    }

    Ok(())
}
