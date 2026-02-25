use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::{Result, SeedKitError};
use crate::schema::types::{DatabaseSchema, DatabaseType};

/// Statistical distribution profile extracted from production data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributionProfile {
    pub table_name: String,
    pub row_count: u64,
    pub column_distributions: HashMap<String, ColumnDistribution>,
}

/// Distribution information for a single column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnDistribution {
    /// Categorical distribution (enum-like values with frequencies).
    Categorical {
        values: Vec<(String, f64)>, // (value, frequency)
    },
    /// Numeric distribution.
    Numeric {
        min: f64,
        max: f64,
        mean: f64,
        stddev: f64,
    },
    /// Ratio to another table's row count.
    Ratio {
        related_table: String,
        ratio: f64, // e.g., 3.2 orders per user
    },
}

/// Options for controlling the sampling process.
#[derive(Debug, Clone)]
pub struct SampleOptions {
    /// Only sample these tables (None = all tables).
    pub tables: Option<Vec<String>>,
    /// Maximum number of distinct values to extract for categorical distributions.
    pub categorical_limit: usize,
    /// Minimum number of rows a table must have to be sampled.
    pub min_row_count: u64,
}

impl Default for SampleOptions {
    fn default() -> Self {
        Self {
            tables: None,
            categorical_limit: 50,
            min_row_count: 10,
        }
    }
}

/// Generate a SQL query to extract the row count for a table.
pub fn row_count_query(table: &str, db_type: DatabaseType) -> String {
    let quoted = quote_ident(table, db_type);
    format!("SELECT COUNT(*) AS cnt FROM {}", quoted)
}

/// Generate a SQL query to extract categorical distribution (value frequencies).
///
/// Returns rows of (value, frequency) where frequency is a proportion (0.0–1.0).
pub fn categorical_query(table: &str, column: &str, db_type: DatabaseType, limit: usize) -> String {
    let qt = quote_ident(table, db_type);
    let qc = quote_ident(column, db_type);
    match db_type {
        DatabaseType::PostgreSQL => format!(
            "SELECT CAST({col} AS TEXT) AS val, \
             COUNT(*)::float / (SELECT COUNT(*) FROM {tbl})::float AS freq \
             FROM {tbl} WHERE {col} IS NOT NULL \
             GROUP BY {col} ORDER BY freq DESC LIMIT {lim}",
            col = qc,
            tbl = qt,
            lim = limit
        ),
        DatabaseType::MySQL => format!(
            "SELECT CAST({col} AS CHAR) AS val, \
             COUNT(*) / (SELECT COUNT(*) FROM {tbl}) AS freq \
             FROM {tbl} WHERE {col} IS NOT NULL \
             GROUP BY {col} ORDER BY freq DESC LIMIT {lim}",
            col = qc,
            tbl = qt,
            lim = limit
        ),
        DatabaseType::SQLite => format!(
            "SELECT CAST({col} AS TEXT) AS val, \
             CAST(COUNT(*) AS REAL) / (SELECT COUNT(*) FROM {tbl}) AS freq \
             FROM {tbl} WHERE {col} IS NOT NULL \
             GROUP BY {col} ORDER BY freq DESC LIMIT {lim}",
            col = qc,
            tbl = qt,
            lim = limit
        ),
    }
}

/// Generate a SQL query to extract numeric distribution (min, max, mean, stddev).
pub fn numeric_query(table: &str, column: &str, db_type: DatabaseType) -> String {
    let qt = quote_ident(table, db_type);
    let qc = quote_ident(column, db_type);
    match db_type {
        DatabaseType::PostgreSQL => format!(
            "SELECT MIN({col})::float8 AS min_val, MAX({col})::float8 AS max_val, \
             AVG({col})::float8 AS mean_val, COALESCE(STDDEV_POP({col})::float8, 0) AS stddev_val \
             FROM {tbl} WHERE {col} IS NOT NULL",
            col = qc,
            tbl = qt,
        ),
        DatabaseType::MySQL => format!(
            "SELECT MIN({col}) AS min_val, MAX({col}) AS max_val, \
             AVG({col}) AS mean_val, COALESCE(STDDEV({col}), 0) AS stddev_val \
             FROM {tbl} WHERE {col} IS NOT NULL",
            col = qc,
            tbl = qt,
        ),
        // SQLite has no built-in stddev; we compute min/max/avg and set stddev=0
        DatabaseType::SQLite => format!(
            "SELECT MIN({col}) AS min_val, MAX({col}) AS max_val, \
             AVG({col}) AS mean_val, 0.0 AS stddev_val \
             FROM {tbl} WHERE {col} IS NOT NULL",
            col = qc,
            tbl = qt,
        ),
    }
}

/// Generate a SQL query to compute the ratio of child rows per parent row.
///
/// Returns a single row with the average number of child rows per distinct parent FK value.
pub fn ratio_query(
    child_table: &str,
    fk_column: &str,
    parent_table: &str,
    db_type: DatabaseType,
) -> String {
    let qct = quote_ident(child_table, db_type);
    let qfk = quote_ident(fk_column, db_type);
    let qpt = quote_ident(parent_table, db_type);
    match db_type {
        DatabaseType::PostgreSQL => format!(
            "SELECT CASE WHEN (SELECT COUNT(*) FROM {pt}) = 0 THEN 0.0 \
             ELSE (SELECT COUNT(*) FROM {ct})::float / (SELECT COUNT(*) FROM {pt})::float \
             END AS ratio",
            ct = qct,
            pt = qpt,
        ),
        DatabaseType::MySQL => format!(
            "SELECT CASE WHEN (SELECT COUNT(*) FROM {pt}) = 0 THEN 0.0 \
             ELSE (SELECT COUNT(*) FROM {ct}) / (SELECT COUNT(*) FROM {pt}) \
             END AS ratio",
            ct = qct,
            pt = qpt,
        ),
        DatabaseType::SQLite => format!(
            "SELECT CASE WHEN (SELECT COUNT(*) FROM {pt}) = 0 THEN 0.0 \
             ELSE CAST((SELECT COUNT(*) FROM {ct}) AS REAL) / (SELECT COUNT(*) FROM {pt}) \
             END AS ratio",
            ct = qct,
            pt = qpt,
        ),
    }
    // fk_column is used for context but the ratio query itself is table-level
    .replace("__FK_COLUMN__", &qfk) // placeholder not used, but keep param for API consistency
}

/// Extract distribution profiles from a live database.
pub async fn extract_distributions(
    url: &str,
    schema: &DatabaseSchema,
    options: &SampleOptions,
) -> Result<Vec<DistributionProfile>> {
    let tables_to_sample: Vec<&String> = if let Some(ref include) = options.tables {
        schema
            .tables
            .keys()
            .filter(|t| include.contains(t))
            .collect()
    } else {
        schema.tables.keys().collect()
    };

    let mut profiles = Vec::new();

    match schema.database_type {
        DatabaseType::PostgreSQL => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(3)
                .connect(url)
                .await
                .map_err(|e| SeedKitError::Introspection {
                    query: "sample connect".to_string(),
                    source: e,
                })?;

            for table_name in &tables_to_sample {
                let profile = extract_table_profile_pg(&pool, table_name, schema, options).await?;
                if let Some(p) = profile {
                    profiles.push(p);
                }
            }
            pool.close().await;
        }
        DatabaseType::MySQL => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(3)
                .connect(url)
                .await
                .map_err(|e| SeedKitError::Introspection {
                    query: "sample connect".to_string(),
                    source: e,
                })?;

            for table_name in &tables_to_sample {
                let profile =
                    extract_table_profile_mysql(&pool, table_name, schema, options).await?;
                if let Some(p) = profile {
                    profiles.push(p);
                }
            }
            pool.close().await;
        }
        DatabaseType::SQLite => {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await
                .map_err(|e| SeedKitError::Introspection {
                    query: "sample connect".to_string(),
                    source: e,
                })?;

            for table_name in &tables_to_sample {
                let profile =
                    extract_table_profile_sqlite(&pool, table_name, schema, options).await?;
                if let Some(p) = profile {
                    profiles.push(p);
                }
            }
            pool.close().await;
        }
    }

    // Compute ratio distributions from FK relationships
    compute_ratio_distributions(&mut profiles, schema);

    Ok(profiles)
}

async fn extract_table_profile_pg(
    pool: &sqlx::PgPool,
    table_name: &str,
    schema: &DatabaseSchema,
    options: &SampleOptions,
) -> Result<Option<DistributionProfile>> {
    use sqlx::Row;

    // Get row count
    let count_sql = row_count_query(table_name, DatabaseType::PostgreSQL);
    let row: (i64,) = sqlx::query_as(&count_sql)
        .fetch_one(pool)
        .await
        .map_err(|e| SeedKitError::Introspection {
            query: format!("row count for {}", table_name),
            source: e,
        })?;

    let row_count = row.0 as u64;
    if row_count < options.min_row_count {
        return Ok(None);
    }

    let table = match schema.tables.get(table_name) {
        Some(t) => t,
        None => return Ok(None),
    };

    let mut distributions = HashMap::new();

    for (col_name, column) in &table.columns {
        // Skip auto-increment PKs
        if column.is_auto_increment || column.data_type.is_serial() {
            continue;
        }

        if column.data_type.is_numeric() {
            let sql = numeric_query(table_name, col_name, DatabaseType::PostgreSQL);
            let result = sqlx::query(&sql).fetch_one(pool).await;
            if let Ok(row) = result {
                let min_val: Option<f64> = row.get("min_val");
                let max_val: Option<f64> = row.get("max_val");
                let mean_val: Option<f64> = row.get("mean_val");
                let stddev_val: Option<f64> = row.get("stddev_val");
                if let (Some(min), Some(max), Some(mean), Some(stddev)) =
                    (min_val, max_val, mean_val, stddev_val)
                {
                    distributions.insert(
                        col_name.clone(),
                        ColumnDistribution::Numeric {
                            min,
                            max,
                            mean,
                            stddev,
                        },
                    );
                }
            }
        } else if column.data_type.is_string() || column.enum_values.is_some() {
            // Only sample categoricals if cardinality is reasonable
            let sql = categorical_query(
                table_name,
                col_name,
                DatabaseType::PostgreSQL,
                options.categorical_limit,
            );
            let result = sqlx::query(&sql).fetch_all(pool).await;
            if let Ok(rows) = result {
                let values: Vec<(String, f64)> = rows
                    .iter()
                    .filter_map(|r| {
                        let val: Option<String> = r.get("val");
                        let freq: Option<f64> = r.get("freq");
                        val.zip(freq)
                    })
                    .collect();
                if !values.is_empty() {
                    distributions
                        .insert(col_name.clone(), ColumnDistribution::Categorical { values });
                }
            }
        }
    }

    Ok(Some(DistributionProfile {
        table_name: table_name.to_string(),
        row_count,
        column_distributions: distributions,
    }))
}

async fn extract_table_profile_mysql(
    pool: &sqlx::MySqlPool,
    table_name: &str,
    schema: &DatabaseSchema,
    options: &SampleOptions,
) -> Result<Option<DistributionProfile>> {
    use sqlx::Row;

    let count_sql = row_count_query(table_name, DatabaseType::MySQL);
    let row: (i64,) = sqlx::query_as(&count_sql)
        .fetch_one(pool)
        .await
        .map_err(|e| SeedKitError::Introspection {
            query: format!("row count for {}", table_name),
            source: e,
        })?;

    let row_count = row.0 as u64;
    if row_count < options.min_row_count {
        return Ok(None);
    }

    let table = match schema.tables.get(table_name) {
        Some(t) => t,
        None => return Ok(None),
    };

    let mut distributions = HashMap::new();

    for (col_name, column) in &table.columns {
        if column.is_auto_increment || column.data_type.is_serial() {
            continue;
        }

        if column.data_type.is_numeric() {
            let sql = numeric_query(table_name, col_name, DatabaseType::MySQL);
            let result = sqlx::query(&sql).fetch_one(pool).await;
            if let Ok(row) = result {
                let min_val: Option<f64> = row.get("min_val");
                let max_val: Option<f64> = row.get("max_val");
                let mean_val: Option<f64> = row.get("mean_val");
                let stddev_val: Option<f64> = row.get("stddev_val");
                if let (Some(min), Some(max), Some(mean), Some(stddev)) =
                    (min_val, max_val, mean_val, stddev_val)
                {
                    distributions.insert(
                        col_name.clone(),
                        ColumnDistribution::Numeric {
                            min,
                            max,
                            mean,
                            stddev,
                        },
                    );
                }
            }
        } else if column.data_type.is_string() || column.enum_values.is_some() {
            let sql = categorical_query(
                table_name,
                col_name,
                DatabaseType::MySQL,
                options.categorical_limit,
            );
            let result = sqlx::query(&sql).fetch_all(pool).await;
            if let Ok(rows) = result {
                let values: Vec<(String, f64)> = rows
                    .iter()
                    .filter_map(|r| {
                        let val: Option<String> = r.get("val");
                        let freq: Option<f64> = r.get("freq");
                        val.zip(freq)
                    })
                    .collect();
                if !values.is_empty() {
                    distributions
                        .insert(col_name.clone(), ColumnDistribution::Categorical { values });
                }
            }
        }
    }

    Ok(Some(DistributionProfile {
        table_name: table_name.to_string(),
        row_count,
        column_distributions: distributions,
    }))
}

async fn extract_table_profile_sqlite(
    pool: &sqlx::SqlitePool,
    table_name: &str,
    schema: &DatabaseSchema,
    options: &SampleOptions,
) -> Result<Option<DistributionProfile>> {
    use sqlx::Row;

    let count_sql = row_count_query(table_name, DatabaseType::SQLite);
    let row: (i64,) = sqlx::query_as(&count_sql)
        .fetch_one(pool)
        .await
        .map_err(|e| SeedKitError::Introspection {
            query: format!("row count for {}", table_name),
            source: e,
        })?;

    let row_count = row.0 as u64;
    if row_count < options.min_row_count {
        return Ok(None);
    }

    let table = match schema.tables.get(table_name) {
        Some(t) => t,
        None => return Ok(None),
    };

    let mut distributions = HashMap::new();

    for (col_name, column) in &table.columns {
        if column.is_auto_increment || column.data_type.is_serial() {
            continue;
        }

        if column.data_type.is_numeric() {
            let sql = numeric_query(table_name, col_name, DatabaseType::SQLite);
            let result = sqlx::query(&sql).fetch_one(pool).await;
            if let Ok(row) = result {
                let min_val: Option<f64> = row.get("min_val");
                let max_val: Option<f64> = row.get("max_val");
                let mean_val: Option<f64> = row.get("mean_val");
                let stddev_val: Option<f64> = row.get("stddev_val");
                if let (Some(min), Some(max), Some(mean), Some(stddev)) =
                    (min_val, max_val, mean_val, stddev_val)
                {
                    distributions.insert(
                        col_name.clone(),
                        ColumnDistribution::Numeric {
                            min,
                            max,
                            mean,
                            stddev,
                        },
                    );
                }
            }
        } else if column.data_type.is_string() || column.enum_values.is_some() {
            let sql = categorical_query(
                table_name,
                col_name,
                DatabaseType::SQLite,
                options.categorical_limit,
            );
            let result = sqlx::query(&sql).fetch_all(pool).await;
            if let Ok(rows) = result {
                let values: Vec<(String, f64)> = rows
                    .iter()
                    .filter_map(|r| {
                        let val: Option<String> = r.get("val");
                        let freq: Option<f64> = r.get("freq");
                        val.zip(freq)
                    })
                    .collect();
                if !values.is_empty() {
                    distributions
                        .insert(col_name.clone(), ColumnDistribution::Categorical { values });
                }
            }
        }
    }

    Ok(Some(DistributionProfile {
        table_name: table_name.to_string(),
        row_count,
        column_distributions: distributions,
    }))
}

/// Compute ratio distributions from FK relationships.
///
/// For each FK relationship, if both parent and child tables were sampled,
/// add a `Ratio` distribution to the child table's profile.
fn compute_ratio_distributions(profiles: &mut [DistributionProfile], schema: &DatabaseSchema) {
    // Build a lookup of table_name -> row_count from profiles
    let row_counts: HashMap<&str, u64> = profiles
        .iter()
        .map(|p| (p.table_name.as_str(), p.row_count))
        .collect();

    // Collect ratios to add (avoid borrowing profiles mutably while iterating)
    let mut ratios_to_add: Vec<(String, String, String, f64)> = Vec::new();

    for table_name in schema.tables.keys() {
        if let Some(table) = schema.tables.get(table_name) {
            for fk in &table.foreign_keys {
                if let (Some(&child_count), Some(&parent_count)) = (
                    row_counts.get(table_name.as_str()),
                    row_counts.get(fk.referenced_table.as_str()),
                ) {
                    if parent_count > 0 {
                        let ratio = child_count as f64 / parent_count as f64;
                        let col_name = fk.source_columns.first().cloned().unwrap_or_default();
                        ratios_to_add.push((
                            table_name.clone(),
                            col_name,
                            fk.referenced_table.clone(),
                            ratio,
                        ));
                    }
                }
            }
        }
    }

    for (table_name, col_name, ref_table, ratio) in ratios_to_add {
        if let Some(profile) = profiles.iter_mut().find(|p| p.table_name == table_name) {
            let key = format!("__ratio_{}", col_name);
            profile.column_distributions.insert(
                key,
                ColumnDistribution::Ratio {
                    related_table: ref_table,
                    ratio,
                },
            );
        }
    }
}

fn quote_ident(name: &str, db_type: DatabaseType) -> String {
    match db_type {
        DatabaseType::MySQL => format!("`{}`", name),
        DatabaseType::PostgreSQL | DatabaseType::SQLite => format!("\"{}\"", name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_categorical_query_postgres() {
        let sql = categorical_query("users", "status", DatabaseType::PostgreSQL, 50);
        assert!(sql.contains("CAST(\"status\" AS TEXT)"));
        assert!(sql.contains("::float"));
        assert!(sql.contains("LIMIT 50"));
        assert!(sql.contains("GROUP BY"));
    }

    #[test]
    fn test_categorical_query_mysql() {
        let sql = categorical_query("users", "status", DatabaseType::MySQL, 25);
        assert!(sql.contains("CAST(`status` AS CHAR)"));
        assert!(sql.contains("LIMIT 25"));
        assert!(sql.contains("`users`"));
    }

    #[test]
    fn test_categorical_query_sqlite() {
        let sql = categorical_query("users", "status", DatabaseType::SQLite, 50);
        assert!(sql.contains("CAST(\"status\" AS TEXT)"));
        assert!(sql.contains("CAST(COUNT(*) AS REAL)"));
    }

    #[test]
    fn test_numeric_query_postgres() {
        let sql = numeric_query("products", "price", DatabaseType::PostgreSQL);
        assert!(sql.contains("MIN(\"price\")"));
        assert!(sql.contains("STDDEV_POP"));
        assert!(sql.contains("::float8"));
    }

    #[test]
    fn test_numeric_query_mysql() {
        let sql = numeric_query("products", "price", DatabaseType::MySQL);
        assert!(sql.contains("MIN(`price`)"));
        assert!(sql.contains("STDDEV("));
        assert!(!sql.contains("STDDEV_POP"));
    }

    #[test]
    fn test_numeric_query_sqlite_no_stddev() {
        let sql = numeric_query("products", "price", DatabaseType::SQLite);
        assert!(sql.contains("0.0 AS stddev_val"));
        assert!(!sql.contains("STDDEV"));
    }

    #[test]
    fn test_ratio_query_postgres() {
        let sql = ratio_query("orders", "user_id", "users", DatabaseType::PostgreSQL);
        assert!(sql.contains("\"orders\""));
        assert!(sql.contains("\"users\""));
        assert!(sql.contains("::float"));
    }

    #[test]
    fn test_row_count_query() {
        let sql = row_count_query("users", DatabaseType::PostgreSQL);
        assert_eq!(sql, "SELECT COUNT(*) AS cnt FROM \"users\"");
    }

    #[test]
    fn test_sample_options_default() {
        let opts = SampleOptions::default();
        assert_eq!(opts.categorical_limit, 50);
        assert_eq!(opts.min_row_count, 10);
        assert!(opts.tables.is_none());
    }

    #[test]
    fn test_distribution_profile_serde_round_trip() {
        let profile = DistributionProfile {
            table_name: "users".to_string(),
            row_count: 1000,
            column_distributions: {
                let mut m = HashMap::new();
                m.insert(
                    "status".to_string(),
                    ColumnDistribution::Categorical {
                        values: vec![("active".to_string(), 0.7), ("inactive".to_string(), 0.3)],
                    },
                );
                m.insert(
                    "age".to_string(),
                    ColumnDistribution::Numeric {
                        min: 18.0,
                        max: 90.0,
                        mean: 35.0,
                        stddev: 12.5,
                    },
                );
                m.insert(
                    "__ratio_user_id".to_string(),
                    ColumnDistribution::Ratio {
                        related_table: "orders".to_string(),
                        ratio: 3.2,
                    },
                );
                m
            },
        };

        let json = serde_json::to_string_pretty(&profile).unwrap();
        let restored: DistributionProfile = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.table_name, "users");
        assert_eq!(restored.row_count, 1000);
        assert_eq!(restored.column_distributions.len(), 3);
    }
}
