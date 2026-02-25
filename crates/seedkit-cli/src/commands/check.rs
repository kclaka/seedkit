use std::path::Path;
use std::process;

use anyhow::{bail, Context, Result};

use seedkit_core::check;
use seedkit_core::lock;
use seedkit_core::schema::introspect::{database_type_from_url, SchemaIntrospector};
use seedkit_core::schema::types::DatabaseType;

use crate::args::{CheckArgs, CheckFormat};

/// Run schema drift detection against seedkit.lock.
///
/// Exit codes:
///   0 — no drift detected
///   1 — drift detected (or error)
pub async fn run(args: &CheckArgs) -> Result<()> {
    let lock_path = Path::new(lock::LOCK_FILE_NAME);
    if !lock_path.exists() {
        bail!(
            "No {} found. Run `seedkit generate` first to create a lock file.",
            lock::LOCK_FILE_NAME,
        );
    }

    let lock_file = lock::read_lock_file(lock_path)?;

    // Resolve DB and introspect
    let db_url = resolve_db_url(args.db.as_deref())?;
    let db_type = database_type_from_url(&db_url)?;

    let schema = match db_type {
        DatabaseType::PostgreSQL => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(5)
                .connect(&db_url)
                .await
                .context("Failed to connect to PostgreSQL")?;
            let introspector = seedkit_core::schema::postgres::PostgresIntrospector::new(pool);
            introspector.introspect().await?
        }
        DatabaseType::MySQL => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(5)
                .connect(&db_url)
                .await
                .context("Failed to connect to MySQL")?;
            let db_name = db_url
                .rsplit('/')
                .next()
                .map(|s| s.split('?').next().unwrap_or(s).to_string())
                .unwrap_or("mysql".to_string());
            let introspector = seedkit_core::schema::mysql::MySqlIntrospector::new(pool, db_name);
            introspector.introspect().await?
        }
        DatabaseType::SQLite => {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(&db_url)
                .await
                .context("Failed to connect to SQLite")?;
            let introspector = seedkit_core::schema::sqlite::SqliteIntrospector::new(pool);
            introspector.introspect().await?
        }
    };

    let report = check::check_drift(&lock_file.schema_snapshot, &lock_file.schema_hash, &schema);

    match args.format {
        CheckFormat::Json => {
            let json = serde_json::to_string_pretty(&report)
                .context("Failed to serialize drift report")?;
            println!("{}", json);
        }
        CheckFormat::Text => {
            println!("{}", report.summary());
        }
    }

    if report.has_drift {
        process::exit(1);
    }

    Ok(())
}

fn resolve_db_url(explicit: Option<&str>) -> Result<String> {
    if let Some(url) = explicit {
        return Ok(url.to_string());
    }
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return Ok(url);
    }
    if dotenvy::dotenv().is_ok() {
        if let Ok(url) = std::env::var("DATABASE_URL") {
            return Ok(url);
        }
    }
    // Try seedkit.toml
    if let Some(config) = seedkit_core::config::read_config(std::path::Path::new("."))? {
        if let Some(url) = config.database.url {
            return Ok(url);
        }
    }
    Err(seedkit_core::error::SeedKitError::NoDatabaseUrl.into())
}
