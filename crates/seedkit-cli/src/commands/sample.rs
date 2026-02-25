use std::path::Path;

use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};

use seedkit_core::sample;
use seedkit_core::sample::mask::mask_pii_distributions;
use seedkit_core::sample::stats::{extract_distributions, SampleOptions};
use seedkit_core::schema::introspect::{database_type_from_url, SchemaIntrospector};
use seedkit_core::schema::types::DatabaseType;

use crate::args::SampleArgs;

pub async fn run(args: &SampleArgs) -> Result<()> {
    let config = seedkit_core::config::read_config(Path::new("."))?;

    let db_url = resolve_db_url(args.db.as_deref(), config.as_ref())?;
    let db_type = database_type_from_url(&db_url)?;

    // Step 1: Introspect schema
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} [{prefix}] {msg}")
            .unwrap(),
    );
    pb.set_prefix("1/3");
    pb.set_message("Introspecting schema...");
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

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
            let db_name = extract_mysql_db_name(&db_url).unwrap_or("mysql".to_string());
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

    pb.finish_with_message(format!(
        "Introspecting schema... done ({} tables)",
        schema.table_count()
    ));

    // Step 2: Extract distributions
    let pb2 = ProgressBar::new_spinner();
    pb2.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} [{prefix}] {msg}")
            .unwrap(),
    );
    pb2.set_prefix("2/3");
    pb2.set_message("Sampling distributions...");
    pb2.enable_steady_tick(std::time::Duration::from_millis(100));

    let options = SampleOptions {
        tables: if args.tables.is_empty() {
            None
        } else {
            Some(args.tables.clone())
        },
        categorical_limit: args.categorical_limit,
        min_row_count: args.min_rows,
    };

    let mut profiles = extract_distributions(&db_url, &schema, &options).await?;

    pb2.finish_with_message(format!(
        "Sampling distributions... done ({} tables profiled)",
        profiles.len()
    ));

    // Step 3: Mask PII and save
    let pb3 = ProgressBar::new_spinner();
    pb3.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} [{prefix}] {msg}")
            .unwrap(),
    );
    pb3.set_prefix("3/3");
    pb3.set_message("Masking PII...");

    let masked_count = mask_pii_distributions(&mut profiles);
    if masked_count > 0 {
        pb3.finish_with_message(format!(
            "Masking PII... done ({} columns masked)",
            masked_count
        ));
    } else {
        pb3.finish_with_message("Masking PII... done (no PII detected)");
    }

    // Save profiles
    let output_path = args.output.as_deref().unwrap_or(sample::PROFILES_FILE_NAME);
    let path = Path::new(output_path);
    sample::save_profiles(&profiles, path)?;

    // Summary
    let total_distributions: usize = profiles.iter().map(|p| p.column_distributions.len()).sum();
    eprintln!(
        "\nSaved {} distribution profiles ({} column distributions) to {}",
        profiles.len(),
        total_distributions,
        output_path
    );
    eprintln!("Use with: seedkit generate --subset {}", output_path);

    Ok(())
}

fn resolve_db_url(
    explicit: Option<&str>,
    config: Option<&seedkit_core::config::SeedKitConfig>,
) -> Result<String> {
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
    if let Some(cfg) = config {
        if let Some(ref url) = cfg.database.url {
            return Ok(url.clone());
        }
    }
    Err(seedkit_core::error::SeedKitError::NoDatabaseUrl.into())
}

fn extract_mysql_db_name(url: &str) -> Option<String> {
    url.rsplit('/')
        .next()
        .map(|s| s.split('?').next().unwrap_or(s).to_string())
}
