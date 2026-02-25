use anyhow::{Context, Result};

use seedkit_core::graph::cycle::break_cycles;
use seedkit_core::graph::dag::DependencyGraph;
use seedkit_core::graph::visualize::{self, GraphFormat as VizFormat};
use seedkit_core::schema::introspect::{database_type_from_url, SchemaIntrospector};
use seedkit_core::schema::types::DatabaseType;

use crate::args::GraphArgs;

pub async fn run(args: &GraphArgs) -> Result<()> {
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

    let mut dep_graph = DependencyGraph::from_schema(&schema);
    let deferred = break_cycles(&mut dep_graph, &[])?;

    let format = match args.format {
        crate::args::GraphFormat::Mermaid => VizFormat::Mermaid,
        crate::args::GraphFormat::Dot => VizFormat::Dot,
    };

    let output = visualize::visualize(&dep_graph, &deferred, format);
    println!("{}", output);

    Ok(())
}
