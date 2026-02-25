use anyhow::{Context, Result};
use comfy_table::Table as ComfyTable;

use seedkit_core::classify::rules::classify_schema;
use seedkit_core::generate::engine;
use seedkit_core::generate::plan::GenerationPlan;
use seedkit_core::graph::cycle::break_cycles;
use seedkit_core::graph::dag::DependencyGraph;
use seedkit_core::graph::topo::topological_sort;
use seedkit_core::schema::introspect::{database_type_from_url, SchemaIntrospector};
use seedkit_core::schema::types::DatabaseType;

use crate::args::PreviewArgs;

pub async fn run(args: &PreviewArgs) -> Result<()> {
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

    let mut dep_graph = DependencyGraph::from_schema(&schema);
    let deferred = break_cycles(&mut dep_graph, &[])?;
    let insertion_order = topological_sort(&dep_graph)?;

    let seed = 42u64; // Fixed seed for preview
    let plan = GenerationPlan::build(
        &schema,
        &classifications,
        &insertion_order.tables,
        deferred,
        args.rows,
        &std::collections::BTreeMap::new(),
        seed,
        None,
        &std::collections::BTreeMap::new(),
        None,
    );

    let data = engine::execute_plan(&plan, &schema, None)?;

    for (table_name, rows) in &data.tables {
        if rows.is_empty() {
            continue;
        }

        println!("━━━ {} ({} rows) ━━━", table_name, rows.len());

        let columns: Vec<&String> = rows[0].keys().collect();

        let mut t = ComfyTable::new();
        t.set_header(columns.iter().map(|c| c.as_str()).collect::<Vec<_>>());

        for row in rows {
            let values: Vec<String> = columns
                .iter()
                .map(|col| {
                    row.get(*col)
                        .map(|v| {
                            let s = format!("{}", v);
                            if s.len() > 40 {
                                format!("{}...", &s[..37])
                            } else {
                                s
                            }
                        })
                        .unwrap_or_else(|| "NULL".to_string())
                })
                .collect();
            t.add_row(values);
        }

        println!("{}\n", t);
    }

    Ok(())
}
