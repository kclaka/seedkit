use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

use anyhow::{bail, Context, Result};
use indicatif::{ProgressBar, ProgressStyle};

use seedkit_core::check::compute_schema_hash;
use seedkit_core::classify::rules::classify_schema;
use seedkit_core::classify::semantic::SemanticType;
use seedkit_core::generate::engine;
use seedkit_core::generate::plan::{filter_insertion_order, GenerationPlan};
use seedkit_core::graph::cycle::break_cycles;
use seedkit_core::graph::dag::DependencyGraph;
use seedkit_core::graph::topo::topological_sort;
use seedkit_core::llm;
use seedkit_core::lock;
use seedkit_core::lock::types::{LockConfig, LockFile};
use seedkit_core::output;
use seedkit_core::schema::introspect::{database_type_from_url, SchemaIntrospector};
use seedkit_core::schema::types::{DatabaseSchema, DatabaseType};

use crate::args::{GenerateArgs, OutputFormat};

pub async fn run(args: &GenerateArgs) -> Result<()> {
    // Load optional seedkit.toml config
    let config = seedkit_core::config::read_config(Path::new("."))?;

    let db_url = resolve_db_url(args.db.as_deref(), config.as_ref())?;
    let db_type = database_type_from_url(&db_url)?;

    // Phase 1: Introspect
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} [{prefix}] {msg}")
            .unwrap(),
    );
    pb.set_prefix("1/4");
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
        "Introspecting schema... ✓ {} tables, {} foreign keys",
        schema.table_count(),
        schema.foreign_key_count()
    ));

    // If --from-lock, load the lock file and validate schema hash
    let lock_path = Path::new(lock::LOCK_FILE_NAME);
    let restored_lock = if args.from_lock {
        if !lock_path.exists() {
            bail!(
                "No {} found in current directory. Run `seedkit generate` first to create one.",
                lock::LOCK_FILE_NAME
            );
        }
        let lf = lock::read_lock_file(lock_path)?;

        let current_hash = compute_schema_hash(&schema);
        if current_hash != lf.schema_hash && !args.force {
            bail!(
                "Schema has changed since {} was created.\n\
                 Lock hash: {}\n\
                 Current:   {}\n\
                 \n\
                 Run with --force to regenerate, or delete {} and run fresh.",
                lock::LOCK_FILE_NAME,
                &lf.schema_hash[..16],
                &current_hash[..16],
                lock::LOCK_FILE_NAME,
            );
        }
        Some(lf)
    } else {
        None
    };

    // Phase 2: Analyze dependencies
    let pb2 = ProgressBar::new_spinner();
    pb2.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} [{prefix}] {msg}")
            .unwrap(),
    );
    pb2.set_prefix("2/4");
    pb2.set_message("Analyzing dependencies...");
    pb2.enable_steady_tick(std::time::Duration::from_millis(100));

    let mut dep_graph = DependencyGraph::from_schema(&schema);
    let cycle_break_hints: Vec<String> = config
        .as_ref()
        .map(|c| c.graph.break_cycle_at.clone())
        .unwrap_or_default();
    let deferred = break_cycles(&mut dep_graph, &cycle_break_hints)?;
    let insertion_order = topological_sort(&dep_graph)?;

    pb2.finish_with_message(format!(
        "Analyzing dependencies... ✓ {} circular dependencies resolved",
        deferred.len()
    ));

    // Classify columns (rule-based)
    let rule_classifications = classify_schema(&schema);

    // Optionally enhance with LLM classification (--ai flag)
    let (classifications, ai_cache) = if args.ai {
        enhance_with_llm(&rule_classifications, &schema, args.model.as_deref()).await?
    } else if let Some(ref lf) = restored_lock {
        // Restore cached AI classifications from lock file
        restore_ai_from_lock(&rule_classifications, lf)
    } else {
        (rule_classifications, None)
    };

    // Build generation plan — use lock file values when restoring
    let (seed, row_count, table_row_overrides, base_time) = if let Some(ref lf) = restored_lock {
        let overrides = lf.config.table_row_overrides.clone();
        let bt = lf.parse_base_time();
        (lf.seed, lf.config.default_row_count, overrides, bt)
    } else {
        let seed = args.seed.unwrap_or_else(|| {
            // Check seedkit.toml for a fixed seed
            if let Some(ref cfg) = config {
                if let Some(s) = cfg.generate.seed {
                    return s;
                }
            }
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        });

        // Merge table row overrides: seedkit.toml as base, CLI --table-rows on top
        let mut overrides = config
            .as_ref()
            .map(|c| c.table_row_overrides())
            .unwrap_or_default();
        // CLI flags override config file
        for (k, v) in args.parse_table_rows() {
            overrides.insert(k, v);
        }

        // Row count: CLI --rows takes priority, then seedkit.toml, then default (100)
        let rows = if args.rows != 100 {
            args.rows
        } else {
            config
                .as_ref()
                .and_then(|c| c.generate.rows)
                .unwrap_or(args.rows)
        };

        (seed, rows, overrides, None)
    };

    // Apply --include / --exclude table filtering.
    // When restoring from lock, use the lock's include/exclude lists.
    let (include, exclude) = if let Some(ref lf) = restored_lock {
        (
            lf.config.include_tables.clone(),
            lf.config.exclude_tables.clone(),
        )
    } else {
        (args.include.clone(), args.exclude.clone())
    };
    let filtered_order =
        filter_insertion_order(&insertion_order.tables, &schema, &include, &exclude);

    let column_overrides = config
        .as_ref()
        .map(|c| c.columns.clone())
        .unwrap_or_default();

    // Load distribution profiles if --subset is specified
    let dist_profiles = if let Some(ref subset_path) = args.subset {
        let path = std::path::Path::new(subset_path);
        let profiles = seedkit_core::sample::load_profiles(path)
            .map_err(|e| anyhow::anyhow!("Failed to load distribution profiles: {}", e))?;
        eprintln!(
            "Loaded {} distribution profiles from {}",
            profiles.len(),
            subset_path
        );
        Some(profiles)
    } else {
        None
    };

    let plan = GenerationPlan::build(
        &schema,
        &classifications,
        &filtered_order,
        deferred,
        row_count,
        &table_row_overrides,
        seed,
        base_time,
        &column_overrides,
        dist_profiles.as_deref(),
    );

    // Phase 3: Generate data
    let total_rows: usize = plan.table_plans.iter().map(|t| t.row_count).sum();
    let pb3 = ProgressBar::new(total_rows as u64);
    pb3.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.cyan} [3/4] Generating data... {bar:40.cyan/dim} {pos}/{len} ({eta})",
            )
            .unwrap()
            .progress_chars("█▓░"),
    );

    let data = engine::execute_plan(
        &plan,
        &schema,
        Some(&|_table, current, _total| {
            pb3.set_position(current as u64);
        }),
    )?;

    pb3.finish_with_message(format!("Generating data... ✓ ({} rows)", total_rows));

    // Phase 4: Output
    let is_direct = args.output.as_deref() == Some("direct");

    if is_direct {
        // Direct database insertion
        let pb4 = ProgressBar::new(total_rows as u64);
        pb4.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.cyan} [4/4] Inserting into database... {bar:40.cyan/dim} {pos}/{len} ({eta})")
                .unwrap()
                .progress_chars("█▓░"),
        );

        output::direct::insert_direct(
            &data,
            &schema,
            &db_url,
            Some(&|current, _total| {
                pb4.set_position(current as u64);
            }),
        )
        .await?;

        pb4.finish_with_message(format!(
            "Inserting into database... ✓ ({} rows)",
            total_rows
        ));
        eprintln!(
            "\n✓ Inserted {} rows across {} tables into {}",
            total_rows,
            data.tables.len(),
            schema.database_type,
        );
    } else {
        let pb4 = ProgressBar::new_spinner();
        pb4.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.cyan} [4/4] {msg}")
                .unwrap(),
        );
        pb4.set_prefix("4/4");

        match &args.output {
            Some(path) => {
                pb4.set_message(format!("Writing to {}...", path));
                let file = File::create(path)
                    .with_context(|| format!("Failed to create output file: {}", path))?;
                let mut writer = BufWriter::new(file);

                match args.output_format() {
                    OutputFormat::Sql => {
                        if args.copy && matches!(schema.database_type, DatabaseType::PostgreSQL) {
                            output::sql::write_postgres_copy(&mut writer, &data, &schema)?;
                        } else {
                            output::sql::write_sql(&mut writer, &data, &schema)?;
                        }
                    }
                    OutputFormat::Json => {
                        output::json::write_json(&mut writer, &data)?;
                    }
                    OutputFormat::Csv => {
                        output::csv::write_csv(&mut writer, &data)?;
                    }
                }

                pb4.finish_with_message(format!("Writing to {}... ✓", path));
                eprintln!(
                    "\n✓ Generated {} rows across {} tables → {}",
                    total_rows,
                    data.tables.len(),
                    path
                );
            }
            None => {
                // Write to stdout
                pb4.set_message("Writing to stdout...");
                let stdout = std::io::stdout();
                let mut writer = BufWriter::new(stdout.lock());

                match args.output_format() {
                    OutputFormat::Sql => {
                        output::sql::write_sql(&mut writer, &data, &schema)?;
                    }
                    OutputFormat::Json => {
                        output::json::write_json(&mut writer, &data)?;
                    }
                    OutputFormat::Csv => {
                        output::csv::write_csv(&mut writer, &data)?;
                    }
                }

                pb4.finish_with_message("Writing to stdout... ✓");
            }
        }
    }

    // Write lock file (always, so teammates can reproduce)
    let lock_file = LockFile::new(
        compute_schema_hash(&schema),
        seed,
        plan.base_time,
        LockConfig {
            default_row_count: row_count,
            table_row_overrides,
            ai_enabled: args.ai,
            include_tables: args.include.clone(),
            exclude_tables: args.exclude.clone(),
            ai_classifications: ai_cache.clone(),
            column_overrides: if column_overrides.is_empty() {
                None
            } else {
                Some(
                    column_overrides
                        .iter()
                        .filter(|(_, cfg)| cfg.values.is_some())
                        .map(|(k, cfg)| {
                            (
                                k.clone(),
                                seedkit_core::lock::types::ColumnOverrideLock {
                                    values: cfg.values.clone(),
                                    weights: cfg.weights.clone(),
                                },
                            )
                        })
                        .collect(),
                )
            },
        },
        schema,
    );
    lock::write_lock_file(&lock_file, lock_path)?;
    eprintln!("Lock file written to {}", lock::LOCK_FILE_NAME);

    Ok(())
}

/// Resolve database URL from args, env, .env file, or seedkit.toml.
fn resolve_db_url(
    explicit: Option<&str>,
    config: Option<&seedkit_core::config::SeedKitConfig>,
) -> Result<String> {
    if let Some(url) = explicit {
        return Ok(url.to_string());
    }

    // Try environment variable
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return Ok(url);
    }

    // Try .env file
    if dotenvy::dotenv().is_ok() {
        if let Ok(url) = std::env::var("DATABASE_URL") {
            return Ok(url);
        }
    }

    // Try seedkit.toml
    if let Some(cfg) = config {
        if let Some(ref url) = cfg.database.url {
            return Ok(url.clone());
        }
    }

    Err(seedkit_core::error::SeedKitError::NoDatabaseUrl.into())
}

fn extract_mysql_db_name(url: &str) -> Option<String> {
    // mysql://user:pass@host:port/database
    url.rsplit('/')
        .next()
        .map(|s| s.split('?').next().unwrap_or(s).to_string())
}

/// Enhance rule-based classifications with LLM analysis.
///
/// Sends the schema to the configured LLM provider, caches the response,
/// and merges the results (LLM overrides only for `Unknown` columns).
async fn enhance_with_llm(
    rule_classifications: &HashMap<(String, String), SemanticType>,
    schema: &DatabaseSchema,
    model_override: Option<&str>,
) -> Result<(
    HashMap<(String, String), SemanticType>,
    Option<BTreeMap<String, BTreeMap<String, SemanticType>>>,
)> {
    let provider = llm::client::LlmProvider::from_env(model_override)
        .context("--ai flag requires an LLM API key")?;

    let schema_hash = compute_schema_hash(schema);

    // Check cache first
    let response = if let Some(cached) = llm::client::load_cached_response(&schema_hash) {
        eprintln!("Using cached LLM classification (schema unchanged)");
        cached
    } else {
        eprintln!("Sending schema to LLM for classification...");
        let ddl = llm::prompt::schema_to_compact_ddl(schema);
        let prompt = llm::prompt::classification_prompt(&ddl);
        let resp = provider
            .classify(&prompt)
            .await
            .context("LLM classification request failed")?;
        llm::client::save_cached_response(&schema_hash, &resp);
        resp
    };

    let merged = llm::parse::merge_classifications(rule_classifications, &response);
    let cache = llm::parse::build_ai_classification_cache(rule_classifications, &merged);
    let ai_cache = if cache.is_empty() { None } else { Some(cache) };

    Ok((merged, ai_cache))
}

/// Restore AI classifications from a lock file into the classification map.
#[allow(clippy::type_complexity)]
fn restore_ai_from_lock(
    rule_classifications: &HashMap<(String, String), SemanticType>,
    lock_file: &LockFile,
) -> (
    HashMap<(String, String), SemanticType>,
    Option<BTreeMap<String, BTreeMap<String, SemanticType>>>,
) {
    let mut merged = rule_classifications.clone();

    if let Some(ref ai_cache) = lock_file.config.ai_classifications {
        for (table, columns) in ai_cache {
            for (column, semantic_type) in columns {
                let key = (table.clone(), column.clone());
                // Only apply AI classification if rule engine says Unknown
                if merged.get(&key).copied().unwrap_or(SemanticType::Unknown)
                    == SemanticType::Unknown
                {
                    merged.insert(key, *semantic_type);
                }
            }
        }
    }

    (merged, lock_file.config.ai_classifications.clone())
}
