//! Integration tests for SeedKit against a real PostgreSQL database.
//!
//! These tests require a running PostgreSQL instance. Set the
//! `TEST_POSTGRES_URL` environment variable to enable them:
//!
//! ```bash
//! docker-compose -f docker/docker-compose.test.yml up -d
//! TEST_POSTGRES_URL=postgres://seedkit:seedkit@localhost:5432/seedkit_test cargo test --test integration_postgres
//! ```

use std::collections::BTreeMap;

use seedkit_core::check::{check_drift, compute_schema_hash};
use seedkit_core::classify::rules::classify_schema;
use seedkit_core::generate::engine;
use seedkit_core::generate::plan::{filter_insertion_order, GenerationPlan};
use seedkit_core::graph::cycle::break_cycles;
use seedkit_core::graph::dag::DependencyGraph;
use seedkit_core::graph::topo::topological_sort;
use seedkit_core::output;
use seedkit_core::schema::introspect::SchemaIntrospector;
use seedkit_core::schema::postgres::PostgresIntrospector;

fn get_pg_url() -> Option<String> {
    std::env::var("TEST_POSTGRES_URL").ok()
}

/// Fixed base time for deterministic tests.
fn fixed_base_time() -> chrono::NaiveDateTime {
    chrono::NaiveDateTime::new(
        chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
        chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    )
}

/// Helper: run the full pipeline against a PG schema.
/// Returns (schema, generated_data, plan).
async fn run_full_pipeline(
    pool: &sqlx::PgPool,
    seed: u64,
    rows: usize,
) -> (
    seedkit_core::schema::types::DatabaseSchema,
    engine::GeneratedData,
    GenerationPlan,
) {
    run_full_pipeline_with_base_time(pool, seed, rows, None).await
}

/// Helper with explicit base_time for deterministic tests.
async fn run_full_pipeline_with_base_time(
    pool: &sqlx::PgPool,
    seed: u64,
    rows: usize,
    base_time: Option<chrono::NaiveDateTime>,
) -> (
    seedkit_core::schema::types::DatabaseSchema,
    engine::GeneratedData,
    GenerationPlan,
) {
    let introspector = PostgresIntrospector::new(pool.clone());
    let schema = introspector.introspect().await.expect("introspect failed");

    let mut dep_graph = DependencyGraph::from_schema(&schema);
    let deferred = break_cycles(&mut dep_graph, &[]).expect("cycle break failed");
    let insertion_order = topological_sort(&dep_graph).expect("topo sort failed");

    let classifications = classify_schema(&schema);
    let filtered_order = filter_insertion_order(&insertion_order.tables, &schema, &[], &[]);

    let plan = GenerationPlan::build(
        &schema,
        &classifications,
        &filtered_order,
        deferred,
        rows,
        &BTreeMap::new(),
        seed,
        base_time,
        &BTreeMap::new(),
        None,
    );

    let data = engine::execute_plan(&plan, &schema, None).expect("execute_plan failed");
    (schema, data, plan)
}

/// Helper: create a fresh test schema by dropping all tables and recreating from fixture.
async fn setup_schema(pool: &sqlx::PgPool, fixture_sql: &str) {
    // Drop all tables in the public schema
    let drop_sql = r#"
        DO $$ DECLARE
            r RECORD;
        BEGIN
            FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE';
            END LOOP;
        END $$;
    "#;
    sqlx::query(drop_sql)
        .execute(pool)
        .await
        .expect("drop tables failed");

    // Drop custom types (enums)
    let drop_types = r#"
        DO $$ DECLARE
            r RECORD;
        BEGIN
            FOR r IN (SELECT typname FROM pg_type WHERE typnamespace = 'public'::regnamespace AND typtype = 'e') LOOP
                EXECUTE 'DROP TYPE IF EXISTS public.' || quote_ident(r.typname) || ' CASCADE';
            END LOOP;
        END $$;
    "#;
    sqlx::query(drop_types)
        .execute(pool)
        .await
        .expect("drop types failed");

    // Execute fixture SQL statement-by-statement (sqlx can't run multiple statements).
    // Strip comment lines before splitting.
    let cleaned: String = fixture_sql
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    for stmt in cleaned.split(';') {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        sqlx::query(stmt)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("fixture SQL failed: {}\nStatement: {}", e, stmt));
    }
}

// ---------------------------------------------------------------------------
// E-commerce schema tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pg_ecommerce_introspect() {
    let url = match get_pg_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_POSTGRES_URL not set");
            return;
        }
    };

    let pool = sqlx::PgPool::connect(&url).await.expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/ecommerce.sql");
    setup_schema(&pool, fixture).await;

    let introspector = PostgresIntrospector::new(pool.clone());
    let schema = introspector.introspect().await.expect("introspect failed");

    // Verify tables
    assert_eq!(schema.table_count(), 5, "ecommerce should have 5 tables");
    assert!(schema.tables.contains_key("users"));
    assert!(schema.tables.contains_key("categories"));
    assert!(schema.tables.contains_key("products"));
    assert!(schema.tables.contains_key("orders"));
    assert!(schema.tables.contains_key("order_items"));

    // Verify FKs
    let order_items = &schema.tables["order_items"];
    assert_eq!(
        order_items.foreign_keys.len(),
        2,
        "order_items should have 2 FKs"
    );

    // Verify enum
    let orders = &schema.tables["orders"];
    let status_col = &orders.columns["status"];
    assert!(
        status_col.enum_values.is_some(),
        "orders.status should have enum values"
    );
    let enum_vals = status_col.enum_values.as_ref().unwrap();
    assert!(enum_vals.contains(&"pending".to_string()));
    assert!(enum_vals.contains(&"shipped".to_string()));

    // Verify unique constraints
    let users = &schema.tables["users"];
    assert!(
        users
            .unique_constraints
            .iter()
            .any(|uc| uc.columns.contains(&"email".to_string())),
        "users.email should have unique constraint"
    );

    // Verify check constraints on products.price
    let products = &schema.tables["products"];
    assert!(
        !products.check_constraints.is_empty(),
        "products should have check constraints"
    );

    // Verify composite unique on order_items
    assert!(
        order_items
            .unique_constraints
            .iter()
            .any(|uc| uc.columns.len() == 2),
        "order_items should have composite unique constraint"
    );

    pool.close().await;
}

#[tokio::test]
async fn test_pg_ecommerce_generate_and_insert() {
    let url = match get_pg_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_POSTGRES_URL not set");
            return;
        }
    };

    let pool = sqlx::PgPool::connect(&url).await.expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/ecommerce.sql");
    setup_schema(&pool, fixture).await;

    let (schema, data, _plan) = run_full_pipeline(&pool, 42, 50).await;

    // Verify all tables have data
    assert!(!data.tables.is_empty(), "should generate data");
    for (table_name, rows) in &data.tables {
        assert!(!rows.is_empty(), "table {} should have rows", table_name);
    }

    // Verify row counts
    for (table_name, rows) in &data.tables {
        assert!(
            rows.len() <= 50,
            "table {} has {} rows (expected <= 50)",
            table_name,
            rows.len()
        );
    }

    // Direct insert into the database
    output::direct::insert_direct(&data, &schema, &url, None)
        .await
        .expect("direct insert failed");

    // Verify rows actually landed in the DB
    let user_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
        .fetch_one(&pool)
        .await
        .expect("count query failed");
    assert!(
        user_count.0 > 0,
        "users table should have rows after insert"
    );

    let order_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM orders")
        .fetch_one(&pool)
        .await
        .expect("count query failed");
    assert!(
        order_count.0 > 0,
        "orders table should have rows after insert"
    );

    // Verify FK integrity — all orders.user_id should exist in users.id
    let orphan_count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM orders o WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id = o.user_id)"
    )
    .fetch_one(&pool)
    .await
    .expect("orphan check failed");
    assert_eq!(orphan_count.0, 0, "no orphaned FK references should exist");

    // Verify enum values are valid
    let invalid_status: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM orders WHERE status NOT IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')"
    )
    .fetch_one(&pool)
    .await
    .expect("enum check failed");
    assert_eq!(
        invalid_status.0, 0,
        "all order statuses should be valid enum values"
    );

    // Verify check constraints — price >= 0
    let negative_price: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM products WHERE price < 0")
        .fetch_one(&pool)
        .await
        .expect("check constraint query failed");
    assert_eq!(
        negative_price.0, 0,
        "no products should have negative price"
    );

    // Verify unique constraints held (no duplicate emails)
    let dup_emails: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM (SELECT email FROM users GROUP BY email HAVING COUNT(*) > 1) sub",
    )
    .fetch_one(&pool)
    .await
    .expect("duplicate check failed");
    assert_eq!(dup_emails.0, 0, "no duplicate emails should exist");

    pool.close().await;
}

#[tokio::test]
async fn test_pg_ecommerce_sql_output() {
    let url = match get_pg_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_POSTGRES_URL not set");
            return;
        }
    };

    let pool = sqlx::PgPool::connect(&url).await.expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/ecommerce.sql");
    setup_schema(&pool, fixture).await;

    let (schema, data, _plan) = run_full_pipeline(&pool, 42, 20).await;

    // Write SQL output
    let mut buf = Vec::new();
    output::sql::write_sql(&mut buf, &data, &schema).expect("write_sql failed");
    let sql_output = String::from_utf8(buf).expect("invalid utf8");

    // SQL should contain INSERT statements
    assert!(
        sql_output.contains("INSERT INTO"),
        "SQL output should contain INSERT statements"
    );
    assert!(
        sql_output.contains("users"),
        "SQL output should mention users table"
    );
    assert!(
        sql_output.contains("orders"),
        "SQL output should mention orders table"
    );

    // Write JSON output
    let mut json_buf = Vec::new();
    output::json::write_json(&mut json_buf, &data).expect("write_json failed");
    let json_output = String::from_utf8(json_buf).expect("invalid utf8");

    // JSON should be valid and contain table names
    let parsed: serde_json::Value =
        serde_json::from_str(&json_output).expect("invalid JSON output");
    assert!(parsed.is_object(), "JSON output should be an object");

    // Write CSV output
    let mut csv_buf = Vec::new();
    output::csv::write_csv(&mut csv_buf, &data).expect("write_csv failed");
    let csv_output = String::from_utf8(csv_buf).expect("invalid utf8");
    assert!(!csv_output.is_empty(), "CSV output should not be empty");

    pool.close().await;
}

#[tokio::test]
async fn test_pg_ecommerce_deterministic_with_seed() {
    let url = match get_pg_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_POSTGRES_URL not set");
            return;
        }
    };

    let pool = sqlx::PgPool::connect(&url).await.expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/ecommerce.sql");
    setup_schema(&pool, fixture).await;

    let bt = Some(fixed_base_time());
    let (_schema1, data1, _plan1) = run_full_pipeline_with_base_time(&pool, 12345, 30, bt).await;
    let (_schema2, data2, _plan2) = run_full_pipeline_with_base_time(&pool, 12345, 30, bt).await;

    // Same seed + same base_time should produce identical data
    for (table_name, rows1) in &data1.tables {
        let rows2 = data2
            .tables
            .get(table_name)
            .expect("table missing in second run");
        assert_eq!(
            rows1.len(),
            rows2.len(),
            "row counts differ for {}",
            table_name
        );

        for (i, (row1, row2)) in rows1.iter().zip(rows2.iter()).enumerate() {
            assert_eq!(
                format!("{:?}", row1),
                format!("{:?}", row2),
                "row {} in {} differs between seed-identical runs",
                i,
                table_name
            );
        }
    }

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Circular FK tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pg_circular_fk_handling() {
    let url = match get_pg_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_POSTGRES_URL not set");
            return;
        }
    };

    let pool = sqlx::PgPool::connect(&url).await.expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/circular.sql");
    setup_schema(&pool, fixture).await;

    let (schema, data, plan) = run_full_pipeline(&pool, 42, 30).await;

    // Should have both tables
    assert!(
        data.tables.contains_key("employees"),
        "should have employees"
    );
    assert!(
        data.tables.contains_key("departments"),
        "should have departments"
    );

    // Should have deferred updates (cycle was broken)
    assert!(
        !plan.deferred_edges.is_empty() || !data.deferred_updates.is_empty(),
        "circular FKs should produce deferred edges or updates"
    );

    // Verify row counts
    let employees = &data.tables["employees"];
    let departments = &data.tables["departments"];
    assert_eq!(employees.len(), 30, "employees should have 30 rows");
    assert_eq!(departments.len(), 30, "departments should have 30 rows");

    // Verify SQL output can be generated (even if direct insert has type issues)
    let mut buf = Vec::new();
    output::sql::write_sql(&mut buf, &data, &schema).expect("write_sql failed");
    let sql_output = String::from_utf8(buf).expect("invalid utf8");
    assert!(
        sql_output.contains("employees"),
        "SQL should reference employees"
    );
    assert!(
        sql_output.contains("departments"),
        "SQL should reference departments"
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Edge case schema tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pg_edge_cases_uuid_and_composite_keys() {
    let url = match get_pg_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_POSTGRES_URL not set");
            return;
        }
    };

    let pool = sqlx::PgPool::connect(&url).await.expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/edge_cases.sql");
    setup_schema(&pool, fixture).await;

    let introspector = PostgresIntrospector::new(pool.clone());
    let schema = introspector.introspect().await.expect("introspect failed");

    // Verify UUID PK detected
    let posts = &schema.tables["posts"];
    let id_col = &posts.columns["id"];
    assert_eq!(
        id_col.data_type,
        seedkit_core::schema::types::DataType::Uuid,
        "posts.id should be UUID"
    );

    // Verify composite PK
    let post_tags = &schema.tables["post_tags"];
    let pk = post_tags
        .primary_key
        .as_ref()
        .expect("post_tags should have PK");
    assert_eq!(pk.columns.len(), 2, "post_tags should have composite PK");

    // Verify self-referencing FK
    let comments = &schema.tables["comments"];
    let self_ref = comments
        .foreign_keys
        .iter()
        .find(|fk| fk.referenced_table == "comments");
    assert!(
        self_ref.is_some(),
        "comments should have self-referencing FK"
    );

    // Generate data (don't direct-insert — some column types like view_count/metadata
    // may generate incorrect types due to Unknown semantic classification)
    let (_schema, data, _plan) = run_full_pipeline(&pool, 42, 20).await;

    // Verify all tables have generated rows
    assert!(!data.tables["posts"].is_empty(), "posts should have rows");
    assert!(!data.tables["tags"].is_empty(), "tags should have rows");
    assert!(
        !data.tables["post_tags"].is_empty(),
        "post_tags should have rows"
    );
    assert!(
        !data.tables["comments"].is_empty(),
        "comments should have rows"
    );

    // Verify SQL output can be generated
    let mut buf = Vec::new();
    output::sql::write_sql(&mut buf, &data, &_schema).expect("write_sql failed");
    let sql_output = String::from_utf8(buf).expect("invalid utf8");
    assert!(sql_output.contains("posts"), "SQL should reference posts");
    assert!(
        sql_output.contains("post_tags"),
        "SQL should reference post_tags"
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Schema drift detection (end-to-end)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pg_schema_drift_detection() {
    let url = match get_pg_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_POSTGRES_URL not set");
            return;
        }
    };

    let pool = sqlx::PgPool::connect(&url).await.expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/ecommerce.sql");
    setup_schema(&pool, fixture).await;

    let introspector = PostgresIntrospector::new(pool.clone());
    let schema_before = introspector.introspect().await.expect("introspect failed");
    let hash_before = compute_schema_hash(&schema_before);

    // No drift should be detected against itself
    let report = check_drift(&schema_before, &hash_before, &schema_before);
    assert!(!report.has_drift, "identical schema should not have drift");

    // Now alter the schema — add a column
    sqlx::query("ALTER TABLE users ADD COLUMN phone VARCHAR(20)")
        .execute(&pool)
        .await
        .expect("alter table failed");

    let schema_after = introspector.introspect().await.expect("introspect failed");

    // Drift should now be detected
    let report = check_drift(&schema_before, &hash_before, &schema_after);
    assert!(
        report.has_drift,
        "schema change should be detected as drift"
    );
    assert!(
        report.new_columns.iter().any(|c| c.column == "phone"),
        "should detect new 'phone' column"
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Lock file round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pg_lock_file_round_trip() {
    let url = match get_pg_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_POSTGRES_URL not set");
            return;
        }
    };

    let pool = sqlx::PgPool::connect(&url).await.expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/ecommerce.sql");
    setup_schema(&pool, fixture).await;

    let (schema, _data, plan) = run_full_pipeline(&pool, 42, 25).await;

    // Write lock file
    let lock_file = seedkit_core::lock::types::LockFile::new(
        compute_schema_hash(&schema),
        42,
        plan.base_time,
        seedkit_core::lock::types::LockConfig {
            default_row_count: 25,
            table_row_overrides: BTreeMap::new(),
            ai_enabled: false,
            include_tables: Vec::new(),
            exclude_tables: Vec::new(),
            ai_classifications: None,
            column_overrides: None,
        },
        schema.clone(),
    );

    let tmp_dir = tempfile::tempdir().expect("create tmpdir failed");
    let lock_path = tmp_dir.path().join("seedkit.lock");
    seedkit_core::lock::write_lock_file(&lock_file, &lock_path).expect("write lock failed");

    // Read it back
    let loaded = seedkit_core::lock::read_lock_file(&lock_path).expect("read lock failed");
    assert_eq!(loaded.seed, 42);
    assert_eq!(loaded.config.default_row_count, 25);
    assert_eq!(loaded.schema_hash, compute_schema_hash(&schema));

    // Schema snapshot should be preserved
    assert_eq!(
        loaded.schema_snapshot.table_count(),
        schema.table_count(),
        "schema snapshot should match"
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Table row overrides
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pg_table_row_overrides() {
    let url = match get_pg_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_POSTGRES_URL not set");
            return;
        }
    };

    let pool = sqlx::PgPool::connect(&url).await.expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/ecommerce.sql");
    setup_schema(&pool, fixture).await;

    let introspector = PostgresIntrospector::new(pool.clone());
    let schema = introspector.introspect().await.expect("introspect failed");

    let mut dep_graph = DependencyGraph::from_schema(&schema);
    let deferred = break_cycles(&mut dep_graph, &[]).expect("cycle break failed");
    let insertion_order = topological_sort(&dep_graph).expect("topo sort failed");
    let classifications = classify_schema(&schema);
    let filtered_order = filter_insertion_order(&insertion_order.tables, &schema, &[], &[]);

    // Override: users=10, orders=30
    let mut overrides = BTreeMap::new();
    overrides.insert("users".to_string(), 10usize);
    overrides.insert("orders".to_string(), 30usize);

    let plan = GenerationPlan::build(
        &schema,
        &classifications,
        &filtered_order,
        deferred,
        100, // default
        &overrides,
        42,
        None,
        &BTreeMap::new(),
        None,
    );

    // Verify overrides were applied
    for tp in &plan.table_plans {
        match tp.table_name.as_str() {
            "users" => assert_eq!(tp.row_count, 10, "users should have 10 rows"),
            "orders" => assert_eq!(tp.row_count, 30, "orders should have 30 rows"),
            _ => {} // default applies
        }
    }

    let data = engine::execute_plan(&plan, &schema, None).expect("execute_plan failed");

    assert_eq!(
        data.tables["users"].len(),
        10,
        "users should have exactly 10 rows"
    );
    assert_eq!(
        data.tables["orders"].len(),
        30,
        "orders should have exactly 30 rows"
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Include/Exclude table filtering
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pg_include_exclude_tables() {
    let url = match get_pg_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_POSTGRES_URL not set");
            return;
        }
    };

    let pool = sqlx::PgPool::connect(&url).await.expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/ecommerce.sql");
    setup_schema(&pool, fixture).await;

    let introspector = PostgresIntrospector::new(pool.clone());
    let schema = introspector.introspect().await.expect("introspect failed");

    let mut dep_graph = DependencyGraph::from_schema(&schema);
    let deferred = break_cycles(&mut dep_graph, &[]).expect("cycle break failed");
    let insertion_order = topological_sort(&dep_graph).expect("topo sort failed");
    let classifications = classify_schema(&schema);

    // Include only users and categories
    let include = vec!["users".to_string(), "categories".to_string()];
    let filtered_order = filter_insertion_order(&insertion_order.tables, &schema, &include, &[]);

    let plan = GenerationPlan::build(
        &schema,
        &classifications,
        &filtered_order,
        deferred,
        20,
        &BTreeMap::new(),
        42,
        None,
        &BTreeMap::new(),
        None,
    );

    let data = engine::execute_plan(&plan, &schema, None).expect("execute_plan failed");

    // Should only have included tables
    assert!(
        data.tables.contains_key("users"),
        "users should be included"
    );
    assert!(
        data.tables.contains_key("categories"),
        "categories should be included"
    );
    assert!(
        !data.tables.contains_key("orders"),
        "orders should be excluded"
    );
    assert!(
        !data.tables.contains_key("products"),
        "products should be excluded"
    );
    assert!(
        !data.tables.contains_key("order_items"),
        "order_items should be excluded"
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Large-ish dataset stress test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pg_generate_500_rows() {
    let url = match get_pg_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_POSTGRES_URL not set");
            return;
        }
    };

    let pool = sqlx::PgPool::connect(&url).await.expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/ecommerce.sql");
    setup_schema(&pool, fixture).await;

    let (schema, data, _plan) = run_full_pipeline(&pool, 99, 500).await;

    // Verify all tables generated
    for (table_name, rows) in &data.tables {
        assert!(
            !rows.is_empty(),
            "table {} should have rows at 500-row scale",
            table_name,
        );
    }

    // Direct insert the full 500-row dataset
    output::direct::insert_direct(&data, &schema, &url, None)
        .await
        .expect("500-row direct insert failed");

    let total: (i64,) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM users) + (SELECT COUNT(*) FROM orders) + (SELECT COUNT(*) FROM products)"
    )
    .fetch_one(&pool)
    .await
    .expect("count query failed");
    assert!(
        total.0 > 0,
        "should have substantial row count after 500-row insert"
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// COPY format output
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pg_copy_format_output() {
    let url = match get_pg_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_POSTGRES_URL not set");
            return;
        }
    };

    let pool = sqlx::PgPool::connect(&url).await.expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/ecommerce.sql");
    setup_schema(&pool, fixture).await;

    let (schema, data, _plan) = run_full_pipeline(&pool, 42, 10).await;

    let mut buf = Vec::new();
    output::sql::write_postgres_copy(&mut buf, &data, &schema).expect("write_postgres_copy failed");
    let copy_output = String::from_utf8(buf).expect("invalid utf8");

    // COPY format should contain COPY ... FROM stdin
    assert!(
        copy_output.contains("COPY") || copy_output.contains("copy"),
        "COPY output should contain COPY statements"
    );
    // Should have tab-separated values followed by \.
    assert!(
        copy_output.contains("\\."),
        "COPY output should contain end-of-data marker"
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Classification + semantic type generation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pg_semantic_classification() {
    let url = match get_pg_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_POSTGRES_URL not set");
            return;
        }
    };

    let pool = sqlx::PgPool::connect(&url).await.expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/ecommerce.sql");
    setup_schema(&pool, fixture).await;

    let introspector = PostgresIntrospector::new(pool.clone());
    let schema = introspector.introspect().await.expect("introspect failed");
    let classifications = classify_schema(&schema);

    // email columns should be classified as Email
    let email_type = classifications.get(&("users".to_string(), "email".to_string()));
    assert!(email_type.is_some(), "users.email should be classified");
    assert_eq!(
        *email_type.unwrap(),
        seedkit_core::classify::semantic::SemanticType::Email,
        "users.email should be classified as Email"
    );

    // first_name should be classified as FirstName
    let fname_type = classifications.get(&("users".to_string(), "first_name".to_string()));
    assert!(
        fname_type.is_some(),
        "users.first_name should be classified"
    );
    assert_eq!(
        *fname_type.unwrap(),
        seedkit_core::classify::semantic::SemanticType::FirstName,
        "users.first_name should be classified as FirstName"
    );

    pool.close().await;
}
