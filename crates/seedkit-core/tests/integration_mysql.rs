//! Integration tests for SeedKit against a real MySQL database.
//!
//! These tests require a running MySQL instance. Set the
//! `TEST_MYSQL_URL` environment variable to enable them:
//!
//! ```bash
//! docker-compose -f docker/docker-compose.test.yml up -d
//! TEST_MYSQL_URL=mysql://seedkit:seedkit@localhost:3306/seedkit_test cargo test --test integration_mysql
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
use seedkit_core::schema::mysql::MySqlIntrospector;

fn get_mysql_url() -> Option<String> {
    std::env::var("TEST_MYSQL_URL").ok()
}

/// Fixed base time for deterministic tests.
fn fixed_base_time() -> chrono::NaiveDateTime {
    chrono::NaiveDateTime::new(
        chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
        chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    )
}

fn extract_db_name(url: &str) -> String {
    url.rsplit('/')
        .next()
        .map(|s| s.split('?').next().unwrap_or(s).to_string())
        .unwrap_or_else(|| "seedkit_test".to_string())
}

/// Helper: run the full pipeline against a MySQL schema.
async fn run_full_pipeline(
    pool: &sqlx::MySqlPool,
    db_name: &str,
    seed: u64,
    rows: usize,
) -> (
    seedkit_core::schema::types::DatabaseSchema,
    engine::GeneratedData,
    GenerationPlan,
) {
    run_full_pipeline_with_base_time(pool, db_name, seed, rows, None).await
}

/// Helper with explicit base_time for deterministic tests.
async fn run_full_pipeline_with_base_time(
    pool: &sqlx::MySqlPool,
    db_name: &str,
    seed: u64,
    rows: usize,
    base_time: Option<chrono::NaiveDateTime>,
) -> (
    seedkit_core::schema::types::DatabaseSchema,
    engine::GeneratedData,
    GenerationPlan,
) {
    let introspector = MySqlIntrospector::new(pool.clone(), db_name.to_string());
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

/// Helper: drop all tables and recreate from fixture SQL.
/// MySQL fixtures need to be executed statement-by-statement.
async fn setup_schema(pool: &sqlx::MySqlPool, fixture_sql: &str) {
    // Disable FK checks for cleanup
    sqlx::query("SET FOREIGN_KEY_CHECKS = 0")
        .execute(pool)
        .await
        .expect("disable FK checks failed");

    // Get all tables (CAST to CHAR for MySQL 8.4+ VARBINARY compatibility)
    let tables: Vec<(String,)> = sqlx::query_as(
        "SELECT CAST(table_name AS CHAR) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'"
    )
    .fetch_all(pool)
    .await
    .expect("list tables failed");

    // Drop each table
    for (table_name,) in &tables {
        let drop_sql = format!("DROP TABLE IF EXISTS `{}`", table_name);
        sqlx::query(&drop_sql)
            .execute(pool)
            .await
            .expect("drop table failed");
    }

    // Re-enable FK checks
    sqlx::query("SET FOREIGN_KEY_CHECKS = 1")
        .execute(pool)
        .await
        .expect("enable FK checks failed");

    // Execute fixture SQL statement-by-statement. Strip comment lines first.
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
async fn test_mysql_ecommerce_introspect() {
    let url = match get_mysql_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_MYSQL_URL not set");
            return;
        }
    };

    let db_name = extract_db_name(&url);
    let pool = sqlx::MySqlPool::connect(&url)
        .await
        .expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/ecommerce_mysql.sql");
    setup_schema(&pool, fixture).await;

    let introspector = MySqlIntrospector::new(pool.clone(), db_name);
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

    // Verify enum on orders.status
    let orders = &schema.tables["orders"];
    let status_col = &orders.columns["status"];
    assert!(
        status_col.enum_values.is_some(),
        "orders.status should have enum values"
    );

    // Verify unique constraint on users.email
    let users = &schema.tables["users"];
    assert!(
        users
            .unique_constraints
            .iter()
            .any(|uc| uc.columns.contains(&"email".to_string())),
        "users.email should have unique constraint"
    );

    pool.close().await;
}

#[tokio::test]
async fn test_mysql_ecommerce_generate_and_insert() {
    let url = match get_mysql_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_MYSQL_URL not set");
            return;
        }
    };

    let db_name = extract_db_name(&url);
    let pool = sqlx::MySqlPool::connect(&url)
        .await
        .expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/ecommerce_mysql.sql");
    setup_schema(&pool, fixture).await;

    let (schema, data, _plan) = run_full_pipeline(&pool, &db_name, 42, 50).await;

    // Verify all tables have data
    for (table_name, rows) in &data.tables {
        assert!(!rows.is_empty(), "table {} should have rows", table_name);
    }

    // Direct insert
    output::direct::insert_direct(&data, &schema, &url, None)
        .await
        .expect("direct insert failed");

    // Verify rows in DB
    let user_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
        .fetch_one(&pool)
        .await
        .expect("count query failed");
    assert!(user_count.0 > 0, "users table should have rows");

    let order_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM orders")
        .fetch_one(&pool)
        .await
        .expect("count query failed");
    assert!(order_count.0 > 0, "orders table should have rows");

    // Verify FK integrity
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
    assert_eq!(invalid_status.0, 0, "all order statuses should be valid");

    // Verify check constraints — price >= 0
    let negative_price: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM products WHERE price < 0")
        .fetch_one(&pool)
        .await
        .expect("check constraint query failed");
    assert_eq!(
        negative_price.0, 0,
        "no products should have negative price"
    );

    pool.close().await;
}

#[tokio::test]
async fn test_mysql_ecommerce_sql_output() {
    let url = match get_mysql_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_MYSQL_URL not set");
            return;
        }
    };

    let db_name = extract_db_name(&url);
    let pool = sqlx::MySqlPool::connect(&url)
        .await
        .expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/ecommerce_mysql.sql");
    setup_schema(&pool, fixture).await;

    let (schema, data, _plan) = run_full_pipeline(&pool, &db_name, 42, 20).await;

    // Write SQL output
    let mut buf = Vec::new();
    output::sql::write_sql(&mut buf, &data, &schema).expect("write_sql failed");
    let sql_output = String::from_utf8(buf).expect("invalid utf8");

    assert!(
        sql_output.contains("INSERT INTO"),
        "should contain INSERT statements"
    );

    // MySQL should use backtick quoting
    assert!(
        sql_output.contains('`'),
        "MySQL SQL output should use backtick quoting"
    );

    pool.close().await;
}

#[tokio::test]
async fn test_mysql_ecommerce_deterministic_with_seed() {
    let url = match get_mysql_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_MYSQL_URL not set");
            return;
        }
    };

    let db_name = extract_db_name(&url);
    let pool = sqlx::MySqlPool::connect(&url)
        .await
        .expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/ecommerce_mysql.sql");
    setup_schema(&pool, fixture).await;

    let bt = Some(fixed_base_time());
    let (_schema1, data1, _plan1) =
        run_full_pipeline_with_base_time(&pool, &db_name, 12345, 30, bt).await;
    let (_schema2, data2, _plan2) =
        run_full_pipeline_with_base_time(&pool, &db_name, 12345, 30, bt).await;

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
async fn test_mysql_circular_fk_handling() {
    let url = match get_mysql_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_MYSQL_URL not set");
            return;
        }
    };

    let db_name = extract_db_name(&url);
    let pool = sqlx::MySqlPool::connect(&url)
        .await
        .expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/circular_mysql.sql");
    setup_schema(&pool, fixture).await;

    let (schema, data, _plan) = run_full_pipeline(&pool, &db_name, 42, 30).await;

    assert!(
        data.tables.contains_key("employees"),
        "should have employees"
    );
    assert!(
        data.tables.contains_key("departments"),
        "should have departments"
    );

    // Verify row counts
    assert_eq!(
        data.tables["employees"].len(),
        30,
        "employees should have 30 rows"
    );
    assert_eq!(
        data.tables["departments"].len(),
        30,
        "departments should have 30 rows"
    );

    // Verify SQL output works
    let mut buf = Vec::new();
    output::sql::write_sql(&mut buf, &data, &schema).expect("write_sql failed");
    let sql_output = String::from_utf8(buf).expect("invalid utf8");
    assert!(
        sql_output.contains("employees"),
        "SQL should reference employees"
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Schema drift detection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mysql_schema_drift_detection() {
    let url = match get_mysql_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_MYSQL_URL not set");
            return;
        }
    };

    let db_name = extract_db_name(&url);
    let pool = sqlx::MySqlPool::connect(&url)
        .await
        .expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/ecommerce_mysql.sql");
    setup_schema(&pool, fixture).await;

    let introspector = MySqlIntrospector::new(pool.clone(), db_name.clone());
    let schema_before = introspector.introspect().await.expect("introspect failed");
    let hash_before = compute_schema_hash(&schema_before);

    // No drift
    let report = check_drift(&schema_before, &hash_before, &schema_before);
    assert!(!report.has_drift, "identical schema should not have drift");

    // Alter schema
    sqlx::query("ALTER TABLE users ADD COLUMN phone VARCHAR(20)")
        .execute(&pool)
        .await
        .expect("alter table failed");

    let introspector2 = MySqlIntrospector::new(pool.clone(), db_name);
    let schema_after = introspector2.introspect().await.expect("introspect failed");

    let report = check_drift(&schema_before, &hash_before, &schema_after);
    assert!(report.has_drift, "schema change should be detected");
    assert!(
        report.new_columns.iter().any(|c| c.column == "phone"),
        "should detect new 'phone' column"
    );

    pool.close().await;
}

// ---------------------------------------------------------------------------
// Large dataset stress test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mysql_generate_500_rows() {
    let url = match get_mysql_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping: TEST_MYSQL_URL not set");
            return;
        }
    };

    let db_name = extract_db_name(&url);
    let pool = sqlx::MySqlPool::connect(&url)
        .await
        .expect("connect failed");
    let fixture = include_str!("../../../tests/fixtures/schemas/ecommerce_mysql.sql");
    setup_schema(&pool, fixture).await;

    let (schema, data, _plan) = run_full_pipeline(&pool, &db_name, 99, 500).await;

    for (table_name, rows) in &data.tables {
        assert!(
            !rows.is_empty(),
            "table {} should have rows at 500-row scale",
            table_name,
        );
    }

    output::direct::insert_direct(&data, &schema, &url, None)
        .await
        .expect("500-row direct insert failed");

    let total: (i64,) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM users) + (SELECT COUNT(*) FROM orders) + (SELECT COUNT(*) FROM products)"
    )
    .fetch_one(&pool)
    .await
    .expect("count query failed");
    assert!(total.0 > 0, "should have rows after 500-row insert");

    pool.close().await;
}
