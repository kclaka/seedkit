//! Benchmarks for the generation engine — the core hot path.
//!
//! Measures rows-per-second throughput for `execute_plan` across
//! different table sizes, column counts, and strategy mixes.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::collections::{BTreeMap, HashMap};

use seedkit_core::classify::semantic::SemanticType;
use seedkit_core::generate::engine::execute_plan;
use seedkit_core::generate::plan::*;
use seedkit_core::sample::stats::ColumnDistribution;
use seedkit_core::schema::types::*;

/// Build a schema with one table containing N semantic columns (no FKs).
fn single_table_schema(num_columns: usize) -> (DatabaseSchema, HashMap<(String, String), SemanticType>) {
    let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "bench".to_string());
    let mut table = Table::new("items".to_string());
    let mut classifications = HashMap::new();

    let types = [
        ("email", DataType::VarChar, SemanticType::Email),
        ("first_name", DataType::VarChar, SemanticType::FirstName),
        ("last_name", DataType::VarChar, SemanticType::LastName),
        ("price", DataType::Numeric, SemanticType::Price),
        ("created_at", DataType::TimestampTz, SemanticType::CreatedAt),
        ("is_active", DataType::Boolean, SemanticType::BooleanFlag),
        ("description", DataType::Text, SemanticType::Description),
        ("status", DataType::VarChar, SemanticType::Status),
        ("quantity", DataType::Integer, SemanticType::Quantity),
        ("url", DataType::VarChar, SemanticType::Url),
    ];

    for i in 0..num_columns {
        let (name, dt, st) = &types[i % types.len()];
        let col_name = if i < types.len() {
            name.to_string()
        } else {
            format!("{}_{}", name, i / types.len())
        };
        let col = Column::new(col_name.clone(), dt.clone(), dt.to_string());
        table.columns.insert(col_name.clone(), col);
        classifications.insert(("items".to_string(), col_name), *st);
    }

    schema.tables.insert("items".to_string(), table);
    (schema, classifications)
}

/// Build a schema with parent/child FK relationship.
fn fk_schema() -> (DatabaseSchema, HashMap<(String, String), SemanticType>) {
    let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "bench".to_string());
    let mut classifications = HashMap::new();

    // Parent: users
    let mut users = Table::new("users".to_string());
    let mut id_col = Column::new("id".to_string(), DataType::Serial, "serial".to_string());
    id_col.is_auto_increment = true;
    users.columns.insert("id".to_string(), id_col);
    users.primary_key = Some(PrimaryKey {
        columns: vec!["id".to_string()],
        name: None,
    });
    let email_col = Column::new("email".to_string(), DataType::VarChar, "varchar".to_string());
    users.columns.insert("email".to_string(), email_col);
    classifications.insert(("users".to_string(), "id".to_string()), SemanticType::AutoIncrement);
    classifications.insert(("users".to_string(), "email".to_string()), SemanticType::Email);

    // Child: orders
    let mut orders = Table::new("orders".to_string());
    let mut order_id = Column::new("id".to_string(), DataType::Serial, "serial".to_string());
    order_id.is_auto_increment = true;
    orders.columns.insert("id".to_string(), order_id);
    orders.primary_key = Some(PrimaryKey {
        columns: vec!["id".to_string()],
        name: None,
    });
    let user_id_col = Column::new("user_id".to_string(), DataType::Integer, "integer".to_string());
    orders.columns.insert("user_id".to_string(), user_id_col);
    orders.foreign_keys.push(ForeignKey {
        name: Some("orders_user_id_fkey".to_string()),
        source_columns: vec!["user_id".to_string()],
        referenced_table: "users".to_string(),
        referenced_columns: vec!["id".to_string()],
        on_delete: ForeignKeyAction::Cascade,
        on_update: ForeignKeyAction::NoAction,
        is_deferrable: false,
    });
    let amount_col = Column::new("amount".to_string(), DataType::Numeric, "numeric".to_string());
    orders.columns.insert("amount".to_string(), amount_col);

    classifications.insert(("orders".to_string(), "id".to_string()), SemanticType::AutoIncrement);
    classifications.insert(("orders".to_string(), "user_id".to_string()), SemanticType::ExternalId);
    classifications.insert(("orders".to_string(), "amount".to_string()), SemanticType::Price);

    schema.tables.insert("users".to_string(), users);
    schema.tables.insert("orders".to_string(), orders);
    (schema, classifications)
}

fn bench_single_table_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/single_table");

    let (schema, classifications) = single_table_schema(10);
    let insertion_order = vec!["items".to_string()];
    let empty_overrides = BTreeMap::new();
    let empty_col_overrides = BTreeMap::new();

    for row_count in [100, 1000, 10_000] {
        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(
            BenchmarkId::new("rows", row_count),
            &row_count,
            |b, &rows| {
                let plan = GenerationPlan::build(
                    &schema,
                    &classifications,
                    &insertion_order,
                    Vec::new(),
                    rows,
                    &empty_overrides,
                    42,
                    None,
                    &empty_col_overrides,
                    None,
                );
                b.iter(|| {
                    execute_plan(&plan, &schema, None).unwrap();
                });
            },
        );
    }
    group.finish();
}

fn bench_column_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/column_count");
    let row_count = 1000;
    let empty_overrides = BTreeMap::new();
    let empty_col_overrides = BTreeMap::new();

    for col_count in [5, 10, 20] {
        let (schema, classifications) = single_table_schema(col_count);
        let insertion_order = vec!["items".to_string()];

        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(
            BenchmarkId::new("cols", col_count),
            &col_count,
            |b, _| {
                let plan = GenerationPlan::build(
                    &schema,
                    &classifications,
                    &insertion_order,
                    Vec::new(),
                    row_count,
                    &empty_overrides,
                    42,
                    None,
                    &empty_col_overrides,
                    None,
                );
                b.iter(|| {
                    execute_plan(&plan, &schema, None).unwrap();
                });
            },
        );
    }
    group.finish();
}

fn bench_fk_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/foreign_keys");
    let (schema, classifications) = fk_schema();
    let insertion_order = vec!["users".to_string(), "orders".to_string()];
    let empty_col_overrides = BTreeMap::new();

    // Parent:child ratios — 100 users + varying order counts
    for order_count in [500, 2000, 10_000] {
        let mut overrides = BTreeMap::new();
        overrides.insert("users".to_string(), 100);
        overrides.insert("orders".to_string(), order_count);
        let total = 100 + order_count;

        group.throughput(Throughput::Elements(total as u64));
        group.bench_with_input(
            BenchmarkId::new("orders", order_count),
            &order_count,
            |b, _| {
                let plan = GenerationPlan::build(
                    &schema,
                    &classifications,
                    &insertion_order,
                    Vec::new(),
                    100,
                    &overrides,
                    42,
                    None,
                    &empty_col_overrides,
                    None,
                );
                b.iter(|| {
                    execute_plan(&plan, &schema, None).unwrap();
                });
            },
        );
    }
    group.finish();
}

fn bench_value_list_strategy(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/value_list");

    let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "bench".to_string());
    let mut table = Table::new("items".to_string());
    let col = Column::new("color".to_string(), DataType::VarChar, "varchar".to_string());
    table.columns.insert("color".to_string(), col);
    schema.tables.insert("items".to_string(), table);

    let plan = GenerationPlan {
        table_plans: vec![TableGenerationPlan {
            table_name: "items".to_string(),
            row_count: 10_000,
            column_plans: vec![ColumnGenerationPlan {
                column_name: "color".to_string(),
                semantic_type: SemanticType::Unknown,
                strategy: GenerationStrategy::ValueList {
                    values: vec![
                        "red".into(), "blue".into(), "green".into(),
                        "black".into(), "white".into(),
                    ],
                    weights: Some(vec![0.25, 0.20, 0.20, 0.20, 0.15]),
                },
                nullable: false,
                null_probability: 0.0,
                check_constraints: Vec::new(),
            }],
            correlation_groups: Vec::new(),
        }],
        deferred_edges: Vec::new(),
        seed: 42,
        default_row_count: 10_000,
        base_time: chrono::Utc::now().naive_utc(),
        sequence_offset: 0,
    };

    group.throughput(Throughput::Elements(10_000));
    group.bench_function("weighted_10k", |b| {
        b.iter(|| {
            execute_plan(&plan, &schema, None).unwrap();
        });
    });
    group.finish();
}

fn bench_distribution_strategy(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/distribution");

    let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "bench".to_string());
    let mut table = Table::new("items".to_string());
    let col = Column::new("price".to_string(), DataType::Numeric, "numeric".to_string());
    table.columns.insert("price".to_string(), col);
    schema.tables.insert("items".to_string(), table);

    let plan = GenerationPlan {
        table_plans: vec![TableGenerationPlan {
            table_name: "items".to_string(),
            row_count: 10_000,
            column_plans: vec![ColumnGenerationPlan {
                column_name: "price".to_string(),
                semantic_type: SemanticType::Unknown,
                strategy: GenerationStrategy::Distribution {
                    distribution: ColumnDistribution::Numeric {
                        min: 0.0,
                        max: 1000.0,
                        mean: 49.99,
                        stddev: 25.0,
                    },
                },
                nullable: false,
                null_probability: 0.0,
                check_constraints: Vec::new(),
            }],
            correlation_groups: Vec::new(),
        }],
        deferred_edges: Vec::new(),
        seed: 42,
        default_row_count: 10_000,
        base_time: chrono::Utc::now().naive_utc(),
        sequence_offset: 0,
    };

    group.throughput(Throughput::Elements(10_000));
    group.bench_function("numeric_normal_10k", |b| {
        b.iter(|| {
            execute_plan(&plan, &schema, None).unwrap();
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_single_table_generation,
    bench_column_count,
    bench_fk_generation,
    bench_value_list_strategy,
    bench_distribution_strategy,
);
criterion_main!(benches);
