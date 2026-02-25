//! Benchmarks for column classification — regex matching and schema-wide classification.
//!
//! Classification runs once per introspection, but regex compilation and matching
//! are worth measuring to catch regressions from rule changes.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use seedkit_core::classify::rules::{classify_column, classify_schema};
use seedkit_core::schema::types::*;

/// Column name/type pairs representing a realistic mix of columns.
fn realistic_columns() -> Vec<(&'static str, DataType)> {
    vec![
        ("id", DataType::Serial),
        ("email", DataType::VarChar),
        ("first_name", DataType::VarChar),
        ("last_name", DataType::VarChar),
        ("password_hash", DataType::VarChar),
        ("created_at", DataType::TimestampTz),
        ("updated_at", DataType::TimestampTz),
        ("is_active", DataType::Boolean),
        ("age", DataType::Integer),
        ("price", DataType::Numeric),
        ("description", DataType::Text),
        ("avatar_url", DataType::VarChar),
        ("phone", DataType::VarChar),
        ("city", DataType::VarChar),
        ("zip_code", DataType::VarChar),
        ("country", DataType::VarChar),
        ("status", DataType::VarChar),
        ("metadata", DataType::Jsonb),
        ("slug", DataType::VarChar),
        ("quantity", DataType::Integer),
    ]
}

fn bench_classify_single_column(c: &mut Criterion) {
    let mut group = c.benchmark_group("classify/single_column");

    // Benchmark individual column classification across different match depths.
    // "email" is an early match, "status" is mid-list, "zzz_unknown" misses all rules.
    let cases = vec![
        ("early_match", "email", DataType::VarChar),
        ("mid_match", "status", DataType::VarChar),
        ("late_match", "tenant_id", DataType::Integer),
        ("no_match", "zzz_unknown", DataType::VarChar),
        ("camel_case", "firstName", DataType::VarChar),
        ("type_constrained", "age", DataType::Integer),
    ];

    for (label, col_name, data_type) in &cases {
        group.bench_with_input(
            BenchmarkId::new("type", label),
            &(col_name, data_type),
            |b, &(name, dt)| {
                b.iter(|| {
                    classify_column(name, dt, "users", false, false, None);
                });
            },
        );
    }
    group.finish();
}

fn bench_classify_schema(c: &mut Criterion) {
    let mut group = c.benchmark_group("classify/schema");
    let columns = realistic_columns();

    for table_count in [10, 50, 100] {
        let schema = build_schema(table_count, &columns);
        let total_columns = table_count * columns.len();

        group.throughput(Throughput::Elements(total_columns as u64));
        group.bench_with_input(
            BenchmarkId::new("tables", table_count),
            &schema,
            |b, schema| {
                b.iter(|| {
                    classify_schema(schema);
                });
            },
        );
    }
    group.finish();
}

/// Build a schema with N tables, each containing the realistic column set.
fn build_schema(table_count: usize, columns: &[(&str, DataType)]) -> DatabaseSchema {
    let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "bench".to_string());

    let table_prefixes = [
        "users",
        "orders",
        "products",
        "reviews",
        "categories",
        "tags",
        "comments",
        "posts",
        "sessions",
        "notifications",
        "invoices",
        "payments",
        "addresses",
        "companies",
        "departments",
        "employees",
        "projects",
        "tasks",
        "events",
        "logs",
    ];

    for i in 0..table_count {
        let table_name = if i < table_prefixes.len() {
            table_prefixes[i].to_string()
        } else {
            format!("table_{}", i)
        };

        let mut table = Table::new(table_name.clone());
        for (col_name, data_type) in columns {
            let mut col = Column::new(
                col_name.to_string(),
                data_type.clone(),
                data_type.to_string(),
            );
            if *col_name == "id" {
                col.is_auto_increment = true;
            }
            table.columns.insert(col_name.to_string(), col);
        }
        table.primary_key = Some(PrimaryKey {
            columns: vec!["id".to_string()],
            name: None,
        });
        schema.tables.insert(table_name, table);
    }

    schema
}

criterion_group!(benches, bench_classify_single_column, bench_classify_schema);
criterion_main!(benches);
