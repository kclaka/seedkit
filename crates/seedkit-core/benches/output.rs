//! Benchmarks for output formatters — SQL, JSON, and CSV serialization.
//!
//! Measures throughput of formatting pre-generated data into various output
//! formats. Uses a black-hole writer to isolate formatter cost from I/O.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use indexmap::IndexMap;
use std::borrow::Cow;
use std::io::Write;

use seedkit_core::generate::engine::GeneratedData;
use seedkit_core::generate::value::Value;
use seedkit_core::output::{csv, json, sql};
use seedkit_core::schema::types::*;

/// A writer that discards all output — isolates formatter cost from I/O.
struct NullWriter;

impl Write for NullWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Build pre-generated data with N rows of realistic column types.
fn make_generated_data(row_count: usize) -> GeneratedData {
    let mut rows = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let mut row = IndexMap::new();
        row.insert(
            "name".to_string(),
            Value::String(Cow::Owned(format!("User {}", i))),
        );
        row.insert(
            "email".to_string(),
            Value::String(Cow::Owned(format!("user{}@example.com", i))),
        );
        row.insert("age".to_string(), Value::Int(20 + (i as i64 % 60)));
        row.insert("price".to_string(), Value::Float(9.99 + i as f64 * 0.01));
        row.insert("active".to_string(), Value::Bool(i % 3 != 0));
        row.insert(
            "created_at".to_string(),
            Value::Timestamp(
                chrono::NaiveDateTime::new(
                    chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                    chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
                ) + chrono::Duration::seconds(i as i64),
            ),
        );
        if i % 10 == 0 {
            row.insert("bio".to_string(), Value::Null);
        } else {
            row.insert(
                "bio".to_string(),
                Value::String(Cow::Owned(format!(
                    "A longer description field that contains commas, \"quotes\", and other special characters for row {}.",
                    i
                ))),
            );
        }
        rows.push(row);
    }

    let mut tables = IndexMap::new();
    tables.insert("users".to_string(), rows);

    GeneratedData {
        tables,
        deferred_updates: Vec::new(),
    }
}

fn bench_sql_output(c: &mut Criterion) {
    let mut group = c.benchmark_group("output/sql");
    let schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "bench".to_string());

    for row_count in [100, 1000, 10_000] {
        let data = make_generated_data(row_count);
        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(BenchmarkId::new("rows", row_count), &data, |b, data| {
            b.iter(|| {
                let mut w = NullWriter;
                sql::write_sql(&mut w, data, &schema).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_json_output(c: &mut Criterion) {
    let mut group = c.benchmark_group("output/json");

    for row_count in [100, 1000, 10_000] {
        let data = make_generated_data(row_count);
        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(BenchmarkId::new("rows", row_count), &data, |b, data| {
            b.iter(|| {
                let mut w = NullWriter;
                json::write_json(&mut w, data).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_csv_output(c: &mut Criterion) {
    let mut group = c.benchmark_group("output/csv");

    for row_count in [100, 1000, 10_000] {
        let data = make_generated_data(row_count);
        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(BenchmarkId::new("rows", row_count), &data, |b, data| {
            b.iter(|| {
                let mut w = NullWriter;
                csv::write_csv(&mut w, data).unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_sql_output,
    bench_json_output,
    bench_csv_output
);
criterion_main!(benches);
