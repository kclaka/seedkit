use indexmap::IndexMap;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use std::borrow::Cow;
use tracing::warn;

use crate::error::{Result, SeedKitError};
use crate::generate::foreign_key::ForeignKeyPool;
use crate::generate::plan::*;
use crate::generate::providers::generate_value;
use crate::generate::unique::UniqueTracker;
use crate::generate::value::Value;
use crate::schema::types::DatabaseSchema;

/// The result of generating data for all tables.
#[derive(Debug)]
pub struct GeneratedData {
    /// Map from table name to generated rows.
    /// Each row is an IndexMap (not HashMap) to preserve column insertion order,
    /// which ensures deterministic JSON/CSV output for lockfile reproducibility.
    pub tables: IndexMap<String, Vec<IndexMap<String, Value>>>,
    /// Deferred FK updates to execute after all inserts.
    pub deferred_updates: Vec<DeferredUpdate>,
}

/// A deferred FK update (for cycle-breaking).
#[derive(Debug)]
pub struct DeferredUpdate {
    pub table_name: String,
    pub row_index: usize,
    pub column_name: String,
    pub value: Value,
}

/// Progress reporting batch size — avoids terminal I/O overhead on every row.
const PROGRESS_BATCH_SIZE: usize = 100;

/// Execute a generation plan and produce all the data.
///
/// **Clean-slate assumption:** For auto-increment primary keys, SeedKit
/// synthesizes sequential IDs starting from `sequence_offset + 1`. This
/// assumes tables are empty (or truncated) before insertion. If seeding into
/// a database with existing rows, either:
/// - Use `--truncate` to reset tables and sequences before inserting, or
/// - Use `direct.rs` with `INSERT ... RETURNING id` to capture real
///   database-assigned IDs (Phase 2 RETURNING pipeline).
///
/// The `sequence_offset` in `GenerationPlan` allows callers to shift the
/// synthesized IDs (e.g., if the sequence starts at 5001 instead of 1).
#[allow(clippy::type_complexity)]
pub fn execute_plan(
    plan: &GenerationPlan,
    schema: &DatabaseSchema,
    progress_callback: Option<&dyn Fn(&str, usize, usize)>,
) -> Result<GeneratedData> {
    let mut rng = StdRng::seed_from_u64(plan.seed);
    let mut fk_pool = ForeignKeyPool::new();
    let mut unique_tracker = UniqueTracker::new();
    let mut generated = GeneratedData {
        tables: IndexMap::new(),
        deferred_updates: Vec::new(),
    };

    // Register unique constraints (both single and composite)
    for table_plan in &plan.table_plans {
        if let Some(table) = schema.tables.get(&table_plan.table_name) {
            for uc in &table.unique_constraints {
                unique_tracker.register_constraint(&table_plan.table_name, &uc.columns);
            }
            // Also register PK as unique
            if let Some(pk) = &table.primary_key {
                unique_tracker.register_constraint(&table_plan.table_name, &pk.columns);
            }
        }
    }

    let total_rows: usize = plan.table_plans.iter().map(|t| t.row_count).sum();
    let mut rows_generated = 0usize;

    for table_plan in &plan.table_plans {
        let mut table_rows = Vec::with_capacity(table_plan.row_count);

        for row_idx in 0..table_plan.row_count {
            let row = generate_row(
                table_plan,
                row_idx,
                &mut rng,
                &fk_pool,
                &mut unique_tracker,
                schema,
                plan.base_time,
            )?;

            // Record PK values into the FK pool so child tables can reference them.
            // For auto-increment PKs that we skip during generation,
            // synthesize sequential IDs offset by `sequence_offset`.
            // See the clean-slate assumption in the doc comment above.
            if let Some(table) = schema.tables.get(&table_plan.table_name) {
                if let Some(pk) = &table.primary_key {
                    for pk_col in &pk.columns {
                        if let Some(value) = row.get(pk_col) {
                            if !value.is_null() {
                                fk_pool.record_value(&table_plan.table_name, pk_col, value.clone());
                            }
                        } else {
                            // Auto-increment PK not in row — synthesize sequential ID
                            let col = table.columns.get(pk_col);
                            let is_auto = col
                                .map(|c| c.is_auto_increment || c.data_type.is_serial())
                                .unwrap_or(false);
                            if is_auto {
                                let id = plan.sequence_offset as i64 + row_idx as i64 + 1;
                                fk_pool.record_value(
                                    &table_plan.table_name,
                                    pk_col,
                                    Value::Int(id),
                                );
                            }
                        }
                    }
                }
            }

            table_rows.push(row);
            rows_generated += 1;

            // Batched progress reporting — only call every N rows to reduce I/O overhead
            if let Some(cb) = progress_callback {
                if rows_generated.is_multiple_of(PROGRESS_BATCH_SIZE)
                    || rows_generated == total_rows
                {
                    cb(&table_plan.table_name, rows_generated, total_rows);
                }
            }
        }

        generated
            .tables
            .insert(table_plan.table_name.clone(), table_rows);
    }

    // Generate deferred FK values — these are UPDATE statements that run
    // after all INSERTs to resolve circular dependencies.
    for deferred in &plan.deferred_edges {
        if let Some(rows) = generated.tables.get(&deferred.source_table) {
            for (row_idx, _row) in rows.iter().enumerate() {
                for (src_col, tgt_col) in deferred
                    .source_columns
                    .iter()
                    .zip(deferred.target_columns.iter())
                {
                    if let Some(value) =
                        fk_pool.pick_reference(&deferred.target_table, tgt_col, &mut rng)
                    {
                        generated.deferred_updates.push(DeferredUpdate {
                            table_name: deferred.source_table.clone(),
                            row_index: row_idx,
                            column_name: src_col.clone(),
                            value,
                        });
                    }
                }
            }
        }
    }

    Ok(generated)
}

/// Maximum attempts to regenerate an entire row when a composite unique
/// constraint collision is detected.
const MAX_ROW_RETRIES: usize = 50;

/// Generate a single row for a table, with full retry for composite unique
/// constraint collisions.
///
/// The outer loop retries the entire row if a composite constraint fires.
/// Single-column unique constraints are retried at the column level (cheaper).
fn generate_row(
    table_plan: &TableGenerationPlan,
    row_index: usize,
    rng: &mut StdRng,
    fk_pool: &ForeignKeyPool,
    unique_tracker: &mut UniqueTracker,
    schema: &DatabaseSchema,
    base_time: chrono::NaiveDateTime,
) -> Result<IndexMap<String, Value>> {
    let composite_constraints: Vec<&Vec<String>> = schema
        .tables
        .get(&table_plan.table_name)
        .map(|t| {
            t.unique_constraints
                .iter()
                .filter(|uc| uc.columns.len() > 1)
                .map(|uc| &uc.columns)
                .collect()
        })
        .unwrap_or_default();

    let mut attempts = 0;
    loop {
        let row = generate_row_candidate(
            table_plan,
            row_index,
            rng,
            fk_pool,
            unique_tracker,
            schema,
            base_time,
        )?;

        // Check all composite unique constraints against the candidate row.
        let mut collision = false;
        for columns in &composite_constraints {
            let values: Vec<&Value> = columns.iter().filter_map(|col| row.get(col)).collect();

            if values.len() == columns.len()
                && !unique_tracker.try_insert(&table_plan.table_name, columns, &values)
            {
                collision = true;
                break;
            }
        }

        if !collision {
            return Ok(row);
        }

        attempts += 1;
        if attempts >= MAX_ROW_RETRIES {
            // Find which constraint failed for the error message
            let failed_cols = composite_constraints
                .iter()
                .map(|cols| cols.join(", "))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(SeedKitError::CompositeUniqueExhausted {
                table: table_plan.table_name.clone(),
                columns: failed_cols,
                row_index,
                max_retries: MAX_ROW_RETRIES,
            });
        }

        warn!(
            "Composite unique collision on {} at row {}, retrying (attempt {}/{})",
            table_plan.table_name, row_index, attempts, MAX_ROW_RETRIES
        );
    }
}

/// Generate a single candidate row (columns only, no composite unique check).
fn generate_row_candidate(
    table_plan: &TableGenerationPlan,
    row_index: usize,
    rng: &mut StdRng,
    fk_pool: &ForeignKeyPool,
    unique_tracker: &mut UniqueTracker,
    schema: &DatabaseSchema,
    base_time: chrono::NaiveDateTime,
) -> Result<IndexMap<String, Value>> {
    let mut row = IndexMap::new();

    // First pass: generate correlated groups
    let mut correlated_values: IndexMap<String, Value> = IndexMap::new();
    for group_plan in &table_plan.correlation_groups {
        let group_values = crate::generate::correlated::generate_correlated_group(
            group_plan, row_index, rng, base_time,
        );
        for (col_name, value) in group_values {
            correlated_values.insert(col_name, value);
        }
    }

    for col_plan in &table_plan.column_plans {
        // Check null probability
        if col_plan.nullable && col_plan.null_probability > 0.0 {
            let roll: f64 = rng.random();
            if roll < col_plan.null_probability {
                row.insert(col_plan.column_name.clone(), Value::Null);
                continue;
            }
        }

        let value = match &col_plan.strategy {
            GenerationStrategy::AutoIncrement => {
                // Skip — database handles this
                continue;
            }
            GenerationStrategy::Skip => {
                continue;
            }
            GenerationStrategy::Deferred => {
                // Insert NULL for now, will be updated later
                Value::Null
            }
            GenerationStrategy::ForeignKeyReference {
                referenced_table,
                referenced_column,
            } => match fk_pool.pick_reference(referenced_table, referenced_column, rng) {
                Some(v) => v,
                None => {
                    if col_plan.nullable {
                        Value::Null
                    } else {
                        return Err(SeedKitError::ForeignKeyResolution {
                            source_table: table_plan.table_name.clone(),
                            source_column: col_plan.column_name.clone(),
                            target_table: referenced_table.clone(),
                            target_column: referenced_column.clone(),
                        });
                    }
                }
            },
            GenerationStrategy::EnumValue { values } => {
                if values.is_empty() {
                    Value::Null
                } else {
                    // TODO: Use Arc<str> or string interner in GenerationStrategy::EnumValue
                    // to allow Cow::Borrowed here. Currently .clone() + Cow::Owned still
                    // allocates per row — acceptable for Phase 1, fix in perf tuning phase.
                    let idx = rng.random_range(0..values.len());
                    Value::String(Cow::Owned(values[idx].clone()))
                }
            }
            GenerationStrategy::Correlated { .. } => {
                // Use pre-generated correlated value
                correlated_values
                    .shift_remove(&col_plan.column_name)
                    .unwrap_or_else(|| {
                        generate_value(
                            col_plan.semantic_type,
                            rng,
                            row_index,
                            &col_plan.check_constraints,
                            base_time,
                        )
                    })
            }
            GenerationStrategy::SemanticProvider => generate_value(
                col_plan.semantic_type,
                rng,
                row_index,
                &col_plan.check_constraints,
                base_time,
            ),
            GenerationStrategy::Custom { ref provider_path } => {
                return Err(SeedKitError::Config {
                    message: format!(
                        "Custom JS/WASM provider '{}' for {}.{} is not yet supported. \
                         Use [columns.\"{}.{}\"] values = [...] in seedkit.toml instead.",
                        provider_path,
                        table_plan.table_name,
                        col_plan.column_name,
                        table_plan.table_name,
                        col_plan.column_name,
                    ),
                });
            }
            GenerationStrategy::ValueList {
                ref values,
                ref weights,
            } => {
                if values.is_empty() {
                    Value::Null
                } else if let Some(ref w) = weights {
                    weighted_pick(values, w, rng)
                } else {
                    let idx = rng.random_range(0..values.len());
                    Value::String(Cow::Owned(values[idx].clone()))
                }
            }
        };

        // Single-column unique constraint check with retry
        if let Some(table) = schema.tables.get(&table_plan.table_name) {
            let needs_unique = table
                .unique_constraints
                .iter()
                .any(|uc| uc.columns.len() == 1 && uc.columns[0] == col_plan.column_name)
                || table.primary_key.as_ref().is_some_and(|pk| {
                    pk.columns.len() == 1 && pk.columns[0] == col_plan.column_name
                });

            if needs_unique && !value.is_null() {
                let mut final_value = value;
                let mut col_attempts = 0;
                while !unique_tracker.try_insert_single(
                    &table_plan.table_name,
                    &col_plan.column_name,
                    &final_value,
                ) {
                    col_attempts += 1;
                    if col_attempts >= unique_tracker.max_retries {
                        return Err(SeedKitError::UniqueExhausted {
                            table: table_plan.table_name.clone(),
                            column: col_plan.column_name.clone(),
                            row_index,
                            max_retries: unique_tracker.max_retries,
                        });
                    }
                    final_value = generate_value(
                        col_plan.semantic_type,
                        rng,
                        row_index + col_attempts,
                        &col_plan.check_constraints,
                        base_time,
                    );
                }
                row.insert(col_plan.column_name.clone(), final_value);
                continue;
            }
        }

        row.insert(col_plan.column_name.clone(), value);
    }

    Ok(row)
}

/// Weighted random selection from a value list.
///
/// Uses cumulative distribution for O(n) selection.
/// Edge cases:
/// - All weights zero → uniform fallback
/// - Negative weights → clamped to zero
/// - Single value → always returns it
fn weighted_pick(values: &[String], weights: &[f64], rng: &mut impl Rng) -> Value {
    if values.len() == 1 {
        return Value::String(Cow::Owned(values[0].clone()));
    }

    // Clamp negative weights to zero
    let clamped: Vec<f64> = weights.iter().map(|w| w.max(0.0)).collect();
    let total: f64 = clamped.iter().sum();

    if total <= 0.0 {
        // All weights are zero — uniform fallback
        let idx = rng.random_range(0..values.len());
        return Value::String(Cow::Owned(values[idx].clone()));
    }

    let roll: f64 = rng.random::<f64>() * total;
    let mut cumulative = 0.0;
    for (i, w) in clamped.iter().enumerate() {
        cumulative += w;
        if roll < cumulative {
            return Value::String(Cow::Owned(values[i].clone()));
        }
    }

    // Floating-point edge case — return last value
    Value::String(Cow::Owned(values.last().unwrap().clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::borrow::Cow;

    /// Build a minimal plan with a single table + single column using the given strategy.
    fn single_column_plan(
        table: &str,
        column: &str,
        strategy: GenerationStrategy,
        row_count: usize,
    ) -> GenerationPlan {
        GenerationPlan {
            table_plans: vec![TableGenerationPlan {
                table_name: table.to_string(),
                row_count,
                column_plans: vec![ColumnGenerationPlan {
                    column_name: column.to_string(),
                    semantic_type: crate::classify::semantic::SemanticType::Unknown,
                    strategy,
                    nullable: false,
                    null_probability: 0.0,
                    check_constraints: Vec::new(),
                }],
                correlation_groups: Vec::new(),
            }],
            deferred_edges: Vec::new(),
            seed: 42,
            default_row_count: row_count,
            base_time: chrono::Utc::now().naive_utc(),
            sequence_offset: 0,
        }
    }

    fn empty_schema() -> DatabaseSchema {
        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());
        let table = Table::new("items".to_string());
        schema.tables.insert("items".to_string(), table);
        schema
    }

    #[test]
    fn test_value_list_uniform_distribution() {
        let plan = single_column_plan(
            "items",
            "color",
            GenerationStrategy::ValueList {
                values: vec!["red".into(), "blue".into(), "green".into()],
                weights: None,
            },
            300,
        );
        let schema = empty_schema();
        let data = execute_plan(&plan, &schema, None).unwrap();
        let rows = &data.tables["items"];
        assert_eq!(rows.len(), 300);

        // All values should be from the list
        for row in rows {
            let val = row.get("color").unwrap();
            match val {
                Value::String(s) => {
                    assert!(
                        s == "red" || s == "blue" || s == "green",
                        "Unexpected value: {}",
                        s
                    );
                }
                _ => panic!("Expected String, got {:?}", val),
            }
        }

        // Each value should appear at least once (with 300 rows and 3 values, extremely likely)
        let count_red = rows
            .iter()
            .filter(|r| r.get("color") == Some(&Value::String(Cow::Owned("red".into()))))
            .count();
        let count_blue = rows
            .iter()
            .filter(|r| r.get("color") == Some(&Value::String(Cow::Owned("blue".into()))))
            .count();
        let count_green = rows
            .iter()
            .filter(|r| r.get("color") == Some(&Value::String(Cow::Owned("green".into()))))
            .count();
        assert!(count_red > 0, "red should appear");
        assert!(count_blue > 0, "blue should appear");
        assert!(count_green > 0, "green should appear");
        assert_eq!(count_red + count_blue + count_green, 300);
    }

    #[test]
    fn test_value_list_weighted_distribution() {
        // 90% "a", 10% "b" — over 1000 rows, "a" should dominate
        let plan = single_column_plan(
            "items",
            "tag",
            GenerationStrategy::ValueList {
                values: vec!["a".into(), "b".into()],
                weights: Some(vec![0.9, 0.1]),
            },
            1000,
        );
        let schema = empty_schema();
        let data = execute_plan(&plan, &schema, None).unwrap();
        let rows = &data.tables["items"];

        let count_a = rows
            .iter()
            .filter(|r| r.get("tag") == Some(&Value::String(Cow::Owned("a".into()))))
            .count();
        // With 90% weight over 1000 rows, "a" should appear at least 700 times
        assert!(
            count_a > 700,
            "Expected >700 'a' values with 0.9 weight, got {}",
            count_a
        );
    }

    #[test]
    fn test_value_list_single_value() {
        let plan = single_column_plan(
            "items",
            "status",
            GenerationStrategy::ValueList {
                values: vec!["active".into()],
                weights: Some(vec![1.0]),
            },
            10,
        );
        let schema = empty_schema();
        let data = execute_plan(&plan, &schema, None).unwrap();
        let rows = &data.tables["items"];

        for row in rows {
            assert_eq!(
                row.get("status"),
                Some(&Value::String(Cow::Owned("active".into())))
            );
        }
    }

    #[test]
    fn test_value_list_empty_produces_null() {
        let plan = single_column_plan(
            "items",
            "empty_col",
            GenerationStrategy::ValueList {
                values: Vec::new(),
                weights: None,
            },
            5,
        );
        let schema = empty_schema();
        let data = execute_plan(&plan, &schema, None).unwrap();
        let rows = &data.tables["items"];

        for row in rows {
            assert_eq!(row.get("empty_col"), Some(&Value::Null));
        }
    }

    #[test]
    fn test_value_list_deterministic_with_seed() {
        let strategy = GenerationStrategy::ValueList {
            values: vec!["x".into(), "y".into(), "z".into()],
            weights: Some(vec![0.33, 0.33, 0.34]),
        };
        let plan1 = single_column_plan("items", "val", strategy.clone(), 50);
        let plan2 = single_column_plan("items", "val", strategy, 50);

        let schema = empty_schema();
        let data1 = execute_plan(&plan1, &schema, None).unwrap();
        let data2 = execute_plan(&plan2, &schema, None).unwrap();

        // Same seed = same output
        let vals1: Vec<_> = data1.tables["items"]
            .iter()
            .map(|r| r.get("val").unwrap().clone())
            .collect();
        let vals2: Vec<_> = data2.tables["items"]
            .iter()
            .map(|r| r.get("val").unwrap().clone())
            .collect();
        assert_eq!(vals1, vals2, "Same seed should produce identical output");
    }

    #[test]
    fn test_weighted_pick_all_zeros_uniform_fallback() {
        let values = vec!["a".into(), "b".into(), "c".into()];
        let weights = vec![0.0, 0.0, 0.0];
        let mut rng = StdRng::seed_from_u64(42);

        // Should not panic — falls back to uniform
        let mut results = std::collections::HashSet::new();
        for _ in 0..100 {
            if let Value::String(s) = weighted_pick(&values, &weights, &mut rng) {
                results.insert(s.to_string());
            }
        }
        // All values should appear in uniform distribution over 100 picks
        assert!(results.len() > 1, "Uniform fallback should produce variety");
    }

    #[test]
    fn test_weighted_pick_negative_weights_clamped() {
        let values = vec!["a".into(), "b".into()];
        let weights = vec![-1.0, 1.0];
        let mut rng = StdRng::seed_from_u64(42);

        // Negative weight clamped to 0, so only "b" should ever appear
        for _ in 0..50 {
            if let Value::String(s) = weighted_pick(&values, &weights, &mut rng) {
                assert_eq!(s.as_ref(), "b", "Negative weight should be clamped to 0");
            }
        }
    }

    #[test]
    fn test_custom_provider_path_returns_error() {
        let plan = single_column_plan(
            "items",
            "tax_code",
            GenerationStrategy::Custom {
                provider_path: "./scripts/tax_gen.js".into(),
            },
            5,
        );
        let schema = empty_schema();
        let result = execute_plan(&plan, &schema, None);

        assert!(result.is_err(), "Custom provider should return an error");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("not yet supported"),
            "Error should explain JS/WASM is not supported: {}",
            err_msg
        );
        assert!(
            err_msg.contains("tax_gen.js"),
            "Error should include the path: {}",
            err_msg
        );
    }
}
