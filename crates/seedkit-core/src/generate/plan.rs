use std::collections::{BTreeMap, HashMap};

use crate::classify::semantic::{CorrelationGroup, SemanticType};
use crate::config::ColumnConfig;
use crate::graph::topo::DeferredEdge;
use crate::sample::stats::{ColumnDistribution, DistributionProfile};
use crate::schema::types::{DatabaseSchema, ParsedCheck};

/// The complete generation plan for all tables.
#[derive(Debug, Clone)]
pub struct GenerationPlan {
    /// Tables in insertion order (respects FK dependencies).
    pub table_plans: Vec<TableGenerationPlan>,
    /// Deferred FK edges to resolve after all inserts.
    pub deferred_edges: Vec<DeferredEdge>,
    /// Global seed for deterministic generation.
    pub seed: u64,
    /// Default row count per table (can be overridden per-table).
    pub default_row_count: usize,
    /// Pinned base timestamp for deterministic temporal generation.
    /// Captured once at plan creation time (or read from lockfile),
    /// so regeneration from a lockfile produces identical timestamps
    /// regardless of when it runs.
    pub base_time: chrono::NaiveDateTime,
    /// Offset added to synthesized auto-increment IDs.
    ///
    /// When seeding into an empty database with reset sequences, this is 0
    /// (the default). When seeding into a database where sequences already
    /// have a known starting value, set this to that value so FK references
    /// match the real database-assigned IDs.
    ///
    /// For truly safe seeding into non-empty databases, prefer the
    /// `INSERT ... RETURNING id` pipeline in direct.rs instead.
    pub sequence_offset: u64,
}

/// Generation plan for a single table.
#[derive(Debug, Clone)]
pub struct TableGenerationPlan {
    pub table_name: String,
    pub row_count: usize,
    pub column_plans: Vec<ColumnGenerationPlan>,
    pub correlation_groups: Vec<CorrelationGroupPlan>,
}

/// Generation plan for a single column.
#[derive(Debug, Clone)]
pub struct ColumnGenerationPlan {
    pub column_name: String,
    pub semantic_type: SemanticType,
    pub strategy: GenerationStrategy,
    pub nullable: bool,
    /// Probability of generating NULL (0.0 to 1.0), only if nullable.
    pub null_probability: f64,
    /// Check constraints that apply to this column.
    pub check_constraints: Vec<ParsedCheck>,
}

/// How a column's value should be generated.
#[derive(Debug, Clone)]
pub enum GenerationStrategy {
    /// Auto-incrementing (serial/bigserial) — skip, let DB handle it.
    AutoIncrement,
    /// Reference a value from a parent table's column.
    ForeignKeyReference {
        referenced_table: String,
        referenced_column: String,
    },
    /// Use the semantic type's provider to generate a value.
    SemanticProvider,
    /// Pick from a fixed list of enum values.
    EnumValue { values: Vec<String> },
    /// Generate as part of a correlation group — handled by the group.
    Correlated { group_index: usize },
    /// Deferred — insert NULL now, UPDATE later (for cycle-breaking).
    Deferred,
    /// Skip entirely (e.g., generated columns, columns with server defaults).
    Skip,
    /// Custom provider (JS or WASM).
    Custom { provider_path: String },
    /// Pick from a user-configured value list (from seedkit.toml).
    /// Distinct from EnumValue which comes from database schema enum types.
    ValueList {
        values: Vec<String>,
        weights: Option<Vec<f64>>,
    },
    /// Generate from a sampled production distribution profile.
    Distribution { distribution: ColumnDistribution },
}

/// Plan for generating correlated column values.
#[derive(Debug, Clone)]
pub struct CorrelationGroupPlan {
    pub group: CorrelationGroup,
    pub columns: Vec<(String, SemanticType)>,
}

impl GenerationPlan {
    /// Build a generation plan from a schema and classification results.
    ///
    /// `base_time` is pinned at plan creation so temporal values are deterministic.
    /// When restoring from a lockfile, pass the lockfile's stored `base_time`.
    /// When creating fresh, pass `None` to capture the current wall-clock time.
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        schema: &DatabaseSchema,
        classifications: &HashMap<(String, String), SemanticType>,
        insertion_order: &[String],
        deferred_edges: Vec<DeferredEdge>,
        default_row_count: usize,
        table_row_overrides: &BTreeMap<String, usize>,
        seed: u64,
        base_time: Option<chrono::NaiveDateTime>,
        column_overrides: &BTreeMap<String, ColumnConfig>,
        distribution_profiles: Option<&[DistributionProfile]>,
    ) -> Self {
        let deferred_columns: HashMap<(&str, &str), bool> = deferred_edges
            .iter()
            .flat_map(|e| {
                e.source_columns
                    .iter()
                    .map(move |col| (e.source_table.as_str(), col.as_str()))
            })
            .map(|k| (k, true))
            .collect();

        // Build a distribution lookup: (table, column) -> ColumnDistribution
        let dist_lookup: HashMap<(&str, &str), &ColumnDistribution> = distribution_profiles
            .unwrap_or(&[])
            .iter()
            .flat_map(|p| {
                p.column_distributions
                    .iter()
                    .filter(|(_, d)| !matches!(d, ColumnDistribution::Ratio { .. }))
                    .map(move |(col, dist)| ((p.table_name.as_str(), col.as_str()), dist))
            })
            .collect();

        // Build ratio lookup for adjusting row counts
        let ratio_lookup: HashMap<(&str, &str), f64> = distribution_profiles
            .unwrap_or(&[])
            .iter()
            .flat_map(|p| {
                p.column_distributions
                    .iter()
                    .filter_map(move |(_col, dist)| {
                        if let ColumnDistribution::Ratio {
                            related_table,
                            ratio,
                        } = dist
                        {
                            Some(((p.table_name.as_str(), related_table.as_str()), *ratio))
                        } else {
                            None
                        }
                    })
            })
            .collect();

        let mut table_plans = Vec::new();

        for table_name in insertion_order {
            let table = match schema.tables.get(table_name) {
                Some(t) => t,
                None => continue,
            };

            // Row count: explicit override > ratio-adjusted > default
            let row_count = if let Some(&explicit) = table_row_overrides.get(table_name) {
                explicit
            } else {
                // Check if any FK has a ratio profile that adjusts row count
                let ratio_adjusted = table.foreign_keys.iter().find_map(|fk| {
                    ratio_lookup
                        .get(&(table_name.as_str(), fk.referenced_table.as_str()))
                        .map(|ratio| {
                            let parent_count = table_row_overrides
                                .get(&fk.referenced_table)
                                .copied()
                                .unwrap_or(default_row_count);
                            (parent_count as f64 * ratio).round() as usize
                        })
                });
                ratio_adjusted.unwrap_or(default_row_count)
            };

            // Detect correlation groups for this table
            let mut correlation_groups = Vec::new();
            let mut correlated_columns: HashMap<String, usize> = HashMap::new();

            // Group columns by their correlation group
            let mut group_map: HashMap<CorrelationGroup, Vec<(String, SemanticType)>> =
                HashMap::new();
            for (col_name, _column) in &table.columns {
                if let Some(st) = classifications.get(&(table_name.clone(), col_name.clone())) {
                    if let Some(group) = st.correlation_group() {
                        group_map
                            .entry(group)
                            .or_default()
                            .push((col_name.clone(), *st));
                    }
                }
            }

            for (group, columns) in group_map {
                if columns.len() >= 2 {
                    let group_index = correlation_groups.len();
                    for (col_name, _) in &columns {
                        correlated_columns.insert(col_name.clone(), group_index);
                    }
                    correlation_groups.push(CorrelationGroupPlan { group, columns });
                }
            }

            // Build column plans
            let mut column_plans = Vec::new();
            let pk_columns: Vec<&str> = table
                .primary_key
                .as_ref()
                .map(|pk| pk.columns.iter().map(|s| s.as_str()).collect())
                .unwrap_or_default();

            for (col_name, column) in &table.columns {
                let semantic_type = classifications
                    .get(&(table_name.clone(), col_name.clone()))
                    .copied()
                    .unwrap_or(SemanticType::Unknown);

                // Check for user-configured column overrides (seedkit.toml)
                let col_key = format!("{}.{}", table_name, col_name);
                let config_strategy = column_overrides.get(&col_key).and_then(|cfg| {
                    cfg.values
                        .as_ref()
                        .map(|values| GenerationStrategy::ValueList {
                            values: values.clone(),
                            weights: cfg.weights.clone(),
                        })
                        .or_else(|| {
                            cfg.custom.as_ref().map(|path| GenerationStrategy::Custom {
                                provider_path: path.clone(),
                            })
                        })
                });

                // Determine generation strategy — config overrides take priority
                let strategy = if let Some(s) = config_strategy {
                    s
                } else if column.is_auto_increment || column.data_type.is_serial() {
                    if pk_columns.contains(&col_name.as_str()) {
                        GenerationStrategy::AutoIncrement
                    } else {
                        GenerationStrategy::SemanticProvider
                    }
                } else if deferred_columns.contains_key(&(table_name.as_str(), col_name.as_str())) {
                    GenerationStrategy::Deferred
                } else if let Some(fk) = table
                    .foreign_keys
                    .iter()
                    .find(|fk| fk.source_columns.len() == 1 && fk.source_columns[0] == *col_name)
                {
                    // GUARD: Only use ForeignKeyReference if the referenced table
                    // is actually in this generation plan's insertion order.
                    // If the parent was excluded (via --exclude or --include that
                    // didn't capture it), fall back to semantic generation so the
                    // engine doesn't crash looking for an empty FK pool.
                    if insertion_order.contains(&fk.referenced_table) {
                        GenerationStrategy::ForeignKeyReference {
                            referenced_table: fk.referenced_table.clone(),
                            referenced_column: fk.referenced_columns[0].clone(),
                        }
                    } else {
                        GenerationStrategy::SemanticProvider
                    }
                } else if let Some(dist) =
                    dist_lookup.get(&(table_name.as_str(), col_name.as_str()))
                {
                    GenerationStrategy::Distribution {
                        distribution: (*dist).clone(),
                    }
                } else if let Some(ref values) = column.enum_values {
                    GenerationStrategy::EnumValue {
                        values: values.clone(),
                    }
                } else if let Some(&group_index) = correlated_columns.get(col_name) {
                    GenerationStrategy::Correlated { group_index }
                } else {
                    GenerationStrategy::SemanticProvider
                };

                // Collect applicable check constraints
                let check_constraints: Vec<ParsedCheck> = table
                    .check_constraints
                    .iter()
                    .filter_map(|cc| {
                        cc.parsed.as_ref().and_then(|p| {
                            if check_applies_to_column(p, col_name) {
                                Some(p.clone())
                            } else {
                                None
                            }
                        })
                    })
                    .collect();

                let null_probability =
                    if column.nullable && !pk_columns.contains(&col_name.as_str()) {
                        match semantic_type {
                            SemanticType::DeletedAt => 0.8, // Most rows aren't soft-deleted
                            _ => 0.05, // Small chance of NULL for nullable columns
                        }
                    } else {
                        0.0
                    };

                column_plans.push(ColumnGenerationPlan {
                    column_name: col_name.clone(),
                    semantic_type,
                    strategy,
                    nullable: column.nullable,
                    null_probability,
                    check_constraints,
                });
            }

            table_plans.push(TableGenerationPlan {
                table_name: table_name.clone(),
                row_count,
                column_plans,
                correlation_groups,
            });
        }

        GenerationPlan {
            table_plans,
            deferred_edges,
            seed,
            default_row_count,
            base_time: base_time.unwrap_or_else(|| chrono::Utc::now().naive_utc()),
            sequence_offset: 0,
        }
    }
}

/// Filter an insertion order by include/exclude lists.
///
/// When `include` is non-empty, only tables in the list (plus their FK
/// dependencies) are kept. When `exclude` is non-empty, listed tables are
/// removed. `include` takes priority: if both are set, `include` is applied
/// first and `exclude` is then applied to the result.
///
/// FK dependencies are automatically added — if you `--include orders` and
/// `orders` FK-references `users`, then `users` is included automatically so
/// FK generation doesn't break.
pub fn filter_insertion_order(
    insertion_order: &[String],
    schema: &DatabaseSchema,
    include: &[String],
    exclude: &[String],
) -> Vec<String> {
    if include.is_empty() && exclude.is_empty() {
        return insertion_order.to_vec();
    }

    let mut wanted: std::collections::HashSet<String> = if include.is_empty() {
        // Start with all tables
        insertion_order.iter().cloned().collect()
    } else {
        // Start with explicitly included tables
        let mut set: std::collections::HashSet<String> = include.iter().cloned().collect();
        // Close over FK dependencies: walk each included table's FKs
        // and add referenced tables (transitively).
        let mut queue: Vec<String> = include.to_vec();
        while let Some(table_name) = queue.pop() {
            if let Some(table) = schema.tables.get(&table_name) {
                for fk in &table.foreign_keys {
                    if set.insert(fk.referenced_table.clone()) {
                        queue.push(fk.referenced_table.clone());
                    }
                }
            }
        }
        set
    };

    // Apply excludes
    for table_name in exclude {
        wanted.remove(table_name);
    }

    // Preserve original insertion order (topological)
    insertion_order
        .iter()
        .filter(|t| wanted.contains(t.as_str()))
        .cloned()
        .collect()
}

fn check_applies_to_column(check: &ParsedCheck, column_name: &str) -> bool {
    match check {
        ParsedCheck::GreaterThanOrEqual { column, .. }
        | ParsedCheck::GreaterThan { column, .. }
        | ParsedCheck::LessThanOrEqual { column, .. }
        | ParsedCheck::LessThan { column, .. }
        | ParsedCheck::MinLength { column, .. }
        | ParsedCheck::InValues { column, .. } => column == column_name,
        ParsedCheck::Between { column, .. } => column == column_name,
        ParsedCheck::ColumnLessThan { left, right } => left == column_name || right == column_name,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::*;

    /// Helper: build a schema with users → orders → order_items FK chain.
    fn build_chain_schema() -> DatabaseSchema {
        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());

        let users = Table::new("users".to_string());
        schema.tables.insert("users".to_string(), users);

        let mut orders = Table::new("orders".to_string());
        orders.foreign_keys.push(ForeignKey {
            name: None,
            source_columns: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ForeignKeyAction::NoAction,
            on_update: ForeignKeyAction::NoAction,
            is_deferrable: false,
        });
        schema.tables.insert("orders".to_string(), orders);

        let mut order_items = Table::new("order_items".to_string());
        order_items.foreign_keys.push(ForeignKey {
            name: None,
            source_columns: vec!["order_id".to_string()],
            referenced_table: "orders".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ForeignKeyAction::NoAction,
            on_update: ForeignKeyAction::NoAction,
            is_deferrable: false,
        });
        schema.tables.insert("order_items".to_string(), order_items);

        let products = Table::new("products".to_string());
        schema.tables.insert("products".to_string(), products);

        schema
    }

    #[test]
    fn test_filter_no_include_no_exclude_returns_all() {
        let schema = build_chain_schema();
        let order = vec![
            "users".into(),
            "products".into(),
            "orders".into(),
            "order_items".into(),
        ];
        let result = filter_insertion_order(&order, &schema, &[], &[]);
        assert_eq!(result, order);
    }

    #[test]
    fn test_filter_include_single_table_no_fk_deps() {
        let schema = build_chain_schema();
        let order = vec![
            "users".into(),
            "products".into(),
            "orders".into(),
            "order_items".into(),
        ];
        let result = filter_insertion_order(&order, &schema, &["products".into()], &[]);
        assert_eq!(result, vec!["products"]);
    }

    #[test]
    fn test_filter_include_auto_adds_fk_dependencies() {
        let schema = build_chain_schema();
        let order = vec![
            "users".into(),
            "products".into(),
            "orders".into(),
            "order_items".into(),
        ];
        // Including "orders" should auto-include "users" (FK dependency)
        let result = filter_insertion_order(&order, &schema, &["orders".into()], &[]);
        assert_eq!(result, vec!["users", "orders"]);
    }

    #[test]
    fn test_filter_include_transitive_fk_dependencies() {
        let schema = build_chain_schema();
        let order = vec![
            "users".into(),
            "products".into(),
            "orders".into(),
            "order_items".into(),
        ];
        // Including "order_items" should auto-include "orders" AND "users" transitively
        let result = filter_insertion_order(&order, &schema, &["order_items".into()], &[]);
        assert_eq!(result, vec!["users", "orders", "order_items"]);
    }

    #[test]
    fn test_filter_exclude_removes_table() {
        let schema = build_chain_schema();
        let order = vec![
            "users".into(),
            "products".into(),
            "orders".into(),
            "order_items".into(),
        ];
        let result = filter_insertion_order(&order, &schema, &[], &["products".into()]);
        assert_eq!(result, vec!["users", "orders", "order_items"]);
    }

    #[test]
    fn test_filter_include_and_exclude_combined() {
        let schema = build_chain_schema();
        let order = vec![
            "users".into(),
            "products".into(),
            "orders".into(),
            "order_items".into(),
        ];
        // Include orders (auto-includes users), then exclude users
        let result = filter_insertion_order(&order, &schema, &["orders".into()], &["users".into()]);
        // users excluded even though it was a dependency — user's explicit choice
        assert_eq!(result, vec!["orders"]);
    }

    #[test]
    fn test_filter_preserves_topological_order() {
        let schema = build_chain_schema();
        let order = vec![
            "users".into(),
            "products".into(),
            "orders".into(),
            "order_items".into(),
        ];
        let result = filter_insertion_order(
            &order,
            &schema,
            &["order_items".into(), "products".into()],
            &[],
        );
        // Should preserve: users, products, orders, order_items
        // (users and orders are auto-included as deps of order_items)
        assert_eq!(result, vec!["users", "products", "orders", "order_items"]);
    }

    #[test]
    fn test_filter_include_nonexistent_table_ignored() {
        let schema = build_chain_schema();
        let order = vec!["users".into(), "products".into()];
        let result = filter_insertion_order(
            &order,
            &schema,
            &["users".into(), "nonexistent".into()],
            &[],
        );
        assert_eq!(result, vec!["users"]);
    }

    // --- Dependency guard: FK to excluded parent falls back to SemanticProvider ---

    #[test]
    fn test_fk_to_excluded_parent_uses_semantic_provider() {
        let mut schema = build_chain_schema();

        // Give orders a user_id column so it can be classified
        let user_id_col = Column::new(
            "user_id".to_string(),
            DataType::Integer,
            "integer".to_string(),
        );
        schema
            .tables
            .get_mut("orders")
            .unwrap()
            .columns
            .insert("user_id".to_string(), user_id_col);

        let classifications = HashMap::new();
        // Only "orders" in the insertion order — "users" excluded
        let insertion_order = vec!["orders".to_string()];

        let plan = GenerationPlan::build(
            &schema,
            &classifications,
            &insertion_order,
            Vec::new(),
            10,
            &BTreeMap::new(),
            42,
            None,
            &BTreeMap::new(),
            None,
        );

        // Find the user_id column plan in orders
        let orders_plan = plan
            .table_plans
            .iter()
            .find(|t| t.table_name == "orders")
            .unwrap();
        let user_id_plan = orders_plan
            .column_plans
            .iter()
            .find(|c| c.column_name == "user_id")
            .unwrap();

        // Should be SemanticProvider, NOT ForeignKeyReference (users is not in the plan)
        assert!(
            matches!(user_id_plan.strategy, GenerationStrategy::SemanticProvider),
            "Expected SemanticProvider for FK to excluded parent, got {:?}",
            user_id_plan.strategy
        );
    }

    #[test]
    fn test_fk_to_included_parent_uses_fk_reference() {
        let mut schema = build_chain_schema();

        let user_id_col = Column::new(
            "user_id".to_string(),
            DataType::Integer,
            "integer".to_string(),
        );
        schema
            .tables
            .get_mut("orders")
            .unwrap()
            .columns
            .insert("user_id".to_string(), user_id_col);

        let classifications = HashMap::new();
        // Both "users" and "orders" in the insertion order
        let insertion_order = vec!["users".to_string(), "orders".to_string()];

        let plan = GenerationPlan::build(
            &schema,
            &classifications,
            &insertion_order,
            Vec::new(),
            10,
            &BTreeMap::new(),
            42,
            None,
            &BTreeMap::new(),
            None,
        );

        let orders_plan = plan
            .table_plans
            .iter()
            .find(|t| t.table_name == "orders")
            .unwrap();
        let user_id_plan = orders_plan
            .column_plans
            .iter()
            .find(|c| c.column_name == "user_id")
            .unwrap();

        // Should be ForeignKeyReference since "users" is in the plan
        assert!(
            matches!(
                user_id_plan.strategy,
                GenerationStrategy::ForeignKeyReference { .. }
            ),
            "Expected ForeignKeyReference for FK to included parent, got {:?}",
            user_id_plan.strategy
        );
    }

    // --- Config column override tests ---

    #[test]
    fn test_value_list_override_takes_priority_over_semantic() {
        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());
        let mut table = Table::new("products".to_string());
        let col = Column::new(
            "color".to_string(),
            DataType::VarChar,
            "varchar".to_string(),
        );
        table.columns.insert("color".to_string(), col);
        schema.tables.insert("products".to_string(), table);

        let classifications = HashMap::new();
        let insertion_order = vec!["products".to_string()];

        let mut overrides = BTreeMap::new();
        overrides.insert(
            "products.color".to_string(),
            crate::config::ColumnConfig {
                values: Some(vec!["red".into(), "blue".into()]),
                weights: Some(vec![0.7, 0.3]),
                custom: None,
            },
        );

        let plan = GenerationPlan::build(
            &schema,
            &classifications,
            &insertion_order,
            Vec::new(),
            10,
            &BTreeMap::new(),
            42,
            None,
            &overrides,
            None,
        );

        let col_plan = plan.table_plans[0]
            .column_plans
            .iter()
            .find(|c| c.column_name == "color")
            .unwrap();
        assert!(
            matches!(col_plan.strategy, GenerationStrategy::ValueList { .. }),
            "Expected ValueList from config override, got {:?}",
            col_plan.strategy
        );
    }

    #[test]
    fn test_custom_path_override_produces_custom_strategy() {
        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());
        let mut table = Table::new("orders".to_string());
        let col = Column::new(
            "tax_code".to_string(),
            DataType::VarChar,
            "varchar".to_string(),
        );
        table.columns.insert("tax_code".to_string(), col);
        schema.tables.insert("orders".to_string(), table);

        let mut overrides = BTreeMap::new();
        overrides.insert(
            "orders.tax_code".to_string(),
            crate::config::ColumnConfig {
                values: None,
                weights: None,
                custom: Some("./scripts/tax_gen.js".to_string()),
            },
        );

        let plan = GenerationPlan::build(
            &schema,
            &HashMap::new(),
            &["orders".to_string()],
            Vec::new(),
            10,
            &BTreeMap::new(),
            42,
            None,
            &overrides,
            None,
        );

        let col_plan = plan.table_plans[0]
            .column_plans
            .iter()
            .find(|c| c.column_name == "tax_code")
            .unwrap();
        assert!(
            matches!(col_plan.strategy, GenerationStrategy::Custom { .. }),
            "Expected Custom from config override, got {:?}",
            col_plan.strategy
        );
    }

    #[test]
    fn test_override_for_nonexistent_column_is_harmless() {
        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());
        let mut table = Table::new("users".to_string());
        let col = Column::new("name".to_string(), DataType::VarChar, "varchar".to_string());
        table.columns.insert("name".to_string(), col);
        schema.tables.insert("users".to_string(), table);

        let mut overrides = BTreeMap::new();
        overrides.insert(
            "users.nonexistent".to_string(),
            crate::config::ColumnConfig {
                values: Some(vec!["x".into()]),
                weights: None,
                custom: None,
            },
        );

        // Should not panic — the override simply has no matching column
        let plan = GenerationPlan::build(
            &schema,
            &HashMap::new(),
            &["users".to_string()],
            Vec::new(),
            10,
            &BTreeMap::new(),
            42,
            None,
            &overrides,
            None,
        );

        // "name" column should use SemanticProvider (no override for it)
        let col_plan = plan.table_plans[0]
            .column_plans
            .iter()
            .find(|c| c.column_name == "name")
            .unwrap();
        assert!(
            matches!(col_plan.strategy, GenerationStrategy::SemanticProvider),
            "Nonexistent override should not affect other columns, got {:?}",
            col_plan.strategy
        );
    }

    // --- Distribution profile tests ---

    #[test]
    fn test_ratio_adjusts_row_counts() {
        use crate::sample::stats::{ColumnDistribution, DistributionProfile};

        let schema = build_chain_schema();
        let classifications = HashMap::new();
        let insertion_order = vec![
            "users".to_string(),
            "products".to_string(),
            "orders".to_string(),
            "order_items".to_string(),
        ];

        // Profiles say: orders has 3.2x the rows of users
        let profiles = vec![
            DistributionProfile {
                table_name: "users".to_string(),
                row_count: 1000,
                column_distributions: std::collections::HashMap::new(),
            },
            DistributionProfile {
                table_name: "orders".to_string(),
                row_count: 3200,
                column_distributions: {
                    let mut m = std::collections::HashMap::new();
                    m.insert(
                        "__ratio_user_id".to_string(),
                        ColumnDistribution::Ratio {
                            related_table: "users".to_string(),
                            ratio: 3.2,
                        },
                    );
                    m
                },
            },
        ];

        let plan = GenerationPlan::build(
            &schema,
            &classifications,
            &insertion_order,
            Vec::new(),
            100, // default row count
            &BTreeMap::new(),
            42,
            None,
            &BTreeMap::new(),
            Some(&profiles),
        );

        // users should be 100 (default)
        let users_plan = plan
            .table_plans
            .iter()
            .find(|t| t.table_name == "users")
            .unwrap();
        assert_eq!(users_plan.row_count, 100);

        // orders should be adjusted: 100 * 3.2 = 320
        let orders_plan = plan
            .table_plans
            .iter()
            .find(|t| t.table_name == "orders")
            .unwrap();
        assert_eq!(
            orders_plan.row_count, 320,
            "orders should be ratio-adjusted to 320 (100 * 3.2)"
        );
    }

    #[test]
    fn test_distribution_profile_overrides_semantic_provider() {
        use crate::sample::stats::{ColumnDistribution, DistributionProfile};

        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());
        let mut table = Table::new("products".to_string());
        let col = Column::new(
            "status".to_string(),
            DataType::VarChar,
            "varchar".to_string(),
        );
        table.columns.insert("status".to_string(), col);
        schema.tables.insert("products".to_string(), table);

        let profiles = vec![DistributionProfile {
            table_name: "products".to_string(),
            row_count: 500,
            column_distributions: {
                let mut m = std::collections::HashMap::new();
                m.insert(
                    "status".to_string(),
                    ColumnDistribution::Categorical {
                        values: vec![("active".to_string(), 0.9), ("draft".to_string(), 0.1)],
                    },
                );
                m
            },
        }];

        let plan = GenerationPlan::build(
            &schema,
            &HashMap::new(),
            &["products".to_string()],
            Vec::new(),
            10,
            &BTreeMap::new(),
            42,
            None,
            &BTreeMap::new(),
            Some(&profiles),
        );

        let col_plan = plan.table_plans[0]
            .column_plans
            .iter()
            .find(|c| c.column_name == "status")
            .unwrap();
        assert!(
            matches!(col_plan.strategy, GenerationStrategy::Distribution { .. }),
            "Expected Distribution strategy from profile, got {:?}",
            col_plan.strategy
        );
    }
}
