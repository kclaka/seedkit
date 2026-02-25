use serde::{Deserialize, Serialize};

use crate::schema::types::DatabaseSchema;

/// Result of schema drift detection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftReport {
    pub has_drift: bool,
    pub new_tables: Vec<String>,
    pub removed_tables: Vec<String>,
    pub new_columns: Vec<ColumnRef>,
    pub removed_columns: Vec<ColumnRef>,
    pub changed_columns: Vec<ColumnChange>,
}

impl DriftReport {
    /// Human-readable summary for terminal output.
    pub fn summary(&self) -> String {
        if !self.has_drift {
            return "No schema drift detected.".to_string();
        }

        let mut lines = vec!["Schema drift detected:".to_string()];

        for t in &self.new_tables {
            lines.push(format!("  + table: {}", t));
        }
        for t in &self.removed_tables {
            lines.push(format!("  - table: {}", t));
        }
        for c in &self.new_columns {
            lines.push(format!("  + column: {}.{}", c.table, c.column));
        }
        for c in &self.removed_columns {
            lines.push(format!("  - column: {}.{}", c.table, c.column));
        }
        for c in &self.changed_columns {
            if c.column.is_empty() {
                // Table-level constraint change (FK, unique, check)
                lines.push(format!(
                    "  ~ {}: {} ({})",
                    c.table, c.change_type, c.details
                ));
            } else {
                lines.push(format!(
                    "  ~ {}.{}: {} ({})",
                    c.table, c.column, c.change_type, c.details
                ));
            }
        }

        lines.join("\n")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnRef {
    pub table: String,
    pub column: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnChange {
    pub table: String,
    pub column: String,
    pub change_type: String,
    pub details: String,
}

/// Compute the schema hash for drift detection.
///
/// Deep-sorts all non-deterministic arrays (foreign keys, unique constraints,
/// check constraints) before serializing, so the hash is stable regardless
/// of the order the database returns them. SQL databases do not guarantee
/// row order without an explicit `ORDER BY`, so two introspections of the
/// same schema can return constraints in different orders.
pub fn compute_schema_hash(schema: &DatabaseSchema) -> String {
    use sha2::{Digest, Sha256};
    let normalized = normalize_for_hash(schema);
    let serialized = serde_json::to_string(&normalized).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(serialized.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Clone the schema and sort all non-deterministic arrays so that
/// serialization is order-independent.
fn normalize_for_hash(schema: &DatabaseSchema) -> DatabaseSchema {
    let mut normalized = schema.clone();

    for table in normalized.tables.values_mut() {
        // Sort foreign keys by source columns (deterministic key)
        table.foreign_keys.sort_by(|a, b| {
            a.source_columns
                .cmp(&b.source_columns)
                .then_with(|| a.referenced_table.cmp(&b.referenced_table))
        });

        // Sort unique constraints by column list
        table
            .unique_constraints
            .sort_by(|a, b| a.columns.cmp(&b.columns));

        // Sort check constraints by expression (the stable identifier)
        table
            .check_constraints
            .sort_by(|a, b| a.expression.cmp(&b.expression));
    }

    normalized
}

/// Compare the lock file's stored schema snapshot against the current live schema.
///
/// Uses a fast-path hash comparison first: if the hashes match, returns
/// immediately with no drift. When they differ, delegates to
/// `check_drift_detailed` to produce an actionable column-level diff.
///
/// The lock file stores the full `DatabaseSchema` snapshot (like
/// `package-lock.json` stores the full dependency tree), so this always
/// produces detailed reports — never a bare "hash changed" boolean.
pub fn check_drift(
    schema_snapshot: &DatabaseSchema,
    schema_hash: &str,
    current_schema: &DatabaseSchema,
) -> DriftReport {
    let current_hash = compute_schema_hash(current_schema);

    // Fast path: if hashes match, no drift.
    if current_hash == schema_hash {
        return DriftReport {
            has_drift: false,
            new_tables: Vec::new(),
            removed_tables: Vec::new(),
            new_columns: Vec::new(),
            removed_columns: Vec::new(),
            changed_columns: Vec::new(),
        };
    }

    check_drift_detailed(schema_snapshot, current_schema)
}

/// Detailed drift detection comparing two schema snapshots.
///
/// The `old` schema is the one captured when the lock file was created.
/// The `current` schema is the live database. This produces a full
/// column-level diff.
pub fn check_drift_detailed(old: &DatabaseSchema, current: &DatabaseSchema) -> DriftReport {
    let mut new_tables = Vec::new();
    let mut removed_tables = Vec::new();
    let mut new_columns = Vec::new();
    let mut removed_columns = Vec::new();
    let mut changed_columns = Vec::new();

    // Find new tables (in current but not in old)
    for table_name in current.tables.keys() {
        if !old.tables.contains_key(table_name) {
            new_tables.push(table_name.clone());
        }
    }

    // Find removed tables (in old but not in current)
    for table_name in old.tables.keys() {
        if !current.tables.contains_key(table_name) {
            removed_tables.push(table_name.clone());
        }
    }

    // Compare columns in tables that exist in both schemas
    for (table_name, current_table) in &current.tables {
        let old_table = match old.tables.get(table_name) {
            Some(t) => t,
            None => continue, // Already captured as new_tables
        };

        // New columns
        for col_name in current_table.columns.keys() {
            if !old_table.columns.contains_key(col_name) {
                new_columns.push(ColumnRef {
                    table: table_name.clone(),
                    column: col_name.clone(),
                });
            }
        }

        // Removed columns
        for col_name in old_table.columns.keys() {
            if !current_table.columns.contains_key(col_name) {
                removed_columns.push(ColumnRef {
                    table: table_name.clone(),
                    column: col_name.clone(),
                });
            }
        }

        // Changed columns (type or nullability)
        for (col_name, current_col) in &current_table.columns {
            if let Some(old_col) = old_table.columns.get(col_name) {
                if current_col.data_type != old_col.data_type {
                    changed_columns.push(ColumnChange {
                        table: table_name.clone(),
                        column: col_name.clone(),
                        change_type: "type_changed".to_string(),
                        details: format!("{} → {}", old_col.data_type, current_col.data_type),
                    });
                }
                if current_col.nullable != old_col.nullable {
                    changed_columns.push(ColumnChange {
                        table: table_name.clone(),
                        column: col_name.clone(),
                        change_type: "nullable_changed".to_string(),
                        details: format!(
                            "nullable: {} → {}",
                            old_col.nullable, current_col.nullable
                        ),
                    });
                }
            }
        }

        // Diff foreign keys
        let old_fks: std::collections::BTreeSet<String> = old_table
            .foreign_keys
            .iter()
            .map(|fk| {
                format!(
                    "({}) -> {}({})",
                    fk.source_columns.join(","),
                    fk.referenced_table,
                    fk.referenced_columns.join(",")
                )
            })
            .collect();
        let cur_fks: std::collections::BTreeSet<String> = current_table
            .foreign_keys
            .iter()
            .map(|fk| {
                format!(
                    "({}) -> {}({})",
                    fk.source_columns.join(","),
                    fk.referenced_table,
                    fk.referenced_columns.join(",")
                )
            })
            .collect();
        for added in cur_fks.difference(&old_fks) {
            changed_columns.push(ColumnChange {
                table: table_name.clone(),
                column: String::new(),
                change_type: "fk_added".to_string(),
                details: format!("FOREIGN KEY {}", added),
            });
        }
        for removed in old_fks.difference(&cur_fks) {
            changed_columns.push(ColumnChange {
                table: table_name.clone(),
                column: String::new(),
                change_type: "fk_removed".to_string(),
                details: format!("FOREIGN KEY {}", removed),
            });
        }

        // Diff unique constraints
        let old_uqs: std::collections::BTreeSet<String> = old_table
            .unique_constraints
            .iter()
            .map(|uq| format!("({})", uq.columns.join(",")))
            .collect();
        let cur_uqs: std::collections::BTreeSet<String> = current_table
            .unique_constraints
            .iter()
            .map(|uq| format!("({})", uq.columns.join(",")))
            .collect();
        for added in cur_uqs.difference(&old_uqs) {
            changed_columns.push(ColumnChange {
                table: table_name.clone(),
                column: String::new(),
                change_type: "unique_added".to_string(),
                details: format!("UNIQUE {}", added),
            });
        }
        for removed in old_uqs.difference(&cur_uqs) {
            changed_columns.push(ColumnChange {
                table: table_name.clone(),
                column: String::new(),
                change_type: "unique_removed".to_string(),
                details: format!("UNIQUE {}", removed),
            });
        }

        // Diff check constraints
        let old_cks: std::collections::BTreeSet<String> = old_table
            .check_constraints
            .iter()
            .map(|ck| ck.expression.clone())
            .collect();
        let cur_cks: std::collections::BTreeSet<String> = current_table
            .check_constraints
            .iter()
            .map(|ck| ck.expression.clone())
            .collect();
        for added in cur_cks.difference(&old_cks) {
            changed_columns.push(ColumnChange {
                table: table_name.clone(),
                column: String::new(),
                change_type: "check_added".to_string(),
                details: format!("CHECK ({})", added),
            });
        }
        for removed in old_cks.difference(&cur_cks) {
            changed_columns.push(ColumnChange {
                table: table_name.clone(),
                column: String::new(),
                change_type: "check_removed".to_string(),
                details: format!("CHECK ({})", removed),
            });
        }
    }

    let has_drift = !new_tables.is_empty()
        || !removed_tables.is_empty()
        || !new_columns.is_empty()
        || !removed_columns.is_empty()
        || !changed_columns.is_empty();

    DriftReport {
        has_drift,
        new_tables,
        removed_tables,
        new_columns,
        removed_columns,
        changed_columns,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::*;

    #[allow(clippy::type_complexity)]
    fn make_schema(tables: Vec<(&str, Vec<(&str, DataType, bool)>)>) -> DatabaseSchema {
        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());
        for (table_name, cols) in tables {
            let mut table = Table::new(table_name.to_string());
            for (i, (col_name, dt, nullable)) in cols.iter().enumerate() {
                let mut col = Column::new(col_name.to_string(), dt.clone(), dt.to_string());
                col.nullable = *nullable;
                col.ordinal_position = i as u32;
                table.columns.insert(col_name.to_string(), col);
            }
            schema.tables.insert(table_name.to_string(), table);
        }
        schema
    }

    #[test]
    fn test_no_drift_identical_schemas() {
        let schema = make_schema(vec![(
            "users",
            vec![
                ("id", DataType::Serial, false),
                ("name", DataType::VarChar, false),
            ],
        )]);
        let report = check_drift_detailed(&schema, &schema);
        assert!(!report.has_drift);
    }

    #[test]
    fn test_new_table_detected() {
        let old = make_schema(vec![("users", vec![("id", DataType::Serial, false)])]);
        let current = make_schema(vec![
            ("users", vec![("id", DataType::Serial, false)]),
            ("posts", vec![("id", DataType::Serial, false)]),
        ]);
        let report = check_drift_detailed(&old, &current);
        assert!(report.has_drift);
        assert_eq!(report.new_tables, vec!["posts"]);
    }

    #[test]
    fn test_removed_table_detected() {
        let old = make_schema(vec![
            ("users", vec![("id", DataType::Serial, false)]),
            ("posts", vec![("id", DataType::Serial, false)]),
        ]);
        let current = make_schema(vec![("users", vec![("id", DataType::Serial, false)])]);
        let report = check_drift_detailed(&old, &current);
        assert!(report.has_drift);
        assert_eq!(report.removed_tables, vec!["posts"]);
    }

    #[test]
    fn test_new_column_detected() {
        let old = make_schema(vec![("users", vec![("id", DataType::Serial, false)])]);
        let current = make_schema(vec![(
            "users",
            vec![
                ("id", DataType::Serial, false),
                ("email", DataType::VarChar, false),
            ],
        )]);
        let report = check_drift_detailed(&old, &current);
        assert!(report.has_drift);
        assert_eq!(report.new_columns.len(), 1);
        assert_eq!(report.new_columns[0].column, "email");
    }

    #[test]
    fn test_type_change_detected() {
        let old = make_schema(vec![("users", vec![("age", DataType::Integer, false)])]);
        let current = make_schema(vec![("users", vec![("age", DataType::BigInt, false)])]);
        let report = check_drift_detailed(&old, &current);
        assert!(report.has_drift);
        assert_eq!(report.changed_columns.len(), 1);
        assert_eq!(report.changed_columns[0].change_type, "type_changed");
        assert!(report.changed_columns[0].details.contains("integer"));
        assert!(report.changed_columns[0].details.contains("bigint"));
    }

    #[test]
    fn test_nullable_change_detected() {
        let old = make_schema(vec![("users", vec![("name", DataType::VarChar, false)])]);
        let current = make_schema(vec![("users", vec![("name", DataType::VarChar, true)])]);
        let report = check_drift_detailed(&old, &current);
        assert!(report.has_drift);
        assert_eq!(report.changed_columns.len(), 1);
        assert_eq!(report.changed_columns[0].change_type, "nullable_changed");
    }

    #[test]
    fn test_schema_hash_deterministic() {
        let schema = make_schema(vec![("users", vec![("id", DataType::Serial, false)])]);
        let h1 = compute_schema_hash(&schema);
        let h2 = compute_schema_hash(&schema);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_summary_no_drift() {
        let report = DriftReport {
            has_drift: false,
            new_tables: Vec::new(),
            removed_tables: Vec::new(),
            new_columns: Vec::new(),
            removed_columns: Vec::new(),
            changed_columns: Vec::new(),
        };
        assert_eq!(report.summary(), "No schema drift detected.");
    }

    #[test]
    fn test_summary_with_drift() {
        let report = DriftReport {
            has_drift: true,
            new_tables: vec!["posts".to_string()],
            removed_tables: vec!["legacy".to_string()],
            new_columns: vec![ColumnRef {
                table: "users".into(),
                column: "email".into(),
            }],
            removed_columns: Vec::new(),
            changed_columns: Vec::new(),
        };
        let s = report.summary();
        assert!(s.contains("+ table: posts"));
        assert!(s.contains("- table: legacy"));
        assert!(s.contains("+ column: users.email"));
    }

    // --- Heavy lockfile: check_drift with schema snapshot ---

    #[test]
    fn test_check_drift_hash_match_fast_path() {
        let schema = make_schema(vec![("users", vec![("id", DataType::Serial, false)])]);
        let hash = compute_schema_hash(&schema);

        let report = check_drift(&schema, &hash, &schema);
        assert!(!report.has_drift);
    }

    #[test]
    fn test_check_drift_hash_mismatch_produces_detailed_report() {
        let old = make_schema(vec![("users", vec![("id", DataType::Serial, false)])]);
        let old_hash = compute_schema_hash(&old);

        let current = make_schema(vec![
            (
                "users",
                vec![
                    ("id", DataType::Serial, false),
                    ("email", DataType::VarChar, false),
                ],
            ),
            ("posts", vec![("id", DataType::Serial, false)]),
        ]);

        let report = check_drift(&old, &old_hash, &current);
        assert!(report.has_drift);
        // Must produce actionable details, not empty vectors
        assert_eq!(report.new_tables, vec!["posts"]);
        assert_eq!(report.new_columns.len(), 1);
        assert_eq!(report.new_columns[0].column, "email");
    }

    #[test]
    fn test_check_drift_removed_column_detailed() {
        let old = make_schema(vec![(
            "users",
            vec![
                ("id", DataType::Serial, false),
                ("legacy_field", DataType::Text, true),
            ],
        )]);
        let old_hash = compute_schema_hash(&old);

        let current = make_schema(vec![("users", vec![("id", DataType::Serial, false)])]);

        let report = check_drift(&old, &old_hash, &current);
        assert!(report.has_drift);
        assert_eq!(report.removed_columns.len(), 1);
        assert_eq!(report.removed_columns[0].column, "legacy_field");
    }

    // --- Fix 1: Non-deterministic hash (array ordering) ---

    #[test]
    fn test_hash_stable_regardless_of_fk_order() {
        // Two schemas identical except foreign keys are in different order.
        // Hash must be the same.
        let mut schema_a = make_schema(vec![(
            "orders",
            vec![
                ("id", DataType::Serial, false),
                ("user_id", DataType::Integer, false),
                ("product_id", DataType::Integer, false),
            ],
        )]);
        schema_a.tables.get_mut("orders").unwrap().foreign_keys = vec![
            ForeignKey {
                name: Some("fk_user".to_string()),
                source_columns: vec!["user_id".to_string()],
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: ForeignKeyAction::Cascade,
                on_update: ForeignKeyAction::NoAction,
                is_deferrable: false,
            },
            ForeignKey {
                name: Some("fk_product".to_string()),
                source_columns: vec!["product_id".to_string()],
                referenced_table: "products".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: ForeignKeyAction::NoAction,
                on_update: ForeignKeyAction::NoAction,
                is_deferrable: false,
            },
        ];

        let mut schema_b = schema_a.clone();
        // Reverse the FK order
        schema_b
            .tables
            .get_mut("orders")
            .unwrap()
            .foreign_keys
            .reverse();

        let hash_a = compute_schema_hash(&schema_a);
        let hash_b = compute_schema_hash(&schema_b);
        assert_eq!(hash_a, hash_b, "Hash must be stable regardless of FK order");
    }

    #[test]
    fn test_hash_stable_regardless_of_unique_constraint_order() {
        let mut schema_a = make_schema(vec![(
            "users",
            vec![
                ("id", DataType::Serial, false),
                ("email", DataType::VarChar, false),
                ("username", DataType::VarChar, false),
            ],
        )]);
        schema_a.tables.get_mut("users").unwrap().unique_constraints = vec![
            UniqueConstraint {
                name: Some("uq_email".to_string()),
                columns: vec!["email".to_string()],
            },
            UniqueConstraint {
                name: Some("uq_username".to_string()),
                columns: vec!["username".to_string()],
            },
        ];

        let mut schema_b = schema_a.clone();
        schema_b
            .tables
            .get_mut("users")
            .unwrap()
            .unique_constraints
            .reverse();

        let hash_a = compute_schema_hash(&schema_a);
        let hash_b = compute_schema_hash(&schema_b);
        assert_eq!(
            hash_a, hash_b,
            "Hash must be stable regardless of unique constraint order"
        );
    }

    #[test]
    fn test_hash_stable_regardless_of_check_constraint_order() {
        let mut schema_a = make_schema(vec![(
            "products",
            vec![
                ("price", DataType::Numeric, false),
                ("quantity", DataType::Integer, false),
            ],
        )]);
        schema_a
            .tables
            .get_mut("products")
            .unwrap()
            .check_constraints = vec![
            CheckConstraint {
                name: Some("ck_price".to_string()),
                expression: "price >= 0".to_string(),
                parsed: None,
            },
            CheckConstraint {
                name: Some("ck_qty".to_string()),
                expression: "quantity > 0".to_string(),
                parsed: None,
            },
        ];

        let mut schema_b = schema_a.clone();
        schema_b
            .tables
            .get_mut("products")
            .unwrap()
            .check_constraints
            .reverse();

        let hash_a = compute_schema_hash(&schema_a);
        let hash_b = compute_schema_hash(&schema_b);
        assert_eq!(
            hash_a, hash_b,
            "Hash must be stable regardless of check constraint order"
        );
    }

    // --- Fix 2: Constraint blind spot ---

    #[test]
    fn test_drift_detects_added_foreign_key() {
        let old = make_schema(vec![(
            "orders",
            vec![
                ("id", DataType::Serial, false),
                ("user_id", DataType::Integer, false),
            ],
        )]);

        let mut current = old.clone();
        current
            .tables
            .get_mut("orders")
            .unwrap()
            .foreign_keys
            .push(ForeignKey {
                name: Some("fk_user".to_string()),
                source_columns: vec!["user_id".to_string()],
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: ForeignKeyAction::Cascade,
                on_update: ForeignKeyAction::NoAction,
                is_deferrable: false,
            });

        let report = check_drift_detailed(&old, &current);
        assert!(report.has_drift);
        assert!(
            report
                .changed_columns
                .iter()
                .any(|c| c.change_type == "fk_added"),
            "Should detect added FK: {:?}",
            report.changed_columns
        );
    }

    #[test]
    fn test_drift_detects_removed_unique_constraint() {
        let mut old = make_schema(vec![(
            "users",
            vec![
                ("id", DataType::Serial, false),
                ("email", DataType::VarChar, false),
            ],
        )]);
        old.tables
            .get_mut("users")
            .unwrap()
            .unique_constraints
            .push(UniqueConstraint {
                name: Some("uq_email".to_string()),
                columns: vec!["email".to_string()],
            });

        let current = make_schema(vec![(
            "users",
            vec![
                ("id", DataType::Serial, false),
                ("email", DataType::VarChar, false),
            ],
        )]);

        let report = check_drift_detailed(&old, &current);
        assert!(report.has_drift);
        assert!(
            report
                .changed_columns
                .iter()
                .any(|c| c.change_type == "unique_removed"),
            "Should detect removed unique constraint: {:?}",
            report.changed_columns
        );
    }

    #[test]
    fn test_drift_detects_removed_check_constraint() {
        let mut old = make_schema(vec![(
            "products",
            vec![("price", DataType::Numeric, false)],
        )]);
        old.tables
            .get_mut("products")
            .unwrap()
            .check_constraints
            .push(CheckConstraint {
                name: Some("ck_price".to_string()),
                expression: "price >= 0".to_string(),
                parsed: None,
            });

        let current = make_schema(vec![(
            "products",
            vec![("price", DataType::Numeric, false)],
        )]);

        let report = check_drift_detailed(&old, &current);
        assert!(report.has_drift);
        assert!(
            report
                .changed_columns
                .iter()
                .any(|c| c.change_type == "check_removed"),
            "Should detect removed check constraint: {:?}",
            report.changed_columns
        );
    }

    #[test]
    fn test_drift_no_false_positive_on_identical_constraints() {
        let mut schema = make_schema(vec![(
            "orders",
            vec![
                ("id", DataType::Serial, false),
                ("user_id", DataType::Integer, false),
            ],
        )]);
        schema
            .tables
            .get_mut("orders")
            .unwrap()
            .foreign_keys
            .push(ForeignKey {
                name: Some("fk_user".to_string()),
                source_columns: vec!["user_id".to_string()],
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: ForeignKeyAction::Cascade,
                on_update: ForeignKeyAction::NoAction,
                is_deferrable: false,
            });
        schema
            .tables
            .get_mut("orders")
            .unwrap()
            .unique_constraints
            .push(UniqueConstraint {
                name: Some("uq_user".to_string()),
                columns: vec!["user_id".to_string()],
            });

        let report = check_drift_detailed(&schema, &schema);
        assert!(
            !report.has_drift,
            "Identical schemas with constraints should not report drift"
        );
    }
}
