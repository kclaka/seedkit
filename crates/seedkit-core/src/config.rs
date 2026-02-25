//! # Configuration File Parser
//!
//! Reads and parses `seedkit.toml`, the optional user configuration file that
//! customizes SeedKit's behavior without requiring CLI flags. Supports:
//!
//! - `[database]` — default connection URL
//! - `[generate]` — default row count, seed, AI settings
//! - `[tables.<name>]` — per-table row count overrides
//! - `[columns."<table>.<column>"]` — custom values, weights, or provider paths
//! - `[graph]` — explicit cycle-breaking edge overrides
//!
//! Example `seedkit.toml`:
//!
//! ```toml
//! [database]
//! url = "postgres://localhost/myapp"
//!
//! [generate]
//! rows = 500
//! seed = 42
//! ai = false
//!
//! [tables.users]
//! rows = 1000
//!
//! [tables.orders]
//! rows = 5000
//!
//! [columns."products.color"]
//! values = ["red", "blue", "green", "black", "white"]
//! weights = [0.25, 0.20, 0.20, 0.20, 0.15]
//!
//! [columns."orders.tax_code"]
//! custom = "./scripts/tax_gen.js"
//!
//! [graph]
//! break_cycle_at = ["users.invited_by_id", "comments.parent_id"]
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{Result, SeedKitError};

/// Default config file name.
pub const CONFIG_FILE_NAME: &str = "seedkit.toml";

/// Top-level seedkit.toml structure.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct SeedKitConfig {
    /// Database connection settings.
    pub database: DatabaseConfig,
    /// Default generation settings.
    pub generate: GenerateConfig,
    /// Per-table overrides, keyed by table name.
    pub tables: BTreeMap<String, TableConfig>,
    /// Per-column overrides, keyed by "table.column".
    pub columns: BTreeMap<String, ColumnConfig>,
    /// Dependency graph settings.
    pub graph: GraphConfig,

    /// Absolute path to the directory containing seedkit.toml.
    ///
    /// Populated by `read_config()` so that relative paths in `custom` provider
    /// fields resolve against the config file's location, not the CWD.
    #[serde(skip)]
    pub config_dir: Option<PathBuf>,
}

/// Database connection configuration.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct DatabaseConfig {
    /// Database URL (e.g., "postgres://localhost/myapp").
    pub url: Option<String>,
    /// Schema name to introspect (e.g., "public" for PostgreSQL).
    pub schema: Option<String>,
}

/// Default generation settings.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct GenerateConfig {
    /// Default number of rows per table.
    pub rows: Option<usize>,
    /// Fixed random seed for deterministic generation.
    pub seed: Option<u64>,
    /// Whether to enable AI-enhanced classification by default.
    pub ai: Option<bool>,
    /// Tables to include (empty = all).
    pub include: Option<Vec<String>>,
    /// Tables to exclude.
    pub exclude: Option<Vec<String>>,
}

/// Per-table configuration override.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TableConfig {
    /// Number of rows to generate for this table.
    pub rows: Option<usize>,
}

/// Per-column configuration override.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ColumnConfig {
    /// Fixed list of values to pick from.
    pub values: Option<Vec<String>>,
    /// Weights for each value (must be same length as `values`).
    pub weights: Option<Vec<f64>>,
    /// Path to a custom JS or WASM provider.
    pub custom: Option<String>,
}

/// Dependency graph configuration.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct GraphConfig {
    /// Explicit FK edges to break for cycle resolution.
    ///
    /// Each entry is "table.column" (e.g., "users.invited_by_id").
    /// These edges are deferred (INSERT NULL, UPDATE later) instead of
    /// being resolved by the automatic Tarjan SCC heuristic.
    pub break_cycle_at: Vec<String>,
}

/// Read and parse a seedkit.toml file from the given directory.
///
/// Returns `None` if the file doesn't exist (config is optional).
/// Returns an error if the file exists but can't be parsed.
pub fn read_config(dir: &Path) -> Result<Option<SeedKitConfig>> {
    let path = dir.join(CONFIG_FILE_NAME);
    if !path.exists() {
        return Ok(None);
    }

    let content = std::fs::read_to_string(&path).map_err(|e| SeedKitError::Config {
        message: format!("Failed to read {}: {}", path.display(), e),
    })?;

    let mut config: SeedKitConfig = toml::from_str(&content).map_err(|e| SeedKitError::Config {
        message: format!("Failed to parse {}: {}", path.display(), e),
    })?;

    // Capture the absolute path to the config directory so that relative paths
    // in custom provider fields resolve against the config's location, not CWD.
    config.config_dir = Some(std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf()));

    // Validate semantic constraints that serde can't enforce.
    config.validate()?;

    Ok(Some(config))
}

impl SeedKitConfig {
    /// Build table_row_overrides from the [tables] section.
    pub fn table_row_overrides(&self) -> BTreeMap<String, usize> {
        let mut map = BTreeMap::new();
        for (name, tc) in &self.tables {
            if let Some(rows) = tc.rows {
                map.insert(name.clone(), rows);
            }
        }
        map
    }

    /// Parse break_cycle_at entries into (table, column) pairs.
    ///
    /// Malformed entries (missing the `table.column` dot separator) are logged
    /// as a warning via `tracing::warn` and skipped, so developers see exactly
    /// which config line is being ignored instead of silently losing it.
    pub fn cycle_break_edges(&self) -> Vec<(String, String)> {
        self.graph
            .break_cycle_at
            .iter()
            .filter_map(|entry| {
                let parts: Vec<&str> = entry.splitn(2, '.').collect();
                if parts.len() == 2 {
                    Some((parts[0].to_string(), parts[1].to_string()))
                } else {
                    tracing::warn!(
                        "Invalid graph.break_cycle_at entry: '{}'. \
                         Expected format 'table.column'. Ignoring.",
                        entry
                    );
                    None
                }
            })
            .collect()
    }

    /// Validate semantic constraints that serde cannot enforce.
    ///
    /// Call this immediately after parsing. Catches configuration mistakes
    /// (e.g., mismatched `values`/`weights` lengths) before any expensive
    /// database introspection runs.
    pub fn validate(&self) -> Result<()> {
        for (key, col_cfg) in &self.columns {
            if let Some(ref weights) = col_cfg.weights {
                match col_cfg.values {
                    Some(ref values) => {
                        if values.len() != weights.len() {
                            return Err(SeedKitError::Config {
                                message: format!(
                                    "Column '{}': weights has {} entries but values has {} entries. \
                                     They must be the same length.",
                                    key,
                                    weights.len(),
                                    values.len(),
                                ),
                            });
                        }
                    }
                    None => {
                        return Err(SeedKitError::Config {
                            message: format!(
                                "Column '{}': weights provided without values. \
                                 Add a matching values list or remove the weights.",
                                key,
                            ),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Validate column overrides against the introspected schema.
    ///
    /// Returns a list of warning messages for overrides that reference
    /// tables or columns not present in the schema. Called after introspection
    /// so developers catch stale config entries early.
    pub fn validate_against_schema(
        &self,
        schema: &crate::schema::types::DatabaseSchema,
    ) -> Vec<String> {
        let mut warnings = Vec::new();
        for key in self.columns.keys() {
            if let Some((table, col)) = key.split_once('.') {
                if let Some(table_def) = schema.tables.get(table) {
                    if !table_def.columns.contains_key(col) {
                        warnings.push(format!(
                            "seedkit.toml: [columns.\"{}\"] references column '{}' \
                             which does not exist in table '{}'",
                            key, col, table
                        ));
                    }
                } else {
                    warnings.push(format!(
                        "seedkit.toml: [columns.\"{}\"] references table '{}' \
                         which does not exist in schema",
                        key, table
                    ));
                }
            } else {
                warnings.push(format!(
                    "seedkit.toml: [columns.\"{}\"] is not in 'table.column' format",
                    key
                ));
            }
        }
        warnings
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_full_config() {
        let toml = r#"
[database]
url = "postgres://localhost/myapp"
schema = "public"

[generate]
rows = 500
seed = 42
ai = false

[tables.users]
rows = 1000

[tables.orders]
rows = 5000

[columns."products.color"]
values = ["red", "blue", "green"]
weights = [0.5, 0.3, 0.2]

[columns."orders.tax_code"]
custom = "./scripts/tax_gen.js"

[graph]
break_cycle_at = ["users.invited_by_id", "comments.parent_id"]
"#;

        let config: SeedKitConfig = toml::from_str(toml).unwrap();

        assert_eq!(
            config.database.url.as_deref(),
            Some("postgres://localhost/myapp")
        );
        assert_eq!(config.database.schema.as_deref(), Some("public"));
        assert_eq!(config.generate.rows, Some(500));
        assert_eq!(config.generate.seed, Some(42));
        assert_eq!(config.generate.ai, Some(false));
        assert_eq!(config.tables["users"].rows, Some(1000));
        assert_eq!(config.tables["orders"].rows, Some(5000));

        let colors = config.columns["products.color"].values.as_ref().unwrap();
        assert_eq!(colors, &["red", "blue", "green"]);

        let weights = config.columns["products.color"].weights.as_ref().unwrap();
        assert_eq!(weights.len(), 3);

        assert_eq!(
            config.columns["orders.tax_code"].custom.as_deref(),
            Some("./scripts/tax_gen.js")
        );

        assert_eq!(config.graph.break_cycle_at.len(), 2);
    }

    #[test]
    fn test_parse_empty_config() {
        let toml = "";
        let config: SeedKitConfig = toml::from_str(toml).unwrap();

        assert!(config.database.url.is_none());
        assert!(config.generate.rows.is_none());
        assert!(config.tables.is_empty());
        assert!(config.columns.is_empty());
        assert!(config.graph.break_cycle_at.is_empty());
    }

    #[test]
    fn test_parse_minimal_config() {
        let toml = r#"
[database]
url = "sqlite://dev.db"
"#;

        let config: SeedKitConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.database.url.as_deref(), Some("sqlite://dev.db"));
        assert!(config.generate.rows.is_none());
    }

    #[test]
    fn test_table_row_overrides() {
        let toml = r#"
[tables.users]
rows = 1000

[tables.orders]
rows = 5000

[tables.products]
"#;

        let config: SeedKitConfig = toml::from_str(toml).unwrap();
        let overrides = config.table_row_overrides();

        assert_eq!(overrides.len(), 2);
        assert_eq!(overrides["users"], 1000);
        assert_eq!(overrides["orders"], 5000);
    }

    #[test]
    fn test_cycle_break_edges() {
        let toml = r#"
[graph]
break_cycle_at = ["users.invited_by_id", "comments.parent_id", "invalid"]
"#;

        let config: SeedKitConfig = toml::from_str(toml).unwrap();
        let edges = config.cycle_break_edges();

        // "invalid" has no dot separator, so it's filtered out
        assert_eq!(edges.len(), 2);
        assert_eq!(edges[0], ("users".to_string(), "invited_by_id".to_string()));
        assert_eq!(edges[1], ("comments".to_string(), "parent_id".to_string()));
    }

    #[test]
    fn test_read_config_nonexistent() {
        let result = read_config(Path::new("/nonexistent/dir"));
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_read_config_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("seedkit.toml");
        std::fs::write(
            &config_path,
            r#"
[database]
url = "postgres://localhost/test"

[generate]
rows = 200
"#,
        )
        .unwrap();

        let result = read_config(dir.path()).unwrap();
        assert!(result.is_some());
        let config = result.unwrap();
        assert_eq!(
            config.database.url.as_deref(),
            Some("postgres://localhost/test")
        );
        assert_eq!(config.generate.rows, Some(200));
    }

    #[test]
    fn test_read_config_invalid_toml() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("seedkit.toml");
        std::fs::write(&config_path, "this is not valid [[[toml").unwrap();

        let result = read_config(dir.path());
        assert!(result.is_err());
    }

    // --- Fix 1: cycle_break_edges should warn on malformed entries ---

    #[test]
    fn test_cycle_break_edges_malformed_entry_still_parses_valid() {
        // "invalid" has no dot separator — should be warned about and filtered out.
        // Valid entries should still be returned.
        let toml = r#"
[graph]
break_cycle_at = ["users.invited_by_id", "no_dot_here", "comments.parent_id"]
"#;
        let config: SeedKitConfig = toml::from_str(toml).unwrap();
        let edges = config.cycle_break_edges();

        assert_eq!(edges.len(), 2);
        assert_eq!(edges[0], ("users".to_string(), "invited_by_id".to_string()));
        assert_eq!(edges[1], ("comments".to_string(), "parent_id".to_string()));
        // The tracing::warn is verified by inspection — tests confirm behavior doesn't change.
    }

    // --- Fix 2: config_dir captured for relative path resolution ---

    #[test]
    fn test_config_dir_captured_on_read() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("seedkit.toml");
        std::fs::write(
            &config_path,
            r#"
[columns."orders.tax_code"]
custom = "./scripts/tax_gen.js"
"#,
        )
        .unwrap();

        let config = read_config(dir.path()).unwrap().unwrap();

        // canonicalize expected path too — macOS symlinks /var → /private/var
        let expected = std::fs::canonicalize(dir.path()).unwrap();
        assert_eq!(config.config_dir.as_deref(), Some(expected.as_path()));
    }

    // --- Fix 3: validate() catches mismatched values/weights ---

    #[test]
    fn test_validate_matching_values_weights() {
        let toml = r#"
[columns."products.color"]
values = ["red", "blue", "green"]
weights = [0.5, 0.3, 0.2]
"#;
        let config: SeedKitConfig = toml::from_str(toml).unwrap();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_mismatched_values_weights_fails() {
        let toml = r#"
[columns."products.color"]
values = ["red", "blue", "green", "black", "white"]
weights = [0.25, 0.20, 0.20, 0.20]
"#;
        let config: SeedKitConfig = toml::from_str(toml).unwrap();
        let err = config.validate();
        assert!(err.is_err());
        let msg = format!("{}", err.unwrap_err());
        assert!(
            msg.contains("products.color"),
            "Error should name the column: {}",
            msg
        );
        assert!(
            msg.contains("5"),
            "Error should mention values count: {}",
            msg
        );
        assert!(
            msg.contains("4"),
            "Error should mention weights count: {}",
            msg
        );
    }

    #[test]
    fn test_validate_weights_without_values_fails() {
        let toml = r#"
[columns."orders.status"]
weights = [0.5, 0.3, 0.2]
"#;
        let config: SeedKitConfig = toml::from_str(toml).unwrap();
        let err = config.validate();
        assert!(err.is_err());
        let msg = format!("{}", err.unwrap_err());
        assert!(
            msg.contains("orders.status"),
            "Error should name the column: {}",
            msg
        );
    }

    #[test]
    fn test_validate_values_without_weights_ok() {
        // values without weights is fine — uniform distribution
        let toml = r#"
[columns."products.color"]
values = ["red", "blue", "green"]
"#;
        let config: SeedKitConfig = toml::from_str(toml).unwrap();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_no_columns_ok() {
        let toml = r#"
[generate]
rows = 100
"#;
        let config: SeedKitConfig = toml::from_str(toml).unwrap();
        assert!(config.validate().is_ok());
    }

    // --- Schema validation tests ---

    #[test]
    fn test_validate_against_schema_valid_override() {
        use crate::schema::types::*;

        let toml = r#"
[columns."users.email"]
values = ["a@b.com", "c@d.com"]
"#;
        let config: SeedKitConfig = toml::from_str(toml).unwrap();

        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());
        let mut users = Table::new("users".to_string());
        users.columns.insert(
            "email".to_string(),
            Column::new(
                "email".to_string(),
                DataType::VarChar,
                "varchar".to_string(),
            ),
        );
        schema.tables.insert("users".to_string(), users);

        let warnings = config.validate_against_schema(&schema);
        assert!(
            warnings.is_empty(),
            "Valid override should produce no warnings: {:?}",
            warnings
        );
    }

    #[test]
    fn test_validate_against_schema_nonexistent_table() {
        use crate::schema::types::*;

        let toml = r#"
[columns."ghost.name"]
values = ["x"]
"#;
        let config: SeedKitConfig = toml::from_str(toml).unwrap();
        let schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());

        let warnings = config.validate_against_schema(&schema);
        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0].contains("ghost"),
            "Should mention table: {}",
            warnings[0]
        );
    }

    #[test]
    fn test_validate_against_schema_nonexistent_column() {
        use crate::schema::types::*;

        let toml = r#"
[columns."users.nonexistent"]
values = ["x"]
"#;
        let config: SeedKitConfig = toml::from_str(toml).unwrap();

        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());
        let mut users = Table::new("users".to_string());
        users.columns.insert(
            "email".to_string(),
            Column::new(
                "email".to_string(),
                DataType::VarChar,
                "varchar".to_string(),
            ),
        );
        schema.tables.insert("users".to_string(), users);

        let warnings = config.validate_against_schema(&schema);
        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0].contains("nonexistent"),
            "Should mention column: {}",
            warnings[0]
        );
    }
}
