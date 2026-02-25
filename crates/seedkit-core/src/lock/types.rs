use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::classify::semantic::SemanticType;
use crate::schema::types::DatabaseSchema;

/// The seedkit.lock file structure for deterministic reproducibility.
///
/// When `seedkit generate` runs, it writes this lock file. A teammate
/// can run `seedkit generate --from-lock` to reproduce the exact same
/// dataset. If the schema has changed, SeedKit warns and requires
/// `--force` to regenerate.
///
/// # Merge conflicts
///
/// Treat `seedkit.lock` exactly like `package-lock.json`. If there is a
/// merge conflict, **do not** attempt to resolve it by hand. Instead:
///
/// ```bash
/// git checkout --ours seedkit.lock
/// seedkit generate --force
/// ```
///
/// This regenerates a clean lock file from the current schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockFile {
    /// SHA256 hash of the introspected schema (serialized JSON).
    pub schema_hash: String,
    /// Random seed used for generation.
    pub seed: u64,
    /// SeedKit version that generated this lock file.
    pub seedkit_version: String,
    /// Generation configuration.
    pub config: LockConfig,
    /// Full schema snapshot for detailed drift detection.
    ///
    /// Stored so that `seedkit check` can produce actionable, column-level
    /// drift reports (new tables, removed columns, type changes) instead of
    /// a bare "schema hash changed" boolean.
    pub schema_snapshot: DatabaseSchema,
    /// Pinned base timestamp for deterministic temporal generation.
    /// Stored as ISO 8601 string (e.g., "2025-06-15T12:00:00").
    pub base_time: String,
    /// Timestamp when the lock file was created.
    pub created_at: String,
}

/// Configuration captured in the lock file for reproducible generation.
///
/// Uses `BTreeMap` (not `HashMap`) for `table_row_overrides` to guarantee
/// deterministic alphabetical key ordering in the serialized JSON. This
/// ensures `seedkit.lock` diffs are clean and byte-for-byte reproducible.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockConfig {
    /// Default row count per table.
    pub default_row_count: usize,
    /// Per-table row count overrides. BTreeMap for deterministic JSON ordering.
    pub table_row_overrides: BTreeMap<String, usize>,
    /// Whether AI was used.
    pub ai_enabled: bool,
    /// Tables that were included (empty = all).
    pub include_tables: Vec<String>,
    /// Tables that were excluded.
    pub exclude_tables: Vec<String>,
    /// Cached AI classifications from the `--ai` pass.
    ///
    /// When `--ai` is used, the LLM's final merged classification results are
    /// stored here so that `--from-lock` can restore them without re-querying
    /// the API. LLMs are non-deterministic — the same schema could produce
    /// slightly different classifications on a second call, silently breaking
    /// downstream reproducibility.
    ///
    /// Structure: table_name → (column_name → SemanticType).
    /// `None` when AI was not used. BTreeMap for deterministic JSON ordering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ai_classifications: Option<BTreeMap<String, BTreeMap<String, SemanticType>>>,

    /// Custom column value lists captured from seedkit.toml at generation time.
    ///
    /// Stored so that `--from-lock` can reproduce value list configurations
    /// even if seedkit.toml has changed since the lock was created.
    /// BTreeMap for deterministic JSON ordering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_overrides: Option<BTreeMap<String, ColumnOverrideLock>>,
}

/// Column value override captured in the lock file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnOverrideLock {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weights: Option<Vec<f64>>,
}

impl LockFile {
    pub fn new(
        schema_hash: String,
        seed: u64,
        base_time: chrono::NaiveDateTime,
        config: LockConfig,
        schema: DatabaseSchema,
    ) -> Self {
        Self {
            schema_hash,
            seed,
            seedkit_version: env!("CARGO_PKG_VERSION").to_string(),
            config,
            schema_snapshot: schema,
            base_time: base_time.format("%Y-%m-%dT%H:%M:%S").to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
        }
    }

    /// Parse the stored base_time back to NaiveDateTime.
    pub fn parse_base_time(&self) -> Option<chrono::NaiveDateTime> {
        chrono::NaiveDateTime::parse_from_str(&self.base_time, "%Y-%m-%dT%H:%M:%S").ok()
    }
}
