//! # Lock File
//!
//! The `seedkit.lock` file stores the full generation config, random seed,
//! and a complete schema snapshot so that teammates can reproduce the exact
//! same dataset with `seedkit generate --from-lock`.
//!
//! ## Merge conflicts
//!
//! Treat `seedkit.lock` exactly like `package-lock.json` — it is a
//! machine-generated artifact, **not** a hand-editable config file.
//! If you hit a merge conflict:
//!
//! ```bash
//! git checkout --ours seedkit.lock
//! seedkit generate --force
//! ```
//!
//! This regenerates a clean lock file from the current schema. Never
//! attempt to merge lock file JSON by hand.

pub mod types;

use std::fs;
use std::path::Path;

use self::types::LockFile;
use crate::error::{Result, SeedKitError};

/// Default lock file name.
pub const LOCK_FILE_NAME: &str = "seedkit.lock";

/// Write a lock file to disk atomically.
///
/// Writes to a temporary file in the same directory, then renames it
/// into place. `rename` is atomic on POSIX and Windows, so a crash or
/// Ctrl+C mid-write leaves the original lock file intact instead of
/// producing a corrupted half-written file.
pub fn write_lock_file(lock: &LockFile, path: &Path) -> Result<()> {
    use std::io::Write;

    let json = serde_json::to_string_pretty(lock).map_err(|e| SeedKitError::LockFile {
        message: format!("Failed to serialize lock file: {}", e),
    })?;

    let dir = path.parent().unwrap_or(Path::new("."));
    let tmp_path = dir.join(".seedkit.lock.tmp");

    // Write to temp file, flush to disk
    let mut file = fs::File::create(&tmp_path).map_err(|e| SeedKitError::Output {
        message: format!("Failed to create temp lock file at {}", tmp_path.display()),
        source: e,
    })?;
    file.write_all(json.as_bytes())
        .map_err(|e| SeedKitError::Output {
            message: format!("Failed to write temp lock file at {}", tmp_path.display()),
            source: e,
        })?;
    file.sync_all().map_err(|e| SeedKitError::Output {
        message: "Failed to sync lock file to disk".to_string(),
        source: e,
    })?;

    // Atomic rename into place
    fs::rename(&tmp_path, path).map_err(|e| SeedKitError::Output {
        message: format!(
            "Failed to rename {} → {}",
            tmp_path.display(),
            path.display()
        ),
        source: e,
    })?;

    Ok(())
}

/// Read a lock file from disk.
pub fn read_lock_file(path: &Path) -> Result<LockFile> {
    let content = fs::read_to_string(path).map_err(|e| SeedKitError::Output {
        message: format!("Failed to read lock file from {}", path.display()),
        source: e,
    })?;
    let lock: LockFile = serde_json::from_str(&content).map_err(|e| SeedKitError::LockFile {
        message: format!("Failed to parse lock file: {}", e),
    })?;
    Ok(lock)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lock::types::LockConfig;
    use crate::schema::types::{DatabaseSchema, DatabaseType};
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    fn dummy_schema() -> DatabaseSchema {
        DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string())
    }

    fn make_lock() -> LockFile {
        let bt = chrono::NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(2025, 6, 15).unwrap(),
            chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        );
        LockFile::new(
            "abc123".to_string(),
            42,
            bt,
            LockConfig {
                default_row_count: 100,
                table_row_overrides: BTreeMap::new(),
                ai_enabled: false,
                include_tables: Vec::new(),
                exclude_tables: Vec::new(),
                ai_classifications: None,
                column_overrides: None,
            },
            dummy_schema(),
        )
    }

    #[test]
    fn test_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seedkit.lock");

        let lock = make_lock();
        write_lock_file(&lock, &path).unwrap();
        let loaded = read_lock_file(&path).unwrap();

        assert_eq!(loaded.schema_hash, "abc123");
        assert_eq!(loaded.seed, 42);
        assert_eq!(loaded.config.default_row_count, 100);
    }

    #[test]
    fn test_base_time_round_trip() {
        let lock = make_lock();
        let parsed = lock.parse_base_time().unwrap();
        assert_eq!(parsed.format("%Y-%m-%d").to_string(), "2025-06-15");
        assert_eq!(parsed.format("%H:%M:%S").to_string(), "12:00:00");
    }

    #[test]
    fn test_read_nonexistent_file() {
        let result = read_lock_file(Path::new("/nonexistent/seedkit.lock"));
        assert!(result.is_err());
    }

    #[test]
    fn test_atomic_write_no_tmp_left_behind() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seedkit.lock");
        let tmp_path = dir.path().join(".seedkit.lock.tmp");

        let lock = make_lock();
        write_lock_file(&lock, &path).unwrap();

        // The final file must exist and the temp file must be gone
        assert!(path.exists(), "Lock file should exist after write");
        assert!(
            !tmp_path.exists(),
            "Temp file should be cleaned up after rename"
        );
    }

    #[test]
    fn test_atomic_write_overwrites_existing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seedkit.lock");

        // Write initial lock file
        let lock1 = make_lock();
        write_lock_file(&lock1, &path).unwrap();

        // Overwrite with different seed
        let bt = chrono::NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(2025, 6, 15).unwrap(),
            chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        );
        let lock2 = LockFile::new(
            "def456".to_string(),
            99,
            bt,
            LockConfig {
                default_row_count: 200,
                table_row_overrides: BTreeMap::new(),
                ai_enabled: false,
                include_tables: Vec::new(),
                exclude_tables: Vec::new(),
                ai_classifications: None,
                column_overrides: None,
            },
            dummy_schema(),
        );
        write_lock_file(&lock2, &path).unwrap();

        let loaded = read_lock_file(&path).unwrap();
        assert_eq!(loaded.schema_hash, "def456");
        assert_eq!(loaded.seed, 99);
    }

    #[test]
    fn test_btreemap_deterministic_serialization() {
        // Insert keys in different orders — BTreeMap sorts alphabetically,
        // so both lock files must produce byte-for-byte identical JSON.
        let bt = chrono::NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(2025, 6, 15).unwrap(),
            chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        );

        let mut overrides_a = BTreeMap::new();
        overrides_a.insert("users".to_string(), 500);
        overrides_a.insert("orders".to_string(), 2000);
        overrides_a.insert("products".to_string(), 100);

        let mut overrides_b = BTreeMap::new();
        overrides_b.insert("products".to_string(), 100);
        overrides_b.insert("users".to_string(), 500);
        overrides_b.insert("orders".to_string(), 2000);

        let lock_a = LockFile::new(
            "hash".to_string(),
            42,
            bt,
            LockConfig {
                default_row_count: 100,
                table_row_overrides: overrides_a,
                ai_enabled: false,
                include_tables: Vec::new(),
                exclude_tables: Vec::new(),
                ai_classifications: None,
                column_overrides: None,
            },
            dummy_schema(),
        );
        let lock_b = LockFile::new(
            "hash".to_string(),
            42,
            bt,
            LockConfig {
                default_row_count: 100,
                table_row_overrides: overrides_b,
                ai_enabled: false,
                include_tables: Vec::new(),
                exclude_tables: Vec::new(),
                ai_classifications: None,
                column_overrides: None,
            },
            dummy_schema(),
        );

        let json_a = serde_json::to_string_pretty(&lock_a).unwrap();
        let json_b = serde_json::to_string_pretty(&lock_b).unwrap();

        // Ignore created_at (uses wall clock), compare everything else
        let strip_created_at = |s: &str| -> String {
            s.lines()
                .filter(|l| !l.contains("created_at"))
                .collect::<Vec<_>>()
                .join("\n")
        };
        assert_eq!(strip_created_at(&json_a), strip_created_at(&json_b));
    }

    #[test]
    fn test_ai_classifications_round_trip() {
        use crate::classify::semantic::SemanticType;

        let dir = tempdir().unwrap();
        let path = dir.path().join("seedkit.lock");

        let bt = chrono::NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(2025, 6, 15).unwrap(),
            chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        );

        // Build AI classifications: table → column → SemanticType
        let mut user_cols = BTreeMap::new();
        user_cols.insert("email".to_string(), SemanticType::Email);
        user_cols.insert("name".to_string(), SemanticType::FullName);

        let mut order_cols = BTreeMap::new();
        order_cols.insert("total".to_string(), SemanticType::Price);

        let mut classifications = BTreeMap::new();
        classifications.insert("users".to_string(), user_cols);
        classifications.insert("orders".to_string(), order_cols);

        let lock = LockFile::new(
            "abc".to_string(),
            42,
            bt,
            LockConfig {
                default_row_count: 100,
                table_row_overrides: BTreeMap::new(),
                ai_enabled: true,
                include_tables: Vec::new(),
                exclude_tables: Vec::new(),
                ai_classifications: Some(classifications),
                column_overrides: None,
            },
            dummy_schema(),
        );

        write_lock_file(&lock, &path).unwrap();
        let loaded = read_lock_file(&path).unwrap();

        assert!(loaded.config.ai_classifications.is_some());
        let loaded_cls = loaded.config.ai_classifications.unwrap();
        assert_eq!(loaded_cls.len(), 2);
        assert_eq!(loaded_cls["users"]["email"], SemanticType::Email,);
        assert_eq!(loaded_cls["users"]["name"], SemanticType::FullName,);
        assert_eq!(loaded_cls["orders"]["total"], SemanticType::Price,);
    }

    #[test]
    fn test_ai_classifications_none_when_not_used() {
        let lock = make_lock();
        assert!(lock.config.ai_classifications.is_none());
    }

    #[test]
    fn test_schema_snapshot_round_trip() {
        use crate::schema::types::*;

        let dir = tempdir().unwrap();
        let path = dir.path().join("seedkit.lock");

        let bt = chrono::NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(2025, 6, 15).unwrap(),
            chrono::NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        );

        // Build a schema with a table, columns, and a foreign key
        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "myapp".to_string());
        let mut users = Table::new("users".to_string());
        let mut id_col = Column::new("id".to_string(), DataType::Serial, "serial".to_string());
        id_col.nullable = false;
        id_col.is_auto_increment = true;
        users.columns.insert("id".to_string(), id_col);
        let mut email_col = Column::new(
            "email".to_string(),
            DataType::VarChar,
            "varchar(255)".to_string(),
        );
        email_col.nullable = false;
        users.columns.insert("email".to_string(), email_col);
        schema.tables.insert("users".to_string(), users);

        let lock = LockFile::new(
            "snapshot_hash".to_string(),
            42,
            bt,
            LockConfig {
                default_row_count: 100,
                table_row_overrides: BTreeMap::new(),
                ai_enabled: false,
                include_tables: Vec::new(),
                exclude_tables: Vec::new(),
                ai_classifications: None,
                column_overrides: None,
            },
            schema,
        );

        write_lock_file(&lock, &path).unwrap();
        let loaded = read_lock_file(&path).unwrap();

        // Verify the schema snapshot survived serialization
        assert_eq!(loaded.schema_snapshot.database_name, "myapp");
        assert_eq!(loaded.schema_snapshot.tables.len(), 1);
        let users_table = &loaded.schema_snapshot.tables["users"];
        assert_eq!(users_table.columns.len(), 2);
        assert_eq!(users_table.columns["email"].data_type, DataType::VarChar);
        assert!(!users_table.columns["email"].nullable);
    }
}
