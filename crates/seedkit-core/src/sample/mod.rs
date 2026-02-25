//! # Smart Sampling
//!
//! Connects to a production read-only replica and extracts statistical
//! distributions (value frequencies, numeric ranges, row count ratios)
//! without copying actual data. The generation engine then uses these
//! distributions to produce synthetic data that mirrors production patterns.

use std::path::Path;

use crate::error::{Result, SeedKitError};

pub mod mask;
pub mod stats;

/// Default filename for saved distribution profiles.
pub const PROFILES_FILE_NAME: &str = "seedkit.distributions.json";

/// Save distribution profiles to a JSON file.
pub fn save_profiles(profiles: &[stats::DistributionProfile], path: &Path) -> Result<()> {
    let json = serde_json::to_string_pretty(profiles).map_err(|e| SeedKitError::Config {
        message: format!("Failed to serialize distribution profiles: {}", e),
    })?;
    std::fs::write(path, json).map_err(|e| SeedKitError::Output {
        message: format!("Failed to write profiles to {}", path.display()),
        source: e,
    })?;
    Ok(())
}

/// Load distribution profiles from a JSON file.
pub fn load_profiles(path: &Path) -> Result<Vec<stats::DistributionProfile>> {
    let contents = std::fs::read_to_string(path).map_err(|e| SeedKitError::Output {
        message: format!("Failed to read profiles from {}", path.display()),
        source: e,
    })?;
    let profiles: Vec<stats::DistributionProfile> =
        serde_json::from_str(&contents).map_err(|e| SeedKitError::Config {
            message: format!("Failed to parse distribution profiles: {}", e),
        })?;
    Ok(profiles)
}

#[cfg(test)]
mod tests {
    use super::*;
    use stats::{ColumnDistribution, DistributionProfile};
    use std::collections::HashMap;

    #[test]
    fn test_profiles_save_load_round_trip() {
        let profiles = vec![
            DistributionProfile {
                table_name: "users".to_string(),
                row_count: 1000,
                column_distributions: {
                    let mut m = HashMap::new();
                    m.insert(
                        "status".to_string(),
                        ColumnDistribution::Categorical {
                            values: vec![
                                ("active".to_string(), 0.7),
                                ("inactive".to_string(), 0.2),
                                ("suspended".to_string(), 0.1),
                            ],
                        },
                    );
                    m.insert(
                        "age".to_string(),
                        ColumnDistribution::Numeric {
                            min: 18.0,
                            max: 90.0,
                            mean: 35.0,
                            stddev: 12.5,
                        },
                    );
                    m
                },
            },
            DistributionProfile {
                table_name: "orders".to_string(),
                row_count: 3200,
                column_distributions: {
                    let mut m = HashMap::new();
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

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_profiles.json");

        save_profiles(&profiles, &path).unwrap();
        let loaded = load_profiles(&path).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].table_name, "users");
        assert_eq!(loaded[0].row_count, 1000);
        assert_eq!(loaded[0].column_distributions.len(), 2);
        assert_eq!(loaded[1].table_name, "orders");
        assert_eq!(loaded[1].row_count, 3200);
    }

    #[test]
    fn test_load_nonexistent_file_errors() {
        let result = load_profiles(Path::new("/nonexistent/path.json"));
        assert!(result.is_err());
    }

    #[test]
    fn test_save_load_empty_profiles() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.json");

        save_profiles(&[], &path).unwrap();
        let loaded = load_profiles(&path).unwrap();
        assert!(loaded.is_empty());
    }
}
