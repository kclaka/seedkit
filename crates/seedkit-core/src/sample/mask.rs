//! PII masking for smart sampling.
//!
//! When extracting distribution profiles from production databases,
//! categorical distributions for PII columns (email, phone, SSN, etc.)
//! contain actual user data. This module detects PII columns and removes
//! their categorical distributions so the engine falls back to semantic
//! generation instead of using real production values.
//!
//! Numeric aggregates (min, max, mean, stddev) are safe because they
//! don't contain individual values.

use super::stats::{ColumnDistribution, DistributionProfile};

/// Known PII column name patterns.
///
/// These patterns match common column naming conventions for personally
/// identifiable information across different naming styles (snake_case,
/// camelCase, etc.). The check is case-insensitive and matches substrings.
const PII_PATTERNS: &[&str] = &[
    // Identity
    "email",
    "e_mail",
    "first_name",
    "firstname",
    "last_name",
    "lastname",
    "full_name",
    "fullname",
    "middle_name",
    "middlename",
    "surname",
    "given_name",
    "givenname",
    "family_name",
    "familyname",
    // Contact
    "phone",
    "mobile",
    "telephone",
    "fax",
    "cell_number",
    "contact_number",
    // Address
    "street",
    "address",
    "address_line",
    "city",
    "zip_code",
    "zipcode",
    "postal_code",
    "postalcode",
    // Government IDs
    "ssn",
    "social_security",
    "tax_id",
    "taxid",
    "national_id",
    "passport",
    "driver_license",
    "drivers_license",
    // Financial
    "credit_card",
    "creditcard",
    "card_number",
    "account_number",
    "routing_number",
    "iban",
    "bank_account",
    // Auth
    "password",
    "passwd",
    "password_hash",
    "pass_hash",
    "secret",
    "api_key",
    "apikey",
    "token",
    "auth_token",
    "refresh_token",
    "access_token",
    // Other
    "date_of_birth",
    "dob",
    "birthdate",
    "ip_address",
    "ipaddress",
    "user_agent",
    "useragent",
    "mac_address",
];

/// Check if a column name matches known PII patterns.
///
/// The check is case-insensitive and uses substring matching.
pub fn is_pii_column(name: &str) -> bool {
    let lower = name.to_lowercase();
    PII_PATTERNS.iter().any(|pattern| lower.contains(pattern))
}

/// Mask PII columns in distribution profiles.
///
/// Removes `Categorical` distributions for columns that match PII patterns,
/// since those distributions would contain actual production PII values.
/// `Numeric` distributions are kept because aggregates (min/max/mean/stddev)
/// don't reveal individual values. `Ratio` distributions are always safe.
///
/// Returns the number of distributions masked.
pub fn mask_pii_distributions(profiles: &mut [DistributionProfile]) -> usize {
    let mut masked_count = 0;
    for profile in profiles.iter_mut() {
        let keys_to_remove: Vec<String> = profile
            .column_distributions
            .iter()
            .filter(|(col_name, dist)| {
                is_pii_column(col_name) && matches!(dist, ColumnDistribution::Categorical { .. })
            })
            .map(|(col_name, _)| col_name.clone())
            .collect();

        for key in keys_to_remove {
            profile.column_distributions.remove(&key);
            masked_count += 1;
        }
    }
    masked_count
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_pii_detection_matches_email() {
        assert!(is_pii_column("email"));
        assert!(is_pii_column("user_email"));
        assert!(is_pii_column("Email"));
        assert!(is_pii_column("EMAIL_ADDRESS"));
        assert!(is_pii_column("e_mail"));
    }

    #[test]
    fn test_pii_detection_matches_phone() {
        assert!(is_pii_column("phone"));
        assert!(is_pii_column("phone_number"));
        assert!(is_pii_column("mobile"));
        assert!(is_pii_column("telephone"));
    }

    #[test]
    fn test_pii_detection_matches_ssn() {
        assert!(is_pii_column("ssn"));
        assert!(is_pii_column("social_security_number"));
        assert!(is_pii_column("SSN"));
    }

    #[test]
    fn test_pii_detection_matches_password() {
        assert!(is_pii_column("password"));
        assert!(is_pii_column("password_hash"));
        assert!(is_pii_column("api_key"));
        assert!(is_pii_column("secret"));
    }

    #[test]
    fn test_pii_detection_skips_product_name() {
        assert!(!is_pii_column("product_name"));
        assert!(!is_pii_column("name")); // "name" alone is too generic to flag
        assert!(!is_pii_column("category"));
        assert!(!is_pii_column("status"));
        assert!(!is_pii_column("price"));
        assert!(!is_pii_column("quantity"));
        assert!(!is_pii_column("description"));
        assert!(!is_pii_column("created_at"));
    }

    #[test]
    fn test_mask_categorical_pii_column() {
        let mut profiles = vec![DistributionProfile {
            table_name: "users".to_string(),
            row_count: 1000,
            column_distributions: {
                let mut m = HashMap::new();
                m.insert(
                    "email".to_string(),
                    ColumnDistribution::Categorical {
                        values: vec![
                            ("alice@example.com".to_string(), 0.01),
                            ("bob@example.com".to_string(), 0.01),
                        ],
                    },
                );
                m.insert(
                    "status".to_string(),
                    ColumnDistribution::Categorical {
                        values: vec![("active".to_string(), 0.7), ("inactive".to_string(), 0.3)],
                    },
                );
                m
            },
        }];

        let masked = mask_pii_distributions(&mut profiles);
        assert_eq!(masked, 1, "should mask email but not status");
        assert!(
            !profiles[0].column_distributions.contains_key("email"),
            "email distribution should be removed"
        );
        assert!(
            profiles[0].column_distributions.contains_key("status"),
            "status distribution should remain"
        );
    }

    #[test]
    fn test_mask_numeric_safe() {
        let mut profiles = vec![DistributionProfile {
            table_name: "users".to_string(),
            row_count: 1000,
            column_distributions: {
                let mut m = HashMap::new();
                // Numeric distribution for a PII-like column name
                m.insert(
                    "phone".to_string(),
                    ColumnDistribution::Numeric {
                        min: 1000000000.0,
                        max: 9999999999.0,
                        mean: 5000000000.0,
                        stddev: 2000000000.0,
                    },
                );
                m
            },
        }];

        let masked = mask_pii_distributions(&mut profiles);
        assert_eq!(masked, 0, "numeric distributions should not be masked");
        assert!(
            profiles[0].column_distributions.contains_key("phone"),
            "numeric phone distribution should remain"
        );
    }

    #[test]
    fn test_mask_multiple_pii_columns() {
        let mut profiles = vec![DistributionProfile {
            table_name: "users".to_string(),
            row_count: 500,
            column_distributions: {
                let mut m = HashMap::new();
                m.insert(
                    "email".to_string(),
                    ColumnDistribution::Categorical {
                        values: vec![("a@b.com".to_string(), 1.0)],
                    },
                );
                m.insert(
                    "first_name".to_string(),
                    ColumnDistribution::Categorical {
                        values: vec![("Alice".to_string(), 0.5)],
                    },
                );
                m.insert(
                    "password_hash".to_string(),
                    ColumnDistribution::Categorical {
                        values: vec![("$2b$10$...".to_string(), 0.01)],
                    },
                );
                m.insert(
                    "role".to_string(),
                    ColumnDistribution::Categorical {
                        values: vec![("admin".to_string(), 0.1), ("user".to_string(), 0.9)],
                    },
                );
                m
            },
        }];

        let masked = mask_pii_distributions(&mut profiles);
        assert_eq!(masked, 3, "should mask email, first_name, password_hash");
        assert_eq!(profiles[0].column_distributions.len(), 1);
        assert!(profiles[0].column_distributions.contains_key("role"));
    }
}
