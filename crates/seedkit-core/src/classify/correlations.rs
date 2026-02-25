use std::collections::HashMap;

use crate::classify::semantic::{CorrelationGroup, SemanticType};

/// A detected group of columns that should be generated together.
#[derive(Debug, Clone)]
pub struct DetectedCorrelation {
    pub group: CorrelationGroup,
    pub table_name: String,
    pub columns: Vec<(String, SemanticType)>,
}

/// Detect correlation groups within a schema's classified columns.
pub fn detect_correlations(
    classifications: &HashMap<(String, String), SemanticType>,
) -> Vec<DetectedCorrelation> {
    // Group by (table_name, correlation_group)
    let mut groups: HashMap<(String, CorrelationGroup), Vec<(String, SemanticType)>> =
        HashMap::new();

    for ((table_name, col_name), semantic_type) in classifications {
        if let Some(group) = semantic_type.correlation_group() {
            groups
                .entry((table_name.clone(), group))
                .or_default()
                .push((col_name.clone(), *semantic_type));
        }
    }

    // Only return groups with 2+ columns (a single correlated column isn't meaningful)
    groups
        .into_iter()
        .filter(|(_, cols)| cols.len() >= 2)
        .map(|((table_name, group), columns)| DetectedCorrelation {
            group,
            table_name,
            columns,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_address_correlation() {
        let mut classifications = HashMap::new();
        classifications.insert(
            ("users".to_string(), "street".to_string()),
            SemanticType::StreetAddress,
        );
        classifications.insert(
            ("users".to_string(), "city".to_string()),
            SemanticType::City,
        );
        classifications.insert(
            ("users".to_string(), "state".to_string()),
            SemanticType::State,
        );
        classifications.insert(
            ("users".to_string(), "zip".to_string()),
            SemanticType::ZipCode,
        );

        let correlations = detect_correlations(&classifications);

        assert_eq!(correlations.len(), 1);
        assert_eq!(correlations[0].group, CorrelationGroup::Address);
        assert_eq!(correlations[0].columns.len(), 4);
    }

    #[test]
    fn test_detect_person_identity() {
        let mut classifications = HashMap::new();
        classifications.insert(
            ("users".to_string(), "first_name".to_string()),
            SemanticType::FirstName,
        );
        classifications.insert(
            ("users".to_string(), "last_name".to_string()),
            SemanticType::LastName,
        );
        classifications.insert(
            ("users".to_string(), "email".to_string()),
            SemanticType::Email,
        );

        let correlations = detect_correlations(&classifications);

        assert_eq!(correlations.len(), 1);
        assert_eq!(correlations[0].group, CorrelationGroup::PersonIdentity);
    }

    #[test]
    fn test_single_column_not_correlated() {
        let mut classifications = HashMap::new();
        classifications.insert(
            ("users".to_string(), "city".to_string()),
            SemanticType::City,
        );

        let correlations = detect_correlations(&classifications);
        assert!(correlations.is_empty());
    }
}
