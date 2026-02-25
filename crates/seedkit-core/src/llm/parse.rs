//! # LLM Response Parser
//!
//! Parses JSON classification responses from Claude or OpenAI and merges
//! them with the rule-based classification results. The merge strategy is
//! conservative: LLM overrides only apply to columns that the rule engine
//! classified as `Unknown`.

use std::collections::{BTreeMap, HashMap};

use serde::Deserialize;

use crate::classify::semantic::SemanticType;
use crate::error::{Result, SeedKitError};

/// A single column classification from the LLM response.
#[derive(Debug, Deserialize)]
struct LlmClassification {
    table: String,
    column: String,
    semantic_type: SemanticType,
    #[serde(default)]
    confidence: f64,
}

/// Parse the LLM's JSON response into a list of classifications.
///
/// Uses a two-stage deserialization strategy:
/// 1. Parse the JSON string into a `Vec<serde_json::Value>` (untyped).
/// 2. Attempt to parse each element individually into `LlmClassification`.
///
/// This "firebreak" ensures that a single hallucinated `semantic_type`
/// (e.g., `"UserEmail"` instead of `"Email"`) only drops that one row
/// instead of nuking the entire array.
fn parse_llm_response(response: &str) -> Result<Vec<LlmClassification>> {
    let json_str = extract_json_array(response);

    // Stage 1: Parse into untyped JSON array
    let raw_array: Vec<serde_json::Value> =
        serde_json::from_str(json_str).map_err(|e| SeedKitError::LlmError {
            message: format!(
                "Failed to parse LLM response as JSON array: {}. Response: {}",
                e,
                truncate(response, 200),
            ),
        })?;

    // Stage 2: Parse each object individually, dropping invalid rows
    let mut classifications = Vec::new();
    for raw_obj in raw_array {
        match serde_json::from_value::<LlmClassification>(raw_obj.clone()) {
            Ok(valid) => classifications.push(valid),
            Err(e) => {
                tracing::debug!(
                    "Dropped invalid LLM classification row: {}. Error: {}",
                    raw_obj,
                    e,
                );
            }
        }
    }

    Ok(classifications)
}

/// Extract the JSON array from a response that may contain markdown fences
/// or conversational wrapper text.
///
/// Uses a three-tier extraction strategy:
/// 1. Markdown code fences (`\`\`\`json ... \`\`\``) — most reliable
/// 2. Regex matching `[{...}]` — avoids binding to peripheral brackets
///    in conversational text like `[as requested]`
/// 3. Raw trimmed response — last resort
fn extract_json_array(response: &str) -> &str {
    let trimmed = response.trim();

    // Tier 1: Markdown code fences (most reliable)
    if let Some(start) = trimmed.find("```json") {
        let after_fence = &trimmed[start + 7..];
        if let Some(end) = after_fence.find("```") {
            return after_fence[..end].trim();
        }
    }
    if let Some(start) = trimmed.find("```") {
        let after_fence = &trimmed[start + 3..];
        if let Some(end) = after_fence.find("```") {
            return after_fence[..end].trim();
        }
    }

    // Tier 2: Regex — match `[` followed by `{`, any content, `}` then `]`.
    // This avoids the greedy bracket trap where `find('[')` binds to
    // peripheral brackets in conversational text.
    static ARRAY_RE: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"(?s)\[\s*\{.*\}\s*\]").unwrap());

    if let Some(mat) = ARRAY_RE.find(trimmed) {
        return mat.as_str();
    }

    trimmed
}

/// Merge LLM classifications with rule-based results.
///
/// The merge strategy is conservative:
/// - If the rule engine already assigned a specific type (not `Unknown`),
///   keep the rule-based result.
/// - If the rule engine classified as `Unknown` and the LLM has a
///   classification with confidence >= 0.5, use the LLM result.
pub fn merge_classifications(
    rule_based: &HashMap<(String, String), SemanticType>,
    llm_response: &str,
) -> HashMap<(String, String), SemanticType> {
    let mut merged = rule_based.clone();

    let llm_results = match parse_llm_response(llm_response) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("Failed to parse LLM response, using rule-based only: {}", e);
            return merged;
        }
    };

    for classification in llm_results {
        let key = (classification.table, classification.column);
        let existing = merged.get(&key).copied().unwrap_or(SemanticType::Unknown);

        if existing == SemanticType::Unknown && classification.confidence >= 0.5 {
            merged.insert(key, classification.semantic_type);
        }
    }

    merged
}

/// Build a BTreeMap of AI classifications suitable for storing in the lock file.
///
/// Filters to only include entries where the LLM changed the classification
/// (i.e., the rule engine had `Unknown`).
pub fn build_ai_classification_cache(
    rule_based: &HashMap<(String, String), SemanticType>,
    merged: &HashMap<(String, String), SemanticType>,
) -> BTreeMap<String, BTreeMap<String, SemanticType>> {
    let mut cache: BTreeMap<String, BTreeMap<String, SemanticType>> = BTreeMap::new();

    for ((table, column), semantic_type) in merged {
        let rule_type = rule_based.get(&(table.clone(), column.clone()));
        // Only cache entries where the LLM provided a new classification
        if rule_type.is_none_or(|rt| *rt == SemanticType::Unknown)
            && *semantic_type != SemanticType::Unknown
        {
            cache
                .entry(table.clone())
                .or_default()
                .insert(column.clone(), *semantic_type);
        }
    }

    cache
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        s
    } else {
        &s[..max]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bare_json_array() {
        let response = r#"[
            {"table": "users", "column": "bio", "semantic_type": "Bio", "confidence": 0.9}
        ]"#;

        let results = parse_llm_response(response).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].table, "users");
        assert_eq!(results[0].column, "bio");
        assert_eq!(results[0].semantic_type, SemanticType::Bio);
        assert!((results[0].confidence - 0.9).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_markdown_fenced_json() {
        let response = r#"Here are the classifications:

```json
[
    {"table": "products", "column": "sku", "semantic_type": "Sku", "confidence": 0.95}
]
```

These are my best guesses."#;

        let results = parse_llm_response(response).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].semantic_type, SemanticType::Sku);
    }

    #[test]
    fn test_parse_plain_fenced_json() {
        let response = "```\n[{\"table\":\"t\",\"column\":\"c\",\"semantic_type\":\"Email\",\"confidence\":0.8}]\n```";

        let results = parse_llm_response(response).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].semantic_type, SemanticType::Email);
    }

    #[test]
    fn test_parse_invalid_json() {
        let response = "this is not json at all";
        let result = parse_llm_response(response);
        assert!(result.is_err());
    }

    #[test]
    fn test_merge_overrides_unknown_only() {
        let mut rule_based = HashMap::new();
        rule_based.insert(
            ("users".to_string(), "email".to_string()),
            SemanticType::Email,
        );
        rule_based.insert(
            ("users".to_string(), "bio".to_string()),
            SemanticType::Unknown,
        );
        rule_based.insert(
            ("users".to_string(), "tagline".to_string()),
            SemanticType::Unknown,
        );

        let llm_response = r#"[
            {"table": "users", "column": "email", "semantic_type": "Phone", "confidence": 0.9},
            {"table": "users", "column": "bio", "semantic_type": "Bio", "confidence": 0.85},
            {"table": "users", "column": "tagline", "semantic_type": "Sentence", "confidence": 0.3}
        ]"#;

        let merged = merge_classifications(&rule_based, llm_response);

        // email: rule-based already classified it as Email, LLM should NOT override
        assert_eq!(
            merged[&("users".to_string(), "email".to_string())],
            SemanticType::Email
        );
        // bio: was Unknown, LLM confidence 0.85 >= 0.5, should override
        assert_eq!(
            merged[&("users".to_string(), "bio".to_string())],
            SemanticType::Bio
        );
        // tagline: was Unknown, but LLM confidence 0.3 < 0.5, should stay Unknown
        assert_eq!(
            merged[&("users".to_string(), "tagline".to_string())],
            SemanticType::Unknown
        );
    }

    #[test]
    fn test_merge_with_invalid_llm_response_falls_back() {
        let mut rule_based = HashMap::new();
        rule_based.insert(
            ("users".to_string(), "email".to_string()),
            SemanticType::Email,
        );

        let merged = merge_classifications(&rule_based, "invalid json!!!");

        // Should return rule_based unchanged
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[&("users".to_string(), "email".to_string())],
            SemanticType::Email
        );
    }

    #[test]
    fn test_build_ai_classification_cache() {
        let mut rule_based = HashMap::new();
        rule_based.insert(
            ("users".to_string(), "email".to_string()),
            SemanticType::Email,
        );
        rule_based.insert(
            ("users".to_string(), "bio".to_string()),
            SemanticType::Unknown,
        );

        let mut merged = rule_based.clone();
        merged.insert(("users".to_string(), "bio".to_string()), SemanticType::Bio);

        let cache = build_ai_classification_cache(&rule_based, &merged);

        // Only "bio" should be in the cache (it was Unknown→Bio)
        assert_eq!(cache.len(), 1);
        assert_eq!(cache["users"]["bio"], SemanticType::Bio);
        // "email" was already classified by rules, not in cache
        assert!(cache.get("users").unwrap().get("email").is_none());
    }

    #[test]
    fn test_extract_json_array_with_surrounding_text() {
        let response = "Some text before\n[{\"key\": \"val\"}]\nSome text after";
        let extracted = extract_json_array(response);
        assert_eq!(extracted, r#"[{"key": "val"}]"#);
    }

    #[test]
    fn test_confidence_defaults_to_zero() {
        let response = r#"[{"table": "t", "column": "c", "semantic_type": "Email"}]"#;
        let results = parse_llm_response(response).unwrap();
        assert!((results[0].confidence - 0.0).abs() < f64::EPSILON);
    }

    // --- Fix 1: Greedy bracket trap ---

    #[test]
    fn test_extract_json_array_ignores_peripheral_brackets() {
        // LLM wraps output in conversational text that also contains brackets
        let response = r#"Here is the JSON [as requested]:
[{"table": "users", "column": "email", "semantic_type": "Email", "confidence": 0.9}]
[End of output]"#;

        let extracted = extract_json_array(response);
        // Should extract the actual JSON array, not the "[as requested]...[End of output]" span
        assert!(
            extracted.starts_with("[{"),
            "Expected JSON array start, got: {}",
            extracted
        );
        assert!(
            extracted.ends_with("}]"),
            "Expected JSON array end, got: {}",
            extracted
        );

        // Verify it actually parses
        let results = parse_llm_response(response).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].semantic_type, SemanticType::Email);
    }

    // --- Fix 2: Per-row deserialization firebreak ---

    #[test]
    fn test_hallucinated_semantic_type_drops_one_row_not_all() {
        // 3 rows: two valid, one with a hallucinated "UserEmail" type
        let response = r#"[
            {"table": "users", "column": "email", "semantic_type": "Email", "confidence": 0.9},
            {"table": "users", "column": "bio", "semantic_type": "UserEmail", "confidence": 0.8},
            {"table": "users", "column": "tagline", "semantic_type": "Sentence", "confidence": 0.7}
        ]"#;

        let results = parse_llm_response(response).unwrap();
        // Should get 2 valid rows, not 0 (the hallucinated row is dropped)
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].semantic_type, SemanticType::Email);
        assert_eq!(results[1].semantic_type, SemanticType::Sentence);
    }

    #[test]
    fn test_all_hallucinated_returns_empty_not_error() {
        let response = r#"[
            {"table": "users", "column": "x", "semantic_type": "FooBar", "confidence": 0.9},
            {"table": "users", "column": "y", "semantic_type": "BazQux", "confidence": 0.8}
        ]"#;

        let results = parse_llm_response(response).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_missing_required_fields_drops_row() {
        // Missing "column" field in second row
        let response = r#"[
            {"table": "users", "column": "email", "semantic_type": "Email", "confidence": 0.9},
            {"table": "users", "semantic_type": "Bio", "confidence": 0.8}
        ]"#;

        let results = parse_llm_response(response).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].column, "email");
    }
}
