use std::collections::{HashMap, HashSet};

use crate::generate::value::Value;

/// Tracks generated values for unique constraint enforcement.
pub struct UniqueTracker {
    /// Map from constraint key (table + columns) to set of seen values.
    constraints: HashMap<String, HashSet<String>>,
    /// Maximum retries before giving up.
    pub max_retries: usize,
}

impl UniqueTracker {
    pub fn new() -> Self {
        Self {
            constraints: HashMap::new(),
            max_retries: 1000,
        }
    }

    /// Register a unique constraint to track.
    pub fn register_constraint(&mut self, table_name: &str, columns: &[String]) {
        let key = constraint_key(table_name, columns);
        self.constraints.entry(key).or_default();
    }

    /// Check if a value (or composite value) has been seen before.
    /// If not, record it and return true. If duplicate, return false.
    pub fn try_insert(&mut self, table_name: &str, columns: &[String], values: &[&Value]) -> bool {
        let key = constraint_key(table_name, columns);

        if let Some(seen) = self.constraints.get_mut(&key) {
            let value_key = values
                .iter()
                .map(|v| v.to_unique_key())
                .collect::<Vec<_>>()
                .join("|");

            seen.insert(value_key)
        } else {
            // Constraint not registered, allow anything
            true
        }
    }

    /// Check if a single-column value is unique.
    pub fn try_insert_single(
        &mut self,
        table_name: &str,
        column_name: &str,
        value: &Value,
    ) -> bool {
        self.try_insert(table_name, &[column_name.to_string()], &[value])
    }

    /// Get the number of unique values tracked for a constraint.
    pub fn count(&self, table_name: &str, columns: &[String]) -> usize {
        let key = constraint_key(table_name, columns);
        self.constraints.get(&key).map(|s| s.len()).unwrap_or(0)
    }
}

impl Default for UniqueTracker {
    fn default() -> Self {
        Self::new()
    }
}

fn constraint_key(table_name: &str, columns: &[String]) -> String {
    format!("{}:{}", table_name, columns.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    #[test]
    fn test_unique_tracking() {
        let mut tracker = UniqueTracker::new();
        tracker.register_constraint("users", &["email".to_string()]);

        let v1 = Value::String(Cow::Owned("test@example.com".to_string()));
        assert!(tracker.try_insert_single("users", "email", &v1));

        // Same value should fail
        assert!(!tracker.try_insert_single("users", "email", &v1));

        // Different value should succeed
        let v2 = Value::String(Cow::Owned("other@example.com".to_string()));
        assert!(tracker.try_insert_single("users", "email", &v2));
    }

    #[test]
    fn test_composite_unique() {
        let mut tracker = UniqueTracker::new();
        let cols = vec!["first_name".to_string(), "last_name".to_string()];
        tracker.register_constraint("users", &cols);

        let v1 = Value::String(Cow::Owned("John".to_string()));
        let v2 = Value::String(Cow::Owned("Doe".to_string()));
        assert!(tracker.try_insert("users", &cols, &[&v1, &v2]));

        // Same combo should fail
        assert!(!tracker.try_insert("users", &cols, &[&v1, &v2]));

        // Different combo should succeed
        let v3 = Value::String(Cow::Owned("Jane".to_string()));
        assert!(tracker.try_insert("users", &cols, &[&v3, &v2]));
    }
}
