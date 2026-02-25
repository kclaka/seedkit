//! # Foreign Key Value Pool
//!
//! Tracks generated primary key values so child tables can pick valid FK
//! references. As parent tables are generated first (topological order),
//! their PK values are recorded here. Child table columns with
//! `GenerationStrategy::ForeignKeyReference` then sample from the pool.

use rand::Rng;
use std::collections::HashMap;

use crate::generate::value::Value;

/// Manages pools of generated primary key values for FK references.
pub struct ForeignKeyPool {
    /// Map from (table_name, column_name) to list of generated values.
    pools: HashMap<(String, String), Vec<Value>>,
}

impl ForeignKeyPool {
    pub fn new() -> Self {
        Self {
            pools: HashMap::new(),
        }
    }

    /// Record a generated value for a column (typically a PK column).
    pub fn record_value(&mut self, table_name: &str, column_name: &str, value: Value) {
        self.pools
            .entry((table_name.to_string(), column_name.to_string()))
            .or_default()
            .push(value);
    }

    /// Pick a random value from the pool for a FK reference.
    pub fn pick_reference(
        &self,
        table_name: &str,
        column_name: &str,
        rng: &mut impl Rng,
    ) -> Option<Value> {
        self.pools
            .get(&(table_name.to_string(), column_name.to_string()))
            .and_then(|pool| {
                if pool.is_empty() {
                    None
                } else {
                    Some(pool[rng.random_range(0..pool.len())].clone())
                }
            })
    }

    /// Get the number of values in a pool.
    pub fn pool_size(&self, table_name: &str, column_name: &str) -> usize {
        self.pools
            .get(&(table_name.to_string(), column_name.to_string()))
            .map(|p| p.len())
            .unwrap_or(0)
    }

    /// Get all values in a pool (for deferred FK resolution).
    pub fn get_pool(&self, table_name: &str, column_name: &str) -> Option<&[Value]> {
        self.pools
            .get(&(table_name.to_string(), column_name.to_string()))
            .map(|v| v.as_slice())
    }
}

impl Default for ForeignKeyPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn test_record_and_pick() {
        let mut pool = ForeignKeyPool::new();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        pool.record_value("users", "id", Value::Int(1));
        pool.record_value("users", "id", Value::Int(2));
        pool.record_value("users", "id", Value::Int(3));

        let picked = pool.pick_reference("users", "id", &mut rng);
        assert!(picked.is_some());
        if let Some(Value::Int(v)) = picked {
            assert!((1..=3).contains(&v));
        }
    }

    #[test]
    fn test_empty_pool() {
        let pool = ForeignKeyPool::new();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        assert!(pool.pick_reference("users", "id", &mut rng).is_none());
    }
}
