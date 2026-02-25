use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Statistical distribution profile extracted from production data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributionProfile {
    pub table_name: String,
    pub row_count: u64,
    pub column_distributions: HashMap<String, ColumnDistribution>,
}

/// Distribution information for a single column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnDistribution {
    /// Categorical distribution (enum-like values with frequencies).
    Categorical {
        values: Vec<(String, f64)>, // (value, frequency)
    },
    /// Numeric distribution.
    Numeric {
        min: f64,
        max: f64,
        mean: f64,
        stddev: f64,
    },
    /// Ratio to another table's row count.
    Ratio {
        related_table: String,
        ratio: f64, // e.g., 3.2 orders per user
    },
}

// TODO: Phase 4 - Implement aggregate query execution against prod replicas
