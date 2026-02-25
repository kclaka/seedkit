//! # Smart Sampling
//!
//! Phase 4 feature. Connects to a production read-only replica and extracts
//! statistical distributions (value frequencies, numeric ranges, row count ratios)
//! without copying actual data. The generation engine then uses these
//! distributions to produce synthetic data that mirrors production patterns.

pub mod mask;
pub mod stats;
