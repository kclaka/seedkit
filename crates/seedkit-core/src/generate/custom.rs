//! # Custom Provider Support
//!
//! Placeholder for Phase 4. Custom providers allow users to supply JS scripts
//! or WASM modules via `seedkit.toml` for domain-specific value generation
//! (e.g., tax codes, diagnosis codes, weighted product colors).
//!
//! ```toml
//! [columns."orders.tax_code"]
//! custom = "./scripts/tax_gen.js"
//! ```

/// A custom provider configuration from seedkit.toml.
#[derive(Debug, Clone)]
pub struct CustomProvider {
    pub path: String,
    pub provider_type: CustomProviderType,
}

#[derive(Debug, Clone)]
pub enum CustomProviderType {
    JavaScript,
    Wasm,
    ValueList {
        values: Vec<String>,
        weights: Option<Vec<f64>>,
    },
}

// TODO: Phase 4 - Implement JS/WASM provider execution via deno_core
