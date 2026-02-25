//! # Error Types
//!
//! Defines `SeedKitError`, the unified error enum for every failure mode in
//! the SeedKit pipeline. Every variant includes enough context (table name,
//! column name, row index, SQL snippet) to debug immediately without digging
//! through logs.

use thiserror::Error;

/// All errors that can occur in SeedKit operations.
#[derive(Error, Debug)]
pub enum SeedKitError {
    #[error("Database connection failed: {message}\n  Connection string: {connection_hint}\n  Cause: {source}")]
    Connection {
        message: String,
        connection_hint: String,
        #[source]
        source: sqlx::Error,
    },

    #[error("Schema introspection failed on query '{query}': {source}")]
    Introspection {
        query: String,
        #[source]
        source: sqlx::Error,
    },

    #[error("No database URL provided. SeedKit looks for a connection in this order:\n  1. --db flag\n  2. DATABASE_URL environment variable\n  3. .env file with DATABASE_URL\n  4. seedkit.toml [database] section\n\nExample: seedkit generate --db postgres://localhost/myapp --rows 100")]
    NoDatabaseUrl,

    #[error("Unsupported database scheme '{scheme}'. Supported: postgres://, mysql://, sqlite://")]
    UnsupportedDatabase { scheme: String },

    #[error("Circular dependency detected involving tables: {tables}\n  SeedKit will attempt to break cycles using nullable FK columns.\n  You can override this in seedkit.toml:\n  [graph]\n  break_cycle_at = [\"{suggested_break}\"]")]
    CircularDependency {
        tables: String,
        suggested_break: String,
    },

    #[error("No breakable edge found for circular dependency involving: {tables}\n  All FK columns in the cycle are NOT NULL. Consider:\n  1. Making one FK column nullable\n  2. Adding a break_cycle_at override in seedkit.toml")]
    UnbreakableCycle { tables: String },

    #[error("Failed to generate unique value for {table}.{column} at row {row_index}: {max_retries} retries exhausted\n  Consider reducing --rows or adding more variation to the column's generator")]
    UniqueExhausted {
        table: String,
        column: String,
        row_index: usize,
        max_retries: usize,
    },

    #[error("Insert failed on {table} row {row_index}: {message}\n  SQL: {sql_preview}\n  DB error: {source}")]
    InsertFailed {
        table: String,
        row_index: usize,
        message: String,
        sql_preview: String,
        #[source]
        source: sqlx::Error,
    },

    #[error("Foreign key resolution failed: {source_table}.{source_column} references {target_table}.{target_column}, but target table has no generated rows")]
    ForeignKeyResolution {
        source_table: String,
        source_column: String,
        target_table: String,
        target_column: String,
    },

    #[error("Lock file error: {message}")]
    LockFile { message: String },

    #[error("Schema drift detected: {message}")]
    SchemaDrift { message: String },

    #[error("Configuration error: {message}")]
    Config { message: String },

    #[error("LLM API error: {message}")]
    LlmError { message: String },

    #[error("Output error: {message}: {source}")]
    Output {
        message: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Check constraint cannot be satisfied for {table}.{column}: {constraint}\n  Generated value {value} violates the constraint")]
    CheckConstraintViolation {
        table: String,
        column: String,
        constraint: String,
        value: String,
    },

    #[error("Composite unique constraint exhausted on {table}.({columns}) at row {row_index}: {max_retries} retries exhausted\n  The combination of values for these columns could not be made unique")]
    CompositeUniqueExhausted {
        table: String,
        columns: String,
        row_index: usize,
        max_retries: usize,
    },

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, SeedKitError>;
