use crate::error::Result;
use crate::schema::types::DatabaseSchema;

/// Trait for database schema introspection.
/// Each database backend implements this to extract schema metadata.
pub trait SchemaIntrospector: Send + Sync {
    /// Introspect the database and return the full schema.
    fn introspect(&self) -> impl std::future::Future<Output = Result<DatabaseSchema>> + Send;
}

/// Determine the database type from a connection URL and return the appropriate introspector.
pub fn database_type_from_url(url: &str) -> Result<crate::schema::types::DatabaseType> {
    let scheme = url.split("://").next().unwrap_or("");
    match scheme {
        "postgres" | "postgresql" => Ok(crate::schema::types::DatabaseType::PostgreSQL),
        "mysql" | "mariadb" => Ok(crate::schema::types::DatabaseType::MySQL),
        "sqlite" | "file" => Ok(crate::schema::types::DatabaseType::SQLite),
        other => Err(crate::error::SeedKitError::UnsupportedDatabase {
            scheme: other.to_string(),
        }),
    }
}
