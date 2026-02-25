//! # Prompt Templates
//!
//! Builds structured prompts for LLM-based schema analysis. The prompt
//! includes a compact DDL representation of the database schema and
//! requests a JSON classification response.

use crate::schema::types::DatabaseSchema;

/// Generate a classification prompt from a schema DDL string.
pub fn classification_prompt(ddl: &str) -> String {
    format!(
        r#"You are a database schema analyst. Analyze the following database schema and classify each column with a semantic type.

For each column, return a JSON object with:
- table: the table name
- column: the column name
- semantic_type: one of these exact variants: FirstName, LastName, FullName, Username, DisplayName, Email, Phone, PhoneCountryCode, StreetAddress, City, State, ZipCode, PostalCode, Country, CountryCode, Latitude, Longitude, CompanyName, JobTitle, Department, Industry, Url, DomainName, IpAddress, MacAddress, UserAgent, Slug, Title, Description, Bio, Paragraph, Sentence, HtmlContent, MarkdownContent, ImageUrl, AvatarUrl, ThumbnailUrl, FileUrl, FileName, MimeType, FileSize, Price, Currency, CurrencyCode, Amount, Percentage, CreatedAt, UpdatedAt, DeletedAt, StartDate, EndDate, BirthDate, DateOnly, TimeOnly, Timestamp, PasswordHash, Token, ApiKey, SecretKey, Uuid, AutoIncrement, ExternalId, Sku, OrderNumber, InvoiceNumber, TrackingNumber, Status, Role, Priority, Category, Tag, BooleanFlag, EnumValue, Quantity, Rating, Score, Weight, Height, Age, Duration, SortOrder, JsonData, Color, HexColor, Locale, Timezone, Unknown
- confidence: a number from 0.0 to 1.0 indicating your confidence

**To save tokens, skip columns whose names are already unambiguous.** For example:
- Columns named exactly `email`, `created_at`, `updated_at`, `deleted_at` do not need classification — we already know them.
- Foreign key columns ending in `_id` (like `user_id`, `order_id`) should be skipped.
- Only classify columns where the name alone is ambiguous or domain-specific.

Here is an example of the expected output format:
[
  {{"table": "users", "column": "display_name", "semantic_type": "FullName", "confidence": 0.8}},
  {{"table": "products", "column": "margin", "semantic_type": "Percentage", "confidence": 0.7}}
]

Return ONLY a JSON array with no surrounding text.

Schema DDL:
```sql
{}
```"#,
        ddl
    )
}

/// Build a compact DDL representation from an introspected schema.
///
/// Produces a CREATE TABLE-style summary that includes column names, types,
/// nullability, primary keys, and foreign keys — enough for the LLM to
/// classify columns without needing the full raw DDL.
pub fn schema_to_compact_ddl(schema: &DatabaseSchema) -> String {
    let mut ddl = String::new();

    for (table_name, table) in &schema.tables {
        ddl.push_str(&format!("CREATE TABLE {} (\n", table_name));

        let mut col_lines = Vec::new();
        for (col_name, column) in &table.columns {
            let mut line = format!("  {} {}", col_name, column.raw_type);
            if !column.nullable {
                line.push_str(" NOT NULL");
            }
            if column.is_auto_increment {
                line.push_str(" AUTO_INCREMENT");
            }
            if column.has_default {
                line.push_str(" DEFAULT ...");
            }
            if let Some(ref vals) = column.enum_values {
                line.push_str(&format!(" /* ENUM: {} */", vals.join(", ")));
            }
            col_lines.push(line);
        }

        if let Some(pk) = &table.primary_key {
            col_lines.push(format!("  PRIMARY KEY ({})", pk.columns.join(", ")));
        }

        for fk in &table.foreign_keys {
            col_lines.push(format!(
                "  FOREIGN KEY ({}) REFERENCES {}({})",
                fk.source_columns.join(", "),
                fk.referenced_table,
                fk.referenced_columns.join(", "),
            ));
        }

        for ck in &table.check_constraints {
            col_lines.push(format!("  CHECK ({})", ck.expression));
        }

        ddl.push_str(&col_lines.join(",\n"));
        ddl.push_str("\n);\n\n");
    }

    ddl
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::*;

    #[test]
    fn test_classification_prompt_contains_ddl() {
        let prompt = classification_prompt("CREATE TABLE users (id SERIAL PRIMARY KEY);");
        assert!(prompt.contains("CREATE TABLE users"));
        assert!(prompt.contains("semantic_type"));
        assert!(prompt.contains("JSON array"));
    }

    #[test]
    fn test_classification_prompt_lists_semantic_types() {
        let prompt = classification_prompt("");
        assert!(prompt.contains("FirstName"));
        assert!(prompt.contains("Email"));
        assert!(prompt.contains("PasswordHash"));
        assert!(prompt.contains("Unknown"));
    }

    #[test]
    fn test_schema_to_compact_ddl() {
        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());
        let mut table = Table::new("users".to_string());

        let mut id_col = Column::new("id".to_string(), DataType::Integer, "serial".to_string());
        id_col.is_auto_increment = true;
        table.columns.insert("id".to_string(), id_col);

        let mut name_col = Column::new(
            "name".to_string(),
            DataType::VarChar,
            "varchar(255)".to_string(),
        );
        name_col.nullable = false;
        table.columns.insert("name".to_string(), name_col);

        let mut bio_col = Column::new("bio".to_string(), DataType::Text, "text".to_string());
        bio_col.nullable = true;
        table.columns.insert("bio".to_string(), bio_col);

        table.primary_key = Some(PrimaryKey {
            columns: vec!["id".to_string()],
            name: None,
        });

        schema.tables.insert("users".to_string(), table);

        let ddl = schema_to_compact_ddl(&schema);

        assert!(ddl.contains("CREATE TABLE users"));
        assert!(ddl.contains("id serial"));
        assert!(ddl.contains("AUTO_INCREMENT"));
        assert!(ddl.contains("name varchar(255) NOT NULL"));
        assert!(ddl.contains("bio text"));
        assert!(ddl.contains("PRIMARY KEY (id)"));
    }

    #[test]
    fn test_compact_ddl_includes_foreign_keys() {
        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());

        let users = Table::new("users".to_string());
        schema.tables.insert("users".to_string(), users);

        let mut orders = Table::new("orders".to_string());
        orders.foreign_keys.push(ForeignKey {
            name: None,
            source_columns: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ForeignKeyAction::NoAction,
            on_update: ForeignKeyAction::NoAction,
            is_deferrable: false,
        });
        schema.tables.insert("orders".to_string(), orders);

        let ddl = schema_to_compact_ddl(&schema);
        assert!(ddl.contains("FOREIGN KEY (user_id) REFERENCES users(id)"));
    }

    // --- Fix 1: Enum values and check constraints in DDL ---

    #[test]
    fn test_compact_ddl_includes_enum_values() {
        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());
        let mut table = Table::new("users".to_string());

        let mut status_col = Column::new(
            "status".to_string(),
            DataType::VarChar,
            "varchar(50)".to_string(),
        );
        status_col.nullable = false;
        status_col.enum_values = Some(vec![
            "active".to_string(),
            "suspended".to_string(),
            "banned".to_string(),
        ]);
        table.columns.insert("status".to_string(), status_col);

        schema.tables.insert("users".to_string(), table);

        let ddl = schema_to_compact_ddl(&schema);
        assert!(
            ddl.contains("ENUM: active, suspended, banned"),
            "DDL should contain enum values: {}",
            ddl
        );
    }

    #[test]
    fn test_compact_ddl_includes_check_constraints() {
        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());
        let mut table = Table::new("products".to_string());

        let mut price_col = Column::new(
            "price".to_string(),
            DataType::Numeric,
            "numeric(10,2)".to_string(),
        );
        price_col.nullable = false;
        table.columns.insert("price".to_string(), price_col);

        table.check_constraints.push(CheckConstraint {
            name: Some("products_price_check".to_string()),
            expression: "price >= 0".to_string(),
            parsed: None,
        });

        schema.tables.insert("products".to_string(), table);

        let ddl = schema_to_compact_ddl(&schema);
        assert!(
            ddl.contains("CHECK (price >= 0)"),
            "DDL should contain check constraint: {}",
            ddl
        );
    }

    // --- Fix 2: Output token savings ---

    #[test]
    fn test_prompt_instructs_skip_obvious_columns() {
        let prompt = classification_prompt("");
        // Should tell the LLM to skip obvious columns to save output tokens
        assert!(
            prompt.contains("email"),
            "Prompt should mention skipping 'email'"
        );
        assert!(
            prompt.contains("created_at"),
            "Prompt should mention skipping 'created_at'"
        );
        assert!(
            prompt.contains("_id"),
            "Prompt should mention skipping FK '_id' columns"
        );
    }

    // --- Fix 3: Few-shot example ---

    #[test]
    fn test_prompt_contains_few_shot_example() {
        let prompt = classification_prompt("");
        // Should contain a concrete JSON example to prevent hallucination
        assert!(
            prompt.contains("\"table\""),
            "Prompt should have example with table key"
        );
        assert!(
            prompt.contains("\"semantic_type\""),
            "Prompt should have example with semantic_type key"
        );
        assert!(
            prompt.contains("\"confidence\""),
            "Prompt should have example with confidence key"
        );
        // The example should use real SemanticType variants
        assert!(
            prompt.contains("FullName") || prompt.contains("Percentage"),
            "Example should use real SemanticType variants"
        );
    }
}
