use indexmap::IndexMap;
use sqlx::postgres::PgPool;
use sqlx::Row;

use crate::error::{Result, SeedKitError};
use crate::schema::introspect::SchemaIntrospector;
use crate::schema::types::*;

pub struct PostgresIntrospector {
    pool: PgPool,
    schema_name: String,
}

impl PostgresIntrospector {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            schema_name: "public".to_string(),
        }
    }

    pub fn with_schema(pool: PgPool, schema_name: String) -> Self {
        Self { pool, schema_name }
    }

    async fn introspect_tables(&self) -> Result<IndexMap<String, Table>> {
        let query = "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE' ORDER BY table_name";
        let rows = sqlx::query(query)
            .bind(&self.schema_name)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SeedKitError::Introspection {
                query: "fetch tables".to_string(),
                source: e,
            })?;

        let mut tables = IndexMap::new();
        for row in rows {
            let name: String = row.get("table_name");
            tables.insert(name.clone(), Table::new(name));
        }
        Ok(tables)
    }

    async fn introspect_columns(&self, tables: &mut IndexMap<String, Table>) -> Result<()> {
        let query = r#"
            SELECT
                c.table_name,
                c.column_name,
                c.data_type,
                c.udt_name,
                c.is_nullable,
                c.column_default,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.ordinal_position
            FROM information_schema.columns c
            WHERE c.table_schema = $1
            ORDER BY c.table_name, c.ordinal_position
        "#;

        let rows = sqlx::query(query)
            .bind(&self.schema_name)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SeedKitError::Introspection {
                query: "fetch columns".to_string(),
                source: e,
            })?;

        for row in rows {
            let table_name: String = row.get("table_name");
            let column_name: String = row.get("column_name");
            let data_type_str: String = row.get("data_type");
            let udt_name: String = row.get("udt_name");
            let is_nullable: String = row.get("is_nullable");
            let column_default: Option<String> = row.get("column_default");
            let max_length: Option<i32> = row.get("character_maximum_length");
            let numeric_precision: Option<i32> = row.get("numeric_precision");
            let numeric_scale: Option<i32> = row.get("numeric_scale");
            let ordinal_position: i32 = row.get("ordinal_position");

            let data_type = if data_type_str == "USER-DEFINED" {
                DataType::Enum(udt_name.clone())
            } else if data_type_str == "ARRAY" {
                // PostgreSQL arrays: udt_name starts with underscore
                let inner_type = if let Some(stripped) = udt_name.strip_prefix('_') {
                    DataType::from_raw(stripped)
                } else {
                    DataType::from_raw(&udt_name)
                };
                DataType::Array(Box::new(inner_type))
            } else {
                DataType::from_raw(&data_type_str)
            };

            let is_auto = column_default
                .as_deref()
                .map(|d| d.starts_with("nextval("))
                .unwrap_or(false);

            let has_default = column_default.is_some();

            let mut column = Column::new(column_name.clone(), data_type, data_type_str.clone());
            column.nullable = is_nullable == "YES";
            column.has_default = has_default;
            column.is_auto_increment = is_auto;
            column.max_length = max_length.map(|v| v as u32);
            column.numeric_precision = numeric_precision.map(|v| v as u32);
            column.numeric_scale = numeric_scale.map(|v| v as u32);
            column.ordinal_position = ordinal_position as u32;

            if let Some(table) = tables.get_mut(&table_name) {
                table.columns.insert(column_name, column);
            }
        }

        Ok(())
    }

    async fn introspect_primary_keys(&self, tables: &mut IndexMap<String, Table>) -> Result<()> {
        let query = r#"
            SELECT
                tc.table_name,
                tc.constraint_name,
                kcu.column_name,
                kcu.ordinal_position
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = $1
                AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY tc.table_name, kcu.ordinal_position
        "#;

        let rows = sqlx::query(query)
            .bind(&self.schema_name)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SeedKitError::Introspection {
                query: "fetch primary keys".to_string(),
                source: e,
            })?;

        // Group by table
        let mut pk_map: IndexMap<String, (Option<String>, Vec<String>)> = IndexMap::new();
        for row in rows {
            let table_name: String = row.get("table_name");
            let constraint_name: String = row.get("constraint_name");
            let column_name: String = row.get("column_name");

            let entry = pk_map
                .entry(table_name)
                .or_insert_with(|| (Some(constraint_name), Vec::new()));
            entry.1.push(column_name);
        }

        for (table_name, (name, columns)) in pk_map {
            if let Some(table) = tables.get_mut(&table_name) {
                table.primary_key = Some(PrimaryKey { columns, name });
            }
        }

        Ok(())
    }

    async fn introspect_foreign_keys(&self, tables: &mut IndexMap<String, Table>) -> Result<()> {
        let query = r#"
            SELECT
                tc.table_name,
                tc.constraint_name,
                kcu.column_name,
                ccu.table_name AS referenced_table_name,
                ccu.column_name AS referenced_column_name,
                rc.delete_rule,
                rc.update_rule,
                tc.is_deferrable
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.table_schema
            JOIN information_schema.referential_constraints rc
                ON tc.constraint_name = rc.constraint_name
                AND tc.table_schema = rc.constraint_schema
            WHERE tc.table_schema = $1
                AND tc.constraint_type = 'FOREIGN KEY'
            ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
        "#;

        let rows = sqlx::query(query)
            .bind(&self.schema_name)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SeedKitError::Introspection {
                query: "fetch foreign keys".to_string(),
                source: e,
            })?;

        // Group by (table_name, constraint_name)
        let mut fk_map: IndexMap<(String, String), ForeignKey> = IndexMap::new();
        for row in rows {
            let table_name: String = row.get("table_name");
            let constraint_name: String = row.get("constraint_name");
            let column_name: String = row.get("column_name");
            let ref_table: String = row.get("referenced_table_name");
            let ref_column: String = row.get("referenced_column_name");
            let delete_rule: String = row.get("delete_rule");
            let update_rule: String = row.get("update_rule");
            let is_deferrable: String = row.get("is_deferrable");

            let key = (table_name, constraint_name.clone());
            let entry = fk_map.entry(key).or_insert_with(|| ForeignKey {
                name: Some(constraint_name),
                source_columns: Vec::new(),
                referenced_table: ref_table,
                referenced_columns: Vec::new(),
                on_delete: ForeignKeyAction::parse_action(&delete_rule),
                on_update: ForeignKeyAction::parse_action(&update_rule),
                is_deferrable: is_deferrable == "YES",
            });
            entry.source_columns.push(column_name);
            entry.referenced_columns.push(ref_column);
        }

        for ((table_name, _), fk) in fk_map {
            if let Some(table) = tables.get_mut(&table_name) {
                table.foreign_keys.push(fk);
            }
        }

        Ok(())
    }

    async fn introspect_unique_constraints(
        &self,
        tables: &mut IndexMap<String, Table>,
    ) -> Result<()> {
        let query = r#"
            SELECT
                tc.table_name,
                tc.constraint_name,
                kcu.column_name,
                kcu.ordinal_position
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = $1
                AND tc.constraint_type = 'UNIQUE'
            ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
        "#;

        let rows = sqlx::query(query)
            .bind(&self.schema_name)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SeedKitError::Introspection {
                query: "fetch unique constraints".to_string(),
                source: e,
            })?;

        let mut uc_map: IndexMap<(String, String), Vec<String>> = IndexMap::new();
        for row in rows {
            let table_name: String = row.get("table_name");
            let constraint_name: String = row.get("constraint_name");
            let column_name: String = row.get("column_name");

            uc_map
                .entry((table_name, constraint_name))
                .or_default()
                .push(column_name);
        }

        for ((table_name, constraint_name), columns) in uc_map {
            if let Some(table) = tables.get_mut(&table_name) {
                table.unique_constraints.push(UniqueConstraint {
                    name: Some(constraint_name),
                    columns,
                });
            }
        }

        Ok(())
    }

    async fn introspect_check_constraints(
        &self,
        tables: &mut IndexMap<String, Table>,
    ) -> Result<()> {
        let query = r#"
            SELECT
                tc.table_name,
                tc.constraint_name,
                cc.check_clause
            FROM information_schema.table_constraints tc
            JOIN information_schema.check_constraints cc
                ON tc.constraint_name = cc.constraint_name
                AND tc.constraint_schema = cc.constraint_schema
            WHERE tc.table_schema = $1
                AND tc.constraint_type = 'CHECK'
            ORDER BY tc.table_name
        "#;

        let rows = sqlx::query(query)
            .bind(&self.schema_name)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SeedKitError::Introspection {
                query: "fetch check constraints".to_string(),
                source: e,
            })?;

        for row in rows {
            let table_name: String = row.get("table_name");
            let constraint_name: String = row.get("constraint_name");
            let check_clause: String = row.get("check_clause");

            let parsed = parse_check_constraint(&check_clause);

            if let Some(table) = tables.get_mut(&table_name) {
                table.check_constraints.push(CheckConstraint {
                    name: Some(constraint_name),
                    expression: check_clause,
                    parsed,
                });
            }
        }

        Ok(())
    }

    async fn introspect_enums(&self) -> Result<IndexMap<String, Vec<String>>> {
        let query = r#"
            SELECT
                t.typname AS enum_name,
                e.enumlabel AS enum_value,
                e.enumsortorder
            FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            WHERE n.nspname = $1
            ORDER BY t.typname, e.enumsortorder
        "#;

        let rows = sqlx::query(query)
            .bind(&self.schema_name)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SeedKitError::Introspection {
                query: "fetch enums".to_string(),
                source: e,
            })?;

        let mut enums: IndexMap<String, Vec<String>> = IndexMap::new();
        for row in rows {
            let enum_name: String = row.get("enum_name");
            let enum_value: String = row.get("enum_value");
            enums.entry(enum_name).or_default().push(enum_value);
        }

        Ok(enums)
    }
}

impl SchemaIntrospector for PostgresIntrospector {
    async fn introspect(&self) -> Result<DatabaseSchema> {
        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "postgres".to_string());

        schema.tables = self.introspect_tables().await?;
        self.introspect_columns(&mut schema.tables).await?;
        self.introspect_primary_keys(&mut schema.tables).await?;
        self.introspect_foreign_keys(&mut schema.tables).await?;
        self.introspect_unique_constraints(&mut schema.tables)
            .await?;
        self.introspect_check_constraints(&mut schema.tables)
            .await?;
        schema.enums = self.introspect_enums().await?;

        // Back-fill enum values into columns that reference enum types
        for table in schema.tables.values_mut() {
            for column in table.columns.values_mut() {
                if let DataType::Enum(ref enum_name) = column.data_type {
                    if let Some(values) = schema.enums.get(enum_name) {
                        column.enum_values = Some(values.clone());
                    }
                }
            }
        }

        Ok(schema)
    }
}

/// Parse simple CHECK constraint expressions into structured form.
fn parse_check_constraint(expr: &str) -> Option<ParsedCheck> {
    let expr = expr.trim();
    // Remove outer parens
    let expr = if expr.starts_with('(') && expr.ends_with(')') {
        &expr[1..expr.len() - 1]
    } else {
        expr
    };
    let expr = expr.trim();

    // Pattern: column >= value
    let re_gte = regex::Regex::new(r"^(\w+)\s*>=\s*(-?[\d.]+)$").ok()?;
    if let Some(caps) = re_gte.captures(expr) {
        return Some(ParsedCheck::GreaterThanOrEqual {
            column: caps[1].to_string(),
            value: caps[2].parse().ok()?,
        });
    }

    // Pattern: column > value
    let re_gt = regex::Regex::new(r"^(\w+)\s*>\s*(-?[\d.]+)$").ok()?;
    if let Some(caps) = re_gt.captures(expr) {
        return Some(ParsedCheck::GreaterThan {
            column: caps[1].to_string(),
            value: caps[2].parse().ok()?,
        });
    }

    // Pattern: column <= value
    let re_lte = regex::Regex::new(r"^(\w+)\s*<=\s*(-?[\d.]+)$").ok()?;
    if let Some(caps) = re_lte.captures(expr) {
        return Some(ParsedCheck::LessThanOrEqual {
            column: caps[1].to_string(),
            value: caps[2].parse().ok()?,
        });
    }

    // Pattern: column < value (not column < column)
    let re_lt = regex::Regex::new(r"^(\w+)\s*<\s*(-?[\d.]+)$").ok()?;
    if let Some(caps) = re_lt.captures(expr) {
        return Some(ParsedCheck::LessThan {
            column: caps[1].to_string(),
            value: caps[2].parse().ok()?,
        });
    }

    // Pattern: column1 < column2
    let re_col_lt = regex::Regex::new(r"^(\w+)\s*<\s*(\w+)$").ok()?;
    if let Some(caps) = re_col_lt.captures(expr) {
        let left = &caps[1];
        let right = &caps[2];
        // Only match if right is not a number
        if right.parse::<f64>().is_err() {
            return Some(ParsedCheck::ColumnLessThan {
                left: left.to_string(),
                right: right.to_string(),
            });
        }
    }

    // Pattern: length(column) > 0 or char_length(column) > 0
    let re_len =
        regex::Regex::new(r"^(?:length|char_length|character_length)\((\w+)\)\s*>\s*(\d+)$")
            .ok()?;
    if let Some(caps) = re_len.captures(expr) {
        return Some(ParsedCheck::MinLength {
            column: caps[1].to_string(),
            min: caps[2].parse().ok()?,
        });
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_check_gte() {
        let parsed = parse_check_constraint("(price >= 0)");
        assert!(matches!(
            parsed,
            Some(ParsedCheck::GreaterThanOrEqual { ref column, value }) if column == "price" && value == 0.0
        ));
    }

    #[test]
    fn test_parse_check_gt() {
        let parsed = parse_check_constraint("quantity > 0");
        assert!(matches!(
            parsed,
            Some(ParsedCheck::GreaterThan { ref column, value }) if column == "quantity" && value == 0.0
        ));
    }

    #[test]
    fn test_parse_check_length() {
        let parsed = parse_check_constraint("(length(name) > 0)");
        assert!(matches!(
            parsed,
            Some(ParsedCheck::MinLength { ref column, min }) if column == "name" && min == 0
        ));
    }

    #[test]
    fn test_parse_check_column_lt() {
        let parsed = parse_check_constraint("start_date < end_date");
        assert!(matches!(
            parsed,
            Some(ParsedCheck::ColumnLessThan { ref left, ref right }) if left == "start_date" && right == "end_date"
        ));
    }
}
