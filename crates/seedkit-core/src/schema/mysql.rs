use indexmap::IndexMap;
use sqlx::mysql::MySqlPool;
use sqlx::Row;

use crate::error::{Result, SeedKitError};
use crate::schema::introspect::SchemaIntrospector;
use crate::schema::types::*;

pub struct MySqlIntrospector {
    pool: MySqlPool,
    database_name: String,
}

impl MySqlIntrospector {
    pub fn new(pool: MySqlPool, database_name: String) -> Self {
        Self {
            pool,
            database_name,
        }
    }

    async fn introspect_tables(&self) -> Result<IndexMap<String, Table>> {
        let query = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name";
        let rows = sqlx::query(query)
            .bind(&self.database_name)
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
                table_name,
                column_name,
                data_type,
                column_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                ordinal_position,
                extra,
                column_key
            FROM information_schema.columns
            WHERE table_schema = ?
            ORDER BY table_name, ordinal_position
        "#;

        let rows = sqlx::query(query)
            .bind(&self.database_name)
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
            let column_type: String = row.get("column_type");
            let is_nullable: String = row.get("is_nullable");
            let column_default: Option<String> = row.get("column_default");
            let max_length: Option<i64> = row.get("character_maximum_length");
            let numeric_precision: Option<i64> = row.get("numeric_precision");
            let numeric_scale: Option<i64> = row.get("numeric_scale");
            let ordinal_position: i64 = row.get("ordinal_position");
            let extra: String = row.get("extra");

            let (data_type, enum_values) = if data_type_str == "enum" || data_type_str == "set" {
                // Parse enum values from column_type like "enum('a','b','c')"
                let values = parse_mysql_enum_values(&column_type);
                (DataType::Enum(column_name.clone()), Some(values))
            } else {
                (DataType::from_raw(&data_type_str), None)
            };

            let is_auto = extra.contains("auto_increment");

            let mut column = Column::new(column_name.clone(), data_type, data_type_str);
            column.nullable = is_nullable == "YES";
            column.has_default = column_default.is_some();
            column.is_auto_increment = is_auto;
            column.max_length = max_length.map(|v| v as u32);
            column.numeric_precision = numeric_precision.map(|v| v as u32);
            column.numeric_scale = numeric_scale.map(|v| v as u32);
            column.ordinal_position = ordinal_position as u32;
            column.enum_values = enum_values;

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
                AND tc.table_name = kcu.table_name
            WHERE tc.table_schema = ?
                AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY tc.table_name, kcu.ordinal_position
        "#;

        let rows = sqlx::query(query)
            .bind(&self.database_name)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SeedKitError::Introspection {
                query: "fetch primary keys".to_string(),
                source: e,
            })?;

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
                kcu.referenced_table_name,
                kcu.referenced_column_name,
                rc.delete_rule,
                rc.update_rule
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                AND tc.table_name = kcu.table_name
            JOIN information_schema.referential_constraints rc
                ON tc.constraint_name = rc.constraint_name
                AND tc.table_schema = rc.constraint_schema
            WHERE tc.table_schema = ?
                AND tc.constraint_type = 'FOREIGN KEY'
            ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
        "#;

        let rows = sqlx::query(query)
            .bind(&self.database_name)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SeedKitError::Introspection {
                query: "fetch foreign keys".to_string(),
                source: e,
            })?;

        let mut fk_map: IndexMap<(String, String), ForeignKey> = IndexMap::new();
        for row in rows {
            let table_name: String = row.get("table_name");
            let constraint_name: String = row.get("constraint_name");
            let column_name: String = row.get("column_name");
            let ref_table: String = row.get("referenced_table_name");
            let ref_column: String = row.get("referenced_column_name");
            let delete_rule: String = row.get("delete_rule");
            let update_rule: String = row.get("update_rule");

            let key = (table_name, constraint_name.clone());
            let entry = fk_map.entry(key).or_insert_with(|| ForeignKey {
                name: Some(constraint_name),
                source_columns: Vec::new(),
                referenced_table: ref_table,
                referenced_columns: Vec::new(),
                on_delete: ForeignKeyAction::parse_action(&delete_rule),
                on_update: ForeignKeyAction::parse_action(&update_rule),
                is_deferrable: false, // MySQL doesn't support deferred constraints
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
                AND tc.table_name = kcu.table_name
            WHERE tc.table_schema = ?
                AND tc.constraint_type = 'UNIQUE'
            ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
        "#;

        let rows = sqlx::query(query)
            .bind(&self.database_name)
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
}

impl SchemaIntrospector for MySqlIntrospector {
    async fn introspect(&self) -> Result<DatabaseSchema> {
        let mut schema = DatabaseSchema::new(DatabaseType::MySQL, self.database_name.clone());

        schema.tables = self.introspect_tables().await?;
        self.introspect_columns(&mut schema.tables).await?;
        self.introspect_primary_keys(&mut schema.tables).await?;
        self.introspect_foreign_keys(&mut schema.tables).await?;
        self.introspect_unique_constraints(&mut schema.tables)
            .await?;

        Ok(schema)
    }
}

/// Parse MySQL enum values from column_type string like "enum('a','b','c')"
fn parse_mysql_enum_values(column_type: &str) -> Vec<String> {
    let s = column_type.trim();
    // Find content between parens
    if let Some(start) = s.find('(') {
        if let Some(end) = s.rfind(')') {
            let inner = &s[start + 1..end];
            return inner
                .split(',')
                .map(|v| v.trim().trim_matches('\'').to_string())
                .collect();
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_mysql_enum_values() {
        let values = parse_mysql_enum_values("enum('active','inactive','suspended')");
        assert_eq!(values, vec!["active", "inactive", "suspended"]);
    }
}
