use indexmap::IndexMap;
use sqlx::sqlite::SqlitePool;
use sqlx::Row;

use crate::error::{Result, SeedKitError};
use crate::schema::introspect::SchemaIntrospector;
use crate::schema::types::*;

pub struct SqliteIntrospector {
    pool: SqlitePool,
}

impl SqliteIntrospector {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn introspect_tables(&self) -> Result<IndexMap<String, Table>> {
        let query = "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name";
        let rows = sqlx::query(query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SeedKitError::Introspection {
                query: "fetch tables".to_string(),
                source: e,
            })?;

        let mut tables = IndexMap::new();
        for row in rows {
            let name: String = row.get("name");
            tables.insert(name.clone(), Table::new(name));
        }
        Ok(tables)
    }

    async fn introspect_columns(&self, tables: &mut IndexMap<String, Table>) -> Result<()> {
        let table_names: Vec<String> = tables.keys().cloned().collect();
        for table_name in table_names {
            let query = format!("PRAGMA table_info(\"{}\")", table_name);
            let rows = sqlx::query(&query)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| SeedKitError::Introspection {
                    query: format!("PRAGMA table_info({})", table_name),
                    source: e,
                })?;

            for row in rows {
                let cid: i32 = row.get("cid");
                let name: String = row.get("name");
                let type_str: String = row.get("type");
                let notnull: i32 = row.get("notnull");
                let dflt_value: Option<String> = row.get("dflt_value");
                let pk: i32 = row.get("pk");

                let data_type = DataType::from_raw(&type_str);
                let is_auto = pk > 0 && type_str.to_uppercase().contains("INTEGER");

                let mut column = Column::new(name.clone(), data_type, type_str);
                column.nullable = notnull == 0;
                column.has_default = dflt_value.is_some();
                column.is_auto_increment = is_auto;
                column.ordinal_position = cid as u32;

                if let Some(table) = tables.get_mut(&table_name) {
                    // Set primary key if pk > 0
                    if pk > 0 {
                        let primary_key = table.primary_key.get_or_insert_with(|| PrimaryKey {
                            columns: Vec::new(),
                            name: None,
                        });
                        primary_key.columns.push(name.clone());
                    }
                    table.columns.insert(name, column);
                }
            }
        }

        Ok(())
    }

    async fn introspect_foreign_keys(&self, tables: &mut IndexMap<String, Table>) -> Result<()> {
        let table_names: Vec<String> = tables.keys().cloned().collect();
        for table_name in table_names {
            let query = format!("PRAGMA foreign_key_list(\"{}\")", table_name);
            let rows = sqlx::query(&query)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| SeedKitError::Introspection {
                    query: format!("PRAGMA foreign_key_list({})", table_name),
                    source: e,
                })?;

            // Group by id (FK id)
            let mut fk_map: IndexMap<i32, ForeignKey> = IndexMap::new();
            for row in &rows {
                let id: i32 = row.get("id");
                let table: String = row.get("table");
                let from: String = row.get("from");
                let to: String = row.get("to");
                let on_delete: String = row.get("on_delete");
                let on_update: String = row.get("on_update");

                let entry = fk_map.entry(id).or_insert_with(|| ForeignKey {
                    name: None,
                    source_columns: Vec::new(),
                    referenced_table: table,
                    referenced_columns: Vec::new(),
                    on_delete: ForeignKeyAction::parse_action(&on_delete),
                    on_update: ForeignKeyAction::parse_action(&on_update),
                    is_deferrable: false,
                });
                entry.source_columns.push(from);
                entry.referenced_columns.push(to);
            }

            if let Some(table) = tables.get_mut(&table_name) {
                for (_, fk) in fk_map {
                    table.foreign_keys.push(fk);
                }
            }
        }

        Ok(())
    }

    async fn introspect_unique_constraints(
        &self,
        tables: &mut IndexMap<String, Table>,
    ) -> Result<()> {
        let table_names: Vec<String> = tables.keys().cloned().collect();
        for table_name in table_names {
            let query = format!("PRAGMA index_list(\"{}\")", table_name);
            let indexes = sqlx::query(&query)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| SeedKitError::Introspection {
                    query: format!("PRAGMA index_list({})", table_name),
                    source: e,
                })?;

            for idx_row in &indexes {
                let unique: i32 = idx_row.get("unique");
                let idx_name: String = idx_row.get("name");

                if unique == 1 {
                    let info_query = format!("PRAGMA index_info(\"{}\")", idx_name);
                    let cols = sqlx::query(&info_query)
                        .fetch_all(&self.pool)
                        .await
                        .map_err(|e| SeedKitError::Introspection {
                            query: format!("PRAGMA index_info({})", idx_name),
                            source: e,
                        })?;

                    let columns: Vec<String> = cols.iter().map(|r| r.get("name")).collect();

                    if let Some(table) = tables.get_mut(&table_name) {
                        table.unique_constraints.push(UniqueConstraint {
                            name: Some(idx_name),
                            columns,
                        });
                    }
                }
            }
        }

        Ok(())
    }
}

impl SchemaIntrospector for SqliteIntrospector {
    async fn introspect(&self) -> Result<DatabaseSchema> {
        let mut schema = DatabaseSchema::new(DatabaseType::SQLite, "sqlite".to_string());

        schema.tables = self.introspect_tables().await?;
        self.introspect_columns(&mut schema.tables).await?;
        self.introspect_foreign_keys(&mut schema.tables).await?;
        self.introspect_unique_constraints(&mut schema.tables)
            .await?;

        Ok(schema)
    }
}
