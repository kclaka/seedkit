use petgraph::graph::{DiGraph, NodeIndex};
use std::collections::HashMap;

use crate::schema::types::DatabaseSchema;

/// A directed graph representing table dependencies via foreign keys.
/// Edges point from dependent table to referenced table (child → parent).
pub struct DependencyGraph {
    pub graph: DiGraph<String, EdgeInfo>,
    pub node_indices: HashMap<String, NodeIndex>,
}

/// Information about an edge (foreign key relationship).
#[derive(Debug, Clone)]
pub struct EdgeInfo {
    /// Name of the FK constraint
    pub constraint_name: Option<String>,
    /// Source columns in the dependent table
    pub source_columns: Vec<String>,
    /// Referenced columns in the parent table
    pub referenced_columns: Vec<String>,
    /// Whether the FK column(s) are nullable
    pub is_nullable: bool,
    /// Whether the FK constraint is deferrable
    pub is_deferrable: bool,
}

impl DependencyGraph {
    /// Build a dependency graph from a database schema.
    /// Each table becomes a node, each FK becomes a directed edge from child to parent.
    pub fn from_schema(schema: &DatabaseSchema) -> Self {
        let mut graph = DiGraph::new();
        let mut node_indices = HashMap::new();

        // Add all tables as nodes
        for table_name in schema.tables.keys() {
            let idx = graph.add_node(table_name.clone());
            node_indices.insert(table_name.clone(), idx);
        }

        // Add FK edges: child table → parent table
        for (table_name, table) in &schema.tables {
            for fk in &table.foreign_keys {
                if let (Some(&from_idx), Some(&to_idx)) = (
                    node_indices.get(table_name),
                    node_indices.get(&fk.referenced_table),
                ) {
                    // Check if FK columns are all nullable
                    let is_nullable = fk.source_columns.iter().all(|col_name| {
                        table
                            .columns
                            .get(col_name)
                            .map(|c| c.nullable)
                            .unwrap_or(false)
                    });

                    graph.add_edge(
                        from_idx,
                        to_idx,
                        EdgeInfo {
                            constraint_name: fk.name.clone(),
                            source_columns: fk.source_columns.clone(),
                            referenced_columns: fk.referenced_columns.clone(),
                            is_nullable,
                            is_deferrable: fk.is_deferrable,
                        },
                    );
                }
            }
        }

        Self {
            graph,
            node_indices,
        }
    }

    /// Get the table name for a node index.
    pub fn table_name(&self, idx: NodeIndex) -> &str {
        &self.graph[idx]
    }

    /// Get node index for a table name.
    pub fn node_index(&self, table_name: &str) -> Option<NodeIndex> {
        self.node_indices.get(table_name).copied()
    }

    /// Get all table names in the graph.
    pub fn table_names(&self) -> Vec<&str> {
        self.graph.node_weights().map(|s| s.as_str()).collect()
    }

    /// Get the number of tables.
    pub fn table_count(&self) -> usize {
        self.graph.node_count()
    }

    /// Get the number of FK edges.
    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::*;

    fn make_test_schema() -> DatabaseSchema {
        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());

        // users table (no FKs)
        let mut users = Table::new("users".to_string());
        users.columns.insert(
            "id".to_string(),
            Column::new("id".to_string(), DataType::Serial, "serial".to_string()),
        );
        schema.tables.insert("users".to_string(), users);

        // orders table (FK to users)
        let mut orders = Table::new("orders".to_string());
        orders.columns.insert(
            "id".to_string(),
            Column::new("id".to_string(), DataType::Serial, "serial".to_string()),
        );
        let mut user_id_col = Column::new(
            "user_id".to_string(),
            DataType::Integer,
            "integer".to_string(),
        );
        user_id_col.nullable = false;
        orders.columns.insert("user_id".to_string(), user_id_col);
        orders.foreign_keys.push(ForeignKey {
            name: Some("orders_user_id_fkey".to_string()),
            source_columns: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ForeignKeyAction::Cascade,
            on_update: ForeignKeyAction::NoAction,
            is_deferrable: false,
        });
        schema.tables.insert("orders".to_string(), orders);

        // order_items table (FK to orders)
        let mut items = Table::new("order_items".to_string());
        items.columns.insert(
            "id".to_string(),
            Column::new("id".to_string(), DataType::Serial, "serial".to_string()),
        );
        let mut order_id_col = Column::new(
            "order_id".to_string(),
            DataType::Integer,
            "integer".to_string(),
        );
        order_id_col.nullable = false;
        items.columns.insert("order_id".to_string(), order_id_col);
        items.foreign_keys.push(ForeignKey {
            name: Some("items_order_id_fkey".to_string()),
            source_columns: vec!["order_id".to_string()],
            referenced_table: "orders".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ForeignKeyAction::Cascade,
            on_update: ForeignKeyAction::NoAction,
            is_deferrable: false,
        });
        schema.tables.insert("order_items".to_string(), items);

        schema
    }

    #[test]
    fn test_build_graph() {
        let schema = make_test_schema();
        let graph = DependencyGraph::from_schema(&schema);

        assert_eq!(graph.table_count(), 3);
        assert_eq!(graph.edge_count(), 2);
    }
}
