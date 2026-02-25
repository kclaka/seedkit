use petgraph::algo::toposort;

use crate::error::{Result, SeedKitError};
use crate::graph::dag::DependencyGraph;

/// Result of topological sorting — an ordered list of table names
/// for safe insertion (parents before children).
#[derive(Debug, Clone)]
pub struct InsertionOrder {
    /// Tables in the order they should be inserted (parents first).
    pub tables: Vec<String>,
    /// Edges that were deferred (broken) to resolve cycles.
    pub deferred_edges: Vec<DeferredEdge>,
}

/// A foreign key edge that was broken to resolve a cycle.
/// After all tables are populated, these FKs need UPDATE statements.
#[derive(Debug, Clone)]
pub struct DeferredEdge {
    pub source_table: String,
    pub source_columns: Vec<String>,
    pub target_table: String,
    pub target_columns: Vec<String>,
}

/// Compute topological sort of the dependency graph.
/// Returns tables in insertion order (parents before children).
///
/// If cycles exist, this will fail — use `cycle::break_cycles` first.
pub fn topological_sort(graph: &DependencyGraph) -> Result<InsertionOrder> {
    // petgraph toposort returns nodes from leaves to roots for our edge direction
    // (edges go child → parent), so we need to reverse the result
    match toposort(&graph.graph, None) {
        Ok(sorted_indices) => {
            // toposort with child→parent edges gives parents first naturally
            // Actually petgraph's toposort returns nodes where sources come first
            // Since edges are child→parent, "sources" = nodes with no outgoing edges = leaf tables with no FKs
            // Wait — edges are child→parent, so parents have incoming edges. toposort gives us parents last.
            // We need to reverse.
            let tables: Vec<String> = sorted_indices
                .iter()
                .rev()
                .map(|&idx| graph.table_name(idx).to_string())
                .collect();

            Ok(InsertionOrder {
                tables,
                deferred_edges: Vec::new(),
            })
        }
        Err(cycle_node) => {
            let table_name = graph.table_name(cycle_node.node_id());
            Err(SeedKitError::CircularDependency {
                tables: table_name.to_string(),
                suggested_break: format!("{}.???", table_name),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::dag::DependencyGraph;
    use crate::schema::types::*;

    #[test]
    fn test_topological_sort_simple() {
        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());

        let users = Table::new("users".to_string());
        schema.tables.insert("users".to_string(), users);

        let mut orders = Table::new("orders".to_string());
        let mut user_id_col = Column::new(
            "user_id".to_string(),
            DataType::Integer,
            "integer".to_string(),
        );
        user_id_col.nullable = false;
        orders.columns.insert("user_id".to_string(), user_id_col);
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

        let graph = DependencyGraph::from_schema(&schema);
        let order = topological_sort(&graph).unwrap();

        let users_pos = order.tables.iter().position(|t| t == "users").unwrap();
        let orders_pos = order.tables.iter().position(|t| t == "orders").unwrap();

        assert!(users_pos < orders_pos, "users must come before orders");
    }
}
