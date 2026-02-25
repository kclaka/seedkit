use crate::graph::dag::DependencyGraph;
use crate::graph::topo::DeferredEdge;
use petgraph::visit::EdgeRef;

/// Output format for graph visualization.
pub enum GraphFormat {
    Mermaid,
    Dot,
}

/// Generate a visualization of the dependency graph.
pub fn visualize(
    graph: &DependencyGraph,
    deferred_edges: &[DeferredEdge],
    format: GraphFormat,
) -> String {
    match format {
        GraphFormat::Mermaid => generate_mermaid(graph, deferred_edges),
        GraphFormat::Dot => generate_dot(graph, deferred_edges),
    }
}

fn generate_mermaid(graph: &DependencyGraph, deferred_edges: &[DeferredEdge]) -> String {
    let mut output = String::from("graph TD\n");

    // Add nodes
    for node in graph.graph.node_indices() {
        let name = graph.table_name(node);
        output.push_str(&format!("    {}[{}]\n", name, name));
    }

    output.push('\n');

    // Add edges
    for edge in graph.graph.edge_references() {
        let from = graph.table_name(edge.source());
        let to = graph.table_name(edge.target());
        let label = edge.weight().source_columns.join(", ");
        output.push_str(&format!("    {} -->|{}| {}\n", from, label, to));
    }

    // Add deferred edges (dashed, red)
    for deferred in deferred_edges {
        let label = deferred.source_columns.join(", ");
        output.push_str(&format!(
            "    {} -.->|{} (deferred)| {}\n",
            deferred.source_table, label, deferred.target_table
        ));
    }

    // Style deferred edges
    if !deferred_edges.is_empty() {
        output.push_str("\n    %% Deferred FK edges shown with dashed lines\n");
    }

    output
}

fn generate_dot(graph: &DependencyGraph, deferred_edges: &[DeferredEdge]) -> String {
    let mut output = String::from("digraph dependencies {\n");
    output.push_str("    rankdir=TB;\n");
    output.push_str("    node [shape=box, style=rounded];\n\n");

    // Add edges
    for edge in graph.graph.edge_references() {
        let from = graph.table_name(edge.source());
        let to = graph.table_name(edge.target());
        let label = edge.weight().source_columns.join(", ");
        output.push_str(&format!(
            "    \"{}\" -> \"{}\" [label=\"{}\"];\n",
            from, to, label
        ));
    }

    // Add deferred edges
    for deferred in deferred_edges {
        let label = deferred.source_columns.join(", ");
        output.push_str(&format!(
            "    \"{}\" -> \"{}\" [label=\"{} (deferred)\", style=dashed, color=red];\n",
            deferred.source_table, deferred.target_table, label
        ));
    }

    output.push_str("}\n");
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::dag::DependencyGraph;
    use crate::schema::types::*;

    #[test]
    fn test_mermaid_output() {
        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());
        schema
            .tables
            .insert("users".to_string(), Table::new("users".to_string()));

        let mut orders = Table::new("orders".to_string());
        let mut user_id = Column::new(
            "user_id".to_string(),
            DataType::Integer,
            "integer".to_string(),
        );
        user_id.nullable = false;
        orders.columns.insert("user_id".to_string(), user_id);
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
        let output = visualize(&graph, &[], GraphFormat::Mermaid);

        assert!(output.contains("graph TD"));
        assert!(output.contains("orders"));
        assert!(output.contains("users"));
    }
}
