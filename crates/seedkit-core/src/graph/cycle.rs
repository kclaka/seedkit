use petgraph::algo::tarjan_scc;
use petgraph::graph::EdgeIndex;

use crate::error::{Result, SeedKitError};
use crate::graph::dag::DependencyGraph;
use crate::graph::topo::DeferredEdge;

/// Detect and break cycles in the dependency graph.
/// Returns the list of edges that were removed (deferred) to make the graph acyclic.
///
/// Strategy:
/// 1. Find all strongly connected components (SCCs) using Tarjan's algorithm
/// 2. For each SCC with more than one node, pick an edge to break
/// 3. Prefer nullable FK edges, then deferrable edges, then any edge
/// 4. User overrides via `break_at` take highest priority
pub fn break_cycles(
    graph: &mut DependencyGraph,
    break_at: &[String], // User-specified edges like "users.invited_by_id"
) -> Result<Vec<DeferredEdge>> {
    let mut deferred = Vec::new();

    loop {
        let sccs = tarjan_scc(&graph.graph);
        let cycles: Vec<_> = sccs.into_iter().filter(|scc| scc.len() > 1).collect();

        if cycles.is_empty() {
            break;
        }

        for scc in &cycles {
            // Find the best edge to break in this SCC
            let edge_to_break = find_best_edge_to_break(graph, scc, break_at)?;

            if let Some((edge_idx, deferred_edge)) = edge_to_break {
                deferred.push(deferred_edge);
                graph.graph.remove_edge(edge_idx);
            }
        }
    }

    // Also handle self-referencing FKs (single-node SCCs where node has edge to itself)
    let self_refs = find_self_references(graph);
    for (edge_idx, deferred_edge) in self_refs {
        deferred.push(deferred_edge);
        graph.graph.remove_edge(edge_idx);
    }

    Ok(deferred)
}

fn find_best_edge_to_break(
    graph: &DependencyGraph,
    scc: &[petgraph::graph::NodeIndex],
    break_at: &[String],
) -> Result<Option<(EdgeIndex, DeferredEdge)>> {
    use petgraph::visit::EdgeRef;

    let scc_set: std::collections::HashSet<_> = scc.iter().copied().collect();

    // Collect all edges within this SCC
    let mut candidate_edges: Vec<(EdgeIndex, &crate::graph::dag::EdgeInfo, String, String)> =
        Vec::new();

    for &node in scc {
        let table_name = graph.table_name(node).to_string();
        for edge in graph.graph.edges(node) {
            if scc_set.contains(&edge.target()) {
                let target_name = graph.table_name(edge.target()).to_string();
                candidate_edges.push((edge.id(), edge.weight(), table_name.clone(), target_name));
            }
        }
    }

    if candidate_edges.is_empty() {
        let table_names: Vec<String> = scc
            .iter()
            .map(|&n| graph.table_name(n).to_string())
            .collect();
        return Err(SeedKitError::UnbreakableCycle {
            tables: table_names.join(", "),
        });
    }

    // Priority 1: User-specified break points
    for (edge_idx, info, source_table, _target_table) in &candidate_edges {
        for col in &info.source_columns {
            let qualified = format!("{}.{}", source_table, col);
            if break_at.contains(&qualified) {
                return Ok(Some((
                    *edge_idx,
                    DeferredEdge {
                        source_table: source_table.clone(),
                        source_columns: info.source_columns.clone(),
                        target_table: _target_table.clone(),
                        target_columns: info.referenced_columns.clone(),
                    },
                )));
            }
        }
    }

    // Priority 2: Nullable FK columns
    for (edge_idx, info, source_table, target_table) in &candidate_edges {
        if info.is_nullable {
            return Ok(Some((
                *edge_idx,
                DeferredEdge {
                    source_table: source_table.clone(),
                    source_columns: info.source_columns.clone(),
                    target_table: target_table.clone(),
                    target_columns: info.referenced_columns.clone(),
                },
            )));
        }
    }

    // Priority 3: Deferrable FK constraints
    for (edge_idx, info, source_table, target_table) in &candidate_edges {
        if info.is_deferrable {
            return Ok(Some((
                *edge_idx,
                DeferredEdge {
                    source_table: source_table.clone(),
                    source_columns: info.source_columns.clone(),
                    target_table: target_table.clone(),
                    target_columns: info.referenced_columns.clone(),
                },
            )));
        }
    }

    // Priority 4: Any edge (last resort)
    let (edge_idx, info, source_table, target_table) = &candidate_edges[0];
    Ok(Some((
        *edge_idx,
        DeferredEdge {
            source_table: source_table.clone(),
            source_columns: info.source_columns.clone(),
            target_table: target_table.clone(),
            target_columns: info.referenced_columns.clone(),
        },
    )))
}

fn find_self_references(graph: &DependencyGraph) -> Vec<(EdgeIndex, DeferredEdge)> {
    use petgraph::visit::EdgeRef;

    let mut self_refs = Vec::new();

    for node in graph.graph.node_indices() {
        let table_name = graph.table_name(node).to_string();
        for edge in graph.graph.edges(node) {
            if edge.target() == node {
                self_refs.push((
                    edge.id(),
                    DeferredEdge {
                        source_table: table_name.clone(),
                        source_columns: edge.weight().source_columns.clone(),
                        target_table: table_name.clone(),
                        target_columns: edge.weight().referenced_columns.clone(),
                    },
                ));
            }
        }
    }

    self_refs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::dag::DependencyGraph;
    use crate::schema::types::*;

    #[test]
    fn test_self_reference_detection() {
        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());

        let mut categories = Table::new("categories".to_string());
        categories.columns.insert(
            "id".to_string(),
            Column::new("id".to_string(), DataType::Serial, "serial".to_string()),
        );
        let mut parent_id = Column::new(
            "parent_id".to_string(),
            DataType::Integer,
            "integer".to_string(),
        );
        parent_id.nullable = true;
        categories
            .columns
            .insert("parent_id".to_string(), parent_id);
        categories.foreign_keys.push(ForeignKey {
            name: None,
            source_columns: vec!["parent_id".to_string()],
            referenced_table: "categories".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ForeignKeyAction::NoAction,
            on_update: ForeignKeyAction::NoAction,
            is_deferrable: false,
        });
        schema.tables.insert("categories".to_string(), categories);

        let mut graph = DependencyGraph::from_schema(&schema);
        let deferred = break_cycles(&mut graph, &[]).unwrap();

        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred[0].source_table, "categories");
        assert_eq!(deferred[0].target_table, "categories");
    }

    #[test]
    fn test_mutual_cycle_breaking() {
        let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test".to_string());

        // Table A references B, B references A
        let mut table_a = Table::new("table_a".to_string());
        table_a.columns.insert(
            "id".to_string(),
            Column::new("id".to_string(), DataType::Serial, "serial".to_string()),
        );
        let mut b_id = Column::new("b_id".to_string(), DataType::Integer, "integer".to_string());
        b_id.nullable = true; // nullable so it can be broken
        table_a.columns.insert("b_id".to_string(), b_id);
        table_a.foreign_keys.push(ForeignKey {
            name: None,
            source_columns: vec!["b_id".to_string()],
            referenced_table: "table_b".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ForeignKeyAction::NoAction,
            on_update: ForeignKeyAction::NoAction,
            is_deferrable: false,
        });
        schema.tables.insert("table_a".to_string(), table_a);

        let mut table_b = Table::new("table_b".to_string());
        table_b.columns.insert(
            "id".to_string(),
            Column::new("id".to_string(), DataType::Serial, "serial".to_string()),
        );
        let mut a_id = Column::new("a_id".to_string(), DataType::Integer, "integer".to_string());
        a_id.nullable = false;
        table_b.columns.insert("a_id".to_string(), a_id);
        table_b.foreign_keys.push(ForeignKey {
            name: None,
            source_columns: vec!["a_id".to_string()],
            referenced_table: "table_a".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ForeignKeyAction::NoAction,
            on_update: ForeignKeyAction::NoAction,
            is_deferrable: false,
        });
        schema.tables.insert("table_b".to_string(), table_b);

        let mut graph = DependencyGraph::from_schema(&schema);
        let deferred = break_cycles(&mut graph, &[]).unwrap();

        assert_eq!(deferred.len(), 1);
        // Should break the nullable edge (table_a.b_id)
        assert_eq!(deferred[0].source_table, "table_a");
    }
}
