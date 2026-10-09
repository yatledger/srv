use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use rand::seq::SliceRandom;
use rand::rng;
use crate::DagDb;
use tracing::info;
use std::collections::HashMap;
use crate::graph::weights::{compute_descendants_with_depth_and_weight};

pub async fn start_weight_updater(graph: DagDb) {
    let mut interval = time::interval(Duration::from_secs(1)); // Пересчет каждые 5 секунд

    loop {
        interval.tick().await;

        let mut graph_write = match graph.write() {
            Ok(guard) => guard,
            Err(e) => {
                tracing::error!("Failed to lock graph for writing: {}", e);
                continue;
            }
        };

        // Используем публичный метод get_node_keys
        let nodes: Vec<Arc<str>> = graph_write.get_node_keys();
        if nodes.is_empty() {
            continue;
        }

        let batch_size = (nodes.len() / 5).max(1); // Берем 1/5 узлов, минимум 1
        let mut batch: Vec<Arc<str>> = nodes.clone();
        batch.shuffle(&mut rng());
        batch.truncate(batch_size);

        info!("Updating weights for {} nodes out of {}", batch.len(), nodes.len());

        // Пересчитываем веса только для выбранной пачки
        let weights_map = compute_weights_for_batch(&graph_write, &batch);

        // Обновляем веса в графе для узлов из пачки
        for node in batch {
            if let Some(node_obj) = graph_write.get_node_mut(&node) {
                node_obj.weight = *weights_map.get(&node).unwrap_or(&0.0);
            }
        }
    }
}

// Функция для пересчета весов только для указанной пачки узлов
fn compute_weights_for_batch(
    graph: &crate::graph::dag::DAG,
    batch: &[Arc<str>],
) -> HashMap<Arc<str>, f64> {
    let all_nodes: std::collections::HashSet<Arc<str>> = graph.get_node_keys().into_iter().collect();
    let descendants_map = compute_descendants_with_depth_and_weight(graph.get_nodes(), &all_nodes);

    let mut weights_map = HashMap::new();
    for node in batch {
        let total_weight: f64 = descendants_map
            .get(node)
            .unwrap_or(&Vec::new())
            .iter()
            .map(|d| d.weight)
            .sum();
        weights_map.insert(node.clone(), total_weight);
    }
    weights_map
}