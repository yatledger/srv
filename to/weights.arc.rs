use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node: Arc<str>,
    pub depth: usize,
    pub weight: f64,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub node: Arc<str>,
    pub weight: f64,
}

pub fn calculate_weight(depth: usize) -> f64 {
    1.0 / (depth as f64)
}

pub fn compute_descendants_with_depth_and_weight(
    childrens: &HashMap<Arc<str>, Vec<Arc<str>>>,
    nodes: &HashSet<Arc<str>>,
) -> HashMap<Arc<str>, Vec<NodeInfo>> {
    let mut result: HashMap<Arc<str>, Vec<NodeInfo>> = HashMap::new();
    let mut memo: HashMap<Arc<str>, Vec<NodeInfo>> = HashMap::new();

    for node in nodes {
        let descendants = find_descendants_with_depth_and_weight_cached(childrens, node, &mut memo);
        result.insert(Arc::clone(node), descendants);
    }

    result
}

fn find_descendants_with_depth_and_weight_cached(
    childrens: &HashMap<Arc<str>, Vec<Arc<str>>>,
    start_node: &Arc<str>,
    memo: &mut HashMap<Arc<str>, Vec<NodeInfo>>,
) -> Vec<NodeInfo> {
    if let Some(cached) = memo.get(start_node) {
        return cached.clone();
    }

    let mut descendants: Vec<NodeInfo> = Vec::new();
    let mut visited: HashSet<Arc<str>> = HashSet::new();
    let mut queue: VecDeque<(Arc<str>, usize)> = VecDeque::new();

    if let Some(children) = childrens.get(start_node) {
        for child in children {
            queue.push_back((Arc::clone(child), 1));
        }
    }

    while let Some((current, relative_depth)) = queue.pop_front() {
        if visited.insert(Arc::clone(&current)) {
            let weight = calculate_weight(relative_depth);
            descendants.push(NodeInfo {
                node: Arc::clone(&current),
                depth: relative_depth,
                weight,
            });

            if let Some(cached_children) = memo.get(&current) {
                for d in cached_children {
                    let depth = relative_depth + d.depth;
                    if visited.insert(Arc::clone(&d.node)) {
                        descendants.push(NodeInfo {
                            node: Arc::clone(&d.node),
                            depth,
                            weight: calculate_weight(depth),
                        });
                    }
                }
            } else if let Some(children) = childrens.get(&current) {
                for child in children {
                    if !visited.contains(child) {
                        queue.push_back((Arc::clone(child), relative_depth + 1));
                    }
                }
            }
        }
    }

    memo.insert(Arc::clone(start_node), descendants.clone());
    descendants
}

pub fn compute_weights(
    childrens: &HashMap<Arc<str>, Vec<Arc<str>>>,
    nodes: &HashSet<Arc<str>>,
) -> HashMap<Arc<str>, Vec<Node>> {
    let mut result: HashMap<Arc<str>, Vec<Node>> = HashMap::new();
    let descendants_map = compute_descendants_with_depth_and_weight(childrens, nodes);

    for (node, descendants) in descendants_map {
        let mut weights = Vec::with_capacity(descendants.len());
        for d in descendants {
            weights.push(Node {
                node: Arc::clone(&d.node),
                weight: d.weight,
            });
        }
        result.insert(node, weights);
    }

    result
}
