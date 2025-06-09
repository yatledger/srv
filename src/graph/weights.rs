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
/// Вычисляет вес узла по простой формуле: 1/depth
fn calculate_weight(depth: usize) -> f64 {
    1.0 / depth as f64
}

/// Вычисляет итоговый вес каждого узла на основе суммы весов всех его потомков
pub fn compute_weights(
    childrens: &HashMap<Arc<str>, Vec<Arc<str>>>,
    nodes: &HashSet<Arc<str>>,
) -> Vec<Node> {
    let descendants_map = compute_descendants_with_depth_and_weight(childrens, nodes);
    let mut weights: Vec<Node> = Vec::new();
    
    for (node, descendants) in descendants_map {
        let total_weight: f64 = descendants.iter().map(|d| d.weight).sum();
        weights.push(Node {
            node,
            weight: total_weight,
        });
    }
    
    weights
}

/// Основной алгоритм: вычисляет всех потомков для каждого узла с их глубиной и весом
/// Использует простую формулу: вес = 1/глубина
pub fn compute_descendants_with_depth_and_weight(
    childrens: &HashMap<Arc<str>, Vec<Arc<str>>>,
    nodes: &HashSet<Arc<str>>,
) -> HashMap<Arc<str>, Vec<NodeInfo>> {
    let mut result: HashMap<Arc<str>, Vec<NodeInfo>> = HashMap::new();
    
    for node in nodes {
        let descendants = find_descendants_with_depth_and_weight(childrens, node);
        result.insert(node.clone(), descendants);
    }
    
    result
}

/// Находит всех потомков узла с их глубиной и весом относительно этого узла
fn find_descendants_with_depth_and_weight(childrens: &HashMap<Arc<str>, Vec<Arc<str>>>, start_node: &str) -> Vec<NodeInfo> {
    // TODO Кэшировать только структуру потомков без веса
    let mut descendants: Vec<NodeInfo> = Vec::new();
    let mut visited: HashSet<Arc<str>> = HashSet::new();
    let mut queue: VecDeque<(Arc<str>, usize)> = VecDeque::new();
    
    // Добавляем всех непосредственных детей
    if let Some(children) = childrens.get(start_node) {
        for child in children {
            queue.push_back((child.clone(), 1));
        }
    }

    // BFS для поиска всех потомков
    while let Some((current, relative_depth)) = queue.pop_front() {
        if visited.insert(current.clone()) {
            let weight = calculate_weight(relative_depth);
            descendants.push(NodeInfo {
                node: current.clone(),
                depth: relative_depth,
                weight,
            });
            
            // Добавляем детей текущего узла
            if let Some(children) = childrens.get(&current) {
                for child in children {
                    if !visited.contains(child) {
                        queue.push_back((child.clone(), relative_depth + 1));
                    }
                }
            }
        }
    }

    // Сортируем по глубине, затем по имени для стабильности
    /*descendants.sort_by(|a, b| {
        a.depth.cmp(&b.depth).then_with(|| a.node.cmp(&b.node))
    });*/

    descendants
}