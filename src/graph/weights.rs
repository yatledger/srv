use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node: String,
    pub depth: usize,
    pub weight: f64,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub node: String,
    pub weight: f64,
}
/// Вычисляет вес узла по простой формуле: 1/depth
fn calculate_weight(depth: usize) -> f64 {
    1.0 / depth as f64
}

/// Получить финальные веса в виде отсортированного вектора узлов
pub fn get_all_weights(
    childrens: &HashMap<String, Vec<String>>,
    nodes: &HashSet<String>,
) -> Vec<Node> {
    let weights = compute_weights(childrens, nodes);
    let mut result: Vec<Node> = weights
        .into_iter()
        .map(|(node, weight)| Node { node, weight })
        .collect();
    
    // Сортируем по весу (по убыванию) и алфавиту
    // result.sort_by(|a, b| b.weight.partial_cmp(&a.weight).unwrap_or(std::cmp::Ordering::Equal));
    result.sort_by(|a, b| {
        b.weight.partial_cmp(&a.weight)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.node.cmp(&b.node))
    });
    
    result
}

/// Вычисляет итоговый вес каждого узла на основе суммы весов всех его потомков
fn compute_weights(
    childrens: &HashMap<String, Vec<String>>,
    nodes: &HashSet<String>,
) -> HashMap<String, f64> {
    let descendants_map = compute_descendants_with_depth_and_weight(childrens, nodes);
    let mut weights: HashMap<String, f64> = HashMap::new();
    
    for (node, descendants) in descendants_map {
        let total_weight: f64 = descendants.iter().map(|d| d.weight).sum();
        weights.insert(node, total_weight);
    }
    
    weights
}

/// Основной алгоритм: вычисляет всех потомков для каждого узла с их глубиной и весом
/// Использует простую формулу: вес = 1/глубина
pub fn compute_descendants_with_depth_and_weight(
    childrens: &HashMap<String, Vec<String>>,
    nodes: &HashSet<String>,
) -> HashMap<String, Vec<NodeInfo>> {
    let mut result: HashMap<String, Vec<NodeInfo>> = HashMap::new();
    
    for node in nodes {
        let descendants = find_descendants_with_depth_and_weight(childrens, node);
        result.insert(node.clone(), descendants);
    }
    
    result
}

/// Находит всех потомков узла с их глубиной и весом относительно этого узла
fn find_descendants_with_depth_and_weight(childrens: &HashMap<String, Vec<String>>, start_node: &str) -> Vec<NodeInfo> {
    let mut descendants: Vec<NodeInfo> = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<(String, usize)> = VecDeque::new();

    /*// Добавляем сам узел с глубиной 0 и весом 1.0
    descendants.push(NodeInfo {
        node: start_node.to_string(),
        depth: 0,
        weight: 1.0,
    });
    visited.insert(start_node.to_string());*/
    
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
    descendants.sort_by(|a, b| {
        a.depth.cmp(&b.depth).then_with(|| a.node.cmp(&b.node))
    });

    descendants
}