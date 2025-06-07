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
    parents: &HashMap<String, Vec<String>>,
    nodes: &HashSet<String>,
) -> Vec<Node> {
    let final_weights = compute_final_node_weights(childrens, parents, nodes);
    let mut result: Vec<Node> = final_weights
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

/// Вычисляет финальный вес каждого узла с учётом рекурсивного суммирования
/// весов всех потомков (включая веса потомков потомков)
fn compute_final_node_weights(
    childrens: &HashMap<String, Vec<String>>,
    parents: &HashMap<String, Vec<String>>,
    nodes: &HashSet<String>,
) -> HashMap<String, f64> {
    let basic_weights = compute_node_weights(childrens, nodes);
    let mut final_weights: HashMap<String, f64> = HashMap::new();
    
    // Получаем узлы, отсортированные по уровням (снизу вверх для корректного расчёта)
    let levels = get_nodes_by_levels(childrens, parents, nodes);
    let mut sorted_levels: Vec<_> = levels.keys().collect();
    sorted_levels.sort_by(|a, b| b.cmp(a)); // От глубоких к корневым
    
    // Инициализируем финальные веса базовыми весами
    for (node, weight) in &basic_weights {
        final_weights.insert(node.clone(), *weight);
    }
    
    // Проходим по уровням снизу вверх и добавляем веса потомков
    for &level in &sorted_levels {
        if let Some(nodes_at_level) = levels.get(&level) {
            for node in nodes_at_level {
                if let Some(children) = childrens.get(node) {
                    let mut additional_weight = 0.0;
                    
                    // Добавляем финальные веса всех прямых потомков
                    for child in children {
                        if let Some(child_final_weight) = final_weights.get(child) {
                            additional_weight += child_final_weight;
                        }
                    }
                    
                    // Обновляем финальный вес узла
                    if let Some(current_weight) = final_weights.get_mut(node) {
                        *current_weight += additional_weight;
                    }
                }
            }
        }
    }
    
    final_weights
}

/// Вычисляет итоговый вес каждого узла на основе суммы весов всех его потомков
fn compute_node_weights(
    childrens: &HashMap<String, Vec<String>>,
    nodes: &HashSet<String>,
) -> HashMap<String, f64> {
    let descendants_map = compute_descendants_with_depth_and_weight(childrens, nodes);
    let mut node_weights: HashMap<String, f64> = HashMap::new();
    
    for (node, descendants) in descendants_map {
        let total_weight: f64 = descendants.iter().map(|d| d.weight).sum();
        node_weights.insert(node, total_weight);
    }
    
    node_weights
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

/// Вспомогательный метод для получения узлов по уровням
fn get_nodes_by_levels(
    childrens: &HashMap<String, Vec<String>>,
    parents: &HashMap<String, Vec<String>>,
    nodes: &HashSet<String>,
) -> HashMap<usize, Vec<String>> {
    let node_levels = compute_node_levels(childrens, parents, nodes);
    let mut levels: HashMap<usize, Vec<String>> = HashMap::new();
    
    for (node, level) in node_levels {
        levels.entry(level).or_default().push(node);
    }
    
    // Сортируем узлы в каждом уровне для стабильности
    for nodes in levels.values_mut() {
        nodes.sort();
    }
    
    levels
}

/// Вычисляет уровень (глубину) каждого узла через топологическую сортировку
fn compute_node_levels(
    childrens: &HashMap<String, Vec<String>>,
    parents: &HashMap<String, Vec<String>>,
    nodes: &HashSet<String>,
) -> HashMap<String, usize> {
    let mut levels: HashMap<String, usize> = HashMap::new();
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    let mut queue: VecDeque<String> = VecDeque::new();

    // Инициализация степеней входа
    for node in nodes {
        let degree = parents.get(node).map_or(0, |p| p.len());
        in_degree.insert(node.clone(), degree);
        
        // Узлы без родителей (корни) имеют уровень 1
        if degree == 0 {
            levels.insert(node.clone(), 1);
            queue.push_back(node.clone());
        }
    }

    // Топологическая сортировка с вычислением уровней
    while let Some(current) = queue.pop_front() {
        let current_level = levels[&current];
        
        // Обрабатываем всех детей текущего узла
        if let Some(children) = childrens.get(&current) {
            for child in children {
                // Уменьшаем степень входа
                let child_degree = in_degree.get_mut(child).unwrap();
                *child_degree -= 1;
                
                // Обновляем уровень ребёнка
                let new_level = current_level + 1;
                levels.entry(child.clone())
                    .and_modify(|level| *level = (*level).max(new_level))
                    .or_insert(new_level);
                
                // Если все родители обработаны, добавляем в очередь
                if *child_degree == 0 {
                    queue.push_back(child.clone());
                }
            }
        }
    }

    levels
}