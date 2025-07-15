use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use rayon::prelude::*; // Для параллельной обработки
use rayon::ThreadPoolBuilder;
use num_cpus;
use serde::Serialize;
use crate::graph::dag::Node;

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node: Arc<str>,
    pub depth: usize,
    pub weight: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeDepth {
    pub node: Arc<str>,
    pub depth: usize,
}

/// Вычисляет вес узла по простой формуле: 1/depth
fn calculate_weight(depth: usize) -> f64 {
    1.0 / depth as f64
}


/// Вычисляет итоговый вес каждого узла на основе суммы весов всех его потомков
/*pub fn compute_weights(nodes_map: &HashMap<Arc<str>, Node>) -> HashMap<Arc<str>, f64> {
    let all_nodes: HashSet<Arc<str>> = nodes_map.keys().cloned().collect();
    let descendants_map = compute_descendants_with_depth_and_weight(nodes_map, &all_nodes);

    let mut weights_map: HashMap<Arc<str>, f64> = HashMap::new();
    
    for (node, descendants) in descendants_map {
        let total_weight: f64 = descendants.iter().map(|d| d.weight).sum();
        weights_map.insert(node, total_weight);
    }
    
    weights_map
}*/

/// Основной алгоритм: вычисляет всех потомков для каждого узла с их глубиной и весом
/// Использует простую формулу: вес = 1/глубина
pub fn compute_descendants_with_depth_and_weight(
    nodes_map: &HashMap<Arc<str>, Node>,
    nodes_to_process: &HashSet<Arc<str>>,
) -> HashMap<Arc<str>, Vec<NodeInfo>> {
    // Логика создания пула потоков остается прежней.
    let available_cpus = num_cpus::get();
    let num_threads = (available_cpus / 2).max(1);
    let pool = ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .unwrap();

    pool.install(|| {
        nodes_to_process
            .par_iter()
            .map(|node| {
                // REFACTORED: Передаем `nodes_map` в функцию поиска потомков.
                let descendants = find_descendants_with_depth_and_weight(nodes_map, node);
                (node.clone(), descendants)
            })
            .collect::<HashMap<_, _>>()
    })
}

/// Находит всех потомков узла с их глубиной и весом относительно этого узла
fn find_descendants_with_depth_and_weight(
    nodes_map: &HashMap<Arc<str>, Node>,
    start_node: &str
) -> Vec<NodeInfo> {
    let mut descendants: Vec<NodeInfo> = Vec::new();
    let mut visited: HashSet<Arc<str>> = HashSet::new();
    let mut queue: VecDeque<(Arc<str>, usize)> = VecDeque::new();
    
    // Получаем детей из `GraphNode` в `nodes_map`.
    if let Some(start_node_obj) = nodes_map.get(start_node) {
        for child in &start_node_obj.children {
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
            
            // Получаем детей текущего узла из `nodes_map`.
            if let Some(current_node_obj) = nodes_map.get(current.as_ref()) {
                for child in &current_node_obj.children {
                    if !visited.contains(child) {
                        queue.push_back((child.clone(), relative_depth + 1));
                    }
                }
            }
        }
    }

    descendants
}

/// Вычисляет глубину каждого узла в DAG относительно корневых узлов
/// Глубина корневых узлов (без родителей) = 0
/// Глубина остальных узлов = максимальная глубина родителей + 1
pub fn compute_node_depths(nodes_map: &HashMap<Arc<str>, Node>) -> HashMap<Arc<str>, usize> {
    let mut depths: HashMap<Arc<str>, usize> = HashMap::new();
    let mut in_degree: HashMap<Arc<str>, usize> = HashMap::new();
    let mut queue: VecDeque<Arc<str>> = VecDeque::new();

    // Инициализация: подсчитываем входящие степени для каждого узла
    for (node_id, node) in nodes_map {
        in_degree.insert(node_id.clone(), node.parents.len());
        
        // Если у узла нет родителей - он корневой, глубина = 0
        if node.parents.is_empty() {
            depths.insert(node_id.clone(), 0);
            queue.push_back(node_id.clone());
        }
    }

    // Топологическая сортировка с вычислением глубины
    while let Some(current_node) = queue.pop_front() {
        let current_depth = depths[&current_node];
        
        // Получаем узел из nodes_map
        if let Some(node) = nodes_map.get(&current_node) {
            // Обрабатываем всех детей текущего узла
            for child in &node.children {
                // Уменьшаем входящую степень ребенка
                if let Some(degree) = in_degree.get_mut(child) {
                    *degree -= 1;
                    
                    // Обновляем глубину ребенка (берем максимум из уже известной глубины и новой)
                    let new_depth = current_depth + 1;
                    depths.entry(child.clone())
                        .and_modify(|d| *d = (*d).max(new_depth))
                        .or_insert(new_depth);
                    
                    // Если все родители ребенка обработаны, добавляем его в очередь
                    if *degree == 0 {
                        queue.push_back(child.clone());
                    }
                }
            }
        }
    }

    depths
}

/// Возвращает список всех узлов с их глубинами, отсортированный по глубине
pub fn get_nodes_by_depth(nodes_map: &HashMap<Arc<str>, Node>) -> Vec<NodeDepth> {
    let depths = compute_node_depths(nodes_map);
    
    let mut result: Vec<NodeDepth> = depths
        .into_iter()
        .map(|(node, depth)| NodeDepth { node, depth })
        .collect();
    
    // Сортируем по глубине, затем по имени узла для стабильности
    result.sort_by(|a, b| {
        a.depth.cmp(&b.depth)
            .then_with(|| a.node.cmp(&b.node))
    });
    
    result
}