use crate::graph::dag::Node;
use num_cpus;
use rayon::prelude::*; // Для параллельной обработки
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};

use crate::domain::Hash;

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node: Hash,
    pub depth: usize,
    pub weight: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeDepth {
    pub node: Hash,
    pub depth: usize,
}

/// Глобальный rayon-пул: создаётся один раз на процесс, а не на каждый вызов
/// (находка C16). Половина доступных ядер, минимум один поток.
fn processor_pool() -> &'static Option<rayon::ThreadPool> {
    static POOL: OnceLock<Option<rayon::ThreadPool>> = OnceLock::new();
    POOL.get_or_init(|| {
        let num_threads = (num_cpus::get() / 2).max(1);
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .ok()
    })
}

/// Выполняет замыкание в глобальном пуле (или в стандартном пуле rayon,
/// если собственный создать не удалось).
fn install_in_pool<R>(f: impl FnOnce() -> R + Send) -> R
where
    R: Send,
{
    match processor_pool() {
        Some(pool) => pool.install(f),
        None => f(),
    }
}

/// Вычисляет вклад одного потомка по простой формуле: 1/depth
fn calculate_weight(depth: usize) -> f64 {
    1.0 / depth as f64
}

/// Преобразует сырую сумму весов потомков в ограниченное значение `[0, 1)`.
///
/// Раньше вес узла был суммой вкладов `1/depth` и неограниченно рос с числом
/// потомков (находка C23). Насыщающая функция `x / (1 + x)` монотонна, но
/// ограничена сверху единицей, что делает порог предсказуемым.
pub fn saturate_weight(raw: f64) -> f64 {
    if !raw.is_finite() || raw <= 0.0 {
        return 0.0;
    }
    raw / (1.0 + raw)
}

/// Основной алгоритм: вычисляет всех потомков для каждого узла с их глубиной и весом
/// Использует простую формулу: вес = 1/глубина
///
/// Обход мемоизируется: результаты для уже обработанных узлов переиспользуются
/// (Memoization). Это убирает повторный обход общих подграфов (находка C30).
/// Разные узлы обрабатываются параллельно в глобальном rayon-пуле.
pub fn compute_descendants_with_depth_and_weight(
    nodes_map: &HashMap<Hash, Node>,
    nodes_to_process: &HashSet<Hash>,
) -> HashMap<Hash, Vec<NodeInfo>> {
    // Единый мемоизирующий кэш на вызов, разделяемый между потоками.
    let memo: Mutex<HashMap<Hash, Arc<Vec<NodeInfo>>>> = Mutex::new(HashMap::new());

    install_in_pool(|| {
        nodes_to_process
            .par_iter()
            .map(|node| {
                let descendants = find_descendants_cached(nodes_map, node, &memo);
                (node.clone(), descendants.as_ref().clone())
            })
            .collect::<HashMap<_, _>>()
    })
}

/// Находит потомков узла, переиспользуя мемоизированные результаты.
fn find_descendants_cached(
    nodes_map: &HashMap<Hash, Node>,
    start_node: &Hash,
    memo: &Mutex<HashMap<Hash, Arc<Vec<NodeInfo>>>>,
) -> Arc<Vec<NodeInfo>> {
    if let Some(cached) = memo
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(start_node)
        .cloned()
    {
        return cached;
    }

    let mut descendants: Vec<NodeInfo> = Vec::new();
    let mut visited: HashSet<Hash> = HashSet::new();
    let mut queue: VecDeque<(Hash, usize)> = VecDeque::new();

    if let Some(start_node_obj) = nodes_map.get(start_node) {
        for child in &start_node_obj.children {
            queue.push_back((child.clone(), 1));
        }
    }

    while let Some((current, relative_depth)) = queue.pop_front() {
        if !visited.insert(current.clone()) {
            continue;
        }

        descendants.push(NodeInfo {
            node: current.clone(),
            depth: relative_depth,
            weight: calculate_weight(relative_depth),
        });

        // Если для текущего узла есть готовый результат, переиспользуем его,
        // не обходя подграф повторно.
        let cached = memo
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&current)
            .cloned();
        if let Some(cached) = cached {
            for d in cached.iter() {
                let depth = relative_depth + d.depth;
                if visited.insert(d.node.clone()) {
                    descendants.push(NodeInfo {
                        node: d.node.clone(),
                        depth,
                        weight: calculate_weight(depth),
                    });
                }
            }
        } else if let Some(current_node_obj) = nodes_map.get(&current) {
            for child in &current_node_obj.children {
                if !visited.contains(child) {
                    queue.push_back((child.clone(), relative_depth + 1));
                }
            }
        }
    }

    let result = Arc::new(descendants);
    memo.lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(start_node.clone(), Arc::clone(&result));
    result
}

/// Вычисляет глубину каждого узла в Dag относительно корневых узлов
/// Глубина корневых узлов (без родителей) = 0
/// Глубина остальных узлов = максимальная глубина родителей + 1
pub fn compute_node_depths(nodes_map: &HashMap<Hash, Node>) -> HashMap<Hash, usize> {
    let mut depths: HashMap<Hash, usize> = HashMap::new();
    let mut in_degree: HashMap<Hash, usize> = HashMap::new();
    let mut queue: VecDeque<Hash> = VecDeque::new();

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
                    depths
                        .entry(child.clone())
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
pub fn get_nodes_by_depth(nodes_map: &HashMap<Hash, Node>) -> Vec<NodeDepth> {
    let depths = compute_node_depths(nodes_map);

    let mut result: Vec<NodeDepth> = depths
        .into_iter()
        .map(|(node, depth)| NodeDepth { node, depth })
        .collect();

    // Сортируем по глубине, затем по имени узла для стабильности
    result.sort_by(|a, b| a.depth.cmp(&b.depth).then_with(|| a.node.cmp(&b.node)));

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saturate_weight_is_bounded_and_monotonic() {
        assert_eq!(saturate_weight(0.0), 0.0);
        assert_eq!(saturate_weight(-1.0), 0.0);
        assert_eq!(saturate_weight(f64::NAN), 0.0);
        assert_eq!(saturate_weight(f64::INFINITY), 0.0);

        // Монотонно и всегда < 1.
        let a = saturate_weight(1.0);
        let b = saturate_weight(10.0);
        let c = saturate_weight(1_000_000.0);
        assert!(a < b && b < c);
        assert!(c < 1.0);
        // Экстремально большие входы ограничены сверху единицей.
        assert!(saturate_weight(f64::MAX) <= 1.0);
    }

    fn node(children: &[&str]) -> Node {
        Node {
            parents: Default::default(),
            children: children.iter().map(|c| Hash::from(*c)).collect(),
            data: serde_json::json!({}),
            time: 0,
        }
    }

    /// Алмаз: root -> a, root -> b, a -> shared, b -> shared, shared -> leaf.
    fn diamond() -> HashMap<Hash, Node> {
        let mut map = HashMap::new();
        map.insert(Hash::from("root"), node(&["a", "b"]));
        map.insert(Hash::from("a"), node(&["shared"]));
        map.insert(Hash::from("b"), node(&["shared"]));
        map.insert(Hash::from("shared"), node(&["leaf"]));
        map.insert(Hash::from("leaf"), node(&[]));
        map
    }

    #[test]
    fn memoized_descendants_match_expected_breadth_first_result() {
        let map = diamond();
        let roots: HashSet<Hash> = [Hash::from("root")].into_iter().collect();
        let result = compute_descendants_with_depth_and_weight(&map, &roots);

        let mut got: Vec<(String, usize)> = result[&Hash::from("root")]
            .iter()
            .map(|d| (d.node.to_string(), d.depth))
            .collect();
        got.sort();

        // shared достижим на глубине 2 (через a или b), leaf — на 3.
        assert_eq!(
            got,
            vec![
                ("a".to_string(), 1),
                ("b".to_string(), 1),
                ("leaf".to_string(), 3),
                ("shared".to_string(), 2),
            ]
        );
    }

    #[test]
    fn memoization_does_not_change_results_for_multiple_roots() {
        let map = diamond();
        let roots: HashSet<Hash> = [Hash::from("root"), Hash::from("a"), Hash::from("shared")]
            .into_iter()
            .collect();
        let result = compute_descendants_with_depth_and_weight(&map, &roots);

        // Каждый узел вычисляется независимо и корректно.
        let shared = &result[&Hash::from("shared")];
        assert_eq!(shared.len(), 1);
        assert_eq!(shared[0].node.as_str(), "leaf");
        assert_eq!(shared[0].depth, 1);
    }
}
