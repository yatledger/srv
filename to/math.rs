
/// Выполняет топологическую сортировку графа, возвращая узлы в порядке от листьев к корням.
/// Топологическая сортировка упорядочивает узлы так, чтобы для каждого ребра (u -> v) узел u
/// находился в списке раньше узла v. Это необходимо для алгоритмов, зависящих от порядка обработки,
/// например, для вычисления весов узлов на основе их потомков.
/// 
/// # Алгоритм
/// 1. Создаётся пустое множество `visited` для отслеживания посещённых узлов.
/// 2. Создаётся пустой вектор `order` для хранения порядка узлов.
/// 3. Для каждого узла графа, который ещё не посещён, вызывается `dfs_topo` для рекурсивного обхода.
/// 4. После обхода всех узлов возвращается вектор `order`, содержащий узлы в топологическом порядке.
/// 
/// # Возвращает
/// Вектор `Vec<String>`, содержащий хэши узлов в топологическом порядке (от листьев к корням).
/// 
/// # Примечания
/// - Метод предполагает, что граф ацикличен (DAG). Если граф содержит цикл, результат может быть некорректным,
///   но в текущей реализации циклы предотвращаются проверкой в `add_node_with_parents`.
/// - Используется HashSet для `visited`, чтобы избежать повторного посещения узлов, что важно для графов
///   с несколькими путями к одному узлу.
fn topological_sort(&self) -> Vec<String> {
    // Создаём множество для отслеживания посещённых узлов, чтобы не обрабатывать их повторно.
    let mut visited = HashSet::new();
    // Создаём вектор для хранения порядка узлов в топологической сортировке.
    let mut order = Vec::new();
    // Проходим по всем узлам графа, чтобы учесть все компоненты связности.
    for node in self.adj_list.keys() {
        // Если узел ещё не посещён, запускаем для него DFS-обход.
        if !visited.contains(node) {
            self.dfs_topo(node, &mut visited, &mut order);
        }
    }
    // Возвращаем вектор с узлами в топологическом порядке.
    order
}

/// Вспомогательный метод для топологической сортировки, использующий поиск в глубину (DFS).
/// Рекурсивно обходит граф, начиная с указанного узла, и добавляет узлы в порядок после посещения
/// всех их потомков. Это обеспечивает, что дочерние узлы (листья) появляются в порядке раньше родителей.
/// 
/// # Аргументы
/// * `node` — Хэш текущего узла (ссылка на String), с которого начинается обход.
/// * `visited` — Множество посещённых узлов (HashSet), обновляется во время обхода.
/// * `order` — Вектор, в который добавляются узлы в топологическом порядке.
/// 
/// # Логика работы
/// 1. Помечаем текущий узел как посещённый, добавляя его в `visited`.
/// 2. Если у узла есть дочерние узлы, рекурсивно обходим каждого непосещённого ребёнка.
/// 3. После обработки всех детей добавляем текущий узел в `order`.
/// 
/// # Примечания
/// - Узел добавляется в `order` только после обработки всех его потомков, что гарантирует
///   корректный топологический порядок (листья раньше родителей).
/// - Используется клонирование `node`, так как `order` должен владеть значением, а `node` — это ссылка.
/// - Метод безопасно обрабатывает случай, когда у узла нет детей (листовой узел).
fn dfs_topo(&self, node: &String, visited: &mut HashSet<String>, order: &mut Vec<String>) {
    // Помечаем текущий узел как посещённый, чтобы избежать повторного обхода.
    visited.insert(node.clone());
    // Получаем список дочерних узлов текущего узла, если он есть в графе.
    if let Some(children) = self.adj_list.get(node) {
        // Обходим каждого ребёнка, если он ещё не посещён.
        for child in children {
            if !visited.contains(child) {
                // Рекурсивно вызываем DFS для непосещённого ребёнка.
                self.dfs_topo(child, visited, order);
            }
        }
    }
    // После обработки всех детей добавляем текущий узел в порядок.
    // Это гарантирует, что узел появится в списке после всех своих потомков.
    order.push(node.clone());
}
/// Выполняет обратную топологическую сортировку по reverse_adj_list.
/// 
/// # Возвращает
/// Вектор узлов в топологическом порядке (post-order, родители после детей).
fn reverse_topological_sort(&self) -> Vec<String> {
    let mut visited = HashSet::new();
    let mut stack = Vec::new();

    for node in self.adj_list.keys() {
        if !visited.contains(node) {
            self.dfs_reverse_topo(node, &mut visited, &mut stack);
        }
    }

    stack
}

/// Рекурсивный DFS для обратной топологической сортировки.
/// 
/// # Аргументы
/// * `node` - Текущий узел.
/// * `visited` - Множество посещённых узлов.
/// * `stack` - Стек для хранения узлов в порядке завершения.
fn dfs_reverse_topo(&self, node: &String, visited: &mut HashSet<String>, stack: &mut Vec<String>) {
    if visited.contains(node) {
        return;
    }

    visited.insert(node.clone());

    // Обходим всех родителей через reverse_adj_list
    if let Some(parents) = self.reverse_adj_list.get(node) {
        for parent in parents {
            self.dfs_reverse_topo(parent, visited, stack);
        }
    }

    stack.push(node.clone());
}

/// DFS для сбора потомков.
/// 
/// # Аргументы
/// * `node` - Текущий узел.
/// * `depth` - Текущая глубина.
/// * `descendants` - Хранилище потомков для всех узлов.
/// * `visited` - Множество для предотвращения циклов (хотя в DAG их нет, но на всякий случай).
fn dfs_collect(&self, node: &String, depth: u32, descendants: &mut HashMap<String, Vec<u32>>, visited: &mut HashSet<String>) {
    if depth > 5 || visited.contains(node) {
        return;
    }

    // Добавляем текущий узел как потомка для всех его предков
    if let Some(ancestors) = descendants.get_mut(node) {
        ancestors.push(depth);
    } else {
        descendants.insert(node.clone(), vec![depth]);
    }

    visited.insert(node.clone());

    if let Some(children) = self.adj_list.get(node) {
        for child in children {
            self.dfs_collect(child, depth + 1, descendants, visited);
        }
    }
}
/// Проверяет наличие цикла в графе с использованием алгоритма поиска в глубину (DFS).
/// Этот метод обходит весь граф, начиная с каждого непосещенного узла, и возвращает true,
/// если обнаруживается цикл, и false, если цикла нет.
/// 
/// # Возвращает
/// * `true` — Если в графе есть цикл.
/// * `false` — Если графа ацикличен.

fn has_cycle(&self) -> bool {
    // Создаем два множества для отслеживания состояния узлов:
    let mut visited = HashSet::new(); // Посещенные узлы.
    let mut rec_stack = HashSet::new(); // Узлы в текущем стеке рекурсии (для обнаружения обратных ребер).

    // Проходим по всем узлам графа, чтобы учесть все компоненты связности.
    for node in self.adj_list.keys() {
        // Если для текущего узла обнаружен цикл, завершаем проверку.
        if self.dfs_cycle(node, &mut visited, &mut rec_stack) {
            return true;
        }
    }
    // Если ни для одного узла цикл не найден, граф ацикличен.
    false
}

// Вместо проверки всего графа в has_cycle, можно проверять только пути от нового узла к его родителям. Это требует модификации dfs_cycle:
// В graph.rs:
fn has_cycle_from(&self, start_node: &String) -> bool {
    let mut visited = HashSet::new();
    let mut rec_stack = HashSet::new();
    self.dfs_cycle(start_node, &mut visited, &mut rec_stack)
}

// Используем в add_node_with_parents:
/*for parent in &existing_parents {
    self.add_edge(parent.clone(), node_hash.clone());
    if self.has_cycle_from(&node_hash) {
        self.remove_edge(parent.clone(), node_hash.clone());
        self.remove_node(node_hash.clone());
        return Err("Cycle detected".to_string());
    }
}*/
// Текущая реализация has_cycle выполняет DFS для каждого узла, что даёт сложность O(V + E) для каждого вызова add_node_with_parents. Это дорого, особенно если граф большой.
// Предлагаю кэшировать информацию о циклах или использовать инкрементальную проверку. Для начала можно оптимизировать has_cycle:
// В graph.rs, метод has_cycle:
fn has_cycle(&self) -> bool {
    let mut visited = HashSet::new();
    let mut rec_stack = HashSet::new();
    // Проверяем только узлы, достижимые из новых рёбер, а не весь граф
    for node in self.adj_list.keys() {
        if !visited.contains(node) && self.dfs_cycle(node, &mut visited, &mut rec_stack) {
            return true;
        }
    }
    false
}
/// Вспомогательный метод для проверки цикла с использованием DFS.
/// Использует рекурсивный подход для обхода графа и поиска обратных ребер.
/// 
/// # Аргументы
/// * `node` — Текущий узел (ссылка на String), с которого начинается обход.
/// * `visited` — Множество (HashSet) узлов, которые уже были посещены в процессе обхода.
/// * `rec_stack` — Множество (HashSet) узлов в текущем стеке рекурсии.
/// 
/// # Возвращает
/// * `true` — Если обнаружен цикл.
/// * `false` — Если цикла нет.
fn dfs_cycle(&self, node: &String, visited: &mut HashSet<String>, rec_stack: &mut HashSet<String>) -> bool {
    // Если узел уже находится в стеке рекурсии, это обратное ребро, и мы нашли цикл.
    if rec_stack.contains(node) {
        return true;
    }

    // Если узел уже посещён, но не в текущем стеке, он не создаёт цикла.
    if visited.contains(node) {
        return false;
    }

    // Отмечаем узел как посещённый и добавляем его в стек рекурсии.
    visited.insert(node.clone());
    rec_stack.insert(node.clone());

    // Получаем список дочерних узлов текущего узла, если он есть в графе.
    if let Some(neighbors) = self.adj_list.get(node) {
        // Рекурсивно обходим всех соседей текущего узла.
        for neighbor in neighbors {
            if self.dfs_cycle(neighbor, visited, rec_stack) {
                return true; // Если цикл найден в поддереве, возвращаем true.
            }
        }
    }

    // Удаляем узел из стека рекурсии, так как обход его поддерева завершён.
    rec_stack.remove(node);
    // Если цикла не найдено, возвращаем false.
    false
}

/// Простой метод для получения базовых весов всех узлов
pub fn total(&self) -> Vec<Node> {
    let mut result: Vec<Node> = Vec::new();
    let descendants_map = self.compute_descendants_with_depth_and_weight();
    
    for node in &self.nodes {
        if let Some(descendants) = descendants_map.get(node) {
            let total_weight: f64 = descendants.iter().map(|d| d.weight).sum();
            result.push(Node {
                node: node.clone(),
                weight: total_weight,
            });
        }
    }

    // Сортируем по весу (по убыванию)
    result.sort_by(|a, b| b.weight.partial_cmp(&a.weight).unwrap_or(std::cmp::Ordering::Equal));
    result
}

/// Вычисляет коэффициент k, обратно пропорциональный логарифму N.
/// 
/// # Аргументы
/// * `n` - Входное значение (N >= 1, чтобы избежать log(0) или log(отрицательного)).
/// * `c` - Масштабирующая константа (C > 0).
/// * `base` - Основание логарифма (по умолчанию e, можно передать 10.0 или 2.0).
/// 
/// # Примеры
/// ```
/// let k = log_scaling(100.0, 1.0, 10.0); // k = 1.0 / log10(100) = 0.5
/// ```
/*pub fn log_scaling(n: f64, c: f64, base: f64) -> f64 {
    assert!(n >= 1.0, "N должно быть >= 1, чтобы логарифм был определён.");
    assert!(c > 0.0 && base > 0.0, "C и основание логарифма должны быть положительными.");
    
    c / n.log(base)
}*/

/// Вычисляет коэффициент экспоненциального затухания k = C * exp(-λ * N).
/// 
/// # Аргументы
/// * `n` - Входное значение (N >= 0).
/// * `c` - Начальное значение при N=0 (C > 0).
/// * `lambda` - Коэффициент затухания (λ > 0).
/// 
/// # Пример
/// ```
/// let k = exp_decay(2.0, 1.0, 0.5); // k = 1.0 * exp(-0.5 * 2) ≈ 0.367
/// ```
pub fn exp_decay(n: f64, c: f64, lambda: f64) -> f64 {
    assert!(n >= 0.0 && c > 0.0 && lambda > 0.0, "Некорректные параметры.");
    c * (-lambda * n).exp()
}

async fn get_full_graph_handler(
    State(graph): State<Arc<RwLock<DAG>>>, // Извлекаем граф из состояния.
) -> (StatusCode, Json<FullGraphResponse>) {
    // Получаем блокировку графа для безопасного доступа.
    let graph = match graph.read() {
        Ok(guard) => guard,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(FullGraphResponse {
                    status: "error".to_string(),
                    nodes: vec![],
                    message: Some("Failed to lock graph".to_string()),
                }),
            );
        }
    };

    // Получаем веса всех узлов, отсортированные по убыванию веса и алфавиту.
    let weights = graph.get_all_weights();

    // Кэшируем потомков для всех узлов, чтобы не пересчитывать их повторно.
    let descendants_map = graph.compute_descendants_with_depth_and_weight();

    // Функция для рекурсивного построения иерархии потомков.
    fn build_descendant_hierarchy(
        node: &str,
        descendants_map: &HashMap<String, Vec<NodeInfo>>,
        graph: &DAG,
        visited: &mut HashSet<String>, // Для предотвращения циклов (на случай багов).
    ) -> Vec<DescendantInfo> {
        // Получаем потомков текущего узла.
        let descendants = descendants_map.get(node).unwrap();

        // Строим иерархию только для прямых детей (depth = 1).
        descendants
            .iter()
            .filter(|descendant| descendant.depth == 1) // Берем только прямых детей.
            .map(|descendant| {
                // Проверяем, не посещали ли узел, чтобы избежать бесконечной рекурсии.
                if visited.contains(&descendant.node) {
                    return DescendantInfo {
                        hash: descendant.node.clone(),
                        depth: descendant.depth,
                        weight: descendant.weight,
                        descendants: Vec::new(), // Пустой список, если узел уже обработан.
                    };
                }

                // Добавляем узел в visited.
                visited.insert(descendant.node.clone());

                // Рекурсивно строим потомков для текущего узла.
                let child_descendants = build_descendant_hierarchy(
                    &descendant.node,
                    descendants_map,
                    graph,
                    visited,
                );

                // Удаляем узел из visited после обработки, чтобы он мог быть переиспользован в других ветках.
                visited.remove(&descendant.node);

                DescendantInfo {
                    hash: descendant.node.clone(),
                    depth: descendant.depth,
                    weight: descendant.weight,
                    descendants: child_descendants,
                }
            })
            .collect::<Vec<DescendantInfo>>()
    }

    // Собираем информацию о каждом узле: хэш, вес, иерархические потомки.
    let nodes = weights
        .into_iter()
        .map(|node| {
            // Инициализируем множество посещенных узлов для текущего узла.
            let mut visited = HashSet::new();
            // Строим иерархию потомков для текущего узла.
            let descendants = build_descendant_hierarchy(
                &node.node,
                &descendants_map,
                &graph,
                &mut visited,
            );

            NodeFullInfo {
                hash: node.node,
                weight: node.weight,
                descendants,
            }
        })
        .collect::<Vec<NodeFullInfo>>();

    // Возвращаем успешный ответ с полной информацией о графе.
    (
        StatusCode::OK,
        Json(FullGraphResponse {
            status: "success".to_string(),
            nodes,
            message: None,
        }),
    )
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
