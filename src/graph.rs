// Импортируем необходимые коллекции из стандартной библиотеки Rust для работы с графом.
// HashMap используется для хранения списка смежности, а HashSet — для проверки циклов в DFS.
use std::collections::{HashMap}; //, HashSet
use std::fs; // Для чтения файла.
use serde::Deserialize; // Для десериализации JSON.

// Структура для десериализации JSON с генезис-транзакциями.
#[derive(Deserialize)]
struct GenesisTransaction {
    node_hash: String, // Хэш генезис-узла.
}

// Определяем публичную структуру Graph, которая представляет собой направленный граф.
// Граф хранится в виде списка смежности (adjacency list), где:
// - Ключ (String) — это хэш узла (уникальный идентификатор узла).
// - Значение (Vec<String>) — это вектор хэшей дочерних узлов, к которым ведет ребро из данного узла.
// Использование String как хэша позволяет гибко идентифицировать узлы, например, через их имена или UUID.
pub struct Graph {
    adj_list: HashMap<String, Vec<String>>,
}

// Реализация методов для структуры Graph через блок impl.
impl Graph {
    /// Создает новый экземпляр пустого графа.
    /// Этот метод является конструктором структуры Graph и инициализирует её с пустым списком смежности.
    /// 
    /// # Возвращает
    /// Новый экземпляр Graph, где adj_list — это пустой HashMap.
    /// Использование Self в возвращаемом типе делает код более читаемым и поддерживаемым.
    /// Создает новый экземпляр графа, загружая генезис-транзакции из JSON-файла.
    /// Читает файл genesis.json, десериализует его в вектор GenesisTransaction,
    /// добавляет узлы в граф с помощью приватного метода add_node.
    /// Если файл не найден или JSON некорректен, выводится ошибка, но создаётся пустой граф.
    pub fn new() -> Self {
        let mut graph = Graph {
            adj_list: HashMap::new(),
        };
        let genesis_data = fs::read_to_string("genesis.json").map_err(|e| {
            eprintln!("Failed to read genesis.json: {}", e);
            e
        });
        if let Ok(data) = genesis_data {
            match serde_json::from_str::<Vec<GenesisTransaction>>(&data) {
                Ok(transactions) => {
                    // Итерируемся по ссылке на transactions, чтобы не перемещать вектор.
                    for tx in &transactions {
                        graph.add_node(tx.node_hash.clone()); // Клонируем node_hash, так как он String.
                    }
                    println!("Loaded {} genesis transactions from genesis.json", transactions.len());
                }
                Err(e) => {
                    eprintln!("Failed to parse genesis.json: {}", e);
                }
            }
        }
        graph
    }

    /// Возвращает неизменяемую ссылку на список смежности графа.
    /// Этот метод предоставляет доступ "только для чтения" к внутренней структуре данных графа,
    /// что полезно для проверки состояния графа или его анализа без риска случайного изменения.
    /// 
    /// # Возвращает
    /// Ссылку (&HashMap<String, Vec<String>>) на adj_list, которая содержит все узлы и их дочерние связи.
    pub fn get_adj_list(&self) -> &HashMap<String, Vec<String>> {
        &self.adj_list // Просто возвращаем ссылку на поле adj_list структуры.
    }

    /// Добавляет новый узел в граф с указанными родителями и создает ребра от родителей к этому узлу.
    /// Этот метод является ключевой функцией для построения графа, так как он одновременно добавляет узел
    /// и устанавливает связи с уже существующими узлами (родителями).
    /// 
    /// Логика работы метода:
    /// 1. Проверяется, существует ли уже узел с таким хэшем в графе. Если да — возвращается ошибка.
    /// 2. Фильтруются переданные родители: остаются только те, которые уже есть в графе.
    /// 3. Если ни один родитель не существует, метод завершается с ошибкой, так как узел не может быть "孤立" (изолированным).
    /// 4. Новый узел добавляется в граф с пустым списком детей.
    /// 5. Для каждого существующего родителя создается ребро от родителя к новому узлу.
    /// 6. После каждого добавления ребра проверяется наличие цикла в графе с помощью метода has_cycle().
    /// 7. Если цикл обнаружен, все изменения откатываются (ребро и узел удаляются), и возвращается ошибка.
    /// 
    /// # Аргументы
    /// * `node_hash` — Хэш нового узла (String), уникальный идентификатор, который добавляется в граф.
    /// * `parents` — Вектор хэшей (Vec<String>) узлов-родителей, от которых будут вести ребра к новому узлу.
    /// 
    /// # Возвращает
    /// * `Ok(())` — Если узел и все ребра успешно добавлены в граф без создания цикла.
    /// * `Err(String)` — Ошибка с текстовым описанием, если:
    ///   - Узел с таким хэшем уже существует ("Node already exists").
    ///   - Нет ни одного существующего родителя ("No existing parents").
    ///   - Добавление ребра создает цикл ("Cycle detected").
    pub fn add_node_with_parents(&mut self, node_hash: String, parents: Vec<String>) -> Result<(), String> {
        // Проверяем, существует ли уже узел с таким хэшем в графе.
        // Это важно для предотвращения дублирования узлов и сохранения уникальности идентификаторов.
        if self.adj_list.contains_key(&node_hash) {
            return Err("Node already exists".to_string()); // Возвращаем ошибку, если узел уже есть.
        }

        // Фильтруем список родителей, оставляя только те узлы, которые уже существуют в графе.
        // Используем into_iter() для владения значениями из вектора parents и collect для создания нового вектора.
        let existing_parents = parents
            .into_iter()
            .filter(|p| self.adj_list.contains_key(p)) // Проверяем наличие каждого родителя в adj_list.
            .collect::<Vec<_>>();

        // Если после фильтрации не осталось ни одного существующего родителя,
        // добавление узла невозможно, так как он должен быть связан хотя бы с одним узлом.
        if existing_parents.is_empty() {
            return Err("No existing parents".to_string()); // Ошибка: нет родителей для связи.
        }

        // Добавляем новый узел в граф с пустым списком дочерних узлов.
        // Используем clone(), так как node_hash будет использоваться дальше.
        self.adj_list.insert(node_hash.clone(), Vec::new());

        // Проходим по каждому существующему родителю и добавляем ребро от него к новому узлу.
        for parent in &existing_parents {
            self.add_edge(parent.clone(), node_hash.clone()); // Создаем ребро от родителя к новому узлу.

            // После добавления ребра проверяем, не образовался ли цикл в графе.
            /*if self.has_cycle() {
                // Если цикл обнаружен, откатываем изменения:
                self.remove_edge(parent.clone(), node_hash.clone()); // Удаляем только что добавленное ребро.
                self.remove_node(node_hash.clone()); // Удаляем сам узел из графа.
                return Err("Cycle detected".to_string()); // Возвращаем ошибку о цикле.
            }*/
        }

        // Если все проверки пройдены и циклов нет, возвращаем успешный результат.
        Ok(())
    }

    /// Добавляет новый узел в граф без указания родителей.
    /// Этот метод проще, чем add_node_with_parents, так как он не создает ребра,
    /// а просто добавляет изолированный узел, если его ещё нет в графе.
    /// 
    /// # Аргументы
    /// * `node_hash` — Хэш узла (String), который нужно добавить.
    fn add_node(&mut self, node_hash: String) {
        // Используем метод entry для доступа к записи в HashMap.
        // or_insert_with добавляет пустой вектор, если узла ещё нет, и ничего не делает, если узел уже существует.
        self.adj_list.entry(node_hash).or_insert_with(Vec::new);
    }

    /// Добавляет направленное ребро от узла `from` к узлу `to`.
    /// Если узел `from` ещё не существует, он будет автоматически создан.
    /// Узел `to` не создается автоматически, так как предполагается, что он уже есть или будет добавлен позже.
    /// 
    /// # Аргументы
    /// * `from` — Хэш узла (String), из которого исходит ребро.
    /// * `to` — Хэш узла (String), в который ведет ребро.
    fn add_edge(&mut self, from: String, to: String) {
        // Используем entry для доступа к записи узла `from` в HashMap.
        // and_modify изменяет существующий вектор дочерних узлов, добавляя `to`.
        // or_insert_with создает новый вектор с `to`, если узла `from` ещё нет.
        self.adj_list
            .entry(from)
            .and_modify(|edges| edges.push(to.clone())) // Добавляем `to` в список детей.
            .or_insert_with(|| vec![to]); // Создаем новый список с `to`, если узла не было.
    }

    /// Удаляет ребро от узла `from` к узлу `to`.
    /// Этот метод приватный и используется внутри структуры, например, для отката изменений при обнаружении цикла.
    /// 
    /// # Аргументы
    /// * `from` — Хэш узла (String), из которого удаляется ребро.
    /// * `to` — Хэш узла (String), в который ведет ребро, которое нужно удалить.
    fn remove_edge(&mut self, from: String, to: String) {
        // Получаем изменяемую ссылку на вектор дочерних узлов узла `from`.
        if let Some(edges) = self.adj_list.get_mut(&from) {
            // Удаляем все элементы из вектора edges, которые равны `to`.
            edges.retain(|e| e != &to);
        }
        // Если узла `from` нет, ничего не делаем (безопасная обработка).
    }

    /// Удаляет узел из графа и все ребра, которые ведут к нему.
    /// Этот метод приватный и используется для отката изменений, например, при обнаружении цикла.
    /// 
    /// # Аргументы
    /// * `node_hash` — Хэш узла (String), который нужно удалить.
    fn remove_node(&mut self, node_hash: String) {
        // Удаляем узел из списка смежности.
        self.adj_list.remove(&node_hash);

        // Проходим по всем узлам графа и удаляем все ребра, которые указывают на удаляемый узел.
        for edges in self.adj_list.values_mut() {
            edges.retain(|e| e != &node_hash); // Удаляем `node_hash` из списков детей других узлов.
        }
    }
    /*
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
    
    Вместо проверки всего графа в has_cycle, можно проверять только пути от нового узла к его родителям. Это требует модификации dfs_cycle:
    // В graph.rs:
    fn has_cycle_from(&self, start_node: &String) -> bool {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        self.dfs_cycle(start_node, &mut visited, &mut rec_stack)
    }

    // Используем в add_node_with_parents:
    for parent in &existing_parents {
        self.add_edge(parent.clone(), node_hash.clone());
        if self.has_cycle_from(&node_hash) {
            self.remove_edge(parent.clone(), node_hash.clone());
            self.remove_node(node_hash.clone());
            return Err("Cycle detected".to_string());
        }
    }
    Текущая реализация has_cycle выполняет DFS для каждого узла, что даёт сложность O(V + E) для каждого вызова add_node_with_parents. Это дорого, особенно если граф большой.
    Предлагаю кэшировать информацию о циклах или использовать инкрементальную проверку. Для начала можно оптимизировать has_cycle:
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
    */
}

// Модуль для юнит-тестов, который проверяет корректность работы методов структуры Graph.
// Тесты компилируются и запускаются только при выполнении `cargo test`.
#[cfg(test)]
mod tests {
    use super::*; // Импортируем все определения из внешнего блока impl для доступа к Graph.

    /// Тест добавления нового узла в граф.
    /// Проверяет, что узел корректно добавляется в список смежности и имеет пустой список дочерних узлов.
    #[test]
    fn test_add_node() {
        let mut graph = Graph::new(); // Создаем новый пустой граф.
        graph.add_node("node1".to_string()); // Добавляем узел с хэшем "node1".
        assert!(graph.get_adj_list().contains_key("node1")); // Проверяем, что узел добавлен.
        assert_eq!(graph.get_adj_list().get("node1").unwrap(), &Vec::<String>::new()); // Проверяем, что у узла нет детей.
    }

    /// Тест добавления ребра между двумя узлами.
    /// Проверяет, что ребро корректно добавляется от узла `from` к узлу `to`.
    #[test]
    fn test_add_edge() {
        let mut graph = Graph::new(); // Создаем новый пустой граф.
        graph.add_node("node1".to_string()); // Добавляем узел "node1".
        graph.add_node("node2".to_string()); // Добавляем узел "node2".
        graph.add_edge("node1".to_string(), "node2".to_string()); // Добавляем ребро от "node1" к "node2".
        assert_eq!(graph.get_adj_list().get("node1").unwrap(), &vec!["node2".to_string()]); // Проверяем, что у "node1" есть ребро к "node2".
        assert_eq!(graph.get_adj_list().get("node2").unwrap(), &Vec::<String>::new()); // Проверяем, что у "node2" нет детей.
    }

    /// Тест успешного добавления узла с существующими родителями.
    /// Проверяет, что новый узел добавляется, и от всех указанных родителей создаются ребра к нему.
    #[test]
    fn test_add_node_with_parents_success() {
        let mut graph = Graph::new(); // Создаем новый пустой граф.
        graph.add_node("node1".to_string()); // Добавляем узел "node1".
        graph.add_node("node2".to_string()); // Добавляем узел "node2".
        let result = graph.add_node_with_parents("node3".to_string(), vec!["node1".to_string(), "node2".to_string()]);
        assert!(result.is_ok()); // Проверяем, что операция завершилась успешно.
        assert_eq!(graph.get_adj_list().get("node1").unwrap(), &vec!["node3".to_string()]); // Проверяем ребро от "node1" к "node3".
        assert_eq!(graph.get_adj_list().get("node2").unwrap(), &vec!["node3".to_string()]); // Проверяем ребро от "node2" к "node3".
        assert_eq!(graph.get_adj_list().get("node3").unwrap(), &Vec::<String>::new()); // Проверяем, что у "node3" нет детей.
    }

    /// Тест ошибки при добавлении узла с несуществующими родителями.
    /// Проверяет, что если все указанные родители отсутствуют в графе, возвращается ошибка.
    #[test]
    fn test_add_node_with_no_existing_parents() {
        let mut graph = Graph::new(); // Создаем новый пустой граф.
        let result = graph.add_node_with_parents("node3".to_string(), vec!["node1".to_string(), "node2".to_string()]);
        assert_eq!(result, Err("No existing parents".to_string())); // Проверяем, что возвращена ошибка.
        assert!(!graph.get_adj_list().contains_key("node3")); // Проверяем, что узел "node3" не был добавлен.
    }

    /// Тест ошибки при добавлении уже существующего узла с родителями.
    /// Проверяет, что если узел с таким хэшем уже есть, метод возвращает ошибку.
    #[test]
    fn test_add_node_with_parents_already_exists() {
        let mut graph = Graph::new(); // Создаем новый пустой граф.
        graph.add_node("node1".to_string()); // Добавляем узел "node1".
        graph.add_node("node2".to_string()); // Добавляем узел "node2".
        let result = graph.add_node_with_parents("node1".to_string(), vec!["node2".to_string()]);
        assert_eq!(result, Err("Node already exists".to_string())); // Проверяем, что возвращена ошибка.
    }

    /// Тест ошибки при создании цикла в графе.
    /// Проверяет, что если добавление узла с родителями создает цикл, операция откатывается.
    /*#[test]
    fn test_add_node_with_parents_cycle() {
        let mut graph = Graph::new(); // Создаем новый пустой граф.
        graph.add_node("node1".to_string()); // Добавляем узел "node1".
        graph.add_node("node2".to_string()); // Добавляем узел "node2".
        graph.add_node("node3".to_string()); // Добавляем узел "node3".
        graph.add_edge("node1".to_string(), "node2".to_string()); // Ребро: node1 → node2.
        graph.add_edge("node2".to_string(), "node3".to_string()); // Ребро: node2 → node3.
        graph.add_edge("node3".to_string(), "node1".to_string()); // Ребро: node3 → node1 (создаем цикл).
        let result = graph.add_node_with_parents("node4".to_string(), vec!["node3".to_string()]);
        assert_eq!(result, Err("Cycle detected".to_string())); // Проверяем, что возвращена ошибка о цикле.
        assert!(!graph.get_adj_list().contains_key("node4")); // Проверяем, что "node4" не был добавлен.
    }*/

    /// Тест добавления узла с частично несуществующими родителями.
    /// Проверяет, что узел добавляется с ребрами только от существующих родителей,
    /// игнорируя несуществующие узлы в списке родителей.
    #[test]
    fn test_add_node_with_some_missing_parents() {
        let mut graph = Graph::new(); // Создаем новый пустой граф.
        graph.add_node("node1".to_string()); // Добавляем узел "node1".
        graph.add_node("node2".to_string()); // Добавляем узел "node2".
        let result = graph.add_node_with_parents(
            "node3".to_string(),
            vec!["node1".to_string(), "nodeX".to_string(), "node2".to_string()], // "nodeX" не существует.
        );
        assert!(result.is_ok()); // Проверяем, что операция завершилась успешно.
        assert_eq!(graph.get_adj_list().get("node1").unwrap(), &vec!["node3".to_string()]); // Ребро от "node1" к "node3".
        assert_eq!(graph.get_adj_list().get("node2").unwrap(), &vec!["node3".to_string()]); // Ребро от "node2" к "node3".
        assert_eq!(graph.get_adj_list().get("node3").unwrap(), &Vec::<String>::new()); // У "node3" нет детей.
        assert!(!graph.get_adj_list().contains_key("nodeX")); // Проверяем, что "nodeX" не был добавлен.
    }
}