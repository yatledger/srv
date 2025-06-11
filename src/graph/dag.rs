// Импортируем необходимые коллекции из стандартной библиотеки Rust для работы с графом.
// HashMap используется для хранения списка смежности, а HashSet — для проверки циклов в DFS.
use std::collections::{HashMap, HashSet};
use std::fs; // Для чтения файла.
use std::sync::{Arc};
use serde::Deserialize; // Для десериализации JSON.
use rand::Rng; // Для генерации случайных чисел
use serde_json::Value;
use crate::graph::weights::{Node, NodeInfo, compute_weights, compute_descendants_with_depth_and_weight};
use crate::Adjacency;
use tracing::{info, error, debug};


// Структура для десериализации JSON с генезис-транзакциями.
#[derive(Deserialize)]
struct GenesisTransaction {
    hash: String, // Хэш генезис-узла.
    data: Value
}



pub struct DAG {
    // Граф: узел -> список детей
    childrens: Adjacency,
    // Обратный граф: узел -> список родителей  
    parents: Adjacency,
    // Все узлы
    nodes: HashSet<Arc<str>>,
    weights: Vec<Node>,
    data: HashMap<Arc<str>, Arc<Value>>,
    additions_since_last_weight_calc: usize,
}


impl DAG {
    pub fn new() -> Self {
        let mut graph = DAG {
            childrens: HashMap::new(),
            parents: HashMap::new(),
            nodes: HashSet::new(),
            weights: Vec::new(),
            data: HashMap::new(),
            additions_since_last_weight_calc: 0,
        };
        let genesis_data = fs::read_to_string("genesis.json").map_err(|e| {
            error!("Failed to read genesis.json: {}", e);
            e
        });
        if let Ok(d) = genesis_data {
            match serde_json::from_str::<Vec<GenesisTransaction>>(&d) {
                Ok(transactions) => {
                    // Итерируемся по ссылке на transactions, чтобы не перемещать вектор.
                    for tx in &transactions {
                        let hash = Arc::from(tx.hash.as_str());
                        let data = Arc::new(tx.data.clone());
                        graph.add_node(hash, data);
                        graph.calculate_weights();
                        
                    }
                    info!("Loaded {} genesis transactions from genesis.json", transactions.len());
                }
                Err(e) => {
                    error!("Failed to parse genesis.json: {}", e);
                }
            }
        }
        // Добавляем тестовые данные
        // Узлы C, D с родителем A
        /*graph.add_node("A".to_string());
        graph.add_node("B".to_string());
        let _ = graph.add_node_with_parents("C".to_string(), vec!["A".to_string()]);
        let _ = graph.add_node_with_parents("D".to_string(), vec!["A".to_string()]);

        // Узлы E, F с родителем B
        let _ = graph.add_node_with_parents("E".to_string(), vec!["B".to_string()]);
        let _ = graph.add_node_with_parents("F".to_string(), vec!["A".to_string(), "B".to_string()]);

        // Узел G с родителями C, D
        let _ = graph.add_node_with_parents("G".to_string(), vec!["C".to_string(), "D".to_string()]);

        // Узел H с родителем D
        let _ = graph.add_node_with_parents("H".to_string(), vec!["D".to_string()]);

        // Узлы I, J с родителем E
        let _ = graph.add_node_with_parents("I".to_string(), vec!["E".to_string()]);
        let _ = graph.add_node_with_parents("J".to_string(), vec!["E".to_string()]);

        // Узел K с родителем F
        let _ = graph.add_node_with_parents("K".to_string(), vec!["F".to_string()]);

        // Узел L с родителями G, H
        let _ = graph.add_node_with_parents("L".to_string(), vec!["G".to_string(), "H".to_string()]);

        // Узел M с родителем H
        let _ = graph.add_node_with_parents("M".to_string(), vec!["H".to_string()]);

        // Узлы N, O с родителем I
        let _ = graph.add_node_with_parents("N".to_string(), vec!["I".to_string()]);
        let _ = graph.add_node_with_parents("O".to_string(), vec!["I".to_string()]);

        // Узел P с родителями J, K
        let _ = graph.add_node_with_parents("P".to_string(), vec!["J".to_string(), "K".to_string()]);

        // Узел Q с родителем L
        let _ = graph.add_node_with_parents("Q".to_string(), vec!["L".to_string()]);

        // Узлы R, S с родителем M
        let _ = graph.add_node_with_parents("R".to_string(), vec!["M".to_string()]);
        let _ = graph.add_node_with_parents("S".to_string(), vec!["M".to_string()]);

        // Узел T с родителем N
        let _ = graph.add_node_with_parents("T".to_string(), vec!["N".to_string()]);

        // Узел U с родителем P
        let _ = graph.add_node_with_parents("U".to_string(), vec!["P".to_string()]);

        // Узел V с родителем Q
        let _ = graph.add_node_with_parents("V".to_string(), vec!["Q".to_string()]);

        // Узел W с родителем T
        let _ = graph.add_node_with_parents("W".to_string(), vec!["T".to_string()]);

        // Узел X с родителями U, V
        let _ = graph.add_node_with_parents("X".to_string(), vec!["U".to_string(), "V".to_string()]);

        // Узел Y с родителем W
        let _ = graph.add_node_with_parents("Y".to_string(), vec!["W".to_string()]);

        // Узел Z с родителем X
        let _ = graph.add_node_with_parents("Z".to_string(), vec!["X".to_string()]);*/

        graph
    }

    fn calculate_weights(&mut self) {
        debug!("Calculate weights for {} nodes", self.additions_since_last_weight_calc);
        self.weights = compute_weights(&self.childrens, &self.nodes);
        self.additions_since_last_weight_calc = 0;
    }

    pub fn get_node_data(&self, node: &str) -> Option<&Value> {
        self.data.get(node).map(|arc| arc.as_ref())
    }

    /// Возвращает неизменяемую ссылку на список смежности графа.
    pub fn get_childrens(&self) -> &Adjacency {
        &self.childrens
    }

    pub fn get_parents(&self) -> &Adjacency {
        &self.parents
    }

    pub fn get_weights(&self) -> &Vec<Node> {
        &self.weights
    }

    pub fn get_nodes(&self) -> &HashSet<Arc<str>> {
        &self.nodes
    }

    pub fn get_node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn contains_node(&self, node: &str) -> bool {
        self.nodes.contains(node)
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
    pub fn add_node_with_parents(&mut self, node: Arc<str>, parents: Vec<Arc<str>>, data: Arc<Value>) -> Result<(), String> {
        // Проверяем, существует ли уже узел с таким хэшем в графе.
        // Это важно для предотвращения дублирования узлов и сохранения уникальности идентификаторов.
        if self.contains_node(&node) {
            info!("Node already exists");
            return Err("Node already exists".to_string()); // Возвращаем ошибку, если узел уже есть.
        }

        // Добавляем новый узел в граф с пустым списком дочерних узлов.
        // Используем clone(), так как node_hash будет использоваться дальше.
        self.add_node(node.clone(), data);

        // Фильтруем список родителей, оставляя только те узлы, которые уже существуют в графе.
        // Используем into_iter() для владения значениями из вектора parents и collect для создания нового вектора.
        let existing_parents = parents
            .into_iter()
            .filter(|p| self.childrens.contains_key(p)) // Проверяем наличие каждого родителя в adj_list.
            .collect::<Vec<_>>();

        // Если после фильтрации не осталось ни одного существующего родителя,
        // добавление узла невозможно, так как он должен быть связан хотя бы с одним узлом.
        // TODO: 2 parents!
        if !existing_parents.is_empty() {
            // Проходим по каждому существующему родителю и добавляем ребро от него к новому узлу.
            for parent in &existing_parents {
                self.add_edge(parent.clone(), node.clone());

                // После добавления ребра проверяем, не образовался ли цикл в графе.
                /*if self.has_cycle() {
                    // Если цикл обнаружен, откатываем изменения:
                    self.remove_edge(parent.clone(), node_hash.clone()); // Удаляем только что добавленное ребро.
                    self.remove_node(node_hash.clone()); // Удаляем сам узел из графа.
                    return Err("Cycle detected".to_string()); // Возвращаем ошибку о цикле.
                }*/
            }
        }

        // Увеличиваем счетчик добавлений
        self.additions_since_last_weight_calc += 1;

        // Вероятностный пересчет весов или пересчет по порогу
        let mut rng = rand::rngs::ThreadRng::default();
        let should_recalculate = rng.random::<f64>() < 0.1 || self.additions_since_last_weight_calc >= 10;
        if should_recalculate {
            self.calculate_weights();
        }
        Ok(())
    }

    /// Добавляет новый узел в граф без указания родителей.
    fn add_node(&mut self, node: Arc<str>, data: Arc<Value>) {
        // Используем метод entry для доступа к записи в HashMap.
        // or_insert_with добавляет пустой вектор, если узла ещё нет, и ничего не делает, если узел уже существует.
        // [ ] Doooo
        // [x] Done
        // HACK: hack
        // BUG: bug
        // XXX: xxx
        // FIXME: fix
        self.nodes.insert(node.clone());
        self.childrens.entry(node.clone()).or_insert_with(Vec::new); // TODO: разобраться как правильней сосдавать новый узел
        self.parents.entry(node.clone()).or_insert_with(Vec::new);
        self.data.insert(node, data);
    }

    /// Добавляет направленное ребро от узла `from` к узлу `to`.
    fn add_edge(&mut self, from: Arc<str>, to: Arc<str>) {
        // Используем entry для доступа к записи узла `from` в HashMap.
        // and_modify изменяет существующий вектор дочерних узлов, добавляя `to`.
        // or_insert_with создает новый вектор с `to`, если узла `from` ещё нет.
        self.childrens.entry(from.clone()).and_modify(|edges| edges.push(to.clone())).or_insert_with(|| vec![to.clone()]);
        self.parents.entry(to.clone()).and_modify(|edges| edges.push(from.clone())).or_insert_with(|| vec![from]);
    }

    /// Удаляет ребро от узла `from` к узлу `to`.
    /*fn remove_edge(&mut self, from: String, to: String) {
        if let Some(edges) = self.adj_list.get_mut(&from) {
            edges.retain(|e| e != &to);
        }
        if let Some(edges) = self.reverse_adj_list.get_mut(&to) {
            edges.retain(|e| e != &from);
        }
    }*/

    /// Удаляет узел из графа и все ребра, которые ведут к нему.
    /// Удаляет узел из графа и все связанные с ним рёбра.
    /// 
    /// # Аргументы
    /// * `node` - Хэш узла для удаления.
    /// * `sync` - Если Some(true), пересчитывает веса графа после удаления; если Some(false) или None, веса не пересчитываются.
    pub fn remove_node(&mut self, node: Arc<str>) {
        if let Some(children) = self.childrens.remove(&node) {
            for child in &children {
                if let Some(parents) = self.parents.get_mut(child) {
                    parents.retain(|p| p != &node);
                }
            }
        }
        if let Some(parents) = self.parents.remove(&node) {
            for parent in &parents {
                if let Some(children) = self.childrens.get_mut(parent) {
                    children.retain(|c| c != &node);
                }
            }
        }
        self.nodes.remove(&node);
        self.data.remove(&node);
    }

    /// Удаляет список узлов из графа параллельно с использованием Rayon.
    /// Использует Arc<RwLock<DAG>> для потокобезопасного доступа и вызывает remove_node для каждого узла.
    /// 
    /// # Аргументы
    /// * `graph` - Потокобезопасный граф, обёрнутый в Arc<RwLock<DAG>>.
    /// * `nodes` - Вектор хэшей узлов (Vec<String>) для удаления.
    /// 
    /// # Логика
    /// 1. Проверяется наличие всех узлов в графе с блокировкой чтения.
    /// 2. Для каждого узла параллельно:
    ///    - Получается блокировка записи через RwLock.
    ///    - Вызывается remove_node с sync = Some(false) для удаления узла без пересчёта весов.
    /// 3. После всех удалений пересчитываются веса с одной блокировкой записи.
    /// 
    /// # Возвращает
    /// * `Ok(())` - Если все узлы успешно удалены.
    /// * `Err(String)` - Если хотя бы один узел не существует или не удалось получить блокировку.
    pub fn remove_nodes(&mut self, nodes: Vec<Arc<str>>) -> Result<(), String> {
        for node in nodes {
            if self.contains_node(&node) {
                self.remove_node(node);
            }
        }

        // Увеличиваем счетчик добавлений
        self.additions_since_last_weight_calc += 1;

        // Вероятностный пересчет весов или пересчет по порогу
        self.calculate_weights();
        Ok(())
    }

    pub fn compute_descendants_with_depth_and_weight(&self) -> HashMap<Arc<str>, Vec<NodeInfo>> {
        compute_descendants_with_depth_and_weight(
            &self.childrens,
            &self.nodes
        )
    }
}