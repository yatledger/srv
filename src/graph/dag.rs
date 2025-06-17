// Импортируем необходимые коллекции из стандартной библиотеки Rust для работы с графом.
// HashMap используется для хранения списка смежности, а HashSet — для проверки циклов в DFS.
use std::collections::{HashMap, HashSet};
use std::fs; // Для чтения файла.
use std::sync::{Arc};
use serde::Deserialize; // Для десериализации JSON.
use rand::Rng; // Для генерации случайных чисел
use serde_json::Value;
use crate::graph::weights::{NodeWeight, NodeInfo, compute_weights, compute_descendants_with_depth_and_weight};
use crate::Adjacency;
use tracing::{info, error};


// Структура для десериализации JSON с генезис-транзакциями.
#[derive(Deserialize)]
struct GenesisTransaction {
    hash: String, // Хэш генезис-узла.
    data: Value
}

pub struct Node {
    pub parents: HashSet<Arc<str>>,
    pub children: HashSet<Arc<str>>,
    pub data: Arc<Value>,
    pub weight: f64,
}


pub struct DAG {
    nodes: HashMap<Arc<str>, Node>,
    additions_since_last_weight_calc: usize,
}


impl DAG {
    pub fn new() -> Self {
        let mut dag = DAG {
            nodes: HashMap::new(),
            additions_since_last_weight_calc: 0,
        };
        // Загрузка генезис-транзакций
        if let Ok(genesis_content) = fs::read_to_string("genesis.json") {
            match serde_json::from_str::<Vec<GenesisTransaction>>(&genesis_content) {
                Ok(transactions) => {
                    for tx in &transactions {
                        let hash = Arc::from(tx.hash.as_str());
                        let data = Arc::new(tx.data.clone());
                        
                        // NEW: Создаем полноценный GraphNode для генезис-узла.
                        let genesis_node = Node {
                            parents: HashSet::new(),
                            children: HashSet::new(),
                            data,
                            weight: 0.0, // Вес будет рассчитан ниже
                        };
                        dag.nodes.insert(hash, genesis_node);
                    }
                    // Рассчитываем веса после загрузки всех генезис-узлов.
                    dag.calculate_weights();
                    info!("Loaded {} genesis transactions from genesis.json", transactions.len());
                }
                Err(e) => error!("Failed to parse genesis.json: {}", e),
            }
        } else {
             error!("Could not read genesis.json, starting with an empty graph.");
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

        dag
    }

    // REFACTORED: Логика пересчета весов.
    fn calculate_weights(&mut self) {
        // Передаем текущие узлы в модуль расчета весов.
        let weights_map = compute_weights(&self.nodes);
        
        // Обновляем вес для каждого узла в графе.
        for (hash, node) in self.nodes.iter_mut() {
            node.weight = *weights_map.get(hash).unwrap_or(&0.0);
        }

        self.additions_since_last_weight_calc = 0;
    }

    // REFACTORED: Получение данных узла из новой структуры.
    pub fn get_node_data(&self, node: &str) -> Option<&Value> {
        self.nodes.get(node).map(|n| n.data.as_ref())
    }

    // REFACTORED: Метод теперь "на лету" собирает список смежности детей.
    pub fn get_childrens(&self) -> Adjacency {
        self.nodes
            .iter()
            .map(|(hash, node)| (hash.clone(), node.children.iter().cloned().collect()))
            .collect()
    }
    
    // REFACTORED: Метод теперь "на лету" собирает список смежности родителей.
    pub fn get_parents(&self) -> Adjacency {
        self.nodes
            .iter()
            .map(|(hash, node)| (hash.clone(), node.parents.iter().cloned().collect()))
            .collect()
    }

    // REFACTORED: Метод теперь "на лету" собирает веса всех узлов.
    pub fn get_weights(&self) -> Vec<NodeWeight> {
        self.nodes
            .iter()
            .map(|(hash, node)| NodeWeight {
                node: hash.clone(),
                weight: node.weight,
            })
            .collect()
    }

    // REFACTORED: Возвращает `true`, если узел существует в `nodes`.
    pub fn contains_node(&self, node: &str) -> bool {
        self.nodes.contains_key(node)
    }

    // REFACTORED: Возвращает количество узлов.
    pub fn get_node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn add_node_with_parents(&mut self, node: Arc<str>, parents: Vec<Arc<str>>, data: Arc<Value>) -> Result<(), String> {
        if self.nodes.contains_key(&node) {
            return Err("Node already exists".to_string());
        }

        // Фильтруем список родителей, оставляя только те узлы, которые уже существуют в графе.
        // Используем into_iter() для владения значениями из вектора parents и collect для создания нового вектора.
        let existing_parents = parents
            .into_iter()
            .filter(|p| self.nodes.contains_key(p))
            .collect::<HashSet<_>>();

        // TODO: 2 parents!
        if !existing_parents.is_empty() {
            // Проходим по каждому существующему родителю и добавляем ребро от него к новому узлу.
            for parent_hash in &existing_parents {
                if let Some(parent_node) = self.nodes.get_mut(parent_hash) {
                    parent_node.children.insert(node.clone());
                }
            }
        }

        // Создаем и вставляем новый узел
        let new_node = Node {
            parents: existing_parents,
            children: HashSet::new(),
            data,
            weight: 0.0, // Вес будет пересчитан позже
        };
        self.nodes.insert(node, new_node);

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

    pub fn remove_node(&mut self, node: Arc<str>) {
        if let Some(removed_node) = self.nodes.remove(&node) {
            // Удаляем ссылку на удаленный узел у его родителей
            for parent_hash in &removed_node.parents {
                if let Some(parent_node) = self.nodes.get_mut(parent_hash) {
                    parent_node.children.remove(&node);
                }
            }
            // Удаляем ссылку на удаленный узел у его детей
            for child_hash in &removed_node.children {
                if let Some(child_node) = self.nodes.get_mut(child_hash) {
                    child_node.parents.remove(&node);
                }
            }
        }
        self.nodes.remove(&node);
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
        let node_keys: HashSet<Arc<str>> = self.nodes.keys().cloned().collect();
        compute_descendants_with_depth_and_weight(&self.nodes, &node_keys)
    }
}