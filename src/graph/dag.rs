// Импортируем необходимые коллекции из стандартной библиотеки Rust для работы с графом.
// HashMap используется для хранения списка смежности, а HashSet — для проверки циклов в DFS.
use std::collections::{HashMap, HashSet};
use std::fs; // Для чтения файла.
use std::sync::{Arc};
use serde::{Deserialize, Serialize};
use serde::ser::{Serializer, SerializeMap};
use serde::de::{Deserializer, MapAccess, Visitor};
use std::fmt;
use serde_json::Value;
use crate::graph::weights::{NodeWeight, NodeInfo, compute_descendants_with_depth_and_weight};
use crate::Adjacency;
use tracing::{info, error};


// Структура для десериализации JSON с генезис-транзакциями.
#[derive(Deserialize)]
struct GenesisTransaction {
    hash: String, // Хэш генезис-узла.
    data: Value
}

#[derive(Debug, Clone, Default)]
pub struct Node {
    pub parents: HashSet<Arc<str>>,
    pub children: HashSet<Arc<str>>,
    pub data: Arc<Value>,
    pub weight: f64,
}

#[derive(Debug, Clone)]
pub struct DAG {
    nodes: HashMap<Arc<str>, Node>,
}


// Реализация Serialize для Node
impl Serialize for Node {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry("parents", &self.parents.iter().map(|arc| arc.as_ref()).collect::<Vec<&str>>())?;
        map.serialize_entry("children", &self.children.iter().map(|arc| arc.as_ref()).collect::<Vec<&str>>())?;
        map.serialize_entry("data", &self.data)?;
        map.serialize_entry("weight", &self.weight)?;
        map.end()
    }
}

// Реализация Deserialize для Node
impl<'de> Deserialize<'de> for Node {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct NodeVisitor;

        impl<'de> Visitor<'de> for NodeVisitor {
            type Value = Node;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Node")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Node, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut parents = None;
                let mut children = None;
                let mut data = None;
                let mut weight = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "parents" => {
                            let parents_vec: Vec<String> = map.next_value()?;
                            parents = Some(parents_vec.into_iter().map(Arc::from).collect());
                        }
                        "children" => {
                            let children_vec: Vec<String> = map.next_value()?;
                            children = Some(children_vec.into_iter().map(Arc::from).collect());
                        }
                        "data" => data = Some(map.next_value::<Arc<Value>>()?),
                        "weight" => weight = Some(map.next_value()?),
                        _ => { let _ = map.next_value::<serde_json::Value>()?; }
                    }
                }

                Ok(Node {
                    parents: parents.unwrap_or_default(),
                    children: children.unwrap_or_default(),
                    data: data.ok_or_else(|| serde::de::Error::missing_field("data"))?,
                    weight: weight.unwrap_or(0.0),
                })
            }
        }

        deserializer.deserialize_struct("Node", &["parents", "children", "data", "weight"], NodeVisitor)
    }
}

// Реализация Serialize для DAG
impl Serialize for DAG {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        let nodes: HashMap<&str, &Node> = self.nodes.iter().map(|(k, v)| (k.as_ref(), v)).collect();
        map.serialize_entry("nodes", &nodes)?;
        map.end()
    }
}

// Реализация Deserialize для DAG
impl<'de> Deserialize<'de> for DAG {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DAGVisitor;

        impl<'de> Visitor<'de> for DAGVisitor {
            type Value = DAG;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct DAG")
            }

            fn visit_map<V>(self, mut map: V) -> Result<DAG, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut nodes = None;
                while let Some(key) = map.next_key::<String>()? {
                    if key == "nodes" {
                        let nodes_map: HashMap<String, Node> = map.next_value()?;
                        nodes = Some(nodes_map.into_iter().map(|(k, v)| (Arc::from(k.as_str()), v)).collect());
                    } else {
                        let _ = map.next_value::<serde_json::Value>()?;
                    }
                }

                Ok(DAG {
                    nodes: nodes.unwrap_or_default(),
                })
            }
        }

        deserializer.deserialize_struct("DAG", &["nodes"], DAGVisitor)
    }
}


impl DAG {
    pub fn new() -> Self {
        let mut dag = DAG {
            nodes: HashMap::new(),
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

    // Получение данных узла из новой структуры.
    pub fn get_node_data(&self, node: &str) -> Option<&Value> {
        self.nodes.get(node).map(|n| n.data.as_ref())
    }

    pub fn get_node_keys(&self) -> Vec<Arc<str>> {
        self.nodes.keys().cloned().collect()
    }

    /// Возвращает иммутабельную ссылку на таблицу узлов.
    pub fn get_nodes(&self) -> &HashMap<Arc<str>, Node> {
        &self.nodes
    }

    /// Возвращает мутабельную ссылку на узел по ключу, если он существует.
    pub fn get_node_mut(&mut self, key: &Arc<str>) -> Option<&mut Node> {
        self.nodes.get_mut(key)
    }

    // Метод теперь "на лету" собирает список смежности детей.
    pub fn get_childrens(&self) -> Adjacency {
        self.nodes
            .iter()
            .map(|(hash, node)| (hash.clone(), node.children.iter().cloned().collect()))
            .collect()
    }
    
    // Метод теперь "на лету" собирает список смежности родителей.
    pub fn get_parents(&self) -> Adjacency {
        self.nodes
            .iter()
            .map(|(hash, node)| (hash.clone(), node.parents.iter().cloned().collect()))
            .collect()
    }

    // Метод теперь "на лету" собирает веса всех узлов.
    pub fn get_weights(&self) -> Vec<NodeWeight> {
        self.nodes
            .iter()
            .map(|(hash, node)| NodeWeight {
                node: hash.clone(),
                weight: node.weight,
            })
            .collect()
    }

    // Возвращает `true`, если узел существует в `nodes`.
    pub fn contains_node(&self, node: &str) -> bool {
        self.nodes.contains_key(node)
    }

    // Возвращает количество узлов.
    pub fn get_node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn add_node_with_parents(&mut self, node: Arc<str>, parents: Vec<Arc<str>>, data: Arc<Value>) -> Result<(), String> {
        if self.nodes.contains_key(&node) {
            return Err("Node already exists".to_string());
        }
        // TODO: 2 parents!
        // Фильтруем список родителей, оставляя только те узлы, которые уже существуют в графе.
        // Используем into_iter() для владения значениями из вектора parents и collect для создания нового вектора.
        let existing_parents = parents
            .into_iter()
            .filter(|p| self.nodes.contains_key(p))
            .collect::<HashSet<_>>();

        
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

    pub fn remove_nodes(&mut self, nodes: Vec<Arc<str>>) -> Result<(), String> {
        for node in nodes {
            if self.contains_node(&node) {
                self.remove_node(node);
            }
        }
        Ok(())
    }

    pub fn compute_descendants_with_depth_and_weight(&self) -> HashMap<Arc<str>, Vec<NodeInfo>> {
        let node_keys: HashSet<Arc<str>> = self.nodes.keys().cloned().collect();
        compute_descendants_with_depth_and_weight(&self.nodes, &node_keys)
    }

    pub fn compute_weights_for_batch(&self, nodes: &[Arc<str>]) -> HashMap<Arc<str>, f64> {
       // Преобразуем срез узлов в HashSet для совместимости с compute_descendants_with_depth_and_weight
       let nodes_to_process: HashSet<Arc<str>> = nodes.iter().cloned().collect();
       // Вызываем существующую функцию для вычисления потомков с весами
       let descendants_map = compute_descendants_with_depth_and_weight(&self.nodes, &nodes_to_process);
       // Агрегируем веса потомков для каждого узла
       let mut weights = HashMap::new();
       for (node, descendants) in descendants_map {
           let total_weight: f64 = descendants.iter().map(|d| d.weight).sum();
           weights.insert(node, total_weight);
       }
       weights
    }
}

impl Default for DAG {
    fn default() -> Self {
        Self::new()
    }
}
