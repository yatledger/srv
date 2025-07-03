// Импортируем необходимые коллекции из стандартной библиотеки Rust для работы с графом.
// HashMap используется для хранения списка смежности, а HashSet — для проверки циклов в DFS.
use std::collections::{HashMap, HashSet};
use std::fs; // Для чтения файла.
use std::sync::{Arc};
use serde::{Deserialize, Serialize};
use serde::ser::{Serializer, SerializeMap};
use serde::de::{Deserializer, MapAccess, Visitor};
use std::fmt;
use serde_json::{Value, json};
use tracing::{info, error};
use crate::graph::weights::{NodeWeight, NodeInfo, compute_descendants_with_depth_and_weight};
use crate::Adjacency;
use crate::Tx;
use crate::utils::*;
use ed25519_dalek::{VerifyingKey, Signature, Verifier};
use base58::FromBase58;
use hex::FromHex;


// Структура для десериализации JSON с генезис-транзакциями.
#[derive(Deserialize)]
struct GenesisTransaction {
    hash: String, // Хэш генезис-узла.
    data: Value,
}

#[derive(Debug, Clone, Default)]
pub struct Node {
    pub parents: HashSet<Arc<str>>,
    pub children: HashSet<Arc<str>>,
    pub data: Value,
    pub weight: f64,
}

#[derive(Debug, Clone)]
pub struct DAG {
    nodes: HashMap<Arc<str>, Node>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TxVar {
    ca: Arc<str>,
    to: Arc<str>,
    val: u64,
    msg: Option<String>,
}

pub fn extract_from_var_struct(tx: &Tx) -> Result<TxVar, String> {
    // Десериализуем `var` в структуру `TxData`
    serde_json::from_value(tx.var.clone())
        .map_err(|e| format!("Failed to deserialize var: {}", e))
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
                        let data = tx.data.clone();
                        
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

        dag
    }

    pub fn add_node_with_parents(&mut self, tx: Tx, sign: String, func: String) -> Result<(), String> {

        // Validate parents
        // validate_parents(&content.parents);
        if let Err(err) = validate_parents(&tx.prnts) {
            return Err(err);
        }

        let (ca, to, val, msg) = if func == "transferToken" {
            let TxVar { ca, to, val, msg } = extract_from_var_struct(&tx)?;
            (Some(ca), Some(to), Some(val), Some(msg))
        } else {
            (None, None, None, None)
        };
        // Проверка подписи
        let addr_bytes = tx.addr.from_base58()
            .map_err(|_| "Invalid base58 in signature verification".to_string())?;
        let addr_array: [u8; 32] = addr_bytes
            .try_into()
            .map_err(|_| "Invalid Ed25519 key length".to_string())?;
        let verify_key = VerifyingKey::from_bytes(&addr_array)
            .map_err(|_| "Invalid Ed25519 public key".to_string())?;

        // Получаем отсортированную строку из полей Tx
        let tx_hash_bytes = ordered_sum(&tx)
            .map_err(|e| format!("Ordered sum error: {}", e))?;

        // Декодируем подпись из hex
        let signature_bytes = <[u8; 64]>::from_hex(sign.as_str())
            .map_err(|_| "Invalid hex for signature".to_string())?;
        let signature = Signature::try_from(&signature_bytes)
            .map_err(|_| "Invalid signature format".to_string())?;

        // Проверяем подпись
        if verify_key.verify(tx_hash_bytes.as_bytes(), &signature).is_err() {
            return Err("Signature verification failed".to_string());
        }

        let tx_hash = Arc::from(tx_hash_bytes.to_hex().to_string());

        if self.nodes.contains_key(&tx_hash) {
            return Err("Node already exists".to_string());
        }
        
        let existing_parents: HashSet<Arc<str>> = tx.prnts
            .iter()
            .filter(|p| self.nodes.contains_key(p.as_ref()))
            .cloned()
            .collect();
        
        if !existing_parents.is_empty() {
            // Проходим по каждому существующему родителю и добавляем ребро от него к новому узлу.
            for parent_hash in &existing_parents {
                if let Some(parent_node) = self.nodes.get_mut(parent_hash) {
                    parent_node.children.insert(tx_hash.clone());
                }
            }
        }

        let data_json = json!({
            "prnts": tx.prnts.iter().map(|p| p.as_ref()).collect::<Vec<&str>>(),
            "hash": tx_hash.as_ref(),
            "addr": tx.addr.as_ref(),
            "seq": tx.seq,
            "sign": sign,
            "func": func,
            "var": {
                "ca": ca.as_ref(),
                "to": to.as_ref(),
                "val": val,
                "msg": msg,   
            }
        });
        // Создаем и вставляем новый узел
        let new_node = Node {
            parents: existing_parents,
            children: HashSet::new(),
            data: data_json,
            weight: 0.0, // Вес будет пересчитан позже
        };
        self.nodes.insert(tx_hash, new_node);

        Ok(())
    }

    // Получение данных узла из новой структуры.
    pub fn get_node_data(&self, node: &str) -> Option<&Value> {
        self.nodes.get(node).map(|n| &n.data)
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
    pub fn contains_node(&self, node: &Arc<str>) -> bool {
        self.nodes.contains_key(node)
    }

    // Возвращает количество узлов.
    pub fn get_node_count(&self) -> usize {
        self.nodes.len()
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
                        "data" => data = Some(map.next_value::<Value>()?),
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

impl Default for DAG {
    fn default() -> Self {
        Self::new()
    }
}
