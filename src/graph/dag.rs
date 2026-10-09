// Импортируем необходимые коллекции из стандартной библиотеки Rust для работы с графом.
// HashMap используется для хранения списка смежности, а HashSet — для проверки циклов в DFS.
use std::collections::{HashMap, HashSet};

use crate::Adjacency;
use crate::Tx;
use crate::graph::weights::{
    NodeDepth, NodeInfo, compute_descendants_with_depth_and_weight, get_nodes_by_depth,
};
use serde::de::{Deserializer, MapAccess, Visitor};
use serde::ser::{SerializeMap, Serializer};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct Node {
    pub parents: HashSet<Arc<str>>,
    pub children: HashSet<Arc<str>>,
    pub data: Value,
    pub time: u64,
}

#[derive(Debug, Clone)]
pub struct NodeTime {
    pub node: Arc<str>,
    pub time: u64,
}

#[derive(Debug, Clone)]
pub struct DAG {
    nodes: HashMap<Arc<str>, Node>,
    added: HashMap<Arc<str>, u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TxVar {
    ca: Arc<str>,
    to: Arc<str>,
    val: u64,
    msg: Option<String>,
}

impl TxVar {
    /// Проверяет структуру `var` детерминированно (без сети/времени).
    pub fn validate(&self) -> Result<(), String> {
        if self.ca.trim().is_empty() {
            return Err("var.ca must not be empty".to_string());
        }
        if self.to.trim().is_empty() {
            return Err("var.to must not be empty".to_string());
        }
        if let Some(msg) = &self.msg {
            if msg.len() > 2500 {
                return Err("var.msg must not exceed 2500 characters".to_string());
            }
        }
        Ok(())
    }
}

pub fn extract_from_var_struct(tx: &Tx) -> Result<TxVar, String> {
    // Десериализуем `var` в структуру `TxData`
    serde_json::from_value(tx.var.clone()).map_err(|e| format!("Failed to deserialize var: {}", e))
}

impl DAG {
    pub fn new() -> Self {
        DAG {
            nodes: HashMap::new(),
            added: HashMap::new(),
        }
    }

    pub fn add_node_with_parents(
        &mut self,
        tx_hash: Arc<str>,
        tx: Tx,
        sign: String,
        func: String,
    ) -> Result<(), String> {
        let (ca, to, val, msg) = if func == "transferToken" {
            // Ошибка извлечения `var` не должна ронять узел — возвращаем её как Result.
            let TxVar { ca, to, val, msg } = extract_from_var_struct(&tx)?;
            (Some(ca), Some(to), Some(val), Some(msg))
        } else {
            (None, None, None, None)
        };

        let existing_parents: HashSet<Arc<str>> = tx
            .prnts
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
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        // Создаем и вставляем новый узел
        let new_node = Node {
            parents: existing_parents,
            children: HashSet::new(),
            data: data_json,
            time: timestamp,
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

    pub fn get_time(&self) -> Vec<NodeTime> {
        // Собираем узлы с их временными метками в вектор
        let mut nodes = self
            .nodes
            .iter()
            .map(|(hash, node)| NodeTime {
                node: hash.clone(),
                time: node.time,
            })
            .collect::<Vec<NodeTime>>();
        // Сортируем узлы по времени по возрастанию
        nodes.sort_by(|a, b| a.time.cmp(&b.time));
        nodes
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
        // Добавляем удаленный узел в added с текущим временем в микросекундах
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        self.added.insert(node, timestamp);
    }

    pub fn remove_nodes(&mut self, nodes: Vec<Arc<str>>) -> Result<(), String> {
        for node in nodes {
            if self.contains_node(&node) {
                self.remove_node(node);
            }
        }
        Ok(())
    }

    pub fn is_node_added(&self, node: &Arc<str>) -> bool {
        self.added.contains_key(node)
    }

    pub fn compute_descendants_with_depth_and_weight(&self) -> HashMap<Arc<str>, Vec<NodeInfo>> {
        let node_keys: HashSet<Arc<str>> = self.nodes.keys().cloned().collect();
        compute_descendants_with_depth_and_weight(&self.nodes, &node_keys)
    }

    /// Возвращает все узлы с их глубинами, отсортированные по глубине
    pub fn get_nodes_by_depth(&self) -> Vec<NodeDepth> {
        get_nodes_by_depth(&self.nodes)
    }

    pub fn compute_weights_for_batch(&self, nodes: &[Arc<str>]) -> HashMap<Arc<str>, f64> {
        // Преобразуем срез узлов в HashSet для совместимости с compute_descendants_with_depth_and_weight
        let nodes_to_process: HashSet<Arc<str>> = nodes.iter().cloned().collect();
        // Вызываем существующую функцию для вычисления потомков с весами
        let descendants_map =
            compute_descendants_with_depth_and_weight(&self.nodes, &nodes_to_process);
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
        map.serialize_entry(
            "parents",
            &self
                .parents
                .iter()
                .map(|arc| arc.as_ref())
                .collect::<Vec<&str>>(),
        )?;
        map.serialize_entry(
            "children",
            &self
                .children
                .iter()
                .map(|arc| arc.as_ref())
                .collect::<Vec<&str>>(),
        )?;
        map.serialize_entry("data", &self.data)?;
        map.serialize_entry("time", &self.time)?;
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
                let mut time = None;

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
                        "time" => time = Some(map.next_value()?),
                        _ => {
                            let _ = map.next_value::<serde_json::Value>()?;
                        }
                    }
                }

                Ok(Node {
                    parents: parents.unwrap_or_default(),
                    children: children.unwrap_or_default(),
                    data: data.ok_or_else(|| serde::de::Error::missing_field("data"))?,
                    time: time.unwrap_or(0),
                })
            }
        }

        deserializer.deserialize_struct(
            "Node",
            &["parents", "children", "data", "time"],
            NodeVisitor,
        )
    }
}

// Реализация Serialize для DAG
impl Serialize for DAG {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // В снапшот/на диск обязаны попадать и узлы, и реестр `added`, иначе
        // удалённые узлы «воскреснут» после рестарта (нарушение инварианта).
        let mut map = serializer.serialize_map(Some(2))?;
        let nodes: HashMap<&str, &Node> = self.nodes.iter().map(|(k, v)| (k.as_ref(), v)).collect();
        let added: HashMap<&str, &u64> = self.added.iter().map(|(k, v)| (k.as_ref(), v)).collect();
        map.serialize_entry("nodes", &nodes)?;
        map.serialize_entry("added", &added)?;
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
                // Инициализируем поля для хранения данных
                let mut nodes = None;
                let mut added = None;

                // Читаем ключи и значения из map
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "nodes" => {
                            // Десериализуем nodes как HashMap<String, Node>
                            let nodes_map: HashMap<String, Node> = map.next_value()?;
                            // Преобразуем ключи String в Arc<str>
                            nodes = Some(
                                nodes_map
                                    .into_iter()
                                    .map(|(k, v)| (Arc::from(k.as_str()), v))
                                    .collect(),
                            );
                        }
                        "added" => {
                            // Десериализуем added как HashMap<String, u64>
                            let added_map: HashMap<String, u64> = map.next_value()?;
                            // Преобразуем ключи String в Arc<str>
                            added = Some(
                                added_map
                                    .into_iter()
                                    .map(|(k, v)| (Arc::from(k.as_str()), v))
                                    .collect(),
                            );
                        }
                        // Игнорируем неизвестные поля
                        _ => {
                            let _ = map.next_value::<serde_json::Value>()?;
                        }
                    }
                }

                // Проверяем наличие обязательного поля nodes, added может быть пустым
                let nodes = nodes.unwrap_or_default();
                let added = added.unwrap_or_default();

                // Возвращаем заполненную структуру DAG
                Ok(DAG { nodes, added })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn tx_var(value: serde_json::Value) -> TxVar {
        serde_json::from_value(value).expect("valid TxVar")
    }

    #[test]
    fn tx_var_validate_accepts_well_formed() {
        let var = tx_var(json!({ "ca": "a", "to": "b", "val": 1, "msg": "ok" }));
        assert!(var.validate().is_ok());
    }

    #[test]
    fn tx_var_validate_rejects_empty_fields() {
        let var = tx_var(json!({ "ca": "", "to": "b", "val": 1 }));
        assert!(var.validate().is_err());

        let var = tx_var(json!({ "ca": "a", "to": "  ", "val": 1 }));
        assert!(var.validate().is_err());
    }

    #[test]
    fn tx_var_validate_rejects_long_msg() {
        let long = "x".repeat(2501);
        let var = tx_var(json!({ "ca": "a", "to": "b", "val": 1, "msg": long }));
        assert!(var.validate().is_err());
    }

    #[test]
    fn tx_var_validate_rejects_wrong_val_type() {
        // val должен быть целым числом — структура var невалидна.
        let result: Result<TxVar, _> =
            serde_json::from_value(json!({ "ca": "a", "to": "b", "val": "not-a-number" }));
        assert!(result.is_err());
    }

    #[test]
    fn add_node_with_parents_does_not_panic_on_bad_var() {
        let mut dag = DAG::new();
        let tx = Tx {
            prnts: vec![],
            addr: Arc::from("addr"),
            seq: 0,
            // var не соответствует TxVar: функция transferToken ожидает структуру.
            var: json!({ "unexpected": true }),
        };
        let result = dag.add_node_with_parents(
            Arc::from("hash"),
            tx,
            String::new(),
            "transferToken".to_string(),
        );
        assert!(result.is_err(), "вредоносный var не должен паниковать");
        assert_eq!(dag.get_node_count(), 0);
    }
}
