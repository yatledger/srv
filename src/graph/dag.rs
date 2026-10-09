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
pub struct Dag {
    nodes: HashMap<Arc<str>, Node>,
    /// Реестр удалённых узлов: хэш -> логический номер удаления.
    /// Номер детерминирован (порядок удалений), а не привязан к системным часам,
    /// чтобы состояние совпадало на всех репликах.
    added: HashMap<Arc<str>, u64>,
    /// Счётчик логических номеров удаления (монотонно растёт).
    added_seq: u64,
}

/// Имя функции-перевода. Вынесено в константу, чтобы бизнес-правило `var`
/// не было захардкожено в нескольких местах (V6).
pub const TRANSFER_TOKEN: &str = "transferToken";

/// Верхняя граница размера реестра `added`. При превышении вытесняются самые
/// старые записи, чтобы память не росла бесконечно. Ограничение детерминировано.
pub const MAX_ADDED_ENTRIES: usize = 100_000;

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

impl Dag {
    pub fn new() -> Self {
        Dag {
            nodes: HashMap::new(),
            added: HashMap::new(),
            added_seq: 0,
        }
    }

    pub fn add_node_with_parents(
        &mut self,
        tx_hash: Arc<str>,
        tx: Tx,
        sign: String,
        func: String,
    ) -> Result<(), String> {
        let (ca, to, val, msg) = if func == TRANSFER_TOKEN {
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
        // Добавляем удаленный узел в added с монотонным логическим номером.
        // Детерминировано: порядок удалений одинаков на всех репликах.
        self.added_seq = self.added_seq.wrapping_add(1);
        let seq = self.added_seq;
        self.added.insert(node, seq);
        self.evict_added_if_needed();
    }

    /// Ограничивает размер реестра `added`, вытесняя самые старые записи.
    /// Вытеснение детерминировано (сортировка по логическому номеру и хэшу).
    fn evict_added_if_needed(&mut self) {
        self.evict_added_to(MAX_ADDED_ENTRIES);
    }

    /// Вытесняет записи из `added` до размера `max` (тестируемая версия).
    fn evict_added_to(&mut self, max: usize) {
        if self.added.len() <= max {
            return;
        }
        let mut entries: Vec<(Arc<str>, u64)> =
            self.added.iter().map(|(k, v)| (k.clone(), *v)).collect();
        // Старые — в начале; при равных номерах порядок стабилен по хэшу.
        entries.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        let remove_count = self.added.len() - max;
        for (key, _) in entries.into_iter().take(remove_count) {
            self.added.remove(&key);
        }
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
        // Агрегируем веса потомков и насыщаем результат в [0, 1), чтобы вес не
        // рос неограниченно с числом потомков (находка C23).
        let mut weights = HashMap::new();
        for (node, descendants) in descendants_map {
            let total_weight: f64 = descendants.iter().map(|d| d.weight).sum();
            weights.insert(node, crate::graph::weights::saturate_weight(total_weight));
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

// Реализация Serialize для Dag
impl Serialize for Dag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // В снапшот/на диск обязаны попадать и узлы, и реестр `added`, и
        // монотонный счётчик удалений, иначе удалённые узлы «воскреснут» после
        // рестарта, а номера продолжат расходиться (нарушение инварианта).
        let mut map = serializer.serialize_map(Some(3))?;
        let nodes: HashMap<&str, &Node> = self.nodes.iter().map(|(k, v)| (k.as_ref(), v)).collect();
        let added: HashMap<&str, &u64> = self.added.iter().map(|(k, v)| (k.as_ref(), v)).collect();
        map.serialize_entry("nodes", &nodes)?;
        map.serialize_entry("added", &added)?;
        map.serialize_entry("added_seq", &self.added_seq)?;
        map.end()
    }
}

// Реализация Deserialize для Dag
impl<'de> Deserialize<'de> for Dag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DAGVisitor;

        impl<'de> Visitor<'de> for DAGVisitor {
            type Value = Dag;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Dag")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Dag, V::Error>
            where
                V: MapAccess<'de>,
            {
                // Инициализируем поля для хранения данных
                let mut nodes = None;
                let mut added = None;
                let mut added_seq = None;

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
                        "added_seq" => {
                            added_seq = Some(map.next_value::<u64>()?);
                        }
                        // Игнорируем неизвестные поля
                        _ => {
                            let _ = map.next_value::<serde_json::Value>()?;
                        }
                    }
                }

                // Проверяем наличие обязательного поля nodes, added может быть пустым
                let nodes = nodes.unwrap_or_default();
                let added: HashMap<Arc<str>, u64> = added.unwrap_or_default();
                // Для старых снапшотов без счётчика восстанавливаем его из максимума.
                let added_seq =
                    added_seq.unwrap_or_else(|| added.values().copied().max().unwrap_or(0));

                // Возвращаем заполненную структуру Dag
                Ok(Dag {
                    nodes,
                    added,
                    added_seq,
                })
            }
        }

        deserializer.deserialize_struct("Dag", &["nodes"], DAGVisitor)
    }
}

impl Default for Dag {
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
        let mut dag = Dag::new();
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

    fn valid_tx(parents: Vec<Arc<str>>) -> Tx {
        Tx {
            prnts: parents,
            addr: Arc::from("addr"),
            seq: 0,
            var: json!({ "ca": "a", "to": "b", "val": 1, "msg": "m" }),
        }
    }

    #[test]
    fn add_creates_bidirectional_parent_child_links() {
        let mut dag = Dag::new();
        let parent: Arc<str> = Arc::from("parent");
        dag.add_node_with_parents(
            parent.clone(),
            valid_tx(vec![]),
            String::new(),
            "transferToken".to_string(),
        )
        .unwrap();

        let child: Arc<str> = Arc::from("child");
        dag.add_node_with_parents(
            child.clone(),
            valid_tx(vec![parent.clone()]),
            String::new(),
            "transferToken".to_string(),
        )
        .unwrap();

        // У ребёнка записан родитель, у родителя — ребёнок.
        assert!(dag.get_node_mut(&child).unwrap().parents.contains(&parent));
        assert!(dag.get_node_mut(&parent).unwrap().children.contains(&child));
        assert_eq!(dag.get_node_count(), 2);
        assert!(dag.contains_node(&child));
    }

    #[test]
    fn add_ignores_parent_missing_from_dag() {
        let mut dag = Dag::new();
        let node: Arc<str> = Arc::from("n");
        // Родитель отсутствует в Dag: связь не создаётся, но узел добавляется.
        dag.add_node_with_parents(
            node.clone(),
            valid_tx(vec![Arc::from("ghost")]),
            String::new(),
            "transferToken".to_string(),
        )
        .unwrap();
        assert!(dag.get_node_mut(&node).unwrap().parents.is_empty());
    }

    #[test]
    fn remove_node_moves_it_to_added_and_clears_links() {
        let mut dag = Dag::new();
        let parent: Arc<str> = Arc::from("parent");
        dag.add_node_with_parents(
            parent.clone(),
            valid_tx(vec![]),
            String::new(),
            "transferToken".to_string(),
        )
        .unwrap();
        let child: Arc<str> = Arc::from("child");
        dag.add_node_with_parents(
            child.clone(),
            valid_tx(vec![parent.clone()]),
            String::new(),
            "transferToken".to_string(),
        )
        .unwrap();

        dag.remove_node(child.clone());

        assert!(!dag.contains_node(&child));
        assert!(
            dag.is_node_added(&child),
            "удалённый узел попадает в реестр added"
        );
        // Связь у родителя очищена.
        assert!(!dag.get_node_mut(&parent).unwrap().children.contains(&child));
        assert_eq!(dag.get_node_count(), 1);
    }

    #[test]
    fn remove_nodes_skips_absent_and_removes_present() {
        let mut dag = Dag::new();
        let a: Arc<str> = Arc::from("a");
        dag.add_node_with_parents(
            a.clone(),
            valid_tx(vec![]),
            String::new(),
            "transferToken".to_string(),
        )
        .unwrap();

        dag.remove_nodes(vec![a.clone(), Arc::from("absent")])
            .unwrap();

        assert!(!dag.contains_node(&a));
        assert!(dag.is_node_added(&a));
        assert!(!dag.is_node_added(&Arc::from("absent")));
    }

    #[test]
    fn added_sequence_is_monotonic_and_deterministic() {
        let mut dag = Dag::new();
        for i in 0..3u32 {
            let node: Arc<str> = Arc::from(format!("n{i}").as_str());
            dag.add_node_with_parents(
                node.clone(),
                valid_tx(vec![]),
                String::new(),
                "transferToken".to_string(),
            )
            .unwrap();
            dag.remove_node(node);
        }
        // Логические номера отражают порядок удаления, а не системное время.
        assert_eq!(dag.added.get(&Arc::from("n0")), Some(&1));
        assert_eq!(dag.added.get(&Arc::from("n1")), Some(&2));
        assert_eq!(dag.added.get(&Arc::from("n2")), Some(&3));
    }

    #[test]
    fn added_evicts_oldest_beyond_limit() {
        let mut dag = Dag::new();
        for i in 0..5u32 {
            let node: Arc<str> = Arc::from(format!("n{i}").as_str());
            dag.add_node_with_parents(
                node.clone(),
                valid_tx(vec![]),
                String::new(),
                "transferToken".to_string(),
            )
            .unwrap();
            dag.remove_node(node);
        }
        // Оставляем только 2 самые новые записи.
        dag.evict_added_to(2);
        assert!(!dag.is_node_added(&Arc::from("n0")));
        assert!(!dag.is_node_added(&Arc::from("n1")));
        assert!(!dag.is_node_added(&Arc::from("n2")));
        assert!(dag.is_node_added(&Arc::from("n3")));
        assert!(dag.is_node_added(&Arc::from("n4")));
    }

    #[test]
    fn added_and_seq_survive_serialization_roundtrip() {
        let mut dag = Dag::new();
        for i in 0..2u32 {
            let node: Arc<str> = Arc::from(format!("n{i}").as_str());
            dag.add_node_with_parents(
                node.clone(),
                valid_tx(vec![]),
                String::new(),
                "transferToken".to_string(),
            )
            .unwrap();
            dag.remove_node(node);
        }

        let json = serde_json::to_string(&dag).unwrap();
        let restored: Dag = serde_json::from_str(&json).unwrap();

        assert!(restored.is_node_added(&Arc::from("n0")));
        assert!(restored.is_node_added(&Arc::from("n1")));
        assert_eq!(restored.added_seq, 2);

        // Новое удаление после восстановления продолжает нумерацию.
        let mut restored = restored;
        let node: Arc<str> = Arc::from("n2");
        restored
            .add_node_with_parents(
                node.clone(),
                valid_tx(vec![]),
                String::new(),
                "transferToken".to_string(),
            )
            .unwrap();
        restored.remove_node(node);
        assert_eq!(restored.added.get(&Arc::from("n2")), Some(&3));
    }

    #[test]
    fn legacy_snapshot_without_seq_restores_counter() {
        // Старый снапшот без added_seq должен восстановить счётчик из максимума.
        let legacy = json!({
            "nodes": {},
            "added": { "x": 7, "y": 3 }
        });
        let dag: Dag = serde_json::from_value(legacy).unwrap();
        assert_eq!(dag.added_seq, 7);
    }
}
