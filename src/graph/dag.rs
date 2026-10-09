// Импортируем необходимые коллекции из стандартной библиотеки Rust для работы с графом.
// BTreeMap/BTreeSet дают детерминированный (отсортированный) порядок обхода и
// сериализации, поэтому одинаковое состояние даёт одинаковые байты снапшота на
// всех репликах (находка C36/V14).
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

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

use crate::domain::{Address, Func, Hash};

#[derive(Debug, Clone, Default)]
pub struct Node {
    pub parents: BTreeSet<Hash>,
    pub children: BTreeSet<Hash>,
    pub data: Value,
}

#[derive(Debug, Clone)]
pub struct Dag {
    nodes: BTreeMap<Hash, Node>,
    /// Реестр удалённых узлов: хэш -> логический номер удаления.
    /// Номер детерминирован (порядок удалений), а не привязан к системным часам,
    /// чтобы состояние совпадало на всех репликах.
    added: BTreeMap<Hash, u64>,
    /// Счётчик логических номеров удаления (монотонно растёт).
    added_seq: u64,
    /// Последний применённый `seq` по адресу отправителя (V18: анти-replay).
    /// Детерминированная защита: после вытеснения старого узла из `added`
    /// повторная транзакция с прежним `seq` больше не проходит проверку.
    last_seq: BTreeMap<Address, u32>,
}

/// Имя функции-перевода. Вынесено в константу, чтобы бизнес-правило `var`
/// не было захардкожено в нескольких местах (V6).
pub const TRANSFER_TOKEN: &str = "transferToken";

/// Верхняя граница размера реестра `added`. При превышении вытесняются самые
/// старые записи, чтобы память не росла бесконечно. Ограничение детерминировано.
pub const MAX_ADDED_ENTRIES: usize = 100_000;

#[derive(Debug, Serialize, Deserialize)]
pub struct TxVar {
    ca: Address,
    to: Address,
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
        if let Some(msg) = &self.msg
            && msg.len() > 2500
        {
            return Err("var.msg must not exceed 2500 characters".to_string());
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
            nodes: BTreeMap::new(),
            added: BTreeMap::new(),
            added_seq: 0,
            last_seq: BTreeMap::new(),
        }
    }

    pub fn add_node_with_parents(
        &mut self,
        tx_hash: Hash,
        tx: Tx,
        sign: String,
        func: Func,
    ) -> Result<(), String> {
        let (ca, to, val, msg) = if func == Func::TransferToken {
            // Ошибка извлечения `var` не должна ронять узел — возвращаем её как Result.
            let TxVar { ca, to, val, msg } = extract_from_var_struct(&tx)?;
            (Some(ca), Some(to), Some(val), Some(msg))
        } else {
            (None, None, None, None)
        };

        // V15/C38: родители берём только из живого DAG. Родители из реестра
        // `added` (удалённые узлы) не допускаются — см. `validate_add`, который
        // отклоняет такие команды; здесь фиксируется та же семантика.
        let existing_parents: BTreeSet<Hash> = tx
            .prnts
            .iter()
            .filter(|p| self.nodes.contains_key(p))
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
            "prnts": tx.prnts.iter().map(|p| p.as_str()).collect::<Vec<&str>>(),
            "hash": tx_hash.as_str(),
            "addr": tx.addr.as_str(),
            "seq": tx.seq,
            "sign": sign,
            "func": func,
            "var": {
                "ca": ca.as_ref().map(|a| a.as_str()),
                "to": to.as_ref().map(|a| a.as_str()),
                "val": val,
                "msg": msg,
            }
        });
        // Создаем и вставляем новый узел.
        let new_node = Node {
            parents: existing_parents,
            children: BTreeSet::new(),
            data: data_json,
        };
        self.nodes.insert(tx_hash.clone(), new_node);

        // V18: запоминаем последний `seq` адреса (кроме bootstrap-узлов с пустым
        // addr), чтобы отклонить повтор/откат номера.
        if !tx.addr.as_str().trim().is_empty() {
            self.last_seq
                .entry(tx.addr.clone())
                .and_modify(|s| *s = (*s).max(tx.seq))
                .or_insert(tx.seq);
        }

        Ok(())
    }

    /// Проверяет монотонность `seq` по адресу (V18, анти-replay).
    ///
    /// Пустой `addr` (bootstrap-генезис) проверку не проходит и не запоминается.
    pub fn accepts_seq(&self, addr: &Address, seq: u32) -> Result<(), String> {
        if addr.as_str().trim().is_empty() {
            return Ok(());
        }
        match self.last_seq.get(addr) {
            Some(&last) if seq <= last => Err(format!(
                "non-monotonic seq for {addr}: got {seq}, last applied {last}"
            )),
            _ => Ok(()),
        }
    }

    // Получение данных узла из новой структуры.
    pub fn get_node_data(&self, node: &Hash) -> Option<&Value> {
        self.nodes.get(node).map(|n| &n.data)
    }

    pub fn get_node_keys(&self) -> Vec<Hash> {
        self.nodes.keys().cloned().collect()
    }

    /// Возвращает иммутабельную ссылку на таблицу узлов.
    pub fn get_nodes(&self) -> &BTreeMap<Hash, Node> {
        &self.nodes
    }

    /// Возвращает мутабельную ссылку на узел по ключу, если он существует.
    pub fn get_node_mut(&mut self, key: &Hash) -> Option<&mut Node> {
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

    // Возвращает `true`, если узел существует в `nodes`.
    pub fn contains_node(&self, node: &Hash) -> bool {
        self.nodes.contains_key(node)
    }

    // Возвращает количество узлов.
    pub fn get_node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn remove_node(&mut self, node: Hash) {
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
        let mut entries: Vec<(Hash, u64)> =
            self.added.iter().map(|(k, v)| (k.clone(), *v)).collect();
        // Старые — в начале; при равных номерах порядок стабилен по хэшу.
        entries.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        let remove_count = self.added.len() - max;
        for (key, _) in entries.into_iter().take(remove_count) {
            self.added.remove(&key);
        }
    }

    pub fn remove_nodes(&mut self, nodes: Vec<Hash>) -> Result<(), String> {
        for node in nodes {
            if self.contains_node(&node) {
                self.remove_node(node);
            }
        }
        Ok(())
    }

    pub fn is_node_added(&self, node: &Hash) -> bool {
        self.added.contains_key(node)
    }

    /// Число записей в реестре удалённых узлов (для метрик O2).
    pub fn added_len(&self) -> usize {
        self.added.len()
    }

    pub fn compute_descendants_with_depth_and_weight(&self) -> HashMap<Hash, Vec<NodeInfo>> {
        let node_keys: HashSet<Hash> = self.nodes.keys().cloned().collect();
        compute_descendants_with_depth_and_weight(&self.nodes, &node_keys)
    }

    /// Возвращает все узлы с их глубинами, отсортированные по глубине
    pub fn get_nodes_by_depth(&self) -> Vec<NodeDepth> {
        get_nodes_by_depth(&self.nodes)
    }

    /// Лёгкий снимок рёбер графа для расчёта глубин вне read-лока (Q2/C41).
    /// Клонирует только хэши и структуру, без сериализованных `data`.
    pub fn depth_edges_snapshot(&self) -> crate::graph::weights::DepthEdges {
        self.nodes
            .iter()
            .map(|(h, n)| (h.clone(), (n.parents.len(), n.children.clone())))
            .collect()
    }

    /// Пары `(хэш, число активных родителей)` для публичного `/pool` (Q2/C41).
    /// Считаются под локом, но обход и сортировка — уже снаружи.
    pub fn node_parent_counts(&self) -> Vec<(Hash, usize)> {
        self.nodes
            .iter()
            .map(|(h, n)| (h.clone(), n.parents.len()))
            .collect()
    }

    pub fn compute_weights_for_batch(&self, nodes: &[Hash]) -> HashMap<Hash, f64> {
        // Преобразуем срез узлов в HashSet для совместимости с compute_descendants_with_depth_and_weight
        let nodes_to_process: HashSet<Hash> = nodes.iter().cloned().collect();
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
        // BTreeSet гарантирует отсортированный порядок — байты детерминированы.
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry(
            "parents",
            &self
                .parents
                .iter()
                .map(|h| h.as_str())
                .collect::<Vec<&str>>(),
        )?;
        map.serialize_entry(
            "children",
            &self
                .children
                .iter()
                .map(|h| h.as_str())
                .collect::<Vec<&str>>(),
        )?;
        map.serialize_entry("data", &self.data)?;
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
                let mut parents: Option<BTreeSet<Hash>> = None;
                let mut children: Option<BTreeSet<Hash>> = None;
                let mut data = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "parents" => {
                            let parents_vec: Vec<String> = map.next_value()?;
                            parents = Some(parents_vec.into_iter().map(Hash::from).collect());
                        }
                        "children" => {
                            let children_vec: Vec<String> = map.next_value()?;
                            children = Some(children_vec.into_iter().map(Hash::from).collect());
                        }
                        "data" => data = Some(map.next_value::<Value>()?),
                        // Устаревшее поле `time` (C37/V14) игнорируем при чтении.
                        _ => {
                            let _ = map.next_value::<serde_json::Value>()?;
                        }
                    }
                }

                Ok(Node {
                    parents: parents.unwrap_or_default(),
                    children: children.unwrap_or_default(),
                    data: data.ok_or_else(|| serde::de::Error::missing_field("data"))?,
                })
            }
        }

        deserializer.deserialize_struct("Node", &["parents", "children", "data"], NodeVisitor)
    }
}

// Реализация Serialize для Dag
impl Serialize for Dag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // В снапшот/на диск обязаны попадать и узлы, и реестр `added`, и
        // монотонный счётчик удалений, и карта `last_seq`, иначе удалённые узлы
        // «воскреснут» после рестарта, а номера продолжат расходиться
        // (нарушение инварианта). BTreeMap/BTreeSet дают стабильный порядок
        // (C36/V14): одинаковое состояние → одинаковые байты снапшота.
        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry("nodes", &self.nodes)?;
        map.serialize_entry("added", &self.added)?;
        map.serialize_entry("added_seq", &self.added_seq)?;
        map.serialize_entry("last_seq", &self.last_seq)?;
        map.end()
    }
}

// Реализация Deserialize для Dag
impl<'de> Deserialize<'de> for Dag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DagVisitor;

        impl<'de> Visitor<'de> for DagVisitor {
            type Value = Dag;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Dag")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Dag, V::Error>
            where
                V: MapAccess<'de>,
            {
                // Инициализируем поля для хранения данных
                let mut nodes: Option<BTreeMap<Hash, Node>> = None;
                let mut added: Option<BTreeMap<Hash, u64>> = None;
                let mut added_seq = None;
                let mut last_seq: Option<BTreeMap<Address, u32>> = None;

                // Читаем ключи и значения из map
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "nodes" => {
                            // Десериализуем nodes как BTreeMap<String, Node>
                            let nodes_map: BTreeMap<Hash, Node> = map.next_value()?;
                            // Преобразуем ключи String в доменный Hash
                            nodes = Some(
                                nodes_map
                                    .into_iter()
                                    .map(|(k, v)| (Hash::from(k.as_str()), v))
                                    .collect(),
                            );
                        }
                        "added" => {
                            // Десериализуем added как BTreeMap<String, u64>
                            let added_map: BTreeMap<Hash, u64> = map.next_value()?;
                            // Преобразуем ключи String в доменный Hash
                            added = Some(
                                added_map
                                    .into_iter()
                                    .map(|(k, v)| (Hash::from(k.as_str()), v))
                                    .collect(),
                            );
                        }
                        "added_seq" => {
                            added_seq = Some(map.next_value::<u64>()?);
                        }
                        "last_seq" => {
                            last_seq = Some(map.next_value::<BTreeMap<Address, u32>>()?);
                        }
                        // Игнорируем неизвестные поля
                        _ => {
                            let _ = map.next_value::<serde_json::Value>()?;
                        }
                    }
                }

                // Проверяем наличие обязательного поля nodes, added может быть пустым
                let nodes = nodes.unwrap_or_default();
                let added: BTreeMap<Hash, u64> = added.unwrap_or_default();
                // Для старых снапшотов без счётчика восстанавливаем его из максимума.
                let added_seq =
                    added_seq.unwrap_or_else(|| added.values().copied().max().unwrap_or(0));
                // Старые снапшоты не содержат карту `last_seq` (V18) — читается пустой.
                let last_seq = last_seq.unwrap_or_default();

                // Возвращаем заполненную структуру Dag
                Ok(Dag {
                    nodes,
                    added,
                    added_seq,
                    last_seq,
                })
            }
        }

        deserializer.deserialize_struct("Dag", &["nodes"], DagVisitor)
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
            addr: Address::from("addr"),
            seq: 0,
            // var не соответствует TxVar: функция transferToken ожидает структуру.
            var: json!({ "unexpected": true }),
        };
        let result =
            dag.add_node_with_parents(Hash::from("hash"), tx, String::new(), Func::TransferToken);
        assert!(result.is_err(), "вредоносный var не должен паниковать");
        assert_eq!(dag.get_node_count(), 0);
    }

    fn valid_tx(parents: Vec<Hash>) -> Tx {
        Tx {
            prnts: parents,
            addr: Address::from("addr"),
            seq: 0,
            var: json!({ "ca": "a", "to": "b", "val": 1, "msg": "m" }),
        }
    }

    #[test]
    fn add_creates_bidirectional_parent_child_links() {
        let mut dag = Dag::new();
        let parent = Hash::from("parent");
        dag.add_node_with_parents(
            parent.clone(),
            valid_tx(vec![]),
            String::new(),
            Func::TransferToken,
        )
        .unwrap();

        let child = Hash::from("child");
        dag.add_node_with_parents(
            child.clone(),
            valid_tx(vec![parent.clone()]),
            String::new(),
            Func::TransferToken,
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
        let node = Hash::from("n");
        // Родитель отсутствует в Dag: связь не создаётся, но узел добавляется.
        dag.add_node_with_parents(
            node.clone(),
            valid_tx(vec![Hash::from("ghost")]),
            String::new(),
            Func::TransferToken,
        )
        .unwrap();
        assert!(dag.get_node_mut(&node).unwrap().parents.is_empty());
    }

    #[test]
    fn remove_node_moves_it_to_added_and_clears_links() {
        let mut dag = Dag::new();
        let parent = Hash::from("parent");
        dag.add_node_with_parents(
            parent.clone(),
            valid_tx(vec![]),
            String::new(),
            Func::TransferToken,
        )
        .unwrap();
        let child = Hash::from("child");
        dag.add_node_with_parents(
            child.clone(),
            valid_tx(vec![parent.clone()]),
            String::new(),
            Func::TransferToken,
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
        let a = Hash::from("a");
        dag.add_node_with_parents(
            a.clone(),
            valid_tx(vec![]),
            String::new(),
            Func::TransferToken,
        )
        .unwrap();

        dag.remove_nodes(vec![a.clone(), Hash::from("absent")])
            .unwrap();

        assert!(!dag.contains_node(&a));
        assert!(dag.is_node_added(&a));
        assert!(!dag.is_node_added(&Hash::from("absent")));
    }

    #[test]
    fn added_sequence_is_monotonic_and_deterministic() {
        let mut dag = Dag::new();
        for i in 0..3u32 {
            let node = Hash::from(format!("n{i}").as_str());
            dag.add_node_with_parents(
                node.clone(),
                valid_tx(vec![]),
                String::new(),
                Func::TransferToken,
            )
            .unwrap();
            dag.remove_node(node);
        }
        // Логические номера отражают порядок удаления, а не системное время.
        assert_eq!(dag.added.get(&Hash::from("n0")), Some(&1));
        assert_eq!(dag.added.get(&Hash::from("n1")), Some(&2));
        assert_eq!(dag.added.get(&Hash::from("n2")), Some(&3));
    }

    #[test]
    fn added_evicts_oldest_beyond_limit() {
        let mut dag = Dag::new();
        for i in 0..5u32 {
            let node = Hash::from(format!("n{i}").as_str());
            dag.add_node_with_parents(
                node.clone(),
                valid_tx(vec![]),
                String::new(),
                Func::TransferToken,
            )
            .unwrap();
            dag.remove_node(node);
        }
        // Оставляем только 2 самые новые записи.
        dag.evict_added_to(2);
        assert!(!dag.is_node_added(&Hash::from("n0")));
        assert!(!dag.is_node_added(&Hash::from("n1")));
        assert!(!dag.is_node_added(&Hash::from("n2")));
        assert!(dag.is_node_added(&Hash::from("n3")));
        assert!(dag.is_node_added(&Hash::from("n4")));
    }

    #[test]
    fn added_and_seq_survive_serialization_roundtrip() {
        let mut dag = Dag::new();
        for i in 0..2u32 {
            let node = Hash::from(format!("n{i}").as_str());
            dag.add_node_with_parents(
                node.clone(),
                valid_tx(vec![]),
                String::new(),
                Func::TransferToken,
            )
            .unwrap();
            dag.remove_node(node);
        }

        let json = serde_json::to_string(&dag).unwrap();
        let restored: Dag = serde_json::from_str(&json).unwrap();

        assert!(restored.is_node_added(&Hash::from("n0")));
        assert!(restored.is_node_added(&Hash::from("n1")));
        assert_eq!(restored.added_seq, 2);

        // Новое удаление после восстановления продолжает нумерацию.
        let mut restored = restored;
        let node = Hash::from("n2");
        restored
            .add_node_with_parents(
                node.clone(),
                valid_tx(vec![]),
                String::new(),
                Func::TransferToken,
            )
            .unwrap();
        restored.remove_node(node);
        assert_eq!(restored.added.get(&Hash::from("n2")), Some(&3));
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

    #[test]
    fn last_seq_survives_serialization_roundtrip() {
        // V18: карта last_seq попадает в снапшот и переживает рестарт.
        let mut dag = Dag::new();
        for (node, seq) in [("a", 0u32), ("b", 5u32), ("c", 9u32)] {
            dag.add_node_with_parents(
                Hash::from(node),
                Tx {
                    prnts: vec![],
                    addr: Address::from("addr"),
                    seq,
                    var: json!({ "ca": "a", "to": "b", "val": 1, "msg": "m" }),
                },
                String::new(),
                Func::TransferToken,
            )
            .unwrap();
        }

        let raw = serde_json::to_string(&dag).unwrap();
        let restored: Dag = serde_json::from_str(&raw).unwrap();
        assert_eq!(restored.last_seq.get(&Address::from("addr")), Some(&9));
        // Откат номера после восстановления отклоняется.
        assert!(restored.accepts_seq(&Address::from("addr"), 9).is_err());
        assert!(restored.accepts_seq(&Address::from("addr"), 10).is_ok());
    }

    #[test]
    fn snapshot_serialization_is_deterministic() {
        // V14/C36: одинаковое состояние даёт одинаковые байты снапшота
        // независимо от порядка вставки (BTreeSet/BTreeMap).
        fn build(order: &[&str]) -> Dag {
            let mut dag = Dag::new();
            for node in order {
                dag.add_node_with_parents(
                    Hash::from(*node),
                    Tx {
                        prnts: vec![],
                        addr: Address::from(format!("addr-{node}")),
                        seq: 0,
                        var: json!({ "ca": "a", "to": "b", "val": 1, "msg": "m" }),
                    },
                    String::new(),
                    Func::TransferToken,
                )
                .unwrap();
            }
            dag
        }

        let left = build(&["a", "b", "c", "d"]);
        let right = build(&["d", "c", "b", "a"]);
        assert_eq!(
            serde_json::to_string(&left).unwrap(),
            serde_json::to_string(&right).unwrap(),
            "порядок вставки не должен влиять на байты снапшота"
        );
    }
}
