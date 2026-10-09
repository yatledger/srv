use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::RwLock;

use openraft::EntryPayload;
use openraft::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use serde::Deserialize;
use serde::Serialize;

use super::command::{Request, Response};

use super::typ::*;
use crate::TypeConfig;

use super::db::Db;
use crate::graph::dag::{Dag, extract_from_var_struct};
use crate::utils::validate_func;

/// Детерминированная валидация команды `Add` в state machine.
///
/// Проверки не зависят от сети/времени и дают одинаковый результат на всех
/// репликах при одинаковом состоянии: уникальность узла, существование и
/// уникальность родителей, допустимость `func` и структуры `var`.
///
/// Ограничение на количество родителей (2..100) намеренно не проверяется здесь:
/// это политика входного API, а генезис-узлы легитимно имеют пустой список
/// родителей. Проверка длины остаётся на границе (`validate_parents`).
fn validate_add(dag: &Dag, hash: &str, tx: &crate::Tx, func: &str) -> Result<(), String> {
    let hash: std::sync::Arc<str> = std::sync::Arc::from(hash);

    // Узел с таким хэшем не должен уже существовать (ни в Dag, ни в реестре added).
    if dag.contains_node(&hash) || dag.is_node_added(&hash) {
        return Err("node already exists".to_string());
    }

    // Родители: уникальны, непусты и существуют (в Dag или в реестре added).
    let mut seen = std::collections::HashSet::new();
    for parent in &tx.prnts {
        if parent.trim().is_empty() {
            return Err("parent must not be empty".to_string());
        }
        if !seen.insert(parent.clone()) {
            return Err("parents must be unique".to_string());
        }
        if !dag.contains_node(parent) && !dag.is_node_added(parent) {
            return Err(format!("parent {parent} does not exist"));
        }
    }

    validate_func(func)?;

    // Структура `var` обязательна для известных функций.
    if func == "transferToken" {
        extract_from_var_struct(tx)?.validate()?;
    }

    Ok(())
}

/// Детерминированная валидация команды `Remove` в state machine.
///
/// Список не пуст, все узлы существуют в Dag. Пустой/частично отсутствующий
/// список не применяется.
fn validate_remove(dag: &Dag, nodes: &[std::sync::Arc<str>]) -> Result<(), String> {
    if nodes.is_empty() {
        return Err("node list is empty".to_string());
    }

    for node in nodes {
        if !dag.contains_node(node) {
            return Err(format!("node {node} does not exist"));
        }
    }

    Ok(())
}

#[derive(Debug)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta,

    /// The data of the state machine at the time of this snapshot.
    pub data: SnapshotData,
}

/// Data contained in the Raft state machine.
///
/// Note that we are using `serde` to serialize the
/// `data`, which has a implementation to be serialized. Note that for this test we set both the key
/// and value as String, but you could set any type of value that has the serialization impl.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachineData {
    pub last_applied: Option<LogId>,

    pub last_membership: StoredMembership,

    /// Application data.
    pub dag: Dag,
}

/// Defines a state machine for the Raft cluster. This state machine represents a copy of the
/// data for this node. Additionally, it is responsible for storing the last snapshot of the data.
///
/// Когда `db` задан (`StateMachineStore::open`), состояние персистится в `redb`;
/// при `db == None` (конструктор по умолчанию) хранилище остаётся in-memory и
/// используется в тестах.
#[derive(Debug, Default)]
pub struct StateMachineStore {
    /// The Raft state machine.
    pub state_machine: RwLock<StateMachineData>,

    snapshot_idx: Mutex<u64>,

    /// The last received snapshot.
    current_snapshot: Mutex<Option<StoredSnapshot>>,

    /// Персистентное хранилище (None для in-memory режима).
    db: Option<Db>,
}

/// Именованные ключи в таблице метаданных для state machine.
const KEY_SM_LAST_APPLIED: &str = "sm_last_applied";
const KEY_SM_MEMBERSHIP: &str = "sm_membership";
const KEY_SM_SNAPSHOT: &str = "sm_snapshot";

impl StateMachineStore {
    /// Открывает персистентное state machine по пути `path`, восстанавливая
    /// ранее сохранённое состояние (Dag, `added`, `last_applied`, membership).
    pub fn open(path: &std::path::Path) -> Result<Arc<Self>, String> {
        let db = Db::open(path)?;

        let state_machine: StateMachineData = match db.meta_get(KEY_SM_SNAPSHOT)? {
            Some(bytes) => serde_json::from_slice(&bytes).map_err(|e| e.to_string())?,
            None => StateMachineData::default(),
        };

        Ok(Arc::new(Self {
            state_machine: RwLock::new(state_machine),
            snapshot_idx: Mutex::new(0),
            current_snapshot: Mutex::new(None),
            db: Some(db),
        }))
    }

    /// Сохраняет состояние в `redb`. В in-memory режиме ничего не делает.
    fn persist(&self, sm: &StateMachineData) -> Result<(), String> {
        let Some(db) = &self.db else {
            return Ok(());
        };

        let snapshot = serde_json::to_vec(sm).map_err(|e| e.to_string())?;
        db.meta_set(KEY_SM_SNAPSHOT, &snapshot)?;

        match sm.last_applied {
            Some(log_id) => {
                let bytes = serde_json::to_vec(&log_id).map_err(|e| e.to_string())?;
                db.meta_set(KEY_SM_LAST_APPLIED, &bytes)?;
            }
            None => db.meta_remove(KEY_SM_LAST_APPLIED)?,
        }

        let membership = serde_json::to_vec(&sm.last_membership).map_err(|e| e.to_string())?;
        db.meta_set(KEY_SM_MEMBERSHIP, &membership)?;
        Ok(())
    }
}

fn sm_storage_error(verb: openraft::ErrorVerb, e: String) -> StorageError {
    StorageError::new(
        openraft::ErrorSubject::StateMachine,
        verb,
        openraft::AnyError::error(e),
    )
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot, StorageError> {
        let data;
        let last_applied_log;
        let last_membership;

        {
            // Serialize the data of the state machine.
            let state_machine_guard = self.state_machine.read().await;
            // Теперь `data_clone` имеет тип `StateMachineData`, а не `RwLockReadGuard`.
            let data_clone = state_machine_guard.clone();

            last_applied_log = data_clone.last_applied;
            last_membership = data_clone.last_membership.clone();
            data = data_clone;
        }

        let snapshot_idx = {
            let mut l = self.snapshot_idx.lock().unwrap_or_else(|e| e.into_inner());
            *l += 1;
            *l
        };

        let snapshot_id = if let Some(last) = last_applied_log {
            format!(
                "{}-{}-{}",
                last.committed_leader_id(),
                last.index(),
                snapshot_idx
            )
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        self.persist(&data)
            .map_err(|e| sm_storage_error(openraft::ErrorVerb::Write, e))?;

        {
            let mut current_snapshot = self
                .current_snapshot
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *current_snapshot = Some(snapshot);
        }

        Ok(Snapshot {
            meta,
            snapshot: data,
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), StorageError> {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied,
            state_machine.last_membership.clone(),
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError>
    where
        I: IntoIterator<Item = Entry>,
    {
        let mut res = Vec::new();

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.last_applied = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(Response { value: None }),
                EntryPayload::Normal(ref req) => match req {
                    Request::Add {
                        hash,
                        tx,
                        sign,
                        func,
                    } => {
                        // Детерминированная валидация: не зависит от сети/времени и
                        // одинакова на всех репликах. Применяется валидная команда,
                        // невалидная логируется и возвращает ошибку без изменения Dag.
                        match validate_add(&sm.dag, hash, tx, func) {
                            Ok(()) => match sm.dag.add_node_with_parents(
                                hash.clone(),
                                tx.clone(),
                                sign.clone(),
                                func.clone(),
                            ) {
                                Ok(()) => res.push(Response {
                                    value: Some("Ok".to_string()),
                                }),
                                Err(e) => {
                                    tracing::warn!("Failed to apply Add request: {}", e);
                                    res.push(Response {
                                        value: Some(format!("Error: {e}")),
                                    });
                                }
                            },
                            Err(e) => {
                                tracing::warn!("Validation failed for Add request: {}", e);
                                res.push(Response {
                                    value: Some(format!("Error: {e}")),
                                });
                            }
                        }
                    }
                    Request::Remove { nodes } => {
                        // Детерминированная валидация удаления: все узлы обязаны
                        // существовать в Dag. Пустой список невалиден.
                        match validate_remove(&sm.dag, nodes) {
                            Ok(()) => match sm.dag.remove_nodes(nodes.clone()) {
                                Ok(()) => res.push(Response {
                                    value: Some("Ok".to_string()),
                                }),
                                Err(e) => {
                                    tracing::warn!("Failed to apply Remove request: {}", e);
                                    res.push(Response {
                                        value: Some(format!("Error: {e}")),
                                    });
                                }
                            },
                            Err(e) => {
                                tracing::warn!("Validation failed for Remove request: {}", e);
                                res.push(Response {
                                    value: Some(format!("Error: {e}")),
                                });
                            }
                        }
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(Response { value: None })
                }
            };
        }

        self.persist(&sm)
            .map_err(|e| sm_storage_error(openraft::ErrorVerb::Write, e))?;

        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(&mut self) -> Result<SnapshotData, StorageError> {
        Ok(Default::default())
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: SnapshotData,
    ) -> Result<(), StorageError> {
        tracing::info!("install snapshot");

        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot,
        };

        // Update the state machine.
        {
            let updated_state_machine: StateMachineData = new_snapshot.data.clone();
            self.persist(&updated_state_machine)
                .map_err(|e| sm_storage_error(openraft::ErrorVerb::Write, e))?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine = updated_state_machine;
        }

        // Update current snapshot.
        let mut current_snapshot = self
            .current_snapshot
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, StorageError> {
        match &*self
            .current_snapshot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
        {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: data,
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Tx;
    use serde_json::json;
    use std::sync::Arc;

    fn tx(parents: &[&str]) -> Tx {
        Tx {
            prnts: parents.iter().map(|p| Arc::from(*p)).collect(),
            addr: Arc::from("addr"),
            seq: 0,
            var: json!({ "ca": "a", "to": "b", "val": 1, "msg": "m" }),
        }
    }

    fn valid_tx_var() -> Tx {
        tx(&[])
    }

    #[test]
    fn add_accepts_genesis_without_parents() {
        let dag = Dag::new();
        assert!(validate_add(&dag, "genesis", &valid_tx_var(), "transferToken").is_ok());
    }

    #[test]
    fn add_rejects_duplicate_node() {
        let mut dag = Dag::new();
        let hash: Arc<str> = Arc::from("n1");
        dag.add_node_with_parents(
            hash.clone(),
            valid_tx_var(),
            String::new(),
            "transferToken".to_string(),
        )
        .unwrap();
        let err = validate_add(&dag, "n1", &valid_tx_var(), "transferToken").unwrap_err();
        assert!(err.contains("already exists"));
    }

    #[test]
    fn add_rejects_missing_parent() {
        let dag = Dag::new();
        let err = validate_add(
            &dag,
            "n2",
            &tx(&["missing-a", "missing-b"]),
            "transferToken",
        )
        .unwrap_err();
        assert!(err.contains("does not exist"));
    }

    #[test]
    fn add_rejects_duplicate_parents() {
        // Оба родителя должны существовать, чтобы дойти до проверки уникальности.
        let mut dag = Dag::new();
        let parent_a: Arc<str> = Arc::from("pa");
        let parent_b: Arc<str> = Arc::from("pb");
        dag.add_node_with_parents(
            parent_a.clone(),
            valid_tx_var(),
            String::new(),
            "transferToken".to_string(),
        )
        .unwrap();
        dag.add_node_with_parents(
            parent_b.clone(),
            valid_tx_var(),
            String::new(),
            "transferToken".to_string(),
        )
        .unwrap();

        let err = validate_add(&dag, "n3", &tx(&["pa", "pa"]), "transferToken").unwrap_err();
        assert!(err.contains("unique"));
    }

    #[test]
    fn add_rejects_unknown_func() {
        let dag = Dag::new();
        let err = validate_add(&dag, "n4", &valid_tx_var(), "noSuchFunc").unwrap_err();
        assert!(err.contains("unknown func"));
    }

    #[test]
    fn add_rejects_invalid_var() {
        let dag = Dag::new();
        let mut bad = valid_tx_var();
        bad.var = json!({ "totally": "wrong" });
        let err = validate_add(&dag, "n5", &bad, "transferToken").unwrap_err();
        assert!(err.contains("var"));
    }

    #[test]
    fn add_accepts_existing_parent() {
        let mut dag = Dag::new();
        let parent: Arc<str> = Arc::from("root");
        dag.add_node_with_parents(
            parent.clone(),
            valid_tx_var(),
            String::new(),
            "transferToken".to_string(),
        )
        .unwrap();
        assert!(validate_add(&dag, "child", &tx(&["root"]), "transferToken").is_ok());
    }

    #[test]
    fn remove_rejects_empty_list() {
        let dag = Dag::new();
        assert!(validate_remove(&dag, &[]).is_err());
    }

    #[test]
    fn remove_rejects_missing_node() {
        let dag = Dag::new();
        let nodes = vec![Arc::from("ghost")];
        assert!(validate_remove(&dag, &nodes).is_err());
    }

    #[test]
    fn remove_accepts_existing_node() {
        let mut dag = Dag::new();
        let node: Arc<str> = Arc::from("n");
        dag.add_node_with_parents(
            node.clone(),
            valid_tx_var(),
            String::new(),
            "transferToken".to_string(),
        )
        .unwrap();
        assert!(validate_remove(&dag, &[node]).is_ok());
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "dagdb-sm-test-{}-{}-{id}",
            std::process::id(),
            name
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("sm.redb")
    }

    #[tokio::test]
    async fn state_machine_state_survives_reopen() {
        let path = temp_path("survive");

        // Первый запуск: наполняем Dag и помечаем узел как удалённый (added).
        {
            let store = StateMachineStore::open(&path).unwrap();
            let mut sm = store.state_machine.write().await;
            let node: Arc<str> = Arc::from("node-1");
            sm.dag
                .add_node_with_parents(
                    node.clone(),
                    valid_tx_var(),
                    String::new(),
                    "transferToken".to_string(),
                )
                .unwrap();
            let removed: Arc<str> = Arc::from("node-2");
            sm.dag
                .add_node_with_parents(
                    removed.clone(),
                    valid_tx_var(),
                    String::new(),
                    "transferToken".to_string(),
                )
                .unwrap();
            sm.dag.remove_node(removed.clone());
            store.persist(&sm).unwrap();

            assert!(sm.dag.contains_node(&node));
            assert!(sm.dag.is_node_added(&removed));
        }

        // Второй запуск: состояние должно восстановиться, включая реестр added.
        {
            let store = StateMachineStore::open(&path).unwrap();
            let sm = store.state_machine.read().await;
            assert!(
                sm.dag.contains_node(&Arc::from("node-1")),
                "Dag должен пережить рестарт"
            );
            assert!(
                sm.dag.is_node_added(&Arc::from("node-2")),
                "реестр added должен переживать рестарт"
            );
            assert_eq!(sm.dag.get_node_count(), 1);
        }
    }

    #[tokio::test]
    async fn in_memory_store_does_not_persist() {
        // Default-хранилище остаётся in-memory (без db) — persist безопасен.
        let store = StateMachineStore::default();
        let sm = store.state_machine.read().await;
        store.persist(&sm).unwrap();
    }

    #[tokio::test]
    async fn apply_add_then_reopen_restores_dag() {
        use crate::raft::typ::Entry;
        use openraft::entry::RaftEntry;

        let path = temp_path("apply-add");
        let node_hash: Arc<str> = Arc::from("node-1");
        let req = Request::Add {
            hash: node_hash.clone(),
            tx: valid_tx_var(),
            sign: String::new(),
            func: "transferToken".to_string(),
        };
        let log_id = openraft::testing::log_id::<TypeConfig>(1, 1, 1);
        let entry = Entry::new(log_id, EntryPayload::Normal(req));

        {
            let store = StateMachineStore::open(&path).unwrap();
            let mut sm = store.clone();
            let res = sm.apply(vec![entry]).await.unwrap();
            assert_eq!(res[0].value.as_deref(), Some("Ok"));
            assert!(
                store
                    .state_machine
                    .read()
                    .await
                    .dag
                    .contains_node(&node_hash)
            );
        }

        {
            let store = StateMachineStore::open(&path).unwrap();
            let sm = store.state_machine.read().await;
            assert!(
                sm.dag.contains_node(&node_hash),
                "Dag должен восстановиться после рестарта"
            );
            assert_eq!(sm.last_applied, Some(log_id));
        }
    }

    #[tokio::test]
    async fn apply_rejects_duplicate_add_and_remove_roundtrip() {
        use crate::raft::typ::Entry;
        use openraft::entry::RaftEntry;

        let path = temp_path("apply-dup");
        let node_hash: Arc<str> = Arc::from("node-1");
        let add = Request::Add {
            hash: node_hash.clone(),
            tx: valid_tx_var(),
            sign: String::new(),
            func: "transferToken".to_string(),
        };

        let store = StateMachineStore::open(&path).unwrap();
        let mut sm = store.clone();

        // Первое добавление успешно.
        let e1 = Entry::new(
            openraft::testing::log_id::<TypeConfig>(1, 1, 1),
            EntryPayload::Normal(add.clone()),
        );
        assert_eq!(
            sm.apply(vec![e1]).await.unwrap()[0].value.as_deref(),
            Some("Ok")
        );

        // Повторное добавление того же узла отклоняется (уникальность хэша).
        let e2 = Entry::new(
            openraft::testing::log_id::<TypeConfig>(1, 1, 2),
            EntryPayload::Normal(add),
        );
        let dup = sm.apply(vec![e2]).await.unwrap();
        assert!(dup[0].value.as_deref().unwrap().starts_with("Error"));
        assert_eq!(store.state_machine.read().await.dag.get_node_count(), 1);

        // Удаляем узел: он уходит в added.
        let remove = Request::Remove {
            nodes: vec![node_hash.clone()],
        };
        let e3 = Entry::new(
            openraft::testing::log_id::<TypeConfig>(1, 1, 3),
            EntryPayload::Normal(remove),
        );
        assert_eq!(
            sm.apply(vec![e3]).await.unwrap()[0].value.as_deref(),
            Some("Ok")
        );
        {
            let guard = store.state_machine.read().await;
            assert!(!guard.dag.contains_node(&node_hash));
            assert!(guard.dag.is_node_added(&node_hash));
        }

        // После reopen: узел остаётся удалённым (added переживает рестарт).
        drop(sm);
        drop(store);
        let reopened = StateMachineStore::open(&path).unwrap();
        let guard = reopened.state_machine.read().await;
        assert!(!guard.dag.contains_node(&node_hash));
        assert!(guard.dag.is_node_added(&node_hash));
    }

    #[tokio::test]
    async fn apply_rejects_missing_parent_at_apply_time() {
        use crate::raft::typ::Entry;
        use openraft::entry::RaftEntry;

        // Даже если ранняя проверка на входе пропустила запрос, state machine
        // (авторитетная точка) отклоняет его при применении.
        let path = temp_path("apply-missing-parent");
        let store = StateMachineStore::open(&path).unwrap();
        let mut sm = store.clone();

        let req = Request::Add {
            hash: Arc::from("child"),
            tx: tx(&["nonexistent-parent"]),
            sign: String::new(),
            func: "transferToken".to_string(),
        };
        let entry = Entry::new(
            openraft::testing::log_id::<TypeConfig>(1, 1, 1),
            EntryPayload::Normal(req),
        );

        let res = sm.apply(vec![entry]).await.unwrap();
        assert!(
            res[0].value.as_deref().unwrap().starts_with("Error"),
            "родитель отсутствует — применение должно быть отклонено"
        );
        assert!(
            !store
                .state_machine
                .read()
                .await
                .dag
                .contains_node(&Arc::from("child")),
            "невалидная команда не меняет DAG"
        );
    }
}
