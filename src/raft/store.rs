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
use crate::domain::{Func, Hash};
use crate::graph::dag::{Dag, extract_from_var_struct};

/// Детерминированная валидация команды `Add` в state machine.
///
/// Проверки не зависят от сети/времени и дают одинаковый результат на всех
/// репликах при одинаковом состоянии: уникальность узла, существование и
/// уникальность родителей в живом DAG, монотонность `seq` по адресу (V18),
/// допустимость `func` и структуры `var`.
///
/// Ограничение на количество родителей (2..100) намеренно не проверяется здесь:
/// это политика входного API, а генезис-узлы легитимно имеют пустой список
/// родителей. Проверка длины остаётся на границе (`validate_parents`).
fn validate_add(dag: &Dag, hash: &Hash, tx: &crate::Tx, func: &Func) -> Result<(), String> {
    // Узел с таким хэшем не должен уже существовать (ни в Dag, ни в реестре added).
    if dag.contains_node(hash) || dag.is_node_added(hash) {
        return Err("node already exists".to_string());
    }

    // Родители: уникальны, непусты и существуют **в живом DAG**. Родитель,
    // оставшийся только в реестре `added` (удалённый), не допускается — иначе
    // связь молча терялась бы, а `Node.parents` был бы неполным (V15/C38).
    let mut seen = std::collections::HashSet::new();
    for parent in &tx.prnts {
        if parent.trim().is_empty() {
            return Err("parent must not be empty".to_string());
        }
        if !seen.insert(parent.clone()) {
            return Err("parents must be unique".to_string());
        }
        if !dag.contains_node(parent) {
            if dag.is_node_added(parent) {
                return Err(format!(
                    "parent {parent} was removed and cannot be referenced"
                ));
            }
            return Err(format!("parent {parent} does not exist"));
        }
    }

    // V18: `seq` адреса обязан строго возрастать — защита от replay/stale-подписи
    // после вытеснения старого узла из реестра `added`.
    dag.accepts_seq(&tx.addr, tx.seq)?;

    // Структура `var` обязательна для известных функций.
    if *func == Func::TransferToken {
        extract_from_var_struct(tx)?.validate()?;
    }

    Ok(())
}

/// Детерминированная валидация команды `Remove` в state machine.
///
/// Список не пуст, все узлы существуют в Dag. Пустой/частично отсутствующий
/// список не применяется.
fn validate_remove(dag: &Dag, nodes: &[Hash]) -> Result<(), String> {
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
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StateMachineData {
    pub last_applied: Option<LogId>,

    pub last_membership: StoredMembership,

    /// Application data.
    pub dag: Dag,

    /// Версия схемы состояния (O6). Старые снапшоты без поля читаются как 0
    /// и мигрируют к текущей версии при загрузке.
    #[serde(default)]
    pub schema_version: u32,
}

/// Текущая версия схемы состояния. Увеличивается при несовместимом изменении
/// формата; старые снапшоты прогоняются через [`StateMachineData::migrate`].
///
/// Версия 3 (V18): добавлена карта `Dag.last_seq` (монотонность `seq` по адресу);
/// отсутствие поля в старых снапшотах читается как пустая карта.
pub const SCHEMA_VERSION: u32 = 3;

impl Default for StateMachineData {
    fn default() -> Self {
        Self {
            last_applied: None,
            last_membership: StoredMembership::default(),
            dag: Dag::default(),
            schema_version: SCHEMA_VERSION,
        }
    }
}

impl StateMachineData {
    /// Приводит загруженное состояние к текущей версии схемы (O6).
    ///
    /// Версия «из будущего» — ошибка (узел не должен молча портить данные).
    /// Для версии ниже текущей выполняются шаги миграции; версия 0 → 2
    /// тождественна, т.к. `Dag` уже восстанавливает `added`/`added_seq`.
    pub fn migrate(mut self) -> Result<Self, String> {
        if self.schema_version > SCHEMA_VERSION {
            return Err(format!(
                "state schema version {} is newer than supported {}",
                self.schema_version, SCHEMA_VERSION
            ));
        }
        if self.schema_version < SCHEMA_VERSION {
            // Место для будущих несовместимых миграций формата.
            self.schema_version = SCHEMA_VERSION;
        }
        Ok(self)
    }
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

    /// ID узла-владельца: используется только для человекочитаемых логов
    /// применения (`нода N записала …`), на состояние/детерминизм не влияет.
    node_id: crate::NodeId,
}

/// Именованные ключи в таблице метаданных для state machine.
const KEY_SM_LAST_APPLIED: &str = "sm_last_applied";
const KEY_SM_MEMBERSHIP: &str = "sm_membership";
const KEY_SM_SNAPSHOT: &str = "sm_snapshot";

impl StateMachineStore {
    /// Открывает персистентное state machine по пути `path`, восстанавливая
    /// ранее сохранённое состояние (Dag, `added`, `last_applied`, membership).
    pub fn open(path: &std::path::Path, node_id: crate::NodeId) -> Result<Arc<Self>, String> {
        let db = Db::open(path)?;

        let state_machine: StateMachineData = match db.meta_get(KEY_SM_SNAPSHOT)? {
            Some(bytes) => serde_json::from_slice(&bytes).map_err(|e| e.to_string())?,
            None => StateMachineData::default(),
        };
        // O6: мигрируем схему до текущей версии (старые снапшоты → SCHEMA_VERSION).
        let state_machine = state_machine.migrate()?;

        Ok(Arc::new(Self {
            state_machine: RwLock::new(state_machine),
            snapshot_idx: Mutex::new(0),
            current_snapshot: Mutex::new(None),
            db: Some(db),
            node_id,
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
                EntryPayload::Blank => res.push(Response::blank()),
                EntryPayload::Normal(ref req) => match req {
                    Request::Add {
                        hash,
                        tx,
                        sign,
                        func,
                    } => {
                        // Детерминированная валидация: не зависит от сети/времени и
                        // одинакова на всех репликах. Применяется валидная команда,
                        // невалидная логируется и возвращает отказ без изменения Dag.
                        match validate_add(&sm.dag, hash, tx, func) {
                            Ok(()) => match sm.dag.add_node_with_parents(
                                hash.clone(),
                                tx.clone(),
                                sign.clone(),
                                *func,
                            ) {
                                Ok(()) => {
                                    let node_id = self.node_id;
                                    let short = crate::utils::short_hash(hash.as_str());
                                    tracing::info!("нода {node_id} записала транзакцию {short}");
                                    res.push(Response::ok())
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to apply Add request: {}", e);
                                    res.push(Response::rejected(e));
                                }
                            },
                            Err(e) => {
                                tracing::warn!("Validation failed for Add request: {}", e);
                                res.push(Response::rejected(e));
                            }
                        }
                    }
                    Request::Remove { nodes } => {
                        // Детерминированная валидация удаления: все узлы обязаны
                        // существовать в Dag. Пустой список невалиден.
                        match validate_remove(&sm.dag, nodes) {
                            Ok(()) => match sm.dag.remove_nodes(nodes.clone()) {
                                Ok(()) => res.push(Response::ok()),
                                Err(e) => {
                                    tracing::warn!("Failed to apply Remove request: {}", e);
                                    res.push(Response::rejected(e));
                                }
                            },
                            Err(e) => {
                                tracing::warn!("Validation failed for Remove request: {}", e);
                                res.push(Response::rejected(e));
                            }
                        }
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(Response::blank())
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
            let updated_state_machine: StateMachineData = new_snapshot
                .data
                .clone()
                .migrate()
                .map_err(|e| sm_storage_error(openraft::ErrorVerb::Write, e))?;
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

    fn tx(parents: &[&str]) -> Tx {
        Tx {
            prnts: parents.iter().map(|p| Hash::from(*p)).collect(),
            addr: crate::domain::Address::from("addr"),
            seq: 0,
            var: json!({ "ca": "a", "to": "b", "val": 1, "msg": "m" }),
        }
    }

    /// Транзакция с заданным `seq` (для проверок монотонности V18).
    fn tx_seq(parents: &[&str], seq: u32) -> Tx {
        Tx {
            prnts: parents.iter().map(|p| Hash::from(*p)).collect(),
            addr: crate::domain::Address::from("addr"),
            seq,
            var: json!({ "ca": "a", "to": "b", "val": 1, "msg": "m" }),
        }
    }

    fn valid_tx_var() -> Tx {
        tx(&[])
    }

    #[test]
    fn add_accepts_genesis_without_parents() {
        let dag = Dag::new();
        assert!(
            validate_add(
                &dag,
                &Hash::from("genesis"),
                &valid_tx_var(),
                &Func::TransferToken
            )
            .is_ok()
        );
    }

    #[test]
    fn add_rejects_duplicate_node() {
        let mut dag = Dag::new();
        let hash = Hash::from("n1");
        dag.add_node_with_parents(
            hash.clone(),
            valid_tx_var(),
            String::new(),
            Func::TransferToken,
        )
        .unwrap();
        let err = validate_add(
            &dag,
            &Hash::from("n1"),
            &valid_tx_var(),
            &Func::TransferToken,
        )
        .unwrap_err();
        assert!(err.contains("already exists"));
    }

    #[test]
    fn add_rejects_missing_parent() {
        let dag = Dag::new();
        let err = validate_add(
            &dag,
            &Hash::from("n2"),
            &tx(&["missing-a", "missing-b"]),
            &Func::TransferToken,
        )
        .unwrap_err();
        assert!(err.contains("does not exist"));
    }

    #[test]
    fn add_rejects_duplicate_parents() {
        // Оба родителя должны существовать, чтобы дойти до проверки уникальности.
        let mut dag = Dag::new();
        let parent_a = Hash::from("pa");
        let parent_b = Hash::from("pb");
        dag.add_node_with_parents(
            parent_a.clone(),
            valid_tx_var(),
            String::new(),
            Func::TransferToken,
        )
        .unwrap();
        dag.add_node_with_parents(
            parent_b.clone(),
            valid_tx_var(),
            String::new(),
            Func::TransferToken,
        )
        .unwrap();

        let err = validate_add(
            &dag,
            &Hash::from("n3"),
            &tx_seq(&["pa", "pa"], 2),
            &Func::TransferToken,
        )
        .unwrap_err();
        assert!(err.contains("unique"));
    }

    #[test]
    fn add_rejects_invalid_var() {
        let dag = Dag::new();
        let mut bad = valid_tx_var();
        bad.var = json!({ "totally": "wrong" });
        let err = validate_add(&dag, &Hash::from("n5"), &bad, &Func::TransferToken).unwrap_err();
        assert!(err.contains("var"));
    }

    #[test]
    fn add_accepts_existing_parent() {
        let mut dag = Dag::new();
        let parent = Hash::from("root");
        dag.add_node_with_parents(
            parent.clone(),
            valid_tx_var(),
            String::new(),
            Func::TransferToken,
        )
        .unwrap();
        assert!(
            validate_add(
                &dag,
                &Hash::from("child"),
                &tx_seq(&["root"], 1),
                &Func::TransferToken
            )
            .is_ok()
        );
    }

    #[test]
    fn add_rejects_parent_that_was_removed() {
        // V15/C38: родитель в реестре `added` (удалённый узел) не допускается.
        let mut dag = Dag::new();
        let parent = Hash::from("root");
        dag.add_node_with_parents(
            parent.clone(),
            valid_tx_var(),
            String::new(),
            Func::TransferToken,
        )
        .unwrap();
        dag.remove_node(parent.clone());
        assert!(dag.is_node_added(&parent));

        let err = validate_add(
            &dag,
            &Hash::from("child"),
            &tx(&["root"]),
            &Func::TransferToken,
        )
        .unwrap_err();
        assert!(err.contains("removed"), "получено: {err}");
    }

    #[test]
    fn add_rejects_non_monotonic_seq_for_same_addr() {
        // V18: seq обязан строго возрастать для одного адреса.
        let mut dag = Dag::new();
        dag.add_node_with_parents(
            Hash::from("n0"),
            tx_seq(&[], 0),
            String::new(),
            Func::TransferToken,
        )
        .unwrap();
        assert!(
            dag.accepts_seq(&crate::domain::Address::from("addr"), 1)
                .is_ok()
        );
        // Повтор/откат номера отклоняется.
        let err = validate_add(
            &dag,
            &Hash::from("n1"),
            &tx_seq(&[], 0),
            &Func::TransferToken,
        )
        .unwrap_err();
        assert!(err.contains("non-monotonic"), "получено: {err}");
    }

    #[test]
    fn genesis_empty_addr_is_exempt_from_seq_check() {
        // V18: bootstrap-генезис с пустым addr не участвует в проверке монотонности.
        let dag = Dag::new();
        let mut genesis = valid_tx_var();
        genesis.addr = crate::domain::Address::from("");
        assert!(validate_add(&dag, &Hash::from("g1"), &genesis, &Func::TransferToken).is_ok());
    }

    #[test]
    fn remove_rejects_empty_list() {
        let dag = Dag::new();
        assert!(validate_remove(&dag, &[]).is_err());
    }

    #[test]
    fn remove_rejects_missing_node() {
        let dag = Dag::new();
        let nodes = vec![Hash::from("ghost")];
        assert!(validate_remove(&dag, &nodes).is_err());
    }

    #[test]
    fn remove_accepts_existing_node() {
        let mut dag = Dag::new();
        let node = Hash::from("n");
        dag.add_node_with_parents(
            node.clone(),
            valid_tx_var(),
            String::new(),
            Func::TransferToken,
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
            let store = StateMachineStore::open(&path, 1).unwrap();
            let mut sm = store.state_machine.write().await;
            let node = Hash::from("node-1");
            sm.dag
                .add_node_with_parents(
                    node.clone(),
                    valid_tx_var(),
                    String::new(),
                    Func::TransferToken,
                )
                .unwrap();
            let removed = Hash::from("node-2");
            sm.dag
                .add_node_with_parents(
                    removed.clone(),
                    valid_tx_var(),
                    String::new(),
                    Func::TransferToken,
                )
                .unwrap();
            sm.dag.remove_node(removed.clone());
            store.persist(&sm).unwrap();

            assert!(sm.dag.contains_node(&node));
            assert!(sm.dag.is_node_added(&removed));
        }

        // Второй запуск: состояние должно восстановиться, включая реестр added.
        {
            let store = StateMachineStore::open(&path, 1).unwrap();
            let sm = store.state_machine.read().await;
            assert!(
                sm.dag.contains_node(&Hash::from("node-1")),
                "Dag должен пережить рестарт"
            );
            assert!(
                sm.dag.is_node_added(&Hash::from("node-2")),
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
        let node_hash = Hash::from("node-1");
        let req = Request::Add {
            hash: node_hash.clone(),
            tx: valid_tx_var(),
            sign: String::new(),
            func: Func::TransferToken,
        };
        let log_id = openraft::testing::log_id::<TypeConfig>(1, 1, 1);
        let entry = Entry::new(log_id, EntryPayload::Normal(req));

        {
            let store = StateMachineStore::open(&path, 1).unwrap();
            let mut sm = store.clone();
            let res = sm.apply(vec![entry]).await.unwrap();
            assert_eq!(res[0], Response::ok());
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
            let store = StateMachineStore::open(&path, 1).unwrap();
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
        let node_hash = Hash::from("node-1");
        let add = Request::Add {
            hash: node_hash.clone(),
            tx: valid_tx_var(),
            sign: String::new(),
            func: Func::TransferToken,
        };

        let store = StateMachineStore::open(&path, 1).unwrap();
        let mut sm = store.clone();

        // Первое добавление успешно.
        let e1 = Entry::new(
            openraft::testing::log_id::<TypeConfig>(1, 1, 1),
            EntryPayload::Normal(add.clone()),
        );
        assert_eq!(sm.apply(vec![e1]).await.unwrap()[0], Response::ok());

        // Повторное добавление того же узла отклоняется (уникальность хэша).
        let e2 = Entry::new(
            openraft::testing::log_id::<TypeConfig>(1, 1, 2),
            EntryPayload::Normal(add),
        );
        let dup = sm.apply(vec![e2]).await.unwrap();
        assert!(dup[0].rejection().is_some());
        assert_eq!(store.state_machine.read().await.dag.get_node_count(), 1);

        // Удаляем узел: он уходит в added.
        let remove = Request::Remove {
            nodes: vec![node_hash.clone()],
        };
        let e3 = Entry::new(
            openraft::testing::log_id::<TypeConfig>(1, 1, 3),
            EntryPayload::Normal(remove),
        );
        assert_eq!(sm.apply(vec![e3]).await.unwrap()[0], Response::ok());
        {
            let guard = store.state_machine.read().await;
            assert!(!guard.dag.contains_node(&node_hash));
            assert!(guard.dag.is_node_added(&node_hash));
        }

        // После reopen: узел остаётся удалённым (added переживает рестарт).
        drop(sm);
        drop(store);
        let reopened = StateMachineStore::open(&path, 1).unwrap();
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
        let store = StateMachineStore::open(&path, 1).unwrap();
        let mut sm = store.clone();

        let req = Request::Add {
            hash: Hash::from("child"),
            tx: tx(&["nonexistent-parent"]),
            sign: String::new(),
            func: Func::TransferToken,
        };
        let entry = Entry::new(
            openraft::testing::log_id::<TypeConfig>(1, 1, 1),
            EntryPayload::Normal(req),
        );

        let res = sm.apply(vec![entry]).await.unwrap();
        assert!(
            res[0].rejection().is_some(),
            "родитель отсутствует — применение должно быть отклонено"
        );
        assert!(
            !store
                .state_machine
                .read()
                .await
                .dag
                .contains_node(&Hash::from("child")),
            "невалидная команда не меняет DAG"
        );
    }

    #[test]
    fn migrate_accepts_current_version() {
        let data = StateMachineData::default();
        assert_eq!(data.schema_version, SCHEMA_VERSION);
        let migrated = data.migrate().unwrap();
        assert_eq!(migrated.schema_version, SCHEMA_VERSION);
    }

    #[test]
    fn migrate_upgrades_legacy_and_rejects_future() {
        // Старый снапшот без версии читается как версия 0 и мигрирует к текущей.
        let data = StateMachineData {
            schema_version: 0,
            ..Default::default()
        };
        let migrated = data.migrate().unwrap();
        assert_eq!(migrated.schema_version, SCHEMA_VERSION);

        // Версия «из будущего» — ошибка.
        let future = StateMachineData {
            schema_version: SCHEMA_VERSION + 1,
            ..Default::default()
        };
        assert!(future.migrate().is_err());
    }

    #[tokio::test]
    async fn legacy_state_file_without_schema_version_loads() {
        // Имитируем старый снапшот: JSON без поля schema_version.
        let path = temp_path("legacy-schema");
        let mut legacy = serde_json::to_value(StateMachineData::default()).unwrap();
        legacy.as_object_mut().unwrap().remove("schema_version");
        {
            let db = Db::open(&path).unwrap();
            db.meta_set(
                KEY_SM_SNAPSHOT,
                serde_json::to_vec(&legacy).unwrap().as_slice(),
            )
            .unwrap();
        }
        let store = StateMachineStore::open(&path, 1).expect("старый снапшот должен загрузиться");
        let sm = store.state_machine.read().await;
        assert_eq!(sm.schema_version, SCHEMA_VERSION);
    }

    #[tokio::test]
    async fn file_level_backup_and_restore_preserves_state() {
        // O5: процедура бэкапа сводится к копии DATA_DIR; проверяем, что копия
        // корректно открывается и сохраняет DAG.
        let path = temp_path("backup-source");
        let node = Hash::from("backup-node");
        {
            let store = StateMachineStore::open(&path, 1).unwrap();
            let mut sm = store.state_machine.write().await;
            sm.dag
                .add_node_with_parents(
                    node.clone(),
                    valid_tx_var(),
                    String::new(),
                    Func::TransferToken,
                )
                .unwrap();
            store.persist(&sm).unwrap();
        }

        // «Бэкап»: копируем файл данных.
        let backup = path.with_extension("backup.redb");
        std::fs::copy(&path, &backup).unwrap();

        // «Восстановление»: открываем копию и видим тот же узел.
        let restored = StateMachineStore::open(&backup, 1).unwrap();
        let sm = restored.state_machine.read().await;
        assert!(sm.dag.contains_node(&node));
        assert_eq!(sm.schema_version, SCHEMA_VERSION);
    }
}
