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

use crate::graph::dag::DAG;

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
    pub dag: DAG,
}

/// Defines a state machine for the Raft cluster. This state machine represents a copy of the
/// data for this node. Additionally, it is responsible for storing the last snapshot of the data.
#[derive(Debug, Default)]
pub struct StateMachineStore {
    /// The Raft state machine.
    pub state_machine: RwLock<StateMachineData>,

    snapshot_idx: Mutex<u64>,

    /// The last received snapshot.
    current_snapshot: Mutex<Option<StoredSnapshot>>,
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
            let mut l = self.snapshot_idx.lock().unwrap();
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

        {
            let mut current_snapshot = self.current_snapshot.lock().unwrap();
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
                        // --- ВАША ЛОГИКА ВАЛИДАЦИИ ---
                        // Например, проверяем, что все родители существуют
                        // let parents_exist = tx.prnts.iter().all(|p| sm.dag.get_nodes().contains_key(p));
                        let parents_exist = true;

                        if !parents_exist {
                            // Проверка не пройдена. НЕ меняем DAG.
                            // Отправляем клиенту сообщение об ошибке.
                            tracing::warn!(
                                "Validation failed for Add request: parents do not exist."
                            );
                            res.push(Response {
                                value: Some("Error: One or more parents not found.".to_string()),
                            });
                        } else {
                            // Проверка пройдена. Меняем DAG.
                            let _ = sm.dag.add_node_with_parents(
                                hash.clone(),
                                tx.clone(),
                                sign.clone(),
                                func.clone(),
                            );
                            res.push(Response {
                                value: Some("Ok".to_string()),
                            });
                        }
                    }
                    Request::Remove { nodes } => {
                        // --- ВАША ЛОГИКА ВАЛИДАЦИИ ---
                        // Например, проверяем, что узлы вообще существуют перед удалением
                        // let nodes_exist = nodes.iter().all(|n| sm.dag.get_nodes().contains_key(n));
                        let nodes_exist = true;

                        if !nodes_exist {
                            // Проверка не пройдена. НЕ меняем DAG.
                            tracing::warn!(
                                "Validation failed for Remove request: nodes do not exist."
                            );
                            res.push(Response {
                                value: Some(
                                    "Error: One or more nodes for removal not found.".to_string(),
                                ),
                            });
                        } else {
                            // Проверка пройдена. Меняем DAG.
                            let _ = sm.dag.remove_nodes(nodes.clone());
                            res.push(Response {
                                value: Some("Ok".to_string()),
                            });
                        }
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(Response { value: None })
                }
            };
        }
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
            let mut state_machine = self.state_machine.write().await;
            *state_machine = updated_state_machine;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.lock().unwrap();
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, StorageError> {
        match &*self.current_snapshot.lock().unwrap() {
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
