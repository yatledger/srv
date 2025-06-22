pub mod graph;
pub mod server;
pub mod cleaner;
pub mod updater;
use crate::command::{Request, Response};

use openraft::Config;

use crate::app::App;
use crate::router::Router;
use crate::store::StateMachineData;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use graph::DAG;

pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = Request,
        R = Response,
        SnapshotData = StateMachineData,
);

pub mod typ {
    use crate::TypeConfig;

    pub type Raft = openraft::Raft<TypeConfig>;

    pub type Vote = <TypeConfig as openraft::RaftTypeConfig>::Vote;
    pub type LeaderId = <TypeConfig as openraft::RaftTypeConfig>::LeaderId;
    pub type LogId = openraft::LogId<TypeConfig>;
    pub type Entry = <TypeConfig as openraft::RaftTypeConfig>::Entry;
    pub type EntryPayload = openraft::EntryPayload<TypeConfig>;
    pub type Membership = openraft::membership::Membership<TypeConfig>;
    pub type StoredMembership = openraft::StoredMembership<TypeConfig>;

    pub type Node = <TypeConfig as openraft::RaftTypeConfig>::Node;

    pub type LogState = openraft::storage::LogState<TypeConfig>;

    pub type SnapshotMeta = openraft::SnapshotMeta<TypeConfig>;
    pub type Snapshot = openraft::Snapshot<TypeConfig>;
    pub type SnapshotData = <TypeConfig as openraft::RaftTypeConfig>::SnapshotData;

    pub type IOFlushed = openraft::storage::IOFlushed<TypeConfig>;

    pub type Infallible = openraft::error::Infallible;
    pub type Fatal = openraft::error::Fatal<TypeConfig>;
    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<TypeConfig, E>;
    pub type RPCError<E = openraft::error::Infallible> = openraft::error::RPCError<TypeConfig, E>;

    pub type ErrorSubject = openraft::ErrorSubject<TypeConfig>;
    pub type StorageError = openraft::StorageError<TypeConfig>;
    pub type StreamingError = openraft::error::StreamingError<TypeConfig>;

    pub type RaftMetrics = openraft::RaftMetrics<TypeConfig>;

    pub type ClientWriteError = openraft::error::ClientWriteError<TypeConfig>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<TypeConfig>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<TypeConfig>;
    pub type InitializeError = openraft::error::InitializeError<TypeConfig>;

    pub type VoteRequest = openraft::raft::VoteRequest<TypeConfig>;
    pub type VoteResponse = openraft::raft::VoteResponse<TypeConfig>;
    pub type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;
    pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<TypeConfig>;
    pub type InstallSnapshotRequest = openraft::raft::InstallSnapshotRequest<TypeConfig>;
    pub type InstallSnapshotResponse = openraft::raft::InstallSnapshotResponse<TypeConfig>;
    pub type SnapshotResponse = openraft::raft::SnapshotResponse<TypeConfig>;
    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;
}

pub fn encode<T: serde::Serialize>(t: T) -> String {
    serde_json::to_string(&t).unwrap()
}

pub fn decode<T: serde::de::DeserializeOwned>(s: &str) -> T {
    serde_json::from_str(s).unwrap()
}

#[cfg(test)]
mod test;

pub mod command;  // Новый модуль для команд и ответов
pub mod app;
pub mod store;
pub mod network;
pub mod log;
pub mod router;
pub mod api;

pub type StateMachineStore = store::StateMachineStore;
pub use log::LogStore;

// Псевдоним для списка смежности графа: узел -> список его детей или родителей.
pub type Adjacency = HashMap<Arc<str>, Vec<Arc<str>>>;
pub type DagDb = Arc<RwLock<DAG>>;

pub async fn new_raft(node_id: NodeId, router: Router) -> (typ::Raft, App) {
    // Create a configuration for the raft instance.
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        // Once snapshot is built, delete the logs at once.
        // So that all further replication will be based on the snapshot.
        max_in_snapshot_log_to_keep: 0,
        ..Default::default()
    };

    let config = Arc::new(config.validate().unwrap());

    // Create a instance of where the Raft logs will be stored.
    let log_store = LogStore::default();

    // Create a instance of where the state machine data will be stored.
    let state_machine_store = Arc::new(StateMachineStore::default());

    // Create a local raft instance.
    let raft = openraft::Raft::new(node_id, config, router.clone(), log_store, state_machine_store.clone())
        .await
        .unwrap();

    let app = App::new(node_id, raft.clone(), router, state_machine_store);

    (raft, app)
}
