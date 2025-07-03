use std::collections::HashMap;
use std::sync::{Arc};
use openraft::Config;
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod graph;
mod raft;
pub mod processor;
pub mod server;
pub mod utils;

use raft::app::App;
use raft::typ;
use raft::network::NetworkFactory;
use raft::command::{Request, Response};
use raft::store::{StateMachineStore, StateMachineData};
pub use raft::router;

// Псевдоним для списка смежности графа: узел -> список его детей или родителей.
pub type Adjacency = HashMap<Arc<str>, Vec<Arc<str>>>;

pub use raft::log::LogStore;

pub type NodeId = u64;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Tx {
    prnts: Vec<Arc<str>>,
    addr: Arc<str>,
    seq: u32,
    var: Value,
}

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = Request,
        R = Response,
        SnapshotData = StateMachineData,
);

pub async fn start_raft(node_id: NodeId, http_addr: String) -> (typ::Raft, App) {
    // Create a configuration for the raft instance.
    let config = Config {
        heartbeat_interval: 2500,
        election_timeout_min: 5000,
        election_timeout_max: 10000,
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
    let network = NetworkFactory {};

    // Create a local raft instance.
    let raft = openraft::Raft::new(node_id, config, network, log_store, state_machine_store.clone())
        .await
        .unwrap();

    let router = router::Router::new();

    let app = App::new(node_id, http_addr, raft.clone(), state_machine_store, router);

    (raft, app)
}
