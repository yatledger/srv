use std::collections::HashMap;
use std::sync::{Arc};
use openraft::Config;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use redis::aio::ConnectionManager;

mod graph;
mod raft;
pub mod app;
pub mod processor;
pub mod server;
pub mod web;
pub mod utils;

use crate::app::App;
use raft::log::LogStore;
use raft::typ;
use raft::router::Router as RaftRouter;
use raft::network::NetworkFactory;
use raft::command::{Request, Response};
use raft::store::{StateMachineStore, StateMachineData};
use rocksdb::{DB, Options, ColumnFamilyDescriptor};
use std::path::Path;

// Псевдоним для списка смежности графа: узел -> список его детей или родителей.
pub type Adjacency = HashMap<Arc<str>, Vec<Arc<str>>>;



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
    // Create database path for RocksDB
    let db_path = format!("./raft_data/node_{}", node_id);
    let db_path = Path::new(&db_path);
    std::fs::create_dir_all(db_path.parent().unwrap()).expect("Failed to create Raft data directory");

    // Configure and open RocksDB with required column families
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);

    let meta = ColumnFamilyDescriptor::new("meta", Options::default());
    let logs = ColumnFamilyDescriptor::new("logs", Options::default());

    let db = DB::open_cf_descriptors(&db_opts, db_path, vec![meta, logs])
        .expect("Failed to open RocksDB database");
    let db = Arc::new(db);

    // Create a instance of where the Raft logs will be stored.
    let log_store = LogStore::new(db);

    // Create a instance of where the state machine data will be stored.
    let state_machine_store = Arc::new(StateMachineStore::default());
    let raft_router = RaftRouter::new();
    let network = NetworkFactory::new(raft_router);

    // Create a local raft instance.
    let raft = openraft::Raft::new(node_id, config, network, log_store, state_machine_store.clone())
        .await
        .unwrap();

    let router = web::Router::new();

    // Настраиваем подключение к Redis
    let redis_url = "redis://:REDACTED_ROTATED_SECRET@localhost/0";
    let redis_client = redis::Client::open(redis_url).expect("Failed to create Redis client");
    let redis = ConnectionManager::new(redis_client).await.unwrap();

    let app = App::new(node_id, http_addr, raft.clone(), state_machine_store, router, redis);

    (raft, app)
}
