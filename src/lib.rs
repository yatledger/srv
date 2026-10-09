use openraft::Config;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use redis::aio::ConnectionManager;

pub mod app;
pub mod auth;
pub mod cleanup;
pub mod config;
mod graph;
pub mod processor;
mod raft;
pub mod server;
pub mod tx_logic;
pub mod utils;
pub mod web;

use crate::app::App;
use crate::config::AppConfig;
use raft::command::{Request, Response};
use raft::log::PersistentLogStore;
use raft::network::NetworkFactory;
use raft::router::Router as RaftRouter;
use raft::store::{StateMachineData, StateMachineStore};
use raft::typ;

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

pub async fn start_raft(cfg: &AppConfig) -> Result<(typ::Raft, App), Box<dyn std::error::Error>> {
    let node_id = cfg.id;
    let http_addr = cfg.advertise_addr();

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

    let config = Arc::new(config.validate()?);

    // Персистентные хранилища: Raft-лог и state machine живут в DATA_DIR.
    let data_dir = cfg.data_dir.clone();
    std::fs::create_dir_all(&data_dir)?;

    let log_store = PersistentLogStore::open(&data_dir.join("raft-log.redb"))?;
    let state_machine_store = StateMachineStore::open(&data_dir.join("state-machine.redb"))?;

    let internal_api_token: Arc<str> = Arc::from(cfg.internal_api_token()?);
    let raft_router = RaftRouter::new(
        cfg.raft_http_timeout(),
        cfg.raft_connect_timeout(),
        internal_api_token.to_string(),
    );
    let network = NetworkFactory::new(raft_router);

    // Create a local raft instance.
    let raft = openraft::Raft::new(
        node_id,
        config,
        network,
        log_store,
        state_machine_store.clone(),
    )
    .await?;

    let router = web::Router::new(
        cfg.http_timeout(),
        cfg.http_connect_timeout(),
        internal_api_token.to_string(),
    );

    // Настраиваем подключение к Redis: строка подключения приходит только из окружения.
    let redis_url = cfg.redis_url()?;
    let redis_client = redis::Client::open(redis_url)?;
    let redis = ConnectionManager::new(redis_client).await?;

    let app = App::new(
        node_id,
        http_addr,
        raft.clone(),
        state_machine_store,
        router,
        redis,
        internal_api_token,
        cfg.processor_interval(),
        cfg.weight_threshold,
        cfg.cleanup_batch_size,
    );

    Ok((raft, app))
}
