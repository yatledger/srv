//! `dagdb` — распределённый узел хранения DAG-графа транзакций поверх Raft.
//!
//! Публичная точка входа — [`start_raft`], собирающая узел из конфигурации
//! [`AppConfig`](config::AppConfig). Внутренние модули (граф, Raft, очистка)
//! доступны ограниченно; публичный HTTP-API отделён от внутреннего
//! (Raft/mng) и требует кластерного токена.

use openraft::Config;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use redis::aio::ConnectionManager;

pub mod app;
pub mod audit;
pub mod auth;
pub mod cleanup;
pub mod config;
pub mod domain;
pub mod graph;
pub mod metrics;
pub mod processor;
mod raft;
pub mod ratelimit;
pub mod server;
pub mod shutdown;
pub mod tx_logic;
pub mod utils;
pub mod web;

use crate::app::App;
use crate::config::AppConfig;
use crate::domain::{Address, Hash};
use raft::command::{Request, Response};
use raft::log::PersistentLogStore;
use raft::network::NetworkFactory;
use raft::router::Router as RaftRouter;
use raft::store::{StateMachineData, StateMachineStore};
use raft::typ;

/// Псевдоним для списка смежности графа: узел -> список его детей или родителей.
pub type Adjacency = HashMap<Hash, Vec<Hash>>;

pub use raft::log::LogStore;

/// Идентификатор узла кластера.
pub type NodeId = u64;

/// Транзакция — узел DAG. Подписывается ключом `addr` (ed25519, base58);
/// хэш покрывает все поля вместе с `func` (см. [`utils::ordered_sum`]).
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Tx {
    /// Хэши родительских транзакций.
    prnts: Vec<Hash>,
    /// Публичный ключ ed25519 в base58.
    addr: Address,
    /// Порядковый номер транзакции.
    seq: u32,
    /// Данные вызова (структура зависит от `func`).
    var: Value,
}

impl Tx {
    /// Конструирует транзакцию из полей (публичный конструктор для внешних
    /// тестов/бенчмарков; в рантайме транзакции приходят из JSON).
    pub fn new(prnts: Vec<Hash>, addr: Address, seq: u32, var: Value) -> Self {
        Self {
            prnts,
            addr,
            seq,
            var,
        }
    }

    /// Хэши родительских транзакций.
    pub fn parents(&self) -> &[Hash] {
        &self.prnts
    }

    /// Публичный адрес отправителя.
    pub fn address(&self) -> &Address {
        &self.addr
    }

    /// Порядковый номер.
    pub fn sequence(&self) -> u32 {
        self.seq
    }

    /// Данные вызова.
    pub fn var(&self) -> &Value {
        &self.var
    }
}

openraft::declare_raft_types!(
    /// Конфигурация типов Raft для `dagdb`: команды, ответы и данные снапшота.
    pub TypeConfig:
        D = Request,
        R = Response,
        SnapshotData = StateMachineData,
);

/// Собирает и запускает узел Raft: персистентные хранилища, сеть, Redis и
/// [`App`]. Возвращает дескриптор Raft и состояние приложения.
pub async fn start_raft(
    cfg: &AppConfig,
) -> Result<(typ::Raft, App), Box<dyn std::error::Error + Send + Sync>> {
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

    // Подключаем Redis до открытия хранилищ и запуска Raft: при недоступном или
    // неверно сконфигурированном Redis узел должен завершиться с понятной ошибкой,
    // не удерживая redb-блокировки и не поднимая HTTP (F1). Клиент `redis` 0.32
    // может не вернуть ошибку в этих сценариях, поэтому таймаут — на уровне
    // приложения.
    let redis_url = cfg.redis_url()?;
    let redis_client = redis::Client::open(redis_url)
        .map_err(|e| std::io::Error::other(format!("не удалось разобрать REDIS_URL: {e}")))?;
    let connect = ConnectionManager::new(redis_client);
    let redis = match tokio::time::timeout(cfg.redis_connect_timeout(), connect).await {
        Ok(Ok(manager)) => manager,
        Ok(Err(e)) => {
            return Err(std::io::Error::other(format!(
                "Redis connection failed: {e} (проверьте REDIS_URL и доступность Redis)"
            ))
            .into());
        }
        Err(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "Redis connection failed: превышен таймаут {} с (проверьте REDIS_URL и доступность Redis)",
                    cfg.redis_connect_timeout_secs
                ),
            )
            .into());
        }
    };

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
    )?;
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
    )?;

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
        std::sync::Arc::new(crate::metrics::Metrics::new()),
        std::sync::Arc::new(crate::ratelimit::RateLimiter::new(
            cfg.public_rate_limit_per_sec,
            cfg.public_rate_limit_burst,
        )),
        cfg.max_request_bytes(),
        cfg.trust_proxy,
        crate::shutdown::Shutdown::new().0,
    );

    Ok((raft, app))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{LogFormat, Profile};
    use std::path::PathBuf;
    use std::time::Duration;

    fn config_with_redis(redis_url: &str, timeout_secs: u64) -> AppConfig {
        AppConfig {
            id: 1,
            profile: Profile::Dev,
            log_format: LogFormat::Text,
            addr: "127.0.0.1".to_string(),
            port: 21001,
            bind_addr: None,
            advertise_addr: None,
            data_dir: PathBuf::from("./data/test-redis-timeout"),
            redis_url: Some(redis_url.to_string()),
            internal_api_token: Some("token".to_string()),
            trust_proxy: false,
            http_timeout_secs: 10,
            http_connect_timeout_secs: 3,
            raft_http_timeout_secs: 30,
            raft_connect_timeout_secs: 10,
            redis_connect_timeout_secs: timeout_secs,
            processor_interval_ms: 250,
            weight_threshold: 0.5,
            cleanup_batch_size: 100,
            public_rate_limit_per_sec: 50,
            public_rate_limit_burst: 100,
            max_request_bytes: 1_048_576,
        }
    }

    /// F1: при недоступном Redis `start_raft` обязан вернуть ошибку в пределах
    /// таймаута, а не зависнуть. Адрес `127.0.0.1:1` заведомо не слушает.
    #[tokio::test]
    async fn start_raft_fails_fast_when_redis_unreachable() {
        let cfg = config_with_redis("redis://127.0.0.1:1/0", 1);
        let result = tokio::time::timeout(Duration::from_secs(10), start_raft(&cfg)).await;
        let result = result.expect("start_raft завис: не уложился в 10 с при таймауте Redis 1 с");
        assert!(result.is_err(), "ожидалась ошибка подключения к Redis");
    }
}
