use std::sync::Arc;
use std::time::Duration;

use redis::aio::ConnectionManager;

use crate::NodeId;
use crate::raft::{store::StateMachineStore, typ::Raft};
use crate::web::Router;

/// Representation of an application state.
#[derive(Clone)]
pub struct App {
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub state_machine: Arc<StateMachineStore>,
    pub router: Router,
    pub redis: ConnectionManager,
    /// Кластерный токен для внутренних эндпоинтов.
    pub internal_api_token: Arc<str>,
    /// Интервал фоновой очистки «тяжёлых» узлов.
    pub processor_interval: Duration,
    /// Порог веса, выше которого узел считается «тяжёлым».
    pub weight_threshold: f64,
    /// Максимальный размер батча очистки за цикл.
    pub cleanup_batch_size: usize,
}

impl App {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: NodeId,
        addr: String,
        raft: Raft,
        state_machine: Arc<StateMachineStore>,
        router: Router,
        redis: ConnectionManager,
        internal_api_token: Arc<str>,
        processor_interval: Duration,
        weight_threshold: f64,
        cleanup_batch_size: usize,
    ) -> Self {
        Self {
            id,
            addr,
            raft,
            state_machine,
            router,
            redis,
            internal_api_token,
            processor_interval,
            weight_threshold,
            cleanup_batch_size,
        }
    }
}
