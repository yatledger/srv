use std::sync::Arc;

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
}

impl App {
    pub fn new(
        id: NodeId,
        addr: String,
        raft: Raft,
        state_machine: Arc<StateMachineStore>,
        router: Router,
        redis: ConnectionManager,
        internal_api_token: Arc<str>,
    ) -> Self {
        Self {
            id,
            addr,
            raft,
            state_machine,
            router,
            redis,
            internal_api_token,
        }
    }
}
