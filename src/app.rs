use std::sync::Arc;

use crate::router::Router;
use crate::typ;
use crate::NodeId;
use crate::StateMachineStore;

use tokio::sync::Mutex;

/// Representation of an application state.
#[derive(Clone)]
pub struct App {
    pub id: NodeId,
    pub addr: String,
    pub raft: typ::Raft,
    pub router: Router,
    pub state_machine: Arc<StateMachineStore>,
}

impl App {
    pub fn new(id: NodeId, addr: String, raft: typ::Raft, router: Router, state_machine: Arc<StateMachineStore>) -> Self {
        Self {
            id,
            addr,
            raft,
            router,
            state_machine,
        }
    }
}

pub type SharedApp = Arc<Mutex<App>>;
