use std::sync::Arc;

use super::typ::Raft;
use super::router::Router;
use super::store::StateMachineStore;
use crate::NodeId;

/// Representation of an application state.
#[derive(Clone)]
pub struct App {
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub state_machine: Arc<StateMachineStore>,
    pub router: Router,
}

impl App {
    pub fn new(id: NodeId, addr: String, raft: Raft, state_machine: Arc<StateMachineStore>, router: Router) -> Self {
        Self {
            id,
            addr,
            raft,
            state_machine,
            router,
        }
    }
}
