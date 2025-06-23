use std::sync::Arc;

use crate::typ::Raft;
use crate::NodeId;
use crate::StateMachineStore;

/// Representation of an application state.
#[derive(Clone)]
pub struct App {
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub state_machine: Arc<StateMachineStore>,
}

impl App {
    pub fn new(id: NodeId, addr: String, raft: Raft, state_machine: Arc<StateMachineStore>) -> Self {
        Self {
            id,
            addr,
            raft,
            state_machine,
        }
    }
}
