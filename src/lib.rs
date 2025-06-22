use std::io::Cursor;
pub mod graph;
pub mod server;
pub mod cleaner;
pub mod updater;
use crate::command::{ClientRequest, ClientResponse};

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use graph::DAG;

pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Declare the type configuration for `MemStore`.
    pub TypeConfig:
        D = ClientRequest,
        R = ClientResponse,
        Node = (),
);

pub mod typ {
    use openraft::BasicNode;

    use crate::NodeId;
    use crate::TypeConfig;

    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
    pub type RPCError<E = openraft::error::Infallible> = openraft::error::RPCError<NodeId, BasicNode, RaftError<E>>;

    pub type ClientWriteError = openraft::error::ClientWriteError<NodeId, BasicNode>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<NodeId, BasicNode>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<NodeId, BasicNode>;
    pub type InitializeError = openraft::error::InitializeError<NodeId, BasicNode>;

    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;
}

pub mod command;  // Новый модуль для команд и ответов
pub mod app;
pub mod store;
pub mod network;

// Псевдоним для списка смежности графа: узел -> список его детей или родителей.
pub type Adjacency = HashMap<Arc<str>, Vec<Arc<str>>>;
pub type DagDb = Arc<RwLock<DAG>>;