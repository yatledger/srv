use std::future::Future;
use tracing::{info};
use openraft::error::ReplicationClosed;
use openraft::network::v2::RaftNetworkV2;
use openraft::network::RPCOption;
use openraft::BasicNode;
use openraft::OptionalSend;
use openraft::RaftNetworkFactory;

use crate::router::Router;
use crate::typ::*;
use crate::NodeId;
use crate::TypeConfig;

pub struct NetworkFactory {}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Connection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        let router = Router::default();
        let addr = node.addr.clone();

        Connection { addr, router, target }
    }
}

pub struct Connection {
    router: Router,
    addr: String,
    target: NodeId,
}

impl RaftNetworkV2<TypeConfig> for Connection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse, RPCError> {
        info!("APPEND!");
        let resp = self.router.send(self.target, &self.addr, "/raft/append", req).await?;
        Ok(resp)
    }

    /// A real application should replace this method with customized implementation.
    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse, StreamingError> {
        info!("SNAPSHOT");
        let resp = self.router.send(self.target, &self.addr, "/raft/snapshot", (vote, snapshot.meta, snapshot.snapshot)).await?;
        Ok(resp)
    }

    async fn vote(&mut self, req: VoteRequest, _option: RPCOption) -> Result<VoteResponse, RPCError> {
        info!("VOTE");
        let resp = self.router.send(self.target, &self.addr, "/raft/vote", req).await?;
        Ok(resp)
    }
}
