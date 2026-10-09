use openraft::BasicNode;
use openraft::OptionalSend;
use openraft::RaftNetworkFactory;
use openraft::error::ReplicationClosed;
use openraft::network::RPCOption;
use openraft::network::v2::RaftNetworkV2;
use std::future::Future;

use super::router::Router;
use super::typ::*;
use crate::NodeId;
use crate::TypeConfig;

pub struct NetworkFactory {
    router: Router,
}

impl NetworkFactory {
    pub fn new(router: Router) -> Self {
        Self { router }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Connection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        Connection {
            addr: node.addr.clone(),
            router: self.router.clone(), // <-- Клонируем существующий роутер
            target,
        }
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
        let resp = self
            .router
            .send(self.target, self.addr.clone(), "/raft/append", req)
            .await?;
        Ok(resp)
    }

    /// Полная передача снапшота. Учитывает токен отмены `cancel`: если вызывающая
    /// сторона отменила репликацию, длинная передача прерывается и возвращается
    /// `StreamingError::Closed` (V17/C40), как ожидает openraft.
    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse, StreamingError> {
        let send = self.router.send(
            self.target,
            self.addr.clone(),
            "/raft/snapshot",
            (vote, snapshot.meta, snapshot.snapshot),
        );

        tokio::select! {
            resp = send => Ok(resp?),
            closed = cancel => Err(StreamingError::Closed(closed)),
        }
    }

    async fn vote(
        &mut self,
        req: VoteRequest,
        _option: RPCOption,
    ) -> Result<VoteResponse, RPCError> {
        let resp = self
            .router
            .send(self.target, self.addr.clone(), "/raft/vote", req)
            .await?;
        Ok(resp)
    }
}
