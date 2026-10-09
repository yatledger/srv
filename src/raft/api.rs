use axum::{
    extract::{Json, State},
    http::StatusCode,
};

use std::collections::{BTreeMap, BTreeSet};
use tracing::info;

use openraft::BasicNode;
use openraft::error::decompose::DecomposeResult;

use crate::NodeId;
use crate::app::App;
use crate::raft;
use raft::typ::*;
use tracing::error;

/*
async fn add_learner_minimal(
    State(app): State<App>,
    Json((node_id, addr)): Json<(NodeId, String)>,
) -> Result<Json<Value>, StatusCode> {
    let node = BasicNode { addr };
    let res = app.raft.add_learner(node_id, node, true).await.decompose()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::to_value(res).unwrap()))
}
*/
pub async fn add_learner(
    State(app): State<App>,
    Json(req): Json<(NodeId, String)>,
) -> Result<Json<impl serde::Serialize>, StatusCode> {
    info!("{} {}", req.0, req.1);
    let node_id = req.0;
    let node = BasicNode { addr: req.1 };

    let res = app
        .raft
        .add_learner(node_id, node, true)
        .await
        .decompose()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(res))
}

pub async fn change_membership(
    State(app): State<App>,
    Json(req): Json<BTreeSet<NodeId>>,
) -> Result<Json<impl serde::Serialize>, StatusCode> {
    let res = app
        .raft
        .change_membership(req, false)
        .await
        .decompose()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(res))
}

pub async fn init(
    State(app): State<App>,
    body: String,
) -> Result<Json<impl serde::Serialize>, StatusCode> {
    let mut nodes = BTreeMap::new();

    // Пытаемся распарсить, если не получается или пусто - используем дефолт
    let node_list: Vec<(NodeId, String)> = if body.trim().is_empty() {
        Vec::new()
    } else {
        serde_json::from_str(&body).map_err(|_| StatusCode::BAD_REQUEST)?
    };

    if node_list.is_empty() {
        nodes.insert(
            app.id,
            BasicNode {
                addr: app.addr.clone(),
            },
        );
    } else {
        for (id, addr) in node_list.into_iter() {
            nodes.insert(id, BasicNode { addr });
        }
    };

    let res = app
        .raft
        .initialize(nodes)
        .await
        .decompose()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(res))
}

pub async fn metrics(State(app): State<App>) -> Result<Json<impl serde::Serialize>, StatusCode> {
    let metrics = app.raft.metrics().borrow().clone();
    // let res: Result<RaftMetrics<TypeConfig>, Infallible> = Ok(metrics);
    Ok(Json(metrics))
}

pub async fn vote(
    State(app): State<App>,
    Json(req): Json<VoteRequest>,
) -> Result<Json<impl serde::Serialize>, (StatusCode, String)> {
    app.raft.vote(req).await.map(Json).map_err(|e| {
        error!("vote request failed: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })
}

pub async fn append(
    State(app): State<App>,
    Json(req): Json<AppendEntriesRequest>,
) -> Result<Json<impl serde::Serialize>, (StatusCode, String)> {
    app.raft.append_entries(req).await.map(Json).map_err(|e| {
        error!("append_entries request failed: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })
}

pub async fn snapshot(
    State(app): State<App>,
    req: String,
) -> Result<Json<impl serde::Serialize>, (StatusCode, String)> {
    // Проверка на пустой запрос
    if req.is_empty() {
        error!("Empty snapshot request");
        return Err((StatusCode::BAD_REQUEST, "Empty request body".to_string()));
    }

    // Безопасная десериализация
    let (vote, snapshot_meta, snapshot_data): (Vote, SnapshotMeta, SnapshotData) =
        serde_json::from_str(&req).map_err(|e| {
            error!("Failed to deserialize snapshot request: {}", e);
            (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", e))
        })?;

    let snapshot = Snapshot {
        meta: snapshot_meta,
        snapshot: snapshot_data,
    };

    let res = app
        .raft
        .install_full_snapshot(vote, snapshot)
        .await
        .map_err(|e| {
            error!("Failed to install snapshot: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Snapshot installation failed: {}", e),
            )
        })?;

    Ok(Json(res))
}
