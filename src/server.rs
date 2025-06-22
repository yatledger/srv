use axum::{
    extract::{Json, State},
    http::StatusCode,
    routing::{post, get},
    Router,
    body::Bytes,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{info, error, debug};

use rand::seq::SliceRandom;
use rand::rng;

use crate::command::{Request};

use openraft::ReadPolicy;
use crate::api;

use crate::app::{App};
use crate::NodeId;
use openraft::BasicNode;
use openraft::error::decompose::DecomposeResult;
use crate::typ::*;
use crate::decode;

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
async fn add_learner(
    State(app): State<App>,
    Json(req): Json<(NodeId, String)>,
) -> Result<Json<impl serde::Serialize>, StatusCode> {
    let node_id = req.0;
    let node = BasicNode { addr: req.1 };
    
    let res = app.raft.add_learner(node_id, node, true).await.decompose().unwrap()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    
    Ok(Json(res))
}

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;

async fn change_membership(
    State(app): State<App>,
    Json(req): Json<BTreeSet<NodeId>>,
) -> Result<Json<impl serde::Serialize>, StatusCode> {
    let res = app.raft.change_membership(req, false).await.decompose()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(res))
}

async fn init(
    State(app): State<App>,
    Json(req): Json<Vec<(NodeId, String)>>,
) -> Result<Json<impl serde::Serialize>, StatusCode> {
    let mut nodes = BTreeMap::new();
    if req.is_empty() {
        nodes.insert(app.id, BasicNode { addr: app.addr.clone() });
    } else {
        for (id, addr) in req.into_iter() {
            nodes.insert(id, BasicNode { addr });
        }
    };
    let res = app.raft.initialize(nodes).await.decompose()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(res))
}


async fn metrics(
    State(app): State<App>,
) -> Result<Json<impl serde::Serialize>, StatusCode> {
    let metrics = app.raft.metrics().borrow().clone();
    // let res: Result<RaftMetrics<TypeConfig>, Infallible> = Ok(metrics);
    Ok(Json(metrics))
}

async fn write(
    State(app): State<App>,
    Json(req): Json<Request>,
) -> Result<Json<impl serde::Serialize>, StatusCode> {
    let response = app.raft.client_write(req).await.decompose()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(response))
}

async fn vote(
    State(app): State<App>,
    Json(req): Json<VoteRequest>,
) -> Result<Json<impl serde::Serialize>, StatusCode> {
    let res = app.raft.vote(req).await;
    Ok(Json(res))
}

async fn append(
    State(app): State<App>,
    Json(req): Json<AppendEntriesRequest>,
) -> Result<Json<impl serde::Serialize>, StatusCode> {
    let res = app.raft.append_entries(req).await;
    Ok(Json(res))
}

async fn snapshot(
    State(app): State<App>,
    req: String,
) -> Result<Json<impl serde::Serialize>, StatusCode> {
    let (vote, snapshot_meta, snapshot_data): (Vote, SnapshotMeta, SnapshotData) = decode(&req);
    
    let snapshot = Snapshot {
        meta: snapshot_meta,
        snapshot: snapshot_data,
    };
    
    let res = app.raft.install_full_snapshot(vote, snapshot).await;
    
    Ok(Json(res))
}

#[axum_macros::debug_handler]
pub async fn unified_handler(
    State(mut app): State<App>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    body: Bytes,
) -> StatusCode {
    let path = uri.path().to_string();
    let payload = String::from_utf8_lossy(&body).to_string();
    info!("GET {} with {}", path.clone(), payload.clone());

    match path.as_str() {
        "/app/write" => api::write(&mut app, payload).await,
        "/app/read" => api::read(&mut app, payload).await,

        "/raft/append" => api::append(&mut app, payload).await,
        "/raft/snapshot" => api::snapshot(&mut app, payload).await,
        "/raft/vote" => api::vote(&mut app, payload).await,

        "/mng/change-membership" => api::change_membership(&mut app, payload).await,
        "/mng/init" => api::init(&mut app).await,
        "/mng/metrics" => api::metrics(&mut app).await,

        _ => return StatusCode::NOT_FOUND,
    };

    StatusCode::OK
}

#[derive(Deserialize)]
struct AddNodeRequest {
    hash: String,
    parents: Vec<String>,
    data: Value,
}

#[derive(Serialize)]
struct AddNodeResponse {
    status: String,
    message: Option<String>,
}

#[derive(Serialize)]
struct PoolResponse {
    status: String,
    nodes: Vec<String>,
    message: Option<String>,
}

async fn add_handler(
    State(app): State<App>,
    Json(payload): Json<AddNodeRequest>,
) -> (StatusCode, Json<AddNodeResponse>) {
    debug!("Starting add_handler for node: {}", payload.hash);
    // let duration = start.elapsed(); // Вычисляем время выполнения.
    // println!("add_node_handler took {} ms", duration.as_millis());
    let hash = Arc::from(payload.hash.as_str());
    let parents = payload.parents.into_iter().map(|s| Arc::from(s.as_str())).collect();
    let data = Arc::new(payload.data);

    let request = Request::Add { hash, parents, data };

    match app.raft.client_write(request).await {
        Ok(_response) => (
            StatusCode::OK,
            Json(AddNodeResponse {
                status: "success".to_string(),
                message: None,
            }),
        ),
        Err(e) => {
            error!("Failed to write to Raft: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AddNodeResponse {
                    status: "error".to_string(),
                    message: Some(format!("Raft error: {}", e)),
                }),
            )
        }
    }
}

#[axum_macros::debug_handler]
async fn pool_handler(
    State(app): State<App>,
) -> (StatusCode, Json<PoolResponse>) {
    let ret = app.raft.get_read_linearizer(ReadPolicy::ReadIndex).await;
    let mut nodes = vec![];
    match ret {
        Ok(linearizer) => {
            linearizer.await_ready(&app.raft).await.unwrap();

            let state_machine = app.state_machine.state_machine.lock().unwrap();
            nodes = state_machine.dag.get_weights().clone();
            
        }
        Err(e) => {
            // Другие ошибки Raft
            tracing::error!("Unexpected Raft error: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(PoolResponse {
                    status: "error".to_string(),
                    nodes: vec![],
                    message: Some(format!("Raft error: {}", e)),
                }),
            )
        }
    };
    nodes.sort_by(|a, b| a.weight.partial_cmp(&b.weight).unwrap_or(std::cmp::Ordering::Equal));

    // Извлекаем только хэши узлов.
    let mut nodes: Vec<String> = nodes.into_iter().map(|node| String::from(&*node.node)).collect();
    // Выполняем обрезку и перемешивание только если nodes.len() > 10.
    if nodes.len() > 10 {
        // Обрезаем список до nodes.len() / 3.
        let first_truncate_len = nodes.len() / 3;
        nodes.truncate(first_truncate_len);
        // Перемешиваем список узлов.
        nodes.shuffle(&mut rng());
        // Вычисляем длину обрезанного списка как округлённый квадратный корень от числа узлов.
        let target_len = (nodes.len() as f64).sqrt().ceil() as usize;
        // Обрезаем список до target_len, если он длиннее.
        nodes.truncate(target_len);
            }
    (
        StatusCode::OK,
        Json(PoolResponse {
            status: "success".to_string(),
            nodes,
            message: None,
        }),
    )
}



pub async fn start_server(app: App, http_addr: String) -> Result<(), Box<dyn std::error::Error>> {
    let srv = Router::new()
        .route("/change-membership", post(change_membership))
        .route("/init", post(init))
        .route("/metrics", get(metrics))
        .route("/write", post(write))
        .route("/vote", post(vote))
        .route("/append", post(append))
        .route("/snapshot", post(snapshot))
        .route("/mng/add-learner", post(add_learner))
        .route("/mng/init", post(unified_handler))
        .route("/mng/metrics", post(unified_handler))
        .with_state(app.clone()); // Передаём граф как состояние приложения.

    // TcpListener создаёт асинхронный TCP-сокет для обработки входящих соединений.
    let listener = TcpListener::bind(http_addr.clone()).await?;
    info!("Server running at {}", http_addr);

    // Запускаем Axum-сервер, который обрабатывает запросы.
    axum::serve(listener, srv).await?;

    Ok(())
}