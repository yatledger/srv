use axum::{
    extract::{Json, State},
    http::StatusCode,
    routing::{post, get},
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{info, error, debug};

use rand::seq::SliceRandom;
use rand::rng;

use crate::app::{App};
use crate::NodeId;
use openraft::BasicNode;
use openraft::error::decompose::DecomposeResult;
use crate::typ::*;
use crate::decode;
use crate::command::{Request, ComputeWeightsRequest, SubmitWeightsResponse, NodeWeight};

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
    info!("{} {}", req.0, req.1);
    let node_id = req.0;
    let node = BasicNode { addr: req.1 };
    
    let res = app.raft.add_learner(node_id, node, true).await.decompose().unwrap()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    
    Ok(Json(res))
}

use std::collections::{BTreeMap, BTreeSet};

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
        nodes.insert(app.id, BasicNode { addr: app.addr.clone() });
    } else {
        for (id, addr) in node_list.into_iter() {
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

// Новая структура для представления узла с его потомками.
#[derive(Serialize)]
struct NodeFullInfo {
    hash: String, // Хэш узла.
    weight: f64, // Финальный вес узла.
    descendants: Vec<DescendantInfo>, // Список потомков с глубиной и весом.
}

// Новая структура для представления потомка.
#[derive(Serialize)]
struct DescendantInfo {
    hash: String, // Хэш потомка.
    depth: usize, // Глубина относительно родителя.
    weight: f64, // Вес потомка.
}

// Новая структура для ответа /full_graph.
#[derive(Serialize)]
struct FullGraphResponse {
    status: String,
    graph: Vec<NodeFullInfo>,
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
    let state_machine = app.state_machine.state_machine.lock().unwrap();
    let mut nodes = state_machine.dag.get_weights().clone();
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

// Обработчик для запроса пересчета весов (от лидера к follower)
#[axum_macros::debug_handler]
async fn compute_weights_handler(
    State(app): State<App>, // Arc<App> для совместного доступа
    Json(payload): Json<ComputeWeightsRequest>,
) -> (StatusCode, Json<Result<SubmitWeightsResponse, RaftError>>) {
    info!("Starting compute_weights_handler for {} nodes, dag_version: {}", payload.nodes.len(), payload.dag_version);
    
    // Проверяем согласованность версии DAG
    let metrics = app.raft.metrics().borrow().clone(); // Получаем метрики Raft
    let last_log_index = metrics.last_log_index.unwrap_or(0); // Разворачиваем Option<u64>, используя 0 для None
    if payload.dag_version > last_log_index {
        error!("DAG version mismatch: received {}, local {:?}", payload.dag_version, metrics.last_log_index);
        return (
            StatusCode::PRECONDITION_FAILED,
            Json(Ok(SubmitWeightsResponse {
                node_weights: Vec::new(), // Пустой список весов при несоответствии версии
                dag_version: last_log_index,
                message: Some(format!(
                    "DAG version mismatch: received {}, local {:?}", 
                    payload.dag_version, metrics.last_log_index
                )),
            })),
        );
    }

    // Блокируем state_machine для доступа к DAG
    let state_machine = app.state_machine.state_machine.lock().unwrap();
    
    // Пересчитываем веса для указанных узлов
    let weights_map = state_machine.dag.compute_weights_for_batch(&payload.nodes);
    
    // Формируем ответ с вычисленными весами
    let node_weights = weights_map
        .into_iter()
        .map(|(node, weight)| NodeWeight { node, weight })
        .collect();

    (
        StatusCode::OK,
        Json(Ok(SubmitWeightsResponse {
            node_weights,
            dag_version: payload.dag_version,
            message: None, // Нет ошибки
        })),
    )
}

async fn get_full_graph_handler(
    State(app): State<App>,
) -> (StatusCode, Json<FullGraphResponse>) {
    let state_machine = app.state_machine.state_machine.lock().unwrap();
    let mut nodes = state_machine.dag.get_weights().clone();
    nodes.sort_by(|a, b| {
        b.weight.partial_cmp(&a.weight)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.node.cmp(&b.node))
    });
    // Получаем потомков для всех узлов заранее, чтобы избежать повторных вычислений.
    let descendants_map = state_machine.dag.compute_descendants_with_depth_and_weight();

    // Собираем информацию о каждом узле: хэш, вес, потомки.
    let graph = nodes
        .into_iter()
        .map(|node| {
            // Получаем потомков узла из descendants_map.
            let descendants = descendants_map
                .get(&node.node)
                .unwrap_or(&Vec::new()) // Если нет потомков, возвращаем пустой вектор.
                .iter()
                .map(|descendant| DescendantInfo {
                    hash: String::from(&*descendant.node),
                    depth: descendant.depth,
                    weight: descendant.weight,
                })
                .collect::<Vec<DescendantInfo>>();

            NodeFullInfo {
                hash: String::from(&*node.node),
                weight: node.weight,
                descendants,
            }
        })
        .collect::<Vec<NodeFullInfo>>();

    (
        StatusCode::OK,
        Json(FullGraphResponse {
            status: "success".to_string(),
            graph,
            message: None,
        }),
    )
}



pub async fn start_server(app: App, _addr: String, port: String) -> Result<(), Box<dyn std::error::Error>> {
    let srv = Router::new()
        .route("/add", post(add_handler))
        .route("/pool", get(pool_handler))
        .route("/full", get(get_full_graph_handler))
        .route("/compute_weights", post(compute_weights_handler))
        .route("/write", post(write))
        .route("/raft/vote", post(vote))
        .route("/raft/append", post(append))
        .route("/raft/snapshot", post(snapshot))
        .route("/mng/change-membership", post(change_membership))
        .route("/mng/add-learner", post(add_learner))
        .route("/mng/init", post(init))
        .route("/mng/metrics", post(metrics))
        .with_state(app.clone());

    let http_addr = "0.0.0.0:".to_string() + &port;
    let listener = TcpListener::bind(http_addr.clone()).await?;
    info!("Server running at {}", http_addr);

    axum::serve(listener, srv).await?;

    Ok(())
}