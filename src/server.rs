use axum::{
    extract::{Json, State},
    http::StatusCode,
    routing::{post, get},
    Router,
};

use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::{info, error, debug};
use rand::seq::SliceRandom;
use rand::rng;

use redis::pipe;

use crate::Tx;
use crate::raft;
use crate::utils::*;
use crate::app::App;
use raft::typ::*;
use raft::api::*;
use raft::command::{Request};

use crate::web::{ApiResponse};

#[axum_macros::debug_handler]
async fn add_tx(
    State(app): State<App>,
    Json(payload): Json<TxRead>,
) -> (StatusCode, Json<ApiResponse<AddTxResponse>>) {

    if let Err(err) = validate_parents(&payload.tx.prnts) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::Error {
                status: "error".to_string(),
                message: err,
            }),
        );
    }

    let metrics = app.raft.metrics().borrow().clone();

    let leader_id = match metrics.current_leader {
        Some(id) => id,
        None => {
            error!("No leader found for node {}", app.id);
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::Error {
                    status: "error".to_string(),
                    message: "No leader available".to_string(),
                }),
            );
        }
    };

    let leader_addr = metrics
        .membership_config
        .nodes()
        .find(|(id, _)| **id == leader_id)
        .map(|(_, node)| node.addr.clone())
        .expect("NO LEADER ADDR");

    // Перенаправляем запрос лидеру
    match app.router.send::<_, AddTxResponse>(&leader_addr, "/add", payload).await {
        Ok(response) => {
            debug!("Successfully forwarded to leader at {}", leader_addr);
            (StatusCode::OK, Json(ApiResponse::Success(response)))
        }
        Err(e) => {
            //TODO сделать ошибки как в processor
            error!("Failed to forward request to leader: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::Error {
                    status: "error".to_string(),
                    message: "Failed to contact leader".to_string(),
                }),
            )
        }
    }

}

#[axum_macros::debug_handler]
async fn add_handler(
    State(app): State<App>,
    Json(payload): Json<TxRead>,
) -> (StatusCode, Json<Result<AddTxResponse, RaftError>>) {

    let tx = payload.tx;
    let sign = payload.sign;
    let func = payload.func;

    let request = Request::Add { tx, sign, func };

    match app.raft.client_write(request).await {
        Ok(_response) => (
            StatusCode::OK,
            Json(Ok(AddTxResponse {
                status: "success".to_string(),
                message: None,
            })),
        ),
        Err(e) => {
            error!("Failed to write to Raft: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(Ok(AddTxResponse {
                    status: "error".to_string(),
                    message: Some(format!("Raft error: {}", e)),
                })),
            )
        }
    }
}

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

#[axum_macros::debug_handler]
async fn remove_heavy_nodes_handler(
    State(app): State<App>,
    Json(payload): Json<HeavyNodesRequest>,
) -> (StatusCode, Json<HeavyNodesResponse>) {
    // Проверяем, является ли текущий узел лидером
    if app.raft.metrics().borrow().current_leader != Some(app.id) {
        error!("A non-leader node received a request to /remove_heavy_nodes");
        return (
            StatusCode::FORBIDDEN,
            Json(HeavyNodesResponse {
                status: "error".to_string(),
                message: Some("Only the leader can process this request.".to_string()),
            }),
        );
    }

    // Проверяем, что список узлов не пустой
    if payload.nodes.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(HeavyNodesResponse {
                status: "error".to_string(),
                message: Some("Node list cannot be empty.".to_string()),
            }),
        );
    }

    info!("Leader received a request to remove {} heavy nodes.", payload.nodes.len());

    // Клонируем Redis соединение
    let mut redis = app.redis.clone();
    // Формируем список узлов для удаления, исключая те, что уже есть в added
    let mut nodes_to_remove: Vec<Arc<str>> = Vec::new();
    {
        // Блокируем state_machine для проверки поля added
        let state_machine = app.state_machine.state_machine.lock().unwrap();
        for node in payload.nodes.iter() {
            // Проверяем, отсутствует ли узел в added
            if !state_machine.dag.is_node_added(node) {
                nodes_to_remove.push(node.clone());
                debug!("Node {} not in added, marked for removal.", node);
            } else {
                debug!("Node {} already in added, skipping.", node);
            }
        }
    } // MutexGuard освобождается здесь

    // Если нет узлов для удаления, возвращаем успех
    if nodes_to_remove.is_empty() {
        return (
            StatusCode::OK,
            Json(HeavyNodesResponse {
                status: "success".to_string(),
                message: Some("No new nodes to remove.".to_string()),
            }),
        );
    }

    // Создаем пайплайн для записи в Redis
    let mut redis_pipe = pipe();
    {
        // Блокируем state_machine для получения данных узлов
        let state_machine = app.state_machine.state_machine.lock().unwrap();
        for node in &nodes_to_remove {
            // Получаем данные узла из DAG
            if let Some(node_data) = state_machine.dag.get_node_data(node) {
                // Сериализуем данные узла в JSON
                let node_data_json = match serde_json::to_string(&node_data) {
                    Ok(json) => json,
                    Err(e) => {
                        error!("Failed to serialize node {} data: {}", node, e);
                        continue; // Пропускаем узел при ошибке сериализации
                    }
                };

                // Формируем ключ для Redis
                let redis_key = format!("confirmed:{}", node.as_ref());
                // Добавляем команду в пайплайн для записи узла и его данных
                redis_pipe.set(redis_key, node_data_json).ignore();
                debug!("Node {} added to Redis pipeline with its data.", node);
            } else {
                debug!("No data found for node {}, skipping Redis write.", node);
            }
        }
    } // MutexGuard освобождается здесь

    // Выполняем пакетную запись в Redis
    let redis_result: redis::RedisResult<()> = redis_pipe.query_async(&mut redis).await;
    match redis_result {
        Ok(_) => {
            info!("{} heavy nodes will be removed.", nodes_to_remove.len());
            // Формируем запрос для Raft
            let request = Request::Remove { nodes: nodes_to_remove };
            // Отправляем команду на удаление в Raft
            match app.raft.client_write(request).await {
                Ok(_response) => (
                    StatusCode::OK,
                    Json(HeavyNodesResponse {
                        status: "success".to_string(),
                        message: None,
                    }),
                ),
                Err(e) => {
                    error!("Failed to write RemoveNodes to Raft: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(HeavyNodesResponse {
                            status: "error".to_string(),
                            message: Some(format!("Raft error: {}", e)),
                        }),
                    )
                }
            }
        }
        Err(e) => {
            error!("Failed to write batch to Redis: {}. Nodes will not be removed.", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(HeavyNodesResponse {
                    status: "error".to_string(),
                    message: Some(format!("Redis error: {}", e)),
                }),
            )
        }
    }
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

#[derive(Deserialize, Serialize)]
struct TxRead {
    tx: Tx,
    sign: String,
    func: String,
}

#[derive(Deserialize, Serialize)]
struct AddTxResponse {
    status: String,
    message: Option<String>,
}

#[derive(Serialize)]
struct PoolResponse {
    status: String,
    nodes: Vec<String>,
    message: Option<String>,
}

#[derive(Serialize)]
struct NodeFullInfo {
    hash: String, // Хэш узла.
    weight: f64, // Финальный вес узла.
    descendants: Vec<DescendantInfo>, // Список потомков с глубиной и весом.
}

#[derive(Serialize)]
struct DescendantInfo {
    hash: String, // Хэш потомка.
    depth: usize, // Глубина относительно родителя.
    weight: f64, // Вес потомка.
}

#[derive(Serialize)]
struct FullGraphResponse {
    status: String,
    graph: Vec<NodeFullInfo>,
    message: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct HeavyNodesRequest {
    pub nodes: Vec<Arc<str>>,
}

// NEW: Response for the heavy nodes submission.
// `pub` allows it to be imported by `processor.rs`.
#[derive(Deserialize, Serialize, Debug)]
pub struct HeavyNodesResponse {
    pub status: String,
    pub message: Option<String>,
}

pub async fn start_server(app: App, _addr: String, port: String) -> Result<(), Box<dyn std::error::Error>> {
    let srv = Router::new()
        .route("/", post(add_tx))
        .route("/add", post(add_handler))
        .route("/remove_heavy_nodes", post(remove_heavy_nodes_handler))

        .route("/pool", get(pool_handler))
        .route("/full", get(get_full_graph_handler))
        
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