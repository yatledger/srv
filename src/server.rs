use axum::{
    extract::{Json, State},
    http::StatusCode,
    routing::{post, get},
    Router,
};

use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::{info, error};
use rand::seq::SliceRandom;
use rand::rng;

use crate::Tx;
use crate::raft;
use crate::utils::*;
use raft::app::{App};
use raft::typ::*;
use raft::api::*;
use raft::command::{Request, ComputeWeightsRequest, SubmitWeightsResponse, NodeWeight};

#[axum_macros::debug_handler]
async fn add_tx(
    State(app): State<App>,
    Json(payload): Json<TxRead>,
) -> (StatusCode, Json<AddTxResponse>) {
    //let parents = payload.parents.into_iter().map(|s| Arc::from(s.as_str())).collect();
    //let data = Arc::new(payload.data);

    // Validate parents
    if let Err(err) = validate_parents(&payload.tx.prnts) {
        return (
            StatusCode::BAD_REQUEST,
            Json(AddTxResponse {
                status: "error".to_string(),
                message: Some(err),
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
                Json(AddTxResponse {
                    status: "error".to_string(),
                    message: Some("No leader available".to_string()),
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
    match app.router.send::<_, AddTxResponse>(leader_id, leader_addr.clone(), "/add", payload).await {
        Ok(response) => {
            info!("Sent to leader at {}", leader_addr);
            (StatusCode::OK, Json(response))
        }
        Err(e) => {
            error!("Failed to forward request to leader {}: {}", leader_addr, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AddTxResponse {
                    status: "error".to_string(),
                    message: Some(format!("Failed to contact leader: {}", e)),
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

// Обработчик для запроса пересчета весов (от лидера к follower)
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

pub async fn start_server(app: App, _addr: String, port: String) -> Result<(), Box<dyn std::error::Error>> {
    let srv = Router::new()
        .route("/", post(add_tx))
        .route("/add", post(add_handler))
        .route("/compute_weights", post(compute_weights_handler))

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