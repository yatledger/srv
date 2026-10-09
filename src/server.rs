//! HTTP-сервер: маршрутизация публичного и внутреннего API, хендлеры.
//!
//! Транзакционная логика вынесена в [`crate::tx_logic`], очистка — в
//! [`crate::cleanup`]. Здесь остаются только схемы запросов/ответов и
//! привязка к axum.

use axum::{
    Router,
    extract::{DefaultBodyLimit, Json, State},
    http::StatusCode,
    routing::{get, post},
};

use std::fs;

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{debug, error, info};

use crate::Tx;
use crate::app::App;
use crate::raft;
use crate::tx_logic::{self, PrepareError, TxRead};
use raft::api::*;
use raft::command::Request;

use crate::graph::weights::NodeDepth;
use crate::web::ApiResponse;

/// Стандартный ответ внутреннего API.
#[derive(Deserialize, Serialize, Debug)]
pub struct StandardResponse {
    /// Статус операции.
    pub status: String,
    /// Необязательное пояснение.
    pub message: Option<String>,
}

/// Переводит доменную ошибку подготовки в HTTP-ответ.
fn prepare_error_response(err: PrepareError) -> (StatusCode, Json<ApiResponse<StandardResponse>>) {
    let status = match err {
        PrepareError::Invalid(_) | PrepareError::Conflict(_) => StatusCode::BAD_REQUEST,
        PrepareError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (
        status,
        Json(ApiResponse::Error {
            status: "error".to_string(),
            message: err.message().to_string(),
        }),
    )
}

/// Готовит транзакцию к записи в Raft, используя доменный слой `tx_logic`.
///
/// Проверки, зависящие от состояния (существование узла и родителей), здесь
/// выполняются лишь как **ранний отказ** по текущему снимку реплики. Истина —
/// детерминированная валидация в state machine (`raft::store::validate_add`),
/// которая применяется ко всем репликам в порядке лога. Поэтому возможная
/// гонка «узел/родитель появился между проверкой и применением» (V4) не может
/// привести к расхождению состояния: невалидная команда отвергается при apply.
async fn validate_and_prepare_tx(
    app: &App,
    payload: &TxRead,
) -> Result<Arc<str>, (StatusCode, Json<ApiResponse<StandardResponse>>)> {
    let hash = tx_logic::validate_structure(payload).map_err(prepare_error_response)?;

    let state_machine = app.state_machine.state_machine.read().await;
    let tx_hash = tx_logic::hash_to_hex(&hash);
    tx_logic::validate_against_state(&state_machine.dag, &tx_hash, &payload.tx.prnts)
        .map_err(prepare_error_response)?;

    Ok(tx_hash)
}

#[derive(Deserialize, Serialize)]
struct InternalAddRequest {
    hash: Arc<str>,
    tx: Tx,
    sign: String,
    func: String,
}

#[axum_macros::debug_handler]
async fn add_tx(
    State(app): State<App>,
    Json(payload): Json<TxRead>,
) -> (StatusCode, Json<ApiResponse<StandardResponse>>) {
    let tx_hash = match validate_and_prepare_tx(&app, &payload).await {
        Ok(hash) => hash,
        Err(response) => return response,
    };

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

    let internal_payload = InternalAddRequest {
        hash: tx_hash,
        tx: payload.tx,
        sign: payload.sign,
        func: payload.func,
    };

    if leader_id == app.id {
        return add_handler(State(app), Json(internal_payload)).await;
    }

    let leader_addr = match metrics
        .membership_config
        .nodes()
        .find(|(id, _)| **id == leader_id)
        .map(|(_, node)| node.addr.clone())
    {
        Some(addr) => addr,
        None => {
            error!(
                "Leader {} is not present in membership config on node {}",
                leader_id, app.id
            );
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiResponse::Error {
                    status: "error".to_string(),
                    message: "Leader address is unknown".to_string(),
                }),
            );
        }
    };

    // Перенаправляем запрос лидеру
    match app
        .router
        .send::<_, StandardResponse>(&leader_addr, "/add", internal_payload)
        .await
    {
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
    Json(payload): Json<InternalAddRequest>,
) -> (StatusCode, Json<ApiResponse<StandardResponse>>) {
    // Внутренний эндпоинт обязан проходить те же проверки, что и публичный `/`:
    // подпись, покрытие func/var, существование и уникальность родителей.
    let tx_read = TxRead {
        tx: payload.tx.clone(),
        sign: payload.sign.clone(),
        func: payload.func.clone(),
    };

    let computed_hash = match validate_and_prepare_tx(&app, &tx_read).await {
        Ok(hash) => hash,
        Err(response) => return response,
    };

    // Хэш, переданный внутренним вызывающим, обязан совпадать с вычисленным.
    if computed_hash != payload.hash {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::Error {
                status: "error".to_string(),
                message: "Hash does not match transaction content".to_string(),
            }),
        );
    }

    let request = Request::Add {
        hash: payload.hash,
        tx: payload.tx,
        sign: payload.sign,
        func: payload.func,
    };

    match app.raft.client_write(request).await {
        Ok(_response) => (
            StatusCode::OK,
            Json(ApiResponse::Success(StandardResponse {
                status: "success".to_string(),
                message: None,
            })),
        ),
        Err(e) => {
            error!("Failed to write to Raft: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::Error {
                    status: "error".to_string(),
                    message: format!("Raft error: {}", e),
                }),
            )
        }
    }
}

async fn pool_handler(State(app): State<App>) -> (StatusCode, Json<PoolResponse>) {
    let state_machine = app.state_machine.state_machine.read().await;

    // Получаем список смежности родителей через метод get_parents
    let parents_map = state_machine.dag.get_parents();
    // Собираем узлы и подсчитываем количество активных родителей для каждого
    let mut nodes = state_machine
        .dag
        .get_node_keys()
        .into_iter()
        .map(|node| {
            let active_parents = parents_map
                .get(&node)
                .map(|parents| parents.len())
                .unwrap_or(0);
            (node, active_parents)
        })
        .collect::<Vec<_>>();
    // Сортируем по возрастанию числа активных родителей (меньше родителей — выше приоритет)
    nodes.sort_by(|a, b| a.1.cmp(&b.1));
    // Преобразуем в Vec<String> для ответа
    let mut nodes: Vec<String> = nodes
        .into_iter()
        .map(|(node, _)| String::from(&*node))
        .collect();

    if nodes.len() > 10 {
        let target_len = (nodes.len() as f64).sqrt().ceil() as usize;
        nodes.truncate(target_len);
    } else {
        nodes.truncate(2);
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
) -> (StatusCode, Json<ApiResponse<StandardResponse>>) {
    // Проверяем, является ли текущий узел лидером
    if app.raft.metrics().borrow().current_leader != Some(app.id) {
        error!("A non-leader node received a request to /remove_heavy_nodes");
        return (
            StatusCode::FORBIDDEN,
            Json(ApiResponse::Error {
                status: "error".to_string(),
                message: "Only the leader can process this request.".to_string(),
            }),
        );
    }

    // Проверяем, что список узлов не пустой
    if payload.nodes.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::Error {
                status: "error".to_string(),
                message: "Node list cannot be empty.".to_string(),
            }),
        );
    }

    match crate::cleanup::archive_and_remove(&app, payload.nodes).await {
        Ok(removed) => (
            StatusCode::OK,
            Json(ApiResponse::Success(StandardResponse {
                status: "success".to_string(),
                message: Some(format!("Removed {removed} node(s)")),
            })),
        ),
        Err(e) => {
            error!("Failed to remove heavy nodes: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::Error {
                    status: "error".to_string(),
                    message: e,
                }),
            )
        }
    }
}

async fn get_full_graph_handler(State(app): State<App>) -> (StatusCode, Json<FullGraphResponse>) {
    let state_machine = app.state_machine.state_machine.read().await;
    let nodes = state_machine.dag.get_nodes_by_depth();
    (
        StatusCode::OK,
        Json(FullGraphResponse {
            status: "success".to_string(),
            nodes,
            message: None,
        }),
    )
}

// Структура для десериализации JSON с генезис-транзакциями.
#[derive(Deserialize)]
struct GenesisTransaction {
    hash: String, // Хэш генезис-узла.
    data: TxRead,
}
/// Хендлер загрузки генезис-транзакций из `genesis.json`.
pub async fn load_genesis(
    State(app): State<App>,
) -> (StatusCode, Json<ApiResponse<StandardResponse>>) {
    // Загрузка генезис-транзакций
    // Читаем genesis.json
    let genesis_content = match fs::read_to_string("genesis.json") {
        Ok(content) => content,
        Err(e) => {
            error!("Could not read genesis.json: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::Error {
                    status: "error".to_string(),
                    message: format!("Failed to read genesis.json: {}", e),
                }),
            );
        }
    };
    // Десериализуем транзакции
    let transactions: Vec<GenesisTransaction> = match serde_json::from_str(&genesis_content) {
        Ok(trans) => trans,
        Err(e) => {
            error!("Failed to parse genesis.json: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::Error {
                    status: "error".to_string(),
                    message: format!("Failed to parse genesis.json: {}", e),
                }),
            );
        }
    };
    // Обрабатываем каждую транзакцию
    let mut errors = Vec::new();
    for tx in &transactions {
        let request = Request::Add {
            hash: Arc::from(tx.hash.as_str()),
            tx: tx.data.tx.clone(),
            sign: tx.data.sign.clone(),
            func: tx.data.func.clone(),
        };

        match app.raft.client_write(request).await {
            Ok(_) => {
                debug!(
                    "Successfully added genesis transaction with hash {}",
                    tx.hash
                );
            }
            Err(e) => {
                error!(
                    "Failed to write genesis transaction {} to Raft: {}",
                    tx.hash, e
                );
                errors.push(format!("Failed to add transaction {}: {}", tx.hash, e));
            }
        }
    }
    // Проверяем, были ли ошибки
    if errors.is_empty() {
        info!(
            "Successfully loaded {} genesis transactions",
            transactions.len()
        );
        (
            StatusCode::OK,
            Json(ApiResponse::Success(StandardResponse {
                status: "success".to_string(),
                message: Some(format!(
                    "Loaded {} genesis transactions",
                    transactions.len()
                )),
            })),
        )
    } else {
        error!(
            "Errors occurred while loading genesis transactions: {:?}",
            errors
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::Error {
                status: "error".to_string(),
                message: format!("Failed to load some transactions: {:?}", errors),
            }),
        )
    }
}

#[derive(Serialize)]
struct PoolResponse {
    status: String,
    nodes: Vec<String>,
    message: Option<String>,
}

#[derive(Serialize)]
struct FullGraphResponse {
    status: String,
    nodes: Vec<NodeDepth>,
    message: Option<String>,
}

/// Запрос на архивацию и удаление «тяжёлых» узлов.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct HeavyNodesRequest {
    /// Хэши узлов-кандидатов.
    pub nodes: Vec<Arc<str>>,
}

/// Запускает HTTP-сервер: собирает роутеры и слушает `bind_addr`.
pub async fn start_server(app: App, bind_addr: String) -> Result<(), Box<dyn std::error::Error>> {
    // Публичный API: доступен без аутентификации.
    let public = Router::new()
        .route("/", post(add_tx))
        .route("/pool", get(pool_handler))
        .route("/full", get(get_full_graph_handler));

    // Внутренний API: только с кластерным токеном.
    let internal = Router::new()
        .route("/add", post(add_handler))
        .route("/remove_heavy_nodes", post(remove_heavy_nodes_handler))
        .route("/load-genesis", post(load_genesis))
        .route("/raft/vote", post(vote))
        .route("/raft/append", post(append))
        .route("/raft/snapshot", post(snapshot))
        .route("/mng/change-membership", post(change_membership))
        .route("/mng/add-learner", post(add_learner))
        .route("/mng/init", post(init))
        .route("/mng/metrics", post(metrics))
        .route_layer(axum::middleware::from_fn_with_state(
            app.clone(),
            crate::auth::require_internal_token,
        ));

    let srv = public
        .merge(internal)
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024))
        .with_state(app.clone());

    let listener = TcpListener::bind(&bind_addr).await?;
    info!("Server running at {}", bind_addr);

    axum::serve(listener, srv).await?;

    Ok(())
}
