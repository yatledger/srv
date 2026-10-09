use axum::{
    Router,
    extract::{DefaultBodyLimit, Json, State},
    http::StatusCode,
    routing::{get, post},
};

use std::fs; // Для чтения файла.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{debug, error, info};

use redis::pipe;

use crate::Tx;
use crate::app::App;
use crate::raft;
use crate::utils::*;
use raft::api::*;
use raft::command::Request;

use crate::graph::dag::extract_from_var_struct;
use crate::graph::weights::NodeDepth;
use crate::web::ApiResponse;

use base58::FromBase58;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use hex::FromHex;

#[derive(Deserialize, Serialize, Debug)]
pub struct StandardResponse {
    pub status: String,
    pub message: Option<String>,
}

async fn validate_and_prepare_tx(
    app: &App,
    payload: &TxRead,
) -> Result<Arc<str>, (StatusCode, Json<ApiResponse<StandardResponse>>)> {
    // Проверка родителей
    if let Err(err) = validate_parents(&payload.tx.prnts) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::Error {
                status: "error".to_string(),
                message: err,
            }),
        ));
    }

    // Проверка допустимости функции: func входит в подписываемый контент.
    if let Err(err) = validate_func(&payload.func) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::Error {
                status: "error".to_string(),
                message: err,
            }),
        ));
    }

    // Структура var валидируется до попадания в state machine (client_write).
    if payload.func == "transferToken" {
        match extract_from_var_struct(&payload.tx).and_then(|var| var.validate()) {
            Ok(()) => {}
            Err(err) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(ApiResponse::Error {
                        status: "error".to_string(),
                        message: err,
                    }),
                ));
            }
        }
    }

    // Проверка подписи и вычисление хэша
    let tx_hash_bytes = match ordered_sum(&payload.tx, &payload.func) {
        Ok(bytes) => bytes,
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::Error {
                    status: "error".to_string(),
                    message: format!("Ordered sum error: {}", e),
                }),
            ));
        }
    };

    // Проверка самой подписи
    let signature_bytes = match <[u8; 64]>::from_hex(payload.sign.as_str()) {
        Ok(bytes) => bytes,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::Error {
                    status: "error".to_string(),
                    message: "Invalid hex for signature".to_string(),
                }),
            ));
        }
    };

    let signature = match Signature::try_from(signature_bytes.as_ref()) {
        Ok(sig) => sig,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::Error {
                    status: "error".to_string(),
                    message: "Invalid signature format".to_string(),
                }),
            ));
        }
    };

    let addr_bytes = match payload.tx.addr.from_base58() {
        Ok(bytes) => bytes,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::Error {
                    status: "error".to_string(),
                    message: "Invalid base58 in signature verification".to_string(),
                }),
            ));
        }
    };

    let addr_array: [u8; 32] = match addr_bytes.try_into() {
        Ok(arr) => arr,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::Error {
                    status: "error".to_string(),
                    message: "Invalid Ed25519 key length".to_string(),
                }),
            ));
        }
    };

    let verify_key = match VerifyingKey::from_bytes(&addr_array) {
        Ok(key) => key,
        Err(_) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::Error {
                    status: "error".to_string(),
                    message: "Invalid Ed25519 public key".to_string(),
                }),
            ));
        }
    };

    if verify_key
        .verify(tx_hash_bytes.as_bytes(), &signature)
        .is_err()
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::Error {
                status: "error".to_string(),
                message: "Signature verification failed".to_string(),
            }),
        ));
    }

    let tx_hash = Arc::from(tx_hash_bytes.to_hex().to_string());

    // Блокируем state machine для проверок, зависящих от состояния (существование узлов)
    let state_machine = app.state_machine.state_machine.read().await;

    // Проверяем, что узел еще не существует
    if state_machine.dag.contains_node(&tx_hash) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::Error {
                status: "error".to_string(),
                message: "Node already exists".to_string(),
            }),
        ));
    }

    // Проверяем, что каждый родитель существует
    for parent in &payload.tx.prnts {
        if !state_machine.dag.contains_node(parent) && !state_machine.dag.is_node_added(parent) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::Error {
                    status: "error".to_string(),
                    message: format!("Parent {} does not exist", parent),
                }),
            ));
        }
    }

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

    let leader_addr = metrics
        .membership_config
        .nodes()
        .find(|(id, _)| **id == leader_id)
        .map(|(_, node)| node.addr.clone())
        .expect("NO LEADER ADDR");

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

    /*
    // Получаем все узлы с их глубинами, уже отсортированные по глубине
    let nodes_with_time = state_machine.dag.get_time();

    // Преобразуем в Vec<String> для ответа (узлы уже отсортированы по глубине)
    let mut nodes: Vec<String> = nodes_with_time
        .into_iter()
        .map(|node_time| String::from(&*node_time.node))
        .collect();
    */
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

    // Клонируем Redis соединение
    let mut redis = app.redis.clone();
    // Формируем список узлов для удаления, исключая те, что уже есть в added
    let mut nodes_to_remove: Vec<Arc<str>> = Vec::new();
    {
        // Блокируем state_machine для проверки поля added
        let state_machine = app.state_machine.state_machine.read().await;
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

    // TODO подумать как правиль ошибку форматировать. Если нет узлов для удаления, возвращаем успех
    if nodes_to_remove.is_empty() {
        return (
            StatusCode::OK,
            Json(ApiResponse::Error {
                status: "success".to_string(),
                message: "No new nodes to remove.".to_string(),
            }),
        );
    }

    // Создаем пайплайн для записи в Redis
    let mut redis_pipe = pipe();
    {
        // Блокируем state_machine для получения данных узлов
        let state_machine = app.state_machine.state_machine.read().await;
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

    info!("{} / {}", payload.nodes.len(), nodes_to_remove.len());

    // Выполняем пакетную запись в Redis
    let redis_result: redis::RedisResult<()> = redis_pipe.query_async(&mut redis).await;
    match redis_result {
        Ok(_) => {
            // Формируем запрос для Raft
            let request = Request::Remove {
                nodes: nodes_to_remove,
            };
            // Отправляем команду на удаление в Raft
            match app.raft.client_write(request).await {
                Ok(_response) => (
                    StatusCode::OK,
                    Json(ApiResponse::Success(StandardResponse {
                        status: "success".to_string(),
                        message: None,
                    })),
                ),
                Err(e) => {
                    error!("Failed to write RemoveNodes to Raft: {}", e);
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
        Err(e) => {
            error!(
                "Failed to write batch to Redis: {}. Nodes will not be removed.",
                e
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::Error {
                    status: "error".to_string(),
                    message: format!("Redis error: {}", e),
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

#[derive(Deserialize, Serialize)]
struct TxRead {
    tx: Tx,
    sign: String,
    func: String,
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

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct HeavyNodesRequest {
    pub nodes: Vec<Arc<str>>,
}

pub async fn start_server(app: App, bind_addr: String) -> Result<(), Box<dyn std::error::Error>> {
    let srv = Router::new()
        .route("/", post(add_tx))
        .route("/add", post(add_handler))
        .route("/remove_heavy_nodes", post(remove_heavy_nodes_handler))
        .route("/pool", get(pool_handler))
        .route("/full", get(get_full_graph_handler))
        .route("/load-genesis", post(load_genesis))
        .route("/raft/vote", post(vote))
        .route("/raft/append", post(append))
        .route("/raft/snapshot", post(snapshot))
        .route("/mng/change-membership", post(change_membership))
        .route("/mng/add-learner", post(add_learner))
        .route("/mng/init", post(init))
        .route("/mng/metrics", post(metrics))
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024))
        .with_state(app.clone());

    let listener = TcpListener::bind(&bind_addr).await?;
    info!("Server running at {}", bind_addr);

    axum::serve(listener, srv).await?;

    Ok(())
}
