//! HTTP-сервер: маршрутизация публичного и внутреннего API, хендлеры.
//!
//! Транзакционная логика вынесена в [`crate::tx_logic`], очистка — в
//! [`crate::cleanup`]. Здесь остаются только схемы запросов/ответов и
//! привязка к axum.

use axum::{
    Json, Router,
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{HeaderValue, StatusCode, header},
    middleware::Next,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};

use std::fs;
use std::net::SocketAddr;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::{Instrument, debug, error, info};

use crate::Tx;
use crate::app::App;
use crate::audit::{self, AuditSource};
use crate::domain::{Func, Hash};
use crate::raft;
use crate::tx_logic::{self, PrepareError, TxRead};
use raft::api::*;
use raft::command::Request;

use crate::graph::weights::NodeDepth;
use crate::web::ApiResponse;

/// Заголовок корреляционного идентификатора запроса (O2).
pub const REQUEST_ID_HEADER: &str = "x-request-id";

/// Сквозной идентификатор запроса, назначаемый middleware.
fn next_request_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("req-{seq:016x}")
}

/// Middleware наблюдаемости (O2): назначает корреляционный id, ведёт span и
/// считает HTTP-метрики (длительность и статус).
async fn observability_middleware(
    State(app): State<App>,
    mut request: axum::extract::Request,
    next: Next,
) -> Response {
    let request_id = request
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .unwrap_or_else(next_request_id);

    if let Ok(value) = HeaderValue::from_str(&request_id) {
        request.headers_mut().insert(REQUEST_ID_HEADER, value);
    }

    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let span = tracing::info_span!(
        "http_request",
        request_id = %request_id,
        method = %method,
        path = %path,
    );

    let started = Instant::now();
    let mut response = next.run(request).instrument(span).await;
    let elapsed = started.elapsed();

    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert(REQUEST_ID_HEADER, value);
    }

    app.metrics.observe_http(
        &method,
        &path,
        response.status().as_u16(),
        elapsed.as_secs_f64(),
    );
    response
}

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
) -> Result<(Hash, Func), (StatusCode, Json<ApiResponse<StandardResponse>>)> {
    let (hash, func) = tx_logic::validate_structure(payload).map_err(prepare_error_response)?;

    let state_machine = app.state_machine.state_machine.read().await;
    let tx_hash = tx_logic::hash_to_hex(&hash);
    tx_logic::validate_against_state(&state_machine.dag, &tx_hash, &payload.tx.prnts)
        .map_err(prepare_error_response)?;

    Ok((tx_hash, func))
}

#[derive(Deserialize, Serialize)]
struct InternalAddRequest {
    hash: Hash,
    tx: Tx,
    sign: String,
    func: Func,
}

#[axum_macros::debug_handler]
async fn add_tx(
    State(app): State<App>,
    Json(payload): Json<TxRead>,
) -> (StatusCode, Json<ApiResponse<StandardResponse>>) {
    let (tx_hash, func) = match validate_and_prepare_tx(&app, &payload).await {
        Ok(prepared) => prepared,
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
        func,
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
            // Лидер мог отклонить транзакцию прикладной проверкой (400): сохраняем
            // клиентский статус, не маскируя его транспортной 500-й ошибкой.
            if let crate::web::ApiRouterError::Http { status, .. } = &e
                && status.is_client_error()
            {
                return (
                    *status,
                    Json(ApiResponse::Error {
                        status: "error".to_string(),
                        message: e.to_string(),
                    }),
                );
            }
            error!("Failed to forward request to leader: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::Error {
                    status: "error".to_string(),
                    message: format!("Failed to contact leader: {e}"),
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
        func: payload.func.to_string(),
    };

    let (computed_hash, func) = match validate_and_prepare_tx(&app, &tx_read).await {
        Ok(prepared) => prepared,
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
        hash: payload.hash.clone(),
        tx: payload.tx,
        sign: payload.sign,
        func,
    };

    match app.raft.client_write(request).await {
        Ok(response) => {
            // Прикладной отказ state machine (уникальность узла/родителя) не
            // является ошибкой Raft: он приходит в `response.data.value`.
            // Его обязательно доводим до клиента (K1/C34).
            if let Err(reason) = response.data.as_result() {
                app.metrics.record_tx("add", false);
                audit::add(AuditSource::Api, app.id, &payload.hash, false, Some(reason));
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiResponse::Error {
                        status: "error".to_string(),
                        message: reason.to_string(),
                    }),
                );
            }
            app.metrics.record_tx("add", true);
            audit::add(AuditSource::Api, app.id, &payload.hash, true, None);
            (
                StatusCode::OK,
                Json(ApiResponse::Success(StandardResponse {
                    status: "success".to_string(),
                    message: None,
                })),
            )
        }
        Err(e) => {
            app.metrics.record_tx("add", false);
            audit::add(
                AuditSource::Api,
                app.id,
                &payload.hash,
                false,
                Some(&e.to_string()),
            );
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

/// Параметры пагинации для публичных ручек.
#[derive(Deserialize)]
struct PageParams {
    limit: Option<usize>,
    offset: Option<usize>,
}

impl PageParams {
    /// `(offset, limit)` с ограничением максимального размера страницы.
    fn resolve(&self) -> (usize, usize) {
        const DEFAULT_LIMIT: usize = 100;
        const MAX_LIMIT: usize = 1000;
        let offset = self.offset.unwrap_or(0);
        let limit = self.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
        (offset, limit)
    }
}

async fn pool_handler(
    State(app): State<App>,
    axum::extract::Query(page): axum::extract::Query<PageParams>,
) -> (StatusCode, Json<PoolResponse>) {
    let (offset, limit) = page.resolve();
    let state_machine = app.state_machine.state_machine.read().await;

    // Считаем число активных родителей напрямую, без построения полной карты
    // смежности `get_parents()` для всего графа.
    let mut nodes: Vec<(Hash, usize)> = state_machine
        .dag
        .get_nodes()
        .iter()
        .map(|(node, data)| (node.clone(), data.parents.len()))
        .collect();
    // Сортируем по возрастанию числа активных родителей (меньше — выше приоритет),
    // при равенстве — стабильно по хэшу.
    nodes.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

    let total = nodes.len();
    let nodes: Vec<String> = nodes
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|(node, _)| node.to_string())
        .collect();

    (
        StatusCode::OK,
        Json(PoolResponse {
            status: "success".to_string(),
            nodes,
            total,
            offset,
            limit,
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

    match crate::cleanup::archive_and_remove(&app, payload.nodes, AuditSource::Api).await {
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

async fn get_full_graph_handler(
    State(app): State<App>,
    axum::extract::Query(page): axum::extract::Query<PageParams>,
) -> (StatusCode, Json<FullGraphResponse>) {
    let (offset, limit) = page.resolve();
    let state_machine = app.state_machine.state_machine.read().await;
    let mut nodes = state_machine.dag.get_nodes_by_depth();
    let total = nodes.len();
    // Пагинация уже отсортированного результата (узлы упорядочены по глубине).
    nodes = nodes.into_iter().skip(offset).take(limit).collect();

    (
        StatusCode::OK,
        Json(FullGraphResponse {
            status: "success".to_string(),
            nodes,
            total,
            offset,
            limit,
            message: None,
        }),
    )
}

/// `GET /metrics` — метрики в формате Prometheus text exposition (O2).
async fn metrics_handler(State(app): State<App>) -> Response {
    app.refresh_raft_metrics();
    app.refresh_dag_metrics().await;
    match app.metrics.encode() {
        Ok(body) => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain; version=0.0.4"),
            )],
            body,
        )
            .into_response(),
        Err(e) => {
            error!("failed to encode metrics: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "metrics encoding failed").into_response()
        }
    }
}

/// `GET /health` — liveness: процесс жив (O2).
async fn health_handler() -> (StatusCode, Json<HealthResponse>) {
    (
        StatusCode::OK,
        Json(HealthResponse {
            status: "ok".to_string(),
        }),
    )
}

/// `GET /ready` — readiness: Raft инициализирован, есть лидер, Redis достижим (O2).
async fn ready_handler(State(app): State<App>) -> (StatusCode, Json<ReadyResponse>) {
    let metrics = app.raft.metrics().borrow().clone();
    let has_leader = metrics.current_leader.is_some();

    let mut redis = app.redis.clone();
    let redis_ok = redis::cmd("PING")
        .query_async::<String>(&mut redis)
        .await
        .is_ok();

    let ready = has_leader && redis_ok;
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let mut reasons = Vec::new();
    if !has_leader {
        reasons.push("no leader elected".to_string());
    }
    if !redis_ok {
        reasons.push("redis unavailable".to_string());
    }

    (
        status,
        Json(ReadyResponse {
            status: if ready { "ready" } else { "not_ready" }.to_string(),
            ready,
            has_leader,
            redis_ok,
            reasons,
        }),
    )
}

/// `GET /openapi.json` — спецификация публичного API (O4).
async fn openapi_handler() -> Response {
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        include_str!("../docs/api/openapi.json"),
    )
        .into_response()
}

const SWAGGER_UI: &str = r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>dagdb API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = () => {
      window.ui = SwaggerUIBundle({ url: "/openapi.json", dom_id: "#swagger-ui" });
    };
  </script>
</body>
</html>"##;

/// `GET /docs` — минимальный Swagger UI для `openapi.json` (O4).
async fn docs_handler() -> Html<&'static str> {
    Html(SWAGGER_UI)
}

/// `POST /mng/snapshot` — принудительно строит снапшот (O5: бэкап).
async fn trigger_snapshot_handler(
    State(app): State<App>,
) -> (StatusCode, Json<ApiResponse<StandardResponse>>) {
    match app.raft.trigger().snapshot().await {
        Ok(()) => (
            StatusCode::OK,
            Json(ApiResponse::Success(StandardResponse {
                status: "success".to_string(),
                message: Some("snapshot triggered".to_string()),
            })),
        ),
        Err(e) => {
            error!("failed to trigger snapshot: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::Error {
                    status: "error".to_string(),
                    message: format!("snapshot trigger failed: {e}"),
                }),
            )
        }
    }
}

/// Ответ `/health`.
#[derive(Serialize)]
struct HealthResponse {
    status: String,
}

/// Ответ `/ready`.
#[derive(Serialize)]
struct ReadyResponse {
    status: String,
    ready: bool,
    has_leader: bool,
    redis_ok: bool,
    reasons: Vec<String>,
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
    // V16/S16: `genesis`-узлы не подписаны, поэтому путь `POST /add` (с проверкой
    // подписи) для них неприменим. Зато запись обязана идти через лидера: на
    // follower `client_write` вернёт `ForwardToLeader`, и часть генезиса молча
    // потеряется. Требуем лидера явно, а не полагаемся на удачу.
    if app.raft.metrics().borrow().current_leader != Some(app.id) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiResponse::Error {
                status: "error".to_string(),
                message: "load-genesis must be sent to the current leader".to_string(),
            }),
        );
    }

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
        // K2: сверяем объявленный хэш с каноническим и проверяем структуру
        // (`func`/`var`) до записи в Raft. Неканонический генезис не принимаем.
        let func = match tx_logic::validate_genesis(
            &tx.data.tx,
            &tx.data.func,
            &Hash::from(tx.hash.as_str()),
        ) {
            Ok(func) => func,
            Err(e) => {
                errors.push(format!("Transaction {}: {}", tx.hash, e));
                continue;
            }
        };

        let request = Request::Add {
            hash: Hash::from(tx.hash.as_str()),
            tx: tx.data.tx.clone(),
            sign: tx.data.sign.clone(),
            func,
        };

        match app.raft.client_write(request).await {
            Ok(response) => {
                // Прикладной отказ state machine тоже считаем ошибкой загрузки.
                if let Err(reason) = response.data.as_result() {
                    app.metrics.record_tx("add", false);
                    audit::add(
                        AuditSource::Genesis,
                        app.id,
                        &Hash::from(tx.hash.as_str()),
                        false,
                        Some(reason),
                    );
                    error!(
                        "Genesis transaction {} rejected by state machine: {}",
                        tx.hash, reason
                    );
                    errors.push(format!("Failed to add transaction {}: {}", tx.hash, reason));
                    continue;
                }
                app.metrics.record_tx("add", true);
                audit::add(
                    AuditSource::Genesis,
                    app.id,
                    &Hash::from(tx.hash.as_str()),
                    true,
                    None,
                );
                debug!(
                    "Successfully added genesis transaction with hash {}",
                    tx.hash
                );
            }
            Err(e) => {
                app.metrics.record_tx("add", false);
                audit::add(
                    AuditSource::Genesis,
                    app.id,
                    &Hash::from(tx.hash.as_str()),
                    false,
                    Some(&e.to_string()),
                );
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
    /// Общее число узлов до пагинации.
    total: usize,
    offset: usize,
    limit: usize,
    message: Option<String>,
}

#[derive(Serialize)]
struct FullGraphResponse {
    status: String,
    nodes: Vec<NodeDepth>,
    /// Общее число узлов до пагинации.
    total: usize,
    offset: usize,
    limit: usize,
    message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_params_defaults() {
        let p = PageParams {
            limit: None,
            offset: None,
        };
        assert_eq!(p.resolve(), (0, 100));
    }

    #[test]
    fn page_params_caps_limit_and_uses_offset() {
        let p = PageParams {
            limit: Some(10_000),
            offset: Some(5),
        };
        assert_eq!(p.resolve(), (5, 1000));

        let p = PageParams {
            limit: Some(20),
            offset: Some(40),
        };
        assert_eq!(p.resolve(), (40, 20));
    }
}

/// Запрос на архивацию и удаление «тяжёлых» узлов.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct HeavyNodesRequest {
    /// Хэши узлов-кандидатов.
    pub nodes: Vec<Hash>,
}

/// Middleware ограничения частоты публичного API (O3).
async fn rate_limit_middleware(
    State(app): State<App>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    // Адрес клиента берём из расширений (ConnectInfo), при отсутствии — общий ключ.
    let key = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    if app.rate_limiter.check(&key) {
        next.run(request).await
    } else {
        (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ApiResponse::<StandardResponse>::Error {
                status: "error".to_string(),
                message: "rate limit exceeded".to_string(),
            }),
        )
            .into_response()
    }
}

/// Запускает HTTP-сервер: собирает роутеры, слушает `bind_addr` и корректно
/// завершается по сигналу [`App::shutdown`] (O5).
pub async fn start_server(
    app: App,
    bind_addr: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let max_body = app.max_request_bytes;

    // Публичный API: доступен без аутентификации, ограничен по частоте.
    let public = Router::new()
        .route("/", post(add_tx))
        .route("/pool", get(pool_handler))
        .route("/full", get(get_full_graph_handler))
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .route("/openapi.json", get(openapi_handler))
        .route("/docs", get(docs_handler))
        .route_layer(axum::middleware::from_fn_with_state(
            app.clone(),
            rate_limit_middleware,
        ));

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
        .route("/mng/snapshot", post(trigger_snapshot_handler))
        .route_layer(axum::middleware::from_fn_with_state(
            app.clone(),
            crate::auth::require_internal_token,
        ));

    let srv = public
        .merge(internal)
        // O2: наблюдаемость применяется ко всем маршрутам.
        .route_layer(axum::middleware::from_fn_with_state(
            app.clone(),
            observability_middleware,
        ))
        .layer(DefaultBodyLimit::max(max_body))
        .with_state(app.clone());

    let listener = TcpListener::bind(&bind_addr).await?;
    let local_addr = listener.local_addr()?;
    info!("Server running at {}", local_addr);

    let mut shutdown_signal = app.shutdown.signal();
    axum::serve(
        listener,
        srv.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move { shutdown_signal.cancelled().await })
    .await?;

    info!("HTTP server stopped accepting requests");
    Ok(())
}
