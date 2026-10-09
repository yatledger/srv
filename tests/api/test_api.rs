//! Интеграционные тесты HTTP API (O1).
//!
//! Поднимают один реальный узел и проверяют публичные и внутренние маршруты:
//! health/ready/metrics/openapi, пустой граф, отклонение запросов без токена и
//! отклонение невалидной/неподписанной транзакции.
//!
//! Требуется Redis (в CI — сервис). Без Redis тесты пропускаются.

use std::path::PathBuf;
use std::time::Duration;

use serde_json::Value;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use dagdb::app::App;
use dagdb::config::{AppConfig, LogFormat, Profile};
use dagdb::start_raft;

const TOKEN: &str = "api-test-token-api-test-token-123456";

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

struct Harness {
    app: App,
    server: JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    base: String,
}

async fn start_node(redis_url: &str) -> Harness {
    let id = 1;
    let port = free_port();
    let dir: PathBuf =
        std::env::temp_dir().join(format!("dagdb-api-{}-{port}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let addr = format!("127.0.0.1:{port}");

    let cfg = AppConfig {
        id,
        profile: Profile::Dev,
        log_format: LogFormat::Text,
        addr: "127.0.0.1".to_string(),
        port,
        bind_addr: Some(addr.clone()),
        advertise_addr: Some(addr.clone()),
        data_dir: dir,
        redis_url: Some(redis_url.to_string()),
        internal_api_token: Some(TOKEN.to_string()),
        cluster_nodes: None,
        http_timeout_secs: 10,
        http_connect_timeout_secs: 3,
        raft_http_timeout_secs: 30,
        raft_connect_timeout_secs: 10,
        processor_interval_ms: 1000,
        weight_threshold: 0.9,
        cleanup_batch_size: 100,
        public_rate_limit_per_sec: 0,
        public_rate_limit_burst: 1,
        max_request_bytes: 1_048_576,
    };

    let (_raft, app) = start_raft(&cfg).await.expect("start_raft");
    let server_app = app.clone();
    let bind = addr.clone();
    let server = tokio::spawn(async move { dagdb::server::start_server(server_app, bind).await });

    let harness = Harness {
        app,
        server,
        base: format!("http://127.0.0.1:{port}"),
    };
    wait_ready(&harness.base).await;
    harness
}

impl Harness {
    async fn shutdown(mut self) {
        self.app.shutdown.trigger();
        let _ = tokio::time::timeout(Duration::from_secs(10), &mut self.server).await;
        let _ = self.app.raft.shutdown().await;
    }
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}

async fn wait_ready(base: &str) {
    let client = client();
    for _ in 0..200 {
        if let Ok(resp) = client.get(format!("{base}/health")).send().await
            && resp.status().is_success()
        {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("узел {base} не поднялся");
}

async fn redis_available(redis_url: &str) -> bool {
    let Ok(client) = redis::Client::open(redis_url) else {
        return false;
    };
    let connect = redis::aio::ConnectionManager::new(client);
    let Ok(Ok(mut conn)) = tokio::time::timeout(Duration::from_secs(2), connect).await else {
        return false;
    };
    let ping = async {
        redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .is_ok()
    };
    matches!(
        tokio::time::timeout(Duration::from_secs(2), ping).await,
        Ok(true)
    )
}

async fn setup() -> Option<Harness> {
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
    if !redis_available(&redis_url).await {
        eprintln!("SKIP: Redis недоступен по {redis_url}");
        return None;
    }
    Some(start_node(&redis_url).await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_ready_metrics_openapi() {
    let Some(node) = setup().await else { return };
    let client = client();

    // Health всегда 200.
    let health = client
        .get(format!("{}/health", node.base))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), reqwest::StatusCode::OK);

    // Ready: без лидера узел не готов (503), но эндпоинт отвечает корректно.
    let ready = client
        .get(format!("{}/ready", node.base))
        .send()
        .await
        .unwrap();
    assert!(
        ready.status() == reqwest::StatusCode::OK
            || ready.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE
    );

    // Метрики в формате Prometheus.
    let metrics = client
        .get(format!("{}/metrics", node.base))
        .send()
        .await
        .unwrap();
    assert_eq!(metrics.status(), reqwest::StatusCode::OK);
    let body = metrics.text().await.unwrap();
    assert!(body.contains("dagdb_dag_nodes"), "нет метрик DAG: {body}");

    // OpenAPI-спецификация и Swagger UI.
    let spec = client
        .get(format!("{}/openapi.json", node.base))
        .send()
        .await
        .unwrap();
    assert_eq!(spec.status(), reqwest::StatusCode::OK);
    let spec_json: Value = spec.json().await.unwrap();
    assert_eq!(spec_json["openapi"], "3.0.3");

    let docs = client
        .get(format!("{}/docs", node.base))
        .send()
        .await
        .unwrap();
    assert_eq!(docs.status(), reqwest::StatusCode::OK);

    // Корреляционный id присутствует в ответе.
    let resp = client
        .get(format!("{}/health", node.base))
        .send()
        .await
        .unwrap();
    assert!(resp.headers().contains_key("x-request-id"));

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_pool_and_full_are_empty_and_paginated() {
    let Some(node) = setup().await else { return };
    let client = client();

    let pool: Value = client
        .get(format!("{}/pool?limit=10&offset=0", node.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(pool["status"], "success");
    assert_eq!(pool["total"], 0);
    assert_eq!(pool["limit"], 10);

    let full: Value = client
        .get(format!("{}/full", node.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(full["status"], "success");
    assert_eq!(full["total"], 0);

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn internal_endpoints_require_token() {
    let Some(node) = setup().await else { return };
    let client = client();

    // Без токена внутренние ручки недоступны.
    let no_token = client
        .post(format!("{}/mng/metrics", node.base))
        .send()
        .await
        .unwrap();
    assert_eq!(no_token.status(), reqwest::StatusCode::UNAUTHORIZED);

    let with_token = client
        .post(format!("{}/mng/metrics", node.base))
        .header("x-internal-token", TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(with_token.status(), reqwest::StatusCode::OK);

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_add_rejects_unsigned_transaction() {
    let Some(node) = setup().await else { return };
    let client = client();

    // Неподписанная/невалидная транзакция отклоняется на границе API.
    let body = serde_json::json!({
        "tx": { "prnts": ["a", "b"], "addr": "", "seq": 0, "var": {} },
        "sign": "",
        "func": "transferToken"
    });
    let resp = client
        .post(format!("{}/", node.base))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    node.shutdown().await;
}
