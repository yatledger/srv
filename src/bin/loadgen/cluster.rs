//! In-process кластер `dagdb`: запуск N узлов, bootstrap и остановка.
//!
//! Узлы поднимаются в одном процессе, каждый — на отдельном `std::thread` со
//! своим current-thread runtime. Так HTTP-серверы узлов и поток нагрузки не
//! конкурируют за воркеры общего runtime. Очистка и rate limit выключены
//! (`cleanup_batch_size = 0`, `public_rate_limit_per_sec = 0`), иначе родители
//! «уезжают» в `added`, а часть запросов ловит `429` — и корректностный инвариант
//! становится недостижимым. Требуется Redis (как и для `tests/cluster`).

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use serde_json::Value;

use dagdb::app::App;
use dagdb::config::{AppConfig, LogFormat, Profile};
use dagdb::start_raft;

/// Общий кластерный токен in-process узлов.
pub const TOKEN: &str = "loadgen-internal-token-loadgen-internal-token";

/// Запущенный узел кластера.
struct Node {
    id: u64,
    port: u16,
    dir: PathBuf,
    app: App,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Node {
    /// Базовый URL узла.
    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

/// Запущенный in-process кластер `.`
pub struct Cluster {
    nodes: Vec<Node>,
    keep_data: bool,
}

impl Cluster {
    /// Поднимает `n` узлов и выполняет bootstrap: init, learners, membership,
    /// genesis. Возвращает готовый к нагрузке кластер.
    pub async fn start(
        n: u64,
        redis_url: &str,
        keep_data: bool,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if n == 0 {
            return Err("число узлов должно быть больше 0".into());
        }

        // Резервируем порты заранее (listener сразу закрывается — порт может быть
        // занят другим процессом, но в dev/CI этого достаточно, как в tests/cluster).
        let mut nodes = Vec::new();
        for id in 1..=n {
            let port = free_port();
            let dir = temp_dir(n, id);
            let node = Node::start(id, port, dir, redis_url).await?;
            nodes.push(node);
        }

        let cluster = Cluster { nodes, keep_data };
        cluster.bootstrap().await?;
        Ok(cluster)
    }

    /// Базовые URL всех узлов.
    pub fn base_urls(&self) -> Vec<String> {
        self.nodes.iter().map(Node::base).collect()
    }

    /// Пары `(node_id, base_url)` для драйвера нагрузки.
    pub fn nodes_for_load(&self) -> Vec<(u64, String)> {
        self.nodes.iter().map(|n| (n.id, n.base())).collect()
    }

    /// Итоговые размеры DAG по каждой реплике: `(node_id, total)`.
    ///
    /// Использует публичный `GET /full` (без токена). В случае недоступности узла
    /// размер помечается как 0 — корректностная проверка тогда провалится явно.
    pub async fn dag_totals(&self) -> Vec<(u64, u64)> {
        let client = http_client_async();
        let mut out = Vec::new();
        for node in &self.nodes {
            out.push((node.id, fetch_total(&client, &node.base()).await));
        }
        out
    }

    /// Bootstrap кластера: инициализация, learners, membership и genesis.
    async fn bootstrap(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = http_client_async();
        let urls = self.base_urls();

        for url in &urls {
            wait_health(&client, url).await?;
        }

        // 1. Инициализируем первый узел одиночным кластером.
        admin_post(&client, &urls[0], "/mng/init", &Value::Array(vec![])).await?;

        // 2. Ждём выборов лидера на первом узле.
        let leader = wait_leader(&client, &urls[0]).await?;
        if leader != self.nodes[0].id {
            return Err(format!("ожидался лидер node1, выбран узел {leader}").into());
        }

        // 3. Добавляем остальные узлы как learners.
        for node in self.nodes.iter().skip(1) {
            let body = serde_json::json!([node.id, format!("127.0.0.1:{}", node.port)]);
            admin_post(&client, &urls[0], "/mng/add-learner", &body).await?;
        }

        // 4. Расширяем membership до всех узлов.
        if self.nodes.len() > 1 {
            let members: BTreeSet<u64> = self.nodes.iter().map(|n| n.id).collect();
            let body = serde_json::to_value(members)?;
            admin_post(&client, &urls[0], "/mng/change-membership", &body).await?;
        }

        // 5. Загружаем генезис на текущего лидера (после membership лидер мог
        // остаться node1, но надёжнее определить его заново).
        let leader = wait_leader(&client, &urls[0]).await?;
        let leader_url = self
            .nodes
            .iter()
            .find(|n| n.id == leader)
            .map(Node::base)
            .ok_or_else(|| format!("лидер {leader} не найден среди узлов"))?;
        admin_post(&client, &leader_url, "/load-genesis", &Value::Array(vec![])).await?;

        // 6. Ждём, пока все реплики увидят генезис (3 узла).
        for url in &urls {
            wait_min_total(&client, url, 3).await?;
        }

        Ok(())
    }

    /// Останавливает кластер: сигнал shutdown каждому узлу и join потоков.
    pub async fn shutdown(self) {
        for node in &self.nodes {
            node.app.shutdown.trigger();
        }
        for node in self.nodes {
            let dir = node.dir.clone();
            let keep = self.keep_data;
            if let Some(handle) = node.thread {
                let _ = tokio::task::spawn_blocking(move || handle.join()).await;
            }
            // Финализируем Raft после остановки HTTP-сервера узла.
            if let Err(e) = node.app.raft.shutdown().await {
                eprintln!("loadgen: raft shutdown узла {}: {e}", node.id);
            }
            if !keep {
                let _ = std::fs::remove_dir_all(&dir);
            }
        }
    }
}

impl Node {
    /// Запускает один узел в отдельном потоке.
    async fn start(
        id: u64,
        port: u16,
        dir: PathBuf,
        redis_url: &str,
    ) -> Result<Node, Box<dyn std::error::Error + Send + Sync>> {
        let cfg = node_config(id, port, dir.clone(), redis_url);
        let bind = format!("127.0.0.1:{port}");

        // Сборку узла выполняем на текущем (async) runtime вызывающего: так ошибки
        // конфигурации/Redis возвращаются сразу, а не теряются в потоке.
        let (_raft, app) = start_raft(&cfg).await?;

        let server_app = app.clone();
        let thread = std::thread::Builder::new()
            .name(format!("loadgen-node-{id}"))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("current-thread runtime для узла");
                rt.block_on(async move {
                    if let Err(e) = dagdb::server::start_server(server_app, bind).await {
                        eprintln!("loadgen: сервер узла {id} завершился с ошибкой: {e}");
                    }
                });
            })?;

        Ok(Node {
            id,
            port,
            dir,
            app,
            thread: Some(thread),
        })
    }
}

/// Конфигурация in-process узла с выключенными очисткой и rate limit.
fn node_config(id: u64, port: u16, data_dir: PathBuf, redis_url: &str) -> AppConfig {
    let addr = format!("127.0.0.1:{port}");
    AppConfig {
        id,
        profile: Profile::Dev,
        log_format: LogFormat::Text,
        addr: "127.0.0.1".to_string(),
        port: Some(port),
        bind_addr: Some(addr.clone()),
        advertise_addr: Some(addr),
        data_dir,
        redis_url: Some(redis_url.to_string()),
        internal_api_token: Some(TOKEN.to_string()),
        trust_proxy: false,
        http_timeout_secs: 10,
        http_connect_timeout_secs: 3,
        raft_http_timeout_secs: 30,
        raft_connect_timeout_secs: 10,
        redis_connect_timeout_secs: 5,
        processor_interval_ms: 1000,
        weight_threshold: 0.9,
        // Очистка выключена: удалённые родители уходят в `added` и ломают генерацию.
        cleanup_batch_size: 0,
        // Rate limit выключен: иначе часть запросов ловит 429.
        public_rate_limit_per_sec: 0,
        public_rate_limit_burst: 1,
        max_request_bytes: 1_048_576,
    }
}

/// Свободный локальный TCP-порт.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    listener.local_addr().expect("local_addr").port()
}

/// Уникальный временный каталог данных узла.
fn temp_dir(nodes: u64, id: u64) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "dagdb-loadgen-{}-{nodes}n-{id}-{seq}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp data dir");
    dir
}

/// Асинхронный HTTP-клиент с таймаутом.
fn http_client_async() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("reqwest client")
}

/// Запрашивает `total` графа у узла через публичный `/full`.
async fn fetch_total(client: &reqwest::Client, base: &str) -> u64 {
    match client.get(format!("{base}/full?limit=1")).send().await {
        Ok(resp) => resp
            .json::<Value>()
            .await
            .ok()
            .and_then(|v| v["total"].as_u64())
            .unwrap_or(0),
        Err(_) => 0,
    }
}

/// Ждёт `GET /health` (публичная ручка).
async fn wait_health(
    client: &reqwest::Client,
    base: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for _ in 0..400 {
        if let Ok(resp) = client.get(format!("{base}/health")).send().await
            && resp.status().is_success()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(format!("узел {base} не поднялся за отведённое время").into())
}

/// Ждёт выбора лидера и возвращает его id.
async fn wait_leader(
    client: &reqwest::Client,
    base: &str,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    for _ in 0..400 {
        if let Ok(resp) = client
            .post(format!("{base}/mng/metrics"))
            .header("x-internal-token", TOKEN)
            .send()
            .await
            && let Ok(v) = resp.json::<Value>().await
            && let Some(id) = v["current_leader"].as_u64()
        {
            return Ok(id);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err("лидер не выбран в кластере".into())
}

/// Ждёт, пока публичный `/full` покажет не меньше `min_total` узлов.
async fn wait_min_total(
    client: &reqwest::Client,
    base: &str,
    min_total: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for _ in 0..400 {
        if let Ok(resp) = client.get(format!("{base}/full?limit=1")).send().await
            && let Ok(v) = resp.json::<Value>().await
            && v["total"].as_u64().unwrap_or(0) >= min_total
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(format!("реплика {base} не достигла {min_total} узлов").into())
}

/// POST во внутренний API с кластерным токеном; ошибка при неуспешном статусе.
async fn admin_post(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    body: &Value,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let resp = client
        .post(format!("{base}{path}"))
        .header("x-internal-token", TOKEN)
        .json(body)
        .send()
        .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("{path} на {base}: статус {status}, тело: {text}").into());
    }
    Ok(())
}
