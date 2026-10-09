//! Кластерный интеграционный тест (O1).
//!
//! Поднимает три реальных узла в одном процессе (каждый — со своим runtime и
//! HTTP-сервером на свободном порту), покрывая сценарии из ТЗ: инициализация,
//! add-learner, change-membership, репликация, снапшот, рестарт узла и
//! конкурентные записи.
//!
//! Требуется Redis (в CI поднимается сервисом). Если Redis недоступен, тест
//! пропускается с предупреждением, чтобы локальный прогон без инфраструктуры не
//! падал.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base58::ToBase58;
use ed25519_dalek::Signer;
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use dagdb::Tx;
use dagdb::app::App;
use dagdb::config::{AppConfig, LogFormat, Profile};
use dagdb::domain::{Address, Hash};
use dagdb::graph::dag::TRANSFER_TOKEN;
use dagdb::start_raft;
use dagdb::utils::ordered_sum;

const TOKEN: &str = "cluster-test-token-cluster-test-token";

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn temp_dir(name: &str, id: u64) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "dagdb-cluster-{}-{name}-{id}-{seq}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn make_config(id: u64, port: u16, data_dir: PathBuf, redis_url: &str) -> AppConfig {
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
        cleanup_batch_size: 100,
        // Rate limiting выключен, чтобы конкурентные записи не упирались в лимит.
        public_rate_limit_per_sec: 0,
        public_rate_limit_burst: 1,
        max_request_bytes: 1_048_576,
    }
}

/// Запущенный узел кластера.
struct Node {
    id: u64,
    port: u16,
    dir: PathBuf,
    app: App,
    server: JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
}

impl Node {
    async fn start(id: u64, port: u16, dir: PathBuf, redis_url: &str) -> Node {
        let cfg = make_config(id, port, dir.clone(), redis_url);
        // При рестарте в одном процессе предыдущий экземпляр может ещё не
        // успеть освободить файловые локи redb; повторяем открытие с короткой
        // паузой, чтобы тест не был флейки.
        let mut attempt = 0;
        let (_raft, app) = loop {
            match start_raft(&cfg).await {
                Ok(pair) => break pair,
                Err(e) => {
                    attempt += 1;
                    assert!(attempt < 200, "start_raft не удался: {e}");
                    sleep(Duration::from_millis(50)).await;
                }
            }
        };
        let server_app = app.clone();
        let bind = format!("127.0.0.1:{port}");
        let server =
            tokio::spawn(async move { dagdb::server::start_server(server_app, bind).await });
        Node {
            id,
            port,
            dir,
            app,
            server,
        }
    }

    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    async fn shutdown(mut self) {
        self.app.shutdown.trigger();
        let _ = tokio::time::timeout(Duration::from_secs(10), &mut self.server).await;
        let _ = self.app.raft.shutdown().await;
    }
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

async fn wait_ready(client: &reqwest::Client, base: &str) {
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

async fn admin_post(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    body: Value,
) -> reqwest::Response {
    client
        .post(format!("{base}{path}"))
        .header("x-internal-token", TOKEN)
        .json(&body)
        .send()
        .await
        .expect("admin request")
}

async fn wait_leader(client: &reqwest::Client, base: &str) -> u64 {
    for _ in 0..200 {
        if let Ok(resp) = client
            .post(format!("{base}/mng/metrics"))
            .header("x-internal-token", TOKEN)
            .send()
            .await
            && let Ok(v) = resp.json::<Value>().await
            && let Some(id) = v["current_leader"].as_u64()
        {
            return id;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("лидер не выбран в кластере");
}

/// Ждёт, пока узел увидит хотя бы `min_nodes` узлов в DAG.
async fn wait_node_count(client: &reqwest::Client, base: &str, min_nodes: u64) {
    for _ in 0..200 {
        if let Ok(resp) = client.get(format!("{base}/full")).send().await
            && let Ok(v) = resp.json::<Value>().await
            && v["total"].as_u64().unwrap_or(0) >= min_nodes
        {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("репликация на {base} не достигла {min_nodes} узлов");
}

fn load_genesis<'a>(
    client: &'a reqwest::Client,
    base: &'a str,
) -> impl std::future::Future<Output = ()> + 'a {
    let client = client.clone();
    let base = base.to_string();
    async move {
        let resp = admin_post(&client, &base, "/load-genesis", Value::Array(vec![])).await;
        assert!(
            resp.status().is_success(),
            "genesis не загрузился: {}",
            resp.status()
        );
    }
}

/// Два хэша генезис-узлов, чтобы собрать валидную (≥2 родителей) транзакцию.
async fn genesis_parents(client: &reqwest::Client, base: &str) -> (Hash, Hash) {
    let v: Value = client
        .get(format!("{base}/pool"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let nodes: Vec<String> = v["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n.as_str().unwrap().to_string())
        .collect();
    assert!(
        nodes.len() >= 2,
        "нужно минимум два генезис-узла, есть {}",
        nodes.len()
    );
    (Hash::from(nodes[0].as_str()), Hash::from(nodes[1].as_str()))
}

fn keypair(seed: u8) -> (String, ed25519_dalek::SigningKey) {
    let sk = ed25519_dalek::SigningKey::from_bytes(&[seed; 32]);
    let addr = sk.verifying_key().to_bytes().to_base58();
    (addr, sk)
}

/// Собирает подписанную транзакцию (JSON тела `POST /`).
fn signed_tx(sk: &ed25519_dalek::SigningKey, addr: &str, parents: &[Hash], seq: u32) -> Value {
    let tx = Tx::new(
        parents.to_vec(),
        Address::from(addr),
        seq,
        serde_json::json!({ "ca": "0", "to": "T", "val": seq as u64 + 1, "msg": "cluster" }),
    );
    let hash = ordered_sum(&tx, TRANSFER_TOKEN).unwrap();
    let sign = hex::encode(sk.sign(hash.as_bytes()).to_bytes());
    serde_json::json!({ "tx": tx, "sign": sign, "func": TRANSFER_TOKEN })
}

async fn post_tx(client: &reqwest::Client, base: &str, body: &Value) -> reqwest::StatusCode {
    client
        .post(format!("{base}/"))
        .json(body)
        .send()
        .await
        .expect("tx request")
        .status()
}

/// Проверяет, что Redis доступен; иначе тест считается пропущенным.
/// Ограничен по времени, чтобы недоступный Redis не блокировал прогон.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_init_membership_replication_snapshot_restart_concurrent() {
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string());
    if !redis_available(&redis_url).await {
        eprintln!("SKIP: Redis недоступен по {redis_url}; запустите Redis для кластерного теста");
        return;
    }

    let client = client();
    let port1 = free_port();
    let port2 = free_port();
    let port3 = free_port();

    let node1 = Node::start(1, port1, temp_dir("n1", 1), &redis_url).await;
    let node2 = Node::start(2, port2, temp_dir("n2", 2), &redis_url).await;
    let node3 = Node::start(3, port3, temp_dir("n3", 3), &redis_url).await;

    for node in [&node1, &node2, &node3] {
        wait_ready(&client, &node.base()).await;
    }

    // 1. Инициализация одиночного узла 1.
    let init = admin_post(&client, &node1.base(), "/mng/init", Value::Array(vec![])).await;
    assert!(init.status().is_success(), "init failed: {}", init.status());

    // 2. Лидер и добавление learners 2 и 3.
    let leader = wait_leader(&client, &node1.base()).await;
    assert_eq!(leader, 1, "ожидался лидер node1");
    for (id, port) in [(2u64, port2), (3u64, port3)] {
        let resp = admin_post(
            &client,
            &node1.base(),
            "/mng/add-learner",
            serde_json::json!([id, format!("127.0.0.1:{port}")]),
        )
        .await;
        assert!(
            resp.status().is_success(),
            "add-learner {id} failed: {}",
            resp.status()
        );
    }

    // 3. Изменение состава кластера: {1,2,3}.
    let membership = admin_post(
        &client,
        &node1.base(),
        "/mng/change-membership",
        serde_json::json!([1, 2, 3]),
    )
    .await;
    assert!(
        membership.status().is_success(),
        "change-membership failed: {}",
        membership.status()
    );

    // 4. Генезис и репликация транзакции на все узлы.
    load_genesis(&client, &node1.base()).await;
    for node in [&node1, &node2, &node3] {
        wait_node_count(&client, &node.base(), 3).await;
    }

    let (p1, p2) = genesis_parents(&client, &node1.base()).await;
    let (addr, sk) = keypair(7);
    let tx = signed_tx(&sk, &addr, &[p1.clone(), p2.clone()], 1);

    // Пишем в follower (node2), проверяя пересылку лидеру.
    let mut status = post_tx(&client, &node2.base(), &tx).await;
    for _ in 0..100 {
        if status.is_success() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
        status = post_tx(&client, &node2.base(), &tx).await;
    }
    assert!(status.is_success(), "транзакция не принята: {status}");

    for node in [&node1, &node2, &node3] {
        wait_node_count(&client, &node.base(), 4).await;
    }

    // 5. Принудительный снапшот у лидера.
    let snap = admin_post(&client, &node1.base(), "/mng/snapshot", Value::Null).await;
    assert!(
        snap.status().is_success(),
        "snapshot failed: {}",
        snap.status()
    );

    // 6. Рестарт узла 3 с тем же каталогом данных: состояние переживает рестарт.
    let (dir3, id3, p3) = (node3.dir.clone(), node3.id, node3.port);
    node3.shutdown().await;
    let node3 = Node::start(id3, p3, dir3, &redis_url).await;
    wait_ready(&client, &node3.base()).await;
    wait_node_count(&client, &node3.base(), 4).await;

    // 7. Конкурентные записи (все — в лидер напрямую).
    let mut tasks = Vec::new();
    for seq in 0..10u32 {
        let client = client.clone();
        let base = node1.base();
        let body = signed_tx(&sk, &addr, &[p1.clone(), p2.clone()], 100 + seq);
        tasks.push(tokio::spawn(
            async move { post_tx(&client, &base, &body).await },
        ));
    }
    for task in tasks {
        let status = task.await.unwrap();
        assert!(
            status.is_success(),
            "конкурентная запись отклонена: {status}"
        );
    }

    for node in [&node1, &node2, &node3] {
        wait_node_count(&client, &node.base(), 14).await;
    }

    node1.shutdown().await;
    node2.shutdown().await;
    node3.shutdown().await;
}

#[allow(dead_code)]
fn _assert_app_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Arc<App>>();
}
