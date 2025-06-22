use std::backtrace::Backtrace;
use std::collections::BTreeMap;
use std::panic::PanicHookInfo;
use std::time::Duration;

use openraft::BasicNode;
use dagdb::new_raft;
use dagdb::router::Router;
use dagdb::command::Request;
use dagdb::typ;
use tokio::task;
use tokio::task::LocalSet;
use tracing_subscriber::EnvFilter;

use std::sync::Arc;
use serde_json::{json};

pub fn log_panic(panic: &PanicHookInfo) {
    let backtrace = format!("{:?}", Backtrace::force_capture());

    eprintln!("{}", panic);

    if let Some(location) = panic.location() {
        tracing::error!(
            message = %panic,
            backtrace = %backtrace,
            panic.file = location.file(),
            panic.line = location.line(),
            panic.column = location.column(),
        );
        eprintln!("{}:{}:{}", location.file(), location.line(), location.column());
    } else {
        tracing::error!(message = %panic, backtrace = %backtrace);
    }

    eprintln!("{}", backtrace);
}

/// This test shows how to transfer a snapshot from one node to another:
///
/// - Setup a single node cluster, write some logs, take a snapshot;
/// - Add a learner node-2 to receive snapshot replication, via the complete-snapshot API:
///   - The sending end sends snapshot with `RaftNetwork::full_snapshot()`;
///   - The receiving end deliver the received snapshot to `Raft` with
///     `Raft::install_full_snapshot()`.
#[tokio::test]
async fn test_cluster() {
    std::panic::set_hook(Box::new(|panic| {
        log_panic(panic);
    }));

    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let router = Router::default();

    let local = LocalSet::new();

    let (raft1, app1) = new_raft(1, router.clone()).await;
    let (raft2, app2) = new_raft(2, router.clone()).await;

    let rafts = [raft1, raft2];

    local
        .run_until(async move {
            task::spawn_local(app1.run());
            task::spawn_local(app2.run());

            run_test(&rafts, router).await;
        })
        .await;
}

async fn run_test(rafts: &[typ::Raft], router: Router) {
    let _ = router;

    // Wait for server to start up.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let raft1 = &rafts[0];
    let raft2 = &rafts[1];

    println!("=== init single node cluster");
    {
        let mut nodes = BTreeMap::new();
        nodes.insert(1, BasicNode { addr: "".to_string() });
        raft1.initialize(nodes).await.unwrap();
    }

    println!("=== write 2 logs");
    {
        let hash = <Arc<str>>::from("1cace26889072316f109df9229011ceb35015b19e31c5d5e533cf6ed5c684e0d908de464c646ac626c262c7769f89d9c137458a12481347266");

        // Создаём parents как Vec<Arc<str>>
        let parents: Vec<Arc<str>> = vec![]; // В JSON parents пустой массив, поэтому пустой вектор

        // Создаём data как Arc<Value> из JSON
        let data = Arc::new(json!({
            "parents": ["0", "1"],
            "credit": "",
            "debit": "Hm4GGGtBGiLzHoVSTvXqz5JoV9JXqJWiYrZvXp6GCy4L",
            "amount": 500000000,
            "uniq": "",
            "msg": "хуй",
            "sign": "",
            "time": 174961
        }));

        let hash2 = <Arc<str>>::from("bb4041d38e1c807185465a653ba8b93ab5b072874b3433ab714a56038f6eefd2344b0c653555a66780843b7db7f4115c5ffb0da85fc2b95414");

        // Создаём parents как Vec<Arc<str>>
        let parents2: Vec<Arc<str>> = vec![
            Arc::from("qwe"),
            Arc::from("1cace26889072316f109df9229011ceb35015b19e31c5d5e533cf6ed5c684e0d908de464c646ac626c262c7769f89d9c137458a124813472668e4189fbb7e758"),
        ];

        // Создаём data как Arc<Value> из JSON
        let data2 = Arc::new(json!({
            "parents": ["0", "1"],
            "credit": "",
            "debit": "RvCcJWKYpoEXGm6N72kDj3dA8zTxGTMSBpCLSKcJvdR",
            "amount": 3300000,
            "uniq": "",
            "msg": "в жопе",
            "sign": "",
            "time": 174961
        }));

        let resp = raft1.client_write(Request::add(hash.clone(), parents.clone(), data.clone())).await.unwrap();
        println!("write resp: {:#?}", resp);
        let resp = raft1.client_write(Request::add(hash2.clone(), parents2.clone(), data2.clone())).await.unwrap();
        println!("write resp: {:#?}", resp);
    }

    println!("=== let node-1 take a snapshot");
    {
        raft1.trigger().snapshot().await.unwrap();

        // Wait for a while to let the snapshot get done.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    println!("=== metrics after building snapshot");
    {
        let metrics = raft1.metrics().borrow().clone();
        println!("node 1 metrics: {:#?}", metrics);
        assert_eq!(Some(3), metrics.snapshot.map(|x| x.index));
        assert_eq!(Some(3), metrics.purged.map(|x| x.index));
    }

    println!("=== add-learner node-2");
    {
        let node = BasicNode { addr: "".to_string() };
        let resp = raft1.add_learner(2, node, true).await.unwrap();
        println!("add-learner node-2 resp: {:#?}", resp);
    }

    // Wait for a while to let the node 2 to receive snapshot replication.
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("=== metrics of node 2 that received snapshot");
    {
        let metrics = raft2.metrics().borrow().clone();
        println!("node 2 metrics: {:#?}", metrics);
        assert_eq!(Some(3), metrics.snapshot.map(|x| x.index));
        assert_eq!(Some(3), metrics.purged.map(|x| x.index));
    }

    // In this example, the snapshot is just a copy of the state machine.
    let snapshot = raft2.get_snapshot().await.unwrap();
    println!("node 2 received snapshot: {:#?}", snapshot);
}
