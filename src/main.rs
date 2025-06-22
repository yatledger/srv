use dagdb::graph::DAG;
use std::sync::{Arc, RwLock};
use tracing::info;
use tracing_subscriber;

use dagdb::server;
use dagdb::new_raft;
use dagdb::cleaner;
use dagdb::updater;
use dagdb::router::Router;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Настраиваем логирование с фильтром по переменной окружения RUST_LOG
    tracing_subscriber::fmt()
        .with_env_filter("info") // Устанавливаем уровень info по умолчанию
        .with_ansi(true)
        //.with_target(true)
        //.with_thread_names(true)
        .init();

    // Логируем запуск приложения
    info!("Starting the DAG server");
    // Создаём граф и оборачиваем его в Arc<RwLock>
    let graph = Arc::new(RwLock::new(DAG::new()));

    let cleaner_graph = Arc::clone(&graph);
    tokio::spawn(async move {
        cleaner::start_cleaner(cleaner_graph).await;
    });

    let updater_graph = Arc::clone(&graph);
    tokio::spawn(async move {
        updater::start_weight_updater(updater_graph).await;
    });

    // Запускаем сервер
    server::start_server(graph).await?;

    let router = Router::default();
    new_raft(1, router.clone()).await;

    info!("Server shutdown");
    Ok(())
}