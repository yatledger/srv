use dagdb::server;
use std::sync::{Arc, RwLock}; // Для передачи графа в cleaner
use dagdb::graph::DAG; // Для создания графа
use tracing::info;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Настраиваем логирование с фильтром по переменной окружения RUST_LOG
    tracing_subscriber::fmt()
        .with_env_filter("info") // Устанавливаем уровень info по умолчанию
        .with_ansi(true)
        .with_target(true)
        .with_thread_names(true)
        .init();

    // Логируем запуск приложения
    info!("Starting the DAG server");
    // Создаём граф и оборачиваем его в Arc<RwLock>
    let graph = Arc::new(RwLock::new(DAG::new()));

    // Запускаем сервер
    server::start_server(graph).await?;
    info!("Server shutdown");
    Ok(())
}