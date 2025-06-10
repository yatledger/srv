use dagdb::server;
use std::sync::{Arc, RwLock}; // Для передачи графа в cleaner
use dagdb::graph::DAG; // Для создания графа

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Создаём граф и оборачиваем его в Arc<RwLock>
    let graph = Arc::new(RwLock::new(DAG::new()));

    // Запускаем сервер
    server::start_server(graph).await?;
    Ok(())
}