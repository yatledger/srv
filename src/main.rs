use dagdb::server;
use dagdb::cleaner; // Добавляем импорт модуля cleaner
use std::sync::{Arc, RwLock}; // Для передачи графа в cleaner
use dagdb::graph::DAG; // Для создания графа

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Создаём граф и оборачиваем его в Arc<RwLock>
    let graph = Arc::new(RwLock::new(DAG::new()));
    
    // Запускаем cleaner как фоновую задачу
    let cleaner_graph = Arc::clone(&graph);
    tokio::spawn(async move {
        cleaner::start_cleaner(cleaner_graph).await;
    });

    // Запускаем сервер
    server::start_server(graph).await?;
    Ok(())
}