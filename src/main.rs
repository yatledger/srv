mod graph; // Импортируем модуль graph.rs.
mod server; // Импортируем модуль server.rs.

#[tokio::main] // Макрос для создания асинхронного runtime с Tokio.
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Запускаем сервер.
    server::start_server().await?;
    Ok(())
}