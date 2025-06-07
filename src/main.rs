use dagdb::server;

#[tokio::main] // Макрос для создания асинхронного runtime с Tokio.
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Запускаем сервер.
    server::start_server().await?;
    Ok(())
}