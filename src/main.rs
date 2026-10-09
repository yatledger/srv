use tracing::info;
use tracing_subscriber::fmt::time::ChronoLocal;

use dagdb::config::AppConfig;
use dagdb::processor;
use dagdb::server;
use dagdb::start_raft;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_ansi(true)
        .with_timer(ChronoLocal::new("%m-%d/%H:%M:%S%.3f".to_string()))
        .init();

    // Логируем запуск приложения
    info!("Starting the Dag server");

    let cfg = AppConfig::load();
    let (_raft, app) = start_raft(&cfg).await?;

    let processor_app = app.clone();

    tokio::spawn(async move {
        processor::start_processor(processor_app).await;
    });

    // Запускаем сервер
    server::start_server(app, cfg.bind_addr()).await?;

    info!("Server shutdown");
    Ok(())
}
