use std::sync::Arc;
use tracing::info;
use tracing_subscriber::fmt::time::ChronoLocal;

use dagdb::config::AppConfig;
use dagdb::processor;
use dagdb::server;
use dagdb::start_raft;
use dagdb::web::Router;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_ansi(true)
        .with_timer(ChronoLocal::new("%m-%d/%H:%M:%S%.3f".to_string()))
        .init();

    // Логируем запуск приложения
    info!("Starting the DAG server");

    let cfg = AppConfig::load();
    let (_raft, app) = start_raft(&cfg).await?;

    let router = Router::new(cfg.http_timeout(), cfg.http_connect_timeout());

    let processor_sm = Arc::clone(&app.state_machine);
    let processor_raft = app.raft.clone();
    let processor_node_id = cfg.id;
    let processor_router = router.clone();

    tokio::spawn(async move {
        processor::start_processor(
            processor_sm,
            processor_raft,
            processor_node_id,
            processor_router,
        )
        .await;
    });

    // Запускаем сервер
    server::start_server(app, cfg.bind_addr()).await?;

    info!("Server shutdown");
    Ok(())
}
