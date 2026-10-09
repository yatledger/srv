use tracing::{error, info};
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

    // Фоновый процессор очистки. Его падение не должно оставаться незамеченным:
    // держим JoinHandle и завершаем узел, если задача неожиданно остановилась.
    let processor_handle = tokio::spawn({
        let processor_app = app.clone();
        async move { processor::start_processor(processor_app).await }
    });

    // Запускаем сервер; он работает, пока не остановится сам или не упадёт процессор.
    tokio::select! {
        result = server::start_server(app, cfg.bind_addr()) => {
            result?;
        }
        result = processor_handle => {
            error!("Processor task terminated unexpectedly: {:?}", result);
            return Err("processor task terminated unexpectedly".into());
        }
    }

    info!("Server shutdown");
    Ok(())
}
