use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use dagdb::config::{AppConfig, LogFormat};
use dagdb::{processor, server, start_raft};

/// Настраивает подписчик логов: текстовый или JSON (O2), с корреляционными id.
fn init_tracing(cfg: &AppConfig) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match cfg.log_format {
        LogFormat::Json => {
            builder
                .json()
                .flatten_event(true)
                .with_current_span(false)
                .with_span_list(false)
                .init();
        }
        LogFormat::Text => {
            builder
                .with_ansi(true)
                .with_timer(tracing_subscriber::fmt::time::ChronoLocal::new(
                    "%m-%d/%H:%M:%S%.3f".to_string(),
                ))
                .init();
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg = AppConfig::load();
    init_tracing(&cfg);

    // O8: проверяем конфигурацию под выбранный профиль и падаем с понятной ошибкой.
    if let Err(e) = cfg.validate() {
        error!("{e}");
        return Err(e.into());
    }

    info!(
        profile = %cfg.profile,
        format = ?cfg.log_format,
        "Starting the Dag server"
    );

    let (_raft, app) = start_raft(&cfg).await?;

    // Фоновый процессор очистки. Его падение не должно оставаться незамеченным:
    // держим JoinHandle и завершаем узел, если задача неожиданно остановилась.
    let processor_handle = tokio::spawn({
        let processor_app = app.clone();
        async move { processor::start_processor(processor_app).await }
    });

    // O5: ожидаем сигнал остановки (Ctrl-C/SIGTERM) в отдельной задаче.
    let shutdown = app.shutdown.clone();
    tokio::spawn(async move {
        dagdb::shutdown::shutdown_signal(shutdown).await;
    });

    // Запускаем сервер; он работает, пока не остановится сам или не упадёт процессор.
    tokio::select! {
        result = server::start_server(app.clone(), cfg.bind_addr()) => {
            result?;
        }
        result = processor_handle => {
            error!("Processor task terminated unexpectedly: {:?}", result);
            return Err("processor task terminated unexpectedly".into());
        }
    }

    // После остановки HTTP-сервера корректно финализируем Raft (флаш на диск).
    info!("Server stopped, shutting down Raft");
    if let Err(e) = app.raft.shutdown().await {
        error!("Raft shutdown error: {e}");
    }

    info!("Server shutdown");
    Ok(())
}
