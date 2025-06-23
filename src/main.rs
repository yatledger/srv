//use dagdb::graph::DAG;
//use std::sync::{Arc, RwLock};
use tracing::info;
use tracing_subscriber;

use dagdb::server;
use dagdb::start_raft;
//use dagdb::cleaner;
//use dagdb::updater;
//use dagdb::router::Router;

use clap::Parser;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long)]
    pub id: u64,

    #[clap(long)]
    pub addr: String,

    #[clap(long)]
    pub port: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Настраиваем логирование с фильтром по переменной окружения RUST_LOG
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_ansi(true)
        //.with_target(true)
        //.with_thread_names(true)
        .init();

    // Логируем запуск приложения
    info!("Starting the DAG server");
    
    let options = Opt::parse();
    let http_addr = options.addr.clone() + ":" + &options.port;
    let (_raft, app) = start_raft(options.id, http_addr).await;

    /*// Создаём граф и оборачиваем его в Arc<RwLock>
    let graph = Arc::new(RwLock::new(DAG::new()));

    let cleaner_graph = Arc::clone(&graph);
    tokio::spawn(async move {
        cleaner::start_cleaner(cleaner_graph).await;
    });

    let updater_graph = Arc::clone(&graph);
    tokio::spawn(async move {
        updater::start_weight_updater(updater_graph).await;
    });*/

    // Запускаем сервер
    server::start_server(app, options.addr, options.port).await?;

    info!("Server shutdown");
    Ok(())
}