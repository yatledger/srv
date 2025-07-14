//use dagdb::graph::DAG;
use std::sync::{Arc};
use tracing::info;
use tracing_subscriber;

use dagdb::server;
use dagdb::start_raft;
use dagdb::processor;
use dagdb::web::Router;

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

    let router = Router::new();

    let processor_sm = Arc::clone(&app.state_machine);
    let processor_raft = app.raft.clone();
    let processor_node_id = options.id;
    let processor_router = router.clone();

    tokio::spawn(async move {
        processor::start_processor(processor_sm, processor_raft, processor_node_id, processor_router).await;
    });

    // Запускаем сервер
    server::start_server(app, options.addr, options.port).await?;

    info!("Server shutdown");
    Ok(())
}