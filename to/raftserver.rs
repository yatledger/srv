// HTTP API handlers (если нужно)
#[cfg(feature = "http")]
pub mod http {
    use super::*;
    use warp::{Filter, Reply};
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct AddNodeRequest {
        pub hash: String,
        pub parents: Vec<String>,
        pub data: String,
    }

    #[derive(Deserialize)]
    pub struct SetWeightRequest {
        pub hash: String,
        pub weight: f64,
    }

    pub fn create_routes(service: Arc<DagService>) -> impl Filter<Extract = impl Reply, Error = warp::Rejection> + Clone {
        let add_node = warp::path("add")
            .and(warp::post())
            .and(warp::body::json())
            .and(with_service(service.clone()))
            .and_then(handle_add_node);

        let remove_node = warp::path("remove")
            .and(warp::post())
            .and(warp::body::json())
            .and(with_service(service.clone()))
            .and_then(handle_remove_node);

        let set_weight = warp::path("weight")
            .and(warp::post())
            .and(warp::body::json())
            .and(with_service(service.clone()))
            .and_then(handle_set_weight);

        let metrics = warp::path("metrics")
            .and(warp::get())
            .and(with_service(service.clone()))
            .and_then(handle_metrics);

        add_node.or(remove_node).or(set_weight).or(metrics)
    }

    fn with_service(service: Arc<DagService>) -> impl Filter<Extract = (Arc<DagService>,), Error = std::convert::Infallible> + Clone {
        warp::any().map(move || service.clone())
    }

    async fn handle_add_node(req: AddNodeRequest, service: Arc<DagService>) -> Result<impl Reply, warp::Rejection> {
        match service.add_node(req.hash, req.parents, req.data).await {
            Ok(response) => Ok(warp::reply::json(&response)),
            Err(e) => {
                eprintln!("Error adding node: {}", e);
                Err(warp::reject())
            }
        }
    }

    async fn handle_remove_node(req: serde_json::Value, service: Arc<DagService>) -> Result<impl Reply, warp::Rejection> {
        let hash = req["hash"].as_str().unwrap_or("").to_string();
        match service.remove_node(hash).await {
            Ok(response) => Ok(warp::reply::json(&response)),
            Err(e) => {
                eprintln!("Error removing node: {}", e);
                Err(warp::reject())
            }
        }
    }

    async fn handle_set_weight(req: SetWeightRequest, service: Arc<DagService>) -> Result<impl Reply, warp::Rejection> {
        match service.set_weight(req.hash, req.weight).await {
            Ok(response) => Ok(warp::reply::json(&response)),
            Err(e) => {
                eprintln!("Error setting weight: {}", e);
                Err(warp::reject())
            }
        }
    }

    async fn handle_metrics(service: Arc<DagService>) -> Result<impl Reply, warp::Rejection> {
        let metrics = service.get_metrics().await;
        Ok(warp::reply::json(&metrics))
    }
}

// Пример использования в main.rs
#[cfg(feature = "example")]
pub async fn example_usage() -> Result<(), Box<dyn std::error::Error>> {
    use crate::graph::dag::DAG;
    
    // Создаем DAG
    let dag = Arc::new(RwLock::new(DAG::new()));
    
    // Создаем сервис с Raft
    let service = create_dag_service(dag).await?;
    
    // Тестируем команды
    println!("Adding node...");
    let response = service.add_node("node1".to_string(), vec![], "test data".to_string()).await?;
    println!("Response: {:?}", response);
    
    println!("Setting weight...");
    let response = service.set_weight("node1".to_string(), 1.5).await?;
    println!("Response: {:?}", response);
    
    // Запускаем HTTP сервер (если нужно)
    #[cfg(feature = "http")]
    {
        let service_arc = Arc::new(service);
        let routes = http::create_routes(service_arc);
        
        println!("Starting HTTP server on 127.0.0.1:3030");
        warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
    }
    
    Ok(())
}