use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time;
use tracing::{info, error, debug};
use std::collections::{HashMap};
use tokio::time::timeout;
use redis::aio::MultiplexedConnection;
use redis::pipe;

use crate::NodeId;
use crate::raft;
use raft::store::StateMachineStore;
use raft::typ::Raft;
use raft::router::Router;
use raft::command::{ComputeWeightsRequest, SubmitWeightsResponse, Request};

// Конфигурация для пересчета весов и очистки
const UPDATE_INTERVAL: Duration = Duration::from_secs(5); // Интервал пересчета весов и очистки (5 секунд)
const REQUEST_TIMEOUT: Duration = Duration::from_secs(3); // Тайм-аут для ответа от follower'ов
const WEIGHT_THRESHOLD: f64 = 10.0; // Порог веса для удаления узлов

// Запускает фоновую задачу для пересчета весов узлов DAG и очистки узлов с весами выше порога
pub async fn start_processor(sm: Arc<StateMachineStore>, raft: Raft, node_id: NodeId, router: Router) {
    // Создаем интервал для периодического запуска
    let mut interval = time::interval(UPDATE_INTERVAL);
    let mut last_cleanup_time = Instant::now(); // Время последней очистки

    // Настраиваем подключение к Redis
    let redis_url = "redis://:REDACTED_ROTATED_SECRET@localhost/0";
    let redis_client = redis::Client::open(redis_url).expect("Failed to create Redis client");
    let mut con: MultiplexedConnection = redis_client
        .get_multiplexed_tokio_connection()
        .await
        .expect("Failed to connect to Redis");

    loop {
        interval.tick().await;

        let metrics = raft.metrics().borrow().clone();

        if metrics.current_leader != Some(node_id) {
            debug!("Not a leader (id: {}), skipping processing", node_id);
            continue;
        }

        // Получаем список активных узлов кластера
        let active_nodes: Vec<(NodeId, String)> = metrics
            .membership_config
            .nodes()
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect();
        let num_nodes = active_nodes.len();
        if num_nodes == 0 {
            error!("No active nodes in cluster, skipping processing");
            continue;
        }

        let nodes: Vec<Arc<str>> = {
            let state_machine = sm.state_machine.lock().unwrap();
            state_machine.dag.get_node_keys()
        };

        if nodes.is_empty() {
            debug!("No nodes in DAG, skipping processing");
            continue;
        }
        // Разделяем узлы DAG на подмножества для каждого узла кластера
        let chunk_size = (nodes.len() + num_nodes - 1) / num_nodes; // Округляем вверх
        let chunks: Vec<&[Arc<str>]> = nodes.chunks(chunk_size).collect();
        let mut assignments: HashMap<NodeId, Vec<Arc<str>>> = HashMap::new();
        for (i, (node_id, _)) in active_nodes.iter().enumerate() {
            assignments.insert(
                *node_id,
                chunks.get(i).map(|chunk| chunk.to_vec()).unwrap_or_default(),
            );
        }

        info!(
            "Distributing {} nodes across {} cluster nodes",
            nodes.len(),
            num_nodes
        );

        // Получаем текущую версию DAG из метрик Raft
        let dag_version = metrics.last_log_index.unwrap_or(0);

        // Собираем результаты вычислений
        let mut all_weights: HashMap<Arc<str>, f64> = HashMap::new();
        //let mut rng = ThreadRng::default();

        // Отправляем запросы на пересчет весов каждому узлу
        let mut futures = Vec::new();
        for (node_id, addr) in active_nodes {
            if let Some(node_list) = assignments.get(&node_id) {
                if node_list.is_empty() {
                    continue;
                }
                // Формируем запрос для пересчета весов
                let req = ComputeWeightsRequest {
                    nodes: node_list.clone(),
                    dag_version,
                };
                // Отправляем асинхронный запрос через router
                let future = router.send::<_, SubmitWeightsResponse>(node_id, addr.clone(), "/compute_weights", req);
                futures.push((node_id, future));
            }
        }

        for (node_id, future) in futures {
            match timeout(REQUEST_TIMEOUT, future).await {
                Ok(Ok(response)) => {
                    let response: SubmitWeightsResponse = response;
                    // Проверяем согласованность версии DAG
                    if response.dag_version < dag_version {
                        error!(
                            "Received outdated weights from node {}: version {} < {}",
                            node_id, response.dag_version, dag_version
                        );
                        continue;
                    }
                    // Добавляем полученные веса в общую коллекцию
                    for node_weight in response.node_weights {
                        all_weights.insert(node_weight.node, node_weight.weight);
                    }
                }
                Ok(Err(e)) => {
                    error!("Failed to get weights from node {}: {}", node_id, e);
                    // Пересчитываем локально для узлов, которые должен был обработать этот узел
                    if let Some(failed_nodes) = assignments.get(&node_id) {
                        let state_machine = sm.state_machine.lock().unwrap();
                        let local_weights = state_machine.dag.compute_weights_for_batch(failed_nodes);
                        all_weights.extend(local_weights);
                    }
                }
                Err(_) => {
                    error!("Timeout waiting for weights from node {}", node_id);
                    // Пересчитываем локально
                    if let Some(failed_nodes) = assignments.get(&node_id) {
                        let state_machine = sm.state_machine.lock().unwrap();
                        let local_weights = state_machine.dag.compute_weights_for_batch(failed_nodes);
                        all_weights.extend(local_weights);
                    }
                }
            }
        }
        // Собираем узлы для архивирования (веса выше порога)
        let nodes_to_archive = {
            let state_machine = sm.state_machine.lock().unwrap();
            all_weights
            .iter()
            .filter(|(_, weight)| **weight > WEIGHT_THRESHOLD)
            .filter_map(|(node, _)| {
                // Получаем данные узла для сохранения в Redis
                if let Some(data) = state_machine.dag.get_node_data(node) {
                    Some((node.clone(), data.to_string()))
                } else {
                    None
                }
            })
            .collect::<Vec<(Arc<str>, String)>>()
        };
        // Записываем данные узлов в Redis перед удалением
        let mut redis_pipe = pipe();
        for (node_name, node_data) in &nodes_to_archive {
            let redis_key = format!("confirmed:{}", node_name.as_ref());
            redis_pipe.set(redis_key, node_data.clone()).ignore();
        }

        let redis_result: redis::RedisResult<()> = redis_pipe.query_async(&mut con).await;

        match redis_result {
            Ok(_) => {
                // Формируем команды Remove для Raft
                let nodes_to_remove: Vec<Arc<str>> = nodes_to_archive.iter().map(|(node, _)| node.clone()).collect();
                for node in &nodes_to_remove {
                    let request = Request::Remove { hash: node.clone() };
                    match raft.client_write(request).await {
                        Ok(_) => {
                            debug!("Sent Remove command for node {}", node);
                        }
                        Err(e) => {
                            error!("Failed to send Remove command for node {}: {}", node, e);
                            continue;
                        }
                    }
                }

                // Логируем результаты очистки
                let current_time = Instant::now();
                let time_since_last = current_time.duration_since(last_cleanup_time);
                last_cleanup_time = current_time;
                let state_machine = sm.state_machine.lock().unwrap();
                let remaining_nodes = state_machine.dag.get_node_count();
                info!(
                    "Removed: {}. Remaining: {}. Time: {:.2}s",
                    nodes_to_archive.len(),
                    remaining_nodes,
                    time_since_last.as_secs_f64()
                );
            }
            Err(e) => {
                error!("Failed to write batch to Redis: {}. Nodes will not be removed in this cycle.", e);
            }
        }
        // info!("metrics: {:?}", metrics);
    }
     
}