use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info};

use crate::NodeId;
use crate::raft;
use crate::server::{HeavyNodesRequest, StandardResponse};
use crate::web::{ApiRouterError, Router};
use raft::store::StateMachineStore;
use raft::typ::Raft;
use rand::seq::SliceRandom;

// Конфигурация для пересчета весов и очистки
const UPDATE_INTERVAL: Duration = Duration::from_millis(250); // Интервал пересчета весов и очистки (1 секунда)
const WEIGHT_THRESHOLD: f64 = 5.0; // Порог веса для удаления узлов
const BATCH_SIZE: usize = 100; // Количество узлов для проверки за один раз

// Запускает фоновую задачу для пересчета весов узлов DAG и очистки узлов с весами выше порога
pub async fn start_processor(
    sm: Arc<StateMachineStore>,
    raft: Raft,
    node_id: NodeId,
    router: Router,
) {
    // Создаем интервал для периодического запуска
    let mut interval = time::interval(UPDATE_INTERVAL);

    loop {
        interval.tick().await;

        let metrics = raft.metrics().borrow().clone();

        // Обработку выполняет только follower
        if metrics.current_leader == Some(node_id) {
            continue;
        }

        let leader_id = match metrics.current_leader {
            Some(id) => id,
            None => {
                debug!("No leader found, skipping processing cycle.");
                continue; // Нет лидера, пропускаем итерацию
            }
        };

        // Находим адрес лидера в конфигурации
        let leader_addr = match metrics
            .membership_config
            .nodes()
            .find(|(id, _)| **id == leader_id)
        {
            Some((_, node)) => node.addr.clone(),
            None => {
                error!("Could not find leader address for id {}", leader_id);
                continue;
            }
        };

        let (nodes_to_process, dag) = {
            let state_machine = sm.state_machine.read().await;
            let all_nodes = state_machine.dag.get_node_keys();

            if all_nodes.is_empty() {
                continue;
            }

            (all_nodes, state_machine.dag.clone()) // TODO: сделать так везде. Клонируем DAG, чтобы освободить блокировку
        };

        // TODO подумать как везде вместо clone юзать ссылку
        // Фильтруем узлы, чтобы оставить только те, все ИЗНАЧАЛЬНЫЕ родители которых уже удалены.
        let mut eligible_nodes_for_cleanup: Vec<Arc<str>> = nodes_to_process
            .clone()
            .into_iter()
            .filter(|node_hash| {
                // Проверяем, существует ли узел в DAG
                if let Some(node) = dag.get_nodes().get(node_hash) {
                    // Узел подходит для удаления, если у него нет активных родителей
                    node.parents.is_empty()
                } else {
                    // Если узел не найден, исключаем его
                    false
                }
            })
            .collect();

        // Перемешиваем узлы и выбираем случайный батч
        let eligible_len = eligible_nodes_for_cleanup.len();
        eligible_nodes_for_cleanup.shuffle(&mut rand::rng());
        eligible_nodes_for_cleanup.truncate(BATCH_SIZE.min(eligible_len));

        if eligible_nodes_for_cleanup.is_empty() {
            continue; // Нет узлов, готовых к удалению, в этом батче
        }

        // Вычисляем веса для отфильтрованного списка узлов
        let weights = dag.compute_weights_for_batch(&eligible_nodes_for_cleanup);

        // Фильтруем узлы, вес которых превышает порог
        let heavy_nodes: Vec<Arc<str>> = weights
            .into_iter()
            .filter_map(|(node, weight)| {
                if weight > WEIGHT_THRESHOLD {
                    Some(node)
                } else {
                    None
                }
            })
            .collect();

        // Если найдены "тяжелые" узлы, отправляем их лидеру для удаления
        if !heavy_nodes.is_empty() {
            let req = HeavyNodesRequest {
                nodes: heavy_nodes.clone(),
            };

            // Отправляем запрос лидеру
            match router
                .send::<_, StandardResponse>(&leader_addr, "/remove_heavy_nodes", req)
                .await
            {
                Ok(res) => {
                    debug!(
                        "Successfully submitted heavy nodes to the leader: {:?}",
                        res.message
                    );
                }
                Err(e) => match e {
                    ApiRouterError::Network(net_err) => {
                        error!("Network error while trying to contact leader: {}", net_err);
                    }
                    ApiRouterError::Http { status, text } => {
                        error!(
                            "Leader returned an HTTP error {} with body: {}",
                            status, text
                        );
                    }
                    ApiRouterError::Api { message } => {
                        error!("Leader API returned an error: {}", message);
                    }
                    ApiRouterError::Deserialization(serde_err) => {
                        error!(
                            "Failed to deserialize a response from the leader: {}",
                            serde_err
                        );
                    }
                },
            }
        }
        info!(
            "{} / {} / {}",
            nodes_to_process.len(),
            eligible_len,
            heavy_nodes.len()
        );
    }
}
