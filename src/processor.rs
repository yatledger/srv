use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{info, error, debug};

use crate::NodeId;
use crate::raft;
use raft::store::StateMachineStore;
use raft::typ::Raft;
use raft::router::Router;
use crate::server::{HeavyNodesRequest, HeavyNodesResponse};
use rand::seq::SliceRandom;


// Конфигурация для пересчета весов и очистки
const UPDATE_INTERVAL: Duration = Duration::from_secs(1); // Интервал пересчета весов и очистки (1 секунда)
const WEIGHT_THRESHOLD: f64 = 10.0; // Порог веса для удаления узлов
const BATCH_SIZE: usize = 100; // Количество узлов для проверки за один раз

// Запускает фоновую задачу для пересчета весов узлов DAG и очистки узлов с весами выше порога
pub async fn start_processor(sm: Arc<StateMachineStore>, raft: Raft, node_id: NodeId, router: Router) {
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
        let leader_addr = match metrics.membership_config.nodes().find(|(id, _)| **id == leader_id) {
            Some((_, node)) => node.addr.clone(),
            None => {
                error!("Could not find leader address for id {}", leader_id);
                continue;
            }
        };

        let (nodes_to_process, dag) = {
            let state_machine = sm.state_machine.lock().unwrap();
            let mut all_nodes = state_machine.dag.get_node_keys();

            if all_nodes.is_empty() {
                continue;
            }

            // Перемешиваем узлы и выбираем случайный батч
            all_nodes.shuffle(&mut rand::rng());
            all_nodes.truncate(BATCH_SIZE.min(all_nodes.len()));
            
            (all_nodes, state_machine.dag.clone()) // Клонируем DAG, чтобы освободить блокировку
        };

        // НОВЫЙ ШАГ: Фильтруем узлы, чтобы оставить только те, все ИЗНАЧАЛЬНЫЕ родители которых уже удалены.
        let eligible_nodes_for_cleanup: Vec<Arc<str>> = nodes_to_process
            .into_iter()
            .filter(|node_hash| {
                // Получаем данные узла
                if let Some(node) = dag.get_nodes().get(node_hash) {
                    // Извлекаем `prnts` из поля `data`
                    if let Some(prnts_value) = node.data.get("prnts") {
                        if let Some(prnts_array) = prnts_value.as_array() {
                            // Проверяем, что НИ ОДИН изначальный родитель больше НЕ существует в графе.
                            // .all() вернет true, если для всех родителей условие выполняется (т.е. они не найдены).
                            return prnts_array.iter().all(|p_val| {
                                if let Some(p_str) = p_val.as_str() {
                                    !dag.contains_node(&Arc::from(p_str))
                                } else {
                                    true // Пропускаем некорректные записи в `prnts`
                                }
                            });
                        }
                    }
                }
                // Если узел не найден или у него нет поля `prnts`, не включаем его в список на удаление.
                false
            })
            .collect();

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
            info!("Found {} heavy nodes to remove. Sending to leader.", heavy_nodes.len());
            let req = HeavyNodesRequest { nodes: heavy_nodes };

            // Отправляем запрос лидеру
            match router.send::<_, HeavyNodesResponse>(leader_id, leader_addr.clone(), "/remove_heavy_nodes", req).await {
                Ok(res) => {
                    if res.status == "success" {
                        debug!("Successfully submitted heavy nodes to the leader.");
                    } else {
                        error!("Leader failed to process heavy nodes: {:?}", res.message);
                    }
                }
                Err(e) => {
                    error!("Failed to send heavy nodes to leader {}: {}", leader_addr, e);
                }
            }
        }
    }
}