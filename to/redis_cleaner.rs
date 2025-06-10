use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::time; // Для асинхронного sleep
use rand;
use redis::aio::MultiplexedConnection;
use redis::{pipe};
use crate::graph::DAG;

// Конфигурация для очистителя
const WEIGHT_THRESHOLD: f64 = 25.0; // Порог веса для удаления узлов
const CHECK_INTERVAL_MS: u64 = 25; // Минимальная задержка между проверками (в миллисекундах)
const PROBABILITY: f64 = 0.25; // Вероятность запуска очистки (10%)
const NODE_COUNT_THRESHOLD: usize = 25; // Порог количества узлов для гарантированной очистки

/// Запускает фоновую задачу для очистки узлов с весами выше порога.
/// Использует вероятностный подход и порог по количеству узлов для удаления.
/// 
/// # Аргументы
/// * `graph` - Потокобезопасный граф, обёрнутый в Arc<RwLock<DAG>>.
/// 
/// # Логика
/// 1. В бесконечном цикле выполняется минимальная задержка (CHECK_INTERVAL_MS).
/// 2. Получается список узлов для удаления с весами выше WEIGHT_THRESHOLD.
/// 3. Очистка запускается, если:
///    - Случайное число < PROBABILITY (10%).
///    - Количество узлов для удаления >= NODE_COUNT_THRESHOLD.
/// 4. При очистке:
///    - Удаление узлов выполняется параллельно через Rayon.
/// 5. Если узлов для удаления нет, выводится сообщение, и цикл продолжается.
/// 
/// # Замечания
/// - Минимальная задержка (CHECK_INTERVAL_MS) предотвращает чрезмерную нагрузку на CPU.
/// - Порог NODE_COUNT_THRESHOLD делает очистку адаптивной к состоянию графа.
/// - RwLock обеспечивает безопасный доступ к графу.
pub async fn start_cleaner(graph: Arc<RwLock<DAG>>) {
    let mut last_cleanup_time = Instant::now(); // Время последней очистки
    // Set up Redis connection
    let redis_url = "redis://:REDACTED_ROTATED_SECRET@localhost/0";
    let redis_client = redis::Client::open(redis_url).expect("Failed to create Redis client");
    let mut con: MultiplexedConnection = redis_client
        .get_multiplexed_tokio_connection()
        .await
        .expect("Failed to connect to Redis");

    loop {
        // Минимальная задержка между итерациями
        time::sleep(Duration::from_millis(CHECK_INTERVAL_MS)).await;

        let nodes_to_archive = {
            let graph_read = match graph.read() {
                Ok(guard) => guard,
                Err(e) => {
                    eprintln!("Failed to lock graph for reading: {}", e);
                    continue;
                }
            };

            // Собираем узлы, которые превышают порог веса.
            // Вместе с именем узла сразу получаем его данные.
            graph_read
                .get_weights()
                .iter()
                .filter(|node| node.weight > WEIGHT_THRESHOLD)
                .filter_map(|node_info| {
                    // Для каждого подходящего узла, получаем его данные
                    // Предполагается, что у `graph_read` есть метод `get_node_data`
                    if let Some(data) = graph_read.get_node_data(&node_info.node) {
                        Some((node_info.node.clone(), data.to_string()))
                    } else {
                        None // Если данных нет, узел не будет удален
                    }
                })
                .collect::<Vec<(Arc<str>, String)>>() // Собираем пары (имя, данные)
        };

        // Проверяем, нужно ли запускать очистку
        let should_clean = rand::random::<f64>() < PROBABILITY || nodes_to_archive.len() >= NODE_COUNT_THRESHOLD;
        if !should_clean || nodes_to_archive.is_empty() {
            if nodes_to_archive.is_empty() && should_clean {
                 println!("No nodes to remove");
            }
            continue;
        }

        let mut redis_pipe = pipe();
        for (node_name, node_data) in &nodes_to_archive {
            // Команда SET для каждого узла: ключ - имя узла, значение - его данные
            redis_pipe.set(node_name.as_ref(), node_data.clone()).ignore();
        }

        let redis_result: redis::RedisResult<()> = redis_pipe.query_async(&mut con).await;

        // Вызываем remove_nodes для параллельного удаления
        match redis_result {
            Ok(_) => {
                // Запись в Redis прошла успешно, теперь удаляем узлы из графа
                // println!("Successfully wrote {} nodes to Redis. Proceeding with deletion.", nodes_to_archive.len());
                
                // Получаем только имена узлов для функции удаления
                let nodes_to_remove = nodes_to_archive.iter().map(|(name, _)| name.clone()).collect();

                match DAG::remove_nodes(Arc::clone(&graph), nodes_to_remove) {
                    Ok(()) => {
                        let current_time = Instant::now();
                        let time_since_last = current_time.duration_since(last_cleanup_time);
                        last_cleanup_time = current_time;
                        
                        let remaining_nodes = graph.read().map_or(0, |g| g.get_node_count());
                        
                        println!("Removed {}. Remaining: {}. Time since last cleanup: {:.2}s", 
                                 nodes_to_archive.len(), remaining_nodes, time_since_last.as_secs_f64());
                    },
                    Err(e) => eprintln!("CRITICAL: Failed to remove nodes from graph AFTER writing to Redis: {}", e),
                }
            },
            Err(e) => {
                // Ошибка записи в Redis. Ничего не удаляем из графа, чтобы не потерять данные.
                eprintln!("Failed to write batch to Redis: {}. Nodes will not be removed in this cycle.", e);
            }
        }
    }
}