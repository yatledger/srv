use std::sync::{Arc};
use std::time::{Duration, Instant};
use tokio::time; // Для асинхронного sleep
use rand;
use crate::graph::DAG;
use crate::DagDb;

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
pub async fn start_cleaner(graph: DagDb) {
    let mut last_cleanup_time = Instant::now(); // Время последней очистки

    loop {
        // Минимальная задержка между итерациями
        time::sleep(Duration::from_millis(CHECK_INTERVAL_MS)).await;

        // Получаем блокировку графа для чтения весов
        let nodes_to_remove = {
            let graph_read = match graph.read() {
                Ok(guard) => guard,
                Err(e) => {
                    eprintln!("Failed to lock graph for reading: {}", e);
                    continue;
                }
            };

            // Фильтруем узлы с весами выше порога
            graph_read
                .get_weights()
                .iter()
                .filter(|node| node.weight > WEIGHT_THRESHOLD)
                .map(|node| node.node.clone())
                .collect::<Vec<Arc<str>>>()
        };

        // Проверяем, нужно ли запускать очистку
        let should_clean = rand::random::<f64>() < PROBABILITY || nodes_to_remove.len() >= NODE_COUNT_THRESHOLD;
        if !should_clean {
            continue;
        }

        if nodes_to_remove.is_empty() {
            println!("No nodes to remove");
            continue;
        }

        // Вызываем remove_nodes для параллельного удаления
        match DAG::remove_nodes(Arc::clone(&graph), nodes_to_remove.clone()) {
            Ok(()) => {
                // Вычисляем время с момента последней очистки
                let current_time = Instant::now();
                let time_since_last = current_time.duration_since(last_cleanup_time);
                last_cleanup_time = current_time;
                // Получаем актуальное количество узлов после удаления
                let remaining_nodes = {
                    match graph.read() {
                        Ok(guard) => guard.get_node_count(),
                        Err(_) => 0, // Если не удалось получить блокировку, показываем 0
                    }
                };
                println!("Removed {}. Remaining: {}. Time: {:.2}s", 
                         nodes_to_remove.len(), remaining_nodes, time_since_last.as_secs_f64());
            },
            Err(e) => eprintln!("Failed to remove nodes: {}", e),
        }
    }
}