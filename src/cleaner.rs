use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::time; // Для создания периодического таймера
use crate::graph::DAG; // Импортируем структуру DAG из модуля graph

// Конфигурация для очистителя
const WEIGHT_THRESHOLD: f64 = 10.0; // Порог веса для удаления узлов
const CLEAN_INTERVAL: u64 = 1000; // Интервал очистки в милисекундах

/// Запускает фоновую задачу для периодической очистки узлов с весами выше порога.
/// Использует пул потоков Rayon для параллельного удаления узлов и Tokio для асинхронного таймера.
/// 
/// # Аргументы
/// * `graph` - Потокобезопасный граф, обёрнутый в Arc<RwLock<DAG>>.
/// 
/// # Логика
/// 1. Создаётся периодический таймер с интервалом CLEAN_INTERVAL секунд.
/// 2. При каждом тике таймера:
///    - Получаются текущие веса узлов.
///    - Фильтруются узлы с весами выше порога WEIGHT_THRESHOLD.
///    - Удаление узлов распределяется по пулу потоков Rayon для параллельной обработки.
/// 3. Каждый поток безопасно получает доступ к графу через RwLock для удаления узлов.
/// 
/// # Замечания
/// - Rayon автоматически распределяет задачи по доступным ядрам процессора.
/// - RwLock обеспечивает безопасный доступ к графу для чтения и записи.
/// - Ошибки блокировки обрабатываются с возвратом логов.
pub async fn start_cleaner(graph: Arc<RwLock<DAG>>) {
    // Создаём периодический таймер с интервалом CLEAN_INTERVAL секунд
    let mut interval = time::interval(Duration::from_millis(CLEAN_INTERVAL));
    
    loop {
        // Ожидаем следующий тик таймера
        interval.tick().await;
        //println!("Cleaner task started at {:?}", std::time::Instant::now());

        // Выводим общее количество узлов в графе
        let total_nodes = {
            let graph_read = match graph.read() {
                Ok(guard) => guard,
                Err(e) => {
                    eprintln!("Failed to lock graph for reading: {}", e);
                    continue;
                }
            };
            graph_read.get_node_count()
        };
        println!("Total nodes in graph: {}", total_nodes);

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
                .collect::<Vec<String>>()
        };

        if nodes_to_remove.is_empty() {
            println!("No nodes to remove");
            continue;
        }

        // Вызываем remove_nodes для параллельного удаления
        match DAG::remove_nodes(Arc::clone(&graph), nodes_to_remove.clone()) {
            Ok(()) => println!("Successfully removed nodes {} nodes", nodes_to_remove.len()),
            Err(e) => eprintln!("Failed to remove nodes: {}", e),
        }
    }
}