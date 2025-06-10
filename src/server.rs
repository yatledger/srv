// Импортируем необходимые зависимости для создания HTTP-сервера.
use axum::{
    extract::{Json, State}, // Для извлечения JSON и состояния из запроса.
    http::StatusCode, // Для работы с HTTP-статусами.
    routing::{get, post}, // Для создания POST-эндпоинта.
    Router, // Основной тип для маршрутизации запросов.
};
use serde::{Deserialize, Serialize}; // Для сериализации/десериализации JSON.
use serde_json::Value;
use std::collections::HashMap; // Для возврата графа в JSON.
use std::sync::Arc; // Для безопасного разделения графа между потоками.
use tokio::net::TcpListener; // Для запуска асинхронного TCP-сервера.
use rand::seq::SliceRandom; // Для перемешивания финального списка узлов.
use rand::rng; // Для генерации случайности.
use tracing::{info, error, debug};

// Импортируем структуру DAG из вашего модуля graph.rs.
use crate::DagDb;

// Определяем структуру для десериализации JSON-запроса.
// Она соответствует данным, которые клиент отправляет в POST-запросе на /add_node.
#[derive(Deserialize)]
struct AddNodeRequest {
    hash: String, // Хэш нового узла.
    parents: Vec<String>, // Список хэшей родительских узлов.
    data: Value,
}

// Определяем структуру для сериализации ответа клиенту.
#[derive(Serialize)]
struct AddNodeResponse {
    status: String, // Статус операции: "success" или "error".
    message: Option<String>, // Сообщение об ошибке (если есть).
}

// Структура для ответа с графом.
#[derive(Serialize)]
struct GraphResponse {
    status: String, // Статус операции: "success" или "error".
    graph: Option<HashMap<String, Vec<String>>>, // Список смежности графа.
    message: Option<String>, // Сообщение об ошибке (если есть).
}

#[derive(Serialize)]
struct PoolResponse {
    status: String, // Статус операции: "success" или "error".
    nodes: Vec<String>, // Обрезанный список хэшей узлов.
    message: Option<String>, // Сообщение об ошибке (если есть).
}

#[derive(Serialize)]
struct NodeWeight {
    hash: String,
    weight: f64,
    data: Value,
}

#[derive(Serialize)]
struct WeightsResponse {
    status: String,
    nodes: Vec<NodeWeight>,
    message: Option<String>,
}

// Новая структура для представления узла с его потомками.
#[derive(Serialize)]
struct NodeFullInfo {
    hash: String, // Хэш узла.
    weight: f64, // Финальный вес узла.
    descendants: Vec<DescendantInfo>, // Список потомков с глубиной и весом.
}

// Новая структура для представления потомка.
#[derive(Serialize)]
struct DescendantInfo {
    hash: String, // Хэш потомка.
    depth: usize, // Глубина относительно родителя.
    weight: f64, // Вес потомка.
}

// Новая структура для ответа /full_graph.
#[derive(Serialize)]
struct FullGraphResponse {
    status: String,
    nodes: Vec<NodeFullInfo>,
    message: Option<String>,
}

#[derive(Deserialize)]
struct RemoveRequest {
    hashes: Vec<String>, // Список хэшей узлов для удаления.
}

#[derive(Serialize)]
struct RemoveResponse {
    status: String,
    message: Option<String>,
}

// Определяем асинхронный обработчик POST-запроса на /add_node.
// Принимает JSON с данными запроса и состояние приложения (граф).
async fn add_handler(
    State(graph): State<DagDb>, // Извлекаем граф, защищённый Arc и RwLock для потокобезопасности.
    Json(payload): Json<AddNodeRequest>, // Извлекаем JSON-данные из тела запроса.
) -> (StatusCode, Json<AddNodeResponse>) {
    debug!("Starting add_handler for node: {}", payload.hash);
    // Получаем блокировку графа для безопасного доступа.
    // RwLock обеспечивает синхронизацию между потоками.
    // let start = std::time::Instant::now();
    let mut graph = match graph.write() {
        Ok(guard) => guard,
        Err(_) => {
            // Если не удалось получить блокировку, возвращаем ошибку сервера.
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AddNodeResponse {
                    status: "error".to_string(),
                    message: Some("Failed to lock graph".to_string()),
                }),
            );
        }
    };
    // let duration = start.elapsed(); // Вычисляем время выполнения.
    // println!("add_node_handler took {} ms", duration.as_millis());
    // Преобразуем String в Arc<str>
    let hash = Arc::from(payload.hash.as_str());
    let parents = payload.parents.into_iter().map(|s| Arc::from(s.as_str())).collect();
    let data = Arc::new(payload.data);
    // Вызываем метод add_node_with_parents на графе.
    let result = match graph.add_node_with_parents(hash, parents, data) {
        Ok(()) => (
            // Успешное добавление узла.
            StatusCode::OK,
            Json(AddNodeResponse {
                status: "success".to_string(),
                message: None,
            }),
        ),
        Err(err) => (
            // Ошибка при добавлении узла (например, узел уже существует или цикл).
            StatusCode::BAD_REQUEST,
            Json(AddNodeResponse {
                status: "error".to_string(),
                message: Some(err),
            }),
        ),
    };

    debug!("Finished add_handler for node: {}, status: {}", payload.hash, result.1 .0.status);
    result
}

async fn pool_handler(
    State(graph): State<DagDb>,
) -> (StatusCode, Json<PoolResponse>) {
    // Получаем блокировку графа для безопасного доступа.
    let graph = match graph.read() {
        Ok(guard) => guard,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(PoolResponse {
                    status: "error".to_string(),
                    nodes: vec![],
                    message: Some("Failed to lock graph".to_string()),
                }),
            );
        }
    };

    // Получаем веса узлов из графа.
    let mut nodes = graph.get_weights().clone();
    // Сортируем узлы по возрастанию веса.
    nodes.sort_by(|a, b| a.weight.partial_cmp(&b.weight).unwrap_or(std::cmp::Ordering::Equal));

    // Извлекаем только хэши узлов.
    let mut nodes: Vec<String> = nodes.into_iter().map(|node| String::from(&*node.node)).collect();
    // Выполняем обрезку и перемешивание только если nodes.len() > 10.
    if nodes.len() > 10 {
        // Обрезаем список до nodes.len() / 3.
        let first_truncate_len = nodes.len() / 3;
        nodes.truncate(first_truncate_len);
        // Перемешиваем список узлов.
        nodes.shuffle(&mut rng());
        // Вычисляем длину обрезанного списка как округлённый квадратный корень от числа узлов.
        let target_len = (nodes.len() as f64).sqrt().ceil() as usize;
        // Обрезаем список до target_len, если он длиннее.
        nodes.truncate(target_len);
    }

    // Возвращаем ответ с обрезанным списком узлов.
    (
        StatusCode::OK,
        Json(PoolResponse {
            status: "success".to_string(),
            nodes,
            message: None,
        }),
    )
}

async fn get_weights_handler(State(graph): State<DagDb>) -> (StatusCode, Json<WeightsResponse>) {
    let graph = match graph.read() {
        Ok(guard) => guard,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(WeightsResponse {
                    status: "error".to_string(),
                    nodes: vec![],
                    message: Some("Failed to lock graph".to_string()),
                }),
            );
        }
    };
    let weights = graph.get_weights();
    let nodes = weights
        .into_iter()
        .map(|node| {
            let data = graph
                .get_node_data(&node.node)
                .cloned()
                .unwrap_or(Value::Null);
            NodeWeight {
                hash: String::from(&*node.node),
                weight: node.weight,
                data,
            }
        })
        .collect();
    (
        StatusCode::OK,
        Json(WeightsResponse {
            status: "success".to_string(),
            nodes,
            message: None,
        }),
    )
}

// Обработчик для POST-запроса на /remove.
// Пакетно даляет список узлов из графа.
async fn remove_handler(
    State(graph): State<DagDb>,
    Json(payload): Json<RemoveRequest>,
) -> (StatusCode, Json<RemoveResponse>) {
    debug!("Starting remove_handler for nodes: {:?}", payload.hashes);
    // Получаем блокировку графа для чтения
    let existing_nodes = {
        // Получаем блокировку для чтения в отдельном блоке
        let graph_read = match graph.read() {
            Ok(guard) => guard,
            Err(_) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(RemoveResponse {
                        status: "error".to_string(),
                        message: Some("Failed to lock graph".to_string()),
                    }),
                );
            }
        };
        //debug!("1");
        
        // Фильтруем существующие узлы
        let nodes: Vec<Arc<str>> = payload
            .hashes
            .into_iter()
            .filter(|hash| graph_read.contains_node(hash))
            .map(|hash| Arc::from(hash.as_str()))
            .collect();
        
        nodes
        // Блокировка для чтения автоматически освобождается здесь
    };
    //debug!("2");

    // Если нет существующих узлов, возвращаем ошибку
    if existing_nodes.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(RemoveResponse {
                status: "error".to_string(),
                message: Some("No existing nodes provided".to_string()),
            }),
        );
    }
    //debug!("3");

    let mut graph_write = match graph.write() {
        Ok(guard) => guard,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(RemoveResponse {
                    status: "error".to_string(),
                    message: Some("Failed to lock graph for writing".to_string()),
                }),
            );
        }
    };
    //debug!("4");
    // Пакетное удаление узлов
    let result = match graph_write.remove_nodes(existing_nodes) {
        Ok(()) => (
            StatusCode::OK,
            Json(RemoveResponse {
                status: "success".to_string(),
                message: None,
            }),
        ),
        Err(err) => (
            StatusCode::BAD_REQUEST,
            Json(RemoveResponse {
                status: "error".to_string(),
                message: Some(err),
            }),
        ),
    };

    debug!("Finished remove_handler, status: {}", result.1 .0.status);
    result
}

async fn get_childrens_handler(
    State(graph): State<DagDb>,
) -> (StatusCode, Json<GraphResponse>) {
    // Получаем блокировку графа для безопасного доступа.
    let graph = match graph.read() {
        Ok(guard) => guard,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GraphResponse {
                    status: "error".to_string(),
                    graph: None,
                    message: Some("Failed to lock graph".to_string()),
                }),
            );
        }
    };
    let graph_data = graph
        .get_childrens()
        .iter()
        .map(|(k, v)| (String::from(&**k), v.iter().map(|s| String::from(&**s)).collect()))
        .collect();
    (
        StatusCode::OK,
        Json(GraphResponse {
            status: "success".to_string(),
            graph: Some(graph_data),
            message: None,
        }),
    )
}

async fn get_parents_handler(
    State(graph): State<DagDb>,
) -> (StatusCode, Json<GraphResponse>) {
    // Получаем блокировку графа для безопасного доступа.
    let graph = match graph.read() {
        Ok(guard) => guard,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GraphResponse {
                    status: "error".to_string(),
                    graph: None,
                    message: Some("Failed to lock graph".to_string()),
                }),
            );
        }
    };
    let graph_data = graph
        .get_parents()
        .iter()
        .map(|(k, v)| (String::from(&**k), v.iter().map(|s| String::from(&**s)).collect()))
        .collect();
    (
        StatusCode::OK,
        Json(GraphResponse {
            status: "success".to_string(),
            graph: Some(graph_data),
            message: None,
        }),
    )
}

async fn get_full_graph_handler(
    State(graph): State<DagDb>,
) -> (StatusCode, Json<FullGraphResponse>) {
    let graph = match graph.read() {
        Ok(guard) => guard,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(FullGraphResponse {
                    status: "error".to_string(),
                    nodes: vec![],
                    message: Some("Failed to lock graph".to_string()),
                }),
            );
        }
    };

    let mut weights = graph.get_weights().clone();
    weights.sort_by(|a, b| {
        b.weight.partial_cmp(&a.weight)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.node.cmp(&b.node))
    });
    // Получаем потомков для всех узлов заранее, чтобы избежать повторных вычислений.
    let descendants_map = graph.compute_descendants_with_depth_and_weight();

    // Собираем информацию о каждом узле: хэш, вес, потомки.
    let nodes = weights
        .into_iter()
        .map(|node| {
            // Получаем потомков узла из descendants_map.
            let descendants = descendants_map
                .get(&node.node)
                .unwrap_or(&Vec::new()) // Если нет потомков, возвращаем пустой вектор.
                .iter()
                .map(|descendant| DescendantInfo {
                    hash: String::from(&*descendant.node),
                    depth: descendant.depth,
                    weight: descendant.weight,
                })
                .collect::<Vec<DescendantInfo>>();

            NodeFullInfo {
                hash: String::from(&*node.node),
                weight: node.weight,
                descendants,
            }
        })
        .collect::<Vec<NodeFullInfo>>();

    (
        StatusCode::OK,
        Json(FullGraphResponse {
            status: "success".to_string(),
            nodes,
            message: None,
        }),
    )
}

// Функция для запуска HTTP-сервера.
pub async fn start_server(graph: DagDb) -> Result<(), Box<dyn std::error::Error>> {
    // Создаём новый граф и оборачиваем его в Arc<RwLock<_>> для потокобезопасного разделения.
    // Arc (Atomic Reference Counting) позволяет безопасно делить данные между потоками.
    // RwLock обеспечивает взаимоисключающий доступ к графу.
    // let graph = Arc::new(RwLock::new(DAG::new()));
    // Создаём маршруты для Axum-сервера.
    // Определяем один POST-эндпоинт /add_node, который вызывает add_node_handler.
    let app = Router::new()
        .route("/add", post(add_handler))
        .route("/pool", get(pool_handler))
        .route("/weights", get(get_weights_handler))
        .route("/remove", post(remove_handler))
        .route("/childrens", get(get_childrens_handler))
        .route("/parents", get(get_parents_handler))
        .route("/full", get(get_full_graph_handler))
        .with_state(graph.clone()); // Передаём граф как состояние приложения.

    // Создаём фоновую задачу для вывода количества узлов каждую секунду
    let graph_for_task = graph.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
        loop {
            interval.tick().await; // Ждём следующего тика (1 секунда)
            match graph_for_task.read() {
                Ok(graph) => {
                    let node_count = graph.get_node_count();
                    info!("Current node count: {}", node_count);
                }
                Err(e) => {
                    error!("Failed to lock graph for node count: {}", e);
                }
            }
        }
    });

    // Запускаем сервер на localhost:3000.
    // TcpListener создаёт асинхронный TCP-сокет для обработки входящих соединений.
    let listener = TcpListener::bind("0.0.0.0:3000").await?;
    info!("Server running at http://127.0.0.1:3000");

    // Запускаем Axum-сервер, который обрабатывает запросы.
    axum::serve(listener, app).await?;

    Ok(())
}