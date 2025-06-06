// Импортируем необходимые зависимости для создания HTTP-сервера.
use axum::{
    extract::{Json, State}, // Для извлечения JSON и состояния из запроса.
    http::StatusCode, // Для работы с HTTP-статусами.
    routing::{get, post}, // Для создания POST-эндпоинта.
    Router, // Основной тип для маршрутизации запросов.
};
use serde::{Deserialize, Serialize}; // Для сериализации/десериализации JSON.
use std::collections::HashMap; // Для возврата графа в JSON.
use std::sync::{Arc, RwLock}; // Для безопасного разделения графа между потоками.
use tokio::net::TcpListener; // Для запуска асинхронного TCP-сервера.

// Импортируем структуру DAG из вашего модуля graph.rs.
use crate::graph::{DAG};

// Определяем структуру для десериализации JSON-запроса.
// Она соответствует данным, которые клиент отправляет в POST-запросе на /add_node.
#[derive(Deserialize)]
struct AddNodeRequest {
    hash: String, // Хэш нового узла.
    parents: Vec<String>, // Список хэшей родительских узлов.
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
    data: String, // Пока пустое
    weight: f64,
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

async fn pool_handler(
    State(graph): State<Arc<RwLock<DAG>>>,
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

    // Собираем все хэши узлов из графа.
    let mut nodes: Vec<String> = graph.get_adj_list().keys().cloned().collect();
    // Вычисляем длину обрезанного списка как округлённый квадратный корень от числа узлов.
    let target_len = (nodes.len() as f64).sqrt().ceil() as usize;
    // Обрезаем список до target_len, если он длиннее.
    nodes.truncate(target_len);

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

// Обработчик для GET-запроса на /graph.
// Возвращает текущий граф в виде JSON.
async fn get_graph_handler(
    State(graph): State<Arc<RwLock<DAG>>>, // Извлекаем граф из состояния.
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

    // Возвращаем граф как HashMap.
    (
        StatusCode::OK,
        Json(GraphResponse {
            status: "success".to_string(),
            graph: Some(graph.get_adj_list().clone()), // Клонируем граф для сериализации.
            message: None,
        }),
    )
}

// Определяем асинхронный обработчик POST-запроса на /add_node.
// Принимает JSON с данными запроса и состояние приложения (граф).
async fn add_node_handler(
    State(graph): State<Arc<RwLock<DAG>>>, // Извлекаем граф, защищённый Arc и RwLock для потокобезопасности.
    Json(payload): Json<AddNodeRequest>, // Извлекаем JSON-данные из тела запроса.
) -> (StatusCode, Json<AddNodeResponse>) {
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

    // Вызываем метод add_node_with_parents на графе.
    match graph.add_node_with_parents(payload.hash, payload.parents) {
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
    }
}

async fn get_weights_handler(State(graph): State<Arc<RwLock<DAG>>>) -> (StatusCode, Json<WeightsResponse>) {
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
    let weights = graph.get_all_weights();
    let nodes = weights.into_iter().map(|node| NodeWeight {
        hash: node.node,
        data: String::new(),
        weight: node.weight,
    }).collect();
    (
        StatusCode::OK,
        Json(WeightsResponse {
            status: "success".to_string(),
            nodes,
            message: None,
        }),
    )
}

async fn get_full_graph_handler(
    State(graph): State<Arc<RwLock<DAG>>>,
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

    let weights = graph.get_all_weights();
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
                    hash: descendant.node.clone(),
                    depth: descendant.depth,
                    weight: descendant.weight,
                })
                .collect::<Vec<DescendantInfo>>();

            NodeFullInfo {
                hash: node.node,
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
pub async fn start_server() -> Result<(), Box<dyn std::error::Error>> {
    // Создаём новый граф и оборачиваем его в Arc<RwLock<_>> для потокобезопасного разделения.
    // Arc (Atomic Reference Counting) позволяет безопасно делить данные между потоками.
    // RwLock обеспечивает взаимоисключающий доступ к графу.
    let graph = Arc::new(RwLock::new(DAG::new()));
    // Создаём маршруты для Axum-сервера.
    // Определяем один POST-эндпоинт /add_node, который вызывает add_node_handler.
    let app = Router::new()
        .route("/add_node", post(add_node_handler))
        .route("/graph", get(get_graph_handler))
        .route("/pool", get(pool_handler))
        .route("/weights", get(get_weights_handler))
        .route("/full_graph", get(get_full_graph_handler))
        .with_state(graph); // Передаём граф как состояние приложения.

    // Запускаем сервер на localhost:3000.
    // TcpListener создаёт асинхронный TCP-сокет для обработки входящих соединений.
    let listener = TcpListener::bind("0.0.0.0:3000").await?;
    println!("Server running at http://127.0.0.1:3000");

    // Запускаем Axum-сервер, который обрабатывает запросы.
    axum::serve(listener, app).await?;

    Ok(())
}