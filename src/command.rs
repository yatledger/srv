// Модуль для определения команд и ответов Raft
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use serde_json::Value;

// Команды, которые будут отправляться через Raft для управления DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientRequest {
    // Добавление узла с хэшом, родителями и данными
    Add {
        hash: Arc<str>,
        parents: Vec<Arc<str>>,
        data: Arc<Value>,
    },
    // Удаление узла по хэшу
    Remove {
        hash: Arc<str>,
    },
    // Обновление веса узла (если требуется согласование весов)
    Weight {
        hash: Arc<str>,
        weight: f64,
    },
}
// impl openraft::AppData for ClientRequest {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientResponse {
    Ok,
    Err(String),
}
//impl openraft::AppDataResponse for ClientResponse {}
