// Модуль для определения команд и ответов Raft
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use crate::Tx;

// Команды, которые будут отправляться через Raft для управления DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    // Добавление узла с хэшом, родителями и данными
    Add {
        tx: Tx,
        sign: String,
        func: String,
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
/*
impl Request {
    pub fn add(hash: Arc<str>, parents: Vec<Arc<str>>, data: Arc<Value>) -> Self {
        Self::Add {
            hash,
            parents,
            data,
        }
    }
}
*/

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Response {
    pub value: Option<String>,
}
//impl openraft::AppDataResponse for ClientResponse {}

// Структура для запроса пересчета весов, отправляемого лидером на followers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeWeightsRequest {
    // Список узлов DAG, для которых нужно пересчитать веса
    pub nodes: Vec<Arc<str>>,
    // Версия DAG (номер последнего лога Raft) для проверки согласованности
    pub dag_version: u64,
}

// Структура для ответа от followers с вычисленными весами
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitWeightsResponse {
    // Список пар {узел, вес}
    pub node_weights: Vec<NodeWeight>,
    // Версия DAG, для которой выполнены вычисления
    pub dag_version: u64,
    pub message: Option<String>,
}

// Структура для представления пары узел-вес в ответе
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeWeight {
    pub node: Arc<str>,
    pub weight: f64,
}
