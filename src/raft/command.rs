// Модуль для определения команд и ответов Raft
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use crate::Tx;

// Команды, которые будут отправляться через Raft для управления DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    // Добавление узла с хэшом, родителями и данными
    Add {
        hash: Arc<str>,
        tx: Tx,
        sign: String,
        func: String,
    },
    // Удаление узла по хэшу
    Remove {
        nodes: Vec<Arc<str>>,
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
