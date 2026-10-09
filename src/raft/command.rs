//! Команды и ответы Raft.

use serde::{Deserialize, Serialize};

use crate::Tx;
use crate::domain::{Func, Hash};

/// Команды, отправляемые через Raft для управления DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Добавление узла: хэш, транзакция, подпись и функция.
    Add {
        /// Хэш нового узла.
        hash: Hash,
        /// Транзакция.
        tx: Tx,
        /// Подпись ed25519 в hex.
        sign: String,
        /// Функция транзакции.
        func: Func,
    },
    /// Удаление узлов по хэшам.
    Remove {
        /// Хэши удаляемых узлов.
        nodes: Vec<Hash>,
    },
}

/// Ответ state machine на команду.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Response {
    /// Результат применения (например, `Ok` или текст ошибки).
    pub value: Option<String>,
}
