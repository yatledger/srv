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

/// Типизированный результат применения команды state machine.
///
/// Заменяет прежний струнно-типизированный `Response.value` (`"Ok"` /
/// `"Error: ..."`), из-за которого прикладной отказ легко было принять за успех
/// (находки C34/C43). `Rejected` означает, что команда детерминированно
/// отклонена и состояние DAG **не** изменено.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplyResult {
    /// Команда применена (состояние изменено).
    Ok,
    /// Команда отклонена; строка — человекочитаемая причина.
    Rejected(String),
}

/// Ответ state machine на команду.
///
/// `value == None` — служебная запись (пустой лог или membership), у которой
/// нет прикладного результата.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response {
    /// Прикладной результат применения (если применимо).
    pub value: Option<ApplyResult>,
}

impl Response {
    /// Ответ на успешно применённую команду.
    pub fn ok() -> Self {
        Self {
            value: Some(ApplyResult::Ok),
        }
    }

    /// Ответ на отклонённую команду с причиной.
    pub fn rejected(reason: impl Into<String>) -> Self {
        Self {
            value: Some(ApplyResult::Rejected(reason.into())),
        }
    }

    /// Служебный ответ без прикладного результата (Blank/Membership).
    pub fn blank() -> Self {
        Self { value: None }
    }

    /// `true`, если команда не была отвергнута прикладной валидацией
    /// (успех или служебная запись).
    pub fn is_ok(&self) -> bool {
        matches!(self.value, None | Some(ApplyResult::Ok))
    }

    /// Причина отказа, если команда была отклонена state machine.
    pub fn rejection(&self) -> Option<&str> {
        match &self.value {
            Some(ApplyResult::Rejected(reason)) => Some(reason.as_str()),
            _ => None,
        }
    }

    /// Проверяет прикладной результат: `Err(reason)` при отказе state machine.
    pub fn as_result(&self) -> Result<(), &str> {
        match self.rejection() {
            Some(reason) => Err(reason),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_response_is_success() {
        let r = Response::ok();
        assert!(r.is_ok());
        assert_eq!(r.rejection(), None);
        assert_eq!(r.as_result(), Ok(()));
    }

    #[test]
    fn blank_response_is_not_a_rejection() {
        let r = Response::blank();
        assert!(r.is_ok());
        assert!(r.as_result().is_ok());
    }

    #[test]
    fn rejected_response_carries_reason() {
        let r = Response::rejected("node already exists");
        assert!(!r.is_ok());
        assert_eq!(r.rejection(), Some("node already exists"));
        assert_eq!(r.as_result(), Err("node already exists"));
    }

    #[test]
    fn response_roundtrips_through_json() {
        let r = Response::rejected("boom");
        let json = serde_json::to_string(&r).unwrap();
        let back: Response = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
    }
}
