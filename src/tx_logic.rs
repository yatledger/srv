//! Доменная логика обработки транзакции: подготовка к записи в Raft.
//!
//! Этот слой не знает про HTTP. Он проверяет структуру транзакции
//! (`parents`, `func`, `var`), вычисляет канонический хэш, проверяет подпись
//! и выполняет зависящие от состояния проверки (существование узла и
//! родителей). HTTP-хендлеры лишь транслируют `PrepareError` в статус-коды.

use std::sync::Arc;

use blake3::Hash;
use serde::{Deserialize, Serialize};

use crate::Tx;
use crate::graph::dag::{Dag, TRANSFER_TOKEN, extract_from_var_struct};
use crate::utils::{ordered_sum, validate_func, validate_parents, verify_signature};

/// Тело запроса на добавление транзакции.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct TxRead {
    /// Транзакция.
    pub tx: Tx,
    /// Подпись ed25519 в hex.
    pub sign: String,
    /// Имя функции, входящее в подписываемый контент.
    pub func: String,
}

/// Ошибка подготовки транзакции. `Internal` соответствует сбою обработки
/// (не по вине клиента), остальные — ошибки входных данных.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrepareError {
    /// Некорректные данные, присланные клиентом.
    Invalid(String),
    /// Внутренняя ошибка (например, сериализация при вычислении хэша).
    Internal(String),
    /// Узел или родитель отсутствует/уже существует.
    Conflict(String),
}

impl PrepareError {
    /// Возвращает текст ошибки.
    pub fn message(&self) -> &str {
        match self {
            PrepareError::Invalid(m) | PrepareError::Internal(m) | PrepareError::Conflict(m) => m,
        }
    }
}

impl std::fmt::Display for PrepareError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl std::error::Error for PrepareError {}

/// Вычисляет канонический хэш транзакции (без обращения к состоянию).
pub fn tx_hash(tx: &Tx, func: &str) -> Result<Hash, PrepareError> {
    ordered_sum(tx, func).map_err(PrepareError::Internal)
}

/// Проверяет структуру транзакции до обращения к состоянию:
/// родителей, допустимость `func`, структуру `var` и подпись.
pub fn validate_structure(payload: &TxRead) -> Result<Hash, PrepareError> {
    validate_parents(&payload.tx.prnts).map_err(PrepareError::Invalid)?;
    validate_func(&payload.func).map_err(PrepareError::Invalid)?;

    if payload.func == TRANSFER_TOKEN {
        extract_from_var_struct(&payload.tx)
            .and_then(|var| var.validate())
            .map_err(PrepareError::Invalid)?;
    }

    let hash = tx_hash(&payload.tx, &payload.func)?;
    verify_signature(&payload.tx.addr, &payload.sign, hash.as_bytes())
        .map_err(PrepareError::Invalid)?;
    Ok(hash)
}

/// Проверяет, что узел ещё не существует и все родители присутствуют
/// (в DAG или в реестре `added`).
///
/// Это **ранний отказ** по текущему снимку реплики; истина — детерминированная
/// валидация в state machine при применении (см. V4).
pub fn validate_against_state(
    dag: &Dag,
    hash: &Arc<str>,
    parents: &[Arc<str>],
) -> Result<(), PrepareError> {
    if dag.contains_node(hash) {
        return Err(PrepareError::Conflict("Node already exists".to_string()));
    }
    for parent in parents {
        if !dag.contains_node(parent) && !dag.is_node_added(parent) {
            return Err(PrepareError::Conflict(format!(
                "Parent {parent} does not exist"
            )));
        }
    }
    Ok(())
}

/// Возвращает hex-строку канонического хэша.
pub fn hash_to_hex(hash: &Hash) -> Arc<str> {
    Arc::from(hash.to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base58::ToBase58;
    use ed25519_dalek::Signer;
    use serde_json::json;

    fn keypair() -> (String, ed25519_dalek::SigningKey) {
        let sk = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
        let addr = sk.verifying_key().to_bytes().to_base58();
        (addr, sk)
    }

    fn payload(addr: &str, sign: String, func: &str, parents: Vec<Arc<str>>) -> TxRead {
        TxRead {
            tx: Tx {
                prnts: parents,
                addr: Arc::from(addr),
                seq: 0,
                var: json!({ "ca": "a", "to": "b", "val": 1, "msg": "m" }),
            },
            sign,
            func: func.to_string(),
        }
    }

    #[test]
    fn validate_structure_accepts_signed_payload() {
        let (addr, sk) = keypair();
        let parents = vec![Arc::from("p1"), Arc::from("p2")];
        let unsigned = payload(&addr, String::new(), TRANSFER_TOKEN, parents.clone());
        let hash = tx_hash(&unsigned.tx, &unsigned.func).unwrap();
        let sign = hex::encode(sk.sign(hash.as_bytes()).to_bytes());
        let signed = payload(&addr, sign, TRANSFER_TOKEN, parents);

        assert!(validate_structure(&signed).is_ok());
    }

    #[test]
    fn validate_structure_rejects_func_substitution() {
        let (addr, sk) = keypair();
        let parents = vec![Arc::from("p1"), Arc::from("p2")];
        // Подписываем transferToken, а присылаем другой func.
        let original = payload(&addr, String::new(), TRANSFER_TOKEN, parents.clone());
        let hash = tx_hash(&original.tx, &original.func).unwrap();
        let sign = hex::encode(sk.sign(hash.as_bytes()).to_bytes());
        let forged = payload(&addr, sign, "otherFunc", parents);

        let err = validate_structure(&forged).unwrap_err();
        assert!(matches!(err, PrepareError::Invalid(_)));
    }

    #[test]
    fn validate_structure_rejects_bad_signature() {
        let (addr, _sk) = keypair();
        let parents = vec![Arc::from("p1"), Arc::from("p2")];
        let signed = payload(&addr, "00".repeat(64), TRANSFER_TOKEN, parents);
        assert!(matches!(
            validate_structure(&signed).unwrap_err(),
            PrepareError::Invalid(_)
        ));
    }

    #[test]
    fn validate_against_state_detects_missing_parent() {
        let dag = Dag::new();
        let hash: Arc<str> = Arc::from("h");
        let parents = vec![Arc::from("ghost")];
        assert!(matches!(
            validate_against_state(&dag, &hash, &parents).unwrap_err(),
            PrepareError::Conflict(_)
        ));
    }

    #[test]
    fn prepare_error_status_mapping() {
        assert!(prepare_error_response_status(PrepareError::Invalid("x".into())) == 400);
        assert!(prepare_error_response_status(PrepareError::Conflict("x".into())) == 400);
        assert!(prepare_error_response_status(PrepareError::Internal("x".into())) == 500);
    }

    // Вспомогательная функция повторяет маппинг из server.rs для проверки.
    fn prepare_error_response_status(err: PrepareError) -> u16 {
        use axum::http::StatusCode;
        let status = match err {
            PrepareError::Invalid(_) | PrepareError::Conflict(_) => StatusCode::BAD_REQUEST,
            PrepareError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        status.as_u16()
    }
}
