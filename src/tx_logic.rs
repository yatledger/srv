//! Доменная логика обработки транзакции: подготовка к записи в Raft.
//!
//! Этот слой не знает про HTTP. Он проверяет структуру транзакции
//! (`parents`, `func`, `var`), вычисляет канонический хэш, проверяет подпись
//! и выполняет зависящие от состояния проверки (существование узла и
//! родителей). HTTP-хендлеры лишь транслируют `PrepareError` в статус-коды.

use blake3::Hash as Blake3Hash;
use serde::{Deserialize, Serialize};

use crate::Tx;
use crate::domain::{Func, Hash};
use crate::graph::dag::{Dag, extract_from_var_struct};
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
pub fn tx_hash(tx: &Tx, func: &str) -> Result<Blake3Hash, PrepareError> {
    ordered_sum(tx, func).map_err(PrepareError::Internal)
}

/// Разбирает и проверяет `func`.
pub fn parse_func(func: &str) -> Result<Func, PrepareError> {
    validate_func(func).map_err(PrepareError::Invalid)?;
    func.parse::<Func>().map_err(PrepareError::Invalid)
}

/// Проверяет структуру транзакции до обращения к состоянию:
/// родителей, допустимость `func`, структуру `var` и подпись.
/// Возвращает канонический хэш и типизированную функцию.
pub fn validate_structure(payload: &TxRead) -> Result<(Blake3Hash, Func), PrepareError> {
    validate_parents(&payload.tx.prnts).map_err(PrepareError::Invalid)?;
    let func = parse_func(&payload.func)?;

    if func == Func::TransferToken {
        extract_from_var_struct(&payload.tx)
            .and_then(|var| var.validate())
            .map_err(PrepareError::Invalid)?;
    }

    let hash = tx_hash(&payload.tx, &payload.func)?;
    verify_signature(&payload.tx.addr, &payload.sign, hash.as_bytes())
        .map_err(PrepareError::Invalid)?;
    Ok((hash, func))
}

/// Проверяет, что узел ещё не существует и все родители присутствуют
/// (в DAG или в реестре `added`).
///
/// Это **ранний отказ** по текущему снимку реплики; истина — детерминированная
/// валидация в state machine при применении (см. V4).
pub fn validate_against_state(
    dag: &Dag,
    hash: &Hash,
    parents: &[Hash],
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
pub fn hash_to_hex(hash: &Blake3Hash) -> Hash {
    Hash::from(hash.to_hex().to_string())
}

/// Проверяет генезис-запись перед загрузкой (K2).
///
/// Сверяет объявленный `hash` с каноническим `ordered_sum(tx, func)` и проверяет
/// структуру `var` для известных функций. Подпись и список родителей **не**
/// проверяются: генезис-узлы — bootstrap-овые (пустые `addr`/`sign`), а
/// существование родителей обеспечивает детерминированная валидация state
/// machine при применении.
pub fn validate_genesis(tx: &Tx, func: &str, declared_hash: &Hash) -> Result<Func, PrepareError> {
    let func = parse_func(func)?;

    if func == Func::TransferToken {
        extract_from_var_struct(tx)
            .and_then(|var| var.validate())
            .map_err(PrepareError::Invalid)?;
    }

    let computed = hash_to_hex(&tx_hash(tx, func.as_str())?);
    if computed != *declared_hash {
        return Err(PrepareError::Invalid(format!(
            "genesis hash mismatch: declared {declared_hash}, computed {computed}"
        )));
    }

    Ok(func)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::domain::Address;
    use base58::ToBase58;
    use ed25519_dalek::Signer;
    use serde_json::json;

    fn keypair() -> (String, ed25519_dalek::SigningKey) {
        let sk = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
        let addr = sk.verifying_key().to_bytes().to_base58();
        (addr, sk)
    }

    fn payload(addr: &str, sign: String, func: Func, parents: Vec<Hash>) -> TxRead {
        TxRead {
            tx: Tx {
                prnts: parents,
                addr: Address::from(addr),
                seq: 0,
                var: json!({ "ca": "a", "to": "b", "val": 1, "msg": "m" }),
            },
            sign,
            func: func.as_str().to_string(),
        }
    }

    #[test]
    fn validate_structure_accepts_signed_payload() {
        let (addr, sk) = keypair();
        let parents = vec![Hash::from("p1"), Hash::from("p2")];
        let unsigned = payload(&addr, String::new(), Func::TransferToken, parents.clone());
        let hash = tx_hash(&unsigned.tx, &unsigned.func).unwrap();
        let sign = hex::encode(sk.sign(hash.as_bytes()).to_bytes());
        let signed = payload(&addr, sign, Func::TransferToken, parents);

        assert!(validate_structure(&signed).is_ok());
    }

    #[test]
    fn validate_structure_rejects_func_substitution() {
        let (addr, sk) = keypair();
        let parents = vec![Hash::from("p1"), Hash::from("p2")];
        // Подписываем transferToken, а присылаем другой func.
        let original = payload(&addr, String::new(), Func::TransferToken, parents.clone());
        let hash = tx_hash(&original.tx, &original.func).unwrap();
        let sign = hex::encode(sk.sign(hash.as_bytes()).to_bytes());
        // func на проводе остаётся строкой; подменяем вручную.
        let mut forged = payload(&addr, sign, Func::TransferToken, parents);
        forged.func = "otherFunc".to_string();

        let err = validate_structure(&forged).unwrap_err();
        assert!(matches!(err, PrepareError::Invalid(_)));
    }

    #[test]
    fn validate_structure_rejects_bad_signature() {
        let (addr, _sk) = keypair();
        let parents = vec![Hash::from("p1"), Hash::from("p2")];
        let signed = payload(&addr, "00".repeat(64), Func::TransferToken, parents);
        assert!(matches!(
            validate_structure(&signed).unwrap_err(),
            PrepareError::Invalid(_)
        ));
    }

    #[test]
    fn validate_against_state_detects_missing_parent() {
        let dag = Dag::new();
        let hash = Hash::from("h");
        let parents = vec![Hash::from("ghost")];
        assert!(matches!(
            validate_against_state(&dag, &hash, &parents).unwrap_err(),
            PrepareError::Conflict(_)
        ));
    }

    #[test]
    fn validate_genesis_accepts_canonical_hash_and_genesis_parents() {
        // Генезис-узел: пустой addr/sign, без родителей.
        let tx = Tx {
            prnts: vec![],
            addr: Address::from(""),
            seq: 0,
            var: json!({ "ca": "0", "to": "T", "val": 1, "msg": "genesis" }),
        };
        let hash = hash_to_hex(&tx_hash(&tx, "transferToken").unwrap());
        assert!(validate_genesis(&tx, "transferToken", &hash).is_ok());
    }

    #[test]
    fn validate_genesis_rejects_non_canonical_hash() {
        let tx = Tx {
            prnts: vec![],
            addr: Address::from(""),
            seq: 0,
            var: json!({ "ca": "0", "to": "T", "val": 1, "msg": "genesis" }),
        };
        let wrong = Hash::from("deadbeef");
        let err = validate_genesis(&tx, "transferToken", &wrong).unwrap_err();
        assert!(matches!(err, PrepareError::Invalid(_)));
    }

    #[test]
    fn validate_genesis_rejects_invalid_var() {
        let tx = Tx {
            prnts: vec![],
            addr: Address::from(""),
            seq: 0,
            var: json!({ "totally": "wrong" }),
        };
        let hash = Hash::from("whatever");
        assert!(validate_genesis(&tx, "transferToken", &hash).is_err());
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

    #[test]
    fn shipped_genesis_has_canonical_hashes() {
        // K2/S15: хэши в genesis.json обязаны совпадать с каноническим хэшем
        // содержимого, иначе `load-genesis` их отклонит.
        #[derive(serde::Deserialize)]
        struct GenesisEntry {
            hash: String,
            data: TxRead,
        }

        let raw = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/genesis.json"))
            .expect("genesis.json читается");
        let entries: Vec<GenesisEntry> = serde_json::from_str(&raw).expect("genesis.json валиден");

        assert!(!entries.is_empty(), "genesis.json не должен быть пустым");
        for entry in entries {
            validate_genesis(
                &entry.data.tx,
                &entry.data.func,
                &Hash::from(entry.hash.as_str()),
            )
            .unwrap_or_else(|e| panic!("неканонический genesis {}: {}", entry.hash, e));
        }
    }
}
