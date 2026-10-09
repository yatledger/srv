//! Хэширование, проверка подписи и базовые валидаторы.
//!
//! Канонический хэш транзакции (`ordered_sum`) детерминирован: ключи объектов
//! сортируются рекурсивно, поэтому одинаковый контент даёт одинаковый хэш на
//! всех репликах. Зависящие от состояния проверки живут в `tx_logic`/state machine.

use std::collections::HashSet;

use blake3::Hash as Blake3Hash;
use serde_json::{Value, to_value};

use crate::Tx;
use crate::domain::Hash;

/// Допустимые значения поля `func`.
///
/// `func` участвует в подписываемом/хэшируемом контенте, поэтому список известных
/// значений должен быть одинаковым на всех репликах (детерминизм state machine).
pub const KNOWN_FUNCS: &[&str] = &[crate::graph::dag::TRANSFER_TOKEN];

/// Проверяет, что `func` входит в список известных функций.
pub fn validate_func(func: &str) -> Result<(), String> {
    if KNOWN_FUNCS.contains(&func) {
        Ok(())
    } else {
        Err(format!("unknown func: {func}"))
    }
}

/// Проверяет подпись ed25519 транзакции.
///
/// `addr` — публичный ключ в base58, `sign` — подпись в hex, `message` — байты,
/// которые были подписаны (канонический хэш контента транзакции). Возвращает
/// человекочитаемую ошибку без паник. Детерминирована и не зависит от сети/времени.
pub fn verify_signature(addr: &str, sign: &str, message: &[u8]) -> Result<(), String> {
    use base58::FromBase58;
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};
    use hex::FromHex;

    let signature_bytes =
        <[u8; 64]>::from_hex(sign).map_err(|_| "invalid hex for signature".to_string())?;
    let signature = Signature::try_from(signature_bytes.as_ref())
        .map_err(|_| "invalid signature format".to_string())?;

    let addr_bytes = addr
        .from_base58()
        .map_err(|_| "invalid base58 in signature verification".to_string())?;
    let addr_array: [u8; 32] = addr_bytes
        .try_into()
        .map_err(|_| "invalid Ed25519 key length".to_string())?;
    let verify_key = VerifyingKey::from_bytes(&addr_array)
        .map_err(|_| "invalid Ed25519 public key".to_string())?;

    verify_key
        .verify(message, &signature)
        .map_err(|_| "signature verification failed".to_string())
}

/// Вычисляет хэш подписываемого контента транзакции.
///
/// В контент входят все поля `Tx` **и** `func`. Для устранения неоднозначностей
/// (склейка значений без разделителей) используется каноничная JSON-сериализация
/// с рекурсивно отсортированными ключами объектов; порядок элементов массивов
/// сохраняется. Одинаковый контент всегда даёт одинаковый хэш на всех узлах.
pub fn ordered_sum(tx: &Tx, func: &str) -> Result<Blake3Hash, String> {
    // `to_value` даёт детерминированную структуру JSON для сериализуемых полей `Tx`.
    let mut value = to_value(tx).map_err(|e| format!("Serialization error: {e}"))?;
    {
        let obj = value
            .as_object_mut()
            .ok_or("Tx must serialize to an object")?;
        // `func` обязан входить в подписываемый контент.
        obj.insert("func".to_string(), Value::String(func.to_string()));
    }

    let canonical = canonical_json(&value);
    Ok(blake3::hash(canonical.as_bytes()))
}

/// Каноничная JSON-сериализация: ключи всех объектов сортируются, строки
/// экранируются, вложенность сохраняется. Результат полностью детерминирован.
fn canonical_json(value: &Value) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&n.to_string()),
        Value::String(s) => write_json_string(s, out),
        Value::Array(arr) => {
            out.push('[');
            for (i, item) in arr.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_string(key, out);
                out.push(':');
                // Ключ гарантированно присутствует.
                if let Some(item) = map.get(key) {
                    write_canonical(item, out);
                }
            }
            out.push('}');
        }
    }
}

/// Записывает строку в JSON-виде (экранирование как в serde_json).
fn write_json_string(s: &str, out: &mut String) {
    // `to_string` для строки не может завершиться ошибкой.
    if let Ok(escaped) = serde_json::to_string(s) {
        out.push_str(&escaped);
    }
}

/// Сокращает длинную строку (хэш/адрес) до `head…tail` для человекочитаемых
/// логов. Короткие строки возвращаются как есть. UTF-8-безопасно.
pub fn short_hash(value: &str) -> String {
    const HEAD: usize = 8;
    const TAIL: usize = 4;
    let len = value.chars().count();
    if len <= HEAD + TAIL + 1 {
        return value.to_string();
    }
    let head: String = value.chars().take(HEAD).collect();
    let tail: String = value
        .chars()
        .rev()
        .take(TAIL)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{head}…{tail}")
}

/// Проверяет корректность списка родителей: длина, непустые и уникальные значения.
pub fn validate_parents(parents: &[Hash]) -> Result<(), String> {
    // Check length constraints
    if parents.len() < 2 || parents.len() > 100 {
        return Err("parents must have between 2 and 100 elements".to_string());
    }
    // Check for non-empty strings
    if parents.iter().any(|s| s.trim().is_empty()) {
        return Err("parents must not be empty strings".to_string());
    }
    let unique_parents: HashSet<&Hash> = parents.iter().collect();
    if unique_parents.len() != parents.len() {
        return Err("parents must be unique".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn tx_with_var(var: Value) -> Tx {
        Tx {
            prnts: vec![Hash::from("parent-a"), Hash::from("parent-b")],
            addr: crate::domain::Address::from("addr"),
            seq: 1,
            var,
        }
    }

    #[test]
    fn ordered_sum_is_deterministic() {
        let tx = tx_with_var(json!({ "ca": "a", "to": "b", "val": 5 }));
        let first = ordered_sum(&tx, "transferToken").unwrap();
        let second = ordered_sum(&tx, "transferToken").unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn ordered_sum_changes_when_func_changes() {
        let tx = tx_with_var(json!({ "ca": "a", "to": "b", "val": 5 }));
        let left = ordered_sum(&tx, "transferToken").unwrap();
        let right = ordered_sum(&tx, "otherFunc").unwrap();
        assert_ne!(left, right, "подмена func должна менять хэш");
    }

    #[test]
    fn ordered_sum_has_no_concatenation_collision() {
        // При старой склейке значений без разделителей оба варианта давали "abc".
        let left = tx_with_var(json!({ "ca": "a", "to": "bc" }));
        let right = tx_with_var(json!({ "ca": "ab", "to": "c" }));
        let left_hash = ordered_sum(&left, "transferToken").unwrap();
        let right_hash = ordered_sum(&right, "transferToken").unwrap();
        assert_ne!(left_hash, right_hash, "неоднозначная склейка недопустима");
    }

    #[test]
    fn ordered_sum_covers_all_tx_fields() {
        let base = tx_with_var(json!({ "ca": "a", "to": "b", "val": 5 }));
        let mut changed = base.clone();
        changed.seq = 2;
        assert_ne!(
            ordered_sum(&base, "transferToken").unwrap(),
            ordered_sum(&changed, "transferToken").unwrap()
        );

        let mut changed = base.clone();
        changed.addr = crate::domain::Address::from("other-addr");
        assert_ne!(
            ordered_sum(&base, "transferToken").unwrap(),
            ordered_sum(&changed, "transferToken").unwrap()
        );

        let mut changed = base.clone();
        changed.prnts = vec![Hash::from("parent-a"), Hash::from("parent-c")];
        assert_ne!(
            ordered_sum(&base, "transferToken").unwrap(),
            ordered_sum(&changed, "transferToken").unwrap()
        );
    }

    #[test]
    fn validate_func_rejects_unknown() {
        assert!(validate_func("transferToken").is_ok());
        assert!(validate_func("totallyUnknown").is_err());
    }

    #[test]
    fn short_hash_truncates_with_ellipsis() {
        assert_eq!(short_hash("abc"), "abc");
        assert_eq!(short_hash("0123456789abcdef"), "01234567…cdef");
        // Граница: ровно HEAD+TAIL+1 — не режем.
        assert_eq!(short_hash("0123456789abc"), "0123456789abc");
    }

    #[test]
    fn validate_parents_accepts_valid() {
        let parents = vec![Hash::from("a"), Hash::from("b")];
        assert!(validate_parents(&parents).is_ok());
    }

    #[test]
    fn validate_parents_rejects_too_few_and_too_many() {
        assert!(validate_parents(&[Hash::from("a")]).is_err());
        let many: Vec<Hash> = (0..101)
            .map(|i| Hash::from(i.to_string().as_str()))
            .collect();
        assert!(validate_parents(&many).is_err());
    }

    #[test]
    fn validate_parents_rejects_empty_and_duplicates() {
        assert!(validate_parents(&[Hash::from(""), Hash::from("b")]).is_err());
        assert!(validate_parents(&[Hash::from("a"), Hash::from("a")]).is_err());
    }

    fn signing_key() -> ed25519_dalek::SigningKey {
        ed25519_dalek::SigningKey::from_bytes(&[7u8; 32])
    }

    fn keypair() -> (String, ed25519_dalek::SigningKey) {
        use base58::ToBase58;
        let sk = signing_key();
        let addr = sk.verifying_key().to_bytes().to_base58();
        (addr, sk)
    }

    fn sign(sk: &ed25519_dalek::SigningKey, message: &[u8]) -> String {
        use ed25519_dalek::Signer;
        hex::encode(sk.sign(message).to_bytes())
    }

    #[test]
    fn verify_signature_accepts_valid_signature() {
        let (addr, sk) = keypair();
        let message = b"canonical hash bytes";
        let sig = sign(&sk, message);

        assert!(verify_signature(&addr, &sig, message).is_ok());
    }

    #[test]
    fn verify_signature_rejects_tampered_message() {
        let (addr, sk) = keypair();
        let sig = sign(&sk, b"original message");
        // Подпись валидна, но сообщение подменено.
        assert!(verify_signature(&addr, &sig, b"tampered message").is_err());
    }

    #[test]
    fn verify_signature_rejects_substituted_func_in_hash() {
        // Критичный кейс K2: подмена func меняет хэш, а значит ломает подпись.
        let (addr, sk) = keypair();
        let tx = tx_with_var(json!({ "ca": "a", "to": "b", "val": 5 }));
        let signed_hash = ordered_sum(&tx, "transferToken").unwrap();
        let sig = hex::encode(ed25519_dalek::Signer::sign(&sk, signed_hash.as_bytes()).to_bytes());

        let forged_hash = ordered_sum(&tx, "otherFunc").unwrap();
        assert!(
            verify_signature(&addr, &sig, forged_hash.as_bytes()).is_err(),
            "подмена func не должна проходить проверку подписи"
        );
        assert!(verify_signature(&addr, &sig, signed_hash.as_bytes()).is_ok());
    }

    #[test]
    fn verify_signature_rejects_malformed_inputs() {
        let (addr, sk) = keypair();
        let message = b"m";
        let sig = sign(&sk, message);

        // Не-hex подпись.
        assert!(verify_signature(&addr, "not-hex", message).is_err());
        // Неверная длина подписи.
        assert!(verify_signature(&addr, "abcd", message).is_err());
        // Невалидный base58-адрес.
        assert!(verify_signature("!!!not-base58!!!", &sig, message).is_err());
        // Валидный base58, но неверная длина ключа.
        assert!(verify_signature("1111", &sig, message).is_err());
    }
}

#[cfg(test)]
mod genesis_compat_tests {

    use crate::Tx;
    use serde_json::json;

    // D4-инвариант: newtype-ы сериализуются прозрачно, поэтому `to_value(tx)`
    // (на котором держится канонический хэш K2) не меняется по сравнению с
    // прежними `Arc<str>`-полями.
    #[test]
    fn newtypes_serialize_like_plain_strings() {
        let raw = json!({
            "prnts": ["p1", "p2"],
            "addr": "some-addr",
            "seq": 7,
            "var": { "ca": "0", "to": "T", "val": 5, "msg": "m" }
        });
        let tx: Tx = serde_json::from_value(raw.clone()).unwrap();
        let value = serde_json::to_value(&tx).unwrap();
        assert_eq!(value, raw);
    }
}
