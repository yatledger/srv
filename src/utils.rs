use std::collections::HashSet;
use std::sync::Arc;

use blake3::Hash;
use serde_json::{Value, to_value};

use crate::Tx;

/// Допустимые значения поля `func`.
///
/// `func` участвует в подписываемом/хэшируемом контенте, поэтому список известных
/// значений должен быть одинаковым на всех репликах (детерминизм state machine).
pub const KNOWN_FUNCS: &[&str] = &["transferToken"];

/// Проверяет, что `func` входит в список известных функций.
pub fn validate_func(func: &str) -> Result<(), String> {
    if KNOWN_FUNCS.contains(&func) {
        Ok(())
    } else {
        Err(format!("unknown func: {func}"))
    }
}

/// Вычисляет хэш подписываемого контента транзакции.
///
/// В контент входят все поля `Tx` **и** `func`. Для устранения неоднозначностей
/// (склейка значений без разделителей) используется каноничная JSON-сериализация
/// с рекурсивно отсортированными ключами объектов; порядок элементов массивов
/// сохраняется. Одинаковый контент всегда даёт одинаковый хэш на всех узлах.
pub fn ordered_sum(tx: &Tx, func: &str) -> Result<Hash, String> {
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

/// Проверяет корректность списка родителей: длина, непустые и уникальные значения.
pub fn validate_parents(parents: &[Arc<str>]) -> Result<(), String> {
    // Check length constraints
    if parents.len() < 2 || parents.len() > 100 {
        return Err("parents must have between 2 and 100 elements".to_string());
    }
    // Check for non-empty strings
    if parents.iter().any(|s| s.trim().is_empty()) {
        return Err("parents must not be empty strings".to_string());
    }
    let unique_parents: HashSet<&Arc<str>> = parents.iter().collect();
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
            prnts: vec![Arc::from("parent-a"), Arc::from("parent-b")],
            addr: Arc::from("addr"),
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
        changed.addr = Arc::from("other-addr");
        assert_ne!(
            ordered_sum(&base, "transferToken").unwrap(),
            ordered_sum(&changed, "transferToken").unwrap()
        );

        let mut changed = base.clone();
        changed.prnts = vec![Arc::from("parent-a"), Arc::from("parent-c")];
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
    fn validate_parents_accepts_valid() {
        let parents = vec![Arc::from("a"), Arc::from("b")];
        assert!(validate_parents(&parents).is_ok());
    }

    #[test]
    fn validate_parents_rejects_too_few_and_too_many() {
        assert!(validate_parents(&[Arc::from("a")]).is_err());
        let many: Vec<Arc<str>> = (0..101)
            .map(|i| Arc::from(i.to_string().as_str()))
            .collect();
        assert!(validate_parents(&many).is_err());
    }

    #[test]
    fn validate_parents_rejects_empty_and_duplicates() {
        assert!(validate_parents(&[Arc::from(""), Arc::from("b")]).is_err());
        assert!(validate_parents(&[Arc::from("a"), Arc::from("a")]).is_err());
    }
}
