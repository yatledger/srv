use std::sync::Arc;
use std::collections::HashSet;
//use ed25519_dalek::VerifyingKey;
//use base58::FromBase58;
use serde_json::{to_value, Value};
use itertools::Itertools;
use blake3::Hash;
use crate::Tx;

// Преобразует Tx в строку: сериализует в JSON, сортирует ключи, объединяет значения
pub fn ordered_sum(tx: &Tx) -> Result<Hash, String> {
    // Сериализуем Tx в JSON-объект
    let value = to_value(tx).map_err(|e| format!("Serialization error: {}", e))?;
    let map = value.as_object().ok_or("Tx must serialize to an object")?;
    
    // Собираем значения в отсортированном порядке ключей
    let mut result = String::new();
    for key in map.keys().sorted() {
        let value = map.get(key).unwrap();
        result.push_str(&process_value(value)?);
    }
    
    Ok(blake3::hash(result.as_bytes()))
}

fn process_value(value: &Value) -> Result<String, String> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Array(arr) => {
            Ok(arr.iter()
                .filter_map(|v| v.as_str())
                .collect::<String>())
        },
        Value::Object(obj) => {
            // Рекурсивно обрабатываем объект (например, "var")
            let mut obj_result = String::new();
            for obj_key in obj.keys().sorted() {
                let obj_value = obj.get(obj_key).unwrap();
                obj_result.push_str(&process_value(obj_value)?);
            }
            Ok(obj_result)
        },
        Value::Null => Ok(String::new()),
        Value::Bool(b) => Ok(b.to_string()),
    }
}

pub fn validate_parents(parents: &Vec<Arc<str>>) -> Result<(), String> {
    // Check length constraints
    if parents.len() < 2 || parents.len() > 100 {
        return Err("parents must have between 2 and 25 elements".to_string());
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

/*
pub fn validate_tx_data(data: &Tx) -> Result<(), String> {
    // debit must be a valid Ed25519 public key
    let debit_bytes = data.debit.from_base58().map_err(|_| "invalid base58 for debit")?;
    if debit_bytes.len() != 32 {
        return Err("debit must be 32 bytes".to_string());
    }
    let debit_array: [u8; 32] = debit_bytes
        .try_into()
        .map_err(|_| "invalid Ed25519 key length")?;
    let _ = VerifyingKey::from_bytes(&debit_array).map_err(|_| "invalid Ed25519 public key")?;
    // debit must not equal credit
    if data.debit == data.credit {
        return Err("debit must not be equal to credit".to_string());
    }
    // amount must be positive
    if data.amount == 0 {
        return Err("amount must be greater than 0".to_string());
    }
    // msg, if present, must not exceed 2500 characters
    if let Some(msg) = &data.msg {
        if msg.len() > 2500 {
            return Err("msg must not exceed 2500 characters".to_string());
        }
    }
    Ok(())
}
*/