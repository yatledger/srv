//! Типизированные доменные значения (D4).
//!
//! [`Hash`], [`Address`] и [`Func`] заменяют «голые» `Arc<str>`/`String` в
//! доменных сигнатурах. Все три сериализуются **прозрачно** (как строки),
//! поэтому канонический хэш транзакции (K2) и формат хранения не меняются.

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Хэш узла/родителя (hex-строка).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hash(Arc<str>);

impl Hash {
    /// Конструирует хэш из строки.
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Hash(value.into())
    }

    /// Возвращает строковое представление.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for Hash {
    fn default() -> Self {
        Hash(Arc::from(""))
    }
}

impl From<&str> for Hash {
    fn from(value: &str) -> Self {
        Hash(Arc::from(value))
    }
}

impl From<String> for Hash {
    fn from(value: String) -> Self {
        Hash(Arc::from(value))
    }
}

impl From<Arc<str>> for Hash {
    fn from(value: Arc<str>) -> Self {
        Hash(value)
    }
}

impl Deref for Hash {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Публичный адрес (ключ ed25519 в base58).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Address(Arc<str>);

impl Address {
    /// Конструирует адрес из строки.
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Address(value.into())
    }

    /// Возвращает строковое представление.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for Address {
    fn default() -> Self {
        Address(Arc::from(""))
    }
}

impl From<&str> for Address {
    fn from(value: &str) -> Self {
        Address(Arc::from(value))
    }
}

impl From<String> for Address {
    fn from(value: String) -> Self {
        Address(Arc::from(value))
    }
}

impl Deref for Address {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Имя функции транзакции. Сериализуется строкой, входит в подписываемый контент.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Func {
    /// Перевод токена.
    #[serde(rename = "transferToken")]
    TransferToken,
}

impl Func {
    /// Строковое имя функции (как в подписываемом контенте).
    pub fn as_str(&self) -> &'static str {
        match self {
            Func::TransferToken => "transferToken",
        }
    }
}

impl fmt::Display for Func {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Func {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "transferToken" => Ok(Func::TransferToken),
            other => Err(format!("unknown func: {other}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_serializes_transparently() {
        let h = Hash::new("abc");
        assert_eq!(serde_json::to_string(&h).unwrap(), "\"abc\"");
        let back: Hash = serde_json::from_str("\"abc\"").unwrap();
        assert_eq!(back, h);
    }

    #[test]
    fn address_serializes_transparently() {
        let a = Address::new("key");
        assert_eq!(serde_json::to_string(&a).unwrap(), "\"key\"");
    }

    #[test]
    fn func_roundtrip_and_rejects_unknown() {
        assert_eq!(Func::TransferToken.as_str(), "transferToken");
        assert_eq!(
            serde_json::to_string(&Func::TransferToken).unwrap(),
            "\"transferToken\""
        );
        assert_eq!(
            "transferToken".parse::<Func>().unwrap(),
            Func::TransferToken
        );
        assert!("other".parse::<Func>().is_err());
    }
}
