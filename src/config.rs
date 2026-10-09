//! Конфигурация узла, собираемая из CLI-аргументов и переменных окружения.
//!
//! Приоритет значений: аргумент CLI > переменная окружения > значение по умолчанию.
//! Все секреты (Redis URL, кластерный токен) обязаны приходить из окружения и не
//! должны присутствовать в исходном коде.

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

/// Каталог персистентных данных по умолчанию.
pub const DEFAULT_DATA_DIR: &str = "./data";

/// Конфигурация узла.
#[derive(Parser, Clone, Debug)]
#[command(author, version, about, long_about = None)]
pub struct AppConfig {
    /// Уникальный ID узла в кластере Raft.
    #[arg(long, env = "NODE_ID", default_value_t = 1)]
    pub id: u64,

    /// Хост, публикуемый другим узлам кластера (используется, если не задан ADVERTISE_ADDR).
    #[arg(long, env = "ADVERTISE_HOST", default_value = "127.0.0.1")]
    pub addr: String,

    /// Порт HTTP-сервера (используется, если не заданы BIND_ADDR/ADVERTISE_ADDR).
    #[arg(long, env = "HTTP_PORT", default_value_t = 21001)]
    pub port: u16,

    /// Полный адрес прослушивания HTTP-сервера (host:port); переопределяет host/port.
    #[arg(long, env = "BIND_ADDR")]
    pub bind_addr: Option<String>,

    /// Полный адрес, публикуемый кластеру (host:port); переопределяет --addr/--port.
    #[arg(long, env = "ADVERTISE_ADDR")]
    pub advertise_addr: Option<String>,

    /// Каталог персистентных данных (Raft log, state machine, снапшоты).
    #[arg(long, env = "DATA_DIR", default_value = DEFAULT_DATA_DIR)]
    pub data_dir: PathBuf,

    /// Полная строка подключения к Redis, включая пароль. Секрет, только из окружения.
    #[arg(long, env = "REDIS_URL")]
    pub redis_url: Option<String>,

    /// Общий токен для внутренних/административных эндпоинтов.
    #[arg(long, env = "INTERNAL_API_TOKEN")]
    pub internal_api_token: Option<String>,

    /// Список узлов кластера в формате `id=addr,id=addr,...` (для удобства запуска).
    #[arg(long, env = "CLUSTER_NODES")]
    pub cluster_nodes: Option<String>,

    /// Таймаут HTTP-запросов приложения, секунды.
    #[arg(long, env = "HTTP_TIMEOUT_SECS", default_value_t = 10)]
    pub http_timeout_secs: u64,

    /// Таймаут установки HTTP-соединения приложения, секунды.
    #[arg(long, env = "HTTP_CONNECT_TIMEOUT_SECS", default_value_t = 3)]
    pub http_connect_timeout_secs: u64,

    /// Таймаут Raft HTTP-запросов, секунды.
    #[arg(long, env = "RAFT_HTTP_TIMEOUT_SECS", default_value_t = 30)]
    pub raft_http_timeout_secs: u64,

    /// Таймаут установки Raft HTTP-соединения, секунды.
    #[arg(long, env = "RAFT_CONNECT_TIMEOUT_SECS", default_value_t = 10)]
    pub raft_connect_timeout_secs: u64,

    /// Интервал фоновой очистки «тяжёлых» узлов, миллисекунды.
    #[arg(long, env = "PROCESSOR_INTERVAL_MS", default_value_t = 250)]
    pub processor_interval_ms: u64,

    /// Порог веса (после насыщения, диапазон [0,1)), выше которого узел удаляется.
    #[arg(long, env = "WEIGHT_THRESHOLD", default_value_t = 0.5)]
    pub weight_threshold: f64,

    /// Максимальный размер батча кандидатов на очистку за цикл.
    #[arg(long, env = "CLEANUP_BATCH_SIZE", default_value_t = 100)]
    pub cleanup_batch_size: usize,
}

/// Ошибка загрузки/валидации конфигурации.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    pub message: String,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ConfigError {}

impl AppConfig {
    /// Загружает `.env` (если есть) и парсит конфигурацию из CLI/окружения.
    pub fn load() -> Self {
        // `.env` не обязателен: в контейнере переменные приходят из окружения.
        let _ = dotenvy::dotenv();
        Self::parse()
    }

    /// Полный адрес прослушивания HTTP-сервера.
    pub fn bind_addr(&self) -> String {
        self.bind_addr
            .clone()
            .unwrap_or_else(|| format!("0.0.0.0:{}", self.port))
    }

    /// Полный адрес узла, публикуемый другим узлам кластера.
    pub fn advertise_addr(&self) -> String {
        self.advertise_addr
            .clone()
            .unwrap_or_else(|| format!("{}:{}", self.addr, self.port))
    }

    /// Строка подключения к Redis. Обязательна.
    pub fn redis_url(&self) -> Result<String, ConfigError> {
        self.redis_url
            .clone()
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| ConfigError {
                message: "REDIS_URL не задан: укажите его в окружении или .env".to_string(),
            })
    }

    /// Кластерный токен для внутренних эндпоинтов. Обязателен.
    pub fn internal_api_token(&self) -> Result<String, ConfigError> {
        self.internal_api_token
            .clone()
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| ConfigError {
                message: "INTERNAL_API_TOKEN не задан: укажите его в окружении или .env"
                    .to_string(),
            })
    }

    /// Таймаут HTTP-запросов приложения.
    pub fn http_timeout(&self) -> Duration {
        Duration::from_secs(self.http_timeout_secs)
    }

    /// Таймаут установки HTTP-соединения приложения.
    pub fn http_connect_timeout(&self) -> Duration {
        Duration::from_secs(self.http_connect_timeout_secs)
    }

    /// Таймаут Raft HTTP-запросов.
    pub fn raft_http_timeout(&self) -> Duration {
        Duration::from_secs(self.raft_http_timeout_secs)
    }

    /// Таймаут установки Raft HTTP-соединения.
    pub fn raft_connect_timeout(&self) -> Duration {
        Duration::from_secs(self.raft_connect_timeout_secs)
    }

    /// Интервал фоновой очистки.
    pub fn processor_interval(&self) -> Duration {
        Duration::from_millis(self.processor_interval_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> AppConfig {
        AppConfig {
            id: 1,
            addr: "127.0.0.1".to_string(),
            port: 21001,
            bind_addr: None,
            advertise_addr: None,
            data_dir: PathBuf::from(DEFAULT_DATA_DIR),
            redis_url: Some("redis://localhost/0".to_string()),
            internal_api_token: Some("token".to_string()),
            cluster_nodes: None,
            http_timeout_secs: 10,
            http_connect_timeout_secs: 3,
            raft_http_timeout_secs: 30,
            raft_connect_timeout_secs: 10,
            processor_interval_ms: 250,
            weight_threshold: 5.0,
            cleanup_batch_size: 100,
        }
    }

    #[test]
    fn advertise_addr_falls_back_to_host_port() {
        let cfg = base_config();
        assert_eq!(cfg.advertise_addr(), "127.0.0.1:21001");
    }

    #[test]
    fn advertise_addr_prefers_explicit_value() {
        let mut cfg = base_config();
        cfg.advertise_addr = Some("node1:21001".to_string());
        assert_eq!(cfg.advertise_addr(), "node1:21001");
    }

    #[test]
    fn bind_addr_falls_back_to_port() {
        let cfg = base_config();
        assert_eq!(cfg.bind_addr(), "0.0.0.0:21001");
    }

    #[test]
    fn missing_redis_url_is_an_error() {
        let mut cfg = base_config();
        cfg.redis_url = None;
        assert!(cfg.redis_url().is_err());
        cfg.redis_url = Some("   ".to_string());
        assert!(cfg.redis_url().is_err());
    }

    #[test]
    fn missing_token_is_an_error() {
        let mut cfg = base_config();
        cfg.internal_api_token = None;
        assert!(cfg.internal_api_token().is_err());
    }
}
