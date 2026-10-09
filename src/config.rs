//! Конфигурация узла, собираемая из CLI-аргументов и переменных окружения.
//!
//! Приоритет значений: аргумент CLI > переменная окружения > значение по умолчанию.
//! Все секреты (Redis URL, кластерный токен) обязаны приходить из окружения и не
//! должны присутствовать в исходном коде.

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use tracing::warn;

/// Каталог персистентных данных по умолчанию.
pub const DEFAULT_DATA_DIR: &str = "./data";

/// Порт HTTP-сервера по умолчанию (если не заданы ни `HTTP_PORT`, ни `BIND_ADDR`).
pub const DEFAULT_HTTP_PORT: u16 = 21001;

/// Профиль окружения (O8): набор требований к конфигурации при старте.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Profile {
    /// Локальная разработка: обязательны только Redis/токен, остальное — по умолчанию.
    Dev,
    /// Предпродукционное окружение: проверяются обязательные секреты и их длина.
    Stage,
    /// Продукционное окружение: самые строгие требования к секретам.
    Prod,
}

impl Profile {
    /// Минимальная длина кластерного токена для профиля.
    pub fn min_token_len(&self) -> usize {
        match self {
            Profile::Dev => 1,
            Profile::Stage => 16,
            Profile::Prod => 32,
        }
    }

    /// Требуется ли в этом профиле явно заданный `ADVERTISE_ADDR`.
    pub fn require_advertise_addr(&self) -> bool {
        matches!(self, Profile::Prod)
    }
}

impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Profile::Dev => "dev",
            Profile::Stage => "stage",
            Profile::Prod => "prod",
        })
    }
}

/// Формат структурированных логов (O2): текст или JSON.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormat {
    /// Человекочитаемый текст (локальная разработка).
    Text,
    /// Однострочный JSON (сбор внешними системами).
    Json,
}

/// Конфигурация узла.
#[derive(Parser, Clone, Debug)]
#[command(author, version, about, long_about = None)]
pub struct AppConfig {
    /// Уникальный ID узла в кластере Raft.
    #[arg(long, env = "NODE_ID", default_value_t = 1)]
    pub id: u64,

    /// Профиль окружения: dev | stage | prod.
    #[arg(long, env = "APP_PROFILE", value_enum, default_value_t = Profile::Dev)]
    pub profile: Profile,

    /// Формат логов: text | json.
    #[arg(long, env = "LOG_FORMAT", value_enum, default_value_t = LogFormat::Text)]
    pub log_format: LogFormat,

    /// Хост, публикуемый другим узлам кластера (используется, если не задан ADVERTISE_ADDR).
    #[arg(long, env = "ADVERTISE_HOST", default_value = "127.0.0.1")]
    pub addr: String,

    /// Порт HTTP-сервера. Задаётся `HTTP_PORT`/`--port`; если не задан —
    /// вычисляется из `BIND_ADDR` или берётся значение по умолчанию.
    #[arg(long, env = "HTTP_PORT")]
    pub port: Option<u16>,

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

    /// Доверять заголовку `X-Forwarded-For` при определении ключа rate limit.
    /// Включайте только если узел стоит за доверенным прокси/балансировщиком
    /// (Q4/C44): иначе клиент подделает свой IP.
    #[arg(long, env = "TRUST_PROXY", default_value_t = false)]
    pub trust_proxy: bool,

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

    /// Таймаут подключения к Redis при старте, секунды. При недоступном/неверно
    /// сконфигурированном Redis узел завершается с понятной ошибкой, а не зависает.
    #[arg(long, env = "REDIS_CONNECT_TIMEOUT_SECS", default_value_t = 5)]
    pub redis_connect_timeout_secs: u64,

    /// Интервал фоновой очистки «тяжёлых» узлов, миллисекунды.
    #[arg(long, env = "PROCESSOR_INTERVAL_MS", default_value_t = 250)]
    pub processor_interval_ms: u64,

    /// Порог веса (после насыщения, диапазон [0,1)), выше которого узел удаляется.
    #[arg(long, env = "WEIGHT_THRESHOLD", default_value_t = 0.5)]
    pub weight_threshold: f64,

    /// Максимальный размер батча кандидатов на очистку за цикл.
    #[arg(long, env = "CLEANUP_BATCH_SIZE", default_value_t = 100)]
    pub cleanup_batch_size: usize,

    /// Лимит запросов в секунду на публичный `POST /` (O3). 0 — выключено.
    #[arg(long, env = "PUBLIC_RATE_LIMIT_PER_SEC", default_value_t = 50)]
    pub public_rate_limit_per_sec: u32,

    /// Максимальный всплеск для rate limiter (O3).
    #[arg(long, env = "PUBLIC_RATE_LIMIT_BURST", default_value_t = 100)]
    pub public_rate_limit_burst: u32,

    /// Advisory-лимит размера тела запроса в байтах (O3).
    #[arg(long, env = "MAX_REQUEST_BYTES", default_value_t = 1_048_576)]
    pub max_request_bytes: usize,
}

/// Ошибка загрузки/валидации конфигурации.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    /// Человекочитаемое описание ошибки.
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
    ///
    /// Приоритет (F2): явный `HTTP_PORT`/`--port` перекрывает `BIND_ADDR`, который мог
    /// прийти из `.env` (dotenvy подхватывает его из CWD и навязывает всем узлам один
    /// адрес). Явно переданный в процессе/CLI `BIND_ADDR` без `HTTP_PORT` используется
    /// как есть. Иначе — `0.0.0.0:<порт из PORT, BIND_ADDR или DEFAULT_HTTP_PORT>`.
    pub fn bind_addr(&self) -> String {
        match (&self.bind_addr, self.port) {
            (_, Some(port)) => {
                if let Some(bind) = &self.bind_addr {
                    warn!("HTTP_PORT={port} перекрывает BIND_ADDR={bind}: слушаем 0.0.0.0:{port}");
                }
                format!("0.0.0.0:{port}")
            }
            (Some(bind), None) => bind.clone(),
            (None, None) => format!("0.0.0.0:{}", self.effective_port()),
        }
    }

    /// Полный адрес узла, публикуемый другим узлам кластера.
    pub fn advertise_addr(&self) -> String {
        self.advertise_addr
            .clone()
            .unwrap_or_else(|| format!("{}:{}", self.addr, self.effective_port()))
    }

    /// Эффективный порт узла: явный `HTTP_PORT`/`--port`, иначе порт из `BIND_ADDR`,
    /// иначе [`DEFAULT_HTTP_PORT`]. Используется для advertise-адреса и значения по умолчанию.
    fn effective_port(&self) -> u16 {
        if let Some(port) = self.port {
            return port;
        }
        if let Some(port) = self.bind_addr.as_deref().and_then(port_from_addr) {
            return port;
        }
        DEFAULT_HTTP_PORT
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

    /// Таймаут подключения к Redis при старте.
    pub fn redis_connect_timeout(&self) -> Duration {
        Duration::from_secs(self.redis_connect_timeout_secs)
    }

    /// Интервал фоновой очистки.
    pub fn processor_interval(&self) -> Duration {
        Duration::from_millis(self.processor_interval_ms)
    }

    /// Advisory-лимит размера тела запроса.
    pub fn max_request_bytes(&self) -> usize {
        self.max_request_bytes
    }

    /// Проверяет конфигурацию под выбранный профиль (O8).
    ///
    /// Собирает **все** проблемы сразу и возвращает их одним понятным сообщением,
    /// чтобы при старте не приходилось исправлять ошибки по одной.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut problems: Vec<String> = Vec::new();

        if self.id == 0 {
            problems.push("NODE_ID: идентификатор узла должен быть больше 0".to_string());
        }
        if !self.weight_threshold.is_finite()
            || self.weight_threshold <= 0.0
            || self.weight_threshold >= 1.0
        {
            problems.push(
                "WEIGHT_THRESHOLD: должен быть в диапазоне (0, 1), т.к. вес насыщается в [0,1)"
                    .to_string(),
            );
        }
        if self.cleanup_batch_size == 0 {
            problems.push("CLEANUP_BATCH_SIZE: должен быть больше 0".to_string());
        }
        if self.processor_interval_ms == 0 {
            problems.push("PROCESSOR_INTERVAL_MS: должен быть больше 0".to_string());
        }
        if self.redis_connect_timeout_secs == 0 {
            problems.push("REDIS_CONNECT_TIMEOUT_SECS: должен быть больше 0".to_string());
        }
        if self.max_request_bytes == 0 {
            problems.push("MAX_REQUEST_BYTES: должен быть больше 0".to_string());
        }
        if self.public_rate_limit_per_sec > 0 && self.public_rate_limit_burst == 0 {
            problems.push(
                "PUBLIC_RATE_LIMIT_BURST: должен быть больше 0 при включённом rate limiting"
                    .to_string(),
            );
        }

        match self.redis_url() {
            Ok(_) => {}
            Err(e) => problems.push(e.message),
        }

        match self.internal_api_token() {
            Ok(token) => {
                let min = self.profile.min_token_len();
                if token.len() < min {
                    problems.push(format!(
                        "INTERNAL_API_TOKEN: для профиля '{}' требуется минимум {} символов (сейчас {})",
                        self.profile,
                        min,
                        token.len()
                    ));
                }
            }
            Err(e) => problems.push(e.message),
        }

        if self.profile.require_advertise_addr() && self.advertise_addr.is_none() {
            problems.push(format!(
                "ADVERTISE_ADDR: обязателен для профиля '{}' (нельзя полагаться на ADVERTISE_HOST/PORT)",
                self.profile
            ));
        }

        if problems.is_empty() {
            Ok(())
        } else {
            Err(ConfigError {
                message: format!(
                    "некорректная конфигурация (профиль {}):\n  - {}",
                    self.profile,
                    problems.join("\n  - ")
                ),
            })
        }
    }
}

/// Извлекает порт из `host:port` (`0.0.0.0:21004` → `Some(21004)`).
/// Возвращает `None`, если порт отсутствует или не парсится как `u16`.
fn port_from_addr(addr: &str) -> Option<u16> {
    addr.rsplit_once(':')?.1.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_config() -> AppConfig {
        AppConfig {
            id: 1,
            profile: Profile::Dev,
            log_format: LogFormat::Text,
            addr: "127.0.0.1".to_string(),
            port: Some(21001),
            bind_addr: None,
            advertise_addr: None,
            data_dir: PathBuf::from(DEFAULT_DATA_DIR),
            redis_url: Some("redis://localhost/0".to_string()),
            internal_api_token: Some("token".to_string()),
            trust_proxy: false,
            http_timeout_secs: 10,
            http_connect_timeout_secs: 3,
            raft_http_timeout_secs: 30,
            raft_connect_timeout_secs: 10,
            redis_connect_timeout_secs: 5,
            processor_interval_ms: 250,
            weight_threshold: 0.5,
            cleanup_batch_size: 100,
            public_rate_limit_per_sec: 50,
            public_rate_limit_burst: 100,
            max_request_bytes: 1_048_576,
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

    /// F2 (регрессионный): явный `HTTP_PORT` не должен теряться молча, даже если
    /// `BIND_ADDR` подсунут из `.env` общим адресом `21001`.
    #[test]
    fn bind_addr_prefers_explicit_port_over_bind_addr() {
        let mut cfg = base_config();
        cfg.bind_addr = Some("0.0.0.0:21001".to_string());
        cfg.port = Some(21004);
        assert_eq!(cfg.bind_addr(), "0.0.0.0:21004");
    }

    #[test]
    fn bind_addr_uses_bind_when_port_not_set() {
        let mut cfg = base_config();
        cfg.bind_addr = Some("0.0.0.0:21004".to_string());
        cfg.port = None;
        assert_eq!(cfg.bind_addr(), "0.0.0.0:21004");
    }

    #[test]
    fn bind_addr_defaults_when_neither_set() {
        let mut cfg = base_config();
        cfg.bind_addr = None;
        cfg.port = None;
        assert_eq!(cfg.bind_addr(), format!("0.0.0.0:{DEFAULT_HTTP_PORT}"));
    }

    #[test]
    fn advertise_addr_uses_port_from_bind_when_port_not_set() {
        let mut cfg = base_config();
        cfg.bind_addr = Some("0.0.0.0:21004".to_string());
        cfg.port = None;
        cfg.advertise_addr = None;
        assert_eq!(cfg.advertise_addr(), "127.0.0.1:21004");
    }

    #[test]
    fn redis_connect_timeout_default_is_five_seconds() {
        let cfg = base_config();
        assert_eq!(cfg.redis_connect_timeout(), Duration::from_secs(5));
    }

    #[test]
    fn validate_rejects_zero_redis_connect_timeout() {
        let mut cfg = base_config();
        cfg.redis_connect_timeout_secs = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.message.contains("REDIS_CONNECT_TIMEOUT_SECS"));
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

    #[test]
    fn validate_accepts_sound_dev_config() {
        let cfg = base_config();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_reports_all_problems_at_once() {
        let mut cfg = base_config();
        cfg.id = 0;
        cfg.weight_threshold = 1.5;
        cfg.redis_url = None;
        cfg.internal_api_token = None;
        let err = cfg.validate().unwrap_err();
        // Все проблемы перечислены в одном сообщении.
        assert!(err.message.contains("NODE_ID"));
        assert!(err.message.contains("WEIGHT_THRESHOLD"));
        assert!(err.message.contains("REDIS_URL"));
        assert!(err.message.contains("INTERNAL_API_TOKEN"));
    }

    #[test]
    fn validate_enforces_profile_token_length() {
        let mut cfg = base_config();
        cfg.profile = Profile::Prod;
        cfg.advertise_addr = Some("node1:21001".to_string());
        cfg.internal_api_token = Some("short".to_string());
        assert!(cfg.validate().is_err());

        cfg.internal_api_token = Some("x".repeat(32));
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_prod_requires_advertise_addr() {
        let mut cfg = base_config();
        cfg.profile = Profile::Prod;
        cfg.internal_api_token = Some("x".repeat(32));
        cfg.advertise_addr = None;
        assert!(cfg.validate().is_err());
    }
}
