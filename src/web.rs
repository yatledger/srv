//! Единый внутрикластерный HTTP-клиент и разбор ответов приложения.
//!
//! `HttpClient` инкапсулирует один `reqwest::Client`, кластерный токен и общую
//! обработку ошибок. На его основе построены оба роутера: `Router` (публичный
//! контракт `ApiResponse<T>` для `/add`, `/remove_heavy_nodes`) и
//! `raft::router::Router` (контракт Raft RPC).

use axum::http::StatusCode;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, error};

/// Универсальная обертка для всех ответов от внутренних API.
#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum ApiResponse<T> {
    /// Успешный ответ с полезной нагрузкой.
    Success(T),
    /// Ответ с ошибкой.
    Error {
        /// Статус.
        status: String,
        /// Сообщение.
        message: String,
    },
}

/// Универсальный тип ошибки для внутрикластерных HTTP-вызовов.
#[derive(Debug, thiserror::Error)]
pub enum ApiRouterError {
    /// Ошибка сети/транспорта.
    #[error("network error: {0}")]
    Network(reqwest::Error),
    /// HTTP-ответ с неуспешным статусом.
    #[error("HTTP error {status}: {text}")]
    Http {
        /// HTTP-статус.
        status: StatusCode,
        /// Тело ответа.
        text: String,
    },
    /// Приложение вернуло ошибку.
    #[error("API error: {message}")]
    Api {
        /// Сообщение об ошибке.
        message: String,
    },
    /// Не удалось разобрать ответ.
    #[error("deserialization error: {0}")]
    Deserialization(serde_json::Error),
}

impl From<reqwest::Error> for ApiRouterError {
    fn from(err: reqwest::Error) -> Self {
        ApiRouterError::Network(err)
    }
}

impl From<serde_json::Error> for ApiRouterError {
    fn from(err: serde_json::Error) -> Self {
        ApiRouterError::Deserialization(err)
    }
}

/// Единый внутрикластерный HTTP-клиент: общий `reqwest::Client` и токен.
#[derive(Debug, Clone)]
pub struct HttpClient {
    client: Client,
    internal_token: String,
}
impl HttpClient {
    /// Создаёт клиент с таймаутами и кластерным токеном.
    pub fn new(timeout: Duration, connect_timeout: Duration, internal_token: String) -> Self {
        // `build` может упасть только при инициализации TLS-бэкенда; не паникуем,
        // а откатываемся на клиент по умолчанию.
        let client = Client::builder()
            .timeout(timeout)
            .connect_timeout(connect_timeout)
            .pool_idle_timeout(Duration::from_secs(45))
            .pool_max_idle_per_host(10)
            .build()
            .unwrap_or_else(|e| {
                error!("Failed to build internal HTTP client ({e}); using default client");
                Client::new()
            });
        Self {
            client,
            internal_token,
        }
    }

    /// Отправляет JSON POST и возвращает статус и тело ответа.
    pub async fn post_text<Req: Serialize>(
        &self,
        url: &str,
        req: &Req,
    ) -> Result<(StatusCode, String), ApiRouterError> {
        if let Ok(body) = serde_json::to_string(req) {
            debug!(">>> internal request to {}: {}", url, body);
        } else {
            debug!(">>> internal request to {}", url);
        }

        let response = self
            .client
            .post(url)
            .header(crate::auth::INTERNAL_TOKEN_HEADER, &self.internal_token)
            .json(req)
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await.map_err(ApiRouterError::Network)?;
        debug!("<<< internal reply from {}: {}", url, text);
        Ok((status, text))
    }
}

/// Роутер внутреннего API приложения с контрактом `ApiResponse<T>`.
#[derive(Debug, Clone)]
pub struct Router {
    http: HttpClient,
}

impl Router {
    /// Создаёт API-роутер с общим внутрикластерным клиентом.
    pub fn new(timeout: Duration, connect_timeout: Duration, internal_token: String) -> Self {
        Self {
            http: HttpClient::new(timeout, connect_timeout, internal_token),
        }
    }

    /// Отправляет запрос для внутреннего API приложения.
    /// Ожидает ответ в формате `ApiResponse<Resp>`.
    pub async fn send<Req, Resp>(
        &self,
        addr: &str,
        path: &str,
        req: Req,
    ) -> Result<Resp, ApiRouterError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let url = format!(
            "http://{}/{}",
            addr.trim_end_matches('/'),
            path.trim_start_matches('/')
        );

        let (status, body_text) = self.http.post_text(&url, &req).await?;

        if !status.is_success() {
            error!(
                "API request HTTP error {} from {}: {}",
                status, url, body_text
            );
            return Err(ApiRouterError::Http {
                status,
                text: body_text,
            });
        }

        let api_response: ApiResponse<Resp> = serde_json::from_str(&body_text)?;

        match api_response {
            ApiResponse::Success(data) => Ok(data),
            ApiResponse::Error { message, .. } => Err(ApiRouterError::Api { message }),
        }
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new(
            Duration::from_secs(10),
            Duration::from_secs(3),
            String::new(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_router_error_is_a_std_error_with_display() {
        let err = ApiRouterError::Api {
            message: "boom".to_string(),
        };
        assert_eq!(err.to_string(), "API error: boom");
        let _: &dyn std::error::Error = &err;
    }

    #[test]
    fn http_error_display_includes_status() {
        let err = ApiRouterError::Http {
            status: StatusCode::UNAUTHORIZED,
            text: "no token".to_string(),
        };
        assert!(err.to_string().contains("401"));
    }
}
