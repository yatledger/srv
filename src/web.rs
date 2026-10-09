// src/web.rs

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
    Success(T),
    Error { status: String, message: String },
}

/// Универсальный тип ошибки для ApiRouter, описывающий все возможные проблемы.
#[derive(Debug)]
pub enum ApiRouterError {
    Network(reqwest::Error),
    Http { status: StatusCode, text: String },
    Api { message: String },
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

/// Новый роутер, предназначенный ИСКЛЮЧИТЕЛЬНО для внутрикластерных API-запросов.
#[derive(Debug, Clone)]
pub struct Router {
    client: Client,
    /// Кластерный токен, добавляемый к внутренним запросам.
    internal_token: String,
}

impl Router {
    pub fn new(timeout: Duration, connect_timeout: Duration, internal_token: String) -> Self {
        let client = Client::builder()
            .timeout(timeout)
            .connect_timeout(connect_timeout)
            .build()
            .expect("Failed to create HTTP client for ApiRouter");
        Self {
            client,
            internal_token,
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
        debug!(
            ">>> API request send to {}: {}",
            url,
            serde_json::to_string(&req).unwrap()
        );

        let response = self
            .client
            .post(&url)
            .header(crate::auth::INTERNAL_TOKEN_HEADER, &self.internal_token)
            .json(&req)
            .send()
            .await?;

        let status = response.status();
        let body_text = response.text().await.map_err(ApiRouterError::Network)?;

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

        debug!("<<< API reply recv from {}: {}", url, body_text);

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
