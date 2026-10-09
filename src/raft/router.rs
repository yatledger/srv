use openraft::error::Unreachable;
use reqwest::Client;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::time::Duration;
use tracing::{debug, error};

use super::typ::*;
use crate::NodeId;

/// Унифицированный сетевой маршрутизатор для Raft
#[derive(Debug, Clone)]
pub struct Router {
    client: Client,
    /// Кластерный токен, добавляемый к внутренним Raft-запросам.
    internal_token: String,
}

impl Router {
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
                error!("Failed to build HTTP client for Raft ({e}); using default client");
                Client::new()
            });

        Router {
            client,
            internal_token,
        }
    }

    /// Унифицированный метод отправки запросов
    pub async fn send<Req, Resp>(
        &self,
        to: NodeId,
        addr: String,
        path: &str,
        req: Req,
    ) -> Result<Resp, Unreachable>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        Result<Resp, RaftError>: DeserializeOwned,
    {
        let url = format!(
            "http://{}/{}",
            addr.trim_end_matches('/'),
            path.trim_start_matches('/')
        );

        // Логируем запрос
        if let Ok(req_json) = serde_json::to_string(&req) {
            debug!(">>> network send request to [{}] {}: {}", to, url, req_json);
        }

        // Отправляем HTTP запрос
        let response = self.send_http_request(&url, &req).await?;

        // Читаем ответ
        let response_text = self.read_response_text(response, &url).await?;

        // Парсим ответ
        self.parse_response(&response_text, &url).await
    }

    /// Отправка HTTP запроса
    async fn send_http_request<Req: Serialize>(
        &self,
        url: &str,
        req: &Req,
    ) -> Result<reqwest::Response, Unreachable> {
        self.client
            .post(url)
            .header(crate::auth::INTERNAL_TOKEN_HEADER, &self.internal_token)
            .json(req)
            .send()
            .await
            .map_err(|e| {
                error!("Failed to send request to {}: {}", url, e);
                Unreachable::new(&e)
            })
    }

    /// Чтение тела ответа
    async fn read_response_text(
        &self,
        response: reqwest::Response,
        url: &str,
    ) -> Result<String, Unreachable> {
        // Проверяем статус
        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            error!("HTTP error {} from {}: {}", status, url, error_body);
            return Err(Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("HTTP error {}: {}", status, error_body),
            )));
        }

        // Читаем тело ответа
        let response_text = response.text().await.map_err(|e| {
            error!("Failed to read response from {}: {}", url, e);
            Unreachable::new(&e)
        })?;

        debug!("<<< network recv reply from {}: {}", url, response_text);

        // Проверяем на пустой ответ
        if response_text.is_empty() {
            error!("Empty response from {}", url);
            return Err(Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Empty response from remote node",
            )));
        }

        Ok(response_text)
    }

    /// Унифицированный парсинг ответа
    async fn parse_response<Resp: DeserializeOwned>(
        &self,
        response_text: &str,
        url: &str,
    ) -> Result<Resp, Unreachable>
    where
        Result<Resp, RaftError>: DeserializeOwned,
    {
        // Сначала проверяем, что это валидный JSON
        let _: Value = serde_json::from_str(response_text).map_err(|e| {
            error!("Invalid JSON from {}: {}. Data: {}", url, e, response_text);
            Unreachable::new(&e)
        })?;

        // Стратегия 1: Пробуем парсить как прямой объект Resp
        if let Ok(direct_result) = serde_json::from_str::<Resp>(response_text) {
            debug!("Successfully parsed direct response from {}", url);
            return Ok(direct_result);
        }

        // Стратегия 2: Пробуем парсить как Result<Resp, RaftError>
        match serde_json::from_str::<Result<Resp, RaftError>>(response_text) {
            Ok(Ok(success_result)) => {
                debug!("Successfully parsed wrapped success response from {}", url);
                Ok(success_result)
            }
            Ok(Err(raft_error)) => {
                error!("Raft error from {}: {:?}", url, raft_error);
                Err(Unreachable::new(&std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Raft error: {:?}", raft_error),
                )))
            }
            Err(parse_error) => {
                // Стратегия 3: Пробуем парсить как объект с известными полями
                if let Ok(value) = serde_json::from_str::<Value>(response_text) {
                    if let Some(error_info) = self.extract_error_info(&value) {
                        error!("Error response from {}: {}", url, error_info);
                        return Err(Unreachable::new(&std::io::Error::new(
                            std::io::ErrorKind::Other,
                            error_info,
                        )));
                    }
                }

                error!(
                    "Failed to parse response from {}: {}. Data: {}",
                    url, parse_error, response_text
                );
                Err(Unreachable::new(&parse_error))
            }
        }
    }

    /// Извлечение информации об ошибке из JSON
    fn extract_error_info(&self, value: &Value) -> Option<String> {
        // Проверяем различные форматы ошибок
        if let Some(error_msg) = value.get("error").and_then(|v| v.as_str()) {
            return Some(error_msg.to_string());
        }

        if let Some(message) = value.get("message").and_then(|v| v.as_str()) {
            return Some(message.to_string());
        }

        if let Some(err_type) = value.get("type").and_then(|v| v.as_str()) {
            return Some(format!("Error type: {}", err_type));
        }

        None
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new(
            Duration::from_secs(30),
            Duration::from_secs(10),
            String::new(),
        )
    }
}
