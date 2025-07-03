use std::time::Duration;
use reqwest::Client;
use openraft::error::Unreachable;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::{debug, error};

//use anyhow::Error;
use super::decode;
use super::typ::*;
use crate::NodeId;

/// Симулирует сетевой маршрутизатор, отправляя HTTP-запросы между узлами Raft.
#[derive(Debug, Clone)]
pub struct Router {
    client: Client,
}

impl Router {
    /// Создаёт новый маршрутизатор с указанными адресами узлов.
    pub fn new() -> Self {
        // Настраиваем HTTP-клиент с разумными таймаутами
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .expect("Failed to create HTTP client");

        Router { client }
    }
    
    /// Отправляет запрос `Req` на узел `to` по пути `path` и ждёт ответ `Result<Resp, RaftError>`.
    pub async fn send<Req, Resp>(
        &self,
        to: NodeId,
        addr: String,
        path: &str,
        req: Req,
    ) -> Result<Resp, Unreachable>
    where
        Req: Serialize,
        Result<Resp, RaftError>: DeserializeOwned,
    {
        let url = format!("http://{}/{}", addr.trim_end_matches('/'), path.trim_start_matches('/'));
        debug!(">>> network send request to [{}] {}: {}", to, url, serde_json::to_string(&req).unwrap());

        // Отправляем HTTP POST-запрос с правильным Content-Type
        let response = self.client
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| {
                error!("Failed to send request to {}: {}", url, e);
                Unreachable::new(&e)
            })?;

        // Проверяем статус ответа
        /*if !&response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            error!("HTTP error {} from {}: {}", status, url, error_body);
            //return Err(Unreachable::new(&Error::msg(error_body)));
        }*/

        // Получаем тело ответа
        let res = response
            .text()
            .await
            .map_err(|e| {
                error!("Failed to read response from {}: {}", url, e);
                Unreachable::new(&e)
            })?;

        debug!("<<< network recv reply from {}: {}", url, res);

        // Декодируем ответ
        // TODO если пустой
        decode::<Result<Resp, RaftError>>(&res)
            .map_err(|e| {
                error!("Failed to decode response from {}: {}", url, e);
                Unreachable::new(&e)
            })
    }

}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}