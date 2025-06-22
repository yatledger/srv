use std::collections::BTreeMap;
use std::time::Duration;
use reqwest::Client;
use openraft::error::Unreachable;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::{debug, info, error, warn};

use crate::encode;
use crate::decode;
use crate::typ::RaftError;
use crate::NodeId;

/// Симулирует сетевой маршрутизатор, отправляя HTTP-запросы между узлами Raft.
#[derive(Debug, Clone)]
pub struct Router {
    /// HTTP-клиент для отправки запросов.
    client: Client,
    /// Карта адресов узлов, например, {1: "http://localhost:8080"}.
    pub targets: BTreeMap<NodeId, String>,
}

impl Router {
    /// Создаёт новый маршрутизатор с указанными адресами узлов.
    pub fn new(targets: BTreeMap<NodeId, String>) -> Self {
        // Настраиваем HTTP-клиент с разумными таймаутами
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .expect("Failed to create HTTP client");

        Router { client, targets }
    }

    /// Отправляет запрос `Req` на узел `to` по пути `path` и ждёт ответ `Result<Resp, RaftError>`.
    pub async fn send<Req, Resp>(
        &self,
        to: NodeId,
        addr: &str,
        path: &str,
        req: Req,
    ) -> Result<Resp, Unreachable>
    where
        Req: Serialize,
        Result<Resp, RaftError>: DeserializeOwned,
    {
        /*// Получаем адрес целевого узла
        let addr = self.targets.get(&to).ok_or_else(|| {
            error!("Node {} not found in targets", to);
        });

        // Формируем полный URL
        let url = format!("{}/{}", addr.trim_end_matches('/'), path.trim_start_matches('/'));*/
        // let addr = self.targets.get(&to).expect("Node not found");

        // Формируем полный URL, например, "http://localhost:8080/raft/append".
        let url = format!("http://{}/{}", addr.trim_end_matches('/'), path.trim_start_matches('/'));
        info!("Sending request to {} {}: {}", to, url, encode(&req));
        
        // Кодируем запрос
        let encoded_req = encode(req);
        info!("Sending request to {}: {}", url, encoded_req);

        // Отправляем HTTP POST-запрос с правильным Content-Type
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(encoded_req)
            .send()
            .await
            .map_err(|e| {
                error!("Failed to send request to {}: {}", url, e);
                Unreachable::new(&e)
            })?;

        /*// Проверяем статус ответа
        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            error!("HTTP error {} from {}: {}", status, url, error_body);
            return Err(Unreachable::new(&format!("HTTP {}: {}", status, error_body)));
        }*/

        // Получаем тело ответа
        let resp_str = response
            .text()
            .await
            .map_err(|e| {
                error!("Failed to read response from {}: {}", url, e);
                Unreachable::new(&e)
            })?;

        debug!("Received response from {}: {}", url, resp_str);

        // Декодируем ответ
        // TODO если пустой
        decode::<Result<Resp, RaftError>>(&resp_str)
            .map_err(|e| {
                error!("Failed to decode response from {}: {}", url, e);
                Unreachable::new(&e)
            })
    }

    /// Добавляет новый узел в targets
    pub fn add_target(&mut self, node_id: NodeId, addr: String) {
        info!("Adding target: {} -> {}", node_id, addr);
        self.targets.insert(node_id, addr);
    }

    /// Удаляет узел из targets
    pub fn remove_target(&mut self, node_id: NodeId) {
        if self.targets.remove(&node_id).is_some() {
            info!("Removed target: {}", node_id);
        } else {
            warn!("Tried to remove non-existent target: {}", node_id);
        }
    }

    /// Проверяет доступность узла
    pub async fn health_check(&self, node_id: NodeId) -> bool {
        if let Some(addr) = self.targets.get(&node_id) {
            let url = format!("{}/health", addr.trim_end_matches('/'));
            match self.client.get(&url).send().await {
                Ok(response) => response.status().is_success(),
                Err(_) => false,
            }
        } else {
            false
        }
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new(BTreeMap::new())
    }
}