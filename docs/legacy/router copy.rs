use std::collections::BTreeMap;
use std::time::Duration;
use reqwest::Client;
use openraft::error::Unreachable;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::{debug, info, error, warn};

use crate::typ::*;
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
    pub async fn send<Req, Resp, Err>(&self, url: &str, req: Option<&Req>) -> Result<Result<Resp, Err>, RPCError>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error + Serialize + DeserializeOwned,
    {

        let resp = if let Some(r) = req {
            println!(
                ">>> client send request to {}: {}",
                url,
                serde_json::to_string_pretty(&r).unwrap()
            );
            self.client.post(url.clone()).json(r)
        } else {
            println!(">>> client send request to {}", url,);
            self.client.get(url.clone())
        }
        .send()
        .await
        .map_err(|e| {
            if e.is_connect() {
                // `Unreachable` informs the caller to backoff for a short while to avoid error log flush.
                RPCError::Unreachable(Unreachable::new(&e))
            } else {
                RPCError::Network(NetworkError::new(&e))
            }
        })?;

        let res: Result<Resp, Err> = resp.json().await.map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        println!(
            "<<< client recv reply from {}: {}",
            url,
            serde_json::to_string_pretty(&res).unwrap()
        );

        Ok(res)
    }

}

impl Default for Router {
    fn default() -> Self {
        Self::new(BTreeMap::new())
    }
}