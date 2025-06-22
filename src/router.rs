use std::collections::BTreeMap;
use reqwest::Client;
use openraft::error::Unreachable;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::{debug, error};

use crate::encode;
use crate::decode;
use crate::typ::RaftError;
use crate::NodeId;

/// Симулирует сетевой маршрутизатор, отправляя HTTP-запросы между узлами Raft.
#[derive(Debug, Clone, Default)]
pub struct Router {
    /// HTTP-клиент для отправки запросов.
    client: Client,
    /// Карта адресов узлов, например, {1: "http://localhost:8080"}.
    node_addresses: BTreeMap<NodeId, String>,
}

impl Router {
    /// Создаёт новый маршрутизатор с указанными адресами узлов.
    pub fn new(node_addresses: BTreeMap<NodeId, String>) -> Self {
        Router {
            client: Client::new(),
            node_addresses,
        }
    }

    /// Отправляет запрос `Req` на узел `to` по пути `path` и ждёт ответ `Result<Resp, RaftError>`.
    pub async fn send<Req, Resp>(
        &self,
        to: NodeId,
        path: &str,
        req: Req,
    ) -> Result<Resp, Unreachable>
    where
        Req: Serialize,
        Result<Resp, RaftError>: DeserializeOwned,
    {
        // Получаем адрес целевого узла (например, "http://localhost:8080").
        let addr = self.node_addresses.get(&to).expect("Node not found");

        // Формируем полный URL, например, "http://localhost:8080/raft/append".
        let url = format!("{}/{}", addr.trim_end_matches('/'), path.trim_start_matches('/'));
        debug!("Sending request to {}: {}", url, encode(&req));

        // Кодируем запрос в строку.
        let encoded_req = encode(req);

        // Отправляем HTTP POST-запрос.
        let response = self.client
            .post(&url)
            .body(encoded_req)
            .send()
            .await
            .map_err(|e| {
                error!("Failed to send request to {}: {}", url, e);
                Unreachable::new(&e)
            })?;

        // Получаем тело ответа как строку.
        let resp_str = response
            .text()
            .await
            .map_err(|e| {
                error!("Failed to read response from {}: {}", url, e);
                Unreachable::new(&e)
            })?;

        debug!("Received response from {}: {}", url, resp_str);

        // Декодируем ответ в Result<Resp, RaftError>.
        decode::<Result<Resp, RaftError>>(&resp_str)
            .map_err(|e| {
                error!("Failed to decode response from {}: {}", url, e);
                Unreachable::new(&e)
            })
    }
}