use openraft::error::Unreachable;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::time::Duration;
use tracing::{debug, error};

use crate::NodeId;
use crate::web::HttpClient;

/// Сетевой маршрутизатор для Raft RPC.
///
/// Использует общий внутрикластерный `HttpClient` (тот же `reqwest::Client` и
/// кластерный токен, что и API-роутер). Контракт ответа строгий: успешный
/// HTTP-ответ содержит тело, десериализуемое напрямую в `Resp`; любая
/// HTTP-ошибка превращается в `Unreachable`.
#[derive(Debug, Clone)]
pub struct Router {
    http: HttpClient,
}

impl Router {
    pub fn new(timeout: Duration, connect_timeout: Duration, internal_token: String) -> Self {
        Router {
            http: HttpClient::new(timeout, connect_timeout, internal_token),
        }
    }

    /// Отправляет Raft RPC и разбирает строгий JSON-ответ `Resp`.
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
    {
        let url = format!(
            "http://{}/{}",
            addr.trim_end_matches('/'),
            path.trim_start_matches('/')
        );

        debug!(">>> raft rpc to [{}] {}", to, url);

        let (status, body) = self.http.post_text(&url, &req).await.map_err(|e| {
            error!("Failed to send request to {}: {}", url, e);
            Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))
        })?;

        if !status.is_success() {
            error!("HTTP error {} from {}: {}", status, url, body);
            return Err(Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("HTTP error {}: {}", status, body),
            )));
        }

        if body.is_empty() {
            error!("Empty response from {}", url);
            return Err(Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Empty response from remote node",
            )));
        }

        serde_json::from_str::<Resp>(&body).map_err(|e| {
            error!(
                "Failed to parse response from {}: {}. Data: {}",
                url, e, body
            );
            Unreachable::new(&e)
        })
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
