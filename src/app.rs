use std::sync::Arc;
use axum::{routing::post, Router as AxumRouter};
use tracing::info;

use crate::api;
use crate::router::Router;
use crate::typ;
use crate::NodeId;
use crate::StateMachineStore;
use tokio::net::TcpListener;

use tracing::{error};

/// Representation of an application state.
pub struct App {
    pub id: NodeId,
    pub raft: typ::Raft,
    pub router: Router,
    pub state_machine: Arc<StateMachineStore>,
}

impl App {
    pub fn new(id: NodeId, raft: typ::Raft, router: Router, state_machine: Arc<StateMachineStore>) -> Self {
        Self {
            id,
            raft,
            router,
            state_machine,
        }
    }
let res = match path.as_str() {
                // Application API
                "/app/write" => api::write(&mut self, payload).await,
                "/app/read" => api::read(&mut self, payload).await,

                // Raft API
                "/raft/append" => api::append(&mut self, payload).await,
                "/raft/snapshot" => api::snapshot(&mut self, payload).await,
                "/raft/vote" => api::vote(&mut self, payload).await,

                // Management API
                "/mng/add-learner" => api::add_learner(&mut self, payload).await,
                "/mng/change-membership" => api::change_membership(&mut self, payload).await,
                "/mng/init" => api::init(&mut self).await,
                "/mng/metrics" => api::metrics(&mut self).await,

                _ => panic!("unknown path: {}", path),
            };
    /// Запускает HTTP-сервер для обработки клиентских и Raft-запросов
    pub async fn start(self: Arc<Self>, addr: &str) -> Result<(), Box<dyn std::error::Error>>{
        let app = self.clone();
        let axum_router = AxumRouter::new()
            .route("/app/write", post(move |body: String| async move {
                api::write(&mut app.as_ref(), body).await
            }))
            .route("/app/read", post(move |body: String| async move {
                api::read(&mut app.as_ref(), body).await
            }))
            .route("/raft/append", post(move |body: String| async move {
                api::append(&mut app.as_ref(), body).await
            }))
            .route("/raft/snapshot", post(move |body: String| async move {
                api::snapshot(&mut app.as_ref(), body).await
            }))
            .route("/raft/vote", post(move |body: String| async move {
                api::vote(&mut app.as_ref(), body).await
            }))
            .route("/mng/add-learner", post(move |body: String| async move {
                api::add_learner(&mut app.as_ref(), body).await
            }))
            .route("/mng/change-membership", post(move |body: String| async move {
                api::change_membership(&mut app.as_ref(), body).await
            }))
            .route("/mng/init", post(move |body: String| async move {
                api::init(&mut app.as_ref()).await
            }))
            .route("/mng/metrics", post(move |body: String| async move {
                api::metrics(&mut app.as_ref()).await
            }));

        let addr: String = addr.parse().map_err(|e| {
            error!("Invalid address: {}", e);
            e
        }).expect("Invalid address");
        info!("Starting HTTP server on {}", addr);
        let listener = TcpListener::bind(&addr).await?;

        // Запускаем Axum-сервер, который обрабатывает запросы.
        axum::serve(listener, axum_router).await?;

        Ok(())
    }
}