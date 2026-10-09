//! Состояние приложения (`App`), разделяемое HTTP-хендлерами.

use std::sync::Arc;
use std::time::Duration;

use redis::aio::ConnectionManager;

use crate::NodeId;
use crate::metrics::Metrics;
use crate::raft::{store::StateMachineStore, typ::Raft};
use crate::ratelimit::RateLimiter;
use crate::shutdown::Shutdown;
use crate::web::Router;

/// Состояние приложения: идентификация узла, Raft, state machine, клиенты,
/// наблюдаемость (метрики), лимиты и настройки фоновой очистки. Клонируется в
/// axum-хранилище состояния.
#[derive(Clone)]
pub struct App {
    /// ID текущего узла.
    pub id: NodeId,
    /// Адрес узла, публикуемый кластеру.
    pub addr: String,
    /// Дескриптор локального экземпляра Raft.
    pub raft: Raft,
    /// Персистентное состояние (DAG, membership, снапшот).
    pub state_machine: Arc<StateMachineStore>,
    /// Внутрикластерный HTTP-клиент.
    pub router: Router,
    /// Соединение с Redis (архив подтверждённых узлов).
    pub redis: ConnectionManager,
    /// Кластерный токен для внутренних эндпоинтов.
    pub internal_api_token: Arc<str>,
    /// Интервал фоновой очистки «тяжёлых» узлов.
    pub processor_interval: Duration,
    /// Порог веса, выше которого узел считается «тяжёлым».
    pub weight_threshold: f64,
    /// Максимальный размер батча очистки за цикл.
    pub cleanup_batch_size: usize,
    /// Реестр метрик Prometheus (O2).
    pub metrics: Arc<Metrics>,
    /// Лимитер публичного API (O3).
    pub rate_limiter: Arc<RateLimiter>,
    /// Advisory-лимит размера тела запроса в байтах (O3).
    pub max_request_bytes: usize,
    /// Сигнал graceful shutdown (O5).
    pub shutdown: Shutdown,
}

impl App {
    /// Создаёт состояние приложения из уже собранных компонентов.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: NodeId,
        addr: String,
        raft: Raft,
        state_machine: Arc<StateMachineStore>,
        router: Router,
        redis: ConnectionManager,
        internal_api_token: Arc<str>,
        processor_interval: Duration,
        weight_threshold: f64,
        cleanup_batch_size: usize,
        metrics: Arc<Metrics>,
        rate_limiter: Arc<RateLimiter>,
        max_request_bytes: usize,
        shutdown: Shutdown,
    ) -> Self {
        Self {
            id,
            addr,
            raft,
            state_machine,
            router,
            redis,
            internal_api_token,
            processor_interval,
            weight_threshold,
            cleanup_batch_size,
            metrics,
            rate_limiter,
            max_request_bytes,
            shutdown,
        }
    }

    /// Собирает снимок метрик Raft из текущего состояния и обновляет реестр.
    pub fn refresh_raft_metrics(&self) {
        let m = self.raft.metrics().borrow().clone();
        let last_log_index = m.last_log_index.unwrap_or(0);
        let last_applied_index = m.last_applied.map(|l| l.index).unwrap_or(0);
        let snapshot_index = m.snapshot.map(|l| l.index).unwrap_or(0);
        let leader_id = m.current_leader.unwrap_or(0);
        self.metrics.set_raft(crate::metrics::RaftMetricSnapshot {
            term: m.current_term,
            last_log_index,
            last_applied_index,
            snapshot_index,
            is_leader: m.current_leader == Some(self.id),
            leader_id,
            members: m.membership_config.membership().nodes().count(),
        });
    }

    /// Обновляет метрики DAG из текущего состояния.
    pub async fn refresh_dag_metrics(&self) {
        let sm = self.state_machine.state_machine.read().await;
        self.metrics
            .set_dag(sm.dag.get_node_count(), sm.dag.added_len());
        self.metrics.set_weight_threshold(self.weight_threshold);
    }
}
