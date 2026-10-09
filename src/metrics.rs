//! Наблюдаемость (O2): реестр метрик Prometheus и их текстовый экспорт.
//!
//! Все метрики пишутся в собственный [`Registry`], поэтому их сборка не зависит
//! от глобального состояния. Формат вывода — Prometheus text format, который
//! читают Prometheus, VictoriaMetrics, Telegraf и другие сборщики. InfluxDB2
//! подключается внешним сборщиком (Telegraf `inputs.prometheus` или
//! `remote_write`), изменений в коде узла не требуется.

use prometheus::{
    Gauge, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder,
};

/// Снимок метрик Raft, передаваемый в [`Metrics::set_raft`].
#[derive(Debug, Clone, Copy, Default)]
pub struct RaftMetricSnapshot {
    /// Текущий term.
    pub term: u64,
    /// Последний индекс лога.
    pub last_log_index: u64,
    /// Последний применённый индекс (last_applied).
    pub last_applied_index: u64,
    /// Последний индекс снапшота.
    pub snapshot_index: u64,
    /// Является ли узел лидером (1/0).
    pub is_leader: bool,
    /// ID текущего лидера (0 — неизвестен).
    pub leader_id: u64,
    /// Число узлов в конфигурации membership.
    pub members: usize,
}

/// Реестр метрик узла. Клонируется между хендлерами через `Arc`.
#[derive(Debug)]
pub struct Metrics {
    registry: Registry,
    raft_term: IntGauge,
    raft_last_log_index: IntGauge,
    raft_last_applied_index: IntGauge,
    raft_snapshot_index: IntGauge,
    raft_apply_lag: IntGauge,
    raft_is_leader: IntGauge,
    raft_leader_id: IntGauge,
    raft_members: IntGauge,
    dag_nodes: IntGauge,
    dag_added: IntGauge,
    weight_threshold: Gauge,
    cleanup_cycles: IntCounter,
    cleanup_heavy_pending: IntGauge,
    cleanup_removed_nodes: IntCounter,
    tx_total: IntCounterVec,
    http_requests: IntCounterVec,
    http_duration: HistogramVec,
}

macro_rules! reg {
    ($registry:expr, $metric:expr) => {{
        let metric = $metric;
        if let Err(e) = $registry.register(Box::new(metric.clone())) {
            tracing::error!("failed to register metric: {e}");
        }
        metric
    }};
}

impl Metrics {
    /// Создаёт и регистрирует набор метрик узла.
    pub fn new() -> Self {
        let registry = Registry::new();

        let raft_term = reg!(
            registry,
            IntGauge::new("dagdb_raft_current_term", "Текущий term").unwrap()
        );
        let raft_last_log_index = reg!(
            registry,
            IntGauge::new("dagdb_raft_last_log_index", "Последний индекс Raft-лога").unwrap()
        );
        let raft_last_applied_index = reg!(
            registry,
            IntGauge::new(
                "dagdb_raft_last_applied_index",
                "Последний применённый индекс (last_applied)"
            )
            .unwrap()
        );
        let raft_snapshot_index = reg!(
            registry,
            IntGauge::new("dagdb_raft_snapshot_index", "Последний индекс снапшота").unwrap()
        );
        let raft_apply_lag = reg!(
            registry,
            IntGauge::new(
                "dagdb_raft_apply_lag",
                "Очередь применения: last_log_index - last_applied_index"
            )
            .unwrap()
        );
        let raft_is_leader = reg!(
            registry,
            IntGauge::new("dagdb_raft_is_leader", "1, если узел лидер, иначе 0").unwrap()
        );
        let raft_leader_id = reg!(
            registry,
            IntGauge::new(
                "dagdb_raft_leader_id",
                "ID текущего лидера (0 — неизвестен)"
            )
            .unwrap()
        );
        let raft_members = reg!(
            registry,
            IntGauge::new("dagdb_raft_members", "Число узлов в конфигурации кластера").unwrap()
        );
        let dag_nodes = reg!(
            registry,
            IntGauge::new("dagdb_dag_nodes", "Число активных узлов DAG").unwrap()
        );
        let dag_added = reg!(
            registry,
            IntGauge::new(
                "dagdb_dag_added_entries",
                "Размер реестра удалённых узлов (added)"
            )
            .unwrap()
        );
        let weight_threshold = reg!(
            registry,
            Gauge::new(
                "dagdb_weight_threshold",
                "Порог веса для очистки «тяжёлых» узлов"
            )
            .unwrap()
        );
        let cleanup_cycles = reg!(
            registry,
            IntCounter::new("dagdb_cleanup_cycles_total", "Число циклов очистки").unwrap()
        );
        let cleanup_heavy_pending = reg!(
            registry,
            IntGauge::new(
                "dagdb_cleanup_heavy_pending",
                "Число «тяжёлых» узлов в последнем цикле очистки"
            )
            .unwrap()
        );
        let cleanup_removed_nodes = reg!(
            registry,
            IntCounter::new(
                "dagdb_cleanup_removed_nodes_total",
                "Всего архивировано и удалено узлов"
            )
            .unwrap()
        );
        let tx_total = reg!(
            registry,
            IntCounterVec::new(
                Opts::new(
                    "dagdb_tx_total",
                    "Число транзакций по операции и результату"
                ),
                &["op", "result"]
            )
            .unwrap()
        );
        let http_requests = reg!(
            registry,
            IntCounterVec::new(
                Opts::new("dagdb_http_requests_total", "Число HTTP-запросов"),
                &["method", "path", "status"]
            )
            .unwrap()
        );
        let http_duration = reg!(
            registry,
            HistogramVec::new(
                HistogramOpts::new(
                    "dagdb_http_request_duration_seconds",
                    "Длительность HTTP-запросов в секундах"
                )
                .buckets(vec![
                    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                ]),
                &["method", "path"]
            )
            .unwrap()
        );

        Self {
            registry,
            raft_term,
            raft_last_log_index,
            raft_last_applied_index,
            raft_snapshot_index,
            raft_apply_lag,
            raft_is_leader,
            raft_leader_id,
            raft_members,
            dag_nodes,
            dag_added,
            weight_threshold,
            cleanup_cycles,
            cleanup_heavy_pending,
            cleanup_removed_nodes,
            tx_total,
            http_requests,
            http_duration,
        }
    }

    /// Обновляет метрики Raft из снимка состояния.
    pub fn set_raft(&self, s: RaftMetricSnapshot) {
        self.raft_term.set(s.term as i64);
        self.raft_last_log_index.set(s.last_log_index as i64);
        self.raft_last_applied_index
            .set(s.last_applied_index as i64);
        self.raft_snapshot_index.set(s.snapshot_index as i64);
        let lag = s.last_log_index.saturating_sub(s.last_applied_index);
        self.raft_apply_lag.set(lag as i64);
        self.raft_is_leader.set(i64::from(s.is_leader));
        self.raft_leader_id.set(s.leader_id as i64);
        self.raft_members.set(s.members as i64);
    }

    /// Обновляет метрики DAG.
    pub fn set_dag(&self, nodes: usize, added: usize) {
        self.dag_nodes.set(nodes as i64);
        self.dag_added.set(added as i64);
    }

    /// Устанавливает порог веса (отображается как gauge).
    pub fn set_weight_threshold(&self, threshold: f64) {
        self.weight_threshold.set(threshold);
    }

    /// Регистрирует один цикл очистки.
    pub fn record_cleanup(&self, heavy: usize, removed: usize) {
        self.cleanup_cycles.inc();
        self.cleanup_heavy_pending.set(heavy as i64);
        if removed > 0 {
            self.cleanup_removed_nodes.inc_by(removed as u64);
        }
    }

    /// Считает транзакцию: операция (`add`/`remove`) и результат (`ok`/`error`).
    pub fn record_tx(&self, op: &str, ok: bool) {
        let result = if ok { "ok" } else { "error" };
        self.tx_total.with_label_values(&[op, result]).inc();
    }

    /// Наблюдает HTTP-запрос.
    pub fn observe_http(&self, method: &str, path: &str, status: u16, seconds: f64) {
        let status = status.to_string();
        self.http_requests
            .with_label_values(&[method, path, status.as_str()])
            .inc();
        self.http_duration
            .with_label_values(&[method, path])
            .observe(seconds);
    }

    /// Собирает метрики в Prometheus text format.
    pub fn encode(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let families = self.registry.gather();
        encoder.encode_to_string(&families)
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_exposes_expected_metric_names() {
        let m = Metrics::new();
        m.set_raft(RaftMetricSnapshot {
            term: 3,
            last_log_index: 10,
            last_applied_index: 8,
            snapshot_index: 5,
            is_leader: true,
            leader_id: 1,
            members: 3,
        });
        m.set_dag(42, 7);
        m.set_weight_threshold(0.5);
        m.record_cleanup(4, 2);
        m.record_tx("add", true);
        m.observe_http("POST", "/", 200, 0.01);

        let text = m.encode().unwrap();
        for name in [
            "dagdb_raft_current_term",
            "dagdb_raft_apply_lag",
            "dagdb_raft_is_leader",
            "dagdb_dag_nodes",
            "dagdb_dag_added_entries",
            "dagdb_cleanup_cycles_total",
            "dagdb_tx_total",
            "dagdb_http_requests_total",
        ] {
            assert!(text.contains(name), "нет метрики {name} в выводе:\n{text}");
        }
        // Очередь применения = 10 - 8.
        assert!(text.contains("dagdb_raft_apply_lag 2"));
    }

    #[test]
    fn independent_registries_do_not_conflict() {
        // Два экземпляра метрик не мешают друг другу (нет глобального состояния).
        let _a = Metrics::new();
        let _b = Metrics::new();
    }
}
