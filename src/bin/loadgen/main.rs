//! `loadgen` — нагрузочный генератор транзакций `dagdb`.
//!
//! Отдельный рантайм-инструмент (вне `cargo test`): поднимает in-process кластер
//! `dagdb`, гонит валидный поток подписанных транзакций через `POST /`, проверяет
//! корректность (приём валидных транзакций + консистентность реплик) и печатает
//! отчёт. Подробности — в `docs/edits/feature/load-generator/`.
//!
//! Инструмент переиспользует домен проекта (`Tx`, [`dagdb::utils::ordered_sum`],
//! `TRANSFER_TOKEN`) как единственный источник истины по правилам валидации и
//! **не меняет** публичный HTTP-контракт, формат состояния и контракт Raft.

mod cluster;
mod generator;
mod prng;
mod report;
mod runner;

use std::process::ExitCode;

use clap::Parser;

/// Параметры запуска нагрузочного генератора.
#[derive(Parser, Clone, Debug)]
#[command(
    name = "loadgen",
    author,
    version,
    about = "Нагрузочный генератор dagdb"
)]
struct LoadgenConfig {
    /// Число узлов in-process кластера.
    #[arg(long, default_value_t = 3)]
    nodes: u64,

    /// Число транзакций (взаимоисключающе с `--duration-sec`; по умолчанию 500).
    #[arg(long, conflicts_with = "duration_sec")]
    tx: Option<u64>,

    /// Длительность прогона в секундах (взаимоисключающе с `--tx`).
    #[arg(long)]
    duration_sec: Option<u64>,

    /// Целевой суммарный TPS (0 — без ограничения).
    #[arg(long, default_value_t = 500)]
    tps: u64,

    /// Число конкурентных sender-задач.
    #[arg(long, default_value_t = 8)]
    concurrency: usize,

    /// Число аккаунтов-отправителей.
    #[arg(long, default_value_t = 16)]
    accounts: usize,

    /// Число родителей на транзакцию (зажимается в 2..=min(100,|pool|)).
    #[arg(long, default_value_t = 2)]
    parents: usize,

    /// Seed детерминированного ГПСЧ (воспроизводимость параметров транзакций).
    #[arg(long, default_value_t = 1)]
    seed: u64,

    /// Путь для машинно-читаемого JSON-отчёта (для CI-артефакта).
    #[arg(long)]
    json_out: Option<std::path::PathBuf>,

    /// Сохранить временные каталоги данных узлов после прогона.
    #[arg(long, default_value_t = false)]
    keep_data: bool,

    /// Подробное логирование (debug-уровень).
    #[arg(long, default_value_t = false)]
    verbose: bool,
}

impl LoadgenConfig {
    /// Сколько транзакций сгенерировать: явный `--tx` или неограниченно
    /// (ограничение по времени) — `None`.
    fn tx_budget(&self) -> Option<u64> {
        if self.duration_sec.is_some() {
            None
        } else {
            Some(self.tx.unwrap_or(500))
        }
    }

    /// Строка подключения к Redis: из окружения `REDIS_URL` или локальный дефолт.
    fn redis_url(&self) -> String {
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/0".to_string())
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cfg = LoadgenConfig::parse();
    let level = if cfg.verbose { "debug" } else { "warn" };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    match run(cfg).await {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("loadgen: фатальная ошибка: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Оркестрация прогона: кластер → генератор → нагрузка → отчёт.
///
/// Возвращает `Ok(ok)`, где `ok` — выполнены ли корректностные инварианты.
async fn run(cfg: LoadgenConfig) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let redis_url = cfg.redis_url();
    let cluster = cluster::Cluster::start(cfg.nodes, &redis_url, cfg.keep_data).await?;

    // Узлы живут на собственных потоках/current-thread runtime, поэтому драйвер
    // нагрузки (async-задачи этого runtime) не конкурирует с их HTTP-серверами.
    let gen_cfg = generator::GeneratorConfig {
        seed: cfg.seed,
        accounts: cfg.accounts,
        parents: cfg.parents,
    };
    let run_cfg = runner::RunConfig {
        tps: cfg.tps,
        concurrency: cfg.concurrency,
        tx_budget: cfg.tx_budget(),
        duration_sec: cfg.duration_sec,
    };
    let nodes = cluster.nodes_for_load();
    let load = runner::run_load(&nodes, &gen_cfg, &run_cfg, cfg.verbose).await?;

    // Итоговые размеры DAG по каждой реплике — для проверки консистентности.
    let dag_totals = cluster.dag_totals().await;

    let report = report::build_report(&cfg, &load, &dag_totals, &redis_url);
    report::print_report(&report);
    if let Some(path) = &cfg.json_out {
        report::write_json(&report, path)?;
        println!("JSON-отчёт записан: {}", path.display());
    }

    cluster.shutdown().await;
    Ok(report.ok)
}
