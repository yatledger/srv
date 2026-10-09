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
use std::time::Duration;

use clap::Parser;

use runner::{RunConfig, Sleep};

/// Нижняя граница случайной паузы демо-режима (секунды).
const DEMO_SLEEP_MIN: f64 = 0.1;
/// Верхняя граница случайной паузы демо-режима (секунды).
const DEMO_SLEEP_MAX: f64 = 2.5;
/// Длительность демо-прогона без параметров (секунды).
const DEMO_DURATION_SEC: u64 = 60;

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

    /// Наглядный пофреймовый вывод запроса и ответа (в демо-режиме включён).
    #[arg(long, default_value_t = false)]
    show_tx: bool,

    /// Фиксированная пауза между запросами в секундах (`0` — без паузы).
    ///
    /// В демо-режиме (без аргументов) пауза случайна (`0.1..2.5` с).
    #[arg(long, value_name = "SECS", value_parser = parse_sleep_secs)]
    sleep: Option<f64>,
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

    /// Демо-режим: процесс запущен **без аргументов** (`argv.len() == 1`).
    fn is_demo(&self) -> bool {
        std::env::args_os().len() == 1
    }

    /// Параметры прогона с учётом демо-режима (см. [`resolve`](Self::resolve)).
    fn run_config(&self) -> RunConfig {
        self.resolve(self.is_demo())
    }

    /// Резолвит фактические параметры прогона.
    ///
    /// В демо-режиме (`demo = true`, запуск без аргументов) включаются наглядный
    /// вывод, случайная пауза `0.1..2.5` с, TPS выключается, конкурентность
    /// сбрасывается в `1`, а бюджет задаётся длительностью [`DEMO_DURATION_SEC`].
    /// Иначе действуют явные флаги и обычные дефолты. Вынесено в чистую функцию
    /// от `demo`, чтобы юнит-тесты не зависели от аргументов процесса.
    pub(crate) fn resolve(&self, demo: bool) -> RunConfig {
        if demo {
            return RunConfig {
                tps: 0,
                concurrency: 1,
                tx_budget: None,
                duration_sec: Some(DEMO_DURATION_SEC),
                show_tx: true,
                sleep: Sleep::Jitter {
                    min: DEMO_SLEEP_MIN,
                    max: DEMO_SLEEP_MAX,
                },
            };
        }
        let sleep = match self.sleep {
            Some(secs) if secs > 0.0 => Sleep::Fixed(Duration::from_secs_f64(secs)),
            _ => Sleep::None,
        };
        RunConfig {
            tps: self.tps,
            concurrency: self.concurrency,
            tx_budget: self.tx_budget(),
            duration_sec: self.duration_sec,
            show_tx: self.show_tx,
            sleep,
        }
    }
}

/// Парсер `--sleep <SECS>`: неотрицательное конечное число секунд.
fn parse_sleep_secs(raw: &str) -> Result<f64, String> {
    let secs: f64 = raw
        .parse()
        .map_err(|_| format!("ожидается число секунд, получено «{raw}»"))?;
    if !secs.is_finite() || secs < 0.0 {
        return Err(format!(
            "пауза должна быть неотрицательным числом секунд, получено «{raw}»"
        ));
    }
    Ok(secs)
}

/// Фильтр логирования по умолчанию.
///
/// По умолчанию — `error`: в нагрузочном/демо-прогоне кластер сам себя инициализирует,
/// и `openraft` сыпет диагностическими `WARN` при смене membership во время bootstrap
/// (`membership_log_id changed: …`). Это не дефект, а внутренний шум, который портит
/// наглядный вывод. `--verbose` возвращает `debug`, а `RUST_LOG` (если задан) имеет
/// приоритет над этим значением.
fn default_log_filter(verbose: bool) -> &'static str {
    if verbose { "debug" } else { "error" }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cfg = LoadgenConfig::parse();
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_log_filter(cfg.verbose)));
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
    let run_cfg = cfg.run_config();
    let nodes = cluster.nodes_for_load();
    let load = runner::run_load(&nodes, &gen_cfg, &run_cfg, cfg.verbose).await?;

    // Итоговые размеры DAG по каждой реплике — для проверки консистентности.
    let dag_totals = cluster.dag_totals().await;

    let report = report::build_report(&cfg, &load, &dag_totals, &redis_url);
    report::print_report(&report, !cfg.is_demo());
    if let Some(path) = &cfg.json_out {
        report::write_json(&report, path)?;
        println!("JSON-отчёт записан: {}", path.display());
    }

    cluster.shutdown().await;
    Ok(report.ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Конфиг с обычными дефолтами (`--show-tx`/`--sleep` не заданы).
    fn base() -> LoadgenConfig {
        LoadgenConfig {
            nodes: 3,
            tx: None,
            duration_sec: None,
            tps: 500,
            concurrency: 8,
            accounts: 16,
            parents: 2,
            seed: 1,
            json_out: None,
            keep_data: false,
            verbose: false,
            show_tx: false,
            sleep: None,
        }
    }

    #[test]
    fn demo_resolves_to_live_slow_single_worker() {
        let rc = base().resolve(true);
        assert!(rc.show_tx, "демо включает наглядный вывод");
        assert_eq!(rc.tps, 0, "в демо TPS выключен");
        assert_eq!(rc.concurrency, 1, "демо — один воркер");
        assert_eq!(rc.tx_budget, None);
        assert_eq!(rc.duration_sec, Some(DEMO_DURATION_SEC));
        match rc.sleep {
            Sleep::Jitter { min, max } => {
                assert_eq!((min, max), (DEMO_SLEEP_MIN, DEMO_SLEEP_MAX));
            }
            other => panic!("ожидался Jitter, получено {other:?}"),
        }
    }

    #[test]
    fn explicit_args_disable_demo_defaults() {
        let mut cfg = base();
        cfg.show_tx = true;
        let rc = cfg.resolve(false);
        assert!(rc.show_tx);
        assert_eq!(rc.tps, 500);
        assert_eq!(rc.concurrency, 8);
        assert_eq!(rc.tx_budget, Some(500));
        assert_eq!(rc.duration_sec, None);
        assert!(matches!(rc.sleep, Sleep::None));
    }

    #[test]
    fn sleep_zero_disables_pause_and_positive_is_fixed() {
        let mut cfg = base();
        cfg.sleep = Some(0.0);
        assert!(matches!(cfg.resolve(false).sleep, Sleep::None));

        cfg.sleep = Some(1.5);
        match cfg.resolve(false).sleep {
            Sleep::Fixed(d) => {
                assert!((d.as_secs_f64() - 1.5).abs() < 1e-9);
            }
            other => panic!("ожидался Fixed, получено {other:?}"),
        }
    }

    #[test]
    fn sleep_parser_rejects_negative_and_nan() {
        assert_eq!(parse_sleep_secs("0").unwrap(), 0.0);
        assert_eq!(parse_sleep_secs("2.5").unwrap(), 2.5);
        assert!(parse_sleep_secs("-1").is_err());
        assert!(parse_sleep_secs("NaN").is_err());
        assert!(parse_sleep_secs("inf").is_err());
        assert!(parse_sleep_secs("abc").is_err());
    }

    #[test]
    fn default_log_filter_quiet_unless_verbose() {
        assert_eq!(default_log_filter(false), "error");
        assert_eq!(default_log_filter(true), "debug");
    }
}
