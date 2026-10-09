//! Отчёт о прогоне: корректностные инварианты, throughput и перцентили задержки.
//!
//! Корректностные инварианты (влияют на код возврата): нет прикладных отказов у
//! заведомо валидных транзакций и одинаковый размер DAG на всех репликах.
//! Throughput/перцентили — **информационные** (без SLA).

use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::path::Path;

use serde::Serialize;

use crate::LoadgenConfig;
use crate::generator::SignedTx;
use crate::runner::{ClientOutcome, LoadResult};

/// Агрегация отказов по причине.
#[derive(Serialize, Debug, Clone)]
pub struct Rejection {
    /// Текст причины.
    pub reason: String,
    /// Сколько раз встретилась.
    pub count: u64,
}

/// Перцентили задержки клиента (мс).
#[derive(Serialize, Debug, Clone)]
pub struct Latency {
    /// 50-й перцентиль.
    pub p50: f64,
    /// 95-й перцентиль.
    pub p95: f64,
    /// 99-й перцентиль.
    pub p99: f64,
    /// Среднее.
    pub mean: f64,
    /// Максимум.
    pub max: f64,
}

/// Параметры прогона (для воспроизводимости).
#[derive(Serialize, Debug, Clone)]
pub struct ReportParams {
    /// Число узлов.
    pub nodes: u64,
    /// Бюджет транзакций (если задан).
    pub tx: Option<u64>,
    /// Длительность (если задана).
    pub duration_sec: Option<u64>,
    /// Целевой TPS.
    pub tps: u64,
    /// Конкурентность.
    pub concurrency: usize,
    /// Число аккаунтов.
    pub accounts: usize,
    /// Число родителей.
    pub parents: usize,
    /// Seed.
    pub seed: u64,
}

/// Машинно-читаемый отчёт (схема фиксирована в `plan.md` §4 шаг 9).
#[derive(Serialize, Debug, Clone)]
pub struct Report {
    /// Параметры прогона.
    pub params: ReportParams,
    /// Принято транзакций (HTTP 200).
    pub accepted: u64,
    /// Всего отправлено запросов.
    pub attempted: u64,
    /// Отказы с разбивкой по причинам.
    pub rejected: Vec<Rejection>,
    /// Достигнутый TPS (информационно).
    pub achieved_tps: f64,
    /// Перцентили задержки клиента (информационно).
    pub latency_ms: Latency,
    /// Итоговый размер DAG по каждой реплике.
    pub dag_total_per_node: BTreeMap<u64, u64>,
    /// Выполнены ли корректностные инварианты.
    pub ok: bool,
    /// Обнаруженные нарушения корректности.
    pub errors: Vec<String>,
}

/// Считает отчёт и корректностные инварианты.
pub fn build_report(
    cfg: &LoadgenConfig,
    load: &LoadResult,
    dag_totals: &[(u64, u64)],
    _redis_url: &str,
) -> Report {
    let attempted = load.outcomes.len() as u64;

    let mut accepted = 0u64;
    let mut transport_failures = 0u64;
    let mut reasons: BTreeMap<String, u64> = BTreeMap::new();
    let mut latencies: Vec<f64> = Vec::with_capacity(load.outcomes.len());
    for outcome in &load.outcomes {
        latencies.push(outcome.latency_ms);
        if outcome.status == 200 {
            accepted += 1;
        } else {
            if outcome.transport_error {
                transport_failures += 1;
            }
            let reason = outcome
                .reason
                .clone()
                .unwrap_or_else(|| format!("http {}", outcome.status));
            *reasons.entry(reason).or_insert(0) += 1;
        }
    }

    let mut errors = Vec::new();
    if accepted != attempted {
        errors.push(format!(
            "есть отказы у заведомо валидных транзакций: принято {accepted} из {attempted}"
        ));
    }
    if transport_failures > 0 {
        errors.push(format!("транспортных ошибок: {transport_failures}"));
    }

    let dag_total_per_node: BTreeMap<u64, u64> = dag_totals.iter().copied().collect();
    let mut totals = dag_total_per_node.values().copied();
    if let Some(first) = totals.next()
        && totals.any(|t| t != first)
    {
        errors.push(format!(
            "размер DAG различается по репликам: {dag_total_per_node:?}"
        ));
    }

    let rejected: Vec<Rejection> = reasons
        .into_iter()
        .map(|(reason, count)| Rejection { reason, count })
        .collect();

    let achieved_tps = if load.wall_secs > 0.0 {
        attempted as f64 / load.wall_secs
    } else {
        0.0
    };

    Report {
        params: ReportParams {
            nodes: cfg.nodes,
            tx: cfg.tx_budget(),
            duration_sec: cfg.duration_sec,
            tps: cfg.tps,
            concurrency: cfg.concurrency,
            accounts: cfg.accounts,
            parents: cfg.parents,
            seed: cfg.seed,
        },
        accepted,
        attempted,
        rejected,
        achieved_tps,
        latency_ms: latency_stats(&mut latencies),
        dag_total_per_node,
        ok: errors.is_empty(),
        errors,
    }
}

/// Считает p50/p95/p99, среднее и максимум по выборке задержек.
fn latency_stats(latencies: &mut [f64]) -> Latency {
    if latencies.is_empty() {
        return Latency {
            p50: 0.0,
            p95: 0.0,
            p99: 0.0,
            mean: 0.0,
            max: 0.0,
        };
    }
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = latencies.len();
    let percentile = |p: f64| -> f64 {
        let rank = (p / 100.0 * (n - 1) as f64).round() as usize;
        latencies[rank.min(n - 1)]
    };
    let mean = latencies.iter().sum::<f64>() / n as f64;
    Latency {
        p50: percentile(50.0),
        p95: percentile(95.0),
        p99: percentile(99.0),
        mean,
        max: *latencies.last().unwrap_or(&0.0),
    }
}

/// Печатает человекочитаемый отчёт в stdout.
///
/// `show_params` управляет строкой «параметры»: в демо-режиме (запуск без
/// аргументов) она пропускается — фактические параметры там задаются
/// внутренними демо-дефолтами, а не явными флагами.
pub fn print_report(report: &Report, show_params: bool) {
    println!("=== loadgen: отчёт ===");
    if show_params {
        println!(
            "параметры: узлов={}, tx={:?}, duration_sec={:?}, tps={}, concurrency={}, accounts={}, parents={}, seed={}",
            report.params.nodes,
            report.params.tx,
            report.params.duration_sec,
            report.params.tps,
            report.params.concurrency,
            report.params.accounts,
            report.params.parents,
            report.params.seed,
        );
    }
    println!(
        "принято: {}/{} (достигнуто {:.1} TPS)",
        report.accepted, report.attempted, report.achieved_tps
    );
    if !report.rejected.is_empty() {
        println!("отказы:");
        for r in &report.rejected {
            println!("  - {} x{}", r.reason, r.count);
        }
    }
    println!(
        "задержка мс: p50={:.2} p95={:.2} p99={:.2} mean={:.2} max={:.2}",
        report.latency_ms.p50,
        report.latency_ms.p95,
        report.latency_ms.p99,
        report.latency_ms.mean,
        report.latency_ms.max,
    );
    println!("размер DAG по репликам:");
    for (id, total) in &report.dag_total_per_node {
        println!("  - узел {id}: {total}");
    }
    if report.ok {
        println!("ИТОГ: OK — корректностные инварианты выполнены");
    } else {
        println!("ИТОГ: ОШИБКА — корректностные инварианты нарушены:");
        for e in &report.errors {
            println!("  ! {e}");
        }
    }
}

/// Записывает JSON-отчёт в файл.
pub fn write_json(
    report: &Report,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let json = serde_json::to_string_pretty(report)?;
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, json)?;
    Ok(())
}

/// Палитра ANSI-цветов для наглядного пофреймового вывода.
///
/// Без новых зависимостей: последовательности зашиты строками. `enabled = false`
/// даёт чистый текст без ESC-последовательностей (не-TTY, `NO_COLOR`, JSON-прогон).
#[derive(Clone, Debug)]
pub struct Palette {
    enabled: bool,
}

impl Palette {
    /// Авто-детект: цвета только на TTY и без `NO_COLOR`.
    pub fn detect() -> Self {
        let enabled = std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none();
        Self { enabled }
    }

    /// Палитра с явным включением/выключением (для юнит-тестов).
    #[cfg(test)]
    pub fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    /// Оборачивает текст кодом `code` (например, `"32"`), если цвета включены.
    fn wrap(&self, code: &str, text: &str) -> String {
        if self.enabled {
            format!("\x1b[{code}m{text}\x1b[0m")
        } else {
            text.to_string()
        }
    }

    /// Приглушённый цвет (рамка, шапка).
    fn dim(&self, text: &str) -> String {
        self.wrap("2", text)
    }

    /// Зелёный (успех).
    fn green(&self, text: &str) -> String {
        self.wrap("32", text)
    }

    /// Красный (отказ).
    fn red(&self, text: &str) -> String {
        self.wrap("31", text)
    }
}

/// Сокращает длинную строку до `head…tail` (UTF-8-безопасно, по байтам ASCII-хэшей).
fn shorten(s: &str, head: usize, tail: usize) -> String {
    let len = s.chars().count();
    if len <= head + tail + 1 {
        return s.to_string();
    }
    let head_str: String = s.chars().take(head).collect();
    let tail_str: String = s
        .chars()
        .rev()
        .take(tail)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{head_str}…{tail_str}")
}

/// Строит строку-шапку блока: номер, воркер, `seq`, адрес-отправитель.
pub fn format_request_header(n: u64, worker: usize, signed: &SignedTx) -> String {
    format!(
        "#{n} · worker {worker} · seq {} · addr {}",
        signed.tx.sequence(),
        shorten(signed.tx.address().as_str(), 4, 4),
    )
}

/// Строит строки запроса: цель, `func`, `prnts`, `var`, `sign`.
pub fn format_request_lines(signed: &SignedTx, leader: &str) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!("→ POST {leader}   func={}", signed.func));
    let prnts = signed.tx.parents();
    let short: Vec<String> = prnts.iter().map(|p| shorten(p.as_str(), 3, 3)).collect();
    lines.push(format!("  prnts[{}] {}", prnts.len(), short.join(", ")));

    let var = signed.tx.var();
    let to = var["to"]
        .as_str()
        .map(|s| shorten(s, 4, 4))
        .unwrap_or_default();
    let val = var["val"].as_u64().unwrap_or(0);
    let msg = var["msg"].as_str().unwrap_or("");
    let msg_short = shorten(msg, 12, 4);
    lines.push(format!(
        "  var {{to: {to}, val: {val}, msg: \"{msg_short}\"}}  sign: {}",
        shorten(&signed.sign, 3, 3),
    ));
    lines
}

/// Строит строку ответа: статус (+ reason) и задержка клиента в мс.
pub fn format_response_line(outcome: &ClientOutcome) -> String {
    let latency = format!("{:.1} ms", outcome.latency_ms);
    if outcome.transport_error {
        let reason = outcome.reason.as_deref().unwrap_or("транспортная ошибка");
        return format!("← транспортная ошибка · {latency} · {reason}");
    }
    let phrase = reason_phrase(outcome.status);
    let status = if phrase.is_empty() {
        format!("{}", outcome.status)
    } else {
        format!("{} {phrase}", outcome.status)
    };
    match outcome.reason.as_deref() {
        Some(reason) => format!("← {status} · {latency} · {reason}"),
        None => format!("← {status} · {latency}"),
    }
}

/// Reason-фраза HTTP-статуса (краткий набор; пусто для неизвестных).
fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "",
    }
}

/// Рендерит целый блок «шапка → запрос → ответ» в виде строки (с рамкой и цветами).
pub fn render_frame(
    n: u64,
    worker: usize,
    signed: &SignedTx,
    leader: &str,
    outcome: &ClientOutcome,
    palette: &Palette,
) -> String {
    const WIDTH: usize = 70;
    let header = format_request_header(n, worker, signed);
    let top = format!(
        "┌─ {header} {}",
        "─".repeat(WIDTH.saturating_sub(header.chars().count() + 4))
    );
    let separator_line = "─".repeat(WIDTH);
    let success = outcome.status == 200 && !outcome.transport_error;

    let mut out = String::new();
    out.push_str(&palette.dim(&top));
    out.push('\n');
    for line in format_request_lines(signed, leader) {
        out.push_str(&palette.dim(&format!("│ {line}")));
        out.push('\n');
    }
    let response = format_response_line(outcome);
    let response_colored = if success {
        palette.green(&format!("│ {response}"))
    } else {
        palette.red(&format!("│ {response}"))
    };
    out.push_str(&response_colored);
    out.push('\n');
    out.push_str(&palette.dim(&format!("└{separator_line}")));
    out.push('\n');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::ClientOutcome;

    fn outcome(status: u16, ms: f64) -> ClientOutcome {
        ClientOutcome {
            status,
            reason: if status == 200 {
                None
            } else {
                Some("boom".to_string())
            },
            latency_ms: ms,
            transport_error: false,
        }
    }

    fn cfg() -> LoadgenConfig {
        LoadgenConfig {
            nodes: 3,
            tx: Some(10),
            duration_sec: None,
            tps: 100,
            concurrency: 2,
            accounts: 4,
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
    fn latency_percentiles_are_sane() {
        let mut v: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        let stats = latency_stats(&mut v);
        assert_eq!(stats.max, 100.0);
        assert!(stats.p50 >= 49.0 && stats.p50 <= 51.0);
        assert!(stats.p95 >= 94.0 && stats.p95 <= 96.0);
        assert!(stats.p99 >= 98.0 && stats.p99 <= 100.0);
    }

    #[test]
    fn report_ok_when_all_accepted_and_replicas_equal() {
        let load = LoadResult {
            outcomes: vec![outcome(200, 1.0), outcome(200, 2.0)],
            wall_secs: 1.0,
        };
        let report = build_report(&cfg(), &load, &[(1, 5), (2, 5), (3, 5)], "redis://x");
        assert!(report.ok, "errors: {:?}", report.errors);
        assert_eq!(report.accepted, 2);
    }

    #[test]
    fn report_fails_on_application_rejection() {
        let load = LoadResult {
            outcomes: vec![outcome(200, 1.0), outcome(400, 1.0)],
            wall_secs: 1.0,
        };
        let report = build_report(&cfg(), &load, &[(1, 5), (2, 5)], "redis://x");
        assert!(!report.ok);
        assert_eq!(report.rejected.len(), 1);
        assert_eq!(report.rejected[0].reason, "boom");
    }

    #[test]
    fn report_fails_on_replica_divergence() {
        let load = LoadResult {
            outcomes: vec![outcome(200, 1.0)],
            wall_secs: 1.0,
        };
        let report = build_report(&cfg(), &load, &[(1, 5), (2, 6)], "redis://x");
        assert!(!report.ok);
        assert!(report.errors.iter().any(|e| e.contains("различается")));
    }

    fn signed() -> SignedTx {
        use dagdb::Tx;
        use dagdb::domain::{Address, Hash};
        SignedTx {
            tx: Tx::new(
                vec![
                    Hash::from("9c1aaaaaaaaaaaaaaaaaaaaaaaaaaaaa2b"),
                    Hash::from("4debbbbbbbbbbbbbbbbbbbbbbbbbbb0f7"),
                ],
                Address::from("7Qkzzzzzzzzzzzzzzzzzzzzzzzzzzz3fA"),
                12,
                serde_json::json!({"to": "Bm8zzzzzzzzzzzzzzzzzzzzzzzzzzqL2", "val": 48215, "msg": "hello"}),
            ),
            sign: "3abccccccccccccccccccccccccccccccc9d".to_string(),
            func: "transferToken".to_string(),
        }
    }

    #[test]
    fn render_frame_contains_key_fields_without_color_when_disabled() {
        let palette = Palette::new(false);
        let block = render_frame(
            12,
            0,
            &signed(),
            "http://127.0.0.1:21001",
            &outcome(200, 14.7),
            &palette,
        );
        assert!(block.contains("#12"));
        assert!(block.contains("worker 0"));
        assert!(block.contains("seq 12"));
        assert!(block.contains("func=transferToken"));
        assert!(block.contains("200 OK"));
        assert!(block.contains("14.7 ms"));
        assert!(
            !block.contains('\x1b'),
            "без цвета ESC-последовательности запрещены"
        );
    }

    #[test]
    fn render_frame_colors_when_enabled() {
        let palette = Palette::new(true);
        let ok = render_frame(1, 0, &signed(), "http://x", &outcome(200, 1.0), &palette);
        assert!(ok.contains("\x1b[32m"), "успех должен быть зелёным");
        let fail = render_frame(1, 0, &signed(), "http://x", &outcome(400, 1.0), &palette);
        assert!(fail.contains("\x1b[31m"), "отказ должен быть красным");
    }

    #[test]
    fn response_line_shows_reason_for_rejection() {
        let line = format_response_line(&outcome(400, 3.2));
        assert!(line.contains("400 Bad Request"));
        assert!(line.contains("boom"));
        // Успех — без reason.
        assert!(!format_response_line(&outcome(200, 3.2)).contains("boom"));
    }

    #[test]
    fn shorten_keeps_bounds() {
        assert_eq!(shorten("abcdefghij", 3, 3), "abc…hij");
        assert_eq!(shorten("abc", 4, 4), "abc");
    }
}
