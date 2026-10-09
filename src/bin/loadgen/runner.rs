//! Драйвер нагрузки: конкурентные sender-задачи, статусы и задержки.
//!
//! Нагрузка отправляется **лидеру напрямую**: follower может ещё не применить
//! свежие родители, и его ранняя проверка состояния (`tx_logic::validate_against_state`)
//! ложно отклонила бы валидную транзакцию. Запись через лидера реплицируется на
//! все узлы, поэтому консистентность реплик всё равно проверяется.
//!
//! Аккаунты делятся на непрерывные блоки, по блоку на sender-задачу, поэтому
//! `seq` каждого аккаунта монотонен без межзадачной синхронизации, а запросы в
//! пределах аккаунта уходят в порядке выпуска.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::Value;

use dagdb::domain::Hash;

use crate::cluster::TOKEN;
use crate::generator::{Generator, GeneratorConfig, SignedTx, choose_parents};
use crate::prng::Prng;
use crate::report::{Palette, render_frame};

/// Модель паузы между запросами (ортогональна целевому TPS).
#[derive(Clone, Debug, PartialEq)]
pub enum Sleep {
    /// Пауза отсутствует (максимальная скорость) — `--sleep 0` / без флага.
    None,
    /// Фиксированная пауза — `--sleep N>0`.
    Fixed(Duration),
    /// Случайная пауза `[min, max]` — демо-режим (без параметров).
    Jitter {
        /// Нижняя граница, секунды.
        min: f64,
        /// Верхняя граница, секунды.
        max: f64,
    },
}

/// Параметры прогона.
#[derive(Clone, Debug)]
pub struct RunConfig {
    /// Целевой суммарный TPS (0 — без ограничения).
    pub tps: u64,
    /// Число конкурентных sender-задач.
    pub concurrency: usize,
    /// Бюджет транзакций (`None` — без ограничения).
    pub tx_budget: Option<u64>,
    /// Длительность прогона в секундах (`None` — по бюджету).
    pub duration_sec: Option<u64>,
    /// Печатать наглядный пофреймовый вывод запроса/ответа.
    pub show_tx: bool,
    /// Пауза между запросами.
    pub sleep: Sleep,
}

/// Общий мьютекс вывода: один блок печатается атомарно при любой конкурентности.
static OUTPUT_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Результат одного клиентского запроса.
#[derive(Clone, Debug)]
pub struct ClientOutcome {
    /// HTTP-статус ответа (0 — транспортная ошибка).
    pub status: u16,
    /// Прикладная причина отказа (тело ошибки при 4xx/5xx).
    pub reason: Option<String>,
    /// Задержка клиента, миллисекунды.
    pub latency_ms: f64,
    /// Транспортная ошибка (нет ответа).
    pub transport_error: bool,
}

/// Итог прогона нагрузки.
pub struct LoadResult {
    /// Исходы всех запросов.
    pub outcomes: Vec<ClientOutcome>,
    /// Фактическая длительность прогона, секунды.
    pub wall_secs: f64,
}

/// Запускает нагрузку: `--concurrency` sender-задач, общий бюджет и таймер.
pub async fn run_load(
    nodes: &[(u64, String)],
    gen_cfg: &GeneratorConfig,
    run_cfg: &RunConfig,
    verbose: bool,
) -> Result<LoadResult, Box<dyn std::error::Error + Send + Sync>> {
    if nodes.is_empty() {
        return Err("нет узлов для нагрузки".into());
    }
    let leader = resolve_leader(nodes).await?;
    let generator = Arc::new(Generator::new(gen_cfg.clone()));

    let workers = run_cfg.concurrency.max(1).min(generator.account_count());
    let budget = run_cfg.tx_budget;
    let issued = Arc::new(AtomicU64::new(0));
    let frame = Arc::new(AtomicU64::new(0));
    let palette = Palette::detect();
    let deadline = run_cfg
        .duration_sec
        .map(|secs| Instant::now() + Duration::from_secs(secs));

    let started = Instant::now();
    let mut handles = Vec::with_capacity(workers);
    for worker_id in 0..workers {
        let generator = Arc::clone(&generator);
        let issued = Arc::clone(&issued);
        let frame = Arc::clone(&frame);
        let leader = leader.clone();
        let run_cfg = run_cfg.clone();
        let palette = palette.clone();
        handles.push(tokio::spawn(async move {
            worker_loop(
                worker_id, workers, &generator, &leader, &run_cfg, budget, deadline, &issued,
                &frame, &palette, verbose,
            )
            .await
        }));
    }

    let mut outcomes = Vec::new();
    for handle in handles {
        let mut worker_outcomes = handle.await?;
        outcomes.append(&mut worker_outcomes);
    }
    let wall_secs = started.elapsed().as_secs_f64();
    Ok(LoadResult {
        outcomes,
        wall_secs,
    })
}

/// Тело sender-задачи.
#[allow(clippy::too_many_arguments)]
async fn worker_loop(
    worker_id: usize,
    workers: usize,
    generator: &Generator,
    leader: &str,
    run_cfg: &RunConfig,
    budget: Option<u64>,
    deadline: Option<Instant>,
    issued: &AtomicU64,
    frame: &AtomicU64,
    palette: &Palette,
    verbose: bool,
) -> Vec<ClientOutcome> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("sender reqwest client");
    let mut rng = Prng::new(run_cfg_seed(worker_id));
    // Пауза разыгрывается **отдельным** ГПСЧ, чтобы не сдвигать поток параметров
    // транзакций (детерминизм при одном `--seed`).
    let mut sleep_rng = Prng::new(sleep_seed(worker_id));

    // Аккаунты потока — непрерывный блок без пересечения с другими задачами.
    let (account_start, account_len) = account_block(worker_id, workers, generator.account_count());
    let mut account_step = 0usize;

    // Родителей берём с лидера: его DAG — актуальный источник живых узлов.
    let mut pool = fetch_pool(&client, leader).await;
    let mut outcomes = Vec::new();

    // Пейсинг под целевой TPS: на задачу приходится tps / workers.
    let per_worker_tps = if run_cfg.tps == 0 {
        0.0
    } else {
        (run_cfg.tps as f64 / workers as f64).max(1.0)
    };
    let interval = if per_worker_tps > 0.0 {
        Some(Duration::from_secs_f64(1.0 / per_worker_tps))
    } else {
        None
    };
    let start = Instant::now();
    let mut next_slot = start;
    let mut since_pool_refresh = 0usize;

    loop {
        if let Some(limit) = budget
            && issued.fetch_add(1, Ordering::Relaxed) >= limit
        {
            break;
        }
        if let Some(deadline) = deadline
            && Instant::now() >= deadline
        {
            break;
        }

        // Пейсинг по целевому TPS.
        if let Some(interval) = interval {
            let now = Instant::now();
            if next_slot > now {
                tokio::time::sleep(next_slot - now).await;
            }
            next_slot += interval;
        }

        // Пауза `--sleep` (ортогональна TPS; сюда же попадает демо-джиттер).
        sleep_between(&run_cfg.sleep, &mut sleep_rng).await;

        // Обновляем пул живых узлов, чтобы нагрузка шла и на новые транзакции.
        since_pool_refresh += 1;
        if since_pool_refresh >= 128 || pool.len() < 2 {
            let fresh = fetch_pool(&client, leader).await;
            if !fresh.is_empty() {
                pool = fresh;
            }
            since_pool_refresh = 0;
        }

        let Some(k) = generator.effective_parent_count(pool.len()) else {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        };

        let index = account_start + (account_step % account_len);
        account_step += 1;
        let parents = choose_parents(&pool, k, &mut rng);
        // «Шлюз» аккаунта: блоки аккаунтов и так не пересекаются, но лок делает
        // контракт устойчивым и сериализует выдачу `seq`.
        let signed = {
            let _gate = generator.account_gate(index);
            generator.build(index, &parents, &mut rng)
        };
        let outcome = send_with_retry(&client, leader, &signed, verbose).await;
        if run_cfg.show_tx {
            let n = frame.fetch_add(1, Ordering::Relaxed) + 1;
            // Блок печатается под общим мьютексом — при `concurrency > 1` строки
            // разных транзакций не перемешиваются (R6).
            let block = render_frame(n, worker_id, &signed, leader, &outcome, palette);
            let _guard = OUTPUT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            print!("{block}");
        }
        outcomes.push(outcome);
    }

    outcomes
}

/// Выдерживает паузу `sleep` (фиксированную, случайную или отсутствующую).
async fn sleep_between(sleep: &Sleep, rng: &mut Prng) {
    match sleep {
        Sleep::None => {}
        Sleep::Fixed(d) => tokio::time::sleep(*d).await,
        Sleep::Jitter { min, max } => {
            let secs = rng.range_f64(*min, *max);
            tokio::time::sleep(Duration::from_secs_f64(secs)).await;
        }
    }
}

/// Непрерывный блок аккаунтов `[start, start+len)` для задачи `worker_id`.
///
/// Блоки разных задач не пересекаются, поэтому `seq` каждого аккаунта монотонен
/// без межзадачной синхронизации. Задачи сверх числа аккаунтов получают пустой
/// блок (`len == 0`); драйвер их не создаёт (`workers <= accounts`).
fn account_block(worker_id: usize, workers: usize, accounts: usize) -> (usize, usize) {
    let effective = workers.min(accounts).max(1);
    if worker_id >= effective {
        return (0, 0);
    }
    let base = accounts / effective;
    let extra = accounts % effective;
    let start = worker_id * base + worker_id.min(extra);
    let len = base + usize::from(worker_id < extra);
    (start, len)
}

/// Seed ГПСЧ выбора родителей для задачи.
fn run_cfg_seed(worker_id: usize) -> u64 {
    (worker_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03
}

/// Seed **отдельного** ГПСЧ паузы (не влияет на поток параметров транзакций).
fn sleep_seed(worker_id: usize) -> u64 {
    (worker_id as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0xA5A5_5A5A_1234_5678)
}

/// Отправляет транзакцию лидеру с ретраями на временные сбои (503/сеть).
async fn send_with_retry(
    client: &reqwest::Client,
    leader: &str,
    signed: &SignedTx,
    verbose: bool,
) -> ClientOutcome {
    let body = serde_json::json!({
        "tx": signed.tx,
        "sign": signed.sign,
        "func": signed.func,
    });

    let mut attempt = 0u32;
    loop {
        let started = Instant::now();
        match client.post(leader).json(&body).send().await {
            Ok(resp) => {
                let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
                let status = resp.status().as_u16();
                // 503 (нет лидера) и 5xx — временные: короткий ретрай.
                if (status == 503 || status >= 500) && attempt < 50 {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                let reason = if status == 200 {
                    None
                } else {
                    let text = resp.text().await.unwrap_or_default();
                    if verbose {
                        eprintln!("loadgen: отклонено ({status}): {text}");
                    }
                    Some(parse_reason(&text, status))
                };
                return ClientOutcome {
                    status,
                    reason,
                    latency_ms,
                    transport_error: false,
                };
            }
            Err(e) => {
                let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
                if attempt < 50 {
                    attempt += 1;
                    if verbose {
                        eprintln!("loadgen: транспортная ошибка ({e}), повтор");
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                return ClientOutcome {
                    status: 0,
                    reason: Some(format!("transport error: {e}")),
                    latency_ms,
                    transport_error: true,
                };
            }
        }
    }
}

/// Извлекает человекочитаемую причину из тела ошибки `ApiResponse`.
fn parse_reason(body: &str, status: u16) -> String {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v["message"].as_str().map(str::to_string))
        .unwrap_or_else(|| format!("http {status}"))
}

/// Определяет URL лидера, опрашивая `/mng/metrics` всех узлов.
async fn resolve_leader(
    nodes: &[(u64, String)],
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    for _ in 0..400 {
        for (id, url) in nodes {
            if let Ok(resp) = client
                .post(format!("{url}/mng/metrics"))
                .header("x-internal-token", TOKEN)
                .send()
                .await
                && let Ok(v) = resp.json::<Value>().await
                && v["current_leader"].as_u64() == Some(*id)
            {
                return Ok(url.clone());
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err("не удалось определить лидера кластера".into())
}

/// Запрашивает живой пул узлов (`/pool`) у заданного узла.
async fn fetch_pool(client: &reqwest::Client, base: &str) -> Vec<Hash> {
    match client.get(format!("{base}/pool?limit=1000")).send().await {
        Ok(resp) => resp
            .json::<Value>()
            .await
            .ok()
            .and_then(|v| {
                v["nodes"].as_array().map(|arr| {
                    arr.iter()
                        .filter_map(|n| n.as_str())
                        .map(Hash::from)
                        .collect()
                })
            })
            .unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_reason_extracts_message() {
        let body = r#"{"status":"error","message":"parent x does not exist"}"#;
        assert_eq!(parse_reason(body, 400), "parent x does not exist");
        assert_eq!(parse_reason("not json", 400), "http 400");
    }

    #[test]
    fn account_blocks_cover_all_accounts_without_overlap() {
        for workers in 1..=8 {
            for accounts in 1..=17 {
                let effective = workers.min(accounts).max(1);
                let mut seen = vec![0usize; accounts];
                let mut total = 0;
                for worker_id in 0..workers {
                    let (start, len) = account_block(worker_id, workers, accounts);
                    assert!(start + len <= accounts, "блок выходит за пределы аккаунтов");
                    if worker_id >= effective {
                        assert_eq!(len, 0, "лишняя задача должна получить пустой блок");
                        continue;
                    }
                    assert!(len > 0, "рабочая задача должна получить непустой блок");
                    for slot in seen.iter_mut().skip(start).take(len) {
                        *slot += 1;
                        total += 1;
                    }
                }
                // Каждый аккаунт покрыт ровно один раз, всего — `accounts`.
                assert_eq!(total, accounts);
                assert!(seen.iter().all(|&c| c == 1));
            }
        }
    }
}
