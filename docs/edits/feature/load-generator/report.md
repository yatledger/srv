# Отчёт о реализации `feature/load-generator` (N01)

> Группа: **edits/feature/load-generator** · Тип: **feature** · Статус: ✅ **выполнено**.
> Сопутствующие документы: [`spec.md`](spec.md) · [`plan.md`](plan.md) · [`tracker.md`](tracker.md).

## Кто и когда

- Исполнитель: агентская сессия (OpenCode).
- Дата: 2026-10-09.
- Коммит-база: `5b3e1b7` (`feature generator`, пакет ТЗ/плана).
- Ветка: `feat/load-generator` (в текущем рабочем дереве — `main`).

## Что сделано

Реализованы все четыре этапа `N01.1`–`N01.4`:

- **N01.1. Каркас + in-process кластер.** Бинарь `loadgen` с clap-парсером; подъём
  `N` узлов `dagdb` в одном процессе (каждый на своём потоке с current-thread
  runtime), bootstrap (init → leader → add-learner → change-membership → genesis),
  graceful shutdown и очистка временных каталогов. Очистка и rate limit узлов
  выключены (`cleanup_batch_size = 0`, `public_rate_limit_per_sec = 0`).
- **N01.2. Генератор валидных транзакций.** Детерминированный PRNG без новых
  зависимостей; ключи ed25519 из seed-производных байт; монотонный `seq` по
  аккаунту; родители — `k` живых узлов из `/pool`, зажатые в `2..=min(100,|pool|)`;
  канонический хэш и подпись — через `dagdb::utils::ordered_sum`/`TRANSFER_TOKEN`
  (без копий логики).
- **N01.3. Драйвер и отчёт.** Конкурентные async sender-задачи, целевой TPS через
  пейсинг, сбор `(status, reason, latency)`; подсчёт принятых/отклонённых,
  достигнутого TPS, p50/p95/p99, размеров DAG по репликам; JSON-отчёт заданной
  схемой. Корректностные инварианты: нет отказов валидных транзакций и одинаковый
  размер DAG на всех репликах (влияют на код возврата).
- **N01.4. Скрипт, CI, документация.** `scripts/loadgen.sh` (проверка Redis, сборка
  release, запуск), job `load` в `.github/workflows/ci.yml` (`workflow_dispatch` и
  nightly, вне PR-пайплайна, публикует JSON-артефакт), раздел README.

## Изменённые файлы

Новые:

- `src/bin/loadgen/main.rs` — точка входа, clap, оркестрация и код возврата;
- `src/bin/loadgen/cluster.rs` — in-process кластер и bootstrap;
- `src/bin/loadgen/generator.rs` — генератор валидных транзакций;
- `src/bin/loadgen/runner.rs` — драйвер нагрузки;
- `src/bin/loadgen/report.rs` — агрегация и вывод отчёта;
- `src/bin/loadgen/prng.rs` — детерминированный ГПСЧ;
- `scripts/loadgen.sh` — обёртка прогона.

Правки:

- `.github/workflows/ci.yml` — job `load` + триггеры `workflow_dispatch`/`schedule`;
- `README.md` — раздел «Нагрузочное тестирование» и карта кода;
- `docs/edits/feature/README.md`, `docs/edits/feature/load-generator/{tracker,report}.md` — статусы.

Расхождения с `plan.md` §3: модуль точки входа размещён как `src/bin/loadgen/main.rs`
(а не `src/bin/loadgen.rs`) — иначе Rust ищет подмодули `src/bin/*.rs`. Новых
зависимостей нет; использован async `reqwest` (вариант `blocking` не подключён в
`Cargo.toml`, а добавлять фичу план не предполагал).

## Пройденные проверки

| Проверка | Результат |
|---|---|
| `cargo fmt --all` | ✅ без замечаний |
| `cargo clippy --all-targets -- -D warnings` | ✅ зелёно |
| `cargo test --bin loadgen` | ✅ 15 тестов |
| Прогон `loadgen` (`--nodes 3 --tx 60 --tps 200 --concurrency 4 --accounts 8`) | ✅ accepted 60/60, rejected 0, DAG 63/63/63 |
| Корректностные инварианты | ✅ нет отказов валидных tx, реплики совпали (код 0) |
| CI-job `load` | добавлен (проверка в GH Actions — при пуше) |

## Отклонения от плана

- Точка входа — `src/bin/loadgen/main.rs` (см. выше).
- Использован async `reqwest` вместо blocking (не тянуть новую feature-зависимость).
- Число узлов по умолчанию — 3 (как в `tests/cluster`).
- JSON-схема — как предложено в `plan.md` §4 шаг 9 (добавлено поле `attempted`).

## Что дальше

Кандидаты в отдельные задачи: внешний режим `--target` (нагрузка на удалённый
кластер с предупреждением о недоверенных средах), измерения ресурсов узлов
(CPU/RSS), распределённая генерация нагрузки, пороги SLA как отдельный nightly-гейт.
