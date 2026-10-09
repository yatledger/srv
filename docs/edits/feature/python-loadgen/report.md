# Отчёт о реализации `feature/python-loadgen` (N02)

> Группа: **edits/feature/python-loadgen** · Тип: **feature** · Статус: ✅ **выполнено**.
> Сопутствующие: [`spec.md`](spec.md) · [`plan.md`](plan.md) · [`tracker.md`](tracker.md).

## Кто и когда

- Исполнитель: агентская сессия (OpenCode).
- Дата начала проектирования: 2026-10-09.
- Дата реализации: 2026-10-09.
- Коммит-база: `7618fe2` (пакет ТЗ/плана — `614850c`).
- Ветка: `main` (рабочее дерево).

## Что сделано

Реализованы все четыре этапа `N02.1`–`N02.4`:

- **N02.1. Канонический хэш/подпись.** `python/loadgen/crypto.py` — построчный
  порт `canonical_json`/`ordered_sum` из `src/utils.rs` (рекурсивная сортировка
  ключей, сохранение порядка массивов, JSON-экранирование как в `serde_json`),
  `func` в контенте, `blake3` (hex); `sign_message` — ed25519 над **сырыми**
  байтами хэша. Сверено с реальным Rust-тестом (`tests/_tmp_hash_check.rs`,
  удалён): хэш `1e935195da0c4dfe20b7dbf408907d923027b850e80474d4c5a61dbe199cabad`,
  подпись `989464ef…1410d` — совпали побайтово.
- **N02.2. Генератор и клиент.** `generator.py` — один настраиваемый генератор
  (аккаунты из `keys.py`, монотонный `seq` под `asyncio.Lock`, `k = clamp(random,
  parents_min, parents_max)`, `k ∈ 2..min(100,|pool|)`, `var.ca` = адрес
  отправителя); `client.py` — `httpx.AsyncClient` с `pool`/`full`/`health`,
  ретраями и экспоненциальным backoff на `429`/`503`/`5xx`/сетевых ошибках,
  ротацией узла, классификацией `accepted`/`rejected`/`error` и извлечением
  `message` из `ApiResponse`.
- **N02.3. Драйвер и отчёт.** `runner.py` — `--concurrency` sender-задач,
  непрерывные блоки аккаунтов (монотонность без межзадачной синхронизации),
  пейсинг целевого TPS, периодический refresh `/pool`; `report.py` — агрегация
  accepted/rejected по причинам, достигнутый TPS, `p50/p95/p99`, `GET /full` по
  каждому узлу, корректностные инварианты (нет отказов валидных tx, `total`
  совпадает) → `ok`, JSON-отчёт; `main.py` — оркестрация, `KeyboardInterrupt`,
  код возврата `0`/`1`. Убраны мёртвые генераторы исходника (R8).
- **N02.4. Тесты, docs, E2E.** `python/tests/` — 22 теста (crypto golden +
  негатив, генератор, отчёт); `python/README.md`; E2E на реальном кластере.

## Изменённые файлы

Новые:

- `python/loadgen/{__init__,__main__,crypto,config,generator,client,runner,report,main}.py`;
- `python/keys.py` — тестовые ключи (seed-производные, детерминированные), override из `LOADGEN_KEYS`;
- `python/requirements.txt`, `python/pytest.ini`, `python/README.md`;
- `python/tests/{test_crypto,test_generator,test_report}.py`.

Правки (docs/инфраструктура):

- `.gitignore` — `.venv/`, `__pycache__/`, `.pytest_cache/`, `python/*.json`;
- `docs/edits/feature/README.md`, `docs/edits/feature/python-loadgen/{spec,plan,tracker}.md` — статусы.

**Rust-код (`src/`, `Cargo.toml`, `docs/api/`, `.env.example`) не изменялся.**

## Пройденные проверки

| Проверка | Результат |
|---|---|
| `pytest -q` (Python) | ✅ 22 passed |
| Сверка хэша/подписи с Rust (`tests/_tmp_hash_check.rs`, удалён) | ✅ побайтовое совпадение |
| `cargo fmt --all -- --check` | ✅ без замечаний |
| `cargo clippy --all-targets -- -D warnings` | ✅ зелёно |
| `cargo test --all` | ⚠️ lib/api/loadgen зелёные; `tests/cluster` падает (см. ниже) |
| E2E на кластере (Redis + 4 узла) | ✅ 120/120 принято, DAG 123 на всех репликах, код 0 |

### E2E (детали)

Redis (Docker) + 4 узла `dagdb` на хосте (`21001..21004`), `PUBLIC_RATE_LIMIT_PER_SEC=0`,
`CLEANUP_BATCH_SIZE=1`, `PROCESSOR_INTERVAL_MS=600000`. Bootstrap: `mng/init` →
`add-learner` → `change-membership` → `load-genesis`; все реплики увидели `total=3`.

Прогон: `--tx 120 --tps 200 --concurrency 8 --accounts 16 --parents-min 2 --parents-max 6
--seed 42 --internal-token … --json-out …`.

```
принято: 120/120 (достигнуто 88.0 TPS)
отказы: —
задержка мс: p50=40.50 p95=777.31 p99=793.75 mean=88.10 max=796.66
DAG: 123 / 123 / 123 / 123
ИТОГ: OK — корректностные инварианты выполнены (exit=0)
```

## Отклонения от плана

- **Golden-вектор из `spec.md` §2.3** (`0d5636a3…`, подпись `76a781b8…`) не удалось
  воспроизвести: ключ `pk0` из контрольной tx (§2.3) в репозитории не сохранился
  (в `genesis.json` другие адреса; исходный `keys.py` владельца не был закоммичен).
  Для golden-теста сгенерирован **новый** детерминированный набор тестовых ключей
  (`keys.py`) и вектор сверен с реальным Rust-тестом. Совместимость порта с Rust
  подтверждена на новом векторе; сам алгоритм хэша/подписи не изменился.
- Ключи генерируются детерминированно из `blake3("dagdb-loadgen-key-v1" + i)` —
  воспроизводимо и пригодно для golden-теста (в исходном боте владелец задавал
  ключи вручную).
- `--parents-min/--parents-max` вместо единственного `--parents` (см. `spec.md` R4):
  диапазон числа родителей.
- E2E выполнен на настоящих узлах `dagdb` на хосте + Redis в Docker (не через
  `docker compose`): это дало тот же реальный HTTP-кластер и не потребовало
  пересборки образа с Python.

## Независящее падение `tests/cluster`

`tests/cluster/test_cluster.rs::cluster_init_membership_replication_snapshot_restart_concurrent`
падает на шаге 7 («конкурентные записи»): 10 задач одновременно пишут в лидер
транзакции **одного адреса** с `seq = 100..109`. Так как порядок применения в Raft
не гарантирован, транзакция с меньшим `seq` может прийти после большей и
детерминированно отклоняется проверкой V18 (монотонность `seq`) → `400`.
Это **предсуществующее** свойство теста и V18, не связанное с N02: Rust-код не
менялся, а сам инвариант V18 — целевое поведение. Отклонение зафиксировано, из
скоупа N02 вынесено (кандидат в отдельный `fix`).

`cargo test --all` при этом прошёл для `--lib` (97+), `--test api` (5) и
`--bin loadgen` (15); падает только `--test cluster`.

## Что дальше

- Кандидат `fix`: `tests/cluster` — сериализовать конкурентные записи одного
  адреса (уникальные адреса или последовательный `seq`) либо явно документировать
  ожидаемые отказы V18.
- Кандидат: CI-job для `pytest` (вне обязательного PR-пайплайна).
- Кандидат: вынос `keys.py` в `.gitignore`/env по мере надобности (override
  `LOADGEN_KEYS` уже реализован).

---

## Приложение: предпроектная сверка (2026-10-09)

Проведена при подготовке ТЗ (исходники не менялись):

- Ключи `sk[i]`/`pk[i]` корректны (`base58(verify_key(sk)) == pk`).
- Подпись исходного бота **отклоняется** узлом: `BadSignatureError` (причина — см. `spec.md` §2.3).
- Верный порт `canonical_json + blake3` воспроизводит хэш Rust для контрольной tx:
  `0d5636a3c5dabbaccf44136a38c59e627bf87d5d5103e4053192a3e5b77c01af`
  (сверено временным Rust-тестом `tests/_tmp_hash_check.rs`, удалён после проверки).
- Исправленная подпись (ed25519 над сырым digest) узлом **принимается**:
  `76a781b8f80baeb4bb13f9910bb857cbc89487e3cc6a7db8316a68c44d958683d5b8fa30154acc11d85f25313014ff123f966012721669927e1a27eb3426940b`.
