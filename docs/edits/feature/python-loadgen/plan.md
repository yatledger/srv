# N02 — план реализации Python-нагрузчика

> Группа: **edits/feature/python-loadgen** · Тип: **feature** · Статус: 📐 **проектирование**
> (ТЗ/план; реализация — после «ок» владельца).
> Сопутствующие: [`spec.md`](spec.md) (ТЗ), [`tracker.md`](tracker.md), [`report.md`](report.md).
> Родственная задача: [`../load-generator/`](../load-generator/) (`N01`, Rust-`loadgen`).

## 1. Цель и границы

Перенести внешний Python-бот в репозиторий (`python/`) как **внешний HTTP-клиент** к кластеру
`dagdb`: корректный канонический хэш/подпись, валидная генерация транзакций, CLI, устойчивость,
отчёт с проверкой консистентности. Границы и не-цели — [`spec.md`](spec.md) §4.

## 2. Требования к поведению после реализации

См. R1–R9 в [`spec.md`](spec.md) §3. Кратко: внешний клиент; порт `canonical_json`+`blake3`+ed25519 из
`src/utils.rs`; монотонный `seq`; 2..100 живых родителей из `/pool`; один настраиваемый генератор; CLI;
retry/backoff на `429`/`503`/таймаут; отчёт (accepted/rejected по причинам, TPS, p50/p95/p99,
`/full` по узлам) + JSON; корректностные инварианты влияют на код возврата.

## 3. Затрагиваемые файлы

Все — **новые**; Rust-код не трогаем.

| Файл | Роль |
|---|---|
| `python/README.md` | Запуск, параметры, ограничения, пометка о ключах |
| `python/requirements.txt` | Зависимости: `httpx`, `pynacl`, `blake3`, `base58` (+ `pytest` для тестов) |
| `python/keys.py` | `sk`/`pk` (решение владельца); override из env |
| `python/loadgen/__init__.py` | Пакет, версия |
| `python/loadgen/__main__.py` | `python -m loadgen` → `main()` |
| `python/loadgen/crypto.py` | `canonical_json`/`ordered_sum` (blake3) + `sign_message` — **порт `src/utils.rs`** |
| `python/loadgen/config.py` | CLI (argparse) + валидация параметров |
| `python/loadgen/generator.py` | Один настраиваемый генератор: аккаунты, монотонный `seq`, родители, `var` |
| `python/loadgen/client.py` | `httpx.AsyncClient`: `POST /`, `GET /pool`, `/full`, `/health`, retry/backoff |
| `python/loadgen/runner.py` | Конкурентные sender-задачи, пейсинг целевого TPS, сбор статусов/латентностей |
| `python/loadgen/report.py` | Агрегация, корректностные инварианты, текст + JSON |
| `python/loadgen/main.py` | Оркестрация, код возврата |
| `python/tests/test_crypto.py` | Golden-вектор: сверка хэша/подписи с Rust |
| `python/tests/test_generator.py` | Монотонность `seq`, границы числа родителей, детерминизм seed |
| `python/tests/test_report.py` | Правило ok/ошибка, агрегация причин |
| `docs/edits/feature/python-loadgen/*` | Задача (этот пакет) |
| `docs/edits/feature/README.md` | Добавить `python-loadgen` в таблицу |
| `.gitignore` | Отчёты прогона (`python/*.json`, `.venv/`, `__pycache__/`) — при необходимости |

Комментарии/доки — RU; `python/README.md` на RU. Rust-регламент (`cargo fmt/clippy/test`) остаётся
зелёным (мы его не трогаем).

## 4. Шаги реализации

### Этап 1 — хэш/подпись (фундамент)

1. `crypto.py`: точный порт `canonical_json` из `src/utils.rs` (`write_canonical`): `null/true/false`,
   числа как в `serde_json`, строки через `json.dumps(ensure_ascii=False)`, массивы — порядок как есть,
   объекты — ключи `sorted()`; `ordered_sum(tx, func)` добавляет `func` и возвращает `blake3.hexdigest()`;
   `sign_message(sk_b58, hash_hex)` — ed25519 над **сырыми** байтами хэша (`bytes.fromhex`), hex.
2. `tests/test_crypto.py`: golden-вектор из сверки (§2.3 `spec.md`):
   `tx={prnts:["aaa","bbb"],addr:pk0,seq:1,var:{ca:pk0,to:pk1,val:100,msg:"hello"}}`,
   `func="transferToken"` → хэш `0d5636a3…c01af`, подпись `76a781b8…6940b` (проверяется `VerifyKey`).
3. Кратковременная сверка с Rust (временный `tests/_tmp_hash_check.rs`, удаляется) — уже выполнена на
   этапе проектирования; при реализации прогнать заново.

### Этап 2 — генератор и клиент

4. `config.py`: argparse — `--hosts` (список или базовый хост), `--tx`|`--duration-sec`, `--tps`,
   `--concurrency`, `--accounts`, `--parents-min`/`--parents-max`, `--seed`, `--json-out`, `--retry`,
   `--timeout`, `--internal-token`, `--verbose`.
5. `generator.py`:
   - аккаунты — из `keys.py` (`sk`/`pk`); адрес = `pk[i]`;
   - **монотонный `seq` на аккаунт** (счётчик в объекте аккаунта; при конкурентности — `asyncio.Lock`
     на аккаунт, чтобы `seq` выдавался и подтверждался по порядку; альтернатива — по аккаунту на задачу);
   - `to` — случайный `pk`; `val` — случайное ≥ 0; `msg` — короткая строка;
   - родители — `k = clamp(random, parents_min, parents_max)`, `2 ≤ k ≤ min(100, |pool|)`; при `|pool|<2`
     — ожидание/ретрай; выбор — окно/случайное подмножество живого пула;
   - сборка `tx`, `hash = ordered_sum(tx, "transferToken")`, `sign = sign_message(sk[i], hash)`.
6. `client.py`: общий `httpx.AsyncClient`; методы `pool()`, `full()`, `health()`, `post_tx(body)`;
   retry с экспоненциальным backoff на `429`/`503`/сетевых ошибках; ротация недоступного узла;
   классификация ответа: `2xx` → accepted, `4xx` → rejected (+`message`), иное → error.

### Этап 3 — драйвер и отчёт

7. `runner.py`: `--concurrency` sender-задач; пейсинг целевого TPS (общий токен-бакет либо
   per-worker интервал); сбор `(status, reason, latency)`; периодический refresh `/pool`.
8. `report.py`: агрегация accepted/rejected по причинам, достигнутый TPS, p50/p95/p99,
   `GET /full` на каждом узле → `total`; корректностные инварианты (нет отказов валидных tx;
   `total` совпадает) → `ok`; JSON по схеме (§R6). Информационные метрики на код возврата не влияют.
9. `main.py`: оркестрация, обработка `KeyboardInterrupt`, код возврата (`0` — ok, `1` — нарушение
   инварианта/ошибка параметров).

### Этап 4 — тесты, docs, E2E

10. `tests/`: крипто (golden), генератор (монотонность/границы/детерминизм), отчёт (правило ok).
11. `python/README.md`: примеры запуска (локальный Docker-кластер и удалённый `--hosts`), параметры,
    ограничения, пометка про ключи/безопасность.
12. `docs/edits/feature/README.md` — добавить `N02`.
13. **E2E в Docker:** поднять Redis + 4 узла (`21001..21004`) с `PUBLIC_RATE_LIMIT_PER_SEC=0`,
    загрузить genesis, прогнать короткий сценарий, проверить инварианты; зафиксировать результат в
    `report.md`.

## 5. Тесты

- **`test_crypto.py`** — golden-вектор хэша/подписи (порт `utils.rs`); отрицательный кейс — подпись
  старого формата (`blake3(ordered_sum)`) узлом отклоняется.
- **`test_generator.py`** — `seq` строго растёт на аккаунт при параллельной выдаче; число родителей
  в `2..=min(100,|pool|)`; при одном `--seed` параметры воспроизводимы.
- **`test_report.py`** — accepted/rejected по причинам, правило `ok`, схема JSON.
- Прогон: `pytest -q` из `python/`. Rust: `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`,
  `cargo test --all` (Redis для интеграционных) — должны остаться зелёными (Rust не меняем).

## 6. Docs

`python/README.md`; `docs/edits/feature/python-loadgen/{spec,plan,tracker,report}.md`;
`docs/edits/feature/README.md`. Публичный API/`openapi.json`/`Cargo.toml`/`.env.example` — **не трогаем**.

## 7. DoD

- Python-нагрузчик запускается против Docker-кластера и завершает короткий прогон с кодом `0`.
- Печатает accepted/rejected (по причинам), достигнутый TPS, p50/p95/p99, `total` по репликам.
- Все валидные tx приняты, `total` совпадает на всех репликах, при одном `--seed` параметры
  воспроизводимы.
- `pytest -q` зелёный; Rust-проверки (`fmt`/`clippy`/`test`) остаются зелёными.
- Инварианты `AGENTS.md` §6 не нарушены; `src/` не изменён; `SCHEMA_VERSION`/Raft-контракт/публичный API
  не менялись.
- `python/README.md` и `docs/edits/feature/` обновлены.

## 8. Риски и откат

| Риск | Митигация |
|---|---|
| Расхождение Python-хэша с Rust из-за деталей сериализации (числа/юникод/экранирование) | golden-вектор + временная сверка с реальным Rust-тестом; порт построчно с `canonical_json` |
| `seq` при конкурентности (несколько задач на один аккаунт) | `asyncio.Lock` на аккаунт либо один аккаунт на задачу; юнит-тест на монотонность |
| `429`/rate limit и «уезжающие» родители | `PUBLIC_RATE_LIMIT_PER_SEC=0` и `cleanup_batch_size=0` на время прогона; retry/backoff; повторный `/pool` |
| Утечка секретов (решение владельца коммитить `keys.py`) | пометка в README; тестовые ключи дев-стенда; override из env; возможность вынести файл в `.gitignore` позже |
| Недоступность удалённого кластера | ротация хостов, таймауты, явная ошибка и ненулевой код возврата |
| Python-зависимости отсутствуют в окружении | `python/requirements.txt`; проверка окружения при старте |

**Откат:** изменения аддитивны (новый каталог `python/` + документы). Удаление каталога и записей в
docs возвращает проект в исходное состояние; данных/схемы не затрагивается. Точка отката — `7618fe2`.

## 9. Этапность и вне скоупа

- **Этап 1 (MVP):** крипто-порт + генератор + клиент + базовый отчёт + JSON + тесты.
- **Этап 2:** перцентили, `/full` по узлам, retry/backoff, `--target`/удалённый кластер.
- **Этап 3:** документация, E2E, при желании — CI-задача (вне PR).
- **Вне скоупа:** in-process кластер (это `N01`), «реалистичные» генераторы, распределённая нагрузка,
  TLS, измерения ресурсов, обязательный PR-CI.

## 10. Открытые вопросы / решения владельца

Закрыты (§1 `spec.md`). На реализацию: финальный набор CLI-флагов; нужен ли `--internal-token`;
оставлять ли `keys.py` в git или вынести в env позже.
