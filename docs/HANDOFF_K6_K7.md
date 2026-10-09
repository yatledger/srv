# Handoff: K6 и K7 (критический блок, завершение)

> Точка входа для новой сессии, которая продолжает проект `dagdb` и реализует оставшиеся
> задачи критического блока — **K6** (тесты критичных путей) и **K7** (README/.env/инструкция запуска).
>
> Этот документ самодостаточен. Перед началом всё равно прочитай `AGENTS.md`, `docs/PROMPT.md`,
> `docs/AUDIT.md`, `docs/tz/README.md`, `docs/tz/01-critical.md` (как требует регламент).

Дата составления: 2026-10-09.

---

## 1. Где мы находимся

- Репозиторий: `/home/pin/dev/yat/srv`, ветка `main`.
- Рабочее дерево чистое, всё закоммичено и запушено в `origin` (`git@github.com:yatledger/srv.git`).
- Последний commit: `716dc26 docs(tz): mark K5 done and log changes`.
- **K1–K5 выполнены** (статусы ✅ в `docs/tz/README.md`). Остались **K6** и **K7**.

История коммитов (последние):

```
716dc26 docs(tz): mark K5 done and log changes
e956b30 feat(storage): persist raft log and state machine with redb
5b7b56a docs(tz): mark K4 done and log changes
06ff2df security(raft): deterministic validation in state machine
cc195bf docs(tz): mark K1-K3 done and log changes
79f1bb5 security(api): authenticate internal endpoints with cluster token
dab7cbe security(tx): cover func in signed content and harden var handling
3c59c6d security(config): load all configuration and secrets from environment
```

---

## 2. Что уже сделано по задачам (кратко, с деталями реализации)

### K1 — ротация секретов и конфигурация из окружения ✅
- Новый `src/config.rs`: `AppConfig` через `clap` (фича `env`) + `dotenvy`.
  Переменные: `NODE_ID`, `ADVERTISE_ADDR`, `BIND_ADDR`, `HTTP_PORT`, `DATA_DIR`,
  `REDIS_URL`, `INTERNAL_API_TOKEN`, `CLUSTER_NODES`, таймауты
  (`HTTP_TIMEOUT_SECS`, `HTTP_CONNECT_TIMEOUT_SECS`, `RAFT_HTTP_TIMEOUT_SECS`,
  `RAFT_CONNECT_TIMEOUT_SECS`).
- `REDIS_URL` и `INTERNAL_API_TOKEN` — **обязательны** (ошибка при старте, если пусты).
- Хардкод пароля Redis убран из `src/lib.rs` и `to/*.rs`.
- `.env.example` дополнен; `docker-compose.yml` прокидывает `HTTP_PORT`, `INTERNAL_API_TOKEN`.
- Секрет `JQ2Y2dEUcGmmarKw9mDb2x9XXrVSKkst` удалён из **всей истории git** (`git filter-repo`),
  история перезаписана и force-push в `origin`. **Резервная копия до перезаписи**:
  `/tmp/opencode/srv-backup-1791504108.bundle`.
- Локальный `.env` создан (со старым паролем Redis, как просили) и не коммитится.
- ⚠️ **Открытый вопрос:** старый пароль Redis нужно ротировать на реальном инстансе (вне репо).

### K2 — целостность транзакции ✅
- `Tx` включает `func` в хэшируемый/подписываемый контент.
- `src/utils.rs`: вместо склейки без разделителей — каноничный JSON (рекурсивно
  отсортированные ключи). `ordered_sum(tx, func)`.
- Валидация известного `func` (`KNOWN_FUNCS = ["transferToken"]`) и структуры
  `TxVar` (`TxVar::validate`) — **до** `client_write`.
- `DAG::add_node_with_parents` больше не паникует (возвращает `Result`).

### K3 — защита эндпоинтов ✅
- Новый `src/auth.rs`: middleware `x-internal-token`, сравнение в постоянном времени.
- Публичный API: `/`, `/pool`, `/full`. Внутренний (с токеном): `/add`,
  `/remove_heavy_nodes`, `/load-genesis`, `/raft/*`, `/mng/*`.
- `add_handler` повторяет все проверки `add_tx`, включая сверку переданного `hash`
  с вычисленным.
- `web::Router` и processor прокидывают токен.

### K4 — детерминированная валидация в state machine ✅
- `src/raft/store.rs`: заглушки `parents_exist`/`nodes_exist` заменены на
  `validate_add`/`validate_remove` (уникальность узла, существование/уникальность
  родителей, `func`, `var`; для Remove — непустой список и существование узлов).
- ⚠️ Проверку длины родителей `2..100` **намеренно не** ставили в SM: генезис-узлы имеют
  пустой список родителей, иначе сломается bootstrap. Ограничение осталось на границе
  API (`validate_parents`).

### K5 — персистентность ✅
- Новый `src/raft/db.rs` — обёртка над **redb** (чистый Rust): таблицы `meta` и `logs`.
- `src/raft/log.rs` — `PersistentLogStore` (vote, committed, last_purged, записи лога);
  in-memory `LogStore` оставлен для тестов.
- `src/raft/store.rs` — `StateMachineStore::open(path)` персистит DAG/last_applied/
  membership/снапшот; `Default` = in-memory (для тестов и openraft Suite).
- `DAG::serialize` теперь включает реестр `added` (иначе удалённые узлы «воскресали»).
- `start_raft` использует `DATA_DIR`: `raft-log.redb`, `state-machine.redb`.
- End-to-end проверено: рестарт узла сохраняет DAG; в `DATA_DIR` создаются оба файла.

---

## 3. Что нужно сделать: K6 и K7

Технические требования (из `docs/tz/01-critical.md`):

- **K6**: unit-тесты на `ordered_sum` (в т.ч. негативные: подмена `func`, коллизии),
  `validate_parents`, `DAG::add/remove`, валидацию `var`; интеграционный тест
  записи/восстановления состояния.
- **K7**: README описывает назначение, стек, архитектуру, сборку, запуск кластера
  (из 4 узлов + Redis), env-переменные, примеры API.

Критерии готовности блока: `cargo test` покрывает критические сценарии и зелёный в CI.

### 3.1. Что из K6 уже покрыто (НЕ дублировать!)

Всего сейчас **38 тестов**, все зелёные. Уже есть:

| Область | Тесты |
|---|---|
| `ordered_sum`: детерминизм, подмена `func`, коллизия склейки, покрытие полей `Tx` | `utils::tests::ordered_sum_*` |
| `validate_func` | `utils::tests::validate_func_rejects_unknown` |
| `validate_parents` (валидные, мало/много, пустые/дубли) | `utils::tests::validate_parents_*` |
| `TxVar::validate` (валидный, пустые поля, длинный msg, неверный тип) | `graph::dag::tests::tx_var_*` |
| Non-panic `add_node_with_parents` | `graph::dag::tests::add_node_with_parents_does_not_panic_on_bad_var` |
| `validate_add` / `validate_remove` (SM) | `raft::store::tests::add_*`, `remove_*` |
| Permissionлог round-trip/reopen | `raft::db::tests::meta_roundtrip_survives_reopen`, `logs_roundtrip_and_trimming` |
| SM переживает reopen (+ `added`) | `raft::store::tests::state_machine_state_survives_reopen` |
| Авторизация токена | `auth::tests::*` |
| Конфигурация | `config::tests::*` |
| openraft Suite | `raft::test::test_mem_store` |

### 3.2. Что из K6 стоит ДОБАВИТЬ (приоритет)

1. **`DAG::add`/`DAG::remove` отдельно** (сейчас покрыты косвенно через `validate_*`):
   - `add_node_with_parents`: связи parent→child и child→parents корректны;
     повторная вставка того же узла (поведение);
     узел с отсутствующим родителем (что делает DAG).
   - `remove_node`/`remove_nodes`: узел уходит в `added`, связи у родителей/детей
     очищаются; повторное удаление; удаление отсутствующего (поведение).
   - `contains_node`, `is_node_added`, `get_node_count`.
2. **Интеграционный тест записи/восстановления** (полноценнее текущего):
   - Для лога: через `openraft::testing` или `Db` — проверить, что vote/committed/
     логи восстанавливаются. `IOFlushed` публично не конструируется, поэтому либо
     гонять через `Suite`, либо тестировать на уровне `Db` (как уже сделано) —
     можно расширить, проверив `last_purged`.
   - Для SM: применить `Request::Add`/`Request::Remove` через `RaftStateMachine::apply`
     и после reopen проверить и DAG, и `added`, и `last_applied`.
3. **Интеграционный тест `add_tx` через HTTP** (критичный путь): поднять axum-роутер
   (или `start_server` на свободном порту) и проверить:
   - валидную подпись принимает, подмену `func`/`var`/родителей — отклоняет;
   - `401` без токена на внутренних эндпоинтах;
   - duplicate node — отклоняется.
   (Требует дешёвого способа собрать `App` в тесте; `App` требует `Raft`, `Redis`,
   `StateMachineStore` — возможно, проще тестировать `validate_and_prepare_tx`, если
   вынести её в тестируемую функцию, или поднять реальный узел в тесте.)
4. **Тест подписи ed25519**: корректная подпись проходит, изменённый хэш/`func` — нет.
   (Сейчас есть только косвенно через `ordered_sum`.)

### 3.3. K7 — README и запуск

Создать `README.md` (сейчас отсутствует — находка S1/D1). Обязательные разделы:
- назначение; стек; архитектура (модули и их роли); сборка;
- запуск кластера **из 4 узлов + Redis** (`.env.example`, `docker-compose.yml`);
- таблица env-переменных;
- примеры API (публичные и внутренние с `x-internal-token`);
- как прогнать тесты (`cargo fmt --all -- --check`, `cargo clippy --all-targets`, `cargo test --all`).

Файлы `.env.example`, `Dockerfile`, `docker-compose.yml`, `test.sh` уже есть.
⚠️ `test.sh` нерабочий (команды без фона, устаревший формат `/add`) — при K7 стоит либо
починить, либо заменить разделом в README. Это в рамках K7.

---

## 4. Критичные факты о кодовой базе (чтобы не переоткрывать)

- Стек: Rust edition 2024, tokio, axum 0.7, openraft 0.10 (git databendlabs), redis 0.32,
  rayon, ed25519-dalek, base58, blake3, serde/serde_json, clap, dotenvy, **redb 2.6.4**.
- `Tx` (`src/lib.rs`): `prnts`, `addr`, `seq`, `var`; `func`/`sign` передаются отдельно,
  но `func` входит в хэш через `ordered_sum(tx, func)`.
- `tx_hash = blake3(canonical_json(Tx + func))`; подпись проверяется ключом из `tx.addr`
  (base58 ed25519).
- Публичный API vs внутренний разделены в `start_server` (`src/server.rs`).
- Инварианты: детерминированный SM; `func` в хэше; уникальность хэша; `added`
  переживает снапшот/рестарт; публичный/внутренний API разделены. **Не ломать.**
- `openraft` фича `serde` включена — `C::Entry`/`Vote`/`LogId` сериализуемы.

## 5. Известные ограничения / подводные камни

- `cargo clippy -D warnings` в CI пока **красный** из-за 2 предупреждений, не относящихся
  к K1–K5 (задача **D1**): `name \`DAG\` contains a capitalized acronym` и
  `empty line after doc comment` в `src/graph/weights.rs`. **Не ослаблять CI** — чинить
  предупреждения (можно в рамках D1 или, если K6/K7 требует зелёного CI, аккуратно поправить).
- `to/` — мёртвые черновики, **не собираются**. Не трогать без причины; удаление — задача D3.
- `genesis.json` содержит узлы с пустым `addr`/`sign` и пустыми `prnts` — учитывать при
  любой валидации в тестах (genesis без родителей легитимен).
- Тесты персистентности используют `std::env::temp_dir()` и уникальные имена —
  проблем с параллельностью нет.
- openraft Suite (`raft::test::test_mem_store`) требует in-memory store → нельзя делать
  `Default` для `StateMachineStore` персистентным.

## 6. Рабочий процесс (обязателен по `docs/PROMPT.md`)

- Одна задача = один шаг. После каждой: `cargo fmt --all`, `cargo clippy --all-targets`,
  `cargo test --all` — всё зелёное.
- Коммиты Conventional Commits со ссылкой `Refs: K6` / `Refs: K7`.
- После завершения обновить статусы и журнал в `docs/tz/README.md` (`⬜ → ✅`).
- Отчёт в конце: выполненные ID, изменённые файлы, результаты проверок, отклонения, что дальше.
- Не коммитить `.env`, не ослаблять CI, не трогать `to/`.

## 7. Заготовка промпта для новой сессии

```
Ты продолжаешь проект dagdb в /home/pin/dev/yat/srv.

Шаг 1. Прочитай целиком: AGENTS.md, docs/PROMPT.md, docs/AUDIT.md, docs/tz/README.md,
docs/tz/01-critical.md и docs/HANDOFF_K6_K7.md (в нём — контекст K1–K5 и точный объём K6/K7).
docs/PROMPT.md — обязательный регламент целиком.

Шаг 2. Реализуй K6 и K7 из docs/tz/01-critical.md строго по техтребованиям. Учти, что часть
тестов K6 уже написана (см. HANDOFF, раздел 3.1) — не дублируй; добавь то, что перечислено
в разделе 3.2, и README/инструкцию запуска из раздела 3.3.

Шаг 3. Соблюдай: не выдумывать требования сверх ТЗ; следовать стилю; одна задача = один шаг;
после каждой задачи cargo fmt --all + cargo clippy --all-targets + cargo test --all;
не хранить секреты в коде; обновить статусы в docs/tz/README.md; коммиты Conventional Commits
со ссылкой Refs: <ID>.

Шаг 4. Сначала выведи краткий план и список файлов и дождись моего «ок» перед правками.
В конце дай отчёт: выполненные ID, изменённые файлы, результаты проверок, отклонения, что дальше.
```

---

## 8. Открытые вопросы для следующей сессии

1. **Ротация Redis-пароля** на реальном инстансе — вне репозитория (K1), требует доступа к
   серверу. Код и git уже очищены.
2. **Строгий CI (D1):** K6 требует «зелёный в CI». Решить, чинить ли 2 clippy-предупреждения
   в рамках K6 или сослаться на D1. Рекомендация: аккуратно починить в отдельном коммите
   (не смешивая с логикой K6), чтобы CI стал зелёным.
3. **Интеграционный HTTP-тест** (п. 3.2.3): требует собрать `App` в тесте. Если окажется
   дорого — можно вынести чистую часть `validate_and_prepare_tx` в тестируемую функцию, но
   не менять поведение K2/K3.
4. **`test.sh`** — починить или заменить README (в рамках K7).
