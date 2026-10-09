# N04 — план реализации логов событий узлов

> Группа: **edits/feature/node-event-logs** · Тип: **feature** · Статус: ✅ **выполнено**.
> Сопутствующие документы: [`spec.md`](spec.md), [`tracker.md`](tracker.md), [`report.md`](report.md).

## 1. Цель и границы

Добавить человекочитаемые события узлов при записи транзакций («получила / переправила / записала»)
и наглядно показать их в `loadgen` через stderr, не ломая пофреймовый вывод и отчёт. Изменения
аддитивны, инварианты и контракты не затрагиваются.

## 2. Требования к поведению

См. R1–R7 в [`spec.md`](spec.md) §3. Кратко: `info`-события `нода {id} получила/переправила/записала
транзакцию {short_hash}`, короткий хэш, вывод в stderr у `loadgen`, приглушённый `openraft`-шум,
управление `RUST_LOG`/`--verbose`.

## 3. Затрагиваемые файлы

| Файл | Роль | Действие |
|---|---|---|
| `src/utils.rs` | `short_hash(value) -> String` (head…tail) | правка |
| `src/server.rs` | события «получила»/«переправила» в `add_tx` | правка |
| `src/raft/store.rs` | `node_id` в `StateMachineStore`; событие «записала» в `apply` | правка |
| `src/lib.rs` | `pub mod log_filter`; передать `node_id` в `StateMachineStore::open` | правка |
| `src/log_filter.rs` | `default_filter(verbose)` (общий дефолт `info` + `openraft=error`) | **новый** |
| `src/main.rs` | дефолт фильтра через `log_filter::default_filter` | правка |
| `src/bin/loadgen/main.rs` | `default_log_filter` через `log_filter`; stderr + компактный форматтер `NodeLogFormat` | правка |
| `README.md` | раздел наблюдения: события узлов, уровень логов | правка |
| `docs/edits/feature/node-event-logs/*` | задача фичи (этот пакет) | **новые** |
| `docs/edits/feature/README.md` | строка `N04` | правка |

`Cargo.toml`, `.env.example`, `docs/api/`, CI — не меняются (новых зависимостей нет).

## 4. Шаги реализации

1. `utils::short_hash` — сокращение хэша + тест.
2. `raft/store.rs`: поле `node_id`, `open(path, node_id)`, событие «записала» в ветке успешного
   `add_node_with_parents`; тестовые вызовы `open` → `open(…, 1)`.
3. `lib.rs`: `StateMachineStore::open(…, node_id)`; `pub mod log_filter`.
4. `server.rs`: `info!` «получила» на обеих ветках (лидер/не-лидер) и «переправила» после успешного
   форварда.
5. `log_filter.rs`: общий `default_filter(verbose)` `info` + `openraft=error`.
6. `src/main.rs` и `bin/loadgen/main.rs`: подключить `default_filter`; в loadgen — stderr и компактный
   `NodeLogFormat` (без span-контекста).
7. Тесты, `README.md`, пакет документации.

## 5. Тесты

- **Юнит:** `short_hash_truncates_with_ellipsis` (`utils`); `default_log_filter_quiet_unless_verbose`
  (loadgen). Поведенческая проверка событий — ручными прогонами (требует in-process кластера + Redis):
  «получила»/«переправила»/«записала» присутствуют, реплик «записала» равно числу узлов.
- Интеграционные `tests/api`/`tests/cluster` не ломаются; шум в их выводе снижается.

## 6. Docs

`README.md` (наблюдение/логи), `docs/edits/feature/node-event-logs/{spec,plan,tracker,report}.md`,
`docs/edits/feature/README.md`. Публичный API не меняется ⇒ `docs/api/` не трогаем.

## 7. DoD

- [x] В логах видны события «получила/переправила/записала» с коротким хэшем.
- [x] «записала» появляется на всех репликах (по числу узлов на транзакцию).
- [x] `loadgen`: события в stderr, пофреймовый вывод/отчёт — в stdout, без `openraft`-шума.
- [x] `fmt`/`clippy -D warnings`/`test --all` — зелёные (Redis для интеграционных).
- [x] Инварианты §6 не нарушены; `SCHEMA_VERSION`/Raft-контракт/API не менялись; новых зависимостей нет.

## 8. Риски и откат

| Риск | Митигация |
|---|---|
| Логи мешают пофреймовому выводу/JSON | вывод событий — stderr; stdout остаётся машинно-читаемым |
| `info` шумит `openraft` | директива `openraft=error`; `RUST_LOG` переопределяет |
| Влияние на state machine | `node_id` не сериализуется; поведение apply не меняется |
| Двойное «записала» на лидере | лидер применяет лог один раз; дублей нет |

**Откат:** изменения аддитивны (логи + фильтр + форма); откат — `git revert`. Миграция не нужна.
