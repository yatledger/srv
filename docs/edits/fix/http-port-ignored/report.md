# Отчёт о реализации: fix/http-port-ignored (F2)

> Группа: **edits/fix/http-port-ignored** · Тип: **fix** · Статус: **реализовано, проверки пройдены**.
> Ссылки: [`defect.md`](defect.md), [`plan.md`](plan.md), [`tracker.md`](tracker.md).

## Кто / когда

- Исполнитель: агент
- Коммит-база: `60793a1` (документация задачи F2); код-предшественник — `370ce84`
- Дата: 2026-10-09

## Изменённые файлы

| Файл | Что изменено |
|---|---|
| `src/config.rs` | `port: u16` → `Option<u16>` (отличаем «HTTP_PORT задан явно» от дефолта); добавлена константа `DEFAULT_HTTP_PORT`; `bind_addr()` отдаёт приоритет явному `HTTP_PORT`/`--port` над `BIND_ADDR` с предупреждением; `advertise_addr()` использует эффективный порт; хелпер `port_from_addr`; тесты (в т.ч. регрессионный F2). |
| `src/lib.rs` | Тестовая фабрика `AppConfig`: `port: Some(21001)`. |
| `tests/api/test_api.rs` | Фабрика `AppConfig`: `port: Some(port)`. |
| `tests/cluster/test_cluster.rs` | Фабрика `AppConfig`: `port: Some(port)`. |
| `.env.example` | Узлоспецифичные `NODE_ID`/`BIND_ADDR`/`HTTP_PORT`/`ADVERTISE_ADDR`/`DATA_DIR` закомментированы с пояснением; описано, что `.env` читается всеми узлами. |
| `README.md` | Приоритет `HTTP_PORT` над `BIND_ADDR` (+ строка в таблице env и предупреждение об общем `.env` в инструкции запуска). |
| `docs/edits/fix/http-port-ignored/*` | Статусы/отчёт задачи. |

## Пройденные проверки

- [x] `cargo fmt --all`
- [x] `cargo clippy --all-targets -- -D warnings`
- [x] `cargo test --all` — `111` lib + `5` api + `1` cluster, все зелёные
- [x] `cargo bench --no-run`
- [x] Ручная проверка из корня репозитория (`.env` с `BIND_ADDR=0.0.0.0:21001`):
      `NODE_ID=9 HTTP_PORT=21004 … dagdb` → предупреждение `HTTP_PORT=21004 перекрывает BIND_ADDR=…`,
      `Server running at 0.0.0.0:21004`
- [x] Регрессионный тест `bind_addr_prefers_explicit_port_over_bind_addr` (падал до фикса:
      `bind_addr()` возвращал `0.0.0.0:21001`) проходит после.

## Отклонения от плана

- **Тип поля `port`.** Изменён с `u16` на `Option<u16>`, чтобы отличать «`HTTP_PORT` задан явно» от
  значения по умолчанию. Это механически затронуло фабрики `AppConfig` в `src/lib.rs` и тестах.
  Совместимость CLI/env сохранена (значение по умолчанию не изменилось — `21001`).
- **Поведение `BIND_ADDR` из процесса при заданном `HTTP_PORT`.** По выбранному владельцем контракту
  явный `HTTP_PORT` побеждает и его (с предупреждением). Для типового сценария (`.env`-ный общий
  `BIND_ADDR`) это именно то, что нужно; при необходимости приоритет «in-process BIND_ADDR главный»
  уточняется отдельно.
- **`ADVERTISE_ADDR`.** Диагностика расхождения с `HTTP_PORT` не добавлялась (вне решения владельца);
  `advertise_addr()` теперь корректно берёт эффективный порт, когда `ADVERTISE_ADDR` не задан.
- В остальном — по плану.

## Что дальше

- При желании — отдельная `feature`-задача на `--env-file`/`DOTENV_PATH` (общий `.env` с секретами +
  узловой оверрайд), чтобы вообще не держать узлоспецифичные ключи в окружении.
