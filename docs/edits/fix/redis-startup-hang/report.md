# Отчёт о реализации: fix/redis-startup-hang (F1)

> Группа: **edits/fix/redis-startup-hang** · Тип: **fix** · Статус: **реализовано, проверки пройдены**.
> Ссылки: [`defect.md`](defect.md), [`plan.md`](plan.md), [`tracker.md`](tracker.md).

## Кто / когда

- Исполнитель: агент
- Коммит-база: `716cc13` (`f22431d` — docs-оформление задачи)
- Дата: 2026-10-09

## Изменённые файлы

| Файл | Что изменено |
|---|---|
| `src/config.rs` | Поле `redis_connect_timeout_secs` (env `REDIS_CONNECT_TIMEOUT_SECS`, CLI `--redis-connect-timeout-secs`, дефолт `5`), геттер `redis_connect_timeout()`, валидация `> 0`, тесты дефолта/валидации. |
| `src/lib.rs` | Подключение Redis перенесено **до** открытия redb-хранилищ и `Raft::new`; `ConnectionManager::new` обёрнут в `tokio::time::timeout(cfg.redis_connect_timeout(), …)`; таймаут/ошибка маппятся в понятную ошибку без секрета. Добавлен регрессионный тест. |
| `tests/api/test_api.rs` | Новое поле в фабрике `AppConfig`. |
| `tests/cluster/test_cluster.rs` | Новое поле в фабрике `AppConfig`. |
| `.env.example` | `REDIS_CONNECT_TIMEOUT_SECS=5`; пояснение к `REDIS_URL` (пароль обязателен при `--requirepass`, иначе URL без пароля). |
| `README.md` | Строка `REDIS_CONNECT_TIMEOUT_SECS` в таблице env; локальный запуск Redis с `--requirepass` и согласование с `REDIS_URL`. |
| `docs/edits/fix/redis-startup-hang/*` | Статусы/отчёт задачи. |

## Пройденные проверки

- [x] `cargo fmt --all`
- [x] `cargo clippy --all-targets -- -D warnings`
- [x] `cargo test --all` — юнит (`107 passed`) и API (`5 passed`) зелёные; **кластерный тест падает по предсуществующей причине** (см. «Отклонения»)
- [x] `cargo bench --no-run`
- [x] Ручная проверка happy-path: Redis без пароля + `REDIS_URL=redis://127.0.0.1:6379/0` → `Server running`, `GET /health` = 200
- [x] Ручная проверка fail-fast: неверный пароль (`redis://:wrongpass@…`) → выход за ~3 с с `Redis connection failed: превышен таймаут 3 с …`
- [x] Регрессионный тест падает **до** фикса (зависание, `Elapsed(())` за 10 с) и проходит после
- [x] Повторный запуск в том же `DATA_DIR` при недоступном Redis: процесс завершается до создания redb-файлов, `Database already open` не возникает

## Отклонения от плана

- **Порядок инициализации.** Redis-подключение перенесено **до** `PersistentLogStore::open`/`StateMachineStore::open`, а не только до `Raft::new`. Это надёжнее закрывает DoD «повторный запуск больше не натыкается на `Database already open`»: при недоступном Redis redb-файлы вообще не создаются/не открываются. Побочно: `start_raft` теперь не выполняет `create_dir_all(DATA_DIR)` при недоступном Redis (проверено — каталог не создаётся).
- **Прочее — по плану.**

### Предсуществующий сбой кластерного теста

`tests/cluster/test_cluster.rs` падает на шаге «конкурентная запись» (`400 Bad Request`,
`tests/cluster/test_cluster.rs:394`). Сбой **воспроизводится на базовом коммите без правок F1**
(проверено через `git stash`), поэтому регрессией F1 не является. Правки F1 затронули только
тестовую фабрику `AppConfig` (добавлено поле), логика теста не менялась. Требует отдельной
задачи/разбора — в скоуп F1 не входит (см. `AGENTS.md` §5: одна задача — один шаг).

## Что дальше

- ~~Завести отдельную задачу на предсуществующий сбой кластерного теста (конкурентная запись → 400).~~
  Заведена задача **F3** — [`docs/edits/fix/cluster-concurrent-seq/`](../cluster-concurrent-seq/) (причина:
  монотонность `seq` V18 vs параллельные транзакции одного адреса; регрессия `6f75117`).
- При необходимости — отдельная `feature`-задача: ленивое/фоновое переподключение к Redis с `/ready = not_ready` (вне скоупа F1).
