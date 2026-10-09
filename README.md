# dagdb

Распределённый узел хранения DAG-графа транзакций поверх консенсуса Raft.

Лицензия: **MIT OR Apache-2.0** (см. [`LICENSE-MIT`](LICENSE-MIT) и [`LICENSE-APACHE`](LICENSE-APACHE)).

Каждая транзакция — узел графа, ссылающийся на родителей (хэши других транзакций).
Узлы реплицируются через Raft; «утяжелённые» узлы архивируются в Redis и удаляются из
оперативного DAG. Транзакция подписывается ключом ed25519, а её хэш покрывает всё
содержимое, включая `func`.

> Статус: ранний MVP / экспериментальный прототип. Внутренний проект.

## Стек

- **Rust** (edition 2024)
- **openraft 0.10** — консенсус Raft (git databendlabs)
- **axum 0.7** + **tokio** — HTTP-сервер и асинхронный runtime
- **redb** — персистентное хранение Raft-лога и state machine (чистый Rust)
- **redis 0.32** — архив подтверждённых (удалённых из DAG) узлов
- **blake3 / ed25519-dalek / base58** — хэширование и подписи
- **serde / serde_json**, **rayon**, **clap**, **dotenvy**, **tracing**

## Архитектура

```
src/
├── main.rs          точка входа: логи, конфиг, запуск Raft/HTTP, фоновой очистки
├── config.rs        AppConfig: чтение CLI + env (dotenvy), профили, таймауты, секреты
├── lib.rs           TypeConfig, тип Tx, start_raft (сборка узла)
├── server.rs        HTTP-хендлеры; публичный/внутренний API, наблюдаемость, rate limit
├── auth.rs          middleware проверки кластерного токена (x-internal-token)
├── web.rs           внутренний HTTP-клиент (пересылка на лидера, с токеном)
├── processor.rs     фоновая очистка «тяжёлых» узлов (ведёт лидер)
├── cleanup.rs       выбор кандидатов/тяжёлых узлов, архивация в Redis
├── metrics.rs       реестр метрик Prometheus (O2)
├── ratelimit.rs     token-bucket для публичного API (O3)
├── shutdown.rs      graceful shutdown (O5)
├── audit.rs         аудит-события add/remove/membership (O7)
├── graph/
│   ├── dag.rs       DAG: узлы, родители/дети, реестр added, TxVar и валидация
│   └── weights.rs   глубины и веса узлов (BFS, rayon)
└── raft/
    ├── command.rs   команды Request::{Add, Remove} и Response
    ├── log.rs       PersistentLogStore (redb) + in-memory LogStore (тесты)
    ├── store.rs     state machine: валидация, персистентность, версия схемы
    ├── db.rs        обёртка над redb (таблицы meta/logs)
    ├── network.rs   сетевой слой Raft (append/vote/snapshot)
    ├── router.rs    HTTP-транспорт для Raft RPC
    └── api.rs       хендлеры /raft/* и /mng/*
benches/
└── graph_bench.rs   criterion-бенчмарки графа (O9)
scripts/
├── backup.sh        бэкап DATA_DIR + Redis (O5)
└── restore.sh       восстановление из бэкапа (O5)
```

**Поток записи.** Клиент шлёт подписанную транзакцию на `POST /`. Узел проверяет
родителей, `func`, структуру `var`, вычисляет канонический хэш, проверяет подпись и
пересылает запрос лидеру (если сам не лидер). Лидер пишет команду в Raft; state machine
детерминированно валидирует её на каждой реплике и применяет к DAG.

### Инварианты

- Детерминированный state machine: одинаковая команда → одинаковое состояние.
- `func` входит в подписываемый и хэшируемый контент.
- Хэш узла уникален; повторная вставка запрещена.
- Удалённые узлы попадают в реестр `added` и переживают снапшот/рестарт.
- Публичный API отделён от внутреннего (Raft/mng).

## Сборка

```bash
cargo build --release
# либо
./build.sh
```

Бинарник: `target/release/dagdb`.

## Конфигурация

Скопируйте шаблон и заполните значения (файл `.env` в `.gitignore`):

```bash
cp .env.example .env
```

Приоритет: аргумент CLI → переменная окружения → значение по умолчанию.
Секреты (`REDIS_URL`, `INTERNAL_API_TOKEN`) обязательны и задаются только из окружения.

| Переменная | CLI | По умолчанию | Назначение |
|---|---|---|---|
| `NODE_ID` | `--id` | `1` | Уникальный ID узла в кластере |
| `APP_PROFILE` | `--profile` | `dev` | Профиль окружения: `dev`/`stage`/`prod` |
| `LOG_FORMAT` | `--log-format` | `text` | Формат логов: `text`/`json` |
| `ADVERTISE_ADDR` | `--advertise-addr` | — | Адрес узла, публикуемый кластеру (`host:port`) |
| `BIND_ADDR` | `--bind-addr` | `0.0.0.0:<HTTP_PORT>` | Адрес прослушивания HTTP |
| `HTTP_PORT` | `--port` | `21001` | Порт HTTP (если не задан `BIND_ADDR`) |
| `DATA_DIR` | `--data-dir` | `./data` | Каталог персистентных данных |
| `REDIS_URL` | `--redis-url` | — (обязательно) | Строка подключения к Redis |
| `INTERNAL_API_TOKEN` | `--internal-api-token` | — (обязательно) | Кластерный токен внутреннего API |
| `CLUSTER_NODES` | `--cluster-nodes` | — | Список узлов `id=addr,...` (для запуска) |
| `RUST_LOG` | — | `info` | Уровень логов |
| `HTTP_TIMEOUT_SECS` | — | `10` | Таймаут HTTP-запросов приложения |
| `HTTP_CONNECT_TIMEOUT_SECS` | — | `3` | Таймаут установки HTTP-соединения |
| `RAFT_HTTP_TIMEOUT_SECS` | — | `30` | Таймаут Raft HTTP-запросов |
| `RAFT_CONNECT_TIMEOUT_SECS` | — | `10` | Таймаут установки Raft HTTP-соединения |
| `PROCESSOR_INTERVAL_MS` | — | `250` | Интервал фоновой очистки «тяжёлых» узлов |
| `WEIGHT_THRESHOLD` | — | `0.5` | Порог веса (после насыщения) для удаления узла |
| `CLEANUP_BATCH_SIZE` | — | `100` | Размер батча кандидатов на очистку за цикл |
| `PUBLIC_RATE_LIMIT_PER_SEC` | — | `50` | Лимит запросов/с на публичный `POST /` (0 — выключено) |
| `PUBLIC_RATE_LIMIT_BURST` | — | `100` | Всплеск для rate limiter |
| `MAX_REQUEST_BYTES` | — | `1048576` | Advisory-лимит размера тела запроса |

В `DATA_DIR` создаются `raft-log.redb` (Raft-лог, vote, committed) и
`state-machine.redb` (DAG, `added`, membership, снапшот).

## Запуск кластера (4 узла + Redis)

### Через Docker Compose (рекомендуется)

```bash
docker compose up --build
```

Поднимаются Redis и 4 узла (`node1..node4`, порты `21001..21004`).

### Локально

1. Запустите Redis:

   ```bash
   docker run -d --name dagdb-redis -p 6379:6379 redis:7-alpine
   ```

2. Запустите 4 узла (каждый — в своём терминале) с разными `NODE_ID`/портами:

   ```bash
   NODE_ID=1 HTTP_PORT=21001 DATA_DIR=./data/node1 ./target/release/dagdb
   NODE_ID=2 HTTP_PORT=21002 DATA_DIR=./data/node2 ./target/release/dagdb
   NODE_ID=3 HTTP_PORT=21003 DATA_DIR=./data/node3 ./target/release/dagdb
   NODE_ID=4 HTTP_PORT=21004 DATA_DIR=./data/node4 ./target/release/dagdb
   ```

   (Строка `REDIS_URL` и `INTERNAL_API_TOKEN` берутся из `.env`.)

3. Инициализируйте кластер и загрузите генезис (см. примеры ниже). Готовый сценарий —
   [`test.sh`](test.sh): поднимает 4 узла, инициализирует кластер, грузит `genesis.json`
   и делает проверки.

## API

### Публичный (без токена)

| Метод | Путь | Описание |
|---|---|---|
| `POST` | `/` | Принять подписанную транзакцию |
| `GET` | `/pool?limit=&offset=` | Узлы-кандидаты (пагинация, по умолчанию `limit=100`, максимум `1000`) |
| `GET` | `/full?limit=&offset=` | Граф с глубинами (пагинация, те же лимиты) |
| `GET` | `/metrics` | Метрики Prometheus |
| `GET` | `/health` | Liveness |
| `GET` | `/ready` | Readiness (лидер выбран, Redis достижим) |
| `GET` | `/openapi.json` | OpenAPI-спецификация публичного API |
| `GET` | `/docs` | Swagger UI |

Полное описание схемы `Tx`/`var`/`func` и правил валидации — в
[`docs/API.md`](docs/API.md).

### Внутренний (требует заголовок `x-internal-token`)

| Метод | Путь | Описание |
|---|---|---|
| `POST` | `/add` | Запись транзакции (внутрикластерная пересылка) |
| `POST` | `/remove_heavy_nodes` | Архивировать и удалить «тяжёлые» узлы |
| `POST` | `/load-genesis` | Загрузить `genesis.json` |
| `POST` | `/raft/vote`, `/raft/append`, `/raft/snapshot` | Raft RPC |
| `POST` | `/mng/init` | Инициализация кластера |
| `POST` | `/mng/add-learner` | Добавить learner-узел |
| `POST` | `/mng/change-membership` | Изменить состав кластера |
| `POST` | `/mng/metrics` | Метрики Raft |
| `POST` | `/mng/snapshot` | Принудительно построить снапшот (перед бэкапом) |

### Примеры

Инициализация кластера из 4 узлов:

```bash
curl -X POST -H "Content-Type: application/json" \
  -H "x-internal-token: $INTERNAL_API_TOKEN" \
  -d '[[1,"127.0.0.1:21001"],[2,"127.0.0.1:21002"],[3,"127.0.0.1:21003"],[4,"127.0.0.1:21004"]]' \
  http://127.0.0.1:21001/mng/init
```

Загрузка генезиса:

```bash
curl -X POST -H "Content-Type: application/json" \
  -H "x-internal-token: $INTERNAL_API_TOKEN" -d '[]' \
  http://127.0.0.1:21001/load-genesis
```

Публичный граф:

```bash
curl http://127.0.0.1:21001/full
```

Транзакция (`POST /`) имеет вид:

```json
{
  "tx": {
    "prnts": ["<hash-родителя-1>", "<hash-родителя-2>"],
    "addr": "<ed25519 pubkey в base58>",
    "seq": 0,
    "var": { "ca": "0", "to": "<base58>", "val": 100, "msg": "optional" }
  },
  "sign": "<ed25519-подпись в hex, 64 байта>",
  "func": "transferToken"
}
```

Хэш — `blake3` от канонической JSON-сериализации `tx` вместе с `func`
(ключи рекурсивно отсортированы). Подпись — ed25519 над байтами хэша.
Внутренние вызовы (`/add`) не требуют публичной подписи повторно: узел проверяет
переданный `hash` на совпадение с вычисленным.

## Безопасность транспорта

- Публичный API (`/`, `/pool`, `/full`) доступен без аутентификации.
- Все внутренние эндпоинты (`/add`, `/remove_heavy_nodes`, `/load-genesis`,
  `/raft/*`, `/mng/*`) требуют кластерный токен `INTERNAL_API_TOKEN` в заголовке
  `x-internal-token`. Токен передаётся и внутрикластерными клиентами
  (Raft RPC, пересылка на лидера, очистка), поэтому кластер работает как единое
  доверенное пространство.
- Адрес прослушивания задаётся `BIND_ADDR`/`HTTP_PORT`; хардкода `0.0.0.0` в коде нет.
- **TLS между узлами не реализован.** Внутрикластерный трафик идёт по HTTP и
  защищён общим токеном. Шифрование канала (rustls/mTLS) — задача внешнего уровня
  (service mesh/VPN) или отдельного этапа развития; при развёртывании в недоверенной
  сети выносите узлы в приватный сегмент.

## Наблюдаемость

- `GET /metrics` — метрики в формате Prometheus text exposition: состояние Raft
  (term, индексы, очередь применения, лидер, узлы), размер DAG и реестра `added`,
  порог веса, счётчики очистки, транзакций и HTTP (с гистограммой длительности).
  InfluxDB2/VictoriaMetrics/Telegraf подключаются внешним сборщиком
  (`inputs.prometheus` или `remote_write`), изменений в узле не требуется.
- `GET /health` — liveness; `GET /ready` — readiness (есть лидер, Redis достижим).
- Логи структурированы: `LOG_FORMAT=json` включает однострочный JSON, каждый
  HTTP-запрос получает корреляционный `x-request-id` (возвращается в ответе).
- Аудит значимых операций (add/remove/membership) пишется в лог с
  `target="audit"` и указанием источника (`api`/`processor`/`genesis`/`management`).

## Graceful shutdown и бэкап (O5)

По `Ctrl-C`/`SIGTERM` узел перестаёт принимать запросы
(`with_graceful_shutdown`), затем финализирует Raft (`Raft::shutdown`) — состояние
сохраняется в `DATA_DIR` (redb) и переживает рестарт.

Бэкап (свежий снапшот + копия `DATA_DIR` + дамп Redis):

```bash
NODE_URL=http://127.0.0.1:21001 DATA_DIR=./data/node1 \
  INTERNAL_API_TOKEN=$INTERNAL_API_TOKEN REDIS_URL=$REDIS_URL \
  ./scripts/backup.sh          # создаст ./backups/<timestamp>/
```

Восстановление (узел должен быть остановлен):

```bash
BACKUP_SNAPSHOT=./backups/<timestamp> DATA_DIR=./data/node1 ./scripts/restore.sh
```

## Бенчмарки (O9)

```bash
cargo bench
```

Бенчмарки `compute_descendants_with_depth_and_weight` (500 узлов),
`get_nodes_by_depth` (1000 узлов) и `add_node_with_parents` (throughput записи).
CI проверяет компиляцию бенчмарков (`cargo bench --no-run`), не запуская их.

## Тесты и качество

```bash
cargo fmt --all -- --check
cargo clippy --all-targets
cargo test --all
```

CI (`.github/workflows/ci.yml`) выполняет fmt + clippy `-D warnings` + build + test.

## Известные ограничения

- Внутрикластерный трафик идёт без TLS; защита — общий кластерный токен.
- Публичная регистрация узлов/membership не предусмотрена: управление только
  внутренним API с токеном.
- Rate limiter и метрики — в памяти узла (без внешнего хранилища); при
  необходимости горизонтального сбора используйте `/metrics` и внешний сборщик.

Полный перечень находок и план работ — в [`docs/AUDIT.md`](docs/AUDIT.md) и
[`docs/tz/README.md`](docs/tz/README.md). Профили конфигурации: `dev` (по
умолчанию), `stage` (токен ≥16 символов), `prod` (токен ≥32 символов и явный
`ADVERTISE_ADDR`); при старте конфигурация валидируется и все проблемы выводятся
одним сообщением.
