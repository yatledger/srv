# ТЗ №2. Важный уровень 🟠

> Группа: **edits/audit/second-pass** · Тип: аудит · Блок задач `V*` · Статус: ✅ **V11–V18 выполнены**.
> Ссылки на `src/...:NN` — состояние на коммите `a1f4bf0`.
>
> Сопутствующие документы: [`audit.md`](audit.md), [`01-critical.md`](01-critical.md),
> [`03-quality.md`](03-quality.md), [`tracker.md`](tracker.md).

## 0. Статус реализации

Все задачи блока закрыты:

- **V11 ✅** — `Dockerfile` копирует `benches/` и `docs/`; `docker compose build` собирается.
- **V12 ✅** — из `docker-compose.yml` убраны дефолтные секреты: `${REDIS_PASSWORD:?}` и
  `${INTERNAL_API_TOKEN:?}` обязаны приходить из окружения; файл помечен local-only.
- **V13 ✅** — выполнена в рамках K1: введён `ApplyResult`, все точки записи проверяют
  `response.data.as_result()`.
- **V14 ✅** — `Node.parents/children` → `BTreeSet`, `nodes`/`added`/`last_seq` → `BTreeMap`;
  поле `Node.time` (wall-clock) и `NodeTime`/`get_time` удалены. Снапшот детерминирован.
- **V15 ✅** — родитель, оставшийся только в реестре `added`, **отклоняется** (и в
  `validate_add`, и в ранней `validate_against_state`); `Node.parents` больше не бывает неполным.
- **V16 ✅** — `load_genesis` требует `current_leader == self` (иначе `503`): genesis-узлы
  не подписаны и не могут идти через `/add`, поэтому запись безопасно ограничена лидером.
- **V17 ✅** — `full_snapshot` учитывает токен отмены через `tokio::select!` и возвращает
  `StreamingError::Closed`.
- **V18 ✅** — в `Dag` добавлена карта `last_seq: addr → последний seq`; `validate_add`
  отклоняет `seq <= last` (детерминированно); пустой `addr` bootstrap-генезиса освобождён.
  `SCHEMA_VERSION` поднята 2 → 3; старое поле читается как пустая карта.

## 1. Цель

Восстановить сломанную сборку/цепочку развёртывания и убрать серьёзные логические недочёты.

## 2. Задачи

| ID | Задача | Находка | Приоритет |
|----|--------|---------|-----------|
| V11 | Починить сборку Docker-образа (`benches/`, `docs/`) | S12 | P1 |
| V12 | Убрать дефолтные секреты из `docker-compose.yml` | D13 | P1 |
| V13 | Перевести внутренние записи/ответы на типизированный результат | C43 (совместно с K1) | P2 |
| V14 | Недетерминизм порядка и `Node.time` в DAG | C36, C37 | P2 |
| V15 | Семантика родителей из реестра `added` | C38 | P2 |
| V16 | Писать через лидера в `load_genesis` (не ронять genesis) | S16 | P2 |
| V17 | Учитывать отмену при передаче снапшота | C40 | P2 |
| V18 | Монотонность `seq` по адресу (анти-replay) | C45 | P1 |

## 3. Технические требования

- **V11 (S12).** `Dockerfile` не копирует `benches/` и `docs/`; при этом `Cargo.toml:36-38` объявляет
  `[[bench]] name = "graph_bench"`, а `src/server.rs:504` использует
  `include_str!("../docs/api/openapi.json")`. `cargo build --release` в образе падает. Добавить
  `COPY docs ./docs` и `COPY benches ./benches` (или `COPY . .` + `.dockerignore`). Проверить
  `docker compose build`.
- **V12 (D13).** Заменить `CHANGE_ME*` на обязательные переменные окружения (`.env`/secrets),
  пометить compose как local-only. Учесть, что `Profile::Prod` считает токен длины ≥32 валидным
  независимо от значения.
- **V13 (C43).** `Response.value: Option<String>` со строками `"Ok"`/`"Error: ..."` — причина C34.
  Ввести `enum ApplyResult { Ok, Rejected(String) }` (или `Result`-обёртку) и обновить
  `raft/store.rs`, `raft/command.rs`, вызывающие места, тесты.
- **V14 (C36, C37).** Итерация по `children: HashSet` и сериализация `parents`/`children` как
  `HashSet` недетерминированы (порядок `RandomState`), а `Node.time = SystemTime::now()`
  различается на репликах. Перейти на `BTreeSet`/сортировку и детерминированное время (или убрать
  поле). Проверить инвариант «одинаковый вход → одинаковый снапшот».
- **V15 (C38).** `Node.parents` содержит только родителей, присутствовавших в DAG; родители из
  `added` теряются, хотя `data.prnts` их хранит и SM их допускает. Зафиксировать правило: либо
  включать `added`-родителей в `parents`, либо отклонять add с родителем только из `added`.
- **V16 (S16).** `load_genesis` вызывает `app.raft.client_write` на любом узле (`src/server.rs:630`).
  На follower это вернёт `ForwardToLeader` и транзакция потеряется. Либо требовать лидера, либо
  пересылать через `add_handler`-путь (как `add_tx`).
- **V17 (C40).** `NetworkFactory::full_snapshot` игнорирует `_cancel: ReplicationClosed`; учитывать
  отмену (openraft ожидает её обработку).
- **V18 (C45).** Нет проверки монотонности `seq` по `addr`. После вытеснения старого узла из `added`
  (`MAX_ADDED_ENTRIES`, `src/graph/dag.rs:49`) транзакция с прежним `seq` и валидной подписью снова
  проходит проверки (`dag.contains_node`/`is_node_added` — ложь) и применяется повторно (replay).
  Хранить последний `seq` на адрес в состоянии state machine и отклонять `seq <= last`; проверка
  детерминирована и живёт в `validate_add` (`src/raft/store.rs`). Требует расширения состояния `Dag`
  (карта `addr → last_seq`), значит — поднять `SCHEMA_VERSION` и миграцию (см. O6).

## 4. Критерии готовности

- `docker compose build` проходит, граф `benches/` и `docs/` доступны в build context.
- В compose нет секретов, пригодных «как есть» для реального стенда.
- `apply` возвращает типизированный результат; загрузка генезиса на follower не теряет записи.
- Два прогона одного и того же набора команд дают идентичные снапшоты.

## 5. Порядок реализации

V11 → V12 → V16 → V18 → V13 → V14 → V15 → V17.
