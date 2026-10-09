# Отчёт о реализации: fix/raft-router-unreachable-logs (F5)

> Группа: **edits/fix/raft-router-unreachable-logs** · Тип: **fix** · Статус: **реализовано, проверки пройдены**.
> Ссылки: [`defect.md`](defect.md), [`plan.md`](plan.md), [`tracker.md`](tracker.md).

## Кто / когда

- Исполнитель: агентская сессия.
- Коммит-база: `6917917` (ветка `main`).
- Дата: 2026-10-09.

## Изменённые файлы

| Файл | Что изменено |
|---|---|
| `src/raft/router.rs` | Транспортная ошибка Raft RPC: `error!("Failed to send request to …")` → `debug!("raft rpc to [{}] {} unreachable: {}", to, url, e)`. Комментарий: недоступность пира — штатна для Raft. |
| `docs/edits/fix/raft-router-unreachable-logs/*` | Документация задачи (эта папка). |

## Пройденные проверки

- [x] `cargo fmt --all` / `cargo fmt --all -- --check`
- [x] `cargo clippy --all-targets -- -D warnings`
- [x] `cargo test --all` — `28` unit loadgen + `111` lib + `5` api + `1` cluster, все зелёные (Redis доступен)
- [x] `loadgen --tx 30 --concurrency 4`: stderr пуст, `0` строк `Failed to send request`, `0` `ERROR`, итог `OK`
- [x] Три прогона `RUST_LOG=debug --tx 200 --concurrency 8`: `0` строк `unreachable`, `0` `ERROR`
      (на остановке лидер успевал финализироваться до всплеска, а сообщение теперь на `debug`)
- [x] `RUST_LOG=warn`/`debug` по-прежнему управляют выводом (приоритет env сохранён)

## Отклонения от плана

- Прочие ветки роутера (HTTP не-2xx, пустой ответ, ошибка парсинга) оставлены на прежнем уровне
  (`error!`): это не «недоступность пира», а нарушения контракта RPC. Согласуется с §1 `plan.md`.
- Отдельного автотеста на уровень лога не добавлено (проверка уровня хрупка) — покрыто ручными
  прогонами, как и предполагал план.

## Что дальше

- Дополнительных изменений не требуется.
