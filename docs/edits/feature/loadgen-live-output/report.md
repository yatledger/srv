# Отчёт о реализации `feature/loadgen-live-output` (N03)

> Группа: **edits/feature/loadgen-live-output** · Тип: **feature** · Статус: ✅ **выполнено**.
> Сопутствующие документы: [`spec.md`](spec.md) · [`plan.md`](plan.md) · [`tracker.md`](tracker.md).

## Кто и когда

- Исполнитель: агентская сессия.
- Дата: 2026-10-09.
- Коммит-база: `af6aa49` (документы задачи — `1dd86ff`).
- Ветка: `main`.

## Что сделано

Реализованы N03.1–N03.3:

- **N03.1 — CLI и пейсинг.** Добавлены флаги `--show-tx` (`bool`) и `--sleep <SECS>`
  (`Option<f64>` с `value_parser`, отклоняющим отрицательные/`NaN`/`inf`). Детект демо-режима —
  `std::env::args_os().len() == 1`; резолвинг вынесен в чистую `LoadgenConfig::resolve(demo)`.
  В `runner.rs` появилась модель `Sleep::{None, Fixed, Jitter}`, пауза разыгрывается **отдельным**
  ГПСЧ (`sleep_seed`), чтобы не сдвигать детерминированный поток параметров транзакций; добавлен
  `Prng::range_f64`.
- **N03.2 — рендер.** В `report.rs` вынесены чистые функции `Palette` (ANSI-авто-детект по TTY и
  `NO_COLOR`), `format_request_header`, `format_request_lines`, `format_response_line`,
  `render_frame`, `shorten`. В `runner.rs` блок печатается под общим `Mutex` (атомарность при
  `concurrency > 1`); печатается только финальная попытка (служебные ретраи — в `--verbose`).
- **N03.3 — docs/тесты.** Обновлён `README.md` (флаги + подраздел «Демонстрационный режим»),
  статусы задачи. Добавлены юнит-тесты на рендер, палитру, резолвинг демо-дефолтов, `--sleep`
  и `range_f64`.
- **Доработка после ревью.** В демо-режиме (запуск без аргументов) строка «параметры» в итоговом
  отчёте не печатается: `print_report` принимает `show_params`, а `run` передаёт `!cfg.is_demo()`.
  Причина — в демо фактические параметры задаются демо-дефолтами (`tps=0`, `concurrency=1`,
  `duration_sec=60`), а `Report.params` строится из «сырого» `LoadgenConfig`, поэтому строка
  выводила бы вводящие в заблуждение значения. JSON-схема `Report` не менялась.

## Изменённые файлы

| Файл | Действие |
|---|---|
| `src/bin/loadgen/main.rs` | флаги, демо-детект, `resolve`, `parse_sleep_secs`, тесты |
| `src/bin/loadgen/runner.rs` | `Sleep`, `RunConfig.{show_tx,sleep}`, `sleep_between`, `sleep_seed`, атомарный вывод, `render_frame` |
| `src/bin/loadgen/report.rs` | `Palette`, форматтеры блока, тесты |
| `src/bin/loadgen/prng.rs` | `Prng::range_f64`, тест |
| `README.md` | флаги `--show-tx`/`--sleep`, раздел «Демонстрационный режим» |
| `docs/edits/feature/README.md` | статус N03 → выполнено |
| `docs/edits/feature/loadgen-live-output/{spec,plan,tracker,report}.md` | статусы/журнал/отчёт |

`Cargo.toml`, `.env.example`, `docs/api/`, CI, `scripts/loadgen.sh` — не менялись.

## Пройденные проверки

| Проверка | Результат |
|---|---|
| `cargo fmt --all -- --check` | ✅ OK |
| `cargo clippy --all-targets -- -D warnings` | ✅ OK |
| `cargo test --all` | ✅ 24 unit + 5 api + 1 cluster — все зелёные (Redis доступен) |
| Ручной демо-прогон (без параметров, `timeout 9`) | ✅ печатает цветные блоки, идёт медленно |
| `--show-tx --sleep 0 --tx 6 --concurrency 2` | ✅ 6 блоков, все `200 OK`, отчёт OK |
| `--tx 4 --json-out …` | ✅ 0 блоков, JSON-отчёт записан |
| Демо без параметров (полный прогон, ~60 с) | ✅ строка «параметры» не печатается |
| `--tx 3` (явный флаг) | ✅ строка «параметры» печатается |

## Отклонения от плана

- Планировалась и отдельная проверка «демо ограничено `--duration-sec 60`»; в ручном прогоне
  использовался `timeout 9` для краткости. Демо-резолвинг (`tps=0`, `concurrency=1`,
  `duration_sec=Some(60)`, `Jitter 0.1..2.5`) покрыт юнит-тестом
  `demo_resolves_to_live_slow_single_worker`.
- `Palette::new` оставлен только под `#[cfg(test)]` (в рантайме используется `detect`), чтобы не
  держать неиспользуемый публичный конструктор.
- Прочих отклонений от [`plan.md`](plan.md) нет.

## Что дальше

- Задача закрыта. При желании владельца — отдельной задачей: аналогичный «живой» режим для
  Python-клиента `N02`, интерактивный TUI/графики (осознанно вне скоупа N03).
