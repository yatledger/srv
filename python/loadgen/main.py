"""Оркестрация прогона нагрузчика и код возврата (R1, R9).

Внешний клиент: кластер не поднимает, работает против уже запущенного по
публичному HTTP. Код возврата: `0` — корректностные инварианты выполнены,
`1` — нарушение инварианта/ошибка параметров/недоступные узлы.
"""

import asyncio
import sys

from .client import DagdbClient
from .config import Config
from .generator import Generator
from .report import build_report, print_report, write_json
from .runner import run_load


def _load_keypairs(accounts):
    """Возвращает `accounts` пар ключ/адрес, проверяя их валидность."""
    import keys as keys_module

    from .crypto import address_from_sk

    pairs = keys_module.load_keys()
    if not pairs:
        raise ValueError("набор ключей пуст")
    selected = [pairs[i % len(pairs)] for i in range(accounts)]
    for sk, pk in selected:
        if address_from_sk(sk) != pk:
            raise ValueError(f"ключ {pk} не соответствует секрету")
    return selected


async def _check_hosts(client, hosts, timeout=10.0):
    """Ждёт живости хотя бы одного узла; возвращает список живых."""
    import asyncio as _asyncio

    deadline = _asyncio.get_event_loop().time() + timeout
    live = []
    while _asyncio.get_event_loop().time() < deadline:
        live = [host for host in hosts if await client.health(host)]
        if live:
            return live
        await _asyncio.sleep(0.2)
    return live


async def run(cfg: Config) -> int:
    """Выполняет прогон и возвращает код возврата."""
    keypairs = _load_keypairs(cfg.accounts)
    generator = Generator(
        keypairs, parents_min=cfg.parents_min, parents_max=cfg.parents_max
    )

    async with DagdbClient(
        cfg.hosts,
        timeout=cfg.timeout,
        retry=cfg.retry,
        internal_token=cfg.internal_token,
        verbose=cfg.verbose,
    ) as client:
        live = await _check_hosts(client, cfg.hosts)
        if not live:
            print("loadgen: ни один узел не отвечает на /health", file=sys.stderr)
            return 1

        if cfg.internal_token:
            leader = await client.find_leader()
            if leader:
                print(f"loadgen: лидер — {leader}")

        print(
            f"loadgen: запуск нагрузки на {len(live)} узел(ов), "
            f"аккаунтов={cfg.accounts}, tx={cfg.tx_budget}, duration={cfg.duration_sec}"
        )
        load = await run_load(client, generator, cfg)

        # Итоговые размеры DAG по каждому известному узлу — проверка консистентности.
        dag_total_per_node = {}
        for host in cfg.hosts:
            dag_total_per_node[host] = await client.full_total(host)

    report = build_report(cfg, load, dag_total_per_node)
    print_report(report)
    if cfg.json_out:
        write_json(report, cfg.json_out)
        print(f"JSON-отчёт записан: {cfg.json_out}")
    return 0 if report.ok else 1


def main(argv=None) -> int:
    """Точка входа CLI. Возвращает код возврата процесса."""
    try:
        cfg = Config.parse(argv)
    except ValueError as exc:
        print(f"loadgen: ошибка параметров: {exc}", file=sys.stderr)
        return 1
    try:
        return asyncio.run(run(cfg))
    except KeyboardInterrupt:
        print("\nloadgen: прервано пользователем", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 — верхнеуровневый отчёт об ошибке запуска
        print(f"loadgen: фатальная ошибка: {exc}", file=sys.stderr)
        return 1
