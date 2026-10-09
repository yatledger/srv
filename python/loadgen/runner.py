"""Драйвер нагрузки: конкурентные sender-задачи, пейсинг, сбор статусов (R5).

Аккаунты делятся на непрерывные блоки по одной задаче, поэтому `seq` каждого
аккаунта монотонен без межзадачной синхронизации, а запросы в пределах аккаунта
уходят в порядке выпуска. Целевой TPS распределяется пейсингом на каждую задачу.
"""

import asyncio
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .client import ClientOutcome, DagdbClient
from .generator import Generator, choose_parents, derive_rng


@dataclass
class LoadResult:
    """Итог прогона нагрузки."""

    outcomes: List[ClientOutcome] = field(default_factory=list)
    wall_secs: float = 0.0
    errors: List[str] = field(default_factory=list)


def account_block(worker_id: int, workers: int, accounts: int) -> Tuple[int, int]:
    """Непрерывный блок аккаунтов `[start, start+len)` для задачи `worker_id`.

    Блоки не пересекаются, поэтому `seq` аккаунта монотонен без синхронизации.
    Лишние задачи (сверх числа аккаунтов) получают пустой блок.
    """
    effective = max(1, min(workers, accounts))
    if worker_id >= effective:
        return (0, 0)
    base = accounts // effective
    extra = accounts % effective
    start = worker_id * base + min(worker_id, extra)
    length = base + (1 if worker_id < extra else 0)
    return (start, length)


class _Counter:
    """Счётчик выпущенных транзакций (asyncio одном потоке — без атомиков)."""

    def __init__(self):
        self.value = 0

    def take(self, limit: Optional[int]) -> bool:
        """Резервирует слот; `False`, если бюджет исчерпан."""
        if limit is not None and self.value >= limit:
            return False
        self.value += 1
        return True


async def run_load(client: DagdbClient, generator: Generator, cfg) -> LoadResult:
    """Запускает нагрузку и собирает исходы всех запросов."""
    workers = max(1, min(cfg.concurrency, generator.account_count()))
    counter = _Counter()
    budget = cfg.tx_budget
    loop = asyncio.get_event_loop()
    deadline = (
        loop.time() + cfg.duration_sec if cfg.duration_sec is not None else None
    )

    started = loop.time()
    tasks = [
        asyncio.create_task(
            _worker(worker_id, workers, client, generator, cfg, budget, deadline, counter)
        )
        for worker_id in range(workers)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    wall_secs = loop.time() - started

    outcomes: List[ClientOutcome] = []
    errors: List[str] = []
    for result in results:
        if isinstance(result, BaseException):
            errors.append(f"sender task failed: {result!r}")
        else:
            outcomes.extend(result)

    return LoadResult(outcomes=outcomes, wall_secs=wall_secs, errors=errors)


async def _worker(worker_id, workers, client, generator, cfg, budget, deadline, counter):
    """Тело sender-задачи: выпуск и отправка транзакций своего блока аккаунтов."""
    rng = derive_rng(cfg.seed, worker_id)
    account_start, account_len = account_block(
        worker_id, workers, generator.account_count()
    )
    account_step = 0
    outcomes: List[ClientOutcome] = []

    pool = await client.pool()
    since_pool_refresh = 0

    per_worker_tps = (
        0.0 if cfg.tps == 0 else max(1.0, cfg.tps / workers)
    )
    interval = 1.0 / per_worker_tps if per_worker_tps > 0 else None
    loop = asyncio.get_event_loop()
    next_slot = loop.time()

    while True:
        if not counter.take(budget):
            break
        if deadline is not None and loop.time() >= deadline:
            break

        if interval is not None:
            now = loop.time()
            if next_slot > now:
                await asyncio.sleep(next_slot - now)
            next_slot += interval

        since_pool_refresh += 1
        if (since_pool_refresh >= 128 or len(pool) < 2) and account_len > 0:
            fresh = await client.pool()
            if fresh:
                pool = fresh
            since_pool_refresh = 0

        k = generator.effective_parent_count(len(pool), rng)
        if k is None:
            await asyncio.sleep(0.02)
            continue

        index = account_start + (account_step % account_len)
        account_step += 1
        parents = choose_parents(pool, k, rng)
        account = generator.accounts[index]
        async with account.lock:
            body = generator.build(index, parents, rng)
            outcome = await client.send_tx(body)
        outcomes.append(outcome)

    return outcomes
