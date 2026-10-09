"""Отчёт о прогоне: агрегация, корректностные инварианты и рендер (R6).

Корректностные инварианты (влияют на код возврата): нет прикладных отказов у
заведомо валидных транзакций и одинаковый размер DAG на всех репликах.
Throughput и перцентили — информационные (без SLA).
"""

import json
from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Dict, List


@dataclass
class Rejection:
    """Отказ с причиной и числом повторений."""

    reason: str
    count: int


@dataclass
class Latency:
    """Перцентили задержки клиента (мс)."""

    p50: float = 0.0
    p95: float = 0.0
    p99: float = 0.0
    mean: float = 0.0
    max: float = 0.0


@dataclass
class Report:
    """Машинно-читаемый отчёт (схема зафиксирована в `spec.md` §R6)."""

    params: dict
    accepted: int
    attempted: int
    rejected: List[Rejection]
    achieved_tps: float
    latency_ms: Latency
    dag_total_per_node: Dict[str, int]
    ok: bool
    errors: List[str] = field(default_factory=list)


def _percentile(sorted_values, percent):
    """Линейно-интерполированный перцентиль по отсортированной выборке."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    rank = (percent / 100.0) * (len(sorted_values) - 1)
    low = int(rank)
    high = min(low + 1, len(sorted_values) - 1)
    frac = rank - low
    return sorted_values[low] * (1 - frac) + sorted_values[high] * frac


def latency_stats(latencies: List[float]) -> Latency:
    """Считает p50/p95/p99, среднее и максимум по выборке задержек."""
    if not latencies:
        return Latency()
    values = sorted(latencies)
    return Latency(
        p50=_percentile(values, 50),
        p95=_percentile(values, 95),
        p99=_percentile(values, 99),
        mean=sum(values) / len(values),
        max=float(values[-1]),
    )


def build_report(cfg, load, dag_total_per_node) -> Report:
    """Агрегирует исходы, считает инварианты и формирует отчёт.

    `dag_total_per_node` — словарь `{url: total}` по всем известным узлам.
    """
    attempted = len(load.outcomes)
    accepted = 0
    transport_failures = 0
    reasons: Counter = Counter()
    latencies: List[float] = []

    for outcome in load.outcomes:
        latencies.append(outcome.latency_ms)
        if outcome.kind == "accepted":
            accepted += 1
        else:
            if outcome.transport_error:
                transport_failures += 1
            reasons[outcome.reason or f"http {outcome.status}"] += 1

    errors = list(load.errors)
    if accepted != attempted:
        errors.append(
            f"есть отказы у заведомо валидных транзакций: принято {accepted} из {attempted}"
        )
    if transport_failures:
        errors.append(f"транспортных ошибок: {transport_failures}")

    totals = list(dag_total_per_node.values())
    if totals and any(total != totals[0] for total in totals):
        errors.append(f"размер DAG различается по репликам: {dag_total_per_node}")

    rejected = [Rejection(reason, count) for reason, count in sorted(reasons.items())]
    achieved_tps = attempted / load.wall_secs if load.wall_secs > 0 else 0.0

    return Report(
        params=_params_dict(cfg),
        accepted=accepted,
        attempted=attempted,
        rejected=rejected,
        achieved_tps=achieved_tps,
        latency_ms=latency_stats(latencies),
        dag_total_per_node=dict(dag_total_per_node),
        ok=not errors,
        errors=errors,
    )


def _params_dict(cfg):
    return {
        "hosts": list(cfg.hosts),
        "tx": cfg.tx_budget,
        "duration_sec": cfg.duration_sec,
        "tps": cfg.tps,
        "concurrency": cfg.concurrency,
        "accounts": cfg.accounts,
        "parents_min": cfg.parents_min,
        "parents_max": cfg.parents_max,
        "seed": cfg.seed,
    }


def report_to_dict(report: Report) -> dict:
    """Преобразует отчёт в словарь для JSON (dataclass → dict)."""
    data = asdict(report)
    return data


def write_json(report: Report, path) -> None:
    """Записывает JSON-отчёт в файл (создаёт каталоги при необходимости)."""
    import os

    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(report_to_dict(report), handle, ensure_ascii=False, indent=2)


def print_report(report: Report) -> None:
    """Печатает человекочитаемый отчёт в stdout."""
    print("=== loadgen (python): отчёт ===")
    params = report.params
    print(
        "параметры: hosts={}, tx={}, duration_sec={}, tps={}, concurrency={}, "
        "accounts={}, parents={}..{}, seed={}".format(
            ",".join(params["hosts"]),
            params["tx"],
            params["duration_sec"],
            params["tps"],
            params["concurrency"],
            params["accounts"],
            params["parents_min"],
            params["parents_max"],
            params["seed"],
        )
    )
    print(
        "принято: {}/{} (достигнуто {:.1} TPS)".format(
            report.accepted, report.attempted, report.achieved_tps
        )
    )
    if report.rejected:
        print("отказы:")
        for item in report.rejected:
            print(f"  - {item.reason} x{item.count}")
    latency = report.latency_ms
    print(
        "задержка мс: p50={:.2f} p95={:.2f} p99={:.2f} mean={:.2f} max={:.2f}".format(
            latency.p50, latency.p95, latency.p99, latency.mean, latency.max
        )
    )
    print("размер DAG по узлам:")
    for node, total in report.dag_total_per_node.items():
        print(f"  - {node}: {total}")
    if report.ok:
        print("ИТОГ: OK — корректностные инварианты выполнены")
    else:
        print("ИТОГ: ОШИБКА — корректностные инварианты нарушены:")
        for error in report.errors:
            print(f"  ! {error}")
