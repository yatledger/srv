"""Тесты отчёта: агрегация причин, правило `ok`, схема JSON (R6)."""

from loadgen.client import ClientOutcome
from loadgen.report import Report, build_report, latency_stats, report_to_dict
from loadgen.runner import LoadResult


class FakeCfg:
    hosts = ["http://node1"]
    tx_budget = 10
    duration_sec = None
    tps = 100
    concurrency = 2
    accounts = 4
    parents_min = 2
    parents_max = 4
    seed = 1


def outcome(kind, ms, reason=None, transport=False, status=200):
    return ClientOutcome(
        status=status,
        kind=kind,
        reason=reason,
        latency_ms=ms,
        transport_error=transport,
    )


def test_report_ok_when_all_accepted_and_replicas_equal():
    load = LoadResult(
        outcomes=[outcome("accepted", 1.0), outcome("accepted", 2.0)], wall_secs=1.0
    )
    report = build_report(
        FakeCfg(), load, {"http://n1": 5, "http://n2": 5, "http://n3": 5}
    )
    assert report.ok, report.errors
    assert report.accepted == 2
    assert report.attempted == 2
    assert report.achieved_tps == 2.0


def test_report_fails_on_application_rejection():
    load = LoadResult(
        outcomes=[
            outcome("accepted", 1.0),
            outcome("rejected", 1.0, reason="Node already exists", status=400),
        ],
        wall_secs=1.0,
    )
    report = build_report(FakeCfg(), load, {"http://n1": 5, "http://n2": 5})
    assert not report.ok
    assert len(report.rejected) == 1
    assert report.rejected[0].reason == "Node already exists"
    assert report.rejected[0].count == 1


def test_report_aggregates_rejections_by_reason():
    load = LoadResult(
        outcomes=[
            outcome("rejected", 1.0, reason="boom", status=400),
            outcome("rejected", 1.0, reason="boom", status=400),
            outcome("rejected", 1.0, reason="other", status=400),
        ],
        wall_secs=1.0,
    )
    report = build_report(FakeCfg(), load, {"http://n1": 5})
    assert {r.reason: r.count for r in report.rejected} == {"boom": 2, "other": 1}


def test_report_fails_on_replica_divergence():
    load = LoadResult(outcomes=[outcome("accepted", 1.0)], wall_secs=1.0)
    report = build_report(FakeCfg(), load, {"http://n1": 5, "http://n2": 6})
    assert not report.ok
    assert any("различается" in error for error in report.errors)


def test_report_fails_on_transport_error():
    load = LoadResult(
        outcomes=[outcome("error", 1.0, reason="down", transport=True, status=0)],
        wall_secs=1.0,
    )
    report = build_report(FakeCfg(), load, {"http://n1": 5})
    assert not report.ok
    assert any("транспорт" in error for error in report.errors)


def test_latency_percentiles_are_sane():
    stats = latency_stats([float(x) for x in range(1, 101)])
    assert stats.max == 100.0
    assert 49.0 <= stats.p50 <= 51.0
    assert 94.0 <= stats.p95 <= 96.0
    assert 98.0 <= stats.p99 <= 100.0


def test_json_schema_fields_present():
    load = LoadResult(outcomes=[outcome("accepted", 1.0)], wall_secs=1.0)
    report = build_report(FakeCfg(), load, {"http://n1": 5})
    data = report_to_dict(report)
    for field in (
        "params",
        "accepted",
        "attempted",
        "rejected",
        "achieved_tps",
        "latency_ms",
        "dag_total_per_node",
        "ok",
        "errors",
    ):
        assert field in data
    assert set(data["latency_ms"]) == {"p50", "p95", "p99", "mean", "max"}
