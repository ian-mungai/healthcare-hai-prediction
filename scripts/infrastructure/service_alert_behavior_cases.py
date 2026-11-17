"""Cases that show how the service alert rules behave: each rule fires on the series set it is for and stays silent on the sets it must not fire on.

The queries are read from ``monitoring/grafana/provisioning/alerting/rules_service_metrics.yml`` and evaluated by ``promql_subset`` over synthetic series
at one evaluation time. A rule that can never fire, one that always fires and one that misses a boundary each fail a scenario, so a planted defect in a query is
caught by what the query does, not by what its text says. No Docker, AWS or network is used.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from pathlib import Path

from scripts.infrastructure import promql_subset
from scripts.infrastructure.promql_subset import Series

yaml = importlib.import_module("yaml")
RULES = "monitoring/grafana/provisioning/alerting/rules_service_metrics.yml"
NOW = 3600.0
STEP = 30.0
Scenario = tuple[str, list[Series], bool]


def expression(root: Path, uid: str) -> str:
    """The query of one rule, read from the tracked rule file."""
    document = yaml.safe_load((root / RULES).read_text(encoding="utf-8"))
    for group in document["groups"]:
        for rule in group["rules"]:
            if rule["uid"] == uid:
                return str(rule["data"][0]["model"]["expr"])
    raise KeyError(uid)


def samples(first: float, last: float, start: float = NOW - 600, end: float = NOW) -> list[tuple[float, float]]:
    """Counter samples every 30 seconds from ``start`` to ``end``, rising evenly from ``first`` to ``last``."""
    count = int((end - start) / STEP)
    return [(start + index * STEP, first + (last - first) * index / count) for index in range(count + 1)]


def flat(value: float, start: float = NOW - 3000, end: float = NOW) -> list[tuple[float, float]]:
    return samples(value, value, start, end)


def counter(name: str, labels: dict[str, str], first: float, last: float) -> Series:
    return Series(name, labels, samples(first, last))


def errors(name: str, job: str, rise: float, status: str = "500") -> list[Series]:
    return [Series(name, {"job": job, "status": status, "method": "GET"}, samples(1, 1 + rise))]


def server_error_scenarios(name: str, job: str) -> list[Scenario]:
    return [
        ("six more 5xx answers in ten minutes fire", errors(name, job, 6), True),
        ("four more 5xx answers in ten minutes fire", errors(name, job, 4), True),
        ("three more 5xx answers in ten minutes stay silent", errors(name, job, 3), False),
        ("a fast rising 4xx counter stays silent", errors(name, job, 50, "404"), False),
        ("5xx answers of another job stay silent", errors(name, "other", 20), False),
        ("no series stays silent", [], False),
    ]


def heap(used: float, maxima: dict[str, float], extra: list[Series] | None = None) -> list[Series]:
    labels = {"job": "polaris", "area": "heap"}
    found = [Series("jvm_memory_used_bytes", {**labels, "id": "Old"}, flat(used))]
    found += [Series("jvm_memory_max_bytes", {**labels, "id": pool}, flat(value)) for pool, value in maxima.items()]
    return found + (extra or [])


def heap_scenarios() -> list[Scenario]:
    non_heap = [Series("jvm_memory_used_bytes", {"job": "polaris", "area": "nonheap", "id": "Metaspace"}, flat(950))]
    return [
        ("heap at 95% of its maximum fires", heap(950, {"Old": 1000}), True),
        ("heap at 50% of its maximum stays silent", heap(500, {"Old": 1000}), False),
        ("a mix of defined and undefined maxima fires when the heap is high", heap(950, {"Eden": -1, "Old": 1000}), True),
        ("every heap pool without a maximum stays silent", heap(500, {"Eden": -1, "Old": -1}), False),
        ("no maximum series stays silent", heap(500, {}), False),
        ("a high non-heap pool stays silent", heap(100, {"Old": 1000}, non_heap), False),
    ]


BOUNDS = ("0.005", "0.01", "0.025", "0.05", "0.075", "0.1", "0.25", "0.5", "0.75", "1.0", "2.5", "5.0", "7.5", "10.0", "+Inf")


def latency(shares: dict[str, float]) -> list[Series]:
    """Cumulative latency buckets that gained the given counts in ten minutes; a bound that is not named gains what the next lower named bound gained."""
    found, carried = [], 0.0
    for bound in BOUNDS:
        carried = shares.get(bound, carried)
        labels = {"job": "mlflow", "le": bound, "method": "POST", "status": "200", "mlflow_version": "3.17.2"}
        found.append(Series("mlflow_http_request_duration_seconds_bucket", labels, samples(0, carried)))
    return found


def slow_scenarios() -> list[Scenario]:
    return [
        ("one fifth of the requests above seven seconds fires", latency({"2.5": 40, "5.0": 80, "7.5": 90, "10.0": 100, "+Inf": 100}), True),
        ("every request within a tenth of a second stays silent", latency({"0.1": 100}), False),
        ("no traffic stays silent", latency({}), False),
        ("no series stays silent", [], False),
    ]


FAILED = "grafana_alerting_notifications_failed_total"
SELECTOR = f'{FAILED}{{job="grafana"}}'
FIRST_FAILURE_TERM = f" + ((sum({SELECTOR} unless {SELECTOR} offset 15m) > bool 0) or vector(0))"


def failures(points: list[tuple[float, float]], integration: str = "sns") -> Series:
    return Series(FAILED, {"job": "grafana", "org": "1", "integration": integration}, points)


def notification_scenarios() -> list[Scenario]:
    return [
        ("the first failure, whose series starts at one, fires", [failures(flat(1, NOW - 300))], True),
        ("a later failure on a series that is older than the window fires", [failures(flat(3, 0, NOW - 400) + flat(4, NOW - 390))], True),
        ("a failure counted in an older window stays silent", [failures(flat(1, 0))], False),
        ("no series stays silent", [], False),
        ("the first failure of a second integration fires", [failures(flat(2, 0)), failures(flat(1, NOW - 300), "email")], True),
        ("the first failure after a restart fires", [failures(flat(5, 0, 1200) + flat(1, NOW - 300))], True),
        ("a failure of another job stays silent", [Series(FAILED, {"job": "other", "org": "1", "integration": "sns"}, flat(1, NOW - 300))], False),
    ]


def evaluation_scenarios() -> list[Scenario]:
    name = "grafana_alerting_rule_evaluation_failures_total"
    return [
        ("a counter that rises from zero fires", [counter(name, {"job": "grafana", "org": "1"}, 0, 2)], True),
        ("a counter that does not rise stays silent", [counter(name, {"job": "grafana", "org": "1"}, 5, 5)], False),
        ("no series stays silent", [], False),
    ]


def scheduler_scenarios() -> list[Scenario]:
    name = "grafana_alerting_scheduler_behind_seconds"
    return [
        ("two minutes behind fires", [Series(name, {"job": "grafana"}, flat(120))], True),
        ("five seconds behind stays silent", [Series(name, {"job": "grafana"}, flat(5))], False),
        ("no series stays silent", [], False),
    ]


SCENARIOS: dict[str, Callable[[], list[Scenario]]] = {
    "polaris_server_errors": lambda: server_error_scenarios("http_server_requests_seconds_count", "polaris"),
    "polaris_heap_high": heap_scenarios,
    "mlflow_server_errors": lambda: server_error_scenarios("mlflow_http_request_total", "mlflow"),
    "mlflow_slow_requests": slow_scenarios,
    "grafana_rule_evaluation_failing": evaluation_scenarios,
    "grafana_notifications_failing": notification_scenarios,
    "grafana_scheduler_behind": scheduler_scenarios,
}


def judged(query: str, uid: str) -> dict[str, bool]:
    """Each scenario of one rule as a check: true when the query fires, or stays silent, as the scenario says."""
    results: dict[str, bool] = {}
    for name, store, wanted in SCENARIOS[uid]():
        try:
            results[f"{uid}: {name}"] = promql_subset.fires(query, store, NOW) == wanted
        except promql_subset.Unsupported:
            results[f"{uid}: {name}"] = False
    return results


def committed_rule_behaves(uid: str) -> Callable[[Path], dict[str, bool]]:
    def case(root: Path) -> dict[str, bool]:
        return judged(expression(root, uid), uid)

    return case


def mutant_is_caught(change: Callable[[str], str]) -> Callable[[Path], dict[str, bool]]:
    """For every rule, a changed query must fail at least one scenario of that rule."""

    def case(root: Path) -> dict[str, bool]:
        return {f"{uid}: the changed query fails a scenario": not all(judged(change(expression(root, uid)), uid).values()) for uid in SCENARIOS}

    return case


def replaced(old: str, new: str, uid: str) -> Callable[[Path], dict[str, bool]]:
    def case(root: Path) -> dict[str, bool]:
        query = expression(root, uid)
        if old not in query:
            return {f"{uid}: the planted text {old!r} is in the query": False}
        return {f"{uid}: the changed query fails a scenario": not all(judged(query.replace(old, new, 1), uid).values())}

    return case


def evaluator_gives_known_results(root: Path) -> dict[str, bool]:
    """The evaluator reproduces results that follow from the PromQL rules, so the scenarios above rest on a faithful reading."""
    engine = promql_subset.Engine
    one = [Series("c", {"job": "g"}, [(NOW - 10, 4)])]
    rising = [Series("c", {"job": "g"}, [(NOW - 60, 1), (NOW - 30, 3), (NOW, 6)])]
    reset = [Series("c", {"job": "g"}, [(NOW - 60, 5), (NOW - 30, 7), (NOW, 2)])]
    buckets = [Series("b", {"le": le}, [(NOW - 60, 0), (NOW, v)]) for le, v in (("1", 50.0), ("2", 90.0), ("+Inf", 100.0))]
    return {
        "a window with one sample gives no increase": engine(one).run("increase(c[10m])", NOW) == {},
        "an increase is the sample difference": engine(rising).run("increase(c[10m])", NOW) == {frozenset({("job", "g")}): 5.0},
        "a counter reset adds the value after the reset": engine(reset).run("increase(c[10m])", NOW) == {frozenset({("job", "g")}): 4.0},
        "or keeps the left element when both sides have the same labels": engine(rising).run("(sum(c) > bool 99) or vector(7)", NOW) == {frozenset(): 0.0},
        "unless removes the elements that the right side has": engine(one).run("c unless c", NOW) == {},
        "unless keeps an element whose series was absent an hour ago": engine(one).run("sum(c unless c offset 1h)", NOW) == {frozenset(): 4.0},
        "a comparison without bool filters": engine(one).run("c > 9", NOW) == {},
        "a division of a number by an empty vector is empty": engine(one).run("sum(c) / (sum(c > 9))", NOW) == {},
        "the quantile of a bucket set interpolates inside the bucket": engine(buckets).run("histogram_quantile(0.75, sum by (le) (rate(b[10m])))", NOW)
        == {frozenset(): 1.0 + (75 - 50) / (90 - 50)},
        "the quantile above the highest finite bound is that bound": engine(buckets).run("histogram_quantile(0.99, sum by (le) (rate(b[10m])))", NOW)
        == {frozenset(): 2.0},
    }


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "service_alert_evaluator_gives_known_results": evaluator_gives_known_results,
    **{f"service_alert_behaves_{uid}": committed_rule_behaves(uid) for uid in SCENARIOS},
    "service_alert_that_can_never_fire_is_caught": mutant_is_caught(lambda query: f"({query}) * 0"),
    "service_alert_that_always_fires_is_caught": mutant_is_caught(lambda query: f"({query}) + 1"),
    "service_heap_alert_with_a_zero_denominator_is_caught": replaced(
        'sum(jvm_memory_max_bytes{job="polaris",area="heap"} > 0)', 'sum(clamp_min(jvm_memory_max_bytes{job="polaris",area="heap"}, 0))', "polaris_heap_high"
    ),
    "service_notification_alert_without_the_first_failure_is_caught": replaced(FIRST_FAILURE_TERM, "", "grafana_notifications_failing"),
    "service_server_error_alert_with_a_wrong_threshold_is_caught": replaced("> bool 3", "> bool 30", "polaris_server_errors"),
    "service_slow_alert_with_a_wrong_threshold_is_caught": replaced("> bool 5", "> bool 9", "mlflow_slow_requests"),
}
