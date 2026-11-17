"""Cases for the promtool fixtures of the service alert rules and for the PromQL shape check.

Native ``promtool test rules`` is the authority for what a rule query does. The fixtures under ``monitoring/promtool/`` hold the input series and the
alerts that each rule must and must not raise, and the lead runs them in the pinned Prometheus image as a heavy run. These cases need no Docker.
They show that the fixtures stay tied to the tracked rules (the same query, every rule, a firing and a silent evaluation each and the named boundary
scenarios) and that a query with an invalid function call or argument is reported by the shape check. No Python code here claims what a query returns.
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.infrastructure import monitoring_files, promql_subset
from scripts.infrastructure.monitoring_cases import edit, reported

yaml = importlib.import_module("yaml")
RULES = "monitoring/grafana/provisioning/alerting/rules_service_metrics.yml"
PROMTOOL_RULES = monitoring_files.PROMTOOL_RULES
PROMTOOL_TESTS = monitoring_files.PROMTOOL_TESTS
FAILED = 'grafana_alerting_notifications_failed_total{job="grafana"}'
FIRST_FAILURE_TERM = f" + ((sum({FAILED} unless {FAILED} offset 15m) > bool 0) or vector(0))"
RESTART_TERM = f' + ((sum(({FAILED} > 0) and on (instance) (time() - process_start_time_seconds{{job="grafana"}} < 900)) > bool 0) or vector(0))'
Plant = Callable[[Path], None]


def plant_tests(change: Callable[[dict[str, Any]], None]) -> Plant:
    """Change the parsed promtool test file of the scratch tree and write it back."""

    def plant(tree: Path) -> None:
        path = tree / PROMTOOL_TESTS
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        change(document)
        path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")

    return plant


def committed_fixtures_are_tied_to_the_rules(root: Path) -> dict[str, bool]:
    """The tracked fixtures pass the monitoring file check, which compares them with the Grafana rules."""
    problems = [problem for problem in monitoring_files.check(root) if "promtool" in problem]
    rules = yaml.safe_load((root / RULES).read_text(encoding="utf-8"))["groups"][0]["rules"]
    fixture_rules = yaml.safe_load((root / PROMTOOL_RULES).read_text(encoding="utf-8"))["groups"][0]["rules"]
    tests = yaml.safe_load((root / PROMTOOL_TESTS).read_text(encoding="utf-8"))["tests"]
    names = {str(test.get("name")) for test in tests}
    return {
        "the monitoring file check reports no promtool problem": problems == [],
        "every Grafana rule has its promtool rule": {rule["title"] for rule in rules} == {rule["alert"] for rule in fixture_rules},
        "every promtool rule query is the Grafana query wrapped in a comparison with zero": all(
            fixture["expr"] == f"({rule['data'][0]['model']['expr']}) > 0" for rule, fixture in zip(rules, fixture_rules, strict=True)
        ),
        "the named boundary scenarios exist": set(monitoring_files.PROMTOOL_SCENARIOS) <= names,
        "the test file names the rule file next to it": yaml.safe_load((root / PROMTOOL_TESTS).read_text(encoding="utf-8"))["rule_files"]
        == ["service_alerts.yml"],
    }


def promtool_rule_query_differs_from_the_grafana_rule(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, PROMTOOL_RULES, "> bool 60", "> bool 600"), "differs from the Grafana rule")


def grafana_rule_changes_without_its_promtool_rule(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, RULES, "> bool 60", "> bool 90"), "differs from the Grafana rule")


def promtool_rule_is_missing(root: Path) -> dict[str, bool]:
    def plant(tree: Path) -> None:
        path = tree / PROMTOOL_RULES
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        document["groups"][0]["rules"] = document["groups"][0]["rules"][:-1]
        path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")

    return reported(root, plant, "has no promtool rule")


def promtool_scenario_is_dropped(root: Path) -> dict[str, bool]:
    def drop(document: dict[str, Any]) -> None:
        document["tests"] = [test for test in document["tests"] if test.get("name") != "three_observed_5xx"]

    return reported(root, plant_tests(drop), "lacks the scenario three_observed_5xx")


def promtool_rule_never_stays_silent(root: Path) -> dict[str, bool]:
    def drop(document: dict[str, Any]) -> None:
        for test in document["tests"]:
            for evaluation in test.get("alert_rule_test", []):
                if evaluation["alertname"] == "GrafanaSchedulerBehind" and not evaluation.get("exp_alerts"):
                    evaluation["exp_alerts"] = [{"exp_labels": {"severity": "warning", "job": "grafana"}}]

    return reported(root, plant_tests(drop), "GrafanaSchedulerBehind has no promtool evaluation that stays silent")


def promtool_rule_never_fires(root: Path) -> dict[str, bool]:
    def drop(document: dict[str, Any]) -> None:
        for test in document["tests"]:
            for evaluation in test.get("alert_rule_test", []):
                if evaluation["alertname"] == "GrafanaSchedulerBehind":
                    evaluation.pop("exp_alerts", None)

    return reported(root, plant_tests(drop), "GrafanaSchedulerBehind has no promtool evaluation that fires")


def promtool_test_names_another_rule_file(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, PROMTOOL_TESTS, "service_alerts.yml", "other_alerts.yml"), "rule_files must name service_alerts.yml")


def notification_rule_has_an_invalid_function_call(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, RULES, f"increase({FAILED}[15m])", f"increase({FAILED}[15m], 99)"), "not a valid PromQL query")


def notification_rule_without_the_first_appearance_term(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, RULES, FIRST_FAILURE_TERM, ""), "also compare it with")


def notification_rule_without_the_restart_term(root: Path) -> dict[str, bool]:
    return reported(root, lambda tree: edit(tree, RULES, RESTART_TERM, ""), "process_start_time_seconds")


def known_list_lacks_the_process_start_time(root: Path) -> dict[str, bool]:
    return reported(
        root, lambda tree: edit(tree, "monitoring/known_metrics.json", '"process_start_time_seconds"', '"process_start_seconds"'), "process_start_time_seconds"
    )


def tracked_queries(root: Path) -> list[str]:
    found: list[str] = []
    for path in sorted((root / "monitoring/grafana/dashboards").glob("*.json")):
        found += [
            str(target["expr"]) for panel in json.loads(path.read_text(encoding="utf-8"))["panels"] for target in panel.get("targets", []) if target.get("expr")
        ]
    for path in sorted((root / "monitoring/grafana/provisioning/alerting").glob("rules_*.yml")):
        for group in yaml.safe_load(path.read_text(encoding="utf-8"))["groups"]:
            found += [str(step["model"]["expr"]) for rule in group["rules"] for step in rule["data"] if step["model"].get("expr")]
    return found


def rejected(text: str, reason: str) -> bool:
    """True when the shape check refuses the text and its message holds the reason."""
    try:
        promql_subset.validate(text)
    except promql_subset.Unsupported as error:
        return reason in str(error)
    return False


def valid(text: str) -> bool:
    try:
        promql_subset.validate(text)
    except promql_subset.Unsupported:
        return False
    return True


def accepted(text: str, kind: str) -> bool:
    try:
        return promql_subset.validate(text) == kind
    except promql_subset.Unsupported:
        return False


def shape_check_reads_the_tracked_queries(root: Path) -> dict[str, bool]:
    found = tracked_queries(root)
    return {
        "every tracked dashboard and rule query has a valid shape": all(valid(query) for query in found),
        "the tracked queries are many": len(found) > 150,
    }


def shape_check_accepts_the_forms_the_rules_use(root: Path) -> dict[str, bool]:
    return {
        "a comparison with bool over an aggregation of an increase": accepted('sum(increase(x{a="b"}[15m])) > bool 0', "vector"),
        "unless with an offset": accepted("x unless x offset 15m", "vector"),
        "a quantile over a sum by le": accepted("histogram_quantile(0.95, sum by (le) (rate(b[5m])))", "vector"),
        "topk over a grouped sum": accepted("topk(10, sum by (a) (x))", "vector"),
        "vector matching with on": accepted("(x > 0) and on (instance) (time() - y < 900)", "vector"),
        "a vector filled with or": accepted("(sum(x) > bool 0) or vector(0)", "vector"),
        "a number": accepted("0.95", "scalar"),
    }


def shape_check_rejects_invalid_calls(root: Path) -> dict[str, bool]:
    return {
        "increase with an extra argument": rejected("increase(x[15m], 99)", "takes 1 argument"),
        "increase of an instant vector": rejected("increase(x)", "needs a range vector"),
        "sum with two arguments": rejected("sum(x, y)", "takes 1 argument"),
        "topk with one argument": rejected("topk(x)", "takes 2 arguments"),
        "a quantile whose first argument is a vector": rejected("histogram_quantile(x, y)", "needs a number"),
        "a range vector in an operator": rejected("x[5m] + 1", "range vector"),
        "a function that the check does not know": rejected('label_replace(x, "a", "b", "c", "d")', "is not supported"),
        "a comparison of two numbers without bool": rejected("1 > 2", "needs bool"),
        "a set operator with a number": rejected("rate(x[5m]) or 1", "set operators need vectors"),
        "an aggregation without its argument": rejected("sum by (a)", "expected"),
        "a query that ends in a range vector": rejected("x[5m]", "range vector"),
        "a subquery": rejected("x[5m:1m]", "expected"),
    }


def shape_module_evaluates_nothing(root: Path) -> dict[str, bool]:
    """The module checks syntax and shape only, so no Python code in it claims what Prometheus returns."""
    return {f"the module has no {name}": not hasattr(promql_subset, name) for name in ("Engine", "Series", "fires", "evaluate", "increase", "quantile")}


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "service_alert_fixtures_are_tied_to_the_rules": committed_fixtures_are_tied_to_the_rules,
    "service_alert_promtool_rule_query_differs_from_the_grafana_rule": promtool_rule_query_differs_from_the_grafana_rule,
    "service_alert_grafana_rule_changes_without_its_promtool_rule": grafana_rule_changes_without_its_promtool_rule,
    "service_alert_promtool_rule_is_missing": promtool_rule_is_missing,
    "service_alert_promtool_scenario_is_dropped": promtool_scenario_is_dropped,
    "service_alert_promtool_rule_never_stays_silent": promtool_rule_never_stays_silent,
    "service_alert_promtool_rule_never_fires": promtool_rule_never_fires,
    "service_alert_promtool_test_names_another_rule_file": promtool_test_names_another_rule_file,
    "service_alert_notification_rule_has_an_invalid_function_call": notification_rule_has_an_invalid_function_call,
    "service_alert_notification_rule_without_the_first_appearance_term": notification_rule_without_the_first_appearance_term,
    "service_alert_notification_rule_without_the_restart_term": notification_rule_without_the_restart_term,
    "service_alert_known_list_lacks_the_process_start_time": known_list_lacks_the_process_start_time,
    "service_shape_check_reads_the_tracked_queries": shape_check_reads_the_tracked_queries,
    "service_shape_check_accepts_the_forms_the_rules_use": shape_check_accepts_the_forms_the_rules_use,
    "service_shape_check_rejects_invalid_calls": shape_check_rejects_invalid_calls,
    "service_shape_module_evaluates_nothing": shape_module_evaluates_nothing,
}
