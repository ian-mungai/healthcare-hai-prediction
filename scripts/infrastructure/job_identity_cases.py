"""Cases for the grouping identity of the job metrics (review of 84b3722..7fc9030, finding 6).

The six model sets run as the features family and the train, promote and serve tasks run once per model or HAI type, so each instance pushes under its own
grouping key: ``model_set`` (m1 to m6) or ``job_type`` (a HAI type). A query that aggregates job series by ``job`` and ``phase`` alone would merge the instances
again: a later success of one would hide the failure of another. Every alert expression and dashboard expression that groups or matches by ``job`` and ``phase``
must keep both keys. A planted copy of an old expression must be found, so the case can fail.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.infrastructure import monitoring_files

ALERTS = "monitoring/grafana/provisioning/alerting"
DASHBOARDS = "monitoring/grafana/dashboards"
GROUPING = re.compile(r"\b(?:by|on|ignoring|without)\s*\(([^)]*)\)")


def expressions(root: Path) -> list[tuple[str, str]]:
    """Every PromQL expression of the alert rules and the dashboards, with where it is."""
    found: list[tuple[str, str]] = []
    for path in sorted((root / ALERTS).glob("rules_*.yml")):
        document = monitoring_files.yaml.safe_load(path.read_text(encoding="utf-8"))
        for group in document.get("groups", []):
            for rule in group.get("rules", []):
                for step in rule.get("data", []):
                    expr = step.get("model", {}).get("expr")
                    if isinstance(expr, str):
                        found.append((f"{path.name} {rule.get('title')}", expr))
    for path in sorted((root / DASHBOARDS).glob("*.json")):
        document = json.loads(path.read_text(encoding="utf-8"))
        for panel in document.get("panels", []):
            for target in panel.get("targets", []):
                if isinstance(target.get("expr"), str):
                    found.append((f"{path.name} {panel.get('title')}", target["expr"]))
    return found


def drops_the_model_set(expr: str) -> bool:
    """True when the expression groups or matches by job and phase and one of those lists lacks ``model_set`` or ``job_type``."""
    for names in GROUPING.findall(expr):
        words = {word.strip() for word in names.split(",")}
        if {"job", "phase"} <= words and not {"model_set", "job_type"} <= words:
            return True
    return False


def every_job_and_phase_grouping_keeps_the_model_set(root: Path) -> dict[str, bool]:
    found = expressions(root)
    broken = [where for where, expr in found if drops_the_model_set(expr)]
    jobs = [where for where, expr in found if "hai_job_" in expr]
    return {
        "the alert rules and dashboards were read": len(found) > 40 and bool(jobs),
        "no expression groups or matches by job and phase without the model set": broken == [],
    }


def the_old_expressions_are_found_by_the_check(root: Path) -> dict[str, bool]:
    old: list[str] = [
        'max by (job, phase) (hai_job_finish_timestamp_seconds{outcome="succeeded"})',
        "a > bool on (job, phase) (b)",
    ]
    fixed: list[Any] = [
        'max by (job, phase, model_set, job_type) (hai_job_finish_timestamp_seconds{outcome="succeeded"})',
        "sum by (job, outcome) (changes(hai_job_finish_timestamp_seconds[24h]))",
    ]
    return {
        "a grouping by job and phase alone is found": all(drops_the_model_set(expr) for expr in old),
        "a grouping that keeps the model set, or has no phase, is not": not any(drops_the_model_set(expr) for expr in fixed),
    }


NEWEST = {
    "hai_spark_tasks_failed": "hai_spark_poll_timestamp_seconds",
    "hai_spark_tasks_completed": "hai_spark_poll_timestamp_seconds",
    "hai_spark_jobs_completed": "hai_spark_poll_timestamp_seconds",
    "hai_spark_jobs_failed": "hai_spark_poll_timestamp_seconds",
    "hai_dbt_tests": "hai_job_finish_timestamp_seconds",
    "hai_dbt_models": "hai_job_finish_timestamp_seconds",
    "hai_quality_checks_failed": "hai_job_finish_timestamp_seconds",
    "hai_quality_checks_run": "hai_job_finish_timestamp_seconds",
    "hai_quality_checks_warned": "hai_job_finish_timestamp_seconds",
}


def reads_every_run(expr: str) -> bool:
    """True when the expression reads a result of a finished run (not the running group) without selecting the newest run by its timestamp."""
    for metric, stamp in NEWEST.items():
        if re.search(rf"\b{metric}\b", expr) and 'outcome="running"' not in expr and not re.search(rf"max by \([^)]*\) \({stamp}", expr):
            return True
    return False


def failure_and_latest_run_expressions_read_only_the_newest_run(root: Path) -> dict[str, bool]:
    """A failed run's group stays in the Pushgateway until a later run of the same job replaces it, and the groups of the outcomes are separate, so a
    failure followed by a success leaves the old failure in place (review round 2, finding 8). Each rule and panel that reads these results keeps only the
    group with the newest poll or finish time of its job, phase and instance."""
    found = expressions(root)
    broken = [where for where, expr in found if reads_every_run(expr)]
    reads = [where for where, expr in found if any(metric in expr for metric in NEWEST)]
    return {
        "the rules and panels that read these results were found": len(reads) >= 12,
        "every one of them selects the newest run": broken == [],
        "an expression with no selection is found by the check": reads_every_run('max(hai_spark_tasks_failed{outcome!="running"})'),
        "an expression with the selection is not": not reads_every_run(
            "x and on (job) (hai_spark_poll_timestamp_seconds == on (job) group_left () max by (job) (hai_spark_poll_timestamp_seconds)) or "
            "hai_spark_tasks_failed"
        ),
    }


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "jobs_failure_and_latest_run_expressions_read_only_the_newest_run": failure_and_latest_run_expressions_read_only_the_newest_run,
    "jobs_every_job_and_phase_grouping_keeps_the_model_set": every_job_and_phase_grouping_keeps_the_model_set,
    "jobs_the_old_expressions_are_found_by_the_check": the_old_expressions_are_found_by_the_check,
}
