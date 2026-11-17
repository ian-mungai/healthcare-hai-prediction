"""Hand the dbt and quality results of a layer job to its exit push, as counts by closed status and gate; a missing or unreadable file adds nothing.

The build phase leaves dbt's ``run_results.json`` in the run folder and the validate phase leaves one ``result.json`` per quality gate. The counts are the only
values: no model, table or test name is a label (``scripts.metrics_push`` refuses any label outside its closed sets).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from scripts import metrics_push

STATUSES = ("success", "error", "skipped", "pass", "fail", "warn")


def dbt_results(document: Mapping[str, Any]) -> None:
    """Count the models and the tests of one dbt run by status."""
    try:
        counts: dict[tuple[str, str], int] = {}
        for result in document.get("results", []):
            kind = str(result.get("unique_id", "")).split(".", 1)[0]
            status = str(result.get("status", ""))
            if kind in ("model", "test") and status in STATUSES:
                counts[(kind, status)] = counts.get((kind, status), 0) + 1
        for (kind, status), count in counts.items():
            metrics_push.add("hai_dbt_models" if kind == "model" else "hai_dbt_tests", count, status=status)
    except Exception:
        return


def dbt_file(path: Path) -> None:
    try:
        dbt_results(json.loads(path.read_bytes()))
    except (OSError, ValueError):
        return


def quality_result(kind: str, result: Mapping[str, Any]) -> None:
    """The checks a quality gate ran, failed and warned about, from the ``counts`` of its result."""
    try:
        counts = result.get("counts")
        if not isinstance(counts, Mapping) or kind not in ("silver", "gold", "cross", "features"):
            return
        run = sum(int(value) for value in counts.values() if isinstance(value, int | float))
        metrics_push.add("hai_quality_checks_run", run, kind=kind)
        metrics_push.add("hai_quality_checks_failed", int(counts.get("fail", 0)), kind=kind)
        metrics_push.add("hai_quality_checks_warned", int(counts.get("warn", 0)), kind=kind)
    except Exception:
        return


def quality_file(kind: str, path: Path) -> None:
    try:
        quality_result(kind, json.loads(path.read_bytes()))
    except (OSError, ValueError):
        return
