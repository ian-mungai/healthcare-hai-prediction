"""Hand the dbt and quality results of a layer job to its exit push, as counts by closed status and gate; a missing or unreadable file adds nothing.

The build phase leaves dbt's ``run_results.json`` in the run folder and the validate phase leaves one ``result.json`` per quality gate. The counts are the only
values: no model, table or test name is a label (``scripts.metrics_push`` refuses any label outside its closed sets).
"""

from __future__ import annotations

import json
import time
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


def whole_rows(value: Any) -> int | None:
    """A row count that is a whole number of at least zero, or None."""
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def publish_records(candidates: Mapping[str, Any], commit: Mapping[str, Any], now: float) -> None:
    """The tables, the rows, the tables whose snapshot moved, the empty tables and the commit time of one committed publish. A table name or a snapshot ID
    is never a label, because the layers hold far more tables than a closed set can name."""
    try:
        snapshots, tables = commit.get("snapshot_set"), candidates.get("tables")
        if commit.get("state") != "committed" or not isinstance(snapshots, Mapping) or not isinstance(tables, Mapping):
            return
        rows = [whole_rows(tables[name].get("rows")) for name in snapshots if isinstance(tables.get(name), Mapping)]
        moved = sum(
            1
            for name in snapshots
            if isinstance(tables.get(name), Mapping) and tables[name].get("candidate_snapshot_id") != tables[name].get("base_snapshot_id")
        )
        metrics_push.add("hai_publish_tables", len(snapshots))
        metrics_push.add("hai_publish_rows", sum(count for count in rows if count is not None))
        metrics_push.add("hai_publish_tables_changed", moved)
        metrics_push.add("hai_publish_tables_empty", sum(1 for count in rows if count == 0))
        metrics_push.add("hai_publish_commit_timestamp_seconds", now)
    except Exception:
        return


def publish_folder(folder: Path, now: float | None = None) -> None:
    """The counts of the publish whose records the publish steps left in ``folder``; a missing or unreadable record adds nothing."""
    try:
        candidates = json.loads((folder / "candidates.json").read_bytes())
        commit = json.loads((folder / "commit.json").read_bytes())
    except (OSError, ValueError):
        return
    if isinstance(candidates, Mapping) and isinstance(commit, Mapping):
        publish_records(candidates, commit, time.time() if now is None else now)
