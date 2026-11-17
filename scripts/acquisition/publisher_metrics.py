"""Hand the run record of a publisher task to its exit push, as counts and times with closed labels.

``capture_mode.execute`` returns the run record: the counters of the run and the units by state. This module reads only that record. The labels are the unit
state, the run kind (bootstrap, monthly or retry) and the data source ID from the source registry. A run ID, a snapshot ID, a unit key and free text never
become labels. The record holds no count of stored bytes: raw files are stored as they arrive, so the bytes downloaded are the bytes that enter the capture
root. A last success exists for a data source only when the run fetched at least one of its units and none of its units failed or was refused. Anything
unexpected adds nothing and nothing here raises, so a push problem never fails the job.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from scripts import metrics_push

UNIT_STATES = (
    "completed",
    "unchanged",
    "changed",
    "failed",
    "refused",
    "skipped_complete",
    "skipped_recent_stale",
    "paused_not_attempted",
    "manual_not_attempted",
    "keyed_not_attempted",
)
FETCHED = ("completed", "unchanged", "changed")
UNFINISHED = ("failed", "refused")
MISSING = ("failed", "refused", "paused_not_attempted", "keyed_not_attempted")
RUN_KINDS = ("bootstrap", "monthly", "retry")
DATA_SOURCE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,39}")
COUNTER_METRICS = {
    "bytes_downloaded": "hai_publisher_bytes_downloaded",
    "http_requests": "hai_publisher_http_requests",
    "objects_created": "hai_publisher_objects_created",
}


def sources_of(units: Mapping[str, Any], states: tuple[str, ...]) -> dict[str, int]:
    """The units of the named states per data source ID, for the entries that name a source in the closed form."""
    counts: dict[str, int] = {}
    for state in states:
        for entry in units.get(state, []):
            source = entry.get("source_id") if isinstance(entry, Mapping) else None
            if isinstance(source, str) and DATA_SOURCE.fullmatch(source):
                counts[source] = counts.get(source, 0) + 1
    return counts


def finish_time(record: Mapping[str, Any]) -> float | None:
    """The finish time of the run as Unix seconds, or None when the record has no readable time."""
    try:
        return datetime.fromisoformat(str(record["finished_at_utc"])).timestamp()
    except (KeyError, ValueError, OverflowError, OSError):
        return None


def run_record(record: Mapping[str, Any], kind: str, evidence_problems: int) -> None:
    """The unit counts, the files fetched and missing, the counters, the last success per data source and the evidence problems of one publisher run."""
    try:
        units = record.get("units")
        if not isinstance(units, Mapping):
            return
        sizes = {state: len(units[state]) for state in UNIT_STATES if isinstance(units.get(state), list)}
        for state, size in sizes.items():
            metrics_push.add("hai_publisher_units", size, state=state)
        metrics_push.add("hai_publisher_files_fetched", sum(sizes.get(state, 0) for state in FETCHED))
        if kind in RUN_KINDS:
            metrics_push.add("hai_publisher_files_missing", sum(sizes.get(state, 0) for state in MISSING), run_kind=kind)
        counters = record.get("counters")
        for name, metric in COUNTER_METRICS.items():
            value = counters.get(name) if isinstance(counters, Mapping) else None
            if isinstance(value, int | float) and not isinstance(value, bool) and math.isfinite(value) and value >= 0:
                metrics_push.add(metric, float(value))
        if isinstance(evidence_problems, int) and not isinstance(evidence_problems, bool) and evidence_problems >= 0:
            metrics_push.add("hai_publisher_evidence_problems", evidence_problems)
        unfinished = sources_of(units, UNFINISHED)
        for source, count in unfinished.items():
            metrics_push.add("hai_publisher_source_units_unfinished", count, data_source=source)
        finished = finish_time(record)
        if finished is not None:
            for source in sources_of(units, FETCHED):
                if source not in unfinished:
                    metrics_push.add("hai_publisher_source_last_success_timestamp_seconds", finished, data_source=source)
    except Exception:
        return
