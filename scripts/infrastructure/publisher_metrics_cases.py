"""Cases for the publisher metrics that the publisher task hands to its exit push after it processed the queue.

``scripts.acquisition.publisher_metrics.run_record`` turns the run record that ``capture_mode.execute`` returns into counts of units by state, the files
fetched, the files still missing (labelled with the run kind, so a retry that leaves files missing is visible), the bytes downloaded, one last success time
per data source and the unfinished units per data source. A run ID, a snapshot ID, a unit key and free text never become labels. The cases feed the function
the record shape the code produces and check that the publisher task calls it. No AWS service is contacted.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts import metrics_push
from scripts.acquisition import publisher_metrics

FINISHED = "2026-11-17T19:00:00+00:00"
FINISHED_SECONDS = 1794942000.0


def pending() -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    return {(sample.name, tuple(sorted(sample.labels.items()))): sample.value for sample in metrics_push.drain()}


def key(name: str, **labels: str) -> tuple[str, tuple[tuple[str, str], ...]]:
    return name, tuple(sorted(labels.items()))


def item(source: str, snapshot: str, reason: str | None = None) -> dict[str, Any]:
    return {"unit_key": f"k/{snapshot}", "source_id": source, "snapshot_id": snapshot, "reason": reason}


def record(**units: Any) -> dict[str, Any]:
    empty: dict[str, Any] = {name: [] for name in (*publisher_metrics.UNIT_STATES, "paused_sources")}
    empty.update(units)
    return {
        "kind": "ingestion_run",
        "finished_at_utc": FINISHED,
        "counters": {"http_requests": 14, "bytes_downloaded": 5120, "units_attempted": 9, "objects_created": 11, "unchanged": 1, "changed": 1},
        "units": empty,
    }


def a_run_record_becomes_counts_files_and_bytes(root: Path) -> dict[str, bool]:
    metrics_push.drain()
    units = {
        "completed": [item("BLS", "s1"), item("BLS", "s2")],
        "unchanged": [item("CA", "s3")],
        "changed": [item("CA", "s4")],
        "failed": [item("AHA", "s5", "http_500")],
        "refused": [item("AHA", "s6", "intent_forged")],
        "skipped_complete": ["k/a", "k/b", "k/c"],
        "skipped_recent_stale": ["k/d"],
        "paused_not_attempted": ["s7", "s8"],
        "manual_not_attempted": ["s9"],
        "keyed_not_attempted": [],
    }
    publisher_metrics.run_record(record(**units), "retry", 0)
    found = pending()
    return {
        "each unit state is a count": found.get(key("hai_publisher_units", state="completed")) == 2
        and found.get(key("hai_publisher_units", state="skipped_complete")) == 3
        and found.get(key("hai_publisher_units", state="paused_not_attempted")) == 2
        and found.get(key("hai_publisher_units", state="keyed_not_attempted")) == 0,
        "files fetched are the completed, unchanged and changed units": found.get(key("hai_publisher_files_fetched")) == 4,
        "files missing are the failed, refused and paused units, labelled with the run kind": found.get(key("hai_publisher_files_missing", run_kind="retry"))
        == 4,
        "the bytes downloaded, the requests and the objects created come from the counters": found.get(key("hai_publisher_bytes_downloaded")) == 5120
        and found.get(key("hai_publisher_http_requests")) == 14
        and found.get(key("hai_publisher_objects_created")) == 11,
        "the evidence problems are a number": found.get(key("hai_publisher_evidence_problems")) == 0,
        "no snapshot ID, unit key or run ID is a label": all(
            value not in {f"s{n}" for n in range(1, 10)} and "k/" not in value for _, labels in found for _, value in labels
        ),
    }


def a_source_has_a_last_success_or_its_unfinished_units(root: Path) -> dict[str, bool]:
    metrics_push.drain()
    units = {
        "completed": [item("BLS", "s1"), item("MIXED", "s2")],
        "unchanged": [item("CA", "s3")],
        "failed": [item("MIXED", "s4", "http_500")],
        "refused": [item("AHA", "s5", "intent_forged")],
    }
    publisher_metrics.run_record(record(**units), "monthly", 0)
    found = pending()
    return {
        "a source whose units all finished has the finish time of the run as its last success": found.get(
            key("hai_publisher_source_last_success_timestamp_seconds", data_source="BLS")
        )
        == FINISHED_SECONDS
        and found.get(key("hai_publisher_source_last_success_timestamp_seconds", data_source="CA")) == FINISHED_SECONDS,
        "a source with a failed or refused unit has no last success": key("hai_publisher_source_last_success_timestamp_seconds", data_source="MIXED")
        not in found
        and key("hai_publisher_source_last_success_timestamp_seconds", data_source="AHA") not in found,
        "its unfinished units are counted per source": found.get(key("hai_publisher_source_units_unfinished", data_source="MIXED")) == 1
        and found.get(key("hai_publisher_source_units_unfinished", data_source="AHA")) == 1,
        "a finished source has no unfinished series": key("hai_publisher_source_units_unfinished", data_source="BLS") not in found,
    }


def anything_unexpected_adds_nothing_and_raises_nothing(root: Path) -> dict[str, bool]:
    metrics_push.drain()
    publisher_metrics.run_record(record(completed=[item("bad source!", "s1"), item("BLS", "s2")]), "weird", 0)
    first = pending()
    publisher_metrics.run_record({"units": "broken"}, "monthly", 0)
    publisher_metrics.run_record({}, "monthly", 0)
    publisher_metrics.run_record(record(), "bootstrap", -3)
    publisher_metrics.run_record({**record(), "finished_at_utc": "not a time"}, "bootstrap", -3)
    second = pending()
    return {
        "a source name outside the closed form adds nothing": not any(dict(labels).get("data_source") == "bad source!" for _, labels in first),
        "an unknown run kind adds no missing-files series": not any(name == "hai_publisher_files_missing" for name, _ in first),
        "a broken record raises nothing": True,
        "a bad finish time adds no last success": not any(name == "hai_publisher_source_last_success_timestamp_seconds" for name, _ in second),
        "a negative problem count adds nothing": key("hai_publisher_evidence_problems") not in second,
    }


def every_publisher_metric_is_in_the_push_catalog_with_closed_labels(root: Path) -> dict[str, bool]:
    names = (
        "hai_publisher_units",
        "hai_publisher_files_fetched",
        "hai_publisher_files_missing",
        "hai_publisher_bytes_downloaded",
        "hai_publisher_http_requests",
        "hai_publisher_objects_created",
        "hai_publisher_evidence_problems",
        "hai_publisher_source_last_success_timestamp_seconds",
        "hai_publisher_source_units_unfinished",
    )
    labels = {label for name in names for label in metrics_push.CATALOG[name][1]}
    return {
        "all nine are in the catalog": all(name in metrics_push.CATALOG for name in names),
        "the only label names are state, run_kind and data_source": labels <= {"state", "run_kind", "data_source"},
        "the publisher job family has its run phase": metrics_push.JOB_PHASES["publisher"] == ("run",),
    }


def the_publisher_task_hands_over_its_run_record(root: Path) -> dict[str, bool]:
    source = (root / "scripts/acquisition/publisher_run.py").read_text(encoding="utf-8")
    return {
        "the task calls the function after it processed the queue": "publisher_metrics.run_record(" in source,
        "the call comes before the evidence check can stop the run": source.find("publisher_metrics.run_record(")
        < source.find('raise PublisherError(f"The evidence check'),
    }


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "publisher_a_run_record_becomes_counts_files_and_bytes": a_run_record_becomes_counts_files_and_bytes,
    "publisher_a_source_has_a_last_success_or_its_unfinished_units": a_source_has_a_last_success_or_its_unfinished_units,
    "publisher_anything_unexpected_adds_nothing_and_raises_nothing": anything_unexpected_adds_nothing_and_raises_nothing,
    "publisher_every_publisher_metric_is_in_the_push_catalog_with_closed_labels": every_publisher_metric_is_in_the_push_catalog_with_closed_labels,
    "publisher_the_publisher_task_hands_over_its_run_record": the_publisher_task_hands_over_its_run_record,
}
