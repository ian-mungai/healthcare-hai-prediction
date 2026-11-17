"""Cases for the publish counts that a layer job hands to its exit push after the commit of its publish.

``scripts.lakehouse.layer_metrics.publish_folder`` reads the two records that the publish steps leave in the publish folder: ``candidates.json`` (rows,
candidate snapshot and base snapshot of each table) and ``commit.json`` (the state and the snapshot set). The layers hold 137, 71 and 47 published tables,
which is no small closed set, so a table name or a snapshot ID is never a label. The result is counts per layer job: tables committed, tables whose snapshot
moved, tables with no rows, the rows in all and the time of the commit. The cases write the record shape the publish code writes and check that the publish
phase calls the function after its acceptance. No AWS service is contacted.
"""

from __future__ import annotations

import json
import tempfile
from collections.abc import Callable
from pathlib import Path

from scripts import metrics_push
from scripts.lakehouse import layer_metrics

NOW = 1794942000.0


def pending() -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    return {(sample.name, tuple(sorted(sample.labels.items()))): sample.value for sample in metrics_push.drain()}


def key(name: str, **labels: str) -> tuple[str, tuple[tuple[str, str], ...]]:
    return name, tuple(sorted(labels.items()))


def write(folder: Path, candidates: object, commit: object) -> None:
    (folder / "candidates.json").write_text(json.dumps(candidates), encoding="utf-8")
    (folder / "commit.json").write_text(json.dumps(commit), encoding="utf-8")


def candidates() -> dict[str, object]:
    return {
        "tables": {
            "table_a": {"candidate_snapshot_id": 105, "rows": 1200, "base_snapshot_id": 101},
            "table_b": {"candidate_snapshot_id": 77, "rows": 0, "base_snapshot_id": 77},
            "table_c": {"candidate_snapshot_id": 310, "rows": 30, "base_snapshot_id": None},
        }
    }


def a_committed_publish_becomes_counts_and_a_time(root: Path) -> dict[str, bool]:
    metrics_push.drain()
    with tempfile.TemporaryDirectory(prefix="layer_publish_") as scratch:
        folder = Path(scratch)
        write(folder, candidates(), {"state": "committed", "snapshot_set": {"table_a": 105, "table_b": 77, "table_c": 310}})
        layer_metrics.publish_folder(folder, now=NOW)
    found = pending()
    return {
        "the tables of the snapshot set are counted": found.get(key("hai_publish_tables")) == 3,
        "the rows are summed": found.get(key("hai_publish_rows")) == 1230,
        "tables whose snapshot moved are counted, a new table included": found.get(key("hai_publish_tables_changed")) == 2,
        "tables with no rows are counted": found.get(key("hai_publish_tables_empty")) == 1,
        "the time of the commit is a Unix time": found.get(key("hai_publish_commit_timestamp_seconds")) == NOW,
        "no table name or snapshot ID is a label": not any(labels for _, labels in found),
    }


def anything_but_a_committed_publish_adds_nothing(root: Path) -> dict[str, bool]:
    metrics_push.drain()
    with tempfile.TemporaryDirectory(prefix="layer_publish_") as scratch:
        folder = Path(scratch)
        layer_metrics.publish_folder(folder, now=NOW)
        write(folder, candidates(), {"state": "backed_up", "snapshot_set": {"table_a": 105}})
        layer_metrics.publish_folder(folder, now=NOW)
        write(folder, {"tables": "broken"}, {"state": "committed", "snapshot_set": {}})
        layer_metrics.publish_folder(folder, now=NOW)
        (folder / "candidates.json").write_text("not json", encoding="utf-8")
        layer_metrics.publish_folder(folder, now=NOW)
    found = pending()
    return {
        "no folder, a state that is not committed, broken tables or an unreadable file add nothing": not found,
        "none of them raises": True,
    }


def a_table_with_an_odd_row_count_is_left_out_of_the_sum(root: Path) -> dict[str, bool]:
    metrics_push.drain()
    odd = {"tables": {"table_a": {"candidate_snapshot_id": 1, "rows": 10, "base_snapshot_id": 0}, "table_b": {"rows": "many"}, "table_c": {"rows": -4}}}
    with tempfile.TemporaryDirectory(prefix="layer_publish_") as scratch:
        folder = Path(scratch)
        write(folder, odd, {"state": "committed", "snapshot_set": {"table_a": 1, "table_b": 2, "table_c": 3}})
        layer_metrics.publish_folder(folder, now=NOW)
    found = pending()
    return {
        "only a whole non-negative row count is summed": found.get(key("hai_publish_rows")) == 10,
        "the tables still count": found.get(key("hai_publish_tables")) == 3,
    }


def every_publish_metric_is_in_the_push_catalog_without_labels(root: Path) -> dict[str, bool]:
    names = (
        "hai_publish_tables",
        "hai_publish_rows",
        "hai_publish_tables_changed",
        "hai_publish_tables_empty",
        "hai_publish_commit_timestamp_seconds",
    )
    return {
        "all five are in the catalog": all(name in metrics_push.CATALOG for name in names),
        "none has a label, because the job already names the layer": all(metrics_push.CATALOG[name][1] == () for name in names),
    }


def the_publish_phase_hands_over_its_folder_after_its_acceptance(root: Path) -> dict[str, bool]:
    source = (root / "scripts/lakehouse/layer_run.py").read_text(encoding="utf-8")
    start = source.index("def publish(job: Job")
    body = source[start : source.index("def accepted_publish", start)]
    return {
        "the call is in the publish phase": "layer_metrics.publish_folder(job.publish_folder)" in body,
        "it comes after the commit is accepted": body.find("accept_commit(job)") < body.find("layer_metrics.publish_folder(") != -1,
    }


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "layer_publish_a_committed_publish_becomes_counts_and_a_time": a_committed_publish_becomes_counts_and_a_time,
    "layer_publish_anything_but_a_committed_publish_adds_nothing": anything_but_a_committed_publish_adds_nothing,
    "layer_publish_a_table_with_an_odd_row_count_is_left_out_of_the_sum": a_table_with_an_odd_row_count_is_left_out_of_the_sum,
    "layer_publish_every_publish_metric_is_in_the_push_catalog_without_labels": every_publish_metric_is_in_the_push_catalog_without_labels,
    "layer_publish_the_publish_phase_hands_over_its_folder_after_its_acceptance": the_publish_phase_hands_over_its_folder_after_its_acceptance,
}
