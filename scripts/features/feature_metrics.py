"""Hand the result of a feature step to the exit push of the job that ran it, so the dashboards show the feature store's state from the jobs themselves.

The Feast, reload and release modules each end by printing one JSON line on standard output. The features layer job reads that line and registers a metric for
the exit push (``scripts.metrics_push``): the online mode and publish time of a materialized set, the result of a reload and the result of a release. The label
values are the set name and the result word; a run ID is never a label. Nothing here can fail a job: unreadable output adds nothing.
"""

from __future__ import annotations

import json
import time
from typing import Any

from scripts import metrics_push

ONLINE_MODES = ("load", "hold", "offline")


def last_record(text: str) -> dict[str, Any] | None:
    """The last standard output line that is a JSON object, or None."""
    for line in reversed(text.splitlines()):
        if not line.startswith("{"):
            continue
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if isinstance(record, dict):
            return record
    return None


def materialized(text: str) -> None:
    """The online mode and publish time of a set whose Feast materialize step printed ``text``; the apply step and a set with no online store add no mode."""
    try:
        record = last_record(text)
        if record is None or record.get("step") != "materialize" or not isinstance(record.get("feature_set"), str):
            return
        feature_set = record["feature_set"]
        metrics_push.add("hai_feature_set_publish_timestamp_seconds", time.time(), feature_set=feature_set)
        if record.get("online") in ONLINE_MODES:
            metrics_push.add("hai_feature_set_online_mode", 1, feature_set=feature_set, mode=record["online"])
    except Exception:
        return


def outcome_of(text: str, metric: str) -> None:
    try:
        record = last_record(text)
        if record is not None and isinstance(record.get("feature_set"), str) and isinstance(record.get("outcome"), str):
            metrics_push.add(metric, 1, feature_set=record["feature_set"], result=record["outcome"])
    except Exception:
        return


def reloaded(text: str) -> None:
    """The result of an online store reload: loaded, already_loaded or skipped."""
    outcome_of(text, "hai_feature_reload_result")


def released(text: str) -> None:
    """The result of a release of a held online load: released, already_released or not_held."""
    outcome_of(text, "hai_feature_release_result")
