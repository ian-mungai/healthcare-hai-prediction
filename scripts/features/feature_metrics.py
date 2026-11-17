"""Hand the result of a feature step to the exit push of the job that ran it, so the dashboards show the feature store's state from the jobs themselves.

The Feast, reload and release modules each end by printing one JSON line on standard output. The features layer job reads that line and registers a metric for
the exit push (``scripts.metrics_push``): the online mode and publish time of a materialized set, the result of a reload and the result of a release. The label
values are the set name and the result word; a run ID is never a label. Nothing here can fail a job: unreadable output adds nothing.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

from scripts import metrics_push

ONLINE_MODES = ("load", "hold", "offline")
RELOAD_RESULTS = ("loaded", "already_loaded", "skipped")


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


def push_reload(model: str, text: str | None, seconds: float) -> bool:
    """Push the result of the online store reload of one set to the Pushgateway, in a group of its own: job features, phase reload, the outcome and the set.

    ``text`` is the standard output of ``scripts.features.online_reload``, or None when the reload failed or was refused. The result word, the closed skip
    reason, the rows of a fresh load and the duration are sent. The free text of the record and the run IDs never are. The group of the other outcome is
    deleted, so a set has one current group. The push and the deletion share the pusher's total deadline. False when nothing was pushed. Nothing here raises."""
    try:
        url = os.environ.get(metrics_push.URL_VARIABLE, "")
        if not url:
            return False
        record = (last_record(text) if text is not None else None) or {}
        result = str(record.get("outcome", ""))
        ok_run = result in RELOAD_RESULTS
        pusher = metrics_push.Pusher(url, "features", "reload", group={"feature_set": model})
        samples = [
            metrics_push.Sample("hai_online_reload_result", 1, {"result": result if ok_run else "failed"}),
            metrics_push.Sample("hai_online_reload_duration_seconds", seconds),
            metrics_push.Sample("hai_online_reload_timestamp_seconds", time.time()),
        ]
        if ok_run and result == "skipped":
            reason = "held_not_released" if record.get("skipped_held") else "no_load_evidence"
            samples.append(metrics_push.Sample("hai_online_reload_skip_reason", 1, {"reason": reason}))
        rows = record.get("rows")
        if ok_run and result == "loaded" and isinstance(rows, int) and not isinstance(rows, bool):
            samples.append(metrics_push.Sample("hai_online_reload_rows", rows))
        outcome, other = ("succeeded", "failed") if ok_run else ("failed", "succeeded")
        ends = time.monotonic() + pusher.budget()
        pushed = pusher.push_samples(outcome, samples, within=ends - time.monotonic())
        if pushed:
            pusher.delete_group(other, within=ends - time.monotonic())
        return pushed
    except Exception as error:
        metrics_push.line("metrics_push_failed", step="put", reason=type(error).__name__)
        return False
