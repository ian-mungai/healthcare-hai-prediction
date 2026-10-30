"""Log one finished training run per HAI type to MLflow, once: params, metrics, artifacts, the DVC metafile and the snapshot tags (project decision 20).

A run is found by its two tags (the training run ID and the HAI type), never created twice. A run that carries the completion tag is left as it is. An
unfinished run from an interrupted attempt is reused: a parameter or metric it already holds is not logged again. The completion tag is set last, after the
run is terminated, so a run without it is retried. The client is any object with the MLflow client calls used here (``MlflowClient`` has them).
"""

from __future__ import annotations

import importlib
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from scripts.training import dvc_baseline

EXPERIMENT = "hai_training"
RUN_TAG = "hai_training_run"
TYPE_TAG = "hai_type"
COMPLETE_TAG = "log_run_complete"
UNSAFE = re.compile(r"[^A-Za-z0-9_\-. /]")
NAME_LIMIT = 250


class TrackingError(Exception):
    """A tracking condition with its own failure name."""

    def __init__(self, name: str, detail: str) -> None:
        super().__init__(detail)
        self.name, self.detail = name, detail


class TrackingStore(Protocol):
    """The MLflow client calls this module makes."""

    def get_experiment_by_name(self, name: str) -> Any: ...

    def create_experiment(self, name: str) -> str: ...

    def search_runs(self, experiment_ids: list[str], filter_string: str = "", max_results: int = 1000) -> Sequence[Any]: ...

    def create_run(self, experiment_id: str, tags: dict[str, str] | None = None, run_name: str | None = None) -> Any: ...

    def log_param(self, run_id: str, key: str, value: Any) -> Any: ...

    def log_metric(self, run_id: str, key: str, value: float, timestamp: int | None = None, step: int | None = None) -> None: ...

    def set_tag(self, run_id: str, key: str, value: str) -> None: ...

    def log_artifact(self, run_id: str, local_path: str, artifact_path: str | None = None) -> None: ...

    def set_terminated(self, run_id: str, status: str = "FINISHED") -> None: ...

    def get_run(self, run_id: str) -> Any: ...

    def download_artifacts(self, run_id: str, path: str, dst_path: str | None = None) -> str: ...


@dataclass(frozen=True)
class RunPlan:
    """Everything one HAI type's run records, built and checked before any tracking call."""

    hai_type: str
    tags: Mapping[str, str]
    params: Mapping[str, str]
    metrics: Mapping[str, float]
    artifacts: Sequence[tuple[Path, str]]
    metafile: Path
    matrix_path: Path
    snapshots: Mapping[str, str]


def open_store(uri: str) -> TrackingStore:
    """Open the MLflow client for the tracking URI the job is given; the training image carries the library."""
    client: TrackingStore = importlib.import_module("mlflow.tracking").MlflowClient(tracking_uri=uri)
    return client


def metric_name(text: str) -> str:
    """Return a key MLflow accepts: letters, digits, underscore, dash, period, space and slash, at most 250 characters."""
    return UNSAFE.sub("_", text)[:NAME_LIMIT]


def numbers(prefix: str, value: Any) -> dict[str, float]:
    """Flatten every finite number under a nested result into metric names; text, booleans and missing values are skipped."""
    if isinstance(value, Mapping):
        found: dict[str, float] = {}
        for key, item in value.items():
            found |= numbers(f"{prefix}.{key}", item)
        return found
    if isinstance(value, list):
        found = {}
        for index, item in enumerate(value):
            found |= numbers(f"{prefix}.{index}", item)
        return found
    if isinstance(value, int | float) and not isinstance(value, bool) and math.isfinite(value):
        return {metric_name(prefix): float(value)}
    return {}


def log_plan(store: TrackingStore, experiment_id: str, plan: RunPlan, run: str) -> str:
    """Log one plan into its run, creating the run only when no run carries the training run ID and the type; return the run ID."""
    found = store.search_runs([experiment_id], f"tags.{RUN_TAG} = '{run}' and tags.{TYPE_TAG} = '{plan.hai_type}'", max_results=2)
    if len(found) > 1:
        raise TrackingError("mlflow_duplicate_runs", f"{len(found)} MLflow runs carry the training run {run} and type {plan.hai_type}")
    if found:
        run_id = str(found[0].info.run_id)
        if found[0].data.tags.get(COMPLETE_TAG) == "true":
            return run_id
        known_params, known_metrics = set(found[0].data.params), set(found[0].data.metrics)
    else:
        run_id = str(store.create_run(experiment_id, tags=dict(plan.tags), run_name=f"{run}:{plan.hai_type}").info.run_id)
        known_params, known_metrics = set(), set()
    for key, value in plan.params.items():
        if key not in known_params:
            store.log_param(run_id, key, value)
    for key, number in plan.metrics.items():
        if key not in known_metrics:
            store.log_metric(run_id, key, number)
    for path, folder in plan.artifacts:
        store.log_artifact(run_id, str(path), folder)
    dvc_baseline.log_baseline(plan.metafile, plan.matrix_path, dvc_baseline.Run(store, run_id), plan.snapshots)
    store.set_terminated(run_id, "FINISHED")
    store.set_tag(run_id, COMPLETE_TAG, "true")
    return run_id


def log_all(store: TrackingStore, plans: Sequence[RunPlan], run: str) -> dict[str, str]:
    """Log every plan into the project experiment and return the run ID of each type."""
    experiment = store.get_experiment_by_name(EXPERIMENT)
    experiment_id = str(experiment.experiment_id) if experiment is not None else store.create_experiment(EXPERIMENT)
    return {plan.hai_type: log_plan(store, experiment_id, plan, run) for plan in plans}
