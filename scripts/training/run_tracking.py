"""Log one finished training run per HAI type to MLflow, once: a parent run with its child runs, the DVC metafile and the snapshot tags.

The layout follows ``data_contracts/training/model_contract.json``: experiment ``hai_training_<type>``, a parent run ``<run_id>_<population>`` with the
required tags and a child run per model slot, research question and validation. A run is found by its ``run_id`` and ``step_key`` tags, never created twice.
The parent carries ``bundle_sha256`` last, as the completion mark. A parent that has it with the bundle's value is left as it is. A parent without it is an
interrupted attempt, so it and every run of the same ``run_id`` are deleted (a soft delete) and logged again. A parent with another bundle hash, or two
complete parents, fail. The client is any object with the MLflow client calls used here (``MlflowClient`` has them).
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

RUN_TAG = "run_id"
STEP_TAG = "step_key"
PARENT_STEP = "parent"
PARENT_LINK = "mlflow.parentRunId"
COMPLETE_TAG = "bundle_sha256"
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

    def log_artifacts(self, run_id: str, local_dir: str, artifact_path: str | None = None) -> None: ...

    def delete_run(self, run_id: str) -> None: ...

    def set_terminated(self, run_id: str, status: str = "FINISHED") -> None: ...

    def get_run(self, run_id: str) -> Any: ...

    def download_artifacts(self, run_id: str, path: str, dst_path: str | None = None) -> str: ...


@dataclass(frozen=True)
class RunSpec:
    """One run to log: its step key and tags, parameters, metrics, files and folders."""

    step_key: str
    tags: Mapping[str, str]
    params: Mapping[str, str]
    metrics: Mapping[str, float]
    artifacts: Sequence[tuple[Path, str]] = ()
    folders: Sequence[tuple[Path, str]] = ()


@dataclass(frozen=True)
class TypePlan:
    """Everything one HAI type records, built and checked before any tracking call."""

    hai_type: str
    experiment: str
    bundle_sha256: str
    parent: RunSpec
    children: Sequence[RunSpec]
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


def populate(store: TrackingStore, run_id: str, spec: RunSpec) -> None:
    """Log a spec's parameters, metrics, files and folders into a fresh run."""
    for key, value in spec.params.items():
        store.log_param(run_id, key, value)
    for key, number in spec.metrics.items():
        store.log_metric(run_id, key, number)
    for path, folder in spec.artifacts:
        store.log_artifact(run_id, str(path), folder)
    for path, folder in spec.folders:
        store.log_artifacts(run_id, str(path), folder)


def log_type(store: TrackingStore, plan: TypePlan, run: str) -> str:
    """Log one type's parent and child runs unless a complete parent exists; return the parent run ID."""
    found = store.get_experiment_by_name(plan.experiment)
    experiment_id = str(found.experiment_id) if found is not None else store.create_experiment(plan.experiment)
    existing = list(store.search_runs([experiment_id], f"tags.{RUN_TAG} = '{run}'", max_results=1000))
    parents = [item for item in existing if item.data.tags.get(STEP_TAG) == PARENT_STEP]
    finished = [item for item in parents if COMPLETE_TAG in item.data.tags]
    if any(item.data.tags[COMPLETE_TAG] != plan.bundle_sha256 for item in finished):
        raise TrackingError("mlflow_bundle_conflict", f"a complete MLflow run of {run} and {plan.hai_type} carries another bundle hash")
    if len(finished) > 1:
        raise TrackingError("mlflow_duplicate_runs", f"{len(finished)} complete MLflow runs carry the training run {run} and type {plan.hai_type}")
    if finished:
        return str(finished[0].info.run_id)
    for item in existing:
        store.delete_run(str(item.info.run_id))
    parent_id = str(store.create_run(experiment_id, tags=dict(plan.parent.tags), run_name=f"{run}_{plan.parent.tags['population']}").info.run_id)
    populate(store, parent_id, plan.parent)
    for child in plan.children:
        child_id = str(store.create_run(experiment_id, tags={**child.tags, PARENT_LINK: parent_id}, run_name=child.step_key).info.run_id)
        populate(store, child_id, child)
        store.set_terminated(child_id, "FINISHED")
    dvc_baseline.log_baseline(plan.metafile, plan.matrix_path, dvc_baseline.Run(store, parent_id), plan.snapshots)
    store.set_terminated(parent_id, "FINISHED")
    store.set_tag(parent_id, COMPLETE_TAG, plan.bundle_sha256)
    return parent_id


def log_all(store: TrackingStore, plans: Sequence[TypePlan], run: str) -> dict[str, str]:
    """Log every type and return the parent run ID of each."""
    return {plan.hai_type: log_type(store, plan, run) for plan in plans}
