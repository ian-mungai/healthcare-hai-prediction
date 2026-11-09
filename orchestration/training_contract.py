"""Validate the caller parameters and fixed actions of local training workflows."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from typing import Any

DAGS = ("training_dag", "model_promotion_dag")
RUN_ID = r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}"


def training_run_id(drift_run: str) -> str:
    """The training run's ID, derived from the drift run's so that a retried decision names the same run. The broker derives the same ID to bind its code."""
    return f"{drift_run[:16]}-{hashlib.sha256(f'training:{drift_run}'.encode()).hexdigest()[:8]}"


def model_training_run_id(drift_run: str, model: str) -> str:
    """The run ID of one model's training run for a drift run: the same for a retry of that model, another for every other model."""
    return f"{drift_run[:16]}-{hashlib.sha256(f'training:{model}:{drift_run}'.encode()).hexdigest()[:8]}"


HAI_TYPES = tuple(f"HAI_{index}" for index in range(1, 7))
MODEL_LISTS = {
    "m1": ("m1_ols", "rq4_reduced"),
    "m2": ("m2_elastic_net",),
    "m3": ("m3_mixed",),
    "m4": ("m4_boosting",),
    "m5": ("m5_spatial",),
    "m6": ("m6_fixed_effects",),
}
MODELS = tuple(MODEL_LISTS)
PROMOTION_MODELS = ("m1", "m2", "m4")
MEDIATION_MODELS = ("m2",)
OFFLINE_ACTIONS = ("guard", "fit", "mediation", "hac_validate", "smoke")
WRITER_ACTIONS = ("log_run", "promotion_check", "promote", "research")
TEMPLATES = ("training_job", "training_mlflow", "training_extract")
ACTION_SECONDS = {
    "extract": 1800,
    "version_extract": 900,
    "fit": 10800,
    "mediation": 3000,
    "hac_validate": 1800,
    "log_run": 900,
    "promotion_check": 900,
    "promote": 900,
    "research": 1800,
}
TRAINING_STEPS = (
    "acquire_run",
    "snapshot_training",
    "guard",
    "catalog_up",
    "extract",
    "version_extract",
    "fit",
    "mediation",
    "hac_validate",
    "log_run",
    "promotion_check",
    "run_record",
)
PROMOTION_STEPS = ("acquire_run", "snapshot_training", "promote", "run_record")
PREREQUISITES = {
    "guard": (),
    "extract": ("guard",),
    "version_extract": ("extract",),
    "fit": ("extract", "version_extract"),
    "mediation": ("extract", "version_extract", "fit"),
    "hac_validate": ("extract", "fit", "mediation"),
    "log_run": ("extract", "version_extract", "fit", "mediation", "hac_validate"),
    "promotion_check": ("extract", "version_extract", "fit", "log_run"),
    "promote": (),
    "research": (),
    "smoke": (),
}

INPUT_STEPS = {**PREREQUISITES, "mediation": ("extract",), "hac_validate": ("extract", "fit")}


def train_steps(model: str) -> tuple[str, ...]:
    """The data actions of one model's training phase, in order: the mediation belongs to m2 and the promotion check to the promotion-eligible models."""
    steps = ("version_extract", "fit", *(("mediation",) if model in MEDIATION_MODELS else ()), "hac_validate", "log_run")
    return (*steps, *(("promotion_check",) if model in PROMOTION_MODELS else ()))


def prerequisites_for(action: str, model: str) -> tuple[str, ...]:
    """The steps an action needs, without the mediation for a model that has none."""
    return tuple(step for step in PREREQUISITES[action] if step in ("guard", "extract") or step in train_steps(model))


MAC_LANES = ("local",)
JOB_LANES = ("local", "aws")
# A DAG step launches its job on AWS through the direct ECS launch library, where the MLflow lane is aws. mlflow=local is refused by name, never converted.
LAUNCH_LANES = ("aws",)


def _population_and_types(raw: Mapping[str, Any]) -> None:
    """Require the population and the distinct HAI types that training and research parameters share."""
    if raw["population"] not in ("primary", "sensitivity"):
        raise ValueError("population must be primary or sensitivity")
    types = raw["hai_types"]
    if not isinstance(types, list) or not types or any(not isinstance(item, str) or item not in HAI_TYPES for item in types) or len(set(types)) != len(types):
        raise ValueError("hai_types must contain distinct supported HAI types")


def parameters_for(dag_id: str, raw: Mapping[str, Any], lanes: tuple[str, ...] = MAC_LANES) -> dict[str, Any]:
    """Validate a complete closed parameter set. The Mac route accepts the local MLflow lane only, a job accepts the lanes its caller names (``JOB_LANES``)."""
    if dag_id == "training_dag":
        expected = {"features_run", "population", "hai_types", "trigger", "mlflow", "model"}
        if set(raw) != expected:
            raise ValueError("training parameters must name the publication, population, HAI types, trigger, MLflow lane and model")
        if not isinstance(raw["model"], str) or raw["model"] not in MODELS:
            raise ValueError(f"model must be one of {', '.join(MODELS)}")
        if raw["features_run"] != "latest" and not isinstance(raw["features_run"], str) or not re.fullmatch(f"latest|{RUN_ID}", str(raw["features_run"])):
            raise ValueError("features_run must be latest or a publication run ID")
        _population_and_types(raw)
        if not isinstance(raw["trigger"], str) or not re.fullmatch(f"manual|drift:{RUN_ID}", raw["trigger"]):
            raise ValueError("trigger must be manual or name a drift run")
    elif dag_id == "research_dag":
        if set(raw) != {"base_run", "population", "hai_types", "mlflow"}:
            raise ValueError("research parameters must name the base run, population, HAI types and MLflow lane")
        if not isinstance(raw["base_run"], str) or not re.fullmatch(RUN_ID, raw["base_run"]):
            raise ValueError("base_run must be a base publication run ID")
        _population_and_types(raw)
    elif dag_id == "model_promotion_dag":
        if set(raw) != {"hai_type", "model_version", "promotion_mode", "mlflow"}:
            raise ValueError("promotion parameters must name the HAI type, version, mode and MLflow lane")
        if raw["hai_type"] not in HAI_TYPES or raw["promotion_mode"] not in ("promote", "rollback"):
            raise ValueError("promotion type or mode is unsupported")
        if not isinstance(raw["model_version"], str) or not re.fullmatch(r"[1-9][0-9]{0,18}", raw["model_version"]):
            raise ValueError("model_version must be a positive version string")
    else:
        raise ValueError("training parameters belong only to the training and promotion DAGs")
    if raw["mlflow"] == "local" and lanes == LAUNCH_LANES:
        raise ValueError("mlflow=local is the Mac route's lane. A DAG step launches its job on AWS through the launch library: name mlflow=aws")
    if raw["mlflow"] == "aws" and "aws" not in lanes:
        raise ValueError("mlflow=aws is not built on the Mac route. It runs only as an AWS task. Use mlflow=local")
    if raw["mlflow"] not in lanes:
        raise ValueError(f"mlflow must be {' or '.join(lanes)}")
    return dict(raw)


def action_environment(template: str, action: str, mode: str) -> str:
    """Return the fixed credential kind for an allowed template action."""
    if mode not in ("fixture", "real"):
        raise ValueError("training supports fixture and real modes only")
    if template == "training_job" and action in OFFLINE_ACTIONS:
        return "offline"
    if template == "training_job" and action == "version_extract":
        return "offline" if mode == "fixture" else "dvc_write"
    if template == "training_mlflow" and action in WRITER_ACTIONS:
        return "mlflow"
    if template == "training_extract" and action == "extract":
        return "offline" if mode == "fixture" else "lakehouse_read"
    raise ValueError("training action is not allowed by this template")
