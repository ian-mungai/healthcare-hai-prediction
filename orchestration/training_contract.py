"""Validate the caller parameters and fixed actions of local training workflows."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

DAGS = ("training_dag", "model_promotion_dag")
RUN_ID = r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}"
HAI_TYPES = tuple(f"HAI_{index}" for index in range(1, 7))
OFFLINE_ACTIONS = ("guard", "fit", "mediation", "hac_validate", "smoke")
WRITER_ACTIONS = ("log_run", "promotion_check", "promote")
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


def parameters_for(dag_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a complete closed parameter set, refusing the unbuilt AWS writer route."""
    if dag_id == "training_dag":
        expected = {"features_run", "population", "hai_types", "trigger", "mlflow"}
        if set(raw) != expected:
            raise ValueError("training parameters must name the publication, population, HAI types, trigger and MLflow lane")
        if raw["features_run"] != "latest" and not isinstance(raw["features_run"], str) or not re.fullmatch(f"latest|{RUN_ID}", str(raw["features_run"])):
            raise ValueError("features_run must be latest or a publication run ID")
        if raw["population"] not in ("primary", "sensitivity"):
            raise ValueError("population must be primary or sensitivity")
        types = raw["hai_types"]
        if (
            not isinstance(types, list)
            or not types
            or any(not isinstance(item, str) or item not in HAI_TYPES for item in types)
            or len(set(types)) != len(types)
        ):
            raise ValueError("hai_types must contain distinct supported HAI types")
        if not isinstance(raw["trigger"], str) or not re.fullmatch(f"manual|drift:{RUN_ID}", raw["trigger"]):
            raise ValueError("trigger must be manual or name a drift run")
    elif dag_id == "model_promotion_dag":
        if set(raw) != {"hai_type", "model_version", "promotion_mode", "mlflow"}:
            raise ValueError("promotion parameters must name the HAI type, version, mode and MLflow lane")
        if raw["hai_type"] not in HAI_TYPES or raw["promotion_mode"] not in ("promote", "rollback"):
            raise ValueError("promotion type or mode is unsupported")
        if not isinstance(raw["model_version"], str) or not re.fullmatch(r"[1-9][0-9]{0,18}", raw["model_version"]):
            raise ValueError("model_version must be a positive version string")
    else:
        raise ValueError("training parameters belong only to the training and promotion DAGs")
    if raw["mlflow"] == "aws":
        raise ValueError("the AWS training writer route is not built. Use mlflow=local")
    if raw["mlflow"] != "local":
        raise ValueError("mlflow must be local")
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
