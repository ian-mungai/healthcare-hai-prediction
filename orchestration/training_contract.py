"""Validate the caller parameters and fixed actions of local training workflows."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from typing import Any

DAGS = ("training_dag", "model_promotion_dag")
RUN_ID = r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}"


def training_run_id(drift_run: str) -> str:
    """The training run's ID, derived from the drift run's so that a retried decision names the same run. The broker derives the same ID to bind its code."""
    return f"{drift_run[:16]}-{hashlib.sha256(f'training:{drift_run}'.encode()).hexdigest()[:8]}"


def model_training_run_id(drift_run: str, model: str) -> str:
    """The run ID of one model's training run for a drift run: the same for a retry of that model, another for every other model."""
    return f"{drift_run[:16]}-{hashlib.sha256(f'training:{model}:{drift_run}'.encode()).hexdigest()[:8]}"


def model_drift_run_id(features_run: str) -> str:
    """The run ID of the drift check of one model set's run: the same for a retry of that run. The chain gate binds it to the bronze run's staged code."""
    return f"{features_run[:16]}-{hashlib.sha256(f'drift:{features_run}'.encode()).hexdigest()[:8]}"


def model_features_run_id(base_run: str, model: str) -> str:
    """The run ID of one model set's features run for a base run: the same for a retry of that model, another for every other model."""
    return f"{base_run[:16]}-{hashlib.sha256(f'features:{model}:{base_run}'.encode()).hexdigest()[:8]}"


def trained_run_id(base_run: str, model: str) -> str:
    """The run ID of the training run of one model on a base run, as the features, drift and training DAGs chain it: the same for a retry of that run."""
    return model_training_run_id(model_drift_run_id(model_features_run_id(base_run, model)), model)


def research_run_id(base_run: str) -> str:
    """The run ID of the research run of a base run: the same for a retry, so a base is researched once."""
    return f"{base_run[:16]}-{hashlib.sha256(f'research:{base_run}'.encode()).hexdigest()[:8]}"


CHAIN_STEPS = ("canary_deploy", "canary", "promote", "release", "stable_deploy", "notice")
# The records of one infection type that the research run and the steps after it store, under ``research/<research run>/<kind>_<type>.json`` in the data
# bucket: the research record, the replay report of its canary, the canary check's result and the owner notice's receipt.
RECORD_KINDS = ("research", "replay", "canary", "notice")


def record_key(kind: str, research_run: str, hai_type: str) -> str:
    """The data bucket key of one record of a type after a research run."""
    if kind not in RECORD_KINDS:
        raise ValueError(f"kind must be one of {', '.join(RECORD_KINDS)}")
    if hai_type not in HAI_TYPES or not re.fullmatch(RUN_ID, research_run):
        raise ValueError("a record names a research run ID and a supported HAI type")
    return f"research/{research_run}/{kind}_{hai_type}.json"


def chain_run_id(research_run: str, hai_type: str, step: str) -> str:
    """The run ID of one step of one infection type's promotion chain after a research run: the same for a retry, another for every other type and step.

    The launch library records one launch for each job and run, so each launching step of the chain has a run of its own.
    """
    if step not in CHAIN_STEPS:
        raise ValueError(f"step must be one of {', '.join(CHAIN_STEPS)}")
    if hai_type not in HAI_TYPES:
        raise ValueError(f"type must be one of {', '.join(HAI_TYPES)}")
    if not re.fullmatch(RUN_ID, research_run):
        raise ValueError("run must be a research run ID")
    return f"{research_run[:16]}-{hashlib.sha256(f'chain:{step}:{hai_type}:{research_run}'.encode()).hexdigest()[:8]}"


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
FEATURE_DAGS = {f"features_{model}_dag": model for model in MODELS}
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
# Why a type trains without a drift record: the baseline phase of the drift task found no Production model of the model for it, or its feature schema changed.
TRAIN_WITHOUT_CHECK = ("no_production_model", "production_model_of_another_model", "feature_schema_changed", "no_features")


class GateRefused(ValueError):
    """The records do not support a drift gate decision; the message names what is wrong."""


def decide_drift_gate(model: str, baseline: Mapping[str, Any], records: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    """Return the drift gate decision of ``model`` from the baseline phase's record (``monitored`` types and ``unmonitored`` types with their reasons) and
    the drift record of each monitored type. The drift task writes it into its marker and the drift DAG recomputes it from the same records."""
    if model not in MODELS:
        raise GateRefused(f"{model} is not one of {', '.join(MODELS)}")
    monitored, unmonitored = list(baseline["monitored"]), dict(baseline["unmonitored"])
    if set(monitored) & set(unmonitored) or any(hai_type not in HAI_TYPES for hai_type in (*monitored, *unmonitored)):
        raise GateRefused("the baseline record names a type twice or an unknown type")
    decisions: dict[str, dict[str, Any]] = {}
    for hai_type, reason in sorted(unmonitored.items()):
        if reason not in TRAIN_WITHOUT_CHECK:
            raise GateRefused(f"{hai_type} was not monitored for the reason {reason}, which does not allow a decision")
        decisions[hai_type] = {"train": True, "reason": reason}
    for hai_type in sorted(monitored):
        record = records.get(hai_type)
        if record is None or not isinstance(record.get("triggered"), bool):
            raise GateRefused(f"{hai_type} is monitored and has no drift record with a triggered flag")
        if record["triggered"]:
            decisions[hai_type] = {"train": True, "reason": "drifted"}
        else:
            decisions[hai_type] = {"train": False, "reason": str(record.get("skip_reason") or "not_drifted")}
    train = [hai_type for hai_type in HAI_TYPES if decisions.get(hai_type, {}).get("train")]
    skipped = {hai_type: item["reason"] for hai_type, item in sorted(decisions.items()) if not item["train"]}
    return {
        "schema": 1,
        "kind": "drift_gate",
        "model": model,
        "decisions": decisions,
        "train_types": train,
        "train": bool(train),
        "skip_reasons": {} if train else skipped,
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


CHAIN_PARAMETER_KEYS = {
    "canary_check": {"research_run", "hai_type", "mlflow"},
    "owner_notice": {"research_run", "hai_type", "outcome", "mlflow"},
}
PROMOTION_OUTCOMES = ("promoted", "promoted_not_rolled_out", "held", "rolled_back")
SERVE_MODES = ("canary", "stable")


def serving_parameters_for(raw: Any) -> dict[str, str]:
    """Validate the closed request that asks the serve family to deploy: the infection type and the mode, and nothing else.

    The request names no version and no share. The serve preflight derives the candidate version of a canary from the type's research record, the canary share
    comes from the job table, and the stable version is the Production alias, which the serve job reads. So no caller can deploy any other version.
    """
    if not isinstance(raw, Mapping) or set(raw) != {"hai_type", "deploy_mode"}:
        raise ValueError("a serving request names the HAI type and the mode and nothing else: the version and the share are derived")
    if raw["hai_type"] not in HAI_TYPES or raw["deploy_mode"] not in SERVE_MODES:
        raise ValueError(f"the HAI type is unsupported or the mode is not one of {', '.join(SERVE_MODES)}")
    return {"hai_type": raw["hai_type"], "deploy_mode": raw["deploy_mode"]}


def chain_parameters_for(step: str, raw: Mapping[str, Any], lanes: tuple[str, ...] = LAUNCH_LANES) -> dict[str, Any]:
    """Validate the closed parameter set of a launching step of the promotion chain that research_dag runs after its research task.

    ``canary_check`` and ``owner_notice`` name the research run, the infection type and (for the notice) the outcome. ``promote`` is the promotion of one
    version, as ``model_promotion_dag`` takes it, and the chain never rolls back. Every step runs on AWS, so the MLflow lane is aws.
    """
    if step == "promote":
        checked = parameters_for("model_promotion_dag", raw, lanes)
        if checked["promotion_mode"] != "promote":
            raise ValueError("the promotion chain only promotes")
        return checked
    if step not in CHAIN_PARAMETER_KEYS:
        raise ValueError("chain parameters belong only to the canary check, the promotion and the owner notice")
    if set(raw) != CHAIN_PARAMETER_KEYS[step]:
        raise ValueError(f"{step} parameters must name {', '.join(sorted(CHAIN_PARAMETER_KEYS[step]))}")
    if not isinstance(raw["research_run"], str) or not re.fullmatch(RUN_ID, raw["research_run"]):
        raise ValueError("research_run must be a research run ID")
    if raw["hai_type"] not in HAI_TYPES:
        raise ValueError("hai_type must be a supported HAI type")
    if step == "owner_notice" and raw["outcome"] not in PROMOTION_OUTCOMES:
        raise ValueError(f"outcome must be one of {', '.join(PROMOTION_OUTCOMES)}")
    if raw["mlflow"] != "aws" or "aws" not in lanes:
        raise ValueError("every step of the promotion chain runs on AWS: name mlflow=aws")
    return dict(raw)


def research_requests(model: str, training_run: str, base_run: str, population: str, hai_types: Sequence[str], mlflow: str) -> list[dict[str, Any]]:
    """The one request for ``research_dag`` that a finished training run of ``model`` makes, or none.

    Only m1, m2 and m4 are compared, and only a run that names its base run can say which base to research. The research run derives from the base run, so the
    triggers of the three models name one run; each trigger has its own DAG run (``training__<training run>``), so the last model's trigger is not dropped as a
    repeat of the first model's.
    """
    if model not in PROMOTION_MODELS or not base_run:
        return []
    research = {"base_run": base_run, "population": population, "hai_types": list(hai_types), "mlflow": mlflow}
    return [
        {
            "trigger_run_id": f"training__{training_run}",
            "conf": {"run_id": research_run_id(base_run), "mode": "real", **parameters_for("research_dag", research, LAUNCH_LANES)},
        }
    ]


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
