"""Validate the caller parameters and fixed actions of local training workflows."""

from __future__ import annotations

import hashlib
import re
import secrets
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

DAGS = ("training_dag", "model_promotion_dag")

# The run ID form is ``<name>_<8 hex>[_<attempt>]`` and has no date. One pipeline run has one hex, which starts at its ingestion run: every run of that
# pipeline run carries it. A new attempt adds the suffix ``_2`` to ``_99`` and the suffix carries down to every run the attempt starts; research takes only its
# own attempt. The m1 to m6 and HAI_1 to HAI_6 keys stay in DAG IDs, Feast sets, Redis keys, S3 layout and MLflow tags: only a run ID spells the short names.
MODEL_NAMES = {"m1": "ols", "m2": "elastic_net", "m3": "mixed_effects", "m4": "gradient_boosting", "m5": "spatial", "m6": "fixed_effects"}
INFECTION_NAMES = {"HAI_1": "clabsi", "HAI_2": "cauti", "HAI_3": "ssi_colon", "HAI_4": "ssi_hysterectomy", "HAI_5": "mrsa", "HAI_6": "cdiff"}
MAX_ATTEMPT = 99
# The retry period is days 2 and 3 of the month (owner, Nov 16 2026, 9:06 AM PST); a retry run ID ends in its day of the month.
RETRY_FIRST_DAY = 2
RETRY_LAST_DAY = 3
CHAIN_STEPS = ("canary_deploy", "canary", "promote", "release", "stable_deploy", "notice")
# The names of a run ID without a qualifier, with a model name, with an infection name.
PLAIN_NAMES = ("ingestion", "bronze", "silver", "gold", "features", "metadata", "research", "serving", "promotion")
MODEL_QUALIFIED = ("features", "drift", "training", "research", "reload")
HEX_DIGITS = "[a-f0-9]{8}"
ATTEMPT_TAIL = "(?:_(?:[2-9]|[1-9][0-9]))?"


def _alternatives(names: Any) -> str:
    return "|".join(sorted((re.escape(name) for name in names), key=lambda name: (-len(name), name)))


_MODELS = _alternatives(MODEL_NAMES.values())
_INFECTIONS = _alternatives(INFECTION_NAMES.values())
RETRY_RUN_ID = f"ingestion_retry_{HEX_DIGITS}{ATTEMPT_TAIL}_[{RETRY_FIRST_DAY}-{RETRY_LAST_DAY}]"
# Every run ID of the form, one alternative for each name in the names table of the design.
RUN_ID = (
    f"(?:(?:{_alternatives(PLAIN_NAMES)})_{HEX_DIGITS}{ATTEMPT_TAIL}"
    f"|(?:{_alternatives(MODEL_QUALIFIED)})_(?:{_MODELS})_{HEX_DIGITS}{ATTEMPT_TAIL}"
    f"|(?:{_alternatives(CHAIN_STEPS)})_(?:{_INFECTIONS})_{HEX_DIGITS}{ATTEMPT_TAIL}"
    f"|{RETRY_RUN_ID})"
)
# The run ID of a code run is its own form (a commit time and eight digits of the commit, staged by CI) or, in development, the run ID the code is staged under.
CODE_STAMP_RUN = r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}"
# A run ID that stored data names, whether a record, a row, a pin or a file name: the run ID form or, for a run before the run ID redesign, the stamp form. A
# reader of stored history takes ``STORED_RUN``. A new run, a request and a writer take ``RUN_ID`` alone, so nothing new is ever created in the stamp form.
STORED_RUN = f"(?:{CODE_STAMP_RUN}|{RUN_ID})"
CODE_RUN = STORED_RUN
# The shape of a code run ID that the standalone scripts of the images and the host check (they import nothing of the repository). It is a safe path component
# that holds every code run ID and is wider than ``CODE_RUN`` in the names it takes.
CODE_RUN_SHAPE = r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}|[a-z]+(?:_[a-z]+)*_[a-f0-9]{8}(?:_[0-9]{1,2}){0,2}"


def run_id_pattern(*names: str, qualified: bool = False) -> str:
    """The pattern of the run IDs of ``names``: plain names, or with ``qualified`` the model-qualified or infection-qualified ones."""
    if not qualified:
        unknown = [name for name in names if name not in (*PLAIN_NAMES, "ingestion_retry")]
        if unknown or not names:
            raise ValueError(f"names without a qualifier are {', '.join(PLAIN_NAMES)} and ingestion_retry")
        return "(?:" + "|".join(RETRY_RUN_ID if name == "ingestion_retry" else f"{re.escape(name)}_{HEX_DIGITS}{ATTEMPT_TAIL}" for name in names) + ")"
    if not names or any(name not in (*MODEL_QUALIFIED, *CHAIN_STEPS) for name in names):
        raise ValueError("qualified names are the model names of a run and the chain steps")
    parts = [f"{re.escape(name)}_(?:{_INFECTIONS if name in CHAIN_STEPS else _MODELS})_{HEX_DIGITS}{ATTEMPT_TAIL}" for name in names]
    return "(?:" + "|".join(parts) + ")"


BASE_RUN = run_id_pattern("features")
SET_RUN = run_id_pattern("features", qualified=True)
DRIFT_RUN = run_id_pattern("drift", qualified=True)
RESEARCH_RUN = run_id_pattern("research")


def model_run_pattern(name: str, model: str) -> str:
    """The pattern of the run IDs of ``name`` (features, drift, training or research) for one model, for example ``features_elastic_net_<hex>``."""
    if name not in MODEL_QUALIFIED or model not in MODEL_NAMES:
        raise ValueError(f"a model run name is one of {', '.join(MODEL_QUALIFIED)} and a model one of {', '.join(MODEL_NAMES)}")
    return f"{name}_{re.escape(MODEL_NAMES[model])}_{HEX_DIGITS}{ATTEMPT_TAIL}"


@dataclass(frozen=True)
class RunIdParts:
    """The parts of a run ID: its name, the model or infection name that qualifies it (or None), its hex, its attempt (1 when it has no suffix) and, for a
    retry run, its retry day (otherwise 0)."""

    name: str
    qualifier: str | None
    hex: str
    attempt: int
    day: int


_PARTS = re.compile(
    rf"(?P<name>ingestion_retry|{_alternatives((*PLAIN_NAMES, *MODEL_QUALIFIED, *CHAIN_STEPS))})"
    rf"(?:_(?P<qualifier>{_MODELS}|{_INFECTIONS}))?_(?P<hex>{HEX_DIGITS})(?:_(?P<attempt>[0-9]{{1,2}}))?(?:_(?P<day>[0-9]))?"
)


def run_id_parts(run_id: str) -> RunIdParts:
    """Read a run ID back into its parts. Anything that is not of the form raises ``ValueError``."""
    if not isinstance(run_id, str) or not re.fullmatch(RUN_ID, run_id):
        raise ValueError(f"{run_id!r} is not a run ID of the form <name>_<8 hex>[_<attempt>]")
    found = _PARTS.fullmatch(run_id)
    if found is None:
        raise ValueError(f"{run_id!r} is not a run ID of the form <name>_<8 hex>[_<attempt>]")
    name, qualifier, digits, first, second = found["name"], found["qualifier"], found["hex"], found["attempt"], found["day"]
    if name == "ingestion_retry":
        # A retry run ends in its day; an attempt, when it has one, comes before the day.
        if second is None:
            return RunIdParts(name, None, digits, 1, int(first))
        return RunIdParts(name, None, digits, int(first), int(second))
    return RunIdParts(name, qualifier, digits, 1 if first is None else int(first), 0)


def new_run_hex() -> str:
    """A fresh random hex for a run that starts a pipeline run or starts by hand in the middle of one."""
    return secrets.token_hex(4)


def scheduled_run_hex(logical_date: datetime) -> str:
    """The hex of a scheduled ingestion run: the first eight hexadecimal digits of the SHA-256 digest of its UTC logical date, so a repeat is the same run."""
    if logical_date.tzinfo is None:
        raise ValueError("the logical date has no time zone")
    return hashlib.sha256(logical_date.astimezone(UTC).isoformat().encode()).hexdigest()[:8]


def attempt_of(attempt: Any) -> int:
    """The attempt, a whole number from 1 to ``MAX_ATTEMPT``; anything else raises ``ValueError`` that names the attempt."""
    if isinstance(attempt, bool) or not isinstance(attempt, int) or not 1 <= attempt <= MAX_ATTEMPT:
        raise ValueError(f"the attempt must be a whole number from 1 to {MAX_ATTEMPT}")
    return attempt


def compose_run_id(name: str, digits: str, attempt: int = 1, qualifier: str | None = None) -> str:
    """The run ID of ``name`` with ``digits`` as hex, an attempt suffix from attempt 2 and, where the name takes one, the model or infection name."""
    if not re.fullmatch(HEX_DIGITS, digits):
        raise ValueError("the hex is eight lower-case hexadecimal digits")
    suffix = "" if attempt_of(attempt) == 1 else f"_{attempt}"
    if name in CHAIN_STEPS:
        allowed: Any = INFECTION_NAMES.values()
    elif name in MODEL_QUALIFIED and name not in ("features", "research"):
        allowed = MODEL_NAMES.values()
    elif name in ("features", "research"):
        allowed = (None, *MODEL_NAMES.values())
    elif name in PLAIN_NAMES:
        allowed = (None,)
    else:
        raise ValueError(f"{name} is not a name of the run ID form")
    if qualifier not in allowed:
        raise ValueError(f"{name} does not take the qualifier {qualifier!r}")
    middle = "" if qualifier is None else f"_{qualifier}"
    return f"{name}{middle}_{digits}{suffix}"


def _parent(run_id: str, *names: str) -> RunIdParts:
    """The parts of a parent run ID, refused unless its name is one of ``names``."""
    parts = run_id_parts(run_id)
    if parts.name not in names:
        raise ValueError(f"{run_id!r} is not a run ID of {', '.join(names)}")
    return parts


def chain_layer_run(layer: str, bronze_run: str) -> str:
    """The run ID of ``layer`` (silver, gold, features or metadata) in the chain of ``bronze_run``: the bronze run's hex and attempt."""
    if layer not in ("silver", "gold", "features", "metadata"):
        raise ValueError(f"{layer} is not a layer of the bronze chain")
    parts = _parent(bronze_run, "bronze")
    return compose_run_id(layer, parts.hex, parts.attempt)


def model_features_run_id(base_run: str, model: str) -> str:
    """The run ID of one model set's features run for a base run (a ``features`` run): the base run's hex and attempt, and the model's name."""
    if model not in MODEL_NAMES:
        raise ValueError(f"model must be one of {', '.join(MODEL_NAMES)}")
    parts = _parent(base_run, "features")
    if parts.qualifier is not None:
        raise ValueError(f"{base_run!r} is a model set's run, not a base run")
    return compose_run_id("features", parts.hex, parts.attempt, MODEL_NAMES[model])


MAX_DRIFT_ATTEMPT = MAX_ATTEMPT


def drift_attempt_of(attempt: Any) -> int:
    """The drift attempt, a whole number from 1 to ``MAX_DRIFT_ATTEMPT``; anything else raises ``ValueError`` that names the drift attempt."""
    if isinstance(attempt, bool) or not isinstance(attempt, int) or not 1 <= attempt <= MAX_DRIFT_ATTEMPT:
        raise ValueError(f"the drift attempt must be a whole number from 1 to {MAX_DRIFT_ATTEMPT}")
    return attempt


def model_drift_run_id(features_run: str, attempt: int | None = None) -> str:
    """The run ID of the drift check of one model set's run: the features run's hex, model and attempt, or the attempt the owner names.

    The launch of a run is terminal once it ended, so a drift check that failed needs a run of its own. A later attempt is the owner's explicit choice
    (``trigger_dag --drift-attempt``): its suffix carries down to the training run the check may start.
    """
    parts = _parent(features_run, "features")
    if parts.qualifier is None:
        raise ValueError(f"{features_run!r} is a base run, not a model set's run")
    chosen = parts.attempt if attempt is None else drift_attempt_of(attempt)
    return compose_run_id("drift", parts.hex, chosen, parts.qualifier)


def model_training_run_id(drift_run: str, model: str) -> str:
    """The run ID of one model's training run for a drift run: the drift run's hex and attempt, and the model's name."""
    if model not in MODEL_NAMES:
        raise ValueError(f"model must be one of {', '.join(MODEL_NAMES)}")
    parts = _parent(drift_run, "drift")
    if parts.qualifier != MODEL_NAMES[model]:
        raise ValueError(f"{drift_run!r} is a drift run of another model than {model}")
    return compose_run_id("training", parts.hex, parts.attempt, MODEL_NAMES[model])


def trained_run_id(base_run: str, model: str) -> str:
    """The run ID of the training run of one model on a base run, as the features, drift and training DAGs chain it."""
    return model_training_run_id(model_drift_run_id(model_features_run_id(base_run, model)), model)


MAX_RESEARCH_ATTEMPT = MAX_ATTEMPT


def research_attempt_of(attempt: Any) -> int:
    """The research attempt, a whole number from 1 to ``MAX_RESEARCH_ATTEMPT``; anything else raises ``ValueError`` that names the attempt."""
    if isinstance(attempt, bool) or not isinstance(attempt, int) or not 1 <= attempt <= MAX_RESEARCH_ATTEMPT:
        raise ValueError(f"the research attempt must be a whole number from 1 to {MAX_RESEARCH_ATTEMPT}")
    return attempt


def research_run_id(base_run: str, attempt: int = 1) -> str:
    """The run ID of the research job of a base run: the base run's hex and the research run's own attempt (not the base run's), so a base is researched once.

    The launch of a run is terminal once it ended, so research that failed needs a run of its own. Attempt 1 is the run every base has; a later attempt is the
    owner's explicit choice (``--research-attempt``), and its suffix carries down to its promotion chain.
    """
    parts = _parent(base_run, "features")
    if parts.qualifier is not None:
        raise ValueError(f"{base_run!r} is a model set's run, not a base run")
    return compose_run_id("research", parts.hex, research_attempt_of(attempt))


def research_dag_run_id(training_run: str) -> str:
    """The Airflow run ID of the research DAG run that a training run triggers: ``research_<triggering model>_<hex>``, one for each trigger.

    The research job's own run ID is ``research_<hex>`` (``research_run_id``). The suffix of the training run does not carry here: research takes only its own.
    """
    parts = _parent(training_run, "training")
    return compose_run_id("research", parts.hex, 1, parts.qualifier)


# The records of one infection type that the research run and the steps after it store, under ``research/<research run>/<kind>_<type>.json`` in the data
# bucket: the research record, the replay report of its canary, the canary check's result and the owner notice's receipt.
RECORD_KINDS = ("research", "replay", "canary", "notice")


def record_key(kind: str, research_run: str, hai_type: str) -> str:
    """The data bucket key of one record of a type after a research run, for a research run of the run ID form (the key a writer uses)."""
    if kind not in RECORD_KINDS:
        raise ValueError(f"kind must be one of {', '.join(RECORD_KINDS)}")
    if hai_type not in INFECTION_NAMES or not re.fullmatch(run_id_pattern("research"), research_run):
        raise ValueError("a record names a research run ID and a supported HAI type")
    return f"research/{research_run}/{kind}_{hai_type}.json"


def stored_record_key(kind: str, research_run: str, hai_type: str) -> str:
    """The key of a stored record to read: ``record_key`` for a research run of the run ID form, and for a research run of the stamp form of before the run ID
    redesign the key it was stored under."""
    if re.fullmatch(CODE_STAMP_RUN, research_run) is None:
        return record_key(kind, research_run, hai_type)
    if kind not in RECORD_KINDS or hai_type not in INFECTION_NAMES:
        raise ValueError("a record names a research run ID and a supported HAI type")
    return f"research/{research_run}/{kind}_{hai_type}.json"


def chain_run_id(research_run: str, hai_type: str, step: str) -> str:
    """The run ID of one step of one infection type's promotion chain after a research run: ``<step>_<infection>_<hex>``, with the research run's attempt.

    The launch library records one launch for each job and run, so each launching step of the chain has a run of its own.
    """
    if step not in CHAIN_STEPS:
        raise ValueError(f"step must be one of {', '.join(CHAIN_STEPS)}")
    if hai_type not in INFECTION_NAMES:
        raise ValueError(f"type must be one of {', '.join(INFECTION_NAMES)}")
    parts = _parent(research_run, "research")
    if parts.qualifier is not None:
        raise ValueError("run must be a research run ID")
    return compose_run_id(step, parts.hex, parts.attempt, INFECTION_NAMES[hai_type])


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
        if raw["features_run"] != "latest" and not isinstance(raw["features_run"], str) or not re.fullmatch(f"latest|{SET_RUN}", str(raw["features_run"])):
            raise ValueError("features_run must be latest or the run ID of a model set's features run")
        _population_and_types(raw)
        if not isinstance(raw["trigger"], str) or not re.fullmatch(f"manual|drift:{DRIFT_RUN}", raw["trigger"]):
            raise ValueError("trigger must be manual or name a drift run")
    elif dag_id == "research_dag":
        if set(raw) - {"research_attempt"} != {"base_run", "population", "hai_types", "mlflow"}:
            raise ValueError("research parameters must name the base run, population, HAI types and MLflow lane, and may name a research attempt")
        if not isinstance(raw["base_run"], str) or not re.fullmatch(BASE_RUN, raw["base_run"]):
            raise ValueError("base_run must be the run ID of a features base run")
        if "research_attempt" in raw:
            # Attempt 1 is the default and is left out, so a base has one spelling of its first research run.
            try:
                if research_attempt_of(raw["research_attempt"]) == 1:
                    raise ValueError("attempt 1 is the default")
            except ValueError as error:
                raise ValueError(f"research_attempt is left out for attempt 1 or names a later attempt ({error})") from None
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
    if not isinstance(raw["research_run"], str) or not re.fullmatch(RESEARCH_RUN, raw["research_run"]):
        raise ValueError("research_run must be a research run ID")
    if raw["hai_type"] not in HAI_TYPES:
        raise ValueError("hai_type must be a supported HAI type")
    if step == "owner_notice" and raw["outcome"] not in PROMOTION_OUTCOMES:
        raise ValueError(f"outcome must be one of {', '.join(PROMOTION_OUTCOMES)}")
    if raw["mlflow"] != "aws" or "aws" not in lanes:
        raise ValueError("every step of the promotion chain runs on AWS: name mlflow=aws")
    return dict(raw)


RELEASE_KEYS = frozenset({"hai_type", "model", "features_run"})


def release_parameters_for(raw: Mapping[str, Any]) -> dict[str, str]:
    """Validate the closed parameters of the release step of the promotion chain: the infection type, the promotable model of its challenger and the ID of the
    features run that model trained on. The release phase of the features family releases that run's held online load (``scripts.features.online_release``)
    and nothing else, so the request has no mode, no lane and no other run."""
    if not isinstance(raw, Mapping) or set(raw) != RELEASE_KEYS:
        raise ValueError(f"release parameters must name {', '.join(sorted(RELEASE_KEYS))} and nothing else")
    if raw["hai_type"] not in HAI_TYPES:
        raise ValueError("hai_type must be a supported HAI type")
    if raw["model"] not in PROMOTION_MODELS:
        raise ValueError(f"model must be one of {', '.join(PROMOTION_MODELS)}")
    if not isinstance(raw["features_run"], str) or not re.fullmatch(SET_RUN, raw["features_run"]):
        raise ValueError("features_run must be the ID of a model set's features run")
    return {name: str(raw[name]) for name in sorted(RELEASE_KEYS)}


def research_requests(model: str, training_run: str, base_run: str, population: str, hai_types: Sequence[str], mlflow: str) -> list[dict[str, Any]]:
    """The one request for ``research_dag`` that a finished training run of ``model`` makes, or none.

    Only m1, m2 and m4 are compared, and only a run that names its base run can say which base to research. The research run derives from the base run, so the
    triggers of the three models name one research job run; each trigger has its own Airflow run (``research_dag_run_id``), so the last
    model's trigger is not dropped as a repeat of the first model's.
    """
    if model not in PROMOTION_MODELS or not base_run:
        return []
    research = {"base_run": base_run, "population": population, "hai_types": list(hai_types), "mlflow": mlflow}
    return [
        {
            "trigger_run_id": research_dag_run_id(training_run),
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
