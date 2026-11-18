"""Trigger one ingestion, silver, gold, features, feature store, training, research or serving workflow run on the running stack and wait for its end.

Every wait has a deadline.

Usage::

    .venv/bin/python -m scripts.orchestration.trigger_dag --dag silver_transform_dag --mode real --wait-seconds 7200
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag ingestion_dag --mode real --run-kind bootstrap --capture-root raw/ --wait-seconds 7200
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag ingestion_dag --mode real --capture-root ingestion_test/<run id>/ --wait-seconds 7200
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag gold_transform_dag --mode real --silver-snapshot-run <silver run id> --wait-seconds 7200
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag features_base_dag --mode real --gold-snapshot-run <gold run id> --wait-seconds 7200
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag training_dag --mode real --model m1 --base-run <base run id> --wait-seconds 14400
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag research_dag --mode real --base-run <base run id> --wait-seconds 14400
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag research_dag --mode real --base-run <base run id> --research-attempt 2 --wait-seconds 14400
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag model_promotion_dag --mode real --hai-type HAI_1 --model-version <version> --wait-seconds 3600
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag serving_deploy_dag --mode real --hai-type HAI_1 --deploy-mode stable --wait-seconds 3600
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag gold_transform_dag --run-id <run id> --status --wait-seconds 60
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag gold_transform_dag --run-id <run id> --settle --wait-seconds 600
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag drift_live_check_dag --mode real --model m2 --hai-type HAI_1 --wait-seconds 3600
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag drift_monitoring_dag --run-id <Airflow run ID> --clear-failed --wait-seconds 600
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag drift_monitoring_dag --mode real --model m6 \\
        --features-run <run> --drift-attempt 2 --wait-seconds 600

A workflow whose job publishes from a producer run requires that run as a run ID, as its form does: gold the silver run, features base the
gold run. A missing or ``latest`` producer run is refused before the secret is read and before any request, and so is a mode the workflow's form
does not accept: after the direct ECS launch every workflow here accepts only ``real`` (model promotion takes promote or rollback as
``--promotion-mode``).

``drift_live_check_dag`` starts the on-demand live drift check of the Production baseline of ``--model`` and ``--hai-type``. Its form is the run ID (a new
``drift_<model>_<hex>`` when absent), the mode ``real``, the model and the type, and no other option. The run runs the code staged for its own run
(``lakehouse/run_stage/<run ID>/inputs/code.tar.gz`` and ``code.sha256``), so name the run ID of the staged code with ``--run-id``. It starts no other DAG.

``--base-run`` names the run of ``features_base_dag`` whose model set a training or research run uses. The runs of the chain derive from it
(``orchestration/training_contract.py``), so the trigger derives them and refuses any other value: ``training_dag`` takes the training run of its ``--model``
on the features run of that model's set (``--run-id`` and ``--features-run`` may name only those) and sends the base run, so a run of m1, m2 or m4 starts
``research_dag`` itself for the base. ``research_dag`` takes the research run of the base (``--run-id`` may name only that run). The launch of a run is
terminal once it ended, so a research run that failed cannot start again: ``--research-attempt N`` (2 to 99) starts the research of the same base as a run
of its own, with its own research record and its own promotion chain, and sends the attempt in the conf. Attempt 1, the default, is the run every base has and
sends no attempt. The run ID that is printed is
the research run that ``serving_deploy_dag`` takes as ``--research-run``: a canary needs it, a stable deploy takes none. No other workflow takes ``--base-run``.

``ingestion_dag`` also takes ``--run-kind`` (``bootstrap``, the form's default, or ``monthly``) and ``--capture-root`` (``raw/``, the form's default, or one
``ingestion_test/<run id>/`` folder). Both are checked exactly as the launch preflight checks a publisher request (``require_publisher_request``), before the
secret is read and before any request, and no other workflow takes them. An option left out is left to the form's default.

A trigger creates the run before it polls, so a trigger that fails after the create can leave a queued or running run behind. ``--status`` reads the state of
one run (``absent`` when Airflow holds none) and its task states, and changes nothing. ``--settle`` pauses the DAG, so the scheduler starts nothing more of
it, marks a run that has not ended failed and reads it again until it is terminal; an absent or ended run is left as it is. Both end at ``--wait-seconds``.

``--clear-failed`` runs the failed tasks of one failed run again, in the same run. A launch that the preflight refuses (a launch hold, a code archive that is
not staged yet) writes no claim, so the run and job launch when the cause is gone; a launch that was claimed and ended is terminal and needs a new run
(``--research-attempt`` for research). The command unpauses the DAG, clears the tasks that failed and the tasks that never ran because of them, and prints the
run's state and the cleared task IDs. It refuses a run that is absent, queued, running or successful and a failed run with no failed task, and clears nothing
then. It takes ``--dag drift_monitoring_dag``, which only a features DAG starts, and the Airflow run ID of a run that a DAG started
(``drift_ols_<hex>``). The trigger starts the drift DAG by hand only with ``--drift-attempt N`` (2 to 99): the launch of a drift run is
terminal once it ended, so the drift check of a model run that failed starts again as a run of its own, with ``--model`` and the model run's ``--features-run``.
The run ID derives from the features run and the attempt (``model_drift_run_id``), the conf carries the attempt, the run runs the code staged for its own run
(``lakehouse/run_stage/<run ID>/inputs/code.tar.gz`` and ``code.sha256``) and the training run it may start runs the same code. Attempt 1, the default, is the
run the features DAG starts and is never started by hand.

The session's Airflow token secret is read from Parameter Store into memory (the supervisor stores it at ``up``); a short token is signed from it
and sent only to the loopback API port. Nothing is written but one line of JSON on standard output: the run ID, the end state and the task states.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

from orchestration.ecs_launch import LaunchRefused
from orchestration.ecs_preflight import require_publisher_request
from orchestration.interface import SubmissionRequest
from orchestration.training_contract import DAGS as TRAINING_DAGS
from orchestration.training_contract import (
    HAI_TYPES,
    LAUNCH_LANES,
    MODEL_NAMES,
    MODELS,
    compose_run_id,
    drift_attempt_of,
    model_drift_run_id,
    model_features_run_id,
    new_run_hex,
    parameters_for,
    research_attempt_of,
    research_run_id,
    serving_parameters_for,
    trained_run_id,
)
from orchestration.training_contract import RUN_ID as RUN_ID_PATTERN
from scripts.infrastructure import secret_store
from scripts.orchestration.airflow_api import TERMINAL_RUN_STATES, AirflowApi, AirflowApiError

REPO_ROOT = Path(__file__).resolve().parents[2]
PLATFORM_FILE = REPO_ROOT / "config/orchestration/platform.json"
# The producer-run parameter each workflow's form requires, by workflow.
PRODUCER_RUNS = {"gold_transform_dag": "silver_snapshot_run", "features_base_dag": "gold_snapshot_run"}
RESEARCH_DAG = "research_dag"
SERVING_DAG = "serving_deploy_dag"
LIVE_DAG = "drift_live_check_dag"
DRIFT_DAG = "drift_monitoring_dag"
# The options of each workflow's form besides the mode and the wait, by the name of their argument. A workflow takes no other option: one that is given and is
# not here is refused before the secret is read, whatever its value and even when it equals the default.
ACCEPTED_OPTIONS = {
    "ingestion_dag": {"run_id", "run_kind", "capture_root"},
    "silver_transform_dag": {"run_id"},
    "gold_transform_dag": {"run_id", "silver_snapshot_run"},
    "features_base_dag": {"run_id", "gold_snapshot_run"},
    "training_dag": {"run_id", "base_run", "features_run", "population", "hai_types", "trigger", "mlflow", "model"},
    "research_dag": {"run_id", "base_run", "research_attempt", "population", "hai_types", "mlflow"},
    "model_promotion_dag": {"run_id", "hai_type", "model_version", "promotion_mode", "mlflow"},
    "serving_deploy_dag": {"run_id", "hai_type", "deploy_mode", "research_run"},
    LIVE_DAG: {"run_id", "model", "hai_type"},
    DRIFT_DAG: {"run_id", "model", "features_run", "drift_attempt", "mlflow"},
}
# The options that have a default. They are unset after parsing, so that the ones the caller gave are known, and take their default afterwards.
OPTION_DEFAULTS: dict[str, Any] = {"population": "primary", "hai_types": list(HAI_TYPES), "trigger": "manual", "mlflow": "aws", "promotion_mode": "promote"}
OPTION_NAMES = (
    "run_id",
    "silver_snapshot_run",
    "run_kind",
    "capture_root",
    "gold_snapshot_run",
    "base_run",
    "features_run",
    "research_attempt",
    "drift_attempt",
    "population",
    "hai_types",
    "trigger",
    "mlflow",
    "model",
    "hai_type",
    "model_version",
    "promotion_mode",
    "deploy_mode",
    "research_run",
)
FAILED_TASK_STATES = ("failed", "upstream_failed")
# The workflows whose form takes the closed training parameters of the contract (``parameters_for``).
PARAMETER_DAGS = (*TRAINING_DAGS, RESEARCH_DAG)
DAGS = ("ingestion_dag", "silver_transform_dag", "gold_transform_dag", "features_base_dag", *PARAMETER_DAGS, SERVING_DAG, LIVE_DAG)
# The modes each workflow's form accepts. A workflow the table does not name takes MODES.
FORM_MODES = {dag: ("real",) for dag in DAGS}
MODES = ("real",)
# The form defaults of ingestion_dag, which stand for an option that is left out when the pair is checked.
PUBLISHER_DEFAULTS = {"run_kind": "bootstrap", "capture_root": "raw/"}
RUN_ID = re.compile(RUN_ID_PATTERN)
# The Airflow run ID of a run is its run ID (the research DAG's run that a training run triggers is research_<model>_<hex>, which is a run ID of the form too).
AIRFLOW_RUN_ID = RUN_ID
MAX_WAIT_SECONDS = 4 * 3600
POLL_SECONDS = 2.0
STATUS_CAUSE = re.compile(r"[A-Z]+ /\S*: (status [0-9]{3})(?:: .*)?", re.DOTALL)
DEADLINE_CAUSE = re.compile(r"[A-Z]+ /\S*: the deadline of the read passed")
TRANSPORT_CAUSE = re.compile(r"[A-Z]+ /\S*: ([A-Za-z]+)")
INCOMPLETE_CAUSE = re.compile(r"the task list of run \S+ is incomplete")


class TriggerError(RuntimeError):
    """A refused request or a run that did not end well; the message names the run and the cause, never a secret."""


HAND_START_NAMES = {
    "ingestion_dag": "ingestion",
    "silver_transform_dag": "silver",
    "gold_transform_dag": "gold",
    "features_base_dag": "features",
    "research_dag": "research",
    "model_promotion_dag": "promotion",
    SERVING_DAG: "serving",
}


def new_run_id(dag: str, model: str | None = None) -> str:
    """The run ID of a run started by hand: the workflow's name and a fresh random hex (a training run adds its model's name, and so does the live drift
    check, which is a drift run of its model: ``drift_<model>_<hex>``)."""
    if dag == LIVE_DAG:
        if model not in MODELS:
            raise TriggerError(live_refusal())
        return compose_run_id("drift", new_run_hex(), 1, MODEL_NAMES[model])
    if dag == "training_dag":
        if model not in MODELS:
            raise TriggerError(f"refused: a training run needs --model, one of {', '.join(MODELS)}")
        return compose_run_id("training", new_run_hex(), 1, MODEL_NAMES[model])
    if dag not in HAND_START_NAMES:
        raise TriggerError(f"refused: {dag} has no run ID of its own to make")
    return compose_run_id(HAND_START_NAMES[dag], new_run_hex())


def run_conf(
    dag: str,
    mode: str,
    run_id: str,
    silver_snapshot_run: str | None,
    training: dict[str, Any] | None = None,
    gold_snapshot_run: str | None = None,
    run_kind: str | None = None,
    capture_root: str | None = None,
) -> dict[str, Any]:
    """The validated configuration of one run. Gold names the committed silver run it reads, features and feature store the gold run; the others name
    none. Ingestion may name its run kind and capture root, which the launch preflight checks again."""
    if dag not in DAGS or not RUN_ID.fullmatch(run_id):
        raise TriggerError(f"refused: unknown workflow or run ID ({dag!r}, {run_id!r})")
    accepted = FORM_MODES.get(dag, MODES)
    if mode not in accepted:
        raise TriggerError(f"refused: {dag} accepts only the mode its form offers ({', '.join(accepted)}), not {mode!r}")
    conf: dict[str, Any] = {"run_id": run_id, "mode": mode}
    given_publisher = {"run_kind": run_kind, "capture_root": capture_root}
    if dag == "ingestion_dag":
        conf |= publisher_conf(dag, mode, run_id, given_publisher)
    elif any(value is not None for value in given_publisher.values()):
        raise TriggerError(f"refused: {dag} takes no run kind and no capture root, only ingestion_dag does")
    if dag in (*PARAMETER_DAGS, SERVING_DAG, LIVE_DAG) and (silver_snapshot_run is not None or gold_snapshot_run is not None):
        raise TriggerError("refused: a training, research, serving or live drift workflow reads no silver or gold snapshot run")
    if dag == LIVE_DAG:
        conf.update(live_conf(training or {}))
        return conf
    if dag == SERVING_DAG:
        conf.update(serving_conf(training or {}))
        return conf
    if dag in PARAMETER_DAGS:
        try:
            # A DAG step launches on AWS through the direct launch library, so only the aws MLflow lane is accepted (local is refused by name).
            selected = parameters_for(dag, training or {}, LAUNCH_LANES)
        except ValueError as error:
            raise TriggerError(f"refused: {error}") from None
        if dag == "model_promotion_dag":
            conf.update({name: value for name, value in selected.items() if name != "promotion_mode"})
            conf["mode"] = selected["promotion_mode"]
        else:
            conf.update(selected)
        return conf
    if training is not None:
        raise TriggerError("refused: training parameters belong only to training, research and serving workflows")
    given = {"silver_snapshot_run": silver_snapshot_run, "gold_snapshot_run": gold_snapshot_run}
    needed = PRODUCER_RUNS.get(dag)
    for name, value in given.items():
        if name != needed and value is not None:
            raise TriggerError(f"refused: {dag} reads no {name.replace('_', ' ')}")
    if needed is not None:
        value = given[needed]
        flag = "--" + needed.replace("_", "-")
        if value is None or not RUN_ID.fullmatch(value):
            raise TriggerError(f"refused: {dag} needs {flag} as a run ID (not latest), the producer run its job publishes from; got {value!r}")
        conf[needed] = value
    return conf


def serving_conf(given: dict[str, Any]) -> dict[str, str]:
    """The request of a deploy: the type and the mode as the serve family's closed request takes them, and the research run. A canary names a research run
    ID, as the serve preflight needs its record to derive the challenger; a stable deploy names none and leaves ``latest`` to the form."""
    try:
        closed = serving_parameters_for({name: given.get(name) for name in ("hai_type", "deploy_mode")})
    except ValueError as error:
        raise TriggerError(f"refused: serving: {error}") from None
    research = given.get("research_run")
    if closed["deploy_mode"] == "canary":
        if research is None or not RUN_ID.fullmatch(research):
            raise TriggerError(
                f"refused: a canary deploy needs --research-run as a research run ID (not latest), the run whose record names the challenger; got {research!r}"
            )
        return {**closed, "research_run": research}
    if research is not None:
        raise TriggerError("refused: a stable deploy deploys the Production version and takes no research run")
    return {**closed, "research_run": "latest"}


def live_conf(given: dict[str, Any]) -> dict[str, str]:
    """The form of the live drift check: the model and the HAI type of the Production baseline it checks, each of the contract (the form's own choices)."""
    model, hai_type = given.get("model"), given.get("hai_type")
    if model not in MODELS or hai_type not in HAI_TYPES:
        raise TriggerError(live_refusal())
    return {"model": str(model), "hai_type": str(hai_type)}


def live_refusal() -> str:
    return f"refused: the live drift check needs --model, one of {', '.join(MODELS)}, and --hai-type, one of {', '.join(HAI_TYPES)}"


def base_run_plan(
    dag: str, base_run: str | None, model: str | None, run_id: str | None, features_run: str | None, research_attempt: int | None = None
) -> tuple[str | None, str | None, dict[str, str]]:
    """The run ID, the features run and the extra conf that a base run fixes for a training or research run.

    The training run of a model on a base run, the features run of its set and the research run are the runs the features, drift and training DAGs derive
    from the base run, so a run that names any other ID would belong to no chain. A value left out is derived; a value that is named must be the derived one.
    The research run is also derived from the research attempt, which only ``research_dag`` takes and which needs the base run it derives from.
    """
    if research_attempt is not None and dag != RESEARCH_DAG:
        raise TriggerError(f"refused: {dag} takes no research attempt, only {RESEARCH_DAG} does")
    if research_attempt is not None and base_run is None:
        raise TriggerError("refused: a research attempt needs --base-run, the base run whose research run it derives")
    if base_run is None:
        return run_id, features_run, {}
    if dag not in (*TRAINING_DAGS[:1], RESEARCH_DAG):
        raise TriggerError(f"refused: {dag} takes no base run, only training_dag and research_dag do")
    if not RUN_ID.fullmatch(base_run):
        raise TriggerError(f"refused: --base-run must be the run ID of a features base run (features_<hex>), not {base_run!r}")
    if dag == RESEARCH_DAG:
        try:
            derived, features, extra = research_run_id(base_run, 1 if research_attempt is None else research_attempt), features_run, {}
        except ValueError as error:
            raise TriggerError(f"refused: --research-attempt: {error}") from None
    else:
        if model not in MODELS:
            raise TriggerError(f"refused: --base-run needs --model, one of {', '.join(MODELS)}, to derive the training run")
        derived, features, extra = trained_run_id(base_run, model), model_features_run_id(base_run, model), {"base_run": base_run}
        if features_run is not None and features_run != features:
            raise TriggerError(f"refused: with --base-run the features run is the one of the model's set, {features}, not {features_run!r}")
    if run_id is not None and run_id != derived:
        raise TriggerError(f"refused: with --base-run the run ID is the derived run {derived}, not {run_id!r}")
    return derived, features, extra


def publisher_conf(dag: str, mode: str, run_id: str, given: dict[str, str | None]) -> dict[str, str]:
    """The run kind and capture root that were named, after the pair that the run would use passes the preflight's own check of a publisher request."""
    pair = {name: PUBLISHER_DEFAULTS[name] if value is None else value for name, value in given.items()}
    request = SubmissionRequest(
        "publisher_run", run_id, mode, 1, dag, "trigger", "manual__trigger", publisher={"kind": pair["run_kind"], "capture_root": pair["capture_root"]}
    )
    try:
        require_publisher_request(request)
    except LaunchRefused as error:
        raise TriggerError(f"refused: {error}") from None
    return {name: value for name, value in given.items() if value is not None}


def api_port() -> int:
    """The loopback port of the Airflow API from the committed platform configuration."""
    return int(json.loads(PLATFORM_FILE.read_text())["listeners"]["airflow_api"]["port"])


def safe_cause(error: Exception) -> str:
    """The part of an API error that is safe to print: the HTTP status, a fixed cause or an exception class name, never a response body."""
    text = str(error)
    if found := STATUS_CAUSE.fullmatch(text):
        return f"HTTP {found[1]}"
    if DEADLINE_CAUSE.fullmatch(text):
        return "the wait deadline passed"
    if found := TRANSPORT_CAUSE.fullmatch(text):
        return f"the request failed ({found[1]})"
    if INCOMPLETE_CAUSE.fullmatch(text):
        return text
    return "the API answered in an unexpected way"


def wait_for_end(api: AirflowApi, dag: str, run_id: str, deadline: float, wait_seconds: float) -> dict[str, object]:
    """Poll until the run is in a terminal state; every request and sleep stops at the deadline and a state read after it is refused."""
    last = "not read"
    while True:
        expired = TriggerError(f"{dag} run {run_id}: not ended within {wait_seconds:g} s (last state {last})")
        if time.monotonic() >= deadline:
            raise expired
        record = api.run(dag, run_id)
        if time.monotonic() >= deadline:
            raise expired
        if record["state"] in TERMINAL_RUN_STATES:
            return record
        last = str(record["state"])
        time.sleep(max(min(POLL_SECONDS, deadline - time.monotonic()), 0.0))


def drift_attempt_conf(args: argparse.Namespace) -> dict[str, Any]:
    """The conf of a later attempt's drift run: the features DAG's conf for attempt 1 plus the attempt. Every refusal comes before the secret is read."""
    try:
        attempt = drift_attempt_of(args.drift_attempt)
    except ValueError as error:
        raise TriggerError(f"refused: --drift-attempt: {error}") from None
    if args.model not in MODELS:
        raise TriggerError(f"refused: a drift attempt needs --model, one of {', '.join(MODELS)}")
    if args.features_run is None or not RUN_ID.fullmatch(args.features_run):
        raise TriggerError("refused: a drift attempt needs --features-run, the features run ID of the model's set (not latest)")
    if args.mlflow != "aws":
        raise TriggerError("refused: the drift check runs on AWS, so --mlflow aws is the only lane")
    given = {"--base-run": args.base_run, "--research-attempt": args.research_attempt, "--silver-snapshot-run": args.silver_snapshot_run}
    given |= {"--gold-snapshot-run": args.gold_snapshot_run, "--run-kind": args.run_kind, "--capture-root": args.capture_root}
    given |= {"--hai-type": args.hai_type, "--model-version": args.model_version, "--deploy-mode": args.deploy_mode, "--research-run": args.research_run}
    extra = sorted(name for name, value in given.items() if value is not None)
    if extra:
        raise TriggerError(f"refused: {DRIFT_DAG} takes only --model, --features-run, --drift-attempt and --run-id, not {', '.join(extra)}")
    derived = model_drift_run_id(args.features_run, attempt)
    if args.run_id is not None and args.run_id != derived:
        raise TriggerError(f"refused: the run of drift attempt {attempt} on {args.features_run} is {derived}, the derived run; --run-id may name only that run")
    return {"run_id": derived, "mode": args.mode, "mlflow": "aws", "model": args.model, "features_run": args.features_run, "drift_attempt": attempt}


def require_accepted_options(dag: str, given: set[str]) -> None:
    """Refuse every option that was given and that the form of ``dag`` does not take."""
    extra = sorted(given - ACCEPTED_OPTIONS[dag])
    if extra:
        takes = ", ".join("--" + name.replace("_", "-") for name in sorted(ACCEPTED_OPTIONS[dag]))
        raise TriggerError(
            f"refused: {dag} takes only --mode, --wait-seconds{', ' + takes if takes else ''}, not {', '.join('--' + name.replace('_', '-') for name in extra)}"
        )


def trigger(dag: str, conf: dict[str, Any], deadline: float, wait_seconds: float, api: AirflowApi) -> dict[str, object]:
    """Unpause the workflow, start the run, wait until it ends or the deadline passes, and return its end state and task states."""
    started = time.monotonic()
    run_id = conf["run_id"]
    try:
        api.unpause(dag)
        api.trigger(dag, run_id, conf)
        run = wait_for_end(api, dag, run_id, deadline, wait_seconds)
        tasks = api.task_states(dag, run_id)
    except AirflowApiError as error:
        raise TriggerError(f"{dag} run {run_id}: {safe_cause(error)}") from None
    except (ValueError, KeyError, TypeError):
        raise TriggerError(f"{dag} run {run_id}: the API answered in an unexpected way") from None
    return {"dag": dag, "run_id": run_id, "state": run["state"], "seconds": round(time.monotonic() - started, 1), "tasks": tasks}


def run_state(api: AirflowApi, dag: str, run_id: str) -> dict[str, object]:
    """The run's state and task states, or the state ``absent`` when Airflow answers that it holds no such run."""
    try:
        record = api.run(dag, run_id)
    except AirflowApiError as error:
        if "status 404" in str(error):
            return {"dag": dag, "run_id": run_id, "state": "absent", "tasks": {}}
        raise
    return {"dag": dag, "run_id": run_id, "state": record["state"], "tasks": api.task_states(dag, run_id)}


def settle(api: AirflowApi, dag: str, run_id: str, deadline: float, wait_seconds: float) -> dict[str, object]:
    """Pause the DAG, mark the run failed unless it is absent or has ended, and read it until it is terminal; every call ends at the deadline."""
    api.pause(dag)
    found = run_state(api, dag, run_id)
    if found["state"] == "absent" or found["state"] in TERMINAL_RUN_STATES:
        return found | {"paused": True, "marked_failed": False}
    api.mark_failed(dag, run_id)
    wait_for_end(api, dag, run_id, deadline, wait_seconds)
    return run_state(api, dag, run_id) | {"paused": True, "marked_failed": True}


def inspect(action: str, dag: str, run_id: str, deadline: float, wait_seconds: float, api: AirflowApi) -> dict[str, object]:
    """``status``, ``settle`` or ``clear`` of one run, with the API's answers turned into a TriggerError that names the run and a safe cause."""
    try:
        if action == "status":
            return run_state(api, dag, run_id)
        return clear_failed(api, dag, run_id) if action == "clear" else settle(api, dag, run_id, deadline, wait_seconds)
    except AirflowApiError as error:
        raise TriggerError(f"{dag} run {run_id}: {action}: {safe_cause(error)}") from None
    except (ValueError, KeyError, TypeError):
        raise TriggerError(f"{dag} run {run_id}: {action}: the API answered in an unexpected way") from None


def clear_failed(api: AirflowApi, dag: str, run_id: str) -> dict[str, object]:
    """Run the failed tasks of one failed run again: refuse any other run, then unpause the DAG and clear the failed tasks and those that never ran because
    of them. A task that succeeded is left as it is."""
    found = run_state(api, dag, run_id)
    if found["state"] != "failed":
        raise TriggerError(f"refused: {dag} run {run_id} is {found['state']}, and only a failed run is cleared")
    states = found["tasks"]
    tasks = sorted(task for task, state in states.items() if state in FAILED_TASK_STATES) if isinstance(states, dict) else []
    if not tasks:
        raise TriggerError(f"refused: {dag} run {run_id} has no failed task to clear")
    api.unpause(dag)
    api.clear(dag, run_id, tasks)
    return run_state(api, dag, run_id) | {"cleared": tasks}


def read_token_secret(deadline: float) -> str:
    """The Airflow token secret, read with the time that remains before the command's deadline; the store setup and the read both count."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TriggerError("the deadline passed before the token secret was read")
    outcome = secret_store.fetch_all(REPO_ROOT, None, (secret_store.AIRFLOW_JWT_SECRET,), seconds=remaining)[secret_store.AIRFLOW_JWT_SECRET]
    if isinstance(outcome, secret_store.SecretStoreError):
        raise outcome
    return outcome


def main(argv: list[str] | None = None) -> int:
    cli = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    cli.add_argument("--dag", choices=(*DAGS, DRIFT_DAG), required=True)
    cli.add_argument("--mode", help="the mode of a new run: real for every workflow here; required for a trigger, refused with --status or --settle")
    cli.add_argument("--run-id", help="a run ID to use; a new one is made when absent")
    cli.add_argument("--silver-snapshot-run", help="gold only, required: the committed silver run (a run ID) it publishes from")
    cli.add_argument("--run-kind", help="ingestion_dag only: bootstrap (the form's default) or monthly")
    cli.add_argument("--capture-root", help="ingestion_dag only: raw/ (the form's default) or ingestion_test/<run id>/")
    cli.add_argument("--gold-snapshot-run", help="features and feature store only, required: the committed gold run (a run ID) they publish from")
    cli.add_argument(
        "--base-run", help="training_dag and research_dag: the features_base_dag run whose model set is trained or researched; derives the run IDs"
    )
    cli.add_argument(
        "--features-run",
        help="training_dag: the features run to fit on (latest, the default, or a run ID; with --base-run only the model set's run); a drift attempt: the run",
    )
    cli.add_argument(
        "--research-attempt",
        type=int,
        help="research_dag only, with --base-run: the attempt (2 to 99) of the research of the base, after research that failed; 1, the default, is the first",
    )
    cli.add_argument(
        "--drift-attempt",
        type=int,
        help="drift_monitoring_dag only, with --model and --features-run: the attempt (2 to 99) of the drift check of a model run, after a check that failed",
    )
    cli.add_argument("--population", choices=("primary", "sensitivity"))
    cli.add_argument("--hai-types", choices=HAI_TYPES, nargs="+")
    cli.add_argument("--trigger")
    cli.add_argument("--mlflow", choices=("local", "aws"))
    cli.add_argument(
        "--model", choices=MODELS, help="the model a training run fits (required for training_dag), a drift attempt checks or the live drift check covers"
    )
    cli.add_argument("--hai-type", choices=HAI_TYPES)
    cli.add_argument("--model-version")
    cli.add_argument("--promotion-mode", choices=("promote", "rollback"))
    cli.add_argument("--deploy-mode", help="serving_deploy_dag only, required: canary or stable")
    cli.add_argument("--research-run", help="serving_deploy_dag canary only, required: the research run (a run ID) whose record names the challenger")
    cli.add_argument("--wait-seconds", type=float, required=True, help=f"the deadline of the whole wait, at most {MAX_WAIT_SECONDS} s")
    action = cli.add_mutually_exclusive_group()
    action.add_argument("--status", action="store_true", help="read the state of the run --run-id and change nothing")
    action.add_argument("--settle", action="store_true", help="pause the DAG and mark the run --run-id failed unless it is absent or has ended")
    action.add_argument("--clear-failed", action="store_true", help="run the failed tasks of the failed run --run-id again in the same run")
    args = cli.parse_args(argv)
    given = {name for name in OPTION_NAMES if getattr(args, name) is not None}
    for name, value in OPTION_DEFAULTS.items():
        if getattr(args, name) is None:
            setattr(args, name, value)
    inspecting = "status" if args.status else "settle" if args.settle else "clear" if args.clear_failed else None
    if inspecting and (
        args.run_id is None or args.mode is not None or args.run_kind is not None or args.capture_root is not None or args.drift_attempt is not None
    ):
        cli.error(f"--{inspecting} needs --run-id and takes no --mode, no --run-kind, no --capture-root and no --drift-attempt")
    if not inspecting and args.mode is None:
        cli.error("a trigger needs --mode")
    try:
        if not 0 < args.wait_seconds <= MAX_WAIT_SECONDS:
            raise TriggerError(f"refused: --wait-seconds must be above 0 and at most {MAX_WAIT_SECONDS}")
        deadline = time.monotonic() + args.wait_seconds
        if args.drift_attempt is not None and args.dag != DRIFT_DAG:
            raise TriggerError(f"refused: {args.dag} takes no drift attempt, only {DRIFT_DAG} does")
        if not inspecting and args.dag == DRIFT_DAG and args.drift_attempt in (None, 1):
            raise TriggerError(
                f"refused: {args.dag} is started by the features DAG of its model (attempt 1), not by hand; --drift-attempt 2 to 99 starts a later attempt, "
                "and --status, --settle and --clear-failed take any run"
            )
        if not inspecting and args.dag not in (*DAGS, DRIFT_DAG):
            raise TriggerError(f"refused: {args.dag} is started by the features DAG of its model, not by hand; --status, --settle and --clear-failed take it")
        if inspecting:
            if not AIRFLOW_RUN_ID.fullmatch(args.run_id):
                raise TriggerError(f"refused: {args.run_id!r} is not a run ID")
            found = inspect(
                inspecting, args.dag, args.run_id, deadline, args.wait_seconds, AirflowApi(api_port(), read_token_secret(deadline), deadline=deadline)
            )
            sys.stdout.write(json.dumps(found) + "\n")
            return 0
        if args.dag != SERVING_DAG and (args.deploy_mode is not None or args.research_run is not None):
            raise TriggerError(f"refused: {args.dag} takes no deploy mode and no research run, only {SERVING_DAG} does")
        if args.dag == DRIFT_DAG:
            conf = drift_attempt_conf(args)
            require_accepted_options(args.dag, given)
            result = trigger(args.dag, conf, deadline, args.wait_seconds, AirflowApi(api_port(), read_token_secret(deadline), deadline=deadline))
            sys.stdout.write(json.dumps(result) + "\n")
            return 0 if result["state"] == "success" else 1
        if args.dag != "training_dag" and args.features_run is not None:
            raise TriggerError(f"refused: {args.dag} takes no features run, only training_dag does")
        run_id, features_run, extra = base_run_plan(args.dag, args.base_run, args.model, args.run_id, args.features_run, args.research_attempt)
        training = None
        if args.dag == "training_dag":
            args.features_run = "latest" if features_run is None else features_run
            training = {name: getattr(args, name) for name in ("features_run", "population", "hai_types", "trigger", "mlflow", "model")}
        elif args.dag == RESEARCH_DAG:
            training = {name: getattr(args, name) for name in ("base_run", "population", "hai_types", "mlflow")}
            if args.research_attempt is not None and research_attempt_of(args.research_attempt) > 1:
                training["research_attempt"] = args.research_attempt
        elif args.dag == "model_promotion_dag":
            training = {name: getattr(args, name) for name in ("hai_type", "model_version", "promotion_mode", "mlflow")}
        elif args.dag == SERVING_DAG:
            training = {"hai_type": args.hai_type, "deploy_mode": args.deploy_mode, "research_run": args.research_run}
        elif args.dag == LIVE_DAG:
            if args.model_version is not None:
                raise TriggerError(f"refused: {args.dag} takes no model version, only model_promotion_dag does")
            training = {"model": args.model, "hai_type": args.hai_type}
        conf = run_conf(
            args.dag,
            args.mode,
            new_run_id(args.dag, args.model) if run_id is None else run_id,
            args.silver_snapshot_run,
            training,
            args.gold_snapshot_run,
            args.run_kind,
            args.capture_root,
        )
        conf |= extra
        require_accepted_options(args.dag, given)
        token_secret = read_token_secret(deadline)
        result = trigger(args.dag, conf, deadline, args.wait_seconds, AirflowApi(api_port(), token_secret, deadline=deadline))
    except (TriggerError, secret_store.SecretStoreError) as error:
        sys.stderr.write(f"trigger_dag: {error}\n")
        return 1
    sys.stdout.write(json.dumps(result) + "\n")
    return 0 if result["state"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
