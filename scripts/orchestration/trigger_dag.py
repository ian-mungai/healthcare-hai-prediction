"""Trigger one ingestion, silver, gold, features, feature store or training workflow run on the running orchestration stack and wait for its end.

Every wait has a deadline.

Usage::

    .venv/bin/python -m scripts.orchestration.trigger_dag --dag silver_transform_dag --mode real --wait-seconds 7200
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag gold_transform_dag --mode real --silver-snapshot-run <silver run id> --wait-seconds 7200
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag features_transform_dag --mode real --gold-snapshot-run <gold run id> --wait-seconds 7200
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag gold_transform_dag --run-id <run id> --status --wait-seconds 60
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag gold_transform_dag --run-id <run id> --settle --wait-seconds 600

A workflow whose job publishes from a producer run requires that run as a run ID, as its form does: gold the silver run, features and feature store the
gold run. A missing or ``latest`` producer run is refused before the secret is read and before any request, and so is a mode the workflow's form
does not accept: after the direct ECS launch every workflow here accepts only ``real`` (model promotion takes promote or rollback as
``--promotion-mode``).

A trigger creates the run before it polls, so a trigger that fails after the create can leave a queued or running run behind. ``--status`` reads the state of
one run (``absent`` when Airflow holds none) and its task states, and changes nothing. ``--settle`` pauses the DAG, so the scheduler starts nothing more of
it, marks a run that has not ended failed and reads it again until it is terminal; an absent or ended run is left as it is. Both end at ``--wait-seconds``.

The session's Airflow token secret is read from Parameter Store into memory (the supervisor stores it at ``up``); a short token is signed from it
and sent only to the loopback API port. Nothing is written but one line of JSON on standard output: the run ID, the end state and the task states.
"""

from __future__ import annotations

import argparse
import json
import re
import secrets
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from orchestration.ecs_launch import OWNER_STATEMENT_MIN
from orchestration.training_contract import DAGS as TRAINING_DAGS
from orchestration.training_contract import HAI_TYPES, LAUNCH_LANES, parameters_for
from scripts.infrastructure import secret_store
from scripts.orchestration.airflow_api import TERMINAL_RUN_STATES, AirflowApi, AirflowApiError

REPO_ROOT = Path(__file__).resolve().parents[2]
PLATFORM_FILE = REPO_ROOT / "config/orchestration/platform.json"
SETTLE_DAG = "launch_settle_dag"
# The producer-run parameter each workflow's form requires, by workflow.
PRODUCER_RUNS = {"gold_transform_dag": "silver_snapshot_run", "features_transform_dag": "gold_snapshot_run", "feature_store_dag": "gold_snapshot_run"}
DAGS = ("ingestion_dag", "silver_transform_dag", "gold_transform_dag", "features_transform_dag", "feature_store_dag", *TRAINING_DAGS, SETTLE_DAG)
JOBS_FILE = REPO_ROOT / "config/orchestration/ecs_broker_jobs.json"
STATEMENT_MAX = 500
# The modes each workflow's form accepts. A workflow the table does not name takes MODES.
FORM_MODES = {dag: ("real",) for dag in DAGS}
MODES = ("real",)
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
MAX_WAIT_SECONDS = 4 * 3600
POLL_SECONDS = 2.0
STATUS_CAUSE = re.compile(r"[A-Z]+ /\S*: (status [0-9]{3})(?:: .*)?", re.DOTALL)
DEADLINE_CAUSE = re.compile(r"[A-Z]+ /\S*: the deadline of the read passed")
TRANSPORT_CAUSE = re.compile(r"[A-Z]+ /\S*: ([A-Za-z]+)")
INCOMPLETE_CAUSE = re.compile(r"the task list of run \S+ is incomplete")


class TriggerError(RuntimeError):
    """A refused request or a run that did not end well; the message names the run and the cause, never a secret."""


def new_run_id() -> str:
    """A run ID in the format the workflows accept: the UTC time and eight random hex digits."""
    return f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{secrets.token_hex(4)}"


def settle_conf(mode: str, run_id: str, launch_run: str | None, job: str | None, statement: str | None) -> dict[str, Any]:
    """The validated configuration of a ``launch_settle_dag`` run: the launch's run and job from the job table and the owner's one-line statement of what
    was checked, at least as long as the launch library requires. The DAG run has its own run ID."""
    jobs = json.loads(JOBS_FILE.read_text())["jobs"]
    if mode != "real" or not RUN_ID.fullmatch(run_id):
        raise TriggerError(f"refused: the settle DAG runs in real mode with a run ID of its own ({mode!r}, {run_id!r})")
    if launch_run is None or not RUN_ID.fullmatch(launch_run):
        raise TriggerError(f"refused: --launch-run must name the run of the launch to settle, not {launch_run!r}")
    if job not in jobs:
        raise TriggerError(f"refused: --job must name a job of {JOBS_FILE.name}, not {job!r}")
    if statement is None or not OWNER_STATEMENT_MIN <= len(statement.strip()) <= STATEMENT_MAX or "\n" in statement or "\r" in statement:
        raise TriggerError(f"refused: --statement must be one line of {OWNER_STATEMENT_MIN} to {STATEMENT_MAX} characters naming what was checked")
    return {"run_id": run_id, "mode": mode, "launch_run": launch_run, "job": job, "statement": statement.strip()}


def run_conf(
    dag: str,
    mode: str,
    run_id: str,
    silver_snapshot_run: str | None,
    training: dict[str, Any] | None = None,
    gold_snapshot_run: str | None = None,
) -> dict[str, Any]:
    """The validated configuration of one run. Gold names the committed silver run it reads, features and feature store the gold run; the others name
    none."""
    if dag not in DAGS or not RUN_ID.fullmatch(run_id):
        raise TriggerError(f"refused: unknown workflow or run ID ({dag!r}, {run_id!r})")
    accepted = FORM_MODES.get(dag, MODES)
    if mode not in accepted:
        raise TriggerError(f"refused: {dag} accepts only the mode its form offers ({', '.join(accepted)}), not {mode!r}")
    conf: dict[str, Any] = {"run_id": run_id, "mode": mode}
    if dag in TRAINING_DAGS:
        if silver_snapshot_run is not None or gold_snapshot_run is not None:
            raise TriggerError("refused: a training workflow reads no silver or gold snapshot run")
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
        raise TriggerError("refused: training parameters belong only to training workflows")
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
    """``status`` or ``settle`` of one run, with the API's answers turned into a TriggerError that names the run and a safe cause."""
    try:
        return run_state(api, dag, run_id) if action == "status" else settle(api, dag, run_id, deadline, wait_seconds)
    except AirflowApiError as error:
        raise TriggerError(f"{dag} run {run_id}: {action}: {safe_cause(error)}") from None
    except (ValueError, KeyError, TypeError):
        raise TriggerError(f"{dag} run {run_id}: {action}: the API answered in an unexpected way") from None


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
    cli.add_argument("--dag", choices=DAGS, required=True)
    cli.add_argument("--mode", help="the mode of a new run: real for every workflow here; required for a trigger, refused with --status or --settle")
    cli.add_argument("--run-id", help="a run ID to use; a new one is made when absent")
    cli.add_argument("--silver-snapshot-run", help="gold only, required: the committed silver run (a run ID) it publishes from")
    cli.add_argument("--gold-snapshot-run", help="features and feature store only, required: the committed gold run (a run ID) they publish from")
    cli.add_argument("--features-run", default="latest")
    cli.add_argument("--population", choices=("primary", "sensitivity"), default="primary")
    cli.add_argument("--hai-types", choices=HAI_TYPES, nargs="+", default=list(HAI_TYPES))
    cli.add_argument("--trigger", default="manual")
    cli.add_argument("--mlflow", choices=("local", "aws"), default="aws")
    cli.add_argument("--hai-type", choices=HAI_TYPES)
    cli.add_argument("--model-version")
    cli.add_argument("--promotion-mode", choices=("promote", "rollback"), default="promote")
    cli.add_argument("--launch-run", help=f"{SETTLE_DAG} only: the run of the launch to settle")
    cli.add_argument("--job", help=f"{SETTLE_DAG} only: the job of the launch to settle")
    cli.add_argument("--statement", help=f"{SETTLE_DAG} only: one line naming what the owner checked (aws_tasks list, the ECS console, CloudTrail)")
    cli.add_argument("--wait-seconds", type=float, required=True, help=f"the deadline of the whole wait, at most {MAX_WAIT_SECONDS} s")
    action = cli.add_mutually_exclusive_group()
    action.add_argument("--status", action="store_true", help="read the state of the run --run-id and change nothing")
    action.add_argument("--settle", action="store_true", help="pause the DAG and mark the run --run-id failed unless it is absent or has ended")
    args = cli.parse_args(argv)
    inspecting = "status" if args.status else "settle" if args.settle else None
    if inspecting and (args.run_id is None or args.mode is not None):
        cli.error(f"--{inspecting} needs --run-id and takes no --mode")
    if not inspecting and args.mode is None:
        cli.error("a trigger needs --mode")
    try:
        if not 0 < args.wait_seconds <= MAX_WAIT_SECONDS:
            raise TriggerError(f"refused: --wait-seconds must be above 0 and at most {MAX_WAIT_SECONDS}")
        deadline = time.monotonic() + args.wait_seconds
        if inspecting:
            if not RUN_ID.fullmatch(args.run_id):
                raise TriggerError(f"refused: {args.run_id!r} is not a run ID")
            found = inspect(
                inspecting, args.dag, args.run_id, deadline, args.wait_seconds, AirflowApi(api_port(), read_token_secret(deadline), deadline=deadline)
            )
            sys.stdout.write(json.dumps(found) + "\n")
            return 0
        training = None
        if args.dag == "training_dag":
            training = {name: getattr(args, name) for name in ("features_run", "population", "hai_types", "trigger", "mlflow")}
        elif args.dag == "model_promotion_dag":
            training = {name: getattr(args, name) for name in ("hai_type", "model_version", "promotion_mode", "mlflow")}
        settle_fields = (args.launch_run, args.job, args.statement)
        if args.dag == SETTLE_DAG:
            if args.silver_snapshot_run is not None or args.gold_snapshot_run is not None:
                raise TriggerError(f"refused: {SETTLE_DAG} reads no silver or gold snapshot run")
            conf = settle_conf(args.mode, new_run_id() if args.run_id is None else args.run_id, *settle_fields)
        elif any(field is not None for field in settle_fields):
            raise TriggerError(f"refused: --launch-run, --job and --statement belong only to {SETTLE_DAG}")
        else:
            conf = run_conf(
                args.dag, args.mode, new_run_id() if args.run_id is None else args.run_id, args.silver_snapshot_run, training, args.gold_snapshot_run
            )
        token_secret = read_token_secret(deadline)
        result = trigger(args.dag, conf, deadline, args.wait_seconds, AirflowApi(api_port(), token_secret, deadline=deadline))
    except (TriggerError, secret_store.SecretStoreError) as error:
        sys.stderr.write(f"trigger_dag: {error}\n")
        return 1
    sys.stdout.write(json.dumps(result) + "\n")
    return 0 if result["state"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
