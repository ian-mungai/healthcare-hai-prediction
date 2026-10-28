"""Trigger one silver, gold or maintenance workflow run on the running orchestration stack and wait for its end, with a deadline.

Usage::

    .venv/bin/python -m scripts.orchestration.trigger_dag --dag silver_transform_dag --mode real --wait-seconds 7200
    .venv/bin/python -m scripts.orchestration.trigger_dag --dag gold_transform_dag --mode real --silver-snapshot-run <silver run id> --wait-seconds 7200

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

from orchestration.training_contract import DAGS as TRAINING_DAGS
from orchestration.training_contract import HAI_TYPES, parameters_for
from scripts.infrastructure import secret_store
from scripts.orchestration.airflow_api import TERMINAL_RUN_STATES, AirflowApi, AirflowApiError

REPO_ROOT = Path(__file__).resolve().parents[2]
PLATFORM_FILE = REPO_ROOT / "config/orchestration/platform.json"
DAGS = ("silver_transform_dag", "gold_transform_dag", "gold_maintenance_dag", *TRAINING_DAGS)
MODES = ("fixture", "publish_fixture", "real")
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


def run_conf(dag: str, mode: str, run_id: str, silver_snapshot_run: str | None, training: dict[str, Any] | None = None) -> dict[str, Any]:
    """The validated configuration of one run; a gold run names the committed silver set it reads, the others name none."""
    if dag not in DAGS or mode not in MODES or not RUN_ID.fullmatch(run_id):
        raise TriggerError(f"refused: unknown workflow, mode or run ID ({dag!r}, {mode!r}, {run_id!r})")
    conf: dict[str, Any] = {"run_id": run_id, "mode": mode}
    if dag in TRAINING_DAGS:
        if silver_snapshot_run is not None or mode not in (("fixture", "real") if dag == "training_dag" else ("real",)):
            raise TriggerError("refused: training mode or silver snapshot selection is unsupported")
        try:
            selected = parameters_for(dag, training or {})
        except ValueError as error:
            raise TriggerError(str(error)) from None
        if dag == "model_promotion_dag":
            conf.update({name: value for name, value in selected.items() if name != "promotion_mode"})
            conf["mode"] = selected["promotion_mode"]
        else:
            conf.update(selected)
        return conf
    if training is not None or mode == "fixture":
        raise TriggerError("refused: training parameters or fixture mode belong only to training workflows")
    if dag == "gold_transform_dag":
        snapshot = "latest" if silver_snapshot_run is None else silver_snapshot_run
        if snapshot != "latest" and not RUN_ID.fullmatch(snapshot):
            raise TriggerError(f"refused: silver snapshot run {snapshot!r} is not a run ID")
        conf["silver_snapshot_run"] = snapshot
    elif silver_snapshot_run is not None:
        raise TriggerError(f"refused: only gold_transform_dag reads a silver snapshot run, not {dag}")
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
    cli.add_argument("--mode", choices=MODES, required=True)
    cli.add_argument("--run-id", help="a run ID to use; a new one is made when absent")
    cli.add_argument("--silver-snapshot-run", help="gold only: the committed silver run to read, or latest")
    cli.add_argument("--features-run", default="latest")
    cli.add_argument("--population", choices=("primary", "sensitivity"), default="primary")
    cli.add_argument("--hai-types", choices=HAI_TYPES, nargs="+", default=list(HAI_TYPES))
    cli.add_argument("--trigger", default="manual")
    cli.add_argument("--mlflow", choices=("local", "aws"), default="local")
    cli.add_argument("--hai-type", choices=HAI_TYPES)
    cli.add_argument("--model-version")
    cli.add_argument("--promotion-mode", choices=("promote", "rollback"), default="promote")
    cli.add_argument("--wait-seconds", type=float, required=True, help=f"the deadline of the whole wait, at most {MAX_WAIT_SECONDS} s")
    args = cli.parse_args(argv)
    try:
        if not 0 < args.wait_seconds <= MAX_WAIT_SECONDS:
            raise TriggerError(f"refused: --wait-seconds must be above 0 and at most {MAX_WAIT_SECONDS}")
        deadline = time.monotonic() + args.wait_seconds
        training = None
        if args.dag == "training_dag":
            training = {name: getattr(args, name) for name in ("features_run", "population", "hai_types", "trigger", "mlflow")}
        elif args.dag == "model_promotion_dag":
            training = {name: getattr(args, name) for name in ("hai_type", "model_version", "promotion_mode", "mlflow")}
        conf = run_conf(args.dag, args.mode, new_run_id() if args.run_id is None else args.run_id, args.silver_snapshot_run, training)
        token_secret = read_token_secret(deadline)
        result = trigger(args.dag, conf, deadline, args.wait_seconds, AirflowApi(api_port(), token_secret, deadline=deadline))
    except (TriggerError, secret_store.SecretStoreError) as error:
        sys.stderr.write(f"trigger_dag: {error}\n")
        return 1
    sys.stdout.write(json.dumps(result) + "\n")
    return 0 if result["state"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
