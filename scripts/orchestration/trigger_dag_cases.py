"""Cases of ``scripts.orchestration.trigger_dag``: the trigger and the bounded ``--status``, ``--settle`` and ``--clear-failed`` of one run, on a stand-in.

    .venv/bin/python -B -m scripts.orchestration.trigger_dag_cases --output <report.json>

A trigger creates the Airflow run before it polls, so a lost connection after the create leaves a queued run that the scheduler starts later. The owner
then reads the run's state (``--status``) and settles it (``--settle``): the DAG is paused and a run that has not ended is marked failed, then read again
until it is terminal, all within ``--wait-seconds``. An absent or already ended run is left as it is. Every case runs ``main`` with the token secret and the
API replaced by a stand-in that keeps the runs in memory; nothing reaches Airflow or AWS.
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import io
import json
import re
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration.training_contract import PIPELINE_ACTIVE_DAGS, PIPELINE_DAG, PIPELINE_STAGES
from scripts.orchestration import trigger_dag
from scripts.orchestration.airflow_api import AirflowApiError

DAG = "gold_transform_dag"
RUN = "silver_abcdef12"
SILVER = "silver_12345678"


class StandIn:
    """An Airflow API that keeps runs and the paused flag in memory. ``down`` makes every call fail as a lost connection; ``down_after`` fails every call
    after the first N; ``sticky`` keeps a run in its state after a mark (a scheduler that has not applied it)."""

    def __init__(self, runs: dict[str, str] | None = None, down: bool = False, down_after: int | None = None, sticky: bool = False) -> None:
        self.runs = dict(runs or {})
        self.paused: dict[str, bool] = {}
        self.calls: list[str] = []
        self.confs: list[dict[str, Any]] = []
        self.token_reads = 0
        self.down = down
        self.down_after = down_after
        self.sticky = sticky

    def call(self, name: str) -> None:
        self.calls.append(name)
        if self.down or (self.down_after is not None and len(self.calls) > self.down_after):
            raise AirflowApiError(f"GET /dags/{DAG}: ConnectionRefusedError")

    def unpause(self, dag_id: str) -> None:
        self.call("unpause")
        self.paused[dag_id] = False

    def pause(self, dag_id: str) -> None:
        self.call("pause")
        self.paused[dag_id] = True

    def trigger(self, dag_id: str, dag_run_id: str, conf: dict[str, Any]) -> dict[str, Any]:
        self.call("trigger")
        self.confs.append(dict(conf))
        self.runs[dag_run_id] = "queued"
        return {"dag_run_id": dag_run_id, "state": "queued"}

    def run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
        self.call("run")
        if dag_run_id not in self.runs:
            raise AirflowApiError(f"GET /dags/{dag_id}/dagRuns/{dag_run_id}: status 404: not found")
        return {"dag_run_id": dag_run_id, "state": self.runs[dag_run_id]}

    def task_states(self, dag_id: str, dag_run_id: str) -> dict[str, str | None]:
        self.call("task_states")
        return {"pin_silver": None if self.runs[dag_run_id] == "queued" else self.runs[dag_run_id]}

    def mark_failed(self, dag_id: str, dag_run_id: str) -> None:
        self.call("mark_failed")
        if not self.sticky:
            self.runs[dag_run_id] = "failed"


def invoke(api: StandIn, arguments: list[str]) -> tuple[int, dict[str, Any] | None, str, float]:
    """Run ``main`` with the stand-in; returns the exit code, the JSON it printed, its error text and the seconds it took."""
    out, err = io.StringIO(), io.StringIO()
    started = time.monotonic()

    def read_secret(deadline: float) -> str:
        api.token_reads += 1
        return "SYNTHETIC-secret"

    with (
        mock.patch.object(trigger_dag, "read_token_secret", read_secret),
        mock.patch.object(trigger_dag, "AirflowApi", lambda port, secret, deadline=None: api),
        mock.patch.object(trigger_dag, "POLL_SECONDS", 0.05),
        contextlib.redirect_stdout(out),
        contextlib.redirect_stderr(err),
    ):
        try:
            code = trigger_dag.main(arguments)
        except SystemExit as stopped:
            code = stopped.code if isinstance(stopped.code, int) else 2
    printed = out.getvalue().strip()
    return code, json.loads(printed) if printed else None, err.getvalue(), time.monotonic() - started


def status(*extra: str) -> list[str]:
    return ["--dag", DAG, "--run-id", RUN, "--status", "--wait-seconds", "5", *extra]


def settle(seconds: str = "5") -> list[str]:
    return ["--dag", DAG, "--run-id", RUN, "--settle", "--wait-seconds", seconds]


def lost_after_create() -> dict[str, bool]:
    """The finding: the create succeeds, the first poll loses the connection, the command fails and the run stays queued."""
    api = StandIn(down_after=2)  # unpause and trigger answer; the first poll does not
    code, _, error, _ = invoke(api, ["--dag", DAG, "--mode", "real", "--run-id", RUN, "--silver-snapshot-run", SILVER, "--wait-seconds", "5"])
    return {
        "a trigger that loses the connection after the create exits 1 and names the run": code == 1 and RUN in error,
        "the run it created is still queued in Airflow": api.runs.get(RUN) == "queued",
    }


def status_cases() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for state in ("queued", "running", "success", "failed"):
        api = StandIn({RUN: state})
        code, printed, _, _ = invoke(api, status())
        checks[f"status of a {state} run prints its state and changes nothing"] = (
            code == 0 and printed is not None and printed["state"] == state and "mark_failed" not in api.calls and "pause" not in api.calls
        )
    api = StandIn()
    code, printed, _, _ = invoke(api, status())
    checks["status of a run Airflow does not hold prints absent"] = code == 0 and printed is not None and printed["state"] == "absent"
    api = StandIn(down=True)
    code, printed, error, _ = invoke(api, status())
    checks["status without a connection exits 1, prints nothing and names the run and the cause"] = (
        code == 1 and printed is None and RUN in error and "ConnectionRefusedError" in error
    )
    return checks


def settle_cases() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    for state in ("queued", "running"):
        api = StandIn({RUN: state})
        code, printed, _, _ = invoke(api, settle())
        checks[f"settle of a {state} run pauses the DAG, marks the run failed and prints the terminal state"] = (
            code == 0
            and printed is not None
            and printed["state"] == "failed"
            and printed["marked_failed"] is True
            and api.paused.get(DAG) is True
            and api.calls.index("pause") < api.calls.index("mark_failed")
        )
    for state in ("success", "failed"):
        api = StandIn({RUN: state})
        code, printed, _, _ = invoke(api, settle())
        checks[f"settle of a {state} run pauses the DAG and leaves the run as it is"] = (
            code == 0 and printed is not None and printed["state"] == state and "mark_failed" not in api.calls and api.paused.get(DAG) is True
        )
    api = StandIn()
    code, printed, _, _ = invoke(api, settle())
    checks["settle of an absent run pauses the DAG and prints absent"] = (
        code == 0 and printed is not None and printed["state"] == "absent" and "mark_failed" not in api.calls and api.paused.get(DAG) is True
    )
    api = StandIn({RUN: "queued"}, down=True)
    code, printed, error, _ = invoke(api, settle())
    checks["settle without a connection exits 1 and names the run and the cause"] = (
        code == 1 and printed is None and RUN in error and "ConnectionRefusedError" in error
    )
    api = StandIn({RUN: "queued"}, sticky=True)
    code, printed, error, seconds = invoke(api, settle("1"))
    checks[f"settle of a run that stays queued after the mark ends at its deadline with exit 1 ({seconds:.1f} s)"] = (
        code == 1 and printed is None and RUN in error and seconds < 3
    )
    return checks


DRIFT = "drift_monitoring_dag"
DRIFT_RUN = "drift_ols_abcdef12"
DRIFT_TASKS = {"drift_run": "failed", "decide": "upstream_failed", "trigger_training": "upstream_failed"}


class Clearable(StandIn):
    """A stand-in that keeps task states and clears them: a cleared run is queued again, as the scheduler sees it after the clear call."""

    def __init__(self, runs: dict[str, str] | None = None, tasks: dict[str, str | None] | None = None, **options: Any) -> None:
        super().__init__(runs, **options)
        self.tasks = dict(DRIFT_TASKS if tasks is None else tasks)
        self.cleared: list[tuple[str, str, list[str]]] = []

    def task_states(self, dag_id: str, dag_run_id: str) -> dict[str, str | None]:
        self.call("task_states")
        return dict(self.tasks)

    def clear(self, dag_id: str, dag_run_id: str, task_ids: list[str]) -> None:
        self.call("clear")
        self.cleared.append((dag_id, dag_run_id, list(task_ids)))
        self.runs[dag_run_id] = "queued"


def clear(run: str = DRIFT_RUN, dag: str = DRIFT, *extra: str) -> list[str]:
    return ["--dag", dag, "--run-id", run, "--clear-failed", "--wait-seconds", "5", *extra]


def clear_cases() -> dict[str, bool]:
    """``--clear-failed`` runs the failed tasks of one failed run again in the same run: a launch the preflight refused wrote no claim, so the same run and
    job launch when the cause is gone. Only a failed run with a failed task is cleared, and only its failed tasks."""
    checks: dict[str, bool] = {}
    api = Clearable({DRIFT_RUN: "failed"})
    code, printed, _, _ = invoke(api, clear())
    checks["clear of a failed run unpauses the DAG and clears its failed and upstream failed tasks in the same run"] = (
        code == 0
        and printed is not None
        and api.cleared == [(DRIFT, DRIFT_RUN, ["decide", "drift_run", "trigger_training"])]
        and api.paused.get(DRIFT) is False
        and printed["cleared"] == ["decide", "drift_run", "trigger_training"]
        and printed["state"] == "queued"
        and printed["run_id"] == DRIFT_RUN
    )
    partial_api = Clearable({DRIFT_RUN: "failed"}, {"drift_run": "success", "decide": "failed", "trigger_training": "upstream_failed"})
    invoke(partial_api, clear())
    checks["clear leaves a task that succeeded as it is"] = partial_api.cleared == [(DRIFT, DRIFT_RUN, ["decide", "trigger_training"])]
    for state in ("queued", "running", "success"):
        refusing = Clearable({DRIFT_RUN: state})
        code, printed, error, _ = invoke(refusing, clear())
        checks[f"clear of a {state} run is refused and clears nothing"] = (
            code == 1 and printed is None and state in error and not refusing.cleared and "unpause" not in refusing.calls
        )
    missing = Clearable()
    code, printed, error, _ = invoke(missing, clear())
    checks["clear of a run Airflow does not hold is refused and clears nothing"] = code == 1 and printed is None and "absent" in error and not missing.cleared
    none_failed = Clearable({DRIFT_RUN: "failed"}, {"drift_run": "success", "decide": "success"})
    code, printed, error, _ = invoke(none_failed, clear())
    checks["clear of a failed run with no failed task is refused and clears nothing"] = (
        code == 1 and printed is None and "no failed task" in error and not none_failed.cleared
    )
    down = Clearable({DRIFT_RUN: "failed"}, down=True)
    code, printed, error, _ = invoke(down, clear())
    checks["clear without a connection exits 1 and names the run and the cause"] = code == 1 and printed is None and DRIFT_RUN in error and not down.cleared
    checks["clear needs a run ID and takes no mode and not status"] = all(
        invoke(Clearable({DRIFT_RUN: "failed"}), arguments)[0] == 2
        for arguments in (
            ["--dag", DRIFT, "--clear-failed", "--wait-seconds", "5"],
            [*clear(), "--mode", "real"],
            [*clear(), "--status"],
            [*clear(), "--settle"],
        )
    )
    other = Clearable({RUN: "failed"})
    code, printed, _, _ = invoke(other, clear(RUN, DAG))
    checks["clear works for the other workflows too"] = code == 0 and other.cleared == [(DAG, RUN, ["decide", "drift_run", "trigger_training"])]
    started = Clearable({DRIFT_RUN: "failed"})
    code, _, error, _ = invoke(started, ["--dag", DRIFT, "--mode", "real", "--wait-seconds", "5"])
    checks["the drift DAG cannot be triggered by hand, only read and cleared"] = (
        code == 1 and "drift_monitoring_dag" in error and not started.calls and started.token_reads == 0
    )
    return checks


def parse_cases() -> dict[str, bool]:
    def refused(arguments: list[str]) -> bool:
        return invoke(StandIn({RUN: "queued"}), arguments)[0] == 2

    checks = {
        "status without a run ID is refused": refused(["--dag", DAG, "--status", "--wait-seconds", "5"]),
        "settle without a run ID is refused": refused(["--dag", DAG, "--settle", "--wait-seconds", "5"]),
        "status and settle together are refused": refused(["--dag", DAG, "--run-id", RUN, "--status", "--settle", "--wait-seconds", "5"]),
        "a trigger without a mode is refused": refused(["--dag", DAG, "--run-id", RUN, "--wait-seconds", "5"]),
        "status with a mode is refused": refused([*status("--mode", "real")]),
        "a workflow the trigger does not name is refused": refused(["--dag", "unlisted_dag", "--mode", "real", "--wait-seconds", "5"]),
        "an option the trigger does not know is refused": refused(["--dag", DAG, "--mode", "real", "--statement", "x", "--wait-seconds", "5"]),
    }
    code = invoke(StandIn({RUN: "queued"}), ["--dag", DAG, "--run-id", RUN, "--settle", "--wait-seconds", "0"])[0]
    checks["settle with no wait time is refused"] = code == 1
    return checks


def trigger_cases() -> dict[str, bool]:
    """The trigger itself is unchanged: it unpauses, creates the run and waits for its end."""

    class Ends(StandIn):
        def run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
            record = super().run(dag_id, dag_run_id)
            self.runs[dag_run_id] = "success"
            return record

    api = Ends()
    code, printed, _, _ = invoke(api, ["--dag", DAG, "--mode", "real", "--run-id", RUN, "--silver-snapshot-run", SILVER, "--wait-seconds", "5"])
    ingestion = Ends()
    started, ended, _, _ = invoke(ingestion, ["--dag", "ingestion_dag", "--mode", "real", "--run-id", RUN, "--wait-seconds", "5"])
    extra = invoke(Ends(), ["--dag", "ingestion_dag", "--mode", "real", "--run-id", RUN, "--silver-snapshot-run", SILVER, "--wait-seconds", "5"])[0]
    return {
        "a trigger that ends in success exits 0 with the run's end state": code == 0
        and printed is not None
        and printed["state"] == "success"
        and api.calls[:2] == ["unpause", "trigger"],
        "ingestion_dag triggers with its run ID and mode only": started == 0 and ended is not None and ended["state"] == "success",
        "ingestion_dag refuses a silver snapshot run": extra != 0,
    }


TEST_ROOT = "ingestion_test/ingestion_0a1b2c3d/"


def publisher_cases() -> dict[str, bool]:
    """``--run-kind`` and ``--capture-root`` of ingestion_dag are checked exactly as the preflight checks a publisher request, before the secret is read and
    before any Airflow call."""
    from orchestration.ecs_launch import LaunchRefused
    from orchestration.ecs_preflight import require_publisher_request
    from orchestration.interface import SubmissionRequest

    class Ends(StandIn):
        def run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
            record = super().run(dag_id, dag_run_id)
            self.runs[dag_run_id] = "success"
            return record

    base = ["--dag", "ingestion_dag", "--mode", "real", "--run-id", RUN, "--wait-seconds", "5"]

    def triggered(*extra: str) -> tuple[int, StandIn]:
        api = Ends()
        return invoke(api, [*base, *extra])[0], api

    def preflight_accepts(kind: str, root: str) -> bool:
        request = SubmissionRequest("publisher_run", RUN, "real", 1, "ingestion_dag", "job", "manual__x", publisher={"kind": kind, "capture_root": root})
        try:
            require_publisher_request(request)
        except LaunchRefused:
            return False
        return True

    both, api = triggered("--run-kind", "monthly", "--capture-root", TEST_ROOT)
    kind_only, kind_api = triggered("--run-kind", "monthly")
    root_only, root_api = triggered("--capture-root", TEST_ROOT)
    explicit, explicit_api = triggered("--run-kind", "bootstrap", "--capture-root", "raw/")
    plain, plain_api = triggered()
    kinds = ("bootstrap", "monthly", "weekly", "", "Monthly", "monthly\n", " bootstrap")
    roots = ("raw/", TEST_ROOT, "raw", "", "lakehouse/", "ingestion_test/t1/", "ingestion_test/../raw/", "raw/\n", TEST_ROOT + "x/", TEST_ROOT.upper())
    pairs = [(kind, root) for kind in kinds for root in roots]
    same, refused_quietly = True, True
    for kind, root in pairs:
        code, quiet = triggered("--run-kind", kind, "--capture-root", root)
        same = same and ((code == 0) == preflight_accepts(kind, root))
        if code != 0:
            refused_quietly = refused_quietly and not quiet.calls and quiet.token_reads == 0 and not quiet.confs
    kind_alone = [triggered("--run-kind", kind) for kind in kinds]
    root_alone = [triggered("--capture-root", root) for root in roots]
    other_api = Ends()
    other_code, _, other_error, _ = invoke(
        other_api, ["--dag", DAG, "--mode", "real", "--run-id", RUN, "--silver-snapshot-run", SILVER, "--wait-seconds", "5", "--run-kind", "monthly"]
    )
    root_other_api = Ends()
    root_other_code = invoke(
        root_other_api, ["--dag", DAG, "--mode", "real", "--run-id", RUN, "--silver-snapshot-run", SILVER, "--wait-seconds", "5", "--capture-root", "raw/"]
    )[0]
    code, _, error, _ = invoke(StandIn(), [*base, "--run-kind", "weekly"])
    return {
        "a kind and a test root reach the run's conf": both == 0
        and api.confs == [{"run_id": RUN, "mode": "real", "run_kind": "monthly", "capture_root": TEST_ROOT}],
        "a kind alone sends the kind and leaves the root to the form": kind_only == 0
        and kind_api.confs == [{"run_id": RUN, "mode": "real", "run_kind": "monthly"}],
        "a root alone sends the root and leaves the kind to the form": root_only == 0
        and root_api.confs == [{"run_id": RUN, "mode": "real", "capture_root": TEST_ROOT}],
        "the explicit defaults are sent as given": explicit == 0
        and explicit_api.confs == [{"run_id": RUN, "mode": "real", "run_kind": "bootstrap", "capture_root": "raw/"}],
        "without either option the conf holds the run and the mode only": plain == 0 and plain_api.confs == [{"run_id": RUN, "mode": "real"}],
        f"the trigger accepts exactly the {len(pairs)} kind and root pairs that the preflight accepts": same,
        "every refused pair is refused before the secret is read and before any Airflow call": refused_quietly,
        "a kind alone is accepted exactly when the preflight accepts it with the default root": all(
            (code == 0) == preflight_accepts(kind, "raw/") for (code, _), kind in zip(kind_alone, kinds, strict=True)
        ),
        "a root alone is accepted exactly when the preflight accepts it with the default kind": all(
            (code == 0) == preflight_accepts("bootstrap", root) for (code, _), root in zip(root_alone, roots, strict=True)
        ),
        "a refused kind names the preflight's reason": code == 1 and "publisher_request_has_wrong_kind" in error,
        "another workflow refuses a run kind before any call": other_code == 1
        and "run kind" in other_error
        and not other_api.calls
        and other_api.token_reads == 0,
        "another workflow refuses a capture root before any call": root_other_code == 1 and not root_other_api.calls and root_other_api.token_reads == 0,
        "status with a run kind is refused": invoke(StandIn({RUN: "queued"}), status("--run-kind", "monthly"))[0] == 2,
        "settle with a capture root is refused": invoke(StandIn({RUN: "queued"}), [*settle(), "--capture-root", "raw/"])[0] == 2,
    }


BASE = "features_0b5e0001"
RESEARCH_RUN = "research_5e5ea4c1"
HAI_TYPE = "HAI_1"


class Ends(StandIn):
    """A stand-in whose runs end in success at the first poll."""

    def run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
        record = super().run(dag_id, dag_run_id)
        self.runs[dag_run_id] = "success"
        return record


def refused_quietly(arguments: list[str], words: str) -> bool:
    """The command exits 1, names ``words`` and reads no secret and calls no API."""
    api = Ends()
    code, printed, error, _ = invoke(api, arguments)
    return code == 1 and printed is None and words in error and not api.calls and api.token_reads == 0


def first(api: StandIn) -> dict[str, Any]:
    """The first conf the stand-in received, or an empty one when it received none."""
    return api.confs[0] if api.confs else {}


def base_run_cases() -> dict[str, bool]:
    """A base run names the derived runs of the chain of ``orchestration.training_contract``: the training run of a model, the features run it fits on and the
    research run. The conf must be the one the DAGs build for themselves, so a hand start joins the runs that the chain starts."""
    from orchestration import training_contract as contract

    train = ["--dag", "training_dag", "--mode", "real", "--model", "m1", "--base-run", BASE, "--wait-seconds", "5"]
    features = contract.model_features_run_id(BASE, "m1")
    trained = contract.trained_run_id(BASE, "m1")
    api = Ends()
    code, printed, _, _ = invoke(api, train)
    sensitivity = Ends()
    invoke(sensitivity, [*train, "--population", "sensitivity", "--hai-types", HAI_TYPE])
    explicit = Ends()
    explicit_code = invoke(explicit, [*train, "--run-id", trained, "--features-run", features])[0]
    hand = Ends()
    invoke(hand, ["--dag", "training_dag", "--mode", "real", "--model", "m3", "--wait-seconds", "5"])
    other = Ends()
    invoke(other, [*train[:5], "m4", *train[6:]])
    expected_training = {
        "run_id": trained,
        "mode": "real",
        "features_run": features,
        "population": "primary",
        "hai_types": list(contract.HAI_TYPES),
        "trigger": "manual",
        "mlflow": "aws",
        "model": "m1",
        "base_run": BASE,
    }
    research = ["--dag", "research_dag", "--mode", "real", "--base-run", BASE, "--wait-seconds", "5"]
    research_api = Ends()
    research_code, research_printed, _, _ = invoke(research_api, research)
    types_api = Ends()
    invoke(types_api, [*research, "--hai-types", HAI_TYPE, "--population", "sensitivity"])
    chained = contract.research_requests("m1", trained, BASE, "primary", list(contract.HAI_TYPES), "aws")[0]["conf"]
    second_run = contract.research_run_id(BASE, 2)
    attempt_api, first_attempt_api = Ends(), Ends()
    attempt_code, attempt_printed, _, _ = invoke(attempt_api, [*research, "--research-attempt", "2"])
    invoke(first_attempt_api, [*research, "--research-attempt", "1"])
    serve = ["--dag", "serving_deploy_dag", "--mode", "real", "--hai-type", HAI_TYPE, "--wait-seconds", "5"]
    canary_api, stable_api, unnamed_api = Ends(), Ends(), Ends()
    canary_code = invoke(canary_api, [*serve, "--deploy-mode", "canary", "--research-run", RESEARCH_RUN])[0]
    stable_code = invoke(stable_api, [*serve, "--deploy-mode", "stable"])[0]
    invoke(unnamed_api, ["--dag", "model_promotion_dag", "--mode", "real", "--hai-type", HAI_TYPE, "--model-version", "7", "--wait-seconds", "5"])
    return {
        "training with a base run triggers the derived training run on the derived features run and names the base run": code == 0
        and printed is not None
        and printed["run_id"] == trained
        and api.confs == [expected_training],
        "the derived training run is the run the drift DAG chains for the model": trained
        == contract.model_training_run_id(contract.model_drift_run_id(features), "m1"),
        "the population and HAI types of a training run with a base run are sent as given": first(sensitivity).get("population") == "sensitivity"
        and first(sensitivity).get("hai_types") == [HAI_TYPE]
        and first(sensitivity).get("run_id") == trained,
        "the run ID and the features run may be named when they are the derived ones": explicit_code == 0 and explicit.confs == [expected_training],
        "another model derives its own runs": first(other).get("run_id") == contract.trained_run_id(BASE, "m4")
        and first(other).get("features_run") == contract.model_features_run_id(BASE, "m4"),
        "training without a base run keeps the hand start: latest features, no base run in the conf": "base_run" not in first(hand)
        and first(hand).get("features_run") == "latest",
        "a base run with another run ID is refused before any call": refused_quietly([*train, "--run-id", RUN], "derived"),
        "a base run with another features run is refused before any call": refused_quietly([*train, "--features-run", BASE], "features"),
        "a base run with no model is refused before any call": refused_quietly([*train[:4], *train[6:]], "model"),
        "a base run that is not a run ID is refused before any call": refused_quietly([*train[:7], "latest", *train[8:]], "run ID"),
        "a workflow that takes no base run refuses it before any call": all(
            refused_quietly(["--dag", dag, "--mode", "real", "--base-run", BASE, *extra, "--wait-seconds", "5"], "base run")
            for dag, extra in (
                ("ingestion_dag", ()),
                ("gold_transform_dag", ("--silver-snapshot-run", SILVER)),
                ("model_promotion_dag", ("--hai-type", HAI_TYPE, "--model-version", "7")),
                ("serving_deploy_dag", ("--hai-type", HAI_TYPE, "--deploy-mode", "stable")),
            )
        ),
        "research triggers the research run derived from the base run with the conf the training DAG sends": research_code == 0
        and research_printed is not None
        and research_printed["run_id"] == contract.research_run_id(BASE)
        and research_api.confs == [chained],
        "the research population and HAI types are sent as given": first(types_api).get("hai_types") == [HAI_TYPE]
        and first(types_api).get("population") == "sensitivity",
        "research without a base run is refused before any call": refused_quietly(
            ["--dag", "research_dag", "--mode", "real", "--wait-seconds", "5"], "base_run"
        ),
        "research with another run ID is refused before any call": refused_quietly([*research, "--run-id", RUN], "derived"),
        "research on the local MLflow lane is refused before any call": refused_quietly([*research, "--mlflow", "local"], "mlflow=local"),
        "research attempt 2 triggers the research run of that attempt and records the attempt in the conf": attempt_code == 0
        and attempt_printed is not None
        and attempt_printed["run_id"] == second_run != contract.research_run_id(BASE)
        and attempt_api.confs == [{**chained, "run_id": second_run, "research_attempt": 2}],
        "research attempt 1 is the default run with no attempt in the conf": first_attempt_api.confs == [chained],
        "a research attempt outside 1 to 99 is refused before any call": refused_quietly([*research, "--research-attempt", "0"], "research attempt")
        and refused_quietly([*research, "--research-attempt", "100"], "research attempt"),
        "a research run ID that is not the attempt's derived run is refused before any call": refused_quietly(
            [*research, "--research-attempt", "2", "--run-id", contract.research_run_id(BASE)], "derived"
        )
        and refused_quietly([*research, "--research-attempt", "2", "--run-id", RUN], "derived"),
        "the derived run of attempt 2 may be named": invoke(Ends(), [*research, "--research-attempt", "2", "--run-id", second_run])[0] == 0,
        "a workflow that is not research refuses a research attempt before any call": refused_quietly([*train, "--research-attempt", "2"], "research attempt"),
        "a research attempt without a base run is refused before any call": refused_quietly(
            ["--dag", "research_dag", "--mode", "real", "--research-attempt", "2", "--wait-seconds", "5"], "base run"
        ),
        "a canary deploy names the research run and the type and the mode and nothing else": canary_code == 0
        and canary_api.confs
        == [{"run_id": first(canary_api).get("run_id"), "mode": "real", "hai_type": HAI_TYPE, "deploy_mode": "canary", "research_run": RESEARCH_RUN}],
        "a stable deploy leaves the research run to the form": stable_code == 0
        and stable_api.confs
        == [{"run_id": first(stable_api).get("run_id"), "mode": "real", "hai_type": HAI_TYPE, "deploy_mode": "stable", "research_run": "latest"}],
        "a canary deploy without a research run ID is refused before any call": refused_quietly([*serve, "--deploy-mode", "canary"], "research run")
        and refused_quietly([*serve, "--deploy-mode", "canary", "--research-run", "latest"], "research run"),
        "a stable deploy that names a research run is refused before any call": refused_quietly(
            [*serve, "--deploy-mode", "stable", "--research-run", RESEARCH_RUN], "research run"
        ),
        "a deploy without a mode or with another mode is refused before any call": refused_quietly(serve, "serving")
        and refused_quietly([*serve, "--deploy-mode", "rollout"], "serving"),
        "a deploy without a type is refused before any call": refused_quietly(
            ["--dag", "serving_deploy_dag", "--mode", "real", "--deploy-mode", "stable", "--wait-seconds", "5"], "serving"
        ),
        "a workflow that is not a deploy refuses a deploy mode and a research run before any call": refused_quietly(
            [*train[:8], "--deploy-mode", "stable", "--wait-seconds", "5"], "deploy"
        )
        and refused_quietly(["--dag", "research_dag", "--mode", "real", "--base-run", BASE, "--research-run", RESEARCH_RUN, "--wait-seconds", "5"], "deploy"),
        "the promotion of a version keeps its own options": unnamed_api.confs
        == [{"run_id": first(unnamed_api).get("run_id"), "mode": "promote", "hai_type": HAI_TYPE, "model_version": "7", "mlflow": "aws"}],
    }


FEATURES_RUN = "features_fixed_effects_0a0a0a06"


def drift_attempt_cases() -> dict[str, bool]:
    """The drift check of a model run cannot launch again once its launch ended, so ``--drift-attempt N`` (2 to 99) starts it as a run of its own: the run
    ID derives from the model's features run and the attempt, and every refusal comes before the secret is read and before any Airflow call."""
    from orchestration import training_contract as contract

    class Ends(StandIn):
        def run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
            record = super().run(dag_id, dag_run_id)
            self.runs[dag_run_id] = "success"
            return record

    start = ["--dag", DRIFT, "--mode", "real", "--model", "m6", "--features-run", FEATURES_RUN, "--wait-seconds", "5"]
    second = contract.model_drift_run_id(FEATURES_RUN, 2)
    api = Ends()
    code, printed, _, _ = invoke(api, [*start, "--drift-attempt", "2"])
    named = Ends()
    named_code = invoke(named, [*start, "--drift-attempt", "2", "--run-id", second])[0]
    return {
        "drift attempt 2 triggers the drift run of that attempt with the conf the features DAG builds and names the attempt": code == 0
        and printed is not None
        and printed["run_id"] == second != contract.model_drift_run_id(FEATURES_RUN)
        and api.confs == [{"run_id": second, "mode": "real", "mlflow": "aws", "model": "m6", "features_run": FEATURES_RUN, "drift_attempt": 2}],
        "the derived run of attempt 2 may be named": named_code == 0 and named.confs == api.confs,
        "the drift DAG without an attempt is still refused before any call": refused_quietly(start, "features DAG"),
        "attempt 1 is the features DAG's run and is refused before any call": refused_quietly([*start, "--drift-attempt", "1"], "features DAG"),
        "a drift attempt outside 2 to 99 is refused before any call": refused_quietly([*start, "--drift-attempt", "0"], "drift attempt")
        and refused_quietly([*start, "--drift-attempt", "100"], "drift attempt"),
        "a drift run ID that is not the attempt's derived run is refused before any call": refused_quietly(
            [*start, "--drift-attempt", "2", "--run-id", RUN], "derived"
        )
        and refused_quietly([*start, "--drift-attempt", "2", "--run-id", contract.model_drift_run_id(FEATURES_RUN)], "derived"),
        "a drift attempt needs a model of the contract and a features run ID": refused_quietly(
            ["--dag", DRIFT, "--mode", "real", "--features-run", FEATURES_RUN, "--drift-attempt", "2", "--wait-seconds", "5"], "model"
        )
        and refused_quietly(["--dag", DRIFT, "--mode", "real", "--model", "m6", "--drift-attempt", "2", "--wait-seconds", "5"], "features run")
        and refused_quietly([*start[:7], "latest", *start[8:], "--drift-attempt", "2"], "features run"),
        "a workflow that is not the drift check refuses a drift attempt before any call": refused_quietly(
            ["--dag", "training_dag", "--mode", "real", "--model", "m1", "--base-run", BASE, "--drift-attempt", "2", "--wait-seconds", "5"], "drift attempt"
        ),
        "a drift attempt is refused with a status, settle or clear action": all(
            invoke(Ends(), ["--dag", DRIFT, flag, "--run-id", second, "--drift-attempt", "2", "--wait-seconds", "5"])[0] == 2
            for flag in ("--status", "--settle", "--clear-failed")
        ),
    }


LIVE = "drift_live_check_dag"
LIVE_FORM = Path(__file__).resolve().parents[2] / "dags/drift_live_check_dag.py"


def live_check_cases() -> dict[str, bool]:
    """The on-demand live drift check is started by hand with a run ID, a model and an HAI type: the form of ``dags/drift_live_check_dag.py`` and no other
    option. Every refusal comes before the secret is read and before any Airflow call."""

    class Ends(StandIn):
        def run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
            record = super().run(dag_id, dag_run_id)
            self.runs[dag_run_id] = "success"
            return record

    start = ["--dag", LIVE, "--mode", "real", "--model", "m2", "--hai-type", HAI_TYPE, "--wait-seconds", "5"]
    api, named = Ends(), Ends()
    code, printed, _, _ = invoke(api, start)
    named_code = invoke(named, [*start, "--run-id", RUN])[0]
    declared = {
        key.value
        for node in ast.walk(ast.parse(LIVE_FORM.read_text()))
        if isinstance(node, ast.Dict)
        for key in node.keys
        if isinstance(key, ast.Constant) and key.value in {"run_id", "mode", "model", "hai_type"}
    }
    refused_with = {
        "--features-run": (FEATURES_RUN, "features run"),
        "--base-run": (BASE, "base run"),
        "--drift-attempt": ("2", "drift attempt"),
        "--deploy-mode": ("stable", "deploy"),
        "--research-run": (RESEARCH_RUN, "deploy"),
        "--model-version": ("7", "model version"),
        "--silver-snapshot-run": (SILVER, "silver"),
        "--gold-snapshot-run": (SILVER, "gold"),
        "--run-kind": ("monthly", "run kind"),
        "--capture-root": ("raw/", "capture root"),
        "--research-attempt": ("2", "research attempt"),
        "--population": ("sensitivity", "takes only"),
        "--mlflow": ("local", "takes only"),
        "--hai-types": ("HAI_6", "takes only"),
        "--trigger": ("drift:anything", "takes only"),
        "--promotion-mode": ("rollback", "takes only"),
    }
    return {
        "the live check triggers with the run, the mode, the model and the type and nothing else": code == 0
        and printed is not None
        and printed["state"] == "success"
        and api.confs == [{"run_id": first(api).get("run_id"), "mode": "real", "model": "m2", "hai_type": HAI_TYPE}]
        and re.fullmatch(trigger_dag.RUN_ID.pattern, str(first(api).get("run_id"))) is not None
        and re.fullmatch("drift_elastic_net_[a-f0-9]{8}", str(first(api).get("run_id"))) is not None,
        "a named run ID is used as given": named_code == 0 and named.confs == [{"run_id": RUN, "mode": "real", "model": "m2", "hai_type": HAI_TYPE}],
        "the conf keys are exactly the parameters of the DAG's form": set(first(api)) == declared == {"run_id", "mode", "model", "hai_type"},
        "a run ID that is not a run ID is refused before any call": refused_quietly([*start, "--run-id", "latest"], "run ID"),
        "a mode other than real is refused before any call": refused_quietly([*start[:3], "promote", *start[4:]], "mode"),
        "a live check without a model or without a type is refused before any call": refused_quietly([*start[:4], *start[6:]], "live drift check")
        and refused_quietly([*start[:6], *start[8:]], "live drift check"),
        "a model or a type outside the contract is stopped by the parser": invoke(Ends(), [*start[:5], "m7", *start[6:]])[0] == 2
        and invoke(Ends(), [*start[:7], "HAI_9", *start[8:]])[0] == 2,
        "every option of another workflow is refused before any call": all(
            refused_quietly([*start, flag, value], words) for flag, (value, words) in refused_with.items()
        ),
        "the status of a live run can be read": invoke(StandIn({RUN: "success"}), ["--dag", LIVE, "--status", "--run-id", RUN, "--wait-seconds", "5"])[0] == 0,
    }


PIPELINE_RUN = "pipeline_0a1b2c3d"
BRONZE_RUN = "bronze_0a1b2c3d"
BRONZE = "bronze_dag"
RETRY = "ingestion_retry_dag"
ORIGINAL = "ingestion_0a1b2c3d"
ORIGINAL_ATTEMPT = "ingestion_0a1b2c3d_2"
RETRY_RUN = "ingestion_retry_0a1b2c3d_2"


def bronze_cases() -> dict[str, bool]:
    """``bronze_dag`` is started by hand with a bronze run ID (a new one when absent) and no other option; its status, settle and clear-failed take its
    runs, and it is activated alone (a hand start of one DAG activates only that DAG)."""
    base = ["--dag", BRONZE, "--mode", "real", "--wait-seconds", "5"]

    def started(*extra: str) -> tuple[int, Ends]:
        api = Ends()
        return invoke(api, [*base, *extra])[0], api

    plain_code, plain = started()
    named_code, named = started("--run-id", BRONZE_RUN)
    made = plain.confs[0]["run_id"] if plain.confs else ""
    clearable = Clearable({BRONZE_RUN: "failed"}, {"bronze_stage": "success", "bronze_load": "failed"})
    cleared = invoke(clearable, ["--dag", BRONZE, "--clear-failed", "--run-id", BRONZE_RUN, "--wait-seconds", "5"])
    settling = StandIn({BRONZE_RUN: "running"})
    settled = invoke(settling, ["--dag", BRONZE, "--settle", "--run-id", BRONZE_RUN, "--wait-seconds", "5"])
    return {
        "a start with no run ID makes a bronze run ID and sends the mode only": plain_code == 0
        and re.fullmatch(r"bronze_[a-f0-9]{8}", made) is not None
        and plain.confs == [{"run_id": made, "mode": "real"}],
        "a named bronze run is sent as given": named_code == 0 and named.confs == [{"run_id": BRONZE_RUN, "mode": "real"}],
        "a hand start activates the bronze DAG alone": plain.paused == {BRONZE: False},
        "a run ID of another name is refused before any call": all(
            refused_quietly([*base, "--run-id", bad], "run ID")
            for bad in ("silver_0a1b2c3d", "ingestion_0a1b2c3d", "pipeline_0a1b2c3d", "latest", "bronze_0a1b2c3", "bronze_0a1b2c3d_1")
        ),
        "an option of another workflow is refused before any call": all(
            refused_quietly([*base, flag, value], words)
            for flag, value, words in (
                ("--silver-snapshot-run", SILVER, "silver snapshot"),
                ("--start-from", "bronze", "start"),
                ("--run-kind", "monthly", "run kind"),
            )
        ),
        "the status of a bronze run can be read": invoke(
            StandIn({BRONZE_RUN: "success"}), ["--dag", BRONZE, "--status", "--run-id", BRONZE_RUN, "--wait-seconds", "5"]
        )[0]
        == 0,
        "settle marks a running bronze run failed": settled[0] == 0 and settling.runs[BRONZE_RUN] == "failed",
        "clear-failed runs the failed task of a failed bronze run again": cleared[0] == 0 and clearable.cleared == [(BRONZE, BRONZE_RUN, ["bronze_load"])],
    }


def activation_cases() -> dict[str, bool]:
    """A hand start of the first DAG activates the whole inventory of the pipeline first (a fresh deployment creates every DAG paused), the children before the
    first DAG, and names the DAG that cannot be activated; no run is created then."""
    inventory = list(PIPELINE_ACTIVE_DAGS)
    base = ["--dag", PIPELINE_DAG, "--mode", "real", "--wait-seconds", "5"]
    order: list[str] = []

    class Recording(Ends):
        def unpause(self, dag_id: str) -> None:
            order.append(dag_id)
            super().unpause(dag_id)

    class Missing(Ends):
        def unpause(self, dag_id: str) -> None:
            if dag_id == "silver_transform_dag":
                raise AirflowApiError("GET /dags/silver_transform_dag: status 404: not found")
            super().unpause(dag_id)

    api = Recording()
    api.paused = {dag: True for dag in inventory}
    code = invoke(api, base)[0]
    missing = Missing()
    miss_code, printed, error, _ = invoke(missing, base)
    return {
        "every DAG of the inventory is unpaused before the run is created": code == 0
        and api.paused == {dag: False for dag in inventory}
        and api.calls[: len(inventory) + 1] == ["unpause"] * len(inventory) + ["trigger"],
        "the children are activated before the first DAG": order[-1] == PIPELINE_DAG and sorted(order) == sorted(inventory),
        "a DAG that cannot be activated is named and nothing is created": miss_code == 1
        and printed is None
        and "silver_transform_dag" in error
        and "cannot be activated" in error
        and "trigger" not in missing.calls,
        "a hand start of another workflow activates that workflow alone": invoke(
            Ends(), ["--dag", DAG, "--mode", "real", "--run-id", RUN, "--silver-snapshot-run", SILVER, "--wait-seconds", "5"]
        )[0]
        == 0,
    }


def pipeline_cases() -> dict[str, bool]:
    """The first DAG is started by hand with a ``pipeline_<hex>`` run ID (a new one when absent), a validated ``--start-from`` stage and, for its ingestion
    stage, the run kind and capture root that ``ingestion_dag`` takes. Every refusal comes before the secret is read and before any Airflow call."""
    base = ["--dag", PIPELINE_DAG, "--mode", "real", "--wait-seconds", "5"]

    def started(*extra: str) -> tuple[int, Ends]:
        api = Ends()
        return invoke(api, [*base, *extra])[0], api

    plain_code, plain = started()
    named_code, named = started("--run-id", PIPELINE_RUN, "--start-from", "silver")
    kind_code, kind = started("--run-id", PIPELINE_RUN, "--run-kind", "monthly", "--capture-root", "ingestion_test/ingestion_0a1b2c3d/")
    stages = PIPELINE_STAGES
    each = {stage: started("--run-id", PIPELINE_RUN, "--start-from", stage) for stage in stages}
    made = plain.confs[0]["run_id"] if plain.confs else ""
    return {
        "a start with no run ID makes a pipeline run ID and sends the mode only": plain_code == 0
        and re.fullmatch(r"pipeline_[a-f0-9]{8}", made) is not None
        and plain.confs == [{"run_id": made, "mode": "real"}],
        "a named run and a later start are sent as given": named_code == 0
        and named.confs == [{"run_id": PIPELINE_RUN, "mode": "real", "start_from": "silver"}],
        "a run kind and a capture root reach the conf": kind_code == 0
        and kind.confs == [{"run_id": PIPELINE_RUN, "mode": "real", "run_kind": "monthly", "capture_root": "ingestion_test/ingestion_0a1b2c3d/"}],
        "every stage of the contract is a start the trigger takes": all(code == 0 for code, _ in each.values()) and len(each) == len(stages) == 5,
        "a stage outside the contract is stopped by the parser": all(
            invoke(Ends(), [*base, "--start-from", bad])[0] == 2 for bad in ("metadata", "training", "silver_transform_dag", "")
        ),
        "a run ID of another name is refused before any call": all(
            refused_quietly([*base, "--run-id", bad], "run ID")
            for bad in ("silver_0a1b2c3d", "bronze_0a1b2c3d", "ingestion_0a1b2c3d", "latest", "pipeline_0a1b2c3")
        ),
        "an option of another workflow is refused before any call": refused_quietly([*base, "--silver-snapshot-run", SILVER], "silver snapshot"),
        "a start stage on another workflow is refused before any call": refused_quietly(
            ["--dag", DAG, "--mode", "real", "--run-id", RUN, "--silver-snapshot-run", SILVER, "--wait-seconds", "5", "--start-from", "bronze"], "start"
        ),
        "a refused kind names the preflight's reason": refused_quietly([*base, "--run-kind", "weekly"], "publisher_request_has_wrong_kind"),
        "the status of a pipeline run can be read": invoke(
            StandIn({PIPELINE_RUN: "success"}), ["--dag", PIPELINE_DAG, "--status", "--run-id", PIPELINE_RUN, "--wait-seconds", "5"]
        )[0]
        == 0,
        "status with a start stage is refused": invoke(
            StandIn({PIPELINE_RUN: "queued"}), ["--dag", PIPELINE_DAG, "--status", "--run-id", PIPELINE_RUN, "--wait-seconds", "5", "--start-from", "bronze"]
        )[0]
        == 2,
    }


FAILED_RETRY_TASKS: dict[str, str | None] = {
    "preflight": "failed",
    "job": "upstream_failed",
    "evidence": "upstream_failed",
    "gate": "failed",
    "start_pipeline": "failed",
}


class EndsFailed(Clearable):
    """A stand-in whose runs end in failure at the first poll, with the task states of a retry day whose preflight failed."""

    def __init__(self, runs: dict[str, str] | None = None) -> None:
        super().__init__(runs, FAILED_RETRY_TASKS)

    def run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
        record = super().run(dag_id, dag_run_id)
        self.runs[dag_run_id] = "failed"
        return record


def retry_cases() -> dict[str, bool]:
    """``ingestion_retry_dag`` is started by hand with ``--original-run`` (an ingestion run ID) and ``--day`` (2 or 3), which its form takes in place of a
    run ID. The retry's own run ID derives from the two, so ``--run-id`` may name only that run; the capture root is the only other option. Every refusal
    comes before the secret is read and before any Airflow call, and the conf carries the form's parameters and no run ID."""
    base = ["--dag", RETRY, "--mode", "real", "--wait-seconds", "5"]

    def started(*extra: str) -> tuple[int, Ends]:
        api = Ends()
        return invoke(api, [*base, *extra])[0], api

    day2_code, day2 = started("--original-run", ORIGINAL, "--day", "2")
    day3_code, day3 = started("--original-run", ORIGINAL_ATTEMPT, "--day", "3")
    root_code, root = started("--original-run", ORIGINAL, "--day", "2", "--capture-root", TEST_ROOT)
    named_code, named = started("--original-run", ORIGINAL, "--day", "2", "--run-id", RETRY_RUN)
    printed = invoke(Ends(), [*base, "--original-run", ORIGINAL, "--day", "2"])[1]
    failing = EndsFailed()
    failed_code, failed_printed, _, _ = invoke(failing, [*base, "--original-run", ORIGINAL, "--day", "2"])
    clearing = EndsFailed({RETRY_RUN: "failed"})
    cleared_code = invoke(clearing, ["--dag", RETRY, "--clear-failed", "--run-id", RETRY_RUN, "--wait-seconds", "5"])[0]
    return {
        "a retry that ends failed exits 1 and prints the failed state and the failed tasks": failed_code == 1
        and failed_printed is not None
        and failed_printed["state"] == "failed"
        and failed_printed["tasks"] == FAILED_RETRY_TASKS,
        "clear-failed runs the failed tasks of a failed retry again": cleared_code == 0
        and clearing.cleared == [(RETRY, RETRY_RUN, ["evidence", "gate", "job", "preflight", "start_pipeline"])],
        "a retry start sends the original run, the day and the mode, and no run ID": day2_code == 0
        and day2.confs == [{"original_run": ORIGINAL, "day": 2, "mode": "real"}],
        "the retry runs under the run ID that its original run and day give": list(day2.runs) == [RETRY_RUN]
        and printed is not None
        and printed["run_id"] == RETRY_RUN
        and printed["dag"] == RETRY,
        "a later attempt of the original run puts its suffix before day 3": day3_code == 0
        and day3.confs == [{"original_run": ORIGINAL_ATTEMPT, "day": 3, "mode": "real"}]
        and list(day3.runs) == ["ingestion_retry_0a1b2c3d_2_3"],
        "a capture root reaches the conf": root_code == 0 and root.confs == [{"original_run": ORIGINAL, "day": 2, "mode": "real", "capture_root": TEST_ROOT}],
        "a run ID that is the derived run is accepted": named_code == 0 and list(named.runs) == [RETRY_RUN],
        "a hand start activates the retry DAG alone": day2.paused == {RETRY: False},
        "a retry without its original run or its day is refused before any call": all(
            refused_quietly([*base, *extra], words)
            for extra, words in ((["--day", "2"], "--original-run"), (["--original-run", ORIGINAL], "--day"), ([], "--original-run"))
        ),
        "an original run that is not an ingestion run ID is refused before any call": all(
            refused_quietly([*base, "--original-run", bad, "--day", "2"], "original run ID")
            for bad in ("latest", "bronze_0a1b2c3d", "pipeline_0a1b2c3d", "ingestion_retry_0a1b2c3d_2", "ingestion_0a1b2c3", "scheduled")
        ),
        "a day outside days 2 and 3 is refused before any call": all(
            refused_quietly([*base, "--original-run", ORIGINAL, "--day", bad], "retry day") for bad in ("0", "1", "4", "31", "-2")
        ),
        "a day that is not a whole number is stopped by the parser": invoke(Ends(), [*base, "--original-run", ORIGINAL, "--day", "2.5"])[0] == 2,
        "a run ID that is not the derived run is refused before any call": all(
            refused_quietly([*base, "--original-run", ORIGINAL, "--day", "2", "--run-id", bad], "derived run")
            for bad in ("ingestion_retry_0a1b2c3d_3", "ingestion_0a1b2c3d", "ingestion_retry_ffffffff_2")
        ),
        "a run kind is refused before any call": refused_quietly([*base, "--original-run", ORIGINAL, "--day", "2", "--run-kind", "retry"], "run kind"),
        "a capture root outside the form is refused before any call": all(
            refused_quietly([*base, "--original-run", ORIGINAL, "--day", "2", "--capture-root", bad], "publisher_request_has_wrong_capture_root")
            for bad in ("raw", "other/", "ingestion_test/latest/")
        ),
        "the status of a retry run can be read": invoke(
            StandIn({RETRY_RUN: "success"}), ["--dag", RETRY, "--status", "--run-id", RETRY_RUN, "--wait-seconds", "5"]
        )[0]
        == 0,
        "status with a day or an original run is refused": all(
            invoke(StandIn({RETRY_RUN: "queued"}), ["--dag", RETRY, "--status", "--run-id", RETRY_RUN, "--wait-seconds", "5", *extra])[0] == 2
            for extra in (["--day", "2"], ["--original-run", ORIGINAL])
        ),
    }


FLAG_VALUES = {
    "--start-from": "bronze",
    "--run-id": RUN,
    "--silver-snapshot-run": SILVER,
    "--gold-snapshot-run": SILVER,
    "--run-kind": "monthly",
    "--capture-root": "raw/",
    "--base-run": BASE,
    "--features-run": FEATURES_RUN,
    "--research-attempt": "2",
    "--drift-attempt": "2",
    "--population": "sensitivity",
    "--hai-types": "HAI_6",
    "--trigger": "drift:anything",
    "--mlflow": "aws",
    "--model": "m2",
    "--hai-type": HAI_TYPE,
    "--model-version": "7",
    "--promotion-mode": "rollback",
    "--deploy-mode": "stable",
    "--research-run": RESEARCH_RUN,
    "--original-run": ORIGINAL,
    "--day": "2",
}
# What each workflow's form takes besides the mode and the wait: the contract, written out here and not read from the command.
ACCEPTED_FLAGS = {
    "bronze_dag": {"--run-id"},
    "pipeline_dag": {"--run-id", "--start-from", "--run-kind", "--capture-root"},
    "ingestion_dag": {"--run-id", "--run-kind", "--capture-root"},
    "ingestion_retry_dag": {"--run-id", "--original-run", "--day", "--capture-root"},
    "silver_transform_dag": {"--run-id"},
    "gold_transform_dag": {"--run-id", "--silver-snapshot-run"},
    "features_base_dag": {"--run-id", "--gold-snapshot-run"},
    "training_dag": {"--run-id", "--base-run", "--features-run", "--population", "--hai-types", "--trigger", "--mlflow", "--model"},
    "research_dag": {"--run-id", "--base-run", "--research-attempt", "--population", "--hai-types", "--mlflow"},
    "model_promotion_dag": {"--run-id", "--hai-type", "--model-version", "--promotion-mode", "--mlflow"},
    "serving_deploy_dag": {"--run-id", "--hai-type", "--deploy-mode", "--research-run"},
    "drift_live_check_dag": {"--run-id", "--model", "--hai-type"},
    "drift_monitoring_dag": {"--run-id", "--model", "--features-run", "--drift-attempt", "--mlflow"},
}
REQUIRED_FLAGS = {
    "ingestion_retry_dag": ("--original-run", "--day"),
    "gold_transform_dag": ("--silver-snapshot-run",),
    "features_base_dag": ("--gold-snapshot-run",),
    "training_dag": ("--model",),
    "research_dag": ("--base-run",),
    "model_promotion_dag": ("--hai-type", "--model-version"),
    "serving_deploy_dag": ("--hai-type", "--deploy-mode"),
    "drift_live_check_dag": ("--model", "--hai-type"),
    "drift_monitoring_dag": ("--model", "--features-run", "--drift-attempt"),
}


def every_workflow_takes_only_its_options() -> dict[str, bool]:
    """An option that a workflow's form does not take is refused when it is given, whatever its value and even when it equals the default, before the secret
    is read and before any Airflow call. The table of accepted options is the contract of the forms, and each workflow is tried with each option outside it."""
    results = {}
    for dag, accepted in ACCEPTED_FLAGS.items():
        needed = REQUIRED_FLAGS.get(dag, ())
        base = ["--dag", dag, "--mode", "real", *[part for flag in needed for part in (flag, FLAG_VALUES[flag])], "--wait-seconds", "5"]
        outside = sorted(set(FLAG_VALUES) - accepted - set(needed))
        refused = []
        for flag in outside:
            api = Ends()
            code, printed, _, _ = invoke(api, [*base, flag, FLAG_VALUES[flag]])
            refused.append(code == 1 and printed is None and not api.calls and api.token_reads == 0)
        results[f"{dag} refuses each of its {len(outside)} unsupported options before any call"] = bool(outside) and all(refused)
    return results


GROUPS: dict[str, Callable[[], dict[str, bool]]] = {
    "lost connection": lost_after_create,
    "status": status_cases,
    "settle": settle_cases,
    "clear": clear_cases,
    "parse": parse_cases,
    "trigger": trigger_cases,
    "publisher request": publisher_cases,
    "base run": base_run_cases,
    "drift attempt": drift_attempt_cases,
    "live check": live_check_cases,
    "pipeline": pipeline_cases,
    "bronze": bronze_cases,
    "retry": retry_cases,
    "activation": activation_cases,
    "accepted options": every_workflow_takes_only_its_options,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--output", type=Path, required=True, help="the report file to write")
    arguments = parser.parse_args()
    results = {name: group() for name, group in GROUPS.items()}
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    failed = [f"{group}: {name}" for group, checks in results.items() for name, ok in checks.items() if not ok]
    for line in failed:
        sys.stdout.write(f"FAIL {line}\n")
    total = sum(len(checks) for checks in results.values())
    sys.stdout.write(f"trigger_dag cases: {total - len(failed)} of {total} checks passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
