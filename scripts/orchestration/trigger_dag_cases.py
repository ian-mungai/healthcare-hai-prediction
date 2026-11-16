"""Cases of ``scripts.orchestration.trigger_dag``: the trigger, and the bounded ``--status`` and ``--settle`` of one run, against an Airflow stand-in.

    .venv/bin/python -B -m scripts.orchestration.trigger_dag_cases --output <report.json>

A trigger creates the Airflow run before it polls, so a lost connection after the create leaves a queued run that the scheduler starts later. The owner
then reads the run's state (``--status``) and settles it (``--settle``): the DAG is paused and a run that has not ended is marked failed, then read again
until it is terminal, all within ``--wait-seconds``. An absent or already ended run is left as it is. Every case runs ``main`` with the token secret and the
API replaced by a stand-in that keeps the runs in memory; nothing reaches Airflow or AWS.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

from scripts.orchestration import trigger_dag
from scripts.orchestration.airflow_api import AirflowApiError

DAG = "gold_transform_dag"
RUN = "20261110T120000Z-abcdef12"
SILVER = "20261110T100000Z-12345678"


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


TEST_ROOT = "ingestion_test/20261113T100000Z-0a1b2c3d/"


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


BASE = "20261110T010000Z-0b5e0001"
RESEARCH_RUN = "20261110T010000Z-5e5ea4c1"
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


GROUPS: dict[str, Callable[[], dict[str, bool]]] = {
    "lost connection": lost_after_create,
    "status": status_cases,
    "settle": settle_cases,
    "parse": parse_cases,
    "trigger": trigger_cases,
    "publisher request": publisher_cases,
    "base run": base_run_cases,
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
