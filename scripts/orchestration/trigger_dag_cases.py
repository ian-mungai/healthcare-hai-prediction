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
    kinds = ("bootstrap", "monthly", "retry", "", "Monthly", "monthly\n", " bootstrap")
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
    code, _, error, _ = invoke(StandIn(), [*base, "--run-kind", "retry"])
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


GROUPS: dict[str, Callable[[], dict[str, bool]]] = {
    "lost connection": lost_after_create,
    "status": status_cases,
    "settle": settle_cases,
    "parse": parse_cases,
    "trigger": trigger_cases,
    "publisher request": publisher_cases,
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
