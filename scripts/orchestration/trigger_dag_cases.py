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
import shutil
import sys
import tempfile
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
    with (
        mock.patch.object(trigger_dag, "read_token_secret", lambda deadline: "SYNTHETIC-secret"),
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


SETTLE_DAG = "launch_settle_dag"
STATEMENT = "aws_tasks list names no job task and ECS shows the task STOPPED"


def settle_dag_cases() -> dict[str, bool]:
    """The owner's settle DAG: its run carries the launch's run and job and the owner's statement, each checked before the secret is read."""

    class Ends(StandIn):
        def run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
            record = super().run(dag_id, dag_run_id)
            self.runs[dag_run_id] = "success"
            return record

    posted: list[dict[str, Any]] = []

    class Records(Ends):
        def trigger(self, dag_id: str, dag_run_id: str, conf: dict[str, Any]) -> dict[str, Any]:
            posted.append({"dag_id": dag_id, "dag_run_id": dag_run_id, "conf": conf})
            return super().trigger(dag_id, dag_run_id, conf)

    good = ["--dag", SETTLE_DAG, "--mode", "real", "--launch-run", SILVER, "--job", "silver", "--statement", STATEMENT, "--wait-seconds", "5"]
    api = Records()
    code, printed, _, _ = invoke(api, good)
    conf = posted[0]["conf"] if posted else {}
    checks = {
        "the settle DAG run carries the launch run, the job and the statement in its own new run": code == 0
        and printed is not None
        and printed["state"] == "success"
        and conf.get("launch_run") == SILVER
        and conf.get("job") == "silver"
        and conf.get("statement") == STATEMENT
        and conf.get("mode") == "real"
        and conf.get("run_id") not in (None, SILVER)
        and posted[0]["dag_run_id"] == conf.get("run_id")
    }

    def refused(arguments: list[str]) -> bool:
        api = StandIn()
        code, _, error, _ = invoke(api, arguments)
        return code in (1, 2) and not api.calls and (code == 2 or "refused" in error)

    def without(name: str) -> list[str]:
        index = good.index(name)
        return good[:index] + good[index + 2 :]

    def replaced(name: str, value: str) -> list[str]:
        index = good.index(name)
        return [*good[: index + 1], value, *good[index + 2 :]]

    checks |= {
        "a settle without the launch run is refused before any request": refused(without("--launch-run")),
        "a settle without the job is refused before any request": refused(without("--job")),
        "a settle without a statement is refused before any request": refused(without("--statement")),
        "a launch run that is not a run ID is refused": refused(replaced("--launch-run", "run-1")),
        "a job the job table does not name is refused": refused(replaced("--job", "silverx")),
        "a statement shorter than the launch library's minimum is refused": refused(replaced("--statement", "short")),
        "a statement of more than one line is refused": refused(replaced("--statement", STATEMENT + "\nsecond line")),
        "a settle in fixture mode is refused": refused(replaced("--mode", "publish_fixture")),
        "the settle fields on another DAG are refused": refused(
            ["--dag", DAG, "--mode", "real", "--silver-snapshot-run", SILVER, "--job", "silver", "--wait-seconds", "5"]
        ),
    }
    return checks


def publisher_recovery_cases() -> dict[str, bool]:
    """The owner list's recovery of an unfinished publisher launch: settle it through launch_settle_dag, then start a fresh ingestion run. The publisher
    job is a job of the direct launch only (config/orchestration/direct_jobs.json), so the broker's table still does not name it."""
    posted: list[dict[str, Any]] = []

    class Records(StandIn):
        def run(self, dag_id: str, dag_run_id: str) -> dict[str, Any]:
            record = super().run(dag_id, dag_run_id)
            self.runs[dag_run_id] = "success"
            return record

        def trigger(self, dag_id: str, dag_run_id: str, conf: dict[str, Any]) -> dict[str, Any]:
            posted.append({"dag_id": dag_id, "conf": conf})
            return super().trigger(dag_id, dag_run_id, conf)

    settle = ["--dag", SETTLE_DAG, "--mode", "real", "--launch-run", SILVER, "--job", "publisher", "--statement", STATEMENT, "--wait-seconds", "5"]
    settled = invoke(Records(), settle)[0]
    fresh = invoke(Records(), ["--dag", "ingestion_dag", "--mode", "real", "--run-id", RUN, "--wait-seconds", "5"])[0]
    broker = json.loads((trigger_dag.REPO_ROOT / "config/orchestration/ecs_broker_jobs.json").read_text())["jobs"]
    return {
        "an unfinished publisher launch is settled through the settle DAG": settled == 0
        and bool(posted)
        and posted[0]["dag_id"] == SETTLE_DAG
        and posted[0]["conf"].get("job") == "publisher",
        "a fresh ingestion run starts after the settle": fresh == 0 and len(posted) == 2 and posted[1]["dag_id"] == "ingestion_dag",
        "the broker's job table still does not name the publisher": "publisher" not in broker,
    }


def broken_table_cases() -> dict[str, bool]:
    """A settle refuses, with no API call and no secret read, when the direct launch's job table is missing or is not JSON: the owner sees a refusal,
    not a traceback."""
    settle = ["--dag", SETTLE_DAG, "--mode", "real", "--launch-run", SILVER, "--job", "publisher", "--statement", STATEMENT, "--wait-seconds", "5"]

    def refused_with(damage: Callable[[Path], object]) -> bool:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            shutil.copytree(trigger_dag.REPO_ROOT / "config/orchestration", root / "config/orchestration")
            damage(root / "config/orchestration/direct_jobs.json")
            api = StandIn()
            with mock.patch.object(trigger_dag, "REPO_ROOT", root):
                try:
                    code, _, error, _ = invoke(api, settle)
                except Exception:  # an exception that escapes main is the traceback this case refuses
                    return False
            return code == 1 and not api.calls and "refused" in error and "Traceback" not in error

    return {
        "a missing direct job table is a refusal before any request": refused_with(lambda path: path.unlink()),
        "a direct job table that is not JSON is a refusal before any request": refused_with(lambda path: path.write_text("{not json")),
    }


GROUPS: dict[str, Callable[[], dict[str, bool]]] = {
    "lost connection": lost_after_create,
    "status": status_cases,
    "settle": settle_cases,
    "parse": parse_cases,
    "trigger": trigger_cases,
    "settle DAG": settle_dag_cases,
    "publisher recovery": publisher_recovery_cases,
    "broken job table": broken_table_cases,
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
