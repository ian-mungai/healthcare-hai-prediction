"""Host session cases through the real entry point: ``python -m scripts.infrastructure.host_session <command>`` in a child process.

The in-process cases import ``host_session`` as ``scripts.infrastructure.host_session``. The command line runs the same file as ``__main__``, and
``aws_tasks`` then imports a second copy of it, so a class that the two copies each define is two classes. These cases start the command as it is
started and put the AWS stand-in under it with ``host_session_entry_stand_in`` (a ``sitecustomize.py`` that replaces only the configuration, the Terraform
outputs, the ``aws`` program and the clock). The child's exit code, output and end state are the evidence. The plants harness sets
``HOST_SESSION_ENTRY_SOURCE``, which the child inherits, so the child loads the planted source.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.infrastructure.host_session_entry_stand_in import RESULT, SCENARIO
from scripts.process import CompletedProcess, run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
READER = "stand_in_dev_job_read_silver"
SITE = "from scripts.infrastructure.host_session_entry_stand_in import install\n\ninstall()\n"
CHILD_SECONDS = 120


def entry(
    *arguments: str, faults: tuple[str, ...] = (), tasks: tuple[dict[str, Any], ...] = (), host_on: bool = True
) -> tuple[CompletedProcess[str], dict[str, Any]]:
    """Run ``python -m scripts.infrastructure.host_session`` with ``arguments`` against the stand-in; the answer is the finished child and its end state
    (empty when the child wrote none)."""
    with tempfile.TemporaryDirectory(prefix="host_session_entry_") as directory:
        root = Path(directory)
        (root / "sitecustomize.py").write_text(SITE, encoding="utf-8")
        environment = {
            **os.environ,
            "PYTHONPATH": f"{root}{os.pathsep}{REPO_ROOT}",
            SCENARIO: json.dumps({"faults": list(faults), "tasks": list(tasks), "host_on": host_on}),
            RESULT: str(root / "result.json"),
        }
        finished = run_command(sys.executable, ["-m", "scripts.infrastructure.host_session", *arguments], cwd=REPO_ROOT, env=environment, timeout=CHILD_SECONDS)
        state = json.loads((root / "result.json").read_text(encoding="utf-8")) if (root / "result.json").exists() else {}
    return finished, state


def host_is_on(state: dict[str, Any]) -> bool:
    return state.get("desired") == 1 and state.get("instance_up") is True


def host_is_off(state: dict[str, Any]) -> bool:
    return state.get("desired") == 0 and state.get("instance_up") is False


def clean(finished: CompletedProcess[str]) -> bool:
    """True when the command ended without a Python traceback: every failure is a named line, never an escaped exception."""
    return "Traceback" not in finished.stderr and "Traceback" not in finished.stdout


def stop_without_a_job_task_turns_the_host_off() -> dict[str, bool]:
    finished, state = entry("stop")
    return {
        "the command exits 0": finished.returncode == 0,
        "the check says that no job task runs": "job_tasks: no job task is running" in finished.stdout,
        "the host is off and verified": host_is_off(state) and "host_off_verified" in finished.stdout,
    }


def stop_refuses_while_a_job_task_runs() -> dict[str, bool]:
    finished, state = entry("stop", tasks=({"family": READER, "ends_at": None},))
    return {
        "the command exits 1 with a named refusal": finished.returncode == 1 and "stop failed: HostSessionError stop refused" in finished.stderr,
        "the refusal names the task family": READER in finished.stderr,
        "no traceback": clean(finished),
        "nothing was changed and the host is on": host_is_on(state) and not state.get("writes"),
    }


def stop_waits_for_a_job_task_and_then_stops() -> dict[str, bool]:
    finished, state = entry("stop", "--wait-seconds", "120", tasks=({"family": READER, "ends_at": 40},))
    return {
        "the command exits 0": finished.returncode == 0 and clean(finished),
        "the task is printed and the wait reported": READER in finished.stdout and "stop_waiting" in finished.stdout,
        "the host is off and verified": host_is_off(state) and "host_off_verified" in finished.stdout,
    }


def stop_retries_a_transient_describe_failure() -> dict[str, bool]:
    finished, state = entry("stop", "--wait-seconds", "60", faults=("describe_tasks_failure:1",), tasks=({"family": READER, "ends_at": 20},))
    return {
        "the command exits 0": finished.returncode == 0,
        "no traceback": clean(finished),
        "the stop went on after the failed listing and the task ended": host_is_off(state) and "host_off_verified" in finished.stdout,
    }


def stop_refuses_cleanly_when_the_listing_fails() -> dict[str, bool]:
    checks: dict[str, bool] = {}
    three = tuple({"family": READER, "ends_at": None} for _ in range(3))
    for label, arguments in (("without a wait", ()), ("with a wait", ("--wait-seconds", "60"))):
        finished, state = entry("stop", *arguments, faults=("page_size:1", "repeat_token"), tasks=three)
        checks |= {
            f"{label}: a repeated page token exits 1 with a named refusal": finished.returncode == 1
            and "stop refused" in finished.stderr
            and "could not be listed" in finished.stderr
            and "repeated a page token" in finished.stderr,
            f"{label}: a repeated page token ends in no traceback": clean(finished),
            f"{label}: a repeated page token changes nothing and the host is on": host_is_on(state) and not state.get("writes"),
        }
    finished, state = entry("stop", faults=("describe_tasks_failure:1",), tasks=({"family": READER, "ends_at": None},))
    checks |= {
        "without a wait, one failed describe refuses with a named reason": finished.returncode == 1
        and "stop refused" in finished.stderr
        and "could not be listed" in finished.stderr
        and "describe-tasks reported failures" in finished.stderr,
        "without a wait, one failed describe ends in no traceback": clean(finished),
        "without a wait, one failed describe changes nothing": host_is_on(state) and not state.get("writes"),
    }
    return checks


def stop_force_skips_the_check() -> dict[str, bool]:
    finished, state = entry("stop", "--force-stop", tasks=({"family": READER, "ends_at": None},))
    return {
        "the command exits 0": finished.returncode == 0,
        "the skipped check is reported": "stop_forced" in finished.stdout and "--force-stop" in finished.stdout,
        "the host is off and verified": host_is_off(state) and "host_off_verified" in finished.stdout,
    }


def refresh_names_a_listing_failure_instead_of_a_traceback() -> dict[str, bool]:
    finished, state = entry("refresh", faults=("describe_tasks_failure:1",), tasks=({"family": READER, "ends_at": None},))
    return {
        "the command exits 1": finished.returncode == 1,
        "the failure is a named line": "refresh failed:" in finished.stderr and "describe-tasks reported failures" in finished.stderr,
        "no traceback": clean(finished),
        "nothing was changed": host_is_on(state) and not state.get("writes"),
    }


CASES: dict[str, Callable[[], dict[str, bool]]] = {
    "entry_stop_without_a_job_task_turns_the_host_off": stop_without_a_job_task_turns_the_host_off,
    "entry_stop_refuses_while_a_job_task_runs": stop_refuses_while_a_job_task_runs,
    "entry_stop_waits_for_a_job_task_and_then_stops": stop_waits_for_a_job_task_and_then_stops,
    "entry_stop_retries_a_transient_describe_failure": stop_retries_a_transient_describe_failure,
    "entry_stop_refuses_cleanly_when_the_listing_fails": stop_refuses_cleanly_when_the_listing_fails,
    "entry_stop_force_skips_the_check": stop_force_skips_the_check,
    "entry_refresh_names_a_listing_failure_instead_of_a_traceback": refresh_names_a_listing_failure_instead_of_a_traceback,
}
