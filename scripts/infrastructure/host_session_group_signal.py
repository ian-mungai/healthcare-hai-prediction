"""A terminal Ctrl-C during the host-off request, with a real process and a real AWS command stand-in.

A terminal sends SIGINT to the whole foreground process group, not only to the session. This module forks a driver into its own process group, runs the
real ``Session.stop`` through ``AwsCli`` and the real runner, and replaces ``aws`` with a small Python program that applies a capacity change only
after a short in-flight window. The case sends SIGINT to the whole group during that window and checks that the host still reaches capacity zero and is
verified gone, within a bounded deadline.

The stand-in is ``host_session_fake_aws.py``; it imports nothing from the repository, so it runs with ``python -I``.
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import sys
import tempfile
import time
from pathlib import Path
from types import ModuleType

from scripts.acquisition.s3_store import AwsCli
from scripts.infrastructure import host_session_fake_aws as fake_aws
from scripts.infrastructure.host_session_fake_aws import GROUP, STATE_ENV

DEADLINE_SECONDS = 30.0
POLL_SECONDS = 0.02


def drive_stop(module: ModuleType, root: Path, bin_directory: Path) -> int:
    """In the forked driver: put the stand-in first on PATH, run the real ``stop`` and write its report lines under ``root``. Returns the exit code."""
    os.setsid()
    os.environ["PATH"] = f"{bin_directory}{os.pathsep}{os.environ['PATH']}"
    os.environ[STATE_ENV] = str(root)

    def report(line: str) -> None:
        with (root / "report.txt").open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    names = module.Names("group-signal-cluster", GROUP, "group-signal-provider", "group-signal-project", {"postgres": "group-signal-postgres"}, {})
    cli = AwsCli({"aws_profile": "group-signal-profile", "aws_region": "group-signal-region"}, module.tolerant_runner)
    try:
        module.Session(cli, names, module.Clock(), root / "marker", report).stop()
    except (module.HostSessionError, KeyboardInterrupt, module.TerminationRequested) as error:
        (root / "failure.txt").write_text(type(error).__name__, encoding="utf-8")
        return 1
    return 0


def wait_for(path: Path, pid: int, deadline: float) -> bool:
    """True when ``path`` exists before ``deadline`` and the driver has not exited."""
    while time.monotonic() < deadline:
        if path.exists():
            return True
        if os.waitpid(pid, os.WNOHANG) != (0, 0):
            return False
        time.sleep(POLL_SECONDS)
    return False


def run(module: ModuleType) -> dict[str, bool]:
    """Send SIGINT to the whole process group of a running ``stop`` during its capacity-zero request and check the outcome."""
    with tempfile.TemporaryDirectory(prefix="host_session_group_") as directory:
        root = Path(directory)
        bin_directory = root / "bin"
        bin_directory.mkdir()
        shim = bin_directory / "aws"
        shim.write_text(f'#!/bin/sh\nexec "{sys.executable}" -I "{Path(fake_aws.__file__).resolve()}" "$@"\n', encoding="utf-8")
        shim.chmod(0o755)
        (root / "group.json").write_text(json.dumps({"desired": 1, "instances": ["i-stand-in"]}), encoding="utf-8")
        deadline = time.monotonic() + DEADLINE_SECONDS
        pid = os.fork()
        if pid == 0:
            os._exit(drive_stop(module, root, bin_directory))
        status: int | None = None
        try:
            if wait_for(root / "requested", pid, deadline):
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(pid, signal.SIGINT)
            while status is None and time.monotonic() < deadline:
                finished, raw = os.waitpid(pid, os.WNOHANG)
                if finished:
                    status = os.waitstatus_to_exitcode(raw)
                else:
                    time.sleep(POLL_SECONDS)
        finally:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(pid, signal.SIGKILL)
            if status is None:
                with contextlib.suppress(ChildProcessError):
                    os.waitpid(pid, 0)
        state = json.loads((root / "group.json").read_text(encoding="utf-8"))
        lines = (root / "report.txt").read_text(encoding="utf-8").splitlines() if (root / "report.txt").exists() else []
    return {
        "a SIGINT to the whole process group during the host-off request ends the stop within the deadline": status is not None,
        "the stop does not fail after a group SIGINT": status == 0,
        "the host reaches capacity zero after a group SIGINT": state["desired"] == 0,
        "no instance is left after a group SIGINT": not state["instances"],
        "the stop reports the host verified gone after a group SIGINT": any(line.startswith("host_off_verified") for line in lines),
        "the group SIGINT is reported as deferred": any("signal_deferred" in line and "SIGINT" in line for line in lines),
    }
