"""Open one web interface of the AWS host from this Mac through its Session Manager document.

Run from the repository root: ``bash scripts/open_ui.sh <name> [--local-port N]`` or ``bash scripts/open_ui.sh --list`` (the wrapper runs
``.venv/bin/python -m scripts.infrastructure.open_ui``). ``--list`` prints the Service (the name to open), Port and Health path of each web interface from the
same Terraform, and needs neither AWS nor ``.env``.

The profile, the region, the project name and the environment come from ``.env`` through ``load_configuration``. The script finds the one running instance
tagged ``Name`` ``<PROJECT_NAME>_host_<ENVIRONMENT>`` (it refuses zero or several), prints the browser address and runs ``aws ssm start-session`` through
``scripts.process.run_command`` for the document ``<PROJECT_NAME>_<name>_forward_<ENVIRONMENT>``, which fixes the host port. The session stays open until you
stop it with Ctrl+C or its eight-hour deadline passes. The names it accepts are the keys of the
``web_ui_forwards`` map in ``infra/iam/web_ui_forwards.tf`` and the persistent OpenMetadata document in ``infra/iam/openmetadata_persistent.tf``: the same
Terraform declares the documents, so a new web interface needs no change here. It refuses every other name before it calls AWS. No output names an account ID
or an ARN.

Stopping a session: ``aws ssm start-session`` runs the Session Manager plugin as its child and ignores SIGINT itself while the plugin runs (a terminal Ctrl+C
reaches the whole foreground process group and the plugin handles it). Ctrl+C here, or the deadline, therefore sends SIGINT to the process group of the ``aws``
child only, which holds ``aws`` and the plugin and nothing else, and waits up to ``HAI_OPEN_UI_STOP_SECONDS`` (15) for the plugin to close the session. Only if
the group is still alive after that wait does it send SIGTERM, wait again and send SIGKILL last. A second Ctrl+C during a wait skips the rest of that wait. The
script can show only that the local processes ended and which signal each needed. It cannot prove offline that AWS ended the remote session: after a SIGKILL
the session may stay open until its idle timeout, and the script says so. ``HAI_OPEN_UI_SESSION_SECONDS`` (28800) sets the deadline.
"""

from __future__ import annotations

import argparse
import contextlib
import math
import os
import re
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, NoReturn

from scripts.acquisition.s3_store import AwsCli, StorageError, aws_environment
from scripts.infrastructure.render_project_config import REPO_ROOT, ConfigurationError, load_configuration
from scripts.infrastructure.web_ui_terraform import (
    SERVICES,
    Listeners,
    WebUiTerraformError,
    forward_documents,
    health_path,
    terraform_listeners,
    terraform_text,
)
from scripts.process import CompletedProcess, list_processes, run_command

LOCAL_PORT = re.compile(r"[0-9]{1,5}")
HIGHEST_PORT = 65535
SESSION_SECONDS = 8 * 3600
STOP_SECONDS = 15.0
SESSION_SETTING = "HAI_OPEN_UI_SESSION_SECONDS"
STOP_SETTING = "HAI_OPEN_UI_STOP_SECONDS"
CHILD_SEARCH_SECONDS = 3.0
ERROR_CODE = re.compile(r"An error occurred \(([A-Za-z0-9]+)\)")


class OpenUiError(Exception):
    """A refusal with the reason to show: the exit code is 1, or 2 for a bad argument."""

    def __init__(self, message: str, code: int = 1) -> None:
        super().__init__(message)
        self.code = code


def interface_rows(documents: dict[str, int] | None = None, listeners: Listeners | None = None) -> list[tuple[str, str, str]]:
    """The Service, Port and Health path of each web interface, in name order: the path is ``None`` when its container's health check calls none.

    The documents and the listeners are read from the Terraform unless a caller passes them (the README test plants defects in them).
    """
    documents = forward_documents() if documents is None else documents
    listeners = terraform_listeners(terraform_text(SERVICES))[0] if listeners is None else listeners
    rows = []
    for name, port in sorted(documents.items()):
        try:
            path = health_path(port, listeners) or "None"
        except WebUiTerraformError:
            path = "unknown"
        rows.append((name, str(port), path))
    return rows


def list_text() -> str:
    """The three-column list that ``--list`` prints."""
    rows = [("Service", "Port", "Health path"), *interface_rows()]
    widths = [max(len(row[column]) for row in rows) for column in range(3)]
    return "".join(f"{row[0]:<{widths[0]}}  {row[1]:<{widths[1]}}  {row[2]}\n" for row in rows)


def host_instance(client: AwsCli, project: str, environment: str) -> str:
    """The ID of the one running host instance, found by its Name tag; zero or several refuse."""
    tag = f"{project}_host_{environment}"
    try:
        answer = client.call("ec2", "describe-instances", ["--filters", f"Name=tag:Name,Values={tag}", "Name=instance-state-name,Values=running"])
    except StorageError as error:
        raise OpenUiError(f"the host lookup failed ({error}). Check the profile and the Region in .env.") from None
    found = [item["InstanceId"] for group in answer.get("Reservations", []) for item in group.get("Instances", []) if isinstance(item.get("InstanceId"), str)]
    if not found:
        raise OpenUiError(f"no running instance is tagged Name {tag}. Start the host with: bash scripts/host.sh start")
    if len(found) > 1:
        raise OpenUiError(f"{len(found)} running instances are tagged Name {tag}; exactly one is allowed. Stop the extra ones before you open a web interface.")
    return found[0]


def session_arguments(settings: dict[str, str], name: str, target: str, local_port: int) -> list[str]:
    """The exact argument list of the ``aws`` Session Manager call."""
    document = f"{settings['project_name']}_{name}_forward_{settings['environment']}"
    return [
        "ssm",
        "start-session",
        "--target",
        target,
        "--document-name",
        document,
        "--parameters",
        f"localPortNumber={local_port}",
        "--profile",
        settings["aws_profile"],
        "--region",
        settings["aws_region"],
    ]


class FixedMessageParser(argparse.ArgumentParser):
    """An argument parser whose error names no input: argparse echoes the rejected text, which could be a document ARN or an account ID."""

    def error(self, message: str) -> NoReturn:
        sys.stderr.write("open_ui: the arguments are not valid. Use: open_ui <name> [--local-port N] [--env-file FILE], or open_ui --list\n")
        raise SystemExit(2)


def parse(argv: list[str], names: list[str]) -> argparse.Namespace:
    parser = FixedMessageParser(description=__doc__.splitlines()[0], allow_abbrev=False)
    parser.add_argument("name", nargs="?", help="the web interface to open: " + ", ".join(names))
    parser.add_argument("--list", action="store_true", help="print the Service, Port and Health path of each web interface and stop")
    parser.add_argument("--local-port", default=None, help="the port the browser uses on this Mac (default: the port of the interface)")
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    return parser.parse_args(argv)


def chosen_port(text: str | None, default: int) -> int:
    if text is None:
        return default
    if LOCAL_PORT.fullmatch(text) is None or not 1 <= int(text) <= HIGHEST_PORT:
        raise OpenUiError(f"--local-port must be a whole number from 1 to {HIGHEST_PORT}", 2)
    return int(text)


def seconds_setting(name: str, default: float) -> float:
    """The seconds an environment setting gives, or ``default`` when it is unset. A value that is not a finite number above zero is refused and not echoed."""
    text = os.environ.get(name)
    if text is None:
        return default
    try:
        seconds = float(text)
    except ValueError:
        seconds = math.nan
    if not (math.isfinite(seconds) and seconds > 0):
        raise OpenUiError(f"{name} must be a number of seconds above zero", 2)
    return seconds


def session_children() -> list[int]:
    """The pids of this process's own children that lead a process group: the ``aws`` child. A listing also shows its own short-lived lister as a child, so
    only a child that two listings both show counts."""

    def leaders() -> set[int]:
        return {row.pid for row in list_processes() if row.parent == os.getpid() and row.group == row.pid}

    return sorted(leaders() & leaders())


class AwsSession:
    """``run_command("aws", ...)`` in a worker thread, with events instead of ``Thread.join`` for every wait: in Python 3.12 a Ctrl+C that interrupts
    ``join`` marks a running thread as stopped, so ``is_alive`` would then be wrong."""

    def __init__(self, arguments: list[str], deadline: float) -> None:
        self.begun = threading.Event()
        self.finished = threading.Event()
        self.outcome: dict[str, Any] = {}
        self.arguments = arguments
        self.deadline = deadline
        self.thread = threading.Thread(target=self.work, name="aws_session")

    def work(self) -> None:
        self.begun.set()
        try:
            self.outcome["result"] = run_command("aws", self.arguments, env=aws_environment(), timeout=self.deadline)
        except BaseException as error:
            self.outcome["error"] = error
        finally:
            self.finished.set()

    def running(self) -> bool:
        return not self.finished.is_set()

    def wait(self, seconds: float) -> bool:
        """Wait up to ``seconds`` for the session to end; a second Ctrl+C ends the wait early. True when it has ended."""
        with contextlib.suppress(KeyboardInterrupt):
            self.finished.wait(max(0.0, seconds))
        return self.finished.is_set()


def stop_session(session: AwsSession, stop_seconds: float) -> str:
    """Stop the ``aws`` child's process group with SIGINT, then SIGTERM, then SIGKILL, waiting ``stop_seconds`` after each. Returns the last signal sent
    (``none`` when the child ended by itself first, ``survived`` when it is still alive after SIGKILL)."""
    search_end = time.monotonic() + CHILD_SEARCH_SECONDS
    children: list[int] = []
    while session.running() and not children and time.monotonic() < search_end:
        children = session_children()
        if not children:
            session.wait(0.1)
    sent = "none"
    for name, number in (("interrupt", signal.SIGINT), ("term", signal.SIGTERM), ("kill", signal.SIGKILL)):
        if not session.running():
            return sent
        for pid in children:
            try:
                os.killpg(pid, number)
            except ProcessLookupError:
                continue
        sent = name
        if session.wait(stop_seconds):
            return sent
    return "survived"


def run_session(arguments: list[str], session_seconds: float, stop_seconds: float) -> tuple[str, str, CompletedProcess[str] | None]:
    """Run ``aws`` with ``arguments`` until it ends, the user presses Ctrl+C or the deadline passes. Returns why it stopped (``ended``, ``interrupt`` or
    ``deadline``), the last signal sent to stop it (``none`` when it ended by itself) and its result when it ended by itself or after a stop.

    ``run_command`` runs in a worker thread, so this thread, which alone receives Ctrl+C, can stop the child in steps. The worker's own deadline is only a
    last bound: it is longer than the session deadline plus the three waits, so ``run_command``'s abrupt session stop never comes first.
    """
    session = AwsSession(arguments, session_seconds + 4 * stop_seconds + 60)
    reason = "ended"
    try:
        session.thread.start()
        end = time.monotonic() + session_seconds
        while not session.finished.wait(0.2):
            if time.monotonic() >= end:
                reason = "deadline"
                break
    except KeyboardInterrupt:
        reason = "interrupt"
    # A Ctrl+C can arrive while the thread is still starting (the child may already run): give the thread a moment to begin before judging it.
    if reason == "interrupt":
        session.begun.wait(2.0)
    stop = stop_session(session, stop_seconds) if session.running() else "none"
    if session.running():
        return reason, stop, None
    if "error" in session.outcome:
        raise session.outcome["error"]
    return reason, stop, session.outcome.get("result")


def main(argv: list[str] | None = None) -> int:
    documents = forward_documents()
    names = sorted(documents)
    arguments = parse(sys.argv[1:] if argv is None else argv, names)
    if arguments.list:
        if arguments.name is not None or arguments.local_port is not None:
            sys.stderr.write("open_ui: --list takes no name and no --local-port\n")
            return 2
        sys.stdout.write(list_text())
        return 0
    try:
        session_seconds = seconds_setting(SESSION_SETTING, SESSION_SECONDS)
        stop_seconds = seconds_setting(STOP_SETTING, STOP_SECONDS)
        if arguments.name is None:
            raise OpenUiError(f"name a web interface or use --list. The web interfaces are: {', '.join(names)}", 2)
        if arguments.name not in documents:
            raise OpenUiError(f"the name is not a web interface. The web interfaces are: {', '.join(names)}", 2)
        local_port = chosen_port(arguments.local_port, documents[arguments.name])
        settings, _ = load_configuration(arguments.env_file)
        target = host_instance(AwsCli(settings), settings["project_name"], settings["environment"])
    except (OpenUiError, ConfigurationError) as error:
        sys.stderr.write(f"open_ui: {error}\n")
        return error.code if isinstance(error, OpenUiError) else 1
    sys.stdout.write(f"open_ui: {arguments.name} opens at http://127.0.0.1:{local_port}\nKeep this window open and press Ctrl+C to close the session.\n")
    sys.stdout.flush()
    reason, stop, result = run_session(session_arguments(settings, arguments.name, target, local_port), session_seconds, stop_seconds)
    if stop == "survived" or result is None:
        sys.stderr.write("open_ui: the aws process did not end after SIGKILL. Look for it with: ps -A | grep aws\n")
        return 1
    if stop == "kill":
        sys.stderr.write(
            "open_ui: the Session Manager plugin did not close on SIGINT or SIGTERM and was killed. The AWS session may stay open until its idle timeout.\n"
        )
        return 1
    if reason == "deadline":
        sys.stderr.write("open_ui: the session reached its deadline and the plugin was asked to close it. Run the command again to reopen it.\n")
        return 1
    if reason == "ended" and result.returncode:
        code = ERROR_CODE.search(result.stderr)
        sys.stderr.write(f"open_ui: the session ended with exit code {result.returncode} ({code.group(1) if code else 'request_failed'}).\n")
        return 1
    note = " The script cannot confirm that AWS ended the remote session." if reason == "interrupt" else ""
    sys.stdout.write(f"open_ui: the session is closed.{note}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
