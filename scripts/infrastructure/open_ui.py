"""Open one web interface of the AWS host from this Mac through its Session Manager document.

Run from the repository root: ``bash scripts/open_ui.sh <name> [--local-port N]`` or ``bash scripts/open_ui.sh --list`` (the wrapper runs
``.venv/bin/python -m scripts.infrastructure.open_ui``). ``--list`` prints the Service (the name to open), Port and Health path of each web interface from the
same Terraform, and needs neither AWS nor ``.env``.

The profile, the region, the project name and the environment come from ``.env`` through ``load_configuration``. The script finds the one running instance
tagged ``Name`` ``<PROJECT_NAME>_host_<ENVIRONMENT>`` (it refuses zero or several), prints the browser address and runs ``aws ssm start-session`` as a child
it owns (started through ``scripts.process.start``) for the document ``<PROJECT_NAME>_<name>_forward_<ENVIRONMENT>``, which fixes the host port. The session
stays open until you stop it with Ctrl+C or its eight-hour deadline passes. The names it accepts are the keys of the ``web_ui_forwards`` map in
``infra/iam/web_ui_forwards.tf`` and the persistent OpenMetadata document in ``infra/iam/openmetadata_persistent.tf``: the same Terraform declares the
documents, so a new web interface needs no change here. It refuses every other name before it calls AWS. No output names an account ID or an ARN.

Stopping a session: ``aws ssm start-session`` runs the Session Manager plugin as its child and ignores SIGINT itself while the plugin runs (a terminal Ctrl+C
reaches the whole foreground process group and the plugin handles it). Ctrl+C here, or the deadline, therefore sends SIGINT to the process group of the
``aws`` child, which holds ``aws`` and the plugin and nothing else, and waits up to ``HAI_OPEN_UI_STOP_SECONDS`` (15) for the plugin to close the session.
Only if the group is still alive after that wait does it send SIGTERM, wait again and send SIGKILL last. This process started the child, so it holds its
``Popen`` and its process group is the child's pid: the stop needs no process listing and completes before this process exits, whatever else fails; the report
names the signals actually sent. While the session runs, the SIGINT handler only records each press, so a Ctrl+C never raises an error, and a Ctrl+C during a
wait skips the rest of that wait. The script can show only that the local processes ended and which signal each needed. It cannot prove offline that AWS ended
the remote session: after a SIGKILL the session may stay open until its idle timeout, and the script says so. ``HAI_OPEN_UI_SESSION_SECONDS`` (28800) sets the
deadline.
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
from dataclasses import dataclass, field
from pathlib import Path
from typing import NoReturn

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
from scripts.process import CompletedProcess, find_program, start

LOCAL_PORT = re.compile(r"[0-9]{1,5}")
HIGHEST_PORT = 65535
SESSION_SECONDS = 8 * 3600
STOP_SECONDS = 15.0
SESSION_SETTING = "HAI_OPEN_UI_SESSION_SECONDS"
STOP_SETTING = "HAI_OPEN_UI_STOP_SECONDS"
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


class AwsSession:
    """The ``aws`` child this process started and owns.

    The child is started through ``scripts.process.start`` (the launcher's own start function), in its own session, so this process holds its ``Popen`` and its
    process group is its pid: no process listing is needed to stop it, and no failing listing can strand it. A daemon thread only drains its output and sets
    ``finished`` when it ends and its pipes close. Every wait uses that event, not ``Thread.join``: in Python 3.12 a Ctrl+C that interrupts ``join`` marks a
    running thread as stopped.
    """

    def __init__(self, arguments: list[str]) -> None:
        environment = aws_environment()
        executable = find_program("aws", environment)
        if executable is None:
            raise FileNotFoundError("aws is not on PATH")
        self.arguments = [executable, *arguments]
        self.finished = threading.Event()
        self.presses: list[float] = []
        self.result: CompletedProcess[str] | None = None
        self.process = start(self.arguments, None, environment, new_session=True, errors="replace")
        threading.Thread(target=self.drain, name="aws_session", daemon=True).start()

    def drain(self) -> None:
        stdout, stderr = "", ""
        try:
            stdout, stderr = self.process.communicate()
        finally:
            self.result = CompletedProcess(self.arguments, self.process.returncode, stdout, stderr)
            self.finished.set()

    def press(self, _number: int, _frame: object) -> None:
        """The SIGINT handler while the session runs: it only records the press, so no Ctrl+C can raise inside the stop steps."""
        self.presses.append(time.monotonic())

    def running(self) -> bool:
        return not self.finished.is_set()

    def signal_group(self, number: int) -> None:
        """Send a signal to the child's process group (the child's pid), which holds ``aws`` and the plugin and nothing else."""
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self.process.pid, number)

    def wait(self, seconds: float, skippable: bool = True) -> bool:
        """Wait up to ``seconds`` for the session to end. A Ctrl+C pressed during the wait ends it early when ``skippable``. True when the session has ended."""
        seen = len(self.presses)
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            if self.finished.wait(min(0.1, max(0.0, end - time.monotonic()))):
                break
            if skippable and len(self.presses) > seen:
                break
        return self.finished.is_set()


def stop_session(session: AwsSession, stop_seconds: float) -> list[str]:
    """Stop the child's process group with SIGINT, then SIGTERM, then SIGKILL, waiting ``stop_seconds`` after each, and return the names of the signals
    actually sent (empty when the child had already ended). A Ctrl+C during a wait skips the rest of that wait.
    """
    sent: list[str] = []
    for number in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
        if not session.running():
            break
        session.signal_group(number)
        sent.append(number.name)
        if session.wait(stop_seconds):
            break
    return sent


@dataclass
class SessionEnd:
    """How a session ended: why (``ended``, ``interrupt`` or ``deadline``), the signals sent to stop it, whether the child is still alive and its result."""

    reason: str
    sent: list[str] = field(default_factory=list)
    alive: bool = False
    result: CompletedProcess[str] | None = None


def run_session(arguments: list[str], session_seconds: float, stop_seconds: float) -> SessionEnd:
    """Run ``aws`` with ``arguments`` until it ends, the user presses Ctrl+C or the deadline passes, then stop it if it has not ended.

    This process owns the child, so the stop needs no discovery and always runs to its end before this function returns: SIGINT, wait, SIGTERM, wait, SIGKILL,
    wait, each to the child's process group. From the start until the end of the stop the SIGINT handler only records each press, so no Ctrl+C raises an error.
    A last SIGKILL to the group runs on any way out while the child lives.
    """
    session = AwsSession(arguments)
    reason = "ended"
    sent: list[str] = []
    previous = signal.signal(signal.SIGINT, session.press)
    try:
        end = time.monotonic() + session_seconds
        while not session.finished.wait(0.2):
            if session.presses:
                reason = "interrupt"
                break
            if time.monotonic() >= end:
                reason = "deadline"
                break
        sent = stop_session(session, stop_seconds)
    finally:
        signal.signal(signal.SIGINT, previous)
        if session.running():
            session.signal_group(signal.SIGKILL)
    return SessionEnd(reason, sent, session.running(), session.result)


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
    end = run_session(session_arguments(settings, arguments.name, target, local_port), session_seconds, stop_seconds)
    sent = ", ".join(end.sent) or "none"
    if end.alive or end.result is None:
        sys.stderr.write(f"open_ui: the aws process did not end after SIGKILL (signals sent: {sent}). Look for it with: ps -A | grep aws\n")
        return 1
    if "SIGKILL" in end.sent:
        sys.stderr.write(
            "open_ui: the Session Manager plugin did not close on SIGINT or SIGTERM and was killed "
            f"(signals sent: {sent}). The AWS session may stay open until its idle timeout.\n"
        )
        return 1
    if end.reason == "deadline":
        sys.stderr.write(
            f"open_ui: the session reached its deadline and the plugin was asked to close it (signals sent: {sent}). Run the command again to reopen it.\n"
        )
        return 1
    if end.reason == "ended" and end.result.returncode:
        code = ERROR_CODE.search(end.result.stderr)
        sys.stderr.write(f"open_ui: the session ended with exit code {end.result.returncode} ({code.group(1) if code else 'request_failed'}).\n")
        return 1
    note = " The script cannot confirm that AWS ended the remote session." if end.reason == "interrupt" else ""
    sys.stdout.write(f"open_ui: the session is closed.{note}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
