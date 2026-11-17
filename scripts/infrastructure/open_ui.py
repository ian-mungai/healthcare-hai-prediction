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

Running and stopping a session: ``aws ssm start-session`` runs as an ordinary foreground child of this process, in this process's own process group and with
the terminal's input and output (this process starts it with ``os.posix_spawnp``: ``scripts.process`` always puts a child in a new group or session, which
would take it out of the terminal's foreground group). A terminal Ctrl+C therefore reaches ``aws``, the Session Manager plugin and this process together, as
when ``aws`` is run by hand. This process ignores SIGINT while it waits, so it outlives the child and can report; it blocks SIGINT while it starts the child
so no Ctrl+C can fall between the start and the ignore. Only the deadline (``HAI_OPEN_UI_SESSION_SECONDS``, 28800) stops a session by signal: SIGINT first,
sent to this process's own group, because the installed AWS CLI ignores SIGINT itself while the plugin runs and only the plugin handles it; then, after
``HAI_OPEN_UI_STOP_SECONDS`` (15) without an exit, SIGTERM to the child's pid; then, after another wait, SIGKILL to the child's pid. A signal is sent only
while the child has not been reaped, so no signal can reach a reused pid, and the report names the signals actually sent. This process never exits while its
child runs: an error in the wait runs the same stop first. Limits: SIGTERM and SIGKILL go to the ``aws`` process only, so a plugin that ignored SIGINT could
outlive it; and the script cannot prove offline that AWS ended the remote session, so it says so.
"""

from __future__ import annotations

import argparse
import math
import os
import re
import signal
import sys
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

LOCAL_PORT = re.compile(r"[0-9]{1,5}")
HIGHEST_PORT = 65535
SESSION_SECONDS = 8 * 3600
STOP_SECONDS = 15.0
SESSION_SETTING = "HAI_OPEN_UI_SESSION_SECONDS"
STOP_SETTING = "HAI_OPEN_UI_STOP_SECONDS"


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


def session_expired(end: float) -> bool:
    return time.monotonic() >= end


@dataclass
class SessionEnd:
    """How a session ended: why (``ended`` or ``deadline``), the signals sent to stop it, whether the child is still alive and its exit code (negative for a
    signal, ``None`` when it is not reaped)."""

    reason: str
    sent: list[str] = field(default_factory=list)
    alive: bool = False
    code: int | None = None


class Child:
    """The ``aws`` child, reaped only here: ``poll`` is the one place that waits for it, so a signal is sent only while the pid still belongs to the child."""

    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.code: int | None = None

    def poll(self) -> bool:
        """True once the child has exited and been reaped."""
        if self.code is None:
            reaped, status = os.waitpid(self.pid, os.WNOHANG)
            if reaped:
                self.code = os.waitstatus_to_exitcode(status)
        return self.code is not None

    def wait(self, seconds: float) -> bool:
        """Wait up to ``seconds`` for the child to exit. True once it has."""
        end = time.monotonic() + seconds
        while not self.poll() and time.monotonic() < end:
            time.sleep(0.05)
        return self.poll()


def stop_child(child: Child, stop_seconds: float) -> list[str]:
    """Stop a child that outlived its deadline: SIGINT to this process's own group (as a terminal Ctrl+C reaches ``aws`` and the plugin; this process ignores
    it), then SIGTERM and SIGKILL to the child's pid, waiting ``stop_seconds`` after each. ``poll`` runs before every signal, so none is sent after the child
    is reaped. Returns the names of the signals actually sent.
    """
    sent: list[str] = []
    for number in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
        if child.poll():
            break
        if number == signal.SIGINT:
            os.killpg(os.getpgrp(), number)
        else:
            os.kill(child.pid, number)
        sent.append(number.name)
        if child.wait(stop_seconds):
            break
    return sent


def run_session(arguments: list[str], session_seconds: float, stop_seconds: float) -> SessionEnd:
    """Run ``aws`` with ``arguments`` in the foreground until it exits or the deadline passes, then stop it if it has not exited. It never returns or raises
    while the child runs, unless the child survived SIGKILL.

    SIGINT is blocked while the child starts and ignored from then until the child is reaped, so a Ctrl+C reaches the child (it shares this process's group)
    and never this process in between. The child starts with the default disposition for SIGINT and no signal blocked.
    """
    environment = aws_environment()
    blocked = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
    try:
        child = Child(os.posix_spawnp("aws", ["aws", *arguments], environment, setsigmask=[], setsigdef=[signal.SIGINT]))
        previous = signal.signal(signal.SIGINT, signal.SIG_IGN)
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, blocked)
    reason = "ended"
    sent: list[str] = []
    try:
        end = time.monotonic() + session_seconds
        while not child.poll():
            if session_expired(end):
                reason = "deadline"
                break
            time.sleep(0.1)
        sent = stop_child(child, stop_seconds)
    except BaseException:
        stop_child(child, stop_seconds)
        raise
    finally:
        signal.signal(signal.SIGINT, previous)
    return SessionEnd(reason, sent, not child.poll(), child.code)


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
    except KeyboardInterrupt:
        sys.stderr.write("open_ui: interrupted before the session started. Nothing was started.\n")
        return 130
    except (OpenUiError, ConfigurationError) as error:
        sys.stderr.write(f"open_ui: {error}\n")
        return error.code if isinstance(error, OpenUiError) else 1
    sys.stdout.write(f"open_ui: {arguments.name} opens at http://127.0.0.1:{local_port}\nKeep this window open and press Ctrl+C to close the session.\n")
    sys.stdout.flush()
    end = run_session(session_arguments(settings, arguments.name, target, local_port), session_seconds, stop_seconds)
    sent = ", ".join(end.sent) or "none"
    if end.alive:
        sys.stderr.write(f"open_ui: the aws process did not end after SIGKILL (signals sent: {sent}). Look for it with: ps -A | grep aws\n")
        return 1
    if "SIGKILL" in end.sent:
        sys.stderr.write(
            f"open_ui: aws did not close on SIGINT or SIGTERM and was killed (signals sent: {sent}). The AWS session may stay open until its idle timeout.\n"
        )
        return 1
    if end.reason == "deadline":
        sys.stderr.write(f"open_ui: the session reached its deadline and was asked to close (signals sent: {sent}). Run the command again to reopen it.\n")
        return 1
    if end.code not in (0, 130, -signal.SIGINT):
        sys.stderr.write(f"open_ui: the session ended with exit code {end.code}.\n")
        return 1
    sys.stdout.write("open_ui: the session is closed. The script cannot confirm that AWS ended the remote session.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
