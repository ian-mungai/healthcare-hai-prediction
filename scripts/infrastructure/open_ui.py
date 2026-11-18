"""Open one web interface of the AWS host from this Mac through its Session Manager document.

Run from the repository root: ``bash scripts/open_ui.sh <name> [--local-port N]`` or ``bash scripts/open_ui.sh --list`` (the wrapper runs
``.venv/bin/python -m scripts.infrastructure.open_ui``). ``--list`` prints the Service (the name to open), Port and Health path of each web interface from the
same Terraform, and needs neither AWS nor ``.env``.

The profile, the region, the project name and the environment come from ``.env`` through ``load_configuration``. The script finds the one running instance
tagged ``Name`` ``<PROJECT_NAME>_host_<ENVIRONMENT>`` (it refuses zero or several), prints the browser address and gives ``scripts/open_ui.sh`` the
``aws ssm start-session`` command for the document ``<PROJECT_NAME>_<name>_forward_<ENVIRONMENT>``, which fixes the host port. The wrapper execs that
command (see "Running a session" below). ``--help`` prints the usage to standard error, so the wrapper gets no command and exits 0 without an exec. The
names it accepts are the keys of the ``web_ui_forwards`` map in
``infra/iam/web_ui_forwards.tf`` and the persistent OpenMetadata document in ``infra/iam/openmetadata_persistent.tf``: the same Terraform declares the
documents, so a new web interface needs no change here. It refuses every other name before it calls AWS. No output names an account ID or an ARN.

Running a session: ``open_ui`` only validates, and it supervises nothing. Called by ``scripts/open_ui.sh`` with the internal ``--emit-command`` option, it
prints the validated command to standard output, one part per line, and exits 0: ``env`` (which drops the inherited ``AWS_*`` settings and sets the fixed ones),
then ``aws ssm start-session`` with its exact arguments. Every human message goes to standard error. The wrapper reads the command, checks this process's exit
status and replaces itself with the command (shell ``exec``; Python's ``os.exec*`` cannot pass the lint rule S606, which has no approved suppression). No
process is left to watch the session and no file is made. A terminal Ctrl+C then reaches ``aws`` and the Session Manager plugin as when ``aws`` is run by
hand, and a Ctrl+C before the exec exits 130 with \"Nothing was started\". The only child ``open_ui`` itself causes is the instance lookup, made by ``AwsCli``
and ended before it returns. This script sets no timer and no session limit: AWS Session Manager's own idle timeout and maximum session duration preferences
end the session, and this repository sets neither, so the account defaults apply. It cannot prove offline that AWS ended a remote session.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn

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

if TYPE_CHECKING:
    from _typeshed import SupportsWrite

LOCAL_PORT = re.compile(r"[0-9]{1,5}")
HIGHEST_PORT = 65535


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
    """An argument parser whose error names no input: argparse echoes the rejected text, which could be a document ARN or an account ID.

    Its help goes to standard error: standard output is the command channel that ``scripts/open_ui.sh`` execs, so ``--help`` must leave it empty."""

    def print_help(self, file: SupportsWrite[str] | None = None) -> None:
        super().print_help(sys.stderr if file is None else file)

    def error(self, message: str) -> NoReturn:
        sys.stderr.write("open_ui: the arguments are not valid. Use: bash scripts/open_ui.sh <name> [--local-port N], or bash scripts/open_ui.sh --list\n")
        raise SystemExit(2)


def parse(argv: list[str], names: list[str]) -> argparse.Namespace:
    parser = FixedMessageParser(description=__doc__.splitlines()[0], allow_abbrev=False)
    parser.add_argument("name", nargs="?", help="the web interface to open: " + ", ".join(names))
    parser.add_argument("--list", action="store_true", help="print the Service, Port and Health path of each web interface and stop")
    parser.add_argument("--local-port", default=None, help="the port the browser uses on this Mac (default: the port of the interface)")
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    parser.add_argument("--emit-command", action="store_true", help="internal: print the validated command for scripts/open_ui.sh to exec")
    return parser.parse_args(argv)


def chosen_port(text: str | None, default: int) -> int:
    if text is None:
        return default
    if LOCAL_PORT.fullmatch(text) is None or not 1 <= int(text) <= HIGHEST_PORT:
        raise OpenUiError(f"--local-port must be a whole number from 1 to {HIGHEST_PORT}", 2)
    return int(text)


def exec_command(settings: dict[str, str], name: str, target: str, local_port: int) -> list[str]:
    """The command the shell execs: ``env`` without the inherited ``AWS_*`` settings and with the fixed ones of ``aws_environment``, then ``aws`` and the exact
    Session Manager arguments. The environment is the one ``AwsCli`` gives every project call, so only the explicit profile and region select the identity."""
    unset = [part for key in sorted(os.environ) if key.startswith("AWS_") for part in ("-u", key)]
    fixed = sorted(f"{key}={value}" for key, value in aws_environment().items() if key.startswith("AWS_"))
    return ["env", *unset, *fixed, "aws", *session_arguments(settings, name, target, local_port)]


def run(argv: list[str]) -> int:
    documents = forward_documents()
    names = sorted(documents)
    arguments = parse(argv, names)
    if arguments.list:
        if arguments.name is not None or arguments.local_port is not None:
            sys.stderr.write("open_ui: --list takes no name and no --local-port\n")
            return 2
        (sys.stderr if arguments.emit_command else sys.stdout).write(list_text())
        return 0
    try:
        if arguments.name is None:
            raise OpenUiError(f"name a web interface or use --list. The web interfaces are: {', '.join(names)}", 2)
        if arguments.name not in documents:
            raise OpenUiError(f"the name is not a web interface. The web interfaces are: {', '.join(names)}", 2)
        local_port = chosen_port(arguments.local_port, documents[arguments.name])
        if not arguments.emit_command:
            raise OpenUiError("run it through bash scripts/open_ui.sh, which starts the session after this check", 2)
        settings, _ = load_configuration(arguments.env_file)
        target = host_instance(AwsCli(settings), settings["project_name"], settings["environment"])
    except (OpenUiError, ConfigurationError) as error:
        sys.stderr.write(f"open_ui: {error}\n")
        return error.code if isinstance(error, OpenUiError) else 1
    command = exec_command(settings, arguments.name, target, local_port)
    if any("\n" in part or "\0" in part for part in command):
        sys.stderr.write("open_ui: the validated command holds a line break or a NUL, so it is refused.\n")
        return 1
    sys.stderr.write(
        f"open_ui: {arguments.name} opens at http://127.0.0.1:{local_port}\n"
        "Keep this window open and press Ctrl+C to close the session. AWS Session Manager's idle timeout and maximum session duration also end it.\n"
    )
    sys.stdout.write("".join(part + "\n" for part in command))
    sys.stdout.flush()
    return 0


def main(argv: list[str] | None = None) -> int:
    """Validate and write the command, or refuse. A Ctrl+C before the command is written starts nothing and exits 130."""
    try:
        return run(sys.argv[1:] if argv is None else argv)
    except KeyboardInterrupt:
        sys.stderr.write("open_ui: interrupted. Nothing was started.\n")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
