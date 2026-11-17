"""Open one web interface of the AWS host from this Mac through its Session Manager document.

Run from the repository root: ``bash scripts/open_ui.sh <name> [--local-port N]`` or ``bash scripts/open_ui.sh --list`` (the wrapper runs
``.venv/bin/python -m scripts.infrastructure.open_ui``). ``--list`` prints the Service (the name to open), Port and Health path of each web interface from the
same Terraform, and needs neither AWS nor ``.env``.

The profile, the region, the project name and the environment come from ``.env`` through ``load_configuration``. The script finds the one running instance
tagged ``Name`` ``<PROJECT_NAME>_host_<ENVIRONMENT>`` (it refuses zero or several), prints the browser address and runs ``aws ssm start-session`` through
``scripts.process.run_command`` for the document ``<PROJECT_NAME>_<name>_forward_<ENVIRONMENT>``, which fixes the host port. The session stays open until you
stop it with Ctrl+C or its eight-hour deadline passes; either stops the AWS process and its children. The names it accepts are the keys of the
``web_ui_forwards`` map in ``infra/iam/web_ui_forwards.tf`` and the persistent OpenMetadata document in ``infra/iam/openmetadata_persistent.tf``: the same
Terraform declares the documents, so a new web interface needs no change here. It refuses every other name before it calls AWS. No output names an account ID
or an ARN.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

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
from scripts.process import TimeoutExpired, run_command

LOCAL_PORT = re.compile(r"[0-9]{1,5}")
HIGHEST_PORT = 65535
SESSION_SECONDS = 8 * 3600
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


def parse(argv: list[str], names: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
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
        if arguments.name is None:
            raise OpenUiError(f"name a web interface or use --list. The web interfaces are: {', '.join(names)}", 2)
        if arguments.name not in documents:
            raise OpenUiError(f"{arguments.name!r} is not a web interface. The web interfaces are: {', '.join(names)}", 2)
        local_port = chosen_port(arguments.local_port, documents[arguments.name])
        settings, _ = load_configuration(arguments.env_file)
        target = host_instance(AwsCli(settings), settings["project_name"], settings["environment"])
    except (OpenUiError, ConfigurationError) as error:
        sys.stderr.write(f"open_ui: {error}\n")
        return error.code if isinstance(error, OpenUiError) else 1
    sys.stdout.write(f"open_ui: {arguments.name} opens at http://127.0.0.1:{local_port}\nKeep this window open and press Ctrl+C to close the session.\n")
    sys.stdout.flush()
    try:
        result = run_command("aws", session_arguments(settings, arguments.name, target, local_port), env=aws_environment(), timeout=SESSION_SECONDS)
    except KeyboardInterrupt:
        sys.stdout.write("open_ui: the session is closed.\n")
        return 0
    except TimeoutExpired:
        sys.stderr.write("open_ui: the session reached its eight-hour deadline and is closed. Run the command again to reopen it.\n")
        return 1
    if result.returncode:
        code = ERROR_CODE.search(result.stderr)
        sys.stderr.write(f"open_ui: the session ended with exit code {result.returncode} ({code.group(1) if code else 'request_failed'}).\n")
        return 1
    sys.stdout.write("open_ui: the session is closed.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
