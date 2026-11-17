"""``scripts/infrastructure/open_ui.py`` opens only a web interface the Terraform declares, on the one host, with the exact Session Manager call.

Every case runs the real script as a child process with a stand-in ``aws`` first on ``PATH``. The stand-in answers the instance lookup from a file and
records every call, one argument per line, so a case can compare the exact argument list of each call. Nothing reaches AWS.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

from scripts.infrastructure import web_ui_terraform
from scripts.process import CompletedProcess, run_command
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
FORWARDS = ROOT / "infra" / "iam" / "web_ui_forwards.tf"
PROFILE = "example_project_dev"
REGION = "us-west-2"
INSTANCE = "i-0aaaaaaaaaaaaaaa1"
OTHER_INSTANCE = "i-0bbbbbbbbbbbbbbb2"
ENV_VALUES = {
    "AWS_REGION": REGION,
    "AWS_PROFILE": PROFILE,
    "AWS_ACCOUNT_ID": "111111111111",
    "PROJECT_NAME": "example_project",
    "ENVIRONMENT": "dev",
    "S3_BUCKET": "example-bucket",
}
STAND_IN = """#!/bin/bash
{ printf '%s\\n' "$@"; printf '%s\\n' '--end--'; } >> "$STAND_IN_LOG"
if [ "$1 $2" = "ec2 describe-instances" ]; then cat "$STAND_IN_INSTANCES"; exit 0; fi
if [ "$1 $2" = "ssm start-session" ]; then
  if [ -n "$STAND_IN_SESSION_ERROR" ]; then
    echo "An error occurred ($STAND_IN_SESSION_ERROR) when calling StartSession: arn:aws:ssm:us-west-2:111111111111:x" >&2
    exit 254
  fi
  exit 0
fi
exit 3
"""


def instances(*identifiers: str) -> str:
    return json.dumps({"Reservations": [{"Instances": [{"InstanceId": item, "State": {"Name": "running"}} for item in identifiers]}] if identifiers else []})


class Workspace:
    """A folder with the stand-in ``aws``, its answer file, its call log and an ``.env`` file."""

    def __init__(self, folder: Path, answer: str, values: dict[str, str] | None = None) -> None:
        self.folder = folder
        self.bin = folder / "bin"
        self.bin.mkdir()
        (self.bin / "aws").write_text(STAND_IN)
        (self.bin / "aws").chmod(0o755)
        self.log = folder / "calls.log"
        self.answer = folder / "instances.json"
        self.answer.write_text(answer)
        self.env_file = folder / ".env"
        self.env_file.write_text("".join(f"{key}={value}\n" for key, value in (ENV_VALUES if values is None else values).items()))

    def run(self, *arguments: str, env_file: Path | None = None, session_error: str = "") -> CompletedProcess[str]:
        environment = {
            "PATH": f"{self.bin}{os.pathsep}/usr/bin{os.pathsep}/bin",
            "STAND_IN_LOG": str(self.log),
            "STAND_IN_INSTANCES": str(self.answer),
            "STAND_IN_SESSION_ERROR": session_error,
            "AWS_PROFILE": "an_inherited_profile",
            "AWS_REGION": "eu-west-1",
        }
        return run_command(
            sys.executable,
            ["-m", "scripts.infrastructure.open_ui", *arguments, "--env-file", str(env_file or self.env_file)],
            cwd=ROOT,
            env=environment,
            timeout=60,
        )

    def calls(self) -> list[list[str]]:
        if not self.log.exists():
            return []
        found: list[list[str]] = [[]]
        for line in self.log.read_text().splitlines():
            if line == "--end--":
                found.append([])
            else:
                found[-1].append(line)
        return found[:-1]


def terraform_map_names() -> set[str]:
    """The keys of the ``web_ui_forwards`` map, read here by their own pattern, and the persistent OpenMetadata document key."""
    block = re.search(r"web_ui_forwards = \{\n(.*?)\n  \}", FORWARDS.read_text(), re.DOTALL)
    check(block is not None, "web_ui_forwards.tf has no web_ui_forwards map")
    names = set(re.findall(r"^\s+(\w+)\s+= \{ port = \d+,", block.group(1) if block else "", re.MULTILINE))
    return names | {"openmetadata"}


def test_the_names_the_script_accepts_equal_the_terraform_map(tmp_path: Path) -> None:
    accepted = set(web_ui_terraform.forward_documents())
    check(accepted == terraform_map_names(), f"the script accepts {sorted(accepted)} but the Terraform declares {sorted(terraform_map_names())}")
    refused = Workspace(tmp_path, instances(INSTANCE)).run("nothing_here")
    check(refused.returncode == 2, f"an unknown name must exit 2: {refused.returncode} {refused.stderr}")
    check(all(name in refused.stderr for name in accepted), f"the refusal must list every accepted name: {refused.stderr}")


def test_a_new_map_entry_is_accepted_and_a_removed_one_is_not(tmp_path: Path) -> None:
    text = FORWARDS.read_text()
    added = tmp_path / "added.tf"
    added.write_text(text.replace("    airflow    = {", '    kibana     = { port = 5601, page = "the Kibana page", present = true }\n    airflow    = {', 1))
    removed = tmp_path / "removed.tf"
    removed.write_text(re.sub(r"^\s+prometheus\s+= \{.*\n", "", text, flags=re.MULTILINE))
    persistent = web_ui_terraform.OPENMETADATA_FORWARD
    check(web_ui_terraform.forward_documents(added, persistent).get("kibana") == 5601, "a map entry must be accepted without any change to the script")
    check("prometheus" not in web_ui_terraform.forward_documents(removed, persistent), "a removed map entry must not be accepted")


def test_every_listed_interface_runs_the_exact_session_manager_call(tmp_path: Path) -> None:
    for name, port in sorted(web_ui_terraform.forward_documents().items()):
        folder = tmp_path / name
        folder.mkdir()
        workspace = Workspace(folder, instances(INSTANCE))
        result = workspace.run(name)
        check(result.returncode == 0, f"{name}: exit {result.returncode}: {result.stderr}")
        check(f"http://127.0.0.1:{port}" in result.stdout, f"{name}: the output must give the browser address: {result.stdout}")
        calls = workspace.calls()
        check(len(calls) == 2, f"{name}: expected the lookup and the session, saw {calls}")
        lookup, session = calls
        check(lookup[:2] == ["ec2", "describe-instances"], f"{name}: the first call must be the instance lookup: {lookup}")
        check("Name=tag:Name,Values=example_project_host_dev" in lookup, f"{name}: the lookup must filter by the host Name tag: {lookup}")
        check("Name=instance-state-name,Values=running" in lookup, f"{name}: the lookup must filter by the running state: {lookup}")
        expected = [
            "ssm",
            "start-session",
            "--target",
            INSTANCE,
            "--document-name",
            f"example_project_{name}_forward_dev",
            "--parameters",
            f"localPortNumber={port}",
            "--profile",
            PROFILE,
            "--region",
            REGION,
        ]
        check(session == expected, f"{name}: the session call differs:\n{session}\n{expected}")


def test_a_chosen_local_port_changes_only_the_local_port(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    result = workspace.run("airflow", "--local-port", "18080")
    check(result.returncode == 0, f"exit {result.returncode}: {result.stderr}")
    check("http://127.0.0.1:18080" in result.stdout, f"the address must use the chosen port: {result.stdout}")
    check("localPortNumber=18080" in workspace.calls()[1], f"the session must use the chosen port: {workspace.calls()[1]}")


def test_a_bad_local_port_is_refused_before_any_aws_call(tmp_path: Path) -> None:
    for bad in ("0", "65536", "abc", "-1", "8080 "):
        folder = tmp_path / re.sub(r"\W", "_", bad or "empty")
        folder.mkdir()
        workspace = Workspace(folder, instances(INSTANCE))
        result = workspace.run("airflow", "--local-port", bad)
        check(result.returncode == 2, f"local port {bad!r} must exit 2: {result.returncode} {result.stderr}")
        check(workspace.calls() == [], f"local port {bad!r} must make no AWS call: {workspace.calls()}")


def test_a_name_that_is_not_a_web_interface_is_refused_before_any_aws_call(tmp_path: Path) -> None:
    for number, name in enumerate(("postgres", "catalog", "AWS-StartPortForwardingSession", "../airflow", "airflow;ls", "")):
        folder = tmp_path / f"case{number}"
        folder.mkdir()
        workspace = Workspace(folder, instances(INSTANCE))
        result = workspace.run(name)
        check(result.returncode == 2, f"{name!r} must exit 2: {result.returncode} {result.stderr}")
        check("not a web interface" in result.stderr, f"{name!r}: the message must say so: {result.stderr}")
        check(workspace.calls() == [], f"{name!r} must make no AWS call: {workspace.calls()}")


def test_no_running_host_is_refused_without_a_session(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances())
    result = workspace.run("airflow")
    check(result.returncode == 1, f"no host must exit 1: {result.returncode} {result.stderr}")
    check(
        "example_project_host_dev" in result.stderr and "host.sh start" in result.stderr,
        f"the message must name the tag and the start command: {result.stderr}",
    )
    check(all(call[:2] != ["ssm", "start-session"] for call in workspace.calls()), "no session may start without a host")


def test_two_hosts_are_refused_without_a_session(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE, OTHER_INSTANCE))
    result = workspace.run("airflow")
    check(result.returncode == 1, f"two hosts must exit 1: {result.returncode} {result.stderr}")
    check("2 running instances" in result.stderr, f"the message must give the count: {result.stderr}")
    check(INSTANCE not in result.stderr and OTHER_INSTANCE not in result.stderr, "the message must not print an instance ID")
    check(all(call[:2] != ["ssm", "start-session"] for call in workspace.calls()), "no session may start with two hosts")


def test_a_missing_env_value_or_file_is_refused_before_any_aws_call(tmp_path: Path) -> None:
    for name in ENV_VALUES:
        folder = tmp_path / name
        folder.mkdir()
        workspace = Workspace(folder, instances(INSTANCE), {key: value for key, value in ENV_VALUES.items() if key != name})
        result = workspace.run("airflow")
        check(result.returncode == 1, f"without {name} the script must exit 1: {result.returncode} {result.stderr}")
        check(f"Set a valid {name}" in result.stderr, f"without {name} the message must name it: {result.stderr}")
        check(workspace.calls() == [], f"without {name} no AWS call may happen: {workspace.calls()}")
    folder = tmp_path / "no_file"
    folder.mkdir()
    workspace = Workspace(folder, instances(INSTANCE))
    result = workspace.run("airflow", env_file=folder / "absent.env")
    check(result.returncode == 1 and ".env file does not exist" in result.stderr, f"a missing file must be named: {result.returncode} {result.stderr}")
    check(workspace.calls() == [], "a missing file must make no AWS call")


def test_a_failed_lookup_is_refused_without_a_session(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, "not json")
    result = workspace.run("airflow")
    check(result.returncode == 1, f"an unreadable lookup answer must exit 1: {result.returncode} {result.stderr}")
    check(all(call[:2] != ["ssm", "start-session"] for call in workspace.calls()), "no session may start after a failed lookup")


def test_a_failed_session_reports_only_the_error_category(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    result = workspace.run("airflow", session_error="TargetNotConnected")
    check(result.returncode == 1, f"a failed session must exit 1: {result.returncode}")
    check("TargetNotConnected" in result.stderr, f"the message must name the error category: {result.stderr}")
    check(
        "arn:aws" not in result.stdout + result.stderr and "111111111111" not in result.stdout + result.stderr, "the output must name no ARN and no account ID"
    )


def list_run(*arguments: str, cwd: Path) -> CompletedProcess[str]:
    """Run ``--list`` with no ``aws`` on ``PATH``, from a folder without ``.env``."""
    return run_command(
        sys.executable, ["-m", "scripts.infrastructure.open_ui", *arguments], cwd=cwd, env={"PATH": "/usr/bin:/bin", "PYTHONPATH": str(ROOT)}, timeout=60
    )


def test_list_prints_service_port_and_health_path_without_aws_or_env(tmp_path: Path) -> None:
    result = list_run("--list", cwd=tmp_path)
    check(result.returncode == 0, f"--list must succeed without AWS and .env: {result.returncode} {result.stderr}")
    rows = [re.split(r"\s{2,}", line.strip()) for line in result.stdout.splitlines()]
    check(rows[0] == ["Service", "Port", "Health path"], f"the header must be exactly the three columns: {rows[0]}")
    check(all(len(row) == 3 for row in rows), f"every row must have exactly three columns: {rows}")
    documents = web_ui_terraform.forward_documents()
    check({row[0]: int(row[1]) for row in rows[1:]} == documents, f"the rows must be the documents and their ports: {rows[1:]} {documents}")
    check(["airflow", "8080", "/api/v2/version"] in rows, f"the Airflow row must carry its health path: {rows}")
    check(["openmetadata", "8585", "None"] in rows, f"a service whose health check calls no path must show None: {rows}")
    check(not any(unknown[2] == "unknown" for unknown in rows[1:]), f"every port must have exactly one service listener: {rows}")


def test_list_refuses_a_name_a_local_port_and_a_missing_name(tmp_path: Path) -> None:
    for arguments in (("--list", "airflow"), ("--list", "--local-port", "9000")):
        result = list_run(*arguments, cwd=tmp_path)
        check(
            result.returncode == 2 and "--list takes no name" in result.stderr,
            f"{arguments}: --list must refuse extra arguments: {result.returncode} {result.stderr}",
        )
    result = list_run(cwd=tmp_path)
    check(result.returncode == 2 and "use --list" in result.stderr, f"no name and no --list must exit 2: {result.returncode} {result.stderr}")
