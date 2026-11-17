"""``scripts/infrastructure/open_ui.py`` opens only a web interface the Terraform declares, on the one host, with the exact Session Manager call.

Every case runs the real script as a child process with a stand-in ``aws`` first on ``PATH``. The stand-in answers the instance lookup from a file and
records every call, one argument per line, so a case can compare the exact argument list of each call. Nothing reaches AWS.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import signal
import sys
import time
from pathlib import Path

import pytest

from scripts.infrastructure import open_ui, web_ui_terraform
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
  if [ -z "$STAND_IN_SESSION_MODE" ]; then exit 0; fi
  stamp() { perl -MTime::HiRes=time -e 'printf "%.3f\n", time'; }
  echo "$$" > "$STAND_IN_PIDFILE"
  echo "start $(stamp)" >> "$STAND_IN_MARK"
  if [ -n "$STAND_IN_PS_BREAK" ]; then : > "$STAND_IN_PS_BREAK"; fi
  sleep "${STAND_IN_SLEEP:-60}" &
  sleeper=$!
  case "$STAND_IN_SESSION_MODE" in
    interrupt | hang) trap 'echo "int $(stamp)" >> "$STAND_IN_MARK"; kill "$sleeper" 2>/dev/null; exit 0' INT ;;
    *) trap 'echo "int $(stamp)" >> "$STAND_IN_MARK"' INT ;;
  esac
  case "$STAND_IN_SESSION_MODE" in
    ignore_all) trap 'echo "term $(stamp)" >> "$STAND_IN_MARK"' TERM ;;
    *) trap 'echo "term $(stamp)" >> "$STAND_IN_MARK"; kill "$sleeper" 2>/dev/null; exit 0' TERM ;;
  esac
  if [ "$STAND_IN_SESSION_MODE" != hang ]; then kill -INT "$PPID"; fi
  if [ -n "$STAND_IN_REPEAT_AFTER" ]; then
    sleep "$STAND_IN_REPEAT_AFTER"
    kill -INT "$PPID"
  fi
  while [ "$STAND_IN_SESSION_MODE" = ignore_all ]; do sleep 1; done
  while kill -0 "$sleeper" 2>/dev/null; do wait "$sleeper"; done
  exit 0
fi
exit 3
"""


PS_STAND_IN = """#!/bin/bash
sleep "${STAND_IN_PS_DELAY:-0}"
if [ -n "$STAND_IN_PS_BREAK" ] && [ -e "$STAND_IN_PS_BREAK" ]; then
  echo "ps: the process table cannot be read" >&2
  exit 1
fi
exec /bin/ps "$@"
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
        (self.bin / "ps").write_text(PS_STAND_IN)
        (self.bin / "ps").chmod(0o755)
        self.log = folder / "calls.log"
        self.answer = folder / "instances.json"
        self.answer.write_text(answer)
        self.env_file = folder / ".env"
        self.env_file.write_text("".join(f"{key}={value}\n" for key, value in (ENV_VALUES if values is None else values).items()))

    def run(self, *arguments: str, env_file: Path | None = None, session_error: str = "", extra: dict[str, str] | None = None) -> CompletedProcess[str]:
        environment = {
            "PATH": f"{self.bin}{os.pathsep}/usr/bin{os.pathsep}/bin",
            "STAND_IN_LOG": str(self.log),
            "STAND_IN_INSTANCES": str(self.answer),
            "STAND_IN_SESSION_ERROR": session_error,
            "STAND_IN_MARK": str(self.folder / "marks.log"),
            "STAND_IN_PIDFILE": str(self.folder / "child.pid"),
            **(extra or {}),
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


def test_a_refused_input_is_never_echoed(tmp_path: Path) -> None:
    arn = "arn:aws:ssm:us-west-2:111111111111:document/example_project_airflow_forward_dev"
    account = "111111111111"
    inputs = {
        "an ARN as the name": [arn],
        "an account ID as the name": [account],
        "an ARN as an extra name": ["airflow", arn],
        "an unknown option with an ARN": [f"--document={arn}", "airflow"],
        "an account ID as an unknown option": [f"--{account}", "airflow"],
        "an ARN as the local port": ["airflow", "--local-port", arn],
        "an account ID as the local port": ["airflow", "--local-port", account + "1"],
    }
    for number, (case, arguments) in enumerate(inputs.items()):
        folder = tmp_path / f"case{number}"
        folder.mkdir()
        workspace = Workspace(folder, instances(INSTANCE))
        result = workspace.run(*arguments)
        output = result.stdout + result.stderr
        check(result.returncode == 2, f"{case}: it must exit 2: {result.returncode} {output}")
        check(arn not in output and account not in output and "arn:aws" not in output, f"{case}: the output must not echo any input: {output}")
        check(workspace.calls() == [], f"{case}: it must make no AWS call: {workspace.calls()}")
    folder = tmp_path / "names"
    folder.mkdir()
    refused = Workspace(folder, instances(INSTANCE)).run(arn)
    check(all(name in refused.stderr for name in web_ui_terraform.forward_documents()), f"a refused name must list every accepted name: {refused.stderr}")


def mark_times(workspace: Workspace) -> list[tuple[str, float]]:
    """What the stand-in Session Manager child recorded, in order: ``start``, then each signal it received, each with its time."""
    path = workspace.folder / "marks.log"
    return [(name, float(stamp)) for name, stamp in (line.split() for line in path.read_text().splitlines())] if path.exists() else []


def marks(workspace: Workspace) -> list[str]:
    """The signals the stand-in Session Manager child received, in order."""
    return [name for name, _ in mark_times(workspace) if name != "start"]


def seconds_between(workspace: Workspace, first: str, second: str) -> float:
    """The time from the first mark named ``first`` to the first mark named ``second`` after it."""
    times = mark_times(workspace)
    begin = next(stamp for name, stamp in times if name == first)
    return next(stamp for name, stamp in times if name == second and stamp >= begin) - begin


def stop_child(workspace: Workspace) -> None:
    """Make sure no stand-in child and nothing in its process group (its sleeper) is left running, whatever the case did."""
    path = workspace.folder / "child.pid"
    if path.exists():
        with contextlib.suppress(ProcessLookupError):
            os.killpg(int(path.read_text()), signal.SIGKILL)


def test_ctrl_c_sends_the_interrupt_to_the_aws_child_and_lets_it_close_the_session(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    result = workspace.run("airflow", extra={"STAND_IN_SESSION_MODE": "interrupt", "HAI_OPEN_UI_STOP_SECONDS": "20"})
    check(result.returncode == 0, f"a Ctrl+C that the child handles must end with exit 0: {result.returncode} {result.stderr}")
    check(marks(workspace) == ["int"], f"the child must get SIGINT only, nothing stronger: {marks(workspace)}")
    check("the session is closed" in result.stdout, f"the script must say the session is closed: {result.stdout}")
    check("cannot confirm" in result.stdout, f"the script must state that it cannot confirm the AWS side: {result.stdout}")


def test_a_child_that_ignores_the_interrupt_gets_term_only_after_the_bounded_wait(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    result = workspace.run("airflow", extra={"STAND_IN_SESSION_MODE": "ignore_int", "HAI_OPEN_UI_STOP_SECONDS": "1"})
    check(result.returncode == 0, f"a child that ends on TERM must end with exit 0: {result.returncode} {result.stderr}")
    check(marks(workspace) == ["int", "term"], f"the child must get SIGINT, then SIGTERM: {marks(workspace)}")


def test_a_child_that_ignores_the_interrupt_and_term_is_killed_last_and_reported(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    result = workspace.run("airflow", extra={"STAND_IN_SESSION_MODE": "ignore_all", "HAI_OPEN_UI_STOP_SECONDS": "1"})
    check(result.returncode == 1, f"a killed child must end with exit 1: {result.returncode} {result.stdout} {result.stderr}")
    check(marks(workspace) == ["int", "term"], f"the child must get SIGINT, then SIGTERM, then the kill: {marks(workspace)}")
    check("did not close" in result.stderr and "idle timeout" in result.stderr, f"the message must say the AWS session may stay open: {result.stderr}")
    pid = int((workspace.folder / "child.pid").read_text())
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        pass
    else:
        raise AssertionError(f"the child {pid} must not be left running")


def test_the_session_deadline_stops_the_child_by_the_same_graceful_steps(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    result = workspace.run("airflow", extra={"STAND_IN_SESSION_MODE": "hang", "HAI_OPEN_UI_SESSION_SECONDS": "1", "HAI_OPEN_UI_STOP_SECONDS": "20"})
    check(result.returncode == 1, f"the deadline must end with exit 1: {result.returncode} {result.stdout} {result.stderr}")
    check("deadline" in result.stderr, f"the message must name the deadline: {result.stderr}")
    check(marks(workspace) == ["int"], f"the child must get SIGINT first and only: {marks(workspace)}")


def test_a_bad_time_setting_is_refused_before_any_aws_call(tmp_path: Path) -> None:
    for name in ("HAI_OPEN_UI_STOP_SECONDS", "HAI_OPEN_UI_SESSION_SECONDS"):
        for value in ("abc", "0", "-5", "nan"):
            folder = tmp_path / f"{name}{value}"
            folder.mkdir()
            workspace = Workspace(folder, instances(INSTANCE))
            result = workspace.run("airflow", extra={name: value})
            check(result.returncode == 2 and name in result.stderr, f"{name}={value} must exit 2 and name the setting: {result.returncode} {result.stderr}")
            check(value not in result.stderr.replace(name, ""), f"{name}={value}: the value must not be echoed: {result.stderr}")
            check(workspace.calls() == [], f"{name}={value}: no AWS call may happen")


def test_a_second_ctrl_c_right_after_the_first_still_stops_the_session_cleanly(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    try:
        result = workspace.run(
            "airflow",
            extra={
                "STAND_IN_SESSION_MODE": "interrupt",
                "STAND_IN_REPEAT_AFTER": "0.5",
                "STAND_IN_PS_DELAY": "1.0",
                "STAND_IN_SLEEP": "8",
                "HAI_OPEN_UI_STOP_SECONDS": "20",
            },
        )
        output = result.stdout + result.stderr
        check("Traceback" not in output and "KeyboardInterrupt" not in output, f"a second Ctrl+C must not escape as an error: {output}")
        check(result.returncode == 0, f"the session must close cleanly: {result.returncode} {output}")
        check(marks(workspace) == ["int"], f"the child must still get SIGINT from the stop steps: {marks(workspace)}")
        pid = int((workspace.folder / "child.pid").read_text())
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            pass
        else:
            raise AssertionError(f"the child {pid} must not be left running")
    finally:
        stop_child(workspace)


def test_the_wait_after_each_signal_is_the_configured_time(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    try:
        began = time.monotonic()
        result = workspace.run("airflow", extra={"STAND_IN_SESSION_MODE": "ignore_all", "STAND_IN_SLEEP": "30", "HAI_OPEN_UI_STOP_SECONDS": "2"})
        elapsed = time.monotonic() - began
        check(result.returncode == 1, f"a killed child must end with exit 1: {result.returncode} {result.stderr}")
        gap = seconds_between(workspace, "int", "term")
        check(2.0 <= gap <= 3.5, f"SIGTERM must follow SIGINT after the configured 2 s, not {gap:.2f} s")
        check(elapsed >= 4.0, f"the kill must follow SIGTERM after another 2 s: the whole stop took {elapsed:.2f} s")
    finally:
        stop_child(workspace)


def test_the_default_wait_is_fifteen_seconds(tmp_path: Path) -> None:
    check(open_ui.STOP_SECONDS == 15.0 and open_ui.SESSION_SECONDS == 8 * 3600, "the defaults must be a 15 s wait and an 8 hour deadline")
    workspace = Workspace(tmp_path, instances(INSTANCE))
    try:
        result = workspace.run("airflow", extra={"STAND_IN_SESSION_MODE": "ignore_int", "STAND_IN_SLEEP": "40"})
        check(result.returncode == 0, f"a child that ends on SIGTERM must end with exit 0: {result.returncode} {result.stderr}")
        gap = seconds_between(workspace, "int", "term")
        check(14.8 <= gap <= 18.0, f"with no setting, SIGTERM must follow SIGINT after 15 s, not {gap:.2f} s")
    finally:
        stop_child(workspace)


def test_a_second_ctrl_c_skips_only_the_wait_it_interrupts(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    try:
        result = workspace.run(
            "airflow", extra={"STAND_IN_SESSION_MODE": "ignore_int", "STAND_IN_REPEAT_AFTER": "0.6", "STAND_IN_SLEEP": "30", "HAI_OPEN_UI_STOP_SECONDS": "20"}
        )
        check(result.returncode == 0, f"a child that ends on SIGTERM must end with exit 0: {result.returncode} {result.stderr}")
        check(marks(workspace)[0] == "int", f"the child must get SIGINT first: {marks(workspace)}")
        check(marks(workspace)[-1] == "term", f"the second Ctrl+C must move on to SIGTERM: {marks(workspace)}")
        gap = seconds_between(workspace, "int", "term")
        check(gap < 6.0, f"the second Ctrl+C must skip the 20 s wait, SIGTERM came after {gap:.2f} s")
    finally:
        stop_child(workspace)


def test_the_session_deadline_is_the_configured_time(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    try:
        result = workspace.run(
            "airflow", extra={"STAND_IN_SESSION_MODE": "hang", "STAND_IN_SLEEP": "30", "HAI_OPEN_UI_SESSION_SECONDS": "2", "HAI_OPEN_UI_STOP_SECONDS": "20"}
        )
        check(result.returncode == 1 and "deadline" in result.stderr, f"the deadline must end the session: {result.returncode} {result.stderr}")
        gap = seconds_between(workspace, "start", "int")
        check(2.0 <= gap <= 4.5, f"SIGINT must come at the configured 2 s deadline, not after {gap:.2f} s")
    finally:
        stop_child(workspace)


def test_a_failing_process_listing_cannot_stop_the_bounded_stop_of_the_owned_child(tmp_path: Path) -> None:
    for number, (mode, code, expected_marks, expected_sent) in enumerate(
        (("interrupt", 0, ["int"], "SIGINT"), ("ignore_all", 1, ["int", "term"], "SIGINT, SIGTERM, SIGKILL"))
    ):
        folder = tmp_path / f"case{number}"
        folder.mkdir()
        workspace = Workspace(folder, instances(INSTANCE))
        try:
            began = time.monotonic()
            result = workspace.run(
                "airflow",
                extra={
                    "STAND_IN_SESSION_MODE": mode,
                    "STAND_IN_SLEEP": "30",
                    "STAND_IN_PS_BREAK": str(folder / "ps_broken"),
                    "HAI_OPEN_UI_STOP_SECONDS": "1",
                },
            )
            elapsed = time.monotonic() - began
            output = result.stdout + result.stderr
            check((folder / "ps_broken").exists(), "the case needs the process listing to fail once the session runs")
            check(result.returncode == code, f"{mode}: exit {code} expected, got {result.returncode}: {output}")
            check(elapsed < 12.0, f"{mode}: the parent must exit within the bounded stop, it took {elapsed:.1f} s")
            check(marks(workspace) == expected_marks, f"{mode}: the child must get exactly {expected_marks}: {marks(workspace)}")
            if code:
                check(f"signals sent: {expected_sent}" in result.stderr, f"{mode}: the report must list the signals actually sent: {result.stderr}")
            pid = int((folder / "child.pid").read_text())
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                pass
            else:
                raise AssertionError(f"{mode}: the child {pid} must be gone when the parent exits")
            check("did not end after SIGKILL" not in output, f"{mode}: the report must not claim a SIGKILL that was not needed: {output}")
        finally:
            stop_child(workspace)


def test_a_failure_inside_the_stop_still_kills_the_owned_child(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    for name, value in {
        "PATH": f"{workspace.bin}{os.pathsep}/usr/bin{os.pathsep}/bin",
        "STAND_IN_LOG": str(workspace.log),
        "STAND_IN_MARK": str(workspace.folder / "marks.log"),
        "STAND_IN_PIDFILE": str(workspace.folder / "child.pid"),
        "STAND_IN_SESSION_MODE": "hang",
        "STAND_IN_SLEEP": "30",
    }.items():
        monkeypatch.setenv(name, value)

    def broken(session: object, stop_seconds: float) -> list[str]:
        raise RuntimeError("the stop failed")

    monkeypatch.setattr(open_ui, "stop_session", broken)
    try:
        with pytest.raises(RuntimeError, match="the stop failed"):
            open_ui.run_session(["ssm", "start-session"], 0.5, 1.0)
        time.sleep(0.5)
        pid = int((workspace.folder / "child.pid").read_text())
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            pass
        else:
            raise AssertionError(f"the child {pid} must be killed when the stop fails")
        check(signal.getsignal(signal.SIGINT) is signal.default_int_handler, "the SIGINT handler must be restored")
    finally:
        stop_child(workspace)
