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
if [ "$1 $2" = "ec2 describe-instances" ]; then
  if [ -n "$STAND_IN_LOOKUP_INTERRUPT" ]; then
    kill -INT "$PPID"
    sleep 5
  fi
  cat "$STAND_IN_INSTANCES"
  exit 0
fi
if [ "$1 $2" = "ssm start-session" ]; then
  if [ -n "$STAND_IN_SESSION_ERROR" ]; then
    echo "An error occurred ($STAND_IN_SESSION_ERROR) when calling StartSession: arn:aws:ssm:us-west-2:111111111111:x" >&2
    exit 254
  fi
  if [ -z "$STAND_IN_SESSION_MODE" ]; then exit 0; fi
  stamp() { perl -MTime::HiRes=time -e 'printf "%.3f\n", time'; }
  echo "$$" > "$STAND_IN_PIDFILE"
  echo "start $(stamp)" >> "$STAND_IN_MARK"
  echo "$(ps -o pgid= -p $$ | tr -d ' ') $(ps -o pgid= -p $PPID | tr -d ' ')" > "$STAND_IN_GROUPFILE"
  case "$STAND_IN_SESSION_MODE" in
    terminal_ctrl_c | plugin_exits)
      # Like the real CLI: ignore the interrupt here (a handler, so the plugin starts with the default) and let the plugin, a foreground child, handle it.
      trap 'echo "aws_int $(stamp)" >> "$STAND_IN_MARK"' INT
      session-manager-plugin
      exit $?
      ;;
    *)
      trap 'echo "int $(stamp)" >> "$STAND_IN_MARK"' INT
      case "$STAND_IN_SESSION_MODE" in
        stubborn) trap 'echo "term $(stamp)" >> "$STAND_IN_MARK"' TERM ;;
        *) trap 'echo "term $(stamp)" >> "$STAND_IN_MARK"; exit 0' TERM ;;
      esac
      while true; do sleep 0.1; done
      ;;
  esac
fi
exit 3
"""


PLUGIN_STAND_IN = """#!/bin/bash
stamp() { perl -MTime::HiRes=time -e 'printf "%.3f\\n", time'; }
echo "$$" > "$STAND_IN_PLUGIN_PIDFILE"
sleep "${STAND_IN_SLEEP:-20}" &
sleeper=$!
trap 'echo "plugin_int $(stamp)" >> "$STAND_IN_MARK"; kill "$sleeper" 2>/dev/null; exit 0' INT
if [ "$STAND_IN_SESSION_MODE" = terminal_ctrl_c ]; then kill -INT 0; fi
while kill -0 "$sleeper" 2>/dev/null; do wait "$sleeper"; done
exit 0
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
        (self.bin / "session-manager-plugin").write_text(PLUGIN_STAND_IN)
        (self.bin / "session-manager-plugin").chmod(0o755)
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
            "STAND_IN_PLUGIN_PIDFILE": str(self.folder / "plugin.pid"),
            "STAND_IN_GROUPFILE": str(self.folder / "groups.txt"),
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


def test_a_failed_session_reports_its_exit_code(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    result = workspace.run("airflow", session_error="TargetNotConnected")
    check(result.returncode == 1, f"a failed session must exit 1: {result.returncode}")
    check("exit code 254" in result.stderr, f"the message must give the exit code of aws: {result.stderr}")
    check("TargetNotConnected" in result.stderr, f"aws's own message must reach the terminal, which is the owner's: {result.stderr}")


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
    """What the stand-in processes recorded, in order: ``start``, then each signal one of them received, each with its time."""
    path = workspace.folder / "marks.log"
    return [(name, float(stamp)) for name, stamp in (line.split() for line in path.read_text().splitlines())] if path.exists() else []


def marks(workspace: Workspace) -> list[str]:
    """The signals the stand-in processes received, in order."""
    return [name for name, _ in mark_times(workspace) if name != "start"]


def seconds_between(workspace: Workspace, first: str, second: str) -> float:
    """The time from the first mark named ``first`` to the first mark named ``second`` after it."""
    times = mark_times(workspace)
    begin = next(stamp for name, stamp in times if name == first)
    return next(stamp for name, stamp in times if name == second and stamp >= begin) - begin


def pids(workspace: Workspace) -> list[int]:
    """The stand-in aws and plugin pids the case recorded."""
    return [int((workspace.folder / name).read_text()) for name in ("child.pid", "plugin.pid") if (workspace.folder / name).exists()]


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def stop_child(workspace: Workspace) -> None:
    """Make sure no stand-in process is left running, whatever the case did."""
    for pid in pids(workspace):
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)


def nothing_left(workspace: Workspace, case: str) -> None:
    for pid in pids(workspace):
        check(not alive(pid), f"{case}: the stand-in process {pid} must be gone when open_ui exits")


def deliver_to_stand_ins(workspace: Workspace, number: int) -> None:
    """Stand in for a signal to the process group: these cases run ``open_ui`` inside the test process, whose group is the developer's, so the group signal is
    sent to the stand-in processes only."""
    for pid in pids(workspace):
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, number)


def test_a_terminal_ctrl_c_reaches_aws_and_the_plugin_and_leaves_nothing_running(tmp_path: Path) -> None:
    """A SIGINT to the whole process group, as a terminal sends it: open_ui, aws and the plugin share the group, open_ui outlives the child and reports."""
    workspace = Workspace(tmp_path, instances(INSTANCE))
    try:
        result = workspace.run("airflow", extra={"STAND_IN_SESSION_MODE": "terminal_ctrl_c"})
        output = result.stdout + result.stderr
        check("Traceback" not in output and "KeyboardInterrupt" not in output, f"open_ui must not die on the terminal's Ctrl+C: {output}")
        check(result.returncode == 0, f"the plugin closed the session on Ctrl+C: exit 0 expected, got {result.returncode}: {output}")
        check("the session is closed" in result.stdout and "cannot confirm" in result.stdout, f"open_ui must report after the child ends: {result.stdout}")
        check(
            marks(workspace) == ["plugin_int", "aws_int"] or marks(workspace) == ["aws_int", "plugin_int"],
            f"aws and the plugin must both get the Ctrl+C: {marks(workspace)}",
        )
        aws_group, open_ui_group = (workspace.folder / "groups.txt").read_text().split()
        check(aws_group == open_ui_group, f"aws must run in open_ui's own process group, not a new one: {aws_group} {open_ui_group}")
        nothing_left(workspace, "terminal Ctrl+C")
    finally:
        stop_child(workspace)


def test_a_normal_end_leaves_nothing_running_and_reports_the_exit_code(tmp_path: Path) -> None:
    for number, mode in enumerate(("plugin_exits", "")):
        folder = tmp_path / f"case{number}"
        folder.mkdir()
        workspace = Workspace(folder, instances(INSTANCE))
        try:
            result = workspace.run("airflow", extra={"STAND_IN_SESSION_MODE": mode, "STAND_IN_SLEEP": "1"} if mode else {})
            check(result.returncode == 0, f"a session that ends by itself must exit 0: {result.returncode} {result.stderr}")
            nothing_left(workspace, "normal end")
        finally:
            stop_child(workspace)


def test_the_deadline_sends_only_the_interrupt_when_the_plugin_closes_on_it(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    try:
        began = time.monotonic()
        result = workspace.run("airflow", extra={"STAND_IN_SESSION_MODE": "plugin_exits", "HAI_OPEN_UI_SESSION_SECONDS": "1", "HAI_OPEN_UI_STOP_SECONDS": "20"})
        elapsed = time.monotonic() - began
        check(result.returncode == 1 and "deadline" in result.stderr, f"the deadline must end the session with exit 1: {result.returncode} {result.stderr}")
        check("signals sent: SIGINT" in result.stderr and "SIGTERM" not in result.stderr, f"only SIGINT may be sent: {result.stderr}")
        check("plugin_int" in marks(workspace), f"the plugin must get the interrupt: {marks(workspace)}")
        check(elapsed < 10.0, f"the stop must end as soon as the child is gone, not wait the 20 s: {elapsed:.1f} s")
        nothing_left(workspace, "deadline interrupt")
    finally:
        stop_child(workspace)


def test_the_deadline_escalates_only_as_far_as_needed(tmp_path: Path) -> None:
    for number, (mode, expected_sent, expected_marks) in enumerate(
        (("stubborn_term", "SIGINT, SIGTERM", ["int", "term"]), ("stubborn", "SIGINT, SIGTERM, SIGKILL", ["int", "term"]))
    ):
        folder = tmp_path / f"case{number}"
        folder.mkdir()
        workspace = Workspace(folder, instances(INSTANCE))
        try:
            began = time.monotonic()
            result = workspace.run("airflow", extra={"STAND_IN_SESSION_MODE": mode, "HAI_OPEN_UI_SESSION_SECONDS": "1", "HAI_OPEN_UI_STOP_SECONDS": "1"})
            elapsed = time.monotonic() - began
            check(result.returncode == 1, f"{mode}: the deadline must end with exit 1: {result.returncode} {result.stderr}")
            check(f"signals sent: {expected_sent})" in result.stderr, f"{mode}: the report must list exactly {expected_sent}: {result.stderr}")
            check(marks(workspace) == expected_marks, f"{mode}: the child must receive {expected_marks}: {marks(workspace)}")
            check(("SIGKILL" in result.stderr) == (mode == "stubborn"), f"{mode}: SIGKILL only when the child ignored the others: {result.stderr}")
            check(elapsed >= (3.0 if mode == "stubborn" else 2.0), f"{mode}: each signal must wait the configured second first: {elapsed:.1f} s")
            nothing_left(workspace, mode)
        finally:
            stop_child(workspace)


def test_no_signal_is_sent_once_the_child_is_reaped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    sent: list[tuple[str, int, int]] = []
    real_kill = os.kill
    for name, value in {
        "PATH": f"{workspace.bin}{os.pathsep}/usr/bin{os.pathsep}/bin",
        "STAND_IN_LOG": str(workspace.log),
        "STAND_IN_MARK": str(workspace.folder / "marks.log"),
        "STAND_IN_PIDFILE": str(workspace.folder / "child.pid"),
        "STAND_IN_PLUGIN_PIDFILE": str(workspace.folder / "plugin.pid"),
        "STAND_IN_GROUPFILE": str(workspace.folder / "groups.txt"),
    }.items():
        monkeypatch.setenv(name, value)

    def record_kill(pid: int, number: int) -> None:
        sent.append(("kill", pid, number))
        real_kill(pid, number)

    def record_killpg(group: int, number: int) -> None:
        sent.append(("killpg", group, number))
        for pid in pids(workspace):
            with contextlib.suppress(ProcessLookupError):
                real_kill(pid, number)

    monkeypatch.setattr(os, "kill", record_kill)
    monkeypatch.setattr(os, "killpg", record_killpg)
    try:
        for mode, expected in (("plugin_exits", [signal.SIGINT]), ("stubborn_term", [signal.SIGINT, signal.SIGTERM])):
            sent.clear()
            monkeypatch.setenv("STAND_IN_SESSION_MODE", mode)
            end = open_ui.run_session(["ssm", "start-session"], 0.5, 1.0)
            check(end.sent == [number.name for number in expected], f"{mode}: the signals sent must be {expected}: {end.sent}")
            check(
                [number for _, _, number in sent] == expected,
                f"{mode}: the operating system must see exactly {expected}, nothing after the child was reaped: {sent}",
            )
            check(end.alive is False and end.reason == "deadline", f"{mode}: the session must be over: {end}")
        check(not any(kind == "killpg" and number != signal.SIGINT for kind, _, number in sent), "only the interrupt may go to the group")
    finally:
        stop_child(workspace)


def test_an_error_inside_the_wait_still_stops_and_reaps_the_child(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    for name, value in {
        "PATH": f"{workspace.bin}{os.pathsep}/usr/bin{os.pathsep}/bin",
        "STAND_IN_LOG": str(workspace.log),
        "STAND_IN_MARK": str(workspace.folder / "marks.log"),
        "STAND_IN_PIDFILE": str(workspace.folder / "child.pid"),
        "STAND_IN_PLUGIN_PIDFILE": str(workspace.folder / "plugin.pid"),
        "STAND_IN_GROUPFILE": str(workspace.folder / "groups.txt"),
        "STAND_IN_SESSION_MODE": "stubborn_term",
    }.items():
        monkeypatch.setenv(name, value)

    def broken(end: float) -> bool:
        raise RuntimeError("the wait failed")

    monkeypatch.setattr(open_ui, "session_expired", broken)
    monkeypatch.setattr(os, "killpg", lambda group, number: deliver_to_stand_ins(workspace, number))
    try:
        with pytest.raises(RuntimeError, match="the wait failed"):
            open_ui.run_session(["ssm", "start-session"], 60.0, 1.0)
        nothing_left(workspace, "an error in the wait")
        check(signal.getsignal(signal.SIGINT) is signal.default_int_handler, "the SIGINT disposition must be restored")
    finally:
        stop_child(workspace)


def test_a_ctrl_c_before_the_session_starts_leaves_no_child(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    result = workspace.run("airflow", extra={"STAND_IN_LOOKUP_INTERRUPT": "1"})
    output = result.stdout + result.stderr
    check(result.returncode == 130, f"a Ctrl+C during the host lookup must exit 130: {result.returncode} {output}")
    check("Traceback" not in output and "interrupted" in result.stderr, f"the interruption must be reported plainly: {output}")
    check(all(call[:2] != ["ssm", "start-session"] for call in workspace.calls()), "no session may start after the interruption")


def test_the_defaults_are_a_fifteen_second_wait_and_an_eight_hour_deadline() -> None:
    check(open_ui.STOP_SECONDS == 15.0 and open_ui.SESSION_SECONDS == 8 * 3600, "the defaults must be a 15 s wait and an 8 hour deadline")


def test_the_default_wait_is_fifteen_seconds(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    try:
        result = workspace.run("airflow", extra={"STAND_IN_SESSION_MODE": "stubborn_term", "HAI_OPEN_UI_SESSION_SECONDS": "1"})
        check(result.returncode == 1, f"the deadline must end with exit 1: {result.returncode} {result.stderr}")
        gap = seconds_between(workspace, "int", "term")
        check(14.0 <= gap <= 18.0, f"with no stop setting, SIGTERM must follow SIGINT after 15 s, not {gap:.2f} s")
    finally:
        stop_child(workspace)


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


def test_a_child_that_exited_before_the_stop_gets_no_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    """The stop polls before every signal: a child that has exited (not yet reaped) is reaped and nothing is sent, so no signal can reach a reused pid."""
    sent: list[tuple[str, int]] = []
    monkeypatch.setattr(os, "kill", lambda pid, number: sent.append(("kill", number)))
    monkeypatch.setattr(os, "killpg", lambda group, number: sent.append(("killpg", number)))
    pid = os.posix_spawnp("true", ["true"], os.environ.copy())
    time.sleep(0.5)
    child = open_ui.Child(pid)
    check(open_ui.stop_child(child, 1.0) == [], "a child that already exited must get no signal")
    check(sent == [] and child.code == 0, f"nothing may be sent and the child must be reaped with its exit code: {sent} {child.code}")
