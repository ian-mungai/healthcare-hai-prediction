"""``open_ui`` validates everything, then ``scripts/open_ui.sh`` replaces itself with ``aws ssm start-session``: no process supervises the session.

Two layers are tested with a stand-in ``aws`` first on ``PATH``. The stand-in answers the instance lookup from a file and records every call, one argument per
line. Cases on ``scripts.infrastructure.open_ui`` read the validated command it writes to its ``--exec-args-file`` (``vector``), and cases on the real
``scripts/open_ui.sh`` in a fake checkout prove the exec: the exact arguments, the exact environment, no process between the test and ``aws`` and no file left
behind. Nothing reaches AWS.
"""

from __future__ import annotations

import ast
import json
import os
import re
import shutil
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
if [ "$1 $2" = "ec2 describe-instances" ]; then
  if [ -n "$STAND_IN_LOOKUP_INTERRUPT" ]; then
    kill -INT "$PPID"
    sleep 5
  fi
  cat "$STAND_IN_INSTANCES"
  exit 0
fi
if [ "$1 $2" = "ssm start-session" ]; then
  env | grep '^AWS_' | sort > "$STAND_IN_ENVFILE"
  echo "$PPID" > "$STAND_IN_PPIDFILE"
  if [ -n "$STAND_IN_SESSION_ERROR" ]; then
    echo "An error occurred ($STAND_IN_SESSION_ERROR) when calling StartSession" >&2
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
        self.command_file = folder / "command.bin"
        self.env_file.write_text("".join(f"{key}={value}\n" for key, value in (ENV_VALUES if values is None else values).items()))

    def run(self, *arguments: str, env_file: Path | None = None, session_error: str = "", extra: dict[str, str] | None = None) -> CompletedProcess[str]:
        environment = {
            "PATH": f"{self.bin}{os.pathsep}/usr/bin{os.pathsep}/bin",
            "STAND_IN_LOG": str(self.log),
            "STAND_IN_INSTANCES": str(self.answer),
            "STAND_IN_SESSION_ERROR": session_error,
            "STAND_IN_ENVFILE": str(self.folder / "session_env.txt"),
            "STAND_IN_PPIDFILE": str(self.folder / "session_ppid.txt"),
            **(extra or {}),
            "AWS_PROFILE": "an_inherited_profile",
            "AWS_REGION": "eu-west-1",
        }
        return run_command(
            sys.executable,
            ["-m", "scripts.infrastructure.open_ui", *arguments, "--env-file", str(env_file or self.env_file), "--exec-args-file", str(self.command_file)],
            cwd=ROOT,
            env=environment,
            timeout=60,
        )

    def vector(self) -> list[str] | None:
        """The validated command ``open_ui`` wrote for the shell to exec, or ``None`` when it wrote nothing."""
        if not self.command_file.exists() or self.command_file.stat().st_size == 0:
            return None
        return self.command_file.read_bytes().decode().split("\0")[:-1]

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


FIXED_ENVIRONMENT = ["AWS_IGNORE_CONFIGURED_ENDPOINT_URLS=true", "AWS_MAX_ATTEMPTS=3", "AWS_RETRY_MODE=standard"]


def expected_vector(name: str, port: int) -> list[str]:
    """The command the shell must exec: ``env`` without the inherited ``AWS_*`` settings (the test passes AWS_PROFILE and AWS_REGION), with the fixed ones,
    then ``aws`` and the exact Session Manager arguments."""
    return [
        "env",
        "-u",
        "AWS_PROFILE",
        "-u",
        "AWS_REGION",
        *FIXED_ENVIRONMENT,
        "aws",
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


def test_every_listed_interface_validates_to_the_exact_command(tmp_path: Path) -> None:
    for name, port in sorted(web_ui_terraform.forward_documents().items()):
        folder = tmp_path / name
        folder.mkdir()
        workspace = Workspace(folder, instances(INSTANCE))
        result = workspace.run(name)
        check(result.returncode == 0, f"{name}: exit {result.returncode}: {result.stderr}")
        check(f"http://127.0.0.1:{port}" in result.stdout, f"{name}: the output must give the browser address: {result.stdout}")
        calls = workspace.calls()
        check(len(calls) == 1, f"{name}: only the instance lookup may run, saw {calls}")
        lookup = calls[0]
        check(lookup[:2] == ["ec2", "describe-instances"], f"{name}: the call must be the instance lookup: {lookup}")
        check("Name=tag:Name,Values=example_project_host_dev" in lookup, f"{name}: the lookup must filter by the host Name tag: {lookup}")
        check("Name=instance-state-name,Values=running" in lookup, f"{name}: the lookup must filter by the running state: {lookup}")
        check(workspace.vector() == expected_vector(name, port), f"{name}: the validated command differs:\n{workspace.vector()}\n{expected_vector(name, port)}")


def test_a_chosen_local_port_changes_only_the_local_port(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    result = workspace.run("airflow", "--local-port", "18080")
    check(result.returncode == 0, f"exit {result.returncode}: {result.stderr}")
    check("http://127.0.0.1:18080" in result.stdout, f"the address must use the chosen port: {result.stdout}")
    check(workspace.vector() == expected_vector("airflow", 18080), f"only the local port may change: {workspace.vector()}")


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


def test_no_validation_failure_writes_a_command(tmp_path: Path) -> None:
    """Whatever refuses, nothing is left for the shell to exec: no command file and no start-session call."""
    refusals = {
        "an unknown name": ((), ["nothing_here"], instances(INSTANCE), None),
        "an extra name": ((), ["airflow", "grafana"], instances(INSTANCE), None),
        "a bad local port": ((), ["airflow", "--local-port", "0"], instances(INSTANCE), None),
        "no host": ((), ["airflow"], instances(), None),
        "two hosts": ((), ["airflow"], instances(INSTANCE, OTHER_INSTANCE), None),
        "an unreadable lookup": ((), ["airflow"], "not json", None),
        "a missing PROJECT_NAME": ((), ["airflow"], instances(INSTANCE), "PROJECT_NAME"),
        "a missing AWS_REGION": ((), ["airflow"], instances(INSTANCE), "AWS_REGION"),
    }
    for number, (case, (_unused, arguments, answer, missing)) in enumerate(refusals.items()):
        folder = tmp_path / f"case{number}"
        folder.mkdir()
        values = {key: value for key, value in ENV_VALUES.items() if key != missing} if missing else None
        workspace = Workspace(folder, answer, values)
        result = workspace.run(*arguments)
        check(result.returncode != 0, f"{case}: it must refuse: {result.returncode}")
        check(workspace.vector() is None, f"{case}: no command may be written for the shell to exec: {workspace.vector()}")
        check(all(call[:2] != ["ssm", "start-session"] for call in workspace.calls()), f"{case}: no session may start")


def test_running_without_the_command_file_is_refused_with_a_fixed_message(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    environment = {
        "PATH": f"{workspace.bin}{os.pathsep}/usr/bin{os.pathsep}/bin",
        "STAND_IN_LOG": str(workspace.log),
        "STAND_IN_INSTANCES": str(workspace.answer),
    }
    result = run_command(
        sys.executable, ["-m", "scripts.infrastructure.open_ui", "airflow", "--env-file", str(workspace.env_file)], cwd=ROOT, env=environment, timeout=60
    )
    check(
        result.returncode == 2 and "bash scripts/open_ui.sh" in result.stderr,
        f"a run without --exec-args-file must exit 2 and name the script: {result.stderr}",
    )
    check(workspace.calls() == [], f"nothing may be run before that check: {workspace.calls()}")


def test_a_ctrl_c_during_validation_exits_130_and_starts_nothing(tmp_path: Path) -> None:
    workspace = Workspace(tmp_path, instances(INSTANCE))
    result = workspace.run("airflow", extra={"STAND_IN_LOOKUP_INTERRUPT": "1"})
    output = result.stdout + result.stderr
    check(result.returncode == 130, f"a Ctrl+C during the host lookup must exit 130: {result.returncode} {output}")
    check("Nothing was started" in result.stderr and "Traceback" not in output, f"the interruption must be reported plainly: {output}")
    check(workspace.vector() is None, "no command may be written after an interruption")


def test_open_ui_never_creates_a_child_process_itself() -> None:
    """Static check of ``open_ui.py``: no subprocess, spawn, exec, fork, thread or signal machinery. The one child of a run, the instance lookup, is made by
    the repository's ``AwsCli`` and has ended before ``open_ui`` returns; the session is a ``scripts/open_ui.sh`` exec."""
    tree = ast.parse((ROOT / "scripts" / "infrastructure" / "open_ui.py").read_text())
    forbidden_modules = {"subprocess", "signal", "threading", "multiprocessing", "pty", "asyncio"}
    forbidden_names = {"run_command", "Popen", "posix_spawn", "posix_spawnp", "fork", "forkpty", "system", "popen", "start"}
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found += [alias.name for alias in node.names if alias.name.split(".")[0] in forbidden_modules]
        elif isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[0] in forbidden_modules | {"scripts.process"}:
            found.append(node.module or "")
        elif isinstance(node, ast.ImportFrom) and node.module == "scripts.process":
            found.append("scripts.process")
        elif isinstance(node, ast.Attribute | ast.Name):
            name = node.attr if isinstance(node, ast.Attribute) else node.id
            if name in forbidden_names or re.fullmatch(r"(exec|spawn)[lv]p?e?", name):
                found.append(name)
    check(found == [], f"open_ui.py must create no child process and run no session: {found}")
    text = (ROOT / "scripts" / "infrastructure" / "open_ui.py").read_text()
    check("HAI_OPEN_UI" not in text, "the removed session settings must not be read")


class Checkout:
    """A fake checkout: the real ``scripts/open_ui.sh``, a ``.venv/bin/python`` that runs the real interpreter on the real code, and the stand-in ``aws``."""

    def __init__(self, folder: Path, venv: bool = True) -> None:
        self.workspace = Workspace(folder, instances(INSTANCE))
        self.root = folder / "checkout"
        (self.root / "scripts").mkdir(parents=True)
        shutil.copy(ROOT / "scripts" / "open_ui.sh", self.root / "scripts" / "open_ui.sh")
        (self.root / ".env").write_text(self.workspace.env_file.read_text())
        if venv:
            (self.root / ".venv" / "bin").mkdir(parents=True)
            (self.root / ".venv" / "bin" / "python").write_text(f'#!/bin/bash\nexec "{sys.executable}" "$@"\n')
            (self.root / ".venv" / "bin" / "python").chmod(0o755)
        self.temporary = folder / "tmp"
        self.temporary.mkdir()

    def environment(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        return {
            "PATH": f"{self.workspace.bin}{os.pathsep}/usr/bin{os.pathsep}/bin",
            "PYTHONPATH": str(ROOT),
            "TMPDIR": str(self.temporary),
            "STAND_IN_LOG": str(self.workspace.log),
            "STAND_IN_INSTANCES": str(self.workspace.answer),
            "STAND_IN_ENVFILE": str(self.workspace.folder / "session_env.txt"),
            "STAND_IN_PPIDFILE": str(self.workspace.folder / "session_ppid.txt"),
            "STAND_IN_SESSION_ERROR": "",
            "AWS_PROFILE": "an_inherited_profile",
            "AWS_REGION": "eu-west-1",
            **(extra or {}),
        }

    def run(self, kind: str, extra: dict[str, str] | None = None) -> CompletedProcess[str]:
        """Run the wrapper. The Docker-call guard proves a shell's arguments only when they are literal, so each kind has its own literal command."""
        if kind == "list":
            command = ["scripts/open_ui.sh", "--list"]
        elif kind == "unknown":
            command = ["scripts/open_ui.sh", "nothing_here", "--env-file", ".env"]
        elif kind == "port":
            command = ["scripts/open_ui.sh", "airflow", "--local-port", "18080", "--env-file", ".env"]
        else:
            command = ["scripts/open_ui.sh", "airflow", "--env-file", ".env"]
        return run_command("/bin/bash", command, cwd=self.root, env=self.environment(extra), timeout=60)

    def link_run(self, target: Path) -> CompletedProcess[str]:
        """Run the wrapper through ``./link``, a symbolic link in a folder outside the checkout."""
        tools = self.workspace.folder / "tools"
        tools.mkdir(exist_ok=True)
        (tools / "link").symlink_to(target)
        return run_command("/bin/bash", ["./link", "airflow", "--env-file", ".env"], cwd=tools, env=self.environment(), timeout=60)

    def session_calls(self) -> list[list[str]]:
        return [call for call in self.workspace.calls() if call[:2] == ["ssm", "start-session"]]


def test_the_wrapper_replaces_itself_with_aws_and_nothing_supervises_it(tmp_path: Path) -> None:
    checkout = Checkout(tmp_path)
    result = checkout.run("run")
    check(result.returncode == 0, f"exit {result.returncode}: {result.stdout} {result.stderr}")
    check("http://127.0.0.1:8080" in result.stdout, f"the browser address must be printed before the session: {result.stdout}")
    sessions = checkout.session_calls()
    check(
        sessions == [expected_vector("airflow", 8080)[expected_vector("airflow", 8080).index("ssm") :]],
        f"aws must get exactly the validated arguments: {sessions}",
    )
    environment = (tmp_path / "session_env.txt").read_text().split()
    check(environment == FIXED_ENVIRONMENT, f"aws must get no inherited AWS_ setting, only the fixed ones: {environment}")
    parent = int((tmp_path / "session_ppid.txt").read_text())
    check(parent == os.getpid(), f"no wrapper or supervisor may stand between the caller and aws: aws's parent is {parent}, the caller is {os.getpid()}")
    check(not any(checkout.temporary.iterdir()), f"the wrapper must leave no file behind: {list(checkout.temporary.iterdir())}")


def test_a_chosen_local_port_reaches_aws_through_the_wrapper(tmp_path: Path) -> None:
    checkout = Checkout(tmp_path)
    result = checkout.run("port")
    check(result.returncode == 0, f"exit {result.returncode}: {result.stderr}")
    check("localPortNumber=18080" in checkout.session_calls()[0], f"the chosen port must reach aws: {checkout.session_calls()}")


def test_the_exit_code_of_aws_is_the_exit_code_of_the_wrapper(tmp_path: Path) -> None:
    checkout = Checkout(tmp_path)
    result = checkout.run("run", {"STAND_IN_SESSION_ERROR": "TargetNotConnected"})
    check(
        result.returncode == 254 and "TargetNotConnected" in result.stderr,
        f"aws's own exit code and message must come through: {result.returncode} {result.stderr}",
    )
    check(not any(checkout.temporary.iterdir()), "the wrapper must leave no file behind")


def test_list_and_a_refusal_execute_nothing_and_leave_no_file(tmp_path: Path) -> None:
    for kind in ("list", "unknown"):
        folder = tmp_path / kind
        folder.mkdir()
        checkout = Checkout(folder)
        result = checkout.run(kind)
        check(result.returncode == (0 if kind == "list" else 2), f"{kind}: exit {result.returncode}: {result.stderr}")
        check(checkout.workspace.calls() == [], f"{kind}: aws must not run at all: {checkout.workspace.calls()}")
        check(not any(checkout.temporary.iterdir()), f"{kind}: the wrapper must leave no file behind")
    check("Service" in result.stdout or "not a web interface" in result.stderr, "the last case must say why it ran nothing")


def test_a_ctrl_c_during_validation_through_the_wrapper_starts_nothing(tmp_path: Path) -> None:
    checkout = Checkout(tmp_path)
    result = checkout.run("run", {"STAND_IN_LOOKUP_INTERRUPT": "1"})
    output = result.stdout + result.stderr
    check(
        result.returncode == 130 and "Nothing was started" in result.stderr and "Traceback" not in output,
        f"exit 130 and a plain message expected: {result.returncode} {output}",
    )
    check(checkout.session_calls() == [], "no session may start")
    check(not any(checkout.temporary.iterdir()), "the wrapper must leave no file behind")


def test_a_missing_environment_names_the_install_script_and_runs_nothing(tmp_path: Path) -> None:
    checkout = Checkout(tmp_path, venv=False)
    result = checkout.run("run")
    check(result.returncode == 1 and "bash scripts/install.sh" in result.stderr, f"the install script must be named: {result.returncode} {result.stderr}")
    check(checkout.workspace.calls() == [], "nothing may run without .venv")


def test_the_wrapper_reached_through_a_symbolic_link_uses_its_own_checkout(tmp_path: Path) -> None:
    checkout = Checkout(tmp_path)
    result = checkout.link_run(checkout.root / "scripts" / "open_ui.sh")
    check(result.returncode == 0, f"exit {result.returncode}: {result.stderr}")
    check(len(checkout.session_calls()) == 1, f"the linked wrapper must start the session: {checkout.workspace.calls()}")


def test_the_wrapper_is_plain_bash_that_ends_in_one_exec() -> None:
    text = (ROOT / "scripts" / "open_ui.sh").read_text()
    check(text.startswith("#!/usr/bin/env bash\n") and "set -euo pipefail" in text, "it must be bash with set -euo pipefail")
    check(len(re.findall(r'^exec "\$\{command\[@\]\}"$', text, re.MULTILINE)) == 1, "it must end in exactly one exec of the validated command")
    check("eval" not in text and "bash scripts/install.sh" in text, "it must not use eval and must name the install script")
    check(re.search(r"^trap 'rm -f \"\$command_file\"' EXIT$", text, re.MULTILINE) is not None, "it must remove its command file on every exit")
