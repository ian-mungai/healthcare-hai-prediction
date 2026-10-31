"""Run every command that a document section prints through its real parser, with synthetic values, so that a typo fails before the owner runs it.

The commands come from the ``sh`` fences of a section. Each line is filled with synthetic values and sent to the parser of its program:

- ``aws`` runs the real command line without a profile or credentials, so it parses every option and fails only at the missing credentials;
- ``terraform`` runs the real command in an empty folder, so it parses the options and fails only at the missing state;
- ``python -m`` runs the module until its argument parser returns, and stops there;
- ``bash scripts/infrastructure/terraform.sh`` runs through the caller's stand-in stack;
- ``docker`` is never run, and the shell tools get a quoting check, because the line has to split into words.

Nothing here calls AWS or Docker. The rule that every ``aws`` line names a profile and a region is checked on the printed text."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import re
import runpy
import shlex
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
ACCOUNT = "111111111111"
SHELL_TOOLS = ("printf", "date", "openssl", "cat", "basename", "shasum", "mkdir", "cmp")
PARSE_FAILURES = re.compile(
    "|".join(
        (
            "Unknown options",
            "following arguments are required",
            "[Ii]nvalid choice",
            "usage:",
            "Parameter validation failed",
            "Error parsing parameter",
            "Invalid JSON",
            "Invalid value",
            "Unexpected argument",
            "Too many command line",
            "expected one argument",
            "unrecognized arguments",
            "Unknown output type",
            "SyntaxError",
            "Invalid type",
            "Invalid length",
        )
    )
)
PARSE_PASSED = re.compile("NoCredentials|Unable to locate credentials|could not be found|does not exist|No outputs found|Warning: No outputs")
Wrapper = Callable[[list[str]], tuple[int, str]]


def values(record_dir: Path, overrides: dict[str, str] | None = None) -> dict[str, str]:
    """The synthetic value of every placeholder a section may use. An unknown placeholder fails the check."""
    task = "0123456789abcdef0123456789abcdef"
    return {
        **(overrides or {}),
        "RECORD_DIR": str(record_dir),
        "PROJECT_PROFILE": "example_project_dev",
        "ADMIN_PROFILE": "example_admin",
        "REGION": "us-west-2",
        "PROJECT_NAME": "example_project",
        "ENVIRONMENT": "dev",
        "PROJECT_TAG": "example_project",
        "CLUSTER_NAME": "example_project_cluster_dev",
        "JOBS_GROUP": "example_project_jobs_dev",
        "DATA_BUCKET": "example-project-data-dev",
        "HOST_INSTANCE_ID": "i-0123456789abcdef0",
        "HOST_PRIVATE_IP": "10.0.1.20",
        "APPROVAL_ID": "approval-1",
        "RUN_ID": "20261031T120000Z-abcdef12",
        "SILVER_RUN_ID": "20261031T080000Z-12345678",
        "CODE_SHA256": "a" * 64,
        "CODE_SHA256_FIRST_12": "a" * 12,
        "SHA256": "b" * 64,
        "SIGN_TEXT": "sign-silver-20261031T120000Z-abcdef12-cccccccccccc",
        "TASK_ARN": f"arn:aws:ecs:us-west-2:{ACCOUNT}:task/example_project_cluster_dev/{task}",
        "TASK_ID": task,
        "SUBNET_ID": "subnet-0123456789abcdef0",
        "LAKEHOUSE_SECURITY_GROUP": "sg-0123456789abcdef0",
        "PLAN": "silver_step.tfplan",
        "OWNER": "Example Owner",
        "ANALYTICS_IMAGE": f"{ACCOUNT}.dkr.ecr.us-west-2.amazonaws.com/example_project_analytics_dev@sha256:{'d' * 64}",
    }


def section(text: str, heading: str) -> str:
    found = re.search(rf"^## {re.escape(heading)}\n(.*?)(?=^## |\Z)", text, re.S | re.M)
    return found.group(1) if found else ""


def commands(body: str) -> list[str]:
    """The logical command lines of the ``sh`` fences: a line that ends in a backslash continues on the next one."""
    lines: list[str] = []
    for fence in re.findall(r"```sh\n(.*?)```", body, re.S):
        pending = ""
        for raw in fence.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            pending += line[:-1].rstrip() + " " if line.endswith("\\") else line
            if not line.endswith("\\"):
                lines.append(pending)
                pending = ""
    return lines


def filled(line: str, table: dict[str, str]) -> str:
    """The line with every ``<NAME>`` replaced by its synthetic value. A name without a value raises."""
    unknown = sorted(set(re.findall(r"<([A-Z0-9_]+)>", line)) - table.keys())
    if unknown:
        raise KeyError(f"no synthetic value for {unknown}")
    return re.sub(r"<([A-Z0-9_]+)>", lambda match: table[match.group(1)], line)


def environment(folder: Path) -> dict[str, str]:
    base = {key: value for key, value in os.environ.items() if not key.startswith(("AWS_", "TF_"))}
    return base | {
        "AWS_CONFIG_FILE": os.devnull,
        "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_PAGER": "",
        "HOME": str(folder),
        "CHECKPOINT_DISABLE": "1",
    }


def parse_only(module: str, arguments: list[str], variables: dict[str, str]) -> tuple[int, str]:
    """Run a module's ``__main__`` until its ``argparse`` parser returns, then stop. A parse error ends the run with exit 2 and its message."""

    class Parsed(Exception):
        pass

    real = argparse.ArgumentParser.parse_known_args
    depth = [0]

    def stop(self: argparse.ArgumentParser, args: Any = None, namespace: Any = None) -> Any:
        """The outermost parse runs fully and then stops the program. The parse of a sub-command inside it returns as usual."""
        depth[0] += 1
        try:
            result = real(self, args, namespace)
        finally:
            depth[0] -= 1
        if depth[0] == 0:
            if result[1]:
                self.error(f"unrecognized arguments: {' '.join(result[1])}")
            raise Parsed
        return result

    err = io.StringIO()
    old_argv, old_env = sys.argv, dict(os.environ)
    sys.argv = [module, *arguments]
    os.environ.update(variables)
    try:
        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(io.StringIO()), _patched(argparse.ArgumentParser, "parse_known_args", stop):
            try:
                runpy.run_module(module, run_name="__main__", alter_sys=False)
            except Parsed:
                return 0, ""
            except ImportError as error:
                return 1, f"the module cannot be started: {error}"
            except SystemExit as stopped:
                return stopped.code if isinstance(stopped.code, int) else 2, err.getvalue()
        return 0, ""
    finally:
        sys.argv = old_argv
        os.environ.clear()
        os.environ.update(old_env)


@contextlib.contextmanager
def _patched(owner: Any, name: str, replacement: Any) -> Any:
    original = getattr(owner, name)
    setattr(owner, name, replacement)
    try:
        yield
    finally:
        setattr(owner, name, original)


def tokens_of(line: str) -> tuple[list[str], str | None]:
    """The words of a line and its output redirect, if any."""
    words = shlex.split(line)
    if ">" in words:
        index = words.index(">")
        return words[:index], words[index + 1] if index + 1 < len(words) else None
    return words, None


def check_line(line: str, folder: Path, wrapper: Wrapper) -> list[str]:
    """What is wrong with one filled command line. An empty list means its real parser accepted it."""
    words, redirect = tokens_of(line)
    leading = []
    while words and re.fullmatch(r"[A-Z_]+=\S*", words[0]):
        leading.append(words.pop(0))
    if not words:
        return ["the line has no program"]
    program = words[0]
    if program == "aws":
        if "--profile" not in words or "--region" not in words:
            return ["an aws command names its profile and region on the line"]
        at = words.index("--profile")
        stripped = [*words[:at], *words[at + 2 :]]
        result = run_command("aws", stripped[1:], cwd=folder, env=environment(folder), timeout=60)
        return (
            []
            if PARSE_PASSED.search(result.stderr) or (result.returncode == 0 and not PARSE_FAILURES.search(result.stderr))
            else [(result.stderr or result.stdout).strip()[-300:]]
        )
    if program == "terraform":
        directory = next((word.removeprefix("-chdir=") for word in words if word.startswith("-chdir=")), None)
        rest = [word for word in words[1:] if not word.startswith("-chdir=")]
        if directory is None or not (ROOT / directory).is_dir():
            return [f"the stack folder {directory} does not exist"]
        names = set(re.findall(r'^output "([a-z0-9_]+)"', "".join(path.read_text() for path in (ROOT / directory).glob("*.tf")), re.M))
        if rest[:1] == ["output"] and rest[-1] not in names:
            return [f"the stack declares no output {rest[-1]}"]
        empty = folder / "empty_stack"
        empty.mkdir(exist_ok=True)
        result = run_command("terraform", [f"-chdir={empty}", *rest], cwd=folder, env=environment(folder), timeout=60)
        return [] if not PARSE_FAILURES.search(result.stderr) else [result.stderr.strip()[-300:]]
    if program == "bash" and words[1:2] == ["scripts/infrastructure/terraform.sh"]:
        code, message = wrapper(words[2:])
        return [] if code == 0 else [message[-300:]]
    if program in SHELL_TOOLS:
        return []
    if program == "docker":
        return [] if words[1:2] == ["run"] else ["only docker run is listed, and it is never run here"]
    if program.endswith("python") and words[1:2] == ["-m"]:
        variables = {item.split("=", 1)[0]: item.split("=", 1)[1] for item in leading}
        code, message = parse_only(words[2], words[3:], variables)
        if code != 0:
            return [message.strip()[-300:] or f"exit {code}"]
        if words[2] == "scripts.orchestration.job_overrides" and redirect:
            result = run_command(
                sys.executable,
                ["-m", "scripts.orchestration.job_overrides", *words[3:]],
                cwd=ROOT,
                env=environment(folder) | {"PYTHONPATH": str(ROOT)},
                timeout=60,
            )
            if result.returncode != 0:
                return [result.stderr.strip()[-300:]]
            Path(redirect).parent.mkdir(parents=True, exist_ok=True)
            Path(redirect).write_text(result.stdout)
        return []
    return [f"no parser is known for {program}"]


def check_section(body: str, folder: Path, wrapper: Wrapper, overrides: dict[str, str] | None = None) -> dict[str, list[str]]:
    """Every command of the section against its parser: the line as printed, mapped to its problems."""
    table = values(folder / "record", overrides)
    (folder / "record").mkdir(parents=True, exist_ok=True)
    problems: dict[str, list[str]] = {}
    for line in commands(body):
        try:
            problems[line] = check_line(filled(line, table), folder, wrapper)
        except (KeyError, ValueError) as error:
            problems[line] = [str(error)]
    return problems


def example_json(body: str) -> dict[str, Any] | None:
    """The reviewed jobs file of the section: the first ``json`` fence with its placeholders filled."""
    found = re.search(r"```json\n(.*?)```", body, re.S)
    if not found:
        return None
    table = values(Path("record")) | {
        "REF": f"{ACCOUNT}.dkr.ecr.us-west-2.amazonaws.com/example_project_spark_dev@sha256:{'e' * 64}",
        "EXECUTION_ARN": f"arn:aws:iam::{ACCOUNT}:role/example_project_job_execution_dev",
        "SILVER_TASK_ARN": f"arn:aws:iam::{ACCOUNT}:role/example_project_job_write_silver_task_dev",
        "ID_NAME": "/example_project/aws_polaris_silver_client_id",
        "SECRET_NAME": "/example_project/aws_polaris_silver_client_secret",
    }
    try:
        return json.loads(filled(found.group(1), table))
    except (KeyError, ValueError):
        return None
