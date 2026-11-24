"""E2E of the committed owner scripts in scripts/infrastructure/owner: the steps a script has, the flags each command names and every command against
its parser.

    .venv/bin/python -m scripts.infrastructure.run_owner_scripts_e2e --out <new folder>

The commands of the script ``code_checkout.sh`` run through the parser of their real programs with synthetic values
(``scripts.infrastructure.owner_commands``), so a typo, a missing argument or a wrong option fails here and not on the owner's first run. The reviewed
services file that the script describes passes the real renderer through the real wrapper. Each script is checked as delivered: bash and zsh parse it, no
placeholder is left, every shell variable a command reads was set for good by an earlier command, and the steps the script must have are all there. The alert
email steps also run against stand-in tools (``alert_email_block_cases``). Nothing calls AWS or Docker.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

from scripts.infrastructure import alert_email_block_cases, owner_commands, services_code_inputs
from scripts.infrastructure.run_activation_inputs_e2e import ADMIN, ROOT, Workspace
from scripts.process import run_command

OWNER_FOLDER = ROOT / "scripts/infrastructure/owner"
CODE = "code_checkout.sh"
RESOLVE = "code_checkout_resolve_apply_lock.sh"
STAGING = "code_staging_github.sh"
ALERT = "alert_email_bootstrap"
TEARDOWN = "alert_email_teardown"
ALERT_FILES = (
    "alert_email_bootstrap_02_policy_plan.sh",
    "alert_email_bootstrap_03_policy_apply.sh",
    "alert_email_bootstrap_04_platform_plan.sh",
    "alert_email_bootstrap_05_platform_apply.sh",
    "alert_email_bootstrap_06_topics.sh",
    "alert_email_bootstrap_07_prepare.sh",
    "alert_email_bootstrap_08_alerts_plan.sh",
    "alert_email_bootstrap_09_alerts_apply.sh",
    "alert_email_bootstrap_11_subscriptions.sh",
    "alert_email_teardown_1_state.sh",
    "alert_email_teardown_2_plan.sh",
    "alert_email_teardown_3_apply.sh",
    "alert_email_teardown_4_subscriptions.sh",
)
STATE_PREFIX = "state_prefix_apply.sh"
STATE_PROJECT = "state_migration.sh"
STATE_IAM = "state_migration_iam.sh"
STATE_FILES = (STATE_PREFIX, STATE_PROJECT, STATE_IAM)
EXPECTED_SCRIPTS = frozenset({CODE, RESOLVE, STAGING, *ALERT_FILES, *STATE_FILES})
CODE_STEPS = tuple(range(7))
IAM_FLAGS = {"host_code_fetch_prepared": True}
STACK_OPTIONS = ("--iam", "--platform", "--services", "--jobs", "--alerts")


def script_text(name: str) -> str:
    """The text of one committed owner script."""
    return (OWNER_FOLDER / name).read_text(encoding="utf-8")


def step_numbers(text: str) -> list[int]:
    """The numbers of the ``# Step N:`` markers of a script, in order."""
    return [int(number) for number in re.findall(r"(?m)^# Step (\d+):", text)]


def make_wrapper(workspace: Workspace, samples: dict[str, dict[str, Any]]) -> Any:
    """The wrapper runner of the command checks: the documented options in plan mode through the stand-in stack, and the apply grammar by its own rule."""

    def run(arguments: list[str]) -> tuple[int, str]:
        stack = next((item[2:] for item in arguments if item in STACK_OPTIONS), "storage")
        action = next((index for index, item in enumerate(arguments) if item in ("plan", "apply", "validate")), -1)
        if action < 0:
            return 2, "the wrapper needs plan, apply or validate"
        if arguments[action] == "apply":
            rest = arguments[action + 1 :]
            return (0, "") if len(rest) == 1 and not rest[0].startswith("-") else (2, "apply takes one saved plan file")
        options = arguments[:action]
        if stack == "iam":
            workspace.command("iam", IAM_FLAGS)
        if "--preparation-inputs" in options:
            path = Path(options[options.index("--preparation-inputs") + 1])
            path = path if path.is_absolute() else workspace.root / path
            options[options.index("--preparation-inputs") + 1] = str(path)
            sample = samples.get(path.name)
            if sample is None:
                return 2, f"no reviewed sample for {path.name}"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(sample, sort_keys=True) + "\n")
            options[options.index("--preparation-sha256") + 1] = hashlib.sha256(path.read_bytes()).hexdigest()
        (workspace.root / ".env").write_text(workspace.environment_file.read_text())
        line = " ".join(shlex.quote(item) for item in ["scripts/infrastructure/terraform.sh", *options, "plan", "-out", "step.tfplan"])
        (workspace.root / "call_wrapper.sh").write_text(f"#!/bin/bash\nexec bash {line}\n")
        result = run_command("bash", ["call_wrapper.sh"], cwd=workspace.root, env=workspace.environment() | {"PYTHON_BIN": sys.executable}, timeout=60)
        return result.returncode, result.stderr + result.stdout

    return run


def structure_checks(code: str) -> dict[str, bool]:
    """The folder holds exactly the scripts the procedures need, each one a runnable bash script, and the code checkout holds every one of its steps."""
    present = {path.name for path in OWNER_FOLDER.iterdir()}
    numbers = step_numbers(code)
    read = {name for text in (code, script_text(RESOLVE)) for line in owner_commands.commands(text) for name in owner_commands.REFERENCE.findall(line)}
    known = set(owner_commands.shell_values(Path("record")))
    unused, unset = known - read - {"PWD"}, read - known
    return {
        f"the owner folder holds exactly the expected scripts (missing {sorted(EXPECTED_SCRIPTS - present)}, extra {sorted(present - EXPECTED_SCRIPTS)})": (
            present == EXPECTED_SCRIPTS
        ),
        "every owner script is executable and starts with a bash shebang": all(
            os.access(OWNER_FOLDER / name, os.X_OK) and script_text(name).startswith("#!/usr/bin/env bash\n") for name in sorted(present & EXPECTED_SCRIPTS)
        ),
        f"the code checkout holds steps 0 to {CODE_STEPS[-1]} in order ({numbers})": numbers == list(CODE_STEPS),
        f"every synthetic value is read by a code script and every variable they read has one (unused {sorted(unused)}, missing {sorted(unset)})": (
            not unused and not unset
        ),
    }


def services_code_sample() -> dict[str, Any]:
    """The file that services_code_inputs writes for a synthetic installation that runs MLflow: supported settings kept, installer role and bucket set."""
    existing = {
        "aws_region": "us-west-2",
        "mlflow_artifact_bucket": "example-project-mlflow-dev",
        "mlflow_enabled": True,
        "mlflow_task_role_arn": f"arn:aws:iam::{owner_commands.ACCOUNT}:role/example_project_mlflow_task_dev",
        "project_name": "example-project",
    }
    role = f"arn:aws:iam::{owner_commands.ACCOUNT}:role/example_project_host_code_fetch_task_dev"
    document, _ = services_code_inputs.build(existing, role, "example-project-data-dev")
    return document


ZSH = Path("/bin/zsh")


def unset_names(lines: list[str]) -> list[str]:
    """The shell variables that a command reads before an earlier command set them for good. A line that only assigns variables, and ``read -r NAME``, set them
    for the lines after it. ``NAME=value command`` sets NAME for that one command only, so it sets nothing for the lines after it."""
    assigned: set[str] = {"PWD"}
    unset: list[str] = []
    for line in lines:
        unset += [name for name in owner_commands.REFERENCE.findall(line) if name not in assigned]
        words = shlex.split(owner_commands.SUBSTITUTION.sub("x", line))
        if words and all(re.fullmatch(r"[A-Z][A-Z0-9_]*=.*", word, re.S) for word in words):
            assigned |= {word.split("=", 1)[0] for word in words}
        elif len(words) == 3 and words[:2] == ["read", "-r"] and re.fullmatch(r"[A-Z][A-Z0-9_]*", words[2]):
            assigned.add(words[2])
    return unset


def pasteable_checks(name: str, text: str, folder: Path) -> dict[str, bool]:
    """One script as delivered, before any synthetic substitution: no <NAME> token, a valid syntax for the owner's shells, and every variable a command reads is
    set for good by an earlier command of the script."""
    placeholders = sorted(set(re.findall(r"<[A-Z][A-Z0-9_]*>", text)))
    checks = {f"{name} contains no <NAME> token" + (f": {', '.join(placeholders)}" if placeholders else ""): not placeholders}
    # The script goes to a scratch file and each shell only parses it (-n). The docker-calls guard accepts a shell started on a literal script name.
    script = folder / "block.sh"
    script.write_text(text, encoding="utf-8")
    shells = {"bash": run_command("bash", ["-n", "block.sh"], cwd=folder, timeout=30).returncode == 0}
    if ZSH.exists():
        shells["/bin/zsh"] = run_command("/bin/zsh", ["-n", "block.sh"], cwd=folder, timeout=30).returncode == 0
    else:
        checks["/bin/zsh is not installed on this machine, so bash -n covered the script"] = True
    for label, passed in shells.items():
        checks[f"{name} passes {label} -n as delivered"] = passed
    unset = unset_names(owner_commands.commands(text))
    checks[f"every shell variable of {name} is set for good by an earlier command" + (f": {', '.join(sorted(set(unset)))}" if unset else "")] = not unset
    return checks


def variable_flow_checks() -> dict[str, bool]:
    """The variable rule on planted lines: a temporary assignment sets nothing for later lines, and a command that reads a name before its assignment fails."""
    return {
        "a variable set for one command only is unset for the next line": unset_names(['A="1" printf x', 'printf "$A"']) == ["A"],
        "a variable assigned on its own line is set for the next line": unset_names(['A="1"', 'printf "$A"']) == [],
        "a variable read before its assignment is unset": unset_names(['printf "$A"', 'A="1"']) == ["A"],
        "a name read into by read -r is set for the next line": unset_names(["read -r A", 'printf "$A"']) == [],
    }


WRAPPER = owner_commands.ROOT / "scripts" / "infrastructure" / "terraform.sh"
# The .env spellings the project's dotenv parser accepts for the two settings the first block reads. The other required keys follow in every variant.
DOTENV_VARIANTS = {
    "unquoted": ("AWS_REGION=us-west-2", "AWS_PROFILE=example_project_dev"),
    "double quoted": ('AWS_REGION="us-west-2"', 'AWS_PROFILE="example_project_dev"'),
    "single quoted": ("AWS_REGION='us-west-2'", "AWS_PROFILE='example_project_dev'"),
    "an export prefix, spaces around the equals sign and a comment": (
        "export AWS_REGION = us-west-2  # the region",
        "export AWS_PROFILE = example_project_dev",
    ),
}
DOTENV_REST = (f"AWS_ACCOUNT_ID={owner_commands.ACCOUNT}", "PROJECT_NAME=example_project", "ENVIRONMENT=dev", "S3_BUCKET=example-project-data-dev")


def alert_email_checks() -> dict[str, bool]:
    """The alert email scripts agree with the stacks they act on: the alerts root declares the subscription for the alerts and budget topics and the
    platform stack declares none. The behavior of every script is checked by running it (alert_email_block_cases). The steps come in file name order."""
    root = "".join(path.read_text() for path in (ROOT / "infra/alerts").glob("*.tf"))
    platform = "".join(path.read_text() for path in (ROOT / "infra/platform").glob("*.tf"))
    bootstrap = sorted(name for name in ALERT_FILES if name.startswith(ALERT))
    teardown = sorted(name for name in ALERT_FILES if name.startswith(TEARDOWN))
    numbers = [int(number) for name in bootstrap for number in re.findall(r"step (\d+) of 11", script_text(name))]
    tear_numbers = [int(number) for name in teardown for number in re.findall(r"step (\d+) of 4", script_text(name))]
    calls = [alert_email_block_cases.helper_call(script_text(name)) or "" for name in (*bootstrap, *teardown)]
    order = [call.removeprefix(alert_email_block_cases.HELPER_CALL).strip() for call in calls]
    return {
        "the alerts root declares the subscription for the alerts and budget topics": 'resource "aws_sns_topic_subscription" "email"' in root
        and "alerts = var.alerts_topic_arn" in root
        and "budget = local.budget_topic_arn" in root,
        "the platform stack declares no subscription": "aws_sns_topic_subscription" not in platform,
        f"the bootstrap scripts are the steps 2 to 9 and 11 in the order of their file names ({numbers})": numbers == [*range(2, 10), 11],
        f"the teardown scripts are the steps 1 to 4 in the order of their file names ({tear_numbers})": tear_numbers == [1, 2, 3, 4],
        f"the helper calls come in the order of the procedures ({order})": order
        == [
            'policy-plan --admin-profile "${ADMIN_PROFILE:-}"',
            'policy-apply --admin-profile "${ADMIN_PROFILE:-}"',
            "plan platform",
            "apply platform",
            "topics",
            "prepare",
            "plan alerts",
            "apply alerts",
            "subscriptions --expect confirmed",
            "state",
            "plan alerts --destroy",
            "apply alerts --destroy",
            "subscriptions --expect none",
        ],
        "the administrator profile is named by the SNS policy steps only": [name for name in ALERT_FILES if "ADMIN_PROFILE" in script_text(name)]
        == ["alert_email_bootstrap_02_policy_plan.sh", "alert_email_bootstrap_03_policy_apply.sh"],
    }


def printed_resolve_hint(folder: Path) -> str:
    """What the real wrapper prints after a failed services apply: its ``resolve_hint`` function, taken from terraform.sh and run with a synthetic token."""
    found = re.search(r"^  resolve_hint\(\) \{\n.*?^  \}\n", WRAPPER.read_text(encoding="utf-8"), re.S | re.M)
    work = folder / "resolve_hint"
    work.mkdir(parents=True, exist_ok=True)
    if found is None:
        return ""
    script = f"python_bin=.venv/bin/python\ntoken='example-host:4242:20261112T000000Z:0123abcd'\n{found.group(0)}resolve_hint\n"
    (work / "block.sh").write_text(script, encoding="utf-8")
    return run_command("bash", ["block.sh"], cwd=work, timeout=30).stderr


def recovery_checks(code: str, resolve: str, folder: Path) -> dict[str, bool]:
    """The recovery path of a failed services apply: the owner supplies the statement and the lock token, no printed or written placeholder is needed."""
    lines = [line.strip() for line in owner_commands.commands(resolve)]
    reads = {match.group(1): index for index, line in enumerate(lines) if (match := re.fullmatch(r"read -r ([A-Z][A-Z0-9_]*)", line))}
    resolves = [(index, line) for index, line in enumerate(lines) if "services_apply resolve" in line]
    passed = re.search(r'--statement "\$([A-Z][A-Z0-9_]*)" --token "\$([A-Z][A-Z0-9_]*)"$', resolves[0][1]) if len(resolves) == 1 else None
    placeholders = sorted(set(re.findall(r"<[A-Za-z][A-Za-z0-9_ ]*>", code + resolve)))
    printed = printed_resolve_hint(folder)
    return {
        "the recovery script prompts for the statement and the lock token and passes both to resolve as quoted variables": bool(passed)
        and passed is not None
        and passed.group(1) != passed.group(2)
        and all(name in reads and reads[name] < resolves[0][0] for name in passed.groups()),
        "the recovery script reads the status of the apply lock before it asks for the token": bool(lines)
        and lines[0] == ".venv/bin/python -m scripts.infrastructure.services_apply status",
        "the code checkout names the recovery script and does not send the owner to the command that the failure prints": RESOLVE in code
        and "exactly as the failure printed" not in code
        and "as the failure prints" not in code,
        "no angle-bracket placeholder, lower case included, remains in the code scripts"
        + (f": {', '.join(placeholders)}" if placeholders else ""): not placeholders,
        "the wrapper's failure output still prints a placeholder statement, the hazard that the recovery script avoids": re.search(
            r"--statement '<[^>]+>'", printed
        )
        is not None,
    }


def dotenv_checks(body: str, folder: Path) -> dict[str, bool]:
    """The first block reads the profile and the region through the project's dotenv parser: its two assignments run in bash and zsh on each .env spelling."""
    lines = [line.strip() for line in owner_commands.commands(body)]
    assignments = [line for line in lines if line.startswith(("PROJECT_PROFILE=", "REGION="))]
    checks = {
        "the code checkout reads the project profile and the region through the project's dotenv parser, not by text extraction": len(assignments) == 2
        and all("scripts.infrastructure.project_setting" in line and "sed" not in line for line in assignments)
    }
    for number, (label, (region, profile)) in enumerate(DOTENV_VARIANTS.items()):
        work = folder / f"dotenv_{number}"
        (work / ".venv" / "bin").mkdir(parents=True, exist_ok=True)
        python = work / ".venv" / "bin" / "python"
        python.write_text(f'#!/bin/sh\nexec "{sys.executable}" "$@"\n', encoding="utf-8")
        python.chmod(0o700)
        (work / ".env").write_text("\n".join([region, profile, *DOTENV_REST]) + "\n", encoding="utf-8")
        (work / "block.sh").write_text("\n".join([*assignments, 'printf \'%s|%s\\n\' "$PROJECT_PROFILE" "$REGION"']) + "\n", encoding="utf-8")
        environment = owner_commands.environment(work) | {"PYTHONPATH": str(owner_commands.ROOT)}
        shells = {"bash": run_command("bash", ["block.sh"], cwd=work, env=environment, timeout=60).stdout.strip()}
        if ZSH.exists():
            shells["/bin/zsh"] = run_command("/bin/zsh", ["block.sh"], cwd=work, env=environment, timeout=60).stdout.strip()
        for shell, printed in shells.items():
            checks[f"the code checkout's profile and region assignments give example_project_dev and us-west-2 under {shell} for .env with {label} values"] = (
                printed == "example_project_dev|us-west-2"
            )
    return checks


def command_checks(code_body: str, resolve_body: str, folder: Path) -> dict[str, bool]:
    workspace = Workspace(folder / "workspace")
    code_sample = services_code_sample()
    wrapper = make_wrapper(workspace, {"services_code.json": code_sample})
    code_lines = [line.strip() for line in owner_commands.commands(code_body)]
    tool_lines = [line for line in code_lines if "scripts.infrastructure.services_code_inputs" in line]
    shasums = [
        index
        for index, line in enumerate(code_lines)
        if line.startswith("SERVICES_INPUTS_SHA256=") and 'shasum -a 256 "$RECORD_DIR/services_code.json"' in line
    ]
    plans = [
        index
        for index, line in enumerate(code_lines)
        if "terraform.sh --services" in line and " plan " in line and '--preparation-sha256 "$SERVICES_INPUTS_SHA256"' in line
    ]
    applies = [
        index
        for index, line in enumerate(code_lines)
        if "terraform.sh --services" in line and " apply " in line and '--preparation-sha256 "$SERVICES_INPUTS_SHA256"' in line
    ]
    checks: dict[str, bool] = {
        "the code checkout writes the services file with services_code_inputs from the existing rendered settings, not as hand-written JSON": len(tool_lines)
        == 1
        and "--existing infra/services/deployment.auto.tfvars.json" in tool_lines[0]
        and '--role-file "$RECORD_DIR/host_code_fetch_role.json"' in tool_lines[0]
        and '--bucket-file "$RECORD_DIR/data_bucket.txt"' in tool_lines[0]
        and '--output "$RECORD_DIR/services_code.json"' in tool_lines[0],
        "the code checkout saves the code one-shot role for the tool": any(
            line.startswith("terraform -chdir=infra/iam output -json host_code_fetch_role") and line.endswith('> "$RECORD_DIR/host_code_fetch_role.json"')
            for line in code_lines
        ),
        "the code checkout saves the bucket name for the tool": any(
            line.endswith('> "$RECORD_DIR/data_bucket.txt"') and '"$DATA_BUCKET"' in line for line in code_lines
        ),
        "the code checkout writes the services file, binds its complete bytes by digest in a variable and uses that variable in the plan and the apply": bool(
            tool_lines and shasums and plans and applies
        )
        and code_lines.index(tool_lines[0]) < min(shasums) < min(plans) < min(applies),
        "the code checkout keeps the check that the plan changes at most the one-shot task definition": "must change at most the code one-shot task definition"
        in code_body
        and "a setting was lost" in code_body,
        "the file the tool writes for an installation that runs MLflow keeps that setting and sets the code one-shot role and the bucket": code_sample.get(
            "variables", {}
        ).get("mlflow_enabled")
        is True
        and sorted(code_sample.get("variables", {}))
        == sorted(["code_bucket_name", "code_fetch_task_role_arn", "mlflow_artifact_bucket", "mlflow_enabled", "mlflow_task_role_arn"]),
    }
    policy_apply, services_plan = code_body.find("manage_policies apply --admin-profile"), code_body.find("terraform.sh --services")
    checks["the code checkout applies the project user's policy with the policy manager under the administrator profile before the services apply"] = (
        0 <= policy_apply < services_plan
        and "CONFIRM_IAM_POLICIES=apply-healthcare-hai-prediction-policies" in code_body
        and "host_code_fetch_task" in code_body
    )
    # The services stack is applied only while the host runs: with no instance the ECS deployment circuit breaker rolls every service back
    # (project guide, Services apply). So in the code checkout every services apply comes after the host start, and the code is staged before it.
    commands = [(index, line.strip()) for index, line in enumerate(code_body.splitlines())]
    services_applies = [index for index, line in commands if line.startswith("bash scripts/infrastructure/terraform.sh --services") and " apply " in line]
    host_starts = [index for index, line in commands if line.startswith(".venv/bin/python -m scripts.infrastructure.host_session start")]
    stagings = [index for index, line in commands if line.startswith(".venv/bin/python -m scripts.lakehouse.code_archive build")]
    checks["the code checkout starts the host before every services apply"] = bool(services_applies and host_starts) and min(host_starts) < min(
        services_applies
    )
    checks["the code checkout stages the code before it starts the host"] = bool(stagings and host_starts) and max(stagings) < min(host_starts)
    checks["the recovery script resolves a failed services apply with the apply lock token"] = (
        "services_apply resolve" in resolve_body and "--token" in resolve_body
    )
    checks.update(pasteable_checks(CODE, code_body, folder))
    checks.update(pasteable_checks(RESOLVE, resolve_body, folder))
    checks.update(recovery_checks(code_body, resolve_body, folder))
    checks.update(dotenv_checks(code_body, folder))
    problems = owner_commands.check_shell_section(code_body, folder / "code", wrapper, {"ADMIN_PROFILE": ADMIN})
    checks[f"{CODE}: {len(problems)} printed commands were found"] = len(problems) >= 5
    for number, (line, found) in enumerate(problems.items(), 1):
        checks[f"{CODE}: command {number} parses: {line[:110]}" + (f" -> {found[0][:150]}" if found else "")] = not found
    for prefix in (ALERT, TEARDOWN):
        checks |= alert_block_checks(prefix, alert_email_block_cases.fences(prefix), folder / prefix, wrapper)
    return checks


ALERT_BLOCKS = {ALERT: 9, TEARDOWN: 4}


def alert_block_checks(prefix: str, blocks: list[str], folder: Path, wrapper: Any) -> dict[str, bool]:
    """Every script of an alert procedure is unwrapped by the frame that ``block_problems`` pins, and its helper call goes through the real parser. A
    script that does not unwrap fails its own check and is never skipped."""
    checks = {f"{prefix}: {len(blocks)} scripts were found, {ALERT_BLOCKS[prefix]} expected": len(blocks) == ALERT_BLOCKS[prefix]}
    folder.mkdir(parents=True, exist_ok=True)
    for number, block in enumerate(blocks, 1):
        call = alert_email_block_cases.helper_call(block)
        if call is None:
            checks[f"{prefix}: script {number} unwraps: {alert_email_block_cases.block_problems(block)}"] = False
            continue
        found = owner_commands.check_line(call, folder, wrapper)
        checks[f"{prefix}: script {number} parses: {call[:110]}" + (f" -> {found[0][:150]}" if found else "")] = not found
    return checks


ALERT_MUTANTS = (
    ("a mistyped strict option in the frame", "set -euo pipefail", "set -euo pipefial"),
    ("a mistyped step of the helper call", "alert_email topics", "alert_email topicz"),
    ("a second command after a redirect and a semicolon", "alert_email topics", "alert_email topics > /dev/stdout; printf x > marker"),
    ("a second command after a redirect and &&", "alert_email topics", "alert_email topics > /dev/stdout && printf x > marker"),
    ("a second command after a redirect and a pipe", "alert_email topics", "alert_email topics > /dev/stdout | cat"),
    ("a command substitution in a redirect", "alert_email topics", "alert_email topics > $(printf x)"),
    ("a backtick substitution in a redirect", "alert_email topics", "alert_email topics > `printf x`"),
)


def alert_mutant_checks(folder: Path) -> dict[str, bool]:
    """Each planted typo in one alert script must fail exactly the check of that script, and the unplanted scripts must fail none."""
    wrapper = make_wrapper(Workspace(folder / "alert_mutant_workspace"), {})
    blocks = alert_email_block_cases.fences(ALERT)
    clean = [name for name, ok in alert_block_checks(ALERT, blocks, folder / "alert_mutant_clean", wrapper).items() if not ok]
    checks = {"the unplanted alert scripts fail no check": not clean}
    for label, old, new in ALERT_MUTANTS:
        number = next((index for index, block in enumerate(blocks, 1) if old in block), 0)
        mutated = [block.replace(old, new, 1) if index == number else block for index, block in enumerate(blocks, 1)]
        failed = [name for name, ok in alert_block_checks(ALERT, mutated, folder / "alert_mutant", wrapper).items() if not ok]
        checks[f"alert mutant {label} fails only script {number}"] = number > 0 and len(failed) == 1 and failed[0].startswith(f"{ALERT}: script {number} ")
    return checks


MUTANTS = (
    ("terraform output with two names", "output -json host_code_fetch_role", "output -json host_code_fetch_role project_tag"),
    ("manage_policies without the administrator profile", 'manage_policies plan --admin-profile "$ADMIN_PROFILE"', "manage_policies plan"),
    ("manage_policies apply without the reviewed plan's digest", ' --digest "$PLAN_DIGEST"', ""),
    ("push_images without the repository file", '--repository-urls "$RECORD_DIR/repository_urls.json" ', ""),
    ("a mistyped module", "scripts.lakehouse.code_archive build", "scripts.lakehouse.code_arch build"),
    ("a mistyped code archive option", '--output "$CODE_DIR"', '--outputs "$CODE_DIR"'),
    ("a mistyped wrapper option", '--preparation-sha256 "$SERVICES_INPUTS_SHA256" plan', '--preparation-sha25 "$SERVICES_INPUTS_SHA256" plan'),
    ("a mistyped host start option", 'host_session start --code-run "$RUN_ID"', 'host_session start --code-runs "$RUN_ID"'),
    ("a services apply status without its subcommand", "services_apply status", "services_apply"),
)


def mutant_checks(code_body: str, resolve_body: str, folder: Path) -> dict[str, bool]:
    """Each planted typo in one printed command must fail the parser check of that line."""
    lines = owner_commands.commands(code_body) + owner_commands.commands(resolve_body)
    wrapper = make_wrapper(Workspace(folder / "mutant_workspace"), {})
    checks: dict[str, bool] = {}
    for label, old, new in MUTANTS:
        line = next((item for item in lines if old in item), None)
        if line is None:
            checks[f"mutant {label}: the command exists in the scripts"] = False
            continue
        (folder / "mutant").mkdir(parents=True, exist_ok=True)
        try:
            found = owner_commands.check_line(line.replace(old, new, 1), folder / "mutant", wrapper)
        except ValueError as error:
            found = [str(error)]
        checks[f"mutant {label} is refused"] = bool(found)
    return checks


def alert_call_checks(calls: list[str], folder: Path) -> dict[str, bool]:
    """Every distinct command that the alert blocks ran against the stand-ins goes through the real parser of its program, as the printed lines did before."""
    wrapper = make_wrapper(Workspace(folder / "alert_workspace"), {})
    (folder / "alert_record").mkdir(parents=True, exist_ok=True)
    lines = sorted({line for line in calls})
    checks: dict[str, bool] = {"the alert blocks ran commands against the stand-ins": len(lines) >= 20}
    for line in lines:
        tool, _, rest = line.partition(" ")
        printed = {"wrapper": "bash scripts/infrastructure/terraform.sh", "python": ".venv/bin/python"}.get(tool, tool) + " " + rest
        problems = owner_commands.check_line(printed, folder, wrapper)
        label = re.sub(r"/\S+\.tfplan", "<saved plan>", printed)
        checks[f"the real parser accepts the alert block command: {label[:110]}"] = not problems
    return checks


SEQUENCE_SHIMS = {
    "bin/terraform": """#!/bin/sh
echo "terraform $*" >> "$SEQUENCE_LOG"
case "$*" in
  *"output -raw bucket_name"*) echo example-project-data-dev ;;
  *"output -json host_code_fetch_role"*) echo '{"arn": "role"}' ;;
  *"output -json ecr_repository_urls"*) echo '{"training": "111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_training_dev"}' ;;
esac
""",
    "bin/aws": """#!/bin/sh
echo "aws $*" >> "$SEQUENCE_LOG"
""",
    "scripts/infrastructure/terraform.sh": """#!/bin/sh
echo "wrapper $*" >> "$SEQUENCE_LOG"
while [ $# -gt 0 ]; do
  if [ "$1" = -out ]; then : > "$2"; fi
  shift
done
""",
    ".venv/bin/python": """#!/bin/sh
echo "python $*" >> "$SEQUENCE_LOG"
case "$2" in
  scripts.infrastructure.project_setting)
    if [ "$3" = aws_profile ]; then echo example_project_dev; else echo us-west-2; fi ;;
  infra.iam.scripts.manage_policies)
    if [ "$3" = plan ]; then echo "Plan digest: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"; fi ;;
  scripts.infrastructure.push_images)
    if [ "$3" = require ] && [ ! -f "$SEQUENCE_STATE/training_recorded" ]; then
      echo "docker push example_project_training"
      echo "Record the digest, then run the script again." >&2
      exit 1
    fi ;;
  scripts.infrastructure.image_digest_check)
    case "$(cat "$SEQUENCE_STATE/ecr" 2>/dev/null)" in
      missing)
        echo "docker push example_project_training"
        echo "ECR does not hold the recorded digest of training. Run the commands above, record the new digests, then run the script again." >&2
        exit 1 ;;
      failed)
        echo "ECR could not be checked. Nothing was changed." >&2
        exit 2 ;;
      timeout)
        echo "ECR did not answer within 30 s. Nothing was changed." >&2
        exit 2 ;;
    esac ;;
  scripts.lakehouse.code_archive)
    mkdir -p "$5"
    echo archive > "$5/code.tar.gz"
    echo 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef > "$5/code.sha256" ;;
  scripts.infrastructure.services_code_inputs)
    echo "{}" > "${10}" ;;
esac
""",
}
SEQUENCE_ANSWERS = "example_admin\napply\napply\napply\n"


def sequence_run(root: Path, state: Path, log: Path) -> tuple[int, str, list[str]]:
    """Run the whole code checkout script in the scratch root against the stand-ins, answering its prompts, and return the exit code, its text and the calls."""
    environment = {
        "PATH": f"{root / 'bin'}:/usr/bin:/bin",
        "HOME": str(root),
        "SEQUENCE_LOG": str(log),
        "SEQUENCE_STATE": str(state),
    }
    log.write_text("", encoding="utf-8")
    result = run_command(
        "bash",
        ["-c", "bash scripts/infrastructure/owner/code_checkout.sh < answers.txt"],
        cwd=root,
        env=environment,
        timeout=120,
        own_session=True,
    )
    return result.returncode, result.stdout + result.stderr, log.read_text(encoding="utf-8").splitlines()


GENERATED_FOLDER = re.compile(r"code_archive build --output (/tmp/code_\d{8}T\d{6}Z-[0-9a-f]{8})$")


def sequence_checks(folder: Path) -> dict[str, bool]:
    """The code checkout as a whole, against stand-ins: it stops before the code is staged or the host started while the training image has no recorded
    digest, while ECR lacks the recorded digest and while ECR cannot be asked (an error or a time limit). Once ECR holds it the rerun goes on through the
    code stage, the host start and the services apply in order. The code folders that the runs generate are removed at the end."""
    root = folder / "sequence"
    for name, body in SEQUENCE_SHIMS.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
        path.chmod(0o755)
    script = root / "scripts/infrastructure/owner/code_checkout.sh"
    script.parent.mkdir(parents=True, exist_ok=True)
    script.write_text(script_text(CODE), encoding="utf-8")
    script.chmod(0o755)
    (root / "answers.txt").write_text(SEQUENCE_ANSWERS, encoding="utf-8")
    state = root / "state"
    state.mkdir()
    log = root / "calls.log"
    runs: dict[str, tuple[int, str, list[str]]] = {}
    generated: list[str] = []
    try:
        runs["unrecorded"] = sequence_run(root, state, log)
        (state / "training_recorded").write_text("recorded\n", encoding="utf-8")
        for mode in ("missing", "failed", "timeout"):
            (state / "ecr").write_text(mode + "\n", encoding="utf-8")
            runs[mode] = sequence_run(root, state, log)
        (state / "ecr").write_text("present\n", encoding="utf-8")
        runs["present"] = sequence_run(root, state, log)
    finally:
        generated = [match[1] for _, _, calls in runs.values() for call in calls if (match := GENERATED_FOLDER.search(call))]
        for leftover in generated:
            shutil.rmtree(leftover, ignore_errors=True)

    def position(calls: list[str], *needles: str) -> int:
        return next((index for index, call in enumerate(calls) if all(needle in call for needle in needles)), -1)

    checks: dict[str, bool] = {}
    messages = {
        "unrecorded": "run the script again",
        "missing": "ECR does not hold the recorded digest",
        "failed": "ECR could not be checked",
        "timeout": "ECR did not answer",
    }
    for mode, message in messages.items():
        code, text, calls = runs[mode]
        checks[f"the {mode} run stops with a failing exit status and says why ({message})"] = code != 0 and message in text
        checks[f"the {mode} run asks the image tool for the training digest"] = position(calls, "push_images", "require", "--only training") >= 0
        checks[f"the {mode} run stages no code, starts no host and applies no services stack"] = all(
            position(calls, word) < 0 for word in ("code_archive", "aws s3 cp", "host_session", "wrapper --services")
        )
        if mode != "unrecorded":
            checks[f"the {mode} run asks ECR for the training digest before it stops"] = (
                position(calls, "image_digest_check", "--only training", "--profile") >= 0
            )
    checks["the missing run prints the image commands"] = "docker push" in runs["missing"][1]
    code, text, calls = runs["present"]
    order = [
        position(calls, "wrapper --iam", "apply"),
        position(calls, "manage_policies", "apply"),
        position(calls, "push_images", "require"),
        position(calls, "image_digest_check"),
        position(calls, "code_archive", "build"),
        position(calls, "aws s3 cp", "code.tar.gz"),
        position(calls, "host_session", "start"),
        position(calls, "wrapper --services", "plan"),
        position(calls, "wrapper --services", "apply"),
    ]
    checks[f"the rerun ends with exit status 0 ({code}): {text[-200:]}"] = code == 0
    checks[f"the rerun goes through the IAM apply, the digest checks, the code stage, the host start and the services plan and apply in order ({order})"] = all(
        index >= 0 for index in order
    ) and order == sorted(order)
    checks[f"the runs generated code folders and none is left ({len(generated)} generated)"] = bool(generated) and not any(
        Path(leftover).exists() for leftover in generated
    )
    return checks


def step_mutant_checks(code_body: str, folder: Path) -> dict[str, bool]:
    """A script without one of its steps, or one that reads a variable no earlier command set, fails its own check instead of passing with fewer steps."""
    checks: dict[str, bool] = {}
    without_step = re.sub(r"(?ms)^# Step 3:.*?(?=^# Step 4:)", "", code_body)
    checks["a code checkout without its step 3 fails the structure check"] = step_numbers(without_step) != list(CODE_STEPS)
    without_run_id = code_body.replace('RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$(openssl rand -hex 4)"\n', "")
    (folder / "step_mutant").mkdir(parents=True, exist_ok=True)
    failed = [name for name, ok in pasteable_checks(CODE, without_run_id, folder / "step_mutant").items() if not ok]
    checks["a code checkout that no longer sets RUN_ID fails the variable check"] = any("RUN_ID" in name for name in failed)
    return checks


STATE_ORDER = (
    "state_migration check",
    "state_migration plan --phase before",
    'confirm "',
    "state_migration migrate",
    "state_migration plan --phase after",
    "state_migration verify",
)


def state_order(body: str) -> bool:
    """The state move runs its steps in the safe order: check (read only), plan before, typed confirmation, move, plan after, verification."""
    positions = [body.find(marker) for marker in STATE_ORDER]
    return all(position >= 0 for position in positions) and positions == sorted(positions)


def prefix_shape_checked(body: str) -> bool:
    """The prefix sheet reads the saved plan's JSON before its typed confirmation and names exactly the two approved updates in place."""
    return (
        0 <= body.find("state_migration approve-plan --stack storage --plan") < body.find('confirm "')
        and body.count("--update ") == 2
        and "--update aws_s3_bucket_lifecycle_configuration.data" in body
        and "--update aws_s3_bucket_policy.tls" in body
    )


def state_sheet_checks(folder: Path) -> dict[str, bool]:
    """The three sheets of the Terraform state move: each parses as delivered, every command goes through its real parser, the steps come in the safe
    order, the project stacks never name the administrator profile, the IAM stack always does, and no sheet applies or moves anything before its
    typed confirmation."""
    workspace = Workspace(folder / "state_workspace")
    wrapper = make_wrapper(workspace, {})
    bodies = {name: script_text(name) for name in STATE_FILES}
    overrides = {
        STATE_PREFIX: {"STATE_ADMIN": "role/example_admin", "STORAGE_PLAN": "/work/storage.tfplan"},
        STATE_PROJECT: {"STACK": "platform"},
        STATE_IAM: {"ADMIN_PROFILE": ADMIN},
    }
    checks: dict[str, bool] = {}
    for name, body in bodies.items():
        (folder / f"state_{name}").mkdir(parents=True, exist_ok=True)
        checks.update(pasteable_checks(name, body, folder / f"state_{name}"))
        checks[f"{name}: its steps are numbered from 0 in order"] = step_numbers(body) == list(range(len(step_numbers(body)))) and len(step_numbers(body)) >= 4
        problems = owner_commands.check_shell_section(body, folder / f"state_{name}" / "run", wrapper, overrides[name])
        checks[f"{name}: {len(problems)} printed commands were found"] = len(problems) >= 4
        for number, (line, found) in enumerate(problems.items(), 1):
            checks[f"{name}: command {number} parses: {line[:110]}" + (f" -> {found[0][:150]}" if found else "")] = not found
    prefix, project, iam = bodies[STATE_PREFIX], bodies[STATE_PROJECT], bodies[STATE_IAM]
    checks["the prefix sheet applies the saved plan only after its typed confirmation and checks the live bucket last"] = (
        prefix.find('confirm "') < prefix.find('terraform.sh --state-admin "$STATE_ADMIN" apply') < prefix.find("state_migration check --stack storage")
    )
    checks["the prefix sheet plans and applies with the same --state-admin option, so the wrapper compares the plan with it"] = (
        prefix.count('terraform.sh --state-admin "$STATE_ADMIN"') == 2
    )
    checks["the prefix sheet reads the saved plan's shape before its typed confirmation and names exactly the two approved updates"] = prefix_shape_checked(
        prefix
    )
    checks["the prefix sheet plans without color codes, so its change lines are readable"] = "plan -no-color -out" in prefix
    checks["the project sheet runs the steps in the safe order"] = state_order(project)
    checks["the iam sheet runs the steps in the safe order"] = state_order(iam)
    checks["the project sheet never names the administrator profile in a command"] = "--admin-profile" not in project
    checks["the iam sheet gives the administrator profile to every command"] = all(
        "--admin-profile" in line for line in iam.splitlines() if line.startswith(".venv/bin/python -m scripts.infrastructure.state_migration")
    )
    checks["the project sheet moves nothing before its confirmation and refuses a second typed word"] = (
        project.find('confirm "') < project.find("state_migration migrate") and '[ "$CONFIRMATION" = migrate ]' in project
    )
    checks["all three sheets point to the rollback note"] = all("docs/terraform_state_migration.md" in body for body in (project, iam, prefix))
    mutants = {
        "a move sheet that migrates before its plan": project.replace("state_migration plan --phase before", "state_migration plan --phase after", 1),
        "a move sheet without its verification": project.replace("state_migration verify", "state_migration check", 1),
    }
    for label, body in mutants.items():
        checks[f"{label} fails the order check"] = not state_order(body)
    checks["a prefix sheet that never reads the saved plan's shape fails the shape check"] = not prefix_shape_checked(
        prefix.replace("state_migration approve-plan", "state_migration check", 1)
    )
    mistyped = project.replace("state_migration migrate", "state_migration migrat")
    planted = owner_commands.check_shell_section(mistyped, folder / "state_mutant", wrapper, overrides[STATE_PROJECT])
    checks["a mistyped command of the move sheet fails its parser"] = any(planted.values())
    return checks


def run(output: Path) -> dict[str, dict[str, bool]]:
    code, resolve = script_text(CODE), script_text(RESOLVE)
    block_checks, block_calls = alert_email_block_cases.cases()
    with tempfile.TemporaryDirectory(prefix="owner_scripts_") as folder:
        results = {
            "structure": structure_checks(code) | variable_flow_checks() | step_mutant_checks(code, Path(folder)),
            "sequence": sequence_checks(Path(folder)),
            "alert_email": alert_email_checks(),
            "alert_blocks": block_checks | alert_call_checks(block_calls, Path(folder)) | alert_email_block_cases.mutant_checks(),
            "commands": command_checks(code, resolve, Path(folder)),
            "mutants": mutant_checks(code, resolve, Path(folder)) | alert_mutant_checks(Path(folder)),
            "state_move": state_sheet_checks(Path(folder)),
        }
    output.mkdir(parents=True)
    (output / "report.json").write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--out", type=Path, required=True, help="a new folder for the report")
    arguments = parser.parse_args()
    results = run(arguments.out)
    failed = [f"{group}: {name}" for group, checks in results.items() for name, ok in checks.items() if not ok]
    total = sum(len(checks) for checks in results.values())
    for line in failed:
        sys.stdout.write(f"FAIL {line}\n")
    sys.stdout.write(f"owner scripts E2E: {total - len(failed)} of {total} checks passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
