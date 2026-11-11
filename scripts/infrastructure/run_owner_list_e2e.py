"""E2E of the owner's lists in config/aws/owner_command_lists.json: the order of the steps, the flags each step names and every printed command against its
parser.

    .venv/bin/python -m scripts.infrastructure.run_owner_list_e2e --out <new folder>

The commands of the list "Code checkout on the host" (the text moved out of the tracked documents) run through the parser of their real programs with
synthetic values (``scripts.infrastructure.owner_commands``), so a typo, a missing argument or a wrong option fails here and not on the owner's first run.
The reviewed services file that the list describes passes the real renderer through the real wrapper. The cases of the list "Silver and gold through Airflow
on the host" are in ``scripts.infrastructure.owner_list_host_cases``. Nothing calls AWS or Docker."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shlex
import sys
import tempfile
from pathlib import Path
from typing import Any

from scripts.infrastructure import owner_commands, owner_list_host_cases, services_code_inputs
from scripts.infrastructure.run_activation_inputs_e2e import ADMIN, ROOT, ROWS, STACKS, Workspace, variables_of
from scripts.process import run_command

OWNER_LISTS = ROOT / "config/aws/owner_command_lists.json"
CODE = "Code checkout on the host"
IAM_FLAGS = {"host_code_fetch_prepared": True}
STACK_OPTIONS = ("--iam", "--platform", "--services", "--jobs")


def steps_of(body: str) -> list[str]:
    return [step for step in re.split(r"(?m)^(?=\d+\. )", body) if re.match(r"\d+\. ", step)]


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


def structure_checks(text: str) -> dict[str, bool]:
    body = owner_commands.section(text, CODE)
    steps = steps_of(body)
    numbers = [int(step.split(".", 1)[0]) for step in steps]
    variables = {name for names in (variables_of(directory) for directory in STACKS.values()) for name in names}
    named = {name for name in re.findall(r"`([a-z][a-z0-9_]+)`", body) if name in variables}
    rows = {row.name for row in ROWS}
    return {
        "the code section is one numbered list": bool(body) and numbers == list(range(1, len(numbers) + 1)) and len(numbers) >= 6,
        f"every input the code list names has a row ({sorted(named - rows)})": named <= rows,
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


def pasteable_checks(body: str, folder: Path) -> dict[str, bool]:
    """The shell blocks of a section as delivered, before any synthetic substitution: no <NAME> token, a valid syntax for the owner's shells, and every
    variable a command reads is set by an earlier command of the section."""
    fences = re.findall(r"```sh\n(.*?)```", body, re.S)
    placeholders = sorted(set(re.findall(r"<[A-Z][A-Z0-9_]*>", "\n".join(fences))))
    checks = {
        "the code list has shell blocks": bool(fences),
        "the code list's shell blocks contain no <NAME> token" + (f": {', '.join(placeholders)}" if placeholders else ""): not placeholders,
    }
    # A block goes to a script file and each shell only parses it (-n). The docker-calls guard accepts a shell started on a literal script name.
    script = folder / "block.sh"
    broken: dict[str, list[int]] = {"bash": [], "/bin/zsh": []}
    for number, fence in enumerate(fences, 1):
        script.write_text(fence, encoding="utf-8")
        if run_command("bash", ["-n", "block.sh"], cwd=folder, timeout=30).returncode != 0:
            broken["bash"].append(number)
        if ZSH.exists() and run_command("/bin/zsh", ["-n", "block.sh"], cwd=folder, timeout=30).returncode != 0:
            broken["/bin/zsh"].append(number)
    if not ZSH.exists():
        del broken["/bin/zsh"]
        checks["/bin/zsh is not installed on this machine, so bash -n covered the blocks"] = True
    for label, blocks in broken.items():
        checks[f"every shell block of the code list passes {label} -n as delivered" + (f": blocks {blocks}" if blocks else "")] = not blocks
    assigned: set[str] = {"PWD"}
    unset: list[str] = []
    for line in owner_commands.commands(body):
        stripped = owner_commands.SUBSTITUTION.sub("", line)
        for name in owner_commands.REFERENCE.findall(line):
            if name not in assigned:
                unset.append(name)
        found = re.match(r"([A-Z][A-Z0-9_]*)=", stripped)
        if found:
            assigned.add(found.group(1))
        read = re.match(r"read -r ([A-Z][A-Z0-9_]*)$", stripped)
        if read:
            assigned.add(read.group(1))
    checks[
        "every shell variable of the code list is set by an earlier command of the list" + (f": {', '.join(sorted(set(unset)))}" if unset else "")
    ] = not unset
    return checks


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


def recovery_checks(body: str, folder: Path) -> dict[str, bool]:
    """The recovery path of a failed services apply: the owner supplies the statement and the lock token, no printed or written placeholder is needed."""
    lines = [line.strip() for line in owner_commands.commands(body)]
    reads = {match.group(1): index for index, line in enumerate(lines) if (match := re.fullmatch(r"read -r ([A-Z][A-Z0-9_]*)", line))}
    resolves = [(index, line) for index, line in enumerate(lines) if "services_apply resolve" in line]
    passed = re.search(r'--statement "\$([A-Z][A-Z0-9_]*)" --token "\$([A-Z][A-Z0-9_]*)"$', resolves[0][1]) if len(resolves) == 1 else None
    placeholders = sorted(set(re.findall(r"<[A-Za-z][A-Za-z0-9_ ]*>", body)))
    printed = printed_resolve_hint(folder)
    return {
        "the code list's recovery block prompts for the statement and the lock token and passes both to resolve as quoted variables": bool(passed)
        and passed is not None
        and passed.group(1) != passed.group(2)
        and all(name in reads and reads[name] < resolves[0][0] for name in passed.groups()),
        "the code list does not send the owner to the command that the failure prints": "exactly as the failure printed" not in body
        and "as the failure prints" not in body,
        "no angle-bracket placeholder, lower case included, remains in the code list"
        + (f": {', '.join(placeholders)}" if placeholders else ""): not placeholders,
        "the wrapper's failure output still prints a placeholder statement, the hazard that the recovery block avoids": re.search(
            r"--statement '<[^>]+>'", printed
        )
        is not None,
    }


def dotenv_checks(body: str, folder: Path) -> dict[str, bool]:
    """The first block reads the profile and the region through the project's dotenv parser: its two assignments run in bash and zsh on each .env spelling."""
    lines = [line.strip() for line in owner_commands.commands(body)]
    assignments = [line for line in lines if line.startswith(("PROJECT_PROFILE=", "REGION="))]
    checks = {
        "the code list reads the project profile and the region through the project's dotenv parser, not by text extraction": len(assignments) == 2
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
            checks[f"the code list's profile and region assignments give example_project_dev and us-west-2 under {shell} for .env with {label} values"] = (
                printed == "example_project_dev|us-west-2"
            )
    return checks


def command_checks(text: str, folder: Path) -> dict[str, bool]:
    workspace = Workspace(folder / "workspace")
    code_body = owner_commands.section(text, CODE)
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
        "the code list writes the services file with services_code_inputs from the existing rendered settings, not as hand-written JSON": len(tool_lines) == 1
        and "--existing infra/services/deployment.auto.tfvars.json" in tool_lines[0]
        and '--role-file "$RECORD_DIR/host_code_fetch_role.json"' in tool_lines[0]
        and '--bucket-file "$RECORD_DIR/data_bucket.txt"' in tool_lines[0]
        and '--output "$RECORD_DIR/services_code.json"' in tool_lines[0]
        and owner_commands.example_json(code_body) is None,
        "the code list saves the code one-shot role for the tool": any(
            line.startswith("terraform -chdir=infra/iam output -json host_code_fetch_role") and line.endswith('> "$RECORD_DIR/host_code_fetch_role.json"')
            for line in code_lines
        ),
        "the code list saves the bucket name for the tool": any(
            line.endswith('> "$RECORD_DIR/data_bucket.txt"') and '"$DATA_BUCKET"' in line for line in code_lines
        ),
        "the code list writes the services file, binds its complete bytes by digest in a variable and uses that variable in the plan and the apply": bool(
            tool_lines and shasums and plans and applies
        )
        and code_lines.index(tool_lines[0]) < min(shasums) < min(plans) < min(applies),
        "the code list keeps the check that the plan changes at most the code one-shot task definition": "must change at most the code one-shot task definition"
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
    checks["the code list applies the project user's policy with the policy manager under the administrator profile before the services apply"] = (
        0 <= policy_apply < services_plan
        and "CONFIRM_IAM_POLICIES=apply-healthcare-hai-prediction-policies" in code_body
        and "host_code_fetch_task" in code_body
    )
    # The services stack is applied only while the host runs: with no instance the ECS deployment circuit breaker rolls every service back
    # (project guide, Services apply). So in the code list every services apply comes after the host start, and the code is staged before it.
    commands = [(index, line.strip()) for index, line in enumerate(code_body.splitlines())]
    services_applies = [index for index, line in commands if line.startswith("bash scripts/infrastructure/terraform.sh --services") and " apply " in line]
    host_starts = [index for index, line in commands if line.startswith(".venv/bin/python -m scripts.infrastructure.host_session start")]
    stagings = [index for index, line in commands if line.startswith(".venv/bin/python -m scripts.lakehouse.code_archive build")]
    checks["the code list starts the host before every services apply"] = bool(services_applies and host_starts) and min(host_starts) < min(services_applies)
    checks["the code list stages the code before it starts the host"] = bool(stagings and host_starts) and max(stagings) < min(host_starts)
    checks["the code list resolves a failed services apply with the apply lock token"] = "services_apply resolve" in code_body and "--token" in code_body
    checks.update(pasteable_checks(code_body, folder))
    checks.update(recovery_checks(code_body, folder))
    checks.update(dotenv_checks(code_body, folder))
    problems = owner_commands.check_shell_section(code_body, folder / "code", wrapper, {"ADMIN_PROFILE": ADMIN})
    checks[f"{CODE}: {len(problems)} printed commands were found"] = len(problems) >= 5
    for number, (line, found) in enumerate(problems.items(), 1):
        checks[f"{CODE}: command {number} parses: {line[:110]}" + (f" -> {found[0][:150]}" if found else "")] = not found
    return checks


MUTANTS = (
    ("terraform output with two names", "output -json host_code_fetch_role", "output -json host_code_fetch_role project_tag"),
    ("manage_policies without the administrator profile", "manage_policies plan --admin-profile <ADMIN_PROFILE>", "manage_policies plan"),
    ("manage_policies apply without the reviewed plan's digest", " --digest <PLAN_DIGEST>", ""),
    ("push_images without the repository file", "--repository-urls <RECORD_DIR>/repository_urls.json ", ""),
    ("a mistyped module", "scripts.lakehouse.code_archive build", "scripts.lakehouse.code_arch build"),
    ("an unknown placeholder", "--output /tmp/code_<RUN_ID>", "--output /tmp/code_<CODE_DIR>"),
    ("a mistyped wrapper option", "--preparation-sha256 <SHA256> plan", "--preparation-sha25 <SHA256> plan"),
    ("a mistyped host start option", "host_session start --code-run <RUN_ID>", "host_session start --code-runs <RUN_ID>"),
    ("a services apply status without its subcommand", "services_apply status", "services_apply"),
)


def mutant_checks(text: str, folder: Path) -> dict[str, bool]:
    """Each planted typo in one printed command must fail the parser check of that line."""
    lines = owner_commands.commands(owner_commands.section(text, CODE))
    wrapper = make_wrapper(Workspace(folder / "mutant_workspace"), {})
    checks: dict[str, bool] = {}
    for label, old, new in MUTANTS:
        line = next((item for item in lines if old in item), None)
        if line is None:
            checks[f"mutant {label}: the command exists in the lists"] = False
            continue
        table = owner_commands.values(folder / "mutant" / "record", {"ADMIN_PROFILE": ADMIN})
        (folder / "mutant" / "record").mkdir(parents=True, exist_ok=True)
        try:
            found = owner_commands.check_line(owner_commands.filled(line.replace(old, new, 1), table), folder / "mutant", wrapper)
        except (KeyError, ValueError) as error:
            found = [str(error)]
        checks[f"mutant {label} is refused"] = bool(found)
    return checks


def run(output: Path) -> dict[str, dict[str, bool]]:
    text = "".join(f"## {heading}\n{body}" for heading, body in json.loads(OWNER_LISTS.read_text()).items())
    with tempfile.TemporaryDirectory(prefix="owner_list_") as folder:
        results = {
            "structure": structure_checks(text),
            "commands": command_checks(text, Path(folder)),
            "mutants": mutant_checks(text, Path(folder)),
            "host": owner_list_host_cases.host_checks(text, Path(folder)),
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
    sys.stdout.write(f"owner list E2E: {total - len(failed)} of {total} checks passed\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
