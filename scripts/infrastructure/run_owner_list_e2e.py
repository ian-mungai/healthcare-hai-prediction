"""E2E of the owner's first layer run in docs/aws_broker.md: the order of the steps, the flags each step names and every printed command against its parser.

    .venv/bin/python -m scripts.infrastructure.run_owner_list_e2e --out <new folder>

The commands come from the "Silver first run" and "Gold and features after silver" sections. Each runs through the parser of its real program with synthetic
values (``scripts.infrastructure.owner_commands``), so a typo, a missing argument or a wrong option fails here and not on the owner's first run. The reviewed
jobs file that the list describes passes the real renderer through the real wrapper. Nothing calls AWS or Docker."""

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

from scripts.infrastructure import owner_commands
from scripts.infrastructure.run_activation_inputs_e2e import ADMIN, ROOT, ROWS, STACKS, Workspace, variables_of
from scripts.process import run_command

DOCUMENT = ROOT / "docs/aws_broker.md"
SILVER, GOLD = "Silver first run", "Gold and features after silver"
IAM_FLAGS = {"job_writer_layers_prepared": True, "job_layer_runs_prepared": True, "job_endpoints_enabled": True}
STACK_OPTIONS = ("--iam", "--platform", "--services", "--jobs")


def steps_of(body: str) -> list[str]:
    return [step for step in re.split(r"(?m)^(?=\d+\. )", body) if re.match(r"\d+\. ", step)]


def gold_sample(silver: dict[str, Any]) -> dict[str, Any]:
    """The reviewed file of the gold step: the silver file with the gold role and pair added."""
    variables = json.loads(json.dumps(silver["variables"]))
    variables["layer_jobs"] = ["silver", "gold"]
    variables["job_role_arns"]["write_gold_task"] = variables["job_role_arns"]["write_silver_task"].replace("silver", "gold")
    variables["writer_parameter_names"]["write_gold_task"] = [
        name.replace("silver", "gold") for name in variables["writer_parameter_names"]["write_silver_task"]
    ]
    return silver | {"variables": variables}


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
    body = owner_commands.section(text, SILVER)
    steps = steps_of(body)
    numbers = [int(step.split(".", 1)[0]) for step in steps]
    checks = {"the silver section is one numbered list": bool(body) and numbers == list(range(1, len(numbers) + 1)) and len(numbers) >= 12}

    def index(*needles: str) -> int:
        return next((position for position, step in enumerate(steps) if all(needle in step for needle in needles)), -1)

    iam = index("job_writer_layers_prepared", "job_layer_runs_prepared", "job_endpoints_enabled")
    checks["one IAM step sets the two layer flags and the endpoints flag together"] = iam >= 0
    checks["the IAM step reads job_network and stops unless endpoints_enabled is true"] = (
        iam >= 0 and "endpoints_enabled" in steps[iam] and "job_network" in steps[iam]
    )
    checks["the IAM step states that the plan may replace the host launch template"] = (
        iam >= 0 and "launch template" in steps[iam] and "Stop on any other change" in steps[iam]
    )
    order = [
        ("platform values", index("output -raw cluster_name")),
        ("IAM apply", iam),
        ("policies", index("manage_policies", "CONFIRM_IAM_POLICIES")),
        ("test host start", index("host_session start", "describe-instances")),
        ("catalog identities", index("aws_catalog_setup", "--identity silver")),
        ("images", index("push_images plan", "--repository-urls", "--only spark analytics quality")),
        ("jobs file", index("--jobs", "--preparation-sha256", "layer_jobs")),
        ("staging and approval", index("code_archive", "layer_acceptance approve")),
        ("IAM preflight", index("iam_preflight", "--scope silver")),
        ("dry run", index("run-task", "--dry-run")),
        ("dry run result", index("layer_acceptance task", "layer_acceptance plan", "get-log-events", "silver/publish/")),
        ("real run", index("before.json", "overrides_real.json")),
        ("evidence", index("after.json", "--include '*.json'", "list-tasks")),
        ("host stop and report", index("host_session stop", "layer_acceptance report", "layer_acceptance sign")),
        ("endpoints off", index("job_endpoints_enabled", "`false`")),
    ]
    positions = [position for _, position in order]
    checks[f"the steps come in the order the stacks depend on each other ({positions})"] = all(position >= 0 for position in positions) and positions == sorted(
        positions
    )
    jobs = index("--jobs", "--preparation-sha256", "layer_jobs")
    checks["the jobs step takes its roles, pair names and log group from the outputs of the earlier steps"] = jobs >= 0 and all(
        word in steps[jobs] for word in ("job_roles", "job_writer_layer_contract", "log_group_names", "images")
    )
    preflight = index("iam_preflight", "--scope silver")
    checks["the preflight step stops the list on a FAIL line and names the call that is denied"] = preflight >= 0 and all(
        word in steps[preflight] for word in ("`FAIL`", "Stop the list", "the role, the action and the resource")
    )
    dry = index("run-task", "--dry-run")
    checks["the dry run step requires an empty failures list and settles a lost reply"] = (
        dry >= 0 and "`failures` is empty" in steps[dry] and "lost reply" in steps[dry]
    )
    result = index("layer_acceptance task", "layer_acceptance plan")
    checks["the result step requires every container to exit 0 and reads this task's stream"] = result >= 0 and all(
        word in steps[result]
        for word in ("every container exited 0", "`init`", "`upload`", "silver/publish/<TASK_ID>", "--start-from-head", "stop-task --profile")
    )
    checks["the stop is bounded: the wait ends after 10 minutes and a late task is stopped"] = (
        result >= 0 and "10 minutes" in steps[result] and "6.5 hours" in steps[result]
    )
    gold = owner_commands.section(text, GOLD)
    checks["the gold section is a numbered list and names features as not ready with H3 open"] = (
        bool(steps_of(gold)) and "not ready to run" in gold and "features H3" in gold
    )
    checks["the gold section takes the signed silver run as input and names the feature store grant"] = (
        "--input-run <SILVER_RUN_ID>" in gold and "job_features_image_prepared" in gold
    )
    variables = {name for names in (variables_of(directory) for directory in STACKS.values()) for name in names}
    named = {name for name in re.findall(r"`([a-z][a-z0-9_]+)`", body + gold) if name in variables}
    rows = {row.name for row in ROWS}
    checks[f"every input the lists name has a row ({sorted(named - rows)})"] = named <= rows
    return checks


def command_checks(text: str, folder: Path) -> dict[str, bool]:
    workspace = Workspace(folder / "workspace")
    silver_body = owner_commands.section(text, SILVER)
    sample = owner_commands.example_json(silver_body) or {}
    samples = {"jobs_silver.json": sample, "jobs_gold.json": gold_sample(sample) if sample else {}}
    wrapper = make_wrapper(workspace, samples)
    checks: dict[str, bool] = {
        "the reviewed jobs file of the list is valid JSON with a root and variables": sample.get("root") == "jobs" and "variables" in sample
    }
    for heading in (SILVER, GOLD):
        problems = owner_commands.check_section(owner_commands.section(text, heading), folder / heading.split()[0].lower(), wrapper, {"ADMIN_PROFILE": ADMIN})
        count = len(problems)
        checks[f"{heading}: {count} printed commands were found"] = count >= 5
        for number, (line, found) in enumerate(problems.items(), 1):
            checks[f"{heading}: command {number} parses: {line[:110]}" + (f" -> {found[0][:150]}" if found else "")] = not found
    return checks


MUTANTS = (
    ("terraform output with three names", "output -raw cluster_name", "output -raw cluster_name project_tag log_group_names"),
    ("manage_policies without the administrator profile", "manage_policies plan --admin-profile <ADMIN_PROFILE>", "manage_policies plan"),
    ("push_images without the repository file", "--repository-urls <RECORD_DIR>/repository_urls.json ", ""),
    (
        "describe-tasks without the region",
        "describe-tasks --profile <PROJECT_PROFILE> --region <REGION> --cluster",
        "describe-tasks --profile <PROJECT_PROFILE> --cluster",
    ),
    (
        "a mistyped aws option",
        "--cluster <CLUSTER_NAME> --tasks <TASK_ARN> --query '{failures",
        "--clustr <CLUSTER_NAME> --tasks <TASK_ARN> --query '{failures",
    ),
    ("a mistyped module", "scripts.lakehouse.layer_acceptance task", "scripts.lakehouse.layer_acceptanc task"),
    ("an unknown placeholder", "--layer silver --describe <RECORD_DIR>/describe_dry.json", "--layer silver --describe <RECORD_DIR>/<DESCRIBE_FILE>"),
    ("a broken JSON argument", '\'[{"capacityProvider":"FARGATE","weight":1}]\'', '\'[{"capacityProvider":"FARGATE","weight":1}\''),
    ("an unknown preflight scope", "iam_preflight --scope silver", "iam_preflight --scope bronze"),
    ("a report without its output path", " --out <RECORD_DIR>/qualification_silver.json", ""),
    ("an unknown layer", "layer_acceptance plan --layer silver", "layer_acceptance plan --layer bronze"),
)


def mutant_checks(text: str, folder: Path) -> dict[str, bool]:
    """Each planted typo in one printed command must fail the parser check of that line."""
    lines = [line for heading in (SILVER, GOLD) for line in owner_commands.commands(owner_commands.section(text, heading))]
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
    text = DOCUMENT.read_text()
    with tempfile.TemporaryDirectory(prefix="owner_list_") as folder:
        results = {"structure": structure_checks(text), "commands": command_checks(text, Path(folder)), "mutants": mutant_checks(text, Path(folder))}
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
