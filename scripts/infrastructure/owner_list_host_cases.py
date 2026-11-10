"""Cases of the owner list "Gold through Airflow on the host": a DAG run on the AWS host's Airflow, reached through a Session Manager port forward.

``scripts/infrastructure/run_owner_list_e2e.py`` runs them. The list must start no Fargate task: the job runs on the host through the direct ECS launch
that the DAG step calls. The forward must join the local port that ``scripts.orchestration.trigger_dag`` calls (``config/orchestration/platform.json``)
to the port of the host's API server (``infra/services/airflow.tf``). Every printed command runs through its real parser with synthetic values
(``scripts.infrastructure.owner_commands``), and each planted typo must fail that check. Nothing calls AWS or Airflow.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from scripts.infrastructure import owner_commands

ROOT = Path(__file__).resolve().parents[2]
HOST_DAG = "Gold through Airflow on the host"
PLATFORM_FILE = ROOT / "config/orchestration/platform.json"
AIRFLOW_FILE = ROOT / "infra/services/airflow.tf"
# The wait limit of trigger_dag, read from its source: the infrastructure scripts may not import the orchestration scripts.
TRIGGER_FILE = ROOT / "scripts/orchestration/trigger_dag.py"
MUTANTS = (
    (
        "a broken port forward parameter",
        '\'{"portNumber":["8080"],"localPortNumber":["18080"]}\'',
        '\'{"portNumber":["8080"],"localPortNumber":["18080"]\'',
    ),
    ("a mistyped Session Manager call", "ssm start-session", "ssm start-sessions"),
    ("an unknown workflow", "--dag gold_transform_dag", "--dag gold_dag"),
    ("a mistyped trigger option", "--silver-snapshot-run <SILVER_RUN_ID>", "--silver-snapshot-runs <SILVER_RUN_ID>"),
    ("a trigger without its wait deadline", " --wait-seconds 7200", ""),
    ("an unknown task command", "aws_tasks list", "aws_tasks lst"),
    (
        "a mistyped describe-tasks option",
        "describe-tasks --profile <PROJECT_PROFILE> --region <REGION> --cluster",
        "describe-tasks --profile <PROJECT_PROFILE> --region <REGION> --clustr",
    ),
)


def no_refusal(arguments: list[str]) -> tuple[int, str]:
    """The list calls no Terraform wrapper; a call to it is a defect of the list."""
    return 2, f"the host list calls no Terraform wrapper ({' '.join(arguments)})"


def ports() -> tuple[int, int]:
    """The local port that trigger_dag calls and the port that the host's API server listens on."""
    local = int(json.loads(PLATFORM_FILE.read_text())["listeners"]["airflow_api"]["port"])
    found = re.search(r"airflow api-server --host 127\.0\.0\.1 --port ([0-9]+)", AIRFLOW_FILE.read_text())
    return local, int(found.group(1)) if found else -1


def max_wait_seconds() -> float:
    """MAX_WAIT_SECONDS of trigger_dag, written there as a product of whole numbers; -1 when the line is not found."""
    found = re.search(r"^MAX_WAIT_SECONDS = ([0-9 *]+)$", TRIGGER_FILE.read_text(), re.M)
    if not found:
        return -1.0
    product = 1.0
    for factor in found.group(1).split("*"):
        product *= float(factor)
    return product


def structure_checks(body: str) -> dict[str, bool]:
    steps = [step for step in re.split(r"(?m)^(?=\d+\. )", body) if re.match(r"\d+\. ", step)]
    numbers = [int(step.split(".", 1)[0]) for step in steps]
    checks = {"the host DAG section is one numbered list": bool(body) and numbers == list(range(1, len(numbers) + 1)) and len(numbers) >= 6}

    def index(*needles: str) -> int:
        return next((position for position, step in enumerate(steps) if all(needle in step for needle in needles)), -1)

    checks["the host DAG list starts no Fargate task and no hand-started task"] = bool(body) and "FARGATE" not in body and "run-task" not in body
    order = [
        ("host and twin", index("host_session status", "describe-instances", "_job_gold_host", "requiresCompatibilities")),
        ("port forward", index("ssm start-session", "AWS-StartPortForwardingSession")),
        ("trigger", index("trigger_dag --dag gold_transform_dag", "--silver-snapshot-run <SILVER_RUN_ID>", "--run-id <RUN_ID>")),
        ("launch records", index("lakehouse/launch_records/<RUN_ID>/")),
        ("task evidence", index("describe-tasks", "capacityProviderName")),
        ("drain", index("aws_tasks list", "host_jobs")),
        ("host stop", index("host_session stop", "host_session status")),
    ]
    positions = [position for _, position in order]
    checks[f"the host DAG steps come in the order the run depends on ({positions})"] = all(position >= 0 for position in positions) and positions == sorted(
        positions
    )
    local, remote = ports()
    checks[f"the forward joins local port {local} (trigger_dag) to the API server's port {remote}"] = (
        remote > 0 and f'{{"portNumber":["{remote}"],"localPortNumber":["{local}"]}}' in body
    )
    waits = [float(value) for value in re.findall(r"--wait-seconds ([0-9]+)", body)]
    limit = max_wait_seconds()
    checks[f"every trigger wait is within trigger_dag's limit of {limit:g} s ({waits})"] = (
        limit > 0 and bool(waits) and all(0 < wait <= limit for wait in waits)
    )
    trigger = order[2][1]
    checks["the trigger step names the chain refusal and the token secret that a local start rotates"] = trigger >= 0 and all(
        word in steps[trigger] for word in ("the chain is closed", "first_publishes_signed", "HTTP 401", "local Airflow")
    )
    checks["the trigger step names the silver launch record that the gold preflight needs"] = (
        trigger >= 0 and "lakehouse/launch_records/<SILVER_RUN_ID>/silver/" in steps[trigger]
    )
    checks["the list says the pool host_jobs has one slot after each start and no command changes it"] = all(
        word in body for word in ("`host_jobs`", "one slot", "no repository command")
    )
    forward = order[1][1]
    checks["the forward step names the plugin, the second terminal and the local port check"] = forward >= 0 and all(
        word in steps[forward] for word in ("Session Manager plugin", "second terminal", "lsof")
    )
    return checks


def command_checks(body: str, folder: Path) -> dict[str, bool]:
    problems = owner_commands.check_section(body, folder / "host_dag", no_refusal)
    checks = {f"{HOST_DAG}: {len(problems)} printed commands were found": len(problems) >= 8}
    for number, (line, found) in enumerate(problems.items(), 1):
        checks[f"{HOST_DAG}: command {number} parses: {line[:110]}" + (f" -> {found[0][:150]}" if found else "")] = not found
    return checks


def mutant_checks(body: str, folder: Path) -> dict[str, bool]:
    """Each planted typo in one printed command of the host list must fail the parser check of that line."""
    lines = owner_commands.commands(body)
    record = folder / "host_mutant" / "record"
    record.mkdir(parents=True, exist_ok=True)
    table = owner_commands.values(record)
    checks: dict[str, bool] = {}
    for label, old, new in MUTANTS:
        line = next((item for item in lines if old in item), None)
        if line is None:
            checks[f"host mutant {label}: the command exists in the list"] = False
            continue
        try:
            found = owner_commands.check_line(owner_commands.filled(line.replace(old, new, 1), table), folder / "host_mutant", no_refusal)
        except (KeyError, ValueError) as error:
            found = [str(error)]
        checks[f"host mutant {label} is refused"] = bool(found)
    return checks


def host_checks(text: str, folder: Path) -> dict[str, bool]:
    """Every case of the host list: structure, printed commands and planted typos."""
    body = owner_commands.section(text, HOST_DAG)
    return structure_checks(body) | command_checks(body, folder) | mutant_checks(body, folder)
