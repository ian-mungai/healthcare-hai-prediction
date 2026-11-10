"""Cases of the owner list "Silver and gold through Airflow on the host": DAG runs on the AWS host's Airflow, reached through a Session Manager port forward.

``scripts/infrastructure/run_owner_list_e2e.py`` runs them. The list must start no Fargate task: each job runs on the host through the direct ECS launch
that its DAG step calls. Silver runs first, because the gold preflight needs the silver launch record. Each run has its code staged under its own run ID
before its trigger, because the launch reads ``lakehouse/run_stage/<run>/inputs/code.sha256`` of the run. The owner approves each run ID before it starts
and signs each first publish by hand; the signed publishes are not a code gate (owner, Nov 10 2026), so the list names no closed chain. Every stop
condition leads to one bounded failure path that reads the run's records and tasks, closes the forward and stops and verifies the host.
The forward must join the local port that ``scripts.orchestration.trigger_dag`` calls (``config/orchestration/platform.json``) to the port of the host's
API server (``infra/services/airflow.tf``). Every printed command runs through its real parser with synthetic values
(``scripts.infrastructure.owner_commands``), and each planted typo or planted omission must fail its check. Nothing calls AWS or Airflow.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path

from scripts.infrastructure import owner_commands

ROOT = Path(__file__).resolve().parents[2]
HOST_DAG = "Silver and gold through Airflow on the host"
PLATFORM_FILE = ROOT / "config/orchestration/platform.json"
AIRFLOW_FILE = ROOT / "infra/services/airflow.tf"
# The wait limit of trigger_dag, read from its source: the infrastructure scripts may not import the orchestration scripts.
TRIGGER_FILE = ROOT / "scripts/orchestration/trigger_dag.py"
RUNS = {"silver": "SILVER_RUN_ID", "gold": "GOLD_RUN_ID"}
FAILURE_PATH = "If a step fails"
MUTANTS = (
    (
        "a broken port forward parameter",
        '\'{"portNumber":["8080"],"localPortNumber":["18080"]}\'',
        '\'{"portNumber":["8080"],"localPortNumber":["18080"]\'',
    ),
    ("a mistyped Session Manager call", "ssm start-session", "ssm start-sessions"),
    ("an unknown workflow", "--dag gold_transform_dag", "--dag gold_dag"),
    ("a mistyped trigger option", "--silver-snapshot-run <SILVER_RUN_ID>", "--silver-snapshot-runs <SILVER_RUN_ID>"),
    ("a trigger without its wait deadline", "--mode real --run-id <SILVER_RUN_ID> --wait-seconds 14400", "--mode real --run-id <SILVER_RUN_ID>"),
    ("an unknown task command", "aws_tasks list", "aws_tasks lists"),
    (
        "a mistyped describe-tasks option",
        "describe-tasks --profile <PROJECT_PROFILE> --region <REGION> --cluster",
        "describe-tasks --profile <PROJECT_PROFILE> --region <REGION> --clusters",
    ),
    ("an approval without its code digest", "--run <GOLD_RUN_ID> --code-sha256 <CODE_SHA256> --out", "--run <GOLD_RUN_ID> --out"),
    ("a gold report without its output path", " --out <RECORD_DIR>/qualification_gold.json", ""),
    ("a mistyped settle option", "--run-id <RUN_ID> --settle --wait-seconds 600 >", "--run-id <RUN_ID> --settled --wait-seconds 600 >"),
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


def steps_of(body: str) -> list[str]:
    return [step for step in re.split(r"(?m)^(?=\d+\. )", body) if re.match(r"\d+\. ", step)]


def finder(steps: list[str]) -> Callable[..., int]:
    def index(*needles: str) -> int:
        return next((position for position, step in enumerate(steps) if all(needle in step for needle in needles)), -1)

    return index


def trigger_of(layer: str) -> tuple[str, ...]:
    return (f"trigger_dag --dag {layer}_transform_dag", f"--run-id <{RUNS[layer]}>")


def staged_before_trigger(body: str, layer: str) -> bool:
    """The archive and the digest of the run are copied to its run_stage inputs in a step before the step that triggers the run."""
    index = finder(steps_of(body))
    run = RUNS[layer]
    staged = max(index(f"run_stage/<{run}>/inputs/code.tar.gz"), index(f"run_stage/<{run}>/inputs/code.sha256"))
    archive = index("code_archive build")
    trigger = index(*trigger_of(layer))
    return min(staged, archive) >= 0 and trigger >= 0 and max(staged, archive) < trigger


def failure_path_checks(step: str) -> dict[str, bool]:
    """The failure path settles the Airflow run before it drains and stops: a missing launch record is an unconfirmed state, never a refusal; the run's
    state is read and settled over the forward; when Airflow cannot be reached, a restart hold keeps the run from launching a job at the next start."""

    def at(needle: str) -> int:
        return step.find(needle)

    order = [
        at("lakehouse/launch_records/<RUN_ID>/"),
        at("trigger_dag --dag <DAG_ID> --run-id <RUN_ID> --status"),
        at("trigger_dag --dag <DAG_ID> --run-id <RUN_ID> --settle"),
        at("inputs/code_run.json"),
        at("aws_tasks list"),
        at("host_session stop"),
    ]
    return {
        "the failure path calls a missing launch record an unconfirmed launch state, not a refusal": "unconfirmed" in step
        and "refused before any ECS call" not in step,
        f"the failure path reads the records, then the run's status, settles it, holds it, drains and stops ({order})": all(position >= 0 for position in order)
        and order == sorted(order),
        "the settle is bounded and its success is a terminal or absent run": all(
            word in step for word in ("--settle --wait-seconds 600", "--status --wait-seconds 60", "`failed`", "`absent`")
        ),
        "the restart hold binds the run to a code run with nothing staged and is recorded": all(
            word in step for word in ("restart hold", "<HOLD_RUN_ID>", "restart_hold.txt", "code_digest_not_staged")
        ),
    }


def structure_checks(body: str) -> dict[str, bool]:
    steps = steps_of(body)
    numbers = [int(step.split(".", 1)[0]) for step in steps]
    checks = {"the host DAG section is one numbered list": bool(body) and numbers == list(range(1, len(numbers) + 1)) and len(numbers) >= 12}
    index = finder(steps)
    checks["the host DAG list starts no Fargate task and no hand-started task"] = bool(body) and "FARGATE" not in body and "run-task" not in body
    order = [
        ("host and twins", index("host_session status", "describe-instances", "_job_silver_host", "_job_gold_host", "requiresCompatibilities")),
        ("port forward", index("ssm start-session", "AWS-StartPortForwardingSession")),
        ("silver staging and approval", index("code_archive build", "run_stage/<SILVER_RUN_ID>/inputs/code.sha256", "approve --layer silver")),
        ("silver trigger", index(*trigger_of("silver"), "before_silver.json")),
        ("silver evidence", index("lakehouse/launch_records/<SILVER_RUN_ID>/", "describe-tasks", "after_silver.json", "_job_silver_host")),
        ("silver sign", index("host_session stop", "layer_acceptance report --layer silver", "layer_acceptance sign")),
        ("host restart", index("host_session start", "ssm start-session")),
        ("gold staging and approval", index("run_stage/<GOLD_RUN_ID>/inputs/code.sha256", "approve --layer gold")),
        ("gold trigger", index(*trigger_of("gold"), "--silver-snapshot-run <SILVER_RUN_ID>", "before_gold.json")),
        ("gold evidence", index("lakehouse/launch_records/<GOLD_RUN_ID>/", "describe-tasks", "after_gold.json", "_job_gold_host")),
        ("drain", index("aws_tasks list", "host_jobs")),
        ("gold sign", index("host_session stop", "layer_acceptance report --layer gold", "--input-report", "layer_acceptance sign")),
        ("failure path", index(FAILURE_PATH, "aws_tasks list", "aws_tasks stop", "host_session stop", "host_session status")),
    ]
    positions = [position for _, position in order]
    checks[f"the host DAG steps come in the order the runs depend on ({positions})"] = all(position >= 0 for position in positions) and positions == sorted(
        positions
    )
    for layer in RUNS:
        checks[f"the {layer} run has its code staged under its own run ID before its trigger"] = staged_before_trigger(body, layer)
    planted = re.sub(r"(?m)^.*run_stage/<GOLD_RUN_ID>/inputs/code\.(tar\.gz|sha256).*\n", "", body)
    checks["planted: a list without the gold code staging fails the staging case"] = planted != body and not staged_before_trigger(planted, "gold")
    checks["the code is staged before the version listing that the report compares"] = all(
        0 <= index(f"run_stage/<{run}>/inputs/code.sha256") <= index(f"before_{layer}.json") for layer, run in RUNS.items()
    )
    checks["the list names no closed chain: the signed publishes are a manual step"] = (
        bool(body) and "first_publishes_signed" not in body and "chain is closed" not in body
    )
    checks["the owner signs each first publish by hand"] = (
        all(f"layer_acceptance report --layer {layer}" in body for layer in RUNS) and body.count("layer_acceptance sign") >= 2 and "by hand" in body
    )
    checks["the gold step says it needs the silver launch record of step 5"] = index(*trigger_of("gold")) >= 0 and (
        "lakehouse/launch_records/<SILVER_RUN_ID>/silver/" in steps[index(*trigger_of("gold"))]
    )
    local, remote = ports()
    checks[f"the forward joins local port {local} (trigger_dag) to the API server's port {remote}"] = (
        remote > 0 and body.count(f'{{"portNumber":["{remote}"],"localPortNumber":["{local}"]}}') >= 2
    )
    waits = [float(value) for value in re.findall(r"--mode real .*?--wait-seconds ([0-9]+)", body)]
    limit = max_wait_seconds()
    checks[f"every trigger wait is within trigger_dag's limit of {limit:g} s ({waits})"] = (
        limit > 0 and len(waits) == 2 and all(0 < wait <= limit for wait in waits)
    )
    failure = order[-1][1]
    stops = [position for position, step in enumerate(steps) if re.search(r"\bStop (if|on)\b", step) and position != failure]
    checks[f"every step with a stop condition sends the owner to the failure path ({stops})"] = (
        failure >= 0 and bool(stops) and all(f"step {failure + 1}" in steps[position] for position in stops)
    )
    checks["the failure path is bounded and names what it establishes"] = failure >= 0 and all(
        word in steps[failure] for word in ("at most", "launch_records", "Ctrl-C", "no instance", "new run ID")
    )
    checks |= failure_path_checks(steps[failure] if failure >= 0 else "")
    planted = re.sub(r"(?m)^.*trigger_dag .*--settle.*\n", "", steps[failure]) if failure >= 0 else ""
    checks["planted: a failure path without the settle fails the settle order case"] = bool(planted) and not all(failure_path_checks(planted).values())
    forward_step = index("ssm start-session", "AWS-StartPortForwardingSession")
    checks["a restart hold of an earlier session is settled right after the forward and before any staging"] = forward_step >= 0 and all(
        word in steps[forward_step] for word in ("restart hold", "restart_hold.txt", "--settle")
    )
    trigger = index(*trigger_of("silver"))
    checks["the silver trigger step names the token secret that a local start rotates"] = trigger >= 0 and all(
        word in steps[trigger] for word in ("HTTP 401", "local Airflow")
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
    checks = {f"{HOST_DAG}: {len(problems)} printed commands were found": len(problems) >= 20}
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
