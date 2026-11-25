"""Run the committed alert email owner scripts as written, against stand-in tools, and check what each one does.

Every script of the bootstrap and teardown (``scripts/infrastructure/owner/alert_email_*.sh``) is one bash step that runs one call of
``scripts.infrastructure.alert_email`` under ``set -euo pipefail``, a fixed ERR trap and a change into the repository root. The cases write the
script text to a file in the owner folder of a scratch repository root with a valid ``.env``, the stack folders and a stand-in ``aws``,
``terraform``, wrapper and ``.venv/bin/python`` first on the PATH (``owner_list_stand_ins``), and run it with ``bash``. Every read that the helper
makes can be made to fail or to return the wrong shape, and the case checks that the script stops with a status word before any later command.
Every run of every script adds its terminal output to one list, and one check holds that list to the closed vocabulary of the helper: every line is
``<subject>: <status>`` from its two enums, so no ARN, topic name, account number, address or subscription identifier is printed. The planted
defects are changes to the helper's source, run through the same scripts.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

from scripts.infrastructure import alert_email
from scripts.infrastructure import owner_list_stand_ins as world
from scripts.process import CompletedProcess, run_command

ROOT = Path(__file__).resolve().parents[2]
STAND_INS = Path(world.__file__).resolve()
HELPER_SOURCE = Path(alert_email.__file__).resolve()
HELPER_CALL = ".venv/bin/python -m scripts.infrastructure.alert_email"
ADDRESS = "owner@example.org"
# A valid SHA-256 whose hex digits hold a run of twelve decimal digits, so a mask of account numbers corrupts it.
NUMERIC_DIGEST = hashlib.sha256(b"synthetic-policy-plan-2").hexdigest()
PLACEHOLDER = re.compile(r"<[A-Z][A-Z0-9_]*>")
# One helper command: plain lowercase words and options, and the one quoted variable of the administrator profile. No operator, redirect or substitution fits.
HELPER_LINE = re.compile(rf'{re.escape(HELPER_CALL)}(?: (?:--[a-z][a-z-]*|[a-z][a-z-]*|"\$\{{ADMIN_PROFILE:-\}}"))+')
OWNER_FOLDER = "scripts/infrastructure/owner"
SHEBANG = "#!/usr/bin/env bash"
ROOT_LINE = 'cd "$(dirname "$0")/../../.."'
STRICT_OPTIONS = "set -euo pipefail"
# The trap holds no value: its body has only the exit status variable and one fixed status line of the helper's vocabulary.
TRAP_LINE = re.compile(r"trap '(?P<body>[^']*)' ERR")
TRAP_STATUS = re.compile(r'echo "(?P<text>[a-z ]+: [a-z ]+)"')
# What a retained log must never hold: the address, a topic name or ARN, an account number or a subscription identifier.
LOG_VALUES = re.compile(r"@|example_project|arn:aws|\d{6,}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}")
VOCABULARY = re.compile(
    rf"({'|'.join(re.escape(item.value) for item in alert_email.Subject)}): ({'|'.join(re.escape(item.value) for item in alert_email.Status)})"
)
REMOVED_WORDS = (
    "aws sns",
    "ConfirmationWasAuthenticated",
    "authenticate-on-unsubscribe",
    "AuthenticateOnUnsubscribe",
    "list-subscriptions-by-topic",
    "confirm-subscription",
    "/dev/tty",
    "terraform import",
)
SHIMS = {
    "bin/aws": 'exec "$STAND_IN_PYTHON" "$STAND_IN_SCRIPT" aws "$@"',
    "bin/terraform": 'exec "$STAND_IN_PYTHON" "$STAND_IN_SCRIPT" terraform "$@"',
    "scripts/infrastructure/terraform.sh": 'exec "$STAND_IN_PYTHON" "$STAND_IN_SCRIPT" wrapper "$@"',
    ".venv/bin/python": (
        'if [ "$1" = -m ]; then\n'
        '  case "$2" in\n'
        '    infra.iam.scripts.manage_policies|scripts.infrastructure.render_platform_config) exec "$STAND_IN_PYTHON" "$STAND_IN_SCRIPT" python "$@" ;;\n'
        "  esac\n"
        "fi\n"
        'exec "$STAND_IN_PYTHON" "$@"'
    ),
}
ENV_FILE = (
    f"AWS_ACCOUNT_ID={world.ACCOUNT}\nAWS_PROFILE=example_project_dev\nAWS_REGION=us-west-2\nENVIRONMENT=dev\nPROJECT_NAME=example_project\n"
    "S3_BUCKET=example-project-ci-bucket\nALERT_EMAIL={address}\n"
)


def fences(prefix: str) -> list[str]:
    """The text of every committed script of one procedure (``alert_email_bootstrap`` or ``alert_email_teardown``), in file name order."""
    return [path.read_text(encoding="utf-8") for path in sorted((ROOT / OWNER_FOLDER).glob(f"{prefix}_*.sh"))]


def pick(section: str, *needles: str) -> str:
    """The first script of the procedure whose command lines (not its comments) hold every needle."""
    found = [
        block for block in fences(section) if all(needle in "\n".join(line for line in block.splitlines() if not line.startswith("#")) for needle in needles)
    ]
    return found[0] if found else ""


def leaks(output: str) -> list[str]:
    """The lines of a block's output that are not a status line of the closed vocabulary."""
    return [line for line in output.splitlines() if not VOCABULARY.fullmatch(line)]


TRAP_OUTPUT = "step: stopped\n"
# Every failed block run, as (exit status, whether its output ended with the fixed ERR trap line); cases() reads and clears it.
FAILED_RUNS: list[tuple[int, bool, str]] = []


class Scratch:
    """A scratch repository root with the stand-in tools and a world that a case sets up. Every run adds its terminal output to ``sink``."""

    def __init__(self, root: Path, sink: list[str], address: str = ADDRESS, helper: str = HELPER_CALL) -> None:
        self.root = root
        self.sink = sink
        self.helper = helper
        self.raw = ""
        for name, body in SHIMS.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"#!/bin/sh\n{body}\n")
            path.chmod(0o755)
        (root / "infra/alerts").mkdir(parents=True)
        (root / ".env").write_text(ENV_FILE.replace("{address}", address))
        self.state_file = root / "state.json"
        self.state: dict[str, Any] = {"address": ADDRESS, "aws": {}, "tf": [], "tf_arn": {}, "calls": [], "policy_applied": False}
        self.write()

    def write(self) -> None:
        self.state_file.write_text(json.dumps(self.state))

    def read(self) -> dict[str, Any]:
        self.state = json.loads(self.state_file.read_text())
        return self.state

    def subscribe(self, topic: str, status: str, name: str = "1", endpoint: str = ADDRESS) -> str:
        arn = f"{topic}:00000000-0000-0000-0000-{name:0>12}"
        self.read()
        self.state["aws"].setdefault(topic, []).append({"arn": arn, "endpoint": endpoint, "status": status})
        self.write()
        return arn

    def own(self, address: str, arn: str) -> None:
        self.read()
        self.state["tf"].append(address)
        self.state["tf_arn"][address] = arn
        self.write()
        (self.root / "infra/alerts/terraform.tfstate").write_text("{}\n")

    def confirm_all(self) -> None:
        """The owner clicks the link in every email."""
        self.read()
        for rows in self.state["aws"].values():
            for row in rows:
                row["status"] = "confirmed"
        self.write()

    def set(self, **values: object) -> None:
        self.read()
        self.state.update(values)
        self.write()

    def run(self, block: str, *, admin: bool = True, timeout: int = 120) -> tuple[int, str, list[str]]:
        """Run the block text as delivered, with the helper call replaced only in a planted-defect run."""
        file = self.root / OWNER_FOLDER / "block.sh"
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(block.replace(HELPER_CALL, self.helper))
        before = len(self.read()["calls"])
        environment = {
            "PATH": f"{self.root / 'bin'}:/usr/bin:/bin:/usr/sbin:/sbin",
            "HOME": str(self.root),
            "STAND_IN_STATE": str(self.state_file),
            "STAND_IN_PYTHON": sys.executable,
            "STAND_IN_SCRIPT": str(STAND_INS),
            "PYTHONPATH": str(ROOT),
            "PYTHONDONTWRITEBYTECODE": "1",
            "BLOCK_FILE": str(file),
        }
        if admin:
            environment["ADMIN_PROFILE"] = "example_admin"
        result = run_command("bash", ["-c", 'bash "$BLOCK_FILE"'], cwd=self.root, env=environment, timeout=timeout, own_session=True, errors="replace")
        raw = result.stdout + result.stderr
        self.sink.append(raw)
        self.raw = raw
        output = raw
        if result.returncode != 0:
            # Only a block with the delivered trap counts. Bash ends a set -u stop without the ERR trap but prints its own message, so that stop is visible too.
            if TRAP_LINE.search(block) and "unbound variable" not in raw:
                FAILED_RUNS.append((result.returncode, raw.endswith(TRAP_OUTPUT), (raw.splitlines() or [""])[-1][:40]))
            output = raw.removesuffix(TRAP_OUTPUT)
        return result.returncode, output, self.read()["calls"][before:]

    def log(self) -> str:
        path = self.root / "plans/alert_email/last_run.log"
        return path.read_text() if path.is_file() else ""


def syntax_check(file: Path, shell: str = "bash") -> CompletedProcess[str]:
    """``<shell> -n`` on the file (``bash`` or ``zsh``): the shell parses it and runs nothing."""
    return run_command(
        "bash", ["-c", '"$SYNTAX_SHELL" -n "$BLOCK_FILE"'], env={"PATH": "/usr/bin:/bin", "BLOCK_FILE": str(file), "SYNTAX_SHELL": shell}, timeout=30
    )


def block_problems(block: str) -> list[str]:
    """What a committed step lacks: it is a bash script of exactly the strict options, a fixed value-free ERR trap, the change into the repository root and one
    helper call. Comment lines, which hold the prose of the step, are free."""
    lines = [line for line in block.splitlines() if line.strip() and not line.startswith("#")]
    if len(lines) != 4:
        return [f"the script has {len(lines)} command lines, not 4"]
    problems = []
    if not block.startswith(SHEBANG + "\n"):
        problems.append("not a bash script")
    if lines[0] != STRICT_OPTIONS:
        problems.append("no strict options")
    trap = TRAP_LINE.fullmatch(lines[1])
    status = TRAP_STATUS.fullmatch(trap["body"]) if trap else None
    if not status or not VOCABULARY.fullmatch(status["text"]):
        problems.append("no fixed value-free ERR trap")
    if lines[2] != ROOT_LINE:
        problems.append("no change into the repository root")
    if not HELPER_LINE.fullmatch(lines[3]) or PLACEHOLDER.search("\n".join(lines)):
        problems.append("not one placeholder-free helper command of plain words")
    return problems


def helper_call(block: str) -> str | None:
    """The helper call of a script that is exactly the frame ``block_problems`` pins, or None when the script is not."""
    return None if block_problems(block) else [line for line in block.splitlines() if line.strip() and not line.startswith("#")][3]


def static_checks(procedures: dict[str, str]) -> dict[str, bool]:
    """Every script is a bash step with the strict options, a fixed value-free ERR trap and one placeholder-free call of the helper, with valid syntax in
    bash and in zsh, and nothing of the removed path is left in them."""
    checks: dict[str, bool] = {}
    blocks = [(name, block) for name, prefix in procedures.items() for block in fences(prefix)]
    checks["the alert email scripts are 13 (9 of the bootstrap and 4 of the teardown)"] = len(blocks) == 13
    with tempfile.TemporaryDirectory(prefix="alert_blocks_") as folder:
        for number, (name, block) in enumerate(blocks, 1):
            file = Path(folder) / f"block_{number}.sh"
            file.write_text(block)
            call = next((line for line in block.splitlines() if line.startswith(HELPER_CALL)), "")
            problems = block_problems(block)
            checks[f"{name} block {number} is a strict bash script with a fixed ERR trap ({problems}): {call[len(HELPER_CALL) :].strip()[:60]}"] = not problems
            checks[f"{name} block {number} has valid syntax in bash: {call[len(HELPER_CALL) :].strip()[:60]}"] = syntax_check(file).returncode == 0
            if shutil.which("zsh"):
                checks[f"{name} block {number} has valid syntax in zsh, the owner's terminal: {call[len(HELPER_CALL) :].strip()[:60]}"] = (
                    syntax_check(file, "zsh").returncode == 0
                )
    text = "".join(block for _, block in blocks)
    checks["the scripts keep nothing of the administrator confirmation, the hand import or the terminal token"] = not [
        word for word in REMOVED_WORDS if word in text
    ]
    return checks


def duplicate_changes(first: list[str], second: list[str]) -> list[dict[str, Any]]:
    """The changes of a plan that names the alerts subscription twice, with the given actions, and the budget subscription once."""
    return [
        {"address": world.ALERTS_ADDRESS, "change": {"actions": first}},
        {"address": world.ALERTS_ADDRESS, "change": {"actions": second}},
        {"address": world.BUDGET_ADDRESS, "change": {"actions": ["create"]}},
    ]


def first_call(called: list[str], word: str) -> bool:
    return bool([item for item in called if word in item])


def bootstrap_cases(section: str, sink: list[str], helper: str) -> tuple[dict[str, bool], list[str]]:
    checks: dict[str, bool] = {}
    calls: list[str] = []
    policy_plan = pick(section, "policy-plan")
    policy_apply = pick(section, "policy-apply")
    plan_platform = pick(section, "plan platform")
    apply_platform = pick(section, "apply platform")
    topics = pick(section, " topics")
    prepare = pick(section, " prepare")
    plan_alerts = pick(section, "plan alerts")
    apply_alerts = pick(section, "apply alerts")
    verify = pick(section, "subscriptions --expect confirmed")

    def case(label: str, ok: bool) -> None:
        checks[f"alert block run: {label}"] = bool(ok)

    def scratch_for(folder: str, **extra: object) -> Scratch:
        scratch = Scratch(Path(folder), sink, helper=helper)
        if extra:
            scratch.set(**extra)
        return scratch

    # The whole first bootstrap, block by block, from nothing in AWS to two confirmed subscriptions.
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = scratch_for(folder)
        code, output, called = scratch.run(policy_apply)
        calls += called
        case(
            "a policy apply without a saved digest stops and applies nothing",
            code == 1 and output == "plan digest: missing\n" and not scratch.read()["policy_applied"],
        )
        code, output, called = scratch.run(policy_plan, admin=False)
        calls += called
        case("a policy plan without ADMIN_PROFILE stops before any command", code == 1 and output == "administrator profile: missing\n" and not called)
        code, output, called = scratch.run(policy_plan)
        calls += called
        saved = scratch.root / "plans/alert_email/policy_digest.txt"
        case(
            "the policy plan stores the digest exactly as printed and prints two status words",
            code == 0 and output == "plan digest: saved\npolicy: planned\n" and saved.is_file() and saved.read_text().strip() == world.DIGEST,
        )
        code, output, called = scratch.run(policy_apply)
        calls += called
        case(
            "the policy apply passes the stored digest to the reviewed apply",
            code == 0 and output == "policy: applied\n" and scratch.read()["policy_applied"] and first_call(called, f"--digest {world.DIGEST}"),
        )
        for label, block in (
            ("the platform plan", plan_platform),
            ("the platform apply", apply_platform),
            ("the topic check", topics),
            ("the alerts root preparation", prepare),
        ):
            code, output, called = scratch.run(block)
            calls += called
            case(f"{label} runs as pasted and stops on nothing", code == 0)
        code, output, called = scratch.run(apply_alerts)
        case("an alerts apply without a saved plan stops", code == 1 and output == "alerts: missing\n")
        code, output, called = scratch.run(plan_alerts)
        calls += called
        case(
            "the alerts plan prints a status per subscription and keeps only command identity and exit status in the log",
            code == 0
            and output == "alerts topic: planned\nbudget topic: planned\nalerts: planned\n"
            and "alerts: bash exit 0" in scratch.log()
            and not LOG_VALUES.search(scratch.log()),
        )
        code, output, called = scratch.run(apply_alerts)
        calls += called
        case("the alerts apply creates both subscriptions as pending", code == 0 and output == "alerts: applied\n" and len(scratch.read()["tf"]) == 2)
        code, output, called = scratch.run(verify)
        calls += called
        case("the verification reports both subscriptions pending with exit code 3", code == 3 and output == "alerts topic: pending\nbudget topic: pending\n")
        scratch.confirm_all()
        code, output, called = scratch.run(verify)
        calls += called
        case("the verification passes after the owner clicks both links", code == 0 and output == "alerts topic: confirmed\nbudget topic: confirmed\n")
        case(
            "the log of the whole bootstrap holds no address, topic, account number or subscription identifier",
            bool(scratch.log()) and not LOG_VALUES.search(scratch.log()),
        )
        scratch.set(budget_output="")
        code, output, called = scratch.run(verify)
        case(
            "an empty budget output stops the verification before any AWS call",
            code == 1 and output == "topics: unexpected\n" and not first_call(called, "aws "),
        )

    # A plan digest whose hex digits hold twelve decimal digits is stored and applied exactly.
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = scratch_for(folder, digest=NUMERIC_DIGEST)
        case("the test digest holds a run of twelve decimal digits", bool(re.search(r"\d{12}", NUMERIC_DIGEST)))
        code, output, called = scratch.run(policy_plan)
        calls += called
        saved = scratch.root / "plans/alert_email/policy_digest.txt"
        case("a digest with a twelve digit run is stored unmasked", code == 0 and saved.is_file() and saved.read_text().strip() == NUMERIC_DIGEST)
        code, output, called = scratch.run(policy_apply)
        calls += called
        case(
            "a digest with a twelve digit run reaches the reviewed apply and the policy is applied",
            code == 0 and scratch.read()["policy_applied"] and first_call(called, f"--digest {NUMERIC_DIGEST}"),
        )
        saved.write_text("12345\n")
        code, output, called = scratch.run(policy_apply)
        case(
            "a stored digest that is not 64 hex characters stops the apply before any command",
            code == 1 and output == "plan digest: unexpected\n" and not called,
        )

    # The policy plan must hold the SNS policy only; a step for anything else stops it and stores no digest.
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = scratch_for(folder, policy_steps=["ATTACH POLICY    healthcare_hai_prediction_iam_policy -> some_group"])
        code, output, called = scratch.run(policy_plan)
        calls += called
        case(
            "a policy plan with a step for another policy stops and stores no digest",
            code == 1 and output == "policy: unexpected\n" and not (scratch.root / "plans/alert_email/policy_digest.txt").exists(),
        )

    # The round 4 reads: an empty or wrongly typed topic output stops the block before any AWS call.
    for label, extra in (
        ("an empty platform topic output", {"alerts_output": ""}),
        ("a platform topic output of another topic", {"alerts_output": world.BUDGET_TOPIC}),
        ("a platform topic output of true", {"alerts_output": "true"}),
        ("a budget output that holds a boolean", {"budget_output": '{"alerts_topic": true}'}),
        ("a budget output that is not JSON", {"budget_output": "not json"}),
        ("a budget output of the wrong shape", {"budget_output": "[]"}),
    ):
        with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
            scratch = scratch_for(folder, **extra)
            scratch.subscribe(world.BUDGET_TOPIC, "confirmed", "5")
            for name, block in (("the topic check", topics), ("the verification", verify)):
                code, output, called = scratch.run(block)
                calls += called
                case(
                    f"{label} stops {name} with a status word and no AWS call",
                    code == 1 and output == "topics: unexpected\n" and not first_call(called, "aws "),
                )
                if "alerts_output" in extra:
                    case(f"{label} stops {name} before the budget output is read", not first_call(called, "-chdir=infra/iam"))

    # A listing of the wrong shape stops the verification: a container that is not a list, a row of the wrong type, two rows, a subscription of another topic.
    topic_row = {"SubscriptionArn": "PendingConfirmation", "Protocol": "email", "Endpoint": ADDRESS}
    for label, raw in (
        ("a listing whose Subscriptions is an object", '{"Subscriptions": {}}'),
        ("an empty listing", ""),
        ("a listing that is a list", "[]"),
        ("a listing row of the wrong type", json.dumps({"Subscriptions": [{"SubscriptionArn": 5, "Protocol": "email", "Endpoint": ADDRESS}]})),
        ("two rows for the address", json.dumps({"Subscriptions": [topic_row, topic_row]})),
        (
            "a subscription ARN of another topic",
            json.dumps({"Subscriptions": [{**topic_row, "SubscriptionArn": f"{world.TOPIC_PREFIX}_other_dev:{'0' * 8}-0000-0000-0000-{'0' * 12}"}]}),
        ),
    ):
        with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
            scratch = scratch_for(folder, list_raw=raw)
            code, output, called = scratch.run(verify)
            calls += called
            case(f"{label} stops the verification with a status word", code == 1 and output == "alerts topic: unexpected\n")
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = scratch_for(folder, fail_list=True)
        code, output, called = scratch.run(verify)
        calls += called
        case("a failed listing (exit 255) stops the verification", code == 1 and output == "alerts topic: failed\n")
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = scratch_for(folder)
        scratch.subscribe(world.ALERTS_TOPIC, "confirmed", "3", endpoint="someone@example.org")
        code, output, called = scratch.run(verify)
        calls += called
        case(
            "a subscription of another address is not the owner's: the verification stops with exit code 1",
            code == 1 and output == "alerts topic: none\nbudget topic: none\n",
        )

    # A plan the helper cannot read, or that holds more than the two subscriptions, is never applied.
    for label, extra in (
        ("a plan of the wrong JSON shape", {"show_raw": json.dumps(world.plan_document(None, resource_changes={}))}),
        ("a plan that is not JSON", {"show_raw": "Plan: nothing"}),
        ("a plan document that is an empty object", {"show_raw": "{}"}),
        ("a plan document of a format version only", {"show_raw": '{"format_version": "1.2"}'}),
        ("a plan document of an empty change list only", {"show_raw": '{"resource_changes": []}'}),
        ("a plan document without a Terraform version", {"show_raw": json.dumps(world.plan_document([], terraform_version=None))}),
        ("a plan document without planned values", {"show_raw": json.dumps(world.plan_document([], planned_values=None))}),
        ("a plan document without a configuration", {"show_raw": json.dumps(world.plan_document([], configuration=None))}),
        ("a plan document of another format version type", {"show_raw": json.dumps(world.plan_document([], format_version=1.2))}),
        ("a plan document that errored", {"show_raw": json.dumps(world.plan_document([], errored=True))}),
        ("a plan that names one address twice, deleting it first", {"show_raw": json.dumps(world.plan_document(duplicate_changes(["delete"], ["create"])))}),
        ("a plan that names one address twice, creating it twice", {"show_raw": json.dumps(world.plan_document(duplicate_changes(["create"], ["create"])))}),
        ("a plan that names one address twice with no change", {"show_raw": json.dumps(world.plan_document(duplicate_changes(["no-op"], ["no-op"])))}),
        ("a plan that changes an unexpected resource", {"plan_extra": 'aws_sns_topic_subscription.email["reviewer@example.org"]'}),
        ("a plan that changes another resource type", {"plan_extra": "aws_sns_topic.audit"}),
    ):
        with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
            scratch = scratch_for(folder, **extra)
            code, output, called = scratch.run(plan_alerts)
            calls += called
            case(f"{label} stops the alerts plan with a status word", code == 1 and output == "alerts: unexpected\n")
            plan_file = scratch.root / "plans/alert_email/alerts.tfplan"
            case(
                f"{label} is not applied",
                plan_file.is_file() and scratch.run(apply_alerts)[0] == 1 and not first_call(scratch.read()["calls"], "wrapper --alerts apply"),
            )
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = scratch_for(folder, show_raw='{"resource_changes": [{"address": "aws_ecs_cluster.main", "change": {"actions": ["delete", "create"]}}]}')
        code, output, called = scratch.run(plan_platform)
        calls += called
        case("a platform plan that replaces a resource stops with a status word", code == 1 and output == "platform: unexpected\n")

    # A plan with no change has no ``resource_changes`` member and is valid: nothing to do, in both stacks.
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = scratch_for(folder, show_raw=json.dumps(world.plan_document(None)))
        for label, block, subject in (("alerts", plan_alerts, "alerts"), ("platform", plan_platform, "platform")):
            code, output, called = scratch.run(block)
            calls += called
            case(f"a {label} plan with no change and no change list is valid and reports nothing to do", code == 0 and output == f"{subject}: nothing to do\n")

    # A failed command leaves only its identity and exit status in the log, whatever its output holds.
    secret = f"provider failure for endpoint {ADDRESS} topic {world.ALERTS_TOPIC} subscription {world.ALERTS_TOPIC}:01234567-89ab-cdef-0123-456789abcdef"
    for label, block in (("the alerts plan", plan_alerts), ("the platform plan", plan_platform)):
        with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
            scratch = scratch_for(folder, wrapper_fail=secret)
            code, output, called = scratch.run(block)
            calls += called
            held = scratch.log()
            case(
                f"{label} with a failed command prints a status word and keeps no address, topic, account number or subscription identifier in the log",
                code == 1 and not LOG_VALUES.search(held) and "provider failure" not in held and "exit 1" in held,
            )
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = scratch_for(folder, list_raw=secret, fail_list=False)
        scratch.run(verify)
        case("a listing that holds the address and topics leaves none of it in the log", not LOG_VALUES.search(scratch.log()) and "exit 0" in scratch.log())

    # A bad argument prints a fixed status word, never its value; normal help still works.
    topic_value = f"{world.ALERTS_TOPIC}:01234567-89ab-cdef-0123-456789abcdef"
    for label, arguments in (
        ("an address as the command", ADDRESS),
        ("an address as the stack", f"plan {ADDRESS}"),
        ("an unknown option holding a topic", f"plan alerts --expect {world.ALERTS_TOPIC}"),
        ("a topic as the expectation", f"subscriptions --expect {world.ALERTS_TOPIC}"),
        ("a subscription after topics", f"topics {topic_value}"),
        ("a missing option value", "policy-plan --admin-profile"),
        ("no command", ""),
    ):
        with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
            scratch = scratch_for(folder)
            code, output, called = scratch.run(f"{HELPER_CALL} {arguments}\n")
            case(f"{label} stops with the fixed status word and no command", code == 1 and output == "step: unexpected\n" and not called)
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = scratch_for(folder)
        for name in ("--help", "plan --help", "subscriptions --help"):
            code, output, called = scratch.run(f"{HELPER_CALL} {name}\n")
            sink.pop()
            case(f"`{name}` still prints the usage and exits 0", code == 0 and output.startswith("usage:") and not called)

    # The fixed ERR trap: every failed command ends with one fixed line, even one that stops before the helper prints; its status is kept.
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = scratch_for(folder)
        for status, expected in (
            (0, ""),
            (1, "step: stopped\n"),
            (3, "step: stopped\n"),
            (2, "step: stopped\n"),
            (5, "step: stopped\n"),
            (127, "step: stopped\n"),
        ):
            code, output, called = scratch.run(topics.replace(f"{HELPER_CALL} topics", f'sh -c "exit {status}"'))
            case(
                f"a block whose command ends with {status} exits {status} and prints {expected.strip() or 'nothing'}",
                code == status and scratch.raw == expected,
            )
        code, output, called = scratch.run(topics.replace(f"{HELPER_CALL} topics", 'echo "$UNSET_NAME"; echo reached'))
        sink.pop()
        case("a block runs under set -u: an unset variable stops it before the next command", code != 0 and "reached" not in output)

    # An empty ALERT_EMAIL has nothing to subscribe.
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = Scratch(Path(folder), sink, address="", helper=helper)
        code, output, called = scratch.run(verify)
        case(
            "an empty ALERT_EMAIL ends the verification with nothing to do and no AWS call",
            code == 0 and output == "address: empty\n" and not first_call(called, "aws "),
        )
    return checks, calls


def teardown_cases(section: str, sink: list[str], helper: str) -> tuple[dict[str, bool], list[str]]:
    checks: dict[str, bool] = {}
    calls: list[str] = []
    state = pick(section, " state")
    plan = pick(section, "plan alerts --destroy")
    apply = pick(section, "apply alerts --destroy")
    verify = pick(section, "subscriptions --expect none")

    def case(label: str, ok: bool) -> None:
        checks[f"alert teardown run: {label}"] = bool(ok)

    def managed(scratch: Scratch, alerts: str = "confirmed", budget: str = "confirmed") -> None:
        scratch.own(world.ALERTS_ADDRESS, scratch.subscribe(world.ALERTS_TOPIC, alerts, "7"))
        scratch.own(world.BUDGET_ADDRESS, scratch.subscribe(world.BUDGET_TOPIC, budget, "8"))

    for budget_status in ("confirmed", "pending"):
        with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
            scratch = Scratch(Path(folder), sink, helper=helper)
            managed(scratch, budget=budget_status)
            label = f"two managed instances and a {budget_status} budget subscription"
            code, output, called = scratch.run(state)
            calls += called
            case(
                f"{label}: the state block reports both subscriptions in the state",
                code == 0 and output == "alerts subscription: present\nbudget subscription: present\n",
            )
            code, output, called = scratch.run(plan)
            calls += called
            case(
                f"{label}: the destroy plan holds both subscriptions", code == 0 and output == "alerts topic: planned\nbudget topic: planned\nalerts: planned\n"
            )
            code, output, called = scratch.run(apply)
            calls += called
            case(f"{label}: the project user destroys both subscriptions", code == 0 and output == "alerts: destroyed\n" and scratch.read()["tf"] == [])
            code, output, called = scratch.run(verify)
            calls += called
            case(f"{label}: nothing is left", code == 0 and output == "alerts topic: none\nbudget topic: none\n")
    # A subscription outside the state stays: confirmed stops the check, pending reports exit code 3.
    for left, expected_code in (("confirmed", 1), ("pending", 3)):
        with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
            scratch = Scratch(Path(folder), sink, helper=helper)
            scratch.own(world.ALERTS_ADDRESS, scratch.subscribe(world.ALERTS_TOPIC, "confirmed", "7"))
            scratch.subscribe(world.BUDGET_TOPIC, left, "9")
            scratch.run(plan)
            scratch.run(apply)
            code, output, called = scratch.run(verify)
            calls += called
            case(
                f"a {left} subscription that the state does not hold stays: exit code {expected_code}",
                code == expected_code and output == f"alerts topic: none\nbudget topic: {left}\n",
            )
    # The state reads.
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = Scratch(Path(folder), sink, helper=helper)
        code, output, called = scratch.run(state)
        case(
            "a stack without a state file reports an absent state and succeeds",
            code == 0 and output == "state: absent\n" and first_call(called, "state pull") and not first_call(called, "state list"),
        )
        code, output, called = scratch.run(plan)
        calls += called
        case("a destroy plan without a state has nothing to do", code == 0 and output == "alerts: nothing to do\n")
    # The state is read through Terraform, so a remote state without a local file counts and an empty local file is no state.
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = Scratch(Path(folder), sink, helper=helper)
        managed(scratch)
        (scratch.root / "infra/alerts/terraform.tfstate").unlink()
        scratch.set(remote_state=True)
        code, output, called = scratch.run(state)
        calls += called
        case(
            "a remote state without a local state file reports both subscriptions in the state",
            code == 0 and output == "alerts subscription: present\nbudget subscription: present\n",
        )
        code, output, called = scratch.run(plan)
        calls += called
        case(
            "a remote state without a local state file gives a destroy plan with both subscriptions",
            code == 0 and output == "alerts topic: planned\nbudget topic: planned\nalerts: planned\n",
        )
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = Scratch(Path(folder), sink, helper=helper)
        (scratch.root / "infra/alerts/terraform.tfstate").write_text("")
        code, output, called = scratch.run(state)
        calls += called
        case("an empty local state file with no remote state reports an absent state and succeeds", code == 0 and output == "state: absent\n")
        code, output, called = scratch.run(plan)
        calls += called
        case("an empty local state file gives a destroy plan with nothing to do", code == 0 and output == "alerts: nothing to do\n")
    # A backend with no state object: Terraform prints an empty state from pull (serial 0, no lineage) and fails state list. That is no state.
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = Scratch(Path(folder), sink, helper=helper)
        scratch.set(missing_remote_object=True)
        code, output, called = scratch.run(state)
        calls += called
        case("a remote backend with no state object reports an absent state and succeeds", code == 0 and output == "state: absent\n")
        code, output, called = scratch.run(plan)
        calls += called
        case("a remote backend with no state object gives a destroy plan with nothing to do", code == 0 and output == "alerts: nothing to do\n")
    # A persisted state that holds no resources is a state, not an absent one.
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = Scratch(Path(folder), sink, helper=helper)
        scratch.set(zero_resource_state=True)
        code, output, called = scratch.run(state)
        calls += called
        case(
            "a persisted state with no resources reports both subscriptions absent from it",
            code == 0 and output == "alerts subscription: absent\nbudget subscription: absent\n",
        )
    for label, extra, words in (
        ("a state that cannot be pulled", {"fail_state_pull": True}, ("state: failed\n",)),
        ("a state pull that is not a state document", {"pull_raw": "not json"}, ("state: unexpected\n",)),
    ):
        for remote in (False, True):
            with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
                scratch = Scratch(Path(folder), sink, helper=helper)
                managed(scratch)
                if remote:
                    (scratch.root / "infra/alerts/terraform.tfstate").unlink()
                scratch.set(remote_state=remote, **extra)
                code, output, called = scratch.run(state)
                calls += called
                kind = "remote" if remote else "local"
                case(f"{label} ({kind}) stops the state block with a status word", code == 1 and output in words)
                code, output, called = scratch.run(plan)
                case(f"{label} ({kind}) stops the destroy plan", code == 1 and len(scratch.read()["tf"]) == 2)
    for label, extra in (
        ("a failed state read (invalid state file)", {"fail_state_list": True}),
        ("a state that lists a resource with an address as its key", {"state_extra": 'aws_sns_topic_subscription.email["reviewer@example.org"]'}),
        ("a state that lists a resource with a subscription ARN as its key", {"state_extra": f'aws_sns_topic_subscription.email["{world.ALERTS_TOPIC}:0"]'}),
    ):
        with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
            scratch = Scratch(Path(folder), sink, helper=helper)
            managed(scratch)
            scratch.set(**extra)
            code, output, called = scratch.run(state)
            calls += called
            case(f"{label} stops the state block with a status word and prints no key", code == 1 and output in ("state: failed\n", "state: unexpected\n"))
            code, output, called = scratch.run(plan)
            case(f"{label} stops the destroy plan before it is saved as good", code == 1 and len(scratch.read()["tf"]) == 2)
            case(
                f"{label} leaves the destroy apply without a plan",
                scratch.run(apply)[1] in ("alerts: missing\n", "alerts: unexpected\n", "state: failed\n", "state: unexpected\n"),
            )
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = Scratch(Path(folder), sink, helper=helper)
        managed(scratch)
        scratch.set(plan_extra="aws_sns_topic.audit")
        code, output, called = scratch.run(plan)
        calls += called
        case("a destroy plan that deletes another resource stops with a status word", code == 1 and output == "alerts: unexpected\n")
        case("and it is not applied", scratch.run(apply)[0] == 1 and len(scratch.read()["tf"]) == 2)
    return checks, calls


def cases(helper: str = HELPER_CALL) -> tuple[dict[str, bool], list[str]]:
    """The checks of both procedures and every stand-in command the scripts ran, for the parser checks of the caller."""
    bootstrap, teardown = "alert_email_bootstrap", "alert_email_teardown"
    checks = static_checks({"bootstrap": bootstrap, "teardown": teardown})
    results: dict[str, bool] = {}
    calls: list[str] = []
    sink: list[str] = []
    FAILED_RUNS.clear()
    for name, function, section in (("bootstrap", bootstrap_cases, bootstrap), ("teardown", teardown_cases, teardown)):
        try:
            found, called = function(section, sink, helper)
        except (KeyError, IndexError, ValueError, OSError) as error:
            results[f"alert {name} run: the cases ran to the end ({type(error).__name__}: {error})"] = False
            continue
        results |= found
        calls += called
    offenders = sorted({line[:40] for output in sink for line in leaks(output)})
    results[f"alert block run: every run of every block ({len(sink)} runs) printed only <subject>: <status> lines of the closed vocabulary ({offenders})"] = (
        len(sink) >= 60 and not offenders
    )
    unmarked = sorted({(status, last) for status, marked, last in FAILED_RUNS if not marked})
    results[f"alert block run: every failed block run ({len(FAILED_RUNS)} runs) ends with the fixed ERR trap line (statuses without it: {unmarked})"] = (
        len(FAILED_RUNS) >= 20 and not unmarked
    )
    return checks | results, calls


BUDGET_READ = 'budget = read_json(Subject.TOPICS, run(Subject.TOPICS, "terraform", ["-chdir=infra/iam", "output", "-json", "budget"]))'
# Planted defects: a change to the helper's source, run through the same delivered blocks. Each must fail at least one case.
MUTANTS = (
    (
        "a failed command is ignored",
        "    if result.returncode != 0:\n        raise Stop(subject, Status.FAILED)\n",
        "    if False:\n        raise Stop(subject, Status.FAILED)\n",
    ),
    ("the platform topic output is not compared with .env", "    if alerts != config.alerts_topic:\n", "    if False:\n"),
    (
        "the platform topic output is compared only after the budget read",
        "    if alerts != config.alerts_topic:\n        raise Stop(Subject.TOPICS, Status.UNEXPECTED)\n"
        + f"    {BUDGET_READ}\n    if not isinstance(budget, dict) or",
        f"    {BUDGET_READ}\n    if alerts != config.alerts_topic or not isinstance(budget, dict) or",
    ),
    (
        "the plan envelope is not checked",
        "    if not isinstance(version, str) or not FORMAT_VERSION.fullmatch(version) or not isinstance(tool, str) or not tool:",
        "    if False:",
    ),
    ("the planned values and configuration are not checked", '    for member in ("planned_values", "configuration"):', "    for member in ():"),
    ("a plan that errored is accepted", '    if document.get("errored", False) is not False or not', "    if not"),
    ("a duplicate resource address is accepted", "address in seen or ", ""),
    (
        "the parser prints its own error",
        "    def error(self, message: str) -> NoReturn:\n        raise Stop(Subject.STEP, Status.UNEXPECTED)",
        "    def error(self, message: str) -> NoReturn:\n        super().error(message)",
    ),
    (
        "the listing container type is not checked",
        "    if not isinstance(rows, list):\n        raise Stop(subject, Status.UNEXPECTED)\n",
        "    if False:\n        raise Stop(subject, Status.UNEXPECTED)\n",
    ),
    (
        "a state resource outside the two subscriptions is accepted",
        "    if not set(listed) <= {ALERTS_ADDRESS, BUDGET_ADDRESS} or",
        "    if False and not set(listed) <= {ALERTS_ADDRESS, BUDGET_ADDRESS} or",
    ),
    ("the digest is masked before it is stored", "DIGEST_FILE.write_text(digests[0] + ", "DIGEST_FILE.write_text(MASK.sub('************', digests[0]) + "),
    (
        "the plan is not checked before it is applied",
        '    check_plan(stack, destroy, read_plan(stack, plan, subject), subject)\n    wrapper(subject, stack, "apply"',
        '    wrapper(subject, stack, "apply"',
    ),
    (
        "the output of a command is kept in the log",
        '    note(f"{identity} exit {result.returncode}")\n',
        '    note(f"{identity} exit {result.returncode}")\n    note(result.stdout + result.stderr)\n',
    ),
    (
        "the command output is printed",
        '    note(f"{identity} exit {result.returncode}")\n',
        '    sys.stdout.write(result.stdout)\n    note(f"{identity} exit {result.returncode}")\n',
    ),
    ("a policy step for another policy is accepted", "    if not all(POLICY_STEP.fullmatch(line) for line in steps):", "    if False:"),
)


def mutant_checks() -> dict[str, bool]:
    """Each planted defect in the helper must fail at least one case, so the cases test the helper and not themselves."""
    checks: dict[str, bool] = {}
    source = HELPER_SOURCE.read_text(encoding="utf-8")
    with tempfile.TemporaryDirectory(prefix="alert_mutant_") as folder:
        for number, (label, old, new) in enumerate(MUTANTS):
            if old not in source:
                checks[f"alert helper mutant {label}: the planted text exists in the helper"] = False
                continue
            mutant = Path(folder) / f"mutant_{number}.py"
            mutant.write_text(source.replace(old, new, 1), encoding="utf-8")
            results, _ = cases(helper=f".venv/bin/python {mutant}")
            checks[f"alert helper mutant {label} is caught"] = not all(results.values())
    return checks
