"""The owner commands of the alert email, one subcommand per owner block.

The project user subscribes the address in ``.env`` to every alert topic that the project's Terraform creates (the platform alerts topic and the IAM stack's
budget alerts topic) from the alerts root, ``infra/alerts``. The owner clicks the confirmation link in each email. Teardown destroys the subscriptions with the
same project profile. Run it from the repository root: ``.venv/bin/python -m scripts.infrastructure.alert_email <command>``.

Every read is strict and typed. A command that fails, prints something the parser does not expect or returns a value of the wrong type stops with a status
word and exit code 1, and no later step runs. The only text a command prints is ``<subject>: <status>`` from two closed vocabularies (``Subject`` and
``Status``), so no ARN, topic name, account number, address or subscription identifier can reach the terminal. That holds for a bad argument too: the parser
reports a fixed status word and never the value. The ignored file ``plans/alert_email/last_run.log`` keeps only the identity of each command it runs (the
subject, the program name and a fixed category: the exit status, ``could not run`` or ``timed out``), never its output. A plan is read with
``terraform show -json``, its envelope and its addresses are checked, and it is checked again before it is applied.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, NoReturn

from scripts.infrastructure.render_project_config import ConfigurationError, load_alert_email, load_configuration
from scripts.process import TimeoutExpired, run_command

RECORD = Path("plans/alert_email")
LOG = RECORD / "last_run.log"
DIGEST_FILE = RECORD / "policy_digest.txt"
STATE_FILE = Path("infra/alerts/terraform.tfstate")
TIMEOUT_SECONDS = 600
EXIT_STOPPED = 1
EXIT_PENDING = 3
ADMIN_PROFILE = re.compile(r"[A-Za-z0-9_.@+=,-]{1,128}")
DIGEST_LINE = re.compile(r"^Plan digest: ([0-9a-f]{64})$", re.MULTILINE)
APPLIED_LINE = re.compile(r"^Applied \d+ step\(s\)\.$", re.MULTILINE)
POLICY_STEP = re.compile(r"^(CREATE|KEEP|UPDATE) POLICY {1,12}healthcare_hai_prediction_sns_policy(?: \(deletes versions? [v0-9, ]+\))?$")
STEP_WORDS = re.compile(r"^(CREATE|KEEP|UPDATE|RETIRE|ATTACH|DETACH|ADD) [A-Z]+ ")
SUBSCRIPTION_ID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
FORMAT_VERSION = re.compile(r"1\.\d+")
ALERTS_ADDRESS = 'aws_sns_topic_subscription.email["alerts"]'
BUDGET_ADDRESS = 'aws_sns_topic_subscription.email["budget"]'
# The stack folder, the wrapper option and the saved plan of each stack the owner blocks plan and apply.
STACKS = {"platform": "infra/platform", "alerts": "infra/alerts"}
WRAPPER_SCRIPT = (
    'plan_args=(-out "$WRAPPER_PLAN"); [ -z "${WRAPPER_DESTROY:-}" ] || plan_args=(-destroy -out "$WRAPPER_PLAN"); '
    'case "$WRAPPER_ACTION" in plan) exec bash scripts/infrastructure/terraform.sh "--$WRAPPER_STACK" plan "${plan_args[@]}" ;; '
    'apply) exec bash scripts/infrastructure/terraform.sh "--$WRAPPER_STACK" apply "$WRAPPER_PLAN" ;; esac'
)


class Subject(StrEnum):
    STEP = "step"
    ENVIRONMENT = "repository"
    ADDRESS = "address"
    ADMIN = "administrator profile"
    POLICY = "policy"
    DIGEST = "plan digest"
    PLATFORM = "platform"
    ALERTS = "alerts"
    TOPICS = "topics"
    ALERTS_TOPIC = "alerts topic"
    BUDGET_TOPIC = "budget topic"
    STATE = "state"
    ALERTS_SUBSCRIPTION = "alerts subscription"
    BUDGET_SUBSCRIPTION = "budget subscription"


class Status(StrEnum):
    READY = "ready"
    PLANNED = "planned"
    APPLIED = "applied"
    DESTROYED = "destroyed"
    SAVED = "saved"
    NOTHING_TO_DO = "nothing to do"
    NONE = "none"
    PENDING = "pending"
    CONFIRMED = "confirmed"
    PRESENT = "present"
    ABSENT = "absent"
    EMPTY = "empty"
    MISSING = "missing"
    FAILED = "failed"
    UNEXPECTED = "unexpected"
    STOPPED = "stopped"


class Stop(Exception):
    """A step that must not go on. It carries a subject and a status only, never text from a command."""

    def __init__(self, subject: Subject, status: Status) -> None:
        super().__init__(f"{subject.value}: {status.value}")
        self.subject = subject
        self.status = status


def say(subject: Subject, status: Status) -> None:
    sys.stdout.write(f"{subject.value}: {status.value}\n")


def note(text: str) -> None:
    """Append one line of command identity to the log: only fixed words, program names and exit statuses are written, and the file is for the owner only."""
    RECORD.mkdir(parents=True, exist_ok=True)
    with LOG.open("a", encoding="utf-8") as handle:
        handle.write(text.rstrip("\n") + "\n")
    LOG.chmod(0o600)


def run(subject: Subject, program: str, args: list[str], *, env: dict[str, str] | None = None) -> str:
    """Run one command and return its standard output; a failed command stops the step. The log gets the identity and the exit status, never the output."""
    environment = {**os.environ, **(env or {})}
    identity = f"{subject.value}: {Path(program).name}"
    try:
        result = run_command(program, args, env=environment, timeout=TIMEOUT_SECONDS)
    except TimeoutExpired:
        note(f"{identity} timed out")
        raise Stop(subject, Status.FAILED) from None
    except OSError:
        note(f"{identity} could not run")
        raise Stop(subject, Status.FAILED) from None
    note(f"{identity} exit {result.returncode}")
    if result.returncode != 0:
        raise Stop(subject, Status.FAILED)
    return result.stdout


@dataclass(frozen=True)
class Settings:
    """What ``.env`` says, validated: the project profile and region, the topic ARNs that Terraform must report, and the address."""

    profile: str
    region: str
    address: str
    alerts_topic: str
    budget_topic: str


def settings() -> Settings:
    if not Path(".env").is_file() or not Path("infra/alerts").is_dir():
        raise Stop(Subject.ENVIRONMENT, Status.MISSING)
    try:
        storage, _ = load_configuration(Path(".env"))
        address = load_alert_email(Path(".env")).get("alert_email", "")
    except ConfigurationError:
        raise Stop(Subject.ENVIRONMENT, Status.UNEXPECTED) from None
    prefix = f"arn:aws:sns:{storage['aws_region']}:{storage['expected_account_id']}:{storage['project_name']}"
    return Settings(
        profile=storage["aws_profile"],
        region=storage["aws_region"],
        address=address,
        alerts_topic=f"{prefix}_alerts_{storage['environment']}",
        budget_topic=f"{prefix}_budget_alerts_{storage['environment']}",
    )


def admin_profile(value: str) -> str:
    if not value:
        raise Stop(Subject.ADMIN, Status.MISSING)
    if not ADMIN_PROFILE.fullmatch(value):
        raise Stop(Subject.ADMIN, Status.UNEXPECTED)
    return value


def read_json(subject: Subject, text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        raise Stop(subject, Status.UNEXPECTED) from None


def topics(config: Settings) -> None:
    """Terraform reports exactly the two topic ARNs that .env names, as a string and as the ``alerts_topic`` field of the budget output."""
    alerts = run(Subject.TOPICS, "terraform", ["-chdir=infra/platform", "output", "-raw", "alerts_topic_arn"])
    if alerts != config.alerts_topic:
        raise Stop(Subject.TOPICS, Status.UNEXPECTED)
    budget = read_json(Subject.TOPICS, run(Subject.TOPICS, "terraform", ["-chdir=infra/iam", "output", "-json", "budget"]))
    if not isinstance(budget, dict) or budget.get("alerts_topic") != config.budget_topic:
        raise Stop(Subject.TOPICS, Status.UNEXPECTED)


def subscription_status(config: Settings, subject: Subject, topic: str) -> Status:
    """The state of the address on one topic from the project profile's own listing: none, pending or confirmed. Any other shape stops the step."""
    listing = read_json(
        subject,
        run(
            subject,
            "aws",
            ["sns", "list-subscriptions-by-topic", "--profile", config.profile, "--region", config.region, "--topic-arn", topic, "--output", "json"],
        ),
    )
    rows = listing.get("Subscriptions") if isinstance(listing, dict) else None
    if not isinstance(rows, list):
        raise Stop(subject, Status.UNEXPECTED)
    found: list[str] = []
    for row in rows:
        if not isinstance(row, dict) or not all(isinstance(row.get(key), str) for key in ("SubscriptionArn", "Protocol", "Endpoint")):
            raise Stop(subject, Status.UNEXPECTED)
        if row["Protocol"] == "email" and row["Endpoint"] == config.address:
            found.append(row["SubscriptionArn"])
    if not found:
        return Status.NONE
    if len(found) > 1:
        raise Stop(subject, Status.UNEXPECTED)
    if found[0] == "PendingConfirmation":
        return Status.PENDING
    if re.fullmatch(f"{re.escape(topic)}:{SUBSCRIPTION_ID}", found[0]):
        return Status.CONFIRMED
    raise Stop(subject, Status.UNEXPECTED)


def state_addresses() -> set[str] | None:
    """The resources of the alerts root state, or ``None`` when there is no state file. A resource other than the two subscriptions stops the step."""
    if not STATE_FILE.is_file():
        return None
    listed = run(Subject.STATE, "terraform", ["-chdir=infra/alerts", "state", "list"]).splitlines()
    if not set(listed) <= {ALERTS_ADDRESS, BUDGET_ADDRESS} or len(listed) != len(set(listed)):
        raise Stop(Subject.STATE, Status.UNEXPECTED)
    return set(listed)


def plan_entries(document: Any, subject: Subject) -> list[Any]:
    """The ``resource_changes`` of a supported plan document. The envelope that ``terraform show -json`` prints for every plan must be there: a format version
    1.x, the Terraform version, the planned values and the configuration, each with a root module, and no error. A plan without changes has no change list."""
    if not isinstance(document, dict):
        raise Stop(subject, Status.UNEXPECTED)
    version, tool = document.get("format_version"), document.get("terraform_version")
    if not isinstance(version, str) or not FORMAT_VERSION.fullmatch(version) or not isinstance(tool, str) or not tool:
        raise Stop(subject, Status.UNEXPECTED)
    for member in ("planned_values", "configuration"):
        section = document.get(member)
        if not isinstance(section, dict) or not isinstance(section.get("root_module"), dict):
            raise Stop(subject, Status.UNEXPECTED)
    entries = document.get("resource_changes", [])
    if document.get("errored", False) is not False or not isinstance(entries, list):
        raise Stop(subject, Status.UNEXPECTED)
    return entries


def read_plan(stack: str, plan: Path, subject: Subject) -> dict[str, tuple[str, ...]]:
    """The changes of a saved plan by resource address and actions, from ``terraform show -json``, the envelope and every type checked. A resource address that
    appears twice stops the step. No-op and read entries are left out."""
    raw = run(subject, "terraform", [f"-chdir={STACKS[stack]}", "show", "-json", str(plan.resolve())])
    changes: dict[str, tuple[str, ...]] = {}
    seen: set[str] = set()
    for entry in plan_entries(read_json(subject, raw), subject):
        change = entry.get("change") if isinstance(entry, dict) else None
        actions = change.get("actions") if isinstance(change, dict) else None
        address = entry.get("address") if isinstance(entry, dict) else None
        if not isinstance(address, str) or address in seen or not isinstance(actions, list) or not all(isinstance(action, str) for action in actions):
            raise Stop(subject, Status.UNEXPECTED)
        seen.add(address)
        if actions not in (["no-op"], ["read"]):
            changes[address] = tuple(actions)
    return changes


def check_plan(stack: str, destroy: bool, changes: dict[str, tuple[str, ...]], subject: Subject) -> None:
    """The alerts plan creates only the two subscriptions, or destroys exactly the subscriptions in the state. The platform plan deletes nothing."""
    if stack == "platform":
        if any("delete" in actions for actions in changes.values()):
            raise Stop(subject, Status.UNEXPECTED)
        return
    action = ("delete",) if destroy else ("create",)
    if not set(changes) <= {ALERTS_ADDRESS, BUDGET_ADDRESS} or any(actions != action for actions in changes.values()):
        raise Stop(subject, Status.UNEXPECTED)
    if destroy and set(changes) != (state_addresses() or set()):
        raise Stop(subject, Status.UNEXPECTED)


def plan_path(stack: str, destroy: bool) -> Path:
    return RECORD / f"{stack}{'_destroy' if destroy else ''}.tfplan"


def wrapper(subject: Subject, stack: str, action: str, plan: Path, destroy: bool) -> None:
    environment = {"WRAPPER_STACK": stack, "WRAPPER_ACTION": action, "WRAPPER_PLAN": str(plan.resolve())}
    if destroy:
        environment["WRAPPER_DESTROY"] = "1"
    run(subject, "bash", ["-c", WRAPPER_SCRIPT], env=environment)


def stack_subject(stack: str) -> Subject:
    return Subject.PLATFORM if stack == "platform" else Subject.ALERTS


def command_policy_plan(arguments: argparse.Namespace) -> int:
    profile = admin_profile(arguments.admin_profile)
    if profile == settings().profile:
        raise Stop(Subject.ADMIN, Status.UNEXPECTED)
    output = run(Subject.POLICY, ".venv/bin/python", ["-m", "infra.iam.scripts.manage_policies", "plan", "--admin-profile", profile, "--policy", "sns"])
    digests = DIGEST_LINE.findall(output)
    steps = [line for line in output.splitlines() if STEP_WORDS.match(line)]
    if len(digests) != 1:
        raise Stop(Subject.DIGEST, Status.UNEXPECTED)
    if not all(POLICY_STEP.fullmatch(line) for line in steps):
        raise Stop(Subject.POLICY, Status.UNEXPECTED)
    DIGEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    DIGEST_FILE.write_text(digests[0] + "\n", encoding="utf-8")
    DIGEST_FILE.chmod(0o600)
    say(Subject.DIGEST, Status.SAVED)
    say(Subject.POLICY, Status.PLANNED if any(not line.startswith("KEEP") for line in steps) else Status.NOTHING_TO_DO)
    return 0


def command_policy_apply(arguments: argparse.Namespace) -> int:
    profile = admin_profile(arguments.admin_profile)
    if profile == settings().profile:
        raise Stop(Subject.ADMIN, Status.UNEXPECTED)
    if not DIGEST_FILE.is_file():
        raise Stop(Subject.DIGEST, Status.MISSING)
    saved = DIGEST_FILE.read_text(encoding="utf-8")
    if not re.fullmatch(r"[0-9a-f]{64}\n?", saved):
        raise Stop(Subject.DIGEST, Status.UNEXPECTED)
    environment = {"CONFIRM_IAM_POLICIES": "apply-healthcare-hai-prediction-policies"}
    output = run(
        Subject.POLICY,
        ".venv/bin/python",
        ["-m", "infra.iam.scripts.manage_policies", "apply", "--admin-profile", profile, "--digest", saved.strip(), "--policy", "sns"],
        env=environment,
    )
    if len(APPLIED_LINE.findall(output)) != 1:
        raise Stop(Subject.POLICY, Status.UNEXPECTED)
    say(Subject.POLICY, Status.APPLIED)
    return 0


def command_prepare(arguments: argparse.Namespace) -> int:
    settings()
    run(Subject.ALERTS, "terraform", ["-chdir=infra/alerts", "init", "-input=false", "-lockfile=readonly"])
    run(Subject.ALERTS, ".venv/bin/python", ["-m", "scripts.infrastructure.render_platform_config", "--stack", "alerts"])
    say(Subject.ALERTS, Status.READY)
    return 0


def command_plan(arguments: argparse.Namespace) -> int:
    settings()
    stack, destroy = arguments.stack, arguments.destroy
    subject = stack_subject(stack)
    plan = plan_path(stack, destroy)
    plan.parent.mkdir(parents=True, exist_ok=True)
    wrapper(subject, stack, "plan", plan, destroy)
    changes = read_plan(stack, plan, subject)
    check_plan(stack, destroy, changes, subject)
    if stack == "alerts":
        for address, topic in ((ALERTS_ADDRESS, Subject.ALERTS_TOPIC), (BUDGET_ADDRESS, Subject.BUDGET_TOPIC)):
            if address in changes:
                say(topic, Status.PLANNED)
    say(subject, Status.PLANNED if changes else Status.NOTHING_TO_DO)
    return 0


def command_apply(arguments: argparse.Namespace) -> int:
    settings()
    stack, destroy = arguments.stack, arguments.destroy
    subject = stack_subject(stack)
    plan = plan_path(stack, destroy)
    if not plan.is_file():
        raise Stop(subject, Status.MISSING)
    check_plan(stack, destroy, read_plan(stack, plan, subject), subject)
    wrapper(subject, stack, "apply", plan, destroy)
    say(subject, Status.DESTROYED if destroy else Status.APPLIED)
    return 0


def command_topics(arguments: argparse.Namespace) -> int:
    topics(settings())
    say(Subject.ALERTS_TOPIC, Status.READY)
    say(Subject.BUDGET_TOPIC, Status.READY)
    return 0


def command_subscriptions(arguments: argparse.Namespace) -> int:
    config = settings()
    if not config.address:
        say(Subject.ADDRESS, Status.EMPTY)
        return 0
    topics(config)
    found = {
        Subject.ALERTS_TOPIC: subscription_status(config, Subject.ALERTS_TOPIC, config.alerts_topic),
        Subject.BUDGET_TOPIC: subscription_status(config, Subject.BUDGET_TOPIC, config.budget_topic),
    }
    for subject, status in found.items():
        say(subject, status)
    states = set(found.values())
    if arguments.expect == "any":
        return 0
    wanted = Status.CONFIRMED if arguments.expect == "confirmed" else Status.NONE
    if states == {wanted}:
        return 0
    wrong = Status.NONE if wanted == Status.CONFIRMED else Status.CONFIRMED
    return EXIT_STOPPED if wrong in states else EXIT_PENDING


def command_state(arguments: argparse.Namespace) -> int:
    settings()
    addresses = state_addresses()
    if addresses is None:
        say(Subject.STATE, Status.ABSENT)
        return 0
    for address, subject in ((ALERTS_ADDRESS, Subject.ALERTS_SUBSCRIPTION), (BUDGET_ADDRESS, Subject.BUDGET_SUBSCRIPTION)):
        say(subject, Status.PRESENT if address in addresses else Status.ABSENT)
    return 0


class QuietParser(argparse.ArgumentParser):
    """An argument parser whose error is the fixed status ``step: unexpected``: the message, which holds the value that failed, is never printed. Subparsers
    use the same class."""

    def error(self, message: str) -> NoReturn:
        raise Stop(Subject.STEP, Status.UNEXPECTED)


def parser() -> argparse.ArgumentParser:
    main = QuietParser(description=(__doc__ or "").splitlines()[0])
    commands = main.add_subparsers(dest="command", required=True)
    for name, function in (("policy-plan", command_policy_plan), ("policy-apply", command_policy_apply)):
        item = commands.add_parser(name)
        item.add_argument("--admin-profile", default="")
        item.set_defaults(function=function)
    for name, function in (("plan", command_plan), ("apply", command_apply)):
        item = commands.add_parser(name)
        item.add_argument("stack", choices=sorted(STACKS))
        item.add_argument("--destroy", action="store_true")
        item.set_defaults(function=function)
    item = commands.add_parser("subscriptions")
    item.add_argument("--expect", choices=("any", "confirmed", "none"), default="any")
    item.set_defaults(function=command_subscriptions)
    for name, function in (("prepare", command_prepare), ("topics", command_topics), ("state", command_state)):
        commands.add_parser(name).set_defaults(function=function)
    return main


def main(argv: list[str] | None = None) -> int:
    try:
        arguments = parser().parse_args(argv)
        return int(arguments.function(arguments))
    except Stop as stop:
        note(f"STOP {stop.subject.value}: {stop.status.value}")
        say(stop.subject, stop.status)
        return EXIT_STOPPED
    except (OSError, KeyError, ValueError, TypeError):
        note("STOP step: stopped")
        say(Subject.STEP, Status.STOPPED)
        return EXIT_STOPPED


if __name__ == "__main__":
    sys.exit(main())
