"""Run the delivered alert email owner blocks as written, against stand-in tools, and check what each one does.

Every ``sh`` block of the bootstrap and teardown lists must run as pasted: no placeholder, no value filled in by hand, a ``bash`` heredoc that stops at the
first failure and prints no account number. The cases write the block text to a file in a scratch repository root and run it with ``bash``. The scratch root
has a ``.env``, the stack folders and a stand-in ``aws``, ``terraform``, wrapper and ``.venv/bin/python`` first on the PATH (``owner_list_stand_ins``). Only
the terminal device is replaced: the block reads the confirmation token from ``/dev/tty``, so the cases point that one path at a file. A run of the unchanged
block starts in a new session without a terminal and must stop before any AWS call.
"""

from __future__ import annotations

import json
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

from scripts.infrastructure import owner_commands
from scripts.infrastructure import owner_list_stand_ins as world
from scripts.process import CompletedProcess, run_command

STAND_INS = Path(world.__file__).resolve()
ADDRESS = "owner@example.org"
OTHER_ADDRESS = "someone@example.org"
TYPED = "0123456789abcdef0123456789abcdef"
PLACEHOLDER = re.compile(r"<[A-Z][A-Z0-9_]*>")
ACCOUNT_DIGITS = re.compile(r"\d{12}")
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


def fences(section: str) -> list[str]:
    return re.findall(r"```sh\n(.*?)```", section, re.DOTALL)


def pick(section: str, *needles: str) -> str:
    """The one block of the section that holds every needle."""
    found = [block for block in fences(section) if all(needle in block for needle in needles)]
    return found[0] if found else ""


class Scratch:
    """A scratch repository root with the stand-in tools and a world that a case sets up."""

    def __init__(self, root: Path, address: str | None = ADDRESS) -> None:
        self.root = root
        for name, body in SHIMS.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"#!/bin/sh\n{body}\n")
            path.chmod(0o755)
        (root / "infra/alerts").mkdir(parents=True)
        (root / ".env").write_text(f"ALERT_EMAIL={address or ''}\n")
        (root / "tty_in").write_text(f"{TYPED}\n")
        (root / "tty_out").write_text("")
        self.state_file = root / "state.json"
        self.state: dict[str, Any] = {
            "address": ADDRESS,
            "aws": {},
            "tf": [],
            "tf_arn": {},
            "calls": [],
            "policy_applied": False,
        }
        self.write()

    def write(self) -> None:
        self.state_file.write_text(json.dumps(self.state))

    def read(self) -> dict[str, Any]:
        self.state = json.loads(self.state_file.read_text())
        return self.state

    def subscribe(self, topic: str, status: str, authenticated: bool, name: str = "1", endpoint: str = ADDRESS) -> str:
        arn = f"{topic}:00000000-0000-0000-0000-{name:0>12}"
        self.read()
        self.state["aws"].setdefault(topic, []).append({"arn": arn, "endpoint": endpoint, "status": status, "authenticated": authenticated})
        self.write()
        return arn

    def own(self, address: str, arn: str) -> None:
        self.read()
        self.state["tf"].append(address)
        self.state["tf_arn"][address] = arn
        self.write()

    def set(self, **values: object) -> None:
        self.read()
        self.state.update(values)
        self.write()

    def run(self, block: str, *, tty: bool = True, admin: bool = True, timeout: int = 60) -> tuple[int, str, list[str]]:
        """Run the block text as delivered. With ``tty`` the terminal path of the token read points at the scratch files; without it the block runs as it is."""
        text = block
        if tty:
            text = text.replace("</dev/tty", '<"$FAKE_TTY_IN"').replace(">/dev/tty", '>>"$FAKE_TTY_OUT"')
        file = self.root / "block.sh"
        file.write_text(text)
        before = len(self.read()["calls"])
        environment = {
            "PATH": f"{self.root / 'bin'}:/usr/bin:/bin:/usr/sbin:/sbin",
            "HOME": str(self.root),
            "STAND_IN_STATE": str(self.state_file),
            "STAND_IN_PYTHON": sys.executable,
            "STAND_IN_SCRIPT": str(STAND_INS),
            "FAKE_TTY_IN": str(self.root / "tty_in"),
            "FAKE_TTY_OUT": str(self.root / "tty_out"),
        }
        if admin:
            environment["ADMIN_PROFILE"] = "example_admin"
        environment["BLOCK_FILE"] = str(file)
        result = run_command("bash", ["-c", 'bash "$BLOCK_FILE"'], cwd=self.root, env=environment, timeout=timeout, own_session=True, errors="replace")
        calls = self.read()["calls"][before:]
        return result.returncode, result.stdout + result.stderr, calls

    def tty_out(self) -> str:
        return (self.root / "tty_out").read_text()


def clean(output: str, *, digits: bool = True, arn: bool = False) -> bool:
    """The output holds no account number, no address, no token and, when asked, no ARN."""
    leaked = ADDRESS in output or TYPED in output or (digits and ACCOUNT_DIGITS.search(output)) or (arn and "arn:aws" in output)
    return not leaked


def syntax_check(file: Path) -> CompletedProcess[str]:
    """``bash -n`` on the file: the shell parses it and runs nothing."""
    return run_command("bash", ["-c", 'bash -n "$BLOCK_FILE"'], env={"PATH": "/usr/bin:/bin", "BLOCK_FILE": str(file)}, timeout=30)


def static_checks(sections: dict[str, str]) -> dict[str, bool]:
    """Every block is a complete, placeholder-free bash heredoc with the failure handling and the number mask."""
    checks: dict[str, bool] = {}
    blocks = [(name, block) for name, section in sections.items() for block in fences(section)]
    checks["the alert email lists hold shell blocks"] = len(blocks) >= 19
    with tempfile.TemporaryDirectory(prefix="alert_blocks_") as folder:
        for number, (name, block) in enumerate(blocks, 1):
            label = f"{name} block {number}"
            file = Path(folder) / f"block_{number}.sh"
            file.write_text(block)
            syntax = syntax_check(file)
            inner = block.split("<<'EOF'\n", 1)[1].rsplit("EOF\n", 1)[0] if "<<'EOF'\n" in block else ""
            (Path(folder) / f"inner_{number}.sh").write_text(inner)
            inner_syntax = syntax_check(Path(folder) / f"inner_{number}.sh")
            wrapped = block.startswith("bash <<'EOF'\n") and block.endswith("\nEOF\n")
            handled = "set -Eeuo pipefail" in block and "trap '" in block and " ERR" in block and "mask() { sed -E 's/[0-9]{12}/************/g'; }" in block
            checks[f"{label} runs as pasted: a bash heredoc with failure handling and the account mask, no placeholder, valid syntax"] = (
                wrapped and handled and not PLACEHOLDER.search(block) and syntax.returncode == 0 and inner_syntax.returncode == 0 and "| mask" in block
            )
    return checks


def bootstrap_cases(section: str) -> tuple[dict[str, bool], list[str]]:
    checks: dict[str, bool] = {}
    calls: list[str] = []
    policy_plan = pick(section, "manage_policies plan")
    policy_apply = pick(section, "manage_policies apply")
    platform_plan = pick(section, "--platform plan")
    platform_apply = pick(section, "--platform apply")
    budget_topic = pick(section, "output -json budget", "reports the budget alerts topic")
    status = pick(section, "a confirmed subscription that was not confirmed with authentication")
    removal = pick(section, "aws sns unsubscribe")
    prepare = pick(section, "init -input=false -lockfile=readonly")
    importing = pick(section, "terraform -chdir=infra/alerts import")
    alerts_plan = pick(section, "--alerts plan")
    alerts_apply = pick(section, "--alerts apply")
    confirm = pick(section, "aws sns confirm-subscription")
    verify = pick(section, "Both alert subscriptions are confirmed")

    def case(label: str, ok: bool) -> None:
        checks[f"alert block run: {label}"] = bool(ok)

    # The whole first bootstrap, block by block, from nothing in AWS to two confirmed subscriptions.
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = Scratch(Path(folder))
        code, output, called = scratch.run(policy_apply)
        calls += called
        case("a policy apply without a saved plan stops and applies nothing", code != 0 and not scratch.read()["policy_applied"] and "Step failed" in output)
        code, output, called = scratch.run(policy_plan, admin=False)
        calls += called
        case("a block without ADMIN_PROFILE stops before any command", code != 0 and not called and "ADMIN_PROFILE" in output)
        code, output, called = scratch.run(policy_plan)
        calls += called
        digest = (scratch.root / "plans/alert_email/policy_plan.txt").read_text() if (scratch.root / "plans/alert_email/policy_plan.txt").is_file() else ""
        case(
            "the policy plan prints the digest and saves it without an account number",
            code == 0 and "Plan digest:" in output and clean(output) and clean(digest),
        )
        code, output, called = scratch.run(policy_apply)
        calls += called
        case("the policy apply uses the digest of the saved plan", code == 0 and scratch.read()["policy_applied"] and clean(output))
        for label, block in (
            ("the platform plan", platform_plan),
            ("the platform apply", platform_apply),
            ("the budget topic check", budget_topic),
            ("the alerts root preparation", prepare),
        ):
            code, output, called = scratch.run(block)
            calls += called
            case(f"{label} runs as pasted and prints no account number", code == 0 and clean(output))
        code, output, called = scratch.run(status)
        calls += called
        case(
            "the adoption status block reports no hand subscription and prints no value",
            code == 0 and "no subscription of the address" in output and clean(output, arn=True),
        )
        code, output, called = scratch.run(importing)
        calls += called
        case("the import block imports nothing when there is nothing to adopt", code == 0 and not scratch.read()["tf"] and clean(output, arn=True))
        code, output, called = scratch.run(alerts_plan)
        calls += called
        case("the alerts plan adds both subscriptions", code == 0 and output.count("will be created") == 2 and clean(output))
        code, output, called = scratch.run(alerts_apply)
        calls += called
        case("the alerts apply creates both subscriptions as pending", code == 0 and len(scratch.read()["tf"]) == 2 and clean(output))
        code, output, called = scratch.run(verify)
        case("the verification stops while a subscription is pending", code != 0 and "PendingConfirmation" in output)
        # The owner clicks the link of the alerts email: that subscription is confirmed without authentication, which the project user may remove.
        scratch.read()
        for row in scratch.state["aws"][world.ALERTS_TOPIC]:
            row["status"] = "confirmed"
        scratch.write()
        code, output, called = scratch.run(confirm)
        calls += called
        confirmed = [row for row in scratch.read()["aws"][world.BUDGET_TOPIC] if row["status"] == "confirmed" and row["authenticated"]]
        case(
            "the confirmation block confirms the budget subscription with authentication and prints a status only",
            code == 0 and len(confirmed) == 1 and "confirmed with authentication" in output and clean(output, arn=True) and TYPED not in scratch.tty_out(),
        )
        confirm_calls = [item for item in called if "confirm-subscription" in item]
        attribute_calls = [item for item in called if "get-subscription-attributes" in item]
        case(
            "the attribute is read from the ARN that the confirmation returned",
            len(confirm_calls) == 1 and bool(attribute_calls) and attribute_calls[-1].split("--subscription-arn ")[1].split(" ")[0] == confirmed[0]["arn"],
        )
        case(
            "the confirmation call carries the authenticate flag and the typed token",
            "--authenticate-on-unsubscribe true" in confirm_calls[0] and TYPED in confirm_calls[0],
        )
        code, output, called = scratch.run(verify)
        case("the verification passes with both subscriptions confirmed", code == 0 and clean(output, arn=True))
        code, output, called = scratch.run(confirm, tty=False)
        calls += called
        case(
            "a repeat without a terminal skips the confirmation, as the subscription is already authenticated",
            code == 0 and not [c for c in called if "confirm-subscription" in c],
        )

    # A failed confirmation must end the block, whatever the attribute of another subscription says.
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = Scratch(Path(folder))
        scratch.subscribe(world.BUDGET_TOPIC, "pending", False, "2")
        (scratch.root / "tty_in").write_text("expired-token\n")
        scratch.subscribe(world.ALERTS_TOPIC, "confirmed", True, "9")
        code, output, called = scratch.run(confirm)
        calls += called
        case(
            "a failed confirmation (expired token) ends the block with no attribute read and no success text",
            code != 0
            and not [c for c in called if "get-subscription-attributes" in c]
            and "confirmed with authentication" not in output
            and "true" not in output.replace("authenticate", "")
            and "Step failed" in output
            and clean(output, arn=True),
        )
        (scratch.root / "tty_in").write_text(f"{TYPED}\n")
        scratch.set(confirm_ignores_flag=True)
        code, output, called = scratch.run(confirm)
        calls += called
        case(
            "a confirmation that leaves ConfirmationWasAuthenticated false ends the block",
            code != 0 and "confirmed with authentication" not in output.replace("is confirmed but", ""),
        )
        (scratch.root / "tty_in").write_text("\n")
        scratch.set(confirm_ignores_flag=False, aws={world.BUDGET_TOPIC: []})
        scratch.subscribe(world.BUDGET_TOPIC, "pending", False, "2")
        code, output, called = scratch.run(confirm)
        case("an empty token ends the block before the confirmation", code != 0 and not [c for c in called if "confirm-subscription" in c])
        code, output, called = scratch.run(confirm, tty=False)
        case("the unchanged block without a terminal stops before the confirmation call", code != 0 and not [c for c in called if "confirm-subscription" in c])

    # A hand budget subscription confirmed from the link has no authentication: it is not adopted, it is removed, and a new one is confirmed.
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = Scratch(Path(folder))
        hand_alerts = scratch.subscribe(world.ALERTS_TOPIC, "confirmed", False, "3")
        scratch.subscribe(world.BUDGET_TOPIC, "confirmed", False, "4")
        code, output, called = scratch.run(status)
        calls += called
        case(
            "the status block flags a hand budget subscription without authentication",
            code == 0 and "was not confirmed with authentication" in output and clean(output, arn=True),
        )
        code, output, called = scratch.run(importing)
        calls += called
        case(
            "the import block adopts the confirmed alerts subscription and leaves the unauthenticated budget one",
            code == 0
            and scratch.read()["tf"] == [world.ALERTS_ADDRESS]
            and scratch.state["tf_arn"] == {world.ALERTS_ADDRESS: hand_alerts}
            and clean(output, arn=True),
        )
        code, output, called = scratch.run(importing)
        calls += called
        case(
            "a second import run reports the resource as managed and changes nothing",
            code == 0 and "already managed" in output and scratch.read()["tf"] == [world.ALERTS_ADDRESS],
        )
        code, output, called = scratch.run(confirm)
        case(
            "the confirmation block refuses a confirmed budget subscription without authentication",
            code != 0 and not [c for c in called if "confirm-subscription" in c],
        )
        code, output, called = scratch.run(removal)
        calls += called
        case(
            "the removal block removes it with the administrator and says so without a value",
            code == 0 and not scratch.read()["aws"][world.BUDGET_TOPIC] and clean(output, arn=True),
        )
        code, output, called = scratch.run(alerts_plan)
        case("the alerts plan then adds the budget subscription only", code == 0 and output.count("will be created") == 1 and "budget" in output)

    # A hand budget subscription that was confirmed with authentication is adopted and needs no new confirmation.
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = Scratch(Path(folder))
        scratch.subscribe(world.BUDGET_TOPIC, "confirmed", True, "5")
        code, output, called = scratch.run(importing)
        calls += called
        case("the import block adopts a hand budget subscription confirmed with authentication", code == 0 and scratch.read()["tf"] == [world.BUDGET_ADDRESS])
        code, output, called = scratch.run(removal)
        case("the removal block keeps a subscription that was confirmed with authentication", code == 0 and len(scratch.read()["aws"][world.BUDGET_TOPIC]) == 1)
        # A subscription of another address is never touched.
        scratch.subscribe(world.ALERTS_TOPIC, "confirmed", False, "6", endpoint=OTHER_ADDRESS)
        code, output, called = scratch.run(status)
        case("a subscription of another address is not reported as the owner's", code == 0 and "alerts topic: no subscription of the address" in output)

    # An empty ALERT_EMAIL has nothing to subscribe.
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = Scratch(Path(folder), address=None)
        code, output, called = scratch.run(status)
        case(
            "an empty ALERT_EMAIL ends a block with nothing to do and no AWS call",
            code == 0 and "ALERT_EMAIL is empty" in output and not [c for c in called if c.startswith("aws")],
        )
    return checks, calls


def teardown_cases(section: str) -> tuple[dict[str, bool], list[str]]:
    checks: dict[str, bool] = {}
    calls: list[str] = []
    state_list = pick(section, "state list")
    removal = pick(section, "aws sns unsubscribe")
    drop = pick(section, "state rm")
    plan = pick(section, "plan -destroy")
    apply = pick(section, "subscriptions are destroyed")
    verify = pick(section, "The teardown is not complete")

    def case(label: str, ok: bool) -> None:
        checks[f"alert teardown run: {label}"] = bool(ok)

    for budget_status in ("confirmed", "pending"):
        with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
            scratch = Scratch(Path(folder))
            alerts = scratch.subscribe(world.ALERTS_TOPIC, "confirmed", False, "7")
            budget = scratch.subscribe(world.BUDGET_TOPIC, budget_status, budget_status == "confirmed", "8")
            scratch.own(world.ALERTS_ADDRESS, alerts)
            scratch.own(world.BUDGET_ADDRESS, budget)
            label = f"two managed instances and a {budget_status} budget subscription"
            code, output, called = scratch.run(state_list)
            calls += called
            case(f"{label}: the state block lists both instances", code == 0 and world.ALERTS_ADDRESS in output and world.BUDGET_ADDRESS in output)
            code, output, called = scratch.run(removal)
            calls += called
            held = scratch.read()["aws"][world.BUDGET_TOPIC]
            action = "removes the subscription" if budget_status == "confirmed" else "reports the pending row and removes nothing"
            case(
                f"{label}: the administrator block {action}",
                code == 0 and (not held if budget_status == "confirmed" else len(held) == 1 and "pending" in output) and clean(output, arn=True),
            )
            code, output, called = scratch.run(plan)
            plan_without_drop = output
            calls += called
            case(f"{label}: without the state step the destroy plan holds the budget entry, so the apply is denied", "budget" in plan_without_drop)
            code, output, called = scratch.run(apply)
            case(f"{label}: and that apply fails", code != 0 and "AccessDenied" in output and world.BUDGET_ADDRESS in scratch.read()["tf"])
            code, output, called = scratch.run(drop)
            calls += called
            case(f"{label}: the state block drops the budget instance", code == 0 and scratch.read()["tf"] == [world.ALERTS_ADDRESS])
            code, output, called = scratch.run(drop)
            case(f"{label}: a second run finds nothing to drop and succeeds", code == 0 and "nothing to drop" in output)
            code, output, called = scratch.run(plan)
            calls += called
            case(
                f"{label}: the destroy plan holds the platform alerts subscription only",
                code == 0 and output.count("will be destroyed") == 1 and "budget" not in output and clean(output),
            )
            code, output, called = scratch.run(apply)
            calls += called
            case(f"{label}: the apply succeeds and no instance remains", code == 0 and scratch.read()["tf"] == [])
            code, output, called = scratch.run(verify)
            calls += called
            if budget_status == "confirmed":
                case(
                    f"{label}: the verification finds no subscription left",
                    code == 0 and "No subscription of the address is left" in output and clean(output, arn=True),
                )
            else:
                case(
                    f"{label}: the verification reports the pending row and withholds completion with exit code 3",
                    code == 3 and "PendingConfirmation" in output and "not complete" in output,
                )
                scratch.set(aws={world.ALERTS_TOPIC: [], world.BUDGET_TOPIC: []})
                code, output, called = scratch.run(verify)
                case(f"{label}: a later run after the row expires reports completion", code == 0 and "No subscription of the address is left" in output)
    with tempfile.TemporaryDirectory(prefix="alert_run_") as folder:
        scratch = Scratch(Path(folder))
        scratch.subscribe(world.BUDGET_TOPIC, "confirmed", True, "9")
        code, output, called = scratch.run(verify)
        case("the verification fails while a confirmed subscription is left", code != 0 and "confirmed subscription is left" in output)
    return checks, calls


def cases(text: str) -> tuple[dict[str, bool], list[str]]:
    """The checks of both lists and every stand-in command the blocks ran, for the parser checks of the caller."""
    bootstrap = owner_commands.section(text, "Alert email at bootstrap")
    teardown = owner_commands.section(text, "Alert email teardown")
    checks = static_checks({"bootstrap": bootstrap, "teardown": teardown})
    results: dict[str, bool] = {}
    calls: list[str] = []
    for name, function, section in (("bootstrap", bootstrap_cases, bootstrap), ("teardown", teardown_cases, teardown)):
        try:
            found, called = function(section)
        except (KeyError, IndexError, ValueError, OSError) as error:
            results[f"alert {name} run: the cases ran to the end ({type(error).__name__}: {error})"] = False
            continue
        results |= found
        calls += called
    return checks | results, calls


MUTANTS = (
    ("the attribute check of the confirmed subscription is dropped", 'test "$(authenticated "$CONFIRMED")" = true ||', "true ||"),
    (
        "the attribute is read from another subscription than the one the confirmation returned",
        'test "$(authenticated "$CONFIRMED")" = true',
        f'test "$(authenticated "{world.ALERTS_TOPIC}:00000000-0000-0000-0000-000000000009")" = true',
    ),
    ("the confirmation ARN is printed", 'case "$CONFIRMED" in', 'printf \'%s\\n\' "$CONFIRMED"\n  case "$CONFIRMED" in'),
    ("the typed value is printed", "unset TOKEN", "printf '%s\\n' \"$TOKEN\"\n  unset TOKEN"),
    ("the account mask is removed", "run </dev/null 2>&1 | mask", "run </dev/null 2>&1"),
    (
        "a placeholder returns to the confirmation",
        '--profile "$ADMIN_PROFILE" --region "$REGION" --topic-arn "$BUDGET_TOPIC_ARN" --token',
        "--profile <ADMIN_PROFILE> --region <REGION> --topic-arn <BUDGET_TOPIC_ARN> --token",
    ),
    ("the state step skips the budget instance", 'if in_state "aws_sns_topic_subscription.email[\\"budget\\"]"; then', "if false; then"),
)


def mutant_checks(text: str) -> dict[str, bool]:
    """Each planted defect in one delivered block must fail at least one case, so the cases test the blocks and not themselves."""
    checks: dict[str, bool] = {}
    for label, old, new in MUTANTS:
        if old not in text:
            checks[f"alert block mutant {label}: the planted text exists in the lists"] = False
            continue
        results, _ = cases(text.replace(old, new, 1) if "mask" not in old else text.replace(old, new))
        checks[f"alert block mutant {label} is caught"] = not all(results.values())
    return checks
