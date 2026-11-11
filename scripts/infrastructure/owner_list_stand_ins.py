"""Stand-in aws, terraform, wrapper and Python module commands for the alert email owner blocks. Never contacts AWS.

The owner list E2E puts a small script named ``aws``, ``terraform`` and ``.venv/bin/python`` first on the PATH of a scratch repository root and runs the
delivered blocks unchanged. Each script runs this file with the tool name. The state of the stand-in world is one JSON file (``STAND_IN_STATE``): the
subscriptions that AWS holds per topic, the resources of the alerts root state and the log of every call. A pending subscription shows ``PendingConfirmation``
in a listing, as SNS does. ConfirmSubscription with the token ``expired-token`` fails, and with any other token confirms the pending subscription of the
topic; it sets ConfirmationWasAuthenticated only when it gets ``--authenticate-on-unsubscribe true`` (and the world does not ignore the flag). The project
user cannot remove a subscription of the budget topic, so an apply whose plan deletes it fails.
"""

from __future__ import annotations

import json
import os
import shlex
import sys
from pathlib import Path
from typing import Any

ACCOUNT = "111111111111"
TOPIC_PREFIX = f"arn:aws:sns:us-west-2:{ACCOUNT}:example_project"
ALERTS_TOPIC = f"{TOPIC_PREFIX}_alerts_dev"
BUDGET_TOPIC = f"{TOPIC_PREFIX}_budget_alerts_dev"
ALERTS_ADDRESS = 'aws_sns_topic_subscription.email["alerts"]'
BUDGET_ADDRESS = 'aws_sns_topic_subscription.email["budget"]'
TOPICS = {ALERTS_ADDRESS: ALERTS_TOPIC, BUDGET_ADDRESS: BUDGET_TOPIC}
DIGEST = "ab" * 32
STALE_VALUE = "expired-token"


def load() -> dict[str, Any]:
    return json.loads(Path(os.environ["STAND_IN_STATE"]).read_text())


def save(state: dict[str, Any]) -> None:
    Path(os.environ["STAND_IN_STATE"]).write_text(json.dumps(state, indent=1))


def fail(message: str, code: int = 255) -> int:
    sys.stderr.write(message + "\n")
    return code


def option(arguments: list[str], name: str) -> str | None:
    return arguments[arguments.index(name) + 1] if name in arguments and arguments.index(name) + 1 < len(arguments) else None


def find(state: dict[str, Any], arn: str) -> tuple[str, dict[str, Any]] | None:
    return next(((topic, row) for topic, rows in state["aws"].items() for row in rows if row["arn"] == arn), None)


def aws(arguments: list[str], state: dict[str, Any]) -> int:
    if option(arguments, "--profile") is None or option(arguments, "--region") is None:
        return fail("the stand-in aws needs --profile and --region")
    if arguments[:2] == ["sns", "list-subscriptions-by-topic"]:
        topic = option(arguments, "--topic-arn") or ""
        rows = [
            {
                "SubscriptionArn": "PendingConfirmation" if row["status"] == "pending" else row["arn"],
                "Protocol": "email",
                "Endpoint": row["endpoint"],
                "TopicArn": topic,
            }
            for row in state["aws"].get(topic, [])
        ]
        sys.stdout.write(json.dumps({"Subscriptions": rows}) + "\n")
        return 0
    if arguments[:2] == ["sns", "get-subscription-attributes"]:
        found = find(state, option(arguments, "--subscription-arn") or "")
        if found is None:
            return fail("An error occurred (NotFound) when calling the GetSubscriptionAttributes operation: Subscription does not exist")
        sys.stdout.write("true\n" if found[1]["authenticated"] else "false\n")
        return 0
    if arguments[:2] == ["sns", "confirm-subscription"]:
        topic, token = option(arguments, "--topic-arn") or "", option(arguments, "--token") or ""
        pending = next((row for row in state["aws"].get(topic, []) if row["status"] == "pending"), None)
        if token == STALE_VALUE or pending is None:
            return fail("An error occurred (InvalidParameter) when calling the ConfirmSubscription operation: Invalid token")
        pending["status"] = "confirmed"
        pending["authenticated"] = option(arguments, "--authenticate-on-unsubscribe") == "true" and not state.get("confirm_ignores_flag")
        save(state)
        sys.stdout.write(pending["arn"] + "\n")
        return 0
    if arguments[:2] == ["sns", "unsubscribe"]:
        found = find(state, option(arguments, "--subscription-arn") or "")
        if found is None:
            return fail("An error occurred (NotFound) when calling the Unsubscribe operation: Subscription does not exist")
        state["aws"][found[0]].remove(found[1])
        save(state)
        return 0
    return fail(f"the stand-in aws does not know {' '.join(arguments[:2])}")


def terraform(arguments: list[str], state: dict[str, Any]) -> int:
    directory = next((item.removeprefix("-chdir=") for item in arguments if item.startswith("-chdir=")), "")
    rest = [item for item in arguments if not item.startswith("-chdir=")]
    if directory == "infra/platform" and rest == ["output", "-raw", "alerts_topic_arn"]:
        sys.stdout.write(ALERTS_TOPIC)
        return 0
    if directory == "infra/iam" and rest == ["output", "-json", "budget"]:
        sys.stdout.write(json.dumps({"alerts_topic": BUDGET_TOPIC, "name": "example_project_budget_dev"}) + "\n")
        return 0
    if directory != "infra/alerts":
        return fail(f"the stand-in terraform does not know {directory} {rest}", 1)
    if rest[:1] == ["init"]:
        sys.stdout.write("Terraform has been successfully initialized!\n")
        return 0
    if rest == ["state", "list"]:
        sys.stdout.write("".join(f"{item}\n" for item in state["tf"]))
        return 0
    if rest[:2] == ["state", "rm"]:
        if rest[2] not in state["tf"]:
            return fail("No matching objects found.", 1)
        state["tf"].remove(rest[2])
        save(state)
        sys.stdout.write(f"Removed {rest[2]}\nSuccessfully removed 1 resource instance(s).\n")
        return 0
    if rest[:1] == ["import"]:
        address, identifier = rest[1], rest[2]
        if address in state["tf"]:
            return fail("Error: Resource already managed by Terraform", 1)
        if find(state, identifier) is None:
            return fail("Error: Cannot import non-existent remote object", 1)
        state["tf"].append(address)
        state["tf_arn"][address] = identifier
        save(state)
        sys.stdout.write(f'{address}: Importing from ID "{identifier}"...\nImport successful!\n')
        return 0
    return fail(f"the stand-in terraform does not know {rest}", 1)


def wrapper(arguments: list[str], state: dict[str, Any]) -> int:
    stack = arguments[0].removeprefix("--")
    action, rest = arguments[1], arguments[2:]
    destroy = "-destroy" in rest
    path = Path(rest[rest.index("-out") + 1] if "-out" in rest else rest[-1])
    if stack == "platform":
        if action == "apply" and not path.is_file():
            return fail("no saved plan", 1)
        if action == "plan":
            path.write_text(json.dumps({"stack": stack}))
        sys.stdout.write("Plan: no changes to the platform stack.\n")
        return 0
    if stack != "alerts":
        return fail(f"the stand-in wrapper does not know {stack}", 1)
    if action == "plan":
        planned = [address for address in TOPICS if (address in state["tf"]) == destroy]
        path.write_text(json.dumps({"destroy": destroy, "planned": planned}))
        for address in planned:
            sys.stdout.write(f"  # {address} will be {'destroyed' if destroy else 'created'}\n  topic_arn = {TOPICS[address]}\n")
        sys.stdout.write(f"Plan: {len(planned)} to {'destroy' if destroy else 'add'}.\n")
        return 0
    if not path.is_file():
        return fail("no saved plan", 1)
    plan = json.loads(path.read_text())
    for address in plan["planned"]:
        topic = TOPICS[address]
        if plan["destroy"]:
            if address == BUDGET_ADDRESS:
                return fail(f"Error: AccessDenied: the project user is not authorized to perform sns:Unsubscribe on {topic}", 1)
            state["tf"].remove(address)
            state["aws"][topic] = [row for row in state["aws"].get(topic, []) if row["arn"] != state["tf_arn"].get(address)]
        else:
            arn = f"{topic}:01234567-89ab-cdef-0123-4567{len(state['calls']):08d}"
            state["aws"].setdefault(topic, []).append({"arn": arn, "endpoint": state["address"], "status": "pending", "authenticated": False})
            state["tf"].append(address)
            state["tf_arn"][address] = arn
    save(state)
    sys.stdout.write("Apply complete.\n")
    return 0


def python_module(arguments: list[str], state: dict[str, Any]) -> int:
    module, rest = arguments[1], arguments[2:]
    if module == "infra.iam.scripts.manage_policies":
        if option(rest, "--admin-profile") is None:
            return fail("--admin-profile is required", 2)
        if rest[0] == "plan":
            sys.stdout.write(f"sns policy: add two statements for {TOPIC_PREFIX}_alerts_dev\nPlan digest: {DIGEST}\n")
            return 0
        if os.environ.get("CONFIRM_IAM_POLICIES") != "apply-healthcare-hai-prediction-policies" or option(rest, "--digest") != DIGEST:
            return fail("the confirmation or the reviewed digest does not match", 2)
        state["policy_applied"] = True
        save(state)
        sys.stdout.write("Applied the sns policy.\n")
        return 0
    if module == "scripts.infrastructure.render_platform_config":
        return 0
    return fail(f"the stand-in Python does not know {module}", 1)


def main() -> int:
    tool, arguments = sys.argv[1], sys.argv[2:]
    state = load()
    state["calls"].append(shlex.join([tool, *arguments]))
    save(state)
    if tool == "aws":
        return aws(arguments, state)
    if tool == "terraform":
        return terraform(arguments, state)
    if tool == "wrapper":
        return wrapper(arguments, state)
    return python_module(arguments, state)


if __name__ == "__main__":
    sys.exit(main())
