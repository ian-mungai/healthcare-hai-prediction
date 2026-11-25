"""Stand-in aws, terraform, wrapper and Python module commands for the alert email helper. Never contacts AWS.

The owner list E2E puts a small script named ``aws``, ``terraform`` and ``.venv/bin/python`` first on the PATH of a scratch repository root and runs the
delivered owner blocks, which call ``scripts.infrastructure.alert_email``. Each script runs this file with the tool name. The state of the stand-in world is one
JSON file (``STAND_IN_STATE``): the subscriptions that AWS holds per topic, the resources of the alerts root state and the log of every call. A pending
subscription shows ``PendingConfirmation`` in a listing, as SNS does. The project user holds every action on the alert topics, so nothing is refused.

A case can make one read return something else, as AWS or Terraform do: ``fail_list`` (every listing exits 255), ``list_raw`` (the listing prints that
text), ``fail_state_list`` (``terraform state list`` exits 1), ``fail_state_pull`` (``terraform state pull`` exits 1), ``pull_raw`` (it prints that
text), ``remote_state`` (the state lives in a remote backend: it is read with no local file), ``missing_remote_object`` (the backend holds no state
object: ``state pull`` prints the empty state that Terraform makes up, serial 0 with no lineage, and ``state list`` exits 1), ``zero_resource_state``
(a persisted state with no resources: ``state pull`` prints a state with a lineage and ``state list`` prints nothing and exits 0), ``state_extra``
(``state list`` also lists that resource), ``show_raw`` (``terraform show -json`` prints that text), ``plan_extra`` (the saved plan also changes that
address), ``alerts_output`` and ``budget_output`` (the platform and IAM topic outputs print that text), ``policy_steps`` (the SNS policy plan prints
those step lines too), ``digest`` (it replaces the plan digest) and ``wrapper_fail`` (the wrapper prints that text to standard error and exits 1). The
saved plan reads as Terraform prints it: ``plan_document`` builds the envelope of ``terraform show -json``, with the metadata and the plan structure
of a supported plan and, when it has changes, ``resource_changes``. The alerts root has a state file (``infra/alerts/terraform.tfstate``) once an
import or an apply created something. Without a state (no file, an empty file and no remote state) ``state pull`` prints nothing and exits 0 and
``state list`` exits 1, as Terraform does.
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
STATE_FILE = Path("infra/alerts/terraform.tfstate")


def load() -> dict[str, Any]:
    return json.loads(Path(os.environ["STAND_IN_STATE"]).read_text())


def save(state: dict[str, Any]) -> None:
    Path(os.environ["STAND_IN_STATE"]).write_text(json.dumps(state, indent=1))


def touch_state() -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text("{}\n")


def holds_state(state: dict[str, Any]) -> bool:
    """A remote state, or a local state file that is not empty: Terraform reads an empty file as no state."""
    return bool(state.get("remote_state")) or (STATE_FILE.is_file() and STATE_FILE.stat().st_size > 0)


def fail(message: str, code: int = 255) -> int:
    sys.stderr.write(message + "\n")
    return code


def option(arguments: list[str], name: str) -> str | None:
    return arguments[arguments.index(name) + 1] if name in arguments and arguments.index(name) + 1 < len(arguments) else None


def aws(arguments: list[str], state: dict[str, Any]) -> int:
    if option(arguments, "--profile") != "example_project_dev" or option(arguments, "--region") != "us-west-2":
        return fail("the stand-in aws needs the project profile and region")
    if arguments[:2] != ["sns", "list-subscriptions-by-topic"]:
        return fail(f"the stand-in aws does not know {' '.join(arguments[:2])}")
    if state.get("fail_list"):
        return fail("An error occurred (AuthorizationError) when calling the ListSubscriptionsByTopic operation: not authorized")
    if state.get("list_raw") is not None:
        sys.stdout.write(state["list_raw"] + "\n")
        return 0
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


def plan_document(changes: list[dict[str, Any]] | None, **replace: Any) -> dict[str, Any]:
    """The JSON of ``terraform show -json`` for a saved plan. ``None`` is the no-change form, which has no ``resource_changes`` member; a ``replace`` value of
    ``None`` removes that member."""
    document: dict[str, Any] = {
        "format_version": "1.2",
        "terraform_version": "1.16.4",
        "planned_values": {"root_module": {}},
        "configuration": {"root_module": {}},
        "timestamp": "2026-11-11T00:00:00Z",
        "complete": True,
        "errored": False,
    }
    if changes is not None:
        document["resource_changes"] = changes
    document.update(replace)
    return {key: value for key, value in document.items() if value is not None}


def show(path: str, state: dict[str, Any]) -> int:
    if state.get("show_raw") is not None:
        sys.stdout.write(state["show_raw"] + "\n")
        return 0
    plan = json.loads(Path(path).read_text())
    actions = ["delete"] if plan.get("destroy") else ["create"]
    changes = [{"address": address, "change": {"actions": actions}} for address in plan.get("planned", [])]
    changes.append({"address": "aws_sns_topic.alerts", "change": {"actions": ["no-op"]}})
    if state.get("plan_extra"):
        changes.append({"address": state["plan_extra"], "change": {"actions": actions}})
    sys.stdout.write(json.dumps(plan_document(changes)) + "\n")
    return 0


def terraform(arguments: list[str], state: dict[str, Any]) -> int:
    directory = next((item.removeprefix("-chdir=") for item in arguments if item.startswith("-chdir=")), "")
    rest = [item for item in arguments if not item.startswith("-chdir=")]
    if directory == "infra/platform" and rest == ["output", "-raw", "alerts_topic_arn"]:
        sys.stdout.write(ALERTS_TOPIC if state.get("alerts_output") is None else state["alerts_output"])
        return 0
    if directory == "infra/iam" and rest == ["output", "-json", "budget"]:
        sys.stdout.write(
            state["budget_output"]
            if state.get("budget_output") is not None
            else json.dumps({"alerts_topic": BUDGET_TOPIC, "name": "example_project_budget_dev"})
        )
        return 0
    if rest[:2] == ["show", "-json"]:
        return show(rest[2], state)
    if directory != "infra/alerts":
        return fail(f"the stand-in terraform does not know {directory} {rest}", 1)
    if rest[:1] == ["init"]:
        sys.stdout.write("Terraform has been successfully initialized!\n")
        return 0
    if rest == ["state", "pull"]:
        if state.get("fail_state_pull"):
            return fail("Error: Failed to load state: the state cannot be read", 1)
        if state.get("missing_remote_object"):
            sys.stdout.write(json.dumps({"version": 4, "terraform_version": "1.16.4", "serial": 0, "lineage": "", "outputs": {}, "resources": []}))
            return 0
        if state.get("zero_resource_state"):
            sys.stdout.write(json.dumps({"version": 4, "terraform_version": "1.16.4", "serial": 9, "lineage": "synthetic", "outputs": {}, "resources": []}))
            return 0
        if holds_state(state):
            sys.stdout.write(
                state["pull_raw"] if state.get("pull_raw") is not None else json.dumps({"version": 4, "serial": 7, "lineage": "synthetic", "resources": []})
            )
        return 0
    if rest == ["state", "list"]:
        if state.get("zero_resource_state"):
            return 0
        if state.get("missing_remote_object") or not holds_state(state):
            return fail("No state file was found!", 1)
        if state.get("fail_state_list"):
            return fail("Error: Failed to read the local state: invalid state file format", 1)
        sys.stdout.write("".join(f"{item}\n" for item in [*state["tf"], *([state["state_extra"]] if state.get("state_extra") else [])]))
        return 0
    return fail(f"the stand-in terraform does not know {rest}", 1)


def wrapper(arguments: list[str], state: dict[str, Any]) -> int:
    if state.get("wrapper_fail"):
        return fail(state["wrapper_fail"], 1)
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
        path.write_text(json.dumps({"stack": stack, "destroy": destroy, "planned": planned}))
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
            state["tf"].remove(address)
            state["aws"][topic] = [row for row in state["aws"].get(topic, []) if row["arn"] != state["tf_arn"].get(address)]
        else:
            arn = f"{topic}:01234567-89ab-cdef-0123-4567{len(state['calls']):08d}"
            state["aws"].setdefault(topic, []).append({"arn": arn, "endpoint": state["address"], "status": "pending"})
            state["tf"].append(address)
            state["tf_arn"][address] = arn
    save(state)
    touch_state()
    sys.stdout.write("Apply complete.\n")
    return 0


def python_module(arguments: list[str], state: dict[str, Any]) -> int:
    module, rest = arguments[1], arguments[2:]
    if module == "infra.iam.scripts.manage_policies":
        if option(rest, "--admin-profile") is None:
            return fail("--admin-profile is required", 2)
        digest = state.get("digest", DIGEST)
        if rest[0] == "plan":
            sys.stdout.write(
                f"NOTE the project user holds grants outside this plan on arn:aws:iam::{ACCOUNT}:user/example_project\n"
                "UPDATE POLICY    healthcare_hai_prediction_sns_policy\n"
                + "".join(f"{line}\n" for line in state.get("policy_steps", []))
                + f"Summary: 1 change(s), 0 unchanged.\nPlan digest: {digest}\n"
            )
            return 0
        if os.environ.get("CONFIRM_IAM_POLICIES") != "apply-healthcare-hai-prediction-policies" or option(rest, "--digest") != digest:
            return fail("the confirmation or the reviewed digest does not match", 2)
        state["policy_applied"] = True
        save(state)
        sys.stdout.write(f"APPLIED   UPDATE POLICY    healthcare_hai_prediction_sns_policy: version of {TOPIC_PREFIX}_alerts_dev\nApplied 1 step(s).\n")
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
