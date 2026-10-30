"""A stand-in ``aws`` for the preflight E2E: it answers ``sts get-caller-identity`` and ``iam simulate-principal-policy`` from the rendered policy files.

The environment selects the behavior: ``IAM_PREFLIGHT_INFRA`` is the IAM stack folder to render, ``IAM_PREFLIGHT_MODE`` is ``ok``, ``denied``,
``hang``, ``garbage`` or ``other_user`` and ``IAM_PREFLIGHT_LOG`` receives one JSON line per call. The decision is the offline evaluator's, so it
models the policy files and not IAM itself.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from scripts.infrastructure.iam_preflight_policies import OFFLINE_VALUES, TableError, expand, infra_names, load_table, role_documents
from scripts.infrastructure.policy_evaluator import Request, decide

ACCOUNT = OFFLINE_VALUES["AWS_ACCOUNT_ID"]
PROFILE = f"{OFFLINE_VALUES['PROJECT_NAME']}_{OFFLINE_VALUES['ENVIRONMENT']}"
DECISIONS = {"allow": "allowed", "deny": "explicitDeny", "implicit_deny": "implicitDeny"}


def options(arguments: list[str]) -> dict[str, str]:
    found: dict[str, str] = {}
    for index, token in enumerate(arguments[2:], start=2):
        if token.startswith("--") and index + 1 < len(arguments) and not arguments[index + 1].startswith("--"):
            found[token] = arguments[index + 1]
    return found


def fail(code: str, operation: str) -> int:
    sys.stderr.write(f"\nAn error occurred ({code}) when calling the {operation} operation: stand-in\n")
    return 254


def simulate(given: dict[str, str]) -> dict[str, Any] | None:
    """The evaluator's answer for one request, or None when the role is not in the table."""
    infra = Path(os.environ["IAM_PREFLIGHT_INFRA"])
    names = infra_names(infra)
    values = {**names, **OFFLINE_VALUES}
    table = load_table()
    name = given["--policy-source-arn"].rsplit("/", 1)[-1]
    role = next((item for item in table["roles"].values() if expand(item["name"], values) == name), None)
    if role is None:
        return None
    context: dict[str, tuple[str, ...]] = {}
    for entry in json.loads(given["--context-entries"]):
        context[entry["ContextKeyName"]] = tuple(entry["ContextKeyValues"])
    decision = decide(role_documents(role, infra, OFFLINE_VALUES, names), Request(given["--action-names"], given["--resource-arns"], context))
    result = {
        "EvalActionName": given["--action-names"],
        "EvalResourceName": given["--resource-arns"],
        "EvalDecision": DECISIONS[decision.effect],
        "MissingContextValues": [],
    }
    return {"EvaluationResults": [result], "IsTruncated": False}


def main(arguments: list[str]) -> int:
    mode = os.environ["IAM_PREFLIGHT_MODE"]
    service, operation = arguments[0], arguments[1]
    given = options(arguments)
    with open(os.environ["IAM_PREFLIGHT_LOG"], "a", encoding="utf-8") as log:
        log.write(json.dumps({"service": service, "operation": operation, "profile": given.get("--profile")}) + "\n")
    if given.get("--profile") != PROFILE:
        sys.stderr.write(f"\nThe config profile ({given.get('--profile')}) could not be found\n")
        return 255
    if (service, operation) == ("sts", "get-caller-identity"):
        user = "someone_else" if mode == "other_user" else PROFILE
        sys.stdout.write(json.dumps({"Account": ACCOUNT, "Arn": f"arn:aws:iam::{ACCOUNT}:user/{user}", "UserId": "AIDAEXAMPLE"}))
        return 0
    if (service, operation) != ("iam", "simulate-principal-policy"):
        return fail("UnsupportedOperation", operation)
    if mode == "denied":
        return fail("AccessDenied", "SimulatePrincipalPolicy")
    if mode == "hang":
        time.sleep(60)
    if mode == "garbage":
        sys.stdout.write("not json")
        return 0
    try:
        answer = simulate(given)
    except TableError:
        return fail("InternalFailure", operation)
    if answer is None:
        return fail("NoSuchEntity", "SimulatePrincipalPolicy")
    sys.stdout.write(json.dumps(answer))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
