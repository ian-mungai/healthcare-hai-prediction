"""Check, before an attended AWS step, that each project role holds the grants the project needs: one read-only simulation per table row.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.iam_preflight``. The profile, region and account come from ``.env`` through
``load_configuration`` and the calls go through the project's ``AwsCli`` wrapper under the project profile. For each row of
``config/aws/iam_preflight_checks.json`` the command calls ``iam simulate-principal-policy`` for the role, the action, the resource and the condition
keys of the row, and prints one line, ``PASS`` or ``FAIL``. Only ``allowed`` passes. An implicit or explicit deny, a denied simulation call, a missing
role, an answer that is not understood, a decision that needs a condition key the row lacks and a call that outlives its deadline all fail. The exit
status is 0 when every row passes, 1 when one fails and 2 when the setup is wrong (no configuration, a wrong identity or a table that does not parse).
Output names the table's resource text, never an account ID. The project profile needs ``iam:SimulatePrincipalPolicy`` on the roles, which the IAM
policy files grant after the owner applies them.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import AwsCli, StorageError
from scripts.infrastructure.iam_preflight_policies import SAMPLE_USER_ID, TABLE, TableError, expand, infra_names, load_table
from scripts.infrastructure.render_project_config import REPO_ROOT, ConfigurationError, load_configuration

CALL_SECONDS = 30
TOTAL_SECONDS = 600
DENIED = "the simulation call was denied: the project profile lacks iam:SimulatePrincipalPolicy on this role. The owner applies the IAM policy update first"
CODE_MESSAGES = {
    "AccessDenied": DENIED,
    "AccessDeniedException": DENIED,
    "NoSuchEntity": "the role does not exist yet; the owner applies the stack that creates it",
    "request_unconfirmed": "no answer before the call deadline",
    "invalid_response": "the answer was not understood",
}


def context_entries(row: dict[str, Any], values: dict[str, str]) -> list[dict[str, Any]]:
    """The condition keys of a row, with the one global key that stage-read policies name."""
    entries = [{"ContextKeyName": "aws:userid", "ContextKeyValues": [SAMPLE_USER_ID], "ContextKeyType": "string"}]
    for key, item in row.get("context", {}).items():
        listed = isinstance(item, list)
        entries.append(
            {
                "ContextKeyName": key,
                "ContextKeyValues": [expand(value, values) for value in (item if listed else [item])],
                "ContextKeyType": "stringList" if listed else "string",
            }
        )
    return entries


def decide(answer: dict[str, Any], action: str) -> str | None:
    """None when the one result is ``allowed`` with nothing missing, else the reason it fails."""
    results = answer.get("EvaluationResults")
    if not isinstance(results, list) or len(results) != 1 or not isinstance(results[0], dict) or results[0].get("EvalActionName") != action:
        return "the answer was not understood"
    result = results[0]
    if result.get("EvalDecision") != "allowed":
        return f"{result.get('EvalDecision')}: no policy of the role allows it"
    missing = result.get("MissingContextValues")
    if missing:
        return f"the decision needs condition keys the row does not give: {', '.join(str(key) for key in missing)}"
    return None


def check(client: AwsCli, row: dict[str, Any], role_arn: str, values: dict[str, str], seconds: float) -> str | None:
    """Simulate one row; None means pass, anything else is the failure message."""
    arguments = [
        "--policy-source-arn",
        role_arn,
        "--action-names",
        row["action"],
        "--resource-arns",
        expand(row["resource"], values),
        "--context-entries",
        json.dumps(context_entries(row, values)),
    ]
    try:
        answer = client.call_bounded("iam", "simulate-principal-policy", arguments, timeout=seconds)
    except StorageError as error:
        return CODE_MESSAGES.get(error.code, f"the call failed ({error.code})")
    return decide(answer, row["action"])


def verify_identity(client: AwsCli, settings: dict[str, str]) -> None:
    """The simulations run under the project user of the configured account, as the other project tools require."""
    try:
        identity = client.call_bounded("sts", "get-caller-identity", timeout=CALL_SECONDS)
    except StorageError:
        raise ConfigurationError("Could not verify the project AWS identity; check local profile credentials.") from None
    account, arn = settings["expected_account_id"], identity.get("Arn")
    if identity.get("Account") != account or not isinstance(arn, str) or arn != f"arn:aws:iam::{account}:user/{settings['aws_profile']}":
        raise ConfigurationError("The project profile must authenticate as its same-named IAM user in the configured account.")


def run(table: dict[str, Any], settings: dict[str, str], client: AwsCli, call_seconds: float, total_seconds: float) -> int:
    """Print one line per row and return the exit status."""
    values = {**infra_names(), **{key: settings[source] for key, source in BASE.items()}}
    verify_identity(client, settings)
    deadline = time.monotonic() + total_seconds
    failures = 0
    for row in table["checks"]:
        role = table["roles"][row["role"]]
        left = deadline - time.monotonic()
        if left <= 0:
            message: str | None = "not run: the total deadline passed"
        else:
            message = check(client, row, f"arn:aws:iam::{settings['expected_account_id']}:role/{expand(role['name'], values)}", values, min(call_seconds, left))
        label = f"{row['role']} {row['action']} {row['resource']}"
        if message is None:
            sys.stdout.write(f"PASS {label}\n")
            continue
        failures += 1
        sys.stdout.write(f"FAIL {label}: {message}\n")
    sys.stdout.write(f"{len(table['checks']) - failures} of {len(table['checks'])} checks passed\n")
    return 1 if failures else 0


BASE = {
    "AWS_REGION": "aws_region",
    "AWS_ACCOUNT_ID": "expected_account_id",
    "PROJECT_NAME": "project_name",
    "ENVIRONMENT": "environment",
    "DATA_BUCKET_NAME": "data_bucket_name",
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env", help="The project .env file.")
    parser.add_argument("--table", type=Path, default=TABLE, help="The table of role, action and resource rows.")
    parser.add_argument("--call-seconds", type=float, default=CALL_SECONDS, help="The deadline of one simulation call.")
    parser.add_argument("--total-seconds", type=float, default=TOTAL_SECONDS, help="The deadline of the whole run.")
    arguments = parser.parse_args(argv)
    try:
        settings, _ = load_configuration(arguments.env_file)
        table = load_table(arguments.table)
        return run(table, settings, AwsCli(settings), arguments.call_seconds, arguments.total_seconds)
    except (ConfigurationError, TableError, OSError) as error:
        sys.stderr.write(f"ERROR {error}\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
