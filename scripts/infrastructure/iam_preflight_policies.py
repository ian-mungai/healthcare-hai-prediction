"""Load the IAM preflight table and decide each of its rows offline against the policy files that the IAM stack attaches.

The table ``config/aws/iam_preflight_checks.json`` lists, for each project role, the policies it holds and the actions and resources it needs. This module
renders those policies from ``infra/iam`` with the Terraform defaults and evaluates every row with the offline policy evaluator, so a policy change that
removes a listed grant is found without AWS. A row with ``pending_fix`` names a grant that is known to be missing: the offline check requires it to stay
denied until the fix lands, and then asks for the field to be removed. ``python -m scripts.infrastructure.iam_preflight_policies`` runs the check.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from infra.iam.scripts.render_policy import PolicyRenderError, render_template, terraform_names
from scripts.infrastructure.policy_evaluator import Request, UnsupportedPolicyElement, decide
from scripts.infrastructure.render_project_config import REPO_ROOT

INFRA = REPO_ROOT / "infra" / "iam"
TABLE = REPO_ROOT / "config" / "aws" / "iam_preflight_checks.json"
# The values of the offline run. The project name is the real one because the parameter name defaults start with it.
OFFLINE_VALUES = {
    "AWS_REGION": "us-west-2",
    "AWS_ACCOUNT_ID": "111111111111",
    "PROJECT_NAME": "healthcare_hai_prediction",
    "ENVIRONMENT": "dev",
    "DATA_BUCKET_NAME": "example-hai-data-bucket",
}
PLACEHOLDER = re.compile(r"\$\{([A-Z][A-Z0-9_]*)\}")
ROW_KEYS = {"role", "action", "resource", "context", "pending_fix"}
ROLE_KEYS = {"name", "terraform", "policies", "layer"}
SAMPLE_USER_ID = "AROAEXAMPLE:task"


class TableError(ValueError):
    """The table, a policy or a Terraform input is not in the form this check models."""


def load_table(path: Path = TABLE) -> dict[str, Any]:
    """Read and shape-check the table: each row names a known role, an action and a resource, and no row repeats."""
    try:
        table = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise TableError(f"The preflight table {path.name} is missing or is not JSON.") from None
    if not isinstance(table, dict) or set(table) != {"roles", "checks"} or not isinstance(table["roles"], dict) or not isinstance(table["checks"], list):
        raise TableError("The preflight table must hold exactly the keys roles and checks.")
    for key, role in table["roles"].items():
        if not isinstance(role, dict) or not {"name", "terraform", "policies"} <= set(role) or set(role) - ROLE_KEYS:
            raise TableError(f"Role {key} needs name, terraform and policies, and may add layer.")
    seen: set[str] = set()
    for row in table["checks"]:
        if not isinstance(row, dict) or not {"role", "action", "resource"} <= set(row) or set(row) - ROW_KEYS:
            raise TableError("Each check needs role, action and resource, and may add context and pending_fix.")
        if row["role"] not in table["roles"]:
            raise TableError(f"A check names the unknown role {row['role']}.")
        identity = json.dumps(row, sort_keys=True)
        if identity in seen:
            raise TableError(f"A check is listed twice: {row['role']} {row['action']} {row['resource']}.")
        seen.add(identity)
    return table


def terraform_default(infra: Path, file: str, variable: str) -> str:
    """The string default of one Terraform variable."""
    text = (infra / file).read_text(encoding="utf-8")
    match = re.search(rf'variable "{variable}" \{{.*?default\s*=\s*"([^"]*)"', text, re.DOTALL)
    if match is None:
        raise TableError(f"{file} has no string default for {variable}.")
    return match.group(1)


def terraform_list(infra: Path, file: str, variable: str) -> list[str]:
    """The string list default of one Terraform variable."""
    text = (infra / file).read_text(encoding="utf-8")
    match = re.search(rf'variable "{variable}" \{{.*?default\s*=\s*\[(.*?)\]', text, re.DOTALL)
    if match is None:
        raise TableError(f"{file} has no list default for {variable}.")
    return re.findall(r'"([^"]*)"', match.group(1))


def infra_names(infra: Path = INFRA) -> dict[str, str]:
    """The single names a table row or policy may use: the Terraform variable defaults plus the two broker parameters."""
    names = {key: value for key, value in terraform_names(infra / "variables.tf").items() if isinstance(value, str)}
    names["BROKER_TOKEN_NAME"] = terraform_default(infra, "broker.tf", "broker_token_name")
    names["BROKER_SIGNED_PARAMETER_NAME"] = terraform_default(infra, "broker.tf", "broker_signed_parameter_name")
    for part in ("id", "secret"):
        names[f"AWS_POLARIS_FEATURES_CLIENT_{part.upper()}_NAME"] = terraform_default(
            infra, "job_writer_variables.tf", f"aws_polaris_features_client_{part}_name"
        )
    return names


def expand(text: str, values: Mapping[str, str]) -> str:
    """Replace every placeholder in a table text; an unknown placeholder stops the run instead of passing through."""

    def lookup(match: re.Match[str]) -> str:
        if match.group(1) not in values:
            raise TableError(f"The table uses the unknown placeholder {match.group(1)}.")
        return values[match.group(1)]

    return PLACEHOLDER.sub(lookup, text)


def string_list(text: str) -> list[str]:
    return re.findall(r'"([^"]*)"', text)


def layer_prefixes(infra: Path, layer: str) -> list[str]:
    match = re.search(rf"\b{layer}\s*=\s*\{{\s*prefixes\s*=\s*\[(.*?)\]", (infra / "job_writer.tf").read_text(encoding="utf-8"), re.DOTALL)
    if match is None:
        raise TableError(f"job_writer.tf lists no prefixes for the {layer} layer.")
    return string_list(match.group(1))


def lakehouse_role_policy(text: str, infra: Path, values: Mapping[str, str]) -> dict[str, Any]:
    """Render reader.json or writer.json: the two jsonencode forms that they use, over the prefix lists in roles.tf."""
    roles_text = (infra / "roles.tf").read_text(encoding="utf-8")

    def prefixes(name: str) -> list[str]:
        match = re.search(rf"lakehouse_{name.lower()}\s*=\s*\[(.*?)\]", roles_text, re.DOTALL)
        if match is None:
            raise TableError(f"roles.tf lists no lakehouse_{name.lower()}.")
        return string_list(match.group(1))

    objects = re.compile(r'\$\{jsonencode\(\[for prefix in (\w+) : "arn:aws:s3:::\$\{DATA_BUCKET_NAME\}/\$\{prefix\}\*"\]\)\}')
    listing = re.compile(r'\$\{jsonencode\(flatten\(\[for prefix in (\w+) : \[trimsuffix\(prefix, "/"\), "\$\{prefix\}\*"\]\]\)\)\}')
    text = objects.sub(lambda m: json.dumps([f"arn:aws:s3:::{values['DATA_BUCKET_NAME']}/{p}*" for p in prefixes(m.group(1))]), text)
    text = listing.sub(lambda m: json.dumps([item for p in prefixes(m.group(1)) for item in (p.rstrip("/"), f"{p}*")]), text)
    if "jsonencode" in text:
        raise TableError("A lakehouse role policy uses a jsonencode form that the preflight check does not model.")
    return json.loads(expand(text, values))


def policy_inputs(policy: str, role: Mapping[str, Any], infra: Path, values: Mapping[str, str], names: Mapping[str, str]) -> dict[str, str]:
    """The Terraform-computed placeholders of one owner policy, built the way the IAM stack builds them."""
    bucket, account, region = values["DATA_BUCKET_NAME"], values["AWS_ACCOUNT_ID"], values["AWS_REGION"]
    project, environment = values["PROJECT_NAME"], values["ENVIRONMENT"]
    if policy.endswith("job_stage_read.json"):
        return {"TASK_USER_ID": "${aws:userid}"}
    if policy.endswith("job_writer_layer_storage.json"):
        layer_prefix = layer_prefixes(infra, role["layer"])
        return {
            "BUCKET_ARN": f"arn:aws:s3:::{bucket}",
            "TABLE_OBJECT_ARNS": json.dumps([f"arn:aws:s3:::{bucket}/lakehouse/{p}/*" for p in layer_prefix]),
            "TABLE_LIST_PREFIXES": json.dumps([f"lakehouse/{p}/*" for p in layer_prefix]),
        }
    if policy.endswith("job_writer_execution.json"):
        pairs = [names[f"AWS_POLARIS_{layer}_CLIENT_{part}_NAME"] for layer in ("SILVER", "GOLD", "FEATURES") for part in ("ID", "SECRET")]
        return {"WRITER_PARAMETER_ARNS": json.dumps([f"arn:aws:ssm:{region}:{account}:parameter{name}" for name in pairs])}
    if policy.endswith("broker_jobs.json"):
        families = terraform_list(infra, "broker.tf", "broker_job_families")
        cluster = f"{project}_cluster_{environment}"
        return {
            "CLUSTER_ARN": f"arn:aws:ecs:{region}:{account}:cluster/{cluster}",
            "TASK_ARN_PATTERN": f"arn:aws:ecs:{region}:{account}:task/{cluster}/*",
            "LOG_GROUP_NAME": f"{project}_jobs_{environment}",
            "PROJECT_TAG": project,
            "SIGNED_PARAMETER_NAME": names["BROKER_SIGNED_PARAMETER_NAME"],
            "FAMILIES": json.dumps(sorted(families)),
            "TASK_DEFINITION_ARNS": json.dumps([f"arn:aws:ecs:{region}:{account}:task-definition/{project}_{environment}_job_{f}:*" for f in sorted(families)]),
            "ROLE_ARNS": json.dumps([f"arn:aws:iam::{account}:role/{project}_job_{kind}_{environment}" for kind in ("execution", "train_task")]),
        }
    return {}


def role_documents(role: Mapping[str, Any], infra: Path, values: Mapping[str, str], names: Mapping[str, str]) -> list[dict[str, Any]]:
    """The policy documents of one role, rendered from the files in the IAM stack."""
    documents = []
    for policy in role["policies"]:
        try:
            text = (infra / policy).read_text(encoding="utf-8")
            if policy.startswith("role_policies/"):
                documents.append(lakehouse_role_policy(text, infra, {**names, **values}))
            else:
                documents.append(render_template(text, values, {**names, **policy_inputs(policy, role, infra, values, names)}))
        except (OSError, PolicyRenderError) as error:
            raise TableError(f"{policy}: {error}") from None
    return documents


def request_context(row: Mapping[str, Any], values: Mapping[str, str]) -> dict[str, tuple[str, ...]]:
    """The condition keys of a row: a string value is one value, a list is a list of values."""
    context = {}
    for key, item in row.get("context", {}).items():
        items = item if isinstance(item, list) else [item]
        context[key] = tuple(expand(value, values) for value in items)
    return context


def problems(table: Mapping[str, Any], infra: Path = INFRA, values: Mapping[str, str] = OFFLINE_VALUES) -> list[str]:
    """Every row that the policy files do not support, every policy that Terraform does not attach to its role and every stale ``pending_fix``."""
    names = infra_names(infra)
    found = []
    documents: dict[str, list[dict[str, Any]]] = {}
    for key, role in table["roles"].items():
        terraform = " ".join((infra / file).read_text(encoding="utf-8") for file in role["terraform"])
        for policy in role["policies"]:
            if Path(policy).stem not in terraform:
                found.append(f"role {key}: Terraform {', '.join(role['terraform'])} no longer names {policy}")
        documents[key] = role_documents(role, infra, values, names)
    expanded = {**names, **values}
    for row in table["checks"]:
        label = f"{row['role']} {row['action']} {row['resource']}"
        context = {"aws:userid": (SAMPLE_USER_ID,), **request_context(row, expanded)}
        try:
            decision = decide(documents[row["role"]], Request(row["action"], expand(row["resource"], expanded), context))
        except UnsupportedPolicyElement as error:
            found.append(f"{label}: the policy uses an element the evaluator does not model ({error})")
            continue
        pending = row.get("pending_fix")
        if pending and decision.allowed:
            found.append(f"{label}: now allowed, so remove its pending_fix")
        elif not pending and not decision.allowed:
            found.append(f"{label}: not granted by the policies of role {row['role']} ({decision.effect})")
    return found


def main(argv: list[str] | None = None) -> int:
    """Print the offline problems; exit non-zero when there is one. ``--infra`` names a copy of the IAM stack to check."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--infra", type=Path, default=INFRA, help="The IAM stack folder to check.")
    arguments = parser.parse_args(argv)
    try:
        found = problems(load_table(), arguments.infra)
    except TableError as error:
        sys.stderr.write(f"ERROR {error}\n")
        return 2
    for line in found:
        sys.stdout.write(f"FAIL {line}\n")
    sys.stdout.write(f"{len(found)} problem(s)\n" if found else "OK every listed grant is in the policy files\n")
    return 1 if found else 0


if __name__ == "__main__":
    raise SystemExit(main())
