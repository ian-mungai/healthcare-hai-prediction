"""Evaluate the rendered per-service IAM policies against the approved list, plant defects in them and record what each defect breaks.

Offline: no AWS call, no credentials. The report is written to a new directory.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import logging
import re
import shutil
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from infra.iam.scripts.render_policy import MAX_POLICY_CHARACTERS, non_whitespace_length, render_template, template_service, terraform_names
from scripts.infrastructure import policy_cases
from scripts.infrastructure.policy_evaluator import Decision, Request, allowed_actions, as_list, decide, load_policies
from scripts.infrastructure.policy_script_defects import run_script_defects
from scripts.process import run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
POLICY_DIRECTORY = REPO_ROOT / "infra" / "iam" / "policies"
OWNER_DIRECTORY = REPO_ROOT / "infra" / "iam" / "owner_policies"
ROLE_DIRECTORY = REPO_ROOT / "infra" / "iam" / "role_policies"
VARIABLES = REPO_ROOT / "infra" / "iam" / "variables.tf"
# The longest values the real deployment can use, to check the managed policy size limit with them.
LONGEST_VALUES = {
    "AWS_REGION": "ap-southeast-2",
    "AWS_ACCOUNT_ID": "999999999999",
    "PROJECT_NAME": "healthcare_hai_prediction",
    "ENVIRONMENT": "prod",
    "DATA_BUCKET_NAME": "healthcare-hai-prediction-data-bucket-0123456789ab",
}
ANY_RESOURCE = {item.action for requests in policy_cases.approved_requests().values() for item in requests if item.resource == "*"}


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def code_revision() -> dict[str, Any]:
    """The commit and whether the working tree differs from it, so the report names the code it evaluated."""
    head = run_command("git", ["rev-parse", "--short", "HEAD"], cwd=REPO_ROOT, timeout=30).stdout.strip()
    dirty = run_command("git", ["status", "--porcelain", "--", "infra/iam", "scripts/infrastructure"], cwd=REPO_ROOT, timeout=30).stdout.strip()
    return {"head": head, "uncommitted_changes_in_scope": bool(dirty)}


def load_all() -> dict[str, dict[str, Any]]:
    """The 13 project user policies by template stem and the policies the IAM stack renders, as ``owner_<name>``."""
    documents = load_policies(POLICY_DIRECTORY, policy_cases.VALUES)
    for name in ("snapshots", "task_execution"):
        documents[f"owner_{name}"] = render_template((OWNER_DIRECTORY / f"{name}.json").read_text(encoding="utf-8"), policy_cases.VALUES)
    return documents


def evaluate_cases(documents: dict[str, dict[str, Any]], cases: list[policy_cases.Case]) -> list[dict[str, Any]]:
    """Run every case; return the failures with the decision, the statement and the expected outcome."""
    failures = []
    held = policy_cases.principals(documents)
    for case in cases:
        decision: Decision = decide(held[case.principal], case.request)
        wrong_effect = decision.allowed != case.allowed
        wrong_reason = case.sid is not None and case.sid not in decision.statements
        if wrong_effect or wrong_reason:
            failures.append({"label": case.label, "action": case.request.action, "resource": case.request.resource, "expected_allowed": case.allowed,
                             "expected_statement": case.sid, "effect": decision.effect, "statements": list(decision.statements)})  # fmt: skip
    return failures


def check_inventory(documents: dict[str, dict[str, Any]], operations: dict[str, list[Request]]) -> list[str]:
    """Policy files, allowed actions, wildcards and sizes against the approved list."""
    problems = []
    expected_names = {f"healthcare_hai_prediction_{service}_policy" for service in policy_cases.SERVICES}
    user_names = {name for name in documents if name.startswith("healthcare_hai_prediction_")}
    if user_names != expected_names:
        extra, missing = sorted(user_names - expected_names), sorted(expected_names - user_names)
        problems.append(f"policy files differ from the 13 approved services: extra {extra}, missing {missing}")
    approved = policy_cases.approved_actions(operations)
    for name, actions in policy_cases.APPROVED_OWNER_ACTIONS.items():
        allowed = allowed_actions([documents[f"owner_{name}"]])
        problems += [f"{name}: action not approved: {action}" for action in sorted(allowed - actions)]
        problems += [f"{name}: approved action missing: {action}" for action in sorted(actions - allowed)]
    for name, document in sorted((name, document) for name, document in documents.items() if name in user_names):
        service = template_service(name)
        actual = allowed_actions([document])
        wildcard = sorted(action for action in actual if "*" in action)
        if wildcard:
            problems.append(f"{service}: wildcard actions {wildcard}")
        for sid_action in sorted(actual - approved.get(service, set())):
            problems.append(f"{service}: action not on the approved list: {sid_action}")
        for sid_action in sorted(approved.get(service, set()) - actual):
            problems.append(f"{service}: approved action missing: {sid_action}")
        for statement in document["Statement"]:
            if statement["Effect"] == "Allow" and "*" in as_list(statement["Resource"]):
                outside = sorted(set(as_list(statement["Action"])) - ANY_RESOURCE)
                if outside:
                    problems.append(f"{service}: statement {statement['Sid']} allows {outside} on every resource")
                if "Condition" not in statement and service != "s3":
                    problems.append(f"{service}: statement {statement['Sid']} on every resource has no region condition")
    return problems


def resources(document: dict[str, Any], sid: str) -> set[str]:
    """The resources of the statement with this identifier."""
    return {resource for statement in document["Statement"] if statement["Sid"] == sid for resource in as_list(statement["Resource"])}


def parameter_arn(name: str) -> str:
    return f"arn:aws:ssm:{policy_cases.R}:{policy_cases.A}:parameter{name}"


def secret_arn(name: str) -> str:
    return f"arn:aws:secretsmanager:{policy_cases.R}:{policy_cases.A}:secret:{name}-??????"


ROLE_PLACEHOLDERS = ("LAKEHOUSE_READER_ROLE_NAME", "LAKEHOUSE_WRITER_ROLE_NAME")
EXECUTION_PLACEHOLDERS = tuple(f"{name.upper()}_NAME" for name in policy_cases.EXECUTION_PARAMETERS)
AWS_POLARIS_PLACEHOLDERS = tuple(f"{name.upper()}_NAME" for name in policy_cases.AWS_POLARIS_PARAMETERS)
AWS_PAIRS = ("ssm", "SetUpAwsPolarisPairs")
TASK_EXECUTION = "owner_task_execution"
STS = ("sts", "AssumeLakehouseRolesOnly")
INJECT = (TASK_EXECUTION, "InjectNamedParameters")


def role_arn(name: str) -> str:
    return f"arn:aws:iam::{policy_cases.A}:role/{name}"


def expected_grants(names: dict[str, Any]) -> dict[tuple[str, str], set[str]]:
    """The resources of the statements that carry a name from a variable (secrets, parameters, lakehouse roles), for these names."""
    return {
        ("secretsmanager", "ReadSharedApiKeys"): {secret_arn(name) for name in names["SHARED_SECRET_NAME"]},
        ("ssm", "ReadProjectParameters"): {parameter_arn(name) for name in names["READ_PARAMETER_NAME"]},
        ("ssm", "WriteManagedProjectParameters"): {parameter_arn(name) for name in names["MANAGED_PARAMETER_NAME"]},
        STS: {role_arn(names[key]) for key in ROLE_PLACEHOLDERS},
        INJECT: {parameter_arn(names[key]) for key in EXECUTION_PLACEHOLDERS},
        AWS_PAIRS: {parameter_arn(names[key]) for key in AWS_POLARIS_PLACEHOLDERS},
    }


def statement_resources(documents: dict[str, dict[str, Any]], key: tuple[str, str]) -> set[str]:
    """The resources of one statement, from a project user policy (by service) or the task execution policy."""
    service, sid = key
    return resources(documents[service if service.startswith("owner_") else f"healthcare_hai_prediction_{service}_policy"], sid)


def with_task_execution(documents: dict[str, dict[str, Any]], names: dict[str, Any], directory: Path = OWNER_DIRECTORY) -> dict[str, dict[str, Any]]:
    """The documents with the task execution policy rendered from the same names, unless they already hold one."""
    if TASK_EXECUTION in documents:
        return documents
    text = (directory / "task_execution.json").read_text(encoding="utf-8")
    return {**documents, TASK_EXECUTION: render_template(text, policy_cases.VALUES, names)}


def difference(label: str, actual: set[str], expected: set[str]) -> list[str]:
    if actual == expected:
        return []
    return [f"{label}: extra {sorted(actual - expected)}, missing {sorted(expected - actual)}"]


def check_names(documents: dict[str, dict[str, Any]], variables: Path = VARIABLES) -> list[str]:
    """The names in the statements equal the Terraform variable defaults, and the defaults equal the owner's approved list.

    The parameter read set is the 17 project parameters and the write set the 16 managed ones, the same lists as ``read_parameter_names`` and
    ``managed_parameter_names`` in ``infra/iam/main.tf`` (its tests pin those). The sts policy grants the two lakehouse roles and the task execution
    policy injects nine of the managed parameters, each from its own variable, checked here with ``PROJECT_NAME`` set to a value that no variable
    default contains. The approved lists are written out in ``policy_cases``, not read from a file.
    """
    names = terraform_names(variables)
    documents = with_task_execution(documents, names)
    problems = []
    for key, expected in expected_grants(names).items():
        problems += difference(f"{key[0]} {key[1]} against the variable defaults", statement_resources(documents, key), expected)
    folder = f"/{policy_cases.PARAMETER_FOLDER}/"
    approved: dict[str, set[str]] = {
        "SHARED_SECRET_NAME": set(policy_cases.SHARED_KEYS),
        "READ_PARAMETER_NAME": {folder + name for name in (*policy_cases.PARAMETERS, *policy_cases.READ_ONLY_PARAMETERS)},
        "MANAGED_PARAMETER_NAME": {folder + name for name in policy_cases.PARAMETERS},
    }
    for placeholder, name in zip(ROLE_PLACEHOLDERS, ("reader", "writer"), strict=True):
        approved[placeholder] = {f"{policy_cases.PARAMETER_FOLDER}_lakehouse_{name}"}
    for placeholder, name in zip(EXECUTION_PLACEHOLDERS, policy_cases.EXECUTION_PARAMETERS, strict=True):
        approved[placeholder] = {folder + name}
    for placeholder, name in zip(AWS_POLARIS_PLACEHOLDERS, policy_cases.AWS_POLARIS_PARAMETERS, strict=True):
        approved[placeholder] = {folder + name}
    for placeholder, expected_names in approved.items():
        problems += difference(
            f"{placeholder} variable defaults against the approved list", {str(item) for item in as_list(names[placeholder])}, expected_names
        )
    return problems


def sentinel_names(real: dict[str, Any]) -> dict[str, Any]:
    """Names that appear in no file, in the shape of the real ones: two for each list, one for each single name."""

    def make(key: str, value: str, index: int = 0) -> str:
        return ("/sentinel_folder/" if value.startswith("/") else "sentinel_") + f"{key.lower()}_{index}"

    return {key: ([make(key, value[0], index) for index in (1, 2)] if isinstance(value, list) else make(key, value)) for key, value in real.items()}


def check_name_source(directory: Path = POLICY_DIRECTORY, owner_directory: Path = OWNER_DIRECTORY) -> list[str]:
    """Render the templates with names that are in no file: each name statement must hold exactly those, and no other statement names a parameter or secret.

    A template that lists a name itself, or builds one from ``PROJECT_NAME``, keeps it here, so a second source of names cannot hide behind the right values.
    """
    sentinel = sentinel_names(terraform_names())
    documents = with_task_execution(load_policies(directory, policy_cases.VALUES, names=sentinel), sentinel, owner_directory)
    expected = expected_grants(sentinel)
    problems = []
    for key, wanted in expected.items():
        problems += difference(f"{key[0]} {key[1]} against the injected names", statement_resources(documents, key), wanted)
    public = f"arn:aws:ssm:{policy_cases.R}::parameter/aws/service/ecs/*"
    granted = {
        resource
        for document in documents.values()
        for statement in document["Statement"]
        for resource in as_list(statement["Resource"])
        if ":parameter" in resource
    } - {public}
    allowed = expected[("ssm", "ReadProjectParameters")] | expected[("ssm", "WriteManagedProjectParameters")] | expected[INJECT] | expected[AWS_PAIRS]
    problems += difference("parameter resources of all policies against the injected names", granted, allowed)
    named_secrets = {
        resource
        for document in documents.values()
        for statement in document["Statement"]
        for resource in as_list(statement["Resource"])
        if ":secret:" in resource
    }
    problems += difference("secret resources of all policies against the injected names", named_secrets, expected[("secretsmanager", "ReadSharedApiKeys")])
    return problems


def check_roles(directory: Path = ROLE_DIRECTORY, variables: Path = VARIABLES, policies: Path = POLICY_DIRECTORY) -> list[str]:
    """The sts policy grants the two roles that the role-name variables create; the roles hold no S3 action the s3 policy lacks.

    The sts policy is rendered with ``PROJECT_NAME`` set to a value that the role-name defaults do not contain, so a role name built from it fails.
    """
    names = terraform_names(variables)
    real = load_policies(policies, policy_cases.VALUES, names=names)
    problems = difference("sts AssumeLakehouseRolesOnly against the role name variables", statement_resources(real, STS), expected_grants(names)[STS])
    held = allowed_actions([real["healthcare_hai_prediction_s3_policy"]])
    for name in ("reader", "writer"):
        text = (directory / f"{name}.json").read_text(encoding="utf-8")
        actions = {action for block in re.findall(r'"Action":\s*(\[[^\]]*\]|"[^"]*")', text) for action in re.findall(r'"([a-z0-9]+:[A-Za-z0-9*]+)"', block)}
        problems += [f"the {name} role holds {action}, which the project user's s3 policy does not grant" for action in sorted(actions - held)]
    return problems


def plant(text: str, old: str, new: str) -> str:
    """The text with one change; the planted text must occur once, or the defect would silently not be planted."""
    if text.count(old) != 1:
        raise RuntimeError(f"the planted text must occur once: {old}")
    return text.replace(old, new)


def name_defects(documents: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Plant defects in the names, the roles and their sources, one at a time, and record what the name checks report for each."""
    results: list[dict[str, Any]] = []

    def record(defect: str, found: list[str]) -> None:
        results.append({"defect": defect, "problems": len(found), "first_problem": found[0] if found else "", "caught": bool(found)})

    def drift(documents: dict[str, dict[str, Any]], service: str, sid: str, old: str, new: str) -> dict[str, dict[str, Any]]:
        mutated = copy.deepcopy(documents)
        statement = policy_cases.by_sid(mutated, service, sid)
        statement["Resource"] = [resource.replace(old, new) for resource in as_list(statement["Resource"])]
        return mutated

    def dropped(documents: dict[str, dict[str, Any]], service: str, sid: str, name: str) -> dict[str, dict[str, Any]]:
        mutated = copy.deepcopy(documents)
        statement = policy_cases.by_sid(mutated, service, sid)
        statement["Resource"] = [resource for resource in as_list(statement["Resource"]) if name not in resource]
        return mutated

    record(
        "a parameter name in the read list drifts from its variable default",
        check_names(drift(documents, "ssm", "ReadProjectParameters", "redis_password", "redis_passwd")),
    )
    record(
        "a parameter name in the write list drifts from its variable default",
        check_names(drift(documents, "ssm", "WriteManagedProjectParameters", "postgres_password", "postgres_passwd")),
    )
    record("a managed parameter is missing from the read list", check_names(dropped(documents, "ssm", "ReadProjectParameters", "marquez_db_password")))
    record("a managed parameter is missing from the write list", check_names(dropped(documents, "ssm", "WriteManagedProjectParameters", "airflow_fernet_key")))
    record("the Airflow administrator password is missing from the read list", check_names(dropped(documents, "ssm", "ReadProjectParameters", "airflow_admin")))
    record("a shared API key is missing from the Secrets Manager grant", check_names(dropped(documents, "secretsmanager", "ReadSharedApiKeys", "jev_api_key")))
    record(
        "a shared API key name drifts from its variable default", check_names(drift(documents, "secretsmanager", "ReadSharedApiKeys", "hud_api_key", "hud_key"))
    )
    with tempfile.TemporaryDirectory(prefix="policy_name_defects_") as scratch:
        root = Path(scratch)
        original = VARIABLES.read_text(encoding="utf-8")

        def changed_variables(old: str, new: str, label: str) -> Path:
            path = root / f"{label}.tf"
            path.write_text(plant(original, old, new), encoding="utf-8")
            return path

        parameter_drift = changed_variables('"/healthcare_hai_prediction/redis_password"', '"/healthcare_hai_prediction/redis_passwd"', "parameter_drift")
        record(
            "a variable default drifts: the policy follows it but the approved list does not",
            check_names(load_policies(POLICY_DIRECTORY, policy_cases.VALUES, names=terraform_names(parameter_drift)), parameter_drift),
        )
        admin_drift = changed_variables('"/healthcare_hai_prediction/airflow_admin"', '"/healthcare_hai_prediction/airflow_admin_renamed"', "admin_drift")
        record(
            "the Airflow administrator password is renamed and written as a managed parameter",
            check_names(load_policies(POLICY_DIRECTORY, policy_cases.VALUES, names=terraform_names(admin_drift)), admin_drift),
        )
        role_drift = changed_variables('"healthcare_hai_prediction_lakehouse_writer"', '"healthcare_hai_prediction_lakehouse_writer_two"', "role_drift")
        record(
            "a role-name variable default changes: the sts policy follows it but the approved role does not",
            check_names(load_policies(POLICY_DIRECTORY, policy_cases.VALUES, names=terraform_names(role_drift)), role_drift),
        )

        def copied(label: str) -> Path:
            target = root / label
            shutil.copytree(POLICY_DIRECTORY, target)
            return target

        literal = copied("literal_names")
        template = literal / "healthcare_hai_prediction_ssm_policy.json"
        names = terraform_names()["READ_PARAMETER_NAME"]
        placeholder = '"arn:aws:ssm:${AWS_REGION}:${AWS_ACCOUNT_ID}:parameter${READ_PARAMETER_NAME}"'
        listed = ",\n        ".join(f'"arn:aws:ssm:${{AWS_REGION}}:${{AWS_ACCOUNT_ID}}:parameter{name}"' for name in names)
        template.write_text(plant(template.read_text(encoding="utf-8"), placeholder, listed), encoding="utf-8")
        same_values = check_names(load_policies(literal, policy_cases.VALUES))
        record("a template lists the parameter names itself, right today (only the injected names show it)", check_name_source(literal))
        results[-1]["right_values_pass_the_names_check"] = not same_values
        drifted = copied("literal_drift")
        template = drifted / "healthcare_hai_prediction_ssm_policy.json"
        template.write_text(template.read_text(encoding="utf-8").replace(placeholder, listed.replace("redis_password", "redis_passwd")), encoding="utf-8")
        record("a template lists the parameter names itself and one has drifted", check_names(load_policies(drifted, policy_cases.VALUES)))
        extra = copied("extra_statement")
        template = extra / "healthcare_hai_prediction_ssm_policy.json"
        statement = {
            "Sid": "ReadOneMoreParameter",
            "Effect": "Allow",
            "Action": ["ssm:GetParameter"],
            "Resource": ["arn:aws:ssm:${AWS_REGION}:${AWS_ACCOUNT_ID}:parameter/other_folder/token"],
        }
        document = json.loads(template.read_text(encoding="utf-8"))
        document["Statement"].append(statement)
        template.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
        record("a statement outside the three names one more parameter", check_name_source(extra))
        names_now = terraform_names()
        real_values = {**policy_cases.VALUES, "PROJECT_NAME": "healthcare_hai_prediction"}

        def real_sts(directory: Path) -> set[str]:
            return statement_resources(load_policies(directory, real_values, names=names_now), STS)

        sts = copied("role_names")
        template = sts / "healthcare_hai_prediction_sts_policy.json"
        template.write_text(plant(template.read_text(encoding="utf-8"), "${LAKEHOUSE_READER_ROLE_NAME}", "${PROJECT_NAME}_lakehouse_viewer"), encoding="utf-8")
        record("the sts policy names a lakehouse role the stack does not create", check_roles(policies=sts))
        hard = copied("sts_project_name")
        template = hard / "healthcare_hai_prediction_sts_policy.json"
        template.write_text(plant(template.read_text(encoding="utf-8"), "${LAKEHOUSE_WRITER_ROLE_NAME}", "${PROJECT_NAME}_lakehouse_writer"), encoding="utf-8")
        record("an sts role name is built from PROJECT_NAME, right for the real project name only", check_roles(policies=hard))
        results[-1]["identical_with_the_real_project_name"] = not difference("sts", real_sts(hard), expected_grants(names_now)[STS])
        results[-1]["caught_by_the_name_source_check"] = bool(check_name_source(hard))
        owner = root / "owner_policies"
        shutil.copytree(OWNER_DIRECTORY, owner)
        template = owner / "task_execution.json"
        original_owner = template.read_text(encoding="utf-8")
        template.write_text(plant(original_owner, "parameter${POSTGRES_PASSWORD_NAME}", "parameter/${PROJECT_NAME}/postgres_password"), encoding="utf-8")
        built = render_template(template.read_text(encoding="utf-8"), policy_cases.VALUES)
        record(
            "a task execution parameter is built from PROJECT_NAME, right for the real project name only",
            check_names({**documents, TASK_EXECUTION: built}),
        )
        real_owner = render_template(template.read_text(encoding="utf-8"), real_values, names_now)
        results[-1]["identical_with_the_real_project_name"] = not difference(
            "exec", statement_resources({TASK_EXECUTION: real_owner}, INJECT), expected_grants(names_now)[INJECT]
        )
        results[-1]["caught_by_the_name_source_check"] = bool(check_name_source(owner_directory=owner))
        template.write_text(plant(original_owner, "parameter${REDIS_PASSWORD_NAME}", "parameter/healthcare_hai_prediction/redis_password"), encoding="utf-8")
        record("a task execution parameter is written out in the template (right today)", check_name_source(owner_directory=owner))
        template.write_text(plant(original_owner, "parameter${REDIS_PASSWORD_NAME}", "parameter/healthcare_hai_prediction/redis_passwd"), encoding="utf-8")
        built = render_template(template.read_text(encoding="utf-8"), policy_cases.VALUES)
        record("a task execution parameter is written out in the template and has drifted", check_names({**documents, TASK_EXECUTION: built}))
        widened = root / "role_policies"
        shutil.copytree(ROLE_DIRECTORY, widened)
        writer = widened / "writer.json"
        text = writer.read_text(encoding="utf-8")
        writer.write_text(plant(text, '"s3:ListMultipartUploadParts"', '"s3:ListMultipartUploadParts",\n        "s3:PutObjectAcl"'), encoding="utf-8")
        record("a lakehouse role holds an S3 action that the project user's s3 policy lacks", check_roles(directory=widened))
    return results


def check_sizes() -> list[str]:
    """Each policy, rendered with the longest real values, stays under the managed policy limit."""
    rendered = load_policies(POLICY_DIRECTORY, LONGEST_VALUES)
    return [
        f"{name}: {non_whitespace_length(document)} characters over the {MAX_POLICY_CHARACTERS} limit"
        for name, document in rendered.items()
        if non_whitespace_length(document) > MAX_POLICY_CHARACTERS
    ]


def run_suite(output: Path) -> dict[str, Any]:
    """Evaluate the shipped policies, then every planted defect, and return the report."""
    started = time.perf_counter()
    operations = policy_cases.approved_requests()
    cases = list(policy_cases.all_cases(operations))
    documents = load_all()
    failures = evaluate_cases(documents, cases)
    context_checks = [
        {"label": label, "passed": dict(item.context) == expected, "expected": expected, "observed": dict(item.context)}
        for label, item, expected in policy_cases.request_context_cases()
    ]
    problems = check_inventory(documents, operations) + check_sizes() + check_names(documents) + check_name_source() + check_roles()
    name_checks = name_defects(documents)
    defects = []
    for description, (label_prefix, mutation) in policy_cases.planted_defects().items():
        mutated = copy.deepcopy(documents)
        mutation(mutated)
        caught = evaluate_cases(mutated, cases)
        inventory = check_inventory(mutated, operations)
        intended = [item["label"] for item in caught if item["label"].startswith(label_prefix)]
        defects.append(
            {
                "defect": description,
                "intended_case_prefix": label_prefix,
                "cases_failed": len(caught),
                "inventory_problems": len(inventory),
                "caught_by_intended_case": bool(intended),
                "intended_failing_cases": intended,
                "first_failing_cases": [item["label"] for item in caught[:3]],
                "caught": bool(intended),
            }
        )
    with tempfile.TemporaryDirectory(prefix="policy_script_defects_") as scratch:
        script_report = run_script_defects(Path(scratch))
    scripts_ok = script_report["baseline_passed"] and all(item["caught"] for item in script_report["defects"])
    names_ok = all(item["caught"] for item in name_checks) and all(
        item.get("right_values_pass_the_names_check", True)
        and item.get("identical_with_the_real_project_name", True)
        and item.get("caught_by_the_name_source_check", True)
        for item in name_checks
    )
    passed = (
        not failures and not problems and all(item["caught"] for item in defects) and names_ok and scripts_ok and all(item["passed"] for item in context_checks)
    )
    status = "passed" if passed else "failed"
    report = {
        "schema_version": 1,
        "scope": "per_service_iam_policies_offline",
        "status": status,
        "live_aws_tested": False,
        "started_at_utc": datetime.now(UTC).isoformat(),
        "code": code_revision(),
        "repeat_command": [".venv/bin/python", "-m", "scripts.infrastructure.run_policy_e2e", "--output-directory", "<NEW_EVIDENCE_DIRECTORY>"],
        "substituted_values": policy_cases.VALUES,
        "policy_sha256": {path.name: sha256(path.read_bytes()) for path in sorted(POLICY_DIRECTORY.glob("*_policy.json"))},
        "owner_policy_sha256": {path.name: sha256(path.read_bytes()) for path in sorted(OWNER_DIRECTORY.glob("*.json"))},
        "policies": {
            name: {"statements": len(document["Statement"]), "non_whitespace_characters": non_whitespace_length(document)}
            for name, document in documents.items()
        },
        "cases": {"total": len(cases), "allowed_expected": sum(case.allowed for case in cases), "refused_expected": sum(not case.allowed for case in cases)},
        "failures": failures,
        "request_context_checks": context_checks,
        "inventory_and_size_problems": problems,
        "planted_defects": defects,
        "planted_name_defects": name_checks,
        "script_tests": script_report,
        "seconds": round(time.perf_counter() - started, 2),
        "limits": (
            "Policy evaluation is modeled offline. RunTask context keys follow AWS documentation. "
            "Real AWS authorization, propagated-tag behavior and Terraform's requests are untested."
        ),
    }
    (output / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def main() -> None:
    """Run the suite and write the report to a new directory."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", type=Path, required=True, help="New directory for the report; an existing path is refused.")
    arguments = parser.parse_args()
    output = arguments.output_directory.expanduser().absolute()
    try:
        output.mkdir(mode=0o700, parents=True, exist_ok=False)
    except FileExistsError:
        parser.error("Output directory already exists; choose a new directory.")
    report = run_suite(output)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    logger = logging.getLogger(__name__)
    logger.info(
        "event=policy_e2e status=%s cases=%s failures=%s problems=%s defects_caught=%s/%s",
        report["status"],
        report["cases"]["total"],
        len(report["failures"]),
        len(report["inventory_and_size_problems"]),
        sum(item["caught"] for item in report["planted_defects"]),
        len(report["planted_defects"]),
    )
    for item in report["failures"][:20]:
        logger.error("failed case: %s -> %s %s", item["label"], item["effect"], item["statements"])
    for item in report["request_context_checks"]:
        if not item["passed"]:
            logger.error("request context differs from AWS documentation: %s", item["label"])
    for problem in report["inventory_and_size_problems"][:20]:
        logger.error("problem: %s", problem)
    for item in report["planted_defects"]:
        if not item["caught"]:
            logger.error("planted defect not caught: %s", item["defect"])
    for item in report["planted_name_defects"]:
        if not item["caught"]:
            logger.error("planted name defect not caught: %s", item["defect"])
    scripts = report["script_tests"]
    logger.info(
        "event=policy_script_tests baseline_passed=%s defects_caught=%s/%s",
        scripts["baseline_passed"],
        sum(item["caught"] for item in scripts["defects"]),
        len(scripts["defects"]),
    )
    for item in scripts["defects"]:
        if not item["caught"]:
            logger.error("planted script defect not caught: %s %s", item["defect"], item.get("problem", ""))
    if report["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
