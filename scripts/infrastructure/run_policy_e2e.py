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
from scripts.infrastructure import policy_cases, secret_store
from scripts.infrastructure.policy_evaluator import Decision, Request, allowed_actions, as_list, decide, decide_bounded, load_policies
from scripts.infrastructure.policy_script_defects import run_script_defects
from scripts.process import run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
POLICY_DIRECTORY = REPO_ROOT / "infra" / "iam" / "policies"
OWNER_DIRECTORY = REPO_ROOT / "infra" / "iam" / "owner_policies"
ROLE_DIRECTORY = REPO_ROOT / "infra" / "iam" / "role_policies"
# Outside ``policies/``: the group tooling attaches every file there, and the administrator attaches these at bootstrap.
BOUNDARY_DIRECTORY = REPO_ROOT / "infra" / "iam" / "boundary"
BOUNDARY_FILE = "healthcare_hai_prediction_role_boundary_policy.json"
DELIVERY_BOUNDARY_FILE = "healthcare_hai_prediction_delivery_boundary_policy.json"
VARIABLES = REPO_ROOT / "infra" / "iam" / "variables.tf"
GITHUB_OIDC = REPO_ROOT / "infra" / "iam" / "github_oidc.tf"
IAM_DIRECTORY = REPO_ROOT / "infra" / "iam"
BRONZE_TABLES = REPO_ROOT / "config" / "lakehouse" / "bronze_tables.json"
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


def hcl_list(text: str, name: str) -> list[str]:
    """A list of strings that a Terraform file writes out in one line, such as ``lakehouse_read_prefixes = ["a/", "b/"]``."""
    found = re.findall(rf"^\s*{name}\s*=\s*(\[[^\]\n]*\])", text, re.MULTILINE)
    if len(found) != 1:
        raise RuntimeError(f"{name} must be written once as a one-line list")
    return [str(item) for item in json.loads(found[0])]


def variable_default(name: str) -> str:
    """The string default of one Terraform variable of the IAM stack."""
    found = [match for path in sorted(IAM_DIRECTORY.glob("*variables.tf")) for match in re.findall(
        rf'variable "{name}" \{{[^}}]*?default\s*=\s*"([^"]*)"', path.read_text(encoding="utf-8"), re.DOTALL)]  # fmt: skip
    if len(found) != 1:
        raise RuntimeError(f"the default of {name} must be written once")
    return str(found[0])


def render_role_template(path: Path, inputs: dict[str, str], expressions: dict[str, list[str]] | None = None) -> dict[str, Any]:
    """Render a role policy the way Terraform ``templatefile`` does with the stack's inputs.

    ``inputs`` replaces the placeholders the policy renderer does not know with their text; ``expressions`` replaces a whole
    ``${jsonencode(...)}`` expression with its JSON list. Each must occur in the file, or a changed template would go unchecked.
    """
    text = path.read_text(encoding="utf-8")
    for expression, values in (expressions or {}).items():
        text = plant(text, expression, json.dumps(values))
    for key, value in inputs.items():
        if "${" + key + "}" not in text:
            raise RuntimeError(f"{path.name} holds no ${{{key}}}")
        text = text.replace("${" + key + "}", value)
    return render_template(text, policy_cases.VALUES)


def load_roles() -> dict[str, dict[str, Any]]:
    """The policies of every role the IAM stack creates, keyed ``role/<role>/<policy>``, with the inputs the stack passes.

    Prepared and unattached role policies (the bronze and qualification deltas) are included, so the boundary covers them before they are attached.
    The qualification collection is the first mapped one, because the variable default is empty until the lead selects one.
    """
    roles_tf = (IAM_DIRECTORY / "roles.tf").read_text(encoding="utf-8")
    bucket = f"arn:aws:s3:::{policy_cases.B}"
    owner = OWNER_DIRECTORY
    lakehouse_inputs = {"DATA_BUCKET_NAME": policy_cases.B}
    expressions: dict[str, dict[str, list[str]]] = {}
    for kind in ("READ", "WRITE"):
        listed = hcl_list(roles_tf, f"lakehouse_{kind.lower()}_prefixes")
        expressions[kind] = {
            f'${{jsonencode([for prefix in {kind}_PREFIXES : "arn:aws:s3:::${{DATA_BUCKET_NAME}}/${{prefix}}*"])}}': [
                f"{bucket}/{prefix}*" for prefix in listed
            ],
            f'${{jsonencode(flatten([for prefix in {kind}_PREFIXES : [trimsuffix(prefix, "/"), "${{prefix}}*"]]))}}': [
                item for prefix in listed for item in (prefix.removesuffix("/"), f"{prefix}*")
            ],
        }
    namespaces = hcl_list((IAM_DIRECTORY / "bronze_writer.tf").read_text(encoding="utf-8"), "bronze_namespaces")
    test_namespaces = [f"{name}_e2e" for name in namespaces]
    collections = sorted({str(table["collection"]) for table in json.loads(BRONZE_TABLES.read_text(encoding="utf-8"))["tables"]})
    qualification = collections[0]

    def objects(names: list[str]) -> str:
        return json.dumps([f"{bucket}/lakehouse/{name}/*" for name in names])

    def prefixes(names: list[str]) -> str:
        return json.dumps([f"lakehouse/{name}/*" for name in names])

    def parameters(*variables: str) -> str:
        return json.dumps([f"arn:aws:ssm:{policy_cases.R}:{policy_cases.A}:parameter{variable_default(name)}" for name in variables])

    bronze = {"BUCKET_ARN": bucket, "TABLE_OBJECT_ARNS": objects(namespaces), "TABLE_LIST_PREFIXES": prefixes(namespaces),
              "SOURCE_OBJECT_ARNS": json.dumps([f"{bucket}/{name}/*" for name in collections]),
              "SOURCE_LIST_PREFIXES": json.dumps([f"{name}/*" for name in collections])}  # fmt: skip
    bronze_test = {**bronze, "TABLE_OBJECT_ARNS": objects(test_namespaces), "TABLE_LIST_PREFIXES": prefixes(test_namespaces),
                   "SOURCE_OBJECT_ARNS": json.dumps([f"{bucket}/{qualification}/*"]), "SOURCE_LIST_PREFIXES": json.dumps([f"{qualification}/*"])}  # fmt: skip
    e2e_names = ["silver_e2e", "gold_e2e", "features_e2e", *test_namespaces]
    e2e = {"BUCKET_ARN": bucket, "TEST_OBJECT_ARNS": objects(e2e_names), "TEST_LIST_PREFIXES": prefixes(e2e_names),
           "BRONZE_TEST_OBJECT_ARNS": objects(test_namespaces), "BRONZE_TEST_LIST_PREFIXES": prefixes(test_namespaces),
           "SOURCE_OBJECT_ARNS": json.dumps([f"{bucket}/{qualification}/*"]), "SOURCE_LIST_PREFIXES": json.dumps([f"{qualification}/*"]),
           "E2E_PARAMETER_ARNS": parameters("aws_polaris_e2e_client_id_name", "aws_polaris_e2e_client_secret_name")}  # fmt: skip

    def read(name: str, inputs: dict[str, str] | None = None) -> dict[str, Any]:
        return render_role_template(owner / f"{name}.json", inputs or {})

    # Terraform passes TASK_USER_ID as the IAM policy variable ${aws:userid}, which IAM fills in per role session.
    stage = read("job_stage_read", {"TASK_USER_ID": "${aws:userid}"})
    return {
        "role/lakehouse_reader/access": render_role_template(ROLE_DIRECTORY / "reader.json", lakehouse_inputs, expressions["READ"]),
        "role/lakehouse_writer/access": render_role_template(ROLE_DIRECTORY / "writer.json", lakehouse_inputs, expressions["WRITE"]),
        "role/ecs_host/container_host": read("ecs_host"),
        "role/polaris_task/polaris_task": read("polaris_task"),
        "role/job_execution/job_execution": read("job_execution"),
        "role/job_execution/inject_qualification_pair": read("job_writer_e2e_execution", {"E2E_PARAMETER_ARNS": e2e["E2E_PARAMETER_ARNS"]}),
        "role/job_execution/bronze_execution_delta": read(
            "bronze_execution", {"BRONZE_PARAMETER_ARNS": parameters("aws_polaris_bronze_client_id_name", "aws_polaris_bronze_client_secret_name")}
        ),
        "role/job_offline_task/stage_read": stage,
        "role/job_seal_task/stage_read": stage,
        "role/job_seal_task/no_input_tree": read("job_seal_deny"),
        "role/job_read_task/stage_read": stage,
        "role/job_read_task/source_read": read("job_source_read"),
        "role/job_read_task/qualification_read": read(
            "job_reader_e2e",
            {key: e2e[key] for key in ("BUCKET_ARN", "TEST_OBJECT_ARNS", "TEST_LIST_PREFIXES", "BRONZE_TEST_OBJECT_ARNS", "BRONZE_TEST_LIST_PREFIXES")},
        ),  # fmt: skip
        "role/job_try_writer/try_outputs": read("job_try_writer"),
        "role/job_write_bronze_task/bronze_objects_and_versioned_sources": read("bronze_writer_storage", bronze),
        "role/job_write_e2e_task/qualification_objects_and_versioned_source": read(
            "job_writer_e2e_storage",
            {key: e2e[key] for key in ("BUCKET_ARN", "TEST_OBJECT_ARNS", "TEST_LIST_PREFIXES", "SOURCE_OBJECT_ARNS", "SOURCE_LIST_PREFIXES")},
        ),  # fmt: skip
        "role/job_write_e2e_task/qualification_storage_delta": read("bronze_writer_storage", bronze_test),
        "role/job_write_e2e_task/stage_read": stage,
    }


# What the plan role never reads, the same list as ``github_plan_unreadable`` in infra/iam/github_oidc.tf.
PLAN_UNREADABLE = frozenset(
    {
        "secretsmanager:GetSecretValue", "ssm:GetParameter", "ssm:GetParameters", "logs:GetLogEvents", "logs:FilterLogEvents", "ecr:GetAuthorizationToken",
        "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ec2:GetConsoleOutput", "ec2:GetConsoleScreenshot", "ec2:GetPasswordData", "logs:GetLogRecord",
        "logs:GetQueryResults", "s3:ListBucketVersions", "s3:ListBucketMultipartUploads", "s3:ListMultipartUploadParts",
    }
)  # fmt: skip
PLAN_READ_ACTION = re.compile(r"^[a-z0-9]+:(Describe|List|Get|BatchGet)")


def plan_read_statements(documents: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """The statements of the plan role's reads, built the way ``github_plan_read_statements`` in infra/iam/github_oidc.tf builds them.

    Each approved describe, list or get action keeps the resources and conditions of the project policy statement that allows it, less the actions that
    return a secret value, log content, an image layer or a listing of versions and uploads, and with the region condition added.
    """
    statements = []
    for name, document in sorted(documents.items()):
        if not (name.startswith("healthcare_hai_prediction_") and name.endswith("_policy")):
            continue
        for statement in document["Statement"]:
            actions = sorted(
                action
                for action in dict.fromkeys(as_list(statement["Action"]))
                if PLAN_READ_ACTION.match(action) and action not in PLAN_UNREADABLE and not action.startswith("s3:GetObject")
            )
            if statement["Effect"] != "Allow" or "NotResource" in statement or not actions:
                continue
            condition = dict(statement.get("Condition", {}))
            condition["StringEquals"] = {**condition.get("StringEquals", {}), "aws:RequestedRegion": policy_cases.R}
            statements.append(
                {"Sid": f"Read{template_service(name)}{statement['Sid']}", "Effect": "Allow", "Action": actions, "Resource": as_list(statement["Resource"]),
                 "Condition": condition}
            )  # fmt: skip
    return statements


def load_delivery(documents: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """The policies of the three GitHub delivery roles, keyed ``delivery/<role>/<policy>``, with the values the IAM stack passes.

    The statements follow ``github_statements`` in infra/iam/github_oidc.tf. The deploy role's service and task role names are the test inputs
    ``policy_cases.DEPLOY_SERVICE`` and ``policy_cases.DEPLOY_STREAMLIT``. No delivery role applies Terraform or holds an attached policy.
    """
    state_read = [
        {"Sid": "ReadStateObjects", "Effect": "Allow", "Action": ["s3:GetObject"], "Resource": list(policy_cases.STATE_KEYS)},
        {"Sid": "ListStateKeys", "Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": [policy_cases.BUCKET],
         "Condition": {"StringLike": {"s3:prefix": [f"{policy_cases.STATE_PREFIX}/*"]}}},
    ]  # fmt: skip
    repository = f"arn:aws:ecr:{policy_cases.R}:{policy_cases.A}:repository/{policy_cases.P}_*_{policy_cases.E}"
    statements = {
        "image_push": [
            {"Sid": "GetRegistryToken", "Effect": "Allow", "Action": ["ecr:GetAuthorizationToken"], "Resource": ["*"]},
            {"Sid": "PushProjectImages", "Effect": "Allow", "Resource": [repository], "Action": [
                "ecr:BatchCheckLayerAvailability", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart", "ecr:CompleteLayerUpload", "ecr:PutImage",
                "ecr:BatchGetImage", "ecr:DescribeImages", "ecr:DescribeRepositories"]},
        ],
        "deploy": [
            {"Sid": "ReadCluster", "Effect": "Allow", "Resource": ["*"],
             "Action": ["ecs:DescribeClusters", "ecs:DescribeServices", "ecs:DescribeTasks", "ecs:ListTasks"]},
            {"Sid": "DescribeTaskDefinitions", "Effect": "Allow", "Action": ["ecs:DescribeTaskDefinition"], "Resource": ["*"]},
            {"Sid": "UpdateNamedServices", "Effect": "Allow", "Action": ["ecs:UpdateService"],
             "Resource": [policy_cases.DEPLOY_SERVICE, policy_cases.DEPLOY_STREAMLIT]},
        ],
        "terraform_plan": [
            {"Sid": "NeverReadObjectsExceptStateKeys", "Effect": "Deny", "NotResource": list(policy_cases.STATE_KEYS),
             "Action": ["s3:GetObject", "s3:GetObjectVersion", "s3:GetObjectAttributes", "s3:GetObjectTagging"]},
            *plan_read_statements(documents),
            *state_read,
        ],
    }  # fmt: skip
    loaded = {f"delivery/{role}/access": {"Version": "2012-10-17", "Statement": items} for role, items in statements.items()}
    return loaded


def load_all() -> dict[str, dict[str, Any]]:
    """The 13 project user policies by template stem, the policies the IAM stack renders, as ``owner_<name>``, the role boundary, the delivery
    boundary, every role policy as ``role/<role>/<policy>`` and every delivery role policy as ``delivery/<role>/<policy>``."""
    documents = load_policies(POLICY_DIRECTORY, policy_cases.VALUES)
    documents[policy_cases.BOUNDARY] = render_template((BOUNDARY_DIRECTORY / BOUNDARY_FILE).read_text(encoding="utf-8"), policy_cases.VALUES)
    documents["owner_snapshots"] = render_template((OWNER_DIRECTORY / "snapshots.json").read_text(encoding="utf-8"), policy_cases.VALUES)
    documents[TASK_EXECUTION] = render_task_execution(OWNER_DIRECTORY)
    documents[policy_cases.DELIVERY_BOUNDARY] = render_template((BOUNDARY_DIRECTORY / DELIVERY_BOUNDARY_FILE).read_text(encoding="utf-8"), policy_cases.VALUES)
    return {**documents, **load_roles(), **load_delivery(documents)}


def evaluate_cases(documents: dict[str, dict[str, Any]], cases: list[policy_cases.Case]) -> list[dict[str, Any]]:
    """Run every case; return the failures with the decision, the statement and the expected outcome.

    The boundary is shared by every role, so it cannot keep one role inside its own scope: a role's refusal must also hold under the role's own
    policies alone. The wide roles are the exception; only their boundary refuses them.
    """
    failures = []
    held = policy_cases.principals(documents)
    bounds = policy_cases.boundaries(documents)
    for case in cases:
        boundary = bounds.get(case.principal)
        decision: Decision = decide(held[case.principal], case.request) if boundary is None else decide_bounded(held[case.principal], boundary, case.request)
        if boundary is not None and case.principal not in (policy_cases.WIDE_ROLE, policy_cases.DELIVERY_WIDE) and not case.allowed and not decision.allowed:
            own = decide(held[case.principal], case.request)
            decision = own if own.allowed else decision
        wrong_effect = decision.allowed != case.allowed
        wrong_reason = case.sid is not None and case.sid not in decision.statements
        if wrong_effect or wrong_reason:
            failures.append({"label": case.label, "action": case.request.action, "resource": case.request.resource, "expected_allowed": case.allowed,
                             "expected_statement": case.sid, "effect": decision.effect, "statements": list(decision.statements)})  # fmt: skip
    return failures


# Owner decision D18: GitHub plans and the owner applies, so the roles are these three. A role that applies, or a policy attached beside the inline
# policy, would hold permissions the coverage cases never see, and the boundary could cap them without anyone noticing.
DELIVERY_ROLES = frozenset({"image_push", "deploy", "terraform_plan"})
ATTACHMENT_RESOURCE = re.compile(r'^resource "(aws_iam_(?:role_policy_attachments?_exclusive|role_policy_attachment|policy_attachment))" "(\w+)"', re.MULTILINE)


def check_delivery_roles(path: Path = GITHUB_OIDC) -> list[str]:
    """The delivery roles are exactly the plan, push and deploy roles, and no policy is attached to any of them."""
    text = path.read_text(encoding="utf-8")
    block = re.search(r"^  github_roles = var\.github_oidc_enabled \? \{\n(.*?)^  \} : \{\}", text, re.MULTILINE | re.DOTALL)
    if block is None:
        return [f"{path.name}: the github_roles map is not found"]
    roles = set(re.findall(r"^    (\w+) = \{$", block.group(1), re.MULTILINE))
    problems = []
    if roles != DELIVERY_ROLES:
        problems.append(f"{path.name}: delivery roles {sorted(roles)} differ from {sorted(DELIVERY_ROLES)}: GitHub plans and the owner applies")
    parts = re.split(r"^(?=resource |output |locals )", text, flags=re.MULTILINE)
    for part in parts:
        match = ATTACHMENT_RESOURCE.match(part)
        if match and "aws_iam_role.github" in part:
            problems.append(f"{path.name}: {match.group(1)}.{match.group(2)} attaches a policy to a delivery role; load it into the coverage cases first")
    return problems


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
OPENMETADATA_PLACEHOLDERS = tuple(f"{name.upper()}_NAME" for name in policy_cases.OPENMETADATA_PASSWORDS)
OPENMETADATA_PASSWORDS = ("ssm", policy_cases.OPENMETADATA_STATEMENT)
OPENMETADATA_DENY = ("ssm", policy_cases.OPENMETADATA_DENY)
TASK_EXECUTION = "owner_task_execution"
STS = ("sts", "AssumeLakehouseRolesOnly")
INJECT = (TASK_EXECUTION, "InjectNamedParameters")


def role_arn(name: str) -> str:
    return f"arn:aws:iam::{policy_cases.A}:role/{name}"


def expected_grants(names: dict[str, Any]) -> dict[tuple[str, str], set[str]]:
    """The resources of the statements that carry a name from a variable (secrets, parameters, lakehouse roles), for these names."""
    return {
        ("secretsmanager", "ReadSharedApiKeys"): {secret_arn(name) for name in names["SHARED_SECRET_NAME"]},
        ("ssm", "ManageProjectParameters"): {parameter_arn(name) for name in names["MANAGED_PARAMETER_NAME"]},
        ("ssm", "ReadAirflowAdminParameter"): {parameter_arn(names["AIRFLOW_ADMIN_SECRET_NAME"])},
        STS: {role_arn(names[key]) for key in ROLE_PLACEHOLDERS},
        INJECT: {parameter_arn(names[key]) for key in EXECUTION_PLACEHOLDERS},
        AWS_PAIRS: {parameter_arn(names[key]) for key in AWS_POLARIS_PLACEHOLDERS},
        OPENMETADATA_PASSWORDS: {parameter_arn(names[key]) for key in OPENMETADATA_PLACEHOLDERS},
        OPENMETADATA_DENY: {parameter_arn(names[key]) for key in OPENMETADATA_PLACEHOLDERS},
    }


def statement_resources(documents: dict[str, dict[str, Any]], key: tuple[str, str]) -> set[str]:
    """The resources of one statement, from a project user policy (by service) or the task execution policy."""
    service, sid = key
    return resources(documents[service if service.startswith("owner_") else f"healthcare_hai_prediction_{service}_policy"], sid)


def render_task_execution(directory: Path, names: dict[str, Any] | None = None, values: dict[str, str] | None = None) -> dict[str, Any]:
    """The task execution policy with monitoring on: the base template plus the one Grafana statement, joined under ``InjectNamedParameters``.

    The IAM stack attaches the Grafana statement as a separate policy and only while monitoring is on. The cases evaluate the largest grant, so the
    parameter checks cover all nine names. ``check_monitoring_gate`` holds the base template to eight names and the monitoring template to one.
    """
    values = values or policy_cases.VALUES
    document = render_template((directory / "task_execution.json").read_text(encoding="utf-8"), values, names)
    monitoring = render_template((directory / "task_execution_monitoring.json").read_text(encoding="utf-8"), values, names)
    grant = next(statement for statement in document["Statement"] if statement["Sid"] == "InjectNamedParameters")
    grant["Resource"] = [*as_list(grant["Resource"]), *(resource for statement in monitoring["Statement"] for resource in as_list(statement["Resource"]))]
    return document


def with_task_execution(documents: dict[str, dict[str, Any]], names: dict[str, Any], directory: Path = OWNER_DIRECTORY) -> dict[str, dict[str, Any]]:
    """The documents with the task execution policy rendered from the same names, unless they already hold one."""
    if TASK_EXECUTION in documents:
        return documents
    return {**documents, TASK_EXECUTION: render_task_execution(directory, names)}


def check_monitoring_gate(directory: Path = OWNER_DIRECTORY) -> list[str]:
    """The base task execution template names no Grafana parameter, and the monitoring template names exactly that one."""
    base = (directory / "task_execution.json").read_text(encoding="utf-8")
    monitoring = render_template((directory / "task_execution_monitoring.json").read_text(encoding="utf-8"), policy_cases.VALUES)
    problems = []
    if "GRAFANA" in base.upper():
        problems.append("the base task execution template names the Grafana parameter, so the IAM stack changes while monitoring is off")
    granted = {resource for statement in monitoring["Statement"] for resource in as_list(statement["Resource"])}
    wanted = {f"arn:aws:ssm:{policy_cases.R}:{policy_cases.A}:parameter/{policy_cases.PARAMETER_FOLDER}/grafana_admin_password"}
    if granted != wanted:
        problems.append(f"the monitoring template grants {sorted(granted)}, not exactly the Grafana parameter")
    if any(as_list(statement["Action"]) != ["ssm:GetParameters"] for statement in monitoring["Statement"]):
        problems.append("the monitoring template grants an action other than ssm:GetParameters")
    return problems


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
    problems = check_monitoring_gate()
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
    for placeholder, name in zip(OPENMETADATA_PLACEHOLDERS, policy_cases.OPENMETADATA_PASSWORDS, strict=True):
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
    allowed = (
        expected[("ssm", "ManageProjectParameters")]
        | expected[("ssm", "ReadAirflowAdminParameter")]
        | expected[INJECT]
        | expected[AWS_PAIRS]
        | expected[OPENMETADATA_PASSWORDS]
    )
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
        "a parameter name in the managed list drifts from its variable default",
        check_names(drift(documents, "ssm", "ManageProjectParameters", "redis_password", "redis_passwd")),
    )
    record(
        "another parameter name in the managed list drifts from its variable default",
        check_names(drift(documents, "ssm", "ManageProjectParameters", "postgres_password", "postgres_passwd")),
    )
    record("a managed parameter is missing from the managed list", check_names(dropped(documents, "ssm", "ManageProjectParameters", "marquez_db_password")))
    record(
        "another managed parameter is missing from the managed list", check_names(dropped(documents, "ssm", "ManageProjectParameters", "airflow_fernet_key"))
    )
    record(
        "the Airflow administrator password is missing from its read grant",
        check_names(dropped(documents, "ssm", "ReadAirflowAdminParameter", "airflow_admin")),
    )
    record(
        "a catalog password is missing from its grant",
        check_names(dropped(documents, "ssm", policy_cases.OPENMETADATA_STATEMENT, "openmetadata_db_password")),
    )
    record(
        "a catalog password name in its grant drifts from its variable default",
        check_names(drift(documents, "ssm", policy_cases.OPENMETADATA_STATEMENT, "openmetadata_admin_password", "openmetadata_admin_passwd")),
    )
    record("a shared API key is missing from the Secrets Manager grant", check_names(dropped(documents, "secretsmanager", "ReadSharedApiKeys", "jev_api_key")))
    record(
        "a shared API key name drifts from its variable default", check_names(drift(documents, "secretsmanager", "ReadSharedApiKeys", "hud_api_key", "hud_key"))
    )
    with tempfile.TemporaryDirectory(prefix="policy_name_defects_") as scratch:
        root = Path(scratch)
        original = VARIABLES.read_text(encoding="utf-8")
        for sibling in secret_store.OPENMETADATA_FILES:
            shutil.copy(VARIABLES.parent / sibling, root / sibling)

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
        names = terraform_names()["MANAGED_PARAMETER_NAME"]
        placeholder = '"arn:aws:ssm:${AWS_REGION}:${AWS_ACCOUNT_ID}:parameter${MANAGED_PARAMETER_NAME}"'
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
        built = render_task_execution(owner)
        record(
            "a task execution parameter is built from PROJECT_NAME, right for the real project name only",
            check_names({**documents, TASK_EXECUTION: built}),
        )
        real_owner = render_task_execution(owner, names_now, real_values)
        results[-1]["identical_with_the_real_project_name"] = not difference(
            "exec", statement_resources({TASK_EXECUTION: real_owner}, INJECT), expected_grants(names_now)[INJECT]
        )
        results[-1]["caught_by_the_name_source_check"] = bool(check_name_source(owner_directory=owner))
        template.write_text(plant(original_owner, "parameter${REDIS_PASSWORD_NAME}", "parameter/healthcare_hai_prediction/redis_password"), encoding="utf-8")
        record("a task execution parameter is written out in the template (right today)", check_name_source(owner_directory=owner))
        template.write_text(plant(original_owner, "parameter${REDIS_PASSWORD_NAME}", "parameter/healthcare_hai_prediction/redis_passwd"), encoding="utf-8")
        built = render_task_execution(owner)
        record("a task execution parameter is written out in the template and has drifted", check_names({**documents, TASK_EXECUTION: built}))
        template.write_text(
            plant(
                original_owner,
                'parameter${POLARIS_ROOT_SECRET_NAME}"',
                'parameter${POLARIS_ROOT_SECRET_NAME}",\n        "arn:aws:ssm:${AWS_REGION}:${AWS_ACCOUNT_ID}:parameter${GRAFANA_ADMIN_PASSWORD_NAME}"',
            ),
            encoding="utf-8",
        )
        record("the Grafana password is back in the base task execution template", check_monitoring_gate(owner))
        template.write_text(original_owner, encoding="utf-8")
        widened = root / "role_policies"
        shutil.copytree(ROLE_DIRECTORY, widened)
        writer = widened / "writer.json"
        text = writer.read_text(encoding="utf-8")
        writer.write_text(plant(text, '"s3:ListMultipartUploadParts"', '"s3:ListMultipartUploadParts",\n        "s3:PutObjectAcl"'), encoding="utf-8")
        record("a lakehouse role holds an S3 action that the project user's s3 policy lacks", check_roles(directory=widened))
        oidc = root / "github_oidc.tf"
        original_oidc = GITHUB_OIDC.read_text(encoding="utf-8")
        oidc.write_text(
            plant(
                original_oidc,
                "    terraform_plan = {\n      subjects",
                '    terraform_apply = {\n      subjects    = []\n      description = "x"\n    }\n    terraform_plan = {\n      subjects',
            ),
            encoding="utf-8",
        )
        record("a Terraform apply role is back among the delivery roles", check_delivery_roles(oidc))
        oidc.write_text(
            original_oidc
            + '\nresource "aws_iam_role_policy_attachment" "github_extra" {\n  role       = aws_iam_role.github["deploy"].name\n  policy_arn = "x"\n}\n',
            encoding="utf-8",
        )
        record("a policy is attached to a delivery role", check_delivery_roles(oidc))
    return results


def check_sizes() -> list[str]:
    """Each project user policy and the boundary, rendered with the longest real values, stay under the managed policy limit."""
    rendered = load_policies(POLICY_DIRECTORY, LONGEST_VALUES)
    for path in sorted(BOUNDARY_DIRECTORY.glob("*.json")):
        rendered[path.stem] = render_template(path.read_text(encoding="utf-8"), LONGEST_VALUES)
    return [
        f"{name}: {non_whitespace_length(document)} characters over the {MAX_POLICY_CHARACTERS} limit"
        for name, document in rendered.items()
        if non_whitespace_length(document) > MAX_POLICY_CHARACTERS
    ]


def run_suite(output: Path) -> dict[str, Any]:
    """Evaluate the shipped policies, then every planted defect, and return the report."""
    started = time.perf_counter()
    operations = policy_cases.approved_requests()
    documents = load_all()
    cases = [*policy_cases.all_cases(operations), *policy_cases.coverage_cases(documents)]
    failures = evaluate_cases(documents, cases)
    context_checks = [
        {"label": label, "passed": dict(item.context) == expected, "expected": expected, "observed": dict(item.context)}
        for label, item, expected in policy_cases.request_context_cases()
    ]
    problems = check_inventory(documents, operations) + check_sizes() + check_names(documents) + check_name_source() + check_roles() + check_delivery_roles()
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
