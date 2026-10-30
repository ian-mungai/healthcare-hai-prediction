"""Exercise the grants of the lean job broker role without AWS calls.

The role may start, stop and describe the named job families only, pass the roles of those families and the job execution role, read the staged code
digest, the job logs and the record of the signed first publishes, and nothing else. Each grant is checked against requests written here, apart from the
rendered policy, and every scope mutation of the policy must fail at least one request. Run from the repository root:
``.venv/bin/python -m scripts.infrastructure.broker_policy_cases --output <NEW_DIRECTORY>``.
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path
from typing import Any

from scripts.infrastructure.policy_evaluator import Request, decide

ROOT = Path(__file__).resolve().parents[2]
TEMPLATE = ROOT / "infra/iam/owner_policies/broker_jobs.json"
PARAMETER_TEMPLATE = ROOT / "infra/iam/owner_policies/broker_parameter.json"
ACCOUNT = "111111111111"
REGION = "eu-west-1"
PROJECT = "example_project"
ENVIRONMENT = "dev"
BUCKET = "example-project-ci-bucket"
FAMILIES = ["gold", "silver", "train"]
CLUSTER = f"{PROJECT}_cluster_{ENVIRONMENT}"
CLUSTER_ARN = f"arn:aws:ecs:{REGION}:{ACCOUNT}:cluster/{CLUSTER}"
OTHER_CLUSTER_ARN = f"arn:aws:ecs:{REGION}:{ACCOUNT}:cluster/another_cluster"
SIGNED = "/healthcare_hai_prediction/first_publishes_signed"
BROKER_PARAMETER = "/healthcare_hai_prediction/broker_token"
LOG_GROUP = f"{PROJECT}_jobs_{ENVIRONMENT}"
Case = tuple[str, str, str, bool, dict[str, tuple[str, ...]]]


def role(name: str) -> str:
    return f"arn:aws:iam::{ACCOUNT}:role/{PROJECT}_{name}_{ENVIRONMENT}"


def definition(family: str, revision: str = "7") -> str:
    return f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{PROJECT}_{ENVIRONMENT}_job_{family}:{revision}"


def task(cluster: str = CLUSTER, identifier: str = "0123456789abcdef") -> str:
    return f"arn:aws:ecs:{REGION}:{ACCOUNT}:task/{cluster}/{identifier}"


def values(families: list[str]) -> dict[str, str]:
    """The template inputs, built as infra/iam/broker.tf builds them."""
    roles = {"train": "job_train_task", "silver": "job_write_silver_task", "gold": "job_write_gold_task", "features": "job_write_features_task"}
    return {
        "AWS_REGION": REGION,
        "AWS_ACCOUNT_ID": ACCOUNT,
        "PROJECT_TAG": PROJECT,
        "CLUSTER_ARN": CLUSTER_ARN,
        "TASK_ARN_PATTERN": f"arn:aws:ecs:{REGION}:{ACCOUNT}:task/{CLUSTER}/*",
        "LOG_GROUP_NAME": LOG_GROUP,
        "DATA_BUCKET_NAME": BUCKET,
        "SIGNED_PARAMETER_NAME": SIGNED,
        "BROKER_TOKEN_NAME": BROKER_PARAMETER,
        "FAMILIES": json.dumps(sorted(families)),
        "TASK_DEFINITION_ARNS": json.dumps(
            [f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{PROJECT}_{ENVIRONMENT}_job_{family}:*" for family in sorted(families)]
        ),
        "ROLE_ARNS": json.dumps(sorted({role("job_execution"), *(role(roles[family]) for family in families)})),
    }


def render(path: Path, families: list[str]) -> dict[str, Any]:
    text = path.read_text()
    for name, value in values(families).items():
        text = text.replace("${" + name + "}", value)
    document: dict[str, Any] = json.loads(text)
    return document


def run_context(family: str, **overrides: tuple[str, ...]) -> dict[str, tuple[str, ...]]:
    context = {
        "ecs:cluster": (CLUSTER_ARN,),
        "aws:RequestTag/Project": (PROJECT,),
        "aws:RequestTag/HaiJob": (family,),
        "aws:TagKeys": ("Project", "HaiJob"),
    }
    context.update(overrides)
    return context


def tagged(job: str | None) -> dict[str, tuple[str, ...]]:
    return {} if job is None else {"aws:ResourceTag/HaiJob": (job,)}


def cases() -> list[Case]:
    """Each request the broker role must be allowed or refused, independent of the rendered resources."""
    pass_context: dict[str, tuple[str, ...]] = {"iam:PassedToService": ("ecs-tasks.amazonaws.com",)}
    rows: list[Case] = []

    def add(label: str, action: str, resource: str, expected: bool, context: dict[str, tuple[str, ...]] | None = None) -> None:
        rows.append((label, action, resource, expected, context or {}))

    for family in FAMILIES:
        add(f"run the {family} definition", "ecs:RunTask", definition(family), True, run_context(family))
    add("run another revision of train", "ecs:RunTask", definition("train", "12"), True, run_context("train"))
    for family in ("features", "drift", "plain_silver", "write_gold", "train_extra"):
        add(f"run the unnamed {family} definition", "ecs:RunTask", definition(family), False, run_context("train"))
    add(
        "run a service definition",
        "ecs:RunTask",
        f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{PROJECT}_{ENVIRONMENT}_polaris:1",
        False,
        run_context("train"),
    )
    add("run in another cluster", "ecs:RunTask", definition("train"), False, run_context("train", **{"ecs:cluster": (OTHER_CLUSTER_ARN,)}))
    add(
        "run without the Project tag",
        "ecs:RunTask",
        definition("train"),
        False,
        {k: v for k, v in run_context("train").items() if k != "aws:RequestTag/Project"},
    )
    add("run with another Project tag", "ecs:RunTask", definition("train"), False, run_context("train", **{"aws:RequestTag/Project": ("other_project",)}))
    add("run without the job tag", "ecs:RunTask", definition("train"), False, {k: v for k, v in run_context("train").items() if k != "aws:RequestTag/HaiJob"})
    add("run with an unnamed job tag", "ecs:RunTask", definition("train"), False, run_context("features"))
    add("run with an extra tag key", "ecs:RunTask", definition("train"), False, run_context("train", **{"aws:TagKeys": ("Project", "HaiJob", "Extra")}))
    add("start a task by other means", "ecs:StartTask", definition("train"), False, run_context("train"))
    add("tag a task at RunTask", "ecs:TagResource", task(), True, {"ecs:CreateAction": ("RunTask",)})
    add("tag a task at another time", "ecs:TagResource", task(), False)
    add("tag a task in another cluster", "ecs:TagResource", task(OTHER_CLUSTER_ARN.rsplit("/", 1)[-1]), False, {"ecs:CreateAction": ("RunTask",)})
    for action in ("ecs:StopTask", "ecs:DescribeTasks"):
        for family in FAMILIES:
            add(f"{action} on a {family} task", action, task(), True, tagged(family))
        add(f"{action} on a task without the job tag", action, task(), False, tagged(None))
        add(f"{action} on an unnamed job's task", action, task(), False, tagged("features"))
        add(f"{action} in another cluster", action, task("another_cluster"), False, tagged("train"))
    add("list tasks in the cluster", "ecs:ListTasks", "*", True, {"ecs:cluster": (CLUSTER_ARN,)})
    add("list tasks in another cluster", "ecs:ListTasks", "*", False, {"ecs:cluster": (OTHER_CLUSTER_ARN,)})
    add("list tasks without a cluster", "ecs:ListTasks", "*", False)
    for name in ("job_execution", "job_train_task", "job_write_silver_task", "job_write_gold_task"):
        add(f"pass {name} to ECS tasks", "iam:PassRole", role(name), True, pass_context)
        add(f"pass {name} to ECS itself", "iam:PassRole", role(name), False, {"iam:PassedToService": ("ecs.amazonaws.com",)})
        add(f"pass {name} without a service", "iam:PassRole", role(name), False)
    for name in (
        "job_write_features_task",
        "job_drift_task",
        "job_write_bronze_task",
        "job_read_task",
        "job_try_writer",
        "task_execution",
        "broker_task",
        "mlflow_task",
        "host",
        "job_train_task_extra",
    ):
        add(f"pass the unnamed role {name}", "iam:PassRole", role(name), False, pass_context)
    add("read a code digest", "s3:GetObject", f"arn:aws:s3:::{BUCKET}/lakehouse/run_stage/20261030T100000Z-abcdef01/inputs/code.sha256", True)
    for key in ("code.tar.gz", "other.json", "../code.sha256"):
        add(f"read inputs/{key}", "s3:GetObject", f"arn:aws:s3:::{BUCKET}/lakehouse/run_stage/20261030T100000Z-abcdef01/inputs/{key}", False)
    add("read a gold table", "s3:GetObject", f"arn:aws:s3:::{BUCKET}/lakehouse/gold/table/data/part-0.parquet", False)
    add("read a digest in another bucket", "s3:GetObject", "arn:aws:s3:::another-bucket/lakehouse/run_stage/run/inputs/code.sha256", False)
    add("write a digest", "s3:PutObject", f"arn:aws:s3:::{BUCKET}/lakehouse/run_stage/20261030T100000Z-abcdef01/inputs/code.sha256", False)
    add("list the bucket", "s3:ListBucket", f"arn:aws:s3:::{BUCKET}", False)
    stream = f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:{LOG_GROUP}:log-stream:train/train/0123456789abcdef"
    add("read a job log stream", "logs:GetLogEvents", stream, True)
    add("read a stream of another group", "logs:GetLogEvents", stream.replace(LOG_GROUP, f"{PROJECT}_mlflow_{ENVIRONMENT}"), False)
    add("write a job log stream", "logs:PutLogEvents", stream, False)
    add("read the signed record", "ssm:GetParameter", f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter{SIGNED}", True)
    add("write the signed record", "ssm:PutParameter", f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter{SIGNED}", False)
    add("delete the signed record", "ssm:DeleteParameter", f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter{SIGNED}", False)
    add("read the broker token", "ssm:GetParameter", f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter{BROKER_PARAMETER}", False)
    add("read the MLflow password", "ssm:GetParameter", f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter/healthcare_hai_prediction/mlflow_admin_password", False)
    for action in ("ecs:UpdateService", "ecs:DeleteService", "ecs:CreateService", "ecs:RegisterTaskDefinition", "ecs:DeregisterTaskDefinition"):
        add(f"no {action}", action, f"arn:aws:ecs:{REGION}:{ACCOUNT}:service/{CLUSTER}/{PROJECT}_{ENVIRONMENT}_polaris", False)
    add("no role creation", "iam:CreateRole", role("anything"), False)
    return rows


EXTRA_ACTIONS = ("ecs:UpdateService", "ecs:DeleteService", "s3:DeleteObject", "iam:CreateRole", "ssm:PutParameter", "logs:PutLogEvents")


def satisfying_context(sid: str) -> dict[str, tuple[str, ...]]:
    """A request context that meets every condition of the statement, so only the action and resource decide."""
    contexts: dict[str, dict[str, tuple[str, ...]]] = {
        "RunNamedJobFamilies": run_context("train"),
        "TagTasksItStarts": {"ecs:CreateAction": ("RunTask",)},
        "StopAndDescribeNamedJobTasks": tagged("train"),
        "ListClusterTasks": {"ecs:cluster": (CLUSTER_ARN,)},
        "PassNamedJobRoles": {"iam:PassedToService": ("ecs-tasks.amazonaws.com",)},
    }
    return contexts.get(sid, {})


def extra_action_cases(document: dict[str, Any]) -> list[Case]:
    """On each statement's own resources, with its conditions met, every action it does not list stays refused."""
    rows: list[Case] = []
    for statement in document["Statement"]:
        for resource in as_list(statement["Resource"]):
            concrete = resource.replace("*", "7") if resource != "*" else "*"
            for action in EXTRA_ACTIONS:
                if action not in as_list(statement["Action"]):
                    rows.append((f"{statement['Sid']}: no {action} on {concrete}", action, concrete, False, satisfying_context(statement["Sid"])))
    return rows


def token_cases() -> list[Case]:
    """The execution role's parameter grant: the broker token and nothing else."""
    arn = f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter"
    return [
        ("inject the broker token", "ssm:GetParameters", f"{arn}{BROKER_PARAMETER}", True, {}),
        ("inject the signed record", "ssm:GetParameters", f"{arn}{SIGNED}", False, {}),
        ("inject the MLflow password", "ssm:GetParameters", f"{arn}/healthcare_hai_prediction/mlflow_admin_password", False, {}),
        ("put the broker token", "ssm:PutParameter", f"{arn}{BROKER_PARAMETER}", False, {}),
    ]


def evaluate(document: dict[str, Any], rows: list[Case]) -> list[dict[str, Any]]:
    observations = []
    for label, action, resource, expected, context in rows:
        decision = decide([document], Request(action, resource, context))
        observations.append(
            {"case": label, "expected": expected, "observed": decision.allowed, "passed": decision.allowed == expected, "statements": decision.statements}
        )
    return observations


def mutations(document: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Every scope widening of every statement: a wildcard resource, no condition, an extra action."""
    found = []
    for statement in document["Statement"]:
        variants: list[tuple[str, str, Any]] = [("add_action", "Action", [*as_list(statement["Action"]), "ecs:UpdateService"])]
        if statement["Resource"] != "*":
            variants.append(("widen_resource", "Resource", "*"))
        if "Condition" in statement:
            variants.append(("drop_condition", "Condition", None))
        for label, field, value in variants:
            changed = copy.deepcopy(document)
            target = next(item for item in changed["Statement"] if item["Sid"] == statement["Sid"])
            if value is None:
                target.pop(field)
            else:
                target[field] = value
            found.append((f"{statement['Sid']}_{label}", changed))
    return found


def as_list(value: Any) -> list[Any]:
    return [value] if isinstance(value, str) else list(value)


def verify(output: Path) -> int:
    document = render(TEMPLATE, FAMILIES)
    parameter = render(PARAMETER_TEMPLATE, FAMILIES)
    document_rows = cases() + extra_action_cases(document)
    parameter_rows = token_cases() + extra_action_cases(parameter)
    checks = evaluate(document, document_rows) + evaluate(parameter, parameter_rows)
    defects = []
    for name, changed in mutations(document):
        failures = [item["case"] for item in evaluate(changed, document_rows) if not item["passed"]]
        defects.append({"defect": name, "caught": bool(failures), "failed_cases": failures[:3]})
    for name, changed in mutations(parameter):
        failures = [item["case"] for item in evaluate(changed, parameter_rows) if not item["passed"]]
        defects.append({"defect": f"parameter_{name}", "caught": bool(failures), "failed_cases": failures[:3]})
    output.mkdir(parents=True, exist_ok=False)
    (output / "report.json").write_text(json.dumps({"checks": checks, "defects": defects}, indent=2) + "\n")
    failed = [item["case"] for item in checks if not item["passed"]] + [f"defect not caught: {item['defect']}" for item in defects if not item["caught"]]
    sys.stdout.write(f"{len(checks)} cases, {len(defects)} planted defects, {len(failed)} problems\n")
    for line in failed:
        sys.stdout.write(f"FAIL {line}\n")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, required=True)
    return verify(parser.parse_args().output)


if __name__ == "__main__":
    sys.exit(main())
