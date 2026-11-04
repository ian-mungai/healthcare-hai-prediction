"""Exercise the grants of the Airflow launch role without AWS calls.

The role belongs to the Airflow scheduler. It may start, stop and describe the host definitions of the named job families only, pass the roles of those
families and the job execution role, read the staged code digest, the job logs and the record of the signed first publishes, check the cluster's container
instances, and write each launch record once under its own prefix. It holds nothing else. Each grant is checked against requests written here, apart from
the rendered policies, and every scope widening of a policy must fail at least one request. Run from the repository root:
``.venv/bin/python -m scripts.infrastructure.airflow_launch_policy_cases --output <NEW_DIRECTORY>``.
"""

from __future__ import annotations

import argparse
import copy
import json
import re
import sys
from pathlib import Path
from typing import Any

from scripts.infrastructure.broker_policy_cases import (
    ACCOUNT,
    BUCKET,
    CLUSTER,
    CLUSTER_ARN,
    LOG_GROUP,
    OTHER_CLUSTER_ARN,
    PROJECT,
    REGION,
    SIGNED,
    Case,
    as_list,
    evaluate,
    role,
    run_context,
    tagged,
    task,
)

ROOT = Path(__file__).resolve().parents[2]
JOBS_TEMPLATE = ROOT / "infra/iam/owner_policies/airflow_launch_jobs.json"
RECORDS_TEMPLATE = ROOT / "infra/iam/owner_policies/airflow_launch_records.json"
REQUESTS_TEMPLATE = ROOT / "infra/iam/owner_policies/airflow_launch_requests.json"
ENVIRONMENT = "dev"
FAMILIES = ["bronze", "gold", "promote", "serve", "silver", "train"]
RECORD_PREFIX = "lakehouse/launch_records/"
INLINE_LIMIT = 10240
ECS_TASKS = "ecs-tasks.amazonaws.com"
Context = dict[str, tuple[str, ...]]
ONCE: Context = {"s3:if-none-match": ("*",)}
REQUEST_FILES = {"bronze": "bronze_request.json", "serve": "serving_request.json"}
FAMILY_ROLES = {
    "promote": "job_train_task",
    "train": "job_train_task",
    "serve": "job_serve_task",
    "bronze": "job_write_bronze_task",
    "silver": "job_write_silver_task",
    "gold": "job_write_gold_task",
    "features": "job_write_features_task",
}
RECORDS = f"arn:aws:s3:::{BUCKET}/{RECORD_PREFIX}"
STAGE = f"arn:aws:s3:::{BUCKET}/lakehouse/run_stage/20261030T100000Z-abcdef01/inputs"
EXTRA_ACTIONS = (
    "ecs:UpdateService",
    "ecs:DeleteService",
    "ecs:RegisterTaskDefinition",
    "s3:DeleteObject",
    "iam:CreateRole",
    "ssm:PutParameter",
    "logs:PutLogEvents",
)


def definition(family: str, revision: str = "7") -> str:
    """A host task definition: the family name with the host suffix."""
    return f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{PROJECT}_{ENVIRONMENT}_job_{family}_host:{revision}"


def container_instance(cluster: str = CLUSTER, identifier: str = "0123456789abcdef0123456789abcdef") -> str:
    return f"arn:aws:ecs:{REGION}:{ACCOUNT}:container-instance/{cluster}/{identifier}"


def values(families: list[str]) -> dict[str, str]:
    """The template inputs, built as infra/iam/airflow_launch.tf builds them."""
    requests = [f"arn:aws:s3:::{BUCKET}/lakehouse/run_stage/*/inputs/{REQUEST_FILES[family]}" for family in sorted(families) if family in REQUEST_FILES]
    return {
        "AWS_REGION": REGION,
        "AWS_ACCOUNT_ID": ACCOUNT,
        "PROJECT_TAG": PROJECT,
        "CLUSTER_ARN": CLUSTER_ARN,
        "CONTAINER_INSTANCE_ARN_PATTERN": f"arn:aws:ecs:{REGION}:{ACCOUNT}:container-instance/{CLUSTER}/*",
        "TASK_ARN_PATTERN": f"arn:aws:ecs:{REGION}:{ACCOUNT}:task/{CLUSTER}/*",
        "LOG_GROUP_NAME": LOG_GROUP,
        "DATA_BUCKET_NAME": BUCKET,
        "SIGNED_PARAMETER_NAME": SIGNED,
        "RECORD_PREFIX": RECORD_PREFIX,
        "REQUEST_OBJECT_ARNS": json.dumps(requests),
        "FAMILIES": json.dumps(sorted(families)),
        "TASK_DEFINITION_ARNS": json.dumps(
            [f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{PROJECT}_{ENVIRONMENT}_job_{family}_host:*" for family in sorted(families)]
        ),
        "ROLE_ARNS": json.dumps(sorted({role("job_execution"), *(role(FAMILY_ROLES[family]) for family in families)})),
    }


def render(path: Path, families: list[str]) -> dict[str, Any]:
    text = path.read_text()
    for name, value in values(families).items():
        text = text.replace("${" + name + "}", value)
    document: dict[str, Any] = json.loads(text)
    return document


def job_cases() -> list[Case]:
    """Each request the main policy must allow or refuse, independent of the rendered resources."""
    pass_context: dict[str, tuple[str, ...]] = {"iam:PassedToService": (ECS_TASKS,)}
    cluster_only: Context = {"ecs:cluster": (CLUSTER_ARN,)}
    rows: list[Case] = []

    def add(label: str, action: str, resource: str, expected: bool, context: dict[str, tuple[str, ...]] | None = None) -> None:
        rows.append((label, action, resource, expected, context or {}))

    for family in FAMILIES:
        add(f"run the {family} host definition", "ecs:RunTask", definition(family), True, run_context(family))
    add("run another revision of train", "ecs:RunTask", definition("train", "12"), True, run_context("train"))
    for family in FAMILIES:
        add(f"run the Fargate {family} definition", "ecs:RunTask", definition(family).replace("_host:", ":"), False, run_context(family))
    for family in ("features", "drift", "metadata", "plain_silver", "write_gold"):
        add(f"run the unnamed {family} host definition", "ecs:RunTask", definition(family), False, run_context("train"))
    add("run a host definition of a longer name", "ecs:RunTask", definition("train").replace("train_host", "train_host_extra"), False, run_context("train"))
    add(
        "run a service definition",
        "ecs:RunTask",
        f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{PROJECT}_{ENVIRONMENT}_polaris:1",
        False,
        run_context("train"),
    )
    add("run in another cluster", "ecs:RunTask", definition("train"), False, run_context("train", **{"ecs:cluster": (OTHER_CLUSTER_ARN,)}))
    add("run without a cluster", "ecs:RunTask", definition("train"), False, {k: v for k, v in run_context("train").items() if k != "ecs:cluster"})
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
    add("run without the tag key list", "ecs:RunTask", definition("train"), True, {k: v for k, v in run_context("train").items() if k != "aws:TagKeys"})
    add("run with an extra tag key", "ecs:RunTask", definition("train"), False, run_context("train", **{"aws:TagKeys": ("Project", "HaiJob", "Extra")}))
    add("start a task by other means", "ecs:StartTask", definition("train"), False, run_context("train"))
    add("tag a task at RunTask", "ecs:TagResource", task(), True, {"ecs:CreateAction": ("RunTask",)})
    add("tag a task at another time", "ecs:TagResource", task(), False)
    add("tag a task in another cluster", "ecs:TagResource", task("another_cluster"), False, {"ecs:CreateAction": ("RunTask",)})
    for action in ("ecs:StopTask", "ecs:DescribeTasks"):
        for family in FAMILIES:
            add(f"{action} on a {family} task", action, task(), True, tagged(family))
        add(f"{action} on a task without the job tag", action, task(), False, tagged(None))
        add(f"{action} on an unnamed job's task", action, task(), False, tagged("features"))
        add(f"{action} in another cluster", action, task("another_cluster"), False, tagged("train"))
    add("list tasks in the cluster", "ecs:ListTasks", "*", True, cluster_only)
    add("list tasks in another cluster", "ecs:ListTasks", "*", False, {"ecs:cluster": (OTHER_CLUSTER_ARN,)})
    add("list tasks without a cluster", "ecs:ListTasks", "*", False)
    add("list container instances in the cluster", "ecs:ListContainerInstances", "*", True, cluster_only)
    add("list container instances in another cluster", "ecs:ListContainerInstances", "*", False, {"ecs:cluster": (OTHER_CLUSTER_ARN,)})
    add("list container instances without a cluster", "ecs:ListContainerInstances", "*", False)
    add("describe a container instance of the cluster", "ecs:DescribeContainerInstances", container_instance(), True, cluster_only)
    add("describe a container instance of another cluster", "ecs:DescribeContainerInstances", container_instance("another_cluster"), False, cluster_only)
    add(
        "describe a container instance claiming another cluster",
        "ecs:DescribeContainerInstances",
        container_instance(),
        False,
        {"ecs:cluster": (OTHER_CLUSTER_ARN,)},
    )
    add("describe a container instance without a cluster", "ecs:DescribeContainerInstances", container_instance(), False)
    add("register a container instance", "ecs:RegisterContainerInstance", container_instance(), False, cluster_only)
    add("update a container instance", "ecs:UpdateContainerInstancesState", container_instance(), False, cluster_only)
    for name in ("job_execution", "job_train_task", "job_serve_task", "job_write_bronze_task", "job_write_silver_task", "job_write_gold_task"):
        add(f"pass {name} to ECS tasks", "iam:PassRole", role(name), True, pass_context)
        add(f"pass {name} to ECS itself", "iam:PassRole", role(name), False, {"iam:PassedToService": ("ecs.amazonaws.com",)})
        add(f"pass {name} without a service", "iam:PassRole", role(name), False)
    for name in (
        "job_write_features_task",
        "job_drift_task",
        "job_openmetadata_task",
        "job_read_task",
        "job_try_writer",
        "task_execution",
        "broker_task",
        "airflow_launch_task",
        "control_plane_task",
        "mlflow_task",
        "host",
        "ecs_host",
        "job_train_task_extra",
    ):
        add(f"pass the unnamed role {name}", "iam:PassRole", role(name), False, pass_context)
    add("read a code digest", "s3:GetObject", f"{STAGE}/code.sha256", True)
    add("read a promotion approval", "s3:GetObject", f"{STAGE}/promotion_approval.json", True)
    add("write a promotion approval", "s3:PutObject", f"{STAGE}/promotion_approval.json", False)
    add("read an approval of another name", "s3:GetObject", f"{STAGE}/approval.json", False)
    for key in ("code.tar.gz", "other.json", "../code.sha256", "bronze_request.json", "serving_request.json"):
        add(f"read inputs/{key} through the main policy", "s3:GetObject", f"{STAGE}/{key}", False)
    add("read a gold table", "s3:GetObject", f"arn:aws:s3:::{BUCKET}/lakehouse/gold/table/data/part-0.parquet", False)
    add("read a digest in another bucket", "s3:GetObject", "arn:aws:s3:::another-bucket/lakehouse/run_stage/run/inputs/code.sha256", False)
    add("write a digest", "s3:PutObject", f"{STAGE}/code.sha256", False)
    add("list the bucket", "s3:ListBucket", f"arn:aws:s3:::{BUCKET}", False)
    stream = f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:{LOG_GROUP}:log-stream:train/train/0123456789abcdef"
    add("read a job log stream", "logs:GetLogEvents", stream, True)
    add("read a stream of another group", "logs:GetLogEvents", stream.replace(LOG_GROUP, f"{PROJECT}_mlflow_{ENVIRONMENT}"), False)
    add("write a job log stream", "logs:PutLogEvents", stream, False)
    signed = f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter{SIGNED}"
    add("read the signed record", "ssm:GetParameter", signed, True)
    add("read the signed record by the plural call", "ssm:GetParameters", signed, False)
    add("write the signed record", "ssm:PutParameter", signed, False)
    add("delete the signed record", "ssm:DeleteParameter", signed, False)
    add("read a sibling of the signed record", "ssm:GetParameter", f"{signed}_extra", False)
    add(
        "read a parameter beside the signed record",
        "ssm:GetParameter",
        f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter/healthcare_hai_prediction/other_parameter",
        False,
    )
    add("read the broker token", "ssm:GetParameter", f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter/healthcare_hai_prediction/broker_token", False)
    add("read the MLflow password", "ssm:GetParameter", f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter/healthcare_hai_prediction/mlflow_admin_password", False)
    for action in ("ecs:UpdateService", "ecs:DeleteService", "ecs:CreateService", "ecs:RegisterTaskDefinition", "ecs:DeregisterTaskDefinition"):
        add(f"no {action}", action, f"arn:aws:ecs:{REGION}:{ACCOUNT}:service/{CLUSTER}/{PROJECT}_{ENVIRONMENT}_polaris", False)
    add("no role creation", "iam:CreateRole", role("anything"), False)
    return rows


def record_cases() -> list[tuple[Any, ...]]:
    """The launch records: created once under their prefix, read back, never replaced or removed."""
    key = f"{RECORDS}20261030T100000Z-abcdef01/train/0/intent.json"
    other = f"arn:aws:s3:::{BUCKET}/lakehouse/run_stage/20261030T100000Z-abcdef01/inputs/intent.json"
    return [
        ("create a launch record", "s3:PutObject", key, True, ONCE),
        ("create a record of another run", "s3:PutObject", f"{RECORDS}20261031T100000Z-01234567/gold/1/started.json", True, ONCE),
        ("replace a launch record", "s3:PutObject", key, False),
        ("replace a launch record with an empty precondition", "s3:PutObject", key, False, {"s3:if-none-match": ()}),
        ("write with another precondition value", "s3:PutObject", key, False, {"s3:if-none-match": ("etag-of-the-old-object",)}),
        ("write a record outside the prefix", "s3:PutObject", other, False, ONCE),
        ("write a record to the prefix root of the bucket", "s3:PutObject", f"arn:aws:s3:::{BUCKET}/intent.json", False, ONCE),
        ("write a record under a sibling prefix", "s3:PutObject", f"arn:aws:s3:::{BUCKET}/lakehouse/launch_records_extra/intent.json", False, ONCE),
        ("write a record to another bucket", "s3:PutObject", f"arn:aws:s3:::another-bucket/{RECORD_PREFIX}intent.json", False, ONCE),
        ("write a gold table file", "s3:PutObject", f"arn:aws:s3:::{BUCKET}/lakehouse/gold/table/data/part-0.parquet", False, ONCE),
        ("delete a launch record", "s3:DeleteObject", key, False),
        ("delete a version of a launch record", "s3:DeleteObjectVersion", key, False),
        ("start a multipart write of a launch record", "s3:AbortMultipartUpload", key, False),
        ("change the access list of a launch record", "s3:PutObjectAcl", key, False, ONCE),
        ("read a launch record", "s3:GetObject", key, True),
        ("read a record of another run", "s3:GetObject", f"{RECORDS}20261031T100000Z-01234567/gold/1/started.json", True),
        ("read an object outside the prefix", "s3:GetObject", other, False),
        ("read a gold table through the records policy", "s3:GetObject", f"arn:aws:s3:::{BUCKET}/lakehouse/gold/table/data/part-0.parquet", False),
        ("read a sibling prefix", "s3:GetObject", f"arn:aws:s3:::{BUCKET}/lakehouse/launch_records_extra/intent.json", False),
        ("read a record in another bucket", "s3:GetObject", f"arn:aws:s3:::another-bucket/{RECORD_PREFIX}intent.json", False),
        ("read a version of a launch record", "s3:GetObjectVersion", key, False),
        ("list the record prefix", "s3:ListBucket", f"arn:aws:s3:::{BUCKET}", False, {"s3:prefix": (RECORD_PREFIX,)}),
    ]


def request_cases() -> list[tuple[Any, ...]]:
    """The staged requests of the bronze and serve runs, read one key each. They exist because bronze and serve are named."""
    return [
        ("read the staged bronze request", "s3:GetObject", f"{STAGE}/bronze_request.json", True),
        ("read the staged serving request", "s3:GetObject", f"{STAGE}/serving_request.json", True),
        ("read the code archive", "s3:GetObject", f"{STAGE}/code.tar.gz", False),
        ("read the code digest through the request policy", "s3:GetObject", f"{STAGE}/code.sha256", False),
        ("read a request under another prefix", "s3:GetObject", f"arn:aws:s3:::{BUCKET}/lakehouse/bronze/inputs/bronze_request.json", False),
        ("read a serving request under another prefix", "s3:GetObject", f"arn:aws:s3:::{BUCKET}/serving/deployments/inputs/serving_request.json", False),
        (
            "read a request in another bucket",
            "s3:GetObject",
            "arn:aws:s3:::another-bucket/lakehouse/run_stage/run/inputs/bronze_request.json",
            False,
        ),
        ("write the staged bronze request", "s3:PutObject", f"{STAGE}/bronze_request.json", False),
        ("write the staged serving request", "s3:PutObject", f"{STAGE}/serving_request.json", False),
        ("delete the staged bronze request", "s3:DeleteObject", f"{STAGE}/bronze_request.json", False),
        ("read a deployment record", "s3:GetObject", f"arn:aws:s3:::{BUCKET}/serving/deployments/active/HAI_1.json", False),
    ]


def with_empty_context(rows: list[tuple[Any, ...]]) -> list[Case]:
    """Give every row the five fields of a case: a row without a context gets an empty one."""
    return [(row[0], row[1], row[2], row[3], row[4] if len(row) > 4 else {}) for row in rows]


def satisfying_context(sid: str) -> dict[str, tuple[str, ...]]:
    """A request context that meets every condition of the statement, so only the action and resource decide."""
    contexts: dict[str, dict[str, tuple[str, ...]]] = {
        "RunNamedHostJobs": run_context("train"),
        "TagTasksItStarts": {"ecs:CreateAction": ("RunTask",)},
        "StopAndDescribeNamedJobTasks": tagged("train"),
        "ListClusterTasks": {"ecs:cluster": (CLUSTER_ARN,)},
        "ListClusterContainerInstances": {"ecs:cluster": (CLUSTER_ARN,)},
        "DescribeClusterContainerInstances": {"ecs:cluster": (CLUSTER_ARN,)},
        "PassNamedJobRoles": {"iam:PassedToService": (ECS_TASKS,)},
        "WriteLaunchRecordsOnce": dict(ONCE),
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


def condition_drops(statement: dict[str, Any]) -> list[tuple[str, dict[str, Any] | None]]:
    """One variant per condition key: that key alone removed, and the whole Condition when it was the last one."""
    variants: list[tuple[str, dict[str, Any] | None]] = []
    for operator, keys in statement.get("Condition", {}).items():
        for key in keys:
            remaining = {op: {k: v for k, v in body.items() if not (op == operator and k == key)} for op, body in statement["Condition"].items()}
            remaining = {op: body for op, body in remaining.items() if body}
            variants.append((f"drop_{key.replace(':', '_').replace('/', '_')}", remaining or None))
    return variants


def mutations(document: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Every scope widening of every statement: an extra action, a wildcard resource, each condition key removed."""
    found = []
    for statement in document["Statement"]:
        variants: list[tuple[str, str, Any]] = [("add_action", "Action", [*as_list(statement["Action"]), "ecs:UpdateService"])]
        if statement["Resource"] != "*":
            variants.append(("widen_resource", "Resource", "*"))
        variants += [(label, "Condition", condition) for label, condition in condition_drops(statement)]
        for label, field, value in variants:
            changed = copy.deepcopy(document)
            target = next(item for item in changed["Statement"] if item["Sid"] == statement["Sid"])
            if value is None:
                target.pop(field)
            else:
                target[field] = value
            found.append((f"{statement['Sid']}_{label}", changed))
    return found


def replaced(document: dict[str, Any], sid: str, **fields: Any) -> dict[str, Any]:
    changed = copy.deepcopy(document)
    next(item for item in changed["Statement"] if item["Sid"] == sid).update(fields)
    return changed


def planted(jobs: dict[str, Any], records: dict[str, Any], requests: dict[str, Any]) -> list[tuple[str, str, dict[str, Any]]]:
    """Named widenings that a reviewer could make by hand, beyond the generic ones: the document each belongs to is the first field."""
    ssm = f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter"
    pass_roles = next(item for item in jobs["Statement"] if item["Sid"] == "PassNamedJobRoles")["Resource"]
    definitions = [name.replace("_host:*", ":*") for name in next(item for item in jobs["Statement"] if item["Sid"] == "RunNamedHostJobs")["Resource"]]
    request_resource = next(item for item in requests["Statement"])["Resource"]
    return [
        ("jobs", "RunNamedHostJobs_run_the_Fargate_definitions", replaced(jobs, "RunNamedHostJobs", Resource=definitions)),
        ("jobs", "PassNamedJobRoles_another_family_role", replaced(jobs, "PassNamedJobRoles", Resource=[*pass_roles, role("job_write_features_task")])),
        ("jobs", "PassNamedJobRoles_role_wildcard", replaced(jobs, "PassNamedJobRoles", Resource=[*pass_roles, role("job_*")])),
        ("jobs", "PassNamedJobRoles_the_launch_role_itself", replaced(jobs, "PassNamedJobRoles", Resource=[*pass_roles, role("airflow_launch_task")])),
        ("jobs", "ReadSignedPublishRecord_a_parameter_prefix", replaced(jobs, "ReadSignedPublishRecord", Resource=f"{ssm}/healthcare_hai_prediction/*")),
        ("jobs", "ReadSignedPublishRecord_every_parameter", replaced(jobs, "ReadSignedPublishRecord", Resource=f"{ssm}/*")),
        ("jobs", "ReadSignedPublishRecord_a_name_prefix", replaced(jobs, "ReadSignedPublishRecord", Resource=f"{ssm}{SIGNED}*")),
        ("jobs", "ReadStagedRunRecords_every_input", replaced(jobs, "ReadStagedRunRecords", Resource=f"arn:aws:s3:::{BUCKET}/lakehouse/run_stage/*/inputs/*")),
        ("records", "WriteLaunchRecordsOnce_the_whole_bucket", replaced(records, "WriteLaunchRecordsOnce", Resource=f"arn:aws:s3:::{BUCKET}/*")),
        ("records", "WriteLaunchRecordsOnce_the_lakehouse", replaced(records, "WriteLaunchRecordsOnce", Resource=f"arn:aws:s3:::{BUCKET}/lakehouse/*")),
        (
            "records",
            "WriteLaunchRecordsOnce_any_precondition_value",
            replaced(records, "WriteLaunchRecordsOnce", Condition={"StringLike": {"s3:if-none-match": "*"}}),
        ),
        (
            "records",
            "WriteLaunchRecordsOnce_a_present_precondition",
            replaced(records, "WriteLaunchRecordsOnce", Condition={"Null": {"s3:if-none-match": "false"}}),
        ),
        ("records", "ReadLaunchRecords_the_lakehouse", replaced(records, "ReadLaunchRecords", Resource=f"arn:aws:s3:::{BUCKET}/lakehouse/*")),
        ("records", "ReadLaunchRecords_the_whole_bucket", replaced(records, "ReadLaunchRecords", Resource=f"arn:aws:s3:::{BUCKET}/*")),
        (
            "requests",
            "ReadStagedRequests_every_input",
            replaced(requests, "ReadStagedRequests", Resource=f"arn:aws:s3:::{BUCKET}/lakehouse/run_stage/*/inputs/*"),
        ),
        ("requests", "ReadStagedRequests_any_object", replaced(requests, "ReadStagedRequests", Resource=[*request_resource, f"arn:aws:s3:::{BUCKET}/*"])),
    ]


def size_checks(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The aggregate of the inline policies of one role stays under the IAM limit, white space not counted."""
    size = sum(len(re.sub(r"\s", "", json.dumps(document, separators=(",", ":")))) for document in documents)
    return [{"case": "the inline policies of the role fit the IAM aggregate limit", "expected": INLINE_LIMIT, "observed": size, "passed": size <= INLINE_LIMIT}]


def verify(output: Path) -> int:
    jobs = render(JOBS_TEMPLATE, FAMILIES)
    records = render(RECORDS_TEMPLATE, FAMILIES)
    requests = render(REQUESTS_TEMPLATE, FAMILIES)
    suites = {
        "jobs": (jobs, with_empty_context(job_cases()) + extra_action_cases(jobs)),
        "records": (records, with_empty_context(record_cases()) + extra_action_cases(records)),
        "requests": (requests, with_empty_context(request_cases()) + extra_action_cases(requests)),
    }
    checks = [item for document, rows in suites.values() for item in evaluate(document, rows)]
    checks += size_checks([jobs, records, requests])
    defects = []
    for suite, (document, rows) in suites.items():
        for name, changed in mutations(document):
            failures = [item["case"] for item in evaluate(changed, rows) if not item["passed"]]
            defects.append({"defect": f"{suite}_{name}", "caught": bool(failures), "failed_cases": failures[:3]})
    for suite, name, changed in planted(jobs, records, requests):
        failures = [item["case"] for item in evaluate(changed, suites[suite][1]) if not item["passed"]]
        defects.append({"defect": f"{suite}_{name}", "caught": bool(failures), "failed_cases": failures[:3]})
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
