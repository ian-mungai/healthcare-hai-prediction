"""Exercise the grants of the Airflow launch role without AWS calls.

The role belongs to the Airflow scheduler. It may start, stop and describe the host definitions of the named job families only, pass the roles of those
families and the job execution role, read the staged code digest, the job logs and the record of the signed first publishes, check the cluster's container
instances, and write each launch record once, list and read them under its own prefix. It holds nothing else.

The first check is closed: the permitted (action, resource, required conditions) triples of the role are written below as data, and every Allow statement
of the three policies must be inside them. An action outside the set, a wildcard, a NotAction, a resource outside its triple or a missing required
condition fails whatever the action is, so an unknown dangerous action fails by construction. The behavior cases and the fixed matrix below are extra checks.

The three policies are one role, so every request is decided against all of them together. Each grant is checked against requests written here, apart from
the rendered policies. The refusals do not depend on the policy under inspection: a fixed list of actions is tried against a fixed list of resource
classes, and only the intended pairs may be allowed. Every realistic widening of a policy (a dangerous action on a valid resource, a wider resource, a
missing condition) must fail at least one request. Run from the repository root:
``.venv/bin/python -m scripts.infrastructure.airflow_launch_policy_cases --output <NEW_DIRECTORY>``.
"""

from __future__ import annotations

import argparse
import copy
import json
import re
import sys
from pathlib import Path
from typing import Any, NamedTuple

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
    role,
    run_context,
    tagged,
    task,
)
from scripts.infrastructure.policy_evaluator import Request, UnsupportedPolicyElement, decide

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
Role = dict[str, dict[str, Any]]
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
    "drift": "job_drift_task",
    "metadata": "job_openmetadata_task",
}
ALL_FAMILIES = sorted(FAMILY_ROLES)
RECORDS = f"arn:aws:s3:::{BUCKET}/{RECORD_PREFIX}"
STAGE = f"arn:aws:s3:::{BUCKET}/lakehouse/run_stage/20261030T100000Z-abcdef01/inputs"


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
    for key in ("code.tar.gz", "other.json", "../code.sha256"):
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
    bucket = f"arn:aws:s3:::{BUCKET}"
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
        ("list the record prefix", "s3:ListBucket", bucket, True, {"s3:prefix": (RECORD_PREFIX,)}),
        ("list the record prefix with a delimiter", "s3:ListBucket", bucket, True, {"s3:prefix": (RECORD_PREFIX,), "s3:delimiter": ("/",)}),
        ("list below the record prefix", "s3:ListBucket", bucket, True, {"s3:prefix": (f"{RECORD_PREFIX}20261030T100000Z-abcdef01/train/",)}),
        ("list with no prefix", "s3:ListBucket", bucket, False),
        ("list with an empty prefix", "s3:ListBucket", bucket, False, {"s3:prefix": ("",)}),
        ("list with a delimiter and no prefix", "s3:ListBucket", bucket, False, {"s3:delimiter": ("/",)}),
        ("list the lakehouse", "s3:ListBucket", bucket, False, {"s3:prefix": ("lakehouse/",)}),
        ("list the staged inputs", "s3:ListBucket", bucket, False, {"s3:prefix": ("lakehouse/run_stage/",)}),
        ("list a sibling of the record prefix", "s3:ListBucket", bucket, False, {"s3:prefix": ("lakehouse/launch_records_extra/",)}),
        ("list the record prefix of another bucket", "s3:ListBucket", "arn:aws:s3:::another-bucket", False, {"s3:prefix": (RECORD_PREFIX,)}),
        ("list the record prefix as an object", "s3:ListBucket", f"{bucket}/{RECORD_PREFIX}", False, {"s3:prefix": (RECORD_PREFIX,)}),
        ("list the versions under the record prefix", "s3:ListBucketVersions", bucket, False, {"s3:prefix": (RECORD_PREFIX,)}),
        ("list the multipart uploads under the record prefix", "s3:ListBucketMultipartUploads", bucket, False, {"s3:prefix": (RECORD_PREFIX,)}),
    ]


def request_cases() -> list[tuple[Any, ...]]:
    """The staged requests of the bronze and serve runs, read one key each. They exist because bronze and serve are named."""
    return [
        ("read the staged bronze request", "s3:GetObject", f"{STAGE}/bronze_request.json", True),
        ("read the staged serving request", "s3:GetObject", f"{STAGE}/serving_request.json", True),
        ("read the code archive", "s3:GetObject", f"{STAGE}/code.tar.gz", False),
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


class Resource(NamedTuple):
    """A class of resource: one concrete name, the actions the role may use on it with every condition met, and those it may use with no condition."""

    name: str
    resource: str
    allowed: frozenset[str]
    bare: frozenset[str]


GET = frozenset({"s3:GetObject"})
LISTS = frozenset({"ecs:ListTasks", "ecs:ListContainerInstances"})
# Every condition of every grant met at once, so that only the action and the resource decide.
PERMISSIVE: Context = {
    **run_context("train"),
    "aws:ResourceTag/HaiJob": ("train",),
    "ecs:CreateAction": ("RunTask",),
    "iam:PassedToService": (ECS_TASKS,),
    "s3:prefix": (RECORD_PREFIX,),
    **ONCE,
}
KMS_KEY = f"arn:aws:kms:{REGION}:{ACCOUNT}:key/00000000-1111-2222-3333-444444444444"
KMS_OTHER_KEY = f"arn:aws:kms:{REGION}:{ACCOUNT}:key/99999999-8888-7777-6666-555555555555"
STREAM = f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:{LOG_GROUP}:log-stream:train/train/0123456789abcdef"
SIGNED_ARN = f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter{SIGNED}"
LAKEHOUSE_FILE = f"arn:aws:s3:::{BUCKET}/lakehouse/gold/table/data/part-0.parquet"
RECORD = f"{RECORDS}20261030T100000Z-abcdef01/train/0/intent.json"
NONE: frozenset[str] = frozenset()
RESOURCES = (
    Resource("host definition of a named family", definition("train"), frozenset({"ecs:RunTask"}), NONE),
    Resource("Fargate definition of a named family", definition("train").replace("_host:", ":"), NONE, NONE),
    Resource("host definition of an unnamed family", definition("features"), NONE, NONE),
    Resource("task of the cluster", task(), frozenset({"ecs:TagResource", "ecs:StopTask", "ecs:DescribeTasks"}), NONE),
    Resource("task of another cluster", task("another_cluster"), NONE, NONE),
    Resource("cluster-wide list", "*", LISTS, NONE),
    Resource("container instance of the cluster", container_instance(), frozenset({"ecs:DescribeContainerInstances"}), NONE),
    Resource("container instance of another cluster", container_instance("another_cluster"), NONE, NONE),
    Resource("task role of a named family", role("job_train_task"), frozenset({"iam:PassRole"}), NONE),
    Resource("execution role", role("job_execution"), frozenset({"iam:PassRole"}), NONE),
    Resource("role of an unnamed family", role("job_write_features_task"), NONE, NONE),
    Resource("the launch role itself", role("airflow_launch_task"), NONE, NONE),
    Resource("staged code digest", f"{STAGE}/code.sha256", GET, GET),
    Resource("staged promotion approval", f"{STAGE}/promotion_approval.json", GET, GET),
    Resource("staged bronze request", f"{STAGE}/bronze_request.json", GET, GET),
    Resource("staged serving request", f"{STAGE}/serving_request.json", GET, GET),
    Resource("staged code archive", f"{STAGE}/code.tar.gz", NONE, NONE),
    Resource("launch record", RECORD, frozenset({"s3:GetObject", "s3:PutObject"}), GET),
    Resource("lakehouse table file", LAKEHOUSE_FILE, NONE, NONE),
    Resource("the data bucket", f"arn:aws:s3:::{BUCKET}", frozenset({"s3:ListBucket"}), NONE),
    Resource("job log stream", STREAM, frozenset({"logs:GetLogEvents"}), frozenset({"logs:GetLogEvents"})),
    Resource("log stream of another group", STREAM.replace(LOG_GROUP, f"{PROJECT}_mlflow_{ENVIRONMENT}"), NONE, NONE),
    Resource("signed publish record", SIGNED_ARN, frozenset({"ssm:GetParameter"}), frozenset({"ssm:GetParameter"})),
    Resource("another parameter", f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter/healthcare_hai_prediction/other_parameter", NONE, NONE),
    Resource("a KMS key", KMS_KEY, NONE, NONE),
    Resource("another KMS key", KMS_OTHER_KEY, NONE, NONE),
)
# The actions tried against every class. The list is fixed here: it never reads the policy under inspection.
ACTIONS = (
    "ecs:RunTask",
    "ecs:StartTask",
    "ecs:StopTask",
    "ecs:DescribeTasks",
    "ecs:TagResource",
    "ecs:UntagResource",
    "ecs:ListTasks",
    "ecs:ListContainerInstances",
    "ecs:DescribeContainerInstances",
    "ecs:RegisterContainerInstance",
    "ecs:DeregisterContainerInstance",
    "ecs:UpdateContainerInstancesState",
    "ecs:RegisterTaskDefinition",
    "ecs:DeregisterTaskDefinition",
    "ecs:DescribeTaskDefinition",
    "ecs:CreateService",
    "ecs:UpdateService",
    "ecs:DeleteService",
    "ecs:ListServices",
    "ecs:DescribeClusters",
    "ecs:DeleteCluster",
    "ecs:ExecuteCommand",
    "ecs:UpdateTaskProtection",
    "iam:PassRole",
    "iam:CreateRole",
    "iam:DeleteRole",
    "iam:AttachRolePolicy",
    "iam:PutRolePolicy",
    "iam:UpdateAssumeRolePolicy",
    "sts:AssumeRole",
    "s3:GetObject",
    "s3:GetObjectVersion",
    "s3:PutObject",
    "s3:PutObjectAcl",
    "s3:PutObjectTagging",
    "s3:DeleteObject",
    "s3:DeleteObjectVersion",
    "s3:AbortMultipartUpload",
    "s3:RestoreObject",
    "s3:ListBucket",
    "s3:ListBucketMultipartUploads",
    "s3:ListBucketVersions",
    "s3:PutObjectVersionAcl",
    "s3:PutObjectVersionTagging",
    "s3:DeleteObjectTagging",
    "s3:PutBucketPolicy",
    "s3:DeleteBucketPolicy",
    "s3:PutBucketAcl",
    "ssm:DeleteParameters",
    "kms:CreateGrant",
    "kms:GenerateDataKey",
    "logs:GetLogEvents",
    "logs:FilterLogEvents",
    "logs:PutLogEvents",
    "logs:CreateLogStream",
    "logs:DeleteLogStream",
    "logs:DeleteLogGroup",
    "ssm:GetParameter",
    "ssm:GetParameters",
    "ssm:GetParametersByPath",
    "ssm:GetParameterHistory",
    "ssm:DescribeParameters",
    "ssm:PutParameter",
    "ssm:DeleteParameter",
    "kms:Decrypt",
    "secretsmanager:GetSecretValue",
)


def matrix_cases() -> list[Case]:
    """Every fixed action against every fixed resource class: only the intended pairs are allowed, with every condition met or with none."""
    rows: list[Case] = []
    for item in RESOURCES:
        for action in ACTIONS:
            if action in LISTS and item.resource != "*":
                continue
            rows.append((f"{item.name}: {action} with every condition met", action, item.resource, action in item.allowed, PERMISSIVE))
            rows.append((f"{item.name}: {action} with no condition", action, item.resource, action in item.bare, {}))
    return rows


def train_only_cases() -> list[Case]:
    """A role for the train family alone holds no staged request grant and no other family's definition or role."""
    pass_context: Context = {"iam:PassedToService": (ECS_TASKS,)}
    return [
        ("train only: run the train host definition", "ecs:RunTask", definition("train"), True, run_context("train")),
        ("train only: run the gold host definition", "ecs:RunTask", definition("gold"), False, run_context("train")),
        ("train only: run with the gold tag", "ecs:RunTask", definition("train"), False, run_context("gold")),
        ("train only: pass the train role", "iam:PassRole", role("job_train_task"), True, pass_context),
        ("train only: pass the gold role", "iam:PassRole", role("job_write_gold_task"), False, pass_context),
        ("train only: pass the bronze role", "iam:PassRole", role("job_write_bronze_task"), False, pass_context),
        ("train only: read the staged bronze request", "s3:GetObject", f"{STAGE}/bronze_request.json", False, {}),
        ("train only: read the staged serving request", "s3:GetObject", f"{STAGE}/serving_request.json", False, {}),
        ("train only: create a launch record", "s3:PutObject", RECORD, True, ONCE),
    ]


def condition_drops(statement: dict[str, Any]) -> list[tuple[str, dict[str, Any] | None]]:
    """One variant per condition key: that key alone removed, and the whole Condition when it was the last one."""
    variants: list[tuple[str, dict[str, Any] | None]] = []
    for operator, keys in statement.get("Condition", {}).items():
        for key in keys:
            remaining = {op: {k: v for k, v in body.items() if not (op == operator and k == key)} for op, body in statement["Condition"].items()}
            remaining = {op: body for op, body in remaining.items() if body}
            variants.append((f"drop_{key.replace(':', '_').replace('/', '_')}", remaining or None))
    return variants


# The dangerous actions a careless edit could add to a statement, each valid on that statement's own resources.
WIDENING_ACTIONS = {
    "RunNamedHostJobs": ("ecs:StartTask", "ecs:RegisterTaskDefinition"),
    "TagTasksItStarts": ("ecs:UntagResource",),
    "StopAndDescribeNamedJobTasks": ("ecs:ExecuteCommand", "ecs:UpdateTaskProtection"),
    "ListClusterTasks": ("ecs:ListServices",),
    "ListClusterContainerInstances": ("ecs:DescribeClusters",),
    "DescribeClusterContainerInstances": ("ecs:DeregisterContainerInstance", "ecs:UpdateContainerInstancesState"),
    "PassNamedJobRoles": ("sts:AssumeRole", "iam:AttachRolePolicy"),
    "ReadStagedRunRecords": ("s3:DeleteObject", "s3:PutObject", "s3:GetObjectVersion"),
    "ReadJobLogs": ("logs:PutLogEvents", "logs:DeleteLogStream"),
    "ReadSignedPublishRecord": ("ssm:PutParameter", "ssm:GetParameters", "ssm:DeleteParameter"),
    "WriteLaunchRecordsOnce": ("s3:DeleteObject", "s3:PutObjectAcl"),
    "ReadLaunchRecords": ("s3:DeleteObject", "s3:GetObjectVersion"),
    "ListLaunchRecords": ("s3:ListBucketVersions", "s3:ListBucketMultipartUploads"),
    "ReadStagedRequests": ("s3:PutObject", "s3:DeleteObject"),
}


def widenings(role_documents_: Role) -> list[tuple[str, str, dict[str, Any]]]:
    """Each realistic widening of each statement: a dangerous action added, a wildcard resource and each condition key removed in turn."""
    found: list[tuple[str, str, dict[str, Any]]] = []
    for name, document in role_documents_.items():
        for statement in document["Statement"]:
            sid = statement["Sid"]
            variants: list[tuple[str, str, Any]] = [
                (f"add_{action.replace(':', '_')}", "Action", [*as_list(statement["Action"]), action]) for action in WIDENING_ACTIONS[sid]
            ]
            if statement["Resource"] != "*":
                variants.append(("widen_resource", "Resource", "*"))
            variants += [(label, "Condition", condition) for label, condition in condition_drops(statement)]
            for label, field, value in variants:
                changed = copy.deepcopy(document)
                target = next(item for item in changed["Statement"] if item["Sid"] == sid)
                if value is None:
                    target.pop(field)
                else:
                    target[field] = value
                found.append((name, f"{sid}_{label}", changed))
    return found


def replaced(document: dict[str, Any], sid: str, **fields: Any) -> dict[str, Any]:
    changed = copy.deepcopy(document)
    for item in changed["Statement"]:
        if item["Sid"] == sid:
            item.update(fields)
    return changed


def appended(document: dict[str, Any], statement: dict[str, Any]) -> dict[str, Any]:
    changed = copy.deepcopy(document)
    changed["Statement"].append(statement)
    return changed


def planted(documents: Role) -> list[tuple[str, str, dict[str, Any]]]:
    """Named widenings that a reviewer could make by hand: the document each belongs to is the first field."""
    jobs, records, requests = documents["jobs"], documents["records"], documents["requests"]
    ssm = f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter"
    pass_roles = next(item for item in jobs["Statement"] if item["Sid"] == "PassNamedJobRoles")["Resource"]
    definitions = [name.replace("_host:*", ":*") for name in next(item for item in jobs["Statement"] if item["Sid"] == "RunNamedHostJobs")["Resource"]]
    request_resource = next(item for item in requests["Statement"])["Resource"]
    bucket = f"arn:aws:s3:::{BUCKET}"
    return [
        ("jobs", "RunNamedHostJobs_run_the_Fargate_definitions", replaced(jobs, "RunNamedHostJobs", Resource=definitions)),
        ("jobs", "PassNamedJobRoles_another_family_role", replaced(jobs, "PassNamedJobRoles", Resource=[*pass_roles, role("job_write_features_task")])),
        ("jobs", "PassNamedJobRoles_role_wildcard", replaced(jobs, "PassNamedJobRoles", Resource=[*pass_roles, role("job_*")])),
        ("jobs", "PassNamedJobRoles_the_scheduler_role_itself", replaced(jobs, "PassNamedJobRoles", Resource=[*pass_roles, role("airflow_launch_task")])),
        ("jobs", "ReadSignedPublishRecord_a_parameter_prefix", replaced(jobs, "ReadSignedPublishRecord", Resource=f"{ssm}/healthcare_hai_prediction/*")),
        ("jobs", "ReadSignedPublishRecord_every_parameter", replaced(jobs, "ReadSignedPublishRecord", Resource=f"{ssm}/*")),
        ("jobs", "ReadSignedPublishRecord_a_name_prefix", replaced(jobs, "ReadSignedPublishRecord", Resource=f"{ssm}{SIGNED}*")),
        ("jobs", "ReadStagedRunRecords_every_input", replaced(jobs, "ReadStagedRunRecords", Resource=f"{bucket}/lakehouse/run_stage/*/inputs/*")),
        ("jobs", "ReadStagedRunRecords_delete_the_digest", replaced(jobs, "ReadStagedRunRecords", Action=["s3:GetObject", "s3:DeleteObject"])),
        (
            "jobs",
            "an_extra_statement_deletes_the_digest",
            appended(jobs, {"Sid": "Extra", "Effect": "Allow", "Action": "s3:DeleteObject", "Resource": f"{bucket}/lakehouse/run_stage/*/inputs/code.sha256"}),
        ),
        ("records", "WriteLaunchRecordsOnce_the_whole_bucket", replaced(records, "WriteLaunchRecordsOnce", Resource=f"{bucket}/*")),
        ("records", "WriteLaunchRecordsOnce_the_lakehouse", replaced(records, "WriteLaunchRecordsOnce", Resource=f"{bucket}/lakehouse/*")),
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
        (
            "records",
            "an_extra_statement_puts_without_the_condition",
            appended(records, {"Sid": "Extra", "Effect": "Allow", "Action": "s3:PutObject", "Resource": f"{RECORDS}*"}),
        ),
        (
            "records",
            "an_extra_statement_deletes_a_record",
            appended(records, {"Sid": "Extra", "Effect": "Allow", "Action": "s3:DeleteObject", "Resource": f"{RECORDS}*"}),
        ),
        (
            "records",
            "an_extra_statement_lists_without_the_prefix_condition",
            appended(records, {"Sid": "Extra", "Effect": "Allow", "Action": "s3:ListBucket", "Resource": bucket}),
        ),
        (
            "records",
            "ListLaunchRecords_the_prefix_condition_widened_to_the_bucket_root",
            replaced(records, "ListLaunchRecords", Condition={"StringLike": {"s3:prefix": ["*"]}}),
        ),
        (
            "records",
            "ListLaunchRecords_the_prefix_condition_widened_to_the_lakehouse",
            replaced(records, "ListLaunchRecords", Condition={"StringLike": {"s3:prefix": ["lakehouse/*"]}}),
        ),
        (
            "records",
            "ListLaunchRecords_a_prefix_condition_on_the_wrong_key",
            replaced(records, "ListLaunchRecords", Condition={"StringLike": {"s3:delimiter": ["*"]}}),
        ),
        ("records", "ReadLaunchRecords_the_lakehouse", replaced(records, "ReadLaunchRecords", Resource=f"{bucket}/lakehouse/*")),
        ("records", "ReadLaunchRecords_the_whole_bucket", replaced(records, "ReadLaunchRecords", Resource=f"{bucket}/*")),
        ("jobs", "regression_ssm_DeleteParameters", replaced(jobs, "ReadSignedPublishRecord", Action=["ssm:GetParameter", "ssm:DeleteParameters"])),
        (
            "jobs",
            "regression_kms_CreateGrant",
            replaced(jobs, "ReadSignedPublishRecord", Action=["ssm:GetParameter", "kms:CreateGrant"], Resource=[f"{ssm}{SIGNED}", KMS_KEY]),
        ),
        ("jobs", "regression_s3_PutObjectVersionAcl", replaced(jobs, "ReadStagedRunRecords", Action=["s3:GetObject", "s3:PutObjectVersionAcl"])),
        ("jobs", "regression_s3_DeleteObjectTagging", replaced(jobs, "ReadStagedRunRecords", Action=["s3:GetObject", "s3:DeleteObjectTagging"])),
        ("jobs", "regression_s3_PutBucketPolicy", appended(jobs, {"Sid": "Extra", "Effect": "Allow", "Action": "s3:PutBucketPolicy", "Resource": bucket})),
        ("jobs", "regression_s3_PutBucketAcl", appended(jobs, {"Sid": "Extra", "Effect": "Allow", "Action": "s3:PutBucketAcl", "Resource": bucket})),
        (
            "jobs",
            "regression_kms_Decrypt_on_another_key",
            appended(jobs, {"Sid": "Extra", "Effect": "Allow", "Action": "kms:Decrypt", "Resource": KMS_OTHER_KEY}),
        ),
        ("jobs", "regression_kms_Decrypt_on_every_key", appended(jobs, {"Sid": "Extra", "Effect": "Allow", "Action": "kms:Decrypt", "Resource": "*"})),
        (
            "jobs",
            "regression_ssm_PutParameter",
            appended(jobs, {"Sid": "Extra", "Effect": "Allow", "Action": "ssm:PutParameter", "Resource": f"{ssm}{SIGNED}"}),
        ),
        ("jobs", "regression_NotAction_statement", appended(jobs, {"Sid": "Extra", "Effect": "Allow", "NotAction": "iam:*", "Resource": "*"})),
        ("jobs", "regression_action_wildcard", appended(jobs, {"Sid": "Extra", "Effect": "Allow", "Action": "ssm:Delete*", "Resource": f"{ssm}{SIGNED}"})),
        ("requests", "ReadStagedRequests_every_input", replaced(requests, "ReadStagedRequests", Resource=f"{bucket}/lakehouse/run_stage/*/inputs/*")),
        ("requests", "ReadStagedRequests_any_object", replaced(requests, "ReadStagedRequests", Resource=[*request_resource, f"{bucket}/*"])),
    ]


Conditions = dict[str, dict[str, list[str]]]
STATEMENT_KEYS = {"Sid", "Effect", "Action", "Resource", "Condition"}


class Grant(NamedTuple):
    """One permitted triple: an action, the exact resource patterns it may name and the conditions every statement that grants it must hold."""

    action: str
    resources: tuple[str, ...]
    conditions: Conditions


def allowed_grants(families: list[str]) -> list[Grant]:
    """The complete set of grants of the role for the named families, written here and read from no policy."""
    cluster: Conditions = {"ArnEquals": {"ecs:cluster": [CLUSTER_ARN]}}
    tasks = f"arn:aws:ecs:{REGION}:{ACCOUNT}:task/{CLUSTER}/*"
    named = sorted(families)
    definitions = tuple(f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{PROJECT}_{ENVIRONMENT}_job_{family}_host:*" for family in named)
    roles = tuple(sorted({role("job_execution"), *(role(FAMILY_ROLES[family]) for family in named)}))
    stage = f"arn:aws:s3:::{BUCKET}/lakehouse/run_stage/*/inputs"
    staged = (f"{stage}/code.sha256", f"{stage}/promotion_approval.json", *(f"{stage}/{REQUEST_FILES[family]}" for family in named if family in REQUEST_FILES))
    tagged_tasks: Conditions = {"StringEquals": {"aws:ResourceTag/HaiJob": named}}
    return [
        Grant(
            "ecs:RunTask",
            definitions,
            {
                **cluster,
                "StringEquals": {"aws:RequestTag/Project": [PROJECT], "aws:RequestTag/HaiJob": named},
                "ForAllValues:StringEquals": {"aws:TagKeys": ["HaiJob", "Project"]},
            },
        ),
        Grant("ecs:TagResource", (tasks,), {"StringEquals": {"ecs:CreateAction": ["RunTask"]}}),
        Grant("ecs:StopTask", (tasks,), tagged_tasks),
        Grant("ecs:DescribeTasks", (tasks,), tagged_tasks),
        Grant("ecs:ListTasks", ("*",), cluster),
        Grant("ecs:ListContainerInstances", ("*",), cluster),
        Grant("ecs:DescribeContainerInstances", (f"arn:aws:ecs:{REGION}:{ACCOUNT}:container-instance/{CLUSTER}/*",), cluster),
        Grant("iam:PassRole", roles, {"StringEquals": {"iam:PassedToService": [ECS_TASKS]}}),
        Grant("s3:GetObject", (*staged, f"{RECORDS}*"), {}),
        Grant("s3:PutObject", (f"{RECORDS}*",), {"StringEquals": {"s3:if-none-match": ["*"]}}),
        Grant("s3:ListBucket", (f"arn:aws:s3:::{BUCKET}",), {"StringLike": {"s3:prefix": [f"{RECORD_PREFIX}*"]}}),
        Grant("logs:GetLogEvents", (f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:{LOG_GROUP}:log-stream:*",), {}),
        Grant("ssm:GetParameter", (f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter{SIGNED}",), {}),
    ]


def held_conditions(statement: dict[str, Any]) -> dict[tuple[str, str], list[str]]:
    """The conditions a statement holds, by operator and key, with the values sorted."""
    return {(operator, key): sorted(map(str, as_list(value))) for operator, keys in statement.get("Condition", {}).items() for key, value in keys.items()}


def allowlist_problems(documents: Role, families: list[str]) -> list[str]:
    """Every way the policies of a role leave the allowed set: an unknown element, action or resource, a wildcard, a missing condition or a missing grant."""
    grants = allowed_grants(families)
    found: list[str] = []
    granted: set[tuple[str, str]] = set()
    for name, document in documents.items():
        if set(document) != {"Version", "Statement"} or document["Version"] != "2012-10-17":
            found.append(f"{name}: the document holds more than Version 2012-10-17 and Statement")
        for statement in document["Statement"]:
            label = f"{name}/{statement.get('Sid')}"
            unknown = sorted(set(statement) - STATEMENT_KEYS)
            if unknown:
                found.append(f"{label}: holds an element outside Sid, Effect, Action, Resource and Condition ({', '.join(unknown)})")
                continue
            if statement.get("Effect") == "Deny":
                continue
            if statement.get("Effect") != "Allow" or "Action" not in statement or "Resource" not in statement:
                found.append(f"{label}: is not an Allow statement with an Action and a Resource")
                continue
            held = held_conditions(statement)
            for action in as_list(statement["Action"]):
                entries = [grant for grant in grants if grant.action == action]
                if not entries:
                    found.append(f"{label}: the action {action} is not in the allowed set")
                    continue
                for resource in as_list(statement["Resource"]):
                    matching = [grant for grant in entries if resource in grant.resources]
                    if not matching:
                        found.append(f"{label}: {action} on {resource} is outside the allowed resources")
                    elif not any(
                        all(held.get((operator, key)) == sorted(values) for operator, keys in grant.conditions.items() for key, values in keys.items())
                        for grant in matching
                    ):
                        found.append(f"{label}: {action} on {resource} lacks a required condition")
                    else:
                        granted.add((action, resource))
    found += [
        f"the allowed grant {grant.action} on {resource} is not given by any statement"
        for grant in grants
        for resource in grant.resources
        if (grant.action, resource) not in granted
    ]
    return found


def role_documents(directory: Path, families: list[str]) -> Role:
    """The policies of one role, rendered from a folder of templates. The request policy exists only while bronze or serve is named."""
    documents = {"jobs": render(directory / JOBS_TEMPLATE.name, families), "records": render(directory / RECORDS_TEMPLATE.name, families)}
    if any(family in REQUEST_FILES for family in families):
        documents["requests"] = render(directory / REQUESTS_TEMPLATE.name, families)
    return documents


def evaluate_role(documents: Role, rows: list[Case]) -> list[dict[str, Any]]:
    """Decide each request against all policies of the role together."""
    attached = list(documents.values())
    observations = []
    for label, action, resource, expected, context in rows:
        decision = decide(attached, Request(action, resource, context))
        observations.append(
            {"case": label, "expected": expected, "observed": decision.allowed, "passed": decision.allowed == expected, "statements": decision.statements}
        )
    return observations


def size_checks(label: str, documents: Role) -> list[dict[str, Any]]:
    """The aggregate of the inline policies of one role stays under the IAM limit, white space not counted."""
    size = sum(len(re.sub(r"\s", "", json.dumps(document, separators=(",", ":")))) for document in documents.values())
    return [
        {
            "case": f"the inline policies of the {label} role fit the IAM aggregate limit",
            "expected": INLINE_LIMIT,
            "observed": size,
            "passed": size <= INLINE_LIMIT,
        }
    ]


def allowlist_checks(label: str, documents: Role, families: list[str]) -> list[dict[str, Any]]:
    """The closed check as report rows: one passing row when the role is inside the allowed set, else one failing row per problem."""
    problems = allowlist_problems(documents, families)
    if not problems:
        return [{"case": f"allowlist: the {label} role holds exactly the allowed grants", "expected": True, "observed": True, "passed": True}]
    return [{"case": f"allowlist: {problem}", "expected": True, "observed": False, "passed": False} for problem in problems]


def verify(output: Path, directory: Path = JOBS_TEMPLATE.parent) -> int:
    documents = role_documents(directory, FAMILIES)
    train_only = role_documents(directory, ["train"])
    rows = with_empty_context(job_cases() + record_cases() + request_cases()) + matrix_cases()
    checks = allowlist_checks("six family", documents, FAMILIES) + allowlist_checks("train only", train_only, ["train"])
    checks += evaluate_role(documents, rows)
    checks += evaluate_role(train_only, train_only_cases())
    checks += size_checks("six family", documents) + size_checks("nine family", role_documents(directory, ALL_FAMILIES))
    defects = []
    for name, label, changed in widenings(documents) + planted(documents):
        if changed == documents[name]:
            defects.append({"defect": f"{name}_{label}", "caught": False, "failed_cases": ["the planted change left the policy as it was"]})
            continue
        widened = {**documents, name: changed}
        outside = allowlist_problems(widened, FAMILIES)
        try:
            failures = [item["case"] for item in evaluate_role(widened, rows) if not item["passed"]]
        except UnsupportedPolicyElement as error:
            failures = [f"the evaluator refuses the policy: {error}"]
        defects.append({"defect": f"{name}_{label}", "caught": bool(failures) and bool(outside), "allowlist": outside[:2], "failed_cases": failures[:3]})
    output.mkdir(parents=True, exist_ok=False)
    (output / "report.json").write_text(json.dumps({"checks": checks, "defects": defects}, indent=2) + "\n")
    failed = [item["case"] for item in checks if not item["passed"]] + [
        f"defect not caught by both the allowlist and the cases: {item['defect']}" for item in defects if not item["caught"]
    ]
    sys.stdout.write(f"{len(checks)} cases, {len(defects)} planted defects, {len(failed)} problems\n")
    for line in failed:
        sys.stdout.write(f"FAIL {line}\n")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--policy-directory", type=Path, default=JOBS_TEMPLATE.parent, help="The folder with the three launch policy templates to inspect.")
    arguments = parser.parse_args()
    return verify(arguments.output, arguments.policy_directory)


if __name__ == "__main__":
    sys.exit(main())
