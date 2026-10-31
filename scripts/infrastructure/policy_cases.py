"""The request cases and the approved action inventory that the per-service IAM policies are held to.

The inventory is written from the owner's approved permissions list, not read from the policy files, so a policy that gains or loses an action
no longer equals it. Every approved action has one request that must be allowed and, where the action is scoped, requests in another region,
another account and another project that must not be. The tag cases cover each creating action and each tag change or removal. The forbidden
cases name actions that the project profile must never hold, each with the most permissive context a caller could send.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

from scripts.infrastructure.policy_evaluator import Request, as_list, build_request

R, A, P, E, B = "eu-west-1", "111111111111", "example_project", "dev", "example-project-ci-bucket"
VALUES = {"AWS_REGION": R, "AWS_ACCOUNT_ID": A, "PROJECT_NAME": P, "ENVIRONMENT": E, "DATA_BUCKET_NAME": B}
TAG = {"Project": P}
SERVICES = ("autoscaling", "cloudwatch", "dlm", "ec2", "ecr", "ecs", "iam", "logs", "s3", "secretsmanager", "sns", "ssm", "sts")
USER = "example_project_dev"
LEAF = "0" * 64


@dataclass(frozen=True)
class Case:
    """One request, whether it must be allowed and, for a refusal that a named statement owns, that statement."""

    label: str
    request: Request
    allowed: bool
    sid: str | None = None
    principal: str = "user"


def arn(service: str, rest: str, region: bool = True, account: bool = True) -> str:
    return f"arn:aws:{service}:{R if region else ''}:{A if account else ''}:{rest}"


REPOSITORY = arn("ecr", f"repository/{P}_spark_{E}")
CLUSTER = arn("ecs", f"cluster/{P}_cluster_{E}")
CAPACITY_PROVIDER = arn("ecs", f"capacity-provider/{P}_host_{E}")
SERVICE = arn("ecs", f"service/{P}_cluster_{E}/{P}_{E}_airflow")
DEPLOYMENT = arn("ecs", f"service-deployment/{P}_cluster_{E}/{P}_{E}_airflow/0123456789abcdef")
TASK_DEFINITION = arn("ecs", f"task-definition/{P}_{E}_airflow:3")
TASK = arn("ecs", f"task/{P}_cluster_{E}/0123456789abcdef0123456789abcdef")
CONTAINER_INSTANCE = arn("ecs", f"container-instance/{P}_cluster_{E}/0123456789abcdef0123456789abcdef")
LOG_GROUP = arn("logs", f"log-group:{P}_jobs_{E}")
TOPIC = arn("sns", f"{P}_alerts_{E}")
BUDGET_TOPIC = arn("sns", f"{P}_budget_alerts_{E}")
ALARM = arn("cloudwatch", f"alarm:{P}_run_failed_{E}")
SCHEDULE = arn("dlm", "policy/policy-0123456789abcdef0")
VOLUME = arn("ec2", "volume/vol-0123456789abcdef0")
ADDRESS = arn("ec2", "elastic-ip/eipalloc-0123456789abcdef0")
INSTANCE = arn("ec2", "instance/i-0123456789abcdef0")
HOST_TAGS = {"aws:ResourceTag/Project": P, "aws:ResourceTag/Name": f"{P}_host_{E}"}
GROUP = arn("autoscaling", f"autoScalingGroup:00000000-0000-0000-0000-000000000000:autoScalingGroupName/{P}_host_{E}")
# ECS adds this tag to the host group when it creates the capacity provider; the call is checked against the project user's policy.
ECS_GROUP_TAG = {"AmazonECSManaged": ""}
BUDGET = f"arn:aws:budgets::{A}:budget/{P}_platform_{E}"
DOCUMENT = f"arn:aws:ssm:{R}::document/AWS-StartPortForwardingSession"
SESSION = arn("ssm", f"session/{USER}-0123456789abcdef")
PUBLIC_PARAMETER = f"arn:aws:ssm:{R}::parameter/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id"
# The lakehouse roles take their names from the Terraform variable defaults, whatever PROJECT_NAME is, so the oracle names them outright.
READER = f"arn:aws:iam::{A}:role/healthcare_hai_prediction_lakehouse_reader"
WRITER = f"arn:aws:iam::{A}:role/healthcare_hai_prediction_lakehouse_writer"
# The Athena reader role of the IAM stack, named from PROJECT_NAME and ENVIRONMENT. The project user may assume it and holds no Athena or Glue grant itself.
ATHENA_READER = f"arn:aws:iam::{A}:role/{P}_athena_reader_{E}"
PROJECT_NAME_ROLES = (f"arn:aws:iam::{A}:role/{P}_lakehouse_reader", f"arn:aws:iam::{A}:role/{P}_lakehouse_writer")
BUCKET = f"arn:aws:s3:::{B}"
PARAMETERS = (
    "airflow_api_secret_key",
    "airflow_db_password",
    "airflow_fernet_key",
    "airflow_jwt_secret",
    "grafana_admin_password",
    "marquez_db_password",
    "mlflow_admin_password",
    "mlflow_db_password",
    "polaris_gold_client_id",
    "polaris_gold_client_secret",
    "polaris_reader_client_id",
    "polaris_reader_client_secret",
    "polaris_root_secret",
    "polaris_silver_client_id",
    "polaris_silver_client_secret",
    "postgres_password",
    "redis_password",
)
READ_ONLY_PARAMETERS = ("airflow_admin",)
# The old hand-made Secrets Manager copy of the Airflow administrator password; no policy grants it since the one-time copy was done.
LEGACY_AIRFLOW_ADMIN = "healthcare-hai-prediction/airflow-admin"
# The nine control-plane parameters the task execution role injects; each is one of PARAMETERS.
EXECUTION_PARAMETERS = (
    "postgres_password",
    "airflow_db_password",
    "marquez_db_password",
    "airflow_fernet_key",
    "airflow_jwt_secret",
    "airflow_api_secret_key",
    "redis_password",
    "polaris_root_secret",
    "grafana_admin_password",
)
SHARED_KEYS = ("bls_api_key", "census_api_key", "hud_api_key", "jev_api_key")
AWS_READER_PARAMETERS = ("aws_polaris_reader_client_id", "aws_polaris_reader_client_secret")
# The AWS catalog setup's writer pairs: the project user writes all twelve AWS Polaris names and reads these ten besides the reader pair.
AWS_WRITER_PARAMETERS = tuple(
    f"aws_polaris_{identity}_client_{part}" for identity in ("e2e", "bronze", "silver", "gold", "serving") for part in ("id", "secret")
)
AWS_POLARIS_PARAMETERS = (*AWS_READER_PARAMETERS, *AWS_WRITER_PARAMETERS)
JOB_TASK_ROLES = ("job_execution", "job_offline_task", "job_seal_task", "job_read_task")


# The project user's parameter names are the defaults of the Terraform root variables, whatever PROJECT_NAME is, so the oracle names them outright.
PARAMETER_FOLDER = "healthcare_hai_prediction"


def parameter(name: str) -> str:
    """The ARN of one of the project user's parameters."""
    return arn("ssm", f"parameter/{PARAMETER_FOLDER}/{name}")


def project_name_parameter(name: str) -> str:
    """The ARN of a parameter in the PROJECT_NAME folder, which no secret-name variable names."""
    return arn("ssm", f"parameter/{P}/{name}")


def role(name: str) -> str:
    return f"arn:aws:iam::{A}:role/{P}_{name}_{E}"


def request(action: str, resource: str, tags: dict[str, str] | None = None, extra: dict[str, Any] | None = None, region: str | None = R) -> Request:
    return build_request(action, resource, region=region, tags=tags, extra=extra)


# Each approved action with the request that must be allowed. The region key is sent only where AWS evaluates it.
def approved_requests() -> dict[str, list[Request]]:
    """Per service, one allowed request for every approved action."""
    tagged = dict(tags=TAG)
    ecs_tagged = {"ecs:propagate-tags": "TASK_DEFINITION"}
    only_cluster = {"ecs:cluster": CLUSTER}
    ops: dict[str, list[Request]] = {name: [] for name in SERVICES}

    def add(service: str, action: str, resource: str, **kwargs: Any) -> None:
        ops[service].append(request(f"{service}:{action}", resource, **kwargs))

    for action in ("DescribeAutoScalingGroups", "DescribeAutoScalingInstances", "DescribeScalingActivities"):
        add("autoscaling", action, "*")
    add("autoscaling", "SetDesiredCapacity", GROUP, region=None)
    add("autoscaling", "CreateOrUpdateTags", GROUP, region=None, tags=ECS_GROUP_TAG)
    add("cloudwatch", "PutMetricAlarm", ALARM, **tagged)
    for action in ("DeleteAlarms", "ListTagsForResource", "TagResource", "UntagResource"):
        add("cloudwatch", action, ALARM, region=None, tags={"DataClassification": "Confidential"} if action.endswith("agResource") else None)
    add("cloudwatch", "DescribeAlarms", "*")
    add("dlm", "CreateLifecyclePolicy", "*", **tagged)
    for action in ("GetLifecyclePolicy", "UpdateLifecyclePolicy", "DeleteLifecyclePolicy", "ListTagsForResource", "TagResource", "UntagResource"):
        add(
            "dlm",
            action,
            SCHEDULE,
            region=None,
            extra={"aws:ResourceTag/Project": P},
            tags={"DataClassification": "x"} if action.endswith("agResource") else None,
        )
    add("dlm", "GetLifecyclePolicies", "*")
    for action in (
        "DescribeAddresses",
        "DescribeAddressesAttribute",
        "DescribeAvailabilityZones",
        "DescribeInstances",
        "DescribeInternetGateways",
        "DescribeNetworkInterfaces",
    ):
        add("ec2", action, "*")
    for action in ("DescribeRouteTables", "DescribeSecurityGroupRules", "DescribeSecurityGroups", "DescribeSnapshots", "DescribeSubnets", "DescribeTags"):
        add("ec2", action, "*")
    for action in ("DescribeVolumes", "DescribeVolumesModifications", "DescribeVpcAttribute", "DescribeVpcs"):
        add("ec2", action, "*")
    add("ec2", "CreateVolume", VOLUME, region=None, **tagged)
    add("ec2", "AllocateAddress", ADDRESS, region=None, **tagged)
    add("ec2", "CreateTags", VOLUME, region=None, tags=TAG, extra={"ec2:CreateAction": "CreateVolume"})
    add("ec2", "DeleteVolume", VOLUME, region=None, extra={"aws:ResourceTag/Project": P})
    add("ec2", "ReleaseAddress", ADDRESS, region=None, extra={"aws:ResourceTag/Project": P})
    add("ec2", "GetConsoleOutput", INSTANCE, region=None, extra=HOST_TAGS)
    add("ecr", "GetAuthorizationToken", "*")
    for action in ("BatchCheckLayerAvailability", "InitiateLayerUpload", "UploadLayerPart", "CompleteLayerUpload", "PutImage", "BatchGetImage"):
        add("ecr", action, REPOSITORY, region=None)
    for action in ("GetDownloadUrlForLayer", "DescribeImages", "ListImages"):
        add("ecr", action, REPOSITORY, region=None)
    add("ecr", "CreateRepository", REPOSITORY, region=None, **tagged)
    for action in ("DeleteRepository", "DescribeRepositories", "PutLifecyclePolicy", "GetLifecyclePolicy", "DeleteLifecyclePolicy", "ListTagsForResource"):
        add("ecr", action, REPOSITORY, region=None)
    for action in ("TagResource", "UntagResource"):
        add("ecr", action, REPOSITORY, region=None, tags={"DataClassification": "Confidential"}, extra={"aws:ResourceTag/Project": P})
    for action in ("ListClusters", "ListTaskDefinitions", "ListTaskDefinitionFamilies", "DescribeTaskDefinition"):
        add("ecs", action, "*")
    add("ecs", "CreateCluster", CLUSTER, region=None, **tagged)
    add("ecs", "CreateCapacityProvider", CAPACITY_PROVIDER, region=None, **tagged)
    for action in ("DeleteCluster", "DescribeClusters", "UpdateCluster", "UpdateClusterSettings", "PutClusterCapacityProviders", "ListContainerInstances"):
        add("ecs", action, CLUSTER, region=None)
    add("ecs", "ListTagsForResource", CLUSTER, region=None)
    add("ecs", "DescribeContainerInstances", CONTAINER_INSTANCE, region=None)
    for action in ("UpdateCapacityProvider", "DeleteCapacityProvider", "DescribeCapacityProviders"):
        add("ecs", action, CAPACITY_PROVIDER, region=None)
    add("ecs", "CreateService", SERVICE, region=None, extra=ecs_tagged, **tagged)
    for action in ("UpdateService", "DeleteService", "DescribeServices", "ListTagsForResource", "DescribeServiceDeployments", "ListServiceDeployments"):
        add("ecs", action, SERVICE, region=None)
    add("ecs", "DescribeServiceDeployments", DEPLOYMENT, region=None)
    add("ecs", "ListServices", "*", region=None, extra=only_cluster)
    add("ecs", "ListServices", SERVICE, region=None)
    add("ecs", "RegisterTaskDefinition", TASK_DEFINITION, region=None, **tagged)
    add("ecs", "RunTask", TASK_DEFINITION, region=None, extra=only_cluster, **tagged)
    for action in ("StopTask", "DescribeTasks"):
        add("ecs", action, TASK, region=None)
    add("ecs", "ListTasks", "*", region=None, extra=only_cluster)
    add("ecs", "TagResource", SERVICE, region=None, tags=TAG, extra={"ecs:CreateAction": "CreateService"})
    add("iam", "PassRole", role("task_execution"), region=None, extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"})
    add("iam", "PassRole", role("snapshots"), region=None, extra={"iam:PassedToService": "dlm.amazonaws.com"})
    for name in JOB_TASK_ROLES:
        add("iam", "PassRole", role(name), region=None, extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"})
    for simulated in (role("job_read_task"), READER, WRITER):
        add("iam", "SimulatePrincipalPolicy", simulated, region=None)
    add("logs", "CreateLogGroup", LOG_GROUP, region=None, **tagged)
    for action in (
        "DeleteLogGroup",
        "PutRetentionPolicy",
        "DeleteRetentionPolicy",
        "ListTagsForResource",
        "ListTagsLogGroup",
        "UntagResource",
        "GetLogEvents",
        "FilterLogEvents",
    ):
        add("logs", action, LOG_GROUP, region=None, tags={"DataClassification": "Confidential"} if action.endswith("agResource") else None)
    add("logs", "TagResource", LOG_GROUP, region=None, tags={"DataClassification": "Confidential"})
    add("logs", "DescribeLogGroups", "*")
    add("secretsmanager", "GetSecretValue", arn("secretsmanager", "secret:bls_api_key-AbCdEf"), region=None)
    add("secretsmanager", "DescribeSecret", arn("secretsmanager", "secret:jev_api_key-AbCdEf"), region=None)
    add("sns", "CreateTopic", TOPIC, region=None, **tagged)
    for action in ("DeleteTopic", "GetTopicAttributes", "SetTopicAttributes", "ListTagsForResource", "TagResource", "UntagResource"):
        add("sns", action, TOPIC, region=None, tags={"DataClassification": "x"} if action.endswith("agResource") else None)
    for name in (*PARAMETERS, *READ_ONLY_PARAMETERS):
        add("ssm", "GetParameter", parameter(name), region=None)
    for name in PARAMETERS:
        add("ssm", "PutParameter", parameter(name), region=None)
    for name in AWS_POLARIS_PARAMETERS:
        add("ssm", "GetParameter", parameter(name), region=None)
        add("ssm", "PutParameter", parameter(name), region=None)
    add("ssm", "GetParameters", PUBLIC_PARAMETER, region=None)
    add(
        "ssm",
        "StartSession",
        INSTANCE,
        region=None,
        extra={"ssm:resourceTag/Project": P, "ssm:resourceTag/Name": f"{P}_host_{E}", "ssm:SessionDocumentAccessCheck": "true"},
    )
    add("ssm", "StartSession", DOCUMENT, region=None)
    for action in ("ResumeSession", "TerminateSession"):
        add("ssm", action, SESSION, region=None, extra={"aws:username": USER})
    for target in (READER, WRITER, ATHENA_READER):
        add("sts", "AssumeRole", target, region=None)
    add("sts", "AssumeRole", role("job_try_writer"), region=None, extra={"sts:RoleSessionName": "hai_try_example"})
    add("s3", "GetObjectVersion", f"{BUCKET}/lakehouse/run_stage/run/inputs/pins.json", region=None)
    return ops


def bucket_requests() -> list[Request]:
    """One allowed request for each approved S3 action on the bucket or a legacy capture key."""
    bucket_actions = (
        "ListBucket",
        "GetBucketLocation",
        "GetBucketAcl",
        "GetBucketCORS",
        "GetBucketWebsite",
        "GetBucketVersioning",
        "GetBucketLogging",
        "GetBucketNotification",
        "GetBucketObjectLockConfiguration",
        "GetBucketOwnershipControls",
        "GetBucketPolicy",
        "GetBucketPolicyStatus",
        "GetBucketPublicAccessBlock",
        "GetBucketRequestPayment",
        "GetBucketTagging",
        "GetAccelerateConfiguration",
        "GetEncryptionConfiguration",
        "GetLifecycleConfiguration",
        "GetReplicationConfiguration",
        "PutBucketVersioning",
        "PutLifecycleConfiguration",
        "PutEncryptionConfiguration",
        "PutBucketPublicAccessBlock",
        "PutBucketOwnershipControls",
        "PutBucketPolicy",
        "PutBucketTagging",
        "ListBucketVersions",
        "ListBucketMultipartUploads",
    )
    object_actions = (
        "GetObject",
        "GetObjectVersion",
        "GetObjectAttributes",
        "PutObject",
        "GetObjectTagging",
        "PutObjectTagging",
        "AbortMultipartUpload",
        "ListMultipartUploadParts",
    )
    found = [request(f"s3:{action}", BUCKET, region=None) for action in bucket_actions]
    found.append(request("s3:CreateBucket", BUCKET, region=None, extra={"s3:LocationConstraint": R}))
    found += [request(f"s3:{action}", f"{BUCKET}/raw/x", region=None) for action in object_actions]
    found += [
        request(f"s3:{action}", f"{BUCKET}/lakehouse/silver/x", region=None)
        for action in ("GetObject", "PutObject", "DeleteObject", "AbortMultipartUpload", "ListMultipartUploadParts")
    ]
    return found


def single_copy_cases() -> Iterator[Case]:
    """The single-copy folder cases of the earlier plan test, replayed against the renamed S3 policy."""
    for folder in ("bronze_objects", "bronze_evidence"):
        for suffix in (
            f"sha256/{LEAF}/references/x",
            f"sha256/{LEAF}/data/x",
            f"sha256/{LEAF}/audit/x",
            f"sha256/{LEAF}/manifests/x",
            f"sha256/{LEAF}/datasets/x",
            f"sha256/{LEAF}/extra",
            f"sha256/{LEAF}/a/datasets/x",
            "references/x",
            "data/x",
            "audit/x",
            "manifests/x",
            "other/a/datasets/x",
        ):
            for action in ("PutObject", "PutObjectTagging", "PutObjectVersionTagging", "AbortMultipartUpload"):
                for header in (True, False):
                    yield s3_case(f"single copy: {action} {folder}/{suffix} header={header}", action, f"{folder}/{suffix}", header, False)
        for leaf in ("", "abc123", f"{LEAF}a", "0" * 63, f"{LEAF}%2Fx", f"{'0' * 61}%2F", f"%{'0' * 63}", f"{'0' * 63}/"):
            for header in (True, False):
                yield s3_case(f"single copy: PutObject leaf {leaf!r} header={header}", "PutObject", f"{folder}/sha256/{leaf}", header, False)
        yield s3_case(f"single copy: create once {folder}", "PutObject", f"{folder}/sha256/{LEAF}", True, True)
        yield s3_case(f"single copy: create without the header {folder}", "PutObject", f"{folder}/sha256/{LEAF}", False, False)
        for action in ("GetObject", "GetObjectVersion"):
            yield s3_case(f"single copy: {action} {folder}", action, f"{folder}/sha256/{LEAF}", False, True)
        for action in ("PutObjectTagging", "AbortMultipartUpload", "DeleteObject"):
            yield s3_case(f"single copy: {action} {folder} refused", action, f"{folder}/sha256/{LEAF}", True, False)
    yield s3_case("legacy capture key keeps PutObject", "PutObject", "cms/hospitals/references/capture/x", False, True)
    yield s3_case("legacy dataset key keeps PutObjectTagging", "PutObjectTagging", "cms/hospitals/datasets/ds/x", False, True)
    yield s3_case("legacy audit key keeps AbortMultipartUpload", "AbortMultipartUpload", "cms/hospitals/audit/run/x", False, True)
    for action in ("DeleteObject", "DeleteObjectVersion", "PutObjectAcl"):
        yield s3_case(f"no {action} on a legacy capture key", action, "raw/x", True, False)
        yield s3_case(f"no {action} outside lakehouse", action, "other/x", True, False)
    yield s3_case("lakehouse tables are managed", "DeleteObject", "lakehouse/silver/t/data/x.parquet", False, True)
    yield s3_case("no whole-bucket object write", "PutObject", "unlisted/x", False, False)


def s3_case(label: str, action: str, key: str, header: bool, allowed: bool) -> Case:
    """An S3 object request; the header flag sets whether ``If-None-Match`` is sent (``s3:if-none-match`` is then not null)."""
    extra = {"s3:if-none-match": "*"} if header else {}
    return Case(label, request(f"s3:{action}", f"{BUCKET}/{key}", extra=extra, region=None), allowed)


def widen(resource: str) -> list[tuple[str, str, str | None]]:
    """Variants of a resource ARN in another region, account and project, with the context region they replace."""
    found = []
    for label, old, new in (("region", R, "us-east-1"), ("account", A, "222222222222"), ("project", P, "other_project"), ("environment", f"_{E}", "_prod")):
        if old in resource:
            found.append((label, resource.replace(old, new), new if label == "region" else None))
    return found


def scope_cases(operations: dict[str, list[Request]]) -> Iterator[Case]:
    """Every approved request must be allowed; the same request aimed at another region, account, project or environment must not be."""
    for requests in operations.values():
        for item in requests:
            yield Case(f"{item.action} on {item.resource.rsplit(':', 1)[-1][:60]}", item, True)
            for label, resource, region in widen(item.resource):
                context = dict(item.context)
                if label == "region" and "aws:RequestedRegion" in context:
                    context["aws:RequestedRegion"] = (region or R,)
                if label == "project":
                    for key, values in context.items():
                        if key != "aws:username":
                            context[key] = tuple(value.replace(P, "other_project") for value in values)
                yield Case(f"{item.action} in another {label}: {resource.rsplit(':', 1)[-1][:60]}", Request(item.action, resource, context), False)
            if "aws:RequestedRegion" in item.context and item.resource == "*":
                yield Case(
                    f"{item.action} with the region of another project",
                    Request(item.action, item.resource, {**item.context, "aws:RequestedRegion": ("us-east-1",)}),
                    False,
                )


def tag_variants(action: str, resource: str, extra: dict[str, Any] | None = None, region: str | None = None) -> Iterator[Case]:
    """One creating request with the exact tag (allowed) and the seven ways to get the tag wrong (refused)."""
    yield Case(f"create {action}: exact Project tag", request(action, resource, tags=TAG, extra=extra, region=region), True)
    yield Case(
        f"create {action}: exact Project tag and another tag", request(action, resource, tags={**TAG, "Environment": E}, extra=extra, region=region), True
    )
    bad: dict[str, dict[str, str]] = {
        "no tags": {},
        "lowercase key": {"project": P},
        "uppercase key": {"PROJECT": P},
        "padded key": {"Project ": P},
        "another value": {"Project": "other_project"},
        "other tags only": {"Environment": E},
        "lowercase key and another tag": {"project": P, "Environment": E},
    }
    for name, tags in bad.items():
        yield Case(f"create {action}: {name}", request(action, resource, tags=tags or None, extra=extra, region=region), False)


CREATES: tuple[tuple[str, str, dict[str, Any]], ...] = (
    ("ecr:CreateRepository", REPOSITORY, {}),
    ("ecs:CreateCluster", CLUSTER, {}),
    ("ecs:CreateCapacityProvider", CAPACITY_PROVIDER, {}),
    ("ecs:CreateService", SERVICE, {"ecs:propagate-tags": "TASK_DEFINITION"}),
    ("ecs:RegisterTaskDefinition", TASK_DEFINITION, {}),
    ("ecs:RunTask", TASK_DEFINITION, {"ecs:cluster": CLUSTER}),
    ("ec2:CreateVolume", VOLUME, {}),
    ("ec2:AllocateAddress", ADDRESS, {}),
    ("logs:CreateLogGroup", LOG_GROUP, {}),
    ("sns:CreateTopic", TOPIC, {}),
    ("dlm:CreateLifecyclePolicy", "*", {}),
    ("cloudwatch:PutMetricAlarm", ALARM, {}),
)


def container_instance_cases() -> Iterator[Case]:
    """DescribeContainerInstances acts on container-instance ARNs of the project cluster, not on the cluster ARN (review round 5 finding 6)."""
    yield Case(
        "ecs:DescribeContainerInstances on the cluster ARN (wrong resource type)", request("ecs:DescribeContainerInstances", CLUSTER, region=None), False
    )
    other = CONTAINER_INSTANCE.replace(f"{P}_cluster_{E}", f"{P}_other_cluster_{E}")
    yield Case("ecs:DescribeContainerInstances in another cluster of the project", request("ecs:DescribeContainerInstances", other, region=None), False)
    yield Case("ecs:ListContainerInstances on the cluster ARN", request("ecs:ListContainerInstances", CLUSTER, region=None), True)


def conflicting_project_tag_cases() -> Iterator[Case]:
    """A task, service or tag request that sets its own Project tag is refused when the value is not the project's."""
    sid = "DenyAnotherProjectTagOnRunTaskServicesAndTags"
    run = {"ecs:cluster": CLUSTER}
    yield Case("ecs:RunTask with the project's own Project tag", request("ecs:RunTask", TASK_DEFINITION, tags=TAG, extra=run, region=None), True)
    yield Case(
        "ecs:RunTask with the project tag and another tag",
        request("ecs:RunTask", TASK_DEFINITION, tags={**TAG, "Environment": E}, extra=run, region=None),
        True,
    )
    yield Case("ecs:RunTask with another tag only", request("ecs:RunTask", TASK_DEFINITION, tags={"Environment": E}, extra=run, region=None), False)
    for key in ("Project", "project", "PROJECT"):
        yield Case(
            f"ecs:RunTask sets {key} to another project",
            request("ecs:RunTask", TASK_DEFINITION, tags={key: "other_project"}, extra=run, region=None),
            False,
            "DenyCreatesWithoutProjectTagValue",
        )
        yield Case(
            f"ecs:RunTask sets {key} to an empty value",
            request("ecs:RunTask", TASK_DEFINITION, tags={key: ""}, extra=run, region=None),
            False,
            "DenyCreatesWithoutProjectTagValue",
        )
        yield Case(
            f"ecs:TagResource sets {key} to another project while RunTask",
            request("ecs:TagResource", TASK, tags={key: "other_project"}, extra={"ecs:CreateAction": "RunTask"}, region=None),
            False,
            sid,
        )
    for create_action, resource in (("CreateService", SERVICE), ("CreateCluster", CLUSTER), ("RegisterTaskDefinition", TASK_DEFINITION)):
        yield Case(
            f"ecs:TagResource sets Project to another project while {create_action}",
            request("ecs:TagResource", resource, tags={"Project": "other_project"}, extra={"ecs:CreateAction": create_action}, region=None),
            False,
            sid,
        )
    yield Case(
        "ecs:CreateService sets Project to another project",
        request("ecs:CreateService", SERVICE, tags={"Project": "other_project"}, extra={"ecs:propagate-tags": "SERVICE"}, region=None),
        False,
    )


def tag_cases() -> Iterator[Case]:
    """Tag on create, tag change, tag removal and the ECS propagation rules."""
    for action, resource, extra in CREATES:
        yield from tag_variants(action, resource, extra, region=R if resource == "*" else None)
    for tags, label in (({"ec2:CreateAction": "CreateVolume"}, "volume"), ({"ec2:CreateAction": "AllocateAddress"}, "address")):
        resource = VOLUME if label == "volume" else ADDRESS
        yield Case(f"ec2:CreateTags while creating a {label}", request("ec2:CreateTags", resource, tags=TAG, extra=tags, region=None), True)
        yield Case(
            f"ec2:CreateTags while creating a {label} with a lowercase key",
            request("ec2:CreateTags", resource, tags={"project": P}, extra=tags, region=None),
            False,
        )
        yield Case(
            f"ec2:CreateTags later on a {label} with the project value",
            request("ec2:CreateTags", resource, tags=TAG, extra={"aws:ResourceTag/Project": P}, region=None),
            False,
            "DenyTaggingAfterCreate",
        )
        yield Case(
            f"ec2:CreateTags later on a {label} with another tag",
            request("ec2:CreateTags", resource, tags={"Owner": "x"}, extra={"aws:ResourceTag/Project": P}, region=None),
            False,
            "DenyTaggingAfterCreate",
        )
        yield Case(
            f"ec2:DeleteTags on a {label} removing Project", request("ec2:DeleteTags", resource, tags={"Project": P}, region=None), False, "DenyRemovingTags"
        )
        yield Case(f"ec2:DeleteTags on a {label} deleting every tag", request("ec2:DeleteTags", resource, region=None), False, "DenyRemovingTags")
    families = (("ecr", REPOSITORY), ("logs", LOG_GROUP), ("sns", TOPIC), ("cloudwatch", ALARM), ("dlm", SCHEDULE))
    for service, resource in families:
        owned = {"aws:ResourceTag/Project": P}
        for key in ("Project", "project", "PROJECT"):
            yield Case(
                f"{service}:UntagResource removes {key}",
                request(f"{service}:UntagResource", resource, tags={key: ""}, extra=owned, region=None),
                False,
                "DenyRemovingProjectTag",
            )
            yield Case(
                f"{service}:TagResource changes {key}",
                request(f"{service}:TagResource", resource, tags={key: "other_project"}, extra=owned, region=None),
                False,
                "DenyChangingProjectTag",
            )
        # On tag-on-create AWS already sees the new resource's Project tag as aws:ResourceTag/Project (first deploy, Oct 23 2026).
        yield Case(f"{service}:TagResource sets Project on creation", request(f"{service}:TagResource", resource, tags=TAG, extra=owned, region=None), True)
        yield Case(
            f"{service}:TagResource changes another tag",
            request(f"{service}:TagResource", resource, tags={"DataClassification": "x"}, extra=owned, region=None),
            True,
        )
        yield Case(
            f"{service}:UntagResource removes another tag",
            request(f"{service}:UntagResource", resource, tags={"DataClassification": ""}, extra=owned, region=None),
            True,
        )
    # DLM grants management by the Project tag, so relabelling another project's schedule would hand it over.
    yield Case(
        "dlm:TagResource sets Project on another project's schedule",
        request("dlm:TagResource", SCHEDULE, tags=TAG, extra={"aws:ResourceTag/Project": "other_project"}, region=None),
        False,
    )
    ecs_resources = (CLUSTER, CAPACITY_PROVIDER, SERVICE, TASK_DEFINITION, TASK)
    for resource in ecs_resources:
        name = resource.split(":", 5)[-1].split("/")[0]
        yield Case(
            f"ecs:TagResource later on a {name} with the project value",
            request("ecs:TagResource", resource, tags=TAG, extra={"aws:ResourceTag/Project": P}, region=None),
            False,
            "DenyTagChangesAfterCreate",
        )
        yield Case(
            f"ecs:UntagResource on a {name} removes Project",
            request("ecs:UntagResource", resource, tags=TAG, extra={"aws:ResourceTag/Project": P}, region=None),
            False,
            "DenyTagChangesAfterCreate",
        )
        yield Case(
            f"ecs:UntagResource on a {name} removes another tag",
            request("ecs:UntagResource", resource, tags={"Owner": ""}, region=None),
            False,
            "DenyTagChangesAfterCreate",
        )
    for create_action, resource in (
        ("CreateCluster", CLUSTER),
        ("CreateCapacityProvider", CAPACITY_PROVIDER),
        ("CreateService", SERVICE),
        ("RegisterTaskDefinition", TASK_DEFINITION),
        ("RunTask", TASK),
    ):
        yield Case(
            f"ecs:TagResource while {create_action}",
            request("ecs:TagResource", resource, tags=TAG, extra={"ecs:CreateAction": create_action}, region=None),
            True,
        )
    yield Case(
        "ecs:TagResource with an unknown create action",
        request("ecs:TagResource", CLUSTER, tags=TAG, extra={"ecs:CreateAction": "CreateTaskSet"}, region=None),
        False,
    )
    yield from conflicting_project_tag_cases()
    for value in ("NONE", "none", "", "SERVICES"):
        yield Case(
            f"ecs:CreateService propagates {value!r}",
            request("ecs:CreateService", SERVICE, tags=TAG, extra={"ecs:propagate-tags": value}, region=None),
            False,
            "DenyServicesAndTasksThatDoNotPropagateTags",
        )
    yield Case(
        "ecs:CreateService omits propagation", request("ecs:CreateService", SERVICE, tags=TAG, region=None), False, "DenyServicesAndTasksThatDoNotPropagateTags"
    )
    yield Case(
        "ecs:CreateService propagates the service tags",
        request("ecs:CreateService", SERVICE, tags=TAG, extra={"ecs:propagate-tags": "SERVICE"}, region=None),
        True,
    )
    yield Case(
        "ecs:CreateService propagates the task definition tags",
        request("ecs:CreateService", SERVICE, tags=TAG, extra={"ecs:propagate-tags": "TASK_DEFINITION"}, region=None),
        True,
    )
    yield Case(
        "ecs:UpdateService turns propagation off",
        request("ecs:UpdateService", SERVICE, extra={"ecs:propagate-tags": "NONE"}, region=None),
        False,
        "DenyUpdatesThatStopTagPropagation",
    )
    yield Case("ecs:UpdateService keeps propagation", request("ecs:UpdateService", SERVICE, extra={"ecs:propagate-tags": "SERVICE"}, region=None), True)
    yield Case("ecs:UpdateService leaves propagation as it is", request("ecs:UpdateService", SERVICE, region=None), True)
    yield from run_task_cases()


def run_task_cases() -> Iterator[Case]:
    """RunTask must carry an explicit exact Project tag. Its propagation option is not an IAM condition key."""
    cluster = {"ecs:cluster": CLUSTER}
    for propagation in (None, "NONE", "TASK_DEFINITION"):
        options = {} if propagation is None else {"ecs:propagate-tags": propagation}
        for tags, allowed, label in ((None, False, "without request tags"), (TAG, True, "with explicit Project")):
            yield Case(
                f"ecs:RunTask {label}, propagation={propagation!r}",
                request("ecs:RunTask", TASK_DEFINITION, tags=tags, extra={**cluster, **options}, region=None),
                allowed,
            )
    for resource, context, label in (
        (TASK_DEFINITION, {}, "without a cluster"),
        (TASK_DEFINITION, {"ecs:cluster": CLUSTER.replace("cluster_dev", "cluster_prod")}, "in another cluster"),
        (TASK_DEFINITION.replace(P, "other_project"), cluster, "with another project's task definition"),
    ):
        yield Case(f"ecs:RunTask {label}", request("ecs:RunTask", resource, tags=TAG, extra=context, region=None), False)
    for resource, label in ((SERVICE, "project service"), (SERVICE.replace(P, "other_project"), "another project's service")):
        yield Case(
            f"ecs:UpdateService force deployment on {label}",
            request("ecs:UpdateService", resource, extra=cluster, region=None),
            resource == SERVICE,
        )


def request_context_cases() -> Iterator[tuple[str, Request, dict[str, tuple[str, ...]]]]:
    """AWS request-context projections, with expectations independent of the policy templates."""
    options = {"ecs:cluster": CLUSTER, "ecs:propagate-tags": "TASK_DEFINITION", "ecs:enable-ecs-managed-tags": "true"}
    yield (
        "RunTask propagation does not supply IAM option keys or request tags",
        request("ecs:RunTask", TASK_DEFINITION, extra=options, region=None),
        {"ecs:cluster": (CLUSTER,)},
    )
    yield (
        "RunTask explicit tags supply the exact tag key and value",
        request("ecs:RunTask", TASK_DEFINITION, tags=TAG, extra=options, region=None),
        {"ecs:cluster": (CLUSTER,), "aws:RequestTag/Project": (P,), "aws:TagKeys": ("Project",)},
    )
    yield (
        "RunTask option-key filtering is case insensitive",
        request("ECS:RUNTASK", TASK_DEFINITION, extra={"ECS:PROPAGATE-TAGS": "TASK_DEFINITION"}, region=None),
        {},
    )
    for action in ("ecs:CreateService", "ecs:UpdateService"):
        yield (
            f"{action} supports the propagation and managed-tag condition keys",
            request(action, SERVICE, extra=options, region=None),
            {"ecs:cluster": (CLUSTER,), "ecs:propagate-tags": ("TASK_DEFINITION",), "ecs:enable-ecs-managed-tags": ("true",)},
        )


def session_cases() -> Iterator[Case]:
    """Session Manager: port forwarding to the project host only, and only the caller's own sessions."""
    context = {"ssm:resourceTag/Project": P, "ssm:resourceTag/Name": f"{P}_host_{E}", "ssm:SessionDocumentAccessCheck": "true"}
    yield Case("session: untagged instance", request("ssm:StartSession", INSTANCE, extra={"ssm:SessionDocumentAccessCheck": "true"}, region=None), False)
    yield Case(
        "session: instance of another project",
        request("ssm:StartSession", INSTANCE, extra={**context, "ssm:resourceTag/Project": "other_project"}, region=None),
        False,
    )
    yield Case(
        "session: another host name", request("ssm:StartSession", INSTANCE, extra={**context, "ssm:resourceTag/Name": f"{P}_other_{E}"}, region=None), False
    )
    yield Case(
        "session: document access check off",
        request("ssm:StartSession", INSTANCE, extra={**context, "ssm:SessionDocumentAccessCheck": "false"}, region=None),
        False,
    )
    yield Case("session: shell document", request("ssm:StartSession", f"arn:aws:ssm:{R}::document/SSM-SessionManagerRunShell", region=None), False)
    yield Case(
        "session: another user's session",
        request("ssm:TerminateSession", arn("ssm", "session/other_user-0123456789abcdef"), extra={"aws:username": USER}, region=None),
        False,
    )
    yield Case(
        "session: resume another user's session",
        request("ssm:ResumeSession", arn("ssm", "session/other_user-0123456789abcdef"), extra={"aws:username": USER}, region=None),
        False,
    )


def console_cases() -> Iterator[Case]:
    """Console output: the project host only, so the boot log copied to the console can be read and no other instance's."""
    read = "ec2:GetConsoleOutput"
    yield Case("console: the project host", request(read, INSTANCE, extra=HOST_TAGS, region=None), True)
    yield Case("console: instance without tags", request(read, INSTANCE, region=None), False)
    yield Case(
        "console: instance of another project", request(read, INSTANCE, extra={**HOST_TAGS, "aws:ResourceTag/Project": "other_project"}, region=None), False
    )
    yield Case("console: another host name", request(read, INSTANCE, extra={**HOST_TAGS, "aws:ResourceTag/Name": f"{P}_other_{E}"}, region=None), False)
    yield Case("console: the project tag only", request(read, INSTANCE, extra={"aws:ResourceTag/Project": P}, region=None), False)
    yield Case("console: the name tag only", request(read, INSTANCE, extra={"aws:ResourceTag/Name": f"{P}_host_{E}"}, region=None), False)
    yield Case("console: every instance", request(read, "*", extra=HOST_TAGS, region=None), False)
    for action in ("GetConsoleScreenshot", "GetPasswordData"):
        yield Case(f"console: no ec2:{action} on the project host", request(f"ec2:{action}", INSTANCE, extra=HOST_TAGS, region=None), False)


def parameter_cases() -> Iterator[Case]:
    """Parameter Store: exact names, no batch or path read, no write to the read-only name."""
    yield Case(
        "parameter: overwrite the Airflow administrator password",
        request("ssm:PutParameter", parameter("airflow_admin"), extra={"ssm:Overwrite": "true"}, region=None),
        False,
    )
    yield Case(
        "parameter: create the Airflow administrator password without overwrite",
        request("ssm:PutParameter", parameter("airflow_admin"), extra={"ssm:Overwrite": "false"}, region=None),
        False,
    )
    yield Case(
        "parameter: create the Airflow administrator password with no overwrite flag",
        request("ssm:PutParameter", parameter("airflow_admin"), region=None),
        False,
    )
    yield Case("parameter: read another name in the project folder", request("ssm:GetParameter", parameter("other_secret"), region=None), False)
    yield Case("parameter: write another name in the project folder", request("ssm:PutParameter", parameter("other_secret"), region=None), False)
    yield Case("parameter: read a name without the project folder", request("ssm:GetParameter", arn("ssm", "parameter/postgres_password"), region=None), False)
    yield Case(
        "parameter: read the whole folder as a path", request("ssm:GetParametersByPath", arn("ssm", f"parameter/{PARAMETER_FOLDER}"), region=None), False
    )
    yield Case(
        "parameter: read the whole folder with a wildcard", request("ssm:GetParameter", arn("ssm", f"parameter/{PARAMETER_FOLDER}/*"), region=None), False
    )
    for action in ("ssm:GetParameter", "ssm:PutParameter"):
        yield Case(
            f"parameter: {action} in the folder of another project",
            request(action, parameter("postgres_password").replace(PARAMETER_FOLDER, "other_project"), region=None),
            False,
        )
        yield Case(
            f"parameter: {action} in the PROJECT_NAME folder, which the variable defaults do not name",
            request(action, project_name_parameter("postgres_password"), region=None),
            False,
        )
    yield Case("parameter: batch read of a project name", request("ssm:GetParameters", parameter("postgres_password"), region=None), False)
    yield Case("parameter: single read of the public ECS parameter", request("ssm:GetParameter", PUBLIC_PARAMETER, region=None), False)
    yield Case(
        "parameter: public parameter outside ecs",
        request("ssm:GetParameters", f"arn:aws:ssm:{R}::parameter/aws/service/ami-amazon-linux-latest/x", region=None),
        False,
    )
    yield Case("parameter: write the public ECS parameter", request("ssm:PutParameter", PUBLIC_PARAMETER, region=None), False)
    for name in (
        "ssm:DescribeParameters",
        "ssm:DeleteParameter",
        "ssm:DeleteParameters",
        "ssm:LabelParameterVersion",
        "ssm:GetParameterHistory",
        "kms:Decrypt",
        "kms:GenerateDataKey",
        "kms:CreateGrant",
    ):
        yield Case(f"parameter: no {name}", request(name, parameter("postgres_password"), region=None), False)
    for action in ("secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret", "secretsmanager:PutSecretValue", "secretsmanager:DeleteSecret"):
        yield Case(
            f"secret: no {action} on the old Airflow password",
            request(action, arn("secretsmanager", f"secret:{LEGACY_AIRFLOW_ADMIN}-AbCdEf"), region=None),
            False,
        )
    yield Case(
        "secret: a name that only starts like the legacy Airflow password",
        request("secretsmanager:GetSecretValue", arn("secretsmanager", f"secret:{LEGACY_AIRFLOW_ADMIN}_extra-AbCdEf"), region=None),
        False,
    )
    for secret in ("other_key", "airflow-admin", "bls_api_key_extra"):
        yield Case(
            f"secret: {secret} is not a shared key",
            request("secretsmanager:GetSecretValue", arn("secretsmanager", f"secret:{secret}-AbCdEf"), region=None),
            False,
        )
    yield Case(
        "secret: shared key with a long suffix",
        request("secretsmanager:GetSecretValue", arn("secretsmanager", "secret:bls_api_key-AbCdEfGh"), region=None),
        False,
    )
    yield Case(
        "secret: shared key of another region",
        request("secretsmanager:GetSecretValue", arn("secretsmanager", "secret:bls_api_key-AbCdEf").replace(R, "us-east-1"), region=None),
        False,
    )
    for action in ("CreateSecret", "PutSecretValue", "UpdateSecret", "DeleteSecret", "RotateSecret", "TagResource", "ListSecrets", "GetResourcePolicy"):
        yield Case(f"secret: no {action}", request(f"secretsmanager:{action}", arn("secretsmanager", "secret:bls_api_key-AbCdEf"), region=None), False)
    for secret in SHARED_KEYS:
        yield Case(f"secret: read {secret}", request("secretsmanager:GetSecretValue", arn("secretsmanager", f"secret:{secret}-AbCdEf"), region=None), True)


def pass_role_cases() -> Iterator[Case]:
    """PassRole to the roles each service needs, and no other role or service."""
    for name in ("task_execution", "polaris_task", "control_plane_task"):
        yield Case(
            f"PassRole {name} to ECS tasks", request("iam:PassRole", role(name), extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"}, region=None), True
        )
        for service in ("ec2.amazonaws.com", "dlm.amazonaws.com", "ecs.amazonaws.com", "budgets.amazonaws.com"):
            yield Case(f"PassRole {name} to {service}", request("iam:PassRole", role(name), extra={"iam:PassedToService": service}, region=None), False)
        yield Case(f"PassRole {name} without a service", request("iam:PassRole", role(name), region=None), False)
    for service in ("ecs-tasks.amazonaws.com", "ec2.amazonaws.com", "budgets.amazonaws.com"):
        yield Case(f"PassRole snapshots to {service}", request("iam:PassRole", role("snapshots"), extra={"iam:PassedToService": service}, region=None), False)
    yield Case(
        "PassRole ecs_host is not the user's", request("iam:PassRole", role("ecs_host"), extra={"iam:PassedToService": "ec2.amazonaws.com"}, region=None), False
    )
    for target in (READER, WRITER):
        yield Case(
            f"PassRole lakehouse role {target.rsplit('_', 1)[-1]}",
            request("iam:PassRole", target, extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"}, region=None),
            False,
        )
    for target in (f"arn:aws:iam::{A}:role/{P}_ecs_host_{E}", f"arn:aws:iam::{A}:role/administrator", f"arn:aws:iam::{A}:role/{P}_snapshots_prod"):
        yield Case(f"AssumeRole {target.rsplit('/', 1)[-1]}", request("sts:AssumeRole", target, region=None), False)
    for target in PROJECT_NAME_ROLES:
        yield Case(
            f"AssumeRole {target.rsplit('/', 1)[-1]}, named from PROJECT_NAME, not by the role variables", request("sts:AssumeRole", target, region=None), False
        )
    yield Case("AssumeRole a lakehouse role of another account", request("sts:AssumeRole", READER.replace(A, "222222222222"), region=None), False)
    yield Case("AssumeRole athena_reader", request("sts:AssumeRole", ATHENA_READER, region=None), True, "AssumeAthenaReaderOnly")
    for label, target in (
        ("athena_reader of another environment", ATHENA_READER.replace(f"_{E}", "_other")),
        ("athena_reader of another account", ATHENA_READER.replace(A, "222222222222")),
        ("athena_reader with a suffix", f"{ATHENA_READER}_extra"),
        ("another athena role", f"arn:aws:iam::{A}:role/{P}_athena_writer_{E}"),
    ):
        yield Case(f"AssumeRole {label}", request("sts:AssumeRole", target, region=None), False)
    athena_workgroup = arn("athena", "workgroup/hai_mirror")
    glue_table = arn("glue", "table/hai_mirror_gold/dim_state")
    for action, target in (
        ("athena:StartQueryExecution", athena_workgroup),
        ("athena:GetQueryResults", athena_workgroup),
        ("athena:GetWorkGroup", athena_workgroup),
        ("glue:GetDatabase", arn("glue", "database/hai_mirror_gold")),
        ("glue:GetTable", glue_table),
        ("glue:CreateTable", glue_table),
        ("s3:GetObject", f"{BUCKET}/athena/results/query.csv"),
    ):
        service = action.split(":")[0]
        if service != "s3":
            yield Case(f"the project user has no direct {action}", request(action, target), False)
    for action in ("AssumeRoleWithWebIdentity", "AssumeRoleWithSAML", "GetFederationToken", "GetSessionToken"):
        yield Case(f"sts: no {action}", request(f"sts:{action}", READER, region=None), False)


def job_project_cases() -> Iterator[Case]:
    """The four job grants and their resource, condition and action boundaries for the project user."""
    for name in JOB_TASK_ROLES:
        yield Case(
            f"job pass: {name} to ECS tasks",
            request("iam:PassRole", role(name), extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"}, region=None),
            True,
            "PassReadJobRoles",
        )
        for service in ("ec2.amazonaws.com", "ecs.amazonaws.com", "dlm.amazonaws.com"):
            yield Case(f"job pass: {name} to {service}", request("iam:PassRole", role(name), extra={"iam:PassedToService": service}, region=None), False)
        yield Case(f"job pass: {name} without a service", request("iam:PassRole", role(name), region=None), False)
    for name in ("job_try_writer", "job_other_task", "job_write_other_task"):
        yield Case(
            f"job pass: refuse {name}",
            request("iam:PassRole", role(name), extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"}, region=None),
            False,
        )
    for layer in ("silver", "gold", "features", "bronze"):
        name = f"job_write_{layer}_task"
        yield Case(
            f"layer pass: {name} to ECS tasks",
            request("iam:PassRole", role(name), extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"}, region=None),
            True,
            "PassLayerWriterRoles",
        )
        for service in ("ec2.amazonaws.com", "ecs.amazonaws.com", "dlm.amazonaws.com"):
            yield Case(f"layer pass: {name} to {service}", request("iam:PassRole", role(name), extra={"iam:PassedToService": service}, region=None), False)
        yield Case(f"layer pass: {name} without a service", request("iam:PassRole", role(name), region=None), False)
    for name in ("job_train_task", "mlflow_task"):
        yield Case(
            f"training pass: {name} to ECS tasks",
            request("iam:PassRole", role(name), extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"}, region=None),
            True,
            "PassTrainingRoles",
        )
        for service in ("ec2.amazonaws.com", "ecs.amazonaws.com", "dlm.amazonaws.com"):
            yield Case(f"training pass: {name} to {service}", request("iam:PassRole", role(name), extra={"iam:PassedToService": service}, region=None), False)
        yield Case(f"training pass: {name} without a service", request("iam:PassRole", role(name), region=None), False)
    yield Case(
        "broker pass: broker_task to ECS tasks",
        request("iam:PassRole", role("broker_task"), extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"}, region=None),
        True,
        "PassBrokerRole",
    )
    for service in ("ec2.amazonaws.com", "ecs.amazonaws.com", "dlm.amazonaws.com"):
        yield Case(
            f"broker pass: broker_task to {service}", request("iam:PassRole", role("broker_task"), extra={"iam:PassedToService": service}, region=None), False
        )
    yield Case("broker pass: broker_task without a service", request("iam:PassRole", role("broker_task"), region=None), False)
    for name in ("broker_task_extra", "broker", "job_broker_task"):
        yield Case(
            f"broker pass: refuse {name}",
            request("iam:PassRole", role(name), extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"}, region=None),
            False,
        )
    for name in ("job_train_task_extra", "mlflow_task_extra", "job_train", "mlflow"):
        yield Case(
            f"training pass: refuse {name}",
            request("iam:PassRole", role(name), extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"}, region=None),
            False,
        )
    for session in ("hai_try_example", "hai_try_", "unrelated", "hai_try", "other_hai_try_example", None):
        allowed = session in ("hai_try_example", "hai_try_")
        yield Case(
            f"job assume: session {session!r}",
            request("sts:AssumeRole", role("job_try_writer"), extra={"sts:RoleSessionName": session} if session is not None else None, region=None),
            allowed,
            "AssumeTryOutputRole" if allowed else None,
        )
    for name in (*JOB_TASK_ROLES, "job_other_writer"):
        yield Case(
            f"job assume: refuse {name}",
            request("sts:AssumeRole", role(name), extra={"sts:RoleSessionName": "hai_try_example"}, region=None),
            False,
        )
    yield Case(
        "job stage: versioned input",
        request("s3:GetObjectVersion", f"{BUCKET}/lakehouse/run_stage/run/inputs/code.tar", region=None),
        True,
        "ReadStageVersions",
    )
    for key in ("lakehouse/silver/table/metadata.json", "lakehouse/run_stage_extra/run/input", "lakehouse/run_stage", "unlisted/x"):
        yield Case(f"job stage: refuse {key}", request("s3:GetObjectVersion", f"{BUCKET}/{key}", region=None), False)
    yield Case(
        "job stage: refuse another bucket",
        request("s3:GetObjectVersion", "arn:aws:s3:::other-bucket/lakehouse/run_stage/run/inputs/code.tar", region=None),
        False,
    )
    for name in AWS_READER_PARAMETERS:
        yield Case(f"job reader: read {name}", request("ssm:GetParameter", parameter(name), region=None), True, "SetUpAwsPolarisPairs")
        for action in ("GetParameters", "GetParametersByPath", "GetParameterHistory"):
            yield Case(f"job reader: refuse {action} {name}", request(f"ssm:{action}", parameter(name), region=None), False)
        for suffix in ("_extra", "/child"):
            yield Case(f"job reader: refuse {name}{suffix}", request("ssm:GetParameter", parameter(name + suffix), region=None), False)
    yield Case("job reader: refuse writer parameter", request("ssm:GetParameter", parameter("aws_polaris_writer_client_secret"), region=None), False)


PARAMETER_ACTIONS = (
    "GetParameter",
    "PutParameter",
    "GetParameters",
    "GetParametersByPath",
    "GetParameterHistory",
    "DeleteParameter",
    "DeleteParameters",
    "LabelParameterVersion",
    "UnlabelParameterVersion",
    "AddTagsToResource",
    "RemoveTagsFromResource",
    "ListTagsForResource",
)


def project_parameter_grid_cases() -> Iterator[Case]:
    """Every action on every project parameter: a managed name may be read and put, the Airflow administrator password may be read only, and no other
    action reaches any of them. The grid is written out here, so it holds whatever statements carry the grants."""
    for name in (*PARAMETERS, *READ_ONLY_PARAMETERS):
        allowed = {"GetParameter", "PutParameter"} if name in PARAMETERS else {"GetParameter"}
        for action in PARAMETER_ACTIONS:
            yield Case(f"parameter grid: {action} {name}", request(f"ssm:{action}", parameter(name), region=None), action in allowed)


def aws_polaris_pair_cases() -> Iterator[Case]:
    """The AWS catalog setup under the project user: write the twelve AWS Polaris names, read the ten writer names, nothing wider and no listing."""
    for name in AWS_POLARIS_PARAMETERS:
        for action in ("PutParameter", "GetParameter"):
            yield Case(f"aws pair: {action} {name}", request(f"ssm:{action}", parameter(name), region=None), True, "SetUpAwsPolarisPairs")
        for action in ("GetParameters", "GetParametersByPath", "GetParameterHistory", "DeleteParameter", "DeleteParameters", "LabelParameterVersion"):
            yield Case(f"aws pair: refuse {action} {name}", request(f"ssm:{action}", parameter(name), region=None), False)
        for action in ("AddTagsToResource", "RemoveTagsFromResource"):
            yield Case(f"aws pair: refuse {action} {name}", request(f"ssm:{action}", parameter(name), region=None), False)
        for suffix in ("_extra", "/child"):
            for action in ("PutParameter", "GetParameter"):
                yield Case(f"aws pair: refuse {action} {name}{suffix}", request(f"ssm:{action}", parameter(name + suffix), region=None), False)
        for label, resource in (
            ("another region", parameter(name).replace(f":{R}:", ":eu-west-2:")),
            ("another account", parameter(name).replace(f":{A}:", ":222222222222:")),
            ("another folder", parameter(name).replace(f"/{PARAMETER_FOLDER}/", "/another_project/")),
        ):
            for action in ("PutParameter", "GetParameter"):
                yield Case(f"aws pair: refuse {action} {name} in {label}", request(f"ssm:{action}", resource, region=None), False)
    for name in ("aws_polaris_features_client_id", "aws_polaris_features_client_secret", "aws_polaris_root_secret", "aws_polaris_writer_client_secret"):
        for action in ("PutParameter", "GetParameter"):
            yield Case(f"aws pair: refuse {action} {name}", request(f"ssm:{action}", parameter(name), region=None), False)
    yield Case("aws pair: refuse the account-wide parameter listing", request("ssm:DescribeParameters", "*"), False)
    yield Case(
        "aws pair: refuse a listing scoped to a pair name", request("ssm:DescribeParameters", parameter("aws_polaris_gold_client_id"), region=None), False
    )


FORBIDDEN = {
    "ec2": (
        "RunInstances",
        "StartInstances",
        "StopInstances",
        "TerminateInstances",
        "RebootInstances",
        "ModifyInstanceAttribute",
        "ModifyInstanceMetadataOptions",
        "CreateLaunchTemplate",
        "CreateLaunchTemplateVersion",
        "ModifyLaunchTemplate",
        "DeleteLaunchTemplate",
        "DeleteLaunchTemplateVersions",
        "CreateVpc",
        "DeleteVpc",
        "ModifyVpcAttribute",
        "CreateSubnet",
        "DeleteSubnet",
        "CreateInternetGateway",
        "AttachInternetGateway",
        "DetachInternetGateway",
        "DeleteInternetGateway",
        "CreateRouteTable",
        "DeleteRouteTable",
        "CreateRoute",
        "DeleteRoute",
        "AssociateRouteTable",
        "CreateSecurityGroup",
        "DeleteSecurityGroup",
        "AuthorizeSecurityGroupIngress",
        "AuthorizeSecurityGroupEgress",
        "RevokeSecurityGroupIngress",
        "RevokeSecurityGroupEgress",
        "CreateNatGateway",
        "CreateVpcEndpoint",
        "CopySnapshot",
        "CreateSnapshot",
        "CreateSnapshots",
        "DeleteSnapshot",
        "ModifySnapshotAttribute",
        "CopyImage",
        "CreateImage",
        "RegisterImage",
        "ImportImage",
        "ImportSnapshot",
        "ImportVolume",
        "ModifyVolume",
        "AttachVolume",
        "DetachVolume",
        "AssociateAddress",
        "DisassociateAddress",
        "EnableFastSnapshotRestores",
        "CreateReplaceRootVolumeTask",
        "CreateRestoreImageTask",
        "CreateStoreImageTask",
        "RequestSpotInstances",
        "CreateFleet",
        "RunScheduledInstances",
        "CreateKeyPair",
        "GetConsoleScreenshot",
        "GetPasswordData",
    ),
    "autoscaling": (
        "CreateAutoScalingGroup",
        "UpdateAutoScalingGroup",
        "StartInstanceRefresh",
        "CancelInstanceRefresh",
        "DeleteAutoScalingGroup",
        "AttachInstances",
        "DetachInstances",
        "EnterStandby",
        "ExitStandby",
        "SetInstanceProtection",
        "SetInstanceHealth",
        "PutScalingPolicy",
        "PutScheduledUpdateGroupAction",
        "CreateOrUpdateTags",
        "DeleteTags",
        "CreateLaunchConfiguration",
        "DeleteLaunchConfiguration",
        "TerminateInstanceInAutoScalingGroup",
        "SuspendProcesses",
        "ResumeProcesses",
        "AttachLoadBalancerTargetGroups",
        "PutWarmPool",
        "PutLifecycleHook",
        "RollbackInstanceRefresh",
    ),
    "budgets": (
        "ModifyBudget",
        "DeleteBudget",
        "CreateBudget",
        "CreateBudgetAction",
        "UpdateBudgetAction",
        "DeleteBudgetAction",
        "ExecuteBudgetAction",
        "UpdateNotification",
        "DeleteNotification",
        "CreateNotification",
        "CreateSubscriber",
        "DeleteSubscriber",
        "UpdateSubscriber",
        "TagResource",
        "UntagResource",
    ),
    "iam": (
        "CreateRole",
        "DeleteRole",
        "UpdateAssumeRolePolicy",
        "PutRolePolicy",
        "AttachRolePolicy",
        "DetachRolePolicy",
        "CreatePolicy",
        "CreatePolicyVersion",
        "DeletePolicy",
        "SetDefaultPolicyVersion",
        "AttachUserPolicy",
        "DetachUserPolicy",
        "PutUserPolicy",
        "DeleteUserPolicy",
        "AttachGroupPolicy",
        "AddUserToGroup",
        "CreateGroup",
        "CreateAccessKey",
        "CreateUser",
        "CreateServiceLinkedRole",
        "CreateInstanceProfile",
        "AddRoleToInstanceProfile",
        "PutUserPermissionsBoundary",
        "UpdateLoginProfile",
    ),
    "ecs": (
        "DeregisterTaskDefinition",
        "DeleteTaskDefinitions",
        "StartTask",
        "ExecuteCommand",
        "CreateTaskSet",
        "UpdateTaskSet",
        "DeleteTaskSet",
        "RegisterContainerInstance",
        "DeregisterContainerInstance",
        "SubmitTaskStateChange",
        "PutAccountSetting",
        "PutAccountSettingDefault",
        "UpdateContainerInstancesState",
        "UpdateContainerAgent",
        "PutAttributes",
        "DiscoverPollEndpoint",
    ),
    "ecr": (
        "BatchDeleteImage",
        "SetRepositoryPolicy",
        "DeleteRepositoryPolicy",
        "PutRegistryPolicy",
        "PutReplicationConfiguration",
        "CreatePullThroughCacheRule",
        "StartImageScan",
        "PutImageTagMutability",
        "PutImageScanningConfiguration",
        "ReplicateImage",
        "BatchImportUpstreamImage",
        "CreateRepositoryCreationTemplate",
    ),
    "logs": (
        "PutResourcePolicy",
        "CreateLogDelivery",
        "PutLogEvents",
        "CreateLogStream",
        "DeleteLogStream",
        "PutMetricFilter",
        "PutSubscriptionFilter",
        "AssociateKmsKey",
        "StartQuery",
        "CreateExportTask",
        "PutDestination",
    ),
    "cloudwatch": (
        "PutMetricData",
        "PutDashboard",
        "DeleteDashboards",
        "SetAlarmState",
        "EnableAlarmActions",
        "DisableAlarmActions",
        "PutCompositeAlarm",
        "PutAnomalyDetector",
    ),
    "sns": ("Publish", "Subscribe", "Unsubscribe", "SetSubscriptionAttributes", "AddPermission", "RemovePermission", "ListTopics"),
    "dlm": ("CreateLifecyclePolicy_other",),
    "kms": (
        "Decrypt",
        "Encrypt",
        "GenerateDataKey",
        "GenerateDataKeyWithoutPlaintext",
        "CreateGrant",
        "CreateKey",
        "DescribeKey",
        "ScheduleKeyDeletion",
        "PutKeyPolicy",
    ),
    "elasticloadbalancing": (
        "CreateLoadBalancer",
        "CreateTargetGroup",
        "CreateListener",
        "ModifyLoadBalancerAttributes",
        "DeleteLoadBalancer",
        "DescribeLoadBalancers",
    ),
    "ssm": (
        "SendCommand",
        "StartAutomationExecution",
        "CreateAssociation",
        "StartSession_shell",
        "DeleteParameter",
        "DescribeParameters",
        "GetParametersByPath",
        "DescribeInstanceInformation",
    ),
    "secretsmanager": ("CreateSecret", "PutSecretValue", "ListSecrets", "DeleteSecret", "PutResourcePolicy"),
    "sts": ("GetFederationToken", "AssumeRoleWithWebIdentity"),
    "lambda": ("CreateFunction", "InvokeFunction"),
    "cloudformation": ("CreateStack",),
    "organizations": ("DescribeOrganization",),
    "ce": ("GetCostAndUsage",),
}


def forbidden_cases() -> Iterator[Case]:
    """Actions that the project profile must never hold, sent with every context key that could satisfy a condition."""
    rich = {
        "aws:ResourceTag/Project": P,
        "ec2:CreateAction": "CreateVolume",
        "ecs:propagate-tags": "TASK_DEFINITION",
        "ecs:cluster": CLUSTER,
        "ec2:LaunchTemplate": "arn:aws:ec2:x",
        "iam:PassedToService": "ecs-tasks.amazonaws.com",
        "ssm:resourceTag/Project": P,
        "autoscaling:LaunchTemplateVersionSpecified": "true",
        "ec2:ResourceTag/Project": P,
    }
    resources = {
        "ec2": VOLUME,
        "autoscaling": GROUP,
        "budgets": BUDGET,
        "iam": role("ecs_host"),
        "ecs": CLUSTER,
        "ecr": REPOSITORY,
        "logs": LOG_GROUP,
        "cloudwatch": ALARM,
        "sns": BUDGET_TOPIC,
        "dlm": SCHEDULE,
        "kms": f"arn:aws:kms:{R}:{A}:key/00000000-0000-0000-0000-000000000000",
        "elasticloadbalancing": f"arn:aws:elasticloadbalancing:{R}:{A}:loadbalancer/app/{P}_x_{E}/1",
        "ssm": parameter("postgres_password"),
        "secretsmanager": arn("secretsmanager", "secret:bls_api_key-AbCdEf"),
        "sts": READER,
        "lambda": arn("lambda", f"function:{P}_x"),
        "cloudformation": arn("cloudformation", f"stack/{P}/1"),
        "organizations": "*",
        "ce": "*",
    }
    for service, actions in FORBIDDEN.items():
        for action in actions:
            if action.endswith(("_other", "_shell")):
                continue
            topics = (TOPIC, BUDGET_TOPIC) if service == "sns" and action in ("Publish", "Subscribe", "Unsubscribe") else (resources[service],)
            for resource in topics:
                yield Case(
                    f"forbidden {service}:{action} on {resource.rsplit(':', 1)[-1][:40]}", request(f"{service}:{action}", resource, tags=TAG, extra=rich), False
                )
    yield Case(
        "forbidden ssm:StartSession shell document",
        request("ssm:StartSession", f"arn:aws:ssm:{R}::document/SSM-SessionManagerRunShell", extra=rich, region=None),
        False,
    )
    for action in ("SetTopicAttributes", "DeleteTopic", "TagResource", "UntagResource", "GetTopicAttributes", "ListTagsForResource"):
        yield Case(f"the budget topic is not the project's: sns:{action}", request(f"sns:{action}", BUDGET_TOPIC, tags=TAG, extra=rich, region=None), False)
    yield Case(
        "forbidden autoscaling:SetDesiredCapacity on another group",
        request("autoscaling:SetDesiredCapacity", GROUP.replace("host_dev", "other_dev"), extra=rich, region=None),
        False,
    )
    yield Case(
        "forbidden autoscaling:SetDesiredCapacity on a group with the same prefix",
        request("autoscaling:SetDesiredCapacity", GROUP + "_extra", extra=rich, region=None),
        False,
    )
    yield Case("forbidden autoscaling:SetDesiredCapacity on every group", request("autoscaling:SetDesiredCapacity", "*", extra=rich, region=None), False)
    yield Case(
        "forbidden autoscaling:SetDesiredCapacity on the group of another account",
        request("autoscaling:SetDesiredCapacity", GROUP.replace(A, "222222222222"), extra=rich, region=None),
        False,
    )
    yield Case("project group SetDesiredCapacity is allowed", request("autoscaling:SetDesiredCapacity", GROUP, region=None), True)
    yield Case(
        "ECS tags the project group while creating the capacity provider",
        request("autoscaling:CreateOrUpdateTags", GROUP, tags=ECS_GROUP_TAG, region=None),
        True,
    )
    yield Case(
        "forbidden autoscaling:CreateOrUpdateTags with the ECS tag and another tag",
        request("autoscaling:CreateOrUpdateTags", GROUP, tags={**ECS_GROUP_TAG, "Owner": "x"}, region=None),
        False,
    )
    yield Case("forbidden autoscaling:CreateOrUpdateTags without tags", request("autoscaling:CreateOrUpdateTags", GROUP, region=None), False)
    yield Case(
        "forbidden autoscaling:CreateOrUpdateTags with the ECS tag on another group",
        request("autoscaling:CreateOrUpdateTags", GROUP.replace("host_dev", "other_dev"), tags=ECS_GROUP_TAG, region=None),
        False,
    )
    yield Case(
        "forbidden ec2:CreateVolume from a snapshot",
        request("ec2:CreateVolume", arn("ec2", "snapshot/snap-0123456789abcdef0", account=False), tags=TAG, extra=rich, region=None),
        False,
    )
    yield Case("forbidden ec2:DeleteVolume of an untagged volume", request("ec2:DeleteVolume", VOLUME, region=None), False)
    yield Case(
        "forbidden ec2:DeleteVolume of another project's volume",
        request("ec2:DeleteVolume", VOLUME, extra={"aws:ResourceTag/Project": "other_project"}, region=None),
        False,
    )
    yield Case("forbidden ec2:ReleaseAddress of an untagged address", request("ec2:ReleaseAddress", ADDRESS, region=None), False)
    yield Case(
        "forbidden ec2:ReleaseAddress of another project's address",
        request("ec2:ReleaseAddress", ADDRESS, extra={"aws:ResourceTag/Project": "other_project"}, region=None),
        False,
    )
    yield Case("forbidden dlm:UpdateLifecyclePolicy of an untagged schedule", request("dlm:UpdateLifecyclePolicy", SCHEDULE, region=None), False)
    yield Case(
        "forbidden dlm:UpdateLifecyclePolicy of another project's schedule",
        request("dlm:UpdateLifecyclePolicy", SCHEDULE, extra={"aws:ResourceTag/Project": "other_project"}, region=None),
        False,
    )
    yield Case("forbidden logs of another project", request("logs:GetLogEvents", LOG_GROUP.replace(P, "other_project"), region=None), False)
    yield Case("forbidden logs outside the environment", request("logs:DeleteLogGroup", LOG_GROUP.replace("_dev", "_prod"), region=None), False)
    yield Case("forbidden ecr token outside the region", request("ecr:GetAuthorizationToken", "*", region="us-east-1"), False)


SNAPSHOT_SOURCE = arn("ec2", "volume/vol-0123456789abcdef0")
SNAPSHOT = f"arn:aws:ec2:{R}::snapshot/snap-0123456789abcdef0"
DATA_VOLUME_NAME = f"{P}_host_data_{E}"
SCHEDULE_TAGS = {"Project": P, "SnapshotCreator": "DataLifecycleManager"}


def snapshot_cases() -> Iterator[Case]:
    """The snapshot role (Data Lifecycle Manager): a snapshot only with the exact Project key and value, tags only on create, deletes only its own."""
    volume = {"aws:ResourceTag/Name": DATA_VOLUME_NAME, "aws:ResourceTag/Project": P}

    def case(
        label: str,
        action: str,
        resource: str,
        allowed: bool,
        tags: dict[str, str] | None = None,
        extra: dict[str, Any] | None = None,
        sid: str | None = None,
        region: str | None = None,
    ) -> Case:
        return Case(f"dlm: {label}", request(action, resource, tags=tags, extra=extra, region=region), allowed, sid, "snapshots")

    yield case("snapshot of the data volume with the tag", "ec2:CreateSnapshot", SNAPSHOT_SOURCE, True, SCHEDULE_TAGS, volume)
    yield case("new snapshot with the tag", "ec2:CreateSnapshot", SNAPSHOT, True, SCHEDULE_TAGS)
    yield case("snapshot of the data volume without the tag", "ec2:CreateSnapshot", SNAPSHOT_SOURCE, False, {"SnapshotCreator": "DataLifecycleManager"}, volume)
    yield case("new snapshot without the tag", "ec2:CreateSnapshot", SNAPSHOT, False, {"SnapshotCreator": "DataLifecycleManager"})
    yield case("new snapshot with no tags", "ec2:CreateSnapshot", SNAPSHOT, False)
    yield case("new snapshot with another Project", "ec2:CreateSnapshot", SNAPSHOT, False, {"Project": "other_project"})
    yield case("snapshot of another volume", "ec2:CreateSnapshot", SNAPSHOT_SOURCE, False, SCHEDULE_TAGS, {**volume, "aws:ResourceTag/Name": "other_volume"})
    for key in ("project", "PROJECT", "Project "):
        tags = {key: P, "SnapshotCreator": "DataLifecycleManager"}
        exact_key_deny = None if key.strip() != key else "DenySnapshotsWithoutExactProjectKey"
        for action in ("ec2:CreateSnapshot", "ec2:CreateSnapshots", "ec2:CopySnapshot"):
            yield case(f"{action} with the key {key!r}", action, SNAPSHOT, False, tags, sid=exact_key_deny)
        yield case(
            f"snapshot of the data volume with the key {key!r}",
            "ec2:CreateSnapshot",
            SNAPSHOT_SOURCE,
            False,
            tags,
            volume,
            sid=exact_key_deny,
        )
    yield case("tag on create", "ec2:CreateTags", SNAPSHOT, True, SCHEDULE_TAGS, {"ec2:CreateAction": "CreateSnapshot"})
    yield case("scheduler tags only", "ec2:CreateTags", SNAPSHOT, True, {"SnapshotCreator": "DataLifecycleManager"}, {"ec2:CreateAction": "CreateSnapshot"})
    yield case("tag on create with another Project", "ec2:CreateTags", SNAPSHOT, False, {"Project": "other_project"}, {"ec2:CreateAction": "CreateSnapshot"})
    yield case(
        "tag on create with the key 'project' and another value",
        "ec2:CreateTags",
        SNAPSHOT,
        False,
        {"project": "other_project"},
        {"ec2:CreateAction": "CreateSnapshot"},
        "DenyChangingProjectTag",
    )
    yield case("retag an existing snapshot", "ec2:CreateTags", SNAPSHOT, False, TAG)
    yield case("tag a volume", "ec2:CreateTags", SNAPSHOT_SOURCE, False, TAG, {"ec2:CreateAction": "CreateSnapshot"})
    schedule = {**volume, "aws:ResourceTag/aws:dlm:lifecycle-schedule-name": "daily"}
    yield case("delete a scheduled snapshot", "ec2:DeleteSnapshot", SNAPSHOT, True, None, schedule)
    yield case("delete a snapshot the schedule did not make", "ec2:DeleteSnapshot", SNAPSHOT, False, None, volume)
    yield case(
        "delete another schedule's snapshot",
        "ec2:DeleteSnapshot",
        SNAPSHOT,
        False,
        None,
        {**volume, "aws:ResourceTag/aws:dlm:lifecycle-schedule-name": "weekly"},
    )
    rich = {**schedule, "ec2:CreateAction": "CreateSnapshot"}
    for action in ("CopySnapshot_x", "CreateImage", "DeleteTags", "EnableFastSnapshotRestores", "ModifySnapshotAttribute", "CreateVolume", "RunInstances"):
        yield case(f"no ec2:{action}", f"ec2:{action}", SNAPSHOT, False, TAG, rich)
    for action in ("DescribeInstances", "DescribeSnapshots", "DescribeVolumes"):
        yield case(f"read {action}", f"ec2:{action}", "*", True, region=R)


def execution_cases() -> Iterator[Case]:
    """The task execution role: it injects exactly nine parameters, pulls project images and writes project logs."""
    names = EXECUTION_PARAMETERS

    def case(label: str, action: str, resource: str, allowed: bool, region: str | None = None) -> Case:
        return Case(f"execution: {label}", request(action, resource, region=region), allowed, None, "execution")

    for name in names:
        yield case(f"inject {name}", "ssm:GetParameters", parameter(name), True)
    yield case("the Airflow administrator password is not injected", "ssm:GetParameters", parameter("airflow_admin"), False)
    yield case("another name in the project folder", "ssm:GetParameters", parameter("other_secret"), False)
    yield case("the same name in another project", "ssm:GetParameters", parameter("postgres_password").replace(PARAMETER_FOLDER, "other_project"), False)
    yield case("the same name in another region", "ssm:GetParameters", parameter("postgres_password").replace(R, "us-east-1"), False)
    yield case(
        "the name in the PROJECT_NAME folder, which the variable defaults do not name", "ssm:GetParameters", project_name_parameter("postgres_password"), False
    )
    yield case("a name without the project folder", "ssm:GetParameters", arn("ssm", "parameter/postgres_password"), False)
    for action in ("ssm:GetParameter", "ssm:GetParametersByPath", "ssm:PutParameter", "ssm:DescribeParameters", "secretsmanager:GetSecretValue", "kms:Decrypt"):
        yield case(f"no {action}", action, parameter("postgres_password"), False)
    yield case("registry token", "ecr:GetAuthorizationToken", "*", True, R)
    yield case("registry token in another region", "ecr:GetAuthorizationToken", "*", False, "us-east-1")
    for action in ("BatchCheckLayerAvailability", "BatchGetImage", "GetDownloadUrlForLayer"):
        yield case(f"pull with {action}", f"ecr:{action}", REPOSITORY, True)
        yield case(f"{action} in another project's repository", f"ecr:{action}", REPOSITORY.replace(P, "other_project"), False)
    yield case("no push", "ecr:PutImage", REPOSITORY, False)
    stream = arn("logs", f"log-group:{P}_jobs_{E}:log-stream:task/abc")
    yield case("write a project log", "logs:PutLogEvents", stream, True)
    yield case("create a project log stream", "logs:CreateLogStream", stream, True)
    yield case("write another project's log", "logs:PutLogEvents", stream.replace(P, "other_project"), False)
    yield case("no log group create", "logs:CreateLogGroup", LOG_GROUP, False)


def all_cases(operations: dict[str, list[Request]]) -> Iterator[Case]:
    """Every case the evaluator suite runs."""
    yield from scope_cases(operations)
    yield from scope_cases({"s3": bucket_requests()})
    yield from single_copy_cases()
    yield from tag_cases()
    yield from container_instance_cases()
    yield from session_cases()
    yield from console_cases()
    yield from parameter_cases()
    yield from pass_role_cases()
    yield from job_project_cases()
    yield from project_parameter_grid_cases()
    yield from aws_polaris_pair_cases()
    yield from forbidden_cases()
    yield from snapshot_cases()
    yield from execution_cases()


def approved_actions(operations: dict[str, list[Request]]) -> dict[str, set[str]]:
    """The approved action set per service: the actions of the approved requests plus the S3 bucket and object actions."""
    found = {service: {item.action for item in requests} for service, requests in operations.items()}
    found["s3"] = {item.action for item in bucket_requests()} | {"s3:DeleteObject"}
    return found


Mutation = Callable[[dict[str, dict[str, Any]]], None]


def statements(documents: dict[str, dict[str, Any]], service: str) -> list[dict[str, Any]]:
    return list(documents[f"healthcare_hai_prediction_{service}_policy"]["Statement"])


def by_sid(documents: dict[str, dict[str, Any]], service: str, sid: str) -> dict[str, Any]:
    return next(statement for statement in documents[f"healthcare_hai_prediction_{service}_policy"]["Statement"] if statement["Sid"] == sid)


def drop(documents: dict[str, dict[str, Any]], service: str, *sids: str) -> None:
    key = f"healthcare_hai_prediction_{service}_policy"
    documents[key]["Statement"] = [statement for statement in documents[key]["Statement"] if statement["Sid"] not in sids]


def add_action(documents: dict[str, dict[str, Any]], service: str, sid: str, *actions: str) -> None:
    by_sid(documents, service, sid)["Action"] = [*as_list(by_sid(documents, service, sid)["Action"]), *actions]


def owner_statement(documents: dict[str, dict[str, Any]], name: str, sid: str) -> dict[str, Any]:
    return next(statement for statement in documents[f"owner_{name}"]["Statement"] if statement["Sid"] == sid)


def drop_owner(documents: dict[str, dict[str, Any]], name: str, *sids: str) -> None:
    documents[f"owner_{name}"]["Statement"] = [statement for statement in documents[f"owner_{name}"]["Statement"] if statement["Sid"] not in sids]


def principals(documents: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """The policy documents each principal holds: the project user, the snapshot role and the task execution role."""
    user = [document for name, document in documents.items() if name.startswith("healthcare_hai_prediction_")]
    return {
        "user": user,
        "snapshots": [documents["owner_snapshots"]],
        "execution": [documents["owner_task_execution"]],
    }


APPROVED_OWNER_ACTIONS = {
    "snapshots": {"ec2:CreateSnapshot", "ec2:CreateTags", "ec2:DeleteSnapshot", "ec2:DescribeInstances", "ec2:DescribeSnapshots", "ec2:DescribeVolumes"},
    "task_execution": {
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "ssm:GetParameters",
    },
}


def planted_defects() -> dict[str, tuple[str, Mutation]]:
    """Named defects planted in rendered copies of the policies; each must fail at least one case."""

    def lowercase_key_ecr(documents: dict[str, dict[str, Any]]) -> None:
        drop(documents, "ecr", "DenyRepositoriesWithoutExactProjectKey")
        del by_sid(documents, "ecr", "CreateTaggedRepositories")["Condition"]["ForAnyValue:StringEquals"]

    def lowercase_key_volume(documents: dict[str, dict[str, Any]]) -> None:
        drop(documents, "ec2", "DenyCreatesWithoutExactProjectKey")
        for sid in ("CreateTaggedDataVolumes", "AllocateTaggedAddresses", "TagVolumesAndAddressesWhileCreating"):
            del by_sid(documents, "ec2", sid)["Condition"]["ForAnyValue:StringEquals"]

    def untagged_create_logs(documents: dict[str, dict[str, Any]]) -> None:
        drop(documents, "logs", "DenyLogGroupsWithoutProjectTagValue", "DenyLogGroupsWithoutExactProjectKey")
        del by_sid(documents, "logs", "CreateTaggedLogGroups")["Condition"]

    def service_without_propagation(documents: dict[str, dict[str, Any]]) -> None:
        drop(documents, "ecs", "DenyServicesAndTasksThatDoNotPropagateTags")
        del by_sid(documents, "ecs", "CreateTaggedServicesThatPropagateTags")["Condition"]["StringEquals"]["ecs:propagate-tags"]

    def remove_run_task_from_deny(documents: dict[str, dict[str, Any]], sid: str) -> None:
        statement = by_sid(documents, "ecs", sid)
        statement["Action"] = [action for action in as_list(statement["Action"]) if action != "ecs:RunTask"]

    def task_without_project_tag(documents: dict[str, dict[str, Any]]) -> None:
        for sid in ("DenyCreatesWithoutProjectTagValue", "DenyCreatesWithoutExactProjectKey"):
            remove_run_task_from_deny(documents, sid)
        condition = by_sid(documents, "ecs", "RunTaggedProjectTasks")["Condition"]
        del condition["StringEquals"]
        del condition["ForAnyValue:StringEquals"]

    def task_without_exact_project_key(documents: dict[str, dict[str, Any]]) -> None:
        remove_run_task_from_deny(documents, "DenyCreatesWithoutExactProjectKey")
        del by_sid(documents, "ecs", "RunTaggedProjectTasks")["Condition"]["ForAnyValue:StringEquals"]

    def task_requires_unsupported_propagation_key(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "RunTaggedProjectTasks")["Condition"]["StringEquals"]["ecs:propagate-tags"] = "TASK_DEFINITION"

    def task_denies_missing_unsupported_propagation_key(documents: dict[str, dict[str, Any]]) -> None:
        documents["healthcare_hai_prediction_ecs_policy"]["Statement"].append(
            {
                "Sid": "DenyMissingUnsupportedPropagation",
                "Effect": "Deny",
                "Action": "ecs:RunTask",
                "Resource": "*",
                "Condition": {"StringNotEquals": {"ecs:propagate-tags": "TASK_DEFINITION"}},
            }
        )

    def task_in_any_cluster(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "ecs", "RunTaggedProjectTasks")["Condition"]["ArnEquals"]

    def task_from_any_definition(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "RunTaggedProjectTasks")["Resource"] = "*"

    def service_recovery_refused(documents: dict[str, dict[str, Any]]) -> None:
        statement = by_sid(documents, "ecs", "ManageProjectServices")
        statement["Action"] = [action for action in statement["Action"] if action != "ecs:UpdateService"]

    def service_recovery_in_any_project(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "ManageProjectServices")["Resource"] = f"arn:aws:ecs:{R}:{A}:service/*"

    def update_turns_propagation_off(documents: dict[str, dict[str, Any]]) -> None:
        drop(documents, "ecs", "DenyUpdatesThatStopTagPropagation")

    def instance_launch(documents: dict[str, dict[str, Any]]) -> None:
        documents["healthcare_hai_prediction_ec2_policy"]["Statement"].append(
            {"Sid": "Launch", "Effect": "Allow", "Action": ["ec2:RunInstances"], "Resource": "*"}
        )

    def group_update(documents: dict[str, dict[str, Any]]) -> None:
        add_action(documents, "autoscaling", "TurnProjectHostOnAndOff", "autoscaling:UpdateAutoScalingGroup")

    def group_refresh(documents: dict[str, dict[str, Any]]) -> None:
        add_action(documents, "autoscaling", "TurnProjectHostOnAndOff", "autoscaling:StartInstanceRefresh")

    def capacity_on_any_group(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "autoscaling", "TurnProjectHostOnAndOff")["Resource"] = "*"

    def budget_access(documents: dict[str, dict[str, Any]]) -> None:
        documents["healthcare_hai_prediction_iam_policy"]["Statement"].append(
            {"Sid": "Budget", "Effect": "Allow", "Action": ["budgets:ModifyBudget"], "Resource": "*"}
        )

    def topic_wildcard(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "sns", "ManageAlertTopic")["Resource"] = f"arn:aws:sns:{R}:{A}:{P}_*"

    def tag_change_allowed(documents: dict[str, dict[str, Any]]) -> None:
        drop(documents, "logs", "DenyChangingProjectTag", "DenyRemovingProjectTag")

    def ecr_cross_project(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecr", "PushAndPullProjectImages")["Resource"] = f"arn:aws:ecr:{R}:{A}:repository/*"

    def parameter_wildcard(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ssm", "ManageProjectParameters")["Resource"] = f"arn:aws:ssm:{R}:{A}:parameter/{PARAMETER_FOLDER}/*"

    def parameter_path_read(documents: dict[str, dict[str, Any]]) -> None:
        add_action(documents, "ssm", "ManageProjectParameters", "ssm:GetParametersByPath")

    def admin_password_write(documents: dict[str, dict[str, Any]]) -> None:
        add_action(documents, "ssm", "ReadAirflowAdminParameter", "ssm:PutParameter")

    def roles_wildcard(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "sts", "AssumeLakehouseRolesOnly")["Resource"] = f"arn:aws:iam::{A}:role/*"

    def athena_roles_wildcard(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "sts", "AssumeAthenaReaderOnly")["Resource"] = f"arn:aws:iam::{A}:role/*"

    def athena_role_prefix(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "sts", "AssumeAthenaReaderOnly")["Resource"] = f"arn:aws:iam::{A}:role/{P}_athena_*"

    def athena_any_environment(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "sts", "AssumeAthenaReaderOnly")["Resource"] = f"arn:aws:iam::{A}:role/{P}_athena_reader_*"

    def athena_direct_access(documents: dict[str, dict[str, Any]]) -> None:
        documents["healthcare_hai_prediction_sts_policy"]["Statement"].append(
            {"Sid": "PlantedDirectAthena", "Effect": "Allow", "Action": ["athena:StartQueryExecution", "glue:GetTable"], "Resource": "*"}
        )

    def percent_deny_missing(documents: dict[str, dict[str, Any]]) -> None:
        drop(documents, "s3", "DenyPercentSignInSingleCopyKeys")

    def older_grants_deny_missing(documents: dict[str, dict[str, Any]]) -> None:
        drop(documents, "s3", "DenyOlderGrantsInSingleCopyFolders")

    def create_once_unconditional(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "s3", "CreateSingleCopyContentObjectsOnce")["Condition"]

    def tags_after_create(documents: dict[str, dict[str, Any]]) -> None:
        drop(documents, "ec2", "DenyTaggingAfterCreate")
        del by_sid(documents, "ec2", "TagVolumesAndAddressesWhileCreating")["Condition"]["StringEquals"]["ec2:CreateAction"]

    def pass_role_any_service(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "iam", "PassTaskRolesToEcsTasks")["Condition"]

    def pass_host_role(documents: dict[str, dict[str, Any]]) -> None:
        condition = {"StringEquals": {"iam:PassedToService": "ec2.amazonaws.com"}}
        documents["healthcare_hai_prediction_iam_policy"]["Statement"].append(
            {"Sid": "Host", "Effect": "Allow", "Action": ["iam:PassRole"], "Resource": role("ecs_host"), "Condition": condition}
        )

    def session_to_any_instance(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "ssm", "PortForwardToProjectHost")["Condition"]["StringEquals"]["ssm:resourceTag/Name"]

    def console_of_any_named_instance(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "ec2", "ReadProjectHostConsole")["Condition"]["StringEquals"]["aws:ResourceTag/Name"]

    def console_of_any_project(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "ec2", "ReadProjectHostConsole")["Condition"]["StringEquals"]["aws:ResourceTag/Project"]

    def console_of_every_instance(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ec2", "ReadProjectHostConsole")["Resource"] = "*"

    def console_screenshot(documents: dict[str, dict[str, Any]]) -> None:
        add_action(documents, "ec2", "ReadProjectHostConsole", "ec2:GetConsoleScreenshot")

    def session_of_any_user(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ssm", "EndOwnSessions")["Resource"] = f"arn:aws:ssm:{R}:{A}:session/*"

    def deregister_task_definitions(documents: dict[str, dict[str, Any]]) -> None:
        add_action(documents, "ecs", "ManageProjectServices", "ecs:DeregisterTaskDefinition")
        by_sid(documents, "ecs", "ManageProjectServices")["Resource"] = "*"

    def ecs_other_project(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "ManageProjectCluster")["Resource"] = f"arn:aws:ecs:{R}:{A}:cluster/*"

    def secrets_create(documents: dict[str, dict[str, Any]]) -> None:
        add_action(documents, "secretsmanager", "ReadSharedApiKeys", "secretsmanager:CreateSecret")

    def region_condition_dropped(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "ecr", "GetRegistryLoginToken")["Condition"]

    def snapshot_lowercase_key(documents: dict[str, dict[str, Any]]) -> None:
        drop_owner(documents, "snapshots", "DenySnapshotsWithoutExactProjectKey")
        del owner_statement(documents, "snapshots", "CreateTaggedSnapshots")["Condition"]["ForAnyValue:StringEquals"]

    def snapshot_delete_any(documents: dict[str, dict[str, Any]]) -> None:
        del owner_statement(documents, "snapshots", "DeleteScheduledSnapshots")["Condition"]["StringEquals"]["aws:ResourceTag/aws:dlm:lifecycle-schedule-name"]

    def execution_wildcard(documents: dict[str, dict[str, Any]]) -> None:
        owner_statement(documents, "task_execution", "InjectNamedParameters")["Resource"] = f"arn:aws:ssm:{R}:{A}:parameter/{PARAMETER_FOLDER}/*"

    def container_instance_on_cluster(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "DescribeProjectContainerInstances")["Resource"] = f"arn:aws:ecs:{R}:{A}:cluster/{P}_cluster_{E}"

    def container_instance_in_any_cluster(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "DescribeProjectContainerInstances")["Resource"] = f"arn:aws:ecs:{R}:{A}:container-instance/*"

    def another_project_tag_on_run_task_only(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "DenyAnotherProjectTagOnRunTaskServicesAndTags")["Action"] = ["ecs:CreateService", "ecs:TagResource"]
        remove_run_task_from_deny(documents, "DenyCreatesWithoutProjectTagValue")
        del by_sid(documents, "ecs", "RunTaggedProjectTasks")["Condition"]["StringEquals"]

    def another_project_tag_allowed(documents: dict[str, dict[str, Any]]) -> None:
        another_project_tag_on_run_task_only(documents)
        drop(documents, "ecs", "DenyAnotherProjectTagOnRunTaskServicesAndTags")

    def another_project_tag_on_tagging_only(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "DenyAnotherProjectTagOnRunTaskServicesAndTags")["Action"] = ["ecs:RunTask", "ecs:CreateService"]

    def own_project_tag_refused(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "DenyAnotherProjectTagOnRunTaskServicesAndTags")["Condition"] = {"Null": {"aws:RequestTag/Project": "false"}}

    def job_resource(service: str, sid: str, index: int, resource: str) -> Mutation:
        def mutate(documents: dict[str, dict[str, Any]]) -> None:
            statement = by_sid(documents, service, sid)
            resources = list(as_list(statement["Resource"]))
            resources[index] = resource
            statement["Resource"] = resources

        return mutate

    def job_pass_without_service_condition(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "iam", "PassReadJobRoles")["Condition"]

    def broker_pass_without_service_condition(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "iam", "PassBrokerRole")["Condition"]

    def training_pass_without_service_condition(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "iam", "PassTrainingRoles")["Condition"]

    def job_pass_to_ecs_service(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "iam", "PassReadJobRoles")["Condition"]["StringEquals"]["iam:PassedToService"] = "ecs.amazonaws.com"

    def job_assume_without_session_condition(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "sts", "AssumeTryOutputRole")["Condition"]

    def job_reader_batch_allowed(documents: dict[str, dict[str, Any]]) -> None:
        add_action(documents, "ssm", "SetUpAwsPolarisPairs", "ssm:GetParameters")

    def aws_pair_action(sid: str, action: str) -> Mutation:
        def mutate(documents: dict[str, dict[str, Any]]) -> None:
            add_action(documents, "ssm", sid, action)

        return mutate

    def aws_pair_statement_dropped(documents: dict[str, dict[str, Any]]) -> None:
        drop(documents, "ssm", "SetUpAwsPolarisPairs")

    def aws_pair_listing_granted(documents: dict[str, dict[str, Any]]) -> None:
        documents["healthcare_hai_prediction_ssm_policy"]["Statement"].append(
            {"Sid": "ListParameters", "Effect": "Allow", "Action": "ssm:DescribeParameters", "Resource": "*"}
        )

    aws_pair_defects: dict[str, tuple[str, Mutation]] = {
        f"aws pair resource {name} widened": (
            f"aws pair: refuse PutParameter {name}_extra",
            job_resource("ssm", "SetUpAwsPolarisPairs", index, parameter(name) + "*"),
        )
        for index, name in enumerate(AWS_POLARIS_PARAMETERS)
        if name in AWS_WRITER_PARAMETERS
    }
    aws_pair_defects.update(
        {
            "aws pair write statement dropped": ("aws pair: PutParameter aws_polaris_gold_client_secret", aws_pair_statement_dropped),
            "aws pair delete action added": (
                "aws pair: refuse DeleteParameter aws_polaris_reader_client_id",
                aws_pair_action("SetUpAwsPolarisPairs", "ssm:DeleteParameter"),
            ),
            "aws pair batch read added": (
                "aws pair: refuse GetParameters aws_polaris_e2e_client_id",
                aws_pair_action("SetUpAwsPolarisPairs", "ssm:GetParameters"),
            ),
            "aws pair history read added": (
                "aws pair: refuse GetParameterHistory aws_polaris_bronze_client_secret",
                aws_pair_action("SetUpAwsPolarisPairs", "ssm:GetParameterHistory"),
            ),
            "aws pair tagging added": (
                "aws pair: refuse AddTagsToResource aws_polaris_silver_client_id",
                aws_pair_action("SetUpAwsPolarisPairs", "ssm:AddTagsToResource"),
            ),
            "account-wide parameter listing granted": ("aws pair: refuse the account-wide parameter listing", aws_pair_listing_granted),
        }
    )
    job_defects: dict[str, tuple[str, Mutation]] = {
        f"job PassRole resource {name} widened": (
            "job pass: refuse job_try_writer",
            job_resource("iam", "PassReadJobRoles", index, f"arn:aws:iam::{A}:role/{P}_job_*_{E}"),
        )
        for index, name in enumerate(JOB_TASK_ROLES)
    }
    job_defects.update(
        {
            f"job reader resource {name} widened": (
                f"job reader: refuse {name}_extra",
                job_resource("ssm", "SetUpAwsPolarisPairs", index, parameter(name) + "*"),
            )
            for index, name in enumerate(AWS_READER_PARAMETERS)
        }
    )
    job_defects.update(
        {
            "job try-writer resource widened": (
                "job assume: refuse job_execution",
                job_resource("sts", "AssumeTryOutputRole", 0, f"arn:aws:iam::{A}:role/{P}_job_*_{E}"),
            ),
            "job stage version resource widened": ("job stage: refuse lakehouse/silver", job_resource("s3", "ReadStageVersions", 0, f"{BUCKET}/lakehouse/*")),
            "training PassRole resource widened": (
                "training pass: refuse job_train_task_extra",
                job_resource("iam", "PassTrainingRoles", 0, role("job_train_task*")),
            ),
            "broker PassRole resource widened": ("broker pass: refuse broker_task_extra", job_resource("iam", "PassBrokerRole", 0, role("broker_task*"))),
            "broker PassRole service condition removed": ("broker pass: broker_task without a service", broker_pass_without_service_condition),
            "training PassRole service condition removed": ("training pass: job_train_task without a service", training_pass_without_service_condition),
            "layer PassRole writer-family wildcard": (
                "job pass: refuse job_write_other_task",
                job_resource("iam", "PassLayerWriterRoles", 0, role("job_write_*_task")),
            ),
            "job PassRole service condition removed": ("job pass: job_execution without a service", job_pass_without_service_condition),
            "job PassRole service changed to ECS": ("job pass: job_execution to ecs.amazonaws.com", job_pass_to_ecs_service),
            "job try-writer session condition removed": ("job assume: session 'unrelated'", job_assume_without_session_condition),
            "job reader batch action added": ("job reader: refuse GetParameters ", job_reader_batch_allowed),
        }
    )
    return {
        **job_defects,
        **aws_pair_defects,
        "DescribeContainerInstances is granted on the cluster ARN instead of the container-instance ARN (review round 5 finding 6)": (
            "ecs:DescribeContainerInstances on container-instance",
            container_instance_on_cluster,
        ),
        "DescribeContainerInstances reaches the container instances of every cluster": (
            "ecs:DescribeContainerInstances in another project",
            container_instance_in_any_cluster,
        ),
        "a task, service or tag request may set another project's Project tag (review round 5 finding 2)": (
            "ecs:RunTask sets Project to another project",
            another_project_tag_allowed,
        ),
        "RunTask may set another project's Project tag": ("ecs:RunTask sets Project to another project", another_project_tag_on_run_task_only),
        "ecs:TagResource may set another project's Project tag while creating": (
            "ecs:TagResource sets Project to another project while CreateService",
            another_project_tag_on_tagging_only,
        ),
        "the Project tag of a run task is refused even with the project's value": ("ecs:RunTask with the project's own Project tag", own_project_tag_refused),
        "the snapshot role accepts a lowercase project key (review round 4 finding 1)": (
            "dlm: ec2:CreateSnapshot with the key 'project'",
            snapshot_lowercase_key,
        ),
        "the snapshot role deletes snapshots its schedule did not make": ("dlm: delete a snapshot the schedule did not make", snapshot_delete_any),
        "the execution role reads every parameter in the project folder": ("execution: another name in the project folder", execution_wildcard),
        "ecr create accepts a lowercase project key (review round 4 finding 1)": ("create ecr:CreateRepository: lowercase key", lowercase_key_ecr),
        "ec2 volume and address creates accept a lowercase project key": ("create ec2:CreateVolume: lowercase key", lowercase_key_volume),
        "log group create without the Project tag": ("create logs:CreateLogGroup: no tags", untagged_create_logs),
        "service created without tag propagation (finding 3)": ("ecs:CreateService propagates 'NONE'", service_without_propagation),
        "run task without an explicit Project tag": ("ecs:RunTask without request tags, propagation='NONE'", task_without_project_tag),
        "run task accepts a lowercase Project key": ("create ecs:RunTask: lowercase key", task_without_exact_project_key),
        "run task requires the unsupported propagation condition key": (
            "ecs:RunTask with explicit Project, propagation='TASK_DEFINITION'",
            task_requires_unsupported_propagation_key,
        ),
        "run task denied because the unsupported propagation condition key is absent": (
            "ecs:RunTask with explicit Project, propagation='TASK_DEFINITION'",
            task_denies_missing_unsupported_propagation_key,
        ),
        "run task in a different cluster": ("ecs:RunTask in another cluster", task_in_any_cluster),
        "run task from another project's definition": ("ecs:RunTask with another project's task definition", task_from_any_definition),
        "forced service recovery loses UpdateService": ("ecs:UpdateService force deployment on project service", service_recovery_refused),
        "forced service recovery reaches other projects": ("ecs:UpdateService force deployment on another project's service", service_recovery_in_any_project),
        "service update turns propagation off (finding 3)": ("ecs:UpdateService turns propagation off", update_turns_propagation_off),
        "the project user can launch instances (finding 2)": ("forbidden ec2:RunInstances", instance_launch),
        "the project user can update the host group or its template version (finding 4)": ("forbidden autoscaling:UpdateAutoScalingGroup", group_update),
        "the project user can refresh the host group (finding 4)": ("forbidden autoscaling:StartInstanceRefresh", group_refresh),
        "SetDesiredCapacity on any group": ("forbidden autoscaling:SetDesiredCapacity on another group", capacity_on_any_group),
        "the project user can modify the budget": ("forbidden budgets:ModifyBudget", budget_access),
        "the alert topic pattern reaches the budget topic": ("the budget topic is not the project's: sns:SetTopicAttributes", topic_wildcard),
        "the Project tag can be changed or removed later": ("logs:TagResource changes Project", tag_change_allowed),
        "image push and pull on every repository": ("ecr:PutImage in another project", ecr_cross_project),
        "parameter read by wildcard": ("parameter: read another name in the project folder", parameter_wildcard),
        "parameter path read": ("forbidden ssm:GetParametersByPath", parameter_path_read),
        "write access to the read-only administrator password": ("parameter: overwrite the Airflow administrator password", admin_password_write),
        "sts AssumeRole on every role": ("AssumeRole administrator", roles_wildcard),
        "the Athena statement reaches every role": ("AssumeRole administrator", athena_roles_wildcard),
        "the Athena statement reaches every Athena role": ("AssumeRole another athena role", athena_role_prefix),
        "the Athena statement reaches the reader of every environment": ("AssumeRole athena_reader of another environment", athena_any_environment),
        "the Athena statement carries direct Athena and Glue actions": ("the project user has no direct athena:StartQueryExecution", athena_direct_access),
        "the percent-sign deny is missing": ("single copy: PutObject leaf", percent_deny_missing),
        "the older-grants deny is missing": ("single copy: PutObject bronze_objects", older_grants_deny_missing),
        "the single-copy create loses its header condition": ("single copy: create without the header", create_once_unconditional),
        "tags written to an existing volume": ("ec2:CreateTags later on a volume", tags_after_create),
        "PassRole to any service": ("PassRole task_execution to ec2.amazonaws.com", pass_role_any_service),
        "PassRole of the host role": ("PassRole ecs_host is not the user's", pass_host_role),
        "port forwarding to any tagged instance": ("session: another host name", session_to_any_instance),
        "sessions of another user": ("session: another user's session", session_of_any_user),
        "console output of any instance with the project tag": ("console: another host name", console_of_any_named_instance),
        "console output of any project's host": ("console: instance of another project", console_of_any_project),
        "console output of every instance": ("console: every instance", console_of_every_instance),
        "console screenshot beside the console output": ("console: no ec2:GetConsoleScreenshot on the project host", console_screenshot),
        "task definitions can be deregistered": ("forbidden ecs:DeregisterTaskDefinition", deregister_task_definitions),
        "cluster actions on every cluster": ("ecs:DeleteCluster in another project", ecs_other_project),
        "secret creation": ("secret: no CreateSecret", secrets_create),
        "registry token in every region": ("forbidden ecr token outside the region", region_condition_dropped),
    }
