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

from scripts.infrastructure.policy_evaluator import POLICY_VARIABLE, Request, as_list, build_request

R, A, P, E, B = "eu-west-1", "111111111111", "example_project", "dev", "example-project-ci-bucket"
VALUES = {"AWS_REGION": R, "AWS_ACCOUNT_ID": A, "PROJECT_NAME": P, "ENVIRONMENT": E, "DATA_BUCKET_NAME": B}
TAG = {"Project": P}
# The policy files of the project user, by the service part of their name. ssmcap holds the services apply command's grants on the launch hold and apply lock.
SERVICES = ("autoscaling", "cloudwatch", "dlm", "ec2", "ecr", "ecs", "iam", "logs", "s3", "secretsmanager", "sns", "ssm", "ssmcap", "sts")
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
# A subscription ARN is its topic ARN, a colon and an identifier. IAM matches GetSubscriptionAttributes and Unsubscribe on that ARN, so a pattern that ends
# in ":*" after the topic name reaches the subscriptions of that one topic.
SUBSCRIPTION_ID = "01234567-89ab-cdef-0123-456789abcdef"
SUBSCRIPTION = f"{TOPIC}:{SUBSCRIPTION_ID}"
BUDGET_SUBSCRIPTION = f"{BUDGET_TOPIC}:{SUBSCRIPTION_ID}"
# Alert permissions (owner, Nov 11 2026): the project user has full permissions on any alert topic the project's Terraform creates, so the policy gives sns:*
# on the exact ARN of each alert topic and of its subscriptions (the platform alerts topic and the IAM stack's budget alerts topic). The platform and IAM
# Terraform tests assert that each stack's own topic ARN is in the rendered policy. A topic that is not named gets nothing, whatever its name looks like.
ALERT_STATEMENT = "ManageAlertTopics"
ALERT_WILDCARD_ACTION = "sns:*"
ALERT_TOPIC_ACTIONS = (
    "Publish",
    "Subscribe",
    "ConfirmSubscription",
    "AddPermission",
    "RemovePermission",
    "GetTopicAttributes",
    "SetTopicAttributes",
    "DeleteTopic",
    "ListSubscriptionsByTopic",
    "ListTagsForResource",
    "TagResource",
    "UntagResource",
    "GetDataProtectionPolicy",
    "PutDataProtectionPolicy",
)
ALERT_SUBSCRIPTION_ACTIONS = ("GetSubscriptionAttributes", "SetSubscriptionAttributes", "Unsubscribe")
ALERT_TOPICS = {"the alerts topic": TOPIC, "the budget alerts topic": BUDGET_TOPIC}
ALARM = arn("cloudwatch", f"alarm:{P}_run_failed_{E}")
SCHEDULE = arn("dlm", "policy/policy-0123456789abcdef0")
VOLUME = arn("ec2", "volume/vol-0123456789abcdef0")
ADDRESS = arn("ec2", "elastic-ip/eipalloc-0123456789abcdef0")
INSTANCE = arn("ec2", "instance/i-0123456789abcdef0")
HOST_TAGS = {"aws:ResourceTag/Project": P, "aws:ResourceTag/Name": f"{P}_host_{E}"}
DATA_VOLUME_TAGS = {"aws:ResourceTag/Project": P, "aws:ResourceTag/Name": f"{P}_host_data_{E}"}
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
LOGS_BUCKET = f"arn:aws:s3:::{B}-logs"
LOGS_WITHOUT = ("ListBucketVersions", "ListBucketMultipartUploads")
LOGS_WRITES = (
    "PutLifecycleConfiguration",
    "PutEncryptionConfiguration",
    "PutBucketPublicAccessBlock",
    "PutBucketOwnershipControls",
    "PutBucketPolicy",
    "PutBucketTagging",
)
PARAMETERS = (
    "airflow_api_secret_key",
    "airflow_db_password",
    "airflow_fernet_key",
    "airflow_jwt_secret",
    "grafana_admin_password",
    "mlflow_admin_password",
    "mlflow_db_password",
    "model_api_key_hashes",
    "model_api_mlflow_credential",
    "model_api_mlflow_user",
    "model_api_redis_password",
    "polaris_gold_client_id",
    "polaris_gold_client_secret",
    "polaris_reader_client_id",
    "polaris_reader_client_secret",
    "polaris_root_secret",
    "polaris_silver_client_id",
    "polaris_silver_client_secret",
    "postgres_password",
    "redis_password",
    "streamlit_model_api_key",
)
READ_ONLY_PARAMETERS = ("airflow_admin",)
# The old hand-made Secrets Manager copy of the Airflow administrator password; no policy grants it since the one-time copy was done.
LEGACY_AIRFLOW_ADMIN = "healthcare-hai-prediction/airflow-admin"
# The eight control-plane parameters the task execution role injects; each is one of PARAMETERS.
EXECUTION_PARAMETERS = (
    "postgres_password",
    "airflow_db_password",
    "airflow_fernet_key",
    "airflow_jwt_secret",
    "airflow_api_secret_key",
    "redis_password",
    "polaris_root_secret",
    "grafana_admin_password",
)
SHARED_KEYS = ("bls_api_key", "census_api_key", "hud_api_key", "jev_api_key")
AWS_READER_PARAMETERS = ("aws_polaris_reader_client_id", "aws_polaris_reader_client_secret")
# The AWS catalog setup's writer pairs: the project user writes all fourteen AWS Polaris names and reads these twelve besides the reader pair.
AWS_WRITER_PARAMETERS = tuple(
    f"aws_polaris_{identity}_client_{part}" for identity in ("e2e", "bronze", "silver", "gold", "features", "serving") for part in ("id", "secret")
)
AWS_POLARIS_PARAMETERS = (*AWS_READER_PARAMETERS, *AWS_WRITER_PARAMETERS)
# The catalog setup creates these two passwords as the project user. PutParameter carries the condition key ssm:Overwrite (a string), so an explicit
# Deny on Overwrite = true keeps the two names create-only. The grant is limited to the two names.
OPENMETADATA_PASSWORDS = ("openmetadata_admin_password", "openmetadata_db_password")
# The services stack no longer manages a Parameter Store parameter (Nov 11 2026: the host job cap is an Airflow container setting), so the project user holds
# no grant on the parameter that carried the cap and no metadata listing; the cases below pin both refusals.
RETIRED_CAP_PARAMETER = "host_job_cap"
# Launch hold during services applies (owner A, Nov 10 2026): the services apply command puts and reads the hold; it never deletes or tags it.
HOST_LAUNCH_HOLD = "host_launch_hold"
HOST_LAUNCH_HOLD_STATEMENT = "SetHostLaunchHold"
HOST_LAUNCH_HOLD_ACTIONS = ("PutParameter", "GetParameter")
# Apply lock (owner A, Nov 10 2026): the services apply command creates the lock without overwrite, reads its holder and deletes its own lock.
SERVICES_APPLY_LOCK = "services_apply_lock"
SERVICES_APPLY_LOCK_STATEMENT = "ManageServicesApplyLock"
SERVICES_APPLY_LOCK_ACTIONS = ("PutParameter", "GetParameter", "DeleteParameter")
OPENMETADATA_STATEMENT = "SetUpOpenMetadataPasswords"
OPENMETADATA_DENY = "DenyOpenMetadataPasswordOverwrite"
OPENMETADATA_NEIGHBORS = (
    "openmetadata_bot_lineage",
    "openmetadata_bot_ingestion",
    "openmetadata_bot_comparison",
    "openmetadata_admin",
    "openmetadata_password",
)
JOB_TASK_ROLES = ("job_execution",)


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
        add("cloudwatch", action, ALARM, region=None, tags={"DataClassification": "Public"} if action.endswith("agResource") else None)
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
    add("ec2", "CreateTags", VOLUME, region=None, tags={"DataClassification": "Public"}, extra=DATA_VOLUME_TAGS)
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
        add("ecr", action, REPOSITORY, region=None, tags={"DataClassification": "Public"}, extra={"aws:ResourceTag/Project": P})
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
    for simulated in (role("job_execution"), READER, WRITER, f"arn:aws:iam::{A}:user/{USER}"):
        add("iam", "SimulatePrincipalPolicy", simulated, region=None, extra={"aws:username": USER})
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
        add("logs", action, LOG_GROUP, region=None, tags={"DataClassification": "Public"} if action.endswith("agResource") else None)
    add("logs", "TagResource", LOG_GROUP, region=None, tags={"DataClassification": "Public"})
    add("logs", "DescribeLogGroups", "*")
    add("secretsmanager", "GetSecretValue", arn("secretsmanager", "secret:bls_api_key-AbCdEf"), region=None)
    add("secretsmanager", "DescribeSecret", arn("secretsmanager", "secret:jev_api_key-AbCdEf"), region=None)
    add("sns", "CreateTopic", TOPIC, region=None, **tagged)
    for topic in (TOPIC, BUDGET_TOPIC):
        for action in ALERT_TOPIC_ACTIONS:
            add("sns", action, topic, region=None, tags={"DataClassification": "x"} if action.endswith("agResource") else None)
        for action in ALERT_SUBSCRIPTION_ACTIONS:
            add("sns", action, f"{topic}:{SUBSCRIPTION_ID}", region=None)
    for name in (*PARAMETERS, *READ_ONLY_PARAMETERS):
        add("ssm", "GetParameter", parameter(name), region=None)
    for name in PARAMETERS:
        add("ssm", "PutParameter", parameter(name), region=None)
    for name in AWS_POLARIS_PARAMETERS:
        add("ssm", "GetParameter", parameter(name), region=None)
        add("ssm", "PutParameter", parameter(name), region=None)
    add("ssm", "GetParameters", PUBLIC_PARAMETER, region=None)
    for action in HOST_LAUNCH_HOLD_ACTIONS:
        ops["ssmcap"].append(request(f"ssm:{action}", parameter(HOST_LAUNCH_HOLD), region=None))
    for action in SERVICES_APPLY_LOCK_ACTIONS:
        ops["ssmcap"].append(request(f"ssm:{action}", parameter(SERVICES_APPLY_LOCK), region=None))
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
    # The logging bucket of the access logs: the configuration reads, the six settings the storage stack applies and its creation. Nothing else.
    found += [
        request(f"s3:{action}", LOGS_BUCKET, region=None) for action in bucket_actions if action.startswith(("Get", "List")) and action not in LOGS_WITHOUT
    ]
    found += [request(f"s3:{action}", LOGS_BUCKET, region=None) for action in LOGS_WRITES]
    found.append(request("s3:CreateBucket", LOGS_BUCKET, region=None, extra={"s3:LocationConstraint": R}))
    found.append(request("s3:PutBucketLogging", BUCKET, region=None))
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


PROMOTE_RUN = "20261024T000000Z-1234abcd"


def promote_cases() -> Iterator[Case]:
    """The owner's access to the promotion records: read and create once under promote/, and nothing else.

    The listing comes from the whole-bucket ``s3:ListBucket`` of the bucket statements, so these cases cover the object actions only.
    """
    for name in (f"promote/{PROMOTE_RUN}/attempt.json", f"promote/{PROMOTE_RUN}/abandoned.json", "promote/models/hai_model_HAI_1/lease-0000000001.json"):
        yield s3_case(f"promote records: GetObject {name}", "GetObject", name, False, True)
        yield s3_case(f"promote records: create {name} once", "PutObject", name, True, True)
        yield s3_case(f"promote records: overwrite {name} without the header refused", "PutObject", name, False, False)
        yield Case(
            f"promote records: PutObject {name} with another if-none-match value refused",
            request("s3:PutObject", f"{BUCKET}/{name}", extra={"s3:if-none-match": "abc"}, region=None),
            False,
        )
        for action in ("DeleteObject", "DeleteObjectVersion", "GetObjectVersion", "PutObjectTagging", "PutObjectAcl", "AbortMultipartUpload"):
            for header in (True, False):
                yield s3_case(f"promote records: {action} {name} header={header} refused", action, name, header, False)
    for name in ("promoted/x.json", "promote", "other/promote/x.json", "unlisted/x.json"):
        yield s3_case(f"promote records: GetObject outside promote/ {name} refused", "GetObject", name, False, False)
        for header in (True, False):
            yield s3_case(f"promote records: PutObject outside promote/ {name} header={header} refused", "PutObject", name, header, False)


TRAIN_TASK = "job_train_task"
LAUNCH_TASK = "airflow_launch_task"
UNBOUNDED_ROLES = (TRAIN_TASK, LAUNCH_TASK)
RESEARCH_RUN = "20261109T000000Z-ea260ed0"
RESEARCH_KEYS = tuple(f"research/{RESEARCH_RUN}/{kind}_HAI_1.json" for kind in ("research", "replay", "canary", "notice"))
RESEARCH_OTHER_KEYS = ("researched/x.json", "research", "other/research/x.json", "promote/x.json", "lakehouse/gold/x.parquet", "unlisted/x.json")
RESEARCH_REFUSED_ACTIONS = ("DeleteObject", "DeleteObjectVersion", "GetObjectVersion", "PutObjectTagging", "PutObjectAcl", "AbortMultipartUpload")
NOTICE_REFUSED_ACTIONS = (
    "sns:Subscribe",
    "sns:Unsubscribe",
    "sns:SetTopicAttributes",
    "sns:DeleteTopic",
    "sns:CreateTopic",
    "sns:AddPermission",
    "sns:RemovePermission",
    "sns:GetTopicAttributes",
    "sns:ListSubscriptionsByTopic",
    "sns:TagResource",
    "sns:*",
)


def research_case(principal: str, label: str, action: str, resource: str, allowed: bool, sid: str | None = None, extra: dict[str, Any] | None = None) -> Case:
    return Case(label, request(action, resource, extra=extra, region=None), allowed, sid, principal)


def research_cases() -> Iterator[Case]:
    """The research records of the promote task role and the launch role, and the owner notice of the promote task role.

    The promote task role creates each record once, reads it and lists the prefix, under ``research/`` alone. The launch role reads the records only. The task
    role publishes to the alerts topic and nothing else of SNS. Every refusal here is a request the grant must not allow: another prefix, another bucket,
    another topic (the budget topic too), another account, region, project or environment, and any other action.
    """
    once = {"s3:if-none-match": "*"}
    for key in RESEARCH_KEYS:
        resource = f"{BUCKET}/{key}"
        yield research_case(TRAIN_TASK, f"research records: GetObject {key}", "s3:GetObject", resource, True, "ReadResearchRecords")
        yield research_case(TRAIN_TASK, f"research records: create {key} once", "s3:PutObject", resource, True, "CreateResearchRecords", once)
        yield research_case(TRAIN_TASK, f"research records: overwrite {key} without the header refused", "s3:PutObject", resource, False)
        yield research_case(
            TRAIN_TASK,
            f"research records: PutObject {key} with another if-none-match value refused",
            "s3:PutObject",
            resource,
            False,
            extra={"s3:if-none-match": "abc"},
        )
        yield research_case(
            TRAIN_TASK, f"research records: {key} in another bucket refused", "s3:GetObject", resource.replace(BUCKET, f"{BUCKET}-other"), False
        )
        for action in RESEARCH_REFUSED_ACTIONS:
            for header in (True, False):
                yield research_case(
                    TRAIN_TASK, f"research records: {action} {key} header={header} refused", f"s3:{action}", resource, False, extra=once if header else None
                )
        yield research_case(LAUNCH_TASK, f"launch research read: GetObject {key}", "s3:GetObject", resource, True, "ReadResearchRecords")
        yield research_case(
            LAUNCH_TASK, f"launch research read: {key} in another bucket refused", "s3:GetObject", resource.replace(BUCKET, f"{BUCKET}-other"), False
        )
        for header in (True, False):
            yield research_case(
                LAUNCH_TASK, f"launch research read: PutObject {key} header={header} refused", "s3:PutObject", resource, False, extra=once if header else None
            )
        for action in RESEARCH_REFUSED_ACTIONS:
            yield research_case(LAUNCH_TASK, f"launch research read: {action} {key} refused", f"s3:{action}", resource, False)
    for key in RESEARCH_OTHER_KEYS:
        resource = f"{BUCKET}/{key}"
        yield research_case(TRAIN_TASK, f"research records: GetObject outside research/ {key} refused", "s3:GetObject", resource, False)
        yield research_case(LAUNCH_TASK, f"launch research read: GetObject outside research/ {key} refused", "s3:GetObject", resource, False)
        for header in (True, False):
            yield research_case(
                TRAIN_TASK,
                f"research records: PutObject outside research/ {key} header={header} refused",
                "s3:PutObject",
                resource,
                False,
                extra=once if header else None,
            )
    for prefix in ("research/", f"research/{RESEARCH_RUN}/"):
        listing = {"s3:prefix": (prefix,)}
        yield research_case(TRAIN_TASK, f"research records: list under {prefix}", "s3:ListBucket", BUCKET, True, "ListResearchRecords", listing)
        yield research_case(LAUNCH_TASK, f"launch research read: list under {prefix} refused", "s3:ListBucket", BUCKET, False, extra=listing)
    for prefix in ("", "promote/", "researched/", "research", "lakehouse/", "other/research/"):
        yield research_case(
            TRAIN_TASK,
            f"research records: list outside research/ with prefix {prefix!r} refused",
            "s3:ListBucket",
            BUCKET,
            False,
            extra={"s3:prefix": (prefix,)},
        )
    yield research_case(TRAIN_TASK, "research records: list without a prefix refused", "s3:ListBucket", BUCKET, False)
    yield research_case(
        TRAIN_TASK, "research records: list in another bucket refused", "s3:ListBucket", f"{BUCKET}-other", False, extra={"s3:prefix": ("research/",)}
    )
    yield research_case(LAUNCH_TASK, "launch research read: list without a prefix refused", "s3:ListBucket", BUCKET, False)

    def notice(label: str, action: str, topic: str, allowed: bool, region: str = R) -> Case:
        return Case(f"owner notice: {label}", request(action, topic, region=region), allowed, "PublishOwnerNotices" if allowed else None, TRAIN_TASK)

    yield notice("Publish to the alerts topic", "sns:Publish", TOPIC, True)
    yield notice("Publish to the budget topic refused", "sns:Publish", BUDGET_TOPIC, False)
    yield notice("Publish to a topic that only starts like the alerts topic refused", "sns:Publish", f"{TOPIC}_other", False)
    yield notice("Publish to a topic that only ends like the alerts topic refused", "sns:Publish", TOPIC.replace(f"{P}_alerts", f"other_{P}_alerts"), False)
    yield notice("Publish to every topic refused", "sns:Publish", arn("sns", "*"), False)
    for label, topic, region in widen(TOPIC):
        yield notice(f"Publish to the alerts topic of another {label} refused", "sns:Publish", topic, False, region or R)
    for action in NOTICE_REFUSED_ACTIONS:
        yield notice(f"{action} on the alerts topic refused", action, TOPIC, False)
    yield Case("owner notice: the launch role does not publish", request("sns:Publish", TOPIC), False, None, LAUNCH_TASK)


def logging_bucket_cases() -> Iterator[Case]:
    """The logging bucket may be created and configured, and the project bucket may send its access logs there. Nothing else follows."""
    other = f"{BUCKET}-other"
    for action in ("PutBucketLogging", "PutBucketVersioning", "PutBucketAcl", "DeleteBucket", "PutReplicationConfiguration"):
        yield Case(f"logging bucket: {action} refused", request(f"s3:{action}", LOGS_BUCKET, region=None), False)
    for action in ("GetObject", "GetObjectVersion", "PutObject", "DeleteObject", "PutObjectTagging", "AbortMultipartUpload"):
        yield Case(f"logging bucket: {action} on a log object refused", request(f"s3:{action}", f"{LOGS_BUCKET}/data_bucket_logs/x", region=None), False)
    yield Case("logging bucket: object keys cannot be listed with versions", request("s3:ListBucketVersions", LOGS_BUCKET, region=None), False)
    yield Case("logging: DeleteBucket on the project bucket refused", request("s3:DeleteBucket", BUCKET, region=None), False)
    yield Case("logging: PutBucketLogging on another bucket refused", request("s3:PutBucketLogging", other, region=None), False)
    yield Case("logging: PutBucketLogging on every bucket refused", request("s3:PutBucketLogging", "arn:aws:s3:::*", region=None), False)
    yield Case(
        "logging bucket: CreateBucket in another region",
        request("s3:CreateBucket", LOGS_BUCKET, region=None, extra={"s3:LocationConstraint": "us-east-1"}),
        False,
    )
    yield Case("logging bucket: CreateBucket without a region", request("s3:CreateBucket", LOGS_BUCKET, region=None), False)
    yield Case("logging bucket: CreateBucket with another name", request("s3:CreateBucket", other, region=None, extra={"s3:LocationConstraint": R}), False)
    yield Case("logging bucket: settings on another bucket", request("s3:PutBucketPolicy", other, region=None), False)
    yield Case("logging bucket: the project bucket keeps its own settings", request("s3:PutBucketPolicy", BUCKET, region=None), True)


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
    sid = "DenyOtherTag"
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
            "DenyNoTag",
        )
        yield Case(
            f"ecs:RunTask sets {key} to an empty value",
            request("ecs:RunTask", TASK_DEFINITION, tags={key: ""}, extra=run, region=None),
            False,
            "DenyNoTag",
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
    for value in ("Public", "Confidential"):
        yield Case(
            f"ec2:CreateTags later sets DataClassification {value} on the data volume",
            request("ec2:CreateTags", VOLUME, tags={"DataClassification": value}, extra=DATA_VOLUME_TAGS, region=None),
            True,
        )
    later: tuple[tuple[str, str, dict[str, str], dict[str, Any], str | None], ...] = (
        ("another value", VOLUME, {"DataClassification": "Restricted"}, DATA_VOLUME_TAGS, None),
        ("a lowercase key", VOLUME, {"dataclassification": "Public"}, DATA_VOLUME_TAGS, None),
        ("another key beside it", VOLUME, {"DataClassification": "Public", "Owner": "x"}, DATA_VOLUME_TAGS, "DenyTaggingAfterCreate"),
        ("the Project key beside it", VOLUME, {"DataClassification": "Public", "Project": P}, DATA_VOLUME_TAGS, "DenyTaggingAfterCreate"),
        ("another project volume", VOLUME, {"DataClassification": "Public"}, {**DATA_VOLUME_TAGS, "aws:ResourceTag/Project": "other_project"}, None),
        ("another named volume", VOLUME, {"DataClassification": "Public"}, {**DATA_VOLUME_TAGS, "aws:ResourceTag/Name": f"{P}_host_root_{E}"}, None),
        ("an untagged volume", VOLUME, {"DataClassification": "Public"}, {}, None),
        ("an address", ADDRESS, {"DataClassification": "Public"}, DATA_VOLUME_TAGS, None),
        ("the host instance", INSTANCE, {"DataClassification": "Public"}, DATA_VOLUME_TAGS, None),
    )
    for label, resource, tags, extra, sid in later:
        yield Case(
            f"ec2:CreateTags later sets DataClassification with {label}", request("ec2:CreateTags", resource, tags=tags, extra=extra, region=None), False, sid
        )
    yield Case(
        "ec2:DeleteTags removes DataClassification from the data volume",
        request("ec2:DeleteTags", VOLUME, tags={"DataClassification": ""}, extra=DATA_VOLUME_TAGS, region=None),
        False,
        "DenyRemovingTags",
    )
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
            "DenyRetag",
        )
        yield Case(
            f"ecs:UntagResource on a {name} removes Project",
            request("ecs:UntagResource", resource, tags=TAG, extra={"aws:ResourceTag/Project": P}, region=None),
            False,
            "DenyRetag",
        )
        yield Case(
            f"ecs:UntagResource on a {name} removes another tag",
            request("ecs:UntagResource", resource, tags={"Owner": ""}, region=None),
            False,
            "DenyRetag",
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
            "DenyNoProp",
        )
    yield Case("ecs:CreateService omits propagation", request("ecs:CreateService", SERVICE, tags=TAG, region=None), False, "DenyNoProp")
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
        "DenyStopProp",
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
    bronze_database, bronze_table = arn("glue", "database/hai_mirror_bronze"), arn("glue", "table/hai_mirror_bronze/acs_api_detailed")
    for action, target in (
        ("glue:GetDatabase", bronze_database),
        ("glue:GetTable", bronze_table),
        ("glue:GetTables", bronze_table),
        ("glue:CreateTable", bronze_table),
    ):
        yield Case(f"the project user has no direct {action} on the bronze mirror", request(action, target), False)
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
    for name in ("job_train_task", "mlflow_task", "host_code_fetch_task"):
        yield Case(
            f"training pass: {name} to ECS tasks",
            request("iam:PassRole", role(name), extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"}, region=None),
            True,
            "PassTrainingRoles",
        )
        for service in ("ec2.amazonaws.com", "ecs.amazonaws.com", "dlm.amazonaws.com"):
            yield Case(f"training pass: {name} to {service}", request("iam:PassRole", role(name), extra={"iam:PassedToService": service}, region=None), False)
        yield Case(f"training pass: {name} without a service", request("iam:PassRole", role(name), region=None), False)
    # The services apply registers the Airflow scheduler's task definition with the Airflow launch role as its task role (infra/services/main.tf).
    yield Case(
        "airflow launch pass: airflow_launch_task to ECS tasks",
        request("iam:PassRole", role("airflow_launch_task"), extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"}, region=None),
        True,
        "PassAirflowLaunchRole",
    )
    for service in ("ec2.amazonaws.com", "ecs.amazonaws.com", "dlm.amazonaws.com"):
        yield Case(
            f"airflow launch pass: airflow_launch_task to {service}",
            request("iam:PassRole", role("airflow_launch_task"), extra={"iam:PassedToService": service}, region=None),
            False,
        )
    yield Case("airflow launch pass: airflow_launch_task without a service", request("iam:PassRole", role("airflow_launch_task"), region=None), False)
    for name in ("airflow_launch_task_extra", "airflow_launch", "job_airflow_launch_task"):
        yield Case(
            f"airflow launch pass: refuse {name}",
            request("iam:PassRole", role(name), extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"}, region=None),
            False,
        )
    # The jobs apply registers the publisher host task definition with the publisher task role as its task role (infra/jobs). The project user's own policy
    # carries the grant; no separate owner policy does.
    yield Case(
        "publisher pass: job_publisher_task to ECS tasks",
        request("iam:PassRole", role("job_publisher_task"), extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"}, region=None),
        True,
        "PassPublisherTaskRole",
    )
    for service in ("ec2.amazonaws.com", "ecs.amazonaws.com", "dlm.amazonaws.com"):
        yield Case(
            f"publisher pass: job_publisher_task to {service}",
            request("iam:PassRole", role("job_publisher_task"), extra={"iam:PassedToService": service}, region=None),
            False,
        )
    yield Case("publisher pass: job_publisher_task without a service", request("iam:PassRole", role("job_publisher_task"), region=None), False)
    for name in ("job_publisher_task_extra", "job_publisher", "job_publisher_execution_task"):
        yield Case(
            f"publisher pass: refuse {name}",
            request("iam:PassRole", role(name), extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"}, region=None),
            False,
        )
    # The services apply registers the model API and dashboard task definitions with the serving execution role and each service's task role (infra/services).
    for name in SERVING_ROLE_NAMES:
        yield Case(
            f"serving pass: {name} to ECS tasks",
            request("iam:PassRole", role(name), extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"}, region=None),
            True,
            "PassServingRoles",
        )
        for service in ("ec2.amazonaws.com", "ecs.amazonaws.com", "dlm.amazonaws.com"):
            yield Case(f"serving pass: {name} to {service}", request("iam:PassRole", role(name), extra={"iam:PassedToService": service}, region=None), False)
        yield Case(f"serving pass: {name} without a service", request("iam:PassRole", role(name), region=None), False)
    for name in ("serving_execution_extra", "model_api_task_extra", "streamlit_task_extra", "serving", "model_api", "streamlit", "model_api_execution"):
        yield Case(
            f"serving pass: refuse {name}",
            request("iam:PassRole", role(name), extra={"iam:PassedToService": "ecs-tasks.amazonaws.com"}, region=None),
            False,
        )
    for name in ("job_train_task_extra", "mlflow_task_extra", "job_train", "mlflow", "host_code_fetch_task_extra", "host_code_fetch", "code_fetch_task"):
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
        "ReadCatalogMetadataAndStageVersions",
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
    """The AWS catalog setup under the project user: write the fourteen AWS Polaris names, read the twelve writer names, nothing wider and no listing."""
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
    for name in ("aws_polaris_root_secret", "aws_polaris_writer_client_secret"):
        for action in ("PutParameter", "GetParameter"):
            yield Case(f"aws pair: refuse {action} {name}", request(f"ssm:{action}", parameter(name), region=None), False)


def launch_hold_cases() -> Iterator[Case]:
    """The launch hold and the apply lock of the services apply command: every parameter action on those two names is allowed exactly when the command
    needs it, no neighbouring name, region, account or folder gets any of them, the parameter that carried the host job cap and the metadata listing are
    refused, and the grants reach no other parameter or key."""
    for action in PARAMETER_ACTIONS:
        yield Case(
            f"retired host job cap: refuse {action} {RETIRED_CAP_PARAMETER}", request(f"ssm:{action}", parameter(RETIRED_CAP_PARAMETER), region=None), False
        )
    for action in ("DeleteParameter", "AddTagsToResource", "RemoveTagsFromResource", "ListTagsForResource"):
        for name in ("postgres_password", "redis_password", OPENMETADATA_PASSWORDS[0], "aws_polaris_gold_client_secret"):
            yield Case(f"parameter grants: refuse {action} {name}", request(f"ssm:{action}", parameter(name), region=None), False)
    yield Case("parameter grants: no parameter metadata listing", request("ssm:DescribeParameters", "*"), False)
    for action in PARAMETER_ACTIONS:
        allowed = action in HOST_LAUNCH_HOLD_ACTIONS
        yield Case(
            f"launch hold: {action} {HOST_LAUNCH_HOLD}",
            request(f"ssm:{action}", parameter(HOST_LAUNCH_HOLD), region=None),
            allowed,
            HOST_LAUNCH_HOLD_STATEMENT if allowed else None,
        )
    for name in ("host_launch_hold_extra", "host_launch_hold/child", "host_launch"):
        for action in HOST_LAUNCH_HOLD_ACTIONS:
            yield Case(f"launch hold: refuse {action} {name}", request(f"ssm:{action}", parameter(name), region=None), False)
    for action in PARAMETER_ACTIONS:
        allowed = action in SERVICES_APPLY_LOCK_ACTIONS
        yield Case(
            f"apply lock: {action} {SERVICES_APPLY_LOCK}",
            request(f"ssm:{action}", parameter(SERVICES_APPLY_LOCK), region=None),
            allowed,
            SERVICES_APPLY_LOCK_STATEMENT if allowed else None,
        )
    yield Case("apply lock: delete the launch hold", request("ssm:DeleteParameter", parameter(HOST_LAUNCH_HOLD), region=None), False)
    for name in ("services_apply_lock_extra", "services_apply_lock/child", "services_apply"):
        for action in SERVICES_APPLY_LOCK_ACTIONS:
            yield Case(f"apply lock: refuse {action} {name}", request(f"ssm:{action}", parameter(name), region=None), False)
    for label, resource in (
        ("another region", parameter(SERVICES_APPLY_LOCK).replace(f":{R}:", ":eu-west-2:")),
        ("another account", parameter(SERVICES_APPLY_LOCK).replace(f":{A}:", ":222222222222:")),
        ("another folder", parameter(SERVICES_APPLY_LOCK).replace(f"/{PARAMETER_FOLDER}/", "/another_project/")),
    ):
        for action in SERVICES_APPLY_LOCK_ACTIONS:
            yield Case(f"apply lock: refuse {action} in {label}", request(f"ssm:{action}", resource, region=None), False)
    yield Case("parameter grants: no parameter listing in another region", request("ssm:DescribeParameters", "*", region="eu-west-2"), False)
    key = arn("kms", "key/00000000-0000-0000-0000-000000000000")
    for action in ("Decrypt", "Encrypt", "GenerateDataKey"):
        yield Case(f"parameter grants: refuse kms:{action}", request(f"kms:{action}", key), False)


def alert_subscription_cases() -> Iterator[Case]:
    """The alert topics: every action on the exact topic ARNs and their subscriptions, and nothing on a topic whose name only looks like one."""
    for label, topic in ALERT_TOPICS.items():
        for action in ALERT_TOPIC_ACTIONS:
            tags = {"DataClassification": "x"} if action.endswith("agResource") else None
            yield Case(f"alert topics: allow sns:{action} on {label}", request(f"sns:{action}", topic, tags=tags, region=None), True, ALERT_STATEMENT)
        for action in ALERT_SUBSCRIPTION_ACTIONS:
            resource = f"{topic}:{SUBSCRIPTION_ID}"
            yield Case(f"alert topics: allow sns:{action} on a subscription of {label}", request(f"sns:{action}", resource, region=None), True, ALERT_STATEMENT)
    topics = {
        "a longer topic name": arn("sns", f"{P}_alerts_{E}_extra"),
        "a topic name with a suffix": arn("sns", f"{P}_alerts_{E}2"),
        "a longer budget topic name": arn("sns", f"{P}_budget_alerts_{E}_extra"),
        "a topic without the environment": arn("sns", f"{P}_alerts"),
        "a topic that is not an alert topic": arn("sns", f"{P}_other_topic_{E}"),
        "a topic that only mentions alerts": arn("sns", f"{P}_alerts_{E}_other_topic_{E}"),
        "the alerts topic of a project whose name extends the project name": arn("sns", f"{P}_other_alerts_{E}"),
        "a topic of that project that only ends in alerts": arn("sns", f"{P}_other_notalerts_{E}"),
        "a topic of this project that only ends in alerts": arn("sns", f"{P}_notalerts_{E}"),
        "the budget alerts topic of a project whose name extends the project name": arn("sns", f"{P}_other_budget_alerts_{E}"),
        "the alerts topic of another environment": arn("sns", f"{P}_alerts_prod"),
        "the budget alerts topic of another environment": arn("sns", f"{P}_budget_alerts_prod"),
        "the alerts topic of another project": arn("sns", f"other_project_alerts_{E}"),
        "the budget alerts topic of another project": arn("sns", f"other_project_budget_alerts_{E}"),
        "the alerts topic in another region": TOPIC.replace(f":{R}:", ":eu-west-2:"),
        "the budget alerts topic in another region": BUDGET_TOPIC.replace(f":{R}:", ":eu-west-2:"),
        "the alerts topic of another account": TOPIC.replace(f":{A}:", ":222222222222:"),
        "the budget alerts topic of another account": BUDGET_TOPIC.replace(f":{A}:", ":222222222222:"),
    }
    for action in ("Subscribe", "Publish", "DeleteTopic", "SetTopicAttributes", "AddPermission", "GetSubscriptionAttributes", "Unsubscribe"):
        for label, topic in topics.items():
            yield Case(f"alert topics: refuse sns:{action} on {label}", request(f"sns:{action}", topic, region=None), False)
            yield Case(
                f"alert topics: refuse sns:{action} on a subscription of {label}", request(f"sns:{action}", f"{topic}:{SUBSCRIPTION_ID}", region=None), False
            )
    for action in ("ListTopics", "ListSubscriptions"):
        yield Case(f"alert topics: refuse sns:{action}", request(f"sns:{action}", "*"), False)


def openmetadata_password_cases() -> Iterator[Case]:
    """The catalog password setup under the project user: put and read exactly the two password names, nothing wider, no listing and no key use."""
    for name in OPENMETADATA_PASSWORDS:
        for action in ("PutParameter", "GetParameter"):
            yield Case(f"openmetadata passwords: {action} {name}", request(f"ssm:{action}", parameter(name), region=None), True, OPENMETADATA_STATEMENT)
        for action in ("GetParameters", "GetParametersByPath", "GetParameterHistory", "DeleteParameter", "DeleteParameters", "LabelParameterVersion"):
            yield Case(f"openmetadata passwords: refuse {action} {name}", request(f"ssm:{action}", parameter(name), region=None), False)
        for action in ("AddTagsToResource", "RemoveTagsFromResource"):
            yield Case(f"openmetadata passwords: refuse {action} {name}", request(f"ssm:{action}", parameter(name), region=None), False)
        for suffix in ("_extra", "/child"):
            for action in ("PutParameter", "GetParameter"):
                yield Case(f"openmetadata passwords: refuse {action} {name}{suffix}", request(f"ssm:{action}", parameter(name + suffix), region=None), False)
        for label, resource in (
            ("another region", parameter(name).replace(f":{R}:", ":eu-west-2:")),
            ("another account", parameter(name).replace(f":{A}:", ":222222222222:")),
            ("another folder", parameter(name).replace(f"/{PARAMETER_FOLDER}/", "/another_project/")),
        ):
            for action in ("PutParameter", "GetParameter"):
                yield Case(f"openmetadata passwords: refuse {action} {name} in {label}", request(f"ssm:{action}", resource, region=None), False)
    for name in OPENMETADATA_PASSWORDS:
        resource = parameter(name)
        yield Case(
            f"openmetadata passwords: create {name} with Overwrite=false",
            request("ssm:PutParameter", resource, extra={"ssm:Overwrite": "false"}, region=None),
            True,
            OPENMETADATA_STATEMENT,
        )
        for value in ("true", "True", "TRUE"):
            yield Case(
                f"openmetadata passwords: refuse overwrite of {name} with Overwrite={value}",
                request("ssm:PutParameter", resource, extra={"ssm:Overwrite": value}, region=None),
                False,
            )
        yield Case(
            f"openmetadata passwords: read {name} next to an overwrite key",
            request("ssm:GetParameter", resource, extra={"ssm:Overwrite": "true"}, region=None),
            True,
        )
    yield Case(
        "openmetadata passwords: overwrite stays allowed for a managed name",
        request("ssm:PutParameter", parameter("redis_password"), extra={"ssm:Overwrite": "true"}, region=None),
        True,
        "ManageProjectParameters",
    )
    yield Case(
        "openmetadata passwords: overwrite stays allowed for an AWS Polaris pair name",
        request("ssm:PutParameter", parameter("aws_polaris_gold_client_secret"), extra={"ssm:Overwrite": "true"}, region=None),
        True,
        "SetUpAwsPolarisPairs",
    )
    for name in OPENMETADATA_NEIGHBORS:
        for action in ("PutParameter", "GetParameter"):
            yield Case(f"openmetadata passwords: refuse {action} {name}", request(f"ssm:{action}", parameter(name), region=None), False)
    key = arn("kms", "key/00000000-0000-0000-0000-000000000000")
    for action in ("Decrypt", "Encrypt", "GenerateDataKey"):
        yield Case(f"openmetadata passwords: refuse kms:{action}", request(f"kms:{action}", key), False)


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
    "sns": ("ListTopics",),
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


ROLE_BOUNDARY = f"arn:aws:iam::{A}:policy/{P}_role_boundary"


BOUNDARY = "role_boundary"
# The roles the IAM stack creates, by the name part between the project and the environment. Each holds the documents keyed
# ``role/<name>/<policy>``, or ``owner_<name>`` for the snapshot and task execution roles, and runs under the boundary.
# The control-plane task role has no policy.
ROLES = (
    "lakehouse_reader",
    "lakehouse_writer",
    "ecs_host",
    "task_execution",
    "polaris_task",
    "control_plane_task",
    "snapshots",
    "job_execution",
    "job_try_writer",
    "job_write_bronze_task",
    "job_write_e2e_task",
)
WIDE_ROLE = "wide_role"
SESSION_USER_ID = "AROAEXAMPLEEXAMPLE:task"
# A role whose own policy grants far more than any project role: only the boundary stands between it and the account.
WIDE_ROLE_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Everything",
            "Effect": "Allow",
            "Action": [
                "iam:*",
                "budgets:*",
                "organizations:*",
                "account:*",
                "sts:AssumeRole",
                "s3:AbortMultipartUpload",
                "s3:DeleteObject",
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:PutObject",
                "ssm:GetParameters",
                "ssm:UpdateInstanceInformation",
                "ec2:AssociateAddress",
                "ec2:AttachVolume",
                "ec2:CreateSnapshot",
                "ec2:DeleteSnapshot",
                "ec2:RunInstances",
            ],
            "Resource": "*",
        }
    ],
}


def bounded_role_cases() -> Iterator[Case]:
    """A role that holds ``iam:*``, the budget or another bucket in its own policy gets ``deny`` or ``implicit_deny`` from the boundary."""

    def case(label: str, action: str, resource: str, allowed: bool, sid: str | None = None, extra: dict[str, str] | None = None) -> Case:
        return Case(f"bounded: {label}", request(action, resource, extra=extra), allowed, sid, WIDE_ROLE)

    deny = "DenyIamOrganizationsAccountAndBudgets"
    for action, resource in (
        ("iam:CreateRole", f"arn:aws:iam::{A}:role/{P}_x"),
        ("iam:AttachRolePolicy", f"arn:aws:iam::{A}:role/{P}_x"),
        ("iam:PutRolePolicy", f"arn:aws:iam::{A}:role/{P}_ecs_host_{E}"),
        ("iam:DeleteRolePermissionsBoundary", f"arn:aws:iam::{A}:role/{P}_ecs_host_{E}"),
        ("iam:PassRole", f"arn:aws:iam::{A}:role/{P}_ecs_host_{E}"),
        ("iam:CreateUser", f"arn:aws:iam::{A}:user/{P}_x"),
        ("iam:CreatePolicyVersion", ROLE_BOUNDARY),
        ("budgets:ModifyBudget", BUDGET),
        ("organizations:LeaveOrganization", "*"),
        ("account:PutAlternateContact", "*"),
    ):
        yield case(f"wide role {action}", action, resource, False, deny, {"iam:PermissionsBoundary": ROLE_BOUNDARY})
    for name in ("administrator", "healthcare_realtime_x", "other_project_role"):
        yield case(f"wide role assumes {name}", "sts:AssumeRole", f"arn:aws:iam::{A}:role/{name}", False, "DenyAssumeRoleOutsideProject")
    yield case("wide role assumes a project role the boundary does not grant", "sts:AssumeRole", f"arn:aws:iam::{A}:role/{P}_x", False)
    for bucket in ("other-bucket", f"{B}-copy"):
        yield case(f"wide role reads {bucket}", "s3:GetObject", f"arn:aws:s3:::{bucket}/lakehouse/x", False)
    yield case("wide role reads the project bucket", "s3:GetObject", f"{BUCKET}/lakehouse/x", True)
    yield case("wide role reads a parameter of another folder", "ssm:GetParameters", arn("ssm", "parameter/other_project/token"), False)
    yield case("wide role launches an instance", "ec2:RunInstances", INSTANCE, False)
    yield from boundary_scope_cases(case)


def boundary_scope_cases(case: Callable[..., Case]) -> Iterator[Case]:
    """Design section 1: the boundary allows no more than the union of the current roles' scopes (review of parts 1 and 2, finding 2).

    Each refused request is one that every current role refuses, sent by the wide role, so only the boundary decides it.
    """
    source = f"{BUCKET}/cms/hospitals/original.csv"
    for action in ("s3:DeleteObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:GetObject"):
        yield case(f"wide role {action} on a source object", action, source, False)
        yield case(f"wide role {action} on a lakehouse object", action, f"{BUCKET}/lakehouse/silver/x", True)
    yield case("wide role reads a source object version", "s3:GetObjectVersion", source, True)
    data = {"aws:ResourceTag/Project": P, "aws:ResourceTag/Name": DATA_VOLUME_NAME}
    scheduled = {**data, "aws:ResourceTag/aws:dlm:lifecycle-schedule-name": "daily"}
    yield case("wide role deletes a scheduled data snapshot", "ec2:DeleteSnapshot", SNAPSHOT, True, extra=scheduled)
    for label, context in (
        ("a manual project snapshot", {"aws:ResourceTag/Project": P, "aws:ResourceTag/Name": f"{P}_manual_backup_{E}"}),
        ("an unscheduled data snapshot", data),
        ("a weekly data snapshot", {**data, "aws:ResourceTag/aws:dlm:lifecycle-schedule-name": "weekly"}),
        ("a daily snapshot named for another volume", {**scheduled, "aws:ResourceTag/Name": f"{P}_other_volume_{E}"}),
    ):
        yield case(f"wide role deletes {label}", "ec2:DeleteSnapshot", SNAPSHOT, False, extra=context)
    other_volume = {**data, "aws:ResourceTag/Name": f"{P}_other_volume_{E}"}
    yield case("wide role snapshots the data volume", "ec2:CreateSnapshot", VOLUME, True, extra=data)
    yield case("wide role snapshots a project volume with another Name", "ec2:CreateSnapshot", VOLUME, False, extra=other_volume)
    yield case("wide role attaches the data volume", "ec2:AttachVolume", VOLUME, True, extra=data)
    yield case("wide role attaches a project volume with another Name", "ec2:AttachVolume", VOLUME, False, extra=other_volume)
    other_host = {**HOST_TAGS, "aws:ResourceTag/Name": f"{P}_other_{E}"}
    for action, kind, resource in (
        ("ec2:AttachVolume", "instance", INSTANCE),
        ("ec2:AssociateAddress", "instance", INSTANCE),
        ("ec2:AssociateAddress", "address", ADDRESS),
        ("ec2:AssociateAddress", "network interface", arn("ec2", "network-interface/eni-0123456789abcdef0")),
    ):
        yield case(f"wide role {action} on the host {kind}", action, resource, True, extra=HOST_TAGS)
        yield case(f"wide role {action} on a project {kind} with another Name", action, resource, False, extra=other_host)
    register = "ssm:UpdateInstanceInformation"
    host = {"ssm:resourceTag/Project": P, "ssm:resourceTag/Name": f"{P}_host_{E}"}
    yield case("wide role registers the host with Session Manager", register, INSTANCE, True, extra=host)
    yield case("wide role registers a project instance with another Name", register, INSTANCE, False, extra={**host, "ssm:resourceTag/Name": f"{P}_other_{E}"})


# The three GitHub delivery roles (infra/iam/github_oidc.tf) hold the delivery boundary, a second managed policy beside the role boundary:
# the role boundary has no room for them (it has no room for the delivery grants in the 6,144 characters IAM allows).
DELIVERY_BOUNDARY = "delivery_boundary"
DELIVERY_ROLES = ("image_push", "deploy", "terraform_plan", "code_staging")
DELIVERY_WIDE = "delivery_wide"
# The deploy role has a boundary of its own (D18): the serving registration and PassRole grants do not fit beside the push and plan grants in 6,144 characters.
DEPLOY_BOUNDARY = "deploy_boundary"
DEPLOY_WIDE = "deploy_wide"
# The code staging role has a boundary of its own too: each push to main stages the commit's code archive as a code run and names it in one parameter.
CODE_STAGING_BOUNDARY = "code_staging_boundary"
CODE_STAGING_WIDE = "code_staging_wide"
CODE_RUN_ID = "20261111T120000Z-0a1b2c3d"
CODE_STAGE = f"{BUCKET}/lakehouse/run_stage/{CODE_RUN_ID}/inputs"
CODE_FILES = (f"{CODE_STAGE}/code.tar.gz", f"{CODE_STAGE}/code.sha256")
CODE_RUN_PARAMETER = arn("ssm", f"parameter/{P}/current_code_run")
CODE_ONCE = {"s3:if-none-match": "*"}
WIDE_PRINCIPALS = (DELIVERY_WIDE, DEPLOY_WIDE, CODE_STAGING_WIDE)
STATE_PREFIX = "terraform_state"
STATE_STACKS = ("platform", "services", "jobs")
STATE_KEYS = tuple(f"{BUCKET}/{STATE_PREFIX}/{stack}/terraform.tfstate" for stack in STATE_STACKS)
LOCK_KEYS = tuple(f"{key}.tflock" for key in STATE_KEYS)
# D18 saved plans: CI saves each plan and its digest under this prefix. The plan role may put objects there and nothing else.
PLAN_PREFIX = "terraform_plans"
PLAN_COMMIT = "0123456789abcdef0123456789abcdef01234567"
PLAN_OBJECTS = tuple(f"{BUCKET}/{PLAN_PREFIX}/{stack}/{PLAN_COMMIT}/{name}" for stack in STATE_STACKS for name in ("plan.bin", "plan.sha256"))
DEPLOY_SERVICE = arn("ecs", f"service/{P}_cluster_{E}/{P}_{E}_model_api")
DEPLOY_STREAMLIT = arn("ecs", f"service/{P}_cluster_{E}/{P}_{E}_streamlit")
# Services of the cluster that no delivery role may update: they serve no user and hold the data stores or the orchestration.
UNSERVED_SERVICES = tuple(arn("ecs", f"service/{P}_cluster_{E}/{P}_{E}_{name}") for name in ("airflow", "postgres", "polaris", "mlflow"))
# D18 model releases: the deploy role registers the serving families and passes the two serving task roles and the dedicated serving execution role. The shared
# task execution role reads the PostgreSQL and Polaris root secrets for the tasks that name them, so it stays out of reach.
SERVING_SERVICES = ("model_api", "streamlit")
SERVING_ROLE_NAMES = ("model_api_task", "streamlit_task", "serving_execution")
SERVING_ROLES = tuple(f"arn:aws:iam::{A}:role/{P}_{name}_{E}" for name in SERVING_ROLE_NAMES)
REFUSED_PASS_ROLE_NAMES = (
    "task_execution",
    "polaris_task",
    "control_plane_task",
    "ecs_host",
    "job_execution",
    "job_write_bronze_task",
    "gh_deploy",
    "gh_terraform_plan",
    "snapshots",
)
SERVING_REVISIONS = tuple(arn("ecs", f"task-definition/{P}_{E}_{name}:7") for name in SERVING_SERVICES)
DEPLOY_TASK_ROLES = SERVING_ROLES
# D18 model releases: GitHub registers serving revisions that name a dedicated execution role. It must read none of the control-plane secrets.
SERVING_PARAMETERS = (
    "model_api_key_hashes",
    "aws_polaris_serving_client_id",
    "aws_polaris_serving_client_secret",
    "model_api_mlflow_user",
    "model_api_mlflow_credential",
    "model_api_redis_password",
    "streamlit_model_api_key",
)
SERVING_EXECUTION = "serving_execution"
ECS_TASKS = "ecs-tasks.amazonaws.com"
# A delivery role whose own policy grants far more than the delivery work: only the delivery boundary stands between it and the account.
DELIVERY_WIDE_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Everything",
            "Effect": "Allow",
            "Action": ["ecr:*", "ecs:*", "s3:*", "ec2:*", "iam:*", "logs:*", "ssm:*", "secretsmanager:*", "sns:*", "sts:AssumeRole", "budgets:*"],
            "Resource": "*",
        }
    ],
}


def delivery_cases() -> Iterator[Case]:
    """The deploy role's work with the requests AWS sends for it, and what stays refused for every delivery role under the delivery boundary.

    The image push and plan requests come from their policies in ``coverage_cases``; the deploy role's policy names ``*`` for actions that
    AWS authorizes on one service, task or cluster, so its requests are written out.
    """

    def deploy(label: str, action: str, resource: str, extra: dict[str, str] | None = None) -> Case:
        return Case(f"delivery allowed: deploy {label}", request(action, resource, extra=extra), True, None, f"delivery_{DELIVERY_ROLES[1]}")

    yield deploy("describes the cluster", "ecs:DescribeClusters", CLUSTER)
    yield deploy("describes its service", "ecs:DescribeServices", DEPLOY_SERVICE)
    yield deploy("describes a task of the cluster", "ecs:DescribeTasks", TASK)
    yield deploy("lists the tasks of the project cluster", "ecs:ListTasks", "*", {"ecs:cluster": CLUSTER})
    yield deploy("updates the model API service", "ecs:UpdateService", DEPLOY_SERVICE)
    yield deploy("updates the dashboard service", "ecs:UpdateService", DEPLOY_STREAMLIT)
    yield deploy("describes a task definition", "ecs:DescribeTaskDefinition", "*")

    # The shared task execution role reads the PostgreSQL and Polaris root secrets for the tasks that name them. A delivery role that could register a
    # task definition, pass that role to ECS and update a service could pull those secrets into a crafted task. So the deploy role registers only the serving
    # families (with the Project tag), passes only the two serving task roles and the dedicated serving execution role, and updates only the serving
    # services. Each link is checked for the role itself and under the boundary alone.
    deploy_role = f"delivery_{DELIVERY_ROLES[1]}"

    def chain(label: str, action: str, resource: str, extra: dict[str, str] | None = None, tags: dict[str, str] | None = None) -> Iterator[Case]:
        for principal in (deploy_role, DEPLOY_WIDE):
            yield Case(f"delivery refused: crafted task definition chain: {label}", request(action, resource, extra=extra, tags=tags), False, None, principal)

    def allowed_link(label: str, action: str, resource: str, extra: dict[str, str] | None = None, tags: dict[str, str] | None = None) -> Iterator[Case]:
        for principal in (deploy_role, DEPLOY_WIDE):
            yield Case(f"delivery allowed: serving release: {label}", request(action, resource, extra=extra, tags=tags), True, None, principal)

    for revision in SERVING_REVISIONS:
        family = revision.rsplit("/", 1)[-1].rsplit(":", 1)[0]
        yield from allowed_link(f"registers {family}", "ecs:RegisterTaskDefinition", revision, tags=TAG)
        yield from allowed_link(f"tags {family} while registering", "ecs:TagResource", revision, {"ecs:CreateAction": "RegisterTaskDefinition"})
    for name in ("airflow", "postgres", "polaris", "mlflow", "model_api_extra", "streamlit_extra", "control_plane"):
        yield from chain(f"registers the family {name}", "ecs:RegisterTaskDefinition", arn("ecs", f"task-definition/{P}_{E}_{name}:7"), None)
        yield from chain(
            f"registers the family {name} with the Project tag", "ecs:RegisterTaskDefinition", arn("ecs", f"task-definition/{P}_{E}_{name}:7"), tags=TAG
        )
    for label, revision in (
        ("a family of another environment", arn("ecs", f"task-definition/{P}_prod_model_api:7")),
        ("a family of another project", arn("ecs", f"task-definition/other_project_{E}_model_api:7")),
        ("a family in another account", SERVING_REVISIONS[0].replace(A, "222222222222")),
    ):
        yield from chain(f"registers {label}", "ecs:RegisterTaskDefinition", revision, tags=TAG)
    # The boundary keeps the tag conditions of the inline policy, so a role whose policy grows cannot register without the Project tag or re-tag a revision.
    yield from chain("registers a serving family without the Project tag", "ecs:RegisterTaskDefinition", SERVING_REVISIONS[0])
    yield from chain(
        "registers a serving family with another project's tag", "ecs:RegisterTaskDefinition", SERVING_REVISIONS[0], tags={"Project": "other_project"}
    )
    yield from chain("registers a serving family with a lowercase project key", "ecs:RegisterTaskDefinition", SERVING_REVISIONS[0], tags={"project": P})
    yield from chain("re-tags a serving revision", "ecs:TagResource", SERVING_REVISIONS[0])
    yield from chain("tags a serving revision for another create action", "ecs:TagResource", SERVING_REVISIONS[0], {"ecs:CreateAction": "CreateService"})
    yield from chain(
        "tags another family while registering",
        "ecs:TagResource",
        arn("ecs", f"task-definition/{P}_{E}_airflow:7"),
        {"ecs:CreateAction": "RegisterTaskDefinition"},
    )
    for action in ("ecs:DeregisterTaskDefinition", "ecs:RunTask", "ecs:CreateService"):
        yield from chain(f"{action} on a serving family", action, SERVING_REVISIONS[0])
    for role in SERVING_ROLES:
        yield from allowed_link(f"passes {role.rsplit('/', 1)[-1]} to ECS tasks", "iam:PassRole", role, {"iam:PassedToService": ECS_TASKS})
    for name in REFUSED_PASS_ROLE_NAMES:
        yield from chain(f"passes {name} to ECS tasks", "iam:PassRole", f"arn:aws:iam::{A}:role/{P}_{name}_{E}", {"iam:PassedToService": ECS_TASKS})
    for role in SERVING_ROLES:
        yield from chain(
            f"passes {role.rsplit('/', 1)[-1]} to a service other than ECS tasks", "iam:PassRole", role, {"iam:PassedToService": "ec2.amazonaws.com"}
        )
    yield from chain(
        "passes the serving execution role of another environment",
        "iam:PassRole",
        f"arn:aws:iam::{A}:role/{P}_serving_execution_prod",
        {"iam:PassedToService": ECS_TASKS},
    )
    for service in UNSERVED_SERVICES:
        yield from chain(f"updates a service that is not served: {service.rsplit('/', 1)[-1]}", "ecs:UpdateService", service)

    def wide(label: str, action: str, resource: str, extra: dict[str, str] | None = None, sid: str | None = None) -> Iterator[Case]:
        for principal in WIDE_PRINCIPALS:
            yield Case(f"delivery refused: {label}", request(action, resource, extra=extra), False, sid, principal)

    # Saved plans (D18): the plan role puts the plan and its digest under terraform_plans/ and does nothing else with that prefix. A plan holds values,
    # so no delivery role reads, lists, deletes or replaces another job's plan, and no write leaves the prefix.
    plan_role = f"delivery_{DELIVERY_ROLES[2]}"
    who = {plan_role: DELIVERY_ROLES[2], DELIVERY_WIDE: "boundary", DEPLOY_WIDE: "deploy boundary"}
    for key in PLAN_OBJECTS:
        yield Case(f"delivery allowed: terraform_plan saves {key.split('/', 1)[1]}", request("s3:PutObject", key), True, None, plan_role)
        yield Case(f"delivery allowed: boundary lets a delivery role save {key.split('/', 1)[1]}", request("s3:PutObject", key), True, None, DELIVERY_WIDE)
    for principal in (plan_role, DELIVERY_WIDE, DEPLOY_WIDE):
        for label, resource in (
            ("a platform state key", STATE_KEYS[0]),
            ("the IAM stack state", f"{BUCKET}/{STATE_PREFIX}/iam/terraform.tfstate"),
            ("a state lock", LOCK_KEYS[0]),
            ("a lakehouse object", f"{BUCKET}/lakehouse/silver/x"),
            ("a source object", f"{BUCKET}/census/x"),
            ("the bucket root", f"{BUCKET}/x"),
            ("a prefix that only starts like the plans prefix", f"{BUCKET}/{PLAN_PREFIX}_backup/platform/{PLAN_COMMIT}/plan.bin"),
            ("a prefix that nearly matches the plans prefix", f"{BUCKET}/terraform_plan/platform/{PLAN_COMMIT}/plan.bin"),
            ("the plans prefix of another bucket", f"arn:aws:s3:::other-bucket/{PLAN_PREFIX}/platform/{PLAN_COMMIT}/plan.bin"),
            ("the plans prefix of the logs bucket", f"{BUCKET}-logs/{PLAN_PREFIX}/platform/{PLAN_COMMIT}/plan.bin"),
        ):
            yield Case(f"delivery refused: saved plans: {who[principal]} puts {label}", request("s3:PutObject", resource), False, None, principal)
        for action in ("s3:GetObject", "s3:GetObjectVersion", "s3:DeleteObject", "s3:DeleteObjectVersion", "s3:PutObjectAcl"):
            yield Case(
                f"delivery refused: saved plans: {who[principal]} uses {action} on a saved plan",
                request(action, PLAN_OBJECTS[0]),
                False,
                None,
                principal,
            )
    # The plan role may still list the data bucket (the storage stack refresh needs s3:ListBucket on it), so it sees the key names of saved plans:
    # the stack and the commit. The plans themselves stay unreadable.
    for role in (DELIVERY_ROLES[0], DELIVERY_ROLES[1]):
        yield Case(f"delivery refused: saved plans: {role} puts a saved plan", request("s3:PutObject", PLAN_OBJECTS[0]), False, None, f"delivery_{role}")
    # The state keys stay readable and unwritable for the plan role while it saves plans.
    yield Case("delivery allowed: terraform_plan still reads a state key", request("s3:GetObject", STATE_KEYS[0]), True, None, plan_role)
    # Delivery plan role (owner A, Nov 10 2026): the plan role lists no parameter metadata and no parameter tags.
    yield Case("delivery refused: terraform_plan lists parameter metadata", request("ssm:DescribeParameters", "*"), False, None, plan_role)
    yield Case(
        "delivery refused: terraform_plan lists the tags of the launch hold",
        request("ssm:ListTagsForResource", parameter(HOST_LAUNCH_HOLD)),
        False,
        None,
        plan_role,
    )
    yield Case(
        "delivery refused: saved plans: terraform_plan writes a state key",
        request("s3:PutObject", STATE_KEYS[0]),
        False,
        "NeverWriteObjectsExceptSavedPlans",
        plan_role,
    )

    other_repository = arn("ecr", f"repository/other_project_spark_{E}")
    for label, resource in (
        ("another project's repository", other_repository),
        ("a repository of another environment", arn("ecr", f"repository/{P}_spark_prod")),
        ("a repository without the environment", arn("ecr", f"repository/{P}_spark")),
        ("a repository in another account", REPOSITORY.replace(A, "222222222222")),
    ):
        yield from wide(f"push to {label}", "ecr:PutImage", resource)
    for action in (
        "CreateRepository",
        "DeleteRepository",
        "BatchDeleteImage",
        "PutLifecyclePolicy",
        "SetRepositoryPolicy",
        "DeleteLifecyclePolicy",
        "PutImageTagMutability",
    ):
        yield from wide(f"ecr:{action} on a project repository", f"ecr:{action}", REPOSITORY)
    for action in (
        "CreateService",
        "DeleteService",
        "DeleteCluster",
        "RunTask",
        "StartTask",
        "StopTask",
        "DeregisterTaskDefinition",
        "CreateCluster",
        "PutClusterCapacityProviders",
    ):
        yield from wide(f"ecs:{action}", f"ecs:{action}", SERVICE if "Service" in action else CLUSTER)
    for label, resource in (
        ("a service of another cluster", arn("ecs", f"service/other_cluster/{P}_{E}_model_api")),
        ("a service outside the project service names", arn("ecs", f"service/{P}_cluster_{E}/other_service")),
        ("a service of another environment", arn("ecs", f"service/{P}_cluster_prod/{P}_prod_model_api")),
    ):
        yield from wide(f"ecs:UpdateService on {label}", "ecs:UpdateService", resource)
    for role in (
        f"{P}_ecs_host_{E}",
        f"{P}_job_write_bronze_task_{E}",
        f"{P}_gh_terraform_plan_{E}",
        f"{P}_gh_deploy_{E}",
        "administrator",
        f"{P}_model_api_task_prod",
    ):
        yield from wide(f"passes {role} to ECS tasks", "iam:PassRole", f"arn:aws:iam::{A}:role/{role}", {"iam:PassedToService": ECS_TASKS})
    for service in ("ec2.amazonaws.com", "lambda.amazonaws.com"):
        yield from wide(f"passes a task role to {service}", "iam:PassRole", DEPLOY_TASK_ROLES[0], {"iam:PassedToService": service})
    yield from wide("passes a task role without a service", "iam:PassRole", DEPLOY_TASK_ROLES[0], sid="DenyIam")
    for action in ("iam:CreateRole", "iam:AttachRolePolicy", "iam:PutRolePolicy", "iam:CreateUser", "iam:GetRole"):
        yield from wide(action, action, f"arn:aws:iam::{A}:role/{P}_x_{E}", sid="DenyIam")
    yield from wide("budgets:ModifyBudget", "budgets:ModifyBudget", BUDGET, sid="DenyOrganizationsAccountAndBudgets")
    yield from wide("organizations:LeaveOrganization", "organizations:LeaveOrganization", "*", sid="DenyOrganizationsAccountAndBudgets")
    for name in (f"{P}_gh_terraform_plan_{E}", f"{P}_gh_image_push_{E}", "administrator"):
        yield from wide(f"assumes {name}", "sts:AssumeRole", f"arn:aws:iam::{A}:role/{name}")
    yield from wide("assumes a role of another project", "sts:AssumeRole", f"arn:aws:iam::{A}:role/other_project_x", sid="DenyAssumeRoleOutsideProject")
    states = f"{BUCKET}/{STATE_PREFIX}"
    for label, resource in (
        ("the IAM stack state", f"{states}/iam/terraform.tfstate"),
        ("the storage stack state", f"{states}/storage/terraform.tfstate"),
        ("the state of a stack the delivery roles do not plan", f"{states}/network/terraform.tfstate"),
        ("a lakehouse object", f"{BUCKET}/lakehouse/silver/x"),
        ("a source object", f"{BUCKET}/census/x"),
        ("another bucket's state", f"arn:aws:s3:::other-bucket/{STATE_PREFIX}/platform/terraform.tfstate"),
    ):
        yield from wide(f"reads {label}", "s3:GetObject", resource)
    for label, resource in (
        ("the IAM stack state", f"{states}/iam/terraform.tfstate"),
        ("a lakehouse object", f"{BUCKET}/lakehouse/silver/x"),
        ("the bucket root", f"{BUCKET}/x"),
    ):
        yield from wide(f"writes {label}", "s3:PutObject", resource)
    for label, resource in (
        ("a state object", STATE_KEYS[0]),
        ("the lock of the IAM stack", f"{states}/iam/terraform.tfstate.tflock"),
        ("a lakehouse object", f"{BUCKET}/lakehouse/silver/x"),
    ):
        yield from wide(f"deletes {label}", "s3:DeleteObject", resource)
    for label, resource in (
        ("a platform state key", STATE_KEYS[0]),
        ("a services state key", STATE_KEYS[1]),
        ("a state backup", f"{states}/platform/terraform.tfstate.backup"),
    ):
        yield from wide(f"writes {label}", "s3:PutObject", resource)
    yield from wide("holds a state lock", "s3:PutObject", LOCK_KEYS[0])
    yield from wide("deletes a state lock", "s3:DeleteObject", LOCK_KEYS[0])
    yield from wide("lists another bucket", "s3:ListBucket", "arn:aws:s3:::other-bucket")
    yield from wide("reads a state version", "s3:GetObjectVersion", STATE_KEYS[0])
    yield from wide("reads a parameter", "ssm:GetParameters", arn("ssm", f"parameter/{P}/x"))
    yield from wide("reads a secret value", "secretsmanager:GetSecretValue", arn("secretsmanager", "secret:census_api_key-abcdef"))
    # The metadata reads name the approved shared secrets and the exact state keys: no pattern reaches a neighbour.
    for name in SHARED_KEYS:
        yield Case(
            f"delivery allowed: boundary describes the shared secret {name}",
            request("secretsmanager:DescribeSecret", arn("secretsmanager", f"secret:{name}-AbCdEf"), region=None),
            True,
            None,
            DELIVERY_WIDE,
        )
    for name in ("another_project_api_key", "bls_api_key_extra", "other_api_key", "census_key"):
        yield from wide(f"describes the secret {name}", "secretsmanager:DescribeSecret", arn("secretsmanager", f"secret:{name}-AbCdEf"), None)
    for label, name in (
        ("a devops log group of the environment", f"{P}_jobs_{E}_devops"),
        ("a devops log group without the environment", f"{P}_devops"),
        ("a log group of another project", f"other_project_jobs_{E}"),
    ):
        yield from wide(f"lists the tags of {label}", "logs:ListTagsLogGroup", arn("logs", f"log-group:{name}"))
    for key in STATE_KEYS:
        yield Case(f"delivery allowed: boundary reads the state key {key.rsplit('/', 2)[-2]}", request("s3:GetObject", key), True, None, DELIVERY_WIDE)
    for suffix in (".backup", ".tflock", ".tfstate", "-copy"):
        yield from wide(f"reads a platform state key with the suffix {suffix}", "s3:GetObject", f"{STATE_KEYS[0]}{suffix}")
    yield from wide("reads log events", "logs:GetLogEvents", f"{LOG_GROUP}:log-stream:x")
    yield from wide("launches an instance", "ec2:RunInstances", INSTANCE)
    yield from wide("terminates an instance", "ec2:TerminateInstances", INSTANCE)
    yield from wide("publishes to the alert topic", "sns:Publish", TOPIC)
    # The CI plan role never reads an alert subscription (owner, Nov 11 2026): neither the delivery boundary nor the plan role holds
    # sns:GetSubscriptionAttributes, on the platform alerts topic or the budget alerts topic, so a pull request plan can never see an alert address.
    for label, resource in (
        ("a subscription of the alerts topic", SUBSCRIPTION),
        ("the alerts topic", TOPIC),
        ("a subscription of the budget alerts topic", BUDGET_SUBSCRIPTION),
        ("the budget alerts topic", BUDGET_TOPIC),
    ):
        yield from wide(f"reads the attributes of {label}", "sns:GetSubscriptionAttributes", resource)
        yield Case(
            f"delivery refused: terraform_plan reads the attributes of {label}", request("sns:GetSubscriptionAttributes", resource), False, None, plan_role
        )
        for action in ("sns:Subscribe", "sns:Unsubscribe", "sns:SetSubscriptionAttributes", "sns:ConfirmSubscription", "sns:ListSubscriptionsByTopic"):
            yield from wide(f"changes {label} with {action}", action, resource)


def coverage_requests(statement: dict[str, Any], tag_context: dict[str, tuple[str, ...]]) -> Iterator[Request]:
    """Requests that one role statement allows: each action on each resource, with every condition value the statement accepts.

    A wildcard in a resource or value becomes a sample word. IAM sends ``aws:userid`` for every role session, so a policy variable in a resource is
    that session's identifier.
    The request tags of the role's other statements for the same action are sent too, so a deny that needs them does not fire.
    """
    positive: list[tuple[str, list[str]]] = []
    for operator, keys in statement.get("Condition", {}).items():
        if "Not" in operator or operator.endswith("IfExists"):
            continue
        for key, expected in keys.items():
            if operator == "Null":
                if str(as_list(expected)[0]).lower() == "false":
                    positive.append((key, ["sample"]))
                continue
            # Only a pattern operator reads a wildcard in its value as one. Exact comparison, such as the once condition "*", keeps it as written.
            matches = operator.startswith(("StringLike", "ArnLike"))
            positive.append((key, [str(value).replace("*", "sample").replace("?", "a") if matches else str(value) for value in as_list(expected)]))
    combinations: list[dict[str, tuple[str, ...]]] = [{}]
    for key, values in positive:
        combinations = [{**combination, key: (value,)} for combination in combinations for value in values]
    for action in as_list(statement["Action"]):
        for resource in as_list(statement["Resource"]):
            concrete = POLICY_VARIABLE.sub(SESSION_USER_ID, resource)
            concrete = concrete if concrete == "*" else concrete.replace("*", "sample").replace("?", "a")
            for combination in combinations:
                context = {"aws:RequestedRegion": (R,), "aws:userid": (SESSION_USER_ID,), **tag_context, **combination}
                yield Request(action, concrete, context)


def coverage_cases(documents: dict[str, dict[str, Any]]) -> Iterator[Case]:
    """Every request a role's own policies allow today is allowed under the boundary: the boundary covers all current role access.

    The delivery roles' requests follow the same rule against the delivery boundary. The deploy role is covered by ``delivery_cases``.
    """
    held_by = principals(documents)
    for role in (name for name in DELIVERY_ROLES if name != "deploy"):
        # Every document the role holds, the inline policy and any attached policy: a policy the boundary caps would otherwise pass unnoticed.
        for key in sorted(key for key in documents if key.startswith(f"delivery/{role}/")):
            for statement in documents[key]["Statement"]:
                if statement["Effect"] == "Allow":
                    for item in coverage_requests(statement, {}):
                        label = f"delivery allowed: {role} {key.rsplit('/', 1)[-1]} {statement.get('Sid')} {item.action} {item.resource}"
                        yield Case(label, item, True, None, f"delivery_{role}")
    for name in ROLES:
        allows = [statement for document in held_by[name] for statement in document["Statement"] if statement["Effect"] == "Allow"]
        for statement in allows:
            actions = set(as_list(statement["Action"]))
            tag_context: dict[str, tuple[str, ...]] = {
                str(key): (str(as_list(expected)[0]),)
                for other in allows
                if actions & set(as_list(other["Action"]))
                for operator, keys in other.get("Condition", {}).items()
                if "Not" not in operator
                for key, expected in keys.items()
                if key.startswith("aws:RequestTag/") or key == "aws:TagKeys"
            }
            for item in coverage_requests(statement, tag_context):
                yield Case(f"coverage: {name} {statement.get('Sid')} {item.action} {item.resource}", item, True, None, name)


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
        "sns": "*",
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
            topics = (resources[service],)
            for resource in topics:
                yield Case(
                    f"forbidden {service}:{action} on {resource.rsplit(':', 1)[-1][:40]}", request(f"{service}:{action}", resource, tags=TAG, extra=rich), False
                )
    yield Case(
        "forbidden ssm:StartSession shell document",
        request("ssm:StartSession", f"arn:aws:ssm:{R}::document/SSM-SessionManagerRunShell", extra=rich, region=None),
        False,
    )
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
    # EC2 authorizes the volume part of CreateSnapshot without the request tags; the snapshot part carries them and refuses a wrong tag.
    yield case("volume part of the data volume snapshot without request tags", "ec2:CreateSnapshot", SNAPSHOT_SOURCE, True, None, volume)
    yield case(
        "volume part of the data volume snapshot with scheduler tags only",
        "ec2:CreateSnapshot",
        SNAPSHOT_SOURCE,
        True,
        {"SnapshotCreator": "DataLifecycleManager"},
        volume,
    )
    yield case(
        "volume part of another project's volume without request tags",
        "ec2:CreateSnapshot",
        SNAPSHOT_SOURCE,
        False,
        None,
        {**volume, "aws:ResourceTag/Project": "other_project"},
    )
    yield case(
        "volume part of another volume without request tags",
        "ec2:CreateSnapshot",
        SNAPSHOT_SOURCE,
        False,
        None,
        {**volume, "aws:ResourceTag/Name": "other_volume"},
    )
    yield case("new snapshot without the tag", "ec2:CreateSnapshot", SNAPSHOT, False, {"SnapshotCreator": "DataLifecycleManager"})
    yield case("new snapshot with no tags", "ec2:CreateSnapshot", SNAPSHOT, False)
    yield case("new snapshot with another Project", "ec2:CreateSnapshot", SNAPSHOT, False, {"Project": "other_project"})
    yield case("snapshot of another volume", "ec2:CreateSnapshot", SNAPSHOT_SOURCE, False, SCHEDULE_TAGS, {**volume, "aws:ResourceTag/Name": "other_volume"})
    for key in ("project", "PROJECT", "Project "):
        tags = {key: P, "SnapshotCreator": "DataLifecycleManager"}
        exact_key_deny = None if key.strip() != key else "DenySnapshotsWithoutExactProjectKey"
        for action in ("ec2:CreateSnapshot", "ec2:CreateSnapshots", "ec2:CopySnapshot"):
            yield case(f"{action} with the key {key!r}", action, SNAPSHOT, False, tags, sid=exact_key_deny)
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
    """The task execution role: it injects exactly eight parameters, pulls project images and writes project logs."""
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


def code_staging_cases() -> Iterator[Case]:
    """The code staging role's work and what stays refused for it, for the role itself and for its boundary alone.

    The role creates the two code files of a run once and puts the one parameter that names the current code run. It cannot bind a run to a code run
    (``code_run.json``), stage inputs or a request, read, list or delete anything, or put another parameter.
    """
    role = f"delivery_{DELIVERY_ROLES[3]}"
    for principal in (role, CODE_STAGING_WIDE):
        for key in CODE_FILES:
            name = key.rsplit("/", 1)[-1]
            yield Case(
                f"delivery allowed: code staging creates {name} once ({principal})", request("s3:PutObject", key, extra=CODE_ONCE), True, None, principal
            )
        yield Case(
            f"delivery allowed: code staging names the current code run ({principal})", request("ssm:PutParameter", CODE_RUN_PARAMETER), True, None, principal
        )

        def refused(label: str, action: str, resource: str, extra: dict[str, Any] | None = None, principal: str = principal) -> Case:
            return Case(f"delivery refused: code staging {label} ({principal})", request(action, resource, extra=extra), False, None, principal)

        yield refused("puts a code file without the once condition", "s3:PutObject", CODE_FILES[0])
        yield refused("puts a code file with another precondition", "s3:PutObject", CODE_FILES[0], {"s3:if-none-match": "etag-of-an-old-object"})
        yield refused("puts a code file with a version check", "s3:PutObject", CODE_FILES[0], {"s3:if-match": '"9b2cf535f27731c974343645a3985328"'})
        for label, name in (
            ("binds a run to a code run", "code_run.json"),
            ("stages the inputs archive", "publisher_inputs.tar.gz"),
            ("stages a publisher request", "publisher_request.json"),
            ("stages a bronze request", "bronze_request.json"),
            ("puts a file that only starts like a code file", "codex.tar.gz"),
            ("puts a file named code with no extension", "code"),
        ):
            yield refused(label, "s3:PutObject", f"{CODE_STAGE}/{name}", CODE_ONCE)
        for label, resource in (
            ("puts a code file outside the inputs folder", f"{BUCKET}/lakehouse/run_stage/{CODE_RUN_ID}/code.tar.gz"),
            ("puts a code file in another folder of the lakehouse", f"{BUCKET}/lakehouse/silver/code.tar.gz"),
            ("puts a code file under raw", f"{BUCKET}/raw/code.tar.gz"),
            ("puts a code file at the bucket root", f"{BUCKET}/code.tar.gz"),
            ("puts a code file in the logs bucket", f"{LOGS_BUCKET}/lakehouse/run_stage/{CODE_RUN_ID}/inputs/code.tar.gz"),
            ("puts a code file in another bucket", f"arn:aws:s3:::other-bucket/lakehouse/run_stage/{CODE_RUN_ID}/inputs/code.tar.gz"),
        ):
            yield refused(label, "s3:PutObject", resource, CODE_ONCE)
        for action in ("s3:GetObject", "s3:GetObjectVersion", "s3:DeleteObject", "s3:DeleteObjectVersion", "s3:PutObjectAcl", "s3:AbortMultipartUpload"):
            yield refused(f"uses {action} on a code file", action, CODE_FILES[0], CODE_ONCE)
        for action in ("s3:ListBucket", "s3:ListBucketVersions"):
            yield refused(f"uses {action} on the bucket", action, BUCKET, {"s3:prefix": "lakehouse/run_stage/"})
        for label, resource in (
            ("another parameter of the project", arn("ssm", f"parameter/{P}/host_job_cap")),
            ("a parameter that only starts like the code run one", arn("ssm", f"parameter/{P}/current_code_run_extra")),
            ("the code run parameter of another project", arn("ssm", "parameter/other_project/current_code_run")),
            ("the code run parameter in another account", CODE_RUN_PARAMETER.replace(A, "222222222222")),
        ):
            yield refused(f"puts {label}", "ssm:PutParameter", resource)
        for action in ("ssm:GetParameter", "ssm:GetParameters", "ssm:DeleteParameter", "ssm:LabelParameterVersion", "ssm:AddTagsToResource"):
            yield refused(f"uses {action} on the code run parameter", action, CODE_RUN_PARAMETER)
        for action, resource in (
            ("ecr:PutImage", REPOSITORY),
            ("ecs:RunTask", arn("ecs", f"task-definition/{P}_{E}_job_publisher_host:1")),
            ("ecs:UpdateService", DEPLOY_SERVICE),
            ("iam:PassRole", SERVING_ROLES[0]),
            ("iam:CreateRole", arn("iam", f"role/{P}_other_{E}", region=False)),
            ("secretsmanager:GetSecretValue", arn("secretsmanager", "secret:jev_api_key-abcdef")),
            ("sts:AssumeRole", arn("iam", f"role/{P}_job_publisher_task_{E}", region=False)),
            ("sns:Publish", arn("sns", f"{P}_alerts_{E}")),
        ):
            yield refused(f"uses {action}", action, resource)


def serving_execution_cases() -> Iterator[Case]:
    """The dedicated serving execution role: it injects exactly the six serving parameters, pulls two images and writes two log groups.

    GitHub registers serving task definition revisions that name this role (D18), so it must read none of the control-plane secrets that the shared task
    execution role injects. The shared role's nine parameters are each refused.
    """

    def case(label: str, action: str, resource: str, allowed: bool, region: str | None = R) -> Case:
        return Case(f"serving execution: {label}", request(action, resource, region=region), allowed, None, SERVING_EXECUTION)

    for name in SERVING_PARAMETERS:
        yield case(f"injects {name}", "ssm:GetParameters", parameter(name), True)
    yield case("cannot inject the Redis password of the control plane, only its own read-only user's", "ssm:GetParameters", parameter("redis_password"), False)
    for name in EXECUTION_PARAMETERS:
        yield case(f"cannot inject the control-plane parameter {name}", "ssm:GetParameters", parameter(name), False)
    for name in ("airflow_admin", "other_secret", "mlflow_db_password", "mlflow_admin_password", "model_api_key_hashes_extra"):
        yield case(f"cannot inject {name}", "ssm:GetParameters", parameter(name), False)
    yield case(
        "cannot inject a serving name of another project",
        "ssm:GetParameters",
        parameter("model_api_key_hashes").replace(PARAMETER_FOLDER, "other_project"),
        False,
    )
    yield case("cannot inject a serving name in another region", "ssm:GetParameters", parameter("model_api_key_hashes").replace(R, "us-east-1"), False)
    for action in ("ssm:GetParameter", "ssm:GetParametersByPath", "ssm:PutParameter", "ssm:DescribeParameters", "secretsmanager:GetSecretValue", "kms:Decrypt"):
        yield case(f"no {action}", action, parameter("model_api_key_hashes"), False)
    yield case("cannot read a Secrets Manager secret", "secretsmanager:GetSecretValue", arn("secretsmanager", "secret:census_api_key-AbCdEf"), False)
    yield case("registry token", "ecr:GetAuthorizationToken", "*", True)
    yield case("registry token in another region", "ecr:GetAuthorizationToken", "*", False, "us-east-1")
    for service in SERVING_SERVICES:
        for action in ("BatchCheckLayerAvailability", "BatchGetImage", "GetDownloadUrlForLayer"):
            yield case(f"pulls {service} with {action}", f"ecr:{action}", arn("ecr", f"repository/{P}_{service}_{E}"), True)
    for name in ("airflow", "training", "spark", "polaris", "model_api_extra"):
        yield case(f"cannot pull the {name} image", "ecr:BatchGetImage", arn("ecr", f"repository/{P}_{name}_{E}"), False)
    yield case("cannot pull a serving image of another environment", "ecr:BatchGetImage", arn("ecr", f"repository/{P}_model_api_prod"), False)
    yield case("cannot pull another project's serving image", "ecr:BatchGetImage", arn("ecr", f"repository/other_project_model_api_{E}"), False)
    yield case("cannot push", "ecr:PutImage", arn("ecr", f"repository/{P}_model_api_{E}"), False)
    # The services stack sends both serving containers to the shared airflow log group, each under its own stream prefix.
    group = f"log-group:{P}_airflow_{E}"
    for service in SERVING_SERVICES:
        stream = arn("logs", f"{group}:log-stream:{service}/{service}/abc")
        yield case(f"writes the {service} log stream", "logs:PutLogEvents", stream, True)
        yield case(f"creates a {service} log stream", "logs:CreateLogStream", stream, True)
    for name in ("airflow_scheduler", "postgres", "polaris"):
        yield case(f"cannot write the {name} log stream", "logs:PutLogEvents", arn("logs", f"{group}:log-stream:{name}/{name}/abc"), False)
    for name in ("jobs", "postgres", "model_api"):
        yield case(
            f"cannot write the {name} log group", "logs:PutLogEvents", arn("logs", f"log-group:{P}_{name}_{E}:log-stream:model_api/model_api/abc"), False
        )
    yield case(
        "cannot write another project's log", "logs:PutLogEvents", arn("logs", f"log-group:other_project_airflow_{E}:log-stream:model_api/model_api/abc"), False
    )
    yield case("cannot create a log group", "logs:CreateLogGroup", arn("logs", f"log-group:{P}_model_api_{E}"), False)


def all_cases(operations: dict[str, list[Request]]) -> Iterator[Case]:
    """Every case the evaluator suite runs."""
    current = list(current_cases(operations))
    yield from current
    yield from bounded_role_cases()
    yield from delivery_cases()
    yield from code_staging_cases()
    yield from serving_execution_cases()


def current_cases(operations: dict[str, list[Request]]) -> Iterator[Case]:
    """The cases of the policies attached today."""
    yield from scope_cases(operations)
    yield from scope_cases({"s3": bucket_requests()})
    yield from single_copy_cases()
    yield from promote_cases()
    yield from research_cases()
    yield from logging_bucket_cases()
    yield from tag_cases()
    yield from container_instance_cases()
    yield from session_cases()
    yield from console_cases()
    yield from parameter_cases()
    yield from pass_role_cases()
    yield from job_project_cases()
    yield from project_parameter_grid_cases()
    yield from aws_polaris_pair_cases()
    yield from openmetadata_password_cases()
    yield from launch_hold_cases()
    yield from alert_subscription_cases()
    yield from forbidden_cases()
    yield from snapshot_cases()
    yield from execution_cases()


def approved_actions(operations: dict[str, list[Request]]) -> dict[str, set[str]]:
    """The approved action set per service: the actions of the approved requests plus the S3 bucket and object actions."""
    found = {service: {item.action for item in requests} for service, requests in operations.items()}
    found["s3"] = {item.action for item in bucket_requests()} | {"s3:DeleteObject"}
    found["sns"] = {ALERT_WILDCARD_ACTION}
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
    """The policy documents each principal holds: the project user, the snapshot role, the task execution role,
    every role of the IAM stack and the wide test role."""
    user = [document for name, document in documents.items() if name.startswith("healthcare_hai_prediction_")]
    roles = {name: [document for key, document in documents.items() if key.startswith(f"role/{name}/")] for name in ROLES}
    # A delivery role holds its inline policy and every policy attached to it. None is attached today; ``check_delivery_roles`` fails when one is.
    delivery = {f"delivery_{name}": [document for key, document in documents.items() if key.startswith(f"delivery/{name}/")] for name in DELIVERY_ROLES}
    unbounded = {name: [document for key, document in documents.items() if key.startswith(f"role/{name}/")] for name in UNBOUNDED_ROLES}
    return {
        **roles,
        **unbounded,
        "user": user,
        "snapshots": [documents["owner_snapshots"]],
        "execution": [documents["owner_task_execution"]],
        "task_execution": [documents["owner_task_execution"]],
        SERVING_EXECUTION: [documents["owner_serving_execution"]],
        WIDE_ROLE: [WIDE_ROLE_POLICY],
        **delivery,
        DELIVERY_WIDE: [DELIVERY_WIDE_POLICY],
        DEPLOY_WIDE: [DELIVERY_WIDE_POLICY],
        CODE_STAGING_WIDE: [DELIVERY_WIDE_POLICY],
    }


DELIVERY_BOUNDARIES = {"deploy": DEPLOY_BOUNDARY, "code_staging": CODE_STAGING_BOUNDARY}


def boundaries(documents: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """The permissions boundary of each bounded principal: every role, never the project user. The delivery roles hold the delivery boundary."""
    return {
        **{name: [documents[BOUNDARY]] for name in ("snapshots", "execution", *ROLES, WIDE_ROLE)},
        **{f"delivery_{name}": [documents[DELIVERY_BOUNDARIES.get(name, DELIVERY_BOUNDARY)]] for name in DELIVERY_ROLES},
        DELIVERY_WIDE: [documents[DELIVERY_BOUNDARY]],
        DEPLOY_WIDE: [documents[DEPLOY_BOUNDARY]],
        CODE_STAGING_WIDE: [documents[CODE_STAGING_BOUNDARY]],
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
        drop(documents, "ecs", "DenyNoProp")
        del by_sid(documents, "ecs", "MakeService")["Condition"]["StringEquals"]["ecs:propagate-tags"]

    def remove_run_task_from_deny(documents: dict[str, dict[str, Any]], sid: str) -> None:
        statement = by_sid(documents, "ecs", sid)
        statement["Action"] = [action for action in as_list(statement["Action"]) if action != "ecs:RunTask"]

    def task_without_project_tag(documents: dict[str, dict[str, Any]]) -> None:
        for sid in ("DenyNoTag", "DenyBadKey"):
            remove_run_task_from_deny(documents, sid)
        condition = by_sid(documents, "ecs", "RunTask")["Condition"]
        del condition["StringEquals"]
        del condition["ForAnyValue:StringEquals"]

    def task_without_exact_project_key(documents: dict[str, dict[str, Any]]) -> None:
        remove_run_task_from_deny(documents, "DenyBadKey")
        del by_sid(documents, "ecs", "RunTask")["Condition"]["ForAnyValue:StringEquals"]

    def task_requires_unsupported_propagation_key(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "RunTask")["Condition"]["StringEquals"]["ecs:propagate-tags"] = "TASK_DEFINITION"

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
        del by_sid(documents, "ecs", "RunTask")["Condition"]["ArnEquals"]

    def task_from_any_definition(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "RunTask")["Resource"] = "*"

    def service_recovery_refused(documents: dict[str, dict[str, Any]]) -> None:
        statement = by_sid(documents, "ecs", "Services")
        statement["Action"] = [action for action in statement["Action"] if action != "ecs:UpdateService"]

    def service_recovery_in_any_project(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "Services")["Resource"] = f"arn:aws:ecs:{R}:{A}:service/*"

    def update_turns_propagation_off(documents: dict[str, dict[str, Any]]) -> None:
        drop(documents, "ecs", "DenyStopProp")

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

    def alert_resources(documents: dict[str, dict[str, Any]], resources: Any) -> None:
        by_sid(documents, "sns", ALERT_STATEMENT)["Resource"] = resources

    topic_pattern = f"arn:aws:sns:{R}:{A}:{P}_*alerts_{E}"

    def topic_wildcard(documents: dict[str, dict[str, Any]]) -> None:
        alert_resources(documents, [f"arn:aws:sns:{R}:{A}:{P}_*", f"arn:aws:sns:{R}:{A}:{P}_*:*"])

    def alerts_on_every_topic(documents: dict[str, dict[str, Any]]) -> None:
        alert_resources(documents, "*")

    def alerts_match_topic_prefix(documents: dict[str, dict[str, Any]]) -> None:
        alert_resources(documents, [f"{topic_pattern}*", f"{topic_pattern}*:*"])

    def alerts_match_any_infix(documents: dict[str, dict[str, Any]]) -> None:
        alert_resources(documents, [f"arn:aws:sns:{R}:{A}:*alerts_{E}", f"arn:aws:sns:{R}:{A}:*alerts_{E}:*"])

    def alerts_match_project_infix(documents: dict[str, dict[str, Any]]) -> None:
        alert_resources(documents, [topic_pattern, f"{topic_pattern}:*"])

    def alerts_ignore_environment(documents: dict[str, dict[str, Any]]) -> None:
        alert_resources(documents, [f"arn:aws:sns:{R}:{A}:{P}_*alerts_*", f"arn:aws:sns:{R}:{A}:{P}_*alerts_*:*"])

    def alerts_without_subscriptions(documents: dict[str, dict[str, Any]]) -> None:
        alert_resources(documents, [TOPIC, BUDGET_TOPIC])

    def alerts_removed(documents: dict[str, dict[str, Any]]) -> None:
        drop(documents, "sns", ALERT_STATEMENT)

    def alerts_only_platform_topic(documents: dict[str, dict[str, Any]]) -> None:
        alert_resources(documents, [TOPIC, f"{TOPIC}:*"])

    def alerts_narrowed_actions(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "sns", ALERT_STATEMENT)["Action"] = ["sns:Subscribe", "sns:GetSubscriptionAttributes", "sns:Unsubscribe"]

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

    def logging_any_bucket(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "s3", "SetAccessLoggingOnApprovedBucket")["Resource"] = "arn:aws:s3:::*"

    def logging_on_logging_bucket(documents: dict[str, dict[str, Any]]) -> None:
        statement = by_sid(documents, "s3", "SetAccessLoggingOnApprovedBucket")
        statement["Resource"] = [statement["Resource"], f"arn:aws:s3:::{B}-logs"]

    def logging_bucket_objects(documents: dict[str, dict[str, Any]]) -> None:
        documents["healthcare_hai_prediction_s3_policy"]["Statement"].append(
            {"Sid": "ReadLogs", "Effect": "Allow", "Action": ["s3:GetObject"], "Resource": f"arn:aws:s3:::{B}-logs/*"}
        )

    def logging_bucket_any_region(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "s3", "CreateApprovedBucketInApprovedRegion")["Condition"]

    def logging_bucket_delete(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "s3", "ManageLogBucketSecurity")["Action"].append("s3:DeleteBucket")

    def logging_bucket_versioning(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "s3", "ManageLogBucketSecurity")["Action"].append("s3:PutBucketVersioning")

    def logging_bucket_wildcard(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "s3", "ManageLogBucketSecurity")["Resource"] = f"arn:aws:s3:::{B}-*"

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

    def athena_direct_bronze_glue(documents: dict[str, dict[str, Any]]) -> None:
        documents["healthcare_hai_prediction_sts_policy"]["Statement"].append(
            {
                "Sid": "PlantedDirectBronzeGlue",
                "Effect": "Allow",
                "Action": ["glue:GetDatabase", "glue:GetTable", "glue:GetTables"],
                "Resource": [arn("glue", "database/hai_mirror_bronze"), arn("glue", "table/hai_mirror_bronze/*")],
            }
        )

    def promote_create_unconditional(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "s3", "CreatePromotionRecords")["Condition"]

    def promote_read_whole_bucket(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "s3", "ReadPromotionRecords")["Resource"] = f"arn:aws:s3:::{B}/*"

    def promote_records_deleted(documents: dict[str, dict[str, Any]]) -> None:
        statement = by_sid(documents, "s3", "CreatePromotionRecords")
        statement["Action"] = [*as_list(statement["Action"]), "s3:DeleteObject"]

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
        add_action(documents, "ecs", "Services", "ecs:DeregisterTaskDefinition")
        by_sid(documents, "ecs", "Services")["Resource"] = "*"

    def ecs_other_project(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "Cluster")["Resource"] = f"arn:aws:ecs:{R}:{A}:cluster/*"

    def secrets_create(documents: dict[str, dict[str, Any]]) -> None:
        add_action(documents, "secretsmanager", "ReadSharedApiKeys", "secretsmanager:CreateSecret")

    def region_condition_dropped(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "ecr", "GetRegistryLoginToken")["Condition"]

    def snapshot_lowercase_key(documents: dict[str, dict[str, Any]]) -> None:
        drop_owner(documents, "snapshots", "DenySnapshotsWithoutExactProjectKey")
        del owner_statement(documents, "snapshots", "CreateTaggedSnapshots")["Condition"]["ForAnyValue:StringEquals"]

    def snapshot_denies_on_volume(documents: dict[str, dict[str, Any]]) -> None:
        for sid in ("DenySnapshotsWithoutProjectTagValue", "DenySnapshotsWithoutExactProjectKey"):
            owner_statement(documents, "snapshots", sid)["Resource"] = "*"

    def classification_tag_any_volume(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "ec2", "TagDataVolumeClassification")["Condition"]["StringEquals"]["aws:ResourceTag/Name"]

    def classification_tag_any_key(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "ec2", "TagDataVolumeClassification")["Condition"]["ForAllValues:StringEquals"]
        condition = by_sid(documents, "ec2", "DenyTaggingAfterCreate")["Condition"]
        condition["ForAllValues:StringNotEquals"] = condition.pop("ForAnyValue:StringNotEquals")

    def classification_tag_any_value(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "ec2", "TagDataVolumeClassification")["Condition"]["StringEquals"]["aws:RequestTag/DataClassification"]

    def snapshot_delete_any(documents: dict[str, dict[str, Any]]) -> None:
        del owner_statement(documents, "snapshots", "DeleteScheduledSnapshots")["Condition"]["StringEquals"]["aws:ResourceTag/aws:dlm:lifecycle-schedule-name"]

    def execution_wildcard(documents: dict[str, dict[str, Any]]) -> None:
        owner_statement(documents, "task_execution", "InjectNamedParameters")["Resource"] = f"arn:aws:ssm:{R}:{A}:parameter/{PARAMETER_FOLDER}/*"

    def container_instance_on_cluster(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "Instances")["Resource"] = f"arn:aws:ecs:{R}:{A}:cluster/{P}_cluster_{E}"

    def container_instance_in_any_cluster(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "Instances")["Resource"] = f"arn:aws:ecs:{R}:{A}:container-instance/*"

    def another_project_tag_on_run_task_only(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "DenyOtherTag")["Action"] = ["ecs:CreateService", "ecs:TagResource"]
        remove_run_task_from_deny(documents, "DenyNoTag")
        del by_sid(documents, "ecs", "RunTask")["Condition"]["StringEquals"]

    def another_project_tag_allowed(documents: dict[str, dict[str, Any]]) -> None:
        another_project_tag_on_run_task_only(documents)
        drop(documents, "ecs", "DenyOtherTag")

    def another_project_tag_on_tagging_only(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "DenyOtherTag")["Action"] = ["ecs:RunTask", "ecs:CreateService"]

    def own_project_tag_refused(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ecs", "DenyOtherTag")["Condition"] = {"Null": {"aws:RequestTag/Project": "false"}}

    def job_resource(service: str, sid: str, index: int, resource: str) -> Mutation:
        def mutate(documents: dict[str, dict[str, Any]]) -> None:
            statement = by_sid(documents, service, sid)
            resources = list(as_list(statement["Resource"]))
            resources[index] = resource
            statement["Resource"] = resources

        return mutate

    def job_pass_without_service_condition(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "iam", "PassReadJobRoles")["Condition"]

    def airflow_launch_pass_without_service_condition(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "iam", "PassAirflowLaunchRole")["Condition"]

    def publisher_pass_without_service_condition(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "iam", "PassPublisherTaskRole")["Condition"]

    def serving_pass_without_service_condition(documents: dict[str, dict[str, Any]]) -> None:
        del by_sid(documents, "iam", "PassServingRoles")["Condition"]

    def serving_pass_without_streamlit_role(documents: dict[str, dict[str, Any]]) -> None:
        statement = by_sid(documents, "iam", "PassServingRoles")
        statement["Resource"] = [item for item in as_list(statement["Resource"]) if item != role("streamlit_task")]

    def training_pass_without_code_fetch_role(documents: dict[str, dict[str, Any]]) -> None:
        statement = by_sid(documents, "iam", "PassTrainingRoles")
        statement["Resource"] = [item for item in as_list(statement["Resource"]) if item != role("host_code_fetch_task")]

    def training_pass_code_fetch_role_widened(documents: dict[str, dict[str, Any]]) -> None:
        statement = by_sid(documents, "iam", "PassTrainingRoles")
        statement["Resource"] = [role("host_code_fetch_task*") if item == role("host_code_fetch_task") else item for item in as_list(statement["Resource"])]

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
        }
    )

    def om_statement_action(action: str) -> Mutation:
        def mutate(documents: dict[str, dict[str, Any]]) -> None:
            add_action(documents, "ssm", OPENMETADATA_STATEMENT, action)

        return mutate

    def om_statement_dropped(documents: dict[str, dict[str, Any]]) -> None:
        drop(documents, "ssm", OPENMETADATA_STATEMENT)

    def om_neighbor_added(documents: dict[str, dict[str, Any]]) -> None:
        statement = by_sid(documents, "ssm", OPENMETADATA_STATEMENT)
        statement["Resource"] = [*as_list(statement["Resource"]), parameter("openmetadata_bot_lineage")]

    def om_key_use_granted(documents: dict[str, dict[str, Any]]) -> None:
        documents["healthcare_hai_prediction_ssm_policy"]["Statement"].append({"Sid": "UseKey", "Effect": "Allow", "Action": "kms:Decrypt", "Resource": "*"})

    def om_deny_dropped(documents: dict[str, dict[str, Any]]) -> None:
        drop(documents, "ssm", OPENMETADATA_DENY)

    def om_deny_inverted(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ssm", OPENMETADATA_DENY)["Condition"] = {"StringEqualsIgnoreCase": {"ssm:Overwrite": "false"}}

    def om_deny_case_sensitive(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ssm", OPENMETADATA_DENY)["Condition"] = {"StringEquals": {"ssm:Overwrite": "true"}}

    def om_deny_one_name(documents: dict[str, dict[str, Any]]) -> None:
        statement = by_sid(documents, "ssm", OPENMETADATA_DENY)
        statement["Resource"] = as_list(statement["Resource"])[:1]

    def om_deny_everywhere(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ssm", OPENMETADATA_DENY)["Resource"] = f"arn:aws:ssm:{R}:{A}:parameter/{PARAMETER_FOLDER}/*"

    def om_deny_other_action(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ssm", OPENMETADATA_DENY)["Action"] = ["ssm:DeleteParameter"]

    def om_deny_as_allow(documents: dict[str, dict[str, Any]]) -> None:
        by_sid(documents, "ssm", OPENMETADATA_DENY)["Effect"] = "Allow"

    openmetadata_defects: dict[str, tuple[str, Mutation]] = {
        f"openmetadata password resource {name} widened": (
            f"openmetadata passwords: refuse PutParameter {name}_extra",
            job_resource("ssm", OPENMETADATA_STATEMENT, index, parameter(name) + "*"),
        )
        for index, name in enumerate(OPENMETADATA_PASSWORDS)
    }
    openmetadata_defects.update(
        {
            "openmetadata password overwrite deny dropped": (
                f"openmetadata passwords: refuse overwrite of {OPENMETADATA_PASSWORDS[0]} with Overwrite=true",
                om_deny_dropped,
            ),
            "openmetadata password overwrite deny turned around": (
                f"openmetadata passwords: refuse overwrite of {OPENMETADATA_PASSWORDS[1]} with Overwrite=true",
                om_deny_inverted,
            ),
            "openmetadata password overwrite deny matches the value by case": (
                f"openmetadata passwords: refuse overwrite of {OPENMETADATA_PASSWORDS[0]} with Overwrite=True",
                om_deny_case_sensitive,
            ),
            "openmetadata password overwrite deny covers one name": (
                f"openmetadata passwords: refuse overwrite of {OPENMETADATA_PASSWORDS[1]} with Overwrite=true",
                om_deny_one_name,
            ),
            "openmetadata password overwrite deny covers the whole folder": (
                "openmetadata passwords: overwrite stays allowed for a managed name",
                om_deny_everywhere,
            ),
            "openmetadata password overwrite deny names another action": (
                f"openmetadata passwords: refuse overwrite of {OPENMETADATA_PASSWORDS[0]} with Overwrite=true",
                om_deny_other_action,
            ),
            "openmetadata password overwrite deny is an allow": (
                f"openmetadata passwords: refuse overwrite of {OPENMETADATA_PASSWORDS[0]} with Overwrite=true",
                om_deny_as_allow,
            ),
            "openmetadata password statement dropped": (f"openmetadata passwords: PutParameter {OPENMETADATA_PASSWORDS[1]}", om_statement_dropped),
            "openmetadata password delete action added": (
                f"openmetadata passwords: refuse DeleteParameter {OPENMETADATA_PASSWORDS[0]}",
                om_statement_action("ssm:DeleteParameter"),
            ),
            "openmetadata password batch read added": (
                f"openmetadata passwords: refuse GetParameters {OPENMETADATA_PASSWORDS[0]}",
                om_statement_action("ssm:GetParameters"),
            ),
            "openmetadata password history read added": (
                f"openmetadata passwords: refuse GetParameterHistory {OPENMETADATA_PASSWORDS[1]}",
                om_statement_action("ssm:GetParameterHistory"),
            ),
            "openmetadata password tagging added": (
                f"openmetadata passwords: refuse AddTagsToResource {OPENMETADATA_PASSWORDS[0]}",
                om_statement_action("ssm:AddTagsToResource"),
            ),
            "openmetadata bot token name added to the grant": ("openmetadata passwords: refuse PutParameter openmetadata_bot_lineage", om_neighbor_added),
            "openmetadata password key use granted on every key": ("openmetadata passwords: refuse kms:Decrypt", om_key_use_granted),
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
            "job stage version resource widened": (
                "job stage: refuse lakehouse/silver",
                job_resource("s3", "ReadCatalogMetadataAndStageVersions", 1, f"{BUCKET}/lakehouse/*"),
            ),
            "training PassRole resource widened": (
                "training pass: refuse job_train_task_extra",
                job_resource("iam", "PassTrainingRoles", 0, role("job_train_task*")),
            ),
            "training PassRole lacks the code one-shot task role": ("training pass: host_code_fetch_task to ECS tasks", training_pass_without_code_fetch_role),
            "code one-shot PassRole resource widened": (
                "training pass: refuse host_code_fetch_task_extra",
                training_pass_code_fetch_role_widened,
            ),
            "airflow launch PassRole resource widened": (
                "airflow launch pass: refuse airflow_launch_task_extra",
                job_resource("iam", "PassAirflowLaunchRole", 0, role("airflow_launch_task*")),
            ),
            "airflow launch PassRole service condition removed": (
                "airflow launch pass: airflow_launch_task without a service",
                airflow_launch_pass_without_service_condition,
            ),
            "publisher PassRole resource widened": (
                "publisher pass: refuse job_publisher_task_extra",
                job_resource("iam", "PassPublisherTaskRole", 0, role("job_publisher_task*")),
            ),
            "publisher PassRole service condition removed": (
                "publisher pass: job_publisher_task without a service",
                publisher_pass_without_service_condition,
            ),
            "serving PassRole resource widened": (
                "serving pass: refuse serving_execution_extra",
                job_resource("iam", "PassServingRoles", 0, role("serving_execution*")),
            ),
            "serving PassRole lacks the dashboard task role": ("serving pass: streamlit_task to ECS tasks", serving_pass_without_streamlit_role),
            "serving PassRole service condition removed": ("serving pass: serving_execution without a service", serving_pass_without_service_condition),
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

    def boundary_statement(documents: dict[str, dict[str, Any]], sid: str) -> dict[str, Any]:
        return next(statement for statement in documents[BOUNDARY]["Statement"] if statement["Sid"] == sid)

    def boundary_shared_prefix(documents: dict[str, dict[str, Any]]) -> None:
        boundary_statement(documents, "DenyAssumeRoleOutsideProject")["NotResource"] = f"arn:aws:iam::{A}:role/healthcare_*"

    def boundary_reads_every_bucket(documents: dict[str, dict[str, Any]]) -> None:
        documents[BOUNDARY]["Statement"].append({"Sid": "ReadEverything", "Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"})

    def boundary_without_host_action(documents: dict[str, dict[str, Any]]) -> None:
        statement = boundary_statement(documents, "JoinProjectCluster")
        statement["Action"] = [action for action in statement["Action"] if action != "ecs:RegisterContainerInstance"]

    def boundary_without_iam_deny(documents: dict[str, dict[str, Any]]) -> None:
        documents[BOUNDARY]["Statement"] = [
            statement for statement in documents[BOUNDARY]["Statement"] if statement["Sid"] != "DenyIamOrganizationsAccountAndBudgets"
        ]

    def boundary_only_project_tag(sid: str) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            boundary_statement(documents, sid)["Condition"] = {"StringEquals": {"aws:ResourceTag/Project": P}}

        return mutation

    def boundary_writes_every_object(documents: dict[str, dict[str, Any]]) -> None:
        boundary_statement(documents, "ProjectLakehouseObjects")["Resource"] = f"{BUCKET}/*"

    def delivery_statement(documents: dict[str, dict[str, Any]], sid: str, boundary: str = DELIVERY_BOUNDARY) -> dict[str, Any]:
        return next(statement for statement in documents[boundary]["Statement"] if statement["Sid"] == sid)

    def delivery_without_action(sid: str, action: str, boundary: str = DELIVERY_BOUNDARY) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            statement = delivery_statement(documents, sid, boundary)
            statement["Action"] = [item for item in as_list(statement["Action"]) if item != action]

        return mutation

    def delivery_resource(sid: str, resource: str | list[str], boundary: str = DELIVERY_BOUNDARY) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            delivery_statement(documents, sid, boundary)["Resource"] = resource

        return mutation

    def delivery_extra_statement(
        sid: str, action: str, resource: str | list[str], condition: dict[str, Any] | None = None, boundary: str = DELIVERY_BOUNDARY
    ) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            statement: dict[str, Any] = {"Sid": sid, "Effect": "Allow", "Action": action, "Resource": resource}
            if condition:
                statement["Condition"] = condition
            documents[boundary]["Statement"].insert(0, statement)

        return mutation

    def deploy_policy_extra_statement(sid: str, action: str, resource: str | list[str]) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            documents[f"delivery/{DELIVERY_ROLES[1]}/access"]["Statement"].append({"Sid": sid, "Effect": "Allow", "Action": action, "Resource": resource})
            documents[DEPLOY_BOUNDARY]["Statement"].insert(0, {"Sid": sid, "Effect": "Allow", "Action": action, "Resource": resource})

        return mutation

    def delivery_add_resource(sid: str, resource: str, boundary: str = DELIVERY_BOUNDARY) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            statement = delivery_statement(documents, sid, boundary)
            statement["Resource"] = [*as_list(statement["Resource"]), resource]

        return mutation

    def delivery_remove_resource(sid: str, fragment: str, boundary: str = DELIVERY_BOUNDARY) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            statement = delivery_statement(documents, sid, boundary)
            statement["Resource"] = [resource for resource in as_list(statement["Resource"]) if fragment not in resource]

        return mutation

    def delivery_pass_roles_without_iam_deny(documents: dict[str, dict[str, Any]]) -> None:
        delivery_without_iam_deny(documents)
        shared = [f"arn:aws:iam::{A}:role/{P}_task_execution_{E}"]
        delivery_extra_statement("PassAgain", "iam:PassRole", shared, {"StringEquals": {"iam:PassedToService": ECS_TASKS}}, DEPLOY_BOUNDARY)(documents)

    def delivery_without_iam_deny(documents: dict[str, dict[str, Any]]) -> None:
        documents[DEPLOY_BOUNDARY]["Statement"] = [statement for statement in documents[DEPLOY_BOUNDARY]["Statement"] if statement["Sid"] != "DenyIam"]

    def deploy_statement(documents: dict[str, dict[str, Any]], sid: str) -> dict[str, Any]:
        return next(statement for statement in documents[f"delivery/{DELIVERY_ROLES[1]}/access"]["Statement"] if statement["Sid"] == sid)

    def deploy_resource(sid: str, resource: str | list[str]) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            deploy_statement(documents, sid)["Resource"] = resource

        return mutation

    def deploy_drops(*sids: str) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            policy = documents[f"delivery/{DELIVERY_ROLES[1]}/access"]
            policy["Statement"] = [statement for statement in policy["Statement"] if statement["Sid"] not in sids]

        return mutation

    def deploy_without_condition(sid: str) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            del deploy_statement(documents, sid)["Condition"]

        return mutation

    def boundary_iam_deny_without_condition(documents: dict[str, dict[str, Any]]) -> None:
        statement = delivery_statement(documents, "DenyIam", DEPLOY_BOUNDARY)
        statement.pop("Condition", None)

    def role_and_boundary(sid: str, resource: str | list[str]) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            deploy_resource(sid, resource)(documents)
            delivery_resource(sid, resource, DEPLOY_BOUNDARY)(documents)

        return mutation

    def pass_condition_gone(documents: dict[str, dict[str, Any]]) -> None:
        deploy_without_condition("PassServingRoles")(documents)
        boundary_iam_deny_without_condition(documents)
        delivery_statement(documents, "PassServingRoles", DEPLOY_BOUNDARY).pop("Condition", None)

    def boundary_condition(sid: str, change: Callable[[dict[str, Any]], None]) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            change(delivery_statement(documents, sid, DEPLOY_BOUNDARY))

        return mutation

    def condition_gone(statement: dict[str, Any]) -> None:
        del statement["Condition"]

    def project_tag_any_value(statement: dict[str, Any]) -> None:
        statement["Condition"] = {"StringLike": {"aws:RequestTag/Project": "*"}, "ForAnyValue:StringEquals": statement["Condition"]["ForAnyValue:StringEquals"]}

    def tag_keys_unchecked(statement: dict[str, Any]) -> None:
        del statement["Condition"]["ForAnyValue:StringEquals"]

    def any_create_action(statement: dict[str, Any]) -> None:
        statement["Condition"] = {"StringLike": {"ecs:CreateAction": "*"}}

    def one_unconditional_statement(documents: dict[str, dict[str, Any]]) -> None:
        statements = documents[DEPLOY_BOUNDARY]["Statement"]
        documents[DEPLOY_BOUNDARY]["Statement"] = [item for item in statements if item["Sid"] != "TagRegisteredTaskDefinitions"]
        joined = delivery_statement(documents, "RegisterServingTaskDefinitions", DEPLOY_BOUNDARY)
        joined["Action"] = ["ecs:RegisterTaskDefinition", "ecs:TagResource"]
        del joined["Condition"]

    def deregister_too(documents: dict[str, dict[str, Any]]) -> None:
        families = [arn("ecs", f"task-definition/{P}_{E}_{name}:*") for name in SERVING_SERVICES]
        deploy_policy_extra_statement("DeregisterServingTaskDefinitions", "ecs:DeregisterTaskDefinition", families)(documents)

    serving_families = [arn("ecs", f"task-definition/{P}_{E}_*:*")]
    pass_wildcard = [f"arn:aws:iam::{A}:role/{P}_*_task_{E}", SERVING_ROLES[2]]
    serving_release_defects: dict[str, tuple[str, Mutation]] = {
        "serving release: the deploy role cannot register": ("delivery allowed: serving release: registers", deploy_drops("RegisterServingTaskDefinitions")),
        "serving release: the boundary cannot register": (
            "delivery allowed: serving release: registers",
            delivery_without_action("RegisterServingTaskDefinitions", "ecs:RegisterTaskDefinition", DEPLOY_BOUNDARY),
        ),
        "serving release: the deploy role registers in every family of the project": (
            "delivery refused: crafted task definition chain: registers the family airflow",
            deploy_resource("RegisterServingTaskDefinitions", serving_families),
        ),
        "serving release: the boundary allows every family of the project": (
            "delivery refused: crafted task definition chain: registers the family airflow",
            delivery_resource("RegisterServingTaskDefinitions", serving_families, DEPLOY_BOUNDARY),
        ),
        "serving release: the deploy role registers a family that only starts like a serving one": (
            "delivery refused: crafted task definition chain: registers the family model_api_extra",
            deploy_resource(
                "RegisterServingTaskDefinitions", [arn("ecs", f"task-definition/{P}_{E}_model_api*"), arn("ecs", f"task-definition/{P}_{E}_streamlit*")]
            ),
        ),
        "serving release: registration without the Project tag condition": (
            "delivery refused: crafted task definition chain: registers a serving family without the Project tag",
            deploy_without_condition("RegisterServingTaskDefinitions"),
        ),
        "serving release: tagging without the registration condition": (
            "delivery refused: crafted task definition chain: re-tags a serving revision",
            deploy_without_condition("TagRegisteredTaskDefinitions"),
        ),
        "serving release: the boundary registers without the Project tag condition": (
            "delivery refused: crafted task definition chain: registers a serving family without the Project tag",
            boundary_condition("RegisterServingTaskDefinitions", condition_gone),
        ),
        "serving release: the boundary accepts any Project tag value": (
            "delivery refused: crafted task definition chain: registers a serving family with another project's tag",
            boundary_condition("RegisterServingTaskDefinitions", project_tag_any_value),
        ),
        "serving release: the boundary accepts a lowercase project key": (
            "delivery refused: crafted task definition chain: registers a serving family with a lowercase project key",
            boundary_condition("RegisterServingTaskDefinitions", tag_keys_unchecked),
        ),
        "serving release: the boundary tags without the registration condition": (
            "delivery refused: crafted task definition chain: re-tags a serving revision",
            boundary_condition("TagRegisteredTaskDefinitions", condition_gone),
        ),
        "serving release: the boundary tags for any create action": (
            "delivery refused: crafted task definition chain: tags a serving revision for another create action",
            boundary_condition("TagRegisteredTaskDefinitions", any_create_action),
        ),
        "serving release: the boundary tags every family": (
            "delivery refused: crafted task definition chain: tags another family while registering",
            delivery_resource("TagRegisteredTaskDefinitions", serving_families, DEPLOY_BOUNDARY),
        ),
        "serving release: the boundary joins registration and tagging in one unconditional statement": (
            "delivery refused: crafted task definition chain: re-tags a serving revision",
            one_unconditional_statement,
        ),
        "serving release: the deploy role tags every family": (
            "delivery refused: crafted task definition chain: tags another family while registering",
            deploy_resource("TagRegisteredTaskDefinitions", serving_families),
        ),
        "serving release: the deploy role may also deregister": (
            "delivery refused: crafted task definition chain: ecs:DeregisterTaskDefinition",
            deregister_too,
        ),
        "serving release: the deploy role passes the shared task execution role too": (
            "delivery refused: crafted task definition chain: passes task_execution to ECS tasks",
            role_and_boundary("PassServingRoles", [*SERVING_ROLES, f"arn:aws:iam::{A}:role/{P}_task_execution_{E}"]),
        ),
        "serving release: the deploy role passes every task role of the project": (
            "delivery refused: crafted task definition chain: passes polaris_task to ECS tasks",
            role_and_boundary("PassServingRoles", pass_wildcard),
        ),
        "serving release: the serving execution role matches every environment": (
            "delivery refused: crafted task definition chain: passes the serving execution role of another environment",
            role_and_boundary("PassServingRoles", [SERVING_ROLES[0], SERVING_ROLES[1], f"arn:aws:iam::{A}:role/{P}_serving_execution_*"]),
        ),
        "serving release: PassRole may go to any service": (
            "delivery refused: crafted task definition chain: passes example_project_model_api_task_dev to a service other than ECS tasks",
            pass_condition_gone,
        ),
        "serving release: the boundary cannot pass the serving execution role": (
            "delivery allowed: serving release: passes example_project_serving_execution_dev to ECS tasks",
            delivery_remove_resource("PassServingRoles", "serving_execution", DEPLOY_BOUNDARY),
        ),
        "serving release: the IAM deny of the boundary refuses the serving roles": (
            "delivery allowed: serving release: passes example_project_model_api_task_dev to ECS tasks",
            boundary_iam_deny_without_condition,
        ),
    }

    state = f"{BUCKET}/{STATE_PREFIX}"
    delivery_defects: dict[str, tuple[str, Mutation]] = {
        "delivery defect: image push reaches the repositories of every environment": (
            "delivery refused: push to a repository of another environment",
            delivery_resource("PushAndReadProjectImages", arn("ecr", f"repository/{P}_*")),
        ),
        "delivery defect: the boundary lacks ecr:PutImage": (
            "delivery allowed: image_push access PushProjectImages ecr:PutImage",
            delivery_without_action("PushAndReadProjectImages", "ecr:PutImage"),
        ),
        "delivery defect: UpdateService reaches every service of the cluster": (
            "delivery refused: ecs:UpdateService on a service outside the project service names",
            delivery_resource("UpdateProjectServices", arn("ecs", f"service/{P}_cluster_{E}/*"), DEPLOY_BOUNDARY),
        ),
        "delivery defect: UpdateService reaches every service the project names": (
            "delivery refused: crafted task definition chain: updates a service that is not served",
            delivery_resource("UpdateProjectServices", arn("ecs", f"service/{P}_cluster_{E}/{P}_{E}_*"), DEPLOY_BOUNDARY),
        ),
        "delivery defect: the boundary allows task definition registration": (
            "delivery refused: crafted task definition chain: registers the family airflow",
            delivery_extra_statement("RegisterAgain", "ecs:RegisterTaskDefinition", "*", None, DEPLOY_BOUNDARY),
        ),
        "delivery defect: the deploy role's own policy registers task definitions": (
            "delivery refused: crafted task definition chain: registers the family airflow",
            deploy_policy_extra_statement("RegisterAgain", "ecs:RegisterTaskDefinition", "*"),
        ),
        "delivery defect: the boundary passes the shared task execution role and the IAM deny is missing": (
            "delivery refused: crafted task definition chain: passes task_execution to ECS tasks",
            delivery_pass_roles_without_iam_deny,
        ),
        "delivery defect: state access covers the whole bucket": (
            "delivery refused: reads a lakehouse object",
            delivery_resource("ReadProjectTerraformState", f"{BUCKET}/*"),
        ),
        "delivery defect: state access covers every stack": (
            "delivery refused: reads the IAM stack state",
            delivery_resource("ReadProjectTerraformState", f"{state}/*"),
        ),
        "delivery defect: lock release deletes state objects": (
            "delivery refused: deletes a state object",
            delivery_extra_statement("DeleteStateAgain", "s3:DeleteObject", f"{state}/*"),
        ),
        "delivery defect: the metadata reads reach every secret that ends in _api_key": (
            "delivery refused: describes the secret another_project_api_key",
            delivery_add_resource("ReadProjectMetadata", arn("secretsmanager", "secret:*_api_key-??????")),
        ),
        "delivery defect: the metadata reads reach a log group with any suffix": (
            "delivery refused: lists the tags of a devops log group of the environment",
            delivery_add_resource("ReadProjectMetadata", arn("logs", f"log-group:{P}_*_{E}*")),
        ),
        "delivery defect: state reads reach every key that starts with terraform.tfstate": (
            "delivery refused: reads a platform state key with the suffix .backup",
            delivery_add_resource("ReadProjectTerraformState", f"{STATE_KEYS[0]}*"),
        ),
        "delivery defect: the boundary lets a delivery role read the alert subscriptions": (
            "delivery refused: reads the attributes of a subscription of the alerts topic",
            delivery_extra_statement(
                "ReadAlertSubscriptions", "sns:GetSubscriptionAttributes", [f"arn:aws:sns:{R}:{A}:{P}_alerts_{E}", f"arn:aws:sns:{R}:{A}:{P}_alerts_{E}:*"]
            ),
        ),
        "delivery defect: the boundary lets a delivery role read the budget alert subscriptions": (
            "delivery refused: reads the attributes of a subscription of the budget alerts topic",
            delivery_extra_statement("ReadBudgetSubscriptions", "sns:GetSubscriptionAttributes", [f"{BUDGET_TOPIC}:*"]),
        ),
        "delivery defect: the boundary lets a delivery role read every alert subscription": (
            "delivery refused: reads the attributes of a subscription of the alerts topic",
            delivery_extra_statement("ReadAlertSubscriptions", "sns:GetSubscriptionAttributes", [f"arn:aws:sns:{R}:{A}:{P}_*alerts_{E}:*"]),
        ),
        "delivery defect: the boundary lets a delivery role subscribe": (
            "delivery refused: changes the alerts topic with sns:Subscribe",
            delivery_extra_statement("SubscribeAgain", "sns:Subscribe", TOPIC),
        ),
        "delivery defect: the boundary lacks a shared secret": (
            "delivery allowed: boundary describes the shared secret jev_api_key",
            delivery_remove_resource("ReadProjectMetadata", "jev_api_key"),
        ),
        "delivery defect: the boundary lacks a state key": (
            "delivery allowed: boundary reads the state key jobs",
            delivery_remove_resource("ReadProjectTerraformState", "/jobs/"),
        ),
        "delivery defect: the push and plan boundary has no IAM deny": (
            "delivery refused: iam:CreateRole",
            lambda documents: documents[DELIVERY_BOUNDARY].update(
                {"Statement": [item for item in documents[DELIVERY_BOUNDARY]["Statement"] if item["Sid"] != "DenyIam"]}
            ),
        ),
        "delivery defect: the deploy boundary has no IAM deny": (
            "delivery refused: iam:CreateRole",
            lambda documents: documents[DEPLOY_BOUNDARY].update(
                {"Statement": [item for item in documents[DEPLOY_BOUNDARY]["Statement"] if item["Sid"] != "DenyIam"]}
            ),
        ),
        "code staging defect: the boundary lets the role bind a run to a code run": (
            "delivery refused: code staging binds a run to a code run",
            delivery_resource("StageCodeFilesOnce", f"{BUCKET}/lakehouse/run_stage/*/inputs/*", CODE_STAGING_BOUNDARY),
        ),
        "code staging defect: the boundary lets the role put anywhere in the lakehouse": (
            "delivery refused: code staging puts a code file in another folder of the lakehouse",
            delivery_resource("StageCodeFilesOnce", f"{BUCKET}/lakehouse/*", CODE_STAGING_BOUNDARY),
        ),
        "code staging defect: the boundary drops the once condition": (
            "delivery refused: code staging puts a code file without the once condition",
            lambda documents: delivery_statement(documents, "StageCodeFilesOnce", CODE_STAGING_BOUNDARY).pop("Condition"),
        ),
        "code staging defect: the role policy drops the once condition": (
            "delivery refused: code staging puts a code file without the once condition (delivery_code_staging)",
            lambda documents: next(item for item in documents["delivery/code_staging/access"]["Statement"] if item["Sid"] == "StageCodeFilesOnce").pop(
                "Condition"
            ),
        ),
        "code staging defect: the boundary lets the role read a code file": (
            "delivery refused: code staging uses s3:GetObject on a code file",
            lambda documents: delivery_statement(documents, "StageCodeFilesOnce", CODE_STAGING_BOUNDARY).update({"Action": ["s3:PutObject", "s3:GetObject"]}),
        ),
        "code staging defect: the boundary lets the role put any project parameter": (
            "delivery refused: code staging puts another parameter of the project",
            delivery_resource("NameCurrentCodeRun", arn("ssm", f"parameter/{P}/*"), CODE_STAGING_BOUNDARY),
        ),
        "code staging defect: the boundary lets the role read the parameter": (
            "delivery refused: code staging uses ssm:GetParameter on the code run parameter",
            lambda documents: delivery_statement(documents, "NameCurrentCodeRun", CODE_STAGING_BOUNDARY).update(
                {"Action": ["ssm:PutParameter", "ssm:GetParameter"]}
            ),
        ),
        "code staging defect: the boundary lacks the code file put": (
            "delivery allowed: code staging creates code.tar.gz once",
            delivery_without_action("StageCodeFilesOnce", "s3:PutObject", CODE_STAGING_BOUNDARY),
        ),
        "code staging defect: the boundary lacks the parameter put": (
            "delivery allowed: code staging names the current code run",
            delivery_without_action("NameCurrentCodeRun", "ssm:PutParameter", CODE_STAGING_BOUNDARY),
        ),
        "delivery defect: the boundary lacks a plan read": (
            "delivery allowed: terraform_plan access Readec2DescribeProjectInfrastructure ec2:DescribeVpcs",
            delivery_without_action("RegionalReads", "ec2:DescribeVpcs"),
        ),
    }

    def plan_role_statement(documents: dict[str, dict[str, Any]], sid: str) -> dict[str, Any]:
        return next(statement for statement in documents[f"delivery/{DELIVERY_ROLES[2]}/access"]["Statement"] if statement["Sid"] == sid)

    def plan_role_drops(*sids: str) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            policy = documents[f"delivery/{DELIVERY_ROLES[2]}/access"]
            policy["Statement"] = [statement for statement in policy["Statement"] if statement["Sid"] not in sids]

        return mutation

    def plan_role_save_resource(resource: str, *, drop_write_deny: bool = True) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            plan_role_statement(documents, "SaveTerraformPlans")["Resource"] = [resource]
            if drop_write_deny:
                plan_role_drops("NeverWriteObjectsExceptSavedPlans")(documents)

        return mutation

    def plan_role_save_action(action: str) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            statement = plan_role_statement(documents, "SaveTerraformPlans")
            statement["Action"] = [*as_list(statement["Action"]), action]
            plan_role_drops("NeverWriteObjectsExceptSavedPlans")(documents)
            delivery_extra_statement("ExtraPlanAction", action, f"{BUCKET}/{PLAN_PREFIX}/*")(documents)

        return mutation

    def plan_role_reads_plans(documents: dict[str, dict[str, Any]]) -> None:
        documents[f"delivery/{DELIVERY_ROLES[2]}/access"]["Statement"].append(
            {"Sid": "ReadPlansAgain", "Effect": "Allow", "Action": ["s3:GetObject"], "Resource": [f"{BUCKET}/{PLAN_PREFIX}/*"]}
        )
        plan_role_statement(documents, "NeverReadObjectsExceptStateKeys")["NotResource"] = [*STATE_KEYS, f"{BUCKET}/{PLAN_PREFIX}/*"]
        delivery_extra_statement("ReadPlansAgain", "s3:GetObject", f"{BUCKET}/{PLAN_PREFIX}/*")(documents)

    def image_push_saves_plans(documents: dict[str, dict[str, Any]]) -> None:
        documents[f"delivery/{DELIVERY_ROLES[0]}/access"]["Statement"].append(
            {"Sid": "SaveTerraformPlans", "Effect": "Allow", "Action": ["s3:PutObject"], "Resource": [f"{BUCKET}/{PLAN_PREFIX}/*"]}
        )

    def plan_role_reads_subscriptions(resources: list[str]) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            documents[f"delivery/{DELIVERY_ROLES[2]}/access"]["Statement"].append(
                {"Sid": "ReadSubscriptionsAgain", "Effect": "Allow", "Action": ["sns:GetSubscriptionAttributes"], "Resource": resources}
            )
            delivery_extra_statement("ReadSubscriptionsAgain", "sns:GetSubscriptionAttributes", resources)(documents)

        return mutation

    plan_delivery_defects: dict[str, tuple[str, Mutation]] = {
        "saved plans: the plan role cannot save": ("delivery allowed: terraform_plan saves", plan_role_drops("SaveTerraformPlans")),
        "saved plans: the boundary lacks the plan save": (
            "delivery allowed: boundary lets a delivery role save",
            delivery_without_action("SaveTerraformPlans", "s3:PutObject"),
        ),
        "saved plans: the plan role writes the whole bucket": (
            "delivery refused: saved plans: terraform_plan puts a lakehouse object",
            plan_role_save_resource(f"{BUCKET}/*"),
        ),
        "saved plans: the plan role writes the state prefix": (
            "delivery refused: saved plans: terraform_plan puts a platform state key",
            plan_role_save_resource(f"{BUCKET}/{STATE_PREFIX}/*"),
        ),
        "saved plans: the plan role resource drops the slash of the prefix": (
            "delivery refused: saved plans: terraform_plan puts a prefix that only starts like the plans prefix",
            plan_role_save_resource(f"{BUCKET}/{PLAN_PREFIX}*"),
        ),
        "saved plans: the boundary allows every object": (
            "delivery refused: saved plans: boundary puts a lakehouse object",
            delivery_resource("SaveTerraformPlans", f"{BUCKET}/*"),
        ),
        "saved plans: the boundary resource drops the slash of the prefix": (
            "delivery refused: saved plans: boundary puts a prefix that only starts like the plans prefix",
            delivery_resource("SaveTerraformPlans", f"{BUCKET}/{PLAN_PREFIX}*"),
        ),
        "saved plans: the boundary allows the plans of every bucket": (
            "delivery refused: saved plans: boundary puts the plans prefix of another bucket",
            delivery_resource("SaveTerraformPlans", f"arn:aws:s3:::*/{PLAN_PREFIX}/*"),
        ),
        "alert subscriptions: the plan role reads a subscription of the alerts topic": (
            "delivery refused: terraform_plan reads the attributes of a subscription of the alerts topic",
            plan_role_reads_subscriptions([f"{TOPIC}:*"]),
        ),
        "alert subscriptions: the plan role reads a subscription of the budget alerts topic": (
            "delivery refused: terraform_plan reads the attributes of a subscription of the budget alerts topic",
            plan_role_reads_subscriptions([f"{BUDGET_TOPIC}:*"]),
        ),
        "alert subscriptions: the plan role reads every alert subscription": (
            "delivery refused: terraform_plan reads the attributes of a subscription of the alerts topic",
            plan_role_reads_subscriptions([f"arn:aws:sns:{R}:{A}:{P}_*alerts_{E}:*"]),
        ),
        "saved plans: the plan role reads saved plans": (
            "delivery refused: saved plans: terraform_plan uses s3:GetObject on a saved plan",
            plan_role_reads_plans,
        ),
        "saved plans: the plan role deletes saved plans": (
            "delivery refused: saved plans: terraform_plan uses s3:DeleteObject on a saved plan",
            plan_role_save_action("s3:DeleteObject"),
        ),
        "saved plans: the plan role replaces the ACL of a saved plan": (
            "delivery refused: saved plans: terraform_plan uses s3:PutObjectAcl on a saved plan",
            plan_role_save_action("s3:PutObjectAcl"),
        ),
        "saved plans: the image push role saves plans": ("delivery refused: saved plans: image_push puts a saved plan", image_push_saves_plans),
        "saved plans: the write deny is gone and the allow reaches the state prefix": (
            "delivery refused: saved plans: terraform_plan writes a state key",
            plan_role_save_resource(f"{BUCKET}/{STATE_PREFIX}/*"),
        ),
    }

    def serving_statement(documents: dict[str, dict[str, Any]], sid: str) -> dict[str, Any]:
        return next(statement for statement in documents["owner_serving_execution"]["Statement"] if statement["Sid"] == sid)

    def serving_adds_resource(sid: str, resource: str) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            statement = serving_statement(documents, sid)
            statement["Resource"] = [*as_list(statement["Resource"]), resource]

        return mutation

    def serving_sets_resource(sid: str, resource: str | list[str]) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            serving_statement(documents, sid)["Resource"] = resource

        return mutation

    def serving_adds_action(sid: str, action: str) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            statement = serving_statement(documents, sid)
            statement["Action"] = [*as_list(statement["Action"]), action]

        return mutation

    def serving_drops(*sids: str) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            policy = documents["owner_serving_execution"]
            policy["Statement"] = [statement for statement in policy["Statement"] if statement["Sid"] not in sids]

        return mutation

    def serving_without_resource(sid: str, fragment: str) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            statement = serving_statement(documents, sid)
            statement["Resource"] = [resource for resource in as_list(statement["Resource"]) if fragment not in resource]

        return mutation

    def serving_creates_groups(documents: dict[str, dict[str, Any]]) -> None:
        documents["owner_serving_execution"]["Statement"].append(
            {"Sid": "CreateGroups", "Effect": "Allow", "Action": "logs:CreateLogGroup", "Resource": arn("logs", f"log-group:{P}_*")}
        )

    def serving_reads_secrets(documents: dict[str, dict[str, Any]]) -> None:
        documents["owner_serving_execution"]["Statement"].append(
            {"Sid": "ReadSecrets", "Effect": "Allow", "Action": "secretsmanager:GetSecretValue", "Resource": "*"}
        )

    serving_execution_defects: dict[str, tuple[str, Mutation]] = {
        "serving execution: injects the Redis password": (
            "serving execution: cannot inject the control-plane parameter redis_password",
            serving_adds_resource("InjectServingParameters", parameter("redis_password")),
        ),
        "serving execution: injects the PostgreSQL password": (
            "serving execution: cannot inject the control-plane parameter postgres_password",
            serving_adds_resource("InjectServingParameters", parameter("postgres_password")),
        ),
        "serving execution: injects every parameter of the project folder": (
            "serving execution: cannot inject the control-plane parameter polaris_root_secret",
            serving_sets_resource("InjectServingParameters", parameter("*")),
        ),
        "serving execution: injects a name that only starts like a serving one": (
            "serving execution: cannot inject model_api_key_hashes_extra",
            serving_sets_resource("InjectServingParameters", [parameter("model_api_key_hashes*"), *[parameter(name) for name in SERVING_PARAMETERS[1:]]]),
        ),
        "serving execution: reads one parameter at a time": (
            "serving execution: no ssm:GetParameter",
            serving_adds_action("InjectServingParameters", "ssm:GetParameter"),
        ),
        "serving execution: reads a Secrets Manager secret": ("serving execution: cannot read a Secrets Manager secret", serving_reads_secrets),
        "serving execution: decrypts with a key": ("serving execution: no kms:Decrypt", serving_adds_action("InjectServingParameters", "kms:Decrypt")),
        "serving execution: pulls every image of the project": (
            "serving execution: cannot pull the airflow image",
            serving_sets_resource("PullServingImages", arn("ecr", f"repository/{P}_*")),
        ),
        "serving execution: pulls serving images of every environment": (
            "serving execution: cannot pull a serving image of another environment",
            serving_sets_resource("PullServingImages", [arn("ecr", f"repository/{P}_model_api_*"), arn("ecr", f"repository/{P}_streamlit_*")]),
        ),
        "serving execution: pushes images": ("serving execution: cannot push", serving_adds_action("PullServingImages", "ecr:PutImage")),
        "serving execution: writes every stream of the shared log group": (
            "serving execution: cannot write the airflow_scheduler log stream",
            serving_sets_resource("WriteServingLogs", arn("logs", f"log-group:{P}_airflow_{E}:log-stream:*")),
        ),
        "serving execution: writes the logs of every group": (
            "serving execution: cannot write the jobs log group",
            serving_sets_resource("WriteServingLogs", arn("logs", f"log-group:{P}_*:log-stream:*")),
        ),
        "serving execution: creates log groups": (
            "serving execution: cannot create a log group",
            serving_creates_groups,
        ),
        "serving execution: loses the parameter injection": ("serving execution: injects model_api_key_hashes", serving_drops("InjectServingParameters")),
        "serving execution: loses one serving parameter": (
            "serving execution: injects streamlit_model_api_key",
            serving_without_resource("InjectServingParameters", "streamlit_model_api_key"),
        ),
        "serving execution: loses the image pull": ("serving execution: pulls model_api with BatchGetImage", serving_drops("PullServingImages")),
        "serving execution: loses the log write": ("serving execution: writes the model_api log stream", serving_drops("WriteServingLogs")),
        "serving execution: loses the registry token": ("serving execution: registry token", serving_drops("EcrToken")),
    }

    def research_statement(documents: dict[str, dict[str, Any]], key: str, sid: str) -> dict[str, Any]:
        return next(statement for statement in documents[key]["Statement"] if statement["Sid"] == sid)

    def research_sets(key: str, sid: str, field: str, value: Any) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            research_statement(documents, key, sid)[field] = value

        return mutation

    def research_adds_action(key: str, sid: str, *actions: str) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            statement = research_statement(documents, key, sid)
            statement["Action"] = [*as_list(statement["Action"]), *actions]

        return mutation

    def research_drops_condition(key: str, sid: str) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            del research_statement(documents, key, sid)["Condition"]

        return mutation

    def research_removes(key: str, sid: str) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            documents[key]["Statement"] = [statement for statement in documents[key]["Statement"] if statement["Sid"] != sid]

        return mutation

    def research_appends(key: str, statement: dict[str, Any]) -> Mutation:
        def mutation(documents: dict[str, dict[str, Any]]) -> None:
            documents[key]["Statement"].append(statement)

        return mutation

    records = f"role/{TRAIN_TASK}/research_records"
    notice_key = f"role/{TRAIN_TASK}/owner_notice"
    launch = f"role/{LAUNCH_TASK}/research_records"
    research_defects: dict[str, tuple[str, Mutation]] = {
        "research records: the create loses its header condition": ("research records: overwrite", research_drops_condition(records, "CreateResearchRecords")),
        "research records: the create reaches every object of the bucket": (
            "research records: PutObject outside research/",
            research_sets(records, "CreateResearchRecords", "Resource", "*"),
        ),
        "research records: the create reaches a prefix that only starts with research": (
            "research records: PutObject outside research/",
            research_sets(records, "CreateResearchRecords", "Resource", f"{BUCKET}/research*"),
        ),
        "research records: the records may be deleted": (
            "research records: DeleteObject",
            research_adds_action(records, "CreateResearchRecords", "s3:DeleteObject"),
        ),
        "research records: the records may be tagged": (
            "research records: PutObjectTagging",
            research_adds_action(records, "CreateResearchRecords", "s3:PutObjectTagging"),
        ),
        "research records: the read reaches every object of the bucket": (
            "research records: GetObject outside research/",
            research_sets(records, "ReadResearchRecords", "Resource", f"{BUCKET}/*"),
        ),
        "research records: the read reaches every resource": (
            "research records: GetObject outside research/",
            research_sets(records, "ReadResearchRecords", "Resource", "*"),
        ),
        "research records: the read reaches other versions": (
            "research records: GetObjectVersion",
            research_adds_action(records, "ReadResearchRecords", "s3:GetObjectVersion"),
        ),
        "research records: the list loses its prefix condition": (
            "research records: list without a prefix",
            research_drops_condition(records, "ListResearchRecords"),
        ),
        "research records: the list takes any prefix": (
            "research records: list outside research/",
            research_sets(records, "ListResearchRecords", "Condition", {"StringLike": {"s3:prefix": ["*"]}}),
        ),
        "research records: the list reaches the whole bucket's objects": (
            "research records: list in another bucket",
            research_sets(records, "ListResearchRecords", "Resource", "arn:aws:s3:::*"),
        ),
        "research records: the create statement is gone": ("research records: create", research_removes(records, "CreateResearchRecords")),
        "research records: the read statement is gone": ("research records: GetObject research/", research_removes(records, "ReadResearchRecords")),
        "research records: the list statement is gone": ("research records: list under", research_removes(records, "ListResearchRecords")),
        "launch research read: the read reaches every object of the bucket": (
            "launch research read: GetObject outside research/",
            research_sets(launch, "ReadResearchRecords", "Resource", f"{BUCKET}/*"),
        ),
        "launch research read: the read reaches every resource": (
            "launch research read: GetObject outside research/",
            research_sets(launch, "ReadResearchRecords", "Resource", "*"),
        ),
        "launch research read: the read may write": ("launch research read: PutObject", research_adds_action(launch, "ReadResearchRecords", "s3:PutObject")),
        "launch research read: the read may delete": (
            "launch research read: DeleteObject",
            research_adds_action(launch, "ReadResearchRecords", "s3:DeleteObject"),
        ),
        "launch research read: the read may list": (
            "launch research read: list under",
            research_appends(launch, {"Sid": "ListAnything", "Effect": "Allow", "Action": "s3:ListBucket", "Resource": BUCKET}),
        ),
        "launch research read: the read statement is gone": ("launch research read: GetObject research/", research_removes(launch, "ReadResearchRecords")),
        "owner notice: the publish reaches every topic": (
            "owner notice: Publish to the budget topic",
            research_sets(notice_key, "PublishOwnerNotices", "Resource", "*"),
        ),
        "owner notice: the publish reaches the budget topic too": (
            "owner notice: Publish to the budget topic",
            research_sets(notice_key, "PublishOwnerNotices", "Resource", [TOPIC, BUDGET_TOPIC]),
        ),
        "owner notice: the publish reaches topics that start like the alerts topic": (
            "owner notice: Publish to a topic that only starts like",
            research_sets(notice_key, "PublishOwnerNotices", "Resource", f"{TOPIC}*"),
        ),
        "owner notice: the publish reaches the topic of every region": (
            "owner notice: Publish to the alerts topic of another region",
            research_sets(notice_key, "PublishOwnerNotices", "Resource", TOPIC.replace(R, "*")),
        ),
        "owner notice: the publish reaches the topic of every account": (
            "owner notice: Publish to the alerts topic of another account",
            research_sets(notice_key, "PublishOwnerNotices", "Resource", TOPIC.replace(A, "*")),
        ),
        "owner notice: the action is every SNS action": ("owner notice: sns:Subscribe", research_sets(notice_key, "PublishOwnerNotices", "Action", "sns:*")),
        "owner notice: the role may subscribe": ("owner notice: sns:Subscribe", research_adds_action(notice_key, "PublishOwnerNotices", "sns:Subscribe")),
        "owner notice: the role may change the topic": (
            "owner notice: sns:SetTopicAttributes",
            research_adds_action(notice_key, "PublishOwnerNotices", "sns:SetTopicAttributes"),
        ),
        "owner notice: the statement is gone": ("owner notice: Publish to the alerts topic", research_removes(notice_key, "PublishOwnerNotices")),
    }

    return {
        **job_defects,
        **serving_execution_defects,
        **research_defects,
        **delivery_defects,
        **serving_release_defects,
        **plan_delivery_defects,
        **aws_pair_defects,
        **openmetadata_defects,
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
        "the snapshot denies match the volume part of CreateSnapshot": (
            "dlm: volume part of the data volume snapshot without request tags",
            snapshot_denies_on_volume,
        ),
        "the classification tag reaches any project volume": (
            "ec2:CreateTags later sets DataClassification with another named volume",
            classification_tag_any_volume,
        ),
        "both key limits on the classification tag removed (allow key list and deny set operator)": (
            "ec2:CreateTags later sets DataClassification with another key beside it",
            classification_tag_any_key,
        ),
        "the classification tag takes any value": ("ec2:CreateTags later sets DataClassification with another value", classification_tag_any_value),
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
        "the alert topics statement uses a project name pattern": (
            "alert topics: refuse sns:SetTopicAttributes on a topic that is not an alert topic",
            topic_wildcard,
        ),
        "the alert topics statement reaches every topic": ("alert topics: refuse sns:Subscribe on a topic that is not an alert topic", alerts_on_every_topic),
        "the alert topics statement matches a longer topic name": ("alert topics: refuse sns:Subscribe on a longer topic name", alerts_match_topic_prefix),
        "the alert topics statement matches a topic of another project": (
            "alert topics: refuse sns:Subscribe on the alerts topic of another project",
            alerts_match_any_infix,
        ),
        "the alert topics statement keeps the infix pattern of the project name": (
            "alert topics: refuse sns:Subscribe on the alerts topic of a project whose name extends the project name",
            alerts_match_project_infix,
        ),
        "the alert topics statement ignores the environment": (
            "alert topics: refuse sns:Subscribe on the alerts topic of another environment",
            alerts_ignore_environment,
        ),
        "the alert topics statement misses the subscriptions": (
            "alert topics: allow sns:GetSubscriptionAttributes on a subscription of the budget alerts topic",
            alerts_without_subscriptions,
        ),
        "the alert topics statement is removed": ("alert topics: allow sns:Subscribe on the budget alerts topic", alerts_removed),
        "the alert topics statement misses the budget topic": ("alert topics: allow sns:Publish on the budget alerts topic", alerts_only_platform_topic),
        "the alert topics statement allows only subscribing": ("alert topics: allow sns:Publish on the alerts topic", alerts_narrowed_actions),
        "the Project tag can be changed or removed later": ("logs:TagResource changes Project", tag_change_allowed),
        "image push and pull on every repository": ("ecr:PutImage in another project", ecr_cross_project),
        "parameter read by wildcard": ("parameter: read another name in the project folder", parameter_wildcard),
        "parameter path read": ("forbidden ssm:GetParametersByPath", parameter_path_read),
        "write access to the read-only administrator password": ("parameter: overwrite the Airflow administrator password", admin_password_write),
        "sts AssumeRole on every role": ("AssumeRole administrator", roles_wildcard),
        "the logging setting is allowed on every bucket": ("logging: PutBucketLogging on every bucket", logging_any_bucket),
        "the logging setting is allowed on the logging bucket": ("logging bucket: PutBucketLogging refused", logging_on_logging_bucket),
        "the logging bucket gets object reads": ("logging bucket: GetObject on a log object refused", logging_bucket_objects),
        "the logging bucket may be created in any region": ("logging bucket: CreateBucket in another region", logging_bucket_any_region),
        "the logging bucket may be deleted": ("logging bucket: DeleteBucket refused", logging_bucket_delete),
        "the logging bucket gets versioning": ("logging bucket: PutBucketVersioning refused", logging_bucket_versioning),
        "the logging bucket settings reach a name pattern": ("logging bucket: settings on another bucket", logging_bucket_wildcard),
        "the Athena statement reaches every role": ("AssumeRole administrator", athena_roles_wildcard),
        "the Athena statement reaches every Athena role": ("AssumeRole another athena role", athena_role_prefix),
        "the Athena statement reaches the reader of every environment": ("AssumeRole athena_reader of another environment", athena_any_environment),
        "the Athena statement carries direct Athena and Glue actions": ("the project user has no direct athena:StartQueryExecution", athena_direct_access),
        "the Athena statement carries direct Glue read on the bronze mirror": (
            "the project user has no direct glue:GetTable on the bronze mirror",
            athena_direct_bronze_glue,
        ),
        "the promotion create loses its header condition": ("promote records: overwrite", promote_create_unconditional),
        "the promotion read reaches the whole bucket": ("promote records: GetObject outside promote/", promote_read_whole_bucket),
        "the promotion records may be deleted": ("promote records: DeleteObject", promote_records_deleted),
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
        "design 3 defect 6: the boundary's AssumeRole deny uses the shared prefix healthcare_*": (
            "bounded: wide role assumes healthcare_realtime_x",
            boundary_shared_prefix,
        ),
        "design 3 defect 7: the boundary allows s3:GetObject on every resource": ("bounded: wide role reads other-bucket", boundary_reads_every_bucket),
        "design 3 defect 8: the boundary lacks an action the host role needs": (
            "coverage: ecs_host JoinProjectCluster ecs:RegisterContainerInstance",
            boundary_without_host_action,
        ),
        "the boundary does not deny IAM, Organizations, account and budget actions": ("bounded: wide role iam:CreateRole", boundary_without_iam_deny),
        "the boundary deletes objects across the bucket (review of parts 1 and 2, finding 2)": (
            "bounded: wide role s3:DeleteObject on a source object",
            boundary_writes_every_object,
        ),
        "the boundary deletes project snapshots the schedule did not make": (
            "bounded: wide role deletes a manual project snapshot",
            boundary_only_project_tag("DeleteScheduledSnapshots"),
        ),
        "the boundary attaches any project volume": (
            "bounded: wide role attaches a project volume with another Name",
            boundary_only_project_tag("AttachAndSnapshotDataVolume"),
        ),
        "the boundary associates addresses with any project instance": (
            "bounded: wide role ec2:AssociateAddress on a project instance with another Name",
            boundary_only_project_tag("HostAttachments"),
        ),
    }
