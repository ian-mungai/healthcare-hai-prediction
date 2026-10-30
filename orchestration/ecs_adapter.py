"""Start, poll, stop and report one ECS task per job for the lean broker target.

The adapter holds no state. A job key names one task: the key gives both the ``clientToken`` and the ``startedBy`` tag of ``RunTask``, so a repeated
start returns the task that already runs (``list_tasks`` finds it by tag) or the one the first call created. Only the named job families start. Every call
has a 5 second connect and 10 second read limit with two retries, and the caller owns the job deadline. A failure that a later call can cure raises
``TaskUnavailable``, the rest raise ``TaskRefused``. Reasons shown to callers carry no ARN and no account number.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

TRANSIENT_CODES = frozenset({"ThrottlingException", "ServerException", "ServiceUnavailableException", "RequestLimitExceeded", "InternalFailure"})
PENDING_STATUSES = frozenset({"PROVISIONING", "PENDING", "ACTIVATING"})
STOPPED = "STOPPED"
NO_EXIT_CODE = 125
USER_STOP_CODE = 143
REASON_LIMIT = 200
ARN = re.compile(r"arn:[^\s,;]+")
ACCOUNT = re.compile(r"\b[0-9]{12}\b")


class TaskRefused(Exception):
    """The request cannot succeed as it stands: an unnamed family, a denied call or a refused parameter."""


class TaskUnavailable(Exception):
    """The call failed in a way a later identical call can cure: throttling, a timeout, no capacity."""


class EcsClient(Protocol):
    """The boto3 ECS client calls the adapter makes."""

    def run_task(self, **arguments: Any) -> dict[str, Any]: ...

    def describe_tasks(self, **arguments: Any) -> dict[str, Any]: ...

    def stop_task(self, **arguments: Any) -> dict[str, Any]: ...

    def list_tasks(self, **arguments: Any) -> dict[str, Any]: ...


class LogsClient(Protocol):
    """The boto3 CloudWatch Logs client call the adapter makes."""

    def get_log_events(self, **arguments: Any) -> dict[str, Any]: ...


@dataclass(frozen=True)
class EcsSettings:
    """Where a job runs. ``family_prefix`` is ``<project>_<environment>_job_``, so a job named ``train`` runs the definition ``<prefix>train``."""

    cluster: str
    family_prefix: str
    subnets: tuple[str, ...]
    security_groups: tuple[str, ...]
    project_tag: str
    log_group: str
    families: frozenset[str]


@dataclass(frozen=True)
class TaskReport:
    """One task's state. ``exit_code`` is set once the task ended. ``containers`` pairs each container with its exit code, or None when it never ran."""

    state: str
    exit_code: int | None
    reason: str
    containers: tuple[tuple[str, int | None], ...] = ()


def client_config() -> Config:
    """Every cloud call ends after 5 seconds to connect and 10 to read, with two retries."""
    return Config(connect_timeout=5, read_timeout=10, retries={"max_attempts": 2, "mode": "standard"})


def scrub(text: str) -> str:
    """A reason without ARNs or account numbers, at most 200 characters."""
    return ACCOUNT.sub("[account]", ARN.sub("[arn]", text))[:REASON_LIMIT]


def classify(error: Exception) -> Exception:
    """A cloud error as the adapter's transient or permanent refusal."""
    if isinstance(error, ClientError):
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code in TRANSIENT_CODES:
            return TaskUnavailable(code)
        return TaskRefused(code or "ClientError")
    return TaskUnavailable(type(error).__name__)


class EcsAdapter:
    """One ECS cluster's task calls for the broker."""

    def __init__(self, ecs: EcsClient, logs: LogsClient, settings: EcsSettings) -> None:
        self.ecs = ecs
        self.logs = logs
        self.settings = settings

    def start(self, family: str, key: str, environment: Mapping[str, Mapping[str, str]]) -> str:
        """Run the family's task once for ``key`` and return its ARN. ``environment`` maps a container name to the variables it receives."""
        if family not in self.settings.families:
            raise TaskRefused("family_not_named")
        digest = hashlib.sha256(key.encode()).hexdigest()
        started_by = f"hai-{digest[:40]}"
        try:
            running = self.ecs.list_tasks(cluster=self.settings.cluster, startedBy=started_by)["taskArns"]
            if running:
                return str(running[0])
            reply = self.ecs.run_task(
                cluster=self.settings.cluster,
                taskDefinition=f"{self.settings.family_prefix}{family}",
                count=1,
                capacityProviderStrategy=[{"capacityProvider": "FARGATE", "weight": 1}],
                networkConfiguration={
                    "awsvpcConfiguration": {
                        "subnets": list(self.settings.subnets),
                        "securityGroups": list(self.settings.security_groups),
                        "assignPublicIp": "DISABLED",
                    }
                },
                overrides={
                    "containerOverrides": [
                        {"name": name, "environment": [{"name": variable, "value": value} for variable, value in values.items()]}
                        for name, values in environment.items()
                    ]
                },
                propagateTags="TASK_DEFINITION",
                enableECSManagedTags=True,
                tags=[{"key": "Project", "value": self.settings.project_tag}],
                startedBy=started_by,
                clientToken=digest,
            )
        except (ClientError, BotoCoreError) as error:
            raise classify(error) from None
        if reply["tasks"]:
            return str(reply["tasks"][0]["taskArn"])
        reasons = " ".join(str(item.get("reason", "")) for item in reply["failures"])
        if re.search(r"capacity|RESOURCE:|AGENT", reasons, re.IGNORECASE):
            raise TaskUnavailable(scrub(reasons))
        raise TaskRefused(scrub(reasons) or "no_task_started")

    def poll(self, task_arn: str) -> TaskReport:
        """The task's state now. A task counts as passed only when every one of its containers exited 0."""
        try:
            reply = self.ecs.describe_tasks(cluster=self.settings.cluster, tasks=[task_arn])
        except (ClientError, BotoCoreError) as error:
            raise classify(error) from None
        if not reply["tasks"]:
            return TaskReport("gone", None, "not_found")
        task = reply["tasks"][0]
        status = task.get("lastStatus", "")
        if status != STOPPED:
            return TaskReport("pending" if status in PENDING_STATUSES else "running", None, "")
        containers = tuple((str(item["name"]), item.get("exitCode")) for item in task.get("containers", []))
        reason = scrub(str(task.get("stoppedReason", "")))
        codes = [code for _, code in containers]
        if containers and all(code == 0 for code in codes):
            return TaskReport("succeeded", 0, reason, containers)
        failing = next((code for code in codes if code not in (0, None)), None)
        fallback = USER_STOP_CODE if task.get("stopCode") == "UserInitiated" else NO_EXIT_CODE
        return TaskReport("failed", failing if failing is not None else fallback, reason, containers)

    def stop(self, task_arn: str, reason: str) -> None:
        """Ask ECS to stop the task. A task that already ended is not an error."""
        try:
            self.ecs.stop_task(cluster=self.settings.cluster, task=task_arn, reason=reason[:100])
        except (ClientError, BotoCoreError) as error:
            if isinstance(error, ClientError) and "not found" in str(error.response.get("Error", {}).get("Message", "")).lower():
                return
            raise classify(error) from None

    def tail(self, task_arn: str, container: str, stream_prefix: str, lines: int) -> str:
        """The last ``lines`` log lines of one container, or nothing while its stream does not exist."""
        stream = f"{stream_prefix}/{container}/{task_arn.rsplit('/', 1)[-1]}"
        try:
            events = self.logs.get_log_events(logGroupName=self.settings.log_group, logStreamName=stream, limit=lines, startFromHead=False)["events"]
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                return ""
            raise classify(error) from None
        except BotoCoreError as error:
            raise classify(error) from None
        return "\n".join(str(event["message"]) for event in events)
