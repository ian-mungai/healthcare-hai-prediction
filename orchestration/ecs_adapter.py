"""Start, poll, stop and report one ECS task per job for the lean broker target.

The adapter holds no state. A job key names one task: the key gives both the ``clientToken`` and the ``startedBy`` tag of ``RunTask``, so a repeated
start returns the task that already runs (``list_tasks`` finds it by tag) or the one the first call created. Only the named job families start. Every call
has a 5 second connect and 10 second read limit, and the caller owns the job deadline. Reads have two SDK retries. RunTask has none: it goes through a client
of its own that makes one attempt per call (``send_config``), so every request that may reach ECS passes the caller's guard, and a retry can never answer
for a request whose reply was lost (a task may exist). The adapter refuses a RunTask client with retries. A job runs in one of two places. ``fargate`` is the
awsvpc task of the family's own definition. ``host`` is the bridge task of its ``_host`` definition on the host's EC2 capacity provider: it carries no awsvpc
network, and the capacity provider strategy is always sent, because the cluster default strategy would place the task on Fargate. A failure that a later call
can cure raises ``TaskUnavailable``, the rest raise ``TaskRefused``. Reasons shown to callers carry no ARN and no account number.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

TRANSIENT_CODES = frozenset({"ThrottlingException", "ServerException", "ServiceUnavailableException", "RequestLimitExceeded", "InternalFailure"})
STOPPED_PAGES = 10
DESCRIBE_BATCH = 100
PENDING_STATUSES = frozenset({"PROVISIONING", "PENDING", "ACTIVATING"})
STOPPED = "STOPPED"
NO_EXIT_CODE = 125
USER_STOP_CODE = 143
FARGATE = "fargate"
HOST = "host"
HOST_SUFFIX = "_host"
GUARD = "guard"  # the container of a layer task that ends the task when a phase fails (scripts/orchestration/task_guard.py)
REASON_LIMIT = 200
ARN = re.compile(r"arn:[^\s,;]+")
ACCOUNT = re.compile(r"\b[0-9]{12}\b")


class TaskRefused(Exception):
    """The request cannot succeed as it stands: an unnamed family, a denied call or a refused parameter."""


class TaskUnavailable(Exception):
    """The call failed in a way a later identical call can cure: throttling, a timeout, no capacity. After a timeout or a lost reply a task may exist."""


class SendUnknown(TaskUnavailable):
    """The RunTask call may have reached ECS and its answer is unknown (a timeout, a lost reply, a server error). A task may exist."""


class SendWithheld(Exception):
    """The caller's guard refused the RunTask call just before it was made. Nothing was sent."""


class CapacityUnavailable(TaskUnavailable):
    """RunTask answered that the host has no room. The answer is definite: no task was created."""


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
    """Where a job runs. ``family_prefix`` is ``<project>_<environment>_job_``, so a job named ``train`` runs the definition ``<prefix>train`` on Fargate and
    ``<prefix>train_host`` on the capacity provider ``host_capacity_provider``, which stays empty while no job runs on the host."""

    cluster: str
    family_prefix: str
    subnets: tuple[str, ...]
    networks: Mapping[str, tuple[str, ...]]
    project_tag: str
    log_group: str
    families: frozenset[str]
    host_capacity_provider: str = ""


@dataclass(frozen=True)
class TaskFound:
    """A task found by its identity. ``created_at`` is the time ECS created it, in epoch seconds, or None when ECS gave none."""

    arn: str
    created_at: float | None


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


def send_config() -> Config:
    """The RunTask client: the same limits and one attempt per call, with no SDK retry (``total_max_attempts`` 1, which botocore takes over a
    ``max_attempts`` from the environment or the config file). The caller owns every repeat, each behind its own guard."""
    return Config(connect_timeout=5, read_timeout=10, retries={"total_max_attempts": 1, "mode": "standard"})


def sends_once(client: Any) -> bool:
    """True when a botocore client makes one attempt per call. A stand-in without the SDK's client metadata has no retry handler."""
    config = getattr(getattr(client, "meta", None), "config", None)
    if config is None:
        return True
    return bool((getattr(config, "retries", None) or {}).get("total_max_attempts") == 1)


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
    """One ECS cluster's task calls for the broker and the launcher. ``sender`` is the client RunTask goes through, ``ecs`` when not given. It must make one
    attempt per call, else the adapter refuses to be built."""

    def __init__(self, ecs: EcsClient, logs: LogsClient, settings: EcsSettings, sender: EcsClient | None = None) -> None:
        self.ecs = ecs
        self.logs = logs
        self.settings = settings
        self.sender = ecs if sender is None else sender
        if not sends_once(self.sender):
            raise ValueError(
                "the RunTask client must make one attempt per call (build it with send_config): an SDK retry would send again after the caller's guard "
                "and could hide the lost reply of a request that created a task"
            )

    @classmethod
    def from_session(cls, session: Any, settings: EcsSettings) -> EcsAdapter:
        """The adapter over a boto3 session: reads with ``client_config`` and RunTask through its own client with ``send_config``."""
        return cls(
            session.client("ecs", config=client_config()),
            session.client("logs", config=client_config()),
            settings,
            sender=session.client("ecs", config=send_config()),
        )

    @staticmethod
    def identity(key: str) -> str:
        """The ECS ``startedBy`` value of the task started for ``key``: the same on every start, so the task can be found again after a lost reply."""
        return f"hai-{hashlib.sha256(key.encode()).hexdigest()[:40]}"

    def find(self, key: str) -> str | None:
        """The ARN of the running task started for ``key``, or None when ECS lists none. A failed listing raises, so the caller keeps the question open."""
        try:
            running = self.ecs.list_tasks(cluster=self.settings.cluster, startedBy=self.identity(key))["taskArns"]
        except (ClientError, BotoCoreError) as error:
            raise classify(error) from None
        return str(running[0]) if running else None

    def unstopped(self, definition: str) -> list[str]:
        """The ARNs of the tasks of a task definition family that ECS does not show STOPPED: every task it lists with desired status RUNNING (pending or
        running), and every task with desired status STOPPED whose last status is not STOPPED yet (a stopping task still runs its containers). A failed or
        unfinished listing, or a describe reply that leaves a listed task out, raises: neither is evidence that the family is idle."""
        try:
            busy = [str(arn) for arn in self.ecs.list_tasks(cluster=self.settings.cluster, family=definition)["taskArns"]]
            if busy:
                return busy
            token: str | None = None
            for _ in range(STOPPED_PAGES):
                page = self.ecs.list_tasks(cluster=self.settings.cluster, family=definition, desiredStatus=STOPPED, **({"nextToken": token} if token else {}))
                arns = [str(arn) for arn in page["taskArns"]]
                for start in range(0, len(arns), DESCRIBE_BATCH):
                    batch = arns[start : start + DESCRIBE_BATCH]
                    described = {
                        str(task.get("taskArn")): task for task in self.ecs.describe_tasks(cluster=self.settings.cluster, tasks=batch).get("tasks") or []
                    }
                    if set(batch) - set(described):
                        raise TaskUnavailable("stopping_task_not_described")
                    busy += [arn for arn in batch if described[arn].get("lastStatus") != STOPPED]
                token = page.get("nextToken")
                if not token:
                    return busy
        except (ClientError, BotoCoreError) as error:
            raise classify(error) from None
        raise TaskUnavailable("stopped_task_listing_too_long")

    def lookup(self, key: str, definition: str) -> TaskFound | None:
        """The task started for ``key`` in any state ECS still lists: running first, then stopped (ECS keeps a stopped task for about an hour), so a task that
        ended before the caller looked is found too. None means no listed task carries the identity. A failed or unfinished listing raises.
        ``definition`` is the task definition family of the key's task: the stopped listing names it, so only that family's tasks are described (a role may
        describe only the tasks of the families it starts)."""
        started_by = self.identity(key)
        try:
            running = self.ecs.list_tasks(cluster=self.settings.cluster, startedBy=started_by)["taskArns"]
            if running:
                found = self.ecs.describe_tasks(cluster=self.settings.cluster, tasks=[str(running[0])]).get("tasks") or []
                return TaskFound(str(running[0]), self.created_at(found[0]) if found else None)
            token: str | None = None
            for _ in range(STOPPED_PAGES):
                page = self.ecs.list_tasks(cluster=self.settings.cluster, desiredStatus=STOPPED, family=definition, **({"nextToken": token} if token else {}))
                arns = [str(arn) for arn in page["taskArns"]]
                for start in range(0, len(arns), DESCRIBE_BATCH):
                    for task in self.ecs.describe_tasks(cluster=self.settings.cluster, tasks=arns[start : start + DESCRIBE_BATCH]).get("tasks") or []:
                        if task.get("startedBy") == started_by:
                            return TaskFound(str(task["taskArn"]), self.created_at(task))
                token = page.get("nextToken")
                if not token:
                    return None
        except (ClientError, BotoCoreError) as error:
            raise classify(error) from None
        raise TaskUnavailable("stopped_task_listing_too_long")

    @staticmethod
    def created_at(task: Mapping[str, Any]) -> float | None:
        created = task.get("createdAt")
        return created.timestamp() if isinstance(created, datetime) else None

    def definition(self, family: str, placement: str = FARGATE) -> str:
        """The task definition that runs the family in the placement."""
        if placement == FARGATE:
            return f"{self.settings.family_prefix}{family}"
        if placement == HOST:
            return f"{self.settings.family_prefix}{family}{HOST_SUFFIX}"
        raise TaskRefused("placement_unknown")

    def placement(self, placement: str, network: str) -> dict[str, Any]:
        """The ``RunTask`` arguments that choose where the task runs. A refusal comes before any cloud call."""
        if placement == FARGATE:
            if network not in self.settings.networks:
                raise TaskRefused("network_not_bound")
            return {
                "capacityProviderStrategy": [{"capacityProvider": "FARGATE", "weight": 1}],
                "networkConfiguration": {
                    "awsvpcConfiguration": {
                        "subnets": list(self.settings.subnets),
                        "securityGroups": list(self.settings.networks[network]),
                        "assignPublicIp": "DISABLED",
                    }
                },
            }
        if placement == HOST:
            if not self.settings.host_capacity_provider:
                raise TaskRefused("host_provider_not_set")
            return {"capacityProviderStrategy": [{"capacityProvider": self.settings.host_capacity_provider, "weight": 1}]}
        raise TaskRefused("placement_unknown")

    def start(
        self,
        family: str,
        key: str,
        environment: Mapping[str, Mapping[str, str]],
        network: str,
        placement: str = FARGATE,
        guard: Callable[[], None] | None = None,
    ) -> str:
        """Run the family's task once for ``key`` and return its ARN. ``environment`` maps a container name to the variables it receives. ``network`` names the
        class whose security groups a Fargate task gets: each family runs only in the network its job table row approves. A host task has no security group,
        the host firewall bounds it. ``guard`` runs immediately before the RunTask call, after the discovery of a running task, and raises ``SendWithheld`` to
        stop the call. The call is one request with no SDK retry. A RunTask call that may have been sent and has no answer raises ``SendUnknown``."""
        if family not in self.settings.families:
            raise TaskRefused("family_not_named")
        where = self.placement(placement, network)
        digest = hashlib.sha256(key.encode()).hexdigest()
        started_by = self.identity(key)
        try:
            running = self.ecs.list_tasks(cluster=self.settings.cluster, startedBy=started_by)["taskArns"]
        except (ClientError, BotoCoreError) as error:
            raise classify(error) from None
        if running:
            return str(running[0])
        if guard is not None:
            guard()
        try:
            reply = self.sender.run_task(
                cluster=self.settings.cluster,
                taskDefinition=self.definition(family, placement),
                count=1,
                **where,
                overrides={
                    "containerOverrides": [
                        {"name": name, "environment": [{"name": variable, "value": value} for variable, value in values.items()]}
                        for name, values in environment.items()
                    ]
                },
                propagateTags="TASK_DEFINITION",
                enableECSManagedTags=True,
                tags=[{"key": "Project", "value": self.settings.project_tag}, {"key": "HaiJob", "value": family}],
                startedBy=started_by,
                clientToken=digest,
            )
        except (ClientError, BotoCoreError) as error:
            failure = classify(error)
            if isinstance(failure, TaskUnavailable):
                raise SendUnknown(str(failure)) from None
            raise failure from None
        if reply["tasks"]:
            return str(reply["tasks"][0]["taskArn"])
        reasons = " ".join(str(item.get("reason", "")) for item in reply["failures"])
        if re.search(r"capacity|RESOURCE:|AGENT", reasons, re.IGNORECASE):
            raise CapacityUnavailable(scrub(reasons))
        raise TaskRefused(scrub(reasons) or "no_task_started")

    def poll(self, task_arn: str) -> TaskReport:
        """The task's state now. A task counts as passed only when every one of its containers exited 0, except the guard of a layer task: ECS stops the guard
        when the last phase ends, so its exit code says nothing about the job, and a failed phase fails the job with the phase's own code. A reply that
        describes no task, or has no task list, raises ``TaskUnavailable``: ECS documents no guarantee that it describes a task it created, so such a reply
        says nothing about how the task is doing, and every caller keeps the task unresolved."""
        try:
            reply = self.ecs.describe_tasks(cluster=self.settings.cluster, tasks=[task_arn])
        except (ClientError, BotoCoreError) as error:
            raise classify(error) from None
        tasks = reply.get("tasks") or []
        if not tasks:
            raise TaskUnavailable("task_not_described")
        task = tasks[0]
        status = task.get("lastStatus", "")
        if status != STOPPED:
            return TaskReport("pending" if status in PENDING_STATUSES else "running", None, "")
        containers = tuple((str(item["name"]), item.get("exitCode")) for item in task.get("containers", []))
        reason = scrub(str(task.get("stoppedReason", "")))
        codes = [code for name, code in containers if name != GUARD]
        if codes and all(code == 0 for code in codes):
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
