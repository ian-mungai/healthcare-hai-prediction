"""Start, poll, stop and report one ECS task per job for the direct ECS launch.

The adapter holds no state. A job key names one task: the key gives both the ``clientToken`` and the ``startedBy`` tag of ``RunTask``, so a repeated
start returns the task that already runs (``list_tasks`` finds it by tag) or the one the first call created. Only the named job families start. Every call
has a 5 second connect and 10 second read limit, and the caller owns the job deadline. Reads have two SDK retries. RunTask has none: it goes through a client
of its own that makes one attempt per call (``send_config``), so every request that may reach ECS passes the caller's guard, and a retry can never answer
for a request whose reply was lost (a task may exist). The guard runs again at the SDK's send boundary, after signing and any credential refresh, so a hold
that is set while the SDK works still withholds the call. The adapter refuses a RunTask client with retries. Every job runs in one place: the bridge task of the
family's ``_host`` definition on the host's EC2 capacity provider. It carries no awsvpc network, and the capacity provider strategy is always sent, so no
change of the cluster default can move a task. A failure that a later call can cure raises ``TaskUnavailable``, the rest raise ``TaskRefused``. Reasons shown
to callers carry no ARN and no account number.
"""

from __future__ import annotations

import contextvars
import hashlib
import json
import re
from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

TRANSIENT_CODES = frozenset({"ThrottlingException", "ServerException", "ServiceUnavailableException", "RequestLimitExceeded", "InternalFailure"})
GIB = 1024**3
STOPPED_PAGES = 10
DESCRIBE_BATCH = 100
PENDING_STATUSES = frozenset({"PROVISIONING", "PENDING", "ACTIVATING"})
STOPPED = "STOPPED"
NO_EXIT_CODE = 125
USER_STOP_CODE = 143
HOST_SUFFIX = "_host"
GUARD = "guard"  # the container of a layer task that ends the task when a phase fails (scripts/orchestration/task_guard.py)
REASON_LIMIT = 200
ARN = re.compile(r"arn:[^\s,;]+")
ACCOUNT = re.compile(r"\b[0-9]{12}\b")
# The closed failure record that a job phase writes as one log line (the writer is scripts/phase_failure.py, which the job images hold and this module cannot
# import: the Airflow image mounts a fixed module list). The lists below are the writer's, and tests/test_phase_failure.py keeps them equal. Only a line that
# fits the record exactly is read: a checked phase name, codes of the fixed list and numeric facts, so no text of a job can reach the launch record.
FAILURE_MARKER = "phase_failure: "
FAILURE_VERSION = 1
FAILURE_MAX_CODES = 8
FAILURE_MAX_FACT = 1 << 62
FAILURE_MAX_LINE = 2000
FAILURE_PHASES = (
    "init",
    "fetch",
    "pin",
    "build",
    "validate",
    "verify",
    "export",
    "publish",
    "upload",
    "maintain",
    "feast",
    "load",
    "check",
    "inventory",
    "stage",
    "prepare",
    "extract",
    "train",
    "promote",
    "research",
    "canary",
    "notify",
    "baseline",
    "current",
    "serve",
    "ingest",
    "run",
    "guard",
    "unknown",
)
FAILURE_STARTUP_CODES = (
    "credentials",
    "dbt_extension_directory_missing",
    "dbt_extension_missing",
    "dbt_profile_unreadable",
    "dbt_target_downloads_extensions",
    "dbt_target_not_executed",
    "dbt_target_refused",
    "dbt_target_unknown",
    "ecr_address_unusable",
    "ecr_unreachable",
    "input_file_missing",
    "input_file_unreadable",
    "mlflow_address_unusable",
    "mlflow_unreachable",
    "mode_files_unenumerated",
    "openmetadata_address_unusable",
    "openmetadata_unreachable",
    "plan_inputs_empty",
    "plan_inputs_missing",
    "polaris_address_unusable",
    "polaris_unreachable",
    "redis_address_unusable",
    "redis_unreachable",
    "region_invalid",
    "region_unset",
    "s3_address_unusable",
    "s3_unreachable",
    "startup_check_internal_error",
    "task_environment_unrecognised",
)
FAILURE_CAPACITY_CODES = (
    "capacity_cpu_unreadable",
    "capacity_engine_floor",
    "capacity_file_garbled",
    "capacity_memory_unreadable",
    "capacity_override_invalid",
    "capacity_profile_unknown",
    "capacity_spill_floor",
)
FAILURE_ERROR_CODES = (
    "archive_error",
    "binding_error",
    "bronze_error",
    "check_error",
    "config_error",
    "deploy_refused",
    "dispatch_error",
    "guard_timeout",
    "key_error",
    "os_error",
    "phase_failed",
    "phase_stopped",
    "pipeline_error",
    "provenance_error",
    "publish_error",
    "run_refused",
    "step_error",
    "step_failed",
    "type_error",
    "unexpected_error",
    "value_error",
)
FAILURE_CONTAINER_CODES = (
    "container_create_error",
    "container_initialization_error",
    "container_inspect_error",
    "container_out_of_memory",
    "container_pull_error",
    "container_reason_other",
    "container_runtime_error",
    "container_start_error",
    "container_timeout",
)
FAILURE_CODES = (*FAILURE_STARTUP_CODES, *FAILURE_CAPACITY_CODES, *FAILURE_ERROR_CODES, *FAILURE_CONTAINER_CODES)
FAILURE_FACTS = ("available", "engine_floor", "exit_code", "headroom", "memory", "sibling_reserve", "siblings", "spill_floor", "spill_free")
FAILURE_BYTE_FACTS = ("available", "engine_floor", "headroom", "memory", "sibling_reserve", "spill_floor", "spill_free")
# Where a stored record came from: the phase's own line (``job``), the state of the task alone (``task``) or the guard that reported its own time limit
# (``guard``).
FAILURE_SOURCES = ("job", "task", "guard")
FAILURE_TEMPLATES = {
    **{code: code.replace("_", " ") for code in FAILURE_CODES},
    "capacity_engine_floor": (
        "only {available_gib} GiB is left for the engines, below the floor of {engine_floor_gib} GiB (memory {memory_gib} GiB, headroom {headroom_gib} GiB, "
        "{siblings} other containers)"
    ),
    "capacity_spill_floor": "only {spill_free_gib} GiB is free for the spill, below the floor of {spill_floor_gib} GiB",
    "capacity_memory_unreadable": "the memory of the container cannot be read",
    "capacity_cpu_unreadable": "the CPU count of the container cannot be read",
    "capacity_file_garbled": "a capacity file of the container cannot be read as a number",
    "capacity_override_invalid": "an explicit capacity override is not understood",
    "capacity_profile_unknown": "the capacity profile is unknown",
    "guard_timeout": "the guard stopped the task at its time limit",
    "phase_failed": "the phase failed (exit code {exit_code})",
    "phase_stopped": "the container was stopped by ECS (exit code {exit_code})",
    "dbt_target_not_executed": "the dbt target check was not executed because the task capacity could not be derived",
}
CONTAINER_REASONS = {
    "CannotStartContainerError": "container_start_error",
    "CannotPullContainerError": "container_pull_error",
    "CannotCreateContainerError": "container_create_error",
    "CannotInspectContainerError": "container_inspect_error",
    "DockerTimeoutError": "container_timeout",
    "ContainerRuntimeTimeoutError": "container_timeout",
    "ContainerRuntimeError": "container_runtime_error",
    "ResourceInitializationError": "container_initialization_error",
    "OutOfMemoryError": "container_out_of_memory",
}


class TaskRefused(Exception):
    """The request cannot succeed as it stands: an unnamed family, a denied call or a refused parameter."""


class TaskUnavailable(Exception):
    """The call failed in a way a later identical call can cure: throttling, a timeout, no capacity. After a timeout or a lost reply a task may exist."""


class SendUnknown(TaskUnavailable):
    """The RunTask call may have reached ECS and its answer is unknown (a timeout, a lost reply, a server error). A task may exist."""


class SendWithheld(Exception):
    """The caller's guard refused the RunTask call just before it was made. Nothing was sent."""


class GuardFailed(Exception):
    """The caller's guard raised ``error`` at the send boundary. Nothing was sent, and the error is the guard's own, not a failure of the call."""

    def __init__(self, error: Exception) -> None:
        super().__init__(str(error))
        self.error = error


class CapacityUnavailable(TaskUnavailable):
    """RunTask answered that the host has no room. The answer is definite: no task was created."""


class EcsClient(Protocol):
    """The boto3 ECS client calls the adapter makes."""

    def run_task(self, **arguments: Any) -> dict[str, Any]: ...

    def describe_tasks(self, **arguments: Any) -> dict[str, Any]: ...

    def stop_task(self, **arguments: Any) -> dict[str, Any]: ...

    def list_tasks(self, **arguments: Any) -> dict[str, Any]: ...


@runtime_checkable
class DefinitionClient(Protocol):
    """The boto3 ECS client call that reads a task definition. Only the launch library's preflight needs it, so it is not part of ``EcsClient``."""

    def describe_task_definition(self, **arguments: Any) -> dict[str, Any]: ...


class LogsClient(Protocol):
    """The boto3 CloudWatch Logs client call the adapter makes."""

    def get_log_events(self, **arguments: Any) -> dict[str, Any]: ...


@dataclass(frozen=True)
class EcsSettings:
    """Where a job runs. ``family_prefix`` is ``<project>_<environment>_job_``, so a job named ``train`` runs the definition ``<prefix>train_host`` on the
    capacity provider ``host_capacity_provider``. An empty provider refuses every start."""

    cluster: str
    family_prefix: str
    project_tag: str
    log_group: str
    families: frozenset[str]
    host_capacity_provider: str


@dataclass(frozen=True)
class TaskFound:
    """A task found by its identity. ``created_at`` is the time ECS created it, in epoch seconds, or None when ECS gave none."""

    arn: str
    created_at: float | None


@dataclass(frozen=True)
class TaskReport:
    """One task's state. ``exit_code`` is set once the task ended. ``containers`` pairs each container with its exit code, or None when it never ran.
    ``reasons`` pairs a container with the code (``container_reason_code``) of the reason ECS gives for it, for the containers that have one."""

    state: str
    exit_code: int | None
    reason: str
    containers: tuple[tuple[str, int | None], ...] = ()
    reasons: tuple[tuple[str, str], ...] = ()


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


def fact_ok(value: object) -> bool:
    return type(value) is int and abs(value) <= FAILURE_MAX_FACT


def closed_failure(record: object, *, stored: bool) -> dict[str, Any] | None:
    """The record when it fits the closed form exactly, else None. A line of a job has the keys v, phase, codes and facts. A stored record adds the container
    (a name of the phase list) and its source. Every value is a number or a name of a fixed list, so no text can pass."""
    keys = {"v", "phase", "codes", "facts", "container", "source"} if stored else {"v", "phase", "codes", "facts"}
    if not isinstance(record, dict) or set(record) != keys or type(record["v"]) is not int or record["v"] != FAILURE_VERSION:
        return None
    codes, facts = record["codes"], record["facts"]
    valid = (
        record["phase"] in FAILURE_PHASES
        and isinstance(codes, list)
        and 1 <= len(codes) <= FAILURE_MAX_CODES
        and len(set(map(str, codes))) == len(codes)
        and all(isinstance(code, str) and code in FAILURE_CODES for code in codes)
        and isinstance(facts, dict)
        and all(isinstance(name, str) and name in FAILURE_FACTS and fact_ok(value) for name, value in facts.items())
        and (not stored or (record["container"] in FAILURE_PHASES and record["source"] in FAILURE_SOURCES))
    )
    if not valid or not isinstance(record["phase"], str):
        return None
    return {key: (list(record[key]) if key == "codes" else dict(record[key]) if key == "facts" else record[key]) for key in sorted(keys)}


def reject_constant(_name: str) -> None:
    raise ValueError("a constant that is not JSON")


def parse_failure_line(line: str) -> dict[str, Any] | None:
    """The closed record that one log line holds, or None for any other line (free text, a record with another key, an unlisted name, a text value)."""
    if not line.startswith(FAILURE_MARKER) or len(line) > FAILURE_MAX_LINE:
        return None
    try:
        found = json.loads(line[len(FAILURE_MARKER) :], parse_constant=reject_constant)
    except ValueError:
        return None
    return closed_failure(found, stored=False)


def failure_from_tail(tail: str) -> dict[str, Any] | None:
    """The last closed record in a log tail. A line that is not one is skipped and never repaired, and no other line is read."""
    for item in reversed(tail.splitlines()):
        found = parse_failure_line(item)
        if found is not None:
            return found
    return None


def stored_failure(record: object) -> dict[str, Any]:
    """The record of an end record, checked against the closed form again. Anything else raises ``ValueError``."""
    found = closed_failure(record, stored=True)
    if found is None:
        raise ValueError("the failure record is not in the closed form")
    return found


def container_reason_code(text: object) -> str:
    """The code for the reason ECS gives for a container, from its leading error name alone. An empty reason has no code, and a name that is not listed is
    ``container_reason_other``. The rest of the text is never read."""
    if not isinstance(text, str) or not text.strip():
        return ""
    match = re.match(r"[A-Za-z]+", text.strip())
    return CONTAINER_REASONS.get(match.group(0), "container_reason_other") if match else "container_reason_other"


def render_failure(record: Mapping[str, Any]) -> str:
    """The message of a stored record, from the fixed templates and its own numbers."""
    facts = record["facts"]
    values: dict[str, str] = {name: str(value) for name, value in facts.items()}
    values.update({f"{name}_gib": f"{facts[name] / GIB:.2f}" for name in FAILURE_BYTE_FACTS if name in facts})
    shown = "; ".join(FAILURE_TEMPLATES[code].format_map(defaultdict(lambda: "?", values)) for code in record["codes"])
    return f"phase {record['phase']} failed in container {record['container']}: {shown}"


def classify(error: Exception) -> Exception:
    """A cloud error as the adapter's transient or permanent refusal."""
    if isinstance(error, ClientError):
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code in TRANSIENT_CODES:
            return TaskUnavailable(code)
        return TaskRefused(code or "ClientError")
    return TaskUnavailable(type(error).__name__)


class EcsAdapter:
    """One ECS cluster's task calls for the launcher. ``sender`` is the client RunTask goes through, ``ecs`` when not given. It must make one
    attempt per call, else the adapter refuses to be built."""

    def __init__(self, ecs: EcsClient, logs: LogsClient, settings: EcsSettings, sender: EcsClient | None = None) -> None:
        self.ecs = ecs
        self.logs = logs
        self.settings = settings
        self.sender = ecs if sender is None else sender
        self.at_send: contextvars.ContextVar[Callable[[], None] | None] = contextvars.ContextVar("at_send", default=None)
        events = getattr(getattr(self.sender, "meta", None), "events", None)
        if events is not None:
            # First, so that nothing that answers the send runs before the guard; botocore runs this after signing and any credential refresh.
            events.register_first("before-send.ecs.RunTask", self.check_at_send, unique_id=f"hai-send-guard-{id(self)}")
        if not sends_once(self.sender):
            raise ValueError(
                "the RunTask client must make one attempt per call (build it with send_config): an SDK retry would send again after the caller's guard "
                "and could hide the lost reply of a request that created a task"
            )

    def check_at_send(self, **_: Any) -> None:
        """The guard of the call in progress, run by the SDK right before the request leaves; a refusal stops the call and nothing is sent."""
        guard = self.at_send.get()
        if guard is not None:
            guard()

    def send(self, guard: Callable[[], None] | None, arguments: dict[str, Any]) -> dict[str, Any]:
        """RunTask with ``guard`` run again at the SDK's send boundary. A client without SDK events (a stand-in) is guarded by the call before it only."""
        if guard is None:
            return self.sender.run_task(**arguments)
        refused: list[Exception] = []

        def at_send() -> None:
            try:
                guard()
            except Exception as error:
                refused.append(error)
                raise

        token = self.at_send.set(at_send)
        try:
            return self.sender.run_task(**arguments)
        except Exception:
            if refused:
                raise GuardFailed(refused[0]) from None
            raise
        finally:
            self.at_send.reset(token)

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

    def definition(self, family: str) -> str:
        """The task definition that runs the family."""
        return f"{self.settings.family_prefix}{family}{HOST_SUFFIX}"

    def is_revision(self, family: str, arn: str) -> bool:
        """True when ``arn`` names one revision of the task definition that runs the family."""
        return re.fullmatch(rf"arn:aws[a-z-]*:ecs:[a-z0-9-]+:[0-9]{{12}}:task-definition/{re.escape(self.definition(family))}:[1-9][0-9]*", arn) is not None

    def task_definition(self, family: str) -> dict[str, Any]:
        """The latest active revision of the task definition that runs the family, as ECS describes it: a family name without a revision names that
        revision. A failed call raises ``TaskUnavailable`` or ``TaskRefused``, a reply without a task definition ``TaskUnavailable``."""
        if family not in self.settings.families:
            raise TaskRefused("family_not_named")
        if not isinstance(self.ecs, DefinitionClient):
            raise TaskRefused("task_definition_reader_missing")
        try:
            reply = self.ecs.describe_task_definition(taskDefinition=self.definition(family))
        except (ClientError, BotoCoreError) as error:
            raise classify(error) from None
        found = reply.get("taskDefinition")
        if not isinstance(found, dict):
            raise TaskUnavailable("task_definition_not_described")
        return dict(found)

    def placement(self) -> dict[str, Any]:
        """The ``RunTask`` arguments that place the task on the host capacity provider. A refusal comes before any cloud call."""
        if not self.settings.host_capacity_provider:
            raise TaskRefused("host_provider_not_set")
        return {"capacityProviderStrategy": [{"capacityProvider": self.settings.host_capacity_provider, "weight": 1}]}

    def start(
        self,
        family: str,
        key: str,
        environment: Mapping[str, Mapping[str, str]],
        guard: Callable[[], None] | None = None,
        revision: str | None = None,
    ) -> str:
        """Run the family's task once for ``key`` and return its ARN. ``environment`` maps a container name to the variables it receives. The task has no
        security group: the host firewall bounds it. ``guard`` runs immediately before the RunTask call, after the discovery of a running task, and raises
        ``SendWithheld`` to stop the call. The call is one request with no SDK retry. A RunTask call that may have been sent and has no answer raises
        ``SendUnknown``. ``revision``, when given, is the ARN of the one revision of the family's definition to run, in place of its latest active revision."""
        if family not in self.settings.families:
            raise TaskRefused("family_not_named")
        if revision is not None and not self.is_revision(family, revision):
            raise TaskRefused("revision_not_of_family")
        where = self.placement()
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
            reply = self.send(
                guard,
                dict(
                    cluster=self.settings.cluster,
                    taskDefinition=revision if revision is not None else self.definition(family),
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
                ),
            )
        except GuardFailed as failed:
            raise failed.error from None
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
        reasons = tuple((str(item["name"]), code) for item in task.get("containers", []) if (code := container_reason_code(item.get("reason"))))
        reason = scrub(str(task.get("stoppedReason", "")))
        codes = [code for name, code in containers if name != GUARD]
        if codes and all(code == 0 for code in codes):
            return TaskReport("succeeded", 0, reason, containers, reasons)
        failing = next((code for code in codes if code not in (0, None)), None)
        fallback = USER_STOP_CODE if task.get("stopCode") == "UserInitiated" else NO_EXIT_CODE
        return TaskReport("failed", failing if failing is not None else fallback, reason, containers, reasons)

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
