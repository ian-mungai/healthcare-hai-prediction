"""Stand-ins for the ECS, CloudWatch Logs and S3 client calls of the direct ECS launch, for offline end-to-end cases.

``FakeEcs`` has the call surface of the boto3 ECS client that ``orchestration.ecs_adapter`` uses: ``run_task``, ``describe_tasks``, ``stop_task`` and
``list_tasks``. It enforces the request rules the service documents: one task per call, a client token of at most 64 printable characters (a repeat of
a token returns the first task), ``startedBy`` of at most 128 characters, and no ``startedBy`` together with ``desiredStatus`` in ``list_tasks``. A task moves
on each ``describe_tasks``: provisioning, then running, then stopped with the exit codes planned for it. It never leaves the machine.

The cluster registers one capacity provider, the EC2 provider ``host_provider``, and it is the cluster default. A definition whose name ends in ``_host`` is a
bridge task for it: ECS refuses an awsvpc network for it and a strategy that names another provider. Any other definition is an awsvpc task, which no provider
of the cluster can run. ``host_full`` makes the host answer ``RESOURCE:CPU``, as ECS does when the instance has no room. ``describe_overrides`` replaces the
next DescribeTasks replies, for example with one that names no task while the task runs. ``definitions`` names the containers of a task definition: ECS
refuses an override for a container that the definition lacks, and so does the fake for each definition named there (a definition not named there takes any
override).

``SdkStub`` is a different kind of stand-in: a real botocore client whose HTTP layer alone is replaced by scripted answers, so the SDK's own retry handling
runs as it does against AWS. It sends nothing over the network: every request is answered by the ``before-send`` event, and the endpoint is a reserved
``.invalid`` host.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

import botocore.session
from botocore.awsrequest import AWSResponse
from botocore.compat import HTTPHeaders
from botocore.config import Config
from botocore.exceptions import ClientError, ReadTimeoutError

ACCOUNT = "111111111111"
CLUSTER = "stand-in-cluster"
REGION = "stand-in-region"
FINISH_AFTER = 3
HOST_SUFFIX = "_host"
STOPPED = "STOPPED"
STAND_IN_CREDENTIAL = "stand-in"  # signs the requests of SdkStub, which never leave the process


def client_error(code: str, operation: str, message: str = "stand-in refusal") -> ClientError:
    """The error a botocore client raises for an API refusal."""
    return ClientError({"Error": {"Code": code, "Message": message}}, operation)


STARTED_BY = re.compile(r"[A-Za-z0-9/_-]{0,128}")
TASK_DEFINITION = re.compile(r"[A-Za-z0-9_-]{1,255}(:[0-9]+)?|arn:aws:ecs:[a-z0-9-]+:[0-9]{12}:task-definition/[A-Za-z0-9_-]{1,255}:[0-9]+")
LAUNCH_TYPES = frozenset({"EC2", "EXTERNAL", "MANAGED_INSTANCES"})
PROPAGATE_TAGS = frozenset({"TASK_DEFINITION", "NONE"})  # RunTask refuses SERVICE, which only a service may set
OVERRIDES_LIMIT = 8192  # characters of the overrides JSON, the formatting included (the bundled RunTask model)


def refuse_run_task(message: str) -> ClientError:
    return client_error("InvalidParameterException", "RunTask", message)


def check_run_task(arguments: Mapping[str, Any]) -> None:
    """Refuse a RunTask request as ECS refuses it, for the fields the launch library sends (the rules are those of the bundled ECS service model): a launch
    type and a capacity provider strategy exclude each other, a strategy has at most six providers with weights 0 to 1000 (one above 0) and bases 0 to
    100000, ``propagateTags`` is TASK_DEFINITION or NONE, the overrides hold at most 8192 characters of JSON and name each container once, ``startedBy``
    holds only letters, digits, hyphens, slashes and underscores, there are at most 50 tags with unique keys, and the task definition is a family with an
    optional revision or an ARN. Raises the error ECS answers."""
    if "launchType" in arguments and arguments.get("capacityProviderStrategy"):
        raise refuse_run_task("Specifying both a launch type and capacity provider strategy is not supported. Remove one and try again.")
    if "launchType" in arguments and arguments["launchType"] not in LAUNCH_TYPES:
        raise refuse_run_task(f"Unknown launch type {arguments['launchType']}.")
    strategy = arguments.get("capacityProviderStrategy") or []
    if len(strategy) > 6 or any(
        not item.get("capacityProvider") or not 0 <= item.get("weight", 0) <= 1000 or not 0 <= item.get("base", 0) <= 100000 for item in strategy
    ):
        raise refuse_run_task("The capacity provider strategy is not valid.")
    if strategy and not any(item.get("weight", 0) > 0 for item in strategy):
        raise refuse_run_task("At least one capacity provider in a strategy must have a weight greater than zero.")
    if not TASK_DEFINITION.fullmatch(str(arguments.get("taskDefinition", ""))):
        raise refuse_run_task("The task definition is not a family name with an optional revision, or an ARN.")
    if not STARTED_BY.fullmatch(str(arguments.get("startedBy", ""))):
        raise refuse_run_task("startedBy allows only letters, numbers, hyphens, slashes and underscores, up to 128 characters.")
    if arguments.get("propagateTags", "NONE") not in PROPAGATE_TAGS:
        raise refuse_run_task("propagateTags is not a valid value.")
    overrides = arguments.get("overrides") or {}
    if len(json.dumps(overrides, separators=(",", ":"))) > OVERRIDES_LIMIT:
        raise refuse_run_task(f"Container overrides length must be at most {OVERRIDES_LIMIT}.")
    names = [item.get("name") for item in overrides.get("containerOverrides", [])]
    if any(not name for name in names) or len(set(names)) != len(names):
        raise refuse_run_task("Each container override names one container, once.")
    for item in overrides.get("containerOverrides", []):
        if any(not isinstance(pair.get("name"), str) or not pair["name"] or not isinstance(pair.get("value"), str) for pair in item.get("environment", [])):
            raise refuse_run_task("An environment override needs a name and a string value.")
    tags = arguments.get("tags") or []
    keys = [item.get("key") for item in tags]
    if (
        len(tags) > 50
        or len(set(keys)) != len(keys)
        or any(not key or len(key) > 128 or len(item.get("value", "")) > 256 for key, item in zip(keys, tags, strict=True))
    ):
        raise refuse_run_task("The tags are not valid: at most 50, unique keys of 1 to 128 characters and values of at most 256.")


class FakeEcs:
    """Keeps the tasks of one stand-in cluster; ``faults`` queues an error for the next call of an operation."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.tasks: dict[str, dict[str, Any]] = {}
        self.tokens: dict[str, str] = {}
        self.token_made: dict[str, float] = {}
        self.faults: dict[str, list[Exception | None]] = {}
        self.lose_reply_once = False
        self.hold = False
        self.plan: dict[str, int | None] = {}
        self.omit: set[str] = set()
        self.finish_after = FINISH_AFTER
        self.host_provider = "stand-in_host"  # the registered EC2 capacity provider of the cluster
        self.host_full = False
        self.clock: Callable[[], float] | None = None  # when set, a new task carries createdAt from it
        self.run_task_hook: Callable[[str], None] | None = None  # called with "before" and "after" around the creation of a task, to interleave other callers
        self.token_ttl: float | None = None  # seconds a client token is honored, from the creation of its task
        self.call_hook: Callable[[str], None] | None = None  # called with the operation name at the start of every call, to interleave other callers
        self.visible_delay: float | None = None  # seconds before ECS lists a new task: the service documents no listing guarantee
        self.purge_after: float | None = None  # seconds after which ECS no longer lists a stopped task
        self.forget_stopped_tokens = False  # worst case for a repeated client token after the task stopped, which the service documentation leaves open
        self.list_page = 100  # tasks per ListTasks page
        self.instance_free = {"CPU": 4096, "MEMORY": 31000}  # what the host's container instance reports as remaining
        self.stop_lag = 0  # describe calls a stopped task keeps answering RUNNING after StopTask accepted the request, as ECS does while it shuts down
        self.describe_overrides: list[dict[str, Any] | None] = []  # replies for the next describe_tasks calls, in order (None lets a call through)
        self.definitions: dict[str, frozenset[str]] = {}  # the containers of a task definition, for the definitions a case names

    def count(self, operation: str) -> int:
        return sum(1 for name, _ in self.calls if name == operation)

    def _enter(self, operation: str, arguments: Mapping[str, Any]) -> None:
        self.calls.append((operation, dict(arguments)))
        if arguments.get("cluster") != CLUSTER:
            raise client_error("ClusterNotFoundException", operation)
        if self.call_hook is not None:
            self.call_hook(operation)
        queue = self.faults.get(operation)
        if queue:
            fault = queue.pop(0)
            if fault is not None:  # None lets this call through, to interleave failing and working calls
                raise fault

    def _check_shape(self, arguments: Mapping[str, Any]) -> None:
        """Refuse what ECS refuses for the task definition's shape: only a bridge task of the host provider can run in this cluster."""
        on_host = str(arguments["taskDefinition"]).endswith(HOST_SUFFIX)
        strategy = arguments.get("capacityProviderStrategy") or [{"capacityProvider": self.host_provider, "weight": 1}]
        providers = {str(item["capacityProvider"]) for item in strategy}
        if providers != {self.host_provider}:
            raise client_error("InvalidParameterException", "RunTask", "The capacity provider was not found in the cluster.")
        if on_host and "networkConfiguration" in arguments:
            raise client_error("InvalidParameterException", "RunTask", "Network configuration is not valid for a bridge task definition.")
        if not on_host:
            raise client_error("InvalidParameterException", "RunTask", "The task definition is not compatible with the capacity provider.")

    def run_task(self, **arguments: Any) -> dict[str, Any]:
        self._enter("run_task", arguments)
        check_run_task(arguments)
        token = arguments.get("clientToken", "")
        started_by = arguments.get("startedBy", "")
        if arguments.get("count") != 1 or not 1 <= len(token) <= 64 or len(started_by) > 128 or not all(33 <= ord(c) <= 126 for c in token):
            raise client_error("InvalidParameterException", "RunTask")
        self._check_shape(arguments)
        known = self.definitions.get(str(arguments["taskDefinition"]))
        lacking = sorted({item["name"] for item in arguments.get("overrides", {}).get("containerOverrides", [])} - known) if known is not None else []
        if lacking:
            raise client_error("InvalidParameterException", "RunTask", f"Override for container named {lacking[0]} is not a container in the TaskDefinition.")
        expired = self.token_ttl is not None and self.clock is not None and token in self.tokens and self.clock() - self.token_made[token] > self.token_ttl
        if token in self.tokens and not expired and not (self.forget_stopped_tokens and self.tasks[self.tokens[token]]["lastStatus"] == STOPPED):
            return {"tasks": [self.tasks[self.tokens[token]]], "failures": []}
        if self.host_full:
            return {"tasks": [], "failures": [{"arn": f"arn:aws:ecs:{REGION}:{ACCOUNT}:container-instance/{CLUSTER}/stand-in", "reason": "RESOURCE:CPU"}]}
        if self.run_task_hook is not None:
            self.run_task_hook("before")
        names = [item["name"] for item in arguments.get("overrides", {}).get("containerOverrides", [])]
        names = ["init", *names] if str(arguments["taskDefinition"]).removesuffix(HOST_SUFFIX).endswith("_train") else names
        digest = hashlib.sha256(token.encode() + (b"again%d" % len(self.tasks) if token in self.tokens else b"")).hexdigest()[:32]
        arn = f"arn:aws:ecs:{REGION}:{ACCOUNT}:task/{CLUSTER}/{digest}"
        task = {
            "taskArn": arn,
            "taskDefinitionArn": f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{arguments['taskDefinition']}:1",
            "startedBy": started_by,
            "lastStatus": "PROVISIONING",
            "desiredStatus": "RUNNING",
            "containers": [{"name": name, "lastStatus": "PENDING"} for name in names if name not in self.omit],
            "polls": 0,
            "request": dict(arguments),
        }
        if self.clock is not None:
            task["createdAt"] = datetime.fromtimestamp(self.clock(), UTC)
            task["createdClock"] = self.clock()
        task.update(
            launchType="EC2",
            capacityProviderName=self.host_provider,
            containerInstanceArn=f"arn:aws:ecs:{REGION}:{ACCOUNT}:container-instance/{CLUSTER}/stand-in",
        )
        self.tasks[arn] = task
        self.tokens[token] = arn
        if self.clock is not None:
            self.token_made[token] = self.clock()
        if self.run_task_hook is not None:
            self.run_task_hook("after")
        if self.lose_reply_once:
            self.lose_reply_once = False
            raise ReadTimeoutError(endpoint_url="https://ecs.invalid")
        return {"tasks": [self._view(task)], "failures": []}

    def list_tasks(self, **arguments: Any) -> dict[str, Any]:
        self._enter("list_tasks", arguments)
        if "startedBy" in arguments and "desiredStatus" in arguments:
            raise client_error("InvalidParameterException", "ListTasks")
        wanted = arguments.get("desiredStatus", "RUNNING")
        found = [
            arn
            for arn, task in self.tasks.items()
            if ("startedBy" not in arguments or task["startedBy"] == arguments["startedBy"])
            and ("family" not in arguments or str(task["request"]["taskDefinition"]).split(":")[0] == arguments["family"])
            and task["desiredStatus"] == wanted
            and self._listed(task)
        ]
        start = int(arguments.get("nextToken", "0"))
        reply: dict[str, Any] = {"taskArns": found[start : start + self.list_page]}
        if start + self.list_page < len(found):
            reply["nextToken"] = str(start + self.list_page)
        return reply

    def describe_tasks(self, **arguments: Any) -> dict[str, Any]:
        self._enter("describe_tasks", arguments)
        if self.describe_overrides:
            override = self.describe_overrides.pop(0)
            if override is not None:  # ECS documents no guarantee that a task it created is described, so a reply may name no task at all
                return override
        tasks, failures = [], []
        for arn in arguments["tasks"]:
            task = self.tasks.get(arn)
            if task is None or not self._listed(task):
                failures.append({"arn": arn, "reason": "MISSING"})
                continue
            task["polls"] += 1
            if task.get("stopping") is not None:
                task["stopping"] -= 1
                if task["stopping"] <= 0:
                    self._finish(task, {item["name"]: 137 for item in task["containers"]}, "Task stopped by user", "UserInitiated")
                tasks.append(self._view(task))
                continue
            if task["lastStatus"] != "STOPPED" and not self.hold and task["polls"] >= self.finish_after:
                self._finish(task, self.plan, "Essential container in task exited")
            elif task["lastStatus"] == "PROVISIONING" and task["polls"] >= 2:
                task["lastStatus"] = "RUNNING"
            tasks.append(self._view(task))
        return {"tasks": tasks, "failures": failures}

    def list_container_instances(self, **arguments: Any) -> dict[str, Any]:
        self._enter("list_container_instances", arguments)
        return {"containerInstanceArns": [f"arn:aws:ecs:{REGION}:{ACCOUNT}:container-instance/{CLUSTER}/stand-in"]}

    def describe_container_instances(self, **arguments: Any) -> dict[str, Any]:
        self._enter("describe_container_instances", arguments)
        remaining = [{"name": name, "type": "INTEGER", "integerValue": value} for name, value in self.instance_free.items()]
        return {
            "containerInstances": [
                {"containerInstanceArn": arn, "status": "ACTIVE", "remainingResources": remaining} for arn in arguments["containerInstances"]
            ],
            "failures": [],
        }

    def finish(self, arn: str, exits: Mapping[str, int | None] | None = None) -> None:
        """End a task now, as if its containers exited with ``exits`` (a container not named exits 0)."""
        self._finish(self.tasks[arn], exits or {}, "Essential container in task exited")

    def stop_task(self, **arguments: Any) -> dict[str, Any]:
        self._enter("stop_task", arguments)
        task = self.tasks.get(arguments["task"])
        if task is None:
            raise client_error("InvalidParameterException", "StopTask", "The referenced task was not found.")
        if task["lastStatus"] != "STOPPED" and self.stop_lag:
            task["desiredStatus"] = "STOPPED"
            task["stopping"] = self.stop_lag
        elif task["lastStatus"] != "STOPPED":
            self._finish(task, {item["name"]: 137 for item in task["containers"]}, f"Task stopped by user: {arguments.get('reason', '')}", "UserInitiated")
        return {"task": self._view(task)}

    def _listed(self, task: Mapping[str, Any]) -> bool:
        """ECS lists a new task only after a delay and stops listing a stopped task after a while."""
        if self.clock is not None and self.visible_delay is not None and self.clock() - task.get("createdClock", 0.0) < self.visible_delay:
            return False
        if self.purge_after is None or self.clock is None or task.get("lastStatus") != STOPPED or "stoppedClock" not in task:
            return True
        return bool(self.clock() - task["stoppedClock"] <= self.purge_after)

    def _finish(self, task: dict[str, Any], exits: Mapping[str, int | None], reason: str, code: str = "EssentialContainerExited") -> None:
        task.update(lastStatus="STOPPED", desiredStatus="STOPPED", stoppedReason=reason, stopCode=code)
        if self.clock is not None:
            task["stoppedClock"] = self.clock()
        for container in task["containers"]:
            code_of = exits.get(container["name"], 0)
            container["lastStatus"] = "STOPPED"
            if code_of is not None:
                container["exitCode"] = code_of

    @staticmethod
    def _view(task: Mapping[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in task.items() if key not in ("polls", "request", "stopping", "stoppedClock", "createdClock")}


class FakeLogs:
    """CloudWatch Logs events by stream name."""

    def __init__(self) -> None:
        self.streams: dict[str, list[str]] = {}
        self.calls: list[dict[str, Any]] = []

    def get_log_events(self, **arguments: Any) -> dict[str, Any]:
        self.calls.append(dict(arguments))
        lines = self.streams.get(arguments["logStreamName"])
        if lines is None:
            raise client_error("ResourceNotFoundException", "GetLogEvents")
        return {"events": [{"message": line} for line in lines[-arguments["limit"] :]]}


class FakeS3:
    """Small S3 objects by key, for the code digest sidecar."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.reads: list[str] = []
        self.after_read: dict[str, bytes] = {}

    def get_object(self, **arguments: Any) -> dict[str, Any]:
        self.reads.append(arguments["Key"])
        body = self.objects.get(arguments["Key"])
        if body is None:
            raise client_error("NoSuchKey", "GetObject")
        if arguments["Key"] in self.after_read:
            self.objects[arguments["Key"]] = self.after_read.pop(arguments["Key"])
        return {"Body": _Body(body)}


class _Body:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def read(self, limit: int | None = None) -> bytes:
        return self.data if limit is None else self.data[:limit]


class _Raw:
    """The raw body of a scripted HTTP reply, as botocore reads it."""

    def __init__(self, data: bytes) -> None:
        self.data = data

    def stream(self, **_: Any) -> Any:
        yield self.data


class SdkStub:
    """A boto3-like session whose clients are real botocore clients with scripted HTTP answers, so the SDK's retry handler runs offline.

    ``answers`` maps an operation name (``RunTask``) to the answers of its successive HTTP requests; the last one repeats. An answer is a dict (a 200 reply
    body), an exception (raised as the transport failure, for example ``ReadTimeoutError`` after the request reached the service) or a ``(code, status)``
    pair (an error reply). ``sent`` lists every operation that reached the HTTP layer, and ``on_send`` runs with the operation name before it is answered.
    An operation without an answer fails the case loudly.
    """

    def __init__(self, answers: Mapping[str, list[Any]]) -> None:
        self.answers = {name: list(items) for name, items in answers.items()}
        self.sent: list[str] = []
        self.on_send: Callable[[str], None] | None = None

    def client(self, service: str, config: Config | None = None) -> Any:
        session = botocore.session.Session()
        client = session.create_client(
            service,
            region_name="us-east-1",
            endpoint_url=f"https://{service}.stand-in.invalid",
            aws_access_key_id=STAND_IN_CREDENTIAL,
            aws_secret_access_key=STAND_IN_CREDENTIAL,
            config=config,
        )
        client.meta.events.register(f"before-send.{client.meta.service_model.service_id.hyphenize()}", self._answer)
        return client

    def _answer(self, request: Any, event_name: str, **_: Any) -> Any:
        """Answer one send; botocore's event hooks are typed as returning None, but a before-send hook returns the response."""
        operation = event_name.rsplit(".", 1)[-1]
        self.sent.append(operation)
        if self.on_send is not None:
            self.on_send(operation)
        queue = self.answers.get(operation)
        if not queue:
            raise AssertionError(f"the SDK stub has no answer for {operation}")
        answer = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(answer, BaseException):
            raise answer
        status, body = (200, answer) if isinstance(answer, dict) else (answer[1], {"__type": answer[0], "message": "stand-in refusal"})
        headers = HTTPHeaders()
        headers["Content-Type"] = "application/x-amz-json-1.1"
        headers["x-amzn-RequestId"] = "stand-in"
        return AWSResponse(request.url, status, headers, _Raw(json.dumps(body, default=str).encode()))
