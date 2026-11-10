"""Stand-ins for the S3 client calls and the clock of the direct ECS launch core, for offline end-to-end cases.

``FakeLaunchBucket`` has the call surface of the boto3 S3 client that ``orchestration.ecs_launch.S3LaunchStore`` uses: ``put_object`` and ``get_object``. It
models the launch role. A write needs ``IfNoneMatch="*"`` and an existing key answers ``PreconditionFailed``. The decision record alone may be replaced, by a
write with ``IfMatch`` on the ETag it was read with. Any key outside the allowed prefix answers ``AccessDenied``. An absent key answers ``NoSuchKey``, as it
does when the role may list the prefix. There is no list call and no delete call, because the library never lists or deletes. Switches model faults:
``absent_reads_denied`` is a role that cannot list (an absent key answers ``AccessDenied``), ``deny_reads`` is a role that lost its read grant, ``deny_keys``
denies the read of single records while writes stay allowed, and ``read_hook`` and ``write_hook`` run another caller at the start of a read or a write.
``FakeClock`` moves time only when a case sleeps, so ten minutes of waiting take no real time. ``RevisionEcs`` is the fake ECS with task definition
revisions: ``register`` makes a family's latest active revision, ``describe_task_definition`` answers with it, and ``run_task`` takes the ARN of a
registered revision as well as a family name.
"""

from __future__ import annotations

import copy
import hashlib
import re
from collections.abc import Callable
from typing import Any

from botocore.exceptions import ReadTimeoutError

from orchestration.ecs_launch import Clock
from scripts.orchestration.ecs_broker_fake import ACCOUNT, REGION, FakeEcs, client_error

START_TIME = 1_800_000_000.0


class FakeClock:
    """Virtual wall and monotonic time that only ``sleep`` moves."""

    def __init__(self) -> None:
        self.now = START_TIME
        self.slept: list[float] = []
        self.on_sleep: Callable[[float], None] | None = None

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds
        if self.on_sleep is not None:
            self.on_sleep(self.now)

    def clock(self) -> Clock:
        return Clock(wall=lambda: self.now, monotonic=lambda: self.now, sleep=self.sleep)


REVISION_ARN = re.compile(r"arn:aws:ecs:[a-z0-9-]+:[0-9]{12}:task-definition/([A-Za-z0-9_-]+):([0-9]+)")


class RevisionEcs(FakeEcs):
    """The fake ECS with task definition revisions. ``register`` adds a revision of a family and makes it the latest active one, which
    ``describe_task_definition`` answers for the family name, as ECS does. ``run_task`` takes a family name or the ARN of a registered revision, refuses
    any other ARN as ECS does, and runs the revision under its family. ``sent`` lists the task definition of every RunTask call as the caller sent it."""

    def __init__(self) -> None:
        super().__init__()
        self.revisions: dict[str, dict[str, Any]] = {}
        self.registered: set[str] = set()
        self.sent: list[str] = []

    def register(self, family: str, memory: int, label: str | None, cpu: int = 3072) -> str:
        """Register a revision of ``family`` with the task memory and the capacity label of its init container (none when ``label`` is None)."""
        number = 1 + sum(1 for arn in self.registered if arn.rsplit("/", 1)[-1].rsplit(":", 1)[0] == family)
        arn = f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{family}:{number}"
        init: dict[str, Any] = {"name": "init"}
        if label is not None:
            init["dockerLabels"] = {"hai.host_capacity": label}
        self.registered.add(arn)
        self.revisions[family] = {
            "taskDefinitionArn": arn,
            "family": family,
            "revision": number,
            "status": "ACTIVE",
            "cpu": str(cpu),
            "memory": str(memory),
            "containerDefinitions": [init, {"name": "work"}],
        }
        return arn

    def describe_task_definition(self, **arguments: Any) -> dict[str, Any]:
        self.calls.append(("describe_task_definition", dict(arguments)))  # the call names no cluster, so the cluster check of _enter does not apply
        queue = self.faults.get("describe_task_definition")
        if queue:
            fault = queue.pop(0)
            if fault is not None:
                raise fault
        found = self.revisions.get(str(arguments.get("taskDefinition")))
        if found is None:
            raise client_error("ClientException", "DescribeTaskDefinition", "Unable to describe task definition.")
        return {"taskDefinition": copy.deepcopy(found)}

    def run_task(self, **arguments: Any) -> dict[str, Any]:
        name = str(arguments.get("taskDefinition"))
        self.sent.append(name)
        match = REVISION_ARN.fullmatch(name)
        if match is not None:
            if name not in self.registered:
                raise client_error("ClientException", "RunTask", "TaskDefinition not found.")
            arguments = {**arguments, "taskDefinition": match.group(1)}
        return super().run_task(**arguments)


class _Body:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def read(self) -> bytes:
        return self.data


class FakeLaunchBucket:
    """One bucket with write-once objects and queued faults per operation."""

    def __init__(self, bucket: str, allowed_prefix: str) -> None:
        self.bucket = bucket
        self.allowed_prefix = allowed_prefix
        self.objects: dict[str, bytes] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.faults: dict[str, list[Exception]] = {}
        self.put_faults: dict[str, list[Exception]] = {}  # queued errors for a write whose key ends with the name, before anything is stored
        self.lose_reply_once = False
        self.read_hook: Callable[[str], None] | None = None  # called with the key at the start of every read
        self.write_hook: Callable[[str], None] | None = None  # called with the key at the start of every write
        self.deny_reads = False
        self.absent_reads_denied = False
        self.deny_keys: set[str] = set()

    def _enter(self, operation: str, arguments: dict[str, Any], key: str) -> None:
        self.calls.append((operation, dict(arguments)))
        if arguments.get("Bucket") != self.bucket or not key.startswith(self.allowed_prefix):
            raise client_error("AccessDenied", operation)
        queue = self.faults.get(operation)
        if queue:
            raise queue.pop(0)

    def count(self, operation: str) -> int:
        return sum(1 for name, _ in self.calls if name == operation)

    @staticmethod
    def etag(body: bytes) -> str:
        return '"' + hashlib.sha256(body).hexdigest() + '"'

    def put_object(self, **arguments: Any) -> dict[str, Any]:
        self._enter("put_object", arguments, arguments["Key"])
        if self.write_hook is not None:
            self.write_hook(arguments["Key"])
        for suffix, queue in self.put_faults.items():
            if arguments["Key"].endswith(suffix) and queue:
                raise queue.pop(0)
        if "IfMatch" in arguments:
            if not arguments["Key"].endswith("/decision.json") or "IfNoneMatch" in arguments:
                raise client_error("AccessDenied", "put_object", "only the decision record may be replaced, and only by a version-checked write")
            current = self.objects.get(arguments["Key"])
            if current is None or self.etag(current) != arguments["IfMatch"]:
                raise client_error("PreconditionFailed", "put_object")
            self.objects[arguments["Key"]] = arguments["Body"] if isinstance(arguments["Body"], bytes) else str(arguments["Body"]).encode()
            return {"ETag": self.etag(self.objects[arguments["Key"]])}
        if arguments.get("IfNoneMatch") != "*":
            raise client_error("AccessDenied", "put_object", "an unconditional write is not allowed under the launch prefix")
        if arguments["Key"] in self.objects:
            raise client_error("PreconditionFailed", "put_object")
        body = arguments["Body"]
        self.objects[arguments["Key"]] = body if isinstance(body, bytes) else str(body).encode()
        if self.lose_reply_once:
            self.lose_reply_once = False
            raise ReadTimeoutError(endpoint_url="https://s3.invalid")
        return {"ETag": '"stand-in"'}

    def list_objects_v2(self, **arguments: Any) -> dict[str, Any]:
        """The keys under a prefix inside the allowed prefix, in key order, ``MaxKeys`` (at most 1000) at a time, as S3 lists them."""
        self._enter("list_objects_v2", arguments, arguments.get("Prefix", ""))
        keys = sorted(key for key in self.objects if key.startswith(arguments.get("Prefix", "")))
        start = int(arguments.get("ContinuationToken", "0"))
        size = min(int(arguments.get("MaxKeys", 1000)), 1000)
        reply: dict[str, Any] = {"Contents": [{"Key": key} for key in keys[start : start + size]], "IsTruncated": start + size < len(keys)}
        if reply["IsTruncated"]:
            reply["NextContinuationToken"] = str(start + size)
        return reply

    def get_object(self, **arguments: Any) -> dict[str, Any]:
        self._enter("get_object", arguments, arguments["Key"])
        if self.read_hook is not None:
            self.read_hook(arguments["Key"])
        if self.deny_reads or arguments["Key"] in self.deny_keys:
            raise client_error("AccessDenied", "get_object")
        if arguments["Key"] not in self.objects:
            raise client_error("AccessDenied" if self.absent_reads_denied else "NoSuchKey", "get_object")
        return {"Body": _Body(self.objects[arguments["Key"]]), "ETag": self.etag(self.objects[arguments["Key"]])}
