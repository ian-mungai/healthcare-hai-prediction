"""Stand-ins for the S3 client calls and the clock of the direct ECS launch core, for offline end-to-end cases.

``FakeLaunchBucket`` has the call surface of the boto3 S3 client that ``orchestration.ecs_launch.S3LaunchStore`` uses: ``put_object`` and ``get_object``. It
models the launch role: a write needs ``IfNoneMatch="*"`` and an existing key answers ``PreconditionFailed``; any key outside the allowed prefix answers
``AccessDenied``; an absent key answers ``NoSuchKey``, as it does when the role may list the prefix. It has no list call and no delete call, because the
library never lists or deletes. Three switches model faults: ``absent_reads_denied`` is a role that cannot list (an absent key answers ``AccessDenied``),
``deny_reads`` is a role that lost its read grant, and ``deny_keys`` denies the read of single records while writes stay allowed. ``FakeClock`` moves time
only when a case sleeps, so ten minutes of waiting take no real time.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from botocore.exceptions import ReadTimeoutError

from orchestration.ecs_launch import Clock
from scripts.orchestration.ecs_broker_fake import client_error

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

    def put_object(self, **arguments: Any) -> dict[str, Any]:
        self._enter("put_object", arguments, arguments["Key"])
        if arguments.get("IfNoneMatch") != "*":
            raise client_error("AccessDenied", "put_object", "an unconditional write is not allowed under the launch prefix")
        for suffix, queue in self.put_faults.items():
            if arguments["Key"].endswith(suffix) and queue:
                raise queue.pop(0)
        if arguments["Key"] in self.objects:
            raise client_error("PreconditionFailed", "put_object")
        body = arguments["Body"]
        self.objects[arguments["Key"]] = body if isinstance(body, bytes) else str(body).encode()
        if self.lose_reply_once:
            self.lose_reply_once = False
            raise ReadTimeoutError(endpoint_url="https://s3.invalid")
        return {"ETag": '"stand-in"'}

    def get_object(self, **arguments: Any) -> dict[str, Any]:
        self._enter("get_object", arguments, arguments["Key"])
        if self.deny_reads or arguments["Key"] in self.deny_keys:
            raise client_error("AccessDenied", "get_object")
        if arguments["Key"] not in self.objects:
            raise client_error("AccessDenied" if self.absent_reads_denied else "NoSuchKey", "get_object")
        return {"Body": _Body(self.objects[arguments["Key"]])}
