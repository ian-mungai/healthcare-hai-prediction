"""In-memory stand-ins of the three AWS services the catalog setup task uses besides Parameter Store: S3 with conditional writes, the launch hold parameter
and the ECS task listing. The catalog setup E2E uses them; they speak the client calls and error codes of ``catalog_setup_state`` and nothing else.
"""

from __future__ import annotations

import hashlib
import io
import threading
import time
from collections.abc import Callable
from typing import Any

from botocore.exceptions import ClientError, ConnectionClosedError, ReadTimeoutError

from scripts.lakehouse.catalog_setup_state import client_options

# A synthetic account ID, built at run time so no file holds a twelve-digit number.
ACCOUNT = "".join(["1"] * 12)


def refusal(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "refused"}}, operation)


class FakeS3:
    """A bucket whose writes honour ``IfNoneMatch="*"`` and ``IfMatch``; ``denied`` keys answer AccessDenied, ``fail_put`` makes every write fail."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.denied: set[str] = set()
        self.fail_put = False
        self.versions = 0
        self.guard = threading.Lock()

    def version(self, body: bytes) -> str:
        self.versions += 1
        return f'"{hashlib.md5(body + str(self.versions).encode(), usedforsecurity=False).hexdigest()}"'

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        key, body = kwargs["Key"], kwargs["Body"]
        with self.guard:
            self.calls.append(("put", key, {name: kwargs[name] for name in ("IfNoneMatch", "IfMatch") if name in kwargs}))
            if self.fail_put or key in self.denied:
                raise refusal("AccessDenied", "PutObject")
            if kwargs.get("IfNoneMatch") == "*" and key in self.objects:
                raise refusal("PreconditionFailed", "PutObject")
            if "IfMatch" in kwargs and (key not in self.objects or self.objects[key][1] != kwargs["IfMatch"]):
                raise refusal("PreconditionFailed", "PutObject")
            etag = self.version(body)
            self.objects[key] = (body, etag)
            return {"ETag": etag}

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        key = kwargs["Key"]
        with self.guard:
            self.calls.append(("get", key, {}))
            if key in self.denied:
                raise refusal("AccessDenied", "GetObject")
            if key not in self.objects:
                raise refusal("NoSuchKey", "GetObject")
            body, etag = self.objects[key]
            return {"Body": io.BytesIO(body), "ETag": etag}

    def delete_object(self, **kwargs: Any) -> dict[str, Any]:
        key = kwargs["Key"]
        with self.guard:
            self.calls.append(("delete", key, {name: kwargs[name] for name in ("IfMatch",) if name in kwargs}))
            if key in self.denied:
                raise refusal("AccessDenied", "DeleteObject")
            if "IfMatch" in kwargs and (key not in self.objects or self.objects[key][1] != kwargs["IfMatch"]):
                raise refusal("PreconditionFailed", "DeleteObject")
            self.objects.pop(key, None)
            return {}


class FakeHold:
    """The launch hold parameter: a create without overwrite is refused while it exists."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.calls: list[tuple[str, str]] = []
        self.fail_delete = False
        self.drop_reply = False

    def put_parameter(self, **kwargs: Any) -> dict[str, Any]:
        name = kwargs["Name"]
        self.calls.append(("put", name))
        if kwargs.get("Overwrite") is not False or kwargs.get("Type") != "String":
            raise refusal("ValidationException", "PutParameter")
        if name in self.values:
            raise refusal("ParameterAlreadyExists", "PutParameter")
        self.values[name] = kwargs["Value"]
        if self.drop_reply:
            raise ConnectionClosedError(endpoint_url="stand-in")
        return {"Version": 1}

    def get_parameter(self, **kwargs: Any) -> dict[str, Any]:
        name = kwargs["Name"]
        self.calls.append(("get", name))
        if name not in self.values:
            raise refusal("ParameterNotFound", "GetParameter")
        return {"Parameter": {"Name": name, "Value": self.values[name]}}

    def delete_parameter(self, **kwargs: Any) -> dict[str, Any]:
        name = kwargs["Name"]
        self.calls.append(("delete", name))
        if self.fail_delete:
            raise refusal("AccessDeniedException", "DeleteParameter")
        self.values.pop(name, None)
        return {}


class FakeClock:
    """A clock that moves only when something sleeps, and runs the events that fall due while it moves."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.events: list[tuple[float, Callable[[], None]]] = []

    def __call__(self) -> float:
        return self.now

    def at(self, seconds: float, action: Callable[[], None]) -> None:
        """Run ``action`` once the clock has moved ``seconds`` from now."""
        self.events.append((self.now + seconds, action))

    def sleep(self, seconds: float) -> None:
        target = self.now + seconds
        for due, action in sorted(self.events, key=lambda item: item[0]):
            if due <= target:
                self.now = max(self.now, due)
                self.events.remove((due, action))
                action()
        self.now = target


class TimedClients:
    """The ``clients(service, seconds_left)`` factory of the state code: it records each call with the time left it was given and the read limit that
    ``client_options`` derives from it and, for a service with a latency above that limit, waits the limit (``block``) and answers with a read timeout. ``hang``
    makes every call of a service wait that long first, whatever the limits."""

    def __init__(self, **services: Any) -> None:
        self.services = services
        self.calls: list[tuple[str, str, float, float]] = []
        self.latency: dict[str, float] = {}
        self.hang: dict[str, float] = {}
        self.block = True

    def __call__(self, service: str, left: float) -> Any:
        owner = self

        class Client:
            def __getattr__(self, operation: str) -> Callable[..., Any]:
                target = getattr(owner.services[service], operation)

                def call(**arguments: Any) -> Any:
                    # Work that no socket timeout bounds, such as a credential refresh or a name lookup, before the request is made.
                    time.sleep(owner.hang.get(service, 0.0))
                    read = float(client_options(left)["read_timeout"])
                    owner.calls.append((service, operation, left, read))
                    if owner.latency.get(service, 0.0) > read:
                        if owner.block:
                            time.sleep(read)
                        raise ReadTimeoutError(endpoint_url="stand-in")
                    return target(**arguments)

                return call

        return Client()


class FakeEcs:
    """The tasks of one cluster. ``tasks`` are task dicts as DescribeTasks answers them; ``fail`` makes the listing fail."""

    def __init__(self) -> None:
        self.tasks: list[dict[str, Any]] = []
        self.fail = False
        self.calls: list[str] = []
        self.missing: set[str] = set()
        self.silent: set[str] = set()
        self.lag: dict[str, int] = {}

    def add(self, family: str, last_status: str, desired: str = "RUNNING") -> str:
        """Add a task of ``family`` and answer its ARN."""
        number = len(self.tasks) + 1
        arn = f"arn:aws:ecs:us-west-2:{ACCOUNT}:task/cluster/{number:032x}"
        self.tasks.append(
            {
                "taskArn": arn,
                "group": f"family:{family}",
                "taskDefinitionArn": f"arn:aws:ecs:us-west-2:{ACCOUNT}:task-definition/{family}:1",
                "lastStatus": last_status,
                "desiredStatus": desired,
            }
        )
        return arn

    def list_tasks(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("list")
        if self.fail:
            raise refusal("AccessDeniedException", "ListTasks")
        return {"taskArns": [task["taskArn"] for task in self.tasks if task["desiredStatus"] == kwargs["desiredStatus"]]}

    def describe_tasks(self, **kwargs: Any) -> dict[str, Any]:
        """Tasks as DescribeTasks answers them. An ARN in ``missing`` is answered as a MISSING failure, one in ``silent`` is left out of both lists, and one
        in ``lag`` is a MISSING failure for that many calls before it is described."""
        self.calls.append("describe")
        found, failures = [], []
        for task in self.tasks:
            arn = task["taskArn"]
            if arn not in kwargs["tasks"] or arn in self.silent:
                continue
            if arn in self.missing or self.lag.get(arn, 0) > 0:
                self.lag[arn] = max(self.lag.get(arn, 0) - 1, 0)
                failures.append({"arn": arn, "reason": "MISSING"})
            else:
                found.append(task)
        return {"tasks": found, "failures": failures}
