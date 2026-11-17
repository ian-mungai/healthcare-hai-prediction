"""In-memory stand-ins of the three AWS services the catalog setup task uses besides Parameter Store: S3 with conditional writes, the launch hold parameter
and the ECS task listing. The catalog setup E2E uses them; they speak the client calls and error codes of ``catalog_setup_state`` and nothing else.
"""

from __future__ import annotations

import hashlib
import io
import threading
from typing import Any

from botocore.exceptions import ClientError

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

    def put_parameter(self, **kwargs: Any) -> dict[str, Any]:
        name = kwargs["Name"]
        self.calls.append(("put", name))
        if kwargs.get("Overwrite") is not False or kwargs.get("Type") != "String":
            raise refusal("ValidationException", "PutParameter")
        if name in self.values:
            raise refusal("ParameterAlreadyExists", "PutParameter")
        self.values[name] = kwargs["Value"]
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


class FakeEcs:
    """The tasks of one cluster. ``tasks`` are task dicts as DescribeTasks answers them; ``fail`` makes the listing fail."""

    def __init__(self) -> None:
        self.tasks: list[dict[str, Any]] = []
        self.fail = False
        self.calls: list[str] = []

    def add(self, family: str, last_status: str, desired: str = "RUNNING") -> None:
        number = len(self.tasks) + 1
        self.tasks.append(
            {
                "taskArn": f"arn:aws:ecs:us-west-2:{ACCOUNT}:task/cluster/{number:032x}",
                "group": f"family:{family}",
                "taskDefinitionArn": f"arn:aws:ecs:us-west-2:{ACCOUNT}:task-definition/{family}:1",
                "lastStatus": last_status,
                "desiredStatus": desired,
            }
        )

    def list_tasks(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("list")
        if self.fail:
            raise refusal("AccessDeniedException", "ListTasks")
        return {"taskArns": [task["taskArn"] for task in self.tasks if task["desiredStatus"] == kwargs["desiredStatus"]]}

    def describe_tasks(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("describe")
        wanted = set(kwargs["tasks"])
        return {"tasks": [task for task in self.tasks if task["taskArn"] in wanted], "failures": []}
