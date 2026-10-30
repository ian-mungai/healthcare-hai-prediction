"""Stand-ins for the ECS, CloudWatch Logs and S3 client calls of the lean broker target, for offline end-to-end cases.

``FakeEcs`` has the call surface of the boto3 ECS client that ``orchestration.ecs_adapter`` uses: ``run_task``, ``describe_tasks``, ``stop_task`` and
``list_tasks``. It enforces the request rules the service documents: one task per call, a client token of at most 64 printable characters (a repeat of
a token returns the first task), ``startedBy`` of at most 128 characters, and no ``startedBy`` together with ``desiredStatus`` in ``list_tasks``. A task moves
on each ``describe_tasks``: provisioning, then running, then stopped with the exit codes planned for it. It never leaves the machine.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

from botocore.exceptions import ClientError, ReadTimeoutError

ACCOUNT = "000000000000"
CLUSTER = "stand-in-cluster"
REGION = "stand-in-region"
FINISH_AFTER = 3


def client_error(code: str, operation: str, message: str = "stand-in refusal") -> ClientError:
    """The error a botocore client raises for an API refusal."""
    return ClientError({"Error": {"Code": code, "Message": message}}, operation)


class FakeEcs:
    """Keeps the tasks of one stand-in cluster; ``faults`` queues an error for the next call of an operation."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.tasks: dict[str, dict[str, Any]] = {}
        self.tokens: dict[str, str] = {}
        self.faults: dict[str, list[Exception]] = {}
        self.lose_reply_once = False
        self.hold = False
        self.plan: dict[str, int | None] = {}
        self.omit: set[str] = set()
        self.finish_after = FINISH_AFTER
        self.stop_lag = 0  # describe calls a stopped task keeps answering RUNNING after StopTask accepted the request, as Fargate does while it shuts down

    def count(self, operation: str) -> int:
        return sum(1 for name, _ in self.calls if name == operation)

    def _enter(self, operation: str, arguments: Mapping[str, Any]) -> None:
        self.calls.append((operation, dict(arguments)))
        if arguments.get("cluster") != CLUSTER:
            raise client_error("ClusterNotFoundException", operation)
        queue = self.faults.get(operation)
        if queue:
            raise queue.pop(0)

    def run_task(self, **arguments: Any) -> dict[str, Any]:
        self._enter("run_task", arguments)
        token = arguments.get("clientToken", "")
        started_by = arguments.get("startedBy", "")
        if arguments.get("count") != 1 or not 1 <= len(token) <= 64 or len(started_by) > 128 or not all(33 <= ord(c) <= 126 for c in token):
            raise client_error("InvalidParameterException", "RunTask")
        if token in self.tokens:
            return {"tasks": [self.tasks[self.tokens[token]]], "failures": []}
        names = [item["name"] for item in arguments.get("overrides", {}).get("containerOverrides", [])]
        names = ["init", *names] if arguments["taskDefinition"].endswith("_train") else names
        digest = hashlib.sha256(token.encode()).hexdigest()[:32]
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
        self.tasks[arn] = task
        self.tokens[token] = arn
        if self.lose_reply_once:
            self.lose_reply_once = False
            raise ReadTimeoutError(endpoint_url="https://ecs.invalid")
        return {"tasks": [self._view(task)], "failures": []}

    def list_tasks(self, **arguments: Any) -> dict[str, Any]:
        self._enter("list_tasks", arguments)
        if "startedBy" in arguments and "desiredStatus" in arguments:
            raise client_error("InvalidParameterException", "ListTasks")
        found = [arn for arn, task in self.tasks.items() if task["startedBy"] == arguments.get("startedBy") and task["desiredStatus"] == "RUNNING"]
        return {"taskArns": found}

    def describe_tasks(self, **arguments: Any) -> dict[str, Any]:
        self._enter("describe_tasks", arguments)
        tasks, failures = [], []
        for arn in arguments["tasks"]:
            task = self.tasks.get(arn)
            if task is None:
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

    @staticmethod
    def _finish(task: dict[str, Any], exits: Mapping[str, int | None], reason: str, code: str = "EssentialContainerExited") -> None:
        task.update(lastStatus="STOPPED", desiredStatus="STOPPED", stoppedReason=reason, stopCode=code)
        for container in task["containers"]:
            code_of = exits.get(container["name"], 0)
            container["lastStatus"] = "STOPPED"
            if code_of is not None:
                container["exitCode"] = code_of

    @staticmethod
    def _view(task: Mapping[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in task.items() if key not in ("polls", "request", "stopping")}


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
