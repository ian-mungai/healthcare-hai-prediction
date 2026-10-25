"""Reconcile one persisted ECS try without repeating its external dispatch."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, Protocol

from orchestration.ecs_target import PlacementError
from orchestration.worker_protocol import cap_seconds, validate


class Api(Protocol):
    """The bounded worker facade, including its separately integrated definition reader."""

    def call(self, operation: str, params: Mapping[str, Any], *, deadline: float) -> dict[str, Any]: ...


class Clock(Protocol):
    """Clock readings supplied by the runtime or a deterministic stand-in."""

    def time(self) -> float: ...

    def monotonic(self) -> float: ...


def result(api: Api, operation: str, params: Mapping[str, Any], deadline: float, clock: Clock) -> dict[str, Any]:
    """Perform one deadline-bound read and reject an incomplete worker result."""
    if clock.monotonic() >= deadline:
        raise PlacementError("ECS operation deadline expired")
    if operation != "describe_task_definition":
        validate({"id": "dispatch_read", "op": operation, "params": dict(params), "budget_s": min(cap_seconds(operation), deadline - clock.monotonic())})
    try:
        reply = api.call(operation, params, deadline=deadline)
    except (OSError, TimeoutError):
        raise PlacementError("ECS observation is incomplete") from None
    if clock.monotonic() >= deadline:
        raise PlacementError("ECS operation deadline expired")
    if reply.get("ok") is not True or not isinstance(reply.get("result"), dict):
        raise PlacementError("ECS observation is incomplete")
    return dict(reply["result"])


def task_arns(value: Any, request: Mapping[str, Any]) -> list[str]:
    """Return task ARNs bound to the request's exact cluster."""
    prefix = request["cluster"].replace(":cluster/", ":task/") + "/"
    if not isinstance(value, list) or any(
        not isinstance(arn, str) or not arn.startswith(prefix) or re.fullmatch(r"[A-Za-z0-9_-]+", arn[len(prefix) :]) is None for arn in value
    ):
        raise PlacementError("task identity differs from the cluster")
    return value


def _identity(task: Mapping[str, Any], request: Mapping[str, Any], definition: Mapping[str, Any], project: str) -> None:
    family = request["taskDefinition"].rsplit("/", 1)[1].rsplit(":", 1)[0]
    if any(
        task.get(key) != value
        for key, value in (("cluster_arn", request["cluster"]), ("task_definition_arn", request["taskDefinition"]), ("group", "family:" + family))
    ):
        raise PlacementError("task identity differs from the dispatch intent")
    tags = task.get("tags")
    if not isinstance(tags, list) or any(not isinstance(row, dict) or set(row) != {"key", "value"} for row in tags):
        raise PlacementError("task project tag is missing or malformed")
    if len({row["key"] for row in tags}) != len(tags) or {row["key"]: row["value"] for row in tags}.get("Project") != project:
        raise PlacementError("task project tag differs from the dispatch intent")
    if task.get("security_groups") != request["networkConfiguration"]["awsvpcConfiguration"]["securityGroups"]:
        raise PlacementError("task network differs from the dispatch intent")
    containers = task.get("containers")
    if (
        not isinstance(containers, list)
        or len(containers) != 1
        or containers[0].get("name") != definition["container_name"]
        or containers[0].get("image") != definition["image"]
    ):
        raise PlacementError("task image digest differs from the dispatch intent")


def scan(api: Api, dispatch: Mapping[str, Any], task_arn: str | None, *, deadline: float, clock: Clock) -> dict[str, Any] | None:
    """Scan both desired states to completion, then adopt exactly one matching task.

    Empty scans leave the dispatch unknown. They never prove that a writer did
    not start. Failures, repeated page tokens and missing descriptions retain
    the original cap reservation and prevent a replacement try.
    """
    request = dispatch["request"]
    family = request["taskDefinition"].rsplit("/", 1)[1].rsplit(":", 1)[0]
    arns: set[str] = set()
    if task_arn is not None:
        arns.update(task_arns([task_arn], request))
    for status in ("RUNNING", "STOPPED"):
        token = None
        seen: set[str] = set()
        while True:
            params = {"cluster": request["cluster"], "family": family, "desiredStatus": status}
            if token is not None:
                params["nextToken"] = token
            page = result(api, "list_tasks", params, deadline, clock)
            arns.update(task_arns(page.get("task_arns"), request))
            token = page.get("next_token")
            if token is None:
                break
            if not isinstance(token, str) or not token or token in seen:
                raise PlacementError("repeated task page or malformed page token")
            seen.add(token)
    matched = []
    ordered = sorted(arns)
    for offset in range(0, len(ordered), 100):
        batch = ordered[offset : offset + 100]
        described = result(api, "describe_tasks", {"cluster": request["cluster"], "tasks": batch, "include": ["TAGS"]}, deadline, clock)
        tasks = described.get("tasks")
        if described.get("failures") != [] or not isinstance(tasks, list) or any(not isinstance(task, dict) for task in tasks):
            raise PlacementError("incomplete task describe")
        if len(tasks) != len(batch) or {task.get("task_arn") for task in tasks} != set(batch):
            raise PlacementError("incomplete task describe")
        for task in tasks:
            if task.get("started_by") == request["startedBy"]:
                _identity(task, request, dispatch["definition"], dispatch["project_tag"])
                matched.append(task)
    if len(matched) > 1:
        raise PlacementError("more than one task matches the live try")
    return matched[0] if matched else None
