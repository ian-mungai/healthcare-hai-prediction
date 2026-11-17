"""Cases for the ECS read policy of the host state exporter (review of 84b3722..7fc9030, finding 3).

An IAM statement allows an action only on a resource type that the action supports. ``ecs:ListServices`` and ``ecs:ListTasks`` support no resource type, so a
statement that names the cluster ARN as the resource of ``ecs:ListServices`` grants nothing and the exporter's first call fails. The way to scope them is
``Resource: "*"`` with an ``ecs:cluster`` ``ArnEquals`` condition. The table below is the resource types of each action in the AWS service authorization
reference for Amazon ECS (an empty set: the action has no resource type, so only ``*`` is valid). The cases judge each pair of action and resource in
``infra/iam/owner_policies/host_state_read.json`` against the table, which a comparison of policy JSON with itself cannot do, and judge a planted copy with the
old statement, so the case can fail.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

POLICY = "infra/iam/owner_policies/host_state_read.json"
CLUSTER = "arn:aws:ecs:us-west-2:111111111111:cluster/example_cluster"
VALUES = {
    "CLUSTER_ARN": CLUSTER,
    "CONTAINER_INSTANCE_ARN_PATTERN": "arn:aws:ecs:us-west-2:111111111111:container-instance/example_cluster/*",
    "SERVICE_ARN_PATTERN": "arn:aws:ecs:us-west-2:111111111111:service/example_cluster/*",
    "TASK_ARN_PATTERN": "arn:aws:ecs:us-west-2:111111111111:task/example_cluster/*",
}
RESOURCE_TYPES: dict[str, frozenset[str]] = {
    "ecs:ListTasks": frozenset(),
    "ecs:ListServices": frozenset(),
    "ecs:ListContainerInstances": frozenset({"cluster"}),
    "ecs:DescribeContainerInstances": frozenset({"container-instance"}),
    "ecs:DescribeServices": frozenset({"service"}),
    "ecs:DescribeTasks": frozenset({"task"}),
}


def rendered(root: Path) -> list[dict[str, Any]]:
    text = (root / POLICY).read_text(encoding="utf-8")
    for name, value in VALUES.items():
        text = text.replace("${" + name + "}", value)
    statements: list[dict[str, Any]] = json.loads(text)["Statement"]
    return statements


def kind_of(resource: str) -> str:
    """The resource type of an ARN: ``*`` for the wildcard, else the word before the first slash of the resource part."""
    return "*" if resource == "*" else resource.split(":", 5)[5].split("/", 1)[0]


def problems(statements: list[dict[str, Any]]) -> list[str]:
    """Every pair of action and resource that the table says grants nothing, and every wildcard that has no cluster condition."""
    found: list[str] = []
    for statement in statements:
        actions = [statement["Action"]] if isinstance(statement["Action"], str) else statement["Action"]
        resources = [statement["Resource"]] if isinstance(statement["Resource"], str) else statement["Resource"]
        for action in actions:
            if action not in RESOURCE_TYPES:
                found.append(f"{action} is not in the table of resource types")
                continue
            for resource in resources:
                kind, supported = kind_of(resource), RESOURCE_TYPES[action]
                if (kind == "*" and supported) or (kind != "*" and kind not in supported):
                    found.append(f"{statement['Sid']}: {action} does not support the resource {kind}")
                if kind == "*" and statement.get("Condition", {}).get("ArnEquals", {}).get("ecs:cluster") != CLUSTER:
                    found.append(f"{statement['Sid']}: a wildcard resource needs the cluster condition")
    return found


def every_action_is_allowed_on_a_resource_type_it_supports(root: Path) -> dict[str, bool]:
    statements = rendered(root)
    listed = {action for statement in statements for action in ([statement["Action"]] if isinstance(statement["Action"], str) else statement["Action"])}
    return {
        "every action of the policy is in the table": listed <= set(RESOURCE_TYPES),
        "no pair of action and resource grants nothing, and every wildcard carries the cluster condition": problems(statements) == [],
        "the policy allows no write, run or stop action": all(action.startswith(("ecs:List", "ecs:Describe")) for action in listed),
    }


def the_old_statement_is_found_by_the_check(root: Path) -> dict[str, bool]:
    old = [
        {"Sid": "ListClusterServices", "Effect": "Allow", "Action": "ecs:ListServices", "Resource": CLUSTER},
        {"Sid": "ListClusterTasks", "Effect": "Allow", "Action": "ecs:ListTasks", "Resource": "*"},
    ]
    return {
        "the cluster ARN as the resource of ListServices is reported": any(
            "ListServices does not support the resource cluster" in line for line in problems(old)
        ),
        "a wildcard without the cluster condition is reported": any("needs the cluster condition" in line for line in problems(old)),
    }


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "iam_every_action_is_allowed_on_a_resource_type_it_supports": every_action_is_allowed_on_a_resource_type_it_supports,
    "iam_the_old_statement_is_found_by_the_check": the_old_statement_is_found_by_the_check,
}
