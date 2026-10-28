"""Build and dispatch a canonical ECS request from verified renderer and registry records."""

from __future__ import annotations

import copy
import hashlib
import json
import math
import re
import time
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from datetime import timedelta
from email.utils import parsedate_to_datetime
from typing import Any, Protocol

from orchestration import aws_qualify, ecs_admission, ecs_reconcile
from orchestration.ecs_stage import validate_contract
from orchestration.ecs_target import PlacementError, Size, validate_execution
from orchestration.worker_protocol import ProtocolError, validate
from scripts.infrastructure.render_ecs_target import TargetError, validate_target
from services.hai.bootstrap.integrity import StageError, run_prefix, try_prefix

ENVIRONMENT_NAMES = (
    "HAI_ATTEMPT_ID",
    "HAI_TRY_ID",
    "HAI_RUN_ID",
    "HAI_GENERATION",
    "HAI_STAGE_PREFIX",
    "HAI_JOB_SPEC_SHA256",
    "HAI_DEADLINE_UTC",
)
CLASS_NETWORKS = {"offline": "offline", "seal": "offline", "lakehouse_read": "lakehouse", "lakehouse_write": "lakehouse"}


class Rendered(Protocol):
    """The relevant public fields of the existing launcher's actual render result."""

    body: dict[str, Any]
    secret_env: frozenset[str]
    secret_values: frozenset[str]
    finite: bool
    deadline_utc: int | None


def canonical(value: Any) -> bytes:
    """Encode a persisted request or verified job deterministically."""
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()
    except (ValueError, TypeError):
        raise PlacementError("invalid dispatch record") from None


def job_from_rendered(base: Mapping[str, Any], rendered: Rendered, attempt: Mapping[str, Any]) -> dict[str, Any]:
    """Carry the real renderer's command and plain environment into its staged job.

    Input maps, archives and declarations still come from the verified staging
    adapter. Secret values and local credential mounts cannot cross this seam.
    """
    if not rendered.finite or rendered.deadline_utc != attempt["deadline_utc"]:
        raise PlacementError("render changed the fixed deadline")
    body = rendered.body
    entrypoint = body.get("Entrypoint")
    if not isinstance(entrypoint, list) or not entrypoint or entrypoint[0] != "/opt/hai/deadline_run":
        raise PlacementError("render lacks the fixed deadline entrypoint")
    environment: dict[str, str] = {}
    for item in body.get("Env", []):
        if not isinstance(item, str) or "=" not in item:
            raise PlacementError("render contains malformed environment")
        name, value = item.split("=", 1)
        if name in environment:
            raise PlacementError("render contains duplicate environment")
        environment[name] = value
    if environment.pop("HAI_DEADLINE_UTC", None) != str(attempt["deadline_utc"]):
        raise PlacementError("render changed the fixed deadline")
    if not rendered.secret_env <= set(base["secret_names"]):
        raise PlacementError("render contains an undeclared secret name")
    environment = {name: value for name, value in environment.items() if name not in rendered.secret_env}
    if any(
        name in environment
        for name in ("AWS_PROFILE", "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
    ):
        raise PlacementError("render contains local AWS credentials")
    if any(mount.get("Target", "").startswith("/aws") for mount in body.get("HostConfig", {}).get("Mounts", [])):
        raise PlacementError("render contains a local credential mount")
    job = copy.deepcopy(dict(base))
    job["command"] = [*entrypoint[1:], *body.get("Cmd", [])]
    job["plain_environment"] = environment
    execution = attempt["execution"]
    if "admission" not in execution:
        raise PlacementError("render lacks an admission binding")
    job["job_class"] = execution["admission"]["job_class"]
    job["catalog_authority"] = copy.deepcopy(execution["catalog_authority"])
    if execution["writer_authorization"] is not None:
        job["writer_authorization"] = copy.deepcopy(execution["writer_authorization"])
    raw = canonical(job).decode()
    if any(value and value in raw for value in rendered.secret_values):
        raise PlacementError("render would stage a secret value")
    return job


def execution_pin(container: Mapping[str, Any], task: Mapping[str, Any]) -> str:
    """Reproduce the existing execution-pin subject and Terraform JSON encoding.

    The worker's definition reader must normalize SDK task attributes into the
    exported task-settings shape. Missing or extra fields change this pin.
    """
    volumes = task.get("volume")
    mounts = container.get("mountPoints")
    if not isinstance(volumes, list) or not isinstance(mounts, list):
        raise PlacementError("active definition lacks volume settings")
    names = {mount["sourceVolume"] for mount in mounts}
    subject = {
        "container": {key: value for key, value in container.items() if key not in ("memory", "memoryReservation")},
        "task": {
            "settings": {key: value for key, value in task.items() if key != "volume"},
            "volumes": {volume["name"]: volume for volume in volumes if volume["name"] in names},
        },
    }
    encoded = json.dumps(subject, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))
    for character, replacement in (("&", "\\u0026"), ("<", "\\u003c"), (">", "\\u003e"), ("\u2028", "\\u2028"), ("\u2029", "\\u2029")):
        encoded = encoded.replace(character, replacement)
    return hashlib.sha256(encoded.encode()).hexdigest()


def verify_definition(target: Mapping[str, Any], definition: Mapping[str, Any], described: Mapping[str, Any]) -> None:
    """Compare the live fixed revision with the renderer's digest-bound definition."""
    if described.get("status") != "ACTIVE":
        raise PlacementError("active definition is required")
    arn = definition["task_definition_arn"]
    match = re.fullmatch(r"arn:aws:ecs:[a-z0-9-]+:[0-9]{12}:task-definition/([A-Za-z0-9_-]+):[1-9][0-9]*", arn)
    task, container = described.get("task"), described.get("container")
    if match is None or described.get("task_definition_arn") != arn or not isinstance(task, dict) or task.get("family") != match[1]:
        raise PlacementError("active definition differs from the fixed revision")
    if not isinstance(container, dict) or container.get("name") != definition["container_name"] or container.get("image") != definition["image"]:
        raise PlacementError("active definition image digest differs from the target")
    if not isinstance(task.get("tags"), dict) or task["tags"].get("Project") != target["project_tag"]:
        raise PlacementError("active definition project tag differs from the target")
    if (
        task.get("task_role_arn") != target["job_roles"][definition["task_role_key"]]["arn"]
        or task.get("execution_role_arn") != target["job_roles"]["execution"]["arn"]
    ):
        raise PlacementError("active definition role differs from the target")
    if execution_pin(container, task) != definition["execution_sha256"]:
        raise PlacementError("active definition execution pin differs from the target")


def build_request(
    target: dict[str, Any],
    definition_name: str,
    job: Mapping[str, Any],
    job_reference: Mapping[str, Any],
    attempt: Mapping[str, Any],
    placement: Mapping[str, Any],
    described: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the complete closed request, checking all identities before RunTask."""
    try:
        admission = attempt["execution"]["admission"]
        aws_qualify.validate_dispatch(target, definition_name, job, attempt, placement, described)
        job_class = admission["job_class"]
        if job.get("job_class") != job_class or target["definitions"][definition_name]["job_class"] != job_class:
            raise PlacementError("job class differs from its definition")
        if target["class_networks"].get(job_class) != CLASS_NETWORKS[job_class]:
            raise PlacementError("class network mapping differs from the job class")
        validate_target(target)
        if target["enabled"] is not True:
            raise PlacementError("ECS target is disabled")
        definition = target["definitions"][definition_name]
        verify_definition(target, definition, described)
        validate_contract("ecs_job", job, "invalid_dispatch_job")
        Size(placement["cpu"], placement["memory_mib"], placement["storage_gib"])
        if placement["target"] != "ecs" or placement["try_id"] != attempt["execution"]["live_try"] or placement["deadline_utc"] != attempt["deadline_utc"]:
            raise PlacementError("dispatch differs from the live reservation")
        if job_class == "lakehouse_write" and placement["capacity_provider"] != "FARGATE":
            raise PlacementError("writers require ECS on demand")
        if any(
            job[key] != value
            for key, value in (
                ("attempt_id", attempt["id"]),
                ("try_id", placement["try_id"]),
                ("template", attempt["template"]),
                ("generation", attempt["generation"]),
                ("run_id", admission["run_id"]),
            )
        ):
            raise PlacementError("job identity differs from the live reservation")
        if job.get("catalog_authority") != attempt["execution"]["catalog_authority"]:
            raise PlacementError("job catalog authority differs from admission")
        if job_class.startswith("lakehouse_"):
            catalog = target["catalog"]
            if (
                catalog is None
                or catalog["authority_epoch"] != attempt["execution"]["catalog_epoch"]
                or job["plain_environment"].get("POLARIS_URI") != catalog["endpoint"]
            ):
                raise PlacementError("job catalog endpoint or epoch differs from admission")
        if job["task_role_id"] != target["job_roles"][definition["task_role_key"]]["unique_id"]:
            raise PlacementError("job role differs from the active definition")
        if sorted(job["secret_names"]) != sorted(item["name"] for item in described["container"].get("secrets", [])):
            raise PlacementError("job secret names differ from the active definition")
        raw = canonical(job)
        if (
            set(job_reference) != {"key", "version_id", "sha256", "size"}
            or job_reference["key"] != try_prefix(job["run_id"], job["generation"], placement["try_id"]) + "job.json"
            or not isinstance(job_reference["version_id"], str)
            or not job_reference["version_id"]
            or job_reference["size"] != len(raw)
            or job_reference["sha256"] != hashlib.sha256(raw).hexdigest()
            or len(raw) > target["max_record_bytes"]
        ):
            raise PlacementError("job reference differs from the verified job")
        values = (
            attempt["id"],
            placement["try_id"],
            job["run_id"],
            str(job["generation"]),
            run_prefix(job["run_id"], job["generation"]),
            job_reference["sha256"],
            str(attempt["deadline_utc"]),
        )
        overrides = {
            "cpu": str(placement["cpu"]),
            "memory": str(placement["memory_mib"]),
            "ephemeralStorage": {"sizeInGiB": placement["storage_gib"]},
            "containerOverrides": [
                {
                    "name": definition["container_name"],
                    "environment": [{"name": name, "value": value} for name, value in zip(ENVIRONMENT_NAMES, values, strict=True)],
                }
            ],
        }
        request = {
            "cluster": target["cluster_arn"],
            "taskDefinition": definition["task_definition_arn"],
            "count": 1,
            "capacityProviderStrategy": [{"capacityProvider": placement["capacity_provider"], "weight": 1}],
            "networkConfiguration": {
                "awsvpcConfiguration": {
                    "subnets": [target["job_network"]["subnet_id"]],
                    "securityGroups": [target["job_network"]["security_group_ids"][CLASS_NETWORKS[job_class]]],
                    "assignPublicIp": "DISABLED",
                }
            },
            "overrides": overrides,
            "startedBy": placement["try_id"],
            "clientToken": hashlib.sha256(("hai-try|" + placement["try_id"]).encode()).hexdigest()[:48],
            "propagateTags": "TASK_DEFINITION",
            "enableECSManagedTags": True,
            "enableExecuteCommand": False,
        }
        validate({"id": "dispatch_request", "op": "run_task", "params": request, "budget_s": 25})
        return request
    except (TargetError, StageError, ProtocolError, KeyError, TypeError, ValueError) as error:
        raise PlacementError(str(error) if isinstance(error, (TargetError, StageError, ProtocolError)) else "malformed dispatch contract") from None


def clock_observation(result: Mapping[str, Any], account: str, now: float) -> dict[str, Any]:
    """Bind the caller account and STS Date header to a fresh skew measurement."""
    if result.get("account") != account:
        raise PlacementError("STS caller account differs from the target")
    try:
        date = parsedate_to_datetime(result["date_utc"])
        if date.utcoffset() != timedelta(0) or not math.isfinite(now):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise PlacementError("STS Date header is missing or malformed") from None
    skew = abs(date.timestamp() - now)
    if skew > 2:
        raise PlacementError("STS clock skew exceeds two seconds")
    return {"account": account, "date_utc": result["date_utc"], "observed_at": now, "skew_seconds": skew}


class Dispatcher:
    """Run a persisted dispatch saga inside an owned worker, never the launcher's event loop.

    ``transaction`` is the launcher's existing exclusive durable registry
    transaction. The normalized definition reader and enriched task descriptions
    must be integrated into the bounded worker API before live use.
    """

    def __init__(
        self,
        api: ecs_reconcile.Api,
        transaction: Callable[[], AbstractContextManager[dict[str, Any]]],
        *,
        registry_kind: str,
        clock: ecs_reconcile.Clock = time,
    ) -> None:
        self.api, self.transaction, self.registry_kind, self.clock = api, transaction, registry_kind, clock

    def _placement(self, state: Mapping[str, Any], attempt_id: str, try_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        attempt = state["attempts"][attempt_id]
        validate_execution(attempt["execution"])
        if attempt["execution"]["live_try"] != try_id:
            raise PlacementError("dispatch no longer owns the live try")
        placement = next(item for item in attempt["execution"]["placements"] if item["try_id"] == try_id)
        return attempt, placement

    def submit(
        self, attempt_id: str, try_id: str, *, target: dict[str, Any], definition_name: str, job: Mapping[str, Any], job_reference: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Commit exact request intent and a sent marker before the single external call."""
        with self.transaction() as state:
            attempt, placement = self._placement(state, attempt_id, try_id)
            ecs_admission.current(state, attempt, registry_kind=self.registry_kind)
            if attempt.get("cancel_intent") or attempt.get("timeout_intent"):
                raise PlacementError("dispatch authority ended before submission")
            deadline = self.clock.monotonic() + attempt["deadline_utc"] - self.clock.time()
            if self.clock.monotonic() >= deadline:
                raise PlacementError("ECS operation deadline expired")
            saved = copy.deepcopy(placement.get("dispatch"))
        if saved is not None:
            return self.reconcile(attempt_id, try_id, deadline=deadline)
        observation = clock_observation(ecs_reconcile.result(self.api, "caller_identity", {}, deadline, self.clock), target["account_id"], self.clock.time())
        described = ecs_reconcile.result(
            self.api,
            "describe_task_definition",
            {"taskDefinition": target["definitions"][definition_name]["task_definition_arn"], "include": ["TAGS"]},
            deadline,
            self.clock,
        )
        with self.transaction() as state:
            attempt, placement = self._placement(state, attempt_id, try_id)
            ecs_admission.current(state, attempt, registry_kind=self.registry_kind)
            if self.clock.monotonic() >= deadline:
                raise PlacementError("ECS operation deadline expired")
            if placement.get("dispatch") is not None:
                raise PlacementError("dispatch already has a persisted request")
            if attempt.get("cancel_intent") or attempt.get("timeout_intent"):
                raise PlacementError("dispatch authority ended before submission")
            if not 0 <= self.clock.time() - observation["observed_at"] <= 60:
                raise PlacementError("STS clock observation is stale")
            request = build_request(target, definition_name, job, job_reference, attempt, placement, described)
            placement["dispatch"] = {
                "request": copy.deepcopy(request),
                "request_sha256": hashlib.sha256(canonical(request)).hexdigest(),
                "job_reference": copy.deepcopy(dict(job_reference)),
                "definition": copy.deepcopy(target["definitions"][definition_name]),
                "project_tag": target["project_tag"],
                "clock_observation": observation,
                "sent": True,
            }
            qualification = aws_qualify.validate_dispatch(target, definition_name, job, attempt, placement, described)
            if qualification is not None:
                placement["dispatch"]["qualification_job"] = {"step": job["qualification"]["step"], "task_role_id": job["task_role_id"]}
                placement["dispatch"]["qualification"] = aws_qualify.dispatch_record(qualification, target, attempt, placement, request)
            placement.update(state="unknown", dispatch_attempted=True)
            validate_execution(attempt["execution"])
        if self.clock.monotonic() >= deadline:
            raise PlacementError("ECS operation deadline expired")
        try:
            reply = self.api.call("run_task", request, deadline=deadline)
        except (OSError, TimeoutError):
            reply = {"ok": False}
        if reply.get("ok") is not True:
            return {"state": "unknown", "try_id": try_id}
        response = reply.get("result", {})
        arns = response.get("task_arns")
        if response.get("failures") != [] or not isinstance(arns, list) or len(arns) != 1:
            raise PlacementError("RunTask result requires reconciliation")
        with self.transaction() as state:
            _attempt, placement = self._placement(state, attempt_id, try_id)
            placement["task_arn"] = ecs_reconcile.task_arns(arns, request)[0]
        return self.reconcile(attempt_id, try_id, deadline=deadline)

    def reconcile(self, attempt_id: str, try_id: str, *, deadline: float) -> dict[str, Any]:
        """Observe the same live try without issuing another writer or extending its budget."""
        with self.transaction() as state:
            attempt, placement = self._placement(state, attempt_id, try_id)
            saved, task_arn = copy.deepcopy(placement["dispatch"]), placement["task_arn"]
            deadline = min(deadline, self.clock.monotonic() + attempt["deadline_utc"] - self.clock.time())
        task = ecs_reconcile.scan(self.api, saved, task_arn, deadline=deadline, clock=self.clock)
        with self.transaction() as state:
            attempt, placement = self._placement(state, attempt_id, try_id)
            if task is None:
                placement["state"] = "unknown"
            else:
                status = task.get("last_status")
                if status not in ("PROVISIONING", "PENDING", "ACTIVATING", "RUNNING", "DEACTIVATING", "STOPPING", "DEPROVISIONING", "STOPPED"):
                    raise PlacementError("unrecognized task status")
                placement["task_arn"] = task["task_arn"]
                placement["started"] = placement["started"] or status == "RUNNING" or task.get("started") is True
                placement["state"] = "running" if status == "RUNNING" else "stopping" if status in ("DEACTIVATING", "STOPPING", "DEPROVISIONING") else "pending"
                if status == "STOPPED":
                    placement.update(state="fenced", fenced_at=self.clock.time(), no_start=False)
                    attempt["execution"]["live_try"] = None
            validate_execution(attempt["execution"])
            return copy.deepcopy(placement)
