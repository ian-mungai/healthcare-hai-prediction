"""AWS-first placement, resource sizing and durable admission records.

The launcher calls these functions inside its registry transaction. External task dispatch follows the committed intent.
This module makes no network, credential, Docker or process call.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from functools import cache
from pathlib import Path
from typing import Any, Protocol

from jsonschema import Draft202012Validator

TARGETS = ("auto", "ecs", "docker")
TARGET_DOCKER = "docker"
MAX_TASKS = 3
MAX_CPU = 16 * 1024
MAX_MEMORY_MIB = 120 * 1024
PATIENCE_SECONDS = 120
OBSERVATION_SECONDS = 60
TERMINAL_PLACEMENTS = ("fenced", "complete")
BOTTLENECKS = ("capacity", "patience", "cloud_cap", "breaker")
GIB = 1024**3


class PlacementError(Exception):
    """A placement record or deployment contract cannot authorize execution."""


@dataclass(frozen=True)
class Size:
    """Fargate CPU units, memory in MiB and ephemeral storage in GiB."""

    cpu: int
    memory_mib: int
    storage_gib: int

    def __post_init__(self) -> None:
        if any(type(value) is not int for value in (self.cpu, self.memory_mib, self.storage_gib)):
            raise PlacementError("resource sizes must be integers")
        if self.cpu not in (1024, 2048, 4096, 8192, 16384) or not 2048 <= self.memory_mib <= MAX_MEMORY_MIB or not 21 <= self.storage_gib <= 200:
            raise PlacementError("resource sizes exceed the supported cloud limits")
        minimum, maximum, step = {1024: (2, 8, 1), 2048: (4, 16, 1), 4096: (8, 30, 1), 8192: (16, 60, 4), 16384: (32, 120, 8)}[self.cpu]
        if not minimum * 1024 <= self.memory_mib <= maximum * 1024 or (self.memory_mib - minimum * 1024) % (step * 1024):
            raise PlacementError("CPU and memory do not form a supported Fargate size")


@dataclass(frozen=True)
class Usage:
    """Resources held by every cloud placement without a confirmed terminal state."""

    tasks: int = 0
    cpu: int = 0
    memory_mib: int = 0

    def __post_init__(self) -> None:
        if any(type(value) is not int or value < 0 for value in (self.tasks, self.cpu, self.memory_mib)):
            raise PlacementError("cloud usage must contain nonnegative integers")

    def fits(self, size: Size) -> bool:
        return self.tasks + 1 <= MAX_TASKS and self.cpu + size.cpu <= MAX_CPU and self.memory_mib + size.memory_mib <= MAX_MEMORY_MIB


@dataclass(frozen=True)
class Availability:
    """Fresh backend evidence supplied by the launcher and its bounded task facade."""

    enabled: bool = True
    reason: str = "ready"
    observed_at: float | None = None
    no_start: bool = False
    on_demand_tried: bool = False
    portable: bool = False
    local_available: bool = True
    local_free: bool = True
    local_fits: bool = True
    usage: Usage = Usage()


@dataclass(frozen=True)
class Decision:
    """One action for the current admission pass without an external side effect."""

    action: str
    target: str | None
    reason: str
    provider: str | None = None


class Runtime(Protocol):
    """Nonblocking integration supplied by the reviewed worker and run-store lanes.

    Dispatch must enqueue bounded work from the persisted record. Advance reconciles that same live try and owns its cleanup.
    The facade must not mint grants or dispatch synchronously on the launcher's event loop.
    """

    def availability(self, attempt: Mapping[str, Any], now: float) -> Availability:
        """Return fresh backend and portability evidence for this attempt."""
        ...

    def size(self, attempt: Mapping[str, Any]) -> Size:
        """Return measured resource requirements for the verified job descriptor."""
        ...

    def dispatch(self, attempt: Mapping[str, Any], placement: Mapping[str, Any]) -> None:
        """Enqueue one persisted placement, using its fixed deadline and try identity."""
        ...

    def advance(self, state: dict[str, Any], attempt: dict[str, Any], now: float) -> None:
        """Reconcile, stop or apply only the persisted live try without blocking."""
        ...


def load_jobs(root: Path) -> dict[str, Any]:
    """Load the closed rollout table without reading deployment or credential files."""
    try:
        value: dict[str, Any] = json.loads((root / "config/orchestration/ecs_jobs.json").read_text())
        schema = json.loads((root / "config/orchestration/ecs_jobs.schema.json").read_text())
        if list(Draft202012Validator(schema).iter_errors(value)):
            raise PlacementError("ECS jobs violate their schema")
        keys = [(row["template"], row.get("layer"), row.get("step"), row["class"]) for row in value["jobs"]]
        if len(keys) != len(set(keys)):
            raise PlacementError("ECS jobs contain duplicate selectors")
        return value
    except (OSError, ValueError) as error:
        raise PlacementError(f"ECS jobs are unreadable: {type(error).__name__}") from None


def row_for(jobs: Mapping[str, Any], template: str, params: Mapping[str, Any], job_class: str) -> dict[str, Any] | None:
    """Return the exact closed template, layer, step and class row or no eligibility."""
    for row in jobs["jobs"]:
        if (
            row["template"] == template
            and row["class"] == job_class
            and ("layer" not in row or row["layer"] == params.get("layer"))
            and ("step" not in row or row["step"] == params.get("step"))
        ):
            return dict(row)
    return None


def initialize(attempt: dict[str, Any], requested: str, now: float, catalog_epoch: int | None) -> None:
    """Bind per-job intent once without changing replay identity or the fixed deadline."""
    if requested not in TARGETS:
        raise PlacementError("target must be auto, ecs or docker")
    if "execution" in attempt:
        validate_execution(attempt["execution"])
        if attempt["execution"]["requested_target"] != requested or attempt["execution"]["deadline_utc"] != attempt["deadline_utc"]:
            raise PlacementError("target differs from the persisted attempt")
        return
    attempt["execution"] = {
        "version": 1,
        "requested_target": requested,
        "queued_at": now,
        "deadline_utc": attempt["deadline_utc"],
        "catalog_epoch": catalog_epoch,
        "live_try": None,
        "placements": [],
    }


def active_usage(attempts: Iterable[Mapping[str, Any]]) -> Usage:
    """Count pending, running, stopping and unknown tasks until their stop is confirmed."""
    tasks = cpu = memory = 0
    for attempt in attempts:
        if "execution" in attempt:
            validate_execution(attempt["execution"])
        for placement in attempt.get("execution", {}).get("placements", []):
            if placement["target"] == "ecs" and placement["state"] not in TERMINAL_PLACEMENTS:
                tasks += 1
                cpu += placement["cpu"]
                memory += placement["memory_mib"]
    return Usage(tasks, cpu, memory)


def select(attempt: Mapping[str, Any], job_class: str, size: Size, view: Availability, now: float) -> Decision:
    """Prefer AWS and permit Mac fallback only from fresh, measured, effect-free bottlenecks."""
    execution = attempt["execution"]
    requested = execution["requested_target"]
    live = execution["live_try"]
    if live is not None:
        placement = next(item for item in execution["placements"] if item["try_id"] == live)
        if placement["state"] not in TERMINAL_PLACEMENTS:
            patience_expired = view.reason == "patience" and not placement["started"] and now - execution["queued_at"] >= PATIENCE_SECONDS
            action = "fence" if now >= execution["deadline_utc"] or patience_expired else "reconcile"
            return Decision(action, None, "live_try_unconfirmed")
        raise PlacementError("a terminal placement remains live")
    if now >= execution["deadline_utc"]:
        return Decision("expire", None, "deadline")
    if job_class == "lakehouse_write":
        return Decision("refuse", None, "writer_not_enabled")
    if not view.enabled:
        return Decision("refuse", None, "ecs_disabled")
    if view.reason not in ("ready", *BOTTLENECKS):
        return Decision("refuse", None, "cloud_failure")
    previous = [item for item in execution["placements"] if item["target"] == "ecs" and item["state"] == "fenced"]
    if any(item["started"] or not item["no_start"] for item in previous):
        return Decision("refuse", None, "prior_try_not_replayable")
    no_start = previous[-1]["no_start"] if previous else view.no_start
    on_demand_tried = any(item["capacity_provider"] == "FARGATE" for item in previous)
    cap_ok = view.usage.fits(size)
    reason = "cloud_cap" if not cap_ok and view.reason == "ready" else view.reason
    if reason == "ready":
        if requested == "docker":
            return Decision("refuse", None, "fallback_not_allowed")
        return Decision("dispatch", "ecs", "aws_first", "FARGATE_SPOT")
    fresh = view.observed_at is not None and 0 <= now - view.observed_at <= OBSERVATION_SECONDS
    if reason != "cloud_cap" and not fresh or reason == "cloud_cap" and cap_ok and not fresh:
        return Decision("wait", None, "stale_bottleneck")
    if reason in ("capacity", "patience") and not no_start:
        return Decision("reconcile", None, "no_start_unproven")
    if reason == "patience" and now - execution["queued_at"] < PATIENCE_SECONDS:
        return Decision("wait", None, "aws_patience")
    if reason == "capacity" and not on_demand_tried and cap_ok:
        if requested == "docker":
            return Decision("refuse", None, "on_demand_not_tried")
        return Decision("dispatch", "ecs", "spot_capacity_unavailable", "FARGATE")
    if requested == "ecs":
        return Decision("wait", None, reason)
    if not (view.local_available and view.local_free and view.local_fits and view.portable):
        return Decision("wait", None, reason)
    return Decision("dispatch", TARGET_DOCKER, reason)


def reserve(attempt: dict[str, Any], size: Size, decision: Decision, now: float, usage: Usage | None = None) -> dict[str, Any]:
    """Reserve exactly one placement under the caller's registry lock before dispatch."""
    execution = attempt["execution"]
    if decision.target == "ecs" and usage is None:
        raise PlacementError("cloud placement requires current usage")
    usage = usage if usage is not None else Usage()
    if execution["live_try"] is not None:
        raise PlacementError("a live placement already exists")
    if decision.action != "dispatch" or decision.target not in ("ecs", "docker") or now >= execution["deadline_utc"]:
        raise PlacementError("placement is not admitted")
    if decision.target == "ecs" and (not usage.fits(size) or decision.provider not in ("FARGATE_SPOT", "FARGATE")):
        raise PlacementError("cloud placement exceeds its cap or lacks a capacity provider")
    if decision.target == "docker" and (decision.reason not in BOTTLENECKS or decision.provider is not None):
        raise PlacementError("local placement lacks an approved fallback reason")
    try_id = f"hai_{attempt['id']}_{len(execution['placements']) + 1}"
    if re.fullmatch(r"[A-Za-z0-9_-]{1,100}", try_id) is None:
        raise PlacementError("try identity is invalid")
    placement = {
        "try_id": try_id,
        "target": decision.target,
        "capacity_provider": decision.provider,
        "state": "intent",
        "reason": decision.reason,
        "created_at": now,
        "deadline_utc": execution["deadline_utc"],
        "cpu": size.cpu,
        "memory_mib": size.memory_mib,
        "storage_gib": size.storage_gib,
        "task_arn": None,
        "started": False,
        "cap_before": usage_record(usage),
    }
    execution["placements"].append(placement)
    execution["live_try"] = try_id
    return placement


def fenced(attempt: dict[str, Any], try_id: str, now: float, *, no_start: bool) -> None:
    """Record a confirmed stop from the task facade and release only that live placement."""
    execution = attempt["execution"]
    if execution["live_try"] != try_id:
        raise PlacementError("fence does not name the live try")
    placement = next(item for item in execution["placements"] if item["try_id"] == try_id)
    if no_start and placement["started"]:
        raise PlacementError("a started task cannot become a no-start failure")
    placement.update(state="fenced", fenced_at=now, no_start=no_start)
    execution["live_try"] = None


def ecs_plan(
    memory_need_gib: float,
    vcpu_min: float,
    *,
    inputs_gib: float,
    outputs_total_gib: float,
    scratch_gib: float,
    temp_cap_gib: float,
    image_compressed_gib: float,
    image_uncompressed_gib: float,
) -> Size:
    """Size all image, input, output, copy and spill bytes within the fixed cloud limits."""
    values = (memory_need_gib, vcpu_min, inputs_gib, outputs_total_gib, scratch_gib, temp_cap_gib, image_compressed_gib, image_uncompressed_gib)
    if any(not math.isfinite(value) or value < 0 for value in values) or memory_need_gib == 0 or vcpu_min == 0:
        raise PlacementError("resource requirements must be finite and nonnegative")
    disk = max(21, math.ceil(inputs_gib + 1.5 * outputs_total_gib + scratch_gib + temp_cap_gib + image_compressed_gib + image_uncompressed_gib) + 5)
    if disk > 200:
        raise PlacementError("ephemeral storage exceeds 200 GiB")
    need = math.ceil(memory_need_gib + 2)
    for cpu, minimum, maximum, step in ((1, 2, 8, 1), (2, 4, 16, 1), (4, 8, 30, 1), (8, 16, 60, 4), (16, 32, 120, 8)):
        memory = minimum + max(0, math.ceil((need - minimum) / step)) * step
        if cpu >= vcpu_min and memory <= maximum:
            return Size(cpu * 1024, memory * 1024, disk)
    raise PlacementError("resource requirements exceed the fixed cloud cap")


def public_execution(attempt: Mapping[str, Any]) -> dict[str, Any]:
    """Return the schema-validated execution record for the broker's ledger."""
    validate_execution(attempt["execution"])
    return json.loads(json.dumps(attempt["execution"]))


@cache
def execution_validator() -> Draft202012Validator:
    schema = json.loads((Path(__file__).resolve().parents[1] / "data_contracts/orchestration/execution.schema.json").read_text())
    return Draft202012Validator(schema)


def validate_execution(execution: Mapping[str, Any]) -> None:
    """Check stored execution shape and its single-live-placement and deadline invariants."""
    if list(execution_validator().iter_errors(execution)):
        raise PlacementError("execution record violates its schema")
    placements = execution["placements"]
    identities = [item["try_id"] for item in placements]
    active = [item["try_id"] for item in placements if item["state"] not in TERMINAL_PLACEMENTS]
    if len(identities) != len(set(identities)) or active != ([] if execution["live_try"] is None else [execution["live_try"]]):
        raise PlacementError("execution record has inconsistent live placement authority")
    if any(item["deadline_utc"] != execution["deadline_utc"] for item in placements):
        raise PlacementError("a placement changed the attempt deadline")


def usage_record(usage: Usage) -> dict[str, int]:
    """Return the cap snapshot persisted with dispatch intent."""
    return asdict(usage)
