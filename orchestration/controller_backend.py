"""Controller integration boundaries for secure credentials and disabled local execution."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from orchestration.launcher import Ctx, Launcher

LOCAL_METHODS = frozenset(
    {
        "allocate",
        "release_allocation",
        "plan",
        "image",
        "session",
        "snapshot",
        "catalog_up",
        "catalog_down",
        "pipeline_catalog_up",
        "pipeline_catalog_release",
        "pipeline_network_up",
        "pipeline_network_down",
        "pipeline_test_catalog",
        "orchestration_up",
        "orchestration_down",
        "lineage_up",
        "lineage_down",
        "pull",
        "docker_restart",
    }
)


class TokenProvider(Protocol):
    """Memory-only credentials backed by a secure store, with asynchronous durable acquire owned by its provider.

    ``acquire`` runs on the launcher loop inside its request transaction. It must record pending intent and return
    without a network wait. Only confirmed publication may make a run held. It can return the launcher's pending sentinel.
    ``read`` is used by clients outside launcher transactions. No implementation may write plaintext credentials to disk.
    """

    def signing_key(self) -> bytes:
        """Load the existing signing key before the launcher begins serving, within the provider's startup deadline."""
        ...

    def read(self, run_id: str) -> str:
        """Read one run token in memory or raise a named missing or unavailable error."""
        ...

    def absent(self, run_id: str) -> bool:
        """Return true only after the secure store confirms absence, never after an unavailable read."""
        ...

    def acquire(self, server: Launcher, ctx: Ctx) -> Any:
        """Record or reconcile secure publication without blocking the loop or changing request identity."""
        ...

    def start(self, server: Launcher) -> None:
        """Reconcile recorded token work without blocking the launcher loop."""
        ...

    def tick(self, server: Launcher) -> None:
        """Schedule committed token intents and apply their deadlines outside request transactions."""
        ...

    def release(self, server: Launcher, ctx: Ctx) -> None:
        """Record retirement together with terminal run state and invalidate token caches."""
        ...

    def recover(self, server: Launcher, ctx: Ctx) -> dict[str, Any]:
        """Abort or retry a recorded token operation after explicit owner authorization."""
        ...

    def close(self, *, deadline: float) -> None:
        """Confirm owned token workers stopped within the aggregate cleanup deadline."""
        ...


def local_work_blockers(state: Mapping[str, Any]) -> list[str]:
    """Name recorded local work that must be drained before a disabled backend can serve."""
    problems = []
    for identity, attempt in state.get("attempts", {}).items():
        execution = attempt.get("execution") or {}
        placements = execution.get("placements", [])
        live = execution.get("live_try")
        local_live = any(row.get("try_id") == live and row.get("target") == "docker" for row in placements)
        if attempt.get("phase") != "ended" and (not execution or local_live or attempt.get("container_id")):
            problems.append(f"local attempt {identity}")
    for identity, allocation in state.get("allocations", {}).items():
        if allocation.get("state") not in ("released", "failed"):
            problems.append(f"local allocation {identity}")
    for identity in state.get("registrations", {}):
        problems.append(f"local allocation registration {identity}")
    for identity, copy in state.get("copies", {}).items():
        if copy.get("phase") in ("copying", "abandoning"):
            problems.append(f"local snapshot copy {identity}")
    for identity, run in state.get("runs", {}).items():
        if run.get("seal"):
            problems.append(f"local seal intent {identity}")
        network = run.get("pipeline_network") or {}
        if network.get("phase", "down") != "down":
            problems.append(f"local pipeline network {identity}")
    for identity, operation in state.get("operations", {}).items():
        if operation.get("state") == "open":
            problems.append(f"local operation {identity}")
    for identity, container in state.get("containers", {}).items():
        if not container.get("ended"):
            problems.append(f"local container or network {identity}")
    for name in ("catalog", "orchestration", "lineage"):
        service = state.get("launcher", {}).get(name) or {}
        if service.get("phase", "down") != "down":
            problems.append(f"local {name} service")
    if state.get("launcher", {}).get("restart"):
        problems.append("local Docker restart")
    return problems
