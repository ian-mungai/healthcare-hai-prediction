"""Capacity facts as the capacity module itself measures them, for the failure record cases.

The facts of a capacity refusal come only from ``task_capacity.derive`` (a refused derivation), never from a job's own exception, so the cases take them from
there."""

from __future__ import annotations

from typing import Any

from scripts.lakehouse import task_capacity

GIB = 1 << 30


def refusal(memory: int = GIB, siblings: int = 1, spill_free: int | None = 100 * GIB) -> task_capacity.CapacityError:
    """The refusal of a derivation for a container with ``memory`` bytes, ``siblings`` other containers and ``spill_free`` free spill bytes."""
    try:
        task_capacity.derive(task_capacity.Capacity(memory, "case", 4, "case", siblings, "case", spill_free))
    except task_capacity.CapacityError as error:
        return error
    raise AssertionError("the capacity was not refused")


def engine_facts(memory: int = GIB, siblings: int = 1) -> Any:
    return refusal(memory, siblings).failure_facts


def spill_facts(spill_free: int = GIB // 2) -> Any:
    return refusal(16 * GIB, 1, spill_free).failure_facts
