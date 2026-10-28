"""A nonblocking task facade for synthetic placement cases, with no service calls."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from orchestration.ecs_target import Availability, PlacementError, Size


class FakeEcs:
    """Keep synthetic task observations and require durable intent before accepting dispatch."""

    def __init__(self, registry_file: Path) -> None:
        self.registry_file = registry_file
        self.view = Availability(portable=True)
        self.calls: list[dict[str, Any]] = []
        self.states: dict[str, str] = {}

    def availability(self, attempt: Mapping[str, Any], now: float) -> Availability:
        return self.view

    def size(self, attempt: Mapping[str, Any]) -> Size:
        return Size(1024, 4096, 21)

    def dispatch(self, attempt: Mapping[str, Any], placement: Mapping[str, Any]) -> None:
        state = json.loads(self.registry_file.read_text())
        saved = state["attempts"][attempt["id"]]["execution"]
        if saved["live_try"] != placement["try_id"] or saved["placements"][-1] != placement:
            raise PlacementError("dispatch preceded durable intent")
        if placement["try_id"] not in self.states:
            self.calls.append(dict(placement))
            self.states[placement["try_id"]] = "pending"

    def advance(self, state: dict[str, Any], attempt: dict[str, Any], now: float) -> None:
        execution = attempt["execution"]
        live = next(item for item in execution["placements"] if item["try_id"] == execution["live_try"])
        if live["try_id"] in self.states:
            live["state"] = self.states[live["try_id"]]
            live["task_arn"] = f"arn:aws:ecs:us-west-2:{'0' * 12}:task/synthetic/{live['try_id']}"
            live["started"] = live["state"] == "running"
