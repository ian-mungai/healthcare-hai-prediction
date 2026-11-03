"""Match persisted parser diagnostics without accepting log scaffolding as a cause."""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any


def diagnostic_bodies(value: Any, selected: bool = False) -> list[str]:
    if isinstance(value, str):
        return [value.strip().splitlines()[0]] if selected and value.strip() else []
    if isinstance(value, dict):
        return [body for key, item in value.items() for body in diagnostic_bodies(item, key in {"error", "errors", "reason", "why", "message"})]
    if isinstance(value, list):
        return [body for item in value for body in diagnostic_bodies(item, selected)]
    return []


def parse_cause_present(document: dict[str, Any], logs: Sequence[str], fault: str) -> bool:
    """Require one diagnostic body to identify the exact parser failure."""
    bodies = diagnostic_bodies(document)
    bodies += [line.strip()[6:].strip() for log in logs for line in log.splitlines() if line.strip().lower().startswith("error:")]
    if fault == "invalid_json":
        return any(
            "jsondecodeerror" in body.lower() or "invalid json" in body.lower() or body.lower().startswith(("expecting value", "expecting property name"))
            for body in bodies
        )
    field = fault.removeprefix("missing_")
    patterns = (
        r"keyerror\s*:\s*(.+)",
        r"keyerror\((.+)\)",
        r"(?:missing|required|absent)\s+(?:required\s+)?(?:field|key)s?\s*:?\s*(.+)",
        r"(?:field|key)\s+(.+?)\s+(?:is\s+)?(?:missing|required|absent)",
    )
    for body in bodies:
        for pattern in patterns:
            match = re.fullmatch(pattern, body.strip(), re.IGNORECASE)
            if match is None:
                continue
            name = match[1].strip()
            if pattern in patterns[2:] and name == f"[{field}]":
                return True
            if len(name) >= 2 and name[0] in "'\"`" and name[-1] == name[0]:
                name = name[1:-1]
            if name == field:
                return True
    return False
