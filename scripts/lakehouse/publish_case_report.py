"""Validate case evidence before a publication check can report success."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from typing import Any


def read_cases(path: Path, expected: Sequence[str]) -> list[dict[str, Any]]:
    """Read exactly the requested case occurrences with consistent passing evidence."""
    try:
        rows = json.loads(path.read_text())
    except FileNotFoundError as error:
        raise ValueError(f"{path.name}: missing required report") from error
    except (json.JSONDecodeError, UnicodeError) as error:
        raise ValueError(f"{path.name}: invalid JSON report") from error
    except OSError as error:
        raise ValueError(f"{path.name}: report could not be read") from error
    if not isinstance(rows, list):
        raise ValueError(f"{path.name}: report must be a list")
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"{path.name}: row {index} must be an object")
        for field in ("case", "status", "checks", "error"):
            if field not in row:
                raise ValueError(f"{path.name}: row {index} is missing {field}")
        for field in ("case", "status", "error"):
            if not isinstance(row[field], str):
                raise ValueError(f"{path.name}: row {index} {field} must be a string")
        name = row["case"]
        if row["status"] != "pass":
            raise ValueError(f"{path.name}: case {name} status is not pass")
        checks = row["checks"]
        if not isinstance(checks, dict) or not checks or any(not isinstance(key, str) or value is not True for key, value in checks.items()):
            raise ValueError(f"{path.name}: case {name} checks must be nonempty and strictly true")
        if row["error"]:
            raise ValueError(f"{path.name}: case {name} error is not empty")
    wanted = Counter(expected)
    actual = Counter(row["case"] for row in rows)
    if actual != wanted:
        missing = ", ".join((wanted - actual).elements()) or "none"
        extra = ", ".join((actual - wanted).elements()) or "none"
        raise ValueError(f"{path.name}: case inventory mismatch; missing {missing}; unexpected or repeated {extra}")
    return rows
