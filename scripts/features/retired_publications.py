"""The features publications that the owner retired: the runs of the old single path, recorded as a cutover.

The cutover is in ``data_contracts/features/retired_publications.json``.

A run ID carries no time, so a publication is retired by the time its ``publish_runs`` row records: a reader of a namespace with an entry ignores every row
committed before the entry's ``retired_before`` time (a UTC time such as ``2026-11-12T00:00:00Z``). A namespace without an entry (the fixture namespace
``features_e2e``) retires nothing. The module imports nothing outside the standard library, so the readers of the feature image and the host checks share it.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

UTC_TIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
RETIRED = Path(__file__).resolve().parents[2] / "data_contracts/features/retired_publications.json"


class RetiredError(ValueError):
    """The retirement record is missing or malformed."""


def moment_of(value: Any) -> datetime:
    """A recorded time as an aware UTC datetime: a datetime (a naive one is read as UTC) or an ISO 8601 text. Anything else is refused."""
    try:
        moment = datetime.fromisoformat(value) if isinstance(value, str) else value
    except ValueError:
        raise RetiredError(f"{value!r} is not a recorded time") from None
    if not isinstance(moment, datetime):
        raise RetiredError(f"{value!r} is not a recorded time")
    return moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)


def cutover(namespace: str, path: Path = RETIRED) -> datetime | None:
    """The ``retired_before`` time of the namespace, or None when the namespace retires nothing."""
    try:
        document = json.loads(path.read_text())
        entries = document["namespaces"]
        entry = entries.get(namespace)
        value = None if entry is None else entry["retired_before"]
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        raise RetiredError(f"{path.name} is missing or malformed") from None
    if value is None:
        return None
    if not isinstance(value, str) or not UTC_TIME.fullmatch(value):
        raise RetiredError(f"{path.name}: retired_before of {namespace} is not a UTC time such as 2026-11-12T00:00:00Z")
    return moment_of(value)


def is_retired(namespace: str, committed_at: Any, path: Path = RETIRED) -> bool:
    """True when the publication was committed (as its row records) before the namespace's cutover."""
    before = cutover(namespace, path)
    return before is not None and moment_of(committed_at) < before
