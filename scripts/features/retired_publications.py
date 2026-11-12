"""The features publications that the owner retired: the runs of the old single path, recorded as a cutover.

The cutover is in ``data_contracts/features/retired_publications.json``.

A run ID starts with its UTC start time (``20261106T000414Z-48f409da``). A reader of a namespace with an entry ignores every run that started before the
entry's ``retired_before`` time. A namespace without an entry (the fixture namespace ``features_e2e``) retires nothing. The module imports nothing outside the
standard library, so the readers of the feature image and the host checks share it.
"""

from __future__ import annotations

import json
from pathlib import Path

RETIRED = Path(__file__).resolve().parents[2] / "data_contracts/features/retired_publications.json"
STAMP_LENGTH = 16


class RetiredError(ValueError):
    """The retirement record is missing or malformed."""


def cutover(namespace: str, path: Path = RETIRED) -> str | None:
    """The ``retired_before`` time of the namespace, or None when the namespace retires nothing."""
    try:
        document = json.loads(path.read_text())
        entries = document["namespaces"]
        entry = entries.get(namespace)
        value = None if entry is None else entry["retired_before"]
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        raise RetiredError(f"{path.name} is missing or malformed") from None
    if value is not None and (not isinstance(value, str) or len(value) != STAMP_LENGTH or not value.endswith("Z")):
        raise RetiredError(f"{path.name}: retired_before of {namespace} is not a run ID time")
    return value


def is_retired(namespace: str, run_id: str, path: Path = RETIRED) -> bool:
    """True when the run started before the namespace's cutover."""
    before = cutover(namespace, path)
    return before is not None and run_id[:STAMP_LENGTH] < before
