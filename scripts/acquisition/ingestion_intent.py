"""The intent of one capture attempt: the snapshot ID a retry after a crash reuses, so its data objects keep their keys.

Capture mode writes the intent at ``<root>ingestion_intents/<unit_key>/<generation>.json`` before it stores any object of the unit, create-once. The
generation is the number of the unit's completion records plus one, so a later capture of a changed unit gets a new intent and a new snapshot ID. A
retry finds its predecessor's intent, rewrites its own receipt to that snapshot ID and stores under the same keys. No skip decision reads this folder.
Nothing in this module reads or writes storage.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

from scripts.acquisition.completion_record import DIGEST, canonical

KIND = "ingestion_unit_intent"
SCHEMA_VERSION = 1
INTENTS_FOLDER = "ingestion_intents/"
SNAPSHOT_ID = re.compile(r"(?P<source>[A-Za-z0-9._-]+)__[0-9]{8}T[0-9]{6}Z__[a-f0-9]{8,64}")
FIELDS = frozenset({"kind", "schema_version", "unit_key", "generation", "snapshot_id", "run_id"})


class IntentError(ValueError):
    """An intent that is malformed or does not agree with its own key. It is refused, never replaced by a new snapshot ID."""


def snapshot_source(snapshot_id: str) -> str | None:
    """The source ID a snapshot ID starts with, or None when it is not a snapshot ID."""
    found = SNAPSHOT_ID.fullmatch(snapshot_id)
    return found.group("source") if found else None


def intent_prefix(root: str, key: str) -> str:
    return f"{root}{INTENTS_FOLDER}{key}/"


def intent_key(root: str, key: str, generation: int) -> str:
    return f"{intent_prefix(root, key)}{generation}.json"


def build(*, unit_key: str, generation: int, snapshot_id: str, run_id: str) -> dict[str, Any]:
    intent = {"kind": KIND, "schema_version": SCHEMA_VERSION, "unit_key": unit_key, "generation": generation, "snapshot_id": snapshot_id, "run_id": run_id}
    check(intent)
    return intent


def encode(intent: Mapping[str, Any]) -> bytes:
    """The bytes a writer stores: the canonical form plus a final newline."""
    return canonical(intent) + b"\n"


def check(intent: Any) -> None:
    """Refuse an intent that is not exactly this schema."""
    if not (isinstance(intent, dict) and set(intent) == FIELDS and intent["kind"] == KIND and intent["schema_version"] == SCHEMA_VERSION):
        raise IntentError("Intent has other fields than its schema")
    generation = intent["generation"]
    if not (isinstance(generation, int) and not isinstance(generation, bool) and generation >= 1):
        raise IntentError("Intent generation is not a positive whole number")
    if not (isinstance(intent["unit_key"], str) and DIGEST.fullmatch(intent["unit_key"])):
        raise IntentError("Intent unit key is malformed")
    if not (isinstance(intent["run_id"], str) and intent["run_id"]):
        raise IntentError("Intent run ID is empty or not text")
    if not (isinstance(intent["snapshot_id"], str) and snapshot_source(intent["snapshot_id"])):
        raise IntentError("Intent snapshot ID is malformed")


def parse(body: bytes, *, key: str, root: str, source_id: str | None = None) -> dict[str, Any]:
    """Read a stored intent by its key: the key must be exactly the one its own unit key and generation name, so a copied or renamed intent is refused.

    With ``source_id`` the snapshot ID must also belong to that source.
    """
    try:
        intent = json.loads(body)
    except ValueError:
        raise IntentError("Intent is not JSON") from None
    check(intent)
    if key != intent_key(root, intent["unit_key"], intent["generation"]):
        raise IntentError("Intent differs from the unit and generation its key names")
    if source_id is not None and snapshot_source(intent["snapshot_id"]) != source_id:
        raise IntentError("Intent snapshot ID belongs to another source")
    return intent
