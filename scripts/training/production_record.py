"""The Production record of each infection type, under ``promote/production/<HAI_TYPE>/`` in the data bucket.

After the promote action moves the ``production`` alias of a model (or finds it already there), it writes one record per promotion run: the infection type,
the model, the version, the run, the mode, the lease generation of the run, the version's ``feature_set`` and ``feature_set_schema_sha256`` tags and the time.
The features job reads these records to decide whether an online model set loads or holds (``scripts.features.feast_sets``), so it needs no MLflow access:
only the training jobs and the owner hold MLflow credentials (owner decision "Production schema source for online sets", Nov 10 2026).

The key is ``promote/production/<HAI_TYPE>/<generation>.json`` with the ten-digit lease generation of the run (``promote_lease``). The lease serializes the
promotions of one model and its generation only grows, so the record with the highest key is the newest Production state of the type, whatever the clocks
say. A record is written create-only. A retry of the same run holds the same generation and finds its own record: it keeps the first one when every field but
the time is the same, and refuses any other record under that key.

Limit: a run the owner abandons (``promote_owner resolve``) writes no record, so after such a resolution the newest record can name the version production
held before that run.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

from scripts.training import promote_receipt
from scripts.training.promote_receipt import ReceiptError

PREFIX = f"{promote_receipt.PREFIX}/production"
HAI_TYPE = re.compile(r"HAI_[0-9]{1,2}")
KEY = re.compile(r"([0-9]{10})\.json")
SET_TAG = "feature_set"
HASH_TAG = "feature_set_schema_sha256"
TIME = "promoted_at"
FIELDS = ("schema", "hai_type", "model", "version", "run_id", "mode", "generation", SET_TAG, HASH_TAG)


def prefix_of(hai_type: str) -> str:
    """The key prefix of the records of one infection type. Raises ReceiptError for a name that is not an infection type."""
    if not HAI_TYPE.fullmatch(hai_type):
        raise ReceiptError(f"{hai_type!r} is not an infection type")
    return f"{PREFIX}/{hai_type}/"


def key_of(hai_type: str, generation: int) -> str:
    """The object key of the record that the run holding lease generation ``generation`` writes."""
    return f"{prefix_of(hai_type)}{generation:010d}.json"


def build(hai_type: str, request: Mapping[str, Any], generation: int, tags: Mapping[str, Any], at: str) -> dict[str, Any]:
    """The record of a promotion run that left ``request['version']`` in production."""
    return {
        "schema": 1,
        "hai_type": hai_type,
        "model": request["model"],
        "version": request["version"],
        "run_id": request["run_id"],
        "mode": request["mode"],
        "generation": generation,
        SET_TAG: tags.get(SET_TAG),
        HASH_TAG: tags.get(HASH_TAG),
        TIME: at,
    }


def parse(body: bytes, key: str) -> dict[str, Any]:
    """The stored record, or ReceiptError when the bytes are not a record."""
    try:
        record = json.loads(body)
    except ValueError:
        record = None
    if not isinstance(record, dict) or set(record) != {*FIELDS, TIME}:
        raise ReceiptError(f"{key} is not a Production record")
    return record


def same(first: Mapping[str, Any], second: Mapping[str, Any]) -> bool:
    """Whether two records name the same promotion (every field but the time)."""
    return all(first.get(name) == second.get(name) for name in FIELDS)


def store(client: Any, bucket: str, record: Mapping[str, Any]) -> str:
    """Write the record once and return ``created`` or ``kept``.

    A record under the key that names the same promotion is kept (a retry). Another record, or a write that is neither stored nor readable, raises ReceiptError.
    """
    key = key_of(str(record["hai_type"]), int(record["generation"]))
    try:
        if promote_receipt.put_once(client, bucket, key, promote_receipt.canonical(record)):
            return "created"
    except promote_receipt.Uncertain:
        pass
    stored = promote_receipt.read(client, bucket, key)
    if stored is None:
        raise ReceiptError(f"{key} was not stored and does not exist")
    if not same(parse(stored, key), record):
        raise ReceiptError(f"{key} already holds the Production record of another promotion")
    return "kept"


def newest(client: Any, bucket: str, hai_type: str, model: str) -> dict[str, Any] | None:
    """The newest Production record of the infection type, or None when no promotion wrote one.

    The listing decides which records exist. A key the promote action does not write, a record of another type or model, or a record whose generation is not
    its key raises ReceiptError, so a damaged prefix never reads as an older Production state.
    """
    prefix = prefix_of(hai_type)
    generations: list[int] = []
    for key in promote_receipt.list_keys(client, bucket, prefix):
        match = KEY.fullmatch(key.removeprefix(prefix))
        if match is None:
            raise ReceiptError(f"{key} is not a Production record, so the Production model of {hai_type} cannot be read")
        generations.append(int(match.group(1)))
    if not generations:
        return None
    key = key_of(hai_type, max(generations))
    record = parse(promote_receipt.fetch(client, bucket, key), key)
    if record["hai_type"] != hai_type or record["model"] != model or record["generation"] != max(generations):
        raise ReceiptError(f"{key} names {record['hai_type']} {record['model']} generation {record['generation']}, not {hai_type} {model}")
    return record


def current(client: Any, bucket: str, models: Mapping[str, str]) -> dict[str, dict[str, Any] | None]:
    """The newest record of each infection type of ``models`` (infection type to registered model name), None for a type without one."""
    return {hai_type: newest(client, bucket, hai_type, models[hai_type]) for hai_type in sorted(models)}
