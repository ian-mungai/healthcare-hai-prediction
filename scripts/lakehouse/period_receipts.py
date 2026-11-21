"""The capture receipts of the files bronze loads, read from S3 for the period generators (POS, ownership and geography).

A loaded file's manifest lists its capture receipt (role ``capture_receipt``) with the key, exact version ID, SHA-256 and length of the stored object. The
generators read that object by key and version, so a capture that exists only in S3 gets its period without any local acquisition record. The bytes are
checked against the manifest's SHA-256 and length, and a mismatch stops the run. Only the S3 reader is called: nothing is written.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from scripts.lakehouse.bronze import RetiredObjects, Storage

RECEIPT_ROLE = "capture_receipt"
FIELDS = ("table", "sha256", "release_id", "file_name")


class ReceiptError(ValueError):
    """A receipt the manifest lists cannot be read, or its bytes differ from what the manifest records."""


def source() -> tuple[Storage, str, dict[str, Any], RetiredObjects]:
    """The S3 reader, the project bucket, the table map and the retired list that a real run reads."""
    from scripts.lakehouse import bronze
    from scripts.lakehouse.catalog import deployment

    settings = deployment()
    os.environ.setdefault("AWS_PROFILE", settings["aws_profile"])
    os.environ.setdefault("AWS_REGION", settings["aws_region"])
    return bronze.S3Storage(), settings["data_bucket_name"], bronze.load_table_map(), bronze.load_retired()


def stored_receipts(storage: Storage, bucket: str, inputs: Iterable[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Read the receipts that the manifests of the loaded files list, by key and version, once each; return them by table. Check each SHA-256 and length."""
    objects: dict[tuple[str, str], Mapping[str, Any]] = {}
    by_table: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for item in inputs:
        for entry in json.loads(item["manifest_evidence_json"])["evidence_objects"]:
            if entry.get("role") == RECEIPT_ROLE:
                identity = (entry["object"]["key"], entry["object"]["version_id"])
                objects[identity] = entry["object"]
                by_table[item["table"]].add(identity)
    bodies = {}
    for identity, stored in sorted(objects.items()):
        body = storage.get(stored.get("bucket") or bucket, *identity)
        if hashlib.sha256(body).hexdigest() != stored["sha256"] or len(body) != stored["byte_count"]:
            raise ReceiptError(f"receipt {identity[0]} (version {identity[1]}) differs from the SHA-256 or length its manifest records")
        bodies[identity] = json.loads(body)
    return {table: [bodies[identity] for identity in sorted(identities)] for table, identities in by_table.items()}


def loaded(tables: Iterable[str]) -> tuple[list[dict[str, str]], dict[str, list[dict[str, Any]]]]:
    """The loaded files of the tables, as bronze discovers them from the S3 manifests, and the receipts those manifests list, by table."""
    from scripts.lakehouse import bronze

    storage, bucket, config, retired = source()
    inputs, _ = bronze.discover(storage, bucket, config, tuple(tables), retired)
    return [{name: item[name] for name in FIELDS} for item in inputs], stored_receipts(storage, bucket, inputs)
