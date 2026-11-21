"""The capture receipts of the files bronze loads, read from S3 for the period generators (POS, ownership and geography).

Each loaded file's manifest lists exactly one capture receipt (role ``capture_receipt``) with the key, exact version ID, SHA-256 and length of the stored
object. The generators read that object by bucket, key and version, so a capture that exists only in S3 gets its period without any local acquisition
record. A manifest that lists no receipt or two, a listed object without a required field, two manifests that declare one stored receipt differently, and
bytes that differ from the recorded SHA-256 or length stop the run with the manifest named. Only the S3 reader is called: nothing is written.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from scripts.lakehouse.bronze import RetiredObjects, Storage

RECEIPT_ROLE = "capture_receipt"
FIELDS = ("table", "sha256", "release_id", "file_name")
SHA256 = re.compile(r"[0-9a-f]{64}")
Identity = tuple[str, str, str]


class ReceiptError(ValueError):
    """A receipt a manifest lists is missing, extra or malformed, or its bytes differ from the manifest's record."""


def source() -> tuple[Storage, str, dict[str, Any], RetiredObjects]:
    """The S3 reader, the project bucket, the table map and the retired list that a real run reads."""
    from scripts.lakehouse import bronze
    from scripts.lakehouse.catalog import deployment

    settings = deployment()
    os.environ.setdefault("AWS_PROFILE", settings["aws_profile"])
    os.environ.setdefault("AWS_REGION", settings["aws_region"])
    return bronze.S3Storage(), settings["data_bucket_name"], bronze.load_table_map(), bronze.load_retired()


def listed_receipt(manifest_key: str, evidence: Iterable[Mapping[str, Any]], bucket: str) -> dict[str, Any]:
    """The one receipt a manifest lists, with every required field present; a listed object without a bucket is in the configured bucket."""
    entries = [entry for entry in evidence if entry.get("role") == RECEIPT_ROLE]
    if len(entries) != 1:
        raise ReceiptError(f"manifest {manifest_key} lists {len(entries)} capture receipts; exactly one is required")
    stored = entries[0].get("object")
    if not isinstance(stored, Mapping):
        raise ReceiptError(f"manifest {manifest_key}: its capture receipt names no object")
    valid = {
        "key": isinstance(stored.get("key"), str) and bool(stored["key"]),
        "version_id": isinstance(stored.get("version_id"), str) and bool(stored["version_id"]),
        "sha256": isinstance(stored.get("sha256"), str) and SHA256.fullmatch(stored["sha256"]) is not None,
        "byte_count": isinstance(stored.get("byte_count"), int) and not isinstance(stored["byte_count"], bool) and stored["byte_count"] >= 0,
    }
    for field, ok in valid.items():
        if not ok:
            raise ReceiptError(f"manifest {manifest_key}: its capture receipt lacks {field}, or it is not valid")
    return {
        "bucket": stored.get("bucket") or bucket,
        "key": stored["key"],
        "version_id": stored["version_id"],
        "sha256": stored["sha256"],
        "byte_count": stored["byte_count"],
    }


def stored_receipts(storage: Storage, bucket: str, inputs: Iterable[Mapping[str, Any]], tables: Iterable[str]) -> dict[str, list[dict[str, Any]]]:
    """Read the receipts of the loaded files of the tables, by bucket, key and version, once each; return them by table.

    Each manifest is checked first (one receipt, every field), then conflicting declarations of one stored receipt are refused before the receipts are
    deduplicated, then each receipt is read and checked against its SHA-256 and length.
    """
    wanted = set(tables)
    manifests: dict[tuple[str, str], dict[str, Any]] = {}
    for item in inputs:
        if item["table"] not in wanted:
            continue
        manifest = (str(item["manifest_key"]), str(item["manifest_version_id"]))
        if manifest not in manifests:
            evidence = json.loads(item["manifest_evidence_json"])["evidence_objects"]
            manifests[manifest] = {"stored": listed_receipt(manifest[0], evidence, bucket), "tables": set()}
        manifests[manifest]["tables"].add(item["table"])
    declared: dict[Identity, tuple[str, dict[str, Any]]] = {}
    for (manifest_key, _version), info in sorted(manifests.items()):
        stored = info["stored"]
        first_key, first = declared.setdefault((stored["bucket"], stored["key"], stored["version_id"]), (manifest_key, stored))
        if (first["sha256"], first["byte_count"]) != (stored["sha256"], stored["byte_count"]):
            raise ReceiptError(
                f"conflicting declarations of receipt {stored['key']} (version {stored['version_id']}): manifest {first_key} and manifest {manifest_key} "
                "record a different SHA-256 or length"
            )
    bodies: dict[Identity, dict[str, Any]] = {}
    for identity, (manifest_key, stored) in sorted(declared.items()):
        body = storage.get(*identity)
        if hashlib.sha256(body).hexdigest() != stored["sha256"] or len(body) != stored["byte_count"]:
            raise ReceiptError(
                f"manifest {manifest_key}: receipt {identity[1]} (version {identity[2]}) differs from the SHA-256 or length its manifest records"
            )
        try:
            receipt = json.loads(body)
        except ValueError as error:
            raise ReceiptError(f"manifest {manifest_key}: receipt {identity[1]} is not valid JSON") from error
        if not isinstance(receipt, dict):
            raise ReceiptError(f"manifest {manifest_key}: receipt {identity[1]} is not a JSON object")
        bodies[identity] = receipt
    by_table: dict[str, dict[Identity, dict[str, Any]]] = defaultdict(dict)
    for info in manifests.values():
        stored = info["stored"]
        identity = (stored["bucket"], stored["key"], stored["version_id"])
        for table in info["tables"]:
            by_table[table][identity] = bodies[identity]
    return {table: [receipts[identity] for identity in sorted(receipts)] for table, receipts in by_table.items()}


def loaded(tables: Iterable[str], receipted: Iterable[str]) -> tuple[list[dict[str, str]], dict[str, list[dict[str, Any]]]]:
    """The loaded files of the tables, as bronze discovers them from the S3 manifests, and the receipts of the tables that take their period from one."""
    from scripts.lakehouse import bronze

    storage, bucket, config, retired = source()
    inputs, _ = bronze.discover(storage, bucket, config, tuple(tables), retired)
    return [{name: item[name] for name in FIELDS} for item in inputs], stored_receipts(storage, bucket, inputs, receipted)
