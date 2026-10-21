"""The gold publish records the launcher reads: name patterns, the consumer result name, reading a record and the one rule that accepts a consumer result.

They use the standard library only, so the launcher can import them without depending on the publish steps' Spark runtime. ``gold_publish`` imports
them from here.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
SHA256 = re.compile(r"[a-f0-9]{64}")
RESULT_NAME = "consumer_result.json"
READER_NAME = re.compile(r"[A-Za-z0-9_.:-]{1,128}")
# The Athena API returns a column name of 1 to 128 characters (Glue stores 255); Iceberg keeps column names in metadata files, not in PostgreSQL (63).
IDENT_MAX = 128


class PublishError(RuntimeError):
    """A step refuses or fails; the message carries names, counts and hashes, never row values or credentials."""


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_record(path: Path) -> tuple[dict[str, Any], str]:
    """A record and its SHA-256, or a refusal when it is absent or unreadable."""
    if not path.is_file():
        raise PublishError(f"{path.name} is missing")
    data = path.read_bytes()
    try:
        document = json.loads(data)
    except ValueError as error:
        raise PublishError(f"{path.name} is not valid JSON") from error
    if not isinstance(document, dict):
        raise PublishError(f"{path.name} is not a record")
    return document, sha256_bytes(data)


def accept_consumer(result: Mapping[str, Any], completion: Mapping[str, Any], run_id: str) -> None:
    """The one acceptance rule for a consumer result, used by the launcher when it records its acceptance and by every publish step.

    Accept a result only when it belongs to ``run_id``, when the digest of the file it read inside its container and the digest it was told to expect both
    equal the digest in ``completion``, and when the consumer says it exported and names the manifest. Raises otherwise.
    """
    expected = str(completion.get("sha256", ""))
    if not SHA256.fullmatch(expected):
        raise PublishError("the completion record carries no SHA-256 of the sealed copy")
    if result.get("run_id") != run_id:
        raise PublishError("the consumer result is rejected: it belongs to another run")
    if result.get("in_container_sha256") != expected or result.get("expected_sha256") != expected:
        raise PublishError("the consumer result is rejected: the digest it computed in the container differs from the completion record")
    if result.get("exported") is not True or not SHA256.fullmatch(str(result.get("manifest_sha256", ""))):
        raise PublishError("the consumer result is rejected: it did not export")
