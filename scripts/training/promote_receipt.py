"""The durable records of one promotion, under ``promote/<run>/`` in the data bucket.

The workspace of a task ends with the task, so two records outlive it. Both are written create-only, and a run ID is the identity of the promotion.

- ``intent.json`` is written before the alias moves. It names the promotion (schema, run, model, version, mode) and the version production held when the
  promotion started. A retry in a new task reads it instead of observing production again, so the true ``previous_version`` survives a crash between the
  alias move and the receipt.
- ``promote.json`` is the receipt. It is the intent's promotion with the intent's ``previous_version``, and it is the only completion record. A second write
  keeps the first receipt and accepts only the same bytes.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

PREFIX = "promote"
RECORD = "promote.json"
INTENT = "intent.json"
EXISTS = ("PreconditionFailed", "ConditionalRequestConflict")
ABSENT = ("NoSuchKey", "NotFound", "404")
PROMOTION_FIELDS = ("schema", "run_id", "model", "version", "mode")


class ReceiptError(Exception):
    """A durable record cannot be stored or read, or does not match the one stored."""


class Uncertain(ReceiptError):
    """A write failed in transit, so the record may or may not be stored."""


class RequestConflict(ReceiptError):
    """The records of the run name another promotion than the request."""


def key_of(run: str, record: str = RECORD) -> str:
    """The object key of one record of one run."""
    return f"{PREFIX}/{run}/{record}"


def canonical(document: Mapping[str, Any]) -> bytes:
    """The canonical bytes of a record."""
    return (json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def s3_client() -> Any:
    """The S3 client of the task, with bounded connection and read times."""
    import boto3
    from botocore.config import Config

    return boto3.client("s3", config=Config(connect_timeout=5, read_timeout=15, retries={"max_attempts": 3, "mode": "standard"}))


def code_of(error: ClientError) -> str:
    """The S3 error code of a failed call."""
    return str(error.response.get("Error", {}).get("Code", "ClientError"))


def put_once(client: Any, bucket: str, key: str, body: bytes) -> bool:
    """Create the object and return True, or return False when the key already holds one; raise Uncertain when the reply was lost."""
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*", ContentType="application/json")
    except ClientError as error:
        if code_of(error) in EXISTS:
            return False
        raise ReceiptError(f"{key} was not stored: {code_of(error)}") from None
    except BotoCoreError as error:
        raise Uncertain(f"{key} was not stored: {type(error).__name__}") from None
    return True


def read(client: Any, bucket: str, key: str) -> bytes | None:
    """Return the stored bytes, or None when the key is absent. A refused read is never taken as absence."""
    try:
        return bytes(client.get_object(Bucket=bucket, Key=key)["Body"].read())
    except ClientError as error:
        if code_of(error) in ABSENT:
            return None
        raise ReceiptError(f"{key} cannot be read: {code_of(error)}") from None
    except BotoCoreError as error:
        raise ReceiptError(f"{key} cannot be read: {type(error).__name__}") from None


def parse_intent(stored: bytes, key: str) -> dict[str, Any]:
    """Return the stored intent, or raise ReceiptError when it is not an intent."""
    try:
        intent = json.loads(stored)
    except ValueError:
        intent = None
    if not isinstance(intent, dict) or set(intent) != {*PROMOTION_FIELDS, "previous_version"} or not isinstance(intent["previous_version"], (str, type(None))):
        raise ReceiptError(f"{key} is not a promotion intent")
    return intent


def open_intent(client: Any, bucket: str, run: str, request: Mapping[str, Any], observed: str | None) -> tuple[dict[str, Any], bool]:
    """Store the intent once and return it with whether this call created it.

    A stored intent wins over ``observed``: it holds the version production held before the first attempt. An intent of another promotion raises
    RequestConflict. A write whose reply was lost is read back, and a missing intent raises ReceiptError, so the alias never moves without one.
    """
    key = key_of(run, INTENT)
    wanted = {**request, "previous_version": observed}
    try:
        if put_once(client, bucket, key, canonical(wanted)):
            return wanted, True
    except Uncertain:
        pass
    stored = read(client, bucket, key)
    if stored is None:
        raise ReceiptError(f"{key} was not stored and does not exist")
    intent = parse_intent(stored, key)
    if any(intent[name] != request[name] for name in PROMOTION_FIELDS):
        raise RequestConflict(f"{key} records another promotion than this request")
    return intent, False


def completed(client: Any, bucket: str, run: str) -> bytes | None:
    """The stored receipt of the run, or None when there is none. Raises ReceiptError when the read is refused, because a refusal does not show absence."""
    return read(client, bucket, key_of(run))


def upload(client: Any, bucket: str, run: str, body: bytes) -> str:
    """Store the receipt once and return ``created`` or ``kept``; raise ReceiptError when it cannot be stored or other bytes hold the key."""
    key = key_of(run)
    if put_once(client, bucket, key, body):
        return "created"
    stored = read(client, bucket, key)
    if stored != body:
        raise ReceiptError(f"{key} already holds another receipt")
    return "kept"
