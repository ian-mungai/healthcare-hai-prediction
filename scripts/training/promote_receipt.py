"""The durable records of one promotion, under ``promote/<run>/`` in the data bucket.

The workspace of a task ends with the task, so these records outlive it. All are written create-only, and a run ID is the identity of the promotion. The
model lease (``promote_lease``) gives the run its exclusive right to move the model before the first of them is written.

- ``intent.json`` is written before the alias moves. It names the promotion (schema, run, model, version, mode) and the version production held when the
  promotion started. A retry in a new task reads it instead of observing production again, so the true ``previous_version`` survives a crash between the
  alias move and the receipt.
- ``attempt.json`` is written once, immediately before the alias request is sent, and only the call that created it sends the request. A retry that finds it
  knows an earlier attempt may have sent the request, and sends nothing (``promote.start_attempt``).
- ``promote.json`` is the receipt. It is the intent's promotion with the intent's ``previous_version``, and it is the only completion record. A second write
  keeps the first receipt and accepts only the same bytes.
- ``abandoned.json`` is written by the owner (``promote_owner resolve``) for a run that will never finish. The run then refuses to start again.

Absence is decided by an explicit prefix listing (``records_of``), never by a failed read: S3 answers a missing key with 403 instead of 404 when the role
cannot list, and a refused read is not evidence. A key is read only after the listing showed it.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

PREFIX = "promote"
RECORD = "promote.json"
INTENT = "intent.json"
ATTEMPT = "attempt.json"
ABANDONED = "abandoned.json"
MAX_PAGES = 50
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


def open_attempt(client: Any, bucket: str, run: str, request: Mapping[str, Any]) -> bool:
    """Store the attempt record once and return True only when this call created it.

    A write whose reply was lost is read back. The stored record then cannot be told from an earlier attempt's, so the answer is False and the caller sends
    nothing. A record that is not stored raises ReceiptError.
    """
    key = key_of(run, ATTEMPT)
    try:
        return put_once(client, bucket, key, canonical(request))
    except Uncertain:
        pass
    if read(client, bucket, key) is None:
        raise ReceiptError(f"{key} was not stored and does not exist")
    return False


def list_keys(client: Any, bucket: str, prefix: str) -> list[str]:
    """Every key under the prefix, in listing order. Raises ReceiptError when the listing is refused, fails or never ends."""
    keys: list[str] = []
    token: str | None = None
    for _ in range(MAX_PAGES):
        arguments: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token is not None:
            arguments["ContinuationToken"] = token
        try:
            page = client.list_objects_v2(**arguments)
        except ClientError as error:
            raise ReceiptError(f"{prefix} cannot be listed: {code_of(error)}") from None
        except BotoCoreError as error:
            raise ReceiptError(f"{prefix} cannot be listed: {type(error).__name__}") from None
        keys.extend(str(item["Key"]) for item in page.get("Contents", []))
        if not page.get("IsTruncated"):
            return keys
        token = page.get("NextContinuationToken")
        if not token:
            raise ReceiptError(f"{prefix} listing is truncated without a continuation token")
    raise ReceiptError(f"{prefix} lists more than {MAX_PAGES} pages")


def records_of(client: Any, bucket: str, run: str) -> set[str]:
    """The names of the records the run has stored. A listing that fails raises ReceiptError, so no failure is read as an empty run."""
    prefix = f"{PREFIX}/{run}/"
    return {key.removeprefix(prefix) for key in list_keys(client, bucket, prefix)}


def fetch(client: Any, bucket: str, key: str) -> bytes:
    """The bytes of a key the listing showed. A key that cannot be read raises ReceiptError."""
    stored = read(client, bucket, key)
    if stored is None:
        raise ReceiptError(f"{key} was listed and cannot be read")
    return stored


def stored_intent(client: Any, bucket: str, run: str) -> dict[str, Any]:
    """The stored intent of a run whose listing showed it."""
    key = key_of(run, INTENT)
    return parse_intent(fetch(client, bucket, key), key)


def upload(client: Any, bucket: str, run: str, body: bytes) -> str:
    """Store the receipt once and return ``created`` or ``kept``; raise ReceiptError when it cannot be stored or other bytes hold the key."""
    key = key_of(run)
    if put_once(client, bucket, key, body):
        return "created"
    stored = read(client, bucket, key)
    if stored != body:
        raise ReceiptError(f"{key} already holds another receipt")
    return "kept"
