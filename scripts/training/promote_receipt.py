"""The durable copy of the detailed promotion receipt, under ``promote/<run>/promote.json`` in the data bucket.

The receipt in the task workspace names the version that production held before. The workspace ends with the task, so the promote phase copies the receipt
here before it reports success. The write is create-only. A run ID is the identity of the promotion, so a second write for it keeps the first receipt:
the same bytes are accepted, and so is a receipt of the same promotion whose ``previous_version`` already names the promoted version, which is what a
rerun in a new task records after the first task moved the alias. Any other receipt under the key is refused.
"""

from __future__ import annotations

import json
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

PREFIX = "promote"
RECORD = "promote.json"
EXISTS = ("PreconditionFailed", "ConditionalRequestConflict")
SAME_PROMOTION = ("schema", "run_id", "model", "version", "mode")


class ReceiptError(Exception):
    """The durable receipt cannot be stored or does not match the one stored."""


def key_of(run: str) -> str:
    """The object key of one run's receipt."""
    return f"{PREFIX}/{run}/{RECORD}"


def s3_client() -> Any:
    """The S3 client of the task, with bounded connection and read times."""
    import boto3
    from botocore.config import Config

    return boto3.client("s3", config=Config(connect_timeout=5, read_timeout=15, retries={"max_attempts": 3, "mode": "standard"}))


def same_promotion(stored: bytes, body: bytes) -> bool:
    """Whether the stored receipt is this promotion's receipt, taken before a rerun found the alias already moved."""
    try:
        first, again = json.loads(stored), json.loads(body)
    except ValueError:
        return False
    if not isinstance(first, dict) or not isinstance(again, dict):
        return False
    return stored == body or (all(first.get(name) == again.get(name) for name in SAME_PROMOTION) and again.get("previous_version") == again.get("version"))


def upload(client: Any, bucket: str, run: str, body: bytes) -> str:
    """Store the receipt once and return ``created`` or ``kept``; raise ReceiptError when it cannot be stored or another receipt holds the key."""
    key = key_of(run)
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*", ContentType="application/json")
        return "created"
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") not in EXISTS:
            raise ReceiptError(f"the receipt {key} was not stored: {error.response.get('Error', {}).get('Code', 'ClientError')}") from None
    except BotoCoreError as error:
        raise ReceiptError(f"the receipt {key} was not stored: {type(error).__name__}") from None
    try:
        stored = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except (ClientError, BotoCoreError) as error:
        raise ReceiptError(f"the receipt {key} exists but cannot be read: {type(error).__name__}") from None
    if not same_promotion(stored, body):
        raise ReceiptError(f"{key} already holds another receipt")
    return "kept"
