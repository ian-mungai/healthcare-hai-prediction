"""Secret-safe BLS HTTPS requests with durable quota reservations and bounded retries."""

import fcntl
import http.client
import json
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from scripts.acquisition import redownload_controls as controls
from scripts.acquisition.bls_api_contract import digest, request_for
from scripts.acquisition.s3_store import AwsCli, encoded_json, write_once
from scripts.acquisition.source_registry import REPO_ROOT, read_json, require


class QuotaPause(ValueError):
    """Collection can resume after the rolling local or publisher quota resets."""


class RetryableRequest(ValueError):
    """Sanitized transient transport failure."""


@contextmanager
def collection_lock(root: Path) -> Iterator[None]:
    """Hold an OS lock shared by every batch in the collection; stale processes release it."""
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".collection.lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("BLS collector already running") from None
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def reserve(root: Path, identity: str, limit: int, now: datetime) -> None:
    """Reserve and persist a counted request before sending it, including failed attempts."""
    with controls.quota_lock(root, REPO_ROOT) as paths:
        if controls.recent(paths, now) >= limit:
            raise QuotaPause("Local BLS rolling-24-hour request budget reached; resume after oldest reservation expires")
        controls.begin(identity)
        paths = sorted((root / "requests").glob("*.json"))
        write_once(root / "requests" / f"{len(paths) + 1:06}.json", encoded_json({"batch_id": identity, "reserved_at_utc": now.isoformat()}))


def request_live(plan: dict, batch: dict, client: AwsCli) -> bytes:
    """Consume the AWS secret only inside this runtime boundary; return public data bytes."""
    identity = client.call("sts", "get-caller-identity")
    settings = client.configuration
    expected = f"arn:aws:iam::{settings['expected_account_id']}:user/{settings['aws_profile']}"
    require(identity.get("Account") == settings["expected_account_id"] and identity.get("Arn") == expected, "BLS project identity differs; secret not read")
    secret = client.call("secretsmanager", "get-secret-value", ["--secret-id", plan["credential_reference"]])
    try:
        credential = json.loads(secret["SecretString"])["api_key"]
        require(isinstance(credential, str) and len(credential) >= 16 and credential.strip() == credential, "BLS credential structure invalid")
    except (KeyError, TypeError, json.JSONDecodeError):
        raise ValueError("BLS credential structure invalid") from None
    parsed = urlsplit(plan["endpoint"])
    connection = http.client.HTTPSConnection(parsed.netloc, timeout=60)
    try:
        body = json.dumps(request_for(batch) | {"registrationkey": credential}).encode()
        connection.request("POST", parsed.path, body=body, headers={"Content-Type": "application/json", "Accept": "application/json"})
        response = connection.getresponse()
        controls.response_status(response)
        if response.status == 429:
            raise QuotaPause("BLS publisher quota reached; do not retry now")
        if response.status >= 500:
            raise RetryableRequest("BLS server unavailable")
        require(response.status == 200 and "json" in response.getheader("Content-Type", ""), "BLS HTTP status/type rejected; no redirect followed")
        raw = controls.read(response, 32 * 1024**2 + 1)
        require(0 < len(raw) <= 32 * 1024**2, "BLS response size rejected")
        require(credential.encode() not in raw and json.dumps(credential)[1:-1].encode() not in raw, "BLS response echoed credential; nothing persisted")
        payload = json.loads(raw)
        require(credential not in json.dumps(payload, ensure_ascii=False), "BLS response echoed credential; nothing persisted")
        require(isinstance(payload, dict), "BLS response envelope invalid")
        if payload.get("status") != "REQUEST_SUCCEEDED":
            messages = str(payload.get("message", [])).lower()
            if any(t in messages for t in ("daily", "threshold", "quota", "rate limit")):
                raise QuotaPause("BLS publisher quota reached; do not retry now")
            raise ValueError("BLS rejected request; details withheld to protect credentials")
        return raw
    except (OSError, http.client.HTTPException):
        raise RetryableRequest("BLS network request failed; response details withheld") from None
    except json.JSONDecodeError:
        raise ValueError("BLS returned invalid JSON; response details withheld") from None
    finally:
        connection.close()


def fetch(
    plan: dict, batch: dict, root: Path, allow_network: bool, client: AwsCli | None, request: Callable | None = None, sleep: Callable = time.sleep
) -> dict:
    """Reuse one atomic response/provenance envelope or make at most three counted requests."""
    path = root / "batches" / batch["id"] / "transport.json"
    if path.exists():
        envelope = read_json(path)
        raw = bytes.fromhex(envelope["body_hex"])
        require(envelope["request"] == request_for(batch) and envelope["endpoint"] == plan["endpoint"], "BLS cached request differs")
        require(envelope["sha256"] == digest(raw) and envelope["bytes"] == len(raw), "BLS cached response differs")
        return envelope
    if not allow_network or client is None:
        raise ValueError("BLS response missing; --fetch required")
    action = request or request_live
    for attempt in range(3):
        reserve(root, batch["id"], plan["request_limit_24h"], datetime.now(UTC))
        try:
            raw = controls.returned(action, plan, batch, client)
            break
        except RetryableRequest:
            if attempt == 2:
                raise
            sleep(2 ** (attempt + 1))
    envelope = {
        "endpoint": plan["endpoint"],
        "request": request_for(batch),
        "retrieved_at_utc": datetime.now(UTC).isoformat(),
        "sha256": digest(raw),
        "bytes": len(raw),
        "body_hex": raw.hex(),
        "http_status": 200,
    }
    write_once(path, encoded_json(envelope))
    sleep(plan["request_spacing_seconds"])
    return envelope
