"""Bounded HUD GET transport: runtime-only bearer token, no redirects and safe immutable cache."""

import http.client
import json
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode, urlsplit

from scripts.acquisition import hud_api_contract as contract
from scripts.acquisition import redownload_controls as controls
from scripts.acquisition.bls_api_transport import RetryableRequest
from scripts.acquisition.bls_api_transport import collection_lock as collection_lock
from scripts.acquisition.s3_store import AwsCli, encoded_json, write_once
from scripts.acquisition.source_registry import read_json, require


def request_live(plan: dict, batch: dict, client: AwsCli) -> bytes:
    """Read only the named secret inside the request runtime; send it only as a header."""
    require(plan["endpoint"] == contract.ENDPOINT and plan["credential_reference"] == contract.CREDENTIAL, "HUD request scope differs; secret not read")
    parameters = contract.request_for(batch)
    identity, settings = client.call("sts", "get-caller-identity"), client.configuration
    expected = f"arn:aws:iam::{settings['expected_account_id']}:user/{settings['aws_profile']}"
    require(identity.get("Account") == settings["expected_account_id"] and identity.get("Arn") == expected, "HUD project identity differs; secret not read")
    secret = client.call("secretsmanager", "get-secret-value", ["--secret-id", contract.CREDENTIAL])
    # Same JSON {"api_key": ...} shape as the shared BLS and Census secrets.
    try:
        credential = json.loads(secret["SecretString"])["api_key"]
    except (KeyError, TypeError, json.JSONDecodeError):
        raise ValueError("HUD credential structure invalid") from None
    require(isinstance(credential, str) and re.fullmatch(r"[A-Za-z0-9._-]{20,4096}", credential) is not None, "HUD credential structure invalid")
    parsed = urlsplit(contract.ENDPOINT)
    connection = http.client.HTTPSConnection(parsed.netloc, timeout=300)
    try:
        headers = {"Accept": "application/json", "Authorization": f"Bearer {credential}"}
        connection.request("GET", parsed.path + "?" + urlencode(parameters), headers=headers)
        response = connection.getresponse()
        controls.response_status(response)
        if response.status in {429, 500, 502, 503, 504}:
            raise RetryableRequest("HUD transient publisher failure")
        require(
            response.status == 200 and "json" in response.getheader("Content-Type", ""),
            f"HUD HTTP {response.status} or non-JSON type rejected; no redirect followed",
        )
        raw = controls.read(response, 256 * 1024**2 + 1)
        require(0 < len(raw) <= 256 * 1024**2, "HUD response size rejected")
        require(credential.encode() not in raw, "HUD response echoed credential; nothing persisted")
        # Envelope and field checks run after caching, so a schema change is reviewable offline.
        json.loads(raw)
        return raw
    except (OSError, http.client.HTTPException):
        raise RetryableRequest("HUD network failure; request details withheld") from None
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise ValueError("HUD invalid JSON; response details withheld") from None
    finally:
        connection.close()


def fetch(
    plan: dict, batch: dict, root: Path, allow_network: bool, client: AwsCli | None, request: Callable | None = None, sleep: Callable = time.sleep
) -> dict:
    """Reuse successful raw transport or retry only transient failures three times."""
    path = root / "batches" / batch["id"] / "transport.json"
    if path.exists():
        envelope = read_json(path)
        raw = bytes.fromhex(envelope["body_hex"])
        require(envelope["request"] == contract.request_for(batch) and envelope["endpoint"] == contract.ENDPOINT, "HUD cached request differs")
        require(envelope["sha256"] == contract.digest(raw) and envelope["bytes"] == len(raw) and envelope["http_status"] == 200, "HUD cached response differs")
        return envelope
    if not allow_network or client is None:
        raise ValueError("HUD response missing; --fetch required")
    action = request or request_live
    for attempt in range(3):
        controls.begin(batch["id"])
        try:
            raw = controls.returned(action, plan, batch, client)
            break
        except RetryableRequest:
            if attempt == 2:
                raise
            sleep(2 ** (attempt + 1))
    envelope = {
        "endpoint": contract.ENDPOINT,
        "request": contract.request_for(batch),
        "retrieved_at_utc": datetime.now(UTC).isoformat(),
        "sha256": contract.digest(raw),
        "bytes": len(raw),
        "body_hex": raw.hex(),
        "http_status": 200,
    }
    write_once(path, encoded_json(envelope))
    sleep(plan["request_spacing_seconds"])
    return envelope
