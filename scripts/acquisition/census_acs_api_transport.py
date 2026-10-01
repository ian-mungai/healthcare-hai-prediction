"""Bounded Census GET transport: runtime-only key, no redirects and safe immutable cache."""

import http.client
import json
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode, urlsplit

from scripts.acquisition import census_acs_api_contract as contract
from scripts.acquisition import redownload_controls as controls
from scripts.acquisition.bls_api_transport import RetryableRequest
from scripts.acquisition.bls_api_transport import collection_lock as collection_lock
from scripts.acquisition.s3_store import AwsCli, encoded_json, write_once
from scripts.acquisition.source_registry import read_json, require


def request_live(plan: dict, batch: dict, client: AwsCli) -> bytes:
    """Read only the named secret inside the request runtime; never persist its URL."""
    require(plan["endpoint"] == contract.ENDPOINT and plan["credential_reference"] == "census_api_key", "Census request scope differs; secret not read")
    return request_json(contract.ENDPOINT, contract.request_for(batch), client)


def request_json(endpoint: str, parameters: dict, client: AwsCli) -> bytes:
    """GET one public Census endpoint with the runtime-only key; never persist its URL or echo."""
    require(endpoint.startswith("https://api.census.gov/data/") and "key" not in parameters, "Census request scope differs; secret not read")
    identity, settings = client.call("sts", "get-caller-identity"), client.configuration
    expected = f"arn:aws:iam::{settings['expected_account_id']}:user/{settings['aws_profile']}"
    require(identity.get("Account") == settings["expected_account_id"] and identity.get("Arn") == expected, "Census project identity differs; secret not read")
    secret = client.call("secretsmanager", "get-secret-value", ["--secret-id", "census_api_key"])
    try:
        credential = json.loads(secret["SecretString"])["api_key"]
        require(isinstance(credential, str) and len(credential) >= 16 and credential.isascii() and credential.isalnum(), "Census credential structure invalid")
    except (KeyError, TypeError, json.JSONDecodeError):
        raise ValueError("Census credential structure invalid") from None
    parsed = urlsplit(endpoint)
    connection = http.client.HTTPSConnection(parsed.netloc, timeout=120)
    try:
        connection.request("GET", parsed.path + "?" + urlencode(parameters | {"key": credential}), headers={"Accept": "application/json"})
        response = connection.getresponse()
        controls.response_status(response)
        if response.status in {429, 500, 502, 503, 504}:
            raise RetryableRequest("Census transient publisher failure")
        require(response.status == 200 and "json" in response.getheader("Content-Type", ""), "Census HTTP status/type rejected; no redirect followed")
        raw = controls.read(response, 64 * 1024**2 + 1)
        require(0 < len(raw) <= 64 * 1024**2, "Census response size rejected")
        require(credential.encode() not in raw, "Census response echoed credential; nothing persisted")
        payload = json.loads(raw)
        require(credential not in json.dumps(payload, ensure_ascii=False), "Census response echoed credential; nothing persisted")
        require(isinstance(payload, list), "Census JSON response shape rejected")
        return raw
    except (OSError, http.client.HTTPException):
        raise RetryableRequest("Census network failure; request details withheld") from None
    except json.JSONDecodeError:
        raise ValueError("Census invalid JSON; response details withheld") from None
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
        require(envelope["request"] == contract.request_for(batch) and envelope["endpoint"] == contract.ENDPOINT, "Census cached request differs")
        require(
            envelope["sha256"] == contract.digest(raw) and envelope["bytes"] == len(raw) and envelope["http_status"] == 200, "Census cached response differs"
        )
        return envelope
    if not allow_network or client is None:
        raise ValueError("Census response missing; --fetch required")
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
