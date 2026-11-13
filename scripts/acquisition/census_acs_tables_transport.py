"""Bounded Census transport for the ACS table-vintage units: one keyed data request and one unkeyed dictionary request, stored as files.

The keyed request is the shared ``request_json`` (runtime-only key, 64 MiB cap, no redirect, echo check). The dictionary request has no key, an 8 MiB cap and
the same host check, status rule and retry rule. The raw replies are written as files, not as hex copies inside a JSON envelope.
"""

import http.client
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from scripts.acquisition import census_acs_tables_contract as contract
from scripts.acquisition import redownload_controls as controls
from scripts.acquisition.bls_api_contract import digest
from scripts.acquisition.bls_api_transport import RetryableRequest
from scripts.acquisition.census_acs_api_transport import request_json
from scripts.acquisition.s3_store import AwsCli, encoded_json, write_once
from scripts.acquisition.source_registry import read_json, require
from scripts.acquisition.transport import https_connection, require_approved

DICTIONARY_CAP = 8 * 1024**2
DICTIONARY_PATH = re.compile(r"/data/[0-9]{4}/acs/acs5(?:/subject|/profile)?/groups/[A-Z0-9]+\.json")
TRANSIENT = {429, 500, 502, 503, 504}


def request_dictionary(url: str) -> bytes:
    """GET one public group dictionary without a key; refuse any other host, path or query."""
    parsed = urlsplit(url)
    require(
        parsed.scheme == "https" and parsed.netloc == "api.census.gov" and not parsed.query and DICTIONARY_PATH.fullmatch(parsed.path) is not None,
        "Census tables dictionary request scope differs",
    )
    require_approved(url)
    connection = https_connection(parsed.netloc, timeout=120)
    try:
        connection.request("GET", parsed.path, headers={"Accept": "application/json"})
        response = connection.getresponse()
        controls.response_status(response)
        if response.status in TRANSIENT:
            raise RetryableRequest("Census transient publisher failure")
        require(response.status == 200 and "json" in response.getheader("Content-Type", ""), "Census HTTP status/type rejected; no redirect followed")
        raw = controls.read(response, DICTIONARY_CAP + 1)
        require(0 < len(raw) <= DICTIONARY_CAP, "Census dictionary size rejected")
        contract.parse_dictionary(raw)
        return raw
    except (OSError, http.client.HTTPException):
        raise RetryableRequest("Census network failure; request details withheld") from None
    finally:
        connection.close()


def live_request(plan: dict, batch: dict, client: AwsCli) -> bytes:
    """One live request for a batch: the unkeyed dictionary when the batch is marked ``kind: dictionary``, otherwise the keyed group request."""
    require(plan["credential_reference"] == "census_api_key", "Census tables request scope differs; secret not read")
    if batch.get("kind") == "dictionary":
        return request_dictionary(contract.dictionary_url(batch))
    return request_json(contract.endpoint(batch["product"], batch["year"]), contract.request_for(batch), client)


def attempt(action: Callable, plan: dict, request_batch: dict, client: AwsCli, key: str, sleep: Callable) -> bytes:
    """One request with three bounded attempts for transient failures only."""
    for number in range(3):
        controls.begin(key)
        try:
            return controls.returned(action, plan, request_batch, client)
        except RetryableRequest:
            if number == 2:
                raise
            sleep(2 ** (number + 1))
    raise AssertionError("unreachable")


def fetch(
    plan: dict, batch: dict, root: Path, allow_network: bool, client: AwsCli | None, request: Callable | None = None, sleep: Callable = time.sleep
) -> dict:
    """The stored response, dictionary and envelope of one batch; stored files are reused, so a repeat or resume requests only what is missing."""
    folder = root / "batches" / batch["id"] / "transport"
    paths = {"data": folder / "response.json", "dictionary": folder / "group.json", "envelope": folder / "request.json"}
    if paths["envelope"].exists():
        envelope = read_json(paths["envelope"])
        raw, group = paths["data"].read_bytes(), paths["dictionary"].read_bytes()
        require(envelope["request"] == contract.request_for(batch) and envelope["endpoint"] == batch["endpoint"], "Census tables cache differs")
        require(
            envelope["sha256"] == digest(raw) and envelope["bytes"] == len(raw) and envelope["dictionary_sha256"] == digest(group),
            "Census tables cached response differs",
        )
        return envelope
    if not allow_network or client is None:
        raise ValueError("Census tables response missing; --fetch required")
    action = request or live_request
    bodies: dict[str, bytes] = {}
    for kind, key in (("data", batch["id"]), ("dictionary", batch["id"] + ":dictionary")):
        if paths[kind].exists():
            bodies[kind] = paths[kind].read_bytes()
            continue
        bodies[kind] = attempt(action, plan, batch | ({"kind": "dictionary"} if kind == "dictionary" else {}), client, key, sleep)
        write_once(paths[kind], bodies[kind])
        sleep(plan["request_spacing_seconds"])
    envelope = {
        "endpoint": batch["endpoint"],
        "request": contract.request_for(batch),
        "dictionary_url": contract.dictionary_url(batch),
        "retrieved_at_utc": datetime.now(UTC).isoformat(),
        "sha256": digest(bodies["data"]),
        "bytes": len(bodies["data"]),
        "dictionary_sha256": digest(bodies["dictionary"]),
        "dictionary_bytes": len(bodies["dictionary"]),
        "http_status": 200,
    }
    write_once(paths["envelope"], encoded_json(envelope))
    return envelope
