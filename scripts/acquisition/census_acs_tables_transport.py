"""Bounded Census transport for the ACS table-vintage units: one keyed data request and one unkeyed dictionary request, stored as files.

Both requests share one bounded GET: the host check, one absolute deadline over the name lookup, the connection, the TLS handshake and the reply (a clock
that shuts the socket at the deadline, so a reply that arrives a byte at a time ends there too), no redirect, a byte cap and the transient-status rule. The
keyed request adds the runtime-only key, a 64 MiB cap and the echo check; the dictionary request has no key and an 8 MiB cap. The raw replies are written as
files, not as hex copies inside a JSON envelope.
"""

import http.client
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlsplit

from scripts.acquisition import api_keys, echo_guard, rate_limit
from scripts.acquisition import census_acs_tables_contract as contract
from scripts.acquisition import redownload_controls as controls
from scripts.acquisition.bls_api_contract import digest
from scripts.acquisition.bls_api_transport import RetryableRequest
from scripts.acquisition.s3_store import AwsCli, encoded_json, write_once
from scripts.acquisition.source_registry import read_json, require
from scripts.acquisition.transport import RequestClock, connect_within, https_connection, require_approved

DATA_CAP = 64 * 1024**2
PIECE = 1024**2
DICTIONARY_CAP = 8 * 1024**2
REQUEST_SECONDS = 120.0
DATA_PATH = re.compile(r"/data/[0-9]{4}/acs/acs5(?:/subject|/profile)?")
DICTIONARY_PATH = re.compile(r"/data/[0-9]{4}/acs/acs5(?:/subject|/profile)?/groups/[A-Z0-9]+\.json")
TRANSIENT = {500, 502, 503, 504}
RATE_LIMITED = 429


def bounded_get(url: str, target: str, cap: int, label: str) -> bytes:
    """GET TARGET (path and query) from the host of URL within REQUEST_SECONDS in all, whatever the name lookup, the connection or the reply does.

    The name lookup, the connection and the TLS handshake share one deadline; the reply is read with the time that is left. Status 200 with a JSON content
    type is the only accepted answer, a redirect is not followed and a reply over CAP is refused. The connection is closed on every path.
    """
    parsed = urlsplit(url)
    require_approved(url)
    connection = https_connection(parsed.netloc, timeout=REQUEST_SECONDS)
    try:
        with RequestClock(REQUEST_SECONDS):
            connect_within(connection, REQUEST_SECONDS)
            connection.request("GET", target, headers={"Accept": "application/json"})
            within(connection)
            response = connection.getresponse()
            controls.response_status(response)
            if response.status == RATE_LIMITED:
                raise rate_limit.from_response(response)
            if response.status in TRANSIENT:
                raise RetryableRequest("Census transient publisher failure")
            require(response.status == 200 and "json" in response.getheader("Content-Type", ""), "Census HTTP status/type rejected; no redirect followed")
            within(connection)
            raw = controls.read(TimedReply(response, connection), cap + 1)
            require(0 < len(raw) <= cap, f"Census {label} size rejected")
            within(connection)
            return raw
    except (OSError, http.client.HTTPException):
        raise RetryableRequest("Census network failure; request details withheld") from None
    finally:
        connection.close()


class TimedReply:
    """A reply whose body is read in pieces, with the request deadline checked between them, so a slow sender cannot hold the read past it."""

    def __init__(self, response: Any, connection: object) -> None:
        self.response, self.connection = response, connection

    def read(self, size: int) -> bytes:
        parts: list[bytes] = []
        while size > 0:
            within(self.connection)
            piece = self.response.read(min(size, PIECE))
            if not piece:
                break
            parts.append(piece)
            size -= len(piece)
        return b"".join(parts)


def within(connection: object) -> None:
    """Give the connection's socket the time left to the request deadline, or fail when none is left; a stand-in without a deadline is left alone."""
    deadline, sock = getattr(connection, "deadline", None), getattr(connection, "sock", None)
    if deadline is None:
        return
    left = deadline - time.monotonic()
    if left <= 0:
        raise TimeoutError("The request did not finish before its deadline.")
    if sock is not None:
        sock.settimeout(left)


def request_dictionary(url: str) -> bytes:
    """GET one public group dictionary without a key; refuse any other host, path or query."""
    parsed = urlsplit(url)
    require(
        parsed.scheme == "https" and parsed.netloc == "api.census.gov" and not parsed.query and DICTIONARY_PATH.fullmatch(parsed.path) is not None,
        "Census tables dictionary request scope differs",
    )
    raw = bounded_get(url, parsed.path, DICTIONARY_CAP, "dictionary")
    contract.parse_dictionary(raw)
    return raw


def request_keyed(endpoint: str, parameters: dict, client: AwsCli) -> bytes:
    """GET one public Census data endpoint with the runtime-only key; never persist its URL or echo."""
    parsed = urlsplit(endpoint)
    require(
        parsed.scheme == "https" and parsed.netloc == "api.census.gov" and DATA_PATH.fullmatch(parsed.path) is not None and "key" not in parameters,
        "Census request scope differs; secret not read",
    )
    require_approved(endpoint)
    credential = api_keys.checked_read(client, "census_api_key", "Census")
    require(len(credential) >= 16 and credential.isascii() and credential.isalnum(), "Census credential structure invalid")
    raw = bounded_get(endpoint, parsed.path + "?" + urlencode(parameters | {"key": credential}), DATA_CAP, "response")
    require(isinstance(echo_guard.checked_json(raw, credential, "Census"), list), "Census JSON response shape rejected")
    return raw


def live_request(plan: dict, batch: dict, client: AwsCli) -> bytes:
    """One live request for a batch: the unkeyed dictionary when the batch is marked ``kind: dictionary``, otherwise the keyed group request."""
    require(plan["credential_reference"] == "census_api_key", "Census tables request scope differs; secret not read")
    if batch.get("kind") == "dictionary":
        return request_dictionary(contract.dictionary_url(batch))
    return request_keyed(contract.endpoint(batch["product"], batch["year"]), contract.request_for(batch), client)


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
        require(
            envelope["request"] == contract.request_for(batch)
            and envelope["endpoint"] == batch["endpoint"]
            and envelope["dictionary_url"] == contract.dictionary_url(batch),
            "Census tables cache differs",
        )
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
