"""Capture mode: ingest each queue unit once and let its completion record decide skip (design 8.3, decisions D3 and D10).

The run reads the tracked queue and ``capture_specs.json``, never a stored receipt. For each unit it lists ``<root>ingestion_units/<unit_key>/`` before any
request. A verified record there means no request. Otherwise the unit is downloaded into its own working folder, stored through the project's storage path,
read back by version and closed by one create-once completion record. Before it stores anything, the unit claims a create-once intent that names the
snapshot ID its objects are stored under, so a retry after a crash stores under the same keys (``ingestion_intent.py``). The working folder is deleted
after the record, or after a failure. A monthly run (``recheck``) that downloads a payload an older record has while the newest record has another writes a
reversion record that names the older record's objects (``recheck_unit``), and writes nothing when the newest record has the payload. The first attempted
unit of a source that fails its contract pauses that source for the run. A publisher's HTTP 429 also pauses its source and stores a create-once cooldown under
``<root>ingestion_cooldowns/<source_id>/`` (``source_cooldown.py``). A later run of that root, whatever its run ID, sends no request to the source before the
resume time and reports the source paused with that time. A cooldown that cannot be read or saved never allows a request. The run writes one
run record and, only when something was attempted, one state object under ``<root>ingestion_runs/<run_id>/``. One run owns its process while it executes:
it replaces process-wide transport functions, host variables and redirect routes, so a second run started in the same process raises ``RunInProgress``.
It sits beside the earlier redownload and compare paths in ``redownload.py``, which later work removes.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shutil
import tempfile
import threading
import time
import urllib.request
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from scripts.acquisition import (
    bls_api_transport,
    census_acs_api_transport,
    census_acs_tables_transport,
    collect_mmd_api,
    hud_api_transport,
    rate_limit,
    redownload,
    redownload_controls,
    redownload_route,
    source_cooldown,
    task_role,
    transport,
)
from scripts.acquisition import completion_record as records
from scripts.acquisition import ingestion_intent as intents
from scripts.acquisition.capture import capture, receipt_validator
from scripts.acquisition.collection_layout import capture_root, load_routes
from scripts.acquisition.mapped_archives import expand_tree, stored_member_names
from scripts.acquisition.s3_store import StorageError, encoded_json, upload_snapshot
from scripts.acquisition.source_registry import RegistryError, canonical_hash, load_registry, read_json, require
from scripts.acquisition.transport import CaptureError, StopAtRateLimit, canonical_url

BYTE_CAP = 128 * 2**30
STALE_RETRY = timedelta(hours=20)
MAX_ATTEMPTS = 2
UNIT_FOLDER = re.compile(r"[a-f0-9]{64}")
MANUAL_MODES = redownload.MANUAL_MODES
COLLECTOR_MODES = frozenset(redownload.PLANNED) | {"mmd_api"}
SKIPPED_ROLES = frozenset({"capture_receipt", "transport_audit", "archive_inventory"})
API_TRANSPORTS: tuple[Any, ...] = (bls_api_transport, census_acs_api_transport, census_acs_tables_transport, hud_api_transport, collect_mmd_api)
LINEAGE_ID = {
    "bls_api_v2": "batch_id",
    "census_acs_api": "batch_id",
    "census_acs_detailed": "batch_id",
    "census_acs_tables": "batch_id",
    "hud_api": "batch_id",
    "cms_owners_org": "release_id",
    "onc_mu_hospital": "file_id",
    "hcai_util_workbook": "year_id",
}
URL_BOUND = frozenset({"file", "cms_owners_org", "onc_mu_hospital", "hcai_util_workbook"})
REVERTED = "payload_reverted"
CONFLICT_READS = 3
CONFLICT_WAIT = 1.0
COUNTERS = (
    "http_requests",
    "bytes_downloaded",
    "units_attempted",
    "objects_created",
    "intents_created",
    "bls_ledger_entries_added",
    "skipped_complete",
    "skipped_recent_stale",
    "unchanged",
    "changed",
)


class ContractFailure(ValueError):
    """A unit's download or content failed its contract. The reason is a fixed code, never response text."""


class RateLimit(ContractFailure):
    """The publisher rate-limited a unit. The reason is the fixed code ``http_429``, which pauses the source. The delay is the publisher's Retry-After in
    whole seconds (``form`` says how it was named, or ``default``), and the run stores it as the source's cooldown."""

    def __init__(self, retry_after_seconds: int, form: str = "seconds") -> None:
        super().__init__("http_429")
        self.retry_after_seconds = retry_after_seconds
        self.form = form


class SpecError(ValueError):
    """A unit's specification cannot decide its request identity."""


class IntentRefused(Exception):
    """A stored intent that cannot be trusted. The unit is refused with this fixed reason and never gets a new snapshot ID."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class RunInProgress(RuntimeError):
    """A second capture run asked to start while this process already executes one. It is refused before it changes anything."""


RUN_OWNER = threading.Lock()


class Observer:
    """The run's one request and byte observer. A request counts when it is sent and a response counts when its body is read, so a failed or rejected
    response counts too. The byte cap is enforced against the same count. The observer holds no URL, header or credential."""

    def __init__(self, byte_cap: int) -> None:
        self.byte_cap, self.requests, self.bytes = byte_cap, 0, 0

    def sent(self) -> None:
        self.requests += 1

    def received(self, body: bytes) -> bytes:
        self.bytes += len(body)
        if self.bytes > self.byte_cap:
            raise ContractFailure("byte_cap_exceeded")
        return body


def count_request(handler: Any, request: Any) -> Any:
    """The preprocessor urllib calls for each HTTPS request an opener sends, a redirect's next request included."""
    handler.observer.sent()
    return request


def request_counter(observer: Observer) -> Any:
    """A handler that reports the opener's requests. urllib finds its ``https_request`` by name, so the class is built with that name."""
    handler = type("RequestCounter", (urllib.request.BaseHandler,), {"https_request": count_request})()
    handler.observer = observer
    return handler


class CountingTransport(transport.ReviewedTransport):
    """The reviewed transport with the run's observer below the redirect policy."""

    def __init__(self, base: transport.ReviewedTransport, observer: Observer) -> None:
        super().__init__(*base.handlers, request_counter(observer))


def observed_connection(observer: Observer, original: Callable[..., Any]) -> Callable[..., Any]:
    """A connection factory whose connections report each request they send."""

    def connect(netloc: str, timeout: float) -> Any:
        connection = original(netloc, timeout)
        send = connection.request

        def request(*arguments: Any, **options: Any) -> Any:
            observer.sent()
            return send(*arguments, **options)

        connection.request = request
        return connection

    return connect


@contextmanager
def owning_process() -> Iterator[None]:
    """One run owns the process at a time. A run replaces process-wide transport functions, host variables and reviewed redirect routes, so an
    overlapping run is refused visibly. The reviewed redirect routes a run installed are removed when it ends."""
    if not RUN_OWNER.acquire(blocking=False):
        raise RunInProgress("Another capture run owns this process; a run starts only after the earlier one ends")
    routes = dict(transport.SIGNED_REDIRECT_ROUTES)
    try:
        yield
    finally:
        transport.SIGNED_REDIRECT_ROUTES.clear()
        transport.SIGNED_REDIRECT_ROUTES.update(routes)
        RUN_OWNER.release()


@contextmanager
def observing(observer: Observer) -> Iterator[None]:
    """Route every direct API connection and every response read of the process through the observer, then restore them.

    The API transports import ``https_connection`` by name and read bodies through ``redownload_controls.read``, so these are the two places every send
    and every read pass. ``owning_process`` keeps a second run from nesting its own replacements.
    """
    saved: list[tuple[Any, Any]] = []
    for module in API_TRANSPORTS:
        saved.append((module, module.https_connection))
        module.https_connection = observed_connection(observer, module.https_connection)
    read = redownload_controls.read
    redownload_controls.read = lambda response, size: observer.received(read(response, size))
    try:
        yield
    finally:
        redownload_controls.read = read
        for module, original in saved:
            module.https_connection = original


def observed_request(observer: Observer, request: Callable | None) -> Callable | None:
    """An injected API request function, counted as one send and its returned body as received bytes."""
    if request is None:
        return None

    def call(*arguments: Any) -> bytes:
        observer.sent()
        return observer.received(request(*arguments))

    return call


ABSENT_CODES = frozenset({"404", "NoSuchKey", "NoSuchVersion", "NotFound"})


class Journal:
    """The keys this run created, noted at the storage boundary when a create succeeds, so a failed upload keeps the creates before its failure.

    ``faults`` counts the storage calls that failed, for any reason except the two answers the run asks for: a create that found its key taken (the
    create-once answer) and a head that found no object. It counts at the boundary itself, so a failure that a caller turns into a refusal or a problem text is
    counted as well as one that propagates. The run reads it around each unit and pauses the unit's source when it moved, whichever position the unit has in
    its source.
    """

    def __init__(self) -> None:
        self.keys: list[str] = []
        self.faults = 0

    def fault(self, operation: str, error: StorageError) -> None:
        answer = (operation == "put-object" and error.code in redownload.CREATE_CONFLICTS) or (operation == "head-object" and error.code in ABSENT_CODES)
        if not answer:
            self.faults += 1


class JournalStorage:
    """The run's storage client. Every successful put-object it makes is noted in the journal; every other attribute is the wrapped client's."""

    def __init__(self, inner: Any, journal: Journal) -> None:
        self.inner, self.journal = inner, journal

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
        try:
            reply = self.inner.call(service, operation, arguments)
        except StorageError as error:
            self.journal.fault(operation, error)
            raise
        self.note(operation, arguments)
        return reply

    def call_bounded(self, service: str, operation: str, arguments: list[str] | None = None, *, timeout: float) -> dict:
        try:
            reply = self.inner.call_bounded(service, operation, arguments, timeout=timeout)
        except StorageError as error:
            self.journal.fault(operation, error)
            raise
        self.note(operation, arguments)
        return reply

    def note(self, operation: str, arguments: list[str] | None) -> None:
        if operation == "put-object" and arguments:
            self.journal.keys.append(arguments[arguments.index("--key") + 1])

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


class JournalS3:
    """A keyed collector's S3 client, noting each ledger entry it creates."""

    def __init__(self, inner: Any, journal: Journal) -> None:
        self.inner, self.journal = inner, journal

    def put_object(self, **arguments: Any) -> Any:
        reply = self.inner.put_object(**arguments)
        self.journal.keys.append(arguments["Key"])
        return reply

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass
class CaptureRun:
    """One capture invocation: the tracked inputs, the root, the run identity and the injectable boundaries."""

    queue_path: Path
    specs_path: Path
    root: str
    run_id: str
    storage: Any
    scratch: Path
    code_sha256: str
    bundle_sha256: str
    hosts: Path | None = None
    hosts_approval: Path | None = None
    opener: Any = None
    adapters: Mapping[str, Callable[..., Path]] = field(default_factory=dict)
    task_keys: Callable[[str], Any] | None = None
    api_request: Callable | None = None
    recheck: bool = False
    recheck_units: frozenset[str] = frozenset()
    byte_cap: int = BYTE_CAP
    stale_retry: timedelta = STALE_RETRY
    clock: Callable[[], datetime] = utc_now
    log: Callable[[dict], None] | None = None
    cache: dict[str, Any] = field(default_factory=dict)
    journal: Journal | None = None


def registry_of(run: CaptureRun, unit: dict) -> dict:
    """The registry view the storage check reads for a unit. The reviewed registry loads once per run."""
    if "registry" not in run.cache:
        run.cache["registry"] = load_registry()
    return redownload.queue_approved_registry(unit, run.cache["registry"])


def validator_of(run: CaptureRun) -> Any:
    if "validator" not in run.cache:
        run.cache["validator"] = receipt_validator()
    return run.cache["validator"]


def client_for(run: CaptureRun, handler: str) -> Any:
    """What a keyed collector reads its key through: a reader of that collector's key only, storing through the run's one storage client."""
    if run.task_keys is None:
        raise ContractFailure("keys_not_enabled")
    reader = run.task_keys(handler)
    reader.storage = run.storage
    factory = getattr(reader, "factory", None)
    if factory is not None and run.journal is not None:
        journal = run.journal
        reader.factory = lambda service, region: JournalS3(factory(service, region), journal) if service == "s3" else factory(service, region)
    return reader


def planned_item(run: CaptureRun, handler: str, spec: Mapping[str, Any]) -> tuple[dict, dict]:
    """The locked plan and the one planned item the specification names. No stored receipt is read."""
    identity = spec.get("plan_item")
    if not (isinstance(identity, str) and identity):
        raise SpecError("A collector unit needs a plan_item in its specification")
    if f"plans:{handler}" not in run.cache:
        run.cache[f"plans:{handler}"] = redownload.load_plans_of(handler)
    group = redownload.PLANNED[handler][2]
    matches = [
        (plan, plan if group is None else item)
        for plan in run.cache[f"plans:{handler}"]
        for item in ([plan] if group is None else plan[group])
        if item["id"] == identity
    ]
    if len(matches) != 1:
        raise SpecError("The plan_item matches no planned item or several")
    return matches[0]


def rate_limit_of(run: CaptureRun, limited: rate_limit.RateLimited) -> RateLimit:
    """The pausing failure of a typed 429: the publisher's Retry-After header normalized to whole seconds against the run's clock, or the stated default."""
    seconds, form = source_cooldown.retry_after_seconds(limited.retry_after, run.clock())
    return RateLimit(seconds, form)


def adapt_bls(unit: dict, spec: dict, folder: Path, run: CaptureRun, opener: Any) -> Path:
    from scripts.acquisition import bls_api_transport as api
    from scripts.acquisition import collect_bls_api as collector

    plan, batch = planned_item(run, "bls_api_v2", spec)
    try:
        result = collector.execute(plan, batch, folder, True, False, client_for(run, "bls_api_v2"), {}, run.api_request or api.request_live)
    except api.QuotaPause:
        raise ContractFailure("bls_quota") from None
    return Path(result["receipt_path"])


def adapt_census_api(unit: dict, spec: dict, folder: Path, run: CaptureRun, opener: Any) -> Path:
    from scripts.acquisition import census_acs_api_transport as api
    from scripts.acquisition import collect_census_acs_api as collector

    plan, batch = planned_item(run, "census_acs_api", spec)
    try:
        result = collector.execute(plan, batch, folder, True, False, client_for(run, "census_acs_api"), {}, run.api_request or api.request_live)
    except rate_limit.RateLimited as limited:
        raise rate_limit_of(run, limited) from None
    return Path(result["receipt_path"])


def adapt_census_detailed(unit: dict, spec: dict, folder: Path, run: CaptureRun, opener: Any) -> Path:
    from scripts.acquisition import collect_census_acs_detailed as collector

    plan, batch = planned_item(run, "census_acs_detailed", spec)
    request = run.api_request or collector.live_request
    try:
        result = collector.execute(plan, batch, folder, True, False, client_for(run, "census_acs_detailed"), {}, request)
    except rate_limit.RateLimited as limited:
        raise rate_limit_of(run, limited) from None
    return Path(result["receipt_path"])


def adapt_census_tables(unit: dict, spec: dict, folder: Path, run: CaptureRun, opener: Any) -> Path:
    from scripts.acquisition import census_acs_tables_transport as api
    from scripts.acquisition import collect_census_acs_tables as collector

    plan, batch = planned_item(run, "census_acs_tables", spec)
    try:
        result = collector.execute(plan, batch, folder, True, False, client_for(run, "census_acs_tables"), {}, run.api_request or api.live_request)
    except rate_limit.RateLimited as limited:
        raise rate_limit_of(run, limited) from None
    return Path(result["receipt_path"])


def adapt_hud_api(unit: dict, spec: dict, folder: Path, run: CaptureRun, opener: Any) -> Path:
    from scripts.acquisition import collect_hud_api as collector
    from scripts.acquisition import hud_api_transport as api

    plan, batch = planned_item(run, "hud_api", spec)
    try:
        result = collector.execute(plan, batch, folder, True, False, client_for(run, "hud_api"), {}, run.api_request or api.request_live)
    except rate_limit.RateLimited as limited:
        raise rate_limit_of(run, limited) from None
    return Path(result["receipt_path"])


def adapt_mmd(unit: dict, spec: dict, folder: Path, run: CaptureRun, opener: Any) -> Path:
    from scripts.acquisition import collect_mmd_api as collector

    mmd = spec["mmd"]
    try:
        result = collector.execute(mmd["measure_id"], mmd["year"], True, False, root=folder, client=run.storage)
    except rate_limit.RateLimited as limited:
        raise rate_limit_of(run, limited) from None
    return Path(result["receipt_path"])


def adapt_cms_owners(unit: dict, spec: dict, folder: Path, run: CaptureRun, opener: Any) -> Path:
    from scripts.acquisition import store_cms_owners as collector

    plan, release = planned_item(run, "cms_owners_org", spec)
    return Path(collector.execute(plan, release, folder, True, False, None, {}, opener, StopAtRateLimit(attempts=MAX_ATTEMPTS))["receipt_path"])


def adapt_hcai_util(unit: dict, spec: dict, folder: Path, run: CaptureRun, opener: Any) -> Path:
    from scripts.acquisition import store_hcai_util as collector

    plan, entry = planned_item(run, "hcai_util_workbook", spec)
    return Path(collector.execute(plan, entry, folder, True, False, None, {}, opener, StopAtRateLimit(attempts=MAX_ATTEMPTS))["receipt_path"])


def adapt_onc_mu(unit: dict, spec: dict, folder: Path, run: CaptureRun, opener: Any) -> Path:
    from scripts.acquisition import store_onc_mu as collector

    plan, _ = planned_item(run, "onc_mu_hospital", spec)
    # The plan raises the time and size bounds for this one large file; only the attempt count is lowered.
    limits = StopAtRateLimit(attempts=MAX_ATTEMPTS, timeout_seconds=120, max_seconds=plan["limits"]["max_seconds"], max_bytes=plan["limits"]["max_bytes"])
    return Path(collector.execute(plan, folder, True, False, None, {}, opener, limits)["receipt_path"])


ADAPTERS: dict[str, Callable[..., Path]] = {
    "bls_api_v2": adapt_bls,
    "census_acs_api": adapt_census_api,
    "census_acs_detailed": adapt_census_detailed,
    "census_acs_tables": adapt_census_tables,
    "hud_api": adapt_hud_api,
    "mmd_api": adapt_mmd,
    "cms_owners_org": adapt_cms_owners,
    "hcai_util_workbook": adapt_hcai_util,
    "onc_mu_hospital": adapt_onc_mu,
}
PAUSING = frozenset({"bls_quota", "http_429", "storage_contract"})
INTENT_REASONS = frozenset({"intent_forged", "intent_unreadable"})


def base_text(digest_hex: str) -> str:
    return base64.b64encode(bytes.fromhex(digest_hex)).decode()


def load_inputs(run: CaptureRun) -> tuple[list[dict], dict[str, dict], str]:
    """The tracked queue checked against its lock, and the specification checked against that queue."""
    body = run.queue_path.read_bytes()
    lock = read_json(run.queue_path.with_name(run.queue_path.stem + ".lock.json"))
    queue_sha256 = hashlib.sha256(body).hexdigest()
    require(queue_sha256 == lock["queue_sha256"], "Queue differs from its lock")
    units = json.loads(body)["queue"]
    require(len(units) == lock["entries"] and len({unit["snapshot_id"] for unit in units}) == len(units), "Queue entries differ from its lock")
    document = read_json(run.specs_path)
    require(document.get("kind") == "capture_specs" and document.get("queue_sha256") == queue_sha256, "Capture specification belongs to another queue")
    return units, document["specs"], queue_sha256


def member_map_id(spec: Mapping[str, Any]) -> str | None:
    members = spec.get("archive_members")
    return records.sha256_text(records.canonical(sorted(members, key=lambda item: item["member_chain"]))) if members else None


def unit_identity(unit: dict, spec: Mapping[str, Any]) -> dict[str, Any]:
    """The canonical request identity of a unit: source, handler mode, request and reviewed selection. Never a snapshot ID, a time or a run."""
    handler = redownload.handler_mode(unit)
    if spec.get("mode") != unit["mode"]:
        raise SpecError("The unit's specification names another mode")
    selection: dict[str, Any] = {"pages": unit["pages"], "members": member_map_id(spec)}
    if handler == "mmd_api":
        mmd = spec.get("mmd")
        if not (isinstance(mmd, dict) and isinstance(mmd.get("measure_id"), str) and isinstance(mmd.get("year"), int)):
            raise SpecError("An MMD unit needs its measure and year in its specification")
        selection["mmd"] = {"measure_id": mmd["measure_id"], "year": mmd["year"]}
    elif handler in COLLECTOR_MODES:
        if not (isinstance(spec.get("plan_item"), str) and spec["plan_item"]):
            raise SpecError("A collector unit needs a plan_item in its specification")
        selection["plan_item"] = spec["plan_item"]
    request = {"method": "POST" if handler == "bls_api_v2" else "GET", "url": canonical_url(unit["url"])}
    return {"source_id": unit["source_id"], "mode": handler, "request": request, "selection": selection}


def unit_key_of(unit: dict, spec: Mapping[str, Any]) -> str:
    return records.unit_key(unit_identity(unit, spec))


def expectation(run: CaptureRun, unit: dict, spec: Mapping[str, Any]) -> dict[str, Any]:
    """What a stored record and its receipt must say for this unit, taken from the trusted queue, specification and routes and never from the record."""
    if "routes" not in run.cache:
        run.cache["registry"] = run.cache.get("registry") or load_registry()
        run.cache["routes"] = load_routes(run.cache["registry"])
    route = run.cache["routes"].get(unit["source_id"])
    if route is None:
        raise SpecError("The unit's source has no collection route")
    handler = redownload.handler_mode(unit)
    mmd = spec.get("mmd") if handler == "mmd_api" else None
    return {
        "source_id": unit["source_id"],
        "mode": handler,
        "collection": f"{route['publisher']}/{route['collection']}",
        "url": canonical_url(unit["url"]),
        "method": "POST" if handler == "bls_api_v2" else "GET",
        "plan_item": spec.get("plan_item"),
        "mmd": {"measure_id": mmd["measure_id"], "year": mmd["year"]} if mmd else None,
        "members": spec.get("archive_members") or None,
    }


def named(storage: Any) -> dict[str, str]:
    return {"Bucket": storage.configuration["data_bucket_name"], "ExpectedBucketOwner": storage.configuration["expected_account_id"]}


def listing_page(call: Callable[..., dict], operation: str, storage: Any, **arguments: Any) -> dict:
    """One page of a listing through the store's own client. An SDK failure, a refusal or a lost connection, becomes a ``StorageError`` that holds only the
    provider's short code, as every call through the task storage does, so a run can isolate it to the source that asked."""
    try:
        return call(**named(storage), **arguments)
    except (ClientError, BotoCoreError) as error:
        failure = StorageError(operation, task_role.error_code(error))
        if (journal := getattr(storage, "journal", None)) is not None:
            journal.fault(operation, failure)
        raise failure from None


def list_objects(storage: Any, prefix: str) -> list[dict]:
    """Every current object under a prefix with its last modified time, from the store's own listing."""
    found: list[dict] = []
    options: dict[str, str] = {}
    while True:
        page = listing_page(storage.client.list_objects_v2, "list-objects-v2", storage, Prefix=prefix, **options)
        found.extend(page.get("Contents", []))
        if not page.get("IsTruncated"):
            return found
        options = {"ContinuationToken": page["NextContinuationToken"]}


def list_versions(storage: Any, prefixes: Iterable[str]) -> list[dict]:
    """Every object version and delete marker under the prefixes, sorted, as plain facts a later listing can be compared with."""
    found: dict[tuple[str, str, str], dict] = {}
    for prefix in prefixes:
        options: dict[str, str] = {}
        while True:
            page = listing_page(storage.client.list_object_versions, "list-object-versions", storage, Prefix=prefix, **options)
            for item in page.get("Versions", []):
                found[(item["Key"], item["VersionId"], "version")] = {
                    "key": item["Key"],
                    "version_id": item["VersionId"],
                    "etag": item.get("ETag"),
                    "kind": "version",
                }
            for item in page.get("DeleteMarkers", []):
                found[(item["Key"], item["VersionId"], "delete_marker")] = {
                    "key": item["Key"],
                    "version_id": item["VersionId"],
                    "etag": None,
                    "kind": "delete_marker",
                }
            if not page.get("IsTruncated"):
                break
            options = {"KeyMarker": page["NextKeyMarker"], "VersionIdMarker": page["NextVersionIdMarker"]}
    return [found[name] for name in sorted(found)]


def diff_versions(before: list[dict], after: list[dict], allowed_new: Mapping[str, int]) -> list[str]:
    """What differs between two version listings, as problems: a removed or changed version, any new delete marker, and more new versions of a key than
    ``allowed_new`` names for it."""
    problems = []
    earlier = {(item["key"], item["version_id"], item["kind"]): item for item in before}
    later = {(item["key"], item["version_id"], item["kind"]): item for item in after}
    for name, item in earlier.items():
        if name not in later or later[name]["etag"] != item["etag"]:
            problems.append(f"version of {item['key']} was removed or replaced")
    added: Counter = Counter()
    for name, item in later.items():
        if name in earlier:
            continue
        if item["kind"] == "delete_marker":
            problems.append(f"new delete marker on {item['key']}")
        else:
            added[item["key"]] += 1
    for key, count in sorted(added.items()):
        if key not in allowed_new:
            problems.append(f"unexpected new version of {key}")
        elif count > allowed_new[key]:
            problems.append(f"{count} new versions of {key}; the run may add {allowed_new[key]}")
    return problems


def verify_object(storage: Any, item: Mapping[str, Any]) -> str | None:
    """Check one object a record names by its exact version: length and SHA-256 checksum. None means it matches."""
    try:
        reply = storage.call("s3api", "head-object", ["--key", item["key"], "--version-id", item["version_id"], "--checksum-mode", "ENABLED"])
    except StorageError as error:
        return f"{item['key']} cannot be read at its version ({error.code})"
    if reply.get("VersionId") != item["version_id"] or reply.get("ContentLength") != item["byte_count"]:
        return f"{item['key']} differs from its recorded version or length"
    if reply.get("ChecksumSHA256") != base_text(item["sha256"]):
        return f"{item['key']} differs from its recorded digest"
    return None


def read_version(storage: Any, key: str, version_id: str) -> bytes:
    """The bytes of one stored object at an exact version."""
    with tempfile.TemporaryDirectory(prefix="record_read_") as folder:
        local = Path(folder) / "record.json"
        storage.call("s3api", "get-object", ["--key", key, "--version-id", version_id, str(local)])
        return local.read_bytes()


def read_stored(storage: Any, key: str) -> bytes:
    """The bytes of one stored object at its current version."""
    return read_version(storage, key, str(storage.call("s3api", "head-object", ["--key", key])["VersionId"]))


def read_record(storage: Any, key: str, root: str) -> dict[str, Any]:
    """Read one stored record at its current version and refuse it unless it is exactly the schema and names its own key."""
    return records.parse(read_stored(storage, key), key=key, root=root)


def read_intent(storage: Any, key: str, root: str, source_id: str | None = None) -> dict[str, Any]:
    """Read one stored intent at its current version. A malformed or copied intent and an unreadable one are refused with their own fixed reasons."""
    try:
        return intents.parse(read_stored(storage, key), key=key, root=root, source_id=source_id)
    except intents.IntentError:
        raise IntentRefused("intent_forged") from None
    except StorageError:
        raise IntentRefused("intent_unreadable") from None


def create_new(storage: Any, key: str, body: bytes) -> bool:
    """Create one object if no object has its key. False means another writer's object is already there; it is never replaced."""
    digest = hashlib.sha256(body).hexdigest()
    with tempfile.TemporaryDirectory(prefix="run_object_") as folder:
        local = Path(folder) / "object.json"
        local.write_bytes(body)
        arguments = ["--key", key, "--body", str(local), "--if-none-match", "*", "--checksum-algorithm", "SHA256", "--checksum-sha256", base_text(digest)]
        try:
            storage.call("s3api", "put-object", [*arguments, "--server-side-encryption", "AES256", "--metadata", json.dumps({"sha256": digest})])
        except StorageError as conflict:
            if conflict.code not in redownload.CREATE_CONFLICTS:
                raise
            return False
    return True


def claim_intent(run: CaptureRun, unit: dict, key: str, generation: int, snapshot_id: str) -> str:
    """The snapshot ID this attempt stores under: its own when it wrote the intent, else the one an earlier attempt's intent already names."""
    target = intents.intent_key(run.root, key, generation)
    body = intents.encode(intents.build(unit_key=key, generation=generation, snapshot_id=snapshot_id, run_id=run.run_id))
    if create_new(run.storage, target, body):
        return snapshot_id
    return read_intent(run.storage, target, run.root, unit["source_id"])["snapshot_id"]


def read_json_object(storage: Any, item: Mapping[str, Any]) -> Any:
    """The JSON an object a record names holds, read at the exact version the record names."""
    return json.loads(read_version(storage, item["key"], item["version_id"]))


def stored_name(entry: Mapping[str, Any]) -> str:
    """The name a stored object carries: its stored file name, else the last part of its storage path."""
    return str(entry.get("stored_file_name") or Path(entry["storage_path"]).name)


def listed_objects(entries: Iterable[Mapping[str, Any]]) -> tuple[set[tuple], set[tuple], set[tuple]]:
    """The receipt, data and other objects manifest entries list, as comparable facts: key, exact version, digest and length, then the stored file name of
    a data object or the role of another one. Only the transport and inventory roles are left out."""
    receipts: set[tuple] = set()
    data: set[tuple] = set()
    other: set[tuple] = set()
    for entry in entries:
        role, item = entry["role"], entry["object"]
        fact = (item["key"], str(item["version_id"]), item["sha256"], item["byte_count"])
        if role == "capture_receipt":
            receipts.add(fact)
        elif role == "data":
            data.add((*fact, stored_name(entry)))
        elif role not in SKIPPED_ROLES:
            other.add((*fact, role))
    return receipts, data, other


def recorded_objects(record: Mapping[str, Any]) -> tuple[set[tuple], set[tuple], set[tuple]]:
    """The receipt, data and other objects a record names, as the same facts ``listed_objects`` gives."""

    def fact(item: Mapping[str, Any]) -> tuple:
        return (item["key"], item["version_id"], item["sha256"], item["byte_count"])

    return (
        {fact(record["receipt"])},
        {(*fact(item), item["stored_file_name"]) for item in record["data_objects"]},
        {(*fact(item), item["role"]) for item in record["other_objects"]},
    )


def manifest_entries(storage: Any, record: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Every object entry the record's manifests list, each manifest read at the exact version the record names."""
    return [entry for manifest in record["manifests"] for entry in read_json_object(storage, manifest)["objects"]]


def object_set_problems(record: Mapping[str, Any], entries: Iterable[Mapping[str, Any]]) -> list[str]:
    """Problems with the object set, in both directions: what the record names must be what its manifests list, with the same roles and stored names."""
    problems: list[str] = []
    for found, said, label in zip(listed_objects(entries), recorded_objects(record), ("receipt", "data object", "other object"), strict=True):
        problems.extend(f"{key} is not listed by the record's manifests as it is named" for key, *_ in sorted(said - found))
        problems.extend(f"{key} is listed by the record's manifests as a {label} that the record omits or names otherwise" for key, *_ in sorted(found - said))
    return problems


def member_problems(storage: Any, entries: list[dict[str, Any]], reviewed: list[dict[str, Any]] | None) -> list[str]:
    """Problems with the archive member map the manifests bind: the stored map and the stored members must be the specification's reviewed member map."""
    # Each dataset folder's manifest lists the archive's one member map, so repeated listings of the same stored object count once.
    distinct = {json.dumps(entry["object"], sort_keys=True) for entry in entries if entry["role"] == "archive_inventory"}
    inventories = [json.loads(text) for text in sorted(distinct)]
    if not reviewed:
        return ["the manifests list an archive member map that the specification does not name"] if inventories else []
    if len(inventories) != 1:
        return ["the manifests do not list exactly one archive member map"]
    body = read_version(storage, inventories[0]["key"], str(inventories[0]["version_id"]))
    if hashlib.sha256(body).hexdigest() != inventories[0]["sha256"]:
        return ["the archive member map differs from its listed digest"]
    wanted = {(tuple(item["member_chain"]), item["role"], item["dataset_id"]) for item in reviewed}
    stored = {(tuple(item["member_chain"]), item["role"], item["dataset_id"]) for item in json.loads(body)["members"]}
    placed = {(tuple(entry["member_chain"]), entry["role"], entry["mapped_dataset_id"]) for entry in entries if "member_chain" in entry}
    problems = []
    if stored != wanted:
        problems.append("the stored archive member map differs from the specification's reviewed member map")
    if placed != {(chain, role, dataset if role == "data" else None) for chain, role, dataset in wanted}:
        problems.append("the stored members' roles or dataset folders differ from the specification's reviewed member map")
    return problems


def receipt_problems(receipt: Mapping[str, Any], expected: Mapping[str, Any]) -> list[str]:
    """Problems with the receipt a record names against the request the trusted specification describes."""
    try:
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        acquisition, found_source = receipt["acquisition"], receipt["source"]["source_record_id"]
    except (KeyError, TypeError, ValueError):
        return ["the receipt has no readable source, acquisition or lineage"]
    problems = []
    if found_source != expected["source_id"]:
        problems.append("the receipt belongs to another source")
    if lineage.get("mode") != expected["mode"]:
        problems.append("the receipt belongs to another handler")
    if expected["mode"] in URL_BOUND and (
        canonical_url(acquisition.get("requested_url") or "") != expected["url"] or acquisition.get("request_method") != expected["method"]
    ):
        problems.append("the receipt answers another request")
    name = LINEAGE_ID.get(expected["mode"])
    if name is not None and lineage.get(name) != expected["plan_item"]:
        problems.append("the receipt names another planned item")
    if expected["mmd"] is not None and (lineage.get("measure_id"), lineage.get("year")) != (expected["mmd"]["measure_id"], expected["mmd"]["year"]):
        problems.append("the receipt names another measure or year")
    return problems


def binding_problems(storage: Any, record: Mapping[str, Any], expected: Mapping[str, Any] | None) -> list[str]:
    """Problems with the binding of a record: its objects are exactly what its manifests list and, when the requested unit is known, its receipt, handler
    and member map are that unit's."""
    try:
        entries = manifest_entries(storage, record)
        problems = object_set_problems(record, entries)
    except (StorageError, ValueError, KeyError, TypeError):
        return ["the record's manifests cannot be read as a list of objects"]
    if expected is None:
        return problems
    problems.extend(f"the record names another {name}" for name in ("source_id", "mode", "collection") if record[name] != expected[name])
    try:
        problems.extend(receipt_problems(read_json_object(storage, record["receipt"]), expected))
    except (StorageError, ValueError):
        problems.append("the receipt cannot be read")
    try:
        problems.extend(member_problems(storage, entries, expected["members"]))
    except (StorageError, ValueError, KeyError, TypeError):
        problems.append("the archive member map cannot be read")
    return problems


def verify_record(storage: Any, key: str, root: str, expected: Mapping[str, Any] | None = None) -> list[str]:
    """Problems with one stored record: unreadable, malformed, copied under another key, naming an object that does not match, or not the requested unit's."""
    try:
        record = read_record(storage, key, root)
    except (records.RecordError, StorageError) as error:
        return [f"{key}: {error}"]
    found = [problem for item in records.objects_named(record) if (problem := verify_object(storage, item))]
    return found or [f"{key}: {problem}" for problem in binding_problems(storage, record, expected)]


def verify_records(storage: Any, root: str) -> list[str]:
    """The host evidence step for records: every record under the root's unit folders verifies by version."""
    problems: list[str] = []
    prefix = f"{root}{records.UNITS_FOLDER}"
    for item in list_objects(storage, prefix):
        problems.extend(verify_record(storage, item["Key"], root))
    return problems


def evidence_prefixes(root: str) -> list[str]:
    """What the host evidence step lists before and after a run: the capture root and the BLS ledger, which every root shares."""
    return sorted({root, task_role.BLS_LEDGER_PREFIX})


def intent_problems(storage: Any, root: str, run_record: Mapping[str, Any], new_intents: list[str]) -> list[str]:
    """Problems with the intents of a run: a new intent that does not read as its key says, and a unit it stored whose intent is missing."""
    problems = []
    for key in new_intents:
        try:
            read_intent(storage, key, root)
        except IntentRefused as refusal:
            problems.append(f"{key}: {refusal.reason}")
    for item in [*run_record["units"]["completed"], *run_record["units"]["changed"]]:
        found = list_objects(storage, records.unit_prefix(root, item["unit_key"]))
        if item["reason"] == REVERTED:
            parts = records.key_payload(newest(found)["Key"], root, item["unit_key"]) if found else None
            if parts is None or parts[1] != run_record["run_id"]:
                problems.append(f"unit {item['unit_key']} claims a reversion but its newest record is not this run's reversion record")
            continue
        generation = len(found)
        if not list_objects(storage, intents.intent_key(root, item["unit_key"], generation)):
            problems.append(f"unit {item['unit_key']} has no intent for generation {generation}")
    return problems


def tally_writes(root: str, keys: Iterable[str]) -> dict[str, int]:
    """What a run's recorded writes were: intents, shared ledger entries, run objects and, for every other key, stored objects and completion records."""
    counts = {"intents_created": 0, "bls_ledger_entries_added": 0, "run_objects": 0, "objects_created": 0}
    for key in keys:
        if key.startswith(f"{root}{intents.INTENTS_FOLDER}"):
            counts["intents_created"] += 1
        elif key.startswith(task_role.BLS_LEDGER_PREFIX):
            counts["bls_ledger_entries_added"] += 1
        elif key.startswith((f"{root}{records.RUNS_FOLDER}", f"{root}{source_cooldown.FOLDER}")):
            counts["run_objects"] += 1
        else:
            counts["objects_created"] += 1
    return counts


def outcome_problems(units: Mapping[str, Any]) -> list[str]:
    """A bootstrap run is complete only when every unit it was given ended with a completion record or a recent stale record."""
    problems = [f"unit {item['unit_key'] or item['snapshot_id']} did not complete: {item['reason']}" for item in [*units["failed"], *units["refused"]]]
    problems.extend(f"unit {snapshot_id} was not attempted ({name})" for name in ("paused_not_attempted", "keyed_not_attempted") for snapshot_id in units[name])
    return problems


def skip_problems(storage: Any, root: str, run_record: Mapping[str, Any]) -> list[str]:
    """Problems with the units the run skipped: each has its verified completion record or a stale record younger than the retry interval."""
    units, counters, problems = run_record["units"], run_record["counters"], []
    if counters["skipped_complete"] != len(units["skipped_complete"]) or counters["skipped_recent_stale"] != len(units["skipped_recent_stale"]):
        problems.append("the skip counters differ from the units the run record lists")
    problems.extend(
        f"unit {key} was skipped as complete but has no completion record"
        for key in units["skipped_complete"]
        if not list_objects(storage, records.unit_prefix(root, key))
    )
    started, retry = datetime.fromisoformat(run_record["started_at_utc"]), timedelta(seconds=run_record["stale_retry_seconds"])
    for key in units["skipped_recent_stale"]:
        found = list_objects(storage, records.stale_prefix(root, key))
        if not found or started - newest(found)["LastModified"] >= retry:
            problems.append(f"unit {key} was skipped as recently stale but has no stale record from the retry interval")
    return problems


def check_run(storage: Any, run_record: Mapping[str, Any], before: list[dict]) -> list[str]:
    """The host evidence step: reconcile every unit, verify the records by version and diff the version listings before and after the run.

    The only new versions allowed are the writes the run recorded at its storage boundary, one new version for each, and exactly one new version of its
    run record. A recorded write must be new in the listing, nothing may be removed, replaced or marked deleted, and a run that attempted nothing allows
    no new shared ledger entry. Every unit ends with a verified record, or the run fails.
    """
    root, run_id, counters, units = run_record["root"], run_record["run_id"], run_record["counters"], run_record["units"]
    after = list_versions(storage, evidence_prefixes(root))
    known = {(item["key"], item["version_id"], item["kind"]) for item in before}
    new_keys = [item["key"] for item in after if (item["key"], item["version_id"], item["kind"]) not in known and item["kind"] == "version"]
    run_key, writes, listed_new = records.run_record_key(root, run_id), Counter(run_record["writes"]), Counter(new_keys)
    proof = counters["units_attempted"] == 0
    foreign = Counter({key: 1 for key in listed_new if key.startswith(task_role.BLS_LEDGER_PREFIX) and key not in writes})
    problems = diff_versions(before, after, writes + Counter({run_key: 1}) + (Counter() if proof else foreign))
    if proof and foreign:
        problems.append("the shared BLS ledger changed during a run that attempted nothing")
    problems.extend(f"recorded write {key} is not a new version in the listing" for key in sorted(writes - listed_new))
    if run_key not in listed_new:
        problems.append("the run record is not new")
    tally = tally_writes(root, run_record["writes"])
    said = {name: counters[name] for name in ("objects_created", "intents_created", "bls_ledger_entries_added")}
    if said != {name: tally[name] for name in said}:
        problems.append(f"the recorded writes hold { ({name: tally[name] for name in said}) }; the counters say {said}")
    new_intents = sorted(key for key in new_keys if key.startswith(f"{root}{intents.INTENTS_FOLDER}"))
    if len(new_intents) != counters["intents_created"]:
        problems.append(f"the listing holds {len(new_intents)} new intents; the counters say {counters['intents_created']}")
    problems.extend(outcome_problems(units))
    problems.extend(skip_problems(storage, root, run_record))
    problems.extend(intent_problems(storage, root, run_record, new_intents))
    problems.extend(verify_records(storage, root))
    if not run_record["recheck"]:
        for item in units["completed"]:
            found = list_objects(storage, records.unit_prefix(root, item["unit_key"]))
            if len(found) != 1:
                problems.append(f"unit {item['unit_key']} has {len(found)} completion records")
    return problems


def newest(found: list[dict]) -> dict:
    return max(found, key=lambda item: (item["LastModified"], item["Key"]))


def clear_scratch(scratch: Path) -> None:
    """Remove the working folders an earlier process left. Only folders named like a unit key are touched."""
    scratch.mkdir(parents=True, exist_ok=True)
    for child in scratch.iterdir():
        if child.is_dir() and not child.is_symlink() and UNIT_FOLDER.fullmatch(child.name):
            shutil.rmtree(child)


def local_payload(receipt: dict, mapping: dict | None) -> list[tuple[str, str]]:
    """The (stored name, SHA-256) pairs of the data-role files, known before anything is stored."""
    if mapping is not None:
        names = stored_member_names(mapping["members"])
        return [(names[tuple(item["member_chain"])], item["sha256"]) for item in mapping["members"] if item["role"] == "data"]
    return [(item["stored_file_name"], item["sha256"]) for item in receipt["artifacts"] if item["role"] == "data"]


def archive_mapping(unit: Mapping[str, Any], spec: Mapping[str, Any], receipt_path: Path, registry: dict) -> dict:
    """The reviewed member map of a fresh archive: roles and datasets from the specification, digests from the fresh members."""
    receipt = read_json(receipt_path)
    artifact = receipt["artifacts"][0]
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    reviewed = {tuple(item["member_chain"]): item for item in spec["archive_members"]}
    route = load_routes(registry)[unit["source_id"]]
    with tempfile.TemporaryDirectory(dir=receipt_path.parent) as scratch:
        leaves = expand_tree(receipt_path.parent / artifact["storage_path"], Path(scratch) / "leaves", artifact["sha256"])
        if set(reviewed) != set(leaves):
            raise ContractFailure("archive_members_differ")
        members = [
            {
                "member_chain": list(chain),
                "sha256": leaves[chain]["sha256"],
                "bytes": leaves[chain]["byte_count"],
                "role": item["role"],
                "dataset_id": item["dataset_id"],
            }
            for chain, item in reviewed.items()
        ]
    return {
        "registry_sha256": canonical_hash(registry),
        "source_ids": [unit["source_id"]],
        "route_ids": [lineage["route_id"]],
        "url": unit["url"],
        "release_date": receipt["release"]["release_date"],
        "collection_root": f"{route['publisher']}/{route['collection']}",
        "sha256": artifact["sha256"],
        "bytes": artifact["byte_count"],
        "members": members,
    }


def download_name(unit: Mapping[str, Any], spec: Mapping[str, Any]) -> str:
    stored = spec.get("stored_data_files") or []
    if stored:
        return str(stored[0])
    tail = re.sub(r"[^A-Za-z0-9._-]", "_", unit["url"].split("?")[0].rsplit("/", 1)[-1])
    return tail if tail and tail[0].isalnum() else "download_" + tail


def release_bindings(unit: Mapping[str, Any], spec: Mapping[str, Any], registry: Mapping[str, Any]) -> dict[str, str]:
    """The receipt lineage bindings a held source's specification names, through the tracked release record. An unusable binding is a specification error."""
    from scripts.acquisition import history_routes

    source = next(item for item in registry["sources"] if item["source_id"] == unit["source_id"])
    try:
        return history_routes.lineage_bindings(source, {"release_binding": spec["release_binding"]})
    except CaptureError:
        raise SpecError("The release binding is unknown or the source is not held") from None


def capture_file(unit: dict, spec: dict, folder: Path, run: CaptureRun, opener: Any) -> Path:
    """Download one file unit into the unit's working folder and return its local receipt. The plan comes from the specification, not a stored receipt."""
    registry = registry_of(run, unit)
    template = spec.get("receipt_template")
    if not isinstance(template, dict):
        raise SpecError("A file unit needs a receipt_template in its specification")
    fmt = spec.get("expected_format")
    if not isinstance(fmt, str):
        raise SpecError("A file unit needs an expected_format in its specification")
    template = {name: value for name, value in template.items() if name != "expected_sha256"}
    plan = template | {"route_id": redownload.route_id_of(unit, registry), "file_name": download_name(unit, spec), "expected_format": fmt}
    if "release_binding" in spec:
        plan |= {"lineage_bindings": release_bindings(unit, spec, registry)}
    if spec.get("reviewed_unsigned_redirect"):
        from scripts.acquisition.history_redirects import install_redirect

        install_redirect(unit["url"], spec["reviewed_unsigned_redirect"])
    limits = StopAtRateLimit(attempts=MAX_ATTEMPTS, max_bytes=min(4 * 2**30, max(2 * unit["bytes"], 2**20)))
    return capture(plan, folder, registry, validator_of(run), limits, opener)


def stored_objects(reconciliation: dict) -> tuple[dict, list[dict], list[dict], list[dict]]:
    """The receipt, manifest, data and other objects a storage call recorded, each with its exact version."""
    items = reconciliation["objects"]
    receipt = next(item for item in items if item["role"] == "capture_receipt")
    data: list[dict] = []
    other: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for item in items:
        record = item["object"]
        if item["role"] in SKIPPED_ROLES or (item["role"], record["key"]) in seen:
            continue
        seen.add((item["role"], record["key"]))
        (data if item["role"] == "data" else other).append(record | {"role": item["role"], "stored_file_name": stored_name(item)})
    return receipt["object"], [item["object"] for item in reconciliation["manifests"]], data, other


@dataclass
class Result:
    """What one unit ended with: its key, its source and how it counted."""

    key: str
    unit: dict
    status: str
    reason: str | None = None


def rechecks(run: CaptureRun, unit: Mapping[str, Any]) -> bool:
    """Whether the run downloads the unit again and compares its payload: every unit of a monthly run and the named snapshots of a retry."""
    return run.recheck or unit["snapshot_id"] in run.recheck_units


def close_unit(run: CaptureRun, unit: dict, key: str, target: str, body: bytes, expected: Mapping[str, Any], reason: str | None = None) -> Result:
    """Create the unit's completion record once. When another run created it first, the unit is complete if that record verifies as this unit's.

    A conflict whose winner cannot be read yet is read again a bounded number of times. The loser claims no creation: only its own successful puts count.
    """
    ended = "changed" if rechecks(run, unit) else "completed"
    if create_new(run.storage, target, body):
        return Result(key, unit, ended, reason)
    mark = None if run.journal is None else run.journal.faults
    for read in range(CONFLICT_READS):
        try:
            read_record(run.storage, target, run.root)
        except StorageError:
            if read + 1 < CONFLICT_READS:
                time.sleep(CONFLICT_WAIT * 2**read)
            continue
        except records.RecordError as error:
            return Result(key, unit, "refused", str(error))
        if run.journal is not None and mark is not None:
            run.journal.faults = mark
        problems = verify_record(run.storage, target, run.root, expected)
        return Result(key, unit, "refused", problems[0]) if problems else Result(key, unit, ended, reason)
    raise ContractFailure("completion_conflict_unreadable")


def recheck_unit(run: CaptureRun, unit: dict, key: str, payload: str, retrieved_at_utc: str, expected: Mapping[str, Any]) -> Result | None:
    """A monthly run's decision for a downloaded payload against the unit's records, or None when no record has that payload (a new version).

    The unit's newest record has the payload: unchanged, nothing is written. An older record has it and the newest one does not (A, then B, then A): a
    reversion record names the older record's objects under a key that adds this run's ID, so the newest record is that payload again. No object is copied.
    """
    found = list_objects(run.storage, records.unit_prefix(run.root, key))
    same = [item for item in found if (named := records.key_payload(item["Key"], run.root, key)) is not None and named[0] == payload]
    if not same:
        return None
    latest = newest(found)
    chosen = latest if latest in same else newest(same)
    problems = verify_record(run.storage, chosen["Key"], run.root, expected)
    if problems:
        return Result(key, unit, "refused", problems[0])
    if chosen is latest:
        return Result(key, unit, "unchanged")
    older = read_record(run.storage, chosen["Key"], run.root)
    body = records.reverted(older, run_id=run.run_id, code_sha256=run.code_sha256, bundle_sha256=run.bundle_sha256, retrieved_at_utc=retrieved_at_utc)
    return close_unit(run, unit, key, records.record_key(run.root, key, payload, run.run_id), records.encode(body), expected, REVERTED)


def attempt(run: CaptureRun, unit: dict, spec: dict, key: str, opener: Any, state: dict, expected: Mapping[str, Any]) -> Result:
    """Capture, store and close one unit. Its working folder is always removed."""
    folder = run.scratch / key
    folder.mkdir(parents=True)
    try:
        handler = redownload.handler_mode(unit)
        if state["bytes_downloaded"] + unit["bytes"] > run.byte_cap:
            raise ContractFailure("byte_cap_exceeded")
        if shutil.disk_usage(run.scratch).free <= 2 * unit["bytes"] + 2**30:
            raise ContractFailure("not_enough_disk_space")
        registry = registry_of(run, unit)
        adapter = {**ADAPTERS, **run.adapters}.get(handler)
        if adapter is None:
            require(handler == "file", "The unit has no reviewed handler")
        try:
            receipt_path = adapter(unit, spec, folder, run, opener) if adapter else capture_file(unit, spec, folder, run, opener)
        except rate_limit.RateLimited as limited:
            raise rate_limit_of(run, limited) from None
        receipt = read_json(receipt_path)
        if receipt["snapshot_status"] != "acquired_unvalidated":
            raise ContractFailure((receipt["quality_profile"]["checks_failed"] or ["capture_failed"])[0])
        mapping = archive_mapping(unit, spec, receipt_path, registry) if spec.get("archive_members") else None
        payload = records.payload_sha256(local_payload(receipt, mapping))
        target = records.record_key(run.root, key, payload)
        if rechecks(run, unit) and (decided := recheck_unit(run, unit, key, payload, receipt["acquisition"]["retrieved_at_utc"], expected)) is not None:
            return decided
        generation = len(list_objects(run.storage, records.unit_prefix(run.root, key))) + 1
        try:
            snapshot_id = claim_intent(run, unit, key, generation, receipt["snapshot_id"])
        except IntentRefused as refusal:
            return Result(key, unit, "refused", refusal.reason)
        if snapshot_id != receipt["snapshot_id"]:
            receipt_path.write_bytes(encoded_json(receipt | {"snapshot_id": snapshot_id}))
        stored = upload_snapshot(receipt_path, [], run.storage, {}, registry, validator_of(run), archive_mapping=mapping)
        receipt_object, manifests, data, other = stored_objects(read_json(Path(stored["reconciliation_path"])))
        route = load_routes(registry)[unit["source_id"]]
        body = records.build(
            key=key,
            source_id=unit["source_id"],
            collection=f"{route['publisher']}/{route['collection']}",
            mode=redownload.handler_mode(unit),
            run_id=run.run_id,
            code_sha256=run.code_sha256,
            bundle_sha256=run.bundle_sha256,
            retrieved_at_utc=receipt["acquisition"]["retrieved_at_utc"],
            receipt=receipt_object,
            manifests=manifests,
            data_objects=data,
            other_objects=other,
        )
        if body["payload_sha256"] != payload:
            raise ContractFailure("payload_differs_after_storage")
        return close_unit(run, unit, key, target, records.encode(body), expected)
    finally:
        shutil.rmtree(folder)


def failure_reason(error: Exception) -> str:
    """A fixed code for a failed unit. Response text and exception messages never reach a record."""
    if isinstance(error, ContractFailure):
        return str(error)
    if isinstance(error, SpecError):
        return "spec_incomplete"
    return redownload.error_category(error)


def decide_skip(run: CaptureRun, unit: Mapping[str, Any], key: str, now: datetime, expected: Mapping[str, Any]) -> tuple[str, str | None]:
    """Before any request: ``complete`` (a verified record), ``refused`` (a record that does not verify), ``stale`` (retry interval not over) or ``go``."""
    if not rechecks(run, unit):
        found = list_objects(run.storage, records.unit_prefix(run.root, key))
        if found:
            problems = verify_record(run.storage, newest(found)["Key"], run.root, expected)
            return ("refused", problems[0]) if problems else ("complete", None)
    stale = list_objects(run.storage, records.stale_prefix(run.root, key))
    if stale and now - newest(stale)["LastModified"] < run.stale_retry:
        return "stale", None
    return "go", None


def cooldown_status(run: CaptureRun, source: str, now: datetime) -> tuple[str, datetime | None]:
    """Whether a source may be asked at NOW: ``open``, ``cooling`` with its resume time, or ``unreadable``.

    The resume time is the largest one saved under the source's cooldown folder by any run of this root. A listing that fails or a key that is no cooldown
    record gives ``unreadable``, and the caller treats it as blocked, so a request is never sent on a cooldown that cannot be read.
    """
    try:
        keys = [item["Key"] for item in list_objects(run.storage, source_cooldown.prefix(run.root, source))]
        latest = source_cooldown.latest(run.root, source, keys)
    except (StorageError, source_cooldown.CooldownError):
        return "unreadable", None
    return ("cooling", latest) if latest is not None and now < latest else ("open", None)


def save_cooldown(run: CaptureRun, source: str, seconds: int, form: str) -> datetime:
    """Save the source's cooldown create-once and return its resume time. A failed save raises, so the run stops before another request."""
    observed = run.clock()
    until = source_cooldown.resume_time(observed, seconds)
    put_once(run, source_cooldown.key(run.root, source, until, run.run_id), source_cooldown.body(source, run.run_id, observed, seconds, form, until))
    return until


def entry(result: Result) -> dict[str, Any]:
    return {"unit_key": result.key, "source_id": result.unit["source_id"], "snapshot_id": result.unit["snapshot_id"], "reason": result.reason}


def execute(run: CaptureRun) -> dict[str, Any]:
    """Process the queue once and return the run record, which is also stored create-once. A crash leaves no run record.

    The downloader reads the approved hosts file from the environment. The run sets the files it checked against the queue and restores the old values.
    A run that starts while another run executes in the process raises ``RunInProgress`` before it reads or changes anything.
    """
    with owning_process():
        units, specs, queue_sha256 = load_inputs(run)
        require(capture_root() == run.root, "The run's root differs from the capture root the storage path writes under")
        hosts, approval = run.hosts, run.hosts_approval
        if hosts is None or approval is None:
            raise RegistryError("A capture run needs the approved hosts file and its approval")
        problems = redownload_route.host_problems(run.queue_path.read_bytes(), hosts, approval)
        require(not problems, "Approved hosts: " + "; ".join(problems))
        names = (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE)
        earlier = {name: os.environ.get(name) for name in names}
        os.environ[names[0]], os.environ[names[1]] = str(hosts), str(approval)
        try:
            return process(run, units, specs, queue_sha256)
        finally:
            for name, value in earlier.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


def process(run: CaptureRun, units: list[dict], specs: dict[str, dict], queue_sha256: str) -> dict[str, Any]:
    base = run.opener if run.opener is not None else transport.ReviewedTransport(transport.PublicHTTPSHandler())
    require(isinstance(base, transport.ReviewedTransport), "The opener must be a reviewed transport, so its redirects are reviewed")
    observer, journal = Observer(run.byte_cap), Journal()
    opener = CountingTransport(base, observer)
    run = replace(run, storage=JournalStorage(run.storage, journal), journal=journal, api_request=observed_request(observer, run.api_request))
    clear_scratch(run.scratch)
    started = run.clock()
    counters: Counter = Counter()
    state: dict[str, Any] = {"attempts": {}, "bytes_downloaded": 0, "paused_sources": []}
    done: dict[str, list] = {"completed": [], "unchanged": [], "changed": [], "failed": [], "refused": [], "skipped_complete": [], "skipped_recent_stale": []}
    outside: dict[str, list[str]] = {"manual_not_attempted": [], "keyed_not_attempted": [], "paused_not_attempted": []}
    paused: set[str] = set()
    tried: set[str] = set()
    checked: set[str] = set()
    cooling: dict[str, str] = {}
    with observing(observer):
        for unit in units:
            if unit["disposition"] not in redownload.ACTIVE:
                continue
            if unit["disposition"] == "approved_manual" or redownload.handler_mode(unit) in MANUAL_MODES:
                outside["manual_not_attempted"].append(unit["snapshot_id"])
                continue
            source = unit["source_id"]
            if source in paused:
                outside["paused_not_attempted"].append(unit["snapshot_id"])
                continue
            handler = redownload.handler_mode(unit)
            if handler in redownload.SECRET_MODES and run.task_keys is None and handler not in run.adapters:
                outside["keyed_not_attempted"].append(unit["snapshot_id"])
                continue
            first = source not in tried
            spec = specs.get(unit["snapshot_id"])
            try:
                if spec is None:
                    raise SpecError("The unit has no specification")
                key, expected = unit_key_of(unit, spec), expectation(run, unit, spec)
            except SpecError:
                tried.add(source)
                done["failed"].append({"unit_key": None, "source_id": source, "snapshot_id": unit["snapshot_id"], "reason": "spec_incomplete"})
                if first:
                    paused.add(source)
                continue
            decided = journal.faults
            try:
                verdict, why = decide_skip(run, unit, key, run.clock(), expected)
            except StorageError as error:
                tried.add(source)
                done["failed"].append({"unit_key": key, "source_id": source, "snapshot_id": unit["snapshot_id"], "reason": failure_reason(error)})
                paused.add(source)
                continue
            if journal.faults != decided:
                paused.add(source)
            if verdict != "go":
                if verdict == "complete":
                    done["skipped_complete"].append(key)
                elif verdict == "stale":
                    done["skipped_recent_stale"].append(key)
                else:
                    done["refused"].append({"unit_key": key, "source_id": source, "snapshot_id": unit["snapshot_id"], "reason": why})
                continue
            if source not in checked:
                checked.add(source)
                found, until = cooldown_status(run, source, run.clock())
                if found != "open":
                    cooling[source] = "unreadable" if until is None else until.isoformat()
            if source in cooling:
                outside["paused_not_attempted"].append(unit["snapshot_id"])
                if run.log is not None:
                    run.log({"snapshot_id": unit["snapshot_id"], "source_id": source, "status": "paused_cooldown", "resume_at_utc": cooling[source]})
                continue
            tried.add(source)
            counters["units_attempted"] += 1
            state["attempts"][key] = state["attempts"].get(key, 0) + 1
            limited: RateLimit | None = None
            started_with = journal.faults
            try:
                result = attempt(run, unit, spec, key, opener, state, expected)
            except (CaptureError, StorageError, RegistryError, ValueError, OSError, KeyError) as error:
                result = Result(key, unit, "failed", failure_reason(error))
                limited = error if isinstance(error, RateLimit) else None
            state["bytes_downloaded"] = observer.bytes
            if result.reason == source_cooldown.REASON:
                seconds, form = (limited.retry_after_seconds, limited.form) if limited else (source_cooldown.DEFAULT_SECONDS, "default")
                cooling[source] = save_cooldown(run, source, seconds, form).isoformat()
            if result.status in {"failed", "refused"} and result.reason not in INTENT_REASONS and (first or result.reason in PAUSING):
                paused.add(source)
            if journal.faults != started_with:
                paused.add(source)
            if result.status in {"unchanged", "changed"}:
                counters[result.status] += 1
            done[result.status].append(entry(result))
            if run.log is not None:
                run.log(
                    {"snapshot_id": unit["snapshot_id"], "source_id": source, "status": result.status}
                    | ({} if limited is None else {"retry_after_seconds": limited.retry_after_seconds})
                )
    return finish(run, started, observer, journal, state, done, outside, paused | set(cooling), counters, queue_sha256, cooling)


def finish(
    run: CaptureRun,
    started: datetime,
    observer: Observer,
    journal: Journal,
    state: dict[str, Any],
    done: dict[str, list],
    outside: dict[str, list[str]],
    paused: set[str],
    counters: Counter,
    queue_sha256: str,
    cooling: Mapping[str, str],
) -> dict[str, Any]:
    """Save the run state, derive the counters from the observer and the journal, and store the run record last.

    ``paused`` also holds the sources that wait on a cooldown. ``cooling`` names each such source with the time it may be asked again, ``unreadable`` when its
    cooldown could not be read; the run record lists it as ``source_cooldowns`` only when it is not empty.
    """
    state["paused_sources"] = sorted(paused)
    save_state(run, state)
    done["skipped_complete"].sort()
    done["skipped_recent_stale"].sort()
    writes = sorted(journal.keys)
    counters.update({name: count for name, count in tally_writes(run.root, writes).items() if name != "run_objects"})
    counters["http_requests"], counters["bytes_downloaded"], counters["skipped_complete"], counters["skipped_recent_stale"] = (
        observer.requests,
        observer.bytes,
        len(done["skipped_complete"]),
        len(done["skipped_recent_stale"]),
    )
    run_record = {
        "kind": "ingestion_run",
        "run_id": run.run_id,
        "root": run.root,
        "queue_sha256": queue_sha256,
        "code_sha256": run.code_sha256,
        "bundle_sha256": run.bundle_sha256,
        "recheck": run.recheck,
        "stale_retry_seconds": int(run.stale_retry.total_seconds()),
        "started_at_utc": started.isoformat(),
        "finished_at_utc": run.clock().isoformat(),
        "counters": {name: counters[name] for name in COUNTERS},
        "writes": writes,
        "units": {**done, "paused_sources": sorted(paused), **outside},
    }
    if cooling:
        run_record["source_cooldowns"] = dict(sorted(cooling.items()))
    save_run_record(run, run_record)
    return run_record


def put_once(run: CaptureRun, key: str, body: bytes) -> None:
    with tempfile.TemporaryDirectory(prefix="run_object_") as folder:
        local = Path(folder) / "object.json"
        local.write_bytes(body)
        redownload.create_once(run.storage, key, local)


def save_state(run: CaptureRun, state: dict[str, Any]) -> None:
    """Save the run state, only when it changed from the empty state a fresh run starts with."""
    if state["attempts"] or state["paused_sources"] or state["bytes_downloaded"]:
        put_once(run, f"{records.run_state_prefix(run.root, run.run_id)}000001.json", encoded_json(state))


def save_run_record(run: CaptureRun, run_record: dict[str, Any]) -> None:
    put_once(run, records.run_record_key(run.root, run.run_id), encoded_json(run_record))
