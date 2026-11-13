"""Capture mode: ingest each queue unit once and let its completion record decide skip (design 8.3, decisions D3 and D10).

The run reads the tracked queue and ``capture_specs.json``, never a stored receipt. For each unit it lists ``<root>ingestion_units/<unit_key>/`` before any
request. A verified record there means no request. Otherwise the unit is downloaded into its own working folder, stored through the project's storage path,
read back by version and closed by one create-once completion record. Before it stores anything, the unit claims a create-once intent that names the
snapshot ID its objects are stored under, so a retry after a crash stores under the same keys (``ingestion_intent.py``). The working folder is deleted
after the record, or after a failure. The first attempted unit of a source that fails its contract pauses that source for the run. The run writes one
run record and, only when something was attempted, one state object under ``<root>ingestion_runs/<run_id>/``. It sits beside the earlier redownload and
compare paths in ``redownload.py``, which later work removes.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shutil
import tempfile
import urllib.request
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from scripts.acquisition import completion_record as records
from scripts.acquisition import ingestion_intent as intents
from scripts.acquisition import redownload, redownload_route, task_role, transport
from scripts.acquisition.capture import capture, receipt_validator
from scripts.acquisition.collection_layout import capture_root, load_routes
from scripts.acquisition.mapped_archives import expand_tree
from scripts.acquisition.s3_store import StorageError, encoded_json, upload_snapshot
from scripts.acquisition.source_registry import RegistryError, canonical_hash, load_registry, read_json, require
from scripts.acquisition.transport import CaptureError, Limits, canonical_url

BYTE_CAP = 128 * 2**30
STALE_RETRY = timedelta(hours=20)
MAX_ATTEMPTS = 2
UNIT_FOLDER = re.compile(r"[a-f0-9]{64}")
MANUAL_MODES = redownload.MANUAL_MODES
COLLECTOR_MODES = frozenset(redownload.PLANNED) | {"mmd_api"}
SKIPPED_ROLES = frozenset({"capture_receipt", "transport_audit", "archive_inventory"})
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


class SpecError(ValueError):
    """A unit's specification cannot decide its request identity."""


class IntentRefused(Exception):
    """A stored intent that cannot be trusted. The unit is refused with this fixed reason and never gets a new snapshot ID."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def count_request(handler: Any, request: Any) -> Any:
    """The preprocessor urllib calls for each HTTPS request an opener sends, a redirect's next request included."""
    handler.count += 1
    return request


def request_counter() -> Any:
    """A handler that counts the opener's requests. urllib finds its ``https_request`` by name, so the class is built with that name."""
    handler = type("RequestCounter", (urllib.request.BaseHandler,), {"https_request": count_request})()
    handler.count = 0
    return handler


class CountingTransport(transport.ReviewedTransport):
    """The reviewed transport with the run's one shared request counter below the redirect policy."""

    def __init__(self, base: transport.ReviewedTransport) -> None:
        self.counter = request_counter()
        super().__init__(*base.handlers, self.counter)


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
    byte_cap: int = BYTE_CAP
    stale_retry: timedelta = STALE_RETRY
    clock: Callable[[], datetime] = utc_now
    log: Callable[[dict], None] | None = None
    cache: dict[str, Any] = field(default_factory=dict)


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
    return Path(collector.execute(plan, batch, folder, True, False, client_for(run, "census_acs_api"), {}, run.api_request or api.request_live)["receipt_path"])


def adapt_census_detailed(unit: dict, spec: dict, folder: Path, run: CaptureRun, opener: Any) -> Path:
    from scripts.acquisition import collect_census_acs_detailed as collector

    plan, batch = planned_item(run, "census_acs_detailed", spec)
    request = run.api_request or collector.live_request
    return Path(collector.execute(plan, batch, folder, True, False, client_for(run, "census_acs_detailed"), {}, request)["receipt_path"])


def adapt_hud_api(unit: dict, spec: dict, folder: Path, run: CaptureRun, opener: Any) -> Path:
    from scripts.acquisition import collect_hud_api as collector
    from scripts.acquisition import hud_api_transport as api

    plan, batch = planned_item(run, "hud_api", spec)
    return Path(collector.execute(plan, batch, folder, True, False, client_for(run, "hud_api"), {}, run.api_request or api.request_live)["receipt_path"])


def adapt_mmd(unit: dict, spec: dict, folder: Path, run: CaptureRun, opener: Any) -> Path:
    from scripts.acquisition import collect_mmd_api as collector

    mmd = spec["mmd"]
    return Path(collector.execute(mmd["measure_id"], mmd["year"], True, False, root=folder, client=run.storage)["receipt_path"])


def adapt_cms_owners(unit: dict, spec: dict, folder: Path, run: CaptureRun, opener: Any) -> Path:
    from scripts.acquisition import store_cms_owners as collector

    plan, release = planned_item(run, "cms_owners_org", spec)
    return Path(collector.execute(plan, release, folder, True, False, None, {}, opener, Limits(attempts=MAX_ATTEMPTS))["receipt_path"])


def adapt_hcai_util(unit: dict, spec: dict, folder: Path, run: CaptureRun, opener: Any) -> Path:
    from scripts.acquisition import store_hcai_util as collector

    plan, entry = planned_item(run, "hcai_util_workbook", spec)
    return Path(collector.execute(plan, entry, folder, True, False, None, {}, opener, Limits(attempts=MAX_ATTEMPTS))["receipt_path"])


def adapt_onc_mu(unit: dict, spec: dict, folder: Path, run: CaptureRun, opener: Any) -> Path:
    from scripts.acquisition import store_onc_mu as collector

    plan, _ = planned_item(run, "onc_mu_hospital", spec)
    # The plan raises the time and size bounds for this one large file; only the attempt count is lowered.
    limits = Limits(attempts=MAX_ATTEMPTS, timeout_seconds=120, max_seconds=plan["limits"]["max_seconds"], max_bytes=plan["limits"]["max_bytes"])
    return Path(collector.execute(plan, folder, True, False, None, {}, opener, limits)["receipt_path"])


ADAPTERS: dict[str, Callable[..., Path]] = {
    "bls_api_v2": adapt_bls,
    "census_acs_api": adapt_census_api,
    "census_acs_detailed": adapt_census_detailed,
    "hud_api": adapt_hud_api,
    "mmd_api": adapt_mmd,
    "cms_owners_org": adapt_cms_owners,
    "hcai_util_workbook": adapt_hcai_util,
    "onc_mu_hospital": adapt_onc_mu,
}
PAUSING = frozenset({"bls_quota", "http_429"})
INTENT_REASONS = frozenset({"intent_forged", "intent_unreadable"})


def base_text(digest_hex: str) -> str:
    return base64.b64encode(bytes.fromhex(digest_hex)).decode()


def member_name(chain: Iterable[str]) -> str:
    """The stored name of an archive member, as the mapped storage path names it."""
    name = re.sub(r"[^A-Za-z0-9._-]", "_", Path(list(chain)[-1]).name)
    return ("member_" + name if not name or not name[0].isalnum() else name)[:200]


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


def named(storage: Any) -> dict[str, str]:
    return {"Bucket": storage.configuration["data_bucket_name"], "ExpectedBucketOwner": storage.configuration["expected_account_id"]}


def list_objects(storage: Any, prefix: str) -> list[dict]:
    """Every current object under a prefix with its last modified time, from the store's own listing."""
    found: list[dict] = []
    options: dict[str, str] = {}
    while True:
        page = storage.client.list_objects_v2(**named(storage), Prefix=prefix, **options)
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
            page = storage.client.list_object_versions(**named(storage), Prefix=prefix, **options)
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


def diff_versions(before: list[dict], after: list[dict], allowed_new: set[str]) -> list[str]:
    """What differs between two version listings, as problems: a removed or changed version, any new delete marker, a new version outside ``allowed_new``."""
    problems = []
    earlier = {(item["key"], item["version_id"], item["kind"]): item for item in before}
    later = {(item["key"], item["version_id"], item["kind"]): item for item in after}
    for name, item in earlier.items():
        if name not in later or later[name]["etag"] != item["etag"]:
            problems.append(f"version of {item['key']} was removed or replaced")
    for name, item in later.items():
        if name in earlier:
            continue
        if item["kind"] == "delete_marker":
            problems.append(f"new delete marker on {item['key']}")
        elif item["key"] not in allowed_new:
            problems.append(f"unexpected new version of {item['key']}")
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


def read_stored(storage: Any, key: str) -> bytes:
    """The bytes of one stored object at its current version."""
    reply = storage.call("s3api", "head-object", ["--key", key])
    with tempfile.TemporaryDirectory(prefix="record_read_") as folder:
        local = Path(folder) / "record.json"
        storage.call("s3api", "get-object", ["--key", key, "--version-id", str(reply["VersionId"]), str(local)])
        return local.read_bytes()


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


def claim_intent(run: CaptureRun, unit: dict, key: str, generation: int, snapshot_id: str, counters: Counter) -> str:
    """The snapshot ID this attempt stores under: its own when it wrote the intent, else the one an earlier attempt's intent already names."""
    target = intents.intent_key(run.root, key, generation)
    body = intents.encode(intents.build(unit_key=key, generation=generation, snapshot_id=snapshot_id, run_id=run.run_id))
    if create_new(run.storage, target, body):
        counters["intents_created"] += 1
        return snapshot_id
    return read_intent(run.storage, target, run.root, unit["source_id"])["snapshot_id"]


def verify_record(storage: Any, key: str, root: str) -> list[str]:
    """Problems with one stored record: unreadable, malformed, copied under another key, or naming an object that does not match."""
    try:
        record = read_record(storage, key, root)
    except (records.RecordError, StorageError) as error:
        return [f"{key}: {error}"]
    return [problem for item in records.objects_named(record) if (problem := verify_object(storage, item))]


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
        generation = len(list_objects(storage, records.unit_prefix(root, item["unit_key"])))
        if not list_objects(storage, intents.intent_key(root, item["unit_key"], generation)):
            problems.append(f"unit {item['unit_key']} has no intent for generation {generation}")
    return problems


def check_run(storage: Any, run_record: Mapping[str, Any], before: list[dict]) -> list[str]:
    """The host evidence step: verify the records by version and diff the version listings before and after the run.

    The run may add exactly its run record (and its state object when it attempted units) beside the objects and intents its own counters name, and
    nothing may be removed, replaced or marked deleted. Each unit the run stored has its intent.
    """
    root, run_id, counters = run_record["root"], run_record["run_id"], run_record["counters"]
    after = list_versions(storage, evidence_prefixes(root))
    known = {(item["key"], item["version_id"], item["kind"]) for item in before}
    new = [item for item in after if (item["key"], item["version_id"], item["kind"]) not in known and item["kind"] == "version"]
    own = f"{root}{records.RUNS_FOLDER}{run_id}/"
    own_keys = {item["key"] for item in new if item["key"].startswith(own)}
    allowed = {records.run_record_key(root, run_id), f"{records.run_state_prefix(root, run_id)}000001.json"}
    problems = diff_versions(before, after, {item["key"] for item in new})
    problems.extend(f"unexpected run object {key}" for key in sorted(own_keys - allowed))
    if records.run_record_key(root, run_id) not in own_keys:
        problems.append("the run record is not new")
    ledger = sum(item["key"].startswith(task_role.BLS_LEDGER_PREFIX) for item in new)
    new_intents = sorted(item["key"] for item in new if item["key"].startswith(f"{root}{intents.INTENTS_FOLDER}"))
    created = len(new) - len(own_keys) - ledger - len(new_intents)
    if created != counters["objects_created"] or ledger != counters["bls_ledger_entries_added"]:
        said = (counters["objects_created"], counters["bls_ledger_entries_added"])
        problems.append(f"the listing holds {created} new objects and {ledger} ledger entries; the counters say {said[0]} and {said[1]}")
    if len(new_intents) != counters["intents_created"]:
        problems.append(f"the listing holds {len(new_intents)} new intents; the counters say {counters['intents_created']}")
    problems.extend(intent_problems(storage, root, run_record, new_intents))
    problems.extend(verify_records(storage, root))
    if not run_record["recheck"]:
        for item in run_record["units"]["completed"]:
            found = list_objects(storage, records.unit_prefix(root, item["unit_key"]))
            if len(found) != 1:
                problems.append(f"unit {item['unit_key']} has {len(found)} completion records")
    return problems


def newest(found: list[dict]) -> dict:
    return max(found, key=lambda item: (item["LastModified"], item["Key"]))


def ledger_entries(storage: Any) -> int:
    return len(list_objects(storage, task_role.BLS_LEDGER_PREFIX))


def clear_scratch(scratch: Path) -> None:
    """Remove the working folders an earlier process left. Only folders named like a unit key are touched."""
    scratch.mkdir(parents=True, exist_ok=True)
    for child in scratch.iterdir():
        if child.is_dir() and not child.is_symlink() and UNIT_FOLDER.fullmatch(child.name):
            shutil.rmtree(child)


def local_payload(receipt: dict, mapping: dict | None) -> list[tuple[str, str]]:
    """The (stored name, SHA-256) pairs of the data-role files, known before anything is stored."""
    if mapping is not None:
        return [(member_name(item["member_chain"]), item["sha256"]) for item in mapping["members"] if item["role"] == "data"]
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
    limits = Limits(attempts=MAX_ATTEMPTS, max_bytes=min(4 * 2**30, max(2 * unit["bytes"], 2**20)))
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
        name = item.get("stored_file_name") or Path(item["storage_path"]).name
        (data if item["role"] == "data" else other).append(record | {"role": item["role"], "stored_file_name": name})
    return receipt["object"], [item["object"] for item in reconciliation["manifests"]], data, other


@dataclass
class Result:
    """What one unit ended with: its key, its source and how it counted."""

    key: str
    unit: dict
    status: str
    reason: str | None = None


def attempt(run: CaptureRun, unit: dict, spec: dict, key: str, counters: Counter, opener: Any, state: dict) -> Result:
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
        receipt_path = adapter(unit, spec, folder, run, opener) if adapter else capture_file(unit, spec, folder, run, opener)
        receipt = read_json(receipt_path)
        if receipt["snapshot_status"] != "acquired_unvalidated":
            raise ContractFailure((receipt["quality_profile"]["checks_failed"] or ["capture_failed"])[0])
        counters["bytes_downloaded"] += sum(item["byte_count"] for item in receipt["artifacts"] if item["role"] in {"data", "api_page"})
        state["bytes_downloaded"] += unit["bytes"]
        mapping = archive_mapping(unit, spec, receipt_path, registry) if spec.get("archive_members") else None
        payload = records.payload_sha256(local_payload(receipt, mapping))
        target = records.record_key(run.root, key, payload)
        if run.recheck and (found := list_objects(run.storage, target)):
            problems = verify_record(run.storage, found[0]["Key"], run.root)
            return Result(key, unit, "refused", problems[0]) if problems else Result(key, unit, "unchanged")
        generation = len(list_objects(run.storage, records.unit_prefix(run.root, key))) + 1
        try:
            snapshot_id = claim_intent(run, unit, key, generation, receipt["snapshot_id"], counters)
        except IntentRefused as refusal:
            return Result(key, unit, "refused", refusal.reason)
        if snapshot_id != receipt["snapshot_id"]:
            receipt_path.write_bytes(encoded_json(receipt | {"snapshot_id": snapshot_id}))
        stored = upload_snapshot(receipt_path, [], run.storage, {}, registry, validator_of(run), archive_mapping=mapping)
        counters["objects_created"] += stored["objects_created"]
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
        local = folder / "record.json"
        local.write_bytes(records.encode(body))
        redownload.create_once(run.storage, target, local)
        counters["objects_created"] += 1
        return Result(key, unit, "changed" if run.recheck else "completed")
    finally:
        shutil.rmtree(folder)


def failure_reason(error: Exception) -> str:
    """A fixed code for a failed unit. Response text and exception messages never reach a record."""
    if isinstance(error, ContractFailure):
        return str(error)
    if isinstance(error, SpecError):
        return "spec_incomplete"
    return redownload.error_category(error)


def decide_skip(run: CaptureRun, key: str, now: datetime) -> tuple[str, str | None]:
    """Before any request: ``complete`` (a verified record), ``refused`` (a record that does not verify), ``stale`` (retry interval not over) or ``go``."""
    if not run.recheck:
        found = list_objects(run.storage, records.unit_prefix(run.root, key))
        if found:
            problems = verify_record(run.storage, newest(found)["Key"], run.root)
            return ("refused", problems[0]) if problems else ("complete", None)
    stale = list_objects(run.storage, records.stale_prefix(run.root, key))
    if stale and now - newest(stale)["LastModified"] < run.stale_retry:
        return "stale", None
    return "go", None


def entry(result: Result) -> dict[str, Any]:
    return {"unit_key": result.key, "source_id": result.unit["source_id"], "snapshot_id": result.unit["snapshot_id"], "reason": result.reason}


def execute(run: CaptureRun) -> dict[str, Any]:
    """Process the queue once and return the run record, which is also stored create-once. A crash leaves no run record.

    The downloader reads the approved hosts file from the environment. The run sets the files it checked against the queue and restores the old values.
    """
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
    opener = CountingTransport(base)
    clear_scratch(run.scratch)
    started = run.clock()
    counters: Counter = Counter()
    state: dict[str, Any] = {"attempts": {}, "bytes_downloaded": 0, "paused_sources": []}
    ledger_before = ledger_entries(run.storage)
    done: dict[str, list[dict]] = {"completed": [], "unchanged": [], "changed": [], "failed": [], "refused": []}
    paused: set[str] = set()
    tried: set[str] = set()
    manual: list[str] = []
    keys_off: list[str] = []
    held: list[str] = []
    for unit in units:
        if unit["disposition"] not in redownload.ACTIVE:
            continue
        if unit["disposition"] == "approved_manual" or redownload.handler_mode(unit) in MANUAL_MODES:
            manual.append(unit["snapshot_id"])
            continue
        source = unit["source_id"]
        if source in paused:
            held.append(unit["snapshot_id"])
            continue
        handler = redownload.handler_mode(unit)
        if handler in redownload.SECRET_MODES and run.task_keys is None and handler not in run.adapters:
            keys_off.append(unit["snapshot_id"])
            continue
        spec = specs.get(unit["snapshot_id"])
        try:
            if spec is None:
                raise SpecError("The unit has no specification")
            key = unit_key_of(unit, spec)
        except SpecError:
            done["failed"].append({"unit_key": None, "source_id": source, "snapshot_id": unit["snapshot_id"], "reason": "spec_incomplete"})
            continue
        verdict, why = decide_skip(run, key, run.clock())
        if verdict != "go":
            if verdict == "complete":
                counters["skipped_complete"] += 1
            elif verdict == "stale":
                counters["skipped_recent_stale"] += 1
            else:
                done["refused"].append({"unit_key": key, "source_id": source, "snapshot_id": unit["snapshot_id"], "reason": why})
            continue
        first = source not in tried
        tried.add(source)
        counters["units_attempted"] += 1
        state["attempts"][key] = state["attempts"].get(key, 0) + 1
        try:
            result = attempt(run, unit, spec, key, counters, opener, state)
        except (CaptureError, StorageError, RegistryError, ValueError, OSError, KeyError) as error:
            result = Result(key, unit, "failed", failure_reason(error))
        if result.status in {"failed", "refused"} and result.reason not in INTENT_REASONS and (first or result.reason in PAUSING):
            paused.add(source)
        if result.status in {"unchanged", "changed"}:
            counters[result.status] += 1
        done[result.status].append(entry(result))
        if run.log is not None:
            run.log({"snapshot_id": unit["snapshot_id"], "source_id": source, "status": result.status})
    counters["http_requests"] = opener.counter.count
    counters["bls_ledger_entries_added"] = max(0, ledger_entries(run.storage) - ledger_before)
    state["paused_sources"] = sorted(paused)
    run_record = {
        "kind": "ingestion_run",
        "run_id": run.run_id,
        "root": run.root,
        "queue_sha256": queue_sha256,
        "code_sha256": run.code_sha256,
        "bundle_sha256": run.bundle_sha256,
        "recheck": run.recheck,
        "started_at_utc": started.isoformat(),
        "finished_at_utc": run.clock().isoformat(),
        "counters": {name: counters[name] for name in COUNTERS},
        "units": {**done, "paused_sources": sorted(paused), "manual_not_attempted": manual, "keyed_not_attempted": keys_off, "paused_not_attempted": held},
    }
    save_state(run, state)
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
