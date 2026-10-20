"""Exercise storage reuse and manifest observation preservation with synthetic inputs only."""

from __future__ import annotations

import argparse
import copy
import hashlib
import inspect
import json
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from jinja2 import Environment, StrictUndefined, select_autoescape

from scripts.acquisition import s3_store, single_copy
from scripts.acquisition.capture import capture
from scripts.acquisition.source_registry import canonical_hash
from scripts.acquisition.tests.test_capture import Opener, Response, setup_capture
from scripts.acquisition.tests.test_s3_store import OUTPUTS, FakeS3
from scripts.acquisition.transport import CaptureError
from scripts.lakehouse import bronze, bronze_metadata, ipps_file_labels, pins, storage_metadata_comparison
from scripts.lakehouse.duckdb_cli import duckdb_binary
from scripts.process import run_command


class ManifestStorage:
    """Read synthetic versioned manifest bytes through the bronze storage protocol."""

    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self.objects = objects

    def list_keys(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        return sorted(identity for identity in self.objects if identity[0].startswith(prefix))

    def list_current(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        return self.list_keys(bucket, prefix)

    def get(self, bucket: str, key: str, version_id: str) -> bytes:
        return self.objects[(key, version_id)]

    def download(self, bucket: str, key: str, version_id: str, path: Path) -> None:
        path.write_bytes(self.get(bucket, key, version_id))


def observation_aliases(root: Path) -> bool:
    """Discover two manifest observations with one loaded logical identity without losing either manifest."""
    payload = b"synthetic\nvalue\n"
    digest = hashlib.sha256(payload).hexdigest()
    physical = {"bucket": "synthetic-bucket", "key": "cms/test/datasets/one/synthetic.csv", "version_id": "v1", "sha256": digest, "byte_count": len(payload)}
    manifests = {
        (f"cms/test/manifests/{dataset}/manifest.json", "v1"): json.dumps(
            {
                "snapshot_id": "capture",
                "source_id": "synthetic",
                "release_id": "2026-01-01",
                "objects": [{"role": "data", "dataset_id": dataset, "object": physical}],
            }
        ).encode()
        for dataset in ("one", "two")
    }
    storage = ManifestStorage(manifests)
    table = {"table": "synthetic_files", "collection": "cms/test", "member_pattern": r"\.csv$", "format": "csv"}
    path = root / "tables.json"
    path.write_text(json.dumps({"tables": [table]}))
    config = bronze.load_table_map(path)
    inputs, excluded = bronze.discover(storage, "synthetic-bucket", config, ["synthetic_files"])
    observations = [*inputs, *(item["copy"] for item in excluded if "copy" in item)]
    return len(inputs) == 1 and len(observations) == 2 and len({item["manifest_key"] for item in observations}) == 2


def cross_release_reuse(root: Path) -> bool:
    """Store identical synthetic bytes for two releases through the actual storage boundary."""
    path = root / "synthetic.csv"
    path.write_bytes(b"synthetic\nvalue\n")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    client: Any = FakeS3()
    client.storage_policy = {"enabled": True, "objects": {}, "evidence_objects": {}, "bootstrap_sha256": "0" * 64}
    first, _ = s3_store.store_file(client, path, f"cms/test/datasets/one/release_date=2026-01-01/{digest}/one.csv")
    second, _ = s3_store.store_file(client, path, f"cms/test/datasets/one/release_date=2026-04-01/{digest}/two.csv")
    return first == second and len(client.objects) == 1


def missing_client_policy(root: Path) -> bool:
    """A wrapper missing its required policy must fail before any storage request."""
    path = root / "missing_policy.csv"
    path.write_bytes(b"synthetic\nvalue\n")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    client: Any = FakeS3()
    if hasattr(client, "storage_policy"):
        del client.storage_policy
    try:
        s3_store.store_file(client, path, f"cms/test/datasets/one/release_date=2026-01-01/{digest}/one.csv")
    except AttributeError as error:
        return "storage_policy" in str(error) and not client.calls
    return False


def comparison_uses_pinned_cli(root: Path) -> bool:
    """Run the actual read-only comparison query and require the project executable at its process boundary."""
    calls: list[tuple[str, list[str]]] = []
    database = root / "comparison_probe.duckdb"
    run_command(str(duckdb_binary()), [str(database), "-init", "/dev/null", "-c", "create table synthetic_marker(value integer)"], timeout=30, check=True)
    original = run_command

    def observed(program: str, args: list[str], **kwargs: Any) -> Any:
        calls.append((program, args))
        return original(program, args, **kwargs)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(storage_metadata_comparison, "run_command", observed)
        rows = storage_metadata_comparison.query(database, "select 173 as synthetic_result")
    return rows == [{"synthetic_result": 173}] and bool(calls) and all(program == str(duckdb_binary()) and "-readonly" in args for program, args in calls)


def evidence_prefixes(root: Path) -> dict[str, bool]:
    """Separate identical data/audit bytes by category, keeping the disabled path and unknown-category refusal."""
    payload = b"synthetic\nevidence\n"
    path = root / "evidence.txt"
    path.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    data_key = f"cms/test/datasets/one/release_date=2026-01-01/{digest}/data.csv"
    audit_key = f"cms/test/audit/capture_one/{digest}/manifest.json"
    reference_key = f"cms/test/references/release_date=2026-01-01/{digest}/readme.txt"
    client: Any = FakeS3()
    client.storage_policy = {"enabled": True, "objects": {}, "evidence_objects": {}, "bootstrap_sha256": "0" * 64}
    data, _ = s3_store.store_file(client, path, data_key)
    audit, _ = s3_store.store_file(client, path, audit_key)
    reference, _ = s3_store.store_file(client, path, reference_key)
    data_again, data_created = s3_store.store_file(client, path, data_key)
    audit_again, audit_created = s3_store.store_file(client, path, audit_key)
    results = {
        "audit_payload_named_manifest_stays_on_evidence_prefix": audit["key"] == "bronze_evidence/sha256/" + digest,
        "identical_data_and_audit_bytes_have_one_copy_per_prefix": data["key"] == "bronze_objects/sha256/" + digest
        and audit["key"] != data["key"]
        and len(client.objects) == 2
        and data_again == data
        and audit_again == audit
        and not data_created
        and not audit_created,
        "reference_payload_uses_data_content_index": reference == data,
    }
    disabled: Any = FakeS3()
    data_off, _ = s3_store.store_file(disabled, path, data_key)
    audit_off, _ = s3_store.store_file(disabled, path, audit_key)
    results["setting_off_reaches_neither_new_prefix"] = (
        data_off["key"] == data_key
        and audit_off["key"] == audit_key
        and all(not key.startswith(("bronze_objects/", "bronze_evidence/")) for key in disabled.objects)
    )
    unknown: Any = FakeS3()
    unknown.storage_policy = client.storage_policy
    try:
        s3_store.store_file(unknown, path, f"cms/test/unknown/capture_one/{digest}/data.csv")
    except CaptureError as error:
        results["unknown_category_fails_before_storage"] = "approved storage prefixes" in str(error) and not unknown.calls
    else:
        results["unknown_category_fails_before_storage"] = False
    return results


def evidence_capture_workflow(root: Path) -> dict[str, bool]:
    """Publish an evidence-only synthetic capture through upload_snapshot, retaining its original manifest path."""
    with pytest.MonkeyPatch.context() as patch:
        source, plan, validator = inspect.unwrap(setup_capture)(patch)
        patch.setattr(s3_store, "require_collection_scope", lambda _source_id: None)
        patch.setattr("scripts.acquisition.source_registry.require_collection_scope", lambda _source_id: None)
        registry = {"sources": [source]}
        route = {"publisher": "example_publisher", "collection": "example_collection", "layout_sha256": "a" * 64}
        patch.setattr(s3_store, "load_routes", lambda _registry: {source["source_id"]: route})
        receipt = capture(plan, root / "evidence_capture", registry, validator, opener=Opener(Response(b"id,value\nsynthetic,one\n")))
        document = json.loads(receipt.read_text())
        document["snapshot_status"], document["verification"]["status"] = "evidence_only_partial", "evidence_only"
        receipt.write_bytes(s3_store.encoded_json(document))
        client: Any = FakeS3()
        client.storage_policy = {"enabled": True, "objects": {}, "evidence_objects": {}, "bootstrap_sha256": "0" * 64}
        s3_store.upload_snapshot(receipt, [], client, OUTPUTS, registry, validator, audit_only=True)
        manifest = next((receipt.parent / "s3_collections").glob("*/manifest.json"))
        body = json.loads(manifest.read_text())
        reconciliation = json.loads((receipt.parent / "s3_collections_reconciliation.json").read_text())
        record = reconciliation["manifests"][0]["object"]
        expected = f"example_publisher/example_collection/audit/{document['snapshot_id']}/{s3_store.fingerprint(manifest)[0]}/manifest.json"
        before = copy.deepcopy(client.objects)
        s3_store.upload_snapshot(receipt, [], client, OUTPUTS, registry, validator, audit_only=True)
        return {
            "evidence_only_capture_payloads_use_evidence_prefix": bool(body["objects"])
            and all(item["object"]["key"].startswith("bronze_evidence/sha256/") for item in body["objects"]),
            "evidence_only_manifest_keeps_legacy_audit_path": record["key"] == expected and client.objects[expected]["body"] == manifest.read_bytes(),
            "evidence_only_retry_keeps_versions_and_manifest": before == client.objects,
        }


def evidence_index_workflow(root: Path) -> dict[str, bool]:
    """Bind separate immutable maps to identical data/evidence bytes and race only the evidence namespace."""
    path = root / "scoped_index.csv"
    path.write_bytes(b"synthetic\nscoped-index\n")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    client: Any = FakeS3()
    data_key = f"cms/index/datasets/one/release_date=2026-01-01/{digest}/data.csv"
    audit_key = f"cms/index/audit/capture_one/{digest}/payload.csv"
    data, _ = s3_store.store_file(client, path, data_key)
    audit, _ = s3_store.store_file(client, path, audit_key)
    fields = ("bucket", "key", "version_id", "sha256", "byte_count")
    document: dict[str, Any] = {
        "schema_version": 2,
        "generation": "0" * 64,
        "objects": {digest: {name: data[name] for name in fields}},
        "evidence_objects": {digest: {name: audit[name] for name in fields}},
    }
    index = root / "scoped_index.json"
    policy = root / "scoped_policy.json"

    def configured() -> dict[str, Any]:
        index.write_text(json.dumps(document))
        policy.write_text(
            json.dumps({"single_copy_enabled": True, "bootstrap_index": str(index), "bootstrap_index_sha256": hashlib.sha256(index.read_bytes()).hexdigest()})
        )
        return single_copy.load_policy(policy)

    results = {}
    try:
        client.storage_policy = configured()
        before = copy.deepcopy(client.objects)
        data_again, made_data = s3_store.store_file(client, path, data_key)
        audit_again, made_audit = s3_store.store_file(client, path, audit_key)
        results["separate_bootstrap_indexes_reuse_exact_versions"] = (
            data_again == data
            and audit_again == audit
            and data_again["key"] != audit_again["key"]
            and not made_data
            and not made_audit
            and before == client.objects
        )
    except CaptureError:
        results["separate_bootstrap_indexes_reuse_exact_versions"] = False
    document["evidence_objects"][digest] = document["objects"][digest]
    try:
        configured()
    except CaptureError as error:
        results["cross_scope_bootstrap_reference_is_refused"] = "crosses its data or evidence scope" in str(error)
    else:
        results["cross_scope_bootstrap_reference_is_refused"] = False
    client.storage_policy = {"enabled": True, "objects": {}, "evidence_objects": document["evidence_objects"], "bootstrap_sha256": "0" * 64}
    before_calls = len(client.calls)
    try:
        s3_store.store_file(client, path, audit_key)
    except CaptureError as error:
        results["client_index_cannot_reuse_a_cross_scope_reference"] = "crosses its data or evidence scope" in str(error) and len(client.calls) == before_calls
    else:
        results["client_index_cannot_reuse_a_cross_scope_reference"] = False

    racing: Any = ConcurrentS3("bronze_evidence/sha256/" + digest)
    racing.storage_policy = {"enabled": True, "objects": {}, "evidence_objects": {}, "bootstrap_sha256": "0" * 64}
    data_record, _ = s3_store.store_file(racing, path, data_key)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(s3_store.store_file, racing, path, audit_key) for _ in range(2)]
        records = [future.result(timeout=30)[0] for future in futures]
    results["evidence_prefix_has_independent_conditional_create"] = (
        records[0] == records[1]
        and records[0]["key"] == "bronze_evidence/sha256/" + digest
        and data_record["key"] == "bronze_objects/sha256/" + digest
        and len(racing.objects) == 2
        and racing.conflicts >= 1
    )
    return results


class ConcurrentS3(FakeS3):
    """Model atomic conditional creation while two real capture workflows observe the same missing payload."""

    def __init__(self, target: str) -> None:
        super().__init__()
        self.target = target
        self.barrier = threading.Barrier(2)
        self.lock = threading.Lock()
        self.heads = 0
        self.conflicts = 0

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict[str, Any]:
        args = arguments or []
        key = args[args.index("--key") + 1] if "--key" in args else ""
        if operation == "head-object" and key == self.target:
            if not self.lock.acquire(timeout=5):
                raise TimeoutError("synthetic S3 head lock exceeded five seconds")
            try:
                rendezvous = self.heads < 2
                self.heads += 1
            finally:
                self.lock.release()
            if rendezvous:
                failure = None
                try:
                    answer = super().call(service, operation, args)
                except s3_store.StorageError as error:
                    failure = error
                    answer = {}
                self.barrier.wait(timeout=5)
                if failure is not None:
                    raise failure
                return answer
        if operation == "put-object":
            if not self.lock.acquire(timeout=5):
                raise TimeoutError("synthetic S3 create lock exceeded five seconds")
            try:
                if key in self.objects:
                    self.conflicts += 1
                    raise s3_store.StorageError(operation, "PreconditionFailed")
                return super().call(service, operation, args)
            finally:
                self.lock.release()
        return super().call(service, operation, args)


class CrashAfterUpload(FakeS3):
    """Leave a created payload behind while reporting an interrupted upload to the actual capture workflow."""

    def __init__(self, target: str) -> None:
        super().__init__()
        self.target = target
        self.crashed = False

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict[str, Any]:
        args = arguments or []
        result = super().call(service, operation, args)
        if operation == "put-object" and "--key" in args and args[args.index("--key") + 1] == self.target and not self.crashed:
            self.crashed = True
            raise s3_store.StorageError(operation, "request_unconfirmed")
        return result


def capture_workflows(root: Path) -> dict[str, bool]:
    """Run policy checks, capture publication, concurrent creation and crash recovery through upload_snapshot."""
    payload = b"id,value\nsynthetic,one\n"
    digest = hashlib.sha256(payload).hexdigest()
    target = "bronze_objects/sha256/" + digest
    results = {}
    with pytest.MonkeyPatch.context() as patch:
        source, plan, validator = inspect.unwrap(setup_capture)(patch)
        patch.setattr(s3_store, "require_collection_scope", lambda _source_id: None)
        patch.setattr("scripts.acquisition.source_registry.require_collection_scope", lambda _source_id: None)
        registry = {"sources": [source]}
        patch.setattr(
            s3_store,
            "load_routes",
            lambda _registry: {
                source["source_id"]: {
                    "publisher": "example_publisher",
                    "collection": "example_collection",
                    "layout_sha256": "a" * 64,
                }
            },
        )

        def receipt(folder: str, date: str) -> Path:
            chosen = copy.deepcopy(plan)
            chosen["release"]["release_date"] = date
            return capture(chosen, root / folder, registry, validator, opener=Opener(Response(payload)))

        def active(client: Any) -> Any:
            client.storage_policy = {"enabled": True, "objects": {}, "evidence_objects": {}, "bootstrap_sha256": "0" * 64}
            return client

        def upload(path: Path, client: Any) -> dict[str, Any]:
            return s3_store.upload_snapshot(path, [], client, OUTPUTS, registry, validator)

        first, second = receipt("concurrent_first", "2026-01-01"), receipt("concurrent_second", "2026-04-01")
        concurrent = active(ConcurrentS3(target))
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(upload, path, concurrent) for path in (first, second)]
            completed = [future.result(timeout=30) for future in futures]
        manifests = [json.loads(next((path.parent / "s3_collections").glob("*/manifest.json")).read_text()) for path in (first, second)]
        objects = [next(item["object"] for item in manifest["objects"] if item["role"] == "data") for manifest in manifests]
        results["concurrent_captures_share_one_physical_version"] = objects[0] == objects[1] and objects[0]["key"] == target and concurrent.conflicts >= 1
        results["concurrent_captures_keep_both_observations"] = (
            len({manifest["snapshot_id"] for manifest in manifests}) == 2
            and {manifest["release_id"] for manifest in manifests} == {"2026-01-01", "2026-04-01"}
            and len(completed) == 2
        )
        receipts = [
            next(item for item in manifest["objects"] if item["storage_path"] == path.name) for path, manifest in zip((first, second), manifests, strict=True)
        ]
        results["enabled_capture_receipt_keeps_role_bytes_and_lineage_on_evidence_prefix"] = all(
            item["role"] == "capture_receipt"
            and item["object"]["key"] == "bronze_evidence/sha256/" + hashlib.sha256(path.read_bytes()).hexdigest()
            and concurrent.objects[item["object"]["key"]]["body"] == path.read_bytes()
            and manifest["registry_sha256"] == canonical_hash(registry)
            for path, manifest, item in zip((first, second), manifests, receipts, strict=True)
        )
        final_manifests = []
        for path, manifest in zip((first, second), manifests, strict=True):
            local = next((path.parent / "s3_collections").glob("*/manifest.json"))
            reconciliation = json.loads((path.parent / "s3_collections_reconciliation.json").read_text())
            key = reconciliation["manifests"][0]["object"]["key"]
            expected = (
                f"example_publisher/example_collection/manifests/release_date={manifest['release_id']}/"
                f"{manifest['snapshot_id']}/{hashlib.sha256(local.read_bytes()).hexdigest()}/manifest.json"
            )
            final_manifests.append(key == expected and concurrent.objects[key]["body"] == local.read_bytes())
        results["enabled_capture_final_manifest_keeps_approved_publication_path"] = all(final_manifests)
        before = copy.deepcopy(concurrent.objects)
        upload(first, concurrent)
        results["same_operation_retry_keeps_versions_and_manifest"] = (
            before == concurrent.objects and json.loads(next((first.parent / "s3_collections").glob("*/manifest.json")).read_text()) == manifests[0]
        )

        interrupted = receipt("interrupted", "2026-07-01")
        crash = active(CrashAfterUpload(target))
        try:
            upload(interrupted, crash)
        except s3_store.StorageError as error:
            interrupted_at_payload = (
                error.code == "request_unconfirmed" and target in crash.objects and not (interrupted.parent / "s3_collections_reconciliation.json").exists()
            )
        else:
            interrupted_at_payload = False
        version = crash.objects.get(target, {}).get("VersionId")
        upload(interrupted, crash)
        after_retry = json.loads(next((interrupted.parent / "s3_collections").glob("*/manifest.json")).read_text())
        results["crash_after_upload_resumes_same_observation"] = (
            interrupted_at_payload
            and crash.objects[target]["VersionId"] == version
            and after_retry["snapshot_id"] == json.loads(interrupted.read_text())["snapshot_id"]
        )
        reuse = receipt("after_interruption", "2026-10-01")
        upload(reuse, crash)
        after_reuse = json.loads(next((reuse.parent / "s3_collections").glob("*/manifest.json")).read_text())
        before_object = next(item["object"] for item in after_retry["objects"] if item["role"] == "data")
        after_object = next(item["object"] for item in after_reuse["objects"] if item["role"] == "data")
        results["post_crash_new_release_reuses_payload"] = (
            version is not None
            and crash.objects.get(target, {}).get("VersionId") == version
            and before_object == after_object
            and len([record for key, record in crash.objects.items() if key.startswith("bronze_objects/") and record["body"] == payload]) == 1
        )

        disabled_path = receipt("disabled_capture", "2026-09-01")
        disabled = FakeS3()
        upload(disabled_path, disabled)
        local = next((disabled_path.parent / "s3_collections").glob("*/manifest.json"))
        manifest = json.loads(local.read_text())
        stored_receipt = next(item for item in manifest["objects"] if item["storage_path"] == disabled_path.name)
        prefix = f"example_publisher/example_collection/manifests/release_date=2026-09-01/{manifest['snapshot_id']}/"
        legacy_receipt = prefix + hashlib.sha256(disabled_path.read_bytes()).hexdigest() + "/" + disabled_path.name
        legacy_manifest = prefix + hashlib.sha256(local.read_bytes()).hexdigest() + "/manifest.json"
        results["disabled_capture_receipt_and_final_manifest_keep_legacy_paths"] = (
            stored_receipt["role"] == "capture_receipt"
            and stored_receipt["object"]["key"] == legacy_receipt
            and disabled.objects[legacy_receipt]["body"] == disabled_path.read_bytes()
            and disabled.objects[legacy_manifest]["body"] == local.read_bytes()
        )

        held = receipt("held", "2027-01-01")
        source["preferred_route"] = "access_hold"
        held_receipt = json.loads(held.read_text())
        lineage = json.loads(held_receipt["lineage"]["extraction_or_query"])
        lineage["registry_sha256"] = canonical_hash(registry)
        held_receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
        held.write_text(json.dumps(held_receipt))
        before = copy.deepcopy(crash.objects)
        try:
            upload(held, crash)
        except CaptureError as error:
            results["policy_hold_blocks_reuse_before_any_write"] = "access hold" in str(error) and before == crash.objects
        else:
            results["policy_hold_blocks_reuse_before_any_write"] = False
    return results


def bootstrap_workflow(root: Path) -> dict[str, bool]:
    """Bootstrap a verified legacy object through metadata export and exercise immutable acquisition reuse."""
    path = root / "bootstrap.csv"
    path.write_bytes(b"synthetic\nlegacy\n")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    client: Any = FakeS3()
    old_key = f"cms/bootstrap/datasets/one/release_date=2026-01-01/{digest}/bootstrap.csv"
    record, _ = s3_store.store_file(client, path, old_key)
    manifest = {
        "snapshot_id": "first",
        "source_id": "synthetic",
        "release_id": "2026-01-01",
        "objects": [{"role": "data", "dataset_id": "one", "object": record}],
    }
    manifest_key = "cms/bootstrap/manifests/first/manifest.json"
    storage = ManifestStorage({(manifest_key, "v1"): json.dumps(manifest).encode()})
    table = {"table": "bootstrap_files", "collection": "cms/bootstrap", "member_pattern": r"\.csv$", "format": "csv"}
    table_path = root / "bootstrap_tables.json"
    table_path.write_text(json.dumps({"tables": [table]}))
    config = bronze.load_table_map(table_path)
    inputs, excluded = bronze.discover(storage, client.configuration["data_bucket_name"], config, ["bootstrap_files"])
    generation = bronze_metadata.build(["bootstrap_files"], inputs, excluded, [], ["bootstrap_files"])
    index = root / "bootstrap_index.json"
    index_sha = bronze_metadata.write_bootstrap(generation, index)
    policy = root / "storage_policy.json"
    policy.write_text(json.dumps({"single_copy_enabled": True, "bootstrap_index": str(index), "bootstrap_index_sha256": index_sha}))
    client.storage_policy = single_copy.load_policy(policy)
    before = copy.deepcopy(client.objects)
    new_key = f"cms/bootstrap/datasets/one/release_date=2026-04-01/{digest}/renamed.csv"
    reused, created = s3_store.store_file(client, path, new_key)
    results = {"bootstrap_reuses_exact_legacy_key_and_version": not created and reused == record and before == client.objects}
    client.overrides["get-object"] = s3_store.StorageError("get-object", "NoSuchVersion")
    try:
        s3_store.store_file(client, path, new_key)
    except s3_store.StorageError as error:
        results["missing_indexed_version_has_no_upload_fallback"] = error.code == "NoSuchVersion" and before == client.objects
    else:
        results["missing_indexed_version_has_no_upload_fallback"] = False
    del client.overrides["get-object"]
    client.corrupt = True
    try:
        s3_store.store_file(client, path, new_key)
    except CaptureError as error:
        results["corrupt_indexed_bytes_are_refused"] = "byte hash or length" in str(error) and before == client.objects
    else:
        results["corrupt_indexed_bytes_are_refused"] = False
    client.corrupt = False
    index.write_bytes(index.read_bytes() + b" ")
    try:
        single_copy.load_policy(policy)
    except CaptureError as error:
        results["bootstrap_hash_drift_is_refused"] = "approved SHA-256" in str(error)
    else:
        results["bootstrap_hash_drift_is_refused"] = False
    results["committed_single_copy_setting_is_off"] = not single_copy.load_policy()["enabled"]
    return results


def bootstrap_scope(root: Path) -> dict[str, bool]:
    """Export valid data references deterministically and reject complete generations with non-data object keys."""
    item = synthetic_metadata_item("bootstrap_scope")
    digest = item["sha256"]
    item["key"] = f"cms/scope/datasets/one/release_date=2026-01-01/{digest}/payload.csv"
    rows = bronze_metadata.build([item["table"]], [item], [], [], [item["table"]])
    content = bronze_metadata.bootstrap_bytes(rows)
    document = json.loads(content)
    results = {
        "bootstrap_data_export_preserves_exact_identity": document["objects"]
        == {digest: {"bucket": item["bucket"], "key": item["key"], "version_id": "v1", "sha256": digest, "byte_count": 1}}
        and document["evidence_objects"] == {},
        "bootstrap_export_is_independent_of_row_order": content == bronze_metadata.bootstrap_bytes(list(reversed(rows))),
    }
    for category in ("audit", "manifests"):
        changed = {**item, "key": f"cms/scope/{category}/capture/{digest}/payload.csv"}
        generation = bronze_metadata.build([item["table"]], [changed], [], [], [item["table"]])
        bronze_metadata.validate(generation)
        output = root / f"refused_{category}_bootstrap.json"
        try:
            bronze_metadata.write_bootstrap(generation, output)
        except bronze_metadata.MetadataError as error:
            results[f"bootstrap_refuses_{category}_object_before_artifact"] = "data-object scope" in str(error) and not output.exists()
        else:
            results[f"bootstrap_refuses_{category}_object_before_artifact"] = False
    return results


def manifest_exemptions(root: Path) -> dict[str, bool]:
    """Preserve exact category-specific manifest names and refuse payload exemptions before storage."""
    path = root / "manifest_payload.csv"
    path.write_bytes(b"synthetic\npayload\n")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    results = {}
    for category, flag in (("manifests", False), ("audit", True)):
        policy = {"enabled": True, "objects": {}, "evidence_objects": {}, "bootstrap_sha256": "0" * 64}
        names: tuple[str, ...] = (
            "payload.csv",
            "Manifest.json",
            "manifest.json.backup",
            "Retired_versions.json",
            "retired_versions.json.backup",
            "Redaction_manifest.json",
            "redaction_manifest.json.backup",
        )
        if category == "audit":
            names += ("retired_versions.json", "redaction_manifest.json")
        for name in names:
            client: Any = FakeS3()
            client.storage_policy = policy
            key = f"cms/exemption/{category}/capture/{digest}/{name}"
            try:
                s3_store.store_file(client, path, key, manifest=flag)
            except CaptureError as error:
                results[f"{category}_{name}_exemption_refused_before_storage"] = "manifest.json" in str(error) and not client.calls
            else:
                results[f"{category}_{name}_exemption_refused_before_storage"] = False
            disabled: Any = FakeS3()
            record, created = s3_store.store_file(disabled, path, key, manifest=flag)
            results[f"{category}_{name}_setting_off_preserved"] = created and record["key"] == key
        valid: Any = FakeS3()
        valid.storage_policy = policy
        key = f"cms/exemption/{category}/capture/{digest}/manifest.json"
        first, created = s3_store.store_file(valid, path, key, manifest=flag)
        replay, created_again = s3_store.store_file(valid, path, key, manifest=flag)
        results[f"{category}_valid_manifest_keeps_path_and_retry"] = first == replay and first["key"] == key and created and not created_again
    content = b'{"kind":"synthetic_manifest_compatibility"}\n'
    record_digest = hashlib.sha256(content).hexdigest()
    for name, partition in (("retired_versions.json", "retirements"), ("redaction_manifest.json", "release_date=2026-01-01")):
        local = root / name
        local.write_bytes(content)
        key = f"cms/exemption/manifests/{partition}/{record_digest}/{name}"
        for flag in (False, True):
            label = f"manifests_{name}_manifest_{flag}"
            accepted: Any = FakeS3()
            accepted.storage_policy = policy
            try:
                first, created = s3_store.store_file(accepted, local, key, manifest=flag)
                results[f"{label}_creation_keeps_exact_key_bytes_and_version"] = (
                    created
                    and first["key"] == key
                    and first["version_id"] == accepted.objects[key]["VersionId"]
                    and first["sha256"] == record_digest
                    and first["byte_count"] == len(content)
                    and accepted.objects[key]["body"] == content
                    and set(accepted.objects) == {key}
                )
                stored_before_retry = copy.deepcopy(accepted.objects)
                replay, created_again = s3_store.store_file(accepted, local, key, manifest=flag)
                results[f"{label}_retry_keeps_immutable_record_and_bytes"] = first == replay and not created_again and accepted.objects == stored_before_retry
            except CaptureError as error:
                sys.stderr.write(json.dumps({"key": key, "manifest": flag, "refusal": str(error), "storage_calls": len(accepted.calls)}) + "\n")
                results[f"{label}_creation_keeps_exact_key_bytes_and_version"] = False
                results[f"{label}_retry_keeps_immutable_record_and_bytes"] = False
            disabled = FakeS3()
            record, created = s3_store.store_file(disabled, local, key, manifest=flag)
            results[f"{label}_setting_off_keeps_exact_key_and_bytes"] = created and record["key"] == key and disabled.objects[key]["body"] == content
    return results


def retirement_transport(root: Path) -> dict[str, bool]:
    """Carry real retirement-file evidence through discovery and metadata, refusing lossy mappings and stale entries."""
    collection, name = "cms/retirement", "retirement_files"
    obj = {"bucket": "synthetic", "key": collection + "/data.csv", "version_id": "v1", "sha256": "a" * 64, "byte_count": 1}
    manifest: dict[str, Any] = {"snapshot_id": "capture", "source_id": "synthetic", "release_id": "2026-01-01", "objects": [{"role": "data", "object": obj}]}
    key = collection + "/manifests/old/manifest.json"
    raw = json.dumps(manifest).encode()
    storage = ManifestStorage({(key, "v1"): raw})
    config = {"tables": [{"table": name, "collection": collection, "member_pattern": r"\.csv$", "format": "csv", "roles": ["data"], "encodings": ["utf-8"]}]}
    record = {"key": obj["key"], "version_id": "v1", "sha256": obj["sha256"], "reason": "synthetic retirement", "receipt": "synthetic receipt"}
    path = root / "retirement_transport.json"
    path.write_text(json.dumps({"objects": [record]}))
    retired = bronze.load_retired(path)
    found, excluded = bronze.discover(storage, "synthetic", config, [name], retired)
    rows = bronze_metadata.build([name], found, excluded, [], [name])
    results = {
        "retirement_file_audit_reaches_metadata": not found
        and len(rows) == 1
        and rows[0]["retired"]
        and json.loads(rows[0]["retirement_record_json"]) == record,
        "missing_retirement_file_preserves_unretired_discovery": len(
            bronze.discover(storage, "synthetic", config, [name], bronze.load_retired(root / "absent_retirements.json"))[0]
        )
        == 1,
    }
    lossy: Any = dict(retired.checksums)
    try:
        bronze.discover(storage, "synthetic", config, [name], lossy)
    except bronze.BronzeError as error:
        results["copied_retirement_mapping_fails_before_discovery"] = "retirement evidence" in str(error)
    else:
        results["copied_retirement_mapping_fails_before_discovery"] = False
    for label, entries, message in (
        ("malformed", [{**record, "sha256": "bad"}], "64 hex"),
        ("duplicate", [record, record], "repeats"),
        ("checksum", [{**record, "sha256": "b" * 64}], "SHA-256 differs"),
        ("version", [{**record, "version_id": "other"}], "matches no manifest object"),
        ("stale", [{**record, "key": collection + "/absent.csv"}], "matches no manifest object"),
    ):
        path.write_text(json.dumps({"objects": entries}))
        try:
            bronze.discover(storage, "synthetic", config, [name], bronze.load_retired(path))
        except bronze.BronzeError as error:
            results[f"retirement_{label}_fails_for_intended_reason"] = message in str(error)
        else:
            results[f"retirement_{label}_fails_for_intended_reason"] = False
    replacement = copy.deepcopy(manifest)
    replacement["objects"][0]["role"] = "reference"
    replacement.update(
        supersedes={"key": key, "version_id": "v1", "sha256": hashlib.sha256(raw).hexdigest()},
        correction={"changes": [{"key": obj["key"], "version_id": "v1", "from_role": "data", "to_role": "reference"}]},
    )
    storage.objects[(collection + "/manifests/new/manifest.json", "v2")] = json.dumps(replacement).encode()
    found, excluded = bronze.discover(storage, "synthetic", config, [name], retired)
    results["superseded_retired_entry_still_matches"] = not found and any(entry.get("retired") for entry in excluded)
    return results


def release_copy_contract() -> dict[str, bool]:
    """Execute the actual dbt physical-copy test against independently specified synthetic physical states."""
    root = Path(__file__).resolve().parents[2]
    macro = (root / "dbt/macros/gold_facts.sql").read_text()
    start = macro.index("{% test release_copy_consistency(model) %}") + len("{% test release_copy_consistency(model) %}")
    text = macro[start : macro.index("{% endtest %}", start)]
    environment = Environment(undefined=StrictUndefined, autoescape=select_autoescape(default_for_string=False))
    tested = environment.from_string(text).render(model="selected_releases", ref=lambda _name: "copy_rows")
    row = "('synthetic', 'key_one', 'version_one', true, false, 1, 2, '2026-01-01')"
    columns = "member_sha256, s3_key, s3_version_id, is_canonical, is_retired, checksum_count, row_count, publication_release"
    prefix = "with selected_releases(member_sha256) as (values ('synthetic')), copy_rows({columns}) as ({rows}) "
    count_sql = "{prefix} select count(*) as failures from ({tested}) as violations"
    legacy = "select member_sha256 from copy_rows group by member_sha256 having count(distinct publication_release) > 1"
    zero_rows = "select * from (values {row}) where false"
    samples = (
        ("one_live_file_passes", "values " + row, 0),
        ("same_label_two_live_files_fail", "values " + row + ", " + row.replace("key_one", "key_two").replace("true", "false"), 1),
        ("zero_live_files_fail", zero_rows.format(row=row), 1),
        ("missing_version_fails", "values " + row.replace("'version_one'", "null"), 1),
        ("empty_version_fails", "values " + row.replace("'version_one'", "''"), 1),
        ("checksum_disagreement_fails", "values " + row.replace(", 1, 2,", ", 2, 2,"), 1),
        ("retired_version_fails", "values " + row.replace("true, false", "true, true"), 1),
    )
    results = {}
    for name, rows, failures in samples:
        setup = prefix.format(columns=columns, rows=rows)
        sql = count_sql.format(prefix=setup, tested=tested)
        output = run_command(str(duckdb_binary()), [":memory:", "-json", "-init", "/dev/null", "-c", sql], timeout=30, check=True)
        results[name] = json.loads(output.stdout)[0]["failures"] == failures
        if name == "same_label_two_live_files_fail":
            mutant = run_command(
                str(duckdb_binary()),
                [":memory:", "-json", "-init", "/dev/null", "-c", count_sql.format(prefix=setup, tested=legacy)],
                timeout=30,
                check=True,
            )
            results["failing_first_label_only_test_misses_physical_duplicate"] = json.loads(mutant.stdout)[0]["failures"] == 0
    return results


def synthetic_metadata_item(name: str) -> dict[str, Any]:
    """Return a complete synthetic manifest-entry input for metadata publication."""
    return dict(
        table=name,
        observation_id=name,
        object_key=name,
        sha256=hashlib.sha256(name.encode()).hexdigest(),
        bucket="synthetic",
        key=name,
        version_id="v1",
        source_id="synthetic",
        snapshot_id="capture",
        dataset_id=name,
        release_id="2026-01-01",
        release_partition="2026-01-01",
        manifest_key=name + "/manifest.json",
        manifest_version_id="v1",
        manifest_sha256="0" * 64,
        member_chain=[name + ".csv"],
        file_name=name + ".csv",
        byte_count=1,
    )


def metadata_scope() -> dict[str, bool]:
    """Reject partial first bootstraps and retain complete untouched-table metadata on later reloads."""
    first, second = synthetic_metadata_item("first"), synthetic_metadata_item("second")
    try:
        bronze_metadata.build(["first"], [first], [], [], all_tables=["first", "second"])
    except bronze_metadata.MetadataError:
        refused_partial = True
    else:
        refused_partial = False
    original = bronze_metadata.build(["first", "second"], [first, second], [], [], all_tables=["first", "second"])
    try:
        bronze_metadata.build(["first", "second"], [first], [], [], all_tables=["first", "second"])
    except bronze_metadata.MetadataError:
        missing_table_refused = True
    else:
        missing_table_refused = False
    alias = {**first, "observation_id": "first-new-observation", "manifest_key": "first/new-manifest.json", "release_id": "2026-04-01"}
    updated = bronze_metadata.build(["first"], [first], [{"copy": alias}], original, all_tables=["first", "second"])
    envelope = {"generation", "generation_row_count", "generation_object_count"}
    untouched_before = [{key: value for key, value in row.items() if key not in envelope} for row in original if row["table_name"] == "second"]
    untouched_after = [{key: value for key, value in row.items() if key not in envelope} for row in updated if row["table_name"] == "second"]
    try:
        bronze_metadata.validate_successor(original, [row for row in updated if row["table_name"] != "second"])
    except bronze_metadata.MetadataError:
        refuses_loss = True
    else:
        refuses_loss = False
    committed = [row["table"] for row in bronze.load_table_map()["tables"]]
    complete = bronze_metadata.build(committed, [synthetic_metadata_item(name) for name in committed], [], [], committed)
    return {
        "first_one_group_metadata_publication_refused": refused_partial,
        "full_map_metadata_bootstrap_succeeds": {row["table_name"] for row in original} == {"first", "second"},
        "full_map_selection_missing_table_is_refused": missing_table_refused,
        "later_group_reload_preserves_untouched_table_bytes": original[0]["generation"] != updated[0]["generation"]
        and json.dumps(untouched_before, sort_keys=True) == json.dumps(untouched_after, sort_keys=True),
        "generation_that_loses_table_fails_closed": refuses_loss,
        "backing_metadata_is_in_pin_set": "file_metadata" in pins.mapped_tables(),
        "backing_generation_covers_full_committed_map": {row["table_name"] for row in complete if row["kind"] == "observation"} == set(committed),
        "physical_index_covers_full_committed_map": {row["table_name"] for row in complete if row["kind"] == "object"} == set(committed),
    }


def metadata_retirements() -> dict[str, bool]:
    """Exercise observation-only checksums and reject unaccounted or multiply live physical versions."""
    seed = synthetic_metadata_item("retirements")
    empty = hashlib.sha256(b"").hexdigest()
    failed = [
        {
            **seed,
            "observation_id": f"failed-{i}",
            "object_key": f"failed-{i}",
            "key": f"failed-{i}",
            "sha256": empty,
            "byte_count": 0,
            "retired": True,
            "failed_attempt": True,
            "manifest_entry_json": json.dumps({"receipt": f"receipt-{i}"}),
        }
        for i in range(5)
    ]

    def accepted(inputs: list[dict[str, Any]], excluded: list[dict[str, Any]], previous: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = bronze_metadata.build([seed["table"]], inputs, [{"copy": row} for row in excluded], previous, [seed["table"]])
        bronze_metadata.validate_successor(previous, rows)
        return rows

    results = {}
    try:
        rows = accepted([], failed, [])
        results["five_retired_empty_failed_attempts_are_observation_only"] = (
            len(rows) == 5
            and {row["kind"] for row in rows} == {"observation"}
            and all(json.loads(row["manifest_entry_json"])["receipt"].startswith("receipt-") for row in rows)
        )
    except (bronze_metadata.MetadataError, KeyError):
        results["five_retired_empty_failed_attempts_are_observation_only"] = False
    alias = {**seed, "observation_id": "redacted-alias", "object_key": "redacted-alias", "manifest_entry_json": '{"redaction_reference":"synthetic"}'}
    original = accepted([seed], [alias], [])
    try:
        retired = accepted([], [{**row, "retired": True} for row in (seed, alias)], original)
        results["aliased_then_retired_redaction_keeps_observations"] = (
            len(retired) == 2
            and all(row["kind"] == "observation" and row["retired"] for row in retired)
            and any(row["manifest_entry_json"] == alias["manifest_entry_json"] for row in retired)
        )
    except (bronze_metadata.MetadataError, KeyError):
        results["aliased_then_retired_redaction_keeps_observations"] = False
    for name, changed in (
        ("two_live_versions_still_refused", {**alias, "version_id": "v2"}),
        ("unaccounted_observation_without_live_object_refused", seed),
    ):
        try:
            accepted([seed] if "two_live" in name else [], [changed], [])
        except bronze_metadata.MetadataError as error:
            results[name] = "live" in str(error) or "physical" in str(error)
        else:
            results[name] = False
    return results


def pre_load_successor_guard(root: Path) -> bool:
    """Drive bronze.main with synthetic adapters; a resurrected observation must stop before any data mutation."""
    first = synthetic_metadata_item("preload")
    retired = {**first, "observation_id": "retired-alias", "object_key": "retired-alias", "retired": True}
    previous = bronze_metadata.build(["preload"], [first], [{"copy": retired}], [], ["preload"])
    deployment = root / "synthetic_deployment.json"
    deployment.write_text('{"data_bucket_name":"synthetic"}')
    mutations = []
    validator = bronze_metadata.validate_successor
    from scripts.lakehouse import dictionary

    def publish(_spark: Any, _namespace: str, rows: list[dict[str, Any]]) -> str:
        validator(previous, rows)
        raise AssertionError("resurrection must be refused")

    def mutate(*_args: Any) -> dict[str, Any]:
        mutations.append("data")
        return {}

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(sys, "argv", ["bronze", "--tables", "preload"])
        patch.setattr(bronze, "DEPLOYMENT", deployment)
        patch.setattr(bronze, "load_table_map", lambda: {"namespace": "synthetic", "tables": [{"table": "preload"}]})
        patch.setattr(bronze, "S3Storage", lambda: None)
        session = SimpleNamespace(spark_session=lambda *_args, **_kwargs: SimpleNamespace(stop=lambda: None), job_memory=lambda: "synthetic")
        patch.setitem(sys.modules, "scripts.lakehouse.session", session)
        patch.setattr(bronze, "load_retired", bronze.RetiredObjects)
        patch.setattr(bronze, "discover", lambda *_args: ([first], [{"copy": {**retired, "retired": False}}]))
        patch.setattr(bronze_metadata, "read", lambda *_args: previous)
        patch.setattr(bronze, "load_objects", mutate)
        patch.setattr(bronze, "prune", mutate)
        patch.setattr(bronze, "write_copies", lambda *_args: 0)
        patch.setattr(bronze, "verify", lambda *_args: {"preload": {"passed": True}})
        patch.setattr(dictionary, "build", lambda *_args: {})
        patch.setattr(bronze_metadata, "publish", publish)
        code = bronze.main()
    return code == 1 and not mutations


def fixture_generation(root: Path) -> bool:
    """Persist the real fixture SQL and require exactly the builder's complete alias/retirement generation."""
    from scripts.lakehouse import run_staging_e2e as staging

    root = root / "fixture_generation"
    root.mkdir()
    files = {
        "fixture": staging.fixture_csv(staging.BASE),
        "copies": staging.copies_csv(staging.BASE),
        "column_map": staging.column_map_csv(staging.BASE),
        "file_preambles": staging.file_preambles_csv(staging.BASE),
    }
    files.update({table: staging.wide_csv(staging.BASE, table) for table in staging.WIDE_COLUMNS})
    variables = []
    for name, content in files.items():
        path = root / (name + ".csv")
        path.write_text(content)
        variables.append(f"SET VARIABLE {name}_csv = '{path}';")
    rows = staging.fixture_metadata(staging.BASE)
    metadata = root / "metadata.json"
    metadata.write_text(json.dumps(rows))
    variables.append(f"SET VARIABLE metadata_json = '{metadata}';")
    query = "{variables}\n{fixture}\nselect * from bronze.file_metadata;"
    sql = query.format(variables="\n".join(variables), fixture=staging.FIXTURE_SQL)
    result = run_command(str(duckdb_binary()), [":memory:", "-json", "-init", "/dev/null", "-c", sql], timeout=30, check=True)
    stored = json.loads(result.stdout)
    bronze_metadata.validate(stored)
    return (
        sorted(json.dumps(row, sort_keys=True) for row in stored) == sorted(json.dumps(row, sort_keys=True) for row in rows)
        and sum(row["observation_id"] == "synthetic-alias" for row in stored) == 1
        and sum(row["observation_id"] == "synthetic-retired" and row["retired"] for row in stored) == 1
    )


def observation_only_sql_guard(root: Path) -> dict[str, bool]:
    """The actual dbt generation gate accepts retired history and refuses a missing live-object kind."""
    item = {**synthetic_metadata_item("history"), "retired": True}
    rows = bronze_metadata.build(["history"], [], [{"copy": item}], [], ["history"])
    metadata = root / "history.json"
    metadata.write_text(json.dumps(rows))
    macro = (Path(__file__).resolve().parents[2] / "dbt/macros/file_metadata.sql").read_text()
    template = Environment(autoescape=select_autoescape(default_for_string=False)).from_string(macro + "{{ assert_file_metadata_generation() }}")
    source = "read_json_auto('{path}')"
    context = {
        "target": SimpleNamespace(name="fixture"),
        "env_var": lambda _key, default="": default,
        "fromjson": json.loads,
        "source": lambda *_args: source.format(path=metadata),
    }
    query = template.render(**context)
    accepted = run_command(str(duckdb_binary()), [":memory:", "-json", "-init", "/dev/null", "-c", query], timeout=30, check=True)
    rows[0]["retired"] = False
    metadata.write_text(json.dumps(rows))
    refused = run_command(str(duckdb_binary()), [":memory:", "-json", "-init", "/dev/null", "-c", query], timeout=30)
    return {
        "dbt_gate_accepts_observation_only_retired_generation": json.loads(accepted.stdout)[0]["is_complete"] is True,
        "dbt_gate_refuses_missing_live_object_kind": refused.returncode != 0 and "incomplete generation or is missing" in refused.stderr,
    }


def null_generation_sql_guard(root: Path) -> dict[str, bool]:
    """The actual dbt generation gate accepts the builder's complete generation and refuses a NULL in any of six fields of its second observation."""
    live = synthetic_metadata_item("live")
    alias = {**live, "observation_id": "live-alias", "manifest_key": "live/alias-manifest.json"}
    rows = bronze_metadata.build(["live"], [live], [{"copy": alias}], [], ["live"])
    bronze_metadata.validate(rows)
    metadata = root / "null_guard.json"
    macro = (Path(__file__).resolve().parents[2] / "dbt/macros/file_metadata.sql").read_text()
    template = Environment(autoescape=select_autoescape(default_for_string=False)).from_string(macro + "{{ assert_file_metadata_generation() }}")
    context = {
        "target": SimpleNamespace(name="fixture"),
        "env_var": lambda _key, default="": default,
        "fromjson": json.loads,
        "source": lambda *_args: f"read_json_auto('{metadata}')",
    }
    query = template.render(**context)

    def gate(field: str | None) -> Any:
        changed = copy.deepcopy(rows)
        if field is not None:
            changed[2][field] = None
        metadata.write_text(json.dumps(changed))
        return run_command(str(duckdb_binary()), [":memory:", "-json", "-init", "/dev/null", "-c", query], timeout=30)

    accepted = gate(None)
    results = {"dbt_gate_accepts_one_complete_generation": accepted.returncode == 0 and json.loads(accepted.stdout)[0]["is_complete"] is True}
    for field in ("kind", "generation", "retired", "failed_attempt", "generation_row_count", "generation_object_count"):
        refused = gate(field)
        results[f"dbt_gate_refuses_null_{field}"] = refused.returncode != 0 and "incomplete generation or is missing" in refused.stderr
    return results


def publication_date_parser() -> bool:
    """Render the actual dimension over each supported label syntax and require stored parser results."""
    samples = (
        ("plain.csv", "2026-04-01", "2026-04-01"),
        ("plain.csv", "2026-4-1", None),
        ("plain.csv", "20260401", None),
        ("2026-02-30.zip!plain.csv", "2026-01-01", None),
        ("2026-01-01.zip!2026-04-01.zip!plain.csv", "capture", "2026-04-01"),
        ("plain.csv", "source__20260401T000000Z__" + "a" * 32, None),
    )
    rows, expected = [], {}
    for index, (member, label, day) in enumerate(samples):
        parsed = bronze_metadata.release_label(member, label)
        if parsed["publication_date"] != day:
            return False
        literal = "null" if parsed["publication_date"] is None else "DATE '" + str(parsed["publication_date"]) + "'"
        rows.append(f"('checksum-{index}', 'key-{index}', '{parsed['publication_release']}', {literal})")
        expected["checksum-" + str(index)] = day
    model = (Path(__file__).resolve().parents[2] / "dbt/models/gold/dimensions/dim_release.sql").read_text()
    rendered = Environment(autoescape=select_autoescape(default_for_string=False)).from_string(model).render(ref=lambda name: name)
    prefix = """with stg_bronze__file_copies(member_sha256, object_key, publication_release, publication_date) as (values {rows}),
    stg_bronze__files as (select member_sha256, []::DATE[] as publication_dates, []::VARCHAR[] as download_run_ids from stg_bronze__file_copies),
    geography_file_periods as (select null::VARCHAR as member_sha256, null::DATE as period_start, null::DATE as period_end where false),
    pos_file_periods as (select * from geography_file_periods), ownership_release_periods as (select * from geography_file_periods),
    rendered as ({model}) select member_sha256, release_date::VARCHAR as release_date from rendered where member_sha256 != '<unknown>'"""
    result = run_command(
        str(duckdb_binary()), [":memory:", "-json", "-init", "/dev/null", "-c", prefix.format(rows=",".join(rows), model=rendered)], timeout=30, check=True
    )
    return {row["member_sha256"]: row["release_date"] for row in json.loads(result.stdout)} == expected


def failed_attempt_publications(root: Path) -> dict[str, bool]:
    """Keep failed receipts in the ledger while actual reader SQL excludes their publication labels and dates."""
    project = Path(__file__).resolve().parents[2]
    variables = __import__("yaml").safe_load((project / "dbt/dbt_project.yml").read_text())["vars"]
    table = next(iter(variables["copy_tables"]))
    live = synthetic_metadata_item(table)
    retired = {**live, "observation_id": "retired", "object_key": "retired", "release_id": "2026-04-01", "retired": True}
    failed = {**live, "observation_id": "failed", "object_key": "failed", "release_id": "2026-10-01", "failed_attempt": True}
    failed_only = {**failed, "observation_id": "failed-only", "object_key": "failed-only", "sha256": "f" * 64}
    rows = bronze_metadata.build([table], [live], [{"copy": item} for item in (retired, failed, failed_only)], [], [table])
    metadata = root / "failed_attempt_publications.json"
    metadata.write_text(json.dumps(rows))
    environment = Environment(undefined=StrictUndefined, autoescape=select_autoescape(default_for_string=False))
    fragments = [
        storage_metadata_comparison.INPUT_SQL.format(path=str(metadata).replace("'", "''")),
        "stg_bronze__metadata as (select * from metadata_input)",
        "stg_bronze__file_labels as (select null::VARCHAR as bronze_table, null::VARCHAR as member_sha256, "
        "null::VARCHAR as role, null::VARCHAR as identity_year where false)",
        "staging_label_holds as (select null::VARCHAR as bronze_table, null::VARCHAR as member_sha256, "
        "null::VARCHAR as issue, null::VARCHAR as reason where false)",
        "geography_file_periods as (select null::VARCHAR as member_sha256, null::DATE as period_start, null::DATE as period_end where false)",
        "pos_file_periods as (select * from geography_file_periods)",
        "ownership_release_periods as (select * from geography_file_periods)",
    ]
    for name in ("stg_bronze__release_observations", "stg_bronze__physical_object_index", "stg_bronze__file_copies", "stg_bronze__files", "dim_release"):
        folder = "gold/dimensions" if name == "dim_release" else "silver/staging"
        text = (project / f"dbt/models/{folder}/{name}.sql").read_text()
        if name == "stg_bronze__file_copies":
            loaded_template = (
                "with loaded as (select table_name as bronze_table, _object_key as object_key, 1 as checksum_count, "
                "1 as row_count from metadata_input where kind = 'object'),\n{sql}"
            )
            text = loaded_template.format(sql=text[text.index("copies as (") :])
        rendered = environment.from_string(text).render(config=lambda **_kwargs: "", ref=lambda name: name, var=lambda name: variables[name])
        fragments.append(name + " as (" + rendered + ")")
    query = """with {fragments}
    select files.publication_releases, files.publication_dates, files.latest_publication_date,
        release.publication_dates as dimension_dates, release.release_date,
        (select count(*) from stg_bronze__release_observations where is_failed_attempt) as failed_ledger_count,
        (select count(*) from stg_bronze__files) as files_count,
        (select count(*) from dim_release where member_sha256 != '<unknown>') as dimension_count
    from stg_bronze__files as files inner join dim_release as release using (member_sha256)
    """
    sql = query.format(fragments=",\n".join(fragments))
    output = run_command(str(duckdb_binary()), [":memory:", "-json", "-init", "/dev/null", "-c", sql], timeout=30, check=True)
    observed = json.loads(output.stdout)[0]
    dates = ["2026-01-01", "2026-04-01"]
    return {
        "failed_attempt_labels_excluded_but_retired_labels_kept": observed["publication_releases"] == dates,
        "failed_attempt_dates_excluded_from_files_and_dimension": observed["publication_dates"] == dates and observed["dimension_dates"] == dates,
        "failed_attempt_cannot_advance_latest_publication_date": observed["latest_publication_date"] == "2026-04-01",
        "failed_attempt_audit_rows_retained_without_dimension_release": observed["failed_ledger_count"] == 2
        and observed["files_count"] == observed["dimension_count"] == 1
        and observed["release_date"] == "2026-01-01",
    }


def metadata_supersession(root: Path) -> dict[str, bool]:
    """Discover validated corrections, retain old labels and transfer only an unchanged canonical physical reference."""
    collection, name = "cms/supersession", "supersession_files"
    table = {"table": name, "collection": collection, "member_pattern": r"\.csv$", "format": "csv"}
    path = root / "supersession_tables.json"
    path.write_text(json.dumps({"tables": [table]}))
    config = bronze.load_table_map(path)
    obj: dict[str, Any] = {"bucket": "synthetic", "key": collection + "/data.csv", "version_id": "v1", "sha256": "a" * 64, "byte_count": 1}
    original: dict[str, Any] = {
        "snapshot_id": "same-capture",
        "source_id": "synthetic",
        "release_id": "2026-01-01",
        "objects": [
            {"role": "data", "dataset_id": "one", "object": obj},
            {"role": "data", "dataset_id": "unmatched", "object": {**obj, "key": collection + "/unused.txt"}},
        ],
    }
    key, version = collection + "/manifests/old/manifest.json", "v1"
    raw = json.dumps(original).encode()
    storage = ManifestStorage({(key, version): raw})
    inputs, excluded = bronze.discover(storage, "synthetic", config, [name])
    before = bronze_metadata.build([name], inputs, excluded, [], [name])
    frozen_keys = {(name, obj["sha256"]): inputs[0]["object_key"]}
    frozen_ids = {(name, obj["sha256"]): inputs[0]["observation_id"]}
    successor_key = collection + "/manifests/new/manifest.json"

    def corrected(role: str = "data", changes: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        new = copy.deepcopy(original)
        new["supersedes"] = {"key": key, "version_id": version, "sha256": hashlib.sha256(raw).hexdigest()}
        new["correction"] = {"changes": [{"key": obj["key"], "version_id": "v1", "from_role": "data", "to_role": role}]}
        new["objects"][0]["role"] = role
        new["objects"][0]["object"].update(changes or {})
        storage.objects[(successor_key, "v2")] = json.dumps(new).encode()
        found, dropped = bronze.discover(
            storage,
            "synthetic",
            config,
            [name],
            canonical_keys=frozen_keys,
            canonical_observations=frozen_ids,
            published_observations={row["observation_id"] for row in before if row["kind"] == "observation"},
        )
        rows = bronze_metadata.build([name], found, dropped, before, [name])
        bronze_metadata.validate_successor(before, rows)
        return rows

    results = {}
    try:
        rows = corrected()
        old = next(row for row in rows if row["kind"] == "observation" and row["observation_id"] == inputs[0]["observation_id"])
        physical = next(row for row in rows if row["kind"] == "object")
        results["supersession_keeps_and_marks_published_label_history"] = (
            sum(row["kind"] == "observation" for row in rows) == 2
            and old["publication_date"] == "2026-01-01"
            and old["superseded_by_manifest_key"] == successor_key
            and old["superseded_by_manifest_version_id"] == "v2"
            and not old["loaded"]
        )
        results["superseded_frozen_canonical_transfers_only_observation_id"] = (
            physical["_object_key"] == inputs[0]["object_key"]
            and physical["s3_version_id"] == "v1"
            and physical["sha256"] == obj["sha256"]
            and physical["publication_date"] == "2026-01-01"
            and physical["observation_id"] != inputs[0]["observation_id"]
            and physical["canonical_previous_observation_id"] == inputs[0]["observation_id"]
            and physical["canonical_superseding_manifest_key"] == successor_key
            and physical["canonical_superseding_manifest_version_id"] == "v2"
        )
        found, dropped = bronze.discover(
            storage,
            "synthetic",
            config,
            [name],
            canonical_keys=frozen_keys,
            canonical_observations={(name, obj["sha256"]): physical["observation_id"]},
            published_observations={row["observation_id"] for row in rows if row["kind"] == "observation"},
        )
        replay = bronze_metadata.build([name], found, dropped, rows, [name])
        bronze_metadata.validate_successor(rows, replay)
        results["supersession_reload_keeps_audit_and_generation"] = replay == rows
        reports = [entry for entry in dropped if "copy" not in entry]
        results["superseded_unmatched_entry_reported_once"] = reports == excluded and len(reports) == 1
    except (bronze.BronzeError, bronze_metadata.MetadataError, KeyError):
        results["supersession_keeps_and_marks_published_label_history"] = False
        results["superseded_frozen_canonical_transfers_only_observation_id"] = False
        results["supersession_reload_keeps_audit_and_generation"] = False
        results["superseded_unmatched_entry_reported_once"] = False
    for label, role, changes in (("checksum", "data", {"sha256": "b" * 64}), ("version", "data", {"version_id": "v3"}), ("role", "reference", {})):
        try:
            corrected(role, changes)
        except (bronze.BronzeError, bronze_metadata.MetadataError) as error:
            results[f"canonical_transfer_changed_{label}_refused"] = (
                "beyond the named roles" in str(error) if label != "role" else name in str(error) and inputs[0]["object_key"] in str(error)
            )
        else:
            results[f"canonical_transfer_changed_{label}_refused"] = False
    try:
        bronze_metadata.build([name], [], [], before, [name])
    except bronze_metadata.MetadataError as error:
        results["disappearance_without_supersedes_still_refused"] = "disappeared" in str(error)
    else:
        results["disappearance_without_supersedes_still_refused"] = False
    return results


def family_scope_guard(root: Path, frozen: bool) -> dict[str, bool]:
    """Require family-map additions in the analytical observation view while keeping the physical index unfiltered."""
    project = Path(__file__).resolve().parents[2]
    names = ["first", "second"]
    generation = bronze_metadata.build(names, [synthetic_metadata_item(name) for name in names], [], [], names)
    metadata = root / "family_scope.json"
    metadata.write_text(json.dumps(generation))
    environment = Environment(undefined=StrictUndefined, autoescape=select_autoescape(default_for_string=False))
    observations = environment.from_string((project / "dbt/models/silver/staging/stg_bronze__release_observations.sql").read_text())
    physical = environment.from_string((project / "dbt/models/silver/staging/stg_bronze__physical_object_index.sql").read_text())
    base = {"config": lambda **_kwargs: "", "ref": lambda _name: "metadata_rows"}
    query = "with metadata_rows as (select * from read_json_auto('{path}')) select distinct bronze_table from ({model}) as result order by bronze_table"

    def tables(model: str) -> list[str]:
        result = run_command(
            str(duckdb_binary()), [":memory:", "-json", "-init", "/dev/null", "-c", query.format(path=str(metadata), model=model)], timeout=30, check=True
        )
        return [row["bronze_table"] for row in json.loads(result.stdout)]

    original = tables(observations.render(**base, var=lambda _name: {"first": "synthetic"}))
    expanded = tables(observations.render(**base, var=lambda _name: {"first": "synthetic"} if frozen else {name: "synthetic" for name in names}))
    full_index = tables(physical.render(**base))
    return {
        "analytical_observations_follow_family_scope": original == ["first"],
        "family_map_addition_appears_in_observations": expanded == names,
        "physical_index_keeps_unfiltered_table_coverage": full_index == names,
    }


def metadata_pin_source() -> dict[str, bool]:
    """Require a backing snapshot on every lakehouse target and render its exact version in the dbt source."""
    project = Path(__file__).resolve().parents[2]
    environment = Environment(undefined=StrictUndefined, autoescape=select_autoescape(default_for_string=False))
    template = environment.from_string((project / "dbt/macros/file_metadata.sql").read_text())

    def compile_source(target_name: str, snapshots: dict[str, str], required: str = "0") -> str:
        values = {"HAI_BUILD_PINS_JSON": json.dumps(snapshots), "HAI_PROVENANCE_REQUIRED": required}

        def fail(message: str) -> None:
            raise ValueError(message)

        module: Any = template.make_module(
            {
                "target": SimpleNamespace(name=target_name),
                "env_var": lambda name, default="": values.get(name, default),
                "fromjson": json.loads,
                "source": lambda _schema, _table: "lakehouse.bronze.file_metadata",
                "exceptions": SimpleNamespace(raise_compiler_error=fail),
            }
        )
        return str(module.file_metadata_source()).strip()

    results = {}
    for name, target_name, required in (
        ("lakehouse_target_without_metadata_pin_fails_closed", "lakehouse", "0"),
        ("provenance_required_without_metadata_pin_fails_closed", "fixture", "1"),
    ):
        try:
            compile_source(target_name, {}, required)
        except ValueError:
            results[name] = True
        else:
            results[name] = False
    results["lakehouse_metadata_source_uses_exact_pinned_snapshot"] = "at (version => 173)" in compile_source("lakehouse", {"file_metadata": "173"})
    results["synthetic_fixture_without_catalog_pin_is_allowed"] = compile_source("fixture", {}) == "lakehouse.bronze.file_metadata"
    return results


def label_binding() -> dict[str, bool]:
    """Require one classification per original key while keeping every observation eligible for the seed join."""
    stored = ipps_file_labels.Stored("cms_ipps_text_lines", "original-key", "a" * 64, "capture", "", ("FY2026_CMI.txt",))
    duplicate_removed = len(ipps_file_labels.labels([stored, stored], {})) == 1
    conflict = ipps_file_labels.Stored(stored.bronze_table, stored.object_key, stored.member_sha256, stored.snapshot_id, "", ("FY2025_CMI.txt",))
    try:
        ipps_file_labels.labels([stored, conflict], {})
    except ipps_file_labels.LabelError:
        conflict_closed = True
    else:
        conflict_closed = False
    return {"identical_generated_label_rows_removed": duplicate_removed, "conflicting_original_key_labels_fail_closed": conflict_closed}


def label_reader_guards() -> dict[str, bool]:
    """Run the dbt seed guard and observation join with duplicate and conflicting synthetic seed rows."""
    root = Path(__file__).resolve().parents[2]
    environment = Environment(undefined=StrictUndefined, autoescape=select_autoescape(default_for_string=False))
    module: Any = environment.from_string((root / "dbt/macros/file_metadata.sql").read_text()).make_module({"ref": lambda _name: "seed_labels"})
    guard = str(module.assert_original_label_keys())
    model = environment.from_string((root / "dbt/models/silver/staging/stg_bronze__file_labels.sql").read_text()).render(
        config=lambda **_kwargs: "", ref=lambda name: "observations" if name == "stg_bronze__release_observations" else "seed_labels"
    )
    setup = """create table seed_labels as select 'cms_ipps_text_lines' as bronze_table, 'original' as object_key,
    'checksum' as member_sha256, 'names' as label_source, 'cmi' as family, 'data' as role, 2026 as rule_fiscal_year,
    2024 as data_fiscal_year, 2024 as identity_year, 'final' as rule_stage;
    insert into seed_labels select * from seed_labels;
    create table observations as select 'observation-one' as observation_id, 'original' as copy_key,
    'cms_ipps_text_lines' as bronze_table, 'original' as object_key, 'checksum' as member_sha256,
    'FY2026_CMI.txt' as file_name, 'ipps' as source_family;
    insert into observations select 'observation-two', copy_key, bronze_table, object_key, member_sha256, file_name, source_family from observations;
    """
    query = (
        "{setup} select count(*) as labels, count(distinct observation_id) as identities from ({model}) as result "
        "cross join ({guard}) as checked where checked.labels_are_unambiguous"
    )
    result = run_command(
        str(duckdb_binary()),
        [":memory:", "-json", "-init", "/dev/null", "-c", query.format(setup=setup, guard=guard, model=model)],
        timeout=30,
        check=True,
    )
    metrics = json.loads(result.stdout)[-1]
    conflict_query = "{setup} update seed_labels set rule_fiscal_year = 2025 where rowid = 1; {guard}"
    conflicting = conflict_query.format(setup=setup, guard=guard)
    refused = run_command(str(duckdb_binary()), [":memory:", "-json", "-init", "/dev/null", "-c", conflicting], timeout=30)
    unguarded = run_command(
        str(duckdb_binary()),
        [
            ":memory:",
            "-json",
            "-init",
            "/dev/null",
            "-c",
            query.format(setup=setup, guard="select true as labels_are_unambiguous", model=model.replace("select distinct", "select")),
        ],
        timeout=30,
        check=True,
    )
    old = json.loads(unguarded.stdout)[-1]
    return {
        "seed_maps_every_observation_once": metrics == {"labels": 2, "identities": 2},
        "dbt_label_conflict_guard_fails_before_materialization": refused.returncode != 0 and "conflicting IPPS label classifications" in refused.stderr,
        "failing_first_undeduplicated_seed_repeats_observation_labels": old["labels"] == 4 and old["identities"] == 2,
    }


def main() -> int:
    """Write a bounded synthetic check report and return failure when any named assertion fails."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--mutant", choices=("legacy_storage", "legacy_metadata", "missing_metadata_pin", "lost_table_allowed", "frozen_family_map"))
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="storage_single_copy_") as temporary:
        root = Path(temporary)
        with pytest.MonkeyPatch.context() as patch:
            if args.mutant == "legacy_storage":
                original_store = s3_store.store_file

                def legacy_store(client: Any, path: Path, key: str, *, manifest: bool = False) -> tuple[dict[str, Any], bool]:
                    proxy: Any = SimpleNamespace(configuration=client.configuration, storage_policy={"enabled": False}, call=client.call)
                    return original_store(proxy, path, key, manifest=manifest)

                patch.setattr(s3_store, "store_file", legacy_store)
            if args.mutant == "legacy_metadata":
                original_build = bronze_metadata.build

                def partial_build(names: Any, inputs: Any, excluded: Any, previous: Any, all_tables: Any) -> list[dict[str, Any]]:
                    return original_build(names, inputs, excluded, [], names)

                patch.setattr(bronze_metadata, "build", partial_build)
            if args.mutant == "missing_metadata_pin":
                original_tables = pins.mapped_tables
                patch.setattr(pins, "mapped_tables", lambda: [name for name in original_tables() if name != "file_metadata"])
            if args.mutant == "lost_table_allowed":
                patch.setattr(bronze_metadata, "validate_successor", lambda _previous, _rows: None)
            results = {"manifest_aliases_preserved": observation_aliases(root), "cross_release_payload_reused": cross_release_reuse(root)}
            results["client_without_policy_refuses_before_storage"] = missing_client_policy(root)
            results["comparison_uses_only_pinned_read_only_cli"] = comparison_uses_pinned_cli(root)
            results.update(evidence_prefixes(root))
            results.update(evidence_capture_workflow(root))
            results.update(evidence_index_workflow(root))
            results.update(capture_workflows(root))
            results.update(bootstrap_workflow(root))
            results.update(bootstrap_scope(root))
            results.update(manifest_exemptions(root))
            results.update(retirement_transport(root))
            results.update(release_copy_contract())
            results.update(label_binding())
            results.update(label_reader_guards())
            results.update(metadata_scope())
            results.update(metadata_retirements())
            results["successor_validation_precedes_data_load"] = pre_load_successor_guard(root)
            results["canonical_date_uses_stored_publication_parser"] = publication_date_parser()
            results.update(failed_attempt_publications(root))
            results["fixture_metadata_uses_builder_with_alias_and_retirement"] = fixture_generation(root)
            results.update(observation_only_sql_guard(root))
            results.update(null_generation_sql_guard(root))
            results.update(metadata_supersession(root))
            results.update(metadata_pin_source())
            results.update(family_scope_guard(root, args.mutant == "frozen_family_map"))
            _sql, reader_hashes = storage_metadata_comparison.render_models(root / "metadata.json")
    report = {
        "macros_sha256": {
            name: hashlib.sha256((Path(__file__).resolve().parents[2] / name).read_bytes()).hexdigest()
            for name in ("dbt/macros/file_metadata.sql", "dbt/macros/gold_facts.sql", "dbt/macros/cmi_files.sql")
        },
        "configuration_sha256": {
            name: hashlib.sha256((Path(__file__).resolve().parents[2] / name).read_bytes()).hexdigest()
            for name in (
                "config/lakehouse/bronze_tables.json",
                "dbt/dbt_project.yml",
                "config/acquisition/storage_policy.json",
                "config/quality_tools.json",
                "dbt/seeds/ipps_occmix_copy_labels.csv",
            )
        },
        "source_sha256": {
            str(path.relative_to(Path(__file__).resolve().parents[2])): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in {
                Path(__file__).resolve(),
                Path(s3_store.__file__),
                Path(single_copy.__file__),
                Path(bronze.__file__),
                Path(bronze_metadata.__file__),
                Path(ipps_file_labels.__file__),
                Path(pins.__file__),
                Path(storage_metadata_comparison.__file__),
                Path(__file__).with_name("duckdb_cli.py"),
                Path(__file__).with_name("run_staging_e2e.py"),
                Path(__file__).with_name("package_fixture.py"),
                Path(inspect.getfile(capture)),
                Path(inspect.getfile(FakeS3)),
                Path(inspect.getfile(Opener)),
            }
        },
        "reader_models_sha256": reader_hashes,
        "mutant": args.mutant,
        "created_utc": datetime.now(UTC).isoformat(),
        "inputs": "synthetic only",
        "boundary": "discovery and storage with in-memory S3",
        "results": results,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(
        json.dumps(
            {
                "report": str(args.report),
                "passed": sum(results.values()),
                "total": len(results),
                "failed": [name for name, passed in results.items() if not passed],
            },
            sort_keys=True,
        )
        + "\n"
    )
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
