"""Check and store the AHRQ Compendium hospital linkage files the owner saved in a browser, with immutable originals and verified S3 storage.

Offline and read-only by default: without ``--execute`` nothing is written (no lock, staged copy, receipt or folder).
Each file is independent: a file that fails its checks is held and named, never partly stored, and the others continue.
``--build-plan`` binds the reviewed request list to the saved files for locking; it writes nothing to S3. The same model as
the reference-document download, with a data role for the six linkage workbooks and the AHRQ access hold released by one bound record.
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from datetime import datetime
from pathlib import Path
from typing import cast

from scripts.acquisition import ahrq_compendium_contract as contract
from scripts.acquisition import hud_xlsx_contract as download_metadata
from scripts.acquisition.bls_api_transport import collection_lock
from scripts.acquisition.capture import base_receipt, receipt_validator, validate_receipt
from scripts.acquisition.collect_hud_api import runtime
from scripts.acquisition.collect_mmd_api import artifact
from scripts.acquisition.collection_layout import CAPTURE_ROOT_VARIABLE, capture_root, load_routes, object_prefix
from scripts.acquisition.dataset_layout import source_folder
from scripts.acquisition.legacy_versions import plan_matches
from scripts.acquisition.raw_root import RAW_ROOT, is_test_root
from scripts.acquisition.s3_store import AwsCli, StorageError, component, encoded_json, fingerprint, upload_snapshot, verify_version, write_once
from scripts.acquisition.single_copy import load_policy
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require
from scripts.acquisition.transport import CaptureError
from scripts.infrastructure.render_project_config import load_configuration

LOGGER = logging.getLogger(__name__)
ROOT = REPO_ROOT / "data/datasets/historical_acquisition/ahrq_compendium"


def planned_download(entry: dict, downloads: Path) -> bytes:
    """Read a saved file and require the planned origin, hash and size; nothing is written."""
    body = contract.read_download(entry, downloads)
    require((contract.digest(body), len(body)) == (entry["sha256"], entry["bytes"]), "AHRQ download differs from the plan")
    return body


OUTSIDE = "AHRQ local folder is outside the tool's own folder"


def confine(root: Path, branch: Path, file_name: str) -> None:
    """Refuse a local destination that is, or passes through, a symbolic link, or that resolves outside the owned folder, before anything is written.

    Every folder and file this tool writes for one planned file is named here, so a link at any of them is caught without listing the folder.
    """
    base, folder = root.resolve(), source_folder(contract.SOURCE_ID)
    capture = branch / "capture"
    destinations = [
        root / "files",
        branch,
        branch / "source",
        branch / "source" / file_name,
        branch / "source" / "download.json",
        branch / "completed.json",
        capture,
        capture / "receipt.json",
        capture / "raw",
        capture / "raw" / file_name,
        capture / "evidence",
        capture / "evidence" / "download_proof.json",
        capture / "s3_collections_reconciliation.json",
        capture / "s3_collections",
        capture / "s3_collections" / folder,
        capture / "s3_collections" / folder / "manifest.json",
    ]
    for path in destinations:
        require(not path.is_symlink() and path.resolve().is_relative_to(base), OUTSIDE)


def stage(entry: dict, downloads: Path, branch: Path) -> Path:
    """Copy the exact planned file write-once with its origin proof; after that the Downloads copy is not needed."""
    cached, proof = branch / "source" / entry["file_name"], branch / "source" / "download.json"
    # Both files present means staging finished; otherwise redo it (write_once accepts identical bytes).
    if cached.exists() and proof.exists():
        require(fingerprint(cached) == (entry["sha256"], entry["bytes"]), "AHRQ staged copy differs from the plan")
        return cached
    body = planned_download(entry, downloads)
    created = download_metadata.download_created_at(downloads / entry["file_name"])
    write_once(cached, body)
    require(fingerprint(cached) == (entry["sha256"], entry["bytes"]), "AHRQ staged copy differs from the plan")
    # Only the planned URL is kept; a referring page the browser also recorded is not evidence.
    write_once(proof, encoded_json({"file_name": entry["file_name"], "origin_url": entry["origin_url"], "created_at_utc": created}))
    return cached


def make_receipt(plan: dict, entry: dict, cached: Path, branch: Path) -> Path:
    """Write the original, its download proof and the receipt; a browser download has no HTTP status, so none is recorded."""
    registry, validator = load_registry(expected_sha256=plan["registry_sha256"]), receipt_validator()
    source = contract.source_of(registry)
    download = read_json(cached.parent / "download.json")
    proof = download | {"sha256": entry["sha256"], "bytes": entry["bytes"], "saved_by": "owner, browser"}
    snapshot = branch / "capture"
    files = (
        (f"raw/{entry['file_name']}", cached.read_bytes(), entry["role"], True),
        ("evidence/download_proof.json", encoded_json(proof), "export_receipt", False),
    )
    for name, body, _, _ in files:
        write_once(snapshot / name, body)
    label = (
        f"AHRQ Compendium hospital linkage file, {entry['vintage']} vintage" if entry["role"] == "data" else f"{entry['title']} ({entry['vintage']} vintage)"
    )
    metadata = {
        "publisher": entry["publisher"],
        "release": {
            "advertised_history": None,
            "dataset_version": entry["vintage"],
            "observed_history": f"{entry['title']}, saved by hand in a browser from the publisher's URL",
            "publisher_release_label": label,
            "publisher_updated_at": None,
            "release_date": None,
            "revision_status": "unknown",
        },
        "measurement_periods": [
            {"label": None, "start_date": None, "end_date": None, "period_type": "unknown", "source_basis": "unknown", "target_alignment": "unknown"}
        ],
        "governance": {
            "access_class": "public",
            "contains_phi": False,
            "contains_pii": False,
            "credential_reference": None,
            "credentials_required": False,
            "license_or_terms_url": None,
            "use_restrictions": contract.USE_RESTRICTIONS,
        },
    }
    timestamp = datetime.fromisoformat(download["created_at_utc"]).strftime("%Y%m%dT%H%M%SZ")
    receipt = base_receipt(source | {"preferred_route": "file_download"}, metadata, f"AHRQ_SYSTEM__{timestamp}__{entry['id'][:32]}", validator)
    receipt["acquisition"].update(
        transport_mode="download" if entry["role"] == "data" else "reference_document",
        requested_url=entry["origin_url"],
        resolved_url=entry["origin_url"],
        retrieved_at_utc=download["created_at_utc"],
        http_status=None,
        request_method="manual_download",
        tool_name="browser_ahrq_download",
        tool_version="1.0.0",
    )
    basis = contract.release_basis(download["created_at_utc"])
    receipt["artifacts"] = [artifact(snapshot / name, snapshot, role, i, original) for i, (name, _, role, original) in enumerate(files, 1)]
    receipt["artifacts"][0].update(media_type=entry["media_type"], hash_scope="complete_file")
    receipt["quality_profile"].update(
        checks_passed=["planned_origin_url", "planned_sha256_and_size", "file_signature"],
        warnings=[
            "Acquisition checks only. Sheets, columns, reference dates and model eligibility remain unreviewed.",
            "Saved by hand in a browser; retrieval time is the file creation time; no HTTP status or headers exist for it.",
            "AHRQ publishes no terms text for these files; the evidence checked is the terms record in the receipt lineage, and no reuse license is inferred.",
            "The registry access hold was lifted for this capture by the owner's recorded release.",
            *([contract.BASIS_WARNING] if basis["relation"] == "capture_precedes_release_record" else []),
        ],
    )
    receipt["lineage"]["extraction_or_query"] = json.dumps(
        {
            "mode": contract.MODE,
            "route_id": f"{entry['source_id']}:manual_download:{entry['id'][:16]}",
            "scope": f"{entry['title']} ({entry['role']}), saved by hand from {entry['origin_url']}",
            "file_id": entry["id"],
            "plan_sha256": canonical_hash(plan),
            "registry_sha256": plan["registry_sha256"],
            "schema_sha256": canonical_hash(validator.schema),
            "expected_sha256": entry["sha256"],
            "code_sha256": contract.require_code(),
            "access_release_sha256": plan["access_release"]["sha256"],
            "terms_record": contract.TERMS_RECORD,
            "release_basis": basis,
            "model_eligible": False,
        }
    )
    path = snapshot / "receipt.json"
    if path.exists():
        receipt = read_json(path)
    validate_receipt(receipt, validator, snapshot)
    contract.verify_capture(receipt, source, json.loads(receipt["lineage"]["extraction_or_query"]), snapshot, False)
    write_once(path, encoded_json(receipt))
    return path


def verify_local(path: Path) -> dict:
    """Verify the stored original and its proof without network or mutation."""
    receipt = read_json(path)
    validate_receipt(receipt, receipt_validator(), path.parent)
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    registry = load_registry(expected_sha256=lineage["registry_sha256"])
    contract.verify_capture(receipt, contract.source_of(registry), lineage, path.parent, False)
    return receipt


CONTENT_KEY = re.compile(r"(bronze_objects|bronze_evidence)/sha256/[0-9a-f]{64}")
READ_OPERATIONS = frozenset({"head-object", "get-object"})


class PrefixGuard:
    """Hand the storage writer a client that refuses any write outside this collection's prefix before the request is sent.

    A key is allowed under the collection's own prefix, or as the content-addressed key the single-copy policy selects; every other key, and every write
    operation other than a create-only object put, is refused.
    """

    def __init__(self, client: AwsCli) -> None:
        self.client = client

    def __getattr__(self, name: str) -> object:
        return getattr(self.client, name)

    def check(self, operation: str, arguments: list[str] | None) -> None:
        """Refuse the request unless it is a read, or a create-only put of a key this capture may write."""
        args = arguments or []
        if "--key" in args and operation not in READ_OPERATIONS:
            key = args[args.index("--key") + 1]
            root = capture_root()
            own = key.startswith(f"{root}{contract.PUBLISHER_PREFIX}")
            content = key.startswith(root) and CONTENT_KEY.fullmatch(key.removeprefix(root)) is not None and bool(self.client.storage_policy["enabled"])
            require(operation == "put-object" and (own or content), "AHRQ upload outside its collection prefix")
        else:
            require(
                operation in READ_OPERATIONS or not operation.startswith(("put-", "delete-", "copy-", "restore-")), "AHRQ upload outside its collection prefix"
            )

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
        self.check(operation, arguments)
        return self.client.call(service, operation, arguments)

    def call_bounded(self, service: str, operation: str, arguments: list[str] | None = None, *, timeout: float) -> dict:
        self.check(operation, arguments)
        return self.client.call_bounded(service, operation, arguments, timeout=timeout)


def selected_policy(client: AwsCli | None) -> dict:
    """The single-copy policy the storage writer selects: the client's, or the committed one when replaying without a client."""
    return client.storage_policy if client is not None else load_policy()


def verify_storage(path: Path, receipt: dict, settings: dict, policy: dict) -> None:
    """Check exact local artifacts, S3 destination under the writer's policy, manifest and immutable version evidence."""
    root = path.parent
    reconciliation = read_json(root / "s3_collections_reconciliation.json")
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    route = load_routes(load_registry(expected_sha256=lineage["registry_sha256"]))[contract.SOURCE_ID]
    folder = source_folder(contract.SOURCE_ID)
    require(reconciliation["snapshot_id"] == receipt["snapshot_id"], "AHRQ storage snapshot differs")
    require(all(reconciliation[k] == v for k, v in route.items()), "AHRQ storage route differs")
    entries = reconciliation["objects"]
    roles = {a["storage_path"]: a["role"] for a in receipt["artifacts"]} | {"receipt.json": "capture_receipt"}
    require(len(entries) == len(roles) and {e["storage_path"] for e in entries} == set(roles), "AHRQ storage artifact set differs")

    def check_object(record: dict, local: Path, role: str, manifest: bool = False) -> None:
        sha, size = fingerprint(local)
        single = policy["enabled"] and not manifest
        prefix = object_prefix(
            route, folder, "transport_audit" if single and role == "capture_receipt" else role, receipt["snapshot_id"], receipt["snapshot_id"], False
        )
        allowed = {f"{prefix}/{sha}/{component(local.name)}"}
        if single:
            scope = "evidence_objects" if prefix.removeprefix(capture_root()).split("/")[2] == "audit" else "objects"
            known = policy[scope].get(sha)
            content = "bronze_evidence" if scope == "evidence_objects" else "bronze_objects"
            allowed = {f"{capture_root()}{content}/sha256/{sha}"} | ({known["key"]} if known else set())
            require(known is None or record["key"] != known["key"] or record["version_id"] == known["version_id"], "AHRQ storage key differs")
        require(record["key"] in allowed and record["bucket"] == settings["data_bucket_name"], "AHRQ storage key differs")
        require(record["sha256"] == sha and record["byte_count"] == size, "AHRQ stored bytes differ")
        require(isinstance(record["version_id"], str) and record["version_id"] not in {"", "null"}, "AHRQ storage version missing")
        require(record["verification"] == "version_get_sha256_and_length_match", "AHRQ storage readback missing")

    for entry in entries:
        check_object(entry["object"], root / entry["storage_path"], roles[entry["storage_path"]])
    manifests = reconciliation["manifests"]
    require(len(manifests) == 1 and manifests[0]["dataset_id"] == folder, "AHRQ storage manifest set differs")
    manifest_path = root / "s3_collections" / folder / "manifest.json"
    manifest = read_json(manifest_path)
    require(
        manifest["objects"] == entries and manifest["source_id"] == contract.SOURCE_ID and manifest["snapshot_id"] == receipt["snapshot_id"],
        "AHRQ manifest differs",
    )
    require(manifest["model_eligible"] is False and manifest["snapshot_status"] == "acquired_unvalidated", "AHRQ manifest hold differs")
    check_object(manifests[0]["object"], manifest_path, "capture_receipt", True)


def check_completion(plan: dict, entry: dict, branch: Path, receipt_path: Path, receipt: dict) -> dict:
    """Read the local completion record of one file and check it against the receipt, the stored reconciliation file and the plan."""
    done_path = branch / "completed.json"
    require(done_path.is_file(), "AHRQ completion missing: no record that this file was stored")
    done = read_json(done_path)
    require(done["receipt_sha256"] == fingerprint(receipt_path)[0] and done["file_id"] == entry["id"], "AHRQ completion receipt differs")
    reconciliation = receipt_path.parent / "s3_collections_reconciliation.json"
    require(done["reconciliation_sha256"] == fingerprint(reconciliation)[0], "AHRQ completion storage evidence differs")
    require(done["model_eligible"] is False and done["status"] == "stored" and plan_matches(plan, done["plan_sha256"]), "AHRQ completion hold differs")
    return done


def execute(plan: dict, entry: dict, root: Path, downloads: Path, upload: bool, client: AwsCli | None, outputs: dict, read_only: bool = False) -> dict:
    """Run the integrated capture/storage path for one file; a valid completion, or a read-only run, performs no writes."""
    require(canonical_hash(plan) == canonical_hash(contract.load_plan()) and entry in plan["files"], "AHRQ selected plan or file differs")
    capture_root()  # a root outside the allowed forms is refused before any effect
    branch = root / "files" / entry["id"]
    if not read_only:
        confine(root, branch, entry["file_name"])
    receipt_path = branch / "capture/receipt.json"
    if not receipt_path.exists():
        contract.require_code()
        if read_only:
            planned_download(entry, downloads)
            return {
                "file_id": entry["id"],
                "source_id": entry["source_id"],
                "file_name": entry["file_name"],
                "role": entry["role"],
                "vintage": entry["vintage"],
                "status": "download_verified",
                "model_eligible": False,
                "plan_sha256": canonical_hash(plan),
            }
        receipt_path = make_receipt(plan, entry, stage(entry, downloads, branch), branch)
    receipt = verify_local(receipt_path)
    require(json.loads(receipt["lineage"]["extraction_or_query"])["file_id"] == entry["id"], "AHRQ selected file differs from receipt")
    done_path = branch / "completed.json"
    if done_path.exists():
        done = check_completion(plan, entry, branch, receipt_path, receipt)
        settings = client.configuration if client is not None else load_configuration(REPO_ROOT / ".env")[0]
        verify_storage(receipt_path, receipt, settings, selected_policy(client))
        return done
    result = {
        "file_id": entry["id"],
        "source_id": entry["source_id"],
        "file_name": entry["file_name"],
        "role": entry["role"],
        "vintage": entry["vintage"],
        "status": "capture_ready",
        "model_eligible": False,
        "receipt_path": str(receipt_path),
        "receipt_sha256": fingerprint(receipt_path)[0],
        "plan_sha256": canonical_hash(plan),
    }
    if upload:
        contract.require_code()
        if client is None:
            raise ValueError("AHRQ storage client missing")
        registry = load_registry(expected_sha256=plan["registry_sha256"])
        stored = upload_snapshot(receipt_path, [], cast(AwsCli, PrefixGuard(client)), outputs, registry, receipt_validator())
        verify_storage(receipt_path, receipt, client.configuration, client.storage_policy)
        result.update(
            status="stored", reconciliation_path=stored["reconciliation_path"], reconciliation_sha256=fingerprint(Path(stored["reconciliation_path"]))[0]
        )
        write_once(done_path, encoded_json(result))
    return result


def run_all(
    plan: dict, root: Path, downloads: Path, upload: bool, client: AwsCli | None, outputs: dict, read_only: bool = False
) -> tuple[list[dict], list[dict]]:
    """Process every planned file independently; return completed results and held files with their public reason."""
    results, held = [], []
    with nullcontext() if read_only else collection_lock(root):
        for entry in plan["files"]:
            try:
                results.append(execute(plan, entry, root, downloads, upload, client, outputs, read_only))
            except (ValueError, OSError, KeyError) as error:
                # require() messages are fixed public text; other exceptions are reported by type only.
                reason = str(error) if isinstance(error, ValueError) and str(error).startswith(("AHRQ", "Unreviewed AHRQ")) else type(error).__name__
                held.append({"file_name": entry["file_name"], "file_id": entry["id"], "reason": reason})
    return results, held


VERIFY_OBJECT_SECONDS = 120.0
VERIFY_TOTAL_SECONDS = 1800.0
LOCAL_STATUS = {"stored": "local_record_of_upload"}
LOCAL_NOTE = "These results come from local records of past uploads and are not a check of S3. Run --verify-s3 before removing any local copy."


class ReadOnlyReader:
    """Hand the version check a client that only reads objects, within the per-object limit and the deadline of the whole run."""

    def __init__(self, client: AwsCli, deadline: float, clock: Callable[[], float]) -> None:
        self.client, self.deadline, self.clock = client, deadline, clock

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
        require(service == "s3api" and operation in READ_OPERATIONS, "AHRQ verification may only read stored objects")
        remaining = self.deadline - self.clock()
        require(remaining > 0, "AHRQ verification deadline reached")
        return self.client.call_bounded(service, operation, arguments, timeout=min(VERIFY_OBJECT_SECONDS, remaining))


def verify_object(reader: ReadOnlyReader, record: dict, sha256: str, size: int) -> None:
    """Read one recorded version: its size is checked by a HEAD before any download, then the stored bytes are hashed and compared."""
    require(0 < size <= contract.MAX_BYTES, "AHRQ S3 object size is outside the planned bound")
    head = reader.call("s3api", "head-object", ["--key", record["key"], "--version-id", record["version_id"]])
    require(head.get("VersionId") == record["version_id"], "AHRQ S3 version differs from the recorded one")
    require(head.get("ContentLength") == size, "AHRQ S3 object size differs from the record")
    verify_version(cast(AwsCli, reader), record["key"], record["version_id"], sha256, size)


def key_prefix(key: str) -> str:
    """The collection prefix of a stored key, with any capture root before it; a content-addressed key has none."""
    return key[: key.index(contract.PUBLISHER_PREFIX) + len(contract.PUBLISHER_PREFIX)] if contract.PUBLISHER_PREFIX in key else "(content-addressed key)"


def verify_remote_file(plan: dict, entry: dict, root: Path, client: AwsCli, reader: ReadOnlyReader) -> dict:
    """Prove one file on S3: its local records are consistent, then every recorded object is read back at its exact version."""
    branch = root / "files" / entry["id"]
    receipt_path = branch / "capture/receipt.json"
    require(receipt_path.is_file(), "AHRQ receipt missing: nothing is recorded as stored for this file")
    receipt = verify_local(receipt_path)
    require(json.loads(receipt["lineage"]["extraction_or_query"])["file_id"] == entry["id"], "AHRQ selected file differs from receipt")
    check_completion(plan, entry, branch, receipt_path, receipt)
    verify_storage(receipt_path, receipt, client.configuration, client.storage_policy)
    reconciliation = read_json(receipt_path.parent / "s3_collections_reconciliation.json")
    objects = [(e["storage_path"], e["object"]) for e in reconciliation["objects"]] + [("manifest.json", reconciliation["manifests"][0]["object"])]
    for path, record in objects:
        planned = path == f"raw/{entry['file_name']}"
        expected = (entry["sha256"], entry["bytes"]) if planned else (record["sha256"], record["byte_count"])
        require((record["sha256"], record["byte_count"]) == expected, "AHRQ stored record differs from the locked plan")
        verify_object(reader, record, *expected)
    return {
        "file_name": entry["file_name"],
        "vintage": entry["vintage"],
        "status": "verified_on_s3",
        "objects_verified": len(objects),
        "bucket": objects[0][1]["bucket"],
        "key_prefix": key_prefix(objects[0][1]["key"]),
    }


def verify_remote(plan: dict, root: Path, client: AwsCli, clock: Callable[[], float] = time.monotonic) -> tuple[list[dict], list[dict]]:
    """Read back every planned file from S3 without writing anywhere; return the proven files and the files that are not proven, with the reason."""
    reader, results, failed = ReadOnlyReader(client, clock() + VERIFY_TOTAL_SECONDS, clock), [], []
    for entry in plan["files"]:
        try:
            results.append(verify_remote_file(plan, entry, root, client, reader))
        except StorageError as error:
            failed.append({"file_name": entry["file_name"], "file_id": entry["id"], "reason": f"AHRQ S3 read failed: {error.code}"})
        except CaptureError as error:
            failed.append({"file_name": entry["file_name"], "file_id": entry["id"], "reason": f"AHRQ S3 check failed: {error}"})
        except (ValueError, OSError, KeyError) as error:
            reason = str(error) if isinstance(error, ValueError) and str(error).startswith("AHRQ") else type(error).__name__
            failed.append({"file_name": entry["file_name"], "file_id": entry["id"], "reason": reason})
    return results, failed


def verification_summary(total: int, results: list[dict], failed: list[dict]) -> str:
    """Say in plain words whether every planned file is proven on S3, and whether the local copies may go."""
    if len(results) == total and not failed:
        return (
            f"All {total} files are proven on S3: each recorded version was read back and matches the locked hash and size. "
            "The local copies may now be removed."
        )
    names = ", ".join(sorted({f["file_name"] for f in failed}) or ["no file was checked"])
    return f"NOT PROVEN on S3: {names}. Do not remove any local copy."


def root_choice(requested: str | None) -> str:
    """The capture root the command line chose. An environment root that the command line did not choose, or an unsupported form, is refused."""
    inherited = os.environ.get(CAPTURE_ROOT_VARIABLE, "")
    chosen = "" if requested is None else requested
    require(
        inherited in {"", chosen},
        f"AHRQ capture root: {CAPTURE_ROOT_VARIABLE} is {inherited!r} but the command line does not choose it. "
        f"Pass --capture-root {inherited} or unset the variable.",
    )
    require(chosen in {"", RAW_ROOT} or is_test_root(chosen), f"AHRQ capture root: {chosen!r} is not raw/ or ingestion_test/<run>/")
    return chosen


@contextmanager
def chosen_root(chosen: str) -> Iterator[str]:
    """Run with the chosen capture root in the environment the storage writer reads, and put the earlier value back afterwards."""
    previous = os.environ.get(CAPTURE_ROOT_VARIABLE)
    os.environ[CAPTURE_ROOT_VARIABLE] = chosen
    try:
        yield chosen
    finally:
        if previous is None:
            os.environ.pop(CAPTURE_ROOT_VARIABLE, None)
        else:
            os.environ[CAPTURE_ROOT_VARIABLE] = previous


def destination(client: AwsCli, root: str) -> dict:
    """The full destination, printed before any storage call: the configured bucket, the key prefix and whether the single-copy policy adds content keys."""
    return {
        "bucket": client.configuration["data_bucket_name"],
        "key_prefix": f"{root}{contract.PUBLISHER_PREFIX}",
        "capture_root": root or "(none, the original layout)",
        "single_copy_content_keys": f"{root}bronze_objects/sha256/ and {root}bronze_evidence/sha256/" if client.storage_policy["enabled"] else "off",
    }


def main() -> None:
    """Validate offline by default; --build-plan writes the plan; --execute is the only write; --verify-s3 proves the stored files by reading S3."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--downloads", type=Path, default=Path.home() / "Downloads" / "ahrq_compendium")
    parser.add_argument("--build-plan", action="store_true", help="Bind the reviewed request list to the saved files; writes the plan and its lock")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--execute", action="store_true")
    modes.add_argument("--verify-s3", action="store_true", help="Read every stored object back at its recorded version; writes and deletes nothing")
    parser.add_argument(
        "--capture-root",
        help="The bucket root to use: raw/ or ingestion_test/<run>/. Omit it for the original layout; an environment root is refused unless this names it",
    )
    parser.add_argument("--validate-receipt", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.validate_receipt:
        receipt = verify_local(args.validate_receipt)
        sys.stdout.write(json.dumps({"status": "valid", "snapshot_id": receipt["snapshot_id"], "model_eligible": False}) + "\n")
        return
    if args.build_plan:
        plan = contract.build_plan(read_json(contract.REQUESTS_PATH)["files"], args.downloads)
        write_once(contract.PLAN_PATH, encoded_json(plan))
        write_once(contract.PLAN_PATH.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
        sys.stdout.write(json.dumps({"plan": str(contract.PLAN_PATH), "files": [f["file_name"] for f in plan["files"]]}) + "\n")
        return
    try:
        chosen = root_choice(args.capture_root)
    except ValueError as error:
        LOGGER.error(str(error))
        sys.exit(2)
    with chosen_root(chosen) as root:
        plan = contract.load_plan()
        client, outputs = runtime() if (args.execute or args.verify_s3) else (None, {})
        if client is not None:
            LOGGER.info(json.dumps({"destination": destination(client, root)}))
        if args.verify_s3:
            proven, failed = verify_remote(plan, ROOT, cast(AwsCli, client))
            for line in proven:
                LOGGER.info(json.dumps(line, sort_keys=True))
            for item in failed:
                LOGGER.error(json.dumps({"status": "not_verified_on_s3"} | item, sort_keys=True))
            LOGGER.info(verification_summary(len(plan["files"]), proven, failed))
            sys.exit(0 if len(proven) == len(plan["files"]) and not failed else 1)
        results, held = run_all(plan, ROOT, args.downloads, args.execute, client, outputs, not args.execute)
    for result in results:
        LOGGER.info(
            json.dumps(
                {"file_name": result["file_name"], "vintage": result["vintage"], "status": LOCAL_STATUS.get(result["status"], result["status"])}, sort_keys=True
            )
        )
    for item in held:
        LOGGER.error(json.dumps({"status": "held"} | item, sort_keys=True))
    note = {"note": LOCAL_NOTE} if any(r["status"] == "stored" for r in results) else {}
    LOGGER.info(json.dumps({"completed": len(results), "held": [h["file_name"] for h in held]} | note))
    sys.exit(1 if held else 0)


if __name__ == "__main__":
    main()
