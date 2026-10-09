"""Conditional, content-addressed S3 storage with version-specific byte verification."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import os
import re
import sys
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

from scripts.acquisition.archive_review import is_zip_container, storage_entries
from scripts.acquisition.capture import receipt_validator, validate_receipt
from scripts.acquisition.cli_tools import executable, run_argv
from scripts.acquisition.collection_layout import load_routes, object_prefix
from scripts.acquisition.dataset_layout import CATEGORIES, dataset_id, group_entries
from scripts.acquisition.legacy_versions import bound_record
from scripts.acquisition.source_registry import RegistryError, canonical_hash, load_registry, read_json, require_collection_scope
from scripts.acquisition.storage_controls import reuse_identity, verify_route
from scripts.acquisition.transport import CaptureError, inspect_payload, validate_request
from scripts.infrastructure.render_project_config import REPO_ROOT, load_configuration
from scripts.process import SubprocessError, TimeoutExpired


class StorageError(CaptureError):
    """A sanitized AWS operation failure without provider response bodies."""

    def __init__(self, operation: str, code: str) -> None:
        self.code = code
        super().__init__(f"{operation}: {code}. No permission changes or overwrite attempted.")


def fingerprint(path: Path) -> tuple[str, int]:
    """Return a file's SHA-256 digest and byte count without modifying its contents."""
    with path.open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
    return digest, path.stat().st_size


def component(value: str) -> str:
    """Validate and return a bounded storage-key component."""
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}", value):
        raise CaptureError("Invalid storage key component.")
    return value


def write_once(path: Path, content: bytes) -> None:
    """Atomically publish bytes or verify identical existing evidence without overwriting it."""
    from scripts.acquisition.redownload_controls import CURRENT, validate_path, writing

    if (boundary := CURRENT.get()) is not None:
        validate_path(path, boundary.root)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != content:
            raise CaptureError("Existing local evidence differs; do not overwrite it.")
        return
    writing(path, len(content))
    # Publish complete bytes atomically; interruption cannot leave a partial final manifest.
    with tempfile.TemporaryDirectory(dir=path.parent) as temporary:
        staged = Path(temporary) / "content"
        staged.write_bytes(content)
        try:
            os.link(staged, path)
        except FileExistsError:
            if path.read_bytes() != content:
                raise CaptureError("Concurrent local evidence differs.") from None


def encoded_json(value: dict) -> bytes:
    """Return deterministic UTF-8 JSON bytes with finite numbers and a trailing newline."""
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True, allow_nan=False) + "\n").encode()


class AwsCli:
    """Project-scoped AWS CLI adapter with bounded calls and sanitized errors."""

    def __init__(self, configuration: dict[str, str], runner: Any = None) -> None:
        self.configuration = configuration
        self.runner = runner or run_argv

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
        """Run a bounded project-scoped AWS command and return JSON or a sanitized storage error."""
        from scripts.acquisition.redownload_controls import CURRENT

        if CURRENT.get() is not None and (service, operation) not in {("sts", "get-caller-identity"), ("secretsmanager", "get-secret-value")}:
            raise ValueError("AWS action outside redownload read-only scope")
        settings = self.configuration
        environment = {key: value for key, value in os.environ.items() if not key.startswith("AWS_")}
        environment.update(AWS_IGNORE_CONFIGURED_ENDPOINT_URLS="true", AWS_MAX_ATTEMPTS="3", AWS_RETRY_MODE="standard")
        command = [
            "aws",
            service,
            operation,
            "--profile",
            settings["aws_profile"],
            "--region",
            settings["aws_region"],
            "--output",
            "json",
            "--no-cli-pager",
            "--no-cli-auto-prompt",
            "--cli-connect-timeout",
            "15",
            "--cli-read-timeout",
            "60",
        ]
        if service == "s3api":
            command += ["--bucket", settings["data_bucket_name"], "--expected-bucket-owner", settings["expected_account_id"]]
        try:
            result = self.runner(command + (arguments or []), capture_output=True, text=True, env=environment, timeout=180)
        except (OSError, TimeoutExpired):
            raise StorageError(operation, "request_unconfirmed") from None
        if result.returncode:
            code = re.search(r"An error occurred \(([A-Za-z0-9]+)\)", result.stderr)
            raise StorageError(operation, code.group(1) if code else "request_failed")
        try:
            payload = json.loads(result.stdout)
        except ValueError:
            raise StorageError(operation, "invalid_response") from None
        if not isinstance(payload, dict):
            raise StorageError(operation, "invalid_response")
        return payload


def preflight(client: AwsCli, outputs: dict) -> None:
    """Reject any mismatch in project identity, destination or required bucket protections."""
    settings = client.configuration
    account, profile = settings["expected_account_id"], settings["aws_profile"]
    identity = client.call("sts", "get-caller-identity")
    arn = identity.get("Arn", "")
    if identity.get("Account") != account or not isinstance(arn, str) or not arn.startswith(f"arn:aws:iam::{account}:user/"):
        raise CaptureError("Project account and IAM-user identity do not match .env.")
    if arn.rsplit("/", 1)[-1] != profile:
        raise CaptureError("The project profile must authenticate as its same-named IAM user.")
    expected = {name: f"s3://{settings['data_bucket_name']}/{{publisher}}/{{collection}}/{name}/" for name in CATEGORIES}
    if outputs.get("bucket_name", {}).get("value") != settings["data_bucket_name"] or outputs.get("collection_prefixes", {}).get("value") != expected:
        raise CaptureError("Terraform outputs and .env storage destination differ.")
    location = client.call("s3api", "get-bucket-location").get("LocationConstraint") or "us-east-1"
    if location != settings["aws_region"]:
        raise CaptureError("Bucket region differs from .env.")
    if client.call("s3api", "get-bucket-versioning").get("Status") != "Enabled":
        raise CaptureError("Bucket versioning must be enabled before capture storage.")
    public = client.call("s3api", "get-public-access-block").get("PublicAccessBlockConfiguration", {})
    if any(public.get(key) is not True for key in ("BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets")):
        raise CaptureError("All four public-access protections must be enabled.")
    rules = client.call("s3api", "get-bucket-encryption").get("ServerSideEncryptionConfiguration", {}).get("Rules", [])
    if len(rules) != 1 or rules[0].get("ApplyServerSideEncryptionByDefault", {}).get("SSEAlgorithm") != "AES256":
        raise CaptureError("The approved SSE-S3 encryption configuration is missing.")
    ownership = client.call("s3api", "get-bucket-ownership-controls").get("OwnershipControls", {}).get("Rules", [])
    if ownership != [{"ObjectOwnership": "BucketOwnerEnforced"}]:
        raise CaptureError("Bucket-owner-enforced ownership is required.")


def verify_version(client: AwsCli, key: str, version: Any, digest: str, size: int) -> None:
    """Verify one immutable S3 version against expected bytes, checksum and encryption."""
    if not isinstance(version, str) or not version or version == "null":
        raise CaptureError("S3 did not return a usable immutable version ID.")
    checksum = base64.b64encode(bytes.fromhex(digest)).decode()
    with tempfile.TemporaryDirectory(prefix="s3_verify_") as temporary:
        local = Path(temporary) / "object"
        result = client.call("s3api", "get-object", ["--key", key, "--version-id", version, "--checksum-mode", "ENABLED", str(local)])
        if result.get("VersionId") != version or result.get("ContentLength") != size or fingerprint(local) != (digest, size):
            raise CaptureError("Downloaded S3 version differs from the local byte hash or length.")
        if result.get("ChecksumSHA256") != checksum or result.get("ServerSideEncryption") != "AES256":
            raise CaptureError("S3 checksum or encryption differs from the approved stored object.")


def store_file(client: AwsCli, path: Path, key: str) -> tuple[dict, bool]:
    """Create or verify one content-addressed object and return its version record and creation flag."""
    parts = PurePosixPath(key).parts
    if len(parts) < 6 or parts[2] not in CATEGORIES or any(part in {"", ".", ".."} for part in key.split("/")):
        raise CaptureError("Object key is outside approved storage prefixes.")
    dataset_id(parts[0])
    dataset_id(parts[1])
    if parts[2] == "datasets":
        dataset_id(parts[3])
    if key.lower().endswith(".zip") or is_zip_container(path):
        raise CaptureError("ZIP containers must remain local; upload verified extracted contents only.")
    digest, size = fingerprint(path)
    if digest not in parts or size > 512 * 1024**2:
        raise CaptureError("Object keys must contain the actual SHA-256; single-file storage is bounded to 512 MiB.")
    created = False
    try:
        head = client.call("s3api", "head-object", ["--key", key, "--checksum-mode", "ENABLED"])
    except StorageError as error:
        if error.code not in {"404", "NoSuchKey", "NotFound"}:
            raise
        checksum = base64.b64encode(bytes.fromhex(digest)).decode()
        try:
            head = client.call(
                "s3api",
                "put-object",
                [
                    "--key",
                    key,
                    "--body",
                    str(path),
                    "--if-none-match",
                    "*",
                    "--checksum-algorithm",
                    "SHA256",
                    "--checksum-sha256",
                    checksum,
                    "--server-side-encryption",
                    "AES256",
                    "--metadata",
                    json.dumps({"sha256": digest}),
                ],
            )
            created = True
        except StorageError as conflict:
            if conflict.code not in {"412", "PreconditionFailed", "409", "ConditionalRequestConflict"}:
                raise
            # A racing writer may have completed the same content; never replace it.
            head = client.call("s3api", "head-object", ["--key", key, "--checksum-mode", "ENABLED"])
    version = head.get("VersionId")
    verify_version(client, key, version, digest, size)
    return {
        "bucket": client.configuration["data_bucket_name"],
        "key": key,
        "version_id": version,
        "sha256": digest,
        "byte_count": size,
        "verification": "version_get_sha256_and_length_match",
    }, created


def extract_references(root: Path, receipt: dict, selections: list[dict]) -> list[dict]:
    """Return validated, explicitly selected archive references while preserving their original bytes."""
    references = []
    names: set[str] = set()
    for selection in selections:
        if set(selection) != {"artifact_id", "member", "file_name", "role", "format"}:
            raise CaptureError("Archive reference selections require exact member, name, role, format and parent artifact.")
        name, member = selection["file_name"], selection["member"]
        if selection["role"] not in {"dictionary", "methodology", "layout", "manifest"} or name in names:
            raise CaptureError("Reference roles must be explicit and stored filenames unique.")
        names.add(name)
        validate_request(receipt["acquisition"]["requested_url"], name, selection["format"], selection["role"])
        member_path = PurePosixPath(member)
        if member_path.is_absolute() or ".." in member_path.parts or "\\" in member:
            raise CaptureError("Unsafe archive member path.")
        candidates = [item for item in receipt["artifacts"] if item["artifact_id"] == selection["artifact_id"] and item["compression"] == "zip"]
        if len(candidates) != 1:
            raise CaptureError("Reference parent must be an acquired ZIP artifact.")
        parent = candidates[0]
        with zipfile.ZipFile(root / parent["storage_path"]) as archive:
            matches = [item for item in archive.infolist() if item.filename == member]
            if len(matches) != 1 or matches[0].is_dir() or matches[0].file_size > 20 * 1024**2 or (matches[0].external_attr >> 16) & 0o170000 == 0o120000:
                raise CaptureError("Archive reference is missing, ambiguous, oversized or a symbolic link.")
            content = archive.read(matches[0])
        target = root / "reference" / name
        with tempfile.TemporaryDirectory() as temporary:
            check = Path(temporary) / name
            check.write_bytes(content)
            failure = inspect_payload(check, selection["format"], selection["role"])
            if failure:
                raise CaptureError(f"Archive reference validation failed: {failure}")
        write_once(target, content)
        references.append(
            {
                "storage_path": target.relative_to(root).as_posix(),
                "role": selection["role"],
                "archive_member": member,
                "parent_artifact_id": parent["artifact_id"],
                "parent_sha256": parent["sha256"],
                "original_member_bytes_unchanged": True,
                "member_crc32": f"{matches[0].CRC:08x}",
            }
        )
    return references


TERMS_ACCEPTANCE_PATH = REPO_ROOT / "config/acquisition/terms_acceptance_20260928.json"
# Release records are append-only: a receipt binds one record by SHA-256, so an earlier record is never edited.
ACCESS_RELEASE_PATHS = (
    REPO_ROOT / "config/acquisition/access_releases_20260929.json",
    REPO_ROOT / "config/acquisition/access_releases_20261002.json",
)
# New receipts bind the newest record.
ACCESS_RELEASE_PATH = ACCESS_RELEASE_PATHS[-1]


def access_released(source: dict, lineage: dict) -> bool:
    """Return True only when a recorded release frees this source's registry access_hold.

    The base registry stays immutable until collection closeout, so a held source can be
    stored only when its receipt lineage binds the exact terms acceptance record (current or archived).
    """
    try:
        body = bound_record(TERMS_ACCEPTANCE_PATH, lineage["terms_sha256"])
        bound = body is not None
        records = [d for d in json.loads(body)["datasets"] if d["source_id"] == source["source_id"]] if body is not None else []
    except (OSError, KeyError, TypeError, ValueError, RegistryError):
        return released_by_record(source, lineage)
    return (bound and len(records) == 1 and records[0].get("terms_accepted") is True) or released_by_record(source, lineage)


def released_by_record(source: dict, lineage: dict) -> bool:
    """Return True only when the receipt binds the exact access-release record naming this source.

    For holds that are not about terms (for example a privacy exception), the release is a record rather than a
    terms acceptance, bound by its SHA-256 in the receipt lineage. The bound record is looked up among the listed
    records and their archived predecessors, so receipts that bind an earlier record keep verifying.
    """
    for path in ACCESS_RELEASE_PATHS:
        try:
            body = bound_record(path, lineage["access_release_sha256"])
            if body is None:
                continue
            records = [r for r in json.loads(body)["releases"] if r["source_id"] == source["source_id"]]
        except (OSError, KeyError, TypeError, ValueError, RegistryError):
            return False
        return len(records) == 1 and records[0].get("released") is True
    return False


def upload_snapshot(
    receipt_path: Path,
    references: list[dict],
    client: AwsCli,
    outputs: dict,
    registry: dict,
    validator: Any,
    *,
    audit_only: bool = False,
    shared: dict | None = None,
    progress: Any = None,
    archive_mapping: dict | None = None,
) -> dict:
    """Store a validated capture without clearing access, quality or model holds.

    Parameters
    ----------
    receipt_path : Path
        Complete local receipt whose artifacts and lineage must verify.
    references : list[dict]
        Explicit approved archive-reference selections.
    client : AwsCli
        Project-scoped storage client; live calls occur through this boundary.
    outputs, registry : dict
        Expected infrastructure outputs and selected source registry.
    validator : Any
        Receipt schema validator used before storage.
    audit_only : bool
        Permit incomplete captures only as evidence.
    shared : dict or None
        Previously verified object records eligible for checked reuse.
    progress : callable or None
        Receives bounded dataset progress counters.
    archive_mapping : dict or None
        Explicit checksum-bound mapping of every archive leaf.

    Returns
    -------
    dict
        Verified object counts and the local reconciliation path.

    Raises
    ------
    CaptureError
        Receipt, route, local bytes or destination checks fail.
    """
    receipt, root = read_json(receipt_path), receipt_path.parent
    validate_receipt(receipt, validator, root)
    evidence_only = receipt["snapshot_status"] != "acquired_unvalidated"
    if evidence_only and not audit_only:
        raise CaptureError("Only complete acquired-unvalidated captures can enter this storage workflow.")
    if evidence_only and references:
        raise CaptureError("Failed captures cannot supply extracted reference documents.")
    source_id, run_id = component(receipt["source"]["source_record_id"]), component(receipt["snapshot_id"])
    release_id = component(receipt["release"]["release_date"] or run_id)
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    if lineage.get("registry_sha256") != canonical_hash(registry):
        from scripts.acquisition.source_registry import RegistryError

        try:
            registry = load_registry(expected_sha256=lineage.get("registry_sha256"))
        except RegistryError:
            raise CaptureError("Receipt registry/schema fingerprints differ from the selected locked inputs.") from None
    if lineage.get("registry_sha256") != canonical_hash(registry) or lineage.get("schema_sha256") != canonical_hash(validator.schema):
        raise CaptureError("Receipt registry/schema fingerprints differ from the selected locked inputs.")
    sources = [source for source in registry["sources"] if source["source_id"] == source_id]
    if len(sources) != 1 or (sources[0]["preferred_route"] == "access_hold" and not access_released(sources[0], lineage)):
        raise CaptureError("Source is unknown or on access hold.")
    require_collection_scope(source_id)
    verify_route(receipt, sources[0], lineage, root, evidence_only)
    route = load_routes(registry)[source_id]
    preflight(client, outputs)
    extracted = extract_references(root, receipt, references) if not evidence_only else []
    if archive_mapping is not None:
        from scripts.acquisition.mapped_archives import mapped_storage_entries

        if evidence_only or references:
            raise CaptureError("Explicit archive maps require a complete capture and carry their own reference selections.")
        expanded, containers = mapped_storage_entries(receipt_path, receipt, registry, archive_mapping, route)
    else:
        expanded, _, containers = storage_entries(receipt_path, receipt, validator, extracted) if not evidence_only else ([], [], set())
    entries = []
    local_only: dict[str, dict] = {}
    for item in receipt["artifacts"]:
        if item["sha256"] in containers:
            continue
        if is_zip_container(root / item["storage_path"]):
            if not evidence_only:
                raise CaptureError("Complete ZIP capture was not expanded; no storage writes are permitted.")
            local_only[item["storage_path"]] = {
                "storage_path": item["storage_path"],
                "sha256": item["sha256"],
                "byte_count": item["byte_count"],
                "reason": "archive_payload_retained_locally_only",
            }
            continue
        entries.append({"storage_path": item["storage_path"], "role": item["role"], "artifact_id": item["artifact_id"]})
    entries.extend(expanded)
    entries.extend(item for item in extracted if item["parent_sha256"] not in containers)
    entries.append({"storage_path": receipt_path.name, "role": "capture_receipt"})
    # Audit bytes equal to a selected payload reuse its S3 object through an explicit manifest alias.
    for path in sorted((root / "audit").rglob("*")):
        if path.is_file():
            relative = path.relative_to(root).as_posix()
            if not path.resolve().is_relative_to(root.resolve()) or path.is_symlink():
                raise CaptureError("Audit evidence must stay inside its snapshot.")
            if not re.fullmatch(r"audit/request_[0-9]{4}/attempt_[0-9]{2}/[A-Za-z0-9][A-Za-z0-9._-]{0,199}", relative):
                raise CaptureError("Unexpected audit evidence path.")
            digest, size = fingerprint(path)
            if digest in containers:
                continue
            if is_zip_container(path):
                local_only[relative] = {"storage_path": relative, "sha256": digest, "byte_count": size, "reason": "archive_payload_retained_locally_only"}
                continue
            entries.append({"storage_path": relative, "role": "transport_audit"})
    for entry in entries:
        if str(entry.get("stored_file_name", "")).lower().endswith(".zip") or is_zip_container(root / entry["storage_path"]):
            raise CaptureError("ZIP container remains in the upload selection; no storage writes are permitted.")
    groups = group_entries(root, receipt, entries, containers, registry, mapped=archive_mapping is not None)
    stored: list[dict] = []
    manifests: list[dict] = []
    created_count = 0
    shared = shared if shared is not None else {}
    reuse_group = reuse_identity(receipt)
    collection_objects: dict[str, dict] = {}
    for folder, dataset_entries in sorted(groups.items()):
        dataset_objects: list[dict] = []
        by_digest: dict[str, dict] = {}
        for entry in dataset_entries:
            path = root / entry["storage_path"]
            digest, _ = fingerprint(path)
            role = entry["role"]
            if role == "transport_audit" and digest in by_digest:
                item = {**entry, "object": by_digest[digest], "reuses_identical_payload": True}
            else:
                prefix = object_prefix(route, folder, role, release_id, run_id, evidence_only)
                key = f"{prefix}/{digest}/{component(entry.get('stored_file_name', path.name))}"
                reuse_key = canonical_hash({"scope": reuse_group, "sha256": digest, "role": role, "key": key})
                candidate = shared.get(reuse_key) if role in {"data", "api_page"} and not evidence_only else None
                if key in collection_objects:
                    record, created = collection_objects[key], False
                elif candidate:
                    if candidate["bucket"] != client.configuration["data_bucket_name"] or candidate["sha256"] != digest or candidate["key"] != key:
                        raise CaptureError("Shared storage reference differs from the requested bytes or destination.")
                    verify_version(client, key, candidate["version_id"], digest, path.stat().st_size)
                    record, created = candidate, False
                else:
                    record, created = store_file(client, path, key)
                collection_objects[key] = record
                if role in {"data", "api_page"} and not evidence_only:
                    shared[reuse_key] = record
                created_count += created
                item = {**entry, "object": record}
                by_digest[digest] = record
            dataset_objects.append(item)
        manifest = {
            "storage_contract_version": "4.0.0",
            **route,
            "dataset_id": folder,
            "snapshot_id": run_id,
            "snapshot_status": receipt["snapshot_status"],
            "source_id": source_id,
            "release_id": release_id,
            "registry_sha256": lineage["registry_sha256"],
            "objects": dataset_objects,
            "model_eligible": False,
            "review_status": "comprehensive_schema_metadata_and_measure_review_pending",
            "dictionary_status": "applicability_and_sections_pending"
            if any(item["role"] == "dictionary" or item.get("reference_kind") == "dictionary_candidate" for item in dataset_objects)
            else "missing_or_unidentified",
            "dictionary_sections": [],
            "acquisition_holds": [
                {"archive_member": item["archive_member"], "reason": item["hold_reason"]} for item in dataset_objects if item.get("hold_reason")
            ],
        }
        if containers:
            manifest.update(
                archive_storage_policy="extracted_members_only_zip_retained_locally",
                local_archive_containers=[item for item in receipt["artifacts"] if item["sha256"] in containers],
                documentation_review_status="shared_collection_candidates_not_coverage_verified",
            )
        if local_only:
            manifest.update(local_only_archive_payloads=list(local_only.values()), archive_storage_policy="extracted_members_only_zip_retained_locally")
        manifest_path = root / "s3_collections" / folder / "manifest.json"
        write_once(manifest_path, encoded_json(manifest))
        digest, _ = fingerprint(manifest_path)
        prefix = object_prefix(route, folder, "capture_receipt", release_id, run_id, evidence_only)
        manifest_object, created = store_file(client, manifest_path, f"{prefix}/{digest}/manifest.json")
        created_count += created
        manifests.append({"dataset_id": folder, "object": manifest_object})
        stored.extend(dataset_objects)
        if progress:
            progress({"dataset_id": folder, "datasets_verified": len(manifests), "dataset_count": len(groups), "objects_created": created_count})
    reconciliation = {"storage_contract_version": "4.0.0", **route, "manifests": manifests, "snapshot_id": run_id, "objects": stored}
    reconciliation_path = root / "s3_collections_reconciliation.json"
    write_once(reconciliation_path, encoded_json(reconciliation))
    unique_count = len({item["object"]["key"] for item in stored}) + len(manifests)
    return {
        "snapshot_id": run_id,
        "dataset_count": len(groups),
        "objects_verified": unique_count,
        "objects_created": created_count,
        "objects_reused": unique_count - created_count,
        "reconciliation_path": str(reconciliation_path),
        "model_eligible": False,
    }


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description="Store one completed capture in the .env-selected Terraform bucket; do not approve model use.")
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--references", required=True, type=Path)
    parser.add_argument("--audit-only", action="store_true", help="Permit failed/partial captures only under audit; never promote them into raw data.")
    arguments = parser.parse_args()
    try:
        settings, _ = load_configuration(REPO_ROOT / ".env")
        result = run_argv([executable("terraform"), f"-chdir={REPO_ROOT / 'infra'}", "output", "-json"], capture_output=True, text=True, timeout=30, check=True)
        selections = read_json(arguments.references)["references"]
        if not isinstance(selections, list):
            raise CaptureError("Reference selections must be a list.")
        report = upload_snapshot(
            arguments.receipt,
            selections,
            AwsCli(settings),
            json.loads(result.stdout),
            load_registry(),
            receipt_validator(),
            audit_only=arguments.audit_only,
            progress=lambda value: logging.getLogger(__name__).info(json.dumps(value), extra={"progress": value}),
        )
    except (ValueError, OSError, KeyError, TypeError, SubprocessError, zipfile.BadZipFile) as error:
        parser.error(str(error) if not isinstance(error, SubprocessError) else "Local Terraform outputs could not be read.")
    sys.stdout.write(str(json.dumps(report, indent=2)) + "\n")


if __name__ == "__main__":
    main()
