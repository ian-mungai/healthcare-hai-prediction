"""Permanently delete the listed S3 object versions after re-verifying each local original.

Read-only by default: verifies identity, local originals, live versions and sizes. ``--execute`` deletes exactly the
(key, version ID) pairs in the deletion list, reads each back and writes a retirement record. When a list entry names a
``replacement``, that redacted version must be live with its recorded size before the original is touched. Failure
modes are in ``deletion_failure_modes.md`` and ``replacement_failure_modes.md``. Run from the repository root:

    PYTHONPATH=. .venv/bin/python scripts/acquisition/one_off/privacy_review/delete_listed_versions.py [--execute]
    PYTHONPATH=. .venv/bin/python scripts/acquisition/one_off/privacy_review/delete_listed_versions.py \
        --list <list.json> --retirement <retired.json> --require-replacements [--execute]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import AwsCli, encoded_json, verify_version, write_once
from scripts.acquisition.transport import CaptureError
from scripts.infrastructure.render_project_config import load_configuration, verify_project_identity
from scripts.process import CompletedProcess, run_command

HERE = Path("data/privacy_review/20260927")
DELETION_LIST = HERE / "deletion_list_excel_and_contacts.json"
GRANT = Path("infra/iam/policies/remediation_delete.json")
RETIREMENT = HERE / "retired_objects.json"


class Stop(RuntimeError):
    """A check failed; nothing after this object is attempted."""


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def approved_prefixes() -> list[str]:
    """The dataset prefixes the temporary grant covers; keys outside them are never touched."""
    resources = json.loads(GRANT.read_text())["Statement"][0]["Resource"]
    return [resource.removeprefix("arn:aws:s3:::${DATA_BUCKET_NAME}/").removesuffix("/*") for resource in resources]


def aws(settings: dict[str, str], *args: str) -> CompletedProcess[str]:
    environment = {key: value for key, value in os.environ.items() if key not in {"AWS_PROFILE", "AWS_DEFAULT_PROFILE"}}
    options = ["--profile", settings["aws_profile"], "--region", settings["aws_region"], "--output", "json"]
    return run_command("aws", [*args, *options], env=environment, timeout=120)


def live_version(settings: dict[str, str], key: str, version: str) -> dict[str, Any] | None:
    """Return the version's metadata, or None when S3 reports it does not exist."""
    result = aws(settings, "s3api", "head-object", "--bucket", settings["data_bucket_name"], "--key", key, "--version-id", version)
    if result.returncode == 0:
        return dict(json.loads(result.stdout))
    if "(404)" in result.stderr or "Not Found" in result.stderr:
        return None
    raise Stop("head-object failed for a listed version; check AWS access")


def remaining(settings: dict[str, str], key: str) -> int:
    """Count versions and delete markers left for exactly this key."""
    result = aws(settings, "s3api", "list-object-versions", "--bucket", settings["data_bucket_name"], "--prefix", key)
    if result.returncode:
        raise Stop("list-object-versions failed; check AWS access")
    listing = json.loads(result.stdout or "{}")
    return sum(item["Key"] == key for item in [*listing.get("Versions", []), *listing.get("DeleteMarkers", [])])


def validate_scope(entries: list[dict[str, Any]], replacements: dict[str, Any] | None, retirement: Path) -> None:
    """Reject ambiguous targets, substituted replacements or an incompatible existing retirement record before deletion."""
    identities = [(entry["key"], entry["version_id"]) for entry in entries]
    if not identities or len(identities) != len(set(identities)) or any(not version or version == "null" for _key, version in identities):
        raise Stop("empty, duplicate or invalid deletion identities")
    if replacements is not None:
        copies = {(item["original"]["key"], item["original"]["version_id"]): item for item in replacements["replacements"]}
        if len(copies) != len(replacements["replacements"]) or set(copies) != set(identities):
            raise Stop("replacement record does not match the exact deletion scope")
        for entry in entries:
            item = copies[(entry["key"], entry["version_id"])]
            if item["original"]["sha256"] != entry["sha256"] or item["original"]["byte_count"] != entry["bytes"] or item["redacted"] != entry["replacement"]:
                raise Stop("replacement binding differs from the approved record")
            original_parts, copy_parts = entry["key"].split("/"), entry["replacement"]["key"].split("/")
            expected = original_parts.copy()
            expected[3] += "_redacted"
            expected[-2] = entry["replacement"]["sha256"]
            if copy_parts != expected or original_parts[-2] != entry["sha256"]:
                raise Stop("replacement key is not the exact derivative of its original")
    if retirement.exists():
        previous = json.loads(retirement.read_text(encoding="utf-8"))["objects"]
        saved = {(item["key"], item["version_id"]): item for item in previous}
        if len(saved) != len(previous) or set(saved) != set(identities):
            raise Stop("existing retirement record covers different versions")
        for entry in entries:
            old = saved[(entry["key"], entry["version_id"])]
            if old.get("status") not in {"deleted_verified", "already_absent"} or old["sha256"] != entry["sha256"] or old["bytes"] != entry["bytes"]:
                raise Stop("existing retirement record conflicts with the deletion list")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--execute", action="store_true", help="permanently delete the listed versions")
    parser.add_argument("--list", type=Path, default=DELETION_LIST, help="deletion list (default: the Excel and contact-table list)")
    parser.add_argument("--retirement", type=Path, default=RETIREMENT, help="write-once retirement record for this list")
    parser.add_argument("--require-replacements", action="store_true", help="every entry must name a live redacted replacement")
    parser.add_argument("--replacements", type=Path, help="immutable original-to-replacement record")
    parser.add_argument("--list-sha256", help="required with --execute: the reviewed deletion-list SHA-256")
    parser.add_argument("--replacements-sha256", help="required with --execute --require-replacements")
    args = parser.parse_args()
    started = datetime.now(UTC)
    record: dict[str, Any] = {"mode": "execute" if args.execute else "dry_run", "started_utc": started.isoformat(), "results": [], "status": "running"}
    output = HERE / f"deletion_run_{started.strftime('%Y%m%dT%H%M%S%fZ')}_{record['mode']}.json"
    try:
        list_digest = sha256(args.list)
        if (args.execute or args.list_sha256) and list_digest != args.list_sha256:
            raise Stop("deletion list hash does not match the reviewed list")
        if args.require_replacements and args.replacements is None:
            raise Stop("replacement record is required")
        replacements = json.loads(args.replacements.read_text(encoding="utf-8")) if args.replacements else None
        if args.replacements and args.execute and sha256(args.replacements) != args.replacements_sha256:
            raise Stop("replacement record hash does not match the reviewed record")
        listing = json.loads(args.list.read_text())
        entries = listing["objects"]
        validate_scope(entries, replacements, args.retirement)
        record["deletion_list_sha256"] = list_digest
        record["replacements_sha256"] = sha256(args.replacements) if args.replacements else None
        settings, _ = load_configuration(Path(".env"))
        verify_project_identity(settings)
        record["identity_verified"] = True
        prefixes = approved_prefixes()
        if args.require_replacements and not all(entry.get("replacement") for entry in entries):
            raise Stop("an entry has no redacted replacement")
        for entry in entries:
            key, version = entry["key"], entry["version_id"]
            if not any(key.startswith(prefix + "/") for prefix in prefixes):
                raise Stop("a listed key is outside the approved prefixes")
            local = Path(entry["local_path"])
            if not local.is_file() or sha256(local) != entry["sha256"]:
                raise Stop("a local original is missing or changed; nothing further is deleted")
            live = live_version(settings, key, version)
            result = {"key": key, "version_id": version, "bytes": entry["bytes"], "sha256": entry["sha256"], "local_original_verified": True}
            if entry.get("replacement"):
                copy = entry["replacement"]
                if copy["key"] == key or not any(copy["key"].startswith(prefix + "_redacted/") for prefix in prefixes):
                    raise Stop("a replacement key is not the redacted copy of its dataset")
                present = live_version(settings, copy["key"], copy["version_id"])
                if present is None or present.get("ContentLength") != copy["byte_count"]:
                    raise Stop("a redacted replacement is not live; its original is kept")
                verify_version(AwsCli(settings), copy["key"], copy["version_id"], copy["sha256"], copy["byte_count"])
                result["replacement_verified_live"] = True
            if live is None:
                record["results"].append({**result, "status": "already_absent"})
                continue
            if live.get("ContentLength") != entry["bytes"]:
                raise Stop("a live version's size differs from the list")
            if not args.execute:
                record["results"].append({**result, "status": "would_delete"})
                continue
            if aws(settings, "s3api", "delete-object", "--bucket", settings["data_bucket_name"], "--key", key, "--version-id", version).returncode:
                raise Stop("delete-object failed; check the temporary grant")
            if live_version(settings, key, version) is not None:
                raise Stop("a deleted version is still readable")
            record["results"].append({**result, "status": "deleted_verified", "deleted_utc": datetime.now(UTC).isoformat()})
        if args.execute:
            record["keys_with_remaining_versions_or_markers"] = sum(remaining(settings, entry["key"]) > 0 for entry in entries)
            if record["keys_with_remaining_versions_or_markers"]:
                raise Stop("versions or delete markers remain for listed keys")
            retired = sorted(record["results"], key=lambda item: item["key"])
            reason = listing.get("reason", "Confidential personal data removed from S3; originals kept locally")
            if not args.retirement.exists():
                write_once(args.retirement, encoded_json({"reason": reason, "objects": retired, "deletion_list_sha256": list_digest}))
        record["status"] = "passed"
    except Stop as error:
        record.update(status="stopped", reason=str(error))
    except (CaptureError, ValueError, KeyError, TypeError, IndexError, OSError) as error:
        record.update(status="stopped", reason=type(error).__name__)
    finally:
        record["finished_utc"] = datetime.now(UTC).isoformat()
        record["counts"] = {
            status: sum(item["status"] == status for item in record["results"]) for status in ("would_delete", "deleted_verified", "already_absent")
        }
        output.write_text(json.dumps(record, indent=1) + "\n")
    sys.stdout.write(json.dumps({"status": record["status"], "mode": record["mode"], "counts": record["counts"], "record": str(output)}) + "\n")
    return 0 if record["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
