"""Permanently delete the listed duplicate S3 object versions after checking each kept twin (failure modes 212 to 217).

Read-only by default: checks the account, that every listed version is live with its recorded size and SHA-256, and
that its kept twin is live with the same SHA-256. ``--execute`` also needs ``--list-sha256`` (the reviewed list's
hash); it deletes exactly the listed (key, version ID) pairs, reads each back, and stops at the first check that fails.
Each run writes a record next to this script. Run from the repository root:

    .venv/bin/python scripts/acquisition/one_off/storage_dedup/delete_duplicate_versions.py [--execute --list-sha256 <hash>]
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

HERE = Path("data/lakehouse_planning/dedup_20261003")
DELETION_LIST = HERE / "deletion_list.json"
GRANT = Path("infra/iam/policies/remediation_delete.json")
DEPLOYMENT = Path("infra/deployment.auto.tfvars.json")


class Stop(RuntimeError):
    """A check failed; nothing after this object is attempted."""


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def approved_prefixes() -> list[str]:
    """The collection prefixes the temporary grant covers; keys outside them are never touched."""
    resources = json.loads(GRANT.read_text())["Statement"][0]["Resource"]
    return [resource.removeprefix("arn:aws:s3:::${DATA_BUCKET_NAME}/").removesuffix("/*") for resource in resources]


def head(client: Any, bucket: str, key: str, version: str) -> dict[str, Any] | None:
    """Return the version's metadata with its stored checksum, or None when S3 reports it does not exist."""
    from botocore.exceptions import ClientError

    try:
        return dict(client.head_object(Bucket=bucket, Key=key, VersionId=version, ChecksumMode="ENABLED"))
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NoSuchVersion", "NotFound"}:
            return None
        raise Stop("head-object failed for a listed version; check AWS access") from error


def matches(meta: dict[str, Any] | None, digest: str, size: int) -> bool:
    """Whether a live version has the recorded size and S3-stored SHA-256."""
    checksum = base64.b64encode(bytes.fromhex(digest)).decode()
    return meta is not None and meta.get("ContentLength") == size and meta.get("ChecksumSHA256") == checksum


def remaining(client: Any, bucket: str, key: str) -> int:
    """Count versions and delete markers left for exactly this key."""
    listing = client.list_object_versions(Bucket=bucket, Prefix=key)
    return sum(item["Key"] == key for item in [*listing.get("Versions", []), *listing.get("DeleteMarkers", [])])


def validate(entries: list[dict[str, Any]], prefixes: list[str]) -> None:
    """Refuse an empty, repeated or out-of-scope list, or an entry whose kept twin is itself listed."""
    identities = [(entry["key"], entry["version_id"]) for entry in entries]
    if not identities or len(identities) != len(set(identities)) or any(not version or version == "null" for _, version in identities):
        raise Stop("empty, duplicate or invalid deletion identities")
    listed = set(identities)
    for entry in entries:
        if not any(entry["key"].startswith(prefix + "/") for prefix in prefixes):
            raise Stop("a listed key is outside the approved prefixes")
        kept = (entry["kept"]["key"], entry["kept"]["version_id"])
        if kept in listed or kept == (entry["key"], entry["version_id"]):
            raise Stop("a kept twin is itself on the deletion list")


def main() -> int:
    import boto3

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--execute", action="store_true", help="permanently delete the listed versions")
    parser.add_argument("--list-sha256", help="required with --execute: the reviewed deletion-list SHA-256")
    args = parser.parse_args()
    started = datetime.now(UTC)
    record: dict[str, Any] = {"mode": "execute" if args.execute else "dry_run", "started_utc": started.isoformat(), "results": [], "status": "running"}
    output = HERE / f"deletion_run_{started.strftime('%Y%m%dT%H%M%S%fZ')}_{record['mode']}.json"
    try:
        digest = file_sha256(DELETION_LIST)
        record["deletion_list_sha256"] = digest
        if args.execute and digest != args.list_sha256:
            raise Stop("deletion list hash does not match the reviewed list")
        entries = json.loads(DELETION_LIST.read_text())["objects"]
        validate(entries, approved_prefixes())
        settings = json.loads(DEPLOYMENT.read_text())
        session = boto3.Session(profile_name=settings["aws_profile"], region_name=settings["aws_region"])
        if session.client("sts").get_caller_identity()["Account"] != str(settings["expected_account_id"]):
            raise Stop("the AWS profile is not in the project account")
        record["identity_verified"] = True
        client, bucket = session.client("s3"), settings["data_bucket_name"]
        for entry in entries:
            key, version, sha, size = entry["key"], entry["version_id"], entry["sha256"], entry["byte_count"]
            kept = entry["kept"]
            result = {"key": key, "version_id": version, "sha256": sha, "bytes": size, "kept_key": kept["key"], "kept_version_id": kept["version_id"]}
            if not matches(head(client, bucket, kept["key"], kept["version_id"]), sha, size):
                raise Stop("a kept twin is missing or differs; its duplicate is kept")
            result["kept_verified"] = True
            live = head(client, bucket, key, version)
            if live is None:
                record["results"].append({**result, "status": "already_absent"})
                continue
            if not matches(live, sha, size):
                raise Stop("a listed version's size or checksum differs from the list")
            if not args.execute:
                record["results"].append({**result, "status": "would_delete"})
                continue
            client.delete_object(Bucket=bucket, Key=key, VersionId=version)
            if head(client, bucket, key, version) is not None:
                raise Stop("a deleted version is still readable")
            record["results"].append({**result, "status": "deleted_verified", "deleted_utc": datetime.now(UTC).isoformat()})
        if args.execute:
            record["keys_with_remaining_versions_or_markers"] = sum(remaining(client, bucket, entry["key"]) > 0 for entry in entries)
            if record["keys_with_remaining_versions_or_markers"]:
                raise Stop("versions or delete markers remain for listed keys")
        record["status"] = "passed"
    except Stop as error:
        record.update(status="stopped", reason=str(error))
    except (ValueError, KeyError, TypeError, OSError) as error:
        record.update(status="stopped", reason=type(error).__name__)
    finally:
        record["finished_utc"] = datetime.now(UTC).isoformat()
        statuses = ("would_delete", "deleted_verified", "already_absent")
        record["counts"] = {status: sum(item["status"] == status for item in record["results"]) for status in statuses}
        output.write_text(json.dumps(record, indent=1) + "\n")
    sys.stdout.write(json.dumps({"status": record["status"], "mode": record["mode"], "counts": record["counts"], "record": str(output)}) + "\n")
    return 0 if record["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
