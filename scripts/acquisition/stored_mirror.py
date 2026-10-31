"""Give the run 3 task the stored captures it compares against, without a local data folder and without a secret.

``build`` runs once on the owner's Mac. It copies each active unit's stored receipt (and its job file) into a mirror folder that the task receives as
digest-checked inputs, and it writes an index that names, for each stored payload, the exact S3 key and version that holds it. The payload bytes
are not copied. In the task, ``Mirror.materialize`` reads one payload by its exact key and version with the task role, and keeps it only when its
SHA-256 and length match. An exact match is decided from the receipt digest, so most units read nothing from S3.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import tempfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from scripts.acquisition import unit_classification
from scripts.acquisition.data_paths import current
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.transport import CaptureError

ACTIVE = ("api", "direct_download", "approved_manual")
INVENTORY_COLUMNS = ("source_id", "snapshot_id", "s3_key", "s3_version_id", "sha256")
MIRROR_VARIABLE = "HAI_STORED_MIRROR"
INDEX_VARIABLE = "HAI_STORED_INDEX"
REQUIRED_ENVIRONMENT = (MIRROR_VARIABLE, INDEX_VARIABLE, "EXPECTED_ACCOUNT_ID", "TASK_ROLE_NAME", "AWS_REGION")
CHUNK = 1024 * 1024


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_inventory(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = [column for column in INVENTORY_COLUMNS if column not in (reader.fieldnames or [])]
        if missing:
            raise CaptureError(f"The inventory lacks the columns {', '.join(missing)}.")
        rows = list(reader)
    if not rows:
        raise CaptureError("The inventory is empty, so it cannot locate any stored payload.")
    return rows


def copy_once(source: Path, target: Path) -> None:
    write_once(target, source.read_bytes())


QUEUE_RECORDS = ("queue.json", "queue.lock.json", "controls.json", "independent_review.json", "scope.json", unit_classification.NAME)


def copy_run_records(queue_path: Path, base: Path, mirror: Path) -> None:
    """Carry the queue, its lock, its frozen controls and review, its scope, its unit classification and every frozen input.

    The frozen inputs are checked against the queue's digests.
    """
    folder = queue_path.parent
    for name in QUEUE_RECORDS:
        if (folder / name).is_file() and folder.is_relative_to(base):
            copy_once(folder / name, mirror / (folder / name).relative_to(base))
    for relative, expected in json.loads(queue_path.read_text(encoding="utf-8")).get("inputs_sha256", {}).items():
        if relative.endswith(".md"):
            continue
        path = base / current(relative) if "/" in relative else base / "data/acquisition_planning/full_redownload_20260929" / relative
        if sha256_of(path.read_bytes()) != expected:
            raise CaptureError(f"The frozen input {relative} differs from the queue's digest.")
        copy_once(path, mirror / path.relative_to(base))
    from scripts.acquisition.collect_mmd_api import TEMPLATE

    if (base / TEMPLATE).is_file():
        copy_once(base / TEMPLATE, mirror / TEMPLATE)


def build(queue_path: Path, base: Path, inventory_path: Path, output: Path, bucket: str) -> dict[str, int]:
    """Write the mirror folder, its index and its manifest under ``output``. A second build with the same inputs changes nothing."""
    queue = json.loads(queue_path.read_text(encoding="utf-8"))["queue"]
    rows = read_inventory(inventory_path)
    by_digest: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        by_digest.setdefault(row["sha256"], []).append(row)
    mirror = output / "mirror"
    copy_run_records(queue_path, base, mirror)
    objects: dict[str, dict[str, Any]] = {}
    unavailable: list[str] = []
    units = 0
    for unit in sorted((item for item in queue if item["disposition"] in ACTIVE), key=lambda item: item["snapshot_id"]):
        relative = current(unit["receipt"])
        receipt_path = base / relative
        body = receipt_path.read_bytes()
        if sha256_of(body) != unit["receipt_sha256"]:
            raise CaptureError(f"The stored receipt of {unit['snapshot_id']} differs from the queue's digest.")
        copy_once(receipt_path, mirror / relative)
        job = next((parent / "job.json" for parent in receipt_path.parents if (parent / "job.json").is_file()), None)
        if job is not None and job.is_relative_to(base):
            copy_once(job, mirror / job.relative_to(base))
        for artifact in json.loads(body)["artifacts"]:
            if "storage_path" not in artifact:
                continue
            payload = (Path(relative).parent / artifact["storage_path"]).as_posix()
            candidates = sorted(by_digest.get(artifact["sha256"], []), key=lambda row: (row["snapshot_id"] != unit["snapshot_id"], row["s3_key"]))
            if not candidates:
                unavailable.append(payload)
                continue
            row = candidates[0]
            objects[payload] = {
                "bucket": bucket,
                "key": row["s3_key"],
                "version_id": row["s3_version_id"],
                "sha256": artifact["sha256"],
                "byte_count": artifact["byte_count"],
            }
        units += 1
    index = {"bucket": bucket, "objects": dict(sorted(objects.items())), "unavailable": sorted(set(unavailable))}
    write_once(output / "stored_index.json", encoded_json(index))
    files = [
        {"path": path.relative_to(mirror).as_posix(), "sha256": sha256_of(path.read_bytes()), "bytes": path.stat().st_size}
        for path in sorted(mirror.rglob("*"))
        if path.is_file()
    ]
    manifest = {"files": files, "index_sha256": sha256_of(encoded_json(index))}
    write_once(output / "mirror_manifest.json", encoded_json(manifest))
    return {"units": units, "payloads_indexed": len(objects), "payloads_unavailable": len(set(unavailable)), "mirror_files": len(files)}


class Mirror:
    """Reads stored payloads by exact key and version, and keeps one only when its bytes match the receipt."""

    def __init__(self, root: Path, index: dict[str, Any], client: Any) -> None:
        self.root = root.resolve()
        self.objects: dict[str, dict[str, Any]] = index["objects"]
        self.client = client

    def materialize(self, path: Path) -> bool:
        """Put the stored payload at ``path`` if the index names it. Return whether the file is there."""
        target = path.resolve()
        if not target.is_relative_to(self.root):
            raise CaptureError("The stored payload path is outside the mirror.")
        if target.is_file():
            return True
        entry = self.objects.get(target.relative_to(self.root).as_posix())
        if entry is None:
            return False
        answer = self.client.get_object(Bucket=entry["bucket"], Key=entry["key"], VersionId=entry["version_id"])
        if answer.get("VersionId") not in (None, entry["version_id"]):
            raise CaptureError("S3 answered with another version than the one the index names.")
        target.parent.mkdir(parents=True, exist_ok=True)
        digest, size = hashlib.sha256(), 0
        with tempfile.NamedTemporaryFile(dir=target.parent, prefix=".fetch_", delete=False) as handle:
            temporary = Path(handle.name)
            try:
                for chunk in answer["Body"].iter_chunks(CHUNK):
                    handle.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
            except BaseException:
                temporary.unlink(missing_ok=True)
                raise
        if digest.hexdigest() != entry["sha256"] or size != entry["byte_count"]:
            temporary.unlink(missing_ok=True)
            raise CaptureError("The stored payload read from S3 differs from its receipt digest or length.")
        os.replace(temporary, target)
        return True


def require_task_identity(sts: Any, account: str, role_name: str) -> None:
    """Accept only the expected task role in the expected account, never a user or another role."""
    answer = sts.get_caller_identity()
    if answer.get("Account") != account or not re.fullmatch(
        rf"arn:aws:sts::{re.escape(account)}:assumed-role/{re.escape(role_name)}/[\w+=,.@-]+", answer.get("Arn", "")
    ):
        raise CaptureError("The task identity is not the expected publisher role.")


def default_client(service: str, region: str) -> Any:
    import boto3

    session = boto3.session.Session(region_name=region)
    if service == "sts":
        return session.client("sts")
    if service == "s3":
        return session.client("s3")
    raise CaptureError(f"The task makes no {service} client.")


def from_environment(environment: Mapping[str, str], factory: Callable[[str, str], Any] = default_client) -> Mirror:
    """Build the task's mirror from its environment. The S3 client is made only after the identity check passes."""
    missing = [name for name in REQUIRED_ENVIRONMENT if not environment.get(name)]
    if missing:
        raise CaptureError(f"The task environment lacks {', '.join(missing)}.")
    region = environment["AWS_REGION"]
    require_task_identity(factory("sts", region), environment["EXPECTED_ACCOUNT_ID"], environment["TASK_ROLE_NAME"])
    index = json.loads(Path(environment[INDEX_VARIABLE]).read_text(encoding="utf-8"))
    return Mirror(Path(environment[MIRROR_VARIABLE]), index, factory("s3", region))


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build the stored capture mirror the run 3 task receives as inputs.")
    parser.add_argument("--queue", required=True, type=Path)
    parser.add_argument("--base", required=True, type=Path, help="the repository folder that holds the stored captures under data/")
    parser.add_argument("--inventory", required=True, type=Path, help="CSV of every stored object: source_id, snapshot_id, s3_key, s3_version_id, sha256")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        summary = build(args.queue, args.base, args.inventory, args.output, args.bucket)
    except (CaptureError, OSError, KeyError, ValueError) as error:
        parser.error(str(error))
    sys.stdout.write(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
