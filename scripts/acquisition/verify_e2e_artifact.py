"""Verify copied live E2E evidence offline without changing its artifact directory."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import sys
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO


class ArtifactError(ValueError):
    """Invalid or inconsistent local E2E evidence."""

    pass


def require(condition: bool, message: str) -> None:
    """Raise the module-specific validation error when the required condition is false."""
    if not condition:
        raise ArtifactError(message)


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Return a JSON object from key/value pairs, rejecting duplicate keys."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        require(key not in result, "Duplicate JSON keys are not permitted.")
        result[key] = value
    return result


def reject_constant(value: str) -> None:
    """Reject non-finite numeric constants encountered while decoding JSON."""
    raise ArtifactError("Non-finite JSON numbers are not permitted.")


def finite_float(value: str) -> float:
    """Decode a JSON float, rejecting non-finite results."""
    result = float(value)
    require(math.isfinite(result), "Non-finite JSON numbers are not permitted.")
    return result


def relative_path(value: Any) -> PurePosixPath:
    """Validate and return a canonical relative POSIX evidence path."""
    require(isinstance(value, str) and bool(value) and "\\" not in value and "\0" not in value, "Evidence paths must be relative POSIX paths.")
    path = PurePosixPath(value)
    require(
        bool(path.parts) and not path.is_absolute() and ".." not in path.parts and ":" not in path.parts[0] and path.as_posix() == value,
        "Evidence paths must be canonical and cannot escape the artifact root.",
    )
    return path


def open_regular(root_fd: int, path: PurePosixPath) -> BinaryIO:
    """Open an artifact-relative regular file without following symlinks."""
    # Resolve each component through directory descriptors so symlinks cannot redirect reads.
    directory = os.dup(root_fd)
    try:
        for part in path.parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    finally:
        os.close(directory)
    try:
        require(stat.S_ISREG(os.fstat(descriptor).st_mode), "Evidence must be a regular file.")
        return os.fdopen(descriptor, "rb")
    except BaseException:
        os.close(descriptor)
        raise


def validate_manifest(manifest: Any) -> list[tuple[PurePosixPath, dict[str, Any]]]:
    """Validate a successful E2E manifest and return its unique hashed evidence paths."""
    require(isinstance(manifest, dict), "The artifact manifest must be a JSON object.")
    require(type(manifest.get("schema_version")) is int and manifest["schema_version"] == 1, "Expected artifact schema_version 1.")
    require(manifest.get("status") == "passed" and manifest.get("kind") == "live_acquisition_e2e", "Expected a passed live_acquisition_e2e artifact.")
    require(isinstance(manifest.get("run_id"), str) and bool(manifest["run_id"].strip()), "A nonempty run_id is required.")
    require(isinstance(manifest.get("commands"), list), "commands must be a list.")
    require(isinstance(manifest.get("aws_writes"), dict), "aws_writes must be an object.")
    require(manifest.get("model_eligible") is False, "model_eligible must be boolean false.")
    assertions = manifest.get("assertions")
    require(isinstance(assertions, list) and bool(assertions), "At least one assertion is required.")
    for assertion in assertions:
        require(
            isinstance(assertion, dict) and isinstance(assertion.get("name"), str) and bool(assertion["name"].strip()),
            "Each assertion requires a nonempty name.",
        )
        require(assertion.get("passed") is True, "Every assertion must have passed=true.")
    files = manifest.get("files")
    require(isinstance(files, list) and bool(files), "At least one copied evidence file is required.")
    seen: set[PurePosixPath] = set()
    entries = []
    for record in files:
        require(isinstance(record, dict), "Each evidence file record must be an object.")
        path = relative_path(record.get("path"))
        require(path not in seen and path != PurePosixPath("artifact.json"), "Duplicate paths and manifest self-references are not permitted.")
        require(
            isinstance(record.get("sha256"), str) and re.fullmatch(r"[a-fA-F0-9]{64}", record["sha256"]) is not None,
            "Each evidence file requires a 64-digit hexadecimal SHA-256.",
        )
        require(type(record.get("byte_count")) is int and record["byte_count"] >= 0, "Each evidence file requires a nonnegative integer byte_count.")
        seen.add(path)
        entries.append((path, record))
    return entries


def verify_workflow(root_fd: int, manifest: dict[str, Any]) -> dict[str, Any]:
    """Verify recorded acquisition, archive placement and replay invariants from hashed evidence."""
    paths = {entry["path"] for entry in manifest["files"]}

    def document(name: str) -> Any:
        """Read a named, hash-listed workflow document without following symlinks."""
        require(name in paths, "Workflow document is missing from the hashed evidence.")
        with open_regular(root_fd, relative_path(name)) as handle:
            return json.load(handle, object_pairs_hook=unique_object, parse_constant=reject_constant, parse_float=finite_float)

    before, after = document("before.json"), document("after_replay.json")
    require(document("after_capture.json") == after, "Replay changed object inventory or versions.")
    result, baseline = document("batch_result.json"), document("baseline_reconciliations.json")
    expected_status = {"e2e_csv": "stored_unvalidated", "e2e_archive": "stored_unvalidated", "e2e_rejected_archive": "evidence_only"}
    require(
        len(result["events"]) == 3 and {event["job_id"]: event["status"] for event in result["events"]} == expected_status and result["unprocessed_jobs"] == 0,
        "Recorded workflow scenarios did not all complete as expected.",
    )
    expected_commands = {"capture_store": 1, "replay_e2e_csv": 0, "replay_e2e_archive": 0, "replay_e2e_rejected_archive": 0}
    require(
        len(manifest["commands"]) == 4 and {item["name"]: item["returncode"] for item in manifest["commands"]} == expected_commands,
        "Recorded command exit statuses differ from the live workflow contract.",
    )
    reconciliations = {}
    stored: dict[tuple[str, str], dict[str, Any]] = {}
    for job in expected_status:
        matches = [
            name
            for name in paths
            if name.startswith("state/") and f"/jobs/{job}/captures/" in name and PurePosixPath(name).name == "s3_collections_reconciliation.json"
        ]
        require(len(matches) == 1, "Expected one complete reconciliation per E2E scenario.")
        reconciliation = document(matches[0])
        reconciliations[job] = reconciliation
        for entry in reconciliation["objects"] + reconciliation["manifests"]:
            item = entry["object"]
            identity = (item["key"], item["version_id"])
            require(identity not in stored or stored[identity] == item, "Conflicting recorded S3 versions.")
            stored[identity] = item
    original_versions = {(item["Key"], item["VersionId"]): item for item in before["versions"]}
    final_versions = {(item["Key"], item["VersionId"]): item for item in after["versions"]}
    require(set(final_versions) == set(original_versions) | set(stored), "Unexplained or missing S3 object versions.")
    require(all(final_versions[identity] == record for identity, record in original_versions.items()), "A prior version changed.")
    require(before["delete_markers"] == after["delete_markers"], "Delete markers changed.")
    for identity, record in stored.items():
        require(
            final_versions[identity]["IsLatest"] is True and final_versions[identity]["Size"] == record["byte_count"],
            "A reconciled version is not current or has the wrong size.",
        )

    def members(reconciliation: dict) -> set[tuple]:
        """Return member placement, byte identity and hold tuples for comparison."""
        result = set()
        for entry in reconciliation["objects"]:
            if "archive_member" not in entry:
                continue
            item = entry["object"]
            parts = item["key"].split("/")
            destination = item["key"] if parts[2] in {"datasets", "references"} else "/".join(parts[:3])
            result.add((entry["archive_member"], entry["dataset_id"], entry["role"], item["sha256"], item["byte_count"], entry.get("hold_reason"), destination))
        return result

    previous_members = members(baseline["archive"])
    require(
        bool(previous_members) and members(reconciliations["e2e_archive"]) == previous_members,
        "Archive member placement, bytes or holds differ from the pinned baseline.",
    )
    before_keys = {item["Key"] for item in before["objects"]}
    after_keys = {item["Key"] for item in after["objects"]}
    require(after_keys == before_keys | {identity[0] for identity in stored}, "Unexplained or missing current S3 objects.")
    writes = manifest["aws_writes"]
    require(
        writes
        == {
            "new_objects": len(after_keys - before_keys),
            "new_versions": len(final_versions) - len(original_versions),
            "replay_new_objects": 0,
            "deletions": 0,
        },
        "Recorded AWS write counts differ from the evidence.",
    )
    return {"workflow_invariants_verified": True, "archive_member_assignments_verified": len(previous_members), "s3_versions_reconciled": len(stored)}


def verify_artifact(artifact: Path) -> dict[str, Any]:
    """Verify local artifact bytes and workflow evidence without contacting live services."""
    require(artifact.name == "artifact.json", "Select the runner's artifact.json manifest.")
    root_fd = os.open(artifact.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        with open_regular(root_fd, PurePosixPath(artifact.name)) as handle:
            encoded = handle.read()
        manifest = json.loads(encoded.decode("utf-8"), object_pairs_hook=unique_object, parse_constant=reject_constant, parse_float=finite_float)
        entries = validate_manifest(manifest)
        total_bytes = 0
        for path, record in entries:
            digest, count = hashlib.sha256(), 0
            with open_regular(root_fd, path) as handle:
                before = os.fstat(handle.fileno())
                for block in iter(lambda: handle.read(1024**2), b""):
                    digest.update(block)
                    count += len(block)
                after = os.fstat(handle.fileno())
            require(
                (before.st_size, before.st_mtime_ns, before.st_ctime_ns) == (after.st_size, after.st_mtime_ns, after.st_ctime_ns),
                f"Evidence changed during verification: {path}",
            )
            require(count == record["byte_count"], f"Evidence byte count differs: {path}")
            require(digest.hexdigest() == record["sha256"].lower(), f"Evidence SHA-256 differs: {path}")
            total_bytes += count
        workflow = verify_workflow(root_fd, manifest)
    finally:
        os.close(root_fd)
    return {
        "status": "artifact_verified",
        "schema_version": 1,
        "kind": "live_acquisition_e2e",
        "run_id": manifest["run_id"],
        "artifact_sha256": hashlib.sha256(encoded).hexdigest(),
        "assertions_verified": len(manifest["assertions"]),
        "files_verified": len(entries),
        "bytes_verified": total_bytes,
        "live_services_rechecked": False,
        "model_eligible": False,
        "verifier_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        **workflow,
    }


def main() -> int:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", required=True, type=Path, help="Path to the saved artifact.json manifest.")
    parser.add_argument("--output", type=Path, help="Optional new summary file outside the artifact directory.")
    args = parser.parse_args()
    try:
        if args.output is not None:
            require(not args.output.resolve().is_relative_to(args.artifact.parent.resolve()), "The summary output must be outside the artifact root.")
            require(not args.output.exists() and not args.output.is_symlink(), "The summary output must be a new file.")
        summary = verify_artifact(args.artifact)
        encoded = json.dumps(summary, indent=2) + "\n"
        if args.output is not None:
            require(not args.output.resolve().is_relative_to(args.artifact.parent.resolve()), "The summary output must be outside the artifact root.")
            descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(encoded)
    except (ValueError, OSError, RuntimeError, KeyError, TypeError, IndexError) as error:
        reason = str(error) if isinstance(error, ArtifactError) else f"Evidence could not be processed ({type(error).__name__})."
        sys.stdout.write(str(json.dumps({"status": "invalid_artifact", "reason": reason, "live_services_rechecked": False, "model_eligible": False})) + "\n")
        return 1
    sys.stdout.write(str(encoded) + "")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
