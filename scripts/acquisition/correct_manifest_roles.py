"""Write corrected storage manifests that change only the named object roles, beside the manifests they supersede.

Failure modes 242 to 248 (data/acquisition_planning/brz011_manifest_correction_20261004/failure_modes.md). The committed
specification names each manifest by key, version and SHA-256 and each change by object key, version, old role and new
role. A corrected manifest keeps every other field, adds ``supersedes`` and ``correction``, and is stored under its own
SHA-256 in the same manifests folder; the original is never replaced. Read-only by default: a dry run writes the
corrected manifests locally. ``--store`` needs the specification's SHA-256, reads each written version back and writes
a local record that the storage check counts. Run from the repository root:

    .venv/bin/python -m scripts.acquisition.correct_manifest_roles [--store --specification-sha256 <hash>]
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import AwsCli, StorageError, encoded_json, fingerprint, store_file, write_once
from scripts.acquisition.transport import CaptureError
from scripts.infrastructure.render_project_config import REPO_ROOT, load_configuration, verify_project_identity

SPECIFICATION = REPO_ROOT / "config/acquisition/manifest_corrections.json"
OUTPUT = REPO_ROOT / "data/datasets/manifest_corrections"
ROLES = {"data", "dictionary", "methodology", "reference", "layout"}


def read_version(client: AwsCli, key: str, version: str) -> bytes:
    """Return the bytes of one exact S3 version."""
    with tempfile.TemporaryDirectory(prefix="manifest_correction_") as temporary:
        path = Path(temporary) / "manifest.json"
        result = client.call("s3api", "get-object", ["--key", key, "--version-id", version, str(path)])
        if result.get("VersionId") != version:
            raise CaptureError("S3 returned a different manifest version.")
        return path.read_bytes()


def corrected(original: bytes, correction: dict[str, Any], issue: str, decision: str) -> dict[str, Any]:
    """Return the manifest with only the named roles changed and its supersession recorded [243] [244]."""
    named = correction["manifest"]
    if hashlib.sha256(original).hexdigest() != named["sha256"]:
        raise CaptureError(f"{named['key']}: the stored manifest's SHA-256 differs from the specification.")
    manifest = json.loads(original)
    if "supersedes" in manifest or "correction" in manifest:
        raise CaptureError(f"{named['key']}: the manifest is already a correction.")
    result = copy.deepcopy(manifest)
    for change in correction["changes"]:
        if change["to_role"] not in ROLES or change["from_role"] == change["to_role"]:
            raise CaptureError(f"{change['key']}: the new role is not an allowed storage role.")
        found = [item for item in result["objects"] if (item["object"].get("key"), item["object"].get("version_id")) == (change["key"], change["version_id"])]
        if not found:
            raise CaptureError(f"{change['key']}: the object is not in the manifest.")
        matching = [item for item in found if item.get("role") == change["from_role"]]
        if len(matching) != 1:
            raise CaptureError(f"{change['key']}: the manifest does not list the object once with the old role.")
        matching[0]["role"] = change["to_role"]
    result["supersedes"] = {key: named[key] for key in ("key", "version_id", "sha256")}
    result["correction"] = {"issue": issue, "decision": decision, "reason": correction["reason"], "changes": correction["changes"]}
    return result


def run(specification: Path, client: AwsCli, output: Path, store: bool, spec_sha256: str | None) -> dict[str, Any]:
    """Build every corrected manifest; with store, write each to S3 once, verify it and record it locally."""
    digest = fingerprint(specification)[0]
    if store and spec_sha256 != digest:
        raise CaptureError("--store needs the reviewed specification's SHA-256.")
    spec = json.loads(specification.read_text(encoding="utf-8"))
    if spec.get("kind") != "storage_manifest_corrections" or not spec.get("corrections"):
        raise CaptureError("Not a storage manifest correction specification.")
    keys = [item["manifest"]["key"] for item in spec["corrections"]]
    if len(keys) != len(set(keys)):
        raise CaptureError("A manifest is named twice in the specification.")
    built = []
    # Build and check every correction before any write [244].
    for item in spec["corrections"]:
        named = item["manifest"]
        body = encoded_json(corrected(read_version(client, named["key"], named["version_id"]), item, spec["issue"], spec["decision"]))
        sha = hashlib.sha256(body).hexdigest()
        prefix, _sha, name = named["key"].rsplit("/", 2)
        if name != "manifest.json":
            raise CaptureError(f"{named['key']}: not a storage manifest key.")
        local = output / named["key"].rsplit("/", 2)[0].split("/manifests/", 1)[1] / sha / "manifest.json"
        write_once(local, body)
        built.append({"supersedes": named, "key": f"{prefix}/{sha}/manifest.json", "sha256": sha, "local_path": str(local)})
    results = []
    for entry in built:
        result = {**entry, "created": False}
        if store:
            record, created = store_file(client, Path(entry["local_path"]), entry["key"])
            result.update(object=record, created=created)
        results.append(result)
    summary: dict[str, Any] = {"mode": "store" if store else "dry_run", "specification_sha256": digest, "corrections": results}
    if store:
        # The record carries bucket, key, version and byte count, so the storage check counts the objects [247].
        record_path = output / f"correction_record_{digest[:16]}.json"
        write_once(
            record_path,
            encoded_json(
                {
                    "kind": "storage_manifest_correction_record",
                    "issue": spec["issue"],
                    "specification_sha256": digest,
                    "corrections": [{"supersedes": item["supersedes"], "object": item["object"]} for item in results],
                }
            ),
        )
        summary["record_path"] = str(record_path)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--specification", type=Path, default=SPECIFICATION)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--store", action="store_true", help="write the corrected manifests to S3")
    parser.add_argument("--specification-sha256", help="required with --store")
    args = parser.parse_args()
    try:
        settings, _ = load_configuration(REPO_ROOT / ".env")
        verify_project_identity(settings)
        summary = run(args.specification, AwsCli(settings), args.output, args.store, args.specification_sha256)
    except (CaptureError, StorageError, ValueError, KeyError, OSError) as error:
        parser.error(str(error))
    for item in summary["corrections"]:
        item.pop("object", None)
    sys.stdout.write(json.dumps(summary, indent=1) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
