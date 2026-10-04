"""Synthetic E2E for correct_manifest_roles: corrected manifests written beside the originals in a fake S3, no AWS.

Failure modes 242 to 248 (data/acquisition_planning/brz011_manifest_correction_20261004/failure_modes.md).

.venv/bin/python -m scripts.acquisition.run_manifest_correction_e2e
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import shutil
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition import correct_manifest_roles as correction
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.tests.test_s3_store import SETTINGS, FakeS3
from scripts.acquisition.transport import CaptureError
from scripts.infrastructure.render_project_config import REPO_ROOT

REPORT_ROOT = REPO_ROOT / "data/e2e/manifest_correction"
SNAPSHOT = "HPSA__20260101T000000Z__0123456789abcdef0123456789abcdef"
COLLECTION = "hrsa/shortage_areas"


def stored(client: FakeS3, key: str, body: bytes) -> dict[str, Any]:
    """Seed one object in the fake S3 and return its version record."""
    version = f"version_{len(client.objects) + 1}"
    checksum = base64.b64encode(hashlib.sha256(body).digest()).decode()
    client.objects[key] = {"VersionId": version, "ContentLength": len(body), "ChecksumSHA256": checksum, "ServerSideEncryption": "AES256", "body": body}
    return {"bucket": SETTINGS["data_bucket_name"], "key": key, "version_id": version, "sha256": hashlib.sha256(body).hexdigest(), "byte_count": len(body)}


def original(client: FakeS3) -> tuple[dict[str, Any], dict[str, Any]]:
    """Store a detail file under the dictionary role and its manifest; return the manifest record and the file record."""
    body = b"HPSA Name,HPSA ID,\r\nExample Area,1234567890,\r\n"
    digest = hashlib.sha256(body).hexdigest()
    data = stored(client, f"{COLLECTION}/references/capture_id={SNAPSHOT}/{digest}/DETAIL.csv", body)
    manifest = {
        "storage_contract_version": "4.0.0",
        "publisher": "hrsa",
        "collection": "shortage_areas",
        "dataset_id": "hpsa",
        "snapshot_id": SNAPSHOT,
        "source_id": "HPSA",
        "release_id": SNAPSHOT,
        "model_eligible": False,
        "objects": [
            {"artifact_id": "artifact_0001", "dataset_id": "hpsa", "role": "dictionary", "storage_path": "raw/DETAIL.csv", "object": data},
            {"artifact_id": "artifact_0002", "dataset_id": "hpsa", "role": "transport_audit", "storage_path": "audit/DETAIL.csv", "object": data},
        ],
    }
    body = encoded_json(manifest)
    key = f"{COLLECTION}/manifests/capture_id={SNAPSHOT}/{SNAPSHOT}/{hashlib.sha256(body).hexdigest()}/manifest.json"
    return stored(client, key, body), data


def specification(path: Path, manifest: dict[str, Any], data: dict[str, Any], **change: str) -> Path:
    """Write a correction specification naming one role change."""
    spec = {
        "kind": "storage_manifest_corrections",
        "issue": "BRZ-011",
        "decision": "Synthetic owner decision.",
        "corrections": [
            {
                "manifest": {key: manifest[key] for key in ("key", "version_id", "sha256")},
                "reason": "The detail file holds the designations themselves.",
                "changes": [{"key": data["key"], "version_id": data["version_id"], "from_role": "dictionary", "to_role": "data", **change}],
            }
        ],
    }
    write_once(path, encoded_json(spec))
    return path


def refused(action: Callable[[], object], fragment: str) -> bool:
    try:
        action()
    except CaptureError as error:
        return fragment in str(error)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, help="report path; default data/e2e/manifest_correction/report_<time>.json")
    args = parser.parse_args()
    root = Path(tempfile.mkdtemp(prefix="manifest_correction_e2e_"))
    results: dict[str, bool] = {}
    try:
        client = FakeS3()
        manifest, data = original(client)
        spec = specification(root / "spec.json", manifest, data)
        spec_sha = fingerprint(spec)[0]
        before = dict(client.objects)
        # [248] A dry run writes the corrected manifest locally and nothing to S3.
        dry = correction.run(spec, client, root / "dry", store=False, spec_sha256=None)
        local = json.loads(Path(dry["corrections"][0]["local_path"]).read_text())
        results["dry_run_writes_nothing_to_s3"] = client.objects == before and not any(call[0] == "put-object" for call in client.calls)
        # [243] Only the named role changes; the original is named by key, version and SHA-256.
        old = json.loads(client.objects[manifest["key"]]["body"])
        roles = [item["role"] for item in local["objects"]]
        same = {key: value for key, value in local.items() if key not in {"objects", "supersedes", "correction"}} == {
            key: value for key, value in old.items() if key != "objects"
        }
        unchanged = all(
            {k: v for k, v in new.items() if k != "role"} == {k: v for k, v in previous.items() if k != "role"}
            for new, previous in zip(local["objects"], old["objects"], strict=True)
        )
        results["only_named_role_changes"] = roles == ["data", "transport_audit"] and same and unchanged
        results["supersedes_names_original"] = local["supersedes"] == {key: manifest[key] for key in ("key", "version_id", "sha256")}
        results["correction_recorded"] = local["correction"]["issue"] == "BRZ-011" and local["correction"]["changes"][0]["to_role"] == "data"
        # [248] Writing needs the specification's SHA-256.
        results["store_without_hash_refused"] = refused(lambda: correction.run(spec, client, root / "stored", store=True, spec_sha256=None), "SHA-256")
        results["store_with_wrong_hash_refused"] = refused(lambda: correction.run(spec, client, root / "stored", store=True, spec_sha256="0" * 64), "SHA-256")
        results["refusals_write_nothing"] = client.objects == before
        # [242] [246] [247] The corrected manifest is a new content-addressed object, read back, and recorded locally.
        first = correction.run(spec, client, root / "stored", store=True, spec_sha256=spec_sha)
        written = first["corrections"][0]["object"]
        prefix = manifest["key"].rsplit("/", 2)[0]
        results["new_key_beside_original"] = written["key"] == f"{prefix}/{written['sha256']}/manifest.json" and written["key"] != manifest["key"]
        results["original_unchanged"] = client.objects[manifest["key"]] == before[manifest["key"]]
        results["written_once_and_read_back"] = (
            [call[0] for call in client.calls].count("put-object") == 1
            and written["verification"] == "version_get_sha256_and_length_match"
            and first["corrections"][0]["created"]
        )
        record = json.loads(Path(first["record_path"]).read_text())
        results["local_record_names_object"] = record["corrections"][0]["object"] == written and record["specification_sha256"] == spec_sha
        # [245] A rerun finds the same key and verifies it instead of writing.
        second = correction.run(spec, client, root / "stored", store=True, spec_sha256=spec_sha)
        results["rerun_writes_nothing"] = (
            [call[0] for call in client.calls].count("put-object") == 1
            and second["corrections"][0]["object"] == written
            and not second["corrections"][0]["created"]
        )
        # [244] The wrong manifest, a missing object or a different old role is refused before any write.
        bad = dict(manifest, sha256="1" * 64)
        results["wrong_manifest_hash_refused"] = refused(
            lambda: correction.run(specification(root / "bad_hash.json", bad, data), client, root / "bad1", store=False, spec_sha256=None), "SHA-256"
        )
        results["missing_object_refused"] = refused(
            lambda: correction.run(
                specification(root / "missing.json", manifest, data, key=data["key"].replace("DETAIL", "OTHER")),
                client,
                root / "bad2",
                store=False,
                spec_sha256=None,
            ),
            "not in the manifest",
        )
        results["wrong_old_role_refused"] = refused(
            lambda: correction.run(
                specification(root / "role.json", manifest, data, from_role="reference"), client, root / "bad3", store=False, spec_sha256=None
            ),
            "role",
        )
        results["unknown_new_role_refused"] = refused(
            lambda: correction.run(
                specification(root / "newrole.json", manifest, data, to_role="anything"), client, root / "bad4", store=False, spec_sha256=None
            ),
            "role",
        )
    except Exception as error:  # Report a crash, such as a missing module, as a failed run.
        results["stopped_at"] = False
        sys.stdout.write(f"stopped: {type(error).__name__}: {error}\n")
    finally:
        shutil.rmtree(root, ignore_errors=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report = {"run_at_utc": stamp, "inputs": "synthetic only", "aws_calls": 0, "scenarios": results, "passed": sum(results.values()), "total": len(results)}
    write_once(args.output or REPORT_ROOT / f"report_{stamp}.json", encoded_json(report))
    for name, passed in sorted(results.items()):
        sys.stdout.write(f"{'PASS' if passed else 'FAIL'} {name}\n")
    sys.stdout.write(f"{report['passed']} of {report['total']} scenarios passed\n")
    return 0 if results and all(results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
