"""Synthetic E2E for verify_storage_records: the real command on generated records and saved listings, no AWS.

.venv/bin/python -m scripts.acquisition.run_storage_records_e2e
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.infrastructure.render_project_config import REPO_ROOT
from scripts.process import run_command

BUCKET = "synthetic-bucket"
PREFIX = "example_pub/example_collection/datasets/example/capture_id=SYN"


def version(key: str, version_id: str, size: int) -> dict[str, Any]:
    return {"Key": key, "VersionId": version_id, "Size": size, "IsLatest": True}


def main() -> int:
    root = REPO_ROOT / "data/e2e/storage_records" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    evidence = root / "evidence"
    kept, retired, missing = (f"{PREFIX}/{name}" for name in ("kept.csv", "retired.csv", "missing.csv"))
    redacted, manifest = f"{PREFIX}_redacted/r.csv", "example_pub/example_collection/manifests/capture_id=SYN/m/redaction_manifest.json"
    records = {
        "objects": [
            {"object": {"bucket": BUCKET, "key": kept, "version_id": "v1", "byte_count": 10}},
            {"object": {"bucket": BUCKET, "key": retired, "version_id": "v2", "byte_count": 20}},
            {"object": {"bucket": BUCKET, "key": missing, "version_id": "v3", "byte_count": 30}},
            {"object": {"bucket": "another-bucket", "key": "elsewhere/x.csv", "version_id": "v8", "byte_count": 1}},
        ]
    }
    write_once(evidence / "capture" / "s3_collections_reconciliation.json", encoded_json(records))
    write_once(evidence / "inventory" / "before.json", encoded_json({"versions": [version("old/raw.csv", "v9", 5)]}))
    retirement = root / "retired_objects.json"
    write_once(retirement, encoded_json({"objects": [{"key": retired, "version_id": "v2", "bytes": 20, "status": "deleted_verified"}]}))
    replacements = root / "replacements.json"
    write_once(
        replacements,
        encoded_json(
            {
                "replacements": [
                    {"redacted": {"key": redacted, "version_id": "r1", "byte_count": 9}, "manifest": {"key": manifest, "version_id": "m1", "byte_count": 4}}
                ]
            }
        ),
    )
    base = [version(kept, "v1", 10), version(missing, "v3", 30), version("old/raw.csv", "v9", 5), version(redacted, "r1", 9), version(manifest, "m1", 4)]
    scenarios: list[tuple[str, dict[str, Any], int, dict[str, Any]]] = [
        ("all_accounted", {"Versions": base}, 0, {"retired_versions_confirmed_absent": 1, "live_versions_known_only_from_saved_inventories": 1}),
        ("rerun_same_result", {"Versions": base}, 0, {"recorded_versions": 3, "replacement_versions_verified": 2}),
        ("recorded_version_missing", {"Versions": [item for item in base if item["Key"] != missing]}, 1, {"failure_counts.missing": 1}),
        ("retired_version_present", {"Versions": [*base, version(retired, "v2", 20)]}, 1, {"failure_counts.retired_still_present": 1}),
        (
            "retired_left_as_delete_marker",
            {"Versions": base, "DeleteMarkers": [{"Key": retired, "VersionId": "v2"}]},
            1,
            {"failure_counts.retired_still_present": 1},
        ),
        ("size_changed", {"Versions": [version(kept, "v1", 11), *base[1:]]}, 1, {"failure_counts.size_mismatch": 1}),
        ("replacement_missing", {"Versions": [item for item in base if item["Key"] != redacted]}, 1, {"failure_counts.replacement_missing": 1}),
        ("unrecorded_version", {"Versions": [*base, version("stray/object.csv", "v7", 3)]}, 1, {"unrecorded": 1}),
        ("truncated_listing", {"Versions": base, "IsTruncated": True}, 2, {}),
        ("inventory_version_missing", {"Versions": [item for item in base if item["Key"] != "old/raw.csv"]}, 1, {"failure_counts.inventory_missing": 1}),
        ("malformed_listing", {"Versions": "invalid"}, 2, {}),
        ("duplicate_listing", {"Versions": [*base, base[0]]}, 2, {}),
    ]
    cases, results = [], {}
    status = "failed"
    try:
        for name, listing, expected_code, expected in scenarios:
            listing_path = root / "listings" / f"{name}.json"
            write_once(listing_path, encoded_json(listing))
            arguments = ["-m", "scripts.acquisition.verify_storage_records", "--output", str(root / "runs" / name), "--evidence-root", str(evidence)]
            arguments += ["--retirements", str(retirement), "--replacements", str(replacements), "--listing", str(listing_path), "--bucket", BUCKET]
            result = run_command(sys.executable, arguments, cwd=REPO_ROOT, timeout=120)
            summary = json.loads(result.stdout) if result.stdout.strip() else {}
            observed = {field: summary.get(field.split(".")[0], {}).get(field.split(".")[1]) if "." in field else summary.get(field) for field in expected}
            passed = result.returncode == expected_code and observed == expected
            results[name] = summary
            cases.append({"case": name, "passed": passed, "returncode": result.returncode, "expected": expected, "observed": observed})
            if not passed:
                raise AssertionError(name)
        stable = (
            "status",
            "recorded_versions",
            "recorded_live_verified",
            "retired_versions_confirmed_absent",
            "replacement_versions_verified",
            "failure_counts",
        )
        same = all(results["all_accounted"][field] == results["rerun_same_result"][field] for field in stable)
        cases.append({"case": "repeat_gives_same_business_result", "passed": same})
        other_bucket_ignored = results["all_accounted"]["recorded_versions"] == 3
        cases.append({"case": "records_for_other_buckets_ignored", "passed": other_bucket_ignored})
        if not (same and other_bucket_ignored):
            raise AssertionError("repeat_or_bucket_scope")
        arguments[arguments.index("--listing") + 1] = str(root / "listings" / "all_accounted.json")
        arguments[arguments.index("--output") + 1] = str(root / "runs" / "synthetic_scope")
        write_once(evidence / "e2e" / "broken_fixture.json", b"{intentional invalid synthetic JSON")
        write_once(evidence / "e2e" / "foreign_listing.json", encoded_json({"Versions": [version("fake/data.csv", "fake-v", 5)]}))
        result = run_command(sys.executable, arguments, cwd=REPO_ROOT, timeout=120)
        cases.append({"case": "declared_synthetic_root_excluded", "passed": result.returncode == 0})
        if result.returncode != 0:
            raise AssertionError("declared_synthetic_root_excluded")
        nested = evidence / "capture" / "e2e" / "broken_record.json"
        write_once(nested, b"{not a declared synthetic root")
        result = run_command(sys.executable, arguments, cwd=REPO_ROOT, timeout=120)
        cases.append({"case": "nested_same_name_not_excluded", "passed": result.returncode == 2})
        if result.returncode != 2:
            raise AssertionError("nested_same_name_not_excluded")
        nested.unlink()
        # Schema-review outputs are derived review evidence, not upload records (owner decision Oct 1 2026).
        review = evidence / "schema_review" / "final_pass"
        write_once(review / "broken_review.json", b"{intentional invalid review output")
        with (review / "oversized_keysets.json").open("wb") as handle:
            handle.truncate(51 * 1024**2)  # Sparse: over the inspection limit without writing 51 MB.
        arguments[arguments.index("--output") + 1] = str(root / "runs" / "schema_review_scope")
        result = run_command(sys.executable, arguments, cwd=REPO_ROOT, timeout=120)
        walk = (json.loads(result.stdout) if result.stdout.strip() else {}).get("evidence_walk", {})
        excluded = result.returncode == 0 and walk.get("excluded_schema_review_json_files") == 2
        cases.append({"case": "schema_review_root_excluded_and_counted", "passed": excluded, "evidence_walk": walk})
        if not excluded:
            raise AssertionError("schema_review_root_excluded_and_counted")
        # An upload record kept only under schema_review is never verified (review STORAGE_SCHEMA_REVIEW_R1).
        hidden = {"bucket": BUCKET, "key": "hidden/x.csv", "version_id": "h1", "byte_count": 10}
        write_once(evidence / "schema_review" / "upload.json", encoded_json({"object": hidden}))
        write_once(root / "listings" / "hidden_live.json", encoded_json({"Versions": [*base, version("hidden/x.csv", "h1", 11)]}))
        hidden_args = [*arguments]
        hidden_args[hidden_args.index("--listing") + 1] = str(root / "listings" / "hidden_live.json")
        hidden_args[hidden_args.index("--output") + 1] = str(root / "runs" / "hidden_unrecorded")
        result = run_command(sys.executable, hidden_args, cwd=REPO_ROOT, timeout=120)
        summary = json.loads(result.stdout) if result.stdout.strip() else {}
        unrecorded = result.returncode == 1 and summary.get("unrecorded") == 1 and summary.get("recorded_versions") == 3
        cases.append({"case": "excluded_upload_reported_unrecorded", "passed": unrecorded})
        if not unrecorded:
            raise AssertionError("excluded_upload_reported_unrecorded")
        inventory = evidence / "inventory" / "hidden.json"
        write_once(inventory, encoded_json({"versions": [version("hidden/x.csv", "h1", 10)]}))
        hidden_args[hidden_args.index("--output") + 1] = str(root / "runs" / "hidden_inventory_only")
        result = run_command(sys.executable, hidden_args, cwd=REPO_ROOT, timeout=120)
        summary = json.loads(result.stdout) if result.stdout.strip() else {}
        # Existing policy: a saved inventory accounts for the version without a size check; it is never counted as verified.
        inventory_only = (
            result.returncode == 0
            and summary.get("live_versions_known_only_from_saved_inventories") == 2
            and summary.get("recorded_versions") == 3
            and summary.get("recorded_live_verified") == results["all_accounted"]["recorded_live_verified"]
            and summary.get("failure_counts", {}).get("size_mismatch") == 0
        )
        cases.append({"case": "excluded_upload_with_saved_inventory_is_inventory_only", "passed": inventory_only})
        if not inventory_only:
            raise AssertionError("excluded_upload_with_saved_inventory_is_inventory_only")
        inventory.unlink()
        nested_review = evidence / "capture" / "schema_review" / "broken_record.json"
        write_once(nested_review, b"{not the declared review root")
        arguments[arguments.index("--output") + 1] = str(root / "runs" / "nested_schema_review")
        result = run_command(sys.executable, arguments, cwd=REPO_ROOT, timeout=120)
        cases.append({"case": "nested_schema_review_not_excluded", "passed": result.returncode == 2})
        if result.returncode != 2:
            raise AssertionError("nested_schema_review_not_excluded")
        nested_review.unlink()
        arguments[arguments.index("--output") + 1] = str(root / "runs" / "synthetic_scope_after_review")
        empty = evidence / "discovery" / "groups.json"
        write_once(empty, b"")
        dispositions = root / "empty_dispositions.json"
        write_once(
            dispositions,
            encoded_json(
                {
                    "kind": "empty_publisher_capture_dispositions",
                    "files": [{"path": "discovery/groups.json", "sha256": fingerprint(empty)[0], "reason": "Synthetic failed publisher metadata download."}],
                }
            ),
        )
        scoped_args = [*arguments, "--empty-capture-dispositions", str(dispositions)]
        scoped_args[scoped_args.index("--output") + 1] = str(root / "runs" / "empty_capture")
        result = run_command(sys.executable, scoped_args, cwd=REPO_ROOT, timeout=120)
        cases.append({"case": "reviewed_empty_capture", "passed": result.returncode == 0})
        if result.returncode != 0:
            raise AssertionError("reviewed_empty_capture")
        empty.write_text("{changed", encoding="utf-8")
        result = run_command(sys.executable, scoped_args, cwd=REPO_ROOT, timeout=120)
        cases.append({"case": "changed_disposed_capture_rejected", "passed": result.returncode == 2})
        if result.returncode != 2:
            raise AssertionError("changed_disposed_capture_rejected")
        empty.unlink()
        (evidence / "broken_record.json").write_text("{broken", encoding="utf-8")
        arguments[arguments.index("--listing") + 1] = str(root / "listings" / "all_accounted.json")
        result = run_command(sys.executable, arguments, cwd=REPO_ROOT, timeout=120)
        cases.append({"case": "unreadable_evidence_rejected", "passed": result.returncode == 2})
        if result.returncode != 2:
            raise AssertionError("unreadable_evidence_rejected")
        status = "passed"
    except AssertionError as error:
        cases.append({"case": "stopped_at", "passed": False, "detail": str(error)})
    finally:
        artifact = {
            "kind": "synthetic_storage_records_e2e",
            "status": status,
            "completed_at_utc": datetime.now(UTC).isoformat(),
            "python_version": sys.version,
            "implementation_sha256": fingerprint(Path(__file__).with_name("verify_storage_records.py"))[0],
            "runner_sha256": fingerprint(Path(__file__))[0],
            "cases": cases,
            "reproduce": ".venv/bin/python -m scripts.acquisition.run_storage_records_e2e",
            "tested_boundary": "The check's command line on synthetic records, retirement and replacement files and saved listings.",
            "untested_boundaries": ["Live S3 listing and project identity (verified by the live run)"],
            "aws_calls": 0,
            "cleanup": "Synthetic files kept in the run folder; nothing created in AWS.",
        }
        write_once(root / "artifact.json", encoded_json(artifact))
    sys.stdout.write(json.dumps({"status": status, "cases": len(cases), "artifact": str(root / "artifact.json")}) + "\n")
    return 0 if status == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
