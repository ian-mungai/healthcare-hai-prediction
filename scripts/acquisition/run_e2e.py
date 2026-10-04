"""User-approved live-data exception; opt-in HTTPS-to-S3 verification, never routine CI."""

from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import json
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition.batch import validate_batch
from scripts.acquisition.capture import receipt_validator, validate_receipt
from scripts.acquisition.cli_tools import executable, run_argv
from scripts.acquisition.data_paths import current
from scripts.acquisition.s3_store import AwsCli, encoded_json, fingerprint, preflight, write_once
from scripts.acquisition.source_registry import canonical_hash, load_registry, read_json
from scripts.acquisition.transport import CaptureError
from scripts.infrastructure.render_project_config import REPO_ROOT, load_configuration
from scripts.process import SubprocessError


def require(condition: bool, name: str, assertions: list[dict]) -> None:
    """Raise the module-specific validation error when the required condition is false."""
    assertions.append({"name": name, "passed": bool(condition)})
    if not condition:
        raise CaptureError(f"E2E assertion failed: {name}")


def evidence(path: Path, value: dict) -> None:
    """Write an immutable JSON evidence document, rejecting conflicting existing bytes."""
    write_once(path, encoded_json(value))


def inventory(client: AwsCli) -> dict:
    """Return complete S3 object and version listings or reject truncated responses."""
    listing = client.call("s3api", "list-objects-v2")
    versions = client.call("s3api", "list-object-versions")
    if listing.get("IsTruncated") or versions.get("IsTruncated"):
        raise CaptureError("Incomplete S3 inventory cannot verify an E2E run.")
    return {"objects": listing.get("Contents", []), "versions": versions.get("Versions", []), "delete_markers": versions.get("DeleteMarkers", [])}


def records(reconciliation: dict) -> dict[str, dict]:
    """Index reconciled object records by key, rejecting conflicting references."""
    result: dict[str, dict] = {}
    for entry in reconciliation["objects"] + reconciliation["manifests"]:
        item = entry["object"]
        if item["key"] in result and result[item["key"]] != item:
            raise CaptureError("Conflicting references to one S3 object.")
        result[item["key"]] = item
    return result


def independent_readback(client: AwsCli, output: Path, stored: dict[str, dict], assertions: list[dict]) -> None:
    """Read each exact S3 version and preserve hash, size and encryption verification evidence."""
    checked = []
    for index, (key, record) in enumerate(sorted(stored.items()), 1):
        directory = output / "readback" / hashlib.sha256(key.encode()).hexdigest()
        directory.mkdir(parents=True, exist_ok=False)
        target = directory / "payload"
        response = client.call("s3api", "get-object", ["--key", key, "--version-id", record["version_id"], "--checksum-mode", "ENABLED", str(target)])
        with target.open("rb") as handle:
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        size = target.stat().st_size
        valid = (
            digest == record["sha256"]
            and size == record["byte_count"]
            and response.get("VersionId") == record["version_id"]
            and response.get("ContentLength") == size
            and response.get("ServerSideEncryption") == "AES256"
            and response.get("ChecksumSHA256") == base64.b64encode(bytes.fromhex(digest)).decode()
        )
        evidence(directory / "response.json", response)
        require(valid, f"independent_persistent_version_readback_{index}", assertions)
        checked.append({**record, "downloaded_path": target.relative_to(output).as_posix(), "actual_sha256": digest, "actual_byte_count": size})
        if index % 20 == 0:
            logging.getLogger(__name__).info(
                "S3 readback progress: %s/%s objects", index, len(stored), extra={"objects_verified": index, "object_count": len(stored)}
            )
    evidence(output / "readback.json", {"objects": checked, "verification": "independent_sha256_length_version_checksum_and_encryption"})


def command(output: Path, name: str, args: list[str], commands: list[dict], expected: int = 0) -> dict:
    """Run a fixed E2E argument list and return its JSON object while preserving command evidence."""
    logging.getLogger(__name__).info("Running E2E command: %s", name, extra={"command_name": name})
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("AWS_", "TF_CLI_ARGS"))}
    completed = run_argv(args, cwd=REPO_ROOT, env=environment, capture_output=True, text=True, timeout=2400)
    write_once(output / "commands" / f"{name}.stdout", completed.stdout.encode())
    write_once(output / "commands" / f"{name}.stderr", completed.stderr.encode())
    commands.append({"name": name, "argv": args, "returncode": completed.returncode, "expected_returncode": expected})
    if completed.returncode != expected:
        raise CaptureError(f"E2E command failed: {name}; inspect its preserved command evidence.")
    result = json.loads(completed.stdout)
    if not isinstance(result, dict):
        raise CaptureError("E2E command did not return a JSON object.")
    return result


def selected_batch(scenario: dict) -> tuple[dict, dict]:
    """Return bounded live-test jobs and baselines from the explicitly selected scenario."""
    registry, validator = load_registry(), receipt_validator()
    batch = read_json(REPO_ROOT / scenario["batch_plan"])
    validate_batch(batch, registry, validator)
    candidates = [job for job in batch["jobs"] if job["job_id"] == scenario["csv_job_id"]]
    if len(candidates) != 1 or candidates[0]["plan"]["expected_format"] != "csv":
        raise CaptureError("Select exactly one already-approved CSV job.")
    csv_plan = copy.deepcopy(candidates[0]["plan"])
    archive_plan = read_json(REPO_ROOT / scenario["archive_plan"])
    references = read_json(REPO_ROOT / scenario["archive_references"])["references"]
    baseline: dict[str, dict] = {}
    for name, plan in (("csv", csv_plan), ("archive", archive_plan)):
        path = REPO_ROOT / scenario[f"{name}_baseline_receipt"]
        receipt = read_json(path)
        validate_receipt(receipt, validator, path.parent)
        if receipt["snapshot_status"] != "acquired_unvalidated" or len(receipt["artifacts"]) != 1:
            raise CaptureError("E2E baselines must be complete single-file captures.")
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        if receipt["source"]["source_record_id"] != plan["source_id"] or lineage["route_id"] != plan["route_id"]:
            raise CaptureError("E2E baseline differs from the selected approved route.")
        plan["expected_sha256"] = receipt["artifacts"][0]["sha256"]
        baseline[name] = read_json(path.parent / "s3_collections_reconciliation.json")
    if archive_plan["expected_format"] != "zip":
        raise CaptureError("The archive scenario requires a previously captured ZIP route.")
    rejected = copy.deepcopy(archive_plan)
    rejected["expected_sha256"] = "0" * 64 if archive_plan["expected_sha256"] != "0" * 64 else "1" * 64
    limits = {"attempts": 1, "max_pages": 1, "max_seconds": 300, "timeout_seconds": 30}
    jobs = [
        {"job_id": "e2e_csv", "plan": csv_plan, "references": candidates[0]["references"], "limits": {**limits, "max_bytes": 8 * 1024**2}},
        {"job_id": "e2e_archive", "plan": archive_plan, "references": references, "limits": {**limits, "max_bytes": 24 * 1024**2}},
        {"job_id": "e2e_rejected_archive", "plan": rejected, "references": [], "limits": {**limits, "max_bytes": 24 * 1024**2}},
    ]
    selected = {"batch_version": 1, "registry_sha256": canonical_hash(registry), "jobs": jobs}
    validate_batch(selected, registry, validator)
    return selected, baseline


def execute(output: Path, batch: dict, baseline: dict, assertions: list[dict], commands: list[dict], writes: dict) -> None:
    """Execute the selected live acquisition scenarios and record storage and replay assertions."""
    settings, _ = load_configuration(REPO_ROOT / ".env")
    client = AwsCli(settings)
    rendered = run_argv([executable("terraform"), f"-chdir={REPO_ROOT / 'infra'}", "output", "-json"], capture_output=True, text=True, check=True, timeout=30)
    preflight(client, json.loads(rendered.stdout))
    before = inventory(client)
    evidence(output / "before.json", before)
    result = command(
        output,
        "capture_store",
        [
            sys.executable,
            "-m",
            "scripts.acquisition.batch",
            "--plan",
            str(output / "batch.json"),
            "--state-root",
            str(output / "state"),
            "--execute",
            "--max-jobs",
            "3",
            "--max-download-bytes",
            str(64 * 1024**2),
        ],
        commands,
        1,
    )
    evidence(output / "batch_result.json", result)
    statuses = {item["job_id"]: item["status"] for item in result["events"]}
    expected = {"e2e_csv": "stored_unvalidated", "e2e_archive": "stored_unvalidated", "e2e_rejected_archive": "evidence_only"}
    require(statuses == expected and result["unprocessed_jobs"] == 0, "fresh_real_captures_and_expected_checksum_rejection", assertions)
    require(result["reserved_download_bytes"] <= 64 * 1024**2, "download_budget_preserved", assertions)
    expected_records: dict[str, dict] = {}
    for job, event in zip(batch["jobs"], result["events"], strict=True):
        require(job["job_id"] == event["job_id"], "job_order_preserved_" + job["job_id"], assertions)
        receipt_path = Path(current(event["receipt_path"]))
        receipt = read_json(receipt_path)
        validate_receipt(receipt, receipt_validator(), receipt_path.parent)
        reconciliation = read_json(Path(event["reconciliation_path"]))
        stored = records(reconciliation)
        expected_records.update(stored)
        require(not any(key.lower().endswith(".zip") for key in stored), "no_zip_objects_" + job["job_id"], assertions)
        require(reconciliation["storage_contract_version"] == "4.0.0", "collection_layout_" + job["job_id"], assertions)
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        require(
            all(item["http_status"] == 200 and item["attempts"] == 1 for item in lineage["requests"]),
            "real_complete_https_response_" + job["job_id"],
            assertions,
        )
        manifests = [read_json(path) for path in sorted((receipt_path.parent / "s3_collections").glob("*/manifest.json"))]
        require(bool(manifests) and all(item["model_eligible"] is False for item in manifests), "not_model_eligible_" + job["job_id"], assertions)
        if job["job_id"] == "e2e_rejected_archive":
            require("expected_hash_mismatch" in receipt["quality_profile"]["checks_failed"], "negative_case_rejected_for_expected_reason", assertions)
            require(all(key.split("/")[2] == "audit" for key in stored), "rejected_archive_only_audit_metadata", assertions)
            require(all(item.get("local_only_archive_payloads") for item in manifests), "rejected_zip_retained_locally", assertions)
        else:
            name = "archive" if job["plan"]["expected_format"] == "zip" else "csv"
            require(len(reconciliation["manifests"]) == len(baseline[name]["manifests"]), "dataset_manifest_count_" + name, assertions)
            require(receipt["artifacts"][0]["sha256"] == job["plan"]["expected_sha256"], "pinned_publisher_bytes_" + name, assertions)
            if name == "archive":
                require(
                    all(item.get("archive_storage_policy") == "extracted_members_only_zip_retained_locally" for item in manifests),
                    "archive_contents_only_storage_policy",
                    assertions,
                )
                reference_keys = {entry["object"]["key"] for entry in reconciliation["objects"] if entry["role"] == "dictionary"}
                require(len(reference_keys) == 1, "shared_dictionary_stored_once", assertions)
                old_holds = {(item["archive_member"], item["hold_reason"]) for item in baseline[name]["objects"] if item.get("hold_reason")}
                new_holds = {(item["archive_member"], item["hold_reason"]) for item in reconciliation["objects"] if item.get("hold_reason")}
                require(old_holds == new_holds, "publisher_metadata_holds_preserved", assertions)
    after_capture = inventory(client)
    evidence(output / "after_capture.json", after_capture)
    for job, event in zip(batch["jobs"], result["events"], strict=True):
        references_path = output / (job["job_id"] + "_references.json")
        evidence(references_path, {"references": job["references"]})
        args = [sys.executable, "-m", "scripts.acquisition.s3_store", "--receipt", event["receipt_path"], "--references", str(references_path)]
        if job["job_id"] == "e2e_rejected_archive":
            args.append("--audit-only")
        replay = command(output, "replay_" + job["job_id"], args, commands)
        evidence(output / (job["job_id"] + "_replay.json"), replay)
        require(replay["objects_created"] == 0 and replay["objects_verified"] > 0, "live_version_readback_and_zero_new_objects_" + job["job_id"], assertions)
    after = inventory(client)
    evidence(output / "after_replay.json", after)
    require(after == after_capture, "replay_preserves_all_keys_versions_and_delete_markers", assertions)
    before_versions = {(item["Key"], item["VersionId"]): item for item in before["versions"]}
    after_versions = {(item["Key"], item["VersionId"]): item for item in after["versions"]}
    require(all(after_versions.get(key) == value for key, value in before_versions.items()), "all_prior_versions_unchanged", assertions)
    require(before["delete_markers"] == after["delete_markers"], "no_deletions", assertions)
    before_keys = {item["Key"] for item in before["objects"]}
    after_keys = {item["Key"] for item in after["objects"]}
    require(after_keys == before_keys | set(expected_records), "all_new_objects_explained_by_reconciliations", assertions)
    require(not any(key.lower().endswith(".zip") for key in after_keys - before_keys), "no_new_zip_anywhere_in_bucket", assertions)
    latest = {item["Key"]: item for item in after["versions"] if item["IsLatest"]}
    require(
        all(latest[key]["VersionId"] == item["version_id"] and latest[key]["Size"] == item["byte_count"] for key, item in expected_records.items()),
        "all_recorded_versions_are_current_and_size_matched",
        assertions,
    )
    writes.update(new_objects=len(after_keys - before_keys), new_versions=len(after_versions) - len(before_versions), replay_new_objects=0, deletions=0)
    independent_readback(client, output, expected_records, assertions)


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description="Real publisher HTTPS, ZIP extraction and S3 E2E; --execute opts into bounded writes. No cleanup.")
    parser.add_argument("--scenario", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    output = args.output.resolve()
    assertions: list[dict] = []
    commands: list[dict] = []
    writes: dict[str, Any] = {"new_objects": None, "new_versions": None, "replay_new_objects": None, "deletions": 0}
    try:
        scenario = read_json(args.scenario)
        batch, baseline = selected_batch(scenario)
        if not args.execute:
            sys.stdout.write(str(json.dumps({"status": "validated_offline", "jobs": 3, "maximum_download_bytes": 64 * 1024**2, "aws_calls": 0})) + "\n")
            return
        os.umask(0o077)
        output.mkdir(parents=True, exist_ok=False, mode=0o700)
    except (ValueError, OSError, KeyError, TypeError) as error:
        parser.error(str(error))
    started = datetime.now(UTC).isoformat()
    status = "failed"
    error_type = None
    try:
        evidence(output / "scenario.json", scenario)
        evidence(output / "batch.json", batch)
        evidence(output / "baseline_reconciliations.json", baseline)
        sources = sorted((REPO_ROOT / "scripts/acquisition").glob("*.py"))
        sources += [REPO_ROOT / "scripts/infrastructure/render_project_config.py", REPO_ROOT / "config/acquisition/collection_layout.json"]
        evidence(output / "implementation.json", {"files": [{"path": str(path.relative_to(REPO_ROOT)), "sha256": fingerprint(path)[0]} for path in sources]})
        execute(output, batch, baseline, assertions, commands, writes)
        status = "passed"
    except (ValueError, OSError, KeyError, TypeError, SubprocessError) as error:
        error_type = type(error).__name__
        assertions.append({"name": "execution_completed_without_error", "passed": False})
        logging.getLogger(__name__).error("E2E failed; evidence retained", extra={"error_type": error_type})
    finally:
        files = []
        for path in sorted(output.rglob("*")):
            if path.is_file():
                digest, size = fingerprint(path)
                files.append({"path": path.relative_to(output).as_posix(), "sha256": digest, "byte_count": size})
        artifact = {
            "schema_version": 1,
            "kind": "live_acquisition_e2e",
            "run_id": output.name,
            "status": status,
            "error_type": error_type,
            "started_at_utc": started,
            "finished_at_utc": datetime.now(UTC).isoformat(),
            "commands": commands,
            "assertions": assertions,
            "files": files,
            "aws_writes": writes,
            "model_eligible": False,
            "reproduce": [
                sys.executable,
                "-m",
                "scripts.acquisition.run_e2e",
                "--scenario",
                str(args.scenario.resolve()),
                "--output",
                "<new_evidence_directory>",
                "--execute",
            ],
        }
        evidence(output / "artifact.json", artifact)
        sys.stdout.write(
            str(
                json.dumps(
                    {
                        "status": status,
                        "assertions": len(assertions),
                        "artifact": str(output / "artifact.json"),
                        "artifact_sha256": hashlib.sha256((output / "artifact.json").read_bytes()).hexdigest(),
                        "aws_writes": writes,
                    },
                    indent=2,
                )
            )
            + "\n"
        )
    if status != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
