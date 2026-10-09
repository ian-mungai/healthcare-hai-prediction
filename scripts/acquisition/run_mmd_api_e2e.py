"""Exercise the MMD collector's real offline CLI against disposable pilot copies."""

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from scripts.acquisition.cli_tools import run_argv
from scripts.acquisition.data_paths import current
from scripts.acquisition.mmd_api_contract import (
    CURRENT_CODE_FILES,
    PLAN_PATH,
    condition_for,
    current_code_hashes,
    load_plan,
    reconstruct,
    reviewed_code_versions,
)
from scripts.acquisition.run_mmd_api_collection import ATTEMPTS, with_retries
from scripts.acquisition.s3_store import StorageError, encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import RegistryError, read_json, require


def command(arguments: list[str]) -> tuple[int, str]:
    """Run the actual local CLI without network or cloud flags."""
    result = run_argv(
        [sys.executable, "-m", "scripts.acquisition.collect_mmd_api", *arguments],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode, (result.stdout + result.stderr).strip()


def rewrite_fips(target: Path, receipt: dict, lineage: dict, kind: str) -> None:
    """Change county codes past the probe window and re-seal every dependent hash."""
    roles = {a["role"]: a for a in receipt["artifacts"]}
    raw_path, evidence_path = target / roles["api_page"]["storage_path"], target / roles["export_receipt"]["storage_path"]
    rows = json.loads(raw_path.read_bytes())
    # Probes cover offsets 0-3, so edits start at index 4 to leave completeness evidence intact.
    zero_led = [i for i in range(4, len(rows)) if rows[i]["fips"].startswith("0")]
    first, second = zero_led[0], zero_led[1]
    if kind == "fips_four_digit":
        rows[first]["fips"] = rows[first]["fips"][1:]
    elif kind == "fips_three_digit":
        rows[first]["fips"] = rows[first]["fips"][2:]
    else:
        rows[second]["fips"] = rows[first]["fips"][1:]
    raw = json.dumps(rows).encode()
    raw_path.write_bytes(raw)
    roles["api_page"]["sha256"], roles["api_page"]["byte_count"] = fingerprint(raw_path)
    evidence = read_json(evidence_path)
    evidence["main"].update(sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw))
    evidence_path.write_bytes(encoded_json(evidence))
    roles["export_receipt"]["sha256"], roles["export_receipt"]["byte_count"] = fingerprint(evidence_path)
    lineage["expected_sha256"] = lineage["derivation"]["input_sha256"] = hashlib.sha256(raw).hexdigest()


def unknown_county_case(name: str, replacement: str | None) -> dict:
    """Rebuild the CSV from the real cached C258.19 2023 rows, whose extra 49990 row stopped version 2."""
    plan = load_plan()
    rows = json.loads((PLAN_PATH.parent / "c258_19/2023/transport/main.json").read_bytes())
    require(sum(r["fips"] == "49990" for r in rows) == 1, "Cached C258.19 2023 evidence changed")
    if replacement:
        rows = [dict(r, fips=replacement) if r["fips"] == "49990" else r for r in rows]
    try:
        derived, _ = reconstruct(plan, condition_for(plan, "C258.19", 2023), 2023, rows)
    except (ValueError, OSError, KeyError, TypeError, StopIteration) as error:
        return {"name": name, "observed": f"{type(error).__name__}: {error}", "derived_sha256": None}
    utah_unknown = next(line for line in derived.decode().split("\r\n") if ",49990," in line)
    return {"name": name, "observed": "reconstructed", "derived_sha256": hashlib.sha256(derived).hexdigest(), "row_49990": utah_unknown}


def retry_case(name: str, failures: list[Exception], expect_success: bool, expect_attempts: int) -> dict:
    """Drive the queue retry wrapper with a scripted action; no network, AWS or waiting."""
    attempts: list[int] = []

    def action() -> None:
        attempts.append(len(attempts) + 1)
        if len(attempts) <= len(failures):
            raise failures[len(attempts) - 1]

    retries: list[dict] = []
    try:
        with_retries(action, retries, name, sleep=lambda _seconds: None)
        succeeded = True
    except (ValueError, OSError, KeyError, TypeError, StopIteration):
        succeeded = False
    passed = succeeded == expect_success and len(attempts) == expect_attempts
    return {"name": name, "attempts": len(attempts), "recorded_retries": len(retries), "succeeded": succeeded, "passed": passed}


def branch_files(measure: str, year: int) -> dict[str, tuple[str, int]]:
    """Fingerprint every local file for one condition-year to prove a rerun writes nothing."""
    root = PLAN_PATH.parent / measure / str(year)
    return {str(p): fingerprint(p) for p in sorted(root.rglob("*")) if p.is_file()}


def negative_copy(source: Path, kind: str, directory: Path) -> Path:
    """Mutate only a disposable local candidate; the production snapshot is read only."""
    target = directory / kind
    shutil.copytree(source.parent, target)
    receipt = read_json(target / "receipt.json")
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    if kind.startswith("fips_"):
        rewrite_fips(target, receipt, lineage, kind)
    elif kind == "unreviewed_code_hashes":
        lineage["code_sha256"] = dict.fromkeys(lineage["code_sha256"], "0" * 64)
    elif kind == "wrong_filter":
        receipt["acquisition"]["request_parameters"]["condition"] = "999"
    elif kind == "model_promotion":
        lineage["model_eligible"] = True
    elif kind == "wrong_plan":
        lineage["plan_sha256"] = "0" * 64
    elif kind == "missing_code_hashes":
        lineage.pop("code_sha256")
    elif kind == "empty_code_hashes":
        lineage["code_sha256"] = {}
    elif kind == "extra_code_file":
        lineage["code_sha256"] = dict(lineage["code_sha256"], **{"scripts/acquisition/unlisted.py": "0" * 64})
    elif kind == "current_file_set_unreviewed":
        lineage["code_sha256"] = dict.fromkeys(CURRENT_CODE_FILES, "0" * 64)
    elif kind == "derived_bytes":
        item = next(a for a in receipt["artifacts"] if a["role"] == "data")
        path = target / item["storage_path"]
        path.write_bytes(path.read_bytes() + b"\r\n")
        item["sha256"], item["byte_count"] = fingerprint(path)
    elif kind == "raw_checksum":
        item = next(a for a in receipt["artifacts"] if a["role"] == "api_page")
        path = target / item["storage_path"]
        path.write_bytes(path.read_bytes() + b"\n")
    else:
        raise ValueError(kind)
    receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
    (target / "receipt.json").write_bytes(encoded_json(receipt))
    return target / "receipt.json"


def fresh_capture_case(source: Path, directory: Path) -> dict:
    """Rebuild a new capture offline from the stored responses, so current code fingerprints reach verify_capture."""
    from scripts.acquisition.collect_mmd_api import capture

    receipt = read_json(source)
    lineage = json.loads(receipt["lineage"]["extraction_or_query"])
    root = directory / "fresh_root"
    transport = root / "c258_07" / "2023" / "transport"
    transport.mkdir(parents=True)
    stored_transport = source.parents[2] / "transport"
    for name in ("main.json", "main_metadata.json"):
        shutil.copy2(stored_transport / name, transport / name)
    evidence = read_json(source.parent / "evidence" / "export_evidence.json")
    for probe in evidence["probes"]:
        offset = probe["url"].rsplit("_offset=", 1)[1]
        (transport / f"probe_{offset}.json").write_bytes(probe["body_utf8"].encode())
        (transport / f"probe_{offset}_metadata.json").write_bytes(encoded_json({k: v for k, v in probe.items() if k != "body_utf8"}))
    from scripts.acquisition import collect_mmd_api, mmd_api_contract

    # Approve the current fingerprint in memory only, so the capture reaches verify_capture; the real catalog is never written.
    approved = [*reviewed_code_versions(), current_code_hashes()]
    calls: list[dict] = []

    def verify(receipt: dict, source: dict, lineage: dict, folder: Path, evidence_only: bool) -> None:
        calls.append(json.loads(receipt["lineage"]["extraction_or_query"])["code_sha256"])
        mmd_api_contract.verify_capture(receipt, source, lineage, folder, evidence_only)

    try:
        with (
            patch.object(collect_mmd_api, "reviewed_code_versions", lambda: approved),
            patch.object(mmd_api_contract, "reviewed_code_versions", lambda: approved),
            patch.object(collect_mmd_api, "verify_capture", verify),
        ):
            path = capture("C258.07", 2023, False, root=root)
        fresh = json.loads(read_json(path)["lineage"]["extraction_or_query"])
        observed = "valid"
        passed = (
            bool(calls)
            and all(set(call) == set(CURRENT_CODE_FILES) for call in calls)
            and set(fresh["code_sha256"]) == set(CURRENT_CODE_FILES)
            and fresh["expected_sha256"] == lineage["expected_sha256"]
        )
    except (RegistryError, StorageError, OSError, KeyError, ValueError) as error:
        observed, passed = f"{type(error).__name__}: {error}", False
    return {"name": "fresh_capture_current_code_offline", "observed": observed.replace(str(directory), "<disposable copy>"), "passed": passed}


def main() -> None:
    """Retain bounded evidence for actual CLI replay and refusal scenarios."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    ready = read_json(PLAN_PATH.parent / "c258_07/2023/capture_ready.json")
    # The record keeps the path it was written with; the path map gives where it lives now.
    source = Path(current(ready["receipt_path"]))
    before = {str(p): fingerprint(p) for p in source.parent.rglob("*") if p.is_file()}
    scenarios = []
    baseline = ["--measure-id", "C258.07", "--year", "2023"]
    # A finished year is revalidated, not recaptured: new code would create a second snapshot for it.
    for name in ("finished_capture_valid_one", "finished_capture_valid_two"):
        code, output = command([*baseline, "--validate-receipt", str(source)])
        scenarios.append({"name": name, "expected_exit": 0, "observed_exit": code, "passed": code == 0, "output": output})
    expected = {
        "fips_three_digit": "Native FIPS width/value changed",
        "fips_padding_collision": "Duplicate native FIPS",
        "unreviewed_code_hashes": "MMD capture implementation changed; review before reuse",
        "wrong_filter": "MMD transport differs",
        "model_promotion": "MMD review hold differs",
        "wrong_plan": "MMD authorization binding differs",
        "derived_bytes": "Derived CSV differs",
        "raw_checksum": "Artifact hash or byte count differs",
        "missing_code_hashes": "MMD code binding is incomplete",
        "empty_code_hashes": "MMD code binding is incomplete",
        "extra_code_file": "MMD code binding is incomplete",
        "current_file_set_unreviewed": "MMD capture implementation changed; review before reuse",
    }
    with tempfile.TemporaryDirectory(prefix="mmd_cli_negative_") as temporary:
        for name, error in expected.items():
            path = negative_copy(source, name, Path(temporary))
            code, output = command([*baseline, "--validate-receipt", str(path)])
            scenarios.append(
                {
                    "name": name,
                    "expected_exit": "nonzero",
                    "observed_exit": code,
                    "expected_error": error,
                    "passed": code != 0 and error in output,
                    "output": output.replace(temporary, "<disposable copy>"),
                }
            )
        path = negative_copy(source, "fips_four_digit", Path(temporary))
        code, output = command([*baseline, "--validate-receipt", str(path)])
        scenarios.append(
            {
                "name": "fips_four_digit_accepted",
                "expected_exit": 0,
                "observed_exit": code,
                "passed": code == 0 and '"status": "valid"' in output,
                "output": output.replace(temporary, "<disposable copy>"),
            }
        )
    with tempfile.TemporaryDirectory(prefix="mmd_fresh_capture_") as temporary:
        scenarios.append(fresh_capture_case(source, Path(temporary)))
    accepted = unknown_county_case("unknown_county_990_accepted", None)
    accepted["passed"] = accepted["observed"] == "reconstructed" and ",49990,," in accepted["row_49990"]
    rejected = unknown_county_case("unknown_non_990_rejected", "49991")
    rejected["passed"] = "New missing geographic lookup requires review" in rejected["observed"]
    scenarios += [accepted, rejected]
    # A finished year is re-verified and returned; neither rerun may add files, request data or upload.
    for name, flags, status in (("finished_year_rerun_offline", [], "stored"), ("finished_year_rerun_execute", ["--execute"], "stored")):
        before_files = branch_files("c258_07", 2023)
        code, output = command([*baseline, *flags])
        unchanged = branch_files("c258_07", 2023) == before_files
        scenarios.append(
            {
                "name": name,
                "observed_exit": code,
                "files_unchanged": unchanged,
                "passed": code == 0 and unchanged and f'"status": "{status}"' in output,
                "output": output,
            }
        )
    transient_error = StorageError("head-object", "request_failed")
    scenarios += [
        retry_case("retry_recovers_after_two_transient_failures", [transient_error, OSError("connection reset")], True, 3),
        retry_case("retry_gives_up_after_limit", [transient_error] * ATTEMPTS, False, ATTEMPTS),
        retry_case("no_retry_for_data_errors", [RegistryError("New missing geographic lookup requires review")], False, 1),
    ]
    for name, mid, year, error in (
        ("unregistered_measure", "C258.99", "2023", "not in the approved locked registry"),
        ("unavailable_year", "C258.06", "2021", "availability_hold"),
        # C258.80 resolves through the additions plan; 2020 predates CMS's behavioral-health menu for code 136.
        ("addition_unavailable_year", "C258.80", "2020", "availability_hold"),
    ):
        code, output = command(["--measure-id", mid, "--year", year])
        scenarios.append({"name": name, "expected_error": error, "observed_exit": code, "passed": code != 0 and error in output, "output": output})
    after = {str(p): fingerprint(p) for p in source.parent.rglob("*") if p.is_file()}
    code, output = command(["--measure-id", "C258.07", "--year", "2022", "--validate-receipt", str(source)])
    scenarios.append({"name": "validation_year_mismatch", "observed_exit": code, "passed": code != 0 and "Validation year differs" in output, "output": output})
    scenarios.append({"name": "original_snapshot_unchanged", "passed": before == after})
    result = {
        "status": "passed" if all(s["passed"] for s in scenarios) else "failed",
        "created_at_utc": datetime.now(UTC).isoformat(),
        "scope": "Real offline collector CLI, public pilot copies, no API requests or AWS calls",
        "scenarios": scenarios,
        "input_receipt": str(source),
        "input_receipt_sha256": fingerprint(source)[0],
        "python_version": sys.version,
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "collector_code_sha256": current_code_hashes(),
        "collector_code_listed": current_code_hashes() in reviewed_code_versions(),
        "temporary_copies_removed": True,
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_mmd_api_e2e --output {args.output.parent}/rerun_{args.output.name}",
        "untested": [
            "Live S3 path is verified separately",
            "Live 2012-2018 county-code capture is verified by the resumed queue run",
            "No suppressed/non-numeric input accepted; those stop for review",
            "No other-source API route change",
        ],
    }
    write_once(args.output, encoded_json(result))
    sys.stdout.write(json.dumps({"status": result["status"], "scenarios": len(scenarios), "artifact": str(args.output)}) + "\n")
    require(result["status"] == "passed", "MMD CLI E2E scenario failed")


if __name__ == "__main__":
    main()
