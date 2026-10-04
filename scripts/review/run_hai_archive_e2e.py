"""Run the amended-inventory build, the first-pass audit and the HAI archive review through their real CLIs.

Real inputs run twice and must be byte-identical. Synthetic inputs (generated values only) exercise the rejection and
counting paths. Every run, including a failed one, replaces ``data/e2e/hai_archive_review/report.json``.

Usage::

    SOURCE_ROOT/.venv/bin/python -m scripts.review.run_hai_archive_e2e --source-root SOURCE_ROOT
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import platform
import shutil
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import CompletedProcess, run_command
from scripts.review.review_remaining import digest, write_json

ROOT = Path(__file__).resolve().parents[2]
REVIEW = ROOT / "data" / "schema_review" / "2026_09_30"
E2E = ROOT / "data" / "e2e" / "hai_archive_review"
SOURCE = "main-hai-pdc"
# The telephone value is assembled at run time so this file holds no telephone-shaped literal.
PLANTED = ("SYNTHETIC PLANTED HOSPITAL NAME", "-".join(("555", "010", "9999")), "990001")
HEADER = (
    "Facility ID,Facility Name,Address,City/Town,State,ZIP Code,County/Parish,Telephone Number,"
    "Measure ID,Measure Name,Compared to National,Score,Footnote,Start Date,End Date"
)


def cli(module: str, args: list[str], timeout: float = 900) -> CompletedProcess[str]:
    """Start one review module exactly as an operator would."""
    return run_command(sys.executable, ["-m", module, *args], cwd=ROOT, timeout=timeout)


def tree_digest(folder: Path) -> list[Any]:
    """File count and one checksum over every file's relative path and checksum below a folder."""
    files = {str(path.relative_to(folder)): digest(path) for path in sorted(folder.rglob("*")) if path.is_file()}
    return [len(files), hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()]


def make_zip(members: dict[str, bytes]) -> bytes:
    """Build a deterministic archive in memory."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in sorted(members.items()):
            archive.writestr(zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0)), data)
    return buffer.getvalue()


def hospital_csv(rows: list[tuple[str, str, str, str]]) -> bytes:
    """A synthetic hospital table: (facility, measure, score, footnote) rows with planted name and telephone values."""
    lines = [HEADER]
    for facility, measure, score, footnote in rows:
        label = "Synthetic infection measure" + ("" if measure.endswith("SIR") else ": Observed Cases")
        contact = f"{facility},{PLANTED[0]},1 Example Way,Sampletown,ZZ,00000,Sample,{PLANTED[1]}"
        lines.append(f'{contact},{measure},{label},Not Available,{score},"{footnote}",01/01/2029,12/31/2029')
    return ("\n".join(lines) + "\n").encode()


def synthetic_source(root: Path) -> Path:
    """Write four synthetic captures and their inventory; return the inventory path."""
    first = [
        ("990001", "HAI_1_SIR", "0.500", ""),
        ("990001", "HAI_1_SIR", "0.500", ""),  # duplicate grain row
        ("990002", "HAI_1_SIR", "0.500", "3"),
        ("09000A", "HAI_1_SIR", "Not Available", "13, ***"),
        ("990001", "HAI_1_NUMERATOR", "0", ""),
        ("990002", "HAI_1_NUMERATOR", "4", ""),
        ("09000A", "HAI_1_NUMERATOR", "Not Available", "13"),
    ]
    second = [row if row[:2] != ("990002", "HAI_1_SIR") else ("990002", "HAI_1_SIR", "0.750", "3") for row in first[1:] if row[0] != "09000A"]
    dates = (
        b"Measure ID,Measure Name,Measure Start Quarter,Start Date,Measure End Quarter,End Date\n"
        b"HAI_1,Synthetic infection measure,1Q2029,01/01/2029,4Q2029,12/31/2029\n"
    )
    quarter = make_zip(
        {
            "Healthcare_Associated_Infections-Hospital.csv": hospital_csv(first),
            "__MACOSX/._Healthcare_Associated_Infections-Hospital.csv": b"\x00\x05\x16\x07synthetic resource fork",
            "Footnote_Crosswalk.csv": b"Footnote,Footnote Text\n3,Synthetic shorter period.\n13,Synthetic cannot be calculated.\n",
            "Measure_Dates.csv": dates,
            "Unrelated_Contacts.csv": f"Name,Telephone\n{PLANTED[0]},{PLANTED[1]}\n".encode(),
        }
    )
    archives = {
        "annual": ("hospitals_annual_2030.zip", make_zip({"hospitals_2030-01-15.zip": quarter, "manifest.json": b"{}"})),
        "revised": (
            "hospitals_2030-04-15.zip",
            make_zip({"77hc-ibv8_2030-04-01_Healthcare_Associated_Infections-Hospital.csv": hospital_csv(second), "Measure_Dates.csv": dates}),
        ),
        "changed": ("hospitals_2030-07-15.zip", make_zip({"Healthcare_Associated_Infections-Hospital.csv": hospital_csv(first)})),
        "absent": ("hospitals_2030-10-15.zip", make_zip({"Healthcare_Associated_Infections-Hospital.csv": hospital_csv(first)})),
    }
    candidates = []
    for label, (name, data) in sorted(archives.items()):
        capture = root / "captures" / label
        (capture / "raw").mkdir(parents=True)
        if label != "absent":
            (capture / "raw" / name).write_bytes(data + (b"tampered" if label == "changed" else b""))
        artifact = {
            "role": "data",
            "stored_file_name": name,
            "storage_path": f"raw/{name}",
            "byte_count": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
        receipt = capture / "receipt.json"
        receipt.write_text(json.dumps({"artifacts": [artifact]}, sort_keys=True))
        candidates.append(
            {
                "source_id": SOURCE,
                "snapshot_id": f"synthetic_{label}",
                "receipt": str(receipt.relative_to(root)),
                "receipt_sha256": digest(receipt),
                "status": "candidate_not_authorized_for_execution",
                "inventory_origin": "synthetic",
            }
        )
    inventory = root / "inventory.json"
    inventory.write_text(json.dumps({"candidates": candidates}, sort_keys=True))
    return inventory


def synthetic_review(assertions: dict[str, dict[str, Any]]) -> None:
    """Rejections, counting and redaction through the real archive-review CLI."""
    root = E2E / "synthetic" / "source_root"
    inventory = synthetic_source(root)
    output = E2E / "synthetic" / "hai_archives.json"
    result = cli("scripts.review.inspect_hai_archives", ["--source-root", str(root), "--inventory", str(inventory), "--output", str(output)])
    report = json.loads(output.read_text()) if result.returncode == 0 else {}
    status = {c["snapshot_id"]: c["review_status"] for c in report.get("captures", [])}
    expect(assertions, "synthetic_changed_bytes_rejected_as_artifact_mismatch", "artifact_mismatch", status.get("synthetic_changed"))
    expect(assertions, "synthetic_absent_archive_reported_not_local", "artifact_not_local", status.get("synthetic_absent"))
    expect(assertions, "synthetic_valid_archives_reviewed", ["archive_reviewed"] * 2, [status.get("synthetic_annual"), status.get("synthetic_revised")])
    nested = [a for a in report.get("archives", {}).values() if a["release"] == "2030-01-15"]
    expect(assertions, "synthetic_nested_archive_opened_once", 1, len(nested))
    expect(
        assertions,
        "synthetic_resource_fork_skipped_not_matched",
        [1, 1],
        [nested[0]["resource_fork_members_skipped"], len(nested[0]["tables"]["hai_hospital"])] if nested else None,
    )
    tables = [t for t in report.get("tables", {}).values() if t["class"] == "hai_hospital" and t["releases"] == ["2030-01-15"]]
    expect(assertions, "synthetic_duplicate_grain_row_counted", 1, tables[0]["issues"]["duplicate_key_rows"] if tables else None)
    expect(
        assertions,
        "synthetic_not_available_score_counted_not_zeroed",
        1,
        tables[0]["measures"]["HAI_1_SIR"]["score"].get("token:Not Available") if tables else None,
    )
    expect(assertions, "synthetic_symbol_footnote_code_counted", 1, tables[0]["measures"]["HAI_1_SIR"]["footnote_codes"].get("***") if tables else None)
    dated = [row["measure_dates_check"] for row in report.get("release_timeline", [])]
    expect(assertions, "synthetic_period_checked_against_measure_dates", [{"measure_date_tables": 1, "per_table": [{"agree": 2}]}] * 2, dated)
    totals = report.get("period_comparison", {}).get("transition_totals", [])
    observed = [totals[0].get("numeric_value_changed"), totals[0].get("entities_removed")] if len(totals) == 1 else None
    expect(assertions, "synthetic_revised_score_and_removed_entity_flagged", [1, 2], observed)
    text = output.read_text() if output.is_file() else "".join(PLANTED)
    expect(assertions, "synthetic_planted_name_phone_and_facility_id_absent_from_output", [False, False, False], [value in text for value in PLANTED])


def synthetic_inventory(assertions: dict[str, dict[str, Any]]) -> None:
    """The inventory builder rejects inconsistent amendments for the stated reason."""
    folder = E2E / "synthetic" / "inventory"
    folder.mkdir(parents=True)
    entry = {"snapshot_id": "one", "source_id": "SYNTHETIC", "status": "candidate_not_authorized_for_execution", "receipt": "r", "receipt_sha256": "0"}
    (folder / "base.json").write_text(json.dumps({"candidates": [entry]}))
    amendments = {
        "duplicate": {"added_candidates": [entry], "reclassified_candidates": [], "candidate_count_after": 2, "acquired_candidates_after": 2},
        "unknown": {
            "added_candidates": [],
            "reclassified_candidates": [{"snapshot_id": "missing", "from": entry["status"], "to": "failed_attempt_excluded", "replaced_by": "x"}],
            "candidate_count_after": 1,
            "acquired_candidates_after": 0,
        },
        "miscount": {
            "added_candidates": [dict(entry, snapshot_id="two")],
            "reclassified_candidates": [],
            "candidate_count_after": 5,
            "acquired_candidates_after": 5,
        },
    }
    reasons = {"duplicate": "added_snapshot_already_in_inventory", "unknown": "reclassified_snapshot_not_in_base", "miscount": "count_mismatch"}
    for name, amendment in amendments.items():
        (folder / f"{name}.json").write_text(json.dumps(amendment))
        args = ["--base", str(folder / "base.json"), "--amendment", str(folder / f"{name}.json"), "--output", str(folder / f"{name}_out.json")]
        result = cli("scripts.review.build_review_inventory", args)
        observed = [result.returncode, reasons[name] in result.stderr, (folder / f"{name}_out.json").exists()]
        expect(assertions, f"synthetic_inventory_{name}_rejected_for_stated_reason", [1, True, False], observed)


def expect(assertions: dict[str, dict[str, Any]], name: str, expected: Any, observed: Any) -> None:
    """Record one assertion with its expected and observed values."""
    assertions[name] = {"expected": expected, "observed": observed, "status": "pass" if expected == observed else "fail"}


def real_inventory(source_root: Path, assertions: dict[str, dict[str, Any]]) -> Path:
    """Rebuild the effective inventory twice from the frozen inputs and compare it with the source checkout."""
    inputs = REVIEW / "inputs"
    planning = source_root / "data" / "acquisition_planning" / "full_redownload_20260929"
    current = [digest(planning / "capture_inventory.json"), digest(planning / "capture_inventory_amendment1.json")]
    frozen = [digest(inputs / "capture_inventory.json"), digest(inputs / "capture_inventory_amendment1.json")]
    expect(assertions, "frozen_inputs_match_source_checkout_now", current, frozen)
    outputs = [inputs / "capture_inventory_effective.json", E2E / "rebuild" / "capture_inventory_effective.json"]
    for output in outputs:
        args = ["--base", str(inputs / "capture_inventory.json"), "--amendment", str(inputs / "capture_inventory_amendment1.json"), "--output", str(output)]
        cli("scripts.review.build_review_inventory", args)
    expect(assertions, "effective_inventory_rebuild_byte_identical", digest(outputs[0]), digest(outputs[1]))
    inventory = json.loads(outputs[0].read_text())
    base = json.loads((inputs / "capture_inventory.json").read_text())
    observed = [inventory["candidate_count"], inventory["accepted_count"], inventory["failed_attempt_count"], inventory["origin_counts"].get("amendment1")]
    expect(
        assertions, "effective_inventory_is_base_plus_12_with_2_failed_attempts", [len(base["candidates"]) + 12, len(base["candidates"]) + 10, 2, 12], observed
    )
    return outputs[0]


def real_audit(source_root: Path, inventory: Path, assertions: dict[str, dict[str, Any]]) -> None:
    """First-pass audit of the effective inventory, twice, reconciled against the inventory and the earlier audit."""
    for run in (1, 2):
        args = ["--source-root", str(source_root), "--inventory", str(inventory), "--output", str(REVIEW / f"audit_run{run}")]
        cli("scripts.review.review_remaining", args)
    expect(assertions, "audit_paired_outputs_byte_identical", tree_digest(REVIEW / "audit_run1"), tree_digest(REVIEW / "audit_run2"))
    report = json.loads((REVIEW / "audit_run1" / "report.json").read_text())
    earlier = json.loads((ROOT / "data" / "schema_review" / "2026_09_29" / "verified_run1" / "report.json").read_text())
    observed = [report["candidate_total"], report["included_candidates"], report["included_candidates"] + sum(report["excluded_candidates"].values())]
    expect(
        assertions,
        "audit_counts_reconcile_to_inventory_and_earlier_audit",
        [earlier["candidate_total"] + 12, earlier["included_candidates"] + 12, earlier["candidate_total"] + 12],
        observed,
    )
    verified: set[str] = set()
    missing: list[str] = []
    for profile in sorted((REVIEW / "audit_run1" / "sources").glob("*.json")):
        for candidate in json.loads(profile.read_text())["candidates"]:
            for artifact in candidate.get("artifacts", []):
                (verified.add if artifact["integrity"] == "verified" else missing.append)(artifact["sha256"])
    expect(assertions, "every_non_local_artifact_has_identical_bytes_verified_locally", [2, True], [len(missing), all(value in verified for value in missing)])


def real_review(source_root: Path, inventory: Path, assertions: dict[str, dict[str, Any]]) -> None:
    """HAI archive review of the real archives, twice, reconciled against the inventory and an earlier independent row count."""
    outputs = [REVIEW / f"hai_run{run}" / "hai_archives.json" for run in (1, 2)]
    for output in outputs:
        cli("scripts.review.inspect_hai_archives", ["--source-root", str(source_root), "--inventory", str(inventory), "--output", str(output)])
    expect(assertions, "hai_review_paired_outputs_byte_identical", digest(outputs[0]), digest(outputs[1]))
    report = json.loads(outputs[0].read_text())
    candidates = [c for c in json.loads(inventory.read_text())["candidates"] if c["source_id"] == SOURCE]
    expect(
        assertions,
        "every_hai_capture_has_an_explicit_status",
        sorted(c["snapshot_id"] for c in candidates),
        sorted(c["snapshot_id"] for c in report["captures"]),
    )
    unreviewed = [c for c in report["captures"] if c["review_status"] != "archive_reviewed"]
    expect(
        assertions,
        "only_unreviewed_capture_is_s3_only_with_identical_local_bytes",
        [["artifact_not_local", True]],
        [[c["review_status"], c.get("same_bytes_reviewed_locally")] for c in unreviewed],
    )
    hospital = [t for t in report["tables"].values() if t["class"] == "hai_hospital"]
    expect(
        assertions,
        "hospital_tables_all_profiled_with_unique_grain",
        [0, 0],
        [sum(t["status"] != "profiled" for t in hospital), sum(t["issues"]["duplicate_key_rows"] for t in hospital)],
    )
    # Independent evidence: row counts recorded in a capture receipt by the earlier file-first audit, before this code existed.
    receipt = next(c for c in candidates if "cms_annual_review" in c["receipt"])
    history = json.loads(json.loads((source_root / receipt["receipt"]).read_text())["release"]["observed_history"])
    rows = {release: table["rows"] for table in hospital for release in table["releases"]}
    expect(assertions, "hospital_row_counts_match_earlier_independent_audit", [h["row_count"] for h in history], [rows.get(h["release"]) for h in history])
    checks = [result for row in report["release_timeline"] for result in row["measure_dates_check"]["per_table"]]
    expect(
        assertions, "every_release_period_agrees_with_its_measure_date_table", [True, True], [bool(checks), all(set(result) == {"agree"} for result in checks)]
    )
    expect(assertions, "model_hold_retained", False, report["model_eligible"])


def main() -> int:
    """Run every scenario and write the evidence report."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-root", type=Path, required=True)
    source_root = parser.parse_args().source_root.resolve()
    for scratch in (E2E / "synthetic", E2E / "rebuild"):
        shutil.rmtree(scratch, ignore_errors=True)
    assertions: dict[str, dict[str, Any]] = {}
    synthetic_inventory(assertions)
    synthetic_review(assertions)
    inventory = real_inventory(source_root, assertions)
    real_audit(source_root, inventory, assertions)
    real_review(source_root, inventory, assertions)
    modules = ("build_review_inventory", "inspect_hai_archives", "review_remaining", "profile_sources", "run_hai_archive_e2e")
    failed = sorted(name for name, result in assertions.items() if result["status"] != "pass")
    report = {
        "feature": "Amended review inventory and CMS HAI archive content review",
        "created_utc": datetime.now(UTC).isoformat(),
        "environment": {"python": platform.python_version(), "platform": platform.platform(), "dependencies": "standard library only"},
        "code_revision": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, check=True).stdout.strip(),
        "uncommitted_review_code_sha256": {f"scripts/review/{name}.py": digest(ROOT / "scripts" / "review" / f"{name}.py") for name in modules},
        "input_sha256": {str(path.relative_to(ROOT)): digest(path) for path in sorted((REVIEW / "inputs").glob("*.json"))},
        "output_sha256": {
            str(path.relative_to(ROOT)): digest(path) for path in (REVIEW / "audit_run1" / "report.json", REVIEW / "hai_run1" / "hai_archives.json")
        },
        "reproduce": "From the review worktree: SOURCE_ROOT/.venv/bin/python -m scripts.review.run_hai_archive_e2e --source-root SOURCE_ROOT",
        "assertions": assertions,
        "result": "fail" if failed else "pass",
        "failed_assertions": failed,
        "limits": [
            "Offline local bytes only: no S3 or publisher request. Two artifacts held only in S3 are covered through identical local checksums.",
            "Synthetic scenarios use generated values; they prove rejection, counting and redaction paths, not publisher behavior.",
            "Row-count agreement with the earlier audit covers the two releases that audit recorded, not all 33.",
            "Review code is uncommitted and outside CI by user decision. No clean-checkout rebuild is claimed.",
            "No hold is cleared; outputs stay local and need the whole-project privacy review before publication.",
        ],
        "cleanup": "Synthetic inputs and the rebuilt inventory stay under data/e2e/hai_archive_review/; each run replaces them. No process is left running.",
    }
    write_json(E2E / "report.json", report)
    sys.stdout.write(json.dumps({"result": report["result"], "assertions": len(assertions), "failed": failed}) + "\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
