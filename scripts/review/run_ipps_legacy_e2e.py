"""Exercise the IPPS legacy CLI twice and its real persistence/integrity boundaries with synthetic sources.

Run on the bundled Python with the approved isolated dependencies on PYTHONPATH:
``python -m scripts.review.run_ipps_legacy_e2e --source-root ROOT``. Artifacts remain local and uncommitted.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import io
import json
import platform
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pdfplumber
import xlrd

from scripts.process import run_command
from scripts.review.review_remaining import digest, write_json

ROOT = Path(__file__).resolve().parents[2]
REVIEW = ROOT / "data/schema_review/2026_09_30"
E2E = ROOT / "data/e2e/ipps_legacy"
MODULE = "scripts.review.inspect_ipps_legacy"
INVENTORY = REVIEW / "inputs/capture_inventory_effective.json"


def cli(arguments: list[str], label: str) -> int:
    """Call the real CLI, retaining only its safe stdout and error categories."""
    result = run_command(sys.executable, ["-m", MODULE, *arguments], cwd=ROOT, timeout=120)
    (E2E / f"{label}_stdout.log").write_text(result.stdout)
    (E2E / f"{label}_stderr.log").write_text(result.stderr)
    return result.returncode


def synthetic(root: Path, variant: str) -> Path:
    """Independent fixed positions and planted synthetic text, bound to ordinary receipt artifacts."""
    root.mkdir(parents=True, exist_ok=True)
    fields = [
        {"start": 1, "width": 6, "format": "$6.", "declared_end": 6, "page": 1, "title": "Provider Number"},
        {"start": 8, "width": 6, "format": "6.2", "declared_end": 13, "page": 1, "title": "Measure"},
        {"start": 15, "width": 30, "format": "$30.", "declared_end": 44, "page": 1, "title": "Contact"},
    ]
    rows = []
    for provider, measure, gap in [("000101", "12.34", " "), ("000101", ".", " "), ("000102", "", "!")]:
        rows.append(provider + gap + measure.rjust(6) + " " + "SYNTHETIC PLANTED PERSON".ljust(30))
    content = ("\r\n".join([*rows, "000103", "\x1a"]) + "\r\n").encode()
    member = "../synthetic.txt" if variant == "unsafe_member" else "synthetic.txt"
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(member, content)
    files = {"data.zip": stream.getvalue(), "layout.pdf": b"SYNTHETIC pinned dictionary; profile uses a frozen plan"}
    artifacts = []
    for name, data in files.items():
        (root / name).write_bytes(data)
        artifacts.append({"stored_file_name": name, "storage_path": name, "byte_count": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    receipt = root / "receipt.json"
    write_json(receipt, {"artifacts": artifacts})
    specs = [{"receipt": "receipt.json", "receipt_sha256": digest(receipt), "name": a["stored_file_name"], "sha256": a["sha256"]} for a in artifacts]
    pair: dict[str, Any] = {
        "year": 1994,
        "mode": "fixed_width",
        "data": specs[0],
        "dictionary": specs[1],
        "fields": fields,
        "member": member,
        "member_sha256": hashlib.sha256(content).hexdigest(),
    }
    if variant == "receipt_mismatch":
        pair["dictionary"]["receipt_sha256"] = "0" * 64
    elif variant == "artifact_missing":
        # Point to a nonexistent data path in the receipt; no existing file is deleted.
        artifacts[0]["storage_path"] = "absent.zip"
        write_json(receipt, {"artifacts": artifacts})
        for spec in specs:
            spec["receipt_sha256"] = digest(receipt)
    elif variant == "artifact_mismatch":
        (root / "data.zip").write_bytes(b"SYNTHETIC wrong bytes")
    elif variant == "member_mismatch":
        pair["member_sha256"] = "0" * 64
    elif variant == "overlapping_layout":
        pair["fields"] = copy.deepcopy(fields)
        pair["fields"][1]["start"] = 6
    plan = root / "plan.json"
    write_json(plan, {"years": [pair]})
    return plan


def main() -> int:
    """Record every assertion and the exact offline tested boundary."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    args = parser.parse_args()
    E2E.mkdir(parents=True, exist_ok=True)
    checks: dict[str, dict[str, Any]] = {}

    def expect(name: str, expected: Any, observed: Any) -> None:
        checks[name] = {"expected": expected, "observed": observed, "status": "pass" if observed == expected else "fail"}

    for number in (1, 2):
        plan = REVIEW / f"ipps_plan_run{number}.json"
        output = REVIEW / f"ipps_run{number}.json"
        code = cli(["build-plan", "--source-root", str(args.source_root), "--inventory", str(INVENTORY), "--output", str(plan)], f"plan{number}")
        expect(f"build_plan_{number}", 0, code)
        code = cli(["profile", "--source-root", str(args.source_root), "--plan", str(plan), "--output", str(output)], f"profile{number}")
        expect(f"profile_{number}", 0, code)
    expect("paired_plans_identical", True, (REVIEW / "ipps_plan_run1.json").read_bytes() == (REVIEW / "ipps_plan_run2.json").read_bytes())
    expect("paired_profiles_identical", True, (REVIEW / "ipps_run1.json").read_bytes() == (REVIEW / "ipps_run2.json").read_bytes())
    result = json.loads((REVIEW / "ipps_run1.json").read_text())
    plan_data = json.loads((REVIEW / "ipps_plan_run1.json").read_text())
    expect("all_eight_years", list(range(1994, 2002)), [y["year"] for y in result["years"]])
    expect("all_report_rows", 41542, sum(y["rows"] for y in result["years"]))
    expect("no_prose_year_as_field", False, any(f["start"] == 199 for y in plan_data["years"] for f in y["fields"]))
    expect("no_unmapped_bytes", 0, sum(y.get("rows_with_nonblank_unmapped_bytes", 0) for y in result["years"]))
    grain_records = [record for year in result["years"] for record in (year["sheets"] if year["mode"] == "workbook" else [year])]
    expect("no_duplicate_providers", 0, sum(record["provider_duplicate_rows"] for record in grain_records))
    expect("no_malformed_providers", 0, sum(record["provider_malformed"] for record in grain_records))
    workbook_records = [sheet for year in result["years"] if year["mode"] == "workbook" for sheet in year["sheets"]]
    expect("no_workbook_error_cells", 0, sum(sheet["excel_error_cells"] for sheet in workbook_records))
    expect("no_numeric_workbook_provider_cells", 0, sum(sheet["numeric_provider_cells"] for sheet in workbook_records))
    expect(
        "no_numeric_field_text_exceptions",
        0,
        sum(
            field["categories"].get("text", 0)
            for year in result["years"]
            if year["mode"] == "fixed_width"
            for field in year["fields"]
            if not field["format"].startswith("$")
        ),
    )
    expect("no_short_rows", 0, sum(y.get("short_rows", 0) for y in result["years"]))
    expect("seven_dos_markers", 7, sum(y.get("terminal_dos_markers", 0) for y in result["years"]))
    expect(
        "fy2001_native_workbook",
        ["workbook", 34, 4983],
        [result["years"][-1]["mode"], result["years"][-1]["sheets"][0]["columns"], result["years"][-1]["rows"]],
    )
    # Independent count from the archive records, not from the profiler's counters or helper functions.
    counts = []
    for pair in plan_data["years"][:-1]:
        receipt = args.source_root / pair["data"]["receipt"]
        artifact = next(a for a in json.loads(receipt.read_text())["artifacts"] if a["stored_file_name"] == pair["data"]["name"])
        with zipfile.ZipFile(receipt.parent / artifact["storage_path"]) as archive:
            records = archive.read(pair["member"]).split(b"\r\n")
        counts.append(sum(bool(record) and record != b"\x1a" for record in records))
    expect("independent_fixed_record_counts", counts, [y["rows"] for y in result["years"][:-1]])
    for variant in ("valid", "receipt_mismatch", "artifact_missing", "artifact_mismatch", "member_mismatch", "unsafe_member", "overlapping_layout"):
        root = E2E / "synthetic" / variant
        plan = synthetic(root, variant)
        output = root / "output.json"
        code = cli(["profile", "--source-root", str(root), "--plan", str(plan), "--output", str(output)], variant)
        expect(f"synthetic_{variant}_exit", 0 if variant == "valid" else 1, code)
        if variant == "valid":
            report = json.loads(output.read_text())["years"][0]
            expect(
                "synthetic_rows_and_grain",
                [4, 1, 4, 1, 1, 1],
                [
                    report[k]
                    for k in [
                        "rows",
                        "provider_duplicate_rows",
                        "provider_leading_zero_rows",
                        "short_rows",
                        "rows_with_nonblank_unmapped_bytes",
                        "terminal_dos_markers",
                    ]
                ],
            )
            expect("synthetic_measure_categories", {"numeric": 1, "sas_missing": 1, "empty": 2}, report["fields"][1]["categories"])
            expect("synthetic_no_row_text_leaked", False, "SYNTHETIC PLANTED PERSON" in output.read_text())
        else:
            expect(f"synthetic_{variant}_cause", True, f"IPPS review refused: {variant}" in (E2E / f"{variant}_stderr.log").read_text())
            expect(f"synthetic_{variant}_no_output", False, output.exists())
    record = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "feature": "offline IPPS legacy layout review",
        "assertions": checks,
        "status": "pass" if all(c["status"] == "pass" for c in checks.values()) else "fail",
        "environment": {"python": platform.python_version(), "pdfplumber": pdfplumber.__version__, "xlrd": xlrd.__version__},
        "code_sha256": {p.name: digest(p) for p in [Path(__file__), ROOT / "scripts/review/inspect_ipps_legacy.py"]},
        "inventory_sha256": digest(INVENTORY),
        "plan_sha256": digest(REVIEW / "ipps_plan_run1.json"),
        "output_sha256": digest(REVIEW / "ipps_run1.json"),
        "reproduce": "PYTHONPATH=.review_dependencies BUNDLED_PYTHON -m scripts.review.run_ipps_legacy_e2e --source-root SOURCE_ROOT",
        "limits": [
            "offline local source bytes only",
            "synthetic negative scenarios",
            "no staging transformations or model eligibility",
            "review code uncommitted; no clean-checkout reproduction claim",
            "full CI and conformance wiring deferred by user",
        ],
        "cleanup": "foreground commands exited; no process, network request, S3 write or source mutation",
    }
    write_json(E2E / "report.json", record)
    sys.stdout.write(json.dumps({"status": record["status"], "assertions": len(checks)}) + "\n")
    return 0 if record["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
