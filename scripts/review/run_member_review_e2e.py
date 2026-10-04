"""E2E evidence for the dictionary passages and the member review (steps 2 to 5).

Synthetic inputs (generated values only) run through the real member-review CLI. The dictionary extraction runs twice.
The two full member-review runs take about 20 minutes each, so they are started separately; this runner checks that
both used the current code and inventory, and that their outputs are byte-identical.

Usage (the interpreter needs openpyxl, pypdf and xlrd)::

    PYTHONPATH=.review_dependencies BUNDLED_PYTHON -m scripts.review.run_member_review_e2e --source-root SOURCE_ROOT
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

import openpyxl

from scripts.process import CompletedProcess, run_command
from scripts.review.review_remaining import digest, write_json

ROOT = Path(__file__).resolve().parents[2]
REVIEW = ROOT / "data" / "schema_review" / "2026_09_30"
E2E = ROOT / "data" / "e2e" / "member_review"
INVENTORY = REVIEW / "inputs" / "capture_inventory_effective.json"
# The telephone value is assembled at run time so this file holds no telephone-shaped literal.
PLANTED = ("SYNTHETIC PLANTED PERSON", "-".join(("555", "010", "8888")))


def cli(module: str, args: list[str]) -> CompletedProcess[str]:
    """Start one review module exactly as an operator would."""
    return run_command(sys.executable, ["-m", module, *args], cwd=ROOT, timeout=1800)


def expect(assertions: dict[str, dict[str, Any]], name: str, expected: Any, observed: Any) -> None:
    """Record one assertion with its expected and observed values."""
    assertions[name] = {"expected": expected, "observed": observed, "status": "pass" if expected == observed else "fail"}


def tree_digest(folder: Path) -> list[Any]:
    """File count and one checksum over every file's relative path and checksum below a folder."""
    files = {str(path.relative_to(folder)): digest(path) for path in sorted(folder.rglob("*")) if path.is_file()}
    return [len(files), hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()]


def workbook_bytes() -> bytes:
    """A workbook with a title row above its header and a planted name and telephone number in its cells."""
    book = openpyxl.Workbook()
    sheet = book.worksheets[0]
    sheet.title = "Synthetic"
    for row in (["Synthetic report title"], ["Facility Number", "Contact", "Phone", "Beds"], ["000101", PLANTED[0], PLANTED[1], 12], ["000102", "", "", 30]):
        sheet.append(row)
    buffer = io.BytesIO()
    book.save(buffer)
    return buffer.getvalue()


def synthetic_members() -> dict[str, dict[str, bytes]]:
    """Synthetic sources: archive members and standalone files keyed by capture label."""
    sahie = "\n".join(
        [
            "Synthetic prose line one",
            "Synthetic prose line two",
            "year,version,statefips,countyfips,geocat,agecat,racecat,sexcat,iprcat,NIPR,contact",
            f"2030,1,01,001,40,0,0,0,0,100,{PLANTED[0]}",
            f"2030,1,01,001,40,0,0,0,0,100,{PLANTED[1]}",
            "2030,1,01,003,40,0,0,0,0,200,",
        ]
    )
    hcris = "\n".join(
        [
            "rpt_rec_num,Provider CCN,Fiscal Year Begin Date,Fiscal Year End Date,Number of Beds",
            "900001,010001,10/01/2029,09/30/2030,12",
            "900002,010001,10/01/2030,09/30/2031,12",
            "900003,020002,01/01/2030,12/31/2030,40",
        ]
    )
    utf16 = "PROV\tCBSA\tHours\n010001\t10100\t1234\n010002\t10100\t99\n".encode("utf-16")
    return {
        "SAHIE/synthetic_sahie": {"sahie.zip": _zip({"sahie_2030.csv": sahie.encode()})},
        "CMS_HCRIS_PUF/synthetic_hcris": {"CostReport_2030_Final.csv": hcris.encode()},
        "ACS/synthetic_headerless": {
            "seq.zip": _zip({"e2030xx0001000.txt": b"ACSSF,2030e5,xx,000,0001,0000001,15,27,3\nACSSF,2030e5,xx,000,0001,0000002,8,11,0\n"})
        },
        "CMS_OCCMIX/synthetic_utf16": {"wage.zip": _zip({"provwage.txt": utf16, "cmi.txt": b"010001 00863 01.8137\n010005 00373 01.3688\n"})},
        "HCAI_FINANCE/synthetic_workbook": {"hospitaldata.xlsx": workbook_bytes()},
        "HPSA/synthetic_changed": {"changed.csv": b"State,Count\nZZ,1\n"},
    }


def _zip(members: dict[str, bytes]) -> bytes:
    """Deterministic in-memory archive."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in sorted(members.items()):
            archive.writestr(zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0)), data)
    return buffer.getvalue()


def synthetic_source(root: Path) -> Path:
    """Write the synthetic captures with receipts and an inventory; return the inventory path."""
    candidates = []
    for label, files in synthetic_members().items():
        source, name = label.split("/")
        capture = root / name
        (capture / "raw").mkdir(parents=True)
        artifacts = []
        for file_name, data in files.items():
            (capture / "raw" / file_name).write_bytes(data + (b"tampered" if name == "synthetic_changed" else b""))
            artifacts.append(
                {
                    "role": "data",
                    "stored_file_name": file_name,
                    "storage_path": f"raw/{file_name}",
                    "byte_count": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
            )
        receipt = capture / "receipt.json"
        receipt.write_text(json.dumps({"artifacts": artifacts}, sort_keys=True))
        candidates.append(
            {"source_id": source, "snapshot_id": name, "receipt": str(receipt.relative_to(root)), "receipt_sha256": digest(receipt), "status": "synthetic"}
        )
    inventory = root / "inventory.json"
    inventory.write_text(json.dumps({"candidates": candidates}, sort_keys=True))
    return inventory


def first_table(report: dict[str, Any]) -> dict[str, Any]:
    """The profile of the only table member in a single-member synthetic source."""
    return next(iter(report["members"].values()))["profile"]


def synthetic_review(assertions: dict[str, dict[str, Any]]) -> None:
    """Header detection, keys, periods, encodings and redaction through the real CLI."""
    root = E2E / "synthetic" / "source_root"
    inventory = synthetic_source(root)
    output = E2E / "synthetic" / "members"
    sources = ["SAHIE", "CMS_HCRIS_PUF", "ACS", "CMS_OCCMIX", "HCAI_FINANCE", "HPSA"]
    result = cli("scripts.review.inspect_members", ["--source-root", str(root), "--inventory", str(inventory), "--output", str(output), "--sources", *sources])
    expect(assertions, "synthetic_cli_exit_code", 0, result.returncode)
    load = {source: json.loads((output / f"{source}.json").read_text()) for source in sources if (output / f"{source}.json").exists()}
    sahie = first_table(load["SAHIE"]) if "SAHIE" in load else {}
    key = sahie.get("candidate_keys", [{}])[0]
    expect(assertions, "prose_lines_above_header_skipped", [2, 3], [sahie.get("header_row_index"), sahie.get("data_rows_at_modal_width")])
    expect(assertions, "duplicate_composite_key_counted", ["tested", 1], [key.get("status"), key.get("duplicate_rows")])
    county: dict[str, Any] = next((c["identifier"] for c in sahie.get("columns", []) if c["name"] == "countyfips"), {})
    expect(assertions, "leading_zero_identifier_counted", 2, county.get("leading_zero_values"))
    hcris = first_table(load["CMS_HCRIS_PUF"]) if "CMS_HCRIS_PUF" in load else {}
    periods = hcris.get("reporting_periods") or {}
    expect(
        assertions,
        "hcris_period_outside_file_year_and_repeat_ccn_counted",
        [1, 1],
        [periods.get("begin_outside_file_fiscal_year"), periods.get("ccns_with_several_reports")],
    )
    acs = first_table(load["ACS"]) if "ACS" in load else {}
    expect(assertions, "headerless_numeric_file_gets_no_header", [None, 2], [acs.get("header_row_index", "missing"), acs.get("data_rows_at_modal_width")])
    occmix = {m["locations"][0]["location"].split("!")[-1]: m["profile"] for m in load.get("CMS_OCCMIX", {}).get("members", {}).values()}
    expect(
        assertions,
        "utf16_text_read_as_table",
        ["utf-16", "table", 2],
        [occmix.get("provwage.txt", {}).get(k) for k in ("encoding", "status", "data_rows_at_modal_width")],
    )
    expect(
        assertions,
        "whitespace_columns_read_as_table",
        ["whitespace", 3],
        [occmix.get("cmi.txt", {}).get("delimiter"), len(occmix.get("cmi.txt", {}).get("columns", []))],
    )
    sheet = next(iter(first_table(load["HCAI_FINANCE"])["sheets"].values())) if "HCAI_FINANCE" in load else {}
    expect(assertions, "workbook_title_row_skipped", [1, 2], [sheet.get("header_row_index"), sheet.get("data_rows_at_modal_width")])
    changed = load.get("HPSA", {}).get("captures", [{}])[0].get("artifacts", [{}])[0].get("status")
    expect(assertions, "changed_artifact_rejected", "artifact_mismatch", changed)
    text = "".join(path.read_text() for path in sorted(output.glob("*.json")))
    expect(assertions, "planted_name_and_phone_absent_from_output", [False, False], [value in text for value in PLANTED])


def real_checks(source_root: Path, assertions: dict[str, dict[str, Any]]) -> None:
    """Dictionary extraction twice; the two separately started member runs checked for code, input and output identity."""
    outputs = [E2E / f"passages_run{run}.json" for run in (1, 2)]
    for output in outputs:
        cli("scripts.review.extract_dictionary_passages", ["--source-root", str(source_root), "--inventory", str(INVENTORY), "--output", str(output)])
    expect(assertions, "dictionary_passages_byte_identical", digest(outputs[0]), digest(outputs[1]))
    passages = json.loads(outputs[0].read_text())
    july = "July 2021" in passages["documents_matching"].get("q1_q2_2020_not_reported", [])
    expect(assertions, "july_2021_dictionary_states_q1_q2_2020_not_reported", True, july)
    runs = [REVIEW / f"members_run{run}" for run in (1, 2)]
    reports = [json.loads((run / "report.json").read_text()) for run in runs]
    code = digest(ROOT / "scripts" / "review" / "inspect_members.py")
    expect(
        assertions, "member_runs_used_current_code_and_inventory", [[code, digest(INVENTORY)]] * 2, [[r["code_sha256"], r["inventory_sha256"]] for r in reports]
    )
    expect(assertions, "member_runs_byte_identical", tree_digest(runs[0]), tree_digest(runs[1]))
    hcris = json.loads((runs[0] / "CMS_HCRIS_PUF.json").read_text())["members"].values()
    profiled = {m["locations"][0]["location"]: m["profile"]["data_rows_at_modal_width"] for m in hcris if m["profile"]["status"] == "table"}
    # Independent count: newline-terminated records minus the header, read straight from the capture files.
    counted: dict[str, int] = {}
    for path in sorted(source_root.glob("data/**/raw/CostReport_20*_Final.csv")):
        counted.setdefault(path.name, path.read_bytes().count(b"\n") - 1)
    expect(assertions, "hcris_rows_match_independent_line_count", dict(sorted(counted.items())), dict(sorted(profiled.items())))
    expect(assertions, "model_hold_retained", [False, False], [r["model_eligible"] for r in reports])


def main() -> int:
    """Run every scenario and write the evidence report."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-root", type=Path, required=True)
    source_root = parser.parse_args().source_root.resolve()
    shutil.rmtree(E2E / "synthetic", ignore_errors=True)
    assertions: dict[str, dict[str, Any]] = {}
    synthetic_review(assertions)
    real_checks(source_root, assertions)
    failed = sorted(name for name, result in assertions.items() if result["status"] != "pass")
    modules = ("extract_dictionary_passages", "inspect_members", "run_member_review_e2e")
    report = {
        "feature": "HAI dictionary passages and full member review of the step 3 to 5 sources",
        "created_utc": datetime.now(UTC).isoformat(),
        "environment": {"python": platform.python_version(), "openpyxl": openpyxl.__version__},
        "code_revision": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, check=True).stdout.strip(),
        "uncommitted_review_code_sha256": {f"scripts/review/{name}.py": digest(ROOT / "scripts" / "review" / f"{name}.py") for name in modules},
        "input_sha256": {str(INVENTORY.relative_to(ROOT)): digest(INVENTORY)},
        "reproduce": [
            "PYTHONPATH=.review_dependencies BUNDLED_PYTHON -m scripts.review.inspect_members --source-root SOURCE_ROOT "
            "--inventory data/schema_review/2026_09_30/inputs/capture_inventory_effective.json --output data/schema_review/2026_09_30/members_runN (N = 1, 2)",
            "PYTHONPATH=.review_dependencies BUNDLED_PYTHON -m scripts.review.run_member_review_e2e --source-root SOURCE_ROOT",
        ],
        "assertions": assertions,
        "result": "fail" if failed else "pass",
        "failed_assertions": failed,
        "limits": [
            "Offline local bytes only; no S3 or publisher request.",
            "Synthetic scenarios use generated values; they prove parsing, counting and redaction paths, not publisher behavior.",
            "The two full member runs were started separately from this runner; it verifies their code, input and output identity.",
            "Fixed-width files without a layout are counted by line length only. PDFs other than the HAI dictionaries get page and text counts only.",
            "Review code is uncommitted and outside CI by decision. No hold is cleared.",
        ],
        "cleanup": "Synthetic inputs under data/e2e/member_review/synthetic are replaced on each run. No process is left running.",
    }
    write_json(E2E / "report.json", report)
    sys.stdout.write(json.dumps({"result": report["result"], "assertions": len(assertions), "failed": failed}) + "\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
