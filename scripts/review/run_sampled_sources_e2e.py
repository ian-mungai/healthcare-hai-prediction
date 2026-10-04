"""E2E evidence for the full-row profiling of the 15 sampled sources.

The two profiling runs take about 40 minutes each and are started separately. This runner checks that both used the
current reader and inventory and are byte-identical, rebuilds the summary twice through its real CLI, and reconciles
row totals for the single-table CSV sources with an independent newline count of the capture files. The reader's
parsing, redaction and rejection paths are covered by the member-review E2E (same unchanged reader).

Usage (the interpreter needs openpyxl, pypdf and xlrd)::

    PYTHONPATH=.review_dependencies BUNDLED_PYTHON -m scripts.review.run_sampled_sources_e2e --source-root SOURCE_ROOT
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import run_command
from scripts.review.review_remaining import digest, safe_path, write_json

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "schema_review" / "2026_10_01"
E2E = ROOT / "data" / "e2e" / "sampled_sources"
INVENTORY = ROOT / "data" / "schema_review" / "2026_09_30" / "inputs" / "capture_inventory_effective.json"
LINE_COUNTED = ("HHS", "CMS_GV", "CMS-MUP-DRG", "CMS_MEDICARE_PROVIDER")
MODULES = ("inspect_members", "profile_sampled_sources", "summarize_sampled_sources", "run_sampled_sources_e2e")


def expect(assertions: dict[str, dict[str, Any]], name: str, expected: Any, observed: Any) -> None:
    """Record one assertion with its expected and observed values."""
    assertions[name] = {"expected": expected, "observed": observed, "status": "pass" if expected == observed else "fail"}


def tree_digest(folder: Path) -> list[Any]:
    """File count and one checksum over every file's relative path and checksum below a folder."""
    files = {str(path.relative_to(folder)): digest(path) for path in sorted(folder.rglob("*")) if path.is_file()}
    return [len(files), hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()]


def line_count(source_root: Path, source: str) -> int:
    """Records in every distinct local CSV data artifact of one source: newline count minus the header line."""
    total, seen = 0, set()
    for candidate in json.loads(INVENTORY.read_text())["candidates"]:
        if candidate["source_id"] != source or candidate["status"] != "candidate_not_authorized_for_execution":
            continue
        receipt = safe_path(source_root, candidate["receipt"])
        for artifact in json.loads(receipt.read_text())["artifacts"]:
            if artifact["role"] != "data" or artifact["sha256"] in seen or not artifact["stored_file_name"].lower().endswith(".csv"):
                continue
            seen.add(artifact["sha256"])
            data = (receipt.parent / artifact["storage_path"]).read_bytes()
            total += data.count(b"\n") + (0 if data.endswith(b"\n") else 1) - 1
    return total


def main() -> int:
    """Run every check and write the evidence report."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-root", type=Path, required=True)
    source_root = parser.parse_args().source_root.resolve()
    assertions: dict[str, dict[str, Any]] = {}
    runs = [OUT / f"sampled_run{n}" for n in (1, 2)]
    reports = [json.loads((run / "report.json").read_text()) for run in runs]
    reader = digest(ROOT / "scripts" / "review" / "inspect_members.py")
    expect(
        assertions, "runs_used_current_reader_and_inventory", [[reader, digest(INVENTORY)]] * 2, [[r["code_sha256"], r["inventory_sha256"]] for r in reports]
    )
    expect(assertions, "runs_byte_identical", tree_digest(runs[0]), tree_digest(runs[1]))
    expect(assertions, "fifteen_sources_profiled", 15, len(reports[0]["sources"]))
    expect(assertions, "no_artifact_rejected", [], sorted(s for s in reports[0]["artifact_status"] if s in {"artifact_mismatch", "artifact_not_local"}))
    summaries = [E2E / f"summary_check{n}.json" for n in (1, 2)]
    for path in summaries:
        result = run_command(sys.executable, ["-m", "scripts.review.summarize_sampled_sources", "--profiles", str(runs[0]), "--output", str(path)], cwd=ROOT)
        expect(assertions, f"summary_cli_exit_{path.stem}", 0, result.returncode)
    expect(assertions, "summary_byte_identical", digest(summaries[0]), digest(summaries[1]))
    expect(assertions, "summary_matches_recorded_summary", digest(OUT / "sampled_summary_run1.json"), digest(summaries[0]))
    summary = json.loads(summaries[0].read_text())["sources"]
    expect(
        assertions,
        "rows_match_independent_line_count",
        {source: line_count(source_root, source) for source in LINE_COUNTED},
        {source: summary[source]["totals"]["data_rows"] for source in LINE_COUNTED},
    )
    expect(assertions, "model_hold_retained", [False, False], [r["model_eligible"] for r in reports])
    failed = sorted(name for name, value in assertions.items() if value["status"] != "pass")
    report = {
        "feature": "Full-row profiling of the 15 previously sampled sources",
        "created_utc": datetime.now(UTC).isoformat(),
        "environment": {"python": platform.python_version()},
        "code_revision": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, check=True).stdout.strip(),
        "uncommitted_review_code_sha256": {f"scripts/review/{name}.py": digest(ROOT / "scripts" / "review" / f"{name}.py") for name in MODULES},
        "input_sha256": {str(INVENTORY.relative_to(ROOT)): digest(INVENTORY)},
        "reproduce": [
            "PYTHONPATH=.review_dependencies BUNDLED_PYTHON -m scripts.review.profile_sampled_sources --source-root SOURCE_ROOT "
            "--inventory data/schema_review/2026_09_30/inputs/capture_inventory_effective.json --output data/schema_review/2026_10_01/sampled_runN (N = 1, 2)",
            "python -m scripts.review.summarize_sampled_sources --profiles data/schema_review/2026_10_01/sampled_run1 "
            "--output data/schema_review/2026_10_01/sampled_summary_run1.json",
            "PYTHONPATH=.review_dependencies BUNDLED_PYTHON -m scripts.review.run_sampled_sources_e2e --source-root SOURCE_ROOT",
        ],
        "assertions": assertions,
        "result": "fail" if failed else "pass",
        "failed_assertions": failed,
        "limits": [
            "Offline local bytes only; no S3 or publisher request.",
            "The reader's parsing, redaction and rejection paths are covered by the member-review E2E; this runner checks identity and totals.",
            "Line counts reconcile single-table CSV sources only. Review code is uncommitted and outside CI by decision. No hold is cleared.",
        ],
        "cleanup": "Summary checks under data/e2e/sampled_sources are replaced on each run. No process is left running.",
    }
    write_json(E2E / "report.json", report)
    sys.stdout.write(json.dumps({"result": report["result"], "assertions": len(assertions), "failed": failed}) + "\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
