"""E2E evidence for review items 1 to 5 of Oct 1 2026: documents, reference layouts, SAIPE, HAI archive members, deep archives.

Each step's two real runs were started separately. This runner checks that each pair used the current code and
inventory and is byte-identical, and reconciles selected counts: every SAIPE file has one national and 51 state
records, and the HAI member review covers every capture of the HAI archive review.

Usage::

    PYTHONPATH=.review_dependencies BUNDLED_PYTHON -m scripts.review.run_remaining_items_e2e
"""

from __future__ import annotations

import hashlib
import json
import platform
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import run_command
from scripts.review.review_remaining import digest, write_json

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "schema_review" / "2026_10_01"
E2E = ROOT / "data" / "e2e" / "remaining_items"
INVENTORY = ROOT / "data" / "schema_review" / "2026_09_30" / "inputs" / "capture_inventory_effective.json"
MODULES = ("catalog_documents", "profile_saipe", "inspect_members", "inspect_deep_archives", "summarize_sampled_sources", "run_remaining_items_e2e")


def expect(assertions: dict[str, dict[str, Any]], name: str, expected: Any, observed: Any) -> None:
    """Record one assertion with its expected and observed values."""
    assertions[name] = {"expected": expected, "observed": observed, "status": "pass" if expected == observed else "fail"}


def tree_digest(path: Path) -> list[Any]:
    """File count and one checksum over every file below a folder (or one file)."""
    paths = [path] if path.is_file() else sorted(p for p in path.rglob("*") if p.is_file())
    files = {"file" if p == path else str(p.relative_to(path)): digest(p) for p in paths}
    return [len(files), hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()]


def code(name: str) -> str:
    """Checksum of one review module."""
    return digest(ROOT / "scripts" / "review" / f"{name}.py")


def main() -> int:
    """Run every check and write the evidence report."""
    assertions: dict[str, dict[str, Any]] = {}
    pairs = {
        "documents": ("documents_run1", "documents_run2", "catalog.json", "catalog_documents"),
        "saipe": ("saipe_run1.json", "saipe_run2.json", None, "profile_saipe"),
        "hai_members": ("hai_members_run1", "hai_members_run2", "report.json", "inspect_members"),
        "deep_archives": ("deep_archives_run1.json", "deep_archives_run2.json", None, "inspect_deep_archives"),
    }
    for name, (first, second, report_name, module) in pairs.items():
        expect(assertions, f"{name}_runs_byte_identical", tree_digest(OUT / first), tree_digest(OUT / second))
        report = json.loads(((OUT / first / report_name) if report_name else OUT / first).read_text())
        expect(assertions, f"{name}_used_current_code_and_inventory", [code(module), digest(INVENTORY)], [report["code_sha256"], report["inventory_sha256"]])
        expect(assertions, f"{name}_model_hold_retained", False, report["model_eligible"])
    saipe = json.loads((OUT / "saipe_run1.json").read_text())["files"]
    full = {name: f["levels"] for name, f in saipe.items() if name.endswith(("all.dat", "all.txt"))}
    expect(
        assertions,
        "saipe_every_full_file_has_one_national_and_51_state_records",
        [[1, 51]] * len(full),
        [[lv.get("national"), lv.get("state_total")] for lv in full.values()],
    )
    hai_archive = json.loads((ROOT / "data" / "schema_review" / "2026_09_30" / "hai_run1" / "hai_archives.json").read_text())["capture_status"]
    members = json.loads((OUT / "hai_members_run1" / "report.json").read_text())
    expect(assertions, "hai_member_review_covers_every_archive_capture", sum(hai_archive.values()), sum(members["captures"]["main-hai-pdc"].values()))
    catalog = json.loads((OUT / "documents_run1" / "catalog.json").read_text())
    expect(assertions, "every_document_source_cataloged", 15, len(catalog["by_source"]))
    failed = sorted(name for name, value in assertions.items() if value["status"] != "pass")
    write_json(
        E2E / "report.json",
        {
            "feature": "Review items 1 to 5 (Oct 1 2026)",
            "created_utc": datetime.now(UTC).isoformat(),
            "environment": {"python": platform.python_version()},
            "code_revision": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, check=True).stdout.strip(),
            "uncommitted_review_code_sha256": {f"scripts/review/{m}.py": code(m) for m in MODULES},
            "input_sha256": {str(INVENTORY.relative_to(ROOT)): digest(INVENTORY)},
            "assertions": assertions,
            "result": "fail" if failed else "pass",
            "failed_assertions": failed,
            "limits": [
                "Offline local bytes only; no S3 or publisher request.",
                "The real runs were started separately; this runner checks their identity and selected counts.",
                "Review code is uncommitted and outside CI by decision. No hold is cleared.",
            ],
            "cleanup": "No process is left running.",
        },
    )
    sys.stdout.write(json.dumps({"result": "fail" if failed else "pass", "assertions": len(assertions), "failed": failed}) + "\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
