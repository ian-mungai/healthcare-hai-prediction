"""E2E evidence for the final review pass: HAI measure definitions and cross-source keys.

Synthetic captures (generated values only) run through the real key-extraction and comparison CLIs. The real runs
take tens of minutes, so they are started separately; this runner checks that each pair used the current code and
inventory, that each pair is byte-identical, and reconciles selected counts against earlier independent reviews.

Usage (the interpreter needs openpyxl, pypdf and xlrd)::

    PYTHONPATH=.review_dependencies BUNDLED_PYTHON -m scripts.review.run_final_pass_e2e --source-root SOURCE_ROOT
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import openpyxl

from scripts.process import CompletedProcess, run_command
from scripts.review.review_remaining import digest, write_json

ROOT = Path(__file__).resolve().parents[2]
REVIEW = ROOT / "data" / "schema_review" / "2026_09_30"
FINAL = REVIEW / "final_pass"
E2E = ROOT / "data" / "e2e" / "final_pass"
INVENTORY = REVIEW / "inputs" / "capture_inventory_effective.json"
MODULES = ("extract_hai_definitions", "extract_cross_source_keys", "analyze_cross_source_keys", "run_final_pass_e2e")


def cli(module: str, args: list[str]) -> CompletedProcess[str]:
    """Start one review module exactly as an operator would."""
    return run_command(sys.executable, ["-m", module, *args], cwd=ROOT, timeout=3600)


def expect(assertions: dict[str, dict[str, Any]], name: str, expected: Any, observed: Any) -> None:
    """Record one assertion with its expected and observed values."""
    assertions[name] = {"expected": expected, "observed": observed, "status": "pass" if expected == observed else "fail"}


def synthetic_files() -> dict[str, dict[str, bytes]]:
    """Synthetic captures keyed by source and capture name; every value is generated."""
    return {
        "MMD/synthetic_mmd": {
            "mmd_ffs_county_c1_prevalence_2030.csv": b"geography,year,fips,analysis_value\n"
            b"County,2030,1001,5\nCounty,2030,6037.0,5\nState/Territory,2030,1,5\nCounty,2030,9110,5\nCounty,2030,9001,5\nCounty,2030,9990,5\n"
        },
        "ACS/synthetic_acs": {"acsdt5y2030-b00001.dat": b"GEO_ID|B00001_E001\n0500000US01001|10\n1400000US01001020100|3\n0400000US01|99\n"},
        "SAHIE/synthetic_sahie": {"sahie-2030.csv": b"Synthetic prose line\nyear,statefips,countyfips,geocat\n2030,1,1,50\n2030,6,37,50\n2030,6,0,40\n"},
        "HUD/synthetic_hud": {"ZIP-COUNTY_122030.csv": b"zip,county,tot_ratio\n00501,36103,1\n10001,36061,0.6\n10001,36047,0.4\n1234,01001,1\nABCDE,01001,1\n"},
        "CMS_POS/synthetic_pos": {
            "POS_OTHER_DEC30.csv": b"PRVDR_NUM,PRVDR_CTGRY_CD,ZIP_CD,FIPS_STATE_CD,FIPS_CNTY_CD\n"
            b"330101,01,10001,36,61\n33013,01,00501,36,103\n335001,02,10001,36,61\n"
        },
        "ADJ/synthetic_adj": {"county_adjacency2010.txt": b"Synthetic A|01001|Synthetic B|01003|\nSynthetic B|01003|Synthetic A|01001|\n"},
        "RUCC/synthetic_nokey": {"2030-codes.csv": b"alpha,beta\n1,2\n"},
        "CMS_HCRIS_PUF/synthetic_changed": {"CostReport_2030_Final.csv": b"Provider CCN,Zip Code\n010001,35004-\n"},
    }


def synthetic_source(root: Path) -> Path:
    """Write the synthetic captures with receipts and an inventory; one artifact is altered after its checksum is taken."""
    candidates = []
    for label, files in synthetic_files().items():
        source, name = label.split("/")
        capture = root / name
        (capture / "raw").mkdir(parents=True)
        artifacts = []
        for file_name, data in files.items():
            (capture / "raw" / file_name).write_bytes(data + (b"tampered\n" if name == "synthetic_changed" else b""))
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
            {
                "source_id": source,
                "snapshot_id": name,
                "receipt": str(receipt.relative_to(root)),
                "receipt_sha256": digest(receipt),
                "status": "candidate_not_authorized_for_execution",
            }
        )
    inventory = root / "inventory.json"
    inventory.write_text(json.dumps({"candidates": candidates}, sort_keys=True))
    return inventory


def synthetic_checks(assertions: dict[str, dict[str, Any]]) -> None:
    """Normalization, classification, pairing and rejection through the real extraction and comparison CLIs."""
    root = E2E / "synthetic" / "source_root"
    inventory = synthetic_source(root)
    output = E2E / "synthetic" / "keys"
    sources = ["MMD", "ACS", "SAHIE", "HUD", "CMS_POS", "ADJ", "RUCC", "CMS_HCRIS_PUF"]
    only = [part for source in sources for part in ("--only", source)]
    result = cli("scripts.review.extract_cross_source_keys", ["--source-root", str(root), "--inventory", str(inventory), "--output", str(output), *only])
    expect(assertions, "synthetic_extract_exit_code", 0, result.returncode)
    report = json.loads((output / "report.json").read_text())["sources"]
    keys = json.loads((output / "keysets.json").read_text())["keysets"]
    expect(assertions, "mmd_state_row_filtered_and_codes_padded", ["01001", "06037", "09001", "09110", "09990"], keys["MMD"]["county"]["all"]["2030"])
    expect(
        assertions,
        "mmd_padding_and_float_suffix_counted",
        [5, 1],
        [report["MMD"]["normalization_counts"].get("county_padded_4_to_5"), report["MMD"]["normalization_counts"].get("float_suffix_stripped")],
    )
    expect(
        assertions,
        "acs_county_prefix_parsed_other_levels_dropped",
        [["01001"], 2],
        [keys["ACS"]["county"]["all"]["2030"], report["ACS"]["normalization_counts"].get("county_other_geography_level_dropped")],
    )
    expect(assertions, "sahie_state_and_county_parts_composed_after_prose", ["01001", "06000", "06037"], keys["SAHIE"]["county"]["all"]["2030"])
    expect(
        assertions,
        "hud_zip_padded_invalid_kept_out_and_pairs_recorded",
        [["00501", "01234", "10001"], 1, ["00501|36103", "01234|01001", "10001|36047", "10001|36061"]],
        [keys["HUD"]["zip"]["all"]["2030-12"], report["HUD"]["normalization_counts"].get("zip_invalid"), keys["HUD"]["zip|county"]["all"]["2030-12"]],
    )
    expect(
        assertions,
        "pos_hospital_subset_padding_and_county_pairs",
        [["033013", "330101"], ["033013|36103", "330101|36061"]],
        [keys["CMS_POS"]["hospital"]["hospital_category_01"]["2030"], keys["CMS_POS"]["hospital|county"]["hospital_category_01"]["2030"]],
    )
    expect(assertions, "headerless_adjacency_read_by_position", ["01001", "01003"], keys["ADJ"]["county"]["all"]["2010"])
    expect(assertions, "table_without_key_columns_reported", ["no_key_columns"], [t["status"] for t in report["RUCC"]["tables"]])
    expect(assertions, "changed_artifact_rejected", {"artifact_mismatch": 1}, report["CMS_HCRIS_PUF"]["artifact_status"])
    comparison = E2E / "synthetic" / "cross_source.json"
    result = cli("scripts.review.analyze_cross_source_keys", ["--keysets", str(output / "keysets.json"), "--output", str(comparison)])
    expect(assertions, "synthetic_analyze_exit_code", 0, result.returncode)
    universes = json.loads(comparison.read_text())["county_universes"]
    mmd = universes.get("MMD|all|2030", {})
    expect(
        assertions,
        "county_codes_classified",
        {"connecticut_legacy_county": 1, "connecticut_planning_region": 1, "state_county": 2, "unknown_or_unassigned_county": 1},
        mmd.get("categories"),
    )
    expect(assertions, "connecticut_mixed_coding_flagged", "both", mmd.get("connecticut"))


def pair_identity(assertions: dict[str, dict[str, Any]], name: str, paths: list[Path], code: str, inventory_key: str | None) -> list[dict[str, Any]]:
    """Both outputs of a pair exist, match byte for byte and record the current code (and inventory when stated)."""
    documents = [json.loads(path.read_text()) for path in paths]
    expect(assertions, f"{name}_byte_identical", digest(paths[0]), digest(paths[1]))
    observed = [[d.get("code_sha256"), d.get(inventory_key) if inventory_key else None] for d in documents]
    wanted = [[digest(ROOT / "scripts" / "review" / f"{code}.py"), digest(INVENTORY) if inventory_key else None]] * 2
    expect(assertions, f"{name}_used_current_code_and_inputs", wanted, observed)
    return documents


def real_checks(assertions: dict[str, dict[str, Any]]) -> None:
    """Identity of the separately started real runs and reconciliation against earlier independent reviews."""
    definitions = pair_identity(assertions, "definitions", [FINAL / f"definitions_run{n}.json" for n in (1, 2)], "extract_hai_definitions", "inventory_sha256")[
        0
    ]
    expect(assertions, "thirty_dictionary_editions_read", 30, definitions["distinct_dictionaries"])
    absent = {
        name: sorted(e["edition"] for e in definitions["edition_documents"] if e["section_status"].get(name) == "section_absent")
        for name in ("hospital_table", "footnotes", "description")
    }
    expect(assertions, "only_known_sections_absent", {"description": [], "footnotes": ["July 2026"], "hospital_table": ["July 2021"]}, absent)
    checks = definitions["sir_checks"].values()
    expect(
        assertions,
        "every_published_sir_equals_observed_over_predicted",
        True,
        sum(c["sir_checked"] for c in checks) > 0 and all(c["sir_checked"] == c["sir_equals_observed_over_predicted"] for c in checks),
    )
    families = definitions["measure_families"]
    expect(assertions, "dictionary_and_data_measure_families_agree", families["dictionary"], families["hospital_tables"])
    keys = pair_identity(assertions, "keys_report", [FINAL / f"keys_run{n}" / "report.json" for n in (1, 2)], "extract_cross_source_keys", "inventory_sha256")[
        0
    ]
    pair_identity(assertions, "keysets", [FINAL / f"keys_run{n}" / "keysets.json" for n in (1, 2)], "extract_cross_source_keys", "inventory_sha256")
    pair_identity(assertions, "cross_source", [FINAL / f"cross_source_run{n}.json" for n in (1, 2)], "analyze_cross_source_keys", None)
    keysets = json.loads((FINAL / "keys_run1" / "keysets.json").read_text())["keysets"]
    # Independent reconciliation 1: the Sep 29 full HUD profile counted 39,887 distinct ZIPs across all quarters.
    hud_profile = json.loads((ROOT / "data" / "schema_review" / "2026_09_29" / "verified_full1" / "HUD.json").read_text())
    hud_zips = {z for codes in keysets["HUD"]["zip"]["all"].values() for z in codes}
    expect(assertions, "hud_distinct_zips_match_sep29_full_profile", hud_profile["columns"]["zip"]["distinct"], len(hud_zips))
    # Independent reconciliation 2: facilities per HAI hospital table in the Sep 30 archive review.
    hai_review = json.loads((REVIEW / "hai_run1" / "hai_archives.json").read_text())["tables"]
    window = "2024-01-01/2024-12-31"
    entities = sorted({t["distinct_entities"] for t in hai_review.values() if t.get("class") == "hai_hospital" and window in json.dumps(t.get("measures", {}))})
    expect(assertions, "hai_2024_facilities_match_archive_review", entities, [len(keysets["main-hai-pdc"]["hospital"]["listed"][window])])
    expect(
        assertions,
        "no_source_read_failed",
        [],
        sorted({s for s, v in keys["sources"].items() for t in v["tables"] if str(t.get("status", "")).startswith("read_failed")}),
    )
    expect(assertions, "model_hold_retained", [False, False], [definitions["model_eligible"], keys["model_eligible"]])


def main() -> int:
    """Run every scenario and write the evidence report."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.parse_args()
    shutil.rmtree(E2E / "synthetic", ignore_errors=True)
    assertions: dict[str, dict[str, Any]] = {}
    synthetic_checks(assertions)
    real_checks(assertions)
    failed = sorted(name for name, result in assertions.items() if result["status"] != "pass")
    report = {
        "feature": "Final review pass: HAI measure definitions and cross-source join keys",
        "created_utc": datetime.now(UTC).isoformat(),
        "environment": {"python": platform.python_version(), "openpyxl": openpyxl.__version__},
        "code_revision": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, check=True).stdout.strip(),
        "uncommitted_review_code_sha256": {f"scripts/review/{name}.py": digest(ROOT / "scripts" / "review" / f"{name}.py") for name in MODULES},
        "input_sha256": {str(INVENTORY.relative_to(ROOT)): digest(INVENTORY)},
        "reproduce": [
            "BUNDLED_PYTHON -m scripts.review.extract_hai_definitions --source-root SOURCE_ROOT --inventory "
            "data/schema_review/2026_09_30/inputs/capture_inventory_effective.json --output data/schema_review/2026_09_30/final_pass/definitions_runN.json",
            "PYTHONPATH=.review_dependencies BUNDLED_PYTHON -m scripts.review.extract_cross_source_keys --source-root SOURCE_ROOT --inventory "
            "data/schema_review/2026_09_30/inputs/capture_inventory_effective.json --output data/schema_review/2026_09_30/final_pass/keys_runN",
            "BUNDLED_PYTHON -m scripts.review.analyze_cross_source_keys --keysets data/schema_review/2026_09_30/final_pass/keys_runN/keysets.json "
            "--output data/schema_review/2026_09_30/final_pass/cross_source_runN.json",
            "(N = 1, 2), then: PYTHONPATH=.review_dependencies BUNDLED_PYTHON -m scripts.review.run_final_pass_e2e --source-root SOURCE_ROOT",
        ],
        "assertions": assertions,
        "result": "fail" if failed else "pass",
        "failed_assertions": failed,
        "limits": [
            "Offline local bytes only; no S3 or publisher request.",
            "Synthetic scenarios use generated values; they prove normalization, classification and rejection paths, not publisher behavior.",
            "The real runs were started separately from this runner; it verifies their code, input and output identity.",
            "Key comparisons are identifier set operations, not joins. Review code is uncommitted and outside CI by decision. No hold is cleared.",
        ],
        "cleanup": "Synthetic inputs under data/e2e/final_pass/synthetic are replaced on each run. No process is left running.",
    }
    write_json(E2E / "report.json", report)
    sys.stdout.write(json.dumps({"result": report["result"], "assertions": len(assertions), "failed": failed}) + "\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
