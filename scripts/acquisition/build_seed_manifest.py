"""One-time development step: list the files of the bootstrap seed folder in its manifest.

The seed folder holds what a run in an empty environment cannot fetch: the plan inputs that are data files (12 browser-history CSVs, 3 MMD references and the
2010 county adjacency file) and the stored data files of the queue's manual HUD and WONDER units (the ACS units move to the Census API). A WONDER unit's
county_year.csv is rebuilt on the host from its original, so the seed holds the original only. The manifest lists each file with the folder it takes and, where
a tracked source holds one, its SHA-256: the browser CSVs from the comparison report, the MMD references from the MMD plan, the HUD workbooks from the HUD
plan and the WONDER originals from the WONDER plan. Every other file has no digest until the files are added; the check then refuses a present file without one.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path, PurePosixPath
from typing import Any

from scripts.acquisition.check_baseline_run import EXPECTED_NAME, ROOT, SEED_KIND, SEED_MANIFEST, SEED_MODES, SEED_ROOT, file_entries
from scripts.acquisition.manual_content import MODES, REBUILT_MODES
from scripts.acquisition.wonder_export_contract import stored_name

FOLDERS = (("/mmd_browser_history/", "mmd_browser_history"), ("/mmd_api_comparison/", "mmd_references"), ("/structure_history/", "structure_history"))
HOLDS = {"county_adjacency2010.txt": "pending: a Latin-1 file waits on the removed-names hook fix"}
REPORT = "plan_inputs/data/e2e/mmd_api_comparison/20260925/report.json"
MMD_PLAN = "plan_inputs/data/datasets/historical_acquisition/mmd_api_history/20260925/collection_plan.json"
HUD_PLAN = Path("config/acquisition/hud_xlsx_plan_2010q1_2020q4.json")
WONDER_PLAN = Path("config/acquisition/wonder_export_plan.json")


def input_folder(path: str) -> str:
    """The seed folder of a plan input, by where its path says it was captured."""
    for marker, folder in FOLDERS:
        if marker in path:
            return folder
    raise ValueError(f"{path} has no seed folder")


def build(queue_bytes: bytes, specs: dict[str, Any], inputs: list[str], digests: dict[str, str], originals: dict[str, str]) -> bytes:
    """The manifest bytes for the plan INPUTS held in the seed and the stored files of the manual HUD and WONDER units in SPECS.

    DIGESTS and ORIGINALS map file names to SHA-256. A WONDER unit lists only its original: the host rebuilds county_year.csv from it.
    """
    files: list[dict[str, Any]] = []
    for path in inputs:
        name = PurePosixPath(path).name
        files.append(
            {"path": f"{input_folder(path)}/{name}", "sha256": digests.get(name), "plan_input": path} | ({"hold": HOLDS[name]} if name in HOLDS else {})
        )
    for unit in json.loads(queue_bytes)["queue"]:
        if unit["disposition"] == "approved_manual" and unit["mode"] in SEED_MODES:
            for name in specs["specs"][unit["snapshot_id"]]["stored_data_files"]:
                if not (unit["mode"] in REBUILT_MODES and name == MODES[unit["mode"]][1]):
                    files.append({"path": f"manual_units/{unit['snapshot_id']}/{name}", "sha256": originals.get(name), "unit": unit["snapshot_id"]})
    document = {"kind": SEED_KIND, "queue_sha256": hashlib.sha256(queue_bytes).hexdigest(), "files": sorted(files, key=lambda item: item["path"])}
    return (json.dumps(document, indent=1, sort_keys=True) + "\n").encode()


def main() -> None:
    """Write the manifest of the tracked bundle into the seed folder: the digests come from tracked files only."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=SEED_ROOT / SEED_MANIFEST)
    args = parser.parse_args()
    root = args.root
    tracked = {item["path"].removeprefix("plan_inputs/") for item in file_entries(root) if item["path"].startswith("plan_inputs/")}
    expected = json.loads((root / EXPECTED_NAME).read_text(encoding="utf-8"))["expected_plan_inputs"]
    report = json.loads((root / REPORT).read_text(encoding="utf-8"))
    digests = {PurePosixPath(year["original_path"]).name: year["original_sha256"] for year in report["years"]}
    plan = json.loads((root / MMD_PLAN).read_text(encoding="utf-8"))
    digests |= {PurePosixPath(item["path"]).name: item["sha256"] for item in plan["references"].values()}
    originals = {batch["file_name"]: batch["sha256"] for batch in json.loads(HUD_PLAN.read_text(encoding="utf-8"))["batches"]}
    originals |= {stored_name(batch): batch["sha256"] for batch in json.loads(WONDER_PLAN.read_text(encoding="utf-8"))["batches"]}
    specs = json.loads((root / "capture_specs.json").read_text(encoding="utf-8"))
    body = build((root / "queue.json").read_bytes(), specs, [path for path in expected if path not in tracked], digests, originals)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(body)
    sys.stdout.write(json.dumps({"files": len(json.loads(body)["files"]), "sha256": hashlib.sha256(body).hexdigest()}) + "\n")


if __name__ == "__main__":
    main()
