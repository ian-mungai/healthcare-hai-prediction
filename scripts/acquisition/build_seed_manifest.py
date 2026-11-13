"""One-time development step: list the files of the bootstrap seed folder in its manifest.

The seed folder holds what a run in an empty environment cannot fetch: the plan inputs that are data files (12 browser-history CSVs, 3 MMD references and the
2010 county adjacency file) and the stored data files of the queue's manual HUD and WONDER units (the ACS units move to the Census API). A unit's derived
file (a HUD crosswalk.csv or a WONDER county_year.csv) is rebuilt on the host from its original, so the seed holds the original only. The manifest lists
each file with the folder it takes and, where a tracked source holds one, its SHA-256: the browser CSVs from the comparison report, the MMD references
from the MMD plan, the county adjacency file from the geography file period seed, the HUD workbooks from the HUD plan and the WONDER originals from the
WONDER plan. Every other file has no digest until the files are added; the check then refuses a present file without one.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

from scripts.acquisition.check_baseline_run import EXPECTED_NAME, ROOT, SEED_KIND, SEED_MANIFEST, SEED_MODES, SEED_ROOT, file_entries
from scripts.acquisition.manual_content import MODES, REBUILT_MODES
from scripts.acquisition.wonder_export_contract import stored_name

FOLDERS = (("/mmd_browser_history/", "mmd_browser_history"), ("/mmd_api_comparison/", "mmd_references"), ("/structure_history/", "structure_history"))
REPORT = "plan_inputs/data/e2e/mmd_api_comparison/20260925/report.json"
MMD_PLAN = "plan_inputs/data/datasets/historical_acquisition/mmd_api_history/20260925/collection_plan.json"
HUD_PLAN = Path("config/acquisition/hud_xlsx_plan_2010q1_2020q4.json")
WONDER_PLAN = Path("config/acquisition/wonder_export_plan.json")
PERIODS = Path("dbt/seeds/geography_file_periods.csv")
CAPTURES_FOLDER, RAW_FOLDER = "captures", "raw"


def input_folder(path: str) -> str:
    """The seed folder of a plan input, by where its path says it was captured."""
    for marker, folder in FOLDERS:
        if marker in path:
            return folder
    raise ValueError(f"{path} has no seed folder")


def build(queue_bytes: bytes, specs: dict[str, Any], inputs: list[str], digests: dict[str, str], originals: dict[str, str]) -> bytes:
    """The manifest bytes for the plan INPUTS held in the seed and the stored files of the manual HUD and WONDER units in SPECS.

    DIGESTS and ORIGINALS map file names to SHA-256. A HUD or WONDER unit lists only its original: the host rebuilds crosswalk.csv or county_year.csv from it.
    """
    files: list[dict[str, Any]] = []
    for path in inputs:
        name = PurePosixPath(path).name
        files.append({"path": f"{input_folder(path)}/{name}", "sha256": digests.get(name), "plan_input": path})
    for unit in json.loads(queue_bytes)["queue"]:
        if unit["disposition"] == "approved_manual" and unit["mode"] in SEED_MODES:
            for name in specs["specs"][unit["snapshot_id"]]["stored_data_files"]:
                if not (unit["mode"] in REBUILT_MODES and name == MODES[unit["mode"]][1]):
                    files.append({"path": f"manual_units/{unit['snapshot_id']}/{name}", "sha256": originals.get(name), "unit": unit["snapshot_id"]})
    document = {"kind": SEED_KIND, "queue_sha256": hashlib.sha256(queue_bytes).hexdigest(), "files": sorted(files, key=lambda item: item["path"])}
    return (json.dumps(document, indent=1, sort_keys=True) + "\n").encode()


def capture_release(path: str) -> str | None:
    """The release (snapshot ID) of a plan input stored by the capture step, or None when the path has another layout.

    The capture step writes the original to ``<job>/captures/<source ID>/<snapshot ID>/raw/<file name>`` (``capture.py``: ``snapshot_root / "raw"``).
    """
    parts = PurePosixPath(path).parts
    return parts[-3] if len(parts) >= 5 and parts[-5] == CAPTURES_FOLDER and parts[-2] == RAW_FOLDER else None


def period_digests(inputs: list[str], rows: Sequence[Mapping[str, str]]) -> dict[str, str]:
    """The SHA-256 of each plan input that a geography file period row names: the row's release is the input's capture folder and its file name is the input's.

    Only the capture folder position of the path counts: another folder that carries a release ID selects nothing. Two rows with different digests for one
    input are refused.
    """
    found: dict[str, str] = {}
    for path in inputs:
        name, release = PurePosixPath(path).name, capture_release(path)
        digests = {row["member_sha256"] for row in rows if release is not None and row["file_name"] == name and row["release_id"] == release}
        if len(digests) > 1:
            raise ValueError(f"{path} has more than one digest in {PERIODS}")
        found |= {name: digests.pop()} if digests else {}
    return found


def manifest_body(root: Path) -> bytes:
    """The manifest of the bundle at ROOT: the digests come from tracked files only."""
    tracked = {item["path"].removeprefix("plan_inputs/") for item in file_entries(root) if item["path"].startswith("plan_inputs/")}
    expected = json.loads((root / EXPECTED_NAME).read_text(encoding="utf-8"))["expected_plan_inputs"]
    report = json.loads((root / REPORT).read_text(encoding="utf-8"))
    digests = {PurePosixPath(year["original_path"]).name: year["original_sha256"] for year in report["years"]}
    plan = json.loads((root / MMD_PLAN).read_text(encoding="utf-8"))
    digests |= {PurePosixPath(item["path"]).name: item["sha256"] for item in plan["references"].values()}
    inputs = [path for path in expected if path not in tracked]
    with PERIODS.open(encoding="utf-8", newline="") as handle:
        digests |= period_digests(inputs, list(csv.DictReader(handle)))
    originals = {batch["file_name"]: batch["sha256"] for batch in json.loads(HUD_PLAN.read_text(encoding="utf-8"))["batches"]}
    originals |= {stored_name(batch): batch["sha256"] for batch in json.loads(WONDER_PLAN.read_text(encoding="utf-8"))["batches"]}
    specs = json.loads((root / "capture_specs.json").read_text(encoding="utf-8"))
    return build((root / "queue.json").read_bytes(), specs, inputs, digests, originals)


def main() -> None:
    """Write the manifest of the tracked bundle into the seed folder."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=SEED_ROOT / SEED_MANIFEST)
    args = parser.parse_args()
    body = manifest_body(args.root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(body)
    sys.stdout.write(json.dumps({"files": len(json.loads(body)["files"]), "sha256": hashlib.sha256(body).hexdigest()}) + "\n")


if __name__ == "__main__":
    main()
