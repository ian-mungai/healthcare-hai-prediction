"""Profile every SAIPE state-and-county file by value position, without a captured publisher layout.

Each record is a run of numeric fields followed by the area name, the state postal code and (in later years) a file
tag. Because the layout document was not captured, values are profiled by their position in that numeric run: the
count of numeric fields per record, and per position the minimum, maximum, decimal share and non-numeric markers.
Geography comes from the first two fields (state and county FIPS). Values are aggregates of public estimates; no
row is written.

Usage::

    python -m scripts.review.profile_saipe --source-root SOURCE_ROOT \
        --inventory data/schema_review/2026_09_30/inputs/capture_inventory_effective.json --output saipe_run1.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from scripts.review.review_remaining import digest, safe_path, write_json

NUMBER = re.compile(r"^-?\d+(?:\.\d+)?$")
YEAR = re.compile(r"est(\d\d)", re.IGNORECASE)


def profile_file(text: str) -> dict[str, Any]:
    """Per-position statistics of the leading numeric fields of every record."""
    widths: Counter[int] = Counter()
    levels: Counter[str] = Counter()
    positions: dict[int, dict[str, Any]] = {}
    markers: Counter[str] = Counter()
    for line in text.splitlines():
        tokens = line.split()
        if not tokens:
            continue
        run = 0
        for token in tokens:
            if NUMBER.match(token):
                run += 1
                continue
            if token in {".", "-", "*", "(X)"} and run >= 2:
                markers[token] += 1
                run += 1
                continue
            break
        widths[run] += 1
        if run >= 2:
            levels["state_total" if tokens[1] == "0" and tokens[0] != "00" else "national" if tokens[0] == "00" else "county"] += 1
        for index, token in enumerate(tokens[:run]):
            if not NUMBER.match(token):
                continue
            value = float(token)
            slot = positions.setdefault(index, {"count": 0, "min": value, "max": value, "decimals": 0})
            slot["count"] += 1
            slot["min"], slot["max"] = min(slot["min"], value), max(slot["max"], value)
            slot["decimals"] += "." in token
    return {
        "records": sum(widths.values()),
        "numeric_fields_per_record": dict(sorted((str(k), v) for k, v in widths.items())),
        "levels": dict(sorted(levels.items())),
        "missing_markers": dict(sorted(markers.items())),
        "positions": {str(k): {**v, "min": round(v["min"], 3), "max": round(v["max"], 3)} for k, v in sorted(positions.items())},
    }


def main() -> int:
    """Run the profile from its real CLI."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.source_root.resolve()
    files: dict[str, Any] = {}
    for candidate in sorted(json.loads(args.inventory.read_text())["candidates"], key=lambda c: c["snapshot_id"]):
        if candidate["source_id"] != "SAIPE" or candidate["status"] != "candidate_not_authorized_for_execution":
            continue
        receipt = safe_path(root, candidate["receipt"])
        for artifact in json.loads(receipt.read_text())["artifacts"]:
            if artifact["role"] != "data":
                continue
            path = receipt.parent / artifact["storage_path"]
            if not path.is_file() or digest(path) != artifact["sha256"]:
                files[artifact["stored_file_name"]] = {"status": "artifact_not_local_or_changed"}
                continue
            match = YEAR.search(artifact["stored_file_name"])
            year = (("19" if int(match.group(1)) > 70 else "20") + match.group(1)) if match else "unknown"
            files[artifact["stored_file_name"]] = {"year": year, "sha256": artifact["sha256"], **profile_file(path.read_text(encoding="latin-1"))}
    write_json(
        args.output,
        {
            "version": 1,
            "code_sha256": digest(Path(__file__)),
            "inventory_sha256": digest(args.inventory),
            "files": dict(sorted(files.items())),
            "model_eligible": False,
        },
    )
    sys.stdout.write(json.dumps({"files": len(files)}) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
