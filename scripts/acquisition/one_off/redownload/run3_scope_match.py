"""Match the frozen redownload queue against the files bronze uses (read-only; local evidence for the run 3 scope).

Input: bronze_copies.csv, exported from bronze.stored_copies with scripts/lakehouse/query.sh. Run from the repository root:

    python3 scripts/acquisition/one_off/redownload/run3_scope_match.py
"""

import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path("data/acquisition_planning/run3_scope_20261004")
ACTIVE = ("api", "direct_download", "approved_manual")
FREEZE_DAY = "20260930"


def main() -> None:
    rows = list(csv.DictReader((HERE / "bronze_copies.csv").open()))
    queue = json.loads(Path("data/redownload_checks/20261001/queue.json").read_text())["queue"]
    active = {unit["snapshot_id"]: unit for unit in queue if unit["disposition"] in ACTIVE}
    disposition = {unit["snapshot_id"]: unit["disposition"] for unit in queue}
    loaded_snapshots = {row["snapshot_id"] for row in rows if row["loaded"] == "true"}
    copy_snapshots = {row["snapshot_id"] for row in rows}
    snapshots_by_sha = defaultdict(set)
    for row in rows:
        snapshots_by_sha[row["sha256"]].add(row["snapshot_id"])
    used = sorted(snapshot for snapshot in active if snapshot in loaded_snapshots)
    outside = [row for row in rows if row["loaded"] == "true" and row["snapshot_id"] not in active]
    twins = sorted({snapshot for row in outside for snapshot in snapshots_by_sha[row["sha256"]] & set(active)} - set(used))
    uncovered = [row for row in outside if not snapshots_by_sha[row["sha256"]] & set(active)]

    def kind(row: dict[str, str]) -> str:
        found = re.search(r"__(\d{8})T", row["snapshot_id"])
        if found and found.group(1) >= FREEZE_DAY:
            return "stored_after_queue_freeze"
        return disposition.get(row["snapshot_id"], "not_in_queue")

    result = {
        "kind": "run3_scope_match",
        "basis": "Run 3 covers only the files bronze uses.",
        "bronze_loaded_files": len({row["sha256"] for row in rows if row["loaded"] == "true"}),
        "queue_active_units": len(active),
        "units_with_loaded_files": len(used),
        "twin_units_for_files_loaded_from_superseded_captures": twins,
        "units_with_duplicate_files_only": sorted(s for s in active if s in copy_snapshots and s not in loaded_snapshots),
        "units_bronze_does_not_read": sorted(s for s in active if s not in copy_snapshots),
        "loaded_files_no_unit_covers": {
            name: sorted({(row["source_id"], row["snapshot_id"], row["s3_key"].rsplit("/", 1)[-1]) for row in uncovered if kind(row) == name})
            for name in sorted({kind(row) for row in uncovered})
        },
        "bytes_units_with_loaded_files": sum(active[s]["bytes"] or 0 for s in used),
        "by_source_units_with_loaded_files": dict(Counter(active[s]["source_id"] for s in used).most_common()),
    }
    (HERE / "scope_match.json").write_text(json.dumps(result, indent=1) + "\n")
    summary = {
        key: (
            len(value) if isinstance(value, list) else {k: len(v) for k, v in value.items()} if isinstance(value, dict) and key.startswith("loaded") else value
        )
        for key, value in result.items()
        if key != "by_source_units_with_loaded_files"
    }
    sys.stdout.write(json.dumps(summary, indent=1) + "\n")


if __name__ == "__main__":
    main()
