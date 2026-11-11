"""List the BLS units whose planned batch ends before a year, as a run 3 exclusion list for ``build_queue_run3_scope.py``, offline.

Each unit's batch ID (in its receipt path) is joined to the batch windows of ``config/acquisition/bls_api_plan.json``; the plan and its batch IDs are not
changed. A batch that starts before the year and ends on or after it is refused, never split. Other sources get their own list entries by hand, each
with a reason, and the list is reviewed before the queue is built. An existing list that differs is never overwritten.

Usage (repository root):
    python3 scripts/acquisition/one_off/redownload/list_run3_exclusions_bls.py --output data/redownload_checks/20261111_run3_scope/exclusions.json \\
        --before-year 2006 --reason owner_bls_2006_to_2025_only
"""

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
PARENT = "data/redownload_checks/20261004"
PLAN = "config/acquisition/bls_api_plan.json"
ACTIVE = ("api", "direct_download", "approved_manual")
BATCH = re.compile(r"/batches/([0-9a-f]{64})/")
REASON = re.compile(r"[a-z0-9_]+")


def listed(root: Path, before_year: int, reason: str) -> list[dict]:
    plan = json.loads((root / PLAN).read_text(encoding="utf-8"))
    windows = {batch["id"]: (batch["start_year"], batch["end_year"]) for batch in plan["batches"]}
    excluded = []
    for unit in json.loads((root / PARENT / "queue.json").read_text(encoding="utf-8"))["queue"]:
        if unit["source_id"] == "BLS" and unit["mode"] == "bls_api_v2" and unit["disposition"] in ACTIVE:
            found = BATCH.search(unit["receipt"])
            if found is None or found[1] not in windows:
                sys.exit(f"The BLS unit {unit['snapshot_id']} is not in the plan.")
            start, end = windows[found[1]]
            if start < before_year <= end:
                sys.exit(f"The batch of {unit['snapshot_id']} spans {start} to {end}, which the scope rule does not split.")
            if end < before_year:
                excluded.append({"snapshot_id": unit["snapshot_id"], "reason": reason})
    return sorted(excluded, key=lambda item: item["snapshot_id"])


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output", required=True, help="the list to write, relative to the root")
    parser.add_argument("--before-year", type=int, required=True)
    parser.add_argument("--reason", required=True)
    args = parser.parse_args(argv)
    if not REASON.fullmatch(args.reason):
        sys.exit("The reason must be a lower-case code of letters, digits and underscores.")
    body = (
        json.dumps({"kind": "run3_scope_exclusions", "excluded": listed(args.root, args.before_year, args.reason)}, indent=1, sort_keys=True) + "\n"
    ).encode()
    target = args.root / args.output
    if target.exists() and target.read_bytes() != body:
        sys.exit("A different exclusion list already exists; do not overwrite it.")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(body)
    sys.stdout.write(json.dumps({"excluded": len(json.loads(body)["excluded"])}) + "\n")


if __name__ == "__main__":
    main()
