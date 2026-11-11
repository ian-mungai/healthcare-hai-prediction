"""Freeze the run 3 queue for the owner's BLS scope (2006 to 2025 only) offline, from the reviewed run 3 queue.

The BLS plan and its batch IDs stay as they are. Each BLS unit whose planned batch ends before 2006 leaves the run: it keeps its entry and its receipt digest,
becomes ``not_used_by_bronze`` with no route and gets the flag ``run3_scope:owner_bls_2006_to_2025_only``. The stored captures are not touched.
No network or AWS request is made, and no receipt is read.

Usage (repository root):
    python3 scripts/acquisition/one_off/redownload/build_queue_run3_bls2006.py --expect-dropped 260
Writes queue.json and queue.lock.json to data/redownload_checks/20261111_bls2006/; refuses to overwrite a different queue.
The controls and the independent review are separate steps (freeze_controls_run3.py and the reviewer).
"""

import argparse
import collections
import hashlib
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
PARENT = "data/redownload_checks/20261004"
OUTPUT = "data/redownload_checks/20261111_bls2006"
PLAN = "config/acquisition/bls_api_plan.json"
FIRST_YEAR = 2006
FLAG = "run3_scope:owner_bls_2006_to_2025_only"
ACTIVE = ("api", "direct_download", "approved_manual")
BATCH = re.compile(r"/batches/([0-9a-f]{64})/")


def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def budgets_of(queue: list[dict]) -> dict:
    """The same budget block the run 3 freeze writes, from the units still in scope."""
    in_scope = [unit for unit in queue if unit["disposition"] in ACTIVE]
    by_route = collections.Counter(unit["route"] for unit in in_scope)
    calls = sum(1 for unit in in_scope if unit["source_id"] == "BLS" and unit["mode"] == "bls_api_v2")
    return {
        "acquisition_units": len(in_scope),
        "units_by_route": dict(sorted(by_route.items())),
        "max_primary_attempts": 2 * len(in_scope),
        "pages_recorded": sum(unit["pages"] for unit in in_scope),
        "recorded_bytes": sum(unit["bytes"] for unit in in_scope),
        "byte_cap": 128 * 2**30,
        "bls_calls": calls,
        "bls_rolling_24h_limit": 450,
        "bls_minimum_windows": -(-calls // 450),
    }


def windows(root: Path) -> dict[str, tuple[int, int]]:
    plan = json.loads((root / PLAN).read_text(encoding="utf-8"))
    return {batch["id"]: (batch["start_year"], batch["end_year"]) for batch in plan["batches"]}


def derive(root: Path, expect_dropped: int) -> bytes:
    parent_body = (root / PARENT / "queue.json").read_bytes()
    lock = json.loads((root / PARENT / "queue.lock.json").read_text(encoding="utf-8"))
    if sha(parent_body) != lock["queue_sha256"]:
        sys.exit("The reviewed run 3 queue differs from its lock.")
    parent = json.loads(parent_body)
    if budgets_of(parent["queue"]) != parent["budgets"]:
        sys.exit("The reviewed run 3 queue holds budgets its own units do not give.")
    planned = windows(root)
    queue, dropped = [], 0
    for unit in parent["queue"]:
        unit = dict(unit, flags=list(unit["flags"]))
        if unit["source_id"] == "BLS" and unit["mode"] == "bls_api_v2" and unit["disposition"] in ACTIVE:
            found = BATCH.search(unit["receipt"])
            if found is None or found[1] not in planned:
                sys.exit(f"The BLS unit {unit['snapshot_id']} is not in the plan.")
            start, end = planned[found[1]]
            if start < FIRST_YEAR <= end:
                sys.exit(f"The batch of {unit['snapshot_id']} spans {start} to {end}, which the scope rule does not split.")
            if end < FIRST_YEAR:
                unit.update(disposition="not_used_by_bronze", route=None)
                unit["flags"].append(FLAG)
                dropped += 1
        queue.append(unit)
    if dropped != expect_dropped:
        sys.exit(f"The scope rule drops {dropped} BLS units, expected {expect_dropped}. Nothing was written.")
    document = {
        **{key: value for key, value in parent.items() if key not in ("queue", "budgets", "dispositions", "entries", "inputs_sha256")},
        "entries": len(queue),
        "dispositions": dict(sorted(collections.Counter(unit["disposition"] for unit in queue).items())),
        "budgets": budgets_of(queue),
        "inputs_sha256": {**parent["inputs_sha256"], f"{PARENT}/queue.json": sha(parent_body)},
        "queue": queue,
    }
    return (json.dumps(document, indent=1, sort_keys=True) + "\n").encode()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output", default=OUTPUT)
    parser.add_argument("--expect-dropped", type=int, required=True)
    args = parser.parse_args(argv)
    body = derive(args.root, args.expect_dropped)
    target = args.root / args.output / "queue.json"
    if target.exists() and target.read_bytes() != body:
        sys.exit("A different frozen queue already exists; do not overwrite it.")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(body)
    document = json.loads(body)
    target.with_name("queue.lock.json").write_text(json.dumps({"queue_sha256": sha(body), "entries": document["entries"]}, indent=1) + "\n")
    sys.stdout.write(json.dumps({key: document[key] for key in ("entries", "dispositions", "budgets")} | {"queue_sha256": sha(body)}, indent=1) + "\n")


if __name__ == "__main__":
    main()
