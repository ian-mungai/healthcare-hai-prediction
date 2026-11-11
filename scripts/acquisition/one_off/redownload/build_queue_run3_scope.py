"""Freeze the run 3 queue for an explicit, reviewed list of units that leave the run, offline, from the reviewed run 3 queue.

The list (``exclusions.json``, kind ``run3_scope_exclusions``) names each unit by snapshot ID with one reason code. Each listed unit keeps its entry and its
receipt digest, becomes ``not_used_by_bronze`` with no route and gets the flag ``run3_scope:<reason>``. Every other unit is copied unchanged. The list and the
reviewed queue are frozen inputs of the new queue, so the controls bind both. Excluding more units later is a rerun with a longer list. The stored
captures are not touched. No network or AWS request is made, and no receipt is read.

Usage (repository root):
    python3 scripts/acquisition/one_off/redownload/list_run3_exclusions_bls.py --output data/redownload_checks/20261111_run3_scope/exclusions.json \\
        --before-year 2006 --reason owner_bls_2006_to_2025_only
    python3 scripts/acquisition/one_off/redownload/build_queue_run3_scope.py --exclusions data/redownload_checks/20261111_run3_scope/exclusions.json \\
        --output data/redownload_checks/20261111_run3_scope --expect-dropped 260
Writes queue.json and queue.lock.json to the output folder; refuses to overwrite a different queue.
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
OUTPUT = "data/redownload_checks/20261111_run3_scope"
KIND = "run3_scope_exclusions"
ACTIVE = ("api", "direct_download", "approved_manual")
REASON = re.compile(r"[a-z0-9_]+")


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


def read_parent(root: Path) -> tuple[bytes, dict]:
    body = (root / PARENT / "queue.json").read_bytes()
    lock = json.loads((root / PARENT / "queue.lock.json").read_text(encoding="utf-8"))
    if sha(body) != lock["queue_sha256"]:
        sys.exit("The reviewed run 3 queue differs from its lock.")
    parent = json.loads(body)
    if budgets_of(parent["queue"]) != parent["budgets"]:
        sys.exit("The reviewed run 3 queue holds budgets its own units do not give.")
    return body, parent


def read_exclusions(path: Path, units: dict[str, dict]) -> dict[str, str]:
    """Return reason by snapshot ID, refusing anything that does not fit the queue."""
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("kind") != KIND or not isinstance(document.get("excluded"), list):
        sys.exit(f"The exclusion list is not a {KIND} record.")
    reasons: dict[str, str] = {}
    for item in document["excluded"]:
        name, reason = item.get("snapshot_id"), item.get("reason")
        if not (isinstance(reason, str) and REASON.fullmatch(reason)):
            sys.exit(f"The reason for {name} must be a lower-case code of letters, digits and underscores.")
        if name in reasons:
            sys.exit(f"The exclusion list names {name} twice.")
        if name not in units:
            sys.exit(f"The unit {name} is not in the queue.")
        if units[name]["disposition"] not in ACTIVE:
            sys.exit(f"The unit {name} is not an active unit, so it has nothing to leave.")
        reasons[name] = reason
    return reasons


def derive(root: Path, exclusions: str, expect_dropped: int) -> bytes:
    parent_body, parent = read_parent(root)
    list_path = root / exclusions
    reasons = read_exclusions(list_path, {unit["snapshot_id"]: unit for unit in parent["queue"]})
    if len(reasons) != expect_dropped:
        sys.exit(f"The exclusion list drops {len(reasons)} units, expected {expect_dropped}. Nothing was written.")
    queue = []
    for unit in parent["queue"]:
        unit = dict(unit, flags=list(unit["flags"]))
        if unit["snapshot_id"] in reasons:
            unit.update(disposition="not_used_by_bronze", route=None)
            unit["flags"].append(f"run3_scope:{reasons[unit['snapshot_id']]}")
        queue.append(unit)
    document = {
        **{key: value for key, value in parent.items() if key not in ("queue", "budgets", "dispositions", "entries", "inputs_sha256")},
        "entries": len(queue),
        "dispositions": dict(sorted(collections.Counter(unit["disposition"] for unit in queue).items())),
        "budgets": budgets_of(queue),
        "inputs_sha256": {**parent["inputs_sha256"], f"{PARENT}/queue.json": sha(parent_body), exclusions: sha(list_path.read_bytes())},
        "queue": queue,
    }
    return (json.dumps(document, indent=1, sort_keys=True) + "\n").encode()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--exclusions", required=True, help="the reviewed exclusion list, relative to the root")
    parser.add_argument("--output", default=OUTPUT)
    parser.add_argument("--expect-dropped", type=int, required=True)
    args = parser.parse_args(argv)
    body = derive(args.root, args.exclusions, args.expect_dropped)
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
