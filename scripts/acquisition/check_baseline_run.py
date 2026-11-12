"""Offline check of the tracked baseline run inputs bundle, run by CI on every pull request that touches it.

It recomputes the inputs lock, then checks the queue (lock, active units only, no duplicate IDs, budget sums) and the hosts file with its approval.
It makes no request and reads nothing outside the bundle folder.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path

from scripts.acquisition.redownload_route import ACTIVE, host_problems
from scripts.acquisition.s3_store import encoded_json

ROOT = Path("config/acquisition/baseline_run")
LOCK_NAME = "inputs.lock.json"


def file_entries(root: Path) -> list[dict]:
    """Return path, SHA-256 and byte count of every file in the bundle except the lock, sorted by path."""
    entries = []
    for path in sorted(item for item in root.rglob("*") if item.is_file() and item != root / LOCK_NAME):
        body = path.read_bytes()
        entries.append({"path": path.relative_to(root).as_posix(), "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)})
    return entries


def bundle_digest(entries: list[dict]) -> str:
    """Return the SHA-256 of the sorted entry list in canonical JSON."""
    return hashlib.sha256(json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def build_lock(root: Path) -> bytes:
    """Return the inputs lock bytes for the files now in the bundle."""
    entries = file_entries(root)
    return encoded_json({"bundle_sha256": bundle_digest(entries), "files": entries})


def lock_problems(root: Path) -> list[str]:
    """Return why the files differ from the recorded lock: a changed byte, a missing file, an extra file or a changed bundle digest."""
    try:
        lock = json.loads((root / LOCK_NAME).read_text(encoding="utf-8"))
        recorded = {item["path"]: item for item in lock["files"]}
    except (OSError, ValueError, KeyError, TypeError) as error:
        return [f"{LOCK_NAME} is unusable: {error}"]
    found = {item["path"]: item for item in file_entries(root)}
    problems = [f"{path} is missing from the bundle" for path in sorted(recorded.keys() - found.keys())]
    problems += [f"{path} is not in the lock" for path in sorted(found.keys() - recorded.keys())]
    problems += [f"{path} differs from the lock" for path in sorted(recorded.keys() & found.keys()) if recorded[path] != found[path]]
    if lock.get("bundle_sha256") != bundle_digest(sorted(recorded.values(), key=lambda item: item["path"])):
        problems.append("the bundle digest differs from the lock entries")
    return problems


def queue_problems(root: Path) -> list[str]:
    """Return why the queue is not a locked, active-only, duplicate-free queue whose budgets equal its units' sums."""
    body = (root / "queue.json").read_bytes()
    queue = json.loads(body)
    lock = json.loads((root / "queue.lock.json").read_text(encoding="utf-8"))
    units = queue["queue"]
    problems = []
    if hashlib.sha256(body).hexdigest() != lock["queue_sha256"]:
        problems.append("the queue differs from its lock")
    if lock["entries"] != len(units) or queue["entries"] != len(units):
        problems.append(f"the queue holds {len(units)} units but its lock or header says {lock['entries']} and {queue['entries']}")
    seen: set[str] = set()
    for unit in units:
        if unit["disposition"] not in ACTIVE:
            problems.append(f"unit {unit['snapshot_id']} has disposition {unit['disposition']}, not an active disposition")
        if unit["snapshot_id"] in seen:
            problems.append(f"duplicate snapshot ID {unit['snapshot_id']}")
        seen.add(unit["snapshot_id"])
    expected = {
        "acquisition_units": len(units),
        "recorded_bytes": sum(unit["bytes"] for unit in units),
        "pages_recorded": sum(unit["pages"] for unit in units),
        "units_by_route": dict(Counter(unit["route"] for unit in units)),
    }
    for name, value in expected.items():
        budget = queue["budgets"].get(name)
        if budget is not None and (dict(sorted(budget.items())) if isinstance(budget, dict) else budget) != (
            dict(sorted(value.items())) if isinstance(value, dict) else value
        ):
            problems.append(f"budgets.{name} is {budget}, the units sum to {value}")
    return problems


def problems(root: Path = ROOT) -> list[str]:
    """Return every problem of the bundle at ROOT, or an empty list."""
    found = lock_problems(root)
    try:
        found += queue_problems(root)
        found += host_problems((root / "queue.json").read_bytes(), root / "publisher_hosts_scope.json", root / "hosts_approval.json")
    except (OSError, ValueError, KeyError, TypeError) as error:
        found.append(f"the queue or hosts files are unusable: {error}")
    return found


def main() -> None:
    """Check the bundle and exit 1 with every problem, or write ``--write-lock`` first when asked to refresh the lock after a reviewed change."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--write-lock", action="store_true")
    args = parser.parse_args()
    if args.write_lock:
        (args.root / LOCK_NAME).write_bytes(build_lock(args.root))
    found = problems(args.root)
    if found:
        sys.stderr.write("\n".join(found) + "\n")
        raise SystemExit(1)
    sys.stdout.write(json.dumps({"bundle": "ok", "files": len(file_entries(args.root))}) + "\n")


if __name__ == "__main__":
    main()
