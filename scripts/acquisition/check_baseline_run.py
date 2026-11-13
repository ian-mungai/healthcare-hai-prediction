"""Offline check of the tracked baseline run inputs bundle, run by CI on every pull request that touches it.

It recomputes the inputs lock, then checks the queue (lock, active units only, no duplicate IDs, budget sums) and the hosts file with its approval.
It makes no request and reads nothing outside the bundle folder.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path

from scripts.acquisition.redownload_route import ACTIVE, host_problems
from scripts.acquisition.s3_store import encoded_json

ROOT = Path("config/acquisition/baseline_run")
LOCK_NAME = "inputs.lock.json"
ENVIRONMENT_MARKERS = ("s3://", "arn:aws", "/Users/", "/private/", "/var/folders/")


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


def specs_problems(root: Path) -> list[str]:
    """Return why the capture specification does not belong to the queue, misses a unit, names a unit twice or holds a bucket, key or local path."""
    body = (root / "queue.json").read_bytes()
    text = (root / "capture_specs.json").read_text(encoding="utf-8")
    document = json.loads(text)
    ids = {unit["snapshot_id"] for unit in json.loads(body)["queue"]}
    problems = []
    if document["queue_sha256"] != hashlib.sha256(body).hexdigest():
        problems.append("the capture specification belongs to another queue")
    problems += [f"unit has no capture specification: {snapshot}" for snapshot in sorted(ids - document["specs"].keys())]
    problems += [f"capture specification names a unit the queue lacks: {snapshot}" for snapshot in sorted(document["specs"].keys() - ids)]
    if document["units"] != len(document["specs"]):
        problems.append("the capture specification unit count differs from its entries")
    if any(marker in text for marker in ENVIRONMENT_MARKERS):
        problems.append("the capture specification holds a bucket path, object key or local path")
    return problems


def pin_problems(root: Path) -> list[str]:
    """Return why a plan input is not accounted for: every expected input has exactly one home, a tracked copy, an S3 pin or a pending note."""
    document = json.loads((root / "remote_pins.json").read_text(encoding="utf-8"))
    expected = set(document["expected_plan_inputs"])
    tracked = {item["path"].removeprefix("plan_inputs/") for item in file_entries(root) if item["path"].startswith("plan_inputs/")}
    remote = {pin["path"]: pin for pin in document["remote"]}
    pending = {item["path"] for item in document["pending"]}
    problems = []
    for path in sorted(expected):
        homes = [name for name, group in (("tracked", tracked), ("pinned", remote.keys()), ("pending", pending)) if path in group]
        if not homes:
            problems.append(f"{path} has no tracked copy, S3 pin or pending note")
        elif len(homes) > 1:
            problems.append(f"{path} is {' and also '.join(homes)}")
    problems += [f"{path} is a tracked plan input the expected list lacks" for path in sorted(tracked - expected)]
    for path, pin in sorted(remote.items()):
        key, version = pin.get("s3_key"), pin.get("s3_version_id")
        sha = pin.get("sha256")
        complete = isinstance(sha, str) and re.fullmatch(r"[0-9a-f]{64}", sha) and isinstance(pin.get("bytes"), int) and pin["bytes"] > 0
        complete = (
            complete and isinstance(key, str) and bool(key) and "://" not in key and not key.startswith("/") and isinstance(version, str) and bool(version)
        )
        if not complete:
            problems.append(f"the S3 pin is incomplete: {path}")
    return problems


def problems(root: Path = ROOT) -> list[str]:
    """Return every problem of the bundle at ROOT, or an empty list."""
    found = lock_problems(root)
    try:
        found += queue_problems(root)
        found += specs_problems(root)
        found += pin_problems(root)
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
    pending = json.loads((args.root / "remote_pins.json").read_text(encoding="utf-8"))["pending"]
    sys.stderr.write("".join(f"pending: {item['path']}: {item['reason']}\n" for item in pending))
    sys.stdout.write(json.dumps({"bundle": "ok", "files": len(file_entries(args.root)), "pending": len(pending)}) + "\n")


if __name__ == "__main__":
    main()
