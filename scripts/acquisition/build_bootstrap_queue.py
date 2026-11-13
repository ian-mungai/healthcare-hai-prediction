"""One-time development step: build the tracked bootstrap capture queue from the local scope v2 queue.

The source queue holds 3,096 entries. The bootstrap keeps the entries bronze uses (api, direct_download, approved_manual), writes the source's
digest and the dropped counts into a ``derived_from`` record, locks the result and rewrites the approved hosts file for it. The source is not
tracked, so hosted CI cannot run this script. CI checks the tracked result instead (``check_baseline_run``).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from scripts.acquisition.redownload_route import ACTIVE, queue_hosts
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.transport import hosts_digest

SOURCE_SHA256 = "c1b0e0032d9417fd9e008b07dfa701169fc9063702dc95792a7009373344fd42"
SOURCE_PATH = "data/redownload_checks/20261111_run3_scope_v2/queue.json"
KIND = "bootstrap_capture_queue"
RULE = "keep disposition in api, direct_download, approved_manual; drop mode reference_download (owner, Nov 12 2026: the documents are not seeded or captured)"
DROPPED_MODES = ("reference_download",)
# The secret scan reads this flag with its date suffix as an API key. The harness tests the flag by prefix only, so the shortened flag is equal for it.
DATED_FLAG = re.compile(r"^(browser_export_moved_to_api:parity_verified)_\d{8}_\d{8}$")
DROPPED = ("not_used_by_bronze", "superseded_duplicate", "failed_attempt_excluded", "derived_replay_only", "dropped")


def encode_queue(queue: dict[str, Any]) -> bytes:
    """Write the queue in the layout of the source queue: sorted keys, one-space indent, trailing newline."""
    return (json.dumps(queue, indent=1, sort_keys=True) + "\n").encode()


def trim_queue(body: bytes, expected_sha256: str = SOURCE_SHA256) -> bytes:
    """Return the trimmed queue bytes. Refuse a source whose digest differs and any unit with an unknown disposition."""
    digest = hashlib.sha256(body).hexdigest()
    if digest != expected_sha256:
        raise ValueError(f"The source queue digest {digest} differs from the expected {expected_sha256}")
    source = json.loads(body)
    kept, dropped = [], []
    for unit in source["queue"]:
        if unit["disposition"] in ACTIVE:
            kept.append(unit)
        elif unit["disposition"] in DROPPED:
            dropped.append(unit)
        else:
            raise ValueError(f"Source unit {unit['snapshot_id']} has an unknown disposition {unit['disposition']}")
    by_mode = [unit for unit in kept if unit["mode"] in DROPPED_MODES]
    kept = [unit for unit in kept if unit["mode"] not in DROPPED_MODES]
    rewrites: dict[str, dict[str, Any]] = {}
    for unit in kept:
        for index, flag in enumerate(unit["flags"]):
            if match := DATED_FLAG.match(flag):
                unit["flags"][index] = match.group(1)
                entry = rewrites.setdefault(match.group(1), {"dated_suffixes_removed": [], "units": 0})
                entry["units"] += 1
                suffix = flag.removeprefix(match.group(1) + "_")
                if suffix not in entry["dated_suffixes_removed"]:
                    entry["dated_suffixes_removed"].append(suffix)
    queue = {key: value for key, value in source.items() if key not in ("run", "inputs_sha256", "queue")}
    queue["kind"] = KIND
    queue["entries"] = len(kept)
    queue["budgets"] = budgets_without(source["budgets"], by_mode)
    queue["dispositions"] = dict(sorted(Counter(unit["disposition"] for unit in kept).items()))
    queue["queue"] = kept
    queue["derived_from"] = {
        "path": SOURCE_PATH,
        "sha256": digest,
        "entries": len(source["queue"]),
        "rule": RULE,
        "dropped_by_disposition": dict(sorted(Counter(unit["disposition"] for unit in dropped).items())),
        "dropped_snapshot_ids_sha256": hashlib.sha256("\n".join(sorted(unit["snapshot_id"] for unit in dropped)).encode()).hexdigest(),
        "dropped_by_mode": dict(sorted(Counter(unit["mode"] for unit in by_mode).items())),
        "dropped_mode_snapshot_ids_sha256": hashlib.sha256("\n".join(sorted(unit["snapshot_id"] for unit in by_mode)).encode()).hexdigest(),
        "inputs_sha256": source["inputs_sha256"],
        **({"flag_rewrites": rewrites} if rewrites else {}),
    }
    return encode_queue(queue)


def budgets_without(budgets: dict[str, Any], units: list[dict[str, Any]]) -> dict[str, Any]:
    """The budgets of the source queue less UNITS: their count, pages, bytes, route counts and primary attempts. Other budgets stay as approved."""
    result = dict(budgets)
    attempts = budgets.get("max_primary_attempts")
    per_unit, rest = divmod(attempts, budgets["acquisition_units"]) if attempts is not None else (0, 0)
    if rest:
        raise ValueError("The attempt budget is not a whole multiple of the unit count")
    for unit in units:
        result["acquisition_units"] -= 1
        result["pages_recorded"] -= unit["pages"]
        result["recorded_bytes"] -= unit["bytes"]
        if attempts is not None:
            result["max_primary_attempts"] -= per_unit
        result["units_by_route"] = {route: count - (route == unit["route"]) for route, count in result["units_by_route"].items()}
    if units:
        result["units_by_route"] = {route: count for route, count in result["units_by_route"].items() if count}
    return result


def queue_lock(trimmed: bytes) -> bytes:
    """Return the lock bytes for a trimmed queue: its digest and entry count."""
    return encoded_json({"queue_sha256": hashlib.sha256(trimmed).hexdigest(), "entries": len(json.loads(trimmed)["queue"])})


def rewrite_hosts(approved: dict[str, Any], trimmed: bytes) -> bytes:
    """Return the hosts file for the trimmed queue. The host list stays as approved, so the approval still matches its digest."""
    hosts = approved["hosts"]
    if approved["hosts_sha256"] != hosts_digest(hosts):
        raise ValueError("The approved hosts file differs from its digest")
    missing = sorted(queue_hosts(json.loads(trimmed)) - set(hosts))
    if missing:
        raise ValueError(f"The approved list lacks hosts the queue needs: {', '.join(missing)}")
    return encoded_json({"hosts": hosts, "hosts_sha256": approved["hosts_sha256"], "queue_sha256": hashlib.sha256(trimmed).hexdigest()})


def build(source: Path, hosts: Path, approval: Path, output: Path) -> dict[str, Any]:
    """Write queue.json, queue.lock.json, publisher_hosts_scope.json and hosts_approval.json into a new folder and return their counts."""
    trimmed = trim_queue(source.read_bytes())
    approved = json.loads(hosts.read_text(encoding="utf-8"))
    hosts_bytes = rewrite_hosts(approved, trimmed)
    approval_document = json.loads(approval.read_text(encoding="utf-8"))
    if approval_document.get("hosts_sha256") != approved["hosts_sha256"]:
        raise ValueError("The hosts approval names another hosts file")
    output.mkdir(parents=True, exist_ok=False)
    files = {
        "queue.json": trimmed,
        "queue.lock.json": queue_lock(trimmed),
        "publisher_hosts_scope.json": hosts_bytes,
        "hosts_approval.json": approval.read_bytes(),
    }
    for name, content in files.items():
        (output / name).write_bytes(content)
    return {
        "units": len(json.loads(trimmed)["queue"]),
        "hosts": len(approved["hosts"]),
        "sha256": {name: hashlib.sha256(content).hexdigest() for name, content in files.items()},
    }


def main() -> None:
    """Build the bootstrap queue, lock, hosts file and approval into a new OUTPUT folder and print their digests."""
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source", "hosts", "approval", "output"):
        parser.add_argument(name, type=Path)
    args = parser.parse_args()
    sys.stdout.write(json.dumps(build(args.source, args.hosts, args.approval, args.output), indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
