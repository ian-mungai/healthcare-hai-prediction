"""Offline check of the tracked baseline run inputs bundle, run by CI on every pull request that touches it.

It recomputes the inputs lock, then checks the queue (lock, active units only, no duplicate IDs, budget sums), the capture specification, the hosts file with
its approval and the required secrets list against the secrets the collector plans and the key reader name. It also checks that every expected plan input has
exactly one home (a tracked copy or an entry of the bootstrap seed manifest) and that the seed folder matches its manifest: the files the queue's manual HUD and
WONDER units store, the digests, the Git LFS rule. It makes no request and reads only the bundle, the seed folder and the collector plans.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path

from scripts.acquisition.api_keys import SHARED_KEYS
from scripts.acquisition.redownload_route import ACTIVE, host_problems
from scripts.acquisition.s3_store import encoded_json
from scripts.process import run_command

ROOT = Path("config/acquisition/baseline_run")
SEED_ROOT = Path("config/acquisition/bootstrap_seed")
LOCK_NAME = "inputs.lock.json"
EXPECTED_NAME = "expected_plan_inputs.json"
SEED_MANIFEST = "manifest.json"
SEED_DOCUMENTS = frozenset({SEED_MANIFEST, "README.md"})
SEED_KIND = "bootstrap_seed_manifest"
# The manual units whose stored files the seed holds. The ACS units are manual too, but they move to the Census API collector (owner, Nov 12 2026).
SEED_MODES = ("hud_xlsx", "wonder_export")
SECRETS_NAME = "required_secrets.json"
SECRETS_KIND = "bootstrap_required_secrets"
CONFIG = Path("config/acquisition")
# The collector mode of a queue unit to the secret its collector reads.
MODE_SECRETS = {"bls_api_v2": "bls_api_key", "hud_api": "hud_api_key", "census_acs_api": "census_api_key", "census_acs_detailed": "census_api_key"}
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


def home_problems(root: Path, manifest: dict) -> list[str]:
    """Return why a plan input is not accounted for: each expected input has exactly one home, a tracked copy in the bundle or a seed manifest entry."""
    expected = set(json.loads((root / EXPECTED_NAME).read_text(encoding="utf-8"))["expected_plan_inputs"])
    tracked = {item["path"].removeprefix("plan_inputs/") for item in file_entries(root) if item["path"].startswith("plan_inputs/")}
    seeded = Counter(item["plan_input"] for item in manifest["files"] if "plan_input" in item)
    problems = []
    for path in sorted(expected):
        if path not in tracked and path not in seeded:
            problems.append(f"{path} has no tracked copy or seed entry")
        elif path in tracked and path in seeded:
            problems.append(f"{path} is tracked and also in the seed manifest")
        elif seeded[path] > 1:
            problems.append(f"{path} is in the seed manifest {seeded[path]} times")
    problems += [f"{path} is a tracked plan input the expected list lacks" for path in sorted(tracked - expected)]
    problems += [f"{path} is not an expected plan input" for path in sorted(set(seeded) - expected)]
    return problems


def expected_seed_files(root: Path) -> dict[str, str]:
    """The seed path of every stored data file of a manual HUD or WONDER unit, to its unit: ``manual_units/<unit>/<file name>``."""
    queue = json.loads((root / "queue.json").read_bytes())["queue"]
    specs = json.loads((root / "capture_specs.json").read_text(encoding="utf-8"))["specs"]
    return {
        f"manual_units/{unit['snapshot_id']}/{name}": unit["snapshot_id"]
        for unit in queue
        if unit["disposition"] == "approved_manual" and unit["mode"] in SEED_MODES
        for name in specs[unit["snapshot_id"]]["stored_data_files"]
    }


def seed_problems(root: Path, seed_root: Path, manifest: dict, *, require_files: bool = False) -> list[str]:
    """Return why the seed folder differs from its manifest or the queue: another queue, wrong files for the manual HUD and WONDER units, bad paths, digests."""
    problems = []
    if manifest.get("kind") != SEED_KIND:
        problems.append(f"the seed manifest is not a {SEED_KIND}")
    if manifest.get("queue_sha256") != hashlib.sha256((root / "queue.json").read_bytes()).hexdigest():
        problems.append("the seed manifest belongs to another queue")
    entries = manifest["files"]
    listed = Counter(item["path"] for item in entries)
    problems += [f"{path} is listed twice" for path, count in sorted(listed.items()) if count > 1]
    units = {item["path"]: item["unit"] for item in entries if "unit" in item}
    wanted = expected_seed_files(root)
    problems += [
        f"the seed manifest lacks the stored file {Path(path).name} of unit {unit}" for path, unit in sorted(wanted.items()) if units.get(path) != unit
    ]
    problems += [f"{path} names a stored file the queue does not expect" for path in sorted(units.keys() - wanted.keys())]
    for item in entries:
        path, sha = item["path"], item.get("sha256")
        if Path(path).is_absolute() or ".." in Path(path).parts or not path:
            problems.append(f"{path} is not a relative path inside the seed folder")
            continue
        if ("plan_input" in item) == ("unit" in item):
            problems.append(f"{path} must name exactly one of a plan input and a unit")
        if sha is not None and not (isinstance(sha, str) and re.fullmatch(r"[0-9a-f]{64}", sha)):
            problems.append(f"{path} has an unusable digest")
            continue
        found = seed_root / path
        if not found.is_file():
            if require_files and "hold" not in item:
                problems.append(f"{path} is missing from the seed folder")
        elif sha is None:
            problems.append(f"{path} has no recorded digest")
        elif hashlib.sha256(found.read_bytes()).hexdigest() != sha:
            problems.append(f"{path} differs from the manifest digest")
    on_disk = {item.relative_to(seed_root).as_posix() for item in seed_root.rglob("*") if item.is_file()} - SEED_DOCUMENTS
    problems += [f"{path} is not in the manifest" for path in sorted(on_disk - listed.keys())]
    return problems


def lfs_problems(paths: list[str], folder: Path = SEED_ROOT) -> list[str]:
    """Return the seed paths that Git does not store through LFS: the attribute lookup needs no file, so it runs before the files exist."""
    if not paths:
        return []
    answer = run_command("git", ["check-attr", "filter", "--", *(str(folder / path) for path in paths)], check=True).stdout
    stored = {line.rsplit(": filter: ", 1)[0] for line in answer.splitlines() if line.endswith(": filter: lfs")}
    return [f"{path} is not stored through Git LFS" for path in paths if str(folder / path) not in stored]


def secrets_read_by_code(config: Path = CONFIG) -> set[str]:
    """The secret names the ingestion code reads: the shared publisher keys of the key reader and the credential every tracked collector plan names."""
    names = set(SHARED_KEYS)
    for path in sorted(config.glob("*.json")):
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except ValueError:
            continue
        if isinstance(document, dict) and isinstance(document.get("credential_reference"), str):
            names.add(document["credential_reference"])
    return names


def secret_problems(root: Path, names: set[str]) -> list[str]:
    """Return why the required secrets list differs from NAMES, the secrets the code reads, or from the queue: names only, each provisioned, units counted."""
    document = json.loads((root / SECRETS_NAME).read_text(encoding="utf-8"))
    specs = json.loads((root / "capture_specs.json").read_text(encoding="utf-8"))["specs"]
    units = Counter(MODE_SECRETS[spec["mode"]] for spec in specs.values() if spec["mode"] in MODE_SECRETS)
    problems = [f"{SECRETS_NAME} is not a {SECRETS_KIND}"] if document.get("kind") != SECRETS_KIND else []
    listed = Counter(item["name"] for item in document["secrets"])
    problems += [f"{name} is listed twice" for name, count in sorted(listed.items()) if count > 1]
    problems += [f"{SECRETS_NAME} lacks {name}, which the code reads" for name in sorted(names - listed.keys())]
    problems += [f"{SECRETS_NAME} names {name}, which no code reads" for name in sorted(listed.keys() - names)]
    for item in document["secrets"]:
        for field in ("read_by", "used_for", "provision"):
            if not (isinstance(item.get(field), str) and item[field].strip()):
                problems.append(f"{item['name']} has no {'provisioning' if field == 'provision' else field} text")
        if item.get("queue_units", units[item["name"]]) != units[item["name"]]:
            problems.append(f"{item['name']} says {item['queue_units']} queue units, the queue has {units[item['name']]}")
    return problems


def problems(root: Path = ROOT, seed_root: Path = SEED_ROOT, *, require_files: bool = False) -> list[str]:
    """Return every problem of the bundle at ROOT and the seed folder at SEED_ROOT, or an empty list."""
    found = lock_problems(root)
    try:
        manifest = json.loads((seed_root / SEED_MANIFEST).read_text(encoding="utf-8"))
        found += queue_problems(root)
        found += specs_problems(root)
        found += home_problems(root, manifest)
        found += seed_problems(root, seed_root, manifest, require_files=require_files)
        found += secret_problems(root, secrets_read_by_code())
        found += host_problems((root / "queue.json").read_bytes(), root / "publisher_hosts_scope.json", root / "hosts_approval.json")
    except (OSError, ValueError, KeyError, TypeError) as error:
        found.append(f"the queue, seed or hosts files are unusable: {error}")
    return found


def main() -> None:
    """Check the bundle and the seed and exit 1 with every problem.

    ``--write-lock`` first refreshes the lock after a reviewed change.
    ``--require-files`` demands every seed file without a hold, for a run that fetched the seed.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--seed-root", type=Path, default=SEED_ROOT)
    parser.add_argument("--write-lock", action="store_true")
    parser.add_argument("--require-files", action="store_true")
    args = parser.parse_args()
    if args.write_lock:
        (args.root / LOCK_NAME).write_bytes(build_lock(args.root))
    found = problems(args.root, args.seed_root, require_files=args.require_files)
    manifest = json.loads((args.seed_root / SEED_MANIFEST).read_text(encoding="utf-8")) if (args.seed_root / SEED_MANIFEST).is_file() else {"files": []}
    found += lfs_problems([item["path"] for item in manifest["files"]], args.seed_root)
    if found:
        sys.stderr.write("\n".join(found) + "\n")
        raise SystemExit(1)
    held = [item for item in manifest["files"] if "hold" in item]
    sys.stderr.write("".join(f"hold: {item['path']}: {item['hold']}\n" for item in held))
    present = sum((args.seed_root / item["path"]).is_file() for item in manifest["files"])
    sys.stdout.write(
        json.dumps({"bundle": "ok", "files": len(file_entries(args.root)), "seed_files": len(manifest["files"]), "seed_present": present, "held": len(held)})
        + "\n"
    )


if __name__ == "__main__":
    main()
