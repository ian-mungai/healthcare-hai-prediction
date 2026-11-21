"""Offline check of the tracked baseline run inputs bundle, run by CI on every pull request that touches it.

It recomputes the inputs lock, then checks the queue (lock, active units only, no duplicate IDs, budget sums), the capture specification, the hosts file with
its approval and the required secrets list against the secrets the collector plans and the key reader name. It also checks that every expected plan input has
exactly one home (a tracked copy or an entry of the bootstrap seed manifest) and that the seed folder matches its manifest: the files the queue's manual HUD and
WONDER units store (a unit's seed holds only its original), the digests, the Git LFS rule, and that each seeded HUD workbook and WONDER original rebuilds
the derived file its unit's last capture records. It makes no request and reads only the bundle, the seed folder and the collector plans.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path

from scripts.acquisition.ahrq_seed_copy import COPIES
from scripts.acquisition.api_keys import SHARED_KEYS
from scripts.acquisition.history_routes import RELEASE_BINDINGS, lineage_bindings
from scripts.acquisition.manual_content import (
    MODES,
    REBUILT_MODES,
    SEED_ONLY_MODES,
    ContentRefusal,
    ManualUnitError,
    Scopes,
    accept,
    archive_binding,
    rebuild,
    recorded_derived,
)
from scripts.acquisition.redownload_route import ACTIVE, host_problems
from scripts.acquisition.s3_store import access_released, encoded_json
from scripts.process import run_command

ROOT = Path("config/acquisition/baseline_run")
SEED_ROOT = Path("config/acquisition/bootstrap_seed")
LOCK_NAME = "inputs.lock.json"
EXPECTED_NAME = "expected_plan_inputs.json"
SEED_MANIFEST = "manifest.json"
SEED_DOCUMENTS = frozenset({SEED_MANIFEST, "README.md", "release_assets.json"})
SEED_KIND = "bootstrap_seed_manifest"
# The manual units whose stored files the seed holds. The ACS units are manual too, but they move to the Census API collector (owner, Nov 12 2026).
SEED_MODES = ("hud_xlsx", "wonder_export", "ahrq_compendium", "hospital_archive")
SECRETS_NAME = "required_secrets.json"
SECRETS_KIND = "bootstrap_required_secrets"
CONFIG = Path("config/acquisition")
# The collector mode of a queue unit to the secret its collector reads.
MODE_SECRETS = {
    "bls_api_v2": "bls_api_key",
    "hud_api": "hud_api_key",
    "census_acs_api": "census_api_key",
    "census_acs_detailed": "census_api_key",
    "census_acs_tables": "census_api_key",
}
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


def release_problems(specs: dict[str, dict]) -> list[str]:
    """Return why a fresh capture of a held source's reference document would fail at storage on a Git-only tree.

    A template never carries a release hash, which would bind a record that only an ignored archive holds. The unit names the tracked record in
    ``release_binding`` and storage must free the unit's source through that record.
    """
    problems = []
    for snapshot, spec in sorted(specs.items()):
        template = spec.get("receipt_template", {})
        if "lineage_bindings" in template:
            problems.append(f"unit {snapshot} carries a release hash in its receipt template: name release_binding instead")
        name = spec.get("release_binding")
        if name is None:
            continue
        # A unit without a receipt template (a seed-only unit) names its source in the specification. A source with its own record is bound by that record.
        source = {"source_id": template.get("source_id") or spec.get("source_id")}
        if name not in RELEASE_BINDINGS:
            problems.append(f"unit {snapshot} names the release binding {name}, which is no tracked record")
        elif not access_released(source, lineage_bindings({"preferred_route": "access_hold"} | source, {"release_binding": name})):
            problems.append(f"the tracked {name} record does not release {source['source_id']} of unit {snapshot}")
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
    return problems + release_problems(document["specs"])


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
    """The seed path of every stored data file of a manual HUD or WONDER unit, to its unit: ``manual_units/<unit>/<file name>``.

    A unit whose derived file is rebuilt from its original (a HUD workbook or a WONDER export) is seeded with the original only.
    """
    queue = json.loads((root / "queue.json").read_bytes())["queue"]
    specs = json.loads((root / "capture_specs.json").read_text(encoding="utf-8"))["specs"]
    return {
        f"manual_units/{unit['snapshot_id']}/{name}": unit["snapshot_id"]
        for unit in queue
        if unit["disposition"] == "approved_manual" and unit["mode"] in SEED_MODES
        for name in specs[unit["snapshot_id"]]["stored_data_files"]
        if not (unit["mode"] in REBUILT_MODES and name == MODES[unit["mode"]][1])
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


def derivation_problems(root: Path, seed_root: Path, scopes: Scopes | None = None) -> list[str]:
    """Return why a HUD or WONDER unit's derived file cannot be proven from its seeded original.

    Each unit must record the SHA-256 of its derived file in its last capture. A seeded original, bound to the plan by its digest, must rebuild a file
    equal to that digest and size. An absent original is pending, like any other absent seed file.
    """
    queue = json.loads((root / "queue.json").read_bytes())["queue"]
    specs = json.loads((root / "capture_specs.json").read_text(encoding="utf-8"))["specs"]
    scopes = Scopes() if scopes is None else scopes
    found = []
    for unit in queue:
        if unit["disposition"] != "approved_manual" or unit["mode"] not in REBUILT_MODES:
            continue
        spec, derived = specs[unit["snapshot_id"]], MODES[unit["mode"]][1]
        if recorded_derived(unit["mode"], spec) is None:
            found.append(f"unit {unit['snapshot_id']} records no digest of its derived {derived}")
            continue
        originals = [name for name in spec["stored_data_files"] if name != derived]
        if len(originals) != 1:
            found.append(f"unit {unit['snapshot_id']} needs exactly one original besides its derived {derived}")
            continue
        path = seed_root / f"manual_units/{unit['snapshot_id']}/{originals[0]}"
        if path.is_file():
            try:
                rebuild(unit["mode"], path, originals[0], spec, scopes)
            except ContentRefusal:
                found.append(f"manual_units/{unit['snapshot_id']}/{originals[0]} does not rebuild the recorded {derived} of unit {unit['snapshot_id']}")
    return found


def seed_only_problems(root: Path, seed_root: Path, scopes: Scopes | None = None) -> list[str]:
    """Return the seeded originals of the seed-only units that their plan or specification does not bind. An absent original is pending."""
    queue = json.loads((root / "queue.json").read_bytes())["queue"]
    specs = json.loads((root / "capture_specs.json").read_text(encoding="utf-8"))["specs"]
    scopes = Scopes() if scopes is None else scopes
    found = []
    for unit in queue:
        if unit["disposition"] != "approved_manual" or unit["mode"] not in SEED_ONLY_MODES:
            continue
        names = specs[unit["snapshot_id"]]["stored_data_files"]
        if len(names) != 1:
            found.append(f"unit {unit['snapshot_id']} needs exactly one stored file")
            continue
        try:
            binding = archive_binding(unit, specs[unit["snapshot_id"]]) if unit["mode"] == "hospital_archive" else None
        except ManualUnitError as refusal:
            found.append(f"unit {unit['snapshot_id']}: {refusal}")
            continue
        path = seed_root / f"manual_units/{unit['snapshot_id']}/{names[0]}"
        if path.is_file():
            try:
                accept(unit["mode"], path, names[0], scopes, binding)
            except ContentRefusal:
                found.append(f"manual_units/{unit['snapshot_id']}/{names[0]} is not the file the locked plan binds for unit {unit['snapshot_id']}")
    return found


def copy_problems(root: Path, plan: dict) -> list[str]:
    """Return why the seeded copies of the AHRQ files are not recorded exactly: a spec records the original and the change for a copy only.

    The plan must bind the copy by its own digest, never the original, so the unit and the seeded file agree.
    """
    queue = json.loads((root / "queue.json").read_bytes())["queue"]
    specs = json.loads((root / "capture_specs.json").read_text(encoding="utf-8"))["specs"]
    planned = {item["file_name"]: item for item in plan["files"]}
    found = []
    for unit in queue:
        if unit["disposition"] != "approved_manual" or unit["mode"] not in SEED_ONLY_MODES:
            continue
        spec = specs[unit["snapshot_id"]]
        name = spec["stored_data_files"][0]
        recorded = COPIES.get(name)
        if recorded is None:
            if "seeded_copy" in spec:
                found.append(f"unit {unit['snapshot_id']} records a seeded_copy for {name}, which is no seeded copy")
            continue
        if spec.get("seeded_copy") != recorded:
            found.append(f"unit {unit['snapshot_id']} must record the original and the change of its seeded_copy exactly")
        entry = planned.get(name)
        if entry is None or (entry["sha256"], entry["bytes"]) == (recorded["original_sha256"], recorded["original_bytes"]):
            found.append(f"the plan binds the original of {name}, not its seeded copy")
    return found


def release_assets(seed_root: Path) -> list[dict]:
    """The assets of the seed release manifest, or none when the seed has no such manifest."""
    path = seed_root / "release_assets.json"
    return json.loads(path.read_text(encoding="utf-8"))["assets"] if path.is_file() else []


def release_problems_of_seed(root: Path, seed_root: Path) -> list[str]:
    """Return why the seed release manifest is not exactly what the queue and the specifications give: the units whose seed files are release assets, each with
    its size, its recorded digest and its address."""
    from scripts.acquisition.build_release_manifest import build

    path = seed_root / "release_assets.json"
    if not path.is_file():
        return []
    document = json.loads(path.read_text(encoding="utf-8"))
    specs = json.loads((root / "capture_specs.json").read_text(encoding="utf-8"))
    expected = build((root / "queue.json").read_bytes(), specs, document["tag"])
    return [] if json.loads(expected) == document else ["release_assets.json differs from the queue and the specifications it is built from"]


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
        found += derivation_problems(root, seed_root)
        found += seed_only_problems(root, seed_root)
        found += release_problems_of_seed(root, seed_root)
        found += copy_problems(root, Scopes().plan("ahrq_compendium"))
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
    released = {item["path"] for item in release_assets(args.seed_root)}
    found += lfs_problems([item["path"] for item in manifest["files"] if item["path"] not in released], args.seed_root)
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
