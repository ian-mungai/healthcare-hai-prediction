"""One-time development step: move every pin of the old legacy registry hash to the rewritten registry and re-lock what pinned it.

The rewritten registry has a new canonical hash. This step (1) keeps the old registry version in the version catalog, with its files archived,
(2) lists the rewritten version, (3) moves the old hash in every plan and planning file, (4) re-locks each plan and decision whose canonical hash
changed, repeating until no lock changes (a plan can pin another plan's hash), (5) archives each old plan and lists it as a predecessor of the new
one so captures that recorded it still verify, and (6) re-locks the projection policies. The current registry's own lineage value stays as written.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any

from scripts.acquisition.derivative_policy import lock_value
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.source_registry import canonical_hash

CONFIG = "config/acquisition"
LOCK_KEYS = ("plan_sha256", "decision_sha256")
CATALOGS = ("registry_versions", "legacy_versions", "source_registry", "registry_additions")
ARCHIVED_FOLDER = "registry_legacy_20260929"


def digest(body: bytes) -> str:
    """SHA-256 of bytes."""
    return hashlib.sha256(body).hexdigest()


def catalog_bytes(value: dict[str, Any]) -> bytes:
    """The layout of the two version catalogs: two-space indent, insertion order, trailing newline."""
    return (json.dumps(value, indent=2) + "\n").encode()


def pin_files(config: Path) -> list[Path]:
    """The JSON files that hold pins as text: not locks, not the catalogs handled by name, not the tracked bundle (its bytes are pinned by digest)."""
    skipped = tuple(CATALOGS)
    return [
        path
        for path in sorted(config.rglob("*.json"))
        if not path.name.endswith(".lock.json") and not path.name.startswith(skipped) and "baseline_run" not in path.relative_to(config).parts
    ]


def replace_pin(config: Path, old: str, new: str) -> None:
    """Replace one hash with another in every pin file that holds it."""
    for path in pin_files(config):
        text = path.read_text(encoding="utf-8")
        if old in text:
            path.write_text(text.replace(old, new), encoding="utf-8")


def versions_catalog(root: Path, old: str, new: str, old_dir: Path, new_dir: Path, archive: str) -> None:
    """List the rewritten registry version and keep the old one, with its files archived inside the legacy archive."""
    config = root / CONFIG
    catalog = json.loads((config / "registry_versions.json").read_text(encoding="utf-8"))
    versions = catalog["legacy_versions"]
    if any(item["registry_sha256"] == new for item in versions):
        raise ValueError(f"The registry hash {new} is already re-pinned")
    position = next((index for index, item in enumerate(versions) if item["registry_sha256"] == old), None)
    if position is None:
        raise ValueError(f"The registry hash {old} is not in the version catalog")
    original = versions[position]
    for item in original["files"].values():
        if digest((old_dir / Path(item["path"]).name).read_bytes()) != item["sha256"]:
            raise ValueError(f"{item['path']} differs from its catalogued digest")
    rewritten = copy.deepcopy(original)
    rewritten["registry_sha256"] = new
    for item in rewritten["files"].values():
        item["sha256"] = digest((new_dir / Path(item["path"]).name).read_bytes())
    for item in original["files"].values():
        name = Path(item["path"]).name
        target = root / archive / ARCHIVED_FOLDER / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(old_dir / name, target)
        item["path"] = f"{archive}/{ARCHIVED_FOLDER}/{name}"
    original["registry_path"] = original["files"]["registry"]["path"]
    original["lock_path"] = original["files"]["lock"]["path"]
    versions[position] = rewritten
    versions.append(original)
    (config / "registry_versions.json").write_bytes(catalog_bytes(catalog))
    (config / "registry_versions.lock.json").write_bytes(encoded_json({"catalog_sha256": canonical_hash(catalog)}))


def relock_to_fixed_point(root: Path) -> dict[str, tuple[Path, str, str]]:
    """Recompute every plan and decision lock whose canonical hash changed, and move the old hash in the files that pin it, until nothing changes."""
    config = root / CONFIG
    changed: dict[str, tuple[Path, str, str]] = {}
    for _ in range(20):
        progress = False
        for lock_path in sorted(config.glob("*.lock.json")):
            lock = json.loads(lock_path.read_text(encoding="utf-8"))
            keys = [key for key in LOCK_KEYS if set(lock) == {key}]
            base = lock_path.with_name(lock_path.name.removesuffix(".lock.json") + ".json")
            if not keys or not base.is_file():
                continue
            fresh = canonical_hash(json.loads(base.read_text(encoding="utf-8")))
            if fresh != lock[keys[0]]:
                changed[lock[keys[0]]] = (base, keys[0], fresh)
                lock_path.write_bytes(encoded_json({keys[0]: fresh}))
                replace_pin(config, lock[keys[0]], fresh)
                progress = True
        if not progress:
            return changed
    raise ValueError("The locks did not settle after 20 passes")


def legacy_catalog(root: Path, changed: dict[str, tuple[Path, str, str]], before: dict[Path, bytes], archive: str) -> None:
    """Archive each old plan or decision and list it as a predecessor of the new one."""
    config = root / CONFIG
    catalog = json.loads((config / "legacy_versions.json").read_text(encoding="utf-8"))
    for old, (base, key, fresh) in changed.items():
        target = root / archive / base.name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(before[base])
        entry = {"archive_path": f"{archive}/{base.name}", "sha256": digest(before[base]), "canonical_sha256": old}
        if key == "plan_sha256":
            catalog["plans"][fresh] = [*catalog["plans"].pop(old, []), entry]
        else:
            catalog["records"].setdefault(base.relative_to(root).as_posix(), []).append(entry)
    (config / "legacy_versions.json").write_bytes(catalog_bytes(catalog))
    (config / "legacy_versions.lock.json").write_bytes(encoded_json({"catalog_sha256": canonical_hash(catalog)}))


def relock_policies(config: Path) -> None:
    """Rebuild the projection policy lock the way the policy builder writes it."""
    policies = json.loads((config / "projection_policies.json").read_text(encoding="utf-8"))
    lock = {"policies_sha256": lock_value(policies), "entries": {entry["id"]: lock_value(entry) for entry in policies["entries"]}}
    (config / "projection_policies.lock.json").write_bytes(encoded_json(lock))


def repin(root: Path, old: str, new: str, old_dir: Path, new_dir: Path, archive: str) -> dict[str, Any]:
    """Re-pin the repository at ROOT from the old registry hash to the new one. ARCHIVE is a repository-relative folder inside the legacy archive."""
    config = root / CONFIG
    before = {path: path.read_bytes() for path in config.rglob("*.json")}
    versions_catalog(root, old, new, old_dir, new_dir, archive)
    replace_pin(config, old, new)
    changed = relock_to_fixed_point(root)
    legacy_catalog(root, changed, before, archive)
    relock_policies(config)
    return {"locks_changed": sorted(base.name for base, _, _ in changed.values())}


def main() -> None:
    """Re-pin the repository in the current folder and print the files whose locks changed."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("old", help="the old registry canonical hash")
    parser.add_argument("new", help="the rewritten registry canonical hash")
    parser.add_argument("old_dir", type=Path, help="folder with the four original registry files")
    parser.add_argument("new_dir", type=Path, help="folder with the four rewritten registry files")
    parser.add_argument("--archive", default="data/acquisition_planning/acquisition_legacy_20261009/pre_registry_rewrite_20261112")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    sys.stdout.write(json.dumps(repin(args.root, args.old, args.new, args.old_dir, args.new_dir, args.archive), indent=2) + "\n")


if __name__ == "__main__":
    main()
