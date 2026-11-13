"""Re-pinning moves every pin of the old registry hash to the rewritten registry, re-locks the plans and catalogs, and archives the old versions."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts.acquisition import repin_registry as repin
from scripts.acquisition.derivative_policy import lock_value
from scripts.acquisition.source_registry import canonical_hash
from tests.support import check

OLD = "1" * 64
NEW = "2" * 64
ARCHIVE = "data/acquisition_planning/acquisition_legacy_20261009"
REGISTRY_FOLDER = "data/acquisition_planning/registry_legacy_20260929"
NAMES = ("source_registry.json", "source_registry_lock.json", "registry_additions.json", "registry_additions_lock.json")


def dump(value: dict, indent: int = 2, sort: bool = True) -> bytes:
    return (json.dumps(value, indent=indent, sort_keys=sort) + "\n").encode()


def digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def tree(root: Path) -> dict:
    """A small repository: a registry version, plan A pinned to it, plan B pinned to plan A's hash, catalogs and a policy file."""
    config = root / "config/acquisition"
    config.mkdir(parents=True)
    old_dir = root / "old_registry"
    old_dir.mkdir()
    for name in NAMES:
        (old_dir / name).write_text(f"{name} old\n")
    plan_a = {"name": "a", "registry_sha256": OLD}
    (config / "a_plan.json").write_bytes(dump(plan_a))
    (config / "a_plan.lock.json").write_bytes(dump({"plan_sha256": canonical_hash(plan_a)}))
    plan_b = {"name": "b", "registry_sha256": OLD, "base_plan_sha256": canonical_hash(plan_a)}
    (config / "b_plan.json").write_bytes(dump(plan_b))
    (config / "b_plan.lock.json").write_bytes(dump({"plan_sha256": canonical_hash(plan_b)}))
    decision = {"plan_sha256": canonical_hash(plan_b)}
    (config / "decision.json").write_bytes(dump(decision))
    (config / "decision.lock.json").write_bytes(dump({"decision_sha256": canonical_hash(decision)}))
    paths = {name: {"path": f"{REGISTRY_FOLDER}/{name}", "sha256": digest(f"{name} old\n".encode())} for name in NAMES}
    versions = {
        "catalog_version": 1,
        "legacy_versions": [
            {
                "registry_sha256": OLD,
                "registry_path": paths[NAMES[0]]["path"],
                "lock_path": paths[NAMES[1]]["path"],
                "files": dict(zip(("registry", "lock", "additions", "additions_lock"), paths.values(), strict=True)),
            }
        ],
    }
    (config / "registry_versions.json").write_bytes(dump(versions, sort=False))
    (config / "registry_versions.lock.json").write_bytes(dump({"catalog_sha256": canonical_hash(versions)}))
    legacy = {"catalog_version": 1, "plans": {canonical_hash(plan_a): [], canonical_hash(plan_b): []}, "records": {"config/acquisition/decision.json": []}}
    (config / "legacy_versions.json").write_bytes(dump(legacy, sort=False))
    (config / "legacy_versions.lock.json").write_bytes(dump({"catalog_sha256": canonical_hash(legacy)}))
    entry = {"id": "r1", "plan_sha256": canonical_hash(plan_a)}
    policies = {"version": 1, "entries": [entry]}
    (config / "projection_policies.json").write_bytes(dump(policies))
    (config / "projection_policies.lock.json").write_bytes(dump({"policies_sha256": lock_value(policies), "entries": {"r1": lock_value(entry)}}))
    new_dir = root / "new_registry"
    new_dir.mkdir()
    for name in NAMES:
        (new_dir / name).write_text(f"{name} new\n")
    return {"old_dir": old_dir, "new_dir": new_dir, "plan_a": plan_a, "plan_b": plan_b}


def run(root: Path, parts: dict) -> dict:
    return repin.repin(root, OLD, NEW, parts["old_dir"], parts["new_dir"], f"{ARCHIVE}/pre_registry_rewrite")


def test_pins_move_and_locks_follow_the_changed_plans(tmp_path: Path) -> None:
    parts = tree(tmp_path)
    run(tmp_path, parts)
    config = tmp_path / "config/acquisition"
    plan_a, plan_b = (json.loads((config / f"{n}_plan.json").read_text()) for n in "ab")
    check(plan_a["registry_sha256"] == NEW and plan_b["registry_sha256"] == NEW, "registry pin moved")
    check(plan_b["base_plan_sha256"] == canonical_hash(plan_a), "plan B follows plan A's new hash")
    for name, plan in (("a_plan", plan_a), ("b_plan", plan_b)):
        check(json.loads((config / f"{name}.lock.json").read_text())["plan_sha256"] == canonical_hash(plan), f"{name} lock")
    decision = json.loads((config / "decision.json").read_text())
    check(decision["plan_sha256"] == canonical_hash(plan_b), "decision pins plan B's new hash")
    check(json.loads((config / "decision.lock.json").read_text())["decision_sha256"] == canonical_hash(decision), "decision lock")


def test_catalogs_keep_the_old_versions_and_list_the_new_ones(tmp_path: Path) -> None:
    parts = tree(tmp_path)
    run(tmp_path, parts)
    config = tmp_path / "config/acquisition"
    versions = json.loads((config / "registry_versions.json").read_text())
    by_hash = {v["registry_sha256"]: v for v in versions["legacy_versions"]}
    check(set(by_hash) == {OLD, NEW}, "both registry versions listed")
    for name, key in zip(NAMES, ("registry", "lock", "additions", "additions_lock"), strict=True):
        new_file = by_hash[NEW]["files"][key]
        old_file = by_hash[OLD]["files"][key]
        check(new_file["path"] == f"{REGISTRY_FOLDER}/{name}" and new_file["sha256"] == digest(f"{name} new\n".encode()), "new entry names the rewritten file")
        archived = tmp_path / old_file["path"]
        check(old_file["path"].startswith(f"{ARCHIVE}/") and digest(archived.read_bytes()) == old_file["sha256"], "old file archived and still bound")
    check(json.loads((config / "registry_versions.lock.json").read_text())["catalog_sha256"] == canonical_hash(versions), "registry catalog lock")
    legacy = json.loads((config / "legacy_versions.json").read_text())
    plan_a = json.loads((config / "a_plan.json").read_text())
    entries = legacy["plans"][canonical_hash(plan_a)]
    check(len(entries) == 1 and entries[0]["canonical_sha256"] == canonical_hash(parts["plan_a"]), "old plan A listed under its new hash")
    check(digest((tmp_path / entries[0]["archive_path"]).read_bytes()) == entries[0]["sha256"], "old plan A archived")
    check(OLD not in json.dumps(legacy) and len(legacy["records"]["config/acquisition/decision.json"]) == 1, "decision predecessor recorded")
    check(json.loads((config / "legacy_versions.lock.json").read_text())["catalog_sha256"] == canonical_hash(legacy), "legacy catalog lock")


def test_policies_follow_the_new_plan_hash_and_relock(tmp_path: Path) -> None:
    parts = tree(tmp_path)
    run(tmp_path, parts)
    config = tmp_path / "config/acquisition"
    policies = json.loads((config / "projection_policies.json").read_text())
    plan_a = json.loads((config / "a_plan.json").read_text())
    check(policies["entries"][0]["plan_sha256"] == canonical_hash(plan_a), "policy pin moved")
    lock = json.loads((config / "projection_policies.lock.json").read_text())
    check(lock["policies_sha256"] == lock_value(policies) and lock["entries"]["r1"] == lock_value(policies["entries"][0]), "policy lock")


def test_a_second_run_is_refused_because_the_old_hash_is_gone(tmp_path: Path) -> None:
    parts = tree(tmp_path)
    run(tmp_path, parts)
    with pytest.raises(ValueError, match="already re-pinned"):
        run(tmp_path, parts)


def test_a_source_registry_file_that_changed_since_its_catalog_entry_is_refused(tmp_path: Path) -> None:
    parts = tree(tmp_path)
    (parts["old_dir"] / NAMES[0]).write_text("tampered\n")
    with pytest.raises(ValueError, match="differs from its catalogued digest"):
        run(tmp_path, parts)


def test_the_tracked_bundle_is_never_rewritten_by_a_pin_move(tmp_path: Path) -> None:
    parts = tree(tmp_path)
    bundled = tmp_path / "config/acquisition/baseline_run/plan_inputs/data/x/collection_plan.json"
    bundled.parent.mkdir(parents=True)
    bundled.write_text(f'{{"registry_sha256": "{OLD}"}}\n')
    run(tmp_path, parts)
    check(OLD in bundled.read_text(), "bundle file untouched")
