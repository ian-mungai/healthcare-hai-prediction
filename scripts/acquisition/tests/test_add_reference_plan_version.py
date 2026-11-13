"""The reference download plan moves to version 2, which holds no file. The version 1 plan is archived and catalogued as its predecessor, once."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts.acquisition import add_reference_plan_version as step
from scripts.acquisition import reference_download_contract as contract
from scripts.acquisition import source_registry
from scripts.acquisition.legacy_versions import catalog
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.source_registry import canonical_hash
from tests.support import check

ARCHIVE = "data/acquisition_planning/acquisition_legacy_20261009/pre_seed_plan_versions_20261112"
V1_CANONICAL = "72c2f5b5e38e6b07dfee74216b9cfd4be4b19673513919032d53bf9a1ba4af1c"
V1 = {"version": 1, "model_eligible": False, "registry_sha256": "1" * 64, "files": [{"file_name": "a.txt"}]}
SHIPPED = "2" * 64


def tree(root: Path) -> None:
    config = root / "config/acquisition"
    config.mkdir(parents=True)
    (config / step.PLAN).write_bytes(encoded_json(V1))
    (config / step.PLAN).with_suffix(".lock.json").write_bytes(encoded_json({"plan_sha256": canonical_hash(V1)}))
    empty = {"catalog_version": 1, "plans": {}, "records": {}}
    (config / "legacy_versions.json").write_text(json.dumps(empty, indent=2) + "\n")
    (config / "legacy_versions.lock.json").write_bytes(encoded_json({"catalog_sha256": canonical_hash(empty)}))
    versions = {"catalog_version": 1, "current_registry_sha256": SHIPPED, "legacy_versions": []}
    (config / "registry_versions.json").write_text(json.dumps(versions, indent=2) + "\n")
    (config / "registry_versions.lock.json").write_bytes(encoded_json({"catalog_sha256": canonical_hash(versions)}))


def test_the_successor_binds_the_registry_the_repository_ships_and_holds_no_file(tmp_path: Path) -> None:
    tree(tmp_path)
    step.apply(tmp_path, ARCHIVE)
    plan = json.loads((tmp_path / "config/acquisition" / step.PLAN).read_text())
    check(plan == {"version": 2, "model_eligible": False, "registry_sha256": SHIPPED, "files": []}, f"version 2 plan: {plan}")
    lock = json.loads((tmp_path / "config/acquisition/reference_downloads_plan.lock.json").read_text())
    check(lock == {"plan_sha256": canonical_hash(plan)}, "lock follows the plan")


def test_the_version_1_plan_is_archived_and_catalogued_as_the_predecessor(tmp_path: Path) -> None:
    tree(tmp_path)
    before = (tmp_path / "config/acquisition" / step.PLAN).read_bytes()
    step.apply(tmp_path, ARCHIVE)
    plan = json.loads((tmp_path / "config/acquisition" / step.PLAN).read_text())
    archived = tmp_path / ARCHIVE / step.PLAN
    check(archived.read_bytes() == before, "the old bytes are archived")
    entries = json.loads((tmp_path / "config/acquisition/legacy_versions.json").read_text())["plans"][canonical_hash(plan)]
    check(
        entries == [{"archive_path": f"{ARCHIVE}/{step.PLAN}", "sha256": hashlib.sha256(before).hexdigest(), "canonical_sha256": canonical_hash(V1)}], "entry"
    )
    lock = json.loads((tmp_path / "config/acquisition/legacy_versions.lock.json").read_text())
    check(lock["catalog_sha256"] == canonical_hash(json.loads((tmp_path / "config/acquisition/legacy_versions.json").read_text())), "catalog lock")


def test_a_second_run_is_refused_and_changes_nothing(tmp_path: Path) -> None:
    tree(tmp_path)
    step.apply(tmp_path, ARCHIVE)
    files = {p: p.read_bytes() for p in (tmp_path / "config/acquisition").iterdir()}
    with pytest.raises(ValueError, match="version 1"):
        step.apply(tmp_path, ARCHIVE)
    check(files == {p: p.read_bytes() for p in (tmp_path / "config/acquisition").iterdir()}, "nothing changed")


def test_a_version_catalog_that_differs_from_its_lock_is_refused(tmp_path: Path) -> None:
    tree(tmp_path)
    (tmp_path / "config/acquisition/registry_versions.lock.json").write_bytes(encoded_json({"catalog_sha256": "0" * 64}))
    with pytest.raises(ValueError, match="version catalog"):
        step.apply(tmp_path, ARCHIVE)


def test_a_plan_whose_lock_differs_is_refused(tmp_path: Path) -> None:
    tree(tmp_path)
    (tmp_path / "config/acquisition/reference_downloads_plan.lock.json").write_bytes(encoded_json({"plan_sha256": "0" * 64}))
    with pytest.raises(ValueError, match="lock"):
        step.apply(tmp_path, ARCHIVE)


def test_the_committed_plan_is_version_2_with_the_catalogued_version_1_predecessor() -> None:
    plan = contract.load_plan()
    check(plan["version"] == 2 and plan["files"] == [], "committed plan holds no file")
    entries = catalog()["plans"][canonical_hash(plan)]
    check([e["canonical_sha256"] for e in entries] == [V1_CANONICAL], "version 1 is the only predecessor")
    check(entries[0]["archive_path"] == f"{ARCHIVE}/{step.PLAN}", "archived beside the other predecessors of this round")


def test_the_committed_plan_loads_on_a_tree_that_holds_no_ignored_archive(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A clean Git checkout has no legacy registry archive: the plan's registry is the one the repository ships."""
    monkeypatch.setattr(source_registry, "REPO_ROOT", tmp_path)
    plan = contract.load_plan()
    shipped = json.loads(Path("config/acquisition/registry_versions.json").read_text(encoding="utf-8"))["current_registry_sha256"]
    check(plan["registry_sha256"] == shipped, "the plan binds the shipped registry")
