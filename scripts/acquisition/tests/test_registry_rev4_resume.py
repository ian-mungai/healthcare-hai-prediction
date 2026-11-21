"""The revision 4 builder survives an interruption after any write step: a rerun finishes the work and the plan and registry loaders accept the result.

The starting state is the repository as it was before the builder first ran, rebuilt from Git and the local revision 3 archive.
The collector code version lists come from HEAD: the builder does not regenerate them, and a later collector relist adds versions to them.
The test is skipped when that archive or the history is absent, because the archive is local-only.
"""

from __future__ import annotations

import json
import shutil
import sys
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from scripts.acquisition import census_acs_tables_contract, legacy_versions, nber_hc_contract, reference_download_contract
from scripts.acquisition import source_registry as registry
from scripts.acquisition.one_off.registry_rev4 import build_rev4 as builder
from scripts.process import run_command
from tests.support import check

BUILDER = "scripts/acquisition/one_off/registry_rev4/build_rev4.py"
WRITES = [
    "source_registry.json",
    "source_registry_lock.json",
    "registry_additions.json",
    "registry_additions_lock.json",
    "registry_versions.json",
    "registry_versions.lock.json",
]
STEPS = ["archive", *(f"write:{name}" for name in WRITES), "replace_pin", "relock", "archive_plans", "legacy_catalog", "policies"]
TRACKED = registry.REPO_ROOT / "config/acquisition"
# The builder never writes the receipt index: source_receipt_index.py builds it from the local capture receipts after every change of the verification
# log, so the earlier state keeps the index of its own log and the committed index belongs to the current one.
WRITTEN_ELSEWHERE = {"source_receipt_index.json"}
# The placement derivation (scripts.acquisition.source_metadata) rewrites the rule and detail text of the stored placements after the builder ran, and the
# additions lock follows the additions file. The builder reproduces the revision 3 text of them, so these two files are compared without that text.
REWRITTEN_LATER = {"registry_additions.json", "registry_additions_lock.json"}


def git(*arguments: str) -> str:
    return run_command("git", list(arguments), cwd=registry.REPO_ROOT, check=True).stdout


@pytest.fixture(scope="module")
def before(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The configuration folder as it was before the builder first ran, with revision 3 in place."""
    archived = builder.REV3_ARCHIVE
    if not (archived / "source_registry.json").is_file():
        pytest.skip("the revision 3 archive is local-only and absent here")
    added = git("log", "--diff-filter=A", "--format=%H", "-n1", "--", BUILDER).strip()
    if not added:
        pytest.skip("the commit that added the builder is not in this history")
    root = tmp_path_factory.mktemp("rev4_before")
    target = root / "config/acquisition"
    target.mkdir(parents=True)
    for line in git("ls-tree", f"{added}^", "config/acquisition/").splitlines():
        meta, name = line.split("\t")
        if meta.split()[1] == "blob" and name.endswith(".json"):
            body = git("show", f"HEAD:{name}") if name.endswith("_code_versions.json") else git("show", f"{added}^:{name}")
            source = archived / Path(name).name
            keep = body.startswith("version https://git-lfs") and source.is_file()
            (root / name).write_bytes(source.read_bytes() if keep else body.encode())
    return root


def run(root: Path, stop: str | None = None, cli: bool = False) -> None:
    """Run the builder's write mode inside the sandbox, stopping after the named step; with CLI, through the command line entry point."""
    config = root / "config/acquisition"
    with ExitStack() as stack:
        for module, name, value in [
            (builder, "ROOT", root),
            (builder, "CONFIG", config),
            (builder, "CATALOG", config / "registry_versions.json"),
            (builder, "CATALOG_LOCK", config / "registry_versions.lock.json"),
            (builder, "REV3_ARCHIVE", root / "data/acquisition_planning/acquisition_legacy_20261009/registry_rev3"),
            (builder, "STOP_AFTER", stop),
            (registry, "REPO_ROOT", root),
            (registry, "VERSION_CATALOG_PATH", config / "registry_versions.json"),
            (registry, "VERSION_CATALOG_LOCK_PATH", config / "registry_versions.lock.json"),
        ]:
            stack.enter_context(patch.object(module, name, value))
        stack.enter_context(patch.object(sys, "argv", ["build_rev4", "--write"]))
        check((builder.main() if cli else builder.run(True)) == 0, "the builder reports success")


def loaders_accept(root: Path) -> None:
    """The registry loader and the three real plan loaders accept the folder, and it holds the committed bytes."""
    config = root / "config/acquisition"
    current = registry.load_registry(config / "source_registry.json", config / "source_registry_lock.json")
    check(current["registry_revision"] == 4, "the registry is revision 4")
    with (
        patch.object(registry, "REPO_ROOT", root),
        patch.object(registry, "VERSION_CATALOG_PATH", config / "registry_versions.json"),
        patch.object(registry, "VERSION_CATALOG_LOCK_PATH", config / "registry_versions.lock.json"),
    ):
        earlier = registry.load_registry(config / "source_registry.json", config / "source_registry_lock.json", expected_sha256=builder.REV3)
    check(registry.canonical_hash(earlier) == builder.REV3, "revision 3 resolves through the catalog")
    census_acs_tables_contract.load_plan(config / "census_acs_tables_plan.json")
    reference_download_contract.load_plan(config / "reference_downloads_plan.json")
    with patch.object(nber_hc_contract, "gate_sha256", nber_hc_contract.registry_gate_sha256):
        nber_hc_contract.load_plan(config / "nber_hospital_compare_plan.json")
    skipped = WRITTEN_ELSEWHERE | REWRITTEN_LATER
    differing = [path.name for path in sorted(config.glob("*.json")) if path.name not in skipped and path.read_bytes() != (TRACKED / path.name).read_bytes()]
    check(not differing, f"these files differ from the committed ones: {differing}")
    check(
        without_rederived_text(config) == without_rederived_text(TRACKED),
        "the additions and their lock differ from the committed ones beyond the placement text",
    )


def without_rederived_text(config: Path) -> dict[str, Any]:
    """The additions file without the rule and detail of each stored placement and evidence record, and its lock without the additions hash."""
    additions = json.loads((config / "registry_additions.json").read_text(encoding="utf-8"))
    records = [*additions["dataset_id_placements"], *(record for entry in additions["source_metadata"] for record in entry["dataset_ids_evidence"])]
    for record in records:
        record.pop("rule", None)
        record.pop("detail", None)
    lock = json.loads((config / "registry_additions_lock.json").read_text(encoding="utf-8"))
    lock.pop("additions_sha256")
    return {"additions": additions, "lock": lock}


def sandbox(before: Path, tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    shutil.copytree(before, root)
    return root


def test_the_steps_are_the_steps_the_builder_marks() -> None:
    marked = {step for step in STEPS if step.startswith("write:")}
    check(marked == {f"write:{name}" for name in WRITES}, "the write steps name every output file")


def test_a_full_run_from_the_earlier_state_reproduces_the_committed_files(before: Path, tmp_path: Path) -> None:
    root = sandbox(before, tmp_path)
    run(root)
    loaders_accept(root)


@pytest.mark.parametrize("step", STEPS)
def test_a_rerun_after_an_interruption_finishes_the_work(before: Path, tmp_path: Path, step: str) -> None:
    root = sandbox(before, tmp_path)
    with pytest.raises(builder.Interrupted):
        run(root, stop=step)
    run(root)
    loaders_accept(root)
    run(root)
    loaders_accept(root)


def test_a_partial_archive_is_completed(before: Path, tmp_path: Path) -> None:
    root = sandbox(before, tmp_path)
    archive = root / "data/acquisition_planning/acquisition_legacy_20261009/registry_rev3"
    archive.mkdir(parents=True)
    (archive / "source_registry.json").write_bytes(b'{"truncated": ')
    (archive / "source_registry_lock.json").write_bytes((root / "config/acquisition/source_registry_lock.json").read_bytes())
    run(root)
    loaders_accept(root)
    for name in ("source_registry.json", "source_registry_lock.json", "registry_additions.json", "registry_additions_lock.json"):
        check((archive / name).read_bytes() == (builder.REV3_ARCHIVE / name).read_bytes(), f"the archived {name} is the revision 3 file")


def test_a_plan_changed_after_the_pin_moved_is_refused_not_archived(before: Path, tmp_path: Path) -> None:
    root = sandbox(before, tmp_path)
    config = root / "config/acquisition"
    original = registry.canonical_hash(json.loads((before / "config/acquisition/census_acs_tables_plan.json").read_bytes()))
    with pytest.raises(builder.Interrupted):
        run(root, stop="replace_pin")
    plan = config / "census_acs_tables_plan.json"
    unedited = plan.read_text(encoding="utf-8")
    plan.write_text(unedited.replace('"model_eligible": false', '"model_eligible": false, "extra": 1', 1), encoding="utf-8")
    for attempt in range(3):
        with pytest.raises(registry.RegistryError, match="does not match the lock it had"):
            run(root)
        check(not (root / builder.PLANS_ARCHIVE / plan.name).exists(), f"attempt {attempt + 1}: the changed plan was not archived as the predecessor")
        check(
            json.loads(plan.with_suffix(".lock.json").read_bytes()) == {"plan_sha256": original},
            f"attempt {attempt + 1}: the plan lock still names the real predecessor",
        )
    plan.write_text(unedited, encoding="utf-8")
    run(root)
    loaders_accept(root)
    with (
        patch.object(legacy_versions, "REPO_ROOT", root),
        patch.object(legacy_versions, "ARCHIVE", root / "data/acquisition_planning/acquisition_legacy_20261009"),
        patch.object(legacy_versions, "CATALOG_PATH", config / "legacy_versions.json"),
        patch.object(legacy_versions, "CATALOG_LOCK_PATH", config / "legacy_versions.lock.json"),
    ):
        check(legacy_versions.plan_matches(json.loads(plan.read_bytes()), original), "the real historical predecessor still resolves")


def test_a_plan_outside_the_pins_changed_after_the_pin_moved_is_refused_before_any_lock_moves(before: Path, tmp_path: Path) -> None:
    """census_acs_api_plan_dp02pr.json pins census_acs_api_plan.json, and neither names the registry, so the relock would cascade into the dependent pin."""
    root = sandbox(before, tmp_path)
    config = root / "config/acquisition"
    with pytest.raises(builder.Interrupted):
        run(root, stop="replace_pin")
    upstream, dependent = config / "census_acs_api_plan.json", config / "census_acs_api_plan_dp02pr.json"
    unedited = upstream.read_text(encoding="utf-8")
    upstream.write_text(unedited.replace('"model_eligible": false', '"model_eligible": false, "extra": 1', 1), encoding="utf-8")
    check(upstream.read_text(encoding="utf-8") != unedited, "the planted edit changed the upstream plan")
    held = {path: path.read_bytes() for path in (upstream.with_suffix(".lock.json"), dependent.with_suffix(".lock.json"), dependent, upstream)}
    original = registry.canonical_hash(json.loads((before / "config/acquisition" / dependent.name).read_bytes()))
    for attempt in range(4):
        with pytest.raises(registry.RegistryError, match="outside the plans that pin the registry"):
            run(root, cli=attempt == 3)
        moved = [path.name for path, body in held.items() if path.read_bytes() != body]
        check(not moved, f"attempt {attempt + 1}: these files changed: {moved}")
        archived = [name for name in (upstream.name, dependent.name) if (root / builder.PLANS_ARCHIVE / name).exists()]
        check(not archived, f"attempt {attempt + 1}: these predecessors were archived: {archived}")
    check(json.loads(dependent.with_suffix(".lock.json").read_bytes()) == {"plan_sha256": original}, "the dependent plan lock still names its real predecessor")
    upstream.write_text(unedited, encoding="utf-8")
    run(root)
    loaders_accept(root)
