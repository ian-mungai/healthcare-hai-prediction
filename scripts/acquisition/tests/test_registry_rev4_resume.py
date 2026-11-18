"""The revision 4 builder survives an interruption after any write step: a rerun finishes the work and the plan and registry loaders accept the result.

The starting state is the repository as it was before the builder first ran, rebuilt from Git and the local revision 3 archive.
The test is skipped when that archive or the history is absent, because the archive is local-only.
"""

from __future__ import annotations

import shutil
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.acquisition import census_acs_tables_contract, nber_hc_contract, reference_download_contract
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
            body = git("show", f"{added}^:{name}")
            source = archived / Path(name).name
            keep = body.startswith("version https://git-lfs") and source.is_file()
            (root / name).write_bytes(source.read_bytes() if keep else body.encode())
    return root


def run(root: Path, stop: str | None = None) -> None:
    """Run the builder's write mode inside the sandbox, stopping after the named step."""
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
        check(builder.run(True) == 0, "the builder reports success")


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
    differing = [path.name for path in sorted(config.glob("*.json")) if path.read_bytes() != (TRACKED / path.name).read_bytes()]
    check(not differing, f"these files differ from the committed ones: {differing}")


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
