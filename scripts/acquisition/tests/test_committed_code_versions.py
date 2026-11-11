"""The committed reviewed-version lists admit the code that is on disk, and no version accepted at the last commit was changed or dropped."""

from __future__ import annotations

from pathlib import Path
from types import ModuleType

import pytest

from scripts.acquisition import cms_owners_contract, hcai_util_contract, onc_mu_contract
from scripts.acquisition.code_closure import closure_hashes
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.source_registry import REPO_ROOT, RegistryError
from scripts.process import run_command
from tests.support import check

VERSIONS = REPO_ROOT / "config/acquisition"
ENTRIES = {
    "bls_api": ("bls_api", "scripts.acquisition.collect_bls_api"),
    "census_acs_api": ("census_acs_api", "scripts.acquisition.collect_census_acs_api"),
    "census_acs_detailed": ("census_acs_detailed", "scripts.acquisition.collect_census_acs_detailed"),
    "hud_api": ("hud_api", "scripts.acquisition.collect_hud_api"),
    "hud_api_workbook": ("hud_api", "scripts.acquisition.store_hud_xlsx"),
    "mmd_api": ("mmd_api", "scripts.acquisition.collect_mmd_api"),
    "reference_download": ("reference_download", "scripts.acquisition.store_reference_download"),
    "wonder_export": ("wonder_export", "scripts.acquisition.store_wonder_export"),
}


@pytest.mark.parametrize("contract", [cms_owners_contract, hcai_util_contract, onc_mu_contract], ids=["cms_owners", "hcai_util", "onc_mu"])
def test_collector_admits_its_current_code_against_the_committed_list(contract: ModuleType) -> None:
    """The collectors the Run 3 route changed refuse to fetch unless their committed list holds their code as it is on disk."""
    try:
        admitted = contract.require_code()
    except RegistryError as failure:
        raise AssertionError(f"{contract.__name__} does not admit its current code: {failure}") from failure
    check(admitted == contract.collector_code_hashes(), "The admitted fingerprints are not the closure of the entry module")


@pytest.mark.parametrize("name", sorted(ENTRIES))
def test_other_collector_closure_is_listed_in_its_committed_versions(name: str) -> None:
    """The closure of every other collector entry module appears in its committed list, read from the real file."""
    stem, entry = ENTRIES[name]
    listed = [version["code_sha256"] for version in read_code_versions(VERSIONS / f"{stem}_code_versions.json")["versions"]]
    check(closure_hashes(entry) in listed, f"{name}: no committed version holds the closure of {entry}")


def head_lists() -> dict[str, str]:
    """Return each version list as committed at the last commit, keyed by repository path."""
    names = run_command("git", ["ls-tree", "--name-only", "HEAD", "config/acquisition/"], cwd=REPO_ROOT, check=True).stdout.split()
    return {name: run_command("git", ["show", f"HEAD:{name}"], cwd=REPO_ROOT, check=True).stdout for name in names if name.endswith("_code_versions.json")}


def test_every_version_map_at_the_last_commit_is_still_listed_unchanged(tmp_path: Path) -> None:
    """A stored capture stays verifiable only while the exact map it recorded is listed, so history may only grow at the end."""
    committed = head_lists()
    check(len(committed) >= 10, f"Expected the ten version lists at the last commit, found {len(committed)}")
    changed = []
    for name, text in sorted(committed.items()):
        before = tmp_path / Path(name).name
        before.write_text(text)
        old = [version["code_sha256"] for version in read_code_versions(before)["versions"]]
        new = [version["code_sha256"] for version in read_code_versions(REPO_ROOT / name)["versions"]]
        changed += [f"{name} version {number}" for number, (kept, now) in enumerate(zip(old, new, strict=False), start=1) if kept != now]
        if len(new) < len(old):
            changed.append(f"{name} dropped versions {len(new) + 1} to {len(old)}")
    check(not changed, "Accepted fingerprint maps changed or were dropped: " + ", ".join(changed))
