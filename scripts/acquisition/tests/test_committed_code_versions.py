"""The committed reviewed-version lists admit the code that is on disk, and no version accepted at the last commit was changed or dropped."""

from __future__ import annotations

import re
from functools import cache
from pathlib import Path
from types import ModuleType

import pytest

from scripts.acquisition import cms_owners_contract, hcai_util_contract, onc_mu_contract
from scripts.acquisition.code_closure import closure_hashes
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.source_registry import REPO_ROOT, RegistryError
from scripts.process import run_command
from scripts.quality.repo_checks import load_yaml
from tests.support import check

VERSIONS = REPO_ROOT / "config/acquisition"
ENTRIES = {
    "bls_api": ("bls_api", "scripts.acquisition.collect_bls_api"),
    "census_acs_api": ("census_acs_api", "scripts.acquisition.collect_census_acs_api"),
    "census_acs_detailed": ("census_acs_detailed", "scripts.acquisition.collect_census_acs_detailed"),
    "census_acs_tables": ("census_acs_tables", "scripts.acquisition.collect_census_acs_tables"),
    "hud_api": ("hud_api", "scripts.acquisition.collect_hud_api"),
    "hud_api_workbook": ("hud_api", "scripts.acquisition.store_hud_xlsx"),
    "mmd_api": ("mmd_api", "scripts.acquisition.collect_mmd_api"),
    "nber_hc": ("nber_hc", "scripts.acquisition.store_nber_hc"),
    "reference_download": ("reference_download", "scripts.acquisition.store_reference_download"),
    "wonder_export": ("wonder_export", "scripts.acquisition.store_wonder_export"),
}


CONTRACTS = [cms_owners_contract, hcai_util_contract, onc_mu_contract]
COMMIT_HOOK = "collector-code-versions"


@cache
def entry_hashes(entry: str) -> dict[str, str]:
    """The closure of an entry module, computed once for the cases below."""
    return closure_hashes(entry)


@pytest.mark.parametrize("contract", CONTRACTS, ids=["cms_owners", "hcai_util", "onc_mu"])
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
    check(entry_hashes(entry) in listed, f"{name}: no committed version holds the closure of {entry}")


def test_every_committed_version_list_has_its_closure_checked() -> None:
    """A list that no case checks cannot go stale unseen: each ``*_code_versions.json`` belongs to an entry above or to one of the three contract cases."""
    contract_lists = {"cms_owners", "hcai_util", "onc_mu"}
    checked = {stem for stem, _ in ENTRIES.values()} | contract_lists
    present = {path.name.removesuffix("_code_versions.json") for path in VERSIONS.glob("*_code_versions.json")}
    check(present <= checked, f"version lists with no closure check: {sorted(present - checked)}")


def test_the_commit_hook_selects_every_file_of_every_collector_closure() -> None:
    """The commit hook that runs these cases (.pre-commit-config.yaml) selects by a file pattern: a closure file the pattern misses could change unchecked."""
    document = load_yaml((REPO_ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise AssertionError(".pre-commit-config.yaml must contain a mapping")
    hooks = [entry for repo in document["repos"] for entry in repo["hooks"] if entry["id"] == COMMIT_HOOK]
    check(len(hooks) == 1 and bool(hooks[0].get("files")), f"The hook {COMMIT_HOOK} needs a files pattern")
    pattern = re.compile(hooks[0]["files"])
    entries = {entry for _, entry in ENTRIES.values()} | {contract.CODE_ENTRY for contract in CONTRACTS}
    files = {path for entry in entries for path in entry_hashes(entry)} | {
        path.relative_to(REPO_ROOT).as_posix() for path in VERSIONS.glob("*_code_versions.json")
    }
    missed = sorted(path for path in files if not pattern.search(path))
    check(not missed, f"Files that the {COMMIT_HOOK} pattern in .pre-commit-config.yaml does not select: {missed}")


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
