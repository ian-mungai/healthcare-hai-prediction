"""The committed reviewed-version lists admit the code that is on disk, and no version accepted at the last commit was changed or dropped."""

from __future__ import annotations

import ast
from functools import cache
from pathlib import Path
from types import ModuleType

import pytest

from scripts.acquisition import cms_owners_contract, hcai_util_contract, onc_mu_contract
from scripts.acquisition.code_closure import closure_hashes
from scripts.acquisition.code_versions import read_code_versions
from scripts.acquisition.source_registry import REPO_ROOT, RegistryError
from scripts.process import run_command
from scripts.quality.committed_lists import COLLECTOR_FILES
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
# Calls that bind a collector to a committed code-version list: a module outside the contracts and the E2E runners that makes one is a collector entry.
BINDING_CALLS = {"require_code", "reviewed_code_versions", "closure_hashes", "collector_code_hashes"}


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
    """The commit hook selects staged paths with COLLECTOR_FILES (scripts/quality/committed_lists.py): a closure file it misses could change unchecked."""
    entries = {entry for _, entry in ENTRIES.values()} | {contract.CODE_ENTRY for contract in CONTRACTS}
    files = {path for entry in entries for path in entry_hashes(entry)} | {
        path.relative_to(REPO_ROOT).as_posix() for path in VERSIONS.glob("*_code_versions.json")
    }
    missed = sorted(path for path in files if not COLLECTOR_FILES.search(path))
    check(not missed, f"Files that COLLECTOR_FILES in scripts/quality/committed_lists.py does not select: {missed}")


def binding_names(source: str) -> set[str]:
    """The binding calls that a module makes, under any import alias: only a call counts. A called name resolves through its import
    (``from m import require_code as rc`` then ``rc()``) and a called attribute counts by its own name (``module.require_code()``, whatever the module's name).
    An import, a re-export or a variable of the same name is no call, so it binds nothing."""
    tree = ast.parse(source)
    imported = {alias.asname or alias.name: alias.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) for alias in node.names}
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name):
            names.add(imported.get(node.func.id, node.func.id))
        elif isinstance(node.func, ast.Attribute):
            names.add(node.func.attr)
    return names & BINDING_CALLS


@pytest.mark.parametrize(
    "source",
    [
        "from scripts.acquisition.code_versions import require_code\nrequire_code()\n",
        "from scripts.acquisition.code_versions import require_code as admit\nadmit()\n",
        "from scripts.acquisition.code_versions import collector_code_hashes as hashes, require_code as admit\nhashes()\n",
        "from scripts.acquisition import code_versions as versions\nversions.require_code()\n",
        "import scripts.acquisition.code_closure as closure\nclosure.closure_hashes('entry')\n",
    ],
)
def test_a_binding_call_is_found_under_an_import_alias(source: str) -> None:
    """An alias of the import or of its module does not hide the call that binds a collector to a list."""
    check(binding_names(source), f"No binding call found in: {source!r}")


@pytest.mark.parametrize(
    "source",
    [
        "from scripts.process import run_command as run\nrun()\n",
        "def receipt(entry):\n    closure_hashes = {}\n    return closure_hashes.get(entry)\n",
        "from scripts.acquisition.collect_bls_api import closure_hashes, require_code\n__all__ = ['closure_hashes', 'require_code']\n",
        "from scripts.acquisition.code_closure import closure_hashes as hashes\nfingerprint = hashes\n",
    ],
    ids=["unrelated_call", "local_variable", "re_export", "alias_not_called"],
)
def test_a_module_without_a_binding_call_binds_nothing(source: str) -> None:
    """Only a call binds a collector to a list: an unrelated call, a local variable of that name, a re-export or an uncalled alias binds nothing."""
    check(not binding_names(source), f"A module with no binding call was taken for a collector entry: {source!r}")


def test_every_collector_entry_is_checked_against_its_list() -> None:
    """A collector that binds itself to a list (a call of require_code, reviewed_code_versions, closure_hashes or collector_code_hashes, under any alias)
    must be an entry above or a contract entry, so a new collector that reuses an existing list has its own closure checked too."""
    checked = {entry for _, entry in ENTRIES.values()} | {contract.CODE_ENTRY for contract in CONTRACTS}
    bound: set[str] = set()
    for path in sorted((REPO_ROOT / "scripts/acquisition").glob("*.py")):
        if path.stem.endswith(("_contract", "_e2e")):
            continue
        if binding_names(path.read_text(encoding="utf-8")):
            bound.add(f"scripts.acquisition.{path.stem}")
    check(bound <= checked, f"Collector entries with no closure check (add each to ENTRIES with its list): {sorted(bound - checked)}")


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
