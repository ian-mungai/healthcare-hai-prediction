"""Prove typed code-fingerprint records preserve exact historical approval mappings, and that each collector binds only the code it can run."""

import argparse
import copy
import json
import shutil
import tempfile
from collections.abc import Callable
from functools import partial
from pathlib import Path
from unittest.mock import patch

from scripts.acquisition import bls_api_contract, census_acs_api_contract, census_acs_detailed_contract, code_closure, hud_api_contract
from scripts.acquisition.source_registry import REPO_ROOT, RegistryError, canonical_hash, read_json, require


def exercise() -> list[dict]:
    """Exercise real metadata through legacy and typed representations plus malformed copies."""
    from scripts.acquisition.code_versions import read_code_versions
    from scripts.acquisition.legacy_versions import ARCHIVE, catalog, predecessor_bytes

    cases = []
    manifest = read_json(REPO_ROOT / "config/acquisition/code_version_migration.json")
    records = catalog().get("records", {})
    for row in manifest["files"]:
        tracked = REPO_ROOT / "config/acquisition" / row["file"]
        entries = records.get(f"config/acquisition/{row['file']}", [])
        # A list with current-state wording is checked through its archived original (failure mode S7).
        if entries and not ARCHIVE.is_dir():
            # The archive is private and never in Git, so a clean checkout (CI) records these checks as skipped [S55].
            skipped = {"passed": True, "skipped": "private legacy archive absent"}
            cases.append({"name": "historical_equivalence_" + row["file"], **skipped})
            cases.append({"name": "successor_keeps_every_version_" + row["file"], **skipped})
            continue
        source = REPO_ROOT / entries[0]["archive_path"] if entries else tracked
        if entries:
            predecessor_bytes(entries[0])
        normalized = read_code_versions(source)
        normalized["versions"] = normalized["versions"][: row["versions"]]
        cases.append({"name": "historical_equivalence_" + row["file"], "passed": canonical_hash(normalized) == row["original_normalized_sha256"]})
        if entries:
            kept = [{k: v for k, v in version.items() if k != "reason"} for version in read_code_versions(source)["versions"]]
            now = [{k: v for k, v in version.items() if k != "reason"} for version in read_code_versions(tracked)["versions"]]
            cases.append({"name": "successor_keeps_every_version_" + row["file"], "passed": now[: len(kept)] == kept})
    with tempfile.TemporaryDirectory(prefix="code_fingerprints_") as directory:
        path = Path(directory) / "versions.json"
        for source in sorted((REPO_ROOT / "config/acquisition").glob("*_code_versions.json")):
            original = read_code_versions(source)
            typed = copy.deepcopy(original)
            for version in typed["versions"]:
                version["code_sha256"] = [{"file": key, "sha256": value} for key, value in sorted(version["code_sha256"].items())]
            path.write_text(json.dumps(typed))
            cases.append({"name": source.name, "passed": read_code_versions(path) == original})
        baseline = typed
        for name in ["duplicate", "invalid_digest", "invalid_path", "unknown_field"]:
            changed = copy.deepcopy(baseline)
            rows = changed["versions"][0]["code_sha256"]
            if name == "duplicate":
                rows.append(dict(rows[0]))
            elif name == "invalid_digest":
                rows[0]["sha256"] = "untrusted"
            elif name == "invalid_path":
                rows[0]["file"] = "../private.py"
            else:
                rows[0]["extra"] = True
            path.write_text(json.dumps(changed))
            try:
                read_code_versions(path)
                passed = False
            except RegistryError:
                passed = True
            cases.append({"name": name, "passed": passed})
    return cases


CONTRACTS = [bls_api_contract, census_acs_api_contract, census_acs_detailed_contract, hud_api_contract]
SYNTHETIC = {
    "pkg/__init__.py": "",
    "pkg/sub/__init__.py": "",
    "pkg/entry.py": "import pkg.sub.deep\nfrom . import sibling\nfrom .sub import named\nfrom pkg import helper\nimport json\n",
    "pkg/sibling.py": "from ..other import thing\n",
    "pkg/sub/deep.py": "",
    "pkg/sub/named.py": "",
    "pkg/helper.py": "def late():\n    from pkg.lazy import value\n    return value\n",
    "pkg/lazy.py": "mod = __import__('pkg.literal')\n",
    "pkg/literal.py": "COMMAND = ['python', '-m', 'pkg.launched']\nTEMPLATE = 'pkg/data/query.sql'\n",
    "pkg/launched.py": "import pkg.child\n",
    "pkg/child.py": "",
    "pkg/data/query.sql": "select 1\n",
    "other.py": "thing = 1\nimport entry_unused_attr\n",
    "pkg/unrelated.py": "import pkg.child\n",
    "pkg/cycle_a.py": "import pkg.cycle_b\n",
    "pkg/cycle_b.py": "import pkg.cycle_a\n",
    "pkg/computed.py": "import importlib\nname = 'pkg.' + 'x'\nimportlib.import_module(name)\n",
    "pkg/execs.py": "exec('x = 1')\n",
    "pkg/broken.py": "def (:\n",
    "pkg/attr.py": "from pkg.helper import late\n",
    "hidden.py": "",
    "pkg/hidden.py": "",
    "wildpkg/__init__.py": "__all__ = ['hidden']\n",
    "wildpkg/hidden.py": "",
    "wildpkg/other.py": "",
    "pkg/alias_ok.py": "__import__('pkg.child')\n__import__('pkg.sub.named', globals(), locals(), [], 0)\n",
    "pkg/rel_import.py": "import importlib\nimportlib.import_module('.hidden', 'pkg')\n",
    "pkg/rel_dunder.py": "__import__('hidden', globals(), locals(), ['VALUE'], 1)\n",
    "pkg/fromlist.py": "__import__('wildpkg', fromlist=['hidden'])\n",
    "pkg/wild.py": "from wildpkg import *\n",
    "pkg/wild_module.py": "from pkg.child import *\n",
    "pkg/dual.py": "import pkg.lazy\n",
    "pkg/dual/__init__.py": "import pkg.child\n",
    "pkg/uses_dual.py": "import pkg.dual\nfrom pkg import dual\n",
    "pkg/alias_import.py": "from importlib import import_module as load\nload('.'.join(['pkg', 'hidden']))\n",
    "pkg/alias_module.py": "import importlib as il\nil.import_module('.'.join(['pkg', 'hidden']))\n",
    "pkg/alias_dunder.py": "from builtins import __import__ as imp\nimp('.'.join(['pkg', 'hidden']))\n",
    "pkg/alias_exec.py": "import builtins\nbuiltins.exec('x = 1')\n",
    "pkg/alias_exec_as.py": "import builtins as b\nb.eval('1')\n",
    "pkg/alias_exec_from.py": "from builtins import compile as build\nbuild('1', 'f', 'eval')\n",
    "pkg/alias_runpy.py": "from runpy import run_module\nrun_module('pkg.hidden')\n",
    "pkg/alias_runpy_as.py": "import runpy as r\nr.run_path('x.py')\n",
    "pkg/value_function.py": "run = exec\nrun('x = 1')\n",
    "pkg/value_import.py": "import importlib\nloader = importlib.import_module\nloader('pkg.hidden')\n",
    "pkg/value_module.py": "import builtins\ngetattr(builtins, 'ex' + 'ec')('x = 1')\n",
    "pkg/rel_no_package.py": "import importlib\nimportlib.import_module('.hidden')\n",
    "pkg/rel_computed_package.py": "import importlib\nimportlib.import_module('.hidden', __package__)\n",
    "pkg/rel_foreign_globals.py": "__import__('hidden', {}, {}, ['VALUE'], 1)\n",
    "pkg/rel_other_globals.py": "__import__('hidden', dict(), {}, ['VALUE'], 1)\n",
    "pkg/rel_computed_level.py": "level = 1\n__import__('hidden', globals(), locals(), ['VALUE'], level)\n",
    "pkg/fromlist_computed.py": "names = ['hidden']\n__import__('wildpkg', fromlist=names)\n",
    "pkg/starred_tail.py": "rest = (globals(), locals(), [], 1)\n__import__('hidden', *rest)\n",
    "pkg/starred.py": "import importlib\nnames = ['pkg.hidden']\nimportlib.import_module(*names)\n",
    "pkg/native.so": "binary\n",
    "pkg/uses_native.py": "import pkg.native\n",
    "pkg/runs_shell.py": "COMMAND = ['bash', 'pkg/helper.sh']\n",
    "pkg/helper.sh": "python -m pkg.hidden_target\n",
    "pkg/hidden_target.py": "",
    "pkg/runs_tool.py": "COMMAND = ['pkg/tool_bit']\n",
    "pkg/tool_bit": "plain text with the executable bit\n",
    "pkg/runs_shebang.py": "COMMAND = ['pkg/tool_shebang']\n",
    "pkg/tool_shebang": "#!/usr/bin/env python3\nimport pkg.hidden_target\n",
    "pkg/wild_importlib.py": "from importlib import *\nimport_module('.'.join(['pkg', 'hidden']))\n",
    "pkg/wild_runpy.py": "from runpy import *\nrun_module('.'.join(['pkg', 'hidden']))\n",
    "pkg/wild_outside.py": "from os.path import *\n",
    "pkg/import_pkgutil.py": "import pkgutil\n",
    "pkg/import_imp.py": "import imp\n",
    "pkg/import_builtins.py": "import builtins\n",
    "pkg/import_submodule.py": "import importlib.util\n",
    "pkg/import_from_submodule.py": "from importlib.machinery import SourceFileLoader\n",
    "pkg/import_name_from.py": "from somewhere import importlib\n",
    "pkg/reexport_loader.py": "from pkg.sub.loader import load\nload('.'.join(['pkg', 'hidden']))\n",
    "pkg/sub/loader.py": "from importlib import import_module as load\n",
    "pkg/scoped_builtin.py": "def unused():\n    from json import loads as __import__\n__import__('.'.join(['pkg', 'hidden']))\n",
    "pkg/name_store.py": "exec = 1\n",
    "pkg/name_parameter.py": "def run(compile):\n    return 1\n",
    "pkg/name_def.py": "def eval():\n    pass\n",
    "pkg/name_builtins.py": "print(__builtins__)\n",
    "pkg/alias_as_eval.py": "from json import loads as eval\n",
    "pkg/name_attribute.py": "import json\njson.__import__('pkg.hidden')\n",
    "pkg/dunder_forbidden.py": "__import__('importlib')\n",
    "pkg/dunder_level.py": "__import__('hidden', globals(), locals(), ['VALUE'], 1)\n",
    "pkg/dunder_empty.py": "__import__()\n",
    "pkg/dotted_fromlist.py": "__import__('dotpkg', fromlist=['sub.hidden'])\n",
    "pkg/dotted_star.py": "__import__('dotpkg', fromlist=['sub.*'])\n",
    "pkg/dotted_all.py": "__import__('dotall', fromlist=['*'])\n",
    "pkg/dotted_all_import.py": "from dotall import *\n",
    "dotpkg/__init__.py": "",
    "dotpkg/sub/__init__.py": "",
    "dotpkg/sub/hidden.py": "VALUE = 7\n",
    "dotall/__init__.py": "__all__ = ['sub.hidden']\n",
    "dotall/sub/__init__.py": "",
    "dotall/sub/hidden.py": "VALUE = 7\n",
    "pkg/runs_dot.py": "COMMAND = ['python', './pkg/tool_shebang']\n",
    "pkg/runs_inner_dot.py": "COMMAND = ['pkg/./tool_shebang']\n",
    "pkg/runs_up.py": "COMMAND = ['pkg/sub/../tool_shebang']\n",
    "pkg/runs_slashes.py": "COMMAND = ['pkg//tool_shebang']\n",
    "pkg/runs_trailing.py": "COMMAND = ['pkg/tool_shebang/']\n",
    "pkg/runs_from_folder.py": "COMMAND = ['../pkg/tool_shebang']\n",
    "pkg/runs_backslash.py": "COMMAND = ['pkg\\\\tool_shebang']\n",
    "pkg/runs_bit_dot.py": "COMMAND = ['./pkg/tool_bit']\n",
    "pkg/runs_shell_dot.py": "COMMAND = ['bash', './pkg//helper.sh']\n",
    "pkg/runs_os_join.py": "import os\nCOMMAND = [os.path.join('pkg', 'tool_shebang')]\n",
    "pkg/runs_os_join_dot.py": "import os\nCOMMAND = [os.path.join('.', 'pkg', 'tool_shebang')]\n",
    "pkg/runs_path_div.py": "from pathlib import Path\nCOMMAND = [str(Path('pkg') / 'tool_shebang')]\n",
    "pkg/runs_path_args.py": "from pathlib import Path\nCOMMAND = [str(Path('.', 'pkg', 'tool_shebang'))]\n",
    "pkg/runs_joinpath.py": "from pathlib import Path\nCOMMAND = [str(Path('pkg').joinpath('tool_shebang'))]\n",
    "pkg/runs_unknown_base.py": "from pathlib import Path\nCOMMAND = [str(Path(__file__).parent.parent / 'pkg' / 'tool_shebang')]\n",
    "pkg/runs_data_dot.py": "TEMPLATE = './pkg//data/./query.sql'\n",
    "pkg/runs_data_join.py": "import os\nTEMPLATE = os.path.join('pkg', 'data', 'query.sql')\n",
    "pkg/runs_outside_root.py": "COMMAND = ['../outside_tool']\n",
    "tools/outside.sh": "python -m pkg.hidden_target\n",
    "pkg/runs_outside.py": "COMMAND = ['tools/outside.sh']\n",
}
EXECUTABLE_BIT = ["pkg/tool_bit"]
EXPECTED = {
    "pkg.entry": [
        "other.py",
        "pkg/__init__.py",
        "pkg/child.py",
        "pkg/data/query.sql",
        "pkg/entry.py",
        "pkg/helper.py",
        "pkg/lazy.py",
        "pkg/launched.py",
        "pkg/literal.py",
        "pkg/sibling.py",
        "pkg/sub/__init__.py",
        "pkg/sub/deep.py",
        "pkg/sub/named.py",
    ],
    "pkg.cycle_a": ["pkg/__init__.py", "pkg/cycle_a.py", "pkg/cycle_b.py"],
    "pkg.attr": ["pkg/__init__.py", "pkg/attr.py", "pkg/child.py", "pkg/data/query.sql", "pkg/helper.py", "pkg/lazy.py", "pkg/launched.py", "pkg/literal.py"],
    # Aliases still bind literal imports, by identity.
    "pkg.alias_ok": ["pkg/__init__.py", "pkg/alias_ok.py", "pkg/child.py", "pkg/sub/__init__.py", "pkg/sub/named.py"],
    # A dotted fromlist entry binds every component of the path.
    "pkg.dotted_fromlist": ["dotpkg/__init__.py", "dotpkg/sub/__init__.py", "dotpkg/sub/hidden.py", "pkg/__init__.py", "pkg/dotted_fromlist.py"],
    "pkg.dotted_star": ["dotpkg/__init__.py", "dotpkg/sub/__init__.py", "dotpkg/sub/hidden.py", "pkg/__init__.py", "pkg/dotted_star.py"],
    # A wildcard of a package binds modules at any depth, because __all__ may hold a dotted name.
    "pkg.dotted_all": ["dotall/__init__.py", "dotall/sub/__init__.py", "dotall/sub/hidden.py", "pkg/__init__.py", "pkg/dotted_all.py"],
    "pkg.dotted_all_import": ["dotall/__init__.py", "dotall/sub/__init__.py", "dotall/sub/hidden.py", "pkg/__init__.py", "pkg/dotted_all_import.py"],
    # A named code file is normalized before it is bound.
    "pkg.runs_data_dot": ["pkg/__init__.py", "pkg/data/query.sql", "pkg/runs_data_dot.py"],
    "pkg.runs_data_join": ["pkg/__init__.py", "pkg/data/query.sql", "pkg/runs_data_join.py"],
    # A fromlist adds only its named submodule.
    "pkg.fromlist": ["pkg/__init__.py", "pkg/fromlist.py", "wildpkg/__init__.py", "wildpkg/hidden.py"],
    # A wildcard import of a package adds every submodule, because __all__ may name any of them.
    "pkg.wild": ["pkg/__init__.py", "pkg/wild.py", "wildpkg/__init__.py", "wildpkg/hidden.py", "wildpkg/other.py"],
    "pkg.wild_module": ["pkg/__init__.py", "pkg/child.py", "pkg/wild_module.py"],
    # With both pkg/dual.py and pkg/dual/__init__.py, Python loads the package.
    "pkg.uses_dual": ["pkg/__init__.py", "pkg/child.py", "pkg/dual/__init__.py", "pkg/uses_dual.py"],
}
REFUSED = [
    "pkg.rel_import",
    "pkg.rel_dunder",
    "pkg.wild_importlib",
    "pkg.wild_runpy",
    "pkg.wild_outside",
    "pkg.import_pkgutil",
    "pkg.import_imp",
    "pkg.import_builtins",
    "pkg.import_submodule",
    "pkg.import_from_submodule",
    "pkg.import_name_from",
    "pkg.reexport_loader",
    "pkg.scoped_builtin",
    "pkg.name_store",
    "pkg.name_parameter",
    "pkg.name_def",
    "pkg.name_builtins",
    "pkg.name_attribute",
    "pkg.alias_as_eval",
    "pkg.dunder_forbidden",
    "pkg.dunder_level",
    "pkg.dunder_empty",
    "pkg.runs_dot",
    "pkg.runs_inner_dot",
    "pkg.runs_up",
    "pkg.runs_slashes",
    "pkg.runs_trailing",
    "pkg.runs_from_folder",
    "pkg.runs_backslash",
    "pkg.runs_bit_dot",
    "pkg.runs_shell_dot",
    "pkg.runs_os_join",
    "pkg.runs_os_join_dot",
    "pkg.runs_path_div",
    "pkg.runs_path_args",
    "pkg.runs_joinpath",
    "pkg.runs_unknown_base",
    "pkg.runs_outside_root",
    "pkg.runs_absolute",
    "pkg.computed",
    "pkg.execs",
    "pkg.broken",
    "pkg.missing",
    "pkg.alias_import",
    "pkg.alias_module",
    "pkg.alias_dunder",
    "pkg.alias_exec",
    "pkg.alias_exec_as",
    "pkg.alias_exec_from",
    "pkg.alias_runpy",
    "pkg.alias_runpy_as",
    "pkg.value_function",
    "pkg.value_import",
    "pkg.value_module",
    "pkg.rel_no_package",
    "pkg.rel_computed_package",
    "pkg.rel_foreign_globals",
    "pkg.rel_other_globals",
    "pkg.rel_computed_level",
    "pkg.fromlist_computed",
    "pkg.starred",
    "pkg.starred_tail",
    "pkg.uses_native",
    "pkg.runs_shell",
    "pkg.runs_tool",
    "pkg.runs_shebang",
    "pkg.runs_outside",
]


def refused(action: Callable[[], object]) -> bool:
    """True when the action raises the registry validation error."""
    try:
        action()
    except RegistryError:
        return True
    return False


def synthetic_cases(root: Path) -> list[dict]:
    """Closure rules on a small tree that has one example of every failure mode."""
    for name, text in SYNTHETIC.items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(text)
    for name in EXECUTABLE_BIT:
        (root / name).chmod(0o755)
    (root / "pkg/runs_absolute.py").write_text(f"COMMAND = [{str(root / 'pkg/tool_shebang')!r}]\n")
    (root.parent / "outside_tool").write_text("not part of the repository\n")
    cases = [{"name": f"closure_{entry}", "passed": code_closure.closure(entry, root, ("pkg",)) == sorted(files)} for entry, files in EXPECTED.items()]
    cases += [{"name": f"refuses_{module}", "passed": refused(partial(code_closure.closure, module, root, ("pkg",)))} for module in REFUSED]
    return cases


def contract_cases(tree: Path) -> list[dict]:
    """Each real collector: an unrelated edit keeps the hash, an edit to anything it runs changes it, an unreviewed version is refused."""
    cases = []
    for contract in CONTRACTS:
        entry = contract.CODE_ENTRY
        name = entry.rsplit(".", 1)[1]
        with patch.object(code_closure, "REPO_ROOT", tree):
            files = code_closure.closure(entry, tree)
            entry_file = entry.replace(".", "/") + ".py"
            imported = code_closure.references(entry_file, tree, code_closure.CODE_DIRECTORIES)
            direct = sorted(f for f in imported if f.endswith(".py") and f != entry_file)
            transitive = sorted(f for f in files if f.endswith(".py") and f not in imported and f != entry_file)
            outside = [
                p.relative_to(tree).as_posix() for p in sorted((tree / "scripts/acquisition").glob("*.py")) if p.relative_to(tree).as_posix() not in files
            ]
            cases.append(
                {
                    "name": f"{name}_closure_is_narrower_than_the_directory",
                    "passed": bool(outside) and len(files) < len(list((tree / "scripts/acquisition").glob("*.py"))),
                }
            )
            baseline = contract.collector_code_hashes()
            cases.append({"name": f"{name}_hash_binds_exactly_the_closure", "passed": sorted(baseline) == files})
            versions = tree / f"{name}_versions.json"
            versions.write_text('{"versions": [{"code_sha256": ' + json.dumps(baseline) + "}]}")
            with patch.object(contract, "VERSIONS_PATH", versions):
                cases.append({"name": f"{name}_reviewed_version_accepted", "passed": contract.require_code() == baseline})
                for label, target in [("unrelated", outside[0]), ("direct", direct[0]), ("transitive", transitive[0]), ("entry", entry_file)]:
                    original = (tree / target).read_bytes()
                    (tree / target).write_bytes(original + b"\n# edit\n")
                    try:
                        changed = contract.collector_code_hashes() != baseline
                        if label == "unrelated":
                            cases.append(
                                {"name": f"{name}_unrelated_edit_keeps_hash_and_review", "passed": not changed and contract.require_code() == baseline}
                            )
                        else:
                            cases.append({"name": f"{name}_{label}_edit_changes_hash", "passed": changed})
                            cases.append({"name": f"{name}_{label}_edit_is_refused_until_reviewed", "passed": refused(contract.require_code)})
                    finally:
                        (tree / target).write_bytes(original)
    return cases


def hud_xlsx_cases(tree: Path) -> list[dict]:
    """The workbook collector shares the HUD API versions file but binds its own closure, which includes the HUD API closure."""
    from scripts.acquisition import store_hud_xlsx

    cases = []
    with patch.object(code_closure, "REPO_ROOT", tree):
        api_files = set(code_closure.closure(hud_api_contract.CODE_ENTRY, tree))
        xlsx_files = set(code_closure.closure(store_hud_xlsx.CODE_ENTRY, tree))
        cases.append({"name": "hud_xlsx_closure_contains_hud_api_closure_and_its_own_files", "passed": api_files < xlsx_files})
        baseline = hud_api_contract.collector_code_hashes(store_hud_xlsx.CODE_ENTRY)
        versions = tree / "hud_xlsx_versions.json"
        versions.write_text(json.dumps({"versions": [{"code_sha256": baseline}]}))
        target = tree / "scripts/acquisition/store_hud_xlsx.py"
        original = target.read_bytes()
        with patch.object(hud_api_contract, "VERSIONS_PATH", versions):
            cases.append({"name": "hud_xlsx_reviewed_version_accepted", "passed": hud_api_contract.require_code(entry=store_hud_xlsx.CODE_ENTRY) == baseline})
            target.write_bytes(original + b"\n# edit\n")
            try:
                cases.append(
                    {"name": "hud_xlsx_own_edit_is_refused", "passed": refused(lambda: hud_api_contract.require_code(entry=store_hud_xlsx.CODE_ENTRY))}
                )
            finally:
                target.write_bytes(original)
    return cases


def main() -> None:
    """Write an immutable local report; no acquisition or cloud calls."""
    from scripts.acquisition.s3_store import encoded_json, write_once

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    cases = exercise()
    with tempfile.TemporaryDirectory(prefix="code_closure_") as directory:
        synthetic = Path(directory) / "synthetic"
        synthetic.mkdir()
        copy_root = Path(directory) / "repo"
        shutil.copytree(REPO_ROOT / "scripts", copy_root / "scripts", ignore=shutil.ignore_patterns("__pycache__", "tests"))
        cases += synthetic_cases(synthetic) + contract_cases(copy_root) + hud_xlsx_cases(copy_root)
    write_once(args.output, encoded_json({"cases": cases, "passed": all(c["passed"] for c in cases)}))
    require(all(c["passed"] for c in cases), "Fingerprint representation E2E failed")


if __name__ == "__main__":
    main()
