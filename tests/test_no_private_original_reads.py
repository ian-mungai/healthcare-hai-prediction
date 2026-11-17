"""No live code reads a private original folder.

Bronze pseudonymizes, so every capture stores the whole file under raw/ and no code keeps an owner-only original. The only place the folder name may
still appear is an expression that asserts the folder is absent.
"""

from __future__ import annotations

import ast
from pathlib import Path

from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
FOLDERS = ("scripts", "orchestration", "dags", "tests", "infra", ".github")
SUFFIXES = {".py", ".sh", ".tf", ".hcl", ".yml", ".yaml"}
NAME = "private_" + "original"


def folded_all(nodes: list[ast.expr], separator: str) -> str | None:
    """The literal pieces joined by the separator, or None when any piece is not a literal."""
    texts = []
    for node in nodes:
        text = folded(node)
        if text is None:
            return None
        texts.append(text)
    return separator.join(texts)


def folded(node: ast.AST) -> str | None:
    """The text an expression always builds from literals alone (a string, a joined string, a sum or join of strings), or None."""
    match node:
        case ast.Constant(value=str() as text):
            return text
        case ast.JoinedStr(values=parts):
            return folded_all(parts, "")
        case ast.FormattedValue(value=inner, format_spec=None, conversion=-1):
            return folded(inner)
        case ast.BinOp(op=ast.Add(), left=left, right=right):
            first, second = folded(left), folded(right)
            return None if first is None or second is None else first + second
        case ast.Call(func=ast.Attribute(value=ast.Constant(value=str() as separator), attr="join"), args=[ast.List(elts=items) | ast.Tuple(elts=items)]):
            return folded_all(items, separator)
    return None


def names_folder(node: ast.AST) -> bool:
    """True when the expression is a string or a part of a string that forms the folder name."""
    text = folded(node)
    return text is not None and NAME in text


def plain_path(node: ast.AST) -> bool:
    """True for a path built only from names, attributes, literals and the division operator: nothing in it reads a file."""
    match node:
        case ast.Name() | ast.Constant():
            return True
        case ast.Attribute(value=inner):
            return plain_path(inner)
        case ast.BinOp(op=ast.Div(), left=left, right=right):
            return plain_path(left) and plain_path(right)
    return False


def asserts_absence(node: ast.AST) -> bool:
    """True for ``not <path>.exists()`` and ``not any(<name> in <path>.parts for ...)``, the two ways a test says the folder was not made."""
    if not (isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not) and isinstance(node.operand, ast.Call)):
        return False
    call = node.operand
    match call:
        case ast.Call(func=ast.Attribute(value=path, attr="exists"), args=[], keywords=[]):
            return plain_path(path)
        case ast.Call(func=ast.Name(id="any"), args=[ast.GeneratorExp(elt=ast.Compare(ops=[ast.In()], left=left, comparators=[ast.Attribute(attr="parts")]))]):
            return names_folder(left)
    return False


def violations_in_source(source: str) -> list[int]:
    """The line numbers of every expression that forms the folder name outside an expression that asserts the folder is absent."""
    tree = ast.parse(source)
    allowed: set[int] = set()
    for node in ast.walk(tree):
        if asserts_absence(node):
            allowed.update(id(inner) for inner in ast.walk(node))
    found: set[int] = set()
    for node in ast.walk(tree):
        if id(node) not in allowed and isinstance(node, (ast.Constant, ast.JoinedStr, ast.BinOp, ast.Call)) and names_folder(node):
            found.add(node.lineno)
    return sorted(found)


def violations_in_text(source: str) -> list[int]:
    """The line numbers of a script or a Terraform file that name the folder at all."""
    return [number for number, line in enumerate(source.splitlines(), start=1) if NAME in line]


def violations_in_file(path: Path) -> list[int]:
    text = path.read_text(encoding="utf-8")
    return violations_in_source(text) if path.suffix == ".py" else violations_in_text(text)


def code_files() -> list[Path]:
    return sorted(
        path
        for folder in FOLDERS
        for path in (ROOT / folder).rglob("*")
        if path.suffix in SUFFIXES and path.is_file() and ".terraform" not in path.parts and path != Path(__file__).resolve()
    )


def test_no_live_code_names_a_private_original_folder_except_to_assert_it_is_absent() -> None:
    found = [f"{path.relative_to(ROOT)}:{number}" for path in code_files() for number in violations_in_file(path)]
    check(not found, f"these lines still use a private original folder: {found}")


def test_the_scan_sees_the_code_it_guards() -> None:
    names = {path.name for path in code_files()}
    check({"stage_publisher.py", "stored_mirror.py", "collector_state.py", "model_metadata.py"} <= names, "the scan must reach the guarded modules")


PLANTED_READERS = {
    "a negated reader": 'def read(root):\n    return not (root / "private_original" / "owners.csv").read_bytes()\n',
    "a joined literal": 'def read(root):\n    return (root / ("private_" + "original") / "owners.csv").read_bytes()\n',
    "a joined literal split three ways": 'def read(root):\n    return (root / ("private_" + "orig" + "inal")).read_bytes()\n',
    "an f-string": "def read(root):\n    return (root / f\"{'private_'}original\").read_bytes()\n",
    "a joined list": 'def read(root):\n    return (root / "".join(["private_", "original"])).read_bytes()\n',
    "a read in an absence check": 'def read(root):\n    return not (root / "private_original").read_text().exists()\n',
    "a read beside an absence check": 'def read(root):\n    return not (root / "x").exists() and (root / "private_original").read_bytes()\n',
    "a test of membership that reads": 'def read(root):\n    return not any("private_original" in p.read_text() for p in root.rglob("*"))\n',
    "a plain reader": 'def read(root):\n    return (root / "private_original" / "owners.csv").read_bytes()\n',
}
PLANTED_ABSENCE_CHECKS = {
    "a folder that was not made": 'def check(run):\n    return not (run / "private_original").exists() and not (run / "derived").exists()\n',
    "no folder in a state tree": 'def check(run):\n    return not any("private_original" in path.parts for path in run.state_root.rglob("*"))\n',
}


def test_the_guard_flags_every_planted_reader() -> None:
    missed = [name for name, source in PLANTED_READERS.items() if not violations_in_source(source)]
    check(not missed, f"the guard passes these planted readers: {missed}")


def test_the_guard_accepts_the_two_absence_forms() -> None:
    wrong = [name for name, source in PLANTED_ABSENCE_CHECKS.items() if violations_in_source(source)]
    check(not wrong, f"the guard flags these absence checks: {wrong}")


def test_the_cli_descriptions_no_longer_promise_version_1_verification() -> None:
    from scripts.acquisition import hcai_util_contract, run_cms_owners_e2e, run_hcai_util_e2e, run_onc_mu_e2e, stage_publisher

    claims = {
        "run_cms_owners_e2e": run_cms_owners_e2e.__doc__ or "",
        "run_onc_mu_e2e": run_onc_mu_e2e.__doc__ or "",
        "run_hcai_util_e2e": run_hcai_util_e2e.__doc__ or "",
    }
    stale = [name for name, text in claims.items() if "version 1" in text or "keep verifying" in text]
    check(not stale, f"these descriptions still promise version 1 verification: {stale}")
    check("refuses an original" not in (stage_publisher.__doc__ or ""), "the stage publisher description still says it refuses an original")
    check("local original" not in (hcai_util_contract.verify_capture.__doc__ or ""), "the HCAI verifier description still names a local original")


def test_the_hcai_contract_keeps_no_sheet_file_name_helper() -> None:
    from scripts.acquisition import hcai_util_contract

    check(not hasattr(hcai_util_contract, "slug"), "slug served only the removed sheet derivation")
