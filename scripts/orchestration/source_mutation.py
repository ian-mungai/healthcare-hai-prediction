"""Run a case against a mutated copy of one function: the source mutation helpers of the engine cases."""

from __future__ import annotations

import inspect
import json
import sys
import tempfile
import textwrap
import types
from collections.abc import Callable
from contextlib import contextmanager
from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Any

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]


def require(value: bool, cause: str) -> None:
    if not value:
        raise RuntimeError(cause)


class MutantLintError(Exception):
    """Stop mutation preparation when a generated copy fails unchanged lint."""


def lint_mutant(paths: tuple[Path, ...], base: Path, label: str) -> None:
    require(all(path.is_relative_to(base) for path in paths), "mutation_copy_location:" + label)
    commands = (["check", "--fix"], ["format"], ["check", "--output-format", "json"])
    checks: list[dict[str, Any]] = []
    for arguments in commands:
        command = ["-m", "ruff", *arguments, "--config", str(ROOT / "pyproject.toml"), *(str(path) for path in paths)]
        checked = run_command(sys.executable, command, cwd=ROOT, timeout=15, check=False)
        checks.append({"command": command, "exit_code": checked.returncode, "stdout": checked.stdout, "stderr": checked.stderr})
    if checks[0]["exit_code"] not in (0, 1) or any(row["exit_code"] != 0 for row in checks[1:]):
        record = {"label": label, "copies": [str(path) for path in paths], "checks": checks}
        (base / "blocked.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
        raise MutantLintError("mutation_copy_ruff:" + label + ":" + checks[-1]["stdout"].strip())


def changed_function(owner: Any, attribute: str, edits: tuple[tuple[str, str], ...], base: Path) -> Callable[..., Any]:
    original = inspect.unwrap(getattr(owner, attribute))
    original_source = inspect.getsource(original)
    source = textwrap.dedent(original_source)
    for before, after in edits:
        require(source.count(before) == 1, "mutation_anchor:" + attribute)
        source = source.replace(before, after, 1)
    module_source = Path(original.__code__.co_filename).read_text()
    require(module_source.count(original_source) == 1, "mutation_function_source:" + attribute)
    indentation = original_source[: len(original_source) - len(original_source.lstrip())]
    with tempfile.TemporaryDirectory(prefix="source_mutation_", dir=base) as temporary:
        path = Path(temporary) / "mutated_module.py"
        path.write_text(module_source.replace(original_source, textwrap.indent(source, indentation), 1))
        lint_mutant((path,), base, attribute)
        code = SourceFileLoader("hai_case_mutation", str(path)).get_code("hai_case_mutation")
        if code is None:
            raise RuntimeError("mutation_code_missing:" + attribute)
        for name in original.__qualname__.split("."):
            candidates = [value for value in code.co_consts if isinstance(value, types.CodeType) and value.co_name == name]
            require(len(candidates) == 1, "mutation_code_identity:" + attribute)
            code = candidates[0]
    function = types.FunctionType(code, original.__globals__, original.__name__, original.__defaults__, original.__closure__)
    function.__kwdefaults__ = original.__kwdefaults__
    return contextmanager(function) if hasattr(getattr(owner, attribute), "__wrapped__") else function
