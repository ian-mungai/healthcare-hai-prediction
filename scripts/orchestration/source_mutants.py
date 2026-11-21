"""Run cases against a source file with one change in memory, to prove that a case notices the behaviour that the change removes.

``mutated`` makes Python read ``path`` with ``old`` replaced by ``new`` for every import inside the ``with`` block (the cases import the DAG runtime and the DAG
files afresh, so each import sees the change) and never writes a file. The text to replace must occur exactly once, so a mutant that stops matching its source
fails at once instead of passing as an unchanged run. The saved bytecode of the file is refused while the change is on, so the changed text is compiled, and
no bytecode is saved while it is on, so the compiled change is never read as the file.
"""

from __future__ import annotations

import contextlib
import importlib
import io
import sys
from collections.abc import Callable, Iterator
from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Any
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]


@contextlib.contextmanager
def mutated(path: str, old: str, new: str) -> Iterator[None]:
    """Every read of the file ``path`` (relative to the repository) sees its text with the one occurrence of ``old`` replaced by ``new``: an import, a run by
    ``runpy`` (the DAG files). A module that the process holds is reloaded in place and again after; one that a package holds as an attribute is taken off the
    package for the block (the cases import the DAG runtime that way) and put back after."""
    target = ROOT / path
    text = target.read_text()
    if text.count(old) != 1:
        raise ValueError(f"{path} holds {text.count(old)} copies of the text to change, not one: {old[:80]!r}")
    changed = text.replace(old, new)
    original_data: Callable[..., bytes] = SourceFileLoader.get_data
    original_open: Callable[[Any], Any] = io.open_code

    def get_data(self: SourceFileLoader, name: str) -> bytes:
        if Path(name).resolve() == target.resolve():
            return changed.encode()
        if name.endswith(".pyc") and Path(name).name.split(".")[0] == target.stem:
            raise OSError("the saved bytecode is not used while a mutant is on")
        data: bytes = original_data(self, name)
        return data

    def open_code(name: Any) -> Any:
        if Path(str(name)).resolve() == target.resolve():
            return io.BytesIO(changed.encode())
        return original_open(name)

    name = path.removesuffix(".py").replace("/", ".")
    package, _, child = name.rpartition(".")
    module = sys.modules.get(name)
    parent = sys.modules.get(package) if package else None
    stale = getattr(parent, child, None) if parent is not None and module is None else None
    try:
        with (
            mock.patch.object(SourceFileLoader, "get_data", get_data),
            mock.patch.object(io, "open_code", open_code),
            mock.patch.object(sys, "dont_write_bytecode", True),
        ):
            if module is not None:
                importlib.reload(module)
            elif stale is not None:
                delattr(parent, child)  # ``from package import module`` reads this attribute before it imports anything: a stale one would hide the change
            yield
    finally:
        if module is not None:
            importlib.reload(module)
        elif stale is not None:
            sys.modules.pop(name, None)
            setattr(parent, child, stale)


def failing(cases: dict[str, Callable[[], Any]]) -> list[str]:
    """The names of the cases that fail (any exception) when run now, in order."""
    broken = []
    for name, case in cases.items():
        try:
            case()
        except BaseException:
            broken.append(name)
    return broken
