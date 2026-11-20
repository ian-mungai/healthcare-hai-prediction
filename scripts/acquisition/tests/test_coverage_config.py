"""The acquisition coverage configuration never credits execution of one file to another file.

A test that unpacks the code archive runs a copy of ``scripts/acquisition`` under a pytest folder, and the child process records it as its own files. Those rows
are not the tree's source: they double the statement count and hide the tree's coverage. The configuration leaves such copies out of the measure. It must not
merge their lines into the tree's file either, because a path alone cannot show that a copy holds the same source: another checkout, a worktree or an archive of
another revision with the same relative path and line numbers would then turn a failing result into a pass. The cases build the data without a tracer (the suite
may run under coverage itself) and report it with the real configuration and its 90 percent gate."""

from __future__ import annotations

import io
import json
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from coverage import Coverage, CoverageData

from tests.support import check

CONFIG = Path(__file__).resolve().parents[3] / "config/acquisition/coverage.ini"
RELATIVE = Path("scripts/acquisition/sample.py")
SOURCE = """def left():
    a = 1
    b = 2
    c = 3
    return a + b + c
def right():
    return 7
def missing():
    a = 9
    return a
"""
OTHER_SOURCE = SOURCE.replace("a = 9", "a = 900").replace("return a\n", "return a + 42\n")
LEFT = {1, 2, 3, 4, 5, 6, 8}
RIGHT = {1, 6, 7, 8}
MISSING = {1, 6, 8, 9, 10}


@pytest.fixture
def work() -> Iterator[Path]:
    """A folder outside pytest's own temporary tree: the configuration leaves out the acquisition files under a ``pytest-of-*`` folder, as the copies are."""
    with tempfile.TemporaryDirectory(prefix="coverage_config_") as folder:
        yield Path(folder).resolve()


def write_source(root: Path, body: str) -> Path:
    path = root / RELATIVE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


def write_data(folder: Path, name: str, path: Path, lines: set[int]) -> str:
    data = CoverageData(basename=str(folder / f"{name}.cov"))
    data.add_lines({str(path): lines})
    data.write()
    return data.data_filename()


def report(work: Path, monkeypatch: pytest.MonkeyPatch, extra: tuple[str, str, str]) -> tuple[dict[str, set[int]], str, float]:
    """Combine the data of the tree's own run and of one other copy, and report it with the real configuration from the tree's folder."""
    canonical = work / "canonical"
    own = write_source(canonical, SOURCE)
    root, body, calls = extra
    other = write_source(work / root, body)
    inputs = [write_data(work, "local", own, LEFT), write_data(work, "other", other, {"RIGHT": RIGHT, "MISSING": MISSING}[calls])]
    monkeypatch.chdir(canonical)
    cov = Coverage(config_file=str(CONFIG), data_file=str(work / "result.cov"))
    cov.combine(inputs, strict=True, keep=True)
    cov.save()
    total = cov.report(file=io.StringIO())
    target = work / "report.json"
    cov.json_report(outfile=str(target))
    listed = json.loads(target.read_text(encoding="utf-8"))["files"]
    return {name: set(entry["executed_lines"]) for name, entry in listed.items()}, str(own), total


FOREIGN = [
    pytest.param(("unrelated/.worktrees/worker", OTHER_SOURCE, "MISSING"), id="another worktree with other source"),
    pytest.param(("unrelated/healthcare-hai-prediction", OTHER_SOURCE, "MISSING"), id="another checkout with other source"),
    pytest.param(("pytest-of-another/pytest-99/other0/root", OTHER_SOURCE, "MISSING"), id="an archive of other source"),
    pytest.param(("unrelated/other-repo", OTHER_SOURCE, "MISSING"), id="an unrelated folder with other source"),
    pytest.param(("pytest-of-review/pytest-4/archive0/root", SOURCE, "RIGHT"), id="an identical archive"),
]


@pytest.mark.parametrize("extra", FOREIGN)
def test_no_other_copy_credits_lines_to_the_tree_file(work: Path, monkeypatch: pytest.MonkeyPatch, extra: tuple[str, str, str]) -> None:
    files, own, total = report(work, monkeypatch, extra)
    held = files.get(str(RELATIVE), set())
    check(held == LEFT, f"the tree file holds only the lines its own run executed: {sorted(held)}")
    check(total < 90, f"the gate still fails on the tree's own 70 percent: {total}")


def test_archive_copies_are_not_listed_beside_the_tree(work: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    files, _, _ = report(work, monkeypatch, ("pytest-of-review/pytest-4/archive0/root", SOURCE, "RIGHT"))
    listed = [name for name in files if "pytest-of-" in name]
    check(not listed, f"a pytest archive copy is not measured source: {listed}")
