"""The acquisition coverage gate counts the checkout's own acquisition files, all of them, and nothing else.

The gate (scripts/acquisition/run_checks.sh and the combining job) reports through a copy of config/acquisition/coverage.ini bound to the checkout's own
``scripts/acquisition`` folder (``shards.bound_config``, resolved when the copy is written). A test that unpacks the code archive runs a copy of the code as a
child process and the tracer records its files too; another checkout or a foreign trace can arrive as input as well. No path spelling may drop a tracked file
from the total, and no file outside the folder may raise or lower it. The cases run a real tracer (``coverage run`` in a child process, as pytest-cov's
``patch = subprocess`` does), combine the data and report it with the gate's 90 percent threshold."""

from __future__ import annotations

import os
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

from scripts.acquisition import shards
from scripts.process import run_command
from tests.support import check

CONFIG = Path(__file__).resolve().parents[3] / "config/acquisition/coverage.ini"
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
NESTED = Path("scripts/acquisition/pytest-of-project/plugin/scripts/acquisition/missed.py")
SAMPLE = Path("scripts/acquisition/sample.py")


@pytest.fixture
def work() -> Iterator[Path]:
    """A folder of its own, outside pytest's temporary tree: the roots below spell their paths as the cases need."""
    with tempfile.TemporaryDirectory(prefix="coverage_config_") as folder:
        yield Path(folder).resolve()


def child_environment() -> dict[str, str]:
    """The environment of a traced child: no coverage variable of the pytest-cov run that may be measuring this test, and no project import path."""
    return {key: value for key, value in os.environ.items() if not key.startswith("COVERAGE") and key != "PYTHONPATH"}


def write(root: Path, relative: Path, body: str) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


def trace(work: Path, root: Path, name: str, config: Path, calls: dict[Path, tuple[str, ...]]) -> Path:
    """Run the functions of the files in a child process under ``coverage run``, started in ``root``, and return the folder of its data files."""
    runner = work / f"{name}_run.py"
    runner.write_text(
        "import runpy\nfrom pathlib import Path\n\n"
        + "".join(
            f"namespace = runpy.run_path(str(Path({str(path)!r})))\n" + "".join(f"namespace[{call!r}]()\n" for call in names) for path, names in calls.items()
        ),
        encoding="utf-8",
    )
    data = work / name / "data.cov"
    data.parent.mkdir(parents=True, exist_ok=True)
    result = run_command(
        sys.executable,
        ["-m", "coverage", "run", "--rcfile", str(config), "--data-file", str(data), "--source", str(root / "scripts/acquisition"), str(runner)],
        cwd=root,
        env=child_environment(),
        timeout=120,
    )
    check(result.returncode == 0, f"the traced child ran: {result.stdout}{result.stderr}")
    return data.parent


def gate(work: Path, root: Path, traces: list[Path]) -> tuple[int, str, list[str]]:
    """Bind the configuration to ``root``, combine the traces and report with the gate's threshold: the exit status, the text and the files listed."""
    bound = shards.bound_config(CONFIG, root / "scripts/acquisition", work / "bound.ini")
    merged = work / "merged" / "data.cov"  # combine reads the files that carry the name of the data file it writes
    merged.parent.mkdir(parents=True, exist_ok=True)
    combined = run_command(
        sys.executable,
        ["-m", "coverage", "combine", "--rcfile", str(bound), "--data-file", str(merged), *map(str, traces)],
        cwd=root,
        env=child_environment(),
        timeout=120,
    )
    check(combined.returncode == 0, f"the traces combine: {combined.stdout}{combined.stderr}")
    report = run_command(
        sys.executable,
        ["-m", "coverage", "report", "--rcfile", str(bound), "--data-file", str(merged), "--show-missing"],
        cwd=root,
        env=child_environment(),
        timeout=120,
    )
    text = report.stdout + report.stderr
    listed = [line.split()[0] for line in text.splitlines() if line.endswith(".py") or ".py " in line]
    return report.returncode, text, listed


def tree(work: Path, root: Path, full: bool = False) -> Path:
    """The checkout under test: the sample file, which its own run covers fully (``full``) or at 80 percent, and a tracked file under a spelled-out path."""
    write(root, SAMPLE, SOURCE)
    write(root, NESTED, SOURCE)
    calls: dict[Path, tuple[str, ...]] = {root / SAMPLE: ("left", "right", "missing") if full else ("left", "right"), root / NESTED: ("left",)}
    return trace(work, root, "tree", CONFIG, calls)


@pytest.mark.parametrize("parts", [("pytest-of-review", "checkout"), (".worktrees", "pytest-of-review"), ("plain",)])
def test_a_tracked_file_under_a_spelled_path_is_measured_and_counted(work: Path, parts: tuple[str, ...]) -> None:
    root = work.joinpath(*parts)
    status, text, listed = gate(work, root, [tree(work, root, full=True)])
    check(any(name.endswith("pytest-of-project/plugin/scripts/acquisition/missed.py") for name in listed), f"the tracked nested file is in the report:\n{text}")
    check(status != 0, f"its three missed statements fail the gate although the sample file is covered fully:\n{text}")


def test_a_fully_covered_foreign_copy_does_not_raise_the_total(work: Path) -> None:
    root = work / "tree"
    write(root, SAMPLE, SOURCE)
    own = trace(work, root, "own", CONFIG, {root / SAMPLE: ("left", "right")})
    foreign_root = work / "other-checkout"
    write(foreign_root, SAMPLE, OTHER_SOURCE)
    foreign = trace(work, foreign_root, "foreign", CONFIG, {foreign_root / SAMPLE: ("left", "right", "missing")})
    status, text, listed = gate(work, root, [own, foreign])
    check(status != 0 and "80%" in text, f"the tree's own 80 percent decides the gate:\n{text}")
    check(len(listed) == 1, f"only the tree's file is reported: {listed}")


def test_an_archive_copy_is_neither_listed_nor_counted(work: Path) -> None:
    root = work / "tree"
    write(root, SAMPLE, SOURCE)
    own = trace(work, root, "own", CONFIG, {root / SAMPLE: ("left", "right")})
    archive = work / "pytest-of-someone" / "pytest-4" / "archive0" / "root"
    write(archive, SAMPLE, SOURCE)
    copy = trace(work, archive, "archive", CONFIG, {archive / SAMPLE: ("left", "right", "missing")})
    status, text, listed = gate(work, root, [own, copy])
    check(status != 0 and "80%" in text, f"the archive's lines do not lift the tree above its own 80 percent:\n{text}")
    check(not [name for name in listed if "pytest-of-someone" in name], f"the archive copy is not listed: {listed}")


def test_the_tree_alone_at_80_percent_fails_the_gate(work: Path) -> None:
    root = work / "tree"
    write(root, SAMPLE, SOURCE)
    own = trace(work, root, "own", CONFIG, {root / SAMPLE: ("left", "right")})
    status, text, _ = gate(work, root, [own])
    check(status != 0 and "80%" in text, f"the control fails the gate at 80 percent:\n{text}")
