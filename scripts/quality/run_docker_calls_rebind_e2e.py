"""Exercise the reviewed-entry rebind of the Docker-call guard in scratch Git repositories.

Run ``.venv/bin/python -m scripts.quality.run_docker_calls_rebind_e2e``. Each case builds a scratch repository with a copy of
``scripts/quality/docker_calls.py``, ``scripts/process.py`` and a fixture, commits it with reviewed entries the case writes itself, edits the
working tree and runs ``python -m scripts.quality.docker_calls rebind`` there. Expected lines, hashes and messages are computed here, not by
the module under test. No container, AWS, install or network operation runs. Evidence stays under ``data/e2e/docker_calls_rebind``.
"""

from __future__ import annotations

import argparse
import functools
import hashlib
import json
import shutil
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from scripts.process import clear_git_environment, run_command

ROOT = Path(__file__).resolve().parents[2]
REVIEWED = "config/quality/docker_calls_reviewed.json"
RUNNER = "pkg/runner.py"
HELPER = "pkg/helper.py"
GROUPS = "scripts/quality/run_ci_groups_e2e.py"
GROUPS_DEPENDENCIES = (
    "scripts/process.py",
    "scripts/quality/ci_groups.py",
    "scripts/quality/pre_push.py",
    "scripts/quality/repo_checks.py",
    "scripts/quality/scratch_worktree.py",
)
RECORDED = 'recorded = run_command("bash", ["-c", job["steps"][0].get("run", "")], cwd=root, env={**os.environ, "GITHUB_ENV": str(marker)}, timeout=10)'
DEADLINE_RUN = "run = run_command("
DEADLINE_ARGUMENT = '["-c", HOOK_DEADLINE_PROGRAM, "--root", str(root), "--group", "code", "--output-directory", str(output)],'
CALL = "run_command(os.environ[TOOL_NAME], [], timeout=5)"
TWIN = "run_command(os.environ[TWIN_NAME], [], timeout=5)"
PROBLEM = "whose program the check cannot prove"
RUN_SECONDS = 300
HELPER_SOURCE = 'VALUE = "one"\n'
RUNNER_SOURCE = f'''"""A fixture with process starts the guard cannot prove."""

import os

from pkg import helper
from scripts.process import run_command

TOOL_NAME, TWIN_NAME = "TOOL", "TWIN"


def first() -> None:
    {CALL}


def second() -> None:
    {TWIN}
    print(helper.VALUE)


def third() -> None:
    {TWIN}
'''


@dataclass
class Repository:
    """One scratch repository and the reviewed document committed in it."""

    folder: Path
    entries: list[dict[str, object]] = field(default_factory=list)

    def text(self, relative: str) -> str:
        return (self.folder / relative).read_text()

    def write(self, relative: str, text: str) -> None:
        path = self.folder / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def reviewed_bytes(self) -> bytes:
        return (self.folder / REVIEWED).read_bytes()

    def sites(self) -> list[dict[str, object]]:
        loaded: list[dict[str, object]] = json.loads(self.reviewed_bytes())["sites"]
        return loaded


def digest(data: bytes | str) -> str:
    return hashlib.sha256(data.encode() if isinstance(data, str) else data).hexdigest()


def lines_of(text: str, code: str) -> list[int]:
    return [number for number, line in enumerate(text.splitlines(), 1) if line.strip() == code]


def entry(repository: Repository, relative: str, line: int, occurrences: int, dependencies: tuple[str, ...]) -> dict[str, object]:
    code = repository.text(relative).splitlines()[line - 1].strip()
    hashes = {name: "sha256:" + digest((repository.folder / name).read_bytes()) for name in sorted(dependencies)}
    return {
        "path": relative,
        "line": line,
        "code_sha256": digest(code),
        "problem": PROBLEM,
        "occurrences": occurrences,
        "file_sha256": digest((repository.folder / relative).read_bytes()),
        "dependencies": hashes,
        "reason": f"fixture entry for {relative}:{line}",
    }


def git(folder: Path, *arguments: str) -> None:
    run_command("git", list(arguments), cwd=folder, timeout=60, check=True)


def build(workspace: Path, name: str, files: dict[str, str], entries: Callable[[Repository], list[dict[str, object]]]) -> Repository:
    """Copy the guard into a new repository, add the fixture files, write the reviewed entries and commit them."""
    repository = Repository(workspace / name)
    for relative in ("scripts/quality/docker_calls.py", "scripts/process.py"):
        repository.write(relative, (ROOT / relative).read_text())
    for relative, text in files.items():
        repository.write(relative, text)
    repository.entries = entries(repository)
    repository.write(REVIEWED, json.dumps({"note": "fixture", "sites": repository.entries}, indent=2, ensure_ascii=False) + "\n")
    git(repository.folder, "init", "--quiet")
    git(repository.folder, "add", "--all")
    git(
        repository.folder,
        "-c",
        "user.name=fixture",
        "-c",
        "user.email=test@example.invalid",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "--quiet",
        "--message",
        "fixture",
    )
    return repository


def guard(repository: Repository, *arguments: str) -> tuple[int, str, str]:
    result = run_command(sys.executable, ["-m", "scripts.quality.docker_calls", *arguments], cwd=repository.folder, timeout=120)
    return result.returncode, result.stdout, result.stderr


def fixture_entries(repository: Repository) -> list[dict[str, object]]:
    text = repository.text(RUNNER)
    return [
        entry(repository, RUNNER, lines_of(text, CALL)[0], 1, (HELPER, "scripts/process.py")),
        entry(repository, RUNNER, lines_of(text, TWIN)[1], 2, (HELPER, "scripts/process.py")),
    ]


def fixture(workspace: Path, name: str) -> Repository:
    return build(workspace, name, {RUNNER: RUNNER_SOURCE, HELPER: HELPER_SOURCE}, fixture_entries)


def bound_only(before: list[dict[str, object]], after: list[dict[str, object]]) -> bool:
    """True when the entries are the same entries in the same order and differ at most in line, file hash and dependencies."""
    keep = ("path", "code_sha256", "problem", "occurrences", "reason")
    return [[item[key] for key in keep] for item in before] == [[item[key] for key in keep] for item in after] and all(
        set(item) == set(other) for item, other in zip(before, after, strict=True)
    )


def rebound(repository: Repository, lines: list[int], changed: tuple[str, ...], checks: dict[str, bool], absent: tuple[str, ...] = ()) -> dict[str, bool]:
    """Rebind, then require the expected pointers, current hashes, unchanged reviewed fields, a passing guard and an idempotent rerun."""
    before = repository.sites()
    code, stdout, _ = guard(repository, "rebind")
    after = repository.sites()
    checks["rebind exits 0"] = code == 0
    checks["pointers"] = [item["line"] for item in after] == lines
    checks["pointers name the reviewed code"] = all(
        digest(repository.text(str(item["path"])).splitlines()[int(str(item["line"])) - 1].strip()) == item["code_sha256"] for item in after
    )
    checks["file hashes current"] = all(item["file_sha256"] == digest((repository.folder / str(item["path"])).read_bytes()) for item in after)
    checks["only bound fields change"] = bound_only(before, after)
    checks["every moved pointer printed"] = all(
        f"moved {item['path']}: line {old['line']} -> {item['line']}" in stdout for old, item in zip(before, after, strict=True) if old["line"] != item["line"]
    )
    checks["every hash change printed"] = all(part in stdout for part in changed)
    checks["nothing else printed"] = not any(part in stdout for part in absent)
    checks["guard passes after rebind"] = guard(repository, *sorted({str(item["path"]) for item in after}))[0] == 0
    saved = repository.reviewed_bytes()
    rerun_code, rerun_stdout, _ = guard(repository, "rebind")
    checks["rerun changes nothing"] = rerun_code == 0 and repository.reviewed_bytes() == saved and "no change" in rerun_stdout
    return checks


def refused(repository: Repository, messages: tuple[str, ...], checks: dict[str, bool]) -> dict[str, bool]:
    """Rebind must fail with every message and leave the reviewed file byte for byte as it was."""
    saved = repository.reviewed_bytes()
    code, stdout, stderr = guard(repository, "rebind")
    checks["rebind exits 1"] = code == 1
    checks["failure messages"] = all(message in stderr for message in messages)
    checks["nothing written"] = repository.reviewed_bytes() == saved and "nothing written" in stderr and "moved" not in stdout
    return checks


def moved_line(workspace: Path) -> dict[str, bool]:
    repository = fixture(workspace, "moved_line")
    checks = {"guard passes on the fixture": guard(repository)[0] == 0}
    old = repository.text(RUNNER)
    repository.write(RUNNER, "# one\n# two\n# three\n" + old)
    new = repository.text(RUNNER)
    sha_change = f"file hash {RUNNER}: {digest(old)[:12]} -> {digest(new)[:12]}"
    return rebound(repository, [lines_of(new, CALL)[0], lines_of(new, TWIN)[1]], (sha_change,), checks, ("dependency ",))


def duplicate_ordinal(workspace: Path) -> dict[str, bool]:
    repository = fixture(workspace, "duplicate_ordinal")
    old = repository.text(RUNNER)
    marker = "def third() -> None:\n"
    repository.write(RUNNER, old.replace(marker, "# between the twins\n" + marker, 1))
    new = repository.text(RUNNER)
    second_twin = lines_of(new, TWIN)[1]
    checks = {"the twin moved past the first twin": second_twin == lines_of(old, TWIN)[1] + 1}
    return rebound(repository, [lines_of(new, CALL)[0], second_twin], (), checks)


def vanished_line(workspace: Path) -> dict[str, bool]:
    repository = fixture(workspace, "vanished_line")
    old = repository.text(RUNNER)
    line = lines_of(old, CALL)[0]
    repository.write(RUNNER, "# moves the twins\n" + old.replace("    " + CALL + "\n", "    pass\n", 1))
    return refused(repository, (f"{RUNNER}:{line}: the reviewed line no longer exists",), {})


def edited_line(workspace: Path) -> dict[str, bool]:
    repository = fixture(workspace, "edited_line")
    old = repository.text(RUNNER)
    line = lines_of(old, TWIN)[1]
    lines = old.splitlines(keepends=True)
    lines[line - 1] = lines[line - 1].replace("timeout=5", "timeout=6")
    repository.write(RUNNER, "".join(lines))
    checks = {"one twin is left": len(lines_of(repository.text(RUNNER), TWIN)) == 1}
    return refused(repository, (f"{RUNNER}:{line}: the reviewed line is ambiguous",), checks)


def committed_edit(workspace: Path, *, amend: bool) -> dict[str, bool]:
    """An edited reviewed twin, committed: the reviewed copy is then only in history (or, after an amend, nowhere), and rebind must refuse."""
    repository = fixture(workspace, "committed_edit_amend" if amend else "committed_edit")
    old = repository.text(RUNNER)
    line = lines_of(old, TWIN)[1]
    lines = old.splitlines(keepends=True)
    lines[line - 1] = lines[line - 1].replace("timeout=5", "timeout=6")
    repository.write(RUNNER, "".join(lines))
    git(repository.folder, "add", RUNNER)
    commit = ["-c", "user.name=fixture", "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false", "commit", "--quiet"]
    git(repository.folder, *commit, *(["--amend", "--no-edit"] if amend else ["--message", "edit the reviewed twin"]))
    message = "the reviewed copy (file hash) is not in" if amend else "the reviewed line is ambiguous"
    return refused(repository, (f"{RUNNER}:{line}: {message}",), {"one twin is left": len(lines_of(repository.text(RUNNER), TWIN)) == 1})


def dependency_change(workspace: Path) -> dict[str, bool]:
    repository = fixture(workspace, "dependency_change")
    repository.write(HELPER, 'VALUE = "two"\n')
    change = f"dependency {RUNNER}: {HELPER} {digest(HELPER_SOURCE)[:12]} -> {digest(repository.text(HELPER))[:12]}"
    text = repository.text(RUNNER)
    checks = {"guard fails before rebind": guard(repository)[0] == 1}
    return rebound(repository, [lines_of(text, CALL)[0], lines_of(text, TWIN)[1]], (change,), checks, ("file hash ", "moved "))


def noop_rerun(workspace: Path) -> dict[str, bool]:
    repository = fixture(workspace, "noop_rerun")
    saved = repository.reviewed_bytes()
    code, stdout, _ = guard(repository, "rebind")
    return {"rebind exits 0": code == 0, "no change printed": "no change" in stdout, "nothing written": repository.reviewed_bytes() == saved}


def groups_entries(repository: Repository) -> list[dict[str, object]]:
    text = repository.text(GROUPS)
    deadline_runs = [line for line in lines_of(text, DEADLINE_RUN) if text.splitlines()[line + 1].strip() == DEADLINE_ARGUMENT]
    return [entry(repository, GROUPS, lines_of(text, RECORDED)[0], 1, GROUPS_DEPENDENCIES), entry(repository, GROUPS, deadline_runs[0], 1, GROUPS_DEPENDENCIES)]


def ci_groups_pointers(workspace: Path) -> dict[str, bool]:
    """The two run_ci_groups_e2e pointers that drifted: the bash run line and the second of two identical run_command( lines."""
    files = {name: (ROOT / name).read_text() for name in (GROUPS, *GROUPS_DEPENDENCIES)}
    repository = build(workspace, "ci_groups_pointers", files, groups_entries)
    old = repository.text(GROUPS)
    runs = lines_of(old, DEADLINE_RUN)
    checks = {"the deadline run is not the first identical line": len(runs) > 1 and repository.sites()[1]["line"] != runs[0]}
    checks["guard passes on the fixture"] = guard(repository, GROUPS)[0] == 0
    lines = old.splitlines(keepends=True)
    lines.insert(runs[0], "# shifts the later run_command( lines\n")
    lines.insert(0, "# shifts every line\n")
    repository.write(GROUPS, "".join(lines))
    new = repository.text(GROUPS)
    expected = [lines_of(new, RECORDED)[0], next(line for line in lines_of(new, DEADLINE_RUN) if new.splitlines()[line + 1].strip() == DEADLINE_ARGUMENT)]
    checks = rebound(repository, expected, (), checks, ("dependency ",))
    checks["dependencies kept"] = [item["dependencies"] for item in repository.sites()] == [item["dependencies"] for item in repository.entries]
    return checks


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "moved_line": moved_line,
    "duplicate_ordinal": duplicate_ordinal,
    "vanished_line": vanished_line,
    "edited_line": edited_line,
    "committed_edit": functools.partial(committed_edit, amend=False),
    "committed_edit_amend": functools.partial(committed_edit, amend=True),
    "dependency_change": dependency_change,
    "noop_rerun": noop_rerun,
    "ci_groups_pointers": ci_groups_pointers,
}


def main() -> int:
    deadline = time.monotonic() + RUN_SECONDS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=sorted(CASES), help="run only a named case")
    parser.add_argument("--output", type=Path, help="write the run record to this ignored folder")
    args = parser.parse_args()
    clear_git_environment()
    started = datetime.now(UTC)
    out = args.output or ROOT / "data/e2e/docker_calls_rebind" / started.strftime("%Y%m%dT%H%M%S_%fZ")
    out.mkdir(parents=True, exist_ok=False)
    report: dict[str, object] = {
        "started_at_utc": started.isoformat(),
        "command": ".venv/bin/python -m scripts.quality.run_docker_calls_rebind_e2e",
        "revision": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, timeout=15, check=True).stdout.strip(),
        "source_sha256": {name: digest((ROOT / name).read_bytes()) for name in ("scripts/quality/docker_calls.py", GROUPS)},
        "limits": "Scratch repositories with fixture files and a copy of run_ci_groups_e2e.py; no Docker, AWS, installs or network operations run.",
    }
    results: list[dict[str, object]] = []
    workspace = Path(tempfile.mkdtemp(prefix="docker_calls_rebind."))
    try:
        for name in args.case or list(CASES):
            if time.monotonic() > deadline:
                report["error"] = f"the run passed its {RUN_SECONDS} second deadline before case {name}"
                break
            case_started = time.monotonic()
            try:
                checks = CASES[name](workspace)
            except (OSError, ValueError, KeyError, IndexError, StopIteration) as error:
                checks = {f"case raised {type(error).__name__}: {error}": False}
            results.append({"name": name, "checks": checks, "passed": all(checks.values()), "seconds": round(time.monotonic() - case_started, 2)})
            sys.stdout.write(f"{name}: {'passed' if all(checks.values()) else 'FAILED ' + str([key for key, value in checks.items() if not value])}\n")
    finally:
        shutil.rmtree(workspace)
    passed = sum(bool(result["passed"]) for result in results)
    total = len(args.case or CASES)
    success = passed == total and "error" not in report
    report.update(cases=results, passed=passed, total=total, cleanup="done", status="passed" if success else "failed")
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    sys.stdout.write(f"{passed}/{total} passed. Report: {out / 'report.json'}\n")
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
