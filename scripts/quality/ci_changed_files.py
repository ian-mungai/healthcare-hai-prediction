"""The changed-file checks that start the pre-push run, before its long groups (scripts/quality/pre_push.py).

They read only the files the pushed range changed and take a few minutes:

- ``ruff_format`` and ``ruff_check`` on the changed Python files;
- ``mypy`` on the changed Python files that the type check covers (the files in ``config/quality/mypy_runtimes.json`` under ``pending`` wait for their runtime);
- ``sqlfluff`` on the changed dbt SQL files;
- ``docker_calls``, the Docker-call guard, and ``ci_group_files``, the guard of the check group files, which run for every range.

The steps run at once. Every step has the deadline ``pre_push.changed_file_seconds`` of config/quality/ci_groups.json and fails visibly with its name. The
run exits 1 when any step fails, and the pre-push run then stops before the long groups.

    .venv/bin/python -m scripts.quality.ci_changed_files --from <sha> --to <sha> [--root DIR] [--only NAME]
    .venv/bin/python -m scripts.quality.ci_changed_files --file <path> [--file <path>] [--root DIR] [--only NAME]

E2E: ``.venv/bin/python -m scripts.quality.run_ci_groups_e2e --selection-only``.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from scripts.process import CompletedProcess, TimeoutExpired, run_command
from scripts.quality import typecheck

CONFIG = Path("config/quality/ci_groups.json")
GIT_SECONDS = 120
NAMES = ("ruff_format", "ruff_check", "mypy", "sqlfluff", "docker_calls", "ci_group_files")


def changed_in_range(root: Path, start: str, end: str) -> list[str]:
    """The added, copied, modified and renamed paths from ``start`` to ``end``."""
    result = run_command("git", ["-C", str(root), "diff", "--name-only", "--diff-filter=ACMR", start, end], timeout=GIT_SECONDS, check=True)
    return [line for line in result.stdout.splitlines() if line]


def covered_by_mypy(files: list[str]) -> list[str]:
    """The files the type check covers: all but those that wait for their runtime (``pending`` in config/quality/mypy_runtimes.json)."""
    pending = json.loads(typecheck.CONFIG.read_text(encoding="utf-8"))["pending"]
    waiting = typecheck.pending_by_import(typecheck.source_files(), pending)
    return [path for path in files if path not in waiting]


def steps(root: Path, files: list[str], covered: Callable[[list[str]], list[str]] = covered_by_mypy) -> list[dict[str, Any]]:
    """The checks for the changed files: name and the files each reads. A check with no file to read is left out."""
    python = [path for path in files if path.endswith(".py")]
    sql = [path for path in files if path.startswith("dbt/") and path.endswith(".sql")]
    chosen: list[dict[str, Any]] = []
    if python:
        chosen.append({"name": "ruff_format", "files": python})
        chosen.append({"name": "ruff_check", "files": python})
        typed = covered(python)
        if typed:
            chosen.append({"name": "mypy", "files": typed})
    if sql:
        chosen.append({"name": "sqlfluff", "files": [f"./{path}" for path in sql]})
    chosen.append({"name": "docker_calls", "files": []})
    chosen.append({"name": "ci_group_files", "files": []})
    return chosen


def launch(root: Path, step: dict[str, Any], seconds: int) -> CompletedProcess[str]:
    """Start the step's command, written out with literal programs and modules for the Docker-call guard."""
    name, listed = step["name"], step["files"]
    if name == "ruff_format":
        return run_command(sys.executable, ["-m", "ruff", "format", "--check", *listed], cwd=root, timeout=seconds)
    if name == "ruff_check":
        return run_command(sys.executable, ["-m", "ruff", "check", *listed], cwd=root, timeout=seconds)
    if name == "mypy":
        return run_command(sys.executable, ["-m", "mypy", "--check-untyped-defs", "--disallow-untyped-defs", "--", *listed], cwd=root, timeout=seconds)
    if name == "sqlfluff":
        return run_command(".tools/sqlfluff/bin/sqlfluff", ["lint", "--config", ".sqlfluff", *listed], cwd=root, timeout=seconds)
    if name == "docker_calls":
        return run_command(sys.executable, ["-m", "scripts.quality.docker_calls"], cwd=root, timeout=seconds)
    return run_command(sys.executable, ["-m", "scripts.quality.ci_group_files", "--root", str(root)], cwd=root, timeout=seconds)


def run_step(root: Path, step: dict[str, Any], seconds: int) -> dict[str, Any]:
    started = time.monotonic()
    error = ""
    try:
        result = launch(root, step, seconds)
        code, text = result.returncode, result.stdout + result.stderr
    except TimeoutExpired:
        code, text, error = 1, "", f"check {step['name']} exceeded its deadline of {seconds} seconds and was stopped"
    except FileNotFoundError as missing:
        code, text, error = 1, "", f"check {step['name']} could not start: {missing}"
    return {"name": step["name"], "exit": code, "seconds": round(time.monotonic() - started, 1), "output": text, "error": error}


def run(root: Path, files: list[str], only: str = "") -> int:
    config = json.loads((root / CONFIG).read_text(encoding="utf-8"))
    seconds = int(config["pre_push"]["changed_file_seconds"])
    chosen = [step for step in steps(root, files) if not only or step["name"] == only]
    sys.stdout.write(f"changed-file checks: {len(files)} changed files, checks {', '.join(step['name'] for step in chosen) or 'none'}\n")
    sys.stdout.flush()
    with ThreadPoolExecutor(max_workers=max(1, len(chosen))) as pool:
        results = list(pool.map(lambda step: run_step(root, step, seconds), chosen))
    for result in results:
        sys.stdout.write(f"changed-file checks: {'PASS' if result['exit'] == 0 else 'FAIL'} {result['name']} ({result['seconds']} s)\n")
        if result["exit"] != 0:
            sys.stdout.write("\n".join(result["output"].splitlines()[-40:]) + "\n" + result["error"] + "\n")
    failed = [result["name"] for result in results if result["exit"] != 0]
    if failed:
        sys.stdout.write(f"changed-file checks: failed: {', '.join(failed)}\n")
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--from", dest="start", default="")
    parser.add_argument("--to", dest="end", default="")
    parser.add_argument("--file", action="append", default=[], help="a changed path (instead of --from and --to)")
    parser.add_argument("--only", default="", choices=("", *NAMES))
    args = parser.parse_args(argv)
    root = args.root.resolve()
    if args.file:
        files = args.file
    elif args.start and args.end:
        files = changed_in_range(root, args.start, args.end)
    else:
        sys.stderr.write("give --from and --to, or at least one --file\n")
        return 2
    return run(root, files, args.only)


if __name__ == "__main__":
    raise SystemExit(main())
