"""Run one complete partition of the repository's existing manual pre-commit stage."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import ProcessGroupSurvived, TimeoutExpired, run_command
from scripts.quality.repo_checks import load_yaml


def groups(root: Path) -> dict[str, list[str]]:
    """Return disjoint hook groups only when they cover the complete manual stage except its local CI aggregate."""
    config = json.loads((root / "config/quality/ci_groups.json").read_text())
    document = load_yaml((root / ".pre-commit-config.yaml").read_text())
    if not isinstance(document, dict):
        raise ValueError(".pre-commit-config.yaml must contain a mapping")
    expected = [
        hook["id"]
        for repo in document["repos"]
        for hook in repo["hooks"]
        if "manual" in hook.get("stages", document.get("default_stages", ["pre-commit", "manual"])) and hook["id"] != "local_ci"
    ]
    partitions: dict[str, list[str]] = config["hook_groups"]
    actual = [hook for hooks in partitions.values() for hook in hooks]
    if Counter(actual) != Counter(expected):
        missing = sorted((Counter(expected) - Counter(actual)).elements())
        extra = sorted((Counter(actual) - Counter(expected)).elements())
        raise ValueError(f"manual hook partition is incomplete: missing={missing}, repeated or unknown={extra}")
    return partitions


def usable(root: Path, value: str) -> bool:
    return bool(value and set(value) != {"0"} and run_command("git", ["cat-file", "-e", f"{value}^{{commit}}"], cwd=root, timeout=60).returncode == 0)


def group_deadline_seconds(root: Path, group: str) -> int:
    """Return the remaining hook budget from job start, reserving two minutes for evidence retention."""
    workflow = load_yaml((root / ".github/workflows/ci.yml").read_text())
    if not isinstance(workflow, dict):
        raise ValueError(".github/workflows/ci.yml must contain a mapping")
    rows = workflow["jobs"]["hooks"]["strategy"]["matrix"]["include"]
    seconds = next(int(row["timeout"]) * 60 - 120 for row in rows if row["group"] == group)
    if seconds <= 0:
        raise ValueError(f"hook job {group} must allow more than the two-minute evidence margin")
    now = int(time.time())
    started = int(os.environ.get("JOB_STARTED", str(now)))
    return max(0, seconds - (now - started))


def run(root: Path, group: str, hooks: list[str], output: Path, start: str, end: str) -> int:
    """Run unchanged hook entries within the workflow's group deadline and retain failures, including partial timeout output."""
    output.mkdir(parents=True, exist_ok=True)
    ranged = usable(root, start) and usable(root, end)
    selection = ["--from-ref", start, "--to-ref", end] if ranged else ["--all-files"]
    budget = group_deadline_seconds(root, group)
    deadline = time.monotonic() + budget
    results: list[dict[str, Any]] = []
    for hook in hooks:
        remaining = deadline - time.monotonic()
        command = [hook, *selection, "--hook-stage", "manual", "--show-diff-on-failure"]
        sys.stdout.write(f"Hook group {group}: {hook}\n")
        sys.stdout.flush()
        started = time.monotonic()
        try:
            if remaining <= 0:
                raise TimeoutExpired(["pre-commit", "run", *command], budget)
            result = run_command(str(root / ".venv/bin/pre-commit"), ["run", *command], cwd=root, env=os.environ.copy(), timeout=remaining)
        except TimeoutExpired as error:
            stdout = error.stdout.decode(errors="replace") if isinstance(error.stdout, bytes) else error.stdout or ""
            stderr = error.stderr.decode(errors="replace") if isinstance(error.stderr, bytes) else error.stderr or ""
            message = (
                f"no time left for hook {hook}: the {group} group budget is exhausted\n"
                if remaining <= 0
                else f"hook {hook} exceeded the remaining {remaining:.3f} second budget for group {group}\n"
            )
            if isinstance(error, ProcessGroupSurvived):
                message += f"the stop of hook {hook} is not confirmed: {error}\n"
            (output / f"{hook}.log").write_text(stdout + stderr + message)
            sys.stdout.write(stdout)
            sys.stderr.write(stderr + message)
            results.append({"hook": hook, "exit": 1, "seconds": round(time.monotonic() - started, 3), "command": command, "error": message.strip()})
            break
        (output / f"{hook}.log").write_text(result.stdout + result.stderr)
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        results.append({"hook": hook, "exit": result.returncode, "seconds": round(time.monotonic() - started, 3), "command": command})
    failed = len(results) != len(hooks) or any(row["exit"] != 0 for row in results)
    report = {
        "group": group,
        "selection": selection,
        "deadline_seconds": budget,
        "hooks": results,
        "status": "fail" if failed else "pass",
        "configuration_sha256": {
            path: hashlib.sha256((root / path).read_bytes()).hexdigest()
            for path in (".pre-commit-config.yaml", "config/quality/ci_groups.json", ".github/workflows/ci.yml")
        },
    }
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return int(failed)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--group", required=True)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--from", dest="start", default="")
    parser.add_argument("--to", dest="end", default="")
    parser.add_argument("--output-directory", type=Path)
    args = parser.parse_args()
    try:
        partitions = groups(args.root)
        if args.group not in partitions:
            raise ValueError(f"unknown hook group {args.group!r}, choose one of {', '.join(partitions)}")
    except (ValueError, KeyError) as error:
        parser.error(str(error))
    if args.list:
        sys.stdout.write(json.dumps(partitions[args.group]) + "\n")
        return 0
    output = args.output_directory or args.root / "data/e2e/hooks" / args.group / f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{os.getpid()}"
    return run(args.root, args.group, partitions[args.group], output, args.start, args.end)


if __name__ == "__main__":
    raise SystemExit(main())
