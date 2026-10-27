"""The pre-push run: the check groups of the pushed range, once, the way GitHub CI selects them (config/quality/ci_groups.json).

The commit stage holds only fast hooks (.pre-commit-config.yaml). Before a push this run covers the rest:

- ``range`` scope (the default): ``scripts/run_ci.sh`` with ``CI_SCOPE=range`` from the merge base of the pushed head with ``origin/main`` to the head, the
  commit-message and removed-name checks of the hosted hooks job, and the three hosted hook partitions (scripts/quality/ci_hooks.py) over the same range.
- ``full`` scope: ``scripts/run_ci.sh`` with ``CI_SCOPE=full`` (it holds the hook partitions and the acquisition suite) when the range changes a path in
  ``pre_push.full_paths`` (the selector, its configuration, the CI scripts and group bodies, the workflow and the pre-commit configuration), when
  ``origin/main`` or the merge base cannot be found, or with ``--full``.

Every step has a deadline from ``pre_push.timeout_seconds`` and fails visibly with its name when it expires. A passing full run writes a marker for the
pushed head in the Git directory; the acquisition hook (scripts/acquisition/run_checks_pushed.sh) skips once when it finds it, as the full scope ran the suite.

    .venv/bin/python -m scripts.quality.pre_push plan [--root DIR] [--base SHA] [--head SHA] [--full]
    .venv/bin/python -m scripts.quality.pre_push run [same options]

The head is ``--head``, else ``PRE_COMMIT_TO_REF`` (set by the pre-push hook), else ``HEAD``. Only the first pushed reference is checked. The checks read the
working tree, so uncommitted changes to tracked files are reported. E2E: ``.venv/bin/python -m scripts.quality.run_ci_groups_e2e --selection-only``.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import CompletedProcess, TimeoutExpired, run_command

CONFIG = Path("config/quality/ci_groups.json")
MARKER = "hai_pre_push_full"
GIT_SECONDS = 120


def git(root: Path, *args: str) -> tuple[int, str]:
    result = run_command("git", ["-C", str(root), *args], timeout=GIT_SECONDS)
    return result.returncode, result.stdout.strip()


def resolve_head(root: Path, head: str) -> str:
    code, sha = git(root, "rev-parse", "--verify", f"{head or os.environ.get('PRE_COMMIT_TO_REF') or 'HEAD'}^{{commit}}")
    return sha if code == 0 else ""


def resolve_base(root: Path, base: str, head: str) -> tuple[str, str]:
    """The start of the range and the reason it cannot be used (empty when it can)."""
    if not head:
        return "", "the pushed head is not a commit"
    if base:
        code, sha = git(root, "rev-parse", "--verify", f"{base}^{{commit}}")
        return (sha, "") if code == 0 else ("", f"base {base} is not a commit")
    code, sha = git(root, "merge-base", head, "origin/main")
    return (sha, "") if code == 0 and sha else ("", "no merge base with origin/main (origin/main is missing or unrelated)")


def escalating_path(root: Path, config: dict[str, Any], start: str, end: str) -> str:
    code, listing = git(root, "diff", "--name-only", "--no-renames", start, end)
    if code != 0:
        return "<git diff failed>"
    patterns = [re.compile(pattern) for pattern in config["pre_push"]["full_paths"]]
    return next((path for path in listing.splitlines() if any(pattern.search(path) for pattern in patterns)), "")


def command(root: Path, step: dict[str, Any]) -> list[str]:
    """The command line of a step, for the plan; ``launch`` starts the same command."""
    python = str(root / ".venv/bin/python")
    modules = {"repo_checks": "scripts.quality.repo_checks", "ci_hooks": "scripts.quality.ci_hooks"}
    return ["bash", "scripts/run_ci.sh"] if step["kind"] == "run_ci" else [python, "-m", modules[step["kind"]], *step["arguments"]]


def launch(root: Path, step: dict[str, Any], environment: dict[str, str]) -> CompletedProcess[str]:
    arguments, timeout = step["arguments"], step["timeout_seconds"]
    if step["kind"] == "repo_checks":
        return run_command(str(root / ".venv/bin/python"), ["-m", "scripts.quality.repo_checks", *arguments], cwd=root, env=environment, timeout=timeout)
    if step["kind"] == "ci_hooks":
        return run_command(str(root / ".venv/bin/python"), ["-m", "scripts.quality.ci_hooks", *arguments], cwd=root, env=environment, timeout=timeout)
    return run_command("bash", ["scripts/run_ci.sh"], cwd=root, env=environment, timeout=timeout)


def plan(root: Path, base: str = "", head: str = "", full: bool = False) -> dict[str, Any]:
    config = json.loads((root / CONFIG).read_text(encoding="utf-8"))
    end = resolve_head(root, head)
    start, unusable = resolve_base(root, base, end)
    if full:
        scope, reason = "full", "requested with --full"
    elif unusable:
        scope, reason = "full", f"{unusable}: every group runs"
    else:
        trigger = escalating_path(root, config, start, end)
        scope, reason = ("full", f"{trigger} changes the group selector or the CI scripts: every group runs") if trigger else ("range", f"range {start}..{end}")
    output = root / "data/e2e/pre_push" / f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{os.getpid()}"
    seconds = int(config["pre_push"]["timeout_seconds"][scope])
    steps: list[dict[str, Any]] = []
    if start and end:
        steps += [
            {"name": "commit_messages", "kind": "repo_checks", "arguments": ["commit-range", start, end], "env": {}},
            {"name": "removed_names", "kind": "repo_checks", "arguments": ["removed-names"], "env": {"PRE_COMMIT_FROM_REF": start, "PRE_COMMIT_TO_REF": end}},
        ]
    if scope == "range":
        steps.append({"name": "local_ci", "kind": "run_ci", "arguments": [], "env": {"CI_SCOPE": "range", "RANGE_START": start, "RANGE_END": end}})
        steps += [
            {
                "name": f"hooks_{group}",
                "kind": "ci_hooks",
                "arguments": ["--group", group, "--from", start, "--to", end, "--output-directory", str(output / "hooks" / group)],
                "env": {},
            }
            for group in config["hook_groups"]
        ]
    else:
        steps.append({"name": "local_ci", "kind": "run_ci", "arguments": [], "env": {"CI_SCOPE": "full"}})
    for step in steps:
        step["timeout_seconds"] = seconds
        step["argv"] = command(root, step)
    return {"scope": scope, "reason": reason, "start": start, "end": end, "output": str(output), "steps": steps}


def run_step(root: Path, output: Path, step: dict[str, Any]) -> dict[str, Any]:
    environment = {key: value for key, value in os.environ.items() if key not in {"CI_SCOPE", "CI_PART", "RANGE_START", "RANGE_END"}}
    environment.update(step["env"])
    sys.stdout.write(f"pre-push: start {step['name']} (deadline {step['timeout_seconds']} s)\n")
    sys.stdout.flush()
    started = time.monotonic()
    error = ""
    try:
        result = launch(root, step, environment)
        code, text = result.returncode, result.stdout + result.stderr
    except TimeoutExpired:
        code, text, error = 1, "", f"step {step['name']} exceeded its deadline of {step['timeout_seconds']} seconds and was stopped"
    except FileNotFoundError as missing:
        code, text, error = 1, "", f"step {step['name']} could not start: {missing}"
    (output / f"{step['name']}.log").write_text(text + error + "\n", encoding="utf-8")
    return {"name": step["name"], "exit": code, "seconds": round(time.monotonic() - started, 1), "output": text, "error": error}


def run(root: Path, base: str, head: str, full: bool) -> int:
    chosen = plan(root, base, head, full)
    output = Path(chosen["output"])
    output.mkdir(parents=True, exist_ok=True)
    sys.stdout.write(f"pre-push: scope {chosen['scope']}: {chosen['reason']}\n")
    dirty = git(root, "status", "--porcelain", "--untracked-files=no")[1]
    if dirty:
        sys.stdout.write("pre-push: warning: tracked files have uncommitted changes; the checks read the working tree, not only the pushed commits\n")
    with ThreadPoolExecutor(max_workers=len(chosen["steps"])) as pool:
        results = list(pool.map(lambda step: run_step(root, output, step), chosen["steps"]))
    for row in results:
        sys.stdout.write(f"pre-push: {'PASS' if row['exit'] == 0 else 'FAIL'} {row['name']} ({row['seconds']} s)\n")
        if row["exit"] != 0:
            sys.stdout.write("\n".join(row["output"].splitlines()[-80:]) + "\n" + row["error"] + "\n")
    failed = [row["name"] for row in results if row["exit"] != 0]
    (output / "report.json").write_text(
        json.dumps({"plan": chosen, "results": [{k: v for k, v in row.items() if k != "output"} for row in results]}, indent=2) + "\n"
    )
    if failed:
        sys.stdout.write(f"pre-push: failed steps: {', '.join(failed)}; logs in {output}\n")
        return 1
    if chosen["scope"] == "full" and chosen["end"]:
        code, marker = git(root, "rev-parse", "--git-path", MARKER)
        if code == 0:
            (root / marker).write_text(chosen["end"] + "\n", encoding="utf-8")
    sys.stdout.write(f"pre-push: all steps passed; logs in {output}\n")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("plan", "run"))
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--base", default="")
    parser.add_argument("--head", default="")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    if args.command == "plan":
        sys.stdout.write(json.dumps(plan(root, args.base, args.head, args.full), indent=2) + "\n")
        return 0
    return run(root, args.base, args.head, args.full)


if __name__ == "__main__":
    raise SystemExit(main())
