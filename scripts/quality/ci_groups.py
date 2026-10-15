"""Select the check groups of scripts/run_ci.sh from the paths a change touches (config/quality/ci_groups.json).

GitHub CI and local commits use the same lists. Scopes:

- ``full``: every group (the local run before a push; the default of scripts/run_ci.sh).
- ``staged``: the paths in the Git index against HEAD (the commit hook).
- ``range``: the paths changed from ``--from`` to ``--to`` (GitHub CI). A range that cannot be used (missing end, zero or
  unknown commit) selects every group.
- ``paths``: the paths given with ``--path`` (to see what a change would run).

A path in ``global`` or a path that no group lists and that is not ``passive`` selects every group. Renames count with both
names. The unconditional gates (privacy, secrets, credential and data files, writing, the Markdown lint and the
documentation review) are not groups: scripts/run_ci.sh runs them in every scope.

    .venv/bin/python -m scripts.quality.ci_groups plan --scope range --from <sha> --to <sha> --part core [--format shell]

The plan prints as JSON, or with ``--format shell`` as lines: ``reason``, ``run <group>`` and ``skip <group> <reason>``.

E2E: ``.venv/bin/python -m scripts.quality.run_ci_groups_e2e``.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

from scripts.process import run_command

CONFIG = Path("config/quality/ci_groups.json")
SCOPES = ("full", "staged", "range", "paths")
PARTS = {"all", "core", "launcher"}
ORDER = (
    "python_lint",
    "lakehouse_checks",
    "orchestration_checks",
    "repository_code_checks",
    "configuration_checks",
    "sql_style",
    "terraform_checks",
    "launcher",
    "acquisition",
)


def load() -> dict[str, Any]:
    config = json.loads(CONFIG.read_text(encoding="utf-8"))
    unknown = (set(config["groups"]) | set(config["full_only"])) - set(ORDER)
    if unknown:
        raise ValueError(f"unknown groups in {CONFIG}: {', '.join(sorted(unknown))}")
    return config


def groups_of(part: str, config: dict[str, Any]) -> list[str]:
    known = set(config["groups"]) | set(config["full_only"])
    wanted = {"launcher"} if part == "launcher" else known - ({"launcher", "acquisition"} if part == "core" else set())
    return [group for group in ORDER if group in wanted]


def git_paths(*args: str) -> list[str]:
    result = run_command("git", ["diff", "--name-only", "--no-renames", *args], timeout=120, check=True)
    return [line for line in result.stdout.splitlines() if line]


def usable_commit(value: str) -> bool:
    if not value or re.fullmatch(r"0+", value):
        return False
    return run_command("git", ["cat-file", "-e", f"{value}^{{commit}}"], timeout=60).returncode == 0


def plan(scope: str, part: str, start: str = "", end: str = "", paths: list[str] | None = None) -> dict[str, Any]:
    config = load()
    groups = groups_of(part, config)
    changed: list[str] | None
    if scope == "full":
        changed, reason = None, "full scope: every group runs"
    elif scope == "range" and not (usable_commit(start) and usable_commit(end)):
        changed, reason = None, f"no usable range (start '{start or 'none'}', end '{end or 'none'}'): every group runs"
    elif scope == "range":
        changed, reason = git_paths(start, end), f"range {start}..{end}"
    elif scope == "staged":
        changed, reason = git_paths("--cached", "HEAD"), "staged changes"
    else:
        changed, reason = list(paths or []), "given paths"
    if changed is not None:
        trigger = next((path for path in changed if any(re.search(pattern, path) for pattern in config["global"])), None)
        listed = [pattern for patterns in config["groups"].values() for pattern in patterns] + config["passive"]
        unclassified = next((path for path in changed if not any(re.search(pattern, path) for pattern in listed)), None)
        if trigger:
            changed, reason = None, f"{reason}; {trigger} selects every group"
        elif unclassified:
            changed, reason = None, f"{reason}; unclassified path {unclassified} selects every group"
    run: list[str] = []
    skip: dict[str, str] = {}
    for group in groups:
        if group in config["full_only"]:
            if scope == "full":
                run.append(group)
            else:
                skip[group] = "runs only in the full scope (its own hook and CI job cover commits)"
        elif changed is None or any(re.search(pattern, path) for path in changed for pattern in config["groups"][group]):
            run.append(group)
        else:
            skip[group] = "no changed path is on its list"
    return {"scope": scope, "part": part, "reason": reason, "changed": None if changed is None else len(changed), "groups": groups, "run": run, "skip": skip}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("plan", help="print the groups to run")
    command.add_argument("--scope", required=True)
    command.add_argument("--part", default="all")
    command.add_argument("--from", dest="start", default="")
    command.add_argument("--to", dest="end", default="")
    command.add_argument("--path", dest="paths", action="append", default=[])
    command.add_argument("--format", choices=("json", "shell"), default="json")
    args = parser.parse_args(argv)
    if args.scope not in SCOPES or args.part not in PARTS:
        sys.stderr.write(f"scope must be one of {', '.join(SCOPES)} and part one of {', '.join(sorted(PARTS))}; got {args.scope}, {args.part}\n")
        return 2
    result = plan(args.scope, args.part, args.start, args.end, args.paths)
    if args.format == "json":
        sys.stdout.write(json.dumps(result, indent=2) + "\n")
    else:
        lines = [f"reason {result['reason']}", *(f"run {group}" for group in result["run"]), *(f"skip {group} {why}" for group, why in result["skip"].items())]
        sys.stdout.write("\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
