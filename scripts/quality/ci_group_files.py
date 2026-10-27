"""Guard for the body files of the check groups (scripts/ci/groups/<group>.sh, config/quality/ci_groups.json, scripts/run_ci.sh).

Each group of ``groups`` in the configuration has exactly one body file, and each body file belongs to one group. The dispatcher runs every group in
its own shell process that loads only scripts/quality/timing.sh and that one file, so the shell state of a body file (a function it redefines such as
``timed``, ``printf`` or another group's name, variables, options and the working directory) exists only in that process. Files and signals are shared
with the host, which the owner accepts for reviewed code, so the guard checks the shape of each file and not its effects on others. Bash lists the
functions of each file: the file is sourced alone in a clean shell (``env -i``, ``--noprofile --norc``, an empty temporary working folder, 30 seconds
at most) and ``declare -F`` names the functions. A DEBUG trap stops the shell with exit status 87 at the first command the file would run, so the file
holds function definitions and comments only and listing it executes none of its commands. The trap is a convenience for that listing, not a security
boundary: a file can shadow the trap's own commands. The file defines the entry function that the dispatcher calls (the group name, ``launcher_e2e``
for the launcher and ``lakehouse_gold_<unit>`` for each gold unit). No function of a group remains in the dispatcher: its text is searched for a
declaration of any function that a group owns (continued lines, quotes, backslashes and ``$'...'`` escapes in names removed first). A body file is
listed in its own group's path list and matches no other group's list, ``global``, ``passive`` or ``upstream_inputs`` (the broad ``\\.(py|sh)$``
pattern of ``repository_code_checks`` may match it, so that the guards run when a body file changes). Groups in ``full_only`` have no body file. The
guard also requires every configured group to have a hosted job that runs its part through scripts/run_ci.sh. The
guard needs Bash 3.2 or later (macOS ``/bin/bash`` and GitHub's Bash 5).

    .venv/bin/python -m scripts.quality.ci_group_files [--root <repository folder>]

Every finding names the file and the group. E2E: ``.venv/bin/python -m scripts.quality.run_ci_groups_e2e`` (planted drift for each rule).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command
from scripts.quality import ci_groups
from scripts.quality.repo_checks import load_yaml

GROUPS_DIRECTORY = "scripts/ci/groups"
DISPATCHER = "scripts/run_ci.sh"
CONFIG = "config/quality/ci_groups.json"
WORKFLOW = ".github/workflows/ci.yml"
BROAD_GROUP = "repository_code_checks"
ENVIRONMENT = {"PATH": "/usr/bin:/bin"}
SECONDS_PER_FILE = 30
COMMAND_EXIT = 87  # the exit status in PROGRAM
NAME = r"[A-Za-z_][\w.:+-]*"
# set -T makes the trap apply inside sourced files and functions. The first command (the source) is allowed. Afterwards only the listing command may run.
PROGRAM = (
    'set -T; trap \'__n=$((__n + 1)); if [ "$__n" -gt 1 ]; then case "$BASH_COMMAND" in "declare -F") ;; '
    '*) printf "executed: %s\\n" "$BASH_COMMAND" >&2; exit 87; esac; fi\' DEBUG; source "$CI_GROUP_FILE"; declare -F'
)
DECLARATION = re.compile(rf"(?<![\w$.:+-])function(?:\s|#[^\n]*)+({NAME})|(?<![\w$.:+-])({NAME})\s*\(\s*\)")
ANSI_QUOTED = re.compile(r"\$'((?:[^'\\]|\\.)*)'")


def sourced_names(path: Path, name: str) -> tuple[list[str], list[str]]:
    """The function names that Bash lists (``declare -F``) for one body file sourced alone, and the findings about the file."""
    with tempfile.TemporaryDirectory(prefix="ci_group_file.") as folder:
        try:
            result = run_command(
                "/bin/bash",
                ["--noprofile", "--norc", "-c", PROGRAM],
                cwd=Path(folder),
                env={**ENVIRONMENT, "CI_GROUP_FILE": str(path)},
                timeout=SECONDS_PER_FILE,
            )
        except (TimeoutExpired, OSError) as error:
            return [], [f"{name}: Bash could not list its functions: {error}"]
    message = result.stderr.strip().replace(str(path), name)
    if result.returncode == COMMAND_EXIT:
        return [], [f"{name}: runs a command when sourced ({message.splitlines()[-1] if message else 'no message'}), and a body file may only define functions"]
    if result.returncode != 0:
        return [], [f"{name}: Bash could not source it (exit {result.returncode}): {message[-300:]}"]
    if message:
        return [], [f"{name}: Bash printed a message while sourcing it: {message[-300:]}"]
    return re.findall(r"^declare -f (\S+)$", result.stdout, re.MULTILINE), []


def decoded(text: str) -> str:
    """The text with ``$'...'`` strings decoded the way Bash decodes them (``\\x73`` is ``s``), so an encoded function name is read as the name."""
    return ANSI_QUOTED.sub(lambda match: "'" + match.group(1).encode("latin-1", "backslashreplace").decode("unicode_escape") + "'", text)


def dispatcher_names(text: str) -> set[str]:
    """Every name that the dispatcher text declares as a function, read strictly: continued lines go first, then ``$'...'`` strings are decoded and backslashes
    and quote marks removed, and data is not skipped."""
    flat = re.sub(r"\\\n", "", text)
    flat = decoded(flat).replace("\\", "").replace("'", "").replace('"', "")
    return {name for match in DECLARATION.finditer(flat) for name in match.groups() if name}


def owner_of(name: str, groups: list[str]) -> str | None:
    """The group a function name belongs to: the longest group name that equals it or that it starts with and ``_``."""
    claims = [group for group in groups if name == group or name.startswith(f"{group}_")]
    return max(claims, key=len) if claims else None


def entry_functions(group: str, dispatcher: str) -> list[str]:
    """The functions the dispatcher calls for a group: the group name, except the launcher (``launcher_e2e``) and the gold layer (one function for each unit of
    ``gold_units`` in the dispatcher)."""
    if group == "lakehouse_features":
        return ["lakehouse_features_sources", "lakehouse_features_columns", "lakehouse_features_combine"]
    if group == "launcher":
        return ["launcher_e2e"]
    units = re.search(r'^gold_units="([^"]+)"$', dispatcher, re.MULTILINE)
    if group == "lakehouse_gold" and units:
        return [f"lakehouse_gold_{unit}" for unit in units.group(1).split()]
    return [group]


def body_path(group: str) -> str:
    return f"{GROUPS_DIRECTORY}/{group}.sh"


def literal_pattern(group: str) -> str:
    return f"^{GROUPS_DIRECTORY}/{group}\\.sh$"


def list_findings(group: str, config: dict[str, Any]) -> list[str]:
    """Findings about the path lists for the body file of ``group``."""
    path = body_path(group)
    lists: dict[str, list[str]] = config["groups"]
    findings = []
    if literal_pattern(group) not in lists[group]:
        findings.append(f"{path}: not in the path list of its group {group} (add {literal_pattern(group)})")
    for other, patterns in lists.items():
        if other != group and other != BROAD_GROUP and any(re.search(pattern, path) for pattern in patterns):
            findings.append(f"{path}: group {other} lists it too, but it belongs to group {group} only")
    for key in ("global", "passive"):
        if any(re.search(pattern, path) for pattern in config.get(key, [])):
            findings.append(f"{path}: the {key} list of {CONFIG} matches it, so an edit would not select group {group} only")
    for layer, patterns in config.get("upstream_inputs", {}).items():
        if any(re.search(pattern, path) for pattern in patterns):
            findings.append(f"{path}: the upstream_inputs of {layer} match it, but it belongs to group {group} only")
    return findings


def hosted_findings(root: Path, config: dict[str, Any]) -> list[str]:
    workflow = load_yaml((root / WORKFLOW).read_text(encoding="utf-8"))
    if not isinstance(workflow, dict) or not isinstance(workflow.get("jobs"), dict):
        return [f"{WORKFLOW}: no jobs mapping (define the jobs that run the check groups)"]
    covered: set[str] = set()
    findings = []
    for job, definition in workflow["jobs"].items():
        entries = definition.get("strategy", {}).get("matrix", {}).get("include", [{}])
        for step in definition.get("steps", []):
            if step.get("run") != "bash scripts/run_ci.sh":
                continue
            value = step.get("env", {}).get("CI_PART", "all")
            parts = {entry.get("part", "") for entry in entries} if value == "${{ matrix.part }}" else {value}
            for part in sorted(parts):
                if part not in ci_groups.PARTS:
                    findings.append(f"{WORKFLOW}: job {job} has unknown CI_PART={part} (use a part of {DISPATCHER})")
                else:
                    covered.update(ci_groups.groups_of(part, config))
    for group in config["groups"]:
        if group not in covered:
            part = next((part for part, layer in ci_groups.LAYER_PARTS.items() if layer == group), "launcher" if group == "launcher" else "core")
            findings.append(f"{WORKFLOW}: group {group} has no hosted job (add a bash {DISPATCHER} step with CI_PART={part})")
    return findings


def check(root: Path) -> list[str]:
    config = json.loads((root / CONFIG).read_text(encoding="utf-8"))
    groups: list[str] = list(config["groups"])
    folder = root / GROUPS_DIRECTORY
    present = sorted(path.name.removesuffix(".sh") for path in folder.glob("*.sh")) if folder.is_dir() else []
    findings = [f"{body_path(group)}: group {group} has no body file" for group in groups if group not in present]
    findings += [f"{body_path(name)}: belongs to no group of {CONFIG} (add the group or remove the file)" for name in present if name not in groups]
    dispatcher_text = (root / DISPATCHER).read_text(encoding="utf-8")
    findings += [
        f"{DISPATCHER}: defines {name}, a function of group {owner}, which belongs in {body_path(owner)}"
        for name in sorted(dispatcher_names(dispatcher_text))
        if (owner := owner_of(name, groups))
    ]
    for group in (name for name in groups if name in present):
        path = body_path(group)
        names, problems = sourced_names(root / path, path)
        findings += problems
        if not problems:
            findings += [
                f"{path}: does not define {entry}, the function the dispatcher calls for group {group}"
                for entry in entry_functions(group, dispatcher_text)
                if entry not in names
            ]
        findings += list_findings(group, config)
    findings += hosted_findings(root, config)
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    findings = check(args.root)
    for finding in findings:
        sys.stderr.write(finding + "\n")
    if findings:
        sys.stderr.write(
            f"{len(findings)} problem(s) with the check group body files. Fix the file named in each line (docs/quality_checks.md, Check group files).\n"
        )
        return 1
    count = len(json.loads((args.root / CONFIG).read_text(encoding="utf-8"))["groups"])
    sys.stdout.write(f"CI group files: {count} groups, {count} body files, no drift\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
