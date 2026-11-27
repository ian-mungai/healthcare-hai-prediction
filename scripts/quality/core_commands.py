"""Guard that every command of the Core check groups runs exactly once, in exactly one hosted Core part.

The hosted Core work is fifteen jobs (the ``parts`` of ``config/quality/ci_groups.json``), and ``scripts/run_ci.sh`` runs the same groups locally. A command
moves between group files when a group is split, so a command could be dropped or run twice without any test noticing. This guard reads every
``timed "<label>" <command>`` line of the body files of the Core groups and compares the list with the committed inventory
``config/quality/core_commands.json``:

* a command of the inventory that no Core group file runs is *dropped*;
* a command that a Core group file runs and the inventory does not list is *added* (the inventory is updated on purpose with ``--update``, in the same change);
* a command that two lines run, in one group file or in two, is *duplicated*, unless the inventory binds it as *repeated*;
* a repeated command (the Terraform stack loop is one line in each of the two stack groups) runs once in each group and loop that the inventory records
  (``"repeated"``: the group and the ``for stack in`` list of its loop), so a line moved to another group or a stack moved to another loop is a *moved* command;
* a Core group that no part holds, or that two parts hold, is named.

A command is identified by its text after the label, so renaming a group or a label does not change it. Run it from the repository root:

    .venv/bin/python -m scripts.quality.core_commands [--root <folder>] [--update]

E2E (planted drops, duplicates and unplaced groups): ``.venv/bin/python -m scripts.quality.run_ci_groups_e2e``.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
INVENTORY = "config/quality/core_commands.json"
CONFIG = "config/quality/ci_groups.json"
GROUPS_DIRECTORY = "scripts/ci/groups"
# The groups that run in another part than the Core part: the launcher, the lakehouse layers, the full-only groups and the gate stage cases.
NOT_CORE = ("launcher", "gate_stage_checks")
TIMED = re.compile(r'\btimed "(?P<label>[^"]+)" (?P<command>.+?)\s*(?:\|\| .*)?$')


def core_groups(config: dict[str, Any]) -> list[str]:
    """The groups of the Core part in the order of the configuration (the same rule as ``scripts/quality/ci_groups.py groups_of`` for the part core)."""
    held_elsewhere = {*NOT_CORE, *config["full_only"], *config["layers"]}
    return [group for group in config["groups"] if group not in held_elsewhere]


STACK_LOOP = re.compile(r"^(\s*)for stack in ([a-z ]+); do\s*$")


def occurrences_of(path: Path) -> list[tuple[str, str]]:
    """The ``timed`` lines of one body file in file order, each as (command, the stacks of the ``for stack in`` loop around it, or an empty string)."""
    found: list[tuple[str, str]] = []
    indent, stacks = "", ""
    for line in path.read_text(encoding="utf-8").splitlines():
        loop = STACK_LOOP.match(line)
        if loop:
            indent, stacks = loop.group(1), " ".join(loop.group(2).split())
        elif stacks and line == f"{indent}done":
            stacks = ""
        if line.lstrip().startswith("#"):
            continue
        match = TIMED.search(line)
        if match:
            found.append((match.group("command").strip(), stacks))
    return found


def commands_of(path: Path) -> list[str]:
    """The commands of one body file: the text after the label of each ``timed`` line, in file order."""
    return [command for command, _ in occurrences_of(path)]


def inventory(root: Path) -> dict[str, list[str]]:
    """Each Core group with the commands of its body file."""
    config = json.loads((root / CONFIG).read_text(encoding="utf-8"))
    return {group: commands_of(root / GROUPS_DIRECTORY / f"{group}.sh") for group in core_groups(config)}


def placements(root: Path) -> dict[str, list[tuple[str, str]]]:
    """Each command with every place a Core group runs it: (group, stacks of its loop)."""
    config = json.loads((root / CONFIG).read_text(encoding="utf-8"))
    found: dict[str, list[tuple[str, str]]] = {}
    for group in core_groups(config):
        for command, stacks in occurrences_of(root / GROUPS_DIRECTORY / f"{group}.sh"):
            found.setdefault(command, []).append((group, stacks))
    return found


def findings(root: Path) -> list[str]:
    """What differs between the Core group files and the committed inventory, and the Core groups that the parts do not hold exactly once."""
    config = json.loads((root / CONFIG).read_text(encoding="utf-8"))
    problems: list[str] = []
    current = inventory(root)
    seen = placements(root)
    committed = json.loads((root / INVENTORY).read_text(encoding="utf-8"))
    expected = set(committed["commands"])
    repeated = {command: sorted((entry["group"], entry["stacks"]) for entry in entries) for command, entries in committed.get("repeated", {}).items()}
    for command, places in sorted(seen.items()):
        if command in repeated:
            if sorted(places) != repeated[command]:
                state = "duplicated command" if len(places) > len(repeated[command]) else "moved command"
                problems.append(f"{state} (the inventory binds it to {repeated[command]}, the Core groups run it in {sorted(places)}): {command}")
        elif len(places) > 1:
            problems.append(f"duplicated command ({len(places)} lines run it): {command}")
    problems += [f"dropped command (the inventory lists it, no Core group runs it): {command}" for command in sorted(expected - set(seen))]
    problems += [f"added command (a Core group runs it, the inventory lacks it; run --update): {command}" for command in sorted(set(seen) - expected)]
    parts = config.get("parts", {})
    placed = [group for groups in parts.values() for group in groups]
    problems += [f"group {group} is in no hosted Core part" for group in current if group not in placed]
    problems += [f"group {group} is in {placed.count(group)} hosted Core parts" for group in sorted(set(placed)) if placed.count(group) > 1]
    problems += [f"part group {group} is not a Core group" for group in sorted(set(placed) - set(current))]
    return problems


def config_parts(root: Path) -> dict[str, list[str]]:
    """The hosted Core parts with their groups."""
    parts: dict[str, list[str]] = json.loads((root / CONFIG).read_text(encoding="utf-8")).get("parts", {})
    return parts


def update(root: Path) -> None:
    """Write the inventory from the current group files (a deliberate change of the Core commands): each command once, and the places of a repeated one."""
    seen = placements(root)
    body = {
        "commands": sorted(seen),
        "repeated": {
            command: [{"group": group, "stacks": stacks} for group, stacks in sorted(places)] for command, places in sorted(seen.items()) if len(places) > 1
        },
    }
    (root / INVENTORY).write_text(json.dumps(body, indent=2) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", maxsplit=1)[0])
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--update", action="store_true", help="rewrite the inventory from the group files")
    args = parser.parse_args(argv)
    if args.update:
        update(args.root)
        return 0
    problems = findings(args.root)
    for problem in problems:
        sys.stderr.write(problem + "\n")
    if not problems:
        total = sum(len(commands) for commands in inventory(args.root).values())
        sys.stdout.write(f"core commands: {total} commands, each once, in {len(config_parts(args.root))} parts\n")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
