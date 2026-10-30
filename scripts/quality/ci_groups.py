"""Select the check groups of scripts/run_ci.sh from the paths a change touches (config/quality/ci_groups.json).

GitHub CI and local commits use the same lists. Scopes:

- ``full``: every group (the default of scripts/run_ci.sh; the pre-push run uses it when the selector or the CI scripts change).
- ``staged``: the paths in the Git index against HEAD (on demand; no hook runs it).
- ``range``: the paths changed from ``--from`` to ``--to`` (GitHub CI and the pre-push run,
  scripts/quality/pre_push.py). A range that cannot be used (missing end, zero or unknown commit) selects every group.
- ``paths``: the paths given with ``--path`` (to see what a change would run).

The ``launcher`` group lists, besides the few patterns of the configuration, the path pattern of each ``inventory_reads`` entry (files a suite module
lists by inventory, such as every Python file the Docker-call guard parses; the pattern ``.`` of a module whose reads are not reviewed selects the group for
every path, as ``launcher_always``, apart from the group lists, so it never classifies a path), one exact path for each file and one prefix for each folder of
config/quality/launcher_inputs.json: the files that the launcher E2E suite imports or reads and the folders it reads whole (generated and checked by
scripts/quality/launcher_inputs.py).
A path in ``global`` or a path that no group lists and that is not ``passive`` selects every group. Renames count with both
names.
The lakehouse layers (``layers`` in the configuration, upstream first) select one another: a changed path belongs to the most downstream layer whose
list matches it, and that layer and every layer downstream of it run. A path that an upstream layer's checks read although a later layer's list also
matches it is named in ``upstream_inputs`` for the earliest layer that reads it, and belongs to that layer. Each other path is listed for the layer that
reads it first, and no later layer's pattern may match it.
Groups in ``always`` run in every applicable part because their inputs cannot be completely enumerated. Groups in
``always_except_commits`` run unconditionally in the ``full`` and ``range`` scopes (the run before a push and GitHub CI) and
follow their path list in the ``staged`` and ``paths`` scopes (commits).
The unconditional source, privacy, secret, credential, data-file, writing, Markdown and documentation gates are not
groups: scripts/run_ci.sh runs them in every scope.

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
LAUNCHER_INPUTS = Path("config/quality/launcher_inputs.json")
SCOPES = ("full", "staged", "range", "paths")
LAYER_PARTS = {
    "bronze": "lakehouse_bronze",
    "silver": "lakehouse_silver",
    "gold": "lakehouse_gold",
    "features": "lakehouse_features",
    "feature_sources": "lakehouse_features",
    "feature_columns": "lakehouse_features",
    "feature_store": "lakehouse_features",
    "reuse_gate": "reuse_gate_checks",
}
PARTS = {"all", "core", "launcher", "gates", *LAYER_PARTS}
# The inventory_reads pattern of a module whose reads are not reviewed. It selects the launcher group for every changed path, but never classifies a path:
# it stays out of the group lists, so a path that no list names still selects every group.
ALWAYS_PATTERN = "."


def order(config: dict[str, Any]) -> list[str]:
    """The groups in run order: those of ``groups`` in the order of the file, then those of ``full_only``. A new group is one entry in the configuration."""
    return [*config["groups"], *config["full_only"]]


def load() -> dict[str, Any]:
    config: dict[str, Any] = json.loads(CONFIG.read_text(encoding="utf-8"))
    unknown = (set(config.get("always", [])) | set(config.get("always_except_commits", [])) | set(config["layers"])) - set(order(config))
    if unknown:
        raise ValueError(f"unknown groups in {CONFIG}: {', '.join(sorted(unknown))}")
    if set(config.setdefault("upstream_inputs", {})) - set(config["layers"]):
        raise ValueError(f"upstream_inputs in {CONFIG} may name only layers: {', '.join(sorted(set(config['upstream_inputs']) - set(config['layers'])))}")
    inputs = json.loads(LAUNCHER_INPUTS.read_text(encoding="utf-8"))
    patterns = [item["pattern"] for item in inputs.get("inventory_reads", []) if item["pattern"]]
    config["launcher_always"] = [pattern for pattern in patterns if pattern == ALWAYS_PATTERN]
    config["groups"]["launcher"] = [
        *config["groups"]["launcher"],
        *(pattern for pattern in patterns if pattern != ALWAYS_PATTERN),
        *(f"^{re.escape(folder)}/" for folder in inputs["folders"]),
        *(f"^{re.escape(path)}$" for path in inputs["files"]),
    ]
    if [layer for layer in order(config) if layer in config["layers"]] != config["layers"] or set(config["layers"]) - set(config["groups"]):
        raise ValueError(f"layers in {CONFIG} must be groups listed from upstream to downstream in the order of the groups in the file")
    return config


def groups_of(part: str, config: dict[str, Any]) -> list[str]:
    known = set(config["groups"]) | set(config["full_only"])
    if part == "launcher":
        wanted = {"launcher"}
    elif part == "gates":
        wanted = set()
    elif part in LAYER_PARTS:
        wanted = {LAYER_PARTS[part]}
    else:
        wanted = known - ({"launcher", *config["full_only"], *LAYER_PARTS.values()} if part == "core" else set())
    return [group for group in order(config) if group in wanted]


def selected_layers(changed: list[str], config: dict[str, Any]) -> set[str]:
    """The layers a change selects. A path that a layer's ``upstream_inputs`` list names belongs to the earliest layer that names it (that layer's checks read
    it although a later layer's list also matches it); any other path belongs to the most downstream layer whose list matches it. The owner and every layer
    after it are selected."""
    layers: list[str] = config["layers"]
    selected: set[str] = set()
    for path in changed:
        pinned = [index for index, layer in enumerate(layers) if any(re.search(pattern, path) for pattern in config["upstream_inputs"].get(layer, []))]
        owners = [index for index, layer in enumerate(layers) if any(re.search(pattern, path) for pattern in config["groups"][layer])]
        if pinned:
            selected.update(layers[min(pinned) :])
        elif owners:
            selected.update(layers[max(owners) :])
    return selected


def git_paths(*args: str) -> list[str]:
    result = run_command("git", ["diff", "--name-only", "--no-renames", *args], timeout=120, check=True)
    return [line for line in result.stdout.splitlines() if line]


def usable_commit(value: str) -> bool:
    if not value or re.fullmatch(r"0+", value):
        return False
    return run_command("git", ["cat-file", "-e", f"{value}^{{commit}}"], timeout=60).returncode == 0


def plan(scope: str, part: str, start: str = "", end: str = "", paths: list[str] | None = None, config: dict[str, Any] | None = None) -> dict[str, Any]:
    config = config or load()
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
    layers = selected_layers(changed, config) if changed is not None else set()
    for group in groups:
        if group in config["layers"] and changed is not None:
            if group in layers:
                run.append(group)
            else:
                skip[group] = "no changed path is on its list or on an upstream layer's list"
        elif group in config["full_only"]:
            if scope == "full":
                run.append(group)
            else:
                skip[group] = "runs only in the full scope (its own hook and CI job cover commits)"
        elif (
            group in config.get("always", [])
            or (group in config.get("always_except_commits", []) and scope in {"full", "range"})
            or changed is None
            or any(re.search(pattern, path) for path in changed for pattern in config["groups"][group])
            or (group == "launcher" and any(re.search(pattern, path) for path in changed for pattern in config["launcher_always"]))
        ):
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
