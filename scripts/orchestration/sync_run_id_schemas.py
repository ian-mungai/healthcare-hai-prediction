"""Keep the run ID patterns of the JSON schemas equal to the canonical contract (``orchestration/training_contract.py``).

A JSON schema cannot import Python, so each schema that checks a run ID holds a copy of the contract's pattern. This tool finds every copy (a pattern that
holds the pattern of a run ID, of a drift run ID or of a stored run ID), compares it with the contract and rewrites it from it. Run from the repository root:

    .venv/bin/python -m scripts.orchestration.sync_run_id_schemas          # list the copies that differ and exit 1 when there is one
    .venv/bin/python -m scripts.orchestration.sync_run_id_schemas --write  # rewrite them from the contract

``tests/test_run_id_forms.py`` runs the check and sends valid and invalid run IDs through every copy.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from orchestration import training_contract as contract
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_FOLDERS = ("data_contracts", "services", "config")
# A copy starts where the pattern of a run ID, of a drift run ID or of a stored run ID starts and ends at its closing parenthesis. They are the forms of the
# contract. A stored run is the run ID form or the stamp form of before the run ID redesign: only a schema field that names a run a stored record made has it.
CORE_STARTS = {"run": "(?:(?:ingestion|", "drift": "(?:drift_(?:", "stored": f"(?:{contract.CODE_STAMP_RUN}|(?:(?:ingestion|"}
CANONICAL = {"run": contract.RUN_ID, "drift": contract.DRIFT_RUN, "stored": contract.STORED_RUN}


def schema_files(root: Path = ROOT) -> list[Path]:
    """The tracked schema files that can hold a run ID pattern."""
    done = run_command("git", ["ls-files", *(f"{folder}/*.json" for folder in SCHEMA_FOLDERS)], cwd=root, timeout=60)
    if done.returncode:
        raise RuntimeError("git ls-files failed")
    return [root / name for name in done.stdout.split() if name.endswith(".json")]


def core_spans(pattern: str) -> list[tuple[str, int, int]]:
    """Every run ID copy in ``pattern`` in order, as (kind ``run`` or ``drift``, where it starts, where it ends). A pattern may hold more than one."""
    found: list[tuple[str, int, int]] = []
    position = 0
    while position < len(pattern):
        starts = [(pattern.find(start, position), kind) for kind, start in CORE_STARTS.items() if pattern.find(start, position) >= 0]
        if not starts:
            break
        begin, kind = min(starts)
        depth = 0
        for end in range(begin, len(pattern)):
            if pattern[end] == "(" and pattern[end - 1] != "\\":
                depth += 1
            elif pattern[end] == ")" and pattern[end - 1] != "\\":
                depth -= 1
                if depth == 0:
                    break
        else:
            raise ValueError(f"unbalanced parentheses in {pattern[:80]!r}")
        found.append((kind, begin, end + 1))
        position = end + 1
    return found


def substituted(pattern: str, replace: dict[str, str]) -> str:
    """``pattern`` with each run ID copy replaced by the text ``replace`` gives for its kind."""
    out, last = [], 0
    for kind, begin, end in core_spans(pattern):
        out += [pattern[last:begin], replace[kind]]
        last = end
    return "".join(out) + pattern[last:]


def expected(pattern: str) -> str:
    """The pattern with every run ID copy as the contract gives it."""
    return substituted(pattern, CANONICAL)


def patterns_of(node: Any, path: str = "") -> list[tuple[str, str]]:
    """Every ``pattern`` of a schema with its path inside the schema."""
    found: list[tuple[str, str]] = []
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "pattern" and isinstance(value, str):
                found.append((path, value))
            else:
                found += patterns_of(value, f"{path}/{key}")
    elif isinstance(node, list):
        for index, value in enumerate(node):
            found += patterns_of(value, f"{path}[{index}]")
    return found


def copies(root: Path = ROOT) -> list[tuple[Path, str, str, str, str]]:
    """Every pattern with a copy as (file, path in the file, the kinds of its copies joined by +, the pattern it holds, the pattern the contract gives)."""
    found = []
    for path in schema_files(root):
        for where, pattern in patterns_of(json.loads(path.read_text(encoding="utf-8"))):
            if core_spans(pattern):
                found.append((path, where, "+".join(kind for kind, _, _ in core_spans(pattern)), pattern, expected(pattern)))
    return found


def differing(root: Path = ROOT) -> list[tuple[Path, str, str, str]]:
    """The copies that are not what the contract gives, as (file, path in the file, held pattern, expected pattern)."""
    return [(path, where, held, wanted) for path, where, _, held, wanted in copies(root) if held != wanted]


def rewrite(root: Path = ROOT) -> int:
    """Rewrite every differing copy from the contract in place, keeping the file's other text. Returns the number of copies rewritten."""
    changed = 0
    for path in schema_files(root):
        text = path.read_text(encoding="utf-8")
        pairs: dict[str, str] = {}
        for _, pattern in patterns_of(json.loads(text)):
            wanted = expected(pattern)
            if wanted != pattern:
                pairs[json.dumps(pattern)[1:-1]] = json.dumps(wanted)[1:-1]
        for old, new in pairs.items():
            if old not in text:
                raise ValueError(f"{path.name}: the text of a pattern is not in the file as JSON")
            changed += text.count(old)
            text = text.replace(old, new)
        if pairs:
            json.loads(text)
            path.write_text(text, encoding="utf-8")
    return changed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--write", action="store_true", help="rewrite the copies that differ from the contract")
    arguments = parser.parse_args(argv)
    if arguments.write:
        sys.stdout.write(f"rewrote {rewrite()} patterns\n")
        return 0
    wrong = differing()
    for path, where, _, _ in wrong:
        sys.stdout.write(f"{path.relative_to(ROOT)} {where}: differs from the contract\n")
    sys.stdout.write(f"{len(wrong)} of {len(copies())} copies differ\n")
    return 1 if wrong else 0


if __name__ == "__main__":
    sys.exit(main())
