"""Check the training target lists against the gold control bridge (failure modes T1 to T9 in
plans/full_scope_20261022/p4a/targets_failure_modes.md).

Run from the repository root:

    .venv/bin/python -m scripts.training.targets --check                   # the committed lists against the committed bridge
    .venv/bin/python -m scripts.training.targets --check --bridge <csv>    # against another bridge copy (named in the output)

`config/training/targets.json` names the six HAI SIRs as target candidates and the same SIRs from an earlier period as
earlier-outcome candidates (owner decision, Oct 22 2026). Each listed control must map, in the bridge, to exactly one row
of fact `fct_hai_hospital_window`, measure key `<hai_type>_SIR` and value column `score`; each of the six types has
exactly one control in each list and no control is in both. The check reads files only and writes nothing.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TARGETS = REPO_ROOT / "config/training/targets.json"
BRIDGE = REPO_ROOT / "dbt/seeds/gold/hai_outcome_measures_bridge.csv"
HAI_TYPES = tuple(f"HAI_{number}" for number in range(1, 7))
LISTS = ("target_candidates", "earlier_outcome_candidates")
ENTRY_FIELDS = {"control", "hai_type", "label"}
FACT = "fct_hai_hospital_window"
VALUE_COLUMN = "score"


def load_targets(path: Path) -> tuple[dict[str, list[dict[str, str]]], list[str]]:
    """The two lists of the configuration file, or the problems that stop the check before the bridge is read."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return {}, [f"{path.name}: cannot be read as JSON ({type(error).__name__})"]
    if not isinstance(document, dict) or set(document) != {"note", *LISTS}:
        return {}, [f"{path.name}: needs exactly the keys note, {', '.join(LISTS)}"]
    lists: dict[str, list[dict[str, str]]] = {}
    problems: list[str] = []
    for name in LISTS:
        entries = document[name]
        if not isinstance(entries, list) or not all(isinstance(entry, dict) and set(entry) == ENTRY_FIELDS for entry in entries):
            problems.append(f"{path.name}: {name} must be a list of entries with exactly {', '.join(sorted(ENTRY_FIELDS))}")
            continue
        lists[name] = entries
    return lists, problems


def check(targets: Path = TARGETS, bridge: Path = BRIDGE) -> list[str]:
    """Every problem with the target lists against the bridge; an empty list means the lists hold."""
    lists, problems = load_targets(targets)
    if problems:
        return problems
    for name, entries in lists.items():
        types = [entry["hai_type"] for entry in entries]
        if sorted(types) != sorted(HAI_TYPES):
            problems.append(f"{name}: each of {', '.join(HAI_TYPES)} needs exactly one control (found {', '.join(sorted(types))})")
    overlap = {entry["control"] for entry in lists["target_candidates"]} & {entry["control"] for entry in lists["earlier_outcome_candidates"]}
    if overlap:
        problems.append(f"controls in both lists: {', '.join(sorted(overlap))}")
    with bridge.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    for name, entries in lists.items():
        for entry in entries:
            control = entry["control"]
            found = {(row["fact"], row["measure_key"], row["value_column"]) for row in rows if row["measure_control"] == control}
            expected = (FACT, f"{entry['hai_type']}_SIR", VALUE_COLUMN)
            if not found:
                problems.append(f"{name}: {control} is not in the bridge")
            elif found != {expected}:
                listed = f"{entry['hai_type']}_SIR"
                problems.append(f"{name}: {control} is listed as {listed} but the bridge maps it to {', '.join('/'.join(item) for item in sorted(found))}")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", required=True, help="compare the target lists with the bridge")
    parser.add_argument("--targets", type=Path, default=TARGETS)
    parser.add_argument("--bridge", type=Path, default=BRIDGE)
    args = parser.parse_args()
    problems = check(args.targets, args.bridge)
    for problem in problems:
        sys.stderr.write(f"{problem}\n")
    if problems:
        sys.stderr.write(f"target lists do not match the bridge {args.bridge}\n")
        return 1
    sys.stdout.write(f"target lists match the bridge {args.bridge}: 6 targets and 6 earlier-outcome candidates\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
