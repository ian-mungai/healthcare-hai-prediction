"""Write the MMD condition units seed and check the gold MMD bridge against the reviewed MMD collection plans (failure modes
468, 472 and 895 to 899).

Run from the repository root:

    .venv/bin/python -m scripts.lakehouse.mmd_conditions            # write dbt/seeds/mmd_condition_units.csv
    .venv/bin/python -m scripts.lakehouse.mmd_conditions --check    # compare the units seed and the gold MMD bridge with the plans

Each MMD file carries one condition label. The two collection plans that authorized the MMD captures record the control,
condition code, menu label and geography of every condition: 79 in the first plan and the user additions C258.80 and
C258.81 in the second. Each plan must match the SHA-256 the acquisition code pins; the plans are read from the main
checkout's ignored data folder, so the command also runs in a linked worktree. A control's unit is a rate per 100,000
where the registry's exact field says so, otherwise a percentage. A label in two controls, a control twice or a control
missing from the registry and its additions stops the run. The bridge (`dbt/seeds/gold/mmd_conditions_bridge.csv`) is
only checked, never written: each plan label maps to its control exactly once, and the one other row is the C258 parent
with no label. Failure modes: plans/group_c_20261006/failure_modes_c5.md and
plans/wave1_20261011/gate/mmd_conditions_failure_modes.md.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path

from scripts.acquisition.mmd_api_contract import PLANS
from scripts.lakehouse.run_lock import main_checkout

REPO_ROOT = Path(__file__).resolve().parents[2]
UNITS = REPO_ROOT / "dbt/seeds/mmd_condition_units.csv"
BRIDGE = REPO_ROOT / "dbt/seeds/gold/mmd_conditions_bridge.csv"
BRIDGE_FACT = "fct_mmd_prevalence"
PARENT = "C258"
REGISTRY = REPO_ROOT / "config/acquisition/source_registry.json"
ADDITIONS = REPO_ROOT / "config/acquisition/registry_additions.json"
UNIT_COLUMNS = ("condition_label", "value_unit")
GEOGRAPHY = {"c": "county", "s": "state"}
RATE_MARK = "rate per 100000"


class ConditionError(RuntimeError):
    """A plan changed, or its conditions do not give one control per label."""


def plan_conditions(plans: Mapping[str, Path] = PLANS) -> list[dict[str, str]]:
    """Return every condition of the pinned plans; a plan whose content changed stops the run."""
    conditions: list[dict[str, str]] = []
    for pinned, path in sorted(plans.items(), key=lambda item: str(item[1])):
        raw = (main_checkout(REPO_ROOT) / path).read_bytes()
        if hashlib.sha256(raw).hexdigest() != pinned:
            raise ConditionError(f"collection plan {path} does not match its pinned SHA-256")
        conditions.extend(json.loads(raw)["conditions"])
    return conditions


def registry_fields() -> dict[str, str]:
    """Return each control's exact field text from the registry and its user additions."""
    controls = [*json.loads(REGISTRY.read_text())["measure_controls"], *json.loads(ADDITIONS.read_text())["measure_controls"]]
    return {control["id"]: control["preserved_controls"]["current_exact_field"] for control in controls}


def rows_for(conditions: Iterable[Mapping[str, str]], fields: Mapping[str, str]) -> list[dict[str, str]]:
    """Return one seed row per control, ordered by control."""
    rows: dict[str, dict[str, str]] = {}
    labels: dict[str, str] = {}
    for condition in conditions:
        control, label = condition["measure_id"], condition["condition_label"]
        if control in rows:
            raise ConditionError(f"{control} appears twice in the collection plans")
        if label in labels:
            raise ConditionError(f"label {label!r} names both {labels[label]} and {control}")
        if control not in fields:
            raise ConditionError(f"{control} is not in the registry or its additions")
        if condition["geography"] not in GEOGRAPHY:
            raise ConditionError(f"{control} has unknown geography {condition['geography']!r}")
        labels[label] = control
        unit = "per_100000" if RATE_MARK in fields[control].lower() else "percent"
        rows[control] = {
            "measure_control": control,
            "condition_code": str(condition["condition_code"]),
            "condition_label": label,
            "geography_level": GEOGRAPHY[condition["geography"]],
            "value_unit": unit,
        }
    return [rows[control] for control in sorted(rows, key=lambda name: [int(part) for part in name[1:].split(".")])]


def build() -> list[dict[str, str]]:
    """Return one row per control from the pinned plans and the registry."""
    return rows_for(plan_conditions(), registry_fields())


def units_csv(rows: Iterable[Mapping[str, str]]) -> str:
    """Return the units seed's CSV text: each label and its unit, in control order [895]."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=UNIT_COLUMNS, lineterminator="\n", extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def bridge_problems(rows: Iterable[Mapping[str, str]], bridge: Path = BRIDGE) -> list[str]:
    """Each way the bridge's MMD rows differ from the plans' label-to-control pairs [896] [898]."""
    if not bridge.exists():
        return [f"{bridge.relative_to(REPO_ROOT)} is missing"]
    expected = {row["condition_label"]: row["measure_control"] for row in rows}
    seen: dict[str, int] = {}
    problems = []
    with bridge.open(newline="") as handle:
        for number, line in enumerate(csv.DictReader(handle), start=2):
            if line["fact"] != BRIDGE_FACT:
                continue
            label, control = line["measure_key"], line["measure_control"]
            if not label:
                if control != PARENT:
                    problems.append(f"bridge line {number}: a row with no label must be the {PARENT} parent, not {control}")
                continue
            seen[label] = seen.get(label, 0) + 1
            if label not in expected:
                problems.append(f"bridge line {number}: label {label!r} is not in the collection plans")
            elif control != expected[label]:
                problems.append(f"bridge line {number}: label {label!r} maps to {control}, the plans say {expected[label]}")
    problems += [f"bridge: label {label!r} appears {count} times" for label, count in sorted(seen.items()) if count > 1]
    problems += [f"bridge: plan label {label!r} ({expected[label]}) is missing" for label in sorted(expected.keys() - seen.keys())]
    return problems


def main() -> int:
    """Write the units seed, or check it and the bridge against the plans; the bridge is never written [899]."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="compare the units seed and the bridge with the plans instead of writing the seed")
    args = parser.parse_args()
    try:
        rows = build()
    except ConditionError as error:
        sys.stderr.write(f"mmd conditions: {error}\n")
        return 1
    text = units_csv(rows)
    if not args.check:
        UNITS.write_text(text)
        sys.stdout.write(f"mmd conditions: {len(rows)} labels written to {UNITS.relative_to(REPO_ROOT)}\n")
        return 0
    problems = [] if UNITS.exists() and UNITS.read_text() == text else [f"{UNITS.relative_to(REPO_ROOT)} differs from the plans or is missing"]
    problems += bridge_problems(rows)
    for problem in problems:
        sys.stderr.write(f"mmd conditions: {problem}\n")
    if not problems:
        sys.stdout.write(f"mmd conditions: units seed reproduced; bridge matches {len(rows)} plan labels\n")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
