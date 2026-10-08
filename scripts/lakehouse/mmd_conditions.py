"""Write the MMD condition map seed from the reviewed MMD collection plans (failure modes 468 and 472).

Run from the repository root:

    .venv/bin/python -m scripts.lakehouse.mmd_conditions            # write dbt/seeds/mmd_conditions.csv
    .venv/bin/python -m scripts.lakehouse.mmd_conditions --check    # compare with the committed seed

Each MMD file carries one condition label. The two collection plans that authorized the MMD captures record the control,
condition code, menu label and geography of every condition: 79 in the first plan and the user additions C258.80 and
C258.81 in the second. Each plan must match the SHA-256 the acquisition code pins. A control's unit is a rate per 100,000
where the registry's exact field says so, otherwise a percentage. A label in two controls, a control twice or a control
missing from the registry and its additions stops the run. Failure modes:
plans/group_c_20261006/failure_modes_c5.md.
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

REPO_ROOT = Path(__file__).resolve().parents[2]
SEED = REPO_ROOT / "dbt/seeds/mmd_conditions.csv"
REGISTRY = REPO_ROOT / "config/acquisition/source_registry.json"
ADDITIONS = REPO_ROOT / "config/acquisition/registry_additions.json"
COLUMNS = ("measure_control", "condition_code", "condition_label", "geography_level", "value_unit")
GEOGRAPHY = {"c": "county", "s": "state"}
RATE_MARK = "rate per 100000"


class ConditionError(RuntimeError):
    """A plan changed, or its conditions do not give one control per label."""


def plan_conditions(plans: Mapping[str, Path] = PLANS) -> list[dict[str, str]]:
    """Return every condition of the pinned plans; a plan whose content changed stops the run."""
    conditions: list[dict[str, str]] = []
    for pinned, path in sorted(plans.items(), key=lambda item: str(item[1])):
        raw = (REPO_ROOT / path).read_bytes()
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
    """Return the seed rows from the pinned plans and the registry."""
    return rows_for(plan_conditions(), registry_fields())


def as_csv(rows: Iterable[Mapping[str, str]]) -> str:
    """Return the rows as the seed's CSV text."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def main() -> int:
    """Rebuild the MMD condition map and write or check the committed seed."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="compare with the committed seed instead of writing it")
    args = parser.parse_args()
    try:
        text = as_csv(build())
    except ConditionError as error:
        sys.stderr.write(f"mmd conditions: {error}\n")
        return 1
    if args.check:
        same = SEED.exists() and SEED.read_text() == text
        sys.stdout.write(f"mmd conditions: {'committed seed reproduced' if same else 'committed seed differs from the plans'}\n")
        return 0 if same else 1
    SEED.write_text(text)
    sys.stdout.write(f"mmd conditions: {text.count(chr(10)) - 1} controls\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
