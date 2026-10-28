"""Check the MMD condition generator's units seed and bridge check on copies of the committed files; never writes them.

Run ``.venv/bin/python -m scripts.lakehouse.run_mmd_conditions_e2e`` from the repository root (the pinned collection
plans are read from the main checkout). Reports stay in data/e2e/mmd_conditions/.
"""

from __future__ import annotations

import csv
import io
import json
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from scripts.lakehouse import mmd_conditions
from scripts.process import run_command

REPORT_ROOT = Path("data/e2e/mmd_conditions")


def mutated(lines: list[dict[str, str]], change: Callable[[list[dict[str, str]]], None], folder: Path, name: str) -> Path:
    """A copy of the bridge with one change applied, written to the scratch folder."""
    rows = [dict(line) for line in lines]
    change(rows)
    path = folder / f"{name}.csv"
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(lines[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return path


def first_labelled(rows: list[dict[str, str]]) -> dict[str, str]:
    """The bridge's first MMD row that carries a label."""
    return next(row for row in rows if row["fact"] == mmd_conditions.BRIDGE_FACT and row["measure_key"])


def swap(rows: list[dict[str, str]]) -> None:
    """Give the first two labelled rows each other's control."""
    labelled = [row for row in rows if row["fact"] == mmd_conditions.BRIDGE_FACT and row["measure_key"]][:2]
    labelled[0]["measure_control"], labelled[1]["measure_control"] = labelled[1]["measure_control"], labelled[0]["measure_control"]


def main() -> int:
    """Run every scenario, write the report and return 1 when any check fails."""
    started = datetime.now(UTC)
    checks: dict[str, bool] = {}
    evidence: dict[str, object] = {}
    plan_rows = mmd_conditions.build()
    with mmd_conditions.BRIDGE.open(newline="") as handle:
        lines = list(csv.DictReader(handle))

    # 895 and 896: the committed files reproduce through the command itself.
    cli = run_command(sys.executable, ["-m", "scripts.lakehouse.mmd_conditions", "--check"], cwd=mmd_conditions.REPO_ROOT, timeout=300)
    evidence["check"] = {"exit": cli.returncode, "stdout": cli.stdout.strip(), "stderr": cli.stderr.strip()[-600:]}
    checks["the committed units seed and bridge pass --check"] = cli.returncode == 0 and "bridge matches 81 plan labels" in cli.stdout

    # 895: an edited unit no longer reproduces.
    text = mmd_conditions.units_csv(plan_rows)
    edited = text.replace(",percent\n", ",per_100000\n", 1)
    checks["an edited unit differs from the plans"] = edited != text and mmd_conditions.UNITS.read_text() == text

    with tempfile.TemporaryDirectory() as scratch:
        folder = Path(scratch)
        label = first_labelled(lines)["measure_key"]
        cases = {
            "swapped": (swap, "maps to"),
            "dropped": (lambda rows: rows.remove(first_labelled(rows)), f"plan label {label!r}"),
            "invented": (
                lambda rows: rows.append({**first_labelled(rows), "measure_key": "Invented Condition"}),
                "'Invented Condition' is not in the collection plans",
            ),
            "duplicated": (lambda rows: rows.append(dict(first_labelled(rows))), "appears 2 times"),
            "unlabelled_not_parent": (lambda rows: first_labelled(rows).update(measure_key=""), "must be the C258 parent"),
        }
        for name, (change, reason) in cases.items():
            problems = mmd_conditions.bridge_problems(plan_rows, mutated(lines, change, folder, name))
            evidence[name] = problems
            checks[f"896: bridge case {name.replace('_', ' ')} fails"] = any(reason in problem for problem in problems)
        # 898: a missing bridge is a failure, not a pass.
        missing = mmd_conditions.bridge_problems(plan_rows, mmd_conditions.REPO_ROOT / "dbt/seeds/gold/absent_bridge.csv")
        evidence["missing"] = missing
        checks["898: a missing bridge fails"] = missing == ["dbt/seeds/gold/absent_bridge.csv is missing"]

    # 897: the refusals the staging runner relies on stay.
    try:
        mmd_conditions.rows_for([*[{"measure_id": "C258.01", "condition_code": "1", "condition_label": "A", "geography": "c"}] * 2], {"C258.01": "x"})
        checks["897: a control twice is refused"] = False
    except mmd_conditions.ConditionError as error:
        checks["897: a control twice is refused"] = "appears twice" in str(error)

    passed = all(checks.values())
    folder = REPORT_ROOT / started.strftime("%Y%m%dT%H%M%SZ")
    folder.mkdir(parents=True, exist_ok=True)
    report = {"started_utc": started.isoformat(), "status": "pass" if passed else "fail", "checks": checks, "evidence": evidence}
    (folder / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    output = io.StringIO()
    for name, ok in checks.items():
        output.write(f"{'ok  ' if ok else 'FAIL'}  {name}\n")
    output.write(f"{'pass' if passed else 'fail'}: {folder / 'report.json'}\n")
    sys.stdout.write(output.getvalue())
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
