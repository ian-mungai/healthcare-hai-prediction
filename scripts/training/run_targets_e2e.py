"""Check the training target-list check on copies of the committed files; never writes them (failure modes T1 to T9 in
plans/full_scope_20261022/p4a/targets_failure_modes.md).

Run ``.venv/bin/python -m scripts.training.run_targets_e2e`` from the repository root. Reports stay in data/e2e/training_targets/.
"""

from __future__ import annotations

import csv
import json
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import run_command
from scripts.training import targets

REPORT_ROOT = Path("data/e2e/training_targets")


def bridge_copy(folder: Path, name: str, change: Callable[[list[dict[str, str]]], None]) -> Path:
    """A copy of the committed bridge with one change applied."""
    with targets.BRIDGE.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    fields = list(rows[0])
    change(rows)
    path = folder / f"{name}.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return path


def targets_copy(folder: Path, name: str, change: Callable[[dict[str, Any]], None]) -> Path:
    """A copy of the committed target lists with one change applied."""
    document = json.loads(targets.TARGETS.read_text(encoding="utf-8"))
    change(document)
    path = folder / f"{name}.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def row_of(rows: list[dict[str, str]], control: str) -> dict[str, str]:
    return next(row for row in rows if row["measure_control"] == control)


def swap_keys(rows: list[dict[str, str]]) -> None:
    first, second = row_of(rows, "C270"), row_of(rows, "C271")
    first["measure_key"], second["measure_key"] = second["measure_key"], first["measure_key"]


def main() -> int:
    """Run every case, write the report and return 1 when any case fails."""
    started = datetime.now(UTC)
    cases: dict[str, bool] = {}
    evidence: dict[str, Any] = {}

    cli = run_command(sys.executable, ["-m", "scripts.training.targets", "--check"], cwd=targets.REPO_ROOT, timeout=120)
    evidence["committed"] = {"exit": cli.returncode, "stdout": cli.stdout.strip(), "stderr": cli.stderr.strip()[-600:]}
    cases["the committed lists match the committed bridge (T8: the output names the bridge)"] = (
        cli.returncode == 0 and str(targets.BRIDGE) in cli.stdout and "6 targets and 6 earlier-outcome candidates" in cli.stdout
    )

    with tempfile.TemporaryDirectory(prefix="training_targets_") as scratch:
        folder = Path(scratch)
        bridge_cases: dict[str, tuple[Callable[[list[dict[str, str]]], None], str]] = {
            "T1 a listed control missing from the bridge": (lambda rows: rows.remove(row_of(rows, "C271")), "C271 is not in the bridge"),
            "T2 two controls' keys swapped": (swap_keys, "C270 is listed as HAI_2_SIR but the bridge maps it to"),
            "T3 a value column other than score": (
                lambda rows: row_of(rows, "C272").update(value_column="compared_to_national"),
                "C272 is listed as HAI_4_SIR",
            ),
            "T3 a fact other than the HAI windows": (lambda rows: row_of(rows, "C273").update(fact="fct_hai_state_window"), "C273 is listed as HAI_5_SIR"),
            "T7 a control duplicated with another key": (
                lambda rows: rows.append({**row_of(rows, "C274"), "measure_key": "HAI_1_SIR"}),
                "C274 is listed as HAI_6_SIR",
            ),
        }
        for name, (change, expected) in bridge_cases.items():
            problems = targets.check(targets.TARGETS, bridge_copy(folder, name.split()[0], change))
            evidence[name] = problems
            cases[name] = any(expected in problem for problem in problems)

        def duplicate_type(document: dict[str, Any]) -> None:
            document["target_candidates"][3]["hai_type"] = "HAI_3"

        def overlap(document: dict[str, Any]) -> None:
            document["earlier_outcome_candidates"][0]["control"] = "C269"

        def missing_list(document: dict[str, Any]) -> None:
            del document["earlier_outcome_candidates"]

        def extra_field(document: dict[str, Any]) -> None:
            document["target_candidates"][0]["weight"] = "1"

        list_cases: dict[str, tuple[Callable[[dict[str, Any]], None], str]] = {
            "T4 one type twice and one type missing": (duplicate_type, "each of HAI_1"),
            "T5 the same control in both lists": (overlap, "controls in both lists: C269"),
            "T6 a missing list": (missing_list, "needs exactly the keys"),
            "T6 an entry with an extra field": (extra_field, "must be a list of entries with exactly"),
        }
        for name, (list_change, expected) in list_cases.items():
            problems = targets.check(targets_copy(folder, name.split()[0], list_change), targets.BRIDGE)
            evidence[name] = problems
            cases[name] = any(expected in problem for problem in problems)

        broken = folder / "broken.json"
        broken.write_text("{not json", encoding="utf-8")
        problems = targets.check(broken, targets.BRIDGE)
        evidence["T6 a file that is not JSON"] = problems
        cases["T6 a file that is not JSON"] = problems == ["broken.json: cannot be read as JSON (JSONDecodeError)"]

    folder = REPORT_ROOT / started.strftime("%Y%m%dT%H%M%SZ")
    folder.mkdir(parents=True, exist_ok=True)
    report = {"started": started.isoformat(), "cases": cases, "evidence": evidence, "passed": all(cases.values())}
    (folder / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    for name, ok in cases.items():
        sys.stdout.write(f"{'ok  ' if ok else 'FAIL'}  {name}\n")
    sys.stdout.write(f"{sum(cases.values())}/{len(cases)} passed; report {folder.resolve()}/report.json\n")
    return 0 if all(cases.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
