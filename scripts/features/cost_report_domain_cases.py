"""Cost-report domain checks on a real features build of the synthetic fixture, called by the feature store build runner.

    .venv/bin/python -m scripts.features.cost_report_domain_cases <run folder> [--output <report.json>]

The run folder is one that ``features_build`` filled from the fixture rows of ``feature_fixture.DOMAIN_CASES``. The expected status and value of every
control in every case come from the arithmetic and the literal firm domains of design 3.1 below, not from the recipe and not from the model. A recipe that
widens a domain, a model that lets a value through and a count model that disagrees with the nulls each fail here. The build itself runs the two dbt tests
``assert_cost_report_in_domain`` and ``assert_cost_report_domain_counts``; these cases add the exact status and value of each fixture row.
"""

from __future__ import annotations

import argparse
import importlib
import json
import math
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from scripts.features import feature_fixture

DAYS = feature_fixture.DOMAIN_DAYS
FTE = 10.0
DAYS_PER_YEAR = 365.0
SHARES = ("C023", "C024", "C049", "C050", "C052", "C053", "C054")
DOMAIN_MAX = {**dict.fromkeys(SHARES, 1.0), "C045": 1.0}
DOMAIN_MIN = dict.fromkeys(SHARES, 0.0)
FLOORED = ("C028", "C040", "C041", "C042", "C045", "C046", "C048", "C049", "C050", "C052", "C053", "C054", "C057")
NUMERATOR_IS_DENOMINATOR = ("C041", "C057")
CHECKED = tuple(sorted({*DOMAIN_MAX, *DOMAIN_MIN, *FLOORED}))
STATUSES = ("in_domain", "out_of_domain", "denominator_not_positive")
MEASURES_SQL = (
    "select rpt_rec_num, measure_control, domain_status, value_number from int_cost_report_measures where ccn = ? and list_contains(?, measure_control)"
)
CHECKS_SQL = (
    "select measure_control, fiscal_year, rows_evaluated, rows_out_of_domain, rows_bad_denominator from int_cost_report_domain_checks "
    "order by measure_control, fiscal_year"
)


def raw_value(control: str, denominator: float, numerator: float) -> float | None:
    """The value of one control for one case by its rule before any bound; the fixture gives every input a number."""
    if control in ("C023", "C024"):
        return numerator / DAYS
    if control in ("C040", "C042"):
        return DAYS / denominator if denominator else None
    if control == "C057":
        return denominator / denominator if denominator else None
    if control == "C028":
        return FTE / (denominator / DAYS_PER_YEAR) if denominator else None
    if control == "C041":
        return denominator / (denominator / DAYS_PER_YEAR) if denominator else None
    if control == "C048":
        return numerator / (denominator / DAYS_PER_YEAR) if denominator else None
    return numerator / denominator if denominator else None


def expected_row(control: str, denominator: float | None, numerator: float) -> tuple[str, float | None]:
    """The status and the published value the firm domains of design 3.1 give for one control and one case."""
    if control in FLOORED and (denominator is None or denominator <= 0):
        return "denominator_not_positive", None
    raw = raw_value(control, denominator or 0.0, numerator)
    if raw is None:
        return "in_domain", None
    if (control in DOMAIN_MIN and raw < DOMAIN_MIN[control]) or (control in DOMAIN_MAX and raw > DOMAIN_MAX[control]):
        return "out_of_domain", None
    return "in_domain", raw


def expected_rows() -> dict[tuple[str, str], tuple[str, float | None]]:
    """The expected status and value by report and control."""
    return {
        (report, control): expected_row(control, denominator, numerator)
        for report, _, _, denominator, numerator in feature_fixture.DOMAIN_CASES
        for control in CHECKED
        if not (denominator is None and control in NUMERATOR_IS_DENOMINATOR)
    }


def expected_counts() -> dict[tuple[str, int], tuple[int, int, int]]:
    """The expected rows evaluated, out of domain and with a bad denominator by control and fiscal year."""
    counts: dict[tuple[str, int], list[int]] = {}
    for _, _, year, denominator, numerator in feature_fixture.DOMAIN_CASES:
        for control in CHECKED:
            if denominator is None and control in NUMERATOR_IS_DENOMINATOR:
                continue
            status = expected_row(control, denominator, numerator)[0]
            entry = counts.setdefault((control, year), [0, 0, 0])
            entry[0] += 1
            entry[1] += status == "out_of_domain"
            entry[2] += status == "denominator_not_positive"
    return {key: (value[0], value[1], value[2]) for key, value in counts.items()}


def same(found: float | None, wanted: float | None) -> bool:
    """Whether two published values agree: both null, or both numbers equal to 12 digits."""
    if found is None or wanted is None:
        return found is None and wanted is None
    return math.isclose(found, wanted, rel_tol=1e-12, abs_tol=1e-12)


def count_differences(found: dict[tuple[str, int], tuple[int, ...]], wanted: dict[tuple[str, int], tuple[int, ...]], limit: int = 12) -> list[str]:
    """The control and fiscal year keys where the count model and the expected counts differ, each with both count triples; at most ``limit`` keys."""
    return [
        f"{key[0]} {key[1]}: found {found.get(key)} wanted {wanted.get(key)}" for key in sorted(set(found) | set(wanted)) if found.get(key) != wanted.get(key)
    ][:limit]


def built_domains(report: Any, run_folder: Path) -> None:
    """Check every fixture row's status and value, the count model and the build receipt against the expected domain results."""
    duckdb = importlib.import_module("duckdb")
    database = run_folder / "features_work/features.duckdb"
    connection = duckdb.connect(str(database), read_only=True)
    try:
        measures = connection.execute(MEASURES_SQL, [feature_fixture.DOMAIN_CCN, list(CHECKED)]).fetchall()
        checks = connection.execute(CHECKS_SQL).fetchall()
        every_status = {row[0] for row in connection.execute("select distinct domain_status from int_cost_report_measures").fetchall()}
    finally:
        connection.close()
    wanted = expected_rows()
    found = {(row[0], row[1]): (row[2], row[3]) for row in measures}
    report.case("E01", "CR1", "every fixture report gives a row for every checked control", set(found) == set(wanted), rows=len(found), wanted=len(wanted))
    wrong_status = sorted(f"{key[0]} {key[1]}" for key in wanted if key in found and found[key][0] != wanted[key][0])
    report.case(
        "E01", "CR2", "each row has the status the firm domains give (negative, zero, tiny, past and on each bound)", not wrong_status, wrong=wrong_status[:5]
    )
    wrong_value = sorted(f"{key[0]} {key[1]}" for key in wanted if key in found and not same(found[key][1], wanted[key][1]))
    report.case("E01", "CR3", "each row publishes the expected value, null outside the domain and below the floor", not wrong_value, wrong=wrong_value[:5])
    report.case("E01", "CR4", "the status column holds only the three statuses", every_status <= set(STATUSES), found=sorted(every_status))
    seen = {(row[0], row[1]): tuple(row[2:]) for row in checks}
    differing = count_differences(seen, expected_counts())
    report.case(
        "E01", "CR5", "the count model equals the expected counts for every control and fiscal year", not differing, rows=len(checks), differing=differing
    )
    receipt = json.loads((run_folder / "build_receipt.json").read_text())
    published = {
        (row["measure_control"], row["fiscal_year"]): (row["rows_evaluated"], row["rows_out_of_domain"], row["rows_bad_denominator"])
        for row in receipt.get("cost_report_domain_counts", [])
    }
    report.case("E01", "CR6", "the build receipt carries the count model rows", published == seen, rows=len(published))


class Report:
    """The steps of one run, printed as a JSON list."""

    def __init__(self) -> None:
        self.steps: list[dict[str, Any]] = []

    def case(self, case: str, modes: str, name: str, ok: bool, **evidence: Any) -> bool:
        """Record one check with its verdict and the evidence (names and counts only)."""
        self.steps.append({"case": case, "failure_modes": modes, "name": name, "verdict": "pass" if ok else "fail", **evidence})
        return ok


def main(argv: Sequence[str] | None = None) -> int:
    """Check one run folder and write the steps; the exit code is 1 when a check fails."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("run_folder", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    report = Report()
    built_domains(report, args.run_folder)
    text = json.dumps(report.steps, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text)
    sys.stdout.write(text)
    return 1 if any(step["verdict"] == "fail" for step in report.steps) else 0


if __name__ == "__main__":
    raise SystemExit(main())
