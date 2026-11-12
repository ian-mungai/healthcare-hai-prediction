"""Cost-report domain checks on a real features build of the synthetic fixture, called by the feature store build runner.

    .venv/bin/python -m scripts.features.cost_report_domain_cases <run folder> [--output <report.json>]

The run folder is one that ``features_build`` filled from the fixture rows of ``feature_fixture.DOMAIN_CASES`` and ``feature_fixture.BOUND_CASES``. The
expected status and value of every control in every case come from the arithmetic, the literal firm domains of design 3.1 and the literal plausibility bounds of
the owner decision of Nov 9 2026 below, not from the recipe and not from the model. A recipe that widens or drops a bound, a model that lets a value through
and a count model that disagrees with the nulls each fail here. The build itself runs the two dbt tests
``assert_cost_report_in_domain`` and ``assert_cost_report_domain_counts``; these cases add the exact status and value of each fixture row.

The expected counts cover every fixture cost report that holds the domain amounts (the domain cases and the package 3 reports), because the count model
reads them all.
"""

from __future__ import annotations

import argparse
import importlib
import json
import math
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from scripts.features import feature_fixture

# The rule of every checked control, worked out from the cost-report amounts and the literal bounds below, not from the recipe: (numerator amount, denominator
# amount, divisor) where the divisor is the denominator amount per reporting day for the three per-day controls; the floor is above 0 where one is listed.
RULES = {
    "C023": ("total_days_title_xviii", "total_days_v_xviii_xix_unknown", False),
    "C024": ("total_days_title_xix", "total_days_v_xviii_xix_unknown", False),
    "C028": ("fte_employees_on_payroll", "total_bed_days_available", True),
    "C036": ("total_salaries_adjusted", "fte_employees_on_payroll", False),
    "C040": ("total_days_v_xviii_xix_unknown", "total_bed_days_available", False),
    "C041": ("total_discharges_v_xviii_xix_unknown", "total_bed_days_available", True),
    "C042": ("total_days_v_xviii_xix_unknown", "total_discharges_v_xviii_xix_unknown", False),
    "C045": ("net_income_from_service_to_patients", "net_patient_revenue", False),
    "C046": ("total_current_assets", "total_current_liabilities", False),
    "C047": ("total_liabilities", "total_assets", False),
    "C048": ("cash_on_hand_and_in_banks", "less_total_operating_expense", True),
    "C049": ("cost_of_charity_care", "total_costs", False),
    "C050": ("cost_of_uncompensated_care", "total_costs", False),
    "C052": ("net_revenue_from_medicaid", "net_patient_revenue", False),
    "C053": ("total_salaries_from_worksheet_a", "less_total_operating_expense", False),
    "C054": ("contract_labor_direct_patient_care", "less_total_operating_expense", False),
    "C057": ("less_total_operating_expense", "total_discharges_v_xviii_xix_unknown", False),
}
FLOORED = ("C028", "C040", "C041", "C042", "C045", "C046", "C048", "C049", "C050", "C052", "C053", "C054", "C057")
SHARES = ("C023", "C024", "C049", "C050", "C052", "C053", "C054")
# The literal bounds: the firm domains of design 3.1 and the owner's plausibility bounds of Nov 9 2026 (unit in brackets). The margin stays at most 1; the
# balance-sheet ratios C046 and C047 and the days of cash C048 may be negative.
BOUNDS = {
    **dict.fromkeys(SHARES, (0.0, 1.0)),
    "C036": (10000.0, 1000000.0),  # dollars per FTE
    "C040": (0.0, 1.2),  # fraction of bed days
    "C042": (1.0, 60.0),  # days per discharge
    "C045": (-2.0, 1.0),  # fraction of net patient revenue
    "C046": (-50.0, 50.0),  # ratio
    "C047": (-5.0, 5.0),  # ratio
    "C048": (-1000.0, 1000.0),  # days of operating expense
}
DAYS_PER_YEAR = 365.0
CHECKED = tuple(sorted(RULES))
STATUSES = ("in_domain", "out_of_domain", "denominator_not_positive")
MEASURES_SQL = (
    "select rpt_rec_num, measure_control, domain_status, value_number from int_cost_report_measures where ccn = ? and list_contains(?, measure_control)"
)
CHECKS_SQL = (
    "select measure_control, fiscal_year, rows_evaluated, rows_out_of_domain, rows_bad_denominator from int_cost_report_domain_checks "
    "order by measure_control, fiscal_year"
)


def raw_value(control: str, row: dict[str, Any]) -> float | None:
    """The value of one control for one report by its rule before any bound; a zero denominator gives None."""
    numerator_name, denominator_name, per_day = RULES[control]
    numerator, denominator = row[numerator_name], row[denominator_name]
    if per_day:
        denominator = denominator / row["reporting_days"] if denominator is not None else None
    return numerator / denominator if denominator else None


def expected_row(control: str, row: dict[str, Any]) -> tuple[str, float | None]:
    """The status and the published value the firm domains and plausibility bounds give for one control and one report."""
    denominator = row[RULES[control][1]]
    if control in FLOORED and (denominator is None or denominator <= 0):
        return "denominator_not_positive", None
    raw = raw_value(control, row)
    if raw is None:
        return "in_domain", None
    low, high = BOUNDS.get(control, (None, None))
    if (low is not None and raw < low) or (high is not None and raw > high):
        return "out_of_domain", None
    return "in_domain", raw


def domain_reports() -> list[dict[str, Any]]:
    """The cost reports of the domain hospital: the domain cases and the bound cases."""
    return [row for row in feature_fixture.gold_rows()["fct_cost_report"] if row["ccn"] == feature_fixture.DOMAIN_CCN]


def has_amount(control: str, row: dict[str, Any]) -> bool:
    """Whether the report holds the numerator amount of the control; the model gives no row otherwise."""
    return row.get(RULES[control][0]) is not None


def expected_rows() -> dict[tuple[str, str], tuple[str, float | None]]:
    """The expected status and value by report and control, over the domain hospital's reports."""
    return {(row["rpt_rec_num"], control): expected_row(control, row) for row in domain_reports() for control in CHECKED if has_amount(control, row)}


def fixture_reports() -> list[dict[str, Any]]:
    """Every fixture cost report that holds the domain amounts; the count model reads all of them."""
    return [row for row in feature_fixture.gold_rows()["fct_cost_report"] if "net_revenue_from_medicaid" in row]


def expected_counts() -> dict[tuple[str, int], tuple[int, int, int]]:
    """The expected rows evaluated, out of domain and with a bad denominator by control and fiscal year, over every fixture cost report."""
    counts: dict[tuple[str, int], list[int]] = {}
    for row in fixture_reports():
        for control in CHECKED:
            if not has_amount(control, row):
                continue
            status = expected_row(control, row)[0]
            entry = counts.setdefault((control, row["fiscal_year"]), [0, 0, 0])
            entry[0] += 1
            entry[1] += status == "out_of_domain"
            entry[2] += status == "denominator_not_positive"
    return {key: (value[0], value[1], value[2]) for key, value in counts.items()}


def same(found: float | None, wanted: float | None) -> bool:
    """Whether two published values agree: both null, or both numbers equal to 12 digits."""
    if found is None or wanted is None:
        return found is None and wanted is None
    return math.isclose(found, wanted, rel_tol=1e-12, abs_tol=1e-12)


def count_differences(found: Mapping[tuple[str, int], tuple[int, ...]], wanted: Mapping[tuple[str, int], tuple[int, ...]], limit: int = 12) -> list[str]:
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
