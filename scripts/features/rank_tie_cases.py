"""Row-choice ties in the feature models, called by the feature store build runner and runnable alone for the permutation probe.

    .venv/bin/python -m scripts.features.rank_tie_cases probe [--output <report.json>]
    .venv/bin/python -m scripts.features.rank_tie_cases built <run folder> [--output <report.json>]

A feature model that keeps one row of several eligible rows must order the rows completely. With a partial order the rows that tie are kept in the order the
engine reads them, so a rebuild on the same data can publish another value. The as-of rule picks the latest period end; the rules below add what decides between
equal ends, each ending in a key that comes from the source rows and never from the run:

* ONC performance period (``int_spine_operations_measures``): the later start.
* Care Compare window (``int_spine_care_compare_measures``): the later start, then the later release, then the lower measure ID, then the published texts and
  numbers.
* PLACES value (``int_spine_county_measures``): the later edition, then the lower ``places_row_key``.
* County period of one control and field read from two sources (``int_spine_county_measures``): the lower source name.
* Reason text of a cost-report control (``int_cost_report_domain_checks``): the reasons in alphabetical order.

``probe`` runs the real text of each ranking step of the model files, taken by common table expression name, in DuckDB on small input tables, once for every
order of the rows that tie, and compares each result with the winner that ``wanted`` works out from the rows by the rule above. ``built`` checks the rows of a
features build of the fixture (``feature_fixture.tie_rows``) against the same winners. Each probe case also runs against a copy of the model text with the
tie-break removed: that copy must give more than one result across the orders or a result that is not the winner.
"""

from __future__ import annotations

import argparse
import importlib
import itertools
import json
import math
import re
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from scripts.features import feature_fixture

ROOT = Path(__file__).resolve().parents[2]
MODELS = ROOT / "dbt/models/features"
RECIPE = ROOT / "dbt/seeds/features/cost_report_measures_recipe.csv"
REF = re.compile(r"\{\{\s*ref\('([a-z0-9_]+)'\)\s*\}\}")
CTE_HEAD = re.compile(r"^([a-z0-9_]+) as \($", re.MULTILINE)
MAX_ORDERS = 120
CARE_TIE_BREAKS = (
    "periods.period_start desc nulls last",
    "periods.release_date desc nulls last",
    "periods.measure_id asc",
    "periods.value_text asc",
    "periods.value_number asc",
    "periods.footnote_text asc",
    "periods.sample_text asc",
)
INSERT_SQL = "insert into {table} values ({marks})"
SPINE_COLUMNS = (
    "ccn varchar, window_year integer, window_start date, spine_key varchar, is_primary_population boolean, is_sensitivity_population boolean, "
    "is_connecticut boolean, is_maryland boolean"
)
Row = tuple[Any, ...]


def cte_texts(model: str) -> dict[str, str]:
    """The text of every common table expression of one model file by name, with each ``ref`` replaced by the relation name."""
    text = REF.sub(lambda match: match.group(1), (MODELS / model).read_text())
    texts: dict[str, str] = {}
    for head in CTE_HEAD.finditer(text):
        depth, index = 1, head.end()
        while depth:
            character = text[index]
            if character == "'":
                index = text.index("'", index + 1)
            elif text.startswith("--", index):
                index = text.index("\n", index)
            elif character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
            index += 1
        texts[head.group(1)] = text[head.start() : index]
    return texts


def with_clause(model_text: dict[str, str], names: Sequence[str]) -> str:
    """The ``with`` clause of the named expressions in the order given; a left-over template tag is an error."""
    body = ",\n".join(model_text[name] for name in names)
    if "{{" in body or "{%" in body:
        raise RuntimeError(f"a ranking step of the model uses a template tag the probe does not resolve: {names}")
    return "with\n" + body


@dataclass
class Probe:
    """One ranking step: the model, its steps by name, the input tables (name to column definitions and rows), the rows that tie, the final query and the
    winner the rule gives."""

    name: str
    model: str
    steps: tuple[str, ...]
    tables: dict[str, tuple[str, list[Row]]]
    tie_table: str
    final: str
    wanted: Callable[[], list[Row]]
    stripped: tuple[str, str]


def run_once(duckdb: Any, probe: Probe, rows: list[Row], text: dict[str, str]) -> list[Row]:
    """The result of the final query on the input tables with the tie table in the given row order."""
    connection = duckdb.connect()
    try:
        for table, (columns, held) in probe.tables.items():
            connection.execute(f"create table {table} ({columns})")
            marks = ",".join("?" for _ in columns.split(","))
            loaded = rows if table == probe.tie_table else held
            if loaded:
                connection.executemany(INSERT_SQL.format(table=table, marks=marks), loaded)
        return [tuple(row) for row in connection.execute(with_clause(text, probe.steps) + "\n" + probe.final).fetchall()]
    finally:
        connection.close()


def orders(rows: list[Row]) -> list[list[Row]]:
    """Every order of the rows, or about ``MAX_ORDERS`` of them spread evenly over all orders when there are more."""
    step = max(1, math.factorial(len(rows)) // MAX_ORDERS)
    return [list(order) for order in itertools.islice(itertools.permutations(rows), 0, None, step)]


def outcomes(duckdb: Any, probe: Probe, text: dict[str, str]) -> set[str]:
    """The distinct results over all orders of the tie table, each as text."""
    return {json.dumps(run_once(duckdb, probe, order, text), default=str) for order in orders(probe.tables[probe.tie_table][1])}


def strip_tie_break(probe: Probe) -> dict[str, str]:
    """The model's steps with the tie-break of this probe removed; the text it removes must be there."""
    text = dict(cte_texts(probe.model))
    old, new = probe.stripped
    for name in probe.steps:
        if old in text[name]:
            text[name] = text[name].replace(old, new, 1)
            return text
    raise RuntimeError(f"{probe.model} does not hold the tie-break {old!r}")


def spine_row(key: str, ccn: str) -> Row:
    """One spine row of a hospital with the window 2023 (the columns the ranking steps read: ccn, year, start, key, four population flags)."""
    return (ccn, 2023, date(2023, 1, 1), key, True, False, False, False)


def onc_probe() -> Probe:
    flags = "meets_criteria_for_promoting_interoperability_of_ehrs"
    spine = SPINE_COLUMNS
    product = f"ccn varchar, start_date date, end_date date, {flags} boolean, developer_name varchar, chpl_id varchar, cehrt_id varchar"
    onc: list[Row] = [
        ("T1", date(2022, 1, 1), date(2022, 6, 30), True, "Vendor X", "c1", "e1"),
        ("T1", date(2022, 4, 1), date(2022, 6, 30), True, "Vendor Y", "c2", "e2"),
        ("T1", date(2021, 7, 1), date(2022, 6, 30), True, "Vendor Z", "c3", "e3"),
        ("T2", date(2021, 1, 1), date(2021, 12, 30), True, "Vendor A", "c4", "e4"),
        ("T2", date(2022, 1, 1), date(2022, 3, 31), True, "Vendor B", "c5", "e5"),
        ("T2", date(2023, 1, 1), date(2023, 12, 31), True, "Vendor C", "c6", "e6"),
    ]

    def wanted() -> list[Row]:
        found = []
        for ccn in ("T1", "T2"):
            eligible = [row for row in onc if row[0] == ccn and row[2] < date(2023, 1, 1)]
            winner = max(eligible, key=lambda row: (row[2], row[1]))
            found.append((f"{ccn}:2023", winner[1], winner[2], winner[4]))
        return found

    return Probe(
        "onc_period",
        "int_spine_operations_measures.sql",
        ("spine", "onc_periods", "onc_latest"),
        {"int_hospital_spine": (spine, [spine_row("T1:2023", "T1"), spine_row("T2:2023", "T2")]), "fct_onc_ehr_product": (product, onc)},
        "fct_onc_ehr_product",
        "select spine_key, start_date, end_date, developers from onc_latest where recency = 1 order by spine_key",
        wanted,
        (", onc_periods.start_date desc", ""),
    )


def care_probe() -> Probe:
    spine = SPINE_COLUMNS
    windows = (
        "measure_control varchar, entity_id varchar, measure_id varchar, window_start date, window_end date, value_text varchar, value_number double, "
        "footnote_text varchar, sample_text varchar, release_date date"
    )
    ratings = "ccn varchar, release_date date, overall_rating_text varchar, overall_rating integer, overall_rating_footnote varchar, release_file_count integer"
    end = date(2022, 3, 31)
    held: list[Row] = [
        ("C154", "T1", "IMM_3", date(2021, 10, 1), end, "70", 70.0, None, None, date(2022, 7, 31)),
        ("C154", "T1", "IMM_3", date(2022, 1, 1), end, "75", 75.0, None, None, date(2022, 7, 31)),
        ("C154", "T1", "IMM_3", date(2022, 1, 1), end, "80", 80.0, None, None, date(2022, 8, 31)),
        ("C154", "T1", "IMM_3", date(2022, 1, 1), end, "85", 85.0, None, None, date(2022, 8, 31)),
        ("C154", "T1", "IMM_3", date(2022, 1, 1), date(2022, 2, 28), "99", 99.0, None, None, date(2022, 9, 30)),
        ("C154", "T1", "IMM_3", date(2023, 1, 1), date(2023, 3, 31), "11", 11.0, None, None, date(2023, 7, 31)),
        ("C154", "T2", "IMM_3", date(2022, 1, 1), date(2022, 12, 31), "60", 60.0, None, None, date(2023, 2, 1)),
        ("C154", "T2", "IMM_3", date(2021, 1, 1), date(2021, 12, 31), "90", 90.0, None, None, date(2022, 2, 1)),
    ]

    def wanted() -> list[Row]:
        found = []
        for ccn in ("T1", "T2"):
            eligible = [row for row in held if row[1] == ccn and row[4] < date(2023, 1, 1)]
            winner = min(eligible, key=lambda row: (-row[4].toordinal(), -row[3].toordinal(), -row[9].toordinal(), row[2], row[5], row[6]))
            found.append((f"{ccn}:2023", winner[3], winner[4], winner[9], winner[5]))
        return found

    return Probe(
        "care_compare_window",
        "int_spine_care_compare_measures.sql",
        ("spine", "windows", "ratings", "periods", "candidates"),
        {
            "int_hospital_spine": (spine, [spine_row("T1:2023", "T1"), spine_row("T2:2023", "T2")]),
            "int_registry_measure_windows": (windows, held),
            "fct_hgi_release": (ratings, []),
        },
        "int_registry_measure_windows",
        "select spine_key, period_start, period_end, release_date, value_text from candidates where recency = 1 order by spine_key",
        wanted,
        (",\n                ".join(("", *CARE_TIE_BREAKS)), ""),
    )


def places_probe() -> Probe:
    places = (
        "county_fips varchar, data_year integer, data_value_published varchar, data_value double, measureid varchar, datavaluetypeid varchar, "
        "edition varchar, is_all_states boolean, places_row_key varchar"
    )
    held: list[Row] = [
        ("13001", 2022, "10.0", 10.0, "DIABETES", "AgeAdjPrv", "2022", True, "13001-2022-b"),
        ("13001", 2022, "12.0", 12.0, "DIABETES", "AgeAdjPrv", "2022", True, "13001-2022-a"),
        ("13001", 2022, "13.0", 13.0, "DIABETES", "AgeAdjPrv", "2022", True, "13001-2022-c"),
        ("13001", 2022, "7.0", 7.0, "DIABETES", "AgeAdjPrv", "2021", True, "13001-2022-z"),
        ("13001", 2020, "5.0", 5.0, "DIABETES", "AgeAdjPrv", "2021", True, "13001-2020-m"),
    ]

    def wanted() -> list[Row]:
        found = []
        for year in (2020, 2022):
            rows = [row for row in held if row[1] == year]
            newest = max(row[6] for row in rows)
            winner = min((row for row in rows if row[6] == newest), key=lambda row: row[8])
            found.append((winner[0], winner[1], winner[3]))
        return found

    return Probe(
        "places_edition",
        "int_spine_county_measures.sql",
        ("places",),
        {"fct_places_county_value": (places, held)},
        "fct_places_county_value",
        "select county_fips, data_year, value_number from places where release_rank = 1 order by data_year",
        wanted,
        (", places_row_key asc", ""),
    )


def source_probe() -> Probe:
    geos = "spine_key varchar, window_start date, geo_fips varchar"
    periods = "county_fips varchar, value_text varchar, value_number double, period_end date, measure_control varchar, measure_source varchar, field varchar"
    held: list[Row] = [
        ("13001", None, 2.0, date(2022, 12, 31), "C900", "wonder", "rate"),
        ("13001", None, 1.0, date(2022, 12, 31), "C900", "places", "rate"),
        ("13001", None, 3.0, date(2022, 12, 31), "C900", "geographic_variation", "rate"),
        ("13001", None, 9.0, date(2021, 12, 31), "C900", "mmd", "rate"),
    ]

    def wanted() -> list[Row]:
        eligible = [row for row in held if row[3] < date(2023, 1, 1)]
        winner = min(eligible, key=lambda row: (-row[3].toordinal(), row[5]))
        return [("T1:2023", winner[5], winner[3], winner[2])]

    return Probe(
        "county_source",
        "int_spine_county_measures.sql",
        ("candidates", "chosen"),
        {"spine_geos": (geos, [("T1:2023", date(2023, 1, 1), "13001")]), "control_periods": (periods, held)},
        "control_periods",
        "select spine_key, measure_source, period_end, value_number from chosen order by spine_key",
        wanted,
        (", control_periods.measure_source asc", ""),
    )


def reason_texts() -> dict[str, list[str]]:
    """The distinct domain reasons of each recipe control that states a domain or a floor, from the seed file."""
    csv = importlib.import_module("csv")
    found: dict[str, list[str]] = {}
    with RECIPE.open(newline="") as handle:
        for row in csv.DictReader(handle):
            if row["domain_reason"] and row["domain_reason"] not in found.setdefault(row["measure_control"], []):
                found[row["measure_control"]].append(row["domain_reason"])
    return found


def reason_probe() -> Probe:
    recipe = "measure_control varchar, role varchar, domain_min double, domain_max double, denominator_floor double, domain_reason varchar"
    held: list[Row] = [
        ("C900", "numerator", 0.0, 1.0, None, "a share of a total lies between 0 and 1"),
        ("C900", "denominator", None, None, 0.0, "total costs must be above 0"),
        ("C900", "value", 0.0, None, None, "the value is not below 0"),
        ("C901", "value", 0.0, None, None, "the value is not below 0"),
    ]

    def wanted() -> list[Row]:
        return [("C900", "; ".join(sorted({row[5] for row in held if row[0] == "C900"}))), ("C901", "the value is not below 0")]

    return Probe(
        "domain_reason",
        "int_cost_report_domain_checks.sql",
        ("checked_controls",),
        {"cost_report_measures_recipe": (recipe, held)},
        "cost_report_measures_recipe",
        "select measure_control, domain_reason from checked_controls order by measure_control",
        wanted,
        (" order by domain_reason", ""),
    )


def probes() -> list[Probe]:
    return [onc_probe(), care_probe(), places_probe(), source_probe(), reason_probe()]


class Report:
    """The steps of one run, printed as a JSON list."""

    def __init__(self) -> None:
        self.steps: list[dict[str, Any]] = []

    def case(self, case: str, modes: str, name: str, ok: bool, **evidence: Any) -> bool:
        """Record one check with its verdict and the evidence (names and counts only)."""
        self.steps.append({"case": case, "failure_modes": modes, "name": name, "verdict": "pass" if ok else "fail", **evidence})
        return ok


def permuted_ties(report: Report) -> None:
    """Run every probe over all orders of its tie rows, then the same with the tie-break removed from a copy of the model text."""
    duckdb = importlib.import_module("duckdb")
    for probe in probes():
        text = cte_texts(probe.model)
        wanted = json.dumps(probe.wanted(), default=str)
        found = outcomes(duckdb, probe, text)
        count = len(orders(probe.tables[probe.tie_table][1]))
        report.case(
            "E06b",
            "A8",
            f"{probe.name}: all {count} orders of the tied rows give the winner the as-of rule names",
            found == {wanted},
            orders=count,
            results=len(found),
            wanted=wanted,
            found=sorted(found)[:3],
        )
        try:
            planted = outcomes(duckdb, probe, strip_tie_break(probe))
            caught = planted != {wanted}
            detail: dict[str, Any] = {"results_without_tie_break": len(planted)}
        except RuntimeError as error:
            caught, detail = False, {"error": str(error)[:200]}
        report.case("E06b", "A8", f"{probe.name}: with the tie-break removed the orders give another winner or more than one result", caught, **detail)


def instant(value: Any) -> str:
    return value.isoformat() if isinstance(value, date) else str(value)


def built_ties(report: Report, run_folder: Path) -> None:
    """Check the tie rows of the fixture hospital in a features build against the winners of the rule."""
    duckdb = importlib.import_module("duckdb")
    rows = feature_fixture.gold_rows()
    year = feature_fixture.TIE_YEAR - 1
    key = f"{feature_fixture.TIE_CCN}:{feature_fixture.TIE_YEAR}"
    onc = [row for row in rows["fct_onc_ehr_product"] if row["ccn"] == feature_fixture.TIE_CCN]
    care = [row for row in rows["fct_timely_effective_care_window"] if row["entity_id"] == feature_fixture.TIE_CCN]
    places = [row for row in rows["fct_places_county_value"] if row["county_fips"] == feature_fixture.TIE_COUNTY and row["data_year"] == year]
    onc_winner = max(onc, key=lambda row: (row["end_date"], row["start_date"]))
    care_winner = max(care, key=lambda row: (row["window_end"], row["window_start"], row["release_date"]))
    places_winner = min(places, key=lambda row: row["places_row_key"])
    connection = duckdb.connect(str(run_folder / "features_work/features.duckdb"), read_only=True)
    try:
        found_onc = connection.execute(
            "select period_start, value_text from int_spine_operations_measures where spine_key = ? and measure_control = 'C072'", [key]
        ).fetchall()
        found_care = connection.execute(
            "select period_start, value_number from int_spine_care_compare_measures where spine_key = ? and measure_control = 'C154' and field = 'value'", [key]
        ).fetchall()
        found_places = connection.execute(
            "select value_number from int_spine_county_measures where spine_key = ? and measure_source = 'places' and field = 'DIABETES:AgeAdjPrv'", [key]
        ).fetchall()
        found_reasons = dict(connection.execute("select measure_control, domain_reason from int_cost_report_domain_checks group by all").fetchall())
    finally:
        connection.close()
    report.case(
        "E01",
        "RT1",
        "ONC: two periods ending on the same day give the one that starts later",
        found_onc == [(onc_winner["start_date"], onc_winner["developer_name"])],
        found=[(instant(a), b) for a, b in found_onc],
        wanted=[instant(onc_winner["start_date"]), onc_winner["developer_name"]],
    )
    report.case(
        "E01",
        "RT2",
        "Care Compare: two windows ending on the same day give the one that starts later",
        found_care == [(care_winner["window_start"], care_winner["score_number"])],
        found=[(instant(a), b) for a, b in found_care],
        wanted=[instant(care_winner["window_start"]), care_winner["score_number"]],
    )
    report.case(
        "E01",
        "RT3",
        "PLACES: two values of one edition give the one with the lower row key",
        found_places == [(places_winner["data_value"],)],
        found=found_places,
        wanted=places_winner["data_value"],
    )
    wanted_reasons = {control: "; ".join(sorted(reasons)) for control, reasons in reason_texts().items() if len(reasons) > 1}
    wrong = sorted(control for control, text in wanted_reasons.items() if found_reasons.get(control) != text)
    report.case(
        "E01",
        "RT4",
        "the reasons of a cost-report control with more than one are listed in alphabetical order",
        bool(wanted_reasons) and not wrong,
        controls=len(wanted_reasons),
        wrong=wrong[:5],
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the permutation probe, or check one run folder; the exit code is 1 when a check fails."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("mode", choices=("probe", "built"))
    parser.add_argument("run_folder", type=Path, nargs="?")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    report = Report()
    if args.mode == "probe":
        permuted_ties(report)
    else:
        built_ties(report, args.run_folder)
    text = json.dumps(report.steps, indent=2, sort_keys=True, default=str) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text)
    sys.stdout.write(text)
    return 1 if any(step["verdict"] == "fail" for step in report.steps) else 0


if __name__ == "__main__":
    raise SystemExit(main())
