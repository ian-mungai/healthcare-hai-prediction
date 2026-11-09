"""Hospital-window and derived column checks on a real features build of the synthetic fixture, called by the feature store build runner.

    .venv/bin/python -m scripts.features.derived_column_cases <run folder> [--output <report.json>]

The run folder is one that ``features_build`` filled from the fixture rows of ``feature_fixture`` (hospitals 990009, 990010 and 990011 and the rows
``derived_rows`` writes). The expected value of every column in every case is worked out below from the fixture numbers and the arithmetic of design 2.5, 2.6,
2.10 and 2.11, not from the models: a model that ignores the window start, fills a gap year, drops the true zero of C069 or logs a value that is not above 0
fails here. The build itself runs the dbt tests ``assert_spine_pos_derived_fields``, ``assert_spine_cmi_derived_fields``, ``assert_hospital_derived_fields``,
``assert_care_compare_derived_fields``, ``assert_county_derived_fields`` and ``assert_chow_zero_only_inside_register``; these cases add the exact numbers.
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

CCN = feature_fixture.DERIVED_CCN
GAP = feature_fixture.GAP_CCN
NEGATIVE = feature_fixture.NEGATIVE_CCN
CONFLICT = feature_fixture.CONFLICT_CCN
# The wide hospital-window columns by spine key: (ORs per 100 beds, log beds, ownership class, log CMI cases, CMI change); None is a null.
WINDOW = {
    f"{CCN}:2022": (5.0, math.log(200), "for_profit", math.log(1000.0), None),
    f"{CCN}:2023": (None, None, "nonprofit", None, 1.8 - 1.5),
    f"{CCN}:2024": (0.0, math.log(150), "government", None, None),
}
# The derived hospital columns by spine key and column name. Report values: 990009 has FY2020 (denominator 20000, share 500), FY2021 (10000, 2000) and FY2022
# (denominator 0); 990010 has FY2019 (10000, 1000) and FY2022 (10000, 3000); 990011 has FY2022 (-5, 100). C038 is the denominator, C057 is 1, C054 and C045 are
# the share, C040 is 10000 over the denominator and C028 is 10 over the denominator per 365 days.
CHANGE_2022 = {
    "c054": 2000 / 10000 - 500 / 20000,
    "c045": 2000 / 10000 - 500 / 20000,
    "c040": 10000 / 10000 - 10000 / 20000,
    "c028": 10 / (10000 / 365) - 10 / (20000 / 365),
}
HOSPITAL: dict[str, dict[str, float | None]] = {
    f"{CCN}:2022": {"c038__value_log": math.log(10000), "c057__value_log": 0.0, **{f"{key}__value_change": value for key, value in CHANGE_2022.items()}},
    f"{CCN}:2023": {"c038__value_log": None, "c057__value_log": None, **{f"{key}__value_change": None for key in CHANGE_2022}},
    f"{CCN}:2024": {"c038__value_log": None, "c057__value_log": None, **{f"{key}__value_change": None for key in CHANGE_2022}},
    f"{GAP}:2022": {"c038__value_log": math.log(10000), "c057__value_log": 0.0, **{f"{key}__value_change": None for key in CHANGE_2022}},
    f"{GAP}:2023": {"c038__value_log": math.log(10000), "c057__value_log": 0.0, **{f"{key}__value_change": None for key in CHANGE_2022}},
    f"{NEGATIVE}:2023": {"c038__value_log": None, "c057__value_log": None, **{f"{key}__value_change": None for key in CHANGE_2022}},
    # The reports of 2021 disagree on the shares (C045, C054) and agree on occupancy and FTE (C040, C028), which do not read the numerator.
    f"{CONFLICT}:2023": {
        "c038__value_log": math.log(10000),
        "c057__value_log": 0.0,
        "c054__value_change": None,
        "c045__value_change": None,
        "c040__value_change": 0.0,
        "c028__value_change": 0.0,
    },
}
# The occupational-mix survey change of RN hours: no earlier survey for 2022, then 11.5 minus 10.0 for the next two windows.
RNHR_CHANGE = {f"{CCN}:2022": None, f"{CCN}:2023": 1.5, f"{CCN}:2024": 1.5}
# Care Compare: minutes 120, 0 and -3 give a log, null and null; the volume Low, Medium and Not Available gives 1, 2 and null; 990001 reports Very High (4).
CARE = {
    f"{CCN}:2022": (math.log(120), 1.0),
    f"{CCN}:2023": (None, 2.0),
    f"{CCN}:2024": (None, None),
    "990001:2022": (None, 4.0),
}
# County context density: ln(50000 / 100), ln(60000 / 100) and a zero area; county measures ordinal of the code 3 (990009) and of the code 0 (990005).
DENSITY = {f"{CCN}:2022": math.log(500), f"{CCN}:2023": math.log(600), f"{CCN}:2024": None}
RUCC = {f"{CCN}:2022": 3.0, f"{CCN}:2023": 3.0, f"{CCN}:2024": 3.0, "990005:2022": None}
# C069 and its flag: the register starts on 2021-06-01, so a window whose 12 months start earlier is missing; 990009 has two events in 2022 (one 366 days before
# the start of 2024), one on the start of 2024 and one exactly 12 months before it, which counts.
CHOW = {
    f"{CCN}:2022": (None, None),
    f"{CCN}:2023": (2.0, 1.0),
    f"{CCN}:2024": (1.0, 1.0),
    "990001:2022": (None, None),
    "990001:2023": (0.0, 0.0),
    "990001:2024": (0.0, 0.0),
    "990007:2018": (None, None),
    "990002:2023": (0.0, 0.0),
}
WINDOW_SQL = (
    "select spine_key, or_per_100_beds, log_bed_count, ownership_class, cmi_cases_log, cmi_change "
    "from feature_matrix_hospital_window where spine_key like '9900%'"
)
WIDE_SQL = "select spine_key, {columns} from {table}"


def same(found: float | None, wanted: float | None) -> bool:
    """Whether two values agree: both null, or both numbers equal to 12 digits."""
    if found is None or wanted is None:
        return found is None and wanted is None
    return math.isclose(found, wanted, rel_tol=1e-12, abs_tol=1e-12)


def window_cases(report: Any, connection: Any) -> None:
    """The hospital-window columns: ORs per 100 beds, log beds, ownership class, log CMI cases and CMI change."""
    found = {row[0]: row[1:] for row in connection.execute(WINDOW_SQL).fetchall()}
    wrong = sorted(
        f"{key} {name}"
        for key, wanted in WINDOW.items()
        for name, got, want in zip(
            ("or_per_100_beds", "log_bed_count", "ownership_class", "cmi_cases_log", "cmi_change"), found.get(key, (None,) * 5), wanted, strict=True
        )
        if not (got == want if isinstance(want, str) else same(got, want))
    )
    report.case(
        "E01",
        "P3-1",
        "the window table gives the hand-worked POS and CMI columns of every planted window",
        not wrong and set(WINDOW) <= set(found),
        wrong=wrong[:6],
    )
    report.case(
        "E01",
        "P3-2",
        "a snapshot that ends on the window start is not read (2023: the snapshot of 2022-12-31 gives 0 beds and a missing ratio, not the row of 999 beds)",
        found.get(f"{CCN}:2023", (1,))[0] is None and found.get(f"{CCN}:2023", (None, 1))[1] is None,
    )
    columns = {
        row[0]
        for row in connection.execute("select column_name from information_schema.columns where table_name = 'feature_matrix_hospital_window'").fetchall()
    }
    report.case("E01", "P3-3", "RNs per bed is not a column of the window table", "rn_per_bed" not in columns)


def derived_cases(report: Any, connection: Any) -> None:
    """The log, change, ordinal and computed columns of the wide tables."""
    for table, wanted, label, tag in (
        (
            "feature_matrix_hospital",
            HOSPITAL,
            "log and change fields of the cost report (a gap year, a single report and a zero or negative value give null)",
            "P3-4",
        ),
        ("feature_matrix_hospital", {key: {"c030__rnhr_change": value} for key, value in RNHR_CHANGE.items()}, "the occupational-mix survey change", "P3-5"),
        (
            "feature_matrix_care_compare",
            {key: {"c143__value_log": a, "c141__value_ordinal": b} for key, (a, b) in CARE.items()},
            "the log of minutes and the ordinal volume",
            "P3-6",
        ),
        (
            "feature_matrix_county_context",
            {key: {"c191__density_log": value} for key, value in DENSITY.items()},
            "the density log, null for a zero area",
            "P3-7",
        ),
        (
            "feature_matrix_county_measures",
            {key: {"c198__rucc_code_ordinal": value} for key, value in RUCC.items()},
            "the continuum code ordinal, null for the code 0",
            "P3-8",
        ),
        (
            "feature_matrix_operations",
            {key: {"c069__value": count, "c069__any_event": flag} for key, (count, flag) in CHOW.items()},
            "C069 with a true zero from the register start and its any-event flag",
            "P3-9",
        ),
    ):
        names = sorted({f"{name}__number" for entry in wanted.values() for name in entry})
        rows = {row[0]: dict(zip(names, row[1:], strict=True)) for row in connection.execute(WIDE_SQL.format(columns=", ".join(names), table=table)).fetchall()}
        wrong = sorted(
            f"{key} {name}" for key, entry in wanted.items() for name, want in entry.items() if not same(rows.get(key, {}).get(f"{name}__number"), want)
        )
        report.case("E01", tag, f"{table}: {label}", not wrong and set(wanted) <= set(rows), wrong=wrong[:6])


def built_derived(report: Any, run_folder: Path) -> None:
    """Check every planted case against the built tables of a run folder."""
    duckdb = importlib.import_module("duckdb")
    connection = duckdb.connect(str(run_folder / "features_work/features.duckdb"), read_only=True)
    try:
        window_cases(report, connection)
        derived_cases(report, connection)
    finally:
        connection.close()


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
    built_derived(report, args.run_folder)
    text = json.dumps(report.steps, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text)
    sys.stdout.write(text)
    return 1 if any(step["verdict"] == "fail" for step in report.steps) else 0


if __name__ == "__main__":
    raise SystemExit(main())
