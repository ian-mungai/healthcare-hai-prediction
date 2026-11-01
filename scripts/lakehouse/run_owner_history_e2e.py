"""E2E check of the owner history dimension on synthetic rows: the silver period model and the gold table that keys it.

Run from the repository root, no Docker, no network and no real data:

    .venv/bin/python -m scripts.lakehouse.run_owner_history_e2e

The delivered model files are rendered with stand-in refs and run in an in-memory DuckDB over synthetic owner and enrollment rows. Each case
names one rule: periods of one hospital, owner and role never overlap, one current row per open period, an owner change closes one row and
opens one, one missing release is bridged and a longer gap splits the period, a rerun on the same rows changes nothing, a correction that
removes a period removes its rows, an owner row not typed as an organisation gets no period, and no name or contact column reaches the table.
The report goes to ``data/e2e/owner_history/``.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
from jinja2 import Environment, StrictUndefined, select_autoescape

from scripts.lakehouse import gold_publish

ROOT = Path(__file__).resolve().parents[2]
REPORTS = ROOT / "data/e2e/owner_history"
SILVER = "int_hospital_owner_periods"
GOLD = "dim_hospital_owner_history"
RELEASES = [date(2026, month, 28) for month in range(1, 7)]
# Columns the table may hold: keys, the three identities and the period. Names, addresses, flags and shares stay on the release rows.
ALLOWED_GOLD_COLUMNS = {
    "hospital_owner_history_sk",
    "ccn",
    "associate_id_owner",
    "role_code",
    "valid_from",
    "valid_to",
    "is_current",
    "bridged_releases",
}
# (enrollment_id, ccn) of the synthetic enrollments; E9 has no CCN.
ENROLLMENTS = [(f"E{n}", f"01000{n}" if n != 9 else None) for n in range(1, 10)]
# (enrollment_id, associate_id_owner, role_code, release numbers 1 to 6 that list it)
OWNERS = [
    ("E1", "1000000001", "34", [1, 2, 3, 4, 5, 6]),
    ("E2", "1000000002", "34", [1, 2, 3]),
    ("E2", "1000000003", "34", [4, 5, 6]),
    ("E3", "1000000004", "34", [1, 2, 4, 5, 6]),
    ("E4", "1000000005", "34", [1, 5, 6]),
    ("E5", "1000000006", "34", [1, 2, 3, 4, 5, 6]),
    ("E5", "1000000006", "34", [3]),
    ("E9", "1000000007", "34", [1, 2, 3, 4, 5, 6]),
]
# A row typed as an individual: stored rows are organisations only, and the period model must keep it so if such a row ever arrives.
INDIVIDUAL = ("E6", "1000000008", "34", [1, 2, 3, 4, 5, 6])
# Personal names and contacts the owner file carries; the table must not hold any of them.
PERSONAL = "Synthetic Person Name"


def render(name: str, path: Path) -> str:
    env = Environment(undefined=StrictUndefined, autoescape=select_autoescape(default_for_string=False))
    return env.from_string(path.read_text()).render(ref=lambda model: f'"{model}"', config=lambda **_kwargs: "")


def load(connection: duckdb.DuckDBPyConnection, owners: list[tuple[str, str, str, list[int]]]) -> None:
    connection.execute("create or replace table int_hospital_enrollment_rows (enrollment_id varchar, ccn varchar, period_end date)")
    connection.execute(
        "create or replace table int_hospital_owner_rows "
        "(enrollment_id varchar, associate_id_owner varchar, role_code varchar, period_end date, organization_name_owner varchar, type_owner varchar)"
    )
    for number, release in enumerate(RELEASES, start=1):
        for enrollment_id, ccn in ENROLLMENTS:
            connection.execute("insert into int_hospital_enrollment_rows values (?, ?, ?)", [enrollment_id, ccn, release])
        for enrollment_id, owner, role, listed in owners:
            if number in listed:
                connection.execute("insert into int_hospital_owner_rows values (?, ?, ?, ?, ?, 'O')", [enrollment_id, owner, role, release, PERSONAL])
        if number in INDIVIDUAL[3]:
            connection.execute("insert into int_hospital_owner_rows values (?, ?, ?, ?, ?, 'I')", [*INDIVIDUAL[:3], release, PERSONAL])


def build(owners: list[tuple[str, str, str, list[int]]]) -> tuple[duckdb.DuckDBPyConnection, list[dict[str, Any]], list[dict[str, Any]]]:
    connection = duckdb.connect(":memory:")
    load(connection, owners)
    connection.execute(f'create table "{SILVER}" as ' + render(SILVER, ROOT / f"dbt/models/silver/intermediate/{SILVER}.sql"))
    connection.execute(f'create table "{GOLD}" as ' + render(GOLD, ROOT / f"dbt/models/gold/dimensions/{GOLD}.sql"))

    def rows(query: str) -> list[dict[str, Any]]:
        cursor = connection.execute(query)
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]

    return connection, rows("select * from int_hospital_owner_periods order by all"), rows("select * from dim_hospital_owner_history order by all")


def periods(rows: list[dict[str, Any]], ccn: str, owner: str) -> list[tuple[date, date | None, bool]]:
    return sorted((row["valid_from"], row["valid_to"], row["is_current"]) for row in rows if row["ccn"] == ccn and row["associate_id_owner"] == owner)


def check(results: dict[str, bool], name: str, condition: bool) -> None:
    results[name] = bool(condition)


def run() -> dict[str, bool]:
    results: dict[str, bool] = {}
    connection, silver, gold = build(OWNERS)
    ordinary = [row for row in gold if row["ccn"] != "<unknown>"]

    keys: dict[tuple[str, str, str], list[tuple[date, date | None]]] = {}
    for row in silver:
        keys.setdefault((row["ccn"], row["associate_id_owner"], row["role_code"]), []).append((row["valid_from"], row["valid_to"]))
    check(
        results,
        "periods_of_one_key_do_not_overlap",
        all(
            a[1] is not None and a[1] <= b[0]
            for pairs in keys.values()
            for a, b in zip(sorted(pairs, key=lambda p: p[0]), sorted(pairs, key=lambda p: p[0])[1:], strict=False)
        ),
    )
    check(results, "periods_have_positive_length", all(row["valid_to"] is None or row["valid_from"] < row["valid_to"] for row in silver))
    check(
        results,
        "one_current_row_per_open_period",
        all(sum(1 for row in silver if (row["ccn"], row["associate_id_owner"], row["role_code"]) == key and row["is_current"]) <= 1 for key in keys),
    )
    check(results, "current_means_open_end", all(row["is_current"] == (row["valid_to"] is None) for row in silver))
    check(results, "unbroken_listing_is_one_open_period", periods(silver, "010001", "1000000001") == [(RELEASES[0], None, True)])
    check(
        results,
        "owner_change_closes_one_row_and_opens_one",
        periods(silver, "010002", "1000000002") == [(RELEASES[0], RELEASES[3], False)]
        and periods(silver, "010002", "1000000003") == [(RELEASES[3], None, True)],
    )
    check(results, "one_missing_release_is_bridged", periods(silver, "010003", "1000000004") == [(RELEASES[0], None, True)])
    check(results, "bridged_release_is_counted", [row["bridged_releases"] for row in silver if row["ccn"] == "010003"] == [1])
    check(results, "longer_gap_splits_the_period", periods(silver, "010004", "1000000005") == [(RELEASES[0], RELEASES[1], False), (RELEASES[4], None, True)])
    check(results, "repeated_listing_in_one_release_is_one_period", periods(silver, "010005", "1000000006") == [(RELEASES[0], None, True)])
    check(results, "enrollment_without_ccn_has_no_period", not any(row["associate_id_owner"] == "1000000007" for row in silver))
    check(results, "owner_row_not_typed_organisation_has_no_period", not any(row["associate_id_owner"] == INDIVIDUAL[1] for row in silver))

    check(results, "gold_columns_are_the_allowed_set", all(set(row) == ALLOWED_GOLD_COLUMNS for row in gold))
    check(results, "no_name_or_contact_in_gold", PERSONAL not in json.dumps(gold, default=str))
    check(results, "gold_key_unique", len({row["hospital_owner_history_sk"] for row in gold}) == len(gold))
    check(results, "gold_one_unknown_member", sum(1 for row in gold if row["ccn"] == "<unknown>") == 1)
    check(results, "gold_rows_equal_silver_rows_plus_unknown", len(ordinary) == len(silver))

    _, silver_again, gold_again = build(OWNERS)
    check(results, "rerun_on_same_rows_changes_no_rows", silver_again == silver and gold_again == gold)

    corrected = [item for item in OWNERS if item[1] != "1000000005"]
    _, _, gold_corrected = build(corrected)
    removed = {row["hospital_owner_history_sk"] for row in gold} - {row["hospital_owner_history_sk"] for row in gold_corrected}
    added = {row["hospital_owner_history_sk"] for row in gold_corrected} - {row["hospital_owner_history_sk"] for row in gold}
    check(results, "correction_removes_exactly_the_periods_of_the_removed_listing", len(removed) == 2 and not added)
    check(results, "publish_merge_deletes_rows_missing_from_the_new_build", "WHEN NOT MATCHED BY SOURCE THEN DELETE" in gold_publish.MERGE)

    tests = {name: render(name, ROOT / f"dbt/tests/assert_dim_hospital_owner_history_{name}.sql") for name in ("single_current_version", "no_overlap")}
    check_conn = build(OWNERS)[0]
    check(results, "dbt_tests_pass_on_the_built_table", all(not check_conn.execute(sql).fetchall() for sql in tests.values()))
    # A second current period that starts inside the first one.
    check_conn.execute(
        "insert into dim_hospital_owner_history "
        "select 'x', ccn, associate_id_owner, role_code, date '2026-03-01', valid_to, is_current, bridged_releases "
        "from dim_hospital_owner_history where ccn = '010001'"
    )
    check(results, "dbt_tests_fail_on_a_duplicated_current_period", all(check_conn.execute(sql).fetchall() for sql in tests.values()))
    contract = json.loads((ROOT / "data_contracts/lakehouse/gold_contract.json").read_text())
    entry = contract["publish"]["tables"].get(GOLD, {})
    check(
        results,
        "contract_merge_key_is_the_history_key",
        entry.get("merge_key") == ["hospital_owner_history_sk"],
    )
    connection.close()
    return results


def main() -> int:
    started = datetime.now(UTC)
    try:
        results = run()
        error = ""
    except Exception as exc:
        results, error = {}, f"{type(exc).__name__}: {exc}"
    REPORTS.mkdir(parents=True, exist_ok=True)
    report = {"started_at": started.isoformat(), "mode": "synthetic rows, in-memory DuckDB, no Docker", "error": error, "checks": results}
    (REPORTS / f"report_{started.strftime('%Y%m%dT%H%M%SZ')}.json").write_text(json.dumps(report, indent=1) + "\n")
    failed = [name for name, ok in results.items() if not ok]
    sys.stdout.write(f"{len(results) - len(failed)} of {len(results)} checks pass{'; error: ' + error if error else ''}\n")
    for name in failed:
        sys.stdout.write(f"FAIL {name}\n")
    return 1 if failed or error else 0


if __name__ == "__main__":
    sys.exit(main())
