"""Write the measure-ID domains of the published measure keys that the gold bridges name, from bronze.

Domains: PLACES, GV, SVI, DRG, HCAHPS, timely care, maternal health, unplanned visits, complications and deaths, HRRP and MMD.

Run from the repository root that owns the running catalog (read-only; it starts and stops nothing):

    .venv/bin/python -m scripts.lakehouse.catalog up
    .venv/bin/python -m scripts.lakehouse.measure_domains --root <root with query.sh> [--output <seed.csv>] [--check]
    .venv/bin/python -m scripts.lakehouse.catalog down

The domains come from data already in bronze, with no new acquisition and no registry or other
prose. The only input is ``scripts/lakehouse/query.sh`` (DuckDB with bronze attached read-only) running ``SELECT``
statements. Two kinds of domain:

- wide (SVI, GV): ``published_key`` is the bronze column name exactly as ``bronze_dictionary.<table>`` lists it and
  ``published_headers`` holds the publisher headers behind it, joined with ``|``;
- long (every other domain): ``published_key`` is a distinct published value of the identifier column, kept exactly
  (no case folding, no trimming) and ``published_headers`` is empty.

``first_release`` and ``last_release`` are the earliest and latest release in which the key appears (a long key has a
row with that value; a wide key has a column holding at least one stored value). A release is the bronze release date
when ``_release_id`` is a date and otherwise the capture id (``_snapshot_id``), which sorts by capture time; a table
that mixes both kinds stops the run. Failure modes: /tmp/measure_domains/failure_modes.md. The seed is written only when its bytes change.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command

REPO_ROOT = Path(__file__).resolve().parents[2]
SEED = REPO_ROOT / "dbt/seeds/gold_measure_id_domains.csv"
COLUMNS = ("domain", "published_key", "published_headers", "source_table", "source_column", "first_release", "last_release")
RELEASE_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
CAPTURE_ID = re.compile(r"__\d{8}T\d{6}Z__")
NAME = re.compile(r"[a-z0-9_]+")
HEADER_SEPARATOR = "|"
QUERY_TIMEOUT_SECONDS = 1800


class DomainError(RuntimeError):
    """A domain cannot be built exactly; the message names the table, column or command."""


@dataclass(frozen=True)
class Domain:
    """One domain: the reviewed bronze table, its kind and, for a long domain, the identifier column."""

    name: str
    table: str
    wide: bool
    column: str = ""


# Reviewed sources. SVI and GV are wide: B1's fields name bronze columns. The others are long: B1's
# keys are values of the identifier column. Each Care Compare domain is the hospital table its silver window model reads
# (HRRP identifies a measure by Measure Name; MMD by the published condition label).
DOMAINS = (
    Domain("COMPLICATIONS", "cms_cc_complications_and_deaths_hospital", False, "measure_id"),
    Domain("DRG", "cms_medicare_inpatient_by_drg", False, "drg_cd"),
    Domain("GV", "cms_geographic_variation_csv", True),
    Domain("HCAHPS", "cms_cc_hcahps_hospital", False, "hcahps_measure_id"),
    Domain("HRRP", "cms_cc_hospital_readmissions_reduction_program_hospital", False, "measure_name"),
    Domain("MATERNAL", "cms_cc_maternal_health_hospital", False, "measure_id"),
    Domain("MMD", "cms_mmd_csv", False, "condition"),
    Domain("PLACES", "places", False, "measureid"),
    Domain("SVI", "svi", True),
    Domain("TIMELY", "cms_cc_timely_and_effective_care_hospital", False, "measure_id"),
    Domain("UNPLANNED", "cms_cc_unplanned_hospital_visits_hospital", False, "measure_id"),
)


def query(root: Path, sql: str) -> list[dict[str, Any]]:
    """Run one read-only statement through query.sh and return its rows; any failure stops with the command."""
    if not re.match(r"\s*(select|with)\b", sql, re.IGNORECASE):
        raise DomainError(f"only SELECT statements may run: {sql[:60]!r}")
    program = str(root / "scripts/lakehouse/query.sh")
    try:
        result = run_command(program, ["-json", "-c", sql], cwd=root, timeout=QUERY_TIMEOUT_SECONDS)
    except TimeoutExpired as error:
        raise DomainError(f"{program} -json -c ... timed out after {QUERY_TIMEOUT_SECONDS} seconds") from error
    if result.returncode:
        raise DomainError(f"{program} -json -c ... exited {result.returncode}: {(result.stderr or result.stdout)[-600:]}")
    lines = result.stdout.splitlines()
    starts = [index for index, line in enumerate(lines) if line.startswith("[")]
    if not starts:
        raise DomainError(f"query.sh printed no JSON rows for: {sql[:80]!r}")
    rows = json.loads("\n".join(lines[starts[0] :]))
    if not isinstance(rows, list):
        raise DomainError(f"query.sh returned a non-list for: {sql[:80]!r}")
    return rows


def identifier(name: str) -> str:
    """Return a double-quoted SQL identifier after checking it holds only lower-case letters, digits and underscores."""
    if not NAME.fullmatch(name):
        raise DomainError(f"unsafe or unexpected bronze name {name!r}")
    return f'"{name}"'


def release_of(release_id: str | None, snapshot_id: str | None) -> str:
    """Return the release label: the release id when it is a date, else the capture id (snapshot id), which sorts by capture time."""
    if release_id and RELEASE_DATE.fullmatch(release_id):
        return release_id
    if snapshot_id and CAPTURE_ID.search(snapshot_id):
        return snapshot_id
    raise DomainError(f"cannot read a release from release {release_id!r} and snapshot {snapshot_id!r}")


def check_release_kinds(table: str, labels: set[str]) -> None:
    """Stop when a table mixes dated releases with capture ids, whose text order would not be time order."""
    if len({bool(RELEASE_DATE.fullmatch(label)) for label in labels}) > 1:
        raise DomainError(f"{table} mixes dated releases and capture ids")


def dictionary_rows(root: Path, domain: Domain) -> list[dict[str, Any]]:
    """Return the published columns of the domain's table from its bronze_dictionary table, in published order."""
    source = "lakehouse.bronze_dictionary." + identifier(domain.table)
    rows = query(root, " ".join(["select position, column_name, original_headers from", source, "order by position"]))
    if not rows:
        raise DomainError(f"bronze_dictionary.{domain.table} is missing or empty")
    return rows


def wide_rows(root: Path, domain: Domain) -> list[dict[str, str]]:
    """Return one row per published column of a wide table with the headers behind it and its release range."""
    columns = dictionary_rows(root, domain)
    names = [str(row["column_name"]) for row in columns]
    if len(set(names)) != len(names):
        raise DomainError(f"bronze_dictionary.{domain.table} lists a column twice")
    counts = ", ".join(f"count({identifier(name)}) as c{index}" for index, name in enumerate(names))
    table = identifier(domain.table)
    stored = query(root, " ".join(["select _release_id, _snapshot_id,", counts, "from", table, "group by all"]))
    releases: dict[int, list[str]] = {index: [] for index in range(len(names))}
    check_release_kinds(domain.table, {release_of(row["_release_id"], row["_snapshot_id"]) for row in stored})
    for row in stored:
        release = release_of(row["_release_id"], row["_snapshot_id"])
        for index in range(len(names)):
            if int(row[f"c{index}"]) > 0:
                releases[index].append(release)
    output = []
    for index, row in enumerate(columns):
        headers = [str(header) for header in row["original_headers"] or []]
        if not headers or any(HEADER_SEPARATOR in header or not header for header in headers):
            raise DomainError(f"{domain.table}.{names[index]}: published headers {headers!r} are missing, empty or contain {HEADER_SEPARATOR!r}")
        seen = releases[index]
        output.append(
            {
                "domain": domain.name,
                "published_key": names[index],
                "published_headers": HEADER_SEPARATOR.join(headers),
                "source_table": domain.table,
                "source_column": names[index],
                "first_release": min(seen) if seen else "",
                "last_release": max(seen) if seen else "",
            }
        )
    return output


def long_rows(root: Path, domain: Domain) -> list[dict[str, str]]:
    """Return one row per distinct published value of a long table's identifier column with its release range."""
    published = {str(row["column_name"]): row["original_headers"] for row in dictionary_rows(root, domain)}
    if not published.get(domain.column):
        raise DomainError(f"bronze_dictionary.{domain.table} does not list column {domain.column!r} with a published header")
    column, table = identifier(domain.column), identifier(domain.table)
    stored = query(root, " ".join(["select", column, "as published_key, _release_id, _snapshot_id from", table, "group by all"]))
    ranges: dict[str, list[str]] = {}
    check_release_kinds(domain.table, {release_of(row["_release_id"], row["_snapshot_id"]) for row in stored})
    for row in stored:
        key = row["published_key"]
        if not isinstance(key, str) or not key or key != key.strip() or key != key.lstrip("﻿"):
            raise DomainError(f"{domain.table}.{domain.column}: key {key!r} is null, empty or carries whitespace or a byte order mark")
        ranges.setdefault(key, []).append(release_of(row["_release_id"], row["_snapshot_id"]))
    if not ranges:
        raise DomainError(f"{domain.table}.{domain.column} holds no values")
    return [
        {
            "domain": domain.name,
            "published_key": key,
            "published_headers": "",
            "source_table": domain.table,
            "source_column": domain.column,
            "first_release": min(seen),
            "last_release": max(seen),
        }
        for key, seen in ranges.items()
    ]


def build(root: Path) -> list[dict[str, str]]:
    """Return every domain row, sorted by the bytes of (domain, published key, source table, source column)."""
    rows: list[dict[str, str]] = []
    for domain in DOMAINS:
        rows.extend(wide_rows(root, domain) if domain.wide else long_rows(root, domain))
    identity = [(row["domain"], row["published_key"], row["source_table"], row["source_column"]) for row in rows]
    if len(set(identity)) != len(identity):
        raise DomainError("a (domain, published_key, source_table, source_column) combination repeats")
    return sorted(rows, key=lambda row: tuple(row[name].encode() for name in ("domain", "published_key", "source_table", "source_column")))


def render(rows: list[dict[str, str]]) -> str:
    """Return the seed text: ``\\n`` line endings, minimal quoting, no timestamp."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def write_seed(path: Path, text: str, rows: list[dict[str, str]]) -> str:
    """Write the seed atomically after a read-back check; leave an identical file untouched. Return what happened."""
    parsed = list(csv.DictReader(io.StringIO(text)))
    if parsed != rows:
        raise DomainError("the rendered seed does not parse back to the queried rows")
    if path.is_file() and path.read_text() == text:
        return "unchanged"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(text)
    os.replace(temporary, path)
    return "written"


def main() -> int:
    """Build the domains and write the seed, or with --check compare it."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n", maxsplit=1)[0])
    parser.add_argument("--root", type=Path, default=REPO_ROOT, help="repository root whose query.sh and running catalog hold bronze")
    parser.add_argument("--output", type=Path, default=SEED, help="seed file to write")
    parser.add_argument("--check", action="store_true", help="write nothing; exit 1 when the seed differs from bronze")
    args = parser.parse_args()
    try:
        rows = build(args.root.resolve())
        text = render(rows)
        if args.check:
            current = args.output.read_text() if args.output.is_file() else None
            sys.stdout.write(f"{args.output}: {'matches bronze' if current == text else 'differs from bronze'}\n")
            return 0 if current == text else 1
        outcome = write_seed(args.output, text, rows)
    except DomainError as error:
        sys.stderr.write(f"measure_domains: {error}\n")
        return 1
    counts = {domain.name: sum(row["domain"] == domain.name for row in rows) for domain in DOMAINS}
    sys.stdout.write(f"{args.output} {outcome}: {len(rows)} rows {json.dumps(counts, sort_keys=True)}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
