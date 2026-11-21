"""Generate the Provider of Services (POS) file periods that staging reads (failure modes 280 and 281).

Run from the repository root with read-only S3 access (no local acquisition record is read):

    .venv/bin/python -m scripts.lakehouse.pos_file_periods           # rewrite the seed
    .venv/bin/python -m scripts.lakehouse.pos_file_periods --check   # rebuild and compare with the committed seed

Each loaded POS file's period is the temporal coverage the data.cms.gov catalog gave its distribution, as its
capture receipt, read from S3 by key and version, recorded it; file names are never parsed. A loaded file whose release has no recorded coverage
stops the run. Failure modes: plans/hospital_spine_20261005/failure_modes.md.
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from scripts.lakehouse import period_receipts

REPO_ROOT = Path(__file__).resolve().parents[2]
SEED = REPO_ROOT / "dbt/seeds/pos_file_periods.csv"
TABLE = "cms_provider_of_services"
COLUMNS = ("member_sha256", "release_id", "file_name", "period_start", "period_end")
TEMPORAL = re.compile(r'"catalog_temporal": "(\d{4}-\d{2}-\d{2})/(\d{4}-\d{2}-\d{2})"')


class PeriodError(ValueError):
    """A loaded POS file has no recorded catalog coverage, or two."""


def receipt_periods(receipts: Iterable[Mapping[str, Any]]) -> dict[str, tuple[str, str]]:
    """Return each POS capture's catalog coverage from its receipt, keyed by release ID."""
    periods: dict[str, tuple[str, str]] = {}
    for receipt in receipts:
        labels = [period.get("label") or "" for period in receipt.get("measurement_periods", [])]
        found = {match.groups() for label in labels if (match := TEMPORAL.search(label))}
        if not found:
            continue
        release = str(receipt["snapshot_id"])
        if len(found) > 1:
            raise PeriodError(f"release {release} records more than one catalog coverage")
        start, end = found.pop()
        coverage = (start, end)
        if periods.setdefault(release, coverage) != coverage:
            raise PeriodError(f"release {release} has two catalog coverages")
    return periods


def rows_for(loaded: Iterable[Mapping[str, str]], periods: Mapping[str, tuple[str, str]]) -> list[dict[str, str]]:
    """Return one seed row per loaded POS file, sorted by period; a file without coverage stops the run [281]."""
    rows = []
    for item in loaded:
        if item["release_id"] not in periods:
            raise PeriodError(f"POS file {item['file_name']} (release {item['release_id']}) has no recorded catalog coverage")
        start, end = periods[item["release_id"]]
        rows.append(
            {"member_sha256": item["sha256"], "release_id": item["release_id"], "file_name": item["file_name"], "period_start": start, "period_end": end}
        )
    return sorted(rows, key=lambda row: (row["period_end"], row["member_sha256"]))


def build() -> list[dict[str, str]]:
    """Return the seed rows from the loaded files and the receipts that their S3 manifests list."""
    files, receipts = period_receipts.loaded((TABLE,), (TABLE,))
    return rows_for(files, receipt_periods(receipts.get(TABLE, [])))


def as_csv(rows: Iterable[Mapping[str, str]]) -> str:
    """Return the rows as the seed's CSV text."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def main() -> int:
    """Rebuild the POS periods and write or check the committed seed."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="compare with the committed seed instead of writing it")
    args = parser.parse_args()
    try:
        text = as_csv(build())
    except PeriodError as error:
        sys.stderr.write(f"pos file periods: {error}\n")
        return 1
    if args.check:
        same = SEED.exists() and SEED.read_text() == text
        sys.stdout.write(f"pos file periods: {'committed seed reproduced' if same else 'committed seed differs from storage'}\n")
        return 0 if same else 1
    SEED.write_text(text)
    sys.stdout.write(f"pos file periods: {text.count(chr(10)) - 1} files\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
