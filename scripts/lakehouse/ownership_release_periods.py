"""Generate the release periods of the owner, enrollment and change-of-ownership files that staging reads (failure mode 365).

Run from the repository root with read-only S3 access and the local acquisition records:

    .venv/bin/python -m scripts.lakehouse.ownership_release_periods           # rewrite the seed
    .venv/bin/python -m scripts.lakehouse.ownership_release_periods --check   # rebuild and compare with the committed seed

Each loaded file's period and release label are the publisher-stated catalog period its capture receipt records: the
enrollment and change-of-ownership receipts in the acquisition job folders and the owner receipts in the owners batches.
File names are never parsed. A loaded file whose release has no recorded period, or two, stops the run. Failure modes:
data/lakehouse_planning/group_b_20261005/failure_modes_b5a.md.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SEED = REPO_ROOT / "dbt/seeds/ownership_release_periods.csv"
DATASETS = REPO_ROOT / "data/datasets"
RECEIPTS = (
    "acquisition_batches/*/jobs/ENROLL_file_*/captures/*/*/receipt.json",
    "acquisition_batches/*/jobs/CMS_CHOW_file_*/captures/*/*/receipt.json",
    "historical_acquisition/cms_hospital_owners/batches/*/capture/receipt.json",
)
TABLES = ("cms_hospital_owners", "cms_hospital_enrollments", "cms_change_of_ownership")
COLUMNS = ("member_sha256", "bronze_table", "release_id", "release_label", "period_start", "period_end")


class PeriodError(ValueError):
    """A loaded file has no recorded publisher period, or a release records two."""


def receipt_period(receipt: Mapping[str, object]) -> tuple[str, str, str] | None:
    """Return a receipt's release label and publisher-stated period, or None when it states none."""
    listed = receipt.get("measurement_periods")
    stated = {
        (str(period["start_date"]), str(period["end_date"]))
        for period in (listed if isinstance(listed, list) else [])
        if isinstance(period, dict) and period.get("source_basis") == "publisher_stated" and period.get("start_date") and period.get("end_date")
    }
    if not stated:
        return None
    if len(stated) > 1:
        raise PeriodError(f"release {receipt.get('snapshot_id')} records more than one publisher period")
    start, end = stated.pop()
    release = receipt.get("release")
    label = str(release.get("publisher_release_label") or "") if isinstance(release, dict) else ""
    return label, start, end


def receipt_periods(root: Path = DATASETS) -> dict[str, tuple[str, str, str]]:
    """Return each capture's release label and period from the acquisition receipts, keyed by release ID."""
    periods: dict[str, tuple[str, str, str]] = {}
    for pattern in RECEIPTS:
        for path in sorted(root.glob(pattern)):
            receipt = json.loads(path.read_text())
            found = receipt_period(receipt)
            if found is None:
                continue
            release_id = str(receipt["snapshot_id"])
            if periods.setdefault(release_id, found) != found:
                raise PeriodError(f"release {release_id} has two recorded periods")
    return periods


def rows_for(loaded: Iterable[Mapping[str, str]], periods: Mapping[str, tuple[str, str, str]]) -> list[dict[str, str]]:
    """Return one seed row per loaded file, sorted by table and period; a file without a period stops the run [365]."""
    rows = []
    for item in loaded:
        if item["release_id"] not in periods:
            raise PeriodError(f"{item['table']} file {item['file_name']} (release {item['release_id']}) has no recorded period")
        label, start, end = periods[item["release_id"]]
        rows.append(
            {
                "member_sha256": item["sha256"],
                "bronze_table": item["table"],
                "release_id": item["release_id"],
                "release_label": label,
                "period_start": start,
                "period_end": end,
            }
        )
    return sorted(rows, key=lambda row: (row["bronze_table"], row["period_end"], row["member_sha256"]))


def loaded_files() -> list[dict[str, str]]:
    """Read the loaded owner, enrollment and change-of-ownership files from the S3 manifests, read-only, as bronze does."""
    from scripts.lakehouse import bronze
    from scripts.lakehouse.catalog import deployment

    settings = deployment()
    os.environ.setdefault("AWS_PROFILE", settings["aws_profile"])
    os.environ.setdefault("AWS_REGION", settings["aws_region"])
    inputs, _ = bronze.discover(bronze.S3Storage(), settings["data_bucket_name"], bronze.load_table_map(), TABLES, bronze.load_retired())
    return [{"table": item["table"], "sha256": item["sha256"], "release_id": item["release_id"], "file_name": item["file_name"]} for item in inputs]


def build() -> list[dict[str, str]]:
    """Return the seed rows from storage and the receipts."""
    return rows_for(loaded_files(), receipt_periods())


def as_csv(rows: Iterable[Mapping[str, str]]) -> str:
    """Return the rows as the seed's CSV text."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def main() -> int:
    """Rebuild the ownership release periods and write or check the committed seed."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="compare with the committed seed instead of writing it")
    args = parser.parse_args()
    try:
        text = as_csv(build())
    except PeriodError as error:
        sys.stderr.write(f"ownership release periods: {error}\n")
        return 1
    if args.check:
        same = SEED.exists() and SEED.read_text() == text
        sys.stdout.write(f"ownership release periods: {'committed seed reproduced' if same else 'committed seed differs from storage'}\n")
        return 0 if same else 1
    SEED.write_text(text)
    sys.stdout.write(f"ownership release periods: {text.count(chr(10)) - 1} files\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
