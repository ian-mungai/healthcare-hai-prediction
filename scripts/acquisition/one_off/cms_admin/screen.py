"""Pre-storage screen for the locally captured CMS CHOW and Enrollments files (failure modes 1, 3-7).

Run from the repository root after ``capture_local.py``:

    PYTHONPATH=. .venv/bin/python scripts/acquisition/one_off/cms_admin/screen.py

Writes ``screen_report_v2.json`` (write-once) and exits non-zero when any stop condition is found.
"""

import collections
import csv
import io
import json
import re
import sys
from pathlib import Path

from scripts.acquisition.batch import receipt_for_job, validate_batch
from scripts.acquisition.capture import receipt_validator
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json

HERE = REPO_ROOT / "data/acquisition_planning/cms_admin_20260929"
PERSON = re.compile(r"\b(FIRST|LAST|MIDDLE)[ _]?NAME\b|\bTITLE\b|\bBIRTH|\bSSN\b|\bDOB\b|HOME ADDRESS|\bEMAIL\b|\bPHONE\b", re.IGNORECASE)
FACILITY_TYPES = {"00-09", "00-85", "00-24"}


def rows_of(path: Path) -> tuple[list[str], list[list[str]], str]:
    """Header, rows and encoding of a CSV, read as text so identifiers keep their leading zeros."""
    raw = path.read_bytes()
    try:
        text, encoding = raw.decode("utf-8-sig"), "utf-8"
    except UnicodeDecodeError:
        text, encoding = raw.decode("cp1252"), "cp1252"
    table = list(csv.reader(io.StringIO(text)))
    return table[0], table[1:], encoding


def main() -> None:
    """Screen every captured file; stop on personal-data columns, header drift, bad widths or IDs."""
    registry, validator = load_registry(), receipt_validator()
    batch = read_json(HERE / "batch.json")
    root = REPO_ROOT / "data/acquisition_batches" / canonical_hash(batch)
    headers: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    files, stops = [], []
    for job in validate_batch(batch, registry, validator):
        directory = root / "jobs" / job["job_id"]
        receipt_path = receipt_for_job(directory, job, registry, validator)
        if receipt_path is None:
            raise SystemExit(f"{job['job_id']}: no receipt; run capture_local first")
        receipt = read_json(receipt_path)
        artifact = next(a for a in receipt["artifacts"] if a["role"] == "data")
        header, rows, encoding = rows_of(receipt_path.parent / artifact["storage_path"])
        source = job["plan"]["source_id"]
        headers[source][tuple(header)] += 1
        col = {name.strip().upper(): i for i, name in enumerate(header)}
        widths = sum(len(r) != len(header) for r in rows)
        person_cols = [h for h in header if PERSON.search(h)]
        npi_cols = [i for name, i in col.items() if name.startswith("NPI")]
        bad_npi = sum(1 for r in rows for i in npi_cols if len(r) == len(header) and r[i] and not re.fullmatch(r"\d{10}", r[i]))
        type_cols = [i for name, i in col.items() if name.startswith("PROVIDER TYPE CODE")]
        other_types = collections.Counter(r[i] for r in rows for i in type_cols if len(r) == len(header) and r[i] and r[i] not in FACILITY_TYPES)
        dates = [r[col["EFFECTIVE DATE"]] for r in rows if "EFFECTIVE DATE" in col and len(r) == len(header) and r[col["EFFECTIVE DATE"]]]
        duplicates = len(rows) - len({tuple(r) for r in rows})
        period_end = job["plan"]["measurement_periods"][0]["end_date"]
        # CMS writes dates as MM/DD/YYYY; compare as YYYYMMDD.
        ymd = [f"{d[6:10]}{d[0:2]}{d[3:5]}" for d in dates if re.fullmatch(r"\d{2}/\d{2}/\d{4}", d)]
        unparsed = len(dates) - len(ymd)
        late = sum(1 for d in ymd if d > period_end.replace("-", ""))
        early = sum(1 for d in ymd if d < "20160101")
        entry = {
            "job_id": job["job_id"],
            "source_id": source,
            "period_end": period_end,
            "rows": len(rows),
            "encoding": encoding,
            "columns": len(header),
            "bad_width_rows": widths,
            "person_like_columns": person_cols,
            "invalid_npi_values": bad_npi,
            "non_facility_provider_types": dict(other_types),
            "effective_dates_unparsed": unparsed,
            "effective_dates_before_2016": early,
            "effective_dates_after_period": late,
            "exact_duplicate_rows": duplicates,
        }
        files.append(entry)
        if widths or person_cols or bad_npi or unparsed:
            stops.append(entry["job_id"])
    header_sets = {s: [{"columns": list(h), "files": n} for h, n in c.items()] for s, c in headers.items()}
    drift = [s for s, c in headers.items() if len(c) > 1]
    report = {"batch_sha256": canonical_hash(batch), "files": files, "header_sets": header_sets, "header_drift": drift, "stops": stops, "model_eligible": False}
    write_once(HERE / "screen_report_v2.json", encoded_json(report))
    summary = {
        "files": len(files),
        "rows": sum(f["rows"] for f in files),
        "stops": stops,
        "header_drift": drift,
        "non_facility_rows": sum(sum(f["non_facility_provider_types"].values()) for f in files),
        "duplicate_rows": sum(f["exact_duplicate_rows"] for f in files),
        "encodings": dict(collections.Counter(f["encoding"] for f in files)),
        "dates_out_of_range": sum(f["effective_dates_before_2016"] + f["effective_dates_after_period"] for f in files),
    }
    sys.stdout.write(json.dumps(summary) + "\n")
    sys.exit(1 if stops or drift else 0)


if __name__ == "__main__":
    main()
