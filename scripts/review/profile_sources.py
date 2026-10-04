"""Profile a completed source's stored CSVs for the schema and metadata review.

Reads the source's final inventory, verifies every stored CSV against the SHA-256 in its receipt and writes a deterministic
``profile.json``: header variants, per-column value domains and tokens, grain-key uniqueness and coverage by year and geography.
Source files are only read. A separate ``run.json`` records the run identity and the profile's checksum.

Usage::

    .venv/bin/python -m scripts.review.profile_sources --source mmd --output data/schema_review/2026_09_28/mmd
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import platform
import re
import sys
from collections import Counter, defaultdict
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HISTORY = ROOT / "data" / "historical_acquisition"
NUMERIC = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
DISTINCT_CAP = 200_000  # per column; beyond this the distinct count is reported as a lower bound
TOP_TOKENS = 25
log = logging.getLogger("profile_sources")


@dataclass
class ColumnStats:
    """Exact counts for one column across every profiled file."""

    rows: int = 0
    empty: int = 0
    numeric: int = 0
    integer: int = 0
    leading_zero: int = 0
    lengths: Counter[int] = field(default_factory=Counter)
    tokens: Counter[str] = field(default_factory=Counter)
    distinct: set[str] = field(default_factory=set)
    distinct_capped: bool = False
    minimum: float | None = None
    maximum: float | None = None

    def add(self, value: str) -> None:
        self.rows += 1
        if value == "":
            self.empty += 1
            return
        self.lengths[len(value)] += 1
        if len(self.distinct) < DISTINCT_CAP:
            self.distinct.add(value)
        else:
            self.distinct_capped = self.distinct_capped or value not in self.distinct
        if not NUMERIC.match(value):
            self.tokens[value] += 1
            return
        self.numeric += 1
        plain_integer = re.fullmatch(r"[+-]?\d+", value) is not None
        self.integer += plain_integer
        unsigned = value.lstrip("+-")
        self.leading_zero += plain_integer and len(unsigned) > 1 and unsigned[0] == "0"
        number = float(value)
        self.minimum = number if self.minimum is None else min(self.minimum, number)
        self.maximum = number if self.maximum is None else max(self.maximum, number)


CODE_TOKEN_LIMIT = 10  # more distinct non-numeric values than this means free text, not a few status codes


def infer_column_type(stats: ColumnStats, identifier: bool) -> str:
    """Name the type a silver staging model should give this column.

    Declared identifiers and any column with leading zeros stay text, so codes such as FIPS keep their exact form. Numeric
    columns with a few repeated codes (``(X)``, ``-``) are ``numeric_with_tokens``; their codes need a mapping, not a cast.
    Sentinels that look numeric (Census ``-666666666``) are not detected here; the review reads them from ``min``.

    Returns one of ``empty``, ``identifier_text``, ``integer``, ``decimal``, ``numeric_with_tokens`` or ``text``.
    """
    if stats.rows == stats.empty:
        return "empty"
    if identifier or stats.leading_zero:
        return "identifier_text"
    if not stats.tokens:
        return "integer" if stats.integer == stats.numeric else "decimal"
    return "numeric_with_tokens" if stats.numeric and len(stats.tokens) <= CODE_TOKEN_LIMIT else "text"


@dataclass(frozen=True)
class SourceSpec:
    """How to find a source's stored CSVs and which columns define its grain."""

    inputs: Callable[[], list[tuple[Path, str]]]
    key: tuple[str, ...]
    partition: tuple[str, ...]  # a partition's rows must all sit in one file, so per-file key checks cover the source
    year_column: str | None
    geo_column: str
    identifiers: tuple[str, ...]  # codes that must stay text even when every value is numeric (the publisher may drop leading zeros)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def receipt_csv(receipt_path: Path, expected_receipt_sha: str | None = None) -> tuple[Path, str]:
    """Return the stored data CSV named by a capture receipt and its recorded SHA-256."""
    if expected_receipt_sha is not None and sha256_file(receipt_path) != expected_receipt_sha:
        raise ValueError(f"{receipt_path}: receipt checksum differs from the final inventory")
    receipt = json.loads(receipt_path.read_text())
    data = [a for a in receipt["artifacts"] if a["role"] == "data" and a["media_type"] == "text/csv"]
    if len(data) != 1:
        raise ValueError(f"{receipt_path}: expected one data CSV artifact, found {len(data)}")
    return receipt_path.parent / data[0]["storage_path"], data[0]["sha256"]


def mmd_inputs() -> list[tuple[Path, str]]:
    inventory = json.loads((ROOT / "data/e2e/mmd_api_collection/20260926/queue_inventory_final_v4.json").read_text())
    entries = inventory["browser_completed"] + inventory["api_previously_completed"] + inventory["newly_stored"]
    return [receipt_csv(ROOT / entry["receipt_path"]) for entry in entries]


def bls_inputs() -> list[tuple[Path, str]]:
    inventory = json.loads((ROOT / "data/e2e/bls_api_collection/2026_09_27/final_inventory.json").read_text())
    batches = HISTORY / "bls_api_history/2026-09-26/batches"
    return [receipt_csv(batches / b["batch_id"] / "capture/receipt.json", b["receipt_sha256"]) for b in inventory["batches"]]


def census_inputs() -> list[tuple[Path, str]]:
    coverage = json.loads((ROOT / "data/e2e/census_acs_api/20260926/coverage_all_profiles_v2.json").read_text())
    batches = HISTORY / "census_acs_api_history/20260926/batches"
    pairs = [receipt_csv(batches / t["batch_id"] / "capture/receipt.json", t["receipt_sha256"]) for t in coverage["tables"]]
    for (_, sha), table in zip(pairs, coverage["tables"], strict=True):
        if sha != table["csv_sha256"]:
            raise ValueError(f"Census {table['table']}: receipt CSV checksum differs from the coverage record")
    return pairs


MMD_DIMENSIONS = ("population", "year", "geography", "measure", "adjustment", "analysis", "domain", "condition")
MMD_SUBGROUPS = ("primary_sex", "primary_age", "primary_dual", "primary_race", "primary_eligibility")
SOURCES = {
    "mmd": SourceSpec(mmd_inputs, (*MMD_DIMENSIONS, *MMD_SUBGROUPS, "fips"), ("condition", "year", "geography"), "year", "fips", ("fips",)),
    "bls": SourceSpec(bls_inputs, ("seriesID", "year", "period"), ("seriesID", "year"), "year", "county_fips", ("seriesID", "county_fips", "measure_code")),
    "census": SourceSpec(census_inputs, ("GEO_ID",), ("__file__",), None, "GEO_ID", ("GEO_ID", "state", "county")),
}


def read_rows(path: Path) -> tuple[list[str], Iterator[dict[str, str]]]:
    """Return a CSV's header and an iterator over its rows; a row whose width differs from the header fails loudly."""
    handle = path.open(newline="", encoding="utf-8")
    reader = csv.DictReader(handle)
    header = list(reader.fieldnames or [])

    def rows() -> Iterator[dict[str, str]]:
        with handle:
            for row in reader:
                if None in row or len(row) != len(header):
                    raise ValueError(f"{path}:{reader.line_num}: row width differs from the header")
                yield row

    return header, rows()


def profile(spec: SourceSpec) -> dict[str, object]:
    """Profile every stored CSV of one source and return the deterministic profile document."""
    inputs = sorted(spec.inputs(), key=lambda pair: str(pair[0]))
    columns: dict[str, ColumnStats] = defaultdict(ColumnStats)
    headers: dict[tuple[str, ...], list[str]] = defaultdict(list)
    partitions: dict[tuple[str, ...], set[str]] = defaultdict(set)
    duplicate_keys: list[dict[str, object]] = []
    rows_by_year: Counter[str] = Counter()
    geos_by_year: dict[str, set[str]] = defaultdict(set)
    for path, expected_sha in inputs:
        relative = str(path.relative_to(ROOT))
        if sha256_file(path) != expected_sha:
            raise ValueError(f"{relative}: CSV checksum differs from its receipt")
        seen: set[tuple[str, ...]] = set()
        header, rows = read_rows(path)
        for row in rows:
            for name, value in row.items():
                columns[name].add(value)
            key = tuple(row.get(name, "") for name in spec.key)
            if key in seen:
                duplicate_keys.append({"file": relative, "key": list(key)})
            seen.add(key)
            partitions[tuple(relative if name == "__file__" else row[name] for name in spec.partition)].add(relative)
            year = row[spec.year_column] if spec.year_column else "all"
            rows_by_year[year] += 1
            geos_by_year[year].add(row[spec.geo_column])
        headers[tuple(header)].append(relative)
    split = {"|".join(part): sorted(files) for part, files in partitions.items() if len(files) > 1}
    return {
        "inputs": {
            "files": len(inputs),
            "sha256_of_sorted_inputs": hashlib.sha256(json.dumps([[str(p.relative_to(ROOT)), s] for p, s in inputs]).encode()).hexdigest(),
        },
        "rows": sum(rows_by_year.values()),
        "header_variants": [
            {"columns": list(h), "files": len(f), "examples": sorted(f)[:3]} for h, f in sorted(headers.items(), key=lambda item: (-len(item[1]), item[0]))
        ],
        "columns": {name: describe(stats, name in spec.identifiers) for name, stats in sorted(columns.items())},
        "grain": {
            "key": list(spec.key),
            "duplicate_key_rows": len(duplicate_keys),
            "duplicate_examples": duplicate_keys[:20],
            "partitions_split_across_files": split,
        },
        "coverage": {year: {"rows": rows_by_year[year], "geographies": len(geos_by_year[year])} for year in sorted(rows_by_year)},
    }


def describe(stats: ColumnStats, identifier: bool) -> dict[str, object]:
    return {
        "type": infer_column_type(stats, identifier),
        "rows": stats.rows,
        "empty": stats.empty,
        "numeric": stats.numeric,
        "integer": stats.integer,
        "leading_zero": stats.leading_zero,
        "min": stats.minimum,
        "max": stats.maximum,
        "lengths": {str(k): v for k, v in sorted(stats.lengths.items())},
        "distinct": len(stats.distinct),
        "distinct_is_lower_bound": stats.distinct_capped,
        "token_distinct": len(stats.tokens),
        "top_tokens": [[token, count] for token, count in sorted(stats.tokens.items(), key=lambda item: (-item[1], item[0]))[:TOP_TOKENS]],
    }


def write_atomic(path: Path, text: str) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text)
    os.replace(temporary, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", required=True, choices=sorted(SOURCES))
    parser.add_argument("--output", required=True, type=Path, help="Directory for profile.json and run.json; rerunning replaces both")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
    started = datetime.now(UTC).isoformat()
    document = {"source": args.source, **profile(SOURCES[args.source])}
    text = json.dumps(document, indent=2, sort_keys=True) + "\n"
    output = args.output if args.output.is_absolute() else ROOT / args.output
    output.mkdir(parents=True, exist_ok=True)
    previous = (output / "profile.json").read_text() if (output / "profile.json").exists() else None
    write_atomic(output / "profile.json", text)
    run = {
        "source": args.source,
        "started_at_utc": started,
        "finished_at_utc": datetime.now(UTC).isoformat(),
        "command": f".venv/bin/python -m scripts.review.profile_sources --source {args.source} --output {args.output}",
        "code_sha256": sha256_file(Path(__file__)),
        "python": platform.python_version(),
        "profile_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "profile_unchanged_from_previous_run": None if previous is None else previous == text,
    }
    write_atomic(output / "run.json", json.dumps(run, indent=2, sort_keys=True) + "\n")
    log.info("source=%s rows=%s profile_sha256=%s", args.source, document["rows"], run["profile_sha256"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
