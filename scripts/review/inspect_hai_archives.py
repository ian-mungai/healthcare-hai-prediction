"""Review the infection tables inside CMS hospital archives, row by row, against the frozen review inventory.

Every local archive for the source is verified against its receipt, opened in memory and never extracted. Quarterly
archives nested in annual archives are opened one level deep. Only the infection tables, the footnote crosswalk and
the measure-date table are read; other members are listed by name and size. Output holds counts and values from
allow-listed code columns only, so facility names, addresses and telephone numbers never reach it.

Usage::

    .venv/bin/python -m scripts.review.inspect_hai_archives --source-root SOURCE_ROOT \
        --inventory inputs/capture_inventory_effective.json --output hai_run1/hai_archives.json
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import re
import sys
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

from scripts.review.profile_sources import NUMERIC
from scripts.review.review_remaining import digest, safe_path, write_json

SOURCE = "main-hai-pdc"
MEMBER_LIMIT = 512 << 20  # declared uncompressed bytes; larger members are refused, not read
VALUE_CAP = 60  # distinct values emitted per allow-listed column before only a count is kept
SCORE_TOKENS = {"Not Available", "Not Applicable", "N/A", "--"}
MEASURE_ID = re.compile(r"^[A-Za-z0-9_-]{1,40}$")
FOOTNOTE_CODE = r"(?:\d{1,3}|\*{1,3}|[a-z])"
FOOTNOTE_LIST = re.compile(rf"^{FOOTNOTE_CODE}(?:\s*,\s*{FOOTNOTE_CODE})*$")
FOOTNOTE_WITH_TEXT = re.compile(r"(?:^|,\s)(\d{1,3}) - ")
STATE_CODE = re.compile(r"^[A-Z]{2}$")
RELEASE = re.compile(r"hospitals_(\d{4}-\d{2}-\d{2})")
PREFIXED = re.compile(r"^([a-z0-9]{4}_[a-z0-9]{4})(?:_\d{4}_\d{2}_\d{2}_(.+))?$")
log = logging.getLogger("inspect_hai_archives")


@dataclass(frozen=True)
class TableSpec:
    """How one table is recognized and which column roles it must have."""

    dataset_id: str
    titles: tuple[str, ...]
    required: tuple[str, ...]
    entity: str | None = None


TABLES = {
    "pch_hai_hospital": TableSpec(
        "k653_4ka8",
        ("pch_healthcare_associated_infections_hospital", "hospital_quarterly_qualitymeasure_pch_hai_hospital"),
        ("facility", "measure_id", "score", "start", "end"),
        "facility",
    ),
    "hai_hospital": TableSpec("77hc_ibv8", ("healthcare_associated_infections_hospital",), ("facility", "measure_id", "score", "start", "end"), "facility"),
    "hai_state": TableSpec("k2ze_bqvw", ("healthcare_associated_infections_state",), ("state", "measure_id", "score", "start", "end"), "state"),
    "hai_national": TableSpec("yd3s_jyhd", ("healthcare_associated_infections_national",), ("measure_id", "score", "start", "end")),
    "footnote_crosswalk": TableSpec("y9us_9xdf", ("footnote_crosswalk",), ("footnote", "footnote_text")),
    "measure_dates": TableSpec("4j6d_yzce", ("measure_dates",), ("measure_id", "start", "end")),
}
ROLES = {
    "provider_id": "facility",
    "facility_id": "facility",
    "state": "state",
    "measure_id": "measure_id",
    "measure_name": "measure_name",
    "compared_to_national": "compared",
    "score": "score",
    "footnote": "footnote",
    "footnote_text": "footnote_text",
    "measure_start_date": "start",
    "start_date": "start",
    "measure_end_date": "end",
    "end_date": "end",
    "measure_start_quarter": "start_quarter",
    "measure_end_quarter": "end_quarter",
}


def normalize(name: str) -> str:
    """Lower-case a publisher name and collapse punctuation so naming variants compare equal."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def sha256(data: bytes) -> str:
    """Hash in-memory member bytes."""
    return hashlib.sha256(data).hexdigest()


def safe_text(value: str, limit: int) -> str | None:
    """Return publisher label text fit to emit, or None when it could carry contact details."""
    text = value.strip()
    if not text or len(text) > limit or "@" in text or re.search(r"\d{7,}|\d{3}\D{0,2}\d{3}\D\d{4}", text):
        return None
    return text


def iso_date(value: str) -> str | None:
    """Normalize the publisher's two date layouts to ISO; None when unparseable."""
    for layout in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip(), layout).date().isoformat()
        except ValueError:
            continue
    return None


def is_resource_fork(filename: str) -> bool:
    """macOS archive metadata that imitates the names of real members."""
    path = PurePosixPath(filename)
    return "__MACOSX" in path.parts or path.name.startswith("._") or path.name == ".DS_Store"


def classify_member(filename: str) -> str | None:
    """Match a member to a reviewed table by dataset ID or normalized title."""
    path = PurePosixPath(filename)
    if path.suffix.lower() != ".csv":
        return None
    stem = re.sub(r"^pdc_s3_hos_data_", "", normalize(path.stem))
    match = PREFIXED.match(stem)
    dataset_id, title = (match.group(1), match.group(2)) if match else (None, stem)
    for kind, spec in TABLES.items():
        if (dataset_id is not None and dataset_id == spec.dataset_id) or title in spec.titles:
            return kind
    return None


def capped(counter: Counter[str]) -> dict[str, Any]:
    """Emit a small allow-listed value domain, or only its size when it is unexpectedly large."""
    if len(counter) > VALUE_CAP:
        return {"distinct": len(counter), "values_withheld": "distinct count exceeds the emit cap"}
    return dict(sorted(counter.items()))


@dataclass
class MeasureStats:
    """Exact counts for one measure ID within one table."""

    rows: int = 0
    names: Counter[str] = field(default_factory=Counter)
    names_withheld: int = 0
    periods: Counter[str] = field(default_factory=Counter)
    unparseable_dates: int = 0
    score: Counter[str] = field(default_factory=Counter)
    minimum: float | None = None
    maximum: float | None = None
    compared: Counter[str] = field(default_factory=Counter)
    footnotes: Counter[str] = field(default_factory=Counter)
    unavailable_without_footnote: int = 0
    numeric_with_footnote: int = 0
    pairs: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))

    def add_score(self, value: str, has_footnote: bool) -> None:
        score = value.strip()
        if NUMERIC.fullmatch(score):
            number = float(score)
            self.score["numeric"] += 1
            self.score["numeric_zero"] += number == 0
            self.score["numeric_negative"] += number < 0
            self.minimum = number if self.minimum is None else min(self.minimum, number)
            self.maximum = number if self.maximum is None else max(self.maximum, number)
            self.numeric_with_footnote += has_footnote
            return
        label = "blank" if not score else f"token:{score}" if score in SCORE_TOKENS else "other_text"
        self.score[label] += 1
        self.unavailable_without_footnote += not has_footnote

    def describe(self) -> dict[str, Any]:
        return {
            "rows": self.rows,
            "names": capped(self.names),
            "names_withheld": self.names_withheld,
            "periods": dict(sorted(self.periods.items())),
            "unparseable_date_rows": self.unparseable_dates,
            "score": {k: v for k, v in sorted(self.score.items()) if v},
            "score_minimum": self.minimum,
            "score_maximum": self.maximum,
            "compared_to_national": capped(self.compared),
            "footnote_codes": dict(sorted(self.footnotes.items(), key=lambda item: (len(item[0]), item[0]))),
            "unavailable_without_footnote": self.unavailable_without_footnote,
            "numeric_with_footnote": self.numeric_with_footnote,
            "period_digests": {period: sha256("\n".join(sorted(lines)).encode()) for period, lines in sorted(self.pairs.items())},
            "period_entities": {period: len(lines) for period, lines in sorted(self.pairs.items())},
        }


def read_table(data: bytes) -> tuple[str, list[str], list[list[str]]]:
    """Decode a whole table; undecodable bytes become replacement characters that the caller counts."""
    try:
        text, encoding = data.decode("utf-8-sig"), "utf-8-sig"
    except UnicodeDecodeError:
        text, encoding = data.decode("cp1252", errors="replace"), "cp1252"
    reader = csv.reader(io.StringIO(text, newline=""))
    header = next(reader, [])
    return encoding, header, list(reader)


def column_roles(header: list[str]) -> dict[str, int]:
    """Map known roles to column positions; the first matching column wins."""
    roles: dict[str, int] = {}
    for index, name in enumerate(header):
        role = ROLES.get(normalize(name))
        if role is not None and role not in roles:
            roles[role] = index
    return roles


def footnote_codes(cell: str) -> tuple[list[str], str]:
    """Return a footnote cell's codes and its layout: blank, code_list, code_with_text or nonconforming."""
    text = cell.strip()
    if not text:
        return [], "blank"
    if FOOTNOTE_LIST.fullmatch(text):
        return [part.strip() for part in text.split(",")], "code_list"
    codes = FOOTNOTE_WITH_TEXT.findall(text)
    return (codes, "code_with_text") if codes else ([], "nonconforming")


def row_period(row: list[str], roles: dict[str, int]) -> str:
    """One comparable label for a row's reporting period."""
    start, end = iso_date(row[roles["start"]]), iso_date(row[roles["end"]])
    return f"{start}/{end}" if start and end else "<unparseable>"


def base_profile(kind: str, data: bytes, encoding: str, header: list[str], rows: list[list[str]], roles: dict[str, int]) -> dict[str, Any]:
    """Shape facts common to every reviewed table."""
    return {
        "class": kind,
        "bytes": len(data),
        "encoding": encoding,
        "nul_bytes": data.count(b"\x00"),
        "header": [name if safe_text(name, 80) else "<withheld>" for name in header],
        "header_roles": dict(sorted(roles.items(), key=lambda item: item[1])),
        "duplicate_header_names": len(header) - len(set(header)),
        "rows": len(rows),
        "ragged_rows": sum(len(row) != len(header) for row in rows),
        "replacement_cells": sum("\ufffd" in value for row in rows for value in row),
    }


def profile_measure_table(kind: str, rows: list[list[str]], roles: dict[str, int], width: int) -> dict[str, Any]:
    """Count grain, identifiers, periods, scores, footnotes and categories for one infection table."""
    entity_role = TABLES[kind].entity
    measures: dict[str, MeasureStats] = defaultdict(MeasureStats)
    seen: set[tuple[str, str]] = set()
    entities: set[str] = set()
    lengths: Counter[str] = Counter()
    shape: Counter[str] = Counter()
    states: Counter[str] = Counter()
    issues: Counter[str] = Counter()
    layouts: Counter[str] = Counter()
    for row in rows:
        if len(row) != width:
            continue
        entity = row[roles[entity_role]].strip() if entity_role else ""
        measure = row[roles["measure_id"]].strip()
        if not MEASURE_ID.fullmatch(measure):
            issues["nonconforming_measure_id_rows"] += 1
            continue
        issues["blank_key_rows"] += bool(entity_role) and not entity
        issues["duplicate_key_rows"] += (entity, measure) in seen
        seen.add((entity, measure))
        if entity_role == "facility" and entity not in entities:
            lengths[str(len(entity))] += 1
            shape["leading_zero"] += entity.startswith("0")
            shape["contains_letter"] += not entity.isdigit()
        if entity_role == "state":
            states[entity if STATE_CODE.fullmatch(entity) else "<nonconforming>"] += 1
        entities.add(entity)
        stats = measures[measure]
        stats.rows += 1
        if "measure_name" in roles:
            name = safe_text(row[roles["measure_name"]], 200)
            stats.names[name or "<withheld>"] += 1
            stats.names_withheld += name is None
        period = row_period(row, roles)
        stats.periods[period] += 1
        stats.unparseable_dates += period == "<unparseable>"
        codes, layout = footnote_codes(row[roles["footnote"]]) if "footnote" in roles else ([], "no_footnote_column")
        stats.footnotes.update(codes)
        layouts[layout] += 1
        stats.add_score(row[roles["score"]], layout not in {"blank", "no_footnote_column"})
        stats.pairs[period].append(f"{entity}\t{row[roles['score']].strip()}")
        if "compared" in roles:
            category = safe_text(row[roles["compared"]], 80)
            stats.compared[category or "<blank_or_withheld>"] += 1
    profile: dict[str, Any] = {
        "issues": {k: v for k, v in sorted(issues.items())},
        "footnote_cell_layouts": dict(sorted(layouts.items())),
        "distinct_entities": len(entities) if entity_role else None,
        "distinct_keys": len(seen),
        "measures": {name: stats.describe() for name, stats in sorted(measures.items())},
    }
    if entity_role == "facility":
        profile["facility_id_shape"] = {"lengths": dict(sorted(lengths.items())), **{k: v for k, v in sorted(shape.items())}}
    if entity_role == "state":
        profile["state_codes"] = capped(states)
    return profile


def profile_reference_table(kind: str, rows: list[list[str]], roles: dict[str, int], width: int) -> dict[str, Any]:
    """Emit the footnote definitions, or the infection rows of the measure-date table."""
    entries: list[dict[str, str | None]] = []
    withheld = 0
    for row in rows:
        if len(row) != width:
            continue
        if kind == "footnote_crosswalk":
            code, text = row[roles["footnote"]].strip(), safe_text(row[roles["footnote_text"]], 600)
            if not re.fullmatch(r"[A-Za-z0-9*]{1,4}", code) or text is None:
                withheld += 1
                continue
            entries.append({"code": code, "text": text})
        else:
            measure = row[roles["measure_id"]].strip()
            if not (MEASURE_ID.fullmatch(measure) and measure.upper().replace("-", "_").startswith(("HAI_", "PCH_"))):
                continue
            quarters: dict[str, str | None] = {role: safe_text(row[roles[role]], 12) for role in ("start_quarter", "end_quarter") if role in roles}
            entries.append({"measure_id": measure, "start": iso_date(row[roles["start"]]), "end": iso_date(row[roles["end"]]), **quarters})
    entries.sort(key=lambda entry: json.dumps(entry, sort_keys=True))
    return {"entries": entries, "rows_withheld": withheld}


def profile_table(kind: str, data: bytes) -> dict[str, Any]:
    """Profile one distinct table, or mark its layout unresolved without reading values."""
    try:
        encoding, header, rows = read_table(data)
    except csv.Error as error:
        return {"class": kind, "bytes": len(data), "status": "parse_failed", "error_type": type(error).__name__}
    roles = column_roles(header)
    profile = base_profile(kind, data, encoding, header, rows, roles)
    missing = sorted(role for role in TABLES[kind].required if role not in roles)
    if missing:
        return {**profile, "status": "layout_unresolved", "missing_roles": missing}
    detail = profile_reference_table if kind in {"footnote_crosswalk", "measure_dates"} else profile_measure_table
    return {**profile, "status": "profiled", **detail(kind, rows, roles, len(header))}


class Review:
    """Accumulates distinct archives and tables so identical copies are profiled once."""

    def __init__(self) -> None:
        self.archives: dict[str, dict[str, Any]] = {}
        self.tables: dict[str, dict[str, Any]] = {}
        self.table_releases: dict[str, set[str]] = defaultdict(set)
        self.table_locators: dict[str, tuple[Path, str | None, str]] = {}

    def read_member(self, archive: zipfile.ZipFile, info: zipfile.ZipInfo) -> bytes | str:
        """Read one member in memory, or return the reason it was refused."""
        path = PurePosixPath(info.filename)
        if path.is_absolute() or ".." in path.parts:
            return "unsafe_member_name"
        if info.flag_bits & 1:
            return "encrypted_member"
        if info.file_size > MEMBER_LIMIT:
            return "member_exceeds_size_limit"
        try:
            with archive.open(info) as handle:
                data = handle.read(MEMBER_LIMIT + 1)
        except (zipfile.BadZipFile, OSError, NotImplementedError, RuntimeError) as error:
            return f"member_unreadable:{type(error).__name__}"
        return data if len(data) == info.file_size else "member_size_differs_from_declared"

    def add_archive(self, name: str, data: io.BytesIO | Path, archive_sha: str, size: int, outer: Path, nested: str | None = None) -> None:
        """Inspect one distinct archive; a nested quarterly archive (``nested`` set) is never recursed into again."""
        release = RELEASE.search(name)
        label = release.group(1) if release else PurePosixPath(name).stem
        if archive_sha in self.archives:
            entry = self.archives[archive_sha]
            entry["names"] = sorted({*entry["names"], name})
            return
        entry = {"names": [name], "bytes": size, "release": label, "findings": [], "nested_archives": [], "tables": {}, "other_members": []}
        self.archives[archive_sha] = entry
        try:
            archive = zipfile.ZipFile(data)
        except (zipfile.BadZipFile, OSError) as error:
            entry["findings"].append(f"archive_unreadable:{type(error).__name__}")
            return
        with archive:
            infos = sorted((i for i in archive.infolist() if not i.is_dir()), key=lambda i: i.filename)
            entry["member_count"] = len(infos)
            entry["resource_fork_members_skipped"] = sum(is_resource_fork(i.filename) for i in infos)
            entry["member_extensions"] = dict(sorted(Counter(PurePosixPath(i.filename).suffix.lower() for i in infos).items()))
            for info in infos:
                if is_resource_fork(info.filename):
                    continue
                is_nested = PurePosixPath(info.filename).suffix.lower() == ".zip"
                kind = "nested_archive" if is_nested else classify_member(info.filename)
                if kind is None:
                    entry["other_members"].append({"name": safe_text(info.filename, 200) or "<withheld>", "bytes": info.file_size})
                    continue
                content = self.read_member(archive, info)
                if isinstance(content, str):
                    entry["findings"].append(f"{content}:{safe_text(info.filename, 200) or '<withheld>'}")
                    continue
                member_sha = sha256(content)
                if is_nested:
                    entry["nested_archives"].append({"name": info.filename, "sha256": member_sha, "bytes": len(content)})
                    if nested is None:
                        self.add_archive(info.filename, io.BytesIO(content), member_sha, len(content), outer, nested=info.filename)
                    else:
                        entry["findings"].append(f"nested_archive_not_opened_beyond_one_level:{info.filename}")
                    continue
                entry["tables"].setdefault(kind, []).append({"member": info.filename, "sha256": member_sha})
                self.table_releases[member_sha].add(label)
                if member_sha not in self.tables:
                    self.tables[member_sha] = profile_table(kind, content)
                    self.table_locators[member_sha] = (outer, nested, info.filename)
        for kind in TABLES:
            found = len(entry["tables"].get(kind, []))
            if found != 1 and not entry["nested_archives"]:
                entry["findings"].append(f"{kind}:{'absent' if found == 0 else 'multiple_matches'}")


def review_capture(root: Path, candidate: dict[str, Any], review: Review) -> dict[str, Any]:
    """Verify one capture's receipt and archive bytes, then hand the archive to the review."""
    result: dict[str, Any] = {k: candidate.get(k) for k in ("snapshot_id", "receipt", "inventory_origin", "status")}
    result["inventory_status"] = result.pop("status")
    receipt_path = safe_path(root, candidate["receipt"])
    if not receipt_path.is_file() or digest(receipt_path) != candidate["receipt_sha256"]:
        return {**result, "review_status": "receipt_drift"}
    artifacts = [a for a in json.loads(receipt_path.read_text())["artifacts"] if a["role"] == "data"]
    if len(artifacts) != 1:
        return {**result, "review_status": "unexpected_data_artifact_count", "data_artifacts": len(artifacts)}
    artifact = artifacts[0]
    result.update({"artifact_name": artifact["stored_file_name"], "artifact_sha256": artifact["sha256"]})
    path = receipt_path.parent / artifact["storage_path"]
    if not path.is_file():
        return {**result, "review_status": "artifact_not_local"}
    if path.stat().st_size != artifact["byte_count"] or digest(path) != artifact["sha256"]:
        return {**result, "review_status": "artifact_mismatch"}
    review.add_archive(artifact["stored_file_name"], path, artifact["sha256"], artifact["byte_count"], outer=path)
    return {**result, "review_status": "archive_reviewed"}


def measure_family(measure: str) -> str:
    """The measure-date table names a family (``HAI_1``); the infection tables name its parts (``HAI_1_SIR``)."""
    return "_".join(measure.replace("-", "_").split("_")[:2])


def date_agreement(table: dict[str, Any], date_tables: list[dict[str, Any]]) -> dict[str, Any]:
    """Compare each measure's reporting period with the period the release's measure-date table states for its family.

    A release captured more than once can carry differing measure-date tables; each one is checked.
    """
    results: list[dict[str, int]] = []
    for date_table in date_tables:
        if date_table["status"] != "profiled":
            continue
        stated = {entry["measure_id"]: f"{entry['start']}/{entry['end']}" for entry in date_table["entries"]}
        counts: Counter[str] = Counter()
        for measure, stats in table.get("measures", {}).items():
            expected = stated.get(measure_family(measure))
            label = "family_absent_from_measure_dates" if expected is None else "agree" if set(stats["periods"]) == {expected} else "disagree"
            counts[label] += 1
        results.append(dict(sorted(counts.items())))
    return {"measure_date_tables": len(date_tables), "per_table": results}


def release_timeline(review: Review) -> list[dict[str, Any]]:
    """One row per release: which hospital table it carries and whether the bytes repeat an earlier release."""
    by_release: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for archive in review.archives.values():
        for kind in ("hai_hospital", "measure_dates"):
            by_release[archive["release"]][kind].update(table["sha256"] for table in archive["tables"].get(kind, []))
    timeline: list[dict[str, Any]] = []
    first_seen: dict[str, str] = {}
    for release in sorted(by_release):
        date_tables = [review.tables[table_sha] for table_sha in sorted(by_release[release]["measure_dates"])]
        for table_sha in sorted(by_release[release]["hai_hospital"]):
            table = review.tables[table_sha]
            measures = table.get("measures", {})
            periods = Counter(period for stats in measures.values() for period in stats["periods"])
            timeline.append(
                {
                    "release": release,
                    "hai_hospital_sha256": table_sha,
                    "status": table["status"],
                    "rows": table.get("rows"),
                    "facilities": table.get("distinct_entities"),
                    "measure_ids": len(measures),
                    "periods": dict(sorted(periods.items())),
                    "measure_dates_check": date_agreement(table, date_tables),
                    "same_bytes_as_release": first_seen.get(table_sha),
                }
            )
            first_seen.setdefault(table_sha, release)
    return timeline


def load_table(locator: tuple[Path, str | None, str]) -> bytes:
    """Re-read one table from its archive, through the nested archive when there is one."""
    outer_path, nested, member = locator
    with zipfile.ZipFile(outer_path) as outer:
        if nested is None:
            return outer.read(member)
        with zipfile.ZipFile(io.BytesIO(outer.read(nested))) as inner:
            return inner.read(member)


def score_maps(kind: str, data: bytes, keys: set[tuple[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    """Entity-to-score maps for the requested measure periods of one table."""
    _, header, rows = read_table(data)
    roles = column_roles(header)
    entity_role = TABLES[kind].entity
    maps: dict[tuple[str, str], dict[str, str]] = defaultdict(dict)
    for row in rows:
        if len(row) != len(header):
            continue
        key = (row[roles["measure_id"]].strip(), row_period(row, roles))
        if key in keys:
            maps[key][row[roles[entity_role]].strip() if entity_role else ""] = row[roles["score"]].strip()
    return maps


def transition(before: dict[str, str], after: dict[str, str]) -> dict[str, int]:
    """Count how entities and scores differ between two releases of the same measure period."""
    counts: Counter[str] = Counter({"entities_added": len(after.keys() - before.keys()), "entities_removed": len(before.keys() - after.keys())})
    for entity in before.keys() & after.keys():
        old, new = before[entity], after[entity]
        old_numeric, new_numeric = bool(NUMERIC.fullmatch(old)), bool(NUMERIC.fullmatch(new))
        if old == new:
            counts["scores_unchanged"] += 1
        elif old_numeric and new_numeric:
            counts["numeric_value_changed"] += 1
        elif new_numeric:
            counts["became_numeric"] += 1
        elif old_numeric:
            counts["became_unavailable"] += 1
        else:
            counts["token_changed"] += 1
    return dict(sorted(counts.items()))


def period_comparison(review: Review) -> dict[str, Any]:
    """Find measure periods carried by several distinct tables and how their scores differ between releases."""
    seen: dict[tuple[str, str, str], dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for table_sha, table in review.tables.items():
        for measure, stats in table.get("measures", {}).items():
            for period, value_digest in stats["period_digests"].items():
                seen[(table["class"], measure, period)][value_digest].add(table_sha)
    summary: dict[str, Counter[str]] = defaultdict(Counter)
    wanted: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for (kind, measure, period), digests in seen.items():
        releases = {release for tables in digests.values() for table_sha in tables for release in review.table_releases[table_sha]}
        label = (
            "period_scores_differ_between_releases"
            if len(digests) > 1
            else "period_scores_identical_across_releases"
            if len(releases) > 1
            else "period_in_one_release"
        )
        summary[kind][label] += 1
        if len(digests) > 1:
            for tables in digests.values():
                wanted[min(tables)].add((measure, period))
    maps = {
        table_sha: score_maps(review.tables[table_sha]["class"], load_table(review.table_locators[table_sha]), keys)
        for table_sha, keys in sorted(wanted.items())
    }
    differing: list[dict[str, Any]] = []
    totals: dict[tuple[str, str, str, str], Counter[str]] = defaultdict(Counter)
    for (kind, measure, period), digests in sorted(seen.items()):
        if len(digests) < 2:
            continue
        variants = sorted((min(r for t in tables for r in review.table_releases[t]), min(tables)) for tables in digests.values())
        steps = []
        for (old_release, old_sha), (new_release, new_sha) in zip(variants, variants[1:], strict=False):
            counts = transition(maps[old_sha][(measure, period)], maps[new_sha][(measure, period)])
            steps.append({"from_release": old_release, "to_release": new_release, **counts})
            totals[(kind, period, old_release, new_release)].update({"measures": 1, **counts})
        differing.append({"class": kind, "measure_id": measure, "period": period, "score_variants": len(digests), "transitions": steps})
    return {
        "summary": {kind: dict(sorted(counts.items())) for kind, counts in sorted(summary.items())},
        "transition_totals": [
            {"class": kind, "period": period, "from_release": old, "to_release": new, **dict(sorted(counts.items()))}
            for (kind, period, old, new), counts in sorted(totals.items())
        ],
        "differing": differing,
    }


def footnote_definitions(review: Review) -> dict[str, list[dict[str, Any]]]:
    """Each footnote code's wording variants and the releases that carry each wording."""
    variants: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for table_sha, table in review.tables.items():
        if table["class"] == "footnote_crosswalk" and table["status"] == "profiled":
            for entry in table["entries"]:
                variants[entry["code"]][entry["text"]].update(review.table_releases[table_sha])
    return {
        code: [{"text": text, "releases": len(found), "first_release": min(found), "last_release": max(found)} for text, found in sorted(texts.items())]
        for code, texts in sorted(variants.items(), key=lambda item: (len(item[0]), item[0]))
    }


def build_report(root: Path, inventory_path: Path) -> dict[str, Any]:
    """Review every capture of the source in the inventory and assemble the deterministic report."""
    candidates = [c for c in json.loads(inventory_path.read_text())["candidates"] if c["source_id"] == SOURCE]
    review = Review()
    captures = []
    for index, candidate in enumerate(sorted(candidates, key=lambda c: c["snapshot_id"]), start=1):
        captures.append(review_capture(root, candidate, review))
        log.info("capture %d of %d: %s", index, len(candidates), captures[-1]["review_status"])
    verified = {c["artifact_sha256"] for c in captures if c["review_status"] == "archive_reviewed"}
    for capture in captures:
        if capture["review_status"] == "artifact_not_local":
            capture["same_bytes_reviewed_locally"] = capture["artifact_sha256"] in verified
    tables = {k: {**v, "releases": sorted(review.table_releases[k])} for k, v in sorted(review.tables.items())}
    by_class = Counter(t["class"] for t in tables.values())
    return {
        "version": 1,
        "source_id": SOURCE,
        "inventory_sha256": digest(inventory_path),
        "code_sha256": digest(Path(__file__)),
        "captures": captures,
        "capture_status": dict(sorted(Counter(c["review_status"] for c in captures).items())),
        "archives": dict(sorted(review.archives.items())),
        "tables": tables,
        "totals": {
            "captures": len(captures),
            "distinct_archives": len(review.archives),
            "distinct_tables": dict(sorted(by_class.items())),
            "table_status": dict(sorted(Counter(t["status"] for t in tables.values()).items())),
            "rows_in_distinct_tables": {kind: sum(t.get("rows", 0) for t in tables.values() if t["class"] == kind) for kind in sorted(by_class)},
            "archive_findings": sum(len(a["findings"]) for a in review.archives.values()),
        },
        "release_timeline": release_timeline(review),
        "period_comparison": period_comparison(review),
        "footnote_definitions": footnote_definitions(review),
        "model_eligible": False,
        "limits": [
            "Offline local bytes only; no S3 or publisher requests. An archive held only in S3 is reviewed solely through an identical local checksum.",
            "Only the infection tables, footnote crosswalk and measure-date table are read. Other members are listed by name and size.",
            "Identical tables in several archives are profiled once; rows are never summed across copies.",
            "Period comparison covers the score column by entity. It does not decide which release is authoritative.",
            "Data-dictionary PDFs, target construction, leakage and eligibility are outside this check. No holds are cleared.",
        ],
    }


def main() -> int:
    """Run the archive review from its real CLI."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    csv.field_size_limit(16 << 20)
    report = build_report(args.source_root.resolve(), args.inventory.resolve())
    write_json(args.output, report)
    sys.stdout.write(json.dumps({"capture_status": report["capture_status"], **report["totals"]}) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
