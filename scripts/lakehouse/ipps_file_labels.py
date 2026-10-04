"""Generate the IPPS and occupational-mix file labels and text/Excel twin map that staging reads (failure modes 178 to 188).

Run from the repository root with read-only S3 access:

    .venv/bin/python -m scripts.lakehouse.ipps_file_labels           # rewrite the two seeds
    .venv/bin/python -m scripts.lakehouse.ipps_file_labels --check   # rebuild and compare with the committed seeds

Every loaded object in the five IPPS and occupational-mix bronze tables gets a role, a rule fiscal year, a data fiscal
year for CMI files, an identity year and a rule stage. They are read from the file's own name, then its archive chain,
then the container the S3 manifest says it was extracted from, never from capture time [178]. A name with no year
stops the run unless a committed override covers it [179]. Twins are a text file and a workbook in one snapshot with
the same stem [183]. Failure modes: data/lakehouse_planning/staging_families_20261003/failure_modes.md.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
LABELS_SEED = REPO_ROOT / "dbt/seeds/ipps_occmix_copy_labels.csv"
TWINS_SEED = REPO_ROOT / "dbt/seeds/ipps_occmix_twins.csv"
OVERRIDES = REPO_ROOT / "config/lakehouse/ipps_label_overrides.json"
TABLES = (
    "cms_ipps_text_lines",
    "cms_ipps_sheet_rows",
    "cms_ipps_sas",
    "cms_occupational_mix_text_lines",
    "cms_occupational_mix_text_lines_utf16",
    "cms_occupational_mix_sheet_rows",
)
# Each text table and the workbook table its twins are in; two text tables share one workbook table [199].
TEXT_TABLES = {
    "cms_ipps_text_lines": "cms_ipps_sheet_rows",
    "cms_occupational_mix_text_lines": "cms_occupational_mix_sheet_rows",
    "cms_occupational_mix_text_lines_utf16": "cms_occupational_mix_sheet_rows",
}
LABEL_COLUMNS = (
    "bronze_table",
    "object_key",
    "member_sha256",
    "snapshot_id",
    "label_source",
    "family",
    "role",
    "rule_fiscal_year",
    "data_fiscal_year",
    "identity_year",
    "rule_stage",
)
TWIN_COLUMNS = ("text_table", "text_sha256", "workbook_table", "workbook_sha256", "stem")
# Rule stages, each matched as a word pattern in a normalized name; a name may name several ("FR and CN") [178].
STAGES = (
    ("correction", r"correction|\bcn\d?\b|correcting|\bca\b"),
    ("interim", r"\bifc\b|interim"),
    ("proposed", r"nprm|propo?s?ed|\bpr\b|\bpr\d|preliminary|prelim"),
    ("final", r"final|\bfr\b|fr\d\d|\bfn\d\d"),
    ("notice", r"notice|supp"),
)
# Year patterns in order: four-digit FY, two-digit FY, the old abbreviated prefixes, then any bare 20xx.
YEARS = (
    r"fy\s?(\d{4})",
    r"fy\s?(\d{2})(?!\d)",
    r"(?:impfile?|impctf|imppuf|cmif|cmip|cmify|fr|nprm|fn)\s?(\d{2})(?!\d)",
    r"\b(20\d{2})\b",
)
# A CMI file names the rule it was published with; its FY token is then the discharge data year [178].
CMI_RULE = (r"\b(?:fr|pr|nprm|cn)\s?(20\d{2})\b", r"cmi[fp](\d{2})(?!\d)")
FAMILY_DROP = re.compile(
    r"\b(?:fy\s?\d{2,4}|\d+|v\d+|cms|ipps|puf|file|files|data|rule|the|and|of|for|by|with|"
    r"proposed|propsed|nprm|final|fr|cn\d?|ifc|interim|action|comment|correction|correcting|amendment|ca|notice|pr|"
    r"preliminary|prelim|supplemental|supp|january|february|march|april|may|june|july|august|september|october|november|"
    r"december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec|zip|txt|xlsx?|csv|results|tab)\b"
)
FAMILY_ALIASES = (("impfile", "impact"), ("impfil", "impact"), ("impctf", "impact"), ("imppuf", "impact"), ("cmify", "cmi"), ("cmif", "cmi"), ("cmip", "cmi"))


class LabelError(ValueError):
    """A stored file cannot be labelled or paired."""


@dataclass(frozen=True)
class Stored:
    """One loaded object with the names the S3 manifest gives it."""

    bronze_table: str
    object_key: str
    member_sha256: str
    snapshot_id: str
    container: str
    member_chain: tuple[str, ...]


def normalized(text: str) -> str:
    """Return a name in lower case with separators as spaces."""
    return re.sub(r"\s+", " ", re.sub(r"[_\-.()'#,]+", " ", text)).strip().lower()


def full_year(digits: str) -> int:
    """Return a two- or four-digit fiscal year as four digits; two-digit years from 80 are 19xx."""
    value = int(digits)
    if len(digits) == 4:
        return value
    return 1900 + value if value >= 80 else 2000 + value


def first_year(text: str) -> int | None:
    """Return the first fiscal year a name gives, trying the patterns in order."""
    name = normalized(text)
    for pattern in YEARS:
        match = re.search(pattern, name)
        if match:
            return full_year(match.group(1))
    return None


def cmi_years(member: str) -> tuple[int | None, int | None]:
    """Return a CMI file's rule year and data year from its own name, or (None, None) for other files."""
    name = normalized(member)
    if "cmi" not in name:
        return None, None
    rule = next((full_year(match.group(1)) for pattern in CMI_RULE if (match := re.search(pattern, name))), None)
    if rule is None:
        return None, None
    # The data year is the FY token just before "cmi", else the first FY token ("FY19 CMIs (FR 2021)", "FR 2019 CMIs (FY'17").
    token = re.search(r"fy\s?(\d{2,4})\s+cmis?\b", name) or re.search(r"fy\s?(\d{2,4})(?!\d)", name)
    data = full_year(token.group(1)) if token else None
    return rule, data if data != rule else None


def stages(text: str) -> list[str]:
    """Return the rule stages a name mentions."""
    name = normalized(text)
    return [stage for stage, pattern in STAGES if re.search(pattern, name)]


def family(member: str) -> str:
    """Return a descriptive family: the member's name without years, dates, stages, versions and extensions."""
    name = normalized(member.rsplit(".", 1)[0])
    for prefix, alias in FAMILY_ALIASES:
        name = re.sub(rf"\b{prefix}(?=\d|\b)", alias, name)
    words = FAMILY_DROP.sub(" ", name)
    return "_".join(words.split()) or "unnamed"


def label(stored: Stored, overrides: dict[tuple[str, str], dict[str, Any]]) -> dict[str, str]:
    """Return one object's labels, from its names or its committed override [178] [179]."""
    member = stored.member_chain[-1]
    chain = " ".join(stored.member_chain)
    override = overrides.get((stored.snapshot_id, member))
    rule, data = cmi_years(member)
    year = rule or first_year(member) or first_year(chain) or first_year(stored.container)
    stage = stages(member) or stages(chain) or stages(stored.container)
    source = "names"
    if override:
        year, stage, source = int(override["rule_fiscal_year"]), [override["rule_stage"]], "override"
    if year is None:
        raise LabelError(f"{stored.snapshot_id} {member!r}: no fiscal year in its names; add an override with its evidence")
    return {
        "bronze_table": stored.bronze_table,
        "object_key": stored.object_key,
        "member_sha256": stored.member_sha256,
        "snapshot_id": stored.snapshot_id,
        "label_source": source,
        "family": family(member),
        "role": "description" if re.search(r"descri|layout|file information", member, re.IGNORECASE) else "data",
        "rule_fiscal_year": str(year),
        "data_fiscal_year": str(data or ""),
        "identity_year": str(data or year),
        "rule_stage": "+".join(stage) or "unspecified",
    }


def stem(member: str) -> str:
    """Return a member's twin stem: its name without folder or extension, without case, spaces or trailing dots [183]."""
    return member.rsplit("/", 1)[-1].rsplit(".", 1)[0].strip().rstrip(".").strip().lower()


def twins(objects: Sequence[Stored], renamed: Iterable[tuple[str, str]] = ()) -> list[dict[str, str]]:
    """Return the text/workbook twin pairs: one text file and one workbook with the same stem in one snapshot [183].

    Reviewed pairs under different names (text SHA-256, workbook SHA-256) are added when both files share a capture
    [218] [220]; a renamed pair may not use a file another pair uses [219].
    """
    workbook_tables = set(TEXT_TABLES.values())
    groups: dict[tuple[str, str, str], dict[str, set[tuple[str, str]]]] = {}
    for stored in objects:
        if stored.bronze_table in TEXT_TABLES:
            side, workbook_table = "text", TEXT_TABLES[stored.bronze_table]
        elif stored.bronze_table in workbook_tables:
            side, workbook_table = "workbook", stored.bronze_table
        else:
            continue
        group = groups.setdefault((stored.snapshot_id, workbook_table, stem(stored.member_chain[-1])), {"text": set(), "workbook": set()})
        group[side].add((stored.bronze_table, stored.member_sha256))
    pairs: dict[tuple[str, str], dict[str, str]] = {}
    for (snapshot, workbook_table, name), group in sorted(groups.items()):
        if not group["text"] or not group["workbook"]:
            continue
        if len(group["text"]) > 1 or len(group["workbook"]) > 1:
            raise LabelError(f"{snapshot} {name!r}: {len(group['text'])} text files and {len(group['workbook'])} workbooks share one stem")
        (text_table, text_sha), (_, workbook_sha) = next(iter(group["text"])), next(iter(group["workbook"]))
        pairs.setdefault(
            (text_sha, workbook_sha),
            {"text_table": text_table, "text_sha256": text_sha, "workbook_table": workbook_table, "workbook_sha256": workbook_sha, "stem": name},
        )
    by_sha: dict[str, list[Stored]] = {}
    for stored in objects:
        by_sha.setdefault(stored.member_sha256, []).append(stored)
    # A republished text file can pair by name with two workbook versions; a renamed pair must use files no other pair uses.
    used = [sha for pair in pairs for sha in pair]
    for text_sha, workbook_sha in renamed:
        texts = [item for item in by_sha.get(text_sha, []) if item.bronze_table in TEXT_TABLES]
        books = [item for item in by_sha.get(workbook_sha, []) if item.bronze_table in workbook_tables]
        shared = sorted({item.snapshot_id for item in texts} & {item.snapshot_id for item in books})
        if not shared:
            raise LabelError(f"renamed pair {text_sha[:12]}/{workbook_sha[:12]}: the text file and workbook are not stored in one capture")
        text, book = texts[0], books[0]
        if TEXT_TABLES[text.bronze_table] != book.bronze_table:
            raise LabelError(f"renamed pair {text_sha[:12]}/{workbook_sha[:12]}: the text file and workbook belong to different families")
        if text_sha in used or workbook_sha in used:
            raise LabelError(f"renamed pair {text_sha[:12]}/{workbook_sha[:12]}: a file sits in two twin pairs")
        used += [text_sha, workbook_sha]
        pairs[(text_sha, workbook_sha)] = {
            "text_table": text.bronze_table,
            "text_sha256": text_sha,
            "workbook_table": book.bronze_table,
            "workbook_sha256": workbook_sha,
            "stem": "renamed: " + stem(text.member_chain[-1]),
        }
    return [pairs[key] for key in sorted(pairs)]


def load_renamed(path: Path = OVERRIDES) -> list[tuple[str, str]]:
    """Return the reviewed text/workbook pairs under different names, as (text SHA-256, workbook SHA-256) [220]."""
    return [(entry["text_sha256"], entry["workbook_sha256"]) for entry in json.loads(path.read_text()).get("renamed_twins", [])]


def load_overrides(path: Path = OVERRIDES) -> dict[tuple[str, str], dict[str, Any]]:
    """Return the committed overrides by snapshot and member name."""
    entries = json.loads(path.read_text())["overrides"]
    return {(entry["snapshot_id"], entry["member"]): entry for entry in entries}


def labels(objects: Iterable[Stored], overrides: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, str]]:
    """Return every object's labels, sorted, and refuse an override that matches no object."""
    items = list(objects)
    rows = sorted((label(stored, overrides) for stored in items), key=lambda row: (row["bronze_table"], row["object_key"]))
    used = {(stored.snapshot_id, stored.member_chain[-1]) for stored in items}
    stale = sorted(set(overrides) - used)
    if stale:
        raise LabelError(f"{stale[0]}: the override matches no stored object ({len(stale)} such overrides)")
    return rows


def as_csv(rows: Iterable[dict[str, str]], columns: Sequence[str]) -> str:
    """Return rows as CSV text with a header."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(columns), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def collect() -> list[Stored]:
    """Read every stored IPPS and occupational-mix copy and its container name from the S3 manifests, read-only."""
    from scripts.lakehouse import bronze
    from scripts.lakehouse.catalog import deployment

    settings = deployment()
    os.environ.setdefault("AWS_PROFILE", settings["aws_profile"])
    os.environ.setdefault("AWS_REGION", settings["aws_region"])
    storage = bronze.S3Storage()
    bucket = settings["data_bucket_name"]
    inputs, unselected = bronze.discover(storage, bucket, bronze.load_table_map(), TABLES, bronze.load_retired())
    # Bronze loads one copy of each file; the labels cover every stored copy, so conflicting names stay visible [210].
    copies = [*inputs, *(entry["copy"] for entry in unselected if "copy" in entry)]
    manifests: dict[tuple[str, str], dict[str, Any]] = {}
    objects = []
    for item in copies:
        source = (item["manifest_key"], item["manifest_version_id"])
        if source not in manifests:
            manifests[source] = json.loads(storage.get(bucket, *source))
        manifest = manifests[source]
        containers = {entry["artifact_id"]: entry.get("original_file_name", "") for entry in manifest.get("local_archive_containers", [])}
        stored = next(
            entry
            for entry in manifest["objects"]
            if entry["object"]["key"] == item["key"] and (entry.get("member_chain") or [item["key"].rsplit("/", 1)[-1]]) == item["member_chain"]
        )
        container = containers.get(stored.get("parent_artifact_id"), "")
        objects.append(Stored(item["table"], item["object_key"], item["sha256"], item["snapshot_id"], container, tuple(item["member_chain"])))
    return objects


def main() -> int:
    """Rebuild the labels and twin map from S3 and write or check the committed seeds."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="compare with the committed seeds instead of writing them")
    args = parser.parse_args()
    objects = collect()
    try:
        label_text = as_csv(labels(objects, load_overrides()), LABEL_COLUMNS)
        twin_text = as_csv(twins(objects, load_renamed()), TWIN_COLUMNS)
    except LabelError as error:
        sys.stderr.write(f"ipps file labels: {error}\n")
        return 1
    if args.check:
        same = LABELS_SEED.read_text() == label_text and TWINS_SEED.read_text() == twin_text
        sys.stdout.write(f"ipps file labels: {'committed seeds reproduced' if same else 'committed seeds differ from storage'}\n")
        return 0 if same else 1
    LABELS_SEED.write_text(label_text)
    TWINS_SEED.write_text(twin_text)
    sys.stdout.write(f"ipps file labels: {label_text.count(chr(10)) - 1} objects labelled; {twin_text.count(chr(10)) - 1} twin pairs\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
