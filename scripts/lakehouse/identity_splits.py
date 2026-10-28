"""Write and check the identity rule seed of the gold dimensions from the approved classification list.

Run from the repository root:

    .venv/bin/python -m scripts.lakehouse.identity_splits --write    # write dbt/seeds/gold/identity_splits.csv
    .venv/bin/python -m scripts.lakehouse.identity_splits --check    # fail when the list is invalid or the seed drifts from it

``config/lakehouse/identity_classification.csv`` is the approved list: one row per code or measure whose label changed across releases, with its class,
its approval status, the identity rule and the public sources that support the class. Each class has one rule: ``redefined`` gives ``split`` (one identity
per edition), ``renamed`` gives ``single_latest`` (one identity with the latest label) and ``unresolved`` gives ``hold_single_latest`` (one identity with
the latest label until the repository owner resolves it). A ``redefined`` or ``renamed`` row must be ``approved`` and an ``unresolved`` row must be
``owner_hold``; a
``proposed_for_approval`` row stops the generator, so an unapproved key cannot reach a model. The list holds codes, classes, statuses and public
citations only: no approval dates or private decision records, because the repository is public.
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CLASSIFICATION = REPO_ROOT / "config/lakehouse/identity_classification.csv"
SEED = REPO_ROOT / "dbt/seeds/gold/identity_splits.csv"
CLASSIFICATION_COLUMNS = (
    "dimension",
    "code_system",
    "fact",
    "code",
    "classification",
    "approval_status",
    "identity_rule",
    "source_ids",
    "citation_urls",
    "locators",
    "scope_limit",
)
SEED_COLUMNS = ("dimension", "code_system", "fact", "code", "classification", "approval_status", "identity_rule")
DIMENSIONS = ("dim_drg", "dim_measure", "dim_rurality_code")
RULE_OF_CLASS = {"redefined": "split", "renamed": "single_latest", "unresolved": "hold_single_latest"}
STATUS_OF_CLASS = {"redefined": "approved", "renamed": "approved", "unresolved": "owner_hold"}
STATUSES = ("approved", "owner_hold", "proposed_for_approval")
URL_PATTERN = re.compile(r"https://\S+")


class IdentityError(ValueError):
    """The classification list is invalid; the generator writes nothing."""


def read_classification(path: Path = CLASSIFICATION) -> list[dict[str, str]]:
    """Return the rows of the classification list; stop at the first invalid row."""
    if not path.is_file():
        raise IdentityError(f"{path.name} is missing")
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        header = tuple(reader.fieldnames or ())
        records = list(reader)
    if any(None in record for record in records):
        raise IdentityError(f"{path.name}: a row has more fields than the header")
    rows = [{name: value or "" for name, value in record.items()} for record in records]
    if header != CLASSIFICATION_COLUMNS:
        extra = sorted(set(header) - set(CLASSIFICATION_COLUMNS))
        missing = sorted(set(CLASSIFICATION_COLUMNS) - set(header))
        raise IdentityError(f"{path.name}: unexpected columns {extra}, missing columns {missing}; the list holds codes, classes, statuses and citations only")
    seen: set[tuple[str, str, str, str]] = set()
    for number, row in enumerate(rows, start=2):
        validate_row(row, f"{path.name} line {number}")
        key = (row["dimension"], row["code_system"], row["fact"], row["code"])
        if key in seen:
            raise IdentityError(f"{path.name} line {number}: {key} is listed twice")
        seen.add(key)
    return rows


def validate_row(row: dict[str, str], where: str) -> None:
    """Stop with the reason when a row breaks a rule of the list."""
    name = f"{row['dimension']} {row['code']}"
    if row["dimension"] not in DIMENSIONS:
        raise IdentityError(f"{where}: dimension {row['dimension']!r} is not one of {list(DIMENSIONS)}")
    for column in ("code_system", "code"):
        if not row[column].strip():
            raise IdentityError(f"{where}: {name} has no {column}")
    if row["dimension"] == "dim_measure" and not row["fact"]:
        raise IdentityError(f"{where}: {name} needs a fact")
    if row["dimension"] != "dim_measure" and row["fact"]:
        raise IdentityError(f"{where}: {name} must not name a fact")
    if row["classification"] not in RULE_OF_CLASS:
        raise IdentityError(f"{where}: classification {row['classification']!r} is not one of {list(RULE_OF_CLASS)}")
    if row["approval_status"] not in STATUSES:
        raise IdentityError(f"{where}: approval_status {row['approval_status']!r} is not one of {list(STATUSES)}")
    if row["approval_status"] == "proposed_for_approval":
        raise IdentityError(f"{where}: {name} is proposed_for_approval; only an approved class or an owner hold reaches a model")
    expected_rule, expected_status = RULE_OF_CLASS[row["classification"]], STATUS_OF_CLASS[row["classification"]]
    if row["identity_rule"] != expected_rule:
        raise IdentityError(f"{where}: {name} class {row['classification']} needs identity_rule {expected_rule}, not {row['identity_rule']!r}")
    if row["approval_status"] != expected_status:
        raise IdentityError(f"{where}: {name} class {row['classification']} needs approval_status {expected_status}, not {row['approval_status']!r}")
    if not row["source_ids"].strip():
        raise IdentityError(f"{where}: {name} has no source ID")
    urls = row["citation_urls"].split()
    if not urls:
        raise IdentityError(f"{where}: {name} has no citation URL")
    bad = [url for url in urls if not URL_PATTERN.fullmatch(url)]
    if bad:
        raise IdentityError(f"{where}: {name} citation {bad[0]!r} is not an https URL")
    if not row["locators"].strip():
        raise IdentityError(f"{where}: {name} has no locator")


def seed_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Return the seed rows in a fixed order: the list's key columns, class, status and rule."""
    seed = [{column: row[column] for column in SEED_COLUMNS} for row in rows]
    return sorted(seed, key=lambda row: (row["dimension"], row["code_system"], row["fact"], row["code"]))


def render(rows: list[dict[str, str]]) -> str:
    """Return the seed text: Unix line endings and a fixed column order."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=SEED_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(seed_rows(rows))
    return buffer.getvalue()


def write(classification: Path = CLASSIFICATION, seed: Path = SEED) -> None:
    """Write the seed; an invalid list writes nothing."""
    text = render(read_classification(classification))
    seed.parent.mkdir(parents=True, exist_ok=True)
    seed.write_text(text)


def check(classification: Path = CLASSIFICATION, seed: Path = SEED) -> list[str]:
    """Return the problems of the list and the difference between the seed and what the list generates; an empty list is a pass."""
    try:
        wanted = render(read_classification(classification))
    except IdentityError as error:
        return [str(error)]
    if not seed.is_file():
        return [f"{seed.name} is missing; run --write"]
    if seed.read_text() != wanted:
        return [f"{seed.name} differs from what the classification list generates; run --write"]
    return []


def main() -> int:
    """Write or check the seed and report the counts by rule."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true", help="write the seed from the classification list")
    mode.add_argument("--check", action="store_true", help="fail when the list is invalid or the seed differs from it")
    args = parser.parse_args()
    if args.write:
        try:
            write()
        except IdentityError as error:
            sys.stderr.write(f"identity splits: {error}\n")
            return 1
    problems = check()
    for problem in problems:
        sys.stderr.write(f"identity splits: {problem}\n")
    rows = read_classification() if not problems else []
    counts = {rule: sum(1 for row in rows if row["identity_rule"] == rule) for rule in RULE_OF_CLASS.values()}
    sys.stdout.write(f"identity splits: {len(rows)} keys {counts}; {len(problems)} problems\n")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
