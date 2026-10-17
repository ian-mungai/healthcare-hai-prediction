"""Write and check the dataset label seed of the gold source dimension from the source registry.

Run from the repository root:

    .venv/bin/python -m scripts.lakehouse.source_labels --write    # write dbt/seeds/gold/source_labels.csv
    .venv/bin/python -m scripts.lakehouse.source_labels --check    # fail when the registry documents are inconsistent or the seed drifts from them

``config/acquisition/registry_additions.json`` places each bronze ``dataset_id`` under a registry source (``source_metadata`` with its ``publisher`` and
``dataset_ids``; ``dataset_id_placements`` repeats the same placements with the rule that made each one) and lists the IDs no rule places
(``unplaced_dataset_ids``). ``config/acquisition/source_registry.json`` gives each source its ``title``. A placed ID gets its source's publisher and title
as ``label``. A listed unplaced ID gets a row with an empty publisher and label, so the seed says that the registry was asked and has no answer; the source
dimension keeps such a dataset with null attributes and never guesses a label or routes it to the unknown member. An ID in neither list has no row, and a
dbt test fails the build for it. No value is typed by hand.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY = REPO_ROOT / "config/acquisition/source_registry.json"
ADDITIONS = REPO_ROOT / "config/acquisition/registry_additions.json"
SEED = REPO_ROOT / "dbt/seeds/gold/source_labels.csv"
SEED_COLUMNS = ("dataset_id", "publisher", "label", "source_id", "placement")
ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9_]{0,119}")


class LabelError(ValueError):
    """The registry documents are inconsistent; the generator writes nothing."""


def read_documents(registry: Path = REGISTRY, additions: Path = ADDITIONS) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return the parsed registry and registry additions."""
    return json.loads(registry.read_text()), json.loads(additions.read_text())


def text_of(record: dict[str, Any], key: str) -> str:
    """Return a record's text value, stripped; an absent or non-text value is empty."""
    value = record.get(key)
    return value.strip() if isinstance(value, str) else ""


def build_rows(registry: dict[str, Any], additions: dict[str, Any]) -> list[dict[str, str]]:
    """Return the seed rows sorted by dataset ID; stop with the reason when the documents disagree."""
    titles = {text_of(record, "source_id"): text_of(record, "title") for record in registry["sources"]}
    placed: dict[str, dict[str, str]] = {}
    for entry in additions["source_metadata"]:
        source_id = text_of(entry, "source_id")
        if source_id not in titles:
            raise LabelError(f"source {source_id!r} of source_metadata is not in the registry")
        publisher = text_of(entry, "publisher")
        if not publisher:
            raise LabelError(f"source {source_id} has no publisher in source_metadata")
        if not titles[source_id]:
            raise LabelError(f"source {source_id} has no title in the registry")
        for dataset_id in entry["dataset_ids"]:
            if not isinstance(dataset_id, str) or not ID_PATTERN.fullmatch(dataset_id):
                raise LabelError(f"{dataset_id!r} of source {source_id} is not a valid dataset ID")
            if dataset_id in placed:
                raise LabelError(f"dataset {dataset_id} is placed by two sources: {placed[dataset_id]['source_id']} and {source_id}")
            placed[dataset_id] = {"dataset_id": dataset_id, "publisher": publisher, "label": titles[source_id], "source_id": source_id, "placement": "placed"}
    listed = {(item["dataset_id"], item["source_id"]) for item in additions["dataset_id_placements"]}
    if listed != {(dataset_id, row["source_id"]) for dataset_id, row in placed.items()}:
        raise LabelError("dataset_id_placements and source_metadata disagree")
    rows = list(placed.values())
    unplaced: set[str] = set()
    for item in additions["unplaced_dataset_ids"]:
        dataset_id = item["dataset_id"]
        if dataset_id in placed:
            raise LabelError(f"dataset {dataset_id} is both placed and unplaced")
        if dataset_id in unplaced:
            raise LabelError(f"dataset {dataset_id} is listed unplaced twice")
        if not ID_PATTERN.fullmatch(dataset_id):
            raise LabelError(f"{dataset_id!r} is not a valid dataset ID")
        unplaced.add(dataset_id)
        rows.append({"dataset_id": dataset_id, "publisher": "", "label": "", "source_id": "", "placement": "unplaced"})
    return sorted(rows, key=lambda row: row["dataset_id"])


def render(rows: list[dict[str, str]]) -> str:
    """Return the seed text: Unix line endings and a fixed column order."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=SEED_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def write(seed: Path, registry: dict[str, Any], additions: dict[str, Any]) -> None:
    """Write the seed; inconsistent documents write nothing."""
    text = render(build_rows(registry, additions))
    seed.parent.mkdir(parents=True, exist_ok=True)
    seed.write_text(text)


def problems_of(registry: dict[str, Any], additions: dict[str, Any], seed: Path) -> list[str]:
    """Return the problems of the documents and the difference between the seed and what they generate; an empty list is a pass."""
    try:
        wanted = render(build_rows(registry, additions))
    except LabelError as error:
        return [str(error)]
    if not seed.is_file():
        return [f"{seed.name} is missing; run --write"]
    if seed.read_text() != wanted:
        return [f"{seed.name} differs from what the registry generates; run --write"]
    return []


def check(registry: Path = REGISTRY, additions: Path = ADDITIONS, seed: Path = SEED) -> list[str]:
    """Check the committed documents against the committed seed."""
    registry_document, additions_document = read_documents(registry, additions)
    return problems_of(registry_document, additions_document, seed)


def main() -> int:
    """Write or check the seed and report the counts."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true", help="write the seed from the registry documents")
    mode.add_argument("--check", action="store_true", help="fail when the documents are inconsistent or the seed differs from them")
    args = parser.parse_args()
    registry, additions = read_documents()
    if args.write:
        try:
            write(SEED, registry, additions)
        except LabelError as error:
            sys.stderr.write(f"source labels: {error}\n")
            return 1
    problems = problems_of(registry, additions, SEED)
    for problem in problems:
        sys.stderr.write(f"source labels: {problem}\n")
    rows = [] if problems else build_rows(registry, additions)
    placed = sum(1 for row in rows if row["placement"] == "placed")
    sys.stdout.write(f"source labels: {len(rows)} datasets, {placed} placed, {len(rows) - placed} unplaced; {len(problems)} problems\n")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
