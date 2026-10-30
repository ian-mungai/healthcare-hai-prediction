"""Build and check the receipt index of the data source verification log from the local capture receipts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition.source_registry import REPO_ROOT, RegistryError, read_json
from scripts.acquisition.source_verification_log import LOG_PATH, NOT_RECORDED

INDEX_PATH = REPO_ROOT / "config" / "acquisition" / "source_receipt_index.json"
GENERATED_BY = "scripts/acquisition/source_receipt_index.py"
ROOT_FIELDS = {"index_version", "generated_by", "generated_from", "rows"}
DATA_ROOT = REPO_ROOT / "data" / "datasets"
RECEIPT_FOLDERS = ("acquisition_batches", "historical_acquisition", "acquisition_recoveries", "snapshots")
INDEX_VERSION = 1
CONFIDENTIAL = "Confidential"
ROW_FIELDS = (
    "dataset_id",
    "data_class",
    "receipt_count",
    "receipt_set_sha256",
    "newest_receipt_sha256",
    "newest_retrieved_date",
    "newest_manifest_count",
    "newest_object_version",
    "verification_date",
)
COUNT_FIELDS = ("receipt_count", "newest_manifest_count")
# Fixed formats: a value that matches none of them cannot carry a name, a contact, a path or any other free text.
FORMATS = {
    "receipt_set_sha256": re.compile(r"[0-9a-f]{64}"),
    "newest_receipt_sha256": re.compile(r"[0-9a-f]{64}"),
    "newest_retrieved_date": re.compile(r"\d{4}-\d{2}-\d{2}"),
    "newest_object_version": re.compile(r"[A-Za-z0-9._~-]{8,128}"),
    "verification_date": re.compile(r"\d{4}-\d{2}-\d{2}"),
}
EARLIEST = datetime.min.replace(tzinfo=UTC)


class ReceiptIndexError(ValueError):
    """The receipts or the index cannot be used as they are."""


@dataclass(frozen=True)
class Receipt:
    """The facts of one local capture receipt that the index keeps."""

    dataset_id: str
    sha256: str
    retrieved: datetime
    verified: str | None
    version: str | None
    manifests: int


def text_sha256(text: str) -> str:
    """Return the SHA-256 of the text in UTF-8."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def parse_time(value: Any) -> datetime:
    """Return the time of an ISO 8601 text in UTC, or the earliest time when the receipt records none."""
    if not isinstance(value, str):
        return EARLIEST
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return EARLIEST
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def reconciliation_of(folder: Path) -> tuple[str | None, int]:
    """Return the object version of the capture's one manifest, and how many manifest objects the capture's storage record lists."""
    path = folder / "s3_collections_reconciliation.json"
    if not path.exists():
        return None, 0
    try:
        manifests = json.loads(path.read_text(encoding="utf-8")).get("manifests", [])
    except (OSError, ValueError) as error:
        raise ReceiptIndexError(f"{path.parent.name}: the storage record cannot be read ({type(error).__name__})") from error
    if len(manifests) != 1:
        return None, len(manifests)
    version = manifests[0].get("object", {}).get("version_id")
    return (version if isinstance(version, str) else None), 1


def read_receipts(root: Path = DATA_ROOT) -> dict[str, list[Receipt]]:
    """Read every capture receipt under the dataset folders: one entry per distinct receipt file, grouped by dataset ID.

    A receipt that records a failed attempt names no source and is left out. Symbolic links are not followed.
    """
    found: dict[str, dict[str, Receipt]] = {}
    for name in RECEIPT_FOLDERS:
        for folder, _, files in sorted(os.walk(root / name)):
            if "receipt.json" not in files:
                continue
            content = (Path(folder) / "receipt.json").read_bytes()
            try:
                receipt = json.loads(content)
            except ValueError as error:
                raise ReceiptIndexError(f"{Path(folder).name}: the receipt is not JSON ({type(error).__name__})") from error
            source = receipt.get("source") if isinstance(receipt, dict) else None
            dataset_id = source.get("source_record_id") if isinstance(source, dict) else None
            if not isinstance(dataset_id, str):
                continue
            version, manifests = reconciliation_of(Path(folder))
            verified = (receipt.get("verification") or {}).get("verified_at_utc")
            digest = hashlib.sha256(content).hexdigest()
            found.setdefault(dataset_id, {})[digest] = Receipt(
                dataset_id,
                digest,
                parse_time((receipt.get("acquisition") or {}).get("retrieved_at_utc")),
                parse_time(verified).date().isoformat() if verified else None,
                version,
                manifests,
            )
    return {dataset_id: sorted(items.values(), key=lambda item: (item.retrieved, item.sha256)) for dataset_id, items in found.items()}


def build_row(dataset_id: str, data_class: str, receipts: list[Receipt]) -> dict[str, Any]:
    """Build the index row of one dataset from its receipts, oldest first."""
    if not receipts:
        return dict.fromkeys(ROW_FIELDS, NOT_RECORDED) | {"dataset_id": dataset_id, "data_class": data_class, "receipt_count": 0, "newest_manifest_count": 0}
    newest = receipts[-1]
    verified = sorted(item.verified for item in receipts if item.verified)
    return {
        "dataset_id": dataset_id,
        "data_class": data_class,
        "receipt_count": len(receipts),
        "receipt_set_sha256": text_sha256("".join(item.sha256 + "\n" for item in sorted(receipts, key=lambda item: item.sha256))),
        "newest_receipt_sha256": newest.sha256,
        "newest_retrieved_date": newest.retrieved.date().isoformat() if newest.retrieved != EARLIEST else NOT_RECORDED,
        "newest_manifest_count": newest.manifests,
        "newest_object_version": newest.version or NOT_RECORDED,
        "verification_date": verified[-1] if verified else NOT_RECORDED,
    }


def build_index(log: dict[str, Any], log_sha256: str, receipts: dict[str, list[Receipt]]) -> dict[str, Any]:
    """Build the whole index: one row per dataset of the log, sorted by dataset ID. A receipt of a dataset outside the log stops the build."""
    classes = {row["dataset_id"]: row["data_class"] for row in log["rows"]}
    unlisted = sorted(set(receipts) - set(classes))
    if unlisted:
        raise ReceiptIndexError(f"receipts name datasets that the log does not list: {', '.join(unlisted)}")
    return {
        "index_version": INDEX_VERSION,
        "generated_by": GENERATED_BY,
        "generated_from": {"log_sha256": log_sha256},
        "rows": [build_row(dataset_id, classes[dataset_id], receipts.get(dataset_id, [])) for dataset_id in sorted(classes)],
    }


def render(index: dict[str, Any]) -> str:
    """Return the committed text form of the index."""
    return json.dumps(index, indent=2, ensure_ascii=False) + "\n"


def check_row(row: dict[str, Any], log_class: str | None) -> list[str]:
    """Return the problems of one row. A Confidential row is named as such: it may hold only counts, hashes, an object version and dates."""
    name = str(row.get("dataset_id", "?"))
    prefix = f"{CONFIDENTIAL} " if log_class == CONFIDENTIAL else ""
    problems = [f"{prefix}{name}: unexpected field {field}" for field in sorted(set(row) - set(ROW_FIELDS))]
    problems.extend(f"{prefix}{name}: missing field {field}" for field in ROW_FIELDS if field not in row)
    if row.get("data_class") != log_class:
        problems.append(f"{prefix}{name}: data_class {row.get('data_class')!r} differs from the log")
    for field in COUNT_FIELDS:
        value = row.get(field)
        if field in row and (not isinstance(value, int) or isinstance(value, bool) or value < 0):
            problems.append(f"{prefix}{name}: {field} is not a count")
    for field, pattern in FORMATS.items():
        value = row.get(field)
        if field in row and value != NOT_RECORDED and not (isinstance(value, str) and pattern.fullmatch(value)):
            problems.append(f"{prefix}{name}: {field} is not a fixed-format value")
    if row.get("receipt_count") == 0 and any(row.get(field) != NOT_RECORDED for field in FORMATS):
        problems.append(f"{prefix}{name}: a dataset with no receipts holds a receipt fact")
    return problems


def check_index(index: dict[str, Any], log: dict[str, Any], log_sha256: str) -> list[str]:
    """Return every problem of the index against the committed log; an empty list is a pass."""
    problems = []
    if index.get("index_version") != INDEX_VERSION:
        problems.append(f"index_version is not {INDEX_VERSION}")
    problems.extend(f"unexpected field {field}" for field in sorted(set(index) - ROOT_FIELDS))
    if index.get("generated_by") != GENERATED_BY:
        problems.append(f"generated_by is not {GENERATED_BY}")
    generated_from = index.get("generated_from", {})
    problems.extend(f"unexpected field generated_from.{field}" for field in sorted(set(generated_from) - {"log_sha256"}))
    if generated_from.get("log_sha256") != log_sha256:
        problems.append("stale evidence: generated_from.log_sha256 no longer matches the verification log")
    classes = {row["dataset_id"]: row["data_class"] for row in log["rows"]}
    rows = index.get("rows", [])
    seen = [row.get("dataset_id") for row in rows]
    problems.extend(f"duplicate row for {name}" for name in sorted({name for name in seen if seen.count(name) > 1}))
    problems.extend(f"missing row for {name}" for name in sorted(classes) if name not in seen)
    problems.extend(f"unexpected row for {name}" for name in seen if name not in classes)
    for row in rows:
        problems.extend(check_row(row, classes.get(row.get("dataset_id"))))
    return problems


def write_index(index: dict[str, Any], log: dict[str, Any], log_sha256: str, path: Path = INDEX_PATH) -> bool:
    """Write the index through a temporary file after the check passes, never before; return whether the file changed."""
    problems = check_index(index, log, log_sha256)
    if problems:
        raise ReceiptIndexError("the index fails its check and is not written: " + "; ".join(problems))
    text = render(index)
    if path.exists() and path.read_text(encoding="utf-8") == text:
        return False
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)
    return True


def main() -> None:
    """Write the index from the local receipts, or with --check validate the committed index against the committed log."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="check the committed index and change nothing")
    parser.add_argument("--data-root", type=Path, default=DATA_ROOT, help="folder that holds the local dataset folders")
    arguments = parser.parse_args()
    try:
        log = read_json(LOG_PATH)
        log_sha256 = text_sha256(LOG_PATH.read_text(encoding="utf-8"))
        if not arguments.check:
            index = build_index(log, log_sha256, read_receipts(arguments.data_root))
            changed = write_index(index, log, log_sha256)
            sys.stdout.write(f"Receipt index {'written' if changed else 'unchanged'}: {INDEX_PATH.relative_to(REPO_ROOT)}\n")
            return
        index = read_json(INDEX_PATH)
    except (RegistryError, ReceiptIndexError) as error:
        parser.exit(1, f"{error}\n")
    problems = check_index(index, log, log_sha256)
    if INDEX_PATH.read_text(encoding="utf-8") != render(index):
        problems.append("the file text is not the canonical serialization of its content; run python -m scripts.acquisition.source_receipt_index")
    if problems:
        parser.exit(1, "Receipt index check failed:\n" + "\n".join(f"- {problem}" for problem in problems) + "\n")
    sys.stdout.write(f"Receipt index valid: {len(index['rows'])} rows\n")


if __name__ == "__main__":
    main()
