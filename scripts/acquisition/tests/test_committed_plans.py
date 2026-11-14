"""Every committed collector plan loads through its contract, so a stale lock or plan chain fails the gate (failure mode 261).

Several contracts read the collected captures the plans name, which live in the ignored data/ folder. A clean
checkout, as in GitHub CI, has none, so the test is skipped there and runs in the local gate.

A local capture may have lost its file after S3 verified the object (the folder keeps receipt.json and the storage
record). A plan whose first unreadable file is such a file is skipped and the skip reason names the capture.
Any other missing file fails the test: no record, an unverified object or a different checksum proves nothing.
"""

from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest

from scripts.acquisition.redownload import PLANNED
from scripts.acquisition.source_registry import REPO_ROOT, read_json

COLLECTED = REPO_ROOT / "data/datasets"
RECONCILIATION = "s3_collections_reconciliation.json"
VERIFIED = "version_get_sha256_and_length_match"


def verified_objects(record: Path) -> list[dict]:
    """The stored objects of a capture's storage record that S3 returned with the recorded checksum and length."""
    objects = read_json(record).get("objects", [])
    return [{**line["object"], "storage_path": line.get("storage_path")} for line in objects if line.get("object", {}).get("verification") == VERIFIED]


def pruned_capture_file(missing: Path) -> str | None:
    """The stored object that proves a capture file was deleted after S3 verified it, or None when nothing proves that."""
    folder = missing.parent
    while folder != COLLECTED and COLLECTED in folder.parents:
        if (folder / RECONCILIATION).is_file() and (folder / "receipt.json").is_file():
            break
        folder = folder.parent
    else:
        return None
    stored_path = missing.relative_to(folder).as_posix()
    wanted = [item["sha256"] for item in read_json(folder / "receipt.json").get("artifacts", []) if item.get("storage_path") == stored_path]
    stored = [item for item in verified_objects(folder / RECONCILIATION) if item["storage_path"] == stored_path and item["sha256"] in wanted]
    return f"{folder.relative_to(COLLECTED).as_posix()}: {stored[0]['key']}" if stored else None


def pruned_reference_file(missing: Path) -> str | None:
    """The stored object of a reference file (attempt_NN/group.json): its receipt names the checksum and a storage record of the dataset verifies it."""
    receipt_path = missing.parent.parent / "receipt.json"
    if missing.parent.name.startswith("attempt_") and receipt_path.is_file() and (dataset := missing.relative_to(COLLECTED).parts[:2]):
        receipt = read_json(receipt_path)
        if Path(receipt.get("path", "")).parts[-3:] == missing.parts[-3:] and receipt.get("sha256"):
            for record in sorted((COLLECTED.joinpath(*dataset)).rglob(RECONCILIATION)):
                for item in verified_objects(record):
                    if item["sha256"] == receipt["sha256"]:
                        return f"{missing.parent.parent.relative_to(COLLECTED).as_posix()}: {item['key']}"
    return None


def pruned_note(error: FileNotFoundError) -> str | None:
    """Name the pruned capture file behind a missing-file error, or None for any other missing file."""
    if not isinstance(error.filename, str):
        return None
    missing = Path(os.path.abspath(error.filename))
    if COLLECTED not in missing.parents:
        return None
    return pruned_capture_file(missing) or pruned_reference_file(missing)


@pytest.mark.skipif(not COLLECTED.is_dir(), reason="the committed plans read collected captures under data/, absent in a clean checkout")
@pytest.mark.parametrize("mode", sorted(PLANNED))
def test_committed_plan_loads(mode: str) -> None:
    """Load the committed plan the redownload preflight reads for this collector."""
    module_name, loader = PLANNED[mode][:2]
    try:
        getattr(importlib.import_module(f"scripts.acquisition.{module_name}"), loader)()
    except FileNotFoundError as error:
        note = pruned_note(error)
        if note is None:
            raise
        pytest.skip(f"{mode}: capture file deleted locally after S3 verified it ({note}); the plan cannot load without it")
