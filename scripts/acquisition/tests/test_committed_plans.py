"""Every committed collector plan loads through its contract, so a stale lock or plan chain fails the gate (failure mode 261).

Several contracts read the collected captures the plans name, which live in the ignored data/ folder. A clean
checkout, as in GitHub CI, has none, so the test is skipped there and runs in the local gate.

A local capture may have lost its file after S3 verified the object (the folder keeps receipt.json and the storage
record). The loader still runs to its end: a deleted file is read as a stand-in that carries the checksum S3 verified, so the
loader's own comparison with the plan's expected checksum runs, as do the locks, the plan chain and every other metadata check.
A deleted dictionary is rebuilt from the headers of the capture receipt that stored it. Only the checks that need the deleted
bytes (the dictionary labels, the payload) cannot run, and the plan is then skipped with a reason that names the capture.
Any other missing file fails the test: no record, an unverified object or a different checksum proves nothing.
"""

from __future__ import annotations

import importlib
import json
import os
from collections.abc import Callable
from pathlib import Path

import pytest

from scripts.acquisition import census_acs_api_contract, census_acs_detailed_contract
from scripts.acquisition.redownload import PLANNED
from scripts.acquisition.source_registry import REPO_ROOT, read_json

COLLECTED = REPO_ROOT / "data/datasets"
RECONCILIATION = "s3_collections_reconciliation.json"
VERIFIED = "version_get_sha256_and_length_match"
# A label that the English-line selection of both detailed tables accepts; the labels of a deleted dictionary are unknown.
ENGLISH_LABEL = 'less than "very well" Speak English "well"'


class Stored(bytes):
    """Stand-in for a capture file deleted after S3 verified it; `sha256` is the checksum S3 returned."""

    sha256: str


def verified_objects(record: Path) -> list[dict]:
    """The stored objects of a capture's storage record that S3 returned with the recorded checksum and length."""
    objects = read_json(record).get("objects", [])
    return [{**line["object"], "storage_path": line.get("storage_path")} for line in objects if line.get("object", {}).get("verification") == VERIFIED]


def pruned_capture_file(missing: Path) -> tuple[str, str] | None:
    """The capture and stored object (label, sha256) that prove a capture file was deleted after S3 verified it, or None."""
    folder = missing.parent
    while folder != COLLECTED and COLLECTED in folder.parents:
        if (folder / RECONCILIATION).is_file() and (folder / "receipt.json").is_file():
            break
        folder = folder.parent
    else:
        return None
    stored_path = missing.relative_to(folder).as_posix()
    artifacts = read_json(folder / "receipt.json").get("artifacts", [])
    wanted = [item["sha256"] for item in artifacts if item.get("storage_path") == stored_path]
    # an expanded archive member is stored under the checksum of the archive artifact that the receipt lists
    expanded = (
        {f"mapped_expanded/{item['sha256']}/{stored_path.removeprefix('history_members/')}" for item in artifacts}
        if stored_path.startswith("history_members/")
        else set()
    )
    stored = [
        item
        for item in verified_objects(folder / RECONCILIATION)
        if (item["storage_path"] == stored_path and item["sha256"] in wanted) or item["storage_path"] in expanded
    ]
    return (f"{folder.relative_to(COLLECTED).as_posix()}: {stored[0]['key']}", stored[0]["sha256"]) if stored else None


def pruned_reference_file(missing: Path) -> tuple[str, str] | None:
    """The same proof for a reference file (attempt_NN/group.json): its receipt names the checksum and a storage record of the dataset verifies it."""
    receipt_path = missing.parent.parent / "receipt.json"
    if missing.parent.name.startswith("attempt_") and receipt_path.is_file() and (dataset := missing.relative_to(COLLECTED).parts[:2]):
        receipt = read_json(receipt_path)
        if Path(receipt.get("path", "")).parts[-3:] == missing.parts[-3:] and receipt.get("sha256"):
            for record in sorted((COLLECTED.joinpath(*dataset)).rglob(RECONCILIATION)):
                for item in verified_objects(record):
                    if item["sha256"] == receipt["sha256"]:
                        return f"{missing.parent.parent.relative_to(COLLECTED).as_posix()}: {item['key']}", item["sha256"]
    return None


def dictionary_content(dataset: tuple[str, ...], sha256: str) -> bytes:
    """A deleted group dictionary as its variable names, taken from the capture receipt that stored it; empty when no receipt did."""
    for path in sorted(COLLECTED.joinpath(*dataset).rglob("receipt.json")):
        receipt = read_json(path)
        if any(a.get("role") == "dictionary" and a.get("sha256") == sha256 for a in receipt.get("artifacts", [])):
            batch_id = json.loads(receipt["lineage"]["extraction_or_query"]).get("batch_id")
            plan = read_json(census_acs_detailed_contract.PLAN_PATH)
            lines: list[str] = next((b.get("less_lines", []) for b in plan["batches"] if b["id"] == batch_id), [])
            names = [h for h in receipt["schema_profile"]["native_headers"] if h not in {"state", "county"}]
            return json.dumps({"variables": {n: {"label": ENGLISH_LABEL if n in lines else ""} for n in names}}).encode()
    return b""


def reading_stored_files(real: Callable[[Path], bytes], used: list[str]) -> Callable[[Path], bytes]:
    """Path.read_bytes that returns a Stored stand-in for a file proven deleted after S3 verified it, and re-raises any other missing file."""

    def read_bytes(self: Path) -> bytes:
        try:
            return real(self)
        except FileNotFoundError:
            missing = Path(os.path.abspath(self))
            proof = pruned_capture_file(missing) or pruned_reference_file(missing) if COLLECTED in missing.parents else None
            if proof is None:
                raise
            used.append(proof[0])
            stand_in = Stored(dictionary_content(missing.relative_to(COLLECTED).parts[:2], proof[1]))
            stand_in.sha256 = proof[1]
            return stand_in

    return read_bytes


@pytest.mark.skipif(not COLLECTED.is_dir(), reason="the committed plans read collected captures under data/, absent in a clean checkout")
@pytest.mark.parametrize("mode", sorted(PLANNED))
def test_committed_plan_loads(mode: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Load the committed plan the redownload preflight reads for this collector."""
    module_name, loader = PLANNED[mode][:2]
    used: list[str] = []
    monkeypatch.setattr(Path, "read_bytes", reading_stored_files(Path.read_bytes, used))
    for contract in (census_acs_api_contract, census_acs_detailed_contract):
        monkeypatch.setattr(contract, "digest", lambda body, real=contract.digest: body.sha256 if isinstance(body, Stored) else real(body))
    getattr(importlib.import_module(f"scripts.acquisition.{module_name}"), loader)()
    if used:
        pytest.skip(
            f"{mode}: {len(used)} capture files deleted locally after S3 verified them, first {used[0]}; every check that needs no deleted bytes passed"
        )
