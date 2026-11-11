"""The real bronze table map selects the stored files of both capture versions of the ONC attestation and HCAI utilization collections."""

from __future__ import annotations

import hashlib
import json

import pytest

from scripts.lakehouse import bronze
from tests.support import check
from tests.test_bronze_raw_roots import BUCKET, Storage

ONC = "onc/promoting_interoperability"
HCAI = "california_hcai/hospital_utilization"
EVIDENCE = (("evidence/download_proof.json", "export_receipt"), ("references/scope.json", "layout"))


def store_capture(storage: Storage, collection: str, snapshot: str, source: str, files: list[tuple[str, str, bytes]]) -> None:
    """Write one capture as the collectors do: each file under the capture folder, then the manifest that lists them."""
    objects = []
    for name, role, data in files:
        digest = hashlib.sha256(data).hexdigest()
        key = f"{collection}/datasets/{snapshot}/release_date=2026-01-01/{digest}/{name.rsplit('/', 1)[-1]}"
        version = storage.put(key, data)
        objects.append(
            {"role": role, "dataset_id": snapshot, "object": {"bucket": BUCKET, "key": key, "version_id": version, "sha256": digest, "byte_count": len(data)}}
        )
    manifest = {"snapshot_id": snapshot, "source_id": source, "dataset_id": snapshot, "release_id": "2026-01-01", "objects": objects}
    storage.put(f"{collection}/manifests/release_date=2026-01-01/{snapshot}/{snapshot}/manifest.json", json.dumps(manifest).encode())


def selected(storage: Storage, collection: str) -> dict[str, list[str]]:
    """Return each loaded file name by the table of the real map that selects it, over every table of the collection."""
    config = bronze.load_table_map()
    names = [table["table"] for table in config["tables"] if table["collection"] == collection]
    inputs, _ = bronze.discover(storage, BUCKET, config, names)
    found: dict[str, list[str]] = {}
    for item in inputs:
        found.setdefault(item["table"], []).append(item["file_name"])
    return {table: sorted(files) for table, files in found.items()}


@pytest.fixture(autouse=True)
def root_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HAI_READ_RAW_ROOT", raising=False)


def evidence(extra: bytes = b"{}") -> list[tuple[str, str, bytes]]:
    return [(name, role, extra) for name, role in EVIDENCE]


def test_onc_attestations_load_from_both_capture_versions() -> None:
    storage = Storage()
    store_capture(storage, ONC, "ONC_PI__v1", "ONC_PI", [("derived/hospital_attestations.csv", "data", b"CCN,Provider_Type\n1,Hospital\n"), *evidence()])
    store_capture(storage, ONC, "ONC_PI__v2", "ONC_PI", [("raw/mu_report.csv", "data", b"NPI,CCN,Provider_Type,Specialty\n9,1,Hospital,\n"), *evidence(b"[]")])
    found = selected(storage, ONC)
    check(found == {"onc_pi_attestations_csv": ["hospital_attestations.csv", "mu_report.csv"]}, f"Both versions load into the attestation table: {found}")


def test_hcai_utilization_loads_the_version_one_pages_and_the_version_two_workbook() -> None:
    storage = Storage()
    store_capture(
        storage,
        HCAI,
        "UTIL__v1",
        "HCAI_UTIL",
        [("derived/page_1_6.csv", "data", b"a,b\n1,2\n"), ("derived/nonresp_1_6.csv", "data", b"a,b\n3,4\n"), *evidence()],
    )
    store_capture(storage, HCAI, "UTIL__v2", "HCAI_UTIL", [("raw/hcai_util_2024.xlsx", "data", b"PK workbook bytes"), *evidence(b"[]")])
    found = selected(storage, HCAI)
    check(found.get("hcai_util_pages_csv") == ["page_1_6.csv"], f"Version 1 pages still load: {found}")
    check(found.get("hcai_util_nonresponse_csv") == ["nonresp_1_6.csv"], f"Version 1 nonresponse pages still load: {found}")
    check(found.get("hcai_util_workbook_sheet_rows") == ["hcai_util_2024.xlsx"], f"The version 2 workbook loads: {found}")
    check(len(found) == 3, f"No other table selects a file: {found}")
