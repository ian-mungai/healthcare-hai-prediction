"""The Census table captures reach their 15 bronze tables: each planned output has exactly one table and the real capture path feeds discovery."""

from __future__ import annotations

import re
from functools import partial
from pathlib import Path

import pytest

from scripts.acquisition import capture_mode as mode
from scripts.acquisition import census_acs_tables_contract as contract
from scripts.acquisition import collection_layout, s3_store, task_role, transport
from scripts.acquisition.run_census_acs_tables_e2e import Requests
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.source_registry import REPO_ROOT, read_json
from scripts.acquisition.tests.test_capture_mode import World
from scripts.acquisition.tests.test_task_owners import BUCKET, storage
from scripts.lakehouse import bronze
from tests.support import check

BASELINE = REPO_ROOT / "config/acquisition/baseline_run"
EXPORT_NAME = re.compile(r"ACS(?:DP|ST|DT)5Y\d{4}\.([A-Z0-9]+)-Data\.csv")
TEST_ROOT = "ingestion_test/ingestion_abcdef12/"


class ManifestStorage:
    """The bronze reader's view of a bucket stand-in: current keys by prefix and exact versions."""

    def __init__(self, objects: dict) -> None:
        self.objects = objects

    def list_keys(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        return [(key, value[0]) for key, value in self.objects.items() if key.startswith(prefix)]

    def list_current(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        return self.list_keys(bucket, prefix)

    def get(self, bucket: str, key: str, version_id: str) -> bytes:
        version, body, _fields = self.objects[key]
        check(version_id == version, "The bronze read names the stored version")
        return body


def census_tables() -> list[dict]:
    return [table for table in bronze.load_table_map()["tables"] if table["collection"] == "census/acs" and table["roles"] == ["data"]]


def selecting(name: str, dataset_id: str, snapshot_id: str = "") -> list[str]:
    return [table["table"] for table in census_tables() if bronze.select(table, dataset_id, name, snapshot_id)]


def planned_outputs() -> list[tuple[dict, str]]:
    return [(batch, batch["file_name"]) for batch in contract.load_plan()["batches"]]


def test_each_of_the_173_planned_outputs_is_selected_by_exactly_its_own_table() -> None:
    outputs = planned_outputs()
    wrong = {}
    for batch, name in outputs:
        found = selecting(name, "acs", contract.queue_snapshot_id(batch))
        if found != [f"acs_{batch['table'].lower()}"]:
            wrong[name] = found
    check(
        len(outputs) == 173 and not wrong,
        f"{len(wrong)} of {len(outputs)} outputs are not selected by exactly their own table: {dict(list(wrong.items())[:3])}",
    )
    check(len({f"acs_{batch['table'].lower()}" for batch, _ in outputs}) == 15, "all 15 tables are planned")


def test_the_historical_export_names_keep_their_table_under_either_dataset_id() -> None:
    specs = read_json(BASELINE / "capture_specs.json")["specs"]
    queue = read_json(BASELINE / "queue.json")["queue"]
    names = sorted({name for unit in queue if unit["source_id"] == "ACS" for name in specs[unit["snapshot_id"]].get("stored_data_files", [])})
    historical = [name for name in names if EXPORT_NAME.fullmatch(name)]
    check(historical, "the queue's stored ACS data files include export names")
    for name in historical:
        found = EXPORT_NAME.fullmatch(name)
        check(found is not None, name)
        table = f"acs_{found.group(1).lower()}" if found else ""
        for dataset_id in ("acs", table):
            check(selecting(name, dataset_id) == [table], f"{name} under {dataset_id}: {selecting(name, dataset_id)}")


@pytest.mark.parametrize(
    "name",
    [
        "ACSST5Y2010.S0101-Data.csv.bak",
        "xACSST5Y2010.S0101-Data.csv",
        "ACSST5Y2010.S01010-Data.csv",
        "acsst5y2010.s0101-data.csv",
        "ACSST5Y2010.S0101-Notes.csv",
    ],
)
def test_a_file_that_only_looks_like_a_table_export_is_selected_by_no_table(name: str) -> None:
    check(selecting(name, "acs") == [], f"{name}: {selecting(name, 'acs')}")


@pytest.fixture(autouse=True)
def isolated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(s3_store, "load_routes", collection_layout.load_routes)
    for name in (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE):
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)


@pytest.mark.parametrize("root", ["raw/", TEST_ROOT])
def test_the_real_capture_path_feeds_bronze_discovery_under_each_read_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, root: str) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", root)
    monkeypatch.setenv("HAI_READ_RAW_ROOT", "true")
    batches = [next(b for b in contract.load_plan()["batches"] if b["table"] == table and b["year"] == 2010) for table in ("S0101", "DP02", "B19013")]
    versions = tmp_path / "tables_versions.json"
    s3_store.write_once(versions, encoded_json({"versions": [{"code_sha256": contract.collector_code_hashes()}]}))
    monkeypatch.setattr(contract, "VERSIONS_PATH", versions)
    (tmp_path / "world").mkdir()
    world = World(tmp_path / "world", ["x.csv"])
    world.units = [
        {
            "bytes": 64 * 1024**2,
            "disposition": "api",
            "flags": ["browser_export_moved_to_census_api"],
            "mode": "census_acs_tables",
            "pages": 1,
            "route": "publisher_api",
            "snapshot_id": contract.queue_snapshot_id(batch),
            "source_id": "ACS",
            "url": batch["endpoint"],
        }
        for batch in batches
    ]
    world.write_inputs(
        {
            unit["snapshot_id"]: {"mode": "census_acs_tables", "plan_item": batch["id"], "stored_data_files": [batch["file_name"]]}
            for unit, batch in zip(world.units, batches, strict=True)
        }
    )
    keys = partial(task_role.TaskKeys, environment={"AWS_REGION": "us-west-2"})
    result = world.run(root=root, task_keys=keys, api_request=Requests())
    check(len(result["units"]["completed"]) == 3, f"{result['units']}")
    names = [f"acs_{batch['table'].lower()}" for batch in batches]
    inputs, unselected = bronze.discover(
        ManifestStorage(world.bucket.objects), BUCKET, bronze.load_table_map(), names, roots=None if root == "raw/" else (root,)
    )
    check(sorted({item["table"] for item in inputs}) == sorted(names), f"every table gets an input: {sorted({item['table'] for item in inputs})}")
    check(len(inputs) == 3 and not unselected, f"{len(inputs)} inputs, {len(unselected)} unselected data objects")
    check(mode.verify_records(storage(world.bucket), root) == [], "the records verify")
