"""The 173 Census table units in capture mode: each has its own key and handler, its collector runs through the run and the startup check lists its plan."""

from __future__ import annotations

import json
import time
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from scripts.acquisition import capture_mode as mode
from scripts.acquisition import census_acs_tables_contract as contract
from scripts.acquisition import census_acs_tables_transport, collection_layout, redownload, s3_store, task_role, transport
from scripts.acquisition.run_census_acs_tables_e2e import Requests
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.source_registry import REPO_ROOT, read_json
from scripts.acquisition.tests.test_capture_mode import ROOT, World
from scripts.acquisition.tests.test_task_owners import storage
from scripts.infrastructure import startup_check
from tests.support import check

BASELINE = REPO_ROOT / "config/acquisition/baseline_run"
MODE = "census_acs_tables"


@pytest.fixture(autouse=True)
def isolated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(s3_store, "load_routes", collection_layout.load_routes)
    for name in (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE):
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)
    monkeypatch.setenv("HAI_CAPTURE_ROOT", ROOT)
    monkeypatch.setattr(census_acs_tables_transport, "time", SimpleNamespace(sleep=lambda _seconds: None, monotonic=time.monotonic))


def tracked() -> tuple[list[dict], dict[str, dict]]:
    units = [unit for unit in read_json(BASELINE / "queue.json")["queue"] if unit["mode"] == MODE]
    return units, read_json(BASELINE / "capture_specs.json")["specs"]


def batch_of(table: str, year: int) -> dict:
    return next(item for item in contract.load_plan()["batches"] if item["table"] == table and item["year"] == year)


def queue_unit(batch: dict) -> dict:
    return {
        "bytes": 64 * 1024**2,
        "disposition": "api",
        "flags": ["browser_export_moved_to_census_api"],
        "mode": MODE,
        "pages": 1,
        "route": "publisher_api",
        "snapshot_id": contract.queue_snapshot_id(batch),
        "source_id": "ACS",
        "url": batch["endpoint"],
    }


def table_world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, batches: list[dict]) -> World:
    """A capture world whose queue holds the real tracked units of the given plan batches. Only the Census host, the key and the bucket are stand-ins."""
    versions = tmp_path / "tables_versions.json"
    s3_store.write_once(versions, encoded_json({"versions": [{"code_sha256": contract.collector_code_hashes()}]}))
    monkeypatch.setattr(contract, "VERSIONS_PATH", versions)
    (tmp_path / "world").mkdir()
    world = World(tmp_path / "world", ["x.csv"])
    world.units = [queue_unit(batch) for batch in batches]
    world.bodies.clear()
    specs = {
        unit["snapshot_id"]: {"mode": MODE, "plan_item": batch["id"], "stored_data_files": [batch["file_name"]]}
        for unit, batch in zip(world.units, batches, strict=True)
    }
    world.write_inputs(specs)
    return world


def run_tables(world: World, requests: Requests, **fields: Any) -> dict:
    keys = partial(task_role.TaskKeys, environment={"AWS_REGION": "us-west-2"})
    return world.run(task_keys=keys, api_request=requests, **fields)


def test_the_173_tracked_table_units_have_173_distinct_keys_that_do_not_follow_the_queue_order() -> None:
    units, specs = tracked()
    keys = [mode.unit_key_of(unit, specs[unit["snapshot_id"]]) for unit in units]
    check(len(units) == 173 and len(set(keys)) == 173, f"{len(units)} units give {len(set(keys))} distinct keys")
    reversed_keys = [mode.unit_key_of(unit, specs[unit["snapshot_id"]]) for unit in reversed(units)]
    check(reversed_keys == keys[::-1], "a unit's key does not depend on its place in the queue")


def test_two_tables_of_one_product_and_year_share_a_url_and_differ_in_their_identity() -> None:
    first, second = batch_of("S0101", 2010), batch_of("S0601", 2010)
    check(first["endpoint"] == second["endpoint"], "the two units ask one endpoint")
    keys = [mode.unit_key_of(queue_unit(batch), {"mode": MODE, "plan_item": batch["id"]}) for batch in (first, second)]
    check(keys[0] != keys[1], "the plan item keeps their keys apart")
    with pytest.raises(mode.SpecError):
        mode.unit_key_of(queue_unit(first), {"mode": MODE})


def test_the_mode_is_a_planned_collector_with_an_adapter_in_both_harnesses() -> None:
    check(MODE in redownload.PLANNED and MODE in mode.COLLECTOR_MODES, "capture mode plans the mode from its locked plan")
    check(MODE in mode.ADAPTERS and MODE in redownload.ADAPTERS, "both adapter tables name it")
    check(mode.LINEAGE_ID.get(MODE) == "batch_id", "a record is bound to its planned batch")
    check(census_acs_tables_transport in mode.API_TRANSPORTS, "the run counts the transport's requests")
    check(redownload.lane_of(queue_unit(batch_of("S0101", 2010))) == redownload.TASK_KEYED, "the units run in the keyed lane")


def test_a_record_that_names_another_batch_is_refused() -> None:
    first, second = batch_of("S0101", 2010), batch_of("S0601", 2010)
    receipt = {
        "source": {"source_record_id": "ACS"},
        "acquisition": {},
        "lineage": {"extraction_or_query": json.dumps({"mode": MODE, "batch_id": second["id"]})},
    }
    expected = {"source_id": "ACS", "mode": MODE, "plan_item": first["id"], "url": first["endpoint"], "method": "GET", "mmd": None}
    check("the receipt names another planned item" in mode.receipt_problems(receipt, expected), "the other batch is named")
    check(mode.receipt_problems(receipt, expected | {"plan_item": second["id"]}) == [], "its own batch passes")


def test_the_run_counts_the_requests_of_the_tables_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[Any] = []
    original = census_acs_tables_transport.https_connection

    def record(observer: Any, connection: Any) -> Any:
        seen.append(connection)
        return connection

    monkeypatch.setattr(mode, "observed_connection", record)
    with mode.observing(mode.Observer(1)):
        check(original in seen, "the transport's connection function is wrapped for the run")


def test_two_units_of_one_url_each_get_a_record_and_a_second_run_sends_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    batches = [batch_of("S0101", 2010), batch_of("S0601", 2010)]
    world = table_world(tmp_path, monkeypatch, batches)
    requests = Requests()
    before = mode.list_versions(storage(world.bucket), mode.evidence_prefixes(ROOT))
    first = run_tables(world, requests)
    check(len(first["units"]["completed"]) == 2 and not first["units"]["failed"], f"{first['units']}")
    problems = mode.check_run(storage(world.bucket), first, before)
    check(problems == [], f"the evidence step passes: {problems}")
    check(first["counters"]["http_requests"] == len(requests.calls) == 4, f"a data and a dictionary request per unit: {first['counters']} {requests.calls}")
    found = world.records()
    check(len(found) == 2 and {body["mode"] for body in found.values()} == {MODE}, f"one record per unit: {sorted(found)}")
    check(
        {item["data_objects"][0]["stored_file_name"] for item in found.values()} == {batch["file_name"] for batch in batches}, "each record names its own CSV"
    )
    check(mode.verify_records(storage(world.bucket), ROOT) == [], "the records verify by version")
    listed = mode.list_versions(storage(world.bucket), mode.evidence_prefixes(ROOT))
    second = run_tables(world, requests)
    check(second["counters"]["skipped_complete"] == 2 and second["counters"]["http_requests"] == 0 and len(requests.calls) == 4, str(second["counters"]))
    check(mode.check_run(storage(world.bucket), second, listed) == [], "the second run passes the evidence step")


def test_a_unit_whose_response_fails_its_contract_fails_alone_with_a_fixed_reason(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    batches = [batch_of("S0101", 2010), batch_of("S0601", 2010), batch_of("DP02", 2010)]
    world = table_world(tmp_path, monkeypatch, batches)
    requests = Requests()
    requests.override[("S0601", "data")] = b'[["GEO_ID"]]'
    result = run_tables(world, requests)
    failed = result["units"]["failed"]
    check(len(result["units"]["completed"]) == 2 and len(failed) == 1, f"{result['units']}")
    check(failed[0]["snapshot_id"] == contract.queue_snapshot_id(batches[1]) and failed[0]["reason"] == "registry_contract", str(failed))
    check("ACS" not in result["units"]["paused_sources"], "a failure after the source's first attempt does not pause it")
    check(len(world.records()) == 2, "the failed unit wrote no record")


def tracked_needs() -> list[startup_check.Need]:
    document = read_json(BASELINE / "queue.json")
    return startup_check.queue_files(document, controls=None, keys=True, listing=False)


def test_the_startup_check_lists_the_plan_and_lock_of_the_table_units_for_a_keyed_task() -> None:
    needs = [need for need in tracked_needs() if need.owner == MODE]
    check(
        {(need.kind, Path(need.path).name) for need in needs} >= {("plan", "census_acs_tables_plan.json"), ("lock", "census_acs_tables_plan.lock.json")},
        f"the plan and its lock are named: {needs}",
    )
    document = read_json(BASELINE / "queue.json")
    check(
        not [need for need in startup_check.queue_files(document, controls=None, keys=False, listing=False) if need.owner == MODE],
        "a task without keys does not run the mode",
    )


def test_the_plan_check_names_a_plan_file_that_is_missing_from_the_code_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(contract, "PLAN_PATH", Path("config/acquisition/missing_tables_plan.json"))
    needs = startup_check.contract_needs(MODE)
    check(
        [(need.kind, need.path) for need in needs][:2]
        == [("plan", "config/acquisition/missing_tables_plan.json"), ("lock", "config/acquisition/missing_tables_plan.lock.json")],
        str(needs),
    )


def test_every_attempted_unit_of_the_tracked_queue_has_its_own_key_and_a_handler() -> None:
    units = read_json(BASELINE / "queue.json")["queue"]
    specs = read_json(BASELINE / "capture_specs.json")["specs"]
    attempted = [unit for unit in units if unit["disposition"] != "approved_manual" and redownload.handler_mode(unit) not in mode.MANUAL_MODES]
    keys = [mode.unit_key_of(unit, specs[unit["snapshot_id"]]) for unit in attempted]
    check(len(set(keys)) == len(keys), f"{len(keys) - len(set(keys))} attempted units share a key with another unit")
    lacking = sorted({redownload.handler_mode(unit) for unit in attempted} - set(mode.ADAPTERS) - {"file"})
    check(not lacking, f"modes the capture loop would refuse as having no reviewed handler: {lacking}")
