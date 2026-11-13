"""The capture writer's real output through bronze: a routine capture and a reversion load through record selection, and the inventory builder replays them.

Capture mode's own ``World`` fixture runs the actual storage writer against the S3 and publisher stand-ins, so nothing here builds a manifest by hand.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts.acquisition import capture_mode as mode
from scripts.acquisition import collection_layout, s3_store
from scripts.acquisition.tests.test_capture_mode import ROOT, World, flipped_world
from scripts.acquisition.tests.test_task_owners import BUCKET, storage
from scripts.lakehouse import bronze, bronze_metadata, bronze_pin
from tests.support import check


@pytest.fixture(autouse=True)
def writer_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(s3_store, "load_routes", collection_layout.load_routes)
    monkeypatch.setenv("HAI_CAPTURE_ROOT", ROOT)


class StoredBucket:
    """The bronze loader's view of the capture-mode bucket stand-in: current and versioned listings, reads by version and downloads."""

    def __init__(self, world: World) -> None:
        self.world = world

    def list_keys(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        return sorted(
            (item["Key"], item["VersionId"]) for item in self.world.bucket.versions if item["Key"].startswith(prefix) and item["Key"].endswith("/manifest.json")
        )

    def list_current(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        return sorted((item["Key"], item["VersionId"]) for item in self.world.bucket.versions if item["Key"].startswith(prefix))

    def get(self, bucket: str, key: str, version_id: str) -> bytes:
        version, body, _ = self.world.bucket.objects[key]
        check(version == version_id, f"the stand-in holds one version of {key}")
        return body

    def download(self, bucket: str, key: str, version_id: str, path: Path) -> None:
        path.write_bytes(self.get(bucket, key, version_id))


def bronze_table(tmp_path: Path) -> dict:
    """A bronze table map whose one table takes the capture's data file."""
    table = {"table": "hcr", "group": "r", "collection": "cms/hospital_cost_reports", "member_pattern": "a\\.csv$", "format": "csv"}
    path = tmp_path / "bronze_tables.json"
    path.write_text(json.dumps({"version": 1, "namespace": "bronze", "tables": [table]}))
    return bronze.load_table_map(path)


def manifest_roles(world: World) -> list[list[str]]:
    """For each stored manifest, the roles that list the data object's own key and version."""
    found = []
    for key, (_, body, _) in sorted(world.bucket.objects.items()):
        if key.endswith("/manifest.json"):
            entries = json.loads(body)["objects"]
            data = next(entry["object"] for entry in entries if entry["role"] == "data")
            found.append(
                sorted(entry["role"] for entry in entries if (entry["object"]["key"], entry["object"]["version_id"]) == (data["key"], data["version_id"]))
            )
    return found


def test_a_capture_the_writer_stores_with_a_reused_transport_body_loads_through_record_selection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HAI_READ_RAW_ROOT", "true")
    world = World(tmp_path, ["a.csv"])
    world.run()
    check(mode.verify_records(storage(world.bucket), ROOT) == [], "the writer's own evidence step accepts the record")
    check(manifest_roles(world) == [["data", "transport_audit"]], f"the manifest lists the one data object under both roles: {manifest_roles(world)}")
    inputs, _ = bronze.discover(StoredBucket(world), BUCKET, bronze_table(tmp_path), ["hcr"])
    check(len(inputs) == 1 and inputs[0]["sha256"] == hashlib.sha256(world.bodies[world.units[0]["url"]]).hexdigest(), f"the data object loads: {inputs}")
    check(inputs[0]["freshness"] == "current", "it carries the record's label")


def test_the_real_writers_reversion_loads_the_older_payloads_object_and_the_inventory_builder_replays_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HAI_READ_RAW_ROOT", "true")
    world, _, _ = flipped_world(tmp_path)
    world.run(recheck=True)
    check(mode.verify_records(storage(world.bucket), ROOT) == [], "every record verifies")
    stored, table = StoredBucket(world), bronze_table(tmp_path)
    inputs, _ = bronze.discover(stored, BUCKET, table, ["hcr"])
    older = next(body for body in world.records().values() if body["run_id"] == "run01")
    check([item["key"] for item in inputs] == [older["data_objects"][0]["key"]], "bronze loads the objects the reversion record names")
    inventory = bronze_pin.build_inventory(stored, BUCKET, table, ["hcr"])
    replay, _ = bronze.discover(bronze_pin.PinnedStorage(stored, inventory), BUCKET, table, ["hcr"])
    bronze_pin.verify_replay(inventory, replay)
    check([item["key"] for item in replay] == [item["key"] for item in inputs], "the pinned replay names the same objects")


def context(
    previous: list[dict],
) -> tuple[
    dict,
    dict,
    set,
]:
    """The previous generation's canonical keys and observations and its published observations, as the loader passes them to discovery."""
    keys = {(row["table_name"], row["sha256"]): row["_object_key"] for row in previous if row["kind"] == "object"}
    canonical = {(row["table_name"], row["sha256"]): row["observation_id"] for row in previous if row["kind"] == "object"}
    return keys, canonical, {row["observation_id"] for row in previous if row["kind"] == "observation"}


def retired_of(previous: list[dict]) -> dict[str, str | None]:
    return {row["observation_id"]: row["retirement_record_json"] for row in previous if row["kind"] == "observation" and row["retired"]}


def generation(world: World, table: dict, previous: list[dict]) -> tuple[list[dict], list[dict]]:
    """One bronze run's discovery and metadata build with the previous generation's context, as the loader does them. Returns the inputs and the rows."""
    keys, canonical, published = context(previous)
    inputs, unselected = bronze.discover(
        StoredBucket(world),
        BUCKET,
        table,
        ["hcr"],
        None,
        keys,
        canonical,
        published,
        retired_before=retired_of(previous),
        previous_observations=[row for row in previous if row["kind"] == "observation"],
    )
    rows = bronze_metadata.build(["hcr"], inputs, unselected, previous, ["hcr"])
    bronze_metadata.validate_successor(previous, rows)
    return inputs, rows


def test_the_real_writers_reversion_loads_again_as_new_observations_after_bronze_loaded_the_capture_in_between(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HAI_READ_RAW_ROOT", "true")
    world = World(tmp_path, ["a.csv"])
    table, url = bronze_table(tmp_path), world.units[0]["url"]
    world.run()
    _, one = generation(world, table, [])
    served = world.bodies[url]
    world.bodies[url] = b"id,value\n7,14\n"
    world.run(recheck=True)
    _, two = generation(world, table, one)
    world.bodies[url] = served
    world.run(recheck=True)
    check(mode.verify_records(storage(world.bucket), ROOT) == [], "every record verifies")
    inputs, three = generation(world, table, two)
    first = next(body for body in world.records().values() if body["run_id"] == "run01")
    check([item["key"] for item in inputs] == [first["data_objects"][0]["key"]], "bronze loads the objects the reversion record names")
    check(not {item["observation_id"] for item in inputs} & {row["observation_id"] for row in two if row["kind"] == "observation"}, "as new observations")
    check(three[0]["generation"] != two[0]["generation"], "the generation advances to new content")
    check(generation(world, table, three)[1] == three, "a rerun publishes the same generation")
    stored = StoredBucket(world)
    inventory = bronze_pin.build_inventory(stored, BUCKET, table, ["hcr"])
    replay, _ = bronze.discover(
        bronze_pin.PinnedStorage(stored, inventory),
        BUCKET,
        table,
        ["hcr"],
        None,
        *context(two),
        retired_before=retired_of(two),
        previous_observations=[row for row in two if row["kind"] == "observation"],
    )
    bronze_pin.verify_replay(inventory, replay)
    check([item["key"] for item in replay] == [item["key"] for item in inputs], "the pinned replay names the same objects")
