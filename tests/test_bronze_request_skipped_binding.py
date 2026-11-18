"""The staged bronze request names the tables the stage left out, so the preflight, the pin, the load and the check phases all work from one list.

Live finding of Nov 18 2026: the stage report that listed the skipped tables sat beside the request with no tie to it, so a request and a report that were both
changed to drop a table with sources agreed with each other. The request now carries ``skipped_tables`` under the digest that the preflight binds, the pin
lists the sources of those tables again with the task identity, and the load and check phases read the list from the digest-checked request.

Failure modes, each with a case below: the request has no skipped list (exact keys); the list is not names, repeats a name, names an unmapped table, a
requested table or any table of a qualification run; the request changes after the preflight; the report changes alone; the report is missing for a request
that skips a table; request and report both drop a table that has source objects (the pin refuses it); the largest request outgrows the limits.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from orchestration import ecs_preflight
from scripts.lakehouse import bronze, bronze_pin, bronze_run
from scripts.lakehouse.bronze_dispatch import DispatchError, check_skipped
from tests.support import check
from tests.test_bronze_stage import BUCKET, FULL_MAP, FULL_NAMES, RUN, Store

CONFIG = {
    "tables": [
        {"table": "alpha_data", "collection": "first"},
        {"table": "beta_data", "collection": "second"},
        {"table": "gamma_data", "collection": "third"},
    ]
}
COLLECTIONS = bronze_run.collections_of(CONFIG)
INVENTORY = {
    "version": 1,
    "tables": ["alpha_data", "beta_data"],
    "manifests": [],
    "documents": [],
    "objects": [["alpha_data", "raw/a/1.csv", "v1", "a" * 64, 10], ["beta_data", "raw/b/2.csv", "v2", "b" * 64, 20]],
}
INVENTORY_BYTES = bronze_pin.inventory_bytes(INVENTORY)
INVENTORY_SHA = hashlib.sha256(INVENTORY_BYTES).hexdigest()
REPORT = {
    "schema": 1,
    "bronze_run": RUN,
    "requested_tables": ["alpha_data", "beta_data"],
    "skipped_tables": [{"table": "gamma_data", "reason": "no source object"}],
    "skipped_count": 1,
}
GOOD = {"tables": ["alpha_data", "beta_data"], "target": "real", "dry_run": False, "inventory_sha256": INVENTORY_SHA, "skipped_tables": ["gamma_data"]}


def encoded(document: dict[str, Any]) -> bytes:
    return json.dumps(document, sort_keys=True).encode()


def staged_keys(request: dict[str, Any] | bytes, report: dict[str, Any] | None = REPORT) -> dict[str, bytes]:
    objects = {
        f"lakehouse/run_stage/{RUN}/inputs/bronze_request.json": request if isinstance(request, bytes) else encoded(request),
        f"lakehouse/run_stage/{RUN}/inputs/bronze_inventory.json": INVENTORY_BYTES,
    }
    if report is not None:
        objects[f"lakehouse/run_stage/{RUN}/inputs/bronze_stage_report.json"] = encoded(report)
    return objects


def preflight(request: dict[str, Any] | bytes, target: str = "real") -> dict[str, str]:
    store = Store(staged_keys(request))
    return ecs_preflight.bronze_request_reader(store, BUCKET, COLLECTIONS)(RUN)


def test_the_preflight_accepts_a_request_that_names_its_skipped_tables_and_binds_it_by_digest() -> None:
    values = preflight(GOOD)
    check(values["BRONZE_REQUEST_SHA256"] == hashlib.sha256(encoded(GOOD)).hexdigest(), f"{values}")
    check(values["BRONZE_TABLES"] == "", "the tables are passed through the container environment")


@pytest.mark.parametrize(
    "changes",
    [
        {"skipped_tables": "gamma_data"},
        {"skipped_tables": [3]},
        {"skipped_tables": ["gamma_data", "gamma_data"]},
        {"skipped_tables": ["ghost_data"]},
        {"skipped_tables": ["alpha_data"]},
        {"skipped_tables": ["Gamma"]},
        {"target": "qualify", "tables": ["alpha_data"]},
    ],
    ids=["a string", "not names", "repeated", "unmapped", "also requested", "not an identifier", "a qualification run that skips"],
)
def test_the_preflight_refuses_a_skipped_list_that_is_not_distinct_mapped_unrequested_names(changes: dict[str, Any]) -> None:
    with pytest.raises(ecs_preflight.TaskRefused):
        preflight({**GOOD, **changes})


def test_the_preflight_refuses_a_request_without_the_skipped_list_and_one_with_another_key() -> None:
    without = {key: value for key, value in GOOD.items() if key != "skipped_tables"}
    with pytest.raises(ecs_preflight.TaskRefused, match="exactly"):
        preflight(without)
    with pytest.raises(ecs_preflight.TaskRefused, match="exactly"):
        preflight({**GOOD, "skipped": []})


def test_the_check_applies_the_same_rules_to_the_stage_the_pin_and_the_preflight() -> None:
    check(check_skipped(["gamma_data"], ["alpha_data", "beta_data"], "real", COLLECTIONS) == ("gamma_data",), "a valid list was refused")
    check(check_skipped([], ["alpha_data"], "qualify", COLLECTIONS) == (), "an empty list was refused")
    for skipped, tables, target in (
        (["gamma_data", "gamma_data"], ["alpha_data"], "real"),
        (["ghost"], ["alpha_data"], "real"),
        (["alpha_data"], ["alpha_data"], "real"),
        (["gamma_data"], ["alpha_data"], "qualify"),
    ):
        with pytest.raises(DispatchError):
            check_skipped(skipped, tables, target, COLLECTIONS)


def test_the_largest_valid_request_stays_inside_both_limits_with_room_to_grow() -> None:
    """The map names 331 tables, so a request holds them split between ``tables`` and ``skipped_tables`` and never more names than the map has."""
    names = sorted(FULL_NAMES, key=lambda name: (-len(name), name))
    shapes = {"all requested": (names, []), "one skipped": (names[1:], names[:1]), "one requested": (names[:1], names[1:])}
    collections = bronze_run.collections_of(FULL_MAP)
    for label, (tables, skipped) in shapes.items():
        request = {"tables": tables, "target": "real", "dry_run": False, "inventory_sha256": "f" * 64, "skipped_tables": skipped}
        body = encoded(request)
        check(len(body) * 2 <= ecs_preflight.BRONZE_REQUEST_BYTES, f"{label}: {len(body)} bytes against a limit of {ecs_preflight.BRONZE_REQUEST_BYTES}")
        key = f"lakehouse/run_stage/{RUN}/inputs/bronze_request.json"
        values = ecs_preflight.bronze_request_reader(Store({key: body}), BUCKET, collections)(RUN)
        size = sum(len(name) + len(value) for name, value in values.items())
        check(size < 1024, f"{label}: the container values hold {size} characters, with a service limit of 8192 for all containers: {values}")


def pin_job(tmp_path: Path, request: bytes) -> bronze_run.Job:
    return bronze_run.Job(RUN, tmp_path, BUCKET, ("alpha_data", "beta_data"), "real", False, hashlib.sha256(request).hexdigest(), INVENTORY_SHA)


class Pinning:
    """The pin phase over staged inputs, with the live listing of the skipped tables replaced by ``found``."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, objects: dict[str, bytes], found: list[dict[str, Any]], unselected: list[Any] | None = None) -> None:
        self.asked: list[list[str]] = []
        keys = {key.rsplit("/", 1)[-1]: body for key, body in objects.items()}

        def read(run: str, bucket: str, name: str) -> bytes:
            if name not in keys:
                raise ClientError({"Error": {"Code": "AccessDenied", "Message": "no such staged input"}}, "GetObject")
            return keys[name]

        monkeypatch.setattr(bronze_run, "read_staged", read)
        monkeypatch.setattr(bronze_run, "archive_revision", lambda archive, sha: "c" * 40)
        monkeypatch.setattr(bronze_run, "table_map", lambda code: CONFIG)
        monkeypatch.setattr(bronze_run, "storage", lambda: object())
        monkeypatch.setattr(bronze, "load_retired", lambda path: None)

        def discover(
            storage: Any, bucket: str, config: dict[str, Any], names: list[str], *rest: Any, **keywords: Any
        ) -> tuple[list[dict[str, Any]], list[Any]]:
            self.asked.append(list(names))
            return found, list(unselected or [])

        monkeypatch.setattr(bronze, "discover", discover)


def test_the_pin_lists_the_sources_of_the_skipped_tables_again_and_records_them(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    request = encoded(GOOD)
    pinning = Pinning(monkeypatch, staged_keys(request), [])
    job = pin_job(tmp_path, request)
    bronze_run.pin(job, tmp_path / "code.tar.gz", "d" * 64)
    check(pinning.asked == [["gamma_data"]], f"the pin did not list the sources of the skipped table only: {pinning.asked}")
    check(json.loads((job.folder / bronze_run.RECORD).read_text())["skipped_tables"] == ["gamma_data"], "the pin record does not name the skipped table")


def test_the_pin_refuses_a_request_and_report_that_both_drop_a_table_that_has_source_objects(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    request = encoded(GOOD)
    found = [{"table": "gamma_data", "key": "raw/c/3.csv"}]
    Pinning(monkeypatch, staged_keys(request), found)
    with pytest.raises(bronze.BronzeError, match="gamma_data.*source object"):
        bronze_run.pin(pin_job(tmp_path, request), tmp_path / "code.tar.gz", "d" * 64)
    check(not (pin_job(tmp_path, request).folder / bronze_run.RECORD).exists(), "a refused pin wrote its record")


def test_the_pin_refuses_a_request_without_the_skipped_list_or_with_a_list_the_rules_refuse(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for document in ({key: value for key, value in GOOD.items() if key != "skipped_tables"}, {**GOOD, "skipped_tables": ["alpha_data"]}):
        request = encoded(document)
        Pinning(monkeypatch, staged_keys(request), [])
        with pytest.raises((bronze_run.StepError, DispatchError)):
            bronze_run.pin(pin_job(tmp_path, request), tmp_path / "code.tar.gz", "d" * 64)


def test_the_pin_refuses_a_request_that_skips_a_table_when_the_stage_report_is_missing_or_names_other_tables(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    request = encoded(GOOD)
    for report in (None, {**REPORT, "skipped_tables": [], "skipped_count": 0}):
        Pinning(monkeypatch, staged_keys(request, report), [])
        with pytest.raises(bronze_run.StepError):
            bronze_run.pin(pin_job(tmp_path, request), tmp_path / "code.tar.gz", "d" * 64)


def owner_stage(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, tables: str, target: str, found: list[str], history: list[str] | None = None
) -> dict[str, Any]:
    """The owner-side ``inventory`` phase over a map of three tables, with discovery of the unrequested tables replaced: ``found`` have a source object."""
    monkeypatch.setattr(bronze_run, "table_map", lambda code: CONFIG)
    monkeypatch.setattr(bronze_run, "owner_storage", lambda: object())
    monkeypatch.setattr(bronze, "load_retired", lambda path: None)
    monkeypatch.setattr(bronze_pin, "build_inventory", lambda *arguments: {**INVENTORY, "tables": sorted(tables.split(","))})
    monkeypatch.setattr(bronze_pin, "require_objects", lambda inventory: None)
    asked: list[list[str]] = []

    def discover(storage: Any, bucket: str, config: dict[str, Any], names: list[str], *rest: Any, **keywords: Any) -> tuple[list[dict[str, Any]], list[Any]]:
        asked.append(list(names))
        return [{"table": name} for name in names if name in found], [{"copy": {"table": name}} for name in names if name in (history or [])]

    monkeypatch.setattr(bronze, "discover", discover)
    bronze_run.stage_inputs(tmp_path, BUCKET, tables, target, False, tmp_path / "out", RUN)
    return {"asked": asked}


def test_the_owner_staging_of_a_full_request_skips_nothing_and_reads_no_other_table(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    seen = owner_stage(monkeypatch, tmp_path, "alpha_data,beta_data,gamma_data", "real", [])
    request = json.loads((tmp_path / "out" / bronze_run.REQUEST_FILE).read_text())
    check(request["skipped_tables"] == [] and set(request) == bronze_run.REQUEST_KEYS and seen["asked"] == [], f"{request} {seen}")
    check(not (tmp_path / "out" / bronze_run.STAGE_REPORT_FILE).exists(), "a request that skips nothing wrote a stage report")


def test_the_owner_staging_of_a_partial_run_names_the_unrequested_tables_with_no_source_object_and_writes_the_stage_report(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Review round 2 (Medium): the owner command could not carry an accepted absence, so a partial reload after a skipped generation was refused."""
    seen = owner_stage(monkeypatch, tmp_path, "alpha_data", "real", found=["beta_data"])
    out = tmp_path / "out"
    request = json.loads((out / bronze_run.REQUEST_FILE).read_text())
    check(seen["asked"] == [["beta_data", "gamma_data"]], f"the unrequested tables were not read: {seen}")
    check(request["skipped_tables"] == ["gamma_data"], f"an unrequested table with sources or the absent one is wrong: {request}")
    report = json.loads((out / bronze_run.STAGE_REPORT_FILE).read_text())
    check(report["bronze_run"] == RUN and report["requested_tables"] == ["alpha_data"] and report["skipped_count"] == 1, f"{report}")


def test_the_owner_staging_never_skips_a_table_that_has_stored_objects_with_only_rejected_or_retired_observations_or_a_qualification_table(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    owner_stage(monkeypatch, tmp_path, "alpha_data", "real", found=[], history=["beta_data"])
    request = json.loads((tmp_path / "out" / bronze_run.REQUEST_FILE).read_text())
    check(request["skipped_tables"] == ["gamma_data"], f"a table with observation history was skipped: {request}")
    other = tmp_path / "other"
    other.mkdir()
    seen = owner_stage(monkeypatch, other, "alpha_data", "qualify", found=[])
    check(
        json.loads((other / "out" / bronze_run.REQUEST_FILE).read_text())["skipped_tables"] == [] and seen["asked"] == [],
        "a qualification run read or skipped tables",
    )


def test_the_pin_says_a_source_object_appeared_after_staging_and_refuses_a_table_with_only_rejected_or_retired_observations(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    request = encoded(GOOD)
    Pinning(monkeypatch, staged_keys(request), [{"table": "gamma_data", "key": "raw/c/3.csv"}])
    with pytest.raises(bronze.BronzeError, match="appeared after the stage listed|stage a new run"):
        bronze_run.pin(pin_job(tmp_path, request), tmp_path / "code.tar.gz", "d" * 64)
    Pinning(monkeypatch, staged_keys(request), [], [{"copy": {"table": "gamma_data"}}])
    with pytest.raises(bronze.BronzeError, match="rejected, retired or superseded observations"):
        bronze_run.pin(pin_job(tmp_path, request), tmp_path / "code.tar.gz", "d" * 64)
