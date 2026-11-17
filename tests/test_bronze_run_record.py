"""The bronze check phase writes the run record that the lineage and the catalog read: ``lakehouse/bronze_runs/<run>/check.json``, once."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from orchestration import raw_lineage
from orchestration.ecs_lineage import LineageRefused
from scripts.lakehouse import bronze_run
from tests.support import check

RUN = "bronze_3f9a1c07"
BUCKET = "example-bucket"
SHA = "a" * 64
REPORT: dict[str, Any] = {"mode": "load", "passed": True, "tables": ["alpha_data", "beta_data"], "snapshots": {"alpha_data": 41, "beta_data": 42}}
REPORT |= {"metadata_index": {"generation": "7", "path": "x", "sha256": SHA}}
INVENTORY = {
    "version": 1,
    "tables": ["alpha_data", "beta_data"],
    "manifests": [],
    "documents": [],
    "objects": [
        ["alpha_data", "raw/a/1.csv", "v1", SHA, 10],
        ["beta_data", "raw/b/2.csv", "v2", "b" * 64, 20],
        ["gamma_data", "raw/c/3.csv", "v3", "c" * 64, 30],
    ],
}


def job(target: str = "real") -> bronze_run.Job:
    return bronze_run.Job(RUN, Path("/workspace"), BUCKET, ("alpha_data", "beta_data"), target)


def record(**changes: Any) -> dict[str, Any]:
    report = REPORT | changes.pop("report", {})
    body = json.dumps(report, sort_keys=True).encode()
    return bronze_run.run_record(job(), report, body, changes.pop("inventory", INVENTORY))


def test_the_record_has_the_keys_the_lineage_reader_demands_and_names_the_loaded_objects() -> None:
    document = record()
    objects, snapshots = raw_lineage.loaded_objects(document, "check.json", RUN)
    check(snapshots == {"alpha_data": 41, "beta_data": 42}, f"{snapshots}")
    check([item["key"] for item in objects] == ["raw/a/1.csv", "raw/b/2.csv"], f"an object of an unrequested table is named: {objects}")
    check(all(item["bucket"] == BUCKET for item in objects), "an object does not name the bucket")
    body = json.dumps(REPORT, sort_keys=True).encode()
    check(document["report_sha256"] == hashlib.sha256(body).hexdigest() and document["metadata_generation"] == "7", f"{document}")


@pytest.mark.parametrize(
    "changes",
    [
        {"report": {"snapshots": {"alpha_data": 41}}},
        {"report": {"snapshots": {"alpha_data": 41, "beta_data": 0}}},
        {"report": {"snapshots": {"alpha_data": 41, "beta_data": True}}},
        {"report": {"metadata_index": None}},
        {"inventory": {**INVENTORY, "objects": []}},
    ],
)
def test_a_report_without_its_snapshots_or_generation_or_an_inventory_without_objects_gives_no_record(changes: dict[str, Any]) -> None:
    with pytest.raises(bronze_run.StepError):
        record(**changes)


def test_the_record_a_reader_refuses_never_leaves_the_phase() -> None:
    document = record()
    document["objects"][0]["sha256"] = "short"
    with pytest.raises(LineageRefused):
        raw_lineage.loaded_objects(document, "check.json", RUN)


class Client:
    def __init__(self, existing: bytes | None = None) -> None:
        self.existing, self.puts = existing, []

    def put_object(self, **arguments: Any) -> dict[str, Any]:
        from botocore.exceptions import ClientError

        if self.existing is not None:
            raise ClientError({"Error": {"Code": "PreconditionFailed", "Message": "exists"}}, "PutObject")
        self.puts.append(arguments)
        return {}


def test_the_record_is_written_once_and_a_retry_keeps_the_first() -> None:
    client = Client()
    check(bronze_run.store_run_record(client, BUCKET, RUN, record()) == "written", "the first write did not happen")
    put = client.puts[0]
    check(put["Key"] == f"lakehouse/bronze_runs/{RUN}/check.json" and put["IfNoneMatch"] == "*", f"{put}")
    check(json.loads(put["Body"])["run_id"] == RUN, "the stored body is not the record")
    again = Client(existing=b"{}")
    check(bronze_run.store_run_record(again, BUCKET, RUN, record()) == "kept", "a retry replaced the first record")
