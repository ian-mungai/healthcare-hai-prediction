"""The stage phase of bronze: a job under the bronze task role writes the run's request and source inventory, and the launch preflight and the pin read them."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from botocore.exceptions import ClientError

from orchestration import ecs_preflight
from scripts.lakehouse import bronze, bronze_pin, bronze_run
from tests.support import check

RUN = "bronze_3f9a1c07"
BUCKET = "example-bucket"
REQUEST_KEY = f"lakehouse/run_stage/{RUN}/inputs/bronze_request.json"
INVENTORY_KEY = f"lakehouse/run_stage/{RUN}/inputs/bronze_inventory.json"
REPORT_KEY = f"lakehouse/run_stage/{RUN}/inputs/bronze_stage_report.json"
CONFIG = {"tables": [{"table": "alpha_data", "collection": "first"}, {"table": "beta_data", "collection": "second"}]}
SOURCES = {
    "version": 1,
    "tables": ["alpha_data", "beta_data"],
    "manifests": [],
    "documents": [],
    "objects": [["alpha_data", "raw/a/1.csv", "v1", "a" * 64, 10], ["beta_data", "raw/b/2.csv", "v2", "b" * 64, 20]],
}


class Body:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def read(self, limit: int | None = None) -> bytes:
        return self.data if limit is None else self.data[:limit]


class Store:
    """A bucket that keeps what is put and refuses a second write of a key that carries ``IfNoneMatch``."""

    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = dict(objects or {})
        self.puts: list[dict[str, Any]] = []
        self.reads: list[str] = []

    def put_object(self, **arguments: Any) -> dict[str, Any]:
        if arguments.get("IfNoneMatch") == "*" and arguments["Key"] in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed", "Message": "exists"}}, "PutObject")
        self.objects[arguments["Key"]] = arguments["Body"]
        self.puts.append(arguments)
        return {}

    def get_object(self, **arguments: Any) -> dict[str, Any]:
        self.reads.append(arguments["Key"])
        if arguments["Key"] not in self.objects:
            raise ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "the role cannot list the bucket, so S3 answers 403 for a missing key"}}, "GetObject"
            )
        return {"Body": Body(self.objects[arguments["Key"]])}


class Setup:
    def __init__(self, monkeypatch: pytest.MonkeyPatch, store: Store) -> None:
        self.store, self.builds = store, []
        self.task_storage = SimpleNamespace(client=store)
        monkeypatch.setattr(bronze_run, "storage", lambda: self.task_storage)
        monkeypatch.setattr(bronze_run, "owner_storage", self.refuse_owner)
        monkeypatch.setattr(bronze_run, "table_map", lambda code: CONFIG)
        monkeypatch.setattr(bronze, "load_retired", lambda path: [])
        monkeypatch.setattr(bronze_pin, "build_inventory", self.build)

    @staticmethod
    def refuse_owner() -> Any:
        raise AssertionError("the stage phase read the sources with the owner's profile")

    def build(self, source: Any, bucket: str, config: dict[str, Any], tables: list[str], retired: Any) -> dict[str, Any]:
        self.builds.append((source, bucket, tables))
        return SOURCES


def stage(store: Store) -> dict[str, str]:
    return bronze_run.stage_run(Path("/workspace/code"), BUCKET, RUN)


def test_the_stage_reads_the_sources_with_the_task_identity_for_every_table_and_writes_exactly_three_keys_once(monkeypatch: pytest.MonkeyPatch) -> None:
    store = Store()
    setup = Setup(monkeypatch, store)
    done = stage(store)
    check(len(setup.builds) == 1 and setup.builds[0] == (setup.task_storage, BUCKET, ["alpha_data", "beta_data"]), f"{setup.builds}")
    check(
        [put["Key"] for put in store.puts] == [INVENTORY_KEY, REPORT_KEY, REQUEST_KEY],
        "the inventory, then the report, then the request last, and nothing else",
    )
    report = json.loads(store.objects[REPORT_KEY])
    check(report["skipped_tables"] == [] and report["skipped_count"] == 0 and report["bronze_run"] == RUN, f"{report}")
    check(all(put["IfNoneMatch"] == "*" and put["ServerSideEncryption"] == "AES256" for put in store.puts), f"{store.puts}")
    request = json.loads(store.objects[REQUEST_KEY])
    inventory_digest = hashlib.sha256(store.objects[INVENTORY_KEY]).hexdigest()
    check(request == {"tables": ["alpha_data", "beta_data"], "target": "real", "dry_run": False, "inventory_sha256": inventory_digest}, f"{request}")
    check(done["request_sha256"] == hashlib.sha256(store.objects[REQUEST_KEY]).hexdigest() and done["inventory_sha256"] == inventory_digest, f"{done}")


def test_the_preflight_and_the_pin_accept_the_staged_inputs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    store = Store()
    Setup(monkeypatch, store)
    stage(store)
    values = ecs_preflight.bronze_request_reader(store, BUCKET, bronze_run.collections_of(CONFIG))(RUN)
    check(values["BRONZE_TABLES"] == "alpha_data,beta_data" and values["BRONZE_TARGET"] == "real" and values["BRONZE_DRY_RUN"] == "", f"{values}")
    monkeypatch.setattr(bronze_run, "archive_revision", lambda archive, sha: "c" * 40)
    monkeypatch.setattr(bronze_run, "read_staged", lambda run, bucket, name: store.objects[bronze_run.STAGE_KEY.format(run=run, name=name)])
    job = bronze_run.Job(RUN, tmp_path, BUCKET, ("alpha_data", "beta_data"), "real", False, values["BRONZE_REQUEST_SHA256"], values["BRONZE_INVENTORY_SHA256"])
    bronze_run.pin(job, tmp_path / "code.tar.gz", "d" * 64)
    check((job.folder / bronze_run.RECORD).is_file(), "the pin phase wrote no record from the staged inputs")


MOVED = SOURCES | {"objects": [*SOURCES["objects"], ["alpha_data", "raw/a/3.csv", "v3", "e" * 64, 30]]}


def test_a_retry_after_both_writes_gives_the_same_digests_and_writes_nothing_even_when_the_sources_moved(monkeypatch: pytest.MonkeyPatch) -> None:
    store = Store()
    Setup(monkeypatch, store)
    first = stage(store)
    written = len(store.puts)
    monkeypatch.setattr(bronze_pin, "build_inventory", lambda *arguments: MOVED)
    check(stage(store) == first, "a retry gave other digests")
    check(len(store.puts) == written, "a retry wrote an object over a staged one")


def test_a_retry_after_the_inventory_alone_keeps_that_inventory_and_writes_only_the_request(monkeypatch: pytest.MonkeyPatch) -> None:
    first_inventory = bronze_pin.inventory_bytes(SOURCES)
    store = Store({INVENTORY_KEY: first_inventory})
    Setup(monkeypatch, store)
    monkeypatch.setattr(bronze_pin, "build_inventory", lambda *arguments: MOVED)
    done = stage(store)
    check(done["inventory_sha256"] == hashlib.sha256(first_inventory).hexdigest(), "the retry replaced the staged inventory")
    check([put["Key"] for put in store.puts] == [REPORT_KEY, REQUEST_KEY], f"{store.puts}")
    request = json.loads(store.objects[REQUEST_KEY])
    check(request["inventory_sha256"] == done["inventory_sha256"], "the request names an inventory that is not the staged one")


def test_a_fresh_stage_never_reads_a_key_that_does_not_exist_yet(monkeypatch: pytest.MonkeyPatch) -> None:
    """The bronze role cannot list the stage folder, so S3 answers 403 for a missing key: the stage writes first and reads only a key that is there."""
    store = Store()
    Setup(monkeypatch, store)
    stage(store)
    check(not store.reads, f"a fresh stage read {store.reads}")


def test_a_staged_request_that_differs_is_refused_and_never_replaced(monkeypatch: pytest.MonkeyPatch) -> None:
    other = json.dumps({"tables": ["alpha_data"], "target": "real", "dry_run": False, "inventory_sha256": "f" * 64}, sort_keys=True).encode()
    store = Store({REQUEST_KEY: other})
    Setup(monkeypatch, store)
    with pytest.raises(bronze_run.StepError, match="differs"):
        stage(store)
    check(store.objects[REQUEST_KEY] == other, "the staged request was replaced")


ONE_TABLE = SOURCES | {"objects": SOURCES["objects"][:1], "manifests": [["alpha_data", "m/a", "v1"], ["beta_data", "m/b", "v2"]]}


def test_a_table_with_no_source_object_is_left_out_named_with_its_reason_in_the_report_and_counted(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    store = Store()
    Setup(monkeypatch, store)
    monkeypatch.setattr(bronze_pin, "build_inventory", lambda *arguments: ONE_TABLE)
    done = stage(store)
    request = json.loads(store.objects[REQUEST_KEY])
    staged = json.loads(store.objects[INVENTORY_KEY])
    check(request["tables"] == ["alpha_data"] and staged["tables"] == ["alpha_data"], f"{request} {staged['tables']}")
    report = json.loads(store.objects[REPORT_KEY])
    check(report["skipped_tables"] == [{"table": "beta_data", "reason": "no source object"}] and report["skipped_count"] == 1, f"{report}")
    check(report["requested_tables"] == ["alpha_data"] and done["skipped_count"] == 1, f"{report} {done}")
    check("skipped_tables 1: beta_data" in capsys.readouterr().out, "the output does not count and name the table that was left out")


def test_a_retry_reports_the_skipped_tables_of_the_staged_inventory_not_of_a_later_listing(monkeypatch: pytest.MonkeyPatch) -> None:
    store = Store()
    Setup(monkeypatch, store)
    monkeypatch.setattr(bronze_pin, "build_inventory", lambda *arguments: ONE_TABLE)
    first = stage(store)
    written = len(store.puts)
    report = store.objects[REPORT_KEY]
    monkeypatch.setattr(bronze_pin, "build_inventory", lambda *arguments: SOURCES)
    check(stage(store) == first and len(store.puts) == written and store.objects[REPORT_KEY] == report, "a retry changed what the first stage reported")


def test_a_report_that_differs_from_the_inventory_it_describes_is_refused_and_never_replaced(monkeypatch: pytest.MonkeyPatch) -> None:
    other = json.dumps({"schema": 1, "bronze_run": RUN, "requested_tables": ["alpha_data"], "skipped_tables": [], "skipped_count": 0}).encode()
    store = Store({REPORT_KEY: other})
    Setup(monkeypatch, store)
    with pytest.raises(bronze_run.StepError, match="differs"):
        stage(store)
    check(store.objects[REPORT_KEY] == other and REQUEST_KEY not in store.objects, "the staged report was replaced or a request followed it")


def test_sources_with_no_object_at_all_stage_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    store = Store()
    Setup(monkeypatch, store)
    monkeypatch.setattr(bronze_pin, "build_inventory", lambda *arguments: SOURCES | {"objects": []})
    with pytest.raises(bronze.BronzeError, match="no objects"):
        stage(store)
    check(not store.puts, "an inventory with no object was staged")


def test_the_stage_phase_runs_from_the_command_line_and_reports_both_digests(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    store = Store()
    Setup(monkeypatch, store)
    monkeypatch.setattr(bronze_run.task_capacity, "apply", lambda: None)
    check(bronze_run.main(["stage", "--run", RUN, "--work", str(tmp_path), "--bucket", BUCKET]) == 0, "the stage phase failed")
    check(REQUEST_KEY in store.objects and INVENTORY_KEY in store.objects, "the phase wrote no staged input")
    check("request" in capsys.readouterr().out, "the phase named no digest")
    with pytest.raises(SystemExit):
        bronze_run.main(["stage", "--run", "bronze_BAD", "--work", str(tmp_path), "--bucket", BUCKET])


def test_the_stage_phase_checks_the_task_environment_as_its_own_job(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from scripts.infrastructure import startup_check

    calls: list[tuple[str, str]] = []
    Setup(monkeypatch, Store())
    monkeypatch.setattr(bronze_run.startup_check, "check_phase", lambda job, phase: calls.append((job, phase)))
    monkeypatch.setattr(bronze_run.task_capacity, "apply", lambda: None)
    check(bronze_run.main(["stage", "--run", RUN, "--work", str(tmp_path), "--bucket", BUCKET]) == 0, "the stage phase failed")
    check(calls == [("bronze_stage", "stage")], f"{calls}")
    check("stage" in startup_check.JOB_PHASES["bronze_stage"], "the startup check table has no row for the stage phase")
