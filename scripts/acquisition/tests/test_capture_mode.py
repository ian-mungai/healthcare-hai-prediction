"""Capture mode (design 8.3): a completion record per unit decides skip, and a second run sends nothing and writes only its run record (D3, D10)."""

from __future__ import annotations

import copy
import datetime
import hashlib
import io
import json
import zipfile
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import capture_mode as mode
from scripts.acquisition import cms_owners_contract as owners_contract
from scripts.acquisition import (
    collect_bls_api,
    collect_census_acs_api,
    collect_census_acs_detailed,
    collect_hud_api,
    collect_mmd_api,
    collection_layout,
    s3_store,
    store_cms_owners,
    store_hcai_util,
    store_onc_mu,
    transport,
)
from scripts.acquisition import completion_record as record
from scripts.acquisition import ingestion_intent as intent
from scripts.acquisition import onc_mu_contract as onc_contract
from scripts.acquisition import redownload_route as route
from scripts.acquisition.run_cms_owners_e2e import synthetic_plan as owners_plan
from scripts.acquisition.run_onc_mu_e2e import body as onc_body
from scripts.acquisition.run_onc_mu_e2e import locked as onc_locked
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.source_registry import REPO_ROOT, read_json
from scripts.acquisition.tests.test_task_owners import ACCOUNT, BUCKET, TaskS3, Web, opener, storage
from tests.support import check

ROOT = "raw/"
INTENTS = f"{ROOT}ingestion_intents/"
DATASETS = f"{ROOT}cms/hospital_cost_reports/datasets/"
SOURCE = "CMS_HCRIS_PUF"
CODE, BUNDLE = "c" * 64, "b" * 64
BASE_TIME = datetime.datetime(2026, 11, 12, 20, 0, tzinfo=datetime.UTC)


class Crash(BaseException):
    """A process death: nothing below it may catch it as an ordinary failure."""


class Bucket(TaskS3):
    """The versioned bucket stand-in, extended with create-once refusal, version reads, listings and one planted crash."""

    def __init__(self) -> None:
        super().__init__()
        self.versions: list[dict] = []
        self.delete_markers: list[dict] = []
        self.crash_on_prefix: str | None = None
        self.before_put: dict[str, Callable[[], None]] = {}
        self.unreadable_prefix: str | None = None
        self.listed: list[str] = []
        self.clock = 0

    def put_object(self, **arguments: Any) -> dict:
        if self.crash_on_prefix and arguments["Key"].startswith(self.crash_on_prefix):
            self.crash_on_prefix = None
            raise Crash
        for prefix in [name for name in self.before_put if arguments["Key"].startswith(name)]:
            self.before_put.pop(prefix)()
        if arguments["Key"] in self.objects:
            raise self.missing("PutObject", "PreconditionFailed")
        answer = super().put_object(**arguments)
        self.clock += 1
        self.versions.append({"Key": arguments["Key"], "VersionId": answer["VersionId"], "LastModified": BASE_TIME + datetime.timedelta(seconds=self.clock)})
        return answer

    def head_object(self, **arguments: Any) -> dict:
        answer = super().head_object(**arguments)
        if "VersionId" in arguments and arguments["VersionId"] != answer["VersionId"]:
            raise self.missing("HeadObject", "404")
        return answer

    def get_object(self, **arguments: Any) -> dict:
        if self.unreadable_prefix and arguments["Key"].startswith(self.unreadable_prefix):
            raise self.missing("GetObject", "AccessDenied")
        return super().get_object(**arguments)

    def list_objects_v2(self, **arguments: Any) -> dict:
        self.listed.append(arguments["Prefix"])
        found = [item for item in self.versions if item["Key"].startswith(arguments["Prefix"])]
        return {"Contents": [{"Key": item["Key"], "LastModified": item["LastModified"]} for item in found], "IsTruncated": False}

    def list_object_versions(self, **arguments: Any) -> dict:
        found = [item for item in self.versions if item["Key"].startswith(arguments["Prefix"])]
        return {
            "Versions": [{"Key": item["Key"], "VersionId": item["VersionId"], "ETag": "x", "Size": 1} for item in found],
            "DeleteMarkers": [item for item in self.delete_markers if item["Key"].startswith(arguments["Prefix"])],
            "IsTruncated": False,
        }

    def names(self, prefix: str = "") -> set[str]:
        return {item["Key"] for item in self.versions if item["Key"].startswith(prefix)}


class Site(Web):
    """The publisher stand-in. Each request also records what the scratch folder holds at that moment."""

    def __init__(self, bodies: dict[str, bytes]) -> None:
        super().__init__(bodies)
        self.scratch: Path | None = None
        self.scratch_seen: list[list[str]] = []
        self.asked: list[str] = []

    def open(self, request: Any, timeout: float) -> Any:
        self.asked.append(request.full_url)
        if self.scratch is not None and self.scratch.is_dir():
            self.scratch_seen.append(sorted(path.name for path in self.scratch.iterdir()))
        return super().open(request, timeout)


@pytest.fixture(autouse=True)
def isolated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(s3_store, "load_routes", collection_layout.load_routes)
    for name in (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE):
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)
    monkeypatch.setenv("HAI_CAPTURE_ROOT", ROOT)


def template() -> dict:
    """The receipt template an inputs spec carries: a real reviewed job plan without the fields the capture sets itself."""
    batch = read_json(REPO_ROOT / read_json(REPO_ROOT / "config/acquisition/e2e_scenario.json")["batch_plan"])
    plan = dict(next(job for job in batch["jobs"] if job["job_id"] == "CMS_HCRIS_PUF_file_001")["plan"])
    for name in ("route_id", "file_name", "expected_format", "expected_sha256"):
        plan.pop(name, None)
    return plan


def csv_body(number: int) -> bytes:
    return f"id,value\n{number},{number * 2}\n".encode()


def unit_of(name: str, source: str = SOURCE) -> dict:
    url = f"https://data.cms.gov/sites/default/files/synthetic/{name}"
    tail = hashlib.sha256(name.encode()).hexdigest()[:32]
    return {
        "snapshot_id": f"{source}__20260101T000000Z__{tail}",
        "source_id": source,
        "url": url,
        "mode": "file",
        "bytes": 100,
        "pages": 1,
        "disposition": "direct_download",
        "route": "publisher_url",
        "flags": [],
        "receipt": "unused",
        "receipt_sha256": "0" * 64,
    }


def spec_of(name: str, members: list[dict] | None = None) -> dict:
    fmt = "zip" if name.endswith(".zip") else "csv"
    spec = {"mode": "file", "expected_format": fmt, "stored_data_files": [name], "receipt_template": template()}
    return spec | ({"archive_members": members} if members else {})


class World:
    """A frozen queue, its capture specification, a publisher, a bucket and the run settings, all under one temporary folder."""

    def __init__(self, folder: Path, names: list[str], members: dict[str, list[dict]] | None = None) -> None:
        self.folder, self.bucket = folder, Bucket()
        self.units = [unit_of(name) for name in names]
        self.bodies = {unit["url"]: csv_body(index) for index, unit in enumerate(self.units, 1)}
        self.site = Site(self.bodies)
        specs = {unit["snapshot_id"]: spec_of(name, (members or {}).get(name)) for unit, name in zip(self.units, names, strict=True)}
        self.write_inputs(specs)
        self.runs = 0

    def write_inputs(self, specs: dict) -> None:
        queue = self.folder / "queue.json"
        queue.write_bytes(json.dumps({"queue": self.units}).encode())
        digest = hashlib.sha256(queue.read_bytes()).hexdigest()
        queue.with_name("queue.lock.json").write_text(json.dumps({"entries": len(self.units), "queue_sha256": digest}))
        document = {"kind": "capture_specs", "queue_sha256": digest, "units": len(specs), "specs": specs}
        (self.folder / "capture_specs.json").write_bytes(json.dumps(document).encode())
        hosts = self.folder / "hosts.json"
        hosts.unlink(missing_ok=True)
        approval = route.derive_hosts(queue, hosts)["hosts_sha256"]
        (self.folder / "hosts_approval.json").write_text(json.dumps({"hosts_sha256": approval, "approved_by": "synthetic_owner"}))

    def run(self, **fields: Any) -> dict:
        self.runs += 1
        scratch = self.folder / "scratch"
        self.site.scratch = scratch
        settings: dict[str, Any] = {
            "queue_path": self.folder / "queue.json",
            "specs_path": self.folder / "capture_specs.json",
            "root": ROOT,
            "run_id": f"run{self.runs:02}",
            "storage": storage(self.bucket),
            "opener": opener(self.site),
            "scratch": scratch,
            "code_sha256": CODE,
            "bundle_sha256": BUNDLE,
            "hosts": self.folder / "hosts.json",
            "hosts_approval": self.folder / "hosts_approval.json",
        }
        return mode.execute(mode.CaptureRun(**(settings | fields)))

    def records(self) -> dict[str, dict]:
        return {key: json.loads(self.bucket.objects[key][1]) for key in sorted(self.bucket.names(f"{ROOT}ingestion_units/"))}

    def intents(self) -> dict[str, dict]:
        return {key: json.loads(self.bucket.objects[key][1]) for key in sorted(self.bucket.names(INTENTS))}

    def unit_key(self, index: int = 0) -> str:
        unit = self.units[index]
        return mode.unit_key_of(unit, read_json(self.folder / "capture_specs.json")["specs"][unit["snapshot_id"]])


def plant(bucket: Bucket, key: str, body: bytes) -> None:
    bucket.put_object(
        Bucket=BUCKET, ExpectedBucketOwner=ACCOUNT, Key=key, Body=io.BytesIO(body), IfNoneMatch="*", ChecksumSHA256="x", ServerSideEncryption="AES256"
    )


def unplant(bucket: Bucket, key: str) -> None:
    bucket.objects.pop(key)
    bucket.versions[:] = [item for item in bucket.versions if item["Key"] != key]


def kind_of(key: str) -> str:
    """What a stored object is: an intent, a run object, a completion record, audit evidence, a receipt, a manifest or a data object."""
    for prefix, kind in ((INTENTS, "intent"), (f"{ROOT}ingestion_runs/", "run"), (f"{ROOT}ingestion_units/", "record")):
        if key.startswith(prefix):
            return kind
    if "/audit/" in key:
        return "audit"
    return next(
        (kind for tail, kind in (("/receipt.json", "receipt"), ("/manifest.json", "manifest")) if key.endswith(tail)),
        "data" if "/datasets/" in key else "other",
    )


def new_kinds(world: World, before: set[str]) -> Counter:
    return Counter(kind_of(key) for key in world.bucket.names() - before)


@pytest.fixture
def three(tmp_path: Path) -> World:
    return World(tmp_path, ["a.csv", "b.csv", "c.csv"])


def test_run_one_creates_one_completion_record_per_unit(three: World) -> None:
    before = mode.list_versions(storage(three.bucket), mode.evidence_prefixes(ROOT))
    result = three.run()
    check(mode.check_run(storage(three.bucket), result, before) == [], f"the evidence step passes: {mode.check_run(storage(three.bucket), result, before)}")
    found = three.records()
    check(len(found) == 3, f"one record per unit: {sorted(found)}")
    counters = result["counters"]
    check(counters["units_attempted"] == 3 and counters["skipped_complete"] == 0, str(counters))
    check(counters["http_requests"] == three.site.requests == 3, f"one request per unit: {counters} {three.site.requests}")
    for key, body in found.items():
        parsed = record.parse(encoded_json(body), key=key, root=ROOT)
        check(parsed["run_id"] == "run01" and parsed["code_sha256"] == CODE and parsed["bundle_sha256"] == BUNDLE, "the record binds run, code and bundle")
        check(parsed["collection"] == "cms/hospital_cost_reports", parsed["collection"])
        check(all(item["key"] in three.bucket.objects for item in record.objects_named(parsed)), "every named object exists")
    first = f"{ROOT}ingestion_runs/run01/"
    check(three.bucket.names(first) == {f"{first}run.json", f"{first}state/000001.json"}, "one run record and one state object")
    intents = three.intents()
    check(counters["intents_created"] == 3 and len(intents) == 3, f"one intent per captured unit: {sorted(intents)}")
    by_unit = {body["unit_key"]: body for body in found.values()}
    for key, body in intents.items():
        parsed_intent = intent.parse(three.bucket.objects[key][1], key=key, root=ROOT)
        check(parsed_intent["generation"] == 1 and parsed_intent["run_id"] == "run01", f"the first attempt of a unit is generation 1: {parsed_intent}")
        check(body["snapshot_id"] in by_unit[body["unit_key"]]["receipt"]["key"], "the unit's record names objects stored under its intent's snapshot ID")
    check(mode.verify_records(storage(three.bucket), ROOT) == [], "the evidence check passes")


def test_the_unit_key_ignores_the_snapshot_and_follows_the_request() -> None:
    first, second = unit_of("a.csv"), unit_of("a.csv")
    second["snapshot_id"] = "OTHER__20270101T000000Z__" + "e" * 32
    spec = spec_of("a.csv")
    check(mode.unit_key_of(first, spec) == mode.unit_key_of(second, spec), "a new snapshot ID names the same unit")
    changed = unit_of("b.csv")
    check(mode.unit_key_of(first, spec) != mode.unit_key_of(changed, spec), "another request is another unit")
    members = [{"member_chain": ["x.csv"], "role": "data", "dataset_id": "d"}]
    check(mode.unit_key_of(first, spec) != mode.unit_key_of(first, spec | {"archive_members": members}), "another member map is another unit")


def test_run_two_sends_nothing_and_writes_only_its_run_record(three: World) -> None:
    three.run()
    before = {item["Key"]: item["VersionId"] for item in three.bucket.versions}
    listed = mode.list_versions(storage(three.bucket), mode.evidence_prefixes(ROOT))
    asked = three.site.requests
    result = three.run()
    check(mode.check_run(storage(three.bucket), result, listed) == [], "the evidence step passes for run two")
    counters = result["counters"]
    zero = ("http_requests", "bytes_downloaded", "units_attempted", "objects_created", "intents_created", "bls_ledger_entries_added", "skipped_recent_stale")
    check(all(counters[name] == 0 for name in zero) and counters["skipped_complete"] == 3, str(counters))
    check(three.site.requests == asked, "the second run made no request")
    added = {item["Key"] for item in three.bucket.versions} - set(before)
    check(added == {f"{ROOT}ingestion_runs/run02/run.json"}, f"only the run record is new: {sorted(added)}")
    check({item["Key"]: item["VersionId"] for item in three.bucket.versions if item["Key"] in before} == before, "no earlier version changed")


def test_an_existing_record_means_the_unit_gets_no_request(three: World) -> None:
    three.run()
    four = unit_of("d.csv")
    three.units.append(four)
    three.bodies[four["url"]] = csv_body(9)
    specs = read_json(three.folder / "capture_specs.json")["specs"] | {four["snapshot_id"]: spec_of("d.csv")}
    three.write_inputs(specs)
    three.site.asked.clear()
    result = three.run()
    check(three.site.asked == [four["url"]], f"only the new unit was requested: {three.site.asked}")
    check(result["counters"]["skipped_complete"] == 3 and result["counters"]["units_attempted"] == 1, str(result["counters"]))
    check(len(three.records()) == 4, "the new unit got its record")


@pytest.mark.parametrize("change", ["digest", "version"])
def test_a_forged_record_is_refused_not_skipped(tmp_path: Path, change: str) -> None:
    world = World(tmp_path, ["a.csv"])
    world.run()
    key, body = next(iter(world.records().items()))
    if change == "digest":
        body["data_objects"][0]["sha256"] = "f" * 64
    else:
        body["data_objects"][0]["version_id"] = "version_that_never_existed"
    version, _, fields = world.bucket.objects[key]
    world.bucket.objects[key] = (version, encoded_json(body), fields)
    asked, before = world.site.requests, set(world.bucket.names())
    result = world.run()
    check(result["counters"]["skipped_complete"] == 0 and result["counters"]["units_attempted"] == 0, f"a forged record is not a skip: {result['counters']}")
    check(world.site.requests == asked, "a refused unit gets no request")
    check([item["unit_key"] for item in result["units"]["refused"]] == [body["unit_key"]], str(result["units"]))
    check(world.bucket.names() - before == {f"{ROOT}ingestion_runs/run02/run.json"}, "only the run record was added")


def crashed_world(tmp_path: Path, dated: bool) -> tuple[World, set[str]]:
    """A one-unit world whose first run dies after its data objects and before its record. Returns it with the data objects the crash left."""
    world = World(tmp_path, ["a.csv"])
    if dated:
        specs = read_json(world.folder / "capture_specs.json")["specs"]
        for spec in specs.values():
            spec["receipt_template"]["release"]["release_date"] = "2026-01-01"
        world.write_inputs(specs)
    world.bucket.crash_on_prefix = f"{ROOT}ingestion_units/"
    with pytest.raises(Crash):
        world.run()
    check(world.bucket.names(f"{ROOT}ingestion_units/") == set(), "no record was written")
    data = world.bucket.names(f"{ROOT}cms/hospital_cost_reports/datasets/")
    check(bool(data), "the data object was stored before the crash")
    check(len([key for key in world.bucket.names() if key.endswith("/receipt.json")]) == 1, "the crashed attempt left its receipt")
    scratch = world.folder / "scratch"
    check(not scratch.exists() or list(scratch.iterdir()) == [], "the crashed attempt's files are removed")
    return world, data


@pytest.mark.parametrize("dated", [True, False], ids=["dated", "undated"])
def test_a_crash_after_the_data_objects_and_before_the_record_adds_no_data_version_and_orphans_only_receipt_manifest_and_audit(
    tmp_path: Path, dated: bool
) -> None:
    world, data = crashed_world(tmp_path, dated=dated)
    before = set(world.bucket.names())
    result = world.run()
    check(world.bucket.names(DATASETS) == data, "the retry created no data object")
    check(all(len([item for item in world.bucket.versions if item["Key"] == key]) == 1 for key in data), "each data object keeps its one version")
    expected = Counter({"audit": 1, "manifest": 1, "receipt": 1, "record": 1, "run": 2})
    check(
        new_kinds(world, before) == expected, f"the retry adds its audit evidence, receipt, manifest, record and run objects only: {new_kinds(world, before)}"
    )
    check(result["counters"]["units_attempted"] == 1 and result["counters"].get("intents_created") == 0, str(result["counters"]))
    (crashed,) = world.intents().values()
    check(crashed["run_id"] == "run01", "the one intent is the crashed attempt's, reused by the retry")
    (only,) = world.records().values()
    check(crashed["snapshot_id"] in only["receipt"]["key"], "the record names the receipt stored under the reused snapshot ID")
    check(
        all(crashed["snapshot_id"] in item["key"] for item in only["data_objects"] if "capture_id=" in item["key"]),
        "undated data stays under the reused capture ID",
    )


def test_a_crash_retry_of_an_archive_unit_stores_no_new_member_version(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.zip"], {"a.zip": MEMBERS})
    world.bodies[world.units[0]["url"]] = archive(pdf(1), b"id,value\n1,2\n")
    world.bucket.crash_on_prefix = f"{ROOT}ingestion_units/"
    with pytest.raises(Crash):
        world.run()
    members = {key for key in world.bucket.names(f"{ROOT}cms/") if "/datasets/" in key or "/references/" in key}
    check(len(members) >= 2, f"the crash left the data member and the reference: {sorted(members)}")
    before = set(world.bucket.names())
    result = world.run()
    check(
        {key for key in world.bucket.names(f"{ROOT}cms/") if "/datasets/" in key or "/references/" in key} == members, "the retry stored no new member object"
    )
    check(result["counters"]["units_attempted"] == 1 and result["counters"]["intents_created"] == 0 and len(world.records()) == 1, str(result["counters"]))
    check(new_kinds(world, before)["data"] == 0, f"{new_kinds(world, before)}")


def test_a_crash_retry_of_a_collector_unit_with_an_api_page_stores_no_new_data_or_page_version(tmp_path: Path) -> None:
    world, page = api_world(tmp_path)
    adapters = {"hud_api": api_adapter(page)}
    world.bucket.crash_on_prefix = f"{ROOT}ingestion_units/"
    with pytest.raises(Crash):
        world.run(adapters=adapters)
    stored = world.bucket.names(DATASETS)
    check(len(stored) == 2, f"the crash left the data file and the API page: {sorted(stored)}")
    result = world.run(adapters=adapters)
    check(world.bucket.names(DATASETS) == stored, "the retry stored no new data or page object")
    check(result["counters"]["intents_created"] == 0 and len(world.records()) == 1, str(result["counters"]))


def test_the_intent_is_written_before_any_data_object(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv"])
    world.bucket.crash_on_prefix = INTENTS
    with pytest.raises(Crash):
        world.run()
    check(world.bucket.names() == set(), f"a crash on the intent write left nothing stored: {sorted(world.bucket.names())}")
    world.run()
    check(len(world.intents()) == 1 and len(world.records()) == 1, "the retry wrote its intent and then its record")


def test_two_racing_runs_of_one_unit_resolve_to_one_intent(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv"])
    key, target = world.unit_key(), intent.intent_key(ROOT, world.unit_key(), 1)
    other = "CMS_HCRIS_PUF__20260101T000000Z__" + "a" * 32
    rival = intent.encode(intent.build(unit_key=key, generation=1, snapshot_id=other, run_id="rival_run"))
    world.bucket.before_put[INTENTS] = lambda: plant(world.bucket, target, rival)
    result = world.run()
    check(world.bucket.names(INTENTS) == {target}, f"one intent: {sorted(world.bucket.names(INTENTS))}")
    check(result["counters"]["intents_created"] == 0 and result["counters"]["units_attempted"] == 1, str(result["counters"]))
    (only,) = world.records().values()
    check(all(f"capture_id={other}" in item["key"] for item in only["data_objects"]), "the loser stored its data under the winner's snapshot ID")
    check(other in only["receipt"]["key"], "and its receipt too")


def test_a_monthly_run_after_a_completed_record_with_a_changed_payload_uses_the_next_generation_and_a_fresh_snapshot_id(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv"])
    world.run()
    key = world.unit_key()
    (first,) = world.intents().values()
    world.bodies[world.units[0]["url"]] = csv_body(7)
    changed = world.run(recheck=True)
    check(changed["counters"]["changed"] == 1 and changed["counters"]["intents_created"] == 1, str(changed["counters"]))
    check(sorted(world.bucket.names(INTENTS)) == [intent.intent_key(ROOT, key, 1), intent.intent_key(ROOT, key, 2)], "one intent per generation")
    second = world.intents()[intent.intent_key(ROOT, key, 2)]
    check(second["generation"] == 2 and second["snapshot_id"] != first["snapshot_id"], f"a fresh snapshot ID: {first['snapshot_id']} {second['snapshot_id']}")
    by_snapshot = {
        name: [body for body in world.records().values() if name in body["receipt"]["key"]] for name in (first["snapshot_id"], second["snapshot_id"])
    }
    check(all(len(found) == 1 for found in by_snapshot.values()), "each generation has its own record and objects")
    check(
        all(f"capture_id={second['snapshot_id']}" in item["key"] for item in by_snapshot[second["snapshot_id"]][0]["data_objects"]),
        "the new data is a new object",
    )
    again = world.run(recheck=True)
    check(
        again["counters"]["unchanged"] == 1 and again["counters"]["intents_created"] == 0 and len(world.intents()) == 2, "an unchanged recheck writes no intent"
    )


FORGERIES: dict[str, Callable[[dict], Any]] = {
    "not_json": lambda body: b"{not json",
    "extra_field": lambda body: body | {"note": "x"},
    "another_unit": lambda body: body | {"unit_key": "e" * 64},
    "another_generation": lambda body: body | {"generation": 2},
    "another_source": lambda body: body | {"snapshot_id": "OTHER_SOURCE__20260101T000000Z__" + "e" * 32},
    "malformed_snapshot": lambda body: body | {"snapshot_id": "not a snapshot id"},
}


def refused_without_a_trace(world: World, reason: str, before: set[str]) -> None:
    result = world.run()
    check([item["reason"] for item in result["units"]["refused"]] == [reason], f"the unit is refused with its reason: {result['units']}")
    check(result["units"]["failed"] == [] and result["units"]["completed"] == [], "it is neither failed nor completed")
    check(result["units"]["paused_sources"] == [], "the source is not paused: its download passed its contract")
    check(result["counters"]["objects_created"] == 0 and result["counters"]["intents_created"] == 0, str(result["counters"]))
    added = {f"{ROOT}ingestion_runs/run02/run.json", f"{ROOT}ingestion_runs/run02/state/000001.json"}
    check(world.bucket.names() - before == added, f"only the run's own objects were added: {sorted(world.bucket.names() - before - added)}")
    check(world.records() == {}, "no record was written")


@pytest.mark.parametrize("forgery", sorted(FORGERIES))
def test_a_forged_intent_stops_the_unit_visibly_and_never_gets_a_new_snapshot_id(tmp_path: Path, forgery: str) -> None:
    world, _ = crashed_world(tmp_path, dated=False)
    (target,) = world.bucket.names(INTENTS)
    version, body, fields = world.bucket.objects[target]
    forged = FORGERIES[forgery](json.loads(body))
    world.bucket.objects[target] = (version, forged if isinstance(forged, bytes) else encoded_json(forged), fields)
    refused_without_a_trace(world, "intent_forged", set(world.bucket.names()))


def test_an_unreadable_intent_stops_the_unit_visibly(tmp_path: Path) -> None:
    world, _ = crashed_world(tmp_path, dated=False)
    world.bucket.unreadable_prefix = INTENTS
    refused_without_a_trace(world, "intent_unreadable", set(world.bucket.names()))


def test_the_skip_listing_never_reads_the_intent_prefix_and_an_intent_alone_is_not_a_skip(tmp_path: Path) -> None:
    world, _ = crashed_world(tmp_path, dated=False)
    world.bucket.listed.clear()
    result = world.run()
    check(result["counters"]["skipped_complete"] == 0 and result["counters"]["units_attempted"] == 1, str(result["counters"]))
    check(world.bucket.listed != [] and not any(prefix.startswith(INTENTS) for prefix in world.bucket.listed), f"listed prefixes: {world.bucket.listed}")


def test_scratch_holds_at_most_one_units_files_at_a_time(three: World) -> None:
    three.run()
    check(three.site.scratch_seen != [] and all(len(seen) == 1 for seen in three.site.scratch_seen), f"scratch: {three.site.scratch_seen}")
    left = list((three.folder / "scratch").iterdir()) if (three.folder / "scratch").exists() else []
    check(left == [], f"no working files remain: {left}")


def test_a_source_pauses_when_its_first_unit_fails_its_contract(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv", "b.csv", "c.csv"])
    world.bodies[world.units[0]["url"]] = b"<html><body>Please sign in</body></html>"
    result = world.run()
    check(result["units"]["paused_sources"] == [SOURCE], str(result["units"]))
    check(world.site.asked == [world.units[0]["url"]], f"no other unit of the source was requested: {world.site.asked}")
    check(result["units"]["paused_not_attempted"] == [unit["snapshot_id"] for unit in world.units[1:]], "the rest of the source is listed as held")
    check(world.records() == {}, "nothing counts as complete")
    check(result["units"]["failed"][0]["reason"] == "html_instead_of_data", str(result["units"]["failed"]))
    state = world.bucket.names(f"{ROOT}ingestion_runs/run01/state/")
    check(len(state) == 1, f"the changed state was saved once: {sorted(state)}")


MEMBERS = [
    {"member_chain": ["release/data.csv"], "role": "data", "dataset_id": "synthetic_table"},
    {"member_chain": ["release/dictionary.pdf"], "role": "reference", "dataset_id": "synthetic_table"},
]


def archive(reference: bytes, data: bytes) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as handle:
        handle.writestr("release/data.csv", data)
        handle.writestr("release/dictionary.pdf", reference)
    return buffer.getvalue()


def pdf(number: int) -> bytes:
    return b"%PDF-1.7\nexample documentation " + str(number).encode() + b"\n%%EOF"


def test_a_data_change_in_a_later_run_counts_as_changed_and_a_reference_change_as_unchanged(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.zip"], {"a.zip": MEMBERS})
    url = world.units[0]["url"]
    world.bodies[url] = archive(pdf(1), b"id,value\n1,2\n")
    first = world.run()
    check(first["counters"]["units_attempted"] == 1 and len(world.records()) == 1, str(first["counters"]))
    world.bodies[url] = archive(pdf(2), b"id,value\n1,2\n")
    before = set(world.bucket.names())
    same = world.run(recheck=True)
    check(same["counters"]["unchanged"] == 1 and same["counters"]["objects_created"] == 0, str(same["counters"]))
    second = f"{ROOT}ingestion_runs/run02/"
    check(world.bucket.names() - before == {f"{second}run.json", f"{second}state/000001.json"}, "an unchanged unit writes only the run's own records")
    world.bodies[url] = archive(pdf(2), b"id,value\n1,3\n")
    changed = world.run(recheck=True)
    check(changed["counters"]["changed"] == 1 and len(world.records()) == 2, str(changed["counters"]))


def test_the_listing_diff_fails_on_any_extra_version_or_delete_marker(three: World) -> None:
    three.run()
    client = storage(three.bucket)
    before = mode.list_versions(client, [ROOT])
    three.run()
    after = mode.list_versions(client, [ROOT])
    allowed = {f"{ROOT}ingestion_runs/run02/run.json"}
    check(mode.diff_versions(before, after, allowed) == [], "run two differs by exactly its run record")
    check(mode.diff_versions(before, after, set()) != [], "an unexpected new object fails")
    plant(three.bucket, f"{ROOT}stray.json", b"{}")
    extra = mode.list_versions(client, [ROOT])
    check(any("stray.json" in problem for problem in mode.diff_versions(after, extra, set())), "an extra version fails")
    three.bucket.delete_markers.append({"Key": f"{ROOT}cms/marker", "VersionId": "m1"})
    marked = mode.list_versions(client, [ROOT])
    check(any("delete marker" in problem for problem in mode.diff_versions(extra, marked, {f"{ROOT}stray.json"})), "a delete marker fails")


def api_adapter(page: dict[str, bytes]) -> Any:
    """A collector stand-in: the data file is the download, and the raw API page is kept beside it with the role api_page."""

    def adapt(unit: dict, spec: dict, folder: Path, run: mode.CaptureRun, opener: Any) -> Path:
        path = mode.capture_file(unit, spec, folder, run, opener)
        receipt = read_json(path)
        lineage = json.loads(receipt["lineage"]["extraction_or_query"]) | {"batch_id": spec["plan_item"]}
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
        target = path.parent / "raw" / "page_0001.json"
        target.write_bytes(page["body"])
        digest = hashlib.sha256(page["body"]).hexdigest()
        artifact = copy.deepcopy(receipt["artifacts"][0]) | {
            "artifact_id": "artifact_0002",
            "role": "api_page",
            "stored_file_name": "page_0001.json",
            "storage_path": "raw/page_0001.json",
            "byte_count": len(page["body"]),
            "sha256": digest,
        }
        receipt["artifacts"].append(artifact)
        path.write_bytes(encoded_json(receipt))
        return path

    return adapt


def api_world(folder: Path) -> tuple[World, dict[str, bytes]]:
    world = World(folder, ["a.csv"])
    unit = world.units[0]
    unit["mode"] = "hud_api"
    specs = read_json(folder / "capture_specs.json")["specs"]
    (spec,) = specs.values()
    spec |= {"mode": "hud_api", "plan_item": "batch_0001"}
    world.write_inputs(specs)
    return world, {"body": b'{"page": 1}'}


def test_an_api_page_that_differs_while_the_data_roles_are_the_same_counts_as_unchanged(tmp_path: Path) -> None:
    world, page = api_world(tmp_path)
    adapters = {"hud_api": api_adapter(page)}
    first = world.run(adapters=adapters)
    check(first["counters"]["units_attempted"] == 1, str(first))
    (only,) = world.records().values()
    check([item["role"] for item in only["other_objects"]] == ["api_page"], "the page is named, with its role, outside the payload")
    page["body"] = b'{"page": 1, "queried": "later"}'
    second = world.run(adapters=adapters, recheck=True)
    check(second["counters"]["unchanged"] == 1 and second["counters"]["objects_created"] == 0, str(second["counters"]))
    check(len(world.records()) == 1, "no second record for the same data")


def test_a_collector_unit_without_a_plan_item_is_refused_and_never_keyed_by_its_url(tmp_path: Path) -> None:
    world, page = api_world(tmp_path)
    specs = read_json(tmp_path / "capture_specs.json")["specs"]
    for spec in specs.values():
        del spec["plan_item"]
    world.write_inputs(specs)
    result = world.run(adapters={"hud_api": api_adapter(page)})
    check(result["units"]["failed"][0]["reason"] == "spec_incomplete" and world.site.requests == 0, str(result["units"]))
    two = unit_of("b.csv") | {"mode": "hud_api"}
    one = world.units[0] | {"url": two["url"]}
    check(
        mode.unit_key_of(one, {"mode": "hud_api", "plan_item": "x"}) != mode.unit_key_of(one, {"mode": "hud_api", "plan_item": "y"}),
        "the plan item is in the key",
    )


def stale_world(tmp_path: Path) -> tuple[World, str]:
    world = World(tmp_path, ["a.csv"])
    key = mode.unit_key_of(world.units[0], read_json(tmp_path / "capture_specs.json")["specs"][world.units[0]["snapshot_id"]])
    stale = record.stale_prefix(ROOT, key) + "run00.json"
    plant(world.bucket, stale, b"{}")
    return world, stale


def test_a_unit_with_a_recent_stale_record_is_skipped_and_an_old_one_is_tried_again(tmp_path: Path) -> None:
    world, stale = stale_world(tmp_path)
    marked = next(item["LastModified"] for item in world.bucket.versions if item["Key"] == stale)
    skipped = world.run(clock=lambda: marked + datetime.timedelta(hours=19))
    check(
        skipped["counters"]["skipped_recent_stale"] == 1 and world.site.requests == 0 and skipped["counters"]["units_attempted"] == 0, str(skipped["counters"])
    )
    again = world.run(clock=lambda: marked + datetime.timedelta(hours=21))
    check(again["counters"]["units_attempted"] == 1 and len(world.records()) == 1, "after the retry interval the unit is tried again")


def test_the_run_refuses_inputs_that_do_not_match_or_hosts_that_are_missing(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv"])
    with pytest.raises(Exception, match="approved hosts file"):
        world.run(hosts=None)
    (tmp_path / "queue.json").write_bytes(json.dumps({"queue": world.units + [unit_of("z.csv")]}).encode())
    with pytest.raises(Exception, match="differs from its lock"):
        world.run()
    check(world.site.requests == 0 and world.bucket.names() == set(), "nothing was requested or written")


def test_the_run_refuses_a_root_other_than_the_one_the_storage_path_writes_under(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv"])
    with pytest.raises(Exception, match="capture root"):
        world.run(root="ingestion_test/t1/")
    check(world.site.requests == 0 and world.bucket.names() == set(), "nothing was requested or written")


def test_a_unit_over_the_byte_cap_fails_without_a_request(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv"])
    result = world.run(byte_cap=10)
    check(world.site.requests == 0 and result["units"]["failed"][0]["reason"] == "byte_cap_exceeded", str(result["units"]))


def test_the_evidence_step_fails_when_a_version_is_removed_or_a_delete_marker_appears(three: World) -> None:
    client = storage(three.bucket)
    before = mode.list_versions(client, mode.evidence_prefixes(ROOT))
    result = three.run()
    three.bucket.delete_markers.append({"Key": f"{ROOT}cms/gone", "VersionId": "m1"})
    check(any("delete marker" in problem for problem in mode.check_run(client, result, before)), "a delete marker fails the evidence step")
    three.bucket.delete_markers.clear()
    check(mode.check_run(client, result, before) == [], "without it the step passes")
    padded = before + [{"key": f"{ROOT}earlier", "version_id": "v0", "etag": "x", "kind": "version"}]
    check(any("removed or replaced" in problem for problem in mode.check_run(client, result, padded)), "a missing earlier version fails")


def test_the_evidence_step_accounts_for_intent_objects(three: World) -> None:
    client = storage(three.bucket)
    before = mode.list_versions(client, mode.evidence_prefixes(ROOT))
    result = three.run()
    check(result["counters"]["intents_created"] == 3 and mode.check_run(client, result, before) == [], "three intents, all named by the counters, pass")
    stray = intent.intent_key(ROOT, "d" * 64, 1)
    plant(three.bucket, stray, intent.encode(intent.build(unit_key="d" * 64, generation=1, snapshot_id=SOURCE + "__20260101T000000Z__" + "d" * 32, run_id="x")))
    check(any("new intents" in problem for problem in mode.check_run(client, result, before)), "an intent the counters do not name fails")
    unplant(three.bucket, stray)
    check(mode.check_run(client, result, before) == [], "without it the step passes")
    zero = intent.intent_key(ROOT, "d" * 64, 0)
    low = {
        "kind": intent.KIND,
        "schema_version": 1,
        "unit_key": "d" * 64,
        "generation": 0,
        "snapshot_id": SOURCE + "__20260101T000000Z__" + "d" * 32,
        "run_id": "x",
    }
    plant(three.bucket, zero, encoded_json(low))
    check(any(zero in problem for problem in mode.check_run(client, result, before)), "an intent of generation zero fails")
    unplant(three.bucket, zero)
    first = sorted(three.bucket.names(INTENTS))[0]
    version, _, fields = three.bucket.objects[first]
    three.bucket.objects[first] = (version, b"{not json", fields)
    check(any(first in problem for problem in mode.check_run(client, result, before)), "a malformed new intent fails")
    unplant(three.bucket, first)
    check(any("has no intent" in problem for problem in mode.check_run(client, result, before)), "a completed unit without its intent fails")


ADAPTER_MODULES = {
    "bls_api_v2": collect_bls_api,
    "census_acs_api": collect_census_acs_api,
    "census_acs_detailed": collect_census_acs_detailed,
    "hud_api": collect_hud_api,
    "mmd_api": collect_mmd_api,
    "cms_owners_org": store_cms_owners,
    "hcai_util_workbook": store_hcai_util,
    "onc_mu_hospital": store_onc_mu,
}


SWITCH_AT = {"onc_mu_hospital": 3, "mmd_api": 3}


@pytest.mark.parametrize("handler", sorted(ADAPTER_MODULES))
def test_each_collector_adapter_asks_its_collector_for_a_local_capture_and_never_stores_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, handler: str
) -> None:
    from scripts.acquisition import task_role

    calls: list[tuple] = []

    def execute(*arguments: Any, **options: Any) -> dict:
        calls.append((arguments, options))
        return {"receipt_path": str(tmp_path / "receipt.json")}

    monkeypatch.setattr(ADAPTER_MODULES[handler], "execute", execute)
    monkeypatch.setattr(mode, "planned_item", lambda run, name, spec: ({"id": "p", "limits": {"max_seconds": 60, "max_bytes": 1000}}, {"id": "i"}))
    chosen = storage(Bucket())
    run = mode.CaptureRun(
        tmp_path / "q.json",
        tmp_path / "s.json",
        ROOT,
        "run01",
        chosen,
        tmp_path / "scratch",
        CODE,
        BUNDLE,
        task_keys=lambda name: task_role.TaskKeys(name, {"AWS_REGION": "us-west-2"}),
    )
    unit = {"mode": handler}
    spec = {"mode": handler, "plan_item": "i", "mmd": {"measure_id": "M", "year": 2020}}
    path = mode.ADAPTERS[handler](unit, spec, tmp_path / "unit", run, "the-opener")
    check(path == tmp_path / "receipt.json", "the collector's local receipt is returned")
    arguments, options = calls[0]
    at = SWITCH_AT.get(handler, 4)
    check(arguments[at - 1] is True and arguments[at] is False, f"the network is on and storing is off: {arguments}")
    if handler in {"bls_api_v2", "census_acs_api", "census_acs_detailed", "hud_api"}:
        check(arguments[5].storage is chosen, "a keyed collector reads its key through the run's storage")
    if handler in {"cms_owners_org", "hcai_util_workbook", "onc_mu_hospital"}:
        check("the-opener" in arguments, "the counting transport is the one passed on")


def test_a_keyed_unit_is_not_attempted_when_the_keys_switch_is_off(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv"])
    specs = read_json(tmp_path / "capture_specs.json")["specs"]
    world.units[0]["mode"] = "bls_api_v2"
    for spec in specs.values():
        spec |= {"mode": "bls_api_v2", "plan_item": "b1"}
    world.write_inputs(specs)
    result = world.run()
    check(result["units"]["keyed_not_attempted"] == [world.units[0]["snapshot_id"]] and world.site.requests == 0, str(result["units"]))


def test_a_plan_item_names_exactly_one_planned_item(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts.acquisition import redownload

    plans = [{"id": "plan", "batches": [{"id": "b1"}, {"id": "b2"}]}]
    monkeypatch.setattr(redownload, "load_plans_of", lambda handler: plans)
    run = mode.CaptureRun(Path("q"), Path("s"), ROOT, "run01", None, Path("scratch"), CODE, BUNDLE)
    check(mode.planned_item(run, "hud_api", {"plan_item": "b2"}) == (plans[0], {"id": "b2"}), "the named batch of its plan")
    for spec in ({"plan_item": "none"}, {}):
        with pytest.raises(mode.SpecError):
            mode.planned_item(run, "hud_api", spec)


def test_a_real_collector_capture_runs_through_capture_mode_and_a_second_run_sends_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The ONC collector, its locked plan and its contract run for real; only the publisher and the bucket are stand-ins."""
    data = onc_body()
    plan, plan_path = onc_locked(tmp_path, data)
    versions = tmp_path / "onc_versions.json"
    s3_store.write_once(versions, encoded_json({"versions": [{"code_sha256": onc_contract.collector_code_hashes()}]}))
    monkeypatch.setattr(onc_contract, "PLAN_PATH", plan_path)
    monkeypatch.setattr(onc_contract, "VERSIONS_PATH", versions)
    (tmp_path / "world").mkdir()
    world = World(tmp_path / "world", ["x.csv"])
    unit = world.units[0] | {"url": plan["url"], "source_id": onc_contract.SOURCE_ID, "mode": "onc_mu_hospital", "bytes": len(data)}
    world.units[0] = unit
    world.bodies.clear()
    world.bodies[plan["url"]] = data
    world.write_inputs({unit["snapshot_id"]: {"mode": "onc_mu_hospital", "plan_item": plan["id"], "stored_data_files": []}})
    first = world.run()
    check(first["counters"]["units_attempted"] == 1 and len(first["units"]["completed"]) == 1, f"{first['units']} {first['counters']}")
    (only,) = world.records().values()
    check(only["mode"] == "onc_mu_hospital" and len(only["data_objects"]) >= 1, "the record names the collector's data")
    check(mode.verify_records(storage(world.bucket), ROOT) == [], "the record verifies by version")
    again = world.run()
    check(again["counters"]["skipped_complete"] == 1 and again["counters"]["http_requests"] == 0, str(again["counters"]))


def test_an_mmd_unit_is_keyed_by_its_measure_and_year_and_needs_no_plan_item() -> None:
    unit = unit_of("m.csv") | {"mode": "mmd_api"}
    first = {"mode": "mmd_api", "mmd": {"measure_id": "C258.07", "year": 2012}}
    second = {"mode": "mmd_api", "mmd": {"measure_id": "C258.07", "year": 2013}}
    check(mode.unit_key_of(unit, first) != mode.unit_key_of(unit, second), "the year is part of the identity")
    for spec in ({"mode": "mmd_api"}, {"mode": "mmd_api", "mmd": {"measure_id": "C258.07"}}):
        with pytest.raises(mode.SpecError):
            mode.unit_key_of(unit, spec)
    flagged = unit_of("n.csv") | {"flags": ["browser_export_moved_to_api:parity_verified"]}
    check(
        mode.unit_key_of(flagged, {"mode": "file", "mmd": first["mmd"]}) != mode.unit_key_of(flagged, {"mode": "file", "mmd": second["mmd"]}),
        "a flagged file unit is an MMD unit",
    )


def test_a_stale_expected_digest_in_a_receipt_template_is_ignored_and_a_reference_only_unit_still_gets_a_record(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv", "doc.pdf"])
    specs = read_json(tmp_path / "capture_specs.json")["specs"]
    first, second = specs.values()
    first["receipt_template"]["expected_sha256"] = "0" * 64
    second["receipt_template"]["role"] = "methodology"
    second["expected_format"] = "pdf"
    world.write_inputs(specs)
    world.bodies[world.units[1]["url"]] = b"%PDF-1.7\nexample documentation\n%%EOF"
    result = world.run()
    check(len(result["units"]["completed"]) == 2, f"{result['units']}")
    by_name = {item["data_objects"][0]["stored_file_name"] if item["data_objects"] else "": item for item in world.records().values()}
    check(set(by_name) == {"a.csv", ""}, f"the reference-only unit names no data object: {sorted(by_name)}")
    check([item["role"] for item in by_name[""]["other_objects"]] == ["methodology"], "its document is named with its role")


def test_a_real_grouped_plan_item_resolves_and_its_collector_runs_through_capture_mode(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The CMS owners collector reads a plan with a list of releases, so the plan item names one release inside its plan."""
    plan, plan_path, bodies = owners_plan(tmp_path)
    versions = tmp_path / "owners_versions.json"
    s3_store.write_once(versions, encoded_json({"versions": [{"code_sha256": owners_contract.collector_code_hashes()}]}))
    monkeypatch.setattr(owners_contract, "PLAN_PATH", plan_path)
    monkeypatch.setattr(owners_contract, "VERSIONS_PATH", versions)
    release = plan["releases"][0]
    (tmp_path / "world").mkdir()
    world = World(tmp_path / "world", ["x.csv"])
    unit = world.units[0] | {"url": release["url"], "source_id": "CMS_OWNERS", "mode": "cms_owners_org", "bytes": len(bodies[release["url"]])}
    world.units[0] = unit
    world.bodies.clear()
    world.bodies.update(bodies)
    world.write_inputs({unit["snapshot_id"]: {"mode": "cms_owners_org", "plan_item": release["id"], "stored_data_files": []}})
    result = world.run()
    check(len(result["units"]["completed"]) == 1, f"{result['units']} {result['counters']}")
    check(mode.verify_records(storage(world.bucket), ROOT) == [], "the record verifies by version")


def deny_puts(world: World, monkeypatch: pytest.MonkeyPatch, prefix: str, nth: int = 1) -> None:
    """Refuse the nth put whose key starts with the prefix and let every other put through."""
    original, seen = world.bucket.put_object, []

    def put(**arguments: Any) -> dict:
        if arguments["Key"].startswith(prefix):
            seen.append(arguments["Key"])
            if len(seen) == nth:
                raise world.bucket.missing("PutObject", "AccessDenied")
        return original(**arguments)

    monkeypatch.setattr(world.bucket, "put_object", put)


def listed(world: World) -> list[dict]:
    return mode.list_versions(storage(world.bucket), mode.evidence_prefixes(ROOT))


def sibling(world: World, name: str, names: list[str]) -> World:
    """A second world with its own folder and publisher that shares the first one's bucket."""
    folder = world.folder / name
    folder.mkdir()
    other = World(folder, names)
    other.bucket = world.bucket
    return other


def forged_record_world(world: World, change: Callable[[dict], dict]) -> World:
    """A world whose queue holds only b.csv and whose store holds a.csv's valid record relabelled as b.csv's, at b.csv's record path."""
    (_, body), *_ = world.records().items()
    unit = unit_of("b.csv")
    spec = spec_of("b.csv")
    world.units = [unit]
    world.bodies[unit["url"]] = csv_body(99)
    world.write_inputs({unit["snapshot_id"]: spec})
    forged = change(copy.deepcopy(body) | {"unit_key": mode.unit_key_of(unit, spec)})
    plant(world.bucket, record.record_key(ROOT, forged["unit_key"], forged["payload_sha256"]), record.encode(forged))
    return world


RELABELS: dict[str, Callable[[dict], dict]] = {
    "request_of_another_unit": lambda body: body,
    "another_source": lambda body: body | {"source_id": "CMS_OTHER"},
    "another_mode": lambda body: body | {"mode": "onc_mu_hospital"},
    "another_collection": lambda body: body | {"collection": "cms/other_collection"},
}


@pytest.mark.parametrize("relabel", sorted(RELABELS))
def test_a_record_relabelled_for_another_unit_is_refused_and_not_skipped(tmp_path: Path, relabel: str) -> None:
    world = World(tmp_path, ["a.csv"])
    world.run()
    forged_record_world(world, RELABELS[relabel])
    asked = world.site.requests
    result = world.run()
    check(result["counters"]["skipped_complete"] == 0, f"a relabelled record is not a skip: {result['counters']}")
    check(world.site.requests == asked, "a refused unit gets no request")
    check(len(result["units"]["refused"]) == 1 and result["units"]["refused"][0]["reason"], f"the unit is refused with its reason: {result['units']}")


def test_a_record_whose_data_object_comes_from_another_unit_is_refused(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv", "b.csv"])
    world.run()
    first, second = world.records().values()
    forged = record.build(
        key=first["unit_key"],
        source_id=first["source_id"],
        collection=first["collection"],
        mode=first["mode"],
        run_id="forger",
        code_sha256=CODE,
        bundle_sha256=BUNDLE,
        retrieved_at_utc=first["retrieved_at_utc"],
        receipt=first["receipt"],
        manifests=first["manifests"],
        data_objects=second["data_objects"],
    )
    plant(world.bucket, record.record_key(ROOT, forged["unit_key"], forged["payload_sha256"]), record.encode(forged))
    result = world.run()
    check(result["counters"]["skipped_complete"] == 1, f"only the untouched unit is a skip: {result['counters']}")
    check([item["unit_key"] for item in result["units"]["refused"]] == [first["unit_key"]], f"the forged unit is refused: {result['units']['refused']}")


def test_the_evidence_step_fails_a_unit_whose_completion_record_was_never_stored(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = World(tmp_path, ["a.csv"])
    before = listed(world)
    deny_puts(world, monkeypatch, f"{ROOT}ingestion_units/")
    result = world.run()
    check(len(result["units"]["failed"]) == 1 and world.records() == {}, f"the unit failed and has no record: {result['units']}")
    problems = mode.check_run(storage(world.bucket), result, before)
    check(any(world.unit_key() in problem and "did not complete" in problem for problem in problems), f"the failed unit fails the evidence step: {problems}")


def held_world(tmp_path: Path) -> tuple[World, dict]:
    world = World(tmp_path, ["a.csv", "b.csv"])
    world.bodies[world.units[0]["url"]] = b"<html><body>Please sign in</body></html>"
    return world, world.run()


def refused_world(tmp_path: Path) -> tuple[World, dict]:
    world = World(tmp_path, ["a.csv"])
    world.run()
    key, body = next(iter(world.records().items()))
    body["data_objects"][0]["sha256"] = "f" * 64
    version, _, fields = world.bucket.objects[key]
    world.bucket.objects[key] = (version, encoded_json(body), fields)
    return world, world.run()


def keyed_world(tmp_path: Path) -> tuple[World, dict]:
    world = World(tmp_path, ["a.csv"])
    specs = read_json(tmp_path / "capture_specs.json")["specs"]
    world.units[0]["mode"] = "bls_api_v2"
    for spec in specs.values():
        spec |= {"mode": "bls_api_v2", "plan_item": "b1"}
    world.write_inputs(specs)
    return world, world.run()


def unspecified_world(tmp_path: Path) -> tuple[World, dict]:
    world = World(tmp_path, ["a.csv"])
    world.write_inputs({})
    return world, world.run()


INCOMPLETE = {
    "failed_and_held": (held_world, "did not complete"),
    "refused": (refused_world, "did not complete"),
    "keys_off": (keyed_world, "was not attempted"),
    "spec_incomplete": (unspecified_world, "did not complete"),
}


@pytest.mark.parametrize("outcome", sorted(INCOMPLETE))
def test_the_evidence_step_fails_every_incomplete_unit_outcome(tmp_path: Path, outcome: str) -> None:
    build, expected = INCOMPLETE[outcome]
    world, result = build(tmp_path)
    problems = mode.check_run(storage(world.bucket), result, [])
    check(any(expected in problem for problem in problems), f"an incomplete outcome fails the bootstrap check with '{expected}': {problems}")


def test_the_evidence_step_names_a_held_unit(tmp_path: Path) -> None:
    world, result = held_world(tmp_path)
    problems = mode.check_run(storage(world.bucket), result, [])
    check(any("was not attempted" in problem and world.units[1]["snapshot_id"] in problem for problem in problems), f"the held unit is named: {problems}")


def test_the_evidence_step_rejects_an_object_this_run_did_not_record_writing(three: World) -> None:
    before = listed(three)
    result = three.run()
    plant(three.bucket, f"{DATASETS}stray/object.csv", b"id,value\n")
    problems = mode.check_run(storage(three.bucket), result, before)
    check(any("stray/object.csv" in problem and "unexpected" in problem for problem in problems), f"an object the run never recorded writing fails: {problems}")


def test_the_run_record_lists_the_units_it_skipped_and_the_evidence_step_reconciles_them(three: World) -> None:
    three.run()
    before = listed(three)
    result = three.run()
    keys = sorted(three.unit_key(index) for index in range(3))
    check(result["units"]["skipped_complete"] == keys, f"the run record names the units it skipped: {result['units']}")
    check(mode.check_run(storage(three.bucket), result, before) == [], "run two reconciles")
    missing = copy.deepcopy(result)
    missing["units"]["skipped_complete"].append("e" * 64)
    check(
        any("e" * 64 in problem for problem in mode.check_run(storage(three.bucket), missing, before)),
        "a unit the run claims it skipped, with no record, fails",
    )
    stale = copy.deepcopy(result)
    stale["units"]["skipped_recent_stale"] = ["e" * 64]
    check(
        any("e" * 64 in problem and "stale" in problem for problem in mode.check_run(storage(three.bucket), stale, before)),
        "a stale skip without a stale record fails",
    )


def race_world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, conflict: str) -> tuple[World, dict, list[str]]:
    """Two units. While the first unit's record is written, a rival run completes that unit, and the write then reports the conflict."""
    outer = World(tmp_path, ["a.csv", "b.csv"])
    rival = sibling(outer, "rival", ["a.csv"])
    rival.runs = 1
    original, own = outer.bucket.put_object, []
    state: dict[str, Any] = {"racing": False, "lost": False}

    def racing_put(**arguments: Any) -> dict:
        if state["racing"]:
            return original(**arguments)
        if arguments["Key"].startswith(f"{ROOT}ingestion_units/") and not state["lost"]:
            state["racing"] = True
            rival_result = rival.run()
            state["racing"], state["lost"] = False, True
            state["rival"] = rival_result
            raise outer.bucket.missing("PutObject", conflict)
        answer = original(**arguments)
        own.append(arguments["Key"])
        return answer

    monkeypatch.setattr(outer.bucket, "put_object", racing_put)
    result = outer.run()
    return outer, result | {"rival": state["rival"]}, own


@pytest.mark.parametrize("conflict", ["PreconditionFailed", "ConditionalRequestConflict"])
def test_a_run_that_loses_the_completion_race_to_an_equivalent_record_completes_and_claims_no_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, conflict: str
) -> None:
    outer, result, own = race_world(tmp_path, monkeypatch, conflict)
    check(len(result["rival"]["units"]["completed"]) == 1, "the rival run completed the first unit")
    check(result["units"]["failed"] == [] and result["units"]["refused"] == [], f"the loser is not failed: {result['units']}")
    check(result["units"]["paused_sources"] == [] and result["units"]["paused_not_attempted"] == [], "the loser's source is not paused")
    check(len(result["units"]["completed"]) == 2 and len(outer.records()) == 2, f"both units are complete with one record each: {result['units']}")
    created = [key for key in own if not key.startswith((f"{ROOT}ingestion_intents/", f"{ROOT}ingestion_runs/"))]
    check(result["counters"]["objects_created"] == len(created), f"the loser claims only what it created: {result['counters']} {len(created)}")
    check(
        not any(key.startswith(f"{ROOT}ingestion_units/") and key.endswith(outer.unit_key() + "/") for key in own),
        "the loser created no record of the first unit",
    )


def test_a_lost_completion_race_whose_winner_cannot_be_read_retries_a_bounded_number_of_times_then_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    world = World(tmp_path, ["a.csv", "b.csv"])
    slept: list[float] = []
    monkeypatch.setattr("time.sleep", lambda seconds: slept.append(seconds))
    original = world.bucket.put_object

    def conflict(**arguments: Any) -> dict:
        if arguments["Key"].startswith(f"{ROOT}ingestion_units/"):
            raise world.bucket.missing("PutObject", "ConditionalRequestConflict")
        return original(**arguments)

    monkeypatch.setattr(world.bucket, "put_object", conflict)
    result = world.run()
    reasons = [item["reason"] for item in result["units"]["failed"]]
    check(reasons == ["completion_conflict_unreadable"], f"a winner that never reads is a visible failure: {result['units']}")
    check(0 < len(slept) <= 6 and sum(slept) <= 60, f"the retries and their waits are bounded: {slept}")


def test_a_lost_completion_race_to_a_record_that_does_not_verify_is_refused(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv"])
    payload = record.payload_sha256([("a.csv", hashlib.sha256(csv_body(1)).hexdigest())])
    target = record.record_key(ROOT, world.unit_key(), payload)
    world.bucket.before_put[f"{ROOT}ingestion_units/"] = lambda: plant(world.bucket, target, b"{}")
    result = world.run()
    check(len(result["units"]["refused"]) == 1 and result["units"]["completed"] == [], f"a winner that does not verify is refused: {result['units']}")


def test_the_proof_run_fails_the_evidence_step_when_a_state_object_appears_beside_its_run_record(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv"])
    world.run()
    before = listed(world)
    result = world.run()
    plant(world.bucket, f"{ROOT}ingestion_runs/run02/state/000001.json", b"{}")
    problems = mode.check_run(storage(world.bucket), result, before)
    check(any("state/000001.json" in problem for problem in problems), f"a state version the run did not write fails: {problems}")


def test_the_counters_keep_a_successful_create_through_a_failed_upload(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = World(tmp_path, ["a.csv"])
    deny_puts(world, monkeypatch, f"{ROOT}cms/", nth=2)
    result = world.run()
    stored = {key for key in world.bucket.names() if kind_of(key) in {"data", "audit", "receipt", "manifest", "other"}}
    check(len(result["units"]["failed"]) == 1 and len(stored) >= 1, f"an object was stored before the failure: {sorted(stored)} {result['units']}")
    check(result["counters"]["objects_created"] == len(stored), f"the counter keeps the creates of a failed upload: {result['counters']} {sorted(stored)}")


def test_bytes_downloaded_are_the_response_bytes_read_whether_or_not_the_unit_succeeds(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv", "b.csv"])
    page = b"<html><body>Please sign in</body></html>"
    world.bodies[world.units[0]["url"]] = page
    failed = world.run()
    check(failed["counters"]["bytes_downloaded"] == len(page), f"a rejected response counts the bytes received: {failed['counters']}")
    folder = tmp_path / "fresh"
    folder.mkdir()
    fresh = World(folder, ["a.csv", "b.csv", "c.csv"])
    done = fresh.run()
    expected = sum(len(csv_body(number)) for number in (1, 2, 3))
    check(done["counters"]["bytes_downloaded"] == expected, f"a completed unit counts its response bytes only: {done['counters']} {expected}")


def test_the_byte_cap_stops_a_unit_whose_response_is_larger_than_its_estimate(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv"])
    body = b"id,value\n" + b"1,2\n" * 100
    world.bodies[world.units[0]["url"]] = body
    result = world.run(byte_cap=101)
    check(result["units"]["completed"] == [] and world.records() == {}, f"the unit does not complete past the cap: {result['units']}")
    check([item["reason"] for item in result["units"]["failed"]] == ["byte_cap_exceeded"], f"the failure is the cap: {result['units']['failed']}")
    check(result["counters"]["bytes_downloaded"] == len(body), f"the bytes received are counted: {result['counters']}")


def test_a_source_pauses_when_its_first_unit_has_no_specification(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv", "b.csv"])
    specs = read_json(tmp_path / "capture_specs.json")["specs"]
    del specs[world.units[0]["snapshot_id"]]
    world.write_inputs(specs)
    result = world.run()
    check([item["reason"] for item in result["units"]["failed"]] == ["spec_incomplete"], str(result["units"]))
    check(result["units"]["paused_sources"] == [SOURCE], f"the first unit's specification failure pauses its source: {result['units']}")
    check(
        result["units"]["paused_not_attempted"] == [world.units[1]["snapshot_id"]] and world.site.requests == 0,
        "the next unit is held and nothing is requested",
    )


def foreign_ledger_entry(world: World, monkeypatch: pytest.MonkeyPatch, number: int) -> None:
    """Another writer reserves one BLS request while this run reads a record."""
    original = world.bucket.head_object
    injected: list[bool] = []

    def another_writer(**arguments: Any) -> dict:
        if arguments["Key"].startswith(f"{ROOT}ingestion_") and not injected:
            injected.append(True)
            plant(world.bucket, f"{mode.task_role.BLS_LEDGER_PREFIX}{number:010d}.json", b"{}")
        return original(**arguments)

    monkeypatch.setattr(world.bucket, "head_object", another_writer)


def test_another_writers_ledger_entry_is_not_this_runs_and_fails_the_proof_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = World(tmp_path, ["a.csv"])
    world.run()
    before = listed(world)
    foreign_ledger_entry(world, monkeypatch, 1)
    result = world.run()
    check(result["counters"]["units_attempted"] == 0 and result["counters"]["bls_ledger_entries_added"] == 0, f"the run reserved nothing: {result['counters']}")
    problems = mode.check_run(storage(world.bucket), result, before)
    check(any("ledger" in problem for problem in problems), f"the proof run fails when the shared ledger changed: {problems}")


def test_another_writers_ledger_entry_does_not_fail_a_run_that_attempted_units(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world = World(tmp_path, ["a.csv"])
    before = listed(world)
    foreign_ledger_entry(world, monkeypatch, 1)
    original = world.bucket.get_object

    def late_reader(**arguments: Any) -> dict:
        if arguments["Key"].startswith(f"{ROOT}ingestion_intents/"):
            plant(world.bucket, f"{mode.task_role.BLS_LEDGER_PREFIX}{2:010d}.json", b"{}")
        return original(**arguments)

    monkeypatch.setattr(world.bucket, "get_object", late_reader)
    result = world.run()
    check(
        result["counters"]["units_attempted"] == 1 and result["counters"]["bls_ledger_entries_added"] == 0,
        f"foreign entries are not this run's: {result['counters']}",
    )
    check(mode.check_run(storage(world.bucket), result, before) == [], "a run that attempted units tolerates a foreign reservation")


def bls_world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[World, bytes, list[dict], dict]:
    """One BLS unit with the real BLS collector, contract, transport and storage path. Only the connection and the cloud clients are stand-ins."""
    import base64
    from types import SimpleNamespace

    from scripts.acquisition import bls_api_contract as contract
    from scripts.acquisition import bls_api_transport as bls
    from scripts.acquisition import task_role
    from scripts.acquisition.run_bls_api_e2e import fixture
    from scripts.acquisition.source_registry import canonical_hash, load_registry
    from scripts.acquisition.tests.test_task_keys import ENVIRONMENT, Cloud, Reply, connection_class

    batch: dict = {"series_ids": ["LAUCN010010000000003"], "start_year": 2000, "end_year": 2000}
    batch["id"] = contract.batch_id(batch)
    plan = {
        "source_id": "BLS",
        "vintage": "2000-01-01",
        "endpoint": "https://api.bls.gov/publicAPI/v2/timeseries/data/",
        "credential_reference": "bls_api_key",
        "request_limit_24h": 450,
        "request_spacing_seconds": 0,
        "registry_sha256": canonical_hash(load_registry()),
        "references": [],
        "batches": [batch],
        "model_eligible": False,
    }
    plan_path, versions = tmp_path / "bls_plan.json", tmp_path / "bls_versions.json"
    plan_path.write_bytes(encoded_json(plan))
    plan_path.with_suffix(".lock.json").write_bytes(encoded_json({"plan_sha256": canonical_hash(plan)}))
    versions.write_bytes(encoded_json({"versions": [{"code_sha256": contract.collector_code_hashes()}]}))
    monkeypatch.setattr(contract, "PLAN_PATH", plan_path)
    monkeypatch.setattr(contract, "VERSIONS_PATH", versions)
    folder = tmp_path / "world"
    folder.mkdir()
    world = World(folder, ["a.csv"])
    unit = world.units[0] | {"mode": "bls_api_v2", "source_id": "BLS", "url": plan["endpoint"], "bytes": 1}
    world.units = [unit]
    world.write_inputs({unit["snapshot_id"]: {"mode": "bls_api_v2", "plan_item": batch["id"]}})
    payload = fixture(batch)
    sent: list[dict] = []
    monkeypatch.setattr(bls, "https_connection", connection_class(Reply(200, payload), sent))
    cloud = Cloud()

    def ledger_put(**arguments: Any) -> dict:
        body = arguments["Body"]
        arguments["Body"] = io.BytesIO(body)
        arguments["ChecksumSHA256"] = base64.b64encode(hashlib.sha256(body).digest()).decode()
        return world.bucket.put_object(**arguments)

    def factory(service: str, region: str) -> Any:
        if service == "s3":
            return SimpleNamespace(list_objects_v2=world.bucket.list_objects_v2, put_object=ledger_put)
        return cloud.factory(service, region)

    environment = ENVIRONMENT | {"DATA_BUCKET_NAME": BUCKET}

    def keys(handler: str) -> Any:
        return task_role.TaskKeys(handler, environment, factory, clock=lambda: BASE_TIME + datetime.timedelta(minutes=1))

    return world, payload, sent, {"task_keys": keys, "cache": {"plans:bls_api_v2": [plan]}}


def test_a_real_bls_unit_counts_its_direct_https_request_its_response_bytes_and_its_own_ledger_entry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    world, payload, sent, fields = bls_world(tmp_path, monkeypatch)
    before = listed(world)
    result = world.run(**fields)
    check(len(result["units"]["completed"]) == 1, f"the real collector completed: {result['units']}")
    counters = result["counters"]
    check(len(sent) == 1 and counters["http_requests"] == 1, f"the direct request is counted: {len(sent)} {counters}")
    check(counters["bytes_downloaded"] == len(payload), f"the bytes are the response body, not the derived CSV: {counters} {len(payload)}")
    check(counters["bls_ledger_entries_added"] == 1, f"this run's own reservation is counted: {counters}")
    problems = mode.check_run(storage(world.bucket), result, before)
    check(problems == [], f"the evidence step passes: {problems}")


@pytest.mark.parametrize("module", mode.API_TRANSPORTS, ids=lambda module: module.__name__.rsplit(".", 1)[-1])
def test_every_direct_https_boundary_counts_one_request_per_send_and_is_restored_afterwards(module: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[str] = []

    class Connection:
        def request(self, method: str, path: str, **options: Any) -> None:
            sent.append(path)

    def stand_in(netloc: str, timeout: float) -> Connection:
        return Connection()

    monkeypatch.setattr(module, "https_connection", stand_in)
    observer = mode.Observer(byte_cap=10**9)
    with mode.observing(observer):
        module.https_connection("example.test", 5).request("GET", "/one")
        module.https_connection("example.test", 5).request("POST", "/two", body=b"x")
    check(observer.requests == 2 and sent == ["/one", "/two"], f"each send is counted once: {observer.requests} {sent}")
    check(module.https_connection is stand_in, "the module's own connection function is restored")


def hcai_world(tmp_path: Path, binding: str | None, source: str = "HCAI_UTIL") -> World:
    """A one-unit world whose unit is a reference document of the given source, as the held-source specifications are."""
    world = World(tmp_path, ["doc.pdf"])
    unit = world.units[0] | {"source_id": source}
    world.units = [unit]
    world.bodies[unit["url"]] = b"%PDF-1.7\nexample documentation\n%%EOF"
    spec = spec_of("doc.pdf")
    spec["expected_format"] = "pdf"
    spec["receipt_template"] |= {"role": "methodology", "source_id": source}
    if binding is not None:
        spec["release_binding"] = binding
    world.write_inputs({unit["snapshot_id"]: spec})
    return world


def test_a_held_source_unit_captures_through_its_release_binding(tmp_path: Path) -> None:
    world = hcai_world(tmp_path, "access_release")
    result = world.run()
    check(len(result["units"]["completed"]) == 1, f"the unit captured through the binding: {result['units']}")
    (only,) = world.records().values()
    receipt = json.loads(world.bucket.objects[only["receipt"]["key"]][1])
    expected = hashlib.sha256(s3_store.ACCESS_RELEASE_PATH.read_bytes()).hexdigest()
    bound = json.loads(receipt["lineage"]["extraction_or_query"]).get("access_release_sha256")
    check(bound == expected, f"the receipt binds the tracked release record: {bound}")


@pytest.mark.parametrize("case", ["unknown_binding", "binding_on_a_source_that_is_not_held"])
def test_a_spec_with_an_unusable_release_binding_is_refused_before_any_request(tmp_path: Path, case: str) -> None:
    world = hcai_world(tmp_path, "no_such_binding") if case == "unknown_binding" else hcai_world(tmp_path, "access_release", source=SOURCE)
    result = world.run()
    check([item["reason"] for item in result["units"]["failed"]] == ["spec_incomplete"] and world.site.requests == 0, f"{result['units']}")


def test_the_run_restores_the_shared_read_boundary_after_it_ends_and_after_a_crash(tmp_path: Path) -> None:
    from scripts.acquisition import redownload_controls

    shared = redownload_controls.read
    World(tmp_path, ["a.csv"]).run()
    check(redownload_controls.read is shared, "the read function is restored after a run")
    folder = tmp_path / "crash"
    folder.mkdir()
    crashing = World(folder, ["a.csv"])
    crashing.bucket.crash_on_prefix = f"{ROOT}ingestion_units/"
    with pytest.raises(Crash):
        crashing.run()
    check(redownload_controls.read is shared, "the read function is restored after a process death")


def test_a_later_unit_without_a_specification_fails_alone_and_does_not_pause_its_source(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv", "b.csv", "c.csv"])
    specs = read_json(tmp_path / "capture_specs.json")["specs"]
    del specs[world.units[1]["snapshot_id"]]
    world.write_inputs(specs)
    result = world.run()
    check([item["reason"] for item in result["units"]["failed"]] == ["spec_incomplete"], str(result["units"]))
    check(
        len(result["units"]["completed"]) == 2 and result["units"]["paused_sources"] == [], f"only the unit without a specification failed: {result['units']}"
    )


def test_the_evidence_step_fails_a_stale_skip_older_than_the_retry_interval(tmp_path: Path) -> None:
    world, stale = stale_world(tmp_path)
    marked = next(item["LastModified"] for item in world.bucket.versions if item["Key"] == stale)
    before = listed(world)
    result = world.run(clock=lambda: marked + datetime.timedelta(hours=19))
    check(mode.check_run(storage(world.bucket), result, before) == [], "a stale record younger than the interval reconciles")
    late = result | {"started_at_utc": (marked + datetime.timedelta(hours=21)).isoformat()}
    check(any("stale" in problem for problem in mode.check_run(storage(world.bucket), late, before)), "a stale record older than the interval fails")


def test_a_put_through_the_bounded_call_is_journaled_like_any_other_create(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv"])
    journal = mode.Journal()
    journaled = mode.JournalStorage(storage(world.bucket), journal)
    path = tmp_path / "object.json"
    path.write_bytes(b"{}")
    arguments = [
        "--key",
        f"{ROOT}ingestion_runs/x/run.json",
        "--body",
        str(path),
        "--if-none-match",
        "*",
        "--checksum-sha256",
        "e30=",
        "--server-side-encryption",
        "AES256",
    ]
    journaled.call_bounded("s3api", "put-object", [*arguments, "--checksum-algorithm", "SHA256", "--metadata", "{}"], timeout=5)
    journaled.call("s3api", "head-object", ["--key", f"{ROOT}ingestion_runs/x/run.json"])
    check(journal.keys == [f"{ROOT}ingestion_runs/x/run.json"], f"only the successful put is noted: {journal.keys}")


def test_a_record_of_one_planned_item_is_refused_for_another_item_of_the_same_request(tmp_path: Path) -> None:
    world, page = api_world(tmp_path)
    adapters = {"hud_api": api_adapter(page)}
    world.run(adapters=adapters)
    (_, body), *_ = world.records().items()
    specs = read_json(tmp_path / "capture_specs.json")["specs"]
    for spec in specs.values():
        spec["plan_item"] = "batch_0002"
    world.write_inputs(specs)
    forged = body | {"unit_key": world.unit_key()}
    plant(world.bucket, record.record_key(ROOT, forged["unit_key"], forged["payload_sha256"]), record.encode(forged))
    asked = world.site.requests
    result = world.run(adapters=adapters)
    check(result["counters"]["skipped_complete"] == 0 and world.site.requests == asked, f"the other item's record is not a skip: {result['counters']}")
    check(len(result["units"]["refused"]) == 1, f"the unit is refused: {result['units']}")


def rewrite_record(world: World, key: str, body: dict) -> None:
    """Replace a stored record's bytes in place, as a store edit would."""
    version, _, fields = world.bucket.objects[key]
    world.bucket.objects[key] = (version, record.encode(body), fields)


@pytest.mark.parametrize(("name", "value"), [("source_id", "CMS_OTHER"), ("mode", "onc_mu_hospital"), ("collection", "cms/other_collection")])
def test_a_record_that_names_another_source_mode_or_collection_than_its_unit_is_refused(tmp_path: Path, name: str, value: str) -> None:
    world = World(tmp_path, ["a.csv"])
    world.run()
    (key, body), *_ = world.records().items()
    rewrite_record(world, key, body | {name: value})
    asked = world.site.requests
    result = world.run()
    check(result["counters"]["skipped_complete"] == 0 and world.site.requests == asked, f"an edited record is not a skip: {result['counters']}")
    check(
        len(result["units"]["refused"]) == 1 and name.split("_")[0] in result["units"]["refused"][0]["reason"],
        f"the refusal names the field: {result['units']}",
    )


def test_a_record_whose_manifest_lists_its_data_object_at_another_version_is_refused(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv"])
    world.run()
    (key, body), *_ = world.records().items()
    manifest = json.loads(world.bucket.objects[body["manifests"][0]["key"]][1])
    for item in manifest["objects"]:
        if item["object"]["key"] == body["data_objects"][0]["key"]:
            item["object"]["version_id"] = "version_that_lists_another_copy"
    raw = encoded_json(manifest)
    digest = hashlib.sha256(raw).hexdigest()
    forged_key = f"{DATASETS}forged/manifest.json"
    answer = world.bucket.put_object(
        Bucket=BUCKET,
        ExpectedBucketOwner=ACCOUNT,
        Key=forged_key,
        Body=io.BytesIO(raw),
        IfNoneMatch="*",
        ChecksumSHA256=mode.base_text(digest),
        ServerSideEncryption="AES256",
    )
    forged = {"key": forged_key, "version_id": answer["VersionId"], "sha256": digest, "byte_count": len(raw)}
    rewrite_record(world, key, body | {"manifests": [forged]})
    result = world.run()
    check(
        len(result["units"]["refused"]) == 1 and "not listed" in result["units"]["refused"][0]["reason"],
        f"the manifest and the record disagree: {result['units']}",
    )


def mmd_adapter(unit: dict, spec: dict, folder: Path, run: mode.CaptureRun, opener: Any) -> Path:
    """A collector stand-in that keeps the measure and year in the receipt lineage, as the real MMD collector does."""
    path = mode.capture_file(unit, spec, folder, run, opener)
    receipt = read_json(path)
    lineage = json.loads(receipt["lineage"]["extraction_or_query"]) | spec["mmd"]
    receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
    path.write_bytes(encoded_json(receipt))
    return path


def test_a_record_of_one_mmd_year_is_refused_for_another_year(tmp_path: Path) -> None:
    world = World(tmp_path, ["a.csv"])
    world.units[0]["mode"] = "mmd_api"
    specs = read_json(tmp_path / "capture_specs.json")["specs"]
    (spec,) = specs.values()
    spec |= {"mode": "mmd_api", "mmd": {"measure_id": "C258.07", "year": 2012}}
    world.write_inputs(specs)
    adapters = {"mmd_api": mmd_adapter}
    first = world.run(adapters=adapters)
    check(len(first["units"]["completed"]) == 1, f"the stand-in captured the unit: {first['units']}")
    (_, body), *_ = world.records().items()
    spec["mmd"]["year"] = 2013
    world.write_inputs(specs)
    forged = body | {"unit_key": world.unit_key()}
    plant(world.bucket, record.record_key(ROOT, forged["unit_key"], forged["payload_sha256"]), record.encode(forged))
    asked = world.site.requests
    result = world.run(adapters=adapters)
    check(result["counters"]["skipped_complete"] == 0 and world.site.requests == asked, f"the other year's record is not a skip: {result['counters']}")
    check(len(result["units"]["refused"]) == 1, f"the unit is refused: {result['units']}")
