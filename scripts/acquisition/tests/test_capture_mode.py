"""Capture mode (design 8.3): a completion record per unit decides skip, and a second run sends nothing and writes only its run record (D3, D10)."""

from __future__ import annotations

import copy
import datetime
import hashlib
import io
import json
import zipfile
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import capture_mode as mode
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
from scripts.acquisition import redownload_route as route
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.source_registry import REPO_ROOT, read_json
from scripts.acquisition.tests.test_task_owners import ACCOUNT, BUCKET, TaskS3, Web, opener, storage
from tests.support import check

ROOT = "raw/"
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
        self.clock = 0

    def put_object(self, **arguments: Any) -> dict:
        if self.crash_on_prefix and arguments["Key"].startswith(self.crash_on_prefix):
            self.crash_on_prefix = None
            raise Crash
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

    def list_objects_v2(self, **arguments: Any) -> dict:
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
    zero = ("http_requests", "bytes_downloaded", "units_attempted", "objects_created", "bls_ledger_entries_added", "skipped_recent_stale")
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


def one_orphan_receipt(world: World, before: set[str]) -> bool:
    return len({key for key in world.bucket.names() if key.endswith("/receipt.json")} - before) == 1


def test_a_crash_after_the_data_objects_and_before_the_record_adds_no_data_version_for_a_dated_unit(tmp_path: Path) -> None:
    world, data = crashed_world(tmp_path, dated=True)
    receipts = {key for key in world.bucket.names() if key.endswith("/receipt.json")}
    result = world.run()
    check(world.bucket.names(f"{ROOT}cms/hospital_cost_reports/datasets/") == data, "the retry created no data object")
    check(one_orphan_receipt(world, receipts), "one more receipt, and the earlier one is the orphan")
    check(len(world.records()) == 1 and result["counters"]["units_attempted"] == 1, "one record after the retry")


def test_known_gap_an_undated_unit_leaves_one_extra_data_copy_after_a_crash_that_no_record_names(tmp_path: Path) -> None:
    """Not the design property: an undated receipt keys its data by the capture ID, new per attempt, so a crash leaves one extra copy."""
    world, data = crashed_world(tmp_path, dated=False)
    receipts = {key for key in world.bucket.names() if key.endswith("/receipt.json")}
    world.run()
    extra = world.bucket.names(f"{ROOT}cms/hospital_cost_reports/datasets/") - data
    check(len(extra) == 1, f"at most one extra data copy per crashed unit: {sorted(extra)}")
    (only,) = world.records().values()
    check(
        not extra & {item["key"] for item in record.objects_named(only)} or extra <= {item["key"] for item in only["data_objects"]},
        "the record names the retry's copy",
    )
    check(not data & {item["key"] for item in record.objects_named(only)}, "no record names the crashed attempt's copy")
    check(one_orphan_receipt(world, receipts), "one orphan receipt")


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


def test_a_data_change_in_a_later_run_counts_as_changed_and_a_reference_change_as_unchanged(tmp_path: Path) -> None:
    def archive(reference: bytes, data: bytes) -> bytes:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as handle:
            handle.writestr("release/data.csv", data)
            handle.writestr("release/dictionary.pdf", reference)
        return buffer.getvalue()

    members = [
        {"member_chain": ["release/data.csv"], "role": "data", "dataset_id": "synthetic_table"},
        {"member_chain": ["release/dictionary.pdf"], "role": "reference", "dataset_id": "synthetic_table"},
    ]

    def pdf(number: int) -> bytes:
        return b"%PDF-1.7\nexample documentation " + str(number).encode() + b"\n%%EOF"

    world = World(tmp_path, ["a.zip"], {"a.zip": members})
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
    three.bucket.put_object(
        Bucket=BUCKET,
        ExpectedBucketOwner=ACCOUNT,
        Key=f"{ROOT}stray.json",
        Body=io.BytesIO(b"{}"),
        IfNoneMatch="*",
        ChecksumSHA256="x",
        ServerSideEncryption="AES256",
    )
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
    world.bucket.put_object(
        Bucket=BUCKET, ExpectedBucketOwner=ACCOUNT, Key=stale, Body=io.BytesIO(b"{}"), IfNoneMatch="*", ChecksumSHA256="x", ServerSideEncryption="AES256"
    )
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
