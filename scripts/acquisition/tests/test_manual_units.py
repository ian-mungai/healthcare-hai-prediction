"""Offline E2E for manual units on the host: a plain download of each approved_manual unit, robots.txt, spacing, refusals, stale records and their lists.

Everything is synthetic: a stand-in publisher below the reviewed redirect policy, a create-once S3 stand-in and a clock that never sleeps.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import re
import urllib.parse
import urllib.request
import warnings
import zipfile
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from email.message import Message
from pathlib import Path
from typing import Any
from urllib.response import addinfourl

import pytest

from scripts.acquisition import hud_api_contract as hud_api
from scripts.acquisition import hud_xlsx_contract, manual_content, manual_units, stale_record, transport, wonder_export_contract
from scripts.acquisition.run_hud_xlsx_e2e import rows as hud_rows
from scripts.acquisition.run_hud_xlsx_e2e import workbook as hud_workbook
from scripts.acquisition.run_wonder_export_e2e import FLOOR
from scripts.acquisition.run_wonder_export_e2e import export_text as wonder_export_text
from scripts.acquisition.s3_store import StorageError
from scripts.acquisition.tests.test_collector_hosts import hosts
from tests.support import check

HUD_URL = "https://www.huduser.gov/apps/public/uspscrosswalk/home"
WONDER_URL = "https://wonder.cdc.gov/ucd-icd10-expanded.html"
ACS_URL = "https://data.census.gov/api/access/table/download?download_id=synthetic"
OTHER_URL = "https://data.example.gov/files/book.xlsx"
ROBOTS = {"www.huduser.gov": "https://www.huduser.gov/robots.txt", "wonder.cdc.gov": "https://wonder.cdc.gov/robots.txt"}
START = datetime(2026, 11, 13, 2, 0, 0, tzinfo=UTC)
RUN_1 = "20261113T020000Z-aaaaaaaa"
HUD_NAMES = ["ZIP-COUNTY_032010.xlsx", "ZIP-COUNTY_062010.xlsx", "ZIP-COUNTY_092010.xlsx"]


class Answer(addinfourl):
    """An addinfourl with the message urllib's error processor reads."""

    msg = "Answer"


class Clock:
    """Time that only moves when something sleeps or a test advances it."""

    def __init__(self) -> None:
        self.at = START
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.at

    def monotonic(self) -> float:
        return (self.at - START).total_seconds()

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.at += timedelta(seconds=seconds)

    def advance(self, **delta: float) -> None:
        self.at += timedelta(**delta)


@dataclass
class Seen:
    """One request as the publisher stand-in received it."""

    url: str
    headers: dict[str, str]
    at: float


class TimedBody(io.BytesIO):
    """A response body whose first read takes ``seconds`` on the stand-in clock."""

    def __init__(self, data: bytes, clock: Clock, seconds: float) -> None:
        super().__init__(data)
        self.clock, self.seconds = clock, seconds

    def read(self, size: int | None = -1) -> bytes:
        chunk = super().read(size)
        self.clock.advance(seconds=self.seconds)
        self.seconds = 0
        return chunk


@dataclass
class Reply:
    status: int = 200
    body: bytes = b""
    headers: dict[str, str] = field(default_factory=dict)
    read_seconds: float = 0


class Site(urllib.request.BaseHandler):
    """An HTTPS stand-in answering from a table of replies; the last reply for a URL repeats. Unknown robots.txt paths answer 404."""

    handler_order = 100

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.replies: dict[str, list[Reply]] = {}
        self.seen: list[Seen] = []

    def serve(self, url: str, *replies: Reply) -> None:
        self.replies[url] = list(replies)

    def https_open(self, req: Any) -> Any:
        self.seen.append(Seen(req.full_url, {name.lower(): value for name, value in req.header_items()}, self.clock.monotonic()))
        queue = self.replies.get(req.full_url)
        absent = Reply(404 if req.full_url.endswith("/robots.txt") else 500)
        reply = absent if queue is None else (queue.pop(0) if len(queue) > 1 else queue[0])
        headers = Message()
        headers["Content-Type"] = "application/octet-stream"
        headers["Content-Length"] = str(len(reply.body))
        for name, value in reply.headers.items():
            headers[name] = value
        return Answer(TimedBody(reply.body, self.clock, reply.read_seconds), headers, req.full_url, reply.status)

    def urls(self) -> list[str]:
        return [item.url for item in self.seen]


class Bucket:
    """The S3 calls a manual-unit run makes: create-once puts, versioned heads, prefix lists and gets. Nothing is ever replaced or deleted."""

    def __init__(self) -> None:
        self.versions: dict[str, list[tuple[str, bytes]]] = {}
        self.calls: list[tuple[str, str]] = []
        self.inject: list[str] = []

    def seed(self, key: str, body: bytes) -> dict[str, Any]:
        self.versions.setdefault(key, []).append((f"v{len(self.versions.get(key, [])) + 1}", body))
        version, _ = self.versions[key][-1]
        return {"key": key, "version_id": version, "sha256": hashlib.sha256(body).hexdigest(), "byte_count": len(body)}

    def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
        args = arguments or []
        check(service == "s3api" and operation in {"put-object", "head-object", "list-objects-v2", "get-object"}, f"unexpected call {service} {operation}")
        options = dict(zip(args[::2], args[1::2], strict=False))
        key = options.get("--key", options.get("--prefix", ""))
        self.calls.append((operation, key))
        if operation == "list-objects-v2":
            found = sorted(name for name in self.versions if name.startswith(options["--prefix"]))
            return {"Contents": [{"Key": name} for name in found]} if found else {}
        if operation == "put-object":
            check(options.get("--if-none-match") == "*", "an object was written without the create-once condition")
            if self.inject:
                raise StorageError("put-object", self.inject.pop(0))
            if key in self.versions:
                raise StorageError("put-object", "PreconditionFailed")
            self.seed(key, Path(options["--body"]).read_bytes())
            return {"VersionId": "v1"}
        stored = self.versions.get(key)
        if not stored:
            raise StorageError(operation, "404")
        version, body = next(((v, b) for v, b in stored if v == options.get("--version-id")), stored[-1]) if "--version-id" in options else stored[-1]
        if "--version-id" in options and version != options["--version-id"]:
            raise StorageError(operation, "404")
        if operation == "get-object":
            Path(args[-1]).write_bytes(body)
        return {"VersionId": version, "ContentLength": len(body), "ChecksumSHA256": base64.b64encode(hashlib.sha256(body).digest()).decode()}

    def names(self, prefix: str = "") -> list[str]:
        return sorted(name for name in self.versions if name.startswith(prefix))

    def writes(self) -> list[str]:
        return [key for operation, key in self.calls if operation == "put-object"]


class Sink:
    """The normal capture path as the run loop supplies it: stores the data objects and writes the completion record."""

    def __init__(self, bucket: Bucket, root: str, unit_key: Any) -> None:
        self.bucket, self.root, self.unit_key = bucket, root, unit_key
        self.captured: list[tuple[str, dict[str, bytes]]] = []

    def __call__(self, unit: dict, spec: dict, files: dict[str, Path]) -> dict:
        bodies = {name: path.read_bytes() for name, path in sorted(files.items())}
        self.captured.append((unit["snapshot_id"], bodies))
        payload = hashlib.sha256(json.dumps({n: hashlib.sha256(b).hexdigest() for n, b in bodies.items()}, sort_keys=True).encode()).hexdigest()
        for name, body in bodies.items():
            self.bucket.seed(f"{self.root}bronze_objects/{unit['snapshot_id']}/{name}", body)
        key = f"{self.root}ingestion_units/{self.unit_key(unit)}/{payload}.json"
        self.bucket.seed(key, b"{}")
        return {"completion_key": key, "payload_sha256": payload}


def unit_key_of(unit: dict) -> str:
    """The request identity rule of the capture path: SHA-256 of canonical JSON."""
    identity = {"mode": unit["mode"], "snapshot_id": unit["snapshot_id"], "url": unit["url"]}
    return hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def make_unit(source: str, mode: str, url: str, snapshot: str) -> dict:
    return {"source_id": source, "mode": mode, "url": url, "snapshot_id": snapshot, "disposition": "approved_manual", "route": "manual_browser_download"}


def hud_unit(index: int = 0) -> dict:
    return make_unit("HUD", "hud_xlsx", HUD_URL, f"HUD__{index}")


def hud_batch_workbook() -> bytes:
    return hud_workbook(hud_rows(2010, 1))


@dataclass
class World:
    clock: Clock
    site: Site
    bucket: Bucket
    sink: Sink
    root: str
    scratch: Path
    plans: manual_content.Scopes
    counter: list[int]
    specs: dict[str, dict] = field(default_factory=dict)

    def context(self, run_id: str = RUN_1, **changes: Any) -> manual_units.Context:
        settings: dict[str, Any] = {
            "client": self.bucket,
            "root": self.root,
            "run_id": run_id,
            "code_sha256": "c" * 64,
            "bundle_sha256": "b" * 64,
            "clock": self.clock,
            "scratch": self.scratch,
            "capture": self.sink,
            "unit_key": unit_key_of,
            "collection": lambda unit: "example_publisher/example_collection",
            "handlers": (self.site,),
            "scopes": self.plans,
            "on_request": lambda: self.counter.append(1),
        }
        return manual_units.Context(**(settings | changes))

    def add(self, unit: dict, spec: dict) -> dict:
        self.specs[unit["snapshot_id"]] = spec
        return unit

    def last_capture(self, unit: dict, *, present: bool = True, name: str = "data.bin") -> dict:
        """An earlier capture of the unit, seeded in the bucket (or only claimed, when not present)."""
        key = f"raw/example_publisher/example_collection/datasets/d/{unit['snapshot_id']}/{name}"
        body = f"old capture of {unit['snapshot_id']}".encode()
        pointer = (
            self.bucket.seed(key, body) if present else {"key": key, "version_id": "v1", "sha256": hashlib.sha256(body).hexdigest(), "byte_count": len(body)}
        )
        return {"read_root": "raw/", "artifacts": [{"role": "data", **pointer, "retrieved_at_utc": "2026-09-24T01:47:28Z"}]}

    def run(self, units: list[dict], run_id: str = RUN_1, **changes: Any) -> manual_units.Report:
        return manual_units.run(units, self.specs, self.context(run_id, **changes))

    def stale_records(self, unit: dict) -> list[dict]:
        prefix = stale_record.stale_prefix(self.root, unit_key_of(unit))
        return [json.loads(self.bucket.versions[key][-1][1]) for key in self.bucket.names(prefix)]


@pytest.fixture
def world(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> World:
    hosts(monkeypatch, tmp_path, ["www.huduser.gov", "wonder.cdc.gov", "data.census.gov", "download.bls.gov", "data.example.gov"])
    monkeypatch.setattr(hud_xlsx_contract, "DECISION_PATH", tmp_path / "no_repeat_decision.json")
    clock = Clock()
    site = Site(clock)
    bucket = Bucket()
    root = "ingestion_test/t1/"
    batch = {"id": "synthetic", "year": 2010, "quarter": 1, "file_name": HUD_NAMES[0], "county_geography": "2000_census"}
    book = hud_batch_workbook()
    batch |= {"sha256": hashlib.sha256(book).hexdigest(), "bytes": len(book)}
    hud_plan = {"header": hud_xlsx_contract.HEADER, "required_state_fips": hud_api.STATE_FIPS, "batches": [batch]}
    export = wonder_export_text("D158", 2024, 2024)
    wonder_plan = {
        "header": wonder_export_contract.HEADER,
        "fixed_parameters": wonder_export_contract.FIXED_PARAMETERS,
        "min_counties_per_year": FLOOR,
        "required_state_fips": hud_api.STATE_FIPS,
        "batches": [{"database": "D158", "years": [2024, 2024], "sha256": hashlib.sha256(export).hexdigest(), "bytes": len(export)}],
    }
    monkeypatch.setattr(transport.time, "sleep", clock.sleep)
    return World(clock, site, bucket, Sink(bucket, root, unit_key_of), root, tmp_path / "scratch", manual_content.Scopes(hud_plan, wonder_plan), [])


def serve_workbook(world: World, body: bytes | None = None) -> None:
    world.site.serve(HUD_URL, Reply(200, hud_batch_workbook() if body is None else body))


def hud_spec(world: World, unit: dict, name: str = HUD_NAMES[0], *, present: bool = True) -> dict:
    return {"mode": "hud_xlsx", "stored_data_files": sorted([name, "crosswalk.csv"]), "last_capture": world.last_capture(unit, present=present)}


def hud_units(world: World, count: int, *, present: bool = True) -> list[dict]:
    units = [world.add(hud_unit(index), {}) for index in range(count)]
    for unit, name in zip(units, HUD_NAMES, strict=False):
        world.specs[unit["snapshot_id"]] = hud_spec(world, unit, name, present=present)
    return units


def read_all(world: World) -> list[dict]:
    return [json.loads(world.bucket.versions[key][-1][1]) for key in world.bucket.names(f"{world.root}ingestion_stale/")]


def only_stale(world: World, unit: dict) -> dict:
    records = world.stale_records(unit)
    check(len(records) == 1, f"expected one stale record, found {len(records)}")
    return stale_record.parse(json.dumps(records[0]).encode())


def data_objects(world: World) -> list[str]:
    return world.bucket.names(f"{world.root}bronze_objects/") + world.bucket.names(f"{world.root}ingestion_units/")


def test_a_page_served_in_place_of_a_workbook_is_stale_not_expected_format_and_nothing_is_stored_as_data(world: World) -> None:
    [unit] = hud_units(world, 1)
    world.site.serve(HUD_URL, Reply(200, b"<!doctype html><html><body>Crosswalk app</body></html>"))
    report = world.run([unit])
    record = only_stale(world, unit)
    check(record["reason"] == "not_expected_format" and record["http_status"] == 200, f"reason {record['reason']}")
    check(record["last_capture_verified"] is True and len(record["last_capture"]) == 1, "the last capture was verified by HEAD")
    check(data_objects(world) == [] and world.sink.captured == [], "nothing was stored as data and no completion record was written")
    check(report.counts["stale"] == 1 and report.counts["captured"] == 0, f"counts {report.counts}")


def test_a_robots_disallow_sends_no_request_to_the_path(world: World) -> None:
    [unit] = hud_units(world, 1)
    world.site.serve(ROBOTS["www.huduser.gov"], Reply(200, b"User-agent: *\nDisallow: /apps/\n"))
    serve_workbook(world)
    world.run([unit])
    check(world.site.urls() == [ROBOTS["www.huduser.gov"]], f"only robots.txt was requested: {world.site.urls()}")
    check(only_stale(world, unit)["reason"] == "robots_disallowed", "the unit is stale as robots_disallowed")
    check(world.sink.captured == [], "nothing was captured")


def test_the_agent_group_of_robots_txt_wins_over_the_star_group(world: World) -> None:
    [unit] = hud_units(world, 1)
    world.site.serve(ROBOTS["www.huduser.gov"], Reply(200, b"User-agent: HistoricalSourceCapture\nAllow: /\n\nUser-agent: *\nDisallow: /\n"))
    serve_workbook(world)
    world.run([unit])
    check(len(world.sink.captured) == 1 and read_all(world) == [], "the agent's own group allowed the unit")


@pytest.mark.parametrize("reply", [Reply(401), Reply(403), Reply(503), Reply(200, b"<html>not robots</html>")], ids=["401", "403", "503", "html"])
def test_an_unreadable_robots_txt_sends_nothing_to_the_host(world: World, reply: Reply) -> None:
    units = hud_units(world, 2)
    world.site.serve(ROBOTS["www.huduser.gov"], reply)
    serve_workbook(world)
    world.run(units)
    check(world.site.urls() == [ROBOTS["www.huduser.gov"]], f"one robots request, nothing else: {world.site.urls()}")
    check([only_stale(world, unit)["reason"] for unit in units] == ["robots_unreadable"] * 2, "both units are stale as robots_unreadable")


def test_a_missing_robots_txt_allows_everything_and_is_read_once_per_host(world: World) -> None:
    units = hud_units(world, 2)
    serve_workbook(world)
    world.run(units)
    check(world.site.urls().count(ROBOTS["www.huduser.gov"]) == 1, "robots.txt was read once for the host")
    check(len(world.sink.captured) == 1, "the unit that matches the plan was captured")


def test_two_refusals_stop_the_host_and_the_rest_are_stale_host_refused(world: World) -> None:
    units = hud_units(world, 3)
    world.site.serve(HUD_URL, Reply(403))
    world.run(units)
    reasons = [only_stale(world, unit)["reason"] for unit in units]
    check(reasons == ["http_status", "http_status", "host_refused"], f"reasons {reasons}")
    check(world.site.urls() == [ROBOTS["www.huduser.gov"], HUD_URL, HUD_URL], f"the third unit sent nothing: {world.site.urls()}")
    check(only_stale(world, units[0])["http_status"] == 403 and only_stale(world, units[2])["http_status"] is None, "status recorded for the refused units")


def test_a_retry_after_over_thirty_seconds_ends_the_unit_as_rate_limited(world: World) -> None:
    [unit] = hud_units(world, 1)
    world.site.serve(HUD_URL, Reply(429, b"", {"Retry-After": "120"}))
    world.run([unit])
    record = only_stale(world, unit)
    check(record["reason"] == "rate_limited" and record["http_status"] == 429, f"reason {record['reason']}")
    check(world.site.urls().count(HUD_URL) == 1, "no retry followed the long Retry-After")
    check(120 not in world.clock.sleeps, "the run did not wait for it")


def test_retries_are_bounded_by_the_existing_limits(world: World) -> None:
    [unit] = hud_units(world, 1)
    world.site.serve(HUD_URL, Reply(503))
    world.run([unit])
    check(world.site.urls().count(HUD_URL) == 3, "three attempts, the existing bound")
    record = only_stale(world, unit)
    check(record["reason"] == "http_status" and record["http_status"] == 503, "stale as http_status 503")


def test_a_short_retry_after_is_honoured_and_a_later_success_is_captured(world: World) -> None:
    [unit] = hud_units(world, 1)
    world.site.serve(HUD_URL, Reply(503, b"", {"Retry-After": "7"}), Reply(200, hud_batch_workbook()))
    world.run([unit])
    check(len(world.sink.captured) == 1 and read_all(world) == [], "the second attempt was captured")
    check(7 in world.clock.sleeps, "the Retry-After wait was taken")


def test_a_served_file_that_passes_the_contract_is_captured_with_a_completion_record_and_no_stale_record(world: World) -> None:
    [unit] = hud_units(world, 1)
    serve_workbook(world)
    report = world.run([unit])
    check(len(world.sink.captured) == 1 and read_all(world) == [], "captured, no stale record")
    names = sorted(world.sink.captured[0][1])
    check(names == sorted([HUD_NAMES[0], "crosswalk.csv"]), f"stored names {names}")
    check(world.bucket.names(f"{world.root}ingestion_units/"), "a completion record exists")
    check(report.counts["captured"] == 1 and report.stale_units == [], f"report {report.counts}")
    check(not world.scratch.exists() or list(world.scratch.iterdir()) == [], "the unit's working files were removed")


def test_a_workbook_with_other_bytes_than_the_plan_binds_fails_the_contract(world: World) -> None:
    [unit] = hud_units(world, 1)
    serve_workbook(world, hud_workbook([*hud_rows(2010, 1), ["99998", "01001", "1", "0", "0", "1"]]))
    world.run([unit])
    check(only_stale(world, unit)["reason"] == "contract_failed", "the changed bytes stop the unit")
    check(data_objects(world) == [], "nothing stored")


def test_a_wonder_export_that_passes_the_contract_is_captured_and_a_form_page_is_not(world: World) -> None:
    name = "wonder_D158_2024_2024.txt"
    unit = world.add(make_unit("WONDER", "wonder_export", WONDER_URL, "WONDER__0"), {})
    world.specs[unit["snapshot_id"]] = {
        "mode": "wonder_export",
        "stored_data_files": sorted([name, "county_year.csv"]),
        "last_capture": world.last_capture(unit),
    }
    world.site.serve(WONDER_URL, Reply(200, b"<html><form>query form</form></html>"))
    world.run([unit])
    check(only_stale(world, unit)["reason"] == "not_expected_format", "the query form page is not an export")
    world.clock.advance(hours=21)
    world.site.serve(WONDER_URL, Reply(200, wonder_export_text("D158", 2024, 2024)))
    world.run([unit], "20261113T230000Z-bbbbbbbb")
    check(len(world.sink.captured) == 1 and sorted(world.sink.captured[0][1]) == sorted([name, "county_year.csv"]), "the export was captured")


def acs_zip(members: list[tuple[str, bytes]]) -> bytes:
    buffer = io.BytesIO()
    with warnings.catch_warnings(), zipfile.ZipFile(buffer, "w") as archive:
        warnings.simplefilter("ignore")
        for name, body in members:
            archive.writestr(name, body)
    return buffer.getvalue()


def acs_unit(world: World) -> dict:
    unit = world.add(make_unit("ACS", "file", ACS_URL, "ACS__0"), {})
    members = [
        {"member_chain": ["S1701-Data.csv"], "role": "data", "dataset_id": "acs_s1701"},
        {"member_chain": ["S1701-Notes.txt"], "role": "reference", "dataset_id": "acs_s1701"},
    ]
    world.specs[unit["snapshot_id"]] = {
        "mode": "file",
        "expected_format": "zip",
        "stored_data_files": ["ACS_history_x.zip"],
        "archive_members": members,
        "last_capture": world.last_capture(unit),
    }
    return unit


def corrupted_member_zip() -> bytes:
    body = bytearray(acs_zip([("S1701-Data.csv", b"a,b\n1,2\n"), ("S1701-Notes.txt", b"notes\n")]))
    body[body.index(b"a,b\n1,2\n")] ^= 0xFF
    return bytes(body)


ACS_ARCHIVES = {
    "listed_members": acs_zip([("S1701-Data.csv", b"a,b\n1,2\n"), ("S1701-Notes.txt", b"notes\n")]),
    "duplicate_member": acs_zip([("S1701-Data.csv", b"a,b\n1,2\n"), ("S1701-Data.csv", b"a,b\n9,9\n"), ("S1701-Notes.txt", b"notes\n")]),
    "corrupt_member": corrupted_member_zip(),
}


@pytest.mark.parametrize("archive", sorted(ACS_ARCHIVES))
def test_an_acs_unit_is_not_a_manual_unit_whatever_its_archive_holds_and_stops_the_run_before_any_request(world: World, archive: str) -> None:
    [hud] = hud_units(world, 1)
    unit = acs_unit(world)
    serve_workbook(world)
    world.site.serve(ACS_URL, Reply(200, ACS_ARCHIVES[archive]))
    with pytest.raises(manual_units.ManualUnitError, match="not a manual unit"):
        world.run([hud, unit])
    check(world.site.seen == [] and world.bucket.writes() == [] and world.sink.captured == [], "no request, no write, nothing captured")


def test_a_second_run_of_the_same_failing_unit_adds_one_stale_record_and_no_data_object(world: World) -> None:
    [unit] = hud_units(world, 1)
    world.site.serve(HUD_URL, Reply(200, b"<html>page</html>"))
    world.run([unit])
    before = list(world.bucket.names())
    world.clock.advance(hours=2)
    sent = len(world.site.seen)
    repeat = world.run([unit], "20261113T040000Z-bbbbbbbb")
    check(len(world.site.seen) == sent and repeat.counts["skipped_recent_stale"] == 1, "a repeat run within 20 hours sends nothing")
    check(list(world.bucket.names()) == before, "and creates nothing")
    world.clock.advance(hours=19)
    world.run([unit], "20261113T230000Z-cccccccc")
    added = sorted(set(world.bucket.names()) - set(before))
    check(len(added) == 1 and "/ingestion_stale/" in added[0] and added[0].endswith("20261113T230000Z-cccccccc.json"), f"one new stale record: {added}")
    check(data_objects(world) == [], "no data object")


def test_a_failed_pointer_head_lists_the_unit_as_missing(world: World) -> None:
    [present] = hud_units(world, 1)
    absent = world.add(hud_unit(1), {})
    world.specs[absent["snapshot_id"]] = hud_spec(world, absent, HUD_NAMES[1], present=False)
    world.site.serve(HUD_URL, Reply(200, b"<html>page</html>"))
    report = world.run([present, absent])
    check(only_stale(world, present)["last_capture_verified"] is True, "the present pointer verified")
    check(only_stale(world, absent)["last_capture_verified"] is False, "the absent pointer did not")
    check([item["snapshot_id"] for item in report.missing_units] == [absent["snapshot_id"]], f"missing {report.missing_units}")
    check([item["snapshot_id"] for item in report.stale_units] == [present["snapshot_id"]], f"stale {report.stale_units}")


def test_a_pointer_whose_version_or_digest_differs_is_not_verified(world: World) -> None:
    [unit] = hud_units(world, 1)
    pointer = world.specs[unit["snapshot_id"]]["last_capture"]["artifacts"][0]
    pointer["sha256"] = "0" * 64
    world.site.serve(HUD_URL, Reply(200, b"<html>page</html>"))
    report = world.run([unit])
    check(only_stale(world, unit)["last_capture_verified"] is False and len(report.missing_units) == 1, "a wrong digest is missing")


def test_a_unit_with_no_recorded_last_capture_is_missing(world: World) -> None:
    [unit] = hud_units(world, 1)
    del world.specs[unit["snapshot_id"]]["last_capture"]
    world.site.serve(HUD_URL, Reply(200, b"<html>page</html>"))
    report = world.run([unit])
    record = only_stale(world, unit)
    check(record["last_capture"] == [] and record["last_capture_verified"] is False and len(report.missing_units) == 1, "no pointer means missing")


def other_unit(world: World) -> dict:
    unit = world.add(make_unit("OTHER", "hud_xlsx", OTHER_URL, "OTHER__0"), {})
    world.specs[unit["snapshot_id"]] = hud_spec(world, unit)
    return unit


def test_the_robots_result_of_a_non_manual_host_is_recorded_and_its_units_are_still_attempted(world: World) -> None:
    unit = other_unit(world)
    world.site.serve("https://data.example.gov/robots.txt", Reply(200, b"User-agent: *\nDisallow: /\n"))
    world.site.serve(OTHER_URL, Reply(200, hud_batch_workbook()))
    report = world.run([unit])
    result = report.robots["data.example.gov"]
    check(result["enforced"] is False and result["outcome"] == "read" and result["would_disallow"] == 1, f"robots report {result}")
    check(len(world.sink.captured) == 1 and read_all(world) == [], "the unit was still attempted and captured")
    check(OTHER_URL in world.site.urls(), "the publisher was asked for the file")


def test_the_manual_hosts_are_enforced_and_the_report_says_so(world: World) -> None:
    [unit] = hud_units(world, 1)
    world.site.serve(ROBOTS["www.huduser.gov"], Reply(200, b"User-agent: *\nDisallow: /apps/\n"))
    report = world.run([unit])
    result = report.robots["www.huduser.gov"]
    check(result["enforced"] is True and result["outcome"] == "read" and result["would_disallow"] == 1, f"robots report {result}")
    for host in ("huduser.gov", "data.census.gov", "wonder.cdc.gov", "download.bls.gov"):
        check(manual_units.is_manual_host(host), f"{host} is a manual host")
    check(not manual_units.is_manual_host("evilhuduser.gov") and not manual_units.is_manual_host("data.example.gov"), "look-alike and other hosts are not")


def test_no_request_header_carries_an_email_address_or_a_person_name(world: World) -> None:
    units = hud_units(world, 2)
    world.site.serve(HUD_URL, Reply(200, b"<html>page</html>"))
    world.run(units)
    check(len(world.site.seen) >= 3, "robots.txt and both units were requested")
    for item in world.site.seen:
        check(set(item.headers) <= {"accept-encoding", "user-agent", "host"}, f"unexpected headers {sorted(item.headers)}")
        check(item.headers["user-agent"] == "HistoricalSourceCapture/1.0", f"agent {item.headers['user-agent']}")
        check(not re.search(r"\S+@\S+", " ".join(item.headers.values())), "no email address in any header value")
        check(item.headers["accept-encoding"] == "identity", "plain bytes")


def test_requests_to_one_host_are_five_seconds_apart(world: World) -> None:
    units = hud_units(world, 3)
    world.site.serve(HUD_URL, Reply(200, b"<html>page</html>"))
    world.run(units)
    times = [item.at for item in world.site.seen]
    check(len(times) == 4, f"robots and three units: {world.site.urls()}")
    check(
        all(later - earlier >= 5 for earlier, later in zip(times, times[1:], strict=False)), f"gaps {[b - a for a, b in zip(times, times[1:], strict=False)]}"
    )
    check(world.counter and len(world.counter) == 4, "the request counter saw every request")


def test_the_run_record_and_notice_have_stale_and_missing_sections(world: World) -> None:
    [present] = hud_units(world, 1)
    absent = world.add(hud_unit(1), {})
    world.specs[absent["snapshot_id"]] = hud_spec(world, absent, HUD_NAMES[1], present=False)
    world.site.serve(HUD_URL, Reply(200, b"<html>page</html>"))
    report = world.run([present, absent])
    record = report.run_record()
    check([item["snapshot_id"] for item in record["stale_units"]] == [present["snapshot_id"]], f"stale_units {record['stale_units']}")
    check(record["stale_units"][0]["reason"] == "not_expected_format" and record["stale_units"][0]["last_retrieved_at_utc"] == "2026-09-24T01:47:28Z", "fields")
    check([item["snapshot_id"] for item in record["missing_units"]] == [absent["snapshot_id"]], "missing_units")
    text = manual_units.notice_text(report.stale_units, report.missing_units)
    check("Stale: kept the last capture" in text and "HUD: 1 unit, oldest capture 2026-09-24" in text, f"notice text:\n{text}")
    check("Missing" in text and "HUD: 1 unit" in text.split("Missing", 1)[1], f"notice text:\n{text}")
    check(manual_units.notice_text([], []) == "", "nothing to say when nothing is stale")


def test_a_stale_record_is_exactly_the_agreed_schema_and_never_replaced(world: World) -> None:
    [unit] = hud_units(world, 1)
    world.site.serve(HUD_URL, Reply(403))
    world.run([unit])
    [raw] = world.stale_records(unit)
    fields = {"kind", "schema_version", "unit_key", "run_id", "collection", "source_id", "snapshot_id", "attempted_at_utc", "reason", "http_status"}
    fields |= {"code_sha256", "bundle_sha256", "last_capture_verified", "last_capture"}
    check(set(raw) == fields, f"fields {sorted(set(raw) ^ fields)}")
    check(raw["kind"] == "ingestion_unit_stale" and raw["schema_version"] == 1 and raw["run_id"] == RUN_1, "identity fields")
    check(raw["attempted_at_utc"] == "2026-11-13T02:00:05Z", f"robots at 0 s, the unit 5 s later: {raw['attempted_at_utc']}")
    artifact = raw["last_capture"][0]
    check(
        set(artifact) == {"key", "version_id", "sha256", "byte_count", "role", "stored_file_name", "retrieved_at_utc", "read_root"},
        f"artifact {sorted(artifact)}",
    )
    check(artifact["key"].startswith("raw/") and artifact["read_root"] == "raw/" and artifact["stored_file_name"] == "data.bin", f"artifact {artifact}")
    key = stale_record.stale_key(world.root, unit_key_of(unit), RUN_1)
    check(key in world.bucket.names() and world.bucket.writes().count(key) == 1, "written once")
    for bad in (
        b"{}",
        json.dumps({**raw, "reason": "because"}).encode(),
        json.dumps({**raw, "extra": 1}).encode(),
        json.dumps({**raw, "http_status": "403"}).encode(),
    ):
        with pytest.raises(stale_record.StaleRecordError):
            stale_record.parse(bad)


def test_a_mode_without_a_content_contract_is_refused_before_any_request(world: World) -> None:
    unit = world.add(make_unit("BLS", "reference_download", "https://download.bls.gov/pub/time.series/la/la.txt", "BLS__0"), {})
    world.specs[unit["snapshot_id"]] = {"mode": "reference_download", "stored_data_files": []}
    with pytest.raises(manual_units.ManualUnitError, match="reference_download"):
        world.run([unit])
    check(world.site.seen == [] and world.bucket.writes() == [], "no request, no write")


def test_a_unit_with_an_unapproved_host_is_refused_before_any_request(world: World) -> None:
    unit = world.add(make_unit("X", "hud_xlsx", "https://unapproved.example.org/a.xlsx", "X__0"), {})
    world.specs[unit["snapshot_id"]] = hud_spec(world, unit)
    world.run([unit])
    check(world.site.seen == [], "no request left the run")
    check(only_stale(world, unit)["reason"] == "network", "the refusal is recorded as stale")


def test_stale_records_sort_by_run_id_and_the_newest_decides_the_retry_clock(world: World) -> None:
    check(stale_record.stale_key("r/", "a" * 64, RUN_1) == f"r/ingestion_stale/{'a' * 64}/{RUN_1}.json", "key layout")
    check(stale_record.stale_prefix("r/", "a" * 64) == f"r/ingestion_stale/{'a' * 64}/", "prefix layout")
    with pytest.raises(stale_record.StaleRecordError):
        stale_record.stale_key("r/", "short", RUN_1)
    with pytest.raises(stale_record.StaleRecordError):
        stale_record.stale_key("r/", "a" * 64, "../x")


@pytest.mark.parametrize(
    ("value", "too_long"),
    [(None, False), ("7", False), ("30", False), ("31", True), ("120", True), ("Fri, 13 Nov 2026 03:00:00 GMT", True), ("", False)],
    ids=["none", "seven", "thirty", "thirty_one", "long", "http_date", "empty"],
)
def test_the_observer_reads_retry_after_the_way_the_transport_does_and_counts_each_request(world: World, value: str | None, too_long: bool) -> None:
    observer = manual_units.Observer(world.context())
    headers = Message()
    if value is not None:
        headers["Retry-After"] = value
    request = urllib.request.Request(HUD_URL)
    check(observer.https_request(request) is request and world.counter == [1], "the request is passed on unchanged and counted once")
    answer = Answer(io.BytesIO(b""), headers, HUD_URL, 429)
    check(observer.https_response(request, answer) is answer, "the answer is passed on unchanged")
    check(observer.retry_after_too_long(transport.Limits().max_retry_delay) is too_long, f"Retry-After {value!r} against the 30 s limit")


def test_every_manual_unit_of_the_tracked_bundle_is_runnable_and_bound_to_a_locked_plan_or_refused_before_any_request() -> None:
    folder = Path(__file__).resolve().parents[3] / "config/acquisition/baseline_run"
    queue = json.loads((folder / "queue.json").read_text())["queue"]
    specs = json.loads((folder / "capture_specs.json").read_text())["specs"]
    scopes = manual_content.Scopes()
    manual = [unit for unit in queue if unit["disposition"] == "approved_manual"]
    units = [unit for unit in manual if unit["mode"] in {"hud_xlsx", "wonder_export"}]
    check(Counter(unit["source_id"] for unit in units) == {"HUD": 44, "WONDER": 6}, "the manual set is HUD 44 and WONDER 6")
    hud_files = {batch["file_name"] for batch in scopes.plan("hud_xlsx")["batches"]}
    wonder_files = {wonder_export_contract.stored_name(batch) for batch in scopes.plan("wonder_export")["batches"]}
    for unit in units:
        mode, name = manual_content.plan_unit(unit, specs[unit["snapshot_id"]])
        check(mode != "hud_xlsx" or name in hud_files, f"{name} is a planned HUD workbook")
        check(mode != "wonder_export" or name in wonder_files, f"{name} is a planned WONDER export")
        check(manual_units.is_manual_host(urllib.parse.urlsplit(unit["url"]).hostname or ""), f"{unit['url']} is on a manual host")
    refused: Counter[tuple[str, str, bool]] = Counter()
    for unit in (unit for unit in manual if unit not in units):
        with pytest.raises(manual_units.ManualUnitError, match="not a manual unit|no content contract") as error:
            manual_content.plan_unit(unit, specs.get(unit["snapshot_id"]))
        refused[(unit["source_id"], unit["mode"], "not a manual unit" in str(error.value))] += 1
    check(
        refused
        == {
            ("ACS", "file", True): 16,
            ("HUD", "reference_download", False): 2,
            ("WONDER", "reference_download", False): 2,
            ("BLS", "reference_download", False): 1,
        },
        f"refused {refused}",
    )


def redirect(url: str) -> Reply:
    return Reply(302, b"", {"Location": url})


def gaps(world: World) -> list[float]:
    times = [item.at for item in world.site.seen]
    return [later - earlier for earlier, later in zip(times, times[1:], strict=False)]


def test_a_redirect_to_a_path_that_robots_txt_disallows_is_never_requested(world: World) -> None:
    [unit] = hud_units(world, 1)
    blocked = "https://www.huduser.gov/blocked/book.xlsx"
    world.site.serve(ROBOTS["www.huduser.gov"], Reply(200, b"User-agent: *\nDisallow: /blocked/\n"))
    world.site.serve(HUD_URL, redirect(blocked))
    world.site.serve(blocked, Reply(200, hud_batch_workbook()))
    report = world.run([unit])
    check(world.site.urls() == [ROBOTS["www.huduser.gov"], HUD_URL], f"the disallowed destination was not requested: {world.site.urls()}")
    record = only_stale(world, unit)
    check(record["reason"] == "robots_disallowed" and record["http_status"] is None, f"reason {record['reason']}")
    check(
        world.sink.captured == [] and report.counts["captured"] == 0 and report.http_requests == 2, f"counts {report.counts}, requests {report.http_requests}"
    )


def test_a_redirect_to_an_allowed_path_is_followed_captured_and_five_seconds_after_the_first_request(world: World) -> None:
    [unit] = hud_units(world, 1)
    allowed = "https://www.huduser.gov/apps/public/uspscrosswalk/book.xlsx"
    world.site.serve(ROBOTS["www.huduser.gov"], Reply(200, b"User-agent: *\nDisallow: /blocked/\n"))
    world.site.serve(HUD_URL, redirect(allowed))
    world.site.serve(allowed, Reply(200, hud_batch_workbook()))
    world.run([unit])
    check(world.site.urls() == [ROBOTS["www.huduser.gov"], HUD_URL, allowed], f"requests {world.site.urls()}")
    check(len(world.sink.captured) == 1 and read_all(world) == [], "the redirected file was captured")
    check(min(gaps(world)) >= 5, f"gaps {gaps(world)}")


def test_a_redirect_on_a_report_only_host_is_followed_and_counted_in_the_robots_report(world: World) -> None:
    unit = other_unit(world)
    blocked = "https://data.example.gov/blocked/book.xlsx"
    world.site.serve("https://data.example.gov/robots.txt", Reply(200, b"User-agent: *\nDisallow: /blocked/\n"))
    world.site.serve(OTHER_URL, redirect(blocked))
    world.site.serve(blocked, Reply(200, hud_batch_workbook()))
    report = world.run([unit])
    check(blocked in world.site.urls() and len(world.sink.captured) == 1, "the report-only host's redirect was followed")
    check(report.robots["data.example.gov"]["would_disallow"] == 1, f"robots report {report.robots['data.example.gov']}")


def test_retries_are_five_seconds_apart(world: World) -> None:
    [unit] = hud_units(world, 1)
    world.site.serve(HUD_URL, Reply(503), Reply(503), Reply(200, hud_batch_workbook()))
    world.run([unit])
    check(world.site.urls() == [ROBOTS["www.huduser.gov"], HUD_URL, HUD_URL, HUD_URL], f"requests {world.site.urls()}")
    check(min(gaps(world)) >= 5 and len(world.sink.captured) == 1, f"gaps {gaps(world)}")


def test_the_spacing_runs_from_the_end_of_the_last_download(world: World) -> None:
    units = hud_units(world, 2)
    world.site.serve(HUD_URL, Reply(200, b"<html>page</html>", read_seconds=8))
    world.run(units)
    check(
        [item.at for item in world.site.seen] == [0, 5, 18], f"robots, then a unit that took 8 s to read, then the next unit 5 s after it ended: {gaps(world)}"
    )


def test_a_repeat_run_inside_the_retry_interval_keeps_the_stale_and_missing_lists(world: World) -> None:
    [present] = hud_units(world, 1)
    absent = world.add(hud_unit(1), {})
    world.specs[absent["snapshot_id"]] = hud_spec(world, absent, HUD_NAMES[1], present=False)
    world.site.serve(HUD_URL, Reply(200, b"<html>page</html>"))
    first = world.run([present, absent])
    before, sent = list(world.bucket.names()), len(world.site.seen)
    world.clock.advance(hours=2)
    repeat = world.run([present, absent], "20261113T040000Z-bbbbbbbb")
    check(len(world.site.seen) == sent and list(world.bucket.names()) == before, "the repeat sent nothing and wrote nothing")
    check(repeat.counts["skipped_recent_stale"] == 2 and repeat.counts["attempted"] == 0, f"counts {repeat.counts}")
    check(repeat.stale_units == first.stale_units != [] and repeat.missing_units == first.missing_units != [], f"lists {repeat.run_record()}")
    check(repeat.counts["missing"] == 1, f"counts {repeat.counts}")
    check(
        manual_units.notice_text(repeat.stale_units, repeat.missing_units) == manual_units.notice_text(first.stale_units, first.missing_units) != "", "notice"
    )


@pytest.mark.parametrize("code", ["ConditionalRequestConflict", "409", "412", "PreconditionFailed"])
def test_a_conflict_with_no_stored_record_is_a_visible_failure_not_a_stale_unit(world: World, code: str) -> None:
    [unit] = hud_units(world, 1)
    world.site.serve(HUD_URL, Reply(403))
    world.bucket.inject.append(code)
    with pytest.raises(manual_units.ManualUnitError, match="stored stale record"):
        world.run([unit])
    check(world.stale_records(unit) == [], "no stale record exists")


OLD_ATTEMPT = "2026-11-12T00:00:00Z"
RECENT_ATTEMPT = "2026-11-13T01:00:00Z"


def seed_stale_record(world: World, unit: dict, **changes: Any) -> dict:
    """A stale record of the unit that an earlier attempt left behind, by default of this run and older than the retry interval."""
    fields: dict[str, Any] = {
        "unit_key": unit_key_of(unit),
        "run_id": RUN_1,
        "collection": "example_publisher/example_collection",
        "source_id": unit["source_id"],
        "snapshot_id": unit["snapshot_id"],
        "attempted_at_utc": OLD_ATTEMPT,
        "reason": "network",
        "http_status": None,
        "code_sha256": "c" * 64,
        "bundle_sha256": "b" * 64,
        "last_capture_verified": True,
        "last_capture": manual_units.pointers(world.specs[unit["snapshot_id"]]),
    }
    record = stale_record.build(**(fields | changes))
    world.bucket.seed(stale_record.stale_key(world.root, unit_key_of(unit), record["run_id"]), stale_record.encode(record))
    return record


def test_a_conflict_is_settled_by_the_record_that_is_stored(world: World) -> None:
    [unit] = hud_units(world, 1)
    stored = seed_stale_record(world, unit)
    world.site.serve(HUD_URL, Reply(403))
    report = world.run([unit])
    check(report.stale_units == [manual_units.summary(stored)] and report.outcomes[0].reason == "network", f"report {report.run_record()}")
    check(
        world.stale_records(unit) == [stored] and world.bucket.writes().count(stale_record.stale_key(world.root, unit_key_of(unit), RUN_1)) == 1,
        "stored record kept",
    )


IDENTITY_CASES = {
    "old_snapshot": ({"snapshot_id": "HUD__other"}, OLD_ATTEMPT, "newest stale record"),
    "old_source": ({"source_id": "WONDER"}, OLD_ATTEMPT, "newest stale record"),
    "old_collection": ({"collection": "other/wrong"}, OLD_ATTEMPT, "newest stale record"),
    "old_code": ({"code_sha256": "d" * 64}, OLD_ATTEMPT, "stored stale record"),
    "recent_snapshot": ({"snapshot_id": "HUD__other"}, RECENT_ATTEMPT, "newest stale record"),
    "recent_source": ({"source_id": "WONDER"}, RECENT_ATTEMPT, "newest stale record"),
    "recent_collection": ({"collection": "other/wrong"}, RECENT_ATTEMPT, "newest stale record"),
}


@pytest.mark.parametrize(("changes", "attempted", "refusal"), IDENTITY_CASES.values(), ids=IDENTITY_CASES)
def test_a_stored_record_of_another_identity_is_a_visible_failure_whether_it_is_recent_or_old(
    world: World, changes: dict[str, str], attempted: str, refusal: str
) -> None:
    [unit] = hud_units(world, 1)
    seed_stale_record(world, unit, attempted_at_utc=attempted, **changes)
    world.site.serve(HUD_URL, Reply(403))
    with pytest.raises(manual_units.ManualUnitError, match=refusal) as error:
        world.run([unit])
    if refusal == "newest stale record":
        check(
            unit_key_of(unit) in str(error.value) and stale_record.stale_key(world.root, unit_key_of(unit), RUN_1) in str(error.value), "names unit and record"
        )
        check(world.site.seen == [] and world.bucket.writes() == [], "refused before any request or write")


def test_a_recent_stale_record_of_another_run_and_build_still_skips_the_unit(world: World) -> None:
    [unit] = hud_units(world, 1)
    earlier = seed_stale_record(world, unit, run_id="20261112T210000Z-dddddddd", attempted_at_utc=RECENT_ATTEMPT, code_sha256="d" * 64, bundle_sha256="e" * 64)
    report = world.run([unit], "20261113T030000Z-bbbbbbbb")
    check(report.counts["skipped_recent_stale"] == 1 and report.http_requests == 0 and world.bucket.writes() == [], f"counts {report.counts}")
    check(report.stale_units == [manual_units.summary(earlier)] and report.outcomes[0].record == earlier, f"lists {report.run_record()}")


@pytest.mark.parametrize(
    "value",
    ["2026-09-24T01:47:28Z", "2026-09-24T01:47:28+00:00", "2026-09-24T01:47:28.123456+00:00", "2026-09-24T01:47:28.5Z", "2026-09-23T18:47:28-07:00"],
    ids=["z", "offset", "microseconds_and_offset", "fraction_and_z", "other_offset"],
)
def test_a_last_capture_time_in_any_iso_form_is_stored_in_the_record_form(world: World, value: str) -> None:
    [unit] = hud_units(world, 1)
    world.specs[unit["snapshot_id"]]["last_capture"]["artifacts"][0]["retrieved_at_utc"] = value
    world.site.serve(HUD_URL, Reply(403))
    report = world.run([unit])
    check(only_stale(world, unit)["last_capture"][0]["retrieved_at_utc"] == "2026-09-24T01:47:28Z", f"stored {only_stale(world, unit)['last_capture']}")
    check(report.stale_units[0]["last_retrieved_at_utc"] == "2026-09-24T01:47:28Z", f"list {report.stale_units}")


@pytest.mark.parametrize("value", ["not a time", "2026-09-24T01:47:28", "2026-09-24", "", None], ids=["text", "no_offset", "date_only", "empty", "none"])
def test_a_last_capture_time_that_is_no_zoned_time_is_refused_before_any_request(world: World, value: str | None) -> None:
    [unit] = hud_units(world, 1)
    world.specs[unit["snapshot_id"]]["last_capture"]["artifacts"][0]["retrieved_at_utc"] = value
    with pytest.raises(manual_units.ManualUnitError, match="retrieved_at_utc"):
        world.run([unit])
    check(world.site.seen == [] and world.bucket.writes() == [], "no request, not even robots.txt")


@pytest.mark.parametrize("name", ["../bad.xlsx", "dir/bad.xlsx", ".hidden.xlsx", "transport.json", "x" * 201 + ".xlsx"])
def test_a_stored_file_name_that_is_not_plain_is_refused_before_any_request(world: World, name: str) -> None:
    unit = world.add(hud_unit(0), {})
    world.specs[unit["snapshot_id"]] = {"mode": "hud_xlsx", "stored_data_files": sorted([name, "crosswalk.csv"])}
    with pytest.raises(manual_units.ManualUnitError, match="plain stored filename"):
        world.run([unit])
    check(world.site.seen == [] and world.bucket.writes() == [], "no request, not even robots.txt")


@pytest.mark.parametrize("url", ["http://www.huduser.gov/apps/home", "https://www.huduser.gov/apps/home?token=x"])
def test_a_url_that_is_not_a_plain_public_https_address_is_refused_before_any_request(world: World, url: str) -> None:
    unit = world.add(make_unit("HUD", "hud_xlsx", url, "HUD__0"), {})
    world.specs[unit["snapshot_id"]] = hud_spec(world, unit)
    with pytest.raises(manual_units.ManualUnitError, match="HTTPS|Credentials"):
        world.run([unit])
    check(world.site.seen == [] and world.bucket.writes() == [], "no request, not even robots.txt")
