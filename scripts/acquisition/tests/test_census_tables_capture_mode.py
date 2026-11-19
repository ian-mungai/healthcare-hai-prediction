"""The 173 Census table units in capture mode: each has its own key and handler, its collector runs through the run and the startup check lists its plan."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
from botocore.exceptions import ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError

from scripts.acquisition import capture_mode as mode
from scripts.acquisition import census_acs_tables_contract as contract
from scripts.acquisition import census_acs_tables_transport, collection_layout, completion_record, redownload, s3_store, task_role, transport
from scripts.acquisition import collect_census_acs_tables as collector
from scripts.acquisition.run_census_acs_tables_e2e import Reply, Requests, connection_class, dictionary, response
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.source_registry import REPO_ROOT, read_json
from scripts.acquisition.tests.test_capture_mode import ROOT, World
from scripts.acquisition.tests.test_task_keys import ENVIRONMENT, Cloud
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
    check(not world.bucket.names(f"{ROOT}ingestion_cooldowns/") and "source_cooldowns" not in result, "a contract failure stores no cooldown")


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


class Limited(Reply):
    """A 429 reply that names the publisher's delay, as seconds or as the header text given, or names none."""

    def __init__(self, value: int | str | None) -> None:
        super().__init__(429, b"{}")
        self.value = None if value is None else str(value)

    def getheader(self, name: str, default: str = "") -> str:
        return self.value if name == "Retry-After" and self.value is not None else super().getheader(name, default)


def served(path: str) -> Reply:
    """What the Census host answers for one request path: a group dictionary or the group's county table."""
    parsed = urlsplit(path)
    year = int(parsed.path.split("/")[2])
    if "/groups/" in parsed.path:
        return Reply(200, dictionary(parsed.path.rsplit("/", 1)[-1].removesuffix(".json")))
    return Reply(200, response(parse_qs(parsed.query)["get"][0][6:-1], year))


def table_of(path: str) -> str:
    parsed = urlsplit(path)
    return parsed.path.rsplit("/", 1)[-1].removesuffix(".json") if "/groups/" in parsed.path else parse_qs(parsed.query)["get"][0][6:-1]


class Host:
    """The Census host and key service of one run: the real transport and collector, a fake HTTPS connection, a fake key store and recorded sleeps."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, answer: Any = served) -> None:
        self.sent: list[str] = []
        self.sleeps: list[float] = []
        self.cloud = Cloud()
        self.answer = answer
        monkeypatch.setattr(collector, "fetch", partial(census_acs_tables_transport.fetch, sleep=self.sleeps.append))
        monkeypatch.setattr(census_acs_tables_transport, "https_connection", connection_class(lambda path: self.answer(path), self.sent))

    def run(self, world: World, **fields: Any) -> dict:
        keys = partial(task_role.TaskKeys, environment=ENVIRONMENT, factory=self.cloud.factory)
        return world.run(task_keys=keys, **fields)


COOLDOWNS = f"{ROOT}ingestion_cooldowns/"


def cooldown_names(world: World) -> list[str]:
    return sorted(world.bucket.names(COOLDOWNS))


def three_units(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[World, list[dict]]:
    batches = [batch_of("S0101", 2010), batch_of("S0601", 2010), batch_of("DP02", 2010)]
    return table_world(tmp_path, monkeypatch, batches), batches


def test_a_rate_limit_after_an_earlier_success_pauses_the_source_and_stores_when_it_may_resume(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    world, batches = three_units(tmp_path, monkeypatch)
    now = datetime.now(UTC).replace(microsecond=0)
    host = Host(monkeypatch, lambda path: Limited(3600) if table_of(path) == "S0601" else served(path))
    messages: list[dict] = []
    first = host.run(world, clock=lambda: now, log=messages.append)
    units, resume = first["units"], now + timedelta(seconds=3600)
    check(len(units["completed"]) == 1 and [item["reason"] for item in units["failed"]] == ["http_429"], f"{units}")
    check(units["paused_sources"] == ["ACS"] and units["paused_not_attempted"] == [contract.queue_snapshot_id(batches[2])], f"the source is paused: {units}")
    check(not any(table_of(path) == "DP02" for path in host.sent), f"no further ACS request after the 429: {host.sent}")
    check(len(host.sent) == 3 and first["counters"]["http_requests"] == 3, f"two requests for the first unit and one 429: {host.sent}")
    check(host.sleeps == [1, 1], f"only the spacing after the first unit's two requests, no backoff for the 429: {host.sleeps}")
    limited = [item for item in messages if item.get("retry_after_seconds") is not None]
    check(
        len(limited) == 1 and limited[0]["retry_after_seconds"] == 3600 and limited[0]["status"] == "failed", f"the publisher's delay is reported: {messages}"
    )
    check(first["source_cooldowns"] == {"ACS": resume.isoformat()}, f"the run record says when the source may resume: {first.get('source_cooldowns')}")
    check(cooldown_names(world) == [f"{COOLDOWNS}ACS/{resume:%Y%m%dT%H%M%SZ}.run01.json"], f"one cooldown record: {cooldown_names(world)}")
    (name,) = cooldown_names(world)
    body = json.loads(world.bucket.objects[name][1])
    check(body["retry_after_seconds"] == 3600 and body["retry_after_form"] == "seconds" and body["not_before_utc"] == resume.isoformat(), str(body))
    problems = mode.check_run(storage(world.bucket), first, [])
    check(not any("cooldown" in problem for problem in problems), f"the evidence step accepts the cooldown write: {problems}")


def test_a_rerun_before_the_cooldown_ends_sends_nothing_and_a_rerun_after_it_resumes_the_unfinished_units(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    world, batches = three_units(tmp_path, monkeypatch)
    now = datetime.now(UTC).replace(microsecond=0)
    host = Host(monkeypatch, lambda path: Limited(3600) if table_of(path) == "S0601" else served(path))
    host.run(world, clock=lambda: now)
    resume = now + timedelta(seconds=3600)
    host.answer = served
    sent = len(host.sent)
    for moment in (now, resume - timedelta(seconds=1)):
        messages: list[dict] = []
        blocked = host.run(world, clock=lambda moment=moment: moment, log=messages.append)
        check(len(host.sent) == sent and blocked["counters"]["http_requests"] == 0, f"no ACS request before the cooldown ends: {blocked['counters']}")
        check(
            blocked["units"]["paused_sources"] == ["ACS"] and blocked["source_cooldowns"] == {"ACS": resume.isoformat()},
            f"the source is reported paused: {blocked['units']}",
        )
        check(
            sorted(blocked["units"]["paused_not_attempted"]) == sorted(contract.queue_snapshot_id(batch) for batch in batches[1:]),
            f"the unfinished units wait: {blocked['units']}",
        )
        check(blocked["counters"]["skipped_complete"] == 1 and not blocked["units"]["failed"], "a verified completion is still skipped, nothing fails")
        waiting = [item for item in messages if item["status"] == "paused_cooldown"]
        check(len(waiting) == 2 and all(item["resume_at_utc"] == resume.isoformat() for item in waiting), f"the log names the resume time: {messages}")
    check(len(world.records()) == 1 and len(cooldown_names(world)) == 1, "the blocked runs wrote no record and no new cooldown")
    listed = mode.list_versions(storage(world.bucket), mode.evidence_prefixes(ROOT))
    resumed = host.run(world, clock=lambda: resume)
    check(len(resumed["units"]["completed"]) == 2 and resumed["counters"]["skipped_complete"] == 1, f"{resumed['units']} {resumed['counters']}")
    check("source_cooldowns" not in resumed and resumed["units"]["paused_sources"] == [], "nothing is reported paused once the cooldown has ended")
    check(len(world.records()) == 3 and len(cooldown_names(world)) == 1, "every unit has its record and the old cooldown stays as history")
    check(mode.check_run(storage(world.bucket), resumed, listed) == [], "the resumed run passes the evidence step")


@pytest.mark.parametrize(
    ("case", "seconds", "form"), [("none", 3600, "default"), ("garbage", 3600, "default"), ("date", 7200, "http_date"), ("seconds", 90, "seconds")]
)
def test_the_cooldown_follows_the_publishers_header_in_either_form_and_defaults_when_it_is_missing_or_malformed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, case: str, seconds: int, form: str
) -> None:
    world, _batches = three_units(tmp_path, monkeypatch)
    now = datetime.now(UTC).replace(microsecond=0)
    value = {"none": None, "garbage": "soon", "date": format_datetime(now + timedelta(seconds=7200), usegmt=True), "seconds": 90}[case]
    host = Host(monkeypatch, lambda path: Limited(value) if table_of(path) == "S0601" else served(path))
    first = host.run(world, clock=lambda: now)
    resume = now + timedelta(seconds=seconds)
    check(first["source_cooldowns"] == {"ACS": resume.isoformat()}, f"{case}: {first.get('source_cooldowns')}")
    (name,) = cooldown_names(world)
    body = json.loads(world.bucket.objects[name][1])
    check(body["retry_after_seconds"] == seconds and body["retry_after_form"] == form, f"{case}: {body}")


def test_an_unreadable_cooldown_blocks_the_source_instead_of_allowing_a_request(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    world, batches = three_units(tmp_path, monkeypatch)
    host = Host(monkeypatch)
    real = mode.list_objects

    def listing(store: Any, prefix: str) -> list[dict]:
        if "ingestion_cooldowns/" in prefix:
            raise mode.StorageError("list cooldowns", "AccessDenied")
        return real(store, prefix)

    monkeypatch.setattr(mode, "list_objects", listing)
    result = host.run(world)
    check(not host.sent and result["counters"]["http_requests"] == 0, f"no request when the cooldown cannot be read: {host.sent}")
    check(len(result["units"]["paused_not_attempted"]) == len(batches) and result["units"]["paused_sources"] == ["ACS"], f"{result['units']}")
    check(result["source_cooldowns"] == {"ACS": "unreadable"} and not world.records(), f"{result.get('source_cooldowns')}")


def with_cms_unit(world: World, tmp_path: Path) -> tuple[str, str]:
    """Add one real generic file unit of an unrelated source after the ACS units. Returns its source ID and its unit's folder in the bucket."""
    folder = tmp_path / "cms"
    folder.mkdir()
    other = World(folder, ["other.csv"])
    specs = read_json(world.folder / "capture_specs.json")["specs"] | read_json(other.folder / "capture_specs.json")["specs"]
    world.units.extend(other.units)
    world.bodies.update(other.bodies)
    world.write_inputs(specs)
    return other.units[0]["source_id"], completion_record.unit_prefix(ROOT, world.unit_key(len(world.units) - 1))


SDK_ERRORS = {
    "client_error": lambda world: world.bucket.missing("ListObjectsV2", "AccessDenied"),
    "no_connection": lambda _world: EndpointConnectionError(endpoint_url="https://s3.invalid"),
    "connect_timeout": lambda _world: ConnectTimeoutError(endpoint_url="https://s3.invalid"),
    "read_timeout": lambda _world: ReadTimeoutError(endpoint_url="https://s3.invalid"),
}


@pytest.mark.parametrize("error", sorted(SDK_ERRORS))
@pytest.mark.parametrize("where", ["cooldown", "unit"])
def test_a_listing_error_of_the_store_fails_only_its_own_source_by_name_and_an_unrelated_source_completes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, where: str, error: str
) -> None:
    world, batches = three_units(tmp_path, monkeypatch)
    other, other_prefix = with_cms_unit(world, tmp_path)
    host = Host(monkeypatch)
    real = world.bucket.list_objects_v2

    def listing(**arguments: Any) -> dict:
        prefix = arguments["Prefix"]
        broken = f"{COOLDOWNS}ACS/" in prefix if where == "cooldown" else prefix.startswith(f"{ROOT}ingestion_units/") and prefix != other_prefix
        if broken:
            raise SDK_ERRORS[error](world)
        return real(**arguments)

    monkeypatch.setattr(world.bucket, "list_objects_v2", listing)
    before = mode.list_versions(storage(world.bucket), mode.evidence_prefixes(ROOT))
    result = host.run(world)
    monkeypatch.setattr(world.bucket, "list_objects_v2", real)
    units = result["units"]
    check(not host.sent and result["counters"]["http_requests"] == 1, f"no ACS request, one request for the unrelated source: {host.sent} {result['counters']}")
    check(units["paused_sources"] == ["ACS"], f"the failing source is named: {units}")
    check([item["source_id"] for item in units["completed"]] == [other], f"the unrelated source completed: {units}")
    if where == "cooldown":
        check(result["source_cooldowns"] == {"ACS": "unreadable"} and not units["failed"], f"{result.get('source_cooldowns')} {units['failed']}")
        check(len(units["paused_not_attempted"]) == len(batches), f"every ACS unit waits: {units}")
    else:
        check([(item["source_id"], item["reason"]) for item in units["failed"]] == [("ACS", "storage_contract")], f"the failure is named and fixed: {units}")
        check(len(units["paused_not_attempted"]) == len(batches) - 1, f"the rest of the source waits: {units}")
    check(f"{ROOT}ingestion_runs/run01/run.json" in world.bucket.names(), "the run record was saved")
    problems = mode.check_run(storage(world.bucket), result, before)
    ours = [contract.queue_snapshot_id(batch) for batch in batches]
    check(
        len(problems) == len(batches),
        f"the evidence step, which fails the run's exit, names the unfinished units of the failing source and nothing else: {problems}",
    )
    check(all("was not attempted" in item or "did not complete" in item for item in problems), f"only unfinished units are named: {problems}")
    check(
        sum(any(snapshot in item for snapshot in ours) for item in problems) + sum("storage_contract" in item for item in problems) >= len(batches),
        str(problems),
    )
    for text in ("synthetic", "s3.invalid", "AccessDenied"):
        check(text not in json.dumps(result), f"no SDK message text reaches the run record: {text}")


@pytest.mark.parametrize(("recheck", "nth"), [(False, 2), (True, 1)], ids=["generation", "monthly_recheck"])
def test_a_records_listing_error_after_an_earlier_success_pauses_its_source_and_an_unrelated_source_still_completes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, recheck: bool, nth: int
) -> None:
    world, batches = three_units(tmp_path, monkeypatch)
    other, _ = with_cms_unit(world, tmp_path)
    host = Host(monkeypatch)
    prefix, real, seen = completion_record.unit_prefix(ROOT, world.unit_key(1)), world.bucket.list_objects_v2, []

    def listing(**arguments: Any) -> dict:
        if arguments["Prefix"] == prefix:
            seen.append(prefix)
            if len(seen) == nth:
                raise world.bucket.missing("ListObjectsV2", "AccessDenied")
        return real(**arguments)

    monkeypatch.setattr(world.bucket, "list_objects_v2", listing)
    before = mode.list_versions(storage(world.bucket), mode.evidence_prefixes(ROOT))
    result = host.run(world, recheck=recheck)
    units = result["units"]
    finished = units["changed"] if recheck else units["completed"]
    check(not any(table_of(path) == "DP02" for path in host.sent), f"no ACS request after the fault: {host.sent}")
    check(units["paused_sources"] == ["ACS"], f"the faulting source is paused: {units}")
    check(units["paused_not_attempted"] == [contract.queue_snapshot_id(batches[2])], f"its next unit waits: {units}")
    check([item["source_id"] for item in units["failed"]] == ["ACS"] and units["failed"][0]["reason"] == "storage_contract", f"it is named: {units['failed']}")
    check(sorted(item["source_id"] for item in finished) == ["ACS", other], f"the earlier ACS unit and the unrelated source finished: {finished}")
    check(
        len(world.site.asked) == 1 and f"{ROOT}ingestion_runs/run01/run.json" in world.bucket.names(),
        "the unrelated unit was requested and the run is on record",
    )
    problems = mode.check_run(storage(world.bucket), result, before)
    check(len(problems) == 2, f"the evidence step names the failed and the paused unit only: {problems}")


def test_a_cooldown_that_cannot_be_saved_stops_the_run_before_another_request(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    world, _batches = three_units(tmp_path, monkeypatch)
    host = Host(monkeypatch, lambda path: Limited(3600) if table_of(path) == "S0601" else served(path))
    real = mode.put_once

    def put(run: Any, key: str, body: bytes) -> None:
        if "ingestion_cooldowns/" in key:
            raise mode.StorageError("save cooldown", "AccessDenied")
        real(run, key, body)

    monkeypatch.setattr(mode, "put_once", put)
    with pytest.raises(mode.StorageError):
        host.run(world)
    check(len(host.sent) == 3 and not cooldown_names(world), f"the run stopped at the 429: {host.sent}")
    monkeypatch.setattr(mode, "put_once", real)
    host.answer = served
    rerun = host.run(world)
    check(len(rerun["units"]["completed"]) == 2 and not cooldown_names(world), "without a saved cooldown the next run is not blocked, and none was invented")


def test_a_server_error_is_retried_with_backoff_and_does_not_pause_the_source(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    batches = [batch_of("S0101", 2010), batch_of("S0601", 2010), batch_of("DP02", 2010)]
    world = table_world(tmp_path, monkeypatch, batches)
    host = Host(monkeypatch, lambda path: Reply(503, b"{}") if table_of(path) == "S0601" else served(path))
    result = host.run(world)
    units = result["units"]
    check([item["reason"] for item in units["failed"]] == ["retryable_request"] and len(units["completed"]) == 2, f"{units}")
    check(units["paused_sources"] == [] and not units["paused_not_attempted"], "a transient server error pauses nothing")
    check(host.sleeps.count(2) == 1 and host.sleeps.count(4) == 1, f"three attempts with the two backoffs: {host.sleeps}")
    check(not cooldown_names(world) and "source_cooldowns" not in result, "a transient server error stores no cooldown")


def test_the_run_counts_actual_sends_and_bytes_of_the_tables_transport_and_refuses_past_the_byte_cap(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    batch = batch_of("S0101", 2010)
    world = table_world(tmp_path, monkeypatch, [batch])
    host = Host(monkeypatch)
    result = host.run(world)
    sizes = len(response("S0101", 2010)) + len(dictionary("S0101"))
    check(len(result["units"]["completed"]) == 1 and len(host.sent) == 2, f"{result['units']} {host.sent}")
    check(result["counters"]["http_requests"] == 2, f"the sends of this transport are counted: {result['counters']}")
    check(result["counters"]["bytes_downloaded"] == sizes, f"the received bytes are counted: {result['counters']['bytes_downloaded']} of {sizes}")


def test_the_byte_cap_refuses_a_response_the_tables_transport_reads(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    batch = batch_of("S0101", 2010)
    world = table_world(tmp_path, monkeypatch, [batch])
    world.units[0]["bytes"] = 100
    world.write_inputs({world.units[0]["snapshot_id"]: {"mode": MODE, "plan_item": batch["id"], "stored_data_files": [batch["file_name"]]}})
    host = Host(monkeypatch)
    result = host.run(world, byte_cap=5000)
    check([item["reason"] for item in result["units"]["failed"]] == ["byte_cap_exceeded"], f"{result['units']}")
    check(len(host.sent) == 1 and result["counters"]["http_requests"] == 1, f"one request was sent and counted: {host.sent}")
    check(result["counters"]["bytes_downloaded"] > 0 and not world.records(), "the bytes read are counted and no record is written")


def test_skipping_only_the_tables_transport_in_the_observer_loses_its_sends(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    batch = batch_of("S0101", 2010)
    world = table_world(tmp_path, monkeypatch, [batch])
    host = Host(monkeypatch)
    monkeypatch.setattr(mode, "API_TRANSPORTS", tuple(module for module in mode.API_TRANSPORTS if module is not census_acs_tables_transport))
    result = host.run(world)
    check(len(host.sent) == 2 and result["counters"]["http_requests"] == 0, f"without the wrapping the sends are not counted: {result['counters']}")
