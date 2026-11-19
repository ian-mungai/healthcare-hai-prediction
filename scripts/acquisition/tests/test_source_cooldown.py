"""A publisher's Retry-After delay becomes one stored per-source cooldown: both header forms, a stated default, a key form and a typed 429 per transport."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from scripts.acquisition import capture_mode as mode
from scripts.acquisition import (
    census_acs_api_contract,
    census_acs_api_transport,
    collect_census_acs_api,
    collect_census_acs_detailed,
    collect_census_acs_tables,
    collect_hud_api,
    collect_mmd_api,
    hud_api_contract,
    hud_api_transport,
    rate_limit,
    source_cooldown,
)
from scripts.acquisition.run_census_acs_tables_e2e import Reply, connection_class
from tests.support import check

NOW = datetime(2026, 11, 19, 4, 0, 0, tzinfo=UTC)
ROOT = "raw/"
CLIENT: Any = object()
HUD_PLAN = {"endpoint": hud_api_contract.ENDPOINT, "credential_reference": hud_api_contract.CREDENTIAL, "request_spacing_seconds": 1}
HUD_BATCH = {"id": "synthetic_hud", "type": next(iter(hud_api_contract.CROSSWALKS)), "query": "All", "year": 2024, "quarter": 1}
CENSUS_PLAN = {"endpoint": census_acs_api_contract.ENDPOINT, "credential_reference": "census_api_key", "request_spacing_seconds": 1}
CENSUS_BATCH = {"id": "synthetic_census", "table": census_acs_api_contract.TABLES[0]}


@pytest.mark.parametrize(
    ("header", "seconds"),
    [("3600", 3600), (" 120 ", 120), ("0", 0), ("999999", 999999), ("2678400", source_cooldown.MAX_SECONDS), ("9999999999", source_cooldown.MAX_SECONDS)],
)
def test_a_seconds_header_is_kept_as_the_publisher_named_it_up_to_the_bound(header: str, seconds: int) -> None:
    check(source_cooldown.retry_after_seconds(header, NOW) == (seconds, "seconds"), f"{header!r}")


def test_an_http_date_header_gives_the_whole_seconds_to_that_date_and_never_less() -> None:
    target = NOW + timedelta(seconds=7200)
    check(source_cooldown.retry_after_seconds(format_datetime(target, usegmt=True), NOW) == (7200, "http_date"), "the date form")
    check(
        source_cooldown.retry_after_seconds(format_datetime(target, usegmt=True), NOW + timedelta(milliseconds=400)) == (7200, "http_date"),
        "a part second rounds up",
    )


def test_an_http_date_in_the_past_means_no_delay() -> None:
    past = format_datetime(NOW - timedelta(seconds=30), usegmt=True)
    check(source_cooldown.retry_after_seconds(past, NOW) == (0, "http_date"), "no delay")


@pytest.mark.parametrize(
    "header",
    [None, "", "  ", "abc", "-5", "1.5", "1e3", "٣٠", "12345678901", "3600 seconds", "Wed, 32 Nov 2026 25:61:00 GMT", "Wed, 18 Nov 2026 19:00:00 -0000"],
)
def test_a_missing_or_malformed_header_gets_the_stated_default(header: str | None) -> None:
    check(source_cooldown.retry_after_seconds(header, NOW) == (source_cooldown.DEFAULT_SECONDS, "default"), f"{header!r}")
    check(source_cooldown.DEFAULT_SECONDS == 3600, "the stated default is one hour")


def test_the_key_names_the_resume_time_and_run_and_sorts_in_time_order() -> None:
    key = source_cooldown.key(ROOT, "ACS", NOW + timedelta(seconds=3600), "ingestion_ab12cd34")
    check(key == "raw/ingestion_cooldowns/ACS/20261119T050000Z.ingestion_ab12cd34.json", key)
    later = source_cooldown.key(ROOT, "ACS", NOW + timedelta(days=1), "ingestion_ab12cd34")
    check(sorted([later, key]) == [key, later], "later resume times sort later")
    for bad in ("A/B", "", "a b", "../x"):
        with pytest.raises(source_cooldown.CooldownError):
            source_cooldown.key(ROOT, bad, NOW, "ingestion_ab12cd34")
    with pytest.raises(source_cooldown.CooldownError):
        source_cooldown.key(ROOT, "ACS", NOW, "bad run id")


def test_every_source_of_the_tracked_queue_can_name_a_cooldown_folder() -> None:
    queue = json.loads((Path(__file__).resolve().parents[3] / "config/acquisition/baseline_run/queue.json").read_text(encoding="utf-8"))["queue"]
    sources = {unit["source_id"] for unit in queue}
    check(sources and all(source_cooldown.prefix(ROOT, source) for source in sources), f"sources: {sorted(sources)}")


def test_the_latest_resume_time_is_the_largest_and_a_foreign_key_is_refused() -> None:
    keys = [source_cooldown.key(ROOT, "ACS", NOW + timedelta(seconds=n), f"run{n}") for n in (60, 7200, 600)]
    check(source_cooldown.latest(ROOT, "ACS", keys) == NOW + timedelta(seconds=7200), "the largest wins, so no later record shortens an earlier delay")
    check(source_cooldown.latest(ROOT, "ACS", []) is None, "no record means no cooldown")
    for bad in (
        "raw/ingestion_cooldowns/ACS/notes.txt",
        "raw/ingestion_cooldowns/HUD/20261119T050000Z.run1.json",
        "raw/ingestion_cooldowns/ACS/20261319T050000Z.run1.json",
    ):
        with pytest.raises(source_cooldown.CooldownError):
            source_cooldown.latest(ROOT, "ACS", [*keys, bad])


def test_the_stored_body_holds_the_normalized_delay_and_no_header_text() -> None:
    body = json.loads(source_cooldown.body("ACS", "ingestion_ab12cd34", NOW, 3600, "seconds", NOW + timedelta(seconds=3600)))
    check(
        body
        == {
            "kind": "source_cooldown",
            "schema_version": 1,
            "source_id": "ACS",
            "reason": "http_429",
            "run_id": "ingestion_ab12cd34",
            "observed_at_utc": "2026-11-19T04:00:00+00:00",
            "retry_after_seconds": 3600,
            "retry_after_form": "seconds",
            "not_before_utc": "2026-11-19T05:00:00+00:00",
        },
        str(body),
    )


class Store:
    """A bucket stand-in for the cooldown reads and writes of one root: a listing and create-once puts."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}


def stand_in(monkeypatch: pytest.MonkeyPatch, store: Store) -> Any:
    """A run with only what the cooldown functions read: storage, root, run ID and clock."""
    monkeypatch.setattr(mode, "list_objects", lambda _storage, prefix: [{"Key": key} for key in sorted(store.objects) if key.startswith(prefix)])

    def put(_run: Any, key: str, body: bytes) -> None:
        if key in store.objects and store.objects[key] != body:
            raise mode.RegistryError("The saved record differs")
        store.objects[key] = body

    monkeypatch.setattr(mode, "put_once", put)
    return SimpleNamespace(storage=None, root=ROOT, run_id="ingestion_ab12cd34", clock=lambda: NOW)


def test_a_saved_cooldown_blocks_only_its_source_until_it_ends(monkeypatch: pytest.MonkeyPatch) -> None:
    store = Store()
    run = stand_in(monkeypatch, store)
    until = mode.save_cooldown(run, "ACS", 3600, "seconds")
    check(until == NOW + timedelta(seconds=3600), "the resume time is the observation plus the delay")
    check(mode.cooldown_status(run, "ACS", NOW) == ("cooling", until), "blocked at once")
    check(mode.cooldown_status(run, "ACS", until - timedelta(seconds=1)) == ("cooling", until), "blocked one second before the end")
    check(mode.cooldown_status(run, "ACS", until) == ("open", None), "open at the resume time")
    check(mode.cooldown_status(run, "HUD", NOW) == ("open", None), "another source is not blocked")
    other_root: Any = SimpleNamespace(storage=None, root="ingestion_test/ingestion_x/")
    check(mode.cooldown_status(other_root, "ACS", NOW) == ("open", None), "another root has its own cooldowns")


def test_saving_the_same_cooldown_again_creates_nothing_new_and_a_different_one_at_that_key_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    store = Store()
    run = stand_in(monkeypatch, store)
    mode.save_cooldown(run, "ACS", 3600, "seconds")
    mode.save_cooldown(run, "ACS", 3600, "seconds")
    check(len(store.objects) == 1, f"one record after a retried save: {sorted(store.objects)}")
    (key,) = store.objects
    store.objects[key] = b"{}"
    with pytest.raises(mode.RegistryError):
        mode.save_cooldown(run, "ACS", 3600, "seconds")


def test_an_unreadable_listing_or_a_foreign_key_blocks_the_source(monkeypatch: pytest.MonkeyPatch) -> None:
    store = Store()
    run = stand_in(monkeypatch, store)
    store.objects["raw/ingestion_cooldowns/ACS/notes.txt"] = b"x"
    check(mode.cooldown_status(run, "ACS", NOW) == ("unreadable", None), "a key that is no cooldown record is not skipped over")

    def broken(_storage: Any, _prefix: str) -> list[dict]:
        raise mode.StorageError("listing failed", "AccessDenied")

    monkeypatch.setattr(mode, "list_objects", broken)
    check(mode.cooldown_status(run, "ACS", NOW) == ("unreadable", None), "a failed listing never allows a request")


def test_a_typed_rate_limit_keeps_only_a_bounded_printable_header() -> None:
    check(rate_limit.RateLimited("120").retry_after == "120", "a plain header is kept")
    check(rate_limit.RateLimited(None).retry_after is None, "no header")
    check(not issubclass(rate_limit.RateLimited, ValueError), "a batch loop that holds a ValueError and goes on never swallows a rate limit")

    class Headers:
        def __init__(self, value: str) -> None:
            self.value = value

        def getheader(self, name: str, default: str = "") -> str:
            return self.value if name == "Retry-After" else default

    check(rate_limit.from_response(Headers("45")).retry_after == "45", "read from a response")
    check(rate_limit.from_response(Headers("x" * 500)).retry_after is None, "a header over the bound is dropped")
    check(rate_limit.from_response(Headers("4\n5")).retry_after is None, "a header with control characters is dropped")
    check(rate_limit.from_response(Headers("")).retry_after is None, "an empty header is none")


ADAPTERS = [
    ("adapt_census_api", collect_census_acs_api),
    ("adapt_census_detailed", collect_census_acs_detailed),
    ("adapt_census_tables", collect_census_acs_tables),
    ("adapt_hud_api", collect_hud_api),
    ("adapt_mmd", collect_mmd_api),
]


@pytest.mark.parametrize(("adapter", "collector"), ADAPTERS)
def test_every_keyed_adapter_turns_a_typed_rate_limit_into_the_pausing_failure_with_the_normalized_delay(
    monkeypatch: pytest.MonkeyPatch, adapter: str, collector: Any
) -> None:
    for header, expected in (
        ("90", (90, "seconds")),
        (format_datetime(NOW + timedelta(seconds=600), usegmt=True), (600, "http_date")),
        (None, (3600, "default")),
    ):

        def limited(*_arguments: Any, header: str | None = header, **_keywords: Any) -> None:
            raise rate_limit.RateLimited(header)

        monkeypatch.setattr(collector, "execute", limited)
        monkeypatch.setattr(mode, "planned_item", lambda _run, _handler, _spec: ({}, {}))
        monkeypatch.setattr(mode, "client_for", lambda _run, _handler: None)
        run: Any = SimpleNamespace(api_request=None, clock=lambda: NOW, storage=None)
        with pytest.raises(mode.RateLimit) as caught:
            getattr(mode, adapter)({}, {"mmd": {"measure_id": "synthetic", "year": 2023}}, Path("."), run, None)
        check((caught.value.retry_after_seconds, caught.value.form) == expected, f"{adapter} {header!r}: {caught.value.retry_after_seconds}")
        check(str(caught.value) == "http_429", "the fixed reason that pauses the source")
    check("http_429" in mode.PAUSING, "that reason pauses")


def limited_reply(seconds: str | None) -> type:
    class Limited(Reply):
        def __init__(self) -> None:
            super().__init__(429, b"{}")

        def getheader(self, name: str, default: str = "") -> str:
            return seconds if name == "Retry-After" and seconds is not None else super().getheader(name, default)

    return Limited


def test_the_hud_transport_stops_at_a_429_without_a_retry_and_keeps_the_header(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    sent: list[str] = []
    slept: list[float] = []
    reply = limited_reply("120")
    monkeypatch.setattr(hud_api_transport, "https_connection", connection_class(lambda _path: reply(), sent))
    monkeypatch.setattr(hud_api_transport.api_keys, "checked_read", lambda _client, _name, _label: "a" * 24)
    with pytest.raises(rate_limit.RateLimited) as caught:
        hud_api_transport.fetch(HUD_PLAN, HUD_BATCH, tmp_path, True, CLIENT, sleep=slept.append)
    check(caught.value.retry_after == "120" and len(sent) == 1 and slept == [], f"one attempt, no backoff: {sent} {slept}")


def test_the_census_api_transport_stops_at_a_429_without_a_retry_and_keeps_the_header(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    sent: list[str] = []
    slept: list[float] = []
    reply = limited_reply(None)
    monkeypatch.setattr(census_acs_api_transport, "https_connection", connection_class(lambda _path: reply(), sent))
    monkeypatch.setattr(census_acs_api_transport.api_keys, "checked_read", lambda _client, _name, _label: "a" * 32)
    with pytest.raises(rate_limit.RateLimited) as caught:
        census_acs_api_transport.fetch(CENSUS_PLAN, CENSUS_BATCH, tmp_path, True, CLIENT, sleep=slept.append)
    check(caught.value.retry_after is None and len(sent) == 1 and slept == [], f"one attempt, no backoff: {sent} {slept}")


def test_the_mmd_request_stops_at_a_429_with_the_typed_outcome_and_keeps_the_header(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    sent: list[str] = []
    reply = limited_reply("1800")
    monkeypatch.setattr(collect_mmd_api, "https_connection", connection_class(lambda _path: reply(), sent))
    with pytest.raises(rate_limit.RateLimited) as caught:
        collect_mmd_api.fetch(tmp_path, "page", "https://data.cms.gov/data-api/v1/mmd-tool/?measure=x", True)
    check(caught.value.retry_after == "1800" and len(sent) == 1, f"one request: {sent}")
    check(not list(tmp_path.iterdir()), "a rate limit stores no response and no metadata")


def test_a_server_error_of_either_transport_is_still_retried_three_times(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for module, label, plan, batch in (
        (hud_api_transport, "hud", HUD_PLAN, HUD_BATCH),
        (census_acs_api_transport, "census", CENSUS_PLAN, CENSUS_BATCH),
    ):
        sent: list[str] = []
        slept: list[float] = []
        monkeypatch.setattr(module, "https_connection", connection_class(lambda _path: Reply(503, b"{}"), sent))
        monkeypatch.setattr(module.api_keys, "checked_read", lambda _client, _name, _label: "a" * 32)
        with pytest.raises(module.RetryableRequest):
            module.fetch(plan, batch, tmp_path / label, True, CLIENT, sleep=slept.append)
        check(len(sent) == 3 and slept == [2, 4], f"{label}: three attempts with the two backoffs: {len(sent)} {slept}")
