"""Repeatable synthetic verification of the bounded redownload harness, without live publishers, credentials or AWS."""

import argparse
import copy
import io
import json
import os
import stat
import sys
import tempfile
import time
import zipfile
from datetime import UTC, datetime, timedelta
from email.message import Message
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.acquisition import bls_api_contract as bls_contract
from scripts.acquisition import onc_mu_contract as onc_contract
from scripts.acquisition import redownload as harness
from scripts.acquisition import run_onc_mu_e2e as onc_e2e
from scripts.acquisition import run_redownload_controls_e2e as control_e2e
from scripts.acquisition import run_wonder_export_e2e as wonder_e2e
from scripts.acquisition import wonder_export_contract as wonder_contract
from scripts.acquisition.build_wonder_plan import build as build_wonder_plan
from scripts.acquisition.collect_bls_api import execute as bls_execute
from scripts.acquisition.run_bls_api_e2e import fixture as bls_fixture
from scripts.acquisition.run_hud_xlsx_e2e import download_metadata, set_origin
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import canonical_hash, load_registry, read_json, require
from scripts.acquisition.store_onc_mu import execute as onc_execute
from scripts.acquisition.store_wonder_export import execute as wonder_execute
from scripts.acquisition.tests.test_s3_store import FakeS3

SENSITIVE_TEXT = "synthetic-registration-value"


class Headers(Message):
    """HTTP headers with the case-insensitive lookups urllib responses provide."""

    def __init__(self, values: dict[str, str]) -> None:
        super().__init__()
        for key, value in values.items():
            self[key] = value


class Response(io.BytesIO):
    """A minimal HTTP response for the project downloader."""

    def __init__(self, data: bytes, url: str, status: int = 200, media: str = "text/csv") -> None:
        super().__init__(data)
        self.status, self.url = status, url
        self.headers = Headers({"Content-Length": str(len(data)), "Content-Type": media})

    def geturl(self) -> str:
        return self.url


class Web:
    """Answers each URL from a scripted list of (status, body) replies and records every request."""

    def __init__(self) -> None:
        self.replies: dict[str, list[tuple[int, bytes]]] = {}
        self.requests: list[str] = []

    def serve(self, url: str, *replies: tuple[int, bytes]) -> None:
        self.replies[url] = list(replies)

    def open(self, request: Any, timeout: float) -> Response:
        url = request.full_url
        self.requests.append(url)
        replies = self.replies[url]
        status, body = replies.pop(0) if len(replies) > 1 else replies[0]
        return Response(body, url, status, "text/csv" if status == 200 else "text/html")

    def count(self, url: str) -> int:
        return self.requests.count(url)


class Fixture:
    """Builds synthetic stored captures and a locked queue under one temporary base folder."""

    def __init__(self, base: Path) -> None:
        self.base = base
        self.entries: list[dict] = []

    def stored(self, source: str, name: str, body: bytes, disposition: str = "direct_download", flags: list[str] | None = None, mode: str = "file") -> dict:
        sid = f"{source}__20260101T000000Z__{canonical_hash([source, name])[:32]}"
        folder = self.base / "stored" / sid
        (folder / "raw").mkdir(parents=True)
        (folder / "raw" / name).write_bytes(body)
        url = f"https://publisher.example.org/{source.lower()}/{name}"
        receipt = {
            "snapshot_id": sid,
            "snapshot_status": "acquired_unvalidated",
            "acquisition": {"requested_url": url},
            "artifacts": [{"role": "data", "stored_file_name": name, "storage_path": f"raw/{name}", "sha256": harness.digest(body), "byte_count": len(body)}],
            "lineage": {"extraction_or_query": json.dumps({"mode": mode})},
        }
        path = folder / "receipt.json"
        path.write_bytes(encoded_json(receipt))
        route = {"direct_download": "publisher_url", "api": "publisher_api", "approved_manual": "manual_browser_download"}.get(disposition)
        entry = {
            "snapshot_id": sid,
            "source_id": source,
            "receipt": str(path.relative_to(self.base)),
            "receipt_sha256": harness.digest(path.read_bytes()),
            "url": url,
            "mode": mode,
            "bytes": len(body),
            "pages": 1,
            "disposition": disposition,
            "route": route,
            "flags": flags or [],
        }
        self.entries.append(entry)
        return entry

    def paged(self, source: str, pages: list[bytes]) -> dict:
        """A stored paged API capture: one recorded request and one page artifact per page."""
        entry = self.stored(source, "page_0001.json", pages[0], disposition="api", mode="il_directory")
        folder = self.base / "stored" / entry["snapshot_id"]
        artifacts, requests = [], []
        for number, body in enumerate(pages, 1):
            name = f"page_{number:04}.json"
            (folder / "raw" / name).write_bytes(body)
            artifacts.append(
                {"role": "api_page", "stored_file_name": name, "storage_path": f"raw/{name}", "sha256": harness.digest(body), "byte_count": len(body)}
            )
            requests.append({"requested_url": f"{entry['url']}?page={number}"})
        receipt = read_json(folder / "receipt.json") | {"artifacts": artifacts}
        receipt["lineage"] = {"extraction_or_query": json.dumps({"mode": "il_directory", "requests": requests})}
        (folder / "receipt.json").write_bytes(encoded_json(receipt))
        entry["receipt_sha256"] = harness.digest((folder / "receipt.json").read_bytes())
        return entry

    def freeze(self, name: str = "queue.json") -> Path:
        path = self.base / name
        body = encoded_json({"kind": "bounded_redownload_queue", "entries": len(self.entries), "queue": self.entries})
        path.write_bytes(body)
        path.with_name(path.stem + ".lock.json").write_bytes(encoded_json({"queue_sha256": harness.digest(body), "entries": len(self.entries)}))
        return path


def archive(members: dict[str, bytes], year: int, comment: bytes = b"", entry_comments: dict[str, bytes] | None = None) -> bytes:
    """A zip whose bytes depend on its build year, like an export archive the publisher rebuilds on request."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as handle:
        handle.comment = comment
        for name, body in members.items():
            info = zipfile.ZipInfo(name, date_time=(year, 1, 1, 0, 0, 0))
            info.comment = (entry_comments or {}).get(name, b"")
            handle.writestr(info, body)
    return buffer.getvalue()


def recorded(run: Any, snapshot_id: str) -> dict:
    """A unit's outcome, which the calling scenario expects to exist."""
    result = harness.outcome(run, snapshot_id)
    require(result is not None, "Expected an outcome for this unit")
    return result or {}


def exercise(root: Path) -> list[dict]:
    """Drive the real harness with a synthetic queue, a fake publisher and fake adapters."""
    scenarios: list[dict] = []

    def scenario(name: str, action: Any, error: str | None = None) -> None:
        try:
            action()
            scenarios.append({"name": name, "passed": error is None, "observed": "success", "expected": error or "success"})
        except (ValueError, OSError, KeyError, TypeError, StopIteration, IndexError, AssertionError) as failure:
            scenarios.append(
                {"name": name, "passed": error is not None and error in str(failure), "observed": str(failure)[:300], "expected": error or "success"}
            )

    base = root / "base"
    base.mkdir()
    fixture, web = Fixture(base), Web()
    same = fixture.stored("ALPHA", "same.csv", b"a,b\n1,2\n")
    second = fixture.stored("ALPHA", "second.csv", b"a,b\n3,4\n")
    third = fixture.stored("ALPHA", "third.csv", b"a,b\n5,6\n")
    changed = fixture.stored("BETA", "changed.csv", b"a,b\n1,2\n")
    after_change = fixture.stored("BETA", "after.csv", b"a,b\n9,9\n")
    missing = fixture.stored("GAMMA", "missing.csv", b"a,b\n1,2\n")
    busy = fixture.stored("DELTA", "busy.csv", b"a,b\n1,2\n")
    busy_next = fixture.stored("DELTA", "next.csv", b"a,b\n1,2\n")
    flaky = fixture.stored("EPSILON", "flaky.csv", b"a,b\n7,8\n")
    failing = fixture.stored("ZETA", "failing.csv", b"a,b\n7,8\n")
    private = fixture.stored("ETA", "people.csv", b"name\nSynthetic Person\n", flags=["original_contains_personal_details:private_owner_only"])
    duplicate = fixture.stored("ALPHA", "duplicate.csv", b"x\n", disposition="superseded_duplicate")
    dropped = fixture.stored("THETA", "dropped.csv", b"x\n", disposition="dropped")
    derived = fixture.stored("ALPHA", "derived.csv", b"x\n", disposition="derived_replay_only")
    manual = fixture.stored("IOTA", "manual.csv", b"m\n1\n", disposition="approved_manual")
    rebuilt = fixture.stored("NU", "export.zip", archive({"data.csv": b"n\n1\n", "notes.txt": b"notes"}, 2026), disposition="approved_manual")
    revised = fixture.stored("XI", "export.zip", archive({"table.csv": b"x\n1\n"}, 2026), disposition="approved_manual")
    adapted = fixture.stored("KAPPA", "adapted.csv", b"k\n1\n", disposition="api", mode="synthetic_collector")
    adapted_changed = fixture.stored("LAMBDA", "adapted.csv", b"k\n1\n", disposition="api", mode="synthetic_collector")
    adapted_error = fixture.stored("MU", "adapted.csv", b"k\n1\n", disposition="api", mode="synthetic_collector")
    pages = [b'{"page": 1}', b'{"page": 2}']
    paged = fixture.paged("OMICRON", pages)
    paged_changed = fixture.paged("PI", pages)
    queue = fixture.freeze()
    for number, body in enumerate(pages, 1):
        web.serve(f"{paged['url']}?page={number}", (200, body))
        web.serve(f"{paged_changed['url']}?page={number}", (200, body if number == 1 else b'{"page": 3}'))
    for entry, body in (
        (same, b"a,b\n1,2\n"),
        (second, b"a,b\n3,4\n"),
        (third, b"a,b\n5,6\n"),
        (after_change, b"a,b\n9,9\n"),
        (private, b"name\nSynthetic Person\n"),
    ):
        web.serve(entry["url"], (200, body))
    web.serve(changed["url"], (200, b"a,b\n1,3\n"))
    web.serve(missing["url"], (404, b"<html>not found</html>"))
    web.serve(busy["url"], (429, b"<html>slow down</html>"))
    web.serve(flaky["url"], (503, b"<html>try again</html>"), (200, b"a,b\n7,8\n"))
    web.serve(failing["url"], (503, b"<html>try again</html>"))
    for entry in (duplicate, dropped, derived, manual, busy_next):
        web.serve(entry["url"], (200, b"a,b\n1,2\n"))

    def adapter(unit: dict, run: Any) -> Path:
        """A stand-in collector: writes a fresh receipt whose derived data matches, differs or fails."""
        if unit["source_id"] == "MU":
            raise ValueError(f"Synthetic collector failure at {Path.home()}/private token={SENSITIVE_TEXT} https://example.org/x?X-Amz-Signature=abc")
        body = b"k\n1\n" if unit["source_id"] == "KAPPA" else b"k\n2\n"
        folder = run.state_root / "collectors" / unit["mode"] / unit["snapshot_id"]
        (folder / "derived").mkdir(parents=True, exist_ok=True)
        (folder / "derived/adapted.csv").write_bytes(body)
        receipt = {"artifacts": [{"role": "data", "stored_file_name": "adapted.csv", "storage_path": "derived/adapted.csv", "sha256": harness.digest(body)}]}
        (folder / "receipt.json").write_bytes(encoded_json(receipt))
        return folder / "receipt.json"

    manual_folder = root / "manual"
    manual_folder.mkdir()

    def make_run(state: str, **changes: Any) -> Any:
        options: dict[str, Any] = {
            "queue_path": queue,
            "state_root": root / state,
            "base": base,
            "manual_folder": manual_folder,
            "opener": web,
            "adapters": {"synthetic_collector": adapter},
            "sleep": lambda _seconds: None,
        }
        run = harness.Run(**(options | changes))
        control_e2e.approve_fixture(run)
        return run

    run = make_run("state")

    def lock_mismatch() -> None:
        other = Fixture(root / "tampered")
        (root / "tampered").mkdir()
        other.stored("ALPHA", "same.csv", b"a,b\n1,2\n")
        path = other.freeze()
        path.write_bytes(path.read_bytes().replace(b"ALPHA", b"OTHER"))
        harness.execute(harness.Run(queue_path=path, state_root=root / "tampered_state", base=root / "tampered", opener=web, sleep=lambda _s: None))

    scenario("queue_differing_from_its_lock_refused", lock_mismatch, "Queue differs from its lock")

    def changed_receipt() -> None:
        other_base = root / "edited"
        other_base.mkdir()
        other = Fixture(other_base)
        entry = other.stored("ALPHA", "same.csv", b"a,b\n1,2\n")
        path = other.freeze()
        (other_base / entry["receipt"]).write_bytes(b"{}")
        harness.execute(harness.Run(queue_path=path, state_root=root / "edited_state", base=other_base, opener=web, sleep=lambda _s: None))

    scenario("receipt_changed_since_freeze_refused", changed_receipt, "Receipt changed since the queue was frozen")

    def dry_run() -> None:
        summary = harness.execute(run, allow_network=False)
        require(not web.requests and not (root / "state").exists(), "Dry run requested or wrote")
        require(summary["active_units"] == 19 and summary["planned"] == 19, f"Dry-run counts differ: {summary}")

    scenario("dry_run_makes_no_request_and_writes_nothing", dry_run)

    def preflight() -> None:
        result = harness.preflight(run)
        expected = {"collector:synthetic_collector": 3, "file": 11, "manual_file": 3, "paged_api": 2}
        require(result["resolved"] == 19 and result["handlers"] == expected and not web.requests, f"Preflight differs: {result}")

    scenario("preflight_resolves_every_unit_offline", preflight)

    def pilot() -> None:
        summary = harness.execute(run, pilot=True)
        require(web.count(same["url"]) == 1 and web.count(second["url"]) == 0 and web.count(third["url"]) == 0, "Pilot ran more than one unit of a pair")
        require(recorded(run, same["snapshot_id"])["outcome"] == "exact_match", "Pilot outcome differs")
        require(summary["pilot"] is True, "Pilot flag not recorded")

    scenario("pilot_runs_one_unit_per_source_route_and_mode", pilot)

    def full() -> None:
        harness.execute(run, sources={"ALPHA"})
        require(recorded(run, second["snapshot_id"])["outcome"] == "exact_match", "Matching file not recorded")
        require(web.count(same["url"]) == 1, "Completed pilot unit requested again")

    scenario("full_run_continues_after_passed_pilots", full)

    def changed_stops() -> None:
        result = recorded(run, changed["snapshot_id"])
        require(result["outcome"] == "changed_needs_review" and result["fresh_sha256"] != result["stored_sha256"], "Changed bytes not flagged")
        require(harness.outcome(run, after_change["snapshot_id"]) is None and web.count(after_change["url"]) == 0, "Source kept downloading after a change")
        require("BETA" in harness.stopped_sources(run), "Changed source not stopped")
        stored_body = (base / "stored" / changed["snapshot_id"] / "raw/changed.csv").read_bytes()
        require(stored_body == b"a,b\n1,2\n", "Stored capture replaced")

    scenario("changed_file_needs_review_stops_its_source_and_keeps_the_stored_copy", changed_stops)

    def unavailable() -> None:
        result = recorded(run, missing["snapshot_id"])
        require(result["outcome"] == "unavailable" and result["attempts"] == 1 and "http_404" in result["reason"], f"404 outcome differs: {result}")
        require("GAMMA" in harness.stopped_sources(run), "Unavailable source not stopped")

    scenario("http_404_is_unavailable_without_retry", unavailable)

    def paused() -> None:
        require(harness.outcome(run, busy["snapshot_id"]) is None and web.count(busy["url"]) == 1, "HTTP 429 retried or given an outcome")
        require("DELTA" in harness.paused_sources(run), "HTTP 429 did not pause the source")

    scenario("http_429_pauses_the_source_without_retry_or_outcome", paused)

    def retried() -> None:
        result = recorded(run, flaky["snapshot_id"])
        require(result["outcome"] == "exact_match" and result["attempts"] == 2 and web.count(flaky["url"]) == 2, f"Retry outcome differs: {result}")
        result = recorded(run, failing["snapshot_id"])
        require(result["outcome"] == "unavailable" and result["attempts"] == 2 and web.count(failing["url"]) == 2, f"Attempt cap differs: {result}")

    scenario("transient_failure_retried_once_and_never_more_than_two_attempts", retried)

    def inactive() -> None:
        for entry in (duplicate, dropped, derived):
            require(web.count(entry["url"]) == 0 and harness.outcome(run, entry["snapshot_id"]) is None, "Inactive entry requested")

    scenario("duplicate_dropped_and_derived_entries_never_requested", inactive)

    def private_original() -> None:
        result = recorded(run, private["snapshot_id"])
        path = root / "state" / result["fresh_path"]
        require(result["outcome"] == "exact_match" and path.relative_to(root / "state").parts[0] == "private", "Private original outside the private folder")
        require(stat.S_IMODE(path.stat().st_mode) == 0o600 and stat.S_IMODE(path.parent.stat().st_mode) == 0o700, "Private original not owner-only")
        text = "\n".join(p.read_text(errors="ignore") for p in (root / "state").rglob("*.json"))
        require("Synthetic Person" not in text, "Personal detail copied into a record")
        require(
            # The collector failure scenario in this run also keeps a private diagnostic, listed beside the original.
            read_json(root / "state" / "private_retention.json")["originals"] == sorted([f"private/{private['snapshot_id']}", "private/diagnostics"]),
            "Private original missing from the retention record",
        )

    scenario("private_original_owner_only_and_listed_for_deletion", private_original)

    def adapters() -> None:
        require(recorded(run, adapted["snapshot_id"])["outcome"] == "exact_match", "Collector match not recorded")
        result = recorded(run, adapted_changed["snapshot_id"])
        require(result["outcome"] == "changed_needs_review" and result["differing_data_files"] == ["adapted.csv"], f"Collector change differs: {result}")
        result = recorded(run, adapted_error["snapshot_id"])
        require(result["outcome"] == "blocked", "Collector failure not blocked")
        reason = result["reason"]
        require(SENSITIVE_TEXT not in reason and str(Path.home()) not in reason and "X-Amz-Signature=abc" not in reason, f"Reason not sanitized: {reason}")

    scenario("collector_units_compare_derived_data_and_sanitize_failures", adapters)

    def paged_units() -> None:
        require(recorded(run, paged["snapshot_id"])["outcome"] == "exact_match", "Matching pages not recorded")
        result = recorded(run, paged_changed["snapshot_id"])
        require(result["outcome"] == "changed_needs_review" and result["differing_data_files"] == ["page_0002.json"], f"Changed page differs: {result}")

    scenario("paged_api_capture_compared_page_by_page", paged_units)

    def rerun() -> None:
        before = {p: p.read_bytes() for p in sorted((root / "state" / "outcomes").glob("*.json"))}
        requests = len(web.requests)
        harness.execute(run)
        after = {p: p.read_bytes() for p in sorted((root / "state" / "outcomes").glob("*.json"))}
        require(before == after, "Rerun changed or added outcomes")
        require(len(web.requests) == requests, "Rerun repeated a request or ignored a durable pause")

    scenario("rerun_keeps_outcomes_and_does_not_retry_paused_units", rerun)

    def manual_flow() -> None:
        require(harness.outcome(run, manual["snapshot_id"]) is None and web.count(manual["url"]) == 0, "Manual unit requested or given an outcome")
        require(manual["snapshot_id"] in harness.report(run)["pending_manual"], "Missing manual file not reported as waiting")
        path = manual_folder / "manual.csv"
        path.write_bytes(b"m\n1\n")
        set_origin(path, [manual["url"]])
        old = time.time() - 86400
        os.utime(path, (old, old))
        harness.execute(run)
        require(harness.outcome(run, manual["snapshot_id"]) is None, "File older than the run accepted as a fresh download")
        path.unlink()
        path.write_bytes(b"m\n1\n")
        set_origin(path, ["https://elsewhere.example.org/manual.csv"])
        harness.execute(run)
        require(harness.outcome(run, manual["snapshot_id"]) is None, "File from another origin accepted")
        set_origin(path, [manual["url"]])
        harness.execute(run)
        require(recorded(run, manual["snapshot_id"])["outcome"] == "exact_match", "Fresh manual download not accepted")

    scenario("manual_unit_waits_then_accepts_only_a_fresh_file_from_its_origin", manual_flow)

    def rebuilt_archives() -> None:
        same_content = manual_folder / "productDownload_one.zip"
        same_content.write_bytes(archive({"data.csv": b"n\n1\n", "notes.txt": b"notes"}, 2027))
        set_origin(same_content, [rebuilt["url"]])
        new_content = manual_folder / "productDownload_two.zip"
        new_content.write_bytes(archive({"table.csv": b"x\n2\n"}, 2027))
        set_origin(new_content, [revised["url"]])
        harness.execute(run)
        result = recorded(run, rebuilt["snapshot_id"])
        require(result["outcome"] == "exact_match" and result["fresh_sha256"] != result["stored_sha256"], f"Rebuilt archive outcome differs: {result}")
        result = recorded(run, revised["snapshot_id"])
        require(result["outcome"] == "changed_needs_review" and result["differing_data_files"] == ["table.csv"], f"Revised archive outcome differs: {result}")

    scenario("rebuilt_export_archives_are_compared_member_by_member", rebuilt_archives)

    def resume() -> None:
        harness.resume_source(run, "BETA", "Synthetic review: publisher revised the file; continue")
        harness.execute(run, sources={"BETA"})
        require(recorded(run, after_change["snapshot_id"])["outcome"] == "exact_match", "Resumed source did not continue")
        require(recorded(run, changed["snapshot_id"])["outcome"] == "changed_needs_review", "Resume rewrote the changed outcome")

    scenario("recorded_decision_resumes_a_stopped_source_without_rewriting_outcomes", resume)

    def byte_cap() -> None:
        capped = make_run("capped", byte_cap=10)
        harness.execute(capped)

    scenario("byte_cap_stops_before_the_download", byte_cap, "Byte cap would be exceeded")

    def pilot_gate() -> None:
        require(harness.outcome(run, busy_next["snapshot_id"]) is None and web.count(busy_next["url"]) == 0, "Pair ran before its pilot had an outcome")

    scenario("pair_without_a_pilot_outcome_does_not_run", pilot_gate)

    def quota() -> None:
        now = datetime.now(UTC)
        old_root, new_root = root / "quota_old", root / "quota_new"
        for index in range(3):
            write_once(old_root / "requests" / f"{index:06}.json", encoded_json({"reserved_at_utc": (now - timedelta(hours=1)).isoformat()}))
        write_once(old_root / "requests" / "000009.json", encoded_json({"reserved_at_utc": (now - timedelta(hours=30)).isoformat()}))
        write_once(new_root / "requests" / "000001.json", encoded_json({"reserved_at_utc": (now - timedelta(minutes=5)).isoformat()}))
        require(harness.bls_recent_requests([old_root, new_root], now) == 4, "Old and new ledgers not counted together")
        harness.require_bls_budget([old_root, new_root], now, limit=5)
        harness.require_bls_budget([old_root, new_root], now, limit=4)

    scenario("bls_budget_counts_old_and_new_ledgers_together", quota, "BLS rolling 24-hour budget reached")

    def report() -> None:
        # Later scenarios refroze the shared queue's controls for their own roots; a report reads only the bound root (failure mode 63).
        control_e2e.approve_fixture(run)
        document = harness.report(run)
        files = [read_json(p) for p in sorted((root / "state" / "outcomes").glob("*.json"))]
        counts: dict[str, int] = {}
        for item in files:
            counts[item["outcome"]] = counts.get(item["outcome"], 0) + 1
        require(document["outcomes"] == counts and document["model_eligible"] is False and document["s3_writes"] == 0, f"Report differs: {document}")
        require(document["active_units"] == 19 and sum(counts.values()) + len(document["not_attempted"]) == 19, "Report does not account for every unit")

    scenario("report_counts_match_the_outcome_files", report)
    real_bls_adapter(scenario, root)
    real_private_adapter(scenario, root)
    content_scenarios(scenario, root)
    real_wonder_adapter(scenario, root)
    control_e2e.exercise(scenario, root, Fixture, Web)
    return scenarios


def real_bls_adapter(scenario: Any, root: Path) -> None:
    """The real BLS adapter runs the real collector into a fresh root with storage off and a request cap."""
    batch: dict = {"series_ids": ["LAUCN010010000000003"], "start_year": 2000, "end_year": 2000}
    batch["id"] = bls_contract.batch_id(batch)
    plan = {
        "source_id": "BLS",
        "vintage": "2000-01-01",
        "endpoint": "https://api.bls.gov/publicAPI/v2/timeseries/data/",
        "credential_reference": "bls_api_key",
        "request_limit_24h": 5,
        "request_spacing_seconds": 0,
        "registry_sha256": canonical_hash(load_registry()),
        "references": [],
        "batches": [batch],
        "model_eligible": False,
    }
    base = root / "bls_base"
    plan_path, versions = base / "plan.json", base / "versions.json"
    write_once(plan_path, encoded_json(plan))
    write_once(plan_path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    write_once(versions, encoded_json({"versions": [{"version": "synthetic", "code_sha256": bls_contract.code_hashes()}]}))
    payload = bls_fixture(batch)
    calls: list[int] = []

    def request(*_args: Any) -> bytes:
        calls.append(1)
        return payload

    with patch.object(bls_contract, "PLAN_PATH", plan_path), patch.object(bls_contract, "VERSIONS_PATH", versions):
        old_root = base / "old"
        bls_execute(plan, batch, old_root, True, False, FakeS3(), {}, request)
        receipt = old_root / "batches" / batch["id"] / "capture/receipt.json"
        entry = {
            "snapshot_id": read_json(receipt)["snapshot_id"],
            "source_id": "BLS",
            "receipt": str(receipt.relative_to(base)),
            "receipt_sha256": harness.digest(receipt.read_bytes()),
            "url": plan["endpoint"],
            "mode": "bls_api_v2",
            "bytes": 1,
            "pages": 1,
            "disposition": "api",
            "route": "publisher_api",
            "flags": [],
        }
        fixture = Fixture(base)
        fixture.entries.append(entry)
        queue = fixture.freeze()
        run = harness.Run(queue_path=queue, state_root=root / "bls_state", base=base, api_request=request, client_factory=FakeS3, sleep=lambda _s: None)
        control_e2e.approve_fixture(run)

        def matches() -> None:
            before = copy.deepcopy({p: p.read_bytes() for p in sorted(old_root.rglob("*")) if p.is_file() and p.name != ".collection.lock"})
            harness.execute(run)
            result = recorded(run, entry["snapshot_id"])
            require(result["outcome"] == "exact_match" and len(calls) == 2, f"Real BLS adapter outcome differs: {result}")
            after = {p: p.read_bytes() for p in sorted(old_root.rglob("*")) if p.is_file() and p.name != ".collection.lock"}
            require(before == after, "Fresh run changed the stored collection root")
            fresh = root / "bls_state" / "collectors" / "bls_api_v2"
            require(not list(fresh.rglob("completed.json")) and not list(fresh.rglob("s3_collections_reconciliation.json")), "Fresh run stored to S3")

        scenario("real_bls_collector_runs_in_a_fresh_root_with_storage_off", matches)


def real_private_adapter(scenario: Any, root: Path) -> None:
    """The real ONC adapter keeps the fresh original private and blocks a file that no longer matches its plan."""
    base = root / "onc_base"
    base.mkdir()
    data = onc_e2e.body()
    plan, plan_path = onc_e2e.locked(base, data)
    versions = base / "versions.json"
    write_once(versions, encoded_json({"versions": [{"code_sha256": bls_contract.code_hashes()}]}))
    with patch.object(onc_contract, "PLAN_PATH", plan_path), patch.object(onc_contract, "VERSIONS_PATH", versions):
        old_root = base / "old"
        onc_execute(plan, old_root, True, False, None, {}, onc_e2e.Web(data), onc_e2e.LIMITS)
        receipt = old_root / "batches" / plan["id"] / "capture/receipt.json"
        entry = {
            "snapshot_id": read_json(receipt)["snapshot_id"],
            "source_id": "ONC_PI",
            "receipt": str(receipt.relative_to(base)),
            "receipt_sha256": harness.digest(receipt.read_bytes()),
            "url": plan["url"],
            "mode": "onc_mu_hospital",
            "bytes": len(data),
            "pages": 1,
            "disposition": "direct_download",
            "route": "publisher_url",
            "flags": ["original_contains_personal_details:private_owner_only"],
        }
        fixture = Fixture(base)
        fixture.entries.append(entry)
        queue = fixture.freeze()

        def matches() -> None:
            run = harness.Run(queue_path=queue, state_root=root / "onc_state", base=base, opener=onc_e2e.Web(data), sleep=lambda _s: None)
            control_e2e.approve_fixture(run)
            harness.execute(run)
            result = recorded(run, entry["snapshot_id"])
            require(result["outcome"] == "exact_match", f"Real ONC adapter outcome differs: {result}")
            private_root = root / "onc_state" / "collectors" / "onc_mu_hospital" / "private_original"
            originals = list(private_root.rglob("*.csv"))
            require(len(originals) == 1 and stat.S_IMODE(originals[0].stat().st_mode) == 0o600, "Fresh original not owner-only")
            retention = read_json(root / "onc_state" / "private_retention.json")["originals"]
            require(retention == ["collectors/onc_mu_hospital/private_original"], f"Private folder not listed for deletion: {retention}")
            outside = "\n".join(
                p.read_bytes().decode("utf-8", "ignore") for p in (root / "onc_state").rglob("*") if p.is_file() and "private_original" not in p.parts
            )
            require(not any(npi in outside for npi in onc_e2e.CLINICIAN_NPIS), "Clinician data outside the private original")

        scenario("real_onc_collector_keeps_the_fresh_original_private", matches)

        def changed() -> None:
            revised = onc_e2e.body(lambda rows: rows.append(list(rows[1])))
            run = harness.Run(queue_path=queue, state_root=root / "onc_changed", base=base, opener=onc_e2e.Web(revised), sleep=lambda _s: None)
            control_e2e.approve_fixture(run)
            harness.execute(run)
            result = recorded(run, entry["snapshot_id"])
            require(result["outcome"] == "blocked" and result["reason"] == "collector_validation_failed", f"Changed original outcome differs: {result}")
            retention = read_json(root / "onc_changed" / "private_retention.json")["originals"]
            expected = ["collectors/onc_mu_hospital/private_original", "private/diagnostics"]
            require(retention == expected, f"Rejected private original or its diagnostic not listed for deletion: {retention}")
            require("ONC_PI" in harness.stopped_sources(run), "Changed original did not stop its source")

        scenario("real_onc_collector_blocks_a_file_that_no_longer_matches_its_plan", changed)


# Stored and fresh bodies for the content scenarios: each fresh body differs from its stored body in the noted way. Only
# the three fixed formats are compared by content: a Census table-notes member, a stored WONDER export and an Office
# file's core properties [S34].
CENSUS_NOTES = "ACSST5Y2024.S1701-Table-Notes.txt"
WONDER_EXPORT = "wonder_D158_2024_2024.txt"
# The published layouts [S47] to [S49]: Census notes end with a Suggested Citation block between dividers; a WONDER
# export's trailer has dataset, help, Query Date and citation sections before its caveats.
DIVIDER = b"-" * 109
CITATION = (
    b'  U.S. Census Bureau. \\"Poverty Status\\" American Community Survey, ACS 5-Year Estimates Subject Tables, Table S1701, 2024, '
    b"https://data.census.gov/table/ACSST5Y2024.S1701?q=S1701: Accessed on February 10, 2026."
)
NOTE_BLOCK = b"Table Notes\n\nEstimates are subject to sampling error.\n\n"
CITATION_BLOCK = DIVIDER + b"\n\nSuggested Citation\n\n" + CITATION + b"\n  \n" + DIVIDER + b"\n"
DATED = NOTE_BLOCK + CITATION_BLOCK
NOTES = (
    b'"Dataset: Synthetic"\n"Query Parameters:"\n"Year/Month: 2024"\n"---"\n'
    b'"Help: See http://wonder.cdc.gov/wonder/help/ucd.html for more information."\n"---"\n'
    b'"Query Date: Sep 28, 2026 11:17:22 PM"\n"---"\n'
    b'"Suggested Citation: Centers for Disease Control and Prevention."\n'
    b'"Program. Accessed at http://wonder.cdc.gov/x.html on Sep 28, 2026 11:17:22 PM"\n"---"\n'
    b'Caveats:\n"1. Synthetic caveat."\n'
)


def page(text: str, **changes: str) -> bytes:
    """A saved web page: visible text, saved tab state, links, image text, a review date, page code and a data script."""
    v = {
        "state": "nav-link active",
        "link": "/Portal/",
        "reviewed": "Friday, October 02, 2026",
        "script": "2",
        "asset": "a1",
        "alt": "County map",
        "target": "/files/data.csv",
        "data": '{"rows": 1}',
    } | changes
    return (
        f'<!DOCTYPE html><html><head><link rel="stylesheet" href="/site.css?{v["asset"]}"><style>p{{color:red}}</style>'
        f'<script>var t="{v["script"]}";</script><script type="application/json">{v["data"]}</script></head><body>'
        f'<a class="{v["state"]}" href="{v["link"]}">Home</a><a href="{v["target"]}">Data</a><img src="/chart.png" alt="{v["alt"]}">'
        f"<p>{text}</p><!-- saved {v['script']} --><p>This page last reviewed: {v['reviewed']}</p></body></html>"
    ).encode()


def core(stamp: str, description: str = "Data") -> bytes:
    """An Office file's core properties with the generator's build time and a description."""
    return (
        '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
        f"<dc:description><![CDATA[{description}]]></dc:description>"
        f'<dcterms:created xsi:type="dcterms:W3CDTF">{stamp}</dcterms:created><dcterms:modified xsi:type="dcterms:W3CDTF">{stamp}</dcterms:modified>'
        "</cp:coreProperties>"
    ).encode()


def content_scenarios(scenario: Any, root: Path) -> None:
    """The same content downloaded on another date matches; any other difference stays a change; exclusions are named [S17 to S38]."""
    base = root / "content_base"
    base.mkdir()
    fixture, web = Fixture(base), Web()
    dated = fixture.stored("RHO", CENSUS_NOTES, DATED, disposition="approved_manual")
    saved = fixture.stored("SIGMA", "page.html", page("Crosswalk files"), disposition="approved_manual")
    edited = fixture.stored("TAU", "edited.html", page("Edited page"), disposition="approved_manual")
    row = fixture.stored("UPSILON", "rows.txt", b"u\n1\nAccessed on February 10, 2026.\n", disposition="approved_manual")
    elsewhere = fixture.stored("PHI", "other.html", page("Other page"), disposition="approved_manual")
    zipped = fixture.stored("CHI", "export.zip", archive({"data.csv": b"c\n1\n", CENSUS_NOTES: DATED}, 2026), disposition="approved_manual")
    built = archive({"xl/sheet1.xml": b"<row>1</row>", "docProps/core.xml": core("2026-09-23T05:46:17Z")}, 2026)
    workbook = fixture.stored("BETH", "metadata.xlsx", built, disposition="approved_manual")
    other_book = archive({"xl/sheet1.xml": b"<row>3</row>", "docProps/core.xml": core("2026-09-23T05:46:17Z")}, 2026)
    rebuilt_book = fixture.stored("GIMEL", "book.xlsx", other_book, disposition="approved_manual")
    latin = fixture.stored("PSI", "latin.txt", b"caf\xe9\nAccessed on February 10, 2026.\n", disposition="approved_manual")
    direct = fixture.stored("OMEGA", WONDER_EXPORT, b'd,1\n"---"\n' + NOTES)
    collected = fixture.stored("ALEPH", WONDER_EXPORT, b'k\n1\n"---"\n' + NOTES, disposition="api", mode="synthetic_collector")
    # Review round 1 adversarial cases [S26 to S29].
    alt_text = fixture.stored("KAPH", "alt.html", page("Maps"), disposition="approved_manual")
    target = fixture.stored("LAMED", "target.html", page("Files"), disposition="approved_manual")
    data_script = fixture.stored("MEM", "data.html", page("Data"), disposition="approved_manual")
    data_cell = fixture.stored("TETH", "teth.csv", b"name,accessed\nx,Accessed on February 10, 2026\n", disposition="approved_manual")
    above_trailer = fixture.stored("ZAYIN", "zayin.txt", b'"r","Query Date: Sep 28, 2026 11:17:22 PM"\n"---"\n' + NOTES, disposition="approved_manual")
    commented = fixture.stored("HE", "commented.zip", archive({"data.csv": b"h\n1\n"}, 2026, b"Footnote 1"), disposition="approved_manual")
    copied = fixture.stored("NUN", "nun.txt", b"n\n1\n", disposition="approved_manual")
    same_names = fixture.stored("SAMEKH", "samekh.zip", archive({"t.csv": b"s\n1\n"}, 2026), disposition="approved_manual")
    # Review round 2 adversarial cases [S30 to S33].
    script_change = fixture.stored("QOPH", "script.html", page("Rates"), disposition="approved_manual")
    asset_change = fixture.stored("RESH", "asset.html", page("Assets"), disposition="approved_manual")
    link_case = fixture.stored("SHIN", "case.html", page("Links"), disposition="approved_manual")
    notes_csv = fixture.stored("TAV", "notes.csv", b"name,accessed\nx,U.S. Census Bureau. Accessed on February 10, 2026\n", disposition="approved_manual")
    other_path = fixture.stored("PE", "pe.txt", b"p\n1\n", disposition="approved_manual")
    # Review round 5 adversarial cases [S44] [S45]: core properties keep comments, processing instructions and namespaces;
    # a stored copy edited after capture is never compared by content.
    commented_core = core("2026-09-23T05:46:17Z").replace(b"<dc:description>", b"<!-- v1 --><dc:description>")
    with_comment = fixture.stored("ZAYIN2", "comment.xlsx", archive({"docProps/core.xml": commented_core}, 2026), disposition="approved_manual")
    with_pi = fixture.stored(
        "PE2", "pi.xlsx", archive({"docProps/core.xml": b'<?app v="1"?>' + core("2026-09-23T05:46:17Z")}, 2026), disposition="approved_manual"
    )
    namespaced = fixture.stored("QOPH2", "ns.xlsx", archive({"docProps/core.xml": core("2026-09-23T05:46:17Z")}, 2026), disposition="approved_manual")
    tampered = fixture.stored("RESH2", "tampered.txt", b"s\n1\n", disposition="approved_manual")
    (base / "stored" / tampered["snapshot_id"] / "raw" / "tampered.txt").write_bytes(b"s\n2\n")
    # Review round 4 adversarial cases [S39 to S43].
    near_core = fixture.stored("BET2", "near.xlsx", archive({"notdocProps/core.xml": core("2026-09-23T05:46:17Z")}, 2026), disposition="approved_manual")
    dir_payload = fixture.stored("DALET", "dirdata.zip", archive({"notes/": b"", "d.csv": b"t\n1\n"}, 2026), disposition="approved_manual")
    appended = fixture.stored("HE2", "pagezip.html", b"<html>A</html>" + archive({"d.csv": b"a\n1\n"}, 2026), disposition="approved_manual")
    described = fixture.stored(
        "VAV",
        "described.xlsx",
        archive({"docProps/core.xml": core("2026-09-23T05:46:17Z", '<dcterms:created xsi:type="dcterms:W3CDTF">2026-09-23T05:46:17Z</dcterms:created>')}, 2026),
        disposition="approved_manual",
    )
    # Review round 3 adversarial cases [S34 to S37].
    caveat = fixture.stored("AYIN", WONDER_EXPORT, b'w,1\n"---"\n' + NOTES + b'"Caveat: data as of Sep 28, 2026."\n', disposition="approved_manual")
    # Review round 7 adversarial cases [S47] to [S49]: a second stamp, a stamp outside its section, sections out of order
    # and changed citation wording; each download differs from the stored copy only where the old masking looked.
    second_citation = DATED.replace(b"sampling error.\n", b"sampling error.\n" + CITATION.replace(b"February 10", b"January 5") + b"\n")
    two_citations = fixture.stored("KAPH2", CENSUS_NOTES, second_citation, disposition="approved_manual")
    note_stamp = DATED.replace(b"sampling error.", b"Revised. Accessed on January 5, 2026.")
    stamped_note = fixture.stored("LAMED2", CENSUS_NOTES, note_stamp, disposition="approved_manual")
    moved_block = CITATION_BLOCK + NOTE_BLOCK
    citation_first = fixture.stored("MEM2", CENSUS_NOTES, moved_block, disposition="approved_manual")
    reworded = fixture.stored("NUN2", CENSUS_NOTES, DATED, disposition="approved_manual")
    caveat_query = NOTES + b'"2. Query Date: Sep 28, 2026 11:17:22 PM"\n'
    query_in_caveats = fixture.stored("SAMEKH2", WONDER_EXPORT, b'w,1\n"---"\n' + caveat_query, disposition="approved_manual")
    # The citation section moved after the caveats.
    citation_section = (
        b'"Suggested Citation: Centers for Disease Control and Prevention."\n"Program. Accessed at http://wonder.cdc.gov/x.html on Sep 28, 2026 11:17:22 PM"\n'
    )
    citation_last = NOTES.replace(citation_section + b'"---"\n', b"") + b'"---"\n' + citation_section
    citation_after_caveats = fixture.stored("AYIN2", WONDER_EXPORT, b'w,1\n"---"\n' + citation_last, disposition="approved_manual")
    query_extra = NOTES.replace(b'"Query Date: Sep 28, 2026 11:17:22 PM"\n', b'"Query Date: Sep 28, 2026 11:17:22 PM"\n"Note: revised"\n')
    query_section_extra = fixture.stored("PE3", WONDER_EXPORT, b'w,1\n"---"\n' + query_extra, disposition="approved_manual")
    # Review round 8 adversarial cases [S51] [S52]: a note heading inside the citation section, a blank-prefixed caveat
    # section before the Query Date, and a byte-order mark added to notes, a notes member and Office core properties.
    caveat_in_citation = NOTES.replace(b'"Program. Accessed at', b'Caveats:\n"Program. Accessed at')
    citation_caveat = fixture.stored("TSADE2", WONDER_EXPORT, b'w,1\n"---"\n' + caveat_in_citation, disposition="approved_manual")
    help_line = b'"Help: See http://wonder.cdc.gov/wonder/help/ucd.html for more information."\n"---"\n'
    early_caveats = NOTES.replace(help_line, help_line + b'\nCaveats:\n"9. Early note."\n"---"\n')
    blank_caveats = fixture.stored("QOPH3", WONDER_EXPORT, b'w,1\n"---"\n' + early_caveats, disposition="approved_manual")
    bom_stamped = fixture.stored("RESH3", CENSUS_NOTES, DATED, disposition="approved_manual")
    plain_notes = NOTE_BLOCK + DIVIDER + b"\n"
    bom_plain = fixture.stored("SHIN3", CENSUS_NOTES, plain_notes, disposition="approved_manual")
    bom_member = fixture.stored("TAV3", "bom.zip", archive({"data.csv": b"c\n1\n", CENSUS_NOTES: plain_notes}, 2026), disposition="approved_manual")
    bom_core = fixture.stored("ALEPH3", "bom.xlsx", archive({"docProps/core.xml": core("2026-09-23T05:46:17Z")}, 2026), disposition="approved_manual")
    # Review round 9 [S53]: a digit-shaped build time that is not a real UTC time is content.
    not_a_time = fixture.stored("BETH3", "time.xlsx", archive({"docProps/core.xml": core("2026-13-45T05:46:17Z")}, 2026), disposition="approved_manual")
    # Review round 10 [S54]: build times count only in Office core properties with the Office root and namespace bindings.

    def unofficial(stamp: str, variant: str) -> bytes:
        body = core(stamp)
        if variant == "root":
            return body.replace(b"<cp:coreProperties ", b"<data ").replace(b"</cp:coreProperties>", b"</data>")
        uri = b"http://schemas.openxmlformats.org/package/2006/metadata/core-properties" if variant == "cp" else b"http://www.w3.org/2001/XMLSchema-instance"
        return body.replace(uri, b"urn:synthetic:published-data")

    foreign = {
        variant: fixture.stored(
            f"{name}3", f"{variant}.xlsx", archive({"docProps/core.xml": unofficial("2026-09-23T05:46:17Z", variant)}, 2026), disposition="approved_manual"
        )
        for name, variant in (("GIMEL", "root"), ("DALET", "cp"), ("HE", "xsi"))
    }
    directory = fixture.stored(
        "TSADE", "directory.zip", archive({"notes/": b"", "d.csv": b"t\n1\n"}, 2026, entry_comments={"notes/": b"Footnote 1"}), disposition="approved_manual"
    )
    queue = fixture.freeze()
    later = NOTES.replace(b"Sep 28, 2026 11:17:22 PM", b"Oct 9, 2026 3:04:21 PM")
    web.serve(direct["url"], (200, b'd,1\n"---"\n' + later))

    def adapter(unit: dict, run: Any) -> Path:
        """A stand-in collector whose derived file carries a later query time than the stored one."""
        folder = run.state_root / "collectors" / unit["mode"] / unit["snapshot_id"]
        (folder / "derived").mkdir(parents=True, exist_ok=True)
        body = b'k\n1\n"---"\n' + later
        (folder / "derived" / WONDER_EXPORT).write_bytes(body)
        artifact = {"role": "data", "stored_file_name": WONDER_EXPORT, "storage_path": f"derived/{WONDER_EXPORT}", "sha256": harness.digest(body)}
        (folder / "receipt.json").write_bytes(encoded_json({"artifacts": [artifact]}))
        return folder / "receipt.json"

    folder = root / "content_manual"
    folder.mkdir()
    run = harness.Run(
        queue_path=queue,
        state_root=root / "content_state",
        base=base,
        manual_folder=folder,
        opener=web,
        adapters={"synthetic_collector": adapter},
        sleep=lambda _s: None,
    )
    control_e2e.approve_fixture(run)
    harness.execute(run)
    downloads = [
        (dated, CENSUS_NOTES, DATED.replace(b"February 10", b"March 15")),
        (saved, "page.html", page("Crosswalk files", state="nav-link", reviewed="Tuesday, October 06, 2026")),
        (edited, "edited.html", page("Edited page, revised")),
        (row, "rows.txt", b"u\n2\nAccessed on March 15, 2026.\n"),
        (zipped, "productDownload.zip", archive({"data.csv": b"c\n1\n", CENSUS_NOTES: DATED.replace(b"February 10", b"March 15")}, 2027)),
        (latin, "latin.txt", b"caf\xe9\nAccessed on March 15, 2026.\n"),
        (workbook, "metadata.xlsx", archive({"xl/sheet1.xml": b"<row>1</row>", "docProps/core.xml": core("2026-10-08T05:46:01Z")}, 2027)),
        (rebuilt_book, "book.xlsx", archive({"xl/sheet1.xml": b"<row>2</row>", "docProps/core.xml": core("2026-10-08T05:46:01Z")}, 2027)),
        (alt_text, "alt.html", page("Maps", alt="County map (revised)")),
        (target, "target.html", page("Files", target="/files/data2.csv")),
        (data_script, "data.html", page("Data", data='{"rows": 2}')),
        (data_cell, "teth.csv", b"name,accessed\nx,Accessed on March 15, 2026\n"),
        (above_trailer, "zayin.txt", b'"r","Query Date: Oct 9, 2026 3:04:21 PM"\n"---"\n' + later),
        (commented, "commented.zip", archive({"data.csv": b"h\n1\n"}, 2027, b"Footnote 2")),
        (script_change, "script.html", page("Rates", script="6")),
        (caveat, "ayin_export.txt", b'w,1\n"---"\n' + later + b'"Caveat: data as of Oct 9, 2026."\n'),
        (near_core, "near.xlsx", archive({"notdocProps/core.xml": core("2026-10-08T05:46:01Z")}, 2027)),
        (
            with_comment,
            "comment.xlsx",
            archive({"docProps/core.xml": commented_core.replace(b"v1", b"v2").replace(b"2026-09-23T05:46:17Z", b"2026-10-08T05:46:01Z")}, 2027),
        ),
        (with_pi, "pi.xlsx", archive({"docProps/core.xml": b'<?app v="2"?>' + core("2026-10-08T05:46:01Z")}, 2027)),
        (
            namespaced,
            "ns.xlsx",
            archive({"docProps/core.xml": core("2026-10-08T05:46:01Z").replace(b"2001/XMLSchema-instance", b"2001/XMLSchema-other")}, 2027),
        ),
        (tampered, "tampered.txt", b"s\n2\n"),
        (dir_payload, "dirdata.zip", archive({"notes/": b"x", "d.csv": b"t\n1\n"}, 2027)),
        (appended, "pagezip.html", b"<html>B</html>" + archive({"d.csv": b"a\n1\n"}, 2026)),
        (
            described,
            "described.xlsx",
            archive(
                {"docProps/core.xml": core("2026-09-23T05:46:17Z", '<dcterms:created xsi:type="dcterms:W3CDTF">2026-10-08T05:46:01Z</dcterms:created>')}, 2027
            ),
        ),
        (directory, "directory.zip", archive({"notes/": b"", "d.csv": b"t\n1\n"}, 2027, entry_comments={"notes/": b"Footnote 2"})),
        (asset_change, "asset.html", page("Assets", asset="b2")),
        (link_case, "case.html", page("Links", link="/portal/")),
        (notes_csv, "notes.csv", b"name,accessed\nx,U.S. Census Bureau. Accessed on March 15, 2026\n"),
        (two_citations, "kaph2_notes.txt", second_citation.replace(b"January 5", b"March 1")),
        (stamped_note, "lamed2_notes.txt", note_stamp.replace(b"January 5", b"March 1")),
        (citation_first, "mem2_notes.txt", moved_block.replace(b"February 10", b"March 15")),
        (reworded, "nun2_notes.txt", DATED.replace(b"February 10", b"March 15").replace(b", 2024, https", b", 2023, https")),
        (
            query_in_caveats,
            "samekh2_export.txt",
            b'w,1\n"---"\n' + caveat_query.replace(b'"2. Query Date: Sep 28, 2026 11:17:22 PM"', b'"2. Query Date: Oct 9, 2026 3:04:21 PM"'),
        ),
        (citation_after_caveats, "ayin2_export.txt", b'w,1\n"---"\n' + citation_last.replace(b"Sep 28, 2026 11:17:22 PM", b"Oct 9, 2026 3:04:21 PM")),
        (citation_caveat, "tsade2_export.txt", b'w,1\n"---"\n' + caveat_in_citation.replace(b"Sep 28, 2026 11:17:22 PM", b"Oct 9, 2026 3:04:21 PM")),
        (blank_caveats, "qoph3_export.txt", b'w,1\n"---"\n' + early_caveats.replace(b"Sep 28, 2026 11:17:22 PM", b"Oct 9, 2026 3:04:21 PM")),
        (bom_stamped, "resh3_notes.txt", b"\xef\xbb\xbf" + DATED),
        (bom_plain, "shin3_notes.txt", b"\xef\xbb\xbf" + plain_notes),
        (bom_member, "bom.zip", archive({"data.csv": b"c\n1\n", CENSUS_NOTES: b"\xef\xbb\xbf" + plain_notes}, 2026)),
        (not_a_time, "time.xlsx", archive({"docProps/core.xml": core("2026-14-45T05:46:17Z")}, 2027)),
        *((entry, f"{variant}.xlsx", archive({"docProps/core.xml": unofficial("2026-10-08T05:46:01Z", variant)}, 2026)) for variant, entry in foreign.items()),
        (bom_core, "bom.xlsx", archive({"docProps/core.xml": b"\xef\xbb\xbf" + core("2026-09-23T05:46:17Z")}, 2026)),
        (query_section_extra, "pe3_export.txt", b'w,1\n"---"\n' + query_extra.replace(b"Sep 28, 2026 11:17:22 PM", b"Oct 9, 2026 3:04:21 PM")),
    ]
    for entry, name, body in downloads:
        (folder / name).write_bytes(body)
        set_origin(folder / name, [entry["url"]])
    (folder / "tract.html").write_bytes(page("Tract page"))
    set_origin(folder / "tract.html", [elsewhere["url"].replace("other.html", "tract.html")])
    # Same host, other addresses: identical bytes, and a changed archive with the stored member names [S29].
    (folder / "nun_copy.txt").write_bytes(b"n\n1\n")
    set_origin(folder / "nun_copy.txt", [copied["url"].replace("nun.txt", "nun_copy.txt")])
    (folder / "samekh_other.zip").write_bytes(archive({"t.csv": b"s\n2\n"}, 2027))
    set_origin(folder / "samekh_other.zip", [same_names["url"].replace("samekh.zip", "samekh_other.zip")])
    # Identical bytes from the unit's path with another ';' segment, and from another port [S32].
    (folder / "pe_segment.txt").write_bytes(b"p\n1\n")
    set_origin(folder / "pe_segment.txt", [other_path["url"] + ";v=2"])
    (folder / "pe_zero_port.txt").write_bytes(b"p\n1\n")
    set_origin(folder / "pe_zero_port.txt", [other_path["url"].replace("publisher.example.org", "publisher.example.org:0")])
    (folder / "pe_port.txt").write_bytes(b"p\n1\n")
    set_origin(folder / "pe_port.txt", [other_path["url"].replace("publisher.example.org", "publisher.example.org:8443")])
    harness.execute(run)

    def matches() -> None:
        result = recorded(run, workbook["snapshot_id"])
        require(result["outcome"] == "exact_match" and result["excluded_parts"] == ["document_build_time"], f"Same content not matched: {result}")

    scenario("same_content_on_another_date_matches_with_named_exclusions", matches)

    def dated_text_by_bytes() -> None:
        # Census table notes and WONDER exports compare by bytes: a new access or query date is flagged for review [S53].
        for entry in (dated, zipped, direct, collected):
            result = recorded(run, entry["snapshot_id"])
            require(result["outcome"] == "changed_needs_review" and result["excluded_parts"] == [], f"Dated text not flagged: {result}")
        zipped_result = recorded(run, zipped["snapshot_id"])
        require(zipped_result["differing_data_files"] == [CENSUS_NOTES], f"Notes member not named: {zipped_result}")

    scenario("census_notes_and_wonder_exports_with_a_new_date_are_flagged_for_review", dated_text_by_bytes)

    def changes() -> None:
        # A saved page compares by bytes, so a changed tab state and review date are a change for review [S35].
        for entry, name in ((saved, "page.html"), (edited, "edited.html"), (row, "rows.txt"), (latin, "latin.txt")):
            result = recorded(run, entry["snapshot_id"])
            require(result["outcome"] == "changed_needs_review" and result["differing_data_files"] == [name], f"Change not flagged: {result}")
            require(result["excluded_parts"] == [] and result["fresh_path"].endswith(name), f"Changed file not kept for review: {result}")

        result = recorded(run, rebuilt_book["snapshot_id"])
        require(result["outcome"] == "changed_needs_review" and result["differing_data_files"] == ["xl/sheet1.xml"], f"Workbook change not flagged: {result}")

    scenario("changed_text_next_to_a_masked_date_and_undecodable_text_stay_changes", changes)

    def review_cases() -> None:
        cases = (
            (alt_text, "alt.html"),
            (target, "target.html"),
            (data_script, "data.html"),
            (data_cell, "teth.csv"),
            (above_trailer, "zayin.txt"),
            (commented, "(archive comment)"),
            (script_change, "script.html"),
            (asset_change, "asset.html"),
            (link_case, "case.html"),
            (notes_csv, "notes.csv"),
            (caveat, WONDER_EXPORT),
            (near_core, "notdocProps/core.xml"),
            (with_comment, "docProps/core.xml"),
            (with_pi, "docProps/core.xml"),
            (namespaced, "docProps/core.xml"),
            (tampered, "(stored copy differs from its receipt)"),
            (dir_payload, "notes/"),
            (appended, "pagezip.html"),
            (described, "docProps/core.xml"),
            (directory, "notes/ (comment)"),
            (two_citations, CENSUS_NOTES),
            (stamped_note, CENSUS_NOTES),
            (citation_first, CENSUS_NOTES),
            (reworded, CENSUS_NOTES),
            (query_in_caveats, WONDER_EXPORT),
            (citation_after_caveats, WONDER_EXPORT),
            (query_section_extra, WONDER_EXPORT),
            (citation_caveat, WONDER_EXPORT),
            (blank_caveats, WONDER_EXPORT),
            (bom_stamped, CENSUS_NOTES),
            (bom_plain, CENSUS_NOTES),
            (bom_member, CENSUS_NOTES),
            (bom_core, "docProps/core.xml"),
            (not_a_time, "docProps/core.xml"),
            *((entry, "docProps/core.xml") for entry in foreign.values()),
        )
        for entry, name in cases:
            result = recorded(run, entry["snapshot_id"])
            require(result["outcome"] == "changed_needs_review" and result["differing_data_files"] == [name], f"Review case not flagged: {result}")

    scenario("dates_in_data_and_page_text_links_scripts_and_archive_comments_stay_changes", review_cases)

    def elsewhere_pending() -> None:
        for entry in (elsewhere, copied, same_names, other_path):
            require(harness.outcome(run, entry["snapshot_id"]) is None, f"A file from another address on the same host was attributed: {entry['source_id']}")
            require(entry["snapshot_id"] in harness.report(run)["pending_manual"], "Unattributed unit not reported as waiting")

    scenario("a_changed_file_from_another_address_is_never_attributed", elsewhere_pending)

    def reported() -> None:
        document = harness.report(run)
        counts = {"document_build_time": 1}
        require(document["matched_with_excluded_parts"] == counts, f"Excluded parts not counted: {document['matched_with_excluded_parts']}")
        written = b"".join(p.read_bytes() for p in (root / "content_state" / "outcomes").glob("*.json"))
        written += encoded_json(document)
        require(not any(text in written for text in (b"Crosswalk", b"revised", b"March 15", b"Oct 9", b"Footnote")), "Source text in outcomes or the report")

    scenario("reports_count_excluded_parts_and_carry_no_source_text", reported)

    def repeat() -> None:
        before = {p: p.read_bytes() for p in sorted((root / "content_state" / "outcomes").glob("*.json"))}
        harness.execute(run)
        require(before == {p: p.read_bytes() for p in sorted((root / "content_state" / "outcomes").glob("*.json"))}, "Rerun changed outcomes")

    scenario("content_comparison_rerun_keeps_outcomes", repeat)


def real_wonder_adapter(scenario: Any, root: Path) -> None:
    """The real WONDER collector checks a fresh export under its own hash; only the query time may differ [S21]."""
    base = root / "wonder_base"
    base.mkdir()
    downloads = base / "downloads"
    plan = build_wonder_plan(wonder_e2e.downloads_for(downloads), min_counties_per_year=wonder_e2e.FLOOR)
    plan_path, versions = base / "plan.json", base / "versions.json"
    write_once(plan_path, encoded_json(plan))
    write_once(plan_path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
    write_once(versions, encoded_json({"versions": [{"code_sha256": bls_contract.code_hashes()}]}))
    batch = next(item for item in plan["batches"] if item["file_name"] == "pilot_d158.xls")
    stored_body = (downloads / batch["file_name"]).read_bytes()
    with patch.object(wonder_contract, "PLAN_PATH", plan_path), patch.object(wonder_contract, "VERSIONS_PATH", versions):
        wonder_execute(plan, batch, base / "old", downloads, False, None, {})
        receipt = base / "old" / "batches" / batch["id"] / "capture/receipt.json"
        entry = {
            "snapshot_id": read_json(receipt)["snapshot_id"],
            "source_id": "WONDER",
            "receipt": str(receipt.relative_to(base)),
            "receipt_sha256": harness.digest(receipt.read_bytes()),
            "url": "https://wonder.cdc.gov/ucd-icd10-expanded.html",
            "mode": "wonder_export",
            "bytes": len(stored_body),
            "pages": 1,
            "disposition": "approved_manual",
            "route": "manual_browser_download",
            "flags": [],
        }
        fixture = Fixture(base)
        fixture.entries.append(entry)
        queue = fixture.freeze()
        origins = [wonder_e2e.SESSION.format(db="D158"), "https://wonder.cdc.gov/controller/datarequest/D158"]

        def compare(label: str, body: bytes) -> dict:
            folder = root / f"wonder_manual_{label}"
            folder.mkdir()
            run = harness.Run(queue_path=queue, state_root=root / f"wonder_{label}", base=base, manual_folder=folder, sleep=lambda _s: None)
            control_e2e.approve_fixture(run)
            harness.execute(run)
            (folder / batch["file_name"]).write_bytes(body)
            set_origin(folder / batch["file_name"], origins)
            harness.execute(run)
            return recorded(run, entry["snapshot_id"])

        later = stored_body.replace(b"Query Date: Jan 1, 2026 12:00:00 AM", b"Query Date: Oct 9, 2026 3:04:21 PM")

        def same() -> None:
            require(later != stored_body, "Fixture query time did not change")
            result = compare("same", later)
            # The export compares by bytes, so a new query time is flagged; its parsed rows are unchanged [S53].
            require(result["outcome"] == "changed_needs_review" and result["excluded_parts"] == [], f"Real WONDER outcome differs: {result}")
            require(result["differing_data_files"] == ["wonder_D158_2024_2024.txt"], f"Parsed rows changed with the query time: {result}")
            require(batch["sha256"] == harness.digest(stored_body), "The planned hash changed")

        scenario("real_wonder_collector_flags_an_export_that_differs_only_in_query_time", same)

        def revised() -> None:
            body = later.replace(b'"120"\t"10000"', b'"121"\t"10000"', 1)
            result = compare("revised", body)
            require(result["outcome"] == "changed_needs_review", f"Revised WONDER export not flagged: {result}")
            require(result["differing_data_files"] == ["county_year.csv", "wonder_D158_2024_2024.txt"], f"Differing files differ: {result}")

        scenario("real_wonder_collector_flags_a_changed_row_next_to_a_new_query_time", revised)


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="redownload_e2e_") as directory, download_metadata():
        scenarios = exercise(Path(directory))
    passed = all(s["passed"] for s in scenarios)
    report = {
        "status": "passed" if passed else "failed",
        "feature": "Bounded redownload harness: locked queue, fresh state root, no storage, comparison with stored captures",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "code_sha256": bls_contract.code_hashes(),
        "dependency_sha256": bls_contract.digest(Path("requirements.txt").read_bytes()),
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_redownload_e2e --output {args.output}",
        "boundary": (
            "Synthetic stored captures and queue; fake publisher opener and fake collectors; "
            "one real BLS collector run with a fake request; no live publisher or AWS."
        ),
        "cleanup": "Temporary folder removed on exit.",
        "scenarios": scenarios,
    }
    write_once(args.output, encoded_json(report))
    sys.stdout.write(
        json.dumps({"status": report["status"], "passed": sum(s["passed"] for s in scenarios), "total": len(scenarios), "artifact": str(args.output)}) + "\n"
    )
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
