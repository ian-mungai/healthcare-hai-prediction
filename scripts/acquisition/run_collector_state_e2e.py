"""Exercise durable collector state with synthetic S3 versions and real transport boundaries."""

from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import importlib
import json
import platform
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.acquisition import batch, bls_api_transport, collect_bls_api
from scripts.acquisition.s3_store import AwsCli, StorageError, encoded_json, store_file, write_once
from scripts.process import CompletedProcess

NOW = datetime(2026, 10, 25, 12, tzinfo=UTC)


class SyntheticClock:
    """Keep request reservation timestamps fixed across repeatability runs."""

    @staticmethod
    def now(_zone: object = None) -> datetime:
        """Return the synthetic UTC timestamp without reading a wall clock."""
        return NOW


class MemoryObjects:
    """Versioned byte storage with atomic conditions and deterministic injected failures."""

    def __init__(self, module: Any) -> None:
        self.module = module
        self.versions: dict[str, list[Any]] = {}
        self.lock = threading.Lock()
        self.fault: tuple[str, str] | None = None
        self.barrier: threading.Barrier | None = None
        self.calls: list[dict] = []

    def get(self, key: str, version_id: str | None = None, *, deadline: float) -> Any:
        with self.lock:
            self.calls.append({"operation": "get", "key": key, "version_id": version_id})
            candidates = self.versions.get(key, [])
            selected = next((item for item in candidates if item.version_id == version_id), None) if version_id else next(iter(candidates[-1:]), None)
            if selected is None:
                raise self.module.StateMissing("Pinned state object missing")
            return selected

    def put(self, key: str, body: bytes, *, if_match: str | None, if_none_match: bool, deadline: float) -> Any:
        if self.barrier is not None and key.endswith("quotas/bls_v2.json"):
            self.barrier.wait(timeout=5)
        with self.lock:
            self.calls.append({"operation": "put", "key": key, "if_match": if_match, "if_none_match": if_none_match})
            fault = self.fault[1] if self.fault and self.fault[0] in key else None
            if fault and fault != "persistent_outage":
                self.fault = None
            if fault == "denied":
                raise self.module.StateUnavailable("Synthetic AccessDenied")
            if fault in {"outage", "persistent_outage"}:
                raise self.module.StateUnavailable("Synthetic SlowDown", retryable=True)
            if fault == "conflict":
                raise self.module.StateConflict("Conditional state write conflicted")
            if fault == "crash_before":
                raise RuntimeError("Synthetic process stopped before write")
            if fault == "uncertain_before":
                raise self.module.UnconfirmedWrite("Synthetic unconfirmed state write")
            current = next(iter(self.versions.get(key, [])[-1:]), None)
            if if_none_match == (if_match is not None) or if_none_match and current or if_match is not None and (not current or current.etag != if_match):
                raise self.module.StateConflict("Conditional state write conflicted")
            sequence = sum(len(items) for items in self.versions.values()) + 1
            stored = self.module.StoredObject(key, f"version_{sequence}", f'"etag_{sequence}"', body)
            self.versions.setdefault(key, []).append(stored)
            if fault == "crash_after":
                raise RuntimeError("Synthetic process stopped after write")
            if fault == "altered_readback":
                self.versions[key][-1] = self.module.StoredObject(key, stored.version_id, stored.etag, b'{"altered":true}')
            if fault == "lost_response":
                raise self.module.UnconfirmedWrite("Synthetic unconfirmed state write")
            return stored


def ensure(value: Any, message: str) -> None:
    """Fail a case at its observable contract boundary."""
    if not value:
        raise AssertionError(message)


def refused(action: Any, expected: str) -> None:
    """Require the specific refusal rather than an unrelated exception."""
    try:
        action()
    except ValueError as error:
        ensure(expected in str(error), f"Expected {expected!r}, observed {str(error)!r}")
    else:
        raise AssertionError(f"Expected refusal: {expected}")


def exercise(root: Path) -> list[dict]:
    """Drive admissions, persisted reservations, checkpoints and replacement scratch."""
    module = importlib.import_module("scripts.acquisition.collector_state")
    scenarios: list[dict] = []
    permitted = (
        "plan.json",
        "completed.json",
        "partial.json",
        "progress.json",
        "batches/synthetic/transport.json",
        "batches/synthetic/capture/receipt.json",
        "batches/synthetic/completed.json",
        "batches/synthetic/capture/s3_collections_reconciliation.json",
        "batches/synthetic/capture/s3_collections/bls/manifest.json",
    )

    def backend(
        store: MemoryObjects | None = None,
        name: str = "collector",
        owner: str = "first",
        target: Path | None = None,
        binding_changes: dict[str, str] | None = None,
    ) -> Any:
        binding = module.StateBinding("epoch_1", name, "reviewed_logical_root", "a" * 64, "b" * 64, permitted)
        binding = module.StateBinding(**(vars(binding) | (binding_changes or {})))
        return module.CollectorState(
            store or MemoryObjects(module), binding, module.Owner(owner, f"synthetic_task_{owner}"), target or root / name, time.monotonic() + 30
        )

    def prepared(name: str, v1: list[dict] | None = None, v2: list[dict] | None = None) -> Any:
        state = backend(name=name)
        state.prepare(v1 or [], v2 or [], inventory_complete=True, now=NOW)
        return state

    def case(name: str, action: Any) -> None:
        began = time.monotonic()
        try:
            action()
            scenarios.append({"name": name, "status": "passed", "expected": "contract assertions pass", "observed": "contract assertions pass"})
        except (AssertionError, ValueError, OSError, RuntimeError, KeyError, TypeError) as error:
            scenarios.append({"name": name, "status": "failed", "expected": "contract assertions pass", "observed": f"{type(error).__name__}: {error}"})
        scenarios[-1]["seconds"] = round(time.monotonic() - began, 4)

    def imports() -> None:
        state = backend(name="imports")
        refused(lambda: state.prepare([], [], inventory_complete=False, now=NOW), "complete quota inventory")
        ensure(not state.store.versions, "Incomplete import created state")
        future = {"reservation_id": "future", "reserved_at_utc": (NOW + timedelta(seconds=1)).isoformat(), "batch_id": "synthetic"}
        refused(lambda: state.prepare([], [future], inventory_complete=True, now=NOW), "clock")
        future["reserved_at_utc"] = NOW.replace(tzinfo=None).isoformat()
        refused(lambda: state.prepare([], [future], inventory_complete=True, now=NOW), "clock")
        ensure(not state.store.versions, "Invalid clock created state")
        refused(lambda: state.prepare([{"reserved_requests": 1}], [], inventory_complete=True, now=NOW), "clock or reservation invalid")
        ensure(not state.store.versions, "Invalid imported quota created state")

    case("incomplete_import_and_invalid_clock_refused", imports)

    def initial_files() -> None:
        state = backend(name="initial_files")
        imported = [module.StateFile("plan.json", b'{"locked":true}'), module.StateFile("partial.json", b'{"reserved_bytes":123}')]
        state.prepare([], [], inventory_complete=True, now=NOW, files=imported)
        slot = json.loads(state.store.get(module.ADMISSION_KEY, deadline=state.deadline).body)
        ensure(slot["owner"] is None, "Prepared import admitted itself")
        state.claim()
        state.hydrate()
        ensure(json.loads((state.root / "partial.json").read_bytes())["reserved_bytes"] == 123, "Import lost unfinished state")
        before = copy.deepcopy(state.store.versions)
        state.prepare([], [], inventory_complete=True, now=NOW, files=imported)
        ensure(before == state.store.versions, "Repeated import duplicated state")
        refused(lambda: state.prepare([], [], inventory_complete=True, now=NOW, files=[module.StateFile("plan.json", b"{}")]), "binding")

    case("reviewed_root_import_stays_nonadmitting_and_preserves_bytes", initial_files)

    def missing_quota_bootstrap() -> None:
        state = prepared("no_bootstrap")
        state.claim()
        state.reserve("bls_v2", "charged", 450, NOW)
        state.store.versions.pop(module.QUOTA_KEYS["bls_v2"])
        refused(lambda: state.prepare([], [], inventory_complete=True, now=NOW), "missing")
        ensure(module.QUOTA_KEYS["bls_v2"] not in state.store.versions, "Preparation silently recreated a lost quota")

    case("existing_authority_cannot_bootstrap_a_missing_quota", missing_quota_bootstrap)

    def missing_admission() -> None:
        for released in (False, True):
            state = prepared("missing_admission_" + str(released))
            state.claim()
            state.checkpoint([module.StateFile("plan.json", b"{}")])
            if released:
                state.release(task_stopped=True)
            state.store.versions.pop(module.ADMISSION_KEY)
            before = copy.deepcopy(state.store.versions)
            refused(lambda state=state: state.prepare([], [], inventory_complete=True, now=NOW), "Admission state missing; reviewed recovery required")
            ensure(before == state.store.versions, "Missing admission reset an existing authority")

    case("missing_admission_cannot_reset_active_or_advanced_state", missing_admission)

    def exclusion() -> None:
        state = prepared("exclusive")
        with bls_api_transport.collection_lock(state.root, state_backend=state):
            refused(lambda: backend(state.store, name="exclusive", owner="second").claim(), "already active")
            other = backend(state.store, name="other", owner="second")
            other.prepare([], [], inventory_complete=True, now=NOW)
            refused(other.claim, "already active")
        refused(lambda: backend(state.store, name="exclusive", owner="second").claim(), "already active")
        refused(lambda: state.release(task_stopped=False), "confirmed task stop")
        state.release(task_stopped=True)
        replacement = backend(state.store, name="exclusive", owner="second", target=root / "replacement")
        replacement.claim()
        ensure(replacement.hydrate() == [], "Empty imported root gained content")

    case("second_run_and_cross_collector_admission_refused", exclusion)

    def rehydrate() -> None:
        state = prepared("hydrate")
        state.claim()
        state.checkpoint([module.StateFile("plan.json", encoded_json({"plan_sha256": "a" * 64}))])
        state.checkpoint([module.StateFile("partial.json", encoded_json({"pending": True, "reserved_bytes": 99}))])
        reference = state.head["checkpoint"]
        state.checkpoint([module.StateFile("completed.json", encoded_json({"status": "stored", "model_eligible": False}), "completion", (reference,))])
        versions = copy.deepcopy(state.store.versions)
        fresh = backend(state.store, name="hydrate", target=root / "fresh")
        fresh.resume()
        ensure(set(fresh.hydrate()) == {"plan.json", "partial.json", "completed.json"}, "Replacement lost checkpoint paths")
        ensure(json.loads((fresh.root / "partial.json").read_bytes())["reserved_bytes"] == 99, "Replacement reset partial reservation")
        ensure(json.loads((fresh.root / "completed.json").read_bytes())["model_eligible"] is False, "Replacement lost the modeling hold")
        fresh.hydrate()
        fresh.checkpoint([module.StateFile("partial.json", encoded_json({"pending": True, "reserved_bytes": 99}))])
        ensure(versions == state.store.versions, "Idempotent replay created versions")
        refused(lambda: fresh.checkpoint([module.StateFile("plan.json", b"{}")]), "write-once")
        for field, value in (("logical_root", "other_root"), ("authority_epoch", "epoch_2"), ("plan_sha256", "c" * 64), ("code_sha256", "d" * 64)):
            changed = backend(state.store, name="hydrate", binding_changes={field: value})
            refused(changed.resume, "binding")

    case("fresh_scratch_restores_complete_and_partial_state", rehydrate)

    def damaged() -> None:
        state = prepared("damaged")
        state.claim()
        state.checkpoint([module.StateFile("plan.json", b"{}")])
        fresh = backend(state.store, name="damaged", target=root / "bad_scratch")
        fresh.resume()
        manifest_ref = state.head["checkpoint"]
        manifest_object = state.store.get(manifest_ref["key"], manifest_ref["version_id"], deadline=state.deadline)
        manifest = json.loads(manifest_object.body)
        artifact = manifest["files"][0]["object"]
        saved = state.store.versions.pop(artifact["key"])
        refused(fresh.hydrate, "missing")
        ensure(not fresh.root.exists(), "Missing artifact materialized a root")
        state.store.versions[artifact["key"]] = saved
        original = saved[0]
        saved[0] = module.StoredObject(original.key, original.version_id, original.etag, b'{"altered":true}')
        refused(fresh.hydrate, "checksum")
        ensure(not fresh.root.exists(), "Corrupt artifact materialized a root")
        saved[0] = original
        state.store.versions.pop(state.head_key)
        refused(fresh.resume, "missing")
        refused(lambda: fresh.reserve("bls_v2", "synthetic", 450, NOW), "missing")
        refused(lambda: fresh.prepare([], [], inventory_complete=True, now=NOW), "missing")

    case("missing_or_corrupt_pinned_version_blocks_resume", damaged)

    def unsafe() -> None:
        state = prepared("unsafe")
        state.claim()
        for path in ("../escape.json", "private_original/file.json", "raw/archive.zip", "unknown.json"):
            refused(lambda path=path: state.checkpoint([module.StateFile(path, b"{}")]), "permitted")
        refused(lambda: state.checkpoint([module.StateFile("plan.json", b'{"registrationkey":"synthetic"}')]), "metadata")
        before = copy.deepcopy(state.store.versions)
        refused(lambda: state.checkpoint([module.StateFile("plan.json", b"invalid JSON")]), "Invalid state JSON")
        cache = {
            "endpoint": "synthetic",
            "request": {},
            "retrieved_at_utc": NOW.isoformat(),
            "sha256": "a" * 64,
            "bytes": 1,
            "body_hex": "zz",
            "http_status": 200,
        }
        refused(
            lambda: state.checkpoint([module.StateFile("batches/synthetic/transport.json", encoded_json(cache), "public_cache")]),
            "Public cache metadata invalid",
        )
        ensure(before == state.store.versions, "Invalid checkpoint input wrote state")
        state.checkpoint([module.StateFile("plan.json", b"{}")])
        fresh = backend(state.store, name="unsafe", target=root / "symlink_scratch")
        fresh.resume()
        fresh.root.mkdir()
        (fresh.root / "plan.json").symlink_to(root / "outside.json")
        refused(fresh.hydrate, "symlink")
        ensure(not (root / "outside.json").exists(), "Hydration followed symlink")

    case("undeclared_private_payload_and_symlink_refused", unsafe)

    def completion() -> None:
        state = prepared("completion")
        state.claim()
        absent = {"key": "datasets/synthetic/data.json", "version_id": "absent", "sha256": "0" * 64, "bytes": 1}
        refused(lambda: state.checkpoint([module.StateFile("completed.json", b"{}", "completion", (absent,))]), "missing")
        ensure(not state.manifest["files"], "Unverified completion became authoritative")
        refused(lambda: state.checkpoint([module.StateFile("completed.json", b"{}", "completion")]), "completion versions")

    case("completion_requires_every_pinned_version", completion)

    def final_versions() -> None:
        state = prepared("final_versions")
        state.claim()
        state.checkpoint([module.StateFile("plan.json", b"{}")])
        manifest = json.loads(state.store.get(state.head["checkpoint"]["key"], state.head["checkpoint"]["version_id"], deadline=state.deadline).body)
        state.store.versions.pop(manifest["files"][0]["object"]["key"])
        fresh = backend(state.store, name="final_versions", target=root / "unrestorable")
        refused(fresh.resume, "missing")
        refused(lambda: state.release(task_stopped=True), "missing")
        refused(lambda: state.reserve("bls_v2", "after_prune", 450, NOW), "missing")
        slot = json.loads(state.store.get(module.ADMISSION_KEY, deadline=state.deadline).body)
        ensure(slot["owner"] is not None, "Missing final artifact released admission")

    case("pruned_final_artifact_blocks_resume_reservation_and_release", final_versions)

    def lost_and_conflicting() -> None:
        state = prepared("conflicts")
        state.claim()
        state.store.fault = ("heads/current.json", "lost_response")
        state.checkpoint([module.StateFile("plan.json", b"{}")])
        ensure(state.head["generation"] == 1, "Lost successful head response lost the generation")
        fresh = backend(state.store, name="conflicts", target=root / "conflict_fresh")
        fresh.resume()
        state.checkpoint([module.StateFile("partial.json", b"{}")])
        call_count = len(state.store.calls)
        stopped = False
        try:
            fresh.checkpoint([module.StateFile("progress.json", b"{}")])
        except module.StateError:
            stopped = True
        ensure(not any(call["operation"] == "put" for call in state.store.calls[call_count:]), "Stale writer performed a put after its head changed")
        ensure(stopped, "Stale writer did not stop")
        before = copy.deepcopy(state.store.versions[state.head_key])
        state.store.fault = ("heads/current.json", "conflict")
        refused(lambda: state.checkpoint([module.StateFile("progress.json", b"{}")]), "conflict")
        ensure(before == state.store.versions[state.head_key], "Conflict overwrote the winner")
        refused(lambda: state.reserve("bls_v2", "blocked", 450, NOW), "blocked")

    case("lost_success_reconciles_and_stale_writer_stops", lost_and_conflicting)

    def altered_write() -> None:
        state = prepared("altered_write")
        state.claim()
        state.store.fault = ("heads/current.json", "altered_readback")
        refused(lambda: state.checkpoint([module.StateFile("plan.json", b"{}")]), "write readback checksum")

    case("altered_write_readback_blocks_checkpoint", altered_write)

    def interrupted_admission() -> None:
        state = prepared("half_claim")
        state.store.fault = ("heads/current.json", "crash_before")
        try:
            state.claim()
        except RuntimeError:
            pass
        else:
            raise AssertionError("Claim did not stop between its writes")
        refused(lambda: backend(state.store, name="half_claim", owner="second").claim(), "already active")
        fresh = backend(state.store, name="half_claim")
        fresh.resume()
        fresh.reserve("bls_v2", "after_recovery", 450, NOW)
        for index, key in enumerate((module.ADMISSION_KEY, "heads/current.json")):
            selected = prepared("half_release_" + str(index))
            selected.claim()
            selected.store.fault = (key, "crash_after")
            try:
                selected.release(task_stopped=True)
            except RuntimeError:
                pass
            else:
                raise AssertionError("Release did not stop between its writes")
            refused(lambda selected=selected: backend(selected.store, name=selected.binding.collector_id, owner="second").claim(), "already active")
            recovered = backend(selected.store, name=selected.binding.collector_id)
            refused(lambda recovered=recovered: recovered.release(task_stopped=False), "confirmed task stop")
            recovered.release(task_stopped=True)
            successor = backend(selected.store, name=selected.binding.collector_id, owner="second")
            successor.claim()
            successor.reserve("bls_v2", "successor", 450, NOW)

    case("interrupted_claim_and_release_recover_without_takeover", interrupted_admission)

    def half_claim_release() -> None:
        state = prepared("release_half_claim")
        state.store.fault = ("heads/current.json", "crash_before")
        try:
            state.claim()
        except RuntimeError as error:
            ensure("stopped before write" in str(error), "Claim stopped at an unrelated boundary")
        else:
            raise AssertionError("Claim did not stop after admission")
        fresh = backend(state.store, name="release_half_claim")
        before = copy.deepcopy(state.store.versions)
        refused(lambda: fresh.release(task_stopped=True), "call resume() before release()")
        ensure(before == state.store.versions, "Half-claim release changed durable state")
        refused(lambda: backend(state.store, name="release_half_claim", owner="second").resume(), "binding")
        fresh.resume()
        fresh.release(task_stopped=True)
        successor = backend(state.store, name="release_half_claim", owner="second")
        successor.claim()
        successor.reserve("bls_v1", "after_controller_recovery", 25, NOW)

    case("half_claim_release_directs_same_owner_to_resume", half_claim_release)

    def concurrent_reservations() -> None:
        state = prepared("concurrent")
        state.claim()
        second = backend(state.store, name="concurrent", target=root / "concurrent_other")
        second.resume()
        state.store.barrier = threading.Barrier(2)

        def reserve(selected: Any) -> str:
            try:
                selected.reserve("bls_v2", "synthetic", 1, NOW)
                return "reserved"
            except module.StateConflict:
                return "conflicted"

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(reserve, (state, second)))
        state.store.barrier = None
        ensure(sorted(results) == ["conflicted", "reserved"], "Racing quota writes both admitted")
        ledger = json.loads(state.store.get(module.QUOTA_KEYS["bls_v2"], deadline=state.deadline).body)
        ensure(len(ledger["reservations"]) == 1, "Reservation race lost or duplicated the charge")

    case("concurrent_reservations_have_one_conditional_winner", concurrent_reservations)

    def crash_before_http() -> None:
        imports = [{"reservation_id": f"import_{i}", "batch_id": "imported", "reserved_at_utc": NOW.isoformat()} for i in range(448)]
        imports.extend(
            {"reservation_id": label, "batch_id": "imported", "reserved_at_utc": (NOW - timedelta(seconds=age)).isoformat()}
            for label, age in (("expired", 86401), ("boundary", 86400), ("still_charged", 86399))
        )
        state = prepared("crash", v2=imports)
        state.claim()
        state.reserve("bls_v2", "last", 450, NOW)
        fresh = backend(state.store, name="crash", target=root / "after_crash")
        fresh.resume()
        refused(lambda: fresh.reserve("bls_v2", "new", 450, NOW), "budget")
        ledger = json.loads(state.store.get(module.QUOTA_KEYS["bls_v2"], deadline=state.deadline).body)
        ensure(len(ledger["reservations"]) == 450, "Crash refunded an unused reservation")
        refused(lambda: fresh.reserve("bls_v2", "new", 451, NOW), "cap")
        refused(lambda: fresh.reserve("bls_v2", "new", 450, NOW - timedelta(seconds=1)), "clock")
        fresh = prepared(
            "exact_window", v2=[{"reservation_id": f"exact_{index}", "batch_id": "imported", "reserved_at_utc": NOW.isoformat()} for index in range(450)]
        )
        fresh.claim()
        state = fresh
        refused(lambda: fresh.reserve("bls_v2", "before_day", 450, NOW + timedelta(seconds=86399)), "budget")
        fresh.reserve("bls_v2", "at_day", 450, NOW + timedelta(seconds=86400))
        ledger = json.loads(state.store.get(module.QUOTA_KEYS["bls_v2"], deadline=state.deadline).body)
        ensure(len(ledger["reservations"]) == 1, "Expired quota entries were not pruned at the exact boundary")
        first = ledger["reservations"][0]["reservation_id"]
        fresh.reserve("bls_v2", "at_day", 450, NOW + timedelta(seconds=86400))
        ledger = json.loads(state.store.get(module.QUOTA_KEYS["bls_v2"], deadline=state.deadline).body)
        ensure(
            len({item["reservation_id"] for item in ledger["reservations"]}) == 2 and first != ledger["reservations"][-1]["reservation_id"],
            "Pruning reused a reservation identity",
        )

    case("crash_before_request_preserves_project_wide_cap", crash_before_http)

    def legacy() -> None:
        imported = [
            {"reservation_id": "expired", "reserved_at_utc": (NOW - timedelta(hours=24, seconds=1)).isoformat(), "reserved_requests": 25},
            {"reservation_id": "boundary", "reserved_at_utc": (NOW - timedelta(hours=24)).isoformat(), "reserved_requests": 25},
            {"reservation_id": "imported", "reserved_at_utc": (NOW - timedelta(hours=24) + timedelta(minutes=1)).isoformat(), "reserved_requests": 24},
        ]
        state = prepared("legacy", v1=imported)
        state.claim()
        refused(lambda: batch.reserve_bls_requests(state.root, 3, NOW, state_backend=state), "budget")
        batch.reserve_bls_requests(state.root, 1, NOW, state_backend=state)
        ledger = json.loads(state.store.get(module.QUOTA_KEYS["bls_v1"], deadline=state.deadline).body)
        ensure([item["reserved_requests"] for item in ledger["reservations"]] == [24, 1], "Legacy weights changed")
        state.reserve("bls_v2", "separate", 450, NOW)
        refused(lambda: batch.reserve_bls_requests(state.root, 1, NOW + timedelta(seconds=59), state_backend=state), "budget")
        batch.reserve_bls_requests(state.root, 24, NOW + timedelta(minutes=1), state_backend=state)
        ledger = json.loads(state.store.get(module.QUOTA_KEYS["bls_v1"], deadline=state.deadline).body)
        ensure([item["reserved_requests"] for item in ledger["reservations"]] == [1, 24], "Legacy exact expiry lost a live weight or retained history")
        weighted = prepared("weighted", v1=[dict(imported[-1], reserved_requests=3)])
        weighted.claim()
        fresh = backend(weighted.store, name="weighted")
        fresh.resume()
        batch.reserve_bls_requests(fresh.root, 22, NOW, state_backend=fresh)
        refused(lambda: batch.reserve_bls_requests(fresh.root, 1, NOW, state_backend=fresh), "budget")

    case("legacy_transport_preserves_weight_and_separate_cap", legacy)

    def transport() -> None:
        state = prepared("transport")
        state.claim()
        plan = {"endpoint": "https://api.bls.gov/publicAPI/v2/timeseries/data/", "request_limit_24h": 2, "request_spacing_seconds": 0}
        unit = {"id": "synthetic", "series_ids": ["synthetic"], "start_year": 2000, "end_year": 2000}
        calls: list[int] = []

        def request(*arguments: Any) -> bytes:
            ledger = json.loads(state.store.get(module.QUOTA_KEYS["bls_v2"], deadline=state.deadline).body)
            ensure(len(ledger["reservations"]) == len(calls) + 1, "HTTP preceded durable reservation")
            calls.append(1)
            if len(calls) == 1:
                raise bls_api_transport.RetryableRequest("Synthetic lost response")
            return b'{"status":"synthetic_public"}'

        with patch.object(bls_api_transport, "datetime", SyntheticClock):
            bls_api_transport.fetch(plan, unit, state.root, True, AwsCli({}), request=request, sleep=lambda _: None, state_backend=state)
        ensure(len(calls) == 2, "Request retry behavior changed")
        fresh = backend(state.store, name="transport", target=root / "transport_fresh")
        fresh.resume()
        fresh.hydrate()
        bls_api_transport.fetch(plan, unit, fresh.root, False, None, state_backend=fresh)
        ensure(len(calls) == 2, "Hydration repeated an already cached request")
        ensure(not (fresh.root / "requests").exists(), "Cloud transport created an authoritative local ledger")
        with patch.object(bls_api_transport, "datetime", SyntheticClock):
            try:
                bls_api_transport.fetch(plan, unit | {"id": "over_budget"}, fresh.root, True, AwsCli({}), request=request, state_backend=fresh)
            except bls_api_transport.QuotaPause as error:
                ensure("budget" in str(error), "Transport quota pause had an unrelated cause")
            else:
                raise AssertionError("Selected transport did not pause its exhausted quota")
        ensure(len(calls) == 2, "Exhausted selected quota sent another publisher request")

    case("real_fetch_reserves_every_retry_and_rehydrates_cache", transport)

    def outage() -> None:
        state = prepared("outage")
        state.claim()
        state.store.fault = ("quotas/bls_v2.json", "lost_response")
        state.reserve("bls_v2", "confirmed_after_loss", 450, NOW)
        state.store.fault = ("quotas/bls_v2.json", "outage")
        state.reserve("bls_v2", "after_transient", 450, NOW)
        state.store.fault = ("quotas/bls_v2.json", "uncertain_before")
        refused(lambda: state.reserve("bls_v2", "unconfirmed", 450, NOW), "unconfirmed")
        refused(lambda: state.reserve("bls_v2", "again", 450, NOW), "blocked")
        expired = backend(state.store, name="outage")
        expired.deadline = time.monotonic() - 1
        refused(expired.resume, "deadline")

    case("storage_failure_blocks_http_under_absolute_deadline", outage)

    def failed_reservation() -> None:
        def one_fault(fault: str, expected: str) -> None:
            state = prepared("no_http_" + fault)
            state.claim()
            state.store.fault = ("quotas/bls_v2.json", fault)
            calls: list[bytes] = []
            plan = {"endpoint": "https://api.bls.gov/publicAPI/v2/timeseries/data/", "request_limit_24h": 450, "request_spacing_seconds": 0}
            unit = {"id": "synthetic", "series_ids": ["synthetic"], "start_year": 2000, "end_year": 2000}

            def request(*arguments: Any) -> bytes:
                calls.append(b"unexpected")
                return b"{}"

            with patch.object(bls_api_transport, "datetime", SyntheticClock):
                refused(
                    lambda: bls_api_transport.fetch(plan, unit, state.root, True, AwsCli({}), request=request, sleep=lambda _: None, state_backend=state),
                    expected,
                )
            ensure(not calls, "Failed reservation sent HTTP")
            slot = json.loads(state.store.get(module.ADMISSION_KEY, deadline=state.deadline).body)
            ensure(slot["owner"] is not None, "Storage failure released admission")

        for fault, expected in (("conflict", "conflict"), ("uncertain_before", "unconfirmed"), ("denied", "AccessDenied"), ("persistent_outage", "SlowDown")):
            one_fault(fault, expected)

    case("failed_durable_reservation_prevents_real_transport_request", failed_reservation)

    def storage_reads() -> None:
        def one_failure(failure: str) -> None:
            state = prepared("read_" + failure)
            state.claim()
            read = state.store.get
            attempts: list[str] = []
            sleeps: list[float] = []
            requests: list[str] = []
            state.sleep = sleeps.append
            deadline = state.deadline
            before = copy.deepcopy(state.store.versions)

            def get(key: str, version_id: str | None = None, *, deadline: float) -> Any:
                if key == state.head_key:
                    attempts.append(key)
                    if failure != "transient" or len(attempts) == 1:
                        raise module.StateUnavailable("Synthetic read " + failure, retryable=failure != "denied")
                return read(key, version_id, deadline=deadline)

            def request(*_arguments: Any) -> bytes:
                requests.append("request")
                return b"{}"

            with patch.object(state.store, "get", side_effect=get):
                if failure == "transient":
                    state.reserve("bls_v2", "after_read_retry", 450, NOW)
                    ensure(len(attempts) == 2 and len(sleeps) == 1 and not state.blocked, "Transient read did not recover within its retry policy")
                else:
                    plan = {"endpoint": "https://api.bls.gov/publicAPI/v2/timeseries/data/", "request_limit_24h": 450, "request_spacing_seconds": 0}
                    unit = {"id": "synthetic", "series_ids": ["synthetic"], "start_year": 2000, "end_year": 2000}
                    refused(
                        lambda: bls_api_transport.fetch(plan, unit, state.root, True, None, request=request, state_backend=state), "Synthetic read " + failure
                    )
                    ensure(len(attempts) == (3 if failure == "exhausted" else 1), "State read retry count changed")
                    ensure(state.blocked and before == state.store.versions and not requests, "Failed state read permitted effects")
                    slot = json.loads(read(module.ADMISSION_KEY, deadline=deadline).body)
                    ensure(slot["owner"] == state.owner_record, "Failed state read released admission")
                ensure(state.deadline == deadline and all(0 < delay <= 0.1 for delay in sleeps), "State read retry reset its deadline or backoff")

        for failure in ("transient", "exhausted", "denied"):
            one_failure(failure)

    case("state_read_retries_are_bounded_and_failures_prevent_effects", storage_reads)

    def batch_lock() -> None:
        state = prepared("batch_lock")
        state.claim()
        calls: list[str] = []
        with patch.object(batch, "validate_batch", return_value=[]), patch.object(batch, "preflight", side_effect=lambda *_: calls.append("preflight")):
            refused(
                lambda: batch.run_batch({}, state.root, {}, None, object(), {}, execute=True, state_backend=object()),
                "Select a collector state backend explicitly",
            )
            refused(lambda: batch.run_batch({}, state.root, {}, None, object(), {}, execute=True, state_backend=state), "already active")
        ensure(not calls, "Batch preflight ran before cloud admission")

    case("batch_refuses_second_run_before_preflight", batch_lock)

    def executing_batch() -> None:
        job = {
            "job_id": "synthetic_job",
            "plan": {"source_id": "BLS", "mode": "bls_api"},
            "references": [],
            "limits": {"attempts": 3, "max_bytes": 100},
        }
        plan = {"batch_version": 1, "registry_sha256": "c" * 64, "jobs": [job]}
        effects: list[str] = []
        state = prepared("executing_batch")

        def capture(_plan: dict, target: Path, _registry: dict, _validator: Any, _limits: Any) -> Path:
            ledger = json.loads(state.store.get(module.QUOTA_KEYS["bls_v1"], deadline=state.deadline).body)
            ensure(
                [entry["reserved_requests"] for entry in ledger["reservations"]] == [3], "Executing batch did not charge its selected v1 ledger before capture"
            )
            ensure(not (state.root / "quota/bls").exists(), "Selected batch wrote a local quota ledger")
            effects.append("capture")
            receipt = target / "synthetic/receipt.json"
            write_once(receipt, encoded_json({"snapshot_status": "acquired_unvalidated"}))
            return receipt

        def upload(path: Path, *_arguments: Any, **_options: Any) -> dict:
            effects.append("upload")
            reconciliation = path.parent / "reconciliation.json"
            write_once(reconciliation, encoded_json({"objects": []}))
            return {"reconciliation_path": str(reconciliation)}

        with (
            patch.object(batch, "datetime", SyntheticClock),
            patch.object(batch, "validate_batch", return_value=[job]),
            patch.object(batch, "preflight", side_effect=lambda *_: effects.append("preflight")),
            patch.object(batch.s3_store, "load_routes", return_value={"BLS": {"layout_sha256": "d" * 64}}),
            patch.object(batch, "receipt_for_job", return_value=None),
            patch.object(batch, "plan_identity", return_value="synthetic_scope"),
            patch.object(batch, "reuse_identity", return_value="synthetic_scope"),
            patch.object(batch, "upload_snapshot", side_effect=upload),
        ):
            result = batch.run_batch(plan, state.root, {}, None, object(), {}, execute=True, state_backend=state, capture_fn=capture, sleep_fn=lambda _: None)
            ensure([event["status"] for event in result["events"]] == ["stored_unvalidated"], "Executing batch lost its successful job")
            ensure(effects == ["preflight", "capture", "upload"], "Executing batch effects ran out of order")
            ledger = json.loads(state.store.get(module.QUOTA_KEYS["bls_v1"], deadline=state.deadline).body)
            ensure(len(ledger["reservations"]) == 1 and ledger["reservations"][0]["reserved_requests"] == 3, "Executing batch lost the weighted v1 charge")
            ensure(not (state.root / ".execution.lock").exists() and not (state.root / "quota/bls").exists(), "Selected batch used local authority")
            for failure in ("storage", "quota"):
                state = prepared("executing_batch_" + failure)
                effects.clear()
                if failure == "storage":
                    state.store.fault = ("quotas/bls_v1.json", "denied")
                    try:
                        batch.run_batch(
                            plan, state.root, {}, None, object(), {}, execute=True, state_backend=state, capture_fn=capture, sleep_fn=lambda _: None
                        )
                    except module.StateError as error:
                        ensure("Synthetic AccessDenied" in str(error), "Batch raised an unrelated state error")
                    else:
                        raise AssertionError("Batch swallowed StateError instead of stopping the run")
                    ensure(state.blocked, "Storage failure left the batch state usable")
                    ensure(not list(state.root.glob("*/runs/*.json")), "State failure published a batch result")
                else:
                    imported = [{"reservation_id": "full", "reserved_at_utc": NOW.isoformat(), "reserved_requests": 25}]
                    state = prepared("executing_batch_quota_full", v1=imported)
                    result = batch.run_batch(
                        plan, state.root, {}, None, object(), {}, execute=True, state_backend=state, capture_fn=capture, sleep_fn=lambda _: None
                    )
                    ensure(
                        result["events"][0]["status"] == "pending_failure" and "budget" in result["events"][0]["reason"],
                        "Quota refusal changed batch semantics",
                    )
                    ledger = json.loads(state.store.get(module.QUOTA_KEYS["bls_v1"], deadline=state.deadline).body)
                    ensure(ledger["reservations"] == imported, "Quota refusal changed its ledger")
                ensure(effects == ["preflight"], "Failed batch reservation reached capture or upload")
                slot = json.loads(state.store.get(module.ADMISSION_KEY, deadline=state.deadline).body)
                ensure(slot["owner"] == state.owner_record, "Batch failure released durable admission")
                ensure(not (state.root / "quota/bls").exists(), "Failed selected batch wrote a local quota ledger")

    case("executing_batch_charges_selected_ledger_and_propagates_state_errors", executing_batch)

    def selected_collector() -> None:
        state = prepared("selected_collector")
        calls: list[str] = []

        def plan_boundary() -> dict:
            calls.append("plan")
            raise ValueError("Plan boundary reached before admission")

        with patch.object(collect_bls_api.contract, "load_plan", side_effect=plan_boundary):
            refused(lambda: collect_bls_api.execute({}, {}, state.root, False, False, None, {}, state_backend=state), "changed")
        ensure(not calls, "Selected collector reached plan or storage before admission")

    case("selected_collector_requires_admission_before_any_execution", selected_collector)

    def selected_completion() -> None:
        state = prepared("selected_completion")
        state.claim()
        unit = {"id": "synthetic", "series_ids": ["synthetic"], "start_year": 2000, "end_year": 2000}
        plan = {
            "endpoint": "https://api.bls.gov/publicAPI/v2/timeseries/data/",
            "request_limit_24h": 450,
            "request_spacing_seconds": 0,
            "registry_sha256": "c" * 64,
            "batches": [unit],
        }
        requests: list[str] = []
        uploads: list[str] = []
        raw = b'{"synthetic":true}'
        prune_during_upload = False
        receipt = {
            "lineage": {"extraction_or_query": json.dumps({"batch_id": "synthetic"})},
            "acquisition": {"request_parameters": collect_bls_api.contract.request_for(unit)},
            "schema_profile": {"row_count": 1},
            "artifacts": [{"storage_path": "raw/response.json"}, {"storage_path": "derived/observations.csv"}],
        }

        def capture(_plan: dict, _unit: dict, envelope: dict, target: Path) -> Path:
            folder = target / "batches/synthetic/capture"
            write_once(folder / "raw/response.json", bytes.fromhex(envelope["body_hex"]))
            write_once(folder / "derived/observations.csv", b"synthetic\n1\n")
            write_once(folder / "receipt.json", encoded_json(receipt))
            return folder / "receipt.json"

        def verify(path: Path) -> dict:
            ensure((path.parent / "raw/response.json").read_bytes() == raw, "Recovered receipt lost its public source bytes")
            ensure((path.parent / "derived/observations.csv").read_bytes() == b"synthetic\n1\n", "Recovered receipt lost derived bytes")
            return json.loads(path.read_bytes())

        def upload(path: Path, *_arguments: Any) -> dict:
            uploads.append("upload")
            entries = []
            for name in ("raw/response.json", "derived/observations.csv", "receipt.json"):
                body = (path.parent / name).read_bytes()
                key = "synthetic/collection/datasets/bls/" + module.digest(body) + "/" + Path(name).name
                record, _created = store_file(client, path.parent / name, key)
                entries.append({"storage_path": name, "object": record})
            manifest_body = encoded_json({"objects": entries, "model_eligible": False})
            manifest_path = path.parent / "s3_collections/bls/manifest.json"
            write_once(manifest_path, manifest_body)
            key = "synthetic/collection/manifests/bls/" + module.digest(manifest_body) + "/manifest.json"
            record, _created = store_file(client, manifest_path, key, manifest=True)
            reconciliation = {"objects": entries, "manifests": [{"dataset_id": "bls", "object": record}]}
            target = path.parent / "s3_collections_reconciliation.json"
            write_once(target, encoded_json(reconciliation))
            if prune_during_upload:
                state.store.versions.pop(record["key"])
            return {"reconciliation_path": str(target)}

        def request(*_arguments: Any) -> bytes:
            requests.append("request")
            return raw

        class SnapshotClient(AwsCli):
            def call(self, service: str, operation: str, arguments: list[str] | None = None) -> dict:
                ensure(service == "s3api" and arguments is not None, "Snapshot used a non-S3 operation")
                arguments = arguments or []
                key = arguments[arguments.index("--key") + 1]
                try:
                    if operation == "put-object":
                        ensure("--if-none-match" in arguments, "Snapshot omitted conditional creation")
                        body = Path(arguments[arguments.index("--body") + 1]).read_bytes()
                        obj = state.store.put(key, body, if_match=None, if_none_match=True, deadline=state.deadline)
                    else:
                        ensure(operation in {"head-object", "get-object"}, "Snapshot used an unsupported operation")
                        version = arguments[arguments.index("--version-id") + 1] if "--version-id" in arguments else None
                        obj = state.store.get(key, version, deadline=state.deadline)
                        if operation == "get-object":
                            ensure(version is not None, "Snapshot readback omitted its exact version")
                            Path(arguments[-1]).write_bytes(obj.body)
                except module.StateMissing:
                    raise StorageError(operation, "NoSuchKey") from None
                except module.StateConflict:
                    raise StorageError(operation, "PreconditionFailed") from None
                return {
                    "VersionId": obj.version_id,
                    "ContentLength": len(obj.body),
                    "ChecksumSHA256": base64.b64encode(hashlib.sha256(obj.body).digest()).decode(),
                    "ServerSideEncryption": "AES256",
                }

        with (
            patch.object(bls_api_transport, "datetime", SyntheticClock),
            patch.object(collect_bls_api.contract, "load_plan", return_value=plan),
            patch.object(collect_bls_api.contract, "require_code", return_value="b" * 64),
            patch.object(collect_bls_api, "make_receipt", side_effect=capture),
            patch.object(collect_bls_api, "verify_local", side_effect=verify),
            patch.object(collect_bls_api, "verify_storage"),
            patch.object(collect_bls_api, "upload_snapshot", side_effect=upload),
            patch.object(collect_bls_api, "load_registry", return_value={}),
            patch.object(collect_bls_api, "receipt_validator", return_value=None),
        ):
            client = SnapshotClient({"data_bucket_name": "synthetic"})
            client.storage_policy = {"enabled": False}
            ready = collect_bls_api.execute(plan, unit, state.root, True, False, client, {}, request=request, state_backend=state)
            ensure(ready["status"] == "capture_ready", "Capture did not reach the receipt boundary")
            ensure(state.read_file("batches/synthetic/capture/receipt.json") == encoded_json(receipt), "Execute did not checkpoint its receipt")
            fresh = backend(state.store, name="selected_completion", target=root / "receipt_fresh")
            fresh.resume()
            done = collect_bls_api.execute(plan, unit, fresh.root, False, True, client, {}, request=request, state_backend=fresh)
            ensure(done["status"] == "stored" and len(uploads) == 1 and requests == ["request"], "Receipt recovery repeated capture or lost completion")
            ensure(fresh.read_file("batches/synthetic/completed.json") == encoded_json(done), "Execute did not checkpoint completion")
            completed_entry = next(entry for entry in fresh.manifest["files"] if entry["path"] == "batches/synthetic/completed.json")
            ensure(
                completed_entry["kind"] == "completion" and len(completed_entry["required_versions"]) == 4, "Execute completion omitted stored object versions"
            )
            replacement = backend(state.store, name="selected_completion", target=root / "completed_fresh")
            replacement.resume()
            saved = copy.deepcopy(state.store.versions)
            replay = collect_bls_api.execute(plan, unit, replacement.root, False, True, client, {}, request=request, state_backend=replacement)
            ensure(replay == done and requests == ["request"] and len(uploads) == 1, "Completed recovery repeated a publisher request or upload")
            ensure(saved == state.store.versions, "Completed replay changed immutable state")
            manifest_key = next(key for key in state.store.versions if key.startswith("synthetic/collection/manifests/"))
            state.store.versions.pop(manifest_key)
            refused(lambda: backend(state.store, name="selected_completion").resume(), "missing")
            for fresh_scratch in (False, True):
                name = "interrupted_completion_" + str(fresh_scratch)
                state = prepared(name)
                state.claim()
                requests.clear()
                uploads.clear()
                with patch.object(collect_bls_api, "checkpoint_completion", side_effect=RuntimeError("Stopped before completion checkpoint")):
                    try:
                        collect_bls_api.execute(plan, unit, state.root, True, True, client, {}, request=request, state_backend=state)
                    except RuntimeError as error:
                        ensure(str(error) == "Stopped before completion checkpoint", "Completion stopped at an unrelated boundary")
                    else:
                        raise AssertionError("Completion did not stop after upload and local done")
                ensure((state.root / "batches/synthetic/completed.json").exists(), "Crash did not follow local completion")
                ensure(state.read_file("batches/synthetic/completed.json") is None, "Crash followed durable completion")
                stored = {key: copy.deepcopy(versions) for key, versions in state.store.versions.items() if key.startswith("synthetic/collection/")}
                ensure(len(stored) == 4 and all(len(versions) == 1 for versions in stored.values()), "Initial upload duplicated a stored object")
                recovered = backend(state.store, name=name, target=root / (name + "_fresh") if fresh_scratch else state.root)
                recovered.resume()
                ensure(not fresh_scratch or not recovered.root.exists(), "Fresh replacement retained scratch")
                call_count = len(state.store.calls)
                restored = collect_bls_api.execute(plan, unit, recovered.root, False, True, client, {}, request=request, state_backend=recovered)
                ensure(restored["status"] == "stored" and requests == ["request"], "Completion recovery repeated the publisher request")
                ensure(len(uploads) == (2 if fresh_scratch else 1), "Completion recovery exceeded its bounded upload calls")
                ensure(recovered.read_file("batches/synthetic/completed.json") == encoded_json(restored), "Recovery did not checkpoint completion")
                ensure(
                    stored == {key: versions for key, versions in state.store.versions.items() if key in stored},
                    "Recovery duplicated or replaced stored versions",
                )
                ensure(
                    not any(call["operation"] == "put" and call["key"] in stored for call in state.store.calls[call_count:]),
                    "Completion recovery attempted a duplicate stored-object write",
                )
                readbacks = {call["key"] for call in state.store.calls[call_count:] if call["operation"] == "get" and call["version_id"] is not None}
                ensure(set(stored) <= readbacks, "Completion recovery omitted exact-version readback")
                saved = copy.deepcopy(state.store.versions)
                replay = collect_bls_api.execute(plan, unit, recovered.root, False, True, client, {}, request=request, state_backend=recovered)
                ensure(replay == restored and saved == state.store.versions, "Recovered completion replay created another version")
            state = prepared("unconfirmed_completion")
            state.claim()
            prune_during_upload = True
            refused(
                lambda: collect_bls_api.execute(plan, unit, state.root, True, True, client, {}, request=request, state_backend=state),
                "missing",
            )
            ensure(state.read_file("batches/synthetic/completed.json") is None, "Execute published completion without all stored versions")

    case("selected_execute_restores_receipt_and_completion_without_requests", selected_completion)

    def cli_lost_retry() -> None:
        memory = MemoryObjects(module)
        metadata: dict[str, dict[str, str]] = {}
        failure: list[str] = []
        operations: list[str] = []

        def runner(command: list[str], **kwargs: Any) -> CompletedProcess[str]:
            ensure(kwargs["env"]["AWS_MAX_ATTEMPTS"] == "3", "State changed the real CLI retry policy")
            ensure(0 < kwargs["timeout"] <= 30, "CLI lost-write recovery reset its deadline")
            operation = command[2]
            operations.append(operation)
            key = command[command.index("--key") + 1]
            if operation == "put-object":
                value = memory.put(
                    key,
                    Path(command[command.index("--body") + 1]).read_bytes(),
                    if_match=command[command.index("--if-match") + 1] if "--if-match" in command else None,
                    if_none_match="--if-none-match" in command,
                    deadline=time.monotonic() + 30,
                )
                supplied = command[command.index("--metadata") + 1] if "--metadata" in command else ""
                metadata[value.version_id] = dict([supplied.split("=", 1)]) if supplied else {}
                if failure:
                    fault = failure.pop()
                    if fault == "competing":
                        metadata[value.version_id] = {"collector-state-write": "another-operation"}
                    return CompletedProcess(command, 1, "", "An error occurred (PreconditionFailed) when calling PutObject")
            else:
                version = command[command.index("--version-id") + 1] if "--version-id" in command else None
                value = memory.get(key, version, deadline=time.monotonic() + 30)
                Path(command[-1]).write_bytes(value.body)
            return CompletedProcess(
                command, 0, json.dumps({"VersionId": value.version_id, "ETag": value.etag, "Metadata": metadata.get(value.version_id, {})}), ""
            )

        client = AwsCli(
            {"aws_profile": "synthetic", "aws_region": "synthetic", "data_bucket_name": "synthetic", "expected_account_id": "synthetic"}, runner=runner
        )
        selected = backend(name="cli_retry")
        state = module.CollectorState(module.S3StateStore(client), selected.binding, selected.owner, selected.root, selected.deadline)
        state.prepare([], [], inventory_complete=True, now=NOW)
        state.claim()
        failure.append("lost")
        state.reserve("bls_v2", "retry_after_success", 450, NOW)
        ensure(not state.blocked, "Reconciled CLI retry left the writer blocked")
        quota = json.loads(memory.get(module.QUOTA_KEYS["bls_v2"], deadline=state.deadline).body)
        ensure(len(quota["reservations"]) == 1, "CLI retry lost or duplicated a durable charge")
        failure.append("competing")
        refused(lambda: state.reserve("bls_v2", "another_writer", 450, NOW), "conflict")
        ensure(state.blocked and operations.count("get-object") > 0, "Competing CLI write did not retain admission")

        def legacy_runner(command: list[str], **kwargs: Any) -> CompletedProcess[str]:
            ensure(kwargs["timeout"] == 180, "Legacy CLI call changed its existing deadline")
            return CompletedProcess(command, 0, '{"Status":"Enabled"}', "")

        client.runner = legacy_runner
        ensure(client.call("s3api", "get-bucket-versioning") == {"Status": "Enabled"}, "Legacy CLI call lost its parsed response")

    case("real_cli_retry_reconciles_only_its_own_successful_write", cli_lost_retry)

    def local_default() -> None:
        local = root / "local"
        with bls_api_transport.collection_lock(local):
            bls_api_transport.reserve(local, "synthetic", 2, NOW)
            bls_api_transport.reserve(local, "synthetic", 2, NOW)
            refused(lambda: bls_api_transport.reserve(local, "synthetic", 2, NOW), "budget")
        ensure(len(list((local / "requests").glob("*.json"))) == 2, "Local default ledger changed")
        batch.reserve_bls_requests(local, 3, NOW)
        ensure(json.loads(next((local / "quota/bls").glob("*.json")).read_bytes())["reserved_requests"] == 3, "Local legacy weight changed")

    case("unselected_backend_retains_local_behavior", local_default)

    def s3_adapter() -> None:
        memory = MemoryObjects(module)

        class Client(AwsCli):
            def __init__(self) -> None:
                self.error: str | None = None
                self.null_version = False
                self.operations: list[dict] = []

            def call_bounded(self, service: str, operation: str, arguments: list[str] | None = None, *, timeout: float) -> dict:
                ensure(service == "s3api" and arguments is not None, "State adapter called a non-S3 service")
                arguments = arguments or []
                self.operations.append({"operation": operation, "arguments": arguments, "timeout": timeout})
                ensure(0 < timeout <= 30, "State adapter reset its absolute deadline")
                if self.error:
                    raise StorageError(operation, self.error)
                key = arguments[arguments.index("--key") + 1]
                try:
                    if operation == "get-object":
                        version = arguments[arguments.index("--version-id") + 1] if "--version-id" in arguments else None
                        value = memory.get(key, version, deadline=time.monotonic() + timeout)
                        Path(arguments[-1]).write_bytes(value.body)
                    else:
                        ensure(operation == "put-object", "State adapter used an unsupported operation")
                        value = memory.put(
                            key,
                            Path(arguments[arguments.index("--body") + 1]).read_bytes(),
                            if_match=arguments[arguments.index("--if-match") + 1] if "--if-match" in arguments else None,
                            if_none_match="--if-none-match" in arguments,
                            deadline=time.monotonic() + timeout,
                        )
                except module.StateMissing:
                    raise StorageError(operation, "NoSuchKey") from None
                except module.StateConflict:
                    raise StorageError(operation, "PreconditionFailed") from None
                except module.UnconfirmedWrite:
                    raise StorageError(operation, "request_unconfirmed") from None
                except module.StateUnavailable:
                    raise StorageError(operation, "SlowDown") from None
                return {"VersionId": "null" if self.null_version else value.version_id, "ETag": value.etag}

        client = Client()
        store = module.S3StateStore(client)
        selected = backend(name="cli_adapter")
        state = module.CollectorState(store, selected.binding, selected.owner, selected.root, selected.deadline)
        state.prepare([], [], inventory_complete=True, now=NOW)
        state.claim()
        memory.fault = ("quotas/bls_v2.json", "lost_response")
        state.reserve("bls_v2", "through_cli", 450, NOW)
        state.checkpoint([module.StateFile("plan.json", b"{}")])
        state.hydrate()
        ensure(any("--if-match" in item["arguments"] for item in client.operations), "S3 update omitted If-Match")
        ensure(any("--if-none-match" in item["arguments"] for item in client.operations), "S3 creation omitted If-None-Match")
        ensure(any("--version-id" in item["arguments"] for item in client.operations), "S3 readback selected the latest version")
        client.null_version = True
        refused(lambda: store.get(state.head_key, deadline=state.deadline), "version missing")
        client.null_version = False
        for code, expected in (
            ("AccessDenied", "unavailable"),
            ("ConditionalRequestConflict", "conflict"),
            ("SlowDown", "unavailable"),
            ("NoSuchVersion", "missing"),
        ):
            client.error = code
            refused(lambda: store.get(state.head_key, deadline=state.deadline), expected)
        client.error = None
        refused(lambda: store.put("lakehouse/forbidden.json", b"{}", if_match=None, if_none_match=True, deadline=state.deadline), "prefix")
        refused(lambda: store.put(state.head_key, b"{}", if_match=None, if_none_match=False, deadline=state.deadline), "condition")
        refused(lambda: store.get(state.head_key, deadline=time.monotonic() - 1), "deadline")

    case("s3_cli_adapter_conditions_versions_deadlines_and_denials", s3_adapter)
    return scenarios


def main() -> None:
    """Write a repeatable local artifact and return failure for any failed case."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    paths = [
        Path(__file__),
        Path(__file__).with_name("collector_state.py"),
        Path(__file__).with_name("batch.py"),
        Path(__file__).with_name("bls_api_transport.py"),
        Path(__file__).with_name("s3_store.py"),
        Path(__file__).with_name("collect_bls_api.py"),
    ]
    before = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths if path.exists()}
    began = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="collector_state_e2e_") as temporary:
        try:
            cases = exercise(Path(temporary))
        except ModuleNotFoundError as error:
            cases = [{"name": "durable_state_backend_available", "status": "failed", "expected": "collector state backend available", "observed": str(error)}]
    after = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths if path.exists()}
    record = {
        "feature": "collector_durable_state",
        "classification": "Synthetic",
        "created_at_utc": datetime.now(UTC).isoformat(),
        "environment": {"platform": platform.system(), "python": platform.python_version()},
        "code_sha256": before,
        "code_unchanged_during_run": before == after,
        "cases": cases,
        "seconds": round(time.monotonic() - began, 4),
        "status": "passed" if before == after and all(case["status"] == "passed" for case in cases) else "failed",
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_collector_state_e2e --output {args.output}",
        "limits": "In-memory S3 and synthetic requests. No registry, AWS, IAM, ECS, image, network or native acceptance.",
        "cleanup": "done",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(encoded_json(record))
    sys.stdout.write(json.dumps({"status": record["status"], "passed": sum(case["status"] == "passed" for case in cases), "total": len(cases)}) + "\n")
    raise SystemExit(0 if record["status"] == "passed" else 1)


if __name__ == "__main__":
    main()
