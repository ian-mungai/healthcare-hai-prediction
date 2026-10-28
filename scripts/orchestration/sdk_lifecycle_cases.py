"""Check worker failure transitions with synthetic records and controlled dependency failures."""

from __future__ import annotations

import argparse
import base64
import contextlib
import hashlib
import importlib.util
import json
import os
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch
from urllib.parse import quote

from orchestration.catalog_transport import CatalogTransport
from orchestration.ecs_api import EcsApi
from orchestration.sdk_worker import Refused, Sdk, protocol
from orchestration.worker_pool import Worker, WorkerConfig, WorkerPool
from orchestration.worker_recovery import Recovery, WorkerLedger
from scripts.orchestration.sdk_owned_cases import owned_cases, source_hashes
from scripts.process_group import OwnedProcess, Snapshot, StopResult


def _handle(number: int, session: str) -> OwnedProcess:
    return OwnedProcess(number, number, -1, -1, -1, "synthetic_witness", f"synthetic_{number}", session, "control", session, "synthetic", "synthetic")


def recovery_sessions(folder: Path) -> dict[str, bool]:
    """Keep current and active transport records outside previous-session recovery."""
    ledger = WorkerLedger(folder / "registry")
    transport = CatalogTransport(ledger.registry)
    current, active, previous = (_handle(1000001, "current"), _handle(1000002, transport.session), _handle(1000003, "previous"))
    deadline = time.monotonic() + 5
    for handle in (current, active, previous):
        ledger.append_spawn(handle.spawn_record(), deadline=deadline)
    recovery = Recovery(ledger, "current")
    with (
        patch("orchestration.worker_recovery.process_snapshot", return_value=Snapshot("seen")),
        patch("orchestration.worker_recovery.group_state", return_value="gone"),
    ):
        first = recovery.sweep(deadline=deadline)
        second = recovery.sweep(deadline=deadline)
        recovery.refresh_closures(deadline=deadline)
    open_records = ledger.private_open(deadline=deadline)
    events = ledger.redacted(deadline=deadline)
    checks = {
        "first_sweep_only_previous_session": {row["nonce"] for row in first} == {previous.nonce},
        "second_sweep_closes_previous_session": second == [] and previous.nonce in ledger.closed_events(deadline=deadline),
        "current_and_transport_entries_stay_open": {row["nonce"] for row in open_records} == {current.nonce, active.nonce},
        "current_entries_receive_no_recovery_observation": not any(row.get("nonce") in {current.nonce, active.nonce} for row in events),
        "active_transport_does_not_pause_dispatch": not recovery.dispatch_paused,
    }
    transport.close(deadline=deadline)
    with (
        patch("orchestration.worker_recovery.process_snapshot", return_value=Snapshot("seen")),
        patch("orchestration.worker_recovery.group_state", return_value="gone"),
    ):
        retired = recovery.sweep(deadline=deadline)
    checks["closed_transport_session_becomes_recoverable"] = {row["nonce"] for row in retired} == {active.nonce}
    return checks


def ledger_failure_cleanup(folder: Path) -> dict[str, bool]:
    """Attempt every transport stop when the ledger refuses an earlier closure."""
    checks = {}
    for error_type in (OSError, ValueError, TimeoutError):
        transport = CatalogTransport(folder / error_type.__name__)
        transport.handles = [_handle(1000011, transport.session), _handle(1000012, transport.session)]
        stopped: list[int] = []

        def stop(handle: OwnedProcess, deadline: float, stopped: list[int] = stopped) -> StopResult:
            stopped.append(handle.pid)
            return StopResult("confirmed", handle.pid, handle.pgid, True)

        raised = False
        confirmed = True
        with (
            patch("orchestration.catalog_transport.stop_owned", side_effect=stop),
            patch("orchestration.catalog_transport.group_state", return_value="gone"),
            patch.object(transport.ledger, "close", side_effect=error_type("synthetic_ledger_failure")),
        ):
            try:
                confirmed = transport.close(deadline=time.monotonic() + 1)
            except (OSError, ValueError, TimeoutError):
                raised = True
        checks[f"ledger_{error_type.__name__}_continues_cleanup"] = not raised and not confirmed and stopped == [1000012, 1000011]
    return checks


def transport_survivor(folder: Path) -> dict[str, bool]:
    """Expose a failed close beside facade recovery and clear it only after a confirmed retry."""
    transport = CatalogTransport(folder / "registry")
    handle = _handle(1000015, transport.session)
    transport.handles.append(handle)
    deadline = time.monotonic() + 2
    transport.ledger.append_spawn(handle.spawn_record(), deadline=deadline)
    with (
        patch("orchestration.catalog_transport.stop_owned", return_value=StopResult("survivor", handle.pid, handle.pgid, False)),
        patch("orchestration.catalog_transport.group_state", return_value="alive"),
    ):
        closed = transport.close(deadline=deadline)
    checks = {
        "transport_survivor_close_is_unconfirmed": not closed,
        "transport_survivor_is_exposed": tuple(getattr(transport, "survivors", ())) == (handle,),
        "transport_survivor_pauses_dispatch": getattr(transport, "dispatch_paused", False) is True,
    }

    def confirm(handle: OwnedProcess, deadline: float) -> StopResult:
        handle.confirmed = True
        handle.state = "read_only"
        return StopResult("confirmed", handle.pid, handle.pgid, True)

    with patch("orchestration.catalog_transport.stop_owned", side_effect=confirm), patch("orchestration.catalog_transport.group_state", return_value="gone"):
        closed = transport.close(deadline=deadline)
    checks["confirmed_transport_retry_clears_pause"] = (
        closed and not getattr(transport, "survivors", (handle,)) and not getattr(transport, "dispatch_paused", True)
    )
    return checks


def tick_failure(folder: Path) -> dict[str, bool]:
    """Pause dispatch before a supported later ledger failure escapes the public tick call."""
    checks = {}
    for error_type in (OSError, ValueError, TimeoutError):
        api = EcsApi(WorkerConfig("synthetic"), folder / error_type.__name__)
        raised = False
        with patch.object(api.ledger, "open_entries", side_effect=error_type("synthetic_later_read_failure")):
            try:
                api.tick(deadline=time.monotonic() + 2)
            except error_type:
                raised = True
        checks[f"tick_{error_type.__name__}_propagates"] = raised
        checks[f"tick_{error_type.__name__}_pauses_dispatch"] = api.recovery.failed and api.dispatch_paused
        checks[f"tick_{error_type.__name__}_has_typed_alert"] = api.recovery.alert is not None and api.recovery.alert["kind"] == "worker_recovery_unavailable"
        api.close(deadline=time.monotonic() + 1)
    return checks


def stop_slot_deadline(folder: Path) -> dict[str, bool]:
    """Make an admission timeout retain the same worker for the existing retry path."""
    ledger = WorkerLedger(folder / "registry")
    pool = WorkerPool(WorkerConfig("synthetic"), ledger, "current")
    workers = [Worker(_handle(1000020 + number, "current"), "control", "synthetic", time.monotonic(), True) for number in range(3)]
    pool.workers = workers
    for worker in workers[:2]:
        worker.stopping = True
    target = workers[2]
    target.status = "retiring"
    expired = pool.retire(target, time.monotonic() - 1)
    checks = {
        "stop_slot_timeout_is_a_survivor": expired.status == "survivor" and target.status == "unproven" and target.handle.last_stop == "survivor",
        "stop_slot_timeout_pauses_dispatch": pool.dispatch_paused,
    }
    for worker in workers[:2]:
        worker.stopping = False
        worker.status = "idle"
    retried: list[int] = []

    def stop(handle: OwnedProcess, deadline: float) -> StopResult:
        retried.append(handle.pid)
        handle.confirmed = True
        handle.state = "read_only"
        return StopResult("confirmed", handle.pid, handle.pgid, True)

    with patch("orchestration.worker_pool.stop_owned", side_effect=stop), patch("orchestration.worker_pool.group_state", return_value="gone"):
        pool.retry_survivors(time.monotonic() + 1)
    checks["stop_slot_survivor_is_retried"] = retried == [target.handle.pid] and target.cleaned
    return checks


class SyntheticClient:
    """Return a malformed mutating response after recording the one request."""

    def __init__(self) -> None:
        self.requests = 0

    def put_object(self, **params: Any) -> dict[str, Any]:
        """Omit VersionId after the synthetic remote effect."""
        self.requests += 1
        return {}


def uncertain_effect() -> dict[str, bool]:
    """Distinguish local refusal from a malformed result after a mutating call."""
    client = SyntheticClient()
    sdk = Sdk(argparse.Namespace())
    body = "synthetic"
    params = {"bucket": "synthetic", "key": "synthetic", "body_utf8": body, "sha256": hashlib.sha256(body.encode()).hexdigest()}
    unknown = False
    with patch.object(sdk, "_client", return_value=client):
        try:
            sdk.call("s3_put_small", params)
        except protocol.ProtocolError:
            reply = protocol.error_reply("synthetic", "s3_put_small", "protocol", "operation_result_mismatch")
            unknown = reply["error"]["effect_unknown"] is True
        except (KeyError, TypeError, OSError, ValueError):
            pass
        params["sha256"] = "0" * 64
        local_refusal = False
        try:
            sdk.call("s3_put_small", params)
        except Refused as error:
            local_refusal = str(error) == "body_digest_mismatch"
    return {
        "malformed_mutating_result_is_unknown": unknown and client.requests == 1,
        "pre_request_refusal_stays_definite": local_refusal and client.requests == 1,
    }


def constructor_failure(folder: Path) -> dict[str, bool]:
    """Keep the facade available with dispatch paused when initial recovery cannot read."""
    api: EcsApi | None = None
    with patch.object(WorkerLedger, "private_open", side_effect=OSError("synthetic_read_failure")), contextlib.suppress(OSError):
        api = EcsApi(WorkerConfig("synthetic"), folder / "registry")
    checks = {
        "ledger_failure_keeps_facade_available": api is not None,
        "ledger_failure_pauses_dispatch": api is not None and api.dispatch_paused,
        "ledger_failure_has_typed_alert": api is not None and api.recovery.alert is not None and api.recovery.alert["kind"] == "worker_recovery_unavailable",
    }
    if api is not None:
        api.close(deadline=time.monotonic() + 1)
    return checks


def validator_sensitivity(fixtures: Path) -> dict[str, bool]:
    """Require fixture verification to reject a production validator that accepts every frame."""
    from scripts.orchestration.sdk_worker_cases import check_preparation

    fixtures = fixtures.resolve()
    baseline = check_preparation(fixtures)
    with patch("orchestration.worker_protocol.validate", return_value=None):
        mutant = check_preparation(fixtures)
    return {"fixture_baseline_passes": not baseline["failed"], "fixture_checker_rejects_noop_production_validator": bool(mutant["failed"])}


def stderr_redaction() -> dict[str, bool]:
    """Protect complete diagnostic bytes while secrets cross every streaming boundary."""
    try:
        from orchestration.worker_stderr import StreamRedactor
    except ModuleNotFoundError:
        return {"stderr_redactor_available": False}
    marker = 'synthetic/secret+"value'
    variants = (marker.encode(), quote(marker, safe="").encode(), json.dumps(marker)[1:-1].encode(), base64.b64encode(marker.encode()))
    checks = {}
    for index, value in enumerate(variants):
        payload = b"begin:" + value + b":end"
        passed = True
        for split in range(len(payload) + 1):
            redactor = StreamRedactor((marker,))
            observed = redactor.feed(payload[:split]) + redactor.feed(payload[split:]) + redactor.finish()
            passed = passed and observed == b"begin:[redacted]:end"
        checks[f"stderr_variant_{index}_all_boundaries"] = passed
    redactor = StreamRedactor((marker,))
    body = b"x" * (3 * 65536) + b"final_unterminated_line"
    actual = b"".join(redactor.feed(body[index : index + 65536]) for index in range(0, len(body), 65536)) + redactor.finish()
    checks["stderr_nonsecret_bytes_preserved"] = actual == body
    return checks


def startup_close(folder: Path, kind: str) -> dict[str, bool]:
    """Park reader creation and require close to retain that pending startup duty."""
    from orchestration.worker_stderr import StderrReader

    entered, release, closing = threading.Event(), threading.Event(), threading.Event()
    deadline = time.monotonic() + 4
    readers = []

    def reader(*args: Any, **kwargs: Any) -> Any:
        instance = StderrReader(*args, **kwargs)
        readers.append(instance)
        entered.set()
        if not release.wait(timeout=max(0, deadline - time.monotonic())):
            raise TimeoutError("synthetic_reader_start_gate")
        return instance

    def stop(handle: Any, deadline: float) -> StopResult:
        if not handle.confirmed:
            for fd in (handle.stdin, handle.stdout, handle.stderr):
                os.close(fd)
            handle.confirmed = True
            handle.state = "read_only"
        return StopResult("confirmed", handle.pid, handle.pgid, True)

    with contextlib.ExitStack() as patches:
        if kind == "transport":
            owner: Any = CatalogTransport(folder / "registry")
            owner.deadline = deadline
            ends = [os.pipe() for _ in range(3)]
            handle = _handle(1000031, owner.session)
            handle.stdin, handle.stdout, handle.stderr = ends[0][1], ends[1][0], ends[2][0]
            for fd in (ends[0][0], ends[1][1], ends[2][1]):
                os.close(fd)
            patches.enter_context(patch("orchestration.catalog_transport.spawn_owned", return_value=handle))
            patches.enter_context(patch("orchestration.catalog_transport.record_leader_start", return_value=None))
            patches.enter_context(patch("orchestration.catalog_transport.StderrReader", side_effect=reader))
            patches.enter_context(patch("orchestration.catalog_transport.stop_owned", side_effect=stop))
            patches.enter_context(patch("orchestration.catalog_transport.group_state", return_value="gone"))
            patches.enter_context(patch("orchestration.catalog_transport.write_frame", return_value=None))
            patches.enter_context(patch("orchestration.catalog_transport.read_frame", return_value={"ready": True, "role": "relay", "port": 12345}))

            def spawn() -> Any:
                return owner._spawn("relay", [], deadline)
        else:
            path = Path(__file__).resolve().parents[2] / "services/hai/bootstrap/supervisor.py"
            spec = importlib.util.spec_from_file_location("hai_startup_supervisor", path)
            if spec is None or spec.loader is None:
                raise RuntimeError("synthetic_supervisor_loader")
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
            owner = module.Supervisor("synthetic", stderr_folder=folder / "stderr")
            patches.enter_context(patch.object(module.os, "posix_spawn", return_value=1000032))
            patches.enter_context(patch.object(module._stderr, "StderrReader", side_effect=reader))
            patches.enter_context(patch.object(module._portable, "group_state", return_value="gone"))
            patches.enter_context(patch.object(module, "stop_owned", side_effect=lambda handle, deadline: {"confirmed": stop(handle, deadline).confirmed}))

            def spawn() -> Any:
                return owner._spawn(deadline, withhold=False)

        def close() -> bool:
            closing.set()
            return bool(owner.close(deadline=deadline))

        with ThreadPoolExecutor(max_workers=2) as executor:
            creation = executor.submit(spawn)
            reached = entered.wait(timeout=1)
            closure = executor.submit(close)
            close_started = closing.wait(timeout=1)
            returned_early = False
            try:
                returned_early = closure.result(timeout=0.1)
            except TimeoutError:
                pass
            finally:
                release.set()
            creation.result(timeout=max(0.1, deadline - time.monotonic()))
            closure.result(timeout=max(0.1, deadline - time.monotonic()))
        final = owner.close(deadline=time.monotonic() + 1)
    return {
        f"{kind}_startup_gate_reached": reached and close_started,
        f"{kind}_close_does_not_confirm_pending_reader": not returned_early,
        f"{kind}_startup_reader_finished": bool(final) and len(readers) == 1 and readers[0].complete,
    }


def run(folder: Path, fixtures: Path | None = None) -> dict[str, Any]:
    """Run isolated failure checks without spawning a worker or contacting a service."""
    frozen_sources = source_hashes()
    scratch = Path(tempfile.mkdtemp(prefix="worker_lifecycle_", dir=folder))
    (scratch / "retention.json").write_text(
        json.dumps(
            {
                "class": 3,
                "purpose": "Synthetic isolated worker lifecycle payload",
                "created": datetime.now(UTC).isoformat(),
                "end_condition": "The worker repair evidence and its review are accepted",
            },
            indent=2,
        )
        + "\n"
    )
    checks = {
        **owned_cases(scratch / "owned"),
        **recovery_sessions(scratch / "sessions"),
        **ledger_failure_cleanup(scratch / "cleanup"),
        **stop_slot_deadline(scratch / "slots"),
        **uncertain_effect(),
        **constructor_failure(scratch / "constructor"),
        **transport_survivor(scratch / "transport_survivor"),
        **tick_failure(scratch / "tick"),
        **stderr_redaction(),
        **startup_close(scratch / "transport_startup", "transport"),
        **startup_close(scratch / "supervisor_startup", "supervisor"),
    }
    if fixtures is not None:
        checks.update(validator_sensitivity(fixtures))
    checks["owned_sources_unchanged_during_run"] = frozen_sources == source_hashes()
    return {
        "classification": "Synthetic",
        "written_utc": datetime.now(UTC).isoformat(),
        "owned_source_sha256": frozen_sources,
        "checks": checks,
        "failed": [name for name, ok in checks.items() if not ok],
        "limits": ["Isolated transitions with substituted process observations, stop outcomes and SDK client", "No native ownership or launcher proof"],
        "cleanup": "done; no process or listener started",
    }


def main() -> int:
    """Save the passing and failing lifecycle observations as a local report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fixtures", type=Path)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = run(args.output.parent, args.fixtures)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    return int(bool(report["failed"]))


if __name__ == "__main__":
    raise SystemExit(main())
