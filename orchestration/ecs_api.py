"""Expose synchronous bounded AWS calls while SDK imports remain confined to owned workers."""

from __future__ import annotations

import contextlib
import secrets
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from orchestration.worker_pool import WorkerConfig, WorkerPool
from orchestration.worker_protocol import CallInterrupted, ProtocolError, cap_seconds, error_reply, read_frame, validate, write_frame
from orchestration.worker_recovery import Recovery, WorkerLedger
from scripts.process_group import OwnedProcess

__all__ = ["EcsApi", "WorkerConfig"]


class EcsApi:
    """Own one launcher session's calls, process handles and recovery pause state."""

    def __init__(self, config: WorkerConfig, registry: Path) -> None:
        self.session = secrets.token_hex(16)
        self.ledger = WorkerLedger(registry)
        self.recovery = Recovery(self.ledger, self.session)
        self.pool = WorkerPool(config, self.ledger, self.session)
        with contextlib.suppress(OSError, ValueError, TimeoutError):
            self.recovery.sweep(deadline=time.monotonic() + 4)

    @property
    def handles(self) -> tuple[OwnedProcess, ...]:
        """Expose retained ownership identities for the launcher's registry and cleanup evidence."""
        return self.pool.handles

    @property
    def dispatch_paused(self) -> bool:
        """Refuse new task dispatch while a recovered or owned group remains unproven."""
        return self.recovery.dispatch_paused or self.pool.dispatch_paused

    def call(self, operation: str, params: Mapping[str, Any], *, deadline: float, file_key: str | None = None) -> dict[str, Any]:
        """Run one closed call with a fixed deadline covering admission and every response byte."""
        request_id = secrets.token_hex(16)
        if self.pool.closing.is_set():
            return error_reply(request_id, operation, "shutdown", "shutdown")
        if operation == "run_task" and self.dispatch_paused:
            return error_reply(request_id, operation, "bad_request", "worker_recovery_unproven")
        try:
            deadline = min(deadline, time.monotonic() + cap_seconds(operation))
            if time.monotonic() >= deadline:
                return error_reply(request_id, operation, "deadline", "worker_admission_deadline")
            request: dict[str, Any] = {"id": request_id, "op": operation, "params": dict(params), "budget_s": deadline - time.monotonic()}
            validate(request)
        except (ProtocolError, TypeError, ValueError, OverflowError):
            return error_reply(request_id, operation, "bad_request", "closed_operation_refused")
        try:
            worker = self.pool.acquire(operation, request["params"], deadline, file_key)
        except TimeoutError:
            return error_reply(request_id, operation, "deadline", "worker_admission_deadline")
        except RuntimeError:
            return error_reply(request_id, operation, "shutdown" if self.pool.closing.is_set() else "worker_died", "worker_admission_refused")
        except (OSError, ValueError):
            return error_reply(request_id, operation, "worker_died", "worker_spawn_failed")
        try:
            request["budget_s"] = deadline - time.monotonic()
            if operation == "s3_download":
                self.pool.reserve_partial(worker, Path(request["params"]["temp_path"]))
            validate(request)
            write_frame(worker.handle.stdin, request, deadline, self.pool.closing.is_set)
            reply = read_frame(worker.handle.stdout, deadline, self.pool.closing.is_set)
            validate(reply, operation, request_id)
            if reply["ok"] and operation == "get_parameter":
                name, version = request["params"]["name"].rsplit(":", 1)
                if reply["result"]["name"] != name or reply["result"]["version"] != int(version):
                    raise ProtocolError("parameter_version_mismatch")
            if reply["ok"] and operation == "s3_download":
                result = reply["result"]
                expected = request["params"]
                if any(
                    result[name] != expected[other]
                    for name, other in (("temp_path", "temp_path"), ("size", "expected_size"), ("sha256", "expected_sha256"), ("version_id", "version_id"))
                ):
                    raise ProtocolError("download_result_mismatch")
                worker.partials.remove(Path(result["temp_path"]))
                self.pool.reserved_partials.discard(Path(result["temp_path"]))
            if reply["ok"] and operation == "s3_upload_part" and reply["result"]["part_number"] != request["params"]["part_number"]:
                raise ProtocolError("part_result_mismatch")
        except CallInterrupted:
            reply = error_reply(request_id, operation, "shutdown", "shutdown")
        except TimeoutError:
            reply = error_reply(request_id, operation, "deadline", "absolute_call_deadline")
        except EOFError:
            worker.handle.exit_observed = True
            reply = error_reply(request_id, operation, "worker_died", "worker_pipe_closed")
        except ProtocolError:
            reply = error_reply(request_id, operation, "protocol", "worker_frame_refused")
        except (OSError, ValueError):
            reply = error_reply(request_id, operation, "protocol", "worker_io_refused")
        cleanup = self.pool.release(worker, reply, min(time.monotonic() + 5, deadline + 5))
        if cleanup is not None and not cleanup.confirmed:
            return error_reply(request_id, operation, "worker_died", "worker_cleanup_unconfirmed")
        return reply

    def tick(self, *, deadline: float) -> None:
        """Retry owned survivors and refresh read-only recovery without extending the caller's budget."""
        self.pool.retry_survivors(deadline)
        needs_scan = self.recovery.refresh_closures(deadline=deadline)
        if needs_scan or time.monotonic() >= self.recovery.next_sweep:
            self.recovery.sweep(deadline=deadline)
        self.pool.reconcile_closures(self.ledger.closed_events(deadline=deadline))

    def close(self, *, deadline: float) -> dict[str, Any]:
        """Finish SDK cleanup before the launcher's existing seal drain begins."""
        return self.pool.close(min(deadline, time.monotonic() + 10))
