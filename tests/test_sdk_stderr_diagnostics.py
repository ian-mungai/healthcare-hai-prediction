"""Preserve private, bounded SDK cleanup evidence through the actual report entry path."""

from __future__ import annotations

import contextlib
import json
import runpy
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

import pytest

from scripts.orchestration import sdk_runtime_cases as runtime
from scripts.orchestration import sdk_stderr_cases as stderr
from tests.support import check

PRIVATE = "synthetic_private_diagnostic_canary"


class Reader:
    def __init__(self, alive: bool, *, eof: bool = False, failure: str | None = None, events: list[str] | None = None, label: str = "reader") -> None:
        self.alive = alive
        self.eof = eof
        self.failure = failure
        self.events = events if events is not None else []
        self.label = label
        self._thread = SimpleNamespace(ident=None)
        self.path = PRIVATE
        self.values = [PRIVATE]

    def finish(self, *, deadline: float) -> bool:
        self.events.append(self.label + ":finish")
        return not self.alive

    @property
    def is_alive(self) -> bool:
        self.events.append(self.label + ":alive")
        return self.alive


def _handle(reader: Reader | None, *, pid: int = 101, state: str = "reap_only", last_stop: str = "worker_unproven") -> Any:
    return SimpleNamespace(
        stderr_reader=reader, pid=pid, pgid=pid, state=state, last_stop=last_stop, witness=PRIVATE, nonce=PRIVATE, leader_start=PRIVATE, environment=PRIVATE
    )


@pytest.mark.parametrize(
    "present,alive,group,predicate,deciding_group",
    [
        (False, False, "gone", "reader_present", "not_probed"),
        (True, True, "alive", "reader_stopped", "not_probed"),
        (True, False, "alive", "group_gone", "alive"),
        (True, False, "denied", "group_gone", "denied"),
        (True, False, PRIVATE, "group_gone", "unknown"),
    ],
)
def test_failed_predicate_keeps_bounded_observations(present: bool, alive: bool, group: str, predicate: str, deciding_group: str) -> None:
    reader = Reader(alive, eof=True, failure="worker_stderr_read_failed") if present else None
    with patch.object(stderr, "group_state", return_value=group) as probe, pytest.raises(RuntimeError, match="^synthetic_stderr_cleanup_unconfirmed$") as error:
        stderr._join_readers([_handle(reader)])
    record = getattr(error.value, "record", {})
    check(record.get("failed_predicate") == predicate, "cleanup failure must retain its exact deciding predicate")
    check(record["handle_count"] == 1 and record["handle_index"] == 0, "failure binds to the original handle")
    check(
        record["deciding"] == {"reader_present": present, "reader_alive": alive if present else None, "group_state": deciding_group}, "deciding reads preserved"
    )
    subsequent = record["subsequent"]
    check(subsequent["pid"] == 101 and subsequent["pgid"] == 101, "safe process identity retained")
    check(subsequent["state"] == "reap_only" and subsequent["last_stop"] == "worker_unproven", "safe handle state retained")
    check(subsequent["reader_eof"] is (True if present else None), "EOF retained without interpreting completion")
    check(subsequent["reader_failure"] == ("worker_stderr_read_failed" if present else None), "closed reader failure retained")
    if deciding_group == "not_probed":
        check(subsequent["group_state"] == group, "short-circuited group read must be labelled subsequent")
    else:
        check("group_state" not in subsequent, "deciding group result must not be probed again")
    check(probe.call_count == 1, "only one deciding or subsequent group probe")
    check(PRIVATE not in json.dumps(record), "private values must not reach diagnostics")


def test_cleanup_probe_order_and_short_circuit_are_preserved() -> None:
    events: list[str] = []
    handles = [_handle(Reader(False, events=events, label="first")), _handle(Reader(True, events=events, label="second"), pid=102), _handle(None, pid=103)]

    def group(pgid: int) -> str:
        events.append(f"group:{pgid}")
        return "gone"

    with patch.object(stderr, "group_state", side_effect=group), pytest.raises(RuntimeError) as error:
        stderr._join_readers(handles)
    check(
        events == ["first:finish", "second:finish", "first:alive", "group:101", "second:alive", "group:102"], "decisive cleanup probes retain order and count"
    )
    record = getattr(error.value, "record", {})
    check(record.get("handle_index") == 1 and record.get("handle_count") == 3, "first failed handle is identified")
    check(record["deciding"]["group_state"] == "not_probed" and record["subsequent"]["group_state"] == "gone", "supplemental absence never rewrites failure")


@pytest.mark.parametrize(
    "failure", [None, "worker_stderr_start_failed", "worker_stderr_read_failed", "worker_stderr_write_failed", "worker_stderr_deadline", PRIVATE]
)
def test_reader_failure_vocabulary_is_closed(failure: str | None) -> None:
    with patch.object(stderr, "group_state", return_value="denied"), pytest.raises(RuntimeError) as error:
        stderr._join_readers([_handle(Reader(True, failure=failure), state=PRIVATE, last_stop=PRIVATE)])
    record = getattr(error.value, "record", {})
    check("subsequent" in record, "cleanup failure must include subsequent observations")
    check(record["subsequent"]["reader_failure"] == ("unknown" if failure == PRIVATE else failure), "reader failure vocabulary is closed")
    check(record["subsequent"]["state"] == "unknown" and record["subsequent"]["last_stop"] == "unknown", "handle vocabulary is closed")
    check(PRIVATE not in json.dumps(record), "raw diagnostic fields are absent")


def test_success_keeps_original_predicates_without_extra_probes() -> None:
    events: list[str] = []
    with patch.object(stderr, "group_state", return_value="gone") as probe:
        check(
            stderr._join_readers([_handle(Reader(False, failure="worker_stderr_write_failed", events=events))]),
            "stopped reader and absent group retain success",
        )
        check(stderr._join_readers([]), "empty handle set retains success")
    check(events == ["reader:finish", "reader:alive"] and probe.call_count == 1, "successful cleanup gets no diagnostic probes")


def test_original_join_error_is_not_replaced() -> None:
    original = OSError("synthetic_original_join_failure")
    reader = Reader(True)
    with patch.object(reader, "finish", side_effect=original), patch.object(stderr, "group_state") as probe, pytest.raises(OSError) as error:
        stderr._join_readers([_handle(reader)])
    check(error.value is original and not probe.called, "original reader failure propagates before any absence probe")


def test_subsequent_probe_error_cannot_replace_deciding_failure() -> None:
    with (
        patch.object(stderr, "group_state", side_effect=OSError(PRIVATE)),
        pytest.raises(RuntimeError, match="^synthetic_stderr_cleanup_unconfirmed$") as error,
    ):
        stderr._join_readers([_handle(Reader(True))])
    record = getattr(error.value, "record", {})
    check(record.get("failed_predicate") == "reader_stopped", "supplemental read failure cannot replace the original predicate")
    check(record["subsequent"]["group_state"] == "probe_failed", "supplemental read error has a fixed code")
    check(PRIVATE not in json.dumps(record), "supplemental exception text is discarded")


def test_deciding_group_probe_error_keeps_identity() -> None:
    original = OSError("synthetic_original_group_probe_failure")
    with patch.object(stderr, "group_state", side_effect=original), pytest.raises(OSError) as error:
        stderr._join_readers([_handle(Reader(False))])
    check(error.value is original, "an original deciding group probe failure propagates unchanged")


def test_deciding_reader_observation_is_not_replaced_by_later_state() -> None:
    class ChangingReader(Reader):
        @property
        def is_alive(self) -> bool:
            before = self.alive
            self.alive = False
            self.events.append("alive")
            return before

    reader = ChangingReader(True)
    with patch.object(stderr, "group_state", return_value="gone"), pytest.raises(RuntimeError) as error:
        stderr._join_readers([_handle(reader)])
    record = getattr(error.value, "record", {})
    check(record.get("deciding", {}).get("reader_alive") is True, "reader observation binds to the deciding read")
    check(reader.events == ["reader:finish", "alive"], "diagnostics never recheck the deciding reader state")


def test_existing_reader_join_budget_is_preserved() -> None:
    reader = Reader(False)
    reader._thread = SimpleNamespace(ident=7, join=Mock())
    with (
        patch.object(stderr.time, "monotonic", return_value=10.0),
        patch.object(reader, "finish", wraps=reader.finish) as finish,
        patch.object(stderr, "group_state", return_value="gone"),
    ):
        check(stderr._join_readers([_handle(reader)]), "existing join success retained")
    check(finish.call_args.kwargs == {"deadline": 11.0} and reader._thread.join.call_args.kwargs == {"timeout": 1.0}, "same one-second join budget retained")


@contextlib.contextmanager
def _idle_dependencies(*, first: dict[str, Any] | None = None, body_error: BaseException | None = None) -> Any:
    clock = [100.0]
    first = first if first is not None else {"ok": False, "error": {"kind": "worker_died", "code": PRIVATE}, "result": PRIVATE, "id": PRIVATE}
    api = SimpleNamespace(
        handles=[_handle(Reader(False)), _handle(Reader(True, failure="worker_stderr_deadline"), pid=102)],
        dispatch_paused=False,
        call=Mock(side_effect=body_error if body_error is not None else [first, {"ok": True, "result": PRIVATE, "id": PRIVATE}]),
        close=Mock(return_value={"confirmed": False, "survivors": [{"pid": 102, "leader_start": PRIVATE}], "released_by_owner": [], "extra": PRIVATE}),
        ledger=SimpleNamespace(open_entries=Mock(side_effect=AssertionError("ledger is after failed join"))),
    )

    def sleep(_: float) -> None:
        clock[0] += 20

    with (
        patch.object(stderr, "WorkerStandin", return_value=contextlib.nullcontext(SimpleNamespace(endpoint="http://127.0.0.1:1"))),
        patch.object(stderr, "EcsApi", return_value=api) as factory,
        patch.object(stderr.time, "monotonic", side_effect=lambda: clock[0]),
        patch.object(stderr.time, "sleep", side_effect=sleep),
        patch.object(stderr, "group_state", return_value="gone"),
    ):
        yield api, factory


def _idle_record(folder: Path, **kwargs: Any) -> dict[str, Any]:
    with _idle_dependencies(**kwargs) as (api, factory), pytest.raises(RuntimeError, match="^synthetic_stderr_cleanup_unconfirmed$") as error:
        stderr.idle_case(folder)
    check(api.call.call_count == 2 and api.close.call_count == 1 and factory.call_count == 1, "failure neither retries nor starts a fresh session")
    record: dict[str, Any] = getattr(error.value, "record", {})
    check("idle_case" in record, "idle cleanup failure must retain partial case evidence")
    return record


def test_idle_failure_keeps_partial_checks_replies_and_close(tmp_path: Path) -> None:
    record = _idle_record(tmp_path)
    idle = record["idle_case"]
    check(
        idle["checks"]
        == {
            "aged_idle_reader_stays_live": False,
            "idle_does_not_pause_dispatch": True,
            "aged_worker_is_replaced": False,
            "recycle_does_not_pause_dispatch": True,
            "aged_pool_close_confirmed": False,
        },
        "partial booleans remain unchanged and later checks are not invented",
    )
    check(idle["replies"] == {"first": {"ok": False, "error_kind": "worker_died"}, "second": {"ok": True, "error_kind": None}}, "only reply outcomes retained")
    check(idle["close"] == {"confirmed": False, "survivor_count": 1, "released_by_owner_count": 0}, "close result is bounded and private")
    check(PRIVATE not in json.dumps(record), "idle diagnostic evidence has no private fields")


@pytest.mark.parametrize("kind", ["service", "throttle", "network", "bad_request", "deadline", "worker_died", "protocol", "shutdown", PRIVATE])
def test_idle_reply_error_kind_is_closed(tmp_path: Path, kind: str) -> None:
    record = _idle_record(tmp_path, first={"ok": False, "error": {"kind": kind, "message": PRIVATE}, "result": PRIVATE})
    check(record["idle_case"]["replies"]["first"]["error_kind"] == ("unknown" if kind == PRIVATE else kind), "closed reply error kind retained")
    check(PRIVATE not in json.dumps(record), "unknown kind text is discarded")


def test_idle_body_error_is_not_masked(tmp_path: Path) -> None:
    original = ValueError("synthetic_original_body_failure")
    with _idle_dependencies(body_error=original) as (api, _), pytest.raises(ValueError) as error:
        stderr.idle_case(tmp_path)
    check(error.value is original and api.close.call_count == 1, "idle body failure keeps identity after close")


def test_diagnostic_counts_are_capped(tmp_path: Path) -> None:
    with patch.object(stderr, "group_state", return_value="gone"), pytest.raises(RuntimeError) as error:
        stderr._join_readers([_handle(None)] * 1025)
    record = getattr(error.value, "record", {})
    check(record.get("handle_count") == 1024 and record.get("handle_count_capped") is True, "handle count is capped with explicit truncation")
    with _idle_dependencies() as (api, _), pytest.raises(RuntimeError) as idle_error:
        api.close.return_value["survivors"] *= 1025
        api.close.return_value["released_by_owner"] = [101] * 1025
        stderr.idle_case(tmp_path)
    idle = getattr(idle_error.value, "record", {}).get("idle_case", {})
    check(idle.get("close") == {"confirmed": False, "survivor_count": 1024, "released_by_owner_count": 1024}, "close result counts are capped")


@pytest.mark.parametrize("as_main", [False, True])
def test_cleanup_exception_reaches_failed_report(tmp_path: Path, as_main: bool) -> None:
    output = tmp_path / "report.json"
    command = [str(runtime.ROOT / "scripts/orchestration/sdk_runtime_cases.py"), "--mode", "native", "--cases", "reader_idle", "--output", str(output)]
    with _idle_dependencies(), patch.object(sys, "argv", command):
        if as_main:
            with pytest.raises(SystemExit) as error:
                runpy.run_module("scripts.orchestration.sdk_runtime_cases", run_name="__main__")
            result = error.value.code
        else:
            result = runtime.main()
    report = json.loads(output.read_text())
    check(result == 1 and report["failed"] == ["native_case_error"], "cleanup failure retains exit and failed-case binding")
    row = next(row for row in report["cases"] if row["case"] == "native_case_error")
    check(row["checks"] == {"completed": False} and row["error_type"] == "RuntimeError", "failure never becomes a passing case row")
    check(row["message"] == "synthetic_stderr_cleanup_unconfirmed", "original cleanup failure code preserved")
    check(row.get("evidence", {}).get("failed_predicate") == "reader_stopped", "actual exception-to-report path retains deciding evidence")
    check("idle_case" in row["evidence"], "actual exception-to-report path retains partial idle evidence")
    check(not any(row["case"] == "aged_idle_reader" for row in report["cases"]), "partial idle checks remain failure evidence")
    check(PRIVATE not in output.read_text(), "saved report has no private diagnostic values")


def test_report_retains_preceding_rows(tmp_path: Path) -> None:
    output = tmp_path / "report.json"
    prior = [{"case": f"prior_{index}", "checks": {"retained": True}} for index in range(203)]
    with contextlib.ExitStack() as stack:
        for name in (
            "retry_cases",
            "stray_client_cases",
            "pool_cases",
            "scan_timeout_cases",
            "concurrency_cases",
            "recovery_cases",
            "isolation_cases",
            "records_cases",
            "parameter_cases",
            "stderr_cases",
        ):
            stack.enter_context(patch.object(runtime, name, return_value=[]))
        stack.enter_context(patch.object(runtime, "deadline_cases", return_value=prior))
        stack.enter_context(patch.object(stderr, "failure_cases", return_value=[]))
        stack.enter_context(_idle_dependencies())
        stack.enter_context(patch.object(sys, "argv", ["sdk_runtime_cases", "--mode", "native", "--output", str(output)]))
        check(runtime.main() == 1, "preceding passing rows cannot erase cleanup failure")
    report = json.loads(output.read_text())
    check(report["cases"][1:204] == prior and report["failed"] == ["native_case_error"], "all preceding rows stay in order and cleanup still fails")
