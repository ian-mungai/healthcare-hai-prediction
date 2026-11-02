"""Keep failed cleanup authoritative while recording later process observations."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

import pytest

from scripts.orchestration import sdk_runtime_cases as runtime
from tests.support import check

PRIVATE = "synthetic_private_cleanup_value"


def fixture_api(*, confirmed: bool = False) -> tuple[Any, dict[str, Any]]:
    reader = SimpleNamespace(eof=True, complete=True, is_alive=False, failure=None, path=PRIVATE, values=[PRIVATE])
    handle = SimpleNamespace(
        pid=12345,
        pgid=12345,
        state="read_only",
        last_stop="worker_unproven",
        confirmed=False,
        pipes_closed=True,
        exit_observed=True,
        stderr_reader=reader,
        witness=PRIVATE,
        nonce=PRIVATE,
    )
    worker = SimpleNamespace(handle=handle, status="unproven", cleaned=False, stopping=False, owner_released=False)
    cleanup = {"confirmed": confirmed, "released_by_owner": [], "survivors": [{"pid": 12345, "pgid": 12345, "state": "read_only", "status": "worker_unproven"}]}
    api = SimpleNamespace(close=Mock(return_value=cleanup), handles=[handle], pool=SimpleNamespace(workers=[worker], stop_threads=[]))
    return api, cleanup


@pytest.mark.parametrize("group,pid_state", [("gone", "absent"), ("alive", "present"), ("denied", "denied"), ("gone", "unavailable")])
def test_failed_close_retains_subsequent_observations(tmp_path: Path, group: str, pid_state: str) -> None:
    api, cleanup = fixture_api()
    outcome: Any = 12345 if pid_state == "present" else {"absent": ProcessLookupError(), "denied": PermissionError(), "unavailable": OSError()}[pid_state]
    getpgid = Mock(return_value=outcome) if isinstance(outcome, int) else Mock(side_effect=outcome)
    with (
        patch("orchestration.ecs_api.EcsApi", return_value=api),
        patch("scripts.process_group.group_state", return_value=group) as group_probe,
        patch.object(runtime.os, "getpgid", getpgid),
        pytest.raises(runtime.NativeCleanupUnconfirmed, match="^native_worker_cleanup_unconfirmed$") as caught,
        runtime._owned_api(object(), tmp_path),
    ):
        pass
    record = caught.value.record
    check(record["cleanup"] is cleanup and cleanup["confirmed"] is False, "the original failed cleanup is not replaced")
    check("after_close" in record, "failed cleanup must retain later observations")
    observation = record["after_close"]
    check(observation["status"] == "observed", "complete observation is identified")
    row = observation["workers"][0]
    check(row["group_state"] == group and row["pid_state"] == pid_state, "subsequent kernel observations retain their outcomes")
    check(row["state"] == "read_only" and row["last_stop"] == "worker_unproven", "original handle state is retained")
    check(row["confirmed"] is False and row["cleaned"] is False, "physical observations do not promote cached closure")
    check(row["reader"] == {"present": True, "eof": True, "complete": True, "alive": False, "failure": None}, "reader evidence has only allowed fields")
    check(api.close.call_count == 1 and group_probe.call_count == 1 and getpgid.call_count == 1, "observation adds no cleanup retry")
    check(PRIVATE not in json.dumps(record), "private handle and reader values never enter the report")


def test_successful_close_performs_no_diagnostic_probes(tmp_path: Path) -> None:
    api, _ = fixture_api(confirmed=True)
    with (
        patch("orchestration.ecs_api.EcsApi", return_value=api),
        patch("scripts.process_group.group_state") as group,
        patch.object(runtime.os, "getpgid") as pid,
        runtime._owned_api(object(), tmp_path),
    ):
        pass
    check(api.close.call_count == 1 and not group.called and not pid.called, "successful cleanup has no added probes")


def test_observation_failure_cannot_replace_cleanup_failure(tmp_path: Path) -> None:
    api, cleanup = fixture_api()
    with (
        patch("orchestration.ecs_api.EcsApi", return_value=api),
        patch("scripts.process_group.group_state", side_effect=RuntimeError(PRIVATE)),
        pytest.raises(runtime.NativeCleanupUnconfirmed) as caught,
        runtime._owned_api(object(), tmp_path),
    ):
        pass
    check(caught.value.record["cleanup"] is cleanup, "original cleanup identity is preserved")
    check(caught.value.record.get("after_close") == {"status": "unavailable"}, "failed observation is explicit and cannot hide the original refusal")
    check(PRIVATE not in json.dumps(caught.value.record), "observation exception values remain private")


def test_original_body_error_and_cleanup_remain_visible(tmp_path: Path) -> None:
    api, cleanup = fixture_api()
    with (
        patch("orchestration.ecs_api.EcsApi", return_value=api),
        patch("scripts.process_group.group_state", return_value="gone"),
        patch.object(runtime.os, "getpgid", side_effect=ProcessLookupError()),
        pytest.raises(runtime.NativeCleanupUnconfirmed) as caught,
        runtime._owned_api(object(), tmp_path),
    ):
        raise ValueError("synthetic body failure")
    check(caught.value.record["during"] == {"error_type": "ValueError", "message": "synthetic body failure"}, "the body failure remains separate")
    check(caught.value.record["cleanup"] is cleanup, "the cleanup failure remains authoritative")
    check("after_close" in caught.value.record, "subsequent observation is separately labelled")


def test_only_failed_handles_are_observed_and_values_are_closed(tmp_path: Path) -> None:
    api, _ = fixture_api()
    worker = api.pool.workers[0]
    worker.handle.state = PRIVATE
    worker.handle.last_stop = PRIVATE
    worker.status = PRIVATE
    worker.handle.stderr_reader.failure = PRIVATE
    other = SimpleNamespace(handle=SimpleNamespace(pid=99999))
    api.pool.workers.insert(0, other)
    with (
        patch("orchestration.ecs_api.EcsApi", return_value=api),
        patch("scripts.process_group.group_state", return_value="gone"),
        patch.object(runtime.os, "getpgid", side_effect=ProcessLookupError()),
        pytest.raises(runtime.NativeCleanupUnconfirmed) as caught,
        runtime._owned_api(object(), tmp_path),
    ):
        pass
    observation = caught.value.record.get("after_close", {})
    check("workers" in observation, "matching failed handles must be observed")
    check(len(observation["workers"]) == 1, "unrelated handles are not probed")
    row = observation["workers"][0]
    check(row["state"] == row["last_stop"] == row["status"] == row["reader"]["failure"] == "unknown", "diagnostic vocabularies are closed")
    check(PRIVATE not in json.dumps(caught.value.record), "unknown values never reach diagnostics")


def test_failed_worker_observations_are_bounded(tmp_path: Path) -> None:
    api, cleanup = fixture_api()
    workers = []
    survivors = []
    for index in range(9):
        item, _ = fixture_api()
        worker = item.pool.workers[0]
        worker.handle.pid = worker.handle.pgid = 12000 + index
        workers.append(worker)
        survivors.append({"pid": worker.handle.pid, "pgid": worker.handle.pgid})
    api.pool.workers = workers
    cleanup["survivors"] = survivors
    with (
        patch("orchestration.ecs_api.EcsApi", return_value=api),
        patch("scripts.process_group.group_state", return_value="gone") as group,
        patch.object(runtime.os, "getpgid", side_effect=ProcessLookupError()),
        pytest.raises(runtime.NativeCleanupUnconfirmed) as caught,
        runtime._owned_api(object(), tmp_path),
    ):
        pass
    observation = caught.value.record.get("after_close", {})
    check("workers" in observation, "failed close must retain bounded worker evidence")
    check(len(observation["workers"]) == 8 and group.call_count == 8, "at most eight failed workers are probed")
    check(observation["worker_count"] == 9 and observation["truncated"] is True, "omitted observations are explicit")
