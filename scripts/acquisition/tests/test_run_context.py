from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import redownload as harness
from scripts.acquisition.tests.held import hold
from scripts.acquisition.tests.test_redownload_route import execution_run
from scripts.acquisition.tests.test_run_lanes import lane_files, make_unit
from tests.support import check


def test_the_person_flag_on_a_queue_that_is_not_run_3_is_refused_before_any_gate(tmp_path: Path) -> None:
    run = execution_run(tmp_path, 2)
    run.person_lane = True
    with pytest.raises(ValueError, match="belongs to run 3"):
        harness.execute(run, allow_network=True)


def test_a_plain_run_3_on_the_mac_is_refused_because_it_has_no_placement(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    run, _, _ = lane_files(tmp_path)
    run.person_lane = False
    called: list[str] = []

    def record(unit: dict, run: Any) -> dict:
        called.append(unit["snapshot_id"])
        return {}

    monkeypatch.setattr(harness, "handle", record)
    with pytest.raises(ValueError, match="needs a lane or the task mirror"):
        harness.execute(run, allow_network=True)
    check(called == [], "no unit was attempted")


def test_a_run_3_in_the_task_context_needs_the_mirror_and_a_lane_run_must_not_have_one(tmp_path: Path) -> None:
    check(harness.resolve_context(3, person=False, mirror=True) == "task", "the mirror makes it the task")
    check(harness.resolve_context(3, person=True, mirror=False) == "mac_person_units", "the person flag makes the person lane")
    check(harness.resolve_context(2, person=False, mirror=False) == "local", "earlier runs stay local")
    for bad in ({"person": True, "mirror": True}, {"person": False, "mirror": False}):
        with pytest.raises(ValueError):
            harness.resolve_context(3, **bad)


def second_pass(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, units: list[dict], **fields: Any) -> tuple[list[list[str]], list[dict]]:
    seen: list[str] = []

    def handle(unit: dict, run: Any) -> dict:
        seen.append(unit["snapshot_id"])
        return {"outcome": "exact_match"}

    monkeypatch.setattr(harness, "handle", handle)
    monkeypatch.setattr(harness, "in_reviewed_scope", lambda run, source: True)
    run = harness.Run(queue_path=tmp_path / "queue.json", state_root=tmp_path / "run", base=tmp_path, **fields)
    hold(run, units)
    run.state_root.mkdir()
    passes, summaries = [], []
    for _ in range(2):
        seen.clear()
        summaries.append(harness.execute_locked(run, False, True, None, None, None))
        passes.append(list(seen))
    return passes, summaries


def test_a_pilot_of_another_lane_never_blocks_the_tasks_own_units_and_every_unit_is_named(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    first = make_unit("file", 1) | {"snapshot_id": "A__1", "source_id": "A"}
    second = make_unit("file", 2) | {"snapshot_id": "A__2", "source_id": "A"}
    keyed = make_unit("bls", 3) | {"snapshot_id": "K__1", "source_id": "K"}
    passes, summaries = second_pass(monkeypatch, tmp_path, [first, second, keyed], stored_root=tmp_path / "mirror")
    check(passes == [["A__1"], ["A__2"]], f"the task finished its own units: {passes}")
    check(all(summary["not_attempted"]["task_keyed"] == ["K__1"] for summary in summaries), "the keyed unit is named in every pass")


def test_a_unit_the_mirror_does_not_carry_cannot_be_the_pilot_of_a_unit_it_does(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    moved = make_unit("mmd_moved", 1) | {"snapshot_id": "M__1", "source_id": "A"}
    plain = make_unit("file", 2) | {"snapshot_id": "F__1", "source_id": "A"}
    passes, summaries = second_pass(monkeypatch, tmp_path, [moved, plain], stored_root=tmp_path / "mirror")
    check(passes[0] == ["F__1"], f"the file unit ran in the first pass: {passes}")
    check(summaries[0]["not_attempted"]["needs_local_data"] == ["M__1"], "the unit the mirror does not carry is named")


def test_the_task_keeps_its_state_outside_a_read_only_code_tree(tmp_path: Path) -> None:
    code, work = tmp_path / "code", tmp_path / "work"
    (code / "scripts").mkdir(parents=True)
    (code / "scripts" / "tool.py").write_text("code")
    work.mkdir()
    run = harness.Run(queue_path=tmp_path / "queue.json", state_root=work / "state", base=code, state_parent=work, stored_root=tmp_path / "mirror")
    hold(run, [], number=3)
    run.cache["controls_sha256"] = "1" * 64
    for path in (code / "scripts" / "tool.py", code / "scripts", code):
        path.chmod(stat.S_IRUSR | stat.S_IXUSR if path.is_dir() else stat.S_IRUSR)
    try:
        harness.validate_root(run)
        harness.started_at(run)
        check((work / "state" / "run.json").is_file(), "the ownership record is in the workspace")
        check(harness.root_label(run) == "state", "the controls name the state root relative to the workspace")
        check(not (code / "data").exists() and os.access(code, os.W_OK) is False, "the code tree is untouched and still read only")
    finally:
        for path in (code, code / "scripts"):
            path.chmod(stat.S_IRWXU)
        (code / "scripts" / "tool.py").chmod(stat.S_IRUSR | stat.S_IWUSR)


def test_a_state_root_outside_the_workspace_is_still_refused(tmp_path: Path) -> None:
    work = tmp_path / "work"
    work.mkdir()
    run = harness.Run(queue_path=tmp_path / "queue.json", state_root=tmp_path / "elsewhere", base=tmp_path / "code", state_parent=work)
    hold(run, [], number=3)
    with pytest.raises(ValueError, match="outside"):
        harness.validate_root(run)
