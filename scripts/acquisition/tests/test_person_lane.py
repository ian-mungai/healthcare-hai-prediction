from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import redownload as harness
from scripts.acquisition import redownload_route as route
from scripts.acquisition.tests.test_run_lanes import LANES, UNITS, classify, dispatch, lane_files, refused
from tests.support import check

LANE = "mac_person_units"
PERSON_CLASSES = {"needs_person"}
PERSON_IDS = sorted(unit_id for unit_id, lane in LANES.items() if lane in PERSON_CLASSES)
NAMES = ("Jane Q. Example", "Ozzie Sample-Rivera", "Dr. Pat Fictional")


def test_the_person_template_names_the_lane_the_class_and_the_unit_count(tmp_path: Path) -> None:
    folder = tmp_path / "route"
    folder.mkdir()
    queue = folder / "queue.json"
    queue.write_text(json.dumps({"run": 3, "queue": UNITS}))
    classify(folder, queue.read_bytes())
    document = route.write_lane_template(queue, folder / "template.json")
    check(document["lane"] == LANE and document["approved_by"] is None, "the template names the lane and no approver")
    check(document["classes"] == sorted(PERSON_CLASSES) and document["units"] == len(PERSON_IDS), "the template names the classes and the unit count")
    check("originals_stay_local" not in document and "records" not in document, "the template carries no private-data fields")
    check(any("approval" in item for item in route.lane_problems(queue, folder / "template.json")), "an unsigned template is not accepted")


@pytest.mark.parametrize(
    "change",
    [
        {"approved_by": None},
        {"queue_sha256": "0" * 64},
        {"classes": ["needs_person", "private_local"]},
        {"units": 1},
        {"lane": "another_lane"},
    ],
)
def test_the_person_lane_refuses_to_start_unless_the_owner_approval_names_it(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, change: dict) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    run, _, _ = lane_files(tmp_path, **change)
    refused(monkeypatch, run, "lane approval")


def test_the_person_lane_refuses_without_an_approval_file_and_inside_a_task(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    run, approval, _ = lane_files(tmp_path / "a")
    approval.unlink()
    refused(monkeypatch, run, "lane approval")
    task, _, _ = lane_files(tmp_path / "b")
    monkeypatch.setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/example")
    refused(monkeypatch, task, "stay off AWS")
    task.stored_root = task.base
    monkeypatch.delenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
    refused(monkeypatch, task, "stay off AWS")


def test_the_person_lane_needs_the_same_gate_and_the_raw_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("HAI_CAPTURE_ROOT", raising=False)
    run, _, files = lane_files(tmp_path)
    refused(monkeypatch, run, "raw/")
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    files.confirmation.unlink()
    refused(monkeypatch, run, "removal confirmation")


def test_an_approved_person_lane_passes_both_gates_and_stops_at_the_next_gate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    for name in ("HAI_APPROVED_HOSTS", "HAI_APPROVED_HOSTS_APPROVAL"):
        monkeypatch.setenv(name, "placeholder")
    for name in harness.TASK_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    run, _, _ = lane_files(tmp_path)
    with pytest.raises(ValueError, match="independent review"):
        harness.execute(run, allow_network=True)


def test_the_person_lane_and_the_task_partition_the_queue_with_no_unit_in_two(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    full_task = {"stored_root": tmp_path / "mirror", "task_keys": object(), "task_filtered": True}
    runs: dict[str, dict[str, Any]] = {"person": {"person_lane": True}, "task": full_task, "mac": {}}
    attempted: dict[str, list[str]] = {}
    for name, fields in runs.items():
        (tmp_path / name).mkdir()
        attempted[name], _ = dispatch(monkeypatch, tmp_path / name, **fields)
    check(sorted(attempted["person"]) == PERSON_IDS, f"the person lane attempted {sorted(attempted['person'])}")
    check(not set(attempted["person"]) & set(attempted["task"]), "no unit runs in the person lane and the task")
    check(not set(attempted["mac"]) & set(attempted["person"]), "a plain run on the raw route attempts no lane unit")


def test_a_plain_run_on_the_raw_route_lists_the_browser_and_private_units_and_attempts_none(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    seen, summary = dispatch(monkeypatch, tmp_path)
    check(not set(seen) & set(PERSON_IDS), "no browser unit was attempted")
    private = sorted(unit_id for unit_id, lane in LANES.items() if lane == "private_local")
    check(not set(seen) & set(private), "no private unit was attempted")
    listed = summary["not_attempted"]
    check(sorted(listed["needs_person"]) == PERSON_IDS and sorted(listed["private_local"]) == private, "they are listed by reason")
