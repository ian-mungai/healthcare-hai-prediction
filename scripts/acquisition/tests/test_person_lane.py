from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import redownload as harness
from scripts.acquisition import redownload_route as route
from scripts.acquisition.tests.test_run_lanes import LANES, UNITS, dispatch, lane_files, refused
from tests.support import check

LANE = "mac_person_units"
PERSON_CLASSES = {"needs_person", "needs_owner_decision_private"}
PERSON_IDS = sorted(unit_id for unit_id, lane in LANES.items() if lane in PERSON_CLASSES)
NAMES = ("Jane Q. Example", "Ozzie Sample-Rivera", "Dr. Pat Fictional")


def test_the_person_template_names_the_lane_the_classes_and_that_originals_stay_local(tmp_path: Path) -> None:
    folder = tmp_path / "route"
    folder.mkdir()
    queue = folder / "queue.json"
    queue.write_text(json.dumps({"run": 3, "queue": UNITS}))
    document = route.write_lane_template(queue, folder / "template.json", LANE)
    check(
        document["lane"] == LANE and document["approved_by"] is None and document["originals_stay_local"] is True, "the template names the lane and no approver"
    )
    check(document["classes"] == sorted(PERSON_CLASSES) and document["units"] == len(PERSON_IDS), "the template names the classes and the unit count")
    check(document["records"] == "digests, sizes and counts only", "the template states what the lane records")
    check(any("approval" in item for item in route.lane_problems(queue, folder / "template.json", LANE)), "an unsigned template is not accepted")


@pytest.mark.parametrize(
    "change",
    [
        {"approved_by": None},
        {"queue_sha256": "0" * 64},
        {"classes": ["needs_person"]},
        {"units": 1},
        {"lane": "mac_keyed_units"},
        {"originals_stay_local": False},
        {"records": "everything"},
    ],
)
def test_the_person_lane_refuses_to_start_unless_the_owner_approval_names_it(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, change: dict) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    run, _, _ = lane_files(tmp_path, LANE, **change)
    refused(monkeypatch, run, "lane approval")


def test_the_keyed_approval_does_not_open_the_person_lane_and_the_reverse(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    run, _, _ = lane_files(tmp_path / "a", "mac_keyed_units")
    run.keyed_lane, run.person_lane = False, True
    refused(monkeypatch, run, "lane approval")
    other, _, _ = lane_files(tmp_path / "b", LANE)
    other.keyed_lane, other.person_lane = True, False
    refused(monkeypatch, other, "lane approval")


def test_the_person_lane_refuses_without_an_approval_file_inside_a_task_and_with_both_lanes_set(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    run, approval, _ = lane_files(tmp_path / "a", LANE)
    approval.unlink()
    refused(monkeypatch, run, "lane approval")
    task, _, _ = lane_files(tmp_path / "b", LANE)
    monkeypatch.setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/example")
    refused(monkeypatch, task, "stay off AWS")
    task.stored_root = tmp_path / "mirror"
    monkeypatch.delenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
    refused(monkeypatch, task, "stay off AWS")
    both, _, _ = lane_files(tmp_path / "c", LANE)
    both.keyed_lane = True
    refused(monkeypatch, both, "one lane")


def test_the_person_lane_needs_the_same_gate_and_the_raw_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("HAI_CAPTURE_ROOT", raising=False)
    run, _, files = lane_files(tmp_path, LANE)
    refused(monkeypatch, run, "raw/")
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    files.record.unlink()
    refused(monkeypatch, run, "removal record")


def test_an_approved_person_lane_passes_both_gates_and_stops_at_the_next_gate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    for name in ("HAI_APPROVED_HOSTS", "HAI_APPROVED_HOSTS_APPROVAL"):
        monkeypatch.setenv(name, "placeholder")
    for name in harness.TASK_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    run, _, _ = lane_files(tmp_path, LANE)
    with pytest.raises(ValueError, match="independent review"):
        harness.execute(run, allow_network=True)


def test_the_three_lanes_and_the_task_partition_the_queue_with_no_unit_in_two(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    runs: dict[str, dict[str, Any]] = {"person": {"person_lane": True}, "keyed": {"keyed_lane": True}, "task": {"stored_root": tmp_path / "mirror"}, "mac": {}}
    attempted: dict[str, list[str]] = {}
    for name, fields in runs.items():
        (tmp_path / name).mkdir()
        attempted[name], _ = dispatch(monkeypatch, tmp_path / name, **fields)
    check(sorted(attempted["person"]) == PERSON_IDS, f"the person lane attempted {sorted(attempted['person'])}")
    check(not set(attempted["person"]) & set(attempted["keyed"]), "no unit runs in the person and keyed lanes")
    check(not set(attempted["person"]) & set(attempted["task"]), "no unit runs in the person lane and the task")
    check(not set(attempted["mac"]) & (set(attempted["person"]) | set(attempted["keyed"])), "a plain run on the raw route attempts no lane unit")


def test_a_plain_run_on_the_raw_route_lists_the_person_and_private_units_and_attempts_none(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    seen, summary = dispatch(monkeypatch, tmp_path)
    check(not set(seen) & set(PERSON_IDS), "no person or private unit was attempted")
    listed = summary["not_attempted"]
    check(sorted(listed["needs_person"] + listed["needs_owner_decision_private"]) == PERSON_IDS, "they are listed by reason")


def run_private(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, behavior: Any) -> tuple[list[dict], dict, harness.Run]:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    monkeypatch.setattr(harness, "handle", behavior)
    monkeypatch.setattr(harness, "in_reviewed_scope", lambda run, source: True)
    queue = tmp_path / "queue.json"
    private = [unit for unit in UNITS if harness.lane_of(unit) == "needs_owner_decision_private"]
    queue.write_text(json.dumps({"run": 2, "queue": private}))
    run = harness.Run(queue_path=queue, state_root=tmp_path / "run", base=tmp_path, person_lane=True)
    run.cache["queue"] = private
    run.state_root.mkdir()
    logged: list[dict] = []
    summary = harness.execute_locked(run, False, True, None, None, logged.append)
    return logged, summary, run


def everything_written(run: harness.Run, logged: list[dict], summary: dict, captured: str) -> str:
    texts = [json.dumps(logged), json.dumps(summary), captured]
    texts += [path.read_text(errors="replace") for path in sorted(run.state_root.rglob("*")) if path.is_file()]
    return "\n".join(texts)


def test_no_synthetic_name_reaches_an_outcome_a_log_a_summary_or_a_diagnostic(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    def leaky(unit: dict, run: Any) -> dict:
        if unit["snapshot_id"].startswith("owners"):
            return {
                "outcome": "changed_needs_review",
                "attempts": 1,
                "reason": f"row for {NAMES[0]}",
                "differing_data_files": [f"{NAMES[1]}.csv"],
                "excluded_parts": [NAMES[2]],
                "stored_sha256": "a" * 64,
                "fresh_sha256": "b" * 64,
                "fresh_bytes": 12,
                "fresh_path": f"private/{NAMES[1]}/file.csv",
                "note": {"owner": NAMES[0]},
            }
        if unit["snapshot_id"].startswith("hcai"):
            raise ValueError(f"cannot read the row of {NAMES[0]} at {NAMES[2]}")
        raise KeyError(NAMES[1])

    logged, summary, run = run_private(monkeypatch, tmp_path, leaky)
    text = everything_written(run, logged, summary, "".join(capsys.readouterr()))
    leaked = [name for name in NAMES if name in text]
    check(leaked == [], f"names reached a record, log or summary: {leaked}")
    outcome = json.loads((run.state_root / "outcomes" / "owners__12.json").read_text())
    check(
        outcome["outcome"] == "changed_needs_review" and outcome["stored_sha256"] == "a" * 64 and outcome["fresh_bytes"] == 12,
        "digests, sizes and the outcome are kept",
    )
    check(
        outcome["differing_file_count"] == 1 and outcome["excluded_part_count"] == 1 and outcome["withheld_fields"] >= 1,
        "files and parts are reduced to counts",
    )
    blocked = json.loads((run.state_root / "outcomes" / "hcai__13.json").read_text())
    check(
        blocked["outcome"] == "blocked" and blocked["error_category"] in {"validation", "other", "missing_key"},
        "a failure is recorded as a fixed category only",
    )


def test_the_report_of_a_private_run_holds_no_name(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def leaky(unit: dict, run: Any) -> dict:
        return {"outcome": "changed_needs_review", "attempts": 1, "reason": NAMES[0], "differing_data_files": [NAMES[1]], "stored_sha256": "a" * 64}

    _, _, run = run_private(monkeypatch, tmp_path, leaky)
    (run.state_root / "run.json").write_text("{}")
    run.cache["queue_sha256"] = "0" * 64
    monkeypatch.setattr(harness, "validate_root", lambda run: None)
    monkeypatch.setattr(harness, "require_run_binding", lambda run: None)
    document, _name = harness.write_report(run)
    text = json.dumps(document) + "\n".join(path.read_text() for path in (run.state_root / "reports").glob("*.json"))
    check(not any(name in text for name in NAMES), "the report holds no name")
