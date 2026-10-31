from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import redownload as harness
from scripts.acquisition import redownload_route as route
from scripts.acquisition.cli_tools import run_argv
from scripts.acquisition.tests.test_run_lanes import LANES, UNITS, dispatch, lane_files, make_unit, refused
from scripts.process import CompletedProcess
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
    files.confirmation.unlink()
    refused(monkeypatch, run, "removal confirmation")


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


def private_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, receipt: str) -> tuple[dict, harness.Run]:
    """A run 3 person lane with one private unit whose stored receipt path carries a synthetic name."""
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    unit = make_unit("owners", 1) | {"receipt": receipt}
    queue = tmp_path / "queue.json"
    queue.write_text(json.dumps({"run": 3, "queue": [unit]}))
    run = harness.Run(queue_path=queue, state_root=tmp_path / "run", base=tmp_path, person_lane=True)
    run.cache["queue"] = [unit]
    run.state_root.mkdir()
    return unit, run


def everything_written(root: Path, *extra: str) -> str:
    texts = list(extra) + [path.read_text(errors="replace") for path in sorted(root.rglob("*")) if path.is_file()]
    return "\n".join(texts)


def test_the_parent_never_runs_a_private_unit_itself(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    unit, run = private_run(monkeypatch, tmp_path, "stored/owners/receipt.json")
    monkeypatch.setattr(harness, "handle", lambda unit, run: pytest.fail("the logging process loaded a private unit"))
    monkeypatch.setattr(harness, "in_reviewed_scope", lambda run, source: True)
    calls: list[str] = []

    def worker(unit: dict, run: Any) -> dict:
        calls.append(unit["snapshot_id"])
        return {"outcome": "exact_match", "attempts": 1, "stored_sha256": "a" * 64, "fresh_sha256": "a" * 64, "fresh_bytes": 3}

    monkeypatch.setattr(harness, "run_private", worker)
    summary = harness.execute_locked(run, False, True, None, None, None)
    check(calls == ["owners__1"] and summary["outcomes"] == {"exact_match": 1}, "the unit ran in the worker and its fixed result was recorded")


def test_the_worker_silences_its_output_and_a_real_failure_leaves_only_a_category(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    name_path = f"stored/{NAMES[0]}/receipt.json"
    unit, run = private_run(monkeypatch, tmp_path, name_path)
    result = tmp_path / "result.json"
    job = tmp_path / "job.json"
    job.write_text(
        json.dumps(
            {
                "queue_path": str(run.queue_path),
                "state_root": str(run.state_root),
                "base": str(tmp_path),
                "snapshot_id": unit["snapshot_id"],
                "result_path": str(result),
            }
        )
    )
    environment = {**os.environ, "PYTHONPATH": str(harness.REPO_ROOT), "HAI_CAPTURE_ROOT": "raw/"}
    done = run_argv([sys.executable, "-B", "-m", "scripts.acquisition.private_worker", str(job)], cwd=harness.REPO_ROOT, env=environment, timeout=120)
    check(done.stdout == "" and done.stderr == "", f"the worker printed nothing: {done.stdout!r} {done.stderr!r}")
    check(
        set(json.loads(result.read_text())) == {"kind", "category"} and json.loads(result.read_text())["kind"] == "failed",
        f"only a category was kept: {result.read_text()}",
    )
    check(
        not any(name in everything_written(run.state_root, result.read_text(), done.stdout, done.stderr) for name in NAMES[:1]),
        "the name is in no file or stream",
    )


def test_a_failing_diagnostic_writer_adds_no_name_and_no_chained_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    name_path = f"stored/{NAMES[2]}/receipt.json"
    unit, run = private_run(monkeypatch, tmp_path, name_path)
    (run.state_root / "private").write_text("a file where the diagnostics folder must go")
    result = tmp_path / "result.json"
    job = tmp_path / "job.json"
    job.write_text(
        json.dumps(
            {
                "queue_path": str(run.queue_path),
                "state_root": str(run.state_root),
                "base": str(tmp_path),
                "snapshot_id": unit["snapshot_id"],
                "result_path": str(result),
            }
        )
    )
    environment = {**os.environ, "PYTHONPATH": str(harness.REPO_ROOT), "HAI_CAPTURE_ROOT": "raw/"}
    done = run_argv([sys.executable, "-B", "-m", "scripts.acquisition.private_worker", str(job)], cwd=harness.REPO_ROOT, env=environment, timeout=120)
    check(done.returncode == 0 and done.stdout == "" and done.stderr == "", f"the worker still ended quietly: {done.returncode} {done.stderr!r}")
    check(
        json.loads(result.read_text())["kind"] == "failed" and NAMES[2] not in everything_written(run.state_root, result.read_text(), done.stdout, done.stderr),
        "no name anywhere",
    )


def test_run_private_turns_the_workers_answer_into_fixed_fields_and_a_missing_answer_into_a_block(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    name_path = f"stored/{NAMES[1]}/receipt.json"
    unit, run = private_run(monkeypatch, tmp_path, name_path)
    fields = harness.run_private(unit, run)
    check(
        fields["outcome"] == "blocked"
        and fields["error_category"] in {"registry_contract", "file_not_found"}
        and fields["reason"] == "collector_validation_failed",
        f"fixed fields only: {fields}",
    )
    check(NAMES[1] not in json.dumps(fields) + everything_written(run.state_root), "the name reached no field and no file")


def test_the_result_of_the_worker_is_filtered_again_by_the_parent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    unit, run = private_run(monkeypatch, tmp_path, "stored/owners/receipt.json")

    def forged(argv: Any, **kwargs: Any) -> Any:
        result = Path(json.loads(Path(argv[-1]).read_text())["result_path"])
        result.write_text(json.dumps({"kind": "ok", "fields": {"outcome": "exact_match", "reason": NAMES[0], "note": NAMES[1]}, "budget_used": 5}))
        return CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(harness, "run_argv", forged)
    fields = harness.run_private(unit, run)
    check(fields == {"outcome": "exact_match", "withheld_fields": 2}, f"the parent kept only fixed fields: {fields}")


def test_the_report_of_a_private_run_holds_no_name(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    unit, run = private_run(monkeypatch, tmp_path, "stored/owners/receipt.json")
    monkeypatch.setattr(harness, "in_reviewed_scope", lambda run, source: True)
    monkeypatch.setattr(
        harness, "run_private", lambda unit, run: {"outcome": "changed_needs_review", "attempts": 1, "stored_sha256": "a" * 64, "differing_file_count": 1}
    )
    harness.execute_locked(run, False, True, None, None, None)
    (run.state_root / "run.json").write_text("{}")
    run.cache["queue_sha256"] = "0" * 64
    monkeypatch.setattr(harness, "validate_root", lambda run: None)
    monkeypatch.setattr(harness, "require_run_binding", lambda run: None)
    document, _name = harness.write_report(run)
    check(document["outcomes"] == {"changed_needs_review": 1}, "the report counts the outcome only")


def noisy_child(silenced: bool) -> Any:
    environment = {**os.environ, "PYTHONPATH": str(harness.REPO_ROOT), "NOISY_NAME": NAMES[0]}
    return run_argv(
        [sys.executable, "-B", "-m", "scripts.acquisition.tests.noisy_child", "silenced" if silenced else "plain"],
        cwd=harness.REPO_ROOT,
        env=environment,
        timeout=60,
    )


def test_a_silenced_worker_prints_nothing_through_any_route_and_an_unsilenced_one_does() -> None:
    quiet, loud = noisy_child(True), noisy_child(False)
    check(NAMES[0] in loud.stdout + loud.stderr, "control: without the silencing the same code reaches the streams")
    check(
        quiet.stdout == "" and quiet.stderr == "",
        f"print, stderr, raw descriptors, logging, warnings, tracebacks and an uncaught error reach nothing: {quiet.stdout!r} {quiet.stderr!r}",
    )


def test_running_a_private_unit_in_the_logging_process_is_refused_on_the_raw_route_and_in_the_person_lane(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    unit, run = private_run(monkeypatch, tmp_path, "stored/owners/receipt.json")
    monkeypatch.setattr(harness, "in_reviewed_scope", lambda run, source: True)
    monkeypatch.setattr(harness, "handle", lambda unit, run: pytest.fail("a private unit was loaded in the logging process"))
    run.private_in_process = True
    with pytest.raises(ValueError, match="worker"):
        harness.execute_locked(run, False, True, None, None, None)
