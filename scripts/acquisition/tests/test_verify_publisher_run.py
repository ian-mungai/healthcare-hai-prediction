"""The owner's read-only check after a task: it recomputes every outcome from S3 by version and names a claim with no fresh capture inside the task's window."""

from __future__ import annotations

import dataclasses
import io
import json
import shutil
import tarfile
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import publisher_run
from scripts.acquisition import redownload as harness
from scripts.acquisition import verify_publisher_run as verifier
from scripts.acquisition.tests.task_fixture import task_run
from scripts.acquisition.tests.test_task_owners import TaskS3, Web, opener, storage
from scripts.acquisition.tests.test_task_raw_store import STORED, file_run, stored_unit
from tests.support import check

SINCE, UNTIL = datetime(2026, 11, 1, tzinfo=UTC), datetime(2026, 11, 2, tzinfo=UTC)


@pytest.fixture(autouse=True)
def raw_route(monkeypatch: pytest.MonkeyPatch) -> None:
    from scripts.acquisition import collection_layout, s3_store, transport

    monkeypatch.setattr(s3_store, "load_routes", collection_layout.load_routes)
    for name in (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE):
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")


class Stamped(TaskS3):
    """A bucket whose head answer gives the store's own time for each object."""

    def __init__(self, when: datetime) -> None:
        super().__init__()
        self.when = when

    def head_object(self, **arguments: Any) -> dict:
        return super().head_object(**arguments) | {"LastModified": self.when}


def state_body(run: harness.Run) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for path in sorted(run.state_root.rglob("*")):
            relative = path.relative_to(run.state_root).as_posix()
            if path.is_file() and publisher_run.SAVED.fullmatch(relative):
                archive.add(path, arcname=relative)
    return buffer.getvalue()


def checked(run: harness.Run, when: datetime, retrieved: str | None = None) -> dict:
    return verifier.check_state(run, state_body(run), SINCE, UNTIL, run.task_storage)


def test_a_real_run_inside_its_window_passes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = Stamped(SINCE + timedelta(hours=1))
    run, _unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    harness.execute(run)
    monkeypatch.setattr(verifier, "retrieved_at", lambda _receipt: SINCE + timedelta(hours=1))
    report = checked(run, SINCE)
    check(report == {"outcomes": 1, "verified": 1, "not_verified": [], "outside_window": []}, str(report))


@pytest.mark.parametrize("when", [SINCE - timedelta(days=30), UNTIL + timedelta(seconds=1)], ids=["old capture", "after the task"])
def test_a_match_whose_capture_is_stored_outside_the_window_is_named(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, when: datetime) -> None:
    bucket = Stamped(when)
    run, unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    harness.execute(run)
    monkeypatch.setattr(verifier, "retrieved_at", lambda _receipt: SINCE + timedelta(hours=1))
    check(checked(run, SINCE)["outside_window"] == [unit["snapshot_id"]], "the store's own time names it")


def test_a_receipt_that_says_it_was_fetched_outside_the_window_is_named(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = Stamped(SINCE + timedelta(hours=1))
    run, unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    harness.execute(run)
    monkeypatch.setattr(verifier, "retrieved_at", lambda _receipt: SINCE - timedelta(days=30))
    check(checked(run, SINCE)["outside_window"] == [unit["snapshot_id"]], "the receipt's own time names it")


def test_a_changed_capture_labelled_exact_is_named_and_the_check_does_not_stop(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = Stamped(SINCE + timedelta(hours=1))
    run, unit = file_run(monkeypatch, tmp_path, bucket, b"a,b\n3,4\n")
    harness.execute(run)
    path = run.state_root / "outcomes" / f"{unit['snapshot_id']}.json"
    path.write_text(json.dumps(json.loads(path.read_text()) | {"outcome": "exact_match"}))
    report = checked(run, SINCE)
    check(report["not_verified"] == [unit["snapshot_id"]] and report["verified"] == 0, str(report))


def test_the_verifier_makes_no_write(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = Stamped(SINCE + timedelta(hours=1))
    run, _unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    harness.execute(run)
    monkeypatch.setattr(verifier, "retrieved_at", lambda _receipt: SINCE + timedelta(hours=1))

    def refuse(**_arguments: Any) -> dict:
        raise AssertionError("the verifier wrote to the bucket")

    monkeypatch.setattr(bucket, "put_object", refuse)
    checked(run, SINCE)


def test_the_verifier_is_small() -> None:
    check(len(Path(verifier.__file__).read_text().splitlines()) <= 140, "the owner's check stays small enough to read")


def test_the_command_line_reports_and_exits_by_what_it_found(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    bucket = Stamped(SINCE + timedelta(hours=1))
    run, unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    harness.execute(run)
    body = state_body(run)
    key = "raw/run3_state/q/state.tar.gz"
    bucket.objects[key] = ("v9", body, {})
    named = ["--queue", str(run.queue_path), "--state-key", key, "--state-version", "v9"]
    arguments = [*named, "--state-sha256", harness.digest(body), "--since", SINCE.isoformat()]
    monkeypatch.setattr(verifier, "retrieved_at", lambda _receipt: SINCE + timedelta(hours=1))
    check(verifier.main([*arguments, "--until", UNTIL.isoformat()], run.task_storage, tmp_path) == 0, "a clean run exits 0")
    check(unit["snapshot_id"] not in capsys.readouterr().out, "a clean run names no unit")
    check(verifier.main([*arguments, "--until", SINCE.isoformat()], run.task_storage, tmp_path) == 1, "a capture outside the window exits 1")
    check(unit["snapshot_id"] in capsys.readouterr().out, "it names the unit")
    with pytest.raises(SystemExit):
        verifier.main([*named, "--state-sha256", "0" * 64, "--since", SINCE.isoformat(), "--until", UNTIL.isoformat()], run.task_storage, tmp_path)


@pytest.mark.parametrize(
    "change",
    [{"snapshot_id": "another_unit"}, {"source_id": "OTHER_SOURCE"}, {"mode": "other"}, {"route": "other"}, {"outcome": "verified"}],
    ids=["snapshot", "source", "mode", "route", "unknown label"],
)
def test_the_verifier_and_the_task_refuse_the_same_record(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, change: dict) -> None:
    bucket = Stamped(SINCE + timedelta(hours=1))
    run, unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    harness.execute(run)
    monkeypatch.setattr(verifier, "retrieved_at", lambda _receipt: SINCE + timedelta(hours=1))
    path = run.state_root / "outcomes" / f"{unit['snapshot_id']}.json"
    path.write_text(json.dumps(json.loads(path.read_text()) | change))
    report = checked(run, SINCE)
    check(report["not_verified"] == [unit["snapshot_id"]] and report["verified"] == 0, f"the verifier passed it: {report}")
    with pytest.raises(ValueError, match="outcome"):
        harness.execute(dataclasses.replace(run, cache={}))


def test_an_outcome_for_a_unit_outside_the_queue_is_named(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = Stamped(SINCE + timedelta(hours=1))
    run, _unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    harness.execute(run)
    monkeypatch.setattr(verifier, "retrieved_at", lambda _receipt: SINCE + timedelta(hours=1))
    (run.state_root / "outcomes" / "not_in_the_queue.json").write_text(json.dumps({"snapshot_id": "not_in_the_queue", "source_id": "X", "outcome": "blocked"}))
    check("not_in_the_queue" in checked(run, SINCE)["not_verified"], "a record of no queued unit is named")


def test_a_relabelled_unit_with_its_data_removed_fails_the_verifier(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from scripts.acquisition.tests.test_task_raw_store import drop_data

    bucket = Stamped(SINCE + timedelta(hours=1))
    run, unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    harness.execute(run)
    drop_data(bucket, run, unit)
    monkeypatch.setattr(verifier, "retrieved_at", lambda _receipt: SINCE + timedelta(hours=1))
    check(checked(run, SINCE)["not_verified"] == [unit["snapshot_id"]], "the reviewer's forgery is named")


@pytest.mark.parametrize("kind", ["nested", "link", "repeat"])
def test_a_state_the_task_would_refuse_fails_the_verifier(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, kind: str) -> None:
    bucket = Stamped(SINCE + timedelta(hours=1))
    run, _unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as saved:
        names = {"nested": ["outcomes/a/b.json"], "link": ["outcomes/a.json"], "repeat": ["outcomes/a.json", "outcomes/a.json"]}[kind]
        for name in names:
            info = tarfile.TarInfo(name)
            if kind == "link":
                info.type, info.linkname = tarfile.SYMTYPE, "../secret"
                saved.addfile(info)
            else:
                info.size = 2
                saved.addfile(info, io.BytesIO(b"{}"))
    report = verifier.check_state(run, buffer.getvalue(), SINCE, UNTIL, run.task_storage)
    check(report["not_verified"] == ["saved state"], f"the whole state is named: {report}")


def test_the_verifier_names_a_state_whose_ledgers_the_task_would_refuse(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bucket = Stamped(SINCE + timedelta(hours=1))
    run, _unit = file_run(monkeypatch, tmp_path, bucket, STORED)
    harness.execute(run)
    shutil.rmtree(run.state_root / ".budgets")
    monkeypatch.setattr(verifier, "retrieved_at", lambda _receipt: SINCE + timedelta(hours=1))
    check("saved state" in checked(run, SINCE)["not_verified"], "the verifier and the task agree")


# A replacement task inherits the approved outcomes of the task before it. Each outcome is windowed once, in the first saved state that holds it.
SINCE_2, UNTIL_2 = SINCE + timedelta(days=3), SINCE + timedelta(days=4)


class Timeline(TaskS3):
    """A bucket that keeps the store's own time of each object from the moment it was written, so two tasks have two windows."""

    def __init__(self) -> None:
        super().__init__()
        self.now = SINCE + timedelta(hours=1)
        self.written: dict[str, datetime] = {}

    def put_object(self, **arguments: Any) -> dict:
        self.written[arguments["Key"]] = self.now
        return super().put_object(**arguments)

    def head_object(self, **arguments: Any) -> dict:
        return super().head_object(**arguments) | {"LastModified": self.written.get(arguments["Key"], self.now)}


def zipped(value: int) -> bytes:
    """The same release for both units, with a fixed member time, so either order of the pilot stage gives an exact match."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(zipfile.ZipInfo("data.csv", (2026, 1, 1, 0, 0, 0)), f"a,b\n{value},2\n")
    return buffer.getvalue()


def two_tasks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, second_task_time: datetime) -> tuple[harness.Run, bytes, bytes, dict, dict]:
    """Task one finishes the first unit and saves; a replacement task restores that state, skips the first unit, captures the second and saves.

    Returns the replacement run, both saved states and the two units. The receipts' own retrieval times follow the time of their task.
    """
    bucket = Timeline()
    fixture, listed = stored_unit(monkeypatch, tmp_path, zipped(1), "one.zip")
    fixture.entries.append(stored_unit(monkeypatch, tmp_path, zipped(1), "two.zip")[1])
    run = task_run(tmp_path, fixture, opener=opener(Web({listed["url"]: zipped(1)})), task_storage=storage(bucket))
    check(harness.execute(run)["attempted"] == 1, "the pilot stage attempts one unit, so the task ends with the other still to do")
    done = {path.stem for path in (run.state_root / "outcomes").glob("*.json")}
    first, second = (unit for unit in sorted(fixture.entries, key=lambda unit: unit["snapshot_id"] not in done))
    first_state = state_body(run)
    retrieved: dict[str, datetime] = {}

    def remember() -> None:
        for key, (_version, body, _fields) in bucket.objects.items():
            if key.endswith("/receipt.json"):
                retrieved.setdefault(json.loads(body)["snapshot_id"], bucket.written[key])

    remember()
    bucket.now = second_task_time
    shutil.rmtree(run.state_root)
    again = dataclasses.replace(run, cache={})
    with tarfile.open(fileobj=io.BytesIO(first_state), mode="r:gz") as saved:
        saved.extractall(again.state_root, filter="data")
    harness.execute(again)
    remember()
    monkeypatch.setattr(verifier, "retrieved_at", lambda receipt: retrieved[receipt["snapshot_id"]])
    return again, first_state, state_body(again), first, second


def test_an_outcome_inherited_from_an_earlier_task_keeps_its_earlier_window(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    again, first_state, second_state, first, second = two_tasks(monkeypatch, tmp_path, SINCE_2 + timedelta(hours=1))
    earlier = verifier.check_state(dataclasses.replace(again, state_root=tmp_path / "unused"), first_state, SINCE, UNTIL, again.task_storage)
    check(earlier["outside_window"] == [] and earlier["verified"] == 1, f"the first task's own check passes in its own window: {earlier}")
    report = verifier.check_state(again, second_state, SINCE_2, UNTIL_2, again.task_storage, prior=first_state)
    check(report["outside_window"] == [] and report["not_verified"] == [], f"the inherited outcome is not held to the new window: {report}")
    check(report["verified"] == 2 and report["inherited"] == [first["snapshot_id"]], f"both outcomes are recomputed and one is inherited: {report}")
    without = verifier.check_state(again, second_state, SINCE_2, UNTIL_2, again.task_storage)
    check(without["outside_window"] == [first["snapshot_id"]], f"without the prior state the earlier capture is outside the new window: {without}")
    check(second["snapshot_id"] not in without["outside_window"], "the new capture is inside its own window")


def test_a_new_capture_outside_its_own_task_window_is_still_named(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    again, first_state, second_state, first, second = two_tasks(monkeypatch, tmp_path, SINCE - timedelta(days=30))
    report = verifier.check_state(again, second_state, SINCE_2, UNTIL_2, again.task_storage, prior=first_state)
    check(report["outside_window"] == [second["snapshot_id"]], f"only the new capture is checked against the new window: {report}")
    check(report["inherited"] == [first["snapshot_id"]], f"the inherited outcome stays inherited: {report}")


def test_an_inherited_record_that_differs_from_the_prior_state_is_a_new_claim(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    again, first_state, _second_state, first, _second = two_tasks(monkeypatch, tmp_path, SINCE_2 + timedelta(hours=1))
    path = again.state_root / "outcomes" / f"{first['snapshot_id']}.json"
    path.write_text(json.dumps(json.loads(path.read_text()), indent=1))
    report = verifier.check_state(again, state_body(again), SINCE_2, UNTIL_2, again.task_storage, prior=first_state)
    check(report["outside_window"] == [first["snapshot_id"]] and report["inherited"] == [], f"a rewritten record is windowed as new: {report}")


def test_a_prior_state_that_is_not_a_clean_record_set_inherits_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    again, _first_state, second_state, first, _second = two_tasks(monkeypatch, tmp_path, SINCE_2 + timedelta(hours=1))
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as saved:
        info = tarfile.TarInfo("outcomes/a/b.json")
        info.size = 2
        saved.addfile(info, io.BytesIO(b"{}"))
    report = verifier.check_state(again, second_state, SINCE_2, UNTIL_2, again.task_storage, prior=buffer.getvalue())
    check("prior state" in report["not_verified"], f"a bad prior state is named: {report}")
    check(report["inherited"] == [] and first["snapshot_id"] in report["outside_window"], f"it inherits nothing: {report}")


def test_the_command_line_takes_the_prior_state_as_one_named_set(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    again, first_state, second_state, first, _second = two_tasks(monkeypatch, tmp_path, SINCE_2 + timedelta(hours=1))
    bucket = again.task_storage.client
    bucket.objects["raw/run3_state/q/first.tar.gz"] = ("v1", first_state, {})
    bucket.objects["raw/run3_state/q/second.tar.gz"] = ("v2", second_state, {})
    named = [
        "--queue",
        str(again.queue_path),
        "--state-key",
        "raw/run3_state/q/second.tar.gz",
        "--state-version",
        "v2",
        "--state-sha256",
        harness.digest(second_state),
    ]
    window = ["--since", SINCE_2.isoformat(), "--until", UNTIL_2.isoformat()]
    prior = ["--prior-state-key", "raw/run3_state/q/first.tar.gz", "--prior-state-version", "v1", "--prior-state-sha256", harness.digest(first_state)]
    check(verifier.main([*named, *window, *prior], again.task_storage, tmp_path) == 0, "a replacement task with its prior state exits 0")
    check(first["snapshot_id"] in capsys.readouterr().out, "the report lists the inherited unit")
    check(verifier.main([*named, *window], again.task_storage, tmp_path) == 1, "without the prior state the earlier capture is outside the window")
    capsys.readouterr()
    with pytest.raises(SystemExit):
        verifier.main([*named, *window, *prior[:2]], again.task_storage, tmp_path)
    with pytest.raises(SystemExit):
        verifier.main([*named, *window, *prior[:5], "0" * 64], again.task_storage, tmp_path)
