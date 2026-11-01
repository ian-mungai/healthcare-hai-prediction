"""The owner's read-only check after a task: it recomputes every outcome from S3 by version and names a claim with no fresh capture inside the task's window."""

from __future__ import annotations

import io
import json
import tarfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import redownload as harness
from scripts.acquisition import verify_publisher_run as verifier
from scripts.acquisition.tests.test_task_owners import TaskS3
from scripts.acquisition.tests.test_task_raw_store import STORED, file_run
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
        for path in sorted((run.state_root / "outcomes").glob("*.json")):
            archive.add(path, arcname=f"outcomes/{path.name}")
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
    check(len(Path(verifier.__file__).read_text().splitlines()) <= 120, "the owner's check stays small enough to read")


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
