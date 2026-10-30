from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts.acquisition import redownload_route as route
from scripts.acquisition.s3_store import encoded_json
from tests.support import check


def unit(snapshot: str, disposition: str = "direct_download") -> dict:
    return {"snapshot_id": snapshot, "disposition": disposition}


def write_state(folder: Path, units: list[dict], removed: list[str]) -> dict[str, Path]:
    queue = folder / "queue.json"
    body = json.dumps({"queue": units}).encode()
    queue.write_bytes(body)
    lock = folder / "queue.lock.json"
    lock.write_text(json.dumps({"queue_sha256": hashlib.sha256(body).hexdigest()}))
    plan_body = {"kind": "unused_file_removal_plan", "items": [{"snapshot_id": name, "s3_key": f"raw/{name}"} for name in removed]}
    plan = folder / "removal_plan.json"
    plan.write_bytes(encoded_json({**plan_body, "plan_sha256": hashlib.sha256(encoded_json(plan_body)).hexdigest()}))
    plan_sha = json.loads(plan.read_text())["plan_sha256"]
    approval = folder / "approval.json"
    approval.write_text(json.dumps({"plan_sha256": plan_sha, "approved_by": "owner"}))
    record = folder / "removal_record.json"
    record.write_bytes(encoded_json({"plan_sha256": plan_sha, "items": []}))
    return {"queue": queue, "plan": plan, "approval": approval, "record": record}


def problems(paths: dict[str, Path], root: str = "raw/") -> list[str]:
    return route.check(paths["queue"], paths["plan"], paths["approval"], paths["record"], root)


def test_a_ready_route_has_no_problem(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1"), unit("S__2", "not_used_by_bronze")], ["S__9"])
    check(problems(paths) == [], "ready route has no problem")


def test_removal_must_be_recorded_before_run_3(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1")], ["S__9"])
    paths["record"].unlink()
    check(any("removal record" in item for item in problems(paths)), "missing removal record blocks run 3")


def test_the_record_must_belong_to_the_approved_plan(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1")], ["S__9"])
    paths["record"].write_bytes(encoded_json({"plan_sha256": "0" * 64, "items": []}))
    check(any("another plan" in item for item in problems(paths)), "record of another plan blocks run 3")


def test_a_removed_file_cannot_still_be_a_run_3_unit(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1"), unit("S__9")], ["S__9"])
    check(any("S__9" in item and "queue" in item for item in problems(paths)), "a unit the plan removes blocks run 3")


def test_the_capture_root_must_be_raw(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1")], [])
    check(any("raw/" in item for item in problems(paths, root="")), "root layout blocks run 3")


def test_a_queue_that_differs_from_its_lock_blocks_run_3(tmp_path: Path) -> None:
    paths = write_state(tmp_path, [unit("S__1")], [])
    paths["queue"].write_text(json.dumps({"queue": [unit("S__1"), unit("S__2")]}))
    check(any("lock" in item for item in problems(paths)), "queue changed after its lock blocks run 3")


def test_the_command_exits_with_the_problems(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    paths = write_state(tmp_path, [unit("S__1")], [])
    paths["record"].unlink()
    arguments = ["--queue", str(paths["queue"]), "--plan", str(paths["plan"]), "--approval", str(paths["approval"]), "--record", str(paths["record"])]
    with pytest.raises(SystemExit):
        route.main(arguments, root="raw/")
    check("removal record" in capsys.readouterr().err, "problem is printed")
