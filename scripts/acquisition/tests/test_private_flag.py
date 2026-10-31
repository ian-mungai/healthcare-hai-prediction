"""A unit whose original is flagged as naming people never runs in this tool, in any lane or context. The owner checks it locally with its own script."""

from __future__ import annotations

import importlib.util
import io
import json
import urllib.request
import urllib.response
import zipfile
from email.message import Message
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import redownload as harness
from scripts.acquisition import redownload_route as route
from scripts.acquisition import run_redownload_controls_e2e as control_cases
from scripts.acquisition import run_redownload_e2e as fixtures
from scripts.acquisition import transport
from scripts.acquisition.tests import test_redownload_route as route_cases
from scripts.acquisition.tests.test_run_lanes import make_unit
from tests.support import check

NAME = "Jane Q. Example"
FLAG = "original_contains_personal_details:private_owner_only"


def archive(member: str, body: bytes) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as output:
        output.writestr(member, body)
    return buffer.getvalue()


REQUESTS: list[str] = []


class Answer(urllib.response.addinfourl):
    """An addinfourl with the message urllib reads."""

    msg = "OK"


class Replay(urllib.request.HTTPSHandler):
    """Replays the review's changed ZIP, whose one member is named after a synthetic person, and counts every request."""

    def __init__(self, body: bytes) -> None:
        super().__init__()
        self.body = body

    def https_open(self, request: Any) -> Any:
        REQUESTS.append(request.full_url)
        headers = Message()
        headers["Content-Type"] = "application/zip"
        headers["Content-Length"] = str(len(self.body))
        return Answer(io.BytesIO(self.body), headers, request.full_url, 200)


def flagged_run(folder: Path, number: int, context: str) -> tuple[harness.Run, dict]:
    old, fresh = archive("ordinary.csv", b"a,b\n1,2\n"), archive(NAME + ".csv", b"a,b\n3,4\n")
    fixture = fixtures.Fixture(folder)
    unit = fixture.stored("main-hai-pdc", "data.zip", old, flags=[FLAG])
    queue = fixture.freeze()
    document = json.loads(queue.read_bytes()) | {"run": number}
    queue.write_bytes(json.dumps(document).encode())
    queue.with_name("queue.lock.json").write_text(json.dumps({"entries": 1, "queue_sha256": harness.digest(queue.read_bytes())}))
    REQUESTS.clear()
    opener = transport.ReviewedTransport(urllib.request.ProxyHandler({}), Replay(fresh))
    fields: dict[str, Any] = {"opener": opener, "sleep": lambda _s: None}
    if context == "task":
        mirror = folder / "mirror"
        for source in (folder / unit["receipt"]).parent.rglob("*"):
            if source.is_file():
                target = mirror / source.relative_to(folder)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(source.read_bytes())
        fields |= {"stored_root": mirror}
    if context == "person":
        fields |= {"person_lane": True}
    if context == "keyed":
        fields |= {"keyed_lane": True}
    run = harness.Run(queue, folder / "state", base=folder, **fields)
    control_cases.approve_fixture(run)
    if number == 3:
        route_folder = folder / "route"
        route_folder.mkdir()
        paths = route_cases.write_state(route_folder, [route_cases.unit("unused")], [])
        route.derive_hosts(queue, folder / "hosts.json")
        (folder / "hosts_approval.json").write_text(
            json.dumps({"hosts_sha256": json.loads((folder / "hosts.json").read_bytes())["hosts_sha256"], "approved_by": "synthetic_owner"})
        )
        run.route = route.RouteFiles(paths["plan"], paths["confirmation"], folder / "hosts.json", folder / "hosts_approval.json")
        if context in {"person", "keyed"}:
            approval = folder / "lane_approval.json"
            route.write_lane_template(queue, approval, route.PERSON_LANE if context == "person" else route.LANE_NAME)
            document = json.loads(approval.read_text())
            approval.write_text(json.dumps(document | {"approved_by": "synthetic_owner"}))
            run.lane_approval = approval
    return run, unit


@pytest.mark.parametrize(("number", "context"), [(2, "local"), (3, "task"), (3, "person"), (3, "keyed")])
def test_a_flagged_file_is_never_downloaded_compared_or_recorded_in_any_context(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, number: int, context: str
) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/" if number == 3 else "")
    for name in harness.TASK_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    for name in ("HAI_APPROVED_HOSTS", "HAI_APPROVED_HOSTS_APPROVAL"):
        if number == 3:
            monkeypatch.setenv(name, "placeholder")
        else:
            monkeypatch.delenv(name, raising=False)
    run, unit = flagged_run(tmp_path, number, context)
    summary = harness.execute(run)
    check(REQUESTS == [], f"no request was made for the flagged unit: {REQUESTS}")
    check(not list((run.state_root / "outcomes").glob("*.json")), "no outcome was written")
    check(not (run.state_root / "private").exists() and not (run.state_root / "files").exists(), "no original or fresh file was kept in the state")
    text = "\n".join(path.read_text(errors="replace") for path in run.state_root.rglob("*") if path.is_file())
    check(NAME not in text and NAME not in json.dumps(summary), "no name reached the state or the summary")
    check(summary["not_attempted"]["private_local"] == [unit["snapshot_id"]], f"the unit is named once under its reason: {summary.get('not_attempted')}")


def test_the_flag_decides_the_lane_before_any_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    for kind in ("file", "page", "mmd", "bls", "hud_xlsx", "manual_file", "wonder"):
        unit = make_unit(kind, 0) | {"flags": [FLAG]}
        check(harness.lane_of(unit) == "private_local", f"a flagged {kind} unit is private")
    check(harness.lane_of(make_unit("file", 0)) == "task", "an unflagged file unit is still the task's")


def test_a_private_collector_mode_is_private_even_without_the_flag() -> None:
    for kind in ("owners", "hcai", "onc"):
        check(harness.lane_of(make_unit(kind, 0) | {"flags": []}) == "private_local", f"{kind} is private by its mode")


def test_no_context_may_attempt_the_private_lane(tmp_path: Path) -> None:
    for fields in ({}, {"stored_root": tmp_path}, {"keyed_lane": True}, {"person_lane": True}):
        run = harness.Run(queue_path=tmp_path / "q.json", state_root=tmp_path / "s", base=tmp_path, **fields)
        check(harness.may_attempt(run, "private_local") is False, f"{fields} may not attempt it")


def test_the_removed_private_machinery_is_gone() -> None:
    for name in ("run_private", "private_safe", "private_in_process"):
        check(not hasattr(harness, name) and name not in harness.Run.__dataclass_fields__, f"{name} is gone")
    removed = "scripts.acquisition." + "private_" + "worker"
    check(importlib.util.find_spec(removed) is None, "the worker module is gone")
