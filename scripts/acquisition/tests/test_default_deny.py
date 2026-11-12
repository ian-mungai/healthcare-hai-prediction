"""All the data is public: every unit takes its lane from its mode, and a run holds the queue it accepted. The personal handling is gone."""

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
from scripts.acquisition import task_role, transport
from scripts.acquisition.tests import test_redownload_route as route_cases
from scripts.acquisition.tests.test_run_lanes import make_unit
from tests.support import check

NAME = "Jane Q. Example"


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


def synthetic_run(folder: Path, number: int, context: str) -> tuple[harness.Run, dict]:
    old, fresh = archive("ordinary.csv", b"a,b\n1,2\n"), archive(NAME + ".csv", b"a,b\n3,4\n")
    fixture = fixtures.Fixture(folder)
    unit = fixture.stored("main-hai-pdc", "data.zip", old)
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
        fields |= {"stored_root": mirror, "task_storage": object()}
    if context == "person":
        fields |= {"person_lane": True}
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
        if context == "person":
            approval = folder / "lane_approval.json"
            route.write_lane_template(queue, approval)
            document = json.loads(approval.read_text())
            approval.write_text(json.dumps(document | {"approved_by": "synthetic_owner"}))
            run.lane_approval = approval
    return run, unit


def quiet_environment(monkeypatch: pytest.MonkeyPatch, number: int) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/" if number == 3 else "")
    for name in harness.TASK_ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    for name in ("HAI_APPROVED_HOSTS", "HAI_APPROVED_HOSTS_APPROVAL"):
        if number == 3:
            monkeypatch.setenv(name, "placeholder")
        else:
            monkeypatch.delenv(name, raising=False)


def nothing_was_opened(run: harness.Run, summary: object = None) -> None:
    check(REQUESTS == [], f"no request was made: {REQUESTS}")
    check(not list((run.state_root / "outcomes").glob("*.json")), "no outcome was written")
    check(not (run.state_root / "private").exists() and not (run.state_root / "files").exists(), "no original or fresh file was kept in the state")
    text = "\n".join(path.read_text(errors="replace") for path in run.state_root.rglob("*") if path.is_file()) if run.state_root.exists() else ""
    check(NAME not in text and NAME not in json.dumps(summary), "no name reached the state or the summary")


def test_a_unit_whose_original_names_a_person_runs_like_any_other(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The hospitals archive holds a table of named state contacts. It is public, so the unit is fetched with no classification file at all."""
    quiet_environment(monkeypatch, 2)
    run, unit = synthetic_run(tmp_path, 2, "local")
    check(not run.queue_path.with_name("unit_classification.json").exists(), "the queue carries no classification")
    summary = harness.execute(run)
    check([unit["url"]] == REQUESTS and summary["attempted"] == 1, f"the unit was fetched once: {REQUESTS} {summary}")


def test_a_warmed_run_refuses_a_queue_that_changed_on_disk_after_its_review(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    quiet_environment(monkeypatch, 2)
    run, unit = synthetic_run(tmp_path, 2, "local")
    check(harness.load_queue(run)[0]["flags"] == [], "the run holds the queue as reviewed")
    document = json.loads(run.queue_path.read_bytes())
    document["queue"][0]["flags"] = ["changed_after_review"]
    body = json.dumps(document).encode()
    run.queue_path.write_bytes(body)
    run.queue_path.with_name("queue.lock.json").write_text(json.dumps({"entries": 1, "queue_sha256": harness.digest(body)}))
    with pytest.raises(ValueError, match="Execution controls changed"):
        harness.execute(run)
    nothing_was_opened(run)


def test_a_run_that_started_holds_its_accepted_queue_and_refuses_one_that_changes_before_the_next_unit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    quiet_environment(monkeypatch, 2)
    fixture = fixtures.Fixture(tmp_path)
    first = fixture.stored("ALPHA", "a.csv", b"a\n1\n")
    fixture.stored("BETA", "b.csv", b"b\n1\n")
    queue = fixture.freeze()
    run = harness.Run(queue, tmp_path / "state", base=tmp_path, sleep=lambda _s: None)
    control_cases.approve_fixture(run)
    seen: list[str] = []

    def handle(unit: dict, run: Any) -> dict:
        seen.append(unit["snapshot_id"])
        queue.write_bytes(queue.read_bytes() + b" ")
        return {"outcome": "exact_match"}

    monkeypatch.setattr(harness, "handle", handle)
    monkeypatch.setattr(harness, "acknowledged", lambda run: set())
    with pytest.raises(ValueError, match="changed"):
        harness.execute(run)
    check(seen == [first["snapshot_id"]], f"the second unit was never attempted: {seen}")


def test_the_units_a_run_dispatches_cannot_be_changed_through_the_list_it_was_given(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    quiet_environment(monkeypatch, 2)
    run, _unit = synthetic_run(tmp_path, 2, "local")
    harness.load_queue(run)[0]["snapshot_id"] = "tampered"
    check(harness.load_queue(run)[0]["snapshot_id"] != "tampered", "a caller's change never reaches the accepted queue")


@pytest.mark.parametrize("mode", ["cms_owners_org", "onc_mu_hospital", "hcai_util_workbook"])
def test_a_collector_whose_original_names_people_takes_the_plain_task_lane(mode: str) -> None:
    check(harness.lane_of(make_unit("file", 0) | {"mode": mode, "disposition": "direct_download"}) == "task", f"{mode} runs on the task")


def test_the_removed_personal_handling_is_gone() -> None:
    for name in ("unit_classification", "private_digest", "private_" + "worker"):
        check(importlib.util.find_spec("scripts.acquisition." + name) is None, f"the {name} module is gone")
    for name in ("PRIVATE", "NOT_REVIEWED", "TASK_FILTERED", "FILTERED_MODES", "NEVER_IN_THE_TOOL", "filtered_capture", "run_private"):
        check(not hasattr(harness, name), f"the harness has no {name}")
    check("task_filtered" not in harness.Run.__dataclass_fields__, "the run has no filtered switch")
    check(not hasattr(task_role, "FILTERED_VARIABLE") and not hasattr(task_role, "ORIGINAL_FOLDER"), "the task has no filtered lane or original folder")
    source = Path(route.__file__).read_text()
    check("unit_classification" not in source and "classification_sha256" not in source, "the route gate and the lane approval name no classification")
