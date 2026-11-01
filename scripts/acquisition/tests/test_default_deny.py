"""A unit runs only if the queue carries its reviewed non personal classification. Anything else is refused before a byte is opened in every lane."""

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
from scripts.acquisition import transport, unit_classification
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.tests import held
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


REFUSING = {"no file", "stale queue", "edited without sealing", "no reviewer"}


def damage_classification(queue: Path, damage: str) -> None:
    path = queue.with_name(unit_classification.NAME)
    if damage == "no file":
        path.unlink()
        return
    document = json.loads(path.read_bytes())
    if damage == "unknown value":
        next(iter(document["units"].values()))["classification"] = "probably_fine"
    elif damage == "no entry":
        document["units"] = {}
    elif damage == "stale queue":
        document["queue_sha256"] = "0" * 64
    elif damage == "edited without sealing":
        next(iter(document["units"].values()))["source"] = "edited by hand"
        path.write_bytes(json.dumps(document).encode())
        return
    elif damage == "no reviewer":
        document["reviewed_by"] = None
    else:
        return
    path.write_bytes(encoded_json({**document, "classification_sha256": unit_classification.body_digest(document)}))


def synthetic_run(folder: Path, number: int, context: str, damage: str = "none") -> tuple[harness.Run, dict]:
    old, fresh = archive("ordinary.csv", b"a,b\n1,2\n"), archive(NAME + ".csv", b"a,b\n3,4\n")
    fixture = fixtures.Fixture(folder)
    unit = fixture.stored("main-hai-pdc", "data.zip", old)
    if damage == "proposed":
        fixture.unreviewed.add(unit["snapshot_id"])
    if damage == "personal":
        fixture.personal.add(unit["snapshot_id"])
    queue = fixture.freeze()
    document = json.loads(queue.read_bytes()) | {"run": number}
    queue.write_bytes(json.dumps(document).encode())
    queue.with_name("queue.lock.json").write_text(json.dumps({"entries": 1, "queue_sha256": harness.digest(queue.read_bytes())}))
    fixture.classify(queue)
    if damage not in REFUSING:
        damage_classification(queue, damage)
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
    if damage in REFUSING:
        damage_classification(queue, damage)
    return run, unit


CONTEXTS = [(2, "local"), (3, "task"), (3, "person")]


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


@pytest.mark.parametrize(("number", "context"), CONTEXTS)
@pytest.mark.parametrize("damage", ["no file", "stale queue", "edited without sealing", "no reviewer"])
def test_a_missing_stale_or_unsealed_classification_refuses_the_run_before_any_byte_is_opened(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, number: int, context: str, damage: str
) -> None:
    quiet_environment(monkeypatch, number)
    run, _unit = synthetic_run(tmp_path, number, context, damage)
    with pytest.raises(ValueError, match="classification|independent review"):
        harness.execute(run)
    nothing_was_opened(run)


@pytest.mark.parametrize(("number", "context"), CONTEXTS)
@pytest.mark.parametrize("damage", ["proposed", "unknown value", "no entry"])
def test_a_unit_without_a_reviewed_classification_is_named_and_never_attempted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, number: int, context: str, damage: str
) -> None:
    quiet_environment(monkeypatch, number)
    run, unit = synthetic_run(tmp_path, number, context, damage)
    summary = harness.execute(run)
    nothing_was_opened(run, summary)
    check(summary["not_attempted"]["not_reviewed"] == [unit["snapshot_id"]], f"the unit is named under its reason: {summary.get('not_attempted')}")


@pytest.mark.parametrize(("number", "context"), CONTEXTS)
def test_a_unit_reviewed_as_personal_is_named_and_never_attempted(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, number: int, context: str) -> None:
    quiet_environment(monkeypatch, number)
    run, unit = synthetic_run(tmp_path, number, context, "personal")
    summary = harness.execute(run)
    nothing_was_opened(run, summary)
    check(summary["not_attempted"]["private_local"] == [unit["snapshot_id"]], f"the unit is named once under its reason: {summary.get('not_attempted')}")


def test_a_unit_with_no_privacy_flag_is_not_taken_as_non_personal(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    quiet_environment(monkeypatch, 2)
    run, unit = synthetic_run(tmp_path, 2, "local", "proposed")
    check(unit["flags"] == [], "the unit carries no flag and its source is a known contacts table")
    harness.execute(run)
    nothing_was_opened(run)


def test_a_reviewed_non_personal_unit_still_runs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    quiet_environment(monkeypatch, 2)
    run, unit = synthetic_run(tmp_path, 2, "local")
    summary = harness.execute(run)
    check([unit["url"]] == REQUESTS and summary["attempted"] == 1, f"the reviewed unit was fetched once: {REQUESTS} {summary}")


def test_a_warmed_run_refuses_a_queue_that_changed_on_disk_after_its_review(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    quiet_environment(monkeypatch, 2)
    run, unit = synthetic_run(tmp_path, 2, "local")
    check(harness.load_queue(run)[0]["flags"] == [], "the run holds the queue as reviewed")
    document = json.loads(run.queue_path.read_bytes())
    document["queue"][0]["flags"] = ["original_contains_personal_details:private_owner_only"]
    body = json.dumps(document).encode()
    run.queue_path.write_bytes(body)
    run.queue_path.with_name("queue.lock.json").write_text(json.dumps({"entries": 1, "queue_sha256": harness.digest(body)}))
    with pytest.raises(ValueError, match="Execution controls changed|classification"):
        harness.execute(run)
    nothing_was_opened(run)


def test_a_warmed_run_refuses_a_classification_that_changed_on_disk_after_its_review(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    quiet_environment(monkeypatch, 2)
    run, unit = synthetic_run(tmp_path, 2, "local")
    harness.load_queue(run)
    path = run.queue_path.with_name(unit_classification.NAME)
    document = json.loads(path.read_bytes())
    document["units"][unit["snapshot_id"]]["classification"] = unit_classification.PERSONAL
    path.write_bytes(encoded_json({**document, "classification_sha256": unit_classification.body_digest(document)}))
    with pytest.raises(ValueError, match="Execution controls changed"):
        harness.execute(run)
    nothing_was_opened(run)


def test_a_run_that_started_holds_its_accepted_files_and_refuses_one_that_changes_before_the_next_unit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    quiet_environment(monkeypatch, 2)
    fixture = fixtures.Fixture(tmp_path)
    first = fixture.stored("ALPHA", "a.csv", b"a\n1\n")
    second = fixture.stored("BETA", "b.csv", b"b\n1\n")
    queue = fixture.freeze()
    run = harness.Run(queue, tmp_path / "state", base=tmp_path, sleep=lambda _s: None)
    control_cases.approve_fixture(run)
    seen: list[str] = []

    def handle(unit: dict, run: Any) -> dict:
        seen.append(unit["snapshot_id"])
        path = queue.with_name(unit_classification.NAME)
        document = json.loads(path.read_bytes())
        document["units"][second["snapshot_id"]]["classification"] = unit_classification.PERSONAL
        path.write_bytes(encoded_json({**document, "classification_sha256": unit_classification.body_digest(document)}))
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


def test_no_context_may_attempt_the_private_lane(tmp_path: Path) -> None:
    for fields in ({}, {"stored_root": tmp_path}, {"stored_root": tmp_path, "task_keys": object(), "task_storage": object()}, {"person_lane": True}):
        run = harness.Run(queue_path=tmp_path / "q.json", state_root=tmp_path / "s", base=tmp_path, **fields)
        check(harness.may_attempt(run, "private_local") is False, f"{fields} may not attempt it")
        check(harness.may_attempt(run, "not_reviewed") is False, f"{fields} may not attempt an unreviewed unit")


def test_the_flag_no_longer_decides_a_lane() -> None:
    source = Path(route.__file__).read_text()
    check(
        "is_private_unit" not in source and "PRIVATE_FLAG" not in source and "PRIVATE_COLLECTORS" not in source, "the flag rule is gone from the route module"
    )
    check(not hasattr(harness, "PRIVATE_FLAG"), "the harness holds no flag rule")


def test_the_removed_private_machinery_is_gone() -> None:
    for name in ("run_private", "private_safe", "private_in_process"):
        check(not hasattr(harness, name) and name not in harness.Run.__dataclass_fields__, f"{name} is gone")
    removed = "scripts.acquisition." + "private_" + "worker"
    check(importlib.util.find_spec(removed) is None, "the worker module is gone")


PRIVATE_MODES = ["hcai_util_workbook"]


@pytest.mark.parametrize("disposition", ["direct_download", "approved_manual", "api"])
@pytest.mark.parametrize("mode", PRIVATE_MODES)
def test_a_private_collector_mode_has_no_lane_even_when_reviewed_non_personal(mode: str, disposition: str) -> None:
    unit = make_unit("file", 0) | {"mode": mode, "disposition": disposition}
    check(harness.lane_of(unit, {unit["snapshot_id"]: "non_personal"}) == "private_local", f"{mode} as {disposition} is kept out of every lane")


@pytest.mark.parametrize(
    "fields", [{}, {"person_lane": True}, {"stored_root": "mirror"}, {"stored_root": "mirror", "keys": True}], ids=["local", "person", "task", "flags"]
)
def test_a_private_collector_unit_reviewed_non_personal_is_never_dispatched(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fields: dict) -> None:
    monkeypatch.delenv("HAI_CAPTURE_ROOT", raising=False)
    units = [
        make_unit("file", n) | {"snapshot_id": f"P__{n}", "source_id": f"P{n}", "mode": mode, "disposition": "approved_manual" if n % 2 else "direct_download"}
        for n, mode in enumerate(PRIVATE_MODES)
    ]
    monkeypatch.setattr(harness, "handle", lambda unit, run: pytest.fail(f"{unit['snapshot_id']} was dispatched"))
    monkeypatch.setattr(harness, "in_reviewed_scope", lambda run, source: True)
    fields = {key: tmp_path / value if isinstance(value, str) else value for key, value in fields.items()}
    if fields.pop("keys", False):
        fields |= {"task_keys": object(), "task_storage": object()}
    run = harness.Run(queue_path=tmp_path / "queue.json", state_root=tmp_path / "run", base=tmp_path, **fields)
    held.hold(run, units)
    run.state_root.mkdir()
    summary = harness.execute_locked(run, False, True, None, None, None)
    check(sorted(summary["not_attempted"]["private_local"]) == ["P__0"], f"it is named: {summary.get('not_attempted')}")


@pytest.mark.parametrize("mode", ["cms_owners_org", "onc_mu_hospital", "file", "hcai_util_workbook"])
def test_a_unit_reviewed_personal_runs_in_no_context_with_every_flag_on(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mode: str) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    unit = make_unit("file", 0) | {"snapshot_id": "P__0", "source_id": "P", "mode": mode}
    monkeypatch.setattr(harness, "handle", lambda unit, run: pytest.fail("a personal unit was dispatched"))
    monkeypatch.setattr(harness, "in_reviewed_scope", lambda run, source: True)
    run = harness.Run(
        queue_path=tmp_path / "queue.json", state_root=tmp_path / "run", base=tmp_path, stored_root=tmp_path, task_keys=object(), task_storage=object()
    )
    held.hold(run, [unit], {"P__0": "personal"})
    run.state_root.mkdir()
    summary = harness.execute_locked(run, False, True, None, None, None)
    check(summary["not_attempted"]["private_local"] == ["P__0"], f"the unit is named: {summary.get('not_attempted')}")
