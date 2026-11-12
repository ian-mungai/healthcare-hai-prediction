"""The run 3 publisher reads its config from the code root and never from the stored mirror.

A config file that is missing or changed in the code root fails.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from scripts.acquisition import collection_layout, s3_store, transport
from scripts.acquisition import redownload as harness
from scripts.acquisition.run_redownload_controls_e2e import approve_fixture
from scripts.acquisition.source_registry import REPO_ROOT
from scripts.acquisition.tests.test_queue_run3_scope import OUTPUT, WINDOWS, build, list_bls, write_root
from scripts.acquisition.tests.test_task_mmd import start as mmd_start
from scripts.acquisition.tests.test_task_owners import TaskS3
from scripts.acquisition.tests.test_task_raw_store import STORED, file_run
from tests.support import check

QUEUE_BUILDERS = REPO_ROOT / "scripts/acquisition/one_off/redownload"
NAME = "config/acquisition/example_plan.json"
GOOD = b'{"plan": "approved"}\n'
OTHER = b'{"plan": "another"}\n'


@pytest.fixture(autouse=True)
def raw_route(monkeypatch: pytest.MonkeyPatch) -> None:
    """The real collection routes, with the approved hosts names restored after each case."""
    monkeypatch.setattr(s3_store, "load_routes", collection_layout.load_routes)
    for name in (transport.APPROVED_HOSTS_VARIABLE, transport.APPROVED_HOSTS_APPROVAL_VARIABLE):
        monkeypatch.setenv(name, "placeholder")
        monkeypatch.delenv(name)


def write(root: Path, body: bytes, name: str = NAME) -> None:
    (root / name).parent.mkdir(parents=True, exist_ok=True)
    (root / name).write_bytes(body)


def task(tmp_path: Path, code: bytes | None = GOOD, mirrored: bytes | None = None) -> harness.Run:
    """A task run: the code root and the stored mirror are apart, and the controls bind the approved config bytes."""
    code_root, mirror = tmp_path / "code", tmp_path / "mirror"
    code_root.mkdir()
    mirror.mkdir()
    if code is not None:
        write(code_root, code)
    if mirrored is not None:
        write(mirror, mirrored)
    run = harness.Run(queue_path=tmp_path / "queue.json", state_root=tmp_path / "state", base=code_root, stored_root=mirror)
    run.cache["controls"] = {"operational_sha256": {NAME: harness.digest(GOOD)}}
    return run


def test_config_that_exists_only_in_the_code_root_passes(tmp_path: Path) -> None:
    harness.verify_config_inputs(task(tmp_path))


def test_a_different_copy_in_the_mirror_is_not_read(tmp_path: Path) -> None:
    harness.verify_config_inputs(task(tmp_path, mirrored=OTHER))


def test_a_mirror_copy_never_stands_in_for_config_that_is_missing_from_the_code_root(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="missing from the code root"):
        harness.verify_config_inputs(task(tmp_path, code=None, mirrored=GOOD))


def test_config_changed_in_the_code_root_fails_even_when_the_mirror_holds_the_approved_bytes(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Operational input changed"):
        harness.verify_config_inputs(task(tmp_path, code=OTHER, mirrored=GOOD))


def test_a_run_without_a_mirror_reads_the_same_code_root(tmp_path: Path) -> None:
    run = task(tmp_path)
    run.stored_root = None
    harness.verify_config_inputs(run)
    write(run.base, OTHER)
    with pytest.raises(ValueError, match="Operational input changed"):
        harness.verify_config_inputs(run)


def test_the_controls_bind_the_config_bytes_of_the_code_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run = task(tmp_path, mirrored=OTHER)
    monkeypatch.setattr(harness, "load_queue", lambda _run: [])
    monkeypatch.setattr(harness, "run_scope", lambda _run: None)
    monkeypatch.setattr(harness, "accepted", lambda _run: type("Accepted", (), {"queue_sha256": "0" * 64})())
    monkeypatch.setattr(harness, "read_json", lambda _path: {})
    monkeypatch.setattr(harness, "current", lambda relative: relative)
    bound = harness.runtime_controls(run)["operational_sha256"]
    check(bound[NAME] == harness.digest(GOOD), "the bound digest is the code root's file, not the mirror's copy")


def test_a_locked_plan_is_loaded_only_after_the_config_check_reads_the_code_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run = task(tmp_path, code=OTHER, mirrored=GOOD)
    monkeypatch.setattr(harness, "verify_path_map", lambda _run: None)
    monkeypatch.setattr(harness, "load_plans_of", lambda _mode: pytest.fail("a plan was loaded before the config check"))
    with pytest.raises(ValueError, match="Operational input changed"):
        harness.planned({"mode": "bls_api_v2"}, run)


def freeze_with_config(run: harness.Run) -> None:
    """Bind one config file of the code root in the run's controls, then change it right after the review gate accepts them."""
    write(run.base, GOOD)
    approve_fixture(run)


def change_after_gate(monkeypatch: pytest.MonkeyPatch, change: str) -> None:
    gate = harness.review_gate

    def gate_then_change(run: harness.Run) -> str:
        accepted = gate(run)
        if change == "removed":
            (run.base / NAME).unlink()
        else:
            write(run.base, OTHER)
        return accepted

    monkeypatch.setattr(harness, "review_gate", gate_then_change)


CHANGES = [("changed", "Operational input changed"), ("removed", "missing from the code root")]


@pytest.mark.parametrize(("change", "message"), CHANGES, ids=[name for name, _ in CHANGES])
def test_an_mmd_unit_stops_when_bound_config_changes_after_the_gate_and_sends_no_request(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, change: str, message: str
) -> None:
    task, _ = mmd_start(monkeypatch, tmp_path)
    freeze_with_config(task.run)
    change_after_gate(monkeypatch, change)
    before = list(task.world.cms.requests)
    with pytest.raises(ValueError, match=message):
        harness.execute(task.run)
    check(task.seen == [] and task.world.cms.requests == before, f"no unit was dispatched and no publisher request was sent: {task.seen}")
    check(not task.bucket.objects, "nothing was stored")


@pytest.mark.parametrize(("change", "message"), CHANGES, ids=[name for name, _ in CHANGES])
def test_a_file_unit_stops_when_bound_config_changes_after_the_gate_and_sends_no_request(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, change: str, message: str
) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    run, unit = file_run(monkeypatch, tmp_path, TaskS3(), STORED)
    freeze_with_config(run)
    change_after_gate(monkeypatch, change)
    web = run.opener.handlers[1].web
    with pytest.raises(ValueError, match=message):
        harness.execute(run)
    check(web.requests == 0 and harness.outcome(run, unit["snapshot_id"]) is None, "no request was sent and the unit has no outcome")


def test_bound_config_that_stays_unchanged_lets_an_mmd_unit_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    task, _ = mmd_start(monkeypatch, tmp_path)
    freeze_with_config(task.run)
    check(harness.execute(task.run)["outcomes"] == {"exact_match": 1}, "the MMD unit runs")


def test_bound_config_that_stays_unchanged_lets_a_file_unit_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HAI_CAPTURE_ROOT", "raw/")
    run, _unit = file_run(monkeypatch, tmp_path, TaskS3(), STORED)
    freeze_with_config(run)
    check(harness.execute(run)["outcomes"] == {"exact_match": 1}, "the file unit runs")


def inputs_keys(document: dict) -> list[str]:
    return list(document.get("inputs_sha256", {}))


def test_the_run_3_queue_builder_binds_no_config_path_as_an_input() -> None:
    """Config is bound by the controls from the code root. A frozen queue input under config/ would resolve through the stored mirror."""
    tree = ast.parse((QUEUE_BUILDERS / "build_queue_run3.py").read_text(encoding="utf-8"))
    bound = [
        key.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Dict)
        for name, value in zip(node.keys, node.values, strict=True)
        if isinstance(name, ast.Constant) and name.value == "inputs_sha256" and isinstance(value, ast.Dict)
        for key in value.keys
        if isinstance(key, ast.Constant) and isinstance(key.value, str)
    ]
    check(len(bound) >= 3, f"the builder's input list was found: {bound}")
    check(not [name for name in bound if name.startswith("config/")], f"a config path is bound as a queue input: {bound}")


def test_the_run_3_scope_builder_binds_no_config_path_as_an_input(tmp_path: Path) -> None:
    write_root(tmp_path, WINDOWS)
    list_bls(tmp_path)
    build(tmp_path, 2)
    document = json.loads((tmp_path / OUTPUT / "queue.json").read_text())
    check(document["run"] == 3 and len(inputs_keys(document)) >= 3, f"a scoped run 3 queue was built: {inputs_keys(document)}")
    check(not [name for name in inputs_keys(document) if name.startswith("config/")], f"a config path is bound as a queue input: {inputs_keys(document)}")
