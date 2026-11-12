"""The run 3 publisher reads its config from the code root and never from the stored mirror.

A config file that is missing or changed in the code root fails.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.acquisition import redownload as harness
from tests.support import check

NAME = "config/acquisition/example_plan.json"
GOOD = b'{"plan": "approved"}\n'
OTHER = b'{"plan": "another"}\n'


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
