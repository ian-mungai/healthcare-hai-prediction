"""End-to-end check of the run 3 controls freeze: the controls it writes are the ones the publisher task computes from the staged mirror."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import shutil
from pathlib import Path
from types import ModuleType

import pytest

from scripts.acquisition import redownload
from scripts.acquisition.collect_mmd_api import TEMPLATE
from scripts.acquisition.s3_store import encoded_json
from tests.support import check

SCRIPT = Path(__file__).resolve().parents[1] / "one_off/redownload/freeze_controls_run3.py"
QUEUE_FOLDER = "data/redownload_checks/20261111_bls2006"


def freezer() -> ModuleType:
    spec = importlib.util.spec_from_file_location("freeze_controls_run3", SCRIPT)
    check(spec is not None and spec.loader is not None, "the freeze script exists")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_data_root(root: Path) -> Path:
    queue = {
        "run": 3,
        "inputs_sha256": {},
        "queue": [{"snapshot_id": "S__1", "source_id": "S", "mode": "file", "disposition": "not_used_by_bronze", "route": None, "flags": []}],
    }
    body = (json.dumps(queue, indent=1, sort_keys=True) + "\n").encode()
    folder = root / QUEUE_FOLDER
    folder.mkdir(parents=True)
    (folder / "queue.json").write_bytes(body)
    (folder / "queue.lock.json").write_text(json.dumps({"queue_sha256": hashlib.sha256(body).hexdigest(), "entries": 1}))
    (root / TEMPLATE).parent.mkdir(parents=True)
    (root / TEMPLATE).write_text("{}")
    return folder / "queue.json"


def test_the_frozen_controls_equal_the_controls_the_task_computes_from_the_mirror(tmp_path: Path) -> None:
    queue = write_data_root(tmp_path / "mac")
    state = tmp_path / "state"
    freezer().main(["--queue", str(queue), "--data-root", str(tmp_path / "mac"), "--state-parent", str(state)])
    frozen = json.loads((queue.parent / "controls.json").read_text())
    mirror = tmp_path / "mirror"
    shutil.copytree(tmp_path / "mac", mirror)
    task_state = tmp_path / "task_state"
    run = redownload.Run(queue_path=mirror / QUEUE_FOLDER / "queue.json", state_root=task_state / "run", state_parent=task_state, stored_root=mirror)
    check(redownload.runtime_controls(run) == frozen, "the task computes the frozen controls from the mirror layout")
    check(frozen["state_root"] == "run", "the controls bind the task's state root")


def test_the_controls_are_written_once_and_never_overwritten(tmp_path: Path) -> None:
    queue = write_data_root(tmp_path / "mac")
    arguments = ["--queue", str(queue), "--data-root", str(tmp_path / "mac"), "--state-parent", str(tmp_path / "state")]
    freezer().main(arguments)
    written = (queue.parent / "controls.json").read_bytes()
    freezer().main(arguments)
    check((queue.parent / "controls.json").read_bytes() == written, "a second freeze of the same inputs changes nothing")
    (queue.parent / "controls.json").write_bytes(encoded_json({"changed": True}))
    with pytest.raises(SystemExit) as stopped:
        freezer().main(arguments)
    check("do not overwrite" in str(stopped.value), "different controls are never overwritten")


def test_a_code_tree_other_than_the_one_running_is_refused(tmp_path: Path) -> None:
    queue = write_data_root(tmp_path / "mac")
    with pytest.raises(SystemExit) as stopped:
        freezer().main(["--queue", str(queue), "--data-root", str(tmp_path / "mac"), "--state-parent", str(tmp_path / "state"), "--code-root", str(tmp_path)])
    check("code root" in str(stopped.value), "the freeze runs from the code tree it binds")
