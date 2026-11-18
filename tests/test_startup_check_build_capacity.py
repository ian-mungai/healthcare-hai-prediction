"""The build startup check, through the layer driver's real main path, with the capacity derived the way a task derives it.

``tests/test_startup_check.py`` stubs ``task_capacity.apply`` and its environment sets ``HAI_DUCKDB_MAX_TEMP_BYTES`` by hand, so it never saw the order of
the two steps. A host task does not name that cap: ``task_capacity.apply`` derives it from the free space of the spill filesystem, and the build startup
check (``build_target`` -> ``duckdb_temp_settings``) needs it. Here the real ``apply`` runs, with stand-in task metadata and free space, and nothing sets
the cap."""

from __future__ import annotations

import contextlib
import os
from functools import partial
from pathlib import Path
from typing import Any

import pytest

from scripts.infrastructure import startup_check
from scripts.lakehouse import layer_run
from tests.startup_check_support import POLARIS_HOST, Network, host_environment, install_network, profile_with_extensions
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
RUN = "silver_0a1b2c3d"
DIGEST = "a" * 64
GIB = 1024**3
FREE = 100 * GIB
CAP = 60 * GIB
SETTING = "temporary_storage_settings_incomplete"


class Reached(Exception):
    """The build code ran, so the startup check passed."""


def task_document(memory_mib: int) -> dict[str, Any]:
    guard = {"Name": "guard", "KnownStatus": "RUNNING"}
    build = {"Name": "build", "KnownStatus": "RUNNING"}
    return {"container": {"Name": "build"}, "task": {"Limits": {"CPU": 4, "Memory": memory_mib}, "Containers": [guard, build]}}


def drive(monkeypatch: pytest.MonkeyPatch, work: Path, layer: str = "silver", phase: str = "build", memory_mib: int = 16384) -> int:
    """Run the driver's main for one phase. The real ``apply`` reads stand-in task metadata and free space; the phase's own work is the seam that is reached."""

    def dispatch(*_arguments: Any) -> None:
        raise Reached

    real = layer_run.task_capacity.apply
    stand_in = partial(real, root=work / "no-cgroup", metadata=task_document(memory_mib), affinity=4, disk_free=lambda _place: FREE)
    monkeypatch.setattr(layer_run.task_capacity, "apply", stand_in)
    monkeypatch.setattr(layer_run, "task_deadline", lambda _job: contextlib.nullcontext())
    monkeypatch.setattr(layer_run, "check_code_run", lambda _job: None)
    monkeypatch.setattr(layer_run, "check_feature_set", lambda _job: None)
    monkeypatch.setattr(layer_run, "dispatch", dispatch)
    arguments = [phase, "--layer", layer, "--run", RUN, "--code-run-id", RUN, "--work", str(work), "--code-sha256", DIGEST, "--input-run", RUN]
    try:
        return layer_run.main(arguments)
    except Reached:
        return 0


def live_environment(monkeypatch: pytest.MonkeyPatch, work: Path) -> Network:
    """The environment of the live build container: in a task, the spill folder named, and the cap that the task definition does not carry unset."""
    # ``apply`` sets the engine variables in the process environment, so each test works on a copy.
    monkeypatch.setattr(os, "environ", os.environ.copy())
    host_environment(monkeypatch)
    monkeypatch.delenv("HAI_DUCKDB_MAX_TEMP_BYTES")
    network = install_network(monkeypatch, Network())
    profiles = work / "code/data/analytics/dbt/runs" / RUN / "snapshot/project/profiles.yml"
    profile_with_extensions(ROOT / "dbt/profiles.yml", profiles, work / "extensions")
    return network


@pytest.mark.parametrize("layer", ["silver", "gold"])
def test_a_build_in_a_task_derives_the_spill_cap_before_its_startup_check(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str], layer: str
) -> None:
    """Live failure, Nov 18: the build container exited 1 in 0.1 s with ``dbt_target_refused temporary_storage_settings_incomplete``."""
    live_environment(monkeypatch, tmp_path)
    code = drive(monkeypatch, tmp_path, layer)
    err = capsys.readouterr().err
    check(code == 0, f"the {layer} build reaches its own code, but the driver stopped with {code}: {err}")
    check(SETTING not in err, f"no spill finding: {err}")
    check(f'"HAI_DUCKDB_MAX_TEMP_BYTES": "{CAP}"' in err, f"the cap derived from the free space is logged: {err}")
    check(os.environ["HAI_DUCKDB_MAX_TEMP_BYTES"] == str(CAP), "the build container's environment holds the cap")
    check("ok dbt_target" in err, f"the selected target passes: {err}")


def test_a_task_that_names_no_spill_folder_is_still_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    live_environment(monkeypatch, tmp_path)
    monkeypatch.delenv("HAI_DUCKDB_TEMP_DIRECTORY")
    check(drive(monkeypatch, tmp_path) == 1, "a task without a spill folder stops")
    err = capsys.readouterr().err
    check(f"dbt_target_refused {SETTING}" in err, f"the startup check names the incomplete setting: {err}")
    check("HAI_DUCKDB_MAX_TEMP_BYTES" not in os.environ, "nothing derived a cap without a folder")


def test_a_task_too_small_for_the_engines_is_refused_by_the_capacity_with_its_own_text(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    live_environment(monkeypatch, tmp_path)
    check(drive(monkeypatch, tmp_path, memory_mib=1024) == 1, "a task below the floor stops")
    err = capsys.readouterr().err
    check("layer_run: step build (silver" in err and "below the floor" in err, f"the capacity refusal is named: {err}")
    check(SETTING not in err, f"the capacity refusal is not hidden behind a spill finding: {err}")


def test_the_pin_phase_still_checks_before_it_reads_the_capacity(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    live_environment(monkeypatch, tmp_path)
    install_network(monkeypatch, Network(refuse={POLARIS_HOST: TimeoutError("timed out")}))
    check(drive(monkeypatch, tmp_path, phase="pin") == 1, "an unreachable Polaris stops the pin phase")
    err = capsys.readouterr().err
    check(err.startswith("startup_check:") and "task_capacity:" not in err, f"the check refuses first and the capacity is not read: {err}")


def test_the_isolated_child_of_the_check_gets_the_derived_cap(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """A live container runs the check in a child process that receives the cap from this process's environment, as ``apply`` left it."""
    live_environment(monkeypatch, tmp_path)
    monkeypatch.setattr(startup_check, "ISOLATED", True)
    code = drive(monkeypatch, tmp_path)
    err = capsys.readouterr().err
    check(code == 0 and SETTING not in err, f"the child check passes with the derived cap: {err}")
