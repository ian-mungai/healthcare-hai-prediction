"""Every engine-consuming phase of the layer driver derives its limits before its first engine or startup read, proved at run time and not by source lines.

Each case runs ``layer_run.main`` for one phase with the real ``task_capacity.apply``, in the environment of a live task (``AWS_EXECUTION_ENV`` set, the spill
folder named, none of the derived values set by hand) with stand-in task metadata and free space. The phase's own work is replaced by the reads that its engine
makes first: the DuckDB temp settings and connect options, the Spark heap and the thread count, and the dbt target
of a build. A phase that starts before ``apply`` fails its case. The last cases skip ``apply`` for one phase at a time and require that exact case to fail, so a
mutation of the driver cannot pass unseen."""

from __future__ import annotations

import contextlib
import json
import os
from functools import partial
from pathlib import Path
from typing import Any

import pytest

from scripts.features import feature_sets
from scripts.lakehouse import duckdb_bounds, ecs_engine, layer_run, pipeline_build, session, task_capacity
from tests.startup_check_support import Network, host_environment, install_network, profile_with_extensions
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
RUN = "silver_0a1b2c3d"
DIGEST = "a" * 64
GIB = 1024**3
DERIVED = ("DUCKDB_MEMORY_LIMIT", "JOB_THREADS", "SPARK_JOB_MEMORY", "HAI_DUCKDB_MAX_TEMP_BYTES", *task_capacity.THREAD_ALIASES)
PATHS = [
    (layer, phase, chosen) for layer, phases in layer_run.PHASES.items() for phase in phases for chosen in (feature_sets.SETS if layer == "features" else ("",))
]


class Reached(Exception):
    """The phase reached its own work after the startup check and the derivation of its limits."""


def task_document() -> dict[str, Any]:
    guard = {"Name": "guard", "KnownStatus": "RUNNING"}
    return {"container": {"Name": "build"}, "task": {"Limits": {"CPU": 4, "Memory": 16384}, "Containers": [guard, {"Name": "build", "KnownStatus": "RUNNING"}]}}


def stand_in(work: Path) -> partial[task_capacity.Limits | None]:
    return partial(task_capacity.apply, root=work / "no-cgroup", metadata=task_document(), affinity=4, disk_free=lambda _place: 100 * GIB)


def expected(work: Path, layer: str, phase: str) -> dict[str, str]:
    """The derived values this phase must hold when its work starts, derived on a copy of the task environment by the same ``apply``."""
    environ = {"AWS_EXECUTION_ENV": "AWS_ECS_EC2", "HAI_DUCKDB_TEMP_DIRECTORY": "/workspace/duckdb_spill"}
    stand_in(work)(layer_run.capacity_profile(layer, phase), environ)
    return {name: environ[name] for name in DERIVED}


def engine_reads(layer: str) -> dict[str, Any]:
    """What the first engine of a phase reads from the environment, through the production readers. Any missing derived value raises here."""
    found: dict[str, Any] = {
        "temp": ecs_engine.duckdb_temp_settings(),
        "options": duckdb_bounds.options(),
        "spark": session.job_memory(),
        "threads": session.job_threads(),
    }
    if layer in ("silver", "gold"):
        found["target"] = pipeline_build.build_target(layer, "real")
    return found


def prepare(monkeypatch: pytest.MonkeyPatch, work: Path, chosen: str) -> None:
    """The live task: in a task, the spill folder named, and no derived value, no cap, no thread count and no alias in the environment."""
    monkeypatch.setattr(os, "environ", os.environ.copy())
    host_environment(monkeypatch)
    for name in DERIVED:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("LAYER_PHASE", raising=False)
    monkeypatch.setenv("FEATURE_SET", chosen)
    monkeypatch.setenv("DATA_BUCKET_NAME", "example-bucket")
    install_network(monkeypatch, Network())
    profile_with_extensions(ROOT / "dbt/profiles.yml", work / "code/data/analytics/dbt/runs" / RUN / "snapshot/project/profiles.yml", work / "extensions")


def run(monkeypatch: pytest.MonkeyPatch, work: Path, layer: str, phase: str, chosen: str, skip: bool = False, child: bool = False) -> dict[str, Any]:
    """One phase through the real main. It returns what the phase's engine read, or raises what stopped the phase first."""
    prepare(monkeypatch, work, chosen)
    seen: dict[str, Any] = {}

    def dispatch(job: layer_run.Job, _phase: str, _args: Any) -> None:
        seen["reads"] = engine_reads(layer)
        seen["env"] = {name: os.environ.get(name, "") for name in DERIVED}
        if child:
            statement = "import json,os; print(json.dumps({k: os.environ[k] for k in " + repr(list(DERIVED)) + "}))"
            job.code.mkdir(parents=True, exist_ok=True)
            seen["child"] = json.loads(layer_run.run_module(job, "timeit", ["-n", "1", "-r", "1", statement]).splitlines()[0])
        raise Reached

    def at_boundary(real_check: Any) -> Any:
        """The derived values the environment holds at the moment a startup check starts."""

        def wrapped(*args: Any, **kwargs: Any) -> Any:
            seen.setdefault("boundary", {name: os.environ.get(name, "") for name in DERIVED})
            return real_check(*args, **kwargs)

        return wrapped

    real = stand_in(work)
    monkeypatch.setattr(layer_run.startup_check, "check_build", at_boundary(layer_run.startup_check.check_build))
    monkeypatch.setattr(layer_run.task_capacity, "apply", (lambda *_a, **_k: None) if skip else real)
    monkeypatch.setattr(layer_run, "task_deadline", lambda _job: contextlib.nullcontext())
    monkeypatch.setattr(layer_run, "check_code_run", lambda _job: None)
    monkeypatch.setattr(layer_run, "check_feature_set", lambda _job: None)
    monkeypatch.setattr(layer_run, "dispatch", dispatch)
    arguments = [phase, "--layer", layer, "--run", RUN, "--code-run-id", RUN, "--work", str(work), "--code-sha256", DIGEST, "--input-run", RUN]
    try:
        code = layer_run.main(arguments)
    except Reached:
        return seen
    raise AssertionError(f"{layer} {phase} {chosen!r}: the driver stopped with {code} before the phase's work")


def label(path: tuple[str, str, str]) -> str:
    return "-".join(part for part in path if part)


@pytest.mark.parametrize("path", PATHS, ids=label)
def test_every_phase_derives_its_limits_before_its_engine_reads_them(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, path: tuple[str, str, str]) -> None:
    layer, phase, chosen = path
    seen = run(monkeypatch, tmp_path, layer, phase, chosen)
    want = expected(tmp_path, layer, phase)
    check(seen["env"] == want, f"{label(path)} holds the derived values for its profile: {seen['env']} != {want}")
    check(seen["reads"]["temp"]["max_temp_directory_size"] == f"{want['HAI_DUCKDB_MAX_TEMP_BYTES']}B", f"the DuckDB cap is the derived one: {seen['reads']}")
    check(seen["reads"]["options"]["threads"] == want["JOB_THREADS"], f"the engine options carry the derived threads: {seen['reads']['options']}")


@pytest.mark.parametrize("path", [path for path in PATHS if path[1] == "build"], ids=label)
def test_a_build_holds_its_derived_limits_when_its_startup_check_starts(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, path: tuple[str, str, str]) -> None:
    """The build check reads the spill cap that only the derivation sets, so every build (silver, gold, each features set) derives first."""
    layer, phase, chosen = path
    seen = run(monkeypatch, tmp_path, layer, phase, chosen)
    want = expected(tmp_path, layer, phase)
    check(seen.get("boundary") == want, f"{label(path)} holds the derived values at the startup boundary: {seen.get('boundary')} != {want}")


def test_the_features_build_holds_fewer_threads_than_the_other_phases(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The profile follows the phase: the features build needs more memory for each thread."""
    build = run(monkeypatch, tmp_path / "a", "features", "build", "base")["env"]["JOB_THREADS"]
    other = run(monkeypatch, tmp_path / "b", "features", "validate", "base")["env"]["JOB_THREADS"]
    check(int(build) < int(other), f"features build {build} threads, other phases {other}")


@pytest.mark.parametrize("phase", ["validate", "export", "maintain"])
def test_a_child_process_of_a_phase_inherits_the_derived_values(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, phase: str) -> None:
    layer = "gold" if phase == "maintain" else "silver"
    seen = run(monkeypatch, tmp_path, layer, phase, "", child=True)
    check(seen["child"] == seen["env"] and all(seen["child"].values()), f"the child of {phase} holds what the parent derived: {seen['child']} != {seen['env']}")


@pytest.mark.parametrize("path", PATHS, ids=label)
def test_a_driver_that_skips_the_derivation_for_one_phase_fails_that_phase(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, path: tuple[str, str, str]) -> None:
    """The mutation check of the guard: with ``apply`` skipped, the case of the phase must stop before its work, by the startup check or by an engine read."""
    layer, phase, chosen = path
    with pytest.raises(Exception) as stopped:
        run(monkeypatch, tmp_path, layer, phase, chosen, skip=True)
    check("Reached" not in type(stopped.value).__name__, f"{label(path)} reached its work without derived limits")
