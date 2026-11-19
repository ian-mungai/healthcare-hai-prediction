"""The other AWS job entry points (bronze, training, drift and the direct bronze commands) derive their limits before their first engine read, at run time.

The same contract as ``tests/test_layer_capacity_phases.py``, for the entry points whose phases are not the layer driver's: the real ``main``, the real
``task_capacity.apply`` over stand-in task metadata and free space, a live-shaped environment with no derived value set by hand, and the phase's work
replaced by the reads that its engine makes first. Skipping ``apply`` must fail the case of the phase it is skipped for."""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from monitoring import aws_run as drift_run
from scripts.lakehouse import aws_bronze, bronze_run, bronze_writer
from scripts.training import aws_run as training_run
from tests.startup_check_support import MLFLOW_URI, Network, host_environment, install_network
from tests.support import check
from tests.test_layer_capacity_phases import DERIVED, Reached, engine_reads, stand_in

RUN = "silver_0a1b2c3d"
BRONZE_RUN = "bronze_0a1b2c3d"
Runner = Callable[[pytest.MonkeyPatch, Path, Callable[..., Any]], int]


def seam(seen: dict[str, Any]) -> Callable[..., Any]:
    """The first engine reads of a phase. It stops the phase, so nothing after them runs."""

    def reads(*_arguments: Any, **_keywords: Any) -> Any:
        seen["reads"] = engine_reads("training")
        seen["env"] = {name: os.environ.get(name, "") for name in DERIVED}
        raise Reached

    return reads


def training(phase: str) -> Runner:
    def call(monkeypatch: pytest.MonkeyPatch, work: Path, reads: Callable[..., Any]) -> int:
        monkeypatch.setattr(training_run, phase, reads)
        return training_run.main([phase, "--run", RUN, "--work", str(work)])

    return call


def drift(phase: str) -> Runner:
    def call(monkeypatch: pytest.MonkeyPatch, work: Path, reads: Callable[..., Any]) -> int:
        monkeypatch.setattr(drift_run, "prepare_environment", lambda _bucket: None)
        monkeypatch.setattr(drift_run, "open_store", lambda: None)
        monkeypatch.setattr(drift_run, "open_s3", lambda: None)
        monkeypatch.setattr(drift_run, "model_feature_list_sha", lambda _root, _model: None)
        monkeypatch.setattr(drift_run, "prepare_feature_reader", lambda _bucket, _work: None)
        monkeypatch.setattr(drift_run.current_window, "open_reader", lambda *_a, **_k: None)
        monkeypatch.setattr(drift_run, f"{phase}_phase", reads)
        return drift_run.main([phase, "--run", RUN, "--work", str(work), "--bucket", "example-bucket"])

    return call


def bronze(phase: str) -> Runner:
    def call(monkeypatch: pytest.MonkeyPatch, work: Path, reads: Callable[..., Any]) -> int:
        monkeypatch.setattr(bronze_run, "table_map", lambda _code: {})
        monkeypatch.setattr(bronze_run, "collections_of", lambda _config: {})
        monkeypatch.setattr(bronze_run, "check_dispatch", lambda *_a: ["x"])
        monkeypatch.setattr(bronze_run, "dispatch", reads)
        return bronze_run.main([phase, "--run", BRONZE_RUN, "--work", str(work), "--bucket", "example-bucket", "--tables", "x", "--target", "qualify"])

    return call


def direct_bronze(job: str) -> Runner:
    def call(monkeypatch: pytest.MonkeyPatch, work: Path, reads: Callable[..., Any]) -> int:
        binding = work / "binding.json"
        binding.write_text("{}")
        monkeypatch.setattr(bronze_writer.Binding, "from_document", classmethod(lambda _cls, _document: SimpleNamespace()))
        monkeypatch.setattr(bronze_writer, "plan", lambda *_a, **_k: SimpleNamespace(module="x", argv=()))
        monkeypatch.setattr(aws_bronze, "execute", reads)
        return aws_bronze.main(["--binding", str(binding), "--job", job, "--authority-epoch", "1"])

    return call


ENTRIES: dict[str, Runner] = {
    **{f"training-{phase}": training(phase) for phase in ("prepare", "extract", "train", "promote", "research", "canary", "notify")},
    **{f"drift-{phase}": drift(phase) for phase in ("baseline", "current", "check", "publish")},
    "bronze-load": bronze("load"),
    **{f"direct-bronze-{job}": direct_bronze(job) for job in bronze_writer.MUTATIONS},
}


def run(monkeypatch: pytest.MonkeyPatch, work: Path, entry: str, skip: bool = False) -> dict[str, Any]:
    """One phase through its real main: what its engine read first, or what stopped the phase before that."""
    monkeypatch.setattr(os, "environ", os.environ.copy())
    host_environment(monkeypatch, mlflow=MLFLOW_URI)
    for name in DERIVED:
        monkeypatch.delenv(name, raising=False)
    install_network(monkeypatch, Network())
    seen: dict[str, Any] = {}
    real = stand_in(work)
    monkeypatch.setattr(training_run.task_capacity, "apply", (lambda *_a, **_k: None) if skip else real)
    try:
        code = ENTRIES[entry](monkeypatch, work, seam(seen))
    except Reached:
        return seen
    raise AssertionError(f"{entry}: the entry point stopped with {code} before the phase's work")


@pytest.mark.parametrize("entry", sorted(ENTRIES))
def test_every_job_phase_derives_its_limits_before_its_engine_reads_them(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, entry: str) -> None:
    seen = run(monkeypatch, tmp_path, entry)
    check(
        all(seen["env"].values()) and seen["reads"]["temp"]["max_temp_directory_size"].endswith("B"), f"{entry} holds derived limits at its first read: {seen}"
    )


@pytest.mark.parametrize("entry", sorted(ENTRIES))
def test_an_entry_point_that_skips_the_derivation_fails_the_case_of_its_phase(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, entry: str) -> None:
    with pytest.raises(Exception) as stopped:
        run(monkeypatch, tmp_path, entry, skip=True)
    check(
        not isinstance(stopped.value, Reached) and "stopped with" not in str(stopped.value), f"{entry} reached its work without derived limits: {stopped.value}"
    )
