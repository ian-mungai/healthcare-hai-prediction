"""The startup check of every other host job, through each job's real entry point, with stand-ins for the sockets only."""

from __future__ import annotations

import contextlib
import json
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from monitoring import aws_run as drift_run
from scripts.infrastructure import openmetadata_host_ingest, startup_check
from scripts.lakehouse import bronze_run, layer_run
from scripts.serving import deploy_job
from scripts.training import aws_run as training_run
from tests.startup_check_support import MLFLOW_URI, POLARIS_HOST, SERVICE_HOSTS, Network, host_environment, install_network
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
RUN = "20261111T000000Z-0a1b2c3d"
METADATA_HOST = "10.0.0.8"
METADATA_URI = f"http://{METADATA_HOST}:8585/api"
MLFLOW_HOST = "192.0.2.10"
ENTRIES = ("bronze_load", "train_extract", "train_train", "promote", "drift_baseline", "drift_current", "serve", "metadata", "features_feast")


class Reached(Exception):
    """The first real code of the job ran, so the startup check passed."""


@pytest.fixture(autouse=True)
def private_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Some entry points set variables of their own, so each test works on a copy."""
    monkeypatch.setattr(os, "environ", os.environ.copy())


def reach(module: object, name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    def seam(*_arguments: object, **_keywords: object) -> None:
        raise Reached

    monkeypatch.setattr(module, name, seam)


def run_bronze_phase(phase: str) -> Callable[[pytest.MonkeyPatch, Path], int]:
    def call(monkeypatch: pytest.MonkeyPatch, work: Path) -> int:
        reach(bronze_run.task_capacity, "apply", monkeypatch)
        return bronze_run.main([phase, "--run", RUN, "--work", str(work), "--bucket", "example-bucket", "--tables", "x", "--target", "qualify"])

    return call


run_bronze = run_bronze_phase("load")


def run_training(phase: str) -> Callable[[pytest.MonkeyPatch, Path], int]:
    def call(monkeypatch: pytest.MonkeyPatch, work: Path) -> int:
        reach(training_run.task_capacity, "apply", monkeypatch)
        return training_run.main([phase, "--run", RUN, "--work", str(work)])

    return call


def run_drift(phase: str) -> Callable[[pytest.MonkeyPatch, Path], int]:
    def call(monkeypatch: pytest.MonkeyPatch, work: Path) -> int:
        reach(drift_run.task_capacity, "apply", monkeypatch)
        return drift_run.main([phase, "--run", RUN, "--work", str(work)])

    return call


def run_serve(monkeypatch: pytest.MonkeyPatch, work: Path) -> int:
    reach(deploy_job.importlib, "import_module", monkeypatch)
    monkeypatch.setattr(sys, "argv", ["deploy_job", "--run", RUN])
    monkeypatch.setenv(
        "SERVING_REQUEST",
        json.dumps({"hai_type": "HAI_1", "deploy_mode": "stable", "candidate_version": None, "percent": 0, "canary_ttl_hours": None, "research_run": None}),
    )
    monkeypatch.setenv("MODEL_API_MANIFEST_URI", "s3://example-bucket/manifests")
    return deploy_job.main()


def run_metadata(monkeypatch: pytest.MonkeyPatch, work: Path) -> int:
    reach(openmetadata_host_ingest.ingest, "main", monkeypatch)
    environment = {"OM_API": METADATA_URI, "OM_INGESTION_TOKEN": "synthetic"}
    monkeypatch.setenv("OM_API", METADATA_URI)
    return openmetadata_host_ingest.main(["glue", "--config-dir", str(work), "--work-dir", str(work)], env=environment)


def run_features_phase(phase: str) -> Callable[[pytest.MonkeyPatch, Path], int]:
    def call(monkeypatch: pytest.MonkeyPatch, work: Path) -> int:
        reach(layer_run.task_capacity, "apply", monkeypatch)
        monkeypatch.setattr(layer_run, "task_deadline", lambda _job: contextlib.nullcontext())
        monkeypatch.setattr(layer_run, "check_code_run", lambda _job: None)
        monkeypatch.setenv("DATA_BUCKET_NAME", "example-bucket")
        arguments = [phase, "--layer", "features", "--run", RUN, "--code-run-id", RUN, "--work", str(work), "--input-run", RUN]
        return layer_run.main(arguments)

    return call


run_feast = run_features_phase("feast")


# Each job: its runner, the environment it needs and the connect that fails, as (host, port) with the cause named in the log.
JOBS: dict[str, tuple[Callable[[pytest.MonkeyPatch, Path], int], dict[str, Any], tuple[str, int], str]] = {
    "bronze_load": (run_bronze, {"polaris": True}, (POLARIS_HOST, 8181), "polaris_unreachable"),
    "train_extract": (run_training("extract"), {"polaris": True}, (POLARIS_HOST, 8181), "polaris_unreachable"),
    "train_train": (run_training("train"), {"mlflow": MLFLOW_URI}, (MLFLOW_HOST, 5000), "mlflow_unreachable"),
    "promote": (run_training("promote"), {"mlflow": MLFLOW_URI}, (MLFLOW_HOST, 5000), "mlflow_unreachable"),
    "drift_baseline": (run_drift("baseline"), {"mlflow": MLFLOW_URI}, (MLFLOW_HOST, 5000), "mlflow_unreachable"),
    "drift_current": (run_drift("current"), {"polaris": True}, (POLARIS_HOST, 8181), "polaris_unreachable"),
    "serve": (run_serve, {"mlflow": MLFLOW_URI}, (MLFLOW_HOST, 5000), "mlflow_unreachable"),
    "metadata": (run_metadata, {}, (METADATA_HOST, 8585), "openmetadata_unreachable"),
    "features_feast": (run_feast, {"polaris": True}, (POLARIS_HOST, 6379), "redis_unreachable"),
}


def prepare(
    monkeypatch: pytest.MonkeyPatch, entry: str, execution: str = "AWS_ECS_EC2", refuse: bool = False
) -> tuple[Callable[[pytest.MonkeyPatch, Path], int], Network]:
    runner, needs, address, _ = JOBS[entry]
    host_environment(monkeypatch, execution=execution, mlflow=needs.get("mlflow", ""))
    if entry == "metadata":
        monkeypatch.setenv("OM_API", METADATA_URI)
    network = install_network(monkeypatch, Network(refuse={address: TimeoutError("timed out")} if refuse else {}))
    return runner, network


@pytest.mark.parametrize("entry", ENTRIES)
def test_an_unreachable_service_is_named_in_the_first_log_lines(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str], entry: str
) -> None:
    runner, _ = prepare(monkeypatch, entry, refuse=True)
    code = runner(monkeypatch, tmp_path)
    lines = capsys.readouterr().err.splitlines()
    host, port = JOBS[entry][2]
    check(code != 0, f"{entry} stops with a failure code")
    check(lines[0].startswith("startup_check:"), f"the check speaks first: {lines[:1]}")
    check(f"{JOBS[entry][3]} {host}:{port}" in "\n".join(lines[:4]), f"the first lines name the address: {lines[:4]}")


@pytest.mark.parametrize("execution", ["AWS_ECS_EC2", "AWS_ECS_FARGATE"])
@pytest.mark.parametrize("entry", ENTRIES)
def test_a_job_with_its_services_reachable_reaches_its_own_code(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, entry: str, execution: str) -> None:
    runner, network = prepare(monkeypatch, entry, execution=execution)
    with pytest.raises(Reached):
        runner(monkeypatch, tmp_path)
    contacted = {host for host, _ in network.seen}
    check(contacted <= {POLARIS_HOST, MLFLOW_HOST, METADATA_HOST, *SERVICE_HOSTS}, f"only named endpoints were contacted: {network.seen}")
    check(JOBS[entry][2] in network.seen, f"the job's service {JOBS[entry][2]} was probed: {network.seen}")


@pytest.mark.parametrize("entry", ["train_extract", "metadata", "serve"])
def test_a_run_outside_a_task_checks_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, entry: str) -> None:
    runner = JOBS[entry][0]
    for name in ("AWS_EXECUTION_ENV", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "ECS_CONTAINER_METADATA_URI_V4", "ECS_CONTAINER_METADATA_URI"):
        monkeypatch.delenv(name, raising=False)
    network = install_network(monkeypatch, Network())
    monkeypatch.setenv("OM_API", METADATA_URI)
    with pytest.raises(Reached):
        runner(monkeypatch, tmp_path)
    check(network.seen == [], "no connect outside a task")


def test_the_first_phase_of_a_training_job_checks_every_service_the_job_uses(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    host_environment(monkeypatch, mlflow=MLFLOW_URI)
    network = install_network(monkeypatch, Network())
    with pytest.raises(Reached):
        run_training("prepare")(monkeypatch, tmp_path)
    check({(POLARIS_HOST, 8181), (MLFLOW_HOST, 5000)} <= set(network.seen), f"prepare probes Polaris and MLflow: {network.seen}")
    check({host for host, _ in network.seen} == {POLARIS_HOST, *SERVICE_HOSTS}, f"and nothing else: {network.seen}")


def test_a_failed_training_check_leaves_one_json_line_with_the_cause(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runner, _ = prepare(monkeypatch, "train_extract", refuse=True)
    runner(monkeypatch, tmp_path)
    last = json.loads(capsys.readouterr().err.splitlines()[-1])
    check(
        last["phase"] == "extract" and last["failure"] == "startup_check" and "polaris_unreachable" in last["detail"],
        f"the failure record names the cause: {last}",
    )


def test_every_host_job_has_a_row_in_the_check_table() -> None:
    # A model set family is like the features job and runs its code, so the check names the job features for it.
    table = json.loads((ROOT / "config/orchestration/host_jobs.json").read_text())["jobs"]
    jobs = {name for name, need in table.items() if "like" not in need}
    check(jobs <= set(startup_check.JOB_PHASES), f"jobs without a startup check row: {sorted(jobs - set(startup_check.JOB_PHASES))}")


# The first code step of a job, the service it must already reach there although a later phase uses it, and the connect that fails.
FIRST_STEPS = {
    "bronze_pin_polaris": (run_bronze_phase("pin"), (POLARIS_HOST, 8181), "polaris_unreachable"),
    "train_prepare_polaris": (run_training("prepare"), (POLARIS_HOST, 8181), "polaris_unreachable"),
    "train_prepare_mlflow": (run_training("prepare"), (MLFLOW_HOST, 5000), "mlflow_unreachable"),
    "drift_baseline_polaris": (run_drift("baseline"), (POLARIS_HOST, 8181), "polaris_unreachable"),
    "features_pin_redis": (run_features_phase("pin"), (POLARIS_HOST, 6379), "redis_unreachable"),
}


@pytest.mark.parametrize("step", FIRST_STEPS)
def test_the_first_code_step_stops_on_a_service_that_only_a_later_phase_uses(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str], step: str
) -> None:
    runner, address, cause = FIRST_STEPS[step]
    host_environment(monkeypatch, mlflow=MLFLOW_URI)
    install_network(monkeypatch, Network(refuse={address: TimeoutError("timed out")}))
    code = runner(monkeypatch, tmp_path)
    lines = capsys.readouterr().err.splitlines()
    check(code != 0, f"{step} stops with a failure code")
    check(f"{cause} {address[0]}:{address[1]}" in "\n".join(lines[:4]), f"the first lines name the address: {lines[:4]}")


def test_the_first_phase_of_every_job_checks_every_service_the_job_uses() -> None:
    for job, phases in startup_check.JOB_PHASES.items():
        first, *later = phases.values()
        used = set(first)
        check(set().union(*later) <= used, f"{job}: the first phase checks {sorted(used)} but a later phase needs {sorted(set().union(*later))}")


def test_the_first_container_of_each_job_receives_every_service_address_it_checks() -> None:
    """The broker fills the empty variables of a container by name, so a container whose check needs an address must list the variable."""
    jobs = json.loads((ROOT / "config/orchestration/ecs_broker_jobs.json").read_text())["jobs"]
    variables = {"polaris": "POLARIS_URI", "redis": "POLARIS_URI", "mlflow": "MLFLOW_TRACKING_URI"}
    missing = []
    for job, phases in startup_check.JOB_PHASES.items():
        for phase, services in phases.items():
            containers = jobs.get(job, {}).get("containers", {})
            missing += [
                f"{job} {phase} lacks {variables[service]}" for service in services if phase in containers and variables.get(service) not in containers[phase]
            ]
    check(not missing, f"containers whose startup check needs an address they are not given: {missing}")
