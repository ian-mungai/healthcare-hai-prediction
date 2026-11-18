"""The host job startup check, through the layer driver's real entry point, with stand-ins for the sockets only."""

from __future__ import annotations

import contextlib
import socket
import time
from pathlib import Path
from typing import Any

import pytest

from scripts.infrastructure import startup_check
from scripts.lakehouse import layer_run
from tests.startup_check_support import (
    MLFLOW_URI,
    POLARIS_HOST,
    REGISTRY_HOST,
    SERVICE_HOSTS,
    Network,
    host_environment,
    install_network,
    profile_with_extensions,
)
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
RUN = "silver_0a1b2c3d"
DIGEST = "a" * 64


class Reached(Exception):
    """The first real phase code ran, so the startup check passed."""


def drive(monkeypatch: pytest.MonkeyPatch, work: Path, phase: str, layer: str = "silver") -> int:
    """Run the driver's main for one phase with the capacity, deadline and dispatch seams stubbed. Reaching dispatch means the check let the phase start."""

    def dispatch(*_arguments: Any) -> None:
        raise Reached

    monkeypatch.setattr(layer_run.task_capacity, "apply", lambda _profile: None)
    monkeypatch.setattr(layer_run, "task_deadline", lambda _job: contextlib.nullcontext())
    monkeypatch.setattr(layer_run, "check_code_run", lambda _job: None)
    monkeypatch.setattr(layer_run, "check_feature_set", lambda _job: None)
    monkeypatch.setattr(layer_run, "dispatch", dispatch)
    arguments = [phase, "--layer", layer, "--run", RUN, "--code-run-id", RUN, "--work", str(work), "--code-sha256", DIGEST, "--input-run", RUN]
    try:
        return layer_run.main(arguments)
    except Reached:
        return 0


def profiles_of(work: Path) -> Path:
    """Where the build reads its profiles: the sealed project in the run folder."""
    return work / "code/data/analytics/dbt/runs" / RUN / "snapshot/project/profiles.yml"


def first_lines(capsys: pytest.CaptureFixture[str]) -> list[str]:
    return capsys.readouterr().err.splitlines()


def test_a_polaris_address_that_times_out_is_named_in_the_first_log_lines(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Live failure 1: job containers could not reach Polaris at the host address and the connect timed out."""
    host_environment(monkeypatch)
    install_network(monkeypatch, Network(refuse={POLARIS_HOST: TimeoutError("timed out")}))
    check(drive(monkeypatch, tmp_path, "pin") == 1, "the pin phase stops")
    lines = first_lines(capsys)
    check(lines[0].startswith("startup_check:"), f"the check speaks first: {lines[:1]}")
    text = "\n".join(lines[:4])
    check(f"polaris_unreachable {POLARIS_HOST}:8181" in text and "timed out" in text, f"the log names the address and the cause: {text}")
    check("layer_run: step pin (silver" in lines[-2], "the driver reports the failed step")
    check(lines[-1].startswith("phase_failure: ") and '"phase":"pin"' in lines[-1], f"and ends with the phase failure line the launcher keeps: {lines[-1]}")


def test_a_dbt_target_that_would_download_extensions_is_named_before_the_build(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Live failure 2: the host job was not treated as a task, so dbt selected the session profile and tried to download an extension."""
    host_environment(monkeypatch, execution="AWS_ECS_EXTERNAL")
    install_network(monkeypatch, Network())
    profile_with_extensions(ROOT / "dbt/profiles.yml", profiles_of(tmp_path), tmp_path / "extensions")
    check(drive(monkeypatch, tmp_path, "build") == 1, "the build phase stops")
    text = "\n".join(first_lines(capsys)[:4])
    check("task_environment_unrecognised" in text and "AWS_ECS_EXTERNAL" in text, f"the environment value is named: {text}")
    check(
        "dbt_target_downloads_extensions lakehouse_session_bounded" in text and "iceberg" in text, f"the selected target and its extensions are named: {text}"
    )


def test_a_task_target_without_its_extensions_is_named(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    host_environment(monkeypatch)
    install_network(monkeypatch, Network())
    profile_with_extensions(ROOT / "dbt/profiles.yml", profiles_of(tmp_path), tmp_path / "extensions", names=("iceberg", "httpfs"))
    check(drive(monkeypatch, tmp_path, "build") == 1, "a missing extension file stops the build")
    text = "\n".join(first_lines(capsys)[:4])
    check("dbt_extension_missing lakehouse_task" in text and "aws" in text.split("dbt_extension_missing")[1], f"the missing extension is named: {text}")
    (tmp_path / "extensions").rename(tmp_path / "moved")
    check(drive(monkeypatch, tmp_path, "build") == 1, "a missing directory stops the build")
    check("dbt_extension_directory_missing lakehouse_task" in "\n".join(first_lines(capsys)[:4]), "the missing directory is named")


def test_a_host_job_with_everything_in_place_starts(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    host_environment(monkeypatch)
    network = install_network(monkeypatch, Network())
    profile_with_extensions(ROOT / "dbt/profiles.yml", profiles_of(tmp_path), tmp_path / "extensions")
    check(drive(monkeypatch, tmp_path, "pin") == 0, "the pin phase reaches its own code")
    check(drive(monkeypatch, tmp_path, "build") == 0, "the build phase reaches its own code")
    lines = [line for line in first_lines(capsys) if line.startswith("startup_check:")]
    check(any("ok dbt_target lakehouse_task" in line for line in lines), f"the selected target is logged: {lines}")
    check({host for host, _ in network.seen} == {POLARIS_HOST, *SERVICE_HOSTS}, f"only the named endpoints were contacted: {network.seen}")
    check({port for host, port in network.seen if host in SERVICE_HOSTS} == {443}, "S3 and the ECR API are reached on 443")
    check(REGISTRY_HOST not in {host for host, _ in network.seen}, f"the bare registry host has no public DNS record and is never probed: {network.seen}")


@pytest.mark.parametrize("layer", ["gold", "features"])
def test_gold_and_features_check_the_same_things_at_pin(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str], layer: str
) -> None:
    host_environment(monkeypatch)
    install_network(monkeypatch, Network(refuse={POLARIS_HOST: ConnectionRefusedError("refused")}))
    check(drive(monkeypatch, tmp_path, "pin", layer) == 1, f"{layer} stops at pin")
    check(f"polaris_unreachable {POLARIS_HOST}:8181" in "\n".join(first_lines(capsys)[:4]), f"{layer} names the Polaris address")


def test_a_fargate_label_is_refused(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    host_environment(monkeypatch, execution="AWS_ECS_FARGATE")
    install_network(monkeypatch, Network())
    check(drive(monkeypatch, tmp_path, "pin") == 1, "a task that names Fargate stops at pin")
    lines = "\n".join(first_lines(capsys)[:4])
    check("task_environment_unrecognised" in lines and "expected AWS_ECS_EC2" in lines, f"the refusal names the one accepted label: {lines}")


def test_a_run_outside_a_task_checks_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    for name in ("AWS_EXECUTION_ENV", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "ECS_CONTAINER_METADATA_URI_V4", "ECS_CONTAINER_METADATA_URI"):
        monkeypatch.delenv(name, raising=False)
    network = install_network(monkeypatch, Network())
    check(drive(monkeypatch, tmp_path, "pin") == 0, "a local run starts")
    check(network.seen == [] and "startup_check" not in capsys.readouterr().err, "no connect and no log line")


def test_a_static_credential_is_refused_with_its_cause(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    host_environment(monkeypatch)
    install_network(monkeypatch, Network())
    monkeypatch.setenv("AWS_PROFILE", "example_profile")
    check(drive(monkeypatch, tmp_path, "pin") == 1, "a profile inside a task stops the job")
    check("credentials task_credential_source_refused" in "\n".join(first_lines(capsys)[:4]), "the credential cause is named")


def test_a_service_that_cannot_be_reached_on_443_is_named(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    host_environment(monkeypatch)
    install_network(monkeypatch, Network(refuse={SERVICE_HOSTS[0]: OSError("no route")}))
    check(drive(monkeypatch, tmp_path, "pin") == 1, "an unreachable endpoint stops the job")
    check(f"s3_unreachable {SERVICE_HOSTS[0]}:443" in "\n".join(first_lines(capsys)[:4]), "the S3 endpoint is named")


def test_mlflow_is_checked_when_the_job_needs_it(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    host_environment(monkeypatch, mlflow=MLFLOW_URI)
    install_network(monkeypatch, Network(refuse={"192.0.2.10": TimeoutError("timed out")}))
    with pytest.raises(startup_check.StartupCheckError, match=r"mlflow_unreachable 192\.0\.2\.10:5000"):
        startup_check.check_environment("train", polaris=False, mlflow=True)
    check("polaris" not in capsys.readouterr().err, "a job that does not need Polaris does not probe it")


def test_a_real_loopback_socket_is_reached_and_a_closed_port_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    host_environment(monkeypatch, polaris=f"http://127.0.0.1:{port}/api/catalog")
    install_network(monkeypatch, Network())
    startup_check.check_environment("silver", polaris=True)
    listener.close()
    with pytest.raises(startup_check.StartupCheckError, match=rf"polaris_unreachable 127\.0\.0\.1:{port}"):
        startup_check.check_environment("silver", polaris=True)


def test_a_hanging_connect_is_cut_off_within_the_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    host_environment(monkeypatch)
    install_network(monkeypatch, Network(delay=3.0))
    monkeypatch.setattr(startup_check, "CONNECT_SECONDS", 0.3)
    started = time.monotonic()
    with pytest.raises(startup_check.StartupCheckError, match="timed out after 0.3 s"):
        startup_check.check_environment("silver", polaris=True)
    check(time.monotonic() - started < 1.5, "all connects share one bound")


@pytest.mark.parametrize("bytes_value", ["SYNTHETIC_NOT_A_NUMBER", "0", ""])
def test_an_incomplete_spill_setting_is_named_without_its_value(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str], bytes_value: str
) -> None:
    host_environment(monkeypatch)
    install_network(monkeypatch, Network())
    monkeypatch.setenv("HAI_DUCKDB_MAX_TEMP_BYTES", bytes_value)
    check(drive(monkeypatch, tmp_path, "build") == 1, "the build phase stops")
    text = "\n".join(first_lines(capsys)[:4])
    check("dbt_target_refused temporary_storage_settings_incomplete" in text, f"the cause is named in the first lines: {text}")
    check("SYNTHETIC_NOT_A_NUMBER" not in text, f"the value is not repeated: {text}")
