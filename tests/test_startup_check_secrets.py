"""The startup check never writes a secret, a key, a token or an account number, in a log line or in the error it raises. All values here are synthetic."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from scripts.infrastructure import startup_check
from scripts.lakehouse import ecs_engine
from tests.startup_check_support import Network, host_environment, install_network
from tests.support import check

CANARY_A = "SYNTHETIC_PASSWORD"
CANARY_B = "SYNTHETIC_TOKEN"
ACCOUNT = "123456789012"
SECRETS = (CANARY_A, CANARY_B, ACCOUNT, "SYNTHETIC_USER", "SYNTHETIC_PATH")
EMBEDDED = f"SYNTHETIC_USER:{CANARY_A}@192.0.2.10"
TAIL = f"/SYNTHETIC_PATH/{ACCOUNT}?token={CANARY_B}"


def output_of(capsys: pytest.CaptureFixture[str], run: Callable[[], object]) -> str:
    """Everything the check shows: its stderr lines and the message of the error it raises."""
    error = ""
    try:
        run()
    except startup_check.StartupCheckError as caught:
        error = str(caught)
    return capsys.readouterr().err + "\n" + error


def free_of_secrets(text: str) -> None:
    leaked = [secret for secret in SECRETS if secret in text]
    check(not leaked, f"the output carries {leaked}: {text}")


# The variable, its value and the fixed reason that the check names instead of the value.
ADDRESSES = {
    "invalid_port": (f"http://{EMBEDDED}:bad{TAIL}", "has an invalid port"),
    "no_host": (f"http://SYNTHETIC_USER:{CANARY_A}@{TAIL}", "names no host"),
    "no_default_port": (f"ftp://{EMBEDDED}{TAIL}", "names no port"),
    "no_scheme": (f"{EMBEDDED}{TAIL}", "names no host"),
    "unset": ("", "is not set"),
}


@pytest.mark.parametrize("kind", ADDRESSES)
@pytest.mark.parametrize("variable", ["POLARIS_URI", "MLFLOW_TRACKING_URI", "OM_API"])
def test_an_unusable_service_address_is_named_without_its_value(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], variable: str, kind: str
) -> None:
    value, reason = ADDRESSES[kind]
    host_environment(monkeypatch)
    install_network(monkeypatch, Network())
    monkeypatch.setenv(variable, value)
    needs = {"POLARIS_URI": {"polaris": True}, "MLFLOW_TRACKING_URI": {"mlflow": True}, "OM_API": {"openmetadata": True}}[variable]
    text = output_of(capsys, lambda: startup_check.check_environment("train", **needs))
    free_of_secrets(text)
    check(f"{variable} {reason}" in text, f"the variable and the unusable part are named: {text}")


def test_a_redis_address_taken_from_an_unusable_polaris_address_is_named_without_its_value(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    host_environment(monkeypatch, polaris=f"http://{EMBEDDED}:bad{TAIL}")
    install_network(monkeypatch, Network())
    text = output_of(capsys, lambda: startup_check.check_environment("features", polaris=True, redis=True))
    free_of_secrets(text)
    check("redis_address_unusable" in text and "POLARIS_URI has an invalid port" in text, f"the redis cause is named: {text}")


def test_a_usable_address_with_embedded_credentials_logs_only_its_host_and_port(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    host_environment(monkeypatch, mlflow=f"http://{EMBEDDED}:5000{TAIL}")
    install_network(monkeypatch, Network(refuse={"192.0.2.10": TimeoutError("timed out")}))
    text = output_of(capsys, lambda: startup_check.check_environment("train", mlflow=True))
    free_of_secrets(text)
    check("mlflow_unreachable 192.0.2.10:5000" in text, f"the configured host and port are named: {text}")


def test_an_unrecognised_execution_value_is_not_echoed(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    host_environment(monkeypatch, execution=f"{CANARY_B}_{CANARY_A}")
    install_network(monkeypatch, Network())
    text = output_of(capsys, lambda: startup_check.check_environment("silver"))
    free_of_secrets(text)
    check("task_environment_unrecognised" in text and "AWS_EXECUTION_ENV" in text, f"the cause is named: {text}")


def test_a_region_that_is_not_a_region_name_is_not_echoed(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    host_environment(monkeypatch)
    monkeypatch.setenv("AWS_REGION", f"{CANARY_B}.{CANARY_A}")
    network = install_network(monkeypatch, Network())
    text = output_of(capsys, lambda: startup_check.check_environment("silver"))
    free_of_secrets(text)
    check("region_invalid" in text and network.seen == [], f"the region is refused before any connect: {text} {network.seen}")


def test_an_unreadable_dbt_profile_does_not_echo_its_content(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    host_environment(monkeypatch)
    install_network(monkeypatch, Network())
    profiles = tmp_path / "profiles.yml"
    profiles.write_text(f"outputs:\n  password: {CANARY_A}\n  token: [{CANARY_B}\n  key: {{ {ACCOUNT}\n")
    text = output_of(capsys, lambda: startup_check.check_build("silver", "real", profiles))
    free_of_secrets(text)
    check("dbt_profile_unreadable" in text, f"the cause is named: {text}")


@pytest.mark.parametrize(
    "address", ["http://[SYNTHETIC_PASSWORD]/api/catalog", "http://[SYNTHETIC_PASSWORD:5000/api/catalog", f"http://{EMBEDDED}@[::1/api/catalog"]
)
def test_a_malformed_catalog_address_is_refused_by_the_shared_validator_with_its_fixed_reason(monkeypatch: pytest.MonkeyPatch, address: str) -> None:
    """The validator is shared with the build target selection, so its refusal must be the fixed reason and carry no parser message or chained error."""
    host_environment(monkeypatch, polaris=address)
    with pytest.raises(RuntimeError) as caught:
        ecs_engine.validate_task_credentials(require_catalog=True)
    check(str(caught.value) == "task_catalog_endpoint_refused", f"the refusal is the fixed reason: {caught.value!s}")
    check(caught.value.__cause__ is None and caught.value.__suppress_context__, "no parser error is chained to it")


@pytest.mark.parametrize("address", ["http://[SYNTHETIC_PASSWORD]/api/catalog", "http://[SYNTHETIC_PASSWORD:5000/api/catalog"])
def test_a_malformed_catalog_address_is_named_as_a_credentials_finding(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str], address: str
) -> None:
    host_environment(monkeypatch, polaris=address)
    install_network(monkeypatch, Network())
    text = output_of(capsys, lambda: startup_check.check_environment("silver", polaris=True))
    free_of_secrets(text)
    check("credentials task_catalog_endpoint_refused" in text and "internal_error" not in text, f"the named finding is shown: {text}")
    text = output_of(capsys, lambda: startup_check.check_build("silver", "real", tmp_path / "profiles.yml"))
    free_of_secrets(text)
    check("dbt_target_refused task_catalog_endpoint_refused" in text, f"the build check names the refusal: {text}")
