"""Every check of the startup check runs with a planted marker in every input, and no marker reaches stdout, stderr, a log record or a traceback.

The markers are synthetic. A marker stands for a credential, a token, a key, an account number or any other configured value that a diagnostic must not repeat.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import sys
import traceback
from collections.abc import Callable
from pathlib import Path

import pytest

from orchestration.pipeline_protocol import PipelineError
from scripts.acquisition import hcai_util_contract
from scripts.infrastructure import startup_check
from scripts.lakehouse import ecs_engine, pipeline_build
from scripts.process import run_command
from tests.startup_check_faults import MARKER as FAULT_MARKER
from tests.startup_check_faults import LeakyRefusal
from tests.startup_check_support import FORBIDDEN, Network, host_environment, install_network
from tests.support import check
from tests.test_startup_check import drive
from tests.test_startup_check_jobs import run_bronze_phase, run_drift, run_features_phase, run_training

ROOT = Path(__file__).resolve().parents[1]
MARKERS = (
    "SYNTHETIC_PASSWORD",
    "SYNTHETIC_TOKEN",
    "SYNTHETIC_USER",
    "SYNTHETIC_QUERY",
    "SYNTHETIC_FRAGMENT",
    "SYNTHETIC_PROFILE",
    "SYNTHETIC_PATH",
    "SYNTHETIC_HOST",
    "SYNTHETIC_ROOT",
    "SYNTHETIC_KEY",
    "123456789012",
)
USER = "SYNTHETIC_USER:SYNTHETIC_PASSWORD"
TAIL = "SYNTHETIC_PATH/123456789012?token=SYNTHETIC_TOKEN&SYNTHETIC_QUERY=1#SYNTHETIC_FRAGMENT"
# Every malformed or hostile form of a service address that the check must survive without repeating a part of it.
ADDRESSES = [
    f"http://{USER}@192.0.2.10:bad/{TAIL}",
    f"http://{USER}@/{TAIL}",
    f"http://{USER}@192.0.2.10:99999999/{TAIL}",
    f"http://{USER}@192.0.2.10:8181/api/catalog?token=SYNTHETIC_TOKEN#SYNTHETIC_FRAGMENT",
    f"http://{USER}@SYNTHETIC_HOST-123456789012:8181/api/catalog",
    f"http://{USER}@[::1/api/catalog",
    "http://[SYNTHETIC_PASSWORD]/api/catalog",
    "http://[SYNTHETIC_PASSWORD:5000/api/catalog",
    "http://[fe80::1%SYNTHETIC_TOKEN]:8181/api/catalog",
    "http://host-123456789012.SYNTHETIC_HOST.example:8181/api/catalog?token=SYNTHETIC_TOKEN",
    "http://" + "a" * 70 + ".SYNTHETIC_HOST.example:8181/api/catalog",
    f"ftp://{USER}@192.0.2.10/{TAIL}",
    f"{USER}@192.0.2.10/{TAIL}",
    "http://SYNTHETIC_USER@192.0.2.10:8181/api/catalog\nSYNTHETIC_PASSWORD",
    "http://☃SYNTHETIC_HOST:8181/SYNTHETIC_PATH",
    "SYNTHETIC_TOKEN",
    "",
]


@pytest.fixture(autouse=True)
def private_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Some entry points set variables of their own, so each test works on a copy."""
    monkeypatch.setattr(os, "environ", os.environ.copy())


def messages_of(error: BaseException) -> str:
    """The class and message of an error and of every error chained to it, as a traceback prints them, without the source lines of this test."""
    texts = []
    seen: BaseException | None = error
    while seen is not None:
        texts.append("".join(traceback.format_exception_only(seen)))
        seen = seen.__cause__ or (None if seen.__suppress_context__ else seen.__context__)
    return "".join(texts)


def observe(capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture, run: Callable[[], object]) -> str:
    """Everything a run shows: stdout, stderr, log records and the messages of any error that escapes, with its chained errors."""
    caplog.set_level(logging.DEBUG)
    escaped = ""
    try:
        run()
    except Exception as error:
        escaped = messages_of(error)
    captured = capsys.readouterr()
    return captured.out + captured.err + caplog.text + escaped


def clean(text: str, expected: str = "") -> None:
    leaked = [marker for marker in MARKERS if marker in text]
    check(not leaked, f"the output carries {leaked}: {text[:1500]}")
    check(expected in text, f"the check did not run to its named result {expected!r}: {text[:800]}")


@pytest.mark.parametrize("address", ADDRESSES)
def test_a_planted_service_address_never_reaches_the_output(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture, address: str
) -> None:
    host_environment(monkeypatch, polaris=address, mlflow=address)
    monkeypatch.setenv("OM_API", address)
    install_network(monkeypatch, Network())
    text = observe(capsys, caplog, lambda: startup_check.check_environment("train", polaris=True, mlflow=True, openmetadata=True, redis=True))
    clean(text, "startup_check:")


@pytest.mark.parametrize("label", ["AWS_ECS_SYNTHETIC_TOKEN", "SYNTHETIC_TOKEN", "AWS_ECS_EC2\nSYNTHETIC_PASSWORD", "AWS_ECS_" + "SYNTHETIC_KEY" * 3, ""])
def test_a_planted_execution_label_never_reaches_the_output(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture, label: str
) -> None:
    host_environment(monkeypatch, execution=label)
    install_network(monkeypatch, Network())
    clean(observe(capsys, caplog, lambda: startup_check.check_environment("silver", polaris=True)), "task_environment_unrecognised")


@pytest.mark.parametrize("region", ["SYNTHETIC_TOKEN", "us-west-2.SYNTHETIC_HOST.example", "ab-synthetic-key-1", "123456789012", "us-west-2\nSYNTHETIC_KEY"])
@pytest.mark.parametrize("variable", ["AWS_REGION", "AWS_DEFAULT_REGION"])
def test_a_planted_region_never_reaches_the_output(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture, variable: str, region: str
) -> None:
    host_environment(monkeypatch)
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.setenv(variable, region)
    network = install_network(monkeypatch, Network())
    clean(observe(capsys, caplog, lambda: startup_check.check_environment("silver")), "region_invalid")
    check(not network.seen, f"no connect to a host built from the planted region: {network.seen}")


@pytest.mark.parametrize("name", [*FORBIDDEN, "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"])
def test_a_planted_credential_setting_never_reaches_the_output(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture, name: str
) -> None:
    host_environment(monkeypatch)
    monkeypatch.setenv(name, f"{USER}@SYNTHETIC_HOST/{TAIL}")
    install_network(monkeypatch, Network())
    clean(observe(capsys, caplog, lambda: startup_check.check_environment("silver", polaris=True)), "credentials")


def error_of(kind: str) -> Exception:
    text = f"{USER}@SYNTHETIC_HOST/{TAIL}"
    return {
        "oserror": OSError(111, text),
        "timeout": TimeoutError(text),
        "gaierror": socket.gaierror(-2, text),
        "unicode": UnicodeError(text),
        "value": ValueError(text),
        "plain": Exception(text),
    }[kind]


@pytest.mark.parametrize("kind", ["oserror", "timeout", "gaierror", "unicode", "value", "plain"])
def test_a_planted_connect_error_never_reaches_the_output(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture, kind: str
) -> None:
    host_environment(monkeypatch, mlflow="http://192.0.2.10:5000")
    install_network(monkeypatch, Network(refuse={"192.0.2.10": error_of(kind)}))
    clean(observe(capsys, caplog, lambda: startup_check.check_environment("train", polaris=True, mlflow=True)), "unreachable")


def profile(tmp_path: Path, output: object, name: str = "lakehouse_task") -> Path:
    path = tmp_path / "profiles.yml"
    path.write_text(json.dumps({"SYNTHETIC_PROFILE": {"outputs": {name: output}}}))
    return path


def build_observation(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture, profiles: Path) -> str:
    host_environment(monkeypatch)
    install_network(monkeypatch, Network())
    return observe(capsys, caplog, lambda: startup_check.check_build("silver", "real", profiles))


def test_a_planted_profile_value_never_reaches_the_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    missing = tmp_path / "SYNTHETIC_PASSWORD_123456789012"
    empty = tmp_path / "SYNTHETIC_PATH_123456789012"
    empty.mkdir()
    url = f"https://{USER}@SYNTHETIC_HOST/{TAIL}"
    cases = {
        "an extension that is a URL": ({"extensions": [url]}, "dbt_profile_unreadable"),
        "an extension that is not a list": ({"extensions": url}, "dbt_profile_unreadable"),
        "an extension path": ({"extensions": ["../SYNTHETIC_PATH"]}, "dbt_profile_unreadable"),
        "a directory that is not text": ({"extensions": ["iceberg"], "config_options": {"extension_directory": [url]}}, "dbt_profile_unreadable"),
        "options that are not a mapping": ({"extensions": ["iceberg"], "config_options": url}, "dbt_profile_unreadable"),
        "a directory that is missing": (
            {"extensions": ["iceberg"], "config_options": {"extension_directory": str(missing)}},
            "dbt_extension_directory_missing",
        ),
        "a directory without the files": (
            {"extensions": ["iceberg", "httpfs"], "config_options": {"extension_directory": str(empty)}},
            "dbt_extension_missing",
        ),
        "an extension name of the profile": ({"extensions": ["synthetic_key"], "config_options": {"extension_directory": str(empty)}}, "dbt_extension_missing"),
        "no directory": ({"extensions": ["synthetic_password"]}, "dbt_target_downloads_extensions"),
    }
    for label, (output, expected) in cases.items():
        text = build_observation(monkeypatch, capsys, caplog, profile(tmp_path, output))
        leaked = [marker for marker in MARKERS if marker in text]
        check(not leaked and expected in text, f"{label}: {leaked} {expected!r} {text[:600]}")


@pytest.mark.parametrize(
    "text",
    [
        "outputs:\n  password: SYNTHETIC_PASSWORD\n  token: [SYNTHETIC_TOKEN\n  key: { 123456789012\n",
        "[SYNTHETIC_PASSWORD, SYNTHETIC_TOKEN]\n",
        "SYNTHETIC_PASSWORD\n",
        "",
        "SYNTHETIC_PROFILE: SYNTHETIC_PASSWORD\n",
        "SYNTHETIC_PROFILE:\n  outputs: SYNTHETIC_TOKEN\n",
    ],
)
def test_a_planted_profile_text_never_reaches_the_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture, text: str
) -> None:
    profiles = tmp_path / "profiles.yml"
    profiles.write_text(text)
    clean(build_observation(monkeypatch, capsys, caplog, profiles), "dbt_")


def test_a_profile_output_name_and_a_missing_profile_file_never_reach_the_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    clean(build_observation(monkeypatch, capsys, caplog, profile(tmp_path, {}, name="SYNTHETIC_PASSWORD")), "dbt_target_unknown")
    clean(build_observation(monkeypatch, capsys, caplog, tmp_path / "SYNTHETIC_PATH_123456789012.yml"), "dbt_profile_unreadable")


@pytest.mark.parametrize("setting", ["HAI_DUCKDB_MAX_TEMP_BYTES", "HAI_DUCKDB_TEMP_DIRECTORY"])
def test_a_planted_spill_setting_never_reaches_the_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture, setting: str
) -> None:
    host_environment(monkeypatch)
    monkeypatch.setenv(setting, f"{USER}@SYNTHETIC_HOST/{TAIL}")
    install_network(monkeypatch, Network())
    clean(observe(capsys, caplog, lambda: startup_check.check_build("silver", "real", tmp_path / "profiles.yml")), "dbt_target_refused")


def test_a_planted_file_need_never_reaches_the_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    host_environment(monkeypatch)
    present = tmp_path / f"{USER}@SYNTHETIC_HOST"
    present.write_text("{}")
    listing = tmp_path / "plan_inputs.json"
    listing.write_text(json.dumps({f"data/{TAIL}": "SYNTHETIC_KEY"}))
    needs = [
        startup_check.Need(f"{USER}@SYNTHETIC_HOST", "SYNTHETIC_ROOT", f"https://{USER}@SYNTHETIC_HOST/{TAIL}"),
        startup_check.Need("SYNTHETIC_KEY+hcai_util_workbook", "mirror", f"https://{USER}@SYNTHETIC_HOST/{TAIL}"),
        startup_check.Need("hcai_util_workbook", "mirror", present.name),
        startup_check.Need("hcai_util_workbook", "mirror", listing.name, nonempty=True),
        startup_check.Need("SYNTHETIC_PASSWORD", startup_check.UNENUMERATED, ""),
    ]
    roots = {"code": tmp_path, "mirror": tmp_path}
    clean(observe(capsys, caplog, lambda: startup_check.check_files("SYNTHETIC_PASSWORD", needs, roots)), "input_file_missing")


def test_a_planted_plan_and_controls_never_reach_the_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    host_environment(monkeypatch)
    plan = tmp_path / "SYNTHETIC_PATH_123456789012.json"
    plan.write_text(
        json.dumps(
            {
                "registry_sha256": "SYNTHETIC_TOKEN",
                "references": [{"path": f"data/{TAIL}", "sha256": "SYNTHETIC_KEY"}],
                "access_release": {"path": f"config/{USER}.json"},
            }
        )
    )
    monkeypatch.setattr(hcai_util_contract, "PLAN_PATH", plan)
    queue = tmp_path / "queue.json"
    queue.write_text(json.dumps({"queue": [{"disposition": "api", "mode": "hcai_util_workbook", "flags": []}]}))
    controls = tmp_path / "controls.json"
    controls.write_text(json.dumps({"operational_sha256": {f"config/acquisition/{USER}@SYNTHETIC_HOST.json": "SYNTHETIC_KEY"}}))
    roots = {"code": tmp_path / "code", "mirror": tmp_path / "mirror"}

    def run() -> None:
        startup_check.check_queue_files("publisher", queue, controls=controls, keys=True, roots=roots)

    clean(observe(capsys, caplog, run), "input_file_missing")
    plan.write_text(f"{{{USER}@SYNTHETIC_HOST/{TAIL}")
    clean(observe(capsys, caplog, run), "input_file_")
    queue.write_text(f"[{USER}@SYNTHETIC_HOST/{TAIL}")
    clean(observe(capsys, caplog, run), "startup_check_internal_error")


@pytest.mark.parametrize("address", ADDRESSES[:8])
def test_the_real_entry_points_never_show_a_planted_address(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture, address: str
) -> None:
    runners = {
        "silver pin": lambda: drive(monkeypatch, tmp_path, "pin"),
        "silver build": lambda: drive(monkeypatch, tmp_path, "build"),
        "features pin": lambda: run_features_phase("pin")(monkeypatch, tmp_path),
        "bronze pin": lambda: run_bronze_phase("pin")(monkeypatch, tmp_path),
        "train prepare": lambda: run_training("prepare")(monkeypatch, tmp_path),
        "train extract": lambda: run_training("extract")(monkeypatch, tmp_path),
        "drift baseline": lambda: run_drift("baseline")(monkeypatch, tmp_path),
    }
    for name, run in runners.items():
        host_environment(monkeypatch, polaris=address, mlflow=address)
        install_network(monkeypatch, Network())
        text = observe(capsys, caplog, run)
        leaked = [marker for marker in MARKERS if marker in text]
        check(not leaked and "startup_check:" in text, f"{name}: {leaked} {text[:900]}")


SCRIPT = """
import os, sys
from scripts.infrastructure import startup_check as check

def refuse(host, port, timeout):
    raise OSError(111, os.environ["PLANT"])

check.open_connection = refuse
check.ISOLATED = False
getattr(check, sys.argv[1])(*sys.argv[2:])
"""


def run_process(monkeypatch: pytest.MonkeyPatch, entry: str, extra: dict[str, str], *arguments: str) -> str:
    host_environment(monkeypatch)
    environment = {**os.environ, **extra, "PLANT": f"{USER}@SYNTHETIC_HOST/{TAIL}"}
    done = run_command(sys.executable, ["-c", SCRIPT, entry, *arguments], cwd=ROOT, env=environment, timeout=60)
    return done.stdout + done.stderr


@pytest.mark.parametrize(
    "extra",
    [
        {"POLARIS_URI": "http://[SYNTHETIC_PASSWORD]/api/catalog"},
        {"POLARIS_URI": f"http://{USER}@192.0.2.10:bad/{TAIL}"},
        {"AWS_EXECUTION_ENV": "AWS_ECS_SYNTHETIC_TOKEN"},
        {"AWS_REGION": "SYNTHETIC_KEY"},
        {"AWS_SECRET_ACCESS_KEY": "SYNTHETIC_KEY", "AWS_PROFILE": "SYNTHETIC_PROFILE"},
        {"HAI_DUCKDB_MAX_TEMP_BYTES": "SYNTHETIC_TOKEN"},
    ],
)
def test_a_process_that_ends_in_an_uncaught_error_never_prints_a_planted_value(monkeypatch: pytest.MonkeyPatch, extra: dict[str, str]) -> None:
    for entry, arguments in (("check_phase", ("train", "prepare")), ("check_build", ("silver", "real", "SYNTHETIC_PATH_123456789012.yml"))):
        text = run_process(monkeypatch, entry, extra, *arguments)
        leaked = [marker for marker in MARKERS if marker in text]
        check(not leaked and "StartupCheckError" in text, f"{entry}: {leaked} {text[:1200]}")


class LeakyPipelineRefusal(PipelineError):
    """A build refusal whose text conversion writes the marker to every sink it can reach, like the credential refusal of the fault module."""

    def __str__(self) -> str:
        logging.getLogger("dependency").warning("value %s", FAULT_MARKER)
        sys.stderr.write(FAULT_MARKER + "\n")
        return "fixture_role_session_refused"

    __repr__ = __str__


def test_an_exception_whose_text_conversion_writes_is_never_converted_by_the_real_drivers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    """The validator and the build target are shared code, so their refusal is read from its plain code argument, never from its text conversion."""
    runs = {
        "credential refusal at pin": lambda: drive(monkeypatch, tmp_path, "pin"),
        "credential refusal at build": lambda: drive(monkeypatch, tmp_path, "build"),
        "build refusal": lambda: drive(monkeypatch, tmp_path, "build"),
    }
    for name, run in runs.items():
        host_environment(monkeypatch)
        install_network(monkeypatch, Network())
        if name == "build refusal":
            monkeypatch.setattr(pipeline_build, "build_target", refuse_build)
        else:
            monkeypatch.setattr(ecs_engine, "validate_task_credentials", refuse_credentials)
            monkeypatch.setattr(pipeline_build, "validate_task_credentials", refuse_credentials)
        text = observe(capsys, caplog, run)
        check(FAULT_MARKER not in text and "startup_check:" in text, f"{name}: {text[:700]}")
        check("task_credential_source_refused" in text or "fixture_role_session_refused" in text, f"{name}: the fixed code is named: {text[:700]}")


def refuse_credentials(*_arguments: object, **_keywords: object) -> bool:
    raise LeakyRefusal("task_credential_source_refused")


def refuse_build(*_arguments: object, **_keywords: object) -> str:
    raise LeakyPipelineRefusal("fixture_role_session_refused")
