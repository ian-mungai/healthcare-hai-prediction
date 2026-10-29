"""Check task and local engine setup with synthetic inputs, stand-in engines and real scratch files."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import threading
import time
import types
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, datetime
from functools import partial
from importlib.metadata import version
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.lakehouse import pipeline_build, session
from scripts.orchestration.ecs_stage_runtime_cases import changed_function, lint_mutant, require
from scripts.process import run_command

# The secret variable the planted defect reads; named here so the planted text is not itself an environment read.
PLANTED_DBT_VARIABLE = "DBT_ENV_SECRET_POLARIS_CLIENT_SECRET"

ROOT = Path(__file__).resolve().parents[2]
CATALOG = "spark.sql.catalog.hai_lakehouse"
PROVIDER = "software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider"
SOURCES = (
    "scripts/lakehouse/ecs_engine.py",
    "scripts/lakehouse/session.py",
    "scripts/lakehouse/pipeline_build.py",
    "scripts/lakehouse/silver_quality.py",
    "dbt/profiles.yml",
    "scripts/orchestration/ecs_engine_cases.py",
    "scripts/orchestration/ecs_stage_runtime_cases.py",
    "config/quality/mypy_runtimes.json",
)


def environment(folder: Path, *, task: bool = True, bounded: bool = True) -> dict[str, str]:
    values = {
        "AWS_REGION": "us-west-2",
        "POLARIS_URI": "http://verified-catalog.example.invalid:8181/api/catalog",
        "POLARIS_CLIENT_ID": "synthetic_reader",
        "POLARIS_CLIENT_SECRET": "synthetic_spark_value",
        "DBT_ENV_SECRET_POLARIS_CLIENT_ID": "synthetic_reader",
        "DBT_ENV_SECRET_POLARIS_CLIENT_SECRET": "synthetic_dbt_value",
        "JOB_THREADS": "1",
        "DUCKDB_MEMORY_LIMIT": "1GB",
        "SPARK_JOB_MEMORY": "1g",
        "HAI_DEADLINE_UTC": str(time.time() + 30),
    }
    if task:
        values.update(AWS_EXECUTION_ENV="AWS_ECS_FARGATE", AWS_CONTAINER_CREDENTIALS_RELATIVE_URI="/v2/credentials/synthetic_task")
        values["HAI_SPARK_LOCAL_DIRECTORY"] = str(folder / "spark_tmp")
    if bounded:
        values.update(HAI_DUCKDB_TEMP_DIRECTORY=str(folder / "spill"), HAI_DUCKDB_MAX_TEMP_BYTES="1048576")
    return values


def rendered_profile(path: Path, target: str) -> dict[str, Any]:
    yaml = importlib.import_module("yaml")
    jinja = importlib.import_module("jinja2")
    missing = object()

    def env_var(name: str, default: Any = missing) -> str:
        if name in os.environ:
            return os.environ[name]
        if default is not missing:
            return str(default)
        raise RuntimeError("missing_profile_environment:" + name)

    renderer = jinja.Environment(undefined=jinja.StrictUndefined)
    renderer.globals["env_var"] = env_var

    def render(value: Any) -> Any:
        if isinstance(value, str):
            return renderer.from_string(value).render()
        if isinstance(value, list):
            return [render(item) for item in value]
        if isinstance(value, dict):
            return {key: render(item) for key, item in value.items()}
        return value

    return dict(render(yaml.safe_load(path.read_text())["hai"]["outputs"][target]))


def spark(values: dict[str, str], warehouse: Path | None = None) -> dict[str, Any]:
    captured: dict[str, Any] = {}
    startup: list[tuple[str, Any]] = []
    runtime: list[str] = []

    def configure_runtime(name: str, value: Any) -> None:
        runtime.append(name)
        captured[name] = value

    class Builder:
        def appName(self, value: str) -> Builder:
            captured["app_name"] = value
            startup.append(("spark.app.name", value))
            return self

        def master(self, value: str) -> Builder:
            captured["master"] = value
            startup.append(("spark.master", value))
            return self

        def config(self, name: str, value: Any) -> Builder:
            if values.get("AWS_EXECUTION_ENV") == "AWS_ECS_FARGATE":
                require(values["POLARIS_CLIENT_SECRET"] not in str(value), "secret_in_spark_arguments")
            startup.append((name, value))
            captured[name] = value
            return self

        def getOrCreate(self) -> Any:
            captured["startup_configuration"] = tuple(startup)
            captured["runtime_keys"] = runtime
            return types.SimpleNamespace(conf=types.SimpleNamespace(set=configure_runtime))

    module: Any = types.ModuleType("pyspark.sql")
    module.SparkSession = types.SimpleNamespace(builder=Builder())
    with patch.dict(os.environ, values, clear=True), patch.dict(sys.modules, {"pyspark.sql": module}):
        configured: Any = session.spark_session("synthetic_engine_setup", warehouse=warehouse)
    require(configured is not None, "spark_session_missing")
    return captured


def build(
    folder: Path, values: dict[str, str], *, mode: str = "real", layer: str = "silver", transient: bool = False, during_build: Callable[[], None] | None = None
) -> dict[str, Any]:
    output = folder / "out"
    output.mkdir(parents=True)
    project = folder / "project"
    project.mkdir()
    (project / "profiles.yml").write_bytes((ROOT / "dbt/profiles.yml").read_bytes())
    captured: dict[str, Any] = {"invocations": 0, "connections": []}
    seen = threading.Event()
    observer = pipeline_build.disk_observation

    def observe(path: Path, settings: dict[str, str]) -> dict[str, int]:
        result = observer(path, settings)
        if result["spill_logical_bytes"] >= 8192:
            seen.set()
        return result

    class Runner:
        def invoke(self, arguments: list[str]) -> Any:
            captured["invocations"] += 1
            forbidden_values = [value for name, value in values.items() if "SECRET" in name or "TOKEN" in name or name == "AWS_ACCESS_KEY_ID"]
            require(all(value not in argument for value in forbidden_values for argument in arguments), "secret_in_dbt_arguments")
            captured["arguments"] = list(arguments)
            target = arguments[arguments.index("--target") + 1]
            captured["target"] = target
            captured["profile"] = rendered_profile(project / "profiles.yml", target)
            if transient:
                spill = Path(values["HAI_DUCKDB_TEMP_DIRECTORY"])
                spill.mkdir()
                transient_path = spill / "duckdb_temp_storage-0.tmp"
                transient_path.write_bytes(b"x" * 8192)
                require(seen.wait(2), "during_build_spill_not_observed")
                transient_path.unlink()
            if during_build is not None:
                during_build()
            (output / "target").mkdir(exist_ok=True)
            (output / "target/run_results.json").write_text(json.dumps({"results": [{"status": "pass"}], "metadata": {"invocation_id": "synthetic"}}))
            return types.SimpleNamespace(success=True)

    module: Any = types.ModuleType("dbt.cli.main")
    module.dbtRunner = Runner

    class Connection:
        def execute(self, statement: str, parameters: Any = None) -> Connection:
            return self

        def close(self) -> None:
            return None

    def connect(path: str, config: dict[str, str]) -> Connection:
        captured["connections"].append(dict(config))
        Path(path).write_bytes(b"synthetic database stand-in")
        return Connection()

    duckdb: Any = types.ModuleType("duckdb")
    duckdb.connect = connect
    arguments = [
        "pipeline_build",
        "--layer",
        layer,
        "--mode",
        mode,
        "--run",
        "20261025T000000Z-a1b2c3d4",
        "--folder",
        str(output),
        "--project",
        str(project),
        "--source-sha256",
        "1" * 64,
        "--input-sha256",
        "2" * 64,
    ]
    if layer == "gold":
        extract = output / "silver_input"
        (extract / "dim_one").mkdir(parents=True)
        payload = b"synthetic parquet stand-in"
        (extract / "dim_one/part-00000.parquet").write_bytes(payload)
        manifest = {
            "run_id": "20261025T000000Z-a1b2c3d4",
            "pins_sha256": "2" * 64,
            "snapshot_set": {"dim_one": 1},
            "files": {"dim_one/part-00000.parquet": hashlib.sha256(payload).hexdigest()},
        }
        raw = json.dumps(manifest).encode()
        (extract / "manifest.json").write_bytes(raw)
        arguments += ["--extract-sha256", hashlib.sha256(raw).hexdigest()]
    stdout, stderr = io.StringIO(), io.StringIO()
    with (
        patch.dict(os.environ, values, clear=True),
        patch.dict(sys.modules, {"dbt.cli.main": module, "duckdb": duckdb}),
        patch.object(sys, "argv", arguments),
        patch.object(pipeline_build, "disk_observation", observe),
        redirect_stdout(stdout),
        redirect_stderr(stderr),
    ):
        captured["exit_code"] = pipeline_build.main()
    captured["stderr"] = stderr.getvalue()
    receipt = output / "build_receipt.json"
    if receipt.exists():
        captured["receipt"] = json.loads(receipt.read_bytes())
    require(not any(path.suffix == ".env" for path in folder.rglob("*")), "saved_environment_file")
    return captured


def expect_refusal(cause: str, action: Callable[[], Any]) -> None:
    try:
        action()
    except RuntimeError as error:
        require(str(error) == cause, "wrong_refusal:" + cause)
    else:
        raise RuntimeError("missing_refusal:" + cause)


def task_spark(folder: Path) -> None:
    values = environment(folder)
    configured = spark(values)
    require(configured["runtime_keys"] == [CATALOG + ".credential"], "task_spark_runtime_keys")
    require(configured.get(CATALOG + ".credential") == "synthetic_reader:synthetic_spark_value", "task_spark_runtime_credential")
    require(configured[CATALOG + ".client.credentials-provider"] == PROVIDER, "task_spark_provider")
    require(configured[CATALOG + ".uri"] == values["POLARIS_URI"], "task_spark_catalog")
    require(configured.get(CATALOG + ".oauth2-server-uri") == values["POLARIS_URI"] + "/v1/oauth/tokens", "task_spark_oauth_endpoint")
    require(configured["spark.local.dir"] == values["HAI_SPARK_LOCAL_DIRECTORY"], "task_spark_scratch")
    require("credential" in configured.get("spark.redaction.regex", ""), "task_spark_credential_redaction")


def task_dbt(folder: Path) -> None:
    values = environment(folder)
    captured = build(folder, values)
    require(captured["exit_code"] == 0 and captured["target"] == "lakehouse_task", "task_dbt_target")
    profile = captured["profile"]
    s3, catalog = profile["secrets"]
    require(s3["provider"] == "credential_chain" and "profile" not in s3 and "chain" not in s3, "task_dbt_native_provider")
    require(s3.get("refresh") == "auto", "task_dbt_refresh")
    require(catalog["oauth2_server_uri"] == values["POLARIS_URI"] + "/v1/oauth/tokens", "task_dbt_oauth_endpoint")
    require(profile["attach"][0]["options"]["endpoint"] == values["POLARIS_URI"], "task_dbt_catalog_endpoint")
    require(profile["settings"]["max_temp_directory_size"] == "1048576B", "task_dbt_cap")
    require(profile["settings"]["temp_directory"] == values["HAI_DUCKDB_TEMP_DIRECTORY"], "task_dbt_scratch")


def task_profile_endpoint_required(folder: Path) -> None:
    values = environment(folder)
    del values["POLARIS_URI"]
    with patch.dict(os.environ, values, clear=True):
        expect_refusal("missing_profile_environment:POLARIS_URI", lambda: rendered_profile(ROOT / "dbt/profiles.yml", "lakehouse_task"))


def local_paths(folder: Path) -> None:
    values = environment(folder, task=False, bounded=False)
    values.update({"AWS_ACCESS_KEY_ID": "synthetic_access", "AWS_SECRET_ACCESS_KEY": "synthetic_secret", "AWS_SESSION_TOKEN": "synthetic_session"})
    configured = spark(values)
    expected_startup = (
        ("spark.app.name", "synthetic_engine_setup"),
        ("spark.master", "local[1]"),
        ("spark.driver.memory", "1g"),
        ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
        (CATALOG, "org.apache.iceberg.spark.SparkCatalog"),
        ("spark.sql.defaultCatalog", "hai_lakehouse"),
        ("spark.sql.catalogImplementation", "in-memory"),
        ("spark.sql.session.timeZone", "UTC"),
        ("spark.ui.enabled", "false"),
        (CATALOG + ".type", "rest"),
        (CATALOG + ".uri", "http://verified-catalog.example.invalid:8181/api/catalog"),
        (CATALOG + ".warehouse", "hai_lakehouse"),
        (CATALOG + ".credential", "synthetic_reader:synthetic_spark_value"),
        (CATALOG + ".scope", "PRINCIPAL_ROLE:ALL"),
        (CATALOG + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
        (CATALOG + ".client.region", "us-west-2"),
    )
    require(configured["startup_configuration"] == expected_startup, "local_spark_builder_configuration_changed")
    require(configured["runtime_keys"] == [], "local_spark_runtime_configuration_changed")
    require(CATALOG + ".client.credentials-provider" not in configured and "spark.local.dir" not in configured, "local_spark_settings_changed")
    require(configured[CATALOG + ".uri"] == values["POLARIS_URI"], "local_spark_catalog")
    require(configured[CATALOG + ".credential"] == "synthetic_reader:synthetic_spark_value", "local_spark_credentials")
    captured = build(folder, values)
    require(captured["exit_code"] == 0 and captured["target"] == "lakehouse_session", "local_dbt_target")
    require(captured["profile"]["secrets"][0]["chain"] == "env", "local_role_session_chain")
    require("runtime_measurements" not in captured["receipt"], "local_measurements_changed")
    require("temp_directory" not in captured["profile"]["settings"], "local_temp_settings_changed")
    del values["POLARIS_URI"]
    with patch.dict(os.environ, values, clear=True):
        profile = rendered_profile(ROOT / "dbt/profiles.yml", "lakehouse_session")
    require(profile["attach"][0]["options"]["endpoint"] == "http://polaris:8181/api/catalog", "local_catalog_default_changed")
    for layer, expected in (("silver", "fixture"), ("gold", "pinned_silver")):
        case = folder / layer
        case.mkdir()
        clean = environment(case, task=False, bounded=False)
        clean["STAGING_E2E_CASE"] = str(case)
        fixture = build(case, clean, mode="publish_fixture", layer=layer)
        require(fixture["exit_code"] == 0 and fixture["target"] == expected, "local_fixture_target_changed:" + layer)


def credential_refusals(folder: Path) -> None:
    forbidden = (
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_SHARED_CREDENTIALS_FILE",
        "AWS_CONFIG_FILE",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_ROLE_ARN",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
    )
    for name in forbidden:
        child = folder / name
        child.mkdir()
        values = environment(child)
        values[name] = "synthetic_forbidden_value"
        expect_refusal("task_credential_source_refused", partial(spark, values))
        for layer in ("silver", "gold"):
            build_folder = child / layer
            build_folder.mkdir()
            captured = build(build_folder, values, layer=layer)
            cause = layer + ":" + name
            require(captured["exit_code"] == 1 and "task_credential_source_refused" in captured["stderr"], "task_build_credential_refusal:" + cause)
            require(captured["invocations"] == 0, "task_engine_started_after_refusal:" + cause)


def fixture_refusals(folder: Path) -> None:
    for task in (False, True):
        for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
            child = folder / (str(task) + name)
            child.mkdir()
            values = environment(child, task=task, bounded=task)
            values[name] = "synthetic_fixture_session"
            values["STAGING_E2E_CASE"] = str(child)
            expect_refusal("fixture_role_session_refused", partial(spark, values, child / "warehouse"))
            captured = build(child, values, mode="publish_fixture")
            if task:
                require(captured["exit_code"] == 1 and "fixture_role_session_refused" in captured["stderr"], "fixture_build_session_accepted")
                require(captured["invocations"] == 0, "fixture_engine_started_after_refusal")
            else:
                require(captured["exit_code"] == 0 and captured["target"] == "fixture", "local_dbt_fixture_behavior_changed")


def endpoint_refusals(folder: Path) -> None:
    for index, endpoint in enumerate(
        ("", "http://user:password@example.invalid/api/catalog", "http://example.invalid/wrong", "http://example.invalid/api/catalog?x=1")
    ):
        child = folder / str(index)
        child.mkdir()
        values = environment(child)
        values["POLARIS_URI"] = endpoint
        expect_refusal("task_catalog_endpoint_refused", partial(spark, values))
        captured = build(child, values)
        require(captured["exit_code"] == 1 and "task_catalog_endpoint_refused" in captured["stderr"], "task_build_endpoint_refusal")
    for index, provider in enumerate(("", "http://example.invalid/credentials", "/v2/credentials/../other")):
        child = folder / ("provider" + str(index))
        child.mkdir()
        values = environment(child)
        values["AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"] = provider
        expect_refusal("task_credential_provider_missing", partial(spark, values))
        captured = build(child, values)
        require(captured["exit_code"] == 1 and "task_credential_provider_missing" in captured["stderr"], "task_build_provider_refusal")


def storage_refusals(folder: Path) -> None:
    for index, change in enumerate(
        (
            {"HAI_DUCKDB_MAX_TEMP_BYTES": ""},
            {"HAI_DUCKDB_TEMP_DIRECTORY": ""},
            {"HAI_DUCKDB_MAX_TEMP_BYTES": "0"},
            {"HAI_DUCKDB_MAX_TEMP_BYTES": "-1"},
            {"HAI_DUCKDB_MAX_TEMP_BYTES": "1GiB"},
            {"HAI_DUCKDB_TEMP_DIRECTORY": "relative"},
        )
    ):
        child = folder / str(index)
        child.mkdir()
        values = {**environment(child), **change}
        captured = build(child, values)
        require(captured["exit_code"] == 1 and "temporary_storage_settings_incomplete" in captured["stderr"], "invalid_storage_accepted")
        require(captured["invocations"] == 0, "storage_refusal_after_engine_start")


def gold_import(folder: Path) -> None:
    values = environment(folder)
    captured = build(folder, values, layer="gold")
    require(captured["exit_code"] == 0 and captured["target"] == "pinned_silver_bounded", "gold_bounded_target")
    require(captured["connections"] == [{"temp_directory": values["HAI_DUCKDB_TEMP_DIRECTORY"], "max_temp_directory_size": "1048576B"}], "gold_import_cap")
    imported = captured["receipt"]["runtime_measurements"]["import"]
    require(imported["verified_input_bytes"] == len(b"synthetic parquet stand-in"), "gold_import_bytes")
    require(imported.get("hash_seconds", -1) >= 0, "gold_import_hash_measurement")


def during_build_spill(folder: Path) -> None:
    captured = build(folder, environment(folder), transient=True)
    require(captured["exit_code"] == 0, "sampled_build_failed")
    measured = captured["receipt"]["runtime_measurements"]
    require(measured["observed_spill_logical_bytes"] >= 8192, "transient_spill_missing")
    require(measured["minimum_observed_free_disk_bytes"] > 0 and measured.get("samples", 0) >= 3, "disk_samples_missing")
    require(measured["complete_peak"] is False and measured["sampling"] == "periodic_and_boundaries", "sampled_peak_overclaimed")


def sampler_failure(folder: Path) -> None:
    calls = 0
    failed = threading.Event()
    original = pipeline_build.disk_observation

    def failing(path: Path, settings: dict[str, str]) -> dict[str, int]:
        nonlocal calls
        calls += 1
        if calls == 2:
            require(threading.current_thread().name == "hai_storage_sampler", "sampler_failure_not_in_worker")
            failed.set()
            raise OSError("synthetic_storage_sample_failed")
        return original(path, settings)

    with patch.object(pipeline_build, "disk_observation", failing):
        captured = build(folder, environment(folder), during_build=lambda: require(failed.wait(2), "sampler_worker_failure_not_observed"))
    require(captured["exit_code"] == 1 and "temporary_storage_sampling_failed" in captured["stderr"], "sampler_failure_ignored")
    require(calls == 2, "sampler_failure_not_one_shot")
    require("receipt" not in captured, "failed_sampler_receipt_written")
    require(not any(thread.name == "hai_storage_sampler" for thread in threading.enumerate()), "sampler_survived_failure")


def sampler_deadline(folder: Path) -> None:
    values = environment(folder)
    values["HAI_DEADLINE_UTC"] = str(time.time() - 1)
    captured = build(folder, values)
    require(captured["exit_code"] == 1 and "temporary_storage_sampling_deadline" in captured["stderr"], "sampler_deadline_ignored")
    require(captured["invocations"] == 0 and "receipt" not in captured, "expired_sampler_started_build")
    require(not any(thread.name == "hai_storage_sampler" for thread in threading.enumerate()), "sampler_survived_deadline")


def quality_connections(folder: Path) -> None:
    captures: list[dict[str, Any]] = []

    class Connection:
        def __init__(self) -> None:
            self.statement = ""

        def __enter__(self) -> Connection:
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def execute(self, statement: str) -> Connection:
            self.statement = statement
            return self

        def fetchall(self) -> list[tuple[str, ...]]:
            if "information_schema" in self.statement:
                return [("int_silver_build_marker",), ("dim_one",)]
            return [("synthetic", "synthetic", "vsynthetic")]

        def fetchone(self) -> tuple[Any, ...]:
            return ("vsynthetic",) if "version()" in self.statement else (1, "synthetic_fingerprint")

        def table(self, name: str) -> Connection:
            self.statement = ""
            return self

        def project(self, columns: str) -> Connection:
            return self

        def set_alias(self, alias: str) -> Connection:
            return self

        def aggregate(self, expression: str) -> Connection:
            return self

    def connect(path: str, *, read_only: bool, config: dict[str, str]) -> Connection:
        require(read_only, "quality_writable_connection")
        captures.append(dict(config))
        return Connection()

    modules: dict[str, Any] = {
        name: types.ModuleType(name)
        for name in (
            "duckdb",
            "great_expectations",
            "great_expectations.expectations",
            "great_expectations.execution_engine",
            "great_expectations.execution_engine.sqlalchemy_execution_engine",
        )
    }
    modules["duckdb"].connect = connect
    modules["duckdb"].__version__ = "synthetic"
    modules["great_expectations"].__version__ = "1.23.2"
    modules["great_expectations.execution_engine.sqlalchemy_execution_engine"]._PERSISTED_CONNECTION_DIALECTS = ("duckdb",)
    path = ROOT / "scripts/lakehouse/silver_quality.py"
    spec = importlib.util.spec_from_file_location("hai_quality_case", path)
    require(spec is not None and spec.loader is not None, "quality_module_missing")
    if spec is None or spec.loader is None:
        raise RuntimeError("quality_module_missing")
    quality = importlib.util.module_from_spec(spec)
    values = environment(folder)
    with patch.dict(sys.modules, modules), patch.dict(os.environ, values, clear=True):
        spec.loader.exec_module(quality)
        database = folder / "build.duckdb"
        database.write_bytes(b"synthetic database")
        quality.read_marker(database)
        quality.existing_tables(database)
        quality.fingerprints(database, ["dim_one"])
        with quality.private_copy(database) as (copied, digest):
            require(copied.is_relative_to(Path(values["HAI_DUCKDB_TEMP_DIRECTORY"]).parent), "quality_copy_outside_task_scratch")
            require(digest == hashlib.sha256(b"synthetic database").hexdigest(), "quality_copy_identity_changed")
        expected = {"memory_limit": "1GB", "threads": "1", "temp_directory": values["HAI_DUCKDB_TEMP_DIRECTORY"], "max_temp_directory_size": "1048576B"}
        require(len(captures) == 3 and all(value == expected for value in captures), "quality_connection_temp_cap")
        values["HAI_DUCKDB_MAX_TEMP_BYTES"] = ""
        with patch.dict(os.environ, values, clear=True):
            expect_refusal("temporary_storage_settings_incomplete", quality.engine_config)
            arguments = [
                "silver_quality",
                "--database",
                str(database),
                "--suites",
                str(folder),
                "--baselines",
                str(folder / "baselines.json"),
                "--output",
                str(folder / "quality.json"),
            ]
            with patch.object(sys, "argv", arguments), redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                require(quality.main() == 2, "quality_storage_error_exit")
            require(json.loads((folder / "quality.json").read_text())["error"] == "temporary_storage_settings_incomplete", "quality_storage_error_record")
        with patch.dict(os.environ, {"DUCKDB_MEMORY_LIMIT": "1GB", "JOB_THREADS": "1"}, clear=True):
            require(quality.engine_config() == {"memory_limit": "1GB", "threads": "1"}, "local_quality_settings_changed")


def observe(case: Callable[[Path], None], folder: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="engine_case_", dir=folder) as temporary:
        try:
            case(Path(temporary))
        except (RuntimeError, OSError, ValueError, KeyError) as error:
            return {"passed": False, "cause": str(error)}
    return {"passed": True}


def mutations(folder: Path) -> dict[str, Any]:
    result: dict[str, Any] = {}
    edits: tuple[tuple[types.ModuleType, str, tuple[tuple[str, str], ...], Callable[[Path], None], str, str], ...] = (
        (
            session,
            "task_settings",
            (("warehouse is not None and any", "warehouse is None and any"),),
            fixture_refusals,
            "missing_refusal:fixture_role_session_refused",
            "fixture_accepts_role_session",
        ),
        (
            pipeline_build,
            "main",
            (
                (
                    "result = dbtRunner().invoke(arguments)",
                    f"arguments.append(os.environ[{PLANTED_DBT_VARIABLE!r}])\n            result = dbtRunner().invoke(arguments)",
                ),
            ),
            task_dbt,
            "secret_in_dbt_arguments",
            "secret_in_arguments",
        ),
    )
    edits += (
        (
            session,
            "spark_session",
            (
                (
                    "configured = builder.getOrCreate()",
                    (
                        "if warehouse is None:\n"
                        '        builder = builder.config(f"{prefix}.credential", os.environ["POLARIS_CLIENT_SECRET"])\n'
                        "    configured = builder.getOrCreate()"
                    ),
                ),
            ),
            task_spark,
            "secret_in_spark_arguments",
            "secret_in_spark_startup_arguments",
        ),
        (
            pipeline_build,
            "build_target",
            (('and mode == "publish_fixture"', 'and mode != "publish_fixture"'),),
            fixture_refusals,
            "fixture_build_session_accepted",
            "dbt_fixture_accepts_role_session",
        ),
        (
            pipeline_build,
            "storage_measurements",
            (
                (
                    "            try:\n                sample()\n            except (OSError, PipelineError):",
                    "            try:\n                stopped.is_set()\n            except (OSError, PipelineError):",
                ),
            ),
            during_build_spill,
            "during_build_spill_not_observed",
            "omit_during_build_sampling",
        ),
    )
    edits += (
        (
            pipeline_build,
            "build_target",
            (
                (
                    'task = validate_task_credentials(require_catalog=layer == "silver" and mode == "real")',
                    'task = validate_task_credentials(require_catalog=layer == "silver" and mode == "real") if layer != "gold" else False',
                ),
            ),
            credential_refusals,
            "task_build_credential_refusal:gold:AWS_PROFILE",
            "skip_offline_gold_credential_validation",
        ),
        (
            pipeline_build,
            "storage_measurements",
            (("        if errors:\n            raise PipelineError(errors[0])\n", ""),),
            sampler_failure,
            "sampler_failure_ignored",
            "ignore_sampler_worker_error",
        ),
    )
    runtime_credential = "configured.conf.set(f\"{prefix}.credential\", f\"{os.environ['POLARIS_CLIENT_ID']}:{os.environ['POLARIS_CLIENT_SECRET']}\")"
    edits += (
        (
            session,
            "spark_session",
            (("    if warehouse is None and task_catalog:\n        " + runtime_credential + "\n", ""),),
            task_spark,
            "task_spark_runtime_keys",
            "omit_task_runtime_credential",
        ),
        (
            session,
            "spark_session",
            ((runtime_credential, 'configured.conf.set(f"{prefix}.credential", os.environ["POLARIS_CLIENT_ID"])'),),
            task_spark,
            "task_spark_runtime_credential",
            "task_runtime_client_id_only",
        ),
        (
            session,
            "spark_session",
            ((runtime_credential, runtime_credential.replace("{prefix}.credential", "{prefix}.token")),),
            task_spark,
            "task_spark_runtime_keys",
            "task_runtime_wrong_credential_key",
        ),
    )
    for owner, name, replacements, case, cause, label in edits:
        replacement = changed_function(owner, name, replacements, folder)
        with patch.object(owner, name, replacement):
            observed = observe(case, folder)
        result[label] = {"caught": observed.get("cause") == cause, "expected_cause": cause, "observed": observed}
    yaml = importlib.import_module("yaml")
    profiles = yaml.safe_load((ROOT / "dbt/profiles.yml").read_text())
    profiles["hai"]["outputs"]["lakehouse_task"]["secrets"][0]["profile"] = "synthetic_mac_profile"
    with tempfile.TemporaryDirectory(prefix="profile_mutation_", dir=folder) as temporary:
        mutant_root = Path(temporary)
        (mutant_root / "dbt").mkdir()
        (mutant_root / "dbt/profiles.yml").write_text(yaml.safe_dump(profiles))
        with patch(__name__ + ".ROOT", mutant_root):
            observed = observe(task_dbt, folder)
    result["mac_profile_in_task"] = {
        "caught": observed.get("cause") == "task_dbt_native_provider",
        "expected_cause": "task_dbt_native_provider",
        "observed": observed,
    }
    with tempfile.TemporaryDirectory(prefix="quality_mutation_", dir=folder) as temporary:
        mutant_root = Path(temporary)
        path = mutant_root / "scripts/lakehouse/silver_quality.py"
        path.parent.mkdir(parents=True)
        original = (ROOT / "scripts/lakehouse/silver_quality.py").read_text()
        require(original.count("**duckdb_temp_settings()") == 1, "quality_cap_mutation_anchor")
        path.write_text(original.replace("**duckdb_temp_settings()", "**{}"))
        lint_mutant((path,), folder, "omit_quality_cap")
        with patch(__name__ + ".ROOT", mutant_root):
            observed = observe(quality_connections, folder)
    result["omit_quality_cap"] = {
        "caught": observed.get("cause") == "quality_connection_temp_cap",
        "expected_cause": "quality_connection_temp_cap",
        "observed": observed,
    }
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--mutations", action="store_true")
    args = parser.parse_args()
    destination = args.report.resolve()
    if not destination.is_relative_to(ROOT / "data/e2e") or destination.exists():
        parser.error("use a new report below data/e2e")
    destination.parent.mkdir(parents=True, exist_ok=True)
    sources = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES}
    cases = {
        "task_spark": task_spark,
        "task_dbt": task_dbt,
        "task_profile_endpoint_required": task_profile_endpoint_required,
        "local_paths": local_paths,
        "credential_refusals": credential_refusals,
        "fixture_refusals": fixture_refusals,
        "endpoint_refusals": endpoint_refusals,
        "storage_refusals": storage_refusals,
        "gold_import": gold_import,
        "during_build_spill": during_build_spill,
        "quality_connections": quality_connections,
        "sampler_failure": sampler_failure,
        "sampler_deadline": sampler_deadline,
    }
    observed = {name: observe(case, destination.parent) for name, case in cases.items()}
    planted = mutations(destination.parent) if args.mutations else {}
    restored = {name: observe(case, destination.parent) for name, case in cases.items()} if args.mutations else {}
    require(not any(thread.name == "hai_storage_sampler" for thread in threading.enumerate()), "sampler_survived_run")
    unchanged = sources == {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES}
    passed = unchanged and all(row["passed"] for row in observed.values()) and all(row["passed"] for row in restored.values())
    passed = passed and all(row["caught"] for row in planted.values())
    record = {
        "created_utc": datetime.now(UTC).isoformat(),
        "classification": "Synthetic",
        "proof": "engine_setup_with_stand_ins",
        "base_commit": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, timeout=5, check=True).stdout.strip(),
        "sources": sources,
        "candidate_unchanged": unchanged,
        "cases": observed,
        "mutations": planted,
        "restored_cases": restored,
        "dependencies": {"python": sys.version.split()[0], "ruff": version("ruff"), "mypy": version("mypy"), "PyYAML": version("PyYAML")},
        "reproduce": f".venv/bin/python -B -m scripts.orchestration.ecs_engine_cases --report {destination.relative_to(ROOT)}"
        + (" --mutations" if args.mutations else ""),
        "passed": passed,
        "cleanup": "done",
        "limits": ["No native engine, image, Docker, network or AWS execution", "Periodic observations do not prove a continuous peak"],
    }
    destination.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(json.dumps({"passed": passed, "cases": len(observed), "mutations": len(planted), "report": str(destination.relative_to(ROOT))}) + "\n")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
