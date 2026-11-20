"""Light cases of the feature-store orchestration (lane B, step 3): renders, protocol, launcher methods and DAG graphs.

Run ``.venv/bin/python -B -m scripts.orchestration.run_features_pipeline_light [--case <name> ...]``. Nothing is started: no launcher process, socket,
container, Docker or AWS call. The launcher's pipeline methods run in process on a synthetic registry state, and every file lives in a temporary folder
that is removed at the end. The report goes to ``data/e2e/features_pipeline_light/<UTC stamp>/report.json`` (case names, check names and verdicts only).
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import copy
import dataclasses
import functools
import hashlib
import importlib
import importlib.util
import json
import os
import re
import signal
import sys
import tempfile
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

from orchestration import graph, launcher, online_store, pipeline_protocol, pipeline_render
from orchestration.interface import SubmissionRequest
from orchestration.pipeline_session import CLASS_ROLE, RoleSession
from scripts.orchestration.ledger import schema_validator
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
BASE_REVISION = "75f348719b03054e62d887390b4c438ff62ef5ff"
RUN_SECONDS = 120
RUN = "features_0a1b2c3d"
GOLD_RUN = "gold_1a2b3c4d"
FEATURES_DAG = "features_base_dag"
BASE_DAG = FEATURES_DAG
# The feature store DAG and the step DAG of the single features run are gone.
STORE_DAG = "feature_store_dag"
REGISTRY_ID = "0a1b2c3d-1111-4222-8333-444455556666"
ONLINE_NETWORK = "hai-0a1b2c3d-online-store"
FAMILIES = {FEATURES_DAG: "features", STORE_DAG: "feature_store"}
SHA = {name: character * 64 for name, character in (("source", "a"), ("bronze", "b"), ("silver_pin", "c"), ("silver_input", "d"))}
SHA.update({"gold_pin": "e" * 64, "gold_input": "f" * 64, "sealed": "1" * 64})
FAKE_VALUES = {
    "AWS_REGION": "us-test-1",
    "POLARIS_READER_CLIENT_ID": "reader-id-fake",
    "POLARIS_READER_CLIENT_SECRET": "reader-secret-fake",
    "POLARIS_SILVER_WRITER_CLIENT_ID": "silver-id-fake",
    "POLARIS_SILVER_WRITER_CLIENT_SECRET": "silver-secret-fake",
    "POLARIS_GOLD_WRITER_CLIENT_ID": "gold-id-fake",
    "POLARIS_GOLD_WRITER_CLIENT_SECRET": "gold-secret-fake",
    "POLARIS_FEATURES_WRITER_CLIENT_ID": "features-id-fake",
    "POLARIS_FEATURES_WRITER_CLIENT_SECRET": "features-secret-fake",
}
SESSION_KEY = "FAKEKEYID"
ENVIRONMENT = {"JOB_THREADS": "1", "DUCKDB_MEMORY_LIMIT": "1GB", "SPARK_JOB_MEMORY": "1g"}

Checks = dict[str, bool]


class RunnerDeadline(BaseException):
    """The whole run passed its deadline; a BaseException, so the per-case error report cannot catch it and the run ends at once."""


class CaseError(Exception):
    """A case could not set up its fixture."""


@dataclasses.dataclass
class Input:
    """The render input a launcher passes to ``pipeline_render.build``."""

    template: str
    run_folder: Path | None
    run_mode: str
    args: Sequence[str]
    params: Mapping[str, Any]
    pipeline: Mapping[str, Any]
    values: Mapping[str, str]
    snapshot: Path | None
    provenance: Mapping[str, str]
    generation: int


def refused(action: Callable[[], Any], *errors: type[BaseException]) -> bool:
    """True when ``action`` raises one of ``errors``."""
    try:
        action()
    except errors:
        return True
    return False


def write_json(path: Path, document: Any, mode: int = 0o600) -> str:
    """Write a JSON file and return the SHA-256 of its bytes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()
    path.write_bytes(raw)
    path.chmod(mode)
    return hashlib.sha256(raw).hexdigest()


class Fixture:
    """One temporary folder with a run folder, scoped AWS stand-in files, a frozen snapshot and a local catalog folder."""

    def __init__(self, base: Path) -> None:
        self.base = base
        self.folder = base / "runs" / RUN
        self.publish = self.folder / "publish"
        self.publish.mkdir(parents=True)
        self.snapshot = base / "snapshot"
        (self.snapshot / "project").mkdir(parents=True)
        (self.snapshot / "packages").mkdir()
        self.catalog = base / "catalog"
        self.catalog.mkdir()
        self.result_sha = write_json(self.publish / "consumer_result.json", {"run_id": RUN})

    def pipeline(self, mode: str) -> dict[str, Any]:
        """The trusted metadata the launcher would expose for a run with every input accepted."""
        layers = ("silver", "gold", "features")
        return {
            "registry_kind": "test" if mode == "publish_fixture" else "real",
            "source_sha256": SHA["source"],
            "bronze_sha256": SHA["bronze"],
            "silver_pin": {"sha256": SHA["silver_pin"]},
            "silver_input": {"sha256": SHA["silver_input"], "pins_sha256": SHA["silver_pin"]},
            "gold_pin": {"sha256": SHA["gold_pin"]},
            "gold_input": {"sha256": SHA["gold_input"], "pins_sha256": SHA["gold_pin"]},
            "layer_completions": {
                layer: {"run_id": RUN, "generation": 1, "sha256": SHA["sealed"], "file": f"layer_sealed/{layer}/{SHA['sealed']}.duckdb"} for layer in layers
            },
            "layer_exports": {layer: {"sealed_sha256": SHA["sealed"], "generation": 1, "sha256": self.result_sha} for layer in layers},
            "local_catalog": str(self.catalog),
            "online_store": {"phase": "up", "network": ONLINE_NETWORK},
            "network": {"phase": "ready", "names": {"lakehouse_read": "net-read", "lakehouse_write": "net-write"}},
        }

    def render(self, module: ModuleType, template: str, params: Mapping[str, Any], mode: str, pipeline: Mapping[str, Any] | None = None) -> Any:
        """Render one job with ``module``'s ``build``."""
        state = self.pipeline(mode) if pipeline is None else pipeline
        inp = Input(template, self.folder, mode, [], dict(params), state, ENVIRONMENT, self.snapshot, {"PROVENANCE_FAKE": "1"}, 1)
        values = {key: FAKE_VALUES[key] for key in module.needed_values(template, dict(params), mode) if key in FAKE_VALUES}
        selected = module.job_class(template, dict(params), mode)
        # A real job of an AWS class gets the role session of its class; an offline job and every fixture run get none.
        needs_session = mode == "real" and selected != "offline" and template != "pipeline_probe"
        session = RoleSession(CLASS_ROLE[selected], SESSION_KEY, "fake-secret", "fake-token", 2**40) if needs_session else None
        return module.build(inp, ROOT, values, session)


def spec_dict(spec: Any) -> dict[str, Any]:
    """A job specification as plain data."""
    return dataclasses.asdict(spec)


def base_module(name: str, relative: str, scratch: Path) -> ModuleType:
    """Import the base revision's copy of a module from a scratch file."""
    source = run_command("git", ["show", f"{BASE_REVISION}:{relative}"], cwd=ROOT, timeout=30, check=True).stdout
    path = scratch / f"{name}.py"
    path.write_text(source)
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise CaseError(f"cannot load the base copy of {relative}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------------------------------------------------
# Renders (E01 render parts, E05 fixture isolation, regression R1)
# ---------------------------------------------------------------------------------------------------------------------

SILVER_GOLD_JOBS: tuple[tuple[str, dict[str, Any]], ...] = (
    ("pipeline_dbt", {"layer": "silver"}),
    ("pipeline_dbt", {"layer": "gold"}),
    ("pipeline_export", {"layer": "silver"}),
    ("pipeline_export", {"layer": "gold"}),
    ("pipeline_spark", {"job": "pin_silver", "source_run": "latest"}),
    ("pipeline_spark", {"job": "pin_silver", "source_run": GOLD_RUN}),
    ("pipeline_spark", {"job": "read_silver"}),
    ("pipeline_spark", {"job": "record_silver_snapshot"}),
    ("pipeline_spark", {"job": "pin_bronze"}),
    ("pipeline_spark", {"job": "verify_bronze"}),
    *(("pipeline_publish", {"layer": layer, "step": step}) for layer in ("silver", "gold") for step in ("candidates", "reconcile", "backup", "commit")),
    ("pipeline_publish", {"layer": "gold", "step": "maintenance"}),
)


def case_render_regression(fixture: Fixture, scratch: Path) -> Checks:
    """R1: every silver and gold job renders exactly as the base revision's renderer renders it, with the same class and value names."""
    base = base_module("pipeline_render_base", "orchestration/pipeline_render.py", scratch)
    checks: Checks = {}
    for mode in ("publish_fixture", "real"):
        for template, params in SILVER_GOLD_JOBS:
            label = f"{mode}_{template}_{'_'.join(str(value) for value in params.values())}"
            old = spec_dict(fixture.render(base, template, params, mode))
            new = spec_dict(fixture.render(pipeline_render, template, params, mode))
            checks[f"same_spec_{label}"] = old == new
            checks[f"same_class_{label}"] = base.job_class(template, dict(params), mode) == pipeline_render.job_class(template, dict(params), mode)
            checks[f"same_values_{label}"] = base.needed_values(template, dict(params), mode) == pipeline_render.needed_values(template, dict(params), mode)
    return checks


def case_render_build(fixture: Fixture, scratch: Path) -> Checks:
    """E01 (LB1): the features build reads only the accepted gold pin and its own extract."""
    checks: Checks = {}
    for mode in ("publish_fixture", "real"):
        spec = fixture.render(pipeline_render, "pipeline_dbt", {"layer": "features"}, mode)
        arguments = spec.arguments
        checks[f"{mode}_module"] = spec.module == "scripts.features.features_build" and spec.service == "analytics-dbt"
        checks[f"{mode}_layer"] = arguments[arguments.index("--layer") + 1] == "features"
        checks[f"{mode}_input_is_gold_pin"] = arguments[arguments.index("--input-sha256") + 1] == SHA["gold_pin"]
        checks[f"{mode}_extract_is_gold_input"] = arguments[arguments.index("--extract-sha256") + 1] == SHA["gold_input"]
        checks[f"{mode}_offline"] = spec.network == "none" and spec.network_class == "offline"
        checks[f"{mode}_schema_read_only"] = (str(ROOT / "data_contracts/features"), "/workspace/data_contracts/features", True) in [
            tuple(m) for m in spec.mounts
        ]
        for other in ("silver", "gold"):
            other_spec = fixture.render(pipeline_render, "pipeline_dbt", {"layer": other}, mode)
            checks[f"{mode}_{other}_no_schema_mount"] = not any(target == "/workspace/data_contracts/features" for _, target, _ in other_spec.mounts)
        moved = fixture.pipeline(mode)
        moved["gold_input"] = {"sha256": SHA["gold_input"], "pins_sha256": SHA["silver_pin"]}
        checks[f"{mode}_refuses_extract_of_other_pin"] = refused(
            functools.partial(fixture.render, pipeline_render, "pipeline_dbt", {"layer": "features"}, mode, moved), pipeline_protocol.PipelineError
        )
        missing = fixture.pipeline(mode)
        missing["gold_pin"] = {}
        checks[f"{mode}_refuses_without_gold_pin"] = refused(
            functools.partial(fixture.render, pipeline_render, "pipeline_dbt", {"layer": "features"}, mode, missing),
            pipeline_protocol.PipelineError,
        )
        silver_only = fixture.pipeline(mode)
        silver_only.pop("gold_pin")
        silver_only.pop("gold_input")
        checks[f"{mode}_never_reads_silver_pin"] = refused(
            functools.partial(fixture.render, pipeline_render, "pipeline_dbt", {"layer": "features"}, mode, silver_only),
            pipeline_protocol.PipelineError,
        )
    checks["unknown_layer_refused"] = refused(
        lambda: fixture.render(pipeline_render, "pipeline_dbt", {"layer": "bronze"}, "publish_fixture"), pipeline_protocol.PipelineError
    )
    return checks


def case_render_modules(fixture: Fixture, scratch: Path) -> Checks:
    """E01 (LB2): export and publish of features run the scripts.features modules on the sealed copy."""
    checks: Checks = {}
    for mode in ("publish_fixture", "real"):
        spec = fixture.render(pipeline_render, "pipeline_export", {"layer": "features"}, mode)
        checks[f"{mode}_export_module"] = spec.module == "scripts.features.features_export" and spec.service == "quality"
        checks[f"{mode}_export_reads_seal"] = (
            str(fixture.folder / f"layer_sealed/features/{SHA['sealed']}.duckdb"),
            "/workspace/sealed/sealed.duckdb",
            True,
        ) in [tuple(mount) for mount in spec.mounts]
        for step in ("candidates", "reconcile", "backup", "commit"):
            spec = fixture.render(pipeline_render, "pipeline_publish", {"layer": "features", "step": step}, mode)
            checks[f"{mode}_{step}_module"] = spec.module == "scripts.features.features_publish" and spec.arguments[0] == step
    return checks


def case_render_publish(fixture: Fixture, scratch: Path) -> Checks:
    """E01 (LB7): features has no maintenance step; an unaccepted export refuses the publish render."""
    checks: Checks = {}
    for mode in ("publish_fixture", "real"):
        checks[f"{mode}_maintenance_refused"] = refused(
            functools.partial(fixture.render, pipeline_render, "pipeline_publish", {"layer": "features", "step": "maintenance"}, mode),
            pipeline_protocol.PipelineError,
        )
        unexported = fixture.pipeline(mode)
        unexported["layer_exports"] = {}
        checks[f"{mode}_needs_accepted_export"] = refused(
            functools.partial(fixture.render, pipeline_render, "pipeline_publish", {"layer": "features", "step": "commit"}, mode, unexported),
            pipeline_protocol.PipelineError,
        )
    return checks


def case_render_writer(fixture: Fixture, scratch: Path) -> Checks:
    """E01 (LB3): a real features publish writes only as the features writer; an unknown layer is refused, not defaulted."""
    checks: Checks = {}
    names = pipeline_render.needed_values("pipeline_publish", {"layer": "features", "step": "commit"}, "real")
    checks["names_features_writer"] = "POLARIS_FEATURES_WRITER_CLIENT_ID" in names and "POLARIS_FEATURES_WRITER_CLIENT_SECRET" in names
    checks["names_no_gold_writer"] = not any("GOLD" in name or "SILVER" in name for name in names)
    for step in ("candidates", "backup", "commit"):
        spec = fixture.render(pipeline_render, "pipeline_publish", {"layer": "features", "step": step}, "real")
        checks[f"{step}_writer_principal"] = spec.environment.get("POLARIS_CLIENT_ID") == FAKE_VALUES["POLARIS_FEATURES_WRITER_CLIENT_ID"]
        checks[f"{step}_writer_class"] = spec.network_class == "lakehouse_write" and spec.network == "net-write"
    spec = fixture.render(pipeline_render, "pipeline_publish", {"layer": "features", "step": "reconcile"}, "real")
    checks["reconcile_reader_principal"] = spec.environment.get("POLARIS_CLIENT_ID") == FAKE_VALUES["POLARIS_READER_CLIENT_ID"]
    checks["unknown_writer_layer_refused"] = refused(
        lambda: pipeline_render.needed_values("pipeline_publish", {"layer": "bronze", "step": "commit"}, "real"), pipeline_protocol.PipelineError
    )
    return checks


def case_render_spark(fixture: Fixture, scratch: Path) -> Checks:
    """E01 (LB4): gold pin, gold read and the features snapshot record run their fixed modules and record files."""
    checks: Checks = {}
    for mode in ("publish_fixture", "real"):
        spec = fixture.render(pipeline_render, "pipeline_spark", {"job": "pin_gold", "source_run": "latest"}, mode)
        checks[f"{mode}_pin_module"] = spec.module == "scripts.features.features_input" and spec.arguments[0] == "pin"
        checks[f"{mode}_pin_record"] = spec.arguments[spec.arguments.index("--record") + 1] == "/workspace/out/gold_pins.json"
        checks[f"{mode}_pin_latest_names_no_source"] = "--source-run" not in spec.arguments
        named = fixture.render(pipeline_render, "pipeline_spark", {"job": "pin_gold", "source_run": GOLD_RUN}, mode)
        checks[f"{mode}_pin_named_source"] = named.arguments[named.arguments.index("--source-run") + 1] == GOLD_RUN
        read = fixture.render(pipeline_render, "pipeline_spark", {"job": "read_gold"}, mode)
        checks[f"{mode}_read_module"] = read.module == "scripts.features.features_input" and read.arguments[0] == "read"
        checks[f"{mode}_read_expects_gold_pin"] = read.arguments[read.arguments.index("--expected-pin-sha256") + 1] == SHA["gold_pin"]
        record = fixture.render(pipeline_render, "pipeline_spark", {"job": "record_features_snapshot"}, mode)
        checks[f"{mode}_record_module"] = record.module == "scripts.features.features_publish" and record.arguments[0] == "record"
        checks[f"{mode}_record_file"] = record.arguments[record.arguments.index("--record") + 1] == "/workspace/out/features_snapshot.json"
        checks[f"{mode}_source_run_only_on_pins"] = refused(
            functools.partial(fixture.render, pipeline_render, "pipeline_spark", {"job": "read_gold", "source_run": GOLD_RUN}, mode),
            pipeline_protocol.PipelineError,
        )
        for job in ("pin_gold", "read_gold", "record_features_snapshot"):
            spec = fixture.render(pipeline_render, "pipeline_spark", {"job": job}, mode)
            local = "--local-warehouse" in spec.arguments
            checks[f"{mode}_{job}_warehouse"] = local if mode == "publish_fixture" else not local
            checks[f"{mode}_{job}_class"] = spec.network_class == ("offline" if mode == "publish_fixture" else "lakehouse_read")
    checks["unknown_job_refused"] = refused(
        lambda: fixture.render(pipeline_render, "pipeline_spark", {"job": "pin_features"}, "publish_fixture"), pipeline_protocol.PipelineError
    )
    return checks


FEAST_MOUNTS = {
    ("scripts", "/workspace/scripts", True),
    ("orchestration", "/workspace/orchestration", True),
    ("feature_store", "/workspace/feature_store", True),
    ("data_contracts/features", "/workspace/data_contracts/features", True),
}


def case_render_feast(fixture: Fixture, scratch: Path) -> Checks:
    """E01 (LB6): Feast jobs take closed steps; the probe is real-only; the mounts are the design's."""
    checks: Checks = {}
    for mode in ("publish_fixture", "real"):
        for step in ("apply", "materialize"):
            spec = fixture.render(pipeline_render, "pipeline_feast", {"step": step}, mode)
            checks[f"{mode}_{step}_module"] = spec.module == "scripts.features.feast_job" and spec.service == "feature-store"
            checks[f"{mode}_{step}_arguments"] = spec.arguments == [step, "--run", RUN, "--mode", mode]
            repository = {
                (str(Path(source).relative_to(ROOT)), target, read_only) for source, target, read_only in spec.mounts if Path(source).is_relative_to(ROOT)
            }
            deployment = {("infra/deployment.auto.tfvars.json", "/workspace/infra/deployment.auto.tfvars.json", True)} if mode == "real" else set()
            checks[f"{mode}_{step}_repository_mounts"] = repository == FEAST_MOUNTS | deployment
            checks[f"{mode}_{step}_run_folder_writable"] = (str(fixture.folder), "/workspace/out", False) in [tuple(mount) for mount in spec.mounts]
            catalog = [tuple(mount) for mount in spec.mounts if mount[1] == "/workspace/local_catalog"]
            wanted = [(str(fixture.catalog), "/workspace/local_catalog", True)] if mode == "publish_fixture" else []
            checks[f"{mode}_{step}_local_catalog_read_only"] = catalog == wanted
        checks[f"{mode}_unknown_step_refused"] = refused(
            functools.partial(fixture.render, pipeline_render, "pipeline_feast", {"step": "teardown"}, mode), pipeline_protocol.PipelineError
        )
        checks[f"{mode}_extra_param_refused"] = refused(
            functools.partial(fixture.render, pipeline_render, "pipeline_feast", {"step": "apply", "source_run": GOLD_RUN}, mode),
            pipeline_protocol.PipelineError,
        )
    no_catalog = fixture.pipeline("publish_fixture")
    no_catalog["local_catalog"] = ""
    checks["fixture_needs_local_catalog"] = refused(
        functools.partial(fixture.render, pipeline_render, "pipeline_feast", {"step": "apply"}, "publish_fixture", no_catalog), pipeline_protocol.PipelineError
    )
    probe_mounts = fixture.render(pipeline_render, "pipeline_feast", {"step": "probe"}, "real").mounts
    checks["probe_no_local_catalog"] = not any(target == "/workspace/local_catalog" for _, target, _ in probe_mounts)
    checks["probe_fixture_refused"] = refused(
        lambda: fixture.render(pipeline_render, "pipeline_feast", {"step": "probe"}, "publish_fixture"), pipeline_protocol.PipelineError
    )
    probe = fixture.render(pipeline_render, "pipeline_feast", {"step": "probe"}, "real")
    checks["probe_real_registry_key"] = probe.environment.get("FEAST_REGISTRY_KEY") == "lakehouse/feast_e2e/registry.pb"
    return checks


FEATURE_JOBS: tuple[tuple[str, dict[str, Any]], ...] = (
    ("pipeline_spark", {"job": "pin_gold", "source_run": "latest"}),
    ("pipeline_spark", {"job": "read_gold"}),
    ("pipeline_dbt", {"layer": "features"}),
    ("pipeline_export", {"layer": "features"}),
    *(("pipeline_publish", {"layer": "features", "step": step}) for step in ("candidates", "reconcile", "backup", "commit")),
    ("pipeline_spark", {"job": "record_features_snapshot"}),
    ("pipeline_feast", {"step": "apply"}),
    ("pipeline_feast", {"step": "materialize"}),
)


def case_fixture_isolation(fixture: Fixture, scratch: Path) -> Checks:
    """E05/E30 render half (LB5, CR6): fixture feature jobs carry no AWS profile, principal, proxy, REST endpoint or S3 setting."""
    checks: Checks = {}
    for template, params in FEATURE_JOBS:
        label = f"{template}_{'_'.join(str(value) for value in params.values())}"
        spec = fixture.render(pipeline_render, template, params, "publish_fixture")
        leaked = [key for key in spec.environment if key.startswith(("AWS_", "POLARIS_", "HTTP", "http", "JAVA_TOOL")) or key == "REDIS_PASSWORD"]
        checks[f"{label}_no_credentials"] = not leaked
        wanted_network = ONLINE_NETWORK if template == "pipeline_feast" else "none"
        checks[f"{label}_offline"] = spec.network == wanted_network and spec.network_class == "offline"
        checks[f"{label}_no_aws_mount"] = not any(target.startswith("/run/pipeline/aws") for _, target, _ in spec.mounts)
        checks[f"{label}_needs_no_values"] = pipeline_render.needed_values(template, dict(params), "publish_fixture") == ()
    fixture_feast = fixture.render(pipeline_render, "pipeline_feast", {"step": "apply"}, "publish_fixture").environment
    checks["fixture_feast_namespace"] = fixture_feast.get("FEAST_NAMESPACE") == "features_e2e"
    checks["fixture_feast_sql_catalog"] = fixture_feast.get("FEAST_CATALOG_TYPE") == "sql"
    checks["fixture_feast_registry_in_run_folder"] = fixture_feast.get("FEAST_REGISTRY_PATH") == "/workspace/out/feast/registry.pb"
    checks["fixture_feast_no_registry_key"] = "FEAST_REGISTRY_KEY" not in fixture_feast
    real = fixture.render(pipeline_render, "pipeline_feast", {"step": "apply"}, "real")
    checks["real_feast_namespace"] = real.environment.get("FEAST_NAMESPACE") == "features"
    checks["real_feast_rest_catalog"] = real.environment.get("FEAST_CATALOG_TYPE") == "rest"
    checks["real_feast_registry_key"] = real.environment.get("FEAST_REGISTRY_KEY") == "lakehouse/feast/registry.pb"
    checks["real_feast_no_local_registry"] = "FEAST_REGISTRY_PATH" not in real.environment
    checks["real_feast_reader_only"] = (
        real.network_class == "lakehouse_read"
        and real.environment.get("POLARIS_CLIENT_ID") == FAKE_VALUES["POLARIS_READER_CLIENT_ID"]
        and real.environment.get("AWS_ACCESS_KEY_ID") == SESSION_KEY
    )
    checks["real_feast_no_java_options"] = "JAVA_TOOL_OPTIONS" not in real.environment
    checks["redis_host_fixed"] = fixture_feast.get("FEAST_REDIS_HOST") == "redis" and real.environment.get("FEAST_REDIS_HOST") == "redis"
    return checks


# ---------------------------------------------------------------------------------------------------------------------
# Protocol (E08 seal, E31 gold pin)
# ---------------------------------------------------------------------------------------------------------------------


def validation_state(folder: Path, layer: str, template: str, digest: str) -> dict[str, Any]:
    """A registry state with one successful validation attempt of ``template`` that validated ``digest``."""
    result_sha = write_json(folder / f"quality_{layer}/result.json", {"counts": {"fail": 0, "warn": 0}, "build_file_sha256": digest})
    logs = folder / "attempts" / "at-validate"
    logs.mkdir(parents=True, exist_ok=True)
    (logs / "stdout.txt").write_text(json.dumps({"result_sha256": result_sha, "build_file_sha256": digest}) + "\n")
    attempt = {
        "id": "at-validate",
        "owner": f"run:{RUN}",
        "template": template,
        "phase": "ended",
        "reason": "exited",
        "exit_code": 0,
        "cancel_intent": None,
        "generation": 1,
        "created_at": "2026-10-22T12:00:00+00:00",
        "log_tail": str(logs / "log_tail.txt"),
        "params": {"database": str(folder / "staging.duckdb"), "out_host": str(folder / f"quality_{layer}")},
    }
    return {"runs": {RUN: {"run_id": RUN, "generation": 1, "state": "held"}}, "attempts": {attempt["id"]: attempt}}


def case_seal(fixture: Fixture, scratch: Path) -> Checks:
    """E08 (LB9, LB10): the features layer seals and its validation binds only a features validation of this generation."""
    checks: Checks = {}
    staging = fixture.folder / "staging.duckdb"
    staging.write_bytes(b"features build fixture bytes")
    digest = hashlib.sha256(staging.read_bytes()).hexdigest()
    sealed = pipeline_protocol.seal_copy(fixture.folder, "features", digest)
    checks["seal_file"] = sealed == {"file": f"layer_sealed/features/{digest}.duckdb", "sha256": digest}
    checks["seal_read_only"] = (fixture.folder / sealed["file"]).stat().st_mode & 0o222 == 0
    checks["seal_repeat"] = pipeline_protocol.seal_copy(fixture.folder, "features", digest) == sealed
    checks["unknown_layer_refused"] = refused(lambda: pipeline_protocol.seal_copy(fixture.folder, "bronze", digest), pipeline_protocol.PipelineError)
    checks["cross_still_not_a_layer"] = refused(lambda: pipeline_protocol.fixed_layer("cross"), pipeline_protocol.PipelineError)
    state = validation_state(fixture.folder, "features", "pipeline_validate_features", digest)
    accepted = pipeline_protocol.accept_validation(state, RUN, "features", "at-validate", fixture.folder)
    checks["validation_accepted"] = accepted["layer"] == "features" and accepted["sha256"] == digest and accepted["file"] == sealed["file"]
    other = validation_state(fixture.folder, "features", "pipeline_validate_gold", digest)
    checks["gold_validation_refused_for_features"] = refused(
        lambda: pipeline_protocol.accept_validation(other, RUN, "features", "at-validate", fixture.folder), pipeline_protocol.PipelineError
    )
    stale = validation_state(fixture.folder, "features", "pipeline_validate_features", digest)
    stale["runs"][RUN]["generation"] = 2
    checks["stale_generation_refused"] = refused(
        lambda: pipeline_protocol.accept_validation(stale, RUN, "features", "at-validate", fixture.folder), pipeline_protocol.PipelineError
    )
    completion = {"run_id": RUN, "generation": 1, "layer_completions": {"features": {"run_id": RUN, "generation": 1, "sha256": digest}}}
    checks["completion_accepted"] = pipeline_protocol.accepted_completion(completion, "features")["sha256"] == digest
    return checks


def pin_record(mode: str, namespace: str, tables: set[str], source_run: str = GOLD_RUN) -> dict[str, Any]:
    """A pin record of the shape ``silver_publish.pin`` writes."""
    return {
        "schema": 1,
        "run_id": RUN,
        "mode": mode,
        "namespace": namespace,
        "source_run_id": source_run,
        "snapshot_set": {name: index + 1 for index, name in enumerate(sorted(tables))},
    }


def contract_tables(name: str) -> set[str]:
    """The publication tables of a lakehouse contract."""
    document = json.loads((ROOT / "data_contracts/lakehouse" / name).read_text())
    return set(document["publish"]["tables"])


def case_pin_gold(fixture: Fixture, scratch: Path) -> Checks:
    """E31 (LB11, regression R2): a gold pin names the gold namespace of its mode and exactly the gold tables; the silver default is unchanged."""
    gold, silver = contract_tables("gold_contract.json"), contract_tables("silver_publish.json")
    check = pipeline_protocol.check_pin
    checks: Checks = {}
    checks["fixture_gold_accepted"] = not refused(
        lambda: check(pin_record("publish_fixture", "gold_e2e", gold), RUN, "publish_fixture", gold, source="gold"), Exception
    )
    checks["real_gold_accepted"] = not refused(lambda: check(pin_record("real", "gold", gold), RUN, "real", gold, source="gold"), Exception)
    checks["fixture_real_namespace_refused"] = refused(
        lambda: check(pin_record("publish_fixture", "gold", gold), RUN, "publish_fixture", gold, source="gold"), pipeline_protocol.PipelineError
    )
    checks["silver_namespace_refused"] = refused(
        lambda: check(pin_record("publish_fixture", "silver_e2e", gold), RUN, "publish_fixture", gold, source="gold"), pipeline_protocol.PipelineError
    )
    checks["missing_table_refused"] = refused(
        lambda: check(pin_record("publish_fixture", "gold_e2e", gold - {sorted(gold)[0]}), RUN, "publish_fixture", gold, source="gold"),
        pipeline_protocol.PipelineError,
    )
    checks["unknown_source_refused"] = refused(
        lambda: check(pin_record("publish_fixture", "features_e2e", gold), RUN, "publish_fixture", gold, source="features"), pipeline_protocol.PipelineError
    )
    record = pin_record("publish_fixture", "gold_e2e", gold)
    for label, change in (
        ("unknown_mode", {"mode": "fixture"}),
        ("bad_source_run", {"source_run_id": "latest"}),
        ("other_run", {"run_id": GOLD_RUN}),
        ("extra_field", {"note": "x"}),
        ("schema_two", {"schema": 2}),
    ):
        changed = {**record, **change}
        mode = str(changed["mode"])
        checks[f"refuses_{label}"] = refused(functools.partial(check, changed, RUN, mode, gold, source="gold"), pipeline_protocol.PipelineError)
    for label, value in (("zero_snapshot", 0), ("bool_snapshot", True), ("huge_snapshot", 2**63)):
        snapshots = dict(record["snapshot_set"])
        snapshots[sorted(snapshots)[0]] = value
        changed = {**record, "snapshot_set": snapshots}
        checks[f"refuses_{label}"] = refused(functools.partial(check, changed, RUN, "publish_fixture", gold, source="gold"), pipeline_protocol.PipelineError)
    checks["silver_default_accepted"] = not refused(
        lambda: check(pin_record("publish_fixture", "silver_e2e", silver), RUN, "publish_fixture", silver), Exception
    )
    checks["silver_default_refuses_gold"] = refused(
        lambda: check(pin_record("publish_fixture", "gold_e2e", silver), RUN, "publish_fixture", silver), pipeline_protocol.PipelineError
    )
    try:
        check(pin_record("publish_fixture", "gold_e2e", silver), RUN, "publish_fixture", silver)
    except pipeline_protocol.PipelineError as error:
        checks["silver_message_unchanged"] = str(error) == "the silver pin identity does not match this consumer"
    return checks


# ---------------------------------------------------------------------------------------------------------------------
# Launcher methods in process (E31, E08 receipt input, LB12 to LB16)
# ---------------------------------------------------------------------------------------------------------------------


def launcher_state(mode: str) -> dict[str, Any]:
    """A registry state with one held transform run."""
    run = {
        "run_id": RUN,
        "mode": mode,
        "domain": "transform",
        "state": "held",
        "generation": 1,
        "pid": os.getpid(),
        "pid_start": "fake-start",
        "registrations": ["catalog_user"],
    }
    return {"runs": {RUN: run}, "readers": {}, "guards": {}, "attempts": {}, "copies": {}, "registrations": {}, "launcher": {}}


def in_process(fixture: Fixture) -> launcher.Launcher:
    """A launcher object whose pipeline methods can run on a synthetic state; nothing is started."""
    # A bare object of the launcher class: only the attributes its pipeline methods read, as stand-ins (Any, since they are not the real types).
    server: Any = object.__new__(launcher.Launcher)
    server.cfg = SimpleNamespace(runs_root=fixture.base / "runs", root=ROOT)
    server.registry = SimpleNamespace(kind="test", path=fixture.base / "registry")
    server.registry_id = REGISTRY_ID
    server.foreign = []
    return server


def ctx_of(state: dict[str, Any], method: str, params: dict[str, Any]) -> launcher.Ctx:
    """A method context authenticated as the run."""
    connection: Any = None  # a method handler that never answers on its connection needs none
    return launcher.Ctx(connection, method, "req-fake", params, {"run_id": RUN}, "key-fake", state)


def producer(state: dict[str, Any], fixture: Fixture, attempt_id: str, template: str, params: dict[str, Any], output: dict[str, Any]) -> None:
    """Record a successful producer attempt whose last stdout line is ``output``."""
    logs = fixture.folder / "attempts" / attempt_id
    logs.mkdir(parents=True, exist_ok=True)
    (logs / "stdout.txt").write_text(json.dumps(output) + "\n")
    state["attempts"][attempt_id] = {
        "id": attempt_id,
        "owner": f"run:{RUN}",
        "template": template,
        "generation": 1,
        "phase": "ended",
        "reason": "exited",
        "exit_code": 0,
        "cancel_intent": None,
        "params": params,
        "checkout": str(ROOT),
        "created_at": f"2026-10-22T12:00:{len(state['attempts']):02d}+00:00",
        "log_tail": str(logs / "log_tail.txt"),
    }


def launcher_error(action: Callable[[], Any]) -> str:
    """The launcher error text of ``action``, or an empty string when it succeeds."""
    try:
        action()
    except launcher.LauncherError as error:
        return str(error) or "refused"
    return ""


def case_launcher_gold(fixture: Fixture, scratch: Path) -> Checks:
    """E31 (LB12, LB13): reserve, pin and input of gold under their own keys; the reservation and registration protect the pinned set."""
    server = in_process(fixture)
    checks: Checks = {}
    gold = contract_tables("gold_contract.json")
    state = launcher_state("publish_fixture")
    pins = fixture.folder / "gold_pins.json"
    pins.unlink(missing_ok=True)
    digest = write_json(pins, pin_record("publish_fixture", "gold_e2e", gold), 0o644)
    producer(state, fixture, "at-pin", "pipeline_spark", {"job": "pin_gold", "source_run": "latest"}, {"sha256": digest})
    early = launcher_error(lambda: server.m_pipeline_accept_gold_pin(ctx_of(state, "pipeline_accept_gold_pin", {"attempt_id": "at-pin"})))
    checks["pin_before_reserve_refused"] = "no owned reservation" in early and "gold_pin" not in state["runs"][RUN]
    fenced = launcher_state("publish_fixture")
    fenced["guards"][launcher.MAINTENANCE_FENCE] = {"writer": "at-maintenance", "readers": {}}
    checks["reserve_refused_under_fence"] = bool(launcher_error(lambda: server.m_pipeline_reserve_gold(ctx_of(fenced, "pipeline_reserve_gold", {}))))
    reserved = server.m_pipeline_reserve_gold(ctx_of(state, "pipeline_reserve_gold", {}))
    reader = state["readers"].get(f"gold:{RUN}", {})
    checks["reserved"] = reserved == {"reader": f"gold:{RUN}", "state": "reserved"} and reader.get("namespace") == "gold_e2e"
    checks["reserve_repeat"] = server.m_pipeline_reserve_gold(ctx_of(state, "pipeline_reserve_gold", {})) == reserved
    checks["reserve_extra_param_refused"] = bool(launcher_error(lambda: server.m_pipeline_reserve_gold(ctx_of(state, "pipeline_reserve_gold", {"x": 1}))))
    pending = launcher.Launcher.maintenance_protection(state)
    checks["maintenance_protects_reservation"] = pending == [{"kind": "pending", "id": f"gold:{RUN}", "since": reader.get("at")}]
    real_state = launcher_state("real")
    server.m_pipeline_reserve_gold(ctx_of(real_state, "pipeline_reserve_gold", {}))
    checks["real_namespace"] = real_state["readers"][f"gold:{RUN}"]["namespace"] == "gold"
    accepted = server.m_pipeline_accept_gold_pin(ctx_of(state, "pipeline_accept_gold_pin", {"attempt_id": "at-pin"}))
    run = state["runs"][RUN]
    checks["gold_pin_key"] = run.get("gold_pin") == {"sha256": digest, "source_run_id": GOLD_RUN, "attempt_id": "at-pin"} == accepted
    checks["silver_pin_untouched"] = "silver_pin" not in run and "silver_input" not in run
    registered = launcher.Launcher.maintenance_protection(state)
    checks["maintenance_protects_registration"] = registered == [{"kind": "registration", "id": f"gold:{RUN}", "run_id": GOLD_RUN}]
    checks["pipeline_inputs_expose_gold"] = server.pipeline_inputs(state, run)["gold_pin"] == run["gold_pin"]
    producer(state, fixture, "at-pin-silver", "pipeline_spark", {"job": "pin_silver", "source_run": "latest"}, {"sha256": digest})
    checks["silver_pin_job_refused"] = bool(
        launcher_error(lambda: server.m_pipeline_accept_gold_pin(ctx_of(state, "pipeline_accept_gold_pin", {"attempt_id": "at-pin-silver"})))
    )
    producer(state, fixture, "at-pin-other", "pipeline_spark", {"job": "pin_gold", "source_run": "features_00000000"}, {"sha256": digest})
    checks["other_selection_refused"] = bool(
        launcher_error(lambda: server.m_pipeline_accept_gold_pin(ctx_of(state, "pipeline_accept_gold_pin", {"attempt_id": "at-pin-other"})))
    )
    manifest = fixture.folder / "gold_input" / "manifest.json"
    manifest_sha = write_json(manifest, {"run_id": RUN, "mode": "publish_fixture", "pins_sha256": digest}, 0o644)
    producer(state, fixture, "at-read", "pipeline_spark", {"job": "read_gold"}, {"sha256": manifest_sha})
    accepted_input = server.m_pipeline_accept_gold_input(ctx_of(state, "pipeline_accept_gold_input", {"attempt_id": "at-read"}))
    checks["gold_input_key"] = (
        state["runs"][RUN].get("gold_input") == {"sha256": manifest_sha, "pins_sha256": digest, "attempt_id": "at-read"} == accepted_input
    )
    checks["inputs_expose_gold_input"] = server.pipeline_inputs(state, state["runs"][RUN])["gold_input"] == accepted_input
    other_manifest = write_json(manifest, {"run_id": RUN, "mode": "publish_fixture", "pins_sha256": "0" * 64}, 0o644)
    producer(state, fixture, "at-read-other", "pipeline_spark", {"job": "read_gold"}, {"sha256": other_manifest})
    checks["input_of_other_pin_refused"] = bool(
        launcher_error(lambda: server.m_pipeline_accept_gold_input(ctx_of(state, "pipeline_accept_gold_input", {"attempt_id": "at-read-other"})))
    )
    released = launcher_state("publish_fixture")
    server.m_pipeline_reserve_gold(ctx_of(released, "pipeline_reserve_gold", {}))
    server.m_pipeline_reserve_silver(ctx_of(released, "pipeline_reserve_silver", {}))
    server.m_pipeline_catalog_release(ctx_of(released, "pipeline_catalog_release", {}))
    checks["release_removes_both_readers"] = released["readers"] == {}
    methods = launcher.METHODS
    checks["methods_registered"] = all(
        methods[name].kinds == ("run",) for name in ("pipeline_reserve_gold", "pipeline_accept_gold_pin", "pipeline_accept_gold_input")
    )
    return checks


def case_launcher_seal(fixture: Fixture, scratch: Path) -> Checks:
    """E08 (LB15): the features seal needs a receipt whose input is the accepted gold pin."""
    server = in_process(fixture)
    checks: Checks = {}
    for label, receipt_input, expect in (("gold_pin_input", SHA["gold_pin"], True), ("silver_pin_input", SHA["silver_pin"], False)):
        state = launcher_state("publish_fixture")
        state["runs"][RUN].update({"gold_pin": {"sha256": SHA["gold_pin"]}, "silver_pin": {"sha256": SHA["silver_pin"]}, "snapshot": "copy-1"})
        state["copies"]["copy-1"] = {"dbt_tree_sha256": SHA["source"]}
        staging = fixture.folder / "staging.duckdb"
        staging.write_bytes(b"features seal fixture " + label.encode())
        digest = hashlib.sha256(staging.read_bytes()).hexdigest()
        pipeline_protocol.seal_copy(fixture.folder, "features", digest)
        state.update(
            {key: value for key, value in validation_state(fixture.folder, "features", "pipeline_validate_features", digest).items() if key == "attempts"}
        )
        receipt = fixture.folder / "build_receipt.json"
        receipt.unlink(missing_ok=True)
        receipt_sha = write_json(
            receipt, {"run_id": RUN, "layer": "features", "mode": "publish_fixture", "source_sha256": SHA["source"], "input_sha256": receipt_input}, 0o644
        )
        producer(state, fixture, f"at-build-{label}", "pipeline_dbt", {"layer": "features"}, {"receipt_sha256": receipt_sha})
        error = launcher_error(
            functools.partial(server.m_pipeline_accept_seal, ctx_of(state, "pipeline_accept_seal", {"layer": "features", "attempt_id": "at-validate"}))
        )
        completion = state["runs"][RUN].get("layer_completions", {}).get("features", {})
        checks[label] = (not error and completion.get("sha256") == digest) if expect else bool(error) and not completion
    return checks


def case_compose_service(fixture: Fixture, scratch: Path) -> Checks:
    """E01 (LB30): the launcher's image lookup finds the feature-store service, shaped like the quality service and with no network of its own."""
    service = importlib.import_module("yaml").safe_load((ROOT / "docker-compose.yaml").read_text())["services"].get("feature-store", {})
    return {
        "image": service.get("image") == "hai-feature-store:feast0.67.0",
        "build": service.get("build") == {"context": "./services/feature_store", "additional_contexts": {"hai": "./services/hai"}},
        "profiles": service.get("profiles") == ["query"],
        "no_network": service.get("network_mode") == "none",
        "working_dir": service.get("working_dir") == "/workspace",
        "nothing_else": set(service) == {"image", "build", "profiles", "network_mode", "working_dir"},
        "launcher_lookup": launcher.ComposeModel(ROOT / "docker-compose.yaml").image("feature-store") == "hai-feature-store:feast0.67.0",
    }


def case_features_publish_job(fixture: Fixture, scratch: Path) -> Checks:
    """E06a launcher part (LB42): the offline features publish suite runs as a closed spark_e2e job with the features contracts read-only."""
    spec = launcher.SPARK_JOBS.get("features_publish_e2e", {})
    checks: Checks = {
        "shape": spec.get("template") == "spark_e2e"
        and spec.get("module") == "scripts.features.features_publish_cases"
        and spec.get("writes") == ("data/e2e/feature_store",)
        and spec.get("aws") is False
        and spec.get("contract") is True
        and spec.get("contract_files") == ("data_contracts/lakehouse/features_publish.json", "data_contracts/features")
        and spec.get("grammar") == {"--case": "values", "--stamp": "value"}
    }
    if not checks["shape"]:
        return checks
    cases = ["happy_and_types", "retry_and_new_publication", "version_two", "interruption_between_fast_forwards", "tamper", "republish_after_retirement"]
    arguments = ["--case", *cases, "--stamp", "20261023T041210Z"]
    checks["grammar_accepts_cases_and_stamp"] = launcher.validate_spark_args("features_publish_e2e", arguments) == arguments
    checks["grammar_accepts_stamp_only"] = launcher.validate_spark_args("features_publish_e2e", ["--stamp", "20261023T041210Z"]) == [
        "--stamp",
        "20261023T041210Z",
    ]
    for label, bad in (
        ("unknown_flag", ["--mode", "real"]),
        ("shell_text", ["--stamp", "x;rm"]),
        ("empty_case", ["--case"]),
        ("two_stamps", ["--stamp", "a", "b"]),
    ):
        checks[f"grammar_refuses_{label}"] = bool(launcher_error(functools.partial(launcher.validate_spark_args, "features_publish_e2e", bad)))
    renderer: Any = object.__new__(launcher.Renderer)
    renderer.checkout = ROOT
    mounts = {(mount.source, mount.target, mount.read_only) for mount in renderer.spark_mounts("features_publish_e2e", SimpleNamespace(run_folder=None))}
    checks["mounts"] = mounts == {
        (str(ROOT / "scripts"), "/workspace/scripts", True),
        (str(ROOT / "orchestration"), "/workspace/orchestration", True),
        (str(ROOT / "config"), "/workspace/config", True),
        (str(ROOT / "data_contracts/lakehouse/gold_contract.json"), "/workspace/data_contracts/lakehouse/gold_contract.json", True),
        (str(ROOT / "data_contracts/lakehouse/features_publish.json"), "/workspace/data_contracts/lakehouse/features_publish.json", True),
        (str(ROOT / "data_contracts/features"), "/workspace/data_contracts/features", True),
        (str(ROOT / "data/e2e/feature_store"), "/workspace/data/e2e/feature_store", False),
    }
    gold = {(mount.source, mount.target, mount.read_only) for mount in renderer.spark_mounts("gold_publish_e2e", SimpleNamespace(run_folder=None))}
    # The gold suite's base pin case builds the features plan and reads the feature schema, all read-only.
    checks["gold_suite_mounts"] = gold == {
        (str(ROOT / "scripts"), "/workspace/scripts", True),
        (str(ROOT / "orchestration"), "/workspace/orchestration", True),
        (str(ROOT / "config"), "/workspace/config", True),
        (str(ROOT / "data_contracts/lakehouse/gold_contract.json"), "/workspace/data_contracts/lakehouse/gold_contract.json", True),
        (str(ROOT / "data_contracts/lakehouse/features_publish.json"), "/workspace/data_contracts/lakehouse/features_publish.json", True),
        (str(ROOT / "data_contracts/features"), "/workspace/data_contracts/features", True),
        (str(ROOT / "data/e2e/gold_publish"), "/workspace/data/e2e/gold_publish", False),
    }
    return checks


def case_features_publish_prefix(fixture: Fixture, scratch: Path) -> Checks:
    """The features publish layer meets gold's prefix and contract admission: a run's declared prefixes and the checkout's contract must match its mode."""
    checks: Checks = {}
    step = {"layer": "features", "step": "candidates"}
    for mode, declared, wanted in (
        ("real", "lakehouse/silver/", "lakehouse/features/"),
        ("real", "lakehouse/features_e2e/", "lakehouse/features/"),
        ("publish_fixture", "lakehouse/features/", "lakehouse/features_e2e/"),
    ):
        run = {"domain": "transform", "mode": mode, "write_prefixes": [declared]}
        problem = launcher.Launcher.template_problem({"runs": {RUN: run}}, f"run:{RUN}", "pipeline_publish", step) or ""
        checks[f"refuses_{mode}_{declared}"] = f"writes under {wanted}; run {RUN} declares {declared}" in problem
    for mode, declared in (("real", "lakehouse/features/"), ("publish_fixture", "lakehouse/features_e2e/")):
        run = {"domain": "transform", "mode": mode, "write_prefixes": [declared]}
        checks[f"accepts_{mode}"] = launcher.Launcher.template_problem({"runs": {RUN: run}}, f"run:{RUN}", "pipeline_publish", step) is None
    gold = {"domain": "transform", "mode": "real", "write_prefixes": ["lakehouse/silver/"]}
    gold_step = {"layer": "gold", "step": "candidates"}
    checks["gold_still_refused"] = bool(launcher.Launcher.template_problem({"runs": {RUN: gold}}, f"run:{RUN}", "pipeline_publish", gold_step))
    checks["contract_real"] = launcher.publish_contract_problem("features", "real", ROOT) is None
    checks["contract_fixture"] = launcher.publish_contract_problem("features", "publish_fixture", ROOT) is None
    swapped = scratch / "checkout"
    contract = swapped / launcher.LAYER_CONTRACT.get("features", "data_contracts/lakehouse/features_publish.json")
    contract.parent.mkdir(parents=True)
    contract.write_text(json.dumps({"publish": {"namespaces": {"real": "silver", "publish_fixture": "features_e2e"}}}))
    checks["contract_mismatch_refused"] = "sends mode real to lakehouse/silver/" in (launcher.publish_contract_problem("features", "real", swapped) or "")
    return checks


def case_templates_launcher(fixture: Fixture, scratch: Path) -> Checks:
    """E01 (LB16): the launcher's closed table holds the feature templates with the transform domain and the publish modes."""
    checks: Checks = {}
    for name, service in (("pipeline_feast", "feature-store"), ("pipeline_validate_features", "quality")):
        template = launcher.TEMPLATES.get(name)
        checks[f"{name}_present"] = template is not None
        if template is not None:
            checks[f"{name}_shape"] = (
                template.service == service
                and template.owners == ("run",)
                and template.run_domains == ("transform",)
                and template.run_modes == ("publish_fixture", "real")
                and template.finite
            )
    source = (ROOT / "orchestration/launcher.py").read_text()
    checks["feast_renderer"] = '"pipeline_feast": self.render_pipeline,' in source
    checks["validate_renderer"] = re.search(r'for layer in \("silver", "gold", "cross", "features"\)\},', source) is not None
    checks["validate_mounts_schema"] = (
        '"features": (contract, schema)' in source and 'Mount(str(contracts / "features"), "/workspace/data_contracts/features", True)' in source
    )
    return checks


# ---------------------------------------------------------------------------------------------------------------------
# Broker approval (E05, regression R3)
# ---------------------------------------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------------------------------------
# Run control (E01 completed, E08 control seal) and templates (E01 templates)
# ---------------------------------------------------------------------------------------------------------------------


def selector_templates(node: Any) -> list[set[str]]:
    """The template values every ``if input_run_id then template`` rule of a schema allows."""
    found: list[set[str]] = []
    if isinstance(node, dict):
        condition = node.get("if", {})
        if isinstance(condition, dict) and condition.get("required") == ["input_run_id"]:
            template = node.get("then", {}).get("properties", {}).get("template", {})
            found.append(set(template.get("enum", [template.get("const")])))
        for value in node.values():
            found += selector_templates(value)
    elif isinstance(node, list):
        for value in node:
            found += selector_templates(value)
    return found


def case_selector_contracts(fixture: Fixture, scratch: Path) -> Checks:
    """E01 (LB25): a named producer run may select the input of pin_silver or pin_gold, of no other template, in every orchestration contract."""
    checks: Checks = {}
    validator = schema_validator("submission_request.schema.json")
    for template, allowed in (("pin_gold", True), ("pin_silver", True), ("read_gold", False), ("features_build", False)):
        request = SubmissionRequest(template, RUN, "publish_fixture", 1, FEATURES_DAG, template, "manual__fixture", GOLD_RUN)
        errors = list(validator.iter_errors(request.as_dict()))
        checks[f"request_{template}_{'accepted' if allowed else 'refused'}"] = (not errors) if allowed else bool(errors)
    contracts = ROOT / "data_contracts/orchestration"
    for name, rules in (("submission_request", 1), ("ledger_line", 3), ("snapshot_record", 1), ("input_snapshot_record", 1), ("dbt_lineage_record", 1)):
        found = selector_templates(json.loads((contracts / f"{name}.schema.json").read_text()))
        checks[f"{name}_selector_rules"] = len(found) == rules and all(item == {"pin_silver", "pin_gold"} for item in found)
    allowed_templates = set().union(*selector_templates(json.loads((contracts / "submission_request.schema.json").read_text())))
    checks["submission_request_allows_input_run_id_for_pin_silver_and_pin_gold_only"] = allowed_templates == {"pin_silver", "pin_gold"}
    return checks


# ---------------------------------------------------------------------------------------------------------------------
# Online store service (step 5, lane B: E20 and E21 launcher parts, E26 restart handling)
# ---------------------------------------------------------------------------------------------------------------------


def online_compose() -> dict[str, Any]:
    document: dict[str, Any] = importlib.import_module("yaml").safe_load((ROOT / online_store.COMPOSE).read_text())
    return document


def case_online_config(fixture: Fixture, scratch: Path) -> Checks:
    """E20 (LB30): the shipped redis.conf is exactly the closed directive set; persistence, eviction and extra directives are refused."""
    text = (ROOT / online_store.CONFIG).read_text()
    checks: Checks = {"shipped_config": online_store.check_config(text) == online_store.REQUIRED_CONFIG}
    variants = {
        "persistence_on": text.replace('save ""', "save 60 1"),
        "append_only": text.replace("appendonly no", "appendonly yes"),
        "eviction": text.replace("noeviction", "allkeys-lru"),
        "sixteen_databases": text.replace("databases 3", "databases 16"),
        "save_missing": text.replace('save ""\n', ""),
        "password_in_file": text + "requirepass not-here\n",
        "include": text + "include /tmp/other.conf\n",
        "module": text + "loadmodule /tmp/module.so\n",
        "twice": text + "appendonly no\n",
        "unprotected": text.replace("protected-mode yes", "protected-mode no"),
    }
    for name, variant in variants.items():
        checks[f"refuses_{name}"] = refused(functools.partial(online_store.check_config, variant), online_store.OnlineStoreError)
    return checks


def case_online_compose(fixture: Fixture, scratch: Path) -> Checks:
    """E20 (LB31, LB33): the online compose service is the closed shape, pinned by digest, with no port, password only in the environment."""
    document = online_compose()
    raw = document["services"].get(online_store.SERVICE, {})
    checks: Checks = {"only_service": set(document["services"]) == {online_store.SERVICE}}
    checks["shipped_shape"] = not refused(functools.partial(online_store.validate_service, raw), online_store.OnlineStoreError)
    checks["pinned_digest"] = launcher.PINNED_REFERENCE.fullmatch(online_store.IMAGE) is not None and "@sha256:" in raw.get("image", "")
    checks["no_password_in_command"] = all("REDIS_PASSWORD}" not in part for part in raw.get("entrypoint", []))
    variants: dict[str, Callable[[dict[str, Any]], None]] = {
        "tag_only": lambda item: item.update(image="redis:8.10.1"),
        "host_port": lambda item: item.update(ports=["127.0.0.1:6379:6379"]),
        "extra_env": lambda item: item["environment"].update(AWS_PROFILE="x"),
        "writable_conf": lambda item: item.update(volumes=[f"./redis.conf:{online_store.CONFIG_TARGET}"]),
        "extra_mount": lambda item: item["volumes"].append("./data:/data"),
        "no_cap_drop": lambda item: item.pop("cap_drop"),
        "other_network": lambda item: item.update(networks=["online", "frontend"]),
        "password_argument": lambda item: item.update(entrypoint=["redis-server", "--requirepass", "${REDIS_PASSWORD}"]),
        "root_user": lambda item: item.update(user="0:0"),
        "health_with_password_argument": lambda item: item.update(healthcheck={"test": ["CMD", "redis-cli", "-a", "${REDIS_PASSWORD}", "ping"]}),
    }
    for name, change in variants.items():
        variant = copy.deepcopy(raw)
        change(variant)
        checks[f"refuses_{name}"] = refused(functools.partial(online_store.validate_service, variant), online_store.OnlineStoreError)
    body: dict[str, Any] = {
        "Entrypoint": online_store.ENTRYPOINT,
        "Env": ["REDIS_PASSWORD=fake-value", "REDIS_MAXMEMORY=1"],
        "HostConfig": {"NetworkMode": online_store.network_name(REGISTRY_ID), "Mounts": [{"Target": online_store.CONFIG_TARGET, "ReadOnly": True}]},
    }
    checks["body_accepted"] = not refused(functools.partial(online_store.validate_body, body, REGISTRY_ID), online_store.OnlineStoreError)
    bad_bodies = {
        "port": {**body, "HostConfig": {**body["HostConfig"], "PortBindings": {"6379/tcp": [{"HostIp": "127.0.0.1", "HostPort": "6379"}]}}},
        "network": {**body, "HostConfig": {**body["HostConfig"], "NetworkMode": "bridge"}},
        "writable": {**body, "HostConfig": {**body["HostConfig"], "Mounts": [{"Target": online_store.CONFIG_TARGET, "ReadOnly": False}]}},
        "env": {**body, "Env": [*body["Env"], "AWS_PROFILE=x"]},
        "cmd": {**body, "Cmd": ["--requirepass", "fake-value"]},
    }
    for name, bad in bad_bodies.items():
        checks[f"body_refuses_{name}"] = refused(functools.partial(online_store.validate_body, bad, REGISTRY_ID), online_store.OnlineStoreError)
    return checks


def case_online_settings(fixture: Fixture, scratch: Path) -> Checks:
    """E21 (LB32): the memory settings are exactly two bounded integers with maxmemory at 75 % of the container limit."""
    good = online_store.limits(1024**3)
    checks: Checks = {"limits": good == {"HAI_ONLINE_MEM": str(1024**3), "HAI_ONLINE_MAXMEMORY": str(int(1024**3 * 0.75))}}
    checks["accepted"] = online_store.settings_for({**good, "JOB_THREADS": "1"}) == good
    for name, values in {
        "missing": {"HAI_ONLINE_MEM": good["HAI_ONLINE_MEM"]},
        "extra": {**good, "HAI_ONLINE_PORT": "6379"},
        "maxmemory_equal": {**good, "HAI_ONLINE_MAXMEMORY": good["HAI_ONLINE_MEM"]},
        "not_decimal": {**good, "HAI_ONLINE_MEM": "1g"},
        "below_floor": online_store.limits(online_store.MEMORY_FLOOR) | {"HAI_ONLINE_MEM": "1024", "HAI_ONLINE_MAXMEMORY": "768"},
    }.items():
        checks[f"refuses_{name}"] = refused(functools.partial(online_store.settings_for, values), online_store.OnlineStoreError)
    return checks


def case_online_password(fixture: Fixture, scratch: Path) -> Checks:
    """E20 (LB33, LB44, LB46): the test password lives only in memory, one per registry; its form is checked; the real name comes from the store module."""
    registry = scratch / "registry"
    other = scratch / "other_registry"
    checks: Checks = {"unknown_before_start": not online_store.has_test_password(registry)}
    first = online_store.test_password(registry)
    checks["generated"] = online_store.check_password(first) == first
    checks["stable"] = online_store.test_password(registry) == first and online_store.has_test_password(registry)
    checks["per_registry"] = online_store.test_password(other) != first
    checks["nothing_written"] = not registry.exists() and not any(path.is_file() and first.encode() in path.read_bytes() for path in scratch.rglob("*"))
    online_store.forget_test_password(registry)
    checks["forgotten"] = not online_store.has_test_password(registry)
    for label, value in (("space", "a" * 43 + " b"), ("quote", 'a"' * 22), ("newline", "a" * 43 + "\nrequirepass x"), ("short", "a" * 10), ("empty", "")):
        checks[f"refuses_{label}"] = refused(functools.partial(online_store.check_password, value), online_store.OnlineStoreError)
    checks["store_form_accepted"] = online_store.check_password("A" * 43) == "A" * 43
    checks["no_name_without_constant"] = online_store.secret_name(ModuleType("bare")) is None
    named = ModuleType("named")
    setattr(named, online_store.STORE_CONSTANT, "project/name-of-the-secret")
    checks["name_from_module"] = online_store.secret_name(named) == "project/name-of-the-secret"
    checks["no_file_api"] = not any(hasattr(online_store, name) for name in ("ensure_test_password", "read_test_password", "test_password_path"))
    return checks


def case_online_no_secret_on_disk(fixture: Fixture, scratch: Path) -> Checks:
    """E20 (LB44, LB45): resolving the password for Redis and Feast renders writes it nowhere under the run or registry folders, and no process
    argument of the Redis command or health check carries it."""
    registry = fixture.base / "registry"
    registry.mkdir(exist_ok=True)
    renderer: Any = object.__new__(launcher.Renderer)
    renderer.config = SimpleNamespace(registry=SimpleNamespace(kind="test", path=registry))
    renderer.fetched = set()
    checks: Checks = {"refused_before_start": bool(launcher_error(renderer.online_password))}
    password = online_store.test_password(registry)
    checks["resolved_from_memory"] = renderer.online_password() == password and password in renderer.fetched
    for mode in ("publish_fixture", "real"):
        for step in ("apply", "materialize"):
            spec = fixture.render(pipeline_render, "pipeline_feast", {"step": step}, mode)
            checks[f"{mode}_{step}_spec_has_no_value"] = password not in json.dumps(spec_dict(spec))
    found = [path for path in fixture.base.rglob("*") if path.is_file() and password.encode() in path.read_bytes()]
    checks["no_file_holds_password"] = not found
    document = online_compose()
    raw = document["services"][online_store.SERVICE]
    script = raw["entrypoint"][2]
    feed, _, server = script.partition("|")
    checks["stdin_config"] = server.split() == ["exec", "redis-server", online_store.CONFIG_TARGET, "-"]
    checks["printf_builtin_feeds_password"] = feed.strip().startswith("printf ") and "$REDIS_PASSWORD" in feed
    checks["no_password_argument_anywhere"] = "requirepass" not in server and "--requirepass" not in script and "-a " not in raw["healthcheck"]["test"][1]
    checks["health_check_uses_environment"] = raw["healthcheck"]["test"][1].startswith('REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli ')
    online_store.forget_test_password(registry)
    return checks


class StackLauncher:
    """In-process launcher pieces for the online store stack: a recorded spawn and a transaction on one synthetic state."""

    def __init__(self, fixture: Fixture, state: dict[str, Any]) -> None:
        self.server = in_process(fixture)
        self.state = state
        self.spawned: list[tuple[str, str]] = []
        server: Any = self.server
        server.registry_id = REGISTRY_ID
        server.inflight = set()
        server.timings = SimpleNamespace(health_wait=60.0)
        server.docker_ok = True
        server.docker = SimpleNamespace(remove_network=lambda network: None, create_network=lambda *arguments, **options: "net")
        server.status = "serving"
        server.spawn = lambda key, tag, function, *arguments: self.spawned.append((key, tag))

        @contextlib.contextmanager
        def tx(write: bool = True) -> Iterator[dict[str, Any]]:
            yield self.state

        server.tx = tx

    def key_of(self, tag: str) -> str:
        """The worker key of the last worker spawned for one message tag: the record scopes its own keys."""
        return next(key for key, spawned_tag in reversed(self.spawned) if spawned_tag == tag)

    def up(self) -> dict[str, Any]:
        result: dict[str, Any] = self.server.m_pipeline_online_store_up(ctx_of(self.state, "pipeline_online_store_up", {}))
        return result

    def advance(self) -> list[Callable[[], None]]:
        actions: list[Callable[[], None]] = []
        self.server.advance_online_store(self.state, actions)
        for action in actions:
            action()
        return actions


def stack_state(mode: str) -> dict[str, Any]:
    state = launcher_state(mode)
    state.update({"services": {}, "operations": {}, "containers": {}})
    return state


def case_online_stack(fixture: Fixture, scratch: Path) -> Checks:
    """E20, E26 launcher parts (LB36 to LB39): start from down, limits, network, service, health, up; a failed or ended service answers failed."""
    state = stack_state("publish_fixture")
    stack = StackLauncher(fixture, state)
    checks: Checks = {}
    first = stack.up()
    record = state["launcher"].get(online_store.STACK, {})
    checks["starts_from_down"] = first == {"phase": "starting"} and record.get("phase") == "starting" and record.get("step") == "limits"
    checks["limits_spawned"] = any(tag == "onlinelimits" for _, tag in stack.spawned)
    checks["repeat_while_starting"] = stack.up() == {"phase": "starting"} and [tag for _, tag in stack.spawned].count("onlinelimits") == 1
    checks["deadline_set"] = record.get("start_deadline", 0) > 0
    memory = 2 * 1024**3
    online_store.test_password(fixture.base / "registry")
    stack.server.on_onlinelimits(stack.key_of("onlinelimits"), "ok", {"memory": memory})
    checks["settings_recorded"] = record.get("settings") == online_store.limits(memory) and record.get("step") == "network"
    checks["memory_reserved"] = state["services"].get(online_store.SERVICE) == memory
    stack.advance()
    ops = [op for op in state["operations"].values() if op["kind"] == "online_network_create"]
    checks["network_op"] = len(ops) == 1 and ops[0]["network_name"] == online_store.network_name(REGISTRY_ID)
    checks["network_create_spawned"] = any(tag == "onlinenetcreate" for _, tag in stack.spawned)
    stack.server.on_onlinenetcreate(f"op:{ops[0]['op_id']}", "ok", "net-online-id")
    checks["network_recorded"] = record.get("network") == "net-online-id" and record.get("step") == "service"
    stack.advance()
    attempts = [a for a in state["attempts"].values() if a["owner"] == online_store.OWNER]
    checks["one_service_attempt"] = (
        len(attempts) == 1 and attempts[0]["template"] == online_store.TEMPLATE and attempts[0]["params"] == {"service": online_store.SERVICE}
    )
    checks["admissible_while_starting"] = stack.server.admissible(state, attempts[0])
    attempts[0].update({"phase": "started", "container_id": "redis-container"})
    stack.advance()
    checks["health_read"] = any(tag == "onlinehealth" for _, tag in stack.spawned)
    stack.server.on_onlinehealth(stack.key_of("onlinehealth"), "ok", "healthy")
    stack.advance()
    checks["up"] = record.get("phase") == "up"
    checks["up_answer"] = stack.up() == {"phase": "up", "network": online_store.network_name(REGISTRY_ID)}
    inputs = stack.server.pipeline_inputs(state, state["runs"][RUN])
    checks["inputs_expose_network"] = inputs["online_store"] == {"phase": "up", "network": online_store.network_name(REGISTRY_ID)}
    checks["status"] = stack.server.online_store_status(state)["phase"] == "up"
    attempts[0].update({"phase": "ended", "reason": "exited", "exit_code": 137})
    stack.advance()
    checks["ended_service_tears_down"] = record.get("phase") == "tearing_down" and "online-store ended" in record.get("error", "")
    answer = stack.up()
    checks["crash_answers_wait"] = answer.get("phase") == "tearing_down" and "error" not in answer
    checks["no_restart_while_tearing_down"] = len([a for a in state["attempts"].values() if a["owner"] == online_store.OWNER]) == 1
    stack.advance()
    checks["teardown_to_network"] = record.get("stop_step") == "network"
    stack.advance()
    removals = [op for op in state["operations"].values() if op["kind"] == "online_network_remove"]
    checks["network_removal"] = len(removals) == 1 and removals[0]["network"] == "net-online-id"
    stack.server.on_onlinenetremove(f"op:{removals[0]['op_id']}", "ok", None)
    stack.advance()
    checks["confirm_spawned"] = any(tag == "onlineconfirm" for _, tag in stack.spawned)
    stack.server.on_onlineconfirm(stack.key_of("onlineconfirm"), "ok", True)
    checks["down_after_confirm"] = state["launcher"][online_store.STACK]["phase"] == "down" and online_store.SERVICE not in state["services"]
    checks["repair_starts_fresh"] = stack.up() == {"phase": "starting"} and state["launcher"][online_store.STACK]["service"] is None
    lost = stack_state("publish_fixture")
    restarted = StackLauncher(fixture, lost)
    lost["launcher"][online_store.STACK] = {"phase": "up", "step": "done", "service": None, "network": "net-online-id", "error": ""}
    online_store.forget_test_password(fixture.base / "registry")
    restarted.advance()
    lost_record = lost["launcher"][online_store.STACK]
    checks["lost_password_tears_down"] = (
        lost_record["phase"] == "tearing_down" and lost_record.get("restartable") is True and "password" in lost_record["error"]
    )
    failed = stack_state("publish_fixture")
    broken = StackLauncher(fixture, failed)
    broken.up()
    broken.server.on_onlinelimits(broken.key_of("onlinelimits"), "error", RuntimeError("plan refused"))
    answer = broken.up()
    checks["failed_start_answers_error"] = answer.get("phase") == "failed" and "cannot start" in answer.get("error", "")
    released = launcher_state("publish_fixture")
    released["runs"][RUN]["state"] = "finalizing"
    released.update({"services": {}, "operations": {}, "containers": {}})
    finalizing = StackLauncher(fixture, released)
    checks["refused_for_unheld_run"] = bool(launcher_error(finalizing.up))
    expired = stack_state("publish_fixture")
    late = StackLauncher(fixture, expired)
    late.up()
    expired["launcher"][online_store.STACK]["start_deadline"] = 1.0
    late.advance()
    checks["start_deadline_fails"] = expired["launcher"][online_store.STACK].get("phase") == "tearing_down" and "600" in expired["launcher"][
        online_store.STACK
    ].get("error", "")
    checks["method_registered"] = launcher.METHODS["pipeline_online_store_up"].kinds == ("run",)
    checks["template_registered"] = launcher.TEMPLATES[online_store.TEMPLATE].owners == ("stack",)
    return checks


def case_online_join(fixture: Fixture, scratch: Path) -> Checks:
    """E20 (LB34, LB35): Feast jobs reach Redis only on the online network, by reference to the password; a down store refuses the render."""
    checks: Checks = {}
    for mode in ("publish_fixture", "real"):
        for step in ("apply", "materialize"):
            spec = fixture.render(pipeline_render, "pipeline_feast", {"step": step}, mode)
            online = spec.extra.get("online_store", {})
            checks[f"{mode}_{step}_password_reference"] = online.get("password_env") == "REDIS_PASSWORD" and "REDIS_PASSWORD" not in spec.environment
            if mode == "publish_fixture":
                checks[f"{mode}_{step}_only_online_network"] = spec.network == ONLINE_NETWORK and online.get("joins") == {}
            else:
                checks[f"{mode}_{step}_read_plus_online"] = spec.network == "net-read" and online.get("joins") == {ONLINE_NETWORK: []}
            down = fixture.pipeline(mode)
            down["online_store"] = {"phase": "starting", "network": ONLINE_NETWORK}
            checks[f"{mode}_{step}_refused_when_down"] = refused(
                functools.partial(fixture.render, pipeline_render, "pipeline_feast", {"step": step}, mode, down), pipeline_protocol.PipelineError
            )
    probe = fixture.render(pipeline_render, "pipeline_feast", {"step": "probe"}, "real")
    checks["probe_needs_no_online_store"] = "online_store" not in probe.extra
    for template, params in FEATURE_JOBS:
        if template != "pipeline_feast":
            spec = fixture.render(pipeline_render, template, params, "real")
            checks[f"{template}_{'_'.join(str(value) for value in params.values())}_no_online"] = "online_store" not in spec.extra
    source = (ROOT / "orchestration/launcher.py").read_text()
    checks["launcher_resolves_reference"] = "self.online_password()" in source and 'spec.extra.get("online_store")' in source
    return checks


# ---------------------------------------------------------------------------------------------------------------------
# Graph and DAG files (E01 graph, regression R4)
# ---------------------------------------------------------------------------------------------------------------------


def work_steps(dag: str) -> tuple[str, ...] | None:
    path = ROOT / "dags" / f"{dag}.py"
    if not path.is_file():
        return None
    tree = ast.parse(path.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "WORK_STEPS" for target in node.targets):
            value = ast.literal_eval(node.value)
            return tuple(value)
    return None


def case_graph(fixture: Fixture, scratch: Path) -> Checks:
    """E01 (LB24, LB25, LB26, LB27, regression R4): chains, DAG files, parameters, runtime routing and the supervisor's DAG list."""
    checks: Checks = {}
    base = base_module("graph_base", "orchestration/graph.py", scratch)
    triggers = {name: frozenset({"gate"}) for name in graph.TRIGGERS[BASE_DAG]}  # the base DAG starts the six model DAGs behind its gate
    checks[f"{BASE_DAG}_collapsed"] = (
        BASE_DAG in graph.COLLAPSED_DAGS
        and BASE_DAG not in graph.PIPELINE_CHAINS
        and graph.requirements(BASE_DAG)[0] == {**graph.COLLAPSED_UPSTREAM, **triggers}
    )
    checks[f"{BASE_DAG}_reference_valid"] = graph.validate_transform_graph(graph.reference_graph(BASE_DAG), BASE_DAG) == []
    # The loop compares the DAGs of the baseline graph that the current graph still has: a collapsed DAG must hold one task per job, and any other must keep
    # its chain. A baseline DAG that the current graph no longer has is not compared.
    for dag, chain in base.PIPELINE_CHAINS.items():
        if dag in graph.COLLAPSED_DAGS:  # one task per job now: preflight, job, evidence and gate
            checks[f"{dag}_collapsed"] = dag not in graph.PIPELINE_CHAINS and graph.requirements(dag)[0] == graph.COLLAPSED_UPSTREAM
        elif dag in graph.PIPELINE_CHAINS:
            checks[f"{dag}_unchanged"] = graph.PIPELINE_CHAINS[dag] == chain
    checks["transform_dag_unchanged"] = graph.requirements("transform_dag") == base.requirements("transform_dag")
    features_dag = (ROOT / "dags" / f"{BASE_DAG}.py").read_text() if (ROOT / "dags" / f"{BASE_DAG}.py").is_file() else ""
    checks["features_params"] = all(
        text in features_dag
        for text in (
            '"run_id": Param(type="string", pattern=f"^{BASE_RUN}$")',
            '"mode": Param("real", type="string", enum=["real"])',
            '"gold_snapshot_run": Param(type="string", pattern=f"^{run_id_pattern(\'gold\')}$")',
        )
    )
    runtime = (ROOT / "orchestration/dag_runtime.py").read_text()
    checks["runtime_gold_selection"] = (
        re.search(r'"pin_gold": "gold_snapshot_run"', runtime) is not None and re.search(r'"pin_silver": "silver_snapshot_run"', runtime) is not None
    )
    gold_dag = (ROOT / "dags/gold_transform_dag.py").read_text()
    checks["runtime_assets"] = (
        re.search(r'ASSETS = \{"features_commit": "features"\}', runtime) is not None
        and 'build_job_chain("pin_silver", asset="gold")' in gold_dag
        and 'build_job_chain("pin_gold", asset="features")' in features_dag
    )
    return checks


# ---------------------------------------------------------------------------------------------------------------------


CASES: dict[str, Callable[..., Checks]] = {
    "R1_render_regression": case_render_regression,
    "E01_render_build": case_render_build,
    "E01_render_modules": case_render_modules,
    "E01_render_publish": case_render_publish,
    "E01_render_writer": case_render_writer,
    "E01_render_spark": case_render_spark,
    "E01_render_feast": case_render_feast,
    "E05_fixture_isolation": case_fixture_isolation,
    "E08_seal": case_seal,
    "E31_pin_gold": case_pin_gold,
    "E31_launcher_gold": case_launcher_gold,
    "E08_launcher_seal": case_launcher_seal,
    "E01_templates_launcher": case_templates_launcher,
    "E01_graph": case_graph,
    "E01_selector_contracts": case_selector_contracts,
    "E01_compose_service": case_compose_service,
    "E20_online_config": case_online_config,
    "E20_online_compose": case_online_compose,
    "E21_online_settings": case_online_settings,
    "E20_online_password": case_online_password,
    "E20_online_stack": case_online_stack,
    "E20_online_join": case_online_join,
    "E20_online_no_secret_on_disk": case_online_no_secret_on_disk,
    "E06_features_publish_job": case_features_publish_job,
    "E06_features_publish_prefix": case_features_publish_prefix,
}


@contextlib.contextmanager
def deadline(seconds: int) -> Iterator[None]:
    """End the whole run with an error when it passes ``seconds``."""

    def expire(signum: int, frame: Any) -> None:
        raise RunnerDeadline(f"the light feature-store runner passed its {seconds} second deadline")

    previous = signal.signal(signal.SIGALRM, expire)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the selected cases and write the report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=sorted(CASES))
    options = parser.parse_args(argv)
    selected = options.case or list(CASES)
    results: list[dict[str, Any]] = []
    with deadline(RUN_SECONDS), tempfile.TemporaryDirectory(prefix="features_light_") as temporary:
        for name in selected:
            scratch = Path(temporary) / name
            scratch.mkdir()
            fixture = Fixture(scratch / "fixture")
            try:
                checks = CASES[name](fixture, scratch)
                error = ""
            except Exception as failure:
                # A case that raises is reported as failed with its error type and text; the run goes on to the next case.
                checks, error = {}, f"{type(failure).__name__}: {failure}"
            failed = sorted(check for check, ok in checks.items() if not ok)
            results.append(
                {"case": name, "verdict": "pass" if checks and not failed and not error else "fail", "failed": failed, "error": error, "checks": len(checks)}
            )
    folder = ROOT / "data/e2e/features_pipeline_light" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    folder.mkdir(parents=True)
    report: dict[str, Any] = {"scope": "light: renders, protocol, in-process launcher methods, graphs"}
    report["base_revision"] = BASE_REVISION
    report["cases"] = results
    (folder / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    failed_cases = [item["case"] for item in results if item["verdict"] != "pass"]
    sys.stdout.write(json.dumps({"report": str(folder / "report.json"), "passed": len(results) - len(failed_cases), "failed": failed_cases}) + "\n")
    return int(bool(failed_cases))


if __name__ == "__main__":
    sys.exit(main())
