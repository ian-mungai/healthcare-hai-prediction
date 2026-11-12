"""Startup check of a host job: find a broken environment in seconds and name the cause in the first log lines.

A job on the EC2 host or on Fargate runs this at the start of its first code step, from the staged code. It checks only what the job needs, each check with a
fixed bound, and logs one line per check on stderr. Failures come first. A run outside an ECS task checks nothing.

- ``check_phase`` and ``check_environment``: the task environment is recognised, the task credential source is the native provider, the host services the
  job needs (Polaris, MLflow, OpenMetadata, Redis) accept a TCP connect at their configured addresses, and S3 and ECR accept a connect on 443. The connects run
  side by side and share one bound. ``JOB_PHASES`` holds, for every host job, the phases that check and the services each needs.
- ``check_build``: the dbt target that the build selects (``pipeline_build.build_target``) and, for a profile that lists DuckDB extensions, that its extension
  directory exists and holds each of them, so dbt downloads nothing.
- ``check_files``: every repository-relative file that the job will read exists in the task. ``queue_files`` derives them from the collector plans of the
  modes that a publisher queue runs.

The check opens a socket and reads files. It makes no request that needs a grant.
"""

from __future__ import annotations

import importlib
import json
import os
import socket
import sys
import threading
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from urllib.parse import urlsplit

from scripts.lakehouse import ecs_engine

# One bound for all connects of a check. They run side by side, so a check adds at most this and a little more.
CONNECT_SECONDS = 3.0
# ECS gives these to every container of a task, on Fargate and on the host.
ECS_MARKERS = ("ECS_CONTAINER_METADATA_URI_V4", "ECS_CONTAINER_METADATA_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
DEFAULT_PORTS = {"http": 80, "https": 443}
# The host jobs of config/orchestration/host_jobs.json, the phase at which each checks and the host services that phase needs. A phase that is not listed
# checks nothing. The first code step of a job is its first listed phase; the layer jobs check their build separately (``check_build``).
JOB_PHASES: dict[str, dict[str, tuple[str, ...]]] = {
    "silver": {"pin": ("polaris",)},
    "gold": {"pin": ("polaris",)},
    "features": {"pin": ("polaris",), "feast": ("polaris", "redis")},
    "bronze": {"pin": (), "load": ("polaris",)},
    "train": {"prepare": (), "extract": ("polaris",), "train": ("mlflow",)},
    "promote": {"promote": ("mlflow",)},
    "drift": {"baseline": ("mlflow",), "current": ("polaris",)},
    "serve": {"serve": ("mlflow",)},
    "metadata": {"ingest": ("openmetadata",)},
    "publisher": {"run": ()},
}
# The variable that holds each service address. Redis runs on the host of POLARIS_URI, as scripts.features.feast_aws derives it.
SERVICE_VARIABLES = {"polaris": "POLARIS_URI", "mlflow": "MLFLOW_TRACKING_URI", "openmetadata": "OM_API"}


class StartupCheckError(RuntimeError):
    """One or more startup checks failed. The message names each cause."""


@dataclass(frozen=True)
class Finding:
    name: str
    ok: bool
    detail: str


def open_connection(host: str, port: int, timeout: float) -> None:
    """Connect to a TCP port and close at once. The tests replace this seam."""
    socket.create_connection((host, port), timeout=timeout).close()


def in_ecs_job() -> bool:
    return ecs_engine.in_task() or any(os.environ.get(name) for name in ECS_MARKERS)


def check_task_environment() -> Finding:
    value = os.environ.get("AWS_EXECUTION_ENV", "")
    if ecs_engine.in_task():
        return Finding("task_environment", True, value)
    expected = " or ".join(sorted(ecs_engine.TASK_ENVIRONMENTS))
    return Finding(
        "task_environment_unrecognised", False, f"AWS_EXECUTION_ENV is '{value}' in an ECS task, so the job would not use task settings; expected {expected}"
    )


def check_credentials(catalog: bool) -> Finding:
    try:
        ecs_engine.validate_task_credentials(require_catalog=catalog)
    except RuntimeError as error:
        return Finding("credentials", False, str(error))
    return Finding("credentials", True, "native task provider")


def service_target(name: str, variable: str) -> tuple[str, str, int] | Finding:
    """The host and port of a service address in the environment, or the finding that says why it has none."""
    text = os.environ.get(variable, "")
    try:
        address = urlsplit(text)
        host, port = address.hostname, address.port or DEFAULT_PORTS.get(address.scheme)
    except ValueError:
        host, port = None, None
    if not host or not port:
        return Finding(f"{name}_address_unusable", False, f"{variable} is '{text}'")
    return name, host, port


def probe(targets: list[tuple[str, str, int]]) -> list[Finding]:
    """Connect to every target at the same time. A connect that is not done at the shared bound fails as timed out, and no thread outlives the process."""
    outcomes: dict[int, tuple[Exception | None, float]] = {}

    def attempt(index: int, host: str, port: int) -> None:
        started = time.monotonic()
        try:
            open_connection(host, port, CONNECT_SECONDS)
        except OSError as error:
            outcomes[index] = (error, time.monotonic() - started)
        else:
            outcomes[index] = (None, time.monotonic() - started)

    threads = [threading.Thread(target=attempt, args=(index, host, port), daemon=True) for index, (_, host, port) in enumerate(targets)]
    for thread in threads:
        thread.start()
    deadline = time.monotonic() + CONNECT_SECONDS
    for thread in threads:
        thread.join(max(0.0, deadline - time.monotonic()))
    findings = []
    for index, (name, host, port) in enumerate(targets):
        outcome = outcomes.get(index)
        if outcome is None:
            findings.append(Finding(f"{name}_unreachable", False, f"{host}:{port}: timed out after {CONNECT_SECONDS:g} s"))
        elif outcome[0] is not None:
            findings.append(Finding(f"{name}_unreachable", False, f"{host}:{port}: {type(outcome[0]).__name__}: {outcome[0]}"))
        else:
            findings.append(Finding(name, True, f"{host}:{port} {outcome[1]:.2f} s"))
    return findings


def endpoint_targets() -> list[tuple[str, str, int]] | Finding:
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", "")
    if not region:
        return Finding("region_unset", False, "neither AWS_REGION nor AWS_DEFAULT_REGION is set")
    return [("s3", f"s3.{region}.amazonaws.com", 443), ("ecr", f"api.ecr.{region}.amazonaws.com", 443), ("ecr", f"dkr.ecr.{region}.amazonaws.com", 443)]


def report(job: str, label: str, findings: list[Finding], started: float) -> None:
    failed = [finding for finding in findings if not finding.ok]
    for finding in failed:
        sys.stderr.write(f"startup_check: FAILED {finding.name} {finding.detail}\n")
    for finding in findings:
        if finding.ok:
            sys.stderr.write(f"startup_check: ok {finding.name} {finding.detail}\n")
    sys.stderr.write(f"startup_check: {job} {label}: {len(findings)} checks, {len(failed)} failed, {time.monotonic() - started:.1f} s\n")
    sys.stderr.flush()
    if failed:
        raise StartupCheckError("startup check failed: " + "; ".join(f"{finding.name} {finding.detail}" for finding in failed))


def redis_target() -> tuple[str, str, int] | Finding:
    online_protocol = importlib.import_module("scripts.features.online_protocol")
    target = service_target("redis", "POLARIS_URI")
    return Finding("redis_address_unusable", False, target.detail) if isinstance(target, Finding) else ("redis", target[1], online_protocol.PORT)


def check_phase(job: str, phase: str) -> None:
    """Check the environment for one phase of a host job, with the services that ``JOB_PHASES`` lists for it. An unlisted phase checks nothing."""
    needs = JOB_PHASES[job].get(phase)
    if needs is not None:
        check_environment(job, polaris="polaris" in needs, mlflow="mlflow" in needs, openmetadata="openmetadata" in needs, redis="redis" in needs)


def check_environment(job: str, *, polaris: bool = False, mlflow: bool = False, openmetadata: bool = False, redis: bool = False) -> None:
    """The first-step check: the task environment and credentials, the host services that the job needs, and S3 and ECR."""
    if not in_ecs_job():
        return
    started = time.monotonic()
    environment = check_task_environment()
    findings = [environment]
    if environment.ok:
        findings.append(check_credentials(catalog=polaris))
    targets: list[tuple[str, str, int]] = []
    for needed, name in ((polaris, "polaris"), (mlflow, "mlflow"), (openmetadata, "openmetadata")):
        if needed:
            target = service_target(name, SERVICE_VARIABLES[name])
            if isinstance(target, Finding):
                findings.append(target)
            else:
                targets.append(target)
    if redis:
        target = redis_target()
        if isinstance(target, Finding):
            findings.append(target)
        else:
            targets.append(target)
    endpoints = endpoint_targets()
    if isinstance(endpoints, Finding):
        findings.append(endpoints)
    else:
        targets.extend(endpoints)
    findings.extend(probe(targets))
    report(job, "environment", findings, started)


def profile_output(profiles: Path, target: str) -> Mapping | None:
    yaml = importlib.import_module("yaml")
    document = yaml.safe_load(profiles.read_text(encoding="utf-8"))
    for profile in document.values():
        if isinstance(profile, dict) and target in (profile.get("outputs") or {}):
            return profile["outputs"][target]
    return None


def check_target(target: str, profiles: Path) -> Finding:
    """A target that lists DuckDB extensions must name an extension directory that holds them, because a job has no egress to download one."""
    try:
        output = profile_output(profiles, target)
    except (OSError, ValueError, AttributeError) as error:
        return Finding("dbt_profile_unreadable", False, f"{profiles}: {error}")
    if output is None:
        return Finding("dbt_target_unknown", False, f"{target} is not an output of {profiles}")
    extensions = list(output.get("extensions") or [])
    if not extensions:
        return Finding("dbt_target", True, f"{target} (no DuckDB extensions)")
    directory = (output.get("config_options") or {}).get("extension_directory")
    if not directory:
        return Finding(
            "dbt_target_downloads_extensions",
            False,
            f"{target} lists {', '.join(extensions)} and names no extension directory, so dbt would download them and the job has no egress",
        )
    if not Path(directory).is_dir():
        return Finding("dbt_extension_directory_missing", False, f"{target}: {directory} does not exist in this image")
    missing = [name for name in extensions if not any(Path(directory).rglob(f"{name}.duckdb_extension"))]
    if missing:
        return Finding("dbt_extension_missing", False, f"{target}: {', '.join(missing)} not under {directory}")
    return Finding("dbt_target", True, f"{target} (extensions {', '.join(extensions)} in {directory})")


def check_build(layer: str, mode: str, profiles: Path) -> None:
    """The check at the start of the build: the target it would select and what that target needs on disk. A features build writes its own profile."""
    if not in_ecs_job():
        return
    started = time.monotonic()
    pipeline_error = importlib.import_module("orchestration.pipeline_protocol").PipelineError
    pipeline_build = importlib.import_module("scripts.lakehouse.pipeline_build")

    findings = [check_task_environment()]
    if layer != "features":
        try:
            target = pipeline_build.build_target(layer, mode)
        except pipeline_error as error:
            findings.append(Finding("dbt_target_refused", False, str(error)))
        else:
            findings.append(check_target(target, profiles))
    report(layer, "build", findings, started)


@dataclass(frozen=True)
class Need:
    """One file that a collector mode reads: the mode, the root that its reader resolves the path from (``code`` or ``mirror``) and the path.

    A ``nonempty`` need is a JSON listing that must hold at least one entry."""

    owner: str
    root: str
    path: str
    nonempty: bool = False


def read_json(path: str | Path) -> Mapping | None:
    try:
        document = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return document if isinstance(document, dict) else None


def legacy_registry_files(owner: str, registry_sha256: object) -> list[Need]:
    """The legacy registry files at the code root that a plan bound to an older registry loads, from the committed version catalog."""
    registry = importlib.import_module("scripts.acquisition.source_registry")
    catalog = read_json(registry.VERSION_CATALOG_PATH) or {}
    if not registry_sha256 or registry_sha256 == catalog.get("current_registry_sha256"):
        return []
    versions = [item for item in catalog.get("legacy_versions", []) if item.get("registry_sha256") == registry_sha256]
    return [Need(owner, "code", entry["path"]) for version in versions for entry in version.get("files", {}).values()]


def plan_needs(owner: str, plan_path: str | Path) -> list[Need]:
    """The code root files that one locked plan reads while it loads: its legacy registry and the references it lists."""
    current = importlib.import_module("scripts.acquisition.data_paths").current
    plan = read_json(plan_path)
    if plan is None:
        return []
    references = plan.get("references") or []
    references = list(references.values()) if isinstance(references, dict) else references
    return [*legacy_registry_files(owner, plan.get("registry_sha256")), *(Need(owner, "code", str(current(item["path"]))) for item in references)]


def mmd_needs() -> list[Need]:
    """The code root files that ``mmd_api_contract.load_plan`` and ``collect_mmd_api.task_inputs`` read, from the contract's plan list and the plans."""
    collect_mmd_api = importlib.import_module("scripts.acquisition.collect_mmd_api")
    contract = importlib.import_module("scripts.acquisition.mmd_api_contract")
    current = importlib.import_module("scripts.acquisition.data_paths").current
    needs: list[Need] = []
    for sha, path in contract.PLANS.items():
        needs.append(Need("mmd_api", "code", str(path)))
        plan = read_json(path)
        if plan is None:
            continue
        needs.extend(plan_needs("mmd_api", path))
        report = str(current(plan.get("comparison_report", "")))
        needs.append(Need("mmd_api", "code", report))
        needs.extend(Need("mmd_api", "code", str(current(item["original_path"]))) for item in (read_json(report) or {}).get("years", []))
        if sha == contract.ADDITIONS_PLAN_SHA256 and not legacy_registry_files("mmd_api", plan.get("registry_sha256")):
            needs.append(Need("mmd_api", "code", str(contract.ADDITIONS_LOCK_PATH)))
    needs.append(Need("mmd_api", "code", str(collect_mmd_api.TEMPLATE)))
    return needs


def cms_plans() -> list[Path]:
    return [importlib.import_module("scripts.acquisition.cms_owners_contract").PLAN_PATH]


def onc_plans() -> list[Path]:
    return [importlib.import_module("scripts.acquisition.onc_mu_contract").PLAN_PATH]


def bls_plans() -> list[Path]:
    return [importlib.import_module("scripts.acquisition.bls_api_contract").PLAN_PATH]


def hud_plans() -> list[Path]:
    contract = importlib.import_module("scripts.acquisition.hud_api_contract")
    return [contract.PLAN_PATH, contract.RANGE_PLAN_PATH]


# The locked plans of each plan-based collector, from the plan paths its contract holds.
PLAN_PATHS = {"cms_owners_org": cms_plans, "onc_mu_hospital": onc_plans, "bls_api_v2": bls_plans, "hud_api": hud_plans}


def contract_needs(mode: str) -> list[Need]:
    return [need for path in PLAN_PATHS[mode]() for need in plan_needs(mode, path)]


# The file lists the publisher owns, by collector mode: what the mode's locked plans read at the code root while they load.
MODE_FILES = {"mmd_api": mmd_needs, **{mode: partial(contract_needs, mode) for mode in PLAN_PATHS}}
# Modes that re-hash every operational input of the controls from the mirror root before they plan (``redownload.planned``).
MIRROR_CONFIG_MODES = frozenset(PLAN_PATHS)
CONFIG_PREFIX = "config/acquisition/"


def mirror_config_needs(mode: str, controls: Path) -> list[Need]:
    """The operational inputs under ``config/acquisition/`` that the controls bind, read from the mirror root, or the controls file if unreadable."""
    document = read_json(controls)
    if document is None:
        return [Need(mode, "mirror", str(controls))]
    return [Need(mode, "mirror", key) for key in sorted(document.get("operational_sha256", {})) if key.startswith(CONFIG_PREFIX)]


def queue_files(document: Mapping, *, controls: Path | None = None, keys: bool = False) -> list[Need]:
    """Every file that the modes this task attempts read, by the lane and handler rule of the harness, and the plan listing that the plan modes need.

    A keyed mode counts only when the task has keys. ``controls`` is the controls file beside the queue."""
    redownload = importlib.import_module("scripts.acquisition.redownload")
    stored_mirror = importlib.import_module("scripts.acquisition.stored_mirror")
    units = [
        {"flags": [], **item}
        for item in document.get("queue", [])
        if isinstance(item, dict) and item.get("disposition") in stored_mirror.ACTIVE and "mode" in item
    ]
    lanes = {redownload.TASK, *([redownload.TASK_KEYED] if keys else [])}
    modes = sorted({redownload.handler_mode(item) for item in units if redownload.lane_of(item) in lanes})
    needs = [need for mode in modes if mode in MODE_FILES for need in MODE_FILES[mode]()]
    for mode in modes:
        if mode in MIRROR_CONFIG_MODES and controls is not None:
            needs.extend(mirror_config_needs(mode, controls))
    planned = [mode for mode in modes if mode in stored_mirror.PLAN_MODES]
    if planned:
        needs.append(Need("+".join(planned), "mirror", stored_mirror.PLAN_INPUTS_NAME, nonempty=True))
    return list(dict.fromkeys(needs))


def check_need(need: Need, base: Path) -> Finding:
    path = base / need.path
    label = f"{need.owner} {need.root} root: {need.path}"
    if not need.nonempty:
        return Finding("input_file", True, label) if path.is_file() else Finding("input_file_missing", False, label)
    if not path.is_file():
        return Finding("plan_inputs_missing", False, f"{label}: the staged listing of plan input files is absent, so no plan file reaches the code root")
    document = read_json(path)
    if not document:
        return Finding("plan_inputs_empty", False, f"{label}: the staged listing of plan input files holds no entry, so no plan file reaches the code root")
    return Finding("plan_inputs", True, f"{label} ({len(document)} files)")


def check_files(job: str, files: Iterable[Need], roots: Mapping[str, Path] | None = None) -> None:
    """Every named file exists under the root that its reader uses (``code`` defaults to the working folder). A listing must hold an entry."""
    if not in_ecs_job():
        return
    started = time.monotonic()
    bases = {"code": Path.cwd(), **(roots or {})}
    findings = [check_need(need, bases[need.root]) for need in files]
    report(job, "files", findings or [Finding("input_files", True, "none named")], started)
