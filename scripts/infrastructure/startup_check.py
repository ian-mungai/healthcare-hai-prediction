"""Startup check of a host job: find a broken environment in seconds and name the cause in the first log lines.

A job on the EC2 host or on Fargate runs this at the start of its first code step, from the staged code. It checks only what the job needs, each check with a
fixed bound, and logs one line per check on stderr. Failures come first. A run outside an ECS task checks nothing.

- ``check_environment``: the task environment is recognised, the task credential source is the native provider, the host services the job needs accept a TCP
  connect at their configured addresses, and S3 and ECR accept a connect on 443. The connects run side by side and share one bound.
- ``check_build``: the dbt target that the build selects (``pipeline_build.build_target``) and, for a profile that lists DuckDB extensions, that its extension
  directory exists and holds each of them, so dbt downloads nothing.
- ``check_files``: every repository-relative file that the job will read exists in the task. ``queue_files`` derives them from the collector plans of the
  modes that a publisher queue runs.

The check opens a socket and reads files. It makes no request that needs a grant.
"""

from __future__ import annotations

import json
import os
import socket
import sys
import threading
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from orchestration.pipeline_protocol import PipelineError
from scripts.lakehouse import ecs_engine

# One bound for all connects of a check. They run side by side, so a check adds at most this and a little more.
CONNECT_SECONDS = 3.0
# ECS gives these to every container of a task, on Fargate and on the host.
ECS_MARKERS = ("ECS_CONTAINER_METADATA_URI_V4", "ECS_CONTAINER_METADATA_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
DEFAULT_PORTS = {"http": 80, "https": 443}


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


def check_environment(job: str, *, polaris: bool = False, mlflow: bool = False) -> None:
    """The first-step check: the task environment and credentials, the host services that the job needs, and S3 and ECR."""
    if not in_ecs_job():
        return
    started = time.monotonic()
    environment = check_task_environment()
    findings = [environment]
    if environment.ok:
        findings.append(check_credentials(catalog=polaris))
    targets: list[tuple[str, str, int]] = []
    for needed, name, variable in ((polaris, "polaris", "POLARIS_URI"), (mlflow, "mlflow", "MLFLOW_TRACKING_URI")):
        if needed:
            target = service_target(name, variable)
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
    import yaml

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
    from scripts.lakehouse import pipeline_build

    findings = [check_task_environment()]
    if layer != "features":
        try:
            target = pipeline_build.build_target(layer, mode)
        except PipelineError as error:
            findings.append(Finding("dbt_target_refused", False, str(error)))
        else:
            findings.append(check_target(target, profiles))
    report(layer, "build", findings, started)


def read_json(path: str | Path) -> Mapping | None:
    try:
        document = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return document if isinstance(document, dict) else None


def mmd_files() -> list[str]:
    """The files that ``mmd_api_contract.load_plan`` and ``collect_mmd_api.task_inputs`` read, derived from the contract's plan list and the plans."""
    from scripts.acquisition import collect_mmd_api, mmd_api_contract
    from scripts.acquisition.data_paths import current
    from scripts.acquisition.source_registry import canonical_hash, load_registry, registry_version_paths

    names: list[str] = []
    for sha, path in mmd_api_contract.PLANS.items():
        names.append(str(path))
        plan = read_json(path)
        if plan is None:
            continue
        report_path = str(current(plan.get("comparison_report", "")))
        names.append(report_path)
        names.extend(str(current(reference["path"])) for reference in (plan.get("references") or {}).values())
        comparison = read_json(report_path)
        names.extend(str(current(item["original_path"])) for item in (comparison or {}).get("years", []))
        if sha == mmd_api_contract.ADDITIONS_PLAN_SHA256:
            lock = mmd_api_contract.ADDITIONS_LOCK_PATH
            try:
                if plan.get("registry_sha256") != canonical_hash(load_registry()):
                    lock = registry_version_paths(plan["registry_sha256"])["additions_lock"]
            except (OSError, ValueError, KeyError):
                pass
            names.append(str(lock))
    names.append(str(collect_mmd_api.TEMPLATE))
    return names


# The file lists the publisher owns, by collector mode: the files that a mode's locked plans read while they load.
MODE_FILES = {"mmd_api": mmd_files}


def queue_files(document: Mapping) -> list[tuple[str, str]]:
    """The (mode, path) of every file that the modes of a publisher queue read, by the same handler rule that the stored mirror uses to carry them."""
    from scripts.acquisition import redownload, stored_mirror

    units = [unit for unit in document.get("queue", []) if isinstance(unit, dict) and unit.get("disposition") in stored_mirror.ACTIVE]
    modes = sorted({redownload.handler_mode({"flags": [], **unit}) for unit in units if "mode" in unit})
    return [(mode, path) for mode in modes if mode in MODE_FILES for path in MODE_FILES[mode]()]


def check_files(job: str, files: Iterable[tuple[str, str]], root: Path | None = None) -> None:
    """Every named file exists. A path that is not absolute is read from ``root`` (the working folder), where the job's loaders read it."""
    if not in_ecs_job():
        return
    started = time.monotonic()
    base = Path.cwd() if root is None else root
    findings = []
    for owner, name in files:
        found = (base / name).is_file()
        findings.append(Finding("input_file" if found else "input_file_missing", found, f"{owner}: {name}"))
    report(job, "files", findings or [Finding("input_files", True, "none named")], started)
