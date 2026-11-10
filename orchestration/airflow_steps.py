"""Run a DAG step through the direct ECS launch library.

``orchestration/dag_runtime.py`` calls ``launch_step`` for a step of a shipped DAG. A job step passes the preflight (``orchestration/ecs_preflight.py``), then
the launch operator runs the job: the first step of a job launches its one ECS task and waits, and every later step finds the recorded outcome of that
launch. A failed job fails each step, and trying again needs a new run. A control step (one with no job: acquire, seal, release, the gates) and the chain
gate launch nothing: the gate binds the code runs of the chain. Signing the first publishes is the owner's manual step, not a gate here. A staged input
that is not there yet (``InputNotReady``) is retried for ``UNAVAILABLE_SECONDS``, then the step fails with the cause.
A ``LaunchUnavailable`` from the launch library means the launch is not finished: the step fails with it at once and nothing repeats it, because only
the library's own evidence or its manual owner path resolves the launch.
A one-task-per-job DAG (``dag_runtime.build_job_chain``) calls ``preflight_step`` (the gates, no launch), ``job_step`` (the launch) and ``evidence_step``
(the launch's end record says the job passed).
The owner ends an unresolved launch with ``python -m orchestration.airflow_steps settle`` in the scheduler container (``Launcher.owner_settle``).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from functools import partial
from pathlib import Path
from typing import Any

from orchestration.ecs_adapter import HOST, EcsAdapter, EcsSettings, client_config
from orchestration.ecs_launch import Clock, EcsFitProbe, JobSpec, Launcher, LaunchRefused, S3LaunchStore, launch_record_prefix
from orchestration.ecs_preflight import (
    GATE,
    TABLE_MAP_FILE,
    InputNotReady,
    Preflight,
    PreflightSettings,
    S3CodeRuns,
    bronze_request_reader,
    load_tables,
    s3_digest_reader,
    s3_object_reader,
    serving_request_reader,
)
from orchestration.interface import SubmissionRequest

UNAVAILABLE_SECONDS = 600.0
UNAVAILABLE_POLL_SECONDS = 15.0
ROOT = Path(__file__).resolve().parents[1]
SETTINGS = (
    "AWS_REGION",
    "HAI_LAUNCH_CLUSTER",
    "HAI_LAUNCH_FAMILY_PREFIX",
    "HAI_LAUNCH_PROJECT_TAG",
    "HAI_LAUNCH_LOG_GROUP",
    "HAI_LAUNCH_HOST_PROVIDER",
    "HAI_LAUNCH_BUCKET",
    "HAI_LAUNCH_POLARIS_URI",
    "HAI_LAUNCH_MLFLOW_URI",
    "HAI_LAUNCH_SCRATCH_PATH",
)


@dataclass(frozen=True)
class LaunchServices:
    """What a step needs to launch a job: the preflight, the launcher, the operator class, the cluster and the clock the retries wait on."""

    preflight: Preflight
    launcher: Launcher
    operator_class: Callable[..., Any]
    cluster: str
    definition: Callable[[str], str]
    clock: Clock


def services() -> LaunchServices:
    """The launch services of the Airflow image, from the ``HAI_LAUNCH_*`` settings of its environment. No cloud call is made here."""
    values = {name: os.environ.get(name, "") for name in SETTINGS}
    for name, value in values.items():
        if not value:
            raise LaunchRefused(f"{name} is not set in the Airflow service environment")
    import boto3

    from orchestration.ecs_launch_provider import BrokerlessEcsRunTaskOperator

    table, host = load_tables(ROOT)
    session = boto3.session.Session(region_name=values["AWS_REGION"])
    s3 = session.client("s3", config=client_config())
    adapter = EcsAdapter.from_session(
        session,
        EcsSettings(
            cluster=values["HAI_LAUNCH_CLUSTER"],
            family_prefix=values["HAI_LAUNCH_FAMILY_PREFIX"],
            subnets=(),
            networks={},
            project_tag=values["HAI_LAUNCH_PROJECT_TAG"],
            log_group=values["HAI_LAUNCH_LOG_GROUP"],
            families=frozenset(table["jobs"]),
            host_capacity_provider=values["HAI_LAUNCH_HOST_PROVIDER"],
        ),
    )
    scratch = Path(values["HAI_LAUNCH_SCRATCH_PATH"])
    clock = Clock()
    store = S3LaunchStore(s3, values["HAI_LAUNCH_BUCKET"], launch_record_prefix())
    launcher = Launcher(
        adapter,
        store,
        EcsFitProbe(adapter.ecs, values["HAI_LAUNCH_CLUSTER"], lambda: shutil.disk_usage(scratch).free),
        clock,
        scratch_usage=partial(scratch_usage, scratch, sorted(table["jobs"])),
    )
    bucket = values["HAI_LAUNCH_BUCKET"]
    collections = {entry["table"]: entry["collection"] for entry in json.loads((ROOT / TABLE_MAP_FILE).read_text())["tables"]}
    preflight = Preflight(
        table=table,
        host=host,
        settings=PreflightSettings(polaris_uri=values["HAI_LAUNCH_POLARIS_URI"], mlflow_uri=values["HAI_LAUNCH_MLFLOW_URI"]),
        code_digest=s3_digest_reader(s3, bucket),
        approvals=s3_object_reader(s3, bucket, "promotion_approval.json"),
        store=store,
        code_runs=S3CodeRuns(s3, bucket),
        clock=clock.wall,
        bronze_request=bronze_request_reader(s3, bucket, collections),
        serving_request=serving_request_reader(s3, bucket),
    )
    return LaunchServices(preflight, launcher, BrokerlessEcsRunTaskOperator, values["HAI_LAUNCH_CLUSTER"], lambda job: adapter.definition(job, HOST), clock)


def folder_bytes(folder: Path) -> int:
    """The bytes the files below ``folder`` occupy on disk (allocated blocks), counting only what this process can read: a folder it cannot open counts as
    zero, and a link is never followed. So the credit never exceeds what the wipe of the folder frees."""
    total = 0
    for current, folders, files in os.walk(folder, onerror=lambda _: None, followlinks=False):
        for name in (*files, *folders):
            try:
                found = os.lstat(os.path.join(current, name))
            except OSError:
                continue
            if not os.path.isdir(os.path.join(current, name)) or os.path.islink(os.path.join(current, name)):
                total += found.st_blocks * 512
    return total


def scratch_usage(root: Path, families: Sequence[str]) -> dict[str, int]:
    """The bytes each family's scratch folder ``<root>/<family>`` holds, read through Airflow's read-only mount of the host scratch volume (owner
    decision A, Nov 7 2026). The launcher counts the launching family's own bytes as free and names the other families that hold any."""
    return {family: folder_bytes(root / family) if (root / family).is_dir() else 0 for family in families}


def selected_input(preflight: Preflight, request: SubmissionRequest, params: Mapping[str, Any], selections: Mapping[str, str]) -> str | None:
    """The producer run a job publishes from, named by a DAG parameter. Every step of the job gets it, not only the pin step, because the launch binds it."""
    job = preflight.steps.get(request.template)
    parameter = next((name for template, name in selections.items() if preflight.steps.get(template) == job), None) if job else None
    selected = params.get(parameter, "latest") if parameter else "latest"
    return None if selected == "latest" else str(selected)


def with_input(launch: LaunchServices, request: SubmissionRequest, context: Mapping[str, Any], selections: Mapping[str, str]) -> SubmissionRequest:
    """The request with the producer run its job publishes from."""
    return replace(request, input_run_id=selected_input(launch.preflight, request, context["params"], selections))


def prepared(launch: LaunchServices, request: SubmissionRequest) -> JobSpec | None:
    """The launch request that the preflight gates produce, or None for a step with no job. A staged input that is not there yet is retried within
    ``UNAVAILABLE_SECONDS``."""
    end = launch.clock.monotonic() + UNAVAILABLE_SECONDS
    while True:
        try:
            return launch.preflight.prepare(request)
        except InputNotReady as error:
            if launch.clock.monotonic() + UNAVAILABLE_POLL_SECONDS > end:
                raise InputNotReady(f"{error} (still not ready after {UNAVAILABLE_SECONDS:g} seconds)") from None
            launch.clock.sleep(UNAVAILABLE_POLL_SECONDS)


def launch(services_: LaunchServices, spec: JobSpec, context: Mapping[str, Any]) -> None:
    """Launch the job's one ECS task and wait for it, or join the recorded launch of the run and job."""
    services_.operator_class(
        task_id=context["ti"].task_id,
        job=spec,
        launcher=lambda: services_.launcher,
        task_definition=services_.definition(spec.job),
        cluster=services_.cluster,
        overrides={},
    ).execute(context)


def launch_step(request: SubmissionRequest, context: Mapping[str, Any], selections: Mapping[str, str]) -> str:
    """Run the step through the launch library and return its successful state."""
    found = services()
    spec = prepared(found, with_input(found, request, context, selections))
    if spec is not None:
        launch(found, spec, context)
    return "succeeded"


def job_of(template: str) -> str:
    """The job a step belongs to. A one-task-per-job DAG names its job by a step of that job."""
    table, _ = load_tables(ROOT)
    job = table["steps"].get(template)
    if job is None or job == GATE:
        raise LaunchRefused(f"{template} starts no ECS job, so it cannot name the job of a DAG")
    return str(job)


def job_spec(found: LaunchServices, request: SubmissionRequest, context: Mapping[str, Any], selections: Mapping[str, str]) -> JobSpec:
    """The launch request of the job that ``request.template`` names, after every gate passed."""
    spec = prepared(found, with_input(found, request, context, selections))
    if spec is None:
        raise LaunchRefused(f"{request.template} starts no ECS job")
    return spec


def preflight_step(request: SubmissionRequest, context: Mapping[str, Any], selections: Mapping[str, str]) -> str:
    """Pass every gate of the job and return its name, or raise. Nothing is launched and no launch record is written."""
    return job_spec(services(), request, context, selections).job


def job_step(request: SubmissionRequest, context: Mapping[str, Any], selections: Mapping[str, str]) -> str:
    """Launch the job's one ECS task and wait for it, or join the recorded launch of the run and job."""
    found = services()
    launch(found, job_spec(found, request, context, selections), context)
    return "succeeded"


def evidence_step(request: SubmissionRequest, context: Mapping[str, Any], selections: Mapping[str, str]) -> dict[str, Any]:
    """The end record of the run and job's one launch, read back from the launch store. Refuses unless the job and its acceptance checks passed."""
    store = services().preflight.store
    job = job_of(request.template)
    claim = store.run_record(request.run_id, job, "claim")
    end = None
    if claim is not None:
        end = {record["event"]: record for record in store.records(request.run_id, job, [claim["attempt"]])}.get("end")
    if end is None or end.get("outcome") != "succeeded":
        found = end.get("outcome") if end is not None else "no end record"
        raise LaunchRefused(f"no successful launch of {job} is recorded for run {request.run_id}: {found}")
    return {"job": job, "run_id": request.run_id, "attempt": claim["attempt"] if claim else None, "task_arn": end.get("task_arn", "")}


def settle_launch(run_id: str, job: str, statement: str) -> str:
    """The owner's settle of an unresolved launch (``Launcher.owner_settle``), with the scheduler's own launch services and role."""
    outcome = services().launcher.owner_settle(run_id, job, None, statement)
    return f"{job} of run {run_id}: {outcome.outcome} ({outcome.reason})"


def main(argv: list[str] | None = None) -> int:
    """``python -m orchestration.airflow_steps settle --run <run> --job <job> --statement <what was checked>``, in the Airflow scheduler container."""
    parser = argparse.ArgumentParser(description="Owner commands of the direct ECS launch path.")
    commands = parser.add_subparsers(dest="command", required=True)
    settle = commands.add_parser("settle", help="end an unresolved launch after its window, with the family stopped and a statement of what was checked")
    settle.add_argument("--run", required=True)
    settle.add_argument("--job", required=True)
    settle.add_argument("--statement", required=True, help="what was checked: the ECS console and CloudTrail for the launch's identity")
    arguments = parser.parse_args(argv)
    sys.stdout.write(settle_launch(arguments.run, arguments.job, arguments.statement) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
