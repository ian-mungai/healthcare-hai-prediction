"""Run a DAG step through the direct ECS launch library.

``orchestration/dag_runtime.py`` calls ``launch_step`` for a step of a shipped DAG. A job step passes the preflight (``orchestration/ecs_preflight.py``), then
the launch operator runs the job: the first step of a job launches its one ECS task and waits, and every later step finds the recorded outcome of that
launch. A failed job fails each step, and trying again needs a new run. A control step (one with no job: acquire, seal, release, the gates) and the chain
gate launch nothing: the gate binds the code runs of the chain.
A staged input that is not there yet (``InputNotReady``) is retried for ``UNAVAILABLE_SECONDS``, then the step fails with the cause.
A ``LaunchUnavailable`` from the launch library means the launch is not finished: the step fails with it at once and nothing repeats it, because only
the library's own evidence resolves the launch: the next launch of the family settles it at the family gate (``Launcher.auto_settle``) once its task is found.
A one-task-per-job DAG (``dag_runtime.build_job_chain``) calls ``preflight_step`` (the gates, no launch), ``job_step`` (the launch) and ``evidence_step``
(the launch's end record says the job passed; after a bronze, silver, gold or features job it also sends the job's per-table lineage events, outside the
OpenLineage provider's forked process and under their own total deadline).
Each launch operator gets the lineage source of its step (``orchestration/ecs_lineage.py``), so the OpenLineage provider can ask it for the job's datasets.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from functools import partial
from pathlib import Path
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from orchestration import layer_report, promotion_chain
from orchestration.ecs_adapter import EcsAdapter, EcsSettings, client_config
from orchestration.ecs_launch import Clock, EcsFitProbe, JobSpec, Launcher, LaunchError, LaunchRefused, S3LaunchStore, launch_record_prefix
from orchestration.ecs_launch_operator import JobFailed
from orchestration.ecs_lineage import (
    TABLE_EVENT_JOBS,
    DeliveryFailed,
    EvidenceUnavailable,
    LineageRefused,
    LineageSource,
    S3Evidence,
    declared_lineage,
    lineage_layer,
    step_invocation,
)
from orchestration.ecs_preflight import (
    GATE,
    HOST_JOB_CAP_SETTING,
    TABLE_MAP_FILE,
    HeldCapProbe,
    InputNotReady,
    Preflight,
    PreflightSettings,
    S3CodeRuns,
    bronze_request_reader,
    family_of,
    host_cap_reader,
    load_tables,
    s3_digest_reader,
    ssm_current_code_run_reader,
    ssm_launch_hold_reader,
)
from orchestration.interface import SubmissionRequest

LOG = logging.getLogger(__name__)
UNAVAILABLE_SECONDS = 600.0
UNAVAILABLE_POLL_SECONDS = 15.0
JOB_TASK = "job"  # the task of a one-job chain that launches (``dag_runtime.CHAIN_TASKS``); its identity is what the bronze table events name
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
    "HAI_LAUNCH_HOST_CAPACITY",
    HOST_JOB_CAP_SETTING,
    "HAI_LAUNCH_HOST_HOLD_PARAMETER",
    "HAI_LAUNCH_APPLY_LOCK_PARAMETER",
    "HAI_LAUNCH_CURRENT_CODE_RUN_PARAMETER",
)
# The usable-memory inputs the services stack always renders (host_usable_memory_mib defaults to the current host size), checked by the preflight
# against the binding of every host revision.
HOST_CAPACITY = "HAI_LAUNCH_HOST_CAPACITY"


@dataclass(frozen=True)
class LaunchServices:
    """What a step needs to launch a job: the preflight, the launcher, the operator class, the cluster and the clock the retries wait on."""

    preflight: Preflight
    launcher: Launcher
    operator_class: Callable[..., Any]
    cluster: str
    definition: Callable[[str], str]
    clock: Clock
    report: layer_report.ReportAccess | None = None
    # Reads one object of the data bucket: its bytes, or None when the key is absent. The research DAG reads the records that research and the canary store.
    read_object: Callable[[str], bytes | None] | None = None
    lineage: Callable[[SubmissionRequest, JobSpec], Callable[[bool], Mapping[str, Any]]] | None = None  # the lineage source of a step's launch
    evidence: S3Evidence | None = None  # the S3 evidence the evidence step sends a bronze job's per-table lineage events from


def host_fit_probe(inner: EcsFitProbe, hold: Callable[[], str | None], cap: Callable[[], str | None]) -> HeldCapProbe:
    """The fit probe of every host launch: it rereads the launch hold and the host job cap at each fit check and, as the launcher's ``send_check``,
    again immediately before each RunTask (owner A, Nov 10 2026: launch hold during services applies), so a hold set or a cap lowered after the
    preflight or after the fit check withholds the call."""
    return HeldCapProbe(inner, hold, cap)


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
    s3, ssm = (session.client(name, config=client_config()) for name in ("s3", "ssm"))
    adapter = EcsAdapter.from_session(
        session,
        EcsSettings(
            cluster=values["HAI_LAUNCH_CLUSTER"],
            family_prefix=values["HAI_LAUNCH_FAMILY_PREFIX"],
            project_tag=values["HAI_LAUNCH_PROJECT_TAG"],
            log_group=values["HAI_LAUNCH_LOG_GROUP"],
            families=frozenset(table["jobs"]),
            host_capacity_provider=values["HAI_LAUNCH_HOST_PROVIDER"],
        ),
    )
    scratch = Path(values["HAI_LAUNCH_SCRATCH_PATH"])
    report = layer_report.ReportAccess(
        s3=s3,
        bucket=values["HAI_LAUNCH_BUCKET"],
        ecs=adapter.ecs,
        autoscaling=session.client("autoscaling", config=client_config()),
        cluster=values["HAI_LAUNCH_CLUSTER"],
        family_prefix=values["HAI_LAUNCH_FAMILY_PREFIX"],
        host_group=os.environ.get("HAI_LAUNCH_HOST_GROUP", ""),
        directory=Path(os.environ.get("HAI_LAUNCH_REPORT_DIR") or layer_report.DEFAULT_DIRECTORY),
    )
    hold = ssm_launch_hold_reader(ssm, values["HAI_LAUNCH_HOST_HOLD_PARAMETER"], values["HAI_LAUNCH_APPLY_LOCK_PARAMETER"])
    cap = host_cap_reader(values[HOST_JOB_CAP_SETTING])
    clock = Clock()
    probe = host_fit_probe(EcsFitProbe(adapter.ecs, values["HAI_LAUNCH_CLUSTER"], lambda: shutil.disk_usage(scratch).free), hold, cap)
    store = S3LaunchStore(s3, values["HAI_LAUNCH_BUCKET"], launch_record_prefix())
    launcher = Launcher(
        adapter,
        store,
        probe,
        clock,
        scratch_usage=partial(scratch_usage, scratch, sorted(table["jobs"])),
        send_check=probe.authorize,
    )
    bucket = values["HAI_LAUNCH_BUCKET"]
    collections = {entry["table"]: entry["collection"] for entry in json.loads((ROOT / TABLE_MAP_FILE).read_text())["tables"]}
    preflight = Preflight(
        table=table,
        host=host,
        settings=PreflightSettings(polaris_uri=values["HAI_LAUNCH_POLARIS_URI"], mlflow_uri=values["HAI_LAUNCH_MLFLOW_URI"]),
        code_digest=s3_digest_reader(s3, bucket),
        store=store,
        code_runs=S3CodeRuns(s3, bucket),
        clock=clock.wall,
        bronze_request=bronze_request_reader(s3, bucket, collections),
        current_code_run=ssm_current_code_run_reader(
            ssm, values["HAI_LAUNCH_CURRENT_CODE_RUN_PARAMETER"], required=os.environ.get("HAI_ENVIRONMENT") == "prod"
        ),
        host_capacity=values[HOST_CAPACITY],
        task_definition=adapter.task_definition,
        host_job_cap=cap,
        host_launch_hold=hold,
        read_record=object_reader(s3, bucket),
    )
    evidence = S3Evidence(s3, bucket)
    return LaunchServices(
        preflight,
        launcher,
        BrokerlessEcsRunTaskOperator,
        values["HAI_LAUNCH_CLUSTER"],
        lambda job: adapter.definition(job),
        clock,
        report,
        object_reader(s3, bucket),
        lineage_factory(evidence, store),
        evidence,
    )


def object_reader(s3: Any, bucket: str) -> Callable[[str], bytes | None]:
    """Read an object of the data bucket: None when the key is absent, a refusal that names the key when the read fails for any other reason."""

    def read(key: str) -> bytes | None:
        try:
            return bytes(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            if code in ("NoSuchKey", "404", "NotFound"):
                return None
            raise promotion_chain.RecordRefused(f"{key} cannot be read: {code or 'ClientError'}") from None
        except BotoCoreError as error:
            raise promotion_chain.RecordRefused(f"{key} cannot be read: {type(error).__name__}") from None

    return read


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


def send_table_events(source: LineageSource) -> None:
    """Send a succeeded bronze job's per-table events (``LineageSource.send_table_events``) when HTTP lineage is on. A failure or the deadline is logged and
    never fails the step; ``replay`` rebuilds what was not sent. Only the kind of an unexpected failure is logged: its text can hold a header or a key."""
    try:
        sent = source.send_table_events()
    except (DeliveryFailed, LineageRefused, EvidenceUnavailable) as error:  # their texts name a key or a rule, never a credential
        LOG.warning(
            "%s table lineage events of %s attempt %s were not delivered: %s: %s", source.job, source.run_id, source.attempt, type(error).__name__, error
        )
    except Exception as error:  # the job already succeeded: nothing of its lineage may fail the step
        LOG.warning("%s table lineage events of %s attempt %s were not delivered: %s", source.job, source.run_id, source.attempt, type(error).__name__)
    else:
        if sent:
            LOG.info("sent %d %s table lineage events of %s attempt %s", sent, source.job, source.run_id, source.attempt)


def send_job_table_events(found: LaunchServices, request: SubmissionRequest, job: str, attempt: int) -> None:
    """The evidence step's sends of a succeeded bronze or layer job's per-table events. The step runs outside the OpenLineage provider's forked process,
    which the provider kills after its execution timeout (``AIRFLOW__OPENLINEAGE__EXECUTION_TIMEOUT``), so the sends have their own total deadline
    (``ecs_lineage.TABLE_EVENTS_SECONDS``). The events name the job's own task (``JOB_TASK``) and launch attempt, as the replay of that attempt does."""
    try:
        if found.evidence is None:
            return
        invocation = step_invocation(replace(request, task_id=JOB_TASK, attempt=attempt).as_dict())
        declared = declared_lineage(job)
        send_table_events(LineageSource(job, request.run_id, attempt, invocation, declared, found.evidence, found.preflight.store))
    except Exception as error:  # the job already succeeded: no lineage of it may fail the step
        LOG.warning("%s table lineage events of %s were not delivered: %s", job, request.run_id, type(error).__name__)


def lineage_factory(evidence: S3Evidence, store: S3LaunchStore) -> Callable[[SubmissionRequest, JobSpec], Callable[[bool], Mapping[str, Any]]]:
    """The lineage source of a step's launch. Nothing is read or built until the provider asks, so a missing declaration or a refused record happens
    inside the operator's guard (``LaunchGuarantees.lineage_of``) and never stops the job."""

    def lineage(request: SubmissionRequest, spec: JobSpec) -> Callable[[bool], Mapping[str, Any]]:
        def facets(complete: bool) -> Mapping[str, Any]:
            declared = declared_lineage(spec.job)
            return LineageSource(spec.job, spec.run_id, spec.attempt, step_invocation(request.as_dict()), declared, evidence, store).facets(complete)

        return facets

    return lineage


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


def launch(
    services_: LaunchServices, request: SubmissionRequest, spec: JobSpec, context: Mapping[str, Any], built: Callable[[Any], None] | None = None
) -> None:
    """Launch the job's one ECS task and wait for it, or join the recorded launch of the run and job. ``built`` gets the launch operator before it runs, so
    the task that runs it can hand the operator's lineage to the OpenLineage listener after the launch succeeded or failed."""
    operator = services_.operator_class(
        task_id=context["ti"].task_id,
        job=spec,
        launcher=lambda: services_.launcher,
        task_definition=services_.definition(spec.job),
        cluster=services_.cluster,
        overrides={},
        lineage=services_.lineage(request, spec) if services_.lineage else None,
    )
    if built is not None:
        built(operator)
    operator.execute(context)


def launch_step(request: SubmissionRequest, context: Mapping[str, Any], selections: Mapping[str, str]) -> str:
    """Run the step through the launch library and return its successful state."""
    found = services()
    spec = prepared(found, with_input(found, request, context, selections))
    if spec is not None:
        launch(found, request, spec, context)
    return "succeeded"


def job_of(template: str, dag_id: str) -> str:
    """The job a step belongs to. A one-task-per-job DAG names its job by a step of that job; a model set's DAG launches in its set's family."""
    table, host = load_tables(ROOT)
    job = family_of(table["steps"], host, template, dag_id) if template in table["steps"] else None
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


def report_access(found: LaunchServices) -> layer_report.ReportAccess:
    """The clients and the private folder of the layer acceptance report, or a refusal when this launch service has none."""
    if found.report is None:
        raise LaunchRefused("the layer acceptance report is not configured for this launch service")
    return found.report


def job_step(request: SubmissionRequest, context: Mapping[str, Any], selections: Mapping[str, str], built: Callable[[Any], None] | None = None) -> str:
    """Launch the job's one ECS task and wait for it, or join the recorded launch of the run and job. ``built`` gets the launch operator (see ``launch``).

    A silver or gold job saves the listing of every object version under ``lakehouse/`` here, after the gates and right before the launch. This task holds the
    slots of the launch pool (a silver or gold job takes every slot), so no other run's job writes meanwhile, and the writes of another run during the wait
    are in the listing. A listing that
    exists is kept. A listing that fails stops the step before the launch. Right after the launch, still holding the slot, the task saves the closing evidence
    (the listing after the launch, the running tasks of the host family and the host facts), so the job that takes the slot next cannot change it. A closing
    record that fails fails this task, and the evidence task refuses the run."""
    found = services()
    spec = job_spec(found, request, context, selections)
    report = spec.job in layer_report.LAYERS
    if report:
        try:
            layer_report.save_before_listing(report_access(found), spec.job, request.run_id)
        except layer_report.ReportError as error:
            raise LaunchRefused(str(error)) from None
    launch(found, request, spec, context, built)
    if report:
        try:
            layer_report.save_closing_evidence(report_access(found), spec.job, request.run_id)
        except layer_report.ReportError as error:
            raise LaunchRefused(str(error)) from None
    return "succeeded"


def evidence_step(request: SubmissionRequest, context: Mapping[str, Any], selections: Mapping[str, str]) -> dict[str, Any]:
    """The end record of the run and job's one launch, read back from the launch store. Refuses unless the job and its acceptance checks passed. After a
    bronze or layer job passed it also sends the job's per-table lineage events (``send_job_table_events``); a failure of that is logged and never fails the
    step."""
    launch_services = services()
    store = launch_services.preflight.store
    job = job_of(request.template, request.dag_id)
    claim = store.run_record(request.run_id, job, "claim")
    end = None
    if claim is not None:
        end = {record["event"]: record for record in store.records(request.run_id, job, [claim["attempt"]])}.get("end")
    if claim is None or end is None or end.get("outcome") != "succeeded":
        found = end.get("outcome") if end is not None else "no end record"
        raise LaunchRefused(f"no successful launch of {job} is recorded for run {request.run_id}: {found}")
    result: dict[str, Any] = {"job": job, "run_id": request.run_id, "attempt": claim["attempt"], "task_arn": end.get("task_arn", "")}
    if job in layer_report.LAYERS:
        try:
            accepted = layer_report.accept(report_access(launch_services), job, request.run_id)
        except layer_report.ReportError as error:
            raise LaunchRefused(str(error)) from None
        result |= {"report": accepted["report"], "passed": True}
    if lineage_layer(job) in TABLE_EVENT_JOBS:
        send_job_table_events(launch_services, request, job, int(claim["attempt"]))
    return result


def research_missing_models(request: SubmissionRequest) -> list[str]:
    """The promotion-eligible models that have no passed training run on the research request's base run, read from the launch records. Nothing is launched."""
    base_run = str((request.training or {})["base_run"])
    return promotion_chain.missing_models(services().preflight.store, base_run)


def research_chain_step(request: SubmissionRequest, context: Mapping[str, Any]) -> str:
    """Run the promotion chain of every infection type of the research request that has a challenger, and fail with every failure after the last type.

    Each launching step binds its run to the code of the research run, launches once under its own run (a retry joins it) and fails by name when its binding,
    its launch or its job does, so one type's failure never reaches another type.
    """
    found = services()
    training = request.training or {}

    def launch_step_of(template: str, run: str, parameters: dict[str, Any]) -> None:
        # The serve step derives its candidate from the research record of the run it deploys from, so it names that run as its input.
        step = replace(request, template=template, run_id=run, input_run_id=request.run_id if template == "serve_deploy" else None, training=parameters)
        try:
            found.preflight.code_runs.bind(run, found.preflight.code_runs.resolve(request.run_id))
            spec = prepared(found, step)
            if spec is None:
                raise LaunchRefused(f"{template} starts no ECS job")
            launch(found, step, spec, context)
        except (JobFailed, LaunchError) as error:
            raise promotion_chain.StepFailed(f"{type(error).__name__}: {error}") from None

    def read(key: str) -> bytes | None:
        if found.read_object is None:
            raise promotion_chain.RecordRefused("these launch services cannot read the data bucket")
        return found.read_object(key)

    def release(model: str, features_run: str, run: str) -> None:
        promotion_chain.release_online(found, context, model, features_run, run)

    failures = promotion_chain.run_types(
        str(training["base_run"]), request.run_id, list(training["hai_types"]), launch=launch_step_of, read=read, release=release
    )
    if failures:
        raise RuntimeError("the promotion chain failed: " + "; ".join(failures))
    return "chained"
