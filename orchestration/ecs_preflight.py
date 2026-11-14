"""The gates of the direct ECS launch, as a library for the Airflow launch mode, and the staged-input readers.

``Preflight.prepare`` turns the request of one DAG step into the ``JobSpec`` of the direct ECS launch library (``orchestration/ecs_launch.py``). Its gates
are real mode, the previous job's success (``after``), the training parameters, the staged bronze and serving requests, the staged
code digest and the run's code run. Each gate refuses before any record or ECS call. The bound inputs of a run (the code digest, the request hash and the
input run) go into the spec, and the launch library holds every later attempt of the run and job to them.
The host table (``config/orchestration/host_jobs.json``) may give a job its own containers and log container, where its host task definition differs from the
job table's containers (the metadata job).
Host launch capacity binding (owner A, Nov 10 2026): the gates refuse while the usable-memory inputs are not set. After the staged-input reads, the
preflight describes the latest active revision of the job's host definition once and refuses it unless its capacity label is bound to the current
inputs (``checked_revision``). The spec names that exact revision and the launch library runs it, so a revision registered later never runs in its place.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from functools import partial
from pathlib import Path
from typing import Any, Protocol

from botocore.exceptions import BotoCoreError, ClientError
from jsonschema import Draft202012Validator

from orchestration.ecs_adapter import SendWithheld, TaskRefused, TaskUnavailable
from orchestration.ecs_launch import RETRYABLE_CODES, JobSpec, LaunchRefused, LaunchStore, LaunchUnavailable, Resources
from orchestration.ingestion_schedule import RETRY_DAYS, RETRY_KIND, retry_run_id
from orchestration.interface import SubmissionRequest
from orchestration.promotion_chain import RecordRefused, challenger_named, stored_decision
from orchestration.training_contract import (
    FEATURE_DAGS,
    LAUNCH_LANES,
    MODELS,
    PROMOTION_MODELS,
    chain_parameters_for,
    model_drift_run_id,
    model_features_run_id,
    model_training_run_id,
    parameters_for,
    record_key,
    serving_parameters_for,
    trained_run_id,
)

JOBS_FILE = Path("config/orchestration/ecs_broker_jobs.json")
JOBS_SCHEMA = Path("config/orchestration/ecs_broker_jobs.schema.json")
HOST_FILE = Path("config/orchestration/host_jobs.json")
# Jobs only the direct launch runs, in the same schema: the Run 3 publisher is one of them.
DIRECT_JOBS_FILE = Path("config/orchestration/direct_jobs.json")
TABLE_MAP_FILE = Path("config/lakehouse/bronze_tables.json")
BRONZE_REQUEST_BYTES = 4096
BRONZE_REQUEST_KEYS = {"tables", "target", "dry_run", "inventory_sha256"}
APPROVAL_BYTES = 4096
DIGEST = re.compile(r"[a-f0-9]{64}")
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
BRONZE_VARIABLES = ("BRONZE_TABLES", "BRONZE_TARGET", "BRONZE_DRY_RUN", "BRONZE_REQUEST_SHA256", "BRONZE_INVENTORY_SHA256")
SERVING_VARIABLES = ("SERVING_REQUEST", "SERVING_REQUEST_SHA256")
PUBLISHER_VARIABLES = ("PUBLISHER_RUN_KIND", "HAI_CAPTURE_ROOT")
# A bootstrap skips every unit with a completion record. A monthly run downloads each unit again and compares its payload (``recheck``). A retry is a new
# run of the same kind of work as a bootstrap: it names the monthly run it retries and its day, and skips every unit that has a completion record.
KINDS = frozenset({"bootstrap", "monthly", "retry"})
REQUEST_KEYS = frozenset({"kind", "capture_root"})
RETRY_REQUEST_KEYS = REQUEST_KEYS | {"original_run", "day"}
# What a launch may never name: the staged inputs archive, the queue whose state a task restores, the resumed sources and the prior state of the earlier flow.
RETIRED_KEYS = frozenset({"inputs_sha256", "queue_sha256", "resume_sources", "prior_state"})
# The capture root is raw/ or one test folder of one run ID (owner decision D4). The task validates it again.
CAPTURE_ROOT = re.compile(r"raw/|ingestion_test/[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}/")
DEADLINE_VARIABLE = "HAI_DEADLINE_UTC"
# The job room that a family without reserve takes at task level: the services defaults job_cpu_units and host_job_cap_mib (a case compares them).
JOB_ROOM_CPU_UNITS = 3072
JOB_ROOM_MEMORY_MIB = 26816
GATE = "@gate"
CHAIN = "chain"
CHAIN_LAYERS = ("silver", "gold", "features")
# The drift check of a model set: it checks the set of one model run, and its job needs the features job of that run to have passed.
DRIFT_DAG = "drift_monitoring_dag"
JOB_MODE = "real"
# The feature set a DAG's features job builds (the FEATURE_SET of its containers). A DAG that names none cannot launch the features job.
FEATURE_SETS = {"features_base_dag": "base", **FEATURE_DAGS}
NO_STAGED_CODE = "0" * 64  # the bound code digest of a job that fetches no staged code
CAPACITY_LABEL = "hai.host_capacity"  # the label of a host revision's init container that binds it to the host capacity (infra/jobs/host.tf)
HOST_JOB_CAP_SETTING = "HAI_LAUNCH_HOST_JOB_CAP_MIB"  # the host job cap in MiB that the services stack renders into every Airflow container (host_job_cap_mib)
CAPACITY_FORM = re.compile(r"usable_memory_mib=[1-9][0-9]* system_reserve_mib=[0-9]+")  # HAI_LAUNCH_HOST_CAPACITY, from the services stack
BINDING_FORM = re.compile(
    r"job_memory_mib=(?P<job>[1-9][0-9]*) (?P<inputs>usable_memory_mib=(?P<usable>[1-9][0-9]*) system_reserve_mib=(?P<reserve>[0-9]+)) "
    r"service_reservations_mib=(?P<services>[0-9]+)"
)  # the label of a host revision, the services output host_capacity_binding
# The phase of the promote family's one task definition that each step runs: the PHASE variable of its container, which the task command passes to the runner.
PHASES = {"promote": "promote", "research": "research", "canary_check": "canary", "owner_notice": "notify"}
# The steps that the research DAG launches with their own closed parameters, besides its research step.
CHAIN_TEMPLATES = ("canary_check", "owner_notice", "promote")


def phase_of(template: str) -> str:
    """The phase the promote family's task runs for ``template``; a step that names none is refused, so the task never runs a phase by default."""
    try:
        return PHASES[template]
    except KeyError:
        raise LaunchRefused(f"the step {template} names no phase of the promote task") from None


def features_run_of(request: SubmissionRequest) -> str:
    """The features run whose chain the metadata job ingests: the run the DAG form named. It must be a run ID, so a missing value or ``latest`` is refused
    here, before anything launches, and the job can never ingest a chain nobody named."""
    run = request.input_run_id
    if run is None or not RUN_ID.fullmatch(run):
        raise LaunchRefused(f"the metadata job needs FEATURES_RUN, the ID of the features run to ingest, not {run or 'latest'!r}: there is no latest run")
    return run


def feature_set_of(dag_id: str) -> str:
    """The feature set the features job of ``dag_id`` builds; a DAG that names none is refused, so the job never runs a set by default."""
    try:
        return FEATURE_SETS[dag_id]
    except KeyError:
        raise LaunchRefused(f"the DAG {dag_id} names no feature set for the features job") from None


def drift_model(request: SubmissionRequest) -> str:
    """The model whose set a drift check reads, after the gates that bind the check: a model of the contract, and a drift run that derives from the model run
    it checks (the run the chain gate bound to the bronze code), so that no other run is checked under that name."""
    model = (request.training or {}).get("model")
    if model not in MODELS:
        raise LaunchRefused(f"the drift check names {model!r}, which is not one of {', '.join(MODELS)}")
    if request.input_run_id is None or request.run_id != model_drift_run_id(request.input_run_id):
        raise LaunchRefused(f"the drift run {request.run_id} does not derive from the model run {request.input_run_id} it checks")
    return str(model)


class S3Reader(Protocol):
    def get_object(self, **arguments: Any) -> dict[str, Any]: ...


def s3_digest_reader(s3: S3Reader, bucket: str) -> Callable[[str], str]:
    """Read the SHA-256 of a run's staged code archive from ``code.sha256`` beside ``code.tar.gz``."""

    def read(run_id: str) -> str:
        try:
            body = s3.get_object(Bucket=bucket, Key=f"lakehouse/run_stage/{run_id}/inputs/code.sha256")["Body"].read(256)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "NoSuchKey":
                raise TaskUnavailable("code_digest_not_staged") from None
            raise TaskUnavailable("code_digest_unreadable") from None
        except BotoCoreError:
            raise TaskUnavailable("code_digest_unreadable") from None
        text = body.decode(errors="replace").strip()
        if not DIGEST.fullmatch(text):
            raise TaskRefused("code_digest_malformed")
        return text

    return read


def s3_object_reader(s3: S3Reader, bucket: str, name: str) -> Callable[[str], str | None]:
    """Read a small staged record of a run (``lakehouse/run_stage/<run>/inputs/<name>``), or None while the owner has not staged it."""

    def read(run_id: str) -> str | None:
        try:
            body = s3.get_object(Bucket=bucket, Key=f"lakehouse/run_stage/{run_id}/inputs/{name}")["Body"].read(APPROVAL_BYTES + 1)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "NoSuchKey":
                return None
            raise TaskUnavailable("staged_record_unreadable") from None
        except BotoCoreError:
            raise TaskUnavailable("staged_record_unreadable") from None
        return str(body.decode(errors="replace"))

    return read


def bronze_request_reader(s3: S3Reader, bucket: str, collections: Mapping[str, str]) -> Callable[[str], dict[str, str]]:
    """Read and check the run's staged ``bronze_request.json`` and return the values of the bronze containers.

    The file holds exactly ``tables`` (a list of table names), ``target`` (``real`` or ``qualify``), ``dry_run`` (true or false) and
    ``inventory_sha256`` (the digest of the staged source inventory). The tables must be
    in the table map (``collections`` maps each table to its collection) and a qualification run takes the tables of one collection. A file that is not
    staged yet is retryable, a file that fails a check is refused.
    """

    def read(run_id: str) -> dict[str, str]:
        from scripts.lakehouse.bronze_dispatch import DispatchError, check_dispatch

        try:
            body = s3.get_object(Bucket=bucket, Key=f"lakehouse/run_stage/{run_id}/inputs/bronze_request.json")["Body"].read(BRONZE_REQUEST_BYTES + 1)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "NoSuchKey":
                raise TaskUnavailable("bronze_request_not_staged") from None
            raise TaskUnavailable("bronze_request_unreadable") from None
        except BotoCoreError:
            raise TaskUnavailable("bronze_request_unreadable") from None
        if len(body) > BRONZE_REQUEST_BYTES:
            raise TaskRefused("bronze_request_too_large")
        try:
            document = json.loads(body)
        except ValueError:
            raise TaskRefused("bronze_request_malformed") from None
        if not isinstance(document, dict) or set(document) != BRONZE_REQUEST_KEYS:
            raise TaskRefused("bronze_request_needs_exactly_tables_target_and_dry_run")
        tables, target, dry_run, inventory = document["tables"], document["target"], document["dry_run"], document["inventory_sha256"]
        if (
            not isinstance(tables, list)
            or not all(isinstance(name, str) for name in tables)
            or not isinstance(target, str)
            or type(dry_run) is not bool
            or not isinstance(inventory, str)
            or not DIGEST.fullmatch(inventory)
        ):
            raise TaskRefused("bronze_request_has_wrong_types")
        try:
            names = check_dispatch(tables, target, collections)
        except DispatchError as error:
            raise TaskRefused(f"bronze_request_refused: {error}") from None
        values = {"BRONZE_TABLES": ",".join(names), "BRONZE_TARGET": target, "BRONZE_DRY_RUN": "1" if dry_run else ""}
        return values | {"BRONZE_REQUEST_SHA256": hashlib.sha256(body).hexdigest(), "BRONZE_INVENTORY_SHA256": inventory}

    return read


def ssm_current_code_run_reader(ssm: Any, name: str, required: bool = False) -> Callable[[], str | None]:
    """Read the code run that CI staged last from Parameter Store. A parameter that does not exist reads as None, or is refused by name where ``required``
    (production, where no run may fall back to code staged under its own run ID)."""

    def read() -> str | None:
        try:
            return str(ssm.get_parameter(Name=name)["Parameter"]["Value"])
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "ParameterNotFound":
                if required:
                    raise TaskRefused(f"current_code_run_parameter_{name}_is_not_set") from None
                return None
            raise TaskUnavailable("current_code_run_unreadable") from None
        except BotoCoreError:
            raise TaskUnavailable("current_code_run_unreadable") from None

    return read


def publisher_values(request: object, run_id: str) -> dict[str, str]:
    """The ``run`` container's publisher variables of a checked request, with ``PUBLISHER_LAUNCH_SHA256`` the digest of its canonical form, which binds
    every attempt. A request that is not exactly the kind and the capture root (a retry also names its original run and its day, and runs under the run ID
    of that day) is refused before any launch record or cloud call."""
    if isinstance(request, Mapping) and RETIRED_KEYS & set(request):
        raise LaunchRefused("publisher_request_takes_no_inputs_or_prior_state")
    retry = isinstance(request, Mapping) and request.get("kind") == RETRY_KIND
    if not isinstance(request, Mapping) or set(request) != (RETRY_REQUEST_KEYS if retry else REQUEST_KEYS):
        raise LaunchRefused(
            "publisher_retry_request_needs_exactly_kind_capture_root_original_run_and_day" if retry else "publisher_request_needs_exactly_kind_and_capture_root"
        )
    kind, root = request["kind"], request["capture_root"]
    if not isinstance(kind, str) or kind not in KINDS:
        raise LaunchRefused("publisher_request_has_wrong_kind")
    if not isinstance(root, str) or not CAPTURE_ROOT.fullmatch(root):
        raise LaunchRefused("publisher_request_has_wrong_capture_root")
    if retry:
        original, day = request["original_run"], request["day"]
        if not isinstance(original, str) or not RUN_ID.fullmatch(original) or isinstance(day, bool) or not isinstance(day, int) or not 1 <= day <= RETRY_DAYS:
            raise LaunchRefused("publisher_retry_request_has_wrong_original_run_or_day")
        if run_id != retry_run_id(original, day):
            raise LaunchRefused("publisher_retry_run_is_not_the_run_of_that_day")
    canonical = json.dumps(dict(request), sort_keys=True, separators=(",", ":"))
    return {"PUBLISHER_RUN_KIND": kind, "HAI_CAPTURE_ROOT": root, "PUBLISHER_LAUNCH_SHA256": hashlib.sha256(canonical.encode()).hexdigest()}


def require_publisher_request(request: SubmissionRequest) -> dict[str, str]:
    """The checked publisher values of a request whose run ID is a run ID. Nothing here reads the cloud, so a malformed request is refused at once."""
    if not RUN_ID.fullmatch(request.run_id):
        raise LaunchRefused("publisher_request_has_wrong_run_id")
    return publisher_values(request.publisher, request.run_id)


def derived_run_id(parent_run: str, layer: str) -> str:
    """The run ID of a chain layer: the parent's time stamp and eight hex digits of a hash. ``dags/bronze_chain_dag.py`` derives the same IDs."""
    return f"{parent_run[:16]}-{hashlib.sha256(f'{layer}:{parent_run}'.encode()).hexdigest()[:8]}"


def serving_values(
    request: Mapping[str, Any], research_run: str | None, canary_percent: int, read: Callable[[str], bytes | None], canary_ttl_hours: int
) -> dict[str, str]:
    """The values of the serve container for a closed launch request: the infection type and the mode, canary or stable.

    The request names no version, share or lifetime. A canary deploys the challenger of the type's research record in the research run the launch publishes
    from, at the share and for the lifetime the job table fixes, and the derived request names that research run so the job gives every deployment of one
    research canary the expiry of the first. A canary whose stored check result is a hold is refused: a held canary gets no new traffic. A stable deploy names
    none and has no expiry: the serve job writes the Production version alone, which it reads from the registry. A request with any other key, a canary without
    a research run and a type with no challenger are refused. The container receives the derived request and reads nothing else.
    """
    try:
        closed = serving_parameters_for(request)
    except ValueError as error:
        raise TaskRefused(f"serving_request_refused: {error}") from None
    derived: dict[str, Any] = {**closed, "candidate_version": None, "percent": 0, "canary_ttl_hours": None, "research_run": None}
    if closed["deploy_mode"] == "canary":
        if research_run is None:
            raise TaskRefused("serving_request_refused: a canary deploys the challenger of a research run, and the launch names none")
        try:
            named = challenger_named(read, research_run, closed["hai_type"])
        except (RecordRefused, ValueError) as error:
            raise TaskRefused(f"serving_candidate_refused: {error}") from None
        if named is None:
            raise TaskRefused(f"serving_candidate_refused: the research record of {closed['hai_type']} has no challenger to deploy")
        try:
            held = read(record_key("canary", research_run, closed["hai_type"])) is not None and (
                stored_decision(read, research_run, closed["hai_type"], named[0], named[1]) == "hold"
            )
        except (RecordRefused, ValueError) as error:
            raise TaskRefused(f"serving_canary_record_refused: {error}") from None
        if held:
            raise TaskRefused(f"serving_canary_held: the canary check of {closed['hai_type']} in {research_run} held it, so it gets no new traffic")
        derived |= {"candidate_version": named[1], "percent": canary_percent, "canary_ttl_hours": canary_ttl_hours, "research_run": research_run}
    body = json.dumps(derived, sort_keys=True, separators=(",", ":"))
    return {"SERVING_REQUEST": body, "SERVING_REQUEST_SHA256": hashlib.sha256(body.encode()).hexdigest()}


# Launch hold during services applies (owner A, Nov 10 2026): the owner command around every services apply writes HELD_PREFIX... before it
# changes anything and LAUNCH_HOLD_CLEAR only after the ECS services reach steady state. Any other value, or no parameter, holds every host launch.
LAUNCH_HOLD_CLEAR = "clear"
# The longest time from the start of the send boundary's hold and cap reads to the RunTask call. With one RunTask attempt (5 s to connect, 10 s to
# read, orchestration.ecs_adapter.send_config) a task that the reads allowed is sent and answered within 25 s, below the 30 s the services apply
# command waits after it sets the hold (scripts/infrastructure/services_apply.py HOLD_SETTLE_SECONDS).
SEND_AUTHORIZATION_SECONDS = 10.0


def ssm_launch_hold_reader(ssm: Any, name: str, lock_name: str | None = None) -> Callable[[], str | None]:
    """Read the launch hold that the services apply command writes to Parameter Store, or None while the parameter does not exist.

    With ``lock_name`` it first reads the services apply lock: while the lock exists a services apply runs or stopped part way (after taking the
    lock and before setting the hold, or after clearing the hold and before deleting the lock), so the value read is a held value naming the lock,
    whatever the hold says. An unreadable lock is not known to be absent and raises like an unreadable hold."""

    def read() -> str | None:
        try:
            if lock_name is not None:
                try:
                    token = str(ssm.get_parameter(Name=lock_name)["Parameter"]["Value"])
                except ClientError as error:
                    if error.response.get("Error", {}).get("Code") != "ParameterNotFound":
                        raise
                else:
                    return f"the services apply lock is held by {token[:120]}"
            return str(ssm.get_parameter(Name=name)["Parameter"]["Value"])
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "ParameterNotFound":
                return None
            raise TaskUnavailable("launch_hold_unreadable") from None
        except BotoCoreError:
            raise TaskUnavailable("launch_hold_unreadable") from None

    return read


def held(value: str | None) -> str | None:
    """Why a launch hold value holds host launches, or None when it is the clear value."""
    if value is None:
        return "the launch hold parameter was not found (no services apply command has cleared it yet)"
    if value != LAUNCH_HOLD_CLEAR:  # exactly "clear": a padded or differently written value holds launches
        return f"the launch hold is set ({value.strip()[:120]!r})"
    return None


def cap_value(raw: str | None) -> int | None:
    """The host job cap in MiB, or None when the setting is missing or not a whole number above 0."""
    text = (raw or "").strip()
    return int(text) if text.isdecimal() and text.isascii() and int(text) > 0 else None


def host_cap_reader(value: str) -> Callable[[], str | None]:
    """The reader of the host job cap (MiB) that the services stack renders into the Airflow environment as ``HAI_LAUNCH_HOST_JOB_CAP_MIB``. The
    environment of a container does not change, so the reader answers the checked value. A value that is not a whole number of MiB above 0 fails the
    build of the launch services, by the setting's name."""
    cap = cap_value(value)
    if cap is None:
        raise LaunchRefused(
            f"{HOST_JOB_CAP_SETTING} {value[:40]!r} is not a whole number of MiB above 0. Set host_job_cap_mib in the services stack, apply it, then run again"
        )
    return lambda: str(cap)


class HeldCapProbe:
    """The fit probe of host launches: before every RunTask the launcher asks it for the host's free room. It rereads the launch hold and the host
    job cap each time: while the hold is set, or the cap is unreadable, the room is unavailable (the launcher waits and never sends); otherwise the
    free memory is never more than the host job cap, so the host never gets a job above the cap."""

    def __init__(self, inner: Any, hold: Callable[[], str | None], cap: Callable[[], str | None], monotonic: Callable[[], float] = time.monotonic) -> None:
        self.inner, self.hold, self.cap, self.monotonic = inner, hold, cap, monotonic

    def authorize(self, need: Resources) -> None:
        """The send boundary (``Launcher.send_check``), run immediately before every RunTask: reread the cap, then the hold, and withhold the send
        while the hold is set, the cap is unreadable or below the job's memory, or the reads took longer than ``SEND_AUTHORIZATION_SECONDS``."""
        started = self.monotonic()
        try:
            cap = cap_value(self.cap())
            reason = held(self.hold())
        except TaskUnavailable:
            raise SendWithheld from None
        if reason is not None or cap is None or need.memory_mib > cap or self.monotonic() - started > SEND_AUTHORIZATION_SECONDS:
            raise SendWithheld

    def free(self) -> Resources:
        reason = held(self.hold())
        if reason is not None:
            raise TaskUnavailable(f"launch_hold: {reason}")
        cap = cap_value(self.cap())
        if cap is None:
            raise TaskUnavailable("host_job_cap_unreadable")
        free = self.inner.free()
        return Resources(cpu_units=free.cpu_units, memory_mib=min(free.memory_mib, cap), scratch_bytes=free.scratch_bytes)


class InputNotReady(LaunchUnavailable):
    """A staged input of the run is not readable yet, or the store of the staged inputs throttled. This is the preflight's own wait: a later identical call
    can succeed, and ``airflow_steps`` repeats the step for a bounded time. A ``LaunchUnavailable`` that is not this class comes from the launch library: its
    launch is not finished, and nothing repeats it automatically."""


class CodeRuns(Protocol):
    """Which run's staged code a run uses, and the one-time binding of a run to it."""

    def resolve(self, run_id: str) -> str:
        """The code run of the run: the bound one, or the run itself while nothing is bound. A binding that cannot be read raises ``InputNotReady``."""
        ...

    def stored(self, run_id: str) -> str | None:
        """The code run the run is bound to, or None while nothing is bound. A binding that cannot be read raises ``InputNotReady``."""
        ...

    def bind(self, run_id: str, code_run: str) -> None:
        """Bind the run to ``code_run`` once. The same binding again is accepted, another one raises ``LaunchRefused``."""
        ...


class S3CodeRuns:
    """The binding of a run to its code run as a write-once object ``lakehouse/run_stage/<run>/inputs/code_run.json``.

    A missing object is the run's own code. Any other failure to read it is never read as missing. The write sends ``IfNoneMatch="*"``, so the first binding
    stands.
    """

    def __init__(self, client: Any, bucket: str) -> None:
        self.client = client
        self.bucket = bucket

    @staticmethod
    def key(run_id: str) -> str:
        return f"lakehouse/run_stage/{run_id}/inputs/code_run.json"

    def stored(self, run_id: str) -> str | None:
        try:
            body = self.client.get_object(Bucket=self.bucket, Key=self.key(run_id))["Body"].read(256)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "NoSuchKey":
                return None
            raise InputNotReady(f"the code run binding of {run_id} is unreadable") from None
        except BotoCoreError:
            raise InputNotReady(f"the code run binding of {run_id} is unreadable") from None
        try:
            document = json.loads(body)
            code_run = document["code_run"] if isinstance(document, dict) and set(document) == {"code_run"} else None
        except ValueError:
            code_run = None
        if not isinstance(code_run, str) or not RUN_ID.fullmatch(code_run):
            raise LaunchRefused(f"the code run binding of {run_id} is malformed")
        return code_run

    def resolve(self, run_id: str) -> str:
        return self.stored(run_id) or run_id

    def bind(self, run_id: str, code_run: str) -> None:
        body = json.dumps({"code_run": code_run}, sort_keys=True).encode()
        try:
            self.client.put_object(Bucket=self.bucket, Key=self.key(run_id), Body=body, IfNoneMatch="*")
            return
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            if code != "PreconditionFailed":
                if code in RETRYABLE_CODES:
                    raise InputNotReady(f"the code run binding of {run_id} is not written: {code}") from None
                raise LaunchRefused(f"the code run binding of {run_id} was refused: {code or 'ClientError'}") from None
        except BotoCoreError:
            raise InputNotReady(f"the code run binding of {run_id} is not confirmed") from None
        if self.stored(run_id) != code_run:
            raise LaunchRefused(f"run {run_id} is already bound to the code of another run")


@dataclass(frozen=True)
class PreflightSettings:
    """The catalog and MLflow addresses the jobs receive. Both are private addresses of the container host."""

    polaris_uri: str
    mlflow_uri: str


def load_tables(root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """The closed job table (the shared jobs and the direct launch's own jobs, each checked against the schema) and the host table: what each job
    needs of the host and the family of each DAG."""
    validator = Draft202012Validator(json.loads((root / JOBS_SCHEMA).read_text()))
    document: dict[str, Any] = json.loads((root / JOBS_FILE).read_text())
    direct: dict[str, Any] = json.loads((root / DIRECT_JOBS_FILE).read_text())
    for name, found in ((JOBS_FILE.name, document), (DIRECT_JOBS_FILE.name, direct)):
        errors = sorted(validator.iter_errors(found), key=str)
        if errors:
            raise LaunchRefused(f"{name}: {errors[0].message}")
    overlap = sorted((set(document["jobs"]) & set(direct["jobs"])) | (set(document["steps"]) & set(direct["steps"])))
    if overlap:
        raise LaunchRefused(f"{DIRECT_JOBS_FILE.name} repeats {overlap} of {JOBS_FILE.name}")
    document = {**document, "jobs": {**document["jobs"], **direct["jobs"]}, "steps": {**document["steps"], **direct["steps"]}}
    jobs = document["jobs"]
    unknown = sorted({job for job in document["steps"].values() if job not in (None, GATE) and job not in jobs})
    unknown += sorted(row["after"] for row in jobs.values() if "after" in row and row["after"] not in jobs)
    if unknown:
        raise LaunchRefused(f"{JOBS_FILE.name}: steps name unknown jobs {unknown}")
    host: dict[str, Any] = json.loads((root / HOST_FILE).read_text())
    for name, need in host["jobs"].items():
        if "like" in need:  # a family with its own host definition, records and scratch folder and the containers of another job
            if need["like"] not in jobs or name in jobs:
                raise LaunchRefused(f"{HOST_FILE.name}: {name} is like {need['like']}, which is not a job of {JOBS_FILE.name}, or {name} is one itself")
            jobs[name] = jobs[need["like"]]
    if set(host["jobs"]) != set(jobs) or not set(host["dags"].values()) <= set(jobs) | {CHAIN}:
        raise LaunchRefused(f"{HOST_FILE.name} names other jobs than {JOBS_FILE.name}")
    for job, need in host["jobs"].items():
        reserved = (need.get("cpu_units"), need.get("memory_mib"))
        if need.get("reserve"):
            ok = all(isinstance(value, int) and value > 0 for value in reserved) and reserved[0] <= JOB_ROOM_CPU_UNITS and reserved[1] <= JOB_ROOM_MEMORY_MIB
        else:
            ok = reserved == (JOB_ROOM_CPU_UNITS, JOB_ROOM_MEMORY_MIB)
        if not ok:
            rule = "with reserve may reserve at most" if need.get("reserve") else "without reserve reserves exactly"
            raise LaunchRefused(
                f"{HOST_FILE.name}: the {job} row says {reserved[0]} units and {reserved[1]} MiB, but a family {rule} "
                f"{JOB_ROOM_CPU_UNITS} units and {JOB_ROOM_MEMORY_MIB} MiB (the job room)"
            )
        if "containers" not in need and "log_container" not in need:
            continue
        containers = need.get("containers")
        known = {name for names in jobs[job]["containers"].values() for name in names}
        if (
            not isinstance(containers, dict)
            or not containers
            or not all(isinstance(names, list) and set(names) <= known for names in containers.values())
            or need.get("log_container") not in containers
        ):
            raise LaunchRefused(f"{HOST_FILE.name}: the host containers of the {job} job need a log container and only the variables of its job")
    return document, host


def family_of(steps: Mapping[str, str | None], host: Mapping[str, Any], template: str, dag_id: str) -> str | None:
    """The host family a step of ``dag_id`` launches in: the job of its template, or the DAG's own family when the host table makes that family like the
    job (a model set's features DAG launches in its set's family, so six sets run beside each other)."""
    job = steps[template]
    family = host["dags"].get(dag_id)
    if job is not None and family not in (None, CHAIN, job) and host["jobs"].get(family, {}).get("like") == job:
        return str(family)
    return job


class Preflight:
    """The gates applied before a job starts, and the launch request they produce.

    ``prepare`` takes the request of one DAG step and returns the job to launch, or None for a control step. A request that fails a gate raises
    ``LaunchRefused`` before any record is written or any ECS call is made. A staged input that is not there yet, or a service that throttles, raises
    ``InputNotReady``: a later identical call can succeed. A ``LaunchUnavailable`` of the launch store (the history of a run and job) is not caught: it means
    a launch is not finished.
    """

    def __init__(
        self,
        *,
        table: Mapping[str, Any],
        host: Mapping[str, Any],
        settings: PreflightSettings,
        code_digest: Callable[[str], str],
        store: LaunchStore,
        code_runs: CodeRuns,
        clock: Callable[[], float],
        host_capacity: str,
        task_definition: Callable[[str], Mapping[str, Any]],
        host_job_cap: Callable[[], str | None],
        host_launch_hold: Callable[[], str | None] | None = None,
        bronze_request: Callable[[str], dict[str, str]] | None = None,
        current_code_run: Callable[[], str | None] | None = None,
        read_record: Callable[[str], bytes | None] | None = None,
    ) -> None:
        self.jobs: Mapping[str, Any] = table["jobs"]
        self.steps: Mapping[str, str | None] = table["steps"]
        self.host = host
        self.settings = settings
        self.code_digest = code_digest
        self.store = store
        self.code_runs = code_runs
        self.clock = clock
        self.bronze_request = bronze_request
        self.current_code_run = current_code_run
        self.host_capacity = host_capacity
        self.task_definition = task_definition
        self.host_job_cap = host_job_cap
        self.host_launch_hold = host_launch_hold
        self.read_record = read_record

    def containers(self, job: str) -> tuple[dict[str, list[str]], str]:
        """The containers of the job's host task definition and the one whose log is read: the host table's where it names them, else the job table's."""
        need, row = self.host["jobs"][job], self.jobs[job]
        return need.get("containers", row["containers"]), need.get("log_container", row["log_container"])

    @staticmethod
    def read(call: Callable[..., Any], *arguments: Any) -> Any:
        try:
            return call(*arguments)
        except TaskUnavailable as error:
            raise InputNotReady(str(error)) from None
        except TaskRefused as error:
            raise LaunchRefused(str(error)) from None

    def prepare(self, request: SubmissionRequest) -> JobSpec | None:
        if request.template not in self.steps:
            raise LaunchRefused(f"{request.template} has no ECS job")
        job = family_of(self.steps, self.host, request.template, request.dag_id)
        if job == GATE:
            self.record_chain(request.run_id)
            return None
        if job is None:
            return None
        if request.mode != JOB_MODE:
            raise LaunchRefused(f"{request.template} runs on AWS in real mode only")
        if self.jobs[job].get("needs_publisher_run_request"):
            require_publisher_request(request)  # first: a malformed request is refused before any hold, cap, staged-input or launch-record read
        # Both settings come from the environment: they are checked before every gate that reads the cloud, so neither is masked by a read.
        cap = self.host_cap(job, self.require_capacity(job))
        self.require_no_hold(job)
        row = self.jobs[job]
        # A model set builds from the passed features job of its base run (the base family), and a drift check reads the passed features job of its model run,
        # which is in the family of its set. A drift check of a model outside the contract has no such job: drift_model refuses it by name below.
        drift_features = f"features_{(request.training or {}).get('model')}"
        if request.dag_id in FEATURE_DAGS:
            after = self.steps[request.template]
        elif request.dag_id == DRIFT_DAG:
            after = drift_features if drift_features in self.jobs else None
        else:
            after = row.get("after")
        if after:
            self.require_passed(job, after, request.input_run_id)
        if row.get("needs_training_parameters"):
            try:
                if request.dag_id == "research_dag" and request.template in CHAIN_TEMPLATES:
                    chain_parameters_for(request.template, request.training or {}, LAUNCH_LANES)
                else:
                    parameters_for(request.dag_id, request.training or {}, LAUNCH_LANES)
            except ValueError as error:
                raise LaunchRefused(str(error)) from None
        if request.dag_id == DRIFT_DAG:
            drift_model(request)
        research_code = None
        if request.template == "research" and request.dag_id == "research_dag":
            # Research compares the models' runs, so it runs the code the first of them trained with.
            research_code = self.code_runs.resolve(trained_run_id(str((request.training or {})["base_run"]), PROMOTION_MODELS[0]))
        bind = research_code is not None
        if research_code is None and row.get("needs_publisher_run_request"):
            research_code, bind = self.publisher_code_run(request)
        spec = self.spec(request, job, research_code)
        revision, job_memory = self.checked_revision(job, cap)
        if research_code is not None and bind:  # the only write of a job step, after every refusal
            self.code_runs.bind(request.run_id, research_code)
        # ECS places the task by the revision's task memory, so the launcher's fit check asks for exactly that: the host job cap for a family without
        # reserve, its own memory for a family that reserves its own size. The fit probe gives no more than the host job cap.
        need = replace(spec.need, memory_mib=job_memory)
        return replace(spec, task_definition=revision, need=need)

    def publisher_code_run(self, request: SubmissionRequest) -> tuple[str | None, bool]:
        """The code run of a publisher run and whether the run still needs binding to it.

        A scheduled run stages no code of its own. A run with a binding keeps it, whatever the current-code parameter names now. A retry takes the code run
        of its original monthly run (the binding of that run, or the run itself when it ran its own code) and is bound to it once. A run with none takes the
        code run the parameter names (the code CI staged last) and is bound to it once, so every later attempt of the run uses the same code. Where no
        parameter is configured or none is set (development), the run is bound once to the code staged under its own run ID, so a parameter that appears later
        never changes it; production refuses a missing parameter by name. A parameter that cannot be read waits, and one that is
        not a run ID refuses the run."""
        stored = self.code_runs.stored(request.run_id)
        if stored is not None:
            return stored, False
        if isinstance(request.publisher, Mapping) and request.publisher.get("kind") == RETRY_KIND:
            return self.code_runs.resolve(str(request.publisher["original_run"])), True
        if self.current_code_run is None:
            return request.run_id, True
        value = self.read(self.current_code_run)
        if value is None:
            return request.run_id, True
        if not RUN_ID.fullmatch(value):
            raise LaunchRefused("the current code run parameter is not a staged run ID")
        return value, True

    def record_chain(self, bronze_run: str) -> None:
        """Bind each layer run that derives from the bronze run, and for each model the runs of its chain from the base run (its set's features run, the
        drift check of that set and the training run the check may start), to the bronze run's staged code."""
        run = bronze_run
        for layer in CHAIN_LAYERS:
            run = derived_run_id(run, layer)
            self.code_runs.bind(run, bronze_run)
            if layer == "features":
                for model in MODELS:
                    features = model_features_run_id(run, model)
                    drift = model_drift_run_id(features)
                    for chained in (features, drift, model_training_run_id(drift, model)):
                        self.code_runs.bind(chained, bronze_run)

    def require_passed(self, job: str, after: str, input_run: str | None) -> None:
        """Refuse a job unless the launch of the previous job in the run it publishes from ended with every acceptance check passed."""
        if input_run is None:
            raise LaunchRefused(f"the {job} job needs the {after} run it publishes from")
        claim = self.store.run_record(input_run, after, "claim")
        end = None
        if claim is not None:
            end = {record["event"]: record for record in self.store.records(input_run, after, [claim["attempt"]])}.get("end")
        if end is None or end.get("outcome") != "succeeded":
            raise LaunchRefused(f"the {job} job needs the {after} job in run {input_run} to have passed")

    def fixed_values(self, request: SubmissionRequest, job: str) -> dict[str, str]:
        """The values fixed once for a launch: the checked bronze request read once, the publisher's checked run kind and capture root, the serving request
        derived from the closed launch request, and the absolute deadline of the task."""
        row = self.jobs[job]
        names = {name for variables in self.containers(job)[0].values() for name in variables}
        values: dict[str, str] = {}
        readers = (("needs_bronze_request", self.bronze_request, "bronze"),)
        for needed, reader, label in readers:
            if row.get(needed):
                if reader is None:
                    raise LaunchRefused(f"{label}_request_not_configured")
                values |= self.read(reader, request.run_id)
        if row.get("needs_publisher_run_request"):
            values |= require_publisher_request(request)
        if row.get("needs_serving_parameters"):
            if self.read_record is None:
                raise LaunchRefused("serving_records_not_configured")
            values |= self.read(
                serving_values,
                request.training or {},
                request.input_run_id,
                int(row["canary_percent"]),
                self.read_record,
                int(row["canary_ttl_hours"]),
            )
        if DEADLINE_VARIABLE in names:
            values[DEADLINE_VARIABLE] = str(int(self.clock()) + int(row["deadline_seconds"]))
        return values

    def require_capacity(self, job: str) -> str:
        """The current usable-memory inputs (``HAI_LAUNCH_HOST_CAPACITY``), checked before every other job gate, so before any record or cloud call."""
        capacity = self.host_capacity
        if CAPACITY_FORM.fullmatch(capacity) is None:
            raise LaunchRefused(f"the host capacity {capacity[:200]!r} is not in the rendered form usable_memory_mib=<MiB> system_reserve_mib=<MiB>")
        return capacity

    def checked_revision(self, job: str, cap: int) -> tuple[str, int]:
        """Host launch capacity binding (owner A, Nov 10 2026): the ARN and the job memory of the latest active revision of the job's host
        definition, described once, after checking that it is bound to the current usable-memory cap. The services stack renders the cap's inputs
        into ``HAI_LAUNCH_HOST_CAPACITY`` (``usable_memory_mib=<n> system_reserve_mib=<n>``). The jobs stack labels the init container
        of every host revision it registers with the binding it was sized from (label ``hai.host_capacity``: the job memory, the same two inputs and the
        service reservations, from the services output ``host_capacity_binding``). A revision without the label (registered before the job memory came
        from ``host_usable_memory_mib``), bound to other inputs, with a label whose job memory is above the inputs less the reservations, or with task
        memory other than that job memory is refused. The launch runs the returned ARN, so a revision registered after this check never runs in its
        place."""
        capacity = self.require_capacity(job)
        found = self.read(self.task_definition, job)
        name = f"{found.get('family', '')}:{found.get('revision', '')}"
        init = next((item for item in found.get("containerDefinitions") or [] if isinstance(item, dict) and item.get("name") == "init"), {})
        labels = init.get("dockerLabels")
        bound = str(labels.get(CAPACITY_LABEL) or "") if isinstance(labels, dict) else ""
        again = "Render and apply the jobs stack so it registers a revision bound to the current capacity, then run again"
        if found.get("status") != "ACTIVE":
            raise LaunchRefused(f"the latest revision {name} of the {job} host job is not active. {again}")
        if not bound:
            raise LaunchRefused(
                f"the latest active revision {name} of the {job} host job carries no host capacity binding (label {CAPACITY_LABEL} on its init "
                f"container): it was registered before the job memory came from host_usable_memory_mib. {again}"
            )
        binding = BINDING_FORM.fullmatch(bound)
        if binding is None or binding["inputs"] != capacity:
            raise LaunchRefused(
                f"the latest active revision {name} of the {job} host job is bound to {bound[:200]}, but the current host capacity is {capacity}. {again}"
            )
        if int(binding["job"]) > int(binding["usable"]) - int(binding["reserve"]) - int(binding["services"]):
            raise LaunchRefused(f"the binding {bound[:200]} of the latest active revision {name} of the {job} host job gives a job more than the room. {again}")
        if str(found.get("memory", "")) != binding["job"]:
            raise LaunchRefused(
                f"the latest active revision {name} of the {job} host job has {str(found.get('memory', ''))[:20]} MiB of task memory, but its binding "
                f"gives a job {binding['job']} MiB. {again}"
            )
        self.require_within_cap(job, name, int(binding["job"]), cap)
        return str(found.get("taskDefinitionArn", "")), int(binding["job"])

    def require_no_hold(self, job: str) -> None:
        """Launch hold during services applies (owner A, Nov 10 2026): refuse every host launch while a services apply holds launches, and fail
        closed while no hold reader is wired or the hold parameter is missing."""
        if self.host_launch_hold is None:
            raise LaunchRefused(f"no launch hold reader is configured, so no {job} host job launches")
        reason = held(self.read(self.host_launch_hold))
        if reason is not None:
            raise LaunchRefused(
                f"{reason}, so no {job} host job launches while a services apply runs. When the apply finished, wait for its owner command to clear "
                "the hold, or resolve a failed apply with python -m scripts.infrastructure.services_apply resolve, then run again"
            )

    def host_cap(self, job: str, capacity: str) -> int:
        """Host job cap at launch (Nov 11 2026): the services stack renders the cap into the Airflow environment (``HAI_LAUNCH_HOST_JOB_CAP_MIB``, from
        its variable ``host_job_cap_mib``). Refuse a host launch, before any cloud call, while the cap is missing or invalid, and while it is above the
        memory the host leaves: the usable memory less the system reserve, both from the rendered capacity. The service reservations it must also fit
        are checked by the services stack and by the binding of each revision."""
        raw = self.read(self.host_job_cap)
        if raw is None:
            raise LaunchRefused(
                f"the host job cap ({HOST_JOB_CAP_SETTING}) is missing, so no {job} host job launches. "
                "Set host_job_cap_mib in the services stack and apply it, then run again"
            )
        cap = cap_value(raw)
        if cap is None:
            raise LaunchRefused(
                f"the host job cap {raw[:40]!r} is not a whole number of MiB above 0. Set host_job_cap_mib in the services stack, apply it, then run again"
            )
        inputs = CAPACITY_FORM.fullmatch(capacity)
        usable, reserve = (int(item.split("=")[1]) for item in capacity.split()) if inputs else (0, 0)
        if cap > usable - reserve:
            raise LaunchRefused(
                f"the host job cap {cap} MiB ({HOST_JOB_CAP_SETTING}) is above the {usable - reserve} MiB the host leaves ({usable} MiB usable less the "
                f"{reserve} MiB system reserve), so no {job} host job launches. Lower host_job_cap_mib in the services stack, apply it, then run again"
            )
        return cap

    @staticmethod
    def require_within_cap(job: str, name: str, job_memory: int, cap: int) -> None:
        """Refuse a revision whose binding gives a job more memory than the host job cap. The jobs stack learns the cap at its next render, so a lowered
        cap refuses the old revision instead of overfilling the host."""
        if job_memory > cap:
            raise LaunchRefused(
                f"the latest active revision {name} of the {job} host job gives a job {job_memory} MiB, above the current host job cap of {cap} MiB. "
                "Render and apply the jobs stack so it registers a revision sized from the current services, then run again"
            )

    def spec(self, request: SubmissionRequest, job: str, code_run: str | None = None) -> JobSpec:
        """The launch request of a job: the variables of each container, the bound inputs, the deadline, what the host must have free and the
        acceptance. ``prepare`` adds the checked revision of the job's host definition."""
        row = self.jobs[job]
        code_run = code_run or self.code_runs.resolve(request.run_id)
        fixed = self.fixed_values(request, job)
        cache: dict[str, str] = {}

        def digest() -> str:
            if "digest" not in cache:
                cache["digest"] = self.read(self.code_digest, code_run)
            return cache["digest"]

        training = json.dumps({**(request.training or {}), "mlflow": "aws"}, sort_keys=True, separators=(",", ":"))
        names_a_model = bool(row.get("needs_training_parameters")) or request.dag_id == DRIFT_DAG
        values: dict[str, Callable[[], str]] = {
            "RUN_ID": lambda: request.run_id,
            "CODE_RUN_ID": lambda: code_run,
            "CODE_SHA256": digest,
            "TRAINING_PARAMETERS": lambda: training,
            "POLARIS_URI": lambda: self.settings.polaris_uri,
            "MLFLOW_TRACKING_URI": lambda: self.settings.mlflow_uri,
            "INPUT_RUN_ID": lambda: request.input_run_id or "latest",
            "FEATURES_RUN": lambda: features_run_of(request),
            "FEATURE_SET": lambda: drift_model(request) if request.dag_id == DRIFT_DAG else feature_set_of(request.dag_id),
            "PHASE": lambda: phase_of(request.template),
            **{name: partial(fixed.__getitem__, name) for name in (*BRONZE_VARIABLES, *SERVING_VARIABLES, *PUBLISHER_VARIABLES, DEADLINE_VARIABLE)},
        }
        containers, log_container = self.containers(job)
        environment = {container: {name: values[name]() for name in names} for container, names in containers.items()}
        uses_code = any("CODE_SHA256" in names for names in containers.values())
        request_hash = fixed.get("BRONZE_REQUEST_SHA256") or fixed.get("SERVING_REQUEST_SHA256") or fixed.get("PUBLISHER_LAUNCH_SHA256")
        if not request_hash:
            request_hash = hashlib.sha256((training if names_a_model else "").encode()).hexdigest()
        need = self.host["jobs"][job]
        return JobSpec(
            run_id=request.run_id,
            job=job,
            attempt=request.attempt,
            environment=environment,
            bound={"code_digest": digest() if uses_code else NO_STAGED_CODE, "request_hash": request_hash, "input_run": request.input_run_id or "latest"},
            deadline_seconds=int(row["deadline_seconds"]),
            need=Resources(cpu_units=need["cpu_units"], memory_mib=need["memory_mib"], scratch_bytes=need["scratch_gib"] * 2**30),
            acceptance=tuple(row.get("acceptance", ())),
            log_container=log_container,
            log_prefix=str(self.host["jobs"][job].get("like", job)),  # the containers of a model set log under their base job's stream prefix
        )
