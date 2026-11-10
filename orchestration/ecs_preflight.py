"""The gates of the job broker, as a library for the Airflow launch mode, and the staged-input readers both share.

``Preflight.prepare`` turns the request of one DAG step into the ``JobSpec`` of the direct ECS launch library (``orchestration/ecs_launch.py``). It ports the
broker's gates: real mode, the previous job's success (``after``), the training parameters, the promotion approval, the staged
bronze and serving requests, the staged code digest and the run's code run. Each gate refuses before any record or ECS call. The bound inputs of a run (the
code digest, the request hash and the input run) go into the spec, and the launch library holds every later attempt of the run and job to them.
The readers at the top are the broker's own: ``scripts/orchestration/ecs_broker.py`` imports them from here, so one reading of the staged inputs serves both.
Signing the first silver and gold publishes is the owner's manual step, not a gate of this path: ``ssm_signed_reader`` and ``CLOSED`` stay only for the
broker, which still checks the signed record.
The host table (``config/orchestration/host_jobs.json``) may give a job its own containers and log container, where its host task definition differs from the
Fargate one the broker starts (the metadata job).
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from functools import partial
from pathlib import Path
from typing import Any, Protocol

from botocore.exceptions import BotoCoreError, ClientError
from jsonschema import Draft202012Validator

from orchestration import serving_contract
from orchestration.ecs_adapter import TaskRefused, TaskUnavailable
from orchestration.ecs_launch import RETRYABLE_CODES, JobSpec, LaunchRefused, LaunchStore, LaunchUnavailable, Resources
from orchestration.interface import SubmissionRequest
from orchestration.training_approvals import PROMOTION_KEYS
from orchestration.training_contract import LAUNCH_LANES, parameters_for, training_run_id

JOBS_FILE = Path("config/orchestration/ecs_broker_jobs.json")
JOBS_SCHEMA = Path("config/orchestration/ecs_broker_jobs.schema.json")
HOST_FILE = Path("config/orchestration/host_jobs.json")
TABLE_MAP_FILE = Path("config/lakehouse/bronze_tables.json")
BRONZE_REQUEST_BYTES = 4096
BRONZE_REQUEST_KEYS = {"tables", "target", "dry_run", "inventory_sha256"}
SERVING_REQUEST_BYTES = 4096
APPROVAL_BYTES = 4096
APPROVAL_DAY = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
DIGEST = re.compile(r"[a-f0-9]{64}")
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
BRONZE_VARIABLES = ("BRONZE_TABLES", "BRONZE_TARGET", "BRONZE_DRY_RUN", "BRONZE_REQUEST_SHA256", "BRONZE_INVENTORY_SHA256")
SERVING_VARIABLES = ("SERVING_REQUEST", "SERVING_REQUEST_SHA256")
DEADLINE_VARIABLE = "HAI_DEADLINE_UTC"
GATE = "@gate"
CHAIN = "chain"
CHAIN_LAYERS = ("silver", "gold", "features", "drift")
CLOSED = "the chain is closed until the first silver and gold publishes are signed"  # the broker's refusal; the direct path has no signed gate
JOB_MODE = "real"
NO_STAGED_CODE = "0" * 64  # the bound code digest of a job that fetches no staged code


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


def derived_run_id(parent_run: str, layer: str) -> str:
    """The run ID of a chain layer: the parent's time stamp and eight hex digits of a hash. ``dags/bronze_chain_dag.py`` derives the same IDs."""
    return f"{parent_run[:16]}-{hashlib.sha256(f'{layer}:{parent_run}'.encode()).hexdigest()[:8]}"


def serving_request_reader(s3: S3Reader, bucket: str) -> Callable[[str], dict[str, str]]:
    """Read and check the run's staged ``serving_request.json`` and return the values of the serve container.

    The file is the closed record of ``orchestration.serving_contract``: the HAI type, the mode, the candidate version and the percent. A file that is not
    staged yet is retryable, a file that fails a check is refused. The container receives the checked request and never reads the bucket for it.
    """

    def read(run_id: str) -> dict[str, str]:
        try:
            body = s3.get_object(Bucket=bucket, Key=f"lakehouse/run_stage/{run_id}/inputs/{serving_contract.REQUEST_FILE}")["Body"].read(
                SERVING_REQUEST_BYTES + 1
            )
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "NoSuchKey":
                raise TaskUnavailable("serving_request_not_staged") from None
            raise TaskUnavailable("serving_request_unreadable") from None
        except BotoCoreError:
            raise TaskUnavailable("serving_request_unreadable") from None
        if len(body) > SERVING_REQUEST_BYTES:
            raise TaskRefused("serving_request_too_large")
        try:
            document = json.loads(body)
            if not isinstance(document, dict):
                raise ValueError("the request is not an object")
            checked = serving_contract.parameters_for("serving_deploy_dag", document)
        except ValueError as error:
            raise TaskRefused(f"serving_request_refused: {error}") from None
        return {"SERVING_REQUEST": json.dumps(checked, sort_keys=True, separators=(",", ":")), "SERVING_REQUEST_SHA256": hashlib.sha256(body).hexdigest()}

    return read


def ssm_signed_reader(ssm: Any, name: str) -> Callable[[], str | None]:
    """Read the record of the signed first publishes from a Parameter Store value, or None while the parameter does not exist."""

    def read() -> str | None:
        try:
            return str(ssm.get_parameter(Name=name)["Parameter"]["Value"])
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "ParameterNotFound":
                return None
            raise TaskUnavailable("signed_record_unreadable") from None
        except BotoCoreError:
            raise TaskUnavailable("signed_record_unreadable") from None

    return read


class InputNotReady(LaunchUnavailable):
    """A staged input of the run is not readable yet, or the store of the staged inputs throttled. This is the preflight's own wait: a later identical call
    can succeed, and ``airflow_steps`` repeats the step for a bounded time. A ``LaunchUnavailable`` that is not this class comes from the launch library: its
    launch is not finished, and nothing repeats it automatically."""


class CodeRuns(Protocol):
    """Which run's staged code a run uses, and the one-time binding of a run to it."""

    def resolve(self, run_id: str) -> str:
        """The code run of the run: the bound one, or the run itself while nothing is bound. A binding that cannot be read raises ``InputNotReady``."""
        ...

    def bind(self, run_id: str, code_run: str) -> None:
        """Bind the run to ``code_run`` once. The same binding again is accepted, another one raises ``LaunchRefused``."""
        ...


class S3CodeRuns:
    """The binding of a run to its code run as a write-once object ``lakehouse/run_stage/<run>/inputs/code_run.json``.

    A missing object is the run's own code. Any other failure to read it is never read as missing. The write sends ``IfNoneMatch="*"``, so the first binding
    stands: this is the broker's ``chains/<run>.json`` file without a state folder.
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
    """The closed job table (checked against its schema) and the host table: what each job needs of the host and the family of each DAG."""
    document: dict[str, Any] = json.loads((root / JOBS_FILE).read_text())
    errors = sorted(Draft202012Validator(json.loads((root / JOBS_SCHEMA).read_text())).iter_errors(document), key=str)
    if errors:
        raise LaunchRefused(f"{JOBS_FILE.name}: {errors[0].message}")
    jobs = document["jobs"]
    unknown = sorted({job for job in document["steps"].values() if job not in (None, GATE) and job not in jobs})
    unknown += sorted(row["after"] for row in jobs.values() if "after" in row and row["after"] not in jobs)
    if unknown:
        raise LaunchRefused(f"{JOBS_FILE.name}: steps name unknown jobs {unknown}")
    host: dict[str, Any] = json.loads((root / HOST_FILE).read_text())
    if set(host["jobs"]) != set(jobs) or not set(host["dags"].values()) <= set(jobs) | {CHAIN}:
        raise LaunchRefused(f"{HOST_FILE.name} names other jobs than {JOBS_FILE.name}")
    for job, need in host["jobs"].items():
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


class Preflight:
    """The gates the job broker applied before it started a job, and the launch request they produce.

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
        approvals: Callable[[str], str | None],
        store: LaunchStore,
        code_runs: CodeRuns,
        clock: Callable[[], float],
        bronze_request: Callable[[str], dict[str, str]] | None = None,
        serving_request: Callable[[str], dict[str, str]] | None = None,
    ) -> None:
        self.jobs: Mapping[str, Any] = table["jobs"]
        self.steps: Mapping[str, str | None] = table["steps"]
        self.host = host
        self.settings = settings
        self.code_digest = code_digest
        self.approvals = approvals
        self.store = store
        self.code_runs = code_runs
        self.clock = clock
        self.bronze_request = bronze_request
        self.serving_request = serving_request

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
        job = self.steps[request.template]
        if job == GATE:
            self.record_chain(request.run_id)
            return None
        if job is None:
            return None
        if request.mode != JOB_MODE:
            raise LaunchRefused(f"{request.template} runs on AWS in real mode only")
        row = self.jobs[job]
        if row.get("after"):
            self.require_passed(job, row["after"], request.input_run_id)
        if row.get("needs_training_parameters"):
            try:
                parameters_for(request.dag_id, request.training or {}, LAUNCH_LANES)
            except ValueError as error:
                raise LaunchRefused(str(error)) from None
        if row.get("approval") == "promotion":
            self.require_promotion_approval(request)
        if request.template == "drift_run":
            self.code_runs.bind(training_run_id(request.run_id), self.code_runs.resolve(request.run_id))
        return self.spec(request, job)

    def record_chain(self, bronze_run: str) -> None:
        """Bind each layer run that derives from the bronze run, and the training run of the last one, to the bronze run's staged code."""
        run = bronze_run
        for layer in CHAIN_LAYERS:
            run = derived_run_id(run, layer)
            self.code_runs.bind(run, bronze_run)
        self.code_runs.bind(training_run_id(run), bronze_run)

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

    def require_promotion_approval(self, request: SubmissionRequest) -> None:
        """Refuse a promotion unless the owner staged a closed approval record for exactly this run, model, version and operation."""
        raw = self.read(self.approvals, request.run_id)
        if raw is None:
            raise LaunchRefused("a promotion needs the owner's approval record in the run's staged inputs")
        parameters = parameters_for(request.dag_id, request.training or {}, LAUNCH_LANES)
        try:
            body = json.loads(raw) if len(raw) <= APPROVAL_BYTES else None
        except ValueError:
            body = None
        today = datetime.fromtimestamp(self.clock(), UTC).date()
        problem = None
        if not isinstance(body, dict) or set(body) != PROMOTION_KEYS:
            problem = "the approval is not a closed JSON object"
        elif body["run_id"] != request.run_id or body["dag_id"] != request.dag_id or body["scope"] != parameters["promotion_mode"]:
            problem = "the approval names another run, DAG or operation"
        elif body["hai_type"] != parameters["hai_type"] or body["model_version"] != parameters["model_version"]:
            problem = "the approval names another HAI type or model version"
        else:
            day = body["approved_on"]
            try:
                valid = isinstance(day, str) and APPROVAL_DAY.fullmatch(day) is not None and date.fromisoformat(day) <= today
            except ValueError:
                valid = False
            if not valid:
                problem = "the approval date is invalid or in the future"
        if problem is not None:
            raise LaunchRefused(problem)

    def fixed_values(self, request: SubmissionRequest, job: str) -> dict[str, str]:
        """The values fixed once for a launch: the checked bronze and serving requests, read once, and the absolute deadline of the task."""
        row = self.jobs[job]
        names = {name for variables in self.containers(job)[0].values() for name in variables}
        values: dict[str, str] = {}
        for needed, reader, label in (("needs_bronze_request", self.bronze_request, "bronze"), ("needs_serving_request", self.serving_request, "serving")):
            if row.get(needed):
                if reader is None:
                    raise LaunchRefused(f"{label}_request_not_configured")
                values |= self.read(reader, request.run_id)
        if DEADLINE_VARIABLE in names:
            values[DEADLINE_VARIABLE] = str(int(self.clock()) + int(row["deadline_seconds"]))
        return values

    def spec(self, request: SubmissionRequest, job: str) -> JobSpec:
        """The launch request of a job: the variables of each container, the bound inputs, the deadline, what the host must have free and the acceptance."""
        row = self.jobs[job]
        code_run = self.code_runs.resolve(request.run_id)
        fixed = self.fixed_values(request, job)
        cache: dict[str, str] = {}

        def digest() -> str:
            if "digest" not in cache:
                cache["digest"] = self.read(self.code_digest, code_run)
            return cache["digest"]

        training = json.dumps({**(request.training or {}), "mlflow": "aws"}, sort_keys=True, separators=(",", ":"))
        values: dict[str, Callable[[], str]] = {
            "RUN_ID": lambda: request.run_id,
            "CODE_RUN_ID": lambda: code_run,
            "CODE_SHA256": digest,
            "TRAINING_PARAMETERS": lambda: training,
            "POLARIS_URI": lambda: self.settings.polaris_uri,
            "MLFLOW_TRACKING_URI": lambda: self.settings.mlflow_uri,
            "INPUT_RUN_ID": lambda: request.input_run_id or "latest",
            **{name: partial(fixed.__getitem__, name) for name in (*BRONZE_VARIABLES, *SERVING_VARIABLES, DEADLINE_VARIABLE)},
        }
        containers, log_container = self.containers(job)
        environment = {container: {name: values[name]() for name in names} for container, names in containers.items()}
        uses_code = any("CODE_SHA256" in names for names in containers.values())
        request_hash = fixed.get("BRONZE_REQUEST_SHA256") or fixed.get("SERVING_REQUEST_SHA256")
        if not request_hash:
            request_hash = hashlib.sha256((training if row.get("needs_training_parameters") else "").encode()).hexdigest()
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
            log_prefix=job,
        )
