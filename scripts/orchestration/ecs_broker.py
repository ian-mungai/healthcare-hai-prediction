"""The lean job broker for AWS: the host broker's HTTP interface and ledger, with ECS tasks in place of launcher containers.

Airflow DAGs reach it through ``orchestration.client.BrokerClient`` unchanged. It implements ``orchestration.interface.Submissions``.
A submission is a DAG step.
``config/orchestration/ecs_broker_jobs.json`` maps a step to a job, or to nothing for a step the broker answers itself (acquire, seal, release, gates).
A job is one ECS task per run and attempt. Its steps share one workspace, so the first step starts the task and the later steps join it.
The unchanged DAG runtime accepts ``mlflow=local`` only, so the broker accepts both lanes and passes ``aws`` to the training task.

The ledger line ``intent`` is written before ``RunTask``, and ``started`` after it. ``RunTask`` carries a token and a tag from the job key,
so a retry finds the task the first call made. The broker runs no monitor thread: every ``status`` call reads the task and, past the job deadline, stops it.
It imports no launcher and makes no Docker call. The listener binds loopback only and every request needs the bearer token.

    python -m scripts.orchestration.ecs_broker       # settings come from the BROKER_* environment variables
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import secrets
import signal
import sys
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Protocol

from botocore.exceptions import BotoCoreError, ClientError
from jsonschema import Draft202012Validator

from orchestration.ecs_adapter import EcsAdapter, EcsSettings, TaskRefused, TaskUnavailable, client_config
from orchestration.interface import Handle, State, Status, SubmissionRefused, SubmissionRequest
from orchestration.training_contract import JOB_LANES, parameters_for
from scripts.orchestration.ledger import Ledger, schema_validator

JOBS_FILE = Path("config/orchestration/ecs_broker_jobs.json")
JOBS_SCHEMA = Path("config/orchestration/ecs_broker_jobs.schema.json")
OPERATIONS = ("health", "submit", "status", "cancel", "logs", "record")
MAX_BODY_BYTES = 4096
HANDLER_TIMEOUT_SECONDS = 15.0
CONTENT_LENGTH = re.compile(r"[0-9]{1,18}")
SUBMISSION = re.compile(
    r"^([0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}):([a-z][a-z0-9_]{1,40}):(fixture|publish_fixture|real):([0-9]{1,2})(?::([0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}))?$"
)
DIGEST = re.compile(r"[a-f0-9]{64}")
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
GATE = "@gate"
CLOSED = "the chain is closed until the first silver and gold publishes are signed"
JOB_MODE = "real"
TIMEOUT_EXIT = 124
NO_CHECK_EXIT = 125
CANCEL_EXIT = 143
REDACTED = "[redacted]"
OUTCOME_STATE = {"succeeded": State.SUCCEEDED, "control": State.SUCCEEDED, "failed": State.FAILED, "timeout": State.FAILED, "cancelled": State.CANCELLED}


class BrokerError(Exception):
    """The broker's own configuration or table is unusable."""


class Unavailable(Exception):
    """A later identical call can succeed: the cloud throttled, the digest is not staged yet, or a task is not readable now."""


@dataclass(frozen=True)
class BrokerSettings:
    """The catalog and MLflow addresses the jobs receive. Both are private addresses of the container host."""

    polaris_uri: str
    mlflow_uri: str


def load_jobs(root: Path) -> dict[str, Any]:
    """The closed job table, checked against its schema. Every named job must exist."""
    document: dict[str, Any] = json.loads((root / JOBS_FILE).read_text())
    schema = json.loads((root / JOBS_SCHEMA).read_text())
    errors = sorted(Draft202012Validator(schema).iter_errors(document), key=str)
    if errors:
        raise BrokerError(f"{JOBS_FILE.name}: {errors[0].message}")
    unknown = sorted({job for job in document["steps"].values() if job not in (None, GATE) and job not in document["jobs"]})
    unknown += sorted(job["after"] for job in document["jobs"].values() if "after" in job and job["after"] not in document["jobs"])
    if unknown:
        raise BrokerError(f"{JOBS_FILE.name}: steps name unknown jobs {unknown}")
    return document


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


def parse_submission(submission_id: str) -> tuple[str, str, str, int]:
    """The run, template, mode and attempt of a handle's key."""
    found = SUBMISSION.fullmatch(submission_id)
    if found is None:
        raise SubmissionRefused("malformed submission ID")
    return found.group(1), found.group(2), found.group(3), int(found.group(4))


class EcsBroker:
    """Submissions recorded in a ledger and run as ECS tasks."""

    def __init__(
        self,
        state: Path,
        jobs: dict[str, Any],
        adapter: EcsAdapter,
        settings: BrokerSettings,
        code_digest: Callable[[str], str],
        signed_reader: Callable[[], str | None],
        clock: Callable[[], float],
    ) -> None:
        self.ledger = Ledger(state, "ecs_ledger_line.schema.json")
        self.requests = schema_validator("submission_request.schema.json")
        self.jobs = jobs["jobs"]
        self.steps = jobs["steps"]
        self.adapter = adapter
        self.settings = settings
        self.code_digest = code_digest
        self.signed_reader = signed_reader
        self.clock = clock
        self.secrets: set[str] = set()

    def redact(self, text: str) -> str:
        for secret in self.secrets:
            text = text.replace(secret, REDACTED)
        return text

    def job_of(self, request: SubmissionRequest) -> str | None:
        """The request's job, or None for a control step. Refuses what the table, the mode or the enabled families do not allow."""
        if request.template not in self.steps:
            raise SubmissionRefused(f"{request.template} has no ECS job")
        job = self.steps[request.template]
        if job == GATE:
            self.require_signed()
            return None
        if job is None:
            return None
        if request.mode != JOB_MODE:
            raise SubmissionRefused(f"{request.template} runs on AWS in real mode only")
        if job not in self.adapter.settings.families:
            raise SubmissionRefused(f"the {job} job is not enabled")
        if self.jobs[job].get("chain"):
            self.require_signed()
        after = self.jobs[job].get("after")
        if after:
            self.require_passed(job, after, request.input_run_id)
        if self.jobs[job].get("needs_training_parameters"):
            try:
                parameters_for(request.dag_id, request.training or {}, JOB_LANES)
            except ValueError as error:
                raise SubmissionRefused(str(error)) from None
        return job

    def require_signed(self) -> None:
        """Refuse a chain step until the record of the first signed silver and gold publishes names both runs."""
        try:
            raw = self.signed_reader()
        except TaskUnavailable as error:
            raise Unavailable(str(error)) from None
        try:
            record = json.loads(raw) if raw else None
        except ValueError:
            record = None
        if (
            not isinstance(record, dict)
            or set(record) != {"silver", "gold"}
            or not all(isinstance(run, str) and RUN_ID.fullmatch(run) for run in record.values())
        ):
            raise SubmissionRefused(CLOSED)

    def require_passed(self, job: str, after: str, input_run: str | None) -> None:
        """Refuse a job unless the run it publishes from finished its previous job with every acceptance check passed."""
        if input_run is None:
            raise SubmissionRefused(f"the {job} job needs the {after} run it publishes from")
        with self.ledger.locked(input_run):
            lines = self.ledger.read(input_run)
        if not any(line["event"] == "finished" and line.get("job") == after and line.get("outcome") == "succeeded" for line in lines):
            raise SubmissionRefused(f"the {job} job needs a {after} job that passed in run {input_run}")

    def variables(self, request: SubmissionRequest, names: list[str]) -> dict[str, str]:
        values = {
            "RUN_ID": lambda: request.run_id,
            "CODE_SHA256": lambda: self.code_digest(request.run_id),
            "TRAINING_PARAMETERS": lambda: json.dumps({**(request.training or {}), "mlflow": "aws"}, sort_keys=True, separators=(",", ":")),
            "POLARIS_URI": lambda: self.settings.polaris_uri,
            "MLFLOW_TRACKING_URI": lambda: self.settings.mlflow_uri,
            "INPUT_RUN_ID": lambda: request.input_run_id or "latest",
        }
        return {name: values[name]() for name in names}

    def append(self, line: dict[str, Any]) -> dict[str, Any]:
        return self.ledger.append(line)

    def identity(self, request: SubmissionRequest, job: str | None) -> dict[str, Any]:
        return {**request.as_dict(), "submission_id": request.submission_id, "job": job}

    def submit(self, request: SubmissionRequest) -> Handle:
        """Write the intent, then start or join the job once; a repeated request returns the same handle."""
        errors = sorted(self.requests.iter_errors(request.as_dict()), key=str)
        if errors:
            raise SubmissionRefused(f"request refused: {errors[0].message}")
        job = self.job_of(request)
        with self.ledger.locked(request.run_id):
            lines = self.ledger.submissions(request.run_id).get(request.submission_id)
            if lines is None:
                self.append({"event": "intent", **self.identity(request, job), **({"job_key": self.job_key(request, job)} if job else {})})
            elif {key: lines[0].get(key) for key in self.identity(request, job)} != self.identity(request, job):
                raise SubmissionRefused("the key was used with another caller identity")
            self.advance(request, job)
        return Handle(request.submission_id, request.container_name)

    @staticmethod
    def job_key(request: SubmissionRequest, job: str | None) -> str:
        return f"{request.run_id}:{job}:{request.attempt}"

    def advance(self, request: SubmissionRequest, job: str | None) -> None:
        """Move an unfinished submission forward one step; the caller holds the run's ledger lock."""
        lines = self.ledger.submissions(request.run_id)[request.submission_id]
        if lines[-1]["event"] != "intent":
            return
        identity = self.identity(request, job)
        if job is None:
            self.append({"event": "finished", **identity, "exit_code": 0, "outcome": "control"})
            return
        key = self.job_key(request, job)
        joined = next((line for line in self.ledger.read(request.run_id) if line["event"] == "started" and line.get("job_key") == key), None)
        if joined is not None:
            self.append({"event": "started", **identity, "job_key": key, "task_arn": joined["task_arn"], "job_started_at": joined["job_started_at"]})
            return
        try:
            names = self.jobs[job]["containers"]
            environment = {container: self.variables(request, list(variables)) for container, variables in names.items()}
            arn = self.adapter.start(job, key, environment)
        except TaskUnavailable as error:
            raise Unavailable(str(error)) from None
        except TaskRefused as error:
            self.append({"event": "failed_closed", **identity, "reason": self.redact(str(error))})
            raise SubmissionRefused(f"the cloud refused the {job} job: {error}") from None
        started = datetime.fromtimestamp(self.clock(), UTC).isoformat()
        self.append({"event": "started", **identity, "job_key": key, "task_arn": arn, "job_started_at": started})

    def request_of(self, handle: Handle) -> tuple[SubmissionRequest, str | None]:
        run, _, _, _ = parse_submission(handle.submission_id)
        lines = self.ledger.submissions(run).get(handle.submission_id)
        if lines is None:
            raise SubmissionRefused("unknown submission")
        first = lines[0]
        return SubmissionRequest.from_record(first), first["job"]

    def refresh(self, handle: Handle) -> dict[str, Any]:
        """Bring the submission up to date with the cloud and return its latest ledger line."""
        run, _, _, _ = parse_submission(handle.submission_id)
        with self.ledger.locked(run):
            request, job = self.request_of(handle)
            last = self.ledger.submissions(run)[handle.submission_id][-1]
            if last["event"] == "intent":
                self.advance(request, job)
                last = self.ledger.submissions(run)[handle.submission_id][-1]
            if last["event"] != "started":
                return last
            try:
                report = self.adapter.poll(last["task_arn"])
            except TaskUnavailable as error:
                raise Unavailable(str(error)) from None
            identity = self.identity(request, job)
            if report.state in ("pending", "running"):
                elapsed = self.clock() - datetime.fromisoformat(last["job_started_at"]).timestamp()
                if elapsed <= self.jobs[job]["deadline_seconds"]:
                    return last
                self.stop(last["task_arn"], "deadline")
                return self.append({"event": "finished", **identity, "exit_code": TIMEOUT_EXIT, "outcome": "timeout", "reason": "deadline"})
            if report.state == "gone":
                return self.append({"event": "failed_closed", **identity, "reason": "task_not_found"})
            missing = [name for name in self.jobs[job].get("acceptance", []) if dict(report.containers).get(name) != 0] if report.state == "succeeded" else []
            if missing:
                reason = f"acceptance_check_missing:{','.join(missing)}"
                return self.append({"event": "finished", **identity, "exit_code": NO_CHECK_EXIT, "outcome": "failed", "reason": reason})
            outcome = "succeeded" if report.state == "succeeded" else "failed"
            return self.append({"event": "finished", **identity, "exit_code": report.exit_code, "outcome": outcome, "reason": self.redact(report.reason)})

    def stop(self, task_arn: str, reason: str) -> None:
        try:
            self.adapter.stop(task_arn, reason)
        except TaskUnavailable as error:
            raise Unavailable(str(error)) from None
        except TaskRefused:
            return

    def status_of(self, handle: Handle, line: dict[str, Any]) -> Status:
        event = line["event"]
        if event == "intent":
            state = State.PENDING
        elif event == "started":
            state = State.RUNNING
        elif event == "failed_closed":
            state = State.FAILED_CLOSED
        else:
            state = OUTCOME_STATE[line["outcome"]]
        return Status(handle, state, line.get("exit_code"), None, None)

    def status(self, handle: Handle) -> Status:
        return self.status_of(handle, self.refresh(handle))

    def cancel(self, handle: Handle) -> Status:
        """Stop the submission's job. A finished submission is unchanged."""
        run, _, _, _ = parse_submission(handle.submission_id)
        with self.ledger.locked(run):
            request, job = self.request_of(handle)
            last = self.ledger.submissions(run)[handle.submission_id][-1]
            if last["event"] in ("finished", "failed_closed"):
                return self.status_of(handle, last)
            if last["event"] == "started":
                self.stop(last["task_arn"], "cancel")
            done = self.append({"event": "finished", **self.identity(request, job), "exit_code": CANCEL_EXIT, "outcome": "cancelled", "reason": "cancel"})
        return self.status_of(handle, done)

    def logs(self, handle: Handle, tail: int = 200) -> str:
        run, _, _, _ = parse_submission(handle.submission_id)
        request, job = self.request_of(handle)
        started = next((line for line in self.ledger.submissions(run)[handle.submission_id] if line["event"] == "started"), None)
        if job is None or started is None:
            return ""
        try:
            return self.redact(self.adapter.tail(started["task_arn"], self.jobs[job]["log_container"], job, tail))
        except TaskUnavailable as error:
            raise Unavailable(str(error)) from None

    def record(self, handle: Handle) -> dict[str, Any]:
        run, _, _, _ = parse_submission(handle.submission_id)
        self.request_of(handle)
        lines = self.ledger.submissions(run)[handle.submission_id]
        if lines[-1]["event"] not in ("finished", "failed_closed"):
            raise SubmissionRefused("the submission has not finished")
        return dict(lines[-1])


def handle_for(payload: dict[str, Any]) -> Handle:
    """A handle from a request body that names only a submission ID (and a tail for logs)."""
    if set(payload) - {"submission_id", "tail"} or not isinstance(payload.get("submission_id"), str):
        raise SubmissionRefused("the body must name only submission_id (and tail for logs)")
    run, template, _, attempt = parse_submission(payload["submission_id"])
    return Handle(payload["submission_id"], f"hai-{run}-{template}-{attempt}")


def status_body(status: Status) -> dict[str, Any]:
    handle = {"submission_id": status.handle.submission_id, "container_name": status.handle.container_name}
    return {"handle": handle, "state": status.state.value, "exit_code": status.exit_code, "record_path": status.record_path}


def dispatch(broker: EcsBroker, operation: str, payload: dict[str, Any]) -> dict[str, Any]:
    """One interface operation from a request body."""
    if operation == "health":
        return {"broker": "ok"}
    if operation == "submit":
        handle = broker.submit(SubmissionRequest.from_record(payload))
        return {"submission_id": handle.submission_id, "container_name": handle.container_name}
    handle = handle_for(payload)
    if operation == "status":
        return status_body(broker.status(handle))
    if operation == "cancel":
        return status_body(broker.cancel(handle))
    if operation == "logs":
        tail = payload.get("tail", 200)
        if not isinstance(tail, int) or isinstance(tail, bool) or not 1 <= tail <= 10000:
            raise SubmissionRefused("tail must be an integer from 1 to 10000")
        return {"logs": broker.logs(handle, tail)}
    return {"record": broker.record(handle)}


class Listener:
    """The HTTP listener: loopback only, every request needs the bearer token, bodies over 4 KiB are refused."""

    def __init__(self, broker: EcsBroker, host: str, port: int, token: str) -> None:
        if host not in ("127.0.0.1", "::1"):
            raise BrokerError(f"the broker listens on loopback only, not {host}")
        broker.secrets.add(token)
        expected = f"Bearer {token}".encode()

        def record(event: str) -> None:
            with contextlib.suppress(Exception):
                sys.stderr.write(f"broker {event}\n")

        class Handler(BaseHTTPRequestHandler):
            server_version = "hai-broker"
            sys_version = ""
            timeout = HANDLER_TIMEOUT_SECONDS

            def log_message(self, format: str, *args: Any) -> None:
                record(f"http {getattr(self, 'command', '')}")

            def answer(self, code: int, body: dict[str, Any]) -> None:
                data = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self) -> None:
                self.answer(405, {"error": "method not allowed"})

            def do_POST(self) -> None:
                try:
                    self.handle_post()
                except Exception as exc:
                    record(f"internal_error {type(exc).__name__}")
                    with contextlib.suppress(Exception):
                        self.answer(500, {"error": "internal"})

            def handle_post(self) -> None:
                if not secrets.compare_digest(self.headers.get("Authorization", "").encode(), expected):
                    self.answer(401, {"error": "unauthorized"})
                    return
                operation = self.path.removeprefix("/v1/")
                if not self.path.startswith("/v1/") or operation not in OPERATIONS:
                    self.answer(404, {"error": "unknown operation"})
                    return
                values = self.headers.get_all("Content-Length")
                if values is not None and (len(values) != 1 or not CONTENT_LENGTH.fullmatch(values[0])):
                    self.answer(400, {"error": "refused", "message": "Content-Length must be one plain decimal number"})
                    return
                length = int(values[0]) if values else 0
                if length > MAX_BODY_BYTES:
                    self.answer(413, {"error": "body too large"})
                    return
                try:
                    payload = json.loads(self.rfile.read(length) or b"{}")
                    if not isinstance(payload, dict):
                        raise SubmissionRefused("the body must be a JSON object")
                    self.answer(200, dispatch(broker, operation, payload))
                except (SubmissionRefused, ValueError) as exc:
                    self.answer(400, {"error": "refused", "message": broker.redact(str(exc))})
                except Unavailable as exc:
                    self.answer(503, {"error": "unavailable", "message": broker.redact(str(exc))})
                except Exception as exc:
                    record(f"internal_error {operation} {type(exc).__name__}")
                    self.answer(500, {"error": "internal"})

        class QuietServer(ThreadingHTTPServer):
            def handle_error(self, request: Any, client_address: Any) -> None:
                record(f"server_error {type(sys.exc_info()[1]).__name__}")

        self.started = False
        self.server = QuietServer((host, port), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, name="broker-listener", daemon=True)

    @property
    def port(self) -> int:
        return int(self.server.server_address[1])

    def start(self) -> None:
        self.thread.start()
        self.started = True

    def stop(self) -> None:
        if self.started:
            self.server.shutdown()
        self.server.server_close()
        if self.started:
            self.thread.join(timeout=10)
            self.started = False


def required(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name, "")
    if not value:
        raise BrokerError(f"{name} is not set")
    return value


def main(environment: Mapping[str, str] | None = None) -> int:
    """Serve until the container is stopped. Settings come from the BROKER_* variables, the token from the task secret BROKER_TOKEN."""
    import boto3

    env = os.environ if environment is None else environment
    token = required(env, "BROKER_TOKEN")
    if len(token) < 32:
        raise BrokerError("BROKER_TOKEN is shorter than 32 characters")
    region = required(env, "AWS_REGION")
    settings = EcsSettings(
        cluster=required(env, "BROKER_CLUSTER"),
        family_prefix=required(env, "BROKER_FAMILY_PREFIX"),
        subnets=tuple(required(env, "BROKER_SUBNETS").split(",")),
        security_groups=tuple(required(env, "BROKER_SECURITY_GROUPS").split(",")),
        project_tag=required(env, "BROKER_PROJECT_TAG"),
        log_group=required(env, "BROKER_LOG_GROUP"),
        families=frozenset(required(env, "BROKER_FAMILIES").split(",")),
    )
    root = Path(__file__).resolve().parents[2]
    jobs = load_jobs(root)
    unknown = sorted(settings.families - set(jobs["jobs"]))
    if unknown:
        raise BrokerError(f"BROKER_FAMILIES names unknown jobs {unknown}")
    config = client_config()
    session = boto3.session.Session(region_name=region)
    adapter = EcsAdapter(session.client("ecs", config=config), session.client("logs", config=config), settings)
    digest = s3_digest_reader(session.client("s3", config=config), required(env, "BROKER_BUCKET"))
    signed = ssm_signed_reader(session.client("ssm", config=config), required(env, "BROKER_SIGNED_PARAMETER"))
    broker = EcsBroker(
        Path(env.get("BROKER_STATE", "/var/lib/hai/broker")),
        jobs,
        adapter,
        BrokerSettings(required(env, "BROKER_POLARIS_URI"), required(env, "BROKER_MLFLOW_URI")),
        digest,
        signed,
        lambda: datetime.now(UTC).timestamp(),
    )
    listener = Listener(broker, "127.0.0.1", int(env.get("BROKER_PORT", "18790")), token)
    stopping = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopping.set())
    signal.signal(signal.SIGINT, lambda *_: stopping.set())
    listener.start()
    stopping.wait()
    listener.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
