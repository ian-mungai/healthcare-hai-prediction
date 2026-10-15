"""The host job broker: the only path from Airflow to work, as a client of the launcher.

It implements ``orchestration.interface.Submissions``. A submission is checked against ``data_contracts/orchestration/submission_request.schema.json``
and mapped to a launcher template by ``config/orchestration/broker_templates.json`` (schema ``broker_templates.schema.json``). The broker makes no
Docker call and keeps no container state: it writes the ``intent`` ledger line (fsynced), then sends the launcher ``acquire`` (for a new run) and
``submit`` requests with request IDs derived from the submission ID, so a repeat returns the launcher's first answer. Attempts, deadlines,
admission, recovery and release are the launcher's. ``status``, ``cancel``, ``logs`` and ``record`` read the launcher's attempt and bring the ledger
up to date (``started`` when a container exists, ``finished`` or ``failed_closed`` when the attempt ended). The broker runs no monitor thread, so a
Docker outage or a launcher restart cannot leave a supervision gap in it.

The ledger, the forward cache and the dispatch marker record what the broker saw, not what the launcher accepted, and no absence on the broker's own
disk is proof that it never sent. A submission without a settled forward result is always reconciled through the launcher: before the
present inventory row can refuse it, fail it closed or let a cancel return, the broker asks for the attempt that the submission's original ``submit``
request ID created (the launcher's ``status`` target ``submission``, answered to the run's token only). An attempt that is found is reconciled whatever the
row says now; "not found" is the launcher's answer that it never accepted the submit, the valid retry path; an answer that cannot be had leaves the
submission unresolved.

Only the network class ``offline`` runs; a mode of another class, a mode with no launcher row and a name that is not in the file
are refused with that reason. The launcher's closed template table refuses the rest (a changed command, an extra variable or mount, another image).

The supervisor (``scripts/orchestration/supervisor.py``) runs it in-process with its loopback listener. For the owner:

    .venv/bin/python -m scripts.orchestration.broker reconcile      # bring every unfinished submission up to date; print the result
    .venv/bin/python -m scripts.orchestration.broker ledger --run <run_id>
    .venv/bin/python -m scripts.orchestration.broker release --run <run_id> --record <run.json>   # a finalizing run, through the launcher

Tests pass ``--test-registry <folder>/registry``: the state (ledger, records, logs) then lives in ``<folder>``.
The listener's log writes never raise.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import re
import secrets
import sys
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from orchestration import launcher
from orchestration.interface import Handle, State, Status, SubmissionRefused, SubmissionRequest
from scripts.lakehouse import run_lock
from scripts.orchestration.ledger import Ledger, LedgerError, LedgerUnavailable, now, publish_folder, schema_validator

ROOT = Path(__file__).resolve().parents[2]
PLATFORM_FILE = ROOT / "config" / "orchestration" / "platform.json"
TEMPLATES_FILE = ROOT / "config" / "orchestration" / "broker_templates.json"
TEMPLATES_SCHEMA = ROOT / "config" / "orchestration" / "broker_templates.schema.json"
# The launcher's run domain for the Airflow transform DAG.
DOMAIN = "transform"
MAX_BODY_BYTES = 4096
# A request handler gives up on a peer that sends nothing for this long, so a stalled connection cannot hold a thread for ever.
HANDLER_TIMEOUT_SECONDS = 15.0
CONTENT_LENGTH = re.compile(r"[0-9]{1,18}")
CRASH_POINTS = ("after_intent", "after_forward")
REDACTED = "[redacted]"
OPERATIONS = ("health", "submit", "status", "cancel", "logs", "record")
OFFLINE = "offline"
# Launcher refusals that mean "try again later": the submission stays pending and the listener answers 503.
TRANSIENT_CODES = ("degraded", "unavailable", "internal")
# An attempt that ended without an exit code of its own still needs the integer the ledger schema asks for.
END_CODES = {"timeout": 124, "cancelled": 143, "fenced": 143, "create_failed": 125}
UNKNOWN_END = 125
START_WAIT_SECONDS = 15.0
PLACEHOLDER_NAMES = ("run_id", "mode", "run_dir", "build_file")


class BrokerError(Exception):
    """The broker cannot start safely (a second broker, an unreadable inventory)."""


class LauncherUnavailable(Exception):
    """The launcher could not be reached or is degraded; nothing was decided, and the same request can be repeated."""


@dataclass(frozen=True)
class BrokerPaths:
    """Where one broker keeps its state; the real one sits beside the run registry in the main checkout."""

    registry: run_lock.Registry
    state: Path

    @property
    def ledger(self) -> Path:
        """The ledger folder."""
        return self.state / "ledger"

    @property
    def records(self) -> Path:
        """One record file per finished submission."""
        return self.state / "records"

    @property
    def job_logs(self) -> Path:
        """The launcher's saved container output, redacted, kept when a submission finishes."""
        return self.state / "job_logs"

    @property
    def forwards(self) -> Path:
        """The launcher's first answer to each forwarded submission (an attempt ID), so a status call works while the launcher is degraded."""
        return self.state / "forwards"

    @property
    def private(self) -> Path:
        """Owner-only broker files: the broker lock and log."""
        return self.state / "broker"


def resolve_paths(test_registry: str | None) -> BrokerPaths:
    """The real paths (main checkout's ``data/orchestration``) or a test folder's; never a mix."""
    registry = run_lock.resolve(test_registry, Path.cwd())
    if registry.kind == "test" and registry.path.name != "registry":
        raise BrokerError("a test registry folder must be named registry; its parent holds the test state")
    return BrokerPaths(registry, registry.path.parent)


def write_private(path: Path, text: str, root: Path | None = None) -> None:
    """Write an owner-only file atomically inside an owner-only folder. With ``root``, every directory entry from ``root`` down to the file's folder is
    made durable first (``publish_folder``), so that the file's entry is not the only thing that survives a restart."""
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    if root is not None:
        publish_folder(path.parent, root)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(descriptor, text.encode())
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    folder = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(folder)
    finally:
        os.close(folder)


def canonical(value: Any) -> str:
    """The canonical JSON text hashed for a payload."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def sha256_text(text: str) -> str:
    """The SHA-256 of a text."""
    return hashlib.sha256(text.encode()).hexdigest()


def request_id_of(submission_id: str, operation: str = "submit") -> str:
    """The launcher request ID of a submission: the same submission always sends the same ID, so the launcher returns its first answer."""
    return f"brk-{operation}-{sha256_text(submission_id)[:40]}"


def launch_id_of(submission_id: str) -> str:
    """The launcher's logical-launch identifier of a submission."""
    return f"sub-{sha256_text(submission_id)[:24]}"


def submission_request(submission_id: str) -> SubmissionRequest:
    """The request a submission ID stands for, refused when malformed."""
    parts = submission_id.split(":")
    if len(parts) != 4 or not parts[3].isdecimal():
        raise SubmissionRefused(f"malformed submission ID {submission_id!r}")
    request = SubmissionRequest(parts[1], parts[0], parts[2], int(parts[3]))
    errors = sorted(schema_validator("submission_request.schema.json").iter_errors(request.as_dict()), key=str)
    if errors:
        raise SubmissionRefused(f"malformed submission ID: {errors[0].message}")
    return request


def load_templates(path: Path = TEMPLATES_FILE) -> dict[str, Any]:
    """The broker templates, checked against their schema and against the launcher's closed table.

    A mode that forwards to the launcher must have the network class ``offline`` and name a launcher template that a run can submit.
    """
    document: dict[str, Any] = json.loads(path.read_text())
    schema = json.loads(TEMPLATES_SCHEMA.read_text())
    Draft202012Validator.check_schema(schema)
    errors = sorted(Draft202012Validator(schema).iter_errors(document), key=str)
    if errors:
        raise BrokerError(f"{path.name}: {errors[0].message}")
    for name, template in document["templates"].items():
        for mode, row in template["modes"].items():
            row_launcher = row.get("launcher")
            if row_launcher is None:
                continue
            if row["class"] != OFFLINE:
                raise BrokerError(f"{name}/{mode}: network class {row['class']} cannot launch; only offline runs (amendment A18)")
            known = launcher.TEMPLATES.get(row_launcher["template"])
            if known is None or "run" not in known.owners or row_launcher["template"] in ("probe", "catalog_service", "orchestration_service"):
                raise BrokerError(f"{name}/{mode}: the launcher has no template {row_launcher['template']} that a run can submit")
    return document


def substitute(value: Any, values: dict[str, str]) -> Any:
    """Fill ``{run_id}``, ``{mode}``, ``{run_dir}`` and ``{build_file}`` in a row's text, lists and objects; nothing else is substituted."""
    if isinstance(value, str):
        for name in PLACEHOLDER_NAMES:
            value = value.replace("{" + name + "}", values[name])
        return value
    if isinstance(value, list):
        return [substitute(item, values) for item in value]
    if isinstance(value, dict):
        return {key: substitute(item, values) for key, item in value.items()}
    return value


class Broker:
    """Submissions forwarded to the launcher and recorded in a ledger."""

    def __init__(
        self,
        paths: BrokerPaths,
        templates: dict[str, Any],
        client: launcher.LauncherClient,
        *,
        repo_root: Path = ROOT,
        runs_root: Path | None = None,
        crash_at: str | None = None,
    ) -> None:
        """``crash_at`` exists for the E2E only and is refused outside a test registry."""
        if paths.registry.kind != "test" and crash_at is not None:
            raise BrokerError("crash points are test-only")
        self.paths = paths
        self.templates: dict[str, Any] = templates["templates"]
        self.templates_sha256 = sha256_text(canonical(templates))
        self.client = client
        self.repo_root = repo_root
        self.runs_root = runs_root or client.config.runs_root
        self.ledger = Ledger(paths.ledger)
        self.requests_schema = schema_validator("submission_request.schema.json")
        self._crash_at = crash_at
        self._secrets: set[str] = set()
        self._guard = threading.Lock()
        self._lock_file: Any = None

    # Lifecycle

    def start(self) -> dict[str, Any]:
        """Hold the one-broker lock, then bring every unfinished submission up to date with the launcher."""
        self.paths.private.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.paths.private, 0o700)
        handle = (self.paths.private / "broker.lock").open("a")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            raise BrokerError(f"another broker holds {self.paths.private / 'broker.lock'}") from None
        self._lock_file = handle
        try:
            return self.reconcile()
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        """Release the broker lock; the launcher keeps every attempt."""
        if self._lock_file is not None:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
            self._lock_file.close()
            self._lock_file = None

    # Logging and redaction

    def redact(self, text: str) -> str:
        """Replace every secret the broker knows (the session token and each run token it has used) with a marker."""
        with self._guard:
            known = sorted(self._secrets, key=len, reverse=True)
        for value in known:
            text = text.replace(value, REDACTED)
        return text

    def remember_secret(self, value: str) -> None:
        """Add a value to the redaction set."""
        if len(value) >= 8:
            with self._guard:
                self._secrets.add(value)

    def log(self, event: str, detail: str = "") -> None:
        """One redacted line in the broker log."""
        self.paths.private.mkdir(parents=True, exist_ok=True, mode=0o700)
        with (self.paths.private / "broker.log").open("a") as handle:
            handle.write(self.redact(f"{now()} {event} {detail}".rstrip()) + "\n")

    # Rows and payloads

    def row_for(self, request: SubmissionRequest) -> dict[str, Any]:
        """The mode's row, or the refusal with its reason: unknown name, unsupported mode, a class other than offline, no launcher row."""
        template = self.templates.get(request.template)
        if template is None:
            raise SubmissionRefused(f"unknown template {request.template}")
        row = template["modes"].get(request.mode)
        if row is None:
            raise SubmissionRefused(f"template {request.template} does not support mode {request.mode}")
        if row["class"] != OFFLINE:
            raise SubmissionRefused(
                f"template {request.template} in mode {request.mode} needs the network class {row['class']}; only the class offline runs (amendment A18)"
            )
        if not row["no_op"] and row.get("launcher") is None:
            raise SubmissionRefused(f"no launcher template runs {request.template} in mode {request.mode}: {row['reason']}")
        return dict(row)

    def payload_of(self, request: SubmissionRequest, row: dict[str, Any]) -> dict[str, Any]:
        """The launcher ``submit`` parameters of a submission: the row's fixed values with the four placeholders filled, nothing from the caller."""
        run_dir = self.runs_root / request.run_id
        values = {"run_id": request.run_id, "mode": request.mode, "run_dir": str(run_dir), "build_file": str(run_dir / "build.duckdb")}
        target = row["launcher"]
        payload: dict[str, Any] = {
            "template": target["template"],
            "launch_id": launch_id_of(request.submission_id),
            "args": substitute(target["args"], values),
            "params": substitute(target["params"], values),
            "checkout": str(self.repo_root),
        }
        if "timeout_seconds" in target:
            payload["timeout_seconds"] = target["timeout_seconds"]
        return payload

    # Launcher calls

    def call(self, method: str, params: dict[str, Any], principal: dict[str, Any] | None = None, request_id: str | None = None) -> Any:
        """One launcher request; an unreachable or degraded launcher is ``LauncherUnavailable``, a definite refusal stays ``ClientError``."""
        try:
            return self.client.call(method, params, principal, request_id)
        except launcher.ClientError as error:
            if error.code in TRANSIENT_CODES:
                raise LauncherUnavailable(f"the launcher is not available ({error.code}): {error}") from None
            raise
        except (OSError, TimeoutError, ValueError) as error:
            raise LauncherUnavailable(f"the launcher is not reachable: {type(error).__name__}") from None

    def principal(self, run_id: str) -> dict[str, Any]:
        """The run principal; the token comes from the launcher's token store and joins the redaction set. A token that cannot be read is local
        authentication material that is missing or inaccessible: it says nothing about what the launcher accepted, so the question stays open
        (``LauncherUnavailable``) and the same call can be repeated once the store can be read."""
        try:
            principal = self.client.principal_run(run_id)
        except (OSError, launcher.LauncherError) as error:
            raise LauncherUnavailable(f"the run's token cannot be read: {type(error).__name__}") from None
        self.remember_secret(str(principal["token"]))
        return principal

    def token_absent(self, run_id: str) -> bool:
        """True only when the store answers that the run's token file does not exist; a store that cannot be searched answers nothing."""
        try:
            (self.client.config.token_root / f"{run_id}.token").lstat()
        except FileNotFoundError:
            return True
        except OSError:
            return False
        return False

    def ensure_run(self, request: SubmissionRequest) -> dict[str, Any]:
        """The principal of the submission's run: the run this machine's token store already holds, or a new run acquired through the launcher.

        Only a token file that is absent from a store that can be searched sends the broker to ``acquire``, whose request ID is derived from the run, so a
        repeat returns the launcher's first answer: the launcher, not this disk, says whether the run exists. Every other failure to read the store leaves
        the question open. The launcher refuses a second run in the domain, a reused run ID and a real run in a test registry; such a refusal does not say
        whether an earlier submit of this run was accepted, so the lookup stays unasked and the submission unresolved (``LauncherUnavailable``)."""
        try:
            return self.principal(request.run_id)
        except LauncherUnavailable:
            if not self.token_absent(request.run_id):
                raise
        params = {"domain": DOMAIN, "run_id": request.run_id, "mode": request.mode, "holder": "broker", "checkout": str(self.repo_root)}
        try:
            self.call("acquire", params, request_id=request_id_of(f"{request.run_id}:{request.mode}", "acquire"))
        except launcher.ClientError as error:
            raise LauncherUnavailable(f"the run cannot be had, so the launcher cannot be asked about the submission: {error} [{error.code}]") from None
        return self.principal(request.run_id)

    def forward_file(self, request: SubmissionRequest) -> Path:
        """The cache file of the launcher's answer to this submission."""
        return self.paths.forwards / request.run_id / f"{sha256_text(request.submission_id)[:32]}.json"

    def dispatch_file(self, request: SubmissionRequest) -> Path:
        """The marker the broker writes, before it sends ``submit``, for a submission it may have sent. It is a hint for the owner, who can see which
        submissions the broker meant to send. The broker never reads it: no absence of the marker is proof that nothing was sent."""
        return self.paths.ledger / f"{request.run_id}.{sha256_text(request.submission_id)[:32]}.sent"

    def record_dispatch(self, request: SubmissionRequest) -> None:
        """Write the dispatch marker and make its folders durable, before the send. A marker or folder that cannot be made durable leaves the
        submission unsent and unresolved (``LauncherUnavailable``): the same call is repeated, and the next attempt flushes the whole path again."""
        body = json.dumps({"version": 1, "submission_id": request.submission_id, "at": now()}) + "\n"
        try:
            write_private(self.dispatch_file(request), body, self.paths.state)
        except OSError as error:
            raise LauncherUnavailable(f"the broker cannot record that it is about to send: {type(error).__name__}") from None

    def read_forward(self, request: SubmissionRequest) -> dict[str, Any] | None:
        """The cached launcher answer, or None."""
        path = self.forward_file(request)
        if not path.exists():
            return None
        answer: dict[str, Any] = json.loads(path.read_text())
        return answer

    # Ledger helpers

    def append(self, line: dict[str, Any]) -> dict[str, Any]:
        """Append one validated, fsynced ledger line."""
        return self.ledger.append(line)

    def lines_of(self, request: SubmissionRequest) -> list[dict[str, Any]]:
        """The submission's ledger lines."""
        return self.ledger.submissions(request.run_id).get(request.submission_id, [])

    def crash(self, point: str) -> None:
        """Test-only: end the process at a named point, as a crash would."""
        if self._crash_at == point:
            self.log("test_crash", point)
            os._exit(70)

    def write_intent(self, request: SubmissionRequest, row: dict[str, Any], payload: dict[str, Any] | None) -> None:
        """The intent line: written and fsynced before anything is sent to the launcher."""
        self.append(
            {
                "event": "intent",
                "submission_id": request.submission_id,
                **request.as_dict(),
                "spec_sha256": sha256_text(canonical(payload if payload is not None else {"no_op": request.template})),
                "code_sha256": self.templates_sha256,
                "inputs": {},
                "reservation_id": None,
                "reserved_memory_bytes": None,
                "container_name": request.container_name,
            }
        )

    def finish(self, request: SubmissionRequest, exit_code: int, *, no_op: bool = False, record: dict[str, Any] | None = None, **extra: Any) -> None:
        """Write the submission's record file, then its ``finished`` line."""
        lines = self.lines_of(request)
        record_path = self.paths.records / request.run_id / f"{request.container_name}.json"
        body = {
            "submission_id": request.submission_id,
            **request.as_dict(),
            "exit_code": exit_code,
            "no_op": no_op,
            "ledger": lines,
            "written_at": now(),
            **(record or {}),
            **extra,
        }
        write_private(record_path, self.redact(json.dumps(body, indent=2, sort_keys=True, default=str)) + "\n")
        self.append(
            {
                "event": "finished",
                "submission_id": request.submission_id,
                **request.as_dict(),
                "exit_code": exit_code,
                "record_path": str(record_path),
                "observed_peak_memory_bytes": None,
                "no_op": no_op,
                **extra,
            }
        )
        self.log("finished", f"{request.submission_id} exit {exit_code}")

    # Submissions

    def submit(self, request: SubmissionRequest) -> Handle:
        """Refuse, return the existing handle or forward once."""
        errors = sorted(self.requests_schema.iter_errors(request.as_dict()), key=str)
        if errors:
            raise SubmissionRefused(f"request refused: {errors[0].message}")
        handle = Handle(request.submission_id, request.container_name)
        try:
            with self.ledger.locked(request.run_id):
                lines = self.lines_of(request)
                if lines:
                    if lines[-1]["event"] not in ("finished", "failed_closed"):
                        self.advance_locked(request)
                    return handle
                row = self.row_for(request)  # a refused request leaves no line: nothing was accepted
                payload = None if row["no_op"] else self.payload_of(request, row)
                try:
                    self.write_intent(request, row, payload)
                except LedgerUnavailable as error:
                    raise LauncherUnavailable(str(error)) from None  # the intent line is kept: the same submit is repeated
                self.crash("after_intent")
                if row["no_op"]:
                    self.finish(request, 0, no_op=True, reason=row["reason"])
                    return handle
                self.advance_locked(request)
        except LedgerError as error:
            raise SubmissionRefused(f"ledger: {error}") from None
        return handle

    def require_serving(self) -> None:
        """The launcher must be serving: while it is degraded (Docker or its event stream is down) the broker states that instead of the last
        phase the launcher recorded, which can be stale until the launcher has looked at Docker again."""
        deadline = time.monotonic() + START_WAIT_SECONDS
        while True:
            view = self.call("status", {"what": "launcher"})
            if view.get("status") == "serving":
                return
            # A launcher that has just started refuses work until its probe and scan are done; a short wait is not an outage.
            if view.get("status") != "starting" or time.monotonic() > deadline:
                raise LauncherUnavailable(f"the launcher is {view.get('status')}: {view.get('degraded_reason') or 'not serving'}")
            time.sleep(0.2)

    def advance_locked(self, request: SubmissionRequest) -> None:
        """Bring an unfinished submission up to date: complete a no-op, or forward the submission when the launcher has not seen it and read the
        launcher's attempt. A no-op needs nothing from the launcher, so it is completed whether or not the launcher serves."""
        lines = self.lines_of(request)
        if self.recorded_as_noop(request, lines[0]):
            self.complete_noop(request)
            return
        self.require_serving()
        forward = self.read_forward(request)
        if forward is None:
            forward = self.forward_locked(request, lines[0])
            if forward is None:
                return  # the launcher refused it, and the refusal is recorded
        self.apply_attempt(request, forward)

    def recorded_as_noop(self, request: SubmissionRequest, intent: dict[str, Any]) -> bool:
        """True when the intent line was written for a no-op: its identity is the no-op hash of the template, not a launcher payload's."""
        return bool(intent["spec_sha256"] == sha256_text(canonical({"no_op": request.template})))

    def complete_noop(self, request: SubmissionRequest) -> None:
        """Finish a no-op whose intent survived a crash, when the row is still that no-op; a row that changed since fails closed. No launcher payload
        is built and no request is sent."""
        try:
            row = self.row_for(request)
        except SubmissionRefused as error:
            self.append_end(request, str(error))
            return
        if not row["no_op"]:
            self.fail_changed_row(request)
            return
        self.finish(request, 0, no_op=True, reason=row["reason"])

    def fail_changed_row(self, request: SubmissionRequest) -> None:
        """The broker template row is not what the intent recorded: nothing is forwarded and the submission fails closed."""
        self.append(
            {
                "event": "failed_closed",
                "submission_id": request.submission_id,
                **request.as_dict(),
                "reason": "the broker template row changed since the intent was written",
                "containers": [],
            }
        )

    def accepted_forward(self, request: SubmissionRequest, principal: dict[str, Any]) -> dict[str, Any] | None:
        """The launcher's attempt for the submission's original ``submit`` request, or None when the launcher answers that it holds none.

        The launcher is always asked, by the request ID the broker sent, and only its answer decides. The ledger, the forward cache
        and the dispatch marker say what the broker recorded, not what the launcher accepted: a reply can be lost after the launcher accepted the submit, the
        inventory can change before the broker looks again, and a folder can be lost, replaced or written through under the broker. No absence on the
        broker's own disk stands for "never sent": not a missing marker, forward cache or folder, and not a token that is missing. A lookup that cannot be
        asked or answered (the launcher is not reachable or not serving, the run's token cannot be had, the launcher refuses the lookup) leaves the
        submission unresolved (``LauncherUnavailable``): nothing is written and the same call can be repeated."""
        try:
            reply = self.call("status", {"what": "submission", "id": request_id_of(request.submission_id)}, principal)
        except launcher.ClientError as error:
            raise LauncherUnavailable(f"the launcher cannot say whether it accepted the submission: {error} [{error.code}]") from None
        attempt = reply.get("attempt") if reply.get("found") else None
        if attempt is None:
            return None
        return {"attempt_id": attempt["id"], "n": attempt["n"], "deadline_utc": attempt["deadline_utc"], "request_id": request_id_of(request.submission_id)}

    def forward_locked(self, request: SubmissionRequest, intent: dict[str, Any]) -> dict[str, Any] | None:
        """Learn the launcher's attempt for the submission, or send ``submit``. The launcher is asked first, whatever the disk shows: a submission it
        already accepted is found by its original request ID before the present row can refuse it. Only when it answers that it holds none does the row
        decide, and a definite refusal is then recorded as the submission's end. An outage leaves the intent for a retry."""
        principal = self.ensure_run(request)
        accepted = self.accepted_forward(request, principal)
        if accepted is not None:
            write_private(self.forward_file(request), json.dumps(accepted, sort_keys=True) + "\n")
            return accepted
        try:
            row = self.row_for(request)
        except SubmissionRefused as error:
            self.append_end(request, str(error))
            return None
        if row["no_op"]:
            self.fail_changed_row(request)  # the intent holds a launcher payload's identity, and the row is a no-op now
            return None
        payload = self.payload_of(request, row)
        if sha256_text(canonical(payload)) != intent["spec_sha256"]:
            self.fail_changed_row(request)
            return None
        try:
            self.record_dispatch(request)  # durable, with its folders, before the send
            answer = self.call("submit", payload, principal, request_id_of(request.submission_id))
        except launcher.ClientError as error:
            self.append_end(request, f"{error} [{error.code}]")
            return None
        self.crash("after_forward")
        forward = {
            "attempt_id": answer["attempt_id"],
            "n": answer["n"],
            "deadline_utc": answer["deadline_utc"],
            "request_id": request_id_of(request.submission_id),
        }
        write_private(self.forward_file(request), json.dumps(forward, sort_keys=True) + "\n")
        return forward

    def append_end(self, request: SubmissionRequest, reason: str) -> None:
        """A definite refusal ends the submission: a ``finished`` line with exit code 125 and the launcher's reason."""
        reason = self.redact(reason)
        self.log("refused", f"{request.submission_id} {reason}")
        self.finish(request, UNKNOWN_END, refused=True, reason=reason)

    def attempt_of(self, request: SubmissionRequest, forward: dict[str, Any]) -> dict[str, Any]:
        """The launcher's record of the submission's attempt."""
        principal = self.principal(request.run_id)
        record: dict[str, Any] = self.call("status", {"what": "attempt", "id": forward["attempt_id"]}, principal)
        return record

    def apply_attempt(self, request: SubmissionRequest, forward: dict[str, Any]) -> None:
        """Write the ledger lines the launcher's attempt has reached: ``started`` once it has a container, then ``finished`` or ``failed_closed``."""
        attempt = self.attempt_of(request, forward)
        lines = self.lines_of(request)
        events = [line["event"] for line in lines]
        container_id = attempt.get("container_id")
        if container_id and not {"started", "adopted"} & set(events):
            state = "created" if attempt["phase"] in ("creating", "created") else "exited" if attempt["phase"] in ("exiting", "ended") else "running"
            self.append(
                {
                    "event": "started",
                    "submission_id": request.submission_id,
                    **request.as_dict(),
                    "container_id": str(container_id),
                    "container_state": state,
                    "launcher_attempt": attempt["id"],
                }
            )
        if attempt["phase"] != "ended" or events[-1] in ("finished", "failed_closed"):
            return
        if attempt.get("mismatch"):
            self.append(
                {
                    "event": "failed_closed",
                    "submission_id": request.submission_id,
                    **request.as_dict(),
                    "reason": self.redact(str(attempt.get("detail") or "the created container differs from its specification")),
                    "containers": [str(container_id)] if container_id else [],
                }
            )
            return
        reason = str(attempt["reason"])
        # The attempt's own exit code stands for a job that exited; for a stop (deadline, cancel, fence) or a failed create the reason names the code,
        # and the launcher's raw exit code stays in the record.
        exit_code = attempt.get("exit_code") if reason == "exited" else END_CODES.get(reason, attempt.get("exit_code"))
        if exit_code is None:
            exit_code = END_CODES.get(reason, UNKNOWN_END)
        logs = self.saved_logs(request, attempt)
        cancelled = reason in ("cancelled", "fenced") or bool(attempt.get("cancel_intent"))
        summary = {
            "launcher": {key: attempt.get(key) for key in ("id", "template", "n", "phase", "reason", "exit_code", "container_name", "detail", "ended_at")},
            "log_path": str(logs) if logs else None,
        }
        self.finish(request, int(exit_code), record=summary, cancelled=cancelled, end_reason=reason)

    def saved_logs(self, request: SubmissionRequest, attempt: dict[str, Any]) -> Path | None:
        """Keep the launcher's saved output of an ended attempt, redacted; None when the launcher has none."""
        try:
            reply = self.call("logs", {"attempt_id": attempt["id"], "tail": 200}, self.principal(request.run_id))
        except (LauncherUnavailable, launcher.ClientError):
            return None
        text = str(reply.get("text", ""))
        path = self.paths.job_logs / request.run_id / f"{request.container_name}.log"
        write_private(path, self.redact(text))
        return path

    # Reconciliation

    def reconcile(self) -> dict[str, Any]:
        """Bring every unfinished submission up to date: ask the launcher about each, send the ones it has not seen, read the others' attempts."""
        summary: dict[str, Any] = {"updated": [], "pending": [], "ledger_errors": []}
        for run_id in self.ledger.runs():
            try:
                with self.ledger.locked(run_id):
                    for submission_id, lines in self.ledger.submissions(run_id).items():
                        if lines[-1]["event"] in ("finished", "failed_closed"):
                            continue
                        request = submission_request(submission_id)
                        try:
                            self.advance_locked(request)
                            summary["updated"].append(submission_id)
                        except LauncherUnavailable:
                            summary["pending"].append(submission_id)
            except (LedgerError, SubmissionRefused) as error:
                summary["ledger_errors"].append(f"{run_id}: {error}")
                self.log("reconcile_error", f"{run_id} {error}")
        return summary

    # Interface operations

    def _lines(self, handle: Handle) -> tuple[SubmissionRequest, list[dict[str, Any]]]:
        """The ledger lines of one handle, or a refusal."""
        request = submission_request(handle.submission_id)
        if handle.container_name != request.container_name:
            raise SubmissionRefused("the handle's container name does not match its submission ID")
        try:
            with self.ledger.locked(request.run_id):
                lines = self.lines_of(request)
        except LedgerError as error:
            raise SubmissionRefused(f"ledger: {error}") from None
        if not lines:
            raise SubmissionRefused(f"unknown submission {handle.submission_id}")
        return request, lines

    def refresh(self, handle: Handle) -> list[dict[str, Any]]:
        """The submission's lines after bringing them up to date with the launcher."""
        request, lines = self._lines(handle)
        if lines[-1]["event"] in ("finished", "failed_closed"):
            return lines
        try:
            with self.ledger.locked(request.run_id):
                if self.lines_of(request)[-1]["event"] not in ("finished", "failed_closed"):
                    self.advance_locked(request)
                return self.lines_of(request)
        except LedgerError as error:
            raise SubmissionRefused(f"ledger: {error}") from None

    def status(self, handle: Handle) -> Status:
        """The state the last ledger line implies, after reading the launcher's attempt."""
        last = self.refresh(handle)[-1]
        if last["event"] == "finished":
            if last.get("cancelled"):
                return Status(handle, State.CANCELLED, last["exit_code"], last["record_path"])
            state = State.NO_OP if last["no_op"] else State.SUCCEEDED if last["exit_code"] == 0 else State.FAILED
            return Status(handle, state, last["exit_code"], last["record_path"])
        if last["event"] == "failed_closed":
            return Status(handle, State.FAILED_CLOSED, None, None)
        return Status(handle, State.PENDING if last["event"] == "intent" else State.RUNNING, None, None)

    def cancel(self, handle: Handle) -> Status:
        """Ask the launcher to cancel the submission's attempt and report what the launcher did; a finished submission is unchanged.

        Without a forward cache the launcher may still hold an attempt whose reply was lost, so the cache's absence proves nothing: the submission's
        request identity is reconciled through the launcher first (the same ``submit`` request ID returns the launcher's first answer). A launcher that
        cannot be reached or is not serving leaves the outcome unresolved: nothing is written and the same call can be repeated."""
        request, lines = self._lines(handle)
        if lines[-1]["event"] in ("finished", "failed_closed"):
            return self.status(handle)
        try:
            with self.ledger.locked(request.run_id):
                lines = self.lines_of(request)
                if lines[-1]["event"] not in ("finished", "failed_closed"):
                    if self.recorded_as_noop(request, lines[0]):
                        self.complete_noop(request)  # a no-op has no attempt to cancel
                    else:
                        self.require_serving()
                        forward = self.read_forward(request) or self.forward_locked(request, lines[0])
                        if forward is not None:
                            self.call("cancel", {"attempt_id": forward["attempt_id"]}, self.principal(request.run_id))
        except LedgerError as error:
            raise SubmissionRefused(f"ledger: {error}") from None
        return self.status(handle)

    def logs(self, handle: Handle, tail: int = 200) -> str:
        """The launcher's output of the attempt, always redacted."""
        request, _ = self._lines(handle)
        forward = self.read_forward(request)
        if forward is None:
            return ""
        reply = self.call("logs", {"attempt_id": forward["attempt_id"], "tail": tail}, self.principal(request.run_id))
        return self.redact(str(reply.get("text", "")))

    def record(self, handle: Handle) -> dict[str, Any]:
        """The finished ledger line; refused before it exists."""
        last = self.refresh(handle)[-1]
        if last["event"] != "finished":
            raise SubmissionRefused(f"submission {handle.submission_id} has not finished")
        return dict(last)

    # Run control (owner and tests; Airflow reaches none of it)

    def finalize_run(self, run_id: str) -> dict[str, Any]:
        """Move a held run to ``finalizing`` through the launcher."""
        result: dict[str, Any] = self.call("finalize", {}, self.principal(run_id))
        return result

    def release_run(self, run_id: str, record: str | None) -> dict[str, Any]:
        """Release a finalizing run through the launcher. The launcher refuses while any attempt, operation or registration is open and until an
        all-state listing shows no container of the run; the broker never passes the owner flag."""
        result: dict[str, Any] = self.call("release", {"record": record}, self.principal(run_id), request_id_of(f"{run_id}:release", "release"))
        return result


def handle_for(payload: dict[str, Any]) -> Handle:
    """A handle from a request body that names only a submission ID (and a tail for logs)."""
    if set(payload) - {"submission_id", "tail"} or not isinstance(payload.get("submission_id"), str):
        raise SubmissionRefused("the body must name only submission_id (and tail for logs)")
    request = submission_request(payload["submission_id"])
    return Handle(request.submission_id, request.container_name)


def status_body(status: Status) -> dict[str, Any]:
    """A status as JSON."""
    handle = {"submission_id": status.handle.submission_id, "container_name": status.handle.container_name}
    return {"handle": handle, "state": status.state.value, "exit_code": status.exit_code, "record_path": status.record_path}


class Listener:
    """The broker's HTTP listener: every request needs the session token; bodies over 4 KiB are refused."""

    def __init__(self, broker: Broker, host: str, port: int, token: str) -> None:
        """Bind to ``host`` (loopback by design) and ``port``; the token never leaves memory except its file."""
        if host not in ("127.0.0.1", "::1"):
            raise BrokerError(f"the broker listens on loopback only, not {host}")
        broker.remember_secret(token)
        self.broker = broker
        self.started = False
        self.log_failures = 0
        expected = f"Bearer {token}".encode()
        refusal = SubmissionRefused
        listener = self

        def record(event: str, detail: str = "") -> None:
            """A log line that never raises. A refusal can quote the session token, and it is the active exception while a handler logs; a log write that
            fails then chains it, and the thread's default error printer would write the chain, token included, to stderr. The failure is counted, and
            the first is announced by exception type only."""
            try:
                broker.log(event, detail)
            except Exception as error:
                listener.log_failures += 1
                if listener.log_failures == 1:
                    with contextlib.suppress(Exception):
                        sys.stderr.write(f"broker: a log write failed ({type(error).__name__}); later failures are counted and not printed\n")

        class Handler(BaseHTTPRequestHandler):
            server_version = "hai-broker"
            sys_version = ""
            timeout = HANDLER_TIMEOUT_SECONDS

            def log_message(self, format: str, *args: Any) -> None:
                record("http", self.redact_path())

            def redact_path(self) -> str:
                return f"{getattr(self, 'command', '')} {str(getattr(self, 'path', '')).split('?')[0][:64]}"

            def answer(self, code: int, body: dict[str, Any]) -> None:
                data = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self) -> None:
                self.answer(405, {"error": "method not allowed"})

            def body_length(self) -> int:
                """The request body's length: one plain decimal number or nothing. Anything else is refused without echoing the header."""
                values = self.headers.get_all("Content-Length")
                if values is None:
                    return 0
                if len(values) != 1 or not CONTENT_LENGTH.fullmatch(values[0]):
                    raise refusal("Content-Length must be one plain decimal number")
                return int(values[0])

            def do_POST(self) -> None:
                try:
                    self.handle_post()
                except Exception as exc:  # nothing a request does may reach the server's own error output, which prints exception text
                    record("internal_error", f"{self.command} {type(exc).__name__}")
                    with contextlib.suppress(Exception):
                        self.answer(500, {"error": "internal"})

            def handle_post(self) -> None:
                supplied = self.headers.get("Authorization", "").encode()
                if not secrets.compare_digest(supplied, expected):
                    self.answer(401, {"error": "unauthorized"})
                    return
                operation = self.path.removeprefix("/v1/")
                if not self.path.startswith("/v1/") or operation not in OPERATIONS:
                    self.answer(404, {"error": "unknown operation"})
                    return
                try:
                    length = self.body_length()
                except refusal as exc:
                    self.answer(400, {"error": "refused", "message": str(exc)})
                    return
                if length > MAX_BODY_BYTES:
                    self.answer(413, {"error": "body too large"})
                    return
                try:
                    payload = json.loads(self.rfile.read(length) or b"{}")
                    if not isinstance(payload, dict):
                        raise refusal("the body must be a JSON object")
                    self.answer(200, dispatch(broker, operation, payload))
                except (refusal, ValueError) as exc:
                    self.answer(400, {"error": "refused", "message": broker.redact(str(exc))})
                except LauncherUnavailable as exc:
                    self.answer(503, {"error": "launcher unavailable", "message": broker.redact(str(exc))})
                except Exception as exc:  # the listener answers 500 and logs; it must not die
                    record("internal_error", f"{operation} {type(exc).__name__}: {exc}")
                    self.answer(500, {"error": "internal"})

        class QuietServer(ThreadingHTTPServer):
            """The standard server prints a traceback, with the exception text, to stderr for an error outside a handler's own code; this one logs the type."""

            def handle_error(self, request: Any, client_address: Any) -> None:
                record("server_error", type(sys.exc_info()[1]).__name__)

        self.server = QuietServer((host, port), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, name="broker-listener", daemon=True)

    @property
    def port(self) -> int:
        """The bound port."""
        return int(self.server.server_address[1])

    def start(self) -> None:
        """Serve in a background thread."""
        self.thread.start()
        self.started = True

    def stop(self) -> None:
        """Stop serving and close the socket. A listener whose thread never started is only closed: ``shutdown`` waits for a server loop that never ran."""
        if self.started:
            self.server.shutdown()
        self.server.server_close()
        if self.started:
            self.thread.join(timeout=10)
            self.started = False


def dispatch(broker: Broker, operation: str, payload: dict[str, Any]) -> dict[str, Any]:
    """One interface operation from a request body."""
    if operation == "health":
        return {"broker": "ok"}
    if operation == "submit":
        errors = sorted(broker.requests_schema.iter_errors(payload), key=str)
        if errors:
            raise SubmissionRefused(f"request refused: {errors[0].message}")
        handle = broker.submit(SubmissionRequest(payload["template"], payload["run_id"], payload["mode"], payload["attempt"]))
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


# Command line


def build(test_registry: str | None, templates: str | None, crash_at: str | None = None) -> Broker:
    """A broker for the real registry or a test one; another inventory or a crash point only in a test."""
    paths = resolve_paths(test_registry)
    if paths.registry.kind != "test" and templates is not None:
        raise BrokerError("--templates is test-only")
    client = launcher.load_client(test_registry)
    return Broker(paths, load_templates(Path(templates) if templates else TEMPLATES_FILE), client, crash_at=crash_at)


def main() -> int:
    """Owner and test commands; the listener runs inside the supervisor's session."""
    cli = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    cli.add_argument("--test-registry", help="a test registry folder named registry; its parent holds the test state")
    cli.add_argument("--templates", help="another broker template file (test registries only)")
    commands = cli.add_subparsers(dest="command", required=True)
    commands.add_parser("reconcile", help="bring every unfinished submission up to date with the launcher")
    command = commands.add_parser("ledger", help="print one run's ledger after checking it")
    command.add_argument("--run", required=True)
    command = commands.add_parser("release", help="release a finalizing run through the launcher (never with the owner flag)")
    command.add_argument("--run", required=True)
    command.add_argument("--record", required=True)
    command = commands.add_parser("submit-once", help="test-only: start a broker, submit one request and stop (or crash at a point)")
    command.add_argument("--template", required=True)
    command.add_argument("--run", required=True)
    command.add_argument("--mode", required=True)
    command.add_argument("--attempt", type=int, required=True)
    command.add_argument("--crash-at", choices=CRASH_POINTS)
    args = cli.parse_args()
    try:
        if args.command == "ledger":
            result: Any = Ledger(resolve_paths(args.test_registry).ledger).read(args.run)
        else:
            broker = build(args.test_registry, args.templates, getattr(args, "crash_at", None))
            if args.command == "submit-once" and broker.paths.registry.kind != "test":
                raise BrokerError("submit-once is test-only")
            try:
                result = broker.start()
                if args.command == "submit-once":
                    handle = broker.submit(SubmissionRequest(args.template, args.run, args.mode, args.attempt))
                    result = status_body(broker.status(handle))
                elif args.command == "release":
                    result = broker.release_run(args.run, args.record)
            finally:
                broker.close()
    except (BrokerError, SubmissionRefused, LedgerError, run_lock.RegistryError, LauncherUnavailable, launcher.ClientError) as exc:
        sys.stderr.write(f"broker: {exc}\n")
        return 3
    sys.stdout.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
