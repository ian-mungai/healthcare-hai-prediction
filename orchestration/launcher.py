"""The single launcher: the only process that changes container, network or volume state (D1 sections 2 to 9 and 11).

Run from the repository root:

    .venv/bin/python -m orchestration.launcher serve [--test-registry <folder>]     # the launcher; clients start it on demand
    .venv/bin/python -m orchestration.launcher status [--test-registry <folder>]    # health, runs, allocations and attempts
    .venv/bin/python -m orchestration.launcher run --template fixture_dbt ...        # submit one attempt and wait for it

One launcher serves one registry (``data/orchestration/registry/`` in the main checkout, or a test registry) and holds an
exclusive ``flock`` on ``launcher.lock`` for its whole life. Clients make no Docker call: they speak JSON lines over the
registry's Unix socket, authenticated by the peer's user ID, process ID and start time plus a run token where a method
needs one. The launcher keeps every container, network and volume it creates as a recorded operation with an ID; a
result is applied only while the attempt is still in the phase the operation expected (D1 section 4). Compose is read as
a specification only: the launcher builds each container's create specification from the parsed ``docker-compose.yaml``,
the template table and validated values, and records the SHA-256 of that effective specification.

``provenance_vars(run)`` and ``seal(run)`` are the frozen interfaces that scripts/lakehouse/provenance.py fills: a ``real_dbt``
launch needs recorded pins and a complete snapshot, and a gold-eligible build is sealed (D1 section 10).
Failure modes 910 to 919 are in plans/wave2_20261011/d1_design.md section 15 and plans/wp/l1a_failure_modes.md.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import enum
import errno
import fcntl
import hashlib
import hmac
import http.client
import importlib
import json
import logging
import os
import pwd
import queue
import re
import secrets
import selectors
import shutil
import signal
import socket
import stat
import struct
import sys
import threading
import time
import traceback
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from functools import cache, partial
from pathlib import Path
from typing import Any, Literal, NamedTuple, NoReturn

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

from orchestration import catalog_authority, dbt_lineage, ecs_target, lineage_services, network_policy, pipeline_protocol, pipeline_render, pipeline_session
from scripts.infrastructure import secret_store
from scripts.lakehouse import dbt_package_archive, gold_publish_record, memory_budget, pins, run_lock
from scripts.lakehouse import provenance as build_provenance
from scripts.process import run_command

PROTOCOL = 1
REFUSED_EXIT, ERROR_EXIT, DEGRADED_EXIT = 3, 4, 5
NETWORK_CLASSES_ENFORCED = network_policy.CLASSES
LABEL_REGISTRY = "hai.registry_id"
LABEL_RUN = run_lock.CONTAINER_LABEL
LABEL_ALLOCATION = "hai.allocation"
LABEL_GENERATION = "hai.generation"
LABEL_LAUNCH = "hai.launch"
LABEL_ATTEMPT = "hai.attempt"
LABEL_OP = "hai.op"
LABEL_PROBE = "hai.probe"
LABEL_SERVICE = "hai.service"
LABEL_STACK = "hai.stack"
LABEL_ORCH_SERVICE = "hai.orch_service"
LINEAGE_COMPOSE = "services/lineage/docker-compose.lineage.yaml"
LINEAGE_CHECKS_COMPOSE = "services/lineage/docker-compose.checks.yaml"
LINEAGE_ORDER = lineage_services.SERVICES
LINEAGE_STACK = "lineage"
LINEAGE_OWNER = "stack:lineage"
LINEAGE_START_SECONDS = 600
LINEAGE_PRE_START = ("queued", "creating", "created", "starting")
LINEAGE_MEMORY: dict[str, tuple[str, float, int]] = {
    "postgres": ("POSTGRES", 0.03, 256 * 1024**2),
    "api": ("API", 0.06, 1024**3),
    "web": ("WEB", 0.02, 256 * 1024**2),
}
ORCHESTRATION_STACK = "orchestration"
ORCHESTRATION_OWNER = "stack:orchestration"
ORCHESTRATION_COMPOSE = "services/orchestration/docker-compose.orchestration.yaml"
ORCHESTRATION_ORDER = ("postgres", "airflow-migrate", "airflow-apiserver", "airflow-scheduler", "airflow-dag-processor", "airflow-triggerer")
ORCHESTRATION_FINITE = frozenset({"airflow-migrate"})
ORCHESTRATION_MEMORY: dict[str, tuple[str, float, int]] = {
    "postgres": ("POSTGRES", 0.03, 256 * 1024**2),
    "airflow-apiserver": ("APISERVER", 0.05, 1024**3),
    "airflow-scheduler": ("SCHEDULER", 0.08, 2 * 1024**3),
    "airflow-dag-processor": ("DAG_PROCESSOR", 0.04, 1024**3),
    "airflow-triggerer": ("TRIGGERER", 0.02, 512 * 1024**2),
}
ORCHESTRATION_SCHEDULER_BASE = 1024**3
ORCHESTRATION_WORKER_MEMORY = 256 * 1024**2
ORCHESTRATION_STATE_FOLDERS = ("session",)
ORCHESTRATION_DUMPS = ("metadata", "xcom")
ORCHESTRATION_UP_PARAMETERS = ("state", "api_port", "checkout")
PINNED_REFERENCE = re.compile(r"[a-z0-9][a-z0-9._/-]*(?::[A-Za-z0-9._-]{1,128})?@sha256:[a-f0-9]{64}")
DEADLINE_ENTRYPOINT = "/opt/hai/deadline_run"
# The entrypoint passes its timing state to itself through this variable; a job or an image that sets it could forge that state.
DEADLINE_RELAY_VARIABLE = "HAI_DEADLINE_RELAY_B"
# A start nonce a client may pass when it starts the launcher; the launcher reports it in ``ping`` (``""`` when none was given).
LAUNCH_NONCE_VARIABLE = "HAI_LAUNCH_NONCE"
LAUNCH_NONCE = os.environ.get(LAUNCH_NONCE_VARIABLE, "")
DOCKER_RESTART_COMMAND = ("docker", "desktop", "restart")
# D1 section 5 item 1: the admitted host-to-VM skew, the start margin, the TERM-to-KILL grace and the headroom after it.
SKEW_SECONDS = 2
START_MARGIN_SECONDS = 1
DIAGNOSTIC_OWNER = "launcher:diagnostic"
ACTIVE_PHASES = ("creating", "created", "starting", "started", "exiting")
SECRET_NAME = re.compile(r"(PASSWORD|SECRET|TOKEN|ACCESS_KEY)", re.IGNORECASE)
Fetched = str | pipeline_session.RoleSession | secret_store.SecretStoreError
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}")
RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
MAX_LINE = 1 << 20
SESSION_BUFFER_LIMIT = 128 * 1024
INPUT_CHUNK = 16 * 1024
DEFAULT_IMAGE_LABEL = "hai.deadline_entrypoint"
DEFAULT_IMAGE_IMPLEMENTATION = "hai.deadline_implementation"
CAPTURE_LIMIT = 32 * 1024 * 1024
EXCHANGE_BUDGET = 1.0
EXCHANGE_BUDGET_CAP = 4.0
PROBE_BUDGET = 1.0
LIVENESS_CODES = ("unavailable", "unresponsive")
CLIENT_LOG = logging.getLogger("launcher.client")


class LauncherError(Exception):
    """A request the launcher refuses; nothing was changed. ``code`` is a short machine-readable reason."""

    def __init__(self, message: str, code: str = "refused") -> None:
        super().__init__(message)
        self.code = code


@cache
def caller_identity_validator(root: Path) -> Draft202012Validator:
    """Compile the caller contract once per checkout for this launcher process."""
    try:
        request = json.loads((root / "data_contracts/orchestration/submission_request.schema.json").read_text())
        fields = ("dag_id", "task_id", "dag_run_id")
        identity_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": list(fields),
            "properties": {key: request["properties"][key] for key in fields},
        }
        Draft202012Validator.check_schema(identity_schema)
    except (OSError, ValueError, KeyError, TypeError, SchemaError):
        raise LauncherError("caller identity schema is missing or invalid", "identity_schema") from None
    return Draft202012Validator(identity_schema)


class Degraded(LauncherError):
    """Docker or the event stream is not usable; the launcher refuses new work until it is."""

    def __init__(self, message: str) -> None:
        super().__init__(message, "degraded")


@dataclass(frozen=True)
class Timings:
    """Every wait the launcher uses; a test registry may shorten them, the real registry uses the defaults.

    ``compact_threshold`` is a size in bytes: above it, finished records older than ``compact_floor`` seconds move to the archive. A client
    fails after ``gone_after`` seconds of refused connections or ``stuck_after`` seconds without a liveness reply, and logs an exchange that
    takes longer than ``slow_exchange`` seconds beyond the wait it asked for. After such a failure its cleanup (replaying an ambiguous submit,
    cancelling, releasing) has ``cleanup_budget`` seconds in all.
    """

    create_timeout: float = 120.0
    probe_timeout: float = 10.0
    tick: float = 0.2
    backoff_start: float = 1.0
    backoff_cap: float = 30.0
    stop_grace: int = 10
    idle_session: float = 1800.0
    docker_call_timeout: float = 120.0
    health_wait: float = 180.0
    restart_wait: float = 300.0
    # How long a queued launch waits for memory that another process holds before it fails with a named cause.
    memory_wait: float = memory_budget.MEMORY_WAIT_DEADLINE
    # After a pool's cancellations persist, how long its workers keep polling an unreachable launcher before failing visibly.
    cancel_settle: float = 75.0
    # How long a client keeps retrying a submit or status exchange that times out (a launcher busy on a loaded machine) before failing.
    poll_patience: float = 180.0
    history_retain: float = 86400.0
    compact_every: float = 600.0
    compact_threshold: int = 1_000_000
    compact_floor: float = 3600.0
    stuck_after: float = 30.0
    gone_after: float = 1.0
    slow_exchange: float = 0.5
    cleanup_budget: float = 20.0


@dataclass(frozen=True)
class Config:
    """Where the launcher finds its registry, Docker, token store and the repository checkout it was started from."""

    registry: run_lock.Registry
    root: Path
    docker_socket: str
    token_root: Path
    runs_root: Path
    timings: Timings = Timings()
    images: Mapping[str, str] = field(default_factory=dict)
    restart_command: tuple[str, ...] = DOCKER_RESTART_COMMAND
    secrets_endpoint: str | None = None
    secret_fetch_seconds: float = secret_store.FETCH_DEADLINE_SECONDS
    sts_endpoint: str | None = None
    aws_account_id: str = ""
    aws_profile: str = ""
    aws_region: str = ""
    allowed_roots: tuple[Path, ...] = ()
    lakehouse_state: Path = Path("data/lakehouse")
    # The checkout whose data/lakehouse/ holds the catalog database: the main checkout for the real registry (amendments A85 and A86).
    catalog_checkout: Path | None = None
    # A test registry may name the user ID the launcher treats as its own, so a test can show that a peer of another user is refused.
    expected_uid: int | None = None
    ecs_runtime: ecs_target.Runtime | None = None

    @property
    def token_anchor(self) -> Path:
        """The system folder above the token store's dedicated chain: the registry folder of a test registry that holds its tokens, else the root's anchor."""
        if self.registry.kind == "test" and self.token_root.is_relative_to(self.registry.path):
            return self.registry.path
        return token_anchor(self.token_root)

    def secret_store(self) -> secret_store.SecretStore:
        """The Parameter Store client for this registry: the project profile, or the stand-in a test registry names."""
        try:
            return secret_store.open_store(self.catalog_checkout or self.root, self.secrets_endpoint)
        except secret_store.SecretStoreError as error:
            raise LauncherError(str(error), "spec") from None

    def aws_settings(self) -> dict[str, str]:
        """The project AWS profile and region: a test registry's own values, else the rendered Terraform variables."""
        if self.secrets_endpoint is not None:
            return {"AWS_PROFILE": self.aws_profile, "AWS_REGION": self.aws_region}
        try:
            settings = secret_store.project_settings(self.catalog_checkout or self.root)
        except secret_store.SecretStoreError as error:
            raise LauncherError(str(error), "spec") from None
        return {"AWS_PROFILE": settings["aws_profile"], "AWS_REGION": settings["aws_region"]}

    def role_session(self, role: str, deadline_utc: int, session_name: str) -> pipeline_session.RoleSession:
        """A new session of the role for a job that ends at ``deadline_utc``: from AWS for the real registry, from the stand-in for a test registry."""
        return pipeline_session.fetch(
            role,
            settings=self.session_settings(),
            endpoint=self.sts_endpoint,
            test_registry=self.registry.kind == "test",
            deadline_utc=deadline_utc,
            session_name=session_name,
            limit=self.secret_fetch_seconds,
        )

    def session_settings(self) -> dict[str, str]:
        """The project AWS profile, region and account for role sessions: a test registry's own values, else the rendered Terraform variables."""
        if self.registry.kind == "test":
            return {"aws_profile": self.aws_profile, "aws_region": self.aws_region, "expected_account_id": self.aws_account_id}
        try:
            return pipeline_session.deployment_settings(self.catalog_checkout or self.root)
        except pipeline_session.SessionError as error:
            raise LauncherError(str(error), "spec") from None

    @property
    def socket_path(self) -> Path:
        """The launcher's own socket in the registry folder."""
        return self.registry.path / "launcher.sock"

    @property
    def lock_path(self) -> Path:
        """The file whose exclusive lock marks the one launcher of the registry."""
        return self.registry.path / "launcher.lock"


def utc_now() -> str:
    """The current UTC time in ISO format."""
    return datetime.now(UTC).isoformat()


def canonical(value: Any) -> str:
    """Canonical JSON: sorted keys, no spaces."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def sha256_text(text: str) -> str:
    """SHA-256 of text."""
    return hashlib.sha256(text.encode()).hexdigest()


def account_home() -> Path:
    """The account's home folder from the user database; the environment is ignored (D1 section 11 item 1)."""
    return Path(pwd.getpwuid(os.getuid()).pw_dir)


def docker_socket_path() -> str:
    """The Docker Engine socket: ``DOCKER_HOST`` as the Docker command does, else the current context's endpoint."""
    host = os.environ.get("DOCKER_HOST", "")
    if host.startswith("unix://"):
        return host.removeprefix("unix://")
    result = run_command("docker", ["context", "inspect", "--format", "{{.Endpoints.docker.Host}}"], timeout=30)
    endpoint = result.stdout.strip()
    if result.returncode or not endpoint.startswith("unix://"):
        raise Degraded(f"cannot read the Docker socket from the Docker context: {result.stderr.strip()[-200:] or endpoint}")
    return endpoint.removeprefix("unix://")


def real_token_root(registry_id: str) -> Path:
    """The real token root for a registry (D1 section 11 item 1)."""
    return account_home().joinpath(*run_lock.TOKEN_FOLDER) / registry_id


def token_anchor(root: Path) -> Path:
    """The last system folder above a token root: ``Application Support`` for the real root, else the root's parent.

    Every folder below the anchor down to the root is the dedicated chain (D1 section 11 item 1).
    """
    support = account_home() / "Library" / "Application Support"
    return support if root.is_relative_to(support) else root.parent


def private_chain(path: Path, create: bool = True, anchor: Path | None = None) -> None:
    """Check, and create when missing, the dedicated chain from ``anchor`` (exclusive) down to ``path``: every component by ``lstat``.

    A dedicated component must be a real folder (never a link) owned by the user with mode ``0700``. The anchor and every folder above
    it are system folders: owned by the user or root and not group- or world-writable (a sticky folder excepted).
    """
    uid = os.getuid()
    anchor = anchor if anchor is not None else token_anchor(path)
    if not path.is_relative_to(anchor) or path == anchor:
        raise LauncherError(f"{path} is not below the token store's anchor {anchor}", "token_root")
    for ancestor in [anchor, *anchor.parents]:
        info = ancestor.stat()
        if info.st_uid not in (0, uid) or (info.st_mode & 0o022 and not info.st_mode & stat.S_ISVTX):
            raise LauncherError(f"{ancestor} is not owned by the user or root, or is group- or world-writable", "token_root")
    current = anchor
    for part in path.relative_to(anchor).parts:
        current = current / part
        try:
            info = current.lstat()
        except FileNotFoundError:
            if not create:
                raise LauncherError(f"{current} does not exist", "token_root") from None
            current.mkdir(mode=0o700)
            current.chmod(0o700)
            info = current.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise LauncherError(f"{current} must be a real folder; the token store refuses links", "token_root")
        if info.st_uid != uid or info.st_mode & 0o777 != 0o700:
            raise LauncherError(f"{current} must be owned by the user with mode 0700", "token_root")


def write_token_file(root: Path, run_id: str, token: str, anchor: Path | None = None) -> Path:
    """Write a run's token file: new file, ``0600``, fsynced, renamed over any file of an unpublished run, folder fsynced (D1 section 11 item 2)."""
    private_chain(root, anchor=anchor)
    target = root / f"{run_id}.token"
    temporary = root / f".{run_id}.{os.getpid()}.{secrets.token_hex(4)}.tmp"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(token)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, target)
    folder = os.open(root, os.O_RDONLY)
    try:
        os.fsync(folder)
    finally:
        os.close(folder)
    return target


def read_private_file(path: Path, limit: int = 4096) -> bytes:
    """Read a file of the token store: a regular file owned by the user with mode ``0600``, never a link."""
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as error:
        if error.errno == errno.ELOOP:
            raise LauncherError(f"{path} is a link; the token store refuses links", "token_root") from error
        raise
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise LauncherError(f"{path} must be a regular file owned by the user with mode 0600", "token_root")
        return os.read(descriptor, limit)
    finally:
        os.close(descriptor)


def read_token_file(root: Path, run_id: str, anchor: Path | None = None) -> str:
    """Read a run's token from the store, refusing a file that is a link, not owned by the user or readable by others."""
    private_chain(root, create=False, anchor=anchor)
    return read_private_file(root / f"{run_id}.token").decode().strip()


def launcher_key(root: Path, anchor: Path | None = None) -> bytes:
    """The key for secret commitments: a ``0600`` file in the token store's dedicated folder, created once (D1 section 8 item 1)."""
    private_chain(root, anchor=anchor)
    path = root / "launcher_hmac_key"
    if not path.exists() and not path.is_symlink():
        temporary = root / f".key.{os.getpid()}.{secrets.token_hex(4)}.tmp"
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(secrets.token_bytes(32))
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            pass
        finally:
            temporary.unlink(missing_ok=True)
    key = read_private_file(path, 64)
    if len(key) != 32:
        raise LauncherError(f"{path} does not hold a 32-byte key", "token_root")
    return key


def commit_secret(key: bytes, value: str) -> str:
    """The keyed HMAC-SHA-256 commitment that stands in for a secret value in a recorded specification."""
    return "hmac:" + hmac.new(key, value.encode(), hashlib.sha256).hexdigest()


DockerReason = Literal[
    "no connection",
    "request refused",
    "daemon error",
    "unexpected reply",
    "stop failed",
    "kill failed",
    "remove failed",
    "logs failed",
    "network remove failed",
    "pull failed",
    "pull stream failed",
    "events refused",
    "events failed",
    "attach refused",
    "ping failed",
]


def docker_reason(status: int) -> DockerReason:
    """The fixed reason class of a daemon status. The daemon's own text is never kept because it can quote a container's environment."""
    if status == 0:
        return "no connection"
    if 400 <= status < 500:
        return "request refused"
    if status >= 500:
        return "daemon error"
    return "unexpected reply"


class DockerError(Exception):
    """The Docker Engine answered with an error status, or did not answer. It keeps the status and a fixed reason class, never daemon text."""

    def __init__(self, status: int, message: DockerReason) -> None:
        super().__init__(f"Docker {status}: {message}")
        self.status = status
        self.message: DockerReason = message


def failure_text(error: object) -> str:
    """The text a record may keep for a failure: a Docker error as its status and reason, a launcher refusal as the launcher composed it, any other
    exception as its type name."""
    if isinstance(error, (DockerError, LauncherError, memory_budget.BudgetError, build_provenance.ProvenanceError)):
        return str(error)
    return type(error).__name__


class UnixConnection(http.client.HTTPConnection):
    """An HTTP connection over a Unix domain socket."""

    def __init__(self, path: str, timeout: float | None) -> None:
        super().__init__("localhost", timeout=timeout)
        self.socket_path = path

    def connect(self) -> None:
        """Open the Unix socket."""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self.socket_path)
        self.sock = sock


def demultiplex(buffer: bytes) -> tuple[list[tuple[int, bytes]], bytes]:
    """Split a Docker stream (8-byte header frames) into ``(stream, payload)`` frames and the unfinished rest."""
    frames: list[tuple[int, bytes]] = []
    while len(buffer) >= 8:
        size = struct.unpack(">I", buffer[4:8])[0]
        if len(buffer) < 8 + size:
            break
        frames.append((buffer[0], buffer[8 : 8 + size]))
        buffer = buffer[8 + size :]
    return frames, buffer


class EventStream:
    """The Docker events response: lines of JSON until the daemon or this side closes it."""

    def __init__(self, connection: UnixConnection, response: http.client.HTTPResponse) -> None:
        self.connection = connection
        self.response = response

    def readline(self) -> bytes:
        """The next line; an empty result means the stream ended."""
        return self.response.readline()

    def close(self) -> None:
        """Close the connection, which ends a blocked ``readline`` in another thread."""
        with contextlib.suppress(OSError):
            if self.connection.sock is not None:
                self.connection.sock.shutdown(socket.SHUT_RDWR)
        with contextlib.suppress(OSError):
            self.connection.close()


class DockerApi:
    """The Docker Engine API on its Unix socket. Only the launcher constructs this class (D1 section 2 item 4)."""

    def __init__(self, socket_path: str, timeout: float = 120.0, version: str = "v1.43") -> None:
        self.socket_path = socket_path
        self.timeout = timeout
        self.prefix = f"/{version}"

    def request(self, method: str, path: str, body: Any = None, timeout: float | None = None) -> tuple[int, bytes]:
        """One request; connection failures raise ``DockerError`` with status 0."""
        connection = UnixConnection(self.socket_path, self.timeout if timeout is None else timeout)
        payload = None if body is None else json.dumps(body).encode()
        headers = {"Content-Type": "application/json"} if payload is not None else {}
        try:
            connection.request(method, self.prefix + path, body=payload, headers=headers)
            response = connection.getresponse()
            return response.status, response.read()
        except (OSError, http.client.HTTPException) as error:
            raise DockerError(0, "no connection") from error
        finally:
            connection.close()

    def call(self, method: str, path: str, body: Any = None, ok: Sequence[int] = (200, 201, 204), timeout: float | None = None) -> Any:
        """A request whose status must be in ``ok``; returns the decoded JSON body or None."""
        status, data = self.request(method, path, body, timeout)
        if status not in ok:
            raise DockerError(status, docker_reason(status))
        if not data:
            return None
        try:
            return json.loads(data)
        except ValueError:
            return data

    @staticmethod
    def filters(**values: list[str]) -> str:
        """The ``filters`` query value."""
        return json.dumps(values)

    def ping(self) -> bool:
        """True when the daemon answers."""
        status, _ = self.request("GET", "/_ping", timeout=10)
        return status == 200

    def info(self) -> dict[str, Any]:
        """The daemon's ``info``."""
        return dict(self.call("GET", "/info"))

    def containers(self, label: str | None = None, all_states: bool = True) -> list[dict[str, Any]]:
        """Every container, optionally with one label filter."""
        query = f"all={'1' if all_states else '0'}"
        if label:
            query += "&filters=" + _quote(self.filters(label=[label]))
        return list(self.call("GET", f"/containers/json?{query}") or [])

    def inspect(self, ref: str) -> dict[str, Any] | None:
        """A container's full description, or None when it does not exist."""
        try:
            return dict(self.call("GET", f"/containers/{_quote(ref)}/json"))
        except DockerError as error:
            if error.status == 404:
                return None
            raise

    def create(self, name: str, body: dict[str, Any], timeout: float | None = None) -> str:
        """Create a container and return its ID; ``timeout`` bounds the call (D1 section 4 item 8)."""
        return str(self.call("POST", f"/containers/create?name={_quote(name)}", body, ok=(201,), timeout=timeout)["Id"])

    def start(self, container_id: str) -> None:
        """Start a container; already started is fine."""
        self.call("POST", f"/containers/{container_id}/start", ok=(204, 304))

    def stop(self, container_id: str, grace: int) -> None:
        """Stop a container; one that is stopped or gone is fine."""
        status, _ = self.request("POST", f"/containers/{container_id}/stop?t={grace}", timeout=grace + 60)
        if status not in (204, 304, 404):
            raise DockerError(status, "stop failed")

    def kill(self, container_id: str) -> None:
        """Kill a container; one that is stopped or gone is fine."""
        status, _ = self.request("POST", f"/containers/{container_id}/kill")
        if status not in (204, 404, 409):
            raise DockerError(status, "kill failed")

    def remove(self, container_id: str) -> None:
        """Remove a container (forced); one that is gone is fine."""
        status, _ = self.request("DELETE", f"/containers/{container_id}?force=1&v=0")
        if status not in (204, 404):
            raise DockerError(status, "remove failed")

    def logs_tail(self, container_id: str, lines: int = 200) -> str:
        """The last lines of a container's output, standard output and error together."""
        status, data = self.request("GET", f"/containers/{container_id}/logs?stdout=1&stderr=1&tail={lines}")
        if status == 404:
            return ""
        if status != 200:
            raise DockerError(status, "logs failed")
        frames, rest = demultiplex(data)
        if not frames and rest:
            return rest.decode(errors="replace")
        return b"".join(payload for _, payload in frames).decode(errors="replace")

    def logs_split(self, container_id: str, tail: int | None) -> tuple[str, str]:
        """A container's standard output and error as text, all of it or the last ``tail`` lines."""
        count = "all" if tail is None else str(tail)
        status, data = self.request("GET", f"/containers/{container_id}/logs?stdout=1&stderr=1&tail={count}", timeout=300)
        if status == 404:
            return "", ""
        if status != 200:
            raise DockerError(status, "logs failed")
        frames, rest = demultiplex(data)
        if not frames:
            return rest.decode(errors="replace"), ""
        out = b"".join(payload for stream, payload in frames if stream == 1)
        err = b"".join(payload for stream, payload in frames if stream == 2)
        return out.decode(errors="replace"), err.decode(errors="replace")

    def stats(self) -> dict[str, int]:
        """Each running container's memory use in bytes by name."""
        usage: dict[str, int] = {}
        for container in self.containers(all_states=False):
            name = str(container["Names"][0]).lstrip("/")
            data = self.call("GET", f"/containers/{container['Id']}/stats?stream=false&one-shot=true")
            if isinstance(data, dict):
                memory = data.get("memory_stats", {})
                cache = memory.get("stats", {}).get("inactive_file", 0)
                usage[name] = max(0, int(memory.get("usage", 0)) - int(cache))
        return usage

    def networks(self, label: str) -> list[dict[str, Any]]:
        """Networks carrying a label."""
        return list(self.call("GET", "/networks?filters=" + _quote(self.filters(label=[label]))) or [])

    def create_network(self, name: str, labels: dict[str, str], *, internal: bool = False) -> str:
        """Create a bridge network with explicit isolation and ownership labels."""
        return str(
            self.call(
                "POST",
                "/networks/create",
                {
                    "Name": name,
                    "Driver": "bridge",
                    "Labels": labels,
                    "Internal": internal,
                    "EnableIPv6": False,
                    "Options": {"com.docker.network.bridge.gateway_mode_ipv4": "isolated"} if internal else {},
                    "CheckDuplicate": True,
                },
                ok=(201,),
            )["Id"]
        )

    def remove_network(self, network_id: str) -> None:
        """Remove a network; one that is gone is fine."""
        status, _ = self.request("DELETE", f"/networks/{network_id}")
        if status not in (204, 404):
            raise DockerError(status, "network remove failed")

    def connect_network(self, network: str, container_id: str, aliases: Sequence[str]) -> None:
        """Join a container to a network under aliases. API v1.43 creates a container on one network only, so the others are joined before it starts."""
        self.call("POST", f"/networks/{_quote(network)}/connect", {"Container": container_id, "EndpointConfig": {"Aliases": list(aliases)}}, ok=(200,))

    def volumes(self, label: str) -> list[dict[str, Any]]:
        """Volumes carrying a label."""
        data = self.call("GET", "/volumes?filters=" + _quote(self.filters(label=[label]))) or {}
        return list(data.get("Volumes") or [])

    def image(self, reference: str) -> dict[str, Any] | None:
        """An image's description, or None when it is not present."""
        try:
            return dict(self.call("GET", f"/images/{_quote(reference, safe='/:@')}/json"))
        except DockerError as error:
            if error.status == 404:
                return None
            raise

    def pull(self, repository: str, digest: str) -> None:
        """Pull an image by its digest; the reply is a stream of progress lines and an ``error`` line is a failure."""
        status, data = self.request("POST", f"/images/create?fromImage={_quote(repository, safe='/')}&tag={_quote(digest)}", timeout=3600)
        if status != 200:
            raise DockerError(status, "pull failed")
        for line in data.decode(errors="replace").splitlines():
            try:
                progress = json.loads(line)
            except ValueError:
                continue
            if isinstance(progress, dict) and progress.get("error"):
                raise DockerError(500, "pull stream failed")

    def image_history(self, reference: str) -> list[dict[str, Any]]:
        """An image's layer history."""
        return list(self.call("GET", f"/images/{_quote(reference, safe='/:@')}/history") or [])

    def events(self, label: str) -> EventStream:
        """Open the container events stream with a label filter."""
        connection = UnixConnection(self.socket_path, 30)
        query = "filters=" + _quote(self.filters(label=[label], type=["container"]))
        try:
            connection.request("GET", f"{self.prefix}/events?{query}")
            response = connection.getresponse()
        except (OSError, http.client.HTTPException) as error:
            connection.close()
            raise DockerError(0, "events failed") from error
        if response.status != 200:
            connection.close()
            raise DockerError(response.status, "events refused")
        if connection.sock is not None:
            connection.sock.settimeout(None)
        return EventStream(connection, response)

    def attach(self, container_id: str, detach_keys: str) -> socket.socket:
        """Attach to a container with the hijacked connection; returns the raw socket after the upgrade."""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(30)
        sock.connect(self.socket_path)
        query = f"stream=1&stdin=1&stdout=1&stderr=1&detachKeys={_quote(detach_keys)}"
        lines = [
            f"POST {self.prefix}/containers/{container_id}/attach?{query} HTTP/1.1",
            "Host: docker",
            "Upgrade: tcp",
            "Connection: Upgrade",
            "Content-Length: 0",
        ]
        request = "\r\n".join(lines) + "\r\n\r\n"
        sock.sendall(request.encode())
        head = b""
        while b"\r\n\r\n" not in head:
            chunk = sock.recv(1)
            if not chunk:
                break
            head += chunk
        status_line = head.split(b"\r\n", 1)[0].decode(errors="replace")
        if " 101 " not in status_line and " 200 " not in status_line:
            sock.close()
            raise DockerError(0, "attach refused")
        sock.settimeout(None)
        return sock


def _quote(text: str, safe: str = "") -> str:
    """Percent-encode a path or query component."""
    from urllib.parse import quote

    return quote(text, safe=safe)


# ---------------------------------------------------------------------------------------------------------------------
# Compose as a specification only (D1 section 8 item 1)
# ---------------------------------------------------------------------------------------------------------------------

INTERPOLATION = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?:(:?[-?])([^}]*))?\}")
SIZE_UNITS = {"b": 1, "k": 1024, "m": 1024**2, "g": 1024**3}
COMPOSE_ENV_NAMES = (
    "POSTGRES_PASSWORD",
    "POLARIS_CLIENT_ID",
    "POLARIS_CLIENT_SECRET",
    "POLARIS_READER_CLIENT_ID",
    "POLARIS_READER_CLIENT_SECRET",
    "POLARIS_GOLD_WRITER_CLIENT_ID",
    "POLARIS_GOLD_WRITER_CLIENT_SECRET",
    "POLARIS_SILVER_WRITER_CLIENT_ID",
    "POLARIS_SILVER_WRITER_CLIENT_SECRET",
    "AWS_PROFILE",
    "AWS_REGION",
    "POLARIS_MEMORY_LIMIT",
    "POLARIS_DB_MEMORY_LIMIT",
)
ROOT_CLIENT_ID = "root"
STACK_SECRETS = {
    "AIRFLOW_DB_PASSWORD": secret_store.AIRFLOW_DB_PASSWORD,
    "AIRFLOW_FERNET_KEY": secret_store.AIRFLOW_FERNET_KEY,
    "AIRFLOW_JWT_SECRET": secret_store.AIRFLOW_JWT_SECRET,
    "AIRFLOW_API_SECRET_KEY": secret_store.AIRFLOW_API_SECRET_KEY,
    "LINEAGE_DB_PASSWORD": secret_store.MARQUEZ_DB_PASSWORD,
}
SQL_ALCHEMY_CONN = "AIRFLOW_SQL_ALCHEMY_CONN"
COMPOSE_SECRETS = {
    "POSTGRES_PASSWORD": secret_store.POSTGRES_PASSWORD,
    "POLARIS_CLIENT_SECRET": secret_store.POLARIS_ROOT_SECRET,
    "POLARIS_READER_CLIENT_ID": secret_store.POLARIS_READER_CLIENT_ID,
    "POLARIS_READER_CLIENT_SECRET": secret_store.POLARIS_READER_CLIENT_SECRET,
    "POLARIS_GOLD_WRITER_CLIENT_ID": secret_store.POLARIS_GOLD_CLIENT_ID,
    "POLARIS_GOLD_WRITER_CLIENT_SECRET": secret_store.POLARIS_GOLD_CLIENT_SECRET,
    "POLARIS_SILVER_WRITER_CLIENT_ID": secret_store.POLARIS_SILVER_CLIENT_ID,
    "POLARIS_SILVER_WRITER_CLIENT_SECRET": secret_store.POLARIS_SILVER_CLIENT_SECRET,
}
PRINCIPAL_CREDENTIALS = frozenset(name for name in COMPOSE_SECRETS if "READER" in name or "WRITER" in name)


@dataclass(frozen=True)
class Mount:
    """One bind mount of a create specification."""

    source: str
    target: str
    read_only: bool

    def body(self) -> dict[str, Any]:
        """The Engine API form."""
        return {"Type": "bind", "Source": self.source, "Target": self.target, "ReadOnly": self.read_only}


def interpolate(text: str, values: Mapping[str, str]) -> str:
    """Resolve ``${NAME}``, ``${NAME:-default}`` and ``${NAME:?message}`` against ``values`` only (D1 section 8 item 1)."""

    def replace(match: re.Match[str]) -> str:
        name, operator, argument = match.group(1), match.group(2), match.group(3) or ""
        value = values.get(name)
        if value not in (None, ""):
            return str(value)
        if operator == ":?":
            raise LauncherError(f"{name} is required: {argument}", "spec")
        if operator in (":-", "-"):
            return argument
        if value is None:
            raise LauncherError(f"{name} has no value", "spec")
        return ""

    return INTERPOLATION.sub(replace, text)


def parse_size(text: str) -> int:
    """A Compose size such as ``1024m`` or a byte count."""
    match = re.fullmatch(r"(\d+)([bkmg]?)", text.strip().lower())
    if not match:
        raise LauncherError(f"memory limit {text!r} is not a size", "spec")
    return int(match.group(1)) * SIZE_UNITS[match.group(2) or "b"]


def parse_duration_ns(text: str) -> int:
    """A Compose duration such as ``5s`` in nanoseconds."""
    match = re.fullmatch(r"(\d+)(ms|s|m)", text.strip())
    if not match:
        raise LauncherError(f"duration {text!r} is not understood", "spec")
    return int(match.group(1)) * {"ms": 10**6, "s": 10**9, "m": 60 * 10**9}[match.group(2)]


class ComposeModel:
    """The static model of one ``docker-compose.yaml``: parsed once per file hash, never interpolated until a create operation."""

    def __init__(self, path: Path) -> None:
        raw = path.read_bytes()
        self.path = path
        self.root = path.parent
        self.sha256 = hashlib.sha256(raw).hexdigest()
        document = importlib.import_module("yaml").safe_load(raw)
        self.project: str = str(document["name"])
        self.services: dict[str, dict[str, Any]] = dict(document["services"])

    def service(self, name: str) -> dict[str, Any]:
        """One service's raw model."""
        if name not in self.services:
            raise LauncherError(f"docker-compose.yaml has no service {name}", "spec")
        return self.services[name]

    def image(self, name: str) -> str:
        """The service's pinned image reference."""
        return str(self.service(name)["image"])

    def environment(self, name: str, values: Mapping[str, str]) -> dict[str, str]:
        """The service's environment with every reference resolved from ``values``."""
        raw = self.service(name).get("environment") or {}
        return {str(key): interpolate(str(value), values) for key, value in raw.items()}

    def mounts(self, name: str, values: Mapping[str, str]) -> list[Mount]:
        """The service's volumes as bind mounts, relative sources resolved against the checkout."""
        result = []
        for volume in self.service(name).get("volumes") or []:
            if isinstance(volume, str):
                parts = volume.split(":")
                source, target = parts[0], parts[1]
                read_only = len(parts) > 2 and "ro" in parts[2].split(",")
            else:
                if volume.get("type") != "bind":
                    raise LauncherError(f"service {name} has a volume that is not a bind mount", "spec")
                source, target, read_only = str(volume["source"]), str(volume["target"]), bool(volume.get("read_only"))
            source = interpolate(source, values)
            resolved = (self.root / source).resolve() if source.startswith(".") else Path(source)
            result.append(Mount(str(resolved), interpolate(target, values), read_only))
        return result

    def memory(self, name: str, values: Mapping[str, str]) -> int:
        """The service's ``mem_limit`` in bytes."""
        return parse_size(interpolate(str(self.service(name)["mem_limit"]), values))

    def healthcheck(self, name: str) -> dict[str, Any] | None:
        """The service's health check in Engine API form."""
        check = self.service(name).get("healthcheck")
        if not check:
            return None
        values = {
            "Test": list(check["test"]),
            "Interval": parse_duration_ns(str(check["interval"])),
            "Timeout": parse_duration_ns(str(check["timeout"])),
            "Retries": int(check["retries"]),
            "StartPeriod": parse_duration_ns(str(check.get("start_period", "0s"))),
        }
        # The daemon drops a zero value, so the recorded specification leaves it out too (amendment A83).
        return {key: value for key, value in values.items() if value}

    def ports(self, name: str) -> list[tuple[str, int, int]]:
        """Published ports as ``(host_ip, host_port, container_port)``."""
        result = []
        for entry in self.service(name).get("ports") or []:
            host_ip, host_port, container_port = str(entry).split(":")
            result.append((host_ip, int(host_port), int(container_port)))
        return result


@dataclass
class Rendered:
    """A create specification: the Engine API body, and the values the recorded form keeps apart."""

    name: str
    body: dict[str, Any]
    secret_env: frozenset[str]
    secret_values: frozenset[str]
    finite: bool
    deadline_utc: int | None = None
    interpolation: Mapping[str, str] = field(default_factory=dict)
    joins: dict[str, list[str]] = field(default_factory=dict)


def environment_list(env: Mapping[str, str]) -> list[str]:
    """``NAME=value`` entries in a stable order."""
    return [f"{name}={value}" for name, value in sorted(env.items())]


def secret_command_indexes(command: Sequence[str], secret_values: frozenset[str]) -> list[int]:
    """The positions of the command parts that hold a secret value."""
    return [index for index, part in enumerate(command) if any(value and value in part for value in secret_values)]


REDACTION_MARK = "[redacted]"


class SecretSources(NamedTuple):
    """Where a container holds credentials, as its create record saved them: environment names, command positions and positions inside longer values."""

    env: list[str]
    cmd: list[int]
    spans: list[dict[str, Any]]


NO_SECRET_SOURCES = SecretSources([], [], [])


def embedded_spans(key: bytes, parts: Mapping[str, str], secret_values: frozenset[str]) -> list[dict[str, Any]]:
    """Where a secret value sits inside a longer environment value or command part (a URL, an argument): the part, the offset, the length and the
    keyed commitment of the value. The record holds no secret and none of the text around it."""
    spans = []
    for where, text in sorted(parts.items()):
        for value in sorted(secret_values):
            start = text.find(value) if value and value != text else -1
            while start >= 0:
                spans.append({"in": where, "start": start, "length": len(value), "commit": commit_secret(key, value)})
                start = text.find(value, start + 1)
    return spans


def created_secrets(key: bytes, rendered: Rendered, effective_env: Mapping[str, str]) -> tuple[frozenset[str], list[int], list[dict[str, Any]]]:
    """The environment names, command positions and positions inside longer values that hold a credential in a container about to be created."""
    command = [str(part) for part in rendered.body.get("Cmd") or []]
    parts = {f"env:{name}": value for name, value in effective_env.items()}
    parts.update({f"cmd:{index}": part for index, part in enumerate(command)})
    secret_env = rendered.secret_env | frozenset(name for name in effective_env if SECRET_NAME.search(name))
    return secret_env, secret_command_indexes(command, rendered.secret_values), embedded_spans(key, parts, rendered.secret_values)


def recovered_spans(config: Mapping[str, Any], spans: Sequence[Mapping[str, Any]], key: bytes) -> set[str]:
    """The secret values cut from the inspected container at the recorded positions; a value whose commitment differs refuses the whole read."""
    parts = {f"env:{name}": value for name, _, value in (str(entry).partition("=") for entry in config.get("Env") or [])}
    parts.update({f"cmd:{index}": str(part) for index, part in enumerate(config.get("Cmd") or [])})
    found = set()
    for span in spans:
        start = int(span["start"])
        value = parts.get(span["in"], "")[start : start + int(span["length"])]
        if commit_secret(key, value) != span["commit"]:
            raise LauncherError("the container no longer holds the credentials its create record names", "degraded")
        found.add(value)
    return found


def container_secrets(info: Mapping[str, Any] | None, sources: SecretSources, key: bytes) -> frozenset[str]:
    """The credential values a container holds, read from its own inspected configuration: the environment values named in its create record or
    named like a secret, each command part the record marks as holding one and each value the record places inside a longer text."""
    config = (info or {}).get("Config") or {}
    found = set()
    for entry in config.get("Env") or []:
        name, _, value = str(entry).partition("=")
        if name in sources.env or SECRET_NAME.search(name):
            found.add(value)
    command = [str(part) for part in config.get("Cmd") or []]
    found.update(command[index] for index in sources.cmd if index < len(command))
    if info is not None:
        found |= recovered_spans(config, sources.spans, key)
    return frozenset(value for value in found if value)


def redact(text: str, values: frozenset[str]) -> str:
    """The text with every occurrence of a credential value replaced by a mark, longest values first."""
    for value in sorted(values, key=len, reverse=True):
        text = text.replace(value, REDACTION_MARK) if value else text
    return text


def commit_command(key: bytes, command: Sequence[str], indexes: Sequence[int]) -> list[str]:
    """The command with the parts at ``indexes`` replaced by their commitments."""
    return [commit_secret(key, part) if index in indexes else part for index, part in enumerate(command)]


def image_defaults(info: Mapping[str, Any] | None) -> dict[str, Any]:
    """The part of an image's configuration that a container inherits: identity, user, working folder, environment, entrypoint, command, health check."""
    config = (info or {}).get("Config") or {}
    return {
        "id": str((info or {}).get("Id", "")),
        "user": str(config.get("User") or ""),
        "workdir": str(config.get("WorkingDir") or ""),
        "env": [str(item) for item in config.get("Env") or []],
        "entrypoint": [str(item) for item in config.get("Entrypoint") or []],
        "cmd": [str(item) for item in config.get("Cmd") or []],
        "healthcheck": dict(config.get("Healthcheck") or {}),
        "labels": {str(k): str(v) for k, v in (config.get("Labels") or {}).items()},
    }


def effective_config(body: Mapping[str, Any], defaults: Mapping[str, Any]) -> dict[str, Any]:
    """What the daemon makes of a create body and the image's defaults.

    The environment merges by name (the request wins), the user, working folder and health check fall back to the image's. An absent
    entrypoint keeps the image's and an empty list clears it; the command follows the image unless the request sets its own command or a
    non-empty entrypoint (the daemon's merge rule, checked on Docker Desktop, amendment A83).
    """
    env: dict[str, str] = {}
    for entry in [*defaults.get("env", []), *(body.get("Env") or [])]:
        name, _, value = str(entry).partition("=")
        env[name] = value
    requested = body.get("Entrypoint")
    entrypoint = [str(item) for item in requested or []]
    command = [str(item) for item in body.get("Cmd") or []]
    if not entrypoint:
        command = command or list(defaults.get("cmd", []))
        entrypoint = [] if requested == [] else list(defaults.get("entrypoint", []))
    return {
        "env": env,
        "entrypoint": entrypoint,
        "cmd": command,
        "user": str(body.get("User") or defaults.get("user") or ""),
        "workdir": str(body.get("WorkingDir") or defaults.get("workdir") or ""),
        "healthcheck": dict(body.get("Healthcheck") or defaults.get("healthcheck") or {}),
    }


def hardening(host: Mapping[str, Any], spec: Mapping[str, Any] | None = None) -> dict[str, list[str]]:
    """The dropped capabilities and security options of a container. A specification keeps them only when they are set, so a specification
    recorded before they existed keeps its digest; an inspected container is read for the names its specification holds."""
    found = {"cap_drop": sorted(host.get("CapDrop") or []), "security_opt": sorted(host.get("SecurityOpt") or [])}
    return {name: value for name, value in found.items() if (value if spec is None else name in spec)}


def projection(
    body: Mapping[str, Any], key: bytes, secret_env: frozenset[str], secret_cmd: Sequence[int], defaults: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """The canonical, secret-free effective specification of a create body and the image's defaults (D1 section 8 item 1)."""
    defaults = defaults or image_defaults(None)
    host = body.get("HostConfig") or {}
    effective = effective_config(body, defaults)
    env = {name: commit_secret(key, value) if name in secret_env else value for name, value in sorted(effective["env"].items())}
    mounts = sorted([m["Source"], m["Target"], bool(m.get("ReadOnly"))] for m in host.get("Mounts") or [])
    ports = sorted(
        [container, binding["HostIp"], binding["HostPort"]] for container, bindings in (host.get("PortBindings") or {}).items() for binding in bindings
    )
    return {
        **hardening(host),
        "image": body["Image"],
        "image_id": str(defaults.get("id", "")),
        "entrypoint": effective["entrypoint"],
        "cmd": commit_command(key, effective["cmd"], secret_cmd),
        "env": env,
        "labels": {name: value for name, value in sorted((body.get("Labels") or {}).items()) if name.startswith("hai.")},
        "memory": int(host.get("Memory") or 0),
        "mounts": mounts,
        "network": host.get("NetworkMode") or "default",
        **(
            {"attached_networks": sorted((body.get("NetworkingConfig") or {}).get("EndpointsConfig") or {host.get("NetworkMode"): {}})}
            if (body.get("Labels") or {}).get(network_policy.LABEL_CLASS)
            else {}
        ),
        "init": bool(host.get("Init")),
        "restart": str((host.get("RestartPolicy") or {}).get("Name") or ""),
        "auto_remove": bool(host.get("AutoRemove")),
        "tmpfs": sorted(host.get("Tmpfs") or {}),
        "hostname": body.get("Hostname") or "",
        "extra_hosts": sorted(host.get("ExtraHosts") or []),
        "ports": ports,
        "workdir": effective["workdir"],
        "user": effective["user"],
        "tty": bool(body.get("Tty")),
        "open_stdin": bool(body.get("OpenStdin")),
        "healthcheck": effective["healthcheck"],
    }


def inspected_projection(info: Mapping[str, Any], spec: Mapping[str, Any], key: bytes, secret_env: frozenset[str], secret_cmd: Sequence[int]) -> dict[str, Any]:
    """The same projection read back from ``docker inspect``: the whole inspected configuration, with no name left out."""
    config, host = info.get("Config") or {}, info.get("HostConfig") or {}
    env = {}
    for entry in config.get("Env") or []:
        name, _, value = str(entry).partition("=")
        env[name] = commit_secret(key, value) if name in secret_env else value
    mounts = sorted([m["Source"], m["Destination"], not m.get("RW", True)] for m in info.get("Mounts") or [] if m.get("Type", "bind") == "bind")
    ports = sorted(
        [container, binding["HostIp"], binding["HostPort"]] for container, bindings in (host.get("PortBindings") or {}).items() for binding in bindings or []
    )
    labels = {name: value for name, value in sorted((config.get("Labels") or {}).items()) if name in spec["labels"]}
    health = config.get("Healthcheck") or {}
    return {
        **hardening(host, spec),
        "image": config.get("Image"),
        "image_id": str(info.get("Image") or ""),
        "entrypoint": [str(item) for item in config.get("Entrypoint") or []],
        "cmd": commit_command(key, [str(item) for item in config.get("Cmd") or []], secret_cmd),
        "env": dict(sorted(env.items())),
        "labels": labels,
        "memory": int(host.get("Memory") or 0),
        "mounts": mounts,
        "network": host.get("NetworkMode") or "default",
        **({"attached_networks": sorted((info.get("NetworkSettings") or {}).get("Networks") or {})} if "attached_networks" in spec else {}),
        "init": bool(host.get("Init")),
        "restart": str((host.get("RestartPolicy") or {}).get("Name") or ""),
        "auto_remove": bool(host.get("AutoRemove")),
        "tmpfs": sorted(host.get("Tmpfs") or {}),
        "hostname": (config.get("Hostname") or "") if spec["hostname"] else "",
        "extra_hosts": sorted(host.get("ExtraHosts") or []),
        "ports": ports,
        "workdir": str(config.get("WorkingDir") or ""),
        "user": str(config.get("User") or ""),
        "tty": bool(config.get("Tty")),
        "open_stdin": bool(config.get("OpenStdin")),
        # The daemon may add keys of its own to a health check; the keys the specification names are compared.
        "healthcheck": {k: health[k] for k in spec["healthcheck"] if k in health}
        if spec["healthcheck"] and health
        else (health if spec["healthcheck"] else {}),
    }


# ---------------------------------------------------------------------------------------------------------------------
# Templates (D1 section 7)
# ---------------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Template:
    """One closed launch shape: image, entrypoint, mounts, network and principal are fixed by the template table."""

    name: str
    service: str | None
    finite: bool
    owners: tuple[str, ...]
    needs_catalog: bool = False
    default_seconds: int = 3600
    max_seconds: int = 86400
    reader: bool = False
    guard: str | None = None
    session: str | None = None
    run_domains: tuple[str, ...] | None = None
    run_modes: tuple[str, ...] | None = None


TEMPLATES: dict[str, Template] = {
    t.name: t
    for t in (
        Template("dbt_deps", "analytics-dbt", True, ("alloc",), guard="writer", default_seconds=900),
        Template(
            "real_dbt",
            "analytics-dbt",
            True,
            ("run",),
            needs_catalog=True,
            default_seconds=43200,
            max_seconds=86400,
            run_domains=("transform",),
            run_modes=("real",),
        ),
        Template("sealed_consumer", "quality", True, ("run",), default_seconds=3600, max_seconds=7200, run_domains=("transform",), run_modes=("real",)),
        Template("fixture_dbt", "analytics-dbt", True, ("alloc", "run"), guard="reader", default_seconds=3600, run_modes=("fixture", "publish_fixture")),
        Template("fixture_init", "analytics-dbt", True, ("alloc",), default_seconds=900),
        Template("duckdb_helper", "analytics-dbt", True, ("alloc",), default_seconds=900),
        Template("quality_validate", "quality", True, ("alloc", "run"), default_seconds=3600),
        Template("quality_reconcile", "quality", True, ("alloc", "run"), default_seconds=3600),
        Template("query_reader", "analytics", False, ("alloc",), needs_catalog=True, reader=True, session="forward", default_seconds=43200),
        Template("ui_reader", "analytics-ui", False, ("alloc",), needs_catalog=True, reader=True, session="cancel", default_seconds=43200),
        Template("spark_read", "spark", True, ("run",), needs_catalog=True, default_seconds=7200, run_domains=("transform",)),
        Template("spark_write", "spark", True, ("run",), needs_catalog=True, default_seconds=43200, run_modes=("real",)),
        Template("spark_e2e", "spark", True, ("alloc",), default_seconds=7200),
        Template("pipeline_dbt", "analytics-dbt", True, ("run",), run_domains=("transform",), run_modes=("publish_fixture", "real")),
        Template("pipeline_spark", "spark", True, ("run",), run_domains=("transform",), run_modes=("publish_fixture", "real")),
        Template("pipeline_export", "quality", True, ("run",), run_domains=("transform",), run_modes=("publish_fixture", "real")),
        Template("pipeline_publish", "spark", True, ("run",), run_domains=("transform",), run_modes=("publish_fixture", "real")),
        Template("pipeline_probe", "analytics-dbt", True, ("run",), run_domains=("transform",), run_modes=("publish_fixture",)),
        Template("pipeline_proxy", None, False, ("service",)),
        *(
            Template(f"pipeline_validate_{layer}", "quality", True, ("run",), run_domains=("transform",), run_modes=("publish_fixture", "real"))
            for layer in ("silver", "gold", "cross")
        ),
        Template("gold_export", "quality", True, ("alloc", "run"), default_seconds=3600, run_domains=("transform",), run_modes=("publish_fixture", "real")),
        Template(
            "gold_publish",
            "spark",
            True,
            ("run",),
            needs_catalog=True,
            default_seconds=10800,
            run_domains=("transform",),
            run_modes=("publish_fixture", "real"),
        ),
        Template("catalog_service", None, False, ("service",)),
        Template("orchestration_service", None, False, ("stack",)),
        Template("lineage_service", None, False, ("stack",)),
        Template("lineage_dump", None, True, ("alloc",), default_seconds=120, max_seconds=300),
        Template("orchestration_check", None, True, ("alloc",), default_seconds=900, max_seconds=3600),
        Template("orchestration_dump", None, True, ("alloc",), default_seconds=900, max_seconds=1800),
        Template("probe", "analytics", True, ("diagnostic",), default_seconds=60, max_seconds=60),
    )
}
# Catalog services that run to their end: they run under the in-image deadline entrypoint like every finite job (a static binary in the
# polaris-admin-tool image, which has no Python) and the launcher enforces the same deadline as well.
FINITE_SERVICES = frozenset({"polaris-bootstrap"})
# Spark jobs by template: module, whether it writes, the folders it writes under the checkout and the closed argument grammar.
SPARK_JOBS: dict[str, dict[str, Any]] = {
    "pins": {
        "template": "spark_read",
        "module": "scripts.lakehouse.pins",
        "writes": (),
        "aws": True,
        "grammar": {"pin": None, "verify": None, "--run": "value"},
    },
    "bronze": {
        "template": "spark_write",
        "domains": ("acquisition",),
        "prefix": "lakehouse/bronze/",
        "module": "scripts.lakehouse.bronze",
        "writes": ("data/e2e/bronze_load",),
        "aws": True,
        "deployment": True,
        "grammar": {
            "--group": "value",
            "--tables": "value",
            "--bootstrap-all": "flag",
            "--repair-tables": "value",
            "--drop-removed": "flag",
            "--copies-only": "flag",
            "--diagnose": "flag",
        },
    },
    "dictionary": {
        "template": "spark_write",
        "domains": ("acquisition",),
        "module": "scripts.lakehouse.dictionary",
        "writes": (),
        "aws": True,
        "grammar": {"--all": "flag", "--tables": "value"},
    },
    "checksums": {
        "template": "spark_write",
        "domains": ("acquisition",),
        "module": "scripts.lakehouse.checksums",
        "writes": (),
        "aws": True,
        "grammar": {"--tables": "values", "--fixture": "value"},
    },
    "smoke": {"template": "spark_write", "domains": ("acquisition",), "module": "scripts.lakehouse.smoke", "writes": (), "aws": True, "grammar": {}},
    "bronze_e2e": {"template": "spark_e2e", "module": "scripts.lakehouse.run_bronze_e2e", "writes": ("data/e2e/bronze",), "aws": False, "grammar": {}},
    "pins_e2e": {"template": "spark_e2e", "module": "scripts.lakehouse.run_pins_e2e", "writes": ("data/e2e/pins",), "aws": False, "grammar": {}},
    # The offline gold publish: the cases and the single steps run on a local file-based Iceberg catalog (no Polaris, no S3, network none).
    "gold_publish_e2e": {
        "template": "spark_e2e",
        "module": "scripts.lakehouse.run_gold_publish_cases",
        "writes": ("data/e2e/gold_publish",),
        "aws": False,
        "contract": True,
        "grammar": {"--case": "values", "--stamp": "value"},
    },
    "gold_publish_local": {
        "template": "spark_e2e",
        "module": "scripts.lakehouse.gold_publish",
        "writes": ("data/e2e/gold_publish",),
        "aws": False,
        "contract": True,
        "grammar": {
            "candidates": None,
            "reconcile": None,
            "backup": None,
            "commit": None,
            "--run": "value",
            "--mode": "value",
            "--sealed-sha256": "value",
            "--accepted-result-sha256": "value",
            "--publish-dir": "value",
            "--local-warehouse": "value",
        },
    },
}
# The gold publish steps (gold plan F5; failure modes 858 to 867): the module's step, the catalog principal, the files the step needs and whether the run's
# sealed-build record (``completion``) is an input. The launcher builds every argument itself from the run; a client passes none.
GOLD_JOBS: dict[str, dict[str, Any]] = {
    "publish_candidates": {"module": "scripts.lakehouse.gold_publish", "step": "candidates", "principal": "writer", "sealed": True},
    "reconcile_candidates": {"module": "scripts.lakehouse.gold_publish", "step": "reconcile", "principal": "reader", "sealed": True},
    "catalog_backup": {"module": "scripts.lakehouse.gold_publish", "step": "backup", "principal": "writer", "sealed": True, "deployment": True},
    "commit_publish": {"module": "scripts.lakehouse.gold_publish", "step": "commit", "principal": "writer", "sealed": True},
    "gold_maintenance": {"module": "scripts.lakehouse.gold_maintenance", "step": None, "principal": "writer", "sealed": False},
}
SEALED_READERS = ("duckdb_helper", "quality_validate")
MAINTENANCE_FENCE = "gold-maintenance-fence"
GOLD_WRITE_PREFIX = {"publish_fixture": "lakehouse/gold_e2e/", "real": "lakehouse/gold/"}
GOLD_CONTRACT = "data_contracts/lakehouse/gold_contract.json"
SILVER_WRITE_PREFIX = {mode: f"lakehouse/{namespace}/" for mode, namespace in pipeline_protocol.SILVER_NAMESPACE.items()}
SILVER_CONTRACT = "data_contracts/lakehouse/silver_publish.json"
LAYER_WRITE_PREFIX = {"silver": SILVER_WRITE_PREFIX, "gold": GOLD_WRITE_PREFIX}
LAYER_CONTRACT = {"silver": SILVER_CONTRACT, "gold": GOLD_CONTRACT}
SEALED_FOLDER = "sealed"
GOLD_WRITER_ENV = ("POLARIS_GOLD_WRITER_CLIENT_ID", "POLARIS_GOLD_WRITER_CLIENT_SECRET")
READER_ENV = ("POLARIS_READER_CLIENT_ID", "POLARIS_READER_CLIENT_SECRET")
SHA256_HEX = re.compile(r"[a-f0-9]{64}")
DBT_COMMANDS = ("build", "run", "seed", "snapshot", "test", "run-operation", "ls", "parse", "compile", "debug")
DBT_OPTIONS_VALUES = {"--select": "many", "-s": "many", "--exclude": "many", "--selector": "one", "--indirect-selection": "choice"}
DBT_OPTIONS_FLAGS = ("--full-refresh", "--fail-fast", "-x")
DBT_LS_OPTIONS = {"--output": "one", "--output-keys": "many", "--resource-type": "one", "--quiet": "flag"}
INDIRECT_CHOICES = ("eager", "cautious", "buildable", "empty")
ARG_VALUE = re.compile(r"[A-Za-z0-9_.,:=/+@*\[\]-]{1,300}")
# Macros a ``run-operation`` may call; initially none (D1 section 7 item 1).
RUN_OPERATION_ALLOWLIST: dict[str, dict[str, type]] = {}
DBT_VARS_ALLOWED: dict[str, type] = {}
DUCKDB_QUERY_FLAGS = ("-csv", "-json", "-markdown", "-noheader", "-header", "-readonly")


def validate_dbt_args(template: str, args: Sequence[str]) -> list[str]:
    """Check ``args`` against the closed dbt argument schema and return them normalized (D1 section 7 item 1)."""
    if not args or args[0] not in DBT_COMMANDS:
        raise LauncherError(f"the first dbt argument must be one of {', '.join(DBT_COMMANDS)}", "args")
    command = args[0]
    result = [command]
    seen: set[str] = set()
    index = 1
    while index < len(args):
        token = args[index]
        name, equals, attached = token.partition("=") if token.startswith("--") else (token, "", "")
        kind: str | None = DBT_OPTIONS_VALUES.get(name)
        if kind is None and name in DBT_OPTIONS_FLAGS:
            kind = "flag"
        if kind is None and name == "--store-failures":
            if command not in ("build", "test") or template != "real_dbt":
                raise LauncherError("--store-failures is allowed only for build and test on real_dbt", "args")
            kind = "flag"
        if kind is None and command == "ls" and name in DBT_LS_OPTIONS:
            kind = DBT_LS_OPTIONS[name]
        if kind is None and command == "run-operation" and name == "--args":
            kind = "one"
        if kind is None:
            raise LauncherError(f"dbt option {token!r} is not in the closed schema", "args")
        if name in seen and kind != "many":
            raise LauncherError(f"dbt option {name} repeats", "args")
        seen.add(name)
        values: list[str] = []
        if kind == "flag":
            if equals:
                raise LauncherError(f"dbt option {name} takes no value", "args")
            index += 1
        elif equals:
            values, index = [attached], index + 1
        else:
            index += 1
            while index < len(args) and not args[index].startswith("-") and (kind == "many" or not values):
                values.append(args[index])
                index += 1
            if kind == "many" and not values or kind != "many" and len(values) != 1:
                raise LauncherError(f"dbt option {name} needs a value", "args")
        for value in values:
            if name == "--args":
                continue
            if not ARG_VALUE.fullmatch(value):
                raise LauncherError(f"dbt value {value!r} has characters outside the closed schema", "args")
        if kind == "choice" and values[0] not in INDIRECT_CHOICES:
            raise LauncherError(f"--indirect-selection must be one of {', '.join(INDIRECT_CHOICES)}", "args")
        result += [name, *values] if kind != "flag" else [name]
    if command == "run-operation":
        validate_run_operation(args)
    return result


def validate_run_operation(args: Sequence[str]) -> None:
    """A ``run-operation`` names an allowlisted macro and JSON arguments typed by the macro's declaration."""
    if len(args) < 2 or args[1] not in RUN_OPERATION_ALLOWLIST:
        raise LauncherError("run-operation names a macro that is not in the launcher's allowlist", "args")
    declared = RUN_OPERATION_ALLOWLIST[args[1]]
    if "--args" in args:
        try:
            supplied = json.loads(args[args.index("--args") + 1])
        except (ValueError, IndexError) as error:
            raise LauncherError("run-operation --args is not JSON", "args") from error
        if not isinstance(supplied, dict) or any(key not in declared or not isinstance(value, declared[key]) for key, value in supplied.items()):
            raise LauncherError("run-operation --args do not match the macro's declared types", "args")


def validate_spark_args(job: str, args: Sequence[str]) -> list[str]:
    """Check a Spark job's arguments against its closed grammar."""
    grammar = SPARK_JOBS[job]["grammar"]
    result: list[str] = []
    index = 0
    while index < len(args):
        token = args[index]
        if token not in grammar:
            raise LauncherError(f"{job} argument {token!r} is not in the closed schema", "args")
        kind = grammar[token]
        result.append(token)
        index += 1
        if kind in ("value", "values"):
            values: list[str] = []
            while index < len(args) and not args[index].startswith("-") and (kind == "values" or not values):
                values.append(args[index])
                index += 1
            if not values or not all(ARG_VALUE.fullmatch(value) for value in values):
                raise LauncherError(f"{job} argument {token} needs plain values", "args")
            result += values
    return result


def validate_duckdb_flags(flags: Sequence[str]) -> list[str]:
    """Output flags an interactive or helper DuckDB may carry."""
    for flag in flags:
        if flag not in DUCKDB_QUERY_FLAGS:
            raise LauncherError(f"DuckDB flag {flag!r} is not in the closed schema", "args")
    return list(flags)


def resolve_checked(path: Path, what: str) -> Path:
    """``path`` resolved; a symlink loop (``RuntimeError`` on Python 3.12, ``OSError`` later) refuses the mount: what lies behind it cannot be checked."""
    try:
        return path.resolve()
    except (RuntimeError, OSError) as error:
        raise LauncherError(f"{what}: {path} is a symlink loop or cannot be resolved, so it cannot be checked", "mount") from error


def inside(path: Path, roots: Sequence[Path]) -> bool:
    """True when the resolved path is equal to or inside one of the roots; a path or a root that cannot be resolved refuses the mount."""
    target, folders = resolve_checked(path, "path"), [resolve_checked(root, "allowed root") for root in roots]
    return any(target == folder or target.is_relative_to(folder) for folder in folders)


# A mount source is searched this far for a secrets folder; a larger tree is refused rather than admitted unchecked.
MOUNT_SEARCH_LIMIT = 200_000


def folded(path: Path) -> tuple[str, ...]:
    """Path components in one letter case: the Mac's file system treats ``Data`` and ``data`` as the same folder."""
    return tuple(part.casefold() for part in path.parts)


def secrets_part(parts: tuple[str, ...]) -> bool:
    """True when folded components include a ``lakehouse/secrets`` pair (every checkout keeps its secrets in ``data/lakehouse/secrets``)."""
    return ("lakehouse", "secrets") in zip(parts, parts[1:], strict=False)


def identity(path: Path) -> tuple[int, int] | None:
    """The (device, inode) of an existing path, or None."""
    try:
        stat = path.stat()
    except OSError:
        return None
    return (stat.st_dev, stat.st_ino)


def identities(path: Path) -> set[tuple[int, int]]:
    """The (device, inode) of a path and each of its existing ancestors, so another spelling of the same folder is recognized."""
    return {found for found in (identity(candidate) for candidate in (path, *path.parents)) if found is not None}


def resolved_in(source: Path, path: Path) -> Path:
    """``path`` resolved for the check of the mount source ``source``; a loop refuses the mount."""
    return resolve_checked(path, f"mount source {source}")


def held_secrets(source: Path) -> Path | None:
    """The first secrets folder (any letter case) at any depth below a source folder, nearest first.

    The protected name is recognized before a link is set aside, so a link named ``secrets`` counts as the folder. A directory link that
    stays inside the source is followed (each real folder once), because a relative link keeps working inside one bind mount; a link out of
    the source points to a host path the container cannot reach through that mount. A folder that cannot be read refuses the mount.
    Each entry's name is judged under its folder's lexical name and its folder's resolved name (and a link inside the source by its own
    target), before folder deduplication can set either spelling aside: an alias of ``lakehouse`` reached first cannot hide its ``secrets`` child.
    """
    if not source.is_dir():
        return None
    root = resolved_in(source, source)
    queue, visited, seen = [source], {identity(root)}, 0
    while queue:
        folder = queue.pop(0)
        try:
            entries = sorted(os.scandir(folder), key=lambda entry: entry.name)
            parent_names = {folded(folder)[-1:], folded(resolved_in(source, folder))[-1:]}
        except OSError as error:
            raise LauncherError(f"mount source {source}: {folder} cannot be read to check for a secrets folder ({error})", "mount") from error
        for entry in entries:
            child = Path(entry.path)
            if entry.is_symlink() or entry.is_dir(follow_symlinks=False):
                spellings = [(*parent, entry.name.casefold()) for parent in parent_names]
                if entry.is_symlink():
                    with contextlib.suppress(OSError):  # a link to a secrets folder inside the source, whatever the link is named
                        target = resolved_in(source, child)
                        if target == root or target.is_relative_to(root):
                            spellings.append(folded(target)[-2:])
                if any(secrets_part(spelling) for spelling in spellings):
                    return child
            if not entry.is_dir(follow_symlinks=True):
                continue
            if entry.is_symlink():
                target = resolved_in(source, child)
                if not (target == root or target.is_relative_to(root)):
                    continue
            key = identity(child)
            if key is None or key in visited:
                continue
            visited.add(key)
            seen += 1
            if seen > MOUNT_SEARCH_LIMIT:
                raise LauncherError(f"mount source {source} is too large to check for a secrets folder", "mount")
            queue.append(child)
    return None


def within_prefix(path: str, prefix: str) -> bool:
    """Whether a storage path lies inside a prefix, matched on whole path segments; a path with an empty, dot or traversal segment never does."""
    return run_lock.covers(prefix, path)


def publish_contract_problem(layer: str, mode: str, checkout: Path) -> str | None:
    """What refuses a publish render, or None: the checkout's contract for the layer must send the run's mode to that mode's prefix."""
    wanted = LAYER_WRITE_PREFIX[layer].get(mode)
    if wanted is None:
        return f"run mode {mode!r} has no {layer} write prefix"
    contract = LAYER_CONTRACT[layer]
    try:
        namespace = json.loads((checkout / contract).read_text())["publish"]["namespaces"][mode]
    except (OSError, ValueError, KeyError, TypeError):
        return f"{contract} names no {layer} namespace for mode {mode}"
    if f"lakehouse/{namespace}/" != wanted:
        return f"{contract} sends mode {mode} to lakehouse/{namespace}/; {layer} writes for this mode go only under {wanted}"
    return None


def container_path(text: str, prefix: str) -> str:
    """A container-side path below ``prefix`` with no traversal."""
    parts = text.split("/")
    if not text.startswith(prefix) or ".." in parts or "" in parts[1:] or not re.fullmatch(r"[A-Za-z0-9_./=,@-]+", text):
        raise LauncherError(f"container path {text!r} must be a plain path below {prefix}", "args")
    return text


@dataclass
class RenderInput:
    """Everything a template render needs from the attempt and its owner."""

    template: str
    owner_name: str
    launch_id: str
    attempt_n: int
    generation: int
    op_id: str
    values: Mapping[str, str]
    deadline_utc: int
    args: Sequence[str]
    params: Mapping[str, Any]
    labels: Mapping[str, str]
    snapshot: Path | None = None
    run_folder: Path | None = None
    provenance: Mapping[str, str] = field(default_factory=dict)
    image_entrypoint: Sequence[str] = ()
    completion: Mapping[str, Any] | None = None
    run_mode: str = ""
    accepted_result: str = ""
    pipeline: Mapping[str, Any] = field(default_factory=dict)


class Renderer:
    """Builds create specifications from the Compose model, the template table and validated values (D1 section 8 item 1)."""

    def __init__(self, config: Config, compose: ComposeModel, checkout: Path, main: Path, key: bytes) -> None:
        self.config = config
        self.compose = compose
        self.checkout = checkout
        self.main = main
        self.key = key
        self.interpolated: dict[str, str] = {}
        self.fetched: set[str] = set()
        self.recording: set[str] | None = None
        self.supplied: Mapping[str, Fetched] | None = None
        self.builders = {
            "fixture_dbt": self.render_fixture_dbt,
            "fixture_init": self.render_fixture_init,
            "duckdb_helper": self.render_duckdb_helper,
            "dbt_deps": self.render_dbt_deps,
            "quality_validate": self.render_quality_validate,
            "quality_reconcile": self.render_quality_reconcile,
            "query_reader": self.render_query_reader,
            "ui_reader": self.render_ui_reader,
            "real_dbt": self.render_real_dbt,
            "sealed_consumer": self.render_sealed_consumer,
            "spark_read": self.render_spark_read,
            "spark_write": self.render_spark_write,
            "spark_e2e": self.render_spark_e2e,
            "pipeline_dbt": self.render_pipeline,
            **{f"pipeline_validate_{layer}": self.render_pipeline_validate for layer in ("silver", "gold", "cross")},
            "pipeline_spark": self.render_pipeline,
            "pipeline_export": self.render_pipeline,
            "pipeline_publish": self.render_pipeline,
            "pipeline_probe": self.render_pipeline,
            "pipeline_proxy": self.render_pipeline_proxy,
            "gold_export": self.render_gold_export,
            "gold_publish": self.render_gold_publish,
            "probe": self.render_probe,
            "orchestration_check": self.render_orchestration_check,
            "orchestration_dump": self.render_orchestration_dump,
            "lineage_dump": self.render_lineage_dump,
        }

    def network_name(self, registry_id: str) -> str:
        """The launcher-created network of the catalog services and their clients."""
        return f"hai-{registry_id[:8]}-net"

    def compose_values(self, wanted: Sequence[str], base: Mapping[str, str]) -> dict[str, str]:
        """Interpolation values: the allocation's, ``HOME``, the project AWS settings and the secret values the service uses, read from Parameter Store
        into memory for this call only."""
        values = {**base, "HOME": str(account_home())}
        needed = [name for name in wanted if name in COMPOSE_ENV_NAMES and name not in values]
        if needed:
            values.update(self.secret_values([name for name in needed if name in COMPOSE_SECRETS]))
            if "POLARIS_CLIENT_ID" in needed:
                values["POLARIS_CLIENT_ID"] = ROOT_CLIENT_ID
            if any(name in needed for name in ("AWS_PROFILE", "AWS_REGION")):
                settings = self.config.aws_settings()
                values.update({name: settings[name] for name in ("AWS_PROFILE", "AWS_REGION") if name in needed})
        self.interpolated.update({name: values[name] for name in wanted if name in values})
        return values

    def secret_of(self, name: str) -> str:
        """The value of a managed secret. While recording, a placeholder, and the name is noted for the fetch worker. Otherwise the value the worker
        fetched, or the error that stopped it. A render outside the launcher (a test registry) reads the one secret itself, under the fetch deadline."""
        if self.recording is not None:
            self.recording.add(name)
            return "recorded-secret"
        outcome: Fetched | None
        if self.supplied is None:
            if self.config.secrets_endpoint is None:
                raise LauncherError(f"secret {name} must be fetched by the fetch worker", "spec")
            outcome = secret_store.fetch_all(
                self.config.catalog_checkout or self.config.root, self.config.secrets_endpoint, [name], self.config.secret_fetch_seconds
            )[name]
        else:
            outcome = self.supplied.get(name) or secret_store.SecretStoreError(f"secret {name} was not fetched for this render")
        if isinstance(outcome, secret_store.SecretStoreError):
            raise outcome
        if not isinstance(outcome, str):
            raise secret_store.SecretStoreError(f"secret {name} was not fetched for this render")
        return outcome

    def session_of(self, role: str, deadline_utc: int, label: str) -> pipeline_session.RoleSession:
        """The role's session for a job that ends at ``deadline_utc``. While recording, a placeholder, and the session's name is noted for the fetch
        worker. Otherwise the session the worker fetched for this render, or the error that stopped it. A render outside the launcher (a test registry)
        asks for the session itself. A session that no longer reaches the deadline is refused. The values join the credentials that the container's
        record commits and its output hides."""
        name = pipeline_session.name_of(role)
        if self.recording is not None:
            self.recording.add(name)
            return pipeline_session.placeholder(role)
        outcome: Fetched | None
        if self.supplied is None:
            if self.config.sts_endpoint is None:
                raise pipeline_session.SessionError(f"the {role} role session must be fetched by the fetch worker")
            outcome = self.config.role_session(role, deadline_utc, pipeline_session.session_name(label))
        else:
            outcome = self.supplied.get(name)
        if isinstance(outcome, secret_store.SecretStoreError):
            raise outcome
        if not isinstance(outcome, pipeline_session.RoleSession) or outcome.role != role:
            raise pipeline_session.SessionError(f"the {role} role session was not fetched for this render")
        if not outcome.covers(deadline_utc):
            raise pipeline_session.SessionError(f"the {role} role session no longer reaches the job's deadline")
        self.fetched.update(outcome.secret_values())
        return outcome

    def secret_values(self, names: Sequence[str]) -> dict[str, str]:
        """The value of each named compose variable from its secret; a missing secret refuses the render and names the secret."""
        found = {}
        for name in names:
            try:
                found[name] = self.secret_of(COMPOSE_SECRETS[name])
            except secret_store.SecretMissing as error:
                if name not in PRINCIPAL_CREDENTIALS:
                    raise LauncherError(f"{error}; set up the catalog first", "spec") from None
            except secret_store.SecretStoreError as error:
                raise LauncherError(str(error), "spec") from None
        self.fetched.update(found.values())
        return found

    def stack_values(self, service: str) -> dict[str, str]:
        """The secret values the orchestration or lineage service's model names, read from Parameter Store into memory; a missing secret refuses the render."""
        wanted = self.service_names(service)
        names = [name for name in wanted if name in STACK_SECRETS]
        if SQL_ALCHEMY_CONN in wanted and "AIRFLOW_DB_PASSWORD" not in names:
            names.append("AIRFLOW_DB_PASSWORD")
        if not names:
            return {}
        found = {}
        for name in names:
            try:
                found[name] = self.secret_of(STACK_SECRETS[name])
            except secret_store.SecretStoreError as error:
                raise LauncherError(f"{error}; run the stack setup first", "spec") from None
        if SQL_ALCHEMY_CONN in wanted:
            found[SQL_ALCHEMY_CONN] = f"postgresql+psycopg2://airflow:{found['AIRFLOW_DB_PASSWORD']}@postgres:5432/airflow"
        self.fetched.update(found.values())
        return {name: value for name, value in found.items() if name in wanted}

    def service_names(self, service: str) -> list[str]:
        """Variable names a service's model references."""
        return sorted(set(re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)", json.dumps(self.compose.service(service)))))

    def image(self, service: str) -> str:
        """The service's image, as a test registry's override map may rename it."""
        reference = self.compose.image(service)
        return self.config.images.get(reference, reference)

    def checkouts(self) -> list[Path]:
        """Every checkout whose secrets folder is protected, whatever the registry: this one, the launcher's main one, the real main
        checkout of this repository and each worktree under either main checkout's ``data/worktrees``."""
        mains = [self.main]
        with contextlib.suppress(OSError, run_lock.RegistryError):  # outside a Git checkout (a synthetic test root) the listed checkouts still apply
            mains.append(run_lock.main_checkout(self.config.root))
        found = [self.checkout, *mains]
        for main in mains:
            worktrees = main / "data" / "worktrees"
            if worktrees.is_dir():
                found += sorted(entry for entry in worktrees.iterdir() if entry.is_dir())
        return list(dict.fromkeys(resolve_checked(path, "checkout") for path in found))

    def check_mounts(self, mounts: Sequence[Mount]) -> None:
        """Refuse a source that equals, is inside or contains the token root or any checkout's secrets folder (D1 section 7 item 3).

        The resolved source decides, also for an exact credential path: a link that leads into a protected folder is refused.
        """
        protected = [self.config.token_root, self.config.registry.path / "launcher_test.json"]
        protected += [checkout / "data/lakehouse/secrets" for checkout in self.checkouts()]
        exact_credentials = {str(account_home() / ".aws")}
        for mount in mounts:
            source = Path(mount.source)
            if mount.source in exact_credentials and not mount.read_only:
                raise LauncherError("a credential mount must be read-only", "mount")
            resolved = resolve_checked(source, f"mount source {mount.source}")
            # The path as given is checked before it is resolved, so a link named ``secrets`` cannot pass as the folder it points to.
            if secrets_part(folded(source.absolute())) or secrets_part(folded(resolved)):
                raise LauncherError(f"mount source {mount.source} lies in a secrets folder", "mount")
            # A folder that holds any checkout's secrets folder at any depth, listed or not, in any letter case.
            held = held_secrets(resolved)
            if held is not None:
                raise LauncherError(f"mount source {mount.source} contains a secrets folder ({held})", "mount")
            source_id, source_line = identity(resolved), identities(resolved)
            for guarded in protected:
                target = resolve_checked(guarded, f"protected folder {guarded.name}")
                # Compared by spelling in one letter case and by file identity, so a case variant or another name for the same folder still matches.
                spelled = folded(resolved)[: len(folded(target))] == folded(target) or folded(target)[: len(folded(resolved))] == folded(resolved)
                target_id = identity(target)
                inside_target = target_id is not None and target_id in source_line  # the protected folder is the source or one of its ancestors
                holds_target = source_id is not None and source_id in identities(target)  # the source is one of the protected folder's ancestors
                if spelled or inside_target or holds_target:
                    raise LauncherError(f"mount source {mount.source} equals, is inside or contains a protected folder ({guarded.name})", "mount")

    def allowed(self, path: str, what: str, extra_roots: Sequence[Path] = ()) -> Path:
        """A host path from a request: absolute, resolved and inside the data folder of this checkout or the main checkout (or a named extra root)."""
        candidate = Path(path)
        # The path as requested, before any link in it is resolved: a link named ``secrets`` cannot pass as the folder it points to.
        if candidate.is_absolute() and secrets_part(folded(candidate)):
            raise LauncherError(f"{what} {path!r} lies in a secrets folder", "mount")
        roots = [self.checkout / "data", self.main / "data", *self.config.allowed_roots, *extra_roots]
        if not candidate.is_absolute() or not inside(candidate, roots):
            raise LauncherError(f"{what} {path!r} is outside the allowed folders", "args")
        return candidate

    def assemble(
        self,
        inp: RenderInput,
        name: str,
        image: str,
        entrypoint: Sequence[str],
        command: Sequence[str],
        env: Mapping[str, str],
        mounts: Sequence[Mount],
        memory: int,
        network: str,
        *,
        ports: Sequence[tuple[str, int, int]] = (),
        workdir: str = "",
        healthcheck: Mapping[str, Any] | None = None,
        tty: bool = False,
        open_stdin: bool = False,
        aliases: Sequence[str] = (),
        finite: bool = True,
        extra_labels: Mapping[str, str] | None = None,
        secrets: Mapping[str, str] | None = None,
        tmpfs: Sequence[str] = (),
        hostname: str = "",
        user: str = "",
        cap_drop: Sequence[str] = (),
        security_opt: Sequence[str] = (),
    ) -> Rendered:
        """The Engine API create body, with the deadline entrypoint in front of a finite job's command."""
        self.check_mounts(mounts)
        environment = dict(env)
        if DEADLINE_RELAY_VARIABLE in environment:
            raise LauncherError(f"{DEADLINE_RELAY_VARIABLE} belongs to the deadline entrypoint; a job may not set it", "spec")
        secret_values = frozenset(value for value in (*(secrets or {}).values(), *self.fetched) if value)
        secret_env = frozenset(name for name, value in environment.items() if SECRET_NAME.search(name) or value in secret_values)
        secret_values |= frozenset(environment[name] for name in secret_env if environment[name])
        command = list(command)
        wrap = finite
        if finite:
            environment["HAI_DEADLINE_UTC"] = str(inp.deadline_utc)
        body: dict[str, Any] = {
            "Image": image,
            "Cmd": command,
            "Env": environment_list(environment),
            "Labels": {**inp.labels, **(extra_labels or {})},
            "HostConfig": {
                "Memory": memory,
                "NetworkMode": network,
                "Init": True,
                "Mounts": [mount.body() for mount in mounts],
                "RestartPolicy": {"Name": "no"},
                "AutoRemove": False,
            },
        }
        if wrap or entrypoint:
            # Only a set entrypoint is sent: the daemon reads an empty list as "clear the image's entrypoint" (amendment A83).
            body["Entrypoint"] = [DEADLINE_ENTRYPOINT, *entrypoint] if wrap else list(entrypoint)
        if hostname:
            # A container with no network still needs its own name to resolve (the JVM fails otherwise), so the name maps to the loopback.
            body["Hostname"] = hostname
            body["HostConfig"]["ExtraHosts"] = [f"{hostname}:127.0.0.1"]
        if tmpfs:
            # Scratch space for a job whose project is read-only: memory-backed, writable by the image's unprivileged user, gone with the container.
            body["HostConfig"]["Tmpfs"] = dict.fromkeys(tmpfs, "rw,mode=1777,size=64m")
        if workdir:
            body["WorkingDir"] = workdir
        if user:
            body["User"] = user
        if cap_drop:
            body["HostConfig"]["CapDrop"] = list(cap_drop)
        if security_opt:
            body["HostConfig"]["SecurityOpt"] = list(security_opt)
        if tty or open_stdin:
            body.update({"Tty": tty, "OpenStdin": open_stdin, "StdinOnce": False, "AttachStdin": open_stdin, "AttachStdout": True, "AttachStderr": True})
        if ports:
            body["ExposedPorts"] = {f"{container}/tcp": {} for _, _, container in ports}
            body["HostConfig"]["PortBindings"] = {f"{container}/tcp": [{"HostIp": ip, "HostPort": str(host)}] for ip, host, container in ports}
        if healthcheck:
            body["Healthcheck"] = dict(healthcheck)
        if aliases and network not in ("none", "default"):
            body["NetworkingConfig"] = {"EndpointsConfig": {network: {"Aliases": list(aliases)}}}
        interpolation = dict(self.interpolated)
        self.interpolated.clear()
        self.fetched.clear()
        return Rendered(name, body, secret_env, secret_values, finite, inp.deadline_utc if finite else None, interpolation)

    def render(self, inp: RenderInput, registry_id: str) -> Rendered:
        """Render one template. Unknown templates and parameters are refused."""
        template = TEMPLATES[inp.template]
        builder = self.builders[inp.template]
        name = f"hai-{registry_id[:8]}-{inp.owner_name}-{inp.launch_id}-a{inp.attempt_n}"
        try:
            rendered: Rendered = builder(inp, template, name, registry_id)
        except KeyError as error:
            raise LauncherError(f"template {inp.template} needs the parameter or setting {error}", "args") from error
        except (TypeError, ValueError, AttributeError) as error:
            raise LauncherError(f"template {inp.template} parameters are malformed: {type(error).__name__}", "args") from error
        return rendered

    # -- helpers shared by the Compose-backed templates ------------------------------------------------------------

    def resources_mount(self) -> Mount:
        """The shared DuckDB resource script, read-only."""
        return Mount(str(self.checkout / "services/analytics/resources.sql"), "/opt/analytics/resources.sql", True)

    def only_params(self, inp: RenderInput, allowed: Sequence[str]) -> None:
        """Refuse a parameter the template does not define."""
        unknown = sorted(set(inp.params) - set(allowed))
        if unknown:
            raise LauncherError(f"template {inp.template} has no parameter {', '.join(unknown)}", "args")

    def allocation_env(self, inp: RenderInput, names: Sequence[str]) -> dict[str, str]:
        """The allocation's typed counts and sizes."""
        return {name: inp.values[name] for name in names}

    # -- templates -------------------------------------------------------------------------------------------------

    def case_mounts(self, inp: RenderInput, writable: bool) -> tuple[list[Mount], str]:
        """The case folder mounted at its container path, and that container path."""
        host = self.allowed(str(inp.params["case_host"]), "case_host")
        inside_container = container_path(str(inp.params["case_container"]), "/workspace/out/")
        if not host.is_dir():
            raise LauncherError(f"case folder {host} does not exist", "args")
        return [Mount(str(host.resolve()), inside_container, not writable)], inside_container

    def render_fixture_dbt(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """``dbt`` on a case project with no network; the package folder is read-only."""
        self.only_params(inp, ("case_host", "case_container", "packages_host", "dbt_vars"))
        mounts, case = self.case_mounts(inp, writable=True)
        packages = self.allowed(str(inp.params["packages_host"]), "packages_host")
        mounts += [Mount(str(packages.resolve()), "/workspace/out/dbt_packages", True), self.resources_mount()]
        args = validate_dbt_args(inp.template, inp.args)
        command = [*args, "--target", "fixture", "--project-dir", f"{case}/project", "--profiles-dir", f"{case}/project", *self.vars_flag(inp)]
        env = {
            **self.allocation_env(inp, ("JOB_MEMORY_LIMIT", "DUCKDB_MEMORY_LIMIT", "JOB_THREADS")),
            "STAGING_E2E_CASE": case,
            "DBT_TARGET_PATH": f"{case}/target",
            "DBT_LOG_PATH": f"{case}/logs",
            "DBT_PACKAGES_INSTALL_PATH": "/workspace/out/dbt_packages",
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        }
        return self.assemble(inp, name, self.image("analytics-dbt"), ["dbt"], command, env, mounts, int(inp.values["JOB_MEMORY_LIMIT"]), "none")

    def vars_flag(self, inp: RenderInput) -> list[str]:
        """``--vars`` from the typed ``dbt_vars`` parameter: only the names in the launcher's list, with their types."""
        supplied = inp.params.get("dbt_vars") or {}
        if not isinstance(supplied, dict) or any(
            key not in DBT_VARS_ALLOWED or not isinstance(value, DBT_VARS_ALLOWED[key]) for key, value in supplied.items()
        ):
            raise LauncherError("dbt_vars names a variable or type outside the launcher's list", "args")
        return ["--vars", json.dumps(supplied, sort_keys=True)] if supplied else []

    def render_fixture_init(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """``duckdb`` writing a case's fixture bronze database from the case's SQL, no network."""
        self.only_params(inp, ("case_host", "case_container", "database", "sql_file"))
        mounts, case = self.case_mounts(inp, writable=True)
        database = container_path(str(inp.params["database"]), case + "/")
        sql_file = container_path(str(inp.params["sql_file"]), case + "/")
        command = [database, "-cmd", ".read /opt/analytics/resources.sql", "-c", f".read {sql_file}"]
        env = self.allocation_env(inp, ("JOB_MEMORY_LIMIT", "DUCKDB_MEMORY_LIMIT", "JOB_THREADS"))
        return self.assemble(
            inp, name, self.image("analytics-dbt"), ["duckdb"], command, env, [*mounts, self.resources_mount()], int(inp.values["JOB_MEMORY_LIMIT"]), "none"
        )

    def render_duckdb_helper(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """Read-only ``duckdb`` queries on named files, no network. With ``sealed_run`` the database is the run's sealed copy and the container's
        entrypoint checks it before ``duckdb`` runs (D1 section 10 item 6)."""
        self.only_params(inp, ("mounts", "database", "init", "sql", "flags", build_provenance.SEALED_RUN_PARAM))
        sealed = build_provenance.SEALED_RUN_PARAM in inp.params
        if sealed and inp.params.get("database"):
            raise LauncherError("a reader of a sealed run takes no database: it reads the sealed copy", "args")
        mounts = [self.resources_mount()]
        container_roots = []
        for entry in inp.params.get("mounts", []) if sealed else inp.params["mounts"]:
            host = self.allowed(str(entry["host"]), "mount host")
            target = container_path(str(entry["container"]), "/workspace/")
            if sealed and (target + "/").startswith("/workspace/sealed/"):
                raise LauncherError("a mount may not cover the sealed copy", "args")
            mounts.append(Mount(str(host.resolve()), target, True))
            container_roots.append(target)
        command: list[str] = []
        if sealed:
            command = [self.SEALED_DATABASE, "-readonly"]
        elif inp.params.get("database"):
            command = [container_path(str(inp.params["database"]), "/workspace/"), "-readonly"]
        command += ["-cmd", ".read /opt/analytics/resources.sql"]
        if inp.params.get("init"):
            command += ["-init", container_path(str(inp.params["init"]), "/workspace/")]
        command += [*validate_duckdb_flags(inp.params.get("flags") or ["-csv", "-noheader"]), "-c", str(inp.params["sql"])]
        env = self.allocation_env(inp, ("JOB_MEMORY_LIMIT", "DUCKDB_MEMORY_LIMIT", "JOB_THREADS"))
        entrypoint = ["duckdb"]
        if sealed:
            entrypoint, command, extra = self.sealed_reader(inp, entrypoint, command)
            mounts += extra
        return self.assemble(inp, name, self.image("analytics-dbt"), entrypoint, command, env, mounts, int(inp.values["JOB_MEMORY_LIMIT"]), "none")

    SEALED_DATABASE = "/workspace/sealed/build.duckdb"
    SEALED_CHECK = 'python "$1" consume --sealed "$2" --expected-sha256 "$3" --expected-marker "$4" --print-result && shift 4 && exec "$@"'

    def sealed_reader(self, inp: RenderInput, entrypoint: Sequence[str], command: Sequence[str]) -> tuple[list[str], list[str], list[Mount]]:
        """Put a reader behind the consumer entrypoint: the same container hashes the sealed copy it attached, prints the check as the first line of
        standard error and runs the reader only if the digest and the marker are the recorded ones (D1 section 10 item 6).

        Returns the entrypoint, the command and the mounts (the sealed copy and the entrypoint's code, both read-only). The launcher takes the digest and the
        expected marker from its own record of the sealed run; the caller names only the run.
        """
        digest = inp.provenance.get(build_provenance.VARIABLE_SEALED_SHA, "")
        expected = inp.provenance.get(build_provenance.VARIABLE_SEALED_EXPECTED, "")
        if inp.run_folder is None or not build_provenance.SHA256.fullmatch(digest) or not expected:
            raise LauncherError("a reader of a sealed run needs the sealed run", "spec")
        mounts = [
            Mount(str(inp.run_folder / build_provenance.SEALED_FOLDER / f"{digest}.duckdb"), self.SEALED_DATABASE, True),
            Mount(str(self.checkout / "scripts/lakehouse/provenance.py"), "/opt/hai/provenance.py", True),
        ]
        return ["sh", "-c", self.SEALED_CHECK, "sh"], ["/opt/hai/provenance.py", self.SEALED_DATABASE, digest, expected, *entrypoint, *command], mounts

    def render_dbt_deps(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """``dbt deps`` from the lock file into the shared package folder, the only writable mount."""
        self.only_params(inp, ("packages_host", "project_host"))
        packages = self.allowed(str(inp.params["packages_host"]), "packages_host")
        project = Path(str(inp.params.get("project_host") or self.checkout / "dbt"))
        if resolve_checked(project, "project_host") not in (self.checkout.resolve() / "dbt", self.main.resolve() / "dbt"):
            raise LauncherError("project_host must be dbt/ of this checkout or the main checkout", "args")
        env = {
            **self.allocation_env(inp, ("JOB_THREADS",)),
            "DBT_PROFILES_DIR": "/workspace/dbt",
            "DBT_TARGET_PATH": "/scratch/target",
            "DBT_LOG_PATH": "/scratch/logs",
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "JOB_MEMORY_LIMIT": inp.values["JOB_MEMORY_LIMIT"],
            "DUCKDB_MEMORY_LIMIT": inp.values["DUCKDB_MEMORY_LIMIT"],
        }
        record = self.checkout / dbt_package_archive.RECORD
        try:
            # The hash is read first and the verification binds to it, so the job is admitted with the hash of the bytes that were verified.
            record_sha = hashlib.sha256(record.read_bytes()).hexdigest()
            archive = dbt_package_archive.verify(self.checkout, lock=project / "package-lock.yml", record_sha256=record_sha)
        except (dbt_package_archive.ArchiveError, OSError, ValueError, KeyError) as error:
            raise LauncherError(f"dbt package archive refused: {error}", "args") from error
        module = self.checkout / "scripts/lakehouse/dbt_package_archive.py"
        mounts = [
            Mount(str(project.resolve()), "/workspace/dbt", True),
            Mount(str(packages.resolve()), "/workspace/packages", False),
            Mount(str(archive.parent.resolve()), "/workspace/archive", True),
            Mount(str(record.resolve()), "/workspace/archive_record.json", True),
            Mount(str(module.resolve()), "/opt/hai/dbt_package_archive.py", True),
        ]
        command = [
            "install",
            "--lock",
            "/workspace/dbt/package-lock.yml",
            "--record",
            "/workspace/archive_record.json",
            "--archive",
            f"/workspace/archive/{archive.name}",
            "--destination",
            "/workspace/packages",
            "--record-sha256",
            record_sha,
        ]
        return self.assemble(
            inp,
            name,
            self.image("analytics-dbt"),
            ["python", "/opt/hai/dbt_package_archive.py"],
            command,
            env,
            mounts,
            int(inp.values["JOB_MEMORY_LIMIT"]),
            "none",
            workdir="/workspace/dbt",
            tmpfs=("/scratch",),
        )

    def render_quality_validate(
        self,
        inp: RenderInput,
        template: Template,
        name: str,
        registry_id: str,
        module: str = "scripts.lakehouse.silver_quality",
        more_mounts: Sequence[Mount] = (),
    ) -> Rendered:
        """The Great Expectations validation of a build file, no network. With ``sealed_run`` the build file is the run's sealed copy and the container's
        entrypoint checks it before the validation runs (D1 section 10 item 6)."""
        self.only_params(inp, ("database", "suites_host", "suites_hosts", "baselines_host", "out_host", "build_root", build_provenance.SEALED_RUN_PARAM))
        sealed = build_provenance.SEALED_RUN_PARAM in inp.params
        if sealed and ("database" in inp.params or "build_root" in inp.params):
            raise LauncherError("a validation of a sealed run takes no database and no build_root: it reads the sealed copy", "args")
        values = self.compose_values(self.service_names("quality"), inp.values)
        mounts = self.compose.mounts("quality", values)
        build_mount = next(mount for mount in mounts if mount.target == "/workspace/build")
        if sealed:
            mounts = [mount for mount in mounts if mount is not build_mount]
        elif inp.params.get("build_root"):
            # A fixture case's own folder instead of the shared build folder, still read-only (the gold fixture stage).
            replacement = Mount(str(self.allowed(str(inp.params["build_root"]), "build_root").resolve()), "/workspace/build", True)
            mounts = [replacement if mount is build_mount else mount for mount in mounts]
            build_mount = replacement
        if sealed:
            database_argument = self.SEALED_DATABASE
        else:
            database = Path(str(inp.params["database"]))
            if not inside(database, [Path(build_mount.source)]):
                raise LauncherError(f"database {database} is outside the build folder", "args")
            database_argument = f"/workspace/build/{database.resolve().relative_to(Path(build_mount.source).resolve())}"
        contracts = self.checkout / "data_contracts" / "great_expectations"
        # One suite folder (``suites_host``) or several (``suites_hosts``: the silver atomic suites and the gold suites in one validation).
        if ("suites_host" in inp.params) == ("suites_hosts" in inp.params):
            raise LauncherError("quality_validate takes exactly one of suites_host and suites_hosts", "args")
        hosts = inp.params.get("suites_hosts", [inp.params.get("suites_host")])
        if not isinstance(hosts, list) or not 1 <= len(hosts) <= 4 or not all(isinstance(host, str) for host in hosts):
            raise LauncherError("suites_hosts must be a list of one to four folders", "args")
        targets = ["/workspace/suites"] if "suites_host" in inp.params else [f"/workspace/suites/s{index}" for index in range(len(hosts))]
        suites = [Mount(str(self.allowed(host, "suites_host", (contracts,)).resolve()), target, True) for host, target in zip(hosts, targets, strict=True)]
        extra = [
            *suites,
            Mount(str(self.allowed(str(inp.params["baselines_host"]), "baselines_host", (contracts,)).resolve()), "/workspace/baselines.json", True),
            Mount(str(self.allowed(str(inp.params["out_host"]), "out_host").resolve()), "/workspace/out", False),
            *more_mounts,
        ]
        command = [
            "--database",
            database_argument,
            "--suites",
            *targets,
            "--baselines",
            "/workspace/baselines.json",
            "--output",
            "/workspace/out/result.json",
        ]
        env = self.allocation_env(inp, ("DUCKDB_MEMORY_LIMIT", "JOB_THREADS"))
        entrypoint = ["python", "-m", module]
        if sealed:
            entrypoint, command, sealed_mounts = self.sealed_reader(inp, entrypoint, command)
            extra += sealed_mounts
        return self.assemble(
            inp, name, self.image("quality"), entrypoint, command, env, [*mounts, *extra], int(inp.values["JOB_MEMORY_LIMIT"]), "none", workdir="/workspace"
        )

    def render_quality_reconcile(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """``quality_validate`` plus the per-fact reconciliation of the gold facts with their silver models, with the gold contract mounted read-only."""
        contracts = self.checkout / "data_contracts"
        contract = Mount(str(self.allowed(str(contracts / "lakehouse"), "contracts", (contracts,)).resolve()), "/workspace/data_contracts/lakehouse", True)
        return self.render_quality_validate(inp, template, name, registry_id, "scripts.lakehouse.gold_reconcile", (contract,))

    def aws_environment(self, values: Mapping[str, str]) -> dict[str, str]:
        """The AWS profile variables of the analytics and Spark services."""
        return {
            "AWS_PROFILE": values["AWS_PROFILE"],
            "AWS_REGION": values["AWS_REGION"],
            "AWS_CONFIG_FILE": "/aws/config",
            "AWS_SHARED_CREDENTIALS_FILE": "/aws/credentials",
        }

    def render_query_reader(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """The interactive DuckDB shell on the bronze tables, read-only catalog principal, session-bound."""
        self.only_params(inp, ("tty",))
        values = self.compose_values(self.service_names("analytics"), inp.values)
        env = {key: value for key, value in self.compose.environment("analytics", values).items()}
        mounts = self.compose.mounts("analytics", values)
        command = ["-init", "/opt/analytics/bronze.sql", *validate_duckdb_flags([a for a in inp.args if a.startswith("-") and a != "-c"])]
        for flag, value in zip(inp.args, inp.args[1:], strict=False):
            if flag == "-c":
                command += ["-c", value]
        return self.assemble(
            inp,
            name,
            self.image("analytics"),
            ["duckdb"],
            command,
            env,
            mounts,
            int(inp.values["JOB_MEMORY_LIMIT"]),
            self.network_name(registry_id),
            tty=bool(inp.params.get("tty")),
            open_stdin=True,
            finite=False,
        )

    def render_ui_reader(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """The DuckDB UI, published on the loopback relay port only."""
        self.only_params(inp, ())
        values = self.compose_values(self.service_names("analytics-ui"), inp.values)
        env = self.compose.environment("analytics-ui", values)
        mounts = self.compose.mounts("analytics-ui", values)
        for mount in mounts:
            if mount.target.endswith("/extension_data/ui"):
                Path(mount.source).mkdir(parents=True, exist_ok=True)
        return self.assemble(
            inp,
            name,
            self.image("analytics-ui"),
            ["/opt/analytics/ui.sh"],
            [],
            env,
            mounts,
            int(inp.values["JOB_MEMORY_LIMIT"]),
            self.network_name(registry_id),
            ports=self.compose.ports("analytics-ui"),
            finite=False,
        )

    def render_real_dbt(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """``dbt`` on the run's snapshot against the catalog; provenance variables from the frozen interface (D1 section 10)."""
        self.only_params(inp, ())
        if inp.snapshot is None or inp.run_folder is None:
            raise LauncherError("real_dbt needs the run's snapshot and run folder", "spec")
        values = self.compose_values(self.service_names("analytics-dbt"), inp.values)
        env = self.compose.environment("analytics-dbt", values)
        env["DBT_PACKAGES_INSTALL_PATH"] = "/workspace/packages"
        env.update(inp.provenance)
        mounts = []
        for mount in self.compose.mounts("analytics-dbt", values):
            if mount.target == "/workspace/dbt":
                mounts.append(Mount(str(inp.snapshot / "project"), "/workspace/dbt", True))
            elif mount.target == "/workspace/out":
                mounts.append(Mount(str(inp.run_folder), "/workspace/out", False))
            else:
                mounts.append(mount)
        mounts.append(Mount(str(inp.snapshot / "packages"), "/workspace/packages", True))
        command = validate_dbt_args(inp.template, inp.args)
        return self.assemble(
            inp,
            name,
            self.image("analytics-dbt"),
            ["dbt"],
            command,
            env,
            mounts,
            int(inp.values["JOB_MEMORY_LIMIT"]),
            self.network_name(registry_id),
            workdir="/workspace/dbt",
        )

    def render_sealed_consumer(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """A consumer of the sealed build: the sealed copy read-only, no network; its entrypoint hashes the mounted file first (D1 section 10 item 6)."""
        self.only_params(inp, ())
        if list(inp.args) != ["verify"]:
            raise LauncherError("sealed_consumer takes the single argument verify", "args")
        digest = inp.provenance.get(build_provenance.VARIABLE_SEALED_SHA, "")
        if inp.run_folder is None or not build_provenance.SHA256.fullmatch(digest):
            raise LauncherError("sealed_consumer needs a sealed run", "spec")
        results = inp.run_folder / "consumer"
        results.mkdir(parents=True, exist_ok=True)
        values = self.compose_values(self.service_names("quality"), inp.values)
        mounts = [mount for mount in self.compose.mounts("quality", values) if mount.target == "/workspace/scripts"]
        mounts.append(Mount(str(inp.run_folder / build_provenance.SEALED_FOLDER / f"{digest}.duckdb"), "/workspace/sealed/build.duckdb", True))
        mounts.append(Mount(str(results), "/workspace/out", False))
        env = {**self.allocation_env(inp, ("DUCKDB_MEMORY_LIMIT", "JOB_THREADS")), **inp.provenance}
        command = [
            "consume",
            "--sealed",
            "/workspace/sealed/build.duckdb",
            "--expected-sha256",
            digest,
            "--result",
            f"/workspace/out/{inp.launch_id}-a{inp.attempt_n}.json",
            "--expected-marker",
            inp.provenance[build_provenance.VARIABLE_SEALED_EXPECTED],
        ]
        entrypoint = ["python", "-m", "scripts.lakehouse.provenance"]
        return self.assemble(
            inp, name, self.image("quality"), entrypoint, command, env, mounts, int(inp.values["JOB_MEMORY_LIMIT"]), "none", workdir="/workspace"
        )

    def spark_mounts(self, job: str, inp: RenderInput) -> list[Mount]:
        """The narrowed Spark mounts: code and configuration read-only, only the folders the job writes writable (D1 section 7 item 3)."""
        spec = SPARK_JOBS[job]
        mounts = [Mount(str(self.checkout / "scripts"), "/workspace/scripts", True), Mount(str(self.checkout / "config"), "/workspace/config", True)]
        if spec.get("aws"):
            mounts.append(Mount(str(account_home() / ".aws"), "/aws", True))
        if spec.get("deployment"):
            mounts.append(Mount(str(self.checkout / "infra/deployment.auto.tfvars.json"), "/workspace/infra/deployment.auto.tfvars.json", True))
        if spec.get("contract"):
            mounts.append(Mount(str(self.checkout / GOLD_CONTRACT), f"/workspace/{GOLD_CONTRACT}", True))
        for relative in spec["writes"]:
            folder = self.checkout / relative
            folder.mkdir(parents=True, exist_ok=True)
            mounts.append(Mount(str(folder), f"/workspace/{relative}", False))
        if job == "pins" and inp.run_folder is not None:
            run = inp.run_folder.name
            mounts.append(Mount(str(inp.run_folder), f"/workspace/data/analytics/dbt/runs/{run}", False))
        return mounts

    def render_spark(self, inp: RenderInput, name: str, registry_id: str, read_only_principal: bool) -> Rendered:
        """A Spark job container: image, entrypoint and mounts from the table; the principal by template."""
        job = str(inp.params.get("job", ""))
        spec = SPARK_JOBS.get(job)
        if spec is None or spec["template"] != inp.template:
            raise LauncherError(f"{inp.template} has no job {job!r}", "args")
        self.only_params(inp, ("job",))
        args = validate_spark_args(job, inp.args)
        if job == "pins":
            if args[:1] == ["pin"] or args[:1] == ["verify"]:
                if (
                    len(args) != 3
                    or args[1] != "--run"
                    or not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}", args[2])
                    or inp.run_folder is None
                    or args[2] != inp.run_folder.name
                ):
                    raise LauncherError("pins takes pin or verify with --run <run id>", "args")
            else:
                raise LauncherError("pins takes pin or verify with --run <run id>", "args")
        names = [*self.service_names("spark"), *(("POLARIS_READER_CLIENT_ID", "POLARIS_READER_CLIENT_SECRET") if read_only_principal else ())]
        values = self.compose_values(names, inp.values) if spec["aws"] else dict(inp.values)
        env: dict[str, str] = {
            "SPARK_JOB_MEMORY": inp.values["SPARK_JOB_MEMORY"],
            "JOB_THREADS": inp.values["JOB_THREADS"],
        }
        if spec["aws"]:
            env.update(self.aws_environment(values))
            env["POLARIS_URI"] = "http://polaris:8181/api/catalog"
            prefix = "POLARIS_READER_CLIENT" if read_only_principal else "POLARIS_CLIENT"
            if prefix + "_ID" not in values:
                raise LauncherError("the catalog principal credentials are missing; set up the catalog first", "spec")
            env["POLARIS_CLIENT_ID"] = values[prefix + "_ID"]
            env["POLARIS_CLIENT_SECRET"] = values[prefix + "_SECRET"]
        network = self.network_name(registry_id) if spec["aws"] else "none"
        return self.assemble(
            inp,
            name,
            self.image("spark"),
            ["python", "-m", spec["module"]],
            args,
            env,
            self.spark_mounts(job, inp),
            int(inp.values["JOB_MEMORY_LIMIT"]),
            network,
            workdir="/workspace",
            hostname="spark-job" if network == "none" else "",
        )

    def render_spark_read(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """Catalog jobs that only read: the read-only principal."""
        return self.render_spark(inp, name, registry_id, True)

    def render_spark_write(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """Catalog jobs that write: the principal ``main`` uses for the job."""
        return self.render_spark(inp, name, registry_id, False)

    def render_spark_e2e(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """Throwaway-catalog jobs on a private network."""
        return self.render_spark(inp, name, registry_id, False)

    def render_pipeline_validate(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """Run the fixed layer gate with a digest receipt bound to its successful process."""
        layer = inp.template.removeprefix("pipeline_validate_")
        contracts = self.checkout / "data_contracts"
        contract = Mount(str(contracts / "lakehouse"), "/workspace/data_contracts/lakehouse", True)
        rendered = self.render_quality_validate(
            inp,
            template,
            name,
            registry_id,
            "scripts.lakehouse.pipeline_quality",
            (Mount(str(self.checkout / "orchestration"), "/workspace/orchestration", True), *((contract,) if layer == "cross" else ())),
        )
        rendered.body["Cmd"][:0] = ["--kind", layer]
        return rendered

    def render_pipeline(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """Render a fixed layer job from authoritative source, pin, seal and export metadata."""
        try:
            pipeline_render.require_publish_run(inp)
        except pipeline_protocol.PipelineError as error:
            raise LauncherError(f"pipeline specification refused: {type(error).__name__}: {error}", "spec") from None
        if inp.template == "pipeline_publish" and inp.params.get("layer") in LAYER_WRITE_PREFIX:
            problem = publish_contract_problem(str(inp.params["layer"]), inp.run_mode, self.checkout)
            if problem:
                raise LauncherError(problem, "spec")
        try:
            role = pipeline_render.session_role(inp.template, dict(inp.params), inp.run_mode)
            session = self.session_of(role, inp.deadline_utc, name) if role else None
            values = self.compose_values(pipeline_render.needed_values(inp.template, dict(inp.params), inp.run_mode), inp.values)
            spec = pipeline_render.build(inp, self.checkout, values, session)
        except (pipeline_protocol.PipelineError, ValueError, KeyError, OSError) as error:
            raise LauncherError(f"pipeline specification refused: {type(error).__name__}: {error}", "spec") from None
        mounts = [Mount(*parts) for parts in spec.mounts]
        if inp.template == "pipeline_probe":
            if inp.pipeline.get("registry_kind") != "test":
                raise LauncherError("traffic probes require a test registry", "spec")
            if inp.run_folder is None:
                raise LauncherError("the traffic probe has no run folder", "spec")
            selected = str(inp.params["class"])
            if selected != "offline":
                marker = inp.run_folder / "network_sentinels" / f"{selected}.txt"
                mounts.append(Mount(str(marker), "/run/class_sentinel", True))
        labels = {network_policy.LABEL_CLASS: spec.network_class} if spec.network_class != "offline" else {}
        rendered = self.assemble(
            inp,
            name,
            self.image(spec.service),
            ["python", "-m", spec.module],
            spec.arguments,
            spec.environment,
            mounts,
            int(inp.values["JOB_MEMORY_LIMIT"]),
            spec.network,
            workdir="/workspace",
            extra_labels=labels,
            hostname="spark-job" if spec.service == "spark" and spec.network == "none" else "",
            cap_drop=("ALL",),
            security_opt=("no-new-privileges:true",),
        )
        if inp.template == "pipeline_probe" and inp.params["case"] == "catalog":
            if inp.params["class"] != "lakehouse_read":
                raise LauncherError("the synthetic catalog uses the fixed read class", "spec")
            networks = inp.pipeline["network"]["names"]
            self.attach_networks(rendered, {networks[role]: ["polaris"] for role in network_policy.CLASSES})
        return rendered

    @staticmethod
    def attach_networks(rendered: Rendered, memberships: Mapping[str, Sequence[str]]) -> None:
        """Put a container on several networks under aliases: it is created on the first and joined to the others before it starts.

        API v1.43 creates a container on one network only, so every multi-network container (the lineage API, the proxy and the synthetic catalog)
        takes this one path. The first network must be the container's network mode.
        """
        first, *others = memberships
        if rendered.body["HostConfig"]["NetworkMode"] != first:
            raise LauncherError("the first network of a multi-network container must be its network mode", "spec")
        rendered.body["NetworkingConfig"] = {"EndpointsConfig": {first: {"Aliases": list(memberships[first])}}}
        rendered.joins.update({network: list(memberships[network]) for network in others})

    def render_pipeline_proxy(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """Render the credential-free proxy on only this run's two internal networks and its egress network."""
        if inp.run_folder is None or set(inp.params) != {"run_id"} or inp.params["run_id"] != inp.run_folder.name:
            raise LauncherError("the proxy has no owned run", "spec")
        network = inp.pipeline.get("network", {})
        if network.get("phase") not in ("networks_ready", "ready"):
            raise LauncherError("the proxy networks are not ready", "spec")
        names = network["names"]
        name = f"hai-{registry_id[:8]}-svc-egress-{inp.run_folder.name.lower()}"
        mounts = [Mount(str(inp.run_folder / "egress/squid.conf"), "/etc/squid/squid.conf", True)]
        rendered = self.assemble(
            inp,
            name,
            network_policy.PROXY_IMAGE,
            ["/usr/sbin/squid"],
            ["-f", "/etc/squid/squid.conf", "-NYC"],
            {},
            mounts,
            int(inp.values["JOB_MEMORY_LIMIT"]),
            names["egress"],
            finite=False,
            user="13:13",
            tmpfs=("/run/squid",),
            cap_drop=("ALL",),
            security_opt=("no-new-privileges:true",),
            extra_labels={network_policy.LABEL_CLASS: "proxy"},
        )
        self.attach_networks(rendered, {names[role]: [network_policy.PROXY_ALIAS] for role in ("egress", *network_policy.CLASSES)})
        return rendered

    def render_gold_export(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """The consumer of the sealed copy (D1 section 10 item 6): it hashes the mounted file in the container and exports only on the expected digest.

        A run owner's inputs come from the run's ``completion`` record. An allocation (the offline checks) names the file, folder, run and digest.
        """
        self.only_params(inp, ("sealed_host", "publish_host", "run", "sealed_sha256") if inp.run_folder is None else ())
        if inp.run_folder is not None:
            record = inp.completion
            if record is None:
                raise LauncherError("gold_export needs the run's sealed-build record", "spec")
            run_id, sha = inp.run_folder.name, str(record["sha256"])
            sealed_dir = inp.run_folder / "sealed"
            sealed = inp.run_folder / str(record["file"])
            if sealed.is_symlink() or not sealed.is_file() or not inside(sealed, [sealed_dir]):
                raise LauncherError("the run's sealed copy is missing or is not a plain file inside its sealed folder", "spec")
            publish = inp.run_folder / "publish"
            publish.mkdir(parents=True, exist_ok=True)
        else:
            run_id, sha = str(inp.params["run"]), str(inp.params["sealed_sha256"])
            if not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}", run_id) or not SHA256_HEX.fullmatch(sha):
                raise LauncherError("gold_export needs a run ID and a lower-case hexadecimal SHA-256", "args")
            sealed = self.allowed(str(inp.params["sealed_host"]), "sealed_host")
            publish = self.allowed(str(inp.params["publish_host"]), "publish_host")
            if not sealed.is_file() or not publish.is_dir():
                raise LauncherError("sealed_host must be a file and publish_host a folder", "args")
        mounts = [
            Mount(str(self.checkout / "scripts"), "/workspace/scripts", True),
            Mount(str(self.checkout / GOLD_CONTRACT), f"/workspace/{GOLD_CONTRACT}", True),
            Mount(str(sealed.resolve()), "/workspace/sealed/sealed.duckdb", True),
            Mount(str(publish.resolve()), "/workspace/out/publish", False),
        ]
        env = self.allocation_env(inp, ("DUCKDB_MEMORY_LIMIT", "JOB_THREADS"))
        command = ["--run", run_id, "--sealed-sha256", sha]
        return self.assemble(
            inp,
            name,
            self.image("quality"),
            ["python", "-m", "scripts.lakehouse.gold_export"],
            command,
            env,
            mounts,
            int(inp.values["JOB_MEMORY_LIMIT"]),
            "none",
            workdir="/workspace",
        )

    @staticmethod
    def accepted_result_current(inp: RenderInput) -> None:
        """A step runs only against the consumer result the launcher accepted: the run's result file must still hold exactly the accepted bytes."""
        if inp.run_folder is None or not gold_publish_record.SHA256.fullmatch(inp.accepted_result):
            raise LauncherError("the run has no accepted consumer result; call accept_consumer on the finished export first", "spec")
        current = run_file_sha256(inp.run_folder / "publish", gold_publish_record.RESULT_NAME)
        if current != inp.accepted_result:
            raise LauncherError("the consumer result changed since the launcher accepted it", "spec")

    def render_gold_publish(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """A gold publish step as ``gold_writer`` (the reconcile step as the reader): the writer's credential never goes to any other template."""
        job = str(inp.params.get("job", ""))
        spec = GOLD_JOBS.get(job)
        if spec is None:
            raise LauncherError(f"gold_publish has no job {job!r}", "args")
        self.only_params(inp, ("job",))
        if inp.args:
            raise LauncherError(f"gold_publish job {job} takes no arguments: the launcher builds them from the run", "args")
        if inp.run_folder is None or inp.run_mode not in GOLD_WRITE_PREFIX:
            raise LauncherError("gold_publish needs a run in a publish mode", "spec")
        problem = publish_contract_problem("gold", inp.run_mode, self.checkout)
        if problem:
            raise LauncherError(problem, "spec")
        if spec["sealed"] and inp.completion is None:
            raise LauncherError(f"gold_publish job {job} needs the run's sealed-build record", "spec")
        if spec["sealed"]:
            self.accepted_result_current(inp)
        credentials = READER_ENV if spec["principal"] == "reader" else GOLD_WRITER_ENV
        values = self.compose_values(["AWS_PROFILE", "AWS_REGION", *credentials], inp.values)
        if any(name not in values for name in credentials):
            raise LauncherError("the catalog principal credentials are missing; set up the principal first", "spec")
        publish = inp.run_folder / "publish"
        publish.mkdir(parents=True, exist_ok=True)
        mounts = [
            Mount(str(self.checkout / "scripts"), "/workspace/scripts", True),
            Mount(str(self.checkout / "config"), "/workspace/config", True),
            Mount(str(account_home() / ".aws"), "/aws", True),
            Mount(str(self.checkout / GOLD_CONTRACT), f"/workspace/{GOLD_CONTRACT}", True),
            Mount(str(publish.resolve()), "/workspace/out/publish", False),
        ]
        if spec.get("deployment"):
            mounts.append(Mount(str(self.checkout / "infra/deployment.auto.tfvars.json"), "/workspace/infra/deployment.auto.tfvars.json", True))
        env: dict[str, str] = {"SPARK_JOB_MEMORY": inp.values["SPARK_JOB_MEMORY"], "JOB_THREADS": inp.values["JOB_THREADS"]}
        env.update(self.aws_environment(values))
        env["POLARIS_URI"] = "http://polaris:8181/api/catalog"
        env["POLARIS_CLIENT_ID"], env["POLARIS_CLIENT_SECRET"] = values[credentials[0]], values[credentials[1]]
        run_id = inp.run_folder.name
        if spec["step"] is None:
            command = ["--run", run_id, "--mode", inp.run_mode, "--protected", "/workspace/out/publish/protected.json"]
        else:
            command = [spec["step"], "--run", run_id, "--mode", inp.run_mode, "--sealed-sha256", str((inp.completion or {}).get("sha256", ""))]
            command += ["--accepted-result-sha256", inp.accepted_result]
        return self.assemble(
            inp,
            name,
            self.image("spark"),
            ["python", "-m", spec["module"]],
            command,
            env,
            mounts,
            int(inp.values["JOB_MEMORY_LIMIT"]),
            self.network_name(registry_id),
            workdir="/workspace",
        )

    def render_probe(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """The fixed probe container: analytics image, entrypoint ``true``, no mounts, no network (D1 section 2 item 6)."""
        body: dict[str, Any] = {
            "Image": self.image("analytics"),
            "Entrypoint": ["true"],
            "Cmd": [],
            "Env": [],
            "Labels": dict(inp.labels),
            "HostConfig": {"NetworkMode": "none", "Memory": 0, "Mounts": [], "RestartPolicy": {"Name": "no"}, "AutoRemove": False},
        }
        return Rendered(name, body, frozenset(), frozenset(), False)

    def render_service(self, inp: RenderInput, service: str, registry_id: str) -> Rendered:
        """One catalog service container from the Compose model (D1 section 8 item 1)."""
        values = self.compose_values(self.service_names(service), inp.values)
        secrets_in_use: dict[str, str] = {name: values[name] for name in ("POSTGRES_PASSWORD", "POLARIS_CLIENT_SECRET") if name in values}
        raw = self.compose.service(service)
        env = self.compose.environment(service, values)
        command = [interpolate(str(part), values) for part in raw.get("command") or []]
        mounts = self.compose.mounts(service, values)
        for mount in mounts:
            if mount.target == "/var/lib/postgresql/data":
                Path(mount.source).mkdir(mode=0o700, parents=True, exist_ok=True)
        finite = service in FINITE_SERVICES
        name = f"hai-{registry_id[:8]}-svc-{service}"
        # A finite service runs behind the deadline entrypoint, which needs the program to run: the Compose entrypoint, else the image's own.
        entrypoint = list(raw.get("entrypoint") or (inp.image_entrypoint if finite else []))
        return self.assemble(
            inp,
            name,
            self.image(service),
            entrypoint,
            command,
            env,
            mounts,
            self.compose.memory(service, values),
            self.network_name(registry_id),
            ports=self.compose.ports(service),
            healthcheck=self.compose.healthcheck(service),
            aliases=[service],
            finite=finite,
            extra_labels={LABEL_SERVICE: service},
            secrets=secrets_in_use,
        )

    def orchestration_network_name(self, registry_id: str) -> str:
        """The launcher-created network of the orchestration stack."""
        return f"hai-{registry_id[:8]}-orch-net"

    def orchestration_values(self, inp: RenderInput) -> dict[str, str]:
        """The interpolation values of the orchestration Compose model: the launcher's validated settings only, never the environment."""
        values = {name: str(value) for name, value in inp.values.items() if name.startswith("HAI_ORCH_") or name == "JOB_MEMORY_LIMIT"}
        values["HAI_ORCH_UID"] = str(os.getuid())
        return values

    def orchestration_hardening(self, raw: Mapping[str, Any], values: Mapping[str, str]) -> dict[str, Any]:
        """The user, dropped capabilities and security options a service of the orchestration model names."""
        return {
            "user": interpolate(str(raw["user"]), values) if raw.get("user") else "",
            "cap_drop": [str(item) for item in raw.get("cap_drop") or []],
            "security_opt": [str(item) for item in raw.get("security_opt") or []],
        }

    def render_orchestration_service(self, inp: RenderInput, service: str, registry_id: str) -> Rendered:
        """One orchestration service container from the orchestration Compose model (the second service stack, failure mode 933)."""
        values = {**self.orchestration_values(inp), **self.stack_values(service)}
        raw = self.compose.service(service)
        env = self.compose.environment(service, values)
        command = [interpolate(str(part), values) for part in raw.get("command") or []]
        mounts = self.compose.mounts(service, values)
        for mount in mounts:
            if mount.target in ("/var/lib/postgresql/data", "/opt/airflow/logs"):
                Path(mount.source).mkdir(mode=0o700, parents=True, exist_ok=True)
        ports = []
        for entry in raw.get("ports") or []:
            host_ip, host_port, container_port = interpolate(str(entry), values).split(":")
            ports.append((host_ip, int(host_port), int(container_port)))
        finite = service in ORCHESTRATION_FINITE
        entrypoint = list(raw.get("entrypoint") or (inp.image_entrypoint if finite else []))
        return self.assemble(
            inp,
            f"hai-{registry_id[:8]}-svc-orch-{service}",
            self.image(service),
            entrypoint,
            command,
            env,
            mounts,
            self.compose.memory(service, values),
            self.orchestration_network_name(registry_id),
            ports=ports,
            healthcheck=self.compose.healthcheck(service),
            aliases=[service],
            finite=finite,
            extra_labels={LABEL_STACK: ORCHESTRATION_STACK, LABEL_ORCH_SERVICE: service},
            **self.orchestration_hardening(raw, values),
        )

    def render_lineage_service(self, inp: RenderInput, service: str, registry_id: str) -> Rendered:
        """Render only the three lineage services using validated runtime settings."""
        try:
            values = lineage_services.settings_for(service, inp.values)
        except ValueError as error:
            raise LauncherError(str(error), "args") from None
        self.allowed(values["HAI_LINEAGE_STATE"], "lineage state")
        values = {**values, **self.stack_values(service)}
        raw = self.compose.service(service)
        ports = [tuple(interpolate(str(entry), values).split(":")) for entry in raw.get("ports") or []]
        if any(host != "127.0.0.1" for host, _, _ in ports):
            raise LauncherError("lineage ports must bind IPv4 loopback", "spec")
        rendered = self.assemble(
            inp,
            f"hai-{registry_id[:8]}-svc-lineage-{service}",
            self.image(service),
            list(raw.get("entrypoint") or []),
            [interpolate(str(part), values) for part in raw.get("command") or []],
            self.compose.environment(service, values),
            self.compose.mounts(service, values),
            self.compose.memory(service, values),
            f"hai-{registry_id[:8]}-lineage-{'backend' if service == 'postgres' else 'frontend'}",
            ports=[(host, int(host_port), int(container_port)) for host, host_port, container_port in ports],
            healthcheck=self.compose.healthcheck(service),
            aliases=[f"lineage-{service}"],
            finite=False,
            extra_labels={LABEL_STACK: "lineage", LABEL_SERVICE: f"lineage-{service}"},
            **self.orchestration_hardening(raw, values),
        )

        if service == "api":
            rendered.joins[f"hai-{registry_id[:8]}-lineage-backend"] = ["lineage-api"]
        expected = {"postgres": ["backend"], "api": ["frontend", "backend"], "web": ["frontend"]}[service]
        created_on = set(rendered.body["NetworkingConfig"]["EndpointsConfig"])
        if (
            raw.get("networks") != expected
            or len(created_on) != 1
            or created_on | set(rendered.joins) != {f"hai-{registry_id[:8]}-lineage-{role}" for role in expected}
        ):
            raise LauncherError("lineage service network membership differs from its fixed role", "spec")
        try:
            lineage_services.validate_bindings(service, rendered.body)
        except ValueError as error:
            raise LauncherError(str(error), "spec") from None
        return rendered

    def render_orchestration_check(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """The offline check in the Airflow image: no argument, no parameter, no network, read-only repository mounts (failure mode 934)."""
        self.only_params(inp, ())
        if inp.args:
            raise LauncherError("template orchestration_check takes no arguments", "args")
        values = {"HAI_ORCH_REPO": str(self.checkout), "HAI_ORCH_UID": str(os.getuid()), "JOB_MEMORY_LIMIT": inp.values["JOB_MEMORY_LIMIT"]}
        raw = self.compose.service("airflow-check")
        return self.assemble(
            inp,
            name,
            self.image("airflow-check"),
            [str(item) for item in raw["entrypoint"]],
            [interpolate(str(part), values) for part in raw["command"]],
            self.compose.environment("airflow-check", values),
            self.compose.mounts("airflow-check", values),
            self.compose.memory("airflow-check", values),
            "none",
            **self.orchestration_hardening(raw, values),
        )

    def render_orchestration_dump(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """A read-only logical look at the stack's metadata database: ``metadata`` dumps every table, ``xcom`` the XCom rows. The job runs in the thin
        Airflow image on the stack's network, reads the connection string from the read-only metadata folder and writes only to the state folder's dumps
        folder (failure mode 932b)."""
        self.only_params(inp, ("state",))
        if len(inp.args) != 1 or inp.args[0] not in ORCHESTRATION_DUMPS:
            raise LauncherError(f"template orchestration_dump takes one argument: {' or '.join(ORCHESTRATION_DUMPS)}", "args")
        state = self.allowed(str(inp.params["state"]), "state")
        missing = [folder for folder in ORCHESTRATION_STATE_FOLDERS if not (state / folder).is_dir()]
        if not state.is_dir() or missing:
            raise LauncherError(f"state folder {str(state)!r} does not exist or lacks {', '.join(missing)}", "args")
        values = {
            "HAI_ORCH_REPO": str(self.checkout),
            "HAI_ORCH_STATE": str(state.resolve()),
            "HAI_ORCH_UID": str(os.getuid()),
            "JOB_MEMORY_LIMIT": inp.values["JOB_MEMORY_LIMIT"],
            **self.stack_values("airflow-dump"),
        }
        raw = self.compose.service("airflow-dump")
        mounts = self.compose.mounts("airflow-dump", values)
        for mount in mounts:
            if mount.target == "/out":
                Path(mount.source).mkdir(mode=0o700, parents=True, exist_ok=True)
        return self.assemble(
            inp,
            name,
            self.image("airflow-dump"),
            [str(item) for item in raw["entrypoint"]],
            [*(interpolate(str(part), values) for part in raw["command"]), inp.args[0]],
            self.compose.environment("airflow-dump", values),
            mounts,
            self.compose.memory("airflow-dump", values),
            self.orchestration_network_name(registry_id),
            **self.orchestration_hardening(raw, values),
        )

    def render_lineage_dump(self, inp: RenderInput, template: Template, name: str, registry_id: str) -> Rendered:
        """Render a finite read-only Marquez dump on the owned internal backend without published ports."""
        self.only_params(inp, ("state",))
        if inp.args:
            raise LauncherError("lineage_dump takes no arguments", "args")
        state = self.allowed(str(inp.params["state"]), "state")
        if not all((state / part).is_dir() and not (state / part).is_symlink() for part in ("postgres", "config")):
            raise LauncherError("lineage dump state is incomplete", "args")
        (state / "dumps").mkdir(mode=0o700, exist_ok=True)
        values = {
            "HAI_LINEAGE_REPO": str(self.checkout),
            "HAI_LINEAGE_STATE": str(state.resolve()),
            "HAI_ORCH_UID": str(os.getuid()),
            "JOB_MEMORY_LIMIT": inp.values["JOB_MEMORY_LIMIT"],
            **self.stack_values("dump"),
        }
        raw = self.compose.service("dump")
        return self.assemble(
            inp,
            name,
            self.image("dump"),
            list(raw["entrypoint"]),
            list(raw["command"]),
            self.compose.environment("dump", values),
            self.compose.mounts("dump", values),
            self.compose.memory("dump", values),
            f"hai-{registry_id[:8]}-lineage-backend",
            **self.orchestration_hardening(raw, values),
        )


def completion_of(run: Mapping[str, Any]) -> dict[str, Any]:
    """Return a live build's sealed digest and path, refusing invalid provenance or a malformed record."""
    try:
        record = build_provenance.check_completion(run)
    except build_provenance.ProvenanceError as error:
        raise LauncherError(str(error), "refused") from error
    sha, file = str(record.get("sha256", "")), str(record.get("file", ""))
    if not SHA256_HEX.fullmatch(sha) or file != f"{SEALED_FOLDER}/{sha}.duckdb":
        raise LauncherError(f"run {run.get('run_id')} has a malformed sealed-build record", "refused")
    return {"sha256": sha, "file": file, "inode": record.get("inode"), "device": record.get("device")}


def run_file_sha256(folder: Path, name: str, limit: int = 1 << 20) -> str:
    """The SHA-256 of a plain file in a run's folder, opened without following a link; a link, a missing file or a large file is a refusal."""
    try:
        if folder.is_symlink() or not folder.is_dir():
            raise LauncherError(f"{folder.name} of the run is not a plain folder", "spec")
        descriptor = os.open(folder / name, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as error:
        raise LauncherError(f"{name} of the run cannot be opened without following links: {error.strerror or type(error).__name__}", "spec") from error
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise LauncherError(f"{name} of the run is not a regular file", "spec")
        data = os.read(descriptor, limit + 1)
    finally:
        os.close(descriptor)
    if len(data) > limit:
        raise LauncherError(f"{name} of the run is larger than {limit} bytes", "spec")
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------------------------------------------------
# Provenance interfaces, frozen for the provenance package (D1 sections 10 and 14)
# ---------------------------------------------------------------------------------------------------------------------


def provenance_vars(run: Mapping[str, Any]) -> dict[str, str]:
    """The provenance variables a ``real_dbt`` launch of ``run`` carries (D1 section 10); a missing or unrecorded value refuses."""
    from scripts.lakehouse import provenance

    try:
        return provenance.variables(run)
    except provenance.ProvenanceError as error:
        raise LauncherError(f"real_dbt is refused for run {run.get('run_id')}: {error}", "provenance") from error


def seal(run: Mapping[str, Any]) -> dict[str, Any]:
    """Make the sealed copy of a gold-eligible build of ``run`` and return its completion record (D1 section 10 item 5)."""
    from scripts.lakehouse import provenance

    try:
        return provenance.seal_build(run)
    except provenance.ProvenanceError as error:
        raise LauncherError(f"run {run.get('run_id')} cannot be sealed: {error}", "provenance") from error


def consumer_vars(run: Mapping[str, Any]) -> dict[str, str]:
    """The environment of a consumer of the run's sealed copy; a run that is not sealed refuses."""
    try:
        return build_provenance.consumer_variables(run)
    except build_provenance.ProvenanceError as error:
        raise LauncherError(f"run {run.get('run_id')} has no sealed copy: {error}", "provenance") from error


# ---------------------------------------------------------------------------------------------------------------------
# Connections, peers and requests
# ---------------------------------------------------------------------------------------------------------------------

SOL_LOCAL = 0
LOCAL_PEERCRED = 1
LOCAL_PEERPID = 2
PENDING = object()


@dataclass
class Peer:
    """The authenticated process at the other end of a socket: user, process ID and the process's start time."""

    uid: int
    pid: int
    start: str | None


def peer_of(sock: socket.socket) -> Peer:
    """Read the peer's user ID and process ID from the socket (D1 section 2 item 3)."""
    if sys.platform == "darwin":
        credential = sock.getsockopt(SOL_LOCAL, LOCAL_PEERCRED, 76)
        uid = struct.unpack_from("=II", credential)[1]
        pid = struct.unpack("i", sock.getsockopt(SOL_LOCAL, LOCAL_PEERPID, 4))[0]
    else:
        pid, uid, _ = struct.unpack("3i", sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")))
    return Peer(uid, pid, run_lock.process_start(pid))


def parent_pid(pid: int) -> int | None:
    """The parent of ``pid``, or None when it cannot be read."""
    result = run_command("ps", ["-o", "ppid=", "-p", str(pid)], timeout=30)
    text = result.stdout.strip()
    return int(text) if result.returncode == 0 and text.isdigit() else None


def is_descendant(peer: Peer, holder_pid: int, holder_start: str) -> bool:
    """True when the peer is the holder process or one of its descendants; the holder's start time is checked at the end of the chain."""
    current: int | None = peer.pid
    for _ in range(64):
        if current is None or current <= 1:
            return False
        if current == holder_pid:
            return run_lock.process_start(current) == holder_start
        current = parent_pid(current)
    return False


class Conn:
    """One client connection: a socket, an input buffer and the peer."""

    def __init__(self, sock: socket.socket, peer: Peer) -> None:
        self.sock = sock
        self.peer = peer
        self.buffer = b""
        self.closed = False
        self.session: Session | None = None
        self.outbuf = bytearray()
        self.interest = selectors.EVENT_READ
        self.notify: Callable[[Conn], None] | None = None

    def send(self, message: Mapping[str, Any]) -> None:
        """Send one JSON line; a failure closes the connection.

        A connection with a session, or with output still waiting, never blocks the loop: the line joins ``outbuf`` and the loop sends it when
        the socket accepts bytes.
        """
        if self.closed:
            return
        line = json.dumps(message).encode() + b"\n"
        if self.session is None and not self.outbuf:
            try:
                self.sock.settimeout(5)
                self.sock.sendall(line)
            except OSError:
                self.closed = True
            return
        self.outbuf += line
        self.flush()

    def flush(self) -> None:
        """Send as much of ``outbuf`` as the socket accepts without blocking, then tell the loop which events the socket needs."""
        if not self.closed:
            try:
                self.sock.setblocking(False)
                while self.outbuf:
                    del self.outbuf[: self.sock.send(self.outbuf)]
            except BlockingIOError:
                pass
            except OSError:
                self.closed = True
        if self.closed:
            self.outbuf.clear()
        if self.notify is not None:
            self.notify(self)


@dataclass
class Session:
    """An interactive session bound to one attempt's container (D1 section 9)."""

    conn: Conn
    attempt: str
    container_id: str
    docker_socket: socket.socket
    tty: bool
    policy: str
    detached: bool = False
    pending: bytes = b""
    pending_in: bytearray = field(default_factory=bytearray)
    interest: int = selectors.EVENT_READ


@dataclass
class Waiter:
    """A client waiting for a record to reach one of the given values."""

    conn: Conn
    request_id: str
    what: str
    record_id: str
    field: str
    values: tuple[str, ...]
    deadline: float


@dataclass
class Ctx:
    """What a method handler gets: the connection, the authenticated principal and the validated parameters."""

    conn: Conn
    method: str
    request_id: str
    params: dict[str, Any]
    principal: dict[str, Any]
    key: str
    state: dict[str, Any]


@dataclass(frozen=True)
class MethodSpec:
    """How a method is authorized and whether it changes state (D1 section 3 item 1)."""

    kinds: tuple[str, ...]
    mutating: bool = True
    owner_flag: bool = False


METHODS: dict[str, MethodSpec] = {
    "ping": MethodSpec(("any",), mutating=False),
    "status": MethodSpec(("any", "run", "alloc"), mutating=False),
    "plan": MethodSpec(("any",), mutating=False),
    "image": MethodSpec(("any",), mutating=False),
    "acquire": MethodSpec(("any",)),
    "allocate": MethodSpec(("any",)),
    "release_allocation": MethodSpec(("alloc",)),
    "submit": MethodSpec(("run", "alloc")),
    "cancel": MethodSpec(("run", "alloc")),
    "logs": MethodSpec(("run", "alloc"), mutating=False),
    "session": MethodSpec(("run", "alloc"), mutating=False),
    "finalize": MethodSpec(("run",)),
    "mark": MethodSpec(("run",)),
    "enter": MethodSpec(("run",), mutating=False),
    "pin": MethodSpec(("run",)),
    "register": MethodSpec(("run",)),
    "release": MethodSpec(("run",)),
    "recover": MethodSpec(("run",), owner_flag=True),
    "snapshot": MethodSpec(("run",)),
    "accept_consumer": MethodSpec(("run",)),
    "register_reader": MethodSpec(("alloc",)),
    "deregister_reader": MethodSpec(("alloc",)),
    "reserve": MethodSpec(("any",)),
    "register_set": MethodSpec(("any",)),
    "deregister_set": MethodSpec(("any",), owner_flag=False),
    "catalog_up": MethodSpec(("any",)),
    "pipeline_catalog_up": MethodSpec(("run",)),
    "pipeline_catalog_release": MethodSpec(("run",)),
    "pipeline_network_up": MethodSpec(("run",)),
    "pipeline_network_down": MethodSpec(("run",)),
    "pipeline_reserve_silver": MethodSpec(("run",)),
    "pipeline_accept_seal": MethodSpec(("run",)),
    "pipeline_accept_export": MethodSpec(("run",)),
    "pipeline_accept_pin": MethodSpec(("run",)),
    "pipeline_accept_input": MethodSpec(("run",)),
    "pipeline_fixture_pin": MethodSpec(("run",)),
    "pipeline_fixture_verify": MethodSpec(("run",)),
    "pipeline_test_catalog": MethodSpec(("run",)),
    "catalog_down": MethodSpec(("any",)),
    "lineage_up": MethodSpec(("any",)),
    "lineage_down": MethodSpec(("any",)),
    "orchestration_up": MethodSpec(("any",)),
    "orchestration_down": MethodSpec(("any",)),
    "pull": MethodSpec(("any",)),
    "docker_restart": MethodSpec(("any",), owner_flag=True),
    "shutdown": MethodSpec(("any",), owner_flag=True),
    "catalog_authority": MethodSpec(("any",), owner_flag=True),
}


def new_id(prefix: str) -> str:
    """A short random ID with a prefix."""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def finite_attempt(attempt: Mapping[str, Any]) -> bool:
    """One finite predicate for ordinary jobs and one-shot catalog services, shared by every deadline gate."""
    if attempt["template"] == "orchestration_service":
        return attempt["params"].get("service") in ORCHESTRATION_FINITE
    return TEMPLATES[attempt["template"]].finite or (attempt["template"] == "catalog_service" and attempt["params"].get("service") in FINITE_SERVICES)


def is_open(attempt: Mapping[str, Any]) -> bool:
    """True while an attempt has not ended."""
    return attempt["phase"] != "ended"


# Docker templates that write the Mac production catalog in a real run; a pipeline job counts when its class is lakehouse_write.
MAC_WRITE_TEMPLATES = frozenset({"spark_write", "real_dbt", "gold_publish"})


def mac_catalog_write(template_name: str, params: Mapping[str, Any], args: Sequence[str], run_mode: str) -> bool:
    """True for a job that writes the Mac production catalog: it may start only under Mac catalog authority (failure F1).

    Classified by job and validated arguments: a checksum job without ``--fixture`` only reads and ``real_dbt ls`` or ``parse`` never opens the
    build (``provenance.INERT``). The gold candidate reconcile reads the catalog as the reader principal.
    Every other ``spark_write`` job, including ``smoke``, which creates and purges a scratch table, counts as a
    write, and so do arguments outside a job's grammar.
    """
    if run_mode != "real":
        return False
    if template_name.startswith("pipeline_"):
        try:
            return pipeline_render.job_class(template_name, dict(params), run_mode) == "lakehouse_write"
        except pipeline_protocol.PipelineError:
            return False
    if template_name == "real_dbt":
        return not args or build_provenance.command_of(args) not in build_provenance.INERT
    if template_name == "spark_write" and params.get("job") == "checksums":
        try:
            return "--fixture" in validate_spark_args("checksums", [str(token) for token in args])
        except LauncherError:
            return True
    if template_name == "gold_publish" and params.get("job") == "reconcile_candidates":
        return False
    return template_name in MAC_WRITE_TEMPLATES


ARCHIVE_FOLDER = "history"
ARCHIVE_STALLED_WRITES = 3


def write_all(fd: int, data: bytes) -> None:
    """Write every byte of ``data``: a short write continues with the rest, an interrupted write repeats and a call that makes no progress fails."""
    view, stalled = memoryview(data), 0
    while view:
        try:
            count = os.write(fd, view)
        except InterruptedError:
            continue
        if count <= 0:
            stalled += 1
            if stalled >= ARCHIVE_STALLED_WRITES:
                raise OSError(errno.EIO, "a write made no progress")
            continue
        stalled = 0
        view = view[count:]


def read_exact(fd: int, length: int, offset: int) -> bytes:
    """Read ``length`` bytes at ``offset``; fewer bytes than the file holds there is an error."""
    found = b""
    while len(found) < length:
        chunk = os.pread(fd, min(length - len(found), 1 << 20), offset + len(found))
        if not chunk:
            raise OSError(errno.EIO, "the file is shorter than its size")
        found += chunk
    return found


def archive_recover_tail(fd: int) -> int:
    """Cut an incomplete last line off an archive file and return the size that is left: every append ends in a newline, so a tail without one
    is the remains of an append that did not finish, and its records are still in the registry."""
    size = os.fstat(fd).st_size
    end = size
    while end > 0:
        begin = max(0, end - (1 << 16))
        index = read_exact(fd, end - begin, begin).rfind(b"\n")
        if index >= 0:
            end = begin + index + 1
            break
        end = begin
    if end != size:
        os.ftruncate(fd, end)
        os.fsync(fd)
    return end


def archive_verify(fd: int, start: int, data: bytes) -> None:
    """The file holds exactly ``data`` after its first ``start`` bytes."""
    if os.fstat(fd).st_size != start + len(data) or read_exact(fd, len(data), start) != data:
        raise OSError(errno.EIO, "the archive file differs from the bytes written")


class Launcher:
    """The launcher process: one registry, one loop, every Docker side effect recorded as an operation."""

    def __init__(self, config: Config) -> None:
        self.cfg = config
        self.ecs_jobs = ecs_target.load_jobs(config.root)
        self.ecs_runtime = config.ecs_runtime
        self.registry = config.registry
        self.docker = DockerApi(config.docker_socket, config.timings.docker_call_timeout)
        self.timings = config.timings
        self.selector = selectors.DefaultSelector()
        self.listener: socket.socket | None = None
        self.lock_handle: Any = None
        self.conns: dict[int, Conn] = {}
        self.messages: queue.Queue[tuple[str, str, Any]] = queue.Queue()
        self.wake_read, self.wake_write = os.pipe()
        os.set_blocking(self.wake_read, False)
        self.pool = ThreadPoolExecutor(max_workers=16, thread_name_prefix="launcher-worker")
        self.running = True
        self.current: dict[str, Any] | None = None
        self.current_writes = False
        self.registry_id = ""
        self.key = b""
        self.status = "starting"
        self.degraded_reason = ""
        self.docker_ok = False
        self.epoch = 0
        self.stream: EventStream | None = None
        self.stream_acked = False
        self.probe: dict[str, Any] | None = None
        self.next_retry = 0.0
        self.backoff = self.timings.backoff_start
        self.inflight: set[str] = set()
        self.inventory: dict[str, dict[str, Any]] = {}
        self.dirty: set[str] = set()
        self.waiters: list[Waiter] = []
        self.changed = True
        self.scan_seq = 0
        self.scan_done_seq = 0
        self.scan_wall: dict[int, float] = {}
        self.bound: dict[str, str] = {}
        self.last_maintenance = 0.0
        self.last_compact = 0.0
        self.maintained: tuple[int, str] | None = None
        self.compacted_at: tuple[int, str] | None = None
        self.create_deadlines: list[tuple[str, float]] = []
        self.orphan_retries: list[tuple[str, str]] = []
        self.last_failure: dict[str, float] = {}
        self.spawn_epoch: dict[str, int] = {}
        self.waiter_principals: dict[tuple[int, str], dict[str, Any]] = {}
        self.read_waiters: dict[str, tuple[Conn, str]] = {}
        self.release_requests: dict[str, tuple[str, str, str]] = {}
        self.pull_requests: dict[str, str] = {}
        self.attach_info: dict[str, tuple[str, str, bool, str]] = {}
        self.names_seen: dict[str, str] = {}
        self.foreign: list[str] = []
        self.compose_models: dict[str, ComposeModel] = {}
        self.pending_conns: dict[str, list[tuple[Conn, str]]] = {}
        self.sessions: dict[int, Session] = {}
        self.copy_threads: dict[str, threading.Thread] = {}
        self.copy_cancel: set[str] = set()
        self.last_stale_check = 0.0
        self.main_checkout = config.root
        self.image_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self.secret_results: dict[str, tuple[float, dict[str, Fetched]]] = {}
        self.networks_seen: list[str] = []
        self.message_handlers: dict[str, Callable[[str, str, Any], None]] = {
            "scan": self.on_scan,
            "prepare": self.on_prepare,
            "capacity": self.on_capacity,
            "create": self.on_create,
            "start": self.on_start,
            "stop": self.on_stop,
            "collect": self.on_collect,
            "remove": self.on_remove,
            "confirm": self.on_confirm,
            "inspect": self.on_inspect,
            "orphan": self.on_orphan,
            "plan": self.on_plan,
            "planread": self.on_planread,
            "imageread": self.on_imageread,
            "imagedef": self.on_imagedef,
            "secretfetch": self.on_secretfetch,
            "logsread": self.on_logsread,
            "runlist": self.on_runlist,
            "alloclist": self.on_alloclist,
            "alloclist_dead": self.on_alloclist_dead,
            "cataloglimits": self.on_cataloglimits,
            "pipeline_net": self.on_pipeline_net,
            "netcreate": self.on_netcreate,
            "health": self.on_health,
            "netremove": self.on_netremove,
            "catalogconfirm": self.on_catalogconfirm,
            "lineagelimits": self.on_lineagelimits,
            "lineagehealth": self.on_lineagehealth,
            "lineagenetcreate": self.on_lineagenetcreate,
            "lineagenetremove": self.on_lineagenetremove,
            "lineageconfirm": self.on_lineageconfirm,
            "orchlimits": self.on_orchlimits,
            "orchhealth": self.on_orchhealth,
            "orchnetcreate": self.on_orchnetcreate,
            "orchnetremove": self.on_orchnetremove,
            "orchconfirm": self.on_orchconfirm,
            "pull": self.on_pull,
            "restartcheck": self.on_restartcheck,
            "restartcmd": self.on_restartcmd,
            "restartping": self.on_restartping,
            "copy": self.on_copy,
            "rmcopy": self.on_rmcopy,
            "seal": self.on_seal,
            "attach": self.on_attach,
        }
        self.handlers: dict[str, Callable[[Ctx], Any]] = {
            "ping": self.m_ping,
            "status": self.m_status,
            "plan": self.m_plan,
            "image": self.m_image,
            "acquire": self.m_acquire,
            "allocate": self.m_allocate,
            "release_allocation": self.m_release_allocation,
            "submit": self.m_submit,
            "cancel": self.m_cancel,
            "logs": self.m_logs,
            "session": self.m_session,
            "finalize": self.m_finalize,
            "mark": self.m_mark,
            "enter": self.m_enter,
            "pin": self.m_pin,
            "register": self.m_register,
            "release": self.m_release,
            "recover": self.m_recover,
            "snapshot": self.m_snapshot,
            "accept_consumer": self.m_accept_consumer,
            "register_reader": self.m_register_reader,
            "deregister_reader": self.m_deregister_reader,
            "reserve": self.m_reserve,
            "register_set": self.m_register_set,
            "deregister_set": self.m_deregister_set,
            "catalog_up": self.m_catalog_up,
            "pipeline_catalog_up": self.m_pipeline_catalog_up,
            "pipeline_catalog_release": self.m_pipeline_catalog_release,
            "pipeline_network_up": self.m_pipeline_network_up,
            "pipeline_network_down": self.m_pipeline_network_down,
            "pipeline_reserve_silver": self.m_pipeline_reserve_silver,
            "pipeline_accept_seal": self.m_pipeline_accept_seal,
            "pipeline_accept_export": self.m_pipeline_accept_export,
            "pipeline_accept_pin": self.m_pipeline_accept_pin,
            "pipeline_accept_input": self.m_pipeline_accept_input,
            "pipeline_fixture_pin": self.m_pipeline_fixture_pin,
            "pipeline_fixture_verify": self.m_pipeline_fixture_verify,
            "pipeline_test_catalog": self.m_pipeline_test_catalog,
            "catalog_down": self.m_catalog_down,
            "lineage_up": self.m_lineage_up,
            "lineage_down": self.m_lineage_down,
            "orchestration_up": self.m_orchestration_up,
            "orchestration_down": self.m_orchestration_down,
            "pull": self.m_pull,
            "docker_restart": self.m_docker_restart,
            "shutdown": self.m_shutdown,
            "catalog_authority": self.m_catalog_authority,
        }

    # -- registry --------------------------------------------------------------------------------------------------

    @contextlib.contextmanager
    def tx(self, write: bool = True) -> Iterator[dict[str, Any]]:
        """A registry transaction. A transaction opened inside another one joins it (the loop is single-threaded); a write marks the state changed."""
        if self.current is not None:
            if write and not self.current_writes:
                raise RuntimeError("a write inside a read-only transaction")
            yield self.current
            return
        if write:
            self.changed = True
        with run_lock.transaction(self.registry, write=write, create_schema=run_lock.LAUNCHER_SCHEMA) as state:
            self.current, self.current_writes = state, write
            try:
                yield state
            finally:
                self.current = None

    def spawn(self, key: str, tag: str, function: Callable[..., Any], *arguments: Any) -> None:
        """Run a Docker call or other blocking work in a worker; its result returns to the loop as a message."""
        if key in self.inflight:
            return
        self.inflight.add(key)
        self.spawn_epoch[key] = self.epoch

        def work() -> None:
            try:
                outcome: tuple[str, Any] = ("ok", function(*arguments))
            except DockerError as error:
                outcome = ("docker", error)
            except Exception as error:
                outcome = ("error", error)
            self.messages.put((tag, key, outcome))
            with contextlib.suppress(OSError):
                os.write(self.wake_write, b"x")

        self.pool.submit(work)

    # -- start-up --------------------------------------------------------------------------------------------------

    def acquire_lock(self) -> bool:
        """Take the registry's launcher lock; False when another launcher holds it."""
        self.registry.path.mkdir(parents=True, exist_ok=True)
        self.registry.path.chmod(0o700)
        handle = self.cfg.lock_path.open("a")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            return False
        self.lock_handle = handle
        return True

    def open_socket(self) -> None:
        """Bind the Unix socket in the registry folder with owner-only permissions."""
        path = self.cfg.socket_path
        path.unlink(missing_ok=True)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(path))
        os.chmod(path, 0o600)
        listener.listen(64)
        listener.setblocking(False)
        self.listener = listener
        self.selector.register(listener, selectors.EVENT_READ, "listener")
        self.selector.register(self.wake_read, selectors.EVENT_READ, "wake")

    def migration_gate(self) -> None:
        """Migrate a registry without the launcher schema, or refuse to serve (D1 section 2 item 7)."""
        state_exists = self.registry.state_file.exists()
        schema = 0
        if state_exists:
            with run_lock.transaction(self.registry, write=False) as state:
                schema = int(state["schema_version"])
        if schema == run_lock.LAUNCHER_SCHEMA:
            with self.tx(write=False) as state:
                self.registry_id = str(state["registry_id"])
            return
        legacy = self.docker.containers(label=LABEL_RUN) if self.docker_reachable() else None
        with self.tx() as state:
            self.registry_id = str(state["registry_id"])
            if state["schema_version"] == run_lock.LAUNCHER_SCHEMA:
                return
            blockers = self.migration_blockers(state, legacy)
            if blockers:
                raise LauncherError("migration refused: " + "; ".join(blockers), "migration")
            for section in run_lock.LAUNCHER_SECTIONS:
                state.setdefault(section, {})
            state["launcher"] = run_lock.empty_state(self.registry_id, run_lock.LAUNCHER_SCHEMA)["launcher"]
            for run in state["runs"].values():
                run.setdefault("generation", 1)
                run.setdefault("snapshot", None)
                run.setdefault("token_file", None)
            state["schema_version"] = run_lock.LAUNCHER_SCHEMA

    def load_bound(self) -> None:
        """The container-to-attempt index from the registry."""
        with self.tx(write=False) as state:
            self.bound = {a["container_id"]: a["id"] for a in state["attempts"].values() if a.get("container_id") and is_open(a)}

    def repair_pending(self) -> None:
        """A request that waited for a Docker listing when the launcher stopped becomes ``interrupted`` and keeps its payload hash.

        Its allocation goes back to ``held``; a retry of the same payload runs it again and a changed payload is refused.

        ``docker_restart`` is not dropped: its recorded phase is resumed and its final result is stored when it ends (D1 section 8 item 4).
        """
        with self.tx() as state:
            for record in state["requests"].values():
                if record["state"] == "pending" and record.get("method") in ("release", "release_allocation", "pull"):
                    # The record and its payload hash stay: the same request ID with the same payload runs again, a changed payload is refused.
                    record["state"] = "interrupted"
            for allocation in state["allocations"].values():
                if allocation["state"] == "releasing":
                    allocation["state"] = "held"

    def docker_reachable(self) -> bool:
        """True when the daemon answers a ping."""
        try:
            return self.docker.ping()
        except DockerError:
            return False

    @staticmethod
    def migration_blockers(state: Mapping[str, Any], legacy: list[dict[str, Any]] | None) -> list[str]:
        """Each reason a registry cannot be migrated: a run not released, a reservation, a reader or registration, a legacy container."""
        blockers = [f"run {run_id} is {run['state']}" for run_id, run in sorted(state["runs"].items()) if run["state"] != "released"]
        blockers += [f"reservation {reservation_id} exists" for reservation_id in sorted(state["reservations"])]
        blockers += [f"reader {reader} is registered" for reader in sorted(state["readers"])]
        blockers += [f"run {run_id} has registration {run['registrations'][0]}" for run_id, run in sorted(state["runs"].items()) if run["registrations"]]
        if legacy is None:
            blockers.append("Docker does not answer, so the legacy container listing cannot be read")
        else:
            blockers += [f"container {', '.join(c.get('Names') or [c['Id'][:12]])} still carries {LABEL_RUN}" for c in legacy]
        return blockers

    def write_diagnostic_owner(self) -> None:
        """The built-in owner of probe attempts, written with the launcher schema (D1 section 2 item 6)."""
        with self.tx() as state:
            state["launcher"].setdefault("diagnostic", {"owner": DIAGNOSTIC_OWNER, "templates": ["probe"]})
            state["launcher"]["status"] = "starting"

    def serve(self) -> int:
        """Run the launcher until it is told to stop. Returns the process exit code."""
        if not self.acquire_lock():
            sys.stdout.write("launcher already running\n")
            return 0
        sys.stdout.write(f"launcher process {os.getpid()} holds the registry lock\n")
        sys.stdout.flush()
        try:
            private_chain(self.cfg.token_root, anchor=self.cfg.token_anchor)
            self.key = launcher_key(self.cfg.token_root, self.cfg.token_anchor)
            self.migration_gate()
            self.write_diagnostic_owner()
            self.load_bound()
            self.repair_pending()
            self.sweep_copies()
            self.open_socket()
            self.main_checkout = run_lock.main_checkout(self.cfg.root) if self.registry.kind == "real" else self.cfg.root
            self.connect_docker()
            self.loop()
        except LauncherError as error:
            sys.stderr.write(f"launcher: {error}\n")
            return REFUSED_EXIT
        finally:
            self.shutdown_resources()
        return 0

    def drain_seals(self) -> None:
        """Wait until every seal worker has returned and publish each result through ``on_seal`` (D1 section 10 item 5, review delta 2 finding 9).

        A seal worker copies into the run's folder and cannot be cancelled (the frozen ``seal`` interface). A launcher that gave up the registry lock first
        would let a replacement find no worker in its own ``inflight`` set, discard the seal record, finish a recovery and accept a release while the old
        worker still writes. The lock and the seal's open-work protection therefore stay until the worker has stopped, and ``seal_stale`` still decides what
        publishes. Nothing else is applied. The wait has no limit: the process cannot end before its workers do (the interpreter joins them at exit), and a
        hard kill ends a worker with its process. A reply to ``shutdown`` is not proof that the worker ended; the process exit is.
        """
        while any(key.startswith("seal:") for key in self.inflight):
            try:
                tag, key, payload = self.messages.get(timeout=0.2)
            except queue.Empty:
                continue
            self.inflight.discard(key)
            if tag == "seal":
                with contextlib.suppress(run_lock.RegistryError, OSError):
                    self.on_seal(key, *payload)

    def shutdown_resources(self) -> None:
        """Close every socket and thread the launcher opened; the registry lock goes last, after every seal worker has returned."""
        self.running = False
        if self.stream is not None:
            self.stream.close()
        for conn in list(self.conns.values()):
            with contextlib.suppress(OSError):
                conn.sock.close()
        if self.listener is not None:
            with contextlib.suppress(OSError):
                self.listener.close()
        self.cfg.socket_path.unlink(missing_ok=True)
        self.drain_seals()
        self.pool.shutdown(wait=False, cancel_futures=True)
        try:
            with self.tx() as state:
                if "launcher" in state:
                    state["launcher"]["status"] = "stopped"
        except (run_lock.RegistryError, OSError):
            pass
        if self.lock_handle is not None:
            self.lock_handle.close()

    # -- the loop --------------------------------------------------------------------------------------------------

    def loop(self) -> None:
        """The single-threaded loop: sockets, worker messages and timers, until stopped."""
        while self.running:
            for key, mask in self.selector.select(timeout=self.timings.tick):
                if key.data == "listener":
                    self.accept()
                elif key.data == "wake":
                    with contextlib.suppress(BlockingIOError):
                        os.read(self.wake_read, 4096)
                elif key.data == "session_docker":
                    self.session_docker_event(key.fileobj, mask)
                elif isinstance(key.data, Conn):
                    if mask & selectors.EVENT_WRITE:
                        key.data.flush()
                        if key.data.closed:
                            self.close_conn(key.data)
                            continue
                    if mask & selectors.EVENT_READ:
                        self.read_conn(key.data)
            self.guarded(self.drain_messages)
            self.guarded(self.timers)
            self.guarded(self.process_dirty)
            if self.changed:
                self.changed = False
                self.evaluate_waiters()

    def guarded(self, step: Callable[[], None]) -> None:
        """Run one loop step; an unexpected failure is reported and the loop goes on, so a bug in one step cannot stop Docker supervision."""
        try:
            step()
        except Exception:
            self.report_failure(getattr(step, "__name__", "step"))

    def report_failure(self, where: str) -> None:
        """Write the traceback of an unexpected failure to the launcher's log, at most once a second for the same place."""
        now = time.monotonic()
        if now - self.last_failure.get(where, 0.0) > 1.0:
            self.last_failure[where] = now
            sys.stderr.write(f"launcher: unexpected failure in {where}\n{traceback.format_exc()}\n")
            sys.stderr.flush()

    def accept(self) -> None:
        """Accept a connection from the same user."""
        if self.listener is None:
            return
        try:
            sock, _ = self.listener.accept()
        except BlockingIOError:
            return
        try:
            peer = peer_of(sock)
        except OSError:
            sock.close()
            return
        expected = self.cfg.expected_uid if self.cfg.expected_uid is not None else os.getuid()
        if peer.uid != expected:
            sys.stdout.write(f"rejected a connection from user {peer.uid} (process {peer.pid}); the launcher serves user {expected} only\n")
            sys.stdout.flush()
            sock.close()
            return
        sock.setblocking(False)
        conn = Conn(sock, peer)
        conn.notify = self.update_conn
        self.conns[sock.fileno()] = conn
        self.selector.register(sock, selectors.EVENT_READ, conn)

    def close_conn(self, conn: Conn) -> None:
        """Close a client connection; an interactive session on it becomes detached (D1 section 9 item 3)."""
        if conn.closed and conn.sock.fileno() == -1:
            return
        conn.closed = True
        conn.outbuf.clear()
        with contextlib.suppress(KeyError, ValueError, OSError):
            self.selector.unregister(conn.sock)
        self.conns.pop(conn.sock.fileno(), None)
        with contextlib.suppress(OSError):
            conn.sock.close()
        if conn.session is not None:
            conn.session.detached = True
            conn.session.pending_in.clear()
            self.update_session(conn.session)
        self.waiters = [w for w in self.waiters if w.conn is not conn]

    def watch(self, sock: socket.socket, data: Any, have: int, wanted: int) -> int:
        """Change the events a registered socket is selected for (none removes it) and return the new set."""
        if wanted != have and sock.fileno() != -1:
            with contextlib.suppress(KeyError, ValueError, OSError):
                if have and wanted:
                    self.selector.modify(sock, wanted, data)
                elif wanted:
                    self.selector.register(sock, wanted, data)
                else:
                    self.selector.unregister(sock)
        return wanted

    def update_conn(self, conn: Conn) -> None:
        """A client connection is read unless its session's input to Docker is backed up and written while output waits in ``outbuf``."""
        if conn.sock.fileno() == -1 or conn.sock.fileno() not in self.conns:
            return
        backed_up = conn.session is not None and len(conn.session.pending_in) >= SESSION_BUFFER_LIMIT
        wanted = (0 if backed_up else selectors.EVENT_READ) | (selectors.EVENT_WRITE if conn.outbuf else 0)
        conn.interest = self.watch(conn.sock, conn, conn.interest, wanted)
        if conn.session is not None:
            self.update_session(conn.session)

    def update_session(self, session: Session) -> None:
        """The Docker side of a session is read unless the client's output is backed up and written while input waits."""
        docker = session.docker_socket
        if docker.fileno() == -1 or self.sessions.get(docker.fileno()) is not session:
            return
        backed_up = len(session.conn.outbuf) >= SESSION_BUFFER_LIMIT
        wanted = (0 if backed_up else selectors.EVENT_READ) | (selectors.EVENT_WRITE if session.pending_in else 0)
        session.interest = self.watch(docker, "session_docker", session.interest, wanted)

    def read_conn(self, conn: Conn) -> None:
        """Read what a client sent and handle each complete line."""
        try:
            data = conn.sock.recv(65536)
        except BlockingIOError:
            return
        except OSError:
            data = b""
        if not data:
            self.close_conn(conn)
            return
        if conn.session is not None:
            self.session_client_data(conn, data)
            return
        conn.buffer += data
        if len(conn.buffer) > MAX_LINE:
            self.close_conn(conn)
            return
        while b"\n" in conn.buffer and not conn.closed:
            line, conn.buffer = conn.buffer.split(b"\n", 1)
            self.handle_line(conn, line)

    def handle_line(self, conn: Conn, line: bytes) -> None:
        """Parse one request line and answer it."""
        try:
            request = json.loads(line)
            request_id = str(request["id"])
            method = str(request["method"])
        except (ValueError, KeyError, TypeError):
            conn.send({"ok": False, "error": "malformed request", "code": "protocol"})
            return
        if method == "live":
            conn.send({"id": request_id, "ok": True, "result": {"live": True, "pid": os.getpid(), "status": self.status}})
            return
        try:
            result = self.dispatch(conn, request, request_id, method)
            if result is not PENDING:
                conn.send({"id": request_id, "ok": True, "result": result})
        except LauncherError as error:
            conn.send({"id": request_id, "ok": False, "error": str(error), "code": error.code})
        except (run_lock.Refused, run_lock.RegistryError) as error:
            conn.send({"id": request_id, "ok": False, "error": str(error), "code": "refused"})
        except memory_budget.BudgetError as error:
            conn.send({"id": request_id, "ok": False, "error": str(error), "code": "budget"})
        except Exception as error:
            # A bug in a handler must not stop the only process that owns Docker: the request fails and the loop goes on.
            self.report_failure(f"method {method}")
            conn.send({"id": request_id, "ok": False, "error": f"internal error: {type(error).__name__}", "code": "internal"})

    # -- authentication, idempotency and dispatch ------------------------------------------------------------------

    def restart_running(self) -> bool:
        """True while a Docker restart is recorded."""
        with self.tx(write=False) as state:
            return bool(state["launcher"].get("restart"))

    def ecs_ready(self) -> bool:
        return bool(self.ecs_jobs["enabled"] and self.ecs_runtime is not None)

    def cloud_request(self, method: str, params: Mapping[str, Any]) -> bool:
        if not self.ecs_ready():
            return False
        if method == "acquire":
            return True
        return method == "submit" and any(row["template"] == params.get("template") for row in self.ecs_jobs["jobs"])

    def dispatch(self, conn: Conn, request: dict[str, Any], request_id: str, method: str) -> Any:
        """Authenticate, check the request record and run the method in one transaction with its effect (D1 section 3)."""
        if request.get("v") != PROTOCOL:
            raise LauncherError(f"protocol {request.get('v')!r} is not supported", "protocol")
        spec = METHODS.get(method)
        if spec is None:
            raise LauncherError(f"unknown method {method}", "protocol")
        params = request.get("params") or {}
        principal = dict(request.get("principal") or {"kind": "any"})
        if not isinstance(params, dict):
            raise LauncherError("params must be an object", "protocol")
        if method == "submit" and params.get("target") == "ecs" and not self.ecs_ready():
            raise LauncherError("ECS is disabled or its runtime is not integrated", "refused")
        cloud_request = self.cloud_request(method, params)
        if self.restart_running() and method not in ("ping", "status") and not cloud_request:
            raise LauncherError("a Docker restart is running", "refused")
        if self.status == "starting" and method not in ("ping", "status") and not cloud_request:
            raise Degraded("the launcher is starting")
        if self.status == "degraded" and method not in ("ping", "status", "shutdown", "cancel", "logs", "release_allocation", "recover") and not cloud_request:
            raise Degraded(f"degraded: {self.degraded_reason}")
        if principal.get("kind") not in spec.kinds:
            raise LauncherError(f"{method} is not callable by principal {principal.get('kind')!r}", "principal")
        if (
            spec.owner_flag
            and method in ("docker_restart", "shutdown", "recover", "catalog_authority")
            and not principal.get("owner")
            and not params.get("owner")
        ):
            raise LauncherError(f"{method} needs --owner", "principal")
        with self.tx(write=spec.mutating) as state:
            key = self.authenticate(conn, principal, request_id, method, state)
            payload_hash = sha256_text(canonical({"p": params, "o": bool(params.get("owner") or principal.get("owner"))}))
            ctx = Ctx(conn, method, request_id, params, principal, key, state)
            if spec.mutating:
                record = state["requests"].get(key)
                if record is not None:
                    if record["payload_sha256"] != payload_hash:
                        raise LauncherError("this request ID was used with a different payload", "replay")
                    if record["state"] == "done":
                        if record.get("outcome") == "error":
                            raise LauncherError(str(record["result"]["error"]), str(record["result"]["code"]))
                        return record["result"]
                    if record["state"] == "pending":
                        self.pending_conns.setdefault(key, []).append((conn, request_id))
                        return PENDING
            result = self.handlers[method](ctx)
            if spec.mutating:
                if result is PENDING:
                    state["requests"][key] = {"payload_sha256": payload_hash, "state": "pending", "result": None, "method": method, "at": utc_now()}
                    self.pending_conns.setdefault(key, []).append((conn, request_id))
                else:
                    stored = result.pop("_not_stored", None) if isinstance(result, dict) else None
                    state["requests"][key] = {
                        "payload_sha256": payload_hash,
                        "state": "done",
                        "result": dict(result) if isinstance(result, dict) else result,
                        "method": method,
                        "at": utc_now(),
                    }
                    if stored is not None:
                        result = {**result, **stored}
            return result

    def authenticate(self, conn: Conn, principal: dict[str, Any], request_id: str, method: str, state: dict[str, Any]) -> str:
        """Check the principal and return the request-record key prefix for it (D1 section 3 item 1)."""
        kind = principal.get("kind")
        if kind == "any":
            return f"any:{os.getuid()}|{method}|{request_id}"
        if kind == "run":
            run = state["runs"].get(str(principal.get("run_id")))
            token = str(principal.get("token", ""))
            if run is None or not token or not secrets.compare_digest(run_lock.digest(token), run["token_sha256"]):
                raise LauncherError("token does not match the run", "principal")
            return f"run:{run['run_id']}|{method}|{request_id}"
        if kind == "alloc":
            allocation = state["allocations"].get(str(principal.get("alloc_id")))
            if allocation is None:
                raise LauncherError("no such allocation", "principal")
            if not is_descendant(conn.peer, int(allocation["holder_pid"]), str(allocation["holder_start"])):
                raise LauncherError("the caller is not the allocation's holder or a descendant of it", "principal")
            return f"alloc:{allocation['id']}|{method}|{request_id}"
        raise LauncherError("unknown principal", "principal")

    # -- Docker connection, event stream, probe and scan (D1 section 2 item 6) --------------------------------------

    def connect_docker(self) -> None:
        """First connection: subscribe, probe, scan. A failure leaves the launcher degraded and retrying."""
        self.status = "starting"
        self.try_reconnect()

    def mark_degraded(self, reason: str) -> None:
        """Refuse new work until the stream is back; phases are kept with ``docker_state: unknown`` (D1 section 5 item 3)."""
        if self.status != "degraded" or self.degraded_reason != reason:
            self.status = "degraded"
            self.degraded_reason = reason
            self.docker_ok = False
            with self.tx() as state:
                state["launcher"].update({"status": "degraded", "docker_state": "unknown", "degraded_reason": reason})
        if self.stream is not None:
            self.stream.close()
            self.stream = None
        self.stream_acked = False
        self.next_retry = time.monotonic() + self.backoff
        self.backoff = min(self.backoff * 2, self.timings.backoff_cap)

    def try_reconnect(self) -> None:
        """Open the events stream first, then record a new epoch and run the probe; the scan follows the probe's acknowledgment."""
        try:
            if not self.docker.ping():
                raise DockerError(0, "ping failed")
            stream = self.docker.events(f"{LABEL_REGISTRY}={self.registry_id}")
        except DockerError as error:
            self.mark_degraded(f"Docker does not answer: {error.message}")
            return
        self.epoch += 1
        self.stream = stream
        self.stream_acked = False
        self.docker_ok = True
        # A reconnect is tried again only after this stream fails: a lost stream or an unacknowledged probe arms the next try.
        self.next_retry = float("inf")
        with self.tx() as state:
            state["launcher"]["stream_epoch"] = int(state["launcher"].get("stream_epoch", 0)) + 1
            self.epoch = int(state["launcher"]["stream_epoch"])
            state["launcher"]["docker_state"] = "ok"
        threading.Thread(target=self.read_events, args=(stream, self.epoch), name=f"events-{self.epoch}", daemon=True).start()
        self.begin_probe()

    def read_events(self, stream: EventStream, epoch: int) -> None:
        """Thread body: every event line goes to the loop as a message from the first byte; the end of the stream is a message too."""
        try:
            while True:
                line = stream.readline()
                if not line:
                    break
                line = line.strip()
                if line:
                    self.messages.put(("event", str(epoch), json.loads(line)))
                    with contextlib.suppress(OSError):
                        os.write(self.wake_write, b"x")
        except Exception as error:
            # Any failure of the stream, including a close from the loop, ends it; the loop then degrades and reconnects.
            reason = type(error).__name__
        else:
            reason = "closed by the daemon"
        self.messages.put(("stream_lost", str(epoch), reason))
        with contextlib.suppress(OSError):
            os.write(self.wake_write, b"x")

    def begin_probe(self) -> None:
        """Record a ``create`` operation for the probe container; the subscription is proven when its create event arrives."""
        nonce = secrets.token_hex(6)
        launch_id = f"probe-{self.epoch}-{nonce}"
        actions: list[Callable[[], None]] = []
        with self.tx() as state:
            attempt = self.new_attempt(state, DIAGNOSTIC_OWNER, "probe", launch_id, [], {}, request="launcher", checkout=str(self.cfg.root), seconds=60)
            attempt["never_start"] = True
            attempt["probe"] = {"nonce": nonce, "epoch": self.epoch}
            self.start_create(state, attempt, actions, extra_labels={LABEL_PROBE: nonce})
        self.probe = {"nonce": nonce, "attempt": attempt["id"], "deadline": time.monotonic() + self.timings.probe_timeout, "epoch": self.epoch}
        for action in actions:
            action()

    def probe_acknowledged(self) -> None:
        """The probe's create event arrived on this stream: record the acknowledgment and scan before any request is accepted."""
        if self.probe is None or self.stream_acked:
            return
        self.stream_acked = True
        with self.tx() as state:
            attempt = state["attempts"].get(self.probe["attempt"])
            if attempt is not None and is_open(attempt):
                attempt["cancel_intent"] = attempt["cancel_intent"] or {"reason": "probe_done", "at": utc_now()}
        self.dirty.add(self.probe["attempt"])
        self.start_scan()

    def start_scan(self) -> None:
        """List every container, network and volume with the registry label in all states (and legacy containers without it)."""
        self.scan_seq += 1
        sequence = self.scan_seq
        self.scan_wall[sequence] = time.time()
        label = f"{LABEL_REGISTRY}={self.registry_id}"

        def listing() -> dict[str, Any]:
            return {
                "seq": sequence,
                "epoch": self.epoch,
                "containers": self.docker.containers(label=label),
                "networks": self.docker.networks(label),
                "volumes": self.docker.volumes(label),
                "legacy": self.docker.containers(label=LABEL_RUN),
            }

        self.spawn(f"scan:{sequence}", "scan", listing)

    def process_scan(self, listing: dict[str, Any]) -> None:
        """Reconcile a listing: bind or dispose of every container, mark vanished ones, settle unresolved creates (D1 section 4 items 7 and 8)."""
        if listing["epoch"] != self.epoch or self.stream is None or not self.stream_acked:
            return  # a scan of a stream that was lost, or replaced, cannot restore serving (D1 section 2 item 6)
        present: set[str] = set()
        for container in listing["containers"]:
            container_id = container["Id"]
            present.add(container_id)
            name = str(container["Names"][0]).lstrip("/")
            self.inventory[container_id] = {
                **self.inventory.get(container_id, {}),
                "state": container["State"],
                "name": name,
                "labels": container.get("Labels") or {},
            }
            self.consider(container_id)
        for container_id, entry in list(self.inventory.items()):
            if container_id not in present and entry.get("state") != "absent":
                entry["state"] = "absent"
                entry["seen_absent_at"] = time.monotonic()
        self.foreign = [str(c["Names"][0]).lstrip("/") for c in listing["legacy"] if c.get("Labels", {}).get(LABEL_REGISTRY) != self.registry_id]
        self.scan_done_seq = max(self.scan_done_seq, listing["seq"])
        self.names_seen = {str(c["Names"][0]).lstrip("/"): c["Id"] for c in listing["containers"]}
        self.networks_seen = sorted(str(n.get("Name")) for n in listing["networks"])
        self.settle_unresolved_creates(listing["seq"])
        self.settle_network_ops(listing["networks"])
        self.settle_orchestration_network_ops(listing["networks"])
        self.settle_lineage_network_ops(listing["networks"])
        self.dirty.update(self.bound.values())
        self.dirty.update(self.open_attempt_ids())
        if self.status != "serving":
            self.status = "serving"
            self.backoff = self.timings.backoff_start
            with self.tx() as state:
                state["launcher"].update({"status": "serving", "docker_state": "ok", "degraded_reason": ""})
        self.changed = True

    def open_attempt_ids(self) -> list[str]:
        """Attempts that have not ended."""
        with self.tx(write=False) as state:
            return [aid for aid, attempt in state["attempts"].items() if is_open(attempt)]

    def consider(self, container_id: str) -> None:
        """A container was seen: if an attempt owns it, wake the attempt; otherwise inspect it to adopt it or dispose of it."""
        attempt_id = self.bound.get(container_id)
        if attempt_id is not None:
            self.dirty.add(attempt_id)
            return
        self.spawn(f"inspect:{container_id}", "inspect", self.docker.inspect, container_id)

    def on_event(self, epoch: int, event: dict[str, Any]) -> None:
        """Apply one Docker event: update the inventory, acknowledge the probe, hand unknown containers to adoption."""
        if epoch != self.epoch or event.get("Type") != "container":
            return
        action = str(event.get("Action", "")).split(":")[0].strip()
        actor = event.get("Actor") or {}
        container_id = str(actor.get("ID", ""))
        attributes = actor.get("Attributes") or {}
        if attributes.get(LABEL_REGISTRY) != self.registry_id or not container_id:
            return
        entry = self.inventory.setdefault(container_id, {"state": "created", "name": attributes.get("name", ""), "labels": dict(attributes)})
        entry["name"] = attributes.get("name", entry.get("name", ""))
        if action == "create":
            entry["state"] = "created"
            if self.probe is not None and attributes.get(LABEL_PROBE) == self.probe["nonce"] and epoch == self.probe["epoch"]:
                self.probe_acknowledged()
        elif action == "start":
            entry["state"] = "running"
        elif action in ("die", "stop", "kill", "oom"):
            if action == "die":
                entry["state"] = "exited"
                entry["exit"] = int(attributes.get("exitCode", 0) or 0)
        elif action == "destroy":
            entry["state"] = "absent"
        if action in ("create", "start", "die", "destroy", "stop"):
            self.consider(container_id)
        if container_id in self.bound:
            self.dirty.add(self.bound[container_id])

    def drain_messages(self) -> None:
        """Apply every worker result and event that arrived."""
        while True:
            try:
                tag, key, payload = self.messages.get_nowait()
            except queue.Empty:
                return
            self.inflight.discard(key)
            if tag == "event":
                self.on_event(int(key), payload)
                continue
            if tag == "stream_lost":
                if int(key) == self.epoch and self.running:
                    self.mark_degraded("the event stream dropped")
                continue
            outcome, value = payload
            handler = self.message_handlers.get(tag)
            if handler is None:
                continue
            started_in = self.spawn_epoch.pop(key, self.epoch)
            if outcome == "docker" and value.status == 0 and started_in == self.epoch:
                # A call that began on an earlier connection says nothing about the current one.
                self.mark_degraded(f"Docker does not answer: {value.message}")
            handler(key, outcome, value)
            self.changed = True

    def on_scan(self, key: str, outcome: str, value: Any) -> None:
        """Worker message for a scan."""
        if outcome == "ok":
            self.process_scan(value)

    def timers(self) -> None:
        """Time-based work: reconnects, the probe deadline, deadlines, stale holders and waiters."""
        now = time.monotonic()
        if self.status == "degraded" and self.running and now >= self.next_retry:
            self.try_reconnect()
        if self.probe is not None and not self.stream_acked and now > self.probe["deadline"] and self.docker_ok and self.probe["epoch"] == self.epoch:
            self.probe_failed()
        if now - self.last_stale_check > 1.0:
            self.last_stale_check = now
            self.check_holders()
        for waiter in list(self.waiters):
            if now > waiter.deadline:
                self.waiters.remove(waiter)
                self.finish_wait(waiter, timed_out=True)
        self.session_timers(now)
        self.maintenance(now)

    def probe_failed(self) -> None:
        """The probe event did not arrive within the limit: report degraded, refuse new work, no scan (D1 section 2 item 6)."""
        if self.probe is None:
            return
        attempt_id = self.probe["attempt"]
        with self.tx() as state:
            attempt = state["attempts"].get(attempt_id)
            if attempt is not None and is_open(attempt):
                attempt["cancel_intent"] = attempt["cancel_intent"] or {"reason": "probe_failed", "at": utc_now()}
        self.dirty.add(attempt_id)
        self.probe = None
        self.mark_degraded("the subscription was not acknowledged by the probe within the limit")

    # -- attempts, operations and launch (D1 section 4) --------------------------------------------------------------

    def new_attempt(
        self,
        state: dict[str, Any],
        owner: str,
        template: str,
        launch_id: str,
        args: Sequence[str],
        params: Mapping[str, Any],
        *,
        request: str,
        checkout: str,
        seconds: int,
    ) -> dict[str, Any]:
        """Record a queued attempt; the deadline is fixed here, at submit (D1 section 5 item 1)."""
        n = 1 + sum(1 for a in state["attempts"].values() if a["owner"] == owner and a["launch_id"] == launch_id)
        attempt_id = new_id("at")
        generation = self.owner_generation(state, owner)
        attempt = {
            "id": attempt_id,
            "owner": owner,
            "template": template,
            "launch_id": launch_id,
            "n": n,
            "args": list(args),
            "params": dict(params),
            "phase": "queued",
            "reason": None,
            "generation": generation,
            "deadline_utc": int(time.time()) + int(seconds),
            "timeout_seconds": int(seconds),
            "cancel_intent": None,
            "timeout_intent": None,
            "container_name": None,
            "container_id": None,
            "exit_code": None,
            "request": request,
            "checkout": checkout,
            "plan_env": None,
            "attempt_alloc": None,
            "current_op": None,
            "spec_digest": None,
            "cleanup": None,
            "created_at": utc_now(),
            "ended_at": None,
        }
        state["attempts"][attempt_id] = attempt
        return attempt

    @staticmethod
    def owner_generation(state: Mapping[str, Any], owner: str) -> int:
        """The owner's current generation."""
        kind, _, ident = owner.partition(":")
        if kind == "run":
            return int(state["runs"][ident].get("generation", 1))
        if kind == "alloc":
            return int(state["allocations"][ident].get("generation", 1))
        return 1

    @staticmethod
    def template_problem(state: Mapping[str, Any], owner: str, template_name: str, params: Mapping[str, Any]) -> str | None:
        """What the template's owner predicates (D1 section 7) refuse for this owner, or None.

        A run owner needs the domain and mode the template names (and, for a catalog writer job, the acquisition lock with a declared
        write prefix); a reader template needs a reader registration of its allocation.
        """
        template = TEMPLATES[template_name]
        kind, _, ident = owner.partition(":")
        if kind == "run":
            run = state["runs"].get(ident)
            if run is None:
                return f"run {ident} does not exist"
            domains = template.run_domains
            job = SPARK_JOBS.get(str(params.get("job")), {})
            if template_name == "spark_write" and job.get("domains"):
                domains = tuple(job["domains"])
            if domains is not None and run["domain"] not in domains:
                return f"template {template_name} needs a run of domain {' or '.join(domains)}; run {ident} is a {run['domain']} run"
            if template.run_modes is not None and run["mode"] not in template.run_modes:
                return f"template {template_name} needs a run of mode {' or '.join(template.run_modes)}; run {ident} is a {run['mode']} run"
            if template_name == "pipeline_dbt" and params.get("layer") in run.get("layer_completions", {}):
                return "an accepted layer seal refuses a further build in this generation"
            if template_name in ("gold_export", "gold_publish"):
                return Launcher.gold_problem(state, ident, run, template_name, params)
            if template_name == "pipeline_publish" and params.get("layer") in LAYER_WRITE_PREFIX:
                return Launcher.publish_prefix_problem(str(params["layer"]), ident, run, template_name)
            if template_name == "spark_write":
                prefixes = list(run.get("write_prefixes") or [])
                if not prefixes:
                    return f"template spark_write needs declared write prefixes; run {ident} declares none"
                try:
                    run_lock.check_prefixes(prefixes, f"run {ident} declares write prefix")
                except run_lock.Refused as error:
                    return f"template spark_write needs plain declared write prefixes; {error}"
                wanted = job.get("prefix") or ("lakehouse/" if job.get("template") == "spark_write" else None)
                if wanted and not any(run_lock.covers(p, wanted) for p in prefixes):
                    return f"job {params.get('job')} writes under {wanted}; run {ident} declares {', '.join(prefixes)}"
                if wanted:
                    try:
                        run_lock.check_cross_domain(dict(state), run["domain"], [wanted])
                    except run_lock.Refused as error:
                        return str(error)
        elif kind == "alloc" and template.reader:
            if not any(r["kind"] == "reader" and r["allocation"] == ident for r in state["registrations"].values()):
                return f"template {template_name} needs a reader registration of allocation {ident}"
        return None

    @staticmethod
    def gold_problem(state: Mapping[str, Any], ident: str, run: Mapping[str, Any], template_name: str, params: Mapping[str, Any]) -> str | None:
        """What refuses a gold template for this run, or None: a sealed build (except for maintenance) and the gold prefix of its mode declared."""
        try:
            Launcher.gold_inputs(state, f"run:{ident}", template_name, params)
        except LauncherError as error:
            return str(error)
        wanted = GOLD_WRITE_PREFIX[run["mode"]]
        prefixes = list(run.get("write_prefixes") or [])
        if not prefixes or not all(within_prefix(str(p), wanted) for p in prefixes):
            return f"template {template_name} writes under {wanted}; run {ident} declares {', '.join(prefixes) or 'no write prefix'}"
        if template_name == "gold_publish" and GOLD_JOBS[str(params["job"])]["sealed"]:
            acceptance = run.get("consumer_acceptance")
            if not isinstance(acceptance, dict) or acceptance.get("sealed_sha256") != completion_of(run)["sha256"]:
                return f"run {ident} has no accepted consumer result for its sealed copy; call accept_consumer on the finished export first"
        try:
            run_lock.check_cross_domain(dict(state), run["domain"], [wanted])
        except run_lock.Refused as error:
            return str(error)
        return None

    @staticmethod
    def publish_prefix_problem(layer: str, ident: str, run: Mapping[str, Any], template_name: str) -> str | None:
        """What refuses a publish template of a layer for this run, or None: its mode names a prefix and every write prefix it declares lies inside it."""
        wanted = LAYER_WRITE_PREFIX[layer].get(str(run.get("mode")))
        if wanted is None:
            return f"template {template_name} writes {layer}; run {ident} has mode {run.get('mode')!r}, which has no {layer} write prefix"
        outside = [str(p) for p in run.get("write_prefixes") or [] if not within_prefix(str(p), wanted)]
        if outside:
            return f"template {template_name} writes under {wanted}; run {ident} declares {', '.join(outside)}"
        return None

    def admissible(self, state: Mapping[str, Any], attempt: Mapping[str, Any]) -> bool:
        """The owner is held, the generation is current and nothing blocks a positive operation (D1 section 4 item 1a)."""
        owner = attempt["owner"]
        kind, _, ident = owner.partition(":")
        if owner == DIAGNOSTIC_OWNER:
            return not self.stream_acked and self.probe is not None and self.probe["attempt"] == attempt["id"]
        if kind == "stack":
            stack = state["launcher"].get(ident) or {} if ident in ("orchestration", "lineage") else {}
            return stack.get("phase") in ("starting", "up") and not state["launcher"].get("restart")
        if kind == "service" and ident.startswith("pipeline:"):
            run = state["runs"].get(ident.removeprefix("pipeline:"))
            return (
                run is not None
                and run["state"] in ("held", "finalizing")
                and run.get("generation", 1) == attempt.get("generation")
                and run.get("pipeline_network", {}).get("phase") in ("networks_ready", "ready")
            )
        if kind == "service":
            catalog = state["launcher"].get("catalog") or {}
            # A launcher outside the catalog's checkout sends no positive catalog-service operation; cleanup stays allowed (A86, review C3a-1).
            return catalog.get("phase") in ("starting", "up") and not state["launcher"].get("barrier") and not self.catalog_checkout_refusal()
        if kind == "run":
            run = state["runs"].get(ident)
            if run is None or run["state"] != "held" or int(run.get("generation", 1)) != attempt["generation"]:
                return False
        elif kind == "alloc":
            allocation = state["allocations"].get(ident)
            if allocation is None or allocation["state"] != "held" or int(allocation.get("generation", 1)) != attempt["generation"]:
                return False
        else:
            return False
        if "execution" in attempt and not self.catalog_epoch_matches(state, attempt):
            return False
        if state["launcher"].get("restart") and "execution" not in attempt:
            return False
        if self.template_problem(state, owner, attempt["template"], attempt["params"]):
            return False
        return "execution" in attempt or not (TEMPLATES[attempt["template"]].needs_catalog and state["launcher"].get("barrier"))

    def intent_reason(self, attempt: Mapping[str, Any], default: str = "exited") -> str:
        """The recorded end reason for an attempt that was cancelled, timed out or fenced."""
        if attempt.get("mismatch"):
            return "create_failed"
        if attempt.get("timeout_intent"):
            return "timeout"
        if attempt.get("cancel_intent"):
            return "fenced" if attempt["cancel_intent"].get("reason") == "fenced" else "cancelled"
        return default

    def compose_for(self, checkout: Path) -> ComposeModel:
        """The Compose model of a checkout, parsed once per file hash."""
        path = checkout / "docker-compose.yaml"
        if not path.is_file():
            raise LauncherError(f"{checkout} has no docker-compose.yaml", "spec")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest not in self.compose_models:
            self.compose_models[digest] = ComposeModel(path)
        return self.compose_models[digest]

    def renderer_for(self, checkout: Path) -> Renderer:
        """A renderer for a validated checkout."""
        return Renderer(self.cfg, self.compose_for(checkout), checkout, self.main_checkout, self.key)

    def orchestration_compose_for(self, checkout: Path) -> ComposeModel:
        """The orchestration Compose model of a checkout, parsed once per file hash."""
        path = checkout / ORCHESTRATION_COMPOSE
        if not path.is_file():
            raise LauncherError(f"{checkout} has no {ORCHESTRATION_COMPOSE}", "spec")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest not in self.compose_models:
            self.compose_models[digest] = ComposeModel(path)
        return self.compose_models[digest]

    def renderer_of(self, template_name: str, checkout: Path) -> Renderer:
        """The renderer of a template: the orchestration templates read the orchestration Compose model, the others the lakehouse one."""
        if template_name in ("lineage_service", "lineage_dump"):
            path = checkout / (LINEAGE_COMPOSE if template_name == "lineage_service" else LINEAGE_CHECKS_COMPOSE)
            if not path.is_file():
                raise LauncherError("lineage Compose model is missing", "spec")
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if digest not in self.compose_models:
                self.compose_models[digest] = ComposeModel(path)
            return Renderer(self.cfg, self.compose_models[digest], checkout, self.main_checkout, self.key)
        if template_name.startswith("orchestration_"):
            return Renderer(self.cfg, self.orchestration_compose_for(checkout), checkout, self.main_checkout, self.key)
        return self.renderer_for(checkout)

    def check_checkout(self, text: str) -> Path:
        """A request's checkout: an absolute folder of this repository (the registry's main checkout or one of its worktrees)."""
        path = Path(text)
        if not path.is_absolute() or not (path / "docker-compose.yaml").is_file():
            raise LauncherError(f"checkout {text!r} is not a repository folder with docker-compose.yaml", "args")
        if self.registry.kind == "real" and run_lock.main_checkout(path) != self.main_checkout:
            raise LauncherError(f"checkout {text!r} belongs to another repository", "args")
        return path.resolve()

    def attempt_folder(self, attempt: Mapping[str, Any]) -> Path:
        """Where an attempt's exit record and last log lines are kept."""
        kind, _, ident = str(attempt["owner"]).partition(":")
        if kind == "run":
            return self.cfg.runs_root.resolve() / ident / "attempts" / str(attempt["id"])
        if kind == "alloc":
            return self.registry.path.resolve() / "allocations" / ident / "attempts" / str(attempt["id"])
        return self.registry.path.resolve() / "services" / str(attempt["id"])

    def labels_for(self, attempt: Mapping[str, Any], op_id: str, extra: Mapping[str, str] | None) -> dict[str, str]:
        """The full label set of an attempt's container (D1 section 4 item 3)."""
        kind, _, ident = str(attempt["owner"]).partition(":")
        labels = {
            LABEL_REGISTRY: self.registry_id,
            "hai.owner": str(attempt["owner"]),
            LABEL_GENERATION: str(attempt["generation"]),
            LABEL_LAUNCH: str(attempt["launch_id"]),
            LABEL_ATTEMPT: str(attempt["id"]),
            LABEL_OP: op_id,
            "hai.template": str(attempt["template"]),
        }
        if kind == "run":
            labels[LABEL_RUN] = ident
        elif kind == "alloc":
            labels[LABEL_ALLOCATION] = ident
        labels.update(extra or {})
        return labels

    @staticmethod
    def gold_inputs(state: Mapping[str, Any], owner: str, template_name: str, params: Mapping[str, Any]) -> tuple[Mapping[str, Any] | None, str]:
        """The sealed-build record and the mode of a run that a gold template renders for; (None, "") for any other template or owner."""
        kind, _, ident = owner.partition(":")
        if kind != "run" or template_name not in ("gold_export", "gold_publish"):
            return None, ""
        run = state["runs"][ident]
        spec = GOLD_JOBS.get(str(params.get("job")), {})
        needs = template_name == "gold_export" or bool(spec.get("sealed"))
        return (completion_of(run) if needs else None), str(run["mode"])

    @staticmethod
    def gold_accepted(state: Mapping[str, Any], owner: str, template_name: str, params: Mapping[str, Any]) -> str:
        """The SHA-256 of the consumer result the launcher accepted for the run, for a gold step that runs against it; an empty string for any other launch."""
        kind, _, ident = owner.partition(":")
        if kind != "run" or template_name != "gold_publish" or not GOLD_JOBS.get(str(params.get("job")), {}).get("sealed"):
            return ""
        acceptance = state["runs"][ident].get("consumer_acceptance")
        return str(acceptance.get("result_sha256", "")) if isinstance(acceptance, dict) else ""

    def owner_name(self, attempt: Mapping[str, Any]) -> str:
        """The owner part of a container name."""
        kind, _, ident = str(attempt["owner"]).partition(":")
        return {"run": ident, "alloc": ident, "service": "svc", "stack": "orch", "launcher": "diag"}.get(kind, "x")

    def render_attempt(
        self,
        state: Mapping[str, Any],
        attempt: Mapping[str, Any],
        op_id: str,
        extra: Mapping[str, str] | None,
        fetched: Mapping[str, Fetched] | None,
    ) -> tuple[Rendered, frozenset[str]]:
        """Render an attempt's create specification from its template and recorded values.

        With no ``fetched`` secrets the render records the secret names it asks for and uses placeholders; it is a real result only when none are asked
        for. Otherwise the fetched values (or the errors that stopped their fetch) fill the specification.
        """
        template = TEMPLATES[attempt["template"]]
        renderer = self.renderer_of(attempt["template"], Path(attempt["checkout"]))
        renderer.recording = set() if fetched is None else None
        renderer.supplied = fetched
        try:
            rendered = self.render_with(renderer, template, state, attempt, op_id, extra)
            return rendered, frozenset(renderer.recording or ())
        finally:
            renderer.recording, renderer.supplied = None, None
            renderer.fetched.clear()
            renderer.interpolated.clear()

    def render_with(
        self, renderer: Renderer, template: Template, state: Mapping[str, Any], attempt: Mapping[str, Any], op_id: str, extra: Mapping[str, str] | None
    ) -> Rendered:
        """Render an attempt's specification with a renderer whose secret source is already set."""
        labels = self.labels_for(attempt, op_id, extra)
        provenance: dict[str, str] = {}
        snapshot = run_folder = None
        kind, _, ident = str(attempt["owner"]).partition(":")
        if kind == "run":
            run_folder = self.cfg.runs_root / ident
            run_folder.mkdir(parents=True, exist_ok=True)
            if attempt["template"] in ("real_dbt", "pipeline_dbt"):
                if attempt["template"] == "real_dbt" or (attempt["params"].get("layer") == "silver" and state["runs"][ident]["mode"] == "real"):
                    provenance = provenance_vars(state["runs"][ident])
                pointer = state["runs"][ident].get("snapshot")
                if pointer is None:
                    raise LauncherError("the run has no complete snapshot", "spec")
                snapshot = run_folder / "snapshots" / str(pointer)
            if attempt["template"] == "sealed_consumer":
                provenance = consumer_vars(state["runs"][ident])
            if attempt["template"] == "spark_read":
                run_folder = run_folder
        if attempt["template"] in SEALED_READERS and build_provenance.SEALED_RUN_PARAM in attempt["params"]:
            sealed_id = str(attempt["params"][build_provenance.SEALED_RUN_PARAM])
            provenance = self.sealed_reader_vars(state, sealed_id)
            run_folder = self.cfg.runs_root / sealed_id
        pipeline_run = state["runs"].get(ident) if kind == "run" else None
        if attempt["template"] == "pipeline_proxy":
            pipeline_run = state["runs"].get(str(attempt["params"]["run_id"]))
            run_folder = self.cfg.runs_root / str(attempt["params"]["run_id"])
        if kind == "service" and ident.startswith("pipeline:"):
            pipeline_run = state["runs"].get(ident.partition(":")[2])
            run_folder = self.cfg.runs_root / ident.partition(":")[2]
        completion, run_mode = self.gold_inputs(state, str(attempt["owner"]), str(attempt["template"]), attempt["params"])
        if pipeline_run is not None and attempt["template"].startswith("pipeline_"):
            run_mode = str(pipeline_run["mode"])
        inp = RenderInput(
            attempt["template"],
            self.owner_name(attempt),
            attempt["launch_id"],
            attempt["n"],
            attempt["generation"],
            op_id,
            attempt["plan_env"] or {},
            attempt["deadline_utc"],
            attempt["args"],
            attempt["params"],
            labels,
            snapshot,
            run_folder,
            provenance,
            tuple(self.image_cache.get(self.image_reference(attempt), (0.0, {}))[1].get("entrypoint", ())),
            completion,
            run_mode,
            self.gold_accepted(state, str(attempt["owner"]), str(attempt["template"]), attempt["params"]),
            self.pipeline_inputs(state, pipeline_run) if pipeline_run is not None else {},
        )
        if template.name == "catalog_service":
            return renderer.render_service(inp, str(attempt["params"]["service"]), self.registry_id)
        if template.name == "orchestration_service":
            return renderer.render_orchestration_service(inp, str(attempt["params"]["service"]), self.registry_id)
        if template.name == "lineage_service":
            return renderer.render_lineage_service(inp, str(attempt["params"]["service"]), self.registry_id)
        return renderer.render(inp, self.registry_id)

    def image_reference(self, attempt: Mapping[str, Any]) -> str:
        """The image the attempt's template runs (as a test registry may rename it)."""
        template = TEMPLATES[attempt["template"]]
        renderer = self.renderer_of(attempt["template"], Path(attempt["checkout"]))
        if template.name == "pipeline_proxy":
            return network_policy.PROXY_IMAGE
        if template.name in ("catalog_service", "orchestration_service", "lineage_service"):
            return renderer.image(str(attempt["params"]["service"]))
        if template.name == "orchestration_check":
            return renderer.image("airflow-check")
        if template.name == "orchestration_dump":
            return renderer.image("airflow-dump")
        if template.name == "lineage_dump":
            return renderer.image("dump")
        return renderer.image(template.service or "analytics")

    def resolve_image(self, reference: str) -> dict[str, Any] | None:
        """Worker: the image's identity and inherited defaults, or None when the image is absent."""
        info = self.docker.image(reference)
        return None if info is None else image_defaults(info)

    def on_imagedef(self, key: str, outcome: str, value: Any) -> None:
        """An image's defaults arrived: they stay valid for a short time and the waiting attempts move on."""
        if outcome == "ok":
            # An absent image is cached with empty defaults: the create then fails with the daemon's own refusal.
            self.image_cache[key.split(":", 1)[1]] = (time.monotonic(), value if value is not None else image_defaults(None))
        self.changed = True

    IMAGE_DEFAULTS_TTL = 30.0

    def start_create(
        self, state: dict[str, Any], attempt: dict[str, Any], actions: list[Callable[[], None]], extra_labels: Mapping[str, str] | None = None
    ) -> bool:
        """Record a ``create`` operation with the effective specification's digest, then dispatch it (D1 section 4 item 2).

        The effective specification includes the image's identity and inherited defaults, so they are read first (a worker); the attempt
        stays ``queued`` until they are known. Returns False while it waits.
        """
        reference = self.image_reference(attempt)
        cached = self.image_cache.get(reference)
        if cached is None or time.monotonic() - cached[0] > self.IMAGE_DEFAULTS_TTL:
            key = f"imagedef:{reference}"
            if key not in self.inflight:
                actions.append(lambda: self.spawn(key, "imagedef", self.resolve_image, reference))
            return False
        defaults = cached[1]
        finite = finite_attempt(attempt)
        if (
            finite
            and defaults.get("id")
            and (defaults["labels"].get(DEFAULT_IMAGE_LABEL) != "1" or defaults["labels"].get(DEFAULT_IMAGE_IMPLEMENTATION) != "go")
        ):
            self.finish_attempt(
                state, attempt, "create_failed", f"image_without_entrypoint: image {self.image_reference(attempt)} does not carry the Go deadline entrypoint"
            )
            return True
        if attempt.get("probe") and extra_labels is None:
            extra_labels = {LABEL_PROBE: attempt["probe"]["nonce"]}
        op_id = new_id("op")
        fetched = self.secret_results.get(attempt["id"])
        if fetched is not None and time.monotonic() - fetched[0] > self.SECRET_RESULT_TTL:
            self.secret_results.pop(attempt["id"], None)
            fetched = None
        fetch_key = f"secrets:{attempt['id']}"
        if fetched is None and fetch_key in self.inflight:
            return False
        try:
            rendered, wanted = self.render_attempt(state, attempt, op_id, extra_labels, None if fetched is None else fetched[1])
        except (LauncherError, secret_store.SecretStoreError) as error:
            self.secret_results.pop(attempt["id"], None)
            code = error.code if isinstance(error, LauncherError) else "spec"
            self.finish_attempt(state, attempt, "create_failed", f"{code}: {error}")
            return True
        if fetched is None and wanted:
            names = tuple(sorted(wanted))
            deadline, attempt_id = attempt["deadline_utc"], attempt["id"]
            actions.append(lambda: self.spawn(fetch_key, "secretfetch", self.fetch_secrets, names, deadline, attempt_id))
            return False
        self.secret_results.pop(attempt["id"], None)
        if attempt.get("capture_dbt_artifacts"):
            rendered.body.setdefault("Env", []).extend(dbt_lineage.capture_environment(attempt["id"]))
        effective_env = effective_config(rendered.body, defaults)["env"]
        if DEADLINE_RELAY_VARIABLE in effective_env:
            self.finish_attempt(
                state, attempt, "create_failed", f"spec: image {reference} sets {DEADLINE_RELAY_VARIABLE}, which belongs to the deadline entrypoint"
            )
            return True
        secret_env, secret_cmd, secret_spans = created_secrets(self.key, rendered, effective_env)
        spec = projection(rendered.body, self.key, secret_env, secret_cmd, defaults)
        state["operations"][op_id] = {
            "op_id": op_id,
            "attempt": attempt["id"],
            "kind": "create",
            "expected_phase": "creating",
            "generation": attempt["generation"],
            "dispatched_at": time.time(),
            "state": "open",
            "result": None,
            "name": rendered.name,
            "spec_digest": sha256_text(canonical(spec)),
            "projection": spec,
            "image_defaults": {
                **defaults,
                "env": [
                    f"{n}={commit_secret(self.key, v)}" if n in secret_env else f"{n}={v}" for n, _, v in (item.partition("=") for item in defaults["env"])
                ],
            },
            "secret_env": sorted(secret_env),
            "secret_cmd": secret_cmd,
            "secret_spans": secret_spans,
            "compose_sha256": self.compose_for(Path(attempt["checkout"])).sha256,
            "interpolation": {
                name: commit_secret(self.key, value) if name in COMPOSE_ENV_NAMES else value for name, value in sorted(rendered.interpolation.items())
            },
        }
        attempt.update({"phase": "creating", "container_name": rendered.name, "spec_digest": sha256_text(canonical(spec)), "current_op": op_id})
        if rendered.joins:
            attempt["joins"] = {network: list(aliases) for network, aliases in rendered.joins.items()}
        body, name = rendered.body, rendered.name
        actions.append(lambda: self.spawn(f"op:{op_id}", "create", self.docker_create, name, body))
        return True

    SECRET_RESULT_TTL = 30.0

    def fetch_secrets(self, names: tuple[str, ...], deadline_utc: int, attempt_id: str) -> dict[str, Fetched]:
        """Worker: read the named secrets under one absolute deadline and ask for the named role sessions, each for a job that ends at ``deadline_utc``.

        Each session request has its own deadline and a new session every time. The result stays in memory until the attempt's render uses it.
        """
        sessions = [name for name in names if name.startswith(pipeline_session.PREFIX)]
        secrets = [name for name in names if name not in sessions]
        found: dict[str, Fetched] = {}
        if secrets:
            found.update(secret_store.fetch_all(self.cfg.catalog_checkout or self.cfg.root, self.cfg.secrets_endpoint, secrets, self.cfg.secret_fetch_seconds))
        for name in sessions:
            try:
                found[name] = self.cfg.role_session(name.removeprefix(pipeline_session.PREFIX), deadline_utc, pipeline_session.session_name(attempt_id))
            except LauncherError as error:
                found[name] = pipeline_session.SessionError(str(error))
            except pipeline_session.SessionError as error:
                found[name] = error
        return found

    def on_secretfetch(self, key: str, outcome: str, value: Any) -> None:
        """The fetched secrets of an attempt arrived: the queued attempt renders with them on its next pass."""
        if outcome == "ok":
            self.secret_results[key.split(":", 1)[1]] = (time.monotonic(), value)
        self.changed = True

    def docker_create(self, name: str, body: dict[str, Any]) -> str:
        """Worker: create the container; a name that is already taken is looked up and returned, to be adopted or refused."""
        try:
            return self.docker.create(name, body, self.timings.create_timeout)
        except DockerError as error:
            if error.status != 409:
                raise
            existing = self.docker.inspect(name)
            if existing is None:
                raise
            return str(existing["Id"])

    def docker_start(self, container_id: str, joins: Mapping[str, Sequence[str]]) -> None:
        """Worker: join the container to the networks it was not created on, then start it.

        Nothing can start the container between the joins and the start request, and a retry joins only the networks the container does not show yet.
        """
        info = self.docker.inspect(container_id) if joins else None
        if info is not None:
            present = set((info.get("NetworkSettings") or {}).get("Networks") or {})
            for network, aliases in joins.items():
                if network not in present:
                    self.docker.connect_network(network, container_id, aliases)
        self.docker.start(container_id)

    def open_op(self, state: Mapping[str, Any], attempt_id: str, kind: str) -> dict[str, Any] | None:
        """The attempt's open operation of a kind, if any."""
        for op in state["operations"].values():
            if op["attempt"] == attempt_id and op["kind"] == kind and op["state"] == "open":
                return dict(op)
        return None

    def record_op(self, state: dict[str, Any], attempt: Mapping[str, Any], kind: str, expected_phase: str) -> str:
        """Record an operation of a kind for an attempt."""
        op_id = new_id("op")
        state["operations"][op_id] = {
            "op_id": op_id,
            "attempt": attempt["id"],
            "kind": kind,
            "expected_phase": expected_phase,
            "generation": attempt["generation"],
            "dispatched_at": time.time(),
            "state": "open",
            "result": None,
            "container_id": attempt.get("container_id"),
        }
        return op_id

    def settle_op(self, state: dict[str, Any], op_id: str, result: str, **extra: Any) -> None:
        """Mark an operation settled with its result."""
        op = state["operations"].get(op_id)
        if op is not None and op["state"] == "open":
            op.update({"state": "settled", "result": result, "settled_at": utc_now(), **extra})

    def advance_all(self) -> None:
        """One pass over every open attempt and every owner: the state machine of D1 sections 4 to 6."""
        if self.status == "starting" and not self.docker_ok:
            self.expire_lineage_startup()
            if self.ecs_ready():
                cloud_actions: list[Callable[[], None]] = []
                with self.tx() as state:
                    for attempt in state["attempts"].values():
                        if is_open(attempt) and "execution" in attempt:
                            self.advance(state, attempt, cloud_actions)
                for action in cloud_actions:
                    action()
            return
        actions: list[Callable[[], None]] = []
        with self.tx() as state:
            # The checkout refusal of a recorded catalog startup takes effect before any attempt of this pass advances (review C3a-1 delta 1).
            self.refuse_foreign_catalog_start(state)
            for attempt in list(state["attempts"].values()):
                if is_open(attempt):
                    self.advance(state, attempt, actions)
            self.advance_owners(state, actions)
        for action in actions:
            action()

    def process_dirty(self) -> None:
        """Run the state machine when something changed or on every tick."""
        self.dirty.clear()
        self.advance_all()

    def advance(self, state: dict[str, Any], a: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """Move one attempt a step; every Docker side effect is an operation recorded before it is dispatched."""
        execution = a.get("execution")
        if execution and execution["live_try"]:
            live = next(item for item in execution["placements"] if item["try_id"] == execution["live_try"])
            if live["target"] == "ecs":
                if not self.admissible(state, a) or not self.catalog_epoch_matches(state, a):
                    a["cancel_intent"] = a.get("cancel_intent") or {"reason": "fenced", "at": utc_now()}
                if self.ecs_runtime is not None:
                    self.ecs_runtime.advance(state, a, time.time())
                    if a["phase"] == "ended":
                        if execution["live_try"] is not None:
                            raise ecs_target.PlacementError("an unconfirmed cloud task cannot end its attempt")
                        self.finish_attempt(state, a, str(a["reason"]))
                return
        phase = a["phase"]
        if phase == "queued":
            self.advance_queued(state, a, actions)
        elif phase == "creating":
            self.advance_creating(state, a)
        elif phase == "created":
            self.advance_created(state, a, actions)
        elif phase == "starting":
            self.advance_starting(state, a, actions)
        elif phase == "started":
            self.advance_started(state, a, actions)
        elif phase == "exiting":
            self.advance_exiting(state, a, actions)

    def container_state(self, a: Mapping[str, Any]) -> str | None:
        """What the inventory knows of the attempt's container: created, running, exited, absent or None."""
        container_id = a.get("container_id")
        return self.inventory.get(container_id, {}).get("state") if container_id else None

    def advance_queued(self, state: dict[str, Any], a: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """Dispatch a queued attempt when its owner, the machine and the template's preconditions allow it."""
        if a["cancel_intent"] or a["timeout_intent"]:
            self.finish_attempt(state, a, self.intent_reason(a), "cancelled before creation")
            return
        if finite_attempt(a) and time.time() >= a["deadline_utc"] - SKEW_SECONDS - START_MARGIN_SECONDS:
            # Past the host start cutoff a queued finite attempt can never start: it ends now instead of waiting on memory or preparation.
            a["timeout_intent"] = {"reason": "start_after_deadline", "at": utc_now()}
            self.finish_attempt(state, a, self.intent_reason(a), "the start cutoff passed while queued")
            return
        owner = a["owner"]
        if not self.admissible(state, a) and owner != DIAGNOSTIC_OWNER:
            run = state["runs"].get(owner.partition(":")[2]) if owner.startswith("run:") else None
            allocation = state["allocations"].get(owner.partition(":")[2]) if owner.startswith("alloc:") else None
            gone = (run is not None and run["state"] != "held") or (allocation is not None and allocation["state"] != "held")
            if gone or (run is None and allocation is None and not owner.startswith(("service:", "stack:"))):
                a["cancel_intent"] = {"reason": "owner_not_held", "at": utc_now()}
            return
        if "execution" in a and self.advance_target(state, a, actions):
            return
        mac_problem = None if "execution" in a else self.mac_write_problem(state, owner, a["template"], a["params"], a["args"])
        if mac_problem:
            self.finish_attempt(state, a, "fenced", mac_problem)
            return
        if not self.docker_ok or self.status != "serving" and owner != DIAGNOSTIC_OWNER:
            return
        if a["template"] == "real_dbt" and owner.startswith("run:") and build_provenance.is_serialized(a["args"]):
            held = state["runs"].get(owner.partition(":")[2])
            blocker = build_provenance.dispatch_blocker(held, a, state["attempts"].values()) if held is not None else None
            if blocker == "sealed":
                a["cancel_intent"] = {"reason": "sealed", "at": utc_now()}
                return
            if blocker:
                a["waiting_on"] = blocker
                return
        if owner.startswith("alloc:"):
            allocation = state["allocations"][owner.partition(":")[2]]
            active = sum(1 for other in state["attempts"].values() if other["owner"] == owner and other["phase"] in ACTIVE_PHASES)
            if active >= int(allocation.get("workers", 1)):
                return
            a["plan_env"] = allocation["environment"]
            if not self.capacity_cleared(state, a, allocation, actions):
                return
        elif owner.startswith("run:"):
            if a["plan_env"] is None:
                if time.time() < float(a.get("capacity_retry_at") or 0):
                    a["waiting_on"] = "the fresh Mac plan cannot fit the job"
                    return
                if self.heavy_holder(state) is not None:
                    a["waiting_on"] = "the machine"
                    return
                if not a.get("preparing"):
                    a["preparing"] = True
                    actions.append(
                        lambda: self.spawn(f"prepare:{a['id']}", "prepare", self.prepare_attempt, a["id"], dict(a), True, self.growth_reserve_map(state))
                    )
                return
        elif owner.startswith("service:") and a["plan_env"] is None:
            a["plan_env"] = dict(state["launcher"]["catalog"].get("limits", {}))
        elif owner.startswith("stack:") and a["plan_env"] is None:
            a["plan_env"] = dict(state["launcher"][owner.partition(":")[2]].get("settings", {}))
        if not a.get("prepared") and owner != DIAGNOSTIC_OWNER and not owner.startswith(("service:", "stack:")):
            if not a.get("preparing"):
                a["preparing"] = True
                actions.append(lambda: self.spawn(f"prepare:{a['id']}", "prepare", self.prepare_attempt, a["id"], dict(a), False, {}))
            return
        if a["template"] == "real_dbt" and owner.startswith("run:") and build_provenance.is_mutating(a["args"]) and not a.get("cleared"):
            building = state["runs"][owner.partition(":")[2]]
            try:
                build_provenance.clear_build_artifacts(building)
            except build_provenance.ProvenanceError as error:
                self.finish_attempt(state, a, "create_failed", f"provenance: {error}")
                return
            a["cleared"] = True
            building["build_state"] = "open"
            for key in ("completion", "seal_error", "build_attempt", "build_binding"):
                building.pop(key, None)
        if "execution" in a and a["execution"]["live_try"] is None:
            if self.ecs_runtime is None:
                return
            decision = ecs_target.Decision(**a["placement_decision"])
            size = self.ecs_runtime.size(a)
            if int((a.get("plan_env") or {}).get("JOB_MEMORY_LIMIT", 0)) < size.memory_mib * 1024**2:
                a["waiting_on"] = "the fresh Mac plan cannot fit the job"
                a["capacity_retry_at"] = time.time() + self.CAPACITY_RETRY
                if a.get("attempt_alloc"):
                    state["allocations"][a["attempt_alloc"]]["state"] = "released"
                    a.update(attempt_alloc=None, plan_env=None, prepared=False)
                return
            ecs_target.reserve(a, size, decision, time.time())
        self.start_create(state, a, actions)  # False: the image's defaults are being read; the attempt stays queued

    def advance_target(self, state: dict[str, Any], attempt: dict[str, Any], actions: list[Callable[[], None]]) -> bool:
        live_try = attempt["execution"]["live_try"]
        if live_try is not None:
            live = next(item for item in attempt["execution"]["placements"] if item["try_id"] == live_try)
            return live["target"] != "docker"
        if self.ecs_runtime is None:
            attempt["waiting_on"] = "ECS runtime is not integrated"
            return True
        now = time.time()
        owner = str(attempt["owner"]).partition(":")[2]
        run = state["runs"].get(owner, {})
        job_class = pipeline_render.job_class(attempt["template"], attempt["params"], str(run.get("mode", "real")))
        if not self.catalog_epoch_matches(state, attempt):
            self.finish_attempt(state, attempt, "create_failed", "catalog authority changed or is frozen")
            return True
        size = self.ecs_runtime.size(attempt)
        usage = ecs_target.active_usage(state["attempts"].values())
        evidence = self.ecs_runtime.availability(attempt, now)
        view = replace(
            evidence,
            enabled=self.ecs_ready(),
            usage=usage,
            local_available=self.docker_ok and self.status == "serving",
            local_free=self.heavy_holder(state, attempt.get("attempt_alloc")) is None,
        )
        decision = ecs_target.select(attempt, job_class, size, view, now)
        attempt["waiting_on"] = decision.reason
        if decision.action in ("expire", "refuse"):
            self.finish_attempt(state, attempt, "timeout" if decision.action == "expire" else "create_failed", decision.reason)
        elif decision.target == "ecs":
            if attempt.get("preparing"):
                return True
            if attempt.get("attempt_alloc"):
                state["allocations"][attempt["attempt_alloc"]]["state"] = "released"
                attempt.update(attempt_alloc=None, plan_env=None, prepared=False)
            placement = ecs_target.reserve(attempt, size, decision, now, usage)
            attempt_id, try_id = str(attempt["id"]), str(placement["try_id"])
            actions.append(lambda: self.dispatch_ecs(attempt_id, try_id))
        elif decision.target == "docker":
            attempt["placement_decision"] = {"action": decision.action, "target": decision.target, "reason": decision.reason, "provider": decision.provider}
            return False
        return True

    @staticmethod
    def catalog_epoch_matches(state: Mapping[str, Any], attempt: Mapping[str, Any]) -> bool:
        epoch = attempt["execution"]["catalog_epoch"]
        authority = state["launcher"].get("catalog_authority") or {}
        return epoch is None or authority.get("epoch") == epoch and authority.get("frozen") is False and authority.get("target") == "aws"

    def dispatch_ecs(self, attempt_id: str, try_id: str) -> None:
        with self.tx() as state:
            attempt = state["attempts"][attempt_id]
            if attempt["execution"]["live_try"] != try_id:
                return
            placement = next(item for item in attempt["execution"]["placements"] if item["try_id"] == try_id)
            if placement["state"] != "intent":
                return
            if (
                attempt.get("cancel_intent")
                or time.time() >= attempt["deadline_utc"]
                or not self.admissible(state, attempt)
                or not self.catalog_epoch_matches(state, attempt)
            ):
                ecs_target.fenced(attempt, try_id, time.time(), no_start=True)
                self.finish_attempt(state, attempt, "fenced", "dispatch authority ended before submission")
                return
            placement["state"] = "unknown"
            saved_attempt, saved_placement = json.loads(canonical(attempt)), dict(placement)
        if self.ecs_runtime is not None:
            try:
                self.ecs_runtime.dispatch(saved_attempt, saved_placement)
            except (ecs_target.PlacementError, OSError):
                with self.tx() as state:
                    attempt = state["attempts"][attempt_id]
                    if attempt["execution"]["live_try"] == try_id:
                        placement = next(item for item in attempt["execution"]["placements"] if item["try_id"] == try_id)
                        placement["state"] = "unknown"
                        attempt["waiting_on"] = "cloud dispatch requires reconciliation"

    CAPACITY_VALID = 10.0
    CAPACITY_RETRY = 2.0

    def capacity_cleared(self, state: dict[str, Any], a: dict[str, Any], allocation: Mapping[str, Any], actions: list[Callable[[], None]]) -> bool:
        """The fresh capacity predicate of a pooled launch (the former pool's admission): one more share must fit in what is free now.

        ``free + own use - running * share >= share``, or no worker runs and ``free >= share``, from a reading taken for this launch. One
        attempt of an allocation is decided at a time, so two launches never count the same free memory.
        """
        now = time.time()
        if a.get("capacity_at") and now - float(a["capacity_at"]) < self.CAPACITY_VALID:
            return True
        owner = a["owner"]
        mine = [o for o in state["attempts"].values() if o["owner"] == owner]
        if a.get("capacity_pending") or now < float(a.get("capacity_retry_at") or 0):
            return False
        if any(o["id"] != a["id"] and o["phase"] == "queued" and (o.get("capacity_pending") or o.get("capacity_at")) for o in mine):
            return False
        a["capacity_pending"] = True
        running = sum(1 for o in mine if o["phase"] in ACTIVE_PHASES)
        share = int((allocation.get("environment") or {}).get("JOB_MEMORY_LIMIT", 0))
        names = [str(o["container_name"]) for o in mine if o["phase"] in ACTIVE_PHASES and o["container_name"]]
        growth = self.growth_reserve_map(state)
        actions.append(lambda: self.spawn(f"capacity:{a['id']}", "capacity", self.read_capacity, running, share, names, growth))
        return False

    def read_capacity(self, running: int, share: int, names: list[str], growth: dict[str, int]) -> dict[str, Any]:
        """Worker: what is free now, with the registered services' growth reserve held back."""
        reader = self.reader()
        try:
            budget = memory_budget.current(reader)
        except memory_budget.BudgetError as error:
            return {"ok": False, "error": str(error), "free": 0, "running": running}
        use = reader.usage_by_name()
        own = sum(use.get(name, 0) for name in names)
        free = budget.free - sum(max(0, int(limit) - use.get(name, 0)) for name, limit in growth.items())
        ok = free + own - running * share >= share or (running == 0 and free >= share)
        return {"ok": ok, "error": None, "free": free, "own": own, "running": running, "share": share}

    def on_capacity(self, key: str, outcome: str, value: Any) -> None:
        """A capacity reading: the launch may go on, waits for the next reading, or (no worker runs and nothing could be read) fails."""
        attempt_id = key.split(":", 1)[1]
        with self.tx() as state:
            a = state["attempts"].get(attempt_id)
            if a is None:
                return
            a["capacity_pending"] = False
            if outcome != "ok":
                a["capacity_retry_at"] = time.time() + self.CAPACITY_RETRY
                return
            if value["ok"]:
                a["capacity_at"] = time.time()
                a.pop("memory_wait_since", None)
            elif value["error"] and not value["running"]:
                self.finish_attempt(state, a, "create_failed", f"capacity: {value['error']}")
            else:
                now = time.time()
                since = a.setdefault("memory_wait_since", now)
                failure = memory_budget.wait_failure(since, now, self.timings.memory_wait, int(value["free"]), int(value.get("share", 0)))
                if failure and a["phase"] == "queued":
                    self.finish_attempt(state, a, "create_failed", failure)
                else:
                    a["capacity_retry_at"] = now + self.CAPACITY_RETRY
                    a["waiting_on"] = f"memory (free {value['free']}, share {value.get('share', 0)})"
        self.dirty.add(attempt_id)

    def growth_reserve_map(self, state: Mapping[str, Any]) -> dict[str, int]:
        """Registered services' limits keyed by container name: what their unused limits hold back from every plan (D1 section 6)."""
        return {f"hai-{self.registry_id[:8]}-svc-{service}": int(limit) for service, limit in state["services"].items()}

    def prepare_attempt(self, attempt_id: str, a: dict[str, Any], need_plan: bool, growth: dict[str, int]) -> dict[str, Any]:
        """Worker: the memory plan for an attempt allocation, and the check that the image carries the deadline entrypoint (D1 section 5 item 1).

        The plan holds back the registered services' unused limits, as the plan of a shared allocation does.
        """
        result: dict[str, Any] = {"plan": None, "image_error": None}
        if need_plan:
            plan = memory_budget.launch_plan(self.reader(), growth)
            result["plan"] = {"environment": plan.environment(), "record": plan.record()}
        template = TEMPLATES[a["template"]]
        if template.finite and template.service is not None:
            renderer = self.renderer_for(Path(a["checkout"]))
            image = renderer.image(template.service)
            info = self.docker.image(image)
            if info is None:
                result["image_error"] = f"image {image} is not present"
            elif ((info.get("Config") or {}).get("Labels") or {}).get(DEFAULT_IMAGE_LABEL) != "1" or ((info.get("Config") or {}).get("Labels") or {}).get(
                DEFAULT_IMAGE_IMPLEMENTATION
            ) != "go":
                result["image_error"] = f"image {image} does not carry the Go deadline entrypoint ({DEFAULT_IMAGE_LABEL})"
        return result

    def on_prepare(self, key: str, outcome: str, value: Any) -> None:
        """Worker message for ``prepare_attempt``."""
        attempt_id = key.split(":", 1)[1]
        with self.tx() as state:
            a = state["attempts"].get(attempt_id)
            if a is None or a["phase"] != "queued":
                return
            a["preparing"] = False
            if outcome == "docker" and value.status == 0:
                return
            if outcome != "ok":
                self.finish_attempt(state, a, "create_failed", f"prepare: {failure_text(value)}")
                return
            if value["image_error"]:
                self.finish_attempt(state, a, "create_failed", f"image_without_entrypoint: {value['image_error']}")
                return
            if value["plan"] is not None:
                if self.heavy_holder(state) is not None:
                    return
                allocation_id = new_id("al")
                state["allocations"][allocation_id] = {
                    "id": allocation_id,
                    "kind": "attempt",
                    "attempt": attempt_id,
                    "state": "held",
                    "generation": 1,
                    "workers": 1,
                    "holder_pid": os.getpid(),
                    "holder_start": run_lock.process_start(os.getpid()) or "",
                    "environment": value["plan"]["environment"],
                    "record": value["plan"]["record"],
                    "created_at": utc_now(),
                }
                a["attempt_alloc"] = allocation_id
                a["plan_env"] = value["plan"]["environment"]
            a["prepared"] = True
        self.dirty.add(attempt_id)

    def reader(self) -> memory_budget.DockerReader:
        """The Docker readings the memory plan needs, taken through the launcher's own connection."""
        return memory_budget.DockerReader(
            total_memory=lambda: int(self.docker.info()["MemTotal"]), cpu_count=lambda: int(self.docker.info()["NCPU"]), usage_by_name=self.docker.stats
        )

    def heavy_holder(self, state: Mapping[str, Any], exclude: str | None = None) -> str | None:
        """The ID of an allocation that holds the machine, if any (one heavy allocation at a time, D1 section 6)."""
        for allocation_id, allocation in state["allocations"].items():
            if allocation_id != exclude and allocation["state"] in ("planning", "held", "releasing"):
                return str(allocation_id)
        return None

    def advance_creating(self, state: dict[str, Any], a: dict[str, Any]) -> None:
        """An attempt in ``creating`` waits for its result or its container; nothing here dispatches (D1 section 4 item 8)."""
        op_id = a.get("current_op")
        op = state["operations"].get(op_id) if op_id else None
        if op is None or op["state"] != "open":
            return

    def advance_created(self, state: dict[str, Any], a: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """A created container is started only when the owner is still admissible; otherwise it is undone through cleanup."""
        if a.get("never_start") or a["cancel_intent"] or a["timeout_intent"] or not self.admissible(state, a):
            if not (a["cancel_intent"] or a["timeout_intent"]) and not a.get("never_start"):
                a["cancel_intent"] = {"reason": "fenced", "at": utc_now()}
            self.begin_exit(a)
            return
        if not self.docker_ok or self.status != "serving":
            return
        if finite_attempt(a) and time.time() >= a["deadline_utc"] - SKEW_SECONDS - START_MARGIN_SECONDS:
            a["timeout_intent"] = {"reason": "start_after_deadline", "at": utc_now()}
            self.begin_exit(a)
            return
        op_id = self.record_op(state, a, "start", "starting")
        a["phase"] = "starting"
        a["current_op"] = op_id
        container_id, joins = a["container_id"], dict(a.get("joins") or {})
        actions.append(lambda: self.spawn(f"op:{op_id}", "start", self.docker_start, container_id, joins))

    def start_revoked(self, state: Mapping[str, Any], a: dict[str, Any], dispatch: bool = True) -> bool:
        """True when a positive start step is no longer allowed (D1 sections 4 and 5).

        The causes are a cancel or timeout intent, a fenced or inadmissible owner and, for a dispatch, the host start cutoff.

        Records the intent that explains the revocation, so the end reason stays truthful.
        """
        if a["cancel_intent"] or a["timeout_intent"] or a.get("never_start"):
            return True
        if not self.admissible(state, a):
            a["cancel_intent"] = {"reason": "fenced", "at": utc_now()}
            return True
        if dispatch and finite_attempt(a) and time.time() >= a["deadline_utc"] - SKEW_SECONDS - START_MARGIN_SECONDS:
            a["timeout_intent"] = {"reason": "start_after_deadline", "at": utc_now()}
            return True
        return False

    def advance_starting(self, state: dict[str, Any], a: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """An unfinished ``start`` operation is settled by what the container shows; a start is dispatched again only while it is still allowed.

        A revoked attempt (cancel, timeout, fencing, recovery, an owner that is no longer held or the start cutoff) never
        issues another start: the container is undone through cleanup authority, or stopped first when it already runs.
        """
        op = state["operations"].get(a["current_op"])
        container_state = self.container_state(a)
        revoked = self.start_revoked(state, a, dispatch=container_state != "running")
        if container_state == "running":
            self.settle_op(state, a["current_op"], "reconciled")
            a["phase"] = "started"  # a revoked running container is stopped by advance_started under cleanup authority
            return
        if revoked and op is not None and f"op:{op['op_id']}" in self.inflight and container_state not in ("exited", "absent"):
            return  # the start request is in flight: its result settles it (on_start undoes a start that arrives after a revocation)
        if container_state in ("exited", "absent") or revoked:
            self.settle_op(state, a["current_op"], "undone" if revoked and container_state not in ("exited", "absent") else "reconciled")
            self.begin_exit(a)
            return
        if op is not None and op["state"] == "open" and f"op:{op['op_id']}" not in self.inflight and self.docker_ok and self.status == "serving":
            container_id, joins = a["container_id"], dict(a.get("joins") or {})
            actions.append(lambda: self.spawn(f"op:{op['op_id']}", "start", self.docker_start, container_id, joins))

    def advance_started(self, state: dict[str, Any], a: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """A started container is stopped by cancel, timeout or fencing (cleanup authority); an exit moves it to ``exiting``."""
        container_state = self.container_state(a)
        if container_state in ("exited", "absent"):
            self.begin_exit(a)
            return
        if finite_attempt(a) and time.time() > a["deadline_utc"] and not a["timeout_intent"]:
            a["timeout_intent"] = {"reason": "deadline", "at": utc_now()}
        stop_needed = (
            a["cancel_intent"]
            or a["timeout_intent"]
            or (a["owner"].startswith(("run:", "alloc:")) and not self.admissible(state, a) and not state["launcher"].get("restart"))
        )
        if stop_needed and not a["cancel_intent"] and not a["timeout_intent"]:
            a["cancel_intent"] = {"reason": "fenced", "at": utc_now()}
        if (a["cancel_intent"] or a["timeout_intent"]) and self.docker_ok:
            self.ensure_stop(state, a, actions)

    def ensure_stop(self, state: dict[str, Any], a: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """Record a ``stop`` operation if none is open, and dispatch it if it is not in flight."""
        op = self.open_op(state, a["id"], "stop")
        op_id = self.record_op(state, a, "stop", a["phase"]) if op is None else op["op_id"]
        container_id = a["container_id"]
        if f"op:{op_id}" not in self.inflight and container_id:
            grace = self.timings.stop_grace
            actions.append(lambda: self.spawn(f"op:{op_id}", "stop", self.docker_stop, container_id, grace))

    def docker_stop(self, container_id: str, grace: int) -> dict[str, Any] | None:
        """Worker: stop a container, then read it back."""
        self.docker.stop(container_id, grace)
        return self.docker.inspect(container_id)

    def begin_exit(self, a: dict[str, Any]) -> None:
        """Move an attempt to ``exiting``: the cleanup of D1 section 4 item 6 runs from the recorded step."""
        a["phase"] = "exiting"
        a["cleanup"] = a.get("cleanup") or {"step": "collect"}

    def advance_exiting(self, state: dict[str, Any], a: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """Cleanup: save the exit code and last log lines, remove the container, confirm by name that it is gone, then end."""
        cleanup = a["cleanup"] = a.get("cleanup") or {"step": "collect"}
        container_id = a["container_id"]
        if container_id is None:
            self.finish_attempt(state, a, self.intent_reason(a, "create_failed"))
            return
        if not self.docker_ok:
            return
        step = cleanup["step"]
        if step == "collect":
            if self.container_state(a) == "absent":
                cleanup["step"] = "remove"
                return
            folder = self.attempt_folder(a)
            secrets = self.attempt_secrets(state, a)
            actions.append(lambda: self.spawn(f"collect:{a['id']}", "collect", self.collect, container_id, folder, dict(a), secrets))
        elif step == "remove":
            op = self.open_op(state, a["id"], "remove")
            op_id = op["op_id"] if op else self.record_op(state, a, "remove", "exiting")
            actions.append(lambda: self.spawn(f"op:{op_id}", "remove", self.docker_remove, container_id))
        elif step == "confirm":
            name = a["container_name"]
            actions.append(lambda: self.spawn(f"confirm:{a['id']}", "confirm", self.confirm_gone, name))

    @staticmethod
    def attempt_secrets(state: Mapping[str, Any], attempt: Mapping[str, Any]) -> SecretSources:
        """The secret environment names, command positions and positions inside longer values that the attempt's create record holds; they survive a
        launcher restart in the registry."""
        for op in state["operations"].values():
            if op["attempt"] == attempt["id"] and op["kind"] == "create":
                return SecretSources(list(op.get("secret_env") or []), list(op.get("secret_cmd") or []), list(op.get("secret_spans") or []))
        return NO_SECRET_SOURCES

    def collect(self, container_id: str, folder: Path, attempt: dict[str, Any] | None = None, secrets: SecretSources = NO_SECRET_SOURCES) -> dict[str, Any]:
        """Worker: read the exit code and the last log lines and save them in the attempt's folder with the container's credentials redacted.

        The credentials come from the container's own configuration, so redaction holds after a launcher restart; when the configuration cannot be read
        nothing is saved and the step is retried."""
        info = self.docker.inspect(container_id)
        exit_code = None if info is None else int((info.get("State") or {}).get("ExitCode", 0))
        out, err = ("", "") if info is None else self.docker.logs_split(container_id, None)
        held = container_secrets(info, secrets, self.key)
        out, err = redact(out, held), redact(err, held)
        folder.mkdir(parents=True, exist_ok=True)
        # The whole of standard output (capped) is kept for result-bearing templates; the last lines are the human-readable tail.
        (folder / "stdout.txt").write_text(out[-CAPTURE_LIMIT:])
        (folder / "stderr.txt").write_text(err[-CAPTURE_LIMIT:])
        (folder / "log_tail.txt").write_text("".join((out + err).splitlines(keepends=True)[-200:]))
        (folder / "exit.json").write_text(json.dumps({"exit_code": exit_code, "at": utc_now()}) + "\n")
        result: dict[str, Any] = {"exit_code": exit_code, "log": str(folder / "log_tail.txt")}
        if attempt and attempt.get("capture_dbt_artifacts"):
            try:
                if attempt["template"] == "fixture_dbt":
                    source = Path(attempt["params"]["case_host"]).resolve() / "target"
                else:
                    source = self.cfg.runs_root.resolve() / attempt["owner"].partition(":")[2] / "target"
                result["dbt_artifacts"] = dbt_lineage.capture(source, folder / "dbt_artifacts", attempt["id"])
            except (dbt_lineage.DbtLineageError, OSError):
                result["dbt_artifact_error"] = dbt_lineage.CAPTURE_FAILED
        return result

    def docker_remove(self, container_id: str) -> None:
        """Worker: remove a container."""
        self.docker.remove(container_id)

    def confirm_gone(self, name: str) -> bool:
        """Worker: True when no container of that name exists."""
        return not any(str(c["Names"][0]).lstrip("/") == name for c in self.docker.containers(label=f"{LABEL_REGISTRY}={self.registry_id}"))

    def finish_attempt(self, state: dict[str, Any], a: dict[str, Any], reason: str, detail: str = "") -> None:
        """End an attempt: record the reason, release its attempt allocation and guards, update catalog service records."""
        execution = a.get("execution")
        if execution and execution["live_try"]:
            live = next(item for item in execution["placements"] if item["try_id"] == execution["live_try"])
            if live["target"] == "ecs":
                raise ecs_target.PlacementError("an unconfirmed cloud task cannot end its attempt")
            live["state"] = "complete"
            execution["live_try"] = None
        a.update({"phase": "ended", "reason": reason, "ended_at": utc_now(), "detail": detail})
        self.secret_results.pop(a["id"], None)
        if a.get("attempt_alloc"):
            allocation = state["allocations"].get(a["attempt_alloc"])
            if allocation is not None:
                allocation["state"] = "released"
        for guard in state["guards"].values():
            if guard.get("writer") == a["id"]:
                guard["writer"] = None
            guard["readers"].pop(a["id"], None)
        if a["container_id"]:
            self.bound.pop(a["container_id"], None)
            state["containers"].setdefault(a["container_id"], {})["ended"] = True
        for op in state["operations"].values():
            if op["attempt"] == a["id"] and op["state"] == "open" and op["kind"] in ("create", "start", "stop", "remove"):
                op.update({"state": "settled", "result": "attempt_ended", "settled_at": utc_now()})
        if a["owner"] == DIAGNOSTIC_OWNER and self.probe is not None and self.probe.get("attempt") == a["id"] and self.stream_acked:
            self.probe = None
        self.changed = True

    # -- messages from workers -------------------------------------------------------------------------------------

    def on_create(self, key: str, outcome: str, value: Any) -> None:
        """A create reply: a container ID to adopt, or a definite refusal by the daemon (an unknown outcome stays open)."""
        op_id = key.split(":", 1)[1]
        if outcome == "ok":
            self.consider(str(value))
            return
        if outcome == "docker" and value.status == 0:
            return
        with self.tx() as state:
            op = state["operations"].get(op_id)
            if op is None or op["state"] != "open":
                return
            a = state["attempts"].get(op["attempt"])
            self.settle_op(state, op_id, f"refused: {failure_text(value)}")
            if a is not None and is_open(a):
                self.finish_attempt(state, a, "create_failed", failure_text(value))

    def on_start(self, key: str, outcome: str, value: Any) -> None:
        """A start reply: the attempt is ``started`` only if it still expects it, otherwise the start is undone (D1 section 4 item 2)."""
        op_id = key.split(":", 1)[1]
        if outcome == "docker" and value.status == 0:
            return
        with self.tx() as state:
            op = state["operations"].get(op_id)
            if op is None or op["state"] != "open":
                return
            a = state["attempts"].get(op["attempt"])
            if a is None:
                return
            if outcome != "ok":
                self.settle_op(state, op_id, f"failed: {failure_text(value)}")
                if a["phase"] == "starting":
                    a["start_error"] = failure_text(value)
                    self.begin_exit(a)
                return
            self.settle_op(state, op_id, "started")
            if a["phase"] != "starting":
                return
            if a["cancel_intent"] or a["timeout_intent"] or not self.admissible(state, a):
                if not (a["cancel_intent"] or a["timeout_intent"]):
                    a["cancel_intent"] = {"reason": "fenced", "at": utc_now()}
                a["phase"] = "started"
                self.dirty.add(a["id"])
                return
            a["phase"] = "started"
            self.inventory.setdefault(a["container_id"], {})["state"] = "running"
        self.dirty.add(op["attempt"])

    def on_stop(self, key: str, outcome: str, value: Any) -> None:
        """A stop reply: the container is read back; its exit moves the attempt to cleanup."""
        op_id = key.split(":", 1)[1]
        if outcome == "docker" and value.status == 0:
            return
        with self.tx() as state:
            op = state["operations"].get(op_id)
            if op is None or op["state"] != "open":
                return
            self.settle_op(state, op_id, "stopped" if outcome == "ok" else f"failed: {failure_text(value)}")
            a = state["attempts"].get(op["attempt"])
            if a is not None and a["container_id"]:
                entry = self.inventory.setdefault(a["container_id"], {})
                if outcome == "ok" and value is None:
                    entry["state"] = "absent"
                elif outcome == "ok":
                    entry["state"] = "running" if value["State"].get("Running") else "exited"
                    entry["exit"] = value["State"].get("ExitCode")
                self.dirty.add(a["id"])

    def on_collect(self, key: str, outcome: str, value: Any) -> None:
        """Exit record saved: remove the container next."""
        attempt_id = key.split(":", 1)[1]
        if outcome != "ok":
            return
        with self.tx() as state:
            a = state["attempts"].get(attempt_id)
            if a is not None and a["phase"] == "exiting":
                a["exit_code"] = value["exit_code"]
                a["log_tail"] = value["log"]
                for field in ("dbt_artifacts", "dbt_artifact_error"):
                    if field in value:
                        a[field] = value[field]
                a["cleanup"]["step"] = "remove"
        self.dirty.add(attempt_id)

    def on_remove(self, key: str, outcome: str, value: Any) -> None:
        """Removal reply: confirm by name next; a failed removal keeps ``exiting`` and is retried (D1 section 4 item 6)."""
        op_id = key.split(":", 1)[1]
        if outcome == "docker" and value.status == 0:
            return
        with self.tx() as state:
            op = state["operations"].get(op_id)
            if op is None or op["state"] != "open":
                return
            a = state["attempts"].get(op["attempt"])
            if outcome != "ok":
                op["last_error"] = failure_text(value)
                op["retries"] = int(op.get("retries", 0)) + 1
                return
            self.settle_op(state, op_id, "removed")
            if a is not None and a["phase"] == "exiting":
                a["cleanup"]["step"] = "confirm"
                self.inventory.setdefault(a["container_id"], {})["state"] = "absent"
        if op is not None:
            self.dirty.add(op["attempt"])

    def on_confirm(self, key: str, outcome: str, value: Any) -> None:
        """Gone by name: record ``ended`` with its reason."""
        attempt_id = key.split(":", 1)[1]
        if outcome != "ok":
            return
        with self.tx() as state:
            a = state["attempts"].get(attempt_id)
            if a is None or a["phase"] != "exiting":
                return
            if value:
                self.finish_attempt(state, a, self.intent_reason(a), str(a.get("start_error", "")))
                if (
                    a["reason"] == "exited"
                    and a.get("exit_code") == 0
                    and (
                        (a["template"] == "spark_read" and a["args"][:1] == ["pin"])
                        or (a["template"] == "pipeline_spark" and a["params"].get("job") == "pin_bronze")
                    )
                ):
                    self.record_pins(state, a)
                if a["reason"] == "exited" and a.get("exit_code") == 0 and a["template"] == "real_dbt" and build_provenance.is_full_build(a["args"]):
                    self.begin_seal(state, a)
            else:
                a["cleanup"]["step"] = "remove"
        self.dirty.add(attempt_id)

    def begin_seal(self, state: dict[str, Any], a: dict[str, Any]) -> None:
        """A full build ended with exit code 0: the run is ``sealing`` until ``seal`` has decided (D1 section 10 item 5).

        The run records which attempt wrote the build, in which generation and with which arguments; ``seal`` certifies that attempt only. The seal is an
        operation recorded with the run (``seal``: ID, attempt, generation, binding): it is open work of the run until its worker has returned, and its
        result is published only while the run still is what the record says (``seal_stale``).
        """
        run = state["runs"][a["owner"].partition(":")[2]]
        run["build_state"] = "sealing"
        run["build_attempt"] = a["id"]
        run["build_binding"] = build_provenance.binding(a)
        run["seal"] = {"id": new_id("sl"), "attempt": a["id"], "generation": int(run.get("generation", 1)), "binding": run["build_binding"]}
        self.spawn(f"seal:{run['run_id']}:{run['seal']['id']}", "seal", self.seal_worker, dict(run))

    def seal_worker(self, run: Mapping[str, Any]) -> dict[str, Any]:
        """Worker: the frozen ``seal`` interface; a refusal comes back as an error. A test registry's ``seal_hold`` file holds it (two minutes at most)."""
        hold = self.registry.path / "seal_hold"
        deadline = time.monotonic() + 120
        while self.registry.kind == "test" and hold.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        return seal(run)

    @staticmethod
    def seal_stale(run: Mapping[str, Any], record: Any) -> str | None:
        """None while the run still is what its seal record says; else why the seal no longer applies (D1 section 10 items 4 and 5, review delta 1 finding 8).

        The run must still be sealing, held or finalizing, in the generation the seal began in, with the same build attempt and binding.
        """
        if not isinstance(record, Mapping):
            return "the run holds no record of the seal"
        if run.get("build_state") != "sealing":
            return f"the run is {run.get('build_state')}, not sealing"
        if run.get("state") not in ("held", "finalizing"):
            return f"the run is {run.get('state')}"
        if int(run.get("generation", 1)) != record.get("generation"):
            return f"the run is in generation {run.get('generation', 1)}; the seal began in generation {record.get('generation')}"
        if run.get("build_attempt") != record.get("attempt") or run.get("build_binding") != record.get("binding"):
            return "the run names another build than the seal"
        return None

    @staticmethod
    def discard_seal(run: dict[str, Any], reason: str) -> None:
        """Close a seal that no longer applies: no completion, an open build and the reason."""
        run.pop("seal", None)
        run.pop("completion", None)
        run.update({"build_state": "open", "seal_error": f"the seal was discarded: {reason}"})

    def on_seal(self, key: str, outcome: str, value: Any) -> None:
        """The seal worker returned. In one registry transaction the seal is closed and its result is published only if ``seal_stale`` finds the run as the
        record says; otherwise the older seal publishes nothing (a completion record and the ``sealed`` state, or the reason and an open build)."""
        _, run_id, seal_id = key.split(":", 2)
        with self.tx() as state:
            run = state["runs"].get(run_id)
            record = run.get("seal") if run is not None else None
            if run is None or not isinstance(record, Mapping) or record.get("id") != seal_id:
                return
            reason = self.seal_stale(run, record)
            if reason is None and outcome == "ok" and (value.get("build_attempt"), value.get("build_generation")) != (record["attempt"], record["generation"]):
                reason = "the sealed build is not the one the seal began with"
            if reason is not None:
                self.discard_seal(run, reason)
                return
            run.pop("seal")
            if outcome == "ok":
                run.update({"completion": value, "build_state": "sealed"})
                run.pop("seal_error", None)
            else:
                run.update({"build_state": "open", "seal_error": failure_text(value)})
                run.pop("completion", None)

    @staticmethod
    def sealed_reader_vars(state: Mapping[str, Any], run_id: str) -> dict[str, str]:
        """The sealed digest and expected marker of a run's sealed copy for a reader behind the consumer entrypoint; the launcher reads them from its own
        record, a caller supplies only the run ID."""
        run = state["runs"].get(run_id)
        if run is None:
            raise LauncherError(f"run {run_id} does not exist", "provenance")
        return consumer_vars(run)

    def advance_seals(self, state: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """A seal that no worker of this launcher is running (the launcher died) is resumed while it still applies and discarded when it does not; the step
        is idempotent."""
        for run_id, run in state["runs"].items():
            record = run.get("seal")
            if record is None and run.get("build_state") != "sealing":
                continue
            key = f"seal:{run_id}:{record.get('id') if isinstance(record, Mapping) else ''}"
            if key in self.inflight:
                continue
            reason = self.seal_stale(run, record)
            if reason is not None:
                self.discard_seal(run, reason)
            else:
                actions.append(partial(self.spawn, key, "seal", self.seal_worker, dict(run)))

    def safe_read(self, run_id: str, name: str, limit: int = 1 << 20) -> bytes:
        """Read a run file component by component from the runs root with ``O_NOFOLLOW`` along ``runs/<run_id>/<name>`` (D1 section 10 item 2)."""
        if not RUN_ID.fullmatch(run_id) or "/" in name or name in ("", ".", ".."):
            raise LauncherError("not a plain run file", "refused")
        flags = os.O_RDONLY | os.O_NOFOLLOW
        descriptor = os.open(self.cfg.runs_root, flags | os.O_DIRECTORY)
        try:
            run_descriptor = os.open(run_id, flags | os.O_DIRECTORY, dir_fd=descriptor)
        finally:
            os.close(descriptor)
        try:
            file_descriptor = os.open(name, flags, dir_fd=run_descriptor)
        finally:
            os.close(run_descriptor)
        try:
            if not stat.S_ISREG(os.fstat(file_descriptor).st_mode):
                raise LauncherError(f"{name} is not a regular file", "refused")
            return os.read(file_descriptor, limit)
        finally:
            os.close(file_descriptor)

    def record_pins(self, state: dict[str, Any], a: dict[str, Any]) -> None:
        """Read ``pins.json`` safely, check its schema and table set and record its digest only if it equals the writer's (D1 section 10 item 3)."""
        run_id = a["owner"].partition(":")[2]
        run = state["runs"][run_id]
        problem = ""
        try:
            data = self.safe_read(run_id, "pins.json")
            written = json.loads((Path(a["log_tail"]).parent / "stdout.txt").read_text().strip().splitlines()[-1])["sha256"]
            digest = hashlib.sha256(data).hexdigest()
            document = json.loads(data)
            expected = set(pins.mapped_tables(Path(a["checkout"]) / pins.TABLE_MAP))
            if digest != written:
                problem = "the pins file differs from the digest its writer printed"
            elif not isinstance(document, dict) or set(document) != expected or not all(isinstance(v, str) and v.isdigit() for v in document.values()):
                problem = "the pins file does not hold the mapped table set with snapshot IDs"
        except (OSError, ValueError, KeyError, IndexError, LauncherError, pins.PinError) as error:
            problem = f"pins could not be read: {type(error).__name__}"
        if problem:
            run["pins_error"] = problem
            return
        run.pop("pins_error", None)
        run["pins_sha256"] = digest
        run["pins_attempt"] = a["id"]

    def pins_unchanged(self, run_id: str) -> bool:
        """True while ``pins.json`` still has the recorded digest (checked again before the file is used)."""
        with self.tx(write=False) as state:
            recorded = state["runs"][run_id].get("pins_sha256")
        try:
            return bool(recorded) and hashlib.sha256(self.safe_read(run_id, "pins.json")).hexdigest() == recorded
        except (OSError, LauncherError):
            return False

    def on_inspect(self, key: str, outcome: str, value: Any) -> None:
        """An inspected container: adopt it against an open create operation or dispose of it (D1 section 4 items 3 and 7)."""
        container_id = key.split(":", 1)[1]
        if outcome != "ok":
            return
        if value is None:
            entry = self.inventory.get(container_id)
            if entry is not None:
                entry["state"] = "absent"
            if container_id in self.bound:
                self.dirty.add(self.bound[container_id])
            return
        info = value
        config = info.get("Config") or {}
        labels = config.get("Labels") or {}
        if labels.get(LABEL_REGISTRY) != self.registry_id:
            return
        name = str(info.get("Name", "")).lstrip("/")
        running = bool((info.get("State") or {}).get("Running"))
        raw = str((info.get("State") or {}).get("Status", "created"))
        status = "running" if running else ("exited" if raw in ("exited", "dead") else "created")
        self.inventory[container_id] = {"state": status, "name": name, "labels": dict(labels), "exit": (info.get("State") or {}).get("ExitCode")}
        if container_id in self.bound:
            self.dirty.add(self.bound[container_id])
            return
        actions: list[Callable[[], None]] = []
        with self.tx() as state:
            op = state["operations"].get(labels.get(LABEL_OP, ""))
            a = state["attempts"].get(op["attempt"]) if op else None
            matches = op is not None and a is not None and op["kind"] == "create" and op["state"] == "open" and a["phase"] == "creating"
            matches = matches and op is not None and a is not None and a["container_name"] == name and self.labels_match(a, op, labels)
            orphaned = [o for o in state["operations"].values() if o["kind"] == "orphan" and o.get("container_id") == container_id]
            if matches and op is not None and a is not None:
                self.adopt(state, a, op, container_id, info)
            elif not orphaned:
                # An orphan raises more events (stop, die) while and after it is removed, and an inspect reply can arrive after the removal
                # settled; container IDs are never reused, so a container with any orphan operation is disposed of and recorded once.
                self.dispose_orphan(state, container_id, name, labels, actions)
            elif all(o["state"] == "settled" for o in orphaned):
                # A late reply describes a container already removed; it does not return to the inventory.
                self.inventory.pop(container_id, None)
        for action in actions:
            action()
        self.dirty.update(self.bound.values())

    def labels_match(self, a: Mapping[str, Any], op: Mapping[str, Any], labels: Mapping[str, str]) -> bool:
        """The container carries exactly the label set the attempt's create specification set (image labels are not compared)."""
        expected = self.labels_for(a, op["op_id"], self.probe_labels(a))
        return {key: labels.get(key) for key in expected} == expected

    @staticmethod
    def probe_labels(a: Mapping[str, Any]) -> dict[str, str]:
        """The extra label of a probe attempt."""
        return {LABEL_PROBE: a["probe"]["nonce"]} if a.get("probe") else {}

    def adopt(self, state: dict[str, Any], a: dict[str, Any], op: dict[str, Any], container_id: str, info: Mapping[str, Any]) -> None:
        """Adopt a container: bind its immutable ID through one conditional transition (D1 section 4 item 3)."""
        spec = op["projection"]
        found = inspected_projection(info, spec, self.key, frozenset(op["secret_env"]), op["secret_cmd"])
        a["container_id"] = container_id
        self.bound[container_id] = a["id"]
        state["containers"][container_id] = {"attempt": a["id"], "name": a["container_name"], "created_at": utc_now()}
        if found != spec:
            self.settle_op(state, op["op_id"], "digest_mismatch")
            a["cancel_intent"] = a["cancel_intent"] or {"reason": "digest_mismatch", "at": utc_now()}
            differing = sorted(key for key in spec if spec[key] != found.get(key))
            shown = {key: [spec[key], found.get(key)] for key in differing if key not in ("env", "cmd")}
            a["detail"] = f"the container's inspected configuration differs from the recorded effective specification in {', '.join(differing)}"
            a["mismatch_detail"] = shown
            self.begin_exit(a)
            a["mismatch"] = True
            return
        self.settle_op(state, op["op_id"], "adopted")
        if a["cancel_intent"] or a["timeout_intent"] or not self.admissible(state, a) and a["owner"] != DIAGNOSTIC_OWNER:
            if not (a["cancel_intent"] or a["timeout_intent"]):
                a["cancel_intent"] = {"reason": "fenced", "at": utc_now()}
            self.begin_exit(a)
            return
        a["phase"] = "created"

    def dispose_orphan(self, state: dict[str, Any], container_id: str, name: str, labels: Mapping[str, str], actions: list[Callable[[], None]]) -> None:
        """A container that matches no open operation or open attempt is stopped and removed, and the action recorded (D1 section 4 item 7)."""
        op_id = new_id("op")
        state["operations"][op_id] = {
            "op_id": op_id,
            "attempt": None,
            "kind": "orphan",
            "expected_phase": None,
            "generation": int(labels.get(LABEL_GENERATION, 0) or 0),
            "dispatched_at": time.time(),
            "state": "open",
            "result": None,
            "container_id": container_id,
            "name": name,
        }
        state["launcher"].setdefault("orphans", []).append({"container": container_id[:12], "name": name, "at": utc_now()})
        actions.append(lambda: self.spawn(f"op:{op_id}", "orphan", self.docker_dispose, container_id))

    def docker_dispose(self, container_id: str) -> None:
        """Worker: stop and remove a container."""
        info = self.docker.inspect(container_id)
        if info is None:
            return
        if ((info.get("Config") or {}).get("Labels") or {}).get(LABEL_REGISTRY) != self.registry_id:
            raise LauncherError("refusing to dispose of a container without this registry's label", "ownership")
        self.docker.stop(container_id, 1)
        self.docker.remove(container_id)

    def on_orphan(self, key: str, outcome: str, value: Any) -> None:
        """An orphan removal finished."""
        op_id = key.split(":", 1)[1]
        if outcome == "docker" and value.status == 0:
            return
        with self.tx() as state:
            if outcome == "ok":
                self.settle_op(state, op_id, "removed")
                container_id = state["operations"][op_id].get("container_id")
                if container_id:
                    self.inventory.pop(container_id, None)
            else:
                op = state["operations"].get(op_id)
                if op is not None:
                    op["last_error"] = failure_text(value)

    def settle_unresolved_creates(self, scan_seq: int) -> None:
        """An unresolved create settles only when the bounded timeout has passed and a fresh scan shows no container by name (D1 section 4 item 8)."""
        started = self.scan_wall.get(scan_seq, 0.0)
        with self.tx() as state:
            for op in state["operations"].values():
                if op["kind"] != "create" or op["state"] != "open":
                    continue
                if started < op["dispatched_at"] + self.timings.create_timeout or op["name"] in self.names_seen:
                    continue
                if f"op:{op['op_id']}" in self.inflight:
                    continue
                a = state["attempts"].get(op["attempt"])
                self.settle_op(state, op["op_id"], "create_failed", evidence={"scan": scan_seq, "containers_listed": len(self.names_seen), "at": utc_now()})
                if a is not None and is_open(a):
                    self.finish_attempt(state, a, "create_failed", "no container appeared within the create timeout and a fresh scan found none")

    def registry_snapshot(self) -> tuple[tuple[int, str], bytes]:
        """One read of registry.json: its bytes with their size and SHA-256.

        Any outside write that changes a byte changes the signature, whatever it does to the inode or the times, and the signature describes exactly
        the bytes returned with it.
        """
        data = self.registry.state_file.read_bytes()
        return (len(data), hashlib.sha256(data).hexdigest()), data

    def read_open_operations(self, data: bytes) -> None:
        """Parse the registry snapshot ``data`` and keep what the timed work needs from its open operations."""
        state = run_lock.parse_state(data, self.registry_id, self.registry.state_file.name)
        if state["schema_version"] != run_lock.LAUNCHER_SCHEMA:
            raise run_lock.RegistryError(f"{self.registry.state_file.name} is not valid: launcher schema required")
        operations = state["operations"].values()
        open_ops = [op for op in operations if op["state"] == "open"]
        self.create_deadlines = [(op["op_id"], op["dispatched_at"] + self.timings.create_timeout) for op in open_ops if op["kind"] == "create"]
        self.orphan_retries = [(op["op_id"], op["container_id"]) for op in open_ops if op["kind"] == "orphan" and op.get("container_id")]

    def maintenance(self, now: float) -> None:
        """Timed work: scans for expired creates, orphan retries and the move of old records. The registry is parsed only when its file changed.

        Above ``compact_threshold`` bytes the move takes records older than ``compact_floor`` and is tried once per change of the file; the
        periodic pass every ``compact_every`` chooses the age by the current size, so records that age with no change still move.
        """
        if not self.docker_ok or self.status == "starting":
            return
        if now - self.last_maintenance < 0.5:
            return
        self.last_maintenance = now
        signature, snapshot = self.registry_snapshot()
        if signature != self.maintained:
            self.read_open_operations(snapshot)
            self.maintained = signature
        wall = time.time()
        expired = any(wall > deadline and f"op:{op_id}" not in self.inflight for op_id, deadline in self.create_deadlines)
        if expired and not any(k.startswith("scan:") for k in self.inflight):
            self.start_scan()
        for op_id, container_id in self.orphan_retries:
            if f"op:{op_id}" not in self.inflight:
                self.spawn(f"op:{op_id}", "orphan", self.docker_dispose, container_id)
        over = signature[0] > self.timings.compact_threshold
        if now - self.last_compact >= self.timings.compact_every:
            self.last_compact = now
            self.compacted_at = signature
            self.compact_history(self.timings.compact_floor if over else self.timings.history_retain)
        elif over and signature != self.compacted_at:
            self.compacted_at = signature
            self.compact_history(self.timings.compact_floor)

    def history_to_move(self, state: Mapping[str, Any], retain: float | None = None) -> list[tuple[str, str, Any]]:
        """The terminal records older than ``retain`` seconds (default ``history_retain``), as (collection, key, record) (failure modes RH1 to RH10 and RH20).

        An attempt moves only when it ended, none of its operations is open and its owner is a released or failed allocation that nothing live
        names: no later submit can then reuse its attempt number in a container name. Its settled operations and its container record move with
        it, unless the inventory still lists the container. An allocation that an attempt in the registry, a registration, a reader, a session or a
        reservation names stays (RH5 and RH20): the selection drops such an allocation, which keeps the attempts it owns, and repeats until nothing
        more is dropped. Done requests and reservation events move by age; open work never moves.
        """
        cutoff = time.time() - (self.timings.history_retain if retain is None else retain)

        def old(text: Any) -> bool:
            try:
                return datetime.fromisoformat(str(text)).timestamp() < cutoff
            except (TypeError, ValueError):
                return False

        live = json.dumps([state.get(name, {}) for name in ("registrations", "readers", "sessions", "reservations")])
        open_ops = {op["attempt"] for op in state["operations"].values() if op["state"] == "open"}
        by_owner: dict[str, list[dict[str, Any]]] = {}
        named_by: dict[str, set[str]] = {}
        for attempt in state["attempts"].values():
            by_owner.setdefault(str(attempt["owner"]), []).append(attempt)
            if attempt.get("attempt_alloc"):
                named_by.setdefault(str(attempt["attempt_alloc"]), set()).add(attempt["id"])
        chosen: dict[str, list[dict[str, Any]]] = {}
        for alloc_id, allocation in state["allocations"].items():
            owned = by_owner.get(f"alloc:{alloc_id}", [])
            if allocation["state"] not in ("released", "failed") or f'"{alloc_id}"' in live:
                continue
            if any(a["phase"] != "ended" or a["id"] in open_ops or not old(a.get("ended_at")) for a in owned) or not (
                owned or old(allocation.get("created_at"))
            ):
                continue
            chosen[alloc_id] = owned
        while True:
            moving = {a["id"] for owned in chosen.values() for a in owned}
            dropped = [alloc_id for alloc_id in chosen if named_by.get(alloc_id, set()) - moving]
            if not dropped:
                break
            for alloc_id in dropped:
                del chosen[alloc_id]
        moved: list[tuple[str, str, Any]] = []
        attempts: set[str] = set()
        for alloc_id, owned in chosen.items():
            attempts.update(a["id"] for a in owned)
            moved += [("attempts", a["id"], a) for a in owned]
            moved.append(("allocations", alloc_id, state["allocations"][alloc_id]))
        moved += [("operations", key, op) for key, op in state["operations"].items() if op["state"] == "settled" and op.get("attempt") in attempts]
        for container_id, record in state["containers"].items():
            present = container_id in self.inventory and self.inventory[container_id].get("state") != "absent"
            if record.get("attempt") in attempts and not present:
                moved.append(("containers", container_id, record))
        moved += [("requests", key, record) for key, record in state["requests"].items() if record["state"] == "done" and old(record.get("at"))]
        moved += [
            ("reservation_history", f"{event.get('reservation_id')}@{event.get('at')}", event) for event in state["reservation_history"] if old(event.get("at"))
        ]
        return moved

    def compact_history(self, retain: float | None = None) -> None:
        """Move terminal records older than ``retain`` seconds out of the registry into its append-only archive, so a transaction rewrites less.

        The archive lines are written and flushed before the registry is rewritten: a crash between the two repeats lines, never loses a record.
        """
        with self.tx(write=False) as state:
            if not self.history_to_move(state, retain):
                return
        with self.tx() as state:
            moved = self.history_to_move(state, retain)
            if not moved:
                return
            self.archive_history(moved)
            for collection, key, _record in moved:
                if collection != "reservation_history":
                    state[collection].pop(key, None)
            gone = {id(record) for collection, _, record in moved if collection == "reservation_history"}
            state["reservation_history"] = [event for event in state["reservation_history"] if id(event) not in gone]

    def archive_history(self, moved: list[tuple[str, str, Any]]) -> None:
        """Append the moved records to ``history/<UTC date>.jsonl`` beside the registry (owner-only) and make them durable before the caller removes them.

        Every byte is written (a short write continues, an interrupted one repeats, a write that makes no progress fails), synced and read back; a
        failed append is cut back to the size before it. An incomplete last line left by a crash is cut off before the next batch is added, so a
        later batch never joins it. The registry folder is synced after the history folder exists: a new folder's entry is durable before the
        registry is replaced. A link in place of the folder or the day file is refused.
        """
        at = utc_now()
        data = "".join(json.dumps({"collection": c, "key": k, "archived_at": at, "record": r}, sort_keys=True) + "\n" for c, k, r in moved).encode()
        registry_fd = os.open(self.registry.path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            with contextlib.suppress(FileExistsError):
                os.mkdir(ARCHIVE_FOLDER, 0o700, dir_fd=registry_fd)
            folder_fd = os.open(ARCHIVE_FOLDER, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=registry_fd)
            try:
                os.fsync(registry_fd)
                fd = os.open(f"{datetime.now(UTC):%Y%m%d}.jsonl", os.O_RDWR | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600, dir_fd=folder_fd)
                try:
                    if not stat.S_ISREG(os.fstat(fd).st_mode):
                        raise OSError(errno.EPERM, "the archive file is not a regular file")
                    os.fchmod(fd, 0o600)
                    start = archive_recover_tail(fd)
                    try:
                        write_all(fd, data)
                        os.fsync(fd)
                        os.fsync(folder_fd)
                        archive_verify(fd, start, data)
                    except Exception:
                        with contextlib.suppress(OSError):
                            os.ftruncate(fd, start)
                        raise
                finally:
                    os.close(fd)
            finally:
                os.close(folder_fd)
        finally:
            os.close(registry_fd)

    # -- methods: status, runs, allocations, submit, cancel (D1 sections 3, 5 and 6) ----------------------------------

    def m_ping(self, ctx: Ctx) -> dict[str, Any]:
        """Liveness and protocol version."""
        # The start nonce lets the client that started this launcher recognize it (a test kit stops only a launcher it started itself).
        return {"protocol": PROTOCOL, "pid": os.getpid(), "status": self.status, "registry_id": self.registry_id, "launch_nonce": LAUNCH_NONCE}

    def sanitize_run(self, run: Mapping[str, Any]) -> dict[str, Any]:
        """A run entry without its token hash."""
        public = {key: value for key, value in run.items() if key != "token_sha256"}
        public["stale"] = run_lock.is_stale(run)
        if run.get("pins_sha256"):
            public["pins_unchanged"] = self.pins_unchanged(str(run["run_id"]))
        return public

    def lookup(self, state: Mapping[str, Any], what: str, record_id: str, principal: Mapping[str, Any], conn: Conn) -> dict[str, Any]:
        """One record for ``status``, after checking the principal may see it."""
        if what == "launcher":
            return {
                "status": self.status,
                "degraded_reason": self.degraded_reason,
                "docker_ok": self.docker_ok,
                "ecs_ready": self.ecs_ready(),
                "stream_epoch": self.epoch,
                "pid": os.getpid(),
                "registry_id": self.registry_id,
                "foreign_legacy_containers": list(self.foreign),
                "catalog": state["launcher"].get("catalog"),
                "orchestration": state["launcher"].get("orchestration"),
                "barrier": state["launcher"].get("barrier"),
                "restart": state["launcher"].get("restart"),
                "catalog_authority": state["launcher"].get("catalog_authority"),
                "open_attempts": sum(1 for a in state["attempts"].values() if is_open(a)),
                "open_templates": sorted({a["template"] for a in state["attempts"].values() if is_open(a)}),
                "open_operations": sum(1 for o in state["operations"].values() if o["state"] == "open"),
                "containers": sorted(str(e["name"]) for e in self.inventory.values() if e.get("name") and e.get("state") != "absent"),
                "networks": list(self.networks_seen),
                "orphans_removed": len(state["launcher"].get("orphans", [])),
            }
        if what == "registry":
            domains: dict[str, Any] = dict.fromkeys(run_lock.DOMAINS)
            for domain, run_id in state["domains"].items():
                run = state["runs"][run_id]
                domains[domain] = {"run_id": run_id, "state": run["state"], "mode": run["mode"], "holder": run["holder"], "stale": run_lock.is_stale(run)}
            return {
                "registry": state["_path"],
                "registry_id": state["registry_id"],
                "domains": domains,
                "runs": {run_id: self.sanitize_run(run) for run_id, run in state["runs"].items()},
                "readers": {name: {**entry, "stale": run_lock.is_stale(entry)} for name, entry in state["readers"].items()},
                "allocations": {k: dict(v) for k, v in state["allocations"].items()},
                "registrations": dict(state["registrations"]),
            }
        if what == "protected_sets":
            readers = list(state["readers"].values())
            return {
                "publish_runs": sorted({r["publish_run"] for r in readers if r["state"] == "reserved"}),
                "snapshot_sets": sorted({r["snapshot_set"] for r in readers if r["state"] == "registered"}),
            }
        if what == "run":
            if principal.get("kind") != "run" or principal.get("run_id") != record_id:
                raise LauncherError("status of a run needs that run's token", "principal")
            return self.sanitize_run(state["runs"][record_id])
        if what == "allocation":
            allocation = state["allocations"].get(record_id)
            if allocation is None:
                raise LauncherError("no such allocation", "refused")
            if principal.get("kind") != "alloc" and not is_descendant(conn.peer, int(allocation["holder_pid"]), str(allocation["holder_start"])):
                raise LauncherError("status of an allocation needs its holder", "principal")
            return dict(allocation)
        if what == "attempt":
            attempt = state["attempts"].get(record_id)
            if attempt is None:
                raise LauncherError("no such attempt", "refused")
            self.require_owner(state, conn, principal, attempt["owner"])
            return {key: value for key, value in attempt.items() if key not in ("params",)}
        if what == "submission":
            if principal.get("kind") != "run":
                raise LauncherError("status of a submission needs the run's token", "principal")
            key = f"run:{principal.get('run_id')}|submit|{record_id}"
            created = next((a for a in state["attempts"].values() if a["request"] == key), None)
            return {"found": created is not None, "attempt": None if created is None else {k: v for k, v in created.items() if k != "params"}}
        if what == "catalog":
            return dict(state["launcher"].get("catalog") or {"phase": "down"})
        if what == "lineage":
            stack = dict(state["launcher"].get("lineage") or {"phase": "down"})
            stack["service_phases"] = {
                service: {"phase": state["attempts"][attempt_id]["phase"], "reason": state["attempts"][attempt_id]["reason"]}
                for service, attempt_id in stack.get("services", {}).items()
            }
            return stack
        if what == "orchestration":
            return self.orchestration_view(state)
        raise LauncherError(f"unknown status target {what!r}", "args")

    def orchestration_view(self, state: Mapping[str, Any]) -> dict[str, Any]:
        """The orchestration stack's record with each service's attempt phase, and the objects the launcher knows of it (no secret is in it)."""
        stack = dict(state["launcher"].get("orchestration") or {"phase": "down"})
        stack["service_phases"] = {
            service: {"phase": state["attempts"][attempt_id]["phase"], "reason": state["attempts"][attempt_id]["reason"]}
            for service, attempt_id in (stack.get("services") or {}).items()
        }
        return stack

    def require_owner(self, state: Mapping[str, Any], conn: Conn, principal: Mapping[str, Any], owner: str) -> None:
        """The principal must be the owner of ``owner``: the run's token, or the allocation's holder or a descendant (D1 section 3)."""
        kind, _, ident = owner.partition(":")
        if kind == "run" and principal.get("kind") == "run" and principal.get("run_id") == ident:
            return
        if kind == "alloc":
            allocation = state["allocations"].get(ident)
            if allocation is not None and (
                principal.get("kind") == "alloc"
                and principal.get("alloc_id") == ident
                or is_descendant(conn.peer, int(allocation["holder_pid"]), str(allocation["holder_start"]))
            ):
                return
        raise LauncherError(f"the principal does not own {owner}", "principal")

    def m_status(self, ctx: Ctx) -> Any:
        """A record, optionally waiting until a field reaches one of the given values."""
        what = str(ctx.params.get("what", "launcher"))
        record_id = str(ctx.params.get("id", ""))
        record = self.lookup(ctx.state, what, record_id, ctx.principal, ctx.conn)
        until = ctx.params.get("until")
        wait = float(ctx.params.get("wait", 0) or 0)
        if until and wait > 0 and str(record.get(until["field"])) not in [str(v) for v in until["values"]]:
            self.waiters.append(
                Waiter(ctx.conn, ctx.request_id, what, record_id, str(until["field"]), tuple(str(v) for v in until["values"]), time.monotonic() + wait)
            )
            self.waiter_principals[(id(ctx.conn), ctx.request_id)] = dict(ctx.principal)
            return PENDING
        return record

    def evaluate_waiters(self) -> None:
        """Answer every waiter whose record reached the awaited value."""
        for waiter in list(self.waiters):
            try:
                with self.tx(write=False) as state:
                    record = self.lookup(
                        state, waiter.what, waiter.record_id, self.waiter_principals.get((id(waiter.conn), waiter.request_id), {"kind": "any"}), waiter.conn
                    )
            except LauncherError as error:
                self.waiters.remove(waiter)
                waiter.conn.send({"id": waiter.request_id, "ok": False, "error": str(error), "code": error.code})
                continue
            if str(record.get(waiter.field)) in waiter.values:
                self.waiters.remove(waiter)
                waiter.conn.send({"id": waiter.request_id, "ok": True, "result": record})

    def finish_wait(self, waiter: Waiter, timed_out: bool) -> None:
        """A waiter's time ran out: answer with the record as it is."""
        try:
            with self.tx(write=False) as state:
                record = self.lookup(
                    state, waiter.what, waiter.record_id, self.waiter_principals.get((id(waiter.conn), waiter.request_id), {"kind": "any"}), waiter.conn
                )
        except LauncherError as error:
            waiter.conn.send({"id": waiter.request_id, "ok": False, "error": str(error), "code": error.code})
            return
        waiter.conn.send({"id": waiter.request_id, "ok": True, "result": {**record, "_timed_out": timed_out}})

    def m_plan(self, ctx: Ctx) -> Any:
        """The memory plan the launcher computes now (read-only)."""
        key = f"plan:read:{id(ctx.conn)}:{ctx.request_id}"
        self.read_waiters[key] = (ctx.conn, ctx.request_id)
        self.spawn(key, "planread", lambda: memory_budget.launch_plan(self.reader()).record())
        return PENDING

    def m_image(self, ctx: Ctx) -> Any:
        """The local ID of the image a Compose service names (the kit records it with its baselines); ``compose`` may name the orchestration model."""
        checkout = self.check_checkout(str(ctx.params.get("checkout") or self.cfg.root))
        compose = ctx.params.get("compose", "lakehouse")
        if compose not in ("lakehouse", "orchestration", "lineage"):
            raise LauncherError("compose must be lakehouse or orchestration", "args")
        renderer = self.renderer_of("lineage_service" if compose == "lineage" else "orchestration_check" if compose == "orchestration" else "", checkout)
        reference = renderer.image(str(ctx.params.get("service", "")))
        key = f"imageread:{id(ctx.conn)}:{ctx.request_id}"
        self.read_waiters[key] = (ctx.conn, ctx.request_id)
        history = bool(ctx.params.get("history"))
        self.spawn(key, "imageread", self.read_image, reference, history)
        return PENDING

    def read_image(self, reference: str, history: bool) -> dict[str, Any]:
        """Worker: an image's description and, when asked, its layer history (read-only; the kit and the audits use it)."""
        info = self.docker.image(reference)
        return {"image": reference, "info": info, "history": self.docker.image_history(reference) if history and info is not None else None}

    def on_imageread(self, key: str, outcome: str, value: Any) -> None:
        """Answer an image request."""
        conn, request_id = self.read_waiters.pop(key, (None, ""))
        if conn is None:
            return
        if outcome == "ok" and value["info"] is not None:
            result = {"image": value["image"], "id": value["info"]["Id"], "inspect": value["info"], "history": value["history"]}
            conn.send({"id": request_id, "ok": True, "result": result})
        else:
            conn.send(
                {
                    "id": request_id,
                    "ok": False,
                    "error": f"image {value['image'] if outcome == 'ok' else ''} is missing: {value if outcome != 'ok' else ''}",
                    "code": "refused",
                }
            )

    def on_planread(self, key: str, outcome: str, value: Any) -> None:
        """Answer a read-only plan request."""
        conn, request_id = self.read_waiters.pop(key, (None, ""))
        if conn is None:
            return
        if outcome == "ok":
            conn.send({"id": request_id, "ok": True, "result": value})
        elif isinstance(value, memory_budget.BudgetError):
            conn.send({"id": request_id, "ok": False, "error": failure_text(value), "code": "budget"})
        else:
            conn.send({"id": request_id, "ok": False, "error": failure_text(value), "code": "degraded"})

    def complete_request(self, key: str, result: dict[str, Any], error: LauncherError | None = None) -> None:
        """Record the final result of a pending request and answer every connection waiting for it."""
        stored = {"error": str(error), "code": error.code} if error is not None else result
        with self.tx() as state:
            record = state["requests"].get(key)
            if record is not None:
                record.update({"state": "done", "result": stored, "outcome": "error" if error is not None else "ok"})
        for conn, request_id in self.pending_conns.pop(key, []):
            if error is not None:
                conn.send({"id": request_id, "ok": False, "error": str(error), "code": error.code})
            else:
                conn.send({"id": request_id, "ok": True, "result": result})

    # runs -------------------------------------------------------------------------------------------------------

    def m_acquire(self, ctx: Ctx) -> dict[str, Any]:
        """Create a run and return its token once; the token file is written and fsynced before the run is published (D1 section 11)."""
        params = ctx.params
        domain, run_id, mode = str(params.get("domain")), str(params.get("run_id")), str(params.get("mode"))
        if domain not in run_lock.DOMAINS or mode not in run_lock.MODES or not RUN_ID.fullmatch(run_id):
            raise LauncherError("acquire needs a domain, a mode and a plain run ID", "args")
        if self.registry.kind == "test" and mode == "real":
            raise LauncherError("real runs are refused in a test registry", "refused")
        pid = int(params.get("pid") or ctx.conn.peer.pid)
        pid_start = run_lock.process_start(pid)
        if pid_start is None:
            raise LauncherError(f"holder process {pid} is not running", "refused")
        prefixes = [str(p) for p in params.get("write_prefixes", [])]
        token = run_lock.new_run(
            ctx.state, domain, run_id, mode, str(params.get("holder", "manual")), pid, pid_start, prefixes, str(params.get("checkout") or self.cfg.root)
        )
        path = write_token_file(self.cfg.token_root, run_id, token, self.cfg.token_anchor)
        ctx.state["runs"][run_id]["token_file"] = str(path)
        return {"run_id": run_id, "token_file": str(path), "_not_stored": {"token": token}}

    def run_entry(self, ctx: Ctx) -> dict[str, Any]:
        """The run the request's token names."""
        return dict(ctx.state["runs"][ctx.principal["run_id"]])

    def m_enter(self, ctx: Ctx) -> dict[str, Any]:
        """Let a nested launcher with the run's token join it."""
        entry = self.run_entry(ctx)
        if entry["state"] != "held":
            raise LauncherError(f"run {entry['run_id']} is in state {entry['state']}; only a held run accepts launches", "refused")
        return {"run_id": entry["run_id"], "domain": entry["domain"], "mode": entry["mode"]}

    def m_pin(self, ctx: Ctx) -> dict[str, Any]:
        """Record the bronze prefixes a transform run reads."""
        entry = self.run_entry(ctx)
        run_lock.pin_in(ctx.state, entry["run_id"], entry, [str(p) for p in ctx.params.get("prefixes", [])])
        return {"ok": True}

    def m_register(self, ctx: Ctx) -> dict[str, Any]:
        """Add or remove the run's registration as a shared-service user."""
        entry = self.run_entry(ctx)
        kind = str(ctx.params.get("kind"))
        if (
            kind == "catalog_user"
            and bool(ctx.params.get("add", True))
            and (ctx.state["launcher"].get("barrier") or self.catalog_record(ctx.state)["phase"] == "tearing_down")
        ):
            raise LauncherError("catalog teardown has begun", "refused")
        if kind not in run_lock.REGISTRATION_KINDS:
            raise LauncherError(f"registration kind {kind!r} is unknown", "args")
        run_lock.register_in(ctx.state, entry["run_id"], entry, kind, bool(ctx.params.get("add", True)))
        return {"ok": True}

    def m_finalize(self, ctx: Ctx) -> dict[str, Any]:
        """Move a held run to ``finalizing``."""
        return self.mark_run(ctx, "finalizing")

    def m_mark(self, ctx: Ctx) -> dict[str, Any]:
        """Move a held run to ``finalizing`` or ``failed_retained``."""
        return self.mark_run(ctx, str(ctx.params.get("state")))

    def mark_run(self, ctx: Ctx, new: str) -> dict[str, Any]:
        """The run-state transition of ``run_lock.mark`` inside the request's transaction."""
        allowed = {"finalizing": ("held",), "failed_retained": ("held", "finalizing")}
        if new not in allowed:
            raise LauncherError("mark needs finalizing or failed_retained", "args")
        entry = self.run_entry(ctx)
        if entry["state"] == new:
            return {"state": new}
        if entry["state"] not in allowed[new]:
            raise LauncherError(f"run {entry['run_id']} is in state {entry['state']}; it cannot move to {new}", "refused")
        run_lock.set_state(ctx.state, entry["run_id"], new)
        return {"state": new}

    def open_work(self, state: Mapping[str, Any], owner_prefix: str) -> list[str]:
        """What is still open for an owner: attempts, operations and registrations (D1 section 5 item 5)."""
        found = [f"attempt {a['id']} ({a['phase']})" for a in state["attempts"].values() if is_open(a) and (a["owner"] == owner_prefix)]
        attempts = {a["id"] for a in state["attempts"].values() if a["owner"] == owner_prefix}
        found += [f"operation {o['op_id']} ({o['kind']})" for o in state["operations"].values() if o["state"] == "open" and o["attempt"] in attempts]
        kind, _, ident = owner_prefix.partition(":")
        if kind == "run":
            found += [f"registration {r}" for r in state["runs"][ident]["registrations"]]
            if state["runs"][ident].get("seal"):
                found.append(f"seal {state['runs'][ident]['seal']['id']}")
        if kind == "alloc":
            found += [f"registration {r['id']}" for r in state["registrations"].values() if r["allocation"] == ident]
        return found

    def m_release(self, ctx: Ctx) -> Any:
        """Release a finalizing run when every predicate holds, or a failed one with ``--owner``; the container listing is the last predicate."""
        entry = self.run_entry(ctx)
        run_id = entry["run_id"]
        if entry["state"] == "released":
            return {"state": "released"}
        owner_flag = bool(ctx.params.get("owner") or ctx.principal.get("owner"))
        if entry["state"] == "failed_retained":
            if not owner_flag:
                raise LauncherError(f"run {run_id} is in state failed_retained; failed_retained needs --owner", "refused")
        elif entry["state"] == "finalizing":
            record = ctx.params.get("record")
            try:
                run_lock.check_record(run_id, record)
            except run_lock.Refused as error:
                raise LauncherError(str(error), "refused") from error
        else:
            raise LauncherError(f"run {run_id} is in state {entry['state']}; release needs finalizing, or failed_retained with --owner", "refused")
        open_work = self.open_work(ctx.state, f"run:{run_id}")
        if open_work:
            raise LauncherError(f"run {run_id}: {open_work[0]} is open", "refused")
        key = f"runlist:{ctx.key}"
        self.spawn(key, "runlist", lambda: self.docker.containers(label=f"{LABEL_RUN}={run_id}"))
        self.release_requests[key] = (ctx.key, run_id, entry["state"])
        return PENDING

    def on_runlist(self, key: str, outcome: str, value: Any) -> None:
        """The all-state listing by run label: empty means the run is released in the same step (D1 section 5 item 5)."""
        request_key, run_id, expected = self.release_requests.pop(key, ("", "", ""))
        if not request_key:
            return
        if outcome != "ok":
            self.complete_request(request_key, {}, Degraded(f"cannot list the containers of run {run_id}: {failure_text(value)}"))
            return
        with self.tx() as state:
            entry = state["runs"][run_id]
            problems = self.open_work(state, f"run:{run_id}")
            if value:
                self.complete_request(
                    request_key, {}, LauncherError(f"run {run_id}: container {', '.join(str(c['Names'][0]).lstrip('/') for c in value)} remains", "refused")
                )
                return
            if problems or entry["state"] != expected:
                self.complete_request(request_key, {}, LauncherError(f"run {run_id} changed during the release checks; retry", "refused"))
                return
            run_lock.set_state(state, run_id, "released")
        self.complete_request(request_key, {"state": "released"})

    def m_recover(self, ctx: Ctx) -> dict[str, Any]:
        """Owner recovery: one transaction increments the generation and sets ``recovering``; cleanup then fences every container (D1 section 5 item 4)."""
        entry = self.run_entry(ctx)
        run_id = entry["run_id"]
        if entry["state"] in ("failed_retained", "released", "recovering"):
            return {"state": entry["state"], "generation": entry.get("generation", 1)}
        ctx.state["runs"][run_id]["generation"] = int(entry.get("generation", 1)) + 1
        run_lock.set_state(ctx.state, run_id, "recovering")
        for attempt in ctx.state["attempts"].values():
            if attempt["owner"] == f"run:{run_id}" and is_open(attempt):
                attempt["cancel_intent"] = attempt["cancel_intent"] or {"reason": "fenced", "at": utc_now()}
        return {"state": "recovering", "generation": ctx.state["runs"][run_id]["generation"]}

    def advance_owners(self, state: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """Owner-level steps: recovering runs, releasing allocations, the catalog, the Docker restart and snapshot copies."""
        for run_id, run in state["runs"].items():
            unresolved = any(
                op["state"] == "open" and op["kind"] == "create" and state["attempts"].get(op["attempt"], {}).get("owner") == f"run:{run_id}"
                for op in state["operations"].values()
            )
            if run["state"] == "recovering" and not unresolved and not self.open_work(state, f"run:{run_id}"):
                run_lock.set_state(state, run_id, "failed_retained")
        self.advance_allocations(state, actions)
        self.advance_catalog(state, actions)
        self.advance_orchestration(state, actions)
        self.advance_lineage(state, actions)
        self.advance_restart(state, actions)
        self.advance_copies(state, actions)
        self.advance_seals(state, actions)

    # allocations ------------------------------------------------------------------------------------------------

    def m_allocate(self, ctx: Ctx) -> dict[str, Any]:
        """Take the machine for a shared allocation: one heavy allocation at a time, with the plan computed from live readings (D1 section 6)."""
        if self.foreign:
            raise LauncherError(f"foreign legacy containers carry {LABEL_RUN} ({', '.join(self.foreign[:3])}); the owner reconciles them first", "refused")
        holder = self.heavy_holder(ctx.state)
        if holder is not None:
            raise LauncherError(f"allocation {holder} holds the machine", "refused")
        workers = int(ctx.params.get("workers", 1))
        if workers < 1 or workers > 64:
            raise LauncherError("workers must be between 1 and 64", "args")
        pid = ctx.conn.peer.pid
        start = run_lock.process_start(pid)
        if start is None:
            raise LauncherError("the holder process is not running", "refused")
        allocation_id = new_id("al")
        ctx.state["allocations"][allocation_id] = {
            "id": allocation_id,
            "kind": "shared",
            "state": "planning",
            "generation": 1,
            "workers": workers,
            "holder_pid": pid,
            "holder_start": start,
            "environment": None,
            "record": None,
            "services": dict(ctx.state["services"]),
            "created_at": utc_now(),
        }
        # Registered limits are keyed by service; the readings are keyed by container name.
        growth = self.growth_reserve_map(ctx.state)
        self.spawn(f"plan:{allocation_id}", "plan", self.compute_allocation, workers, growth)
        return {"alloc_id": allocation_id, "state": "planning"}

    def compute_allocation(self, workers: int, growth: dict[str, int]) -> dict[str, Any]:
        """Worker: the launch plan, split into equal shares for a pool, with the growth reserve of registered services."""
        reader = self.reader()
        plan = memory_budget.launch_plan(reader, growth)
        return memory_budget.pool_plan(plan, workers)

    def on_plan(self, key: str, outcome: str, value: Any) -> None:
        """The plan arrived: the allocation is held with it, or fails with the reason."""
        allocation_id = key.split(":", 1)[1]
        if outcome == "docker" and value.status == 0:
            with self.tx() as state:
                allocation = state["allocations"].get(allocation_id)
                if allocation is not None and allocation["state"] == "planning":
                    allocation.update({"state": "failed", "error": f"Docker does not answer: {value.message}"})
            return
        with self.tx() as state:
            allocation = state["allocations"].get(allocation_id)
            if allocation is None or allocation["state"] != "planning":
                return
            if outcome != "ok":
                allocation.update({"state": "failed", "error": failure_text(value)})
                return
            allocation.update({"state": "held", "environment": value["environment"], "record": value["record"]})

    def m_release_allocation(self, ctx: Ctx) -> Any:
        """Release an allocation: refused while anything of it is open, then the listing by allocation label must be empty."""
        allocation_id = str(ctx.principal["alloc_id"])
        allocation = ctx.state["allocations"][allocation_id]
        if allocation["state"] in ("released", "failed"):
            return {"state": allocation["state"]}
        if allocation["state"] != "held":
            raise LauncherError(f"allocation {allocation_id} is {allocation['state']}", "refused")
        work = self.open_work(ctx.state, f"alloc:{allocation_id}")
        if work:
            raise LauncherError(f"allocation {allocation_id}: {work[0]} is open", "refused")
        allocation["state"] = "releasing"
        key = f"alloclist:{allocation_id}"
        self.release_requests[key] = (ctx.key, allocation_id, "")
        self.spawn(key, "alloclist", self.list_allocation, allocation_id)
        return PENDING

    def on_alloclist(self, key: str, outcome: str, value: Any) -> None:
        """The all-state listing by allocation label arrived (D1 section 6 item 3)."""
        request_key, allocation_id, _ = self.release_requests.pop(key, ("", "", ""))
        if not request_key:
            return
        with self.tx() as state:
            allocation = state["allocations"][allocation_id]
            if outcome != "ok":
                allocation["state"] = "held"
                self.complete_request(request_key, {}, Degraded(f"cannot list the containers of allocation {allocation_id}: {failure_text(value)}"))
                return
            if value or self.open_work(state, f"alloc:{allocation_id}"):
                allocation["state"] = "held"
                self.complete_request(request_key, {}, LauncherError(f"allocation {allocation_id}: a container or attempt remains", "refused"))
                return
            allocation["state"] = "released"
        self.complete_request(request_key, {"state": "released"})

    def advance_allocations(self, state: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """Holder death: the allocation's open attempts are cancelled by policy and cleaned up; the allocation is then released (D1 section 6 item 4)."""
        for allocation_id, allocation in state["allocations"].items():
            if allocation["kind"] != "shared" or allocation["state"] not in ("held", "planning"):
                continue
            if run_lock.process_start(int(allocation["holder_pid"])) == allocation["holder_start"]:
                continue
            allocation["holder_dead"] = True
            for attempt in state["attempts"].values():
                if attempt["owner"] == f"alloc:{allocation_id}" and is_open(attempt):
                    attempt["cancel_intent"] = attempt["cancel_intent"] or {"reason": "holder_dead", "at": utc_now()}
            if allocation["state"] == "planning":
                allocation["state"] = "failed"
                continue
            if not [item for item in self.open_work(state, f"alloc:{allocation_id}") if not item.startswith("registration ")]:
                # Every attempt of the dead holder has ended and its container is confirmed gone: its reader registrations end with it.
                for registration_id in [r["id"] for r in state["registrations"].values() if r["allocation"] == allocation_id]:
                    state["registrations"].pop(registration_id)
            if not self.open_work(state, f"alloc:{allocation_id}"):
                key = f"alloclist:{allocation_id}"
                if key not in self.inflight and key not in self.release_requests:
                    allocation["state"] = "releasing"
                    self.release_requests[key] = ("", allocation_id, "dead")
                    actions.append(partial(self.spawn, key, "alloclist_dead", self.list_allocation, allocation_id))

    def list_allocation(self, allocation_id: str) -> list[dict[str, Any]]:
        """Worker: every container carrying the allocation's label, in all states."""
        return self.docker.containers(label=f"{LABEL_ALLOCATION}={allocation_id}")

    def on_alloclist_dead(self, key: str, outcome: str, value: Any) -> None:
        """A dead holder's allocation: released once absence is proved, otherwise kept."""
        _, allocation_id, _ = self.release_requests.pop(key, ("", "", ""))
        with self.tx() as state:
            allocation = state["allocations"].get(allocation_id)
            if allocation is None:
                return
            if outcome == "ok" and not value and not self.open_work(state, f"alloc:{allocation_id}"):
                allocation["state"] = "released"
            else:
                allocation["state"] = "held"

    def check_holders(self) -> None:
        """Nothing to do outside the state machine pass; stale holders are found in ``advance_allocations``."""

    # submit and cancel ------------------------------------------------------------------------------------------

    def owner_of(self, ctx: Ctx) -> str:
        """The owner the principal stands for."""
        if ctx.principal["kind"] == "run":
            return f"run:{ctx.principal['run_id']}"
        return f"alloc:{ctx.principal['alloc_id']}"

    def m_submit(self, ctx: Ctx) -> dict[str, Any]:
        """Record a queued attempt in one conditional transition (D1 section 4 item 1a)."""
        params = ctx.params
        identity = params.get("caller_identity")
        if "caller_identity" in params and list(caller_identity_validator(self.cfg.root).iter_errors(identity)):
            raise LauncherError("caller identity violates the request contract", "args")
        template_name = str(params.get("template"))
        requested_target = params.get("target", "auto")
        if requested_target not in ecs_target.TARGETS:
            raise LauncherError("target must be auto, ecs or docker", "args")
        capture = params.get("capture_dbt_artifacts", False)
        if type(capture) is not bool or capture and (identity is None or template_name not in ("fixture_dbt", "real_dbt")):
            raise LauncherError("dbt artifact capture requires a dbt template and validated caller identity", "args")
        template = TEMPLATES.get(template_name)
        if template is None or template_name in ("probe", "catalog_service", "orchestration_service", "lineage_service"):
            raise LauncherError(f"template {template_name!r} cannot be submitted by a client", "refused")
        owner = self.owner_of(ctx)
        kind, _, ident = owner.partition(":")
        run_mode = str(ctx.state["runs"].get(ident, {}).get("mode", "real"))
        job_class = "offline"
        if template_name.startswith("pipeline_"):
            try:
                job_class = pipeline_render.job_class(template_name, dict(params.get("params") or {}), run_mode)
            except pipeline_protocol.PipelineError as error:
                raise LauncherError(str(error), "args") from None
        cloud_row = ecs_target.row_for(self.ecs_jobs, template_name, params.get("params") or {}, job_class)
        cloud_managed = bool(self.ecs_jobs["enabled"] and cloud_row is not None)
        if cloud_managed and self.ecs_runtime is None:
            raise LauncherError("ECS placement is enabled but its runtime is not integrated", "refused")
        if requested_target == "ecs" and (not self.ecs_ready() or cloud_row is None):
            raise LauncherError("ECS is disabled, not integrated or this template is ineligible", "refused")
        if self.ecs_jobs["enabled"] and template_name == "pipeline_publish" and job_class == "lakehouse_write":
            raise LauncherError("AWS writers are not enabled in the read increment", "refused")
        authority = ctx.state["launcher"].get("catalog_authority") or {}
        if (
            cloud_managed
            and job_class.startswith("lakehouse_")
            and (authority.get("target") != "aws" or authority.get("frozen") is not False or type(authority.get("epoch")) is not int or authority["epoch"] < 1)
        ):
            raise LauncherError("AWS lakehouse execution requires an unfrozen authoritative AWS catalog", "refused")
        mac_problem = (
            None
            if cloud_managed
            else self.mac_write_problem(ctx.state, owner, template_name, params.get("params") or {}, [str(a) for a in params.get("args", [])])
        )
        if mac_problem:
            raise LauncherError(mac_problem, "refused")
        if kind not in template.owners:
            raise LauncherError(f"template {template_name} is not available to {kind} owners", "refused")
        launch_id = str(params.get("launch_id", ""))
        if not IDENTIFIER.fullmatch(launch_id):
            raise LauncherError("launch_id must be a plain identifier", "args")
        sealed_run = (params.get("params") or {}).get(build_provenance.SEALED_RUN_PARAM)
        if sealed_run is not None:
            if template_name not in SEALED_READERS or not isinstance(sealed_run, str) or not RUN_ID.fullmatch(sealed_run):
                raise LauncherError(f"{build_provenance.SEALED_RUN_PARAM} takes a run ID and only {', '.join(SEALED_READERS)} accept it", "args")
            self.sealed_reader_vars(ctx.state, sealed_run)
        if kind == "run":
            run = ctx.state["runs"][ident]
            if run["state"] != "held":
                raise LauncherError(f"run {ident} is {run['state']}; only a held run accepts submits", "refused")
            problem = self.template_problem(ctx.state, owner, template_name, dict(params.get("params") or {}))
            if problem:
                raise LauncherError(problem, "refused")
            raw_args = [str(a) for a in params.get("args", [])]
            if template_name == "real_dbt":
                provenance_vars(run)
                if build_provenance.is_mutating(raw_args) and run.get("build_state") == "sealed":
                    raise LauncherError(f"run {ident} is sealed; a mutating launch is refused", "refused")
            if template_name == "sealed_consumer":
                if raw_args != ["verify"]:
                    raise LauncherError("sealed_consumer takes the single argument verify", "args")
                consumer_vars(run)
        else:
            allocation = ctx.state["allocations"][ident]
            if allocation["state"] != "held":
                raise LauncherError(f"allocation {ident} is {allocation['state']}; only a held allocation accepts submits", "refused")
            problem = self.template_problem(ctx.state, owner, template_name, dict(params.get("params") or {}))
            if problem:
                raise LauncherError(problem, "refused")
        if self.status != "serving" and not cloud_managed:
            raise Degraded(f"the launcher is {self.status}")
        if self.foreign and not cloud_managed:
            raise LauncherError("foreign legacy containers exist; the owner reconciles them first", "refused")
        if ctx.state["launcher"].get("restart") and not cloud_managed:
            raise LauncherError("a Docker restart is running", "refused")
        if template_name == "lineage_dump":
            stack = ctx.state["launcher"].get("lineage") or {}
            dump_state = str(Path(str((params.get("params") or {}).get("state", ""))).resolve())
            requested = str(params.get("checkout") or self.cfg.root)
            if stack.get("phase") != "up" or stack.get("state") != dump_state or stack.get("checkout") != requested:
                raise LauncherError("lineage dump requires this registry's active lineage state and checkout", "refused")
        if template_name == "orchestration_dump" and (ctx.state["launcher"].get("orchestration") or {}).get("phase") != "up":
            raise LauncherError("the orchestration stack is not up; the dump needs its database", "refused")
        if template.needs_catalog and not cloud_managed:
            if ctx.state["launcher"].get("barrier"):
                raise LauncherError("a catalog teardown barrier is held", "refused")
            if (ctx.state["launcher"].get("catalog") or {}).get("phase") != "up":
                raise LauncherError("the catalog is not up; call catalog_up first", "refused")
        for other in ctx.state["attempts"].values():
            if other["owner"] == owner and other["launch_id"] == launch_id and is_open(other):
                raise LauncherError(f"launch {launch_id} already has open attempt {other['id']}", "refused")
        checkout = self.check_checkout(str(params.get("checkout") or self.cfg.root))
        seconds = int(params.get("timeout_seconds") or template.default_seconds)
        if not 1 <= seconds <= template.max_seconds:
            raise LauncherError(f"timeout_seconds must be between 1 and {template.max_seconds}", "args")
        args = [str(a) for a in params.get("args", [])]
        attempt_params = dict(params.get("params") or {})
        problem = self.session_problem(ctx.state, owner, template_name, attempt_params, seconds)
        if problem:
            raise LauncherError(problem, "args")
        self.dry_render(ctx.state, owner, template_name, args, attempt_params, checkout)
        self.take_guard(ctx.state, template, attempt_params)
        fenced = (template_name == "gold_publish" and attempt_params.get("job") == "gold_maintenance") or (
            template_name == "pipeline_publish" and attempt_params.get("step") == "maintenance"
        )
        protection = self.maintenance_protection(ctx.state) if fenced else []
        attempt = self.new_attempt(ctx.state, owner, template_name, launch_id, args, attempt_params, request=ctx.key, checkout=str(checkout), seconds=seconds)
        if cloud_managed:
            ecs_target.initialize(attempt, requested_target, time.time(), authority.get("epoch") if job_class.startswith("lakehouse_") else None)
        if identity is not None:
            attempt["caller_identity"] = dict(identity)
        capture = params.get("capture_dbt_artifacts", False)
        if capture:
            attempt["capture_dbt_artifacts"] = True
        self.register_guard(ctx.state, template, attempt)
        if fenced:
            self.take_maintenance_fence(ctx.state, ident, attempt, protection)
        return {"attempt_id": attempt["id"], "n": attempt["n"], "deadline_utc": attempt["deadline_utc"]}

    @staticmethod
    def session_problem(state: Mapping[str, Any], owner: str, template_name: str, params: Mapping[str, Any], seconds: int) -> str | None:
        """What refuses a pipeline job whose timeout a role session cannot cover, or None: the job needs a session when its class needs AWS."""
        kind, _, ident = owner.partition(":")
        run = state["runs"].get(ident) if kind == "run" else None
        if run is None or not template_name.startswith("pipeline_"):
            return None
        try:
            role = pipeline_render.session_role(template_name, dict(params), str(run["mode"]))
        except pipeline_protocol.PipelineError:
            return None
        return pipeline_session.deadline_problem(seconds) if role else None

    PLACEHOLDER_ENV = {"JOB_MEMORY_LIMIT": str(4 * 1024**3), "DUCKDB_MEMORY_LIMIT": "3GB", "SPARK_JOB_MEMORY": "3g", "JOB_THREADS": "1"}

    def dry_render(self, state: Mapping[str, Any], owner: str, template_name: str, args: list[str], params: dict[str, Any], checkout: Path) -> None:
        """Validate arguments, parameters and mounts by rendering with placeholder values; nothing is recorded."""
        renderer = self.renderer_of(template_name, checkout)
        kind, _, ident = owner.partition(":")
        run_folder = None
        if kind == "run":
            run_folder = self.cfg.runs_root / ident
        if template_name in ("real_dbt", "sealed_consumer"):
            return
        provenance: dict[str, str] = {}
        if build_provenance.SEALED_RUN_PARAM in params:
            run_folder = self.cfg.runs_root / "dry"
            provenance = {build_provenance.VARIABLE_SEALED_SHA: "0" * 64, build_provenance.VARIABLE_SEALED_EXPECTED: "{}"}
        completion, run_mode = self.gold_inputs(state, owner, template_name, params)
        pipeline: Mapping[str, Any] = {}
        snapshot = None
        if kind == "run" and template_name.startswith("pipeline_"):
            run = state["runs"][ident]
            run_mode = str(run["mode"])
            pipeline = self.pipeline_inputs(state, run)
            if template_name == "pipeline_dbt":
                pointer = run.get("snapshot")
                if pointer is None or run_folder is None:
                    raise LauncherError("the run has no complete snapshot", "spec")
                snapshot = run_folder / "snapshots" / str(pointer)
                if params.get("layer") == "silver" and run_mode == "real":
                    provenance = provenance_vars(run)
        inp = RenderInput(
            template_name,
            ident,
            "dry",
            1,
            1,
            "op-dry",
            self.PLACEHOLDER_ENV,
            int(time.time()) + 60,
            args,
            params,
            {},
            snapshot,
            run_folder,
            provenance,
            (),
            completion,
            run_mode,
            self.gold_accepted(state, owner, template_name, params),
            pipeline,
        )
        renderer.recording = set()
        try:
            renderer.render(inp, self.registry_id)
        finally:
            renderer.recording = None
            renderer.fetched.clear()
            renderer.interpolated.clear()

    def guard_folder(self, template: Template, params: Mapping[str, Any]) -> str | None:
        """The key of the shared package folder a template touches, if any."""
        if template.guard is None:
            return None
        path = params.get("packages_host")
        return str(Path(str(path)).resolve()) if path else None

    def take_guard(self, state: dict[str, Any], template: Template, params: Mapping[str, Any]) -> None:
        """Refuse a writer while a reader holds the package folder and a reader while a writer holds it (D1 section 10 item 1)."""
        folder = self.guard_folder(template, params)
        if folder is None:
            return
        guard = state["guards"].get(folder, {"writer": None, "readers": {}})
        if template.guard == "writer" and (guard["writer"] or guard["readers"]):
            raise LauncherError(f"the package folder is in use ({'a writer' if guard['writer'] else 'readers'}); dbt_deps is refused", "refused")
        if template.guard == "reader" and guard["writer"]:
            raise LauncherError("dbt_deps holds the package folder; fixture_dbt is refused", "refused")

    @staticmethod
    def maintenance_protection(state: Mapping[str, Any]) -> list[dict[str, str]]:
        """The items the maintenance job must protect, from the readers of the registry; refuses when the fence is held or a reader cannot be named.

        A reservation is a pending item from the time it was taken. A registration names a snapshot set; the set must be a gold run ID, because the job
        keeps sets by committed run and cannot keep a set it cannot name.
        """
        Launcher.require_no_maintenance_fence(state)
        items = []
        for name, reader in sorted(state["readers"].items()):
            if reader.get("namespace") in ("silver", "silver_e2e"):
                continue
            if not gold_publish_record.READER_NAME.fullmatch(name):
                raise LauncherError(f"reader {name} has a name the maintenance job cannot list; maintenance is refused", "refused")
            if reader["state"] == "registered":
                if not gold_publish_record.RUN_ID.fullmatch(str(reader["snapshot_set"])):
                    raise LauncherError(
                        f"reader {name} is registered on {reader['snapshot_set']!r}, which is not a gold run ID; maintenance is refused", "refused"
                    )
                items.append({"kind": "registration", "id": name, "run_id": str(reader["snapshot_set"])})
            else:
                items.append({"kind": "pending", "id": name, "since": str(reader["at"])})
        return items

    def take_maintenance_fence(self, state: dict[str, Any], run_id: str, attempt: Mapping[str, Any], items: list[dict[str, str]]) -> None:
        """Take the maintenance fence for the attempt and write the protected file it fixes, in the registry transaction that admits the job.

        The fence is a registry guard held by the attempt; ``finish_attempt`` frees it at the attempt's end whatever the reason. While it is held, no
        reservation or registration changes, so the file stays true up to the job's expiry call.
        """
        state["guards"][MAINTENANCE_FENCE] = {"writer": attempt["id"], "readers": {}}
        document = {
            "schema": 1,
            "fence": {"run_id": run_id, "attempt_id": attempt["id"], "taken_at": utc_now()},
            "items": items,
        }
        folder = self.cfg.runs_root / run_id / "publish"
        try:
            folder.mkdir(parents=True, exist_ok=True)
            run_lock.write_atomic(folder / "protected.json", json.dumps(document, indent=2, sort_keys=True) + "\n")
        except OSError as error:
            raise LauncherError(f"the protected file of the maintenance job cannot be written: {error.strerror or type(error).__name__}", "refused") from error

    def register_guard(self, state: dict[str, Any], template: Template, attempt: Mapping[str, Any]) -> None:
        """Record the guard owner at admission."""
        folder = self.guard_folder(template, attempt["params"])
        if folder is None:
            return
        guard = state["guards"].setdefault(folder, {"writer": None, "readers": {}})
        if template.guard == "writer":
            guard["writer"] = attempt["id"]
        else:
            guard["readers"][attempt["id"]] = {"at": utc_now()}

    def m_cancel(self, ctx: Ctx) -> dict[str, Any]:
        """Persist a cancel intent; the attempt ends through cleanup (D1 section 4 item 5)."""
        attempt = ctx.state["attempts"].get(str(ctx.params.get("attempt_id")))
        if attempt is None:
            raise LauncherError("no such attempt", "refused")
        self.require_owner(ctx.state, ctx.conn, ctx.principal, attempt["owner"])
        if is_open(attempt) and not attempt["cancel_intent"]:
            attempt["cancel_intent"] = {"reason": "cancelled", "at": utc_now()}
        return {"attempt_id": attempt["id"], "phase": attempt["phase"], "cancel_intent": attempt["cancel_intent"]}

    def m_logs(self, ctx: Ctx) -> Any:
        """The last log lines of an attempt: live from Docker while it exists, otherwise from the saved record."""
        attempt = ctx.state["attempts"].get(str(ctx.params.get("attempt_id")))
        if attempt is None:
            raise LauncherError("no such attempt", "refused")
        self.require_owner(ctx.state, ctx.conn, ctx.principal, attempt["owner"])
        tail = int(ctx.params.get("tail", 200))
        saved = attempt.get("log_tail")
        container_id = attempt.get("container_id")
        if is_open(attempt) and container_id and self.docker_ok:
            key = f"logs:{id(ctx.conn)}:{ctx.request_id}"
            self.read_waiters[key] = (ctx.conn, ctx.request_id)
            secrets = self.attempt_secrets(ctx.state, attempt)
            self.spawn(key, "logsread", lambda: {"text": self.live_log(container_id, tail, secrets)})
            return PENDING
        text = Path(saved).read_text() if saved and Path(saved).is_file() else ""
        return {"text": "".join(text.splitlines(keepends=True)[-tail:])}

    def live_log(self, container_id: str, tail: int, secrets: SecretSources) -> str:
        """Worker: the last log lines of a running container with its credentials redacted; without its configuration no text is returned."""
        held = container_secrets(self.docker.inspect(container_id), secrets, self.key)
        return redact(self.docker.logs_tail(container_id, tail), held)

    def on_logsread(self, key: str, outcome: str, value: Any) -> None:
        """Answer a live log request."""
        conn, request_id = self.read_waiters.pop(key, (None, ""))
        if conn is None:
            return
        if outcome == "ok":
            conn.send({"id": request_id, "ok": True, "result": value})
        else:
            conn.send({"id": request_id, "ok": False, "error": failure_text(value), "code": "degraded"})

    # readers and the legacy snapshot-set readers -------------------------------------------------------------

    def m_register_reader(self, ctx: Ctx) -> dict[str, Any]:
        """Register a reader of the catalog under an allocation; refused while the teardown barrier is held (D1 section 8 item 3)."""
        if ctx.state["launcher"].get("barrier"):
            raise LauncherError("a catalog teardown barrier is held", "refused")
        if (ctx.state["launcher"].get("catalog") or {}).get("phase") != "up":
            raise LauncherError("the catalog is not up", "refused")
        allocation_id = str(ctx.principal["alloc_id"])
        if ctx.state["allocations"][allocation_id]["state"] != "held":
            raise LauncherError("the allocation is not held", "refused")
        registration_id = new_id("rg")
        ctx.state["registrations"][registration_id] = {"id": registration_id, "kind": "reader", "allocation": allocation_id, "at": utc_now()}
        return {"registration_id": registration_id}

    def m_deregister_reader(self, ctx: Ctx) -> dict[str, Any]:
        """Remove a registration; only the holder of its allocation may (D1 section 3 item 1)."""
        registration = ctx.state["registrations"].get(str(ctx.params.get("registration_id")))
        if registration is None:
            return {"ok": True}
        if registration["allocation"] != ctx.principal["alloc_id"]:
            raise LauncherError("the registration belongs to another allocation", "principal")
        ctx.state["registrations"].pop(registration["id"])
        return {"ok": True}

    def m_reserve(self, ctx: Ctx) -> dict[str, Any]:
        """A snapshot-set reader announces the publish run it is about to read [777]."""
        self.require_no_maintenance_fence(ctx.state)
        params = ctx.params
        reader, publish_run = str(params["reader"]), str(params["publish_run"])
        pid = int(params.get("pid") or ctx.conn.peer.pid)
        start = run_lock.process_start(pid)
        if start is None:
            raise LauncherError(f"reader process {pid} is not running", "refused")
        if reader in ctx.state["readers"]:
            raise LauncherError(f"reader {reader} already holds a reservation or registration", "refused")
        ctx.state["readers"][reader] = {
            "domain": str(params["domain"]),
            "publish_run": publish_run,
            "snapshot_set": None,
            "state": "reserved",
            "pid": pid,
            "pid_start": start,
            "at": utc_now(),
        }
        return {"ok": True}

    @staticmethod
    def require_no_maintenance_fence(state: Mapping[str, Any]) -> None:
        """Refuse a change of the readers while a gold maintenance job holds the fence: the job's protected file is final until its attempt ends."""
        holder = (state["guards"].get(MAINTENANCE_FENCE) or {}).get("writer")
        if holder:
            raise LauncherError(f"a gold maintenance job (attempt {holder}) holds the maintenance fence; retry when it has ended", "refused")

    def m_register_set(self, ctx: Ctx) -> dict[str, Any]:
        """Turn a reservation into a registration of the pinned snapshot set [777]."""
        self.require_no_maintenance_fence(ctx.state)
        reader = str(ctx.params["reader"])
        if reader not in ctx.state["readers"]:
            raise LauncherError(f"reader {reader} has no reservation", "refused")
        ctx.state["readers"][reader].update({"snapshot_set": str(ctx.params["snapshot_set"]), "state": "registered", "at": utc_now()})
        return {"ok": True}

    def m_deregister_set(self, ctx: Ctx) -> dict[str, Any]:
        """Remove a snapshot-set reader; a stale one only with the owner's confirmation [777]."""
        reader = str(ctx.params["reader"])
        entry = ctx.state["readers"].get(reader)
        if entry is None:
            return {"ok": True}
        if run_lock.is_stale(entry) and not ctx.params.get("owner_confirmed"):
            raise LauncherError(f"reader {reader} is stale; needs --owner-confirmed", "refused")
        ctx.state["readers"].pop(reader)
        return {"ok": True}

    def m_accept_consumer(self, ctx: Ctx) -> dict[str, Any]:
        """Accept the consumer result of the run's finished export by the one rule of the publish steps and record the exact result (D1 section 10 item 6).

        The launcher reads the result file itself; the client names only the attempt. The record holds the digest of the bytes it read, and every publish
        step is admitted and rendered against that digest only. A result that the rule rejects records nothing, and a result file that changes later no
        longer equals the record, so no step runs on it.
        """
        owner = self.owner_of(ctx)
        _, _, run_id = owner.partition(":")
        run = ctx.state["runs"][run_id]
        if run["state"] != "held":
            raise LauncherError(f"run {run_id} is {run['state']}; only a held run accepts a consumer result", "refused")
        attempt_id = str(ctx.params.get("attempt_id", ""))
        attempt = ctx.state["attempts"].get(attempt_id)
        if attempt is None or attempt["owner"] != owner or attempt["template"] != "gold_export":
            raise LauncherError("the attempt is not a gold_export attempt of this run", "refused")
        if attempt["phase"] != "ended" or attempt.get("reason") != "exited" or attempt.get("exit_code") != 0:
            raise LauncherError("the export attempt has not ended with exit code 0; its result is not accepted", "refused")
        completion = completion_of(run)
        folder = self.cfg.runs_root / run_id / "publish"
        digest = run_file_sha256(folder, gold_publish_record.RESULT_NAME)
        result, read_digest = gold_publish_record.read_record(folder / gold_publish_record.RESULT_NAME)
        if read_digest != digest:
            raise LauncherError("the consumer result changed while the launcher read it", "refused")
        try:
            gold_publish_record.accept_consumer(result, completion, run_id)
        except gold_publish_record.PublishError as error:
            raise LauncherError(str(error), "refused") from error
        run["consumer_acceptance"] = {
            "attempt_id": attempt_id,
            "result_sha256": digest,
            "manifest_sha256": str(result["manifest_sha256"]),
            "sealed_sha256": completion["sha256"],
            "accepted_at": utc_now(),
        }
        return {"accepted": True, "result_sha256": digest}

    def mac_write_problem(self, state: Mapping[str, Any], owner: str, template_name: str, params: Mapping[str, Any], args: Sequence[str]) -> str | None:
        """Why catalog authority refuses this Mac catalog write, or None. A registry without an authority record is still the Mac writer."""
        record = state["launcher"].get("catalog_authority")
        kind, _, ident = owner.partition(":")
        mode = str(state["runs"].get(ident, {}).get("mode", "")) if kind == "run" else ""
        if record is None or not mac_catalog_write(template_name, params, args, mode):
            return None
        try:
            catalog_authority.check_mac_start(record)
        except catalog_authority.AuthorityError as error:
            return f"catalog authority is {record.get('state')} (cutover {record.get('cutover_id')}): {error}"
        return None

    def m_catalog_authority(self, ctx: Ctx) -> dict[str, Any]:
        """Apply one owner command to the catalog authority record in this registry transaction (bronze cutover, failure modes F1 to F5).

        The fence step (freezing to source_fenced) is refused while any Mac catalog write is queued or running; the caller repeats it within
        its own deadline. A writer is never stopped to finish a freeze. A repeated command ID returns the stored record unchanged.
        """
        body = ctx.params.get("command")
        if not isinstance(body, dict):
            raise LauncherError("command must be an object", "args")
        current = ctx.state["launcher"].get("catalog_authority")
        if body.get("action") == "transition" and body.get("state") == "source_fenced" and current and current.get("state") == "freezing":
            open_writes = sorted(
                str(a["id"])
                for a in ctx.state["attempts"].values()
                if is_open(a) and "execution" not in a and self.mac_write_problem(ctx.state, str(a["owner"]), str(a["template"]), a["params"], a["args"])
            )
            if open_writes:
                raise LauncherError(f"Mac catalog writes are still open: {', '.join(open_writes)}", "refused")
        try:
            record = catalog_authority.owner_command(
                ctx.state,
                body,
                intent_directory=self.cfg.registry.path / "catalog_authority_intents",
                owner=bool(ctx.params.get("owner") or ctx.principal.get("owner")),
                tty=ctx.params.get("tty") is True,
                confirmation=str(ctx.params.get("confirmation", "")),
            )
        except catalog_authority.AuthorityError as error:
            raise LauncherError(str(error), "refused") from None
        except ValueError as error:
            raise LauncherError(f"catalog authority intent: {error}", "args") from None
        return record

    def m_shutdown(self, ctx: Ctx) -> dict[str, Any]:
        """Stop the launcher (owner only). A request that names a launch nonce stops only the instance started with it, so a client that
        started one instance cannot stop a replacement another client started on the same registry (failure mode 919)."""
        expected = ctx.params.get("launch_nonce")
        if expected is not None and (not LAUNCH_NONCE or expected != LAUNCH_NONCE):
            raise LauncherError("shutdown names another launcher instance (its launch nonce differs); this launcher keeps serving", "refused")
        self.running = False
        return {"ok": True}

    # -- the catalog services and the Docker restart (D1 section 8) -----------------------------------------------

    SERVICE_MEMORY = {"POLARIS_DB_MEMORY_LIMIT": ("polaris-db", 0.10, 256 * 1024**2), "POLARIS_MEMORY_LIMIT": ("polaris", 0.25, 1024**3)}
    SERVICE_ORDER = ("polaris-db", "polaris-bootstrap", "polaris")

    def catalog_record(self, state: dict[str, Any]) -> dict[str, Any]:
        """The catalog's service state in the registry."""
        return state["launcher"].setdefault("catalog", {"phase": "down", "services": {}})

    def pipeline_inputs(self, state: Mapping[str, Any], run: Mapping[str, Any]) -> dict[str, Any]:
        """Expose only trusted pipeline metadata to a renderer, without run tokens or unrelated credentials."""
        snapshot = state["copies"].get(run.get("snapshot"), {})
        return {
            "registry_kind": self.registry.kind,
            "source_sha256": snapshot.get("dbt_tree_sha256", ""),
            "bronze_sha256": run.get("pins_sha256", run.get("fixture_pins_sha256", "")),
            "silver_pin": run.get("silver_pin", {}),
            "silver_input": run.get("silver_input", {}),
            "layer_completions": run.get("layer_completions", {}),
            "layer_exports": run.get("layer_exports", {}),
            "local_catalog": run.get("local_catalog", ""),
            "network": run.get("pipeline_network", {}),
        }

    @staticmethod
    def pipeline_params(ctx: Ctx, allowed: tuple[str, ...] = ()) -> None:
        """Reject unsupported fields on a pipeline lifecycle method."""
        if set(ctx.params) != set(allowed):
            raise LauncherError("pipeline method parameters do not match its closed contract", "args")

    def m_pipeline_network_up(self, ctx: Ctx) -> dict[str, Any]:
        """Provision owned class networks and start their admitted credential-free proxy."""
        self.pipeline_params(ctx)
        run = self.run_entry(ctx)
        if run["state"] != "held":
            raise LauncherError("only a held run starts class networks", "refused")
        current = ctx.state["runs"][run["run_id"]].get("pipeline_network")
        if current and current.get("generation") != run.get("generation", 1):
            raise LauncherError("class networks belong to another run generation", "refused")
        if current and current["phase"] in ("networks_ready", "ready"):
            proxy = ctx.state["attempts"].get(current.get("proxy"))
            if proxy is None or proxy["phase"] == "ended":
                current["phase"] = "failed"
                return {
                    "phase": "failed",
                    "attempt_id": current.get("proxy"),
                    "error": "the class proxy did not remain running",
                    "proxy": {key: proxy.get(key) for key in ("reason", "exit_code", "detail")} if proxy else {},
                }
            if proxy["phase"] == "started":
                current["phase"] = "ready"
            return {"phase": current["phase"]}
        if current and current["phase"] in ("failed", "releasing", "down"):
            raise LauncherError("class networks require cleanup before a new run", "refused")
        catalog_id = None
        if run["mode"] == "real":
            catalog = self.catalog_record(ctx.state)
            polaris = ctx.state["attempts"].get(catalog.get("services", {}).get("polaris"))
            if catalog["phase"] != "up" or polaris is None or not polaris.get("container_id"):
                raise LauncherError("the catalog is not ready for class attachment", "refused")
            catalog_id = str(polaris["container_id"])
        key = f"pipeline-net-up:{run['run_id']}:{run.get('generation', 1)}"
        if current is None:
            current = {
                "phase": "creating",
                "generation": run.get("generation", 1),
                "names": network_policy.names(self.registry_id, run["run_id"]),
                "catalog_id": catalog_id,
                "started_at": time.time(),
            }
            ctx.state["runs"][run["run_id"]]["pipeline_network"] = current
        if time.time() - current["started_at"] > 600:
            current["phase"] = "failed"
            raise LauncherError("class network startup exceeded 600 seconds", "timeout")
        if key not in self.inflight:
            region = "us-west-2" if self.registry.kind == "test" else self.renderer_for(self.cfg.root).compose_values(("AWS_REGION",), {})["AWS_REGION"]
            self.spawn(key, "pipeline_net", self.create_pipeline_networks, run["run_id"], dict(current), region)
        return {"phase": "creating"}

    def create_pipeline_networks(self, run_id: str, record: dict[str, Any], region: str) -> dict[str, Any]:
        """Worker: adopt exact owned networks, attach the catalog and compute the proxy's memory share."""
        budget = memory_budget.current(self.reader())
        policy = network_policy.load(self.cfg.root / "services/orchestration/egress_proxy.json")
        limit = memory_budget.service_limit(budget, float(policy["memory_share"]), int(policy["memory_floor_bytes"]))
        labels = {LABEL_REGISTRY: self.registry_id, network_policy.LABEL_RUN: run_id}
        deadline = time.monotonic() + 600
        identities = {}
        for role, name in record["names"].items():
            if time.monotonic() >= deadline:
                raise TimeoutError("class network provisioning exceeded 600 seconds")
            found = [item for item in self.docker.networks(self.network_label()) if item.get("Name") == name]
            if len(found) > 1:
                raise LauncherError("multiple owned networks have the class name", "refused")
            identity = str(found[0]["Id"]) if found else self.docker.create_network(name, labels, internal=role != "egress")
            inspected = self.docker.call("GET", f"/networks/{identity}")
            network_policy.check_network(inspected, name, self.registry_id, run_id, role != "egress")
            identities[role] = identity
            catalog_id = record.get("catalog_id")
            if catalog_id and role != "egress" and catalog_id not in (inspected.get("Containers") or {}):
                self.docker.connect_network(identity, catalog_id, ["polaris"])
        folder = self.cfg.runs_root / run_id / "egress"
        folder.mkdir(parents=True, exist_ok=True)
        config = folder / "squid.conf"
        expected = network_policy.squid_config(region)
        if config.exists() or config.is_symlink():
            if config.is_symlink() or config.read_text() != expected:
                raise LauncherError("the proxy allowlist changed during startup", "refused")
        else:
            with config.open("x") as writer:
                writer.write(expected)
                writer.flush()
                os.fsync(writer.fileno())
            config.chmod(0o444)
        return {"ids": identities, "memory": limit}

    def m_pipeline_network_down(self, ctx: Ctx) -> dict[str, Any]:
        """Stop only this run's proxy before removing its owned network attachments."""
        self.pipeline_params(ctx)
        run = self.run_entry(ctx)
        record = ctx.state["runs"][run["run_id"]].get("pipeline_network")
        if record is None or record["phase"] == "down":
            return {"phase": "down"}
        up = f"pipeline-net-up:{run['run_id']}:{record['generation']}"
        if up in self.inflight:
            return {"phase": "draining"}
        catalog_probe = ctx.state["attempts"].get(record.get("catalog_probe"))
        if catalog_probe is not None and is_open(catalog_probe):
            if not catalog_probe["cancel_intent"]:
                catalog_probe["cancel_intent"] = {"reason": "pipeline_release", "at": utc_now()}
                self.dirty.add(catalog_probe["id"])
            return {"phase": "draining"}
        proxy = ctx.state["attempts"].get(record.get("proxy"))
        if proxy is not None and is_open(proxy):
            if not proxy["cancel_intent"]:
                proxy["cancel_intent"] = {"reason": "pipeline_release", "at": utc_now()}
            return {"phase": "draining"}
        record["phase"] = "releasing"
        key = f"pipeline-net-down:{run['run_id']}:{record['generation']}"
        if key not in self.inflight:
            self.spawn(key, "pipeline_net", self.remove_pipeline_networks, run["run_id"], dict(record))
        return {"phase": "releasing"}

    def remove_pipeline_networks(self, run_id: str, record: dict[str, Any]) -> dict[str, Any]:
        """Worker: detach the known catalog and refuse deletion of any foreign or still-used network."""
        for role, name in record["names"].items():
            found = [item for item in self.docker.networks(self.network_label()) if item.get("Name") == name]
            if not found:
                continue
            if len(found) != 1:
                raise LauncherError("class network cleanup found ambiguous ownership", "refused")
            identity = str(found[0]["Id"])
            inspected = self.docker.call("GET", f"/networks/{identity}")
            network_policy.check_network(inspected, name, self.registry_id, run_id, role != "egress")
            catalog_id = record.get("catalog_id")
            if catalog_id and catalog_id in (inspected.get("Containers") or {}):
                self.docker.call("POST", f"/networks/{identity}/disconnect", {"Container": catalog_id, "Force": False})
            self.docker.remove_network(identity)
        return {"removed": True}

    def on_pipeline_net(self, key: str, outcome: str, value: Any) -> None:
        """Publish a network result only to the run generation that requested it."""
        operation, run_id, generation = key.split(":", 2)
        with self.tx() as state:
            run = state["runs"].get(run_id)
            record = run.get("pipeline_network") if run else None
            if run is None or not isinstance(record, dict) or str(record["generation"]) != generation:
                return
            if outcome != "ok":
                record.update({"phase": "failed", "error": f"class network operation failed: {value}"})
                return
            service = f"egress-{run_id.lower()}"
            if operation == "pipeline-net-down":
                record["phase"] = "down"
                state["services"].pop(service, None)
                state["services"].pop(f"catalog-probe-{run_id.lower()}", None)
                return
            if run["state"] != "held" or run.get("generation", 1) != record["generation"]:
                record.update({"phase": "failed", "error": "the network owner changed while provisioning"})
                return
            record.update({"phase": "networks_ready", "ids": value["ids"]})
            state["services"][service] = value["memory"]
            proxy = self.new_attempt(
                state,
                f"service:pipeline:{run_id}",
                "pipeline_proxy",
                "egress",
                [],
                {"run_id": run_id},
                request="pipeline",
                checkout=str(self.cfg.root),
                seconds=86400,
            )
            proxy.update({"generation": record["generation"], "plan_env": {"JOB_MEMORY_LIMIT": str(value["memory"])}, "prepared": True})
            record["proxy"] = proxy["id"]

    def m_pipeline_test_catalog(self, ctx: Ctx) -> dict[str, Any]:
        """Start a credential-free positive-control server on both class networks of a test run."""
        self.pipeline_params(ctx)
        run = self.run_entry(ctx)
        record = ctx.state["runs"][run["run_id"]].get("pipeline_network", {})
        if self.registry.kind != "test" or run["mode"] != "publish_fixture" or record.get("phase") != "ready":
            raise LauncherError("the synthetic catalog requires ready networks on a publication test run", "refused")
        previous = ctx.state["attempts"].get(record.get("catalog_probe"))
        if previous is not None:
            return {"attempt_id": previous["id"], "phase": previous["phase"]}
        proxy = ctx.state["attempts"][record["proxy"]]
        memory = proxy["plan_env"]["JOB_MEMORY_LIMIT"]
        ctx.state["services"][f"catalog-probe-{run['run_id'].lower()}"] = int(memory)
        attempt = self.new_attempt(
            ctx.state,
            f"service:pipeline:{run['run_id']}",
            "pipeline_probe",
            "catalog-probe",
            [],
            {"class": "lakehouse_read", "case": "catalog"},
            request="pipeline",
            checkout=str(self.cfg.root),
            seconds=600,
        )
        attempt.update({"generation": record["generation"], "prepared": True, "plan_env": {"JOB_MEMORY_LIMIT": memory}})
        record["catalog_probe"] = attempt["id"]
        return {"attempt_id": attempt["id"], "phase": attempt["phase"]}

    def m_pipeline_catalog_up(self, ctx: Ctx) -> dict[str, Any]:
        """Register this run before starting a shared catalog, or prepare its isolated file catalog."""
        self.pipeline_params(ctx)
        run = self.run_entry(ctx)
        if run["state"] != "held" or ctx.state["launcher"].get("barrier"):
            raise LauncherError("catalog registration is refused during finalization or teardown", "refused")
        run_lock.register_in(ctx.state, run["run_id"], run, "catalog_user", True)
        if run["mode"] == "publish_fixture":
            if self.registry.kind != "test":
                raise LauncherError("publish_fixture requires an isolated test registry", "refused")
            folder = self.registry.path.parent / "pipeline_catalog"
            folder.mkdir(parents=True, exist_ok=True)
            if folder.is_symlink():
                raise LauncherError("the local catalog is a symlink", "refused")
            ctx.state["runs"][run["run_id"]]["local_catalog"] = str(folder)
            return {"phase": "up", "local": True}
        return self.m_catalog_up(ctx)

    def m_pipeline_catalog_release(self, ctx: Ctx) -> dict[str, Any]:
        """Deregister only this run and tear down only when no registered or active user remains."""
        self.pipeline_params(ctx)
        run = self.run_entry(ctx)
        reader = ctx.state["readers"].get(f"silver:{run['run_id']}")
        if reader is not None:
            if reader.get("owner_run") != run["run_id"]:
                raise LauncherError("the reader belongs to another run", "principal")
            self.require_no_maintenance_fence(ctx.state)
            ctx.state["readers"].pop(f"silver:{run['run_id']}")
        run_lock.register_in(ctx.state, run["run_id"], run, "catalog_user", False)
        if run["mode"] == "publish_fixture":
            return {"phase": "down", "local": True}
        if any("catalog_user" in entry.get("registrations", []) for entry in ctx.state["runs"].values()) or ctx.state["registrations"]:
            return {"phase": "up", "shared": True}
        if any(is_open(a) and TEMPLATES[a["template"]].needs_catalog for a in ctx.state["attempts"].values()):
            return {"phase": "up", "shared": True}
        return self.m_catalog_down(ctx)

    def m_pipeline_reserve_silver(self, ctx: Ctx) -> dict[str, Any]:
        """Reserve silver discovery before its producer is selected, bound to the consuming run."""
        self.pipeline_params(ctx)
        self.require_no_maintenance_fence(ctx.state)
        run = self.run_entry(ctx)
        name = f"silver:{run['run_id']}"
        previous = ctx.state["readers"].get(name)
        if previous is not None:
            if previous.get("owner_run") != run["run_id"]:
                raise LauncherError("the reservation belongs to another run", "principal")
            return {"reader": name, "state": previous["state"]}
        try:
            namespace = pipeline_protocol.silver_namespace(run["mode"])
        except pipeline_protocol.PipelineError as error:
            raise LauncherError(str(error), "refused") from None
        ctx.state["readers"][name] = {
            "owner_run": run["run_id"],
            "namespace": namespace,
            "domain": run["domain"],
            "publish_run": "latest",
            "snapshot_set": None,
            "state": "reserved",
            "pid": run["pid"],
            "pid_start": run["pid_start"],
            "at": utc_now(),
        }
        return {"reader": name, "state": "reserved"}

    def pipeline_attempt(self, ctx: Ctx, template: str, job: str | None = None) -> dict[str, Any]:
        """Require an uncancelled successful producer owned by this run and generation."""
        run = self.run_entry(ctx)
        attempt = ctx.state["attempts"].get(str(ctx.params["attempt_id"]))
        if (
            attempt is None
            or attempt["owner"] != f"run:{run['run_id']}"
            or attempt["template"] != template
            or attempt["generation"] != run.get("generation", 1)
        ):
            raise LauncherError("the producer attempt does not belong to this run generation", "refused")
        if attempt["phase"] != "ended" or attempt["reason"] != "exited" or attempt["exit_code"] != 0 or attempt["cancel_intent"]:
            raise LauncherError("the producer attempt did not finish successfully", "refused")
        if job is not None and attempt["params"].get("job") != job:
            raise LauncherError("the producer attempt ran another job", "refused")
        return attempt

    def m_pipeline_accept_seal(self, ctx: Ctx) -> dict[str, Any]:
        """Accept validated layer bytes and their successful frozen-source build receipt."""
        layer = str(ctx.params.get("layer", ""))
        self.pipeline_params(ctx, ("layer", "attempt_id", "cross_attempt_id") if layer == "gold" else ("layer", "attempt_id"))
        run = self.run_entry(ctx)
        folder = self.cfg.runs_root / run["run_id"]
        try:
            completion = pipeline_protocol.accept_validation(ctx.state, run["run_id"], layer, str(ctx.params["attempt_id"]), folder)
            receipt, receipt_sha = pipeline_protocol.read_record(folder, "build_receipt.json")
            metadata = self.pipeline_inputs(ctx.state, run)
            expected_input = metadata["silver_pin"].get("sha256") if layer == "gold" else metadata["bronze_sha256"]
            if (
                receipt.get("run_id") != run["run_id"]
                or receipt.get("layer") != layer
                or receipt.get("mode") != run["mode"]
                or receipt.get("source_sha256") != metadata["source_sha256"]
                or receipt.get("input_sha256") != expected_input
            ):
                raise pipeline_protocol.PipelineError("the build receipt differs from the frozen source or accepted inputs")
            builds = [
                a
                for a in ctx.state["attempts"].values()
                if a["owner"] == f"run:{run['run_id']}"
                and a["template"] == "pipeline_dbt"
                and a["params"] == {"layer": layer}
                and a["generation"] == run.get("generation", 1)
            ]
            latest = max(builds, key=lambda a: a["created_at"]) if builds else None
            if latest is None or latest["phase"] != "ended" or latest["reason"] != "exited" or latest["exit_code"] != 0 or latest["cancel_intent"]:
                raise pipeline_protocol.PipelineError("the latest frozen-source build did not succeed")
            output = pipeline_protocol.output_record(latest)
            if output.get("receipt_sha256") != receipt_sha:
                raise pipeline_protocol.PipelineError("the build receipt differs from the successful builder output")
            if layer == "gold":
                cross = ctx.state["attempts"].get(str(ctx.params["cross_attempt_id"]))
                if (
                    cross is None
                    or cross["owner"] != latest["owner"]
                    or cross["template"] != "pipeline_validate_cross"
                    or cross["phase"] != "ended"
                    or cross["exit_code"] != 0
                    or cross["reason"] != "exited"
                    or cross["cancel_intent"]
                    or cross["generation"] != latest["generation"]
                    or cross["params"].get("database") != str(folder / "staging.duckdb")
                    or cross["params"].get("out_host") != str(folder / "quality_cross")
                ):
                    raise pipeline_protocol.PipelineError("the cross-layer attempt did not succeed for this run")
                record, cross_sha = pipeline_protocol.read_record(folder, "quality_cross/result.json")
                if pipeline_protocol.output_record(cross).get("result_sha256") != cross_sha:
                    raise pipeline_protocol.PipelineError("the cross-layer result differs from its successful producer receipt")
                reconciliation = record.get("reconciliation", {})
                if (
                    record.get("build_file_sha256") != completion["sha256"]
                    or record.get("counts", {}).get("fail") != 0
                    or record.get("counts", {}).get("warn") != 0
                    or not reconciliation.get("facts")
                    or reconciliation.get("facts") != reconciliation.get("facts_in_contract")
                    or any(reconciliation.get("problems", {}).values())
                ):
                    raise pipeline_protocol.PipelineError("cross-layer checks did not accept exactly these build bytes")
            completion.update({"build_attempt": latest["id"], "receipt_sha256": receipt_sha})
        except (pipeline_protocol.PipelineError, OSError, ValueError, KeyError, IndexError) as error:
            raise LauncherError(f"layer seal refused: {error}", "refused") from None
        ctx.state["runs"][run["run_id"]].setdefault("layer_completions", {})[layer] = completion
        return {"layer": layer, "sha256": completion["sha256"]}

    def m_pipeline_accept_export(self, ctx: Ctx) -> dict[str, Any]:
        """Record the exact consumer-result bytes for a completed layer export."""
        self.pipeline_params(ctx, ("layer", "attempt_id"))
        run = self.run_entry(ctx)
        layer = str(ctx.params["layer"])
        try:
            accepted = pipeline_protocol.accept_export(ctx.state, run["run_id"], layer, str(ctx.params["attempt_id"]), self.cfg.runs_root / run["run_id"])
        except (pipeline_protocol.PipelineError, OSError, ValueError, KeyError, IndexError) as error:
            raise LauncherError(f"layer export refused: {error}", "refused") from None
        ctx.state["runs"][run["run_id"]].setdefault("layer_exports", {})[layer] = accepted
        return {"layer": layer, "sha256": accepted["sha256"]}

    def m_pipeline_accept_pin(self, ctx: Ctx) -> dict[str, Any]:
        """Register only the accepted committed silver set of this run's successful pin producer."""
        self.pipeline_params(ctx, ("attempt_id",))
        self.require_no_maintenance_fence(ctx.state)
        attempt = self.pipeline_attempt(ctx, "pipeline_spark", "pin_silver")
        run = self.run_entry(ctx)
        reader = ctx.state["readers"].get(f"silver:{run['run_id']}")
        if reader is None or reader.get("owner_run") != run["run_id"]:
            raise LauncherError("silver discovery has no owned reservation", "refused")
        try:
            record, digest = pipeline_protocol.read_record(self.cfg.runs_root / run["run_id"], "silver_pins.json")
            if pipeline_protocol.output_record(attempt).get("sha256") != digest:
                raise pipeline_protocol.PipelineError("the silver pin differs from its successful producer output")
            contract = json.loads((Path(attempt["checkout"]) / "data_contracts/lakehouse/silver_publish.json").read_text())
            pipeline_protocol.check_pin(record, run["run_id"], run["mode"], set(contract["publish"]["tables"]))
            selected = attempt["params"].get("source_run")
            if selected not in (None, "latest", record["source_run_id"]):
                raise pipeline_protocol.PipelineError("the pin producer selected another source run")
        except (pipeline_protocol.PipelineError, OSError, ValueError, KeyError, IndexError) as error:
            raise LauncherError(f"silver pin refused: {error}", "refused") from None
        previous = run.get("silver_pin")
        if previous and previous.get("sha256") != digest:
            raise LauncherError("this run already accepted another silver input", "refused")
        accepted = {"sha256": digest, "source_run_id": record["source_run_id"], "attempt_id": attempt["id"]}
        ctx.state["runs"][run["run_id"]]["silver_pin"] = accepted
        reader.update({"state": "registered", "snapshot_set": record["source_run_id"]})
        return accepted

    def m_pipeline_accept_input(self, ctx: Ctx) -> dict[str, Any]:
        """Bind the successful Iceberg extraction manifest to the accepted silver pin."""
        self.pipeline_params(ctx, ("attempt_id",))
        attempt = self.pipeline_attempt(ctx, "pipeline_spark", "read_silver")
        run = self.run_entry(ctx)
        try:
            record, digest = pipeline_protocol.read_record(self.cfg.runs_root / run["run_id"], "silver_input/manifest.json")
            if pipeline_protocol.output_record(attempt).get("sha256") != digest:
                raise pipeline_protocol.PipelineError("the silver extract differs from its successful producer output")
            if (
                record.get("run_id") != run["run_id"]
                or record.get("mode") != run["mode"]
                or record.get("pins_sha256") != run.get("silver_pin", {}).get("sha256")
            ):
                raise pipeline_protocol.PipelineError("the extract is not this run's accepted silver pin")
        except (pipeline_protocol.PipelineError, OSError, ValueError) as error:
            raise LauncherError(f"silver extraction refused: {error}", "refused") from None
        accepted = {"sha256": digest, "pins_sha256": record["pins_sha256"], "attempt_id": attempt["id"]}
        ctx.state["runs"][run["run_id"]]["silver_input"] = accepted
        return accepted

    def m_pipeline_fixture_pin(self, ctx: Ctx) -> dict[str, Any]:
        """Pin synthetic bronze file bytes in an isolated publication fixture."""
        self.pipeline_params(ctx)
        run = self.run_entry(ctx)
        if self.registry.kind != "test" or run["mode"] != "publish_fixture":
            raise LauncherError("a fixture bronze pin requires an isolated publication fixture", "refused")
        digest = pipeline_protocol.digest_file(self.cfg.runs_root / run["run_id"] / "fixture_lakehouse.duckdb")
        previous = run.get("fixture_pins_sha256")
        if previous and previous != digest:
            raise LauncherError("fixture bronze bytes changed after pinning", "refused")
        ctx.state["runs"][run["run_id"]]["fixture_pins_sha256"] = digest
        return {"sha256": digest}

    def m_pipeline_fixture_verify(self, ctx: Ctx) -> dict[str, Any]:
        """Refuse fixture publication after its pinned synthetic bronze bytes change."""
        self.pipeline_params(ctx)
        run = self.run_entry(ctx)
        digest = pipeline_protocol.digest_file(self.cfg.runs_root / run["run_id"] / "fixture_lakehouse.duckdb")
        if run["mode"] != "publish_fixture" or self.registry.kind != "test" or digest != run.get("fixture_pins_sha256"):
            raise LauncherError("fixture bronze bytes differ from the pin", "refused")
        return {"unchanged": True}

    def m_catalog_up(self, ctx: Ctx) -> dict[str, Any]:
        """Start the missing service attempts in dependency order; a barrier or a teardown refuses it (D1 section 8 item 2)."""
        catalog = self.catalog_record(ctx.state)
        if ctx.state["launcher"].get("barrier"):
            raise LauncherError("a catalog teardown barrier is held", "refused")
        if ctx.state["launcher"].get("restart"):
            raise LauncherError("a Docker restart is running", "refused")
        if self.foreign:
            raise LauncherError("foreign legacy containers exist; the owner reconciles them first", "refused")
        if refusal := self.catalog_checkout_refusal():
            raise LauncherError(refusal, "refused")
        if catalog["phase"] in ("up", "starting"):
            return {"phase": catalog["phase"]}
        if catalog["phase"] == "tearing_down":
            raise LauncherError("the catalog is being torn down", "refused")
        ctx.state["launcher"]["catalog"] = {
            "phase": "starting",
            "step": "limits",
            "services": {},
            "error": "",
            "network": None,
            "limits": {},
            "started_at": utc_now(),
        }
        self.spawn("catalog:limits", "cataloglimits", self.compute_catalog_limits)
        return {"phase": "starting"}

    def compute_catalog_limits(self) -> dict[str, int]:
        """Worker: each long-running service's limit, its share of the plan's free memory (Polaris 25%, its database 10%)."""
        budget = memory_budget.current(self.reader())
        return {variable: memory_budget.service_limit(budget, share, minimum) for variable, (_, share, minimum) in self.SERVICE_MEMORY.items()}

    def on_cataloglimits(self, key: str, outcome: str, value: Any) -> None:
        """The service limits are known: record them, register the services' limits for the growth reserve and create the network."""
        with self.tx() as state:
            catalog = self.catalog_record(state)
            if catalog["phase"] != "starting":
                return
            if outcome != "ok":
                catalog.update({"phase": "tearing_down", "after": "down", "error": f"no memory limit for the catalog services: {failure_text(value)}"})
                return
            catalog["limits"] = {variable: f"{limit // 1024**2}m" for variable, limit in value.items()}
            for variable, (service, _, _) in self.SERVICE_MEMORY.items():
                state["services"][service] = int(value[variable])
            catalog["step"] = "network"

    def network_label(self) -> str:
        """The label that marks the catalog network as this registry's."""
        return f"{LABEL_REGISTRY}={self.registry_id}"

    def service_attempt(self, state: dict[str, Any], catalog: dict[str, Any], service: str) -> dict[str, Any]:
        """The service's attempt, created on first use."""
        attempt_id = catalog["services"].get(service)
        if attempt_id is not None:
            return state["attempts"][attempt_id]
        seconds = 600 if service in FINITE_SERVICES else 10**7
        attempt = self.new_attempt(
            state,
            "service:catalog",
            "catalog_service",
            f"svc-{service}",
            [],
            {"service": service},
            request="launcher",
            checkout=str(self.cfg.root),
            seconds=seconds,
        )
        catalog["services"][service] = attempt["id"]
        return attempt

    def catalog_checkout_refusal(self) -> str:
        """Why this launcher may not start catalog services, or an empty string: the Compose binds resolve against the launcher's checkout,
        so from a checkout other than the catalog's the database would be a new, empty one (amendments A85 and A86)."""
        owner = self.cfg.catalog_checkout or self.cfg.root
        if self.cfg.root.resolve() == owner.resolve():
            return ""
        return (
            f"the catalog starts only from a launcher running in the main checkout ({owner}); this launcher runs from {self.cfg.root}. "
            "Stop it (shutdown --owner) and start the launcher from the main checkout"
        )

    def refuse_foreign_catalog_start(self, state: dict[str, Any]) -> None:
        """A recorded startup is never resumed from another checkout: the teardown removes what exists and keeps its operation records."""
        catalog = state["launcher"].get("catalog")
        if catalog and catalog["phase"] == "starting" and (refusal := self.catalog_checkout_refusal()):
            self.fail_catalog(catalog, refusal)

    def advance_catalog(self, state: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """Startup and teardown of the catalog services as a recorded state machine (D1 section 8 items 1 to 3)."""
        catalog = state["launcher"].get("catalog")
        if not catalog or not self.docker_ok or self.status != "serving":
            return
        self.refuse_foreign_catalog_start(state)
        if catalog["phase"] == "starting" and catalog.get("step") not in (None, "limits"):
            self.advance_catalog_start(state, catalog, actions)
        elif catalog["phase"] == "tearing_down":
            self.advance_catalog_stop(state, catalog, actions)

    def fail_catalog(self, catalog: dict[str, Any], message: str) -> None:
        """A failed startup tears the started services down and leaves the catalog down."""
        catalog.update({"phase": "tearing_down", "after": "down", "error": message})

    def advance_catalog_start(self, state: dict[str, Any], catalog: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """network, then the database, the one-time bootstrap, then Polaris."""
        step = catalog["step"]
        if step == "network":
            op = next((o for o in state["operations"].values() if o["kind"] == "network_create" and o["state"] == "open"), None)
            if op is None:
                op_id = self.record_network_op(state, "network_create")
            elif f"op:{op['op_id']}" in self.inflight:
                return
            else:
                op_id = op["op_id"]  # an open operation without a request in flight (a restart, a lost reply): the create is idempotent by name
            name = self.renderer_network_name()
            label = {LABEL_REGISTRY: self.registry_id}
            actions.append(lambda: self.spawn(f"op:{op_id}", "netcreate", self.create_network, name, label))
            return
        if step in ("db", "bootstrap", "polaris"):
            service = {"db": "polaris-db", "bootstrap": "polaris-bootstrap", "polaris": "polaris"}[step]
            if step == "bootstrap" and self.cfg.lakehouse_state.joinpath("polaris_bootstrapped").exists():
                catalog["step"] = "polaris"
                return
            attempt = self.service_attempt(state, catalog, service)
            if attempt["phase"] == "ended":
                if step == "bootstrap" and attempt["reason"] == "exited" and attempt.get("exit_code") == 0:
                    self.cfg.lakehouse_state.mkdir(parents=True, exist_ok=True)
                    self.cfg.lakehouse_state.joinpath("polaris_bootstrapped").write_text(
                        "Polaris realm POLARIS bootstrapped; delete only together with data/lakehouse/polaris_db.\n"
                    )
                    catalog["step"] = "polaris"
                    return
                self.fail_catalog(catalog, f"{service} ended: {attempt['reason']} (exit {attempt.get('exit_code')}) {attempt.get('detail', '')}".strip())
                return
            if step == "bootstrap" or attempt["phase"] != "started":
                return
            health = catalog.setdefault("health", {})
            if health.get(service) == "healthy":
                if step == "db":
                    catalog["step"] = "bootstrap"
                else:
                    catalog.update({"phase": "up", "step": "done", "up_at": utc_now()})
                return
            started = catalog.setdefault("waiting_since", {}).setdefault(service, time.time())
            if time.time() - started > self.timings.health_wait:
                self.fail_catalog(catalog, f"{service} was not healthy within {self.timings.health_wait:.0f} s")
                return
            container_id = attempt["container_id"]
            actions.append(lambda: self.spawn(f"health:{service}", "health", self.read_health, container_id))

    def record_network_op(self, state: dict[str, Any], kind: str, network: str | None = None, name: str | None = None) -> str:
        """Record a network operation with the identity it acts on (the network name, and the ID for a removal)."""
        op_id = new_id("op")
        state["operations"][op_id] = {
            "op_id": op_id,
            "attempt": None,
            "kind": kind,
            "expected_phase": None,
            "generation": 1,
            "dispatched_at": time.time(),
            "state": "open",
            "result": None,
            "network_name": name or self.renderer_network_name(),
            "network": network,
        }
        return op_id

    def settle_network_ops(self, networks: Sequence[Mapping[str, Any]]) -> None:
        """Settle open network operations from a scan: a create whose network exists, a removal whose network is gone (D1 section 4 item 2)."""
        name = self.renderer_network_name()
        found = [str(n["Id"]) for n in networks if n.get("Name") == name]
        with self.tx() as state:
            catalog = self.catalog_record(state)
            for op in list(state["operations"].values()):
                if op["state"] != "open" or op["kind"] not in ("network_create", "network_remove"):
                    continue
                if op["kind"] == "network_create" and found:
                    self.settle_op(state, op["op_id"], "created", evidence="scan")
                    if catalog["phase"] == "starting":
                        catalog.update({"network": found[0], "step": "db"})
                        state["containers"]["network:" + found[0]] = {"kind": "network", "at": utc_now()}
                    elif catalog["phase"] == "tearing_down":
                        catalog["network"] = found[0]  # the teardown removes it
                elif op["kind"] == "network_remove" and not found:
                    self.settle_op(state, op["op_id"], "removed", evidence="scan")
                    if catalog["phase"] == "tearing_down":
                        catalog["network"] = None
                        catalog["stop_step"] = "confirm"

    def renderer_network_name(self) -> str:
        """The catalog network's name."""
        return f"hai-{self.registry_id[:8]}-net"

    def create_network(self, name: str, labels: dict[str, str]) -> str:
        """Worker: create the catalog network, or find the one that exists from an earlier start."""
        try:
            return self.docker.create_network(name, labels)
        except DockerError as error:
            if error.status != 409:
                raise
            found = [n for n in self.docker.networks(self.network_label()) if n.get("Name") == name]
            if not found:
                raise
            return str(found[0]["Id"])

    def on_netcreate(self, key: str, outcome: str, value: Any) -> None:
        """The network exists: the database starts next."""
        op_id = key.split(":", 1)[1]
        if outcome == "docker" and value.status == 0:
            return
        with self.tx() as state:
            catalog = self.catalog_record(state)
            op = state["operations"].get(op_id)
            if op is None or op["state"] != "open":
                return  # a scan already settled it
            self.settle_op(state, op_id, "created" if outcome == "ok" else f"failed: {failure_text(value)}")
            if catalog["phase"] == "tearing_down" and outcome == "ok":
                catalog["network"] = value  # settled during a teardown: the teardown removes it
                return
            if catalog["phase"] != "starting":
                return
            if outcome != "ok":
                self.fail_catalog(catalog, f"cannot create the catalog network: {failure_text(value)}")
                return
            catalog.update({"network": value, "step": "db"})
            state["containers"]["network:" + str(value)] = {"kind": "network", "at": utc_now()}

    def read_health(self, container_id: str) -> str:
        """Worker: the container's health status."""
        info = self.docker.inspect(container_id)
        return "gone" if info is None else str(((info.get("State") or {}).get("Health") or {}).get("Status", "none"))

    def on_health(self, key: str, outcome: str, value: Any) -> None:
        """Record a service's health."""
        if outcome != "ok":
            return
        service = key.split(":", 1)[1]
        with self.tx() as state:
            self.catalog_record(state).setdefault("health", {})[service] = value

    def advance_catalog_stop(self, state: dict[str, Any], catalog: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """Stop and clean up the service attempts in reverse order, remove the network, then confirm absence before clearing the barrier."""
        for service in reversed(self.SERVICE_ORDER):
            attempt_id = catalog["services"].get(service)
            attempt = state["attempts"].get(attempt_id) if attempt_id else None
            if attempt is not None and is_open(attempt):
                if not attempt["cancel_intent"]:
                    attempt["cancel_intent"] = {"reason": "catalog_down", "at": utc_now()}
                return
        step = catalog.get("stop_step", "network")
        create = next((o for o in state["operations"].values() if o["kind"] == "network_create" and o["state"] == "open"), None)
        if step == "network" and create is not None:
            # A create left open by a startup that never finished (a restart, a lost reply, a refused resume): settle it first, by name, so a
            # network it made is known and removed rather than left behind (review C3a-1).
            if f"op:{create['op_id']}" not in self.inflight:
                op_id = create["op_id"]
                name, label = self.renderer_network_name(), {LABEL_REGISTRY: self.registry_id}
                actions.append(lambda: self.spawn(f"op:{op_id}", "netcreate", self.create_network, name, label))
            return
        if step == "network":
            network = catalog.get("network")
            op = next((o for o in state["operations"].values() if o["kind"] == "network_remove" and o["state"] == "open"), None)
            if network is None:
                catalog["stop_step"] = "confirm"
                return
            if op is None:
                op_id = self.record_network_op(state, "network_remove", network)
            elif f"op:{op['op_id']}" in self.inflight:
                return
            else:
                op_id = op["op_id"]  # resumed after a restart or a lost reply: removing a network that is gone is not an error
                network = op.get("network") or network
            actions.append(lambda: self.spawn(f"op:{op_id}", "netremove", self.docker.remove_network, network))
        elif step == "confirm":
            actions.append(lambda: self.spawn("catalog:confirm", "catalogconfirm", self.catalog_absent))

    def catalog_absent(self) -> bool:
        """Worker: True when no catalog service container and no catalog network exist."""
        containers = [c for c in self.docker.containers(label=LABEL_SERVICE) if (c.get("Labels") or {}).get(LABEL_REGISTRY) == self.registry_id]
        networks = [n for n in self.docker.networks(self.network_label()) if (n.get("Labels") or {}).get(LABEL_STACK) != ORCHESTRATION_STACK]
        return not containers and not networks

    def on_netremove(self, key: str, outcome: str, value: Any) -> None:
        """The network is gone: confirm absence next."""
        op_id = key.split(":", 1)[1]
        if outcome == "docker" and value.status == 0:
            return
        with self.tx() as state:
            op = state["operations"].get(op_id)
            if op is None or op["state"] != "open":
                return  # a scan already settled it
            self.settle_op(state, op_id, "removed" if outcome == "ok" else f"failed: {failure_text(value)}")
            catalog = self.catalog_record(state)
            if outcome == "ok" and catalog["phase"] == "tearing_down":
                catalog["network"] = None
                catalog["stop_step"] = "confirm"

    def on_catalogconfirm(self, key: str, outcome: str, value: Any) -> None:
        """Absence shown by an all-state listing: the barrier clears and the catalog is down."""
        if outcome != "ok" or not value:
            return
        with self.tx() as state:
            catalog = self.catalog_record(state)
            if catalog["phase"] != "tearing_down":
                return
            error = catalog.get("error", "")
            state["launcher"]["catalog"] = {"phase": "down", "services": {}, "error": error, "down_at": utc_now()}
            state["launcher"]["barrier"] = None
            for service in self.SERVICE_ORDER:
                state["services"].pop(service, None)

    def m_catalog_down(self, ctx: Ctx) -> dict[str, Any]:
        """Record a teardown barrier before any side effect (D1 section 8 item 3)."""
        catalog = self.catalog_record(ctx.state)
        if any("catalog_user" in run.get("registrations", []) for run in ctx.state["runs"].values()):
            raise LauncherError("catalog users remain registered", "refused")
        if catalog["phase"] == "down":
            return {"phase": "down"}
        if ctx.state["launcher"].get("barrier"):
            return {"phase": "tearing_down"}
        readers = [r for r in ctx.state["registrations"].values() if r["kind"] == "reader"]
        if readers:
            raise LauncherError(f"catalog_down is refused: reader registration {readers[0]['id']} exists", "refused")
        users = [a for a in ctx.state["attempts"].values() if is_open(a) and TEMPLATES[a["template"]].needs_catalog]
        if users:
            raise LauncherError(f"catalog_down is refused: attempt {users[0]['id']} ({users[0]['template']}) needs the catalog", "refused")
        if catalog["phase"] == "starting":
            raise LauncherError("catalog_down is refused: a catalog startup is open", "refused")
        ctx.state["launcher"]["barrier"] = {"phase": "stopping", "at": utc_now()}
        catalog.update({"phase": "tearing_down", "after": "down", "stop_step": "network"})
        return {"phase": "tearing_down"}

    def orchestration_record(self, state: dict[str, Any]) -> dict[str, Any]:
        """The orchestration stack's state in the registry."""
        return state["launcher"].setdefault("orchestration", {"phase": "down", "services": {}})

    def orchestration_network_name(self) -> str:
        """The orchestration stack's network, apart from the catalog's."""
        return f"hai-{self.registry_id[:8]}-orch-net"

    def orchestration_state_folder(self, checkout: Path, text: Any) -> str:
        """The stack's state folder: absolute, inside ``data/orchestration/`` of the checkout or the main checkout (or a test registry's named
        folder), outside any secrets folder and holding the metadata and session folders the supervisor wrote."""
        if not isinstance(text, str) or not Path(text).is_absolute():
            raise LauncherError("state must be an absolute folder", "args")
        candidate = Path(text)
        if secrets_part(folded(candidate)) or secrets_part(folded(candidate.resolve())):
            raise LauncherError(f"state {text!r} lies in a secrets folder", "mount")
        roots = [checkout / "data" / "orchestration", self.main_checkout / "data" / "orchestration", *self.cfg.allowed_roots]
        if not inside(candidate, roots):
            raise LauncherError(f"state {text!r} is outside the allowed folders", "args")
        resolved = candidate.resolve()
        if not resolved.is_dir():
            raise LauncherError(f"state folder {text!r} does not exist", "args")
        missing = [name for name in ORCHESTRATION_STATE_FOLDERS if not (resolved / name).is_dir()]
        if missing:
            raise LauncherError(f"state folder {text!r} lacks {', '.join(missing)}", "args")
        return str(resolved)

    def m_orchestration_up(self, ctx: Ctx) -> dict[str, Any]:
        """Start the orchestration stack: the network, PostgreSQL, the migration and the four Airflow services, each waited for in order."""
        params = ctx.params
        unknown = sorted(set(params) - set(ORCHESTRATION_UP_PARAMETERS))
        if unknown:
            raise LauncherError(f"orchestration_up has no parameter {', '.join(unknown)}", "args")
        port = params.get("api_port")
        if type(port) is not int or not 1024 <= port <= 65535:
            raise LauncherError("api_port must be an integer from 1024 to 65535", "args")
        checkout = self.check_checkout(str(params.get("checkout") or self.cfg.root))
        if not (checkout / ORCHESTRATION_COMPOSE).is_file():
            raise LauncherError(f"{checkout} has no {ORCHESTRATION_COMPOSE}", "args")
        folder = self.orchestration_state_folder(checkout, params.get("state"))
        stack = self.orchestration_record(ctx.state)
        if stack["phase"] == "tearing_down":
            raise LauncherError("the orchestration stack is being torn down", "refused")
        if ctx.state["launcher"].get("restart"):
            raise LauncherError("a Docker restart is running", "refused")
        if self.foreign:
            raise LauncherError("foreign legacy containers exist; the owner reconciles them first", "refused")
        if stack["phase"] in ("up", "starting"):
            wanted = {"checkout": str(checkout), "state": folder, "api_port": port}
            if {name: stack.get(name) for name in wanted} != wanted:
                raise LauncherError("the orchestration stack is up with other parameters; orchestration_down first", "refused")
            return {"phase": stack["phase"]}
        ctx.state["launcher"]["orchestration"] = {
            "phase": "starting",
            "step": "limits",
            "services": {},
            "error": "",
            "network": None,
            "settings": {},
            "health": {},
            "checkout": str(checkout),
            "state": folder,
            "api_port": port,
            "started_at": utc_now(),
        }
        own = f"hai-{self.registry_id[:8]}-svc-orch-"
        growth = {name: limit for name, limit in self.growth_reserve_map(ctx.state).items() if not name.startswith(own)}
        self.spawn("orch:limits", "orchlimits", self.compute_orchestration_limits, growth)
        return {"phase": "starting"}

    def compute_orchestration_limits(self, growth: dict[str, int]) -> dict[str, Any]:
        """Worker: each service's limit as its share of the plan's free memory, with the LocalExecutor parallelism the scheduler's limit allows.

        One resource snapshot gives the plan; the services run at the same time, so the sum of their limits (each raised to its minimum) must fit the
        plan's free memory, or the start is refused here, before any network or container exists."""
        plan = memory_budget.launch_plan(self.reader(), growth)
        limits = {service: memory_budget.service_limit(plan.budget, share, minimum) for service, (_, share, minimum) in ORCHESTRATION_MEMORY.items()}
        total = sum(limits.values())
        if total > plan.budget.free:
            need, have = total / memory_budget.GIB, plan.budget.free / memory_budget.GIB
            raise memory_budget.BudgetError(f"the orchestration services need {need:.2f} GiB at the same time and the plan holds {have:.2f} GiB free")
        workers = (limits["airflow-scheduler"] - ORCHESTRATION_SCHEDULER_BASE) // ORCHESTRATION_WORKER_MEMORY
        return {"limits": limits, "parallelism": max(1, min(int(plan.threads), int(workers)))}

    def on_orchlimits(self, key: str, outcome: str, value: Any) -> None:
        """The limits are known: record them with the launcher's validated settings, register the services' limits and create the network."""
        with self.tx() as state:
            stack = self.orchestration_record(state)
            if stack["phase"] != "starting":
                return
            if outcome != "ok":
                self.fail_orchestration(stack, f"no memory limit for the orchestration services: {failure_text(value)}")
                return
            settings = {
                "HAI_ORCH_REPO": stack["checkout"],
                "HAI_ORCH_STATE": stack["state"],
                "HAI_ORCH_API_PORT": str(stack["api_port"]),
                "HAI_ORCH_PARALLELISM": str(value["parallelism"]),
            }
            for service, (name, _, _) in ORCHESTRATION_MEMORY.items():
                settings[f"HAI_ORCH_MEM_{name}"] = str(value["limits"][service])
                state["services"][f"orch-{service}"] = int(value["limits"][service])
            stack["settings"] = settings
            stack["step"] = "network"

    def fail_orchestration(self, stack: dict[str, Any], message: str) -> None:
        """A failed start tears the started services down and leaves the stack down."""
        stack.update({"phase": "tearing_down", "after": "down", "error": message, "stop_step": "network"})

    def stack_attempt(self, state: dict[str, Any], stack: dict[str, Any], service: str) -> dict[str, Any]:
        """The service's attempt, created on first use."""
        attempt_id = stack["services"].get(service)
        if attempt_id is not None:
            return state["attempts"][attempt_id]
        seconds = 900 if service in ORCHESTRATION_FINITE else 10**7
        attempt = self.new_attempt(
            state,
            ORCHESTRATION_OWNER,
            "orchestration_service",
            f"orch-{service}",
            [],
            {"service": service},
            request="launcher",
            checkout=stack["checkout"],
            seconds=seconds,
        )
        stack["services"][service] = attempt["id"]
        return attempt

    def advance_orchestration(self, state: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """Startup and teardown of the orchestration stack as a recorded state machine, beside the catalog's and sharing none of its records."""
        stack = state["launcher"].get("orchestration")
        if not stack or not self.docker_ok or self.status != "serving":
            return
        if stack["phase"] == "starting" and stack.get("step") not in (None, "limits"):
            self.advance_orchestration_start(state, stack, actions)
        elif stack["phase"] == "tearing_down":
            self.advance_orchestration_stop(state, stack, actions)
        elif stack["phase"] == "up":
            ended = [
                service
                for service, attempt_id in stack["services"].items()
                if service not in ORCHESTRATION_FINITE and state["attempts"][attempt_id]["phase"] == "ended"
            ]
            stack["degraded"] = f"{', '.join(sorted(ended))} ended" if ended else ""

    def advance_orchestration_start(self, state: dict[str, Any], stack: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """The network, then PostgreSQL, the migration and each Airflow service in order; a long-running service must be healthy first."""
        step = stack["step"]
        if step == "network":
            op = next((o for o in state["operations"].values() if o["kind"] == "orch_network_create" and o["state"] == "open"), None)
            if op is None:
                op_id = self.record_network_op(state, "orch_network_create", name=self.orchestration_network_name())
            elif f"op:{op['op_id']}" in self.inflight:
                return
            else:
                op_id = op["op_id"]
            name, labels = self.orchestration_network_name(), {LABEL_REGISTRY: self.registry_id, LABEL_STACK: ORCHESTRATION_STACK}
            actions.append(lambda: self.spawn(f"op:{op_id}", "orchnetcreate", self.create_network, name, labels))
            return
        if step not in ORCHESTRATION_ORDER:
            return
        service = step
        attempt = self.stack_attempt(state, stack, service)
        following = ORCHESTRATION_ORDER[ORCHESTRATION_ORDER.index(service) + 1 :]
        if attempt["phase"] == "ended":
            if service in ORCHESTRATION_FINITE and attempt["reason"] == "exited" and attempt.get("exit_code") == 0:
                stack["step"] = following[0]
                return
            self.fail_orchestration(stack, f"{service} ended: {attempt['reason']} (exit {attempt.get('exit_code')}) {attempt.get('detail', '')}".strip())
            return
        if service in ORCHESTRATION_FINITE or attempt["phase"] != "started":
            return
        if stack["health"].get(service) == "healthy":
            if following:
                stack["step"] = following[0]
            else:
                stack.update({"phase": "up", "step": "done", "up_at": utc_now()})
            return
        started = stack.setdefault("waiting_since", {}).setdefault(service, time.time())
        if time.time() - started > self.timings.health_wait:
            self.fail_orchestration(stack, f"{service} was not healthy within {self.timings.health_wait:.0f} s")
            return
        container_id = attempt["container_id"]
        actions.append(lambda: self.spawn(f"orchhealth:{service}", "orchhealth", self.read_health, container_id))

    def on_orchhealth(self, key: str, outcome: str, value: Any) -> None:
        """Record a stack service's health."""
        if outcome != "ok":
            return
        with self.tx() as state:
            self.orchestration_record(state).setdefault("health", {})[key.split(":", 1)[1]] = value

    def on_orchnetcreate(self, key: str, outcome: str, value: Any) -> None:
        """The stack's network exists: PostgreSQL starts next."""
        op_id = key.split(":", 1)[1]
        if outcome == "docker" and value.status == 0:
            return
        with self.tx() as state:
            stack = self.orchestration_record(state)
            op = state["operations"].get(op_id)
            if op is None or op["state"] != "open":
                return
            self.settle_op(state, op_id, "created" if outcome == "ok" else f"failed: {failure_text(value)}")
            if stack["phase"] == "tearing_down" and outcome == "ok":
                stack["network"] = value
                return
            if stack["phase"] != "starting":
                return
            if outcome != "ok":
                self.fail_orchestration(stack, f"cannot create the orchestration network: {failure_text(value)}")
                return
            stack.update({"network": value, "step": ORCHESTRATION_ORDER[0]})
            state["containers"]["network:" + str(value)] = {"kind": "network", "at": utc_now()}

    def settle_orchestration_network_ops(self, networks: Sequence[Mapping[str, Any]]) -> None:
        """Settle open orchestration network operations from a scan: a create whose network exists, a removal whose network is gone."""
        name = self.orchestration_network_name()
        found = [str(n["Id"]) for n in networks if n.get("Name") == name]
        with self.tx() as state:
            stack = self.orchestration_record(state)
            for op in list(state["operations"].values()):
                if op["state"] != "open" or op["kind"] not in ("orch_network_create", "orch_network_remove"):
                    continue
                if op["kind"] == "orch_network_create" and found:
                    self.settle_op(state, op["op_id"], "created", evidence="scan")
                    if stack["phase"] == "starting":
                        stack.update({"network": found[0], "step": ORCHESTRATION_ORDER[0]})
                        state["containers"]["network:" + found[0]] = {"kind": "network", "at": utc_now()}
                    elif stack["phase"] == "tearing_down":
                        stack["network"] = found[0]
                elif op["kind"] == "orch_network_remove" and not found:
                    self.settle_op(state, op["op_id"], "removed", evidence="scan")
                    if stack["phase"] == "tearing_down":
                        stack.update({"network": None, "stop_step": "confirm"})

    def advance_orchestration_stop(self, state: dict[str, Any], stack: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """Stop the Airflow services together, then PostgreSQL, remove the network, then confirm absence before the stack is down."""
        for group in (tuple(s for s in ORCHESTRATION_ORDER if s != "postgres"), ("postgres",)):
            open_attempts = [
                state["attempts"][stack["services"][s]] for s in group if s in stack["services"] and is_open(state["attempts"][stack["services"][s]])
            ]
            for attempt in open_attempts:
                if not attempt["cancel_intent"]:
                    attempt["cancel_intent"] = {"reason": "orchestration_down", "at": utc_now()}
            if open_attempts:
                return
        step = stack.get("stop_step", "network")
        create = next((o for o in state["operations"].values() if o["kind"] == "orch_network_create" and o["state"] == "open"), None)
        if step == "network" and create is not None:
            if f"op:{create['op_id']}" not in self.inflight:
                op_id = create["op_id"]
                name, labels = self.orchestration_network_name(), {LABEL_REGISTRY: self.registry_id, LABEL_STACK: ORCHESTRATION_STACK}
                actions.append(lambda: self.spawn(f"op:{op_id}", "orchnetcreate", self.create_network, name, labels))
            return
        if step == "network":
            network = stack.get("network")
            op = next((o for o in state["operations"].values() if o["kind"] == "orch_network_remove" and o["state"] == "open"), None)
            if network is None:
                stack["stop_step"] = "confirm"
                return
            if op is None:
                op_id = self.record_network_op(state, "orch_network_remove", network, name=self.orchestration_network_name())
            elif f"op:{op['op_id']}" in self.inflight:
                return
            else:
                op_id = op["op_id"]
                network = op.get("network") or network
            actions.append(lambda: self.spawn(f"op:{op_id}", "orchnetremove", self.docker.remove_network, network))
        elif step == "confirm":
            actions.append(lambda: self.spawn("orch:confirm", "orchconfirm", self.orchestration_absent))

    def orchestration_absent(self) -> bool:
        """Worker: True when no container and no network of the orchestration stack exist, in any state."""
        label = f"{LABEL_STACK}={ORCHESTRATION_STACK}"
        containers = [c for c in self.docker.containers(label=label) if (c.get("Labels") or {}).get(LABEL_REGISTRY) == self.registry_id]
        networks = [n for n in self.docker.networks(label) if (n.get("Labels") or {}).get(LABEL_REGISTRY) == self.registry_id]
        return not containers and not networks

    def on_orchnetremove(self, key: str, outcome: str, value: Any) -> None:
        """The stack's network is gone: confirm absence next."""
        op_id = key.split(":", 1)[1]
        if outcome == "docker" and value.status == 0:
            return
        with self.tx() as state:
            op = state["operations"].get(op_id)
            if op is None or op["state"] != "open":
                return
            self.settle_op(state, op_id, "removed" if outcome == "ok" else f"failed: {failure_text(value)}")
            stack = self.orchestration_record(state)
            if outcome == "ok" and stack["phase"] == "tearing_down":
                stack.update({"network": None, "stop_step": "confirm"})

    def on_orchconfirm(self, key: str, outcome: str, value: Any) -> None:
        """Absence shown by an all-state listing: the stack is down and its limits are released."""
        if outcome != "ok" or not value:
            return
        with self.tx() as state:
            stack = self.orchestration_record(state)
            if stack["phase"] != "tearing_down":
                return
            state["launcher"]["orchestration"] = {"phase": "down", "services": {}, "error": stack.get("error", ""), "down_at": utc_now()}
            for service in ORCHESTRATION_MEMORY:
                state["services"].pop(f"orch-{service}", None)

    def m_orchestration_down(self, ctx: Ctx) -> dict[str, Any]:
        """Stop the Airflow services, then PostgreSQL, remove the network and confirm that nothing of the stack remains."""
        stack = self.orchestration_record(ctx.state)
        if stack["phase"] in ("down", "tearing_down"):
            return {"phase": stack["phase"]}
        stack.update({"phase": "tearing_down", "after": "down", "stop_step": "network"})
        return {"phase": "tearing_down"}

    def ensure_lineage_password(self, folder: Path) -> None:
        """Worker: make sure the database password exists in Parameter Store (generated there once); refuse a new one next to an existing database.

        The whole check runs under one absolute deadline and never on the launcher's loop."""

        def check() -> None:
            store = self.cfg.secret_store()
            if (folder / "postgres" / "PG_VERSION").exists() and store.find(secret_store.MARQUEZ_DB_PASSWORD) is None:
                raise LauncherError(
                    f"the lineage database exists but secret {secret_store.MARQUEZ_DB_PASSWORD} is missing; run the one-time secrets move first", "refused"
                )
            store.ensure(secret_store.MARQUEZ_DB_PASSWORD, secret_store.new_password)

        try:
            secret_store.within(self.cfg.secret_fetch_seconds, check, "the lineage database password check")
        except secret_store.SecretStoreError as error:
            raise LauncherError(str(error), "spec") from None

    def initialize_lineage_config(self, checkout: Path, folder: Path) -> None:
        """Write the Marquez configuration once. It holds a reference to the container's ``MARQUEZ_DB_PASSWORD`` environment value and no secret."""
        config_file = folder / "config" / "marquez.yaml"
        if config_file.exists():
            if not config_file.is_file() or config_file.is_symlink():
                raise LauncherError("incomplete or unsafe lineage runtime configuration", "args")
            return
        os.chmod(folder / "config", 0o700)
        template = (checkout / "services/lineage/marquez.yaml.tmpl").read_text()
        if template.count("${MARQUEZ_DB_PASSWORD}") != 1:
            raise LauncherError("lineage configuration has no unique password placeholder", "args")
        fd = os.open(config_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w") as stream:
            stream.write(template)
            stream.flush()
            os.fsync(stream.fileno())

    def lineage_network_name(self, role: str) -> str:
        """The closed service role determines the registry-owned network name."""
        if role not in ("backend", "frontend"):
            raise LauncherError("unknown lineage network role", "spec")
        return f"hai-{self.registry_id[:8]}-lineage-{role}"

    def lineage_network_role(self, name: str) -> str:
        for role in ("backend", "frontend"):
            if name == self.lineage_network_name(role):
                return role
        raise LauncherError("lineage operation names an unowned network", "spec")

    def create_lineage_network(self, name: str, labels: dict[str, str]) -> str:
        """Create a bridge for the recorded role, or validate it after a lost reply."""
        internal = self.lineage_network_role(name) == "backend"
        try:
            return self.docker.create_network(name, labels, internal=internal)
        except DockerError as error:
            if error.status != 409:
                raise
            found = [n for n in self.docker.networks(self.network_label()) if n.get("Name") == name]
            if len(found) != 1 or bool(found[0].get("Internal")) != internal or (found[0].get("Labels") or {}).get(LABEL_STACK) != LINEAGE_STACK:
                raise LauncherError("existing lineage network differs from its owned role", "refused") from error
            return str(found[0]["Id"])

    def lineage_record(self, state: dict[str, Any]) -> dict[str, Any]:
        return state["launcher"].setdefault("lineage", {"phase": "down", "services": {}, "networks": {}})

    def lineage_state_folder(self, checkout: Path, text: Any) -> str:
        """The stack's state folder: absolute, inside ``data/lineage/`` of the checkout or the main checkout (or a test registry's named
        folder), outside any secrets folder and holding the configuration and database folders."""
        if not isinstance(text, str) or not Path(text).is_absolute():
            raise LauncherError("state must be an absolute folder", "args")
        candidate = Path(text)
        if secrets_part(folded(candidate)) or secrets_part(folded(candidate.resolve())):
            raise LauncherError(f"state {text!r} lies in a secrets folder", "mount")
        roots = [checkout / "data" / "lineage", self.main_checkout / "data" / "lineage", *self.cfg.allowed_roots]
        if not inside(candidate, roots):
            raise LauncherError(f"state {text!r} is outside the allowed folders", "args")
        resolved = candidate.resolve()
        if not resolved.is_dir():
            raise LauncherError(f"state folder {text!r} does not exist", "args")
        missing = [name for name in ("postgres", "config") if not (resolved / name).is_dir() or (resolved / name).is_symlink()]
        if missing:
            raise LauncherError(f"state folder {text!r} lacks {', '.join(missing)}", "args")
        return str(resolved)

    def m_lineage_up(self, ctx: Ctx) -> dict[str, Any]:
        """Start the internal network, PostgreSQL, Marquez API and UI, waiting for health in order."""
        params = ctx.params
        unknown = sorted(set(params) - set(("state", "api_port", "web_port", "checkout")))
        if unknown:
            raise LauncherError(f"lineage_up has no parameter {', '.join(unknown)}", "args")
        port = params.get("api_port")
        web_port = params.get("web_port")
        if type(web_port) is not int or not 1024 <= web_port <= 65535 or web_port == port:
            raise LauncherError("web_port must be a distinct integer from 1024 to 65535", "args")
        if type(port) is not int or not 1024 <= port <= 65535:
            raise LauncherError("api_port must be an integer from 1024 to 65535", "args")
        checkout = self.check_checkout(str(params.get("checkout") or self.cfg.root))
        if not (checkout / LINEAGE_COMPOSE).is_file():
            raise LauncherError(f"{checkout} has no {LINEAGE_COMPOSE}", "args")
        folder = self.lineage_state_folder(checkout, params.get("state"))
        stack = self.lineage_record(ctx.state)
        if stack["phase"] == "tearing_down":
            raise LauncherError("the lineage stack is being torn down", "refused")
        if ctx.state["launcher"].get("restart"):
            raise LauncherError("a Docker restart is running", "refused")
        if self.foreign:
            raise LauncherError("foreign legacy containers exist; the owner reconciles them first", "refused")
        if stack["phase"] in ("up", "starting"):
            wanted = {"checkout": str(checkout), "state": folder, "api_port": port, "web_port": web_port}
            if {name: stack.get(name) for name in wanted} != wanted:
                raise LauncherError("the lineage stack is up with other parameters; lineage_down first", "refused")
            return {"phase": stack["phase"]}
        self.initialize_lineage_config(checkout, Path(folder))
        ctx.state["launcher"]["lineage"] = {
            "phase": "starting",
            "step": "limits",
            "services": {},
            "error": "",
            "networks": {},
            "settings": {},
            "health": {},
            "checkout": str(checkout),
            "state": folder,
            "api_port": port,
            "web_port": web_port,
            "started_at": utc_now(),
            "start_deadline": time.time() + LINEAGE_START_SECONDS,
        }
        own = f"hai-{self.registry_id[:8]}-svc-lineage-"
        growth = {name: limit for name, limit in self.growth_reserve_map(ctx.state).items() if not name.startswith(own)}
        self.spawn("lineage:limits", "lineagelimits", self.compute_lineage_limits, growth, Path(folder))
        return {"phase": "starting"}

    def compute_lineage_limits(self, growth: dict[str, int], folder: Path) -> dict[str, Any]:
        """Worker: check the database password secret, then compute each service limit from one shared resource plan.

        One resource snapshot gives the plan; the services run at the same time, so the sum of their limits (each raised to its minimum) must fit the
        plan's free memory, or the start is refused here, before any network or container exists."""
        self.ensure_lineage_password(folder)
        plan = memory_budget.launch_plan(self.reader(), growth)
        limits = {service: memory_budget.service_limit(plan.budget, share, minimum) for service, (_, share, minimum) in LINEAGE_MEMORY.items()}
        total = sum(limits.values())
        if total > plan.budget.free:
            need, have = total / memory_budget.GIB, plan.budget.free / memory_budget.GIB
            raise memory_budget.BudgetError(f"the lineage services need {need:.2f} GiB at the same time and the plan holds {have:.2f} GiB free")
        return {"limits": limits}

    def on_lineagelimits(self, key: str, outcome: str, value: Any) -> None:
        """The limits are known: record them with the launcher's validated settings, register the services' limits and create the network."""
        with self.tx() as state:
            stack = self.lineage_record(state)
            if stack["phase"] != "starting":
                return
            if outcome != "ok":
                self.fail_lineage(stack, f"the lineage services cannot start: {failure_text(value)}")
                return
            settings = {
                "HAI_LINEAGE_STATE": stack["state"],
                "HAI_LINEAGE_API_PORT": str(stack["api_port"]),
                "HAI_LINEAGE_WEB_PORT": str(stack["web_port"]),
            }
            for service, (name, _, _) in LINEAGE_MEMORY.items():
                settings[f"HAI_LINEAGE_MEM_{name}"] = str(value["limits"][service])
                state["services"][f"lineage-{service}"] = int(value["limits"][service])
            stack["settings"] = settings
            stack["step"] = "network"

    def fail_lineage(self, stack: dict[str, Any], message: str) -> None:
        """A failed start tears the started services down and leaves the stack down."""
        stack.update({"phase": "tearing_down", "after": "down", "error": message, "stop_step": "network"})

    def lineage_stack_attempt(self, state: dict[str, Any], stack: dict[str, Any], service: str) -> dict[str, Any]:
        """The service's attempt, created on first use."""
        attempt_id = stack["services"].get(service)
        if attempt_id is not None:
            return state["attempts"][attempt_id]
        seconds = 10**7
        attempt = self.new_attempt(
            state,
            LINEAGE_OWNER,
            "lineage_service",
            f"lineage-{service}",
            [],
            {"service": service},
            request="launcher",
            checkout=stack["checkout"],
            seconds=seconds,
        )
        stack["services"][service] = attempt["id"]
        return attempt

    def advance_lineage(self, state: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """Startup and teardown of the lineage stack as a recorded state machine, beside the catalog's and sharing none of its records."""
        stack = state["launcher"].get("lineage")
        if not stack or self.expire_lineage_start(state, stack):
            return
        if not self.docker_ok or self.status != "serving":
            return
        if stack["phase"] == "starting" and stack.get("step") not in (None, "limits"):
            self.advance_lineage_start(state, stack, actions)
        elif stack["phase"] == "tearing_down":
            self.advance_lineage_stop(state, stack, actions)
        elif stack["phase"] == "up":
            ended = [service for service, attempt_id in stack["services"].items() if state["attempts"][attempt_id]["phase"] == "ended"]
            stack["degraded"] = f"{', '.join(sorted(ended))} ended" if ended else ""

    def expire_lineage_start(self, state: Mapping[str, Any], stack: dict[str, Any]) -> bool:
        """Record an expired startup as its named timeout and hand the stack to teardown; True when it did.

        Only the stack record changes, so this runs whatever the Docker connection is. Cancelling attempts, removing networks and confirming absence
        stay with ``advance_lineage_stop``, behind the Docker and serving guard, and the limits are released only when absence is confirmed."""
        late = self.lineage_start_phase(state, stack) if stack["phase"] == "starting" else None
        if late is None:
            return False
        self.fail_lineage(stack, f"lineage startup timed out in the {late} phase after the {LINEAGE_START_SECONDS} s startup deadline")
        return True

    def expire_lineage_startup(self) -> None:
        """Record an expired lineage startup while the Docker connection is down: a read first, so a pass with nothing expired writes nothing."""
        with self.tx(write=False) as state:
            stack = state["launcher"].get("lineage")
            expired = bool(stack) and stack["phase"] == "starting" and self.lineage_start_phase(state, stack) is not None
        if expired:
            with self.tx() as state:
                self.expire_lineage_start(state, state["launcher"]["lineage"])

    def lineage_start_phase(self, state: Mapping[str, Any], stack: Mapping[str, Any]) -> str | None:
        """The limits, network or pre-start service phase that outlived the startup deadline; None while in time.

        A service that is started answers to ``health_wait`` and one that ended is reported by its own reason."""
        if time.time() <= stack["start_deadline"]:
            return None
        step = stack["step"]
        if step not in LINEAGE_ORDER:
            return str(step)
        attempt_id = stack["services"].get(step)
        if attempt_id is not None and state["attempts"][attempt_id]["phase"] not in LINEAGE_PRE_START:
            return None
        return f"{step} start"

    def advance_lineage_start(self, state: dict[str, Any], stack: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """Start the network and the three services in order; each service must be healthy first."""
        step = stack["step"]
        if step == "network":
            op = next((o for o in state["operations"].values() if o["kind"] == "lineage_network_create" and o["state"] == "open"), None)
            if op is None:
                missing = [role for role in ("backend", "frontend") if role not in stack.setdefault("networks", {})]
                if not missing:
                    stack["step"] = LINEAGE_ORDER[0]
                    return
                op_id = self.record_network_op(state, "lineage_network_create", name=self.lineage_network_name(missing[0]))
            elif f"op:{op['op_id']}" in self.inflight:
                return
            else:
                op_id = op["op_id"]
            name, labels = state["operations"][op_id]["network_name"], {LABEL_REGISTRY: self.registry_id, LABEL_STACK: LINEAGE_STACK}
            actions.append(lambda: self.spawn(f"op:{op_id}", "lineagenetcreate", self.create_lineage_network, name, labels))
            return
        if step not in LINEAGE_ORDER:
            return
        service = step
        attempt = self.lineage_stack_attempt(state, stack, service)
        following = LINEAGE_ORDER[LINEAGE_ORDER.index(service) + 1 :]
        if attempt["phase"] == "ended":
            self.fail_lineage(stack, f"{service} ended: {attempt['reason']} (exit {attempt.get('exit_code')}) {attempt.get('detail', '')}".strip())
            return
        if attempt["phase"] != "started":
            return
        if stack["health"].get(service) == "healthy":
            if following:
                stack["step"] = following[0]
            else:
                stack.update({"phase": "up", "step": "done", "up_at": utc_now()})
            return
        started = stack.setdefault("waiting_since", {}).setdefault(service, time.time())
        if time.time() - started > self.timings.health_wait:
            self.fail_lineage(stack, f"{service} was not healthy within {self.timings.health_wait:.0f} s")
            return
        container_id = attempt["container_id"]
        actions.append(lambda: self.spawn(f"lineagehealth:{service}", "lineagehealth", self.read_health, container_id))

    def on_lineagehealth(self, key: str, outcome: str, value: Any) -> None:
        """Record a stack service's health."""
        if outcome != "ok":
            return
        with self.tx() as state:
            self.lineage_record(state).setdefault("health", {})[key.split(":", 1)[1]] = value

    def on_lineagenetcreate(self, key: str, outcome: str, value: Any) -> None:
        op_id = key.split(":", 1)[1]
        if outcome == "docker" and value.status == 0:
            return
        with self.tx() as state:
            stack = self.lineage_record(state)
            op = state["operations"].get(op_id)
            if op is None or op["state"] != "open":
                return
            role = self.lineage_network_role(op["network_name"])
            self.settle_op(state, op_id, "created" if outcome == "ok" else f"failed: {failure_text(value)}")
            if outcome == "ok":
                stack.setdefault("networks", {})[role] = value
                state["containers"]["network:" + str(value)] = {"kind": "network", "at": utc_now()}
            elif stack["phase"] == "starting":
                self.fail_lineage(stack, f"cannot create the lineage {role} network: {failure_text(value)}")

    def settle_lineage_network_ops(self, networks: Sequence[Mapping[str, Any]]) -> None:
        """Reconcile each durable role-specific network operation with an all-state scan."""
        with self.tx() as state:
            stack = self.lineage_record(state)
            for op in list(state["operations"].values()):
                if op["state"] != "open" or op["kind"] not in ("lineage_network_create", "lineage_network_remove"):
                    continue
                role = self.lineage_network_role(op["network_name"])
                found = [n for n in networks if n.get("Name") == op["network_name"]]
                if op["kind"] == "lineage_network_create" and found:
                    if (
                        len(found) != 1
                        or bool(found[0].get("Internal")) != (role == "backend")
                        or (found[0].get("Labels") or {}).get(LABEL_STACK) != LINEAGE_STACK
                    ):
                        self.fail_lineage(stack, f"lineage {role} network differs from its recorded role")
                        continue
                    self.settle_op(state, op["op_id"], "created", evidence="scan")
                    network = str(found[0]["Id"])
                    stack.setdefault("networks", {})[role] = network
                    state["containers"]["network:" + network] = {"kind": "network", "at": utc_now()}
                elif op["kind"] == "lineage_network_remove" and not found:
                    self.settle_op(state, op["op_id"], "removed", evidence="scan")
                    stack.setdefault("networks", {}).pop(role, None)

    def advance_lineage_stop(self, state: dict[str, Any], stack: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """Stop the UI and API together, then PostgreSQL, remove the network, then confirm absence before the stack is down."""
        for group in (tuple(s for s in LINEAGE_ORDER if s != "postgres"), ("postgres",)):
            open_attempts = [
                state["attempts"][stack["services"][s]] for s in group if s in stack["services"] and is_open(state["attempts"][stack["services"][s]])
            ]
            for attempt in open_attempts:
                if not attempt["cancel_intent"]:
                    attempt["cancel_intent"] = {"reason": "lineage_down", "at": utc_now()}
            if open_attempts:
                return
        step = stack.get("stop_step", "network")
        create = next((o for o in state["operations"].values() if o["kind"] == "lineage_network_create" and o["state"] == "open"), None)
        if step == "network" and create is not None:
            if f"op:{create['op_id']}" not in self.inflight:
                op_id = create["op_id"]
                name, labels = state["operations"][op_id]["network_name"], {LABEL_REGISTRY: self.registry_id, LABEL_STACK: LINEAGE_STACK}
                actions.append(lambda: self.spawn(f"op:{op_id}", "lineagenetcreate", self.create_lineage_network, name, labels))
            return
        if step == "network":
            roles = [role for role in ("frontend", "backend") if role in stack.setdefault("networks", {})]
            role = roles[0] if roles else None
            network = stack["networks"].get(role)
            op = next((o for o in state["operations"].values() if o["kind"] == "lineage_network_remove" and o["state"] == "open"), None)
            if network is None:
                stack["stop_step"] = "confirm"
                return
            if op is None:
                op_id = self.record_network_op(state, "lineage_network_remove", network, name=self.lineage_network_name(str(role)))
            elif f"op:{op['op_id']}" in self.inflight:
                return
            else:
                op_id = op["op_id"]
                network = op.get("network") or network
            actions.append(lambda: self.spawn(f"op:{op_id}", "lineagenetremove", self.docker.remove_network, network))
        elif step == "confirm":
            actions.append(lambda: self.spawn("lineage:confirm", "lineageconfirm", self.lineage_absent))

    def lineage_absent(self) -> bool:
        """Worker: True when no container and no network of the lineage stack exist, in any state."""
        label = f"{LABEL_STACK}={LINEAGE_STACK}"
        containers = [c for c in self.docker.containers(label=label) if (c.get("Labels") or {}).get(LABEL_REGISTRY) == self.registry_id]
        networks = [n for n in self.docker.networks(label) if (n.get("Labels") or {}).get(LABEL_REGISTRY) == self.registry_id]
        return not containers and not networks

    def on_lineagenetremove(self, key: str, outcome: str, value: Any) -> None:
        """The stack's network is gone: confirm absence next."""
        op_id = key.split(":", 1)[1]
        if outcome == "docker" and value.status == 0:
            return
        with self.tx() as state:
            op = state["operations"].get(op_id)
            if op is None or op["state"] != "open":
                return
            self.settle_op(state, op_id, "removed" if outcome == "ok" else f"failed: {failure_text(value)}")
            stack = self.lineage_record(state)
            if outcome == "ok" and stack["phase"] == "tearing_down":
                stack.setdefault("networks", {}).pop(self.lineage_network_role(op["network_name"]), None)

    def on_lineageconfirm(self, key: str, outcome: str, value: Any) -> None:
        """Absence shown by an all-state listing: the stack is down and its limits are released."""
        if outcome != "ok" or not value:
            return
        with self.tx() as state:
            stack = self.lineage_record(state)
            if stack["phase"] != "tearing_down":
                return
            state["launcher"]["lineage"] = {"phase": "down", "services": {}, "error": stack.get("error", ""), "down_at": utc_now()}
            for service in LINEAGE_MEMORY:
                state["services"].pop(f"lineage-{service}", None)

    def m_lineage_down(self, ctx: Ctx) -> dict[str, Any]:
        """Stop the UI and API, then PostgreSQL, remove the network and confirm that nothing of the stack remains."""
        stack = self.lineage_record(ctx.state)
        if stack["phase"] in ("down", "tearing_down"):
            return {"phase": stack["phase"]}
        stack.update({"phase": "tearing_down", "after": "down", "stop_step": "network"})
        return {"phase": "tearing_down"}

    def template_images(self) -> set[str]:
        """Return only pinned images declared by Compose or the closed pipeline proxy policy: what the templates can run."""
        found: set[str] = {network_policy.PROXY_IMAGE}
        for path in (self.cfg.root / "docker-compose.yaml", self.cfg.root / ORCHESTRATION_COMPOSE, self.cfg.root / LINEAGE_COMPOSE):
            if path.is_file():
                found |= {str(service["image"]) for service in ComposeModel(path).services.values() if service.get("image")}
        return found

    def m_pull(self, ctx: Ctx) -> Any:
        """Pull one image by its digest. The reference must be pinned by digest and named by a template's Compose service; a tag, an unnamed
        digest, a locally built image or any option is refused."""
        unknown = sorted(set(ctx.params) - {"reference"})
        if unknown:
            raise LauncherError(f"pull has no parameter {', '.join(unknown)}", "args")
        reference = ctx.params.get("reference")
        if not isinstance(reference, str) or len(reference) > 300:
            raise LauncherError("reference must be an image reference", "args")
        if "@sha256:" not in reference:
            raise LauncherError(f"image {reference!r} is not pinned by digest; only digest-pinned images are pulled", "refused")
        if not PINNED_REFERENCE.fullmatch(reference):
            raise LauncherError(f"{reference!r} is not a plain digest-pinned image reference", "args")
        if reference not in self.template_images():
            raise LauncherError(f"no template names image {reference}", "refused")
        repository = reference.partition("@")[0].partition(":")[0]
        digest = reference.partition("@")[2]
        key = f"pull:{ctx.key}"
        self.pull_requests[key] = ctx.key
        self.spawn(key, "pull", self.pull_image, repository, digest)
        return PENDING

    def pull_image(self, repository: str, digest: str) -> dict[str, Any]:
        """Worker: pull by digest unless the image is present, and show that it is present afterwards."""
        pinned = f"{repository}@{digest}"
        before = self.docker.image(pinned)
        if before is not None:
            return {"reference": pinned, "pulled": False, "id": str(before.get("Id", ""))}
        self.docker.pull(repository, digest)
        after = self.docker.image(pinned)
        if after is None:
            raise LauncherError(f"image {pinned} is not present after the pull", "failed")
        return {"reference": pinned, "pulled": True, "id": str(after.get("Id", ""))}

    def on_pull(self, key: str, outcome: str, value: Any) -> None:
        """A pull ended: answer the request."""
        request_key = self.pull_requests.pop(key, "")
        if not request_key:
            return
        if outcome == "ok":
            self.complete_request(request_key, value)
        elif outcome == "docker" and value.status == 0:
            self.complete_request(request_key, {}, Degraded(f"Docker does not answer: {value.message}"))
        else:
            self.complete_request(request_key, {}, LauncherError(f"pull failed: {failure_text(value)}", "failed"))

    def m_docker_restart(self, ctx: Ctx) -> Any:
        """Restart Docker Desktop to free the VM's memory, only when nothing of this registry and nothing foreign runs (D1 section 8 item 4)."""
        if ctx.state["launcher"].get("restart"):
            raise LauncherError("a Docker restart is already running", "refused")
        caller = str(ctx.params.get("alloc_id") or "")
        problems = self.restart_problems(ctx.state, caller)
        if problems:
            raise LauncherError(f"docker_restart is refused: {problems[0]}", "refused")
        key = f"restartcheck:{ctx.key}"
        self.release_requests[key] = (ctx.key, caller, "")
        self.spawn(key, "restartcheck", lambda: self.docker.containers(all_states=False))
        return PENDING

    def restart_problems(self, state: Mapping[str, Any], caller: str) -> list[str]:
        """Every reason a restart is refused, from the registry alone."""
        problems = [f"attempt {a['id']} is open" for a in state["attempts"].values() if is_open(a)]
        problems += [f"operation {o['op_id']} is open" for o in state["operations"].values() if o["state"] == "open"]
        problems += [
            f"allocation {k} is {v['state']}" for k, v in state["allocations"].items() if v["state"] in ("planning", "held", "releasing") and k != caller
        ]
        problems += [f"registration {k} exists" for k in state["registrations"]]
        return problems

    def on_restartcheck(self, key: str, outcome: str, value: Any) -> None:
        """The running-container listing arrived: a container the launcher did not create refuses the restart."""
        request_key, caller, _ = self.release_requests.pop(key, ("", "", ""))
        if not request_key:
            return
        if outcome != "ok":
            self.complete_request(request_key, {}, Degraded(f"docker_restart is refused: the running containers cannot be listed ({failure_text(value)})"))
            return
        with self.tx() as state:
            problems = self.restart_problems(state, caller)
            known = {cid for cid, entry in state["containers"].items() if not cid.startswith("network:") and entry.get("name")}
            foreign = [str(c["Names"][0]).lstrip("/") for c in value if c["Id"] not in known]
            if foreign:
                problems.append(f"a running container was not created by this launcher ({foreign[0]})")
            if problems:
                self.complete_request(request_key, {}, LauncherError(f"docker_restart is refused: {problems[0]}", "refused"))
                return
            state["launcher"]["restart"] = {"phase": "restart", "request": request_key, "at": utc_now()}

    def advance_restart(self, state: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """Run the restart command, then wait until the daemon answers again and the stream is serving."""
        restart = state["launcher"].get("restart")
        if not restart:
            return
        if restart["phase"] == "restart":
            if "restart:cmd" not in self.inflight:
                actions.append(lambda: self.spawn("restart:cmd", "restartcmd", self.run_restart_command))
        elif (
            restart["phase"] == "wait"
            and self.status == "serving"
            and self.docker_ok
            and "restart:ping" not in self.inflight
            and time.time() - restart.get("started", time.time()) > 1
        ):
            actions.append(lambda: self.spawn("restart:ping", "restartping", self.docker.ping))

    def run_restart_command(self) -> int:
        """Worker: the restart command of the Docker application."""
        program, *arguments = self.cfg.restart_command
        result = run_command(program, arguments, timeout=self.timings.restart_wait)
        if result.returncode:
            raise LauncherError(f"{' '.join(self.cfg.restart_command)} failed with exit code {result.returncode}")
        return 0

    def on_restartcmd(self, key: str, outcome: str, value: Any) -> None:
        """The restart command ended: wait for the daemon."""
        request_key = ""
        with self.tx() as state:
            restart = state["launcher"].get("restart")
            if not restart:
                return
            request_key = restart["request"]
            if outcome != "ok":
                state["launcher"]["restart"] = None
            else:
                restart.update({"phase": "wait", "started": time.time()})
        if outcome != "ok":
            self.complete_request(request_key, {}, LauncherError(f"docker restart failed: {failure_text(value)}", "refused"))

    def on_restartping(self, key: str, outcome: str, value: Any) -> None:
        """The daemon answers again: the restart is done, with the catalog stopped."""
        if outcome != "ok" or not value:
            return
        request_key = ""
        with self.tx() as state:
            restart = state["launcher"].get("restart")
            if not restart or restart["phase"] != "wait" or self.status != "serving":
                return
            request_key = restart["request"]
            state["launcher"]["restart"] = None
        self.complete_request(request_key, {"phase": "done", "catalog": "down"})

    # -- snapshot copies (D1 section 10 item 1) ---------------------------------------------------------------------

    GENERATED = ("target", "dbt_packages", "logs")

    def m_snapshot(self, ctx: Ctx) -> dict[str, Any]:
        """Ensure the run has a complete snapshot of ``dbt/`` and the resolved package tree, copied once per run."""
        entry = self.run_entry(ctx)
        run_id = entry["run_id"]
        if entry["state"] != "held":
            raise LauncherError(f"run {run_id} is {entry['state']}; only a held run is snapshotted", "refused")
        if entry.get("snapshot"):
            return {"state": "complete", "copy_id": entry["snapshot"]}
        for copy in ctx.state["copies"].values():
            if copy["run"] == run_id and copy["phase"] in ("copying", "abandoning"):
                return {"state": copy["phase"], "copy_id": copy["copy_id"]}
        checkout = self.check_checkout(str(ctx.params.get("checkout") or self.cfg.root))
        packages = self.renderer_for(checkout).allowed(str(ctx.params["packages_host"]), "packages_host")
        guard_key = str(packages.resolve())
        guard = ctx.state["guards"].setdefault(guard_key, {"writer": None, "readers": {}})
        if guard["writer"]:
            raise LauncherError("dbt_deps holds the package folder; the snapshot copy is refused", "refused")
        copy_id = new_id("cp")
        guard["readers"][copy_id] = {"at": utc_now()}
        folder = self.cfg.runs_root / run_id / "snapshots" / copy_id
        ctx.state["copies"][copy_id] = {
            "copy_id": copy_id,
            "run": run_id,
            "generation": int(entry.get("generation", 1)),
            "phase": "copying",
            "guard": guard_key,
            "folder": str(folder),
            "cancel_requested": False,
            "created_at": utc_now(),
        }
        source = checkout / "dbt"
        thread = threading.Thread(target=self.copy_worker, args=(copy_id, source, packages, folder), name=f"copy-{copy_id}", daemon=True)
        self.copy_threads[copy_id] = thread
        thread.start()
        return {"state": "copying", "copy_id": copy_id}

    def copy_worker(self, copy_id: str, source: Path, packages: Path, folder: Path) -> None:
        """Thread body: copy, fsync, make read-only and hash; the loop publishes the result."""
        try:
            result = self.copy_tree(copy_id, source, packages, folder)
            self.messages.put(("copy", copy_id, ("ok", result)))
        except Exception as error:
            self.messages.put(("copy", copy_id, ("error", error)))
        with contextlib.suppress(OSError):
            os.write(self.wake_write, b"x")

    def copy_tree(self, copy_id: str, source: Path, packages: Path, folder: Path) -> dict[str, str]:
        """Copy ``dbt/`` and the package tree into the copy's own folder, checking the cancel flag between files."""
        digest = hashlib.sha256()
        for label, root, skip in (("project", source, self.GENERATED), ("packages", packages, ())):
            destination = folder / label
            destination.mkdir(parents=True, exist_ok=True)
            for path in sorted(item for item in root.rglob("*") if item.is_file()):
                relative = path.relative_to(root)
                if skip and relative.parts[0] in skip:
                    continue
                if copy_id in self.copy_cancel:
                    raise LauncherError("the snapshot copy was cancelled", "cancelled")
                target = destination / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, target)
                digest.update(f"{label}/{relative}".encode() + b"\0" + hashlib.sha256(target.read_bytes()).digest())
                self.copy_hook(copy_id, str(relative))
            self.fsync_tree(destination)
        for path in sorted(folder.rglob("*"), reverse=True):
            path.chmod(0o555 if path.is_dir() else 0o444)
        folder.chmod(0o555)
        return {"dbt_tree_sha256": digest.hexdigest()}

    def copy_hook(self, copy_id: str, relative: str) -> None:
        """A seam for the end-to-end run: it pauses a copy between files. The default does nothing."""

    @staticmethod
    def fsync_tree(root: Path) -> None:
        """fsync every file and folder of a tree."""
        for path in [*sorted(root.rglob("*")), root]:
            descriptor = os.open(path, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def on_copy(self, key: str, outcome: str, value: Any) -> None:
        """The copy thread returned: publish on success; on an error or a cancel the folder is removed and the copy abandoned (D1 section 10 item 1)."""
        thread = self.copy_threads.pop(key, None)
        if thread is not None:
            thread.join()
        with self.tx() as state:
            copy = state["copies"].get(key)
            if copy is None:
                return
            run = state["runs"].get(copy["run"])
            publishable = (
                outcome == "ok"
                and copy["phase"] == "copying"
                and not copy["cancel_requested"]
                and key not in self.copy_cancel
                and run is not None
                and run["state"] == "held"
                and int(run.get("generation", 1)) == copy["generation"]
            )
            if publishable:
                copy.update({"phase": "complete", "dbt_tree_sha256": value["dbt_tree_sha256"]})
                run["snapshot"] = key
                state["guards"][copy["guard"]]["readers"].pop(key, None)
                return
            copy["phase"] = "abandoning"
            copy["reason"] = "cancelled" if key in self.copy_cancel or copy["cancel_requested"] else f"error: {failure_text(value)}"
        self.copy_cancel.discard(key)

    def advance_copies(self, state: dict[str, Any], actions: list[Callable[[], None]]) -> None:
        """Cancel copies whose run changed; remove the folder of an abandoning copy and release its guard once it is gone."""
        for copy_id, copy in state["copies"].items():
            run = state["runs"].get(copy["run"])
            if copy["phase"] == "copying" and (run is None or run["state"] != "held" or int(run.get("generation", 1)) != copy["generation"]):
                copy["cancel_requested"] = True
                self.copy_cancel.add(copy_id)
            if copy["phase"] == "abandoning" and f"rmcopy:{copy_id}" not in self.inflight:
                folder = Path(copy["folder"])
                actions.append(partial(self.spawn, f"rmcopy:{copy_id}", "rmcopy", self.remove_tree, folder))

    @staticmethod
    def remove_tree(folder: Path) -> bool:
        """Worker: remove a copy folder (made read-only) completely."""
        if folder.exists():
            for path in [folder, *folder.rglob("*")]:
                if not path.is_symlink():
                    path.chmod(0o755)
            shutil.rmtree(folder)
        return not folder.exists()

    def on_rmcopy(self, key: str, outcome: str, value: Any) -> None:
        """The folder is gone: the copy is ``abandoned`` and its reader guard is released."""
        copy_id = key.split(":", 1)[1]
        if outcome != "ok" or not value:
            return
        with self.tx() as state:
            copy = state["copies"].get(copy_id)
            if copy is None or copy["phase"] != "abandoning":
                return
            copy["phase"] = "abandoned"
            state["guards"][copy["guard"]]["readers"].pop(copy_id, None)

    def sweep_copies(self) -> None:
        """On start, a ``copying`` or ``abandoning`` record has no live worker: remove its folder, abandon it, release its guard; remove unnamed folders."""
        with self.tx() as state:
            named = set()
            for copy_id, copy in state["copies"].items():
                if copy["phase"] in ("copying", "abandoning"):
                    folder = Path(copy["folder"])
                    self.remove_tree(folder)
                    copy["phase"] = "abandoned"
                    copy["reason"] = copy.get("reason") or "launcher restart"
                    state["guards"].get(copy["guard"], {"readers": {}})["readers"].pop(copy_id, None)
                named.add(str(copy["folder"]))
            for run_id, run in state["runs"].items():
                snapshots = self.cfg.runs_root / run_id / "snapshots"
                if not snapshots.is_dir():
                    continue
                for folder in snapshots.iterdir():
                    if str(folder) not in named and folder.name != str(run.get("snapshot")):
                        self.remove_tree(folder)

    # -- sessions and logs (D1 section 9) ---------------------------------------------------------------------------

    DETACH_KEYS = "ctrl-p,ctrl-q"

    def m_session(self, ctx: Ctx) -> Any:
        """Open an interactive session on one attempt's container; the launcher itself attaches (D1 section 9 item 1)."""
        attempt = ctx.state["attempts"].get(str(ctx.params.get("attempt_id")))
        if attempt is None:
            raise LauncherError("no such attempt", "refused")
        self.require_owner(ctx.state, ctx.conn, ctx.principal, attempt["owner"])
        template = TEMPLATES[attempt["template"]]
        if template.session is None:
            raise LauncherError(f"template {template.name} has no interactive session", "refused")
        if attempt["phase"] != "started" or not attempt["container_id"]:
            raise LauncherError(f"attempt {attempt['id']} is {attempt['phase']}; a session needs a started container", "refused")
        key = f"attach:{id(ctx.conn)}:{ctx.request_id}"
        self.read_waiters[key] = (ctx.conn, ctx.request_id)
        self.attach_info[key] = (attempt["id"], attempt["container_id"], bool(attempt["params"].get("tty")), template.session)
        container_id = attempt["container_id"]
        self.spawn(key, "attach", self.docker.attach, container_id, self.DETACH_KEYS)
        return PENDING

    def on_attach(self, key: str, outcome: str, value: Any) -> None:
        """The attach connection is open: bind it to the client connection."""
        conn, request_id = self.read_waiters.pop(key, (None, ""))
        attempt_id, container_id, tty, policy = self.attach_info.pop(key, ("", "", False, "cancel"))
        if conn is None:
            if outcome == "ok":
                value.close()
            return
        if outcome != "ok" or conn.closed:
            conn.send({"id": request_id, "ok": False, "error": f"attach failed: {failure_text(value)}", "code": "degraded"})
            if outcome == "ok":
                value.close()
            return
        value.setblocking(False)
        session = Session(conn, attempt_id, container_id, value, tty, policy)
        conn.session = session
        self.sessions[value.fileno()] = session
        self.selector.register(value, selectors.EVENT_READ, "session_docker")
        with self.tx() as state:
            attempt = state["attempts"].get(attempt_id)
            if attempt is not None:
                attempt["session_seen"] = time.time()
        conn.send({"id": request_id, "ok": True, "result": {"attached": True, "tty": tty}})

    def session_docker_event(self, sock: Any, mask: int) -> None:
        """Docker is ready: send the input that waits for it, then relay its output."""
        session = self.sessions.get(sock.fileno())
        if session is None:
            return
        if mask & selectors.EVENT_WRITE:
            self.session_send_input(session)
        if mask & selectors.EVENT_READ and self.sessions.get(sock.fileno()) is session:
            self.session_docker_ready(sock)

    def session_send_input(self, session: Session) -> None:
        """Send as much waiting input to Docker as its socket accepts without blocking."""
        try:
            while session.pending_in:
                del session.pending_in[: session.docker_socket.send(session.pending_in)]
        except BlockingIOError:
            pass
        except OSError:
            session.pending_in.clear()
        self.update_conn(session.conn)

    def session_docker_ready(self, sock: Any) -> None:
        """Output from the container: relay it to the client."""
        session = self.sessions.get(sock.fileno())
        if session is None:
            return
        try:
            data = sock.recv(65536)
        except BlockingIOError:
            return
        except OSError:
            data = b""
        if not data:
            self.end_session(session, "output closed")
            return
        if session.tty:
            frames = [(1, data)]
        else:
            frames, session.pending = demultiplex(session.pending + data)
        for stream, payload in frames:
            session.conn.send({"out": base64.b64encode(payload).decode(), "stream": stream})

    def session_client_data(self, conn: Conn, data: bytes) -> None:
        """Input and signals from the client: Ctrl-C is a session message that becomes input or a cancel by policy (D1 section 9 item 2)."""
        session = conn.session
        if session is None:
            return
        conn.buffer += data
        while b"\n" in conn.buffer:
            line, conn.buffer = conn.buffer.split(b"\n", 1)
            try:
                message = json.loads(line)
            except ValueError:
                continue
            if "in" in message:
                with contextlib.suppress(ValueError, TypeError):
                    session.pending_in += base64.b64decode(message["in"])
            elif message.get("signal") == "INT":
                if session.policy == "forward":
                    session.pending_in += b"\x03"
                else:
                    self.cancel_attempt_by_session(session)
            elif message.get("detach"):
                self.end_session(session, "detached", detach=True)
                return
        if len(conn.buffer) > MAX_LINE:
            self.close_conn(conn)
            return
        self.session_send_input(session)

    def cancel_attempt_by_session(self, session: Session) -> None:
        """A session's Ctrl-C under the ``cancel`` policy persists a cancel intent."""
        with self.tx() as state:
            attempt = state["attempts"].get(session.attempt)
            if attempt is not None and is_open(attempt) and not attempt["cancel_intent"]:
                attempt["cancel_intent"] = {"reason": "cancelled", "at": utc_now(), "via": "session"}
        self.dirty.add(session.attempt)

    def end_session(self, session: Session, reason: str, detach: bool = False) -> None:
        """Close the attach connection; the container keeps running on detach and is cleaned up on exit."""
        with contextlib.suppress(KeyError, ValueError, OSError):
            self.selector.unregister(session.docker_socket)
        with contextlib.suppress(OSError):
            session.docker_socket.close()
        self.sessions.pop(session.docker_socket.fileno(), None)
        for fileno, known in list(self.sessions.items()):
            if known is session:
                self.sessions.pop(fileno, None)
        session.detached = True
        session.pending_in.clear()
        session.conn.session = None
        session.conn.send({"closed": reason, "detached": detach})
        self.update_conn(session.conn)

    def session_timers(self, now: float) -> None:
        """A detached or lost session keeps the container for the idle limit, then cancels the attempt (D1 section 9 item 3)."""
        with self.tx(write=False) as state:
            candidates = [
                a["id"]
                for a in state["attempts"].values()
                if is_open(a) and a["phase"] == "started" and TEMPLATES[a["template"]].session and not a["cancel_intent"]
            ]
            seen = {a["id"]: float(a.get("session_seen") or 0) for a in state["attempts"].values() if a["id"] in candidates}
        live = {s.attempt for s in self.sessions.values() if not s.detached}
        for attempt_id in candidates:
            if attempt_id in live:
                continue
            idle_since = seen.get(attempt_id) or 0.0
            if idle_since and time.time() - idle_since > self.timings.idle_session:
                with self.tx() as state:
                    attempt = state["attempts"][attempt_id]
                    attempt["cancel_intent"] = {"reason": "cancelled", "at": utc_now(), "via": "idle"}
                self.dirty.add(attempt_id)
            elif not idle_since:
                with self.tx() as state:
                    state["attempts"][attempt_id]["session_seen"] = time.time()


# ---------------------------------------------------------------------------------------------------------------------
# Configuration, the client and the command line
# ---------------------------------------------------------------------------------------------------------------------


def resolve_config(test_registry: str | None, cwd: Path | None = None) -> Config:
    """Everything the launcher and its clients need to find the registry, Docker and the token store."""
    root = Path(__file__).resolve().parents[1]
    registry = run_lock.resolve(test_registry, cwd or Path.cwd())
    with run_lock.transaction(registry, write=False, create_schema=run_lock.LAUNCHER_SCHEMA) as state:
        registry_id = str(state["registry_id"])
    if registry.kind == "test":
        settings: dict[str, Any] = {}
        test_file = registry.path / "launcher_test.json"
        if test_file.is_file():
            settings = json.loads(test_file.read_text())
        timing_values: dict[str, Any] = {key: int(value) if key == "stop_grace" else float(value) for key, value in dict(settings.get("timings", {})).items()}
        timings = replace(Timings(), **timing_values)
        if timings.history_retain <= timings.poll_patience:
            raise LauncherError(f"history_retain ({timings.history_retain} s) must exceed poll_patience ({timings.poll_patience} s)", "config")
        if timings.compact_floor <= timings.poll_patience:
            raise LauncherError(f"compact_floor ({timings.compact_floor} s) must exceed poll_patience ({timings.poll_patience} s)", "config")
        for name in ("compact_threshold", "stuck_after", "gone_after", "slow_exchange", "cleanup_budget", "memory_wait"):
            if getattr(timings, name) <= 0:
                raise LauncherError(f"{name} ({getattr(timings, name)}) must be positive", "config")
        folder = registry.path / "test_state"
        return Config(
            registry=registry,
            root=root,
            docker_socket=str(settings["docker_socket"]),
            token_root=Path(settings.get("token_root") or registry.path / "run_tokens" / registry_id),
            runs_root=Path(settings.get("runs_root") or folder / "runs"),
            timings=timings,
            images=dict(settings.get("images", {})),
            restart_command=tuple(settings.get("restart_command", DOCKER_RESTART_COMMAND)),
            secrets_endpoint=str(settings["secrets_endpoint"]) if settings.get("secrets_endpoint") else None,
            secret_fetch_seconds=float(settings.get("secret_fetch_seconds", secret_store.FETCH_DEADLINE_SECONDS)),
            sts_endpoint=str(settings["sts_endpoint"]) if settings.get("sts_endpoint") else None,
            aws_account_id=str(settings.get("aws_account_id", "")),
            aws_profile=str(settings.get("aws_profile", "")),
            aws_region=str(settings.get("aws_region", "")),
            lakehouse_state=Path(settings.get("lakehouse_state") or folder / "lakehouse"),
            expected_uid=int(settings["expected_uid"]) if settings.get("expected_uid") is not None else None,
            # A test registry treats its own checkout as the main one; a test may name other read folders (never a real registry).
            allowed_roots=tuple(Path(str(folder)) for folder in settings.get("allowed_roots", ())),
            catalog_checkout=Path(settings["catalog_checkout"]) if settings.get("catalog_checkout") else root,
        )
    main = run_lock.main_checkout(root)
    return Config(
        registry=registry,
        root=root,
        docker_socket=docker_socket_path(),
        token_root=real_token_root(registry_id),
        runs_root=main / "data" / "analytics" / "dbt" / "runs",
        lakehouse_state=main / "data" / "lakehouse",
        catalog_checkout=main,
    )


class ClientError(Exception):
    """A launcher refusal or failure, with its machine-readable code."""

    def __init__(self, message: str, code: str) -> None:
        super().__init__(message)
        self.code = code

    @property
    def transport(self) -> bool:
        """True when the connection to the launcher failed (it is gone or does not answer), not when the launcher refused the request."""
        return self.code in LIVENESS_CODES

    def describe(self, what: str) -> str:
        """The error for a person: ``the launcher refused <what>: ...``, or for a lost connection ``the launcher did not answer <what>: ... [code]``."""
        return f"the launcher did not answer {what}: {self} [{self.code}]" if self.transport else f"the launcher refused {what}: {self}"


@dataclass
class AttemptResult:
    """What a finished attempt left: its end reason, exit code and the saved output."""

    attempt_id: str
    reason: str
    exit_code: int | None
    stdout: str
    stderr: str
    record: dict[str, Any]

    @property
    def ok(self) -> bool:
        """True when the attempt ran and exited with code 0."""
        return self.reason == "exited" and self.exit_code == 0


class ReleaseOutcome(enum.StrEnum):
    """What ``LauncherClient.release_allocation`` achieved.

    ``RELEASED``: the launcher confirmed the release. ``RETAINED``: after a lost connection the work of the allocation was not settled within
    ``cleanup_budget``, so no release was requested and the launcher releases the allocation once its holder is gone. ``UNRESOLVED``: the work was
    settled and the release request got no answer, so whether the allocation is released is not known.
    """

    RELEASED = "released"
    RETAINED = "retained"
    UNRESOLVED = "unresolved"


NO_PRINCIPAL = "any:"


def principal_key(principal: Mapping[str, Any] | None) -> str:
    """What names a principal in a client's records: its kind with its allocation or run ID, or ``NO_PRINCIPAL`` for an exchange without one."""
    if not principal or principal.get("kind", "any") == "any":
        return NO_PRINCIPAL
    return f"{principal['kind']}:{principal.get('alloc_id') or principal.get('run_id') or ''}"


class Patience:
    """What one waiting client has seen of the launcher: whether it answers, whether it makes progress and how long an exchange may take.

    A launcher that refuses connections for ``gone_after`` seconds is gone. A launcher that answers a liveness request is making progress; one that
    stays silent for ``stuck_after`` seconds is stuck. Otherwise the client keeps trying until ``poll_patience`` runs out. A timed-out exchange
    doubles the next exchange's budget up to ``EXCHANGE_BUDGET_CAP``; a quick exchange resets it.
    """

    def __init__(self, client: LauncherClient, principal: Mapping[str, Any]) -> None:
        self.client = client
        self.principal = principal
        self.timings = client.config.timings
        self.failing_since: float | None = None
        self.gone_since: float | None = None
        self.last_progress = time.monotonic()
        self.learned = EXCHANGE_BUDGET

    def budget(self, settle_deadline: float | None = None) -> float:
        """Seconds the next exchange may take: one second once a pool's settle limit counts, else the learned budget within the progress limit."""
        if settle_deadline is not None:
            return max(0.05, min(EXCHANGE_BUDGET, settle_deadline - time.monotonic()))
        return max(0.5, min(self.learned, self.timings.stuck_after - (time.monotonic() - self.last_progress)))

    def call(self, method: str, params: dict[str, Any], principal: dict[str, Any], request_id: str, budget: float) -> Any:
        """One bounded exchange; a reply counts as progress."""
        begun = time.monotonic()
        result = self.client.bounded_call(method, params, principal, request_id, budget)
        self.client.forget_failure(self.principal)
        self.failing_since = self.gone_since = None
        self.last_progress = time.monotonic()
        if self.last_progress - begun < self.timings.slow_exchange:
            self.learned = EXCHANGE_BUDGET
        return result

    def failed(self, error: BaseException, attempt_id: str | None = None) -> None:
        """After a failed exchange: raise the error that ends the wait, or return so the caller tries again."""
        now = time.monotonic()
        self.failing_since = now if self.failing_since is None else self.failing_since
        state = self.client.probe(PROBE_BUDGET)
        if state == "gone":
            self.gone_since = now if self.gone_since is None else self.gone_since
            if now - self.gone_since >= self.timings.gone_after:
                raise self.client.note_failure(
                    ClientError(f"the launcher is not running: nothing answers at {self.client.config.socket_path}", "unavailable"), self.principal, attempt_id
                ) from error
        else:
            self.gone_since = None
            if state == "alive":
                self.last_progress = time.monotonic()
            elif time.monotonic() - self.last_progress >= self.timings.stuck_after:
                raise self.client.note_failure(
                    ClientError(f"the launcher is alive but made no progress for {self.timings.stuck_after:g} s", "unresponsive"), self.principal, attempt_id
                ) from error
            self.learned = min(self.learned * 2, EXCHANGE_BUDGET_CAP)
        if now - self.failing_since >= self.timings.poll_patience:
            raise self.client.note_failure(error, self.principal, attempt_id)


class StartTimeout(TimeoutError):
    """The budget ended while the launcher was being started. Its text comes from a process ID, signal names and fixed phrases only, so a caller can show it."""


class LauncherClient:
    """A client of the launcher's socket. It makes no Docker call (D1 section 2 item 4)."""

    def __init__(self, config: Config, start: bool = True) -> None:
        self.config = config
        self.start = start
        self.counter = 0
        self.live: dict[str, dict[str, Any]] = {}
        self.live_lock = threading.Lock()
        self.cancelling = False
        self.pending_submits: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
        self.cancel_unresolved = threading.Event()
        self.cancel_lock = threading.Lock()
        # Set once every cancellation of an interrupted pool has persisted; the settle limit counts from here.
        self.cancel_persisted_at: float | None = None
        self.socket_factory: Callable[..., socket.socket] = socket.socket
        self.launch_nonce = ""  # passed to a launcher this client starts, so the client can recognize it later
        self.lost: dict[str, float | None] = {}
        self.lost_lock = threading.Lock()
        self.abandoned: set[str] = set()

    # -- connection ------------------------------------------------------------------------------------------------

    @staticmethod
    def seconds_left(limit: float | None, before: str) -> float | None:
        """Seconds until ``limit`` (``None`` without one); a spent budget raises ``TimeoutError`` naming what it ended before."""
        if limit is None:
            return None
        left = limit - time.monotonic()
        if left <= 0:
            raise TimeoutError(f"the budget ended before {before}")
        return left

    def connect(self, budget: float | None = None) -> socket.socket:
        """Connect to the socket, starting the launcher on demand.

        With ``budget`` seconds, every connection attempt, the wait for a started launcher and its pauses share that one limit, and a spent budget
        raises ``TimeoutError``. Cleaning up an intermediate process that outlived the budget can add up to two ``REAP_SECONDS`` waits beyond it
        (``start_launcher``). Without a budget, attempts are unbounded and the wait for a started launcher is 45 seconds.
        """
        limit = None if budget is None else time.monotonic() + budget
        first = self.seconds_left(limit, "connecting to the launcher")
        try:
            return self.open_socket(first)
        except OSError:
            if limit is not None and time.monotonic() >= limit:
                raise TimeoutError("the budget ended while connecting to the launcher") from None
            if not self.start:
                raise ClientError("the launcher is not running", "unavailable") from None
        self.start_launcher(None if limit is None else self.seconds_left(limit, "starting the launcher"))
        deadline = time.monotonic() + 45 if limit is None else min(time.monotonic() + 45, limit)
        while (left := deadline - time.monotonic()) > 0:
            try:
                return self.open_socket(None if limit is None else left)
            except OSError:
                if limit is None:
                    time.sleep(0.1)
                elif (remaining := deadline - time.monotonic()) > 0:
                    time.sleep(min(0.1, remaining))
        if limit is not None and time.monotonic() >= limit:
            raise TimeoutError("the budget ended while the launcher was starting")
        raise ClientError(f"the launcher did not start: {self.log_tail()}", "unavailable")

    def open_socket(self, timeout: float | None = None) -> socket.socket:
        """One connection attempt; with a timeout, connection establishment is bounded too."""
        sock = self.socket_factory(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            if timeout is not None:
                sock.settimeout(timeout)
            sock.connect(str(self.config.socket_path))
        except OSError:
            sock.close()
            raise
        return sock

    def log_tail(self) -> str:
        """The end of the launcher's log, for a failed start."""
        log = self.config.registry.path / "launcher.log"
        return log.read_text()[-400:].strip() if log.is_file() else "no log"

    REAP_SECONDS = 0.5
    REAP_POLL = 0.005

    def start_launcher(self, budget: float | None = None) -> None:
        """Start ``serve`` detached from this process: fork, new session, spawn, exit the middle process.

        With ``budget`` seconds the wait for the middle process ends within them. One still running then gets SIGTERM and SIGKILL, each followed by a
        wait of up to ``REAP_SECONDS`` for it to end, so the call can take up to two of those waits longer than ``budget`` (a slow scheduler or kernel
        adds to that). It raises ``StartTimeout`` naming the process, the signals attempted and whether the process was reaped: after both signals
        it can remain unreaped. A launcher it had already spawned is detached and may keep running. Without a budget the wait has no limit.
        """
        limit = None if budget is None else time.monotonic() + budget
        self.config.registry.path.mkdir(parents=True, exist_ok=True)
        self.config.registry.path.chmod(0o700)
        log = str(self.config.registry.path / "launcher.log")
        arguments = [sys.executable, "-B", "-m", "orchestration.launcher"]
        if self.config.registry.kind == "test":
            arguments += ["--test-registry", str(self.config.registry.path)]
        arguments.append("serve")
        environment = {name: os.environ[name] for name in ("PATH", "HOME", "DOCKER_HOST", "TMPDIR") if name in os.environ}
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        if self.launch_nonce:
            environment[LAUNCH_NONCE_VARIABLE] = self.launch_nonce
        middle = os.fork()
        if middle == 0:
            try:
                os.setsid()
                os.chdir(self.config.root)
                actions = [
                    (os.POSIX_SPAWN_OPEN, 0, os.devnull, os.O_RDONLY, 0),
                    (os.POSIX_SPAWN_OPEN, 1, log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600),
                    (os.POSIX_SPAWN_DUP2, 1, 2),
                ]
                os.posix_spawn(sys.executable, arguments, environment, file_actions=actions)
            finally:
                os._exit(0)
        if limit is None:
            os.waitpid(middle, 0)
        elif not self.reap_within(middle, limit - time.monotonic()):
            raise StartTimeout(self.discard_child(middle))

    def reap_within(self, pid: int, seconds: float) -> bool:
        """Reap a child of this process without blocking, polling for up to ``seconds``; False when it is still running."""
        limit = time.monotonic() + seconds
        while True:
            if os.waitpid(pid, os.WNOHANG)[0]:
                return True
            left = limit - time.monotonic()
            if left <= 0:
                return False
            time.sleep(min(self.REAP_POLL, left))

    def discard_child(self, pid: int) -> str:
        """Send a middle process that outlived the budget SIGTERM, then SIGKILL, waiting up to ``REAP_SECONDS`` after each for it to end.

        Returns the attempts and the observed outcome (reaped, no such child or not reaped), crediting no signal with an exit, and that a launcher the
        process had already spawned may still be running.
        """
        attempts: list[str] = []
        for sent in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.kill(pid, sent)
            except OSError as error:
                attempts.append(f"{sent.name} not sent ({type(error).__name__})")
            else:
                attempts.append(f"{sent.name} sent")
            try:
                if self.reap_within(pid, self.REAP_SECONDS):
                    observed = "the process then ended and was reaped"
                    break
            except ChildProcessError:
                observed = "waitpid reported no such child, so the process was already gone"
                break
        else:
            observed = f"the process had not ended {self.REAP_SECONDS:g} s after the last signal and was not reaped"
        return (
            f"the budget ended while the launcher was being started and the intermediate process {pid} had not ended. {', '.join(attempts)}; {observed}. "
            "A launcher that process had already spawned is detached and may still be running."
        )

    # -- requests --------------------------------------------------------------------------------------------------

    def next_id(self) -> str:
        """A fresh client request ID."""
        self.counter += 1
        return f"{os.getpid()}-{uuid.uuid4().hex[:10]}-{self.counter}"

    def principal_run(self, run_id: str, owner: bool = False) -> dict[str, Any]:
        """A run principal: the token comes from the token store, never from the environment."""
        return {"kind": "run", "run_id": run_id, "token": read_token_file(self.config.token_root, run_id, self.config.token_anchor), "owner": owner}

    @staticmethod
    def principal_alloc(alloc_id: str) -> dict[str, Any]:
        """An allocation principal: the holder or a descendant."""
        return {"kind": "alloc", "alloc_id": alloc_id}

    def call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        principal: dict[str, Any] | None = None,
        request_id: str | None = None,
        timeout: float = 120.0,
        budget: float | None = None,
    ) -> Any:
        """One request and its reply; a refusal raises ``ClientError``.

        ``timeout`` bounds each exchange. With ``budget`` seconds the whole request (connecting, starting the launcher, each exchange and every pause
        before a repeat) ends within them, an exchange gets at most the time left and a spent budget raises ``TimeoutError``. Cleaning up an
        intermediate launcher process that outlived the budget can add up to two ``REAP_SECONDS`` waits (1.0 s in all) beyond it (``start_launcher``).
        """
        try:
            result = self.send(method, params, principal, request_id, timeout, budget)
        except (ClientError, OSError) as error:
            self.note_failure(error, principal)
            raise
        self.forget_failure(principal)
        return result

    def send(
        self, method: str, params: dict[str, Any] | None, principal: dict[str, Any] | None, request_id: str | None, timeout: float, budget: float | None = None
    ) -> Any:
        request_id = request_id or self.next_id()
        begun = time.monotonic()
        deadline = begun + 60
        limit = None if budget is None else begun + budget
        while True:
            sock = self.connect(self.seconds_left(limit, f"connecting for {method}"))
            try:
                left = self.seconds_left(limit, f"the reply to {method}")
                return self.exchange(sock, method, params or {}, principal or {"kind": "any"}, request_id, timeout if left is None else min(timeout, left))
            except ClientError as error:
                # A launcher that has just started refuses work until its probe and scan are done; the same request is sent again.
                if error.code != "degraded" or "is starting" not in str(error):
                    raise
                if limit is not None and time.monotonic() >= limit:
                    raise TimeoutError(f"the budget ended while the launcher was starting, before {method} was answered") from error
                if time.monotonic() > deadline:
                    raise
            finally:
                sock.close()
            time.sleep(0.2 if limit is None else max(0.0, min(0.2, limit - time.monotonic())))

    def note_exchange(self, method: str, params: Mapping[str, Any], seconds: float, outcome: str) -> None:
        asked = float(params.get("wait") or 0.0) if method == "status" else 0.0
        if seconds - asked > self.config.timings.slow_exchange:
            CLIENT_LOG.warning("launcher exchange %s %s %.2f s%s", method, outcome, seconds, f" (asked to wait {asked:.2f} s)" if asked else "")

    def note_failure[E: BaseException](self, error: E, principal: Mapping[str, Any] | None, attempt_id: str | None = None) -> E:
        """Remember a lost connection for the principal whose exchange failed, so its cleanup is bounded; any other error is passed through unchanged.

        The record of a principal (its key, with the cleanup deadline once set) stays until its own work is settled: another worker's exchange does
        not clear it or move its deadline. With ``attempt_id``, the accepted attempt whose wait failed is marked abandoned in the same lock interval,
        so a sibling's success cannot clear the failure before the attempt's cleanup is recorded.
        """
        if isinstance(error, OSError) or (isinstance(error, ClientError) and error.transport):
            with self.live_lock, self.lost_lock:
                if attempt_id is not None and attempt_id in self.live:
                    self.abandoned.add(attempt_id)
                self.lost.setdefault(principal_key(principal), None)
        return error

    def forget_failure(self, principal: Mapping[str, Any] | None) -> None:
        """An exchange of this principal succeeded, which ends a failure with no principal and the principal's own unless work of it was left open.

        Left open means an ambiguous submit or an attempt that its worker's wait gave up on.
        """
        key = principal_key(principal)
        with self.live_lock, self.lost_lock:
            owes = any(principal_key(owner) == key for _, owner in self.pending_submits.values()) or any(
                principal_key(self.live[attempt_id]) == key for attempt_id in self.abandoned if attempt_id in self.live
            )
            self.lost.pop(NO_PRINCIPAL, None)
            if not owes:
                self.lost.pop(key, None)

    def lost_contact(self, principal: Mapping[str, Any] | None) -> bool:
        """True when this principal (or an exchange with none) lost the connection to the launcher and no interrupted pool owns the cleanup."""
        if self.cancelling or self.cancel_unresolved.is_set():
            return False
        with self.lost_lock:
            return principal_key(principal) in self.lost or NO_PRINCIPAL in self.lost

    def cleanup_deadline(self, principal: Mapping[str, Any]) -> float:
        """When the cleanup of this principal must end: ``cleanup_budget`` after its first cleanup step, whatever other workers complete."""
        key = principal_key(principal)
        with self.lost_lock:
            key = key if key in self.lost else NO_PRINCIPAL
            until = self.lost.get(key)
            if until is None:
                until = time.monotonic() + self.config.timings.cleanup_budget
                if key in self.lost:
                    self.lost[key] = until
            return until

    def probe(self, seconds: float) -> str:
        """Send a liveness request, which the launcher answers without reading its registry, and log it like any exchange.

        Returns ``"gone"`` when nothing listens at the socket, ``"alive"`` when any reply arrived within ``seconds`` and ``"silent"`` otherwise.
        """
        begun = time.monotonic()
        state = self.probe_once(seconds)
        outcome = {"alive": "took", "silent": "got no reply after", "gone": "found no listener after"}[state]
        self.note_exchange("live", {}, time.monotonic() - begun, outcome)
        return state

    def probe_once(self, seconds: float) -> str:
        deadline = time.monotonic() + seconds
        message = {"v": PROTOCOL, "id": self.next_id(), "method": "live", "params": {}, "principal": {"kind": "any"}}
        try:
            with self.open_socket(seconds) as sock:
                sock.settimeout(max(0.05, deadline - time.monotonic()))
                sock.sendall(json.dumps(message).encode() + b"\n")
                buffer = b""
                while b"\n" not in buffer:
                    if time.monotonic() >= deadline:
                        return "silent"
                    sock.settimeout(max(0.05, deadline - time.monotonic()))
                    chunk = sock.recv(65536)
                    if not chunk:
                        return "silent"
                    buffer += chunk
        except (FileNotFoundError, ConnectionRefusedError):
            return "gone"
        except OSError:
            return "silent"
        return "alive"

    def exchange(self, sock: socket.socket, method: str, params: dict[str, Any], principal: dict[str, Any], request_id: str, timeout: float) -> Any:
        """Send a request on an open socket and read its reply; ``timeout`` bounds the whole exchange, not each read."""
        begun = time.monotonic()
        outcome = "took"
        try:
            return self.exchange_within(sock, method, params, principal, request_id, timeout)
        except TimeoutError:
            outcome = "timed out after"
            raise
        finally:
            self.note_exchange(method, params, time.monotonic() - begun, outcome)

    def exchange_within(self, sock: socket.socket, method: str, params: dict[str, Any], principal: dict[str, Any], request_id: str, timeout: float) -> Any:
        deadline = time.monotonic() + timeout

        def remaining() -> float:
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError(f"{method}: no complete reply within {timeout} s")
            return left

        message = {"v": PROTOCOL, "id": request_id, "method": method, "params": params, "principal": principal}
        sock.settimeout(remaining())
        sock.sendall(json.dumps(message).encode() + b"\n")
        buffer = b""
        while b"\n" not in buffer:
            # A peer that sends a little before each read times out cannot stretch the exchange: every read gets only what is left.
            sock.settimeout(remaining())
            chunk = sock.recv(65536)
            if not chunk:
                raise ClientError("the launcher closed the connection", "unavailable")
            buffer += chunk
        reply = json.loads(buffer.split(b"\n", 1)[0])
        if not reply.get("ok"):
            raise ClientError(str(reply.get("error")), str(reply.get("code", "refused")))
        return reply["result"]

    def wait_for(self, what: str, record_id: str, field_name: str, values: Sequence[str], principal: dict[str, Any], seconds: float) -> dict[str, Any]:
        """Poll ``status`` with a wait until a field reaches one of the values or the time ends."""
        deadline = time.monotonic() + seconds
        while True:
            left = deadline - time.monotonic()
            record = self.call(
                "status",
                {"what": what, "id": record_id, "wait": min(30.0, max(0.1, left)), "until": {"field": field_name, "values": list(values)}},
                principal,
                timeout=60,
            )
            if str(record.get(field_name)) in values or left <= 0:
                return dict(record)

    # -- convenience -----------------------------------------------------------------------------------------------

    def allocate(self, workers: int = 1, seconds: float = 120.0) -> str:
        """Take the machine for an allocation and wait until it is held; the holder is this process."""
        reply = self.call("allocate", {"workers": workers})
        record = self.wait_for("allocation", reply["alloc_id"], "state", ("held", "failed"), self.principal_alloc(reply["alloc_id"]), seconds)
        if record["state"] != "held":
            raise ClientError(str(record.get("error", f"allocation {reply['alloc_id']} is {record['state']}")), "budget")
        return str(reply["alloc_id"])

    def release_allocation(self, alloc_id: str) -> ReleaseOutcome:
        """Release an allocation and report what happened (``ReleaseOutcome``).

        Without a lost connection this is an ordinary call, and a refusal raises. After a lost connection the work of the allocation is settled first
        (see ``settle``) and the release is one bounded request; that path never raises, so the original failure reaches the caller.
        """
        principal = self.principal_alloc(alloc_id)
        if not self.lost_contact(principal):
            self.call("release_allocation", {}, principal, timeout=120)
            return ReleaseOutcome.RELEASED
        budget = self.config.timings.cleanup_budget
        deadline = self.cleanup_deadline(principal)
        if not self.settle(principal):
            CLIENT_LOG.warning("allocation %s retained: its work was not settled within %g s of the launcher failure", alloc_id, budget)
            return ReleaseOutcome.RETAINED
        try:
            self.cleanup_exchange("release_allocation", {}, principal, self.next_id(), deadline)
        except (ClientError, OSError) as error:
            CLIENT_LOG.warning(
                "allocation %s release unresolved: no confirmation within %g s of the launcher failure (%s)",
                alloc_id,
                budget,
                error.code if isinstance(error, ClientError) else type(error).__name__,
            )
            return ReleaseOutcome.UNRESOLVED
        with self.lost_lock:
            self.lost.pop(principal_key(principal), None)
        return ReleaseOutcome.RELEASED

    def cleanup_exchange(self, method: str, params: dict[str, Any], principal: dict[str, Any], request_id: str, deadline: float) -> Any:
        """One exchange with the existing socket that ends by ``deadline``; it never starts a launcher."""
        left = deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError(f"{method}: the cleanup budget is spent")
        return self.bounded_call(method, params, principal, request_id, min(EXCHANGE_BUDGET, left))

    def cleanup_call(self, method: str, params: dict[str, Any], principal: dict[str, Any], timeout: float = 120.0) -> Any:
        """A request that undoes earlier work: an ordinary call, or after a lost connection one bounded exchange whose failure is logged, not raised."""
        if not self.lost_contact(principal):
            return self.call(method, params, principal, timeout=timeout)
        try:
            return self.cleanup_exchange(method, params, principal, self.next_id(), self.cleanup_deadline(principal))
        except (ClientError, OSError) as error:
            CLIENT_LOG.warning(
                "launcher cleanup %s failed after a launcher failure (%s)", method, error.code if isinstance(error, ClientError) else type(error).__name__
            )
            return None

    def settle(self, principal: dict[str, Any]) -> bool:
        """After a lost connection, settle the work of one principal inside its cleanup deadline; True when nothing of it is ambiguous or open.

        An ambiguous submit is replayed under its own request ID: a refusal drops it, a reply gives its attempt. Each open attempt is cancelled and
        awaited. Work of other principals on this client is not touched.
        """
        if not self.lost_contact(principal):
            return True
        deadline = self.cleanup_deadline(principal)
        key = principal_key(principal)
        cancelled: set[str] = set()
        while time.monotonic() < deadline:
            with self.live_lock:
                pending = {request_id: item for request_id, item in self.pending_submits.items() if principal_key(item[1]) == key}
                attempts = [attempt_id for attempt_id, owner in self.live.items() if principal_key(owner) == key]
            if not pending and not attempts:
                return True
            for request_id, (payload, owner) in pending.items():
                try:
                    reply = self.cleanup_exchange("submit", payload, owner, request_id, deadline)
                except ClientError as error:
                    if not error.transport:
                        with self.live_lock:
                            self.pending_submits.pop(request_id, None)
                    continue
                except OSError:
                    continue
                with self.live_lock:
                    self.pending_submits.pop(request_id, None)
                    self.live[str(reply["attempt_id"])] = owner
            for attempt_id in attempts:
                try:
                    record = self.cleanup_exchange("status", {"what": "attempt", "id": attempt_id}, principal, self.next_id(), deadline)
                    if record.get("phase") == "ended":
                        with self.live_lock:
                            self.live.pop(attempt_id, None)
                            self.abandoned.discard(attempt_id)
                    elif attempt_id not in cancelled:
                        self.cleanup_exchange("cancel", {"attempt_id": attempt_id}, principal, f"cleanup-cancel-{attempt_id}", deadline)
                        cancelled.add(attempt_id)
                except (ClientError, OSError):
                    continue
            time.sleep(0.05)
        return False

    def run_attempt(
        self,
        principal: dict[str, Any],
        template: str,
        launch_id: str,
        args: Sequence[str],
        params: dict[str, Any],
        *,
        checkout: Path | None = None,
        seconds: float | None = None,
    ) -> AttemptResult:
        """Submit one attempt and wait for it to end; returns its saved output.

        The attempt is tracked while this call waits, so ``cancel_open`` can persist a cancel for every attempt a client has open.
        """
        payload = {
            "template": template,
            "launch_id": launch_id,
            "args": list(args),
            "params": params,
            "checkout": str(checkout or self.config.root),
            **({"timeout_seconds": int(seconds)} if seconds else {}),
        }
        request_id = self.next_id()
        with self.live_lock:
            if self.cancelling:
                raise ClientError("the pool is cancelling; no further submits", "cancelled")
            self.pending_submits[request_id] = (payload, principal)
        patience = Patience(self, principal)
        while True:
            try:
                reply = patience.call("submit", payload, principal, request_id, patience.budget())
                break
            except ClientError as error:
                if error.code != "unavailable":
                    with self.live_lock:
                        self.pending_submits.pop(request_id, None)
                    raise
                if self.cancelling:
                    raise
                patience.failed(error)
            except OSError as error:
                # The request may already be persisted. Keep its exact identity for cancel_open to replay and reconcile.
                if self.cancelling:
                    raise
                patience.failed(error)
            # The same request ID again: the launcher returns the persisted reply instead of creating a second attempt.
            time.sleep(0.2)
        attempt_id = str(reply["attempt_id"])
        with self.live_lock:
            self.pending_submits.pop(request_id, None)
            self.live[attempt_id] = principal
        if self.cancelling:
            self.cancel_open()
        ended = False
        unreachable = False
        try:
            while True:
                if self.cancel_unresolved.is_set():
                    raise ClientError(f"cancellation unresolved for {attempt_id}; allocation retained", "cleanup_unresolved")
                settle_deadline = self.settle_deadline()
                if settle_deadline is not None and time.monotonic() >= settle_deadline:
                    # The limit applies whatever the launcher answers: a reachable launcher whose cleanup hangs is no reason to wait without end.
                    with self.live_lock:
                        unresolved = sorted(self.live)
                    reason = (
                        "the launcher stayed unreachable after the cancellations persisted"
                        if unreachable
                        else "an attempt did not end within cancel_settle after the cancellations persisted"
                    )
                    self.record_unresolved(unresolved, [], reason)
                budget = patience.budget(settle_deadline)
                failure: BaseException | None = None
                try:
                    record = patience.call(
                        "status",
                        {"what": "attempt", "id": attempt_id, "wait": min(0.2, budget / 2), "until": {"field": "phase", "values": ["ended"]}},
                        principal,
                        self.next_id(),
                        budget,
                    )
                except OSError as error:
                    failure = error
                except ClientError as error:
                    if error.code != "unavailable":
                        raise
                    failure = error
                unreachable = failure is not None
                if failure is not None and not self.cancelling and not self.cancel_unresolved.is_set():
                    patience.failed(failure, attempt_id)
                    time.sleep(0.2)
                    continue
                if failure is not None:
                    if not self.cancelling or self.cancel_unresolved.is_set():
                        raise failure
                    # After the cancellations persisted, an unreachable launcher is retried until the same settle limit only.
                    if settle_deadline is None:
                        self.cancel_persisted_at = self.cancel_persisted_at or time.monotonic()
                    elif time.monotonic() >= settle_deadline:
                        with self.live_lock:
                            unresolved = sorted(self.live)
                        self.record_unresolved(unresolved, [], "the launcher stayed unreachable after the cancellations persisted")
                    time.sleep(0.05)
                    continue
                if record["phase"] == "ended":
                    ended = True
                    break
        finally:
            with self.live_lock:
                if ended:
                    self.live.pop(attempt_id, None)
                else:
                    self.abandoned.add(attempt_id)
        return self.result_of(record)

    def settle_deadline(self) -> float | None:
        """When an interrupted pool stops waiting for its cancelled attempts: ``cancel_settle`` after the cancellations persisted."""
        return None if self.cancel_persisted_at is None else self.cancel_persisted_at + self.config.timings.cancel_settle

    def bounded_call(self, method: str, params: dict[str, Any], principal: dict[str, Any], request_id: str, seconds: float) -> Any:
        """Exchange with the existing socket only, within one budget that covers connecting and the whole exchange."""
        deadline = time.monotonic() + seconds
        with self.open_socket(seconds) as sock:
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError(f"{method}: the connection used the whole {seconds} s budget")
            return self.exchange(sock, method, params, principal, request_id, left)

    def cancel_open(self) -> list[str]:
        """Reconcile ambiguous submits and persist each cancellation before a pool may join; failure retains admission and names unresolved work."""
        self.cancelling = True
        with self.cancel_lock:
            deadline = time.monotonic() + 5.0
            cancelled: set[str] = set()
            while time.monotonic() < deadline:
                with self.live_lock:
                    pending = dict(self.pending_submits)
                for request_id, (payload, principal) in pending.items():
                    left = deadline - time.monotonic()
                    if left <= 0:
                        break
                    try:
                        reply = self.bounded_call("submit", payload, principal, request_id, min(0.5, left))
                    except (ClientError, OSError):
                        continue
                    with self.live_lock:
                        self.pending_submits.pop(request_id, None)
                        self.live[str(reply["attempt_id"])] = principal
                with self.live_lock:
                    attempts = dict(self.live)
                for attempt_id, principal in attempts.items():
                    if attempt_id in cancelled:
                        continue
                    left = deadline - time.monotonic()
                    if left <= 0:
                        break
                    try:
                        reply = self.bounded_call("cancel", {"attempt_id": attempt_id}, principal, f"pool-cancel-{attempt_id}", min(0.5, left))
                    except (ClientError, OSError):
                        # Lost reply: read the persisted intent without resubmitting a different logical cancellation.
                        left = deadline - time.monotonic()
                        if left <= 0:
                            break
                        try:
                            reply = self.bounded_call("status", {"what": "attempt", "id": attempt_id}, principal, self.next_id(), min(0.5, left))
                        except (ClientError, OSError):
                            continue
                    if reply.get("cancel_intent") or reply.get("phase") == "ended":
                        cancelled.add(attempt_id)
                with self.live_lock:
                    unresolved = sorted((set(self.live) - cancelled) | {f"submit:{request_id}" for request_id in self.pending_submits})
                if not unresolved:
                    self.cancel_persisted_at = self.cancel_persisted_at or time.monotonic()
                    return sorted(cancelled)
                time.sleep(min(0.05, max(0, deadline - time.monotonic())))
            self.record_unresolved(unresolved, sorted(cancelled), "a cancellation was not persisted within the cancel window")

    def record_unresolved(self, unresolved: list[str], cancelled: list[str], reason: str) -> NoReturn:
        """Fail visibly: mark the pool unresolved, write the record and keep admission (the allocation is never released here)."""
        self.cancel_unresolved.set()
        record = {"status": "cleanup_unresolved", "reason": reason, "unresolved": unresolved, "cancelled": cancelled, "allocation_retained": True}
        (self.config.registry.path / "pool_cancel_unresolved.json").write_text(json.dumps(record, indent=2) + "\n")
        raise ClientError(f"cancellation unresolved: {', '.join(unresolved)}; admission retained; pool_cancel_unresolved.json", "cleanup_unresolved")

    @staticmethod
    def result_of(record: dict[str, Any]) -> AttemptResult:
        """Read the saved output of an ended attempt."""
        folder = Path(record["log_tail"]).parent if record.get("log_tail") else None
        stdout = (folder / "stdout.txt").read_text(errors="replace") if folder and (folder / "stdout.txt").is_file() else ""
        stderr = (folder / "stderr.txt").read_text(errors="replace") if folder and (folder / "stderr.txt").is_file() else ""
        return AttemptResult(record["id"], str(record["reason"]), record.get("exit_code"), stdout, stderr, record)


def load_client(test_registry: str | None = None, start: bool = True) -> LauncherClient:
    """A client for the real registry, or a test registry."""
    return LauncherClient(resolve_config(test_registry), start=start)


def parser() -> argparse.ArgumentParser:
    """The command line."""
    cli = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    cli.add_argument("--test-registry", help="a test registry folder; it can never hold a real run")
    commands = cli.add_subparsers(dest="command", required=True)
    commands.add_parser("serve", help="run the launcher in the foreground")
    status = commands.add_parser("status", help="print the launcher, registry or a record")
    status.add_argument("--what", default="launcher", choices=("launcher", "registry", "catalog", "orchestration", "protected_sets"))
    commands.add_parser("plan", help="print the memory plan the launcher computes now")
    commands.add_parser("catalog-up", help="start the catalog services and wait")
    commands.add_parser("catalog-down", help="stop the catalog services and wait")
    restart = commands.add_parser("docker-restart", help="restart Docker Desktop when nothing of this registry runs")
    restart.add_argument("--owner", action="store_true", required=True)
    commands.add_parser("shutdown", help="stop the launcher").add_argument("--owner", action="store_true", required=True)
    commands.add_parser(
        "dbt", help="dbt: `deps` runs the dbt_deps template; any other command needs a real run with recorded pins and a complete snapshot"
    ).add_argument("args", nargs="*")
    query = commands.add_parser(
        "query",
        help="an interactive DuckDB shell on the bronze tables (query_reader)",
        description="Open a DuckDB shell on the bronze tables, or run one statement. Every word after `query` goes to DuckDB unchanged.",
        epilog=f"supported DuckDB arguments: -c SQL (run one statement and exit) and the output flags {' '.join(DUCKDB_QUERY_FLAGS)}.\n"
        "A file or pipe on standard input is the shell's input.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    query.add_argument("args", nargs="*", metavar="DUCKDB_ARG", help="-c SQL and the output flags above")
    commands.add_parser("ui", help="the DuckDB UI on 127.0.0.1:4213 (ui_reader)")
    run = commands.add_parser("run", help="take an allocation, run one attempt of a template and print its output")
    run.add_argument("--template", required=True, choices=sorted(set(TEMPLATES) - {"probe", "catalog_service", "orchestration_service"}))
    run.add_argument("--launch-id", default=None)
    run.add_argument("--param", action="append", default=[], metavar="NAME=JSON")
    run.add_argument("--seconds", type=int)
    run.add_argument("--workers", type=int, default=1)
    run.add_argument("args", nargs="*")
    return cli


def passthrough_split(argv: list[str]) -> tuple[list[str], list[str] | None]:
    """Split ``argv`` at the ``query`` command: the words after it go to DuckDB unchanged (argparse cannot pass a leading ``-c`` through)."""
    index = 0
    while index < len(argv) and argv[index].startswith("-"):
        if argv[index] == "--test-registry":
            index += 2
        elif argv[index].startswith("--test-registry="):
            index += 1
        else:
            return argv, None
    if index < len(argv) and argv[index] == "query":
        tail = argv[index + 1 :]
        if tail[:1] in (["-h"], ["--help"]):
            return argv, None
        return argv[: index + 1], tail[1:] if tail[:1] == ["--"] else tail
    return argv, None


def main() -> int:
    """Run one command; refusals exit 3, errors 4, a degraded launcher 5."""
    head, passthrough = passthrough_split(sys.argv[1:])
    args = parser().parse_args(head)
    if passthrough is not None:
        args.args = passthrough
    try:
        if args.command == "serve":
            return Launcher(resolve_config(args.test_registry)).serve()
        client = load_client(args.test_registry)
        if args.command == "status":
            result: Any = client.call("status", {"what": args.what})
        elif args.command == "plan":
            result = client.call("plan")
        elif args.command == "catalog-up":
            client.call("catalog_up")
            result = client.wait_for("catalog", "", "phase", ("up", "down"), {"kind": "any"}, 600)
        elif args.command == "catalog-down":
            client.call("catalog_down")
            result = client.wait_for("catalog", "", "phase", ("down",), {"kind": "any"}, 600)
        elif args.command == "docker-restart":
            result = client.call("docker_restart", {"owner": True}, {"kind": "any", "owner": True}, timeout=900)
        elif args.command == "shutdown":
            result = client.call("shutdown", {"owner": True}, {"kind": "any", "owner": True})
        elif args.command == "dbt":
            return dbt_command(client, args.args)
        elif args.command in ("query", "ui"):
            return interactive_command(client, args.command, args.args if args.command == "query" else [])
        else:
            return run_command_line(client, args)
    except ClientError as error:
        sys.stderr.write(f"launcher: {error} [{error.code}]\n")
        return DEGRADED_EXIT if error.transport or error.code == "degraded" else REFUSED_EXIT
    except (run_lock.Refused, LauncherError) as error:
        sys.stderr.write(f"refused: {error}\n")
        return REFUSED_EXIT
    except (run_lock.RegistryError, OSError) as error:
        sys.stderr.write(f"registry error: {error}\n")
        return ERROR_EXIT
    sys.stdout.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return 0


def dbt_command(client: LauncherClient, arguments: list[str]) -> int:
    """`dbt deps` is an attempt of ``dbt_deps`` on the shared package folder; every other command is a ``real_dbt`` launch of a real run."""
    root = client.config.root
    if arguments[:1] == ["deps"]:
        main = run_lock.main_checkout(root) if client.config.registry.kind == "real" else root
        packages = main / "data" / "analytics" / "dbt" / "dbt_packages"
        packages.mkdir(parents=True, exist_ok=True)
        allocation = client.allocate()
        try:
            result = client.run_attempt(
                client.principal_alloc(allocation), "dbt_deps", f"deps-{uuid.uuid4().hex[:8]}", ["deps"], {"packages_host": str(packages)}, checkout=root
            )
        finally:
            client.release_allocation(allocation)
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        return result.exit_code if result.exit_code is not None else ERROR_EXIT
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    mode = "fixture" if client.config.registry.kind == "test" else "real"
    client.call("acquire", {"domain": "transform", "run_id": run_id, "mode": mode, "checkout": str(root)})
    principal = client.principal_run(run_id)
    code = ERROR_EXIT
    try:
        client.call("catalog_up")
        client.wait_for("catalog", "", "phase", ("up", "down"), {"kind": "any"}, 900)
        result = client.run_attempt(principal, "real_dbt", "dbt-1", arguments, {}, checkout=root)
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        code = result.exit_code if result.exit_code is not None else ERROR_EXIT
    except ClientError as error:
        sys.stderr.write(f"launcher: {error} [{error.code}]\n")
        code = DEGRADED_EXIT if error.transport else REFUSED_EXIT
    finally:
        if code:
            client.settle(principal)
            client.cleanup_call("mark", {"state": "failed_retained"}, principal)
        else:
            record = client.config.runs_root / run_id / "run_record.json"
            record.parent.mkdir(parents=True, exist_ok=True)
            record.write_text(json.dumps({"run_id": run_id}))
            client.call("finalize", {}, principal)
            client.call("release", {"record": str(record)}, principal, timeout=300)
    return code


def interactive_command(client: LauncherClient, kind: str, arguments: list[str]) -> int:
    """`query` and `ui`: an allocation, a catalog reader registration and one session-bound attempt (D1 section 9)."""
    template = "query_reader" if kind == "query" else "ui_reader"
    client.call("catalog_up")
    record = client.wait_for("catalog", "", "phase", ("up", "down"), {"kind": "any"}, 900)
    if record.get("phase") != "up":
        sys.stderr.write(f"launcher: the catalog did not start: {record.get('error')}\n")
        return ERROR_EXIT
    allocation = client.allocate()
    principal = client.principal_alloc(allocation)
    registration = client.call("register_reader", {}, principal)["registration_id"]
    attempt_id = ""
    try:
        tty = kind == "query" and sys.stdin.isatty() and sys.stdout.isatty()
        reply = client.call(
            "submit",
            {
                "template": template,
                "launch_id": f"{kind}-{uuid.uuid4().hex[:8]}",
                "args": arguments,
                "params": {"tty": tty} if kind == "query" else {},
                "checkout": str(client.config.root),
            },
            principal,
        )
        attempt_id = str(reply["attempt_id"])
        if kind == "ui":
            sys.stderr.write("DuckDB UI starting at http://localhost:4213 (Ctrl-C to stop)\n")
        record = client.wait_for("attempt", attempt_id, "phase", ("started", "ended"), principal, 600)
        if record["phase"] == "started":
            relay_session(client, principal, attempt_id, tty)
        final = client.wait_for("attempt", attempt_id, "phase", ("ended",), principal, 120)
        return int(final.get("exit_code") or 0)
    finally:
        if attempt_id:
            with contextlib.suppress(ClientError):
                client.cleanup_call("cancel", {"attempt_id": attempt_id}, principal)
                if not client.lost_contact(principal):
                    client.wait_for("attempt", attempt_id, "phase", ("ended",), principal, 120)
        client.cleanup_call("deregister_reader", {"registration_id": registration}, principal)
        client.release_allocation(allocation)


def relay_session(client: LauncherClient, principal: dict[str, Any], attempt_id: str, tty: bool) -> None:
    """Relay the terminal to a session: input goes to the launcher, output comes back, Ctrl-C is a session message, never a raw signal.

    Input is read in chunks of ``INPUT_CHUNK`` bytes only while less than ``SESSION_BUFFER_LIMIT`` waits to be sent, and the socket is written
    only when it accepts bytes, so output keeps draining while a large input is sent (failure modes SR1 to SR5). A standard input that cannot
    be watched (``/dev/null`` on macOS, any regular file on Linux) is read in the same loop, one chunk per pass.
    """
    import termios

    sock = client.connect()
    saved = termios.tcgetattr(sys.stdin.fileno()) if tty else None
    selector = selectors.DefaultSelector()
    outgoing, buffer = bytearray(), b""
    stdin_open, stdin_registered, launcher_events = True, False, selectors.EVENT_READ

    def relay_lines() -> bool:
        """Write the output of every complete line in ``buffer``; True once the launcher says the session is closed."""
        nonlocal buffer
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            message = json.loads(line)
            if "out" in message:
                sys.stdout.buffer.write(base64.b64decode(message["out"]))
                sys.stdout.buffer.flush()
            elif "closed" in message:
                return True
        return False

    try:
        request = {"v": PROTOCOL, "id": client.next_id(), "method": "session", "params": {"attempt_id": attempt_id}, "principal": principal}
        sock.sendall(json.dumps(request).encode() + b"\n")
        while b"\n" not in buffer:
            chunk = sock.recv(65536)
            if not chunk:
                raise ClientError("the launcher closed the connection before it answered", "unavailable")
            buffer += chunk
        first, _, buffer = buffer.partition(b"\n")
        reply = json.loads(first)
        if not reply.get("ok"):
            raise ClientError(str(reply.get("error")), str(reply.get("code", "refused")))
        sock.setblocking(False)
        selector.register(sock, launcher_events, "launcher")
        try:
            selector.register(sys.stdin, selectors.EVENT_READ, "stdin")
            watched = stdin_registered = True
        except (OSError, ValueError):
            watched = False
        if tty:
            import tty as tty_module

            tty_module.setraw(sys.stdin.fileno())
        if relay_lines():
            return
        while True:
            room = stdin_open and len(outgoing) < SESSION_BUFFER_LIMIT
            if watched and room != stdin_registered:
                if room:
                    selector.register(sys.stdin, selectors.EVENT_READ, "stdin")
                else:
                    selector.unregister(sys.stdin)
                stdin_registered = room
            wanted = selectors.EVENT_READ | (selectors.EVENT_WRITE if outgoing else 0)
            if wanted != launcher_events:
                selector.modify(sock, wanted, "launcher")
                launcher_events = wanted
            for key, mask in selector.select(timeout=0 if room and not watched else None):
                if key.data == "stdin":
                    data = os.read(sys.stdin.fileno(), INPUT_CHUNK)
                    if data:
                        outgoing += session_input_line(data, tty)
                    else:
                        stdin_open = False
                    continue
                if mask & selectors.EVENT_WRITE:
                    try:
                        del outgoing[: sock.send(outgoing)]
                    except BlockingIOError:
                        pass
                    except (BrokenPipeError, ConnectionResetError):
                        outgoing.clear()
                        stdin_open = False
                if mask & selectors.EVENT_READ:
                    try:
                        chunk = sock.recv(65536)
                    except BlockingIOError:
                        continue
                    if not chunk:
                        return
                    buffer += chunk
                    if relay_lines():
                        return
            if room and not watched:
                data = os.read(sys.stdin.fileno(), INPUT_CHUNK)
                if data:
                    outgoing += session_input_line(data, tty)
                else:
                    stdin_open = False
    except KeyboardInterrupt:
        sock.settimeout(5)
        sock.sendall(json.dumps({"signal": "INT"}).encode() + b"\n")
    finally:
        if saved is not None:
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, saved)
        selector.close()
        sock.close()


def session_input_line(data: bytes, tty: bool) -> bytes:
    """One line of the session protocol for a chunk of input; outside a terminal a lone Ctrl-C is the interrupt signal."""
    message = {"signal": "INT"} if data == b"\x03" and not tty else {"in": base64.b64encode(data).decode()}
    return json.dumps(message).encode() + b"\n"


def run_command_line(client: LauncherClient, args: argparse.Namespace) -> int:
    """``run``: allocation, one attempt, release; the attempt's output is printed and its exit code returned."""
    params: dict[str, Any] = {}
    for item in args.param:
        name, _, value = item.partition("=")
        params[name] = json.loads(value)
    template = TEMPLATES[args.template]
    if template.needs_catalog:
        client.call("catalog_up")
        record = client.wait_for("catalog", "", "phase", ("up", "down"), {"kind": "any"}, 900)
        if record.get("phase") != "up":
            sys.stderr.write(f"launcher: the catalog did not start: {record.get('error')}\n")
            return ERROR_EXIT
    alloc_id = client.allocate(args.workers)
    principal = client.principal_alloc(alloc_id)
    registration = ""
    try:
        if template.reader:
            registration = str(client.call("register_reader", {}, principal)["registration_id"])
        result = client.run_attempt(principal, args.template, args.launch_id or f"cli-{uuid.uuid4().hex[:8]}", args.args, params, seconds=args.seconds)
    finally:
        if registration:
            client.settle(principal)
            client.cleanup_call("deregister_reader", {"registration_id": registration}, principal)
        client.release_allocation(alloc_id)
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    return result.exit_code if result.exit_code is not None else ERROR_EXIT


if __name__ == "__main__":
    sys.exit(main())
