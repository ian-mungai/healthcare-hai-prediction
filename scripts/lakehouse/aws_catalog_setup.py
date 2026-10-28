"""The owner's AWS catalog setup: create or verify the AWS catalog and report the ten AWS Polaris pair parameters, under the project profile only.

Run from the repository root at a terminal:

    .venv/bin/python -m scripts.lakehouse.aws_catalog_setup catalog --instance <HOST_INSTANCE_ID>
    .venv/bin/python -m scripts.lakehouse.aws_catalog_setup check [--instance <HOST_INSTANCE_ID>]
    .venv/bin/python -m scripts.lakehouse.aws_catalog_setup principal --identity reader|e2e|bronze|silver|gold --instance <HOST_INSTANCE_ID>
        [--namespace-approval <APPROVAL_ID>]

The AWS catalog is reached only through ``orchestration.catalog_transport.CatalogTransport``: the Session Manager tunnel to the host's private
port 8181 behind an owned loopback relay, after the network acceptance check. Neither local port may be the Mac Polaris port 8181, so a request
can never reach the Mac catalog. Every catalog request runs once through ``Route.execute`` within the run's absolute deadline. A lost reply
fails visibly and is never repeated inside a run; a rerun converges, because an existing catalog with the expected location and credential
mode is ``already present`` and one that differs is refused, never changed.

The tool refuses to start unless the configured profile resolves to the project user. The Polaris root credential is read from Parameter
Store into memory only and never printed, logged or written; errors carry the action and the HTTP status only. ``check`` reports each of the
ten AWS Polaris pair parameters by exact name with its type and version, never a value: it reads without decryption and drops the encrypted
value unread. With ``--instance`` it also reads the AWS catalog and each existing principal's grants and changes nothing.

``principal`` sets up one AWS identity: its principal role, catalog role, grants and credentials. The grants are the approved plan of the
identity (``bronze_writer.grant_plan`` for ``bronze`` and ``e2e``, which also gets the table grants on ``silver_e2e``, ``gold_e2e`` and
``features_e2e``; ``polaris_writer.grants`` on ``silver`` or ``gold`` only; the Mac reader privileges for ``reader``). Every namespace a grant
names must exist. An absent one is created only under ``--namespace-approval``, which the tool records once for that identity and its exact
namespace set before it creates anything; for ``bronze`` and ``e2e`` the approval also grants ``NAMESPACE_CREATE`` (phase ``active``). After the
grants the tool lists the catalog role's grants and compares them with the plan; any extra, missing or duplicate grant stops it before the
catalog role is assigned (P7). The issued pair goes from the Polaris reply straight to Parameter Store (``principal_credentials``), so a
stored pair that Polaris accepts is kept, and a missing, half-written or refused pair is reset (P3 to P5). Updates of one principal run one at
a time under a lock in the main checkout, taken within the step's deadline before any change (P11).
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import functools
import http.client
import json
import os
import re
import socket
import sys
import threading
import time
import urllib.parse
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from botocore.exceptions import BotoCoreError, ClientError

from orchestration import ecs_target, launcher
from orchestration.catalog_transport import CatalogTransport, TransportSpec
from scripts.acquisition.s3_store import AwsCli, StorageError
from scripts.infrastructure import secret_store
from scripts.lakehouse import bronze_writer, catalog, polaris_writer, principal_credentials, run_lock
from scripts.lakehouse.catalog_journal import durable_directory

CATALOG = "hai_lakehouse"
MAC_POLARIS_PORT = 8181
REPO_ROOT = Path(__file__).resolve().parents[2]
IDENTITIES = ("reader", "e2e", "bronze", "silver", "gold")
OAUTH_PATH = "/api/catalog/v1/oauth/tokens"
CATALOG_PATH = f"/api/management/v1/catalogs/{CATALOG}"
REQUEST_SECONDS = 30.0
READY_SECONDS = 120.0
CLOSE_SECONDS = 30.0
SECRET_SECONDS = 30.0
# The longest the credential step holds the launcher registry lock; the launcher waits for that lock at most run_lock.LOCK_WAIT_SECONDS.
REGISTRY_HOLD_SECONDS = 20.0
MAX_REPLY_BYTES = 1 << 20
MANAGEMENT = "/api/management/v1"
APPROVAL_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
E2E_TABLE_NAMESPACES = ("silver_e2e", "gold_e2e", "features_e2e")
PRINCIPALS = {"reader": "lakehouse_reader", "e2e": "test_writer", "bronze": "bronze_writer", "silver": "silver_writer", "gold": "gold_writer"}

Clock = Callable[[], float]
# Runs one dependent catalog request exactly once and returns its result.
Execute = Callable[[Callable[[], Any]], Any]
Request = Callable[..., tuple[int, Any]]


class SetupError(RuntimeError):
    """A setup step failed; ``code`` names the step and the message never holds a credential or a parameter value."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def direct[R](operation: Callable[[], R], /) -> R:
    """Run an operation with no transport in between (the stand-ins)."""
    return operation()


def check_deadline(deadline: float, clock: Clock = time.monotonic) -> float:
    """The seconds left before the absolute deadline; none left stops the step."""
    left = deadline - clock()
    if left <= 0:
        raise SetupError("deadline", "the catalog setup passed its deadline")
    return left


def pair_names(variables: Path) -> dict[str, tuple[str, str]]:
    """The client ID and secret parameter names of each AWS identity, from the variable defaults in ``infra/iam/variables.tf`` (P12).

    The ten names must be distinct, each must end in its own ``aws_polaris_<identity>_client_id`` or ``_client_secret`` leaf and none may be a
    managed Mac name (``polaris_silver_*``, ``polaris_gold_*`` and the others), so a Mac and an AWS writer can never share a pair.
    """
    names = secret_store.variable_names(variables)
    pairs: dict[str, tuple[str, str]] = {}
    for identity in IDENTITIES:
        found = tuple(names.get(f"AWS_POLARIS_{identity.upper()}_CLIENT_{part.upper()}_NAME", "") for part in ("id", "secret"))
        for name, part in zip(found, ("id", "secret"), strict=True):
            if not name.endswith(f"/aws_polaris_{identity}_client_{part}"):
                raise SetupError("parameters", f"the AWS {identity} client {part} name must be its own aws_polaris_{identity}_client_{part} parameter")
        pairs[identity] = (found[0], found[1])
    flat = [name for pair in pairs.values() for name in pair]
    if len(set(flat)) != len(flat) or set(flat) & {*secret_store.MANAGED, *secret_store.READ_ONLY}:
        raise SetupError("parameters", "the AWS Polaris pair names must be ten distinct names outside the managed Mac names")
    return pairs


@functools.cache
def configured_pairs() -> dict[str, tuple[str, str]]:
    """The checked pair names of the five AWS identities, read once from the committed variable defaults."""
    return pair_names(REPO_ROOT / secret_store.VARIABLES)


def writable_names() -> frozenset[str]:
    return frozenset(name for pair in configured_pairs().values() for name in pair)


class PairVault(secret_store.SecretStore):
    """Parameter Store through the project profile: writes only the ten AWS Polaris pair names and reads those and the Polaris root secret."""

    def __init__(self, parameters: Any, deadline: float | None = None, clock: Clock = time.monotonic) -> None:
        super().__init__(parameters)
        self.deadline, self.clock = deadline, clock

    def bounded(self, deadline: float, clock: Clock = time.monotonic) -> PairVault:
        """The same store with every pair read and write held to the absolute deadline."""
        return PairVault(self.parameters, deadline, clock)

    def within_deadline[T](self, call: Callable[[], T], what: str) -> T:
        if self.deadline is None:
            return call()
        return secret_store.within(min(SECRET_SECONDS, check_deadline(self.deadline, self.clock)), call, what)

    def find(self, name: str) -> str | None:
        return self.within_deadline(lambda: secret_store.SecretStore.find(self, name), f"reading {name}")

    def put(self, name: str, value: str) -> None:
        self.within_deadline(lambda: secret_store.SecretStore.put(self, name, value), f"writing {name}")

    @staticmethod
    def check(name: str, write: bool = False) -> None:
        """Refuse any name outside the ten pair names for a write, and outside those and the root secret for a read, before a request."""
        allowed = writable_names() if write else writable_names() | {secret_store.POLARIS_ROOT_SECRET}
        if name not in allowed:
            raise secret_store.SecretStoreError(f"{name} is not an AWS Polaris pair name this setup may {'write' if write else 'read'}")

    def describe(self, name: str) -> dict[str, Any]:
        """The metadata of one pair parameter by exact name, without decryption: presence, type, version and data type, never a value."""
        if name not in writable_names():
            raise secret_store.SecretStoreError(f"{name} is not an AWS Polaris pair name")
        try:
            reply = self.parameters.get_parameter(Name=name, WithDecryption=False)
        except (ClientError, BotoCoreError) as error:
            if self.code(error) == "ParameterNotFound":
                return {"name": name, "present": False}
            raise self.failure("describing", name, error) from None
        parameter = reply.get("Parameter") or {}
        return {
            "name": name,
            "present": True,
            "type": parameter.get("Type"),
            "version": parameter.get("Version"),
            "data_type": parameter.get("DataType"),
        }


class TunnelRequest:
    """A bounded request to the AWS Polaris behind the owned loopback relay, in the call shape of ``catalog.request``.

    It sends nothing to the Mac Polaris port. Each request runs once through ``execute`` within the run's absolute deadline; a reply that does not
    arrive fails visibly and is not repeated. A refusal returns the HTTP status only, so no reply body reaches an error.
    """

    def __init__(self, port: int, *, deadline: float, execute: Execute = direct, clock: Clock = time.monotonic, realm: str = "POLARIS") -> None:
        if port == MAC_POLARIS_PORT or not 1 <= port <= 65535:
            raise SetupError("network", f"the catalog route must use a loopback port other than the Mac Polaris port {MAC_POLARIS_PORT}")
        self.port, self.deadline, self.execute, self.clock, self.realm = port, deadline, execute, clock, realm

    def __repr__(self) -> str:
        return f"TunnelRequest(127.0.0.1:{self.port})"

    def bounded(self, deadline: float) -> TunnelRequest:
        """The same route with the earlier of the two absolute deadlines."""
        return TunnelRequest(self.port, deadline=min(self.deadline, deadline), execute=self.execute, clock=self.clock, realm=self.realm)

    def __call__(
        self, method: str, path: str, token: str | None = None, body: dict[str, Any] | None = None, form: dict[str, str] | None = None
    ) -> tuple[int, Any]:
        headers = {"Accept": "application/json", "Polaris-Realm": self.realm}
        data = None
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body).encode()
        if form is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            data = urllib.parse.urlencode(form).encode()
        result: tuple[int, Any] = self.execute(lambda: self.send(method, path, data, headers))
        return result

    def send(self, method: str, path: str, data: bytes | None, headers: dict[str, str]) -> tuple[int, Any]:
        """One request within the absolute deadline: a watchdog shuts the socket at expiry, so neither a silent peer nor one that keeps sending a
        few bytes holds the header or body read past it (the cutover's ``bronze_inventory.PolarisHttp.send``)."""
        timeout = min(REQUEST_SECONDS, check_deadline(self.deadline, self.clock))
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=timeout)
        expired = threading.Event()

        def expire() -> None:
            expired.set()
            if connection.sock is not None:
                with contextlib.suppress(OSError):
                    connection.sock.shutdown(socket.SHUT_RDWR)

        watchdog = threading.Timer(timeout, expire)
        watchdog.daemon = True
        watchdog.start()
        try:
            try:
                connection.connect()
                if expired.is_set():
                    raise TimeoutError
                connection.request(method, path, body=data, headers=headers)
                response = connection.getresponse()
                raw = response.read(MAX_REPLY_BYTES + 1)
            except (OSError, http.client.HTTPException) as error:
                if expired.is_set() or self.clock() >= self.deadline:
                    raise SetupError("deadline", f"{method} {path} passed its deadline; it is not repeated") from None
                raise SetupError("catalog_http", f"{method} {path} got no complete reply ({type(error).__name__}); it is not repeated") from None
            if expired.is_set():
                raise SetupError("deadline", f"{method} {path} passed its deadline; it is not repeated")
        finally:
            watchdog.cancel()
            connection.close()
        if len(raw) > MAX_REPLY_BYTES:
            raise SetupError("catalog_http", f"{method} {path} replied with more than {MAX_REPLY_BYTES} bytes")
        if response.status >= 400:
            return response.status, None
        try:
            return response.status, json.loads(raw) if raw else None
        except ValueError:
            raise SetupError("catalog_http", f"{method} {path} replied with a body that is not JSON") from None


class Route:
    """The owned loopback relay in front of the Session Manager tunnel; each request runs once with the route checked on both sides."""

    def __init__(self, transport: CatalogTransport, endpoint: str, *, deadline: float) -> None:
        parsed = urllib.parse.urlsplit(endpoint)
        if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or not parsed.port:
            raise SetupError("network", "the catalog relay endpoint is not a loopback port")
        self.transport, self.port, self.deadline = transport, parsed.port, deadline

    def execute[R](self, operation: Callable[[], R], /) -> R:
        try:
            return self.transport.execute(operation, deadline=self.deadline)
        except SetupError:
            raise
        except RuntimeError as error:
            if str(error) == "catalog_transport_unavailable":
                raise SetupError("transport", "the catalog route is unavailable; it is never reopened inside a run") from None
            raise


def check_ports(spec: TransportSpec) -> None:
    """The tunnel and relay listen on local ports other than the Mac Polaris port (P9)."""
    if MAC_POLARIS_PORT in (spec.tunnel_port, spec.relay_port):
        raise SetupError("network", f"the tunnel and relay ports must differ from the Mac Polaris port {MAC_POLARIS_PORT}")


def network_acceptance(spec: TransportSpec, aws: AwsCli, *, deadline: float, clock: Clock = time.monotonic) -> None:
    """Accept the route only to one running host with no public address on the instance or any interface; the catalog stays private."""
    check_ports(spec)
    left = check_deadline(deadline, clock)
    try:
        described = aws.call_bounded("ec2", "describe-instances", ["--instance-ids", spec.instance], timeout=left)
    except StorageError as error:
        raise SetupError("network", f"describing the catalog host failed ({error.code})") from None
    instances = [item for reservation in described.get("Reservations") or [] for item in reservation.get("Instances") or []]
    if (
        len(instances) != 1
        or (instances[0].get("State") or {}).get("Name") != "running"
        or instances[0].get("PublicIpAddress")
        or any((interface.get("Association") or {}).get("PublicIp") for interface in instances[0].get("NetworkInterfaces") or [])
    ):
        raise SetupError("network", "the catalog host is not one running instance without a public address")


def root_token(request: Request, secret: str) -> str:
    """Exchange the root client credentials for a token held in memory; a refusal names the HTTP status only."""
    form = {"grant_type": "client_credentials", "client_id": launcher.ROOT_CLIENT_ID, "client_secret": secret, "scope": "PRINCIPAL_ROLE:ALL"}
    status, body = request("POST", OAUTH_PATH, form=form)
    if status != 200 or not isinstance(body, dict) or not isinstance(body.get("access_token"), str):
        raise SetupError("authentication", f"Polaris refused the root credentials (HTTP {status})")
    return str(body["access_token"])


def catalog_body(bucket: str, region: str) -> dict[str, Any]:
    """The AWS catalog: the same body the Mac setup creates (``catalog.ensure_catalog``) on the project bucket's ``lakehouse/`` prefix."""
    location = f"s3://{bucket}/lakehouse/"
    return {
        "name": CATALOG,
        "type": "INTERNAL",
        "readOnly": False,
        "properties": {"default-base-location": location},
        "storageConfigInfo": {"storageType": "S3", "allowedLocations": [location], "region": region, "stsUnavailable": True},
    }


def matches(body: Any, bucket: str) -> bool:
    """True when a read catalog has the expected base location, the one allowed location and the static credential mode."""
    location = f"s3://{bucket}/lakehouse/"
    storage = body.get("storageConfigInfo") if isinstance(body, dict) else None
    properties = body.get("properties") if isinstance(body, dict) else None
    return (
        isinstance(storage, dict)
        and isinstance(properties, dict)
        and properties.get("default-base-location") == location
        and storage.get("allowedLocations") == [location]
        and storage.get("stsUnavailable") is True
    )


def verify_catalog(request: Request, access: str, bucket: str) -> str:
    """Read the AWS catalog and change nothing: ``present``, ``absent``, or a refusal when its location or credential mode differs."""
    status, body = request("GET", CATALOG_PATH, access)
    if status == 404:
        return "absent"
    if status != 200:
        raise SetupError("catalog", f"reading catalog {CATALOG} failed (HTTP {status})")
    if not matches(body, bucket):
        raise SetupError("catalog_location", f"catalog {CATALOG} exists with a different location or credential mode; it is never changed here")
    return "present"


def ensure_catalog(request: Request, access: str, bucket: str, region: str) -> str:
    """Create the AWS catalog once or verify it (P10); grant table management to its ``catalog_admin`` role as the Mac setup does.

    The create is sent at most once per run. A conflict (another run created it first) is verified by reading it back. A lost reply raises.
    """
    found = verify_catalog(request, access, bucket)
    outcome = "already present"
    if found == "absent":
        status, _ = request("POST", "/api/management/v1/catalogs", access, body={"catalog": catalog_body(bucket, region)})
        if status == 409:
            if verify_catalog(request, access, bucket) != "present":
                raise SetupError("catalog", f"catalog {CATALOG} reported a conflict but cannot be read back")
        elif status not in (200, 201):
            raise SetupError("catalog", f"creating catalog {CATALOG} failed (HTTP {status})")
        else:
            outcome = "created"
    grant = {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}
    status, _ = request("PUT", f"{CATALOG_PATH}/catalog-roles/catalog_admin/grants", access, body=grant)
    if status not in (200, 201):
        raise SetupError("catalog", f"granting table management on {CATALOG} failed (HTTP {status})")
    return outcome


@dataclass(frozen=True)
class Remote:
    """The AWS catalog behind the route: the loopback port and the call that runs each request once."""

    port: int
    execute: Execute


class Services(Protocol):
    """What the setup needs from the world; the live services use the project profile, the E2E uses stand-ins."""

    @property
    def bucket(self) -> str: ...

    @property
    def region(self) -> str: ...

    @property
    def approvals(self) -> Path: ...

    @property
    def locks(self) -> Path: ...

    @property
    def registry(self) -> run_lock.Registry: ...

    def vault(self) -> PairVault: ...

    def remote(self, instance: str, *, deadline: float) -> contextlib.AbstractContextManager[Remote]: ...


def read_root(vault: PairVault, seconds: float) -> str:
    """The Polaris root secret, read into memory within its own bound; it is never printed or written."""
    try:
        return secret_store.within(seconds, lambda: vault.get(secret_store.POLARIS_ROOT_SECRET), "reading the Polaris root secret")
    except secret_store.SecretStoreError as error:
        raise SetupError("authentication", str(error)) from None


def step_catalog(services: Services, instance: str, seconds: float, clock: Clock = time.monotonic) -> dict[str, Any]:
    """Create or verify the AWS catalog through the route."""
    deadline = clock() + seconds
    vault = services.vault()
    secret = read_root(vault, min(SECRET_SECONDS, check_deadline(deadline, clock)))
    with services.remote(instance, deadline=deadline) as remote:
        request = TunnelRequest(remote.port, deadline=deadline, execute=remote.execute, clock=clock)
        access = root_token(request, secret)
        outcome = ensure_catalog(request, access, services.bucket, services.region)
    return {"catalog": CATALOG, "outcome": outcome}


def step_check(services: Services, instance: str | None, seconds: float, clock: Clock = time.monotonic) -> dict[str, Any]:
    """Report the ten pair parameters by exact name, metadata only, and with an instance the AWS catalog's state; nothing is changed."""
    deadline = clock() + seconds
    vault = services.vault()
    parameters = []
    for identity, pair in configured_pairs().items():
        for name in pair:
            left = min(SECRET_SECONDS, check_deadline(deadline, clock))
            try:
                found = secret_store.within(left, functools.partial(vault.describe, name), f"describing {name}")
            except secret_store.SecretStoreError as error:
                raise SetupError("parameters", str(error)) from None
            parameters.append({"identity": identity, **found})
    result: dict[str, Any] = {"parameters": parameters}
    if instance is not None:
        secret = read_root(vault, min(SECRET_SECONDS, check_deadline(deadline, clock)))
        with services.remote(instance, deadline=deadline) as remote:
            request = TunnelRequest(remote.port, deadline=deadline, execute=remote.execute, clock=clock)
            access = root_token(request, secret)
            result["catalog"] = verify_catalog(request, access, services.bucket)
            if result["catalog"] == "present":
                result["principals"] = check_principals(request, access, services.approvals)
    return result


def namespace_set(identity: str) -> tuple[str, ...]:
    """The namespaces the identity's grants name, in plan order; each must exist before its grant."""
    if identity == "reader":
        return ()
    if identity == "e2e":
        return (*bronze_writer.QUALIFICATION, *E2E_TABLE_NAMESPACES)
    if identity == "bronze":
        return bronze_writer.PRODUCTION
    return (identity,)


def grant_plan(identity: str, approved: bool) -> list[dict[str, Any]]:
    """The exact grants of the identity's catalog role; ``approved`` is the recorded namespace approval, which makes bronze and e2e ``active``."""
    if identity == "reader":
        return [{"grant": {"type": "catalog", "privilege": privilege}} for privilege in catalog.READER_PRIVILEGES]
    if identity in ("bronze", "e2e"):
        phase = "active" if approved else "prepared"
        grants = bronze_writer.grant_plan("real" if identity == "bronze" else "aws_qualify", phase, namespace_creation_approved=approved)
        if identity == "e2e":
            grants += [
                {"grant": {"type": "namespace", "namespace": [name], "privilege": privilege}}
                for name in E2E_TABLE_NAMESPACES
                for privilege in polaris_writer.NAMESPACE_PRIVILEGES
            ]
        return grants
    return polaris_writer.grants(polaris_writer.Writer(PRINCIPALS[identity], (identity,), configured_pairs()[identity]))


def ensure_status(status: int, action: str, allowed: tuple[int, ...] = (200, 201, 204)) -> None:
    """Raise with the action and the HTTP status only."""
    if status not in allowed:
        raise SetupError("principal", f"{action} failed (HTTP {status})")


@dataclass(frozen=True)
class Approval:
    """One owner approval to create an identity's absent namespaces, stored write-once before anything is created (P8)."""

    approval_id: str
    identity: str
    namespaces: tuple[str, ...]

    def document(self) -> dict[str, Any]:
        return {"approval_id": self.approval_id, "identity": self.identity, "namespaces": list(self.namespaces)}


def recorded_approval(folder: Path, identity: str) -> dict[str, Any] | None:
    path = folder / f"{identity}.json"
    if not path.exists():
        return None
    try:
        found = json.loads(path.read_text())
    except (OSError, ValueError):
        raise SetupError("approval", f"the recorded namespace approval of {identity} cannot be read") from None
    if not isinstance(found, dict):
        raise SetupError("approval", f"the recorded namespace approval of {identity} is not an approval")
    return found


def record_approval(folder: Path, approval: Approval) -> None:
    """Store the approval once; the same approval again is accepted, another one for the same identity is refused."""
    found = recorded_approval(folder, approval.identity)
    wanted = approval.document()
    if found is not None:
        if {key: found.get(key) for key in wanted} != wanted:
            raise SetupError("approval", f"{approval.identity} already has another recorded namespace approval; it is never replaced")
        return
    durable_directory(folder)
    temporary = folder / f".{approval.identity}.{os.getpid()}.tmp"
    temporary.write_text(json.dumps({**wanted, "recorded_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")}, sort_keys=True) + "\n")
    try:
        os.link(temporary, folder / f"{approval.identity}.json")
    except FileExistsError:
        temporary.unlink()
        record_approval(folder, approval)
        return
    temporary.unlink()
    descriptor = os.open(folder, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def approval_for(folder: Path, identity: str, approval_id: str | None) -> Approval | None:
    """The approval in force for the identity: the flag's, checked against any recorded one, or the recorded one alone (a rerun)."""
    if approval_id is not None and identity == "reader":
        raise SetupError("approval", "the reader names no namespace; a namespace approval does not apply")
    if approval_id is not None and not APPROVAL_ID.fullmatch(approval_id):
        raise SetupError("approval", "an approval ID has letters, digits, dashes and underscores only, at most 64")
    found = recorded_approval(folder, identity)
    if approval_id is None:
        if found is None:
            return None
        approval_id = str(found.get("approval_id"))
    approval = Approval(approval_id, identity, namespace_set(identity))
    if found is not None and {key: found.get(key) for key in approval.document()} != approval.document():
        raise SetupError("approval", f"{identity} already has another recorded namespace approval; it is never replaced")
    return approval


def listed_grants(request: Request, access: str, role: str) -> list[dict[str, Any]]:
    status, body = request("GET", f"{MANAGEMENT}/catalogs/{CATALOG}/catalog-roles/{role}/grants", access)
    ensure_status(status, f"listing the grants of catalog role {role}")
    grants = body.get("grants") if isinstance(body, dict) else None
    if not isinstance(grants, list):
        raise SetupError("grants", f"the grant listing of catalog role {role} is not a list")
    return [{"grant": item} for item in grants]


def compare_grants(request: Request, access: str, role: str, expected: list[dict[str, Any]]) -> None:
    """The listed grants of the catalog role equal the plan exactly (P7)."""
    try:
        bronze_writer.verify_grants(listed_grants(request, access, role), expected)
    except bronze_writer.BindingError:
        raise SetupError("grants", f"the grants of catalog role {role} differ from the approved plan; the role stays unassigned") from None


@contextlib.contextmanager
def no_live_writer_try(registry: run_lock.Registry, *, deadline: float, clock: Clock = time.monotonic) -> Iterator[None]:
    """Hold the launcher registry lock, under which the launcher admits every try, and stop unless the registry proves no cloud try is live (P5).

    The registry records each cloud try and its placement state but not the catalog pair it holds, so any try without a confirmed end
    (``ecs_target.TERMINAL_PLACEMENTS``) stops the setup of every identity, and a registry that cannot be read proves nothing and stops it too. A
    registry that does not exist yet has admitted nothing. The lock stays held until the credentials are stored, so no try is admitted in between.
    """
    registry.path.mkdir(parents=True, exist_ok=True)
    with (registry.path / "registry.lock").open("a") as handle:
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if clock() >= deadline:
                    raise SetupError("writer_try", "the launcher registry stayed locked until the deadline, so no live writer try can be ruled out") from None
                time.sleep(0.05)
        try:
            if registry.identity_file.exists() or registry.state_file.exists():
                try:
                    identity = run_lock.read_identity(registry)
                    state = run_lock.parse_state(registry.state_file.read_bytes(), identity["registry_id"], registry.state_file.name)
                    usage = ecs_target.active_usage((state.get("attempts") or {}).values())
                except (OSError, run_lock.RegistryError, ecs_target.PlacementError, KeyError, TypeError, AttributeError):
                    raise SetupError("writer_try", "the launcher registry cannot be read, so no live writer try can be ruled out") from None
                if usage.tasks:
                    raise SetupError("writer_try", f"the launcher registry holds {usage.tasks} cloud try without a confirmed end; rerun after it ends")
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def role_names(request: Request, access: str, path: str, what: str) -> set[str] | None:
    """The names in one role listing, or None when its principal or principal role does not exist yet."""
    status, body = request("GET", path, access)
    if status == 404:
        return None
    ensure_status(status, f"listing {what}")
    roles = body.get("roles") if isinstance(body, dict) else None
    if not isinstance(roles, list) or not all(isinstance(item, dict) and isinstance(item.get("name"), str) for item in roles):
        raise SetupError("roles", f"the listing of {what} is not a role list")
    return {str(item["name"]) for item in roles}


def verify_memberships(request: Request, access: str, identity: str) -> None:
    """The identity's principal holds no principal role but its own, and that principal role no catalog role on the catalog but its own (P7).

    Polaris gives a principal every privilege of every role it holds, so an extra role would keep privileges outside the plan. Extras are
    refused, never removed.
    """
    role = PRINCIPALS[identity]
    listings = (
        (f"{MANAGEMENT}/principals/{role}/principal-roles", f"the principal roles of principal {role}"),
        (f"{MANAGEMENT}/principal-roles/{role}/catalog-roles/{CATALOG}", f"the catalog roles of principal role {role}"),
    )
    for path, what in listings:
        found = role_names(request, access, path, what)
        if found is not None and found - {role}:
            raise SetupError("roles", f"{what} include a role outside the approved plan; it is never removed here")


def ensure_principal(
    request: TunnelRequest,
    access: str,
    identity: str,
    approval: Approval | None,
    vault: PairVault,
    *,
    bucket: str,
    lock_path: Path,
    approvals: Path,
    registry: run_lock.Registry,
) -> dict[str, Any]:
    """Set up one identity in order: namespaces, roles, grants, grant comparison, role assignment, credentials, principal assignment."""
    role = PRINCIPALS[identity]
    if verify_catalog(request, access, bucket) == "absent":
        raise SetupError("catalog", f"catalog {CATALOG} does not exist; run the catalog step first")
    verify_memberships(request, access, identity)
    absent = []
    for name in namespace_set(identity):
        status, _ = request("GET", f"/api/catalog/v1/{CATALOG}/namespaces/{name}", access)
        if status == 404:
            absent.append(name)
        else:
            ensure_status(status, f"reading namespace {name}")
    if absent and approval is None:
        raise SetupError("approval", f"namespaces {', '.join(absent)} are absent; creating them needs --namespace-approval")
    if approval is not None:
        record_approval(approvals, approval)
    for name in absent:
        status, _ = request("POST", f"/api/catalog/v1/{CATALOG}/namespaces", access, body={"namespace": [name], "properties": {}})
        ensure_status(status, f"creating namespace {name}", (200, 201, 409))
    status, _ = request("POST", f"{MANAGEMENT}/principal-roles", access, body={"principalRole": {"name": role}})
    ensure_status(status, "creating the principal role", (200, 201, 409))
    status, _ = request("POST", f"{MANAGEMENT}/catalogs/{CATALOG}/catalog-roles", access, body={"catalogRole": {"name": role}})
    ensure_status(status, "creating the catalog role", (200, 201, 409))
    expected = grant_plan(identity, approval is not None)
    for grant in expected:
        status, _ = request("PUT", f"{MANAGEMENT}/catalogs/{CATALOG}/catalog-roles/{role}/grants", access, body=grant)
        ensure_status(status, f"granting {grant['grant']['privilege']} to {role}")
    compare_grants(request, access, role, expected)
    status, _ = request("PUT", f"{MANAGEMENT}/principal-roles/{role}/catalog-roles/{CATALOG}", access, body={"catalogRole": {"name": role}})
    ensure_status(status, "assigning the catalog role")

    def check(status: int, action: str, allowed: tuple[int, ...] = (200, 201, 204)) -> None:
        ensure_status(status, action, allowed)

    hold = min(request.deadline, request.clock() + REGISTRY_HOLD_SECONDS)
    try:
        with no_live_writer_try(registry, deadline=hold, clock=request.clock):
            outcome = principal_credentials.ensure_credentials(
                principal=role,
                names=configured_pairs()[identity],
                vault=vault.bounded(hold, request.clock),
                request=request.bounded(hold),
                access=access,
                check=check,
                lock_path=lock_path,
            )
    except principal_credentials.CredentialError as error:
        raise SetupError("credentials", str(error)) from None
    status, _ = request("PUT", f"{MANAGEMENT}/principals/{role}/principal-roles", access, body={"principalRole": {"name": role}})
    ensure_status(status, "assigning the principal role")
    return {
        "identity": identity,
        "principal": role,
        "credentials": outcome,
        "namespaces_created": absent,
        "phase": "active" if approval is not None and identity in ("bronze", "e2e") else "prepared" if identity in ("bronze", "e2e") else "fixed",
        "grants": len(expected),
    }


def step_principal(services: Services, identity: str, instance: str, approval_id: str | None, seconds: float, clock: Clock = time.monotonic) -> dict[str, Any]:
    """Set up one AWS identity through the route; the approval is checked against the record before any request.

    The principal's lock is taken first, within the step's deadline, so no approval, namespace, role, assignment or pair changes before it
    (P11). The credential step's own lock is then always free.
    """
    if identity not in PRINCIPALS:
        raise SetupError("principal", f"unknown identity {identity}")
    deadline = clock() + seconds
    approval = approval_for(services.approvals, identity, approval_id)
    role = PRINCIPALS[identity]
    held = contextlib.ExitStack()
    try:
        wait = min(principal_credentials.LOCK_WAIT_SECONDS, check_deadline(deadline, clock))
        held.enter_context(principal_credentials.serialized(services.locks / f"aws_{role}.lock", role, wait))
    except principal_credentials.CredentialError as error:
        raise SetupError("credentials", str(error)) from None
    with held:
        vault = services.vault().bounded(deadline, clock)
        secret = read_root(vault, min(SECRET_SECONDS, check_deadline(deadline, clock)))
        with services.remote(instance, deadline=deadline) as remote:
            request = TunnelRequest(remote.port, deadline=deadline, execute=remote.execute, clock=clock)
            access = root_token(request, secret)
            lock_path = services.locks / f"aws_{role}.credentials.lock"
            return ensure_principal(
                request,
                access,
                identity,
                approval,
                vault,
                bucket=services.bucket,
                lock_path=lock_path,
                approvals=services.approvals,
                registry=services.registry,
            )


def check_principals(request: Request, access: str, approvals: Path) -> dict[str, str]:
    """Each identity's role memberships and grants against its plan, read only: ``matches``, ``differs`` or ``absent`` (no catalog role yet)."""
    found: dict[str, str] = {}
    for identity, role in PRINCIPALS.items():
        try:
            verify_memberships(request, access, identity)
        except SetupError as error:
            if error.code != "roles":
                raise
            found[identity] = "differs"
            continue
        status, _ = request("GET", f"{MANAGEMENT}/catalogs/{CATALOG}/catalog-roles/{role}", access)
        if status == 404:
            found[identity] = "absent"
            continue
        ensure_status(status, f"reading catalog role {role}")
        approval = approval_for(approvals, identity, None)
        try:
            compare_grants(request, access, role, grant_plan(identity, approval is not None))
            found[identity] = "matches"
        except SetupError as error:
            if error.code != "grants":
                raise
            found[identity] = "differs"
    return found


class LiveServices:
    """The project profile, checked against the caller identity on first use, and the Session Manager route to the AWS host."""

    def __init__(self, tunnel_port: int, relay_port: int) -> None:
        self.state_root = run_lock.main_checkout(REPO_ROOT) / "data/lakehouse"
        self.ports = (tunnel_port, relay_port)

    @functools.cached_property
    def settings(self) -> dict[str, str]:
        """The project profile, region and bucket; any profile that does not resolve to the project user stops the tool (P2)."""
        from scripts.infrastructure.render_project_config import ConfigurationError, load_configuration, verify_project_identity

        try:
            settings, _ = load_configuration(REPO_ROOT / ".env")
            verify_project_identity(settings)
        except ConfigurationError as error:
            raise SetupError("profile", str(error)) from None
        return settings

    @property
    def bucket(self) -> str:
        return self.settings["data_bucket_name"]

    @property
    def region(self) -> str:
        return self.settings["aws_region"]

    @property
    def approvals(self) -> Path:
        return self.state_root / "catalog_setup" / "namespace_approvals"

    @property
    def locks(self) -> Path:
        return self.state_root / "principal_locks"

    @property
    def registry(self) -> run_lock.Registry:
        """The real launcher registry of the main checkout, whatever checkout the tool runs from."""
        return run_lock.resolve(None, REPO_ROOT)

    def vault(self) -> PairVault:
        import boto3

        session = boto3.Session(profile_name=self.settings["aws_profile"], region_name=self.region)
        return PairVault(session.client("ssm", config=secret_store.client_config()))

    @contextlib.contextmanager
    def remote(self, instance: str, *, deadline: float) -> Iterator[Remote]:
        spec = TransportSpec(instance, self.settings["aws_profile"], self.region, *self.ports)
        network_acceptance(spec, AwsCli(self.settings), deadline=deadline)
        folder = self.state_root / "catalog_setup_transport"
        durable_directory(folder)
        transport = CatalogTransport(folder)
        try:
            endpoint = transport.start(spec, deadline=deadline, ready_deadline=min(deadline, time.monotonic() + READY_SECONDS))
            route = Route(transport, endpoint, deadline=deadline)
            yield Remote(route.port, route.execute)
        finally:
            closed = transport.close(deadline=time.monotonic() + CLOSE_SECONDS)
            if not closed:
                sys.stderr.write("aws catalog setup: the catalog route did not close with every process confirmed gone; check the transport ledger\n")
            if not closed and sys.exc_info()[0] is None:
                raise SetupError("transport", "the catalog route did not close with every process confirmed gone")


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    cli.add_argument("--tunnel-port", type=int, default=18181)
    cli.add_argument("--relay-port", type=int, default=18182)
    cli.add_argument("--seconds", type=float, default=600, help="the deadline of the whole step")
    steps = cli.add_subparsers(dest="step", required=True)
    steps.add_parser("catalog").add_argument("--instance", required=True)
    steps.add_parser("check").add_argument("--instance")
    principal = steps.add_parser("principal")
    principal.add_argument("--identity", required=True, choices=IDENTITIES)
    principal.add_argument("--instance", required=True)
    principal.add_argument("--namespace-approval", help="the owner's approval ID for creating this identity's absent namespaces")
    return cli


def run(args: argparse.Namespace, services: Services) -> dict[str, Any]:
    if args.seconds <= 0:
        raise SetupError("deadline", "--seconds must be positive")
    if args.step == "catalog":
        return step_catalog(services, args.instance, args.seconds)
    if args.step == "principal":
        return step_principal(services, args.identity, args.instance, args.namespace_approval, args.seconds)
    return step_check(services, args.instance, args.seconds)


def main(argv: list[str] | None = None, services: Services | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        result = run(args, services or LiveServices(args.tunnel_port, args.relay_port))
    except (SetupError, secret_store.SecretStoreError, bronze_writer.BindingError, OSError, ValueError) as error:
        code = f" ({error.code})" if isinstance(error, SetupError) else ""
        sys.stderr.write(f"aws catalog setup {args.step}{code}: {error}\n")
        return 1
    sys.stdout.write(json.dumps(result, sort_keys=True, default=str) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
