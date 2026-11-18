"""The AWS catalog setup, run on the host as a one-off task: create or verify the AWS catalog and set up the AWS Polaris pair parameters.

The task runs from the repository checkout on the host (``/opt/hai/repo``) under its own task role, launched by the owner with
``host_session catalog-setup <step>``. It reaches Polaris on the host's loopback port 8181 (host networking), so no tunnel and no project
profile are involved. The steps and their arguments:

    catalog
    check
    inspect
    principal --identity reader|e2e|bronze|silver|gold|features|serving [--namespace-approval <APPROVAL_ID>]

Every catalog request runs once within the step's absolute deadline. A lost reply fails visibly and is never repeated inside a run; a rerun converges,
because an existing catalog with the expected location and credential mode is ``already present`` and one that differs is refused, never changed.

The task refuses to start unless the caller identity is its own task role (``CATALOG_SETUP_ROLE_ARN``). The Polaris root credential is injected by
the task's execution role as ``POLARIS_ROOT_SECRET``, held in memory only and never printed, logged or written; errors carry the action and the HTTP
status only. The result is one JSON line on standard output, which ``host_session`` reads from the task log; it holds names and outcomes, never a value.

``check`` reports each of the fourteen AWS Polaris pair parameters, the features pair included, by exact name with its type and version, never a value.
It reads without decryption and drops the encrypted value unread. ``inspect`` is the authenticated read: it reads the AWS catalog and each identity's
roles and grants and changes nothing.

``principal`` sets up one AWS identity: its principal role, catalog role, grants and credentials. The grants are the approved plan of the
identity (``bronze_writer.grant_plan`` for ``bronze`` and ``e2e``, which also gets the table grants on ``silver_e2e``, ``gold_e2e`` and
``features_e2e``; ``polaris_writer.grants`` on ``silver``, ``gold``, ``features`` or ``serving`` only; the Mac reader privileges for ``reader``).
Every namespace a grant names must exist. An absent one is created only under ``--namespace-approval``, which the task records once, as a write-once S3
record, for that identity and its exact namespace set before it creates anything; for ``bronze`` and ``e2e`` the approval also grants
``NAMESPACE_CREATE`` (phase ``active``). After the grants the task lists the catalog role's grants and compares them with the plan; any extra, missing
or duplicate grant stops it before the catalog role is assigned (P7). The issued pair goes from the Polaris reply straight to Parameter Store
(``principal_credentials``), so a stored pair that Polaris accepts is kept, and a missing, half-written or refused pair is reset (P3 to P5). Updates of
one principal run one at a time under a leased lock in S3 (P11, ``catalog_setup_state``), taken within the step's deadline before any change. A pair
reset runs only under the host launch hold (P5): after taking it the task waits the longest time a launch admitted before it can still need to reach
RunTask, then proves that every job task of the project is confirmed ended, and a task ECS cannot describe counts as unknown and stops the reset. The
credential deadline starts after that wait. Every S3, ECS, hold and identity call is built with limits no longer than the time left of the step. A pair
write that may still commit keeps the hold; ``services_apply resolve`` with the token in the failure message clears it.
"""

from __future__ import annotations

import argparse
import contextlib
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
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from scripts import resource_peaks
from scripts.infrastructure import secret_store
from scripts.lakehouse import bronze_writer, polaris_writer, principal_credentials
from scripts.lakehouse.catalog_setup_state import (
    RESOLVE,
    CatalogState,
    Deadline,
    LaunchGuard,
    LaunchSpec,
    PendingMark,
    StateError,
    client_options,
    error_code,
    run_bounded,
)

CATALOG = "hai_lakehouse"
POLARIS_HOST = "127.0.0.1"
POLARIS_PORT = 8181
ROOT_CLIENT_ID = "root"
REPO_ROOT = Path(__file__).resolve().parents[2]
IDENTITIES = ("reader", "e2e", "bronze", "silver", "gold", "features", "serving")
WRITER_VARIABLES = "job_writer_variables.tf"
OAUTH_PATH = "/api/catalog/v1/oauth/tokens"
CATALOG_PATH = f"/api/management/v1/catalogs/{CATALOG}"
REQUEST_SECONDS = 30.0
SECRET_SECONDS = 30.0
# The longest the credential step holds the host launch hold; every host launch is refused for that long at most.
HOLD_SECONDS = 20.0
# The longest a pair write past its deadline is awaited, locks held: botocore's own bound for one call (secret_store.CLIENT_OPTIONS: 2 attempts
# of 5 s connect and 10 s read) plus its retry backoff.
WRITE_SETTLE_SECONDS = 45.0
# Added to the lease of a principal beyond the step deadline and the settle time.
LEASE_MARGIN_SECONDS = 15.0
MAX_REPLY_BYTES = 1 << 20
MANAGEMENT = "/api/management/v1"
APPROVAL_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
E2E_TABLE_NAMESPACES = ("silver_e2e", "gold_e2e", "features_e2e")
PRINCIPALS = {
    "reader": "lakehouse_reader",
    "e2e": "test_writer",
    "bronze": "bronze_writer",
    "silver": "silver_writer",
    "gold": "gold_writer",
    "features": "features_writer",
    "serving": "serving_writer",
}

Clock = Callable[[], float]
Request = Callable[..., tuple[int, Any]]


class SetupError(RuntimeError):
    """A setup step failed; ``code`` names the step and the message never holds a credential or a parameter value."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def check_deadline(deadline: float, clock: Clock = time.monotonic) -> float:
    """The seconds left before the absolute deadline; none left stops the step."""
    left = deadline - clock()
    if left <= 0:
        raise SetupError("deadline", "the catalog setup passed its deadline")
    return left


def pair_names(variables: Path) -> dict[str, tuple[str, str]]:
    """The client ID and secret parameter names of each AWS identity, from the variable defaults in ``infra/iam/variables.tf`` (P12).

    The fourteen names must be distinct, each must end in its own ``aws_polaris_<identity>_client_id`` or ``_client_secret`` leaf and none may be a
    managed Mac name (``polaris_silver_*``, ``polaris_gold_*`` and the others), so a Mac and an AWS writer can never share a pair.
    """
    names = secret_store.variable_names(variables)
    # The features pair names are the variables of the layer writer file beside variables.tf.
    writer = secret_store.terraform_variables(variables.with_name(WRITER_VARIABLES))
    names |= {name.upper(): value for name, value in writer.items() if name.startswith("aws_polaris_features_client_")}
    pairs: dict[str, tuple[str, str]] = {}
    for identity in IDENTITIES:
        found = tuple(names.get(f"AWS_POLARIS_{identity.upper()}_CLIENT_{part.upper()}_NAME", "") for part in ("id", "secret"))
        for name, part in zip(found, ("id", "secret"), strict=True):
            if not name.endswith(f"/aws_polaris_{identity}_client_{part}"):
                raise SetupError("parameters", f"the AWS {identity} client {part} name must be its own aws_polaris_{identity}_client_{part} parameter")
        pairs[identity] = (found[0], found[1])
    flat = [name for pair in pairs.values() for name in pair]
    if len(set(flat)) != len(flat) or set(flat) & {*secret_store.MANAGED, *secret_store.READ_ONLY}:
        raise SetupError("parameters", "the AWS Polaris pair names must be fourteen distinct names outside the managed Mac names")
    return pairs


@functools.cache
def configured_pairs() -> dict[str, tuple[str, str]]:
    """The checked pair names of the five AWS identities, read once from the committed variable defaults."""
    return pair_names(REPO_ROOT / secret_store.VARIABLES)


def writable_names() -> frozenset[str]:
    return frozenset(name for pair in configured_pairs().values() for name in pair)


class UnconfirmedWrite(secret_store.SecretStoreError):
    """A pair write that may still commit after the step gave up on it; the launch hold stays until the owner clears it."""


class PairVault(secret_store.SecretStore):
    """Parameter Store through the project profile: writes only the fourteen AWS Polaris pair names and reads those and the Polaris root secret."""

    def __init__(self, parameters: Any, deadline: float | None = None, clock: Clock = time.monotonic, pending: PendingMark | None = None) -> None:
        super().__init__(parameters)
        self.deadline, self.clock, self.pending = deadline, clock, pending

    def bounded(self, deadline: float, clock: Clock = time.monotonic, pending: PendingMark | None = None) -> PairVault:
        """The same store with every pair read and write held to the absolute deadline; ``pending`` is the principal's pending-write mark."""
        return PairVault(self.parameters, deadline, clock, pending or self.pending)

    def within_deadline[T](self, call: Callable[[], T], what: str) -> T:
        if self.deadline is None:
            return call()
        return secret_store.within(min(SECRET_SECONDS, check_deadline(self.deadline, self.clock)), call, what)

    def find(self, name: str) -> str | None:
        return self.within_deadline(lambda: secret_store.SecretStore.find(self, name), f"reading {name}")

    def put(self, name: str, value: str) -> None:
        if self.deadline is None:
            secret_store.SecretStore.put(self, name, value)
            return
        self.confirmed(functools.partial(secret_store.SecretStore.put, self, name, value), f"writing {name}")

    def confirmed(self, call: Callable[[], None], what: str) -> None:
        """Run one pair write and return only once it is confirmed finished, so no write outlives the locks of its run (P4, P5).

        A write still running at the deadline is awaited for at most ``WRITE_SETTLE_SECONDS`` more, with every lock still held. One that has
        not ended then leaves the pending-write mark before the locks are released; the write removes the mark when it ends, and until then
        every rerun of the principal refuses (``CatalogState.refuse_pending_write``). Without a mark to leave the write is refused before it starts.
        """
        if self.pending is None or self.deadline is None:
            raise secret_store.SecretStoreError(f"{what} has no pending-write mark, so it is not started")
        left = min(SECRET_SECONDS, check_deadline(self.deadline, self.clock))
        failure: list[BaseException] = []
        done, guard = threading.Event(), threading.Lock()
        abandoned: list[PendingMark] = []

        def work() -> None:
            try:
                call()
            except BaseException as error:
                failure.append(error)
            finally:
                with guard:
                    done.set()
                    for mark in abandoned:
                        mark.clear()

        threading.Thread(target=work, name="pair-write", daemon=True).start()
        if not done.wait(left):
            done.wait(WRITE_SETTLE_SECONDS)
        with guard:
            if not done.is_set():
                try:
                    self.pending.set()
                except StateError as error:
                    raise UnconfirmedWrite(f"{what} did not finish within its deadline and may still commit; {error}") from None
                abandoned.append(self.pending)
                raise UnconfirmedWrite(f"{what} did not finish within its deadline and may still commit; reruns refuse until it ends")
        if failure:
            raise failure[0]

    @staticmethod
    def check(name: str, write: bool = False) -> None:
        """Refuse any name outside the fourteen pair names for a write, and outside those and the root secret for a read, before a request."""
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


class CatalogRequest:
    """A bounded request to the AWS Polaris on the host's loopback, in the call shape of ``catalog.request``.

    Each request is sent once within the run's absolute deadline; a reply that does not arrive fails visibly and is not repeated. A refusal returns
    the HTTP status only, so no reply body reaches an error.
    """

    def __init__(self, port: int, *, deadline: float, clock: Clock = time.monotonic, realm: str = "POLARIS") -> None:
        if not 1 <= port <= 65535:
            raise SetupError("network", "the catalog port is not a port number")
        self.port, self.deadline, self.clock, self.realm = port, deadline, clock, realm

    def __repr__(self) -> str:
        return f"CatalogRequest({POLARIS_HOST}:{self.port})"

    def bounded(self, deadline: float) -> CatalogRequest:
        """The same request path with the earlier of the two absolute deadlines."""
        return CatalogRequest(self.port, deadline=min(self.deadline, deadline), clock=self.clock, realm=self.realm)

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
        return self.send(method, path, data, headers)

    def send(self, method: str, path: str, data: bytes | None, headers: dict[str, str]) -> tuple[int, Any]:
        """One request within the absolute deadline: a watchdog shuts the socket at expiry, so neither a silent peer nor one that keeps sending a
        few bytes holds the header or body read past it. The connected socket is kept here
        because a closing response (``Connection: close`` or HTTP/1.0) detaches it from the connection while its body is still being read."""
        timeout = min(REQUEST_SECONDS, check_deadline(self.deadline, self.clock))
        connection = http.client.HTTPConnection(POLARIS_HOST, self.port, timeout=timeout)
        expired = threading.Event()
        held: list[socket.socket] = []
        response: http.client.HTTPResponse | None = None

        def expire() -> None:
            expired.set()
            for sock in held:
                with contextlib.suppress(OSError):
                    sock.shutdown(socket.SHUT_RDWR)

        watchdog = threading.Timer(timeout, expire)
        watchdog.daemon = True
        watchdog.start()
        try:
            try:
                connection.connect()
                if connection.sock is not None:
                    held.append(connection.sock)
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
            if response is not None:
                response.close()
            connection.close()
            for sock in held:
                sock.close()
        if len(raw) > MAX_REPLY_BYTES:
            raise SetupError("catalog_http", f"{method} {path} replied with more than {MAX_REPLY_BYTES} bytes")
        if response.status >= 400:
            return response.status, None
        try:
            return response.status, json.loads(raw) if raw else None
        except ValueError:
            raise SetupError("catalog_http", f"{method} {path} replied with a body that is not JSON") from None


def root_token(request: Request, secret: str) -> str:
    """Exchange the root client credentials for a token held in memory; a refusal names the HTTP status only."""
    form = {"grant_type": "client_credentials", "client_id": ROOT_CLIENT_ID, "client_secret": secret, "scope": "PRINCIPAL_ROLE:ALL"}
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


class Services(Protocol):
    """What the setup needs from the world; the live services use the task role, the E2E uses stand-ins."""

    @property
    def bucket(self) -> str: ...

    @property
    def region(self) -> str: ...

    @property
    def polaris_port(self) -> int: ...

    @property
    def state(self) -> CatalogState: ...

    @property
    def guard(self) -> LaunchGuard: ...

    def verify_identity(self, deadline: Deadline) -> None: ...

    def root_secret(self) -> str: ...

    def vault(self) -> PairVault: ...


def step_catalog(services: Services, seconds: float, clock: Clock = time.monotonic) -> dict[str, Any]:
    """Create or verify the AWS catalog."""
    deadline = clock() + seconds
    secret = services.root_secret()
    request = CatalogRequest(services.polaris_port, deadline=deadline, clock=clock)
    access = root_token(request, secret)
    outcome = ensure_catalog(request, access, services.bucket, services.region)
    return {"catalog": CATALOG, "outcome": outcome}


def step_check(services: Services, seconds: float, clock: Clock = time.monotonic) -> dict[str, Any]:
    """Report every pair parameter by exact name, metadata only: nothing is decrypted and nothing is changed."""
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
    return {"parameters": parameters}


def step_inspect(services: Services, seconds: float, clock: Clock = time.monotonic) -> dict[str, Any]:
    """Read the AWS catalog and each identity's roles, grants and stored pair, authenticated with the root secret; nothing is changed."""
    deadline = clock() + seconds
    vault = services.vault().bounded(deadline, clock)
    secret = services.root_secret()
    request = CatalogRequest(services.polaris_port, deadline=deadline, clock=clock)
    access = root_token(request, secret)
    result: dict[str, Any] = {"catalog": verify_catalog(request, access, services.bucket)}
    if result["catalog"] == "present":
        result["principals"] = check_principals(request, access, services.state.bounded(Deadline(deadline, clock)))
        result["causes"] = stored_pair_causes(request, vault, result["principals"])
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
        return [{"grant": {"type": "catalog", "privilege": privilege}} for privilege in polaris_writer.READER_PRIVILEGES]
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


def approval_for(state: CatalogState, identity: str, approval_id: str | None) -> Approval | None:
    """The approval in force for the identity: the flag's, checked against any recorded one, or the recorded one alone (a rerun)."""
    if approval_id is not None and identity == "reader":
        raise SetupError("approval", "the reader names no namespace; a namespace approval does not apply")
    if approval_id is not None and not APPROVAL_ID.fullmatch(approval_id):
        raise SetupError("approval", "an approval ID has letters, digits, dashes and underscores only, at most 64")
    found = state.approval(identity)
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


def verify_memberships(request: Request, access: str, identity: str, exact: bool = False) -> None:
    """The identity's principal holds no principal role but its own, and that principal role no catalog role on the catalog but its own (P7).

    Polaris gives a principal every privilege of every role it holds, so an extra role would keep privileges outside the plan. Extras are
    refused, never removed. Creation allows a missing role, which it assigns later; with ``exact`` (the inspection) both assignments must exist.
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
        if exact and found != {role}:
            raise SetupError("roles", f"{what} do not include the approved role")


def verify_stored_pair(request: CatalogRequest, vault: PairVault, identity: str) -> None:
    """Authenticate with the pair read back from Parameter Store; a refused or missing pair is ``stored_pair_mismatch``.

    No conditional write exists, so a timed-out pair write may still apply later and replace the pair. This check catches it at the end of every
    principal run and in ``inspect``; a rerun of the principal step then resets the pair and verifies it again.
    """
    role = PRINCIPALS[identity]
    try:
        stored = [vault.find(name) for name in configured_pairs()[identity]]
    except secret_store.SecretStoreError as error:
        raise SetupError("parameters", str(error)) from None
    try:
        accepted = bool(stored[0] and stored[1]) and principal_credentials.pair_accepted(request, role, str(stored[0]), str(stored[1]))
    except principal_credentials.CredentialError as error:
        raise SetupError("credentials", str(error)) from None
    if not accepted:
        raise SetupError("stored_pair_mismatch", f"the catalog refuses the stored pair of principal {role}; rerun the principal step to reset it")


def stored_pair_causes(request: CatalogRequest, vault: PairVault, principals: dict[str, str]) -> dict[str, str]:
    """``stored_pair_mismatch`` for each identity with a catalog role whose stored pair the catalog refuses; that identity becomes ``differs``."""
    causes: dict[str, str] = {}
    for identity, state in principals.items():
        if state == "absent":
            continue
        try:
            verify_stored_pair(request, vault, identity)
        except SetupError as error:
            if error.code != "stored_pair_mismatch":
                raise
            principals[identity], causes[identity] = "differs", error.code
    return causes


def ensure_principal(
    request: CatalogRequest,
    access: str,
    identity: str,
    approval: Approval | None,
    vault: PairVault,
    *,
    bucket: str,
    state: CatalogState,
    guard: LaunchGuard,
    deadline: Deadline,
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
        state.record_approval(approval.identity, approval.document())
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

    try:
        with guard.no_live_writer_try(deadline) as hold:
            until = min(request.deadline, request.clock() + HOLD_SECONDS)
            try:
                outcome = principal_credentials.ensure_credentials(
                    principal=role,
                    names=configured_pairs()[identity],
                    vault=vault.bounded(until, request.clock, pending=state.pending(role)),
                    request=request.bounded(until),
                    access=access,
                    check=check,
                    lock=contextlib.nullcontext(),
                )
            except UnconfirmedWrite as error:
                hold.keep()
                raise UnconfirmedWrite(
                    f"{error}; the host launch hold stays so no job starts on an unknown pair: clear it with {RESOLVE} {hold.token}"
                ) from None
    except principal_credentials.CredentialError as error:
        raise SetupError("credentials", str(error)) from None
    status, _ = request("PUT", f"{MANAGEMENT}/principals/{role}/principal-roles", access, body={"principalRole": {"name": role}})
    ensure_status(status, "assigning the principal role")
    verify_stored_pair(request, vault, identity)
    return {
        "identity": identity,
        "principal": role,
        "credentials": outcome,
        "namespaces_created": absent,
        "phase": "active" if approval is not None and identity in ("bronze", "e2e") else "prepared" if identity in ("bronze", "e2e") else "fixed",
        "grants": len(expected),
    }


def step_principal(services: Services, identity: str, approval_id: str | None, seconds: float, clock: Clock = time.monotonic) -> dict[str, Any]:
    """Set up one AWS identity; the approval is checked against the record before any request.

    The principal's lease is taken first, within the step's deadline, so no approval, namespace, role, assignment or pair changes before it (P11). The
    credential step takes no lock of its own: the lease covers it, and expires only after the step deadline and the longest a late pair write may commit.
    """
    if identity not in PRINCIPALS:
        raise SetupError("principal", f"unknown identity {identity}")
    deadline = clock() + seconds
    bound = Deadline(deadline, clock)
    state = services.state.bounded(bound)
    services.guard.require_room(bound)
    approval = approval_for(state, identity, approval_id)
    role = PRINCIPALS[identity]
    wait = min(principal_credentials.LOCK_WAIT_SECONDS, check_deadline(deadline, clock))
    hold = check_deadline(deadline, clock) + WRITE_SETTLE_SECONDS + LEASE_MARGIN_SECONDS
    with state.lease(role, wait=wait, hold=hold):
        state.refuse_pending_write(role)
        vault = services.vault().bounded(deadline, clock)
        secret = services.root_secret()
        request = CatalogRequest(services.polaris_port, deadline=deadline, clock=clock)
        access = root_token(request, secret)
        return ensure_principal(request, access, identity, approval, vault, bucket=services.bucket, state=state, guard=services.guard, deadline=bound)


def check_principals(request: Request, access: str, state: CatalogState) -> dict[str, str]:
    """Each identity's role memberships and grants against its plan, read only: ``matches``, ``differs`` or ``absent`` (no catalog role yet).

    ``matches`` needs both exact assignments (the principal role to the principal, the catalog role to the principal role) and the exact grants.
    """
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
        try:
            verify_memberships(request, access, identity, exact=True)
        except SetupError as error:
            if error.code != "roles":
                raise
            found[identity] = "differs"
            continue
        approval = approval_for(state, identity, None)
        try:
            compare_grants(request, access, role, grant_plan(identity, approval is not None))
            found[identity] = "matches"
        except SetupError as error:
            if error.code != "grants":
                raise
            found[identity] = "differs"
    return found


SETTINGS = ("DATA_BUCKET_NAME", "AWS_REGION", "CATALOG_SETUP_ROLE_ARN", "CATALOG_SETUP_LOCK_PARAMETER", "CATALOG_SETUP_CLUSTER", "CATALOG_SETUP_FAMILY_PREFIX")
ROLE_ARN = re.compile(r"arn:aws:iam::([0-9]{12}):role/([A-Za-z0-9_+=,.@-]{1,64})")


class HostServices:
    """The task role, checked against the caller identity before the first step, and Polaris on the host's loopback port."""

    def __init__(self, environ: Mapping[str, str]) -> None:
        missing = [name for name in SETTINGS if not environ.get(name)]
        if missing:
            raise SetupError("settings", f"the task has no value for {', '.join(missing)}")
        self.settings = {name: str(environ[name]) for name in SETTINGS}
        found = ROLE_ARN.fullmatch(self.settings["CATALOG_SETUP_ROLE_ARN"])
        if found is None:
            raise SetupError("settings", "CATALOG_SETUP_ROLE_ARN is not a role ARN")
        self.expected_identity = f"arn:aws:sts::{found.group(1)}:assumed-role/{found.group(2)}/"
        self.creating = threading.Lock()
        # The root secret is read once from the environment the execution role filled; it stays in memory only.
        self.root = environ.get("POLARIS_ROOT_SECRET", "")

    @functools.cached_property
    def session(self) -> Any:
        import boto3

        return boto3.Session(region_name=self.region)

    def client(self, name: str) -> Any:
        return self.session.client(name, config=secret_store.client_config())

    def bounded_client(self, name: str, left: float) -> Any:
        """A client whose connect and read limits are no longer than the seconds left of the step. Clients are made one at a time: a session is not safe
        to share between threads, and a call that gave up on its deadline can still be making its client."""
        with self.creating:
            return self.session.client(name, config=Config(**client_options(left)))

    @property
    def bucket(self) -> str:
        return self.settings["DATA_BUCKET_NAME"]

    @property
    def region(self) -> str:
        return self.settings["AWS_REGION"]

    @property
    def polaris_port(self) -> int:
        return POLARIS_PORT

    @functools.cached_property
    def state(self) -> CatalogState:
        return CatalogState(self.bounded_client, self.bucket)

    @functools.cached_property
    def guard(self) -> LaunchGuard:
        spec = LaunchSpec(self.settings["CATALOG_SETUP_LOCK_PARAMETER"], self.settings["CATALOG_SETUP_CLUSTER"], self.settings["CATALOG_SETUP_FAMILY_PREFIX"])
        return LaunchGuard(self.bounded_client, spec)

    def verify_identity(self, deadline: Deadline) -> None:
        """Any caller that is not the task's own role stops the task before a Parameter Store, S3 or catalog call (P2)."""
        try:
            left = deadline.left("the caller identity")
            reply = run_bounded(lambda: self.bounded_client("sts", left).get_caller_identity(), deadline, "the caller identity")
            arn = str(reply["Arn"])
        except StateError as error:
            raise SetupError("deadline", str(error)) from None
        except (ClientError, BotoCoreError, KeyError) as error:
            raise SetupError(
                "identity", f"the caller identity could not be read ({error_code(error) if isinstance(error, ClientError | BotoCoreError) else 'KeyError'})"
            ) from None
        if not arn.startswith(self.expected_identity):
            raise SetupError("identity", "the caller is not the catalog setup task role")

    def root_secret(self) -> str:
        if not self.root:
            raise SetupError("authentication", "the task has no Polaris root secret; its execution role injects POLARIS_ROOT_SECRET")
        return self.root

    def vault(self) -> PairVault:
        return PairVault(self.client("ssm"))


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    cli.add_argument("--seconds", type=float, default=600, help="the deadline of the whole step")
    steps = cli.add_subparsers(dest="step", required=True)
    steps.add_parser("catalog")
    steps.add_parser("check")
    steps.add_parser("inspect")
    principal = steps.add_parser("principal")
    principal.add_argument("--identity", required=True, choices=IDENTITIES)
    principal.add_argument("--namespace-approval", help="the owner's approval ID for creating this identity's absent namespaces")
    return cli


def run(args: argparse.Namespace, services: Services) -> dict[str, Any]:
    if args.seconds <= 0:
        raise SetupError("deadline", "--seconds must be positive")
    deadline = Deadline.after(args.seconds)
    services.verify_identity(deadline)
    left = deadline.left("the step")
    if args.step == "catalog":
        return step_catalog(services, left)
    if args.step == "principal":
        return step_principal(services, args.identity, args.namespace_approval, left)
    if args.step == "inspect":
        return step_inspect(services, left)
    return step_check(services, left)


def main(argv: list[str] | None = None, services: Services | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        result = run(args, services or HostServices(os.environ))
    except (SetupError, StateError, secret_store.SecretStoreError, bronze_writer.BindingError, OSError, ValueError) as error:
        code = f" ({error.code})" if isinstance(error, SetupError | StateError) else ""
        sys.stderr.write(f"aws catalog setup {args.step}{code}: {error}\n")
        return 1
    sys.stdout.write(json.dumps(result, sort_keys=True, default=str) + "\n")
    return 0


if __name__ == "__main__":
    resource_peaks.recorded(main)
