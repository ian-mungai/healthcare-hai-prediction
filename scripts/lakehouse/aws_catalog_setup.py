"""The owner's AWS catalog setup: create or verify the AWS catalog and report the ten AWS Polaris pair parameters, under the project profile only.

Run from the repository root at a terminal:

    .venv/bin/python -m scripts.lakehouse.aws_catalog_setup catalog --instance <HOST_INSTANCE_ID>
    .venv/bin/python -m scripts.lakehouse.aws_catalog_setup check [--instance <HOST_INSTANCE_ID>]

The AWS catalog is reached only through ``orchestration.catalog_transport.CatalogTransport``: the Session Manager tunnel to the host's private
port 8181 behind an owned loopback relay, after the network acceptance check. Neither local port may be the Mac Polaris port 8181, so a request
can never reach the Mac catalog. Every catalog request runs once through ``Route.execute`` within the run's absolute deadline. A lost reply
fails visibly and is never repeated inside a run; a rerun converges, because an existing catalog with the expected location and credential
mode is ``already present`` and one that differs is refused, never changed.

The tool refuses to start unless the configured profile resolves to the project user. The Polaris root credential is read from Parameter
Store into memory only and never printed, logged or written; errors carry the action and the HTTP status only. ``check`` reports each of the
ten AWS Polaris pair parameters by exact name with its type and version, never a value: it reads without decryption and drops the encrypted
value unread. With ``--instance`` it also reads the AWS catalog and changes nothing.
"""

from __future__ import annotations

import argparse
import contextlib
import functools
import http.client
import json
import sys
import time
import urllib.parse
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from botocore.exceptions import BotoCoreError, ClientError

from orchestration import launcher
from orchestration.catalog_transport import CatalogTransport, TransportSpec
from scripts.acquisition.s3_store import AwsCli, StorageError
from scripts.infrastructure import secret_store
from scripts.lakehouse import run_lock
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
MAX_REPLY_BYTES = 1 << 20

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
        timeout = min(REQUEST_SECONDS, check_deadline(self.deadline, self.clock))
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=timeout)
        try:
            connection.request(method, path, body=data, headers=headers)
            response = connection.getresponse()
            raw = response.read(MAX_REPLY_BYTES + 1)
        except (OSError, http.client.HTTPException) as error:
            raise SetupError("catalog_http", f"{method} {path} got no complete reply ({type(error).__name__}); it is not repeated") from None
        finally:
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
            result["catalog"] = verify_catalog(request, root_token(request, secret), services.bucket)
    return result


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
    return cli


def run(args: argparse.Namespace, services: Services) -> dict[str, Any]:
    if args.seconds <= 0:
        raise SetupError("deadline", "--seconds must be positive")
    if args.step == "catalog":
        return step_catalog(services, args.instance, args.seconds)
    return step_check(services, args.instance, args.seconds)


def main(argv: list[str] | None = None, services: Services | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        result = run(args, services or LiveServices(args.tunnel_port, args.relay_port))
    except (SetupError, secret_store.SecretStoreError, OSError, ValueError) as error:
        code = f" ({error.code})" if isinstance(error, SetupError) else ""
        sys.stderr.write(f"aws catalog setup {args.step}{code}: {error}\n")
        return 1
    sys.stdout.write(json.dumps(result, sort_keys=True, default=str) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
