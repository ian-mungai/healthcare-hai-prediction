"""Check the AWS catalog setup with stand-ins only: a loopback Polaris, the loopback Parameter Store stand-in and the real owned relay.

Run from the repository root: ``.venv/bin/python -m scripts.lakehouse.run_aws_catalog_setup_e2e --out <NEW_DIRECTORY> [--mutations]``. No AWS
service is contacted and no container starts. Each case states one failure mode of the setup and its required behavior. ``--mutations`` plants
each named defect in the setup module in turn and requires its intended case to fail. The report holds names and outcomes only, never a value.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import io
import json
import re
import secrets
import shutil
import socket
import sys
import tempfile
import threading
import time
import urllib.parse
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import ecs_target
from orchestration.catalog_transport import CatalogTransport, TransportSpec
from scripts.acquisition.s3_store import AwsCli
from scripts.infrastructure import secret_store
from scripts.infrastructure.secret_store_stand_in import CIPHERTEXT_MARKER, StandIn, StandInHandler
from scripts.lakehouse import aws_catalog_setup as setup
from scripts.lakehouse import catalog, polaris_writer, principal_credentials, run_lock

REPO_ROOT = Path(__file__).resolve().parents[2]
BUCKET = "example-lakehouse-bucket"
REGION = "us-west-2"
RUN_SECONDS = 120.0
CASE_SECONDS = 60.0
STALL_SECONDS = 3.0
INSTANCE = "i-0123456789abcdef0"
# The suite's work folder; main sets it and removes it at the end.
STATE_ROOT = Path(tempfile.gettempdir())
# The Mac Polaris port, written out so a changed constant in the setup module cannot hide a defect.
MAC_PORT = 8181


class Polaris:
    """A loopback stand-in for the Polaris management and token endpoints, with the faults the cases plant."""

    def __init__(self, root_secret: str) -> None:
        self.root_secret = root_secret
        self.tokens: dict[str, str] = {}
        self.catalog: dict[str, Any] | None = None
        self.namespaces: set[str] = set()
        self.principal_roles: dict[str, set[str]] = {}
        self.grants: dict[str, list[dict[str, Any]]] = {}
        self.principals: dict[str, dict[str, str]] = {}
        self.assigned: dict[str, set[str]] = {}
        self.issued: list[str] = []
        self.calls: list[tuple[str, str]] = []
        self.faults: set[str] = set()
        self.lock = threading.Lock()
        stand_in = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                return

            def reply(self, status: int, body: Any = None) -> None:
                data = json.dumps(body).encode() if body is not None else b""
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def handle_any(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                with stand_in.lock:
                    stand_in.calls.append((self.command, self.path))
                status, body, drop = stand_in.answer(self.command, self.path, self.headers, raw)
                if drop:
                    self.close_connection = True
                    return
                self.reply(status, body)

            do_GET = do_POST = do_PUT = handle_any

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        return int(self.server.server_address[1])

    def count(self, method: str, path: str) -> int:
        with self.lock:
            return sum(1 for call in self.calls if call == (method, path))

    def answer(self, method: str, path: str, headers: Any, raw: bytes) -> tuple[int, Any, bool]:
        if headers.get("Polaris-Realm") != "POLARIS":
            return 400, None, False
        if method == "POST" and path == setup.OAUTH_PATH:
            form = {key: urllib.parse.unquote_plus(value) for key, value in (item.split("=", 1) for item in raw.decode().split("&"))} if raw else {}
            who = None
            if form.get("client_id") == "root" and form.get("client_secret") == self.root_secret and "root_refused" not in self.faults:
                who = "root"
            for name, pair in self.principals.items():
                if (form.get("client_id"), form.get("client_secret")) == (pair["clientId"], pair["clientSecret"]):
                    who = name
            if who is None:
                return 401, None, False
            token = secrets.token_hex(16)
            self.tokens[token] = who
            return 200, {"access_token": token, "token_type": "bearer"}, False
        if self.tokens.get(headers.get("Authorization", "").removeprefix("Bearer ")) != "root":
            return 401, None, False
        if "stall" in self.faults:
            time.sleep(STALL_SECONDS)
        body = json.loads(raw) if raw else None
        with self.lock:
            return self.manage(method, path, body)

    def manage(self, method: str, path: str, body: Any) -> tuple[int, Any, bool]:
        management, catalog = "/api/management/v1", f"/api/catalog/v1/{setup.CATALOG}"
        roles = f"{setup.CATALOG_PATH}/catalog-roles"
        if path == setup.CATALOG_PATH and method == "GET":
            return (200, self.catalog, False) if self.catalog is not None else (404, None, False)
        if path == f"{management}/catalogs" and method == "POST":
            if "conflict" in self.faults:
                self.catalog = body["catalog"]
                self.grants["catalog_admin"] = []
                return 409, None, False
            if self.catalog is not None:
                return 409, None, False
            self.catalog = body["catalog"]
            self.grants["catalog_admin"] = []
            return 201, body["catalog"], "lose_create_reply" in self.faults
        if self.catalog is None:
            return 404, None, False
        if path.startswith(f"{catalog}/namespaces/") and method == "GET":
            return (200, {"namespace": [path.rsplit("/", 1)[1]]}, False) if path.rsplit("/", 1)[1] in self.namespaces else (404, None, False)
        if path == f"{catalog}/namespaces" and method == "POST":
            name = body["namespace"][0]
            if name in self.namespaces:
                return 409, None, False
            self.namespaces.add(name)
            return 200, body, False
        if path == f"{management}/principal-roles" and method == "POST":
            name = body["principalRole"]["name"]
            if name in self.principal_roles:
                return 409, None, False
            self.principal_roles[name] = set()
            return 201, None, False
        if path == roles and method == "POST":
            name = body["catalogRole"]["name"]
            if name in self.grants:
                return 409, None, False
            self.grants[name] = []
            return 201, None, False
        if path.startswith(f"{roles}/") and path.endswith("/grants"):
            role = path.removeprefix(f"{roles}/").removesuffix("/grants")
            if role not in self.grants:
                return 404, None, False
            if method == "GET":
                return 200, {"grants": list(self.grants[role])}, False
            grant = body["grant"]
            if grant["type"] == "namespace" and grant["namespace"][0] not in self.namespaces:
                return 404, None, False
            if grant not in self.grants[role]:
                self.grants[role].append(grant)
            return 201, None, False
        if path.startswith(f"{roles}/") and method == "GET":
            return (200, {"name": path.rsplit("/", 1)[1]}, False) if path.rsplit("/", 1)[1] in self.grants else (404, None, False)
        if path.startswith(f"{management}/principal-roles/") and path.endswith(f"/catalog-roles/{setup.CATALOG}") and method == "GET":
            name = path.removeprefix(f"{management}/principal-roles/").split("/")[0]
            if name not in self.principal_roles:
                return 404, None, False
            return 200, {"roles": [{"name": role} for role in sorted(self.principal_roles[name])]}, False
        if path.startswith(f"{management}/principal-roles/") and path.endswith(f"/catalog-roles/{setup.CATALOG}") and method == "PUT":
            name = path.removeprefix(f"{management}/principal-roles/").split("/")[0]
            if name not in self.principal_roles or body["catalogRole"]["name"] not in self.grants:
                return 404, None, False
            self.principal_roles[name].add(body["catalogRole"]["name"])
            return 201, None, False
        if path == f"{management}/principals" and method == "POST":
            name = body["principal"]["name"]
            if name in self.principals:
                return 409, None, False
            return 201, {"principal": {"name": name}, "credentials": self.issue(name)}, False
        if path.startswith(f"{management}/principals/"):
            rest = path.removeprefix(f"{management}/principals/").split("/")
            name = rest[0]
            if name not in self.principals:
                return 404, None, False
            if len(rest) == 1 and method == "GET":
                return 200, {"name": name}, False
            if rest[1:] == ["principal-roles"] and method == "GET":
                return 200, {"roles": [{"name": role} for role in sorted(self.assigned.get(name, set()))]}, False
            if rest[1:] == ["reset"] and method == "POST":
                return 200, {"credentials": self.issue(name)}, False
            if rest[1:] == ["principal-roles"] and method == "PUT":
                if body["principalRole"]["name"] not in self.principal_roles:
                    return 404, None, False
                self.assigned.setdefault(name, set()).add(body["principalRole"]["name"])
                return 201, None, False
        return 404, None, False

    def issue(self, name: str) -> dict[str, str]:
        """A new pair for the principal; the old one stops working at once, as Polaris resets it."""
        pair = {"clientId": self.principals.get(name, {}).get("clientId") or "id-" + secrets.token_hex(6), "clientSecret": "sec-" + secrets.token_hex(12)}
        self.principals[name] = pair
        self.issued.append(pair["clientSecret"])
        return dict(pair)

    def __enter__(self) -> Polaris:
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)


class Services:
    """Stand-in services: the stand-in Parameter Store as the project profile's vault and the stand-in Polaris as the AWS catalog."""

    bucket = BUCKET
    region = REGION

    def __init__(self, ssm: StandIn, polaris: Polaris, execute: setup.Execute = setup.direct, relay: Path | None = None, state: Path | None = None) -> None:
        self.ssm, self.polaris, self.execute, self.relay = ssm, polaris, execute, relay
        self.state = state or STATE_ROOT / secrets.token_hex(6)
        self.approvals = self.state / "namespace_approvals"
        self.locks = self.state / "principal_locks"
        self.registry = run_lock.Registry(self.state / "registry", "test")
        self.decryption: list[bool] = []
        self.closed: list[bool] = []

    def vault(self) -> setup.PairVault:
        parameters = secret_store.open_store(REPO_ROOT, self.ssm.endpoint).parameters
        real = parameters.get_parameter
        seen = self.decryption

        def spy(**kwargs: Any) -> Any:
            seen.append(bool(kwargs.get("WithDecryption")))
            return real(**kwargs)

        parameters.get_parameter = spy
        return setup.PairVault(parameters)

    @contextlib.contextmanager
    def remote(self, instance: str, *, deadline: float) -> Iterator[setup.Remote]:
        if self.relay is None:
            yield setup.Remote(self.polaris.port, self.execute)
            return
        transport = CatalogTransport(self.relay)
        try:
            endpoint = transport.start_relay(self.polaris.port, deadline=deadline)
            route = setup.Route(transport, endpoint, deadline=deadline)
            yield setup.Remote(route.port, route.execute)
        finally:
            self.closed.append(transport.close(deadline=time.monotonic() + 30))


@contextlib.contextmanager
def world(seeded: dict[str, str] | None = None, faults: set[str] | None = None) -> Iterator[tuple[StandIn, Polaris, str]]:
    root = "root-" + secrets.token_hex(12)
    with StandIn({secret_store.POLARIS_ROOT_SECRET: root, **(seeded or {})}) as ssm, Polaris(root) as polaris:
        polaris.faults |= faults or set()
        yield ssm, polaris, root


def run_main(argv: list[str], services: Any) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = setup.main(argv, services)
    return code, out.getvalue(), err.getvalue()


SECRETS_SEEN: list[str] = []
OUTPUTS: list[str] = []


def call(argv: list[str], services: Any, root: str) -> tuple[int, str, str]:
    SECRETS_SEEN.append(root)
    code, out, err = run_main(["--seconds", str(RUN_SECONDS), *argv], services)
    OUTPUTS.append(out + err)
    return code, out, err


def outcome(out: str) -> Any:
    return json.loads(out) if out.strip() else None


Case = Callable[[Path], dict[str, bool]]


def case_catalog_created(_: Path) -> dict[str, bool]:
    """P10: an absent AWS catalog is created once with the Mac body on the project bucket, and table management is granted to catalog_admin."""
    with world() as (ssm, polaris, root):
        code, out, _err = call(["catalog", "--instance", "i-0123456789abcdef0"], Services(ssm, polaris), root)
        return {
            "the step succeeds": code == 0,
            "the outcome is created": (outcome(out) or {}).get("outcome") == "created",
            "one create was sent": polaris.count("POST", "/api/management/v1/catalogs") == 1,
            "the body is the Mac catalog body on the project bucket": polaris.catalog == setup.catalog_body(BUCKET, REGION),
            "table management was granted": polaris.count("PUT", f"{setup.CATALOG_PATH}/catalog-roles/catalog_admin/grants") == 1,
            "no parameter was written": ssm.writes() == 0,
        }


def case_catalog_rerun(_: Path) -> dict[str, bool]:
    """Retry: a rerun over an existing matching catalog sends no create and reports already present."""
    with world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        first = call(["catalog", "--instance", "i-0123456789abcdef0"], services, root)
        second = call(["catalog", "--instance", "i-0123456789abcdef0"], services, root)
        return {
            "both runs succeed": first[0] == 0 and second[0] == 0,
            "the rerun reports already present": (outcome(second[1]) or {}).get("outcome") == "already present",
            "only one create was ever sent": polaris.count("POST", "/api/management/v1/catalogs") == 1,
        }


def case_catalog_differs(_: Path) -> dict[str, bool]:
    """P10: an existing catalog with another location is refused and never changed."""
    with world() as (ssm, polaris, root):
        polaris.catalog = setup.catalog_body("another-bucket", REGION)
        before = json.dumps(polaris.catalog, sort_keys=True)
        code, _out, err = call(["catalog", "--instance", "i-0123456789abcdef0"], Services(ssm, polaris), root)
        return {
            "the step fails": code == 1,
            "the error names the location check": "(catalog_location)" in err,
            "no create was sent": polaris.count("POST", "/api/management/v1/catalogs") == 0,
            "no grant was sent": polaris.count("PUT", f"{setup.CATALOG_PATH}/catalog-roles/catalog_admin/grants") == 0,
            "the catalog is unchanged": json.dumps(polaris.catalog, sort_keys=True) == before,
        }


def case_lost_create_reply(_: Path) -> dict[str, bool]:
    """Lost reply: the create lands but its reply is lost; the run fails without a second create and the rerun adopts the catalog."""
    with world(faults={"lose_create_reply"}) as (ssm, polaris, root):
        services = Services(ssm, polaris)
        first = call(["catalog", "--instance", "i-0123456789abcdef0"], services, root)
        creates_after_failure = polaris.count("POST", "/api/management/v1/catalogs")
        polaris.faults.clear()
        second = call(["catalog", "--instance", "i-0123456789abcdef0"], services, root)
        return {
            "the first run fails visibly": first[0] == 1 and "(catalog_http)" in first[2],
            "the create was sent once": creates_after_failure == 1,
            "the rerun reports already present": second[0] == 0 and (outcome(second[1]) or {}).get("outcome") == "already present",
            "no second create was ever sent": polaris.count("POST", "/api/management/v1/catalogs") == 1,
        }


def case_create_conflict(_: Path) -> dict[str, bool]:
    """Two runs: a create that meets a conflict reads the catalog back and verifies it instead of failing or retrying the create."""
    with world(faults={"conflict"}) as (ssm, polaris, root):
        code, out, _err = call(["catalog", "--instance", "i-0123456789abcdef0"], Services(ssm, polaris), root)
        return {
            "the step succeeds": code == 0,
            "the outcome is already present": (outcome(out) or {}).get("outcome") == "already present",
            "one create was sent": polaris.count("POST", "/api/management/v1/catalogs") == 1,
            "the catalog was read back": polaris.count("GET", setup.CATALOG_PATH) == 2,
        }


def case_mac_port(_: Path) -> dict[str, bool]:
    """P9: no request or route may use the Mac Polaris port, so the tool can never reach the Mac catalog."""
    checks: dict[str, bool] = {}
    try:
        setup.TunnelRequest(MAC_PORT, deadline=time.monotonic() + 5)
        checks["a request on the Mac port is refused"] = False
    except setup.SetupError as error:
        checks["a request on the Mac port is refused"] = error.code == "network"
    for label, ports in (("tunnel", (MAC_PORT, 18182)), ("relay", (18181, MAC_PORT))):
        try:
            setup.check_ports(TransportSpec("i-0123456789abcdef0", "project", REGION, *ports))
            checks[f"the {label} on the Mac port is refused"] = False
        except setup.SetupError as error:
            checks[f"the {label} on the Mac port is refused"] = error.code == "network"
    return checks


def case_host_ingress(_: Path) -> dict[str, bool]:
    """The route goes only to one running host whose security groups admit only same-account security groups; its public address is allowed."""
    from scripts.lakehouse.run_bronze_cutover_e2e import describe_runner, ingress_variants

    checks: dict[str, bool] = {}
    settings = {"aws_profile": "synthetic", "aws_region": REGION, "data_bucket_name": "synthetic-bucket", "expected_account_id": "synthetic_account"}
    spec = TransportSpec("i-0123456789abcdef0", "project", REGION, 18181, 18182)
    for label, instance, groups, accepted in ingress_variants([]):
        aws = AwsCli(settings, runner=describe_runner(instance, groups))
        try:
            setup.network_acceptance(spec, aws, deadline=time.monotonic() + 10)
            code = ""
        except setup.SetupError as error:
            code = error.code
        checks[f"host ingress: {label}"] = code == ("" if accepted else "network")
    return checks


def case_root_refused(_: Path) -> dict[str, bool]:
    """P1: Polaris refuses the root credentials; the error carries the HTTP status only and nothing is created."""
    with world(faults={"root_refused"}) as (ssm, polaris, root):
        code, out, err = call(["catalog", "--instance", "i-0123456789abcdef0"], Services(ssm, polaris), root)
        return {
            "the step fails": code == 1,
            "the error names authentication and the status": "(authentication)" in err and "HTTP 401" in err,
            "the root secret is not in the output": root not in out + err,
            "no create was sent": polaris.count("POST", "/api/management/v1/catalogs") == 0,
        }


def case_stalled_catalog(_: Path) -> dict[str, bool]:
    """Every wait has a deadline: a catalog that stops answering fails the run near its own deadline, not at the stand-in's pace."""
    with world(faults={"stall"}) as (ssm, polaris, root):
        began = time.monotonic()
        code, _out, err = run_main(["--seconds", "1", "catalog", "--instance", "i-0123456789abcdef0"], Services(ssm, polaris))
        SECRETS_SEEN.append(root)
        OUTPUTS.append(err)
        took = time.monotonic() - began
        return {
            "the step fails": code == 1,
            "it names the lost reply or the deadline": "(catalog_http)" in err or "(deadline)" in err,
            "it stops within two seconds": took < 2.0,
            "no create was sent": polaris.count("POST", "/api/management/v1/catalogs") == 0,
        }


def case_route_lost(_: Path) -> dict[str, bool]:
    """A lost route fails the run visibly and is never reopened inside it; no request reaches the catalog."""

    transport = mock.Mock(spec=CatalogTransport)
    transport.execute.side_effect = RuntimeError("catalog_transport_unavailable")
    with world() as (ssm, polaris, root):
        route = setup.Route(transport, f"http://127.0.0.1:{polaris.port}", deadline=time.monotonic() + 30)
        code, _out, err = call(["catalog", "--instance", "i-0123456789abcdef0"], Services(ssm, polaris, route.execute), root)
        return {
            "the step fails": code == 1,
            "it names the route": "(transport)" in err,
            "the route was tried once": transport.execute.call_count == 1,
            "no request reached the catalog": not polaris.calls,
        }


def case_real_relay(out: Path) -> dict[str, bool]:
    """The real owned relay in front of the stand-in catalog: the catalog is created through it and every relay process is confirmed gone."""
    folder = out / "relay_ledger"
    folder.mkdir(mode=0o700)
    with world() as (ssm, polaris, root):
        services = Services(ssm, polaris, relay=folder)
        code, out_text, _ = call(["catalog", "--instance", "i-0123456789abcdef0"], services, root)
        return {
            "the step succeeds through the relay": code == 0 and (outcome(out_text) or {}).get("outcome") == "created",
            "the relay closed with every process confirmed gone": services.closed == [True],
        }


def case_profile_refused(_: Path) -> dict[str, bool]:
    """P2: a profile that does not resolve to the project user stops the tool before any Parameter Store or catalog call."""
    from scripts.infrastructure import render_project_config

    def refuse(_: dict[str, str]) -> None:
        raise render_project_config.ConfigurationError("the caller is not the project user")

    services = setup.LiveServices.__new__(setup.LiveServices)
    with (
        mock.patch.object(render_project_config, "load_configuration", return_value=({"aws_profile": "admin", "aws_region": REGION}, {})),
        mock.patch.object(render_project_config, "verify_project_identity", refuse),
        mock.patch("boto3.Session", side_effect=AssertionError("no AWS client may be made")) as session,
    ):
        code, out, err = run_main(["check"], services)
        OUTPUTS.append(out + err)
        return {"the tool stops": code == 1, "the error names the profile": "(profile)" in err, "no AWS client was made": not session.called}


def case_check_metadata(_: Path) -> dict[str, bool]:
    """P6: the check reports all ten names by exact name with type and version, reads nothing with decryption and prints no value."""
    pairs = setup.configured_pairs()
    seeded = {pairs["reader"][0]: "reader-id-" + secrets.token_hex(6), pairs["reader"][1]: "reader-secret-" + secrets.token_hex(6)}
    seeded[pairs["gold"][1]] = "gold-secret-" + secrets.token_hex(6)
    with world(seeded) as (ssm, polaris, root):
        services = Services(ssm, polaris)
        code, out, err = call(["check"], services, root)
        found = {item["name"]: item for item in (outcome(out) or {}).get("parameters", [])}
        SECRETS_SEEN.extend(seeded.values())
        return {
            "the check succeeds": code == 0,
            "all ten names are reported": set(found) == set(setup.writable_names()),
            "presence matches the store": all(found[name]["present"] == (name in seeded) for name in found),
            "present names report SecureString and a version": all(found[name].get("type") == "SecureString" and found[name].get("version") for name in seeded),
            "no read asked for decryption": bool(services.decryption) and not any(services.decryption),
            "no value or ciphertext is printed": not any(value in out + err for value in seeded.values()) and CIPHERTEXT_MARKER not in out + err,
            "the root secret was not read": (("GetParameter", secret_store.POLARIS_ROOT_SECRET) not in ssm.requests()),
            "nothing was written": ssm.writes() == 0,
            "no catalog request was sent": not polaris.calls,
        }


def case_check_catalog(_: Path) -> dict[str, bool]:
    """The check with an instance reads the catalog and changes nothing."""
    with world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        absent = call(["inspect", "--instance", "i-0123456789abcdef0"], services, root)
        polaris.catalog = setup.catalog_body(BUCKET, REGION)
        present = call(["inspect", "--instance", "i-0123456789abcdef0"], services, root)
        return {
            "an absent catalog is reported": absent[0] == 0 and (outcome(absent[1]) or {}).get("catalog") == "absent",
            "a present catalog is reported": present[0] == 0 and (outcome(present[1]) or {}).get("catalog") == "present",
            "no create or grant was sent": not [call for call in polaris.calls if call[0] in ("POST", "PUT") and call[1] != setup.OAUTH_PATH],
        }


def case_vault_scope(_: Path) -> dict[str, bool]:
    """P2, P12: the vault writes only the ten AWS pair names, refuses the Mac and root names before any request and reads the root secret."""
    pairs = setup.configured_pairs()
    with world() as (ssm, polaris, root):
        vault = Services(ssm, polaris).vault()
        refused = []
        for name in (secret_store.POLARIS_SILVER_CLIENT_ID, secret_store.POLARIS_GOLD_CLIENT_SECRET, secret_store.POLARIS_ROOT_SECRET, "/other/name"):
            try:
                vault.put(name, "value-" + secrets.token_hex(4))
                refused.append(False)
            except secret_store.SecretStoreError:
                refused.append(True)
        before = len(ssm.requests())
        value = "silver-id-" + secrets.token_hex(6)
        SECRETS_SEEN.append(value)
        vault.put(pairs["silver"][0], value)
        try:
            vault.find(secret_store.POSTGRES_PASSWORD)
            other_read = False
        except secret_store.SecretStoreError:
            other_read = True
        return {
            "the Mac, root and other names are refused": all(refused),
            "a refused write sends no request": before == 0,
            "an AWS pair name is written and read back": ssm.values.get(pairs["silver"][0]) == value,
            "the root secret is readable": vault.find(secret_store.POLARIS_ROOT_SECRET) == root,
            "another managed name is not readable": other_read,
        }


def case_pair_names(out: Path) -> dict[str, bool]:
    """P12: pair names that overlap a Mac name, repeat or lose their own leaf are refused before any call."""
    source = (REPO_ROOT / secret_store.VARIABLES).read_text()
    checks: dict[str, bool] = {"the committed names are ten distinct names": len(setup.writable_names()) == 10}
    plants = {
        "a Mac name": ("/healthcare_hai_prediction/aws_polaris_silver_client_id", "/healthcare_hai_prediction/polaris_silver_client_id"),
        "a repeated name": ("/healthcare_hai_prediction/aws_polaris_gold_client_id", "/healthcare_hai_prediction/aws_polaris_silver_client_id"),
        "another leaf": ("/healthcare_hai_prediction/aws_polaris_e2e_client_secret", "/healthcare_hai_prediction/aws_polaris_e2e_secret"),
    }
    for label, (old, new) in plants.items():
        path = out / f"variables_{re.sub(r'[^a-z]', '_', label)}.tf"
        path.write_text(source.replace(f'default     = "{old}"', f'default     = "{new}"', 1))
        try:
            setup.pair_names(path)
            checks[f"{label} is refused"] = False
        except setup.SetupError as error:
            checks[f"{label} is refused"] = error.code == "parameters"
    return checks


def catalog_world(seeded: dict[str, str] | None = None) -> contextlib.AbstractContextManager[tuple[StandIn, Polaris, str]]:
    @contextlib.contextmanager
    def made() -> Iterator[tuple[StandIn, Polaris, str]]:
        with world(seeded) as (ssm, polaris, root):
            polaris.catalog = setup.catalog_body(BUCKET, REGION)
            polaris.grants["catalog_admin"] = []
            yield ssm, polaris, root

    return made()


def principal(services: Services, root: str, identity: str, approval: str | None = None) -> tuple[int, str, str]:
    argv = ["principal", "--identity", identity, "--instance", INSTANCE, *(["--namespace-approval", approval] if approval else [])]
    result = call(argv, services, root)
    SECRETS_SEEN.extend(services.polaris.issued)
    return result


def stored_pair(ssm: StandIn, identity: str) -> tuple[str | None, str | None]:
    names = setup.configured_pairs()[identity]
    return ssm.values.get(names[0]), ssm.values.get(names[1])


def live_pair(polaris: Polaris, identity: str) -> tuple[str | None, str | None]:
    pair = polaris.principals.get(setup.PRINCIPALS[identity], {})
    return pair.get("clientId"), pair.get("clientSecret")


def same_grants(polaris: Polaris, identity: str, approved: bool) -> bool:
    actual = sorted(json.dumps({"grant": grant}, sort_keys=True) for grant in polaris.grants.get(setup.PRINCIPALS[identity], []))
    return actual == sorted(json.dumps(grant, sort_keys=True) for grant in setup.grant_plan(identity, approved))


def case_reader_principal(_: Path) -> dict[str, bool]:
    """The reader: catalog-wide read grants only, roles assigned, the issued pair stored straight in the two AWS reader names (P1)."""
    with catalog_world() as (ssm, polaris, root):
        code, out, _err = principal(Services(ssm, polaris), root, "reader")
        role = setup.PRINCIPALS["reader"]
        return {
            "the step succeeds": code == 0 and (outcome(out) or {}).get("credentials") == "created",
            "the grants equal the reader plan": same_grants(polaris, "reader", False),
            "every grant is a read privilege": all(grant["privilege"] in catalog.READER_PRIVILEGES for grant in polaris.grants[role]),
            "the catalog role is assigned to the principal role": polaris.principal_roles.get(role) == {role},
            "the principal role is assigned to the principal": polaris.assigned.get(role) == {role},
            "the stored pair is the issued pair": stored_pair(ssm, "reader") == live_pair(polaris, "reader"),
            "no namespace was created": not polaris.namespaces,
        }


def case_rerun_keeps_pair(_: Path) -> dict[str, bool]:
    """P5: a stored pair that Polaris accepts is kept; the rerun issues nothing and writes nothing."""
    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        first = principal(services, root, "reader")
        changes, issued = ssm.changed(), list(polaris.issued)
        second = principal(services, root, "reader")
        return {
            "both runs succeed": first[0] == 0 and second[0] == 0,
            "the rerun keeps the pair": (outcome(second[1]) or {}).get("credentials") == "already present",
            "no new pair was issued": polaris.issued == issued,
            "nothing was written again": ssm.changed() == changes,
        }


def case_put_fails(_: Path) -> dict[str, bool]:
    """P3: Polaris issues a pair and the write fails; the rerun resets the credentials and stores a working pair, and the unstored pair is void."""
    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        ssm.denied.add("PutParameter")
        first = principal(services, root, "reader")
        lost = live_pair(polaris, "reader")
        ssm.denied.clear()
        second = principal(services, root, "reader")
        return {
            "the first run fails": first[0] == 1,
            "a pair had been issued": lost[1] is not None,
            "the rerun resets the credentials": second[0] == 0 and (outcome(second[1]) or {}).get("credentials") == "credentials reset",
            "the stored pair is the live pair": stored_pair(ssm, "reader") == live_pair(polaris, "reader"),
            "the unstored pair no longer works": live_pair(polaris, "reader")[1] != lost[1],
        }


def case_half_pair(_: Path) -> dict[str, bool]:
    """P4: only the client ID was stored before a crash; the run treats the half pair as refused and stores a full new pair."""
    with catalog_world() as (ssm, polaris, root):
        polaris.issue(setup.PRINCIPALS["reader"])
        ssm.values[setup.configured_pairs()["reader"][0]] = live_pair(polaris, "reader")[0] or ""
        code, out, _err = principal(Services(ssm, polaris), root, "reader")
        return {
            "the step succeeds": code == 0,
            "the credentials are reset": (outcome(out) or {}).get("credentials") == "credentials reset",
            "both halves are stored and live": stored_pair(ssm, "reader") == live_pair(polaris, "reader") and None not in stored_pair(ssm, "reader"),
        }


def case_extra_grant(_: Path) -> dict[str, bool]:
    """P7: a grant outside the plan stops the step before the catalog role is assigned; no principal or pair is made."""
    with catalog_world() as (ssm, polaris, root):
        polaris.namespaces |= set(setup.namespace_set("bronze"))
        role = setup.PRINCIPALS["bronze"]
        polaris.grants[role] = [{"type": "namespace", "namespace": ["bronze"], "privilege": "TABLE_DROP"}]
        code, _out, err = principal(Services(ssm, polaris), root, "bronze")
        return {
            "the step fails on the grants": code == 1 and "(grants)" in err,
            "the catalog role is not assigned": not polaris.principal_roles.get(role),
            "no principal was created": role not in polaris.principals,
            "nothing was written": ssm.writes() == 0,
        }


def case_namespace_needs_approval(_: Path) -> dict[str, bool]:
    """P8: absent namespaces without an approval stop the step before any change."""
    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        code, _out, err = principal(services, root, "bronze")
        return {
            "the step fails on the approval": code == 1 and "(approval)" in err,
            "no namespace was created": not polaris.namespaces,
            "no role was created": not polaris.principal_roles and set(polaris.grants) == {"catalog_admin"},
            "no approval was recorded": not services.approvals.exists() or not any(services.approvals.iterdir()),
        }


def case_namespace_approval(_: Path) -> dict[str, bool]:
    """P8: an approval is recorded once for the exact set before creation, grants NAMESPACE_CREATE, carries to a rerun and is never replaced."""
    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        first = principal(services, root, "bronze", "cutover_initial")
        record = json.loads((services.approvals / "bronze.json").read_text()) if (services.approvals / "bronze.json").exists() else {}
        again = principal(services, root, "bronze")
        before = len(polaris.calls)
        other = principal(services, root, "bronze", "another_approval")
        return {
            "the approved run succeeds": first[0] == 0 and (outcome(first[1]) or {}).get("phase") == "active",
            "the absent namespaces were created": polaris.namespaces == set(setup.namespace_set("bronze")),
            "the record names the approval and the exact set": record.get("approval_id") == "cutover_initial"
            and record.get("namespaces") == list(setup.namespace_set("bronze")),
            "the grants are the active plan with NAMESPACE_CREATE": same_grants(polaris, "bronze", True)
            and {"type": "catalog", "privilege": "NAMESPACE_CREATE"} in polaris.grants[setup.PRINCIPALS["bronze"]],
            "a rerun keeps the recorded approval": again[0] == 0 and (outcome(again[1]) or {}).get("phase") == "active",
            "another approval is refused before any request": other[0] == 1 and "(approval)" in other[2] and len(polaris.calls) == before,
        }


def case_prepared_phase(_: Path) -> dict[str, bool]:
    """P8: existing namespaces without an approval get the prepared plan only, with no NAMESPACE_CREATE."""
    with catalog_world() as (ssm, polaris, root):
        polaris.namespaces |= set(setup.namespace_set("bronze"))
        code, out, _err = principal(Services(ssm, polaris), root, "bronze")
        grants = polaris.grants.get(setup.PRINCIPALS["bronze"], [])
        return {
            "the step succeeds in the prepared phase": code == 0 and (outcome(out) or {}).get("phase") == "prepared",
            "the grants are the prepared plan": same_grants(polaris, "bronze", False),
            "no namespace creation is granted": not any(grant["privilege"] == "NAMESPACE_CREATE" for grant in grants),
        }


def case_e2e_principal(_: Path) -> dict[str, bool]:
    """The qualification writer: the twin namespaces plus the table grants on silver_e2e, gold_e2e and features_e2e, in its own pair."""
    with catalog_world() as (ssm, polaris, root):
        code, _out, _err = principal(Services(ssm, polaris), root, "e2e", "qualification_twins")
        grants = polaris.grants.get(setup.PRINCIPALS["e2e"], [])
        return {
            "the step succeeds": code == 0,
            "the seven twin namespaces exist": polaris.namespaces == set(setup.namespace_set("e2e")),
            "the grants equal the active qualification plan": same_grants(polaris, "e2e", True),
            "the silver, gold and features twins get table writes": all(
                {"type": "namespace", "namespace": [name], "privilege": "TABLE_WRITE_DATA"} in grants for name in setup.E2E_TABLE_NAMESPACES
            ),
            "no production namespace is named": not any(grant.get("namespace", [""])[0] in ("bronze", "silver", "gold") for grant in grants),
            "the pair is in the e2e names": stored_pair(ssm, "e2e") == live_pair(polaris, "e2e"),
        }


def case_layer_writers(_: Path) -> dict[str, bool]:
    """P12: the silver and gold writers write their own namespace only, and their pairs go to the AWS names, never the Mac names."""
    checks: dict[str, bool] = {}
    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        for layer, mac in (("silver", secret_store.POLARIS_SILVER_CLIENT_ID), ("gold", secret_store.POLARIS_GOLD_CLIENT_ID)):
            code, _out, _err = principal(services, root, layer, f"{layer}_namespace")
            grants = polaris.grants.get(setup.PRINCIPALS[layer], [])
            checks[f"the {layer} step succeeds"] = code == 0
            checks[f"the {layer} grants equal the writer plan"] = same_grants(polaris, layer, True)
            checks[f"the {layer} grants name only {layer}"] = all(grant.get("namespace", [layer]) == [layer] for grant in grants)
            checks[f"the {layer} grants hold no forbidden privilege"] = not {grant["privilege"] for grant in grants} & set(polaris_writer.FORBIDDEN_PRIVILEGES)
            checks[f"the {layer} pair is in the AWS names"] = stored_pair(ssm, layer) == live_pair(polaris, layer)
            checks[f"the Mac {layer} names are untouched"] = mac not in ssm.values
    return checks


def case_catalog_missing(_: Path) -> dict[str, bool]:
    """P10: a principal before the catalog stops the step with no role, namespace or pair."""
    with world() as (ssm, polaris, root):
        code, _out, err = principal(Services(ssm, polaris), root, "reader")
        return {
            "the step fails on the catalog": code == 1 and "(catalog)" in err,
            "no role was created": not polaris.principal_roles,
            "nothing was written": ssm.writes() == 0,
        }


def case_principal_lock(_: Path) -> dict[str, bool]:
    """P11: a second update of the same principal waits for the lock at most its bound, then stops without issuing or writing a pair."""
    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        lock = services.locks / f"aws_{setup.PRINCIPALS['reader']}.lock"
        with principal_credentials.serialized(lock, "reader", 1.0), mock.patch.object(principal_credentials, "LOCK_WAIT_SECONDS", 0.3):
            began = time.monotonic()
            code, _out, err = principal(services, root, "reader")
            took = time.monotonic() - began
        return {
            "the step stops on the lock": code == 1 and "(credentials)" in err,
            "it waited about its bound": took < 5.0,
            "no pair was issued": not polaris.issued,
            "nothing was written": ssm.writes() == 0,
        }


def case_check_grants(_: Path) -> dict[str, bool]:
    """The check compares each existing catalog role's grants with its plan and changes nothing."""
    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        principal(services, root, "reader")
        clean = call(["inspect", "--instance", INSTANCE], services, root)
        polaris.grants[setup.PRINCIPALS["reader"]].append({"type": "catalog", "privilege": "TABLE_WRITE_DATA"})
        calls = len([item for item in polaris.calls if item[0] in ("POST", "PUT") and item[1] != setup.OAUTH_PATH])
        drifted = call(["inspect", "--instance", INSTANCE], services, root)
        writes = len([item for item in polaris.calls if item[0] in ("POST", "PUT") and item[1] != setup.OAUTH_PATH])
        return {
            "a matching reader is reported": ((outcome(clean[1]) or {}).get("principals") or {}).get("reader") == "matches",
            "an identity without a role is absent": ((outcome(clean[1]) or {}).get("principals") or {}).get("bronze") == "absent",
            "an extra grant is reported": ((outcome(drifted[1]) or {}).get("principals") or {}).get("reader") == "differs",
            "the check changed nothing": writes == calls,
        }


def seed_registry(services: Services, live: bool) -> None:
    """A test launcher registry with one attempt whose cloud try is live, or confirmed complete."""
    now = int(time.time())
    attempt: dict[str, Any] = {"id": "a1", "deadline_utc": now + 3600}
    ecs_target.initialize(attempt, "ecs", now, 1)
    placement = ecs_target.reserve(
        attempt, ecs_target.Size(1024, 2048, 21), ecs_target.Decision("dispatch", "ecs", "aws_first", "FARGATE"), now, ecs_target.Usage()
    )
    if not live:
        ecs_target.fenced(attempt, str(placement["try_id"]), now, no_start=True)
    folder = services.registry.path
    folder.mkdir(parents=True, exist_ok=True)
    registry_id = secrets.token_hex(8)
    (folder / "registry_id").write_text(json.dumps({"registry_id": registry_id, "kind": "test"}) + "\n")
    state = run_lock.empty_state(registry_id, run_lock.LAUNCHER_SCHEMA)
    state["attempts"] = {"a1": attempt}
    (folder / "registry.json").write_text(json.dumps(state) + "\n")


def case_live_writer_try(_: Path) -> dict[str, bool]:
    """P5: while the launcher registry holds a cloud try without a confirmed end, or cannot prove there is none, a rerun that would reset the
    pair stops before any reset; the live pair keeps working. With every try confirmed ended the reset goes ahead."""
    with catalog_world() as (ssm, polaris, root):
        polaris.namespaces |= set(setup.namespace_set("bronze"))
        services = Services(ssm, polaris)
        first = principal(services, root, "bronze")
        held = live_pair(polaris, "bronze")
        del ssm.values[setup.configured_pairs()["bronze"][1]]
        seed_registry(services, live=True)
        live = principal(services, root, "bronze")
        kept = live_pair(polaris, "bronze") == held
        (services.registry.path / "registry.json").write_text("{not json")
        unreadable = principal(services, root, "bronze")
        kept = kept and live_pair(polaris, "bronze") == held
        seed_registry(services, live=False)
        ended = principal(services, root, "bronze")
        return {
            "the first run succeeds": first[0] == 0,
            "a live cloud try stops the rerun": live[0] == 1 and "(writer_try)" in live[2] and "without a confirmed end" in live[2],
            "an unreadable registry stops the rerun": unreadable[0] == 1 and "(writer_try)" in unreadable[2],
            "the live pair was never reset": kept,
            "with every try ended the pair is reset": ended[0] == 0 and (outcome(ended[1]) or {}).get("credentials") == "credentials reset",
            "the stored pair is then the live pair": stored_pair(ssm, "bronze") == live_pair(polaris, "bronze"),
        }


def case_extra_role_membership(_: Path) -> dict[str, bool]:
    """P7: a principal role that already holds another catalog role, or a principal that already holds another principal role, stops the step
    before any assignment, and the read-only inspection reports that identity as differing."""
    role = setup.PRINCIPALS["reader"]
    manage = {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}
    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        polaris.principal_roles[role] = {"catalog_admin"}
        polaris.grants["catalog_admin"] = [manage]
        seeded = principal(services, root, "reader")
        assigned = [item for item in polaris.calls if item[0] == "PUT" and "/catalog-roles/" in item[1] and not item[1].endswith("/grants")]
        untouched = role not in polaris.principals and ssm.writes() == 0
        inspected = call(["inspect", "--instance", INSTANCE], services, root)
    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        polaris.issue(role)
        polaris.principal_roles["service_admin"] = set()
        polaris.assigned[role] = {"service_admin"}
        member = principal(services, root, "reader")
        member_writes = ssm.writes()
    return {
        "an extra catalog role stops the reader": seeded[0] == 1 and "(roles)" in seeded[2],
        "no catalog role was assigned": not assigned,
        "no principal was created and nothing written": untouched,
        "the inspection reports the reader as differing": ((outcome(inspected[1]) or {}).get("principals") or {}).get("reader") == "differs",
        "an extra principal role stops the reader": member[0] == 1 and "(roles)" in member[2],
        "the extra principal role stays and nothing was written": polaris.assigned[role] == {"service_admin"} and member_writes == 0,
    }


# The slow peers: the whole reply trickled, no reply, and complete headers of a closing reply (HTTP/1.0 or Connection: close) with a trickled body.
SLOW_REPLIES = {
    "trickling": (b"", b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 64\r\n\r\n" + b" " * 62 + b"{}"),
    "silent": (b"", b""),
    "HTTP/1.0 body": (b"HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n", b" " * 62 + b"{}"),
    "Connection: close body": (b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n", b" " * 62 + b"{}"),
}


def case_missing_role_assignment(_: Path) -> dict[str, bool]:
    """P7: after a successful reader setup, removing its principal-role assignment or its catalog-role assignment makes the read-only inspection
    report the reader as differing; ``matches`` needs both exact assignments."""
    role = setup.PRINCIPALS["reader"]
    found: dict[str, Any] = {}
    for label in ("principal role", "catalog role"):
        with catalog_world() as (ssm, polaris, root):
            services = Services(ssm, polaris)
            set_up = principal(services, root, "reader")
            clean = call(["inspect", "--instance", INSTANCE], services, root)
            if label == "principal role":
                polaris.assigned[role].discard(role)
            else:
                polaris.principal_roles[role].discard(role)
            found[label] = (set_up[0], clean, call(["inspect", "--instance", INSTANCE], services, root))
    checks: dict[str, bool] = {}
    for label, (set_up, clean, missing) in found.items():
        checks[f"the reader set up and matched before its {label} assignment was removed"] = (
            set_up == 0 and ((outcome(clean[1]) or {}).get("principals") or {}).get("reader") == "matches"
        )
        checks[f"a missing {label} assignment is reported as differing"] = (
            missing[0] == 0 and ((outcome(missing[1]) or {}).get("principals") or {}).get("reader") == "differs"
        )
    return checks


def lock_free(path: Path) -> bool:
    """True when nobody holds the lock file now."""
    with path.open("a") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return True


@contextlib.contextmanager
def held_secret_write(ssm: StandIn, name: str) -> Iterator[tuple[threading.Event, threading.Event, threading.Event]]:
    """The stand-in Parameter Store holds the next write of ``name`` before it commits until ``release`` is set; yields entered, release, finished."""
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    claimed: list[bool] = []

    class HeldWrite(StandInHandler):
        def do_POST(self) -> None:
            payload = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            body = json.loads(payload or b"{}")
            held = not claimed and self.headers.get("X-Amz-Target", "").endswith(".PutParameter") and body.get("Name") == name
            if held:
                claimed.append(True)
                entered.set()
                release.wait(STALL_SECONDS * 3)
            original, self.rfile = self.rfile, io.BytesIO(payload)
            try:
                super().do_POST()
            finally:
                self.rfile = original
                if held:
                    finished.set()

    before = ssm.server.RequestHandlerClass
    ssm.server.RequestHandlerClass = HeldWrite
    try:
        yield entered, release, finished
    finally:
        release.set()
        finished.wait(STALL_SECONDS)
        ssm.server.RequestHandlerClass = before


def case_late_credential_write(_: Path) -> dict[str, bool]:
    """P4 and P5: a credential write still running at the run's deadline keeps the principal lock and the launcher registry lock until it is
    confirmed finished; one that cannot be confirmed in time leaves a pending mark that refuses every rerun until the write has ended, so a late
    write can never overwrite the pair a later run stored."""
    role = setup.PRINCIPALS["reader"]
    secret_name = setup.configured_pairs()["reader"][1]
    args = ["--seconds", "1", "principal", "--identity", "reader", "--instance", INSTANCE]
    checks: dict[str, bool] = {}
    with catalog_world() as (ssm, polaris, root), mock.patch.object(setup, "WRITE_SETTLE_SECONDS", STALL_SECONDS * 2, create=True):
        services = Services(ssm, polaris)
        principal(services, root, "reader")
        del ssm.values[secret_name]
        result: list[tuple[int, str, str]] = []
        with held_secret_write(ssm, secret_name) as (entered, release, finished):
            first = threading.Thread(target=lambda: result.append(call(args, services, root)), daemon=True)
            first.start()
            entered.wait(STALL_SECONDS)
            time.sleep(1.3)
            checks["a write past the deadline keeps the principal lock"] = not lock_free(services.locks / f"aws_{role}.lock")
            checks["a write past the deadline keeps the registry lock"] = not lock_free(services.registry.path / "registry.lock")
            checks["the run waits for its write to be confirmed"] = first.is_alive() and not finished.is_set()
            release.set()
            first.join(STALL_SECONDS * 3)
        rerun = principal(services, root, "reader")
        checks["the confirmed late write fails its run"] = bool(result) and result[0][0] == 1
        checks["a rerun after a confirmed write succeeds with a working pair"] = rerun[0] == 0 and stored_pair(ssm, "reader") == live_pair(polaris, "reader")
    with catalog_world() as (ssm, polaris, root), mock.patch.object(setup, "WRITE_SETTLE_SECONDS", 0.3, create=True):
        services = Services(ssm, polaris)
        principal(services, root, "reader")
        del ssm.values[secret_name]
        with held_secret_write(ssm, secret_name) as (entered, release, finished):
            first_run = call(args, services, root)
            issued = len(polaris.issued)
            refused = principal(services, root, "reader")
            checks["an unconfirmed write fails its run"] = first_run[0] == 1 and not finished.is_set()
            checks["a rerun while the write may commit is refused before any reset"] = (
                refused[0] == 1 and "(credentials)" in refused[2] and len(polaris.issued) == issued
            )
            release.set()
            finished.wait(STALL_SECONDS)
            time.sleep(0.2)
        ended = principal(services, root, "reader")
        checks["once the write has ended a rerun succeeds with a working pair"] = ended[0] == 0 and stored_pair(ssm, "reader") == live_pair(polaris, "reader")
    return checks


def case_stored_pair_mismatch(_: Path) -> dict[str, bool]:
    """P4 and P5: a pair write that lands late (after its client gave up) replaces the stored pair with one Polaris refuses. A run it lands in
    fails with ``stored_pair_mismatch`` and leaves no mark, inspection reports the identity as differing with that cause, and a rerun resets and
    verifies the pair."""
    role = setup.PRINCIPALS["reader"]
    secret_name = setup.configured_pairs()["reader"][1]
    stale = "late-" + secrets.token_hex(12)
    ensure = principal_credentials.ensure_credentials

    def late_write(**kwargs: Any) -> str:
        found = ensure(**kwargs)
        ssm.values[secret_name] = stale
        return found

    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        principal(services, root, "reader")
        ssm.values[secret_name] = stale
        inspected = call(["inspect", "--instance", INSTANCE], services, root)
        report = outcome(inspected[1]) or {}
        repaired = principal(services, root, "reader")
        with mock.patch.object(principal_credentials, "ensure_credentials", late_write):
            ssm.values[secret_name] = stale
            landed = principal(services, root, "reader")
        rerun = principal(services, root, "reader")
        return {
            "inspection reports the late write as differing": (report.get("principals") or {}).get("reader") == "differs",
            "inspection names the stored pair mismatch": (report.get("causes") or {}).get("reader") == "stored_pair_mismatch",
            "a rerun after the late write resets and verifies the pair": repaired[0] == 0
            and (outcome(repaired[1]) or {}).get("credentials") == "credentials reset"
            and stored_pair(ssm, "reader") != (None, stale),
            "a late write inside a run fails it with the named cause": landed[0] == 1 and "(stored_pair_mismatch)" in landed[2],
            "the failed run leaves no pending mark": not setup.pending_write_mark(services.locks, role).exists(),
            "the next rerun repairs the pair": rerun[0] == 0 and stored_pair(ssm, "reader") == live_pair(polaris, "reader"),
        }


def case_late_server_write(_: Path) -> dict[str, bool]:
    """P4 and P5, the review's reproduction with the real SDK timeouts: both attempts of a secret write are held on the server past their client
    read timeout, a rerun stores a working pair, and the held writes then commit. Inspection detects the replaced pair and a rerun repairs it."""
    secret_name = setup.configured_pairs()["reader"][1]
    args = ["principal", "--identity", "reader", "--instance", INSTANCE]
    release, condition = threading.Event(), threading.Condition()
    held: list[bool] = []
    finished: list[bool] = []
    armed = [True]

    class DelayedWrites(StandInHandler):
        def do_POST(self) -> None:
            payload = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            body = json.loads(payload or b"{}")
            delayed = armed[0] and self.headers.get("X-Amz-Target", "").endswith(".PutParameter") and body.get("Name") == secret_name
            if delayed:
                with condition:
                    held.append(True)
                    condition.notify_all()
                release.wait(CASE_SECONDS)
            original, self.rfile = self.rfile, io.BytesIO(payload)
            try:
                super().do_POST()
            finally:
                self.rfile = original
                if delayed:
                    with condition:
                        finished.append(True)
                        condition.notify_all()

    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        principal(services, root, "reader")
        del ssm.values[secret_name]
        before = ssm.server.RequestHandlerClass
        ssm.server.RequestHandlerClass = DelayedWrites
        try:
            first = run_main(["--seconds", "30", *args], services)
            armed[0] = False
            second = principal(services, root, "reader")
            release.set()
            end = time.monotonic() + STALL_SECONDS
            with condition:
                while len(finished) < len(held) and time.monotonic() < end:
                    condition.wait(end - time.monotonic())
        finally:
            release.set()
            ssm.server.RequestHandlerClass = before
        replaced = stored_pair(ssm, "reader") != live_pair(polaris, "reader")
        inspected = outcome(call(["inspect", "--instance", INSTANCE], services, root)[1]) or {}
        repaired = principal(services, root, "reader")
        return {
            "the first run fails after its client timeouts": first[0] == 1 and len(held) == 2,
            "the rerun succeeds before the held writes commit": second[0] == 0,
            "the held writes commit late and replace the pair": len(finished) == len(held) and replaced,
            "inspection reports the late write as a stored pair mismatch": (inspected.get("principals") or {}).get("reader") == "differs"
            and (inspected.get("causes") or {}).get("reader") == "stored_pair_mismatch",
            "a rerun repairs the pair": repaired[0] == 0 and stored_pair(ssm, "reader") == live_pair(polaris, "reader"),
        }


@contextlib.contextmanager
def slow_peer(kind: str) -> Iterator[int]:
    """A loopback peer that reads the request, sends the headers of its ``SLOW_REPLIES`` entry at once, then the rest one byte every 0.04 s."""
    server = socket.create_server(("127.0.0.1", 0))
    stop = threading.Event()
    head, rest = SLOW_REPLIES[kind]

    def serve() -> None:
        with contextlib.suppress(OSError):
            connection, _ = server.accept()
            with connection:
                connection.recv(65536)
                connection.sendall(head)
                for byte in rest:
                    if stop.wait(0.04):
                        return
                    connection.sendall(bytes([byte]))
                stop.wait(STALL_SECONDS)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        yield int(server.getsockname()[1])
    finally:
        stop.set()
        server.close()
        thread.join(timeout=STALL_SECONDS + 2)


def case_request_expiry(_: Path) -> dict[str, bool]:
    """Every wait has a deadline: a peer that trickles its reply or body, or never answers, fails the request at its absolute deadline, not later."""
    checks: dict[str, bool] = {}
    for label in SLOW_REPLIES:
        with slow_peer(label) as port:
            request = setup.TunnelRequest(port, deadline=time.monotonic() + 0.3)
            began = time.monotonic()
            try:
                request("GET", setup.CATALOG_PATH, "token")
                failed = ""
            except setup.SetupError as error:
                failed = error.code
            took = time.monotonic() - began
        checks[f"the {label} peer fails the request on its deadline"] = failed == "deadline"
        checks[f"the {label} peer is cut off within 0.5 s"] = took < 0.5
    return checks


def case_lock_before_changes(_: Path) -> dict[str, bool]:
    """P11, every wait has a deadline: while another update holds the principal's lock past the step deadline, the step stops at its deadline
    before any approval, namespace, role, assignment or pair read."""
    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        lock = services.locks / f"aws_{setup.PRINCIPALS['reader']}.lock"
        held, release = threading.Event(), threading.Event()

        def hold() -> None:
            with principal_credentials.serialized(lock, "reader", 5.0):
                held.set()
                release.wait(0.7)

        holder = threading.Thread(target=hold, daemon=True)
        holder.start()
        held.wait(5.0)
        began = time.monotonic()
        code, out, err = run_main(["--seconds", "0.2", "principal", "--identity", "reader", "--instance", INSTANCE], services)
        took = time.monotonic() - began
        OUTPUTS.append(out + err)
        SECRETS_SEEN.append(root)
        release.set()
        holder.join(timeout=5.0)
        pairs = set(setup.writable_names())
        changes = [item for item in polaris.calls if item[0] in ("POST", "PUT") and item[1] != setup.OAUTH_PATH]
        return {
            "the step stops": code == 1,
            "it stops near its own deadline": took < 0.5,
            "no role, namespace or assignment was sent": not changes and not polaris.principal_roles,
            "no pair name was read": not [name for _action, name in ssm.requests() if name in pairs],
            "nothing was written": ssm.writes() == 0,
        }


def case_check_never_decrypts(_: Path) -> dict[str, bool]:
    """P6: ``check`` takes no instance and never decrypts; the authenticated grant inspection is the separate ``inspect`` step, read only."""
    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        try:
            refused = run_main(["check", "--instance", INSTANCE], services)[0]
        except SystemExit as stop:
            refused = int(stop.code or 0)
        asked = list(ssm.requests()) + [f"{method} {path}" for method, path in polaris.calls]
        code, out, _err = call(["inspect", "--instance", INSTANCE], services, root)
        changes = [item for item in polaris.calls if item[0] in ("POST", "PUT") and item[1] != setup.OAUTH_PATH]
        return {
            "check refuses an instance before any request": refused != 0 and not asked,
            "inspect reports the catalog": code == 0 and (outcome(out) or {}).get("catalog") == "present",
            "inspect reads no pair parameter": not [name for _action, name in ssm.requests() if name in setup.writable_names()],
            "inspect changes nothing": not changes and ssm.writes() == 0,
        }


CASES: dict[str, Case] = {
    "catalog_created": case_catalog_created,
    "catalog_rerun": case_catalog_rerun,
    "catalog_differs": case_catalog_differs,
    "lost_create_reply": case_lost_create_reply,
    "create_conflict": case_create_conflict,
    "mac_port": case_mac_port,
    "host_ingress": case_host_ingress,
    "root_refused": case_root_refused,
    "stalled_catalog": case_stalled_catalog,
    "route_lost": case_route_lost,
    "real_relay": case_real_relay,
    "profile_refused": case_profile_refused,
    "check_metadata": case_check_metadata,
    "check_catalog": case_check_catalog,
    "vault_scope": case_vault_scope,
    "pair_names": case_pair_names,
    "reader_principal": case_reader_principal,
    "rerun_keeps_pair": case_rerun_keeps_pair,
    "put_fails": case_put_fails,
    "half_pair": case_half_pair,
    "extra_grant": case_extra_grant,
    "namespace_needs_approval": case_namespace_needs_approval,
    "namespace_approval": case_namespace_approval,
    "prepared_phase": case_prepared_phase,
    "e2e_principal": case_e2e_principal,
    "layer_writers": case_layer_writers,
    "catalog_missing": case_catalog_missing,
    "principal_lock": case_principal_lock,
    "check_grants": case_check_grants,
    "live_writer_try": case_live_writer_try,
    "extra_role_membership": case_extra_role_membership,
    "request_expiry": case_request_expiry,
    "late_credential_write": case_late_credential_write,
    "stored_pair_mismatch": case_stored_pair_mismatch,
    "late_server_write": case_late_server_write,
    "missing_role_assignment": case_missing_role_assignment,
    "lock_before_changes": case_lock_before_changes,
    "check_never_decrypts": case_check_never_decrypts,
}


def no_secret_case(_: Path) -> dict[str, bool]:
    """P1: no root secret, token or stored value appears in any output of the cases above."""
    text = "\n".join(OUTPUTS)
    return {"no secret appears in any output": bool(OUTPUTS) and not any(value and value in text for value in SECRETS_SEEN)}


def run_cases(out: Path, deadline: float) -> dict[str, dict[str, bool]]:
    results: dict[str, dict[str, bool]] = {}
    for name, case in {**CASES, "no_secret_in_output": no_secret_case}.items():
        if time.monotonic() >= deadline:
            results[name] = {"the case ran before the suite deadline": False}
            continue
        folder = out / name
        folder.mkdir(mode=0o700)
        try:
            results[name] = case(folder)
        except Exception as error:
            results[name] = {f"the case ended with {type(error).__name__}": False}
    return results


# Each planted defect and the case that must fail when it is in place.
MUTATIONS: dict[str, tuple[str, Callable[[], contextlib.AbstractContextManager[Any]]]] = {
    "the location check accepts any catalog": ("catalog_differs", lambda: mock.patch.object(setup, "matches", lambda body, bucket: True)),
    "the check reads with decryption": (
        "check_metadata",
        lambda: mock.patch.object(
            setup.PairVault, "describe", lambda self, name: {"name": name, "present": self.find(name) is not None, "type": "SecureString", "version": 1}
        ),
    ),
    "the vault writes any name": ("vault_scope", lambda: mock.patch.object(setup.PairVault, "check", staticmethod(lambda name, write=False: None))),
    "the Mac port is allowed": ("mac_port", lambda: mock.patch.object(setup, "MAC_POLARIS_PORT", 1)),
    "a conflict is a failure": ("create_conflict", lambda: mock.patch.object(setup, "verify_catalog", lambda *a: "absent")),
    "the grant comparison is skipped": ("extra_grant", lambda: mock.patch.object(setup, "compare_grants", lambda *a: None)),
    "absent namespaces need no approval": (
        "namespace_needs_approval",
        lambda: mock.patch.object(setup, "approval_for", lambda folder, identity, approval_id: setup.Approval("any", identity, setup.namespace_set(identity))),
    ),
    "the approval is not recorded": ("namespace_approval", lambda: mock.patch.object(setup, "record_approval", lambda folder, approval: None)),
    "a working pair is always reset": ("rerun_keeps_pair", lambda: mock.patch.object(principal_credentials, "pair_accepted", lambda *a: False)),
    "the request read has no absolute expiry": (
        "request_expiry",
        lambda: mock.patch.object(setup, "threading", mock.Mock(Event=threading.Event, Timer=lambda *a, **k: mock.Mock())),
    ),
    "the principal lock is not taken first": (
        "lock_before_changes",
        lambda: mock.patch.object(principal_credentials, "serialized", lambda *a: contextlib.nullcontext()),
    ),
    "role memberships are not checked": ("extra_role_membership", lambda: mock.patch.object(setup, "verify_memberships", lambda *a: None)),
    "missing role assignments are accepted": (
        "missing_role_assignment",
        lambda: mock.patch.object(setup, "verify_memberships", lambda request, access, identity, exact=False: None),
    ),
    "a late credential write is abandoned": (
        "late_credential_write",
        lambda: mock.patch.object(
            setup.PairVault, "put", lambda self, name, value: self.within_deadline(lambda: secret_store.SecretStore.put(self, name, value), name)
        ),
    ),
    "the stored pair is not verified": (
        "stored_pair_mismatch",
        lambda: mock.patch.multiple(setup, verify_stored_pair=lambda *a, **k: None, stored_pair_causes=lambda *a, **k: {}),
    ),
    "live writer tries are not checked": ("live_writer_try", lambda: mock.patch.object(setup, "no_live_writer_try", lambda *a, **k: contextlib.nullcontext())),
}


def run_mutations(out: Path, deadline: float) -> dict[str, dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    for label, (intended, plant) in MUTATIONS.items():
        folder = out / f"mutation_{len(found)}"
        folder.mkdir(mode=0o700)
        with plant():
            case = CASES[intended]
            try:
                checks = case(folder)
            except Exception as error:
                checks = {f"ended with {type(error).__name__}": False}
        found[label] = {"intended_case": intended, "caught": not all(checks.values()), "failed_checks": [name for name, ok in checks.items() if not ok]}
        if time.monotonic() >= deadline:
            break
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--out", type=Path, required=True, help="a new directory for the report")
    parser.add_argument("--mutations", action="store_true", help="also plant each defect and require its intended case to fail")
    args = parser.parse_args()
    out = args.out.expanduser().absolute()
    out.mkdir(mode=0o700, parents=True, exist_ok=False)
    deadline = time.monotonic() + (CASE_SECONDS * (len(CASES) + 1))
    global STATE_ROOT
    work = Path(tempfile.mkdtemp(prefix="aws_catalog_setup_", dir=out))
    STATE_ROOT = work / "state"
    try:
        results = run_cases(work, deadline)
        mutations = run_mutations(work, deadline) if args.mutations else {}
    finally:
        shutil.rmtree(work, ignore_errors=True)
    passed = all(all(checks.values()) for checks in results.values()) and all(item["caught"] for item in mutations.values())
    passed = passed and (not args.mutations or len(mutations) == len(MUTATIONS))
    report = {
        "run_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "classification": "synthetic",
        "limits": ["Stand-in Polaris and Parameter Store only; no AWS service, tunnel or container", "the real owned relay is exercised"],
        "repeat_command": [".venv/bin/python", "-m", "scripts.lakehouse.run_aws_catalog_setup_e2e", "--out", "<NEW_DIRECTORY>", "--mutations"],
        "results": results,
        "mutations": mutations,
        "passed": passed,
        "cleanup": "done: stand-ins stopped, relay processes confirmed gone, work folder removed",
    }
    (out / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    for name, checks in results.items():
        for check, ok in checks.items():
            sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}: {check}\n")
    for label, item in mutations.items():
        sys.stdout.write(f"{'CAUGHT' if item['caught'] else 'MISSED'} {label} (case {item['intended_case']})\n")
    sys.stdout.write(f"aws catalog setup e2e: {'passed' if passed else 'failed'}; report {out / 'report.json'}\n")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
