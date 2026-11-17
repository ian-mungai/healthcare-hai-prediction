"""Check the AWS catalog setup job with stand-ins only: a loopback Polaris, the loopback Parameter Store stand-in and in-memory S3, launch hold and ECS.

Run from the repository root: ``.venv/bin/python -m scripts.lakehouse.run_aws_catalog_setup_e2e --out <NEW_DIRECTORY> [--mutations]``. No AWS
service is contacted and no container starts. Each case states one failure mode of the setup and its required behavior. ``--mutations`` plants
each named defect in the setup module in turn and requires its intended case to fail. The report holds names and outcomes only, never a value.
"""

from __future__ import annotations

import argparse
import contextlib
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

from botocore.exceptions import ClientError

from scripts.infrastructure import secret_store
from scripts.infrastructure.secret_store_stand_in import CIPHERTEXT_MARKER, StandIn, StandInHandler
from scripts.lakehouse import aws_catalog_setup as setup
from scripts.lakehouse import catalog_setup_state as state_module
from scripts.lakehouse import polaris_writer, principal_credentials
from scripts.lakehouse.catalog_setup_stand_in import ACCOUNT, FakeClock, FakeEcs, FakeHold, FakeS3, TimedClients
from scripts.lakehouse.catalog_setup_state import CatalogState, Deadline, LaunchGuard, LaunchSpec, StateError

REPO_ROOT = Path(__file__).resolve().parents[2]
BUCKET = "example-lakehouse-bucket"
REGION = "us-west-2"
RUN_SECONDS = 120.0
CASE_SECONDS = 60.0
STALL_SECONDS = 3.0
LOCK_PARAMETER = "/example_project/services_apply_lock"
CLUSTER = "example_cluster"
FAMILY_PREFIX = "example_project_dev_job_"
ROLE_ARN = "arn:aws:iam::" + ACCOUNT + ":role/example_project_catalog_setup_task_dev"
# The suite's work folder; main sets it and removes it at the end.
STATE_ROOT = Path(tempfile.gettempdir())


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
        self.strays: list[tuple[str, str]] = []
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
                    # The client under test always names the realm. A request without it is a local program probing loopback ports.
                    (stand_in.calls if self.headers.get("Polaris-Realm") == "POLARIS" else stand_in.strays).append((self.command, self.path))
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
    """Stand-in services: the stand-in Parameter Store as the task's vault, the stand-in Polaris as the AWS catalog, and in-memory S3, launch hold and ECS."""

    bucket = BUCKET
    region = REGION

    def __init__(self, ssm: StandIn, polaris: Polaris, denied: frozenset[str] = frozenset(), drain: float = 0.0) -> None:
        """``drain`` is the wait after the hold is taken; most cases use none, and the cases of the launch race use the real one on a clock that moves only
        when something sleeps."""
        self.ssm, self.polaris, self.denied = ssm, polaris, denied
        self.s3, self.hold, self.ecs = FakeS3(), FakeHold(), FakeEcs()
        self.time = FakeClock()
        self.clients = TimedClients(s3=self.s3, ssm=self.hold, ecs=self.ecs)
        self.state = CatalogState(self.clients, BUCKET)
        self.guard = LaunchGuard(self.clients, LaunchSpec(LOCK_PARAMETER, CLUSTER, FAMILY_PREFIX), drain=drain, clock=self.time, sleep=self.time.sleep)
        self.decryption: list[bool] = []
        self.refused: list[str] = []
        self.verified: list[bool] = []

    @property
    def polaris_port(self) -> int:
        return self.polaris.port

    def verify_identity(self, deadline: Deadline) -> None:
        self.verified.append(True)

    def root_secret(self) -> str:
        return self.polaris.root_secret

    def vault(self) -> setup.PairVault:
        parameters = secret_store.open_store(REPO_ROOT, self.ssm.endpoint).parameters
        real = parameters.get_parameter
        seen = self.decryption

        denied, refused = self.denied, self.refused

        def spy(**kwargs: Any) -> Any:
            seen.append(bool(kwargs.get("WithDecryption")))
            if kwargs.get("Name") in denied:
                refused.append(kwargs["Name"])
                raise ClientError({"Error": {"Code": "AccessDeniedException", "Message": "not authorized"}}, "GetParameter")
            return real(**kwargs)

        parameters.get_parameter = spy
        return setup.PairVault(parameters)


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
        code, out, _err = call(["catalog"], Services(ssm, polaris), root)
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
        first = call(["catalog"], services, root)
        second = call(["catalog"], services, root)
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
        code, _out, err = call(["catalog"], Services(ssm, polaris), root)
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
        first = call(["catalog"], services, root)
        creates_after_failure = polaris.count("POST", "/api/management/v1/catalogs")
        polaris.faults.clear()
        second = call(["catalog"], services, root)
        return {
            "the first run fails visibly": first[0] == 1 and "(catalog_http)" in first[2],
            "the create was sent once": creates_after_failure == 1,
            "the rerun reports already present": second[0] == 0 and (outcome(second[1]) or {}).get("outcome") == "already present",
            "no second create was ever sent": polaris.count("POST", "/api/management/v1/catalogs") == 1,
        }


def case_create_conflict(_: Path) -> dict[str, bool]:
    """Two runs: a create that meets a conflict reads the catalog back and verifies it instead of failing or retrying the create."""
    with world(faults={"conflict"}) as (ssm, polaris, root):
        code, out, _err = call(["catalog"], Services(ssm, polaris), root)
        return {
            "the step succeeds": code == 0,
            "the outcome is already present": (outcome(out) or {}).get("outcome") == "already present",
            "one create was sent": polaris.count("POST", "/api/management/v1/catalogs") == 1,
            "the catalog was read back": polaris.count("GET", setup.CATALOG_PATH) == 2,
        }


def case_root_refused(_: Path) -> dict[str, bool]:
    """P1: Polaris refuses the root credentials; the error carries the HTTP status only and nothing is created."""
    with world(faults={"root_refused"}) as (ssm, polaris, root):
        code, out, err = call(["catalog"], Services(ssm, polaris), root)
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
        code, _out, err = run_main(["--seconds", "1", "catalog"], Services(ssm, polaris))
        SECRETS_SEEN.append(root)
        OUTPUTS.append(err)
        took = time.monotonic() - began
        return {
            "the step fails": code == 1,
            "it names the lost reply or the deadline": "(catalog_http)" in err or "(deadline)" in err,
            "it stops within two seconds": took < 2.0,
            "no create was sent": polaris.count("POST", "/api/management/v1/catalogs") == 0,
        }


def host_environment() -> dict[str, str]:
    """The environment of the catalog setup task, as its task definition sets it."""
    return {
        "DATA_BUCKET_NAME": BUCKET,
        "AWS_REGION": REGION,
        "CATALOG_SETUP_ROLE_ARN": ROLE_ARN,
        "CATALOG_SETUP_LOCK_PARAMETER": LOCK_PARAMETER,
        "CATALOG_SETUP_CLUSTER": CLUSTER,
        "CATALOG_SETUP_FAMILY_PREFIX": FAMILY_PREFIX,
        "POLARIS_ROOT_SECRET": "root-" + secrets.token_hex(8),
    }


def case_identity_refused(_: Path) -> dict[str, bool]:
    """P2: a caller that is not the task's own role stops the task before any Parameter Store, S3, ECS or catalog call; missing settings stop it first."""
    environment = host_environment()
    made: list[str] = []

    def client_of(caller: str) -> Callable[[Any, str, float], Any]:
        def client(_services: Any, name: str, _left: float) -> Any:
            made.append(name)
            return mock.Mock(get_caller_identity=lambda: {"Arn": caller})

        return client

    other = "arn:aws:sts::" + ACCOUNT + ":assumed-role/example_project_admin_dev/session"
    own = "arn:aws:sts::" + ACCOUNT + ":assumed-role/example_project_catalog_setup_task_dev/0123456789abcdef"
    checks: dict[str, bool] = {}
    with mock.patch.object(setup.HostServices, "bounded_client", client_of(other)):
        services = setup.HostServices(environment)
        code, out, err = run_main(["check"], services)
        OUTPUTS.append(out + err)
        checks["another role stops the task"] = code == 1 and "(identity)" in err
        checks["only the identity call was made"] = made == ["sts"]
    made.clear()
    with mock.patch.object(setup.HostServices, "bounded_client", client_of(own)):
        setup.HostServices(environment).verify_identity(Deadline.after(30))
        checks["the task's own role passes"] = made == ["sts"]
    with mock.patch.object(setup.HostServices, "bounded_client", client_of(own[:-1] + "x/")):
        look_alike = "arn:aws:sts::" + ACCOUNT + ":assumed-role/example_project_catalog_setup_task_dev_extra/session"
        with mock.patch.object(setup.HostServices, "bounded_client", client_of(look_alike)):
            try:
                setup.HostServices(environment).verify_identity(Deadline.after(30))
                checks["a role with the same prefix is refused"] = False
            except setup.SetupError as error:
                checks["a role with the same prefix is refused"] = error.code == "identity"
    for name in ("CATALOG_SETUP_LOCK_PARAMETER", "POLARIS_ROOT_SECRET"):
        trimmed = {key: value for key, value in environment.items() if key != name}
        try:
            setup.HostServices(trimmed).verify_identity(Deadline.after(30)) if name != "POLARIS_ROOT_SECRET" else setup.HostServices(trimmed).root_secret()
            checks[f"a missing {name} is refused"] = False
        except setup.SetupError as error:
            checks[f"a missing {name} is refused"] = error.code in ("settings", "authentication")
    return checks


def case_check_metadata(_: Path) -> dict[str, bool]:
    """The check reports the names the task role may read by exact name with type and version, reads nothing with decryption and prints no value."""
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
            "every name the task role may read is reported": set(found) == set(project_names()),
            "presence matches the store": all(found[name]["present"] == (name in seeded) for name in found),
            "present names report SecureString and a version": all(found[name].get("type") == "SecureString" and found[name].get("version") for name in seeded),
            "no read asked for decryption": bool(services.decryption) and not any(services.decryption),
            "no value or ciphertext is printed": not any(value in out + err for value in seeded.values()) and CIPHERTEXT_MARKER not in out + err,
            "the root secret was not read": (("GetParameter", secret_store.POLARIS_ROOT_SECRET) not in ssm.requests()),
            "nothing was written": ssm.writes() == 0,
            "no catalog request was sent": not polaris.calls,
        }


def project_names() -> set[str]:
    """The pair names the project user's policy grants: every name of every identity, the features pair included."""
    return {name for pair in setup.configured_pairs().values() for name in pair}


def case_features_pair(_: Path) -> dict[str, bool]:
    """The features identity is set up like silver and gold: the issued pair goes straight to its own two names, only names
    and counts are printed, and a rerun keeps the pair."""
    pairs = setup.configured_pairs()["features"]
    with world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        polaris.catalog = setup.catalog_body(BUCKET, REGION)
        argv = ["principal", "--identity", "features", "--namespace-approval", "approval-1"]
        code, out, err = call(argv, services, root)
        SECRETS_SEEN.extend(polaris.issued)
        result = outcome(out) or {}
        first = stored_pair(ssm, "features")
        again = call(argv, services, root)
        return {
            "the step succeeds": code == 0 and err == "",
            "the principal is the features writer": result.get("principal") == "features_writer",
            "the stored pair is the issued pair": first == live_pair(polaris, "features") and all(first),
            "the pair is under its own two names": set(pairs) <= set(ssm.values),
            "no pair value is printed": not any(value and value in out + err for value in first),
            "a rerun keeps the pair": again[0] == 0 and stored_pair(ssm, "features") == first,
        }


def case_check_catalog(_: Path) -> dict[str, bool]:
    """The inspection reads the catalog and changes nothing."""
    with world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        absent = call(["inspect"], services, root)
        polaris.catalog = setup.catalog_body(BUCKET, REGION)
        present = call(["inspect"], services, root)
        return {
            "an absent catalog is reported": absent[0] == 0 and (outcome(absent[1]) or {}).get("catalog") == "absent",
            "a present catalog is reported": present[0] == 0 and (outcome(present[1]) or {}).get("catalog") == "present",
            "no create or grant was sent": not [call for call in polaris.calls if call[0] in ("POST", "PUT") and call[1] != setup.OAUTH_PATH],
        }


def case_vault_scope(_: Path) -> dict[str, bool]:
    """The vault writes only the fourteen AWS pair names, refuses the Mac and root names before any request and reads the root secret."""
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
    (out / setup.WRITER_VARIABLES).write_text((REPO_ROOT / secret_store.VARIABLES).with_name(setup.WRITER_VARIABLES).read_text())
    checks: dict[str, bool] = {"the committed names are fourteen distinct names": len(setup.writable_names()) == 14}
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
    argv = ["principal", "--identity", identity, *(["--namespace-approval", approval] if approval else [])]
    result = call(argv, services, root)
    SECRETS_SEEN.extend(services.polaris.issued)
    return result


def principal_seconds(services: Services, root: str, identity: str, seconds: float) -> tuple[int, str, str]:
    """One principal step with its own step deadline."""
    SECRETS_SEEN.append(root)
    code, out, err = run_main(["--seconds", str(seconds), "principal", "--identity", identity], services)
    OUTPUTS.append(out + err)
    return code, out, err


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
            "every grant is a read privilege": all(grant["privilege"] in polaris_writer.READER_PRIVILEGES for grant in polaris.grants[role]),
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
            "a failed write that is confirmed releases the hold": LOCK_PARAMETER not in services.hold.values,
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
            "no approval was recorded": not [call for call in services.s3.calls if call[0] == "put" and "/approvals/" in call[1]],
        }


def case_namespace_approval(_: Path) -> dict[str, bool]:
    """P8: an approval is recorded once for the exact set before creation as a write-once S3 record, grants NAMESPACE_CREATE, carries to a rerun and is
    never replaced."""
    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        key = services.state.key("approvals", "bronze")
        first = principal(services, root, "bronze", "cutover_initial")
        record = json.loads(services.s3.objects[key][0]) if key in services.s3.objects else {}
        again = principal(services, root, "bronze")
        before = len(polaris.calls)
        other = principal(services, root, "bronze", "another_approval")
        stored = json.loads(services.s3.objects[key][0]) if key in services.s3.objects else {}
        approval_puts = [call for call in services.s3.calls if call[0] == "put" and call[1] == key]
        return {
            "the approved run succeeds": first[0] == 0 and (outcome(first[1]) or {}).get("phase") == "active",
            "the absent namespaces were created": polaris.namespaces == set(setup.namespace_set("bronze")),
            "the record names the approval and the exact set": record.get("approval_id") == "cutover_initial"
            and record.get("namespaces") == list(setup.namespace_set("bronze")),
            "every write of the record is a create only": bool(approval_puts) and all(call[2].get("IfNoneMatch") == "*" for call in approval_puts),
            "the grants are the active plan with NAMESPACE_CREATE": same_grants(polaris, "bronze", True)
            and {"type": "catalog", "privilege": "NAMESPACE_CREATE"} in polaris.grants[setup.PRINCIPALS["bronze"]],
            "a rerun keeps the recorded approval": again[0] == 0 and (outcome(again[1]) or {}).get("phase") == "active",
            "another approval is refused before any request": other[0] == 1 and "(approval)" in other[2] and len(polaris.calls) == before,
            "the recorded approval is unchanged": stored.get("approval_id") == "cutover_initial",
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
    """The silver, gold, features and serving writers write their own namespace only, and their pairs go to the AWS names, never the Mac names."""
    checks: dict[str, bool] = {}
    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        for layer, mac in (
            ("silver", secret_store.POLARIS_SILVER_CLIENT_ID),
            ("gold", secret_store.POLARIS_GOLD_CLIENT_ID),
            ("features", secret_store.PREFIX + "polaris_features_client_id"),
            ("serving", secret_store.PREFIX + "polaris_serving_client_id"),
        ):
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
    """P11: a second update of the same principal waits for the lease at most its bound, then stops without issuing or writing a pair."""
    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        with (
            services.state.bounded(Deadline.after(60)).lease(setup.PRINCIPALS["reader"], wait=1.0, hold=600.0),
            mock.patch.object(principal_credentials, "LOCK_WAIT_SECONDS", 0.3),
        ):
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
        clean = call(["inspect"], services, root)
        polaris.grants[setup.PRINCIPALS["reader"]].append({"type": "catalog", "privilege": "TABLE_WRITE_DATA"})
        calls = len([item for item in polaris.calls if item[0] in ("POST", "PUT") and item[1] != setup.OAUTH_PATH])
        drifted = call(["inspect"], services, root)
        writes = len([item for item in polaris.calls if item[0] in ("POST", "PUT") and item[1] != setup.OAUTH_PATH])
        return {
            "a matching reader is reported": ((outcome(clean[1]) or {}).get("principals") or {}).get("reader") == "matches",
            "an identity without a role is absent": ((outcome(clean[1]) or {}).get("principals") or {}).get("bronze") == "absent",
            "an extra grant is reported": ((outcome(drifted[1]) or {}).get("principals") or {}).get("reader") == "differs",
            "the check changed nothing": writes == calls,
        }


def case_live_writer_try(_: Path) -> dict[str, bool]:
    """P5: while a job task of the project has no confirmed end, or ECS cannot prove there is none, or a services apply holds the launch hold, a
    rerun that would reset the pair stops before any reset; the live pair keeps working. With every task ended the reset goes ahead, and the hold is
    released after every run."""
    with catalog_world() as (ssm, polaris, root):
        polaris.namespaces |= set(setup.namespace_set("bronze"))
        services = Services(ssm, polaris)
        first = principal(services, root, "bronze")
        held = live_pair(polaris, "bronze")
        del ssm.values[setup.configured_pairs()["bronze"][1]]
        services.ecs.add(FAMILY_PREFIX + "bronze_host", "RUNNING")
        services.ecs.add("example_project_dev_airflow_scheduler", "RUNNING")
        live = principal(services, root, "bronze")
        kept = live_pair(polaris, "bronze") == held
        released = LOCK_PARAMETER not in services.hold.values
        services.ecs.tasks.clear()
        services.ecs.add(FAMILY_PREFIX + "gold_host", "RUNNING", desired="STOPPED")
        draining = principal(services, root, "bronze")
        services.ecs.tasks.clear()
        services.ecs.fail = True
        unreadable = principal(services, root, "bronze")
        services.ecs.fail = False
        services.hold.values[LOCK_PARAMETER] = "apply-token"
        applying = principal(services, root, "bronze")
        foreign_kept = services.hold.values.get(LOCK_PARAMETER) == "apply-token"
        kept = kept and live_pair(polaris, "bronze") == held
        del services.hold.values[LOCK_PARAMETER]
        services.ecs.add(FAMILY_PREFIX + "silver_host", "STOPPED", desired="STOPPED")
        services.ecs.add("example_project_dev_airflow_scheduler", "RUNNING")
        ended = principal(services, root, "bronze")
        return {
            "the first run succeeds": first[0] == 0,
            "a live job task stops the rerun": live[0] == 1 and "(writer_try)" in live[2] and "no confirmed end" in live[2],
            "a service task of the project is not a job task": "airflow" not in live[2],
            "a task told to stop that still runs stops the rerun": draining[0] == 1 and "(writer_try)" in draining[2],
            "tasks that cannot be listed stop the rerun": unreadable[0] == 1 and "(writer_try)" in unreadable[2],
            "a services apply holding the hold stops the rerun": applying[0] == 1 and "(writer_try)" in applying[2] and foreign_kept,
            "the live pair was never reset": kept,
            "the hold is released after a stopped run": released,
            "with every task ended the pair is reset": ended[0] == 0 and (outcome(ended[1]) or {}).get("credentials") == "credentials reset",
            "the hold is released after the reset": LOCK_PARAMETER not in services.hold.values,
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
        inspected = call(["inspect"], services, root)
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
            clean = call(["inspect"], services, root)
            if label == "principal role":
                polaris.assigned[role].discard(role)
            else:
                polaris.principal_roles[role].discard(role)
            found[label] = (set_up[0], clean, call(["inspect"], services, root))
    checks: dict[str, bool] = {}
    for label, (set_up, clean, missing) in found.items():
        checks[f"the reader set up and matched before its {label} assignment was removed"] = (
            set_up == 0 and ((outcome(clean[1]) or {}).get("principals") or {}).get("reader") == "matches"
        )
        checks[f"a missing {label} assignment is reported as differing"] = (
            missing[0] == 0 and ((outcome(missing[1]) or {}).get("principals") or {}).get("reader") == "differs"
        )
    return checks


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
    """P4 and P5: a credential write still running at the run's deadline keeps the principal lease and the launch hold until it is
    confirmed finished; one that cannot be confirmed in time leaves a pending mark that refuses every rerun until the write has ended, so a late
    write can never overwrite the pair a later run stored."""
    role = setup.PRINCIPALS["reader"]
    secret_name = setup.configured_pairs()["reader"][1]
    args = ["principal", "--identity", "reader"]
    checks: dict[str, bool] = {}
    # The write deadline is the credential phase's own window, which these cases shorten; the step deadline stays long, so a loaded machine cannot cut it.
    with (
        catalog_world() as (ssm, polaris, root),
        mock.patch.object(setup, "WRITE_SETTLE_SECONDS", STALL_SECONDS * 2, create=True),
        mock.patch.object(setup, "HOLD_SECONDS", 1.0),
    ):
        services = Services(ssm, polaris)
        principal(services, root, "reader")
        del ssm.values[secret_name]
        result: list[tuple[int, str, str]] = []
        with held_secret_write(ssm, secret_name) as (entered, release, finished):
            first = threading.Thread(target=lambda: result.append(call(args, services, root)), daemon=True)
            first.start()
            entered.wait(STALL_SECONDS)
            time.sleep(1.3)
            checks["a write past the deadline keeps the principal lease"] = services.state.key("locks", role) in services.s3.objects
            checks["a write past the deadline keeps the launch hold"] = LOCK_PARAMETER in services.hold.values
            checks["the run waits for its write to be confirmed"] = first.is_alive() and not finished.is_set()
            release.set()
            first.join(STALL_SECONDS * 3)
        rerun = principal(services, root, "reader")
        checks["a late write that is confirmed finished leaves a pair Polaris accepts"] = (
            bool(result) and result[0][0] == 0 and stored_pair(ssm, "reader") == live_pair(polaris, "reader")
        )
        checks["a rerun after a confirmed write succeeds with a working pair"] = rerun[0] == 0 and stored_pair(ssm, "reader") == live_pair(polaris, "reader")
    with (
        catalog_world() as (ssm, polaris, root),
        mock.patch.object(setup, "WRITE_SETTLE_SECONDS", 0.3, create=True),
        mock.patch.object(setup, "HOLD_SECONDS", 1.0),
    ):
        services = Services(ssm, polaris)
        principal(services, root, "reader")
        del ssm.values[secret_name]
        with held_secret_write(ssm, secret_name) as (entered, release, finished):
            first_run = call(args, services, root)
            issued = len(polaris.issued)
            refused = principal(services, root, "reader")
            checks["an unconfirmed write fails its run"] = first_run[0] == 1 and not finished.is_set()
            checks["the unconfirmed write left a pending mark"] = services.state.key("pending", role) in services.s3.objects
            checks["the hold stays after a write that may still commit"] = LOCK_PARAMETER in services.hold.values
            checks["the failure names the command that clears the hold"] = (
                "services_apply resolve" in first_run[2] and services.hold.values.get(LOCK_PARAMETER, "?") in first_run[2]
            )
            checks["a rerun while the write may commit is refused before any reset"] = (
                refused[0] == 1 and "(credentials)" in refused[2] and len(polaris.issued) == issued
            )
            release.set()
            finished.wait(STALL_SECONDS)
            time.sleep(0.2)
        resolve_hold(services)
        ended = principal(services, root, "reader")
        checks["the ended write cleared its mark"] = services.state.key("pending", role) not in services.s3.objects
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
        inspected = call(["inspect"], services, root)
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
            "the failed run leaves no pending mark": services.state.key("pending", role) not in services.s3.objects,
            "the next rerun repairs the pair": rerun[0] == 0 and stored_pair(ssm, "reader") == live_pair(polaris, "reader"),
        }


def case_late_server_write(_: Path) -> dict[str, bool]:
    """P4 and P5, the review's reproduction with the real SDK timeouts: both attempts of a secret write are held on the server past their client
    read timeout, a rerun stores a working pair, and the held writes then commit. Inspection detects the replaced pair and a rerun repairs it."""
    secret_name = setup.configured_pairs()["reader"][1]
    args = ["principal", "--identity", "reader"]
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
        inspected = outcome(call(["inspect"], services, root)[1]) or {}
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
            request = setup.CatalogRequest(port, deadline=time.monotonic() + 0.3)
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
    """P11, every wait has a deadline: while another update holds the principal's lease past the step deadline, the step stops at its deadline
    before any approval, namespace, role, assignment or pair read."""
    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        held, release = threading.Event(), threading.Event()

        def hold() -> None:
            with services.state.bounded(Deadline.after(60)).lease(setup.PRINCIPALS["reader"], wait=5.0, hold=600.0):
                held.set()
                release.wait(0.7)

        holder = threading.Thread(target=hold, daemon=True)
        holder.start()
        held.wait(5.0)
        began = time.monotonic()
        code, out, err = run_main(["--seconds", "0.2", "principal", "--identity", "reader"], services)
        took = time.monotonic() - began
        OUTPUTS.append(out + err)
        SECRETS_SEEN.append(root)
        release.set()
        holder.join(timeout=5.0)
        pairs = set(setup.writable_names())
        changes = [item for item in polaris.calls if item[0] in ("POST", "PUT") and item[1] != setup.OAUTH_PATH]
        return {
            "the step stops": code == 1,
            "the refusal identifies the principal lock": err.startswith(
                f"aws catalog setup principal (credentials): another update of principal {setup.PRINCIPALS['reader']} did not finish within "
            ),
            "it stops near its own deadline": took < 0.5,
            "no role, namespace or assignment was sent": not changes and not polaris.principal_roles,
            "no pair name was read": not [name for _action, name in ssm.requests() if name in pairs],
            "nothing was written": ssm.writes() == 0,
        }


def case_lease_takeover(_: Path) -> dict[str, bool]:
    """P11: a lease is created once, refuses a second holder until its own expiry, is replaced only after that expiry and only at the version that was read,
    and is released only at the version its holder created; nothing is ever deleted."""
    clock = [1000.0]
    s3 = FakeS3()
    state = CatalogState(
        TimedClients(s3=s3), BUCKET, deadline=Deadline.after(3600), clock=lambda: clock[0], sleep=lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    )
    key = state.key("locks", "r")
    checks: dict[str, bool] = {}
    with state.lease("r", wait=1.0, hold=10.0):
        checks["the lease is created without overwrite"] = ("put", key, {"IfNoneMatch": "*"}) in s3.calls
        try:
            with state.lease("r", wait=2.0, hold=10.0):
                checks["a live lease refuses a second holder"] = False
        except StateError as error:
            checks["a live lease refuses a second holder"] = error.code == "credentials" and "lease ends" in str(error)
    released = json.loads(s3.objects[key][0])
    checks["the lease is released at its own version"] = released["expires_at"] == 0 and any(call[0] == "put" and call[2].get("IfMatch") for call in s3.calls)
    checks["no lease record is ever deleted"] = not any(call[0] == "delete" and call[1] == key for call in s3.calls)
    stack = contextlib.ExitStack()
    stack.enter_context(state.lease("r", wait=1.0, hold=10.0))
    clock[0] += 11.0
    with state.lease("r", wait=1.0, hold=10.0):
        taken = json.loads(s3.objects[key][0])
        stack.close()
        after = json.loads(s3.objects[key][0])
        checks["an expired lease is replaced by a second holder"] = taken["expires_at"] > clock[0]
        checks["the first holder never releases the second holder's lease"] = after == taken
    checks["the second holder releases its own lease"] = json.loads(s3.objects[key][0])["expires_at"] == 0
    first, second = state.read(key), state.read(key)
    clock[0] += 100.0
    if first is not None and second is not None:
        won = state.replace(key, {"expires_at": clock[0] + 10}, first[1])
        lost = state.replace(key, {"expires_at": clock[0] + 10}, second[1])
        checks["of two takers of the same version only one wins"] = won is not None and lost is None
    return checks


def case_launch_hold(_: Path) -> dict[str, bool]:
    """P5: the hold is created without overwrite with a token that services_apply resolve accepts, is deleted only while it still holds that token, stays
    when the caller keeps it, and a hold that cannot be released is reported with the command that clears it."""
    from scripts.infrastructure import services_apply

    hold, ecs, clock = FakeHold(), FakeEcs(), FakeClock()
    guard = LaunchGuard(TimedClients(ssm=hold, ecs=ecs), LaunchSpec(LOCK_PARAMETER, CLUSTER, FAMILY_PREFIX), drain=0.0, clock=clock, sleep=clock.sleep)
    deadline = Deadline.after(600)
    checks: dict[str, bool] = {}
    with guard.no_live_writer_try(deadline) as held:
        token = hold.values.get(LOCK_PARAMETER, "")
        checks["the hold token is one that services_apply resolve accepts"] = token == held.token and services_apply.TOKEN.fullmatch(token) is not None
        hold.values[LOCK_PARAMETER] = "apply-token"
    checks["a hold that another party replaced is left alone"] = hold.values.get(LOCK_PARAMETER) == "apply-token"
    hold.values.clear()
    with guard.no_live_writer_try(deadline):
        pass
    checks["a held run deletes its own hold"] = LOCK_PARAMETER not in hold.values and ("delete", LOCK_PARAMETER) in hold.calls
    with guard.no_live_writer_try(deadline) as kept:
        kept.keep()
    checks["a kept hold stays after the step"] = LOCK_PARAMETER in hold.values
    hold.values.clear()
    hold.drop_reply = True
    try:
        with guard.no_live_writer_try(deadline):
            pass
        checks["a hold created without an answer is released by its token"] = False
    except StateError as error:
        checks["a hold created without an answer is released by its token"] = error.unconfirmed and LOCK_PARAMETER not in hold.values
    hold.drop_reply = False
    hold.fail_delete = True
    try:
        with guard.no_live_writer_try(deadline):
            pass
        checks["a hold that cannot be released is reported with its clearing command"] = False
    except StateError as error:
        checks["a hold that cannot be released is reported with its clearing command"] = (
            error.code == "writer_try" and "services_apply resolve" in str(error) and hold.values.get(LOCK_PARAMETER, "?") in str(error)
        )
    return checks


def launcher_window() -> float:
    """The longest time from the start of a launch's hold read to the answer of its RunTask, read from the launcher's own constants: the send boundary's
    authorization window plus the RunTask client's connect and read limits."""
    from orchestration import ecs_adapter, ecs_preflight

    config: Any = ecs_adapter.send_config()
    return float(ecs_preflight.SEND_AUTHORIZATION_SECONDS + config.connect_timeout + config.read_timeout)


def force_reset(ssm: StandIn, identity: str = "bronze") -> None:
    """Remove the stored client secret, so the next run of the identity must reset the pair."""
    ssm.values.pop(setup.configured_pairs()[identity][1], None)


def resolve_hold(services: Services) -> None:
    """Clear the launch hold, when there is one, with the real ``services_apply resolve`` and the token the hold holds, as the owner does."""
    from scripts.infrastructure import services_apply

    parameter = {"value": services_apply.LAUNCH_HOLD_CLEAR}

    def delete() -> None:
        services.hold.values.pop(LOCK_PARAMETER, None)

    store = services_apply.LockStore(
        create=lambda value: services.hold.values.__setitem__(LOCK_PARAMETER, value),
        get=lambda: services.hold.values.get(LOCK_PARAMETER),
        delete=delete,
    )
    steps = services_apply.Steps(
        put=lambda value: parameter.update(value=value), get=lambda: parameter["value"], stable=lambda: True, lock=store, report=lambda line: None
    )
    if LOCK_PARAMETER in services.hold.values:
        steps.resolve("checked that no catalog setup task runs", services.hold.values[LOCK_PARAMETER])


def case_launch_race(_: Path) -> dict[str, bool]:
    """P5: a launch that read the hold as clear just before it was set can still send RunTask afterwards. The reset waits the launcher's whole window after
    the hold, a task that appears in that window stops the reset, and the reset never starts sooner than the window after the hold."""
    window = launcher_window()
    with catalog_world() as (ssm, polaris, root):
        polaris.namespaces |= set(setup.namespace_set("bronze"))
        services = Services(ssm, polaris, drain=state_module.DRAIN_SECONDS)
        first = principal(services, root, "bronze")
        held = live_pair(polaris, "bronze")
        seen: dict[str, float] = {}
        real_put, real_ensure = services.hold.put_parameter, principal_credentials.ensure_credentials

        def note_put(**arguments: Any) -> Any:
            seen["hold"] = services.time.now
            return real_put(**arguments)

        def note_ensure(**arguments: Any) -> Any:
            seen["reset"] = services.time.now
            return real_ensure(**arguments)

        force_reset(ssm)

        def launch_appears() -> None:
            services.ecs.add(FAMILY_PREFIX + "bronze_host", "RUNNING")

        services.time.at(10.0, launch_appears)
        with mock.patch.object(principal_credentials, "ensure_credentials", note_ensure), mock.patch.object(services.hold, "put_parameter", note_put):
            racing = principal(services, root, "bronze")
        entered = "reset" in seen
        kept = live_pair(polaris, "bronze") == held
        released = LOCK_PARAMETER not in services.hold.values
        services.ecs.tasks.clear()
        seen.clear()
        with mock.patch.object(principal_credentials, "ensure_credentials", note_ensure), mock.patch.object(services.hold, "put_parameter", note_put):
            ended = principal(services, root, "bronze")
        return {
            "the first run succeeds": first[0] == 0,
            "a launch admitted before the hold appears during the wait and stops the run": racing[0] == 1
            and "(writer_try)" in racing[2]
            and "no confirmed end" in racing[2],
            "the reset region was never entered": not entered,
            "the live pair was not reset": kept,
            "the hold was released": released,
            "the reset starts only after the launcher's whole window past the hold": ended[0] == 0 and seen.get("reset", 0.0) - seen.get("hold", 0.0) >= window,
        }


def case_unconfirmed_describe(_: Path) -> dict[str, bool]:
    """P5: a job task that ECS lists but cannot describe, answers with no state or names no family is unknown, and unknown is never safe: the step waits a
    bound for it and then stops before any reset. A task that is described as ended, or that becomes describable, lets the reset go ahead."""
    family = FAMILY_PREFIX + "bronze_host"
    with catalog_world() as (ssm, polaris, root):
        polaris.namespaces |= set(setup.namespace_set("bronze"))
        services = Services(ssm, polaris, drain=0.0)
        principal(services, root, "bronze")
        held = live_pair(polaris, "bronze")
        force_reset(ssm)
        arn = services.ecs.add(family, "STOPPED", desired="STOPPED")
        services.ecs.missing.add(arn)
        began = services.time.now
        missing = principal(services, root, "bronze")
        waited = services.time.now - began
        services.ecs.missing.clear()
        services.ecs.silent.add(arn)
        silent = principal(services, root, "bronze")
        services.ecs.silent.clear()
        services.ecs.tasks.append({"taskArn": arn + "x", "lastStatus": "RUNNING", "desiredStatus": "RUNNING"})
        nameless = principal(services, root, "bronze")
        services.ecs.tasks.pop()
        kept = live_pair(polaris, "bronze") == held
        services.ecs.lag[arn] = 2
        lagging = principal(services, root, "bronze")
        force_reset(ssm)
        services.ecs.tasks.clear()
        services.ecs.add(family, "DELETED", desired="STOPPED")
        deleted = principal(services, root, "bronze")
        return {
            "a task described as MISSING stops the reset": missing[0] == 1 and "(writer_try)" in missing[2] and "could not be confirmed ended" in missing[2],
            "the step waited its bound for the task": waited >= state_module.UNKNOWN_WAIT_SECONDS,
            "a listed task that the description leaves out stops the reset": silent[0] == 1 and "could not be confirmed ended" in silent[2],
            "a task that names no family and no group stops the reset": nameless[0] == 1 and "could not be confirmed ended" in nameless[2],
            "the live pair was never reset by an unknown": kept,
            "a task that becomes describable inside the bound lets the reset go ahead": lagging[0] == 0
            and (outcome(lagging[1]) or {}).get("credentials") == "credentials reset",
            "a DELETED task is ended": deleted[0] == 0,
            "the hold is released after each stopped run": LOCK_PARAMETER not in services.hold.values,
        }


def case_hold_recovery(_: Path) -> dict[str, bool]:
    """A setup that dies with the hold taken leaves a token that services_apply resolve accepts. A rerun names the holder and the command, leaves the hold
    alone, and after the real resolve with that token the setup works again."""
    from scripts.infrastructure import services_apply

    with catalog_world() as (ssm, polaris, root):
        polaris.namespaces |= set(setup.namespace_set("bronze"))
        services = Services(ssm, polaris)
        force_reset(ssm)
        with mock.patch.object(LaunchGuard, "release", lambda self, token: None):
            crashed = principal(services, root, "bronze")
        leftover = services.hold.values.get(LOCK_PARAMETER, "")
        blocked = principal(services, root, "bronze")
        unchanged = services.hold.values.get(LOCK_PARAMETER) == leftover
        services.hold.values[LOCK_PARAMETER] = "other.host:42:20261117T200000Z:0123abcd"
        applying = principal(services, root, "bronze")
        services.hold.values[LOCK_PARAMETER] = leftover
        resolve_hold(services)
        cleared = LOCK_PARAMETER not in services.hold.values
        again = principal(services, root, "bronze")
        return {
            "the run that died left its token": crashed[0] == 0
            and services_apply.TOKEN.fullmatch(leftover) is not None
            and leftover.startswith("catalog-setup:"),
            "a rerun is refused and names the leftover holder": blocked[0] == 1 and "(writer_try)" in blocked[2] and leftover in blocked[2],
            "the refusal gives the clearing command with the token": "services_apply resolve" in blocked[2] and f"--token {leftover}" in blocked[2],
            "the leftover hold is not touched by the refused rerun": unchanged,
            "a hold of a services apply is named as such": applying[0] == 1 and "a services apply holds" in applying[2],
            "the real resolve clears the leftover hold": cleared,
            "the setup works again after the resolve": again[0] == 0 and LOCK_PARAMETER not in services.hold.values,
        }


def case_state_deadline(_: Path) -> dict[str, bool]:
    """Every wait has a deadline: the S3, hold and ECS calls are built with limits no longer than the time left and none starts after it. A stalled S3 stops
    a one-second step at about one second, a stalled ECS listing stops the reset and frees the hold, and a step with no room for the launch wait changes
    nothing."""
    with catalog_world() as (ssm, polaris, root):
        stalled = Services(ssm, polaris)
        stalled.clients.latency["s3"] = 30.0
        began = time.monotonic()
        code, out, err = run_main(["--seconds", "1", "principal", "--identity", "reader"], stalled)
        took = time.monotonic() - began
        OUTPUTS.append(out + err)
        SECRETS_SEEN.append(root)
        s3_calls = [call for call in stalled.clients.calls if call[0] == "s3"]
        checks = {
            "a stalled S3 stops the step on its deadline": code == 1 and "(deadline)" in err,
            "it stops within a quarter of a second of the deadline": took < 1.25,
            "every S3 call was limited to the time left": bool(s3_calls) and all(0 < call[3] <= 1.0 for call in s3_calls),
            "nothing was written": not stalled.s3.objects and ssm.writes() == 0 and not polaris.principal_roles,
        }
        scan = Services(ssm, polaris)
        scan.clients.latency["ecs"] = 30.0
        scan.clients.block = False
        scanned = principal(scan, root, "reader")
        checks["a stalled ECS listing stops the reset"] = scanned[0] == 1 and "(writer_try)" in scanned[2] and "no answer" in scanned[2]
        checks["the hold is released after the stalled listing"] = LOCK_PARAMETER not in scan.hold.values
        checks["no pair was issued under the stalled listing"] = not polaris.issued
        checks["every ECS and hold call was limited to its bound"] = all(
            call[3] <= state_module.READ_SECONDS for call in scan.clients.calls if call[0] in ("ecs", "ssm")
        )
        short = Services(ssm, polaris, drain=state_module.DRAIN_SECONDS)
        roles = len(polaris.principal_roles)
        calls = len(polaris.calls)
        tight = principal_seconds(short, root, "reader", 5)
        checks["a step with no room for the launch wait is refused"] = tight[0] == 1 and "(writer_try)" in tight[2] and "no time to wait" in tight[2]
        checks["it changed nothing and took no hold"] = len(polaris.principal_roles) == roles and len(polaris.calls) == calls and not short.hold.calls
        return checks


def case_drain_matches_launcher(_: Path) -> dict[str, bool]:
    """The wait after the hold is read from the launcher: at least its send authorization window plus the RunTask connect and read limits, the same
    30 seconds the services apply waits, and the wait the task's guard uses."""
    from scripts.infrastructure import services_apply

    window = launcher_window()
    guard = setup.HostServices(host_environment()).guard
    return {
        "the wait is longer than the launcher's time from admission to RunTask": window < state_module.DRAIN_SECONDS,
        "the wait equals the services apply settle window": state_module.DRAIN_SECONDS == services_apply.HOLD_SETTLE_SECONDS,
        "the task's guard uses that wait": guard.drain == state_module.DRAIN_SECONDS,
    }


def case_import_closure(_: Path) -> dict[str, bool]:
    """The task runs in the Airflow image from the repository checkout: the setup module reaches, by the imports that run when it loads, no project module
    outside the few it names and no third-party package but botocore and boto3, so it needs nothing the image lacks."""
    import ast

    allowed = {
        "scripts.lakehouse.aws_catalog_setup",
        "scripts.lakehouse.catalog_setup_state",
        "scripts.lakehouse.polaris_writer",
        "scripts.lakehouse.principal_credentials",
        "scripts.lakehouse.bronze_writer",
        "scripts.infrastructure.secret_store",
        "scripts.acquisition.raw_root",
        "scripts.sdk_logs",
    }
    standard = sys.stdlib_module_names
    seen: set[str] = set()
    third_party: set[str] = set()
    pending = ["scripts.lakehouse.aws_catalog_setup"]
    while pending:
        module = pending.pop()
        if module in seen:
            continue
        seen.add(module)
        source = REPO_ROOT / (module.replace(".", "/") + ".py")
        if not source.exists():
            continue
        for node in ast.parse(source.read_text()).body:
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                names = [node.module, *(f"{node.module}.{alias.name}" for alias in node.names)]
            for name in names:
                top = name.split(".")[0]
                if top in ("scripts", "orchestration"):
                    pending.append(name)
                elif top not in standard and top != "__future__":
                    third_party.add(top)
    modules = {name for name in seen if (REPO_ROOT / (name.replace(".", "/") + ".py")).exists()}
    return {
        "no project module outside the reviewed set is imported": modules <= allowed,
        "no third-party package but botocore and boto3 is imported": third_party <= {"botocore", "boto3"},
    }


def case_check_never_decrypts(_: Path) -> dict[str, bool]:
    """P6: ``check`` takes no argument but the deadline and never decrypts; the authenticated grant inspection is the separate ``inspect`` step, read only."""
    with catalog_world() as (ssm, polaris, root):
        services = Services(ssm, polaris)
        try:
            refused = run_main(["check", "--unknown", "x"], services)[0]
        except SystemExit as stop:
            refused = int(stop.code or 0)
        asked = list(ssm.requests()) + [f"{method} {path}" for method, path in polaris.calls]
        code, out, _err = call(["inspect"], services, root)
        changes = [item for item in polaris.calls if item[0] in ("POST", "PUT") and item[1] != setup.OAUTH_PATH]
        return {
            "check refuses an extra argument before any request": refused != 0 and not asked,
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
    "root_refused": case_root_refused,
    "stalled_catalog": case_stalled_catalog,
    "identity_refused": case_identity_refused,
    "check_metadata": case_check_metadata,
    "features_pair": case_features_pair,
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
    "lease_takeover": case_lease_takeover,
    "launch_hold": case_launch_hold,
    "launch_race": case_launch_race,
    "unconfirmed_describe": case_unconfirmed_describe,
    "hold_recovery": case_hold_recovery,
    "state_deadline": case_state_deadline,
    "drain_matches_launcher": case_drain_matches_launcher,
    "import_closure": case_import_closure,
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


ORIGINAL_READ = CatalogState.read
ORIGINAL_SCAN = LaunchGuard.scan


def expired_read(self: CatalogState, key: str) -> Any:
    """The record as it is, with its lease expiry in the past."""
    found = ORIGINAL_READ(self, key)
    return None if found is None else ({**found[0], "expires_at": 0}, found[1])


def unversioned_replace(self: CatalogState, key: str, document: dict[str, Any], version: str, *, window: Deadline | None = None) -> str:
    """A replace that ignores the version it is given."""
    return str(self.call("put_object", window=window, Key=key, Body=json.dumps(document).encode())["ETag"])


def unconditional_release(self: LaunchGuard, token: str) -> None:
    """A release that deletes the hold whoever holds it, with the error translation of the real one."""
    window = Deadline.after(state_module.RELEASE_SECONDS, self.clock)
    try:
        self.call("ssm", "delete_parameter", window, Name=self.spec.lock_parameter)
    except (ClientError, StateError):
        raise StateError("writer_try", f"the host launch hold stays; clear it with {state_module.RESOLVE} {token}") from None


def scan_without_unknown(self: LaunchGuard, deadline: Deadline) -> tuple[int, int]:
    """A scan that counts only the tasks it could describe."""
    return ORIGINAL_SCAN(self, deadline)[0], 0


def unlimited_left(self: Deadline, what: str) -> float:
    """A deadline that never runs out and gives every call the full limits."""
    return 1000.0


def keep_nothing(self: state_module.Hold) -> None:
    """A hold that is released whatever the caller asked."""


# Each planted defect: the case that must fail, the check of that case that must be the one that fails, and the plant. The case must end without an error,
# so a defect that breaks the call it plants into is not counted as caught.
Plant = Callable[[], contextlib.AbstractContextManager[Any]]
MUTATIONS: dict[str, tuple[str, str, Plant]] = {
    "the location check accepts any catalog": (
        "catalog_differs",
        "the error names the location check",
        lambda: mock.patch.object(setup, "matches", lambda body, bucket: True),
    ),
    "the check reads with decryption": (
        "check_metadata",
        "no read asked for decryption",
        lambda: mock.patch.object(
            setup.PairVault, "describe", lambda self, name: {"name": name, "present": self.find(name) is not None, "type": "SecureString", "version": 1}
        ),
    ),
    "the vault writes any name": (
        "vault_scope",
        "the Mac, root and other names are refused",
        lambda: mock.patch.object(setup.PairVault, "check", staticmethod(lambda name, write=False: None)),
    ),
    "the identity is not verified": (
        "identity_refused",
        "another role stops the task",
        lambda: mock.patch.object(setup.HostServices, "verify_identity", lambda self, deadline: None),
    ),
    "a conflict is a failure": (
        "create_conflict",
        "the outcome is already present",
        lambda: mock.patch.object(setup, "verify_catalog", lambda request, access, bucket: "absent"),
    ),
    "the grant comparison is skipped": (
        "extra_grant",
        "the step fails on the grants",
        lambda: mock.patch.object(setup, "compare_grants", lambda request, access, role, expected: None),
    ),
    "absent namespaces need no approval": (
        "namespace_needs_approval",
        "the step fails on the approval",
        lambda: mock.patch.object(setup, "approval_for", lambda state, identity, approval_id: setup.Approval("any", identity, setup.namespace_set(identity))),
    ),
    "the approval is not recorded": (
        "namespace_approval",
        "the record names the approval and the exact set",
        lambda: mock.patch.object(CatalogState, "record_approval", lambda self, identity, document: None),
    ),
    "the approval may be replaced": (
        "namespace_approval",
        "every write of the record is a create only",
        lambda: mock.patch.object(
            CatalogState, "create", lambda self, key, document: self.call("put_object", Key=key, Body=json.dumps(document).encode())["ETag"]
        ),
    ),
    "a working pair is always reset": (
        "rerun_keeps_pair",
        "the rerun keeps the pair",
        lambda: mock.patch.object(principal_credentials, "pair_accepted", lambda request, principal, client_id, client_secret: False),
    ),
    "the request read has no absolute expiry": (
        "request_expiry",
        "the trickling peer fails the request on its deadline",
        lambda: mock.patch.object(setup, "threading", mock.Mock(Event=threading.Event, Timer=lambda *a, **k: mock.Mock())),
    ),
    "the principal lock is not taken first": (
        "lock_before_changes",
        "the refusal identifies the principal lock",
        lambda: mock.patch.object(CatalogState, "lease", lambda self, role, *, wait, hold: contextlib.nullcontext()),
    ),
    "role memberships are not checked": (
        "extra_role_membership",
        "an extra catalog role stops the reader",
        lambda: mock.patch.object(setup, "verify_memberships", lambda request, access, identity, exact=False: None),
    ),
    "missing role assignments are accepted": (
        "missing_role_assignment",
        "a missing principal role assignment is reported as differing",
        lambda: mock.patch.object(setup, "verify_memberships", lambda request, access, identity, exact=False: None),
    ),
    "a late credential write is abandoned": (
        "late_credential_write",
        "the run waits for its write to be confirmed",
        lambda: mock.patch.object(
            setup.PairVault, "put", lambda self, name, value: self.within_deadline(lambda: secret_store.SecretStore.put(self, name, value), name)
        ),
    ),
    "the stored pair is not verified": (
        "stored_pair_mismatch",
        "a late write inside a run fails it with the named cause",
        lambda: mock.patch.multiple(
            setup,
            verify_stored_pair=lambda request, vault, identity: None,
            stored_pair_causes=lambda request, vault, principals: {},
        ),
    ),
    "live writer tries are not checked": (
        "live_writer_try",
        "a live job task stops the rerun",
        lambda: mock.patch.object(LaunchGuard, "no_live_writer_try", lambda self, deadline: contextlib.nullcontext(state_module.Hold("x"))),
    ),
    "a live lease is taken over": ("lease_takeover", "a live lease refuses a second holder", lambda: mock.patch.object(CatalogState, "read", expired_read)),
    "a lease is replaced without its version": (
        "lease_takeover",
        "of two takers of the same version only one wins",
        lambda: mock.patch.object(CatalogState, "replace", unversioned_replace),
    ),
    "the hold is released without its token": (
        "launch_hold",
        "a hold that another party replaced is left alone",
        lambda: mock.patch.object(LaunchGuard, "release", unconditional_release),
    ),
    "no wait follows the hold": (
        "launch_race",
        "a launch admitted before the hold appears during the wait and stops the run",
        lambda: mock.patch.object(state_module, "DRAIN_SECONDS", 0.0),
    ),
    "the wait is shorter than the launcher's window": (
        "drain_matches_launcher",
        "the wait is longer than the launcher's time from admission to RunTask",
        lambda: mock.patch.object(state_module, "DRAIN_SECONDS", 10.0),
    ),
    "a task ECS cannot describe is safe": (
        "unconfirmed_describe",
        "a task described as MISSING stops the reset",
        lambda: mock.patch.object(LaunchGuard, "scan", scan_without_unknown),
    ),
    "a hold with a malformed token": (
        "launch_hold",
        "the hold token is one that services_apply resolve accepts",
        lambda: mock.patch.object(state_module, "new_token", lambda: "catalog_setup-0123456789abcdef0123456789abcdef"),
    ),
    "the hold is released after a write that may still commit": (
        "late_credential_write",
        "the hold stays after a write that may still commit",
        lambda: mock.patch.object(state_module.Hold, "keep", keep_nothing),
    ),
    "calls ignore the time left": ("state_deadline", "every S3 call was limited to the time left", lambda: mock.patch.object(Deadline, "left", unlimited_left)),
    "a leftover hold is not named": (
        "hold_recovery",
        "a rerun is refused and names the leftover holder",
        lambda: mock.patch.object(LaunchGuard, "held_by_another", lambda self, deadline: "the host launch hold is held"),
    ),
}


def run_mutations(out: Path, deadline: float) -> dict[str, dict[str, Any]]:
    """Plant each defect and run its case. A defect is caught only when the case ends without an error and the intended check is among the failed ones."""
    found: dict[str, dict[str, Any]] = {}
    for label, (intended, check, plant) in MUTATIONS.items():
        folder = out / f"mutation_{len(found)}"
        folder.mkdir(mode=0o700)
        error = ""
        with plant():
            try:
                checks = CASES[intended](folder)
            except Exception as raised:
                checks, error = {}, type(raised).__name__
        failed = [name for name, ok in checks.items() if not ok]
        found[label] = {"intended_case": intended, "intended_check": check, "caught": not error and check in failed, "failed_checks": failed, "error": error}
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
        "limits": [
            "Stand-in Polaris, Parameter Store, S3, launch hold and ECS only; no AWS service or container",
            "the task role, the host network and the image are not exercised",
        ],
        "repeat_command": [".venv/bin/python", "-m", "scripts.lakehouse.run_aws_catalog_setup_e2e", "--out", "<NEW_DIRECTORY>", "--mutations"],
        "results": results,
        "mutations": mutations,
        "passed": passed,
        "cleanup": "done: stand-ins stopped, work folder removed",
    }
    (out / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    for name, checks in results.items():
        for check, ok in checks.items():
            sys.stdout.write(f"{'PASS' if ok else 'FAIL'} {name}: {check}\n")
    for label, item in mutations.items():
        sys.stdout.write(f"{'CAUGHT' if item['caught'] else 'MISSED'} {label} (case {item['intended_case']}: {item['intended_check']})\n")
    sys.stdout.write(f"aws catalog setup e2e: {'passed' if passed else 'failed'}; report {out / 'report.json'}\n")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
