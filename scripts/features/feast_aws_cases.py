"""Stand-in cases of the Feast AWS profile (failure modes FA1 to FA29 of the AWS profile package).

    .venv/bin/python -B -m scripts.features.feast_cases --part aws [--case FA6 ...]      # fake catalog, fake Redis, secret-store stand-in; no AWS, no Docker

The profile reads the Iceberg ``features`` namespace through the AWS catalog and writes the host Redis. These cases prove its rules against stand-ins only:
the catalog is a stub (or a loopback token endpoint), Redis is ``online_cases.FakeServer`` and the secret store is ``StoreStandIn`` below.
Nothing here proves the real AWS catalog, the real host Redis or the real Parameter Store.
The report lists case names, failure-mode IDs, check names and verdicts only.
"""

from __future__ import annotations

import abc
import builtins
import contextlib
import hashlib
import http.server
import importlib
import importlib.metadata
import importlib.util
import io
import json
import os
import re
import socket
import sys
import tempfile
import threading
import time
import tomllib
import urllib.parse
from collections.abc import Callable, Iterator, Mapping
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from orchestration import ecs_target
from scripts.features import feast_aws, feast_cases, feast_job, feast_materialize, online_cases
from scripts.features import online_protocol as online
from scripts.features import publication_reads as reads
from scripts.features.feast_harness import Checker, fake_module, stop_worker
from scripts.features.publication_reads import ReadError
from scripts.orchestration.ecs_broker_fake import client_error
from scripts.orchestration.ecs_launch_fake import FakeLaunchBucket
from scripts.training import promote_receipt

SENTINEL = "sentinel-secret-9f31c2"
STORE_VALUE = "pw-from-store-7d2e41"
ISSUED = "issued-token"
LOCAL_YAML_SHA256 = "d534a5b5ae3c615401fb28efb11c7e7ce65d9181a7de2a1df73abeb09269704f"
RUN = online_cases.RUN_A
PORT_OF_NOTHING = "http://127.0.0.1:9/api/catalog"


class StoreStandIn:
    """The secret-store stand-in: a ``get`` that logs every name asked and can fail or stall."""

    def __init__(self, values: Mapping[str, str] | None = None) -> None:
        self.values = dict(values if values is not None else {feast_aws.redis_secret_name(): STORE_VALUE})
        self.asked: list[str] = []
        self.error: Exception | None = None
        self.release = threading.Event()
        self.stall = False

    def get(self, name: str) -> str:
        self.asked.append(name)
        if self.stall:
            self.release.wait(5)
        if self.error is not None:
            raise self.error
        return self.values[name]


class GuardedCatalog:
    """A catalog that answers ``load_table`` of one namespace and records every other call as a write attempt."""

    def __init__(self, inner: feast_cases.StubCatalog) -> None:
        self.inner = inner
        self.loads: list[str] = []
        self.other: list[str] = []

    def load_table(self, identifier: str) -> Any:
        self.loads.append(identifier)
        return self.inner.load_table(identifier)

    def __getattr__(self, name: str) -> Callable[..., Any]:
        def refuse(*arguments: Any, **options: Any) -> Any:
            self.other.append(name)
            raise ReadError(f"the stand-in catalog refuses {name}")

        return refuse


DELEGATION_HEADER = "X-Iceberg-Access-Delegation"
HELD_REPLY_SECONDS = 30.0
VENDING_REFUSED = {"error": {"message": "credential vending is not enabled", "type": "BadRequestException", "code": 400}}


class RestCatalogHandler(http.server.BaseHTTPRequestHandler):
    """A loopback stand-in for the Polaris REST catalog: it records every request and refuses a table or namespace call that asks for vended credentials."""

    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0]
        payload: dict[str, Any]
        if path.endswith("/v1/config"):
            status, payload = 200, {"defaults": {}, "overrides": {}}
        elif "/tables/" in path and self.headers.get(DELEGATION_HEADER) is not None:
            status, payload = 400, VENDING_REFUSED
        else:
            status, payload = 404, {"error": {"message": "the stand-in holds no tables", "type": "NoSuchTableException", "code": 404}}
        self._answer(path, status, payload)

    def do_POST(self) -> None:
        self.rfile.read(int(self.headers.get("Content-Length", "0")))
        path = self.path.split("?", 1)[0]
        if self.headers.get(DELEGATION_HEADER) is not None:
            status, payload = 400, VENDING_REFUSED
        else:
            status, payload = 200, {}
        self._answer(path, status, payload)

    def _answer(self, path: str, status: int, payload: dict[str, Any]) -> None:
        server: Any = self.server
        headers = {name.lower(): value for name, value in self.headers.items()}
        server.requests.append({"path": path, "method": self.command, "headers": headers, "status": status})
        if "/tables/" in path and not server.table_gate.is_set():
            server.table_held.set()
            if not server.table_gate.wait(HELD_REPLY_SECONDS):
                return
        data = json.dumps(payload).encode()
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except OSError:
            return

    def log_message(self, fmt: str, *args: Any) -> None:
        del fmt, args


class CatalogServer(http.server.ThreadingHTTPServer):
    """The loopback server of the REST catalog stand-in. It keeps the connections it accepted, so a case can close the ones a stalled client waits on.

    A case clears ``table_gate`` to hold every table reply until the gate is set again (``table_held`` is set once a reply waits there).
    """

    daemon_threads = True

    def __init__(self, address: tuple[str, int], handler: type[http.server.BaseHTTPRequestHandler]) -> None:
        super().__init__(address, handler)
        self.requests: list[dict[str, Any]] = []
        self.connections: list[socket.socket] = []
        self.table_gate = threading.Event()
        self.table_gate.set()
        self.table_held = threading.Event()

    def get_request(self) -> tuple[socket.socket, Any]:
        connection, address = super().get_request()
        self.connections.append(connection)
        return connection, address

    def abort_connections(self) -> None:
        for connection in self.connections:
            with contextlib.suppress(OSError):
                connection.shutdown(socket.SHUT_RDWR)


@contextlib.contextmanager
def rest_catalog_server() -> Iterator[Any]:
    server = CatalogServer(("127.0.0.1", 0), RestCatalogHandler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.table_gate.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


class RestClientStandIn:
    """Sends the headers that PyIceberg 0.12.0 ``RestCatalog`` sends, for a host without PyIceberg.

    PyIceberg copies every ``header.<name>`` property into the session headers and then calls
    ``setdefault`` for the access delegation header, so only a property that is already present keeps the default out.
    Its table load sends no timeout, like the table request of that client, so only a closed connection ends a stalled reply.
    """

    def __init__(self, name: str, **properties: str | None) -> None:
        del name
        requests = importlib.import_module("requests")
        self.uri = str(properties["uri"]).rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({key[len("header.") :]: value for key, value in properties.items() if key.startswith("header.")})
        self.session.headers["Authorization"] = f"Bearer {properties['token']}"
        self.session.headers.setdefault(DELEGATION_HEADER, "vended-credentials")
        self.session.get(f"{self.uri}/v1/config", params={"warehouse": properties["warehouse"]}, timeout=5).raise_for_status()

    def load_table(self, identifier: str) -> Any:
        namespace, table = identifier.split(".", 1)
        self.session.get(f"{self.uri}/v1/namespaces/{namespace}/tables/{table}").raise_for_status()
        return None

    def create_namespace_if_not_exists(self, namespace: str) -> None:
        self.session.post(f"{self.uri}/v1/namespaces", json={"namespace": [namespace]}, timeout=5).raise_for_status()

    def create_table_if_not_exists(self, identifier: str, schema: Any) -> None:
        namespace, table = identifier.split(".", 1)
        self.session.post(f"{self.uri}/v1/namespaces/{namespace}/tables", json={"name": table, "schema": str(schema)}, timeout=5).raise_for_status()


def aws_environment(**changes: str | None) -> dict[str, str]:
    """The job environment of an AWS Feast job as the job definition would give it (synthetic values)."""
    environ: dict[str, str] = {
        "FEAST_PROFILE": "aws",
        "POLARIS_URI": PORT_OF_NOTHING,
        "POLARIS_CLIENT_ID": "reader-id-1",
        "POLARIS_CLIENT_SECRET": SENTINEL,
        "AWS_REGION": "us-east-2",
        "FEAST_REGISTRY_BUCKET": "example-feature-bucket",
    }
    for name, value in changes.items():
        if value is None:
            environ.pop(name, None)
        else:
            environ[name] = value
    return environ


def loader_for(catalog: Any) -> Callable[..., Any]:
    return lambda name, **properties: catalog


def exchange_stand_in(seen: list[tuple[str, str, str]] | None = None) -> Callable[[str, str, str], str]:
    def exchange(endpoint: str, client_id: str, client_secret: str) -> str:
        if seen is not None:
            seen.append((endpoint, client_id, client_secret))
        return ISSUED

    return exchange


def prepared(
    store: StoreStandIn | None = None, server: online_cases.FakeServer | None = None, **changes: str | None
) -> tuple[dict[str, str], StoreStandIn, online_cases.FakeServer]:
    server = server or online_cases.FakeServer()
    server.phrase = STORE_VALUE
    return aws_environment(**changes), store or StoreStandIn(), server


def redis_factory(server: online_cases.FakeServer) -> Callable[[Mapping[str, str], int], Any]:
    """A Redis client factory whose password is the one in the job environment at connect time, so the fake proves where the password came from."""

    def build(environ: Mapping[str, str], database: int) -> Any:
        return server.client(database, password=environ.get("REDIS_PASSWORD", ""))

    return build


def session_for(
    mode: str,
    environ: dict[str, str],
    store: StoreStandIn,
    server: online_cases.FakeServer,
    catalog: Any,
    *,
    exchange: Callable[[str, str, str], str] | None = None,
    folder: str = "/workspace/out",
    limits: feast_aws.Limits | None = None,
    deadline: Callable[[str], None] | None = None,
    open_store: Callable[[], Any] | None = None,
    clock: Callable[[], float] = time.monotonic,
    total: float = feast_aws.PREPARE_SECONDS,
) -> contextlib.AbstractContextManager[Any]:
    environ.setdefault("FEAST_RUN_FOLDER", folder)
    job = feast_aws.job_settings(environ, "materialize", RUN, mode)
    return feast_aws.catalog_session(
        job,
        environ,
        open_store=open_store or (lambda: store),
        exchange=exchange or exchange_stand_in(),
        loader=lambda name, **properties: catalog,
        redis_factory=redis_factory(server),
        limits=limits or feast_aws.Limits(),
        deadline=deadline,
        clock=clock,
        total=total,
    )


def runs_catalog(namespace: str) -> feast_cases.StubCatalog:
    catalog = feast_cases.StubCatalog(namespace)
    catalog.tables[reads.RUNS_TABLE] = feast_cases.StubTable(reads.RUNS_TABLE, 1, {}, [feast_cases.runs_row(RUN, online_cases.T0, {"t": 5})])
    catalog.tables["t"] = feast_cases.StubTable("t", 5)
    return catalog


class PathBackend(feast_cases.RecordingBackend):
    """The Feast stand-in that keeps the registry path and the registry bootstrap setting the job environment holds when ``apply`` runs, as the real
    backend and its registry store consume them."""

    def __init__(self, schema: dict[str, Any], log: list[str], environ: Mapping[str, str]) -> None:
        super().__init__(schema, log)
        self.environ = environ
        self.consumed = ""
        self.bootstrap = ""

    def apply(self) -> None:
        self.consumed = self.environ.get("FEAST_REGISTRY_PATH", "")
        self.bootstrap = self.environ.get("FEAST_REGISTRY_BOOTSTRAP", "")
        super().apply()


def consumed_registry(mode: str) -> tuple[str, str]:
    """Run a full AWS apply in ``mode`` and return the registry path and the registry bootstrap setting the backend consumed."""
    schema = feast_cases.synthetic_schema()
    with tempfile.TemporaryDirectory(prefix="feast_aws_") as raw:
        catalog, _ = feast_cases.stub_features(schema)
        catalog.namespace = reads.NAMESPACES[mode]
        environ, store, server = prepared(FEAST_RUN_FOLDER=raw)
        backend = PathBackend(schema, [], environ)
        feast_aws.run_job(
            "apply",
            RUN,
            mode,
            environ,
            store=store,
            exchange=exchange_stand_in(),
            loader=loader_for(GuardedCatalog(catalog)),
            redis_factory=redis_factory(server),
            backend=backend,
            lease=lambda: online.writer_lease(environ, control=server.client(online.CONTROL_DB, password=STORE_VALUE)),
            schema=(schema, "c" * 64),
        )
    return backend.consumed, backend.bootstrap


def case_fa1(c: Checker) -> None:
    """FA1: the selector accepts two values, defaults to local and leaves the local profile unchanged."""
    c.check("an unset profile is local", feast_aws.selected_profile({}) == "local")
    c.check("local is local", feast_aws.selected_profile({"FEAST_PROFILE": "local"}) == "local")
    c.check("aws is aws", feast_aws.selected_profile({"FEAST_PROFILE": "aws"}) == "aws")
    for value in ("AWS", "", "cloud", "aws ", "local,aws"):
        c.refused(f"the profile {value!r} is refused", feast_aws.selected_profile, "profile", {"FEAST_PROFILE": value})
    c.check("the local profile uses the committed feature_store.yaml", feast_aws.settings_file_for("local") == feast_job.SETTINGS_FILE)
    c.check("the aws profile uses its own file", feast_aws.settings_file_for("aws") == feast_job.REPO / "feature_store_aws.yaml")
    c.check("feature_store.yaml is byte for byte as before", hashlib.sha256(feast_job.SETTINGS_FILE.read_bytes()).hexdigest() == LOCAL_YAML_SHA256)


def case_fa2(c: Checker) -> None:
    """FA2: the namespace follows the run mode, a disagreeing preset is refused and every table load names the namespace."""
    for mode, namespace in (("real", "features"), ("publish_fixture", "features_e2e")):
        environ = aws_environment()
        settings = feast_aws.aws_settings(environ, mode)
        c.check(f"{mode} reads {namespace}", settings.namespace == namespace)
        environ, store, server = prepared()
        guarded = GuardedCatalog(runs_catalog(namespace))
        with session_for(mode, environ, store, server, guarded):
            c.check(
                f"{mode}: the session sets the namespace and the REST catalog type",
                environ["FEAST_NAMESPACE"] == namespace and environ["FEAST_CATALOG_TYPE"] == "rest",
            )
        c.check(f"{mode}: every load names {namespace}", bool(guarded.loads) and all(item.startswith(f"{namespace}.") for item in guarded.loads))
    other = {"real": "features_e2e", "publish_fixture": "features"}
    for mode, wrong in other.items():
        c.refused(f"{mode} with FEAST_NAMESPACE={wrong} is refused", feast_aws.aws_settings, "namespace", aws_environment(FEAST_NAMESPACE=wrong), mode)
    c.refused("a sql catalog type is refused", feast_aws.aws_settings, "catalog type", aws_environment(FEAST_CATALOG_TYPE="sql"), "real")
    c.refused("an unknown mode is refused", feast_aws.aws_settings, "mode", aws_environment(), "demo")
    c.check(
        "a matching preset is accepted",
        feast_aws.aws_settings(aws_environment(FEAST_NAMESPACE="features", FEAST_CATALOG_TYPE="rest"), "real").namespace == "features",
    )


def case_fa3(c: Checker) -> None:
    """FA3: a full apply and a full materialize write nothing outside the namespace, the registry key or Redis databases 1 and 2."""
    schema = feast_cases.synthetic_schema()
    with tempfile.TemporaryDirectory(prefix="feast_aws_") as raw:
        folder = Path(raw)
        catalog, _ = feast_cases.stub_features(schema)
        catalog.namespace = "features"
        guarded = GuardedCatalog(catalog)
        environ, store, server = prepared(FEAST_RUN_FOLDER=str(folder))
        log: list[str] = []
        record = feast_aws.run_job(
            "apply",
            RUN,
            "real",
            environ,
            store=store,
            exchange=exchange_stand_in(),
            loader=lambda name, **properties: guarded,
            redis_factory=redis_factory(server),
            backend=feast_cases.RecordingBackend(schema, log),
            lease=lambda: online.writer_lease(environ, control=server.client(online.CONTROL_DB, password=STORE_VALUE)),
            schema=(schema, "c" * 64),
        )
        c.check("the apply ran once, then read the registry back", log == ["apply", "read_back"])
        c.check("the apply wrote its record", (folder / "feast_apply.json").is_file() and record["namespace"] == "features")
        c.check("the catalog saw loads of features only", bool(guarded.loads) and all(item.startswith("features.") for item in guarded.loads))
        c.check("the catalog saw no call but load_table", guarded.other == [])
        c.check("database 2 holds no key but the control keys", {key for key in server.dbs[2]} <= {online.LEASE_KEY, online.MARKER_KEY})
        c.check("databases 0 and 1 are empty after an apply", not server.dbs[0] and not server.dbs[1])
        c.check("the registry is the one closed key", environ["FEAST_REGISTRY_PATH"] == f"s3://example-feature-bucket/{reads.REGISTRY_KEYS['apply']}")
    probe = GuardedCatalog(runs_catalog("features"))
    c.refused("the write guard refuses a create call", lambda: probe.create_table("features.x"), "refuses create_table")
    c.check("the refused create call is recorded", probe.other == ["create_table"])


def case_fa4(c: Checker) -> None:
    """FA4: the endpoint is one closed URL from the environment, and no file is opened to find it."""
    good = aws_environment()
    c.check("a plain http endpoint with a port and the catalog path is accepted", feast_aws.aws_settings(good, "real").endpoint == good["POLARIS_URI"])
    for label, value in (
        ("https", "https://host.example.internal:8181/api/catalog"),
        ("user info", "http://user:pw" + chr(64) + "host.example.internal:8181/api/catalog"),
        ("a query", "http://host.example.internal:8181/api/catalog?x=1"),
        ("a fragment", "http://host.example.internal:8181/api/catalog#x"),
        ("no port", "http://host.example.internal/api/catalog"),
        ("another path", "http://host.example.internal:8181/other"),
        ("a file URL", "file:///etc/passwd"),
        ("empty", ""),
    ):
        c.refused(f"an endpoint with {label} is refused", feast_aws.aws_settings, "POLARIS_URI", aws_environment(POLARIS_URI=value), "real")
    c.refused("a missing endpoint is refused", feast_aws.aws_settings, "POLARIS_URI", aws_environment(POLARIS_URI=None), "real")
    opened: list[str] = []
    real_open = builtins.open
    real_read_text = Path.read_text

    def spy_open(file: Any, *arguments: Any, **options: Any) -> Any:
        opened.append(str(file))
        return real_open(file, *arguments, **options)

    def spy_read(self: Path, *arguments: Any, **options: Any) -> str:
        opened.append(str(self))
        return real_read_text(self, *arguments, **options)

    environ, store, server = prepared()
    guarded_catalog = GuardedCatalog(runs_catalog("features"))
    with (
        mock.patch.object(builtins, "open", spy_open),
        mock.patch.object(Path, "read_text", spy_read),
        session_for("real", environ, store, server, guarded_catalog),
    ):
        pass
    c.check("the preparation opened no file", opened == [])


def case_fa5(c: Checker) -> None:
    """FA5: the reader pair comes from the environment, is exchanged once, leaves the environment and never appears in a message."""
    seen: list[tuple[str, str, str]] = []
    environ, store, server = prepared()
    endpoint = environ["POLARIS_URI"]
    catalog = GuardedCatalog(runs_catalog("features"))
    properties: dict[str, Any] = {}

    def loader(name: str, **options: Any) -> Any:
        properties.update(options)
        return catalog

    job = feast_aws.job_settings(dict(environ, FEAST_RUN_FOLDER="/workspace/out"), "materialize", RUN, "real")
    with feast_aws.catalog_session(
        job, environ, open_store=lambda: store, exchange=exchange_stand_in(seen), loader=loader, redis_factory=redis_factory(server), limits=feast_aws.Limits()
    ):
        c.check("the exchange got the endpoint and the reader pair", seen == [(endpoint, "reader-id-1", SENTINEL)])
        c.check("the catalog opened with the token and the endpoint", properties.get("token") == ISSUED and properties.get("uri") == endpoint)
        c.check("the client pair left the environment", "POLARIS_CLIENT_ID" not in environ and "POLARIS_CLIENT_SECRET" not in environ)
        c.check("the token sits in the job environment for Feast", environ.get("FEAST_POLARIS_TOKEN") == ISSUED)
    for name in ("POLARIS_CLIENT_ID", "POLARIS_CLIENT_SECRET"):
        environ, store, server = prepared(**{name: None})
        message = c.refused(f"a missing {name} is refused", lambda e=environ, s=store, v=server: session_for("real", e, s, v, catalog).__enter__(), name)
        c.check(f"the {name} refusal holds no credential", SENTINEL not in message)
    environ, store, server = prepared()
    failing = mock.Mock(side_effect=ReadError("Polaris refused the lakehouse_reader credentials (HTTP 401)"))
    message = c.refused(
        "a refused exchange stops the job", lambda: session_for("real", environ, store, server, catalog, exchange=failing).__enter__(), "HTTP 401"
    )
    c.check("the refusal holds no credential or token", SENTINEL not in message and ISSUED not in message)


def case_fa6(c: Checker) -> None:
    """FA6: the Redis password comes only from the secret store, in memory, and never from the environment or a file."""
    environ, store, server = prepared()
    c.refused(
        "a REDIS_PASSWORD preset in the environment is refused",
        lambda: session_for("real", {**environ, "REDIS_PASSWORD": "from-env"}, store, server, GuardedCatalog(runs_catalog("features"))).__enter__(),
        "REDIS_PASSWORD",
    )
    c.check("the store was not asked after that refusal", store.asked == [])
    written: list[str] = []
    real_open = builtins.open

    def spy_open(file: Any, mode: str = "r", *arguments: Any, **options: Any) -> Any:
        if any(flag in mode for flag in "wax+"):
            written.append(str(file))
        return real_open(file, mode, *arguments, **options)

    guarded = GuardedCatalog(runs_catalog("features"))
    with mock.patch.object(builtins, "open", spy_open), session_for("real", environ, store, server, guarded):
        c.check("the password is in the job environment in memory", environ.get("REDIS_PASSWORD") == STORE_VALUE)
    c.check("the store was asked once for one name", store.asked == [feast_aws.redis_secret_name()])
    c.check("the name is the Redis password secret of the store", feast_aws.redis_secret_name().endswith("/redis_password"))
    c.check("no file was opened for writing", written == [])
    wrong = StoreStandIn({feast_aws.redis_secret_name(): "not-the-phrase"})
    environ, _, server = prepared()
    message = c.refused(
        "a wrong store value reaches the server and its refusal hides the value",
        lambda: session_for("real", environ, wrong, server, guarded).__enter__(),
        "Redis failed at",
    )
    c.check("that refusal holds neither password", "not-the-phrase" not in message and STORE_VALUE not in message)


def case_fa7(c: Checker) -> None:
    """FA7: a missing, failing or silent secret store stops the job inside the deadline and names no value."""
    guarded = GuardedCatalog(runs_catalog("features"))
    environ, _, server = prepared()
    empty = StoreStandIn({})
    c.refused("a missing secret is refused", lambda: session_for("real", environ, empty, server, guarded).__enter__(), "redis_password")
    failing = StoreStandIn()
    failing.error = RuntimeError(f"denied {STORE_VALUE}")
    message = c.refused("a failing store is refused", lambda: session_for("real", environ, failing, server, guarded).__enter__(), "redis_password")
    c.check("the failure names the error type, not its text", "RuntimeError" in message and STORE_VALUE not in message)
    for value in ("", "   "):
        blank = StoreStandIn({feast_aws.redis_secret_name(): value})
        c.refused(f"a blank secret {value!r} is refused", lambda b=blank: session_for("real", environ, b, server, guarded).__enter__(), "empty")
    silent = StoreStandIn()
    silent.stall = True
    started = time.monotonic()
    c.refused(
        "a silent store is refused at the deadline",
        lambda: session_for("real", environ, silent, server, guarded, limits=feast_aws.Limits(secret=0.2)).__enter__(),
        "did not finish within",
    )
    c.check("the wait was bounded", time.monotonic() - started < 2.0)
    silent.release.set()


def case_fa8(c: Checker) -> None:
    """FA8: the Redis host is the host of POLARIS_URI; a FEAST_REDIS_HOST that differs from it is refused, an equal or unset one is accepted."""
    for host in ("polaris", "polaris.example.internal", "10.0.12.7"):
        uri = f"http://{host}:8181/api/catalog"
        c.check(f"{host} in POLARIS_URI gives the Redis host", feast_aws.aws_settings(aws_environment(POLARIS_URI=uri), "real").redis_host == host)
        c.check(
            f"an equal FEAST_REDIS_HOST ({host}) is accepted",
            feast_aws.aws_settings(aws_environment(POLARIS_URI=uri, FEAST_REDIS_HOST=host), "real").redis_host == host,
        )
        c.refused(
            f"a differing FEAST_REDIS_HOST beside {host} is refused",
            feast_aws.aws_settings,
            "FEAST_REDIS_HOST",
            aws_environment(POLARIS_URI=uri, FEAST_REDIS_HOST="10.9.9.9" if host != "10.9.9.9" else "other"),
            "real",
        )
    c.check("an unset FEAST_REDIS_HOST is accepted", feast_aws.aws_settings(aws_environment(FEAST_REDIS_HOST=None), "real").redis_host == "127.0.0.1")
    c.refused("an empty FEAST_REDIS_HOST is refused", feast_aws.aws_settings, "FEAST_REDIS_HOST", aws_environment(FEAST_REDIS_HOST=""), "real")
    guarded = GuardedCatalog(runs_catalog("features"))
    uri = "http://10.0.12.7:8181/api/catalog"
    environ, store, server = prepared(POLARIS_URI=uri)
    with session_for("real", environ, store, server, guarded):
        c.check("the session gives the YAML rendering the derived host", environ.get("FEAST_REDIS_HOST") == "10.0.12.7")
    environ, store, server = prepared(POLARIS_URI=uri, FEAST_REDIS_HOST="10.9.9.9")
    c.refused(
        "a session with a differing FEAST_REDIS_HOST is refused", lambda: session_for("real", environ, store, server, guarded).__enter__(), "FEAST_REDIS_HOST"
    )


def case_fa9(c: Checker) -> None:
    """FA9: a stalled or unreachable Redis ends the preparation with an online-store error, and no retry follows."""
    guarded = GuardedCatalog(runs_catalog("features"))
    environ, store, server = prepared()
    server.stall()
    server.log.clear()
    message = c.refused("a stalled Redis is refused", lambda: session_for("real", environ, store, server, guarded).__enter__(), "Redis failed at")
    c.check("the failure names the error type and no password", "TimeoutError" in message and STORE_VALUE not in message)
    c.check("one command was tried, no retry", len(server.log) == 1)
    server.unstall()

    def unreachable(environ: Mapping[str, str], database: int) -> Any:
        return server.client(database, password=environ.get("REDIS_PASSWORD", ""), unreachable=True)

    environ, store, _ = prepared()
    job = feast_aws.job_settings(dict(environ, FEAST_RUN_FOLDER="/workspace/out"), "materialize", RUN, "real")
    c.refused(
        "an unreachable Redis is refused",
        lambda: feast_aws.catalog_session(
            job,
            environ,
            open_store=lambda: store,
            exchange=exchange_stand_in(),
            loader=lambda n, **p: guarded,
            redis_factory=unreachable,
            limits=feast_aws.Limits(),
        ).__enter__(),
        "ConnectionError",
    )
    c.check("the catalog was not opened before Redis answered", guarded.loads == [])


def case_fa10(c: Checker) -> None:
    """FA10: a silent token endpoint, a catalog that never answers and a catalog that raises each end the preparation visibly."""
    with feast_cases.token_server() as token:
        endpoint = f"http://127.0.0.1:{token.server_port}/api/catalog"
        environ, store, server = prepared(POLARIS_URI=endpoint)
        token.answer, token.delay = (200, {"access_token": "late"}), 1.0
        job = feast_aws.job_settings(dict(environ, FEAST_RUN_FOLDER="/workspace/out"), "materialize", RUN, "real")
        started = time.monotonic()

        def slow_exchange(url: str, client_id: str, client_secret: str) -> str:
            return reads.polaris_token(url, client_id, client_secret, timeout=0.2, attempts=2)

        c.refused(
            "a silent token endpoint is refused",
            lambda: feast_aws.catalog_session(
                job,
                environ,
                open_store=lambda: store,
                exchange=slow_exchange,
                loader=lambda n, **p: GuardedCatalog(runs_catalog("features")),
                redis_factory=redis_factory(server),
                limits=feast_aws.Limits(),
            ).__enter__(),
            "did not answer",
        )
        c.check("the token wait was bounded", time.monotonic() - started < 2.5)
    environ, store, server = prepared()
    release = threading.Event()

    class Hanging:
        def load_table(self, identifier: str) -> Any:
            release.wait(5)
            raise KeyError(identifier)

    started = time.monotonic()
    c.refused(
        "a catalog that never answers is refused at the deadline",
        lambda: session_for("real", environ, store, server, Hanging(), limits=feast_aws.Limits(catalog=0.2)).__enter__(),
        "did not finish within",
    )
    c.check("the catalog wait was bounded", time.monotonic() - started < 2.0)
    release.set()
    environ, store, server = prepared()
    message = c.refused(
        "a catalog without the runs table is refused", lambda: session_for("real", environ, store, server, runs_catalog("other")).__enter__(), "publish_runs"
    )
    c.check("the refusal names the namespace and the error type only", "features.publish_runs" in message and "KeyError" in message)


def case_fa11(c: Checker) -> None:
    """FA11: the phases run in a fixed order under one deadline that names the phase it stops before."""
    environ, store, server = prepared()
    phases: list[str] = []
    with session_for("real", environ, store, server, GuardedCatalog(runs_catalog("features")), deadline=phases.append):
        pass
    c.check(
        "the four phases run in order, then the hand-over",
        phases == ["the secret fetch", "the Redis check", "the token exchange", "the catalog check", feast_aws.HANDOVER],
    )

    def stop_before_redis(phase: str) -> None:
        if phase == "the Redis check":
            raise ReadError(f"the job passed its deadline before {phase}")

    environ, store, server = prepared()
    c.refused(
        "the deadline stops the job and names the phase",
        lambda: session_for("real", environ, store, server, runs_catalog("features"), deadline=stop_before_redis).__enter__(),
        "the Redis check",
    )
    clock = iter([0.0, 10.0, 200.0, 300.0, 400.0])
    check = reads.monotonic_deadline(100.0, clock=lambda: next(clock))
    environ, store, server = prepared()
    c.refused(
        "the shared deadline helper stops a slow preparation",
        lambda: session_for("real", environ, store, server, runs_catalog("features"), deadline=check).__enter__(),
        "deadline",
    )
    c.check("the default total deadline is bounded", 0 < feast_aws.PREPARE_SECONDS <= 600)
    release = threading.Event()

    def blocked_store() -> Any:
        release.wait(5)
        raise RuntimeError("the secret-store client never opened")

    environ, store, server = prepared()
    started = time.monotonic()
    c.refused(
        "a blocked secret-store constructor is refused at the phase limit",
        lambda: session_for(
            "real", environ, store, server, runs_catalog("features"), open_store=blocked_store, limits=feast_aws.Limits(secret=0.2)
        ).__enter__(),
        "did not finish within",
    )
    c.check("the blocked constructor wait was bounded", time.monotonic() - started < 2.0)
    environ, store, server = prepared()
    store.stall = True
    started = time.monotonic()
    message = c.refused(
        "a phase limit above the time left is cut to the time left",
        lambda: session_for("real", environ, store, server, runs_catalog("features"), total=0.3, limits=feast_aws.Limits(secret=30.0)).__enter__(),
        "did not finish within 0.",
    )
    c.check("the secret wait ended with the total deadline", time.monotonic() - started < 2.0 and "30" not in message)
    release.set()
    store.release.set()

    def blocked_loader(name: str, **properties: Any) -> Any:
        threading.Event().wait(5)
        raise RuntimeError("the catalog never opened")

    environ, store, server = prepared()
    started = time.monotonic()
    c.refused(
        "a blocked catalog constructor is refused at the catalog limit",
        lambda: feast_aws.catalog_session(
            feast_aws.job_settings(dict(environ, FEAST_RUN_FOLDER="/workspace/out"), "materialize", RUN, "real"),
            environ,
            open_store=lambda: store,
            exchange=exchange_stand_in(),
            loader=blocked_loader,
            redis_factory=redis_factory(server),
            limits=feast_aws.Limits(catalog=0.2),
        ).__enter__(),
        "did not finish within 0.2",
    )
    c.check("the blocked catalog constructor wait was bounded", time.monotonic() - started < 2.0)
    now = [0.0]

    class SlowFinalLoad(GuardedCatalog):
        def load_table(self, identifier: str) -> Any:
            now[0] = feast_aws.PREPARE_SECONDS + 1
            return super().load_table(identifier)

    environ, store, server = prepared()
    c.refused(
        "a final load that passes the deadline is not handed over",
        lambda: session_for("real", environ, store, server, SlowFinalLoad(runs_catalog("features")), clock=lambda: now[0]).__enter__(),
        "deadline before the hand-over",
    )
    c.check("the password and the token left the environment", "REDIS_PASSWORD" not in environ and "FEAST_POLARIS_TOKEN" not in environ)


def case_fa12(c: Checker) -> None:
    """FA12: the registry is the step's closed S3 key in the bucket of the environment; the deployment file is never read."""
    environ = aws_environment(FEAST_RUN_FOLDER="/workspace/out")
    with mock.patch.object(reads, "deployment_bucket", side_effect=AssertionError("the deployment file was read")):
        for step in ("apply", "materialize"):
            job = feast_aws.job_settings(environ, step, RUN, "real")
            c.check(f"{step}: the registry is the closed key", job.registry_path == f"s3://example-feature-bucket/{reads.REGISTRY_KEYS[step]}")
            c.check(
                f"{step}: the job is a real-mode job in the run folder",
                job.mode == "real" and job.folder == Path("/workspace/out") and job.region == "us-east-2",
            )
    real_key, fixture_key = "lakehouse/feast/registry.pb", "lakehouse/feast_e2e/registry.pb"
    for step in ("apply", "materialize"):
        fixture = feast_aws.job_settings(environ, step, RUN, "publish_fixture")
        c.check(f"{step}: a fixture run uses the literal fixture key", fixture.registry_path == f"s3://example-feature-bucket/{fixture_key}")
        c.check(f"{step}: a real run uses the literal real key", feast_aws.job_settings(environ, step, RUN, "real").registry_path.endswith(f"/{real_key}"))
    c.check("the fixture registry step is the probe key", feast_aws.FIXTURE_REGISTRY_STEP == "probe" and reads.REGISTRY_KEYS["probe"] == fixture_key)
    real_path, real_bootstrap = consumed_registry("real")
    fixture_path, fixture_bootstrap = consumed_registry("publish_fixture")
    c.check("a real apply hands the backend the real registry", real_path == f"s3://example-feature-bucket/{real_key}")
    c.check("a fixture apply hands the backend the fixture registry", fixture_path == f"s3://example-feature-bucket/{fixture_key}")
    c.check(
        "the features job, the registry writer, lets its registry store create a missing registry conditionally",
        real_bootstrap == fixture_bootstrap == "conditional",
    )
    c.refused(
        "FEAST_REGISTRY_PATH is refused",
        feast_aws.job_settings,
        "FEAST_REGISTRY_PATH",
        aws_environment(FEAST_REGISTRY_PATH="registry.pb"),
        "apply",
        RUN,
        "real",
    )
    for bucket in ("", "UPPER", "a..b", "bad bucket"):
        c.refused(f"the bucket {bucket!r} is refused", feast_aws.job_settings, "bucket", aws_environment(FEAST_REGISTRY_BUCKET=bucket), "apply", RUN, "real")
    c.refused("a missing region is refused", feast_aws.job_settings, "AWS_REGION", aws_environment(AWS_REGION=None), "apply", RUN, "real")
    c.refused("a malformed region is refused", feast_aws.job_settings, "AWS_REGION", aws_environment(AWS_REGION="us east"), "apply", RUN, "real")
    c.refused("a malformed run ID is refused", feast_aws.job_settings, "run ID", aws_environment(), "apply", "bad", "real")
    c.refused("the probe step is refused on the AWS profile", feast_aws.job_settings, "step", aws_environment(), "probe", RUN, "real")


def case_fa13(c: Checker) -> None:
    """FA13: the two settings files carry the same stores, so training and serving read the same definitions."""

    def body(path: Path) -> list[str]:
        return [line for line in path.read_text().splitlines() if line.strip() and not line.lstrip().startswith("#")]

    c.check("the AWS settings file exists", feast_aws.AWS_SETTINGS_FILE.is_file())
    registry_store_line = f"  registry_store_type: {REGISTRY_STORE_TYPE}"
    aws_body = body(feast_aws.AWS_SETTINGS_FILE)
    c.check("the AWS file adds the registry store type to the local file's stores and options", registry_store_line in aws_body)
    c.check(
        "the AWS file holds the same stores and options as the local file",
        [line for line in aws_body if line != registry_store_line] == body(feast_job.SETTINGS_FILE),
    )
    text = feast_aws.AWS_SETTINGS_FILE.read_text()
    c.check(
        "every variable of the AWS file is one the profile sets",
        set(reads.VARIABLE.findall(text)) <= set(reads.VARIABLE.findall(feast_job.SETTINGS_FILE.read_text())),
    )
    c.check(
        "the variables of the AWS file are exactly the ones the session sets",
        {first or second for first, second in reads.VARIABLE.findall(text)} == set(feast_aws.YAML_VARIABLES),
    )
    c.check("the AWS file holds no password or host literal", "password=${REDIS_PASSWORD}" in text and "${FEAST_REDIS_HOST}" in text)
    c.check("both profiles use the one definitions module", (feast_job.REPO / "definitions.py").is_file() and not list(feast_job.REPO.glob("definitions_*.py")))


def case_fa14(c: Checker) -> None:
    """FA14: the AWS materialize loads from the namespace of its run mode through the existing load path and marks that publication."""
    for mode, namespace in (("real", "features"), ("publish_fixture", "features_e2e")):
        server = online_cases.FakeServer()
        server.phrase = STORE_VALUE
        rows = [{"spine_key": key} for key in online_cases.KEYS]
        stamps = [{"event_timestamp": online_cases.T0 + timedelta(days=number)} for number in range(len(online_cases.KEYS))]
        catalog = feast_cases.StubCatalog(namespace)
        window = feast_materialize.WINDOW_TABLE
        catalog.tables[reads.SPINE_TABLE] = feast_cases.StubTable(reads.SPINE_TABLE, 11, {}, rows)
        catalog.tables[window] = feast_cases.StubTable(window, 12, {}, stamps)
        snapshots = {reads.SPINE_TABLE: 11, window: 12}
        catalog.tables[reads.RUNS_TABLE] = feast_cases.StubTable(reads.RUNS_TABLE, 99, {}, [feast_cases.runs_row(RUN, online_cases.T0, snapshots)])
        guarded = GuardedCatalog(catalog)
        schema = feast_cases.synthetic_schema()
        with tempfile.TemporaryDirectory(prefix="feast_aws_") as raw:
            environ, store, _ = prepared(FEAST_RUN_FOLDER=raw)
            publication = reads.read_publication(catalog, namespace)

            def hooks(_catalog: Any, pub: reads.Publication, _document: dict[str, Any], *, tables: Any = guarded, held: Any = server) -> online.LoadHooks:
                class Heads(online_cases.Hooks):
                    def verify_heads(self, moment: str) -> None:
                        reads.verify_heads(tables, pub, moment)

                return Heads(held, "v1")

            record = feast_aws.run_job(
                "materialize",
                RUN,
                mode,
                environ,
                store=store,
                exchange=exchange_stand_in(),
                loader=loader_for(guarded),
                redis_factory=redis_factory(server),
                control=server.client(online.CONTROL_DB, password=STORE_VALUE),
                features=server.client(online.FEATURE_DB, password=STORE_VALUE),
                hooks_for=hooks,
                schema=(schema, "d" * 64),
            )
            c.check(f"{mode}: the record names the mode and the entities", record["mode"] == mode and record["entities"] == len(online_cases.KEYS))
            c.check(f"{mode}: the record names the selected publication", record["publication"]["snapshot_set_sha256"] == publication.snapshot_set_sha256)
            c.check(f"{mode}: the marker is ready for the run", server.marker().get("state") == "ready" and server.marker().get("feature_run_id") == RUN)
            c.check(f"{mode}: every load names {namespace}", bool(guarded.loads) and all(item.startswith(f"{namespace}.") for item in guarded.loads))
            c.check(f"{mode}: no write call reached the catalog", guarded.other == [])
            c.check(f"{mode}: databases 1 and 2 hold the load, database 0 nothing", not server.dbs[0] and len(server.dbs[1]) == len(online_cases.KEYS))
            c.check(f"{mode}: the record file is in the run folder", (Path(raw) / "feast_materialize.json").is_file())
            c.check(
                f"{mode}: the record holds no secret",
                SENTINEL not in json.dumps(record) and STORE_VALUE not in json.dumps(record) and ISSUED not in json.dumps(record),
            )


def case_fa15(c: Checker) -> None:
    """FA15: the materialization row exists, is closed to its step, and the switch stays off."""
    root = reads.REPO_ROOT
    jobs = ecs_target.load_jobs(root)
    row = {"template": "pipeline_feast", "step": "materialize", "class": "lakehouse_read"}
    c.check("the materialization row is in the table", row in jobs["jobs"])
    c.check("the rollout switch stays off", jobs["enabled"] is False)
    c.check("the row matches the materialize step", ecs_target.row_for(jobs, "pipeline_feast", {"step": "materialize"}, "lakehouse_read") == row)
    for step in ("apply", "probe", "other"):
        c.check(f"the row does not match the {step} step", ecs_target.row_for(jobs, "pipeline_feast", {"step": step}, "lakehouse_read") is None)
    c.check("the row does not match another class", ecs_target.row_for(jobs, "pipeline_feast", {"step": "materialize"}, "offline") is None)
    c.check("the other rows keep their meaning", ecs_target.row_for(jobs, "pipeline_spark", {"layer": "silver"}, "lakehouse_read") is not None)
    c.check("the table has the earlier rows and the new one", len(jobs["jobs"]) == 9)
    schema = json.loads((root / "config/orchestration/ecs_jobs.schema.json").read_text())
    c.check("the schema names pipeline_feast and the step", "pipeline_feast" in json.dumps(schema) and "materialize" in json.dumps(schema))
    forbidden = dict(jobs, jobs=[*jobs["jobs"], {"template": "pipeline_feast", "step": "apply", "class": "lakehouse_read"}])
    with tempfile.TemporaryDirectory(prefix="feast_aws_") as raw:
        folder = Path(raw)
        (folder / "config/orchestration").mkdir(parents=True)
        (folder / "config/orchestration/ecs_jobs.schema.json").write_text(json.dumps(schema))
        (folder / "config/orchestration/ecs_jobs.json").write_text(json.dumps(forbidden))
        try:
            ecs_target.load_jobs(folder)
            refused = False
        except ecs_target.PlacementError:
            refused = True
    c.check("a row for the apply step is refused by the schema", refused)


def case_fa16(c: Checker) -> None:
    """FA16: the job entry point stays on the local path without the aws profile and calls the AWS runner only with it."""
    calls: list[tuple[Any, ...]] = []

    def fake_run(step: str, run: str, mode: str, environ: Any, **options: Any) -> dict[str, Any]:
        calls.append((step, run, mode))
        return {"step": step, "mode": mode, "entities": 1}

    environ = aws_environment()
    with mock.patch.object(feast_aws, "run_job", fake_run):
        code = feast_job.main(["materialize", "--run", RUN, "--mode", "real"], environ)
    c.check("the aws profile reaches the AWS runner once", code == 0 and calls == [("materialize", RUN, "real")])
    local = {"FEAST_NAMESPACE": "features_e2e"}
    calls.clear()
    with mock.patch.object(feast_aws, "run_job", fake_run):
        code = feast_job.main(["materialize", "--run", RUN, "--mode", "publish_fixture"], local)
    c.check("the local profile never reaches the AWS runner", calls == [] and code == 1)
    with mock.patch.object(feast_aws, "run_job", fake_run):
        code = feast_job.main(["materialize", "--run", RUN, "--mode", "real"], {"FEAST_PROFILE": "cloud"})
    c.check("an unknown profile stops the job", calls == [] and code == 1)


def case_fa17(c: Checker) -> None:
    """FA17: PyIceberg asks Polaris to vend credentials unless told otherwise, and the AWS catalog cannot vend. Feast's reader builds its catalog with no way to
    pass a header, so the session sets the variable that PyIceberg reads when it first loads, before anything can import it."""
    environ, store, server = prepared()
    seen: list[str | None] = []

    def loader(name: str, **properties: Any) -> Any:
        seen.append(environ.get(feast_aws.NO_DELEGATION_VARIABLE))
        return runs_catalog("features")

    job = feast_aws.job_settings(environ, "materialize", RUN, "real")
    session = feast_aws.catalog_session(
        job, environ, open_store=lambda: store, exchange=exchange_stand_in(), loader=loader, redis_factory=redis_factory(server)
    )
    with session:
        pass
    c.check("the variable is empty when the catalog is opened, so no delegation mode is asked for", seen == [""])
    region = environ.get("PYICEBERG_CATALOG__HAI__S3__REGION")
    c.check("PyIceberg's catalog hai gets the job's region, so its S3 file reader resolves no bucket region", region == job.region == "us-east-2")
    c.check("the variable names catalog hai and the header", feast_aws.NO_DELEGATION_VARIABLE == "PYICEBERG_CATALOG__HAI__HEADER__X_ICEBERG_ACCESS_DELEGATION")


def case_fa18(c: Checker) -> None:
    """FA18: the REST catalog asks Polaris for no vended credentials, so a table load reaches the catalog without the access delegation header."""
    real = importlib.util.find_spec("pyiceberg") is not None
    loader: Callable[..., Any] | None = None if real else RestClientStandIn
    client = f"PyIceberg {importlib.import_module('pyiceberg').__version__}" if real else "the PyIceberg 0.12.0 header stand-in"
    with rest_catalog_server() as server:
        endpoint = f"http://127.0.0.1:{server.server_port}/api/catalog"
        settings = reads.CatalogSettings("real", "features", "rest", endpoint=endpoint, token=ISSUED)
        outcome: dict[str, str] = {}

        def open_and_load() -> None:
            try:
                reads.load_table(reads.open_catalog(settings, loader), "features", reads.RUNS_TABLE)
            except ReadError as error:
                outcome["error"] = str(error)
            except Exception as error:
                outcome["error"] = f"open failed ({type(error).__name__})"

        worker = threading.Thread(target=open_and_load, daemon=True)
        worker.start()
        stop_worker(c, f"the catalog open and table load ({client})", worker, server.abort_connections, 20)
        paths = [request["path"] for request in server.requests]
        c.check("the client asked for the catalog config", any(path.endswith("/v1/config") for path in paths))
        c.check("the client asked for the publish_runs table", any(path.endswith(f"/tables/{reads.RUNS_TABLE}") for path in paths))
        delegated = [request["path"] for request in server.requests if DELEGATION_HEADER.lower() in request["headers"]]
        c.check(f"no request carries the {DELEGATION_HEADER} header", delegated == [])
        refused = [request["path"] for request in server.requests if request["status"] == 400]
        c.check("no table load is refused for credential vending", refused == [] and "BadRequestError" not in outcome.get("error", ""))


REGISTRY_STORE_MODULE = "scripts.features.feast_registry_store"
REGISTRY_STORE_TYPE = f"{REGISTRY_STORE_MODULE}.S3ObjectRegistryStore"
REGISTRY_BUCKET = "example-registry-bucket"
REGISTRY_KEY = "lakehouse/feast/registry.pb"
REGISTRY_URI = f"s3://{REGISTRY_BUCKET}/{REGISTRY_KEY}"
PYICEBERG_REGION_VARIABLE = "PYICEBERG_CATALOG__HAI__S3__REGION"
REGISTRY_BOOTSTRAP = ("FEAST_REGISTRY_BOOTSTRAP", "conditional")  # the setting with which the features job, the registry writer, runs
S3_ERROR = '<?xml version="1.0" encoding="UTF-8"?><Error><Code>{code}</Code><Message>{code}</Message></Error>'


class S3Handler(http.server.BaseHTTPRequestHandler):
    """A loopback stand-in for the registry bucket of a role that holds GetObject and PutObject on the registry key and nothing else.

    It records every request, and the ``If-None-Match`` header of every PUT in ``server.conditions``. The object is ``server.stored``;
    ``server.refuse_object`` makes a read of the key answer 403, ``server.allow_put = False`` models a reader role without PutObject and
    ``server.drop_put`` closes the connection of every PUT without an answer, a transport failure. A read of a missing
    key answers 403, not 404, because S3 answers 404 only to a caller that may list the bucket. A PUT with ``If-None-Match: *`` answers 412 when the
    object exists. Any call on the bucket itself (HeadBucket, ListObjectsV2) and any call on another key answers 403, as S3 does for a caller without
    those grants.
    """

    protocol_version = "HTTP/1.1"

    def _route(self) -> None:
        server: Any = self.server
        parts = urllib.parse.urlsplit(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b""
        server.requests.append({"method": self.command, "path": parts.path, "query": parts.query})
        if self.command == "PUT":
            server.conditions.append(self.headers.get("If-None-Match", ""))
        if self.command == "PUT" and server.drop_put:
            self.close_connection = True
            return
        if parts.path != f"/{REGISTRY_BUCKET}/{REGISTRY_KEY}" or (self.command == "PUT" and not server.allow_put):
            self._answer(403, S3_ERROR.format(code="AccessDenied").encode())
        elif self.command == "PUT" and self.headers.get("If-None-Match") == "*" and server.stored is not None:
            self._answer(412, S3_ERROR.format(code="PreconditionFailed").encode())
        elif self.command == "PUT":
            server.stored = body
            self._answer(200, b"", {"ETag": '"0"'})
        elif self.command == "DELETE":
            server.stored = None
            self._answer(204, b"")
        elif server.refuse_object or server.stored is None:
            self._answer(403, S3_ERROR.format(code="AccessDenied").encode())
        else:
            self._answer(200, server.stored, {"Content-Type": "application/octet-stream", "ETag": '"0"'})

    do_GET = do_PUT = do_HEAD = do_DELETE = _route

    def _answer(self, status: int, body: bytes, headers: Mapping[str, str] | None = None) -> None:
        try:
            self.send_response(status)
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)
        except OSError:
            return

    def log_message(self, fmt: str, *args: Any) -> None:
        del fmt, args


@contextlib.contextmanager
def s3_server() -> Iterator[Any]:
    server: Any = http.server.ThreadingHTTPServer(("127.0.0.1", 0), S3Handler)
    server.requests = []
    server.conditions = []
    server.stored = None
    server.refuse_object = False
    server.allow_put = True
    server.drop_put = False
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


class RegistryProtoStandIn:
    """The two fields of Feast's ``Registry`` message that the store touches, for a host without Feast; the bytes are JSON."""

    class Stamp:
        def __init__(self) -> None:
            self.moment = ""

        def FromDatetime(self, value: Any) -> None:
            self.moment = value.isoformat()

    def __init__(self) -> None:
        self.version_id = ""
        self.last_updated = self.Stamp()

    def ParseFromString(self, data: bytes) -> None:
        fields = json.loads(data)
        self.version_id = fields["version_id"]
        self.last_updated.moment = fields["moment"]

    def SerializeToString(self) -> bytes:
        return json.dumps({"version_id": self.version_id, "moment": self.last_updated.moment}).encode()


class RegistryStore(abc.ABC):
    """Feast's ``RegistryStore`` interface as of 0.66.0 and 0.67.0: three abstract methods."""

    @abc.abstractmethod
    def get_registry_proto(self) -> Any: ...

    @abc.abstractmethod
    def update_registry_proto(self, registry_proto: Any) -> None: ...

    @abc.abstractmethod
    def teardown(self) -> None: ...


@contextlib.contextmanager
def registry_store_module(real: bool) -> Iterator[Any]:
    """The project's registry store module: imported against Feast when it is installed, otherwise against the two Feast modules it imports, as stand-ins."""
    if real:
        sys.modules.pop(REGISTRY_STORE_MODULE, None)
        yield importlib.import_module("scripts.features.feast_registry_store")
        return
    stand_ins = {
        "feast.infra.registry.registry_store": fake_module("feast.infra.registry.registry_store", RegistryStore=RegistryStore),
        "feast.protos.feast.core.Registry_pb2": fake_module("feast.protos.feast.core.Registry_pb2", Registry=RegistryProtoStandIn),
    }
    with mock.patch.dict(sys.modules, stand_ins):
        sys.modules.pop(REGISTRY_STORE_MODULE, None)
        yield importlib.import_module("scripts.features.feast_registry_store")


@contextlib.contextmanager
def s3_client_environment(server: Any) -> Iterator[None]:
    """The client settings of a loopback S3 call: obviously fake credentials, no profile, no instance metadata lookup, no config file."""
    settings = {
        "AWS_ENDPOINT_URL_S3": f"http://127.0.0.1:{server.server_port}",
        "AWS_ACCESS_KEY_ID": "registry-case-key-id",
        "AWS_SECRET_ACCESS_KEY": "registry-case-secret",
        "AWS_DEFAULT_REGION": "us-east-2",
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_CONFIG_FILE": os.devnull,
        "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
        "NO_PROXY": "127.0.0.1",
    }
    with mock.patch.dict(os.environ, settings):
        os.environ.pop("AWS_PROFILE", None)
        yield


def bucket_level(requests: list[dict[str, str]]) -> list[str]:
    """The requests that are not a call on the registry key: HeadBucket, ListObjectsV2 and anything else on the bucket."""
    return [f"{request['method']} {request['path']}?{request['query']}" for request in requests if request["path"] != f"/{REGISTRY_BUCKET}/{REGISTRY_KEY}"]


def case_fa19(c: Checker) -> None:
    """FA19: the registry store of the project reads and writes the one registry object with GetObject and PutObject only (no ListBucket needed)."""
    real = importlib.util.find_spec("feast") is not None
    client = "Feast" if real else "the two Feast modules as stand-ins"
    settings = (feast_job.REPO / "feature_store_aws.yaml").read_text()
    c.check("feature_store_aws.yaml names the project registry store", f"registry_store_type: {REGISTRY_STORE_TYPE}\n" in settings)
    with s3_server() as server, s3_client_environment(server), registry_store_module(real) as module:
        config = SimpleNamespace(path=REGISTRY_URI, s3_additional_kwargs=None)
        if real:
            config = importlib.import_module("feast.repo_config").RegistryConfig(
                registry_type="file", path=REGISTRY_URI, registry_store_type=REGISTRY_STORE_TYPE
            )
        store = module.S3ObjectRegistryStore(config, Path("."))
        c.check(f"the store is a Feast RegistryStore ({client})", any(base.__name__ == "RegistryStore" for base in type(store).mro()))

        def outcome() -> str:
            try:
                store.get_registry_proto()
            except FileNotFoundError as error:
                return f"missing {error}"
            except Exception as error:
                return type(error).__name__
            return "read"

        with mock.patch.dict(os.environ):
            os.environ.pop(REGISTRY_BOOTSTRAP[0], None)
            server.allow_put = False
            read_without_list = outcome()
            c.check("a reader's denied read of a missing object stays an access error", read_without_list == "ClientError")
            c.check("a reader never writes the registry", server.conditions == [] and server.stored is None)
            server.allow_put = True
        with mock.patch.dict(os.environ, dict([REGISTRY_BOOTSTRAP])):
            server.drop_put = True
            dropped = outcome()
            server.drop_put = False
            c.check("a conditional create that fails in transport leaves the read's access error", dropped == "ClientError")
            c.check("the dropped create left no object", server.stored is None)
            server.conditions.clear()
            missing = outcome()
            c.check(
                "the writer's denied read of a missing object is a FileNotFoundError that names the URI",
                missing.startswith("missing ") and REGISTRY_URI in missing,
            )
            c.check("the writer shows the object missing by one conditional create", server.conditions == ["*"] and server.stored is not None)
            created = server.stored
            server.refuse_object = True
            c.check("a refused read of an existing object stays an access error for the writer", outcome() == "ClientError")
            c.check("the conditional create never replaces an existing object", server.conditions == ["*", "*"] and server.stored == created)
            server.refuse_object = False
        proto = module.RegistryProto()
        store.update_registry_proto(proto)
        c.check("a write stamps a version ID", bool(proto.version_id))
        c.check("a write puts the object unconditionally", server.conditions == ["*", "*", ""] and server.stored is not None)
        again = store.get_registry_proto()
        c.check("a read returns the object that was written", again.version_id == proto.version_id)
        store.teardown()
        c.check("the teardown deletes the object", server.stored is None and server.requests[-1]["method"] == "DELETE")
        c.check("no request reached the bucket itself or another key", bucket_level(server.requests) == [])
        methods = sorted({request["method"] for request in server.requests})
        c.check("the store used GET, PUT and DELETE on the registry key only", methods == ["DELETE", "GET", "PUT"])
    if real:
        registry = importlib.import_module("feast.infra.registry.registry")
        repo_config = importlib.import_module("feast.repo_config")
        errors = importlib.import_module("feast.errors")
        with s3_server() as server, s3_client_environment(server), tempfile.TemporaryDirectory(prefix="feast_registry_") as raw:
            stock = repo_config.RegistryConfig(registry_type="file", path=REGISTRY_URI)
            try:
                registry.Registry("hai_prediction", stock, Path(raw))
                stock_outcome = "created"
            except errors.S3RegistryBucketForbiddenAccess:
                stock_outcome = "S3RegistryBucketForbiddenAccess"
            except Exception as error:
                stock_outcome = type(error).__name__
            c.check("the stock S3 registry store fails on HeadBucket without ListBucket", stock_outcome == "S3RegistryBucketForbiddenAccess")
            c.check("the stock store's first call is HeadBucket", server.requests[:1] == [{"method": "HEAD", "path": f"/{REGISTRY_BUCKET}", "query": ""}])
        with (
            s3_server() as server,
            s3_client_environment(server),
            tempfile.TemporaryDirectory(prefix="feast_registry_") as raw,
            mock.patch.dict(os.environ, dict([REGISTRY_BOOTSTRAP])),
        ):
            project = repo_config.RegistryConfig(registry_type="file", path=REGISTRY_URI, registry_store_type=REGISTRY_STORE_TYPE, cache_ttl_seconds=0)
            first = registry.Registry("hai_prediction", project, Path(raw))
            c.check("a first Feast registry on the project store creates the object", server.stored is not None)
            second = registry.Registry("hai_prediction", project, Path(raw))
            c.check("a second Feast registry reads the stamped object back", bool(second.cached_registry_proto.version_id) and first is not second)
            c.check("Feast never reached the bucket itself or another key", bucket_level(server.requests) == [])


def case_fa20(c: Checker) -> None:
    """FA20: a catalog client that is still running at its deadline fails the case and is then stopped, so no worker outlives its case."""
    with rest_catalog_server() as server:

        def stalled() -> None:
            with contextlib.suppress(OSError), socket.create_connection(("127.0.0.1", server.server_port), timeout=60) as link:
                link.recv(1)

        def quick() -> None:
            return None

        slow = threading.Thread(target=stalled, daemon=True)
        slow.start()
        fast = threading.Thread(target=quick, daemon=True)
        fast.start()
        slow_checks, fast_checks = Checker(), Checker()
        stop_worker(slow_checks, "the stalled client", slow, server.abort_connections, 0.3)
        stop_worker(fast_checks, "the quick client", fast, server.abort_connections, 0.3)
        c.check("a client past its deadline records a failed deadline check", slow_checks.results[0] == ("the stalled client ends within 0.3 s", False))
        c.check(
            "the stalled client is stopped before the case goes on", slow_checks.results[1] == ("the stalled client is stopped", True) and not slow.is_alive()
        )
        c.check(
            "a client that ends in time passes both checks",
            fast_checks.results == [("the quick client ends within 0.3 s", True), ("the quick client is stopped", True)],
        )


class ReaderFeatureStore:
    """Feast's ``FeatureStore`` as far as its registry: it reads the settings file, fills its variables from the environment and opens the registry store
    the file selects, as Feast 0.67.0 does before any feature read.

    ``registry_store_type`` selects that class. Without it an ``s3://`` path selects Feast's ``S3RegistryStore``, whose first call is HeadBucket
    (``feast/infra/registry/s3.py``); this stand-in makes that call and stops with ``S3RegistryBucketForbiddenAccess`` when it is refused.
    """

    def __init__(self, repo_path: str, fs_yaml_file: Path | None = None) -> None:
        self.settings_file = Path(fs_yaml_file) if fs_yaml_file is not None else Path(repo_path) / "feature_store.yaml"
        text = self.settings_file.read_text()
        for name in feast_aws.YAML_VARIABLES:
            text = text.replace("${" + name + "}", os.environ.get(name, ""))
        registry = importlib.import_module("yaml").safe_load(text)["registry"]
        path = str(registry["path"])
        store_type = registry.get("registry_store_type")
        if store_type is not None:
            if store_type != "scripts.features.feast_registry_store.S3ObjectRegistryStore":
                raise RuntimeError(f"the stand-in knows only the project's registry store, not {store_type}")
            store_class = importlib.import_module("scripts.features.feast_registry_store").S3ObjectRegistryStore
            store = store_class(SimpleNamespace(path=path, s3_additional_kwargs=None), Path(repo_path))
            self.registry = store.get_registry_proto()
            return
        parts = urllib.parse.urlsplit(path)
        botocore_exceptions = importlib.import_module("botocore.exceptions")
        try:
            importlib.import_module("boto3").client("s3").head_bucket(Bucket=parts.hostname)
        except botocore_exceptions.ClientError as error:
            raise RuntimeError(f"S3RegistryBucketForbiddenAccess: {parts.hostname}") from error
        raise RuntimeError("the stand-in stops after HeadBucket; the stock store was selected")


@contextlib.contextmanager
def reader_registry() -> Iterator[Any]:
    """A registry bucket that holds a registry object for a role with GetObject on its key only (no ListBucket, no PutObject), and Feast as a stand-in."""
    stored = RegistryProtoStandIn()
    stored.version_id = "existing-registry"
    stored.last_updated.moment = "2026-11-07T00:00:00+00:00"
    with s3_server() as server, s3_client_environment(server), registry_store_module(False):
        server.stored = stored.SerializeToString()
        server.allow_put = False
        with mock.patch.dict(sys.modules, {"feast": fake_module("feast", FeatureStore=ReaderFeatureStore)}):
            yield server


def opened_reader(open_store: Callable[[], Any]) -> tuple[Any, str]:
    """Open a reader's Feast store against the registry stand-in: the store, or ``None`` with the error that stopped it."""
    try:
        return open_store(), ""
    except Exception as error:
        return None, f"{type(error).__name__}: {error}"


def case_fa21(c: Checker) -> None:
    """FA21: the training extract reads the S3 registry through the project registry store, so its role needs GetObject on the registry key only."""
    historical = importlib.import_module("scripts.features.historical")
    training_extract = importlib.import_module("scripts.features.training_extract")
    with tempfile.TemporaryDirectory(prefix="feast_reader_") as raw, reader_registry() as server:
        environ = training_extract.aws_reader_environment(REGISTRY_BUCKET, raw, "real", region="us-east-2")
        c.check("the training extract names the real registry key", environ["FEAST_REGISTRY_PATH"] == REGISTRY_URI)
        # The two Redis values the extract's reader sets before it opens the store (training_extract.live_reader).
        environ.update({"FEAST_REDIS_HOST": "localhost", "REDIS_PASSWORD": "offline-read-only"})
        with mock.patch.dict(os.environ, environ):
            store, error = opened_reader(lambda: historical.default_store(os.environ))
        c.check(f"the extract's store opens without HeadBucket ({error or 'opened'})", error == "")
        c.check("the extract reads the AWS settings file", store is not None and store.settings_file == feast_aws.AWS_SETTINGS_FILE)
        c.check("the extract read the existing registry", store is not None and store.registry.version_id == "existing-registry")
        c.check("the extract made no call on the bucket itself or another key", bucket_level(server.requests) == [])
        c.check("the extract wrote nothing", server.conditions == [])
    with mock.patch.dict(os.environ, {"FEAST_REDIS_HOST": "localhost", "REDIS_PASSWORD": "offline-read-only", "FEAST_REGISTRY_PATH": "registry.pb"}):
        os.environ.pop("FEAST_PROFILE", None)
        with mock.patch.dict(sys.modules, {"feast": fake_module("feast", FeatureStore=SimpleNamespace)}):
            local = historical.default_store(os.environ)
        c.check("without the aws profile the store reads the local settings file", "fs_yaml_file" not in vars(local))


def case_fa22(c: Checker) -> None:
    """FA22: the model API reads the S3 registry through the project registry store, so its role needs GetObject on the registry key only."""
    main = importlib.import_module("scripts.serving.main")
    with reader_registry() as server:
        environ = {"FEAST_REGISTRY_PATH": REGISTRY_URI, "FEAST_REDIS_HOST": "10.0.0.5", "REDIS_PASSWORD": "model-api-case"}
        with mock.patch.dict(os.environ, environ):
            store, error = opened_reader(main.feature_store)
        c.check(f"the model API's store opens without HeadBucket ({error or 'opened'})", error == "")
        c.check("the model API reads the AWS settings file", store is not None and store.settings_file == feast_aws.AWS_SETTINGS_FILE)
        c.check("the model API read the existing registry", store is not None and store.registry.version_id == "existing-registry")
        c.check("the model API made no call on the bucket itself or another key", bucket_level(server.requests) == [])
        c.check("the model API wrote nothing", server.conditions == [])
    dockerfile = (feast_job.REPO.parent / "services/model_api/Dockerfile").read_text()
    c.check("the model API image holds the registry store module", "scripts/features/feast_registry_store.py" in dockerfile)


def case_fa23(c: Checker) -> None:
    """FA23: the training extract and the drift current window read features through Feast's offline reader, which opens its own PyIceberg catalog hai
    from the address and the token only. Their reader environment must turn off credential vending (the AWS catalog cannot vend) and name the region
    (otherwise PyIceberg's S3 file reader asks the bucket for its region) before PyIceberg first loads."""
    training_extract = importlib.import_module("scripts.features.training_extract")
    with tempfile.TemporaryDirectory(prefix="feast_reader_env_") as raw:
        for mode in ("real", "publish_fixture"):
            environ = training_extract.aws_reader_environment(REGISTRY_BUCKET, raw, mode, region="us-east-2")
            c.check(f"{mode}: the training reader turns off credential vending", environ.get(feast_aws.NO_DELEGATION_VARIABLE) == "")
            c.check(f"{mode}: the training reader names the region for PyIceberg", environ.get(PYICEBERG_REGION_VARIABLE) == "us-east-2")
        for region in ("", "us east"):
            try:
                training_extract.aws_reader_environment(REGISTRY_BUCKET, raw, "real", region=region)
                refused = False
            except training_extract.ExtractError:
                refused = True
            c.check(f"the region {region!r} is refused", refused)
        drift = importlib.import_module("monitoring.aws_run")
        with mock.patch.dict(os.environ, {"AWS_REGION": "us-east-2"}):
            drift.prepare_feature_reader(REGISTRY_BUCKET, Path(raw))
            delegation, named = os.environ.get(feast_aws.NO_DELEGATION_VARIABLE), os.environ.get(PYICEBERG_REGION_VARIABLE)
        c.check("the drift current window turns off credential vending", delegation == "")
        c.check("the drift current window names the task's region for PyIceberg", named == "us-east-2")
        with mock.patch.dict(os.environ):
            os.environ.pop("AWS_REGION", None)
            try:
                drift.prepare_feature_reader(REGISTRY_BUCKET, Path(raw))
                refused = False
            except drift.CheckError:
                refused = True
        c.check("the drift current window refuses to start without AWS_REGION", refused)


# ----------------------------------------------------------------------------------------------------
# The deployed entry points: ``feast_job.main`` and ``feast_materialize.main`` on the AWS profile, as the features job's feast container runs them, with no
# injected loader, session or hook. Only the outside world is a stand-in: the secret store (``open_ssm``), the token exchange, the catalog
# (``open_catalog``), the redis and feast modules, the Feast store (``historical.default_store``), the Feast side of a set load (``SetHooks``) and the data
# bucket that holds the Production records (``promote_receipt.s3_client``).

SET_BASE_RUN = "20261109T010000Z-0000b001"
SET_RUN = "20261109T020000Z-0000a001"
SET_KEYS = [f"AAA00{number}:2022" for number in range(1, 4)]
FEATURE_BUCKET = "example-feature-bucket"
# The fixed variables and the secrets that infra/jobs/layers.tf gives the feast container of the features job (layers.tftest.hcl checks the same list).
DEPLOYED_FEAST_FIXED = {
    "FEAST_PROFILE": "aws",
    "FEAST_REGISTRY_BUCKET": FEATURE_BUCKET,
    "AWS_REGION": "us-east-2",
}
DEPLOYED_FEAST_SECRETS = {"POLARIS_CLIENT_ID": "reader-id-1", "POLARIS_CLIENT_SECRET": SENTINEL}


class ProductionBucket(FakeLaunchBucket):
    """The data bucket as the features task role sees it (infra/iam job_writer_features_feast.json): list and read under ``promote/production/``, and list
    (never read) the lease records of one model under ``promote/models/<model>/``.

    ``production`` maps an infection type to the Production records the promote action wrote for it, oldest generation first. ``leases`` maps an infection
    type to the state of each lease generation of its model (True closed, False open), oldest first. A type without an entry in ``leases`` gets one closed
    generation per record, as the promote action leaves it; a record whose tags are None is not written (a generation that closed without a record)."""

    LEASE_LIST = re.compile(r"promote/models/[A-Za-z][A-Za-z0-9_]*/")

    def __init__(self, production: Mapping[str, list[Mapping[str, Any] | None]] | None = None, leases: Mapping[str, list[bool]] | None = None) -> None:
        super().__init__(FEATURE_BUCKET, "promote/")
        names = json.loads(feature_sets_module().MODEL_CONTRACT.read_text())["registered_models"]
        for hai_type, states in {**{hai_type: [True] * len(records) for hai_type, records in (production or {}).items()}, **(leases or {})}.items():
            for generation, closed in enumerate(states, start=1):
                lease = {"schema": 1, "model": names[hai_type], "generation": generation, "run_id": f"20261109T00000{generation}Z-0000c00{generation}"}
                self.objects[f"promote/models/{names[hai_type]}/lease-{generation:010d}.json"] = json.dumps(lease).encode()
                if closed:
                    self.objects[f"promote/models/{names[hai_type]}/lease-{generation:010d}.done.json"] = json.dumps({**lease, "outcome": "completed"}).encode()
        for hai_type, records in (production or {}).items():
            for generation, tags in enumerate(records, start=1):
                if tags is None:
                    continue
                record = {
                    "schema": 1,
                    "hai_type": hai_type,
                    "model": names[hai_type],
                    "version": str(generation),
                    "run_id": f"20261109T00000{generation}Z-0000c00{generation}",
                    "mode": "promote",
                    "generation": generation,
                    "feature_set": tags.get("feature_set"),
                    "feature_set_schema_sha256": tags.get("feature_set_schema_sha256"),
                    "promoted_at": "2026-11-09T00:00:00+00:00",
                }
                self.objects[f"promote/production/{hai_type}/{generation:010d}.json"] = json.dumps(record).encode()

    def _enter(self, operation: str, arguments: dict[str, Any], key: str) -> None:
        super()._enter(operation, arguments, key)
        granted = key.startswith("promote/production/") or (operation == "list_objects_v2" and self.LEASE_LIST.fullmatch(key) is not None)
        if not granted:
            raise client_error("AccessDenied", operation)

    def list_objects_v2(self, **arguments: Any) -> dict[str, Any]:
        self._enter("list_objects_v2", arguments, str(arguments.get("Prefix", "")))
        keys = sorted(key for key in self.objects if key.startswith(str(arguments["Prefix"])))
        return {"Contents": [{"Key": key} for key in keys], "KeyCount": len(keys), "IsTruncated": False}

    def reads(self) -> list[str]:
        """The prefix or key of every call, in order."""
        return [str(arguments.get("Key", arguments.get("Prefix", ""))) for _, arguments in self.calls]


class Deployed:
    """The stand-in world of one deployed run: the catalog of a base run and a model set run, the fake Redis, the secret store and the registry."""

    def __init__(self, folder: Path, bucket: ProductionBucket) -> None:
        self.folder, self.bucket = folder, bucket
        self.catalog, self.schema, _, _ = feast_cases.set_catalog("m1")
        entry = self.schema["model_sets"]["m1"]
        self.catalog.tables[entry["table"]].rows = [{"spine_key": key, "event_timestamp": online_cases.T0} for key in SET_KEYS]
        self.guarded = GuardedCatalog(self.catalog)
        self.server = online_cases.FakeServer()
        self.server.phrase = STORE_VALUE
        self.store = StoreStandIn()
        self.settings_files: list[Path] = []
        self.loaded: list[tuple[str, int]] = []
        feast_cases.set_folder(folder, SET_BASE_RUN)

    def redis_module(self) -> Any:
        server = self.server

        def connect(**options: Any) -> Any:
            return server.client(int(options["db"]), password=str(options.get("password", "")))

        return fake_module("redis", Redis=connect)

    def hooks_class(self) -> Any:
        world = self

        class SetHooksStandIn:
            """``feast_sets.SetHooks``: one hash per entity of the set's table into the set's own database, and the settings file of the Feast store."""

            def __init__(self, catalog: Any, publication: reads.Publication, entry: Mapping[str, Any], store: Any) -> None:
                self.catalog, self.publication, self.entry = catalog, publication, entry
                world.settings_files.append(store.settings_file)

            def verify_heads(self, moment: str) -> None:
                reads.verify_heads(self.catalog, self.publication, moment)

            def materialize(self, start: Any, end: Any) -> None:
                database = online.set_layout("m1").feature_db
                for key in SET_KEYS:
                    world.server.write_hash(database, key.encode(), {"value": key})
                world.loaded.append((self.publication.run_id, database))

            def parse_key(self, key: bytes) -> bool:
                return key.decode().count(":") == 1

            def verify_samples(self) -> None:
                return None

        return SetHooksStandIn

    @contextlib.contextmanager
    def running(self, environment: Mapping[str, str]) -> Iterator[None]:
        """The process environment of the container and every stand-in, for the length of the block."""
        feast_sets = importlib.import_module("scripts.features.feast_sets")
        historical = importlib.import_module("scripts.features.historical")
        real_version = importlib.metadata.version
        modules = {
            "redis": self.redis_module(),
            "redis.retry": fake_module("redis.retry", Retry=lambda backoff, retries: None),
            "redis.backoff": fake_module("redis.backoff", NoBackoff=lambda: None),
            "feast.infra.online_stores.redis": fake_module("feast.infra.online_stores.redis", RedisOnlineStore=type("RedisOnlineStore", (), {})),
            "mlflow": None,  # the feature store image has no MLflow client: an import fails
        }
        kept = {name: os.environ[name] for name in ("PATH", "HOME", "TMPDIR") if name in os.environ}
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.dict(os.environ, {**kept, **environment}, clear=True))
            stack.enter_context(mock.patch.dict(sys.modules, modules))
            stack.enter_context(mock.patch.object(feast_aws, "open_ssm", lambda region: self.store))
            stack.enter_context(mock.patch.object(reads, "polaris_token", exchange_stand_in()))
            stack.enter_context(mock.patch.object(reads, "open_catalog", lambda settings, loader=None: self.guarded))
            stack.enter_context(mock.patch.object(feast_sets, "SetHooks", self.hooks_class()))
            stack.enter_context(mock.patch.object(promote_receipt, "s3_client", lambda: self.bucket))
            stack.enter_context(mock.patch.object(historical, "default_store", lambda environ, settings_file: SimpleNamespace(settings_file=settings_file)))
            stack.enter_context(mock.patch("importlib.metadata.version", lambda name: "0.67.0" if name == "feast" else real_version(name)))
            yield


def deployed_environment(folder: Path, **changes: str | None) -> dict[str, str]:
    """The feast container's environment of a model set run: the variables of ``aws_environment`` with the run folder, the set and the data bucket."""
    environment = aws_environment(FEAST_RUN_FOLDER=str(folder), FEATURE_SET="m1", FEAST_REGISTRY_BUCKET=FEATURE_BUCKET)
    for name, value in changes.items():
        if value is None:
            environment.pop(name, None)
        else:
            environment[name] = value
    return environment


def entry_point(call: Callable[[], int]) -> tuple[int, str, str]:
    """Run a ``main`` and return its exit code with what it wrote to stdout and stderr."""
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = call()
        except SystemExit as error:
            code = int(error.code or 0)
    return code, out.getvalue(), err.getvalue()


def case_fa24(c: Checker) -> None:
    """FA24: the Feast phase of an online model set (m1) through ``feast_job materialize`` on the AWS profile loads the set through the AWS session (the secret
    store's password, the AWS catalog) and the AWS settings file, into the set's own database."""
    bucket = ProductionBucket()
    with tempfile.TemporaryDirectory(prefix="feast_aws_deployed_") as raw:
        world = Deployed(Path(raw), bucket)
        with world.running(deployed_environment(world.folder)):
            code, out, err = entry_point(lambda: feast_job.main(["materialize", "--run", SET_RUN, "--mode", "publish_fixture"]))
        record = json.loads((world.folder / "feast_materialize.json").read_text()) if (world.folder / "feast_materialize.json").is_file() else {}
        c.check("the step exits 0", code == 0)
        c.check("no failure was written", err == "")
        c.check("the record says the set was loaded online", record.get("online") == "load" and bool(record.get("load_id")))
        c.check(
            "the lease records of every model and the Production records of every infection type were listed",
            sorted(bucket.reads())
            == sorted([f"promote/models/hai_model_HAI_{n}/" for n in range(1, 7)] + [f"promote/production/HAI_{n}/" for n in range(1, 7)]),
        )
        c.check("the load ran once, for the set run, into the set's own database", world.loaded == [(SET_RUN, online.set_layout("m1").feature_db)])
        c.check("the load opened the Feast store with the AWS settings file", world.settings_files == [feast_aws.AWS_SETTINGS_FILE])
        c.check("the load read the AWS catalog (the stand-in of open_catalog)", f"features_e2e.{reads.RUNS_TABLE}" in world.guarded.loads)
        c.check("the Redis password came from the secret store once", world.store.asked == [feast_aws.redis_secret_name()])
        c.check("the set's database holds one key per entity", len(world.server.database_keys(online.set_layout("m1").feature_db)) == len(SET_KEYS))
        c.check("no failure text holds the reader secret", SENTINEL not in out + err)


def case_fa25(c: Checker) -> None:
    """FA25: the deployed feast container decides load or hold from the Production records in the data bucket, with no MLflow address, login or client.
    Its environment is built as the task gets it: the variables of the job table filled by the override generator plus the fixed variables and secrets of
    layers.tf. An online set whose newest Production record of a type names another schema holds its load; an older record of that type does not count."""
    job_overrides = importlib.import_module("scripts.orchestration.job_overrides")
    values = {
        "RUN_ID": SET_RUN,
        "CODE_RUN_ID": SET_RUN,
        "CODE_SHA256": "ab" * 32,
        "POLARIS_URI": PORT_OF_NOTHING,
        "INPUT_RUN_ID": SET_BASE_RUN,
        "FEATURE_SET": "m1",
    }
    document = job_overrides.build("features", values)
    variables = {item["name"]: item["value"] for entry in document["containerOverrides"] if entry["name"] == "feast" for item in entry["environment"]}
    jobs = json.loads((feast_job.REPO.parent / "config/orchestration/ecs_broker_jobs.json").read_text())
    c.check(
        "the job table gives the feast container no MLflow variable",
        not any(name.startswith("MLFLOW") for name in variables) and "MLFLOW" not in json.dumps(jobs["jobs"]["features"]),
    )
    owner_lists = json.loads((feast_job.REPO.parent / "config/aws/owner_command_lists.json").read_text())
    c.check("the owner command lists give the features overrides no MLflow address", "--mlflow-uri" not in owner_lists["Features after gold"])
    group = tomllib.loads((feast_job.REPO.parent / "pyproject.toml").read_text())["dependency-groups"]["feature_store"]
    c.check("the feature store image pins no MLflow client", not any(str(line).startswith("mlflow") for line in group))
    runtime = json.loads((feast_job.REPO.parent / "config/quality/dependency_runtimes.json").read_text())["runtimes"]["feature_store"]["code"]
    c.check(
        "the dependency check reads no MLflow registry reader as feature store code",
        not {"scripts/training/registry.py", "scripts/training/mlflow_alias_read.py", "scripts/training/run_tracking.py"} & set(runtime),
    )
    schema, _ = reads.read_schema()
    mine = importlib.import_module("scripts.features.feature_schema").set_schema_sha256("m1", schema=schema)
    same = {"feature_set": "m1", "feature_set_schema_sha256": mine}
    other = {"feature_set": "m1", "feature_set_schema_sha256": "0" * 64}
    bucket = ProductionBucket({"HAI_1": [same, other], "HAI_2": [other, same], "HAI_3": [{"feature_set": "m2", "feature_set_schema_sha256": "0" * 64}]})
    with tempfile.TemporaryDirectory(prefix="feast_aws_deployed_") as raw:
        world = Deployed(Path(raw), bucket)
        environment = {**DEPLOYED_FEAST_FIXED, **DEPLOYED_FEAST_SECRETS, **variables, "FEAST_RUN_FOLDER": str(world.folder)}
        with world.running(environment):
            code, out, err = entry_point(lambda: feast_job.main(["materialize", "--run", SET_RUN, "--mode", "publish_fixture"]))
        record = json.loads((world.folder / "feast_materialize.json").read_text()) if (world.folder / "feast_materialize.json").is_file() else {}
        hold_path = world.folder / "online_hold.json"
        hold = json.loads(hold_path.read_text()) if hold_path.is_file() else {}
        c.check("the step exits 0", code == 0)
        c.check("no failure was written", err == "")
        c.check("the newest record of HAI_1 (another schema) holds the load and nothing is loaded", record.get("online") == "hold" and world.loaded == [])
        c.check("the hold names the Production schemas of the set (HAI_1 and HAI_2)", hold.get("production_set_schema_sha256") == sorted([mine, "0" * 64]))
        c.check(
            "every read stayed under promote/production/ or was a list of one model's lease records",
            all(key.startswith("promote/production/") or ProductionBucket.LEASE_LIST.fullmatch(key) for key in bucket.reads()),
        )
        c.check(
            "only the newest record of each type was read",
            sorted(key for key in bucket.reads() if key.endswith(".json"))
            == ["promote/production/HAI_1/0000000002.json", "promote/production/HAI_2/0000000002.json", "promote/production/HAI_3/0000000001.json"],
        )
        c.check("no output holds the reader secret", SENTINEL not in out + err)
    damaged = ProductionBucket({"HAI_1": [same]})
    damaged.objects["promote/production/HAI_1/latest.json"] = b"{}"
    with tempfile.TemporaryDirectory(prefix="feast_aws_deployed_") as raw:
        world = Deployed(Path(raw), damaged)
        environment = {**DEPLOYED_FEAST_FIXED, **DEPLOYED_FEAST_SECRETS, **variables, "FEAST_RUN_FOLDER": str(world.folder)}
        with world.running(environment):
            code, _, err = entry_point(lambda: feast_job.main(["materialize", "--run", SET_RUN, "--mode", "publish_fixture"]))
        c.check("a key the promote action does not write fails the step by name", code == 1 and "latest.json" in err and world.loaded == [])


def case_fa26(c: Checker) -> None:
    """FA26: ``feast_materialize --release m1 <RUN_ID>`` on the AWS profile opens one catalog session, releases the held run through it and the AWS settings
    file, and records the release."""
    schema, _ = reads.read_schema()
    mine = importlib.import_module("scripts.features.feature_schema").set_schema_sha256("m1", schema=schema)
    with tempfile.TemporaryDirectory(prefix="feast_aws_release_") as raw:
        world = Deployed(Path(raw), ProductionBucket())
        hold = {"schema": 1, "feature_set": "m1", "run_id": SET_RUN, "base_run": SET_BASE_RUN, "set_schema_sha256": mine, "production_set_schema_sha256": []}
        (world.folder / "online_hold.json").write_text(json.dumps(hold))
        with world.running(deployed_environment(world.folder, FEATURE_SET=None)):
            code, out, err = entry_point(lambda: feast_materialize.main(["--release", "m1", SET_RUN, "--mode", "publish_fixture"]))
        released = world.folder / "online_release.json"
        record = json.loads(released.read_text()) if released.is_file() else {}
        c.check("the release exits 0", code == 0)
        c.check("no failure was written", err == "")
        c.check("the release record names the set, the run and a load", (record.get("feature_set"), record.get("run_id")) == ("m1", SET_RUN))
        c.check("the held run was loaded once into the set's own database", world.loaded == [(SET_RUN, online.set_layout("m1").feature_db)])
        c.check("one session: the secret store was asked once", world.store.asked == [feast_aws.redis_secret_name()])
        c.check("the load opened the Feast store with the AWS settings file", world.settings_files == [feast_aws.AWS_SETTINGS_FILE])
        c.check("no output holds the reader secret", SENTINEL not in out + err)


def case_fa28(c: Checker) -> None:
    """FA28: ``feast_materialize --release m1 <RUN_ID>`` on the AWS profile is refused, by name, while a Production model of m1 is on another schema than the
    held run (owner decision "Shared online set release", Nov 10 2026): nothing is loaded, no release is recorded and the hold stays. Once every Production
    model of the set is on the run's schema, the same command releases the run."""
    schema, _ = reads.read_schema()
    mine = importlib.import_module("scripts.features.feature_schema").set_schema_sha256("m1", schema=schema)
    same = {"feature_set": "m1", "feature_set_schema_sha256": mine}
    other = {"feature_set": "m1", "feature_set_schema_sha256": "0" * 64}
    hold = {
        "schema": 1,
        "feature_set": "m1",
        "run_id": SET_RUN,
        "base_run": SET_BASE_RUN,
        "set_schema_sha256": mine,
        "production_set_schema_sha256": ["0" * 64],
    }
    with tempfile.TemporaryDirectory(prefix="feast_aws_release_guard_") as raw:
        world = Deployed(
            Path(raw), ProductionBucket({"HAI_1": [same], "HAI_4": [other], "HAI_5": [{"feature_set": "m2", "feature_set_schema_sha256": "0" * 64}]})
        )
        (world.folder / "online_hold.json").write_text(json.dumps(hold))
        with world.running(deployed_environment(world.folder, FEATURE_SET=None)):
            code, out, err = entry_point(lambda: feast_materialize.main(["--release", "m1", SET_RUN, "--mode", "publish_fixture"]))
        c.check("the release is refused (exit 1)", code == 1)
        c.check("the refusal names the set and the infection type on another schema", "m1" in err and "HAI_4" in err and "HAI_1" not in err.split("HAI_4")[0])
        c.check("the refusal leaves a model of another set out", "HAI_5" not in err)
        c.check("nothing was loaded and no release was recorded", world.loaded == [] and not (world.folder / "online_release.json").is_file())
        c.check("the hold stays", json.loads((world.folder / "online_hold.json").read_text()) == hold)
        c.check("no output holds the reader secret", SENTINEL not in out + err)
        world.bucket = ProductionBucket({"HAI_1": [same], "HAI_4": [other, same]})
        with world.running(deployed_environment(world.folder, FEATURE_SET=None)):
            code, _, err = entry_point(lambda: feast_materialize.main(["--release", "m1", SET_RUN, "--mode", "publish_fixture"]))
        c.check("once every Production model of the set is on the run's schema the release loads the run", code == 0 and err == "")
        c.check("the held run was loaded once into the set's own database", world.loaded == [(SET_RUN, online.set_layout("m1").feature_db)])


def case_fa27(c: Checker) -> None:
    """FA27: ``FEATURE_SET=all``, the value the broker and the override generator give a run of the single path, reaches ``feast_job`` from the environment
    (the layer driver passes no flag for it) and runs the single path on the AWS profile."""
    schema = feast_cases.synthetic_schema()
    catalog, _ = feast_cases.stub_features(schema)
    with tempfile.TemporaryDirectory(prefix="feast_aws_all_") as raw:
        world = Deployed(Path(raw), ProductionBucket())
        world.guarded = GuardedCatalog(catalog)
        log: list[str] = []
        environment = aws_environment(FEAST_RUN_FOLDER=raw, FEATURE_SET=feature_sets_module().SINGLE_PATH)
        with world.running(environment):
            backend = PathBackend(schema, log, os.environ)
            with (
                mock.patch.object(reads, "read_schema", lambda *arguments, **options: (schema, "c" * 64)),
                mock.patch.object(feast_job, "FeastRepository", lambda settings_file=None: backend),
            ):
                code, out, err = entry_point(lambda: feast_job.main(["apply", "--run", RUN, "--mode", "publish_fixture"]))
        record = json.loads((Path(raw) / feast_job.RECORD_NAME).read_text()) if (Path(raw) / feast_job.RECORD_NAME).is_file() else {}
        c.check("the apply exits 0", code == 0)
        c.check("no refusal of the set value was written", "FEATURE_SET" not in err and err == "")
        c.check("the single path applied once and read the registry back", log.count("apply") == 1 and "read_back" in log)
        c.check("the record is the single path's (no feature set)", bool(record) and "feature_set" not in record)
        del out


def case_fa29(c: Checker) -> None:
    """FA29: the Feast phase and the release of an online set refuse, by name, while a promotion of any registered model is unresolved (owner decision
    "Features reader refuses an unresolved promotion", Nov 10 2026): a lease generation that is open, or a newest closed generation without its Production
    record. The promote action moves the alias before it writes the record, so either state can leave the newest record naming the schema production held
    before. A promotion that closed with its record, earlier abandoned generations included, is resolved. With no promotion of any model yet, the set loads
    and a held run releases (the first release). The task role lists the lease prefix of each model and reads no lease record."""
    schema, _ = reads.read_schema()
    mine = importlib.import_module("scripts.features.feature_schema").set_schema_sha256("m1", schema=schema)
    same = {"feature_set": "m1", "feature_set_schema_sha256": mine}
    elsewhere = {"feature_set": "m2", "feature_set_schema_sha256": "0" * 64}
    hold = {"schema": 1, "feature_set": "m1", "run_id": SET_RUN, "base_run": SET_BASE_RUN, "set_schema_sha256": mine, "production_set_schema_sha256": []}

    def attempt(bucket: ProductionBucket, step: str) -> tuple[int, str, list[Any], bool]:
        """Run the step and return its exit code, its stderr, the loads and whether it wrote its record (the release, or the hold of a materialize)."""
        with tempfile.TemporaryDirectory(prefix="feast_aws_unresolved_") as raw:
            world = Deployed(Path(raw), bucket)
            if step == "release":
                (world.folder / "online_hold.json").write_text(json.dumps(hold))
                with world.running(deployed_environment(world.folder, FEATURE_SET=None)):
                    code, out, err = entry_point(lambda: feast_materialize.main(["--release", "m1", SET_RUN, "--mode", "publish_fixture"]))
                written = (world.folder / "online_release.json").is_file()
            else:
                with world.running(deployed_environment(world.folder)):
                    code, out, err = entry_point(lambda: feast_job.main(["materialize", "--run", SET_RUN, "--mode", "publish_fixture"]))
                written = (world.folder / "online_hold.json").is_file()
            c.check(f"{step}: no output holds the reader secret", SENTINEL not in out + err)
            return code, err, list(world.loaded), written

    refused = {
        "an open lease of a Production model of the set": (
            ProductionBucket({"HAI_1": [same]}, {"HAI_1": [True, False]}),
            ("hai_model_HAI_1", "generation 2", "open"),
        ),
        "an open first lease of a model of another set": (ProductionBucket({"HAI_1": [same]}, {"HAI_5": [False]}), ("hai_model_HAI_5", "generation 1", "open")),
        "a newest closed generation without its Production record": (
            ProductionBucket({"HAI_1": [same, None]}),
            ("hai_model_HAI_1", "generation 2", "no Production record"),
        ),
        "a Production record newer than every lease generation": (
            ProductionBucket({"HAI_1": [same, same]}, {"HAI_1": [True]}),
            ("HAI_1", "generation 2"),
        ),
    }
    for name, (bucket, words) in refused.items():
        for step in ("materialize", "release"):
            code, err, loaded, written = attempt(bucket, step)
            c.check(f"{step} with {name} is refused (exit 1) by name", code == 1 and all(word in err for word in words))
            c.check(f"{step} with {name} loads nothing and records no {'release' if step == 'release' else 'hold'}", loaded == [] and not written)
            c.check(f"{step} with {name} reads no lease record", not any(key.startswith("promote/models/") and key.endswith(".json") for key in bucket.reads()))
    allowed = {
        "a resolved promotion with its record, after an abandoned generation": ProductionBucket({"HAI_1": [same, None, same], "HAI_5": [elsewhere]}),
        "no promotion of any model yet (the first release)": ProductionBucket(),
    }
    for name, bucket in allowed.items():
        for step in ("materialize", "release"):
            code, err, loaded, _ = attempt(bucket, step)
            c.check(f"{step} with {name} loads the run once", code == 0 and err == "" and loaded == [(SET_RUN, online.set_layout("m1").feature_db)])


def feature_sets_module() -> Any:
    return importlib.import_module("scripts.features.feature_sets")


CASES: dict[str, tuple[Callable[[Checker], None], str]] = {
    "FA1": (case_fa1, "FA1"),
    "FA2": (case_fa2, "FA2"),
    "FA3": (case_fa3, "FA3"),
    "FA4": (case_fa4, "FA4"),
    "FA5": (case_fa5, "FA5"),
    "FA6": (case_fa6, "FA6"),
    "FA7": (case_fa7, "FA7"),
    "FA8": (case_fa8, "FA8"),
    "FA9": (case_fa9, "FA9"),
    "FA10": (case_fa10, "FA10"),
    "FA11": (case_fa11, "FA11"),
    "FA12": (case_fa12, "FA12"),
    "FA13": (case_fa13, "FA13"),
    "FA14": (case_fa14, "FA14"),
    "FA15": (case_fa15, "FA15"),
    "FA16": (case_fa16, "FA16"),
    "FA17": (case_fa17, "FA17"),
    "FA18": (case_fa18, "FA18"),
    "FA19": (case_fa19, "FA19"),
    "FA20": (case_fa20, "FA20"),
    "FA21": (case_fa21, "FA21"),
    "FA22": (case_fa22, "FA22"),
    "FA23": (case_fa23, "FA23"),
    "FA24": (case_fa24, "FA24"),
    "FA25": (case_fa25, "FA25"),
    "FA26": (case_fa26, "FA26"),
    "FA27": (case_fa27, "FA27"),
    "FA28": (case_fa28, "FA28"),
    "FA29": (case_fa29, "FA29"),
}
