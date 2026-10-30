"""Stand-in cases of the Feast AWS profile (failure modes FA1 to FA16 of the AWS profile package).

    .venv/bin/python -B -m scripts.features.feast_cases --part aws [--case FA6 ...]      # fake catalog, fake Redis, secret-store stand-in; no AWS, no Docker

The profile reads the Iceberg ``features`` namespace through the AWS catalog and writes the host Redis. These cases prove its rules against stand-ins only:
the catalog is a stub (or a loopback token endpoint), Redis is ``online_cases.FakeServer`` and the secret store is ``StoreStandIn`` below.
Nothing here proves the real AWS catalog, the real host Redis or the real Parameter Store.
The report lists case names, failure-mode IDs, check names and verdicts only.
"""

from __future__ import annotations

import builtins
import contextlib
import hashlib
import json
import tempfile
import threading
import time
from collections.abc import Callable, Mapping
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import ecs_target
from scripts.features import feast_aws, feast_cases, feast_job, feast_materialize, online_cases
from scripts.features import online_protocol as online
from scripts.features import publication_reads as reads
from scripts.features.feast_harness import Checker
from scripts.features.publication_reads import ReadError

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


def aws_environment(**changes: str | None) -> dict[str, str]:
    """The job environment of an AWS Feast job as the job definition would give it (synthetic values)."""
    environ: dict[str, str] = {
        "FEAST_PROFILE": "aws",
        "POLARIS_URI": PORT_OF_NOTHING,
        "POLARIS_CLIENT_ID": "reader-id-1",
        "POLARIS_CLIENT_SECRET": SENTINEL,
        "FEAST_REDIS_HOST": "redis.example.internal",
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
    """The Feast stand-in that keeps the registry path the job environment holds when ``apply`` runs, the path the real backend consumes."""

    def __init__(self, schema: dict[str, Any], log: list[str], environ: Mapping[str, str]) -> None:
        super().__init__(schema, log)
        self.environ = environ
        self.consumed = ""

    def apply(self) -> None:
        self.consumed = self.environ.get("FEAST_REGISTRY_PATH", "")
        super().apply()


def consumed_registry(mode: str) -> str:
    """Run a full AWS apply in ``mode`` and return the registry path the backend consumed."""
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
    return backend.consumed


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
    """FA8: the Redis host is a plain host name or IPv4 address."""
    for value in ("redis", "redis.example.internal", "10.0.12.7"):
        c.check(f"{value} is accepted", feast_aws.aws_settings(aws_environment(FEAST_REDIS_HOST=value), "real").redis_host == value)
    for value in ("", "redis:6379", "redis/db", "red is", "-x", "host\nname", "a" * 300):
        c.refused(f"the host {value[:12]!r} is refused", feast_aws.aws_settings, "FEAST_REDIS_HOST", aws_environment(FEAST_REDIS_HOST=value), "real")
    c.refused("an unset host is refused", feast_aws.aws_settings, "FEAST_REDIS_HOST", aws_environment(FEAST_REDIS_HOST=None), "real")


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
    c.check("a real apply hands the backend the real registry", consumed_registry("real") == f"s3://example-feature-bucket/{real_key}")
    c.check("a fixture apply hands the backend the fixture registry", consumed_registry("publish_fixture") == f"s3://example-feature-bucket/{fixture_key}")
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
    c.check("the AWS file holds the same stores and options as the local file", body(feast_aws.AWS_SETTINGS_FILE) == body(feast_job.SETTINGS_FILE))
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
}
