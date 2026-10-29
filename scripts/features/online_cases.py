"""Online cases of the feature store, lane C step 5 (design E20 to E26 and E33; E23 online against offline is the image part).

    .venv/bin/python -B -m scripts.features.online_cases --part fake                      # now: an in-process FAKE Redis, no Docker
    FEAST_REDIS_HOST=redis REDIS_PASSWORD=... python -B -m scripts.features.online_cases --part redis   # heavy lane: lane B's real service
    python -B -m scripts.features.online_cases --part image                               # heavy lane: Feast image plus the service (E23)

Lane A's ``run_feature_store_e2e.py`` has no ``online`` mode, so these cases live here; its runner can call ``CASES`` by name.

The same case functions run against two servers with one small interface (``client``, ``write_hash``, ``read_hash``, ``restart``, ``advance``, ``stall``,
``unstall``, ``set_config``, ``get_config``): ``FakeServer`` and ``RealServer``. **FakeServer is a fake, not Redis.** It models only the commands the protocol
uses (``SET NX PX``, ``HGETALL``, ``DBSIZE``, ``CONFIG GET``, ``SCAN``, ``EVAL``) and runs a Python translation of the three Lua sources, found by their exact
text. It cannot prove that ``SELECT`` inside a script leaves the caller's database unchanged (design F14, spike S1), real timeouts or the real error text; the
``redis`` part does. A restart is ``FLUSHALL`` on the real service (every database and the lease and marker vanish, as after a restart);
a stall is ``CLIENT PAUSE``.
Report: ``data/e2e/feast_cases/<UTC stamp>/report_<part>.json`` (case names, failure-mode IDs, check names and verdicts only).
"""

from __future__ import annotations

import argparse
import contextlib
import importlib
import importlib.metadata
import os
import sys
import tempfile
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from scripts.features import feast_cases, feast_job, feast_materialize, historical
from scripts.features import online_protocol as online
from scripts.features import publication_reads as reads
from scripts.features import serving_contract_client as serving
from scripts.features.feast_harness import Checker, fake_module, fake_redis_modules, run_cases, write_report
from scripts.features.publication_reads import ReadError

PHRASE = "pw-sentinel-5c1d9e"  # the fake server's password, also planted in exception text to prove the wrapper hides it
RUN_A = "20261022T120000Z-0a1b2c3d"
RUN_B = "20261022T130000Z-1a2b3c4d"
SCHEMA_HASH = "a" * 64
OTHER_HASH = "b" * 64
WORLD = {"feature_matrix_a": 11, "feature_matrix_b": 12}
SETS = reads.Publication("features_e2e", RUN_A, "x", WORLD).snapshot_set_sha256
KEYS = [f"AAA00{number}:2022" for number in range(1, 5)]
WINDOW_SECONDS = 1640995200  # 2022-01-01 00:00 UTC
REFERENCES = ["feature_matrix_hospital_window:bed_count", "feature_matrix_care_compare:c119__value__number"]
NO_WINDOW_MODEL = ["feature_matrix_care_compare:c119__value__number"]
T0 = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)

# The three Lua sources the fake translates, copied here on purpose: a change to online_protocol's scripts makes the fake refuse to run until the translation
# below is reviewed and these copies are updated, so the fake cannot pass for a script it does not model.
MODELED_BEGIN_LOAD = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[2])
redis.call('HSET', KEYS[2], 'state', 'loading', 'load_id', ARGV[2], 'owner_token', ARGV[1], unpack(ARGV, 3))
redis.call('SELECT', 1)
redis.call('FLUSHDB')
return 1
"""
MODELED_PUBLISH = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[2], 'load_id') ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[2], 'state', 'ready', unpack(ARGV, 3))
return 1
"""
MODELED_RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"""


# ---------------------------------------------------------------------------------------------------------------------------------------------
# Servers: a fake with the protocol's command semantics, and the real service.
# ---------------------------------------------------------------------------------------------------------------------------------------------


class FakeClient:
    def __init__(self, server: FakeServer, db: int, password: str, timeout: float, unreachable: bool) -> None:
        self.server, self.db, self.password, self.timeout, self.unreachable = server, db, password, timeout, unreachable

    def enter(self, command: str) -> None:
        server = self.server
        server.log.append(command)
        if self.unreachable:
            raise ConnectionError("connection refused")
        if self.password != server.phrase:
            raise PermissionError(f"invalid password {self.password}")  # the real message never holds it; the fake does, to test the wrapper
        if server.stalled:
            raise TimeoutError("timed out")
        hook, server.before = server.before, None
        try:
            if hook is not None:
                hook(command)
        finally:
            server.before = hook if server.before is None else server.before

    def alive(self, db: int, name: Any) -> bool:
        deadline = self.server.expiry.get((db, name))
        if deadline is not None and self.server.clock >= deadline:
            self.server.dbs[db].pop(name, None)
            del self.server.expiry[(db, name)]
        return name in self.server.dbs[db]

    def set(self, name: str, value: str, *, nx: bool = False, px: int | None = None) -> Any:
        self.enter("SET")
        if nx and self.alive(self.db, name):
            return None
        self.server.dbs[self.db][name] = value
        if px is not None:
            self.server.expiry[(self.db, name)] = self.server.clock + px / 1000
        else:
            self.server.expiry.pop((self.db, name), None)
        return True

    def hgetall(self, name: Any) -> dict[Any, Any]:
        self.enter("HGETALL")
        return dict(self.server.dbs[self.db][name]) if self.alive(self.db, name) else {}

    def dbsize(self) -> int:
        self.enter("DBSIZE")
        return sum(1 for name in list(self.server.dbs[self.db]) if self.alive(self.db, name))

    def config_get(self, pattern: str) -> dict[str, str]:
        self.enter("CONFIG")
        return {pattern: self.server.config[pattern]} if pattern in self.server.config else {}

    def scan_iter(self, *, count: int | None = None) -> Iterator[Any]:
        self.enter("SCAN")
        return iter([name for name in list(self.server.dbs[self.db]) if self.alive(self.db, name)])

    def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any:
        self.enter("EVAL")
        function = self.server.scripts.get(script)
        if function is None:
            raise ValueError("the fake models only the three reviewed Lua sources")
        return function(self, list(keys_and_args[:numkeys]), list(keys_and_args[numkeys:]))


class FakeServer:
    """A FAKE Redis (see the module text): three databases, expiry on a fake clock, and a Python translation of the three Lua sources."""

    kind = "fake"

    def __init__(self) -> None:
        self.phrase = PHRASE
        self.dbs: dict[int, dict[Any, Any]] = {0: {}, 1: {}, 2: {}}
        self.expiry: dict[tuple[int, Any], float] = {}
        self.clock = 0.0
        self.config = {"maxmemory-policy": "noeviction", "maxmemory": str(1 << 30)}
        self.stalled = False
        self.log: list[str] = []
        self.before: Callable[[str], None] | None = None
        self.scripts = {MODELED_BEGIN_LOAD: self.begin_load, MODELED_PUBLISH: self.publish, MODELED_RELEASE: self.release}

    def client(self, db: int, *, password: str | None = None, timeout: float = online.SOCKET_TIMEOUT, unreachable: bool = False) -> FakeClient:
        return FakeClient(self, db, self.phrase if password is None else password, timeout, unreachable)

    # Translations of the Lua sources: the same comparisons in the same order.
    def begin_load(self, client: FakeClient, keys: list[str], args: list[str]) -> int:
        if not client.alive(client.db, keys[0]) or self.dbs[client.db].get(keys[0]) != args[0]:
            return 0
        self.dbs[client.db].pop(keys[1], None)
        self.dbs[client.db][keys[1]] = {"state": "loading", "load_id": args[1], "owner_token": args[0], **dict(zip(args[2::2], args[3::2], strict=True))}
        self.dbs[1].clear()
        return 1

    def publish(self, client: FakeClient, keys: list[str], args: list[str]) -> int:
        if not client.alive(client.db, keys[0]) or self.dbs[client.db].get(keys[0]) != args[0]:
            return 0
        marker: dict[str, str] = self.dbs[client.db].get(keys[1], {}) if client.alive(client.db, keys[1]) else {}
        if marker.get("load_id") != args[1]:
            return 0
        self.dbs[client.db][keys[1]] = marker
        marker.update({"state": "ready", **dict(zip(args[2::2], args[3::2], strict=True))})
        return 1

    def release(self, client: FakeClient, keys: list[str], args: list[str]) -> int:
        if client.alive(client.db, keys[0]) and self.dbs[client.db].get(keys[0]) == args[0]:
            del self.dbs[client.db][keys[0]]
            self.expiry.pop((client.db, keys[0]), None)
            return 1
        return 0

    def write_hash(self, db: int, key: Any, mapping: Mapping[str, str]) -> None:
        """What Feast does through its own connection: write an entity hash into a database."""
        self.dbs[db].setdefault(key, {}).update(mapping)

    def read_hash(self, db: int, key: Any) -> dict[str, str]:
        return dict(self.dbs[db].get(key, {}))

    def restart(self) -> None:
        for database in self.dbs.values():
            database.clear()
        self.expiry.clear()

    def advance(self, seconds: float) -> None:
        self.clock += seconds

    def stall(self) -> None:
        self.stalled = True

    def unstall(self) -> None:
        self.stalled = False

    def set_config(self, name: str, value: str) -> None:
        self.config[name] = value

    def get_config(self, name: str) -> str:
        return self.config[name]

    def lease_holder(self) -> str | None:
        client = self.client(online.CONTROL_DB)
        return self.dbs[2].get(online.LEASE_KEY) if client.alive(2, online.LEASE_KEY) else None

    def marker(self) -> dict[str, str]:
        return dict(self.dbs[2].get(online.MARKER_KEY, {}))

    def database_keys(self, db: int) -> list[Any]:
        return list(self.dbs[db])


class RealServer:
    """Lane B's real service, reached as ``FEAST_REDIS_HOST`` with ``REDIS_PASSWORD``. Restart is FLUSHALL; a stall is CLIENT PAUSE."""

    kind = "redis"

    def __init__(self, environ: Mapping[str, str]) -> None:
        self.environ = dict(environ)
        self.admin: Any = online.connect(self.environ, 0, timeout=5.0, decode=True)
        self.saved: dict[str, str] = {}

    def client(self, db: int, *, password: str | None = None, timeout: float = online.SOCKET_TIMEOUT, unreachable: bool = False) -> Any:
        environ = dict(self.environ)
        if password is not None:
            environ["REDIS_PASSWORD"] = password
        return importlib.import_module("redis").Redis(
            host=environ["FEAST_REDIS_HOST"],
            port=1 if unreachable else online.PORT,
            db=db,
            password=environ["REDIS_PASSWORD"],
            socket_timeout=timeout,
            socket_connect_timeout=timeout,
            retry=online.no_retry(),
            decode_responses=db != online.FEATURE_DB,
        )

    def write_hash(self, db: int, key: Any, mapping: Mapping[str, str]) -> None:
        self.client(db).hset(key, mapping=dict(mapping))

    def read_hash(self, db: int, key: Any) -> dict[str, str]:
        raw = self.client(db).hgetall(key)
        return {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v) for k, v in raw.items()}

    def restart(self) -> None:
        self.admin.flushall()

    def advance(self, seconds: float) -> None:
        time.sleep(seconds)

    def stall(self) -> None:
        self.admin.client_pause(1500)

    def unstall(self) -> None:
        time.sleep(1.7)

    def set_config(self, name: str, value: str) -> None:
        self.saved.setdefault(name, self.get_config(name))
        self.admin.config_set(name, value)

    def get_config(self, name: str) -> str:
        return str(self.admin.config_get(name)[name])

    def lease_holder(self) -> str | None:
        value = self.client(online.CONTROL_DB).get(online.LEASE_KEY)
        return None if value is None else str(value)

    def marker(self) -> dict[str, str]:
        return self.read_hash(online.CONTROL_DB, online.MARKER_KEY)

    def database_keys(self, db: int) -> list[Any]:
        return list(self.client(db).scan_iter())

    def reset(self) -> None:
        for name, value in self.saved.items():
            self.admin.config_set(name, value)
        self.saved.clear()
        self.admin.flushall()


# ---------------------------------------------------------------------------------------------------------------------------------------------
# A stand-in for Feast's materialize and the catalog, written through the server interface.
# ---------------------------------------------------------------------------------------------------------------------------------------------


def field_values(version: str, key: str) -> dict[str, str]:
    """The Feast-shaped hash of one entity in a load of ``version``: a value per reference and the event time of the hospital-window view."""
    fields = {serving.feast_name(reference): f"{version}|{key}|{reference}" for reference in [*REFERENCES, serving.PROBE]}
    fields[serving.feast_name(serving.PROBE) + serving.TIMESTAMP_POSTFIX] = str(WINDOW_SECONDS)
    return fields


class Hooks:
    """Recording stand-ins for head verification and Feast. ``actions`` maps an event name to a function the case wants run at that point."""

    def __init__(self, server: Any, version: str, keys: Sequence[str] = KEYS, **actions: Callable[..., None]) -> None:
        self.server, self.version, self.keys, self.actions = server, version, list(keys), actions
        self.events: list[str] = []
        self.parsed = True

    def verify_heads(self, moment: str) -> None:
        self.events.append(f"heads:{moment}")
        if "heads" in self.actions:
            self.actions["heads"](moment)

    def materialize(self, start: datetime, end: datetime) -> None:
        self.events.append("materialize")
        for index, key in enumerate(self.keys):
            self.server.write_hash(online.FEATURE_DB, key.encode(), field_values(self.version, key))
            if "written" in self.actions:
                self.actions["written"](index)
        if "materialized" in self.actions:
            self.actions["materialized"]()

    def parse_key(self, key: bytes) -> bool:
        return self.parsed and key.decode().count(":") == 1

    def verify_samples(self) -> None:
        self.events.append("samples")


def world(run: str = RUN_A, snapshots: Mapping[str, int] = WORLD) -> tuple[feast_cases.StubCatalog, reads.Publication]:
    """A stub catalog whose tables sit at the given snapshots, and the publication that names them."""
    catalog = feast_cases.StubCatalog()
    for name, snapshot in snapshots.items():
        catalog.tables[name] = feast_cases.StubTable(name, snapshot)
    catalog.tables[reads.RUNS_TABLE] = feast_cases.StubTable(reads.RUNS_TABLE, 1, {}, [feast_cases.runs_row(run, T0, dict(snapshots))])
    return catalog, reads.Publication("features_e2e", run, T0.isoformat(), dict(snapshots))


def plan_for(run: str = RUN_A, count: int = len(KEYS), schema: str = SCHEMA_HASH) -> online.LoadPlan:
    return online.LoadPlan(run, SETS, schema, count, 3, T0, T0 + timedelta(days=1000), "0.66.0")


def never(phase: str) -> None:
    return None


def load(server: Any, hooks: Hooks, plan: online.LoadPlan | None = None, lease_ms: int = 60_000, deadline: Callable[[str], None] = never) -> online.LoadResult:
    return online.run_load(server.client(online.CONTROL_DB), server.client(online.FEATURE_DB), plan or plan_for(), hooks, deadline=deadline, lease_ms=lease_ms)


def reader(server: Any) -> serving.FeatureReader:
    """The Feast reader of the cases: the key's hash in database 1, Feast-shaped; a key never written reads empty with epoch zero (fact F10)."""

    def read(references: list[str], key: str) -> Mapping[str, Sequence[Any]]:
        stored = server.read_hash(online.FEATURE_DB, key.encode())
        stamp = int(stored.get(serving.feast_name(serving.PROBE) + serving.TIMESTAMP_POSTFIX, "0"))
        answer: dict[str, Sequence[Any]] = {}
        for reference in references:
            name = serving.feast_name(reference)
            answer[name] = [stored.get(name)]
            answer[name + serving.TIMESTAMP_POSTFIX] = [stamp]
        return answer

    return read


def markers(server: Any) -> serving.MarkerReader:
    control = server.client(online.CONTROL_DB, timeout=online.SERVING_TIMEOUT)
    return lambda: online.read_marker(control)


def request(server: Any, key: str = KEYS[0], references: Sequence[str] = REFERENCES, schema_hash: str = SCHEMA_HASH, **options: Any) -> serving.Served:
    ccn, _, year = key.partition(":")
    read = options.get("read_features", reader(server))
    return serving.serve(options.get("read_marker", markers(server)), read, references=references, schema_hash=schema_hash, ccn=ccn, window_year=int(year))


def outcome(server: Any, key: str = KEYS[0], **options: Any) -> str:
    return answered(lambda: request(server, key, **options))


def answered(call: Callable[[], serving.Served]) -> str:
    """The outcome of one request as a word: the version tag of its values, ``503`` or ``404``."""
    try:
        served = call()
    except serving.Unavailable:
        return "503"
    except serving.NotFound:
        return "404"
    versions = {str(value).split("|")[0] for value in served.values.values()}
    return versions.pop() if len(versions) == 1 else "mixed"


# ---------------------------------------------------------------------------------------------------------------------------------------------
# Cases. ``make`` builds a clean server for the part that is running.
# ---------------------------------------------------------------------------------------------------------------------------------------------


def case_e20(c: Checker, make: Callable[[], Any]) -> None:
    """D1, RS5, B9: Redis down, stalled or with a wrong password stops the step and names it, never the password; unset settings are named."""
    server = make()
    down = server.client(online.CONTROL_DB, unreachable=True)
    message = c.refused("an unreachable Redis stops the lease step", online.acquire, "lease acquire", down, "token-1")
    c.check("the refusal names the error type", "ConnectionError" in message)
    bad_phrase = "not-the-password"
    wrong = server.client(online.CONTROL_DB, password=bad_phrase)
    message = c.refused("a wrong password stops the lease step", online.acquire, "lease acquire", wrong, "token-1")
    c.check("the refusal never holds the password", bad_phrase not in message and PHRASE not in message and "Error" in message)
    c.check("nothing was written by the refused attempts", server.marker() == {} and server.lease_holder() is None)
    hooks = Hooks(server, "v1")
    stalled = server.client(online.CONTROL_DB, timeout=0.3)
    server.stall()
    message = c.refused(
        "a stalled Redis stops the load", online.run_load, "Redis failed", stalled, server.client(online.FEATURE_DB), plan_for(), hooks, deadline=never
    )
    server.unstall()
    c.check("the stalled load wrote nothing", server.marker() == {})
    stall_after = Hooks(server, "v1", written=lambda index: server.stall() if index == 1 else None)
    control = server.client(online.CONTROL_DB, timeout=0.3)
    c.refused(
        "a stall during the load stops the load at the next Redis call",
        online.run_load,
        "Redis failed",
        control,
        server.client(online.FEATURE_DB, timeout=0.3),
        plan_for(),
        stall_after,
        deadline=never,
    )
    server.unstall()
    c.check("the marker stays loading after the stall", server.marker().get("state") == "loading")
    c.refused("unset settings are named without values", online.connect, "REDIS_PASSWORD", {"FEAST_REDIS_HOST": "redis"}, 2)
    if server.kind == "fake":
        serving_adapter_checks(c)
    reset(server)


def reset(server: Any) -> None:
    if server.kind == "redis":
        server.reset()


class AdapterStore:
    """A stand-in for a Feast store: one online store that keeps the client it is given, as Feast 0.66.0's Redis online store does.

    ``get_online_features`` records the client the online store hands out at the moment of the read, which is the client the real read would use.
    """

    def __init__(self, honours_client: bool = True) -> None:
        self.honours = honours_client
        self.online_store = SimpleNamespace(_client=None, _get_client=self.get_client)
        self.config = SimpleNamespace(online_store=SimpleNamespace())
        self.used: list[Any] = []

    def get_client(self, config: Any) -> Any:
        return self.online_store._client if self.honours else object()

    def _get_provider(self) -> Any:
        return SimpleNamespace(online_store=self.online_store)

    def get_online_features(self, **request: Any) -> Any:
        del request
        self.used.append(self.get_client(self.config.online_store))

        def to_dict(**options: Any) -> dict[str, list[int]]:
            del options
            return {serving.feast_name(serving.PROBE) + serving.TIMESTAMP_POSTFIX: [WINDOW_SECONDS]}

        return SimpleNamespace(to_dict=to_dict)


def serving_adapter_checks(c: Checker) -> None:
    """LO1, LO16 (code review 1, finding 6): every Redis client is built with explicit bounds and zero retries, and both serving adapters use the 1 s client."""
    calls: list[dict[str, Any]] = []
    environ = {"FEAST_REDIS_HOST": "redis", "REDIS_PASSWORD": PHRASE}

    def no_retry(options: Mapping[str, Any]) -> bool:
        retry = options.get("retry")
        return retry is not None and retry.retries == 0 and retry.backoff.kind == "no backoff"

    with fake_redis_modules(calls):
        online.connect(environ, online.CONTROL_DB)
        c.check(
            "a job client connects to database 2 on port 6379 with 5 s timeouts",
            (calls[-1]["db"], calls[-1]["port"], calls[-1]["socket_timeout"], calls[-1]["socket_connect_timeout"]) == (2, 6379, 5.0, 5.0),
        )
        c.check("a job client has explicit zero retries", no_retry(calls[-1]))
        serving.marker_reader(environ)
        c.check(
            "the serving marker client uses 1 s timeouts on database 2",
            (calls[-1]["db"], calls[-1]["socket_timeout"], calls[-1]["socket_connect_timeout"]) == (2, 1.0, 1.0),
        )
        c.check("the serving marker client has explicit zero retries", no_retry(calls[-1]))
        store = AdapterStore()
        store.online_store._client = SimpleNamespace(socket_timeout=5.0, retry=None)
        read = serving.feast_reader(store, environ)
        c.check("building the serving reader replaces an ordinary 5 s client with the serving client", store.online_store._client.socket_timeout == 1.0)
        c.check(
            "that client is database 1 with 1 s timeouts and bytes",
            (calls[-1]["db"], calls[-1]["socket_timeout"], calls[-1]["socket_connect_timeout"], calls[-1]["decode_responses"]) == (1, 1.0, 1.0, False),
        )
        c.check("that client has explicit zero retries", no_retry(calls[-1]))
        read([serving.PROBE], KEYS[0])
        used = store.used[-1]
        c.check(
            "the client a feature read actually uses has 1 s timeouts and zero retries",
            used.socket_timeout == 1.0 and used.socket_connect_timeout == 1.0 and no_retry(vars(used)),
        )
        store.online_store._client = SimpleNamespace(socket_timeout=5.0, retry=None)
        c.refused("a read after the store changed its client is refused", read, "no longer uses the serving client", [serving.PROBE], KEYS[0])
        ready = {"state": "ready", "load_id": "x", "feature_schema_sha256": SCHEMA_HASH}
        c.check(
            "a request on a store that changed its client is 503",
            answered(lambda: serving.serve(lambda: ready, read, references=NO_WINDOW_MODEL, schema_hash=SCHEMA_HASH, ccn="AAA001", window_year=2022)) == "503",
        )
        c.refused(
            "a store that ignores the injected client is refused",
            serving.feast_reader,
            "does not use the serving client",
            AdapterStore(honours_client=False),
            environ,
        )
        job_class = SimpleNamespace(_client=None)
        feast_modules = {
            "feast": fake_module("feast"),
            "feast.infra": fake_module("feast.infra"),
            "feast.infra.online_stores": fake_module("feast.infra.online_stores"),
            "feast.infra.online_stores.redis": fake_module("feast.infra.online_stores.redis", RedisOnlineStore=job_class),
        }
        with mock.patch.dict(sys.modules, feast_modules):
            with online.feast_job_redis(environ):
                inside = job_class._client
                c.check(
                    "Feast's own online stores get a database 1 client with 5 s timeouts and zero retries",
                    (inside.db, inside.socket_timeout, inside.socket_connect_timeout, inside.decode_responses) == (1, 5.0, 5.0, False)
                    and no_retry(vars(inside)),
                )
            c.check("the job client is removed again after the block", job_class._client is None)
            job_class._client = "earlier"
            with online.feast_job_redis(environ):
                pass
            c.check("an earlier class-level client is restored", job_class._client == "earlier")
    c.check("the serving timeout is 1 s and the job timeout 5 s", (online.SERVING_TIMEOUT, online.SOCKET_TIMEOUT) == (1.0, 5.0))


def case_e21(c: Checker, make: Callable[[], Any]) -> None:
    """D1, D9, CR5: an unsafe policy or size is refused before the flush; the earlier ready load is untouched; no message or marker holds the password."""
    server = make()
    first = load(server, Hooks(server, "old"))
    before = (server.marker(), sorted(server.database_keys(online.FEATURE_DB)))
    c.check("a clean load is ready", server.marker().get("state") == "ready" and first.entity_count == len(KEYS))
    messages = []
    server.set_config("maxmemory-policy", "allkeys-lru")
    messages.append(c.refused("an eviction policy is refused", load, "noeviction", server, Hooks(server, "new")))
    c.check("the refused load changed neither the marker nor the data", (server.marker(), sorted(server.database_keys(online.FEATURE_DB))) == before)
    server.set_config("maxmemory-policy", "noeviction")
    server.set_config("maxmemory", "0")
    messages.append(c.refused("no memory limit is refused", load, "no memory limit", server, Hooks(server, "new")))
    limit = 64 * 1024 * 1024
    server.set_config("maxmemory", str(limit))
    per_entity = len(KEYS) * online.BYTES_PER_FIELD
    over = online.LoadPlan(RUN_A, SETS, SCHEMA_HASH, len(KEYS), int(0.7 * limit / per_entity), T0, T0, "0.66.0")
    under = online.LoadPlan(RUN_A, SETS, SCHEMA_HASH, len(KEYS), int(0.5 * limit / per_entity), T0, T0, "0.66.0")
    messages.append(c.refused("an estimate above 60 % of maxmemory is refused", load, "60 %", server, Hooks(server, "new"), over))
    c.check("the guard refused before the flush", server.marker() == before[0])
    c.check("an estimate under 60 % passes", load(server, Hooks(server, "new"), under).entity_count == len(KEYS))
    pieces = [*messages, repr(server.marker())]
    c.check("no message or marker holds the password", not any(PHRASE in piece for piece in pieces))
    bad_phrase = "wrong-secret-xyz"
    wrong = server.client(online.CONTROL_DB, password=bad_phrase)
    message = c.refused("a wrong password is refused", online.acquire, "Redis failed", wrong, "t")
    c.check("the wrong password is not echoed", bad_phrase not in message)
    reset(server)


def case_e22(c: Checker, make: Callable[[], Any]) -> None:
    """D2, D3 (count half): an interrupted load then a retry gives the same content; counts cover database 1 only; foreign keys are refused."""
    server = make()
    first_hooks = Hooks(server, "v1")
    clean = load(server, first_hooks)
    c.check(
        "the heads are verified before the flush and after the last write",
        first_hooks.events == ["heads:before the flush", "materialize", "samples", "heads:after the last write"],
    )
    digest = online.database_digest(server.client(online.FEATURE_DB))
    server.write_hash(online.FEATURE_DB, KEYS[0].encode(), {"x": "changed"})
    c.check("the content digest changes when one field changes", online.database_digest(server.client(online.FEATURE_DB)) != digest)
    clean = load(server, Hooks(server, "v1"))
    c.check("the first load counts every entity and sets ready", clean.entity_count == len(KEYS) and server.marker().get("entity_count") == str(len(KEYS)))
    c.check(
        "control keys do not enter the count", len(server.database_keys(online.FEATURE_DB)) == len(KEYS) and len(server.database_keys(online.CONTROL_DB)) >= 1
    )
    c.check("database 0 stays empty", server.database_keys(0) == [])

    def interrupt(index: int) -> None:
        if index == 1:
            raise RuntimeError("interrupted")

    try:
        load(server, Hooks(server, "v1", written=interrupt))
        raised = False
    except RuntimeError:
        raised = True
    c.check("an interruption leaves the marker loading", raised and server.marker().get("state") == "loading")
    c.check("the interruption left a partial set and released the lease", len(server.database_keys(online.FEATURE_DB)) == 2 and server.lease_holder() is None)
    load(server, Hooks(server, "v1"))
    c.check("the retry gives the same content digest", online.database_digest(server.client(online.FEATURE_DB)) == digest)
    c.check("the retry ends ready", server.marker().get("state") == "ready")
    foreign = Hooks(server, "v1", written=lambda index: server.write_hash(1, b"stray", {"a": "b"}) if index == 0 else None)
    c.refused("a foreign key makes the count differ", load, "database 1 holds", server, foreign)
    c.check("a refused count leaves the marker loading", server.marker().get("state") == "loading")
    bad = Hooks(server, "v1")
    bad.parsed = False
    c.refused("keys that are not Feast entity keys are refused", load, "not Feast entity keys", server, bad)
    server.restart()
    c.refused(
        "a restart in the middle of a load stops it at the count",
        load,
        "database 1 holds",
        server,
        Hooks(server, "v1", written=lambda index: server.restart() if index == 1 else None),
    )
    c.check("the marker stays absent after that restart", server.marker() == {})
    c.check("the retry after refusals is ready again", load(server, Hooks(server, "v1")).entity_count == len(KEYS) and server.marker().get("state") == "ready")
    reset(server)


def case_e23(c: Checker, make: Callable[[], Any]) -> None:
    """D4, D5, B10 (contract half): the probe is always requested; an epoch-zero key is 404, also for a model without a window predictor."""
    server = make()
    load(server, Hooks(server, "v1"))
    seen: list[list[str]] = []

    def spy(references: list[str], key: str) -> Mapping[str, Sequence[Any]]:
        seen.append(list(references))
        return reader(server)(references, key)

    served = request(server, read_features=spy)
    c.check("a known key returns every requested value", served.values == {ref: f"v1|{KEYS[0]}|{ref}" for ref in REFERENCES})
    c.check("the probe was requested once, after the model's references", seen[0] == [*REFERENCES, serving.PROBE] and seen[0].count(serving.PROBE) == 1)
    c.check(
        "the response carries the window start and the schema hash and nothing of the marker",
        (served.features_as_of, served.feature_schema_hash) == ("2022-01-01", SCHEMA_HASH),
    )
    c.check("the response has no marker field", set(vars(served)) == {"values", "features_as_of", "feature_schema_hash"})
    seen.clear()
    served = request(server, references=NO_WINDOW_MODEL, read_features=spy)
    c.check(
        "a model without a hospital-window predictor still requests the probe",
        seen[0] == [*NO_WINDOW_MODEL, serving.PROBE] and list(served.values) == NO_WINDOW_MODEL,
    )
    seen.clear()
    request(server, references=[serving.PROBE], read_features=spy)
    c.check("a model that lists the probe does not get it twice", seen[0] == [serving.PROBE])
    c.check("an unknown key is 404", answered(lambda: request(server, key="ZZZ999:2022")) == "404")
    c.check(
        "an unknown key is 404 for a model without a hospital-window predictor",
        answered(lambda: request(server, key="ZZZ999:2022", references=NO_WINDOW_MODEL)) == "404",
    )
    servable = [*REFERENCES, serving.PROBE]
    serving.check_startup(REFERENCES, servable)
    c.check("a servable feature list starts", True)
    try:
        serving.check_startup([*REFERENCES, "feature_matrix_hospital_window:ccn"], servable)
        started = True
    except serving.StartupRefused:
        started = False
    c.check("a reference outside the servable list stops start-up", not started)
    requests: list[tuple[Any, Any]] = [("AAA00", 2022), ("AAA0011", 2022), ("AAA 01", 2022), ("AAA001", "2022"), ("AAA001", 3000), ("AAA001", True)]
    for ccn, year in requests:
        try:
            serving.spine_key(ccn, year)
            accepted = True
        except ValueError:
            accepted = False
        c.check(f"the request {ccn!r}/{year!r} is not a valid hospital-window", not accepted)
    reset(server)


def case_e24(c: Checker, make: Callable[[], Any]) -> None:
    """D6, D7, D14, D17, D18: every answer during a reload or a restart is the old load's full values, the new load's full values, or 503."""
    server = make()
    load(server, Hooks(server, "old"))
    c.check("before the reload every answer is the old load", all(outcome(server, key) == "old" for key in KEYS))
    c.check("a model trained on another schema gets 503", outcome(server, schema_hash=OTHER_HASH) == "503")
    during: list[str] = []
    load(server, Hooks(server, "new", written=lambda index: during.append(outcome(server))))
    c.check("while the reload writes, every answer is 503", during == ["503"] * len(KEYS))
    c.check("after the reload every answer is the new load", all(outcome(server, key) == "new" for key in KEYS))
    old_marker = server.marker()

    def reload_between_reads(references: list[str], key: str) -> Mapping[str, Sequence[Any]]:
        answer = reader(server)(references, key)
        load(server, Hooks(server, "newer"))
        return answer

    c.check("a reload between the two marker reads gives 503", outcome(server, read_features=reload_between_reads) == "503")
    c.check("the reload really happened", server.marker().get("load_id") != old_marker.get("load_id") and server.marker().get("state") == "ready")
    c.check("the next request sees the newer load", outcome(server) == "newer")
    marker_before_restart = server.marker()

    def restart_and_repair(references: list[str], key: str) -> Mapping[str, Sequence[Any]]:
        answer = reader(server)(references, key)
        server.restart()
        load(server, Hooks(server, "repaired"))
        return answer

    c.check("a restart and a finished repair between the two marker reads gives 503", outcome(server, read_features=restart_and_repair) == "503")
    c.check("the repair has a different load ID than the marker the request read first", server.marker().get("load_id") != marker_before_restart.get("load_id"))
    c.check("the next request sees the repaired load", outcome(server) == "repaired")
    load(server, Hooks(server, "small", keys=KEYS[:2]), plan_for(count=2))
    c.check("a key absent from the new set is not served from the old one", outcome(server, KEYS[3]) == "404")
    c.check("keys of the new set are served", outcome(server, KEYS[0]) == "small")

    def move_table(moment: str) -> None:
        if moment == "after the last write":
            raise ReadError("a table moved during the load")

    load(server, Hooks(server, "steady"))
    steady = (server.marker(), online.database_digest(server.client(online.FEATURE_DB)))

    def move_before(moment: str) -> None:
        if moment == "before the flush":
            raise ReadError("a table moved before the load")

    c.refused("a table moved before the flush stops the load", load, "a table moved", server, Hooks(server, "never", heads=move_before))
    c.check("the earlier ready load is untouched by that refusal", (server.marker(), online.database_digest(server.client(online.FEATURE_DB))) == steady)
    c.refused("a table moved during the load stops the publish", load, "a table moved", server, Hooks(server, "late", heads=move_table))
    c.check("the marker stays loading and requests get 503", server.marker().get("state") == "loading" and outcome(server) == "503")
    c.check("no entity key has an expiry", server.kind != "fake" or not any(db == online.FEATURE_DB for db, _ in server.expiry))
    ready = {"state": "ready", "load_id": "x", "feature_schema_sha256": SCHEMA_HASH}
    c.check("a feature answer without the requested features is 503", outcome(server, read_features=lambda refs, key: {}, read_marker=lambda: ready) == "503")
    markers_seen = iter([ready, {**ready, "state": "loading"}])
    c.check("a second marker that is loading, even with the same load ID, is 503", outcome(server, read_marker=lambda: next(markers_seen)) == "503")
    c.check("a ready marker without a load ID is 503", outcome(server, read_marker=lambda: {"state": "ready", "feature_schema_sha256": SCHEMA_HASH}) == "503")
    reset(server)


def case_e25(c: Checker, make: Callable[[], Any]) -> None:
    """B5, D11, D13, D15, D16, D19, RS3, RS5: one writer at a time; the lease survives the flush; a writer that lost its lease cannot flush or publish."""
    server = make()
    second_errors: list[str] = []
    holder: list[str | None] = []

    def second_writer(index: int) -> None:
        if index == 1:
            holder.append(server.lease_holder())
            try:
                load(server, Hooks(server, "intruder"))
            except ReadError as error:
                second_errors.append(str(error))

    load(server, Hooks(server, "first", written=second_writer))
    c.check("the lease is in database 2 and still held after the flush (D15)", len(holder) == 1 and holder[0] is not None)
    c.check(
        "a second writer is refused at once naming the holder's feature run",
        len(second_errors) == 1 and "held by another writer" in second_errors[0] and RUN_A in second_errors[0],
    )
    c.check("the first load finished ready and the intruder wrote nothing", server.marker().get("state") == "ready" and outcome(server) == "first")
    c.check("the lease is released after the load", server.lease_holder() is None)
    c.check(
        "database 0 is empty, database 1 holds only entities, database 2 only the marker",
        server.database_keys(0) == [] and len(server.database_keys(1)) == len(KEYS) and server.database_keys(2) == [online.MARKER_KEY],
    )
    refused_apply: list[str] = []

    def apply_during(index: int) -> None:
        if index == 0:
            try:
                with online.writer_lease({}, control=server.client(online.CONTROL_DB)):
                    pass
            except ReadError as error:
                refused_apply.append(str(error))

    load(server, Hooks(server, "second", written=apply_during))
    c.check("an apply is refused while a load holds the lease", len(refused_apply) == 1 and "held by another writer" in refused_apply[0])
    with online.writer_lease({}, control=server.client(online.CONTROL_DB)):
        c.refused("a load is refused while an apply holds the lease", load, "held by another writer", server, Hooks(server, "third"))
    c.check("the apply's lease is released at exit", server.lease_holder() is None)
    expired: list[str] = []

    def lose_lease_before_flush(moment: str) -> None:
        if moment == "before the flush":
            server.advance(0.5)
            expired.append("expired")
            load(server, Hooks(server, "newcomer"), lease_ms=60_000)

    c.refused(
        "a writer whose lease expired before the flush is refused",
        load,
        "flush was refused",
        server,
        Hooks(server, "late", heads=lose_lease_before_flush),
        None,
        300,
    )
    c.check("the newcomer's load stands and the late writer flushed nothing", expired == ["expired"] and outcome(server) == "newcomer")
    marker_after = server.marker()

    def usurp_before_publish() -> None:
        server.advance(0.5)
        load(server, Hooks(server, "usurper"), lease_ms=60_000)

    c.refused(
        "a writer whose lease expired before the publish cannot publish",
        load,
        "publish was refused",
        server,
        Hooks(server, "slow", materialized=usurp_before_publish),
        None,
        300,
    )
    c.check(
        "the usurper's marker is ready and its values are served",
        server.marker().get("state") == "ready" and server.marker().get("load_id") != marker_after.get("load_id") and outcome(server) == "usurper",
    )

    def restart_before_publish(moment: str) -> None:
        if moment == "after the last write":
            server.restart()

    c.refused(
        "a restart after the last write makes the publish refused (D19)",
        load,
        "publish was refused",
        server,
        Hooks(server, "doomed", heads=restart_before_publish),
    )
    c.check("the marker stays absent and requests get 503", server.marker() == {} and outcome(server) == "503")
    catalog, publication = world()
    ok, _ = online.readiness(server.client(online.CONTROL_DB), server.client(online.FEATURE_DB), catalog, publication)
    c.check("readiness fails until a repair", not ok)
    load(server, Hooks(server, "repair"))
    c.check("the repair succeeds with the lease free again", server.marker().get("state") == "ready" and server.lease_holder() is None)
    reset(server)


def case_e26(c: Checker, make: Callable[[], Any]) -> None:
    """D10, D19: readiness needs the marker ready twice with one load ID, the entity count, the publication and its table heads."""
    server = make()
    control, features = server.client(online.CONTROL_DB), server.client(online.FEATURE_DB)
    catalog, publication = world()
    ok, reason = online.readiness(control, features, catalog, publication)
    c.check("an empty Redis is not ready", not ok and "absent" in reason)
    load(server, Hooks(server, "v1"))
    ok, _ = online.readiness(control, features, catalog, publication)
    c.check("after a load it is ready", ok)
    ok, _ = online.readiness_for_head(control, features, catalog, "features_e2e")
    c.check("the head of the catalog is the publication it was loaded for", ok)
    _, newer = world(RUN_B, {"feature_matrix_a": 21, "feature_matrix_b": 22})
    ok, reason = online.readiness(control, features, catalog, newer)
    c.check("a newer publication head makes it not ready", not ok and "another publication" in reason)
    _, republished = world(RUN_B)
    ok, reason = online.readiness(control, features, catalog, republished)
    c.check("a new publication of identical tables (same snapshots, another run) is not ready until loaded", not ok and "another publication" in reason)
    catalog.tables["feature_matrix_a"].main = 99
    ok, reason = online.readiness(control, features, catalog, publication)
    c.check("a table that moved makes it not ready, naming the table", not ok and "feature_matrix_a" in reason and "readiness" in reason)
    catalog.tables["feature_matrix_a"].main = WORLD["feature_matrix_a"]
    server.write_hash(online.FEATURE_DB, b"stray:1", {"a": "b"})
    ok, reason = online.readiness(control, features, catalog, publication)
    c.check("an extra key makes it not ready", not ok and "number of entity keys" in reason)
    server.restart()
    ok, _ = online.readiness(control, features, catalog, publication)
    c.check("a restart makes it not ready", not ok and server.marker() == {})
    first = load(server, Hooks(server, "v1"))
    ok, _ = online.readiness(control, features, catalog, publication)
    c.check("the repair makes it ready", ok)
    server.restart()
    second = load(server, Hooks(server, "v1"))
    c.check("every repair has a new load ID", first.load_id != second.load_id)
    server.write_hash(online.CONTROL_DB, online.MARKER_KEY, {"state": "loading"})
    ok, reason = online.readiness(control, features, catalog, publication)
    c.check("a marker that is loading is not ready", not ok and "loading" in reason)
    if server.kind == "fake":
        load(server, Hooks(server, "v1"))
        fired: list[str] = []

        def restart_then_partial_reload(command: str) -> None:
            if command == "DBSIZE" and not fired:
                fired.append(command)
                server.restart()
                for key in KEYS:
                    server.write_hash(online.FEATURE_DB, key.encode(), field_values("partial", key))

        server.before = restart_then_partial_reload
        ok, reason = online.readiness(control, features, catalog, publication)
        server.before = None
        c.check(
            "a restart and a partial reload after the first marker read is not ready", fired == ["DBSIZE"] and not ok and "changed during the check" in reason
        )
        c.check("the marker is still absent after that race", server.marker() == {})
        load(server, Hooks(server, "v1"))
        repaired: list[str] = []

        def restart_then_repair(command: str) -> None:
            if command == "DBSIZE" and not repaired:
                repaired.append(command)
                server.restart()
                load(server, Hooks(server, "again"))

        server.before = restart_then_repair
        ok, reason = online.readiness(control, features, catalog, publication)
        server.before = None
        c.check(
            "a restart and a finished repair between the two marker reads is not ready",
            repaired == ["DBSIZE"] and not ok and "changed during the check" in reason,
        )
        ok, _ = online.readiness(control, features, catalog, publication)
        c.check("the next check after the repair is ready", ok)
    reset(server)


def case_e33(c: Checker, make: Callable[[], Any]) -> None:
    """RS5: every wait is forced to expire: the job deadline at each phase, Redis calls, the lease (no wait) and the serving calls."""
    server = make()
    phases: list[str] = []
    load(server, Hooks(server, "v1"), deadline=phases.append)
    expected = [
        "the lease",
        "the memory guard",
        "the first head check",
        "the flush",
        "materialize",
        "the counts",
        "the samples",
        "the last head check",
        "publish",
    ]
    c.check("the load checks the deadline at every phase", phases == expected)
    for stop in range(len(expected)):
        load(server, Hooks(server, "v1"))
        calls: list[str] = []

        def stopper(phase: str, stop: int = stop, calls: list[str] = calls) -> None:
            calls.append(phase)
            if len(calls) > stop:
                raise ReadError(f"the job passed its deadline before {phase}")

        c.refused(f"a deadline that expires at phase {stop + 1} stops the load there", load, "deadline", server, Hooks(server, "v2"), None, 60_000, stopper)
        c.check(f"phase {stop + 1}: no later phase ran and the lease is released", len(calls) == stop + 1 and server.lease_holder() is None)
        flushed = stop > expected.index("the flush")
        c.check(
            f"phase {stop + 1}: the marker is {'loading' if flushed else 'still ready'}", server.marker().get("state") == ("loading" if flushed else "ready")
        )
    server.stall()
    c.check("a stalled marker read is 503 within the serving timeout", outcome(server) == "503")
    server.unstall()
    load(server, Hooks(server, "v3"))
    online.acquire(server.client(online.CONTROL_DB), "t0", 60_000)
    before = len(getattr(server, "log", []))
    c.refused("a lease held by another writer is refused at once", online.acquire, "held by another writer", server.client(online.CONTROL_DB), "t1")
    sets = getattr(server, "log", [])[before:].count("SET")
    c.check("the refusal used one SET and no retry", server.kind != "fake" or sets == 1)
    reset(server)


def case_serving_read(c: Checker, make: Callable[[], Any]) -> None:
    """LO13 to LO16: the serving client has no retry, treats any Redis or Feast error as 503 and reads the marker twice."""
    server = make()
    load(server, Hooks(server, "v1"))
    calls: list[str] = []

    def counting_marker() -> Mapping[str, str]:
        calls.append("marker")
        return markers(server)()

    request(server, read_marker=counting_marker)
    c.check("a request reads the marker exactly twice", calls == ["marker", "marker"])

    def broken_marker() -> Mapping[str, str]:
        calls.append("broken")
        raise TimeoutError("timed out")

    calls.clear()
    c.check("a marker read that fails is 503 and not retried", outcome(server, read_marker=broken_marker) == "503" and calls == ["broken"])

    def broken_features(references: list[str], key: str) -> Mapping[str, Sequence[Any]]:
        raise ConnectionError("down")

    c.check("a feature read that fails is 503", outcome(server, read_features=broken_features) == "503")
    reset(server)


def case_materialize(c: Checker, make: Callable[[], Any]) -> None:
    """LO11, D7: the materialize step builds its plan at R's snapshots, verifies the heads around the load and leaves the marker loading when a table moves."""
    rows = [{"spine_key": f"AAA00{n}:{year}", "window_year": year} for n, year in ((1, 2021), (2, 2021), (3, 2021), (1, 2022), (2, 2022))]
    c.check(
        "the sample keys are the first and last of each window year",
        feast_materialize.sample_keys(rows) == ["AAA001:2021", "AAA003:2021", "AAA001:2022", "AAA002:2022"],
    )
    c.check("a year with one key gives one sample", feast_materialize.sample_keys([rows[0]]) == ["AAA001:2021"])
    server = make()
    catalog = feast_cases.StubCatalog()
    window_table = feast_materialize.WINDOW_TABLE
    spine = [{"spine_key": key} for key in KEYS]
    stamps = [{"event_timestamp": T0 + timedelta(days=number)} for number in range(len(KEYS))]
    catalog.tables[reads.SPINE_TABLE] = feast_cases.StubTable(reads.SPINE_TABLE, 11, {}, spine)
    catalog.tables[window_table] = feast_cases.StubTable(window_table, 12, {}, stamps)
    catalog.tables["feature_matrix_other"] = feast_cases.StubTable("feature_matrix_other", 13)
    snapshots = {reads.SPINE_TABLE: 11, window_table: 12, "feature_matrix_other": 13}
    catalog.tables[reads.RUNS_TABLE] = feast_cases.StubTable(reads.RUNS_TABLE, 99, {}, [feast_cases.runs_row(RUN_A, T0, snapshots)])
    schema = feast_cases.synthetic_schema()
    publication = reads.read_publication(catalog, "features_e2e")
    plan = feast_materialize.load_plan(catalog, publication, schema, SCHEMA_HASH, "0.66.0")
    c.check("the plan counts the entities and the predictor columns", plan.entity_count == len(KEYS) and plan.predictor_columns == 4)
    c.check(
        "the window is the first and last event time, inclusive",
        plan.window_start == stamps[0]["event_timestamp"] and plan.window_end == stamps[-1]["event_timestamp"],
    )
    c.check(
        "the plan names the publication and the schema",
        plan.feature_run_id == RUN_A and plan.feature_schema_sha256 == SCHEMA_HASH and plan.snapshot_set_sha256 == publication.snapshot_set_sha256,
    )
    catalog.tables[window_table].rows = stamps[:2]
    c.refused(
        "a window table with fewer rows than the entities is refused",
        feast_materialize.load_plan,
        "has 2 rows",
        catalog,
        publication,
        schema,
        SCHEMA_HASH,
        "0.66.0",
    )
    catalog.tables[window_table].rows = stamps
    catalog.tables[reads.SPINE_TABLE].rows = [*spine, spine[0]]
    c.refused("a repeated entity key is refused", feast_materialize.load_plan, "not a clean entity table", catalog, publication, schema, SCHEMA_HASH, "0.66.0")
    catalog.tables[reads.SPINE_TABLE].rows = spine

    class HeadHooks(Hooks):
        def __init__(self, publication: reads.Publication, **actions: Callable[..., None]) -> None:
            super().__init__(server, "v1", KEYS, **actions)
            self.publication = publication

        def verify_heads(self, moment: str) -> None:
            reads.verify_heads(catalog, self.publication, moment)
            super().verify_heads(moment)

    def head_hooks(_catalog: Any, pub: reads.Publication, _document: dict[str, Any]) -> online.LoadHooks:
        return HeadHooks(pub)

    def move(index: int) -> None:
        if index == 1:
            catalog.tables["feature_matrix_other"].main = 777

    def moving_hooks(_catalog: Any, pub: reads.Publication, _document: dict[str, Any]) -> online.LoadHooks:
        return HeadHooks(pub, written=move)

    with tempfile.TemporaryDirectory(prefix="online_cases_") as raw:
        folder = Path(raw)
        environ = feast_cases.fixture_environment(folder)
        settings = feast_job.job_settings(environ, "materialize", RUN_A, "publish_fixture")
        control, features = server.client(online.CONTROL_DB), server.client(online.FEATURE_DB)
        record = feast_materialize.run_materialize(
            settings,
            environ,
            control=control,
            features=features,
            schema=(schema, SCHEMA_HASH),
            loader=lambda name, **properties: catalog,
            register=False,
            hooks_for=head_hooks,
        )
        c.check("the step loads every entity and writes its record", record["entities"] == len(KEYS) and (folder / "feast_materialize.json").is_file())
        job_store = SimpleNamespace(_client=None)
        observed: list[Any] = []

        def feast_hooks(*arguments: Any) -> online.LoadHooks:
            return HeadHooks(arguments[1], materialized=lambda: observed.append(job_store._client))

        def default_store(*arguments: Any) -> object:
            del arguments
            return object()

        patches = {"feast.infra.online_stores.redis": fake_module("feast.infra.online_stores.redis", RedisOnlineStore=job_store)}
        with contextlib.ExitStack() as stack:
            stack.enter_context(fake_redis_modules([]))
            stack.enter_context(mock.patch.dict(sys.modules, patches))
            stack.enter_context(mock.patch.object(feast_materialize, "FeastHooks", feast_hooks))
            stack.enter_context(mock.patch.object(historical, "default_store", default_store))
            stack.enter_context(mock.patch.object(importlib.metadata, "version", lambda name: "test"))
            feast_materialize.run_materialize(
                settings,
                feast_cases.fixture_environment(folder),
                control=control,
                features=features,
                schema=(schema, SCHEMA_HASH),
                loader=lambda name, **properties: catalog,
                register=False,
            )
        client = observed[0] if observed else None
        c.check(
            "while the Feast hooks materialize, Feast's online store has a 5 s, zero-retry database 1 client",
            client is not None and (client.socket_timeout, client.db, client.retry.retries) == (5.0, 1, 0),
        )
        c.check("that client is removed after the step", job_store._client is None)
        c.check(
            "the marker is ready for this publication and schema", server.marker().get("state") == "ready" and server.marker().get("feature_run_id") == RUN_A
        )
        c.check(
            "the marker carries the schema hash and snapshot set digest",
            server.marker().get("feature_schema_sha256") == SCHEMA_HASH and server.marker().get("snapshot_set_sha256") == publication.snapshot_set_sha256,
        )

        environ = feast_cases.fixture_environment(folder)
        c.refused(
            "a table that moves during the load stops the publish",
            feast_materialize.run_materialize,
            "after the last write",
            settings,
            environ,
            control=control,
            features=features,
            schema=(schema, SCHEMA_HASH),
            loader=lambda name, **properties: catalog,
            register=False,
            hooks_for=moving_hooks,
        )
        c.check("the marker stays loading and the lease is released", server.marker().get("state") == "loading" and server.lease_holder() is None)
    reset(server)


# ---------------------------------------------------------------------------------------------------------------------------------------------


FAKE_CASES: dict[str, tuple[Callable[[Checker, Callable[[], Any]], None], str]] = {
    "E20": (case_e20, "LO1 D1 RS5 B9"),
    "E21": (case_e21, "LO8 LO17 D1 D9 CR5"),
    "E22": (case_e22, "LO9 LO10 D2 D3"),
    "E23": (case_e23, "LO14 LO15 D4 D5 B10"),
    "E24": (case_e24, "LO5 LO7 LO11 LO13 D6 D7 D14 D17 D18"),
    "E25": (case_e25, "LO2 LO3 LO4 LO6 B5 D11 D13 D15 D16 D19 RS3 RS5"),
    "E26": (case_e26, "LO6 LO12 D10 D19"),
    "E33": (case_e33, "LO1 LO16 RS5"),
    "E24_serving": (case_serving_read, "LO13 LO16"),
    "E24_materialize": (case_materialize, "LO11 D7"),
}


def bind(function: Callable[[Checker, Callable[[], Any]], None], make: Callable[[], Any]) -> Callable[[Checker], None]:
    return lambda checker: function(checker, make)


def cases_for(part: str) -> dict[str, tuple[Callable[[Checker], None], str]]:
    if part == "fake":
        make: Callable[[], Any] = FakeServer
    else:
        environ = {name: os.environ.get(name, "") for name in ("FEAST_REDIS_HOST", "REDIS_PASSWORD")}
        if not all(environ.values()):
            raise SystemExit("--part redis needs FEAST_REDIS_HOST and REDIS_PASSWORD (the job environment of lane B's service)")
        shared = RealServer(environ)

        def make() -> Any:
            shared.reset()
            return shared

    return {name: (bind(function, make), modes) for name, (function, modes) in FAKE_CASES.items()}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--part", choices=("fake", "redis", "image"), required=True)
    parser.add_argument("--case", action="append", default=[])
    parser.add_argument("--report-dir")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.part == "image":
        from scripts.features import online_image_cases

        cases = online_image_cases.IMAGE_CASES
    else:
        cases = cases_for(args.part)
    report = run_cases(cases, args.case)
    path = write_report(f"online_{args.part}", report, Path(args.report_dir) if args.report_dir else None)
    for item in report["cases"]:
        sys.stdout.write(
            f"{item['verdict'].upper():4} {item['case']} ({item['checks']} checks){'' if item['verdict'] == 'pass' else ': ' + '; '.join(item['failed'])}\n"
        )
    sys.stdout.write(f"{report['verdict'].upper()} report: {path}\n")
    return 0 if report["verdict"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
