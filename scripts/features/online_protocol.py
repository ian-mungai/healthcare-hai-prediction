"""The writer protocol of the online feature store (design p3c sections 4.2 to 4.5).

Redis (lane B's service ``redis:6379``, nothing persisted, ``noeviction``) holds three databases: 0 stays empty, 1 holds only Feast's entity hashes, 2 holds
the control keys ``hai:fs:lease`` (value: the writer's random ``owner_token``) and ``hai:fs:marker`` (a hash: ``state`` ``loading`` or ``ready``, ``load_id``,
``owner_token``, ``feature_run_id``, ``snapshot_set_sha256``, ``feature_schema_sha256``, ``entity_count``, ``materialized_at``, ``feast_version``).

No counter lives in Redis. ``owner_token`` and ``load_id`` are UUID4 values the writer makes, so a restart, which empties every database, cannot make a new
value equal an old one. Every compare-and-act step is one Lua script, run in database 2, with one ``SELECT 1`` inside ``begin_load`` for the flush (design F14;
spike S1 confirms on 8.10.1 that the caller's database stays 2).

``writer_lease(environ)`` is what ``feast_job apply`` takes (lease only). ``run_load`` is the full writer of ``feast_job materialize``: lease, memory and
policy guard, head verification, atomic marker-and-flush, Feast materialize, entity-only counts, samples, head verification, token-checked ``publish``, release.
The marker stays ``loading`` or absent after any failure. Messages and the marker carry names, counts and hashes only, never the password or a value.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib
import re
import sys
import uuid
from collections.abc import Callable, Iterator, Mapping, MutableMapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from scripts.features import publication_reads as reads
from scripts.features.publication_reads import ReadError

FEATURE_DB = 1
CONTROL_DB = 2
PORT = 6379
LEASE_KEY = "hai:fs:lease"
MARKER_KEY = "hai:fs:marker"
LEASE_MS = 3_900_000  # longer than the template's hard limit of 3,600 s
SOCKET_TIMEOUT = 5.0
SERVING_TIMEOUT = 1.0
MEMORY_SHARE = 0.60
# Provisional bytes per field (hash field, protobuf value and container overhead) until the first full-width fixture run measures it (case E28).
BYTES_PER_FIELD = 48
SCAN_COUNT = 1000
SAFE_TEXT = re.compile(r"[A-Za-z0-9:_.-]{1,64}")

# The three compare-and-act scripts. Each runs in database 2 (KEYS[1] is the lease, KEYS[2] the marker). The in-process fake of the cases
# (online_cases.FakeServer) is a line-by-line translation of exactly these sources and is looked up by the exact text.
BEGIN_LOAD = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[2])
redis.call('HSET', KEYS[2], 'state', 'loading', 'load_id', ARGV[2], 'owner_token', ARGV[1], unpack(ARGV, 3))
redis.call('SELECT', 1)
redis.call('FLUSHDB')
return 1
"""
PUBLISH = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[2], 'load_id') ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[2], 'state', 'ready', unpack(ARGV, 3))
return 1
"""
RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"""


class OnlineError(ReadError):
    """A Redis step is refused or fails; the message names the step and the error type, never the password or a value."""


class RedisClient(Protocol):
    """The part of redis-py's client that this module uses."""

    def set(self, name: str, value: str, *, nx: bool = False, px: int | None = None) -> Any: ...

    def hgetall(self, name: str) -> Mapping[Any, Any]: ...

    def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Any: ...

    def dbsize(self) -> int: ...

    def config_get(self, pattern: str) -> Mapping[Any, Any]: ...

    def scan_iter(self, *, count: int | None = None) -> Iterator[Any]: ...


class LoadHooks(Protocol):
    """What the load needs from Feast and the catalog; the cases pass recording stand-ins."""

    def verify_heads(self, moment: str) -> None: ...

    def materialize(self, start: datetime, end: datetime) -> None: ...

    def parse_key(self, key: bytes) -> bool: ...

    def verify_samples(self) -> None: ...


@dataclass(frozen=True)
class LoadPlan:
    """What one load writes and what it must find: the selected publication, the schema digest and the entity and column counts."""

    feature_run_id: str
    snapshot_set_sha256: str
    feature_schema_sha256: str
    entity_count: int
    predictor_columns: int
    window_start: datetime
    window_end: datetime
    feast_version: str


@dataclass(frozen=True)
class LoadResult:
    load_id: str
    entity_count: int


def call[T](step: str, action: Callable[[], T]) -> T:
    """Run one Redis call; any failure becomes an OnlineError that names the step and the error type only (no password, no value)."""
    try:
        return action()
    except OnlineError:
        raise
    except Exception as error:
        raise OnlineError(f"Redis failed at {step} ({type(error).__name__})") from None


def no_retry() -> Any:
    """An explicit retry policy of zero retries: redis-py 7.4.1 retries three times by default, which would turn one wait into several (design 4.4, 4.5)."""
    return importlib.import_module("redis.retry").Retry(importlib.import_module("redis.backoff").NoBackoff(), 0)


def connect(environ: Mapping[str, str], database: int, *, timeout: float = SOCKET_TIMEOUT, decode: bool = True) -> RedisClient:
    """A redis-py client for one database of ``FEAST_REDIS_HOST`` with the password of ``REDIS_PASSWORD`` and bounded connect and command waits."""
    missing = sorted(name for name in ("FEAST_REDIS_HOST", "REDIS_PASSWORD") if not environ.get(name))
    if missing:
        raise OnlineError(f"Redis settings are unset: {', '.join(missing)}")
    client: RedisClient = importlib.import_module("redis").Redis(
        host=environ["FEAST_REDIS_HOST"],
        port=PORT,
        db=database,
        password=environ["REDIS_PASSWORD"],
        socket_timeout=timeout,
        socket_connect_timeout=timeout,
        retry=no_retry(),
        decode_responses=decode,
    )
    return client


@contextlib.contextmanager
def feast_job_redis(environ: Mapping[str, str], *, timeout: float = SOCKET_TIMEOUT) -> Iterator[None]:
    """Give every Feast Redis online store in this process one client with explicit bounds and zero retries, for the length of the block.

    Feast builds its own client from ``feature_store.yaml`` (5 s timeouts) and redis-py would retry it three times. Feast keeps the client in
    ``RedisOnlineStore._client`` and uses it when it is set (Feast 0.66.0, ``_get_client``), so setting it on the class covers the stores Feast creates itself,
    such as the one inside ``apply_total``.
    """
    store_class = importlib.import_module("feast.infra.online_stores.redis").RedisOnlineStore
    previous = store_class.__dict__.get("_client")
    store_class._client = connect(environ, FEATURE_DB, timeout=timeout, decode=False)
    try:
        yield
    finally:
        store_class._client = previous


def safe(value: Any) -> str:
    """A marker value that is shown in a message only when it has the plain form of an ID; otherwise ``unknown``."""
    text = value.decode("ascii", "replace") if isinstance(value, bytes) else str(value)
    return text if SAFE_TEXT.fullmatch(text) else "unknown"


def read_marker(control: RedisClient) -> dict[str, str]:
    """The marker hash as text; empty when there is none."""
    raw = call("marker read", lambda: control.hgetall(MARKER_KEY))
    return {(k.decode() if isinstance(k, bytes) else str(k)): (v.decode() if isinstance(v, bytes) else str(v)) for k, v in raw.items()}


def acquire(control: RedisClient, token: str, lease_ms: int = LEASE_MS) -> None:
    """Take the lease at once or fail naming the feature run that the marker records (the holder)."""
    if not call("lease acquire", lambda: control.set(LEASE_KEY, token, nx=True, px=lease_ms)):
        holder = safe(read_marker(control).get("feature_run_id", "none"))
        raise OnlineError(f"the writer lease is held by another writer (marker feature run {holder})")


def release(control: RedisClient, token: str) -> bool:
    """Delete the lease only if it still equals ``token``; True when this call deleted it."""
    return bool(call("lease release", lambda: control.eval(RELEASE, 1, LEASE_KEY, token)))


@contextlib.contextmanager
def writer_lease(environ: MutableMapping[str, str], *, control: RedisClient | None = None, lease_ms: int = LEASE_MS) -> Iterator[None]:
    """The lease around ``feast apply``: take it at once or fail naming the holder; release it only if still ours (a failed release is reported)."""
    client = control if control is not None else connect(environ, CONTROL_DB)
    token = str(uuid.uuid4())
    acquire(client, token, lease_ms)
    try:
        yield
    finally:
        try:
            release(client, token)
        except OnlineError as error:
            sys.stderr.write(f"{error}; the lease expires by itself\n")


def memory_guard(control: RedisClient, plan: LoadPlan, bytes_per_field: int = BYTES_PER_FIELD) -> None:
    """Refuse a policy other than ``noeviction``, no memory limit, or an estimate above 60 % of ``maxmemory`` (design 4.3 item 2)."""
    policy = call("policy read", lambda: control.config_get("maxmemory-policy")).get("maxmemory-policy")
    limit = int(call("maxmemory read", lambda: control.config_get("maxmemory")).get("maxmemory", 0))
    if policy != "noeviction":
        raise OnlineError("the online store policy is not noeviction")
    if limit <= 0:
        raise OnlineError("the online store has no memory limit")
    estimate = plan.entity_count * plan.predictor_columns * bytes_per_field
    if estimate > MEMORY_SHARE * limit:
        raise OnlineError(f"the estimated size ({estimate} bytes for {plan.entity_count} entities) is above 60 % of maxmemory ({limit} bytes)")


def begin_load(control: RedisClient, token: str, load_id: str, plan: LoadPlan) -> None:
    """Replace the marker (``loading``, new ``load_id``) and flush database 1 in one script, only while this writer holds the lease."""
    extra = ["feature_run_id", plan.feature_run_id, "snapshot_set_sha256", plan.snapshot_set_sha256, "feature_schema_sha256", plan.feature_schema_sha256]
    if not call("begin load", lambda: control.eval(BEGIN_LOAD, 2, LEASE_KEY, MARKER_KEY, token, load_id, *extra)):
        raise OnlineError("the flush was refused: this writer no longer holds the lease")


def publish(control: RedisClient, token: str, load_id: str, plan: LoadPlan, now: datetime) -> None:
    """Set the marker ``ready`` only while this writer holds the lease and the marker still carries this load's ``load_id``."""
    fields = [
        "feature_run_id",
        plan.feature_run_id,
        "snapshot_set_sha256",
        plan.snapshot_set_sha256,
        "feature_schema_sha256",
        plan.feature_schema_sha256,
        "entity_count",
        str(plan.entity_count),
        "materialized_at",
        now.astimezone(UTC).isoformat(),
        "feast_version",
        plan.feast_version,
    ]
    if not call("publish", lambda: control.eval(PUBLISH, 2, LEASE_KEY, MARKER_KEY, token, load_id, *fields)):
        raise OnlineError("the publish was refused: the lease or the load is no longer this writer's")


def entity_counts(features: RedisClient, plan: LoadPlan, parse_key: Callable[[bytes], bool]) -> int:
    """Count entity keys in database 1 only (control keys live in database 2); the count must equal the entity count and every key must parse."""
    size = call("entity count", features.dbsize)
    if size != plan.entity_count:
        raise OnlineError(f"database 1 holds {size} keys; the publication has {plan.entity_count} entities")
    keys = call("key scan", lambda: list(features.scan_iter(count=SCAN_COUNT)))
    bad = sum(1 for key in keys if not parse_key(key if isinstance(key, bytes) else str(key).encode()))
    if len(keys) != size or bad:
        raise OnlineError(f"database 1: {len(keys)} keys scanned, {bad} are not Feast entity keys of the project")
    return size


def run_load(
    control: RedisClient,
    features: RedisClient,
    plan: LoadPlan,
    hooks: LoadHooks,
    *,
    deadline: Callable[[str], None],
    lease_ms: int = LEASE_MS,
    new_id: Callable[[], str] = lambda: str(uuid.uuid4()),
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> LoadResult:
    """The writer protocol of design 4.3, steps 1 to 9. After a failure the marker stays ``loading`` (or absent) and the lease is released if still ours."""
    token, load_id = new_id(), new_id()
    deadline("the lease")
    acquire(control, token, lease_ms)
    try:
        deadline("the memory guard")
        memory_guard(control, plan)
        deadline("the first head check")
        hooks.verify_heads("before the flush")
        deadline("the flush")
        begin_load(control, token, load_id, plan)
        deadline("materialize")
        hooks.materialize(plan.window_start, plan.window_end)
        deadline("the counts")
        count = entity_counts(features, plan, hooks.parse_key)
        deadline("the samples")
        hooks.verify_samples()
        deadline("the last head check")
        hooks.verify_heads("after the last write")
        deadline("publish")
        publish(control, token, load_id, plan, now())
    finally:
        try:
            release(control, token)
        except OnlineError as error:
            sys.stderr.write(f"{error}; the lease expires by itself\n")
    return LoadResult(load_id, count)


def readiness(control: RedisClient, features: RedisClient, catalog: reads.IcebergCatalog, publication: reads.Publication) -> tuple[bool, str]:
    """Whether the online store is ready for ``publication`` (design 4.4 and 3.5).

    The marker is read twice around the other checks and must be ``ready`` with the same ``load_id`` both times, so a restart and a partial reload between
    the reads cannot pass. In between: the marker names this publication (run and snapshot-set digest), ``DBSIZE`` of database 1 equals
    ``entity_count``, and the ``main`` snapshot of every table of the publication is still the one it names.
    """
    first = read_marker(control)
    if first.get("state") != "ready" or not first.get("load_id"):
        return False, f"the marker is {safe(first.get('state', 'absent'))}"
    if first.get("feature_run_id") != publication.run_id or first.get("snapshot_set_sha256") != publication.snapshot_set_sha256:
        return False, "the marker names another publication than the selected one"
    size = call("entity count", features.dbsize)
    if str(size) != first.get("entity_count"):
        return False, "the number of entity keys differs from the marker"
    try:
        reads.verify_heads(catalog, publication, "readiness")
    except ReadError as error:
        return False, str(error)
    second = read_marker(control)
    if second.get("state") != "ready" or second.get("load_id") != first.get("load_id"):
        return False, "the marker changed during the check"
    return True, "ready"


def readiness_for_head(control: RedisClient, features: RedisClient, catalog: reads.IcebergCatalog, namespace: str) -> tuple[bool, str]:
    """``readiness`` for the publication head of the namespace, selected once."""
    return readiness(control, features, catalog, reads.read_publication(catalog, namespace))


def database_digest(features: Any) -> str:
    """SHA-256 over the sorted keys and sorted hash fields of database 1: equal for two loads of the same set (retry equality, failure mode D2)."""
    digest = hashlib.sha256()
    for key in sorted(features.scan_iter(count=SCAN_COUNT)):
        digest.update(key if isinstance(key, bytes) else str(key).encode())
        for field, value in sorted(features.hgetall(key).items()):
            for part in (field, value):
                digest.update(part if isinstance(part, bytes) else str(part).encode())
    return digest.hexdigest()
