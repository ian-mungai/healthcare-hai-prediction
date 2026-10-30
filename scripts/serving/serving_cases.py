"""End-to-end cases of the model API, driven through the service entry point over the project's Redis stand-in, the real writer protocol and file
manifests.

The model, the registry and the prediction sink are explicit stand-ins. FastAPI, the MLflow loader, the PyIceberg sink and the S3 manifest store run
only in the serving image and are not exercised here. Canary proof is replayed requests, never live traffic.
"""

from __future__ import annotations

import ast
import contextlib
import io
import json
import logging
import re
import tempfile
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from scripts.features import online_cases
from scripts.features import online_protocol as online
from scripts.features.feast_harness import Checker
from scripts.serving import canary, deploy, keys, limiter, manifests, metrics, predictions, replay, service

ROOT = Path(__file__).resolve().parents[2]
HAI = "HAI_1"
KEY_A = "sk-test-0123456789-do-not-log"
KEY_B = "sk-test-other-key"
RUN_1 = "20261030T100000Z-aaaaaaa1"
RUN_2 = "20261030T110000Z-aaaaaaa2"
RUN_3 = "20261030T120000Z-aaaaaaa3"
PRODUCTION, CANDIDATE = "3", "4"
CONTRACT = json.loads((ROOT / "data_contracts/serving/predictions.json").read_text())


class NotFound(Exception):
    error_code = "RESOURCE_DOES_NOT_EXIST"


class Registry:
    """A stand-in for the MLflow registry client with the two read calls deploy may make. ``alias_writes`` must stay zero."""

    def __init__(self, production: str | None = PRODUCTION, versions: Sequence[str] = (PRODUCTION, CANDIDATE, "5")) -> None:
        self.aliases = {} if production is None else {"production": production}
        self.versions = {version: {"bundle_sha256": "f" * 64} for version in versions}
        self.alias_writes = 0

    def search_model_versions(self, filter_string: str = "") -> list[Any]:
        return [SimpleNamespace(name=f"hai_model_{HAI}", version=version, run_id=f"run{version}", tags=dict(tags)) for version, tags in self.versions.items()]

    def get_model_version_by_alias(self, name: str, alias: str) -> Any:
        if alias not in self.aliases:
            raise NotFound(alias)
        return SimpleNamespace(name=name, version=self.aliases[alias], tags={})

    def set_registered_model_alias(self, name: str, alias: str, version: str) -> None:
        self.alias_writes += 1


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class Loader:
    """Stub models: the version sets the base of the prediction, so a response names the model that answered. A ``broken`` version raises on predict."""

    def __init__(self) -> None:
        self.broken: set[str] = set()
        self.cost = 0.0
        self.clock: Clock | None = None
        self.unloadable: set[str] = set()
        self.loaded: list[str] = []
        self.released: list[str] = []

    def load(self, hai_type: str, version: str) -> service.LoadedModel:
        if version in self.unloadable:
            raise RuntimeError(f"synthetic load failure for version {version}")
        self.loaded.append(version)

        def predict(row: Mapping[str, Any]) -> float:
            if self.clock is not None:
                self.clock.now += self.cost
            if version in self.broken:
                raise RuntimeError("synthetic prediction failure")
            return float(version) + sum(len(str(value)) for value in row.values()) / 10000

        return service.LoadedModel(version, tuple(online_cases.REFERENCES), online_cases.SCHEMA_HASH, predict)

    def release(self, model: service.LoadedModel) -> None:
        self.released.append(model.version)


class Sink:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.fail = False
        self.timeouts: list[float] = []

    def append(self, rows: Sequence[Mapping[str, Any]], timeout: float) -> None:
        self.timeouts.append(timeout)
        if self.fail:
            raise TimeoutError("synthetic sink timeout")
        self.rows.extend(dict(row) for row in rows)


@dataclass
class World:
    server: Any
    registry: Registry
    store: manifests.FileManifestStore
    loader: Loader
    sink: Sink
    clock: Clock
    api: service.ModelService
    reads: list[str] = field(default_factory=list)
    logs: io.StringIO = field(default_factory=io.StringIO)


def hospital_keys(count: int) -> list[str]:
    return [f"H{number:05d}:2022" for number in range(1, count + 1)]


@contextlib.contextmanager
def world(count: int = 4, rate: int = 600, burst: int = 1000, deployment: bool = True, **options: Any) -> Iterator[World]:
    """A clean serving world: Redis stand-in loaded by the real writer protocol, a deployment written by the real deploy action, a service built over both."""
    server = online_cases.FakeServer()
    names = hospital_keys(count) if count != 4 else list(online_cases.KEYS)
    online_cases.load(server, online_cases.Hooks(server, "v1", names), online_cases.plan_for(count=len(names)))
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        store = manifests.FileManifestStore(Path(folder))
        registry, loader, sink, clock = Registry(), Loader(), Sink(), Clock()
        if deployment:
            deploy.run(
                registry=registry,
                manifests=store,
                hai_type=HAI,
                candidate_version=CANDIDATE,
                percent=options.pop("percent", 20),
                mode="canary",
                deployment_id=RUN_1,
            )
        read_features = options.pop("wrap", lambda inner: inner)(online_cases.reader(server))
        counted: list[str] = []

        def reading(references: list[str], key: str) -> Mapping[str, Sequence[Any]]:
            counted.append(key)
            return read_features(references, key)

        api = service.ModelService(
            keys=keys.KeyRing.from_hashes({"ops": keys.hash_key(KEY_A), "other": keys.hash_key(KEY_B)}),
            limiter=options.pop("limiter", None) or limiter.RateLimiter(rate_per_minute=rate, burst=burst, clock=clock),
            read_marker=online_cases.markers(server),
            read_features=reading,
            manifests=store,
            loader=loader,
            buffer=predictions.PredictionBuffer(
                sink, columns=CONTRACT["columns"], capacity=options.get("capacity", 1000), batch=50, deadline_seconds=5.0, clock=clock
            ),
            clock=clock,
            new_id=iter(f"req{number:04d}" for number in range(10_000)).__next__,
            hai_types=(HAI,),
            refresh_seconds=30.0,
            failure_limit=3,
        )
        loader.clock = clock
        world_ = World(server, registry, store, loader, sink, clock, api, counted)
        yield world_


def ask(w: World, key: str = online_cases.KEYS[0], presented: str | None = KEY_A, **body: Any) -> service.Reply:
    ccn, _, year = key.partition(":")
    return w.api.predict(presented, None, {"hai_type": HAI, "ccn": ccn, "window_year": int(year)} | body)


@contextlib.contextmanager
def captured_logs() -> Iterator[io.StringIO]:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.DEBUG)
    root = logging.getLogger()
    previous = root.level
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    try:
        yield stream
    finally:
        root.removeHandler(handler)
        root.setLevel(previous)


def rows_of(w: World) -> list[dict[str, Any]]:
    w.api.buffer.flush()
    return w.sink.rows


def case_ok(c: Checker) -> None:
    """200: a valid key and a stored hospital-window answer with the stable or canary model, and one prediction row that follows the contract."""
    with world() as w:
        reply = ask(w)
        c.check("a valid request answers 200", reply.status == 200)
        body = reply.body
        c.check(
            "the answer names the model version and where it was routed",
            body.get("model_version") in (PRODUCTION, CANDIDATE) and body.get("served_by") in ("stable", "canary"),
        )
        c.check(
            "the answer carries the features date and the schema hash of the contract",
            body.get("features_as_of") == "2022-01-01" and body.get("feature_schema_sha256") == online_cases.SCHEMA_HASH,
        )
        c.check("the prediction is a number", isinstance(body.get("prediction"), float))
        c.check("the request id is echoed in a header", reply.headers.get("X-Request-ID") == body.get("request_id"))
        rows = rows_of(w)
        c.check("one row was appended", len(rows) == 1)
        row = rows[0] if rows else {}
        c.check("the row has exactly the contract columns", set(row) == set(CONTRACT["columns"]))
        c.check("the row names the key by identifier and never holds the key", row.get("api_key_id") == "ops" and KEY_A not in json.dumps(row, default=str))
        c.check("the row matches the answer", row.get("prediction") == body.get("prediction") and row.get("model_version") == body.get("model_version"))
        c.check("health reports the loaded versions", w.api.health().body.get(HAI, {}).get("stable") == PRODUCTION)


def case_unauthorized(c: Checker) -> None:
    """401: a missing, empty or wrong key is refused alike, before any feature read, and no key reaches a log or an answer."""
    with captured_logs() as logs, world() as w:
        answers = [ask(w, presented=None), ask(w, presented=""), ask(w, presented="sk-test-wrong-key-12345")]
        c.check("a missing, empty and wrong key all answer 401", [a.status for a in answers] == [401, 401, 401])
        c.check("the three refusals are identical", len({json.dumps(a.body, sort_keys=True) for a in answers}) == 1)
        c.check("a refused request reads no feature", w.reads == [])
        c.check("a refused request leaves no row", rows_of(w) == [])
        ask(w)
        text = logs.getvalue() + json.dumps([a.body for a in answers])
        c.check("no key appears in a log line or an answer", KEY_A not in text and "sk-test-wrong-key-12345" not in text and keys.hash_key(KEY_A) not in text)
        c.check("the key ring does not print a key or a hash", KEY_A not in repr(w.api.keys) and keys.hash_key(KEY_A) not in repr(w.api.keys))


def case_rate_limit(c: Checker) -> None:
    """429: a key past its burst is refused with a retry time, other keys are not, and the bucket refills with time."""
    with world(rate=60, burst=3) as w:
        first = [ask(w).status for _ in range(4)]
        c.check("three requests pass and the fourth is limited", first == [200, 200, 200, 429])
        limited = ask(w)
        c.check("the refusal carries a retry time", limited.status == 429 and float(limited.headers.get("Retry-After", "0")) >= 1)
        c.check("another key is not limited", ask(w, presented=KEY_B).status == 200)
        w.clock.now += 1.0
        c.check("one second refills one token at 60 a minute", ask(w).status == 200 and ask(w).status == 429)
        before = len(w.reads)
        ask(w)
        c.check("a limited request reads no feature", len(w.reads) == before)
        unknown_before = w.api.limiter.tracked()
        for index in range(50):
            ask(w, presented=f"sk-test-unknown-{index}")
        c.check("unknown keys create no bucket", w.api.limiter.tracked() == unknown_before)


def case_validation_and_unknown_hospital(c: Checker) -> None:
    """422 for a malformed request, 404 for a hospital-window the store does not hold, and neither leaves a row."""
    with world() as w:
        c.check("an unknown hospital answers 404", ask(w, "ZZZ999:2022").status == 404)
        c.check("a year the store does not hold answers 404", ask(w, "AAA001:2023").status == 404)
        c.check("a malformed ccn answers 422", ask(w, "AAA:2022").status == 422)
        c.check("a boolean year answers 422", w.api.predict(KEY_A, None, {"hai_type": HAI, "ccn": "AAA001", "window_year": True}).status == 422)
        c.check("an unsupported HAI type answers 422", ask(w, hai_type="HAI_9").status == 422)
        c.check("an extra field answers 422", ask(w, surprise=1).status == 422)
        c.check("a body that is not an object answers 422", w.api.predict(KEY_A, None, ["AAA001"]).status == 422)
        c.check(
            "a request id that is not safe text is replaced",
            w.api.predict(KEY_A, "bad id\n", {"hai_type": HAI, "ccn": "AAA001", "window_year": 2022}).body.get("request_id") != "bad id\n",
        )
        c.check("the refusals left one row at most (the valid request)", len(rows_of(w)) == 1)


def case_missing_feature(c: Checker) -> None:
    """503: a feature the store does not answer, a store that is not ready or a changed schema gives no partial prediction and no row."""
    dropped = online_cases.REFERENCES[1]

    def without(inner: Any) -> Any:
        def read(references: list[str], key: str) -> Mapping[str, Sequence[Any]]:
            answer = dict(inner(references, key))
            answer.pop(dropped.replace(":", "__", 1), None)
            return answer

        return read

    with world(wrap=without) as w:
        reply = ask(w)
        c.check("a feature missing from the answer gives 503", reply.status == 503 and "prediction" not in reply.body)
        c.check("a missing feature leaves no row", rows_of(w) == [])
    with world() as w:
        w.server.write_hash(online.CONTROL_DB, online.MARKER_KEY, {"state": "loading"})
        c.check("a store that is loading gives 503", ask(w).status == 503)
        w.server.write_hash(online.CONTROL_DB, online.MARKER_KEY, {"state": "ready", "load_id": "x", "feature_schema_sha256": online_cases.OTHER_HASH})
        c.check("a store with another feature schema gives 503", ask(w).status == 503)
        c.check("no row was written for either", rows_of(w) == [])


def case_split(c: Checker) -> None:
    """The split is a function of the deployment and the hospital-window: pinned values, repeats, two processes and a replayed corpus agree."""
    c.check("the bucket of one key is pinned", canary.bucket("dep1", HAI, "H00001", 2022) == 5291 and canary.bucket("dep1", HAI, "H00002", 2022) == 5788)
    wide = canary.Deployment(HAI, "dep1", PRODUCTION, CANDIDATE, 20)
    c.check("the pinned bucket routes by the percent", canary.route(wide, "H00001", 2022) == "stable" and canary.route(wide, "H00002", 2022) == "stable")
    corpus = [(f"H{number:05d}", 2022) for number in range(1, 10_001)]
    share = sum(canary.route(wide, ccn, year) == "canary" for ccn, year in corpus)
    c.check("a replayed corpus of 10,000 keys sends 1,998 to the canary at 20 percent", share == 1998)
    c.check(
        "zero candidate means no canary",
        all(canary.route(canary.Deployment(HAI, "dep1", PRODUCTION, None, 0), ccn, year) == "stable" for ccn, year in corpus[:500]),
    )
    with world(count=200, percent=20) as w:
        seen = {}
        for key in hospital_keys(200):
            seen[key] = ask(w, key).body
        counts = sum(body.get("served_by") == "canary" for body in seen.values())
        c.check("200 replayed requests through the service split between 23 and 57 to the canary", 23 <= counts <= 57)
        again = {key: ask(w, key).body.get("model_version") for key in hospital_keys(200)}
        c.check("a second replay gives every hospital the same version", again == {key: body["model_version"] for key, body in seen.items()})
        with world(count=200, percent=20) as other:
            c.check(
                "a second process gives every hospital the same version", {key: ask(other, key).body["model_version"] for key in hospital_keys(200)} == again
            )
        c.check(
            "the answer routes as the router does",
            all(body["served_by"] == canary.route(w.api.deployment(HAI), key.split(":")[0], 2022) for key, body in seen.items()),
        )


def case_version_failure(c: Checker) -> None:
    """One version failing never takes the other down: a failing canary falls back and is ejected, a failing stable fails only its own requests."""
    keys_ = hospital_keys(40)
    with world(count=40, percent=50) as w:
        routed = {key: canary.route(w.api.deployment(HAI), key.split(":")[0], 2022) for key in keys_}
        canary_keys = [key for key, kind in routed.items() if kind == "canary"]
        stable_keys = [key for key, kind in routed.items() if kind == "stable"]
        c.check("the corpus holds both kinds", len(canary_keys) >= 5 and len(stable_keys) >= 5)
        w.loader.broken.add(CANDIDATE)
        first = ask(w, canary_keys[0])
        c.check(
            "a failing canary falls back to the stable model with 200",
            first.status == 200 and first.body["model_version"] == PRODUCTION and first.body["fallback"] is True,
        )
        for key in canary_keys[1:4]:
            ask(w, key)
        c.check("three failures eject the canary", w.api.health().body[HAI]["candidate"] == "ejected")
        before = len(w.loader.loaded)
        after = ask(w, canary_keys[4])
        c.check("an ejected canary is not asked again", after.status == 200 and after.body["fallback"] is True and after.body["served_by"] == "stable")
        c.check("every stable request still answers 200 with the stable model", all(ask(w, key).body["model_version"] == PRODUCTION for key in stable_keys))
        c.check(
            "the fallback rows record the routed version and the served one",
            any(row["routed_version"] == CANDIDATE and row["model_version"] == PRODUCTION and row["fallback"] for row in rows_of(w)),
        )
        c.check("no model was loaded again by failing", len(w.loader.loaded) == before)
    with world(count=40, percent=50) as w:
        routed = {key: canary.route(w.api.deployment(HAI), key.split(":")[0], 2022) for key in keys_}
        w.loader.broken.add(PRODUCTION)
        stable_failed = [ask(w, key) for key, kind in routed.items() if kind == "stable"]
        canary_served = [ask(w, key) for key, kind in routed.items() if kind == "canary"]
        c.check("a failing stable model gives 503 for its own requests", bool(stable_failed) and all(r.status == 503 for r in stable_failed))
        c.check(
            "the canary still answers its own requests",
            bool(canary_served) and all(r.status == 200 and r.body["model_version"] == CANDIDATE for r in canary_served),
        )
        c.check("a failed answer leaves no prediction row", all(row["model_version"] == CANDIDATE for row in rows_of(w)))
    with world(count=40, percent=50, deployment=False) as w:
        w.loader.unloadable.add(CANDIDATE)
        deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=CANDIDATE, percent=50, mode="canary", deployment_id=RUN_1)
        w.api.refresh()
        statuses = {ask(w, key).status for key in keys_}
        c.check(
            "a canary that cannot load leaves the stable model serving every request", statuses == {200} and w.api.health().body[HAI]["candidate"] == "failed"
        )


def case_rollback(c: Checker) -> None:
    """A rollback deployment removes the canary after the next refresh, releases its model and leaves the production alias untouched."""
    with world(count=40, percent=50) as w:
        c.check("the canary serves before the rollback", {ask(w, key).body["served_by"] for key in hospital_keys(40)} == {"stable", "canary"})
        outcome = deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=None, percent=0, mode="rollback", deployment_id=RUN_2)
        c.check("the rollback writes a deployment with no candidate", outcome.status == "written" and outcome.deployment.candidate_version is None)
        c.check(
            "a request before the refresh time still sees the old deployment",
            {ask(w, key).body["served_by"] for key in hospital_keys(40)} == {"stable", "canary"},
        )
        w.clock.now += 31
        replies = [ask(w, key) for key in hospital_keys(40)]
        c.check(
            "after the refresh time every request is served by the stable model",
            all(r.status == 200 and r.body["served_by"] == "stable" and r.body["model_version"] == PRODUCTION for r in replies),
        )
        c.check("the canary model was released", w.loader.released == [CANDIDATE] and w.api.health().body[HAI]["candidate"] is None)
        c.check("the deployment id of the answers is the rollback's", {row["deployment_id"] for row in rows_of(w)[-40:]} == {RUN_2})
        c.check("the production alias was never written", w.registry.alias_writes == 0)
    with world() as w:
        current = w.store.read_active(HAI)
        w.store.replace_active(HAI, b"{ not json", None if current is None else current.token)
        w.clock.now += 31
        c.check("an unreadable deployment keeps the last good one", ask(w).status == 200 and w.api.health().body[HAI]["deployment_id"] == RUN_1)


def case_deploy(c: Checker) -> None:
    """Deploy reads the production alias and never moves it, retries have no second effect and every refusal writes nothing."""
    with world(deployment=False) as w:
        first = deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=CANDIDATE, percent=10, mode="canary", deployment_id=RUN_1)
        c.check("the first deploy writes", first.status == "written" and first.deployment.stable_version == PRODUCTION)
        stored = w.store.read_active(HAI)
        again = deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=CANDIDATE, percent=10, mode="canary", deployment_id=RUN_1)
        c.check("a retry changes nothing", again.status == "unchanged" and w.store.read_active(HAI) == stored and w.store.writes == 2)
        c.check(
            "a retry under the same id with other parameters is refused",
            refused(
                lambda: deploy.run(
                    registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=CANDIDATE, percent=30, mode="canary", deployment_id=RUN_1
                )
            ),
        )
        c.check("the refused retry changed nothing", w.store.read_active(HAI) == stored)
        c.check(
            "the candidate must differ from production",
            refused(
                lambda: deploy.run(
                    registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=PRODUCTION, percent=10, mode="canary", deployment_id=RUN_2
                )
            ),
        )
        c.check(
            "an unknown version is refused",
            refused(
                lambda: deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version="99", percent=10, mode="canary", deployment_id=RUN_2)
            ),
        )
        w.registry.versions["5"] = {}
        c.check(
            "a version without a completed bundle is refused",
            refused(
                lambda: deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version="5", percent=10, mode="canary", deployment_id=RUN_2)
            ),
        )
        for percent in (0, 51, -1):
            c.check(f"a canary percent of {percent} is refused", refused(partial(canary_deploy, w, percent)))
        c.check(
            "a rollback with a candidate is refused",
            refused(
                lambda: deploy.run(
                    registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=CANDIDATE, percent=0, mode="rollback", deployment_id=RUN_2
                )
            ),
        )
        w.registry.aliases.clear()
        c.check(
            "a deploy with no production model is refused",
            refused(
                lambda: deploy.run(
                    registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=CANDIDATE, percent=10, mode="canary", deployment_id=RUN_3
                )
            ),
        )
        c.check("the refusals changed nothing and the alias was never written", w.store.read_active(HAI) == stored and w.registry.alias_writes == 0)
        w.registry.aliases["production"] = PRODUCTION
        c.check(
            "a deployment id that is not a run id is refused",
            refused(
                lambda: deploy.run(
                    registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=CANDIDATE, percent=10, mode="canary", deployment_id="../x"
                )
            ),
        )
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        store = manifests.FileManifestStore(Path(folder))
        store.put_history(HAI, RUN_1, b"a")
        c.check("history refuses other content under an existing id", refused(lambda: store.put_history(HAI, RUN_1, b"b")))
        store.replace_active(HAI, b"a", None)
        c.check("a replace with a stale token is refused", refused(lambda: store.replace_active(HAI, b"b", "stale")))


def canary_deploy(w: World, percent: int) -> deploy.Outcome:
    return deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=CANDIDATE, percent=percent, mode="canary", deployment_id=RUN_2)


class CrashingStore(manifests.FileManifestStore):
    """A file store whose next replace of the active record fails, as a deploy that stopped between its two writes."""

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.crash_next_replace = False

    def replace_active(self, hai_type: str, body: bytes, expected_token: str | None) -> None:
        if self.crash_next_replace:
            self.crash_next_replace = False
            raise OSError("synthetic stop between the history write and the active write")
        super().replace_active(hai_type, body, expected_token)


def case_deploy_order(c: Checker) -> None:
    """An older deploy that is retried never replaces a later deployment, and a deploy that stopped between its writes finishes on its retry."""
    with world(deployment=False) as w:

        def canary_run(run: str, version: str = CANDIDATE, percent: int = 10) -> deploy.Outcome:
            return deploy.run(
                registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=version, percent=percent, mode="canary", deployment_id=run
            )

        def rollback_run(run: str) -> deploy.Outcome:
            return deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=None, percent=0, mode="rollback", deployment_id=run)

        canary_run(RUN_1)
        rollback_run(RUN_2)
        after_rollback = w.store.read_active(HAI)
        c.check("the rollback is active", after_rollback is not None and canary.Deployment.parse(after_rollback.body).candidate_version is None)
        c.check("a retry of the older canary deploy is refused", refused(lambda: canary_run(RUN_1)))
        c.check("the refused retry left the rollback active", w.store.read_active(HAI) == after_rollback)
        c.check("a retry of the rollback changes nothing", rollback_run(RUN_2).status == "unchanged" and w.store.read_active(HAI) == after_rollback)
        later = canary_run(RUN_3, "5", 20)
        c.check("a new run still deploys after the refusal", later.status == "written" and later.deployment.candidate_version == "5")
        c.check(
            "each deployment is numbered one above the one it replaced",
            [d.sequence for d in (canary.Deployment.parse(w.store.read_active(HAI).body),)] == [3] if w.store.read_active(HAI) else False,
        )
        c.check("a retry of the oldest deploy is still refused", refused(lambda: canary_run(RUN_1)))
        c.check("the production alias was never written", w.registry.alias_writes == 0)
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        store = CrashingStore(Path(folder))
        registry = Registry()
        store.crash_next_replace = True
        stopped = False
        try:
            deploy.run(registry=registry, manifests=store, hai_type=HAI, candidate_version=CANDIDATE, percent=10, mode="canary", deployment_id=RUN_1)
        except OSError:
            stopped = True
        c.check("a deploy that stops between its writes leaves no active record", stopped and store.read_active(HAI) is None)
        finished = deploy.run(registry=registry, manifests=store, hai_type=HAI, candidate_version=CANDIDATE, percent=10, mode="canary", deployment_id=RUN_1)
        active = store.read_active(HAI)
        c.check(
            "its retry finishes the same deployment",
            finished.status == "written" and active is not None and canary.Deployment.parse(active.body).deployment_id == RUN_1,
        )
        registry.aliases["production"] = "5"
        c.check(
            "a retry after the production alias moved keeps the recorded stable version",
            deploy.run(registry=registry, manifests=store, hai_type=HAI, candidate_version=CANDIDATE, percent=10, mode="canary", deployment_id=RUN_1).status
            == "unchanged",
        )
        c.check(
            "the stable version is the one recorded at the first attempt",
            active is not None and canary.Deployment.parse(active.body).stable_version == PRODUCTION,
        )


def refused(call: Callable[[], object]) -> bool:
    try:
        call()
    except (deploy.DeployRefused, manifests.ManifestConflict):
        return True
    except Exception:
        return False
    return False


def case_predictions(c: Checker) -> None:
    """The prediction append never fails a request, retries without duplicates, is bounded and carries a deadline."""
    with world(capacity=5) as w:
        w.sink.fail = True
        replies = [ask(w, key) for key in list(online_cases.KEYS) * 2]
        c.check("a failing sink does not fail a request", all(r.status == 200 for r in replies))
        w.api.buffer.flush()
        c.check("the failed flush kept the rows, within the capacity", w.api.buffer.pending() == 5 and w.api.buffer.dropped == 3)
        c.check("the flush passed its deadline to the sink", w.sink.timeouts == [5.0])
        w.sink.fail = False
        w.api.buffer.flush()
        c.check("the next flush appends each kept row once", len(w.sink.rows) == 5 and len({row["request_id"] for row in w.sink.rows}) == 5)
        w.api.buffer.flush()
        c.check("a flush with nothing pending appends nothing", len(w.sink.rows) == 5)
    c.check("a row with another column set is refused before it reaches the sink", refused_row())


def refused_row() -> bool:
    buffer = predictions.PredictionBuffer(Sink(), columns=CONTRACT["columns"], capacity=3, batch=1, deadline_seconds=1.0, clock=Clock())
    try:
        buffer.add({"request_id": "x", "api_key": KEY_A})
    except predictions.RowRefused:
        return True
    return False


def case_dag(c: Checker) -> None:
    """serving_deploy_dag: a closed parameter set, the broker chain and no way to ask for the AWS route or a promotion."""
    from orchestration import graph, serving_contract

    ok = {"hai_type": HAI, "candidate_version": CANDIDATE, "percent": 10, "deploy_mode": "canary"}
    c.check("a complete parameter set is accepted", serving_contract.parameters_for("serving_deploy_dag", ok) == ok)
    for name, bad in {
        "an extra parameter": ok | {"alias": "production"},
        "a missing parameter": {key: value for key, value in ok.items() if key != "percent"},
        "an unknown HAI type": ok | {"hai_type": "HAI_9"},
        "a percent over the limit": ok | {"percent": 51},
        "a string percent": ok | {"percent": "10"},
        "a boolean percent": ok | {"percent": True},
        "a zero version": ok | {"candidate_version": "0"},
        "an unknown mode": ok | {"deploy_mode": "promote"},
    }.items():
        c.check(f"{name} is refused", rejected(serving_contract.parameters_for, "serving_deploy_dag", bad))
    c.check(
        "a rollback names no candidate",
        serving_contract.parameters_for("serving_deploy_dag", {"hai_type": HAI, "candidate_version": None, "percent": 0, "deploy_mode": "rollback"})[
            "deploy_mode"
        ]
        == "rollback",
    )
    c.check("a rollback with a candidate is refused", rejected(serving_contract.parameters_for, "serving_deploy_dag", ok | {"deploy_mode": "rollback"}))
    c.check(
        "the chain runs the deploy between the run and its record",
        serving_contract.SERVING_STEPS == ("acquire_run", "snapshot_serving", "serve_deploy", "run_record"),
    )
    c.check("the graph rules know the chain", graph.PIPELINE_CHAINS.get("serving_deploy_dag") == serving_contract.SERVING_STEPS)
    source = ast.parse((ROOT / "dags/serving_deploy_dag.py").read_text())
    calls = [node for node in ast.walk(source) if isinstance(node, ast.Call)]
    c.check("the DAG file builds its tasks through the shared broker runtime", any(getattr(call.func, "id", "") == "build_tasks" for call in calls))
    dag_ids = [kw.value.value for call in calls for kw in call.keywords if kw.arg == "dag_id" and isinstance(kw.value, ast.Constant)]
    c.check(
        "the DAG has the contract's id, no schedule and one active run",
        dag_ids == ["serving_deploy_dag"] and "schedule=None" in ast.unparse(source) and "max_active_runs=1" in ast.unparse(source),
    )


def replay_send(w: World, payload: dict[str, Any]) -> tuple[int, dict[str, str], dict[str, Any]]:
    reply = w.api.predict(KEY_A, None, payload)
    return reply.status, reply.headers, reply.body


def replay_corpus(count: int) -> list[tuple[str, int]]:
    return [(key.split(":")[0], 2022) for key in hospital_keys(count)]


def case_replay_and_compare(c: Checker) -> None:
    """Replayed requests give the split counts and the prediction comparison, wait out a rate limit within a deadline and never print the key."""
    with world(count=200, percent=20) as w:

        def pause(seconds: float) -> None:
            w.clock.now += seconds

        result = replay.run(partial(replay_send, w), replay_corpus(200), HAI, clock=w.clock, sleep=pause, deadline_seconds=60.0)
        c.check("all 200 replayed requests answered 200", result["statuses"] == {"200": 200})
        canary_count = result["served_by"].get("canary", 0)
        c.check("the replay sent between 23 and 57 requests to the canary", 23 <= canary_count <= 57 and canary_count + result["served_by"]["stable"] == 200)
        comparison = result["comparison"]
        c.check(
            "the comparison is made with enough rows on each side",
            comparison["status"] == "ok" and comparison["stable"]["count"] >= 30 and comparison["canary"]["count"] >= 30,
        )
        c.check("disjoint prediction sets give a distance of 1", comparison["ks_statistic"] == 1.0)
        c.check("the mean shift is the gap between the stub models", 0.9 < comparison["mean_shift"] < 1.1)
        c.check("the report holds no key", KEY_A not in json.dumps(result))
    with world(count=4, percent=20) as w:
        small = replay.run(partial(replay_send, w), replay_corpus(4), HAI, clock=w.clock, sleep=lambda seconds: None, deadline_seconds=60.0)
        c.check("too few rows on a side give no comparison", small["comparison"]["status"] == "insufficient")
    with world(count=40, rate=60, burst=5) as w:
        waits: list[float] = []

        def record(seconds: float) -> None:
            waits.append(seconds)
            w.clock.now += seconds

        limited = replay.run(partial(replay_send, w), replay_corpus(40), HAI, clock=w.clock, sleep=record, deadline_seconds=300.0)
        c.check("a rate limit is waited out until every request answers 200", limited["statuses"] == {"200": 40} and len(waits) > 0)
    with world(count=40, rate=60, burst=1) as w:

        def tick(seconds: float) -> None:
            w.clock.now += seconds

        c.check("a replay that cannot finish within its deadline fails", deadline_failed(w, tick))
    c.check("a corpus line that is not a ccn and a year is refused", corpus_refused("AAA001,2022\nnot-a-line\n"))
    c.check("a corpus over the line limit is refused", corpus_refused("AAA001,2022\n" * (replay.MAX_CORPUS + 1)))


def deadline_failed(w: World, tick: Callable[[float], None]) -> bool:
    try:
        replay.run(partial(replay_send, w), replay_corpus(40), HAI, clock=w.clock, sleep=tick, deadline_seconds=3.0)
    except replay.ReplayDeadline:
        return True
    return False


def corpus_refused(text: str) -> bool:
    try:
        replay.parse_corpus(text)
    except ValueError:
        return True
    return False


SAMPLE = re.compile(r"^([a-z_]+)(?:\{([^}]*)\})? (\S+)$")


def scrape(text: str) -> tuple[dict[str, str], list[tuple[str, dict[str, str], float]]]:
    """Parse the exposition text into the TYPE of each family and its samples, refusing a line that is neither a comment nor a sample."""
    types: dict[str, str] = {}
    samples: list[tuple[str, dict[str, str], float]] = []
    for line in text.splitlines():
        if line.startswith("# TYPE "):
            _, _, name, kind = line.split(" ", 3)
            types[name] = kind
        elif line and not line.startswith("#"):
            match = SAMPLE.fullmatch(line)
            if match is None:
                raise ValueError(f"unreadable line {line!r}")
            labels = dict(re.findall(r'([a-z_]+)="((?:[^"\\]|\\.)*)"', match.group(2) or ""))
            samples.append((match.group(1), labels, float(match.group(3))))
    return types, samples


def series(samples: list[tuple[str, dict[str, str], float]], name: str, **labels: str) -> list[float]:
    return [value for sample, found, value in samples if sample == name and all(found.get(key) == wanted for key, wanted in labels.items())]


def case_metrics(c: Checker) -> None:
    """The /metrics text has the names, types and labels of the monitoring contract, counts every answer, bounds its series and holds no key or hospital."""
    contract = json.loads((ROOT / "data_contracts/serving/metrics.json").read_text())["metrics"]
    with world(rate=600, burst=10) as w:
        w.loader.cost = 0.3
        ask(w)
        ask(w, presented=None)
        ask(w, "ZZZ999:2022")
        ask(w, "AAA:2022")
        w.api.health()
        text = w.api.metrics.render()
        types, samples = scrape(text)
        c.check("the families are exactly the contract's, with its types", types == {name: spec["type"] for name, spec in contract.items()})
        counter = [(labels, value) for sample, labels, value in samples if sample == "hai_api_requests_total"]
        c.check(
            "the counter carries the contract's labels and no other",
            all(set(labels) == set(contract["hai_api_requests_total"]["labels"]) for labels, _ in counter),
        )
        for status, expected in (("200", 1), ("401", 1), ("404", 1), ("422", 1)):
            c.check(
                f"one predict answer with status {status} is counted",
                series(samples, "hai_api_requests_total", route="/v1/predict", method="POST", status=status) == [expected],
            )
        c.check("the health answer is counted", series(samples, "hai_api_requests_total", route="/v1/health", method="GET", status="200") == [1])
        buckets = [
            (labels, value) for sample, labels, value in samples if sample == "hai_api_request_duration_seconds_bucket" and labels.get("route") == "/v1/predict"
        ]
        c.check("the histogram buckets carry the route and the bound only", all(set(labels) == {"route", "le"} for labels, _ in buckets))
        ordered = [value for _, value in buckets]
        c.check(
            "the buckets are cumulative and end at +Inf with every predict answer",
            ordered == sorted(ordered) and buckets[-1][0]["le"] == "+Inf" and ordered[-1] == 4,
        )
        c.check(
            "the answer that took 0.3 seconds is above the 0.25 bound and inside the 0.5 bound",
            series(samples, "hai_api_request_duration_seconds_bucket", route="/v1/predict", le="0.25") == [3.0]
            and series(samples, "hai_api_request_duration_seconds_bucket", route="/v1/predict", le="0.5") == [4.0],
        )
        c.check(
            "the sum and the count are the predict answers",
            series(samples, "hai_api_request_duration_seconds_count", route="/v1/predict") == [4.0]
            and abs(series(samples, "hai_api_request_duration_seconds_sum", route="/v1/predict")[0] - 0.3) < 1e-9,
        )
        c.check(
            "a bucket bound sits at one second for the latency alert",
            series(samples, "hai_api_request_duration_seconds_bucket", route="/v1/predict", le="1") == [4.0],
        )
        c.check(
            "only metrics of the contract are emitted",
            {name for name, _, _ in samples if name.startswith("hai_api")}
            <= {
                "hai_api_requests_total",
                "hai_api_request_duration_seconds_bucket",
                "hai_api_request_duration_seconds_sum",
                "hai_api_request_duration_seconds_count",
            },
        )
        c.check(
            "the text holds no key, key hash or hospital",
            KEY_A not in text and keys.hash_key(KEY_A) not in text and "AAA001" not in text and "ZZZ999" not in text,
        )
        c.check("two renders of one state are equal and end with a newline", text == w.api.metrics.render() and text.endswith("\n"))
    with world(rate=1, burst=1) as w:
        ask(w)
        ask(w)
        _, samples = scrape(w.api.metrics.render())
        c.check("a rate limited answer is counted as 429", series(samples, "hai_api_requests_total", route="/v1/predict", method="POST", status="429") == [1])
    with world(limiter=BrokenLimiter(rate_per_minute=60, burst=5, clock=Clock())) as w:
        raised = False
        try:
            ask(w)
        except RuntimeError:
            raised = True
        _, samples = scrape(w.api.metrics.render())
        c.check(
            "an unexpected failure is counted as 500 and still raised",
            raised and series(samples, "hai_api_requests_total", route="/v1/predict", method="POST", status="500") == [1],
        )
    fresh = metrics.Metrics()
    for number in range(1000):
        fresh.observe(metrics.route_label(f"/scan/{number}"), metrics.method_label(f"M{number}"), metrics.status_label(number), 0.001)
    _, samples = scrape(fresh.render())
    c.check(
        "a thousand scanned paths, methods and statuses make a bounded number of series",
        len(samples) <= 60 and series(samples, "hai_api_requests_total", route="other", method="other", status="other") != [],
    )
    c.check(
        "the known routes keep their names",
        [metrics.route_label(path) for path in ("/v1/predict", "/v1/health", "/metrics", "/v1/predict/")] == ["/v1/predict", "/v1/health", "/metrics", "other"],
    )


class BrokenLimiter(limiter.RateLimiter):
    def check(self, key_id: str) -> float | None:
        raise RuntimeError("synthetic failure")


def rejected(call: Callable[..., object], *arguments: Any) -> bool:
    try:
        call(*arguments)
    except ValueError:
        return True
    return False


CASES: dict[str, tuple[Callable[[Checker], None], str]] = {
    "ok_200": (case_ok, "api"),
    "unauthorized_401": (case_unauthorized, "api key"),
    "rate_limit_429": (case_rate_limit, "rate limit"),
    "validation_and_unknown_hospital": (case_validation_and_unknown_hospital, "api"),
    "missing_feature_503": (case_missing_feature, "feast"),
    "split_deterministic": (case_split, "canary"),
    "version_failure_isolated": (case_version_failure, "canary"),
    "rollback": (case_rollback, "canary deploy"),
    "deploy_idempotent": (case_deploy, "deploy"),
    "deploy_order": (case_deploy_order, "deploy"),
    "predictions_append": (case_predictions, "iceberg"),
    "replay_and_compare": (case_replay_and_compare, "canary compare"),
    "metrics": (case_metrics, "monitoring"),
    "dag_contract": (case_dag, "dag"),
}
