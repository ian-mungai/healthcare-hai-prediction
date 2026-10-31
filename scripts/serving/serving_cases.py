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
import math
import re
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from scripts.features import online_cases
from scripts.features import online_protocol as online
from scripts.features.feast_harness import Checker
from scripts.serving import body, canary, deploy, deploy_job, iceberg_sink, keys, limiter, manifests, metrics, predictions, replay, service

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
        self.nonfinite: dict[str, float] = {}
        self.schema_hashes: dict[str, str] = {}
        self.extra_references: dict[str, tuple[str, ...]] = {}
        self.calls = 0
        self.rows: list[Mapping[str, Any]] = []
        self.clock: Clock | None = None
        self.unloadable: set[str] = set()
        self.loaded: list[str] = []
        self.attempts: list[str] = []
        self.released: list[str] = []

    def load(self, hai_type: str, version: str) -> service.LoadedModel:
        self.attempts.append(version)
        if version in self.unloadable:
            raise RuntimeError(f"synthetic load failure for version {version}")
        self.loaded.append(version)

        def predict(row: Mapping[str, Any]) -> float:
            self.calls += 1
            self.rows.append(dict(row))
            if self.clock is not None:
                self.clock.now += self.cost
            if version in self.broken:
                raise RuntimeError("synthetic prediction failure")
            if version in self.nonfinite:
                return self.nonfinite[version]
            return float(version) + sum(len(str(value)) for value in row.values()) / 10000

        references = (*online_cases.REFERENCES, *self.extra_references.get(version, ()))
        return service.LoadedModel(version, references, self.schema_hashes.get(version, online_cases.SCHEMA_HASH), predict)

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
        if "configure" in options:
            options.pop("configure")(loader)
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


def case_recovered_model(c: Checker) -> None:
    """A model that failed to load while a dependency was down loads on a later refresh. An ejected canary stays ejected until a new deployment."""
    keys_ = hospital_keys(40)
    with world(count=40, percent=50, deployment=False) as w:
        w.loader.unloadable.add(CANDIDATE)
        deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=CANDIDATE, percent=50, mode="canary", deployment_id=RUN_1)
        w.api.refresh()
        c.check("a canary that cannot load is failed", w.api.health().body[HAI]["candidate"] == "failed")
        w.loader.unloadable.discard(CANDIDATE)
        w.clock.now += 5.0
        ask(w, keys_[0])
        c.check("no retry inside the refresh interval", w.loader.attempts.count(CANDIDATE) == 1 and w.api.health().body[HAI]["candidate"] == "failed")
        w.clock.now += 31.0
        ask(w, keys_[0])
        c.check("the next refresh loads the recovered canary", w.api.health().body[HAI]["candidate"] == CANDIDATE)
        c.check("and the canary then serves its share", "canary" in {ask(w, key).body["served_by"] for key in keys_})
        attempts = len(w.loader.attempts)
        w.clock.now += 31.0
        ask(w, keys_[0])
        c.check("a loaded model is not loaded again by a later refresh", len(w.loader.attempts) == attempts)
        w.loader.unloadable.add(CANDIDATE)
        w.loader.unloadable.add("5")
        deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version="5", percent=30, mode="canary", deployment_id=RUN_2)
        w.clock.now += 31.0
        ask(w, keys_[0])
        c.check("a new candidate that cannot load is failed while the stable model serves", w.api.health().body[HAI]["candidate"] == "failed")
        w.clock.now += 31.0
        ask(w, keys_[0])
        c.check("a failing load is tried once per refresh interval", w.loader.attempts.count("5") == 2)
    with world(count=40, percent=50, deployment=False) as w:
        w.loader.unloadable.add(PRODUCTION)
        deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=CANDIDATE, percent=50, mode="canary", deployment_id=RUN_1)
        w.api.refresh()
        c.check("a stable model that cannot load makes the service unready", w.api.health().status == 503)
        w.loader.unloadable.discard(PRODUCTION)
        w.clock.now += 31.0
        ask(w, keys_[0])
        c.check("the recovered stable model makes the service ready", w.api.health().status == 200 and w.api.health().body[HAI]["stable"] == PRODUCTION)
    with world(count=40, percent=50, deployment=False) as w:
        w.loader.unloadable.add(CANDIDATE)
        deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=CANDIDATE, percent=50, mode="canary", deployment_id=RUN_1)
        w.api.refresh()
        w.loader.unloadable.discard(CANDIDATE)
        deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=CANDIDATE, percent=40, mode="canary", deployment_id=RUN_2)
        w.clock.now += 31.0
        ask(w, keys_[0])
        c.check("a new deployment of the same failed version loads it", w.api.health().body[HAI]["candidate"] == CANDIDATE)
    with world(count=40, percent=50) as w:
        canary_keys = [key for key in keys_ if canary.route(w.api.deployment(HAI), key.split(":")[0], 2022) == "canary"]
        w.loader.broken.add(CANDIDATE)
        for key in canary_keys[:4]:
            ask(w, key)
        c.check("three failed predictions eject the canary", w.api.health().body[HAI]["candidate"] == "ejected")
        w.loader.broken.discard(CANDIDATE)
        attempts = w.loader.attempts.count(CANDIDATE)
        w.clock.now += 31.0
        ask(w, keys_[0])
        c.check(
            "an ejected canary is not loaded again by a refresh",
            w.api.health().body[HAI]["candidate"] == "ejected" and w.loader.attempts.count(CANDIDATE) == attempts,
        )
        deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=CANDIDATE, percent=40, mode="canary", deployment_id=RUN_2)
        w.clock.now += 31.0
        ask(w, keys_[0])
        c.check("the next deployment gives the ejected canary a fresh load", w.api.health().body[HAI]["candidate"] == CANDIDATE)


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


class StoredObject:
    """The body of an object the S3 stand-in returns."""

    def __init__(self, data: bytes) -> None:
        self.data = data

    def read(self) -> bytes:
        return self.data


class S3Client:
    """A stand-in for the boto3 S3 client with the conditional writes the manifest store uses: ETag matches and create-only writes."""

    class NoSuchKey(Exception):
        pass

    def __init__(self) -> None:
        from botocore.exceptions import ClientError

        self.exceptions = SimpleNamespace(NoSuchKey=self.NoSuchKey, ClientError=ClientError)
        self.objects: dict[str, tuple[str, bytes]] = {}
        self.puts = 0

    def get_object(self, **arguments: Any) -> dict[str, Any]:
        key = arguments["Key"]
        if key not in self.objects:
            raise self.NoSuchKey(key)
        etag, data = self.objects[key]
        return {"ETag": etag, "Body": StoredObject(data)}

    def put_object(self, **arguments: Any) -> None:
        key, create_only, match = arguments["Key"], arguments.get("IfNoneMatch"), arguments.get("IfMatch")
        failed = (create_only == "*" and key in self.objects) or (match is not None and (key not in self.objects or self.objects[key][0] != match))
        if failed:
            raise self.exceptions.ClientError({"Error": {"Code": "PreconditionFailed", "Message": "condition"}}, "PutObject")
        self.puts += 1
        self.objects[key] = (f'"etag{self.puts}"', bytes(arguments["Body"]))


def run_deploy_job(client: S3Client, registry: Registry, run: str, environment: Mapping[str, str], arguments: Sequence[str] = ()) -> tuple[int, str, str]:
    """The ``serve_deploy`` entry point as the task starts it: its arguments and environment, with the cloud clients replaced."""
    out, err = io.StringIO(), io.StringIO()
    modules = {
        "boto3": SimpleNamespace(client=lambda name, config=None: client),
        "botocore.config": SimpleNamespace(Config=lambda **settings: settings),
        "mlflow": SimpleNamespace(MlflowClient=lambda: registry),
    }
    with (
        mock.patch.dict(sys.modules, modules),
        mock.patch.dict("os.environ", dict(environment), clear=True),
        mock.patch.object(sys, "argv", ["deploy_job", "--run", run, *arguments]),
        contextlib.redirect_stdout(out),
        contextlib.redirect_stderr(err),
    ):
        try:
            code = deploy_job.main()
        except SystemExit as stop:
            code = int(str(stop.code))
    return code, out.getvalue(), err.getvalue()


def case_deploy_job(c: Checker) -> None:
    """The serve job as the task runs it: the checked request arrives in the environment, records go to the one fixed prefix and a refusal writes nothing."""
    request = {"hai_type": HAI, "candidate_version": CANDIDATE, "percent": 10, "deploy_mode": "canary"}
    environment = {"SERVING_REQUEST": json.dumps(request), "MODEL_API_MANIFEST_URI": "s3://example-bucket/serving/deployments"}
    client, registry = S3Client(), Registry()
    code, out, err = run_deploy_job(client, registry, RUN_1, environment)
    printed = json.loads(out) if out.strip() else {}
    c.check("the job deploys and prints the result", code == 0 and printed.get("status") == "written" and printed.get("candidate_version") == CANDIDATE)
    c.check(
        "the records are under the fixed prefix and nowhere else",
        sorted(client.objects) == [f"serving/deployments/active/{HAI}.json", f"serving/deployments/history/{HAI}/{RUN_1}.json"],
    )
    before = dict(client.objects)
    code, out, err = run_deploy_job(client, registry, RUN_1, environment)
    c.check("a retry changes nothing", code == 0 and json.loads(out or "{}").get("status") == "unchanged" and client.objects == before)
    refusals = {
        "no request": {k: v for k, v in environment.items() if k != "SERVING_REQUEST"},
        "a request that is not JSON": {**environment, "SERVING_REQUEST": "{"},
        "a request with a field too many": {**environment, "SERVING_REQUEST": json.dumps({**request, "extra": 1})},
        "a percent above the limit": {**environment, "SERVING_REQUEST": json.dumps({**request, "percent": 90})},
        "a manifest address that is not S3": {**environment, "MODEL_API_MANIFEST_URI": "records/folder"},
        "no manifest address": {k: v for k, v in environment.items() if k != "MODEL_API_MANIFEST_URI"},
    }
    for label, changed in refusals.items():
        fresh = S3Client()
        code, out, err = run_deploy_job(fresh, registry, RUN_2, changed)
        c.check(f"{label} exits 2 with the refusal message and writes nothing", code == 2 and "serve_deploy refused" in err and not fresh.objects and not out)
    code, _, err = run_deploy_job(client, registry, RUN_1, {**environment, "SERVING_REQUEST": json.dumps({**request, "percent": 30})})
    c.check("a retry under the same id with another request is refused", code == 2 and "serve_deploy refused" in err and client.objects == before)
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        path = Path(folder) / "serving_request.json"
        path.write_text(json.dumps(request))
        fresh = S3Client()
        code, out, err = run_deploy_job(fresh, registry, RUN_3, {"MODEL_API_MANIFEST_URI": environment["MODEL_API_MANIFEST_URI"]}, ["--request", str(path)])
        c.check("a request file still works", code == 0 and json.loads(out or "{}").get("status") == "written")
    c.check("no run moved the production alias", registry.alias_writes == 0)


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
        c.check(
            "and the refusal says a later deployment replaced it", refusal_text(lambda: canary_run(RUN_1)).startswith("a later deployment replaced this one")
        )
        c.check("the refused retry left the rollback active", w.store.read_active(HAI) == after_rollback)
        c.check("a retry of the rollback changes nothing", rollback_run(RUN_2).status == "unchanged" and w.store.read_active(HAI) == after_rollback)
        later = canary_run(RUN_3, "5", 20)
        c.check("a new run still deploys after the refusal", later.status == "written" and later.deployment.candidate_version == "5")
        newest = w.store.read_active(HAI)
        c.check("each deployment is numbered one above the one it replaced", newest is not None and canary.Deployment.parse(newest.body).sequence == 3)
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


def with_view(reference: str, value: Any, stamp: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """A reader wrapper that answers one reference with the given value and event time, as Feast does for a view row that is absent or published as null."""

    def wrap(inner: Callable[..., Any]) -> Callable[..., Any]:
        def read(references: list[str], key: str) -> Mapping[str, Sequence[Any]]:
            answer = dict(inner(references, key))
            name = reference.replace(":", "__", 1)
            answer[name] = [value]
            answer[name + "__ts"] = [stamp]
            return answer

        return read

    return wrap


def case_absent_row(c: Checker) -> None:
    """A view row that is absent gives 503 and no prediction. A value published as null still reaches the model."""
    view_reference = online_cases.REFERENCES[1]
    for label, stamp in (("an event time of zero", 0), ("no event time", None)):
        with world(wrap=with_view(view_reference, None, stamp)) as w:
            reply = ask(w)
            c.check(f"an absent row with {label} gives 503", reply.status == 503 and "prediction" not in reply.body)
            c.check(f"the model was not asked for {label}", w.loader.calls == 0)
            c.check(f"no row was written for {label}", rows_of(w) == [])
    with world(wrap=with_view(view_reference, None, online_cases.WINDOW_SECONDS)) as w:
        reply = ask(w)
        c.check("a value published as null answers 200", reply.status == 200)
        c.check("the model received the null for its own preprocessing", w.loader.calls == 1 and w.loader.rows[0][view_reference] is None)
    with world(wrap=with_view(view_reference, "v1|AAA001:2022|x", online_cases.WINDOW_SECONDS)) as w:
        c.check("a present row answers 200", ask(w).status == 200)


def candidate_trouble(loader: Loader) -> None:
    loader.schema_hashes[CANDIDATE] = online_cases.OTHER_HASH


def case_candidate_feature_errors(c: Checker) -> None:
    """A feature error that belongs to the canary model sends its requests to the stable model and ejects the canary. A store error does not."""
    keys_ = hospital_keys(40)
    with world(count=40, percent=50, configure=candidate_trouble) as w:
        routed = {key: canary.route(w.api.deployment(HAI), key.split(":")[0], 2022) for key in keys_}
        canary_keys = [key for key, kind in routed.items() if kind == "canary"]
        stable_keys = [key for key, kind in routed.items() if kind == "stable"]
        first = ask(w, canary_keys[0])
        c.check(
            "a canary with another feature schema falls back to the stable model with 200",
            first.status == 200 and first.body["model_version"] == PRODUCTION and first.body["fallback"] is True,
        )
        for key in canary_keys[1:3]:
            ask(w, key)
        c.check("three such failures eject the canary", w.api.health().body[HAI]["candidate"] == "ejected")
        c.check("every stable request still answers 200 from the stable model", all(ask(w, key).body["model_version"] == PRODUCTION for key in stable_keys))
        c.check("the fallback rows name the canary as the routed version", any(row["routed_version"] == CANDIDATE and row["fallback"] for row in rows_of(w)))
    absent = "feature_matrix_extra:x"
    with world(count=40, percent=50, wrap=with_view(absent, None, 0), configure=lambda loader: loader.extra_references.update({CANDIDATE: (absent,)})) as w:
        routed = {key: canary.route(w.api.deployment(HAI), key.split(":")[0], 2022) for key in keys_}
        canary_keys = [key for key, kind in routed.items() if kind == "canary"]
        replies = [ask(w, key) for key in canary_keys[:4]]
        c.check(
            "a canary that needs an absent view row falls back to the stable model",
            all(r.status == 200 and r.body["model_version"] == PRODUCTION for r in replies),
        )
        c.check("and is ejected", w.api.health().body[HAI]["candidate"] == "ejected")
    with world(count=40, percent=50) as w:
        routed = {key: canary.route(w.api.deployment(HAI), key.split(":")[0], 2022) for key in keys_}
        canary_keys = [key for key, kind in routed.items() if kind == "canary"]
        w.server.write_hash(online.CONTROL_DB, online.MARKER_KEY, {"state": "loading"})
        outage = [ask(w, key).status for key in canary_keys[:5]]
        c.check("a store that is loading gives 503 for canary requests", outage == [503] * 5)
        c.check("a store outage does not eject the canary", w.api.health().body[HAI]["candidate"] == CANDIDATE)


class SlowWriter:
    """A stand-in for the PyIceberg append: it waits for ``release`` and then commits, as a slow commit does after the caller gave up."""

    def __init__(self) -> None:
        self.release = threading.Event()
        self.started = threading.Event()
        self.committed: list[str] = []
        self.calls = 0
        self.fail: Exception | None = None
        self.blocking = True

    def __call__(self, rows: Sequence[Mapping[str, Any]], timeout: float) -> None:
        self.calls += 1
        self.started.set()
        if self.blocking:
            self.release.wait(10)
        if self.fail is not None:
            raise self.fail
        self.committed.extend(str(row["request_id"]) for row in rows)


def sink_rows(*names: str) -> list[dict[str, Any]]:
    return [{"request_id": name} for name in names]


def case_sink_late_commit(c: Checker) -> None:
    """After an append timeout the one worker keeps its place, a late commit is remembered and not sent again, and the worker has an owned shutdown."""
    environ = {"POLARIS_URI": "http://127.0.0.1:1/api/catalog", "POLARIS_CLIENT_ID": "writer", "POLARIS_CLIENT_SECRET": "x"}
    writer = SlowWriter()
    sink = iceberg_sink.IcebergSink(environ, writer=writer)
    timed_out = False
    try:
        sink.append(sink_rows("a", "b", "c"), 0.2)
    except TimeoutError:
        timed_out = True
    c.check("a slow commit ends in a timeout for the caller", timed_out and writer.started.is_set())
    second = False
    try:
        sink.append(sink_rows("a", "b", "c", "d"), 0.2)
    except TimeoutError:
        second = True
    c.check("a second append is refused while the first still runs, with no second writer", second and writer.calls == 1)
    writer.release.set()
    c.check("the late commit finishes within a deadline", sink.wait_idle(5.0))
    sink.append(sink_rows("a", "b", "c", "d"), 1.0)
    c.check("the retry sends only the row the late commit did not cover", writer.committed == ["a", "b", "c", "d"] and writer.calls == 2)
    sink.append(sink_rows("e"), 1.0)
    c.check("a later append sends only its own rows", writer.committed == ["a", "b", "c", "d", "e"])
    failing = SlowWriter()
    failing.blocking = False
    failing.fail = OSError("synthetic commit failure")
    other = iceberg_sink.IcebergSink(environ, writer=failing)
    raised = False
    try:
        other.append(sink_rows("x"), 1.0)
    except OSError:
        raised = True
    failing.fail = None
    other.append(sink_rows("x"), 1.0)
    c.check("a failed commit is raised and its rows are sent again", raised and failing.committed == ["x"])
    c.check("close reports a clean stop when nothing runs", sink.close(1.0) is True)
    closed = False
    try:
        sink.append(sink_rows("f"), 1.0)
    except RuntimeError:
        closed = True
    c.check("a closed sink refuses new appends", closed)
    stuck = SlowWriter()
    hanging = iceberg_sink.IcebergSink(environ, writer=stuck)
    with contextlib.suppress(TimeoutError):
        hanging.append(sink_rows("g"), 0.1)
    c.check("close reports a worker that does not stop within its deadline", hanging.close(0.2) is False)
    c.check("a stuck worker is a daemon thread, so it cannot hold the process open", hanging.worker_is_daemon())
    stuck.release.set()
    c.check("the stuck worker ends once released", hanging.wait_idle(5.0))


def case_nonfinite(c: Checker) -> None:
    """A prediction that is not a finite number is a model failure: it triggers fallback and ejection, and it is never returned or recorded."""
    keys_ = hospital_keys(40)
    with world(count=40, percent=50) as w:
        routed = {key: canary.route(w.api.deployment(HAI), key.split(":")[0], 2022) for key in keys_}
        canary_keys = [key for key, kind in routed.items() if kind == "canary"]
        w.loader.nonfinite[CANDIDATE] = math.nan
        first = ask(w, canary_keys[0])
        c.check(
            "a NaN from the canary falls back to the stable model with 200",
            first.status == 200 and first.body["model_version"] == PRODUCTION and first.body["fallback"] is True,
        )
        for key in canary_keys[1:3]:
            ask(w, key)
        c.check("three nonfinite answers eject the canary", w.api.health().body[HAI]["candidate"] == "ejected")
        w.loader.nonfinite[CANDIDATE] = math.inf
        c.check("an infinite answer is handled alike", ask(w, canary_keys[3]).body["model_version"] == PRODUCTION)
        c.check("no recorded prediction is nonfinite", all(math.isfinite(row["prediction"]) for row in rows_of(w)))
    with world(count=40, percent=50) as w:
        routed = {key: canary.route(w.api.deployment(HAI), key.split(":")[0], 2022) for key in keys_}
        stable_keys = [key for key, kind in routed.items() if kind == "stable"]
        canary_keys = [key for key, kind in routed.items() if kind == "canary"]
        w.loader.nonfinite[PRODUCTION] = -math.inf
        refused_ = [ask(w, key) for key in stable_keys[:5]]
        c.check("a nonfinite stable answer gives 503 with no prediction", all(r.status == 503 and "prediction" not in r.body for r in refused_))
        c.check("the canary still answers its own requests", all(ask(w, key).status == 200 for key in canary_keys[:3]))
        c.check("no row holds the nonfinite value", all(math.isfinite(row["prediction"]) for row in rows_of(w)))
        reply = ask(w, stable_keys[0])
        c.check("the refusal body is valid JSON", json.loads(json.dumps(reply.body, allow_nan=False)) == reply.body)


def case_admit_before_body(c: Checker) -> None:
    """The key check and the rate limit come before the body is read, every refusal is counted once, and the streamed body has a size and a time limit."""
    with world(rate=600, burst=3) as w:
        refused_ = w.api.admit(None)
        c.check("a missing key is refused at admission with 401, before any body", isinstance(refused_, service.Reply) and refused_.status == 401)
        c.check("a wrong key is refused alike", isinstance(w.api.admit("sk-test-wrong-key-12345"), service.Reply))
        admitted = [w.api.admit(KEY_A) for _ in range(3)]
        c.check("a valid key is admitted until its burst is used", all(isinstance(item, service.Admitted) for item in admitted))
        limited = w.api.admit(KEY_A)
        c.check("the burst ends in 429 at admission", isinstance(limited, service.Reply) and limited.status == 429 and "Retry-After" in limited.headers)
        _, samples = scrape(w.api.metrics.render())
        c.check(
            "admission refusals are counted once each",
            series(samples, "hai_api_requests_total", route="/v1/predict", method="POST", status="401") == [2.0]
            and series(samples, "hai_api_requests_total", route="/v1/predict", method="POST", status="429") == [1.0],
        )
        c.check("a refused admission read no feature and reserved no room", w.reads == [] and w.api.buffer.pending() == 0)
        granted = [item for item in admitted if isinstance(item, service.Admitted)]
        if len(granted) != 3:
            return
        first, second = granted[0], granted[1]
        reply = w.api.finish(first, None, {"hai_type": HAI, "ccn": "AAA001", "window_year": 2022})
        c.check("an admitted request finishes with the same answer predict gives", reply.status == 200 and isinstance(reply.body.get("prediction"), float))
        _, samples = scrape(w.api.metrics.render())
        c.check(
            "admission and finish count the request once", series(samples, "hai_api_requests_total", route="/v1/predict", method="POST", status="200") == [1.0]
        )
        cut = w.api.reject_body(second, 413, "body_too_large")
        c.check("a refused body answers its status and is counted", cut.status == 413 and cut.body == {"error": "body_too_large"})
        _, samples = scrape(w.api.metrics.render())
        c.check("the refused body shows in the counter", series(samples, "hai_api_requests_total", route="/v1/predict", method="POST", status="413") == [1.0])
    budget = body.BodyBudget(limit=10, seconds=5.0, clock=Clock())
    budget.add(b"123456")
    budget.add(b"7890")
    c.check("a body of exactly the limit is accepted", budget.total == 10)
    c.check("one more byte is refused as too large", raised_body(lambda: budget.add(b"x"), body.BodyTooLarge, 413))
    c.check(
        "a declared length over the limit is refused before any read",
        raised_body(lambda: body.BodyBudget(limit=10, seconds=5.0, clock=Clock()).declare("11"), body.BodyTooLarge, 413),
    )
    c.check(
        "a declared length that is not a number is refused",
        raised_body(lambda: body.BodyBudget(limit=10, seconds=5.0, clock=Clock()).declare("1e3"), body.BodyInvalid, 400),
    )
    clock = Clock()
    slow = body.BodyBudget(limit=100, seconds=5.0, clock=clock)
    slow.add(b"a")
    clock.now += 6
    c.check("a chunk that arrives after the deadline is refused as too slow", raised_body(lambda: slow.add(b"b"), body.BodyTooSlow, 408))
    c.check("the remaining time is clamped to the deadline", abs(body.BodyBudget(limit=1, seconds=5.0, clock=Clock()).remaining() - 5.0) < 1e-9)
    c.check("no time left is refused too", raised_body(slow.remaining, body.BodyTooSlow, 408))


def raised_body(call: Callable[[], object], kind: type[body.BodyRefused], status: int) -> bool:
    try:
        call()
    except body.BodyRefused as error:
        return isinstance(error, kind) and error.status == status
    return False


def case_key_tool(c: Checker) -> None:
    """The documented way to make a key gives a hash of exactly the bytes a caller presents, and the key is never printed."""
    with tempfile.TemporaryDirectory(prefix="serving_keys_") as folder:
        output = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            code = keys.main(["new", "--id", "ops", "--output", folder])
        key_file = Path(folder) / "api_key"
        raw = key_file.read_bytes() if key_file.exists() else b""
        c.check("the tool succeeds and writes the key file", code == 0 and bool(raw))
        c.check("the key file holds the key and no trailing whitespace", raw == raw.strip() and len(raw) == 48)
        c.check("the key file is private to its owner", key_file.exists() and (key_file.stat().st_mode & 0o777) == 0o600)
        printed = output.getvalue()
        entry = json.loads(printed.splitlines()[-1]) if printed.strip() else {}
        c.check("what the tool prints is the hash entry for the key identifier", list(entry) == ["ops"])
        c.check("the key ring made from that entry accepts exactly the key file's text", keys.KeyRing.from_hashes(entry).identify(raw.decode()) == "ops")
        c.check("the key ring refuses the key with a newline added", keys.KeyRing.from_hashes(entry).identify(raw.decode() + "\n") is None)
        c.check("the tool never prints the key", raw.decode() not in printed)
        again = io.StringIO()
        with contextlib.redirect_stdout(again), contextlib.redirect_stderr(again):
            second = keys.main(["new", "--id", "ops", "--output", folder])
        c.check("a second run does not overwrite the key file", second != 0 and key_file.read_bytes() == raw)
        bad = io.StringIO()
        with contextlib.redirect_stdout(bad), contextlib.redirect_stderr(bad):
            wrong = keys.main(["new", "--id", "Not A Key Id", "--output", folder])
        c.check("a key identifier that the ring refuses is refused at creation", wrong != 0)
    text = (ROOT / "docs/model_api.md").read_text()
    c.check(
        "the document points to the tool and no longer hashes a file with a newline",
        "scripts.serving.keys new" in text and "openssl rand" not in text and "shasum" not in text,
    )


def refusal_text(call: Callable[[], object]) -> str:
    try:
        call()
    except deploy.DeployRefused as error:
        return str(error)
    return ""


def refused(call: Callable[[], object]) -> bool:
    try:
        call()
    except (deploy.DeployRefused, manifests.ManifestConflict):
        return True
    except Exception:
        return False
    return False


def previous_record(deployment_id: str, candidate: str | None, percent: int) -> bytes:
    """A deployment record as the first release wrote it: the same format version and no sequence field."""
    document = {"version": 1, "hai_type": HAI, "deployment_id": deployment_id, "stable_version": PRODUCTION, "candidate_version": candidate, "percent": percent}
    return (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()


def case_previous_record_format(c: Checker) -> None:
    """A record written before the sequence field still reads as the first deployment, serves after a cold start and numbers the next deploy one above it."""
    old = previous_record(RUN_1, CANDIDATE, 20)
    c.check("a record without a sequence parses as sequence 1", canary.Deployment.parse(old).sequence == 1)
    try:
        canary.Deployment.parse(old.replace(b'"percent"', b'"extra":1,"percent"'))
        unknown_refused = False
    except canary.DeploymentInvalid:
        unknown_refused = True
    c.check("a record with another unknown field is still refused", unknown_refused)
    with world(deployment=False) as w:
        w.store.put_history(HAI, RUN_1, old)
        w.store.replace_active(HAI, old, None)
        w.clock.now += 31.0
        reply = ask(w)
        c.check("a fresh service serves from a previous-format active record", reply.status == 200 and w.api.health().status == 200)
        later = deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=None, percent=0, mode="rollback", deployment_id=RUN_2)
        c.check("a deploy over it is numbered 2 and writes the current format", later.status == "written" and later.deployment.sequence == 2)
        c.check(
            "a retry of the previous-format deployment is refused as replaced",
            refused(
                lambda: deploy.run(
                    registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=CANDIDATE, percent=20, mode="canary", deployment_id=RUN_1
                )
            ),
        )
        again = deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=None, percent=0, mode="rollback", deployment_id=RUN_2)
        c.check("a retry of the new deployment changes nothing", again.status == "unchanged")
    with world(deployment=False) as w:
        w.store.put_history(HAI, RUN_1, old)
        finished = deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=CANDIDATE, percent=20, mode="canary", deployment_id=RUN_1)
        c.check("a previous-format history record with no active record finishes on retry", finished.status == "written" and finished.deployment.sequence == 1)


def case_predictions(c: Checker) -> None:
    """The append never fails a request, retries without duplicates and has a deadline. A full buffer refuses new requests and drops no acknowledged row."""
    with world(capacity=5) as w:
        w.sink.fail = True
        replies = [ask(w, key) for key in list(online_cases.KEYS) * 2]
        accepted = [reply for reply in replies if reply.status == 200]
        refused_ = [reply for reply in replies if reply.status == 503]
        c.check("a failing sink does not fail a request that has room", len(accepted) == 5)
        c.check(
            "the three requests over the capacity are refused with 503 and a retry time",
            len(refused_) == 3 and all(int(r.headers.get("Retry-After", "0")) >= 1 for r in refused_),
        )
        c.check("a refused request names the backlog", all(r.body.get("error") == "predictions_backlog" for r in refused_))
        calls, reads = w.loader.calls, len(w.reads)
        ask(w)
        c.check("a refused request reads no feature and asks no model", w.loader.calls == calls and len(w.reads) == reads)
        w.api.buffer.flush()
        c.check("the failed flush kept every acknowledged row", w.api.buffer.pending() == 5)
        c.check("the flush passed its deadline to the sink", w.sink.timeouts == [5.0])
        w.sink.fail = False
        w.api.buffer.flush()
        sent = {row["request_id"] for row in w.sink.rows}
        c.check("the next flush appends each acknowledged row once", len(w.sink.rows) == 5 and sent == {r.body["request_id"] for r in accepted})
        w.api.buffer.flush()
        c.check("a flush with nothing pending appends nothing", len(w.sink.rows) == 5)
        c.check("the room is back after the flush", ask(w).status == 200)
        _, samples = scrape(w.api.metrics.render())
        c.check("the refusals are counted as 503 answers", series(samples, "hai_api_requests_total", route="/v1/predict", method="POST", status="503") == [4.0])
    with world(capacity=2) as w:
        for _ in range(5):
            ask(w, "ZZZ999:2022")
            ask(w, "AAA:2022")
        c.check("requests that end without a row give their room back", ask(w).status == 200 and ask(w).status == 200 and ask(w).status == 503)
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


def replay_send(w: World, payload: dict[str, Any], timeout: float) -> tuple[int, dict[str, str], dict[str, Any]]:
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


def case_replay_budget(c: Checker) -> None:
    """A replay never waits longer than the time it has left: a long Retry-After is clamped to the budget and the replay then fails at its deadline."""
    clock = Clock()
    slept: list[float] = []

    def pause(seconds: float) -> None:
        slept.append(seconds)
        clock.now += seconds

    def always_limited(payload: dict[str, Any], timeout: float) -> tuple[int, dict[str, str], dict[str, Any]]:
        return 429, {"Retry-After": "60"}, {"error": "rate_limited"}

    start = clock.now
    failed = False
    try:
        replay.run(always_limited, [("AAA001", 2022)], HAI, clock=clock, sleep=pause, deadline_seconds=1.0)
    except replay.ReplayDeadline:
        failed = True
    c.check("a Retry-After of 60 seconds on a 1 second deadline ends in the deadline error", failed)
    c.check("no wait is longer than the time left", all(seconds <= 1.0 + 1e-9 for seconds in slept))
    c.check("the replay does not run past its deadline", clock.now - start <= 1.0 + 1e-9)
    clock2 = Clock()

    def odd(payload: dict[str, Any], timeout: float) -> tuple[int, dict[str, str], dict[str, Any]]:
        return 429, {"Retry-After": "soon"}, {}

    waits: list[float] = []

    def note(seconds: float) -> None:
        waits.append(seconds)
        clock2.now += seconds

    with contextlib.suppress(replay.ReplayDeadline):
        replay.run(odd, [("AAA001", 2022)], HAI, clock=clock2, sleep=note, deadline_seconds=3.0)
    seen: list[float] = []

    def timed(payload: dict[str, Any], timeout: float) -> tuple[int, dict[str, str], dict[str, Any]]:
        seen.append(timeout)
        clock3.now += 1.5
        return 200, {}, {"served_by": "stable", "prediction": 1.0, "model_version": "3"}

    clock3 = Clock()
    replay.run(timed, [("AAA001", 2022), ("AAA002", 2022)], HAI, clock=clock3, sleep=lambda seconds: None, deadline_seconds=3.0)
    c.check("every request timeout is the smaller of the default and the time left", seen == [3.0, 1.5])
    c.check("a Retry-After that is not a number waits a short default within the budget", bool(waits) and all(0 < seconds <= 3.0 for seconds in waits))


class DrippingApi(BaseHTTPRequestHandler):
    """A real HTTP answer sent one byte at a time, so each read progresses and the whole answer takes far longer than any one read."""

    def log_message(self, format: str, *args: object) -> None:
        return

    def do_POST(self) -> None:
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        raw = json.dumps({"served_by": "stable", "prediction": 1.0, "model_version": "3", "padding": "x" * 400}).encode()
        data = f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(raw)}\r\nConnection: close\r\n\r\n".encode() + raw
        with contextlib.suppress(OSError):
            for index in range(len(data)):
                self.wfile.write(data[index : index + 1])
                self.wfile.flush()
                time.sleep(0.002)
        self.close_connection = True


def case_replay_http_drip(c: Checker) -> None:
    """One deadline covers the whole HTTP exchange: an answer that keeps progressing but takes too long ends the replay at the deadline."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), DrippingApi)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}"
        started = time.monotonic()
        outcome = "completed"
        try:
            replay.run(replay.http_send(url, KEY_A), [("AAA001", 2022)], HAI, clock=time.monotonic, sleep=time.sleep, deadline_seconds=0.1)
        except (TimeoutError, replay.ReplayDeadline) as error:
            outcome = type(error).__name__
        elapsed = time.monotonic() - started
        c.check(f"a dripped answer ends the replay with a deadline error, not a result (got {outcome})", outcome in ("TimeoutError", "ReplayDeadline"))
        c.check(f"and it ends close to the deadline (took {elapsed:.2f} s)", elapsed < 0.6)
        started = time.monotonic()
        report = replay.run(replay.http_send(url, KEY_A), [("AAA001", 2022)], HAI, clock=time.monotonic, sleep=time.sleep, deadline_seconds=30.0)
        c.check("the same answer succeeds inside a generous deadline", report["statuses"] == {"200": 1} and time.monotonic() - started < 10.0)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    clock = Clock()

    def slow(payload: dict[str, Any], timeout: float) -> tuple[int, dict[str, str], dict[str, Any]]:
        clock.now += 2.0
        return 200, {}, {"served_by": "stable", "prediction": 1.0, "model_version": "3"}

    late = False
    try:
        replay.run(slow, [("AAA001", 2022)], HAI, clock=clock, sleep=lambda seconds: None, deadline_seconds=1.0)
    except replay.ReplayDeadline:
        late = True
    c.check("a result that arrives after the deadline is not accepted", late)


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
    "absent_row": (case_absent_row, "feast"),
    "admit_before_body": (case_admit_before_body, "api"),
    "key_tool": (case_key_tool, "api key"),
    "split_deterministic": (case_split, "canary"),
    "version_failure_isolated": (case_version_failure, "canary"),
    "candidate_feature_errors": (case_candidate_feature_errors, "canary"),
    "nonfinite_predictions": (case_nonfinite, "canary"),
    "recovered_model": (case_recovered_model, "canary dependency recovery"),
    "rollback": (case_rollback, "canary deploy"),
    "deploy_idempotent": (case_deploy, "deploy"),
    "deploy_order": (case_deploy_order, "deploy"),
    "deploy_job_entry": (case_deploy_job, "deploy aws"),
    "predictions_append": (case_predictions, "iceberg"),
    "sink_late_commit": (case_sink_late_commit, "iceberg"),
    "replay_and_compare": (case_replay_and_compare, "canary compare"),
    "replay_budget": (case_replay_budget, "canary compare"),
    "replay_http_drip": (case_replay_http_drip, "timeout network"),
    "previous_record_format": (case_previous_record_format, "deploy compatibility"),
    "metrics": (case_metrics, "monitoring"),
    "dag_contract": (case_dag, "dag"),
}
