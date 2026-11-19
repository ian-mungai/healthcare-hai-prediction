"""End-to-end cases of the model API, driven through the service entry point over the project's Redis stand-in, the real writer protocol and file
manifests.

The model, the registry and the prediction sink are explicit stand-ins. FastAPI, the MLflow loader, the PyIceberg sink and the S3 manifest store run
only in the serving image and are not exercised here. Canary proof is replayed requests, never live traffic.
"""

from __future__ import annotations

import ast
import contextlib
import importlib
import importlib.util
import io
import json
import logging
import math
import os
import re
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from scripts.features import feast_aws_cases, online_cases
from scripts.features import online_protocol as online
from scripts.features.feast_harness import Checker, settle_threads, stop_worker
from scripts.infrastructure import push_images, secret_store
from scripts.serving import body, canary, deploy, deploy_job, iceberg_sink, keys, limiter, manifests, metrics, mlflow_loader, predictions, replay, service

ROOT = Path(__file__).resolve().parents[2]
HAI = "HAI_1"
KEY_A = "sk-test-0123456789-do-not-log"
KEY_B = "sk-test-other-key"
RUN_1 = "serving_aaaaaaa1"
RUN_2 = "serving_aaaaaaa2"
RUN_3 = "serving_aaaaaaa3"
RESEARCH = "research_bbbbbbb1"
PRODUCTION, CANDIDATE = "3", "4"
# The wall clock the serving worlds start at (epoch seconds) and the canary lifetime the deploys of these cases ask for.
NOW = 1_800_000_000
TTL_HOURS = 24
CONTRACT = json.loads((ROOT / "data_contracts/serving/predictions.json").read_text())


class NotFound(Exception):
    error_code = "RESOURCE_DOES_NOT_EXIST"


class Registry:
    """A stand-in for the MLflow registry client with the two read calls deploy may make. ``alias_writes`` must stay zero."""

    def __init__(self, production: str | None = PRODUCTION, versions: Sequence[str] = (PRODUCTION, CANDIDATE, "5"), hai_type: str = HAI) -> None:
        self.hai_type = hai_type
        self.aliases = {} if production is None else {"production": production}
        self.versions = {version: {"bundle_sha256": "f" * 64} for version in versions}
        self.alias_writes = 0

    def search_model_versions(self, filter_string: str = "") -> list[Any]:
        return [
            SimpleNamespace(name=f"hai_model_{self.hai_type}", version=version, run_id=f"run{version}", tags=dict(tags))
            for version, tags in self.versions.items()
        ]

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
        self.set_models: dict[str, str] = {}
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

        feature_set = self.set_models.get(version, "")
        base = online_cases.SET_MODEL if feature_set else online_cases.REFERENCES
        default_hash = online_cases.SET_HASH if feature_set else online_cases.SCHEMA_HASH
        references = (*base, *self.extra_references.get(version, ()))
        return service.LoadedModel(version, references, self.schema_hashes.get(version, default_hash), predict, feature_set)

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
    wall: Clock = field(default_factory=Clock)


def hospital_keys(count: int) -> list[str]:
    return [f"H{number:05d}:2022" for number in range(1, count + 1)]


@contextlib.contextmanager
def world(count: int = 4, rate: int = 600, burst: int = 1000, deployment: bool = True, **options: Any) -> Iterator[World]:
    """A clean serving world: Redis stand-in loaded by the real writer protocol, a deployment written by the real deploy action, a service built over both."""
    server = online_cases.FakeServer()
    names = hospital_keys(count) if count != 4 else list(online_cases.KEYS)
    if options.pop("single", True):
        online_cases.load(server, online_cases.Hooks(server, "v1", names), online_cases.plan_for(count=len(names)))
    for feature_set in options.pop("sets", ()):
        online_cases.model_set_load(server, online_cases.ModelSetHooks(server, "s1", feature_set, names), online_cases.SET_HASH, feature_set)
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        store = manifests.FileManifestStore(Path(folder))
        registry, loader, sink, clock, wall = Registry(), Loader(), Sink(), Clock(), Clock()
        wall.now = float(NOW)
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
                canary_ttl_hours=TTL_HOURS,
                research_run=RESEARCH,
                now=NOW,
                deployment_id=RUN_1,
            )
        read_features = options.pop("wrap", lambda inner: inner)(online_cases.reader(server))
        read_marker = options.pop("wrap_marker", lambda inner: inner)(online_cases.markers(server))
        counted: list[str] = []

        def reading(references: list[str], key: str) -> Mapping[str, Sequence[Any]]:
            counted.append(key)
            return read_features(references, key)

        api = service.ModelService(
            keys=keys.KeyRing.from_hashes({"ops": keys.hash_key(KEY_A), "other": keys.hash_key(KEY_B)}),
            limiter=options.pop("limiter", None) or limiter.RateLimiter(rate_per_minute=rate, burst=burst, clock=clock),
            read_marker=read_marker,
            read_features=reading,
            set_readers=online_cases.set_readers(server),
            manifests=store,
            loader=loader,
            buffer=predictions.PredictionBuffer(
                sink, columns=CONTRACT["columns"], capacity=options.get("capacity", 1000), batch=50, deadline_seconds=5.0, clock=clock
            ),
            clock=clock,
            wall=wall,
            new_id=iter(f"req{number:04d}" for number in range(10_000)).__next__,
            hai_types=(HAI,),
            refresh_seconds=30.0,
            failure_limit=3,
        )
        loader.clock = clock
        world_ = World(server, registry, store, loader, sink, clock, api, counted, wall=wall)
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
    wide = canary.Deployment(HAI, "dep1", PRODUCTION, CANDIDATE, 20, written_at=NOW, expires_at=NOW + 3600)
    c.check(
        "the pinned bucket routes by the percent", canary.route(wide, "H00001", 2022, NOW) == "stable" and canary.route(wide, "H00002", 2022, NOW) == "stable"
    )
    corpus = [(f"H{number:05d}", 2022) for number in range(1, 10_001)]
    share = sum(canary.route(wide, ccn, year, NOW) == "canary" for ccn, year in corpus)
    c.check("a replayed corpus of 10,000 keys sends 1,998 to the canary at 20 percent", share == 1998)
    c.check(
        "zero candidate means no canary",
        all(canary.route(canary.Deployment(HAI, "dep1", PRODUCTION, None, 0), ccn, year, NOW) == "stable" for ccn, year in corpus[:500]),
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
            all(body["served_by"] == canary.route(w.api.deployment(HAI), key.split(":")[0], 2022, NOW) for key, body in seen.items()),
        )


def case_version_failure(c: Checker) -> None:
    """One version failing never takes the other down: a failing canary falls back and is ejected, a failing stable fails only its own requests."""
    keys_ = hospital_keys(40)
    with world(count=40, percent=50) as w:
        routed = {key: canary.route(w.api.deployment(HAI), key.split(":")[0], 2022, NOW) for key in keys_}
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
        routed = {key: canary.route(w.api.deployment(HAI), key.split(":")[0], 2022, NOW) for key in keys_}
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
        deploy.run(
            registry=w.registry,
            manifests=w.store,
            hai_type=HAI,
            candidate_version=CANDIDATE,
            percent=50,
            mode="canary",
            canary_ttl_hours=TTL_HOURS,
            research_run=RESEARCH,
            now=NOW,
            deployment_id=RUN_1,
        )
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
        deploy.run(
            registry=w.registry,
            manifests=w.store,
            hai_type=HAI,
            candidate_version=CANDIDATE,
            percent=50,
            mode="canary",
            canary_ttl_hours=TTL_HOURS,
            research_run=RESEARCH,
            now=NOW,
            deployment_id=RUN_1,
        )
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
        deploy.run(
            registry=w.registry,
            manifests=w.store,
            hai_type=HAI,
            candidate_version="5",
            percent=30,
            mode="canary",
            canary_ttl_hours=TTL_HOURS,
            research_run=RESEARCH,
            now=NOW,
            deployment_id=RUN_2,
        )
        w.clock.now += 31.0
        ask(w, keys_[0])
        c.check("a new candidate that cannot load is failed while the stable model serves", w.api.health().body[HAI]["candidate"] == "failed")
        w.clock.now += 31.0
        ask(w, keys_[0])
        c.check("a failing load is tried once per refresh interval", w.loader.attempts.count("5") == 2)
    with world(count=40, percent=50, deployment=False) as w:
        w.loader.unloadable.add(PRODUCTION)
        deploy.run(
            registry=w.registry,
            manifests=w.store,
            hai_type=HAI,
            candidate_version=CANDIDATE,
            percent=50,
            mode="canary",
            canary_ttl_hours=TTL_HOURS,
            research_run=RESEARCH,
            now=NOW,
            deployment_id=RUN_1,
        )
        w.api.refresh()
        c.check("a stable model that cannot load makes the service unready", w.api.health().status == 503)
        w.loader.unloadable.discard(PRODUCTION)
        w.clock.now += 31.0
        ask(w, keys_[0])
        c.check("the recovered stable model makes the service ready", w.api.health().status == 200 and w.api.health().body[HAI]["stable"] == PRODUCTION)
    with world(count=40, percent=50, deployment=False) as w:
        w.loader.unloadable.add(CANDIDATE)
        deploy.run(
            registry=w.registry,
            manifests=w.store,
            hai_type=HAI,
            candidate_version=CANDIDATE,
            percent=50,
            mode="canary",
            canary_ttl_hours=TTL_HOURS,
            research_run=RESEARCH,
            now=NOW,
            deployment_id=RUN_1,
        )
        w.api.refresh()
        w.loader.unloadable.discard(CANDIDATE)
        deploy.run(
            registry=w.registry,
            manifests=w.store,
            hai_type=HAI,
            candidate_version=CANDIDATE,
            percent=40,
            mode="canary",
            canary_ttl_hours=TTL_HOURS,
            research_run=RESEARCH,
            now=NOW,
            deployment_id=RUN_2,
        )
        w.clock.now += 31.0
        ask(w, keys_[0])
        c.check("a new deployment of the same failed version loads it", w.api.health().body[HAI]["candidate"] == CANDIDATE)
    with world(count=40, percent=50) as w:
        canary_keys = [key for key in keys_ if canary.route(w.api.deployment(HAI), key.split(":")[0], 2022, NOW) == "canary"]
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
        deploy.run(
            registry=w.registry,
            manifests=w.store,
            hai_type=HAI,
            candidate_version=CANDIDATE,
            percent=40,
            mode="canary",
            canary_ttl_hours=TTL_HOURS,
            research_run=RESEARCH,
            now=NOW,
            deployment_id=RUN_2,
        )
        w.clock.now += 31.0
        ask(w, keys_[0])
        c.check("the next deployment gives the ejected canary a fresh load", w.api.health().body[HAI]["candidate"] == CANDIDATE)


def case_rollback(c: Checker) -> None:
    """A rollback deployment removes the canary after the next refresh, releases its model and leaves the production alias untouched."""
    with world(count=40, percent=50) as w:
        c.check("the canary serves before the rollback", {ask(w, key).body["served_by"] for key in hospital_keys(40)} == {"stable", "canary"})
        outcome = deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=None, percent=0, mode="stable", deployment_id=RUN_2)
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
        first = deploy.run(
            registry=w.registry,
            manifests=w.store,
            hai_type=HAI,
            candidate_version=CANDIDATE,
            percent=10,
            mode="canary",
            canary_ttl_hours=TTL_HOURS,
            research_run=RESEARCH,
            now=NOW,
            deployment_id=RUN_1,
        )
        c.check("the first deploy writes", first.status == "written" and first.deployment.stable_version == PRODUCTION)
        stored = w.store.read_active(HAI)
        again = deploy.run(
            registry=w.registry,
            manifests=w.store,
            hai_type=HAI,
            candidate_version=CANDIDATE,
            percent=10,
            mode="canary",
            canary_ttl_hours=TTL_HOURS,
            research_run=RESEARCH,
            now=NOW,
            deployment_id=RUN_1,
        )
        c.check("a retry changes nothing", again.status == "unchanged" and w.store.read_active(HAI) == stored and w.store.writes == 3)
        c.check(
            "a retry under the same id with other parameters is refused",
            refused(
                lambda: deploy.run(
                    registry=w.registry,
                    manifests=w.store,
                    hai_type=HAI,
                    candidate_version=CANDIDATE,
                    percent=30,
                    mode="canary",
                    canary_ttl_hours=TTL_HOURS,
                    research_run=RESEARCH,
                    now=NOW,
                    deployment_id=RUN_1,
                )
            ),
        )
        c.check("the refused retry changed nothing", w.store.read_active(HAI) == stored)
        c.check(
            "the candidate must differ from production",
            refused(
                lambda: deploy.run(
                    registry=w.registry,
                    manifests=w.store,
                    hai_type=HAI,
                    candidate_version=PRODUCTION,
                    percent=10,
                    mode="canary",
                    canary_ttl_hours=TTL_HOURS,
                    research_run=RESEARCH,
                    now=NOW,
                    deployment_id=RUN_2,
                )
            ),
        )
        c.check(
            "an unknown version is refused",
            refused(
                lambda: deploy.run(
                    registry=w.registry,
                    manifests=w.store,
                    hai_type=HAI,
                    candidate_version="99",
                    percent=10,
                    mode="canary",
                    canary_ttl_hours=TTL_HOURS,
                    research_run=RESEARCH,
                    now=NOW,
                    deployment_id=RUN_2,
                )
            ),
        )
        w.registry.versions["5"] = {}
        c.check(
            "a version without a completed bundle is refused",
            refused(
                lambda: deploy.run(
                    registry=w.registry,
                    manifests=w.store,
                    hai_type=HAI,
                    candidate_version="5",
                    percent=10,
                    mode="canary",
                    canary_ttl_hours=TTL_HOURS,
                    research_run=RESEARCH,
                    now=NOW,
                    deployment_id=RUN_2,
                )
            ),
        )
        for percent in (0, 51, -1):
            c.check(f"a canary percent of {percent} is refused", refused(partial(canary_deploy, w, percent)))
        c.check(
            "a rollback with a candidate is refused",
            refused(
                lambda: deploy.run(
                    registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=CANDIDATE, percent=0, mode="stable", deployment_id=RUN_2
                )
            ),
        )
        w.registry.aliases.clear()
        c.check(
            "a deploy with no production model is refused",
            refused(
                lambda: deploy.run(
                    registry=w.registry,
                    manifests=w.store,
                    hai_type=HAI,
                    candidate_version=CANDIDATE,
                    percent=10,
                    mode="canary",
                    canary_ttl_hours=TTL_HOURS,
                    research_run=RESEARCH,
                    now=NOW,
                    deployment_id=RUN_3,
                )
            ),
        )
        c.check("the refusals changed nothing and the alias was never written", w.store.read_active(HAI) == stored and w.registry.alias_writes == 0)
        w.registry.aliases["production"] = PRODUCTION
        c.check(
            "a deployment id that is not a run id is refused",
            refused(
                lambda: deploy.run(
                    registry=w.registry,
                    manifests=w.store,
                    hai_type=HAI,
                    candidate_version=CANDIDATE,
                    percent=10,
                    mode="canary",
                    canary_ttl_hours=TTL_HOURS,
                    research_run=RESEARCH,
                    now=NOW,
                    deployment_id="../x",
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
    request = {"hai_type": HAI, "candidate_version": CANDIDATE, "percent": 10, "deploy_mode": "canary", "canary_ttl_hours": TTL_HOURS, "research_run": RESEARCH}
    environment = {"SERVING_REQUEST": json.dumps(request), "MODEL_API_MANIFEST_URI": "s3://example-bucket/serving/deployments"}
    client, registry = S3Client(), Registry()
    code, out, err = run_deploy_job(client, registry, RUN_1, environment)
    printed = json.loads(out) if out.strip() else {}
    c.check("the job deploys and prints the result", code == 0 and printed.get("status") == "written" and printed.get("candidate_version") == CANDIDATE)
    c.check(
        "the records are under the fixed prefix and nowhere else",
        sorted(client.objects)
        == [
            f"serving/deployments/active/{HAI}.json",
            f"serving/deployments/canaries/{HAI}/{RESEARCH}-{CANDIDATE}.json",
            f"serving/deployments/history/{HAI}/{RUN_1}.json",
        ],
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
    stable = {"hai_type": HAI, "candidate_version": None, "percent": 0, "deploy_mode": "stable", "canary_ttl_hours": None, "research_run": None}
    fresh = S3Client()
    code, out, err = run_deploy_job(fresh, registry, RUN_3, environment | {"SERVING_REQUEST": json.dumps(stable)})
    printed = json.loads(out) if out.strip() else {}
    c.check(
        "a stable request writes the production version alone",
        code == 0 and printed.get("stable_version") == PRODUCTION and printed.get("candidate_version") is None and printed.get("percent") == 0,
    )
    code, out, err = run_deploy_job(fresh, registry, RUN_3, environment | {"SERVING_REQUEST": json.dumps(stable)}, ["--request", "request.json"])
    c.check("the job takes no request file", code == 2 and "unrecognized arguments" in err and not out)
    c.check("no run moved the production alias", registry.alias_writes == 0)


class LostWrite(manifests.FileManifestStore):
    """A manifest store whose active write is acknowledged and never lands, as a lost write the deploy has to notice."""

    def replace_active(self, hai_type: str, body: bytes, token: str | None) -> Any:
        return None


class AlteredWrite(manifests.FileManifestStore):
    """A manifest store that persists the active record with one field changed, as a write the service would refuse or misread."""

    def __init__(self, folder: Path, field: str, value: Any) -> None:
        super().__init__(folder)
        self.field, self.value = field, value

    def replace_active(self, hai_type: str, body: bytes, token: str | None) -> Any:
        altered = json.loads(body) | {self.field: self.value}
        return super().replace_active(hai_type, json.dumps(altered).encode(), token)


def case_deploy_verifies_the_active_version(c: Checker) -> None:
    """A deploy reports success only when the active deployment read back is the one it wrote, with the production version as stable and no candidate."""
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        store = LostWrite(Path(folder))
        refused = rejected(
            lambda: deploy.run(registry=Registry(), manifests=store, hai_type=HAI, candidate_version=None, percent=0, mode="stable", deployment_id=RUN_1)
        )
        c.check("a stable deploy whose active write did not land is refused", refused and store.read_active(HAI) is None)
    for changed, value in (("hai_type", "HAI_2"), ("sequence", 9)):
        with tempfile.TemporaryDirectory(prefix="serving_") as folder:
            altered = AlteredWrite(Path(folder), changed, value)
            refused = rejected(
                lambda altered=altered: deploy.run(
                    registry=Registry(), manifests=altered, hai_type=HAI, candidate_version=None, percent=0, mode="stable", deployment_id=RUN_1
                )
            )
            c.check(f"a stable deploy whose active record has another {changed} is refused", refused)
            retry = rejected(
                lambda folder=folder: deploy.run(
                    registry=Registry(),
                    manifests=manifests.FileManifestStore(Path(folder)),
                    hai_type=HAI,
                    candidate_version=None,
                    percent=0,
                    mode="stable",
                    deployment_id=RUN_1,
                )
            )
            c.check(f"the retry of that deploy keeps refusing the record with another {changed}", retry)
    with world(deployment=False) as w:
        outcome = deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=None, percent=0, mode="stable", deployment_id=RUN_1)
        active = w.store.read_active(HAI)
        read_back = canary.Deployment.parse(active.body) if active is not None else None
        c.check(
            "a stable deploy leaves the production version active with no candidate",
            outcome.status == "written"
            and read_back is not None
            and (read_back.stable_version, read_back.candidate_version, read_back.percent) == (PRODUCTION, None, 0),
        )


def active_body(store: Any) -> bytes:
    """The body of the active deployment record of the test type, which the case has written."""
    stored = store.read_active(HAI)
    if stored is None:
        raise AssertionError("no active deployment is written")
    return bytes(stored.body)


def traffic_of(w: World, count: int = 200) -> dict[str, int]:
    """How many of the first ``count`` hospital-windows each model version answers, after the API refreshed."""
    w.clock.now += 31
    counts: dict[str, int] = {}
    for key in hospital_keys(count):
        version = str(ask(w, key).body.get("model_version"))
        counts[version] = counts.get(version, 0) + 1
    return counts


def case_canary_expiry(c: Checker) -> None:
    """The API serves a canary's share only while the wall clock is inside its recorded lifetime. After it, before it began, or on a record whose lifetime is
    missing, malformed or beyond the bound, it serves the stable version alone, with no step and no refresh in between. A stable deployment has no expiry."""
    with world(count=200, percent=10, rate=10**6, burst=10**6, capacity=10**6) as w:
        written = canary.Deployment.parse(active_body(w.store))
        c.check(
            "a canary deployment carries its write time and its expiry, a fixed number of hours later",
            (written.written_at, written.expires_at) == (NOW, NOW + TTL_HOURS * 3600),
        )
        c.check("before its expiry the canary serves its share", set(traffic_of(w)) == {PRODUCTION, CANDIDATE})
        w.wall.now = NOW + TTL_HOURS * 3600 - 1
        c.check("one second before its expiry it still does", CANDIDATE in traffic_of(w))
        w.wall.now = NOW + TTL_HOURS * 3600
        c.check("at its expiry it serves nothing, with no refresh and no later step", traffic_of(w) == {PRODUCTION: 200})
        w.wall.now = NOW + 365 * 86_400
        c.check("and a year later still nothing", traffic_of(w) == {PRODUCTION: 200})
        w.wall.now = NOW - 1
        c.check("a clock behind the write time (skew) serves the stable version alone", traffic_of(w) == {PRODUCTION: 200})
        w.wall.now = NOW + 60
        c.check("a clock inside the lifetime serves the canary again", CANDIDATE in traffic_of(w))
    base = {"version": 1, "hai_type": HAI, "deployment_id": RUN_1, "stable_version": PRODUCTION, "candidate_version": CANDIDATE, "percent": 10, "sequence": 1}
    ttl = TTL_HOURS * 3600
    variants: dict[str, dict[str, Any]] = {
        "no written time and no expiry": {},
        "no expiry": {"written_at": NOW},
        "no written time": {"expires_at": NOW + ttl},
        "an expiry that is text": {"written_at": NOW, "expires_at": str(NOW + ttl)},
        "a written time that is text": {"written_at": str(NOW), "expires_at": NOW + ttl},
        "a boolean expiry": {"written_at": NOW, "expires_at": True},
        "a fractional expiry": {"written_at": NOW, "expires_at": NOW + ttl + 0.5},
        "an expiry that is null": {"written_at": NOW, "expires_at": None},
        "an expiry before the write": {"written_at": NOW, "expires_at": NOW - 10},
        "an expiry equal to the write": {"written_at": NOW, "expires_at": NOW},
        "a negative lifetime": {"written_at": NOW + ttl, "expires_at": NOW},
        "a lifetime beyond the bound": {"written_at": NOW, "expires_at": NOW + canary.MAX_TTL_HOURS * 3600 + 1},
        "an expiry far in the future": {"written_at": NOW, "expires_at": NOW + 10 * 365 * 86_400},
    }
    for label, fields in variants.items():
        with world(count=200, deployment=False, rate=10**6, burst=10**6, capacity=10**6) as w:
            record = json.dumps(base | fields).encode()
            w.store.replace_active(HAI, record, None)
            c.check(f"a canary record with {label} serves the stable version alone", traffic_of(w) == {PRODUCTION: 200})
    with world(count=200, deployment=False, rate=10**6, burst=10**6, capacity=10**6) as w:
        w.store.replace_active(HAI, json.dumps(base | {"written_at": NOW, "expires_at": NOW + ttl}).encode(), None)
        c.check("the same record with a good lifetime serves its share", CANDIDATE in traffic_of(w))
    with world(count=200, deployment=False, rate=10**6, burst=10**6, capacity=10**6) as w:
        deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=None, percent=0, mode="stable", deployment_id=RUN_1)
        document = json.loads(active_body(w.store))
        w.wall.now = NOW + 365 * 86_400
        c.check(
            "a stable deployment carries no expiry and serves for ever",
            "written_at" not in document and "expires_at" not in document and traffic_of(w) == {PRODUCTION: 200},
        )
    c.check(
        "a stable deployment cannot be given an expiry",
        rejected(lambda: canary.Deployment(HAI, RUN_1, PRODUCTION, None, 0, written_at=NOW, expires_at=NOW + ttl)),
    )
    with world(count=200, deployment=False, rate=10**6, burst=10**6, capacity=10**6) as w:
        deploy.run(
            registry=w.registry,
            manifests=w.store,
            hai_type=HAI,
            candidate_version=CANDIDATE,
            percent=10,
            mode="canary",
            canary_ttl_hours=TTL_HOURS,
            research_run=RESEARCH,
            now=NOW,
            deployment_id=RUN_1,
        )
        w.registry.aliases["production"] = CANDIDATE
        deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=None, percent=0, mode="stable", deployment_id=RUN_2)
        c.check(
            "only a promotion turns a canary into stable: the stable deployment replaces the canary and serves the promoted version",
            traffic_of(w) == {CANDIDATE: 200},
        )


def case_deploy_expiry(c: Checker) -> None:
    """A canary deploy needs a lifetime within the bound and a write time, writes both, and a retry never extends them. A stable deploy takes none."""

    def run(mode: str = "canary", ttl: Any = TTL_HOURS, now: Any = NOW, run_id: str = RUN_1, folder: Any = None, research: Any = RESEARCH) -> deploy.Outcome:
        return deploy.run(
            registry=Registry(),
            manifests=manifests.FileManifestStore(folder),
            hai_type=HAI,
            candidate_version=CANDIDATE if mode == "canary" else None,
            percent=10 if mode == "canary" else 0,
            mode=mode,
            deployment_id=run_id,
            canary_ttl_hours=ttl,
            now=now,
            research_run=research if mode == "canary" else None,
        )

    for label, arguments in (
        ("no lifetime", {"ttl": None}),
        ("a lifetime of zero", {"ttl": 0}),
        ("a negative lifetime", {"ttl": -1}),
        ("a lifetime beyond the bound", {"ttl": canary.MAX_TTL_HOURS + 1}),
        ("a boolean lifetime", {"ttl": True}),
        ("a fractional lifetime", {"ttl": 1.5}),
        ("no write time", {"now": None}),
        ("a boolean write time", {"now": True}),
        ("a zero write time", {"now": 0}),
    ):
        with tempfile.TemporaryDirectory(prefix="serving_") as folder:
            c.check(
                f"a canary deploy with {label} is refused and writes nothing",
                rejected(lambda folder=folder, arguments=arguments: run(folder=Path(folder), **arguments)) and not list(Path(folder).rglob("*.json")),
            )
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        c.check("a stable deploy with a lifetime is refused", rejected(lambda: run("stable", folder=Path(folder))))
        c.check("a stable deploy with a lifetime and no write time is refused", rejected(lambda: run("stable", now=None, folder=Path(folder))))
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        written = run(ttl=canary.MAX_TTL_HOURS, folder=Path(folder)).deployment
        c.check("the longest lifetime is allowed", written.expires_at == NOW + canary.MAX_TTL_HOURS * 3600)
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        first = run(folder=Path(folder)).deployment
        again = run(now=NOW + 5 * 3600, folder=Path(folder))
        document = json.loads(active_body(manifests.FileManifestStore(Path(folder))))
        c.check(
            "a retry of the same deployment later changes nothing and does not extend the expiry",
            again.status == "unchanged"
            and (again.deployment.written_at, again.deployment.expires_at) == (first.written_at, first.expires_at)
            and document["expires_at"] == NOW + TTL_HOURS * 3600,
        )
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        run(folder=Path(folder))
        later = run(now=NOW + 5 * 3600, run_id=RUN_2, folder=Path(folder)).deployment
        c.check(
            "a new run id for the same research canary is a new deployment that keeps the first write time and expiry",
            (later.written_at, later.expires_at) == (NOW, NOW + TTL_HOURS * 3600) and later.sequence == 2 and later.deployment_id == RUN_2,
        )
        ended = NOW + TTL_HOURS * 3600
        before = sorted(path.name for path in Path(folder).rglob("*.json"))
        c.check(
            "a new run id for the same research canary at or after its expiry is refused and writes nothing",
            all(rejected(lambda at=at: run(now=at, run_id=RUN_3, folder=Path(folder))) for at in (ended, ended + 3600))
            and sorted(path.name for path in Path(folder).rglob("*.json")) == before,
        )
        again = run(now=ended + 3600, run_id=RUN_2, folder=Path(folder))
        c.check("a retry of the deployment that is active after the expiry is unchanged", again.status == "unchanged" and again.deployment.expires_at == ended)
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        run(folder=Path(folder))
        c.check(
            "another research canary is refused a lifetime of the candidate while the deployment of the first holds one that it cannot be shown to own",
            rejected(lambda: run(now=NOW + 5 * 3600, run_id=RUN_2, folder=Path(folder), research="research_bbbbbbb2")),
        )
        deploy.run(
            registry=Registry(),
            manifests=manifests.FileManifestStore(Path(folder)),
            hai_type=HAI,
            candidate_version=None,
            percent=0,
            mode="stable",
            deployment_id=RUN_3,
        )
        other = run(now=NOW + 5 * 3600, run_id=RUN_2, folder=Path(folder), research="research_bbbbbbb2").deployment
        c.check(
            "another research canary has a lifetime of its own once the first is replaced",
            other.written_at == NOW + 5 * 3600 and other.expires_at == NOW + 5 * 3600 + TTL_HOURS * 3600 and other.sequence == 3,
        )
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        crashing = CrashingStore(Path(folder))
        crashing.crash_next_replace = True
        with contextlib.suppress(OSError):
            deploy.run(
                registry=Registry(),
                manifests=crashing,
                hai_type=HAI,
                candidate_version=CANDIDATE,
                percent=10,
                mode="canary",
                deployment_id=RUN_1,
                canary_ttl_hours=TTL_HOURS,
                now=NOW,
                research_run=RESEARCH,
            )
        finished = run(now=NOW + 3600, folder=Path(folder)).deployment
        c.check("a deploy that stopped after its binding finishes on its retry with the first expiry", finished.expires_at == NOW + TTL_HOURS * 3600)
    for label, research_arguments in (
        ("no research run", {"research": None}),
        ("a malformed research run", {"research": "latest"}),
    ):
        with tempfile.TemporaryDirectory(prefix="serving_") as folder:
            c.check(
                f"a canary deploy with {label} is refused and writes nothing",
                rejected(lambda folder=folder, research_arguments=research_arguments: run(folder=Path(folder), **research_arguments))
                and not list(Path(folder).rglob("*.json")),
            )
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        c.check(
            "a stable deploy with a research run is refused",
            rejected(
                lambda: deploy.run(
                    registry=Registry(),
                    manifests=manifests.FileManifestStore(Path(folder)),
                    hai_type=HAI,
                    candidate_version=None,
                    percent=0,
                    mode="stable",
                    deployment_id=RUN_1,
                    research_run=RESEARCH,
                )
            ),
        )


def case_manual_run_cannot_renew(c: Checker) -> None:
    """The shipped serve form, the launch preflight and the deploy job against the manifest store: a canary has the lifetime of its first deployment, and a
    manual run under another run id for the same research canary neither renews it nor opens a new traffic window after a stored hold."""
    from orchestration import training_contract
    from scripts.orchestration import airflow_mode_cases as air

    challenger = {"model": "m2", "version": CANDIDATE, "verdict": "pass", "source_run": "mlrun4"}
    first_expiry = NOW + TTL_HOURS * 3600
    active_key = f"serving/deployments/active/{HAI}.json"

    def submit(rig: Any, client: S3Client, registry: Registry, run_id: str, now: int) -> tuple[bool, int, dict[str, Any]]:
        """Submit the form of one run, then run the job it launched at ``now``: whether the form passed, the job's exit code and the active record."""
        rig.stage_runs(run_id)
        with air.world(rig, "serving_deploy_dag", air.SERVE_CANARY | {"run_id": run_id}) as current:
            current.strict_enum = True
            before = rig.tasks_started()
            states = current.run_dag("serving_deploy_dag.py")
            launched = rig.tasks_started() > before
            code = 0
            if launched:
                environment = rig.environment(before)["serve"] | {"MODEL_API_MANIFEST_URI": "s3://example-bucket/serving/deployments"}
                with mock.patch.object(deploy_job.time, "time", return_value=float(now)):
                    code, _, _ = run_deploy_job(client, registry, run_id, environment)
        stored = client.objects.get(active_key)
        return set(states.values()) == {"success"}, code, json.loads(stored[1]) if stored else {}

    def traffic_at(client: S3Client, when: float) -> dict[str, int]:
        with world(count=200, deployment=False, rate=10**6, burst=10**6, capacity=10**6) as w:
            w.store.replace_active(HAI, client.objects[active_key][1], None)
            w.wall.now = when
            return traffic_of(w)

    def fresh() -> tuple[Any, S3Client, Registry]:
        rig = air.Rig()
        air.research_record_of(rig, challenger)
        return rig, S3Client(), Registry()

    rig, client, registry = fresh()
    passed, code, first = submit(rig, client, registry, RUN_1, NOW)
    c.check("the first form deploys the canary with its lifetime", passed and code == 0 and first.get("expires_at") == first_expiry)
    passed, code, again = submit(rig, client, registry, RUN_1, NOW + 5 * 3600)
    c.check("the same form again changes nothing", passed and code == 0 and again == first)
    c.check("the canary serves its share before the expiry", CANDIDATE in traffic_at(client, first_expiry - 1))

    rig, client, registry = fresh()
    submit(rig, client, registry, RUN_1, NOW)
    passed, code, second = submit(rig, client, registry, RUN_2, NOW + 23 * 3600)
    c.check(
        "a manual run under another run id keeps the first expiry and write time and mints no new lifetime",
        passed and code == 0 and second.get("deployment_id") == RUN_2 and second.get("sequence") == 2 and second.get("expires_at") == first_expiry,
    )
    c.check("the canary of the second run stops getting traffic at the first expiry", traffic_at(client, first_expiry + 3600) == {PRODUCTION: 200})
    c.check("the second run is also served only before the first expiry", CANDIDATE in traffic_at(client, first_expiry - 1))
    c.check("no run moved the production alias", registry.alias_writes == 0)
    before = dict(client.objects)
    passed, code, _ = submit(rig, client, registry, "serving_aaaaaaa4", first_expiry + 3600)
    c.check("a manual run under another run id after the expiry opens no new window and writes nothing", code == 2 and client.objects == before)
    c.check("the expired canary still serves the stable version alone", traffic_at(client, first_expiry + 7200) == {PRODUCTION: 200})

    rig, client, registry = fresh()
    submit(rig, client, registry, RUN_1, NOW)
    hold = {
        "schema": 1,
        "research_run": air.SERVE_RESEARCH,
        "hai_type": HAI,
        "model": "m2",
        "version": CANDIDATE,
        "decision": "hold",
        "failed": ["requests_answered"],
    }
    rig.stage.objects[air.record_key("canary", air.SERVE_RESEARCH, HAI)] = json.dumps(hold).encode()
    before = dict(client.objects)
    started = rig.tasks_started()
    passed, code, held = submit(rig, client, registry, RUN_2, NOW + 23 * 3600)
    c.check(
        "a manual run after a stored hold is refused before any task starts and writes nothing",
        not passed and rig.tasks_started() == started and client.objects == before and held.get("deployment_id") == RUN_1,
    )
    c.check("the held canary stops getting traffic at its first expiry", traffic_at(client, first_expiry + 3600) == {PRODUCTION: 200})
    for label, changed in (
        ("a result of another candidate", hold | {"version": "5"}),
        ("a result with no decision", {k: v for k, v in hold.items() if k != "decision"}),
    ):
        rig.stage.objects[air.record_key("canary", air.SERVE_RESEARCH, HAI)] = json.dumps(changed).encode()
        passed, code, _ = submit(rig, client, registry, RUN_2, NOW + 23 * 3600)
        c.check(f"a manual run after {label} is refused and writes nothing", not passed and client.objects == before)

    rig, client, registry = fresh()
    chain_id = training_contract.chain_run_id(air.SERVE_RESEARCH, HAI, "canary_deploy")
    old_record = canary.Deployment(HAI, chain_id, PRODUCTION, CANDIDATE, 10, 1, NOW, first_expiry).dump()
    client.objects[f"serving/deployments/history/{HAI}/{chain_id}.json"] = ('"old1"', old_record)
    client.objects[active_key] = ('"old2"', old_record)
    before = dict(client.objects)
    passed, code, _ = submit(rig, client, registry, RUN_2, first_expiry + 3600)
    c.check(
        "a manual run after the expiry over a deployment written before the binding existed opens no new window and writes nothing",
        code == 2 and client.objects == before,
    )
    c.check("that expired canary serves the stable version alone", traffic_at(client, first_expiry + 3600) == {PRODUCTION: 200})
    passed, code, kept = submit(rig, client, registry, RUN_3, first_expiry - 3600)
    c.check(
        "a manual run before that expiry keeps the first expiry", passed and code == 0 and kept.get("expires_at") == first_expiry and kept.get("sequence") == 2
    )


def case_canary_upgrade_state(c: Checker) -> None:
    """Canary deployments written before the binding existed keep their lifetime: the first deployment of a research canary is found by its deterministic
    chain id and its times carry into the new binding, a lifetime whose research canary cannot be established refuses the deploy, and nothing is written."""
    from orchestration import training_contract

    chain_id = training_contract.chain_run_id(RESEARCH, HAI, "canary_deploy")
    first_expiry = NOW + TTL_HOURS * 3600

    def old_record(deployment_id: str, written: Any, expires: Any, candidate: str = CANDIDATE, sequence: int = 1) -> bytes:
        return canary.Deployment(HAI, deployment_id, PRODUCTION, candidate, 10, sequence, written, expires).dump()

    def old_state(folder: str, history: bytes | None, active: bytes | None) -> manifests.FileManifestStore:
        store = manifests.FileManifestStore(Path(folder))
        if history is not None:
            store.put_history(HAI, json.loads(history)["deployment_id"], history)
        if active is not None:
            store.replace_active(HAI, active, None)
        return store

    def go(folder: str, run_id: str, now: int, research: str = RESEARCH) -> deploy.Outcome:
        return deploy.run(
            registry=Registry(),
            manifests=manifests.FileManifestStore(Path(folder)),
            hai_type=HAI,
            candidate_version=CANDIDATE,
            percent=10,
            mode="canary",
            deployment_id=run_id,
            canary_ttl_hours=TTL_HOURS,
            now=now,
            research_run=research,
        )

    def files(folder: str) -> dict[str, bytes]:
        return {str(path.relative_to(folder)): path.read_bytes() for path in sorted(Path(folder).rglob("*.json"))}

    chain = old_record(chain_id, NOW, first_expiry)
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        old_state(folder, chain, chain)
        before = files(folder)
        c.check(
            "a fresh run after the expiry of a deployment that has no binding is refused and writes nothing",
            all(rejected(lambda at=at: go(folder, RUN_2, at)) for at in (first_expiry, first_expiry + 3600)) and files(folder) == before,
        )
        kept = go(folder, RUN_2, NOW + 3 * 3600).deployment
        binding = json.loads(files(folder)[f"canaries/{HAI}/{RESEARCH}-{CANDIDATE}.json"])
        c.check(
            "a fresh run before the expiry carries the first write time and expiry into the binding and the new deployment",
            (kept.written_at, kept.expires_at, kept.sequence) == (NOW, first_expiry, 2)
            and (binding["written_at"], binding["expires_at"], binding["deployment_id"]) == (NOW, first_expiry, chain_id),
        )
        c.check("the carried lifetime then ends the canary like any bound one", rejected(lambda: go(folder, RUN_3, first_expiry + 1)))
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        renewed = old_record(RUN_1, NOW + 30 * 3600, NOW + 54 * 3600, sequence=2)
        old_state(folder, chain, None)
        old_state(folder, renewed, renewed)
        before = files(folder)
        c.check(
            "a deployment renewed before the binding existed does not hide the first lifetime of the chain deployment",
            rejected(lambda: go(folder, RUN_2, NOW + 31 * 3600)) and files(folder) == before,
        )
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        old_state(folder, chain, chain)
        before = files(folder)
        again = go(folder, chain_id, first_expiry + 3600)
        c.check(
            "a retry of the chain deployment that has no binding is unchanged after the expiry and writes nothing",
            again.status == "unchanged" and (again.deployment.written_at, again.deployment.expires_at) == (NOW, first_expiry) and files(folder) == before,
        )
    for label, record in (
        ("a deployment under a manual run id", old_record(RUN_1, NOW, first_expiry)),
        ("a deployment of the candidate with a malformed expiry", old_record(RUN_1, NOW, "never")),
    ):
        with tempfile.TemporaryDirectory(prefix="serving_") as folder:
            old_state(folder, record, record)
            before = files(folder)
            c.check(
                f"a run over {label} that has no binding and no chain deployment is refused, as its lifetime cannot be established, and writes nothing",
                all(rejected(lambda at=at: go(folder, RUN_2, at)) for at in (NOW + 3600, first_expiry + 3600)) and files(folder) == before,
            )
            c.check(
                f"a retry of {label} that has no binding is refused and writes nothing",
                rejected(lambda: go(folder, RUN_1, NOW + 3600)) and files(folder) == before,
            )
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        unclocked = old_record(RUN_1, None, None)
        old_state(folder, unclocked, unclocked)
        c.check("a retry of that unclocked deployment is unchanged", go(folder, RUN_1, NOW + 7200).status == "unchanged")
        fresh = go(folder, RUN_2, NOW + 3600).deployment
        c.check(
            "a deployment from before any expiry holds no lifetime, so the research canary gets its own",
            (fresh.written_at, fresh.expires_at, fresh.sequence) == (NOW + 3600, NOW + 3600 + TTL_HOURS * 3600, 2),
        )
    with tempfile.TemporaryDirectory(prefix="serving_") as folder:
        stable = canary.Deployment(HAI, RUN_1, PRODUCTION, None, 0).dump()
        old_state(folder, stable, stable)
        fresh = go(folder, RUN_2, NOW + 3600).deployment
        c.check("a stable deployment holds no lifetime either", fresh.written_at == NOW + 3600 and fresh.sequence == 2)


def case_canary_retry_identity(c: Checker) -> None:
    """A deploy-job retry names its research canary: the saved deployment must belong to that canary's binding and keep its times, or the retry is refused."""
    other = "research_bbbbbbb2"
    request = {"hai_type": HAI, "candidate_version": CANDIDATE, "percent": 10, "deploy_mode": "canary", "canary_ttl_hours": TTL_HOURS, "research_run": RESEARCH}
    stable = {"hai_type": HAI, "candidate_version": None, "percent": 0, "deploy_mode": "stable", "canary_ttl_hours": None, "research_run": None}
    client, registry = S3Client(), Registry()

    def job(run: str, body: dict[str, Any], now: int) -> tuple[int, dict[str, Any]]:
        environment = {"SERVING_REQUEST": json.dumps(body), "MODEL_API_MANIFEST_URI": "s3://example-bucket/serving/deployments"}
        with mock.patch.object(deploy_job.time, "time", return_value=float(now)):
            code, out, _ = run_deploy_job(client, registry, run, environment)
        return code, json.loads(out) if out.strip() else {}

    first_expiry = NOW + TTL_HOURS * 3600
    c.check("research A deploys its canary", job(RUN_1, request, NOW)[0] == 0)
    c.check("a stable deploy replaces it", job(RUN_3, stable, NOW + 3600)[0] == 0)
    later = first_expiry + 3600
    c.check("research B deploys the same candidate with a lifetime of its own after A ended", job(RUN_2, request | {"research_run": other}, later)[0] == 0)
    saved = json.loads(client.objects[f"serving/deployments/active/{HAI}.json"][1])
    c.check("B's deployment holds B's times", (saved["written_at"], saved["expires_at"]) == (later, later + TTL_HOURS * 3600))
    before = dict(client.objects)
    code, printed = job(RUN_2, request | {"research_run": other}, later + 3600)
    c.check("the retry naming B is unchanged", code == 0 and printed.get("status") == "unchanged" and client.objects == before)
    code, printed = job(RUN_2, request, later + 3600)
    c.check("the retry naming A over B's deployment is refused and writes nothing", code == 2 and not printed and client.objects == before)
    code, printed = job(RUN_1, request, later + 3600)
    c.check("the retry of A's replaced deployment is still refused as replaced", code == 2 and client.objects == before)


def case_expiry_is_not_requestable(c: Checker) -> None:
    """No request field sets, extends or removes an expiry: the closed request has the type and the mode only, the lifetime comes from the job table, the job
    refuses a lifetime outside the bound, and the job table schema bounds it."""
    from jsonschema import Draft202012Validator

    from orchestration import training_contract

    ok = {"hai_type": HAI, "deploy_mode": "canary"}
    for name in ("expires_at", "written_at", "canary_ttl_hours", "ttl", "expiry"):
        c.check(f"a closed request with {name} is refused", rejected(training_contract.serving_parameters_for, ok | {name: 99}))
    environment = {"MODEL_API_MANIFEST_URI": "s3://example-bucket/serving/deployments"}
    request = {"hai_type": HAI, "candidate_version": CANDIDATE, "percent": 10, "deploy_mode": "canary", "canary_ttl_hours": TTL_HOURS, "research_run": RESEARCH}
    with mock.patch.object(deploy_job.time, "time", return_value=float(NOW)):
        client, registry = S3Client(), Registry()
        code, out, err = run_deploy_job(client, registry, RUN_1, environment | {"SERVING_REQUEST": json.dumps(request)})
        active = json.loads(client.objects[f"serving/deployments/active/{HAI}.json"][1]) if code == 0 else {}
        c.check(
            "the job stamps the write time from its clock and the expiry from the derived lifetime",
            (active.get("written_at"), active.get("expires_at")) == (NOW, NOW + TTL_HOURS * 3600),
        )
        for label, changed in (
            ("no lifetime", {k: v for k, v in request.items() if k != "canary_ttl_hours"}),
            ("a lifetime beyond the bound", request | {"canary_ttl_hours": canary.MAX_TTL_HOURS + 1}),
            ("a lifetime of zero", request | {"canary_ttl_hours": 0}),
            ("a stable request with a lifetime", {**request, "deploy_mode": "stable", "candidate_version": None, "percent": 0}),
            ("an expiry field", request | {"expires_at": NOW + 10**9}),
        ):
            fresh = S3Client()
            code, out, err = run_deploy_job(fresh, registry, RUN_2, environment | {"SERVING_REQUEST": json.dumps(changed)})
            c.check(f"the job refuses {label} and writes nothing", code == 2 and "serve_deploy refused" in err and not fresh.objects)
    table = json.loads((ROOT / "config/orchestration/ecs_broker_jobs.json").read_text())
    schema = json.loads((ROOT / "config/orchestration/ecs_broker_jobs.schema.json").read_text())
    validator = Draft202012Validator(schema)
    c.check(
        "the shipped job table holds the lifetime of the serve job",
        table["jobs"]["serve"].get("canary_ttl_hours") == TTL_HOURS and not list(validator.iter_errors(table)),
    )
    for label, value in (("zero", 0), ("beyond the bound", canary.MAX_TTL_HOURS + 1), ("text", "24"), ("a boolean", True), ("a fraction", 1.5)):
        changed = json.loads(json.dumps(table))
        changed["jobs"]["serve"]["canary_ttl_hours"] = value
        c.check(f"the job table schema refuses a lifetime of {label}", bool(list(validator.iter_errors(changed))))
    missing = json.loads(json.dumps(table))
    del missing["jobs"]["serve"]["canary_ttl_hours"]
    c.check("the job table schema refuses a serve job with no lifetime", bool(list(validator.iter_errors(missing))))


def case_chain_rollout(c: Checker) -> None:
    """The promotion chain against the real deployment action, manifest store and API refresh: the canary is live before the check, a passed canary ends as
    the one stable version after the API refreshes, a held or failed canary stops getting traffic at its expiry with no later step, and the notice says only
    what happened."""
    from orchestration import ecs_preflight, promotion_chain, training_contract
    from scripts.orchestration import research_chain_cases as chain

    keys_ = hospital_keys(200)

    def health(w: World) -> tuple[Any, ...]:
        w.clock.now += 31
        ask(w)
        body = w.api.health().body[HAI]
        return (body["stable"], body["candidate"])

    def traffic(w: World) -> set[Any]:
        """The model versions that answer 200 hospital-windows, after the API refreshed."""
        w.clock.now += 31
        return {ask(w, key).body.get("model_version") for key in keys_}

    def recorder_for(research: str, challenger: dict[str, Any], decision: str | None = "promote") -> Any:
        """A recorder whose bucket holds the research record of the research run, and whose canary result, when there is one, is stored when the check runs."""
        recorder = chain.Recorder({})
        recorder.objects[training_contract.record_key("research", research, HAI)] = chain.research_record(HAI, challenger)
        if decision is not None:
            recorder.pending[training_contract.record_key("canary", research, HAI)] = chain.canary_record(
                HAI, decision, challenger=challenger, changes={"research_run": research}
            )
        return recorder

    def run_chain(
        w: World,
        recorder: Any,
        research: str = chain.RESEARCH,
        fail: tuple[str, str] | None = None,
        after_write: bool = False,
        release_fails: bool = False,
        check_fails: bool = False,
    ) -> tuple[list[str], list[tuple[Any, ...]], dict[str, Any]]:
        """Run the chain of the research run against the world's registry, manifest store and API, then read what the API serves at the end of the chain and
        after the canary's lifetime, with no step in between."""
        seen: dict[str, Any] = {}

        def launch(step: str, run: str, params: dict[str, Any]) -> None:
            recorder.calls.append((step, params["hai_type"], run, params))
            if step == "canary_check":
                recorder.objects.update(recorder.pending)
                seen["at_the_check"] = health(w)
                if check_fails:
                    raise promotion_chain.StepFailed("the canary check task failed")
            if step == "serve_deploy":
                if fail == (step, params["deploy_mode"]) and not after_write:
                    raise promotion_chain.StepFailed(f"{step} {params['deploy_mode']} failed")
                values = ecs_preflight.serving_values(params, research, 10, recorder.read, 24)
                request = json.loads(values["SERVING_REQUEST"])
                deploy.run(
                    registry=w.registry,
                    manifests=w.store,
                    hai_type=request["hai_type"],
                    candidate_version=request["candidate_version"],
                    percent=request["percent"],
                    mode=request["deploy_mode"],
                    deployment_id=run,
                    canary_ttl_hours=request["canary_ttl_hours"],
                    now=int(w.wall.now),
                    research_run=request["research_run"],
                )
                if fail == (step, params["deploy_mode"]):
                    raise promotion_chain.StepFailed(f"{step} {params['deploy_mode']} timed out after its write")
            if step == "promote":
                w.registry.aliases["production"] = params["model_version"]
            if step == "release_online" and release_fails:
                raise promotion_chain.StepFailed("the release task failed")

        failures = promotion_chain.run_types(research, [HAI], launch=launch, read=recorder.read)
        seen["after"] = health(w)
        seen["alias"] = w.registry.aliases["production"]
        seen["live"] = traffic(w)
        w.wall.now += TTL_HOURS * 3600
        seen["expired"] = traffic(w)
        return failures, recorder.calls, seen

    def scenario(decision: str | None = "promote", **options: Any) -> tuple[list[str], list[tuple[Any, ...]], dict[str, Any]]:
        with world(count=200, deployment=False, rate=10**6, burst=10**6, capacity=10**6) as w:
            return run_chain(w, recorder_for(chain.RESEARCH, chain.CHALLENGER, decision), **options)

    def names(calls: list[tuple[Any, ...]]) -> list[str]:
        return [f"{call[0]}:{call[3]['deploy_mode']}" if call[0] == "serve_deploy" else str(call[0]) for call in calls]

    both, stable_only, promoted = {PRODUCTION, CANDIDATE}, {PRODUCTION}, {CANDIDATE}
    failures, calls, seen = scenario()
    c.check(
        "a passed canary is deployed first, checked, promoted, released, rolled out stable-only and then announced",
        failures == [] and names(calls) == ["serve_deploy:canary", "canary_check", "promote", "release_online", "serve_deploy:stable", "owner_notice"],
    )
    c.check("the API serves the canary when the check runs", seen["at_the_check"] == (PRODUCTION, CANDIDATE))
    c.check(
        "after the rollout the API serves the promoted version alone, and the stable deployment does not expire",
        seen["after"] == (CANDIDATE, None) and seen["alias"] == CANDIDATE and seen["live"] == promoted and seen["expired"] == promoted,
    )
    c.check("the notice says promoted", calls[-1][3]["outcome"] == "promoted")
    failures, calls, seen = scenario(release_fails=True)
    c.check(
        "a failed release is reported, nothing is rolled out and the notice says the promotion did not roll out",
        len(failures) == 1 and "release" in failures[0] and "serve_deploy:stable" not in names(calls) and calls[-1][3]["outcome"] == "promoted_not_rolled_out",
    )
    c.check(
        "the earlier deployment serves its canary share until it expires and then the stable version alone",
        seen["after"] == (PRODUCTION, CANDIDATE) and seen["alias"] == CANDIDATE and seen["live"] == both and seen["expired"] == stable_only,
    )
    failures, calls, seen = scenario(fail=("serve_deploy", "stable"))
    c.check(
        "a failed rollout is reported, the notice says the promotion did not roll out and the canary expires",
        len(failures) == 1
        and "rollout" in failures[0]
        and calls[-1][3]["outcome"] == "promoted_not_rolled_out"
        and seen["live"] == both
        and seen["expired"] == stable_only,
    )
    ended = ["serve_deploy:canary", "canary_check", "owner_notice"]
    for label, decision, options, expected_failures in (
        ("a missing replay report, which stores a hold", "hold", {}, 0),
        ("a missing canary result", None, {}, 1),
        ("a failed canary check", "promote", {"check_fails": True}, 1),
    ):
        failures, calls, seen = scenario(decision, **options)
        c.check(
            f"{label} deploys nothing more and announces held, and its canary serves its share until its expiry and no traffic after it",
            len(failures) == expected_failures
            and names(calls) == ended
            and calls[-1][3]["outcome"] == "held"
            and seen["alias"] == PRODUCTION
            and seen["live"] == both
            and seen["expired"] == stable_only,
        )
    failures, calls, seen = scenario(fail=("serve_deploy", "canary"))
    c.check(
        "a canary deploy that failed before it wrote deploys nothing more and announces held",
        len(failures) == 1
        and "canary deploy" in failures[0]
        and names(calls) == ["serve_deploy:canary", "owner_notice"]
        and calls[-1][3]["outcome"] == "held"
        and seen["alias"] == PRODUCTION
        and seen["after"] == (None, None),
    )
    failures, calls, seen = scenario(fail=("serve_deploy", "canary"), after_write=True)
    c.check(
        "a canary deploy that timed out after it wrote is reported and announced held, and its canary expires with no later step",
        len(failures) == 1
        and names(calls) == ["serve_deploy:canary", "owner_notice"]
        and calls[-1][3]["outcome"] == "held"
        and seen["live"] == both
        and seen["expired"] == stable_only,
    )
    with world(count=200, deployment=False, rate=10**6, burst=10**6, capacity=10**6) as w:
        w.registry.aliases.pop("production", None)
        recorder = recorder_for(chain.RESEARCH, chain.CHALLENGER, None)
        recorder.objects[training_contract.record_key("research", chain.RESEARCH, HAI)] = chain.research_record(HAI, chain.CHALLENGER, production=None)
        failures, calls, seen = run_chain(w, recorder)
        c.check(
            "with no Production model the first champion is promoted with no canary, rolled out stable-only by the real deploy action and announced",
            failures == []
            and names(calls) == ["promote", "serve_deploy:stable", "owner_notice"]
            and calls[-1][3]["outcome"] == "promoted"
            and seen["alias"] == CANDIDATE
            and seen["after"] == (CANDIDATE, None)
            and seen["live"] == promoted
            and seen["expired"] == promoted,
        )
    other_research = training_contract.research_run_id("features_b45e0002")
    newer = {**chain.CHALLENGER, "version": "5"}
    with world(count=200, deployment=False, rate=10**6, burst=10**6, capacity=10**6) as w:
        first = run_chain(w, recorder_for(chain.RESEARCH, chain.CHALLENGER), release_fails=True)
        failures, calls, seen = run_chain(w, recorder_for(other_research, newer, "hold"), research=other_research, fail=("serve_deploy", "canary"))
        c.check(
            "after a promotion whose release failed, a failed next canary keeps stable 3 and never deploys the promoted version as stable",
            first[2]["after"] == (PRODUCTION, CANDIDATE)
            and first[2]["alias"] == CANDIDATE
            and seen["alias"] == CANDIDATE
            and "serve_deploy:stable" not in names(calls)
            and calls[-1][3]["outcome"] == "held"
            and len(failures) == 1
            and seen["expired"] == stable_only,
        )


def case_images(c: Checker) -> None:
    """The image list names the model API and page images, builds each from its own Dockerfile with the repository as context, and the platform stack
    gets their repositories behind one flag."""
    config = push_images.load_config()
    c.check("the image list is valid", push_images.check(config) == [])
    urls = {name: f"111111111111.dkr.ecr.us-west-2.amazonaws.com/example_project_{name}_dev" for name in config["images"]}
    for name in ("model_api", "streamlit"):
        entry = config["images"].get(name, {})
        c.check(f"{name} is a build with its Dockerfile named", entry.get("kind") == "build" and entry.get("dockerfile") == f"services/{name}/Dockerfile")
        c.check(f"{name} runs only behind its flag", f"{name}_enabled" in str(entry.get("runs_as", "")))
        lines = push_images.plan(config, urls, "example_profile", "us-west-2", [name]) if name in config["images"] else []
        c.check(
            f"the plan builds {name} from the repository root with its Dockerfile",
            any(line.startswith("docker build") and f"-f services/{name}/Dockerfile ." in line for line in lines),
        )
    platform = (ROOT / "infra/platform/main.tf").read_text()
    c.check(
        "the platform stack keeps the serving group of images.json only behind serving_enabled",
        'name if group == "serving"' in platform
        and "var.serving_enabled ? toset([]) : local.serving_images" in platform
        and {name for name, image in config["images"].items() if image.get("repository_group") == "serving"} == {"model_api", "streamlit"},
    )


def case_parameter_names(c: Checker) -> None:
    """The services stack injects the six parameters under the names the IAM stack lets the execution role read: one default per name on both sides."""
    iam: dict[str, str] = {}
    for name in ("variables.tf", "model_api_variables.tf"):
        iam |= secret_store.terraform_variables(ROOT / "infra/iam" / name)
    services: dict[str, str] = {}
    for name in ("model_api.tf", "streamlit.tf"):
        services |= secret_store.terraform_variables(ROOT / "infra/services" / name)
    pairs = {
        "model_api_key_hashes_name": "model_api_key_hashes_name",
        "model_api_writer_client_id_name": "aws_polaris_serving_client_id_name",
        "model_api_writer_client_credential_name": "aws_polaris_serving_client_secret_name",
        "model_api_mlflow_user_name": "model_api_mlflow_user_name",
        "model_api_mlflow_credential_name": "model_api_mlflow_credential_name",
        "streamlit_api_key_name": "streamlit_model_api_key_name",
    }
    for service_variable, iam_variable in pairs.items():
        found = services.get(service_variable)
        c.check(f"{service_variable} has the default of {iam_variable}", found is not None and found == iam.get(iam_variable))
    c.check("the six names are distinct", len({services.get(name) for name in pairs}) == len(pairs))
    pair_ids = [iam.get(f"aws_polaris_serving_client_{part}_name", "") for part in ("id", "secret")]
    c.check(
        "the serving pair names are the leaf names the catalog setup requires",
        [Path(name).name for name in pair_ids] == ["aws_polaris_serving_client_id", "aws_polaris_serving_client_secret"],
    )


def canary_deploy(w: World, percent: int) -> deploy.Outcome:
    return deploy.run(
        registry=w.registry,
        manifests=w.store,
        hai_type=HAI,
        candidate_version=CANDIDATE,
        percent=percent,
        mode="canary",
        canary_ttl_hours=TTL_HOURS,
        research_run=RESEARCH,
        now=NOW,
        deployment_id=RUN_2,
    )


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
                registry=w.registry,
                manifests=w.store,
                hai_type=HAI,
                candidate_version=version,
                percent=percent,
                mode="canary",
                canary_ttl_hours=TTL_HOURS,
                research_run=RESEARCH,
                now=NOW,
                deployment_id=run,
            )

        def rollback_run(run: str) -> deploy.Outcome:
            return deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=None, percent=0, mode="stable", deployment_id=run)

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
            deploy.run(
                registry=registry,
                manifests=store,
                hai_type=HAI,
                candidate_version=CANDIDATE,
                percent=10,
                mode="canary",
                canary_ttl_hours=TTL_HOURS,
                research_run=RESEARCH,
                now=NOW,
                deployment_id=RUN_1,
            )
        except OSError:
            stopped = True
        c.check("a deploy that stops between its writes leaves no active record", stopped and store.read_active(HAI) is None)
        finished = deploy.run(
            registry=registry,
            manifests=store,
            hai_type=HAI,
            candidate_version=CANDIDATE,
            percent=10,
            mode="canary",
            canary_ttl_hours=TTL_HOURS,
            research_run=RESEARCH,
            now=NOW,
            deployment_id=RUN_1,
        )
        active = store.read_active(HAI)
        c.check(
            "its retry finishes the same deployment",
            finished.status == "written" and active is not None and canary.Deployment.parse(active.body).deployment_id == RUN_1,
        )
        registry.aliases["production"] = "5"
        c.check(
            "a retry after the production alias moved keeps the recorded stable version",
            deploy.run(
                registry=registry,
                manifests=store,
                hai_type=HAI,
                candidate_version=CANDIDATE,
                percent=10,
                mode="canary",
                canary_ttl_hours=TTL_HOURS,
                research_run=RESEARCH,
                now=NOW,
                deployment_id=RUN_1,
            ).status
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


def case_feature_refusals_are_logged(c: Checker) -> None:
    """A 503 features_unavailable logs one fixed reason code, so the cause is visible. The log never holds a hospital, a key, a value or an error message."""
    dropped = online_cases.REFERENCES[1].replace(":", "__", 1)

    def stale(inner: Any) -> Any:
        def read(references: list[str], key: str) -> Mapping[str, Sequence[Any]]:
            answer = dict(inner(references, key))
            answer.pop(dropped, None)
            return answer

        return read

    def failing(inner: Any) -> Any:
        def read(references: list[str], key: str) -> Mapping[str, Sequence[Any]]:
            raise RuntimeError("synthetic feature store failure with secret-detail")

        return read

    def moving(inner: Any) -> Any:
        reads = []

        def read() -> Mapping[str, str]:
            reads.append(1)
            marker = dict(inner())
            return marker if len(reads) == 1 else marker | {"load_id": "another-load"}

        return read

    def unreachable(inner: Any) -> Any:
        def read() -> Mapping[str, str]:
            raise online.OnlineError("Redis failed at marker read (ConnectionError)")

        return read

    def refusal(label: str, expected: str, prepare: Callable[[World], None] | None = None, **options: Any) -> str:
        with world(**options) as w:
            if prepare is not None:
                prepare(w)
            with captured_logs() as stream:
                reply = ask(w)
            logged = stream.getvalue()
            c.check(f"{label} answers 503 features_unavailable", reply.status == 503 and reply.body.get("error") == "features_unavailable")
            c.check(f"{label} logs reason={expected} once", logged.count(f"reason={expected}") == 1)
            c.check(f"{label} logs the request id", reply.body.get("request_id", "-") in logged)
            c.check(f"{label} logs no hospital, key or value", "AAA001" not in logged and KEY_A not in logged and "secret-detail" not in logged)
            return logged

    refusal("a restarted store with no marker", "marker_absent", lambda w: w.server.restart())
    refusal("a store that is loading", "marker_loading", lambda w: w.server.write_hash(online.CONTROL_DB, online.MARKER_KEY, {"state": "loading"}))
    refusal(
        "a store with another feature schema",
        "schema_hash_mismatch",
        lambda w: w.server.write_hash(online.CONTROL_DB, online.MARKER_KEY, {"feature_schema_sha256": online_cases.OTHER_HASH}),
    )
    refusal("an answer without a requested feature", "features_missing", wrap=stale)
    refusal("an absent view row", "view_row_absent", wrap=with_view(online_cases.REFERENCES[1], None, 0))
    logged = refusal("a feature read that raises", "feature_read_failed", wrap=failing)
    c.check("a failed feature read logs the error type", "cause=RuntimeError" in logged)
    with world() as w:
        with captured_logs() as stream:
            reply = ask(w, "ZZZ999:2022")
        c.check("a hospital-window the store does not hold answers 404", reply.status == 404)
        c.check("and logs no features refusal", "reason=" not in stream.getvalue())
        with captured_logs() as stream:
            ok = ask(w)
        c.check("an answered request logs no features refusal", ok.status == 200 and "reason=" not in stream.getvalue())
    logged = refusal("an unreachable marker store", "marker_read_failed", wrap_marker=unreachable)
    c.check("a failed marker read logs the step and its error type", "ConnectionError" in logged)
    refusal("a marker that changed during the read", "marker_changed", wrap_marker=moving)


def on_set(feature_set: str) -> Callable[[Loader], None]:
    """Configure every loaded version as a model trained on the set."""

    def configure(loader: Loader) -> None:
        loader.set_models.update({PRODUCTION: feature_set, CANDIDATE: feature_set, "5": feature_set})

    return configure


def case_set_model_served(c: Checker) -> None:
    """A model trained on a model set is served from that set's marker, database and view, and a refused set names its reason in the log."""
    with world(single=False, sets=("m2",), configure=on_set("m2")) as w:
        c.check(
            "the store holds the m2 set and no single-path load", w.server.read_hash(online.CONTROL_DB, online.MARKER_KEY) == {} and w.server.marker() == {}
        )
        reply = ask(w)
        c.check("a model of the m2 set is served with 200", reply.status == 200 and reply.body["model_version"] == PRODUCTION)
        c.check("the response names the set's schema hash", reply.body["feature_schema_sha256"] == online_cases.SET_HASH)
        c.check("the model received the values under the references it lists", w.loader.calls == 1 and set(w.loader.rows[0]) == set(online_cases.SET_MODEL))
        c.check("the values are the set's own load", all(str(value).startswith("s1|") for value in w.loader.rows[0].values()))
        c.check("the row of the prediction holds the set's hash", [row["feature_schema_sha256"] for row in rows_of(w)] == [online_cases.SET_HASH])
        c.check("a hospital-window the set does not hold answers 404", ask(w, "ZZZ999:2022").status == 404)

    def refusal(label: str, expected: str, prepare: Callable[[World], None], **options: Any) -> None:
        with world(**{"single": False, "sets": ("m2",), "configure": on_set("m2"), **options}) as w:
            prepare(w)
            with captured_logs() as stream:
                reply = ask(w)
            c.check(f"{label} answers 503 features_unavailable", reply.status == 503 and reply.body.get("error") == "features_unavailable")
            c.check(f"{label} logs reason={expected} once", stream.getvalue().count(f"reason={expected}") == 1)
            c.check(f"{label} runs no prediction", w.loader.calls == 0)

    marker = online_cases.SET_MARKER
    refusal("a set that is loading", "marker_loading", lambda w: w.server.write_hash(online.CONTROL_DB, marker, {"state": "loading"}))
    refusal("a set with another state", "marker_not_ready", lambda w: w.server.write_hash(online.CONTROL_DB, marker, {"state": "failed"}))
    refusal(
        "a set with another schema",
        "schema_hash_mismatch",
        lambda w: w.server.write_hash(online.CONTROL_DB, marker, {"feature_schema_sha256": online_cases.OTHER_HASH}),
    )
    refusal("a restarted store with no set marker", "marker_absent", lambda w: w.server.restart())
    refusal("a set that was never loaded", "marker_absent", lambda w: None, sets=())
    with world(single=False, sets=("m2",), configure=lambda loader: loader.set_models.update({PRODUCTION: "m3", CANDIDATE: "m3", "5": "m3"})) as w:
        with captured_logs() as stream:
            reply = ask(w)
        c.check(
            "a model of a set with no online database answers 503 and logs set_not_online",
            reply.status == 503 and stream.getvalue().count("reason=set_not_online") == 1,
        )
    with world(configure=on_set("m2")) as w:
        with captured_logs() as stream:
            reply = ask(w)
        c.check("a set model is not served from the single path alone", reply.status == 503 and stream.getvalue().count("reason=marker_absent") == 1)
    with world(sets=("m2",)) as w:
        c.check(
            "a model with no set is still served from the single path next to a loaded set",
            ask(w).status == 200 and w.loader.rows[0].keys() == set(online_cases.REFERENCES),
        )


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
        routed = {key: canary.route(w.api.deployment(HAI), key.split(":")[0], 2022, NOW) for key in keys_}
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
        routed = {key: canary.route(w.api.deployment(HAI), key.split(":")[0], 2022, NOW) for key in keys_}
        canary_keys = [key for key, kind in routed.items() if kind == "canary"]
        replies = [ask(w, key) for key in canary_keys[:4]]
        c.check(
            "a canary that needs an absent view row falls back to the stable model",
            all(r.status == 200 and r.body["model_version"] == PRODUCTION for r in replies),
        )
        c.check("and is ejected", w.api.health().body[HAI]["candidate"] == "ejected")
    with world(count=40, percent=50) as w:
        routed = {key: canary.route(w.api.deployment(HAI), key.split(":")[0], 2022, NOW) for key in keys_}
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


SINK_CLOSE_SECONDS = 5.0


def check_sink_header(c: Checker, deadline: float, stalled: bool) -> None:
    """Append one row through the sink against the REST catalog stand-in and check the table request, with a stalled table reply when ``stalled``."""
    real = importlib.util.find_spec("pyiceberg") is not None
    client = f"PyIceberg {importlib.import_module('pyiceberg').__version__}" if real else "the PyIceberg 0.12.0 header stand-in"
    standin = SimpleNamespace(import_module=lambda _name: SimpleNamespace(load_catalog=feast_aws_cases.RestClientStandIn))
    outcome: dict[str, str] = {}
    before = set(threading.enumerate())
    with feast_aws_cases.rest_catalog_server() as server:
        endpoint = f"http://127.0.0.1:{server.server_port}/api/catalog"
        environ = {"POLARIS_URI": endpoint, "POLARIS_CLIENT_ID": "writer", "POLARIS_CLIENT_SECRET": "x"}
        sink = iceberg_sink.IcebergSink(environ)
        if stalled:
            server.table_gate.clear()

        def append() -> None:
            try:
                sink.append(sink_rows("a"), deadline)
            except Exception as error:
                outcome["error"] = f"{type(error).__name__}: {error}"

        patches: list[Any] = [mock.patch.object(iceberg_sink.reads, "polaris_token", return_value=feast_aws_cases.ISSUED)]
        if not real:
            patches.append(mock.patch.object(iceberg_sink, "importlib", standin))
        owned = set(threading.enumerate())
        with contextlib.ExitStack() as stack:
            for patch in patches:
                stack.enter_context(patch)
            worker = threading.Thread(target=append, daemon=True)
            try:
                worker.start()
                stop_worker(c, f"the sink append ({client})", worker, server.abort_connections, 20)
                if stalled:
                    c.check(
                        "the table reply was held past the caller's deadline",
                        server.table_held.is_set() and outcome.get("error", "").startswith("TimeoutError"),
                    )
                    c.check("the sink's own client was still waiting when the caller gave up", any(t.name == "iceberg_append" for t in threading.enumerate()))
            finally:
                # The caller may have ended at its deadline while the sink's own client still waits, so stop_worker never aborts it: abort here, always.
                server.abort_connections()
                c.check(f"the sink closes within {SINK_CLOSE_SECONDS:g} s ({client})", sink.close(SINK_CLOSE_SECONDS))
                server.table_gate.set()
                worker.join(SINK_CLOSE_SECONDS)
                settle_threads(c, f"the sink client ({client}) before the patches end", owned)
        paths = [request["path"] for request in server.requests]
        c.check("the client asked for the catalog config", any(path.endswith("/v1/config") for path in paths))
        c.check("the client asked for the predictions table", any(path.endswith(f"/tables/{CONTRACT['table']}") for path in paths))
        delegated = [request["path"] for request in server.requests if feast_aws_cases.DELEGATION_HEADER.lower() in request["headers"]]
        c.check(f"no request carries the {feast_aws_cases.DELEGATION_HEADER} header", delegated == [])
        refused = [request["path"] for request in server.requests if request["status"] == 400]
        c.check("no table load is refused for credential vending", refused == [] and "400" not in outcome.get("error", ""))
    settle_threads(c, f"the sink header case ({client})", before)


def case_sink_names_its_region(c: Checker) -> None:
    """The sink names the task's region to PyIceberg, so its S3 file writer never asks the bucket for its region (HeadBucket)."""
    seen: list[dict[str, Any]] = []

    def load_catalog(name: str, **properties: Any) -> object:
        seen.append({"name": name, **properties})
        return object()

    standin = SimpleNamespace(import_module=lambda _name: SimpleNamespace(load_catalog=load_catalog))
    environ = {"POLARIS_URI": "http://10.0.0.5:8181/api/catalog", "POLARIS_CLIENT_ID": "writer", "POLARIS_CLIENT_SECRET": "x", "AWS_REGION": "us-west-2"}
    sink = iceberg_sink.IcebergSink(environ)
    with mock.patch.object(iceberg_sink.reads, "polaris_token", return_value=feast_aws_cases.ISSUED), mock.patch.object(iceberg_sink, "importlib", standin):
        sink._catalog(10.0)
    c.check("the sink opens catalog hai once", [item["name"] for item in seen] == ["hai"])
    c.check("the sink names the task's region", bool(seen) and seen[0].get("s3.region") == "us-west-2")
    c.check("the sink still asks for no vended credentials", bool(seen) and iceberg_sink.reads.ACCESS_DELEGATION_PROPERTY in seen[0])
    c.check("the sink closes", sink.close(1.0))


def case_sink_requests_no_vended_credentials(c: Checker) -> None:
    """The sink opens the REST catalog without asking Polaris for vended credentials, so no table load carries the access delegation header."""
    check_sink_header(c, 10.0, stalled=False)


def case_sink_stalled_reply_leaves_no_client(c: Checker) -> None:
    """A table reply that outlasts the caller deadline ends the caller with a timeout, and the case closes the sink client and leaves no thread."""
    check_sink_header(c, 2.0, stalled=True)


def case_create_table_requests_no_vended_credentials(c: Checker) -> None:
    """The owner's table creation opens the REST catalog without asking Polaris for vended credentials, so no namespace or table creation carries the header."""
    real = importlib.util.find_spec("pyiceberg") is not None
    client = f"PyIceberg {importlib.import_module('pyiceberg').__version__}" if real else "the PyIceberg 0.12.0 header stand-in"
    types = SimpleNamespace(
        NestedField=lambda index, name, kind, required: (index, name, kind), **{kind: (lambda kind=kind: kind) for kind in iceberg_sink.TYPES.values()}
    )
    modules = {
        "pyiceberg.catalog": SimpleNamespace(load_catalog=feast_aws_cases.RestClientStandIn),
        "pyiceberg.types": types,
        "pyiceberg.schema": SimpleNamespace(Schema=lambda *fields: list(fields)),
    }
    standin = SimpleNamespace(import_module=lambda name: modules[name])
    outcome: dict[str, str] = {}
    with feast_aws_cases.rest_catalog_server() as server:
        endpoint = f"http://127.0.0.1:{server.server_port}/api/catalog"
        environ = {"POLARIS_URI": endpoint, "POLARIS_CLIENT_ID": "admin", "POLARIS_CLIENT_SECRET": "x"}

        def create() -> None:
            try:
                iceberg_sink.create_table(environ)
            except Exception as error:
                outcome["error"] = f"{type(error).__name__}: {error}"

        patches: list[Any] = [mock.patch.object(iceberg_sink.reads, "polaris_token", return_value=feast_aws_cases.ISSUED)]
        if not real:
            patches.append(mock.patch.object(iceberg_sink, "importlib", standin))
        with contextlib.ExitStack() as stack:
            for patch in patches:
                stack.enter_context(patch)
            worker = threading.Thread(target=create, daemon=True)
            worker.start()
            stop_worker(c, f"the table creation ({client})", worker, server.abort_connections, 20)
        posts = [request["path"] for request in server.requests if request["method"] == "POST"]
        c.check("the client created the serving namespace", any(path.endswith("/v1/namespaces") for path in posts))
        c.check(f"the client created the {CONTRACT['table']} table", any(path.endswith(f"/namespaces/{CONTRACT['namespace']}/tables") for path in posts))
        delegated = [request["path"] for request in server.requests if feast_aws_cases.DELEGATION_HEADER.lower() in request["headers"]]
        c.check(f"no request carries the {feast_aws_cases.DELEGATION_HEADER} header", delegated == [])
        refused = [request["path"] for request in server.requests if request["status"] == 400]
        c.check("no creation is refused for credential vending", refused == [] and "400" not in outcome.get("error", ""))


def case_nonfinite(c: Checker) -> None:
    """A prediction that is not a finite number is a model failure: it triggers fallback and ejection, and it is never returned or recorded."""
    keys_ = hospital_keys(40)
    with world(count=40, percent=50) as w:
        routed = {key: canary.route(w.api.deployment(HAI), key.split(":")[0], 2022, NOW) for key in keys_}
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
        routed = {key: canary.route(w.api.deployment(HAI), key.split(":")[0], 2022, NOW) for key in keys_}
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
        later = deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=None, percent=0, mode="stable", deployment_id=RUN_2)
        c.check("a deploy over it is numbered 2 and writes the current format", later.status == "written" and later.deployment.sequence == 2)
        c.check(
            "a retry of the previous-format deployment is refused as replaced",
            refused(
                lambda: deploy.run(
                    registry=w.registry,
                    manifests=w.store,
                    hai_type=HAI,
                    candidate_version=CANDIDATE,
                    percent=20,
                    mode="canary",
                    canary_ttl_hours=TTL_HOURS,
                    research_run=RESEARCH,
                    now=NOW,
                    deployment_id=RUN_1,
                )
            ),
        )
        again = deploy.run(registry=w.registry, manifests=w.store, hai_type=HAI, candidate_version=None, percent=0, mode="stable", deployment_id=RUN_2)
        c.check("a retry of the new deployment changes nothing", again.status == "unchanged")
    with world(deployment=False) as w:
        w.store.put_history(HAI, RUN_1, old)
        finished = deploy.run(
            registry=w.registry,
            manifests=w.store,
            hai_type=HAI,
            candidate_version=CANDIDATE,
            percent=20,
            mode="canary",
            canary_ttl_hours=TTL_HOURS,
            research_run=RESEARCH,
            now=NOW,
            deployment_id=RUN_1,
        )
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
    """serving_deploy_dag: a closed request of the HAI type and the mode alone, one job chain and no way to name a version, a share or the AWS route."""
    from orchestration import graph, training_contract

    ok = {"hai_type": HAI, "deploy_mode": "canary"}
    c.check("a closed request is accepted", training_contract.serving_parameters_for(ok) == ok)
    c.check("a stable request is accepted", training_contract.serving_parameters_for(ok | {"deploy_mode": "stable"})["deploy_mode"] == "stable")
    c.check("the closed request and the manual form offer canary and stable only", training_contract.SERVE_MODES == ("canary", "stable"))
    for name, bad in {
        "a candidate version": ok | {"candidate_version": CANDIDATE},
        "a percent": ok | {"percent": 10},
        "a version and a percent": ok | {"candidate_version": CANDIDATE, "percent": 10},
        "an extra parameter": ok | {"alias": "production"},
        "a missing mode": {"hai_type": HAI},
        "a missing type": {"deploy_mode": "canary"},
        "an unknown HAI type": ok | {"hai_type": "HAI_9"},
        "the retired rollback mode": ok | {"deploy_mode": "rollback"},
        "an unknown mode": ok | {"deploy_mode": "promote"},
        "a list": [ok],
    }.items():
        c.check(f"{name} is refused", rejected(training_contract.serving_parameters_for, bad))
    c.check(
        "the graph rules know the DAG as one job chain",
        "serving_deploy_dag" in graph.COLLAPSED_DAGS and "serving_deploy_dag" not in graph.PIPELINE_CHAINS,
    )
    source = ast.parse((ROOT / "dags/serving_deploy_dag.py").read_text())
    calls = [node for node in ast.walk(source) if isinstance(node, ast.Call)]
    c.check(
        "the DAG file builds one job chain of the serve job through the shared runtime",
        [ast.unparse(call) for call in calls if getattr(call.func, "id", "") in ("build_tasks", "build_job_chain")]
        == ["build_job_chain('serve_deploy', family='serve')"],
    )
    form = (ROOT / "dags/serving_deploy_dag.py").read_text()
    c.check(
        "the form asks for the type, the mode and the research run to deploy from, and names no version or share",
        all(f'"{name}": Param' in form for name in ("hai_type", "deploy_mode", "research_run"))
        and "SERVE_MODES" in form
        and not any(word in form for word in ("candidate_version", "percent")),
    )
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


def case_liveness_without_a_deployment(c: Checker) -> None:
    """A service with no deployment is alive and not ready: /v1/live answers 200 for the container health check, /v1/health stays 503 and both are counted."""
    with world(deployment=False) as w:
        w.api.refresh()
        live = w.api.live()
        c.check("a service with no deployment answers live with 200", live.status == 200 and live.body == {"status": "alive"})
        ready = w.api.health()
        c.check("the same service answers health with 503 and no deployment", ready.status == 503 and ready.body[HAI]["deployment_id"] is None)
        c.check("a prediction is refused until a deployment exists", ask(w).status == 503)
        c.check("live stays 200 after the refused prediction", w.api.live().status == 200)
        _, samples = scrape(w.api.metrics.render())
        c.check(
            "the live answers are counted under their own route", series(samples, "hai_api_requests_total", route="/v1/live", method="GET", status="200") == [2]
        )
        c.check(
            "the health answer keeps its own route and status", series(samples, "hai_api_requests_total", route="/v1/health", method="GET", status="503") == [1]
        )
    with world() as w:
        w.api.refresh()
        c.check("a service with a stable model answers both 200", w.api.live().status == 200 and w.api.health().status == 200)


SIX_TYPES = ("HAI_1", "HAI_2", "HAI_3", "HAI_4", "HAI_5", "HAI_6")
GATE_SECONDS = 20.0
START_DEADLINE = 30.0


class BlockingLoader(Loader):
    """A stand-in loader whose loads wait for a gate, so a case can hold the initial load open. A gate that stays shut fails the load after a deadline."""

    def __init__(self) -> None:
        super().__init__()
        self.gate = threading.Event()
        self.started = threading.Event()
        self.expired = False
        self.hold: set[str] | None = None  # the HAI types whose loads wait for the gate; None holds every type

    def load(self, hai_type: str, version: str) -> service.LoadedModel:
        if self.hold is not None and hai_type not in self.hold:
            return super().load(hai_type, version)
        self.started.set()
        if self.expired or not self.gate.wait(GATE_SECONDS):
            self.expired = True
            raise TimeoutError("synthetic load gate stayed shut")
        return super().load(hai_type, version)


@contextlib.contextmanager
def startup_world(
    types: Sequence[str],
    loader: Loader,
    *,
    canary: bool = True,
    deployed: bool = True,
    read_marker: Callable[[], Mapping[str, str]] | None = None,
    read_features: Callable[[list[str], str], Mapping[str, Sequence[Any]]] | None = None,
    **options: Any,
) -> Iterator[SimpleNamespace]:
    """A service built over deployments of every type in ``types`` (with a canary each when asked) and a loader of the case. It reads no feature unless the
    case gives the readers of a feature store."""

    def no_marker() -> Mapping[str, str]:
        raise AssertionError("a startup case reads no marker")

    def no_features(references: list[str], key: str) -> Mapping[str, Sequence[Any]]:
        raise AssertionError("a startup case reads no feature")

    with tempfile.TemporaryDirectory(prefix="serving_start_") as folder:
        store = manifests.FileManifestStore(Path(folder))
        clock, sink = Clock(), Sink()
        for hai_type in types if deployed else ():
            deploy.run(
                registry=Registry(hai_type=hai_type),
                manifests=store,
                hai_type=hai_type,
                candidate_version=CANDIDATE if canary else None,
                percent=20 if canary else 0,
                mode="canary" if canary else "stable",
                canary_ttl_hours=TTL_HOURS if canary else None,
                research_run=RESEARCH if canary else None,
                now=NOW if canary else None,
                deployment_id=RUN_1,
            )
        wall = Clock()
        wall.now = float(NOW)
        api = service.ModelService(
            keys=keys.KeyRing.from_hashes({"ops": keys.hash_key(KEY_A)}),
            limiter=limiter.RateLimiter(rate_per_minute=600, burst=1000, clock=clock),
            read_marker=read_marker or no_marker,
            read_features=read_features or no_features,
            manifests=store,
            loader=loader,
            buffer=predictions.PredictionBuffer(sink, columns=CONTRACT["columns"], capacity=1000, batch=50, deadline_seconds=5.0, clock=clock),
            clock=clock,
            wall=wall,
            new_id=iter(f"req{number:04d}" for number in range(10_000)).__next__,
            hai_types=tuple(types),
            refresh_seconds=30.0,
            failure_limit=3,
            **options,
        )
        yield SimpleNamespace(api=api, loader=loader, clock=clock, store=store, sink=sink)


def predict_to(api: service.ModelService, hai_type: str = HAI) -> service.Reply:
    return api.predict(KEY_A, None, {"hai_type": hai_type, "ccn": "AAA001", "window_year": 2022})


def publish(s: SimpleNamespace, hai_type: str, *, canary: bool = False, stable: str = PRODUCTION, deployment_id: str = RUN_1) -> None:
    """Write the active deployment of one HAI type into the manifests of the startup world."""
    deploy.run(
        registry=Registry(production=stable, hai_type=hai_type),
        manifests=s.store,
        hai_type=hai_type,
        candidate_version=CANDIDATE if canary else None,
        percent=20 if canary else 0,
        mode="canary" if canary else "stable",
        canary_ttl_hours=TTL_HOURS if canary else None,
        research_run=RESEARCH if canary else None,
        now=NOW if canary else None,
        deployment_id=deployment_id,
    )


def in_thread(call: Callable[[], object], name: str) -> threading.Thread:
    worker = threading.Thread(target=call, name=name, daemon=True)
    worker.start()
    return worker


def wait_until(check: Callable[[], bool], seconds: float = START_DEADLINE) -> bool:
    """Poll ``check`` until it holds or the deadline passes."""
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if check():
            return True
        time.sleep(0.02)
    return check()


def case_startup_binds_before_the_load(c: Checker) -> None:
    """The service starts with no model loaded: /v1/live answers 200 at once, /v1/health answers 503 until the first full load ends, and a prediction is
    refused without waiting for the load. The first load covers the stable and the canary model of every served type (12 loads for six types)."""
    loader = BlockingLoader()
    with startup_world(SIX_TYPES, loader, defer_first_load=True) as s:
        c.check("the constructor loads no model", loader.attempts == [])
        c.check("the service is alive before any load", s.api.live().status == 200 and s.api.live().body == {"status": "alive"})
        c.check("the service is not ready before any load", s.api.health().status == 503 and s.api.health().body["HAI_1"]["stable"] == "loading")
        worker = threading.Thread(target=s.api.refresh, name="startup_case_load", daemon=True)
        worker.start()
        c.check("the first load starts", loader.started.wait(START_DEADLINE))
        c.check("live answers 200 while the load runs", s.api.live().status == 200)
        during = s.api.health()
        c.check(
            "health answers 503 while the load runs and names every type as loading",
            during.status == 503 and all(during.body[t]["stable"] == "loading" for t in SIX_TYPES),
        )
        began = time.monotonic()
        refused = predict_to(s.api)
        c.check(
            "a prediction during the load is refused at once with 503 starting",
            refused.status == 503 and refused.body.get("error") == "starting" and time.monotonic() - began < 5,
        )
        loader.gate.set()
        worker.join(START_DEADLINE)
        c.check("the first load ends", not worker.is_alive())
        c.check(
            "the first load held the stable and the canary model of six types",
            len(loader.loaded) == 12 and sorted(loader.loaded) == sorted([PRODUCTION, CANDIDATE] * 6),
        )
        after = s.api.health()
        c.check(
            "health answers 200 after the load, with both models of each type",
            after.status == 200 and all(after.body[t]["candidate"] == CANDIDATE for t in SIX_TYPES),
        )
        c.check("live still answers 200", s.api.live().status == 200)
    with startup_world((HAI,), BlockingLoader(), deployed=False, defer_first_load=True) as s:
        s.api.refresh()
        c.check("with no deployment the service stays alive and not ready, as before", s.api.live().status == 200 and s.api.health().status == 503)


def case_startup_failed_load(c: Checker) -> None:
    """A first load that fails leaves the service unready with the reason, and /v1/live fails so ECS replaces the task. A retry that succeeds makes it live and
    ready again. A load that outlives its deadline fails /v1/live too. A service that never loaded nothing because no deployment exists stays live."""
    loader = Loader()
    loader.unloadable.add(PRODUCTION)
    with startup_world((HAI,), loader, canary=False, defer_first_load=True) as s:
        s.api.refresh()
        health = s.api.health()
        c.check("a failed first load is unready", health.status == 503)
        c.check("the failed stable model is reported with its reason", health.body[HAI]["stable"] == "failed" and health.body[HAI]["reason"] == "RuntimeError")
        live = s.api.live()
        c.check("a failed first load fails the liveness route", live.status == 503 and live.body == {"status": "load_failed"})
        c.check("a prediction after the failed load answers 503", predict_to(s.api).status == 503)
        loader.unloadable.clear()
        s.api.refresh()
        c.check("the retry loads the model and the service is ready", s.api.health().status == 200 and s.api.health().body[HAI]["stable"] == PRODUCTION)
        c.check("the service is live again", s.api.live().status == 200)
        loader.unloadable.add(PRODUCTION)
        s.api.refresh()
        c.check("a later failure never fails the liveness route of a service that was ready", s.api.live().status == 200)
    loader = BlockingLoader()
    with startup_world((HAI,), loader, canary=False, defer_first_load=True, first_load_seconds=600.0) as s:
        worker = threading.Thread(target=s.api.refresh, name="startup_case_deadline", daemon=True)
        worker.start()
        c.check("the load starts", loader.started.wait(START_DEADLINE))
        s.clock.now += 599.0
        c.check("a load inside its deadline is live", s.api.live().status == 200)
        s.clock.now += 2.0
        c.check("a load past its deadline fails the liveness route", s.api.live().status == 503 and s.api.live().body == {"status": "load_timeout"})
        loader.gate.set()
        worker.join(START_DEADLINE)
        c.check("the late load makes the service live and ready", not worker.is_alive() and s.api.live().status == 200 and s.api.health().status == 200)


def case_startup_lifecycle(c: Checker) -> None:
    """The starting state lasts until the first full load succeeded, not until the first manifest pass ended. A deployment that appears after an empty pass is
    loaded under the same deadline, and a prediction during any load before the first readiness is refused at once, for the type that is loaded as well."""
    loader = BlockingLoader()
    with startup_world((HAI,), loader, deployed=False, defer_first_load=True, first_load_seconds=600.0) as s:
        s.api.refresh()
        c.check("an empty first pass leaves the service live and not ready", s.api.live().status == 200 and s.api.health().status == 503)
        publish(s, HAI)
        worker = in_thread(s.api.refresh, "lifecycle_late_deployment")
        c.check("the load of the deployment that came after the empty pass starts", loader.started.wait(START_DEADLINE))
        s.clock.now += 599.0
        c.check("a load inside its deadline is live", s.api.live().status == 200)
        s.clock.now += 2.0
        c.check("the load of that deployment is bound by the deadline too", s.api.live().status == 503 and s.api.live().body == {"status": "load_timeout"})
        began = time.monotonic()
        refused = predict_to(s.api)
        c.check(
            "a prediction past the refresh interval is refused with starting at once, with no refresh of its own",
            refused.status == 503 and refused.body.get("error") == "starting" and time.monotonic() - began < 5,
        )
        loader.gate.set()
        worker.join(START_DEADLINE)
        c.check("the late load ends and the service is live and ready", not worker.is_alive() and s.api.live().status == 200 and s.api.health().status == 200)
    loader = BlockingLoader()
    with startup_world(("HAI_1", "HAI_2"), loader, deployed=False, defer_first_load=True) as s:
        publish(s, "HAI_1")
        loader.hold = {"HAI_2"}
        s.api.refresh()
        c.check(
            "one type loaded and the other without a deployment is not ready",
            s.api.health().status == 503 and s.api.health().body["HAI_1"]["stable"] == PRODUCTION,
        )
        publish(s, "HAI_2")
        worker = in_thread(s.api.refresh, "lifecycle_partial")
        c.check("the retry of the second type starts", loader.started.wait(START_DEADLINE))
        s.clock.now += 31.0
        began = time.monotonic()
        refused = predict_to(s.api, "HAI_1")
        c.check(
            "a prediction for the loaded type waits for no lock during the retry",
            refused.status == 503 and refused.body.get("error") == "starting" and time.monotonic() - began < 5,
        )
        loader.gate.set()
        worker.join(START_DEADLINE)
        c.check("the service is ready when the second type is loaded", s.api.health().status == 200)


def case_startup_manifest_errors(c: Checker) -> None:
    """A manifest that cannot be read is not a model that failed to load. The service stays live and not ready, names the error type and retries, and fails
    liveness only when the error lasts past its bound (a missing credential never recovers). A read that recovers makes the service ready."""
    with startup_world(SIX_TYPES, Loader(), canary=False, defer_first_load=True) as s:
        reads = {"failures": 1}
        real = s.store.read_active

        def flaky(hai_type: str) -> Any:
            if reads["failures"] > 0:
                raise TimeoutError("synthetic read timeout")
            return real(hai_type)

        with mock.patch.object(s.store, "read_active", flaky):
            s.api.refresh()
            health = s.api.health()
            c.check("a manifest read that failed once leaves the service live", s.api.live().status == 200)
            c.check(
                "it is not ready and names the type of the error for every type",
                health.status == 503 and all(health.body[t]["stable"] == "retrying" and health.body[t]["reason"] == "TimeoutError" for t in SIX_TYPES),
            )
            reads["failures"] = 0
            s.clock.now += 30.0
            s.api.refresh()
            c.check(
                "the next read recovers and the service is ready and live",
                s.api.health().status == 200 and s.api.live().status == 200 and s.loader.attempts != [],
            )
    with startup_world(SIX_TYPES, Loader(), canary=False, defer_first_load=True) as s:
        reads = {"failing": True}
        real = s.store.read_active

        def broken(hai_type: str) -> Any:
            if reads["failing"]:
                raise PermissionError("synthetic missing credential")
            return real(hai_type)

        with mock.patch.object(s.store, "read_active", broken):
            s.api.refresh()
            s.clock.now += 299.0
            s.api.refresh()
            c.check("an error inside its bound keeps the service live", s.api.live().status == 200 and s.api.health().status == 503)
            s.clock.now += 2.0
            s.api.refresh()
            c.check("an error past its bound fails the liveness route", s.api.live().status == 503 and s.api.live().body == {"status": "dependency_failed"})
            reads["failing"] = False
            s.clock.now += 30.0
            s.api.refresh()
            c.check("a read that recovers late still makes the service live and ready", s.api.live().status == 200 and s.api.health().status == 200)


def case_startup_admitted_prediction_never_waits(c: Checker) -> None:
    """Before the first readiness a prediction is refused with starting at once, also between two passes of the background load. So a request that came while
    no refresh ran never holds a feature read that then waits for the refresh lock, and a refresh that starts while a request is under way blocks no request."""
    loader = BlockingLoader()
    loader.hold = {"HAI_2"}
    entered, resume, replies = threading.Event(), threading.Event(), []
    with world() as w, startup_world(("HAI_1", "HAI_2"), loader, canary=False, deployed=False, defer_first_load=True) as s:
        feature_marker = online_cases.markers(w.server)

        def read_marker() -> Mapping[str, str]:
            entered.set()
            if not resume.wait(START_DEADLINE):
                raise TimeoutError("synthetic feature read stayed shut")
            return feature_marker()

        s.api._read_marker, s.api._read_features = read_marker, online_cases.reader(w.server)
        publish(s, "HAI_1")
        s.api.refresh()
        c.check("a pass between two passes leaves one type loaded and the service not ready", s.api.health().status == 503 and not s.api.ready())
        request = in_thread(lambda: replies.append(predict_to(s.api, "HAI_1")), "admitted_prediction")
        wait_until(lambda: entered.is_set() or bool(replies))
        publish(s, "HAI_2")
        refresh = in_thread(s.api.refresh, "admitted_partial_refresh")
        c.check("the refresh of the second type starts its slow load", loader.started.wait(START_DEADLINE))
        resume.set()
        request.join(5)
        c.check("the request that came between the passes ends while the refresh still runs", not request.is_alive() and refresh.is_alive())
        c.check("it is refused with starting, with no feature read and no wait for the refresh", len(replies) == 1 and replies[0].status == 503)
        c.check("the refusal names the starting state", bool(replies) and replies[0].body.get("error") == "starting")
        c.check("the service was never ready during the wait", not s.api._ever_ready and s.api.live().status == 200)
        loader.gate.set()
        resume.set()
        refresh.join(START_DEADLINE)
        request.join(START_DEADLINE)
        c.check("the refresh ends and the service is ready", not refresh.is_alive() and s.api.health().status == 200)
        c.check("a prediction is answered once the service was ready", predict_to(s.api, "HAI_1").status == 200)


def case_startup_record_deadline_during_load(c: Checker) -> None:
    """A deployment record that cannot be read fails liveness after its bound while another type's first model load is still running, as it does between
    passes. A service that was ready once stays live in the same state."""
    loader = BlockingLoader()
    loader.hold = {"HAI_2"}
    with startup_world(("HAI_1", "HAI_2"), loader, canary=False, defer_first_load=True) as s:
        real = s.store.read_active
        failing = {"HAI_1": True}

        def read(hai_type: str) -> Any:
            if failing.get(hai_type):
                raise TimeoutError("synthetic record read timeout")
            return real(hai_type)

        with mock.patch.object(s.store, "read_active", read):
            worker = in_thread(s.api.refresh, "record_deadline_pass")
            c.check("the load of the second type starts after the first record failed", loader.started.wait(START_DEADLINE))
            s.clock.now += 299.0
            c.check("299 seconds into the error the service is live", s.api.live().status == 200)
            health = s.api.health().body["HAI_1"]
            c.check("the failed record is reported as retrying with its error type", health["stable"] == "retrying" and health["reason"] == "TimeoutError")
            s.clock.now += 2.0
            expired = s.api.live()
            c.check(
                "301 seconds into the error, while the other load runs, liveness fails",
                expired.status == 503 and expired.body == {"status": "dependency_failed"},
            )
            c.check("the other type is still loading", worker.is_alive() and s.api.health().body["HAI_2"]["stable"] == "loading")
            loader.gate.set()
            worker.join(START_DEADLINE)
            c.check("the pass ends and liveness still fails", not worker.is_alive() and s.api.live().body == {"status": "dependency_failed"})
            failing["HAI_1"] = False
            s.clock.now += 30.0
            s.api.refresh()
            c.check("a record that is readable again makes the service live and ready", s.api.live().status == 200 and s.api.health().status == 200)
    loader = BlockingLoader()
    loader.hold = {"HAI_2"}
    with startup_world(("HAI_1", "HAI_2"), loader, canary=False, deployed=False, defer_first_load=True) as s:
        publish(s, "HAI_1")
        publish(s, "HAI_2")
        loader.gate.set()
        s.api.refresh()
        c.check("a service whose stable models loaded is ready", s.api.health().status == 200)
        loader.gate.clear()
        loader.expired = False
        publish(s, "HAI_2", stable="5", deployment_id=RUN_2)
        real = s.store.read_active

        def read_again(hai_type: str) -> Any:
            if hai_type == "HAI_1":
                raise TimeoutError("synthetic record read timeout")
            return real(hai_type)

        with mock.patch.object(s.store, "read_active", read_again):
            s.clock.now += 31.0
            worker = in_thread(s.api.refresh, "record_deadline_ready_pass")
            c.check("a later load of a new version starts", loader.started.wait(START_DEADLINE))
            s.clock.now += 301.0
            c.check("a service that was ready stays live past the bound while it loads", s.api.live().status == 200)
            loader.gate.set()
            worker.join(START_DEADLINE)
            c.check("and after the pass", not worker.is_alive() and s.api.live().status == 200)


def case_startup_build_and_readiness(c: Checker) -> None:
    """The real build() of main gives a service that loads nothing in its constructor, and a service that was ready stays live when a later version fails."""
    from scripts.serving import main as model_main

    loader = Loader()
    environment = {
        "MODEL_API_KEY_HASHES": json.dumps({"ops": keys.hash_key(KEY_A)}),
        "MODEL_API_HAI_TYPES": HAI,
        "MODEL_API_MANIFEST_URI": "file:///unused",
        "MODEL_API_BIND": "127.0.0.1",
    }
    with tempfile.TemporaryDirectory(prefix="serving_build_") as folder:
        store = manifests.FileManifestStore(Path(folder))
        publish(SimpleNamespace(store=store), HAI)
        sink = SimpleNamespace(append=lambda rows, timeout: None, close=lambda seconds: True)
        with (
            mock.patch.object(model_main, "feature_store", lambda: object()),
            mock.patch.object(model_main, "IcebergSink", lambda environ: sink),
            mock.patch.object(model_main, "manifest_store", lambda uri: store),
            mock.patch.object(model_main, "MlflowLoader", lambda: loader),
            mock.patch.object(model_main.contract, "marker_reader", lambda environ: lambda: {}),
            mock.patch.object(model_main.contract, "feast_reader", lambda feature_store, environ: lambda references, key: {}),
            mock.patch.object(model_main.contract, "store_readers", lambda factory, environ: lambda feature_set: ((lambda: {}), (lambda references, key: {}))),
        ):
            api, _ = model_main.build(environment, model_main.settings_from(environment))
        c.check("the service of build() loads no model in its constructor", loader.attempts == [] and api.live().status == 200 and api.health().status == 503)
    with startup_world((HAI,), Loader(), canary=False, defer_first_load=True) as s:
        publish(s, HAI)
        s.api.refresh()
        c.check("the service is ready after its first load", s.api.health().status == 200)
        s.loader.unloadable.add("5")
        publish(s, HAI, stable="5", deployment_id=RUN_2)
        s.clock.now += 31.0
        s.api.refresh()
        c.check("a later stable version that fails to load makes it not ready", s.api.health().status == 503 and s.api.health().body[HAI]["stable"] == "failed")
        c.check("and never fails the liveness route of a service that was ready", s.api.live().status == 200)


class TypeLoader(BlockingLoader):
    """A stand-in loader that fails every load of the types in ``failing`` and holds the load of ``held`` until the gate opens."""

    def __init__(self, failing: Sequence[str] = (), held: str | None = None) -> None:
        super().__init__()
        self.failing, self.held = set(failing), held
        self.held_at = threading.Event()

    def load(self, hai_type: str, version: str) -> service.LoadedModel:
        if hai_type in self.failing:
            self.attempts.append(version)
            raise RuntimeError(f"synthetic load failure for {hai_type}")
        if hai_type != self.held:
            return Loader.load(self, hai_type, version)
        self.held_at.set()
        return super().load(hai_type, version)


def case_startup_readiness_by_type(c: Checker) -> None:
    """Six served types: the service is not ready until the load of every type has ended, one type that fails is named in /v1/health and leaves the others
    loaded, and a failed load of one type after the first readiness never takes /v1/live down."""
    loader = TypeLoader(held="HAI_6")
    with startup_world(SIX_TYPES, loader, canary=False, defer_first_load=True) as s:
        worker = in_thread(s.api.refresh, "startup_case_type_load")
        c.check("the load of the sixth type starts", loader.held_at.wait(START_DEADLINE))
        c.check("five types are loaded while the sixth waits", wait_until(lambda: len(loader.loaded) == 5))
        during = s.api.health()
        c.check("the service is not ready until every type is loaded", not s.api.ready() and during.status == 503)
        c.check("health lists the five loaded types by version", all(during.body[t]["stable"] == PRODUCTION for t in SIX_TYPES[:5]))
        c.check("health lists the sixth type as loading", during.body["HAI_6"]["stable"] == "loading")
        c.check("live answers 200 while the sixth type loads", s.api.live().status == 200)
        loader.gate.set()
        worker.join(START_DEADLINE)
        c.check("the service is ready when the sixth type is loaded", not worker.is_alive() and s.api.ready() and s.api.health().status == 200)
        loader.failing.add("HAI_4")
        publish(s, "HAI_4", stable="5", deployment_id=RUN_2)
        s.clock.now += 31.0
        s.api.refresh()
        after = s.api.health()
        c.check("a type that fails after the first readiness makes the service not ready", after.status == 503 and not s.api.ready())
        c.check(
            "health names the failed type and its error type, and the five others stay loaded",
            after.body["HAI_4"]["stable"] == "failed"
            and after.body["HAI_4"]["reason"] == "RuntimeError"
            and all(after.body[t]["stable"] == PRODUCTION for t in SIX_TYPES if t != "HAI_4"),
        )
        c.check("a failed load of one type never takes the liveness route down", s.api.live().status == 200)
    failing = TypeLoader(failing=("HAI_4",))
    with startup_world(SIX_TYPES, failing, canary=False, defer_first_load=True) as s:
        s.api.refresh()
        first = s.api.health()
        c.check("one type of six that fails its first load leaves the service not ready", first.status == 503 and not s.api.ready())
        c.check(
            "health names that type and no other",
            first.body["HAI_4"]["stable"] == "failed" and [t for t in SIX_TYPES if first.body[t]["stable"] == "failed"] == ["HAI_4"],
        )
        c.check("the first pass that ended with that failure fails the liveness route, so the container is replaced", s.api.live().status == 503)
        failing.failing.clear()
        s.clock.now += 31.0
        s.api.refresh()
        c.check("the retry loads the type and the service is ready and live", s.api.ready() and s.api.health().status == 200 and s.api.live().status == 200)


def case_startup_ready_log_line(c: Checker) -> None:
    """One log line says that every served type is loaded, because the container health check sees only /v1/live. It comes once, when the service is first
    ready, and not while one type is still failing, and not again after a later refresh or a later failure."""

    def ready_lines(stream: io.StringIO) -> list[str]:
        return [line for line in stream.getvalue().splitlines() if "model API ready" in line]

    failing = TypeLoader(failing=("HAI_4",))
    with startup_world(SIX_TYPES, failing, canary=False, defer_first_load=True) as s, captured_logs() as stream:
        s.api.refresh()
        c.check("no ready line while a type failed to load", ready_lines(stream) == [])
        failing.failing.clear()
        s.clock.now += 31.0
        s.api.refresh()
        lines = ready_lines(stream)
        c.check("one ready line when the last type is loaded", len(lines) == 1)
        c.check("the line names every served type and its stable version", all(f"{t}={PRODUCTION}" in lines[0] for t in SIX_TYPES))
        s.clock.now += 31.0
        s.api.refresh()
        failing.failing.add("HAI_2")
        publish(s, "HAI_2", stable="5", deployment_id=RUN_2)
        s.clock.now += 31.0
        s.api.refresh()
        failing.failing.clear()
        s.clock.now += 31.0
        s.api.refresh()
        c.check("no second ready line after later refreshes, a later failure and its recovery", len(ready_lines(stream)) == 1)
    with startup_world((HAI,), Loader(), canary=False, deployed=False, defer_first_load=True) as s, captured_logs() as stream:
        s.api.refresh()
        c.check("no ready line for a service that waits for its first deployment", ready_lines(stream) == [])


class FakeUvicorn:
    """A stand-in for the uvicorn module whose Server binds a real socket and answers /v1/live from the service, like the real server after its start.

    ``Server.run`` binds, then waits 0.3 s, asks /v1/live over HTTP and notes whether a model load had entered, sets ``started`` as uvicorn does when it is
    listening, and calls the hook of the case.
    """

    def __init__(self, hook: Callable[[SimpleNamespace], None], entered: Callable[[], bool]) -> None:
        self.hook, self.entered, self.seen = hook, entered, {}
        outer = self

        class Config:
            def __init__(self, app: Any, **settings: Any) -> None:
                self.app, self.settings = app, settings

        class Server:
            def __init__(self, config: Any) -> None:
                self.config, self.started = config, False

            def run(self) -> None:
                api = self.config.app

                class Handler(BaseHTTPRequestHandler):
                    def do_GET(self) -> None:
                        reply = api.live()
                        data = json.dumps(reply.body).encode()
                        self.send_response(reply.status)
                        self.send_header("Content-Length", str(len(data)))
                        self.end_headers()
                        self.wfile.write(data)

                    def log_message(self, format: str, *args: Any) -> None:
                        return

                httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
                worker = in_thread(httpd.serve_forever, "fake_uvicorn_http")
                try:
                    time.sleep(0.3)
                    with importlib.import_module("urllib.request").urlopen(f"http://127.0.0.1:{httpd.server_address[1]}/v1/live", timeout=5) as answer:
                        outer.seen["live_status"] = answer.status
                    outer.seen["entered_before_started"] = outer.entered()
                    outer.seen["port"] = self.config.settings.get("port")
                    self.started = True
                    outer.hook(SimpleNamespace(server=self))
                finally:
                    httpd.shutdown()
                    worker.join(5)
                    httpd.server_close()

        self.Config, self.Server = Config, Server


def case_main_binds_before_the_load(c: Checker) -> None:
    """main() starts no model load until the web server listens (an HTTP answer of /v1/live comes first), and the load then runs in the background. A failed
    first load is retried in the background until it succeeds."""
    from scripts.serving import main as model_main

    environment = {
        "MODEL_API_KEY_HASHES": json.dumps({"ops": keys.hash_key(KEY_A)}),
        "MODEL_API_HAI_TYPES": ",".join(SIX_TYPES),
        "MODEL_API_MANIFEST_URI": "file:///unused",
        "MODEL_API_BIND": "127.0.0.1",
        "MODEL_API_REFRESH_SECONDS": "5",
    }
    seen: dict[str, Any] = {}

    def run_main(s: SimpleNamespace, fake: FakeUvicorn) -> None:
        sink = SimpleNamespace(close=lambda seconds: True)
        fake_api = SimpleNamespace(create_app=lambda api: api)
        with (
            mock.patch.dict(os.environ, environment),
            mock.patch.object(model_main, "build", return_value=(s.api, sink)),
            mock.patch.dict(sys.modules, {"scripts.serving.api": fake_api, "uvicorn": SimpleNamespace(Config=fake.Config, Server=fake.Server)}),
        ):
            seen["exit"] = model_main.main()

    loader = BlockingLoader()
    with startup_world(SIX_TYPES, loader, defer_first_load=True) as s:

        def serve_while_loading(hook: SimpleNamespace) -> None:
            seen["live_at_start"] = s.api.live().status
            seen["health_at_start"] = s.api.health().status
            seen["entered_at_start"] = loader.started.wait(START_DEADLINE)
            loader.gate.set()
            seen["ready_later"] = wait_until(lambda: s.api.health().status == 200)

        fake = FakeUvicorn(serve_while_loading, loader.started.is_set)
        run_main(s, fake)
        c.check(
            "main answered /v1/live over HTTP before any model load entered",
            fake.seen.get("live_status") == 200 and fake.seen.get("entered_before_started") is False,
        )
        c.check("the server got the configured port", fake.seen.get("port") == 8000)
        c.check(
            "the load starts once the server listens, with the service live and not ready",
            seen.get("entered_at_start") is True and seen.get("live_at_start") == 200 and seen.get("health_at_start") == 503,
        )
        c.check("the background load makes it ready, and main returns", seen.get("ready_later") is True and seen.get("exit") == 0)
        c.check("no load thread outlives main", not any(thread.name == "model_load" for thread in threading.enumerate()))
    failing = Loader()
    failing.unloadable.add(PRODUCTION)
    seen.clear()
    with startup_world((HAI,), failing, canary=False, defer_first_load=True) as s:

        def serve_with_a_failed_load(hook: SimpleNamespace) -> None:
            seen["failed"] = wait_until(lambda: s.api.live().status == 503)
            failing.unloadable.clear()
            seen["recovered"] = wait_until(lambda: s.api.live().status == 200 and s.api.health().status == 200)

        environment["MODEL_API_HAI_TYPES"] = HAI
        run_main(s, FakeUvicorn(serve_with_a_failed_load, lambda: bool(failing.attempts)))
        c.check("a failed first load fails the liveness route", seen.get("failed") is True)
        c.check("the background retry loads the model once it can and the service recovers", seen.get("recovered") is True)
    with startup_world((HAI,), BlockingLoader(), defer_first_load=True) as s:
        sink = SimpleNamespace(close=lambda seconds: True)
        never = SimpleNamespace(Config=lambda app, **settings: SimpleNamespace(app=app), Server=lambda config: SimpleNamespace(started=False, run=lambda: None))
        with (
            mock.patch.dict(os.environ, environment),
            mock.patch.object(model_main, "build", return_value=(s.api, sink)),
            mock.patch.object(model_main, "SERVER_START_SECONDS", 0.5),
            mock.patch.dict(sys.modules, {"scripts.serving.api": SimpleNamespace(create_app=lambda api: api), "uvicorn": never}),
        ):
            model_main.main()
        c.check("a server that never starts loads no model", s.loader.attempts == [] and not s.loader.started.is_set())


PROXY_SWITCH = "MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD"


class ProbingMlflow:
    """The MLflow 3.17.2 client over an artifact server that stores artifacts in S3.

    A client with no ``MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD`` setting asks ``/server-info``, which advertises presigned downloads for an S3 destination, and
    reads each file straight from S3. The ECS task reaches the tracking server only, so that read fails and MLflow hides the cause behind one generic
    ``MlflowException``. With the setting ``false`` the client streams each file through the tracking server. Both artifact entry points of the loader (the
    run file download and the ``models:/`` model download) check the setting when they are called.
    """

    def __init__(self, listing: Path) -> None:
        self.listing = listing
        self.direct_reads = 0
        self.artifacts = SimpleNamespace(download_artifacts=self.download_artifacts)
        self.sklearn = SimpleNamespace(load_model=self.load_model)
        self.tags = {"feature_schema_sha256": "hash1"}
        self.MlflowClient = lambda: SimpleNamespace(get_model_version=lambda name, version: SimpleNamespace(run_id="run1", tags=dict(self.tags)))

    def proxied(self, what: str) -> bool:
        if os.environ.get(PROXY_SWITCH, "").lower() == "false":
            return True
        self.direct_reads += 1
        raise RuntimeError(f"MlflowException: Failed to download artifacts from path {what!r}, please ensure that the path is correct.")

    def download_artifacts(self, run_id: str, artifact_path: str) -> str:
        self.proxied(artifact_path)
        return str(self.listing)

    def load_model(self, uri: str) -> Any:
        self.proxied(uri)
        return SimpleNamespace(predict=lambda frame: [0.25])


def case_loader_reads_artifacts_through_the_tracking_server(c: Checker) -> None:
    """The loader keeps every artifact download on the tracking server: the presigned path that an S3 artifact store advertises is unreachable from the task."""
    with tempfile.TemporaryDirectory() as folder:
        listing = Path(folder) / "feature_list.json"
        listing.write_text(json.dumps({"references": ["a:1"], "columns": ["a"]}))
        library = ProbingMlflow(listing)
        frames = SimpleNamespace(DataFrame=lambda rows: rows)
        environment = {key: value for key, value in os.environ.items() if key != PROXY_SWITCH}
        with mock.patch.dict(sys.modules, {"mlflow": library, "pandas": frames}), mock.patch.dict(os.environ, environment, clear=True):
            try:
                model = mlflow_loader.MlflowLoader().load(HAI, PRODUCTION)
                failure = ""
            except RuntimeError as error:
                model, failure = None, str(error)
            c.check("the loader loads the version without a direct read of the artifact store", model is not None and library.direct_reads == 0 and not failure)
            c.check(
                "the loaded version carries its feature references and schema hash",
                model is not None and model.references == ("a:1",) and model.schema_hash == "hash1",
            )
            c.check("the loaded model predicts", model is not None and model.predict({"a:1": 1.0}) == 0.25)
            c.check("the setting is false for every later download in the process", os.environ.get(PROXY_SWITCH) == "false")


def case_loader_reads_the_feature_set(c: Checker) -> None:
    """The loader reads the model's feature set and the set's schema hash from the version tags; a set model with no set hash does not load."""
    with tempfile.TemporaryDirectory() as folder:
        listing = Path(folder) / "feature_list.json"
        listing.write_text(json.dumps({"references": ["a:1"], "columns": ["a"]}))
        library = ProbingMlflow(listing)
        frames = SimpleNamespace(DataFrame=lambda rows: rows)

        def loaded(tags: dict[str, str]) -> service.LoadedModel | None:
            library.tags = tags
            with mock.patch.dict(sys.modules, {"mlflow": library, "pandas": frames}):
                try:
                    return mlflow_loader.MlflowLoader().load(HAI, PRODUCTION)
                except ValueError:
                    return None

        both = {"feature_schema_sha256": "whole", "feature_set": "m2", "feature_set_schema_sha256": "setdigest"}
        model = loaded(both)
        c.check("a version tagged with a set loads for that set", model is not None and model.feature_set == "m2")
        c.check("its schema hash is the set's, which the set's marker holds", model is not None and model.schema_hash == "setdigest")
        c.check("its references stay the ones of its feature list", model is not None and model.references == ("a:1",))
        old = loaded({"feature_schema_sha256": "whole"})
        c.check(
            "a version with no set tag loads for the single path with the whole schema hash",
            old is not None and (old.feature_set, old.schema_hash) == ("", "whole"),
        )
        c.check("a version tagged with a set but no set hash does not load", loaded({"feature_schema_sha256": "whole", "feature_set": "m2"}) is None)
        c.check(
            "a version tagged with an empty set loads for the single path",
            (lambda m: m is not None and m.feature_set == "")(loaded({**both, "feature_set": ""})),
        )
        c.check("a version with a set tag that is not text of a set name does not load", loaded({**both, "feature_set": "M 2/x"}) is None)


def case_marker_metrics(c: Checker) -> None:
    """The model API reports what its marker reads show (the state of each feature set's online store, when it was materialized, how many entities it holds,
    when the API last read it) and counts each features refusal by one closed reason. A set name and a state are the only labels besides the reason."""

    def unreachable(inner: Any) -> Any:
        def read() -> Mapping[str, str]:
            raise online.OnlineError("Redis failed at marker read (ConnectionError)")

        return read

    with world() as w:
        ask(w)
        _, samples = scrape(w.api.metrics.render())
        marker = w.server.marker()
        c.check("a ready marker shows its state as 1 and no other state", series(samples, "hai_api_marker_state", feature_set="single", state="ready") == [1.0])
        c.check(
            "the states that are not current are 0",
            all(value == 0.0 for value in series(samples, "hai_api_marker_state", feature_set="single", state="loading")),
        )
        c.check(
            "the materialization time is the marker's",
            series(samples, "hai_api_marker_materialized_timestamp_seconds", feature_set="single")
            == [datetime.fromisoformat(marker["materialized_at"]).timestamp()],
        )
        c.check("the entity count is the marker's", series(samples, "hai_api_marker_entities", feature_set="single") == [float(marker["entity_count"])])
        c.check("the read time is the service clock's", series(samples, "hai_api_marker_read_timestamp_seconds", feature_set="single") == [w.wall.now])
        c.check("a request that was served counts no refusal", not series(samples, "hai_api_feature_refusals_total"))
        ask(w, "ZZZ999:2022")
        _, samples = scrape(w.api.metrics.render())
        c.check(
            "an unknown hospital-window is a refusal of its own reason",
            series(samples, "hai_api_feature_refusals_total", reason="hospital_window_not_found") == [1.0],
        )
        w.server.write_hash(online.CONTROL_DB, online.MARKER_KEY, {"state": "loading"})
        ask(w)
        ask(w)
        _, samples = scrape(w.api.metrics.render())
        c.check("a loading marker is the current state", series(samples, "hai_api_marker_state", feature_set="single", state="loading") == [1.0])
        c.check("and the ready state is 0 again", series(samples, "hai_api_marker_state", feature_set="single", state="ready") == [0.0])
        c.check("each refused request counts its reason", series(samples, "hai_api_feature_refusals_total", reason="marker_loading") == [2.0])
        w.server.restart()
        ask(w)
        _, samples = scrape(w.api.metrics.render())
        c.check("a store with no marker is the absent state", series(samples, "hai_api_marker_state", feature_set="single", state="absent") == [1.0])
        c.check("and its refusal reason is marker_absent", series(samples, "hai_api_feature_refusals_total", reason="marker_absent") == [1.0])
    with world(wrap_marker=unreachable) as w:
        ask(w)
        _, samples = scrape(w.api.metrics.render())
        c.check("a marker read that raises is the unreadable state", series(samples, "hai_api_marker_state", feature_set="single", state="unreadable") == [1.0])
        c.check("and its refusal reason is marker_read_failed", series(samples, "hai_api_feature_refusals_total", reason="marker_read_failed") == [1.0])
        c.check("an unreadable marker leaves no materialization time", not series(samples, "hai_api_marker_materialized_timestamp_seconds"))
    with world(single=False, sets=("m2",), configure=on_set("m2")) as w:
        ask(w)
        _, samples = scrape(w.api.metrics.render())
        c.check("a model set reports its own marker under its name", series(samples, "hai_api_marker_state", feature_set="m2", state="ready") == [1.0])
        c.check("and the single path reports nothing when it is not read", not series(samples, "hai_api_marker_state", feature_set="single"))
    fresh = metrics.Metrics()
    for number in range(1000):
        fresh.refusal(f"reason-{number}<script>")
        fresh.marker_read(f"set{number}", f"state{number}", 1.0, number, 2.0)
    text = fresh.render()
    _, samples = scrape(text)
    c.check("an unknown reason becomes other and adds one series", series(samples, "hai_api_feature_refusals_total", reason="other") == [1000.0])
    c.check(
        "a set name or state that is not closed adds nothing beyond the bound",
        len({labels.get("feature_set") for _, labels, _ in samples if "feature_set" in labels}) <= metrics.MAX_SETS,
    )
    c.check("the series stay bounded", len(text.splitlines()) < 400)
    c.check("the text holds no key, hospital or load ID", KEY_A not in text and "AAA001" not in text and "load_id" not in text)


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
                *contract,
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
        [metrics.route_label(path) for path in ("/v1/predict", "/v1/health", "/v1/live", "/metrics", "/v1/predict/")]
        == ["/v1/predict", "/v1/health", "/v1/live", "/metrics", "other"],
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
    "feature_refusals_are_logged": (case_feature_refusals_are_logged, "feast monitoring"),
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
    "deploy_verifies_the_active_version": (case_deploy_verifies_the_active_version, "deploy"),
    "canary_expiry": (case_canary_expiry, "deploy"),
    "deploy_expiry": (case_deploy_expiry, "deploy"),
    "manual_run_cannot_renew": (case_manual_run_cannot_renew, "deploy"),
    "canary_upgrade_state": (case_canary_upgrade_state, "deploy compatibility"),
    "canary_retry_identity": (case_canary_retry_identity, "deploy"),
    "expiry_is_not_requestable": (case_expiry_is_not_requestable, "deploy"),
    "chain_rollout": (case_chain_rollout, "deploy"),
    "deploy_job_entry": (case_deploy_job, "deploy aws"),
    "images_and_repositories": (case_images, "deploy aws"),
    "parameter_names_agree": (case_parameter_names, "iam services"),
    "predictions_append": (case_predictions, "iceberg"),
    "sink_late_commit": (case_sink_late_commit, "iceberg"),
    "sink_requests_no_vended_credentials": (case_sink_requests_no_vended_credentials, "iceberg polaris"),
    "sink_names_its_region": (case_sink_names_its_region, "iceberg polaris"),
    "sink_stalled_reply_leaves_no_client": (case_sink_stalled_reply_leaves_no_client, "iceberg polaris timeout"),
    "create_table_requests_no_vended_credentials": (case_create_table_requests_no_vended_credentials, "iceberg polaris"),
    "replay_and_compare": (case_replay_and_compare, "canary compare"),
    "replay_budget": (case_replay_budget, "canary compare"),
    "replay_http_drip": (case_replay_http_drip, "timeout network"),
    "previous_record_format": (case_previous_record_format, "deploy compatibility"),
    "liveness_without_a_deployment": (case_liveness_without_a_deployment, "api monitoring"),
    "startup_binds_before_the_load": (case_startup_binds_before_the_load, "api startup"),
    "startup_failed_load": (case_startup_failed_load, "api startup"),
    "main_binds_before_the_load": (case_main_binds_before_the_load, "api startup"),
    "startup_lifecycle": (case_startup_lifecycle, "api startup"),
    "startup_manifest_errors": (case_startup_manifest_errors, "api startup"),
    "startup_admitted_prediction_never_waits": (case_startup_admitted_prediction_never_waits, "api startup"),
    "startup_record_deadline_during_load": (case_startup_record_deadline_during_load, "api startup"),
    "startup_readiness_by_type": (case_startup_readiness_by_type, "api startup"),
    "startup_ready_log_line": (case_startup_ready_log_line, "api startup"),
    "startup_build_and_readiness": (case_startup_build_and_readiness, "api startup"),
    "loader_reads_artifacts_through_the_tracking_server": (case_loader_reads_artifacts_through_the_tracking_server, "api mlflow"),
    "loader_reads_the_feature_set": (case_loader_reads_the_feature_set, "api mlflow feast"),
    "set_model_served": (case_set_model_served, "feast api"),
    "metrics": (case_metrics, "monitoring"),
    "marker_metrics": (case_marker_metrics, "monitoring feast"),
    "dag_contract": (case_dag, "dag"),
}
