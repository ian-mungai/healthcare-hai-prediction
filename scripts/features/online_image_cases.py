"""The image part of the online cases (E22 window half, E23 online against offline, D8 and D12): the real Feast over lane B's real Redis service.

Run in the Feast image on the Redis service's network (the lead's heavy lane), with the job environment of that service::

    FEAST_REDIS_HOST=redis REDIS_PASSWORD=... python -B -m scripts.features.online_cases --part image

The fixture is the PyIceberg SQL catalog of ``feast_image_cases`` (built from the committed ``feature_schema.json``); the registry is a file in its folder.
The cases run the real writer protocol (``feast_materialize.run_materialize``) with the real Feast hooks, so they also cover the key parser,
the sample comparison and the window. They flush the service's databases, so run them only against the test registry's service. Not run in the agent sandbox.
"""

from __future__ import annotations

import importlib
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from scripts.features import feast_image_cases as image
from scripts.features import feast_job, feast_materialize, historical
from scripts.features import online_protocol as online
from scripts.features import publication_reads as reads
from scripts.features import serving_contract_client as serving
from scripts.features.feast_harness import Checker


def real_redis() -> dict[str, str]:
    values = {name: os.environ.get(name, "") for name in ("FEAST_REDIS_HOST", "REDIS_PASSWORD")}
    if not all(values.values()):
        raise SystemExit("the image part needs FEAST_REDIS_HOST and REDIS_PASSWORD of the test registry's online store")
    return values


def prepared(real: dict[str, str]) -> image.Fixture:
    """A fixture with its registry applied, the environment pointing at the real service, and the service's databases empty."""
    fixture = image.build_fixture()
    os.environ.update(real)
    image.apply_fixture(fixture)
    online.connect(os.environ, 0).flushall()  # type: ignore[attr-defined]
    return fixture


def materialize(fixture: image.Fixture) -> dict[str, Any]:
    settings = feast_job.job_settings(os.environ, "materialize", image.RUN_A, "publish_fixture")
    return feast_materialize.run_materialize(settings, os.environ, schema=(fixture.schema, fixture.schema_sha256))


def online_values(store: Any, refs: list[str], keys: list[str]) -> dict[tuple[str, str], Any]:
    answer = store.get_online_features(features=refs, entity_rows=[{"spine_key": key} for key in keys], full_feature_names=True).to_dict()
    return {(key, ref): answer[serving.feast_name(ref)][index] for index, key in enumerate(answer["spine_key"]) for ref in refs}


def case_e23(c: Checker) -> None:
    """D3, D4, D5, D8, B10: the load covers the inclusive window; online equals offline for every fixture key (null for null); an unknown key is 404."""
    real = real_redis()
    with image.preserved_environment():
        fixture = prepared(real)
        try:
            record = materialize(fixture)
            c.check(
                "the load writes every entity and ends ready",
                record["entities"] == len(image.SPINE_KEYS) and online.read_marker(online.connect(os.environ, online.CONTROL_DB)).get("state") == "ready",
            )
            control = online.connect(os.environ, online.CONTROL_DB)
            features: Any = online.connect(os.environ, online.FEATURE_DB, decode=False)
            ready, _ = online.readiness(control, features, image.RUN_A)
            c.check("readiness holds", ready)
            c.check(
                "database 1 holds exactly the entity keys and database 0 is empty",
                features.dbsize() == len(image.SPINE_KEYS) and online.connect(os.environ, 0, decode=False).dbsize() == 0,
            )
            c.check("entity keys never expire", all(features.pttl(key) == -1 for key in features.scan_iter()))
            store = historical.default_store(os.environ)
            keys = [image.key_of(ccn, year) for ccn, year in image.SPINE_KEYS]
            refs = image.references(fixture)
            predictors = [ref for ref in refs if ref in reads.reference_lists(fixture.schema)[0]]
            offline = historical.historical_features(predictors, environ=os.environ, store=store)
            offline_rows = {row["spine_key"]: row for row in offline.frame.to_pylist()}
            served = online_values(store, predictors, keys)
            different = [ref for ref in predictors for key in keys if served[(key, ref)] != offline_rows[key][ref.split(":", 1)[1]]]
            c.check(f"online equals offline for {len(keys)} keys and {len(predictors)} predictors, null for null", not different)
            c.check(
                "the key at the end of the window is served (inclusive window)",
                served[(image.key_of("DDD004", 2023), predictors[0])] is not None
                or any(served[(image.key_of("DDD004", 2023), ref)] is not None for ref in predictors),
            )
            reader = serving.feast_reader(store)
            marker = serving.marker_reader(os.environ)
            probe_refs = [predictors[0]]
            got = serving.serve(marker, reader, references=probe_refs, schema_hash=fixture.schema_sha256, ccn="AAA001", window_year=2021)
            c.check("a known key is served with its window start", got.features_as_of == "2021-01-01")
            without_window = [ref for ref in predictors if not ref.startswith(serving.WINDOW_VIEW)][:1]
            for model in (probe_refs, without_window):
                try:
                    serving.serve(marker, reader, references=model, schema_hash=fixture.schema_sha256, ccn="ZZZ999", window_year=2099)
                    status = 200
                except serving.NotFound:
                    status = 404
                c.check("an unknown key is 404", status == 404)
            servable = reads.read_schema()[0]["servable"]
            c.check("every served reference is registered (B10)", set(predictors) <= set(servable) and serving.PROBE in servable)
        finally:
            image.drop(fixture)


def case_d12(c: Checker) -> None:
    """D2, D6, D12: a corrected value with the same event time is served after the next load (flush and full reload, dedup off); equal content on a rerun."""
    real = real_redis()
    with image.preserved_environment():
        fixture = prepared(real)
        try:
            materialize(fixture)
            features = online.connect(os.environ, online.FEATURE_DB, decode=False)
            first = online.database_digest(features)
            materialize(fixture)
            c.check("a rerun of the same set gives the same content digest", online.database_digest(features) == first)
            table, column = fixture.target
            key = image.key_of("AAA001", 2022)
            iceberg = fixture.table(table)
            pa = importlib.import_module("pyarrow")
            rows = [row for row in iceberg.scan().to_arrow().to_pylist() if row["spine_key"] == key]
            corrected = {**rows[0], column: 123.5}
            iceberg.delete(f"spine_key = '{key}'")
            iceberg.append(pa.Table.from_pylist([corrected], schema=iceberg.scan().to_arrow().schema))
            fixture.publish("20261022T130000Z-1a2b3c4d", committed=datetime.now(UTC) + timedelta(seconds=5))
            materialize(fixture)
            store = historical.default_store(os.environ)
            reference = f"{table}:{column}"
            c.check("the corrected value is served after the reload", online_values(store, [reference], [key])[(key, reference)] == 123.5)
            marker = online.read_marker(online.connect(os.environ, online.CONTROL_DB))
            c.check("the marker names the new publication", marker.get("feature_run_id") == "20261022T130000Z-1a2b3c4d")
        finally:
            image.drop(fixture)


IMAGE_CASES: dict[str, tuple[Callable[[Checker], None], str]] = {
    "E23_online": (case_e23, "LO14 D3 D4 D5 D8 B10"),
    "E22_correction": (case_d12, "D2 D6 D12"),
}
