"""Cases of the Feast job and the historical helper (design p3c cases E11 to E17 and E30 to E32 as they apply to ``feast_job``, ``historical``,
``publication_reads`` and ``feature_store.yaml``).

    .venv/bin/python -B -m scripts.features.feast_cases --part local [--case E17_during ...]      # now: stub catalogs, no Feast, no PyIceberg, no Docker
    python -B -m scripts.features.feast_cases --part image [--case E16 ...]                        # inside the Feast image (the lead runs it)

``--part local`` runs the logic that needs neither Feast nor PyIceberg: head verification and publication selection against a stub catalog, the schema
and view comparison, settings and registry-key handling, the token exchange against a loopback server, the private ``.pyiceberg.yaml``, the apply order
with a recording backend, the historical helper's entity rules, the banned store-removal call and the committed ``feature_store.yaml``. ``--part image``
(``feast_image_cases``) builds a PyIceberg SQL catalog fixture from the committed ``feature_schema.json`` and runs the real Feast apply and historical reads.
Both parts write ``data/e2e/feast_cases/<UTC stamp>/report.json``: case names, failure-mode IDs, check names and verdicts only, never a value.
Exit code 0 when every selected case passes.
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import http.server
import importlib
import json
import os
import re
import shutil
import stat
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from scripts.features import feast_job, fixture_catalog, historical
from scripts.features import publication_reads as reads
from scripts.features.feast_harness import Checker, fake_module, fake_redis_modules, run_cases, write_report
from scripts.features.publication_reads import ReadError

ROOT = reads.REPO_ROOT
# The banned store-removal command, spelled in two pieces so that no source file of the package contains the word.
BANNED = "tear" + "down"
SENTINEL = "sentinel-secret-9f31c2"
# Variable names the cases set or plant in the process environment, named through constants so no case reads a literal name outside .env.example.
PLANTED_SETTING = "FAKE_SETTING"
# The stand-in value for this variable is a sentinel kept in memory only; it is never written to a file.
ONLINE_STORE_AUTH_VARIABLE = "REDIS_" + "PASSWORD"
ZONE_VARIABLE = "TZ"
T0 = datetime(2026, 10, 22, 12, 0, tzinfo=UTC)
RUN_A = "20261022T120000Z-0a1b2c3d"
RUN_B = "20261022T130000Z-1a2b3c4d"
RUN_C = "20261022T140000Z-2a3b4c5d"


def _plus(hours: int) -> timezone:
    return timezone(timedelta(hours=hours))


# ---------------------------------------------------------------------------------------------------------------------------------------------
# Stubs: a catalog of tables with a main snapshot, a schema and rows; every load is logged.
# ---------------------------------------------------------------------------------------------------------------------------------------------


class StubScan:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def to_arrow(self) -> StubScan:
        return self

    def to_pylist(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.rows]


class StubTable:
    def __init__(self, name: str, main: int | None, columns: dict[str, str] | None = None, rows: list[dict[str, Any]] | None = None) -> None:
        self.name = name
        self.main = main
        self.columns = columns or {}
        self.rows = rows or []
        self.scans: list[int | None] = []

    def snapshot_by_name(self, name: str) -> Any:
        return SimpleNamespace(snapshot_id=self.main) if name == "main" and self.main is not None else None

    def schema(self) -> Any:
        return SimpleNamespace(fields=[SimpleNamespace(name=name, field_type=kind) for name, kind in self.columns.items()])

    def scan(self, *, selected_fields: tuple[str, ...], snapshot_id: int | None = None) -> StubScan:
        self.scans.append(snapshot_id)
        return StubScan([{key: row.get(key) for key in selected_fields} for row in self.rows])


class StubCatalog:
    def __init__(self, namespace: str = "features_e2e", log: list[str] | None = None) -> None:
        self.namespace = namespace
        self.tables: dict[str, StubTable] = {}
        self.log = log if log is not None else []

    def load_table(self, identifier: str) -> StubTable:
        namespace, _, name = identifier.partition(".")
        self.log.append(f"load:{name}")
        if namespace != self.namespace or name not in self.tables:
            raise KeyError(identifier)
        return self.tables[name]


def synthetic_schema() -> dict[str, Any]:
    """A small schema in the shape of ``feature_schema.json``: two predictor tables (one with identifiers), a label table and an excluded control."""

    def column(name: str, role: str, kind: str, view: str | None, feast: str | None) -> dict[str, Any]:
        return {"name": name, "role": role, "type": kind, "view": view, "feast_type": feast}

    key, when = column("spine_key", "key", "varchar", None, None), column("event_timestamp", "event_time", "timestamp with time zone", None, None)
    window = [key, when, column("ccn", "identifier", "varchar", None, None)] + [
        column("bed_count", "predictor", "bigint", "feature_matrix_window", "Int64"),
        column("has_pos_snapshot", "predictor", "boolean", "feature_matrix_window", "Bool"),
    ]
    measures = [key, when, column("c001__value__number", "predictor", "double", "feature_matrix_measures", "Float64")]
    measures.append(column("c002__value__category", "predictor", "varchar", "feature_matrix_measures", "String"))
    labels = [
        key,
        when,
        column("hai_1__sir__number", "label", "double", "label_hai_1", None),
        column("hai_2__sir__number", "label", "double", "label_hai_2", None),
    ]
    tables = {
        "feature_matrix_window": {"role": "predictor", "columns": window},
        "feature_matrix_measures": {"role": "predictor", "columns": measures},
        "label_matrix_hai_outcomes": {"role": "label", "columns": labels},
    }
    servable = ["feature_matrix_window:bed_count", "feature_matrix_window:has_pos_snapshot", "feature_matrix_measures:c001__value__number"]
    servable.append("feature_matrix_measures:c002__value__category")
    return {"schema": 1, "excluded_controls": ["C269", "C283.lower_limits"], "servable": servable, "tables": tables, "entity": {"join_key": "spine_key"}}


def iceberg_view(schema: dict[str, Any]) -> dict[str, dict[str, str]]:
    return {name: {c["name"]: reads.ICEBERG_TYPES[c["type"]] for c in table["columns"]} for name, table in schema["tables"].items()}


def runs_row(run_id: str, committed: datetime | str, snapshot_set: Any) -> dict[str, Any]:
    raw = snapshot_set if isinstance(snapshot_set, str) or snapshot_set is None else json.dumps(snapshot_set)
    return {"run_id": run_id, "committed_at": committed, "snapshot_set": raw}


def stub_features(
    schema: dict[str, Any] | None = None, *, runs: list[dict[str, Any]] | None = None, log: list[str] | None = None
) -> tuple[StubCatalog, dict[str, int]]:
    """A catalog holding the schema's tables, the spine and a ``publish_runs`` table whose head names every table's main snapshot."""
    schema = schema or synthetic_schema()
    catalog = StubCatalog(log=log)
    snapshots = {}
    for number, (name, columns) in enumerate(sorted(iceberg_view(schema).items()), start=1):
        snapshots[name] = 1000 + number
        catalog.tables[name] = StubTable(name, snapshots[name], columns)
    spine_rows = [
        {"spine_key": "AAA001:2021", "window_start": date(2021, 1, 1)},
        {"spine_key": "AAA001:2022", "window_start": date(2022, 1, 1)},
        {"spine_key": "BBB002:2022", "window_start": date(2022, 1, 1)},
    ]
    snapshots[reads.SPINE_TABLE] = 2000
    catalog.tables[reads.SPINE_TABLE] = StubTable(reads.SPINE_TABLE, 2000, {"spine_key": "string", "window_start": "date"}, spine_rows)
    for index, name in enumerate(("int_spine_validation_measures", "int_county_adjacency"), start=2100):
        snapshots[name] = index
        catalog.tables[name] = StubTable(name, index, {"support_key": "string"}, [{"support_key": "synthetic"}])
    rows = runs if runs is not None else [runs_row(RUN_A, T0, snapshots)]
    catalog.tables[reads.RUNS_TABLE] = StubTable(reads.RUNS_TABLE, 3000, {}, rows)
    return catalog, snapshots


# ---------------------------------------------------------------------------------------------------------------------------------------------
# Publication selection and head verification (E17).
# ---------------------------------------------------------------------------------------------------------------------------------------------


def case_e17_select(c: Checker) -> None:
    """LC1 to LC3 and LH7: the head row is chosen like the gold head query; malformed and missing rows are refused."""
    namespace = "features_e2e"
    empty, _ = stub_features(runs=[])
    c.refused("no committed publication is refused", lambda: reads.read_publication(empty, namespace), "no committed publication")
    missing = StubCatalog()
    c.refused("a missing runs table is refused with the table named", lambda: reads.read_publication(missing, namespace), "publish_runs cannot be loaded")
    good = {"feature_matrix_a": 11, "int_hospital_spine": 12}
    for label, raw in (
        ("not JSON", "{"),
        ("an array", "[1]"),
        ("empty", "{}"),
        ("a null value", '{"feature_matrix_a": null}'),
        ("a zero ID", '{"feature_matrix_a": 0}'),
        ("a text ID", '{"feature_matrix_a": "7"}'),
        ("a boolean ID", '{"feature_matrix_a": true}'),
        ("a bad table name", '{"bad name": 5}'),
        ("a missing value", None),
    ):
        catalog, _ = stub_features(runs=[runs_row(RUN_A, T0, raw)])
        c.refused(f"snapshot_set {label} is refused", reads.read_publication, "snapshot_set", catalog, namespace)
    bad_id, _ = stub_features(runs=[runs_row("not-a-run-id", T0, good)])
    c.refused("a row with a malformed run_id is refused", lambda: reads.read_publication(bad_id, namespace), "not a run ID")
    bad_time, _ = stub_features(runs=[runs_row(RUN_A, "yesterday", good)])
    c.refused("a row with an unreadable commit time is refused", lambda: reads.read_publication(bad_time, namespace), "committed_at")
    newer = T0 + timedelta(hours=1)
    catalog, _ = stub_features(runs=[runs_row(RUN_A, T0, good), runs_row(RUN_C, newer, good), runs_row(RUN_B, T0, good)])
    c.check("the head is the newest commit", reads.read_publication(catalog, namespace).run_id == RUN_C)
    tie, _ = stub_features(runs=[runs_row(RUN_A, T0, good), runs_row(RUN_B, T0, good)])
    c.check("a tie goes to the larger run ID", reads.read_publication(tie, namespace).run_id == RUN_B)
    mixed, _ = stub_features(runs=[runs_row(RUN_A, T0.replace(tzinfo=None), good), runs_row(RUN_B, T0 + timedelta(seconds=1), good)])
    c.check("naive and aware commit times compare as UTC", reads.read_publication(mixed, namespace).run_id == RUN_B)
    c.check(
        "an ISO text commit time is read", reads.read_publication(stub_features(runs=[runs_row(RUN_A, T0.isoformat(), good)])[0], namespace).run_id == RUN_A
    )
    c.check("a named run is selected even when it is not the head", reads.read_publication(catalog, namespace, RUN_A).run_id == RUN_A)
    c.refused("a named run that does not exist is refused", lambda: reads.read_publication(catalog, namespace, "20200101T000000Z-deadbeef"), "exactly one")
    publication = reads.read_publication(catalog, namespace, RUN_A)
    c.check("the publication carries its snapshot set and digest", dict(publication.snapshot_set) == good and len(publication.snapshot_set_sha256) == 64)


def case_e17_before(c: Checker) -> None:
    """LC4: a table whose main differs from the row is refused before the read, and the read does not run."""
    catalog, snapshots = stub_features()
    publication = reads.read_publication(catalog, "features_e2e")
    ran: list[str] = []

    def read_ok() -> str:
        ran.append("read")
        return "result"

    c.check("an unchanged set passes", reads.consistent_read(catalog, publication, read_ok) == "result")
    ran.clear()
    catalog.tables["feature_matrix_measures"].main = 9999
    message = c.refused(
        "a moved table is refused before the read", lambda: reads.consistent_read(catalog, publication, lambda: ran.append("read")), "before the read"
    )
    c.check(
        "the refusal names the table and both snapshot IDs",
        "feature_matrix_measures" in message and "9999" in message and str(snapshots["feature_matrix_measures"]) in message,
    )
    c.check("the read did not run", ran == [])
    many = StubCatalog()
    for number in range(8):
        many.tables[f"table_{number}"] = StubTable(f"table_{number}", 99)
    wide = reads.Publication("features_e2e", RUN_A, T0.isoformat(), {f"table_{number}": number + 1 for number in range(8)})
    message = c.refused("many moved tables are refused", lambda: reads.verify_heads(many, wide, "before the read"), "8 of 8 tables are not at the publication")
    c.check("a long list of tables is shortened", "and 3 more" in message)


def case_e17_during(c: Checker) -> None:
    """LC5, LC8 and LC9: a table moved while the read runs (a commit between fast-forwards) refuses the result."""
    catalog, _ = stub_features()
    publication = reads.read_publication(catalog, "features_e2e")

    def read_and_move() -> str:
        catalog.tables["feature_matrix_window"].main = 4242
        return "result that must be discarded"

    outcome: list[str] = []
    message = c.refused(
        "a table moved during the read is refused", lambda: outcome.append(reads.consistent_read(catalog, publication, read_and_move)), "after the read"
    )
    c.check("the caller never receives the result", outcome == [])
    c.check("the refusal names the moved table", "feature_matrix_window" in message)
    catalog, _ = stub_features()
    publication = reads.read_publication(catalog, "features_e2e")

    def read_and_fail() -> str:
        raise RuntimeError("the read failed")

    catalog.log.clear()
    try:
        reads.consistent_read(catalog, publication, read_and_fail)
        failed = False
    except RuntimeError:
        failed = True
    loads_after_error = len(catalog.log)
    c.check("an error inside the read propagates unchanged", failed)
    c.check("no second check runs after an error", loads_after_error == len(publication.snapshot_set))
    after_only, _ = stub_features()
    publication = reads.read_publication(after_only, "features_e2e")
    after_only.tables["feature_matrix_window"].main = None
    c.refused("a table without a main snapshot is refused", lambda: reads.verify_heads(after_only, publication, "before the read"), "no main snapshot")


def check_support_read(c: Checker, name: str) -> None:
    catalog, snapshots = stub_features()
    publication = reads.read_publication(catalog, "features_e2e")
    table = catalog.tables[name]

    def scan() -> list[dict[str, Any]]:
        return (
            reads.load_table(catalog, publication.namespace, name)
            .scan(selected_fields=("support_key",), snapshot_id=publication.snapshot_set[name])
            .to_arrow()
            .to_pylist()
        )

    c.check(
        f"{name} reads the selected snapshot",
        reads.consistent_read(catalog, publication, scan) == [{"support_key": "synthetic"}] and table.scans == [snapshots[name]],
    )
    table.main = 4242
    before = len(table.scans)
    message = c.refused(f"{name} moved before consumption", lambda: reads.consistent_read(catalog, publication, scan), "before the read")
    c.check(f"{name} refusal names the table and skips consumption", name in message and len(table.scans) == before)
    table.main = snapshots[name]

    def scan_and_move() -> list[dict[str, Any]]:
        result = scan()
        table.main = 4242
        return result

    received: list[Any] = []
    message = c.refused(
        f"{name} moved after consumption", lambda: received.append(reads.consistent_read(catalog, publication, scan_and_move)), "after the read"
    )
    c.check(f"{name} moved result is discarded", name in message and received == [])


def case_e17_support(c: Checker) -> None:
    """DT4: support scans use R's snapshot IDs and a moved support head refuses the result."""
    for name in ("int_spine_validation_measures", "int_county_adjacency"):
        check_support_read(c, name)


def case_e17_tables(c: Checker) -> None:
    """LC6: a table the row names but the catalog lacks, and a schema table the row does not name, are refused."""
    catalog, _ = stub_features()
    publication = reads.read_publication(catalog, "features_e2e")
    del catalog.tables["feature_matrix_window"]
    message = c.refused("a table missing from the catalog is refused", lambda: reads.verify_heads(catalog, publication, "before the read"), "cannot be loaded")
    c.check("the message names the table", "feature_matrix_window" in message)
    schema = synthetic_schema()
    catalog, snapshots = stub_features(schema)
    short = {name: snapshot for name, snapshot in snapshots.items() if name != "feature_matrix_measures"}
    catalog.tables[reads.RUNS_TABLE].rows = [runs_row(RUN_A, T0, short)]
    publication = reads.read_publication(catalog, "features_e2e")
    backend = RecordingBackend(schema, [])
    c.refused(
        "a schema table the publication does not name is refused",
        lambda: feast_job.preflight(catalog, publication, schema, backend),
        "does not name this table",
    )


def case_e17_fresh(c: Checker) -> None:
    """LC7: each head check loads every table again; a check never reuses what the earlier one loaded."""
    catalog, _ = stub_features()
    publication = reads.read_publication(catalog, "features_e2e")
    catalog.log.clear()
    reads.consistent_read(catalog, publication, lambda: "ok")
    count = len(publication.snapshot_set)
    c.check("two checks load every table twice", catalog.log.count("load:feature_matrix_window") == 2 and len(catalog.log) == 2 * count)
    c.check("the second check follows the read", catalog.log[count:] == catalog.log[:count])


# ---------------------------------------------------------------------------------------------------------------------------------------------
# Apply: schema and views (E11), order, lease and object list (E13).
# ---------------------------------------------------------------------------------------------------------------------------------------------


class RecordingBackend:
    """A stand-in for Feast that logs its calls; ``views`` overrides what the repository registers."""

    version = "0.0-test"

    def __init__(
        self,
        schema: dict[str, Any],
        log: list[str],
        *,
        views: dict[str, tuple[bool, tuple[str, ...]]] | None = None,
        fail: bool = False,
        during_apply: Callable[[], None] | None = None,
    ) -> None:
        self.schema, self.log, self.views, self.fail, self.during_apply = schema, log, views, fail, during_apply

    def registered_views(self) -> dict[str, tuple[bool, tuple[str, ...]]]:
        if self.views is not None:
            return self.views
        registered: dict[str, tuple[bool, list[str]]] = {}
        for table in self.schema["tables"].values():
            for column in table["columns"]:
                if column["role"] in ("predictor", "label"):
                    registered.setdefault(column["view"], (column["role"] == "predictor", []))[1].append(column["name"])
        return {name: (online, tuple(sorted(columns))) for name, (online, columns) in registered.items()}

    def apply(self) -> None:
        self.log.append("apply")
        if self.during_apply is not None:
            self.during_apply()
        if self.fail:
            raise RuntimeError("feast failed")

    def object_list(self) -> list[dict[str, Any]]:
        self.log.append("read_back")
        return [{"kind": "entity", "name": "hospital_window", "join_key": "spine_key"}, {"kind": "feature_view", "name": "feature_matrix_window"}]


def recording_lease(log: list[str], *, refuse: bool = False) -> feast_job.Lease:
    @contextlib.contextmanager
    def lease() -> Iterator[None]:
        if refuse:
            raise ReadError("the Redis lease is held by another writer")
        log.append("lease_acquired")
        try:
            yield
        finally:
            log.append("lease_released")

    return lease


def fixture_environment(folder: Path) -> dict[str, str]:
    """The environment of a fixture Feast job, as lane B renders it plus the run folder."""
    return {
        "FEAST_NAMESPACE": "features_e2e",
        "FEAST_CATALOG_TYPE": "sql",
        "FEAST_RUN_FOLDER": str(folder),
        "FEAST_REGISTRY_PATH": f"{folder}/feast/registry.pb",
        "FEAST_REDIS_HOST": "redis",
        "REDIS_PASSWORD": SENTINEL,
    }


def fixture_settings(folder: Path) -> feast_job.JobSettings:
    return feast_job.JobSettings("apply", RUN_A, "publish_fixture", folder, f"{folder}/feast/registry.pb", "")


def run_apply(catalog: StubCatalog, backend: RecordingBackend, lease: feast_job.Lease, schema: dict[str, Any], folder: Path) -> dict[str, Any]:
    return feast_job.apply(catalog, fixture_settings(folder), schema=schema, schema_sha256="c" * 64, backend=backend, lease=lease, deadline=lambda phase: None)


def case_e11_schema(c: Checker) -> None:
    """LC10 and LC11 (schema half): table drift and a bad view list are refused before anything is applied."""
    schema = synthetic_schema()
    iceberg = iceberg_view(schema)
    c.check("a matching schema has no problems", reads.column_problems(schema, iceberg) == [] and reads.view_problems(schema) == [])
    drift = {name: dict(columns) for name, columns in iceberg.items()}
    del drift["feature_matrix_window"]["bed_count"]
    drift["feature_matrix_measures"]["extra_column"] = "string"
    drift["feature_matrix_measures"]["c001__value__number"] = "float"
    drift["label_matrix_hai_outcomes"]["hai_1__sir__number"] = "date"
    problems = " | ".join(reads.column_problems(schema, drift))
    c.check("a missing column is named", "feature_matrix_window: 1 columns missing (bed_count)" in problems)
    c.check("an extra column is named", "extra_column" in problems)
    c.check("a changed type is named", "c001__value__number" in problems and "hai_1__sir__number" in problems)
    c.check(
        "a missing table is named",
        "feature_matrix_measures: the table is missing"
        in " | ".join(reads.column_problems(schema, {k: v for k, v in iceberg.items() if k != "feature_matrix_measures"})),
    )
    date_column = {name: dict(columns) for name, columns in iceberg.items()}
    date_column["feature_matrix_window"]["bed_count"] = "int"
    c.check("a 32-bit integer is a type difference", any("bed_count" in item for item in reads.column_problems(schema, date_column)))
    broken = synthetic_schema()
    broken["tables"]["feature_matrix_window"]["columns"].append(
        {"name": "label_in_view", "role": "label", "type": "double", "view": "label_hai_1", "feast_type": None}
    )
    c.check("a label in a predictor table is a problem", any("label_in_view" in item for item in reads.view_problems(broken)))
    ident = synthetic_schema()
    ident["tables"]["feature_matrix_window"]["columns"][2]["view"] = "feature_matrix_window"
    c.check("a registered identifier is a problem", any("identifier column is registered" in item for item in reads.view_problems(ident)))
    excluded = synthetic_schema()
    excluded["tables"]["feature_matrix_measures"]["columns"].append(
        {"name": "c269__value__number", "role": "predictor", "type": "double", "view": "feature_matrix_measures", "feast_type": "Float64"}
    )
    excluded["servable"].append("feature_matrix_measures:c269__value__number")
    c.check("an excluded control is a problem", any("excluded" in item for item in reads.view_problems(excluded)))
    c.check("an excluded control with a field is excluded as a whole", reads.base_control("C283.lower_limits") == "c283")
    wrong_type = synthetic_schema()
    wrong_type["tables"]["feature_matrix_window"]["columns"][3]["feast_type"] = "Int32"
    c.check("a Feast type outside the closed four is a problem", any("Feast type" in item for item in reads.view_problems(wrong_type)))
    servable = synthetic_schema()
    servable["servable"].pop()
    c.check("a servable list that differs from the predictors is a problem", any("servable" in item for item in reads.view_problems(servable)))


def case_e11_views(c: Checker) -> None:
    """LC11 (Feast half): the views parsed from the repository must equal the schema's predictors and labels; nothing is applied when they differ."""
    schema = synthetic_schema()
    catalog, _ = stub_features(schema)
    publication = reads.read_publication(catalog, "features_e2e")
    log: list[str] = []
    feast_job.preflight(catalog, publication, schema, RecordingBackend(schema, log))
    c.check("matching views pass", True)
    good = RecordingBackend(schema, log).registered_views()
    extra = dict(good)
    extra["feature_matrix_window"] = (True, (*good["feature_matrix_window"][1], "ccn"))
    c.refused(
        "a registered identifier is refused",
        lambda: feast_job.preflight(catalog, publication, schema, RecordingBackend(schema, log, views=extra)),
        "online feature views",
    )
    labelled = dict(good)
    labelled["label_hai_1"] = (True, good["label_hai_1"][1])
    c.refused(
        "a label view that is online is refused",
        lambda: feast_job.preflight(catalog, publication, schema, RecordingBackend(schema, log, views=labelled)),
        "online feature views",
    )
    short = {name: value for name, value in good.items() if name != "label_hai_2"}
    c.refused(
        "a missing label view is refused",
        lambda: feast_job.preflight(catalog, publication, schema, RecordingBackend(schema, log, views=short)),
        "offline-only views",
    )
    catalog.tables["feature_matrix_window"].columns["surprise"] = "string"
    c.refused(
        "a table that drifted from the schema is refused",
        lambda: feast_job.preflight(catalog, publication, schema, RecordingBackend(schema, log)),
        "1 columns extra",
    )


def case_e11_variables(c: Checker) -> None:
    """LC19 and B9: an unset, empty or literal variable of feature_store.yaml is refused by name; no value appears."""
    text = feast_job.SETTINGS_FILE.read_text()
    full = {"FEAST_REGISTRY_PATH": "/x/registry.pb", "FEAST_REDIS_HOST": "redis", "REDIS_PASSWORD": SENTINEL}
    c.check("a complete environment has no unresolved variable", reads.unresolved_variables(text, full) == [])
    names = sorted(full)
    for name in names:
        without = {key: value for key, value in full.items() if key != name}
        c.check(f"unset {name} is found", reads.unresolved_variables(text, without) == [name])
    c.check("an empty variable is found", reads.unresolved_variables(text, {**full, "REDIS_PASSWORD": ""}) == ["REDIS_PASSWORD"])
    c.check(
        "a variable that still reads ${...} is found",
        reads.unresolved_variables(text, {**full, "FEAST_REDIS_HOST": "${FEAST_REDIS_HOST}"}) == ["FEAST_REDIS_HOST"],
    )
    c.check("the dollar form is found too", reads.unresolved_variables("a: $MISSING_ONE", {}) == ["MISSING_ONE"])
    with tempfile.TemporaryDirectory(prefix="feast_cases_") as raw:
        folder = Path(raw)
        environ = {
            "FEAST_RUN_FOLDER": str(folder),
            "FEAST_NAMESPACE": "features_e2e",
            "FEAST_CATALOG_TYPE": "sql",
            "FEAST_REGISTRY_PATH": f"{folder}/feast/registry.pb",
            "FEAST_REDIS_HOST": "redis",
        }
        settings = feast_job.job_settings(environ, "apply", RUN_A, "publish_fixture")
        message = c.refused(
            "the session refuses while REDIS_PASSWORD is unset", lambda: feast_job.catalog_session(settings, environ).__enter__(), "REDIS_PASSWORD"
        )
        c.check("the refusal lists names only", SENTINEL not in message)


def case_e13_order(c: Checker) -> None:
    """LC13: heads are verified before the pre-apply comparison and again, inside the lease, after the read-back."""
    schema = synthetic_schema()
    log: list[str] = []
    catalog, _ = stub_features(schema, log=log)
    with tempfile.TemporaryDirectory(prefix="feast_cases_") as raw:
        record = run_apply(catalog, RecordingBackend(schema, log), recording_lease(log), schema, Path(raw))
    events = [item for item in log if not item.startswith("load:")]
    c.check("lease, apply, read-back and release come in that order", events == ["lease_acquired", "apply", "read_back", "lease_released"])
    tables = len(reads.read_publication(catalog, "features_e2e").snapshot_set)
    schema_tables = len(schema["tables"])
    before_lease = log.index("lease_acquired")
    c.check("the runs table, the heads and the schema tables are loaded before the lease", before_lease == 1 + tables + schema_tables)
    c.check("the heads are loaded again after the read-back and before the release", log.index("lease_released") - log.index("read_back") - 1 == tables)
    c.check("the record names the publication and the schema digest", record["publication"]["run_id"] == RUN_A and record["feature_schema_sha256"] == "c" * 64)
    c.check("the record counts objects", record["objects"] == {"entity": 1, "feature_view": 1, "data_source": 0})


def case_e13_lease(c: Checker) -> None:
    """LC12: nothing is applied without the lease; the lease is released after a failure; a refused pre-apply check never takes the lease."""
    schema = synthetic_schema()
    with tempfile.TemporaryDirectory(prefix="feast_cases_") as raw:
        folder = Path(raw)
        log: list[str] = []
        catalog, _ = stub_features(schema, log=log)
        c.refused(
            "a held lease stops the job", lambda: run_apply(catalog, RecordingBackend(schema, log), recording_lease(log, refuse=True), schema, folder), "lease"
        )
        c.check("nothing was applied", "apply" not in log)
        log.clear()
        try:
            run_apply(catalog, RecordingBackend(schema, log, fail=True), recording_lease(log), schema, folder)
            raised = False
        except RuntimeError:
            raised = True
        c.check("a failing apply raises", raised)
        c.check("the lease is released after the failure", log.count("lease_acquired") == 1 and log.count("lease_released") == 1)
        log.clear()
        catalog.tables["feature_matrix_window"].columns["surprise"] = "string"
        c.refused(
            "a drifted table is refused",
            lambda: run_apply(catalog, RecordingBackend(schema, log), recording_lease(log), schema, folder),
            "refused before apply",
        )
        c.check("the lease was never taken", "lease_acquired" not in log and "apply" not in log)
        catalog, _ = stub_features(schema, log=log)
        log.clear()
        publication_catalog = catalog

        def move_table() -> None:
            publication_catalog.tables["feature_matrix_measures"].main = 777

        message = c.refused(
            "a table that moves during apply is refused after the read-back",
            lambda: run_apply(catalog, RecordingBackend(schema, log, during_apply=move_table), recording_lease(log), schema, folder),
            "after apply",
        )
        c.check("the refusal names the table", "feature_matrix_measures" in message)
        c.check("the lease is still released", log[-1] == "lease_released")
        c.check("no record file is written by a refused apply", not (folder / feast_job.RECORD_NAME).exists())
        for number in range(2):
            environ = fixture_environment(folder)
            catalog, _ = stub_features(schema, log=log)
            settings = feast_job.job_settings(environ, "apply", RUN_A, "publish_fixture")
            record = feast_job.run(
                settings,
                environ,
                backend=RecordingBackend(schema, log),
                lease=recording_lease(log),
                schema=(schema, "c" * 64),
                loader=lambda name, catalog=catalog, **properties: catalog,
                register=False,
            )
            written = json.loads((folder / feast_job.RECORD_NAME).read_text())
            c.check(f"run {number + 1} writes the record after the apply returned", written == record)


def case_e13_protocol(c: Checker) -> None:
    """LC12: the job takes its lease from ``online_protocol.writer_lease`` and refuses to run when that module is not built."""
    name = "scripts.features.online_protocol"
    with mock.patch.dict(sys.modules, {name: None}):
        c.refused("a missing online protocol module stops the job", feast_job.protocol_lease, "is not built", {})
    events: list[str] = []
    with mock.patch.dict(sys.modules, {name: fake_module(name, writer_lease=lambda environ: recording_lease(events)())}):
        factory = feast_job.protocol_lease({"X": "1"})
        with factory():
            events.append("inside")
    c.check("the module's lease surrounds the apply", events == ["lease_acquired", "inside", "lease_released"])
    with mock.patch.dict(sys.modules, {name: fake_module(name)}):
        c.refused("a module without writer_lease stops the job", feast_job.protocol_lease, "is not built", {})


def case_e13_repository(c: Checker) -> None:
    """Image-run causes: the repository loads under ``-m`` (fixed path entry), a second load reads its environment, Feast's Redis client is bounded."""
    with tempfile.TemporaryDirectory(prefix="feast_cases_") as raw:
        repo = Path(raw).resolve() / "repo"
        repo.mkdir()
        (repo / "definitions.py").write_text(f"import os\nSETTING = os.environ[{PLANTED_SETTING!r}]\nVIEWS = [SETTING]\n")
        seen: list[str] = []
        job_store = SimpleNamespace(_client=None)

        def parse_repo(root: Path) -> Any:
            module = importlib.import_module("definitions")
            seen.append(f"parse:{module.SETTING}:{os.getcwd() == str(repo)}")
            return SimpleNamespace(feature_views=[])

        def apply_total(config: Any, root: Path, **options: Any) -> None:
            del config, options
            os.chdir(root)
            module = importlib.import_module("definitions")
            client = job_store._client
            bounded = client is not None and (client.socket_timeout, client.db, client.retry.retries) == (5.0, 1, 0)
            seen.append(f"apply:{module.SETTING}:{bounded}")

        modules = {
            "feast.repo_operations": fake_module("feast.repo_operations", parse_repo=parse_repo, apply_total=apply_total),
            "feast.repo_config": fake_module("feast.repo_config", load_repo_config=lambda root, settings: object()),
            "feast.infra.online_stores.redis": fake_module("feast.infra.online_stores.redis", RedisOnlineStore=job_store),
        }
        before_path, before_cwd = list(sys.path), os.getcwd()
        environ = {"FEAST_REDIS_HOST": "redis", "REDIS_PASSWORD": SENTINEL}
        backend = feast_job.FeastRepository(repo, repo / "feature_store.yaml", version="test", environ=environ)
        with fake_redis_modules([]), mock.patch.dict(sys.modules, modules), mock.patch.dict(os.environ, {PLANTED_SETTING: "first"}):
            backend.registered_views()
            backend.apply()
            c.check("a repository outside the path and the working directory loads", seen[:2] == ["parse:first:True", "apply:first:True"])
            c.check("Feast's own online store used a database 1 client with 5 s timeouts and zero retries during apply", seen[1].endswith(":True"))
            c.check("the job client is removed from the online store class after apply", job_store._client is None)
            c.check("the working directory is restored after each load", os.getcwd() == before_cwd)
            c.check("the repository folder is on the path once", sys.path.count(str(repo)) == 1)
            os.environ[PLANTED_SETTING] = "second"
            backend.apply()
            backend.registered_views()
            c.check("a second load in the same process reads the new environment", seen[2:] == ["apply:second:True", "parse:second:True"])
        for name in [name for name in sys.modules if name == "definitions"]:
            del sys.modules[name]
        sys.path[:] = before_path


def case_e13_hash(c: Checker) -> None:
    """LC14: the object-list digest ignores order and registry bytes and changes with any object."""
    one: list[dict[str, Any]] = [
        {"kind": "entity", "name": "hospital_window", "join_key": "spine_key"},
        {"kind": "feature_view", "name": "a", "features": [["x", "Float64"]]},
    ]
    reordered = [one[1], one[0]]
    changed = [one[0], {"kind": "feature_view", "name": "a", "features": [["x", "Int64"]]}]
    c.check("the same objects in another order give the same digest", feast_job.object_list_sha256(one) == feast_job.object_list_sha256(reordered))
    c.check("another feature type gives another digest", feast_job.object_list_sha256(one) != feast_job.object_list_sha256(changed))
    c.check("a missing object gives another digest", feast_job.object_list_sha256(one) != feast_job.object_list_sha256(one[:1]))
    schema = synthetic_schema()
    digests = set()
    for _ in range(2):
        catalog, _ = stub_features(schema)
        with tempfile.TemporaryDirectory(prefix="feast_cases_") as raw:
            digests.add(run_apply(catalog, RecordingBackend(schema, []), recording_lease([]), schema, Path(raw))["object_list_sha256"])
    c.check("a repeated apply gives the same object list digest", len(digests) == 1)


# ---------------------------------------------------------------------------------------------------------------------------------------------
# Settings, modes, credentials, the private settings file and static checks (E30).
# ---------------------------------------------------------------------------------------------------------------------------------------------


def case_e30_modes(c: Checker) -> None:
    """LC20 and LC21: mode, namespace and catalog type agree; a fixture holds no AWS profile or Polaris credential and writes no AWS settings."""
    with tempfile.TemporaryDirectory(prefix="feast_cases_") as raw:
        folder = raw
        fixture = {"FEAST_NAMESPACE": "features_e2e", "FEAST_CATALOG_TYPE": "sql", "FEAST_RUN_FOLDER": folder}
        real = {"FEAST_NAMESPACE": "features", "FEAST_CATALOG_TYPE": "rest", "POLARIS_URI": reads.POLARIS_ENDPOINT}
        outside = str(Path(tempfile.gettempdir()) / "elsewhere")
        c.check("a fixture environment is accepted", reads.catalog_settings(fixture, "publish_fixture").catalog_type == "sql")
        c.check("a real environment is accepted", reads.catalog_settings(real, "real", bearer="t").catalog_type == "rest")
        c.refused(
            "a fixture run with the real namespace is refused",
            lambda: reads.catalog_settings({**fixture, "FEAST_NAMESPACE": "features"}, "publish_fixture"),
            "requires namespace",
        )
        c.refused(
            "a fixture run with the REST catalog is refused",
            lambda: reads.catalog_settings({**fixture, "FEAST_CATALOG_TYPE": "rest"}, "publish_fixture"),
            "requires namespace",
        )
        c.refused(
            "a real run with the fixture namespace is refused",
            lambda: reads.catalog_settings({**real, "FEAST_NAMESPACE": "features_e2e"}, "real"),
            "requires namespace",
        )
        c.refused(
            "a real run with the SQL catalog is refused", lambda: reads.catalog_settings({**real, "FEAST_CATALOG_TYPE": "sql"}, "real"), "requires namespace"
        )
        c.refused("an unknown mode is refused", lambda: reads.catalog_settings(fixture, "demo"), "mode is unknown")
        c.refused(
            "a real run with another Polaris endpoint is refused",
            lambda: reads.catalog_settings({**real, "POLARIS_URI": "http://example.invalid/api/catalog"}, "real"),
            "POLARIS_URI",
        )
        for name in ("AWS_PROFILE", "POLARIS_CLIENT_ID", "POLARIS_CLIENT_SECRET", "FEAST_POLARIS_TOKEN"):
            c.refused(f"a fixture run holding {name} is refused", reads.catalog_settings, name, {**fixture, name: "x"}, "publish_fixture")
        c.refused(
            "a SQL catalog file outside the run folder is refused",
            lambda: reads.catalog_settings({**fixture, "FEAST_SQL_CATALOG_URI": "sqlite:////tmp/other/catalog.db"}, "publish_fixture"),
            "inside the run folder",
        )
        c.refused(
            "a SQL catalog path with .. is refused",
            lambda: reads.catalog_settings({**fixture, "FEAST_SQL_CATALOG_URI": f"sqlite:///{folder}/../x.db"}, "publish_fixture"),
            "inside the run folder",
        )
        c.check(
            "the fixture SQL catalog defaults into the run folder",
            reads.catalog_settings(fixture, "publish_fixture").sql_uri == f"sqlite:///{folder}/feast/catalog.db",
        )
        loads: list[dict[str, Any]] = []
        environ = {**fixture, "FEAST_REGISTRY_PATH": f"{folder}/feast/registry.pb", "FEAST_REDIS_HOST": "redis", "REDIS_PASSWORD": SENTINEL}
        settings = feast_job.job_settings(environ, "apply", RUN_A, "publish_fixture")

        def record_loader(name: str, **properties: Any) -> str:
            loads.append({"name": name, **properties})
            return "catalog"

        with feast_job.catalog_session(settings, environ, loader=record_loader, register=False):
            c.check("a fixture session sets no private PyIceberg home", "PYICEBERG_HOME" not in environ)
        c.check(
            "a fixture session opens catalog hai as a SQL catalog without a token or S3 property",
            loads[0]["name"] == "hai" and loads[0]["type"] == "sql" and not any(key in loads[0] for key in ("token", "s3.region", "s3.proxy-uri")),
        )
        c.check(
            "a fixture session sets the variables the definitions read",
            environ["FEAST_SQL_CATALOG_URI"].startswith("sqlite:///") and environ["FEAST_RUN_FOLDER"] == folder,
        )
        c.refused(
            "a fixture run needs a registry file inside the run folder",
            lambda: feast_job.job_settings({**fixture, "FEAST_REGISTRY_PATH": f"{outside}/registry.pb"}, "apply", RUN_A, "publish_fixture"),
            "inside the run folder",
        )
        c.refused(
            "a fixture run with an S3 registry key is refused",
            lambda: feast_job.job_settings({**environ, "FEAST_REGISTRY_KEY": "lakehouse/feast/registry.pb"}, "apply", RUN_A, "publish_fixture"),
            "by file",
        )
        base_real = {
            "FEAST_NAMESPACE": "features",
            "FEAST_CATALOG_TYPE": "rest",
            "POLARIS_URI": reads.POLARIS_ENDPOINT,
            "AWS_PROFILE": "p",
            "AWS_REGION": "us-east-2",
            "POLARIS_CLIENT_ID": "id",
            "POLARIS_CLIENT_SECRET": SENTINEL,
            "FEAST_REGISTRY_KEY": "lakehouse/feast/registry.pb",
            "FEAST_REDIS_HOST": "redis",
            "REDIS_PASSWORD": SENTINEL,
            "FEAST_RUN_FOLDER": folder,
        }
        real_session = dict(base_real)
        deployment = Path(folder) / "deployment.json"
        deployment.write_text(json.dumps({"data_bucket_name": "fake-bucket-name"}))
        real_settings = feast_job.job_settings(base_real, "apply", RUN_A, "real", deployment)
        c.check(
            "a real run builds its registry URI from the key and the bucket field",
            real_settings.registry_path == "s3://fake-bucket-name/lakehouse/feast/registry.pb",
        )
        seen: dict[str, Any] = {}
        issued = "issued-value"

        def real_loader(name: str, **properties: str) -> str:
            seen.update(properties, home=real_session.get("PYICEBERG_HOME", ""), exists=Path(real_session.get("PYICEBERG_HOME", "/nonexistent")).is_dir())
            return "catalog"

        with feast_job.catalog_session(real_settings, real_session, loader=real_loader, exchange=lambda endpoint, client_id, client_secret: issued) as _:
            inside = real_session["PYICEBERG_HOME"]
            c.check("a real session holds a private PyIceberg home while it runs", Path(inside).is_dir())
            c.check(
                "the client secret is gone from the environment after the exchange",
                "POLARIS_CLIENT_SECRET" not in real_session and "POLARIS_CLIENT_ID" not in real_session,
            )
            c.check(
                "the token goes to the catalog and to the Feast variable",
                seen["token"] == issued and real_session["FEAST_POLARIS_TOKEN"] == issued,
            )
            c.check(
                "the REST catalog gets the Polaris endpoint and the warehouse",
                seen["uri"] == reads.POLARIS_ENDPOINT and seen["warehouse"] == reads.WAREHOUSE and seen["type"] == "rest",
            )
        c.check("the private PyIceberg home is removed after the session", not Path(inside).exists() and "PYICEBERG_HOME" not in real_session)
        for name in ("FEAST_REGISTRY_KEY", "AWS_PROFILE", "POLARIS_CLIENT_SECRET"):
            c.refused(
                f"a real run without {name} is refused",
                feast_job.job_settings,
                name,
                {k: v for k, v in base_real.items() if k != name},
                "apply",
                RUN_A,
                "real",
                deployment,
            )
        c.refused(
            "a real run with FEAST_REGISTRY_PATH set is refused",
            lambda: feast_job.job_settings({**base_real, "FEAST_REGISTRY_PATH": "/x"}, "apply", RUN_A, "real", deployment),
            "S3 key only",
        )
        c.refused("a malformed run ID is refused", lambda: feast_job.job_settings(base_real, "apply", "run-1", "real", deployment), "run ID")


def case_e30_registry(c: Checker) -> None:
    """LC22 and LC23: the registry URI comes from a closed key per step and the bucket field."""
    ok = "lakehouse/feast/registry.pb"
    c.check(
        "apply and materialize use the real registry key",
        reads.registry_uri("apply", ok, "my-bucket") == "s3://my-bucket/lakehouse/feast/registry.pb"
        and reads.registry_uri("materialize", ok, "my-bucket").endswith(ok),
    )
    c.check("the probe uses its own key", reads.registry_uri("probe", "lakehouse/feast_e2e/registry.pb", "my-bucket").endswith("feast_e2e/registry.pb"))
    c.refused("the probe key is refused for apply", lambda: reads.registry_uri("apply", "lakehouse/feast_e2e/registry.pb", "my-bucket"), "key")
    c.refused("the real key is refused for the probe", lambda: reads.registry_uri("probe", ok, "my-bucket"), "key")
    for key in ("lakehouse/feast/../gold/registry.pb", "/lakehouse/feast/registry.pb", "lakehouse/feast/registry.pb ", "s3://other/registry.pb", ""):
        c.refused(f"the key {key!r} is refused", reads.registry_uri, "key", "apply", key, "my-bucket")
    c.refused("an unknown step has no key", lambda: reads.registry_uri("delete", ok, "my-bucket"), "key")
    for bucket in ("", "UPPER", "a", "bad bucket", "evil/../x", "a..b"):
        c.refused(f"the bucket {bucket!r} is refused", reads.registry_uri, "bucket", "apply", ok, bucket)
    with tempfile.TemporaryDirectory(prefix="feast_cases_") as raw:
        path = Path(raw) / "deployment.json"
        c.refused("a missing deployment file is refused", lambda: reads.deployment_bucket(path), "deployment file")
        path.write_text("{")
        c.refused("a deployment file that is not JSON is refused", lambda: reads.deployment_bucket(path), "deployment file")
        path.write_text(json.dumps({"other": "x"}))
        c.refused("a deployment file without the bucket field is refused", lambda: reads.deployment_bucket(path), "data_bucket_name")
        path.write_text(json.dumps({"data_bucket_name": "Bad Name"}))
        c.refused("a deployment file with a bad bucket name is refused", lambda: reads.deployment_bucket(path), "data_bucket_name")
        path.write_text(json.dumps({"data_bucket_name": "fake-bucket-name", "aws_profile": SENTINEL}))
        c.check("the bucket field is read and nothing else is returned", reads.deployment_bucket(path) == "fake-bucket-name")


class TokenHandler(http.server.BaseHTTPRequestHandler):
    """A loopback stand-in for the Polaris token endpoint: the case sets the answer on the server."""

    def do_POST(self) -> None:
        server: Any = self.server
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode()
        server.requests.append({"path": self.path, "body": body, "type": self.headers.get("Content-Type")})
        if server.delay:
            time.sleep(server.delay)
        status, payload = server.answer
        data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        try:
            self.send_response(status)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except OSError:
            return

    def log_message(self, fmt: str, *args: Any) -> None:
        del fmt, args


@contextlib.contextmanager
def token_server() -> Iterator[Any]:
    server: Any = http.server.ThreadingHTTPServer(("127.0.0.1", 0), TokenHandler)
    server.requests, server.delay, server.answer = [], 0.0, (200, {"access_token": "issued-token", "token_type": "bearer"})
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def case_e30_token(c: Checker) -> None:
    """LC24: the Polaris token exchange is bounded, names the principal role and never prints the credentials."""
    with token_server() as server:
        endpoint = f"http://127.0.0.1:{server.server_port}/api/catalog"

        def exchange(**options: Any) -> str:
            return reads.polaris_token(endpoint, "client-id-1", SENTINEL, **options)

        c.check("a 200 answer gives the token", exchange() == "issued-token")
        sent = server.requests[0]
        c.check(
            "the request goes to the token path as a form",
            sent["path"] == "/api/catalog/v1/oauth/tokens" and sent["type"] == "application/x-www-form-urlencoded",
        )
        c.check(
            "the form asks for client credentials with all principal roles",
            "grant_type=client_credentials" in sent["body"] and "scope=PRINCIPAL_ROLE%3AALL" in sent["body"],
        )
        for status in (401, 403):
            server.answer = (status, {"error": SENTINEL})
            server.requests.clear()
            message = c.refused(f"HTTP {status} is refused", exchange, f"HTTP {status}")
            c.check(f"HTTP {status} is not tried again", len(server.requests) == 1)
            c.check(
                f"HTTP {status} names the principal role and no secret",
                reads.READER_ROLE in message and SENTINEL not in message and "client-id-1" not in message,
            )
        server.answer = (200, {"token_type": "bearer"})
        c.refused("an answer without a token is refused", exchange, "without a token")
        server.answer = (200, b"not json")
        c.refused("an unreadable answer is refused", exchange, "unreadable")
        server.answer = (200, {"access_token": ""})
        c.refused("an empty token is refused", exchange, "without a token")
        server.answer, server.delay = (200, {"access_token": "late"}), 1.0
        server.requests.clear()
        started = time.monotonic()
        message = c.refused("a slow endpoint is refused", lambda: exchange(timeout=0.2, attempts=2), "2 attempts")
        elapsed = time.monotonic() - started
        c.check("the wait is bounded by attempts times the timeout", elapsed < 1.5)
        c.check("the refusal names the principal role and no secret", reads.READER_ROLE in message and SENTINEL not in message)
        c.check("both attempts reached the server", len(server.requests) == 2)
    c.refused(
        "a refused connection is refused after the attempts",
        lambda: reads.polaris_token("http://127.0.0.1:9/api/catalog", "id", SENTINEL, timeout=0.2),
        "2 attempts",
    )
    c.refused("a non-http endpoint is refused", lambda: reads.polaris_token("file:///etc/passwd", "id", SENTINEL), "http URL")


def case_e30_pyiceberg(c: Checker) -> None:
    """LC25: the private settings file holds no secret, is private, is removed, and is refused when it would be ignored."""
    text = reads.pyiceberg_settings_text("us-east-2")
    c.check(
        "the file names catalog hai with the four S3 settings",
        all(
            item in text
            for item in ("  hai:", "s3.region: us-east-2", "s3.proxy-uri: http://hai-egress:3128", "s3.connect-timeout: 5", "s3.request-timeout: 30")
        ),
    )
    c.check("the file holds no secret-like key", not re.search(r"token|secret|password|credential|key", text, re.IGNORECASE))
    for region in ("", "us east", "US-EAST-2", "us-east-2\ntoken: x", "../x"):
        c.refused(f"the region {region!r} is refused", reads.pyiceberg_settings_text, "region", region)
    before = set(Path(tempfile.gettempdir()).glob("pyiceberg_home_*"))
    environ: dict[str, str] = {"PYICEBERG_HOME": "/previous"}
    with reads.private_pyiceberg_home("us-east-2", environ) as folder:
        settings = folder / ".pyiceberg.yaml"
        c.check("the folder is private", stat.S_IMODE(folder.stat().st_mode) == 0o700)
        c.check("the file is private", stat.S_IMODE(settings.stat().st_mode) == 0o600)
        c.check("PYICEBERG_HOME points at the folder", environ["PYICEBERG_HOME"] == str(folder))
        c.check("the file is the settings text", settings.read_text() == text)
    c.check("the folder is removed and the earlier PYICEBERG_HOME restored", not folder.exists() and environ["PYICEBERG_HOME"] == "/previous")
    environ.pop("PYICEBERG_HOME")
    try:
        with reads.private_pyiceberg_home("us-east-2", environ) as folder:
            raise RuntimeError("the session failed")
    except RuntimeError:
        pass
    c.check("the folder is removed after an error", not folder.exists() and "PYICEBERG_HOME" not in environ)
    with mock.patch.dict(sys.modules, {"pyiceberg.catalog": fake_module("pyiceberg.catalog")}):
        c.refused(
            "a settings file after PyIceberg loaded its configuration is refused",
            lambda: reads.private_pyiceberg_home("us-east-2", environ).__enter__(),
            "already loaded",
        )
    c.check("a refused start leaves no folder behind", set(Path(tempfile.gettempdir()).glob("pyiceberg_home_*")) <= before)


def forbidden_uses(source: str) -> list[str]:
    """Uses of the banned store-removal command in Python source: a name, attribute, import, definition or string (docstrings excluded)."""
    tree = ast.parse(source)
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef)
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
    }
    found = []
    for node in ast.walk(tree):
        names: list[str] = []
        if isinstance(node, ast.Attribute):
            names = [node.attr]
        elif isinstance(node, ast.Name):
            names = [node.id]
        elif isinstance(node, ast.alias):
            names = [node.name, node.asname or ""]
        elif isinstance(node, ast.FunctionDef | ast.ClassDef):
            names = [node.name]
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings:
            names = [node.value]
        if any(BANNED in name.lower() for name in names):
            found.append(f"line {getattr(node, 'lineno', 0)}")
    return found


def package_sources() -> list[Path]:
    """Every Python source of the feature store package that runs in the Feast image, except the cases that name the banned command."""
    own = {"feast_cases.py", "feast_image_cases.py", "run_feature_store_e2e.py"}
    sources = [path for path in sorted((ROOT / "scripts/features").glob("*.py")) if path.name not in own]
    return sources + sorted((ROOT / "feature_store").glob("*.py"))


def case_e30_removal(c: Checker) -> None:
    """LC16 (B7): no module of the package calls, imports, names or spells the banned store-removal command."""
    sources = package_sources()
    c.check("the package sources were found", any(path.name == "feast_job.py" for path in sources))
    findings = {path.name: forbidden_uses(path.read_text()) for path in sources}
    c.check("no module uses the banned command", not any(findings.values()))
    method, spelled, imported, shell = (
        f"store.{BANNED}()",
        f"subprocess.run(['feast', '{BANNED}'])",
        f"from feast.repo_operations import {BANNED}",
        f"os.system('feast {BANNED} -y')",
    )
    c.check("a method call is detected", bool(forbidden_uses(method)))
    c.check("a command spelled in a list is detected", bool(forbidden_uses(spelled)))
    c.check("an import is detected", bool(forbidden_uses(imported)))
    c.check("a shell string is detected", bool(forbidden_uses(shell)))
    c.check("a docstring is not a use", forbidden_uses(f'"""never call {BANNED}"""\nx = 1') == [])
    c.check("a getattr by name is detected", bool(forbidden_uses(f"getattr(store, '{BANNED}')")))


def case_e30_deadline(c: Checker) -> None:
    """LC18: the whole-job deadline is checked between phases and fails visibly."""
    now = [0.0]
    check = reads.monotonic_deadline(1800, clock=lambda: now[0])
    now[0] = 1799
    check("apply")
    c.check("a phase inside the deadline starts", True)
    now[0] = 1801
    message = c.refused("a phase after the deadline is refused", lambda: check("the registry read-back"), "1800 s deadline")
    c.check("the refusal names the phase", "the registry read-back" in message)
    schema = synthetic_schema()
    catalog, _ = stub_features(schema)
    log: list[str] = []
    expired = reads.monotonic_deadline(1800, clock=iter([0.0] + [5000.0] * 20).__next__)
    c.refused(
        "apply stops at the first phase boundary after the deadline",
        lambda: feast_job.apply(
            catalog,
            fixture_settings(Path("/nonexistent")),
            schema=schema,
            schema_sha256="c" * 64,
            backend=RecordingBackend(schema, log),
            lease=recording_lease(log),
            deadline=expired,
        ),
        "deadline",
    )
    c.check("nothing was applied after the deadline", "apply" not in log)
    phases: list[str] = []
    catalog, _ = stub_features(schema)
    feast_job.apply(
        catalog,
        fixture_settings(Path("/nonexistent")),
        schema=schema,
        schema_sha256="c" * 64,
        backend=RecordingBackend(schema, []),
        lease=recording_lease([]),
        deadline=phases.append,
    )
    c.check(
        "every phase boundary checks the deadline",
        phases == ["selecting the publication", "the pre-apply comparison", "apply", "the registry read-back"],
    )
    for stop in range(len(phases)):
        calls: list[str] = []
        applied: list[str] = []

        def stopper(phase: str, stop: int = stop, calls: list[str] = calls) -> None:
            calls.append(phase)
            if len(calls) > stop:
                raise ReadError(f"the job passed its deadline before {phase}")

        catalog, _ = stub_features(schema)
        c.refused(
            f"a deadline that expires at boundary {stop + 1} stops the job there",
            feast_job.apply,
            "deadline",
            catalog,
            fixture_settings(Path("/nonexistent")),
            schema=schema,
            schema_sha256="c" * 64,
            backend=RecordingBackend(schema, applied),
            lease=recording_lease(applied),
            deadline=stopper,
        )
        c.check(f"boundary {stop + 1}: no phase after it started", len(calls) == stop + 1 and ("apply" not in applied or stop >= 3))
    c.check("the job deadline constant is 1800 s, below the template's 3600 s", feast_job.JOB_SECONDS == 1800)


def case_e30_record(c: Checker) -> None:
    """LC15: the record and the refusal messages carry no secret or row value even when the environment holds sentinels."""
    schema = synthetic_schema()
    catalog, _ = stub_features(schema)
    catalog.tables[reads.SPINE_TABLE].rows[0]["spine_key"] = SENTINEL
    with tempfile.TemporaryDirectory(prefix="feast_cases_") as raw:
        folder = Path(raw)
        os.environ[ONLINE_STORE_AUTH_VARIABLE] = SENTINEL
        try:
            record = run_apply(catalog, RecordingBackend(schema, []), recording_lease([]), schema, folder)
            feast_job.write_record(folder / feast_job.RECORD_NAME, record)
        finally:
            os.environ.pop(ONLINE_STORE_AUTH_VARIABLE, None)
        text = (folder / feast_job.RECORD_NAME).read_text()
        c.check("the record holds no sentinel", SENTINEL not in text)
        c.check("the record is JSON with sorted keys and a final newline", json.loads(text) == record and text.endswith("\n"))
        c.check("no temporary file is left by the atomic write", [path.name for path in folder.iterdir()] == [feast_job.RECORD_NAME])
        catalog.tables["feature_matrix_window"].main = 5
        message = c.refused(
            "a moved table is refused", lambda: run_apply(catalog, RecordingBackend(schema, []), recording_lease([]), schema, folder), "before apply"
        )
        c.check("the refusal holds no sentinel", SENTINEL not in message)


def case_e30_yaml(c: Checker) -> None:
    """LC26 (D13 and CR1): the committed settings file names variables only, database 1, 5 s timeouts, no key expiry and dedup off."""
    text = feast_job.SETTINGS_FILE.read_text()
    wanted = (
        "provider: local",
        "type: duckdb",
        "type: redis",
        "redis_type: redis",
        "path: ${FEAST_REGISTRY_PATH}",
        "cache_ttl_seconds: 60",
        "skip_dedup: true",
        "entity_key_serialization_version: 3",
        "project: hai_prediction",
    )
    c.check("the expected settings are present", all(item in text for item in wanted))
    string = next((line for line in text.splitlines() if "connection_string" in line), "")
    c.check(
        "the connection string uses database 1 with 5 s timeouts",
        "db=1," in string and "socket_timeout=5" in string and "socket_connect_timeout=5" in string and string.rstrip().endswith("socket_connect_timeout=5"),
    )
    c.check("the host and the password are variables", "${FEAST_REDIS_HOST}:6379" in string and "password=${REDIS_PASSWORD}" in string)
    c.check("no key expiry is configured", "key_ttl_seconds" not in text)
    c.check(
        "only the three expected variables appear",
        {first or second for first, second in reads.VARIABLE.findall(text)} == {"FEAST_REGISTRY_PATH", "FEAST_REDIS_HOST", "REDIS_PASSWORD"},
    )
    c.check("database 0 is never named", not re.search(r"db=0|\bdb: 0\b", text))
    c.check("no AWS key, token or literal password is in the file", not re.search(r"AKIA|aws_access|secret|token|password=[^$]", text, re.IGNORECASE))


def case_e30_harness(c: Checker) -> None:
    """The report keeps the full traceback of an unexpected exception and names the innermost project frame, not an import-system frame."""

    def broken(checker: Checker) -> None:
        importlib.import_module("a_module_that_does_not_exist")

    quiet = sys.stderr
    with tempfile.TemporaryFile("w+") as sink:
        sys.stderr = sink
        try:
            report = run_cases({"broken": (broken, "X1")}, [])
        finally:
            sys.stderr = quiet
    item = report["cases"][0]
    c.check("the case fails", item["verdict"] == "fail")
    c.check(
        "the traceback is in the report",
        "Traceback (most recent call last)" in item.get("traceback", "") and "a_module_that_does_not_exist" in item["traceback"],
    )
    c.check("the failed check names the project frame and the error type", "ModuleNotFoundError at feast_cases.py:" in item["failed"][0])
    c.check("a passing case carries no traceback", "traceback" not in run_cases({"fine": (lambda checker: checker.check("ok", True), "X1")}, [])["cases"][0])


def case_e30_cli(c: Checker) -> None:
    """LC27 and LC28: the command line matches lane B's render; unbuilt steps exit 2; a refusal exits 1 with the cause and no traceback."""
    args = feast_job.parse(["apply", "--run", RUN_A, "--mode", "publish_fixture"])
    c.check("the render's shape parses", args.step == "apply" and args.run == RUN_A and args.mode == "publish_fixture")
    for step in ("materialize", "probe"):
        c.check(f"{step} is accepted by the parser", feast_job.parse([step, "--run", RUN_A, "--mode", "real"]).step == step)
    quiet = sys.stderr
    with tempfile.TemporaryFile("w+") as sink:
        sys.stderr = sink
        try:
            feast_job.parse(["delete", "--run", RUN_A, "--mode", "real"])
            parsed = True
        except SystemExit:
            parsed = False
        finally:
            sys.stderr = quiet
    c.check("an unknown step is refused by the parser", not parsed)
    original = sys.stderr
    with tempfile.TemporaryFile("w+") as captured:
        sys.stderr = captured
        try:
            unbuilt = feast_job.main(["probe", "--run", RUN_A, "--mode", "real"], {})
            refused = feast_job.main(["apply", "--run", "run-1", "--mode", "real"], {})
            refused_fixture = feast_job.main(["apply", "--run", RUN_A, "--mode", "publish_fixture"], {})
        finally:
            sys.stderr = original
        captured.seek(0)
        output = captured.read()
    c.check("an unbuilt step exits 2", unbuilt == 2 and "later step" in output)
    c.check("a malformed run ID exits 1 with the cause", refused == 1 and "run ID is malformed" in output)
    c.check("a fixture run without a registry file exits 1", refused_fixture == 1 and "inside the run folder" in output)
    c.check("no traceback is printed", "Traceback" not in output)


# ---------------------------------------------------------------------------------------------------------------------------------------------
# Fixture catalog registration (code review 1, finding 2): the Hadoop-published tables reach the SQL catalog of the Feast reader.
# ---------------------------------------------------------------------------------------------------------------------------------------------


def write_hadoop_table(
    warehouse: Path, namespace: str, name: str, versions: Sequence[int | None], *, hint: int | None = None, stub: dict[str, Any] | None = None
) -> Path:
    """A table in the shape the Spark Hadoop catalog writes: ``v<N>.metadata.json`` (refs and snapshots) and ``version-hint.text``.

    ``versions`` gives the ``main`` snapshot of each version (None: no ``main``). ``stub`` is test-only data (columns and rows) the stub reads back.
    """
    folder = warehouse / namespace / name / "metadata"
    folder.mkdir(parents=True, exist_ok=True)
    for number, main in enumerate(versions, start=1):
        document: dict[str, Any] = {"format-version": 2, "snapshots": [], "refs": {}}
        if main is not None:
            document["snapshots"] = [{"snapshot-id": main, "summary": {"total-records": "1"}}]
            document["refs"] = {"main": {"snapshot-id": main, "type": "branch"}}
        (folder / f"v{number}.metadata.json").write_text(json.dumps(document))
    (folder / "version-hint.text").write_text(str(len(versions) if hint is None else hint))
    if stub is not None:
        (folder / "stub.json").write_text(json.dumps(stub, default=lambda value: value.isoformat() if hasattr(value, "isoformat") else str(value)))
    return folder


class RegisteredTable(StubTable):
    def __init__(self, name: str, main: int | None, columns: dict[str, str], rows: list[dict[str, Any]], location: str) -> None:
        super().__init__(name, main, columns, rows)
        self.metadata_location = location


class RegisteringStub(StubCatalog):
    """A stub catalog that can register Hadoop metadata: it reads the ``main`` snapshot from the file and the test-only ``stub.json`` next to it."""

    def __init__(self, namespace: str = "features_e2e", log: list[str] | None = None) -> None:
        super().__init__(namespace, log)
        self.operations: list[str] = []

    def create_namespace_if_not_exists(self, namespace: str) -> None:
        self.operations.append(f"namespace:{namespace}")

    def table_exists(self, identifier: str) -> bool:
        return identifier.partition(".")[2] in self.tables

    def drop_table(self, identifier: str) -> None:
        self.operations.append(f"drop:{identifier}")
        del self.tables[identifier.partition(".")[2]]

    def register_table(self, identifier: str, metadata_location: str) -> None:
        self.operations.append(f"register:{identifier}")
        path = Path(metadata_location.removeprefix("file://"))
        main = fixture_catalog.main_snapshot(path)
        side = json.loads((path.parent / "stub.json").read_text()) if (path.parent / "stub.json").is_file() else {}
        rows = [{key: (datetime.fromisoformat(value) if key == "committed_at" else value) for key, value in row.items()} for row in side.get("rows", [])]
        name = identifier.partition(".")[2]
        self.tables[name] = RegisteredTable(name, main, side.get("columns", {}), rows, metadata_location)


def registered_tables(catalog: RegisteringStub) -> dict[str, Any]:
    return dict(catalog.tables)


def published_warehouse(
    folder: Path, schema: dict[str, Any], runs: list[tuple[str, datetime, dict[str, int]]], tables: Mapping[str, Sequence[int | None]]
) -> Path:
    """A warehouse with the given tables (versions of ``main``), the entity table, and ``publish_runs`` holding the given rows."""
    warehouse = folder / "warehouse"
    columns = iceberg_view(schema)
    for name, versions in tables.items():
        write_hadoop_table(warehouse, "features_e2e", name, versions, stub={"columns": columns.get(name, {}), "rows": []})
    rows = [runs_row(run, committed, json.dumps(snapshots)) for run, committed, snapshots in runs]
    write_hadoop_table(warehouse, "features_e2e", reads.RUNS_TABLE, [1000], stub={"columns": {}, "rows": rows})
    return warehouse


def case_e11_register(c: Checker) -> None:
    """Finding 2: the committed metadata of the local warehouse is registered in the fixture SQL catalog, exactly as the selected publication names it."""
    schema = synthetic_schema()
    names = sorted(schema["tables"])
    with tempfile.TemporaryDirectory(prefix="feast_cases_") as raw:
        folder = Path(raw)
        published = {name: 100 + number for number, name in enumerate(names)}
        tables = {name: [snapshot - 10, snapshot] for name, snapshot in published.items()}
        warehouse = published_warehouse(folder, schema, [(RUN_A, T0, published)], tables)
        catalog = RegisteringStub()
        publication = fixture_catalog.register_publication(catalog, warehouse, "features_e2e")
        c.check("the head publication is selected through the catalog", publication.run_id == RUN_A and dict(publication.snapshot_set) == published)
        c.check(
            "every table of the publication is registered at its published metadata",
            all(registered_tables(catalog)[name].metadata_location.endswith(f"{name}/metadata/v2.metadata.json") for name in names),
        )
        reads.verify_heads(catalog, publication, "after registering")
        c.check("the reader path then verifies the heads", True)
        before = list(catalog.operations)
        fixture_catalog.register_publication(catalog, warehouse, "features_e2e")
        c.check(
            "a second registration of the same publication changes nothing",
            [item for item in catalog.operations[len(before) :] if not item.startswith("namespace")] == [],
        )
        older = {name: snapshot - 10 for name, snapshot in published.items()}
        rows = [(RUN_A, T0, older), (RUN_B, T0 + timedelta(hours=1), published)]
        warehouse = published_warehouse(folder / "again", schema, rows, tables)
        catalog = RegisteringStub()
        c.check("the head is the newest publication", fixture_catalog.register_publication(catalog, warehouse, "features_e2e").run_id == RUN_B)
        named = fixture_catalog.register_publication(catalog, warehouse, "features_e2e", RUN_A)
        c.check(
            "a named earlier publication replaces the entries with the older metadata files",
            named.run_id == RUN_A and all(registered_tables(catalog)[name].metadata_location.endswith("v1.metadata.json") for name in names),
        )
        c.check(
            "the replaced entries were dropped before they were registered again",
            sum(1 for item in catalog.operations if item.startswith("drop:")) == len(names),
        )
        stuck = dict(tables)
        stuck[names[0]] = [published[names[0]] - 10, published[names[0]] - 5]
        warehouse = published_warehouse(folder / "stuck", schema, [(RUN_A, T0, published)], stuck)
        message = c.refused(
            "a table whose main never reached the published snapshot is refused by name",
            fixture_catalog.register_publication,
            "no metadata file",
            RegisteringStub(),
            warehouse,
            "features_e2e",
        )
        c.check("the refusal names the table and the snapshot", names[0] in message and str(published[names[0]]) in message)
        listed = folder / "unlisted"
        write_hadoop_table(listed, "features_e2e", "t", [None, 5])
        broken = listed / "features_e2e" / "t" / "metadata" / "v2.metadata.json"
        broken.write_text(json.dumps({"refs": {"main": {"snapshot-id": 5}}, "snapshots": [{"snapshot-id": 6}]}))
        c.refused(
            "a metadata file whose main snapshot is not listed in it is not used",
            fixture_catalog.metadata_for,
            "no metadata file",
            listed,
            "features_e2e",
            "t",
            5,
        )
        c.refused(
            "a missing runs table is refused",
            fixture_catalog.register_publication,
            "no metadata file",
            RegisteringStub(),
            folder / "none",
            "features_e2e",
        )
        for label, versions_, hint, expected in (
            ("a stale hint (committed v2, hint 1)", [10, 20], "1", 2),
            ("a stale hint several versions behind", [10, 20, 30], "1", 3),
            ("a current hint", [10, 20], "2", 2),
            ("a missing hint", [10, 20], None, 2),
            ("an unreadable hint", [10, 20], "x", 2),
            ("a hint that names a missing file", [10, 20], "9", 2),
        ):
            table = folder / "versions" / label.split()[1]
            write_hadoop_table(table, "n", "t", versions_, hint=1)
            hint_file = table / "n" / "t" / "metadata" / "version-hint.text"
            if hint is None:
                hint_file.unlink()
            else:
                hint_file.write_text(hint)
            found = fixture_catalog.current_version(table / "n" / "t" / "metadata")
            c.check(f"{label}: the current version is {expected}", found == expected)
        gap = folder / "versions" / "gap"
        write_hadoop_table(gap, "n", "t", [10, 20, 30], hint=1)
        (gap / "n" / "t" / "metadata" / "v2.metadata.json").unlink()
        c.check("the search advances only while the next version file exists", fixture_catalog.current_version(gap / "n" / "t" / "metadata") == 1)
        c.refused(
            "a table with no metadata file at all is refused",
            fixture_catalog.current_version,
            "no metadata file",
            folder / "versions" / "empty" / "n" / "t" / "metadata",
        )
        stale = published_warehouse(folder / "stale", schema, [(RUN_A, T0, published)], tables)
        runs_folder = stale / "features_e2e" / reads.RUNS_TABLE / "metadata"
        shutil.copy(runs_folder / "v1.metadata.json", runs_folder / "v2.metadata.json")
        (runs_folder / "version-hint.text").write_text("1")
        stale_catalog = RegisteringStub()
        fixture_catalog.register_publication(stale_catalog, stale, "features_e2e")
        c.check(
            "a stale hint on publish_runs registers the committed version",
            registered_tables(stale_catalog)[reads.RUNS_TABLE].metadata_location.endswith("publish_runs/metadata/v2.metadata.json"),
        )
        (runs_folder / "version-hint.text").unlink()
        for name in names:
            (stale / "features_e2e" / name / "metadata" / "version-hint.text").unlink()
        missing_catalog = RegisteringStub()
        c.check(
            "a warehouse without any version hint is still registered",
            fixture_catalog.register_publication(missing_catalog, stale, "features_e2e").run_id == RUN_A
            and all(name in missing_catalog.tables for name in names),
        )
        c.refused("a namespace that is not an identifier is refused", fixture_catalog.table_folder, "plain identifier", folder, "../x", "t")
        c.check("a file: warehouse URI becomes a folder", fixture_catalog.warehouse_folder("file:///a/b") == Path("/a/b"))
        c.refused("another warehouse scheme is refused", fixture_catalog.warehouse_folder, "file: URI", "s3://bucket/warehouse")
        warehouse = published_warehouse(folder / "job", schema, [(RUN_A, T0, published)], tables)
        environ = fixture_environment(folder / "job")
        environ["FEAST_SQL_WAREHOUSE"] = f"file://{warehouse}"
        settings = feast_job.job_settings(environ, "apply", RUN_A, "publish_fixture")
        log: list[str] = []
        record = feast_job.run(
            settings,
            environ,
            backend=RecordingBackend(schema, log),
            lease=recording_lease(log),
            schema=(schema, "c" * 64),
            loader=lambda name, **properties: RegisteringStub(),
        )
        c.check(
            "the job finds the committed set through the registration (apply ran on the published tables)",
            "apply" in log and record["publication"]["run_id"] == RUN_A,
        )
        patched = RegisteringStub()
        with mock.patch.object(reads, "open_catalog", lambda settings, loader=None: patched):
            environ = fixture_environment(folder / "job")
            environ["FEAST_SQL_WAREHOUSE"] = f"file://{warehouse}"
            environ["FEAST_NAMESPACE"] = "features_e2e"
            historical.default_catalog(environ)
            c.check(
                "the historical helper registers the head publication before it reads",
                all(name in patched.tables for name in names) and reads.RUNS_TABLE in patched.tables,
            )
            older = {name: snapshot - 10 for name, snapshot in published.items()}
            warehouse_two = published_warehouse(folder / "history", schema, [(RUN_A, T0, older), (RUN_B, T0 + timedelta(hours=1), published)], tables)
            environ["FEAST_SQL_WAREHOUSE"] = f"file://{warehouse_two}"
            historical.default_catalog(environ, RUN_A)
            c.check(
                "a named publication registers its own metadata files",
                all(registered_tables(patched)[name].metadata_location.endswith("v1.metadata.json") for name in names),
            )
            historical.default_catalog(environ, RUN_A, register=False)
            c.check(
                "register=False leaves the catalog as it is",
                all(registered_tables(patched)[name].metadata_location.endswith("v1.metadata.json") for name in names),
            )
        c.refused(
            "without the warehouse tables the job refuses instead of finding nothing",
            feast_job.run,
            "no metadata file",
            settings,
            fixture_environment(folder / "empty"),
            backend=RecordingBackend(schema, []),
            lease=recording_lease([]),
            schema=(schema, "c" * 64),
            loader=lambda name, **properties: RegisteringStub(),
        )


# ---------------------------------------------------------------------------------------------------------------------------------------------
# Historical helper (E12, E15, E17 with Feast replaced by a stand-in).
# ---------------------------------------------------------------------------------------------------------------------------------------------


class FakeFrame:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def sort_by(self, key: str) -> FakeFrame:
        return FakeFrame(sorted(self.rows, key=lambda row: row[key]))


class FakeJob:
    def __init__(self, store: FakeStore, entity_df: Any, features: list[str]) -> None:
        self.store, self.entity_df, self.features = store, entity_df, features

    def to_arrow(self) -> FakeFrame:
        if self.store.during_read is not None:
            self.store.during_read()
        keys, times = self.entity_df["spine_key"], self.entity_df["event_timestamp"]
        return FakeFrame(
            [{"spine_key": key, "event_timestamp": moment, **{name: 1.5 for name in self.features}} for key, moment in zip(keys, times, strict=True)][::-1]
        )


class FakeStore:
    def __init__(self, during_read: Callable[[], None] | None = None) -> None:
        self.calls: list[tuple[Any, list[str]]] = []
        self.during_read = during_read

    def get_historical_features(self, entity_df: Any, features: list[str]) -> FakeJob:
        self.calls.append((entity_df, features))
        return FakeJob(self, entity_df, features)


@contextlib.contextmanager
def fake_pandas() -> Iterator[None]:
    """Stand in for pandas (not in the host environment): a frame is its dict of columns, and times stay as they are."""

    def to_datetime(values: Any, utc: bool) -> list[Any]:
        del utc
        return list(values)

    with mock.patch.dict(sys.modules, {"pandas": fake_module("pandas", DataFrame=dict, to_datetime=to_datetime)}):
        yield


def write_schema(folder: Path, schema: dict[str, Any]) -> Path:
    path = folder / "feature_schema.json"
    path.write_text(json.dumps(schema, indent=2, sort_keys=True) + "\n")
    return path


def history(folder: Path, catalog: StubCatalog, store: FakeStore, features: Sequence[str], **options: Any) -> historical.HistoricalRead:
    return historical.historical_features(
        features, environ={"FEAST_NAMESPACE": "features_e2e"}, catalog=catalog, store=store, schema_path=write_schema(folder, synthetic_schema()), **options
    )


def case_e12_utc(c: Checker) -> None:
    """LH4 (B3): the event time is the window start at 00:00 UTC, timezone aware, whatever the process zone is."""
    expected = datetime(2022, 1, 1, tzinfo=UTC)
    original = os.environ.get(ZONE_VARIABLE)
    try:
        for zone in ("UTC", "America/Los_Angeles", "Asia/Tokyo"):
            os.environ[ZONE_VARIABLE] = zone
            time.tzset()
            c.check(f"a date is midnight UTC in {zone}", historical.window_start_utc(date(2022, 1, 1)) == expected)
            c.check(f"a naive midnight is read as UTC in {zone}", historical.window_start_utc(datetime(2022, 1, 1)) == expected)
            c.check(
                f"an aware midnight from another zone keeps its instant in {zone}",
                historical.window_start_utc(datetime(2022, 1, 1, 9, tzinfo=_plus(9))) == expected,
            )
            c.check(f"the result is timezone aware in {zone}", historical.window_start_utc(date(2022, 1, 1)).tzinfo is not None)
    finally:
        if original is None:
            os.environ.pop(ZONE_VARIABLE, None)
        else:
            os.environ[ZONE_VARIABLE] = original
        time.tzset()
    c.refused("a time of day is refused", lambda: historical.window_start_utc(datetime(2022, 1, 1, 12, tzinfo=UTC)), "00:00 UTC")
    c.refused("local midnight of another zone is refused", lambda: historical.window_start_utc(datetime(2022, 1, 1, tzinfo=_plus(9))), "00:00 UTC")
    c.refused("text is refused", lambda: historical.window_start_utc("2022-01-01"), "not a date")


def case_e15_spine(c: Checker) -> None:
    """LH2: a null or repeated key and a null window start in the entity table are refused with counts."""
    clean = [{"spine_key": "A:2021", "window_start": date(2021, 1, 1)}, {"spine_key": "B:2021", "window_start": date(2021, 1, 1)}]
    c.check("a clean spine gives one window per key", len(historical.spine_windows(clean)) == 2)
    c.refused("a null key is refused", lambda: historical.spine_windows([*clean, {"spine_key": None, "window_start": date(2021, 1, 1)}]), "1 null keys")
    c.refused("an empty key is refused", lambda: historical.spine_windows([*clean, {"spine_key": "", "window_start": date(2021, 1, 1)}]), "1 null keys")
    c.refused("a repeated key is refused", lambda: historical.spine_windows([*clean, clean[0]]), "1 repeated keys")
    c.refused(
        "a null window start is refused", lambda: historical.spine_windows([*clean, {"spine_key": "C:2021", "window_start": None}]), "1 null window starts"
    )
    c.refused("an empty spine is refused", lambda: historical.spine_windows([]), "no rows")


def case_e15_entities(c: Checker) -> None:
    """LH1: an entity time other than its key's own window start, a naive time, an unknown or a repeated key is refused before any Feast call."""
    windows = {"AAA001:2021": datetime(2021, 1, 1, tzinfo=UTC), "AAA001:2022": datetime(2022, 1, 1, tzinfo=UTC)}
    c.check("no entities means every spine key, sorted", historical.entity_rows(windows, None) == sorted(windows.items()))
    own = datetime(2021, 1, 1, tzinfo=UTC)
    c.check("a key with its own window start is accepted", historical.entity_rows(windows, [("AAA001:2021", own)]) == [("AAA001:2021", own)])
    c.check(
        "the same instant in another zone is accepted",
        historical.entity_rows(windows, [("AAA001:2021", datetime(2021, 1, 1, 9, tzinfo=_plus(9)))]) == [("AAA001:2021", own)],
    )
    c.refused(
        "a later time is refused", lambda: historical.entity_rows(windows, [("AAA001:2021", datetime(2022, 1, 1, tzinfo=UTC))]), "not its own window start"
    )
    c.refused("one second later is refused", lambda: historical.entity_rows(windows, [("AAA001:2021", own + timedelta(seconds=1))]), "not its own window start")
    c.refused("an earlier time is refused", lambda: historical.entity_rows(windows, [("AAA001:2021", own - timedelta(days=1))]), "not its own window start")
    c.refused("a naive time is refused", lambda: historical.entity_rows(windows, [("AAA001:2021", datetime(2021, 1, 1))]), "naive")
    with_date: list[tuple[str, Any]] = [("AAA001:2021", date(2021, 1, 1))]
    c.refused("a date is refused", lambda: historical.entity_rows(windows, with_date), "naive or missing")
    c.refused("an unknown key is refused", lambda: historical.entity_rows(windows, [("ZZZ999:2021", own)]), "not in int_hospital_spine")
    c.refused("a repeated key is refused", lambda: historical.entity_rows(windows, [("AAA001:2021", own), ("AAA001:2021", own)]), "repeated")
    c.refused("an empty list is refused", lambda: historical.entity_rows(windows, []), "no entity keys")
    with tempfile.TemporaryDirectory(prefix="feast_cases_") as raw, fake_pandas():
        catalog, _ = stub_features()
        store = FakeStore()
        c.refused(
            "the helper refuses a wrong time before any Feast call",
            lambda: history(Path(raw), catalog, store, ["feature_matrix_window:bed_count"], entities=[("AAA001:2021", datetime(2022, 1, 1, tzinfo=UTC))]),
            "own window start",
        )
        c.check("Feast was not called", store.calls == [])


def case_e15_refs(c: Checker) -> None:
    """LH3 and LH8: a reference must be a registered predictor or label; identifiers, excluded controls, typos and repeats are refused."""
    schema = synthetic_schema()
    good = ["feature_matrix_window:bed_count", "feature_matrix_measures:c001__value__number", "label_hai_1:hai_1__sir__number"]
    c.check("predictors and labels are accepted", historical.checked_references(schema, good) == tuple(good))
    for reference in (
        "int_spine_validation_measures:value_number",
        "int_county_adjacency:shared_border_length_m",
        "feature_matrix_window:ccn",
        "feature_matrix_window:spine_key",
        "feature_matrix_window:event_timestamp",
        "feature_matrix_window:bedcount",
        "feature_matrix_measures:bed_count",
        "label_matrix_hai_outcomes:hai_1__sir__number",
        "bed_count",
        "feature_matrix_measures:c269__value__number",
    ):
        c.refused(f"{reference} is refused", historical.checked_references, "not registered", schema, [reference])
    c.refused("a repeat is refused", lambda: historical.checked_references(schema, [good[0], good[0]]), "twice")
    c.refused("an empty request is refused", lambda: historical.checked_references(schema, []), "no feature references")
    shared = synthetic_schema()
    shared["tables"]["feature_matrix_measures"]["columns"].append(
        {"name": "bed_count", "role": "predictor", "type": "bigint", "view": "feature_matrix_measures", "feast_type": "Int64"}
    )
    c.refused(
        "two references with one column name are refused",
        lambda: historical.checked_references(shared, ["feature_matrix_window:bed_count", "feature_matrix_measures:bed_count"]),
        "share one column name",
    )


def case_e17_history(c: Checker) -> None:
    """LC5 and LH7 through the helper: sorted result, entity table read at R, R and the schema digest returned, a moved table refused."""
    with tempfile.TemporaryDirectory(prefix="feast_cases_") as raw, fake_pandas():
        folder = Path(raw)
        catalog, snapshots = stub_features()
        store = FakeStore()
        features = ["feature_matrix_window:bed_count", "feature_matrix_measures:c001__value__number", "label_hai_1:hai_1__sir__number"]
        result = history(folder, catalog, store, features)
        c.check("the result is sorted by key", [row["spine_key"] for row in result.frame.rows] == ["AAA001:2021", "AAA001:2022", "BBB002:2022"])
        c.check("the entity count is the spine", result.entity_count == 3)
        c.check("the result names the publication and the schema digest", result.publication.run_id == RUN_A and len(result.feature_schema_sha256) == 64)
        c.check("the entity table was read at the publication's snapshot", catalog.tables[reads.SPINE_TABLE].scans == [snapshots[reads.SPINE_TABLE]])
        times = store.calls[0][0]["event_timestamp"]
        c.check(
            "every event time is a timezone-aware midnight UTC",
            all(moment.tzinfo is not None and moment.utcoffset() == timedelta(0) and moment.hour == 0 for moment in times),
        )
        c.check(
            "the event times are the window starts",
            times == [datetime(2021, 1, 1, tzinfo=UTC), datetime(2022, 1, 1, tzinfo=UTC), datetime(2022, 1, 1, tzinfo=UTC)],
        )
        c.check("the label reference needs the label table in the publication", True)
        moved = FakeStore(during_read=lambda: setattr(catalog.tables["feature_matrix_window"], "main", 31337))
        outcome: list[Any] = []
        message = c.refused(
            "a commit between fast-forwards during the read refuses the result",
            lambda: outcome.append(history(folder, catalog, moved, features)),
            "after the read",
        )
        c.check("no result reaches the caller", outcome == [] and "feature_matrix_window" in message)
        catalog, snapshots = stub_features()
        catalog.tables["feature_matrix_measures"].main = 4
        called = FakeStore()
        c.refused("a table that moved before the read refuses the read", lambda: history(folder, catalog, called, features), "before the read")
        c.check("Feast was not called after a refused check", called.calls == [])
        catalog, snapshots = stub_features()
        short = {name: snapshot for name, snapshot in snapshots.items() if name != "label_matrix_hai_outcomes"}
        catalog.tables[reads.RUNS_TABLE].rows = [runs_row(RUN_A, T0, short)]
        c.refused(
            "a requested view whose table the publication does not name is refused",
            lambda: history(folder, catalog, FakeStore(), features),
            "label_matrix_hai_outcomes",
        )
        catalog, _ = stub_features()
        c.check("a named earlier publication can be read", history(folder, catalog, FakeStore(), features, publication_run=RUN_A).publication.run_id == RUN_A)
        c.refused(
            "a wrong namespace is refused",
            lambda: historical.historical_features(
                features, environ={"FEAST_NAMESPACE": "gold"}, catalog=catalog, store=FakeStore(), schema_path=write_schema(folder, synthetic_schema())
            ),
            "namespace",
        )


LOCAL_CASES: dict[str, tuple[Callable[[Checker], None], str]] = {
    "E17_select": (case_e17_select, "LC1 LC2 LC3 LH7"),
    "E17_before": (case_e17_before, "LC4"),
    "E17_during": (case_e17_during, "LC5 LC8 LC9"),
    "E17_tables": (case_e17_tables, "LC6"),
    "E17_support": (case_e17_support, "DT2 DT4"),
    "E17_fresh": (case_e17_fresh, "LC7"),
    "E17_history": (case_e17_history, "LC5 LH7"),
    "E11_schema": (case_e11_schema, "LC10 LC11"),
    "E11_views": (case_e11_views, "LC11"),
    "E11_variables": (case_e11_variables, "LC19"),
    "E11_register": (case_e11_register, "LC29 LC30"),
    "E13_order": (case_e13_order, "LC13"),
    "E13_lease": (case_e13_lease, "LC12"),
    "E13_protocol": (case_e13_protocol, "LC12"),
    "E13_repository": (case_e13_repository, "LC31 LC32"),
    "E13_hash": (case_e13_hash, "LC14"),
    "E30_modes": (case_e30_modes, "LC20 LC21"),
    "E30_registry": (case_e30_registry, "LC22 LC23"),
    "E30_token": (case_e30_token, "LC24"),
    "E30_pyiceberg": (case_e30_pyiceberg, "LC25"),
    "E30_removal": (case_e30_removal, "LC16"),
    "E30_deadline": (case_e30_deadline, "LC18"),
    "E30_record": (case_e30_record, "LC15"),
    "E30_yaml": (case_e30_yaml, "LC26"),
    "E30_cli": (case_e30_cli, "LC27 LC28"),
    "E30_harness": (case_e30_harness, "LC33"),
    "E12_utc": (case_e12_utc, "LH4"),
    "E15_spine": (case_e15_spine, "LH2"),
    "E15_entities": (case_e15_entities, "LH1"),
    "E15_refs": (case_e15_refs, "LH3 LH8"),
}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--part", choices=("local", "image"), required=True)
    parser.add_argument("--case", action="append", default=[])
    parser.add_argument("--report-dir", type=Path)
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.part == "local":
        cases = LOCAL_CASES
    else:
        from scripts.features import feast_image_cases

        cases = feast_image_cases.IMAGE_CASES
    report = run_cases(cases, args.case)
    path = write_report(args.part, report, args.report_dir)
    for item in report["cases"]:
        sys.stdout.write(
            f"{item['verdict'].upper():4} {item['case']} ({item['checks']} checks){'' if item['verdict'] == 'pass' else ': ' + '; '.join(item['failed'])}\n"
        )
    sys.stdout.write(f"{report['verdict'].upper()} report: {path}\n")
    return 0 if report["verdict"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
