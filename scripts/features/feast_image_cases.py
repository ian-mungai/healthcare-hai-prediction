"""The image part of the Feast cases (E11 to E17 and E32): the real Feast apply and historical reads over a PyIceberg SQL catalog fixture.

Run inside the Feast image, from the repository root mounted at ``/workspace``::

    python -B -m scripts.features.feast_cases --part image [--case E16 ...]

The fixture is built from the committed ``data_contracts/features/feature_schema.json`` (lane A): every table gets typed columns with deterministic values,
a few planted rows and a ``publish_runs`` row that names each table's ``main`` snapshot. It lives in a temporary folder; the registry is a file in it, Feast
needs no Redis for ``apply`` of a first registry, and no case touches AWS, Polaris or Docker. The cases use the real ``feature_store/definitions.py``.
The environment variables Feast reads are set for the run and restored at the end.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib
import json
import os
import shutil
import tempfile
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from scripts.features import feast_job, historical
from scripts.features import publication_reads as reads
from scripts.features.feast_harness import Checker

RUN_A = "20261022T120000Z-0a1b2c3d"
RUN_B = "20261022T130000Z-1a2b3c4d"
SPINE_KEYS = [("AAA001", 2021), ("AAA001", 2022), ("AAA001", 2023), ("BBB002", 2022), ("CCC003", 2021), ("DDD004", 2023), ("EEE005", 2022)]
EXACT = 0.12345678901234568
OWN_VALUE = 7.25
FUTURE_VALUE = 999.25
EARLIER_VALUE = 5.5
TYPE_NAMES = {"Float64", "Int64", "String", "Bool"}


def window(year: int) -> datetime:
    return datetime(year, 1, 1, tzinfo=UTC)


def key_of(ccn: str, year: int) -> str:
    return f"{ccn}:{year}"


@contextlib.contextmanager
def preserved_environment() -> Iterator[None]:
    """Restore ``os.environ`` and the time zone after a case."""
    saved = dict(os.environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)
        time.tzset()


def arrow_type(kind: str) -> Any:
    pa = importlib.import_module("pyarrow")
    return {
        "varchar": pa.string(),
        "double": pa.float64(),
        "bigint": pa.int64(),
        "boolean": pa.bool_(),
        "timestamp with time zone": pa.timestamp("us", tz="UTC"),
    }[kind]


def cell(kind: str, row: int, column: int) -> Any:
    """A deterministic value; every eleventh cell is null."""
    if (row + column) % 11 == 0:
        return None
    if kind == "double":
        return 1.0 / 3.0 + row * 0.1 + column * 0.001
    if kind == "bigint":
        return row * 10 + column
    if kind == "boolean":
        return (row + column) % 2 == 0
    return f"v{row}_{column}"


@dataclass
class Fixture:
    """A temporary SQL catalog holding the schema's tables, the entity table and ``publish_runs``."""

    folder: Path
    schema: dict[str, Any]
    schema_sha256: str
    catalog: Any
    target: tuple[str, str]

    @property
    def registry(self) -> Path:
        return self.folder / "feast/registry.pb"

    def table(self, name: str) -> Any:
        return self.catalog.load_table(f"features_e2e.{name}")

    def heads(self) -> dict[str, int]:
        return {name: self.table(name).current_snapshot().snapshot_id for name in [*self.schema["tables"], reads.SPINE_TABLE]}

    def publish(self, run_id: str, omit: tuple[str, ...] = (), committed: datetime | None = None) -> None:
        pa = importlib.import_module("pyarrow")
        heads = {name: snapshot for name, snapshot in self.heads().items() if name not in omit}
        schema = pa.schema([("run_id", pa.string()), ("committed_at", pa.timestamp("us", tz="UTC")), ("snapshot_set", pa.string())])
        row = {"run_id": run_id, "committed_at": committed or datetime.now(UTC), "snapshot_set": json.dumps(heads)}
        identifier = f"features_e2e.{reads.RUNS_TABLE}"
        if not self.catalog.table_exists(identifier):
            self.catalog.create_table(identifier, schema=schema)
        self.table(reads.RUNS_TABLE).append(pa.Table.from_pylist([row], schema=schema))

    def append_row(self, name: str) -> None:
        """Move ``main`` of a table: append one row for a key that no spine row names."""
        pa = importlib.import_module("pyarrow")
        table = self.table(name)
        arrow = pa.schema([(field.name, arrow_type_of(field)) for field in table.schema().fields])
        row: dict[str, Any] = {field.name: None for field in table.schema().fields}
        if "spine_key" in row:
            row["spine_key"] = "ZZZ999:2099"
        if "event_timestamp" in row:
            row["event_timestamp"] = window(2099)
        table.append(pa.Table.from_pylist([row], schema=arrow))


def arrow_type_of(field: Any) -> Any:
    pa = importlib.import_module("pyarrow")
    names = {
        "string": pa.string(),
        "double": pa.float64(),
        "long": pa.int64(),
        "boolean": pa.bool_(),
        "timestamptz": pa.timestamp("us", tz="UTC"),
        "date": pa.date32(),
        "int": pa.int32(),
    }
    return names[str(field.field_type)]


def predictor_columns(schema: dict[str, Any]) -> list[tuple[str, str]]:
    """Every double predictor column as (table, column), in table and column order."""
    return [
        (name, c["name"]) for name, table in sorted(schema["tables"].items()) for c in table["columns"] if c["role"] == "predictor" and c["type"] == "double"
    ]


def environment_for(folder: Path) -> dict[str, str]:
    return {
        "FEAST_NAMESPACE": "features_e2e",
        "FEAST_CATALOG_TYPE": "sql",
        "FEAST_RUN_FOLDER": str(folder),
        "FEAST_SQL_WAREHOUSE": f"file://{folder}/warehouse",
        "FEAST_REGISTRY_PATH": f"{folder}/feast/registry.pb",
        "FEAST_REDIS_HOST": "redis",
        "REDIS_PASSWORD": "fixture-password-not-a-secret",
    }


def table_rows(name: str, columns: list[dict[str, Any]], retype: dict[tuple[str, str], str], target: tuple[str, str]) -> tuple[Any, list[dict[str, Any]]]:
    """The Arrow schema and rows of one fixture table: one row per spine key at its window start, plus the planted rows of the target table."""
    pa = importlib.import_module("pyarrow")
    override = {column: kind for (table_name, column), kind in retype.items() if table_name == name}
    arrow = pa.schema(
        [(c["name"], {"int32": pa.int32(), "date": pa.date32()}[override[c["name"]]] if c["name"] in override else arrow_type(c["type"])) for c in columns]
    )
    rows = []
    for number, (ccn, year) in enumerate(SPINE_KEYS):
        row: dict[str, Any] = {}
        for index, c in enumerate(columns):
            if c["name"] == "spine_key":
                row["spine_key"] = key_of(ccn, year)
            elif c["name"] == "event_timestamp":
                row["event_timestamp"] = window(year)
            else:
                row[c["name"]] = None if c["name"] in override else cell(c["type"], number, index)
        rows.append(row)
    if name == target[0]:
        by_key = {row["spine_key"]: row for row in rows}
        by_key[key_of("AAA001", 2021)][target[1]] = EXACT
        by_key[key_of("DDD004", 2023)][target[1]] = OWN_VALUE
        by_key[key_of("EEE005", 2022)][target[1]] = None
        empty = {c["name"]: None for c in columns}
        rows.append({**empty, "spine_key": key_of("DDD004", 2023), "event_timestamp": window(2024), target[1]: FUTURE_VALUE})
        rows.append({**empty, "spine_key": key_of("EEE005", 2022), "event_timestamp": datetime(2021, 12, 31, tzinfo=UTC), target[1]: EARLIER_VALUE})
    return arrow, rows


def build_fixture(retype: dict[tuple[str, str], str] | None = None, publish: bool = True) -> Fixture:
    """Build the fixture in a new folder; ``retype`` gives a column another Arrow type (all null) to plant a type drift."""
    pa = importlib.import_module("pyarrow")
    folder = Path(tempfile.mkdtemp(prefix="feast_image_cases_"))
    (folder / "feast").mkdir()
    (folder / "warehouse").mkdir()
    os.environ.update(environment_for(folder))
    schema, sha = reads.read_schema()
    catalog: Any = reads.open_catalog(reads.catalog_settings(os.environ, "publish_fixture"))
    catalog.create_namespace("features_e2e")
    target = predictor_columns(schema)[0]
    fixture = Fixture(folder, schema, sha, catalog, target)
    for name, table in schema["tables"].items():
        arrow, rows = table_rows(name, table["columns"], retype or {}, target)
        iceberg = catalog.create_table(f"features_e2e.{name}", schema=arrow)
        iceberg.append(pa.Table.from_pylist(rows, schema=arrow))
    spine_schema = pa.schema([("spine_key", pa.string()), ("ccn", pa.string()), ("window_year", pa.int64()), ("window_start", pa.date32())])
    spine_rows = [{"spine_key": key_of(ccn, year), "ccn": ccn, "window_year": year, "window_start": date(year, 1, 1)} for ccn, year in SPINE_KEYS]
    catalog.create_table(f"features_e2e.{reads.SPINE_TABLE}", schema=spine_schema).append(pa.Table.from_pylist(spine_rows, schema=spine_schema))
    if publish:
        fixture.publish(RUN_A)
    return fixture


def drop(fixture: Fixture) -> None:
    shutil.rmtree(fixture.folder, ignore_errors=True)


def apply_fixture(fixture: Fixture, backend: feast_job.FeastBackend | None = None) -> dict[str, Any]:
    """Run the job's apply on the fixture with the real Feast and a lease that does nothing (no Redis in the offline cases)."""
    settings = feast_job.job_settings(os.environ, "apply", RUN_A, "publish_fixture")
    return feast_job.run(settings, os.environ, backend=backend, lease=no_lease, schema=(fixture.schema, fixture.schema_sha256))


@contextlib.contextmanager
def no_lease() -> Iterator[None]:
    yield


def references(fixture: Fixture, per_table: int | None = None) -> list[str]:
    predictors, labels = reads.reference_lists(fixture.schema)
    if per_table is None:
        return [*predictors, *labels]
    chosen: dict[str, list[str]] = {}
    for reference in [*predictors, *labels]:
        chosen.setdefault(reference.split(":", 1)[0], []).append(reference)
    return [reference for items in chosen.values() for reference in items[:per_table]]


def read_history(fixture: Fixture, features: list[str], store: Any = None, **options: Any) -> historical.HistoricalRead:
    return historical.historical_features(features, environ=os.environ, store=store, **options)


class MovingJob:
    def __init__(self, job: Any, mover: Callable[[], None]) -> None:
        self.job, self.mover = job, mover

    def to_arrow(self) -> Any:
        self.mover()
        return self.job.to_arrow()


class MovingStore:
    """The real store, except that ``to_arrow`` first runs ``mover``: a commit that lands while the historical read is running."""

    def __init__(self, store: Any, mover: Callable[[], None]) -> None:
        self.store, self.mover, self.calls = store, mover, 0

    def get_historical_features(self, entity_df: Any, features: list[str]) -> MovingJob:
        self.calls += 1
        return MovingJob(self.store.get_historical_features(entity_df=entity_df, features=features), self.mover)


class MovingBackend(feast_job.FeastRepository):
    """The real Feast backend, except that ``apply`` first runs ``mover``: a commit that lands while the registry is written."""

    def __init__(self, mover: Callable[[], None]) -> None:
        super().__init__()
        self.mover = mover

    def apply(self) -> None:
        self.mover()
        super().apply()


def values_by_key(result: historical.HistoricalRead, column: str) -> dict[str, Any]:
    frame = result.frame
    keys, values = frame.column("spine_key").to_pylist(), frame.column(column).to_pylist()
    return dict(zip(keys, values, strict=True))


def case_e11(c: Checker) -> None:
    """LC10, LC11, LC17 and B2: a good fixture applies; planted drift is refused before Feast writes the registry; only the four Feast types are registered."""
    with preserved_environment():
        fixture = build_fixture()
        try:
            record = apply_fixture(fixture)
            c.check("a clean fixture applies and the registry file exists", fixture.registry.is_file())
            predictor_tables = [name for name, table in fixture.schema["tables"].items() if table["role"] == "predictor"]
            c.check(
                "one entity and one view per predictor table plus six label views",
                record["objects"]["entity"] == 1 and record["objects"]["feature_view"] == len(predictor_tables) + 6,
            )
            c.check("one data source per table with views", record["objects"]["data_source"] == len(fixture.schema["tables"]))
            objects = feast_job.FeastRepository().object_list()
            registered_types = {feature[1] for item in objects if item["kind"] == "feature_view" for feature in item["features"]}
            c.check("only Float64, Int64, String and Bool are registered", registered_types <= TYPE_NAMES and bool(registered_types))
            labels = [item for item in objects if item["kind"] == "feature_view" and item["name"].startswith("label_")]
            c.check("label views are offline only", len(labels) == 6 and not any(item["online"] for item in labels))
            c.check(
                "predictor views are online with ttl 0",
                all(
                    item["online"] and item["ttl_seconds"] == 0 for item in objects if item["kind"] == "feature_view" and not item["name"].startswith("label_")
                ),
            )
            c.check(
                "the entity is the hospital-window with join key spine_key", {"kind": "entity", "name": "hospital_window", "join_key": "spine_key"} in objects
            )
        finally:
            drop(fixture)
    for label, refusal in (
        ("an extra column", "refused before apply"),
        ("a missing table", "does not name this table"),
        ("a 32-bit integer", "another type"),
        ("a date column", "another type"),
    ):
        with preserved_environment():
            if label in ("a 32-bit integer", "a date column"):
                other = predictor_columns(reads.read_schema()[0])[1]
                fixture = build_fixture(retype={other: "int32" if label == "a 32-bit integer" else "date"})
            else:
                fixture = build_fixture()
            target_table = fixture.target[0]
            try:
                if label == "an extra column":
                    iceberg_types = importlib.import_module("pyiceberg.types")
                    with fixture.table(target_table).update_schema() as update:
                        update.add_column("surprise_column", iceberg_types.StringType())
                elif label == "a missing table":
                    fixture.publish(RUN_B, omit=(target_table,), committed=datetime.now(UTC) + timedelta(seconds=5))
                c.refused(f"{label} is refused", apply_fixture, refusal, fixture)
                c.check(f"{label}: Feast wrote no registry", not fixture.registry.exists())
            finally:
                drop(fixture)


def case_e12(c: Checker) -> None:
    """LH4 and B3: the same entity frame and the same values in UTC-8, UTC and UTC+9."""
    with preserved_environment():
        fixture = build_fixture()
        try:
            apply_fixture(fixture)
            features = references(fixture, per_table=2)
            outcomes = []
            for zone in ("America/Los_Angeles", "UTC", "Asia/Tokyo"):
                os.environ["TZ"] = zone
                time.tzset()
                result = read_history(fixture, features)
                outcomes.append((zone, result.frame.to_pylist()))
            c.check("the rows are equal in all three zones", outcomes[0][1] == outcomes[1][1] == outcomes[2][1] and bool(outcomes[0][1]))
            expected = {key_of(ccn, year): window(year) for ccn, year in SPINE_KEYS}
            times = {row["spine_key"]: row["event_timestamp"] for row in outcomes[0][1]}
            c.check("every event time is its own window start at 00:00 UTC", all(times[key] == moment for key, moment in expected.items()))
        finally:
            drop(fixture)


def case_e13(c: Checker) -> None:
    """LC14 and B5: a repeated apply gives the same object list digest."""
    with preserved_environment():
        fixture = build_fixture()
        try:
            first = apply_fixture(fixture)
            second = apply_fixture(fixture)
            c.check("the object list digest is equal", first["object_list_sha256"] == second["object_list_sha256"])
            c.check("the counts are equal", first["objects"] == second["objects"])
        finally:
            drop(fixture)


def case_e14(c: Checker) -> None:
    """LH5 and C1: the Feast layer of the point-in-time cases. Rows after the window and an earlier value under a masked null never reach the result."""
    with preserved_environment():
        fixture = build_fixture()
        try:
            apply_fixture(fixture)
            table, column = fixture.target
            reference = f"{table}:{column}"
            values = values_by_key(read_history(fixture, [reference]), column)
            c.check("a row dated after the window start is not read", values[key_of("DDD004", 2023)] == OWN_VALUE)
            c.check("a null at the window start is not filled from an earlier row", values[key_of("EEE005", 2022)] is None)
            c.check("the exact 17-digit value is read", values[key_of("AAA001", 2021)] == EXACT and repr(values[key_of("AAA001", 2021)]) == repr(EXACT))
            store = historical.default_store(os.environ)
            pandas = importlib.import_module("pandas")
            frame = pandas.DataFrame({"spine_key": [key_of("DDD004", 2023)], "event_timestamp": pandas.to_datetime([window(2025)], utc=True)})
            later = store.get_historical_features(entity_df=frame, features=[reference]).to_arrow().column(column).to_pylist()
            c.check("with the rule disabled (a later entity time) the planted future row is visible", later == [FUTURE_VALUE])
        finally:
            drop(fixture)


def case_e15(c: Checker) -> None:
    """LH1 and C2: an entity time other than the key's window start is refused before Feast is called."""
    with preserved_environment():
        fixture = build_fixture()
        try:
            apply_fixture(fixture)
            store = MovingStore(historical.default_store(os.environ), lambda: None)
            feature = [references(fixture, per_table=1)[0]]
            key = key_of("AAA001", 2021)
            for label, entities, contains in (
                ("a later time", [(key, window(2022))], "own window start"),
                ("a naive time", [(key, datetime(2021, 1, 1))], "naive"),
                ("an unknown key", [("ZZZ999:2021", window(2021))], "not in int_hospital_spine"),
                ("a repeated key", [(key, window(2021)), (key, window(2021))], "repeated"),
            ):
                c.refused(f"{label} is refused", read_history, contains, fixture, feature, store, entities=entities)
            c.check("Feast was never called", store.calls == [])
            result = read_history(fixture, feature, store, entities=[(key, window(2021))])
            c.check("the key's own window start is accepted", result.entity_count == 1 and store.calls == 1)
        finally:
            drop(fixture)


def case_e16(c: Checker) -> None:
    """LH6, C3, C4 and D8: the historical result equals the published tables row for row, null for null, exact to 17 digits, oldest and newest window."""
    with preserved_environment():
        fixture = build_fixture()
        try:
            apply_fixture(fixture)
            features = references(fixture)
            result = read_history(fixture, features)
            c.check("one row per spine key", result.entity_count == len(SPINE_KEYS) and result.frame.num_rows == len(SPINE_KEYS))
            c.check(
                "the result names the publication and the schema digest",
                result.publication.run_id == RUN_A and result.feature_schema_sha256 == hashlib.sha256(reads.SCHEMA_PATH.read_bytes()).hexdigest(),
            )
            c.check("the keys are sorted", result.frame.column("spine_key").to_pylist() == sorted(key_of(ccn, year) for ccn, year in SPINE_KEYS))
            wanted = {key_of(ccn, year): window(year) for ccn, year in SPINE_KEYS}
            mismatches, compared, nulls = [], 0, 0
            by_view: dict[str, list[str]] = {}
            for reference in features:
                by_view.setdefault(reference.split(":", 1)[0], []).append(reference.split(":", 1)[1])
            view_table = {column["view"]: name for name, table in fixture.schema["tables"].items() for column in table["columns"] if column.get("view")}
            for view, columns in by_view.items():
                published = {
                    row["spine_key"]: row
                    for row in fixture.table(view_table[view]).scan().to_arrow().to_pylist()
                    if row["event_timestamp"] == wanted[row["spine_key"]]
                }
                for column in columns:
                    got = values_by_key(result, column)
                    for key in wanted:
                        compared += 1
                        nulls += got[key] is None
                        if got[key] != published[key][column] or repr(got[key]) != repr(published[key][column]):
                            mismatches.append(f"{view}.{column}")
            c.check(f"{compared} values equal their published cells", not mismatches)
            c.check("null cells stayed null", nulls > 0)
            oldest = min(wanted.values())
            oldest_key = next(key for key, moment in wanted.items() if moment == oldest)
            row = next(item for item in result.frame.to_pylist() if item["spine_key"] == oldest_key)
            c.check(
                "the oldest window is served (ttl 0)", any(value is not None for name, value in row.items() if name not in ("spine_key", "event_timestamp"))
            )
            arrow_names = {"Float64": {"double"}, "Int64": {"int64"}, "Bool": {"bool"}, "String": {"string", "large_string"}}
            frame_types = {field.name: str(field.type) for field in result.frame.schema}
            wrong = [
                column["name"]
                for table in fixture.schema["tables"].values()
                for column in table["columns"]
                if column["role"] == "predictor" and frame_types[column["name"]] not in arrow_names[column["feast_type"]]
            ]
            c.check("Float64, Int64, String and Bool columns keep their Arrow type", not wrong)
        finally:
            drop(fixture)


def case_e17(c: Checker) -> None:
    """LC5, LC6 and A10: a commit between fast-forwards during a historical read or an apply is refused."""
    with preserved_environment():
        fixture = build_fixture()
        try:
            apply_fixture(fixture)
            table = fixture.target[0]
            feature = [references(fixture, per_table=1)[0]]
            moving = MovingStore(historical.default_store(os.environ), lambda: fixture.append_row(table))
            outcome: list[Any] = []
            message = c.refused(
                "a table moved during the historical read is refused", lambda: outcome.append(read_history(fixture, feature, moving)), "after the read"
            )
            c.check("no result reaches the caller and the table is named", outcome == [] and table in message)
            c.check("the read really ran", moving.calls == 1)
            counting = MovingStore(historical.default_store(os.environ), lambda: None)
            c.refused("the moved table is refused before the next read", read_history, "before the read", fixture, feature, counting)
            c.check("Feast was not called after a refused check", counting.calls == 0)
        finally:
            drop(fixture)
    with preserved_environment():
        fixture = build_fixture()
        try:
            table = fixture.target[0]
            message = c.refused(
                "a table moved during apply is refused", apply_fixture, "after apply", fixture, MovingBackend(lambda: fixture.append_row(table))
            )
            c.check("the refusal names the table", table in message)
            c.check("no record is written by the refused job", not (fixture.folder / feast_job.RECORD_NAME).exists())
        finally:
            drop(fixture)


def case_e32(c: Checker) -> None:
    """LH3, LH8 and B4: label views hold labels only and are offline only; identifiers and the table name are not references."""
    with preserved_environment():
        fixture = build_fixture()
        try:
            apply_fixture(fixture)
            predictors, labels = reads.reference_lists(fixture.schema)
            registered = {item["name"]: item for item in feast_job.FeastRepository().object_list() if item["kind"] == "feature_view"}
            label_views = {name: item for name, item in registered.items() if name.startswith("label_")}
            from_registry = sorted(f"{name}:{feature[0]}" for name, item in label_views.items() for feature in item["features"])
            c.check("the label views register exactly the label columns", from_registry == labels)
            c.check(
                "no predictor view registers a label column",
                not any(f"{name}:{feature[0]}" in labels for name, item in registered.items() if name not in label_views for feature in item["features"]),
            )
            mixed = [predictors[0], labels[0]]
            result = read_history(fixture, mixed)
            c.check(
                "a predictor and a label are read together",
                result.frame.num_rows == len(SPINE_KEYS) and all(reference.split(":", 1)[1] in result.frame.column_names for reference in mixed),
            )
            c.refused("an identifier is refused", read_history, "not registered", fixture, ["feature_matrix_hospital_window:ccn"])
            c.refused(
                "a label through its table name is refused",
                read_history,
                "not registered",
                fixture,
                [f"label_matrix_hai_outcomes:{labels[0].split(':', 1)[1]}"],
            )
        finally:
            drop(fixture)


IMAGE_CASES: dict[str, tuple[Callable[[Checker], None], str]] = {
    "E11": (case_e11, "LC10 LC11 LC17 B1 B2 B6"),
    "E12": (case_e12, "LH4 B3"),
    "E13": (case_e13, "LC14 B5"),
    "E14": (case_e14, "LH5 C1 C2"),
    "E15": (case_e15, "LH1 C2"),
    "E16": (case_e16, "LH6 C3 C4 D8"),
    "E17": (case_e17, "LC5 LC6 A10 C5"),
    "E32": (case_e32, "LH3 LH8 B4"),
}
