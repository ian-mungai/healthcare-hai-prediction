"""The checked historical-feature read that training and drift must use (design p3c sections 3.5 and 3.6).

``historical_features`` reads point-in-time features from the offline store (Feast over the Iceberg features namespace) and refuses what would make a
training extract wrong or irreproducible:

- the publication is selected once (the head ``publish_runs`` row, or a named run) and the ``main`` snapshot of every table it names is verified before
  the read and again after ``to_arrow()`` consumed the result (``publication_reads.consistent_read``), so a commit between fast-forwards cannot slip in;
- the entity frame is built from ``int_hospital_spine`` read at that publication's snapshot, with each key's event time equal to its own window start at
  00:00 UTC, timezone aware; a caller's entity time that differs from its key's window start, is naive or names an unknown or repeated key is refused;
- every feature reference must be a registered predictor or label of ``feature_schema.json`` (no identifier, no excluded control, no typo);
- the result is sorted by ``spine_key`` and returned with the selected publication row and the SHA-256 of the schema file, which the training package
  records (failure modes C6 and C7).

The caller prepares the environment as the Feast job does (``FEAST_NAMESPACE``, ``FEAST_CATALOG_TYPE``, the registry and catalog variables; real runs also
``FEAST_POLARIS_TOKEN`` and a private ``PYICEBERG_HOME``). Feast, pandas and PyIceberg are imported when needed, so the host checks import none of them.
"""

from __future__ import annotations

import importlib
import os
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any

from scripts.features import fixture_catalog
from scripts.features import publication_reads as reads
from scripts.features.publication_reads import ReadError

REPO = reads.REPO_ROOT / "feature_store"
SETTINGS_FILE = REPO / "feature_store.yaml"
KEY = "spine_key"
EVENT_TIME = "event_timestamp"
WINDOW_START = "window_start"


@dataclass(frozen=True)
class HistoricalRead:
    """A verified historical read: the sorted result (a PyArrow table), the publication it came from and the schema digest."""

    frame: Any
    publication: reads.Publication
    feature_schema_sha256: str
    entity_count: int
    features: tuple[str, ...]


def window_start_utc(value: Any) -> datetime:
    """The event time of a spine row: its window start at 00:00 UTC, timezone aware.

    A date is the calendar day; a datetime must already be midnight (a naive one is read as UTC). Anything else, or a time of day, is refused.
    """
    if isinstance(value, datetime):
        moment = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    elif isinstance(value, date):
        moment = datetime.combine(value, time(0), tzinfo=UTC)
    else:
        raise ReadError("a window start is not a date")
    if moment.timetz().replace(tzinfo=None) != time(0):
        raise ReadError("a window start is not at 00:00 UTC")
    return moment


def spine_windows(rows: Sequence[Mapping[str, Any]]) -> dict[str, datetime]:
    """Spine key to window start; a null or repeated key or a null or non-midnight window start is refused with counts (failure mode LH2)."""
    windows: dict[str, datetime] = {}
    null_keys = repeated = null_starts = 0
    for row in rows:
        key, start = row.get(KEY), row.get(WINDOW_START)
        if not isinstance(key, str) or not key:
            null_keys += 1
        elif key in windows:
            repeated += 1
        elif start is None:
            null_starts += 1
        else:
            windows[key] = window_start_utc(start)
    if null_keys or repeated or null_starts:
        raise ReadError(f"{reads.SPINE_TABLE} is not a clean entity table: {null_keys} null keys, {repeated} repeated keys, {null_starts} null window starts")
    if not windows:
        raise ReadError(f"{reads.SPINE_TABLE} has no rows")
    return windows


def entity_rows(windows: Mapping[str, datetime], entities: Sequence[tuple[str, datetime]] | None) -> list[tuple[str, datetime]]:
    """The entity frame as (key, event time) pairs: every spine key, or the caller's keys with their times checked against their own window start."""
    if entities is None:
        return sorted(windows.items())
    seen: set[str] = set()
    checked = []
    for key, moment in entities:
        if key in seen:
            raise ReadError(f"entity key {key} is repeated")
        seen.add(key)
        if key not in windows:
            raise ReadError(f"entity key {key} is not in {reads.SPINE_TABLE}")
        if not isinstance(moment, datetime) or moment.tzinfo is None:
            raise ReadError(f"entity key {key} has a naive or missing time; give a timezone-aware time")
        if moment.astimezone(UTC) != windows[key]:
            raise ReadError(f"entity key {key} has a time that is not its own window start")
        checked.append((key, windows[key]))
    if not checked:
        raise ReadError("no entity keys were given")
    return sorted(checked)


def checked_references(schema: Mapping[str, Any], features: Sequence[str]) -> tuple[str, ...]:
    """The requested references, each a registered predictor or label of the schema; no repeats, and bare column names must be unique."""
    predictors, labels = reads.reference_lists(schema)
    allowed = set(predictors) | set(labels)
    if not features:
        raise ReadError("no feature references were requested")
    unknown = sorted({reference for reference in features if reference not in allowed})
    if unknown:
        raise ReadError(f"{len(unknown)} feature references are not registered predictors or labels ({reads.listed(unknown)})")
    if len(set(features)) != len(features):
        raise ReadError("a feature reference is requested twice")
    names = [reference.split(":", 1)[1] for reference in features]
    if len(set(names)) != len(names):
        raise ReadError("two requested references share one column name")
    return tuple(features)


def default_store(environ: Mapping[str, str], settings_file: Path = SETTINGS_FILE) -> Any:
    """The Feast store of the repository, after the settings file's variables are shown to be set."""
    missing = reads.unresolved_variables(settings_file.read_text(), environ)
    if missing:
        raise ReadError(f"{settings_file.name} variables are unset or unresolved: {', '.join(missing)}")
    options = {} if settings_file == SETTINGS_FILE else {"fs_yaml_file": settings_file}
    return importlib.import_module("feast").FeatureStore(repo_path=str(REPO), **options)


def default_catalog(environ: Mapping[str, str], publication_run: str | None = None, register: bool = True) -> reads.IcebergCatalog:
    """The catalog of the environment's mode (fixture SQL catalog or Polaris with ``FEAST_POLARIS_TOKEN``).

    A fixture catalog first gets the selected publication of the local warehouse registered (``fixture_catalog``); ``register=False`` is for catalogs that
    already hold their tables.
    """
    mode = reads.MODE_OF_NAMESPACE.get(environ.get("FEAST_NAMESPACE", ""))
    if mode is None:
        raise ReadError("FEAST_NAMESPACE is not a features namespace")
    settings = reads.catalog_settings(environ, mode, bearer=environ.get("FEAST_POLARIS_TOKEN", ""))
    catalog = reads.open_catalog(settings)
    if mode == "publish_fixture" and register:
        registering: Any = catalog
        fixture_catalog.register_publication(registering, fixture_catalog.warehouse_folder(settings.sql_warehouse), settings.namespace, publication_run)
    return catalog


def historical_features(
    features: Sequence[str],
    *,
    entities: Sequence[tuple[str, datetime]] | None = None,
    publication_run: str | None = None,
    environ: MutableMapping[str, str] | None = None,
    register: bool = True,
    catalog: reads.IcebergCatalog | None = None,
    store: Any = None,
    schema_path: Path = reads.SCHEMA_PATH,
) -> HistoricalRead:
    """Read ``features`` for every spine key (or the given keys) at their own window start from one verified publication."""
    live = os.environ if environ is None else environ
    schema, schema_sha256 = reads.read_schema(schema_path)
    references = checked_references(schema, features)
    namespace = live.get("FEAST_NAMESPACE", "")
    if namespace not in reads.MODE_OF_NAMESPACE:
        raise ReadError("FEAST_NAMESPACE is not a features namespace")
    source = catalog if catalog is not None else default_catalog(live, publication_run, register)
    publication = reads.read_publication(source, namespace, publication_run)
    view_tables = {column["view"]: name for name, table in schema["tables"].items() for column in table["columns"] if column.get("view")}
    needed = {view_tables[reference.split(":", 1)[0]] for reference in references} | {reads.SPINE_TABLE}
    missing = sorted(name for name in needed if name not in publication.snapshot_set)
    if missing:
        raise ReadError(f"publication {publication.run_id} does not name {reads.listed(missing)}")
    feature_store = store if store is not None else default_store(live)

    def read() -> tuple[Any, int]:
        spine = reads.load_table(source, namespace, reads.SPINE_TABLE)
        scan = spine.scan(selected_fields=(KEY, WINDOW_START), snapshot_id=publication.snapshot_set[reads.SPINE_TABLE])
        rows = entity_rows(spine_windows(scan.to_arrow().to_pylist()), entities)
        pandas = importlib.import_module("pandas")
        entity_df = pandas.DataFrame({KEY: [key for key, _ in rows], EVENT_TIME: pandas.to_datetime([moment for _, moment in rows], utc=True)})
        frame = feature_store.get_historical_features(entity_df=entity_df, features=list(references)).to_arrow()
        return frame.sort_by(KEY), len(rows)

    frame, count = reads.consistent_read(source, publication, read)
    return HistoricalRead(frame, publication, schema_sha256, count, references)
