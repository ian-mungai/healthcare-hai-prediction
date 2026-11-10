"""The Feast phase of a model set: apply against the set's own publication, then load online, hold the load or stay offline; and the release of a held load.

A model set run publishes one table, ``feature_matrix_<set>``. Its Feast phase

1. ``set_apply`` registers the repository after it compares the set's table with the schema's model set and the label table of the base run that the set
   read (``base_run`` of ``features_snapshot.json``) with the schema's label table. The publication of the set run and the base publication are both
   verified before and after the apply.
2. ``set_materialize`` decides what happens online (``scripts.features.feature_sets.online_action``): nothing for base, offline only for a set that is not
   online, a held load when the set's schema hash differs from the hash of a Production model of that set (the newest Production record of each infection
   type in the data bucket, ``production_reader``), otherwise the load. A held load writes
   ``online_hold.json`` in the run's prefix and loads nothing; the set's table is published and registered offline either way. While a promotion of any model
   is unresolved (an open lease generation, or a newest closed generation without its Production record) the step is refused (``unresolved_promotion``).
3. ``release`` loads a held run online. The training side calls it after the model trained on the new schema is promoted:
   ``python -m scripts.features.feast_materialize --release <SET> <RUN_ID>``. A repeat changes nothing. An unresolved promotion refuses it too.

Each online set (m1, m2 and m4) has its own Redis database (3, 4 and 5) and its own marker (``online_protocol.set_layout``), so the load of one set flushes and
marks nothing of another and a held set keeps serving its last online data. ``online_loader`` is the load step; it is injected so the cases need no Feast.
Records hold names, counts and hashes only.
"""

from __future__ import annotations

import contextlib
import importlib
import importlib.metadata
import json
import os
import tempfile
import urllib.parse
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import UTC
from pathlib import Path
from typing import Any, Protocol

from scripts.features import feast_job, feature_schema, feature_sets, historical
from scripts.features import online_protocol as online
from scripts.features import publication_reads as reads
from scripts.features.publication_reads import ReadError
from scripts.training import production_record, promote_lease, promote_receipt

HOLD_NAME = "online_hold.json"
RELEASE_NAME = "online_release.json"
SNAPSHOT_RECORD = "features_snapshot.json"
RUN_PREFIX = "lakehouse/layer_runs/features"
PRECONDITION_CODES = ("PreconditionFailed", "ConditionalRequestConflict")


@dataclass(frozen=True)
class LoadRequest:
    """What one online load of a set needs: the set, the run whose table is loaded, the base run it read and the set's schema hash."""

    feature_set: str
    run: str
    base_run: str
    set_schema_sha256: str


OnlineLoader = Callable[[LoadRequest], str]
Production = Callable[[], Sequence[str | None]]


class HoldStore(Protocol):
    """Where the hold and release records of a run live: the run's prefix. Writes are create-only."""

    def read(self, name: str, run: str) -> dict[str, Any] | None: ...

    def write_new(self, name: str, run: str, document: Mapping[str, Any]) -> bool: ...


class FolderHoldStore:
    """The records in a local folder, one subfolder per run (fixture runs and the cases)."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def path(self, name: str, run: str) -> Path:
        return self.root / run / name

    def read(self, name: str, run: str) -> dict[str, Any] | None:
        path = self.path(name, run)
        if not path.is_file():
            return None
        document: dict[str, Any] = json.loads(path.read_text())
        return document

    def write_new(self, name: str, run: str, document: Mapping[str, Any]) -> bool:
        path = self.path(name, run)
        path.parent.mkdir(parents=True, exist_ok=True)
        handle, temporary = tempfile.mkstemp(dir=path.parent, prefix=name + ".", suffix=".tmp")
        try:
            with os.fdopen(handle, "w") as stream:
                stream.write(json.dumps(document, indent=2, sort_keys=True) + "\n")
            try:
                os.link(temporary, path)
            except FileExistsError:
                return False
            return True
        finally:
            with contextlib.suppress(OSError):
                os.unlink(temporary)


class RunFolderStore(FolderHoldStore):
    """The records in the run folder of one run itself (``/workspace/out``): the Feast phase writes the hold there and the layer driver uploads it."""

    def __init__(self, folder: Path, run: str) -> None:
        super().__init__(folder)
        self.run = run

    def path(self, name: str, run: str) -> Path:
        if run != self.run:
            raise ReadError(f"the run folder holds the records of run {self.run}, not of run {run}")
        return self.root / name


class S3HoldStore:
    """The records under ``lakehouse/layer_runs/features/<run>/`` of the data bucket; a write succeeds only when no object has the key."""

    def __init__(self, client: Any, bucket: str) -> None:
        self.client, self.bucket = client, bucket

    def key(self, name: str, run: str) -> str:
        return f"{RUN_PREFIX}/{run}/{name}"

    def read(self, name: str, run: str) -> dict[str, Any] | None:
        try:
            body = self.client.get_object(Bucket=self.bucket, Key=self.key(name, run))["Body"].read()
        except self.client.exceptions.NoSuchKey:
            return None
        document: dict[str, Any] = json.loads(body)
        return document

    def write_new(self, name: str, run: str, document: Mapping[str, Any]) -> bool:
        body = json.dumps(document, indent=2, sort_keys=True) + "\n"
        try:
            self.client.put_object(Bucket=self.bucket, Key=self.key(name, run), Body=body, IfNoneMatch="*")
        except self.client.exceptions.ClientError as error:
            if error.response["Error"]["Code"] not in PRECONDITION_CODES:
                raise
            return False
        return True


def refusing_loader(request: LoadRequest) -> str:
    """The default online load: refused by name until the online store holds one load per set instead of one load for all."""
    raise ReadError(
        f"the per-set online store is not built, so set {request.feature_set} (run {request.run}) is not loaded online: "
        "the online protocol flushes the whole feature database at the start of a load"
    )


PROJECT = "hai_prediction"
KEY_VERSION = 3


def set_load_plan(catalog: reads.IcebergCatalog, publication: reads.Publication, entry: Mapping[str, Any], set_hash: str, version: str) -> online.LoadPlan:
    """The plan of one set load, read at the run's snapshot of the set's table: entities, predictor columns and the inclusive time window."""
    table = entry["table"]
    rows = (
        reads.load_table(catalog, publication.namespace, table)
        .scan(selected_fields=("spine_key", "event_timestamp"), snapshot_id=publication.snapshot_set[table])
        .to_arrow()
        .to_pylist()
    )
    keys, times = [row["spine_key"] for row in rows], [row["event_timestamp"] for row in rows]
    if not keys or len(set(keys)) != len(keys) or None in keys or None in times:
        raise ReadError(f"{table} is not a clean entity table: {len(keys)} rows, {len(set(keys))} distinct keys")
    columns = sum(1 for column in entry["columns"] if column["role"] == "predictor")
    return online.LoadPlan(
        publication.run_id, publication.snapshot_set_sha256, set_hash, len(keys), columns, min(times).astimezone(UTC), max(times).astimezone(UTC), version
    )


class SetHooks:
    """The Feast side of a set load: head verification of the run, ``materialize`` of the set's view, key parsing and the sample comparison."""

    def __init__(self, catalog: reads.IcebergCatalog, publication: reads.Publication, entry: Mapping[str, Any], store: Any) -> None:
        self.catalog, self.publication, self.entry, self.store = catalog, publication, entry, store

    def verify_heads(self, moment: str) -> None:
        reads.verify_heads(self.catalog, self.publication, moment)

    def materialize(self, start: Any, end: Any) -> None:
        self.store.materialize(start_date=start, end_date=end, feature_views=[self.entry["view"]])

    def parse_key(self, key: bytes) -> bool:
        suffix = PROJECT.encode()
        if not key.endswith(suffix):
            return False
        try:
            parsed = importlib.import_module("feast.infra.key_encoding_utils").deserialize_entity_key(key[: -len(suffix)], KEY_VERSION)
        except Exception:
            return False
        return list(parsed.join_keys) == ["spine_key"]

    def verify_samples(self) -> None:
        """Compare online values with the published table for its first and last spine key: null for null, every value equal."""
        table, view = self.entry["table"], self.entry["view"]
        rows = (
            reads.load_table(self.catalog, self.publication.namespace, table)
            .scan(selected_fields=("*",), snapshot_id=self.publication.snapshot_set[table])
            .to_arrow()
            .to_pylist()
        )
        ordered = sorted(rows, key=lambda row: row["spine_key"])
        sample = {row["spine_key"]: row for row in (ordered[0], ordered[-1])}
        names = [column["name"] for column in self.entry["columns"] if column["role"] == "predictor"]
        online_values = self.store.get_online_features(
            features=[f"{view}:{name}" for name in names], entity_rows=[{"spine_key": key} for key in sample], full_feature_names=True
        ).to_dict()
        mismatches = 0
        for name in names:
            got = dict(zip(online_values["spine_key"], online_values[f"{view}__{name}"], strict=True))
            mismatches += sum(1 for key, row in sample.items() if got.get(key) != row[name])
        if mismatches:
            raise ReadError(f"{mismatches} sampled online values differ from the published table ({len(sample)} sample keys)")


def online_loader(
    settings: feast_job.JobSettings,
    environ: MutableMapping[str, str],
    *,
    schema: tuple[dict[str, Any], str] | None = None,
    session: Callable[[], contextlib.AbstractContextManager[reads.IcebergCatalog]] | None = None,
    control: online.RedisClient | None = None,
    features: online.RedisClient | None = None,
    hooks_for: Callable[[reads.IcebergCatalog, reads.Publication, Mapping[str, Any]], online.LoadHooks] | None = None,
    settings_file: Path = feast_job.SETTINGS_FILE,
) -> OnlineLoader:
    """The online load of one set: the protocol of ``online_protocol`` with the set's own database and marker, so a load flushes and marks nothing else."""

    def load(request: LoadRequest) -> str:
        document, _ = schema if schema is not None else reads.read_schema()
        entry = document["model_sets"][request.feature_set]
        layout = online.set_layout(request.feature_set)
        deadline = reads.monotonic_deadline(feast_job.JOB_SECONDS)
        opened = session() if session is not None else feast_job.catalog_session(settings, environ)
        with opened as catalog:
            deadline("selecting the publication")
            publication = reads.read_publication(catalog, reads.NAMESPACES[settings.mode], request.run)
            version = importlib.metadata.version("feast") if hooks_for is None else "test"
            plan = set_load_plan(catalog, publication, entry, request.set_schema_sha256, version)
            hooks = (
                hooks_for(catalog, publication, entry)
                if hooks_for is not None
                else SetHooks(catalog, publication, entry, historical.default_store(environ, settings_file))
            )
            redis_bounds = online.feast_job_redis(environ, database=layout.feature_db) if hooks_for is None else contextlib.nullcontext()
            with redis_bounds:
                result = online.run_load(
                    control if control is not None else online.connect(environ, online.CONTROL_DB),
                    features if features is not None else online.connect(environ, layout.feature_db, decode=False),
                    plan,
                    hooks,
                    deadline=deadline,
                    layout=layout,
                )
        return result.load_id

    return load


def base_run_of(folder: Path) -> str:
    """The base run that the set run read, from the record the publish phase left in the run folder."""
    path = folder / SNAPSHOT_RECORD
    try:
        run = json.loads(path.read_text()).get("base_run")
    except (OSError, ValueError, AttributeError):
        raise ReadError(f"{SNAPSHOT_RECORD} of the run folder is missing or unreadable") from None
    if not isinstance(run, str) or not reads.RUN_ID.fullmatch(run):
        raise ReadError(f"{SNAPSHOT_RECORD} names no base run: a model set run records the base run it read")
    return run


def set_preflight(
    catalog: reads.IcebergCatalog,
    own: reads.Publication,
    base: reads.Publication,
    feature_set: str,
    schema: dict[str, Any],
    backend: feast_job.FeastBackend,
) -> None:
    """Everything that can be refused before the lease is taken for a model set: the repository's views and the two tables the set reads and writes."""
    entry = schema["model_sets"][feature_set]
    table, label = entry["table"], feature_schema.LABEL_TABLE
    problems = reads.view_problems(schema) + feast_job.view_list_problems(schema, backend)
    if table not in own.snapshot_set:
        problems.append(f"{table}: the publication does not name this table")
    if label not in base.snapshot_set:
        problems.append(f"{label}: the base publication does not name this table")
    if table in own.snapshot_set and label in base.snapshot_set:
        wanted = {table: {"columns": entry["columns"]}, label: schema["tables"][label]}
        found = {
            table: reads.iceberg_columns(reads.load_table(catalog, own.namespace, table)),
            label: reads.iceberg_columns(reads.load_table(catalog, base.namespace, label)),
        }
        problems += reads.column_problems({"tables": wanted}, found)
    if problems:
        raise ReadError(f"the repository is refused before apply ({len(problems)} problems): {reads.listed(problems)}")


def set_apply(
    catalog: reads.IcebergCatalog,
    settings: feast_job.JobSettings,
    feature_set: str,
    *,
    schema: dict[str, Any],
    schema_sha256: str,
    backend: feast_job.FeastBackend,
    lease: feast_job.Lease,
    deadline: Callable[[str], None],
) -> dict[str, Any]:
    """Apply the repository against the run of a model set and the base run it read; returns the record."""
    namespace = reads.NAMESPACES[settings.mode]
    deadline("selecting the publications")
    base_run = base_run_of(settings.folder)
    own = reads.read_publication(catalog, namespace, settings.run)
    base = reads.read_publication(catalog, namespace, base_run)
    for publication in (own, base):
        reads.verify_heads(catalog, publication, "before apply")
    deadline("the pre-apply comparison")
    set_preflight(catalog, own, base, feature_set, schema, backend)
    with lease():
        deadline("apply")
        backend.apply()
        deadline("the registry read-back")
        objects = backend.object_list()
        for publication in (own, base):
            reads.verify_heads(catalog, publication, "after apply")

    def summary(publication: reads.Publication) -> dict[str, Any]:
        return {"run_id": publication.run_id, "tables": len(publication.snapshot_set), "snapshot_set_sha256": publication.snapshot_set_sha256}

    return {
        "schema": feast_job.SCHEMA_VERSION,
        "step": settings.step,
        "run_id": settings.run,
        "mode": settings.mode,
        "namespace": namespace,
        "feature_set": feature_set,
        "base_run": base_run,
        "publication": {**summary(own), "committed_at": own.committed_at},
        "base_publication": summary(base),
        "feature_schema_sha256": schema_sha256,
        "set_schema_sha256": feature_schema.set_schema_sha256(feature_set, schema=schema),
        "feast_version": backend.version,
        "objects": {kind: sum(1 for item in objects if item["kind"] == kind) for kind in ("entity", "feature_view", "data_source")},
        "object_list_sha256": feast_job.object_list_sha256(objects),
    }


def set_materialize(
    settings: feast_job.JobSettings,
    feature_set: str,
    *,
    schema: dict[str, Any],
    schema_sha256: str,
    base_run: str,
    production: Production,
    loader: OnlineLoader,
    holds: HoldStore,
) -> dict[str, Any]:
    """Decide and perform what the Feast phase does online for a set; returns the record of ``feast_materialize.json``.

    ``production`` returns the schema hash tag of each Production model of the set; it is called only for a set that is online."""
    set_hash = feature_schema.set_schema_sha256(feature_set, schema=schema) if feature_set in feature_sets.MODEL_SETS else ""
    needs_models = feature_set in feature_sets.MODEL_SETS and bool(schema["model_sets"][feature_set]["online"])
    models = list(production()) if needs_models else []
    action = feature_sets.online_action(feature_set, schema, set_hash, models)
    record: dict[str, Any] = {
        "schema": feast_job.SCHEMA_VERSION,
        "step": "materialize",
        "run_id": settings.run,
        "mode": settings.mode,
        "feature_set": feature_set,
        "base_run": base_run,
        "online": action,
        "feature_schema_sha256": schema_sha256,
        "set_schema_sha256": set_hash,
    }
    request = LoadRequest(feature_set, settings.run, base_run, set_hash)
    if action == feature_sets.HOLD:
        hold = {
            "schema": 1,
            "feature_set": feature_set,
            "run_id": settings.run,
            "base_run": base_run,
            "set_schema_sha256": set_hash,
            "production_set_schema_sha256": sorted(set(models), key=lambda value: value or ""),
        }
        if not holds.write_new(HOLD_NAME, settings.run, hold) and holds.read(HOLD_NAME, settings.run) != hold:
            raise ReadError(f"run {settings.run} already has an online hold record with other content")
    elif action == feature_sets.LOAD:
        record["load_id"] = loader(request)
    return record


def release(
    feature_set: str,
    run: str,
    *,
    schema: dict[str, Any],
    schema_sha256: str,
    holds: HoldStore,
    loader: OnlineLoader,
    newest_run: Callable[[str], str],
    production: Callable[[], Mapping[str, str | None]],
) -> dict[str, Any]:
    """Load the held run of a set online and record the release. A run that was released returns its release record and loads nothing again.

    ``newest_run`` returns the newest committed run of the set: an older held run is refused, because its table is no longer the set's data.
    ``production`` returns the schema hash of each Production model of the set by infection type (``production_reader``). While any of them differs from the
    run's schema hash the release is refused by name and the hold stays, so no model is served online features of another schema: a new schema waits until
    every consumer of the set has promoted a model trained on it (owner decision "Shared online set release", Nov 10 2026)."""
    if feature_sets.checked(feature_set) not in feature_sets.MODEL_SETS or not schema["model_sets"][feature_set]["online"]:
        raise ReadError(f"set {feature_set} is not an online model set: only m1, m2 and m4 have a held online load")
    hold = holds.read(HOLD_NAME, run)
    if hold is None:
        raise ReadError(f"run {run} has no online hold record, so there is nothing to release")
    if hold.get("feature_set") != feature_set:
        raise ReadError(f"the hold of run {run} belongs to set {hold.get('feature_set')}, not to set {feature_set}")
    released = holds.read(RELEASE_NAME, run)
    if released is not None:
        return released
    newest = newest_run(feature_set)
    if newest != run:
        raise ReadError(f"run {run} is not the newest committed run of set {feature_set} ({newest}): its held load is superseded")
    set_hash = feature_schema.set_schema_sha256(feature_set, schema=schema)
    if hold.get("set_schema_sha256") != set_hash:
        raise ReadError(f"the schema of set {feature_set} changed since run {run} was held")
    differing = {hai_type: value for hai_type, value in sorted(production().items()) if value != set_hash}
    if differing:
        named = ", ".join(f"{hai_type} ({value or 'no schema hash'})" for hai_type, value in differing.items())
        raise ReadError(
            f"set {feature_set} stays held: the Production models of {named} are on another schema than run {run} ({set_hash}); "
            "release it once every Production model of the set is trained on this schema"
        )
    load_id = loader(LoadRequest(feature_set, run, str(hold["base_run"]), set_hash))
    record = {
        "schema": 1,
        "feature_set": feature_set,
        "run_id": run,
        "base_run": hold["base_run"],
        "set_schema_sha256": set_hash,
        "feature_schema_sha256": schema_sha256,
        "load_id": load_id,
    }
    if holds.write_new(RELEASE_NAME, run, record):
        return record
    stored = holds.read(RELEASE_NAME, run)
    if stored is None:
        raise ReadError(f"the release record of run {run} cannot be read back")
    return stored


def newest_set_run(catalog: reads.IcebergCatalog, namespace: str, feature_set: str, schema: Mapping[str, Any]) -> str:
    """The newest committed run that publishes the set's table."""
    table = schema["model_sets"][feature_set]["table"]
    rows = reads.load_table(catalog, namespace, reads.RUNS_TABLE).scan(selected_fields=("run_id", "committed_at", "snapshot_set")).to_arrow().to_pylist()
    mine = []
    for row in rows:
        if table in reads.parse_snapshot_set(str(row.get("run_id")), row.get("snapshot_set")):
            mine.append((reads.as_utc(row["committed_at"]), str(row["run_id"])))
    if not mine:
        raise ReadError(f"no committed run publishes {table}")
    return max(mine)[1]


def records_bucket(settings: feast_job.JobSettings) -> str:
    """The data bucket that holds the Production records: the bucket of an S3 registry (the AWS profile), else the deployment file's bucket."""
    location = urllib.parse.urlparse(settings.registry_path)
    return location.netloc if location.scheme == "s3" and location.netloc else reads.deployment_bucket()


def unresolved_promotion(client: Any, bucket: str, models: Mapping[str, str]) -> dict[str, int]:
    """The newest closed lease generation of each model by infection type (0 for a model never promoted), or ReadError while a promotion is unresolved.

    The promote action moves the ``production`` alias before it writes the Production record, so a promotion that failed or was abandoned in between leaves
    the newest record naming the schema production held before. A generation that is open is such a promotion, whatever the set of its version, so any open
    generation of any model refuses (owner decision "Features reader refuses an unresolved promotion", Nov 10 2026). Only the listing is read, never a lease
    record."""
    newest: dict[str, int] = {}
    for hai_type in sorted(models):
        try:
            found = promote_lease.generations(client, bucket, models[hai_type])
        except promote_receipt.ReceiptError as error:
            raise ReadError(f"the promotion leases cannot be read: {error}") from None
        opened = sorted(generation for generation, closed in found.items() if not closed)
        if opened:
            raise ReadError(
                f"the promotion of {models[hai_type]} ({hai_type}) is unresolved: lease generation {opened[-1]} is open, so production may not match its "
                "newest Production record. Retry the promotion run, or resolve it with python -m scripts.training.promote_owner resolve, then run this again"
            )
        newest[hai_type] = max(found, default=0)
    return newest


def stored_production(client: Any, bucket: str, feature_set: str) -> dict[str, str | None]:
    """The schema hash of each Production model of the set by infection type, from the newest Production record of each type in the data bucket.

    The leases are listed first (``unresolved_promotion``). Then the newest record of every type must be the record of its model's newest closed generation:
    a generation that closed without its record (a promotion the owner abandoned after the alias request) or a record newer than every generation the
    listing showed (a promotion that ran in between) is refused, so no older Production state decides the load or the release. With no generation and no
    record for any model, nothing is in production and the set loads (the first release)."""
    names = json.loads(feature_sets.MODEL_CONTRACT.read_text())["registered_models"]
    closed = unresolved_promotion(client, bucket, names)
    try:
        records = production_record.current(client, bucket, names)
    except promote_receipt.ReceiptError as error:
        raise ReadError(f"the Production records cannot be read: {error}") from None
    for hai_type, record in records.items():
        generation = int(record["generation"]) if record is not None else 0
        if generation == closed[hai_type]:
            continue
        if generation < closed[hai_type]:
            raise ReadError(
                f"lease generation {closed[hai_type]} of {names[hai_type]} ({hai_type}) closed with no Production record, so production may not match its "
                "newest record. A completed promotion of the model writes its record and ends this refusal"
            )
        raise ReadError(
            f"the Production record of {hai_type} has generation {generation}, newer than lease generation {closed[hai_type]} of {names[hai_type]}: "
            "a promotion ran while the records were read. Run this again"
        )
    return feature_sets.production_models(records, feature_set)


def production_reader(settings: feast_job.JobSettings, feature_set: str) -> Callable[[], dict[str, str | None]]:
    """Read the Production models of the set from the leases and records in the data bucket when called (the task role lists the lease prefix of each model
    under ``promote/models/`` and reads ``promote/production/`` only)."""
    return lambda: stored_production(promote_receipt.s3_client(), records_bucket(settings), feature_set)


def hold_store(settings: feast_job.JobSettings, environ: Mapping[str, str]) -> HoldStore:
    """The store a release reads and writes: the data bucket's run prefix for a real run, the run folder for a fixture run."""
    if settings.mode == "publish_fixture":
        return RunFolderStore(settings.folder, settings.run)
    region = settings.region or environ.get("AWS_REGION")
    return S3HoldStore(importlib.import_module("boto3").client("s3", region_name=region), reads.deployment_bucket())


def run_set_apply(
    settings: feast_job.JobSettings,
    environ: MutableMapping[str, str],
    feature_set: str,
    *,
    backend: feast_job.FeastBackend | None = None,
    lease: feast_job.Lease | None = None,
    schema: tuple[dict[str, Any], str] | None = None,
    session: Callable[[], contextlib.AbstractContextManager[reads.IcebergCatalog]] | None = None,
) -> dict[str, Any]:
    """The apply step of a model set: the record is written to ``feast_apply.json`` after the apply returned."""
    deadline = reads.monotonic_deadline(feast_job.JOB_SECONDS)
    document, schema_sha256 = schema if schema is not None else reads.read_schema()
    writer_lease = lease if lease is not None else feast_job.protocol_lease(environ)
    with session() if session is not None else feast_job.catalog_session(settings, environ) as catalog:
        record = set_apply(
            catalog,
            settings,
            feature_set,
            schema=document,
            schema_sha256=schema_sha256,
            backend=backend if backend is not None else feast_job.FeastRepository(),
            lease=writer_lease,
            deadline=deadline,
        )
    feast_job.write_record(settings.folder / feast_job.RECORD_NAME, record)
    return record


def run_set_materialize(
    settings: feast_job.JobSettings,
    environ: MutableMapping[str, str],
    feature_set: str,
    *,
    schema: tuple[dict[str, Any], str] | None = None,
    production: Production | None = None,
    loader: OnlineLoader | None = None,
    holds: HoldStore | None = None,
) -> dict[str, Any]:
    """The materialize step of a set: decide online, offline or hold, and write ``feast_materialize.json``."""
    document, schema_sha256 = schema if schema is not None else reads.read_schema()
    base_run = base_run_of(settings.folder) if feature_set in feature_sets.MODEL_SETS else ""
    record = set_materialize(
        settings,
        feature_set,
        schema=document,
        schema_sha256=schema_sha256,
        base_run=base_run,
        production=production if production is not None else (lambda: list(production_reader(settings, feature_set)().values())),
        loader=loader if loader is not None else online_loader(settings, environ, schema=(document, schema_sha256)),
        holds=holds if holds is not None else RunFolderStore(settings.folder, settings.run),
    )
    feast_job.write_record(settings.folder / "feast_materialize.json", record)
    return record
