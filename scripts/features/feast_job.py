"""Apply the Feast repository against one verified features publication (design p3c sections 3.3 to 3.5 and 4.5).

Run inside the Feast image by the launcher template ``pipeline_feast``::

    python -m scripts.features.feast_job apply --run <run id> --mode <publish_fixture|real>

``apply`` does these things in this order, and every wait is bounded (the whole job by ``JOB_SECONDS`` between phases, Polaris by 10 s per request):

1. Select the head ``publish_runs`` row R of the mode's namespace and verify that ``main`` of every table it names is at R (``publication_reads``).
2. Compare each table's Iceberg schema with ``data_contracts/features/feature_schema.json`` and check the view lists, before anything is written.
3. Take the Redis lease (``scripts.features.online_protocol.writer_lease``, step 5), run Feast's apply, read the registry back without its cache and
   record the sorted object list.
4. Verify every head again, then release the lease.

Real runs exchange the reader principal's client ID and secret for a Polaris token and write a private ``.pyiceberg.yaml`` for catalog ``hai`` (design 3.3).
The registry is named by ``FEAST_REGISTRY_KEY`` (a closed key per step) and the bucket field of the mounted deployment file; fixture runs name a registry
file in the run folder by ``FEAST_REGISTRY_PATH``. The job never calls Feast's store removal command: the registry and the online keys are only added to
or updated. ``materialize`` and ``probe`` are accepted on the command line (lane B renders them) and exit 2 until steps 5 and S2 build them.

The record ``feast_apply.json`` and every message hold names, counts and hashes only.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib
import importlib.metadata
import json
import os
import sys
import tempfile
from collections.abc import Callable, Iterator, MutableMapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from scripts.features import fixture_catalog
from scripts.features import online_protocol as online
from scripts.features import publication_reads as reads
from scripts.features.publication_reads import ReadError

REPO = reads.REPO_ROOT / "feature_store"
SETTINGS_FILE = REPO / "feature_store.yaml"
RECORD_NAME = "feast_apply.json"
SCHEMA_VERSION = 1
STEPS = ("apply", "materialize", "probe")
BUILT_STEPS = ("apply", "materialize")
JOB_SECONDS = 1800
DEFAULT_RUN_FOLDER = "/workspace/out"
SUMMARY_KEYS = ("step", "mode", "publication", "objects", "object_list_sha256", "load_id", "entities")
LEASE_MODULE = "scripts.features.online_protocol"
LEASE_ENTRY = "writer_lease"
Lease = Callable[[], contextlib.AbstractContextManager[None]]


class FeastBackend(Protocol):
    """What the job needs from Feast; the real implementation is ``FeastRepository``, the cases pass a recording stand-in."""

    version: str

    def registered_views(self) -> dict[str, tuple[bool, tuple[str, ...]]]:
        """Feature view name to (online, column names) as parsed from the repository, without touching any store."""
        ...

    def apply(self) -> None:
        """Run Feast's apply with source validation off (the job compares the schemas itself)."""
        ...

    def object_list(self) -> list[dict[str, Any]]:
        """The registry's entities, feature views and data sources, read without the registry cache."""
        ...


@dataclass(frozen=True)
class JobSettings:
    step: str
    run: str
    mode: str
    folder: Path
    registry_path: str
    region: str


class FeastRepository:
    """The real Feast backend. Feast is imported when a method needs it, so the host checks import no Feast."""

    def __init__(
        self, repo: Path = REPO, settings_file: Path = SETTINGS_FILE, version: str | None = None, environ: MutableMapping[str, str] | None = None
    ) -> None:
        self.environ = os.environ if environ is None else environ
        self.repo = repo.resolve()
        self.settings_file = settings_file
        self.version = version if version is not None else importlib.metadata.version("feast")

    @contextlib.contextmanager
    def loaded(self) -> Iterator[None]:
        """Run Feast's repository loading the way ``feast apply`` does, whatever the process was started with.

        Feast imports the repository's modules by a name relative to the working directory, so the working directory is the repository folder and that folder
        is on ``sys.path`` (a process started with ``-m`` has a fixed path entry, one started with ``-c`` a moving one). The modules read the environment when
        they are imported (the catalog URI and warehouse), so cached ones are dropped first: a second apply in one process sees its own environment.
        """
        previous = os.getcwd()
        folder = str(self.repo)
        for name in [name for name, module in sys.modules.items() if str(getattr(module, "__file__", "") or "").startswith(folder + os.sep)]:
            del sys.modules[name]
        if folder not in sys.path:
            sys.path.insert(0, folder)
        os.chdir(folder)
        try:
            yield
        finally:
            os.chdir(previous)

    def registered_views(self) -> dict[str, tuple[bool, tuple[str, ...]]]:
        parse_repo = importlib.import_module("feast.repo_operations").parse_repo
        with self.loaded():
            contents = parse_repo(self.repo)
        return {view.name: (bool(view.online), tuple(sorted(item.name for item in view.features))) for view in contents.feature_views}

    def apply(self) -> None:
        operations = importlib.import_module("feast.repo_operations")
        config = importlib.import_module("feast.repo_config").load_repo_config(self.repo, self.settings_file)
        with online.feast_job_redis(self.environ), self.loaded():
            operations.apply_total(config, self.repo, skip_source_validation=True)

    def object_list(self) -> list[dict[str, Any]]:
        options = {} if self.settings_file == SETTINGS_FILE else {"fs_yaml_file": self.settings_file}
        store = importlib.import_module("feast").FeatureStore(repo_path=str(self.repo), **options)
        objects: list[dict[str, Any]] = []
        for entity in store.list_entities(allow_cache=False):
            objects.append({"kind": "entity", "name": entity.name, "join_key": entity.join_key})
        for view in store.list_feature_views(allow_cache=False):
            objects.append(
                {
                    "kind": "feature_view",
                    "name": view.name,
                    "entities": sorted(view.entities),
                    "online": bool(view.online),
                    "ttl_seconds": view.ttl.total_seconds() if view.ttl is not None else None,
                    "features": sorted([item.name, str(item.dtype)] for item in view.features),
                    "source": view.batch_source.name,
                }
            )
        for source in store.list_data_sources(allow_cache=False):
            objects.append(
                {
                    "kind": "data_source",
                    "name": source.name,
                    "catalog_type": source.catalog_type,
                    "namespace": source.namespace,
                    "table": source.iceberg_table,
                    "timestamp_field": source.timestamp_field,
                }
            )
        return sorted(objects, key=lambda item: (item["kind"], item["name"]))


def object_list_sha256(objects: Sequence[dict[str, Any]]) -> str:
    """SHA-256 of the canonical, sorted object list. Feast stores update times, so registry bytes differ between equal applies; this digest does not."""
    ordered = sorted(objects, key=lambda item: (item["kind"], item["name"]))
    return hashlib.sha256(json.dumps(ordered, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def job_settings(environ: MutableMapping[str, str], step: str, run: str, mode: str, deployment: Path = reads.DEPLOYMENT_PATH) -> JobSettings:
    """Check the run, the mode and the registry settings against their closed tables (failure modes LC20 to LC23 and LC28)."""
    if not reads.RUN_ID.fullmatch(run):
        raise ReadError("the run ID is malformed")
    if mode not in reads.MODES:
        raise ReadError("the run mode is unknown")
    folder = environ.get("FEAST_RUN_FOLDER", DEFAULT_RUN_FOLDER).rstrip("/")
    if mode == "publish_fixture":
        if environ.get("FEAST_REGISTRY_KEY"):
            raise ReadError("a fixture run names its registry by file, not by S3 key")
        path = environ.get("FEAST_REGISTRY_PATH", "")
        if not path.startswith(f"{folder}/") or ".." in path:
            raise ReadError("the fixture registry file is not inside the run folder")
        return JobSettings(step, run, mode, Path(folder), path, "")
    if environ.get("FEAST_REGISTRY_PATH"):
        raise ReadError("a real run names its registry by S3 key only; FEAST_REGISTRY_PATH is set")
    region = environ.get("AWS_REGION", "")
    missing = sorted(
        name for name in ("FEAST_REGISTRY_KEY", "AWS_PROFILE", "AWS_REGION", "POLARIS_CLIENT_ID", "POLARIS_CLIENT_SECRET") if not environ.get(name)
    )
    if missing:
        raise ReadError(f"a real run needs {', '.join(missing)}")
    uri = reads.registry_uri(step, environ["FEAST_REGISTRY_KEY"], reads.deployment_bucket(deployment))
    return JobSettings(step, run, mode, Path(folder), uri, region)


def prepare_environment(settings: JobSettings, environ: MutableMapping[str, str], token: str) -> None:
    """Set the variables that ``feature_store.yaml`` and ``feature_store/definitions.py`` read, and drop the client secret once it is exchanged."""
    environ["FEAST_REGISTRY_PATH"] = settings.registry_path
    environ["FEAST_RUN_FOLDER"] = str(settings.folder)
    if settings.mode == "real":
        environ["FEAST_CATALOG_ENDPOINT"] = reads.POLARIS_ENDPOINT
        environ["FEAST_POLARIS_TOKEN"] = token
        environ.pop("POLARIS_CLIENT_SECRET", None)
        environ.pop("POLARIS_CLIENT_ID", None)
    else:
        sql = reads.catalog_settings(environ, settings.mode)
        environ["FEAST_SQL_CATALOG_URI"] = sql.sql_uri
        environ["FEAST_SQL_WAREHOUSE"] = sql.sql_warehouse
        (settings.folder / "feast").mkdir(parents=True, exist_ok=True)


def write_record(path: Path, document: dict[str, Any]) -> None:
    """Write the record atomically (a temporary sibling, then a rename)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(dir=path.parent, prefix=path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(handle, "w") as stream:
            stream.write(json.dumps(document, indent=2, sort_keys=True) + "\n")
        os.replace(temporary, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temporary)
        raise


def preflight(catalog: reads.IcebergCatalog, publication: reads.Publication, schema: dict[str, Any], backend: FeastBackend) -> None:
    """Everything that can be refused before the lease is taken and Feast writes (design 3.4 items 2 and 3); raises with the problems listed."""
    problems = reads.view_problems(schema)
    named = set(publication.snapshot_set)
    problems += [f"{name}: the publication does not name this table" for name in sorted(set(schema["tables"]) - named)]
    columns = {name: reads.iceberg_columns(reads.load_table(catalog, publication.namespace, name)) for name in sorted(set(schema["tables"]) & named)}
    problems += reads.column_problems(schema, {**columns, **{name: {} for name in set(schema["tables"]) - named}})
    registered = backend.registered_views()
    predictors, labels = reads.reference_lists(schema)
    online = sorted(f"{view}:{column}" for view, (is_online, view_columns) in registered.items() if is_online for column in view_columns)
    offline = sorted(f"{view}:{column}" for view, (is_online, view_columns) in registered.items() if not is_online for column in view_columns)
    if online != predictors:
        problems.append("the online feature views of the repository differ from the registered predictors of the schema")
    if offline != labels:
        problems.append("the offline-only views of the repository differ from the label columns of the schema")
    if problems:
        raise ReadError(f"the repository is refused before apply ({len(problems)} problems): {reads.listed(problems)}")


def apply(
    catalog: reads.IcebergCatalog,
    settings: JobSettings,
    *,
    schema: dict[str, Any],
    schema_sha256: str,
    backend: FeastBackend,
    lease: Lease,
    deadline: Callable[[str], None],
) -> dict[str, Any]:
    """Apply the repository to one verified publication and return the record (also written to ``feast_apply.json`` by ``run``)."""
    namespace = reads.NAMESPACES[settings.mode]
    deadline("selecting the publication")
    publication = reads.read_publication(catalog, namespace)
    reads.verify_heads(catalog, publication, "before apply")
    deadline("the pre-apply comparison")
    preflight(catalog, publication, schema, backend)
    with lease():
        deadline("apply")
        backend.apply()
        deadline("the registry read-back")
        objects = backend.object_list()
        reads.verify_heads(catalog, publication, "after apply")
    return {
        "schema": SCHEMA_VERSION,
        "step": settings.step,
        "run_id": settings.run,
        "mode": settings.mode,
        "namespace": namespace,
        "publication": {
            "run_id": publication.run_id,
            "committed_at": publication.committed_at,
            "tables": len(publication.snapshot_set),
            "snapshot_set_sha256": publication.snapshot_set_sha256,
        },
        "feature_schema_sha256": schema_sha256,
        "feast_version": backend.version,
        "objects": {kind: sum(1 for item in objects if item["kind"] == kind) for kind in ("entity", "feature_view", "data_source")},
        "object_list_sha256": object_list_sha256(objects),
    }


def protocol_lease(environ: MutableMapping[str, str]) -> Lease:
    """The writer lease of the online protocol (step 5): ``scripts.features.online_protocol.writer_lease(environ)`` returns a context manager."""
    try:
        entry = importlib.import_module("scripts.features.online_protocol").writer_lease
    except (ImportError, AttributeError):
        raise ReadError(f"the Redis lease ({LEASE_MODULE}.{LEASE_ENTRY}) is not built; apply does not run without it") from None

    def acquire() -> contextlib.AbstractContextManager[None]:
        lease: contextlib.AbstractContextManager[None] = entry(environ)
        return lease

    return acquire


@contextlib.contextmanager
def catalog_session(
    settings: JobSettings,
    environ: MutableMapping[str, str],
    *,
    loader: Callable[..., Any] | None = None,
    exchange: Callable[[str, str, str], str] | None = None,
    register: bool = True,
) -> Iterator[reads.IcebergCatalog]:
    """Prepare the environment and open the catalog; real runs exchange the token and hold the private PyIceberg settings for the whole job.

    A fixture run first registers the committed publication of the local warehouse in its SQL catalog (``fixture_catalog``), so the reader path finds it;
    ``register=False`` is for the image cases that build their tables in the SQL catalog themselves. ``loader`` and ``exchange`` replace PyIceberg's
    ``load_catalog`` and the Polaris token exchange in the cases.
    """
    with contextlib.ExitStack() as stack:
        token = ""
        if settings.mode == "real":
            stack.enter_context(reads.private_pyiceberg_home(settings.region, environ))
            token = (exchange if exchange is not None else reads.polaris_token)(
                reads.POLARIS_ENDPOINT, environ["POLARIS_CLIENT_ID"], environ["POLARIS_CLIENT_SECRET"]
            )
        prepare_environment(settings, environ, token)
        missing = reads.unresolved_variables(SETTINGS_FILE.read_text(), environ)
        if missing:
            raise ReadError(f"feature_store.yaml variables are unset or unresolved: {', '.join(missing)}")
        catalog_settings = reads.catalog_settings(environ, settings.mode, bearer=token)
        catalog = reads.open_catalog(catalog_settings, loader)
        if settings.mode == "publish_fixture" and register:
            registering: Any = catalog
            fixture_catalog.register_publication(registering, fixture_catalog.warehouse_folder(catalog_settings.sql_warehouse), catalog_settings.namespace)
        yield catalog


def run(
    settings: JobSettings,
    environ: MutableMapping[str, str],
    *,
    backend: FeastBackend | None = None,
    lease: Lease | None = None,
    schema: tuple[dict[str, Any], str] | None = None,
    loader: Callable[..., Any] | None = None,
    exchange: Callable[[str, str, str], str] | None = None,
    register: bool = True,
    session: Callable[[], contextlib.AbstractContextManager[reads.IcebergCatalog]] | None = None,
) -> dict[str, Any]:
    """Run the apply step for the settings and write its record into the run folder after the apply returned.

    ``session`` replaces ``catalog_session`` (the AWS profile prepares its own environment and catalog).
    """
    deadline = reads.monotonic_deadline(JOB_SECONDS)
    document, schema_sha256 = schema if schema is not None else reads.read_schema()
    writer_lease = lease if lease is not None else protocol_lease(environ)
    with session() if session is not None else catalog_session(settings, environ, loader=loader, exchange=exchange, register=register) as catalog:
        record = apply(
            catalog,
            settings,
            schema=document,
            schema_sha256=schema_sha256,
            backend=backend if backend is not None else FeastRepository(),
            lease=writer_lease,
            deadline=deadline,
        )
    write_record(settings.folder / RECORD_NAME, record)
    return record


def parse(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("step", choices=STEPS)
    parser.add_argument("--run", required=True)
    parser.add_argument("--mode", required=True, choices=reads.MODES)
    return parser.parse_args(list(argv))


def main(argv: Sequence[str] | None = None, environ: MutableMapping[str, str] | None = None) -> int:
    """Exit 0 when the step finished, 1 when it refused or failed (the message names the cause), 2 for a step that is not built yet."""
    args = parse(sys.argv[1:] if argv is None else argv)
    if args.step not in BUILT_STEPS:
        sys.stderr.write(f"feast_job {args.step}: this step is built in a later step of the package (probe: spike S2)\n")
        return 2
    live = os.environ if environ is None else environ
    try:
        from scripts.features import feast_aws

        if feast_aws.selected_profile(live) == "aws":
            record = feast_aws.run_job(args.step, args.run, args.mode, live)
            sys.stdout.write(json.dumps({key: record[key] for key in record if key in SUMMARY_KEYS}, sort_keys=True) + "\n")
            return 0
        settings = job_settings(live, args.step, args.run, args.mode)
        if args.step == "materialize":
            from scripts.features import feast_materialize

            record = feast_materialize.run_materialize(settings, live)
        else:
            record = run(settings, live)
    except ReadError as error:
        sys.stderr.write(f"feast_job {args.step} failed: {error}\n")
        return 1
    sys.stdout.write(json.dumps({key: record[key] for key in record if key in SUMMARY_KEYS}, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
