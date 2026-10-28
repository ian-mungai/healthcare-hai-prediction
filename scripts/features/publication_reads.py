"""Read the features publication consistently: pick the committed row, verify every table head, open the catalog (design p3c sections 3.3 and 3.5).

The Feast offline store reads the current ``main`` of a table when it loads it (design fact F1), and a publisher can fast-forward some tables and stop
before it writes its ``publish_runs`` row. So an unchanged ``publish_runs`` head does not prove that the tables match it. Every reader of the features
namespace (``feast_job``, ``historical``) selects one row R, loads each table R names fresh from the catalog and requires its ``main`` snapshot to equal
R's, before it reads and again after it has fully consumed its result (``consistent_read``). ``main`` only moves forward, so equal heads before and after
mean every table stayed at R during the read.

This module imports neither Feast nor PyIceberg at load time (``open_catalog`` imports PyIceberg when it is called), so the host checks run in ``.venv``.
It records and raises names, counts, hashes and snapshot IDs only, never row values, tokens or secrets.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib
import json
import os
import re
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator, Mapping, MutableMapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = REPO_ROOT / "data_contracts/features/feature_schema.json"
DEPLOYMENT_PATH = Path("/workspace/infra/deployment.auto.tfvars.json")

CATALOG_NAME = "hai"
WAREHOUSE = "hai_lakehouse"
RUNS_TABLE = "publish_runs"
SPINE_TABLE = "int_hospital_spine"
MODES = ("publish_fixture", "real")
NAMESPACES = {"publish_fixture": "features_e2e", "real": "features"}
CATALOG_TYPES = {"publish_fixture": "sql", "real": "rest"}
MODE_OF_NAMESPACE = {namespace: mode for mode, namespace in NAMESPACES.items()}
POLARIS_ENDPOINT = "http://polaris:8181/api/catalog"
TOKEN_PATH = "/v1/oauth/tokens"  # noqa: S105 - a URL path, not a secret
READER_ROLE = "lakehouse_reader"
TOKEN_TIMEOUT_SECONDS = 10.0
TOKEN_ATTEMPTS = 2
S3_PROXY = "http://hai-egress:3128"
S3_CONNECT_TIMEOUT = 5
S3_REQUEST_TIMEOUT = 30
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")
REGION = re.compile(r"[a-z]{2}(-[a-z]+)+-[0-9]")
BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
VARIABLE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)")
# The S3 registry objects a Feast job may write (design 6.5 and section 7 item 4): one for the real registry, one for the probe.
REGISTRY_KEYS = {"apply": "lakehouse/feast/registry.pb", "materialize": "lakehouse/feast/registry.pb", "probe": "lakehouse/feast_e2e/registry.pb"}
# Iceberg type of each schema file type (the file names the SQL type the dbt contract uses).
ICEBERG_TYPES = {"varchar": "string", "double": "double", "bigint": "long", "boolean": "boolean", "timestamp with time zone": "timestamptz"}
FEAST_TYPES = ("Float64", "Int64", "String", "Bool")
LISTED_MAX = 5


class ReadError(RuntimeError):
    """A read is refused or fails; the message carries names, counts and snapshot IDs, never row values or credentials."""


class Scan(Protocol):
    def to_arrow(self) -> Any: ...


class IcebergTable(Protocol):
    def snapshot_by_name(self, name: str) -> Any: ...

    def scan(self, *, selected_fields: tuple[str, ...], snapshot_id: int | None = None) -> Scan: ...

    def schema(self) -> Any: ...


class IcebergCatalog(Protocol):
    def load_table(self, identifier: str) -> IcebergTable: ...


@dataclass(frozen=True)
class Publication:
    """One committed features set R: the ``publish_runs`` row and the snapshot ID of ``main`` that it names for each table."""

    namespace: str
    run_id: str
    committed_at: str
    snapshot_set: Mapping[str, int]

    @property
    def snapshot_set_sha256(self) -> str:
        return hashlib.sha256(json.dumps(dict(sorted(self.snapshot_set.items())), separators=(",", ":")).encode()).hexdigest()


def listed(items: Sequence[str]) -> str:
    """At most five names or messages, then the count of the rest."""
    shown = "; ".join(items[:LISTED_MAX])
    return shown if len(items) <= LISTED_MAX else f"{shown}; and {len(items) - LISTED_MAX} more"


def load_table(catalog: IcebergCatalog, namespace: str, name: str) -> IcebergTable:
    """Load a table fresh from the catalog; any failure names the table and the error type, not its text."""
    try:
        return catalog.load_table(f"{namespace}.{name}")
    except Exception as error:
        raise ReadError(f"{namespace}.{name} cannot be loaded from the catalog ({type(error).__name__})") from None


def as_utc(value: Any) -> datetime:
    """A commit time as an aware UTC datetime; a naive time is read as UTC, a text time as ISO 8601."""
    moment = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(moment, datetime):
        raise ValueError("not a time")
    return moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)


def parse_snapshot_set(run_id: str, raw: Any) -> dict[str, int]:
    """The table-to-snapshot map of one row; anything but a non-empty object of identifiers and positive integers is refused."""
    try:
        document = json.loads(raw) if isinstance(raw, str | bytes) else raw
    except ValueError:
        raise ReadError(f"publish run {run_id}: snapshot_set is not valid JSON") from None
    if not isinstance(document, dict) or not document:
        raise ReadError(f"publish run {run_id}: snapshot_set is not a non-empty object")
    parsed: dict[str, int] = {}
    for name, snapshot in document.items():
        if not isinstance(name, str) or not IDENT.fullmatch(name):
            raise ReadError(f"publish run {run_id}: snapshot_set names a table that is not a plain identifier")
        if isinstance(snapshot, bool) or not isinstance(snapshot, int) or snapshot <= 0:
            raise ReadError(f"publish run {run_id}: snapshot_set gives {name} a snapshot ID that is not a positive integer")
        parsed[name] = snapshot
    return parsed


def read_publication(catalog: IcebergCatalog, namespace: str, run_id: str | None = None) -> Publication:
    """The head ``publish_runs`` row, or the row of the named run.

    The head is the newest ``committed_at``, ties broken by the larger ``run_id``: the order of the gold head query in ``gold_publish.py``.
    """
    table = load_table(catalog, namespace, RUNS_TABLE)
    rows = table.scan(selected_fields=("run_id", "committed_at", "snapshot_set")).to_arrow().to_pylist()
    if not rows:
        raise ReadError(f"{namespace}.{RUNS_TABLE} has no committed publication")
    parsed = []
    for row in rows:
        identity = row.get("run_id")
        if not isinstance(identity, str) or not RUN_ID.fullmatch(identity):
            raise ReadError(f"{namespace}.{RUNS_TABLE} holds a row whose run_id is not a run ID")
        try:
            committed = as_utc(row.get("committed_at"))
        except (TypeError, ValueError):
            raise ReadError(f"publish run {identity}: committed_at is not a time") from None
        parsed.append((committed, identity, row.get("snapshot_set")))
    if run_id is None:
        committed, identity, raw = max(parsed, key=lambda item: (item[0], item[1]))
    else:
        matches = [item for item in parsed if item[1] == run_id]
        if len(matches) != 1:
            raise ReadError(f"{namespace}.{RUNS_TABLE} has {len(matches)} rows for run {run_id}; exactly one is required")
        committed, identity, raw = matches[0]
    return Publication(namespace, identity, committed.isoformat(), parse_snapshot_set(identity, raw))


def verify_heads(catalog: IcebergCatalog, publication: Publication, moment: str) -> None:
    """Require the ``main`` snapshot of every table of R to equal R's; each table is loaded fresh, and all differences are listed."""
    differences = []
    for name, expected in sorted(publication.snapshot_set.items()):
        snapshot = load_table(catalog, publication.namespace, name).snapshot_by_name("main")
        if snapshot is None:
            differences.append(f"{name} has no main snapshot")
        elif int(snapshot.snapshot_id) != expected:
            differences.append(f"{name}: main is {snapshot.snapshot_id}, publication {publication.run_id} names {expected}")
    if differences:
        raise ReadError(f"{moment}: {len(differences)} of {len(publication.snapshot_set)} tables are not at the publication ({listed(differences)})")


def consistent_read[T](catalog: IcebergCatalog, publication: Publication, read: Callable[[], T]) -> T:
    """Verify the heads, run ``read``, verify the heads again and return the result.

    ``read`` must return a fully consumed (materialized) result: a lazy result would be consumed after the second check. An error in ``read``
    propagates unchanged and the second check does not run. A difference at the second check raises, so the caller never receives the result.
    """
    verify_heads(catalog, publication, "before the read")
    result = read()
    verify_heads(catalog, publication, "after the read")
    return result


def read_schema(path: Path = SCHEMA_PATH) -> tuple[dict[str, Any], str]:
    """The committed feature schema and its SHA-256 (``feature_schema_sha256``: the digest of the file's bytes)."""
    try:
        data = path.read_bytes()
        document = json.loads(data)
    except (OSError, ValueError):
        raise ReadError(f"{path.name} is missing or not valid JSON") from None
    if not isinstance(document, dict) or not isinstance(document.get("tables"), dict) or not isinstance(document.get("servable"), list):
        raise ReadError(f"{path.name} has no tables or servable list")
    return document, hashlib.sha256(data).hexdigest()


def base_control(entry: str) -> str:
    """The lower-case column prefix of an excluded control entry such as ``C283.benchmark_category`` (the whole control is excluded)."""
    return re.sub(r"[^a-z0-9]", "_", entry.split(".", 1)[0].lower())


def reference_lists(schema: Mapping[str, Any]) -> tuple[list[str], list[str]]:
    """The predictor references (``<view>:<column>``) and the label references that the schema registers, sorted."""
    predictors, labels = [], []
    for table in schema["tables"].values():
        for column in table["columns"]:
            if column["role"] == "predictor":
                predictors.append(f"{column['view']}:{column['name']}")
            elif column["role"] == "label":
                labels.append(f"{column['view']}:{column['name']}")
    return sorted(predictors), sorted(labels)


def view_problems(schema: Mapping[str, Any]) -> list[str]:
    """Schema-level problems of the registered views: a predictor view with a label, identifier, key or excluded control, or a servable list that
    differs from the registered predictors (failure mode LC11; the Feast-level half compares the parsed repository)."""
    problems = []
    excluded = {base_control(entry) for entry in schema.get("excluded_controls", [])}
    predictors, _ = reference_lists(schema)
    for name, table in schema["tables"].items():
        for column in table["columns"]:
            role, view, column_name = column["role"], column.get("view"), column["name"]
            if role in ("key", "event_time", "identifier") and view is not None:
                problems.append(f"{name}.{column_name}: a {role} column is registered in view {view}")
            if role == "predictor":
                if table["role"] != "predictor" or view != name:
                    problems.append(f"{name}.{column_name}: a predictor column sits outside its own predictor view")
                if column.get("feast_type") not in FEAST_TYPES:
                    problems.append(f"{name}.{column_name}: the Feast type is not one of {', '.join(FEAST_TYPES)}")
                if column_name.split("__", 1)[0] in excluded:
                    problems.append(f"{name}.{column_name}: the control is excluded from predictors")
            if role == "label" and (table["role"] != "label" or not str(view).startswith("label_")):
                problems.append(f"{name}.{column_name}: a label column sits outside a label view")
    if predictors != sorted(schema["servable"]):
        problems.append("the servable list differs from the registered predictor columns")
    return problems


def column_problems(schema: Mapping[str, Any], iceberg: Mapping[str, Mapping[str, str]]) -> list[str]:
    """Differences between the schema file and the Iceberg schemas (table to column to type text): names only, no values."""
    problems = []
    for name, table in sorted(schema["tables"].items()):
        if name not in iceberg:
            problems.append(f"{name}: the table is missing")
            continue
        expected = {column["name"]: ICEBERG_TYPES.get(column["type"], f"unmapped:{column['type']}") for column in table["columns"]}
        actual = dict(iceberg[name])
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        retyped = sorted(column for column in set(expected) & set(actual) if expected[column] != actual[column])
        for label, columns in (("missing", missing), ("extra", extra), ("with another type", retyped)):
            if columns:
                problems.append(f"{name}: {len(columns)} columns {label} ({listed(columns)})")
    return problems


def iceberg_columns(table: IcebergTable) -> dict[str, str]:
    """Column name to Iceberg type text of one table (``string``, ``long``, ``double``, ``boolean``, ``timestamptz``, ``date``, ``int``)."""
    return {field_.name: str(field_.field_type) for field_ in table.schema().fields}


def unresolved_variables(text: str, environ: Mapping[str, str]) -> list[str]:
    """Names of the variables in a Feast settings text that are unset, empty or still literal (Feast leaves an unset ``${NAME}`` as it is, fact F7)."""
    names = {first or second for first, second in VARIABLE.findall(text)}
    return sorted(name for name in names if not environ.get(name) or "${" in environ[name])


def registry_uri(step: str, key: str, bucket: str) -> str:
    """The S3 URI of the registry a step may write: the key must be the step's own closed key, the bucket a plain bucket name."""
    if REGISTRY_KEYS.get(step) != key:
        raise ReadError(f"the registry key is not the one key that step {step} may use")
    if not BUCKET.fullmatch(bucket) or ".." in bucket:
        raise ReadError("data_bucket_name is not a bucket name")
    return f"s3://{bucket}/{key}"


def deployment_bucket(path: Path = DEPLOYMENT_PATH) -> str:
    """The data bucket name from the mounted rendered Terraform variables (field ``data_bucket_name``)."""
    try:
        document = json.loads(path.read_text())
    except (OSError, ValueError):
        raise ReadError("the deployment file is missing or not valid JSON") from None
    bucket = document.get("data_bucket_name") if isinstance(document, dict) else None
    if not isinstance(bucket, str) or not BUCKET.fullmatch(bucket):
        raise ReadError("the deployment file has no valid data_bucket_name")
    return bucket


def polaris_token(
    endpoint: str,
    client_id: str,
    client_secret: str,
    *,
    timeout: float = TOKEN_TIMEOUT_SECONDS,
    attempts: int = TOKEN_ATTEMPTS,
    opener: urllib.request.OpenerDirector | None = None,
) -> str:
    """Exchange the reader's client ID and secret for a Polaris token: 10 s per request and at most 2 requests, and only a connection failure is tried again.

    The request bypasses the egress proxy settings of the environment: Polaris is on the run's own network. Errors name the principal role and the HTTP
    status, never the credentials.
    """
    url = endpoint.rstrip("/") + TOKEN_PATH
    if urllib.parse.urlsplit(url).scheme != "http":
        raise ReadError("the Polaris endpoint is not an http URL")
    form = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret, "scope": "PRINCIPAL_ROLE:ALL"})
    request = urllib.request.Request(  # noqa: S310 - scheme checked above
        url, data=form.encode(), headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded", "Polaris-Realm": "POLARIS"}
    )
    client = opener if opener is not None else urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for _ in range(attempts):
        try:
            with client.open(request, timeout=timeout) as response:
                body = response.read()
        except urllib.error.HTTPError as error:
            raise ReadError(f"Polaris refused the {READER_ROLE} credentials (HTTP {error.code})") from None
        except (urllib.error.URLError, TimeoutError, OSError):
            continue
        try:
            token = json.loads(body).get("access_token")
        except (ValueError, AttributeError):
            raise ReadError(f"Polaris answered the {READER_ROLE} token request with an unreadable body") from None
        if not isinstance(token, str) or not token:
            raise ReadError(f"Polaris answered the {READER_ROLE} token request without a token")
        return token
    raise ReadError(f"Polaris did not answer the {READER_ROLE} token request within {timeout:g} s ({attempts} attempts)")


def pyiceberg_settings_text(region: str) -> str:
    """The content of the private ``.pyiceberg.yaml`` for catalog ``hai`` (design 3.3). It holds no secret: the token travels in the job environment."""
    if not REGION.fullmatch(region):
        raise ReadError("AWS_REGION is not a region name")
    return (
        f"catalog:\n  {CATALOG_NAME}:\n    s3.region: {region}\n    s3.proxy-uri: {S3_PROXY}\n"
        f"    s3.connect-timeout: {S3_CONNECT_TIMEOUT}\n    s3.request-timeout: {S3_REQUEST_TIMEOUT}\n"
    )


@contextlib.contextmanager
def private_pyiceberg_home(region: str, environ: MutableMapping[str, str]) -> Iterator[Path]:
    """Write ``.pyiceberg.yaml`` (mode 0600) in a private temporary folder (0700), point ``PYICEBERG_HOME`` at it and remove both on exit.

    PyIceberg reads its configuration when ``pyiceberg.catalog`` is first imported, so a file written after that is ignored without a message; this
    refuses instead.
    """
    if "pyiceberg.catalog" in sys.modules:
        raise ReadError("PyIceberg already loaded its configuration; the private settings file would be ignored")
    text = pyiceberg_settings_text(region)
    folder = Path(tempfile.mkdtemp(prefix="pyiceberg_home_"))
    previous = environ.get("PYICEBERG_HOME")
    try:
        folder.chmod(0o700)
        descriptor = os.open(folder / ".pyiceberg.yaml", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as handle:
            handle.write(text)
        environ["PYICEBERG_HOME"] = str(folder)
        yield folder
    finally:
        if previous is None:
            environ.pop("PYICEBERG_HOME", None)
        else:
            environ["PYICEBERG_HOME"] = previous
        shutil.rmtree(folder, ignore_errors=True)


@dataclass(frozen=True)
class CatalogSettings:
    """The closed settings of one run's catalog: the mode fixes the namespace and the catalog type."""

    mode: str
    namespace: str
    catalog_type: str
    endpoint: str = ""
    sql_uri: str = ""
    sql_warehouse: str = ""
    token: str = field(default="", repr=False)


def catalog_settings(environ: Mapping[str, str], mode: str, *, token: str = "") -> CatalogSettings:  # noqa: S107 - the default is "no token", not a password
    """Check the catalog settings of the environment against the mode's closed table and return them.

    A fixture run (``publish_fixture``) reads the SQL catalog in its run folder and holds no AWS profile or Polaris credential (failure mode CR6); a real
    run reads the Polaris REST catalog at its one endpoint with a token.
    """
    if mode not in MODES:
        raise ReadError("the run mode is unknown")
    namespace, catalog_type = environ.get("FEAST_NAMESPACE", ""), environ.get("FEAST_CATALOG_TYPE", "")
    if namespace != NAMESPACES[mode] or catalog_type != CATALOG_TYPES[mode]:
        raise ReadError(f"mode {mode} requires namespace {NAMESPACES[mode]} and catalog type {CATALOG_TYPES[mode]}")
    if mode == "real":
        if environ.get("POLARIS_URI") != POLARIS_ENDPOINT:
            raise ReadError("POLARIS_URI is not the run's Polaris endpoint")
        return CatalogSettings(mode, namespace, catalog_type, endpoint=POLARIS_ENDPOINT, token=token)
    forbidden = sorted(name for name in ("AWS_PROFILE", "POLARIS_CLIENT_ID", "POLARIS_CLIENT_SECRET", "FEAST_POLARIS_TOKEN") if environ.get(name))
    if forbidden:
        raise ReadError(f"a fixture run must not hold {', '.join(forbidden)}")
    folder = environ.get("FEAST_RUN_FOLDER", "/workspace/out").rstrip("/")
    uri = environ.get("FEAST_SQL_CATALOG_URI", f"sqlite:///{folder}/feast/catalog.db")
    warehouse = environ.get("FEAST_SQL_WAREHOUSE", "file:///workspace/local_catalog/warehouse")
    if not folder.startswith("/") or not uri.startswith(f"sqlite:///{folder}/") or ".." in uri:
        raise ReadError("the SQL catalog file is not inside the run folder")
    return CatalogSettings(mode, namespace, catalog_type, sql_uri=uri, sql_warehouse=warehouse)


def open_catalog(settings: CatalogSettings, loader: Callable[..., Any] | None = None) -> IcebergCatalog:
    """Open catalog ``hai`` with PyIceberg: Polaris REST with the token (real runs) or the SQL catalog file (fixture runs)."""
    load = loader if loader is not None else importlib.import_module("pyiceberg.catalog").load_catalog
    if settings.catalog_type == "rest":
        if not settings.token:
            raise ReadError("the REST catalog needs a Polaris token")
        properties = {"type": "rest", "uri": settings.endpoint, "warehouse": WAREHOUSE, "token": settings.token}
    else:
        properties = {"type": "sql", "uri": settings.sql_uri, "warehouse": settings.sql_warehouse}
    catalog: IcebergCatalog = load(CATALOG_NAME, **properties)
    return catalog


def monotonic_deadline(seconds: float, clock: Callable[[], float] = time.monotonic) -> Callable[[str], None]:
    """A checker that raises when more than ``seconds`` passed since this call; call it with the name of the phase that is about to start."""
    start = clock()

    def check(phase: str) -> None:
        if clock() - start > seconds:
            raise ReadError(f"the job passed its {seconds:g} s deadline before {phase}")

    return check
