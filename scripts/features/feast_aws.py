"""The AWS profile of the Feast jobs: the Iceberg ``features`` namespace through the AWS catalog offline, the host Redis online.

``FEAST_PROFILE`` selects the profile: unset or ``local`` is the Mac launcher path of ``feast_job`` (unchanged), ``aws`` is this module.
The run mode still fixes the namespace (``real`` reads ``features``, ``publish_fixture`` reads ``features_e2e``).
The profile never reads or writes another one.
Both profiles read the one ``feature_store/definitions.py``. ``feature_store_aws.yaml`` is the AWS profile's settings file and holds the same stores as
``feature_store.yaml``.

Everything the job needs comes from its environment or the secret store, never from a file:

- ``POLARIS_URI`` is the catalog endpoint (``http://<host>:<port>/api/catalog``). ``POLARIS_CLIENT_ID`` and ``POLARIS_CLIENT_SECRET`` are the reader pair that
  the existing ``publication_reads.polaris_token`` exchanges for a token. Both leave the environment after the exchange.
- ``FEAST_REDIS_HOST`` is the address of the host Redis (port 6379, as ``online_protocol`` fixes). ``REDIS_PASSWORD`` must be unset. The password is read once
  from the secret store (one ``get`` of the Redis password secret) and held in the job environment in memory until the session ends.
- ``FEAST_REGISTRY_BUCKET`` names the registry bucket. The registry key is the step's closed key (``reads.REGISTRY_KEYS``). A fixture run uses the separate
  ``lakehouse/feast_e2e/`` key so that it cannot replace the real registry. ``AWS_REGION`` is required. AWS access comes from the task role.

The preparation runs in four bounded phases under one deadline: the secret fetch, a Redis check, the token exchange
and a catalog check (load of ``publish_runs``).
A phase that does not answer ends the job with a message that names the phase and the error type, never a value. Nothing here has run against AWS.
"""

from __future__ import annotations

import contextlib
import functools
import re
import threading
import urllib.parse
from collections.abc import Callable, Iterator, MutableMapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from scripts.features import feast_job, feast_materialize
from scripts.features import online_protocol as online
from scripts.features import publication_reads as reads
from scripts.features.publication_reads import ReadError

PROFILE_VARIABLE = "FEAST_PROFILE"
PROFILES = ("local", "aws")
STEPS = ("apply", "materialize")
AWS_SETTINGS_FILE = feast_job.REPO / "feature_store_aws.yaml"
# The variables of ``feature_store_aws.yaml``; the session sets every one of them (a case compares this list with the file).
YAML_VARIABLES = ("FEAST_REGISTRY_PATH", "FEAST_REDIS_HOST", "REDIS_PASSWORD")
ENDPOINT_PATH = "/api/catalog"
HOST = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?")
PASSWORD_FORM = re.compile(r"[A-Za-z0-9_-]{8,128}")
FIXTURE_REGISTRY_STEP = "probe"
PREPARE_SECONDS = 180.0
PHASES = ("the secret fetch", "the Redis check", "the token exchange", "the catalog check")
SSM_CONNECT_SECONDS = 5
SSM_READ_SECONDS = 10


@dataclass(frozen=True)
class Limits:
    """The wait of each phase in seconds (the token exchange keeps the limits of ``publication_reads.polaris_token``)."""

    secret: float = 30.0
    redis: float = 10.0
    catalog: float = 30.0


@dataclass(frozen=True)
class AwsSettings:
    mode: str
    namespace: str
    endpoint: str
    redis_host: str


DEFAULT_LIMITS = Limits()


class SecretReader(Protocol):
    """What the profile needs from the secret store: the value of one named secret."""

    def get(self, name: str) -> str: ...


def selected_profile(environ: MutableMapping[str, str] | Any) -> str:
    """``local`` when ``FEAST_PROFILE`` is unset, otherwise exactly ``local`` or ``aws``."""
    value = environ.get(PROFILE_VARIABLE)
    if value is None:
        return "local"
    if value not in PROFILES:
        raise ReadError(f"{PROFILE_VARIABLE} is not a known profile ({' or '.join(PROFILES)})")
    return str(value)


def settings_file_for(profile: str) -> Path:
    """The Feast settings file of a profile."""
    if profile not in PROFILES:
        raise ReadError("the profile is unknown")
    return AWS_SETTINGS_FILE if profile == "aws" else feast_job.SETTINGS_FILE


def redis_secret_name() -> str:
    """The secret-store name of the Redis password (the constant of the secret-store module)."""
    from scripts.infrastructure import secret_store

    return str(secret_store.REDIS_PASSWORD)


def check_endpoint(value: str) -> str:
    """One plain ``http`` URL with a host, a port and the catalog path, and nothing else."""
    try:
        parts = urllib.parse.urlsplit(value)
        port = parts.port
        host = parts.hostname
    except ValueError:
        raise ReadError("POLARIS_URI is not a valid URL") from None
    if parts.scheme != "http" or not host or not HOST.fullmatch(host) or port is None:
        raise ReadError("POLARIS_URI must be an http URL with a host and a port")
    if parts.username is not None or parts.password is not None or parts.query or parts.fragment or parts.path != ENDPOINT_PATH:
        raise ReadError(f"POLARIS_URI must be exactly http://<host>:<port>{ENDPOINT_PATH} without user information, query or fragment")
    return value


def aws_settings(environ: MutableMapping[str, str] | Any, mode: str) -> AwsSettings:
    """Check the catalog and Redis settings of the environment against the run mode's closed table; no file is read."""
    if mode not in reads.MODES:
        raise ReadError("the run mode is unknown")
    namespace = reads.NAMESPACES[mode]
    preset = environ.get("FEAST_NAMESPACE")
    if preset is not None and preset != namespace:
        raise ReadError(f"mode {mode} reads namespace {namespace}; FEAST_NAMESPACE is set to another value")
    preset = environ.get("FEAST_CATALOG_TYPE")
    if preset is not None and preset != "rest":
        raise ReadError("the AWS profile uses the rest catalog type only; FEAST_CATALOG_TYPE is set to another value")
    endpoint = check_endpoint(environ.get("POLARIS_URI", ""))
    host = environ.get("FEAST_REDIS_HOST", "")
    if not HOST.fullmatch(host):
        raise ReadError("FEAST_REDIS_HOST must be a plain host name or IPv4 address")
    return AwsSettings(mode, namespace, endpoint, host)


def job_settings(environ: MutableMapping[str, str] | Any, step: str, run: str, mode: str) -> feast_job.JobSettings:
    """The job settings of an AWS step: the registry is the step's closed S3 key in ``FEAST_REGISTRY_BUCKET``; no deployment file is read."""
    if step not in STEPS:
        raise ReadError(f"the AWS profile does not run the step {step}")
    if not reads.RUN_ID.fullmatch(run):
        raise ReadError("the run ID is malformed")
    if mode not in reads.MODES:
        raise ReadError("the run mode is unknown")
    if environ.get("FEAST_REGISTRY_PATH"):
        raise ReadError("an AWS run names its registry by bucket and closed key; FEAST_REGISTRY_PATH is set")
    region = environ.get("AWS_REGION", "")
    if not region:
        raise ReadError("an AWS run needs AWS_REGION")
    if not reads.REGION.fullmatch(region):
        raise ReadError("AWS_REGION is not a region name")
    registry_step = step if mode == "real" else FIXTURE_REGISTRY_STEP
    uri = reads.registry_uri(registry_step, reads.REGISTRY_KEYS[registry_step], environ.get("FEAST_REGISTRY_BUCKET", ""))
    folder = environ.get("FEAST_RUN_FOLDER", feast_job.DEFAULT_RUN_FOLDER).rstrip("/")
    return feast_job.JobSettings(step, run, mode, Path(folder), uri, region)


def bounded[T](seconds: float, call: Callable[[], T], what: str) -> T:
    """Run ``call`` in a thread and wait at most ``seconds``; its own errors reach the caller, a call still running then is abandoned and reported."""
    outcome: list[T] = []
    failure: list[BaseException] = []

    def work() -> None:
        try:
            outcome.append(call())
        except BaseException as error:
            failure.append(error)

    thread = threading.Thread(target=work, name="feast-aws-call", daemon=True)
    thread.start()
    thread.join(seconds)
    if failure:
        raise failure[0]
    if not outcome:
        raise ReadError(f"{what} did not finish within {seconds:g} s")
    return outcome[0]


def fetch_password(store: SecretReader, seconds: float) -> str:
    """The Redis password: one ``get`` of its secret name, bounded; the error text of the store is never repeated."""
    name = redis_secret_name()
    try:
        value = bounded(seconds, lambda: store.get(name), f"the read of secret {name}")
    except ReadError:
        raise
    except Exception as error:
        raise ReadError(f"the secret {name} cannot be read ({type(error).__name__})") from None
    if not isinstance(value, str) or not value.strip():
        raise ReadError(f"the secret {name} is empty")
    if not PASSWORD_FORM.fullmatch(value):
        raise ReadError(f"the secret {name} does not have the URL-safe password form")
    return value


class SsmSecretReader:
    """The secret store of the AWS jobs: Parameter Store through the task role. It reads the Redis password secret and no other name."""

    def __init__(self, client: Any) -> None:
        self.client = client

    def get(self, name: str) -> str:
        if name != redis_secret_name():
            raise ReadError("the AWS profile reads the Redis password secret only")
        value = self.client.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
        return str(value)


def open_ssm(region: str) -> SecretReader:
    """The Parameter Store reader with bounded waits and no retry (the deadline of ``fetch_password`` covers the call)."""
    import boto3
    from botocore.config import Config

    config = Config(connect_timeout=SSM_CONNECT_SECONDS, read_timeout=SSM_READ_SECONDS, retries={"max_attempts": 1})
    return SsmSecretReader(boto3.client("ssm", region_name=region, config=config))


@contextlib.contextmanager
def catalog_session(
    job: feast_job.JobSettings,
    environ: MutableMapping[str, str],
    *,
    store: SecretReader,
    exchange: Callable[[str, str, str], str] | None = None,
    loader: Callable[..., Any] | None = None,
    redis_factory: Callable[[Any, int], Any] = online.connect,
    limits: Limits = DEFAULT_LIMITS,
    deadline: Callable[[str], None] | None = None,
) -> Iterator[reads.IcebergCatalog]:
    """Prepare the environment and open the catalog of the AWS profile in the four bounded phases; the password and the token leave the environment on exit."""
    settings = aws_settings(environ, job.mode)
    if environ.get("REDIS_PASSWORD") is not None:
        raise ReadError("REDIS_PASSWORD is set; the AWS profile reads the password from the secret store only")
    missing = sorted(name for name in ("POLARIS_CLIENT_ID", "POLARIS_CLIENT_SECRET") if not environ.get(name))
    if missing:
        raise ReadError(f"an AWS run needs {', '.join(missing)}")
    check = deadline if deadline is not None else reads.monotonic_deadline(PREPARE_SECONDS)
    exchanged = exchange if exchange is not None else reads.polaris_token
    try:
        check(PHASES[0])
        environ["REDIS_PASSWORD"] = fetch_password(store, limits.secret)
        check(PHASES[1])
        client = redis_factory(environ, online.CONTROL_DB)
        bounded(limits.redis, lambda: online.call("Redis check", client.dbsize), PHASES[1])
        check(PHASES[2])
        token = exchanged(settings.endpoint, environ["POLARIS_CLIENT_ID"], environ["POLARIS_CLIENT_SECRET"])
        environ.pop("POLARIS_CLIENT_SECRET", None)
        environ.pop("POLARIS_CLIENT_ID", None)
        environ.update(
            {
                "FEAST_NAMESPACE": settings.namespace,
                "FEAST_CATALOG_TYPE": "rest",
                "FEAST_CATALOG_ENDPOINT": settings.endpoint,
                "FEAST_POLARIS_TOKEN": token,
                "FEAST_REGISTRY_PATH": job.registry_path,
                "FEAST_RUN_FOLDER": str(job.folder),
                "AWS_DEFAULT_REGION": job.region,
            }
        )
        check(PHASES[3])
        catalog = reads.open_catalog(reads.CatalogSettings(job.mode, settings.namespace, "rest", endpoint=settings.endpoint, token=token), loader)
        bounded(limits.catalog, lambda: reads.load_table(catalog, settings.namespace, reads.RUNS_TABLE), PHASES[3])
        yield catalog
    finally:
        for name in ("REDIS_PASSWORD", "FEAST_POLARIS_TOKEN"):
            environ.pop(name, None)


def run_job(
    step: str,
    run: str,
    mode: str,
    environ: MutableMapping[str, str],
    *,
    store: SecretReader | None = None,
    exchange: Callable[[str, str, str], str] | None = None,
    loader: Callable[..., Any] | None = None,
    redis_factory: Callable[[Any, int], Any] = online.connect,
    limits: Limits = DEFAULT_LIMITS,
    backend: feast_job.FeastBackend | None = None,
    lease: feast_job.Lease | None = None,
    schema: tuple[dict[str, Any], str] | None = None,
    control: online.RedisClient | None = None,
    features: online.RedisClient | None = None,
    hooks_for: Callable[[reads.IcebergCatalog, reads.Publication, dict[str, Any]], online.LoadHooks] | None = None,
) -> dict[str, Any]:
    """Run one AWS step (apply or materialize) through the existing job code with this profile's catalog session and settings file."""
    job = job_settings(environ, step, run, mode)
    reader = store if store is not None else open_ssm(job.region)
    session = functools.partial(catalog_session, job, environ, store=reader, exchange=exchange, loader=loader, redis_factory=redis_factory, limits=limits)
    if step == "materialize":
        return feast_materialize.run_materialize(
            job, environ, control=control, features=features, hooks_for=hooks_for, schema=schema, session=session, settings_file=AWS_SETTINGS_FILE
        )
    repository = backend if backend is not None else feast_job.FeastRepository(settings_file=AWS_SETTINGS_FILE)
    return feast_job.run(job, environ, backend=repository, lease=lease, schema=schema, session=session)
