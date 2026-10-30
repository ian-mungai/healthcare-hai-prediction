"""Start the model API: read the settings from the environment, build the service over the real stores and run it under uvicorn. It runs only in the
serving image.

Every setting arrives through the environment. A missing or malformed one stops the start and names the variable, never a value. The key hashes come
from the secret store through the task definition. Redis calls, the manifest read and the prediction append each have a deadline.
"""

from __future__ import annotations

import importlib
import ipaddress
import json
import logging
import os
import threading
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from scripts.features import serving_contract_client as contract
from scripts.serving import canary, keys, limiter, manifests, predictions, service
from scripts.serving.iceberg_sink import IcebergSink
from scripts.serving.mlflow_loader import MlflowLoader

ROOT = Path(__file__).resolve().parents[2]
FEATURE_REPO = ROOT / "feature_store"
SETTINGS_FILE = FEATURE_REPO / "feature_store_aws.yaml"
COLUMNS = json.loads((ROOT / "data_contracts/serving/predictions.json").read_text())["columns"]
LOG = logging.getLogger(__name__)
SINK_CLOSE_SECONDS = 5.0


class SettingsInvalid(SystemExit):
    """A setting is missing or malformed (the message names the variable only)."""


@dataclass(frozen=True)
class Settings:
    key_hashes: dict[str, str]
    hai_types: tuple[str, ...]
    manifest_uri: str
    rate_per_minute: int
    burst: int
    bind_host: str
    port: int
    refresh_seconds: float
    flush_seconds: float


def number(environ: Mapping[str, str], name: str, default: int, low: int, high: int) -> int:
    raw = environ.get(name, str(default))
    if not raw.isdigit() or not low <= int(raw) <= high:
        raise SettingsInvalid(f"{name} must be a whole number from {low} to {high}")
    return int(raw)


def settings_from(environ: Mapping[str, str]) -> Settings:
    for name in ("MODEL_API_KEY_HASHES", "MODEL_API_HAI_TYPES", "MODEL_API_MANIFEST_URI", "MODEL_API_BIND"):
        if not environ.get(name):
            raise SettingsInvalid(f"{name} is not set")
    try:
        hashes = json.loads(environ["MODEL_API_KEY_HASHES"])
        if not isinstance(hashes, dict):
            raise ValueError
        keys.KeyRing.from_hashes(hashes)
    except (ValueError, keys.KeyRingInvalid):
        raise SettingsInvalid("MODEL_API_KEY_HASHES must be a JSON object of key identifier to SHA-256 hash") from None
    types = tuple(item for item in environ["MODEL_API_HAI_TYPES"].split(",") if item)
    if not types or len(set(types)) != len(types) or any(item not in canary.HAI_TYPES for item in types):
        raise SettingsInvalid("MODEL_API_HAI_TYPES must list distinct supported HAI types")
    uri = environ["MODEL_API_MANIFEST_URI"]
    if not uri.startswith(("s3://", "file:///")):
        raise SettingsInvalid("MODEL_API_MANIFEST_URI must start with s3:// or file:///")
    try:
        ipaddress.ip_address(environ["MODEL_API_BIND"])
    except ValueError:
        raise SettingsInvalid("MODEL_API_BIND must be an IP address") from None
    return Settings(
        hashes,
        types,
        uri,
        number(environ, "MODEL_API_RATE_PER_MINUTE", 60, 1, 6000),
        number(environ, "MODEL_API_BURST", 20, 1, 1000),
        environ["MODEL_API_BIND"],
        number(environ, "MODEL_API_PORT", 8000, 1024, 65535),
        float(number(environ, "MODEL_API_REFRESH_SECONDS", 30, 5, 3600)),
        float(number(environ, "MODEL_API_FLUSH_SECONDS", 10, 1, 600)),
    )


def manifest_store(uri: str) -> manifests.ManifestStore:
    if uri.startswith("file:///"):
        return manifests.FileManifestStore(Path(uri.removeprefix("file://")))
    bucket, _, prefix = uri.removeprefix("s3://").partition("/")
    config = importlib.import_module("botocore.config").Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 2, "mode": "standard"})
    return manifests.S3ManifestStore(importlib.import_module("boto3").client("s3", config=config), bucket, prefix)


def build(environ: Mapping[str, str], settings: Settings) -> tuple[service.ModelService, IcebergSink]:
    store = importlib.import_module("feast").FeatureStore(repo_path=str(FEATURE_REPO), fs_yaml_file=SETTINGS_FILE)
    mutable = dict(environ)
    sink = IcebergSink(environ)
    buffer = predictions.PredictionBuffer(sink, columns=COLUMNS, capacity=5000, batch=50, deadline_seconds=20.0, clock=time.monotonic)
    api = service.ModelService(
        keys=keys.KeyRing.from_hashes(settings.key_hashes),
        limiter=limiter.RateLimiter(rate_per_minute=settings.rate_per_minute, burst=settings.burst, clock=time.monotonic),
        read_marker=contract.marker_reader(mutable),
        read_features=contract.feast_reader(store, mutable),
        manifests=manifest_store(settings.manifest_uri),
        loader=MlflowLoader(),
        buffer=buffer,
        clock=time.monotonic,
        new_id=lambda: uuid.uuid4().hex,
        hai_types=settings.hai_types,
        refresh_seconds=settings.refresh_seconds,
        failure_limit=3,
    )
    return api, sink


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = settings_from(os.environ)
    api, sink = build(os.environ, settings)
    stop = threading.Event()

    def flusher() -> None:
        while not stop.wait(settings.flush_seconds):
            api.buffer.flush()

    thread = threading.Thread(target=flusher, name="prediction_flush", daemon=True)
    thread.start()
    from scripts.serving.api import create_app

    uvicorn = importlib.import_module("uvicorn")
    try:
        uvicorn.run(
            create_app(api),
            host=settings.bind_host,
            port=settings.port,
            access_log=False,
            timeout_keep_alive=5,
            limit_concurrency=32,
            timeout_graceful_shutdown=10,
        )
    finally:
        stop.set()
        thread.join(timeout=5)
        api.buffer.flush()
        if not sink.close(SINK_CLOSE_SECONDS):
            LOG.warning("the prediction append worker did not end within %g s, and it may still commit", SINK_CLOSE_SECONDS)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
