"""Find the layer runs of one chain from the pins its jobs recorded, so the metadata job ingests the dbt copies of exactly that chain.

The metadata job takes the features run ID. The features job recorded the gold run it read (``gold_pins.json`` of the features run,
``source_run_id``), and the gold job recorded the silver run it read (``silver_pins.json`` of the gold run). Each pin file must be of the run it was
read from, of the namespace it pins and of a real mode run, and its ``source_run_id`` must be a run ID. A pin that is missing, unreadable or wrong
stops the chain with its key. Nothing falls back to a latest run. Silver's own pins (``pins.json``) are a flat table to snapshot map and name no
bronze run, so the chain ends at silver.
"""

from __future__ import annotations

import json
import re
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
PIN_KEYS = {"schema", "run_id", "mode", "namespace", "source_run_id", "snapshot_set"}
PIN_KEY = "lakehouse/layer_runs/{layer}/{run}/{name}"
PIN_BYTES = 2 * 1024**2
LINKS = (("features", "gold_pins.json", "gold"), ("gold", "silver_pins.json", "silver"))  # the layer whose run is read, its pin file, the layer the pin names


class ChainError(RuntimeError):
    """The chain cannot be followed. The message names the pin file or the value and the rule."""


def duplicate_free(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, value in pairs:
        if name in result:
            raise ValueError(f"duplicate key {name}")
        result[name] = value
    return result


def source_run(s3: Any, bucket: str, layer: str, run: str, name: str, namespace: str) -> str:
    """The run that the pin file ``name`` of ``layer`` run ``run`` names, after every check of the file."""
    key = PIN_KEY.format(layer=layer, run=run, name=name)
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read(PIN_BYTES + 1)
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", "")) or "ClientError"
        raise ChainError(f"{key}: {'missing' if code == 'NoSuchKey' else code}") from None
    except BotoCoreError as error:
        raise ChainError(f"{key}: {type(error).__name__}") from None
    if len(body) > PIN_BYTES:
        raise ChainError(f"{key}: larger than {PIN_BYTES} bytes")
    try:
        record = json.loads(body, object_pairs_hook=duplicate_free)
    except ValueError as error:
        raise ChainError(f"{key}: {'has a duplicate key' if 'duplicate' in str(error) else 'is not JSON'}") from None
    if not isinstance(record, dict) or set(record) != PIN_KEYS or record["schema"] != 1:
        raise ChainError(f"{key}: does not have exactly the keys {sorted(PIN_KEYS)} of schema version 1")
    if record["mode"] != "real":
        raise ChainError(f"{key}: is not a real mode record ({record['mode']!r})")
    if record["run_id"] != run:
        raise ChainError(f"{key}: belongs to another run ({record['run_id']!r})")
    if record["namespace"] != namespace:
        raise ChainError(f"{key}: names namespace {record['namespace']!r}, not {namespace}")
    source = record["source_run_id"]
    if not isinstance(source, str) or not RUN_ID.fullmatch(source):
        raise ChainError(f"{key}: names {source!r}, which is not a run ID")
    return source


def layer_runs(s3: Any, bucket: str, features_run: str) -> dict[str, str]:
    """The silver, gold and features run IDs of the chain that ends at ``features_run``."""
    if not isinstance(features_run, str) or not RUN_ID.fullmatch(features_run):
        raise ChainError(f"the features run {features_run!r} is not a run ID")
    runs = {"features": features_run}
    for layer, name, below in LINKS:
        runs[below] = source_run(s3, bucket, layer, runs[layer], name, below)
    return {layer: runs[layer] for layer in ("silver", "gold", "features")}
