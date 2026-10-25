"""Validate closed SDK frames with the same schema on the host and in the task image."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import select
import struct
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

MAX_FRAME = 16 * 1024 * 1024
MAX_RECORD = 32 * 1024 * 1024
MUTATING = frozenset(
    {"run_task", "stop_task", "assume_role", "s3_put_small", "s3_upload_part", "s3_mpu_create", "s3_mpu_complete", "s3_mpu_abort", "s3_delete"}
)
SECRET_OPERATIONS = frozenset({"assume_role", "get_parameter"})
CONTROL_OPERATIONS = frozenset({"run_task", "stop_task", "describe_tasks", "list_tasks", "assume_role", "caller_identity", "logs_get_events", "get_parameter"})
KEYWORDS = frozenset(
    {
        "$defs",
        "$id",
        "$ref",
        "$schema",
        "title",
        "description",
        "type",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "allOf",
        "anyOf",
        "oneOf",
        "not",
        "if",
        "then",
        "const",
        "enum",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "minLength",
        "maxLength",
        "pattern",
        "minItems",
        "maxItems",
    }
)


class ProtocolError(ValueError):
    """A frame or closed operation failed validation. The message contains no payload."""


class CallInterrupted(RuntimeError):
    """The parent began shutdown while a pipe operation was in progress."""


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ProtocolError("duplicate_json_key")
        result[key] = value
    return result


def _constant(_value: str) -> None:
    raise ProtocolError("nonfinite_json_number")


def decode(payload: bytes) -> dict[str, Any]:
    """Decode a finite UTF-8 JSON object without duplicate keys."""
    try:
        value = json.loads(payload.decode("utf-8", "strict"), object_pairs_hook=_pairs, parse_constant=_constant)
    except (UnicodeError, ValueError, RecursionError) as error:
        raise ProtocolError("invalid_json_frame") from error
    if not isinstance(value, dict):
        raise ProtocolError("frame_must_be_object")
    return value


def encode(frame: Mapping[str, Any]) -> bytes:
    """Encode one bounded JSON frame without permitting nonfinite numbers."""
    try:
        payload = json.dumps(frame, allow_nan=False, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    except (ValueError, TypeError, RecursionError) as error:
        raise ProtocolError("invalid_outgoing_frame") from error
    if not 0 < len(payload) <= MAX_FRAME:
        raise ProtocolError("frame_length")
    return struct.pack("!I", len(payload)) + payload


def _schema_nodes(node: Mapping[str, Any]) -> None:
    if set(node) - KEYWORDS:
        raise ProtocolError("unsupported_schema_keyword")
    if "$ref" in node and not node["$ref"].startswith("#/$defs/"):
        raise ProtocolError("external_schema_reference")
    for name in ("properties", "$defs"):
        for child in node.get(name, {}).values():
            _schema_nodes(child)
    for name in ("allOf", "anyOf", "oneOf"):
        for child in node.get(name, []):
            _schema_nodes(child)
    for name in ("items", "not", "if", "then"):
        if name in node:
            _schema_nodes(node[name])


def load_schema(path: Path | None = None) -> dict[str, Any]:
    """Load the canonical schema, or its byte-identical in-image copy."""
    if path is None:
        baked = Path(__file__).with_name("sdk_worker.schema.json")
        path = baked if baked.is_file() else Path(__file__).resolve().parents[1] / "data_contracts/orchestration/sdk_worker.schema.json"
    schema = decode(path.read_bytes())
    _schema_nodes(schema)
    return schema


def _matches(value: Any, node: Mapping[str, Any], schema: Mapping[str, Any]) -> bool:
    if "$ref" in node:
        return _matches(value, schema["$defs"][node["$ref"].removeprefix("#/$defs/")], schema)
    if "const" in node and (value != node["const"] or isinstance(value, bool) != isinstance(node["const"], bool)):
        return False
    if "enum" in node and value not in node["enum"]:
        return False
    types = node.get("type", [])
    types = [types] if isinstance(types, str) else types
    predicates = {
        "object": isinstance(value, dict),
        "array": isinstance(value, list),
        "string": isinstance(value, str),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "number": isinstance(value, (int, float)) and not isinstance(value, bool) and (not isinstance(value, float) or math.isfinite(value)),
        "boolean": isinstance(value, bool),
        "null": value is None,
    }
    if types and not any(predicates[name] for name in types):
        return False
    if isinstance(value, dict):
        properties = node.get("properties", {})
        if set(node.get("required", [])) - set(value) or (node.get("additionalProperties") is False and set(value) - set(properties)):
            return False
        if not all(_matches(item, properties[key], schema) for key, item in value.items() if key in properties):
            return False
    if isinstance(value, list):
        if not node.get("minItems", 0) <= len(value) <= node.get("maxItems", len(value)):
            return False
        if "items" in node and not all(_matches(item, node["items"], schema) for item in value):
            return False
    if isinstance(value, str):
        if not node.get("minLength", 0) <= len(value) <= node.get("maxLength", len(value)):
            return False
        if "pattern" in node and re.search(node["pattern"], value) is None:
            return False
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if (isinstance(value, float) and not math.isfinite(value)) or value < node.get("minimum", value) or value > node.get("maximum", value):
            return False
        if "exclusiveMinimum" in node and value <= node["exclusiveMinimum"]:
            return False
    for name, predicate in (("allOf", all), ("anyOf", any)):
        if name in node and not predicate(_matches(value, child, schema) for child in node[name]):
            return False
    if "oneOf" in node and sum(_matches(value, child, schema) for child in node["oneOf"]) != 1:
        return False
    if "not" in node and _matches(value, node["not"], schema):
        return False
    return not ("if" in node and _matches(value, node["if"], schema) and "then" in node and not _matches(value, node["then"], schema))


def validate(frame: Mapping[str, Any], operation: str | None = None, request_id: str | None = None, *, schema: Mapping[str, Any] | None = None) -> None:
    """Validate a request or a correlated operation reply without including payloads in errors."""
    schema = load_schema() if schema is None else schema
    if request_id is None:
        operation = frame.get("op")
        definition = f"{operation}_request"
    else:
        if frame.get("id") != request_id:
            raise ProtocolError("reply_identity_mismatch")
        definition = "error_reply" if frame.get("ok") is False else f"{operation}_reply"
    if definition not in schema["$defs"] or not _matches(dict(frame), schema["$defs"][definition], schema):
        raise ProtocolError("closed_frame_refused")


def cap_seconds(operation: str) -> float:
    """Read the operation cap from the same request schema used for validation."""
    try:
        return float(load_schema()["$defs"][f"{operation}_request"]["properties"]["budget_s"]["maximum"])
    except KeyError as error:
        raise ProtocolError("unknown_operation") from error


def secret_bearing(operation: str, params: Mapping[str, Any]) -> bool:
    """Identify calls whose request or reply must use a fresh memory-only worker."""
    return (
        operation in SECRET_OPERATIONS
        or "session" in params
        or (operation in {"s3_put_small", "s3_get_small", "s3_download"} and "grants" in params.get("key", "").split("/"))
    )


def error_reply(request_id: str, operation: str, kind: str, code: str, *, http_status: int | None = None, request_id_hash: str | None = None) -> dict[str, Any]:
    """Return a typed error with no service body, exception text or session values."""
    return {
        "id": request_id,
        "ok": False,
        "error": {
            "kind": kind,
            "code": code,
            "http_status": http_status,
            "request_id": request_id_hash,
            "effect_unknown": operation in MUTATING and kind in {"deadline", "worker_died", "protocol", "shutdown", "network"},
        },
    }


def request_hash(value: str) -> str | None:
    """Keep request correlation without retaining an arbitrary service-supplied value."""
    return hashlib.sha256(value.encode()).hexdigest() if value else None


def _ready(fd: int, deadline: float, writing: bool, cancelled: Callable[[], bool] | None) -> None:
    while True:
        if cancelled is not None and cancelled():
            raise CallInterrupted("shutdown")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("absolute_pipe_deadline")
        readable, writable, _ = select.select([] if writing else [fd], [fd] if writing else [], [], min(0.05, remaining))
        if readable or writable:
            return


def write_frame(fd: int, frame: Mapping[str, Any], deadline: float, cancelled: Callable[[], bool] | None = None) -> None:
    """Send a frame inside the caller's fixed absolute deadline."""
    payload = memoryview(encode(frame))
    while payload:
        _ready(fd, deadline, True, cancelled)
        try:
            written = os.write(fd, payload[:65536])
        except BlockingIOError:
            continue
        if written <= 0:
            raise EOFError("worker_pipe_closed")
        payload = payload[written:]


def read_frame(fd: int, deadline: float, cancelled: Callable[[], bool] | None = None) -> dict[str, Any]:
    """Read a complete frame while retaining the absolute bound after every byte."""
    prefix = bytearray()
    payload = bytearray()
    required: int | None = None
    while required is None or len(payload) < required:
        _ready(fd, deadline, False, cancelled)
        count = 4 - len(prefix) if required is None else min(65536, required - len(payload))
        try:
            chunk = os.read(fd, count)
        except BlockingIOError:
            continue
        if not chunk:
            if prefix or payload:
                raise ProtocolError("partial_frame")
            raise EOFError("worker_pipe_closed")
        if required is None:
            prefix.extend(chunk)
            if len(prefix) == 4:
                required = struct.unpack("!I", prefix)[0]
                if not 0 < required <= MAX_FRAME:
                    raise ProtocolError("frame_length")
        else:
            payload.extend(chunk)
    return decode(bytes(payload))
