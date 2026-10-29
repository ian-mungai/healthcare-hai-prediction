"""Validate layer publication evidence using fixed run paths and immutable sealed bytes."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from scripts.lakehouse.gold_publish_record import IDENT_MAX, RUN_ID, SHA256, PublishError, accept_consumer

LAYERS = ("silver", "gold", "features")
SILVER_NAMESPACE = {"publish_fixture": "silver_e2e", "real": "silver"}
# The namespaces a pin of each source layer may name, by run mode: a consumer pins only committed sets of its own source layer.
PIN_NAMESPACES = {"silver": SILVER_NAMESPACE, "gold": {"publish_fixture": "gold_e2e", "real": "gold"}}
MAX_RECORD = 32 << 20
MATERIALIZATION_GATE = ".runstore_pending.json"
COPY_SECONDS = 600.0


class PipelineError(RuntimeError):
    """A pipeline operation cannot prove the required input, ownership or completion."""


class RunStore(Protocol):
    """The storage operations used by shared pipeline acceptance and lifecycle code."""

    def seal(self, layer: str, expected: str) -> dict[str, str]: ...

    def sealed_digest(self, layer: str, expected: str, *, state: Mapping[str, Any] | None = None) -> bool: ...

    def clear_build_artifacts(self, run: Mapping[str, Any]) -> None: ...


class LocalRunStore:
    """Preserve the existing descriptor-held local seals and artifact cleanup."""

    def __init__(self, folder: Path) -> None:
        self.folder = folder

    def seal(self, layer: str, expected: str) -> dict[str, str]:
        """Publish the existing hash-checked, write-once local copy."""
        return seal_copy(self.folder, layer, expected)

    def sealed_digest(self, layer: str, expected: str, *, state: Mapping[str, Any] | None = None) -> bool:
        """Compare the existing sealed file with the validated build digest."""
        return digest_file(self.folder / f"layer_sealed/{layer}/{expected}.duckdb") == expected

    def clear_build_artifacts(self, run: Mapping[str, Any]) -> None:
        """Use the existing local build-artifact cleanup without changing its paths."""
        from scripts.lakehouse.provenance import clear_build_artifacts

        clear_build_artifacts(run)


def require_materialized(root: Path) -> None:
    """Refuse reads while a durable remote materialization is incomplete."""
    gate = root / MATERIALIZATION_GATE
    if gate.exists() or gate.is_symlink():
        raise PipelineError("remote materialization is pending recovery")


def source_namespace(source: str, mode: str) -> str:
    """The namespace of a pinned source layer (silver or gold) in a publish mode; any other layer or mode has none."""
    namespace = PIN_NAMESPACES.get(source, {}).get(mode)
    if namespace is None:
        raise PipelineError(f"run mode {mode!r} has no {source} namespace; the publish modes are {' and '.join(PIN_NAMESPACES.get(source, {}))}")
    return namespace


def read_bytes(root: Path, relative: str, limit: int = MAX_RECORD) -> bytes:
    """Read a bounded regular file through directory descriptors without following symlinks."""
    require_materialized(root)
    parts = relative.split("/")
    if not parts or any(part in ("", ".", "..") for part in parts):
        raise PipelineError("the record path is not relative and plain")
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in parts[:-1]:
            following = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = following
        handle = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW, dir_fd=descriptor)
        try:
            if not stat.S_ISREG(os.fstat(handle).st_mode):
                raise PipelineError("the record is not a regular file")
            result = bytearray()
            while len(result) <= limit:
                block = os.read(handle, min(1 << 20, limit + 1 - len(result)))
                if not block:
                    return bytes(result)
                result.extend(block)
            raise PipelineError("the record exceeds its size limit")
        finally:
            os.close(handle)
    finally:
        os.close(descriptor)


def read_record(root: Path, relative: str) -> tuple[dict[str, Any], str]:
    """Return a bounded JSON object and the digest of the exact bytes parsed."""
    raw = read_bytes(root, relative)
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise PipelineError("the pipeline record is not an object")
    return value, hashlib.sha256(raw).hexdigest()


def digest_file(path: Path) -> str:
    """Hash a regular file without following its final path component."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise PipelineError("the input is not a regular file")
        digest = hashlib.sha256()
        deadline = time.monotonic() + COPY_SECONDS
        while block := os.read(descriptor, 1 << 20):
            if time.monotonic() >= deadline:
                raise PipelineError("hashing the pipeline input exceeded 600 seconds")
            digest.update(block)
        return digest.hexdigest()
    finally:
        os.close(descriptor)


def fixed_layer(layer: str) -> str:
    """Require one of the supported publication layers."""
    if layer not in LAYERS:
        raise PipelineError("the publication layer is unknown")
    return layer


def seal_copy(folder: Path, layer: str, expected: str) -> dict[str, str]:
    """Copy the validated build once and publish only a copy whose streamed digest matches validation."""
    fixed_layer(layer)
    if not SHA256.fullmatch(expected):
        raise PipelineError("the validation supplies no build digest")
    target = folder / "layer_sealed" / layer
    target.mkdir(parents=True, exist_ok=True)
    if target.is_symlink() or target.parent.is_symlink():
        raise PipelineError("the sealed destination is a symlink")
    output = target / f"{expected}.duckdb"
    if output.exists() or output.is_symlink():
        if digest_file(output) != expected:
            raise PipelineError("the existing sealed copy changed")
        return {"file": f"layer_sealed/{layer}/{output.name}", "sha256": expected}
    temporary = target / f".{os.urandom(8).hex()}.partial"
    descriptor = os.open(folder / "staging.duckdb", os.O_RDONLY | os.O_NOFOLLOW)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise PipelineError("the validated build is not a regular file")
        digest = hashlib.sha256()
        deadline = time.monotonic() + COPY_SECONDS
        with temporary.open("xb") as writer:
            while block := os.read(descriptor, 1 << 20):
                if time.monotonic() >= deadline:
                    raise PipelineError("sealing the validated build exceeded 600 seconds")
                digest.update(block)
                writer.write(block)
            writer.flush()
            os.fsync(writer.fileno())
            os.fchmod(writer.fileno(), 0o444)
        if digest.hexdigest() != expected:
            raise PipelineError("the copied bytes differ from the validated build")
        try:
            os.link(temporary, output)
        except FileExistsError:
            if digest_file(output) != expected:
                raise PipelineError("a concurrent seal published another build") from None
        directory = os.open(target, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        os.close(descriptor)
        temporary.unlink(missing_ok=True)
    return {"file": f"layer_sealed/{layer}/{output.name}", "sha256": expected}


def output_record(attempt: Mapping[str, Any]) -> dict[str, Any]:
    """Return the final JSON record from the launcher's bounded producer output."""
    try:
        raw = read_bytes(Path(str(attempt["log_tail"])).parent, "stdout.txt")
        document = json.loads(raw.strip().splitlines()[-1])
    except (ValueError, IndexError, KeyError) as error:
        raise PipelineError("the producer has no terminal JSON receipt") from error
    if not isinstance(document, dict):
        raise PipelineError("the producer receipt is not an object")
    return dict(document)


def accept_validation(state: Mapping[str, Any], run_id: str, layer: str, attempt_id: str, folder: Path, *, store: RunStore | None = None) -> dict[str, Any]:
    """Bind a layer seal to a successful quality attempt owned by this run and generation."""
    fixed_layer(layer)
    run = state["runs"][run_id]
    attempt = state["attempts"].get(attempt_id)
    if not isinstance(attempt, Mapping) or attempt.get("owner") != f"run:{run_id}" or attempt.get("template") != f"pipeline_validate_{layer}":
        raise PipelineError("the validation attempt is not this run's quality validation")
    if attempt.get("phase") != "ended" or attempt.get("reason") != "exited" or attempt.get("exit_code") != 0 or attempt.get("cancel_intent"):
        raise PipelineError("the quality validation did not finish successfully")
    if attempt.get("generation") != run.get("generation", 1) or run.get("state") != "held":
        raise PipelineError("the quality validation belongs to a stale run generation")
    matching = [
        item
        for item in state["attempts"].values()
        if item.get("owner") == f"run:{run_id}"
        and item.get("generation") == attempt.get("generation")
        and item.get("template") == attempt["template"]
        and item.get("params", {}).get("out_host") == str(folder / f"quality_{layer}")
    ]
    if max(matching, key=lambda item: item["created_at"])["id"] != attempt_id:
        raise PipelineError("a later quality attempt superseded this validation")
    params = attempt["params"]
    if Path(str(params.get("database"))) != folder / "staging.duckdb" or Path(str(params.get("out_host"))) != folder / f"quality_{layer}":
        raise PipelineError("the quality attempt did not validate this layer's fixed build path")
    result, result_sha = read_record(folder, f"quality_{layer}/result.json")
    if output_record(attempt).get("result_sha256") != result_sha:
        raise PipelineError("the quality result differs from its successful producer receipt")
    if result.get("counts", {}).get("fail") != 0 or result.get("counts", {}).get("warn") != 0:
        raise PipelineError("the quality result does not pass every expectation")
    expected = str(result.get("build_file_sha256", ""))
    if not SHA256.fullmatch(expected) or not (store if store is not None else LocalRunStore(folder)).sealed_digest(layer, expected, state=state):
        raise PipelineError("the sealed bytes are not the quality attempt's validated bytes")
    return {
        "run_id": run_id,
        "layer": layer,
        "generation": run.get("generation", 1),
        "validation_attempt": attempt_id,
        "validation_sha256": result_sha,
        "file": f"layer_sealed/{layer}/{expected}.duckdb",
        "sha256": expected,
    }


def accepted_completion(run: Mapping[str, Any], layer: str) -> dict[str, Any]:
    """Return an authoritative layer completion bound to this run's active generation."""
    fixed_layer(layer)
    value = run.get("layer_completions", {}).get(layer)
    if not isinstance(value, dict) or value.get("run_id") != run.get("run_id") or value.get("generation") != run.get("generation", 1):
        raise PipelineError("the layer has no accepted completion in this run generation")
    if not SHA256.fullmatch(str(value.get("sha256", ""))):
        raise PipelineError("the layer completion has no sealed digest")
    return dict(value)


def accept_export(state: Mapping[str, Any], run_id: str, layer: str, attempt_id: str, folder: Path) -> dict[str, Any]:
    """Accept exact consumer-result bytes only after this layer's successful export attempt."""
    run = state["runs"][run_id]
    completion = accepted_completion(run, layer)
    attempt = state["attempts"].get(attempt_id)
    if (
        not isinstance(attempt, Mapping)
        or attempt.get("owner") != f"run:{run_id}"
        or attempt.get("template") != "pipeline_export"
        or attempt.get("params") != {"layer": layer}
    ):
        raise PipelineError("the consumer is not this run's layer export")
    if (
        attempt.get("phase") != "ended"
        or attempt.get("reason") != "exited"
        or attempt.get("exit_code") != 0
        or attempt.get("cancel_intent")
        or attempt.get("generation") != completion["generation"]
    ):
        raise PipelineError("the layer export did not finish successfully in this generation")
    result, digest = read_record(folder, "publish/consumer_result.json")
    output = output_record(attempt)
    if output.get("exported") is not True or output.get("manifest_sha256") != result.get("manifest_sha256"):
        raise PipelineError("the export result differs from the successful producer receipt")
    try:
        accept_consumer(result, completion, run_id)
    except PublishError as error:
        raise PipelineError(str(error)) from error
    return {"attempt_id": attempt_id, "sha256": digest, "sealed_sha256": completion["sha256"], "generation": completion["generation"]}


def check_pin(record: Mapping[str, Any], run_id: str, mode: str, tables: set[str], source: str = "silver") -> None:
    """Require a complete pin of the ``source`` layer (silver or gold) for one consumer, mode and committed producer."""
    wanted = source_namespace(source, mode)
    if set(record) != {"schema", "run_id", "mode", "namespace", "source_run_id", "snapshot_set"} or record.get("schema") != 1:
        raise PipelineError(f"the {source} pin has an unsupported shape")
    if record["run_id"] != run_id or record["mode"] != mode or record["namespace"] != wanted or not RUN_ID.fullmatch(str(record["source_run_id"])):
        raise PipelineError(f"the {source} pin identity does not match this consumer")
    snapshots = record["snapshot_set"]
    if not isinstance(snapshots, dict) or set(snapshots) != tables:
        raise PipelineError(f"the {source} pin does not name exactly the publication tables")
    if any(type(value) is not int or not 0 < value <= 9223372036854775807 for value in snapshots.values()):
        raise PipelineError(f"the {source} pin contains an invalid snapshot ID")


def safe_name(value: str) -> str:
    """Require a plain lowercase table identifier."""
    if not re.fullmatch(rf"[a-z][a-z0-9_]{{0,{IDENT_MAX - 1}}}", value):
        raise PipelineError("the table name is not a plain identifier")
    return value
