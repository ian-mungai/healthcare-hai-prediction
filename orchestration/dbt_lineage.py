"""Capture invocation-bound dbt artifacts and validate model edges against fixed row datasets."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from functools import cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

FILES = ("manifest.json", "run_results.json")
LIMIT = 32 * 1024**2
MAX_MODELS = 1000
ATTEMPT_ENV = "DBT_ENV_CUSTOM_ENV_HAI_ATTEMPT_ID"
PARTIAL_PARSE_ENV = "DBT_PARTIAL_PARSE"
CAPTURE_FAILED = "native dbt artifacts could not be captured for this attempt"
CAPTURE_WITHHELD = "dbt artifact capture failed; the launcher's detail is withheld"
ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,249}")
SHA = re.compile(r"[a-f0-9]{64}")


class DbtLineageError(ValueError):
    """Artifact provenance or a model edge cannot be bound to its declared invocation."""


def capture_environment(attempt_id: str) -> list[str]:
    """Container variables of a captured dbt attempt: the attempt identity, and a full parse so the manifest never keeps an earlier attempt's identity."""
    return [f"{ATTEMPT_ENV}={attempt_id}", f"{PARTIAL_PARSE_ENV}=false"]


def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise DbtLineageError("dbt artifact repeats a JSON field")
        result[key] = value
    return result


def read_artifact(folder: Path, name: str) -> bytes:
    """Read bounded regular-file bytes through held directories, refusing symlink traversal."""
    if name not in (*FILES, "capture.json"):
        raise DbtLineageError("unknown dbt artifact filename")
    if not folder.is_absolute() or ".." in folder.parts:
        raise DbtLineageError("dbt artifact folder is not an absolute plain path")
    current = os.open(folder.anchor, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in folder.parts[1:]:
            following = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
            os.close(current)
            current = following
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=current)
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode) or before.st_size > LIMIT:
                raise DbtLineageError("dbt artifact is not a bounded regular file")
            chunks = []
            length = 0
            while length <= LIMIT:
                chunk = os.read(descriptor, min(65536, LIMIT + 1 - length))
                if not chunk:
                    break
                chunks.append(chunk)
                length += len(chunk)
            after = os.fstat(descriptor)
            if length > LIMIT or (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                raise DbtLineageError("dbt artifact changed during its bounded read")
            return b"".join(chunks)
        finally:
            os.close(descriptor)
    except OSError:
        raise DbtLineageError("dbt artifact cannot be read without following links") from None
    finally:
        os.close(current)


def documents(raw: dict[str, bytes], attempt_id: str) -> tuple[dict[str, Any], dict[str, Any], str]:
    try:
        parsed = {name: json.loads(value, object_pairs_hook=pairs) for name, value in raw.items()}
        manifest, results = parsed[FILES[0]], parsed[FILES[1]]
        metadata = [item["metadata"] for item in (manifest, results)]
        invocation = metadata[0]["invocation_id"]
        if not isinstance(invocation, str) or not ID.fullmatch(invocation) or metadata[1]["invocation_id"] != invocation:
            raise DbtLineageError("dbt artifacts name different or invalid invocations")
        if any(item.get("env", {}).get("HAI_ATTEMPT_ID") != attempt_id for item in metadata):
            raise DbtLineageError("dbt artifact belongs to another launcher attempt")
        if not isinstance(manifest.get("nodes"), dict) or not isinstance(results.get("results"), list):
            raise DbtLineageError("dbt artifacts lack their native nodes or results")
        return manifest, results, invocation
    except (ValueError, KeyError, TypeError, AttributeError):
        raise DbtLineageError("dbt artifacts are malformed or not bound to this attempt") from None


def capture(source: Path, destination: Path, attempt_id: str) -> dict[str, Any]:
    """Freeze native bytes once before cleanup; retries reuse the completed capture, never newer job files."""
    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    if destination.is_symlink():
        raise DbtLineageError("dbt capture folder must not be a symlink")
    marker = destination / "capture.json"
    if marker.exists():
        try:
            record = json.loads(read_artifact(destination, "capture.json"), object_pairs_hook=pairs)
        except ValueError:
            raise DbtLineageError("dbt capture marker is invalid") from None
        verified_documents(destination, record, attempt_id)
        return dict(record)
    raw = {name: read_artifact(source, name) for name in FILES}
    _, _, invocation = documents(raw, attempt_id)
    record = {"version": 1, "attempt_id": attempt_id, "invocation_id": invocation, "files": {name: hashlib.sha256(raw[name]).hexdigest() for name in FILES}}
    directory = os.open(destination, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for name, body in (*raw.items(), ("capture.json", json.dumps(record, sort_keys=True).encode())):
            try:
                descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o400, dir_fd=directory)
            except FileExistsError:
                if read_artifact(destination, name) != body:
                    raise DbtLineageError("partial dbt capture differs from the original bytes") from None
                continue
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
        os.fsync(directory)
    finally:
        os.close(directory)
    return record


def verified_documents(folder: Path, record: Any, attempt_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Verify the same bytes parsed below against the launcher's recorded hashes and attempt identity."""
    if not isinstance(record, dict) or set(record) != {"version", "attempt_id", "invocation_id", "files"}:
        raise DbtLineageError("dbt artifact record violates its closed contract")
    if type(record["version"]) is not int or record["version"] != 1 or record["attempt_id"] != attempt_id:
        raise DbtLineageError("dbt artifact record belongs to another launcher attempt")
    hashes = record["files"]
    if not isinstance(hashes, dict) or set(hashes) != set(FILES) or any(not isinstance(value, str) or not SHA.fullmatch(value) for value in hashes.values()):
        raise DbtLineageError("dbt artifact hashes are missing or invalid")
    raw = {name: read_artifact(folder, name) for name in FILES}
    if any(hashlib.sha256(raw[name]).hexdigest() != hashes[name] for name in FILES):
        raise DbtLineageError("dbt artifact hash differs from the launcher attempt record")
    manifest, results, invocation = documents(raw, attempt_id)
    if record["invocation_id"] != invocation:
        raise DbtLineageError("dbt invocation differs from the launcher attempt record")
    return manifest, results


def model_edges(manifest: dict[str, Any], results: dict[str, Any], bindings: dict[str, Any], envelope: dict[str, Any]) -> list[dict[str, Any]]:
    """Resolve only executed model dependencies using fixed row bindings; artifacts cannot invent datasets."""
    nodes = {**manifest.get("sources", {}), **manifest["nodes"]}
    inputs = {(item["namespace"], item["name"]) for item in envelope["inputs"] + envelope["outputs"]}
    outputs = {(item["namespace"], item["name"]) for item in envelope["outputs"]}
    edges = []
    seen = set()
    parent_cache: dict[str, set[tuple[str, str]]] = {}

    def dataset(resource: str, model: str, allowed: set[tuple[str, str]]) -> dict[str, str]:
        value = bindings.get(resource)
        if not isinstance(value, dict) or set(value) != {"namespace", "name"} or (value.get("namespace"), value.get("name")) not in allowed:
            raise DbtLineageError(f"model {model}: dependency {resource} is outside the declared lineage")
        return dict(value)

    def parents(resource: str, model: str, visiting: set[str]) -> set[tuple[str, str]]:
        if len(visiting) > 100 or resource in visiting or resource not in nodes:
            raise DbtLineageError(f"model {model}: dependency graph is missing, cyclic or too deep")
        if resource in parent_cache:
            return parent_cache[resource]
        node = nodes[resource]
        if not isinstance(node, dict):
            raise DbtLineageError(f"model {model}: malformed dependency node")
        dependencies = node.get("depends_on", {}).get("nodes", [])
        if (
            not isinstance(dependencies, list)
            or len(dependencies) > MAX_MODELS
            or any(not isinstance(item, str) or not ID.fullmatch(item) for item in dependencies)
        ):
            raise DbtLineageError(f"model {model}: malformed dependency identifiers")
        found: set[tuple[str, str]] = set()
        for dependency in dependencies:
            if dependency not in nodes:
                raise DbtLineageError(f"model {model}: undeclared dependency {dependency}")
            if nodes[dependency].get("config", {}).get("materialized") == "ephemeral":
                found.update(parents(dependency, model, visiting | {resource}))
            else:
                item = dataset(dependency, model, inputs)
                found.add((item["namespace"], item["name"]))
        parent_cache[resource] = found
        return found

    if len(results["results"]) > 10000 or len(nodes) > 10000:
        raise DbtLineageError("dbt artifact has too many nodes or results")
    for result in results["results"]:
        resource = result.get("unique_id") if isinstance(result, dict) else None
        if not isinstance(resource, str) or not ID.fullmatch(resource) or resource not in nodes or resource in seen:
            raise DbtLineageError("dbt result repeats or lacks a valid manifest resource")
        seen.add(resource)
        node = nodes[resource]
        if node.get("resource_type") not in ("model", "seed", "snapshot") or node.get("config", {}).get("materialized") == "ephemeral":
            continue
        status = result.get("status")
        if status not in ("success", "pass", "warn", "error", "fail", "skipped"):
            raise DbtLineageError(f"model {resource}: invalid result status")
        output = dataset(resource, resource, outputs)
        distinct = parents(resource, resource, set())
        edges.append(
            {
                "model": resource,
                "status": status,
                "inputs": [{"namespace": namespace, "name": name} for namespace, name in sorted(distinct)],
                "outputs": [output],
            }
        )
        if len(edges) > MAX_MODELS:
            raise DbtLineageError("dbt artifact has too many executed models")
    return edges


def from_attempt(folder: Path, attempt: dict[str, Any], invocation: dict[str, Any], intent: dict[str, Any]) -> dict[str, Any]:
    manifest, results = verified_documents(folder, attempt.get("dbt_artifacts"), attempt["id"])
    try:
        models = model_edges(manifest, results, intent["dbt_nodes"], intent["lineage"])
    except (TypeError, AttributeError, KeyError):
        raise DbtLineageError("dbt model graph is malformed") from None
    return {
        "version": 1,
        "invocation": invocation,
        "attempt_id": attempt["id"],
        "invocation_id": attempt["dbt_artifacts"]["invocation_id"],
        "files": dict(attempt["dbt_artifacts"]["files"]),
        "models": models,
    }


@cache
def record_validator() -> Draft202012Validator:
    try:
        schema = json.loads((Path(__file__).resolve().parents[1] / "data_contracts/orchestration/dbt_lineage_record.schema.json").read_text())
        Draft202012Validator.check_schema(schema)
    except (OSError, ValueError, SchemaError):
        raise DbtLineageError("dbt lineage schema is missing or invalid") from None
    return Draft202012Validator(schema)


def validate_record(
    value: Any, invocation: dict[str, Any], attempt_id: str, envelope: dict[str, Any], bindings: dict[str, Any], captured: Any
) -> dict[str, Any]:
    """Recheck a persisted model record without reading any producer artifact or environment."""
    if list(record_validator().iter_errors(value)):
        raise DbtLineageError("dbt lineage record violates its closed contract")
    if value["invocation"] != invocation or value["attempt_id"] != attempt_id:
        raise DbtLineageError("dbt lineage record belongs to another invocation or attempt")
    if (
        not isinstance(captured, dict)
        or captured.get("files") != value["files"]
        or captured.get("invocation_id") != value["invocation_id"]
        or captured.get("attempt_id") != attempt_id
    ):
        raise DbtLineageError("dbt lineage hashes differ from the completed launcher record")
    allowed = {(item["namespace"], item["name"]) for item in envelope["inputs"] + envelope["outputs"]}
    outputs = {(item["namespace"], item["name"]) for item in envelope["outputs"]}
    seen = set()
    for edge in value["models"]:
        model = edge["model"]
        if model in seen or edge["outputs"] != [bindings.get(model)]:
            raise DbtLineageError(f"model {model}: repeated or changed fixed dataset binding")
        seen.add(model)
        for field, permitted in (("inputs", allowed), ("outputs", outputs)):
            if any((item["namespace"], item["name"]) not in permitted for item in edge[field]):
                raise DbtLineageError(f"model {model}: dataset is outside the declared lineage")
    return dict(json.loads(json.dumps(value, allow_nan=False)))
