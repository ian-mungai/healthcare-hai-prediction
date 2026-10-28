"""Stage checked control objects and bind immutable result references for ECS jobs."""

from __future__ import annotations

import hashlib
import json
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from services.hai.bootstrap.integrity import MAX_RECORD, StageError, canonical, digest_descriptor, held_file, protected_path, run_prefix, safe_path, try_prefix
from services.hai.bootstrap.outputs import Declaration
from services.hai.bootstrap.redaction import scan_file
from services.hai.bootstrap.transfer import FileRef, Transfer

DATA_SUFFIXES = frozenset((".parquet", ".duckdb", ".db", ".sqlite", ".csv", ".tsv", ".xlsx", ".xls", ".avro", ".jsonl", ".ndjson"))


def input_map(references: Sequence[FileRef]) -> tuple[list[dict[str, Any]], str]:
    """Canonicalize unique logical references and return their exact map digest."""
    if len({reference.path for reference in references}) != len(references):
        raise StageError("duplicate_input")
    document = [reference.document() for reference in sorted(references, key=lambda item: item.path)]
    return document, hashlib.sha256(canonical(document)).hexdigest()


@dataclass(frozen=True)
class ControlSource:
    """One approved Mac control resource, never a real-run dataset upload."""

    source: Path
    path: str
    purpose: Literal["code_archive", "dbt_archive", "resource"]


def stage_controls(
    sources: Sequence[ControlSource],
    transfer: Transfer,
    run_id: str,
    generation: int,
    check_mount_sources: Callable[[Sequence[Path]], None],
    secret_values: Mapping[str, str],
    deadline: float,
) -> tuple[list[dict[str, Any]], str]:
    """Check every source before any upload and return a version-pinned input map.

    ``check_mount_sources`` is the launcher's existing mount and held-secret
    preflight over the complete renderer mount set. The caller must include
    readonly validation suites, baselines and fixtures in that set. This seam
    changes no launcher authorization and performs no dataset upload.
    """
    prefix = run_prefix(run_id, generation)
    if len({source.path for source in sources}) != len(sources):
        raise StageError("duplicate_input")
    for source in sources:
        safe_path(source.path)
        if source.purpose not in ("code_archive", "dbt_archive", "resource") or source.source.suffix.casefold() in DATA_SUFFIXES:
            raise StageError("mac_dataset_refused")
        if protected_path(source.source.absolute().parts):
            raise StageError("unsafe_source")
        if source.purpose in ("code_archive", "dbt_archive") and source.source.suffix != ".tar":
            raise StageError("unsafe_archive")
    check_mount_sources(tuple(source.source for source in sources))
    frozen = []
    for source in sources:
        scan_file(source.source, secret_values, deadline)
        with held_file(source.source.parent, source.source.name) as descriptor:
            digest, size = digest_descriptor(descriptor, deadline)
        frozen.append((source, digest, size))
    references = []
    for source, digest, size in frozen:
        location = "code" if source.purpose == "code_archive" else "trees" if source.purpose == "dbt_archive" else "files"
        key = prefix + "inputs/" + location + "/" + digest + (".tar" if location != "files" else "")
        reference = transfer.upload(source.source, key, source.path, deadline, expected=(digest, size))
        if (reference.sha256, reference.size) != (digest, size):
            raise StageError("source_changed", 71)
        references.append(reference)
    return input_map(references)


def validate_result(
    job: Mapping[str, Any], marker: Mapping[str, Any], declaration: Declaration, live_try: str, task_arn: str, job_sha256: str
) -> tuple[FileRef, ...]:
    """Refuse replay, wrong identities and incomplete output sets before materialization.

    The store must separately confirm the task stop, each object version and all
    downloaded record bytes. This function returns references, never data bodies.
    """
    validate_contract("ecs_outputs", marker, "invalid_result")
    for key in ("run_id", "generation", "attempt_id", "try_id", "template", "input_map_sha256"):
        if key not in job or marker.get(key) != job[key]:
            raise StageError("result_identity", 71)
    if job["try_id"] != live_try or marker.get("task_arn") != task_arn or marker.get("job_spec_sha256") != job_sha256:
        raise StageError("result_identity", 71)
    exit_code = marker.get("exit_code")
    if type(exit_code) is not int or not 0 <= exit_code <= 255:
        raise StageError("invalid_result")
    specs = {item.name: item for item in (*declaration.required, *declaration.optional)}
    required = {item.name for item in declaration.required}
    prefix = try_prefix(job["run_id"], job["generation"], live_try) + "out/"
    objects = marker.get("objects")
    if not isinstance(objects, list):
        raise StageError("invalid_result")
    result: dict[str, FileRef] = {}
    for item in objects:
        if not isinstance(item, dict) or set(item) != {"name", "key", "version_id", "sha256", "size", "kind"}:
            raise StageError("invalid_result")
        name = item["name"]
        if not isinstance(name, str) or name in result or item["key"] != prefix + name:
            raise StageError("result_path", 71)
        reference = FileRef(name, item["key"], item["version_id"], item["sha256"], item["size"])
        if name in ("stdout.txt", "stderr.txt"):
            kind, maximum = "controller_record", MAX_RECORD
        elif not exit_code and name in specs:
            kind, maximum = specs[name].kind, specs[name].max_bytes
        else:
            raise StageError("undeclared_output", 77)
        if item["kind"] != kind or reference.size > maximum:
            raise StageError("output_bound", 71)
        result[name] = reference
    if not exit_code and not (required | {"stdout.txt", "stderr.txt"}) <= result.keys():
        raise StageError("missing_output")
    return tuple(result[name] for name in sorted(result))


def validate_contract(name: str, value: Mapping[str, Any], code: str) -> None:
    """Validate a closed staging record without remote schema retrieval or raw errors."""
    from jsonschema import Draft202012Validator
    from referencing import Registry, Resource

    contracts = Path(__file__).resolve().parents[1] / "data_contracts/orchestration"
    registry: Registry[Any] = Registry()
    documents = {}
    for schema in ("ecs_input_map", "ecs_job", "ecs_outputs", "aws_qualification"):
        document = json.loads((contracts / f"{schema}.schema.json").read_bytes())
        registry = registry.with_resource(document["$id"], Resource.from_contents(document))
        documents[schema] = document
    validator = Draft202012Validator(documents[name], registry=registry)
    if next(validator.iter_errors(value), None) is not None:
        raise StageError(code)


def stage_job(
    job: Mapping[str, Any], transfer: Transfer, allowed_secret_names: Sequence[str], secret_values: Mapping[str, str], spool: Path, deadline: float
) -> FileRef:
    """Validate and stage one immutable, scanned job description under its exact try."""
    requested = job.get("secret_names")
    if not isinstance(requested, list) or not all(isinstance(name, str) for name in requested) or not set(requested) <= set(allowed_secret_names):
        raise StageError("unknown_secret_name")
    validate_contract("ecs_job", job, "invalid_job")
    archives = job.get("archives")
    if not isinstance(archives, dict) or set(archives) != {"code", "dbt"}:
        raise StageError("archive_contract")
    references = [FileRef(**entry) for entry in job["inputs"]]
    mapped, digest = input_map(references)
    if mapped != job["inputs"] or digest != job["input_map_sha256"]:
        raise StageError("input_map_digest", 71)
    raw = canonical(job)
    if len(raw) > MAX_RECORD:
        raise StageError("record_bound")
    key = try_prefix(job["run_id"], job["generation"], job["try_id"]) + "job.json"
    with tempfile.TemporaryDirectory(prefix="job_", dir=spool) as temporary:
        path = Path(temporary) / "job.json"
        path.write_bytes(raw)
        scan_file(path, secret_values, deadline)
        return transfer.upload(path, key, "job.json", deadline, expected=(hashlib.sha256(raw).hexdigest(), len(raw)))
