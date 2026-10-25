"""Declare complete read-job results and refuse every undeclared output."""

from __future__ import annotations

import fnmatch
import hashlib
import math
import os
import re
import stat
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from .integrity import MAX_RECORD, SHA256, StageError, check_deadline, digest_descriptor, held_directory, held_file, safe_path, try_prefix
from .redaction import scan_file
from .transfer import Transfer

TABLE = re.compile(r"[a-z][a-z0-9_]*")


@dataclass(frozen=True)
class Output:
    """One exact logical name, transfer classification and approved byte bound."""

    name: str
    kind: Literal["controller_record", "data"]
    max_bytes: int

    def __post_init__(self) -> None:
        safe_path(self.name)
        if self.kind not in ("controller_record", "data") or type(self.max_bytes) is not int or self.max_bytes < 0:
            raise StageError("invalid_declaration")
        if self.kind == "controller_record" and self.max_bytes > MAX_RECORD:
            raise StageError("record_bound")


@dataclass(frozen=True)
class Declaration:
    """A complete read-job output set with a separate scratch allowance."""

    required: tuple[Output, ...]
    optional: tuple[Output, ...]
    scratch: tuple[str, ...]
    scratch_gib: float

    def __post_init__(self) -> None:
        names = [item.name for item in (*self.required, *self.optional)]
        if len(names) != len(set(names)) or not math.isfinite(self.scratch_gib) or self.scratch_gib < 0:
            raise StageError("invalid_declaration")
        if any(not pattern or pattern.startswith("/") or ".." in pattern.split("/") for pattern in self.scratch):
            raise StageError("invalid_declaration")

    def document(self) -> dict[str, Any]:
        """Return the exact expected-output job fields."""
        return {
            "required": [asdict(item) for item in self.required],
            "optional": [asdict(item) for item in self.optional],
            "scratch": list(self.scratch),
            "scratch_gib": self.scratch_gib,
        }


def read_declaration(template: str, params: Mapping[str, Any], tables: Sequence[str], data_bytes: int, scratch_gib: float) -> Declaration:
    """Expand a read template's exhaustive result names and classifications.

    The caller pins this result to the verified declaration source and its layer
    contract. Writer actions cannot become eligible through this function.
    """
    if type(data_bytes) is not int or data_bytes < 0 or any(TABLE.fullmatch(name) is None for name in tables) or len(set(tables)) != len(tables):
        raise StageError("invalid_declaration")
    records: tuple[str, ...]
    data: tuple[str, ...] = ()
    optional: tuple[str, ...] = ()
    scratch: tuple[str, ...] = ()
    if template == "pipeline_dbt" and set(params) == {"layer"} and params["layer"] in ("silver", "gold"):
        records = ("target/run_results.json", "target/manifest.json", "build_receipt.json")
        data = ("staging.duckdb",)
        optional = ("logs/dbt.log",)
        scratch = ("target/**", "silver_import_*/**")
    elif template in ("pipeline_validate_silver", "pipeline_validate_gold", "pipeline_validate_cross") and not params:
        records = ("result.json",)
    elif template == "pipeline_export" and set(params) == {"layer"} and params["layer"] in ("silver", "gold") and tables:
        records = ("publish/consumer_result.json", "publish/export/manifest.json")
        data = tuple(f"publish/export/{name}.parquet" for name in sorted(tables))
    elif template == "pipeline_publish" and set(params) == {"layer", "step"} and params["layer"] in ("silver", "gold") and params["step"] == "reconcile":
        records = ("publish/reconcile.json",)
    elif (
        template == "pipeline_spark"
        and set(params) <= {"job", "source_run"}
        and params.get("job") in ("pin_silver", "record_silver_snapshot", "pin_bronze", "verify_bronze")
    ):
        if "source_run" in params and params["job"] != "pin_silver":
            raise StageError("invalid_declaration")
        records = {"pin_silver": ("silver_pins.json",), "record_silver_snapshot": ("silver_snapshot.json",), "pin_bronze": ("pins.json",), "verify_bronze": ()}[
            params["job"]
        ]
    elif template == "pipeline_publish":
        raise StageError("writer_ineligible")
    else:
        raise StageError("incomplete_declaration")
    required = tuple(Output(name, "controller_record", MAX_RECORD) for name in records)
    required += tuple(Output(name, "data", data_bytes) for name in data)
    return Declaration(required, tuple(Output(name, "controller_record", MAX_RECORD) for name in optional), scratch, scratch_gib)


def inspect_outputs(root: Path, declaration: Declaration, inputs: Sequence[str], deadline: float) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    """Check the whole output tree before permitting any result upload."""
    required = {item.name for item in declaration.required}
    specs = {item.name: item for item in (*declaration.required, *declaration.optional)}
    input_names = set(inputs)
    for name in input_names:
        safe_path(name)
    found: dict[str, dict[str, Any]] = {}
    scratch: list[dict[str, Any]] = []
    with held_directory(root):
        check_deadline(deadline)

    def walk_error(error: OSError) -> None:
        raise StageError("unsafe_output") from None

    for folder, directories, files in os.walk(root, followlinks=False, onerror=walk_error):
        check_deadline(deadline)
        for name in directories:
            if not stat.S_ISDIR((Path(folder) / name).lstat().st_mode):
                raise StageError("unsafe_output")
        for name in sorted(files):
            path = Path(folder) / name
            relative = str(path.relative_to(root))
            safe_path(relative)
            if relative.endswith(".duckdb.wal"):
                raise StageError("wal_present", 73)
            if relative in input_names and relative not in specs:
                continue
            spec = specs.get(relative)
            discarded = spec is None and any(fnmatch.fnmatchcase(relative, pattern) for pattern in declaration.scratch)
            if spec is None and not discarded:
                raise StageError("undeclared_output", 77)
            with held_file(root, relative) as descriptor:
                digest, size = digest_descriptor(descriptor, deadline)
            if discarded:
                scratch.append({"name": relative, "size": size})
                continue
            if spec is None or size > spec.max_bytes:
                raise StageError("output_bound", 71)
            found[relative] = {"name": relative, "kind": spec.kind, "size": size, "sha256": digest}
    if not required <= found.keys():
        raise StageError("missing_output", 70)
    if sum(item["size"] for item in scratch) > declaration.scratch_gib * (1 << 30):
        raise StageError("scratch_bound", 74)
    return found, sorted(scratch, key=lambda item: item["name"])


def publish_outputs(
    root: Path,
    job: Mapping[str, Any],
    job_sha256: str,
    task_arn: str,
    declaration: Declaration,
    inputs: Sequence[str],
    transfer: Transfer,
    secrets: Mapping[str, str],
    deadline: float,
    *,
    exit_code: int,
    timings: Mapping[str, float],
    minimum_free_disk_bytes: int,
) -> dict[str, Any]:
    """Scan the complete result set and upload its commit marker last.

    A failed job or failed scan ships only permitted clean logs and its failure
    marker. The owning caller supplies the task identity and private writer
    transport after the independent identity and worker gates pass.
    """
    if type(exit_code) is not int or not 0 <= exit_code <= 255 or minimum_free_disk_bytes < 0 or SHA256.fullmatch(job_sha256) is None:
        raise StageError("invalid_result")
    prefix = try_prefix(job["run_id"], job["generation"], job["try_id"])
    logs = (Output("stdout.txt", "controller_record", MAX_RECORD), Output("stderr.txt", "controller_record", MAX_RECORD))
    found: dict[str, dict[str, Any]] = {}
    scratch: list[dict[str, Any]] = []
    if not exit_code:
        complete = Declaration((*declaration.required, *logs), declaration.optional, declaration.scratch, declaration.scratch_gib)
        try:
            found, scratch = inspect_outputs(root, complete, inputs, deadline)
            for name in found:
                scan_file(root / name, secrets, deadline)
        except StageError as error:
            exit_code = error.exit_code
            found = {}
            scratch = []
    if exit_code:
        for spec in logs:
            try:
                with held_file(root, spec.name) as descriptor:
                    digest, size = digest_descriptor(descriptor, deadline)
                if size > MAX_RECORD:
                    continue
                scan_file(root / spec.name, secrets, deadline)
            except StageError:
                continue
            found[spec.name] = {"name": spec.name, "kind": "controller_record", "size": size, "sha256": digest}
    objects = []
    for name, details in sorted(found.items()):
        reference = transfer.upload(root / name, prefix + "out/" + name, name, deadline, expected=(details["sha256"], details["size"]))
        objects.append(
            {
                "name": name,
                "key": reference.key,
                "version_id": reference.version_id,
                "sha256": reference.sha256,
                "size": reference.size,
                "kind": details["kind"],
            }
        )
    marker = {
        "schema_version": 1,
        **{name: job[name] for name in ("run_id", "generation", "attempt_id", "try_id", "template", "input_map_sha256")},
        "task_arn": task_arn,
        "job_spec_sha256": job_sha256,
        "exit_code": exit_code,
        "timings": dict(timings),
        "bytes": sum(item["size"] for item in objects),
        "minimum_free_disk_bytes": minimum_free_disk_bytes,
        "objects": objects,
        "discarded_scratch": scratch,
    }
    from .integrity import canonical

    raw = canonical(marker)
    if len(raw) > MAX_RECORD:
        raise StageError("record_bound")
    with tempfile.TemporaryDirectory(prefix="marker_", dir=root.parent) as temporary:
        path = Path(temporary) / "outputs.json"
        path.write_bytes(raw)
        scan_file(path, secrets, deadline)
        transfer.upload(path, prefix + "outputs.json", "outputs.json", deadline, expected=(hashlib.sha256(raw).hexdigest(), len(raw)))
    return marker
