"""Exercise verified staging helpers with synthetic storage and real filesystem boundaries."""

from __future__ import annotations

import argparse
import base64
import copy
import errno
import hashlib
import importlib
import inspect
import io
import json
import os
import shutil
import sys
import tarfile
import tempfile
import textwrap
import time
import types
from collections.abc import Callable, Mapping
from contextlib import contextmanager, redirect_stdout
from datetime import UTC, datetime
from functools import partial
from importlib.machinery import SourceFileLoader
from importlib.metadata import version
from pathlib import Path
from typing import Any
from unittest.mock import patch
from urllib.parse import quote, quote_plus

from scripts.orchestration.ecs_fake_s3 import FakeS3, Principal, S3Refusal
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
PREFIX = "lakehouse/run_stage/20261024T000000Z-a1b2c3d4/g1/"
PROJECT = Principal("project", "synthetic_project")
WRITER = Principal("writer", "synthetic_writer", PREFIX + "tries/try_one/", 1e20)

TASK_ARN = ":".join(("arn", "aws", "ecs", "us-west-2", "0" * 12, "task/synthetic_cluster/" + "0" * 32))


def require(value: bool, cause: str) -> None:
    if not value:
        raise RuntimeError(cause)


def refused(cause: str, action: Callable[[], Any]) -> None:
    from services.hai.bootstrap.integrity import StageError

    try:
        action()
    except StageError as error:
        require(error.code == cause, f"wrong_refusal:{cause}:{error.code}")
    else:
        raise RuntimeError("missing_refusal:" + cause)


def held_body_errors(folder: Path) -> None:
    from services.hai.bootstrap import integrity

    source = folder / "held.txt"
    source.write_bytes(b"synthetic")
    for label, context in (("directory", integrity.held_directory(folder)), ("file", integrity.held_file(folder, source.name))):
        expected = OSError(errno.ENOSPC, "synthetic disk-full body error")
        try:
            with context:
                raise expected
        except OSError as error:
            require(error is expected, "caller_oserror_changed:" + label)
        except integrity.StageError:
            raise RuntimeError("caller_oserror_relabelled:" + label) from None
        else:
            raise RuntimeError("caller_oserror_swallowed:" + label)

    def missing() -> None:
        with integrity.held_file(folder, "missing"):
            raise RuntimeError("missing_open_accepted")

    refused("unsafe_source", missing)


def try_identifiers(folder: Path) -> None:
    from scripts.orchestration.ecs_stage_contract_cases import validators
    from services.hai.bootstrap.integrity import StageError, try_prefix

    checks = validators()
    samples = (
        ("short", "try_one", True),
        ("leading_dash", "-try", True),
        ("max", "a" * 100, True),
        ("dot", "try.one", False),
        ("long", "a" * 101, False),
        ("empty", "", False),
        ("newline", "try_one\n", False),
    )
    for label, value, valid in samples:
        try:
            prefix = try_prefix("20261024T000000Z-a1b2c3d4", 1, value)
        except StageError as error:
            require(not valid and error.code == "unsafe_try_id", "try_prefix_boundary:" + label)
        else:
            require(valid and prefix == PREFIX + "tries/" + value + "/", "try_prefix_boundary:" + label)
        for contract in ("ecs_job", "ecs_outputs"):
            example = json.loads((ROOT / "data_contracts/orchestration/examples" / (contract + ".json")).read_bytes())
            example["try_id"] = value
            require(checks[contract].is_valid(example) == valid, "try_schema_boundary:" + contract + ":" + label)


def result_field_types(folder: Path) -> None:
    from orchestration.ecs_stage import validate_result
    from services.hai.bootstrap.outputs import Declaration, Output

    marker = json.loads((ROOT / "data_contracts/orchestration/examples/ecs_outputs.json").read_bytes())
    job = {name: marker[name] for name in ("run_id", "generation", "attempt_id", "try_id", "template", "input_map_sha256")}
    declaration = Declaration(tuple(Output(item["name"], item["kind"], 33554432) for item in marker["objects"]), (), (), 0)
    for field, value in (("version_id", 5), ("sha256", 5), ("size", "5"), ("key", 5), ("name", 5)):
        altered = copy.deepcopy(marker)
        altered["objects"][0][field] = value
        refused("invalid_result", partial(validate_result, job, altered, declaration, job["try_id"], marker["task_arn"], marker["job_spec_sha256"]))


def multipart_limits(folder: Path) -> None:
    from scripts.orchestration.ecs_stage_contract_cases import refused as s3_refused

    model = FakeS3(PREFIX)
    identifier = model.create_upload(PROJECT, PREFIX + "inputs/files/bad_parts", {})
    left = model.upload_part(PROJECT, identifier, 1, b"left", model.checksum(b"left"))
    right = model.upload_part(PROJECT, identifier, 2, b"right", model.checksum(b"right"))
    s3_refused("EntityTooSmall", lambda: model.complete(PROJECT, identifier, [(1, left), (2, right)], "*"))
    s3_refused("InvalidPart", lambda: model.upload_part(PROJECT, identifier, 10001, b"x", model.checksum(b"x")))
    require(model.list_uploads(PROJECT, PREFIX)[0] == [identifier], "invalid_multipart_preserves_upload")
    model.abort(PROJECT, identifier)
    require(not model.list_uploads(PROJECT, PREFIX)[0], "invalid_multipart_cleanup")


def denied_multipart_create(folder: Path) -> None:
    from services.hai.bootstrap.transfer import Transfer

    source = folder / "denied.txt"
    source.write_bytes(b"synthetic")
    model = FakeS3(PREFIX)
    client = Transfer(Storage(model, WRITER), reader=Storage(model))
    refused("stage_upload_failed", lambda: client.upload(source, PREFIX + "inputs/files/denied", source.name, time.monotonic() + 10, multipart=True))
    require(not model.list_uploads(PROJECT, PREFIX)[0], "denied_create_no_upload")


def mounted_imports(folder: Path) -> None:
    mount = folder / "mount"
    for relative in (
        "scripts/lakehouse/__init__.py",
        "scripts/lakehouse/ecs_engine.py",
        "scripts/lakehouse/session.py",
        "scripts/lakehouse/silver_quality.py",
    ):
        destination = mount / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, destination)
    (mount / "config").mkdir()
    code = "import sys; sys.path.insert(0, '.'); import scripts.lakehouse.ecs_engine, scripts.lakehouse.session; assert 'orchestration' not in sys.modules"
    result = run_command(sys.executable, ["-I", "-B", "-c", code], cwd=mount, timeout=10, check=False)
    require(result.returncode == 0, "scripts_only_imports")
    quality_code = (
        "import sys, types; sys.path.insert(0, '.')\n"
        "names = ('duckdb', 'great_expectations', 'great_expectations.expectations', "
        "'great_expectations.execution_engine', 'great_expectations.execution_engine.sqlalchemy_execution_engine')\n"
        "for name in names: sys.modules[name] = types.ModuleType(name)\n"
        "import scripts.lakehouse.silver_quality; assert 'orchestration' not in sys.modules"
    )
    result = run_command(sys.executable, ["-I", "-B", "-c", quality_code], cwd=mount, timeout=10, check=False)
    require(result.returncode == 0, "scripts_only_quality_import")


class Storage:
    """Adapt real transfer calls to the independent storage model without repairing requests."""

    def __init__(self, model: FakeS3, principal: Principal = PROJECT) -> None:
        self.model = model
        self.principal = principal
        self.created: list[str] = []
        self.conflicts = 0
        self.abort_failure = False
        self.download_version: str | None = None
        self.now = 0.0

    def invoke(self, operation: str, request: dict[str, Any], deadline: float) -> dict[str, Any]:
        from services.hai.bootstrap.transfer import StorageError

        require(deadline > time.monotonic(), "transport_deadline")
        key = request.get("key", "")
        try:
            if operation == "head":
                head = self.model.head(self.principal, key, request.get("version_id"), now=self.now)
            elif operation == "put":
                body = self.read_slice(request)
                require(FakeS3.checksum(body) == request["checksum"], "put_checksum")
                head = self.model.put(self.principal, key, body, request.get("if_none_match"), request["metadata"], now=self.now)
            elif operation == "create":
                identifier = self.model.create_upload(self.principal, key, request["metadata"], now=self.now)
                self.created.append(identifier)
                if len(self.created) <= self.conflicts:
                    self.model.conflict_on_complete(identifier)
                return {"upload_id": identifier}
            elif operation == "part":
                checksum = self.model.upload_part(
                    self.principal, request["upload_id"], request["number"], self.read_slice(request), request["checksum"], now=self.now
                )
                return {"etag": checksum, "checksum": checksum}
            elif operation == "complete":
                head = self.model.complete(
                    self.principal,
                    request["upload_id"],
                    [(part["number"], part["checksum"]) for part in request["parts"]],
                    request.get("if_none_match"),
                    now=self.now,
                )
            elif operation == "abort":
                if self.abort_failure:
                    raise StorageError("abort_failed", 503)
                self.model.abort(self.principal, request["upload_id"], now=self.now)
                return {}
            elif operation == "download":
                self.download_version = request.get("version_id")
                head = self.model.head(self.principal, key, self.download_version, now=self.now)
                body = self.model.get(self.principal, key, self.download_version, now=self.now)
                with Path(request["destination"]).open("r+b") as destination:
                    destination.seek(request["offset"])
                    destination.write(body[request["offset"] : request["offset"] + request["length"]])
            else:
                raise RuntimeError("unknown_transport_operation")
            if operation in ("put", "complete"):
                return {"version_id": head.version_id}
            return {"version_id": head.version_id, "size": head.size, "metadata": dict(head.metadata)}
        except S3Refusal as error:
            raise StorageError(error.code, error.status) from None

    @staticmethod
    def read_slice(request: dict[str, Any]) -> bytes:
        with Path(request["source"]).open("rb") as stream:
            stream.seek(request.get("offset", 0))
            return stream.read(request["length"])


def archives(folder: Path) -> None:
    from scripts.lakehouse.provenance import tree_digest
    from services.hai.bootstrap import integrity

    source = folder / "source"
    for name in ("scripts", "orchestration", "data_contracts/lakehouse", "project", "packages"):
        (source / name).mkdir(parents=True, exist_ok=True)
    (source / "scripts/job.py").write_text("value = 1\n")
    (source / "project/model.sql").write_text("select 1\n")
    (source / "packages/package.sql").write_text("select 2\n")
    (source / "packages/cache.pyc").write_bytes(b"synthetic bytecode bytes")
    deadline = time.monotonic() + 10
    members = integrity.inventory(source, ("scripts", "orchestration", "data_contracts/lakehouse", "project", "packages"), deadline)
    archive = folder / "code.tar"
    integrity.pack_archive(source, members, archive, deadline)
    destination = folder / "verified"
    integrity.extract_archive(archive, members, destination, deadline)
    require((destination / "scripts/job.py").read_bytes() == b"value = 1\n", "archive_roundtrip")
    require(integrity.dbt_digest(source, deadline) == tree_digest(source), "dbt_digest_parity")
    before = integrity.code_digest(source, members, deadline)
    (source / "scripts/job.py").chmod(0o755)
    changed = integrity.inventory(source, ("scripts", "orchestration", "data_contracts/lakehouse", "project", "packages"), deadline)
    require(integrity.code_digest(source, changed, deadline) != before, "execution_bit_bound")
    refused("source_changed", lambda: integrity.pack_archive(source, members, folder / "changed.tar", deadline))
    (source / "scripts/link").symlink_to("job.py")
    refused("unsafe_source", lambda: integrity.inventory(source, ("scripts",), deadline))
    bad = folder / "bad.tar"
    with tarfile.open(bad, "w") as stream:
        entry = tarfile.TarInfo("../escape")
        entry.size = 0
        stream.addfile(entry)
    refused("unsafe_archive", lambda: integrity.extract_archive(bad, members, folder / "bad", deadline))

    for label in ("absolute", "link", "hard_link", "device", "extra", "oversized"):
        attack = folder / (label + ".tar")
        with tarfile.open(attack, "w") as stream:
            entry = tarfile.TarInfo(str(folder / "escape") if label == "absolute" else members[0].path)
            entry.type = {"link": tarfile.SYMTYPE, "hard_link": tarfile.LNKTYPE, "device": tarfile.CHRTYPE}.get(label, tarfile.REGTYPE)
            entry.linkname = "synthetic_target"
            if label == "extra":
                entry.name = "undeclared.txt"
            if label == "oversized":
                entry.size = members[0].size + 1
            stream.addfile(entry, io.BytesIO(b"x" * entry.size))
        refused("unsafe_archive", partial(integrity.extract_archive, attack, members, folder / (label + "_out"), deadline))
    require(not (folder / "escape").exists(), "archive_escape_absent")

    for label, mode, body, cause in (("mode", 0o755, b"value = 1\n", "unsafe_archive"), ("digest", 0o644, b"value = 9\n", "archive_digest")):
        member = next(item for item in members if item.path == "scripts/job.py")
        attack = folder / ("member_" + label + ".tar")
        with tarfile.open(attack, "w") as stream:
            entry = tarfile.TarInfo(member.path)
            entry.mode = mode
            entry.size = len(body)
            stream.addfile(entry, io.BytesIO(body))
        refused(cause, partial(integrity.extract_archive, attack, (member,), folder / ("member_" + label), deadline))
    held_body_errors(folder)

    original_open = Path.open
    original_mkdir = Path.mkdir
    for label, number in (("disk", errno.ENOSPC), ("quota", errno.EDQUOT), ("access", errno.EACCES)):
        for operation in ("open", "write", "mkdir"):
            target = folder / (label + "_" + operation)
            expected_code = "unsafe_archive" if number == errno.EACCES else "workspace_full"

            def fail_open(path: Path, *args: Any, failed_target: Path = target, failed_errno: int = number, **kwargs: Any) -> Any:
                if path.is_relative_to(failed_target):
                    raise OSError(failed_errno, "synthetic extraction failure")
                return original_open(path, *args, **kwargs)

            def fail_mkdir(path: Path, *args: Any, failed_target: Path = target, failed_errno: int = number, **kwargs: Any) -> None:
                if path.is_relative_to(failed_target):
                    raise OSError(failed_errno, "synthetic extraction failure")
                original_mkdir(path, *args, **kwargs)

            @contextmanager
            def fail_write(path: Path, *args: Any, failed_target: Path = target, failed_errno: int = number, **kwargs: Any) -> Any:
                with original_open(path, *args, **kwargs) as stream:
                    if path.is_relative_to(failed_target):
                        with patch.object(stream, "write", side_effect=OSError(failed_errno, "synthetic extraction failure")):
                            yield stream
                    else:
                        yield stream

            replacement = {"mkdir": fail_mkdir, "open": fail_open, "write": fail_write}[operation]
            with patch.object(Path, "mkdir" if operation == "mkdir" else "open", replacement):
                try:
                    integrity.extract_archive(archive, members, target, deadline)
                except integrity.StageError as error:
                    require(error.code == expected_code, "extraction_error_code:" + label + ":" + operation + ":" + error.code)
                    require(error.exit_code == (70 if number == errno.EACCES else 72), "extraction_error_exit:" + label + ":" + operation)
                else:
                    raise RuntimeError("extraction_error_missing:" + label + ":" + operation)


def transfers(folder: Path) -> None:
    from services.hai.bootstrap import transfer

    model = FakeS3(PREFIX)
    storage = Storage(model)
    client = transfer.Transfer(storage)
    source = folder / "resource.txt"
    source.write_bytes(b"synthetic resource\n")
    deadline = time.monotonic() + 60
    key = PREFIX + "inputs/files/resource"
    reference = client.upload(source, key, "resources/resource.txt", deadline)
    again = client.upload(source, key, "resources/resource.txt", deadline)
    require(reference == again and len(model.versions(PROJECT, key)) == 1, "immutable_repeat")
    destination = folder / "downloaded.txt"
    client.download(reference, destination, deadline)
    require(destination.read_bytes() == source.read_bytes() and storage.download_version == reference.version_id, "versioned_download")
    source.write_bytes(b"different resource\n")
    refused("stage_conflict", lambda: client.upload(source, key, "resources/resource.txt", deadline))
    storage.conflicts = 2
    large = folder / "large.txt"
    large.write_bytes(b"x" * (5 * 1024 * 1024 + 1))
    multipart_key = PREFIX + "inputs/files/multipart"
    client.upload(large, multipart_key, "resources/large.txt", deadline, multipart=True)
    require(len(storage.created) == 3 and len(set(storage.created)) == 3, "fresh_multipart_identity")
    require(model.list_uploads(PROJECT, PREFIX)[0] == [], "multipart_aborts")
    storage.created = []
    storage.conflicts = 3
    refused("multipart_restarts_exhausted", lambda: client.upload(large, PREFIX + "inputs/files/exhausted", "resources/large.txt", deadline, multipart=True))
    require(model.list_uploads(PROJECT, PREFIX)[0] == [], "exhausted_aborts")
    refused("stage_deadline", lambda: client.upload(large, PREFIX + "inputs/files/late", "resources/large.txt", time.monotonic() - 1))

    scoped_model = FakeS3(PREFIX)
    scoped_writer = Storage(scoped_model, WRITER)
    scoped_writer.conflicts = 1
    scoped_reader = Storage(scoped_model, Principal("task", "AROASYNTHETIC:task_one"))
    scoped = transfer.Transfer(scoped_writer, reader=scoped_reader)
    scoped.upload(large, PREFIX + "tries/try_one/out/large.txt", "large.txt", time.monotonic() + 60, multipart=True)
    require(len(scoped_writer.created) == 2, "missing_object_without_list_permission")
    pending_model = FakeS3(PREFIX)
    pending_storage = Storage(pending_model)
    pending_storage.conflicts = 1
    pending_storage.abort_failure = True
    pending_client = transfer.Transfer(pending_storage)
    pending_client.upload(large, PREFIX + "inputs/files/pending", "resources/large.txt", time.monotonic() + 60, multipart=True)
    require(len(pending_client.pending_aborts) == 1 and len(pending_model.list_uploads(PROJECT, PREFIX)[0]) == 1, "abort_failure_ledger")
    pending_storage.abort_failure = False
    for item in pending_client.pending_aborts:
        pending_model.abort(PROJECT, item["upload_id"])
    pending_client.pending_aborts.clear()
    require(not pending_model.list_uploads(PROJECT, PREFIX)[0], "pending_abort_cleanup")
    budget_model = FakeS3(PREFIX)
    budget_storage = Storage(budget_model)
    budget_storage.conflicts = 1
    refused(
        "multipart_budget",
        lambda: transfer.Transfer(budget_storage).upload(large, PREFIX + "inputs/files/short", "resources/large.txt", time.monotonic() + 1, multipart=True),
    )
    require(len(budget_storage.created) == 1 and not budget_model.list_uploads(PROJECT, PREFIX)[0], "no_restart_without_budget")

    database = folder / "source.duckdb"
    database.write_bytes(b"synthetic database")
    database.with_name(database.name + ".wal").write_bytes(b"")
    refused("wal_present", lambda: client.upload(database, PREFIX + "inputs/files/database", database.name, deadline))

    class CorruptDownload(Storage):
        def invoke(self, operation: str, request: dict[str, Any], deadline: float) -> dict[str, Any]:
            reply = super().invoke(operation, request, deadline)
            if operation == "download":
                with Path(request["destination"]).open("r+b") as stream:
                    stream.write(b"!")
            return reply

    corrupt = folder / "corrupt_download.txt"
    refused("input_digest", lambda: transfer.Transfer(CorruptDownload(model)).download(reference, corrupt, deadline))
    require(not corrupt.exists(), "corrupt_download_unexposed")

    class ChangeAfterUpload(Storage):
        def invoke(self, operation: str, request: dict[str, Any], deadline: float) -> dict[str, Any]:
            reply = super().invoke(operation, request, deadline)
            if operation == "put":
                Path(request["source"]).write_bytes(b"changed in transport")
            return reply

    changed = folder / "changed_after_upload.txt"
    changed.write_bytes(b"synthetic original")
    refused(
        "source_changed", lambda: transfer.Transfer(ChangeAfterUpload(FakeS3(PREFIX))).upload(changed, PREFIX + "inputs/files/changed", changed.name, deadline)
    )
    multipart = folder / "three_parts.bin"
    expected = b"A" * (8 << 20) + b"B" * (8 << 20) + b"C" * ((1 << 20) + 17)
    multipart.write_bytes(expected)
    multiple_model = FakeS3(PREFIX)
    multiple_client = transfer.Transfer(Storage(multiple_model))
    multiple_ref = multiple_client.upload(multipart, PREFIX + "inputs/files/three_parts", multipart.name, deadline, multipart=True)
    require(multiple_model.get(PROJECT, multiple_ref.key, multiple_ref.version_id) == expected, "multipart_content")
    downloaded = folder / "three_parts_downloaded.bin"
    multiple_client.download(multiple_ref, downloaded, deadline)
    require(downloaded.read_bytes() == expected, "multipart_download_content")
    multipart_limits(folder)
    denied_multipart_create(folder)


def declarations(folder: Path) -> None:
    from services.hai.bootstrap import outputs

    declaration = outputs.read_declaration("pipeline_dbt", {"layer": "gold"}, (), 1 << 20, 1.0)
    output = folder / "out"
    for name in ("staging.duckdb", "target/manifest.json", "target/run_results.json", "build_receipt.json"):
        path = output / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}\n")
    found, scratch = outputs.inspect_outputs(output, declaration, (), time.monotonic() + 10)
    require(set(found) == {"staging.duckdb", "target/manifest.json", "target/run_results.json", "build_receipt.json"} and not scratch, "exact_build_outputs")
    (output / "unexpected.txt").write_text("synthetic\n")
    refused("undeclared_output", lambda: outputs.inspect_outputs(output, declaration, (), time.monotonic() + 10))
    (output / "unexpected.txt").unlink()
    (output / "staging.duckdb.wal").write_bytes(b"")
    refused("wal_present", lambda: outputs.inspect_outputs(output, declaration, (), time.monotonic() + 10))
    refused("writer_ineligible", lambda: outputs.read_declaration("pipeline_publish", {"step": "commit"}, (), 10, 0))
    export = outputs.read_declaration("pipeline_export", {"layer": "gold"}, ("dim_one", "fct_two"), 1024, 0)
    require(
        {item.name for item in export.required}
        == {"publish/consumer_result.json", "publish/export/manifest.json", "publish/export/dim_one.parquet", "publish/export/fct_two.parquet"},
        "export_full_set",
    )

    bounded = folder / "bounded"
    (bounded / "scratch").mkdir(parents=True)
    (bounded / "result.json").write_bytes(b"12345")
    small = outputs.Declaration((outputs.Output("result.json", "controller_record", 4),), (), (), 0)
    refused("output_bound", lambda: outputs.inspect_outputs(bounded, small, (), time.monotonic() + 10))
    (bounded / "result.json").write_bytes(b"{}")
    (bounded / "scratch/work.bin").write_bytes(b"12345")
    scratch_limited = outputs.Declaration(small.required, (), ("scratch/**",), 4 / (1 << 30))
    refused("scratch_bound", lambda: outputs.inspect_outputs(bounded, scratch_limited, (), time.monotonic() + 10))
    permitted = outputs.Declaration(small.required, (), ("scratch/**",), 5 / (1 << 30))
    (bounded / "input.txt").write_bytes(b"synthetic input")
    found, scratch = outputs.inspect_outputs(bounded, permitted, ("input.txt", "result.json"), time.monotonic() + 10)
    require(set(found) == {"result.json"} and scratch == [{"name": "scratch/work.bin", "size": 5}], "input_exemption_preserves_outputs")
    outside = folder / "outside"
    outside.mkdir()
    (bounded / "linked").symlink_to(outside, target_is_directory=True)
    refused("unsafe_output", lambda: outputs.inspect_outputs(bounded, permitted, ("input.txt",), time.monotonic() + 10))


def secret_streams(folder: Path) -> None:
    from services.hai.bootstrap import redaction

    marker = "synthetic_value_for_redaction"
    variants = [marker.encode(), base64.b64encode(marker.encode())]
    for variant in variants:
        for split in range(len(variant) + 1):
            stream = redaction.Redactor({"synthetic": marker})
            result = stream.feed(b"before " + variant[:split]) + stream.feed(variant[split:] + b" after") + stream.finish()
            require(variant not in result and b"[REDACTED]" in result, "split_redaction")
    encoded_marker = 'synthetic/"value +é_for_redaction'
    encoded_variants = [quote(encoded_marker, safe="").encode(), quote_plus(encoded_marker, safe="").encode(), json.dumps(encoded_marker)[1:-1].encode()]
    encoded_variants += [base64.b64encode(b"x" * alignment + encoded_marker.encode() + b"z") for alignment in range(3)]
    for variant in encoded_variants:
        for split in range(len(variant) + 1):
            stream = redaction.Redactor({"synthetic": encoded_marker})
            result = stream.feed(variant[:split]) + stream.feed(variant[split:]) + stream.finish()
            require(variant not in result and b"[REDACTED]" in result, "encoded_split_redaction")
    for length in (8, 4096):
        redaction.Redactor({"synthetic": "x" * length})
    for length in (7, 4097):
        refused("secret_length", partial(redaction.Redactor, {"synthetic": "x" * length}))
    output = folder / "record.json"
    output.write_bytes(b'{"value":"' + marker.encode() + b'"}')
    refused("secret_in_output", lambda: redaction.scan_file(output, {"synthetic": marker}, time.monotonic() + 10))
    refused("secret_length", lambda: redaction.Redactor({"short": "short"}))
    refused("secret_scan_incomplete", lambda: redaction.rotation_values({"name": 1}, {"name": 3}, lambda name, version: None if version == 2 else marker))


def isolated_command(folder: Path) -> None:
    mounted_imports(folder)
    command = ROOT / "services/hai/bootstrap/run.py"
    result = run_command(sys.executable, ["-I", str(command), "--self-check"], cwd=folder, timeout=10, check=False)
    require(result.returncode == 0 and json.loads(result.stdout)["status"] == "verified_dependencies", "isolated_baked_selfcheck")


def staged_controls(folder: Path) -> None:
    from orchestration.ecs_stage import ControlSource, input_map, stage_controls, validate_result
    from services.hai.bootstrap.integrity import StageError, canonical
    from services.hai.bootstrap.outputs import Declaration, Output
    from services.hai.bootstrap.transfer import FileRef, Transfer

    model = FakeS3(PREFIX)
    client = Transfer(Storage(model))
    source = folder / "fixture.txt"
    source.write_text("synthetic fixture\n")
    sources = (ControlSource(source, "resources/fixture.txt", "resource"),)
    guards: list[tuple[Path, ...]] = []

    def check_sources(paths: Any) -> None:
        require(not model._objects, "preflight_before_upload")
        guards.append(tuple(paths))

    deadline = time.monotonic() + 10
    documents, digest = stage_controls(sources, client, "20261024T000000Z-a1b2c3d4", 1, check_sources, {}, deadline)
    require(guards == [(source,)] and hashlib.sha256(canonical(documents)).hexdigest() == digest, "complete_mount_preflight")
    duplicate = FileRef(**documents[0])
    refused("duplicate_input", lambda: input_map((duplicate, duplicate)))
    dataset = folder / "source.parquet"
    dataset.write_bytes(b"synthetic only")
    refused(
        "mac_dataset_refused",
        lambda: stage_controls(
            (ControlSource(dataset, "resources/data.parquet", "resource"),), client, "20261024T000000Z-a1b2c3d4", 1, check_sources, {}, deadline
        ),
    )
    protected = folder / ".env"
    refused(
        "unsafe_source",
        lambda: stage_controls(
            (ControlSource(protected, "resources/private.txt", "resource"),), client, "20261024T000000Z-a1b2c3d4", 1, check_sources, {}, deadline
        ),
    )
    marker_value = "synthetic_marker_in_resource"
    contaminated = folder / "contaminated.txt"
    contaminated.write_text(marker_value)

    def refuse_mounts(paths: Any) -> None:
        raise StageError("mount_refused")

    refused("mount_refused", lambda: stage_controls(sources, client, "20261024T000000Z-a1b2c3d4", 1, refuse_mounts, {}, deadline))
    refused(
        "secret_in_output",
        lambda: stage_controls(
            (ControlSource(contaminated, "resources/contaminated.txt", "resource"),),
            client,
            "20261024T000000Z-a1b2c3d4",
            1,
            lambda paths: None,
            {"synthetic": marker_value},
            deadline,
        ),
    )
    declaration = Declaration((Output("result.json", "controller_record", 1024),), (), (), 0)
    job = {
        "run_id": "20261024T000000Z-a1b2c3d4",
        "generation": 1,
        "attempt_id": "attempt_one",
        "try_id": "try_one",
        "template": "pipeline_validate_gold",
        "input_map_sha256": digest,
    }
    marker: dict[str, Any] = dict(
        job,
        schema_version=1,
        task_arn=TASK_ARN,
        job_spec_sha256="1" * 64,
        exit_code=0,
        timings={},
        bytes=6,
        minimum_free_disk_bytes=0,
        objects=[],
        discarded_scratch=[],
    )
    marker.update({"schema_version": 1, "timings": {}, "bytes": 6, "minimum_free_disk_bytes": 0, "discarded_scratch": []})
    for name in ("result.json", "stdout.txt", "stderr.txt"):
        marker["objects"].append(
            {"name": name, "key": PREFIX + "tries/try_one/out/" + name, "version_id": "version_one", "sha256": "2" * 64, "size": 2, "kind": "controller_record"}
        )
    require(len(validate_result(job, marker, declaration, "try_one", TASK_ARN, "1" * 64)) == 3, "complete_result_references")
    refused("result_identity", lambda: validate_result(job, marker, declaration, "superseded", TASK_ARN, "1" * 64))
    altered = copy.deepcopy(marker)
    altered["generation"] = 2
    refused("result_identity", lambda: validate_result(job, altered, declaration, "try_one", TASK_ARN, "1" * 64))
    altered = copy.deepcopy(marker)
    altered["objects"][0]["size"] = 1025
    refused("output_bound", lambda: validate_result(job, altered, declaration, "try_one", TASK_ARN, "1" * 64))

    altered = copy.deepcopy(marker)
    altered["task_arn"] = TASK_ARN[:-1] + "1"
    refused("result_identity", lambda: validate_result(job, altered, declaration, "try_one", TASK_ARN, "1" * 64))
    altered = copy.deepcopy(marker)
    altered["objects"][0]["key"] = PREFIX + "tries/other/out/result.json"
    refused("result_path", lambda: validate_result(job, altered, declaration, "try_one", TASK_ARN, "1" * 64))
    altered = copy.deepcopy(marker)
    altered["objects"] = [item for item in altered["objects"] if item["name"] != "result.json"]
    refused("missing_output", lambda: validate_result(job, altered, declaration, "try_one", TASK_ARN, "1" * 64))
    result_field_types(folder)
    try_identifiers(folder)


def verified_command(folder: Path, command: Path | None = None) -> None:
    from services.hai.bootstrap import integrity
    from services.hai.bootstrap.transfer import FileRef

    inputs = folder / "inputs"
    (inputs / "archives").mkdir(parents=True)
    code = folder / "code"
    for name in ("scripts", "orchestration", "data_contracts/lakehouse", "config/orchestration", "data_contracts/orchestration"):
        (code / name).mkdir(parents=True, exist_ok=True)
    declarations = {"config/orchestration/ecs_jobs.json": b"{}\n", "data_contracts/orchestration/ecs_jobs.schema.json": b"{}\n"}
    for name, body in declarations.items():
        (code / name).write_bytes(body)
    (code / "scripts/job.py").write_text("raise RuntimeError('staged_code_ran_before_verification')\n")
    (code / "json.py").write_text("raise RuntimeError('staged_json_imported')\n")
    (inputs / "json.py").write_text("raise RuntimeError('working_directory_imported')\n")
    tree = folder / "snapshot"
    for name in ("project", "packages"):
        (tree / name).mkdir(parents=True)
        (tree / name / "fixture.sql").write_text("select 1\n")
    deadline = time.monotonic() + 30
    roots = (
        "scripts",
        "orchestration",
        "data_contracts/lakehouse",
        "config/orchestration/ecs_jobs.json",
        "data_contracts/orchestration/ecs_jobs.schema.json",
        "json.py",
    )
    code_members = integrity.inventory(code, roots, deadline)
    dbt_members = integrity.inventory(tree, ("project", "packages"), deadline)
    references = []
    for label, source, members in (("code", code, code_members), ("dbt", tree, dbt_members)):
        target = inputs / f"archives/{label}.tar"
        integrity.pack_archive(source, members, target, deadline)
        raw = target.read_bytes()
        references.append(
            FileRef(f"archives/{label}.tar", PREFIX + f"inputs/files/{label}", f"synthetic_version_{label}", hashlib.sha256(raw).hexdigest(), len(raw))
        )
    job = json.loads((ROOT / "data_contracts/orchestration/examples/ecs_job.json").read_bytes())
    job["inputs"] = [reference.document() for reference in references]
    job["input_map_sha256"] = hashlib.sha256(integrity.canonical(job["inputs"])).hexdigest()
    job["code_sha256"] = integrity.code_digest(code, code_members, deadline)
    job["dbt_tree_sha256"] = integrity.dbt_digest(tree, deadline)
    job["archives"] = {
        "code": {"input_path": "archives/code.tar", "members": [member.document() for member in code_members]},
        "dbt": {"input_path": "archives/dbt.tar", "members": [member.document() for member in dbt_members]},
    }
    job["declaration_source"]["sha256"] = hashlib.sha256(declarations["config/orchestration/ecs_jobs.json"]).hexdigest()
    job["declaration_source"]["schema_sha256"] = hashlib.sha256(declarations["data_contracts/orchestration/ecs_jobs.schema.json"]).hexdigest()
    from orchestration.ecs_stage import stage_job
    from services.hai.bootstrap.transfer import Transfer

    model = FakeS3(PREFIX)
    staged = stage_job(job, Transfer(Storage(model)), (), {}, folder, deadline)
    require(staged.path == "job.json" and staged.key == PREFIX + "tries/" + job["try_id"] + "/job.json", "staged_job_identity")
    refused("unknown_secret_name", lambda: stage_job(dict(job, secret_names=["UNDECLARED_SECRET"]), Transfer(Storage(model)), (), {}, folder, deadline))
    refused("input_map_digest", lambda: stage_job(dict(job, input_map_sha256="0" * 64), Transfer(Storage(FakeS3(PREFIX))), (), {}, folder, deadline))
    marker_value = "synthetic_marker_in_job"
    clean_model = FakeS3(PREFIX)
    refused(
        "secret_in_output",
        lambda: stage_job(dict(job, attempt_id=marker_value), Transfer(Storage(clean_model)), (), {"synthetic": marker_value}, folder, deadline),
    )
    require(not clean_model._objects, "secret_job_unstored")
    raw = integrity.canonical(job)
    job_path = folder / "job.json"
    job_path.write_bytes(raw)
    script_directory = command.parent if command is not None else ROOT / "services/hai/bootstrap"
    script = script_directory / "run.py"
    common = [
        "--job",
        str(job_path),
        "--job-sha256",
        hashlib.sha256(raw).hexdigest(),
        "--inputs",
        str(inputs),
        "--run-id",
        job["run_id"],
        "--generation",
        str(job["generation"]),
        "--attempt-id",
        job["attempt_id"],
        "--try-id",
        job["try_id"],
        "--template",
        job["template"],
        "--timeout-seconds",
        "10",
    ]
    result = run_command(sys.executable, ["-I", "-B", str(script), *common, "--workspace", str(folder / "prepared")], cwd=inputs, timeout=15, check=False)
    require(result.returncode == 0 and json.loads(result.stdout)["status"] == "verified_tree", "verified_command_tree")
    require((folder / "prepared/code/scripts/job.py").stat().st_mode & 0o222 == 0, "verified_tree_readonly")
    changed = [*common]
    changed[changed.index("--job-sha256") + 1] = "0" * 64
    result = run_command(sys.executable, ["-I", "-B", str(script), *changed, "--workspace", str(folder / "refused")], cwd=inputs, timeout=15, check=False)
    require(
        result.returncode == 71 and json.loads(result.stderr)["error"] == "job_digest" and not (folder / "refused").exists(), "job_digest_before_extraction"
    )
    changed = [*common]
    changed[changed.index("--attempt-id") + 1] = "different_attempt"
    result = run_command(
        sys.executable, ["-I", "-B", str(script), *changed, "--workspace", str(folder / "wrong_identity")], cwd=inputs, timeout=15, check=False
    )
    require(
        result.returncode == 71 and json.loads(result.stderr)["error"] == "job_identity" and not (folder / "wrong_identity").exists(), "job_identity_binding"
    )
    altered = copy.deepcopy(job)
    altered["declaration_source"]["sha256"] = "0" * 64
    altered_raw = integrity.canonical(altered)
    altered_path = folder / "altered_job.json"
    altered_path.write_bytes(altered_raw)
    changed = [*common]
    changed[changed.index("--job") + 1] = str(altered_path)
    changed[changed.index("--job-sha256") + 1] = hashlib.sha256(altered_raw).hexdigest()
    result = run_command(
        sys.executable, ["-I", "-B", str(script), *changed, "--workspace", str(folder / "wrong_declaration")], cwd=inputs, timeout=15, check=False
    )
    require(result.returncode == 71 and json.loads(result.stderr)["error"] == "declaration_source", "declaration_source_binding")
    (inputs / "archives/code.tar").write_bytes(b"synthetic corrupted archive")
    result = run_command(sys.executable, ["-I", "-B", str(script), *common, "--workspace", str(folder / "corrupt")], cwd=inputs, timeout=15, check=False)
    require(
        result.returncode == 71 and json.loads(result.stderr)["error"] == "input_digest" and not (folder / "corrupt").exists(), "input_digest_before_extraction"
    )


def publication(folder: Path) -> None:
    from orchestration.ecs_stage import validate_result
    from services.hai.bootstrap import outputs
    from services.hai.bootstrap.transfer import Transfer

    root = folder / "out"
    root.mkdir()
    for name in ("result.json", "stdout.txt", "stderr.txt"):
        (root / name).write_text("{}\n" if name.endswith("json") else "synthetic log\n")
    base_declaration = outputs.read_declaration("pipeline_validate_gold", {}, (), 1024, 0)
    declaration = outputs.Declaration(base_declaration.required, (), ("scratch/**",), 5 / (1 << 30))
    (root / "scratch").mkdir()
    (root / "scratch/work.txt").write_bytes(b"scrap")
    job = {
        "run_id": "20261024T000000Z-a1b2c3d4",
        "generation": 1,
        "attempt_id": "attempt_one",
        "try_id": "try_one",
        "template": "pipeline_validate_gold",
        "input_map_sha256": "3" * 64,
    }
    model = FakeS3(PREFIX)
    client = Transfer(Storage(model, WRITER), reader=Storage(model))
    marker = outputs.publish_outputs(
        root,
        job,
        "1" * 64,
        TASK_ARN,
        declaration,
        (),
        client,
        {},
        time.monotonic() + 10,
        exit_code=0,
        timings={"execution": 0.0},
        minimum_free_disk_bytes=1 << 30,
    )
    require(marker["exit_code"] == 0 and len(validate_result(job, marker, declaration, "try_one", TASK_ARN, "1" * 64)) == 3, "published_complete_result")
    require(
        model.get(PROJECT, PREFIX + "tries/try_one/outputs.json") == json.dumps(marker, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(),
        "commit_marker_last",
    )
    require(marker["discarded_scratch"] == [{"name": "scratch/work.txt", "size": 5}], "discarded_scratch_recorded")
    marker_value = "synthetic_marker_in_output"
    (root / "result.json").write_text(marker_value)
    clean_model = FakeS3(PREFIX)
    failure = outputs.publish_outputs(
        root,
        job,
        "1" * 64,
        TASK_ARN,
        declaration,
        (),
        Transfer(Storage(clean_model, WRITER), reader=Storage(clean_model)),
        {"synthetic": marker_value},
        time.monotonic() + 10,
        exit_code=0,
        timings={"execution": 0.0},
        minimum_free_disk_bytes=1 << 30,
    )
    require(failure["exit_code"] == 75 and {item["name"] for item in failure["objects"]} == {"stdout.txt", "stderr.txt"}, "secret_withholds_all_results")
    require(PREFIX + "tries/try_one/out/result.json" not in clean_model._objects, "no_secret_upload")

    (root / "stderr.txt").write_text(marker_value)
    failed_model = FakeS3(PREFIX)
    failure = outputs.publish_outputs(
        root,
        job,
        "1" * 64,
        TASK_ARN,
        declaration,
        (),
        Transfer(Storage(failed_model, WRITER), reader=Storage(failed_model)),
        {"synthetic": marker_value},
        time.monotonic() + 10,
        exit_code=9,
        timings={},
        minimum_free_disk_bytes=1,
    )
    require(
        {item["name"] for item in failure["objects"]} == {"stdout.txt"} and PREFIX + "tries/try_one/out/stderr.txt" not in failed_model._objects,
        "failure_log_secret_withheld",
    )
    (root / "stderr.txt").write_text("synthetic clean log")
    marker_model = FakeS3(PREFIX)
    refused(
        "secret_in_output",
        lambda: outputs.publish_outputs(
            root,
            dict(job, attempt_id=marker_value),
            "1" * 64,
            TASK_ARN,
            declaration,
            (),
            Transfer(Storage(marker_model, WRITER), reader=Storage(marker_model)),
            {"synthetic": marker_value},
            time.monotonic() + 10,
            exit_code=9,
            timings={},
            minimum_free_disk_bytes=1,
        ),
    )
    require(PREFIX + "tries/try_one/outputs.json" not in marker_model._objects, "secret_marker_unstored")


def engine_configuration(folder: Path) -> None:
    from scripts.lakehouse import pipeline_build

    output = folder / "out"
    output.mkdir()
    captures: list[list[str]] = []

    class DbtRunner:
        def invoke(self, arguments: list[str]) -> Any:
            captures.append(arguments)
            (output / "target").mkdir(exist_ok=True)
            (output / "target/run_results.json").write_text(json.dumps({"results": [{"status": "pass"}], "metadata": {"invocation_id": "synthetic"}}))
            return types.SimpleNamespace(success=True)

    fake_dbt: Any = types.ModuleType("dbt.cli.main")
    fake_dbt.dbtRunner = DbtRunner
    environment = {
        "AWS_EXECUTION_ENV": "AWS_ECS_FARGATE",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "/v2/credentials/synthetic_task",
        "AWS_REGION": "us-west-2",
        "POLARIS_URI": "http://catalog.example.invalid:8181/api/catalog",
        "POLARIS_CLIENT_ID": "synthetic_reader",
        "POLARIS_CLIENT_SECRET": "synthetic_reader_value",
        "JOB_THREADS": "1",
        "DUCKDB_MEMORY_LIMIT": "1GB",
        "SPARK_JOB_MEMORY": "1g",
        "HAI_SPARK_LOCAL_DIRECTORY": str(folder / "spark_tmp"),
        "HAI_DUCKDB_TEMP_DIRECTORY": str(folder / "duckdb_tmp"),
        "HAI_DUCKDB_MAX_TEMP_BYTES": "1048576",
    }
    arguments = [
        "pipeline_build",
        "--layer",
        "silver",
        "--mode",
        "publish_fixture",
        "--run",
        "20261024T000000Z-a1b2c3d4",
        "--folder",
        str(output),
        "--project",
        str(folder / "project"),
        "--source-sha256",
        "1" * 64,
        "--input-sha256",
        "2" * 64,
    ]
    with (
        patch.dict(os.environ, environment, clear=True),
        patch.dict(sys.modules, {"dbt.cli.main": fake_dbt}),
        patch.object(sys, "argv", arguments),
        redirect_stdout(io.StringIO()),
    ):
        require(pipeline_build.main() == 0, "bounded_build_entry")
        require(captures[0][captures[0].index("--target") + 1] == "fixture_bounded", "actual_bounded_dbt_target")
        require(pipeline_build.duckdb_temp_settings()["max_temp_directory_size"] == "1048576B", "engine_temp_cap")
    with patch.dict(os.environ, {}, clear=True):
        require(pipeline_build.duckdb_temp_settings() == {}, "unchanged_local_temp_settings")
    extract = output / "silver_input"
    (extract / "dim_one").mkdir(parents=True)
    part = b"synthetic parquet stand-in"
    (extract / "dim_one/part-00000.parquet").write_bytes(part)
    manifest = {
        "run_id": "20261024T000000Z-a1b2c3d4",
        "pins_sha256": "2" * 64,
        "snapshot_set": {"dim_one": 1},
        "files": {"dim_one/part-00000.parquet": hashlib.sha256(part).hexdigest()},
    }
    manifest_raw = json.dumps(manifest).encode()
    (extract / "manifest.json").write_bytes(manifest_raw)

    class DuckConnection:
        def execute(self, statement: str, parameters: Any = None) -> DuckConnection:
            return self

        def close(self) -> None:
            return None

    def connect(path: str, config: dict[str, str]) -> DuckConnection:
        require(config["max_temp_directory_size"] == "1048576B", "actual_import_temp_cap")
        Path(path).write_bytes(b"synthetic database stand-in")
        return DuckConnection()

    fake_duckdb: Any = types.ModuleType("duckdb")
    fake_duckdb.connect = connect
    gold_arguments = [*arguments]
    gold_arguments[gold_arguments.index("--layer") + 1] = "gold"
    gold_arguments += ["--extract-sha256", hashlib.sha256(manifest_raw).hexdigest()]
    with (
        patch.dict(os.environ, environment, clear=True),
        patch.dict(sys.modules, {"dbt.cli.main": fake_dbt, "duckdb": fake_duckdb}),
        patch.object(sys, "argv", gold_arguments),
        redirect_stdout(io.StringIO()),
    ):
        require(pipeline_build.main() == 0, "gold_build_entry")
        require(captures[-1][captures[-1].index("--target") + 1] == "pinned_silver_bounded", "gold_fixture_pinned_profile")
        receipt = json.loads((output / "build_receipt.json").read_bytes())
        require(receipt["runtime_measurements"]["import"]["verified_input_bytes"] == len(part), "actual_import_byte_measurement")
        require(receipt["runtime_measurements"]["complete_peak"] is False, "sampled_peak_not_acceptance")

    yaml = importlib.import_module("yaml")

    profiles = yaml.safe_load((ROOT / "dbt/profiles.yml").read_text())["hai"]["outputs"]
    for profile in ("pinned_silver_bounded", "fixture_bounded", "lakehouse_session_bounded", "lakehouse_task"):
        require("max_temp_directory_size" in profiles[profile]["settings"], "bounded_profile_definition")
    native_secret = profiles["lakehouse_task"]["secrets"][0]
    require(
        native_secret["provider"] == "credential_chain" and "chain" not in native_secret and "profile" not in native_secret, "native_duckdb_chain_definition"
    )

    from scripts.lakehouse import session

    builder_values: dict[str, Any] = {}

    class SparkResult(dict[str, Any]):
        @property
        def conf(self) -> Any:
            return types.SimpleNamespace(set=self.__setitem__)

    class SparkBuilder:
        def appName(self, value: str) -> SparkBuilder:
            return self

        def master(self, value: str) -> SparkBuilder:
            return self

        def config(self, name: str, value: Any) -> SparkBuilder:
            builder_values[name] = value
            return self

        def getOrCreate(self) -> Any:
            return SparkResult(builder_values)

    fake_spark: Any = types.ModuleType("pyspark.sql")
    fake_spark.SparkSession = types.SimpleNamespace(builder=SparkBuilder())
    with patch.dict(os.environ, environment, clear=True), patch.dict(sys.modules, {"pyspark.sql": fake_spark}):
        configured: Any = session.spark_session("synthetic_setup")
        require(
            configured.get("spark.sql.catalog.hai_lakehouse.client.credentials-provider")
            == "software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider",
            "native_container_provider",
        )
        require(configured["spark.local.dir"] == environment["HAI_SPARK_LOCAL_DIRECTORY"], "actual_spark_temp_directory")
    with patch.dict(os.environ, dict(environment, AWS_PROFILE="synthetic_stale_profile"), clear=True), patch.dict(sys.modules, {"pyspark.sql": fake_spark}):
        try:
            session.spark_session("synthetic_stale")
        except RuntimeError as error:
            require(str(error) == "task_credential_source_refused", "wrong_task_credential_refusal")
        else:
            raise RuntimeError("missing_task_credential_refusal")

    with (
        patch.dict(os.environ, {**environment, "AWS_SESSION_TOKEN": "synthetic_fixture_token"}, clear=True),
        patch.dict(sys.modules, {"pyspark.sql": fake_spark}),
    ):
        try:
            session.spark_session("synthetic_fixture", warehouse=folder / "fixture_warehouse")
        except RuntimeError as error:
            require(str(error) == "fixture_role_session_refused", "wrong_fixture_credential_refusal")
        else:
            raise RuntimeError("missing_fixture_credential_refusal")
    fake_quality = {
        name: types.ModuleType(name)
        for name in (
            "duckdb",
            "great_expectations",
            "great_expectations.expectations",
            "great_expectations.execution_engine",
            "great_expectations.execution_engine.sqlalchemy_execution_engine",
        )
    }
    with patch.dict(sys.modules, fake_quality), patch.dict(os.environ, environment, clear=True):
        from scripts.lakehouse import silver_quality

        require(
            silver_quality.engine_config()
            == {"memory_limit": "1GB", "threads": "1", "temp_directory": environment["HAI_DUCKDB_TEMP_DIRECTORY"], "max_temp_directory_size": "1048576B"},
            "quality_temp_cap_wiring",
        )
    with (
        patch.dict(os.environ, dict(environment, HAI_DUCKDB_MAX_TEMP_BYTES=""), clear=True),
        patch.object(sys, "argv", arguments),
        redirect_stdout(io.StringIO()),
    ):
        require(pipeline_build.main() == 1, "invalid_build_settings_sanitized")


def observe(cases: Mapping[str, Callable[[Path], None]], base: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for name, case in cases.items():
        with tempfile.TemporaryDirectory(prefix=name + "_", dir=base) as temporary:
            try:
                case(Path(temporary))
                result[name] = {"passed": True}
            except (RuntimeError, ValueError, OSError, ImportError, KeyError, TypeError, AttributeError) as error:
                result[name] = {"passed": False, "cause": str(error) if isinstance(error, RuntimeError) else type(error).__name__}
    return result


def fingerprints() -> dict[str, str]:
    files = [Path(__file__), ROOT / "orchestration/ecs_stage.py"]
    files += sorted((ROOT / "services/hai/bootstrap").glob("*.py"))
    files += sorted((ROOT / "data_contracts/orchestration").glob("ecs_*.schema.json"))
    files += sorted((ROOT / "data_contracts/orchestration/examples").glob("ecs_*.json"))
    files += [
        ROOT / "scripts/orchestration/ecs_fake_s3.py",
        ROOT / "scripts/lakehouse/provenance.py",
        ROOT / "scripts/lakehouse/ecs_engine.py",
        ROOT / "scripts/lakehouse/pipeline_build.py",
        ROOT / "scripts/lakehouse/session.py",
        ROOT / "scripts/lakehouse/silver_quality.py",
        ROOT / "dbt/profiles.yml",
        ROOT / "scripts/process.py",
    ]
    return {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in files if path.is_file()}


class MutantLintError(Exception):
    """Stop mutation preparation when a generated copy fails unchanged lint."""


def lint_mutant(paths: tuple[Path, ...], base: Path, label: str) -> None:
    require(all(path.is_relative_to(base) for path in paths), "mutation_copy_location:" + label)
    commands = (["check", "--fix"], ["format"], ["check", "--output-format", "json"])
    checks: list[dict[str, Any]] = []
    for arguments in commands:
        command = ["-m", "ruff", *arguments, "--config", str(ROOT / "pyproject.toml"), *(str(path) for path in paths)]
        checked = run_command(sys.executable, command, cwd=ROOT, timeout=15, check=False)
        checks.append({"command": command, "exit_code": checked.returncode, "stdout": checked.stdout, "stderr": checked.stderr})
    if checks[0]["exit_code"] not in (0, 1) or any(row["exit_code"] != 0 for row in checks[1:]):
        record = {"label": label, "copies": [str(path) for path in paths], "checks": checks}
        (base / "blocked.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
        raise MutantLintError("mutation_copy_ruff:" + label + ":" + checks[-1]["stdout"].strip())


def changed_function(owner: Any, attribute: str, edits: tuple[tuple[str, str], ...], base: Path) -> Callable[..., Any]:
    original = inspect.unwrap(getattr(owner, attribute))
    original_source = inspect.getsource(original)
    source = textwrap.dedent(original_source)
    for before, after in edits:
        require(source.count(before) == 1, "mutation_anchor:" + attribute)
        source = source.replace(before, after, 1)
    module_source = Path(original.__code__.co_filename).read_text()
    require(module_source.count(original_source) == 1, "mutation_function_source:" + attribute)
    indentation = original_source[: len(original_source) - len(original_source.lstrip())]
    with tempfile.TemporaryDirectory(prefix="source_mutation_", dir=base) as temporary:
        path = Path(temporary) / "mutated_module.py"
        path.write_text(module_source.replace(original_source, textwrap.indent(source, indentation), 1))
        lint_mutant((path,), base, attribute)
        code = SourceFileLoader("hai_case_mutation", str(path)).get_code("hai_case_mutation")
        if code is None:
            raise RuntimeError("mutation_code_missing:" + attribute)
        for name in original.__qualname__.split("."):
            candidates = [value for value in code.co_consts if isinstance(value, types.CodeType) and value.co_name == name]
            require(len(candidates) == 1, "mutation_code_identity:" + attribute)
            code = candidates[0]
    function = types.FunctionType(code, original.__globals__, original.__name__, original.__defaults__, original.__closure__)
    function.__kwdefaults__ = original.__kwdefaults__
    return contextmanager(function) if hasattr(getattr(owner, attribute), "__wrapped__") else function


def source_mutations(base: Path) -> dict[str, Any]:
    from orchestration import ecs_stage
    from scripts.lakehouse import pipeline_build, session, silver_quality
    from scripts.orchestration import ecs_stage_contract_cases
    from services.hai.bootstrap import integrity, outputs, redaction, transfer

    probes: tuple[tuple[Any, str, tuple[tuple[str, str], ...], Callable[[Path], None], str, str], ...] = (
        (
            integrity,
            "extract_archive",
            ((" or bool(entry.mode & 0o111) != member.executable", ""),),
            archives,
            "missing_refusal:unsafe_archive",
            "omit_extraction_mode",
        ),
        (
            integrity,
            "extract_archive",
            (("if digest.hexdigest() != member.sha256:", "if False:"),),
            archives,
            "missing_refusal:archive_digest",
            "omit_extraction_digest",
        ),
        (
            integrity,
            "extract_archive",
            (
                (
                    "if error.errno in (errno.ENOSPC, errno.EDQUOT):",
                    "workspace_result_enabled = False\n        if workspace_result_enabled and error.errno in (errno.ENOSPC, errno.EDQUOT):",
                ),
            ),
            archives,
            "extraction_error_code:disk:open:unsafe_archive",
            "relabel_workspace_capacity_as_archive",
        ),
        (
            integrity,
            "extract_archive",
            (("if error.errno in (errno.ENOSPC, errno.EDQUOT):", "if error.errno == errno.ENOSPC:"),),
            archives,
            "extraction_error_code:quota:open:unsafe_archive",
            "omit_workspace_quota_result",
        ),
        (
            integrity,
            "extract_archive",
            (("if error.errno in (errno.ENOSPC, errno.EDQUOT):", "if error.errno == errno.EDQUOT:"),),
            archives,
            "extraction_error_code:disk:open:unsafe_archive",
            "omit_workspace_disk_result",
        ),
        (
            integrity,
            "extract_archive",
            (("if error.errno in (errno.ENOSPC, errno.EDQUOT):", "if error.errno is not None:"),),
            archives,
            "extraction_error_code:access:open:workspace_full",
            "treat_every_oserror_as_capacity",
        ),
        (
            integrity,
            "extract_archive",
            (('raise StageError("workspace_full", 72)', 'raise StageError("workspace_full")'),),
            archives,
            "extraction_error_exit:disk:open",
            "omit_workspace_failure_exit",
        ),
        (
            ecs_stage,
            "stage_job",
            (("scan_file(path, secret_values, deadline)", "pass"),),
            verified_command,
            "missing_refusal:secret_in_output",
            "omit_job_secret_scan",
        ),
        (
            ecs_stage,
            "stage_job",
            (('if mapped != job["inputs"] or digest != job["input_map_sha256"]:', "if False:"),),
            verified_command,
            "missing_refusal:input_map_digest",
            "omit_staged_map_digest",
        ),
        (
            ecs_stage,
            "validate_result",
            ((' or marker.get("task_arn") != task_arn', ""),),
            staged_controls,
            "missing_refusal:result_identity",
            "omit_result_task_identity",
        ),
        (
            ecs_stage,
            "validate_result",
            (
                (' or item["key"] != prefix + name', ""),
                ("prefix = try_prefix(", "_prefix = try_prefix("),
            ),
            staged_controls,
            "missing_refusal:result_path",
            "omit_result_key_prefix",
        ),
        (
            ecs_stage,
            "validate_result",
            (
                ("required = {item.name for item in declaration.required}", "_required = {item.name for item in declaration.required}"),
                ('if not exit_code and not (required | {"stdout.txt", "stderr.txt"}) <= result.keys():', "if False:"),
            ),
            staged_controls,
            "missing_refusal:missing_output",
            "omit_required_success_output",
        ),
        (
            outputs,
            "inspect_outputs",
            (('if sum(item["size"] for item in scratch) > declaration.scratch_gib * (1 << 30):', "if False:"),),
            declarations,
            "missing_refusal:scratch_bound",
            "omit_scratch_bound",
        ),
        (
            outputs,
            "inspect_outputs",
            (("if spec is None or size > spec.max_bytes:", "if spec is None:"),),
            declarations,
            "missing_refusal:output_bound",
            "omit_output_size_bound",
        ),
        (
            outputs,
            "publish_outputs",
            (('"discarded_scratch": scratch', '"discarded_scratch": scratch[:0]'),),
            publication,
            "discarded_scratch_recorded",
            "omit_discarded_scratch",
        ),
        (
            outputs,
            "publish_outputs",
            (("scan_file(root / spec.name, secrets, deadline)", "pass"),),
            publication,
            "failure_log_secret_withheld",
            "omit_failure_log_scan",
        ),
        (
            outputs,
            "publish_outputs",
            (("scan_file(path, secrets, deadline)", "pass"),),
            publication,
            "missing_refusal:secret_in_output",
            "omit_marker_secret_scan",
        ),
        (
            outputs,
            "inspect_outputs",
            (("for name in directories:", "for _name in directories:"), ("if not stat.S_ISDIR((Path(folder) / name).lstat().st_mode):", "if False:")),
            declarations,
            "missing_refusal:unsafe_output",
            "omit_output_directory_link_check",
        ),
        (
            outputs,
            "inspect_outputs",
            (("if relative in input_names and relative not in specs:", "if False:"),),
            declarations,
            "undeclared_output",
            "omit_input_name_exemption",
        ),
        (
            transfer.Transfer,
            "download",
            (
                (
                    "if (digest, size) != (reference.sha256, reference.size):",
                    "digest_check_enabled = False\n            if digest_check_enabled and (digest, size) != (reference.sha256, reference.size):",
                ),
            ),
            transfers,
            "missing_refusal:input_digest",
            "omit_download_digest",
        ),
        (
            transfer.Transfer,
            "upload",
            (('if path.name.endswith(".duckdb") and path.with_name(path.name + ".wal").exists():', "if False:"),),
            transfers,
            "missing_refusal:wal_present",
            "omit_upload_wal_check",
        ),
        (
            transfer.Transfer,
            "upload",
            (
                (
                    "if not stable or (final_digest, final_size) != (digest, size):",
                    "source_check_enabled = False\n        if source_check_enabled and (not stable or (final_digest, final_size) != (digest, size)):",
                ),
            ),
            transfers,
            "missing_refusal:source_changed",
            "omit_final_source_recheck",
        ),
    )
    probes += (
        (
            transfer.Transfer,
            "_parts",
            (("chunk = os.pread(descriptor, length, offset)", "chunk = os.pread(descriptor, length, 0)"), ('"offset": offset,', '"offset": 0,')),
            transfers,
            "multipart_content",
            "reuse_first_multipart_offset",
        ),
        (transfer.Transfer, "_parts", (('"number": len(result) + 1,\n', '"number": 1,\n'),), transfers, "stage_upload_failed", "reuse_first_part_number"),
        (
            transfer.Transfer,
            "_parts",
            (("base64.b64encode(hashlib.sha256(chunk).digest())", "base64.b64encode(bytes(32))"),),
            transfers,
            "stage_upload_failed",
            "send_wrong_part_checksum",
        ),
        (
            FakeS3,
            "complete",
            (("if any(len(body) < 5 << 20 for body in bodies[:-1]):", "if False:"),),
            multipart_limits,
            "expected EntityTooSmall, received success",
            "accept_undersized_nonfinal_part",
        ),
        (
            FakeS3,
            "upload_part",
            (("if not 1 <= number <= 10000:", "if number < 1:"),),
            multipart_limits,
            "expected InvalidPart, received success",
            "accept_part_number_above_limit",
        ),
        (
            session,
            "task_settings",
            (("if warehouse is not None and any(", "fixture_check_enabled = False\n    if fixture_check_enabled and any("),),
            engine_configuration,
            "wrong_fixture_credential_refusal",
            "omit_fixture_role_credential_refusal",
        ),
        (silver_quality, "engine_config", (("**duckdb_temp_settings()", "**{}"),), engine_configuration, "quality_temp_cap_wiring", "omit_quality_temp_cap"),
        (
            ecs_stage,
            "validate_result",
            (
                ('    validate_contract("ecs_outputs", marker, "invalid_result")\n', ""),
                (
                    "    return tuple(result[name] for name in sorted(result))",
                    '    validate_contract("ecs_outputs", marker, "invalid_result")\n    return tuple(result[name] for name in sorted(result))',
                ),
            ),
            result_field_types,
            "AttributeError",
            "construct_reference_before_schema",
        ),
        (
            transfer.Transfer,
            "_multipart",
            (('except StorageError:\n            raise StageError("stage_upload_failed", 72) from None', "except StorageError:\n            raise"),),
            denied_multipart_create,
            "storage_call_failed",
            "leak_multipart_create_error",
        ),
        (
            integrity,
            "held_directory",
            (
                (
                    "        yield descriptor\n",
                    '        try:\n            yield descriptor\n        except OSError:\n            raise StageError("unsafe_source") from None\n',
                ),
            ),
            held_body_errors,
            "caller_oserror_relabelled:directory",
            "relabel_directory_body_oserror",
        ),
        (
            integrity,
            "held_file",
            (
                (
                    "            yield descriptor\n",
                    "            try:\n                yield descriptor\n            except OSError:\n"
                    '                raise StageError("unsafe_source") from None\n',
                ),
            ),
            held_body_errors,
            "caller_oserror_relabelled:file",
            "relabel_file_body_oserror",
        ),
        (
            pipeline_build,
            "main",
            (("except TemporaryStorageError as error:", "except TemporaryStorageError:\n            raise\n        except OSError as error:"),),
            engine_configuration,
            "temporary_storage_settings_incomplete",
            "omit_build_temp_error_conversion",
        ),
        (
            redaction.Redactor,
            "_emit",
            (("any(variant in self.pending for variant in self.variants)", "all(variant in self.pending for variant in self.variants)"),),
            secret_streams,
            "split_redaction",
            "prefilter_requires_every_variant",
        ),
    )
    results: dict[str, Any] = {}
    for owner, attribute, edits, case, expected, label in probes:
        replacement = changed_function(owner, attribute, edits, base)
        with patch.object(owner, attribute, replacement):
            observed = observe({label: case}, base)[label]
        results[label] = {
            "caught": observed.get("cause") == expected,
            "expected_cause": expected,
            "observed": observed,
            "target": inspect.unwrap(getattr(owner, attribute)).__module__ + "." + attribute,
            "edits": edits,
        }
    for label, before, after, expected in (
        ("omit_external_job_identity", " or any(job.get(key) != value for key, value in expected.items())", "", "job_identity_binding"),
        (
            "omit_declaration_source_binding",
            "if member is None or member.sha256 != source[sha_key]:",
            "if member is None and source[sha_key]:",
            "declaration_source_binding",
        ),
    ):
        with tempfile.TemporaryDirectory(prefix=label + "_", dir=base) as temporary:
            baked = Path(temporary) / "baked"
            baked.mkdir()
            copies = []
            for name in ("__init__.py", "integrity.py", "outputs.py", "redaction.py", "transfer.py", "run.py"):
                path = baked / name
                source = (ROOT / "services/hai/bootstrap" / name).read_text()
                if name == "run.py":
                    require(source.count(before) == 1, "mutation_anchor:" + label)
                    source = source.replace(before, after, 1)
                path.write_text(source)
                copies.append(str(path))
            lint_mutant(tuple(Path(path) for path in copies), base, label)
            observed = observe({label: partial(verified_command, command=baked / "run.py")}, base)[label]
        results[label] = {
            "caught": observed.get("cause") == expected,
            "expected_cause": expected,
            "observed": observed,
            "target": "services/hai/bootstrap/run.py",
            "edits": ((before, after),),
        }
    schema = json.loads((ROOT / "data_contracts/orchestration/ecs_input_map.schema.json").read_bytes())
    schema["$defs"]["try_id"] = copy.deepcopy(schema["$defs"]["identifier"])
    original_validators = ecs_stage_contract_cases.validators
    with patch.object(ecs_stage_contract_cases, "validators", partial(original_validators, overrides={"ecs_input_map": schema})):
        observed = observe({"schema": try_identifiers}, base)["schema"]
    expected = "try_schema_boundary:ecs_job:leading_dash"
    results["diverge_try_identifier_schema"] = {"caught": observed.get("cause") == expected, "expected_cause": expected, "observed": observed}
    original_copy = shutil.copyfile

    def controller_dependency(source: Path, destination: Path) -> Path:
        original_copy(source, destination)
        if source.name == "ecs_engine.py":
            destination.write_text(destination.read_text() + '\n__import__("orchestration.pipeline_protocol")\n')
            lint_mutant((destination,), base, "restore_controller_import_in_helper")
        return destination

    with patch.object(shutil, "copyfile", controller_dependency):
        observed = observe({"mount": mounted_imports}, base)["mount"]
    expected = "scripts_only_imports"
    results["restore_controller_import_in_helper"] = {"caught": observed.get("cause") == expected, "expected_cause": expected, "observed": observed}
    return results


def main() -> int:
    """Save a repeatable light-check artifact without claiming HTTP, image or AWS acceptance."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", required=True, type=Path)
    args = parser.parse_args()
    destination = args.report.resolve()
    if not destination.is_relative_to(ROOT / "data/e2e/ecs_target"):
        parser.error("the report must stay under data/e2e/ecs_target")
    destination.parent.mkdir(parents=True, exist_ok=True)
    cases: dict[str, Callable[[Path], None]] = {
        "archive_integrity": archives,
        "immutable_transfers": transfers,
        "exhaustive_outputs": declarations,
        "stream_redaction": secret_streams,
        "isolated_entry": isolated_command,
        "staged_controls": staged_controls,
        "verified_command": verified_command,
        "result_publication": publication,
        "engine_configuration": engine_configuration,
    }
    frozen = fingerprints()
    observed = observe(cases, destination.parent)
    mutations: dict[str, Any] = {}
    from scripts.lakehouse import pipeline_build, session
    from services.hai.bootstrap import integrity, outputs, redaction, transfer

    probes: tuple[tuple[Any, str, Callable[..., Any], str, Callable[[Path], None], str, str], ...] = (
        (integrity, "code_digest", lambda *args: "0" * 64, "archive_integrity", archives, "execution_bit_bound", "ignore_executable_mode"),
        (integrity, "dbt_digest", lambda *args: "0" * 64, "archive_integrity", archives, "dbt_digest_parity", "wrong_snapshot_framing"),
        (redaction.Redactor, "feed", lambda self, block: block, "stream_redaction", secret_streams, "split_redaction", "emit_before_redaction"),
        (
            transfer.Transfer,
            "_existing",
            lambda self, key, size, digest, deadline: self.reader.invoke("head", {"key": key}, deadline)["version_id"],
            "immutable_transfers",
            transfers,
            "missing_refusal:stage_conflict",
            "trust_existing_metadata_without_comparison",
        ),
        (transfer.Transfer, "_abort", lambda *args: None, "immutable_transfers", transfers, "multipart_aborts", "omit_multipart_abort"),
        (
            outputs.fnmatch,
            "fnmatchcase",
            lambda *args: True,
            "exhaustive_outputs",
            declarations,
            "missing_refusal:undeclared_output",
            "treat_every_file_as_scratch",
        ),
        (outputs, "scan_file", lambda *args: None, "result_publication", publication, "secret_withholds_all_results", "omit_output_secret_scan"),
    )
    probes += (
        (
            session,
            "validate_task_credentials",
            lambda *args, **kwargs: False,
            "engine_configuration",
            engine_configuration,
            "native_container_provider",
            "omit_native_container_provider",
        ),
        (
            pipeline_build,
            "build_target",
            lambda *args: "fixture",
            "engine_configuration",
            engine_configuration,
            "actual_bounded_dbt_target",
            "omit_bounded_build_profile",
        ),
    )
    probes += (
        (
            pipeline_build,
            "build_target",
            lambda layer, mode: "fixture_bounded",
            "engine_configuration",
            engine_configuration,
            "gold_fixture_pinned_profile",
            "replace_gold_pinned_profile_with_fixture",
        ),
    )
    for owner, attribute, replacement, name, case, cause, label in probes:
        with patch.object(owner, attribute, replacement):
            outcome = observe({name: case}, destination.parent)[name]
        mutations[label] = {"caught": outcome.get("cause") == cause, "expected_cause": cause, "observed": outcome}
    mutations.update(source_mutations(destination.parent))
    restored = observe(cases, destination.parent)
    hashes = fingerprints()
    passed = (
        all(row["passed"] for row in observed.values()) and all(row["passed"] for row in restored.values()) and all(row["caught"] for row in mutations.values())
    )
    passed = passed and frozen == hashes
    record = {
        "created_utc": datetime.now(UTC).isoformat(),
        "classification": "Synthetic",
        "proof": "filesystem_and_storage_model",
        "cases": observed,
        "mutations": mutations,
        "restored_cases": restored,
        "source_hashes": hashes,
        "candidate_unchanged": frozen == hashes,
        "base_commit": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, timeout=5, check=True).stdout.strip(),
        "dependencies": {"python": sys.version.split()[0], "ruff": version("ruff"), "mypy": version("mypy")},
        "reproduce": f"python -B -m scripts.orchestration.ecs_stage_runtime_cases --report {destination.relative_to(ROOT)}",
        "passed": passed,
        "cleanup": "done",
        "limits": [
            "No HTTP transport or cloud calls",
            "No native image or engine execution",
            "No identity premise or grant delivery",
            "No complete numbered-case acceptance",
        ],
    }
    destination.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    (destination.parent / "retention.json").write_text(
        json.dumps(
            {
                "class": 4,
                "purpose": "Verified staging runtime evidence",
                "created": record["created_utc"],
                "end_condition": "The coordinating agent no longer cites this increment",
            },
            indent=2,
        )
        + "\n"
    )
    sys.stdout.write(json.dumps({"passed": passed, "cases": len(observed), "mutations": len(mutations), "report": str(destination.relative_to(ROOT))}) + "\n")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
