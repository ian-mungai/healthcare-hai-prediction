"""The bounded data scan of the bronze cutover's verification (failure mode V1): one table per namespace is read through the storage identity
that AWS read jobs use, from the manifest list down to one data file.

``scan_namespace`` takes the table metadata that the AWS reader identity loaded, reads its current manifest list and one data manifest, decodes
both as Avro object container files and reads the first live data file's head and tail by byte range. A manifest list or manifest that is not
Avro, an absent or unreadable object, a data file whose size differs from its manifest entry and a Parquet file without its magic bytes all
fail with the ``data_read`` cause. Each object is read within the step deadline and at most ``register_aws_catalog.MAX_RECORD`` bytes of it.
``RoleStorage`` reads through the lakehouse reader role's short session instead of the project profile; the session values stay in memory.
"""

from __future__ import annotations

import json
import re
import tempfile
import time
import urllib.parse
import zlib
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

from orchestration import pipeline_session
from scripts.acquisition.s3_store import AwsCli, StorageError
from scripts.lakehouse import register_aws_catalog as reg
from scripts.process import CompletedProcess, run_command

AVRO_MAGIC = b"Obj\x01"
PARQUET_MAGIC = b"PAR1"
CONTENT_RANGE = re.compile(r"bytes \d+-\d+/(\d+)")
# Manifest entry status 2 is a deleted file; manifest-list content 0 holds data files, 1 delete files.
DELETED = 2


class Storage(Protocol):
    """Bounded object reads through one storage identity."""

    @property
    def identity(self) -> str:
        """The identity's label, recorded in the receipt."""
        ...

    def read(self, location: str, byte_range: str | None, *, deadline: float) -> tuple[bytes, int]:
        """Return the bytes of the object (or of the HTTP byte range) and the object's whole size."""
        ...


def refuse(message: str) -> reg.RegistrationError:
    return reg.RegistrationError("data_read", message)


class Decoder:
    """A position in Avro binary data with the named types seen so far."""

    def __init__(self, data: bytes) -> None:
        self.data, self.position = data, 0
        self.named: dict[str, Any] = {}

    def take(self, size: int) -> bytes:
        if size < 0 or self.position + size > len(self.data):
            raise refuse("an Avro value runs past its block")
        chunk = self.data[self.position : self.position + size]
        self.position += size
        return chunk

    def long(self) -> int:
        shift, value = 0, 0
        while True:
            byte = self.take(1)[0]
            value |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return (value >> 1) ^ -(value & 1)
            shift += 7
            if shift > 63:
                raise refuse("an Avro integer is longer than 64 bits")

    def value(self, schema: Any) -> Any:
        if isinstance(schema, list):
            return self.value(schema[self.index(len(schema))])
        if isinstance(schema, str):
            return self.primitive(schema)
        if not isinstance(schema, dict):
            raise refuse("an Avro schema node is malformed")
        kind = schema.get("type")
        if kind == "record":
            self.named[str(schema.get("name"))] = schema
            return {str(field["name"]): self.value(field["type"]) for field in schema.get("fields") or []}
        if kind == "array":
            return self.items(lambda: self.value(schema["items"]))
        if kind == "map":
            return dict(self.items(lambda: (self.text(), self.value(schema["values"]))))
        if kind == "enum":
            self.named[str(schema.get("name"))] = schema
            return schema["symbols"][self.index(len(schema["symbols"]))]
        if kind == "fixed":
            self.named[str(schema.get("name"))] = schema
            return self.take(int(schema["size"]))
        return self.value(kind)

    def primitive(self, name: str) -> Any:
        if name == "null":
            return None
        if name == "boolean":
            return self.take(1) != b"\x00"
        if name in ("int", "long"):
            return self.long()
        if name == "float":
            return self.take(4)
        if name == "double":
            return self.take(8)
        if name == "bytes":
            return self.take(self.long())
        if name == "string":
            return self.text()
        if name in self.named:
            return self.value(self.named[name])
        raise refuse(f"an Avro schema names the unknown type {name!r}")

    def index(self, count: int) -> int:
        index = self.long()
        if not 0 <= index < count:
            raise refuse("an Avro union or enum index is out of range")
        return index

    def text(self) -> str:
        try:
            return self.take(self.long()).decode()
        except UnicodeDecodeError:
            raise refuse("an Avro string is not UTF-8") from None

    def items[T](self, read: Callable[[], T]) -> list[T]:
        """The items of an array or map: blocks of a count and that many items, ended by a zero count (a negative count carries a size)."""
        result: list[T] = []
        while (count := self.long()) != 0:
            if count < 0:
                count = -count
                self.long()
            result += [read() for _ in range(count)]
        return result


def named_types(schema: Any, found: dict[str, Any]) -> dict[str, Any]:
    """Every named record, enum and fixed type of a schema, so a later reference by name decodes in any branch order."""
    if isinstance(schema, list):
        for branch in schema:
            named_types(branch, found)
    elif isinstance(schema, dict):
        if schema.get("type") in ("record", "enum", "fixed") and "name" in schema:
            found[str(schema["name"])] = schema
        for field in schema.get("fields") or []:
            named_types(field.get("type"), found)
        for key in ("items", "values"):
            if key in schema:
                named_types(schema[key], found)
        if isinstance(schema.get("type"), (dict, list)):
            named_types(schema["type"], found)
    return found


def avro_records(raw: bytes, what: str) -> list[Any]:
    """Decode every record of an Avro object container file with the null or deflate codec; anything else fails with ``data_read``."""
    if not raw.startswith(AVRO_MAGIC):
        raise refuse(f"the {what} is not an Avro object container file")
    try:
        header = Decoder(raw)
        header.take(len(AVRO_MAGIC))
        metadata = header.value({"type": "map", "values": "bytes"})
        sync = header.take(16)
        schema = json.loads(metadata["avro.schema"])
        codec = metadata.get("avro.codec", b"null").decode()
        if codec not in ("null", "deflate"):
            raise refuse(f"the {what} uses the Avro codec {codec}, which the scan does not read")
        records: list[Any] = []
        position = header.position
        while position < len(raw):
            block = Decoder(raw)
            block.position = position
            count, size = block.long(), block.long()
            payload = block.take(size)
            if block.take(16) != sync:
                raise refuse(f"the {what} has a block without its sync marker")
            position = block.position
            body = Decoder(zlib.decompress(payload, -15) if codec == "deflate" else payload)
            body.named = named_types(schema, {})
            records += [body.value(schema) for _ in range(count)]
        return records
    except (KeyError, ValueError, TypeError, zlib.error) as error:
        raise refuse(f"the {what} is not a readable Avro file ({type(error).__name__})") from None


def read_object(storage: Storage, location: str, bucket: str, deadline: float, clock: reg.Clock, what: str) -> bytes:
    body, size = read_range(storage, location, bucket, None, deadline, clock, what)
    if not 0 < size <= reg.MAX_RECORD or len(body) != size:
        raise refuse(f"the {what} is empty, incomplete or larger than 32 MiB")
    return body


def read_range(storage: Storage, location: str, bucket: str, byte_range: str | None, deadline: float, clock: reg.Clock, what: str) -> tuple[bytes, int]:
    try:
        target = reg.bucket_location(location, bucket)
        return reg.bounded(lambda: storage.read(target, byte_range, deadline=deadline), deadline, clock)
    except reg.RegistrationError as error:
        if error.code == "deadline":
            raise
        raise refuse(f"reading the {what} through {storage.identity} failed ({error.code})") from None


def scan_namespace(storage: Storage, metadata: Mapping[str, Any], snapshot_id: int, bucket: str, deadline: float, clock: reg.Clock) -> dict[str, Any]:
    """Read one live data file of the snapshot through the storage identity; the result names the counts, never a location."""
    manifest_list = reg.snapshots(metadata, "data_read")[snapshot_id].get("manifest-list")
    manifests = [
        item
        for item in avro_records(read_object(storage, str(manifest_list), bucket, deadline, clock, "manifest list"), "manifest list")
        if isinstance(item, dict) and item.get("content", 0) == 0 and isinstance(item.get("manifest_path"), str)
    ]
    for manifest in manifests:
        entries = avro_records(read_object(storage, manifest["manifest_path"], bucket, deadline, clock, "manifest"), "manifest")
        for entry in entries:
            data_file = entry.get("data_file") if isinstance(entry, dict) else None
            if not isinstance(data_file, dict) or entry.get("status") == DELETED or data_file.get("content", 0) != 0:
                continue
            path, expected, file_format = data_file.get("file_path"), data_file.get("file_size_in_bytes"), str(data_file.get("file_format", ""))
            if not isinstance(path, str) or type(expected) is not int or expected < 8:
                raise refuse("a manifest entry has no data file path or size")
            head, size = read_range(storage, path, bucket, "bytes=0-3", deadline, clock, "data file")
            tail, _ = read_range(storage, path, bucket, f"bytes={expected - 4}-{expected - 1}", deadline, clock, "data file")
            if size != expected:
                raise refuse("a data file's size differs from its manifest entry")
            if file_format.upper() == "PARQUET" and (head, tail) != (PARQUET_MAGIC, PARQUET_MAGIC):
                raise refuse("a data file is not a Parquet file")
            return {"manifests": len(manifests), "data_file_bytes": size, "storage_identity": storage.identity}
    raise refuse("the current snapshot has no live data file to scan")


class RoleStorage:
    """Object reads through the lakehouse reader role's short session, the S3 identity of AWS read jobs, instead of the project profile."""

    identity = "lakehouse_reader"

    def __init__(
        self,
        settings: Mapping[str, str],
        session: pipeline_session.RoleSession,
        *,
        process: Callable[..., CompletedProcess[str]] | None = None,
        clock: reg.Clock = time.monotonic,
    ) -> None:
        self.session, self.process, self.clock = session, process, clock
        self.client = AwsCli(dict(settings), runner=self.run)

    def run(self, command: Sequence[str], *, env: Mapping[str, str], timeout: float, **_: Any) -> CompletedProcess[str]:
        """The AWS command without the profile: the session values select the identity and stay in this process's memory."""
        arguments = list(command)
        index = arguments.index("--profile")
        del arguments[index : index + 2]
        values = {
            "AWS_ACCESS_KEY_ID": self.session.access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.session.secret_access_key,
            "AWS_SESSION_TOKEN": self.session.session_token,
        }
        if self.process is not None:
            return self.process("aws", arguments[1:], env={**env, **values}, timeout=timeout)
        return run_command("aws", arguments[1:], env={**env, **values}, timeout=timeout)

    def read(self, location: str, byte_range: str | None, *, deadline: float) -> tuple[bytes, int]:
        key = urllib.parse.urlsplit(location).path.removeprefix("/")
        reg.check_deadline(deadline, self.clock)
        with tempfile.TemporaryDirectory(prefix="bronze_scan_") as scratch:
            local = Path(scratch) / "object"
            arguments = ["--key", key, *(["--range", byte_range] if byte_range else []), str(local)]
            try:
                result = self.client.call_bounded("s3api", "get-object", arguments, timeout=max(0.001, deadline - self.clock()))
            except StorageError as error:
                raise reg.RegistrationError("transport", f"reading a data object failed ({error.code})") from None
            if not local.exists() or local.stat().st_size > reg.MAX_RECORD:
                raise reg.RegistrationError("transport", "a data object read returned no body or more than 32 MiB")
            body = local.read_bytes()
        found = CONTENT_RANGE.fullmatch(str(result.get("ContentRange", "")))
        size = int(found.group(1)) if found else result.get("ContentLength")
        if type(size) is not int:
            raise reg.RegistrationError("transport", "a data object read returned no size")
        return body, size
