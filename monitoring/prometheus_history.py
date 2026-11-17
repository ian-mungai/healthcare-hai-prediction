"""Keep Prometheus history in S3: ship finished TSDB blocks create-only, and restore them verified before Prometheus starts.

    python -m monitoring.prometheus_history ship --data-dir /prometheus [--interval 300] [--once]
    python -m monitoring.prometheus_history restore --data-dir /host/prometheus --scratch-dir /host/prometheus_restore [--owner 65534:65534]

Environment: ``HISTORY_BUCKET_NAME`` and ``AWS_REGION``. Both run in the Airflow image (boto3 and the standard library only).

A finished block is a folder named by its ULID that Prometheus renamed into place after it wrote every file (``meta.json``, ``index``, ``tombstones`` and
``chunks/NNNNNN``); the data folder also holds the WAL, the head chunks and ``<ULID>.tmp-for-creation`` folders, which are never shipped. With min and max block
duration both 2h Prometheus never rewrites a finished block. A block goes to ``prometheus/blocks/<ULID>/<file>`` and every put is create-only
(``IfNoneMatch="*"``): an object that exists is kept, and a repeat run changes nothing. The files go first, then ``manifest.json`` (size and SHA-256 of every
file, including ``meta.json``) and ``meta.json`` last, so a block whose ``meta.json`` is in S3 is complete.
The shipper role holds only a create-only put and list.

The restore role holds only get and list. For each block with a ``meta.json`` that is not already a folder of the data folder it downloads every file into the
scratch folder (on the same volume), checks size and SHA-256 against the manifest, then the ``meta.json`` identity and the magic bytes of the index, chunk and
tombstone files, skips a block whose time range overlaps a block already there, and moves the folder into place with one rename. An existing block is never
replaced. A block that lacks a manifest file in S3 (the lifecycle cut it) is skipped and reported; a block that fails a check stops the restore with its name.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import re
import shutil
import signal
import sys
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

PREFIX = "prometheus/blocks/"
INDEX_MAGIC = 0xBAAAD700
CHUNKS_MAGIC = 0x85BD40DD
TOMBSTONES_MAGIC = 0x0130BA30
INDEX_FORMATS = (2, 3)
ULID = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")
FILE = re.compile(r"^(meta\.json|index|tombstones|manifest\.json|chunks/[0-9]{6,})$")
REQUIRED = ("meta.json", "index", "tombstones")
READ_SIZE = 1 << 20
DEFAULT_INTERVAL_SECONDS = 300
DEFAULT_PASS_SECONDS = 900
DEFAULT_RESTORE_SECONDS = 1500
PARTIAL_SUFFIX = ".partial"


class HistoryError(Exception):
    """A shipper or restore step that cannot continue."""


@dataclass
class ShipReport:
    shipped: list[str] = field(default_factory=list)
    present: list[str] = field(default_factory=list)
    vanished: list[str] = field(default_factory=list)


@dataclass
class RestoreReport:
    restored: list[str] = field(default_factory=list)
    present: list[str] = field(default_factory=list)
    overlapping: list[str] = field(default_factory=list)
    incomplete: list[str] = field(default_factory=list)
    unfinished: list[str] = field(default_factory=list)


class Vanished(Exception):
    """A block folder or file that Prometheus deleted during the pass."""


def file_digest(path: Path) -> tuple[int, str]:
    """The size and SHA-256 of a file, read in chunks."""
    digest, size = hashlib.sha256(), 0
    with path.open("rb") as handle:
        while chunk := handle.read(READ_SIZE):
            digest.update(chunk)
            size += len(chunk)
    return size, digest.hexdigest()


def check_deadline(until: float | None, what: str) -> None:
    if until is not None and time.monotonic() > until:
        raise HistoryError(f"the deadline passed before {what}")


def listing(client: Any, bucket: str, prefix: str = PREFIX) -> dict[str, int]:
    """Every key under ``prefix`` and its size."""
    found: dict[str, int] = {}
    token: str | None = None
    while True:
        call: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token is not None:
            call["ContinuationToken"] = token
        try:
            page = client.list_objects_v2(**call)
        except Exception as problem:
            raise HistoryError(f"listing {prefix} failed: {problem}") from problem
        found.update({item["Key"]: int(item["Size"]) for item in page.get("Contents", [])})
        token = page.get("NextContinuationToken")
        if not page.get("IsTruncated") or not token:
            return found


def read_meta(block: Path) -> dict[str, Any] | None:
    try:
        meta = json.loads((block / "meta.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return meta if isinstance(meta, dict) else None


def finished_blocks(data: Path) -> Iterator[Path]:
    """The folders of ``data`` that are complete blocks, oldest ULID first; tmp folders, the WAL and blocks that Prometheus marked deletable are left out."""
    for block in sorted(data.iterdir()):
        if not ULID.fullmatch(block.name) or block.is_symlink() or not block.is_dir():
            continue
        meta = read_meta(block)
        if meta is None or not (block / "index").is_file() or meta.get("compaction", {}).get("deletable"):
            continue
        yield block


def block_files(block: Path) -> list[str]:
    """The files of a block, relative, chunks first, then index and tombstones; ``meta.json`` is listed last. A file the format does not name is refused."""
    names = sorted(path.relative_to(block).as_posix() for path in block.rglob("*") if not path.is_dir())
    for name in names:
        if not FILE.fullmatch(name) or name == "manifest.json" or (block / name).is_symlink():
            raise HistoryError(f"{block.name}: unknown file {name}")
    missing = [name for name in REQUIRED if name not in names]
    if missing:
        raise HistoryError(f"{block.name}: lacks {', '.join(missing)}")
    return [name for name in names if name.startswith("chunks/")] + ["index", "tombstones", "meta.json"]


def put_new(client: Any, bucket: str, key: str, body: Any, size: int, existing: dict[str, int]) -> bool:
    """Create ``key`` once. An object that exists must have the same size; answer whether this call created it."""
    if key in existing:
        if existing[key] != size:
            raise HistoryError(f"{key} is in S3 with {existing[key]} bytes and differs from the local {size} bytes; nothing is overwritten")
        return False
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*")
    except Exception as problem:
        code = getattr(problem, "response", {}).get("Error", {}).get("Code")
        if code != "PreconditionFailed":
            raise HistoryError(f"upload of {key} failed: {problem}") from problem
        existing.update(listing(client, bucket, key))
        if existing.get(key) != size:
            raise HistoryError(f"{key} appeared in S3 with another size than the local {size} bytes; nothing is overwritten") from problem
        return False
    existing[key] = size
    return True


def ship_block(client: Any, bucket: str, block: Path, existing: dict[str, int]) -> None:
    """Upload one block: its files, then the manifest, then ``meta.json``."""
    names = block_files(block)
    try:
        digests = {name: file_digest(block / name) for name in names}
    except FileNotFoundError as problem:
        raise Vanished(block.name) from problem
    manifest = {"version": 1, "ulid": block.name, "files": {name: {"size": size, "sha256": sha} for name, (size, sha) in sorted(digests.items())}}
    body = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    base = f"{PREFIX}{block.name}/"
    for name in names[:-1]:
        try:
            with (block / name).open("rb") as handle:
                put_new(client, bucket, base + name, handle, digests[name][0], existing)
        except FileNotFoundError as problem:
            raise Vanished(block.name) from problem
    put_new(client, bucket, base + "manifest.json", body, len(body), existing)
    try:
        with (block / "meta.json").open("rb") as handle:
            put_new(client, bucket, base + "meta.json", handle, digests["meta.json"][0], existing)
    except FileNotFoundError as problem:
        raise Vanished(block.name) from problem


def ship_once(client: Any, bucket: str, data: Path, until: float | None = None) -> ShipReport:
    """One pass over the data folder: every finished block whose ``meta.json`` is not in S3 is uploaded."""
    report, existing = ShipReport(), listing(client, bucket)
    for block in finished_blocks(data):
        check_deadline(until, f"shipping {block.name}")
        if f"{PREFIX}{block.name}/meta.json" in existing:
            report.present.append(block.name)
            continue
        try:
            ship_block(client, bucket, block, existing)
        except Vanished:
            report.vanished.append(block.name)
            continue
        report.shipped.append(block.name)
    return report


def sha_check(name: str, file: str, size: int, digest: str, expected: dict[str, Any]) -> None:
    if size != expected["size"]:
        raise HistoryError(f"{name}/{file}: size {size} differs from the manifest's {expected['size']}")
    if digest != expected["sha256"]:
        raise HistoryError(f"{name}/{file}: sha256 differs from the manifest")


def download(client: Any, bucket: str, name: str, file: str, expected: dict[str, Any], target: Path) -> None:
    """Fetch one object into ``target`` and check its size and SHA-256 against the manifest."""
    try:
        body = client.get_object(Bucket=bucket, Key=f"{PREFIX}{name}/{file}")["Body"]
    except Exception as problem:
        raise HistoryError(f"{name}/{file}: download failed: {problem}") from problem
    digest, size = hashlib.sha256(), 0
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("wb") as handle:
        while chunk := body.read(READ_SIZE):
            handle.write(chunk)
            digest.update(chunk)
            size += len(chunk)
    sha_check(name, file, size, digest.hexdigest(), expected)


def read_manifest(client: Any, bucket: str, name: str, sizes: dict[str, int]) -> dict[str, dict[str, Any]]:
    try:
        raw = client.get_object(Bucket=bucket, Key=f"{PREFIX}{name}/manifest.json")["Body"].read(READ_SIZE)
        manifest = json.loads(raw)
    except Exception as problem:
        raise HistoryError(f"{name}: the manifest cannot be read: {problem}") from problem
    files = manifest.get("files") if isinstance(manifest, dict) else None
    if not isinstance(files, dict) or manifest.get("ulid") != name or not all(FILE.fullmatch(file) and file != "manifest.json" for file in files):
        raise HistoryError(f"{name}: the manifest is malformed")
    if any(not isinstance(value, dict) or not isinstance(value.get("size"), int) or not isinstance(value.get("sha256"), str) for value in files.values()):
        raise HistoryError(f"{name}: the manifest is malformed")
    if any(file not in files for file in REQUIRED):
        raise HistoryError(f"{name}: the manifest lacks a required file")
    extra = sorted(key for key in sizes if key.startswith(f"{PREFIX}{name}/") and key.removeprefix(f"{PREFIX}{name}/") not in {*files, "manifest.json"})
    if extra:
        raise HistoryError(f"{name}: S3 holds objects that the manifest does not list: {', '.join(extra)}")
    return {file: value for file, value in files.items()}


def header(path: Path, size: int) -> bytes:
    with path.open("rb") as handle:
        return handle.read(size)


def verify_block(name: str, folder: Path, files: dict[str, dict[str, Any]]) -> tuple[int, int]:
    """Check the identity and the file headers of a downloaded block; answer its time range."""
    meta = read_meta(folder)
    if meta is None or meta.get("ulid") != name or meta.get("version") != 1:
        raise HistoryError(f"{name}: meta.json names another ulid or version")
    low, high = meta.get("minTime"), meta.get("maxTime")
    if not isinstance(low, int) or not isinstance(high, int) or low >= high:
        raise HistoryError(f"{name}: meta.json has no valid time range")
    index = header(folder / "index", 5)
    if len(index) < 5 or int.from_bytes(index[:4], "big") != INDEX_MAGIC or index[4] not in INDEX_FORMATS:
        raise HistoryError(f"{name}: the index header is not a Prometheus index")
    if int.from_bytes(header(folder / "tombstones", 4), "big") != TOMBSTONES_MAGIC:
        raise HistoryError(f"{name}: the tombstones header is not a Prometheus tombstones file")
    chunks = [file for file in files if file.startswith("chunks/")]
    for file in chunks:
        head = header(folder / file, 5)
        if len(head) < 5 or int.from_bytes(head[:4], "big") != CHUNKS_MAGIC or head[4] != 1:
            raise HistoryError(f"{name}: the chunk file {file} header is not a Prometheus chunk segment")
    if meta.get("stats", {}).get("numChunks", 0) > 0 and not chunks:
        raise HistoryError(f"{name}: meta.json counts chunks but the manifest lists no chunk file")
    return low, high


def hand_over(folder: Path, owner: tuple[int, int]) -> None:
    for path in [folder, *folder.rglob("*")]:
        os.chown(path, *owner)
        path.chmod(0o750 if path.is_dir() else 0o640)


def local_ranges(data: Path) -> list[tuple[int, int]]:
    ranges = []
    for block in sorted(data.iterdir()):
        meta = read_meta(block) if ULID.fullmatch(block.name) and block.is_dir() else None
        if meta is not None and isinstance(meta.get("minTime"), int) and isinstance(meta.get("maxTime"), int):
            ranges.append((meta["minTime"], meta["maxTime"]))
    return ranges


def groups_of(keys: dict[str, int]) -> dict[str, dict[str, int]]:
    """The S3 objects by block. A key outside prometheus/blocks/<ULID>/<known file> stops the restore."""
    groups: dict[str, dict[str, int]] = {}
    for key, size in keys.items():
        name, _, file = key.removeprefix(PREFIX).partition("/")
        if not ULID.fullmatch(name) or not FILE.fullmatch(file):
            raise HistoryError(f"unexpected key {key} under {PREFIX}; nothing was restored")
        groups.setdefault(name, {})[file] = size
    return groups


def same_volume(first: Path, second: Path) -> None:
    if os.stat(first).st_dev != os.stat(second).st_dev:
        raise HistoryError(f"{first} and {second} are not on one volume, so a block cannot move into place by one rename")


def restore_block(
    client: Any, bucket: str, name: str, group: dict[str, int], data: Path, partial: Path, ranges: list[tuple[int, int]], owner: tuple[int, int] | None
) -> str:
    """Restore one block; answer ``restored``, ``overlapping`` or ``incomplete``."""
    manifest = read_manifest(client, bucket, name, {f"{PREFIX}{name}/{file}": size for file, size in group.items()})
    if any(file not in group for file in manifest):
        return "incomplete"
    for file, expected in manifest.items():
        if group[file] != expected["size"]:
            raise HistoryError(f"{name}/{file}: size {group[file]} in S3 differs from the manifest's {expected['size']}")
    shutil.rmtree(partial, ignore_errors=True)
    download(client, bucket, name, "meta.json", manifest["meta.json"], partial / "meta.json")
    meta = read_meta(partial) or {}
    low, high = meta.get("minTime"), meta.get("maxTime")
    if isinstance(low, int) and isinstance(high, int) and any(low < end and start < high for start, end in ranges):
        return "overlapping"
    for file, expected in manifest.items():
        if file != "meta.json":
            download(client, bucket, name, file, expected, partial / file)
    low, high = verify_block(name, partial, manifest)
    if any(low < end and start < high for start, end in ranges):
        return "overlapping"
    if owner is not None:
        hand_over(partial, owner)
    try:
        os.rename(partial, data / name)
    except OSError as problem:
        raise HistoryError(f"{name}: the block could not move into place: {problem}") from problem
    ranges.append((low, high))
    return "restored"


def restore(client: Any, bucket: str, data: Path, scratch: Path, owner: tuple[int, int] | None = None, until: float | None = None) -> RestoreReport:
    """Bring every verified block of S3 back into ``data`` that is not there yet."""
    data.mkdir(parents=True, exist_ok=True)
    scratch.mkdir(parents=True, exist_ok=True)
    same_volume(data, scratch)
    if owner is not None:
        os.chown(data, *owner)
        data.chmod(0o750)
    groups = groups_of(listing(client, bucket))
    report, ranges = RestoreReport(), local_ranges(data)
    for name, group in sorted(groups.items()):
        check_deadline(until, f"restoring {name}")
        partial = scratch / f"{name}{PARTIAL_SUFFIX}"
        if "meta.json" not in group:
            report.unfinished.append(name)
        elif (data / name).exists():
            report.present.append(name)
        else:
            try:
                outcome = restore_block(client, bucket, name, group, data, partial, ranges, owner)
            finally:
                shutil.rmtree(partial, ignore_errors=True)
            getattr(report, outcome).append(name)
    return report


def parse_owner(text: str | None) -> tuple[int, int] | None:
    if text is None:
        return None
    user, _, group = text.partition(":")
    if not user.isdigit() or not group.isdigit():
        raise HistoryError(f"--owner is user:group as numbers, not {text!r}")
    return int(user), int(group)


def make_client() -> Any:
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3", region_name=os.environ["AWS_REGION"], config=Config(connect_timeout=10, read_timeout=60, retries={"max_attempts": 4, "mode": "standard"})
    )


def bucket_name() -> str:
    name = os.environ.get("HISTORY_BUCKET_NAME", "")
    if not name:
        raise HistoryError("HISTORY_BUCKET_NAME is not set")
    return name


def say(event: str, **values: Any) -> None:
    sys.stdout.write(json.dumps({"event": event, **values}, sort_keys=True) + "\n")
    sys.stdout.flush()


def run_ship(arguments: argparse.Namespace) -> int:
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    client, bucket, data = make_client(), bucket_name(), Path(arguments.data_dir)
    while not stop.is_set():
        report = ship_once(client, bucket, data, time.monotonic() + arguments.pass_seconds)
        say("shipped", shipped=report.shipped, present=len(report.present), vanished=report.vanished)
        if arguments.once:
            return 0
        stop.wait(arguments.interval)
    return 0


def run_restore(arguments: argparse.Namespace) -> int:
    report = restore(
        make_client(),
        bucket_name(),
        Path(arguments.data_dir),
        Path(arguments.scratch_dir),
        parse_owner(arguments.owner),
        time.monotonic() + arguments.deadline_seconds,
    )
    say("restored", **{name: value for name, value in vars(report).items()})
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    commands = parser.add_subparsers(dest="command", required=True)
    ship = commands.add_parser("ship", help="upload finished blocks create-only, every --interval seconds")
    ship.add_argument("--data-dir", required=True)
    ship.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SECONDS)
    ship.add_argument("--pass-seconds", type=int, default=DEFAULT_PASS_SECONDS, help="deadline of one pass")
    ship.add_argument("--once", action="store_true")
    ship.set_defaults(run=run_ship)
    back = commands.add_parser("restore", help="download and verify the blocks that the data folder lacks")
    back.add_argument("--data-dir", required=True)
    back.add_argument("--scratch-dir", required=True)
    back.add_argument("--owner", help="user:group ids that own the restored blocks")
    back.add_argument("--deadline-seconds", type=int, default=DEFAULT_RESTORE_SECONDS)
    back.set_defaults(run=run_restore)
    arguments = parser.parse_args(argv)
    try:
        return int(arguments.run(arguments))
    except HistoryError as problem:
        sys.stderr.write(f"prometheus_history {arguments.command} failed: {problem}\n")
        return 1
    finally:
        with contextlib.suppress(Exception):
            sys.stdout.flush()


if __name__ == "__main__":
    sys.exit(main())
