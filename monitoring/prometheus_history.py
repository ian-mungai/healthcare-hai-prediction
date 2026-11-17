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
scratch folder (on the same volume), checks size and SHA-256 against the manifest, then the ``meta.json`` identity and the whole header (magic number, supported
version and length) of the index, chunk and tombstone files, and moves the folder into place with one rename. An existing block is never replaced and a block
whose time range overlaps one there is skipped. A compacted block whose compaction record names archived blocks as its parents or sources and covers their
ranges is placed instead of them, so no range is lost. A block that lacks any object in S3 (the lifecycle cut it, also between the listing and the read) is
skipped and reported; a block that fails a check, a malformed manifest and an access failure stop the restore with the block's name. The restore runs while
Prometheus is stopped (scripts/infrastructure/host_session.py holds its service at zero first).
"""

from __future__ import annotations

import argparse
import bisect
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
INDEX_FORMATS = (1, 2, 3)
TOMBSTONES_FORMATS = (1,)
CHUNKS_FORMATS = (1,)
INDEX_HEADER = 5
TOMBSTONES_HEADER = 5
CHUNKS_HEADER = 8
GONE = ("NoSuchKey", "404", "NotFound")
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
    superseded: list[str] = field(default_factory=list)
    incomplete: list[str] = field(default_factory=list)
    unfinished: list[str] = field(default_factory=list)
    uncovered: list[list[int]] = field(default_factory=list)


class Vanished(Exception):
    """A block folder or file that Prometheus deleted during the pass."""


class Gone(Exception):
    """An object of S3 that the listing showed and that is not there any more (a lifecycle expiry between the listing and the read)."""


@dataclass(frozen=True)
class Candidate:
    """A block of S3 that is complete as listed: its manifest, its time range and the blocks its compaction record says it replaced."""

    name: str
    group: dict[str, int]
    manifest: dict[str, dict[str, Any]]
    low: int
    high: int
    level: int
    covered: frozenset[str]


@dataclass(frozen=True)
class Held:
    """A time range that a block of the data folder (or one restored in this run) holds, and the blocks it stands for."""

    low: int
    high: int
    covered: frozenset[str]


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


def open_object(client: Any, bucket: str, name: str, file: str) -> Any:
    """The body of one object. An object that is not there raises ``Gone``; any other failure (access, network) raises ``HistoryError``."""
    try:
        return client.get_object(Bucket=bucket, Key=f"{PREFIX}{name}/{file}")["Body"]
    except Exception as problem:
        if getattr(problem, "response", {}).get("Error", {}).get("Code") in GONE:
            raise Gone(f"{name}/{file}") from problem
        raise HistoryError(f"{name}/{file}: download failed: {problem}") from problem


def download(client: Any, bucket: str, name: str, file: str, expected: dict[str, Any], target: Path) -> None:
    """Fetch one object into ``target`` and check its size and SHA-256 against the manifest."""
    body = open_object(client, bucket, name, file)
    digest, size = hashlib.sha256(), 0
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("wb") as handle:
        while chunk := body.read(READ_SIZE):
            handle.write(chunk)
            digest.update(chunk)
            size += len(chunk)
    sha_check(name, file, size, digest.hexdigest(), expected)


def read_manifest(client: Any, bucket: str, name: str, sizes: dict[str, int]) -> dict[str, dict[str, Any]]:
    """The manifest of a block. It is not there any more: ``Gone``. It cannot be read or is malformed: ``HistoryError``."""
    body = open_object(client, bucket, name, "manifest.json")
    try:
        manifest = json.loads(body.read(READ_SIZE))
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


def check_header(name: str, label: str, path: Path, magic: int, versions: tuple[int, ...], length: int) -> None:
    """The whole header of a file: at least ``length`` bytes, the magic number and a version that the pinned Prometheus reads."""
    head = header(path, length)
    if len(head) < length or int.from_bytes(head[:4], "big") != magic or head[4] not in versions:
        raise HistoryError(f"{name}: the {label} header is not a complete Prometheus header of a supported version")


def check_meta(name: str, meta: dict[str, Any] | None) -> tuple[int, int]:
    """The identity and the time range of a block from its meta.json."""
    if meta is None or meta.get("ulid") != name or meta.get("version") != 1:
        raise HistoryError(f"{name}: meta.json names another ulid or version")
    low, high = meta.get("minTime"), meta.get("maxTime")
    if not isinstance(low, int) or not isinstance(high, int) or low >= high:
        raise HistoryError(f"{name}: meta.json has no valid time range")
    return low, high


def verify_block(name: str, folder: Path, files: dict[str, dict[str, Any]]) -> tuple[int, int]:
    """Check the identity and the file headers of a downloaded block; answer its time range."""
    meta = read_meta(folder)
    low, high = check_meta(name, meta)
    check_header(name, "index", folder / "index", INDEX_MAGIC, INDEX_FORMATS, INDEX_HEADER)
    check_header(name, "tombstones", folder / "tombstones", TOMBSTONES_MAGIC, TOMBSTONES_FORMATS, TOMBSTONES_HEADER)
    chunks = [file for file in files if file.startswith("chunks/")]
    for file in chunks:
        check_header(name, f"chunk file {file}", folder / file, CHUNKS_MAGIC, CHUNKS_FORMATS, CHUNKS_HEADER)
    if (meta or {}).get("stats", {}).get("numChunks", 0) > 0 and not chunks:
        raise HistoryError(f"{name}: meta.json counts chunks but the manifest lists no chunk file")
    return low, high


def hand_over(folder: Path, owner: tuple[int, int]) -> None:
    for path in [folder, *folder.rglob("*")]:
        os.chown(path, *owner)
        path.chmod(0o750 if path.is_dir() else 0o640)


def covered_by(name: str, meta: dict[str, Any]) -> frozenset[str]:
    """The block and every block its compaction record names as a source or a parent."""
    compaction = meta.get("compaction")
    compaction = compaction if isinstance(compaction, dict) else {}
    parents = [item.get("ulid") for item in compaction.get("parents", []) if isinstance(item, dict)]
    return frozenset(item for item in [name, *compaction.get("sources", []), *parents] if isinstance(item, str))


def level_of(meta: dict[str, Any]) -> int:
    compaction = meta.get("compaction")
    level = compaction.get("level") if isinstance(compaction, dict) else None
    return level if isinstance(level, int) else 1


def local_ranges(data: Path) -> list[Held]:
    held = []
    for block in sorted(data.iterdir()):
        meta = read_meta(block) if ULID.fullmatch(block.name) and block.is_dir() else None
        if meta is not None and isinstance(meta.get("minTime"), int) and isinstance(meta.get("maxTime"), int):
            held.append(Held(meta["minTime"], meta["maxTime"], covered_by(block.name, meta)))
    return held


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


def candidate(client: Any, bucket: str, name: str, group: dict[str, int]) -> Candidate | None:
    """Read the manifest and the meta.json of a block of S3. ``None``: the block is incomplete (an object is missing or expired) and is skipped."""
    if "manifest.json" not in group:
        return None
    try:
        manifest = read_manifest(client, bucket, name, {f"{PREFIX}{name}/{file}": size for file, size in group.items()})
        if any(file not in group for file in manifest):
            return None
        for file, expected in manifest.items():
            if group[file] != expected["size"]:
                raise HistoryError(f"{name}/{file}: size {group[file]} in S3 differs from the manifest's {expected['size']}")
        digest, size, raw = hashlib.sha256(), 0, b""
        body = open_object(client, bucket, name, "meta.json")
        while chunk := body.read(READ_SIZE):
            raw, size = raw + chunk, size + len(chunk)
            digest.update(chunk)
        sha_check(name, "meta.json", size, digest.hexdigest(), manifest["meta.json"])
    except Gone:
        return None
    try:
        meta = json.loads(raw)
    except ValueError:
        raise HistoryError(f"{name}: meta.json is not JSON") from None
    low, high = check_meta(name, meta if isinstance(meta, dict) else None)
    return Candidate(name, group, manifest, low, high, level_of(meta), covered_by(name, meta))


def prepare(client: Any, bucket: str, found: Candidate, partial: Path, owner: tuple[int, int] | None) -> bool:
    """Download and verify one block into ``partial``. ``False``: an object expired during the download and the block is incomplete."""
    shutil.rmtree(partial, ignore_errors=True)
    try:
        for file, expected in found.manifest.items():
            download(client, bucket, found.name, file, expected, partial / file)
    except Gone:
        shutil.rmtree(partial, ignore_errors=True)
        return False
    verify_block(found.name, partial, found.manifest)
    if owner is not None:
        hand_over(partial, owner)
    return True


def covering(held: list[Held], found: Candidate) -> tuple[bool, bool]:
    """Whether ``found`` overlaps a range in ``held`` and whether one of those ranges is a block that replaced it and covers its whole range."""
    overlapped = [item for item in held if found.low < item.high and item.low < found.high]
    return bool(overlapped), any(found.name in item.covered and item.low <= found.low and item.high >= found.high for item in overlapped)


def union(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """The ranges as sorted, disjoint ranges."""
    merged: list[tuple[int, int]] = []
    for low, high in sorted(ranges):
        if merged and low <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], high))
        else:
            merged.append((low, high))
    return merged


def minus(ranges: list[tuple[int, int]], holes: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """The parts of the disjoint ``ranges`` that no range of ``holes`` covers."""
    left = list(ranges)
    for hole_low, hole_high in union(holes):
        left = [piece for low, high in left for piece in ((low, min(high, hole_low)), (max(low, hole_high), high)) if piece[0] < piece[1]]
    return left


def choose(blocks: list[Candidate]) -> list[Candidate]:
    """The set of pairwise disjoint blocks that covers the most time; among equal sets the one with fewer blocks, then with higher compaction levels.

    This is weighted interval scheduling over the blocks by end time. A replacement that covers more than its parents wins, parents win over a replacement
    that leaves part of them or of a neighbour uncovered, and no two chosen blocks overlap.
    """
    ordered = sorted(blocks, key=lambda item: (item.high, item.low, item.name))
    ends = [item.high for item in ordered]
    best: list[tuple[int, int, int]] = [(0, 0, 0)]
    before: list[int] = [0]
    took: list[bool] = [False]
    for block in ordered:
        last = bisect.bisect_right(ends, block.low)
        base = best[last]
        with_block = (base[0] + block.high - block.low, base[1] - 1, base[2] + block.level)
        before.append(last)
        took.append(with_block > best[-1])
        best.append(with_block if took[-1] else best[-1])
    chosen: list[Candidate] = []
    index = len(ordered)
    while index > 0:
        if took[index]:
            chosen.append(ordered[index - 1])
            index = before[index]
        else:
            index -= 1
    return sorted(chosen, key=lambda item: (item.low, item.name))


def restore(client: Any, bucket: str, data: Path, scratch: Path, owner: tuple[int, int] | None = None, until: float | None = None) -> RestoreReport:
    """Bring the verified blocks of S3 back into ``data`` that it lacks, and no block over one that is there.

    Blocks that overlap a block of the data folder are skipped, never replaced. Of the others the restore places the set of pairwise disjoint blocks that covers
    the most time (``choose``), so a compacted block that replaced archived blocks wins over them only when it covers at least what they cover, and a block that
    covers less than its parents or cuts into a neighbour loses to them. When no set of disjoint blocks covers everything the archive covers, the restore stops
    before it places any block. What the local blocks keep from being restored is listed in ``uncovered``. A block that lacks an object in S3 (the lifecycle cut
    it) is skipped and reported, and the choice is made again without it.
    """
    data.mkdir(parents=True, exist_ok=True)
    scratch.mkdir(parents=True, exist_ok=True)
    same_volume(data, scratch)
    if owner is not None:
        os.chown(data, *owner)
        data.chmod(0o750)
    groups = groups_of(listing(client, bucket))
    report, held = RestoreReport(), local_ranges(data)
    found: list[Candidate] = []
    for name, group in sorted(groups.items()):
        check_deadline(until, f"reading {name}")
        if "meta.json" not in group:
            report.unfinished.append(name)
        elif (data / name).exists():
            report.present.append(name)
        elif (block := candidate(client, bucket, name, group)) is None:
            report.incomplete.append(name)
        else:
            found.append(block)
    known = {block.name: block for block in found}
    pool: list[Candidate] = []
    for block in found:
        overlapped, replaced = covering(held, block)
        if replaced:
            report.superseded.append(block.name)
        elif overlapped:
            report.overlapping.append(block.name)
        else:
            pool.append(block)
    prepared: set[str] = set()
    try:
        while True:
            chosen = choose(pool)
            gaps = minus(union([(block.low, block.high) for block in pool]), [(block.low, block.high) for block in chosen])
            if gaps:
                raise HistoryError(f"no set of archived blocks without overlap covers the whole archive (missing {gaps}); nothing was restored")
            lost = None
            for block in chosen:
                check_deadline(until, f"restoring {block.name}")
                if block.name not in prepared:
                    if prepare(client, bucket, block, scratch / f"{block.name}{PARTIAL_SUFFIX}", owner):
                        prepared.add(block.name)
                    else:
                        lost = block
                        break
            if lost is None:
                break
            pool.remove(lost)
            report.incomplete.append(lost.name)
        for block in chosen:
            try:
                os.rename(scratch / f"{block.name}{PARTIAL_SUFFIX}", data / block.name)
            except OSError as problem:
                raise HistoryError(f"{block.name}: the block could not move into place: {problem}") from problem
            prepared.discard(block.name)
            report.restored.append(block.name)
    finally:
        for name in prepared:
            shutil.rmtree(scratch / f"{name}{PARTIAL_SUFFIX}", ignore_errors=True)
    standing = [(item.low, item.high, item.covered.union(*(known[name].covered for name in item.covered if name in known))) for item in chosen]
    for block in pool:
        if block not in chosen:
            replaced = any(block.name in covered and low <= block.low and block.high <= high for low, high, covered in standing)
            (report.superseded if replaced else report.overlapping).append(block.name)
    placed = [(block.low, block.high) for block in chosen] + [(item.low, item.high) for item in held]
    report.uncovered = [[low, high] for low, high in minus(union([(block.low, block.high) for block in found if block.name not in report.incomplete]), placed)]
    for names in (report.restored, report.present, report.overlapping, report.superseded, report.incomplete, report.unfinished):
        names.sort()
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
