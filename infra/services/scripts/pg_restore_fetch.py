"""Fetch step of the Postgres restore: write the dumps of the newest completed backup set to RESTORE_DIR, checked against its marker.

It is the command of the ``postgres_restore_fetch`` container of the postgres task (training image, ``python -I -c``). It reads the markers under
``pg_backups/complete/`` and takes the newest set whose marker is well formed (schema 1, its own run, every entry checked, every database that its own
``members`` record lists present) and whose every requested dump that the set holds exists with the size and SHA-256 its marker records. A set that was
whole when it was written stays usable after a database is enabled later: the new database has no dump in it, and the databases step creates it empty. A
marker with no ``members`` (written before the field existed) is read as before: it must hold at least one requested database, and each one it holds is
restored. A dump streams to disk in 1 MiB chunks, so its size is not held in memory. A set that fails a check is skipped and the next older one is tried (at
most FALLBACKS sets). A dump with no marker is never read, because the dump step writes the marker last. When no marker exists it writes nothing and exits
0: there is nothing to restore. When markers exist and none passes, it exits 1 with the reason, and the databases step then creates and starts nothing. It
writes each dump as ``<database>.dump`` and the run ID as ``RUN``; only the databases named in PG_DATABASES are read. The whole fetch has a deadline and
every S3 call has timeouts.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

PREFIX = "pg_backups"
RUN = re.compile(r"[0-9]{8}T[0-9]{6}Z")
DATABASE = re.compile(r"[a-z][a-z0-9_]{0,62}")
BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
FALLBACKS = 5
CHUNK = 1 << 20
MARKER_LIMIT = 1 << 20
HEX = re.compile(r"[a-f0-9]{64}")
FETCH_SECONDS = 900
MAX_BYTES = 4 * 1024**3


class Refused(Exception):
    """A set cannot be used."""


def marker_keys(client: Any, bucket: str) -> list[str]:
    """The marker keys under ``pg_backups/complete/``, newest first."""
    keys: list[str] = []
    token = None
    while True:
        arguments = {"Bucket": bucket, "Prefix": f"{PREFIX}/complete/", **({"ContinuationToken": token} if token else {})}
        page = client.list_objects_v2(**arguments)
        keys += [item["Key"] for item in page.get("Contents", []) if RUN.fullmatch(item["Key"].rsplit("/", 1)[-1].removesuffix(".json"))]
        if not page.get("IsTruncated"):
            return sorted(keys, reverse=True)
        token = page.get("NextContinuationToken")


def read_marker(client: Any, bucket: str, key: str, run: str, databases: list[str]) -> dict[str, dict[str, Any]]:
    """The checked entries of one marker. Raises Refused unless the marker is schema 1, names its own run, holds at least one requested database and, when it
    has a ``members`` record, that record lists exactly its entries, and every entry has the expected key, a whole-number size within the limit, a SHA-256
    and at least one table."""
    try:
        raw = client.get_object(Bucket=bucket, Key=key)["Body"].read(MARKER_LIMIT + 1)
        marker = json.loads(raw) if len(raw) <= MARKER_LIMIT else None
    except ValueError:
        marker = None
    if not isinstance(marker, dict) or marker.get("schema") != 1 or marker.get("run") != run or not isinstance(marker.get("databases"), dict):
        raise Refused(f"the marker of {run} is not a marker of this run")
    entries = marker["databases"]
    if not entries:
        raise Refused(f"the marker of {run} lists no database")
    for name, item in entries.items():
        size, tables = (item.get("bytes"), item.get("tables")) if isinstance(item, dict) else (None, None)
        good = (
            DATABASE.fullmatch(str(name))
            and isinstance(item, dict)
            and item.get("key") == f"{PREFIX}/{name}/{run}.dump"
            and type(size) is int
            and 0 < size <= MAX_BYTES
            and isinstance(item.get("sha256"), str)
            and HEX.fullmatch(item["sha256"])
            and type(tables) is int
            and tables >= 1
        )
        if not good:
            raise Refused(f"the marker of {run} has a malformed entry for {name}")
    recorded = marker.get("members")  # a marker written before the field existed has none and holds what its writer saw
    if recorded is not None and not (
        isinstance(recorded, list)
        and recorded
        and all(isinstance(name, str) and DATABASE.fullmatch(name) for name in recorded)
        and set(recorded) == set(entries)
    ):
        raise Refused(f"the marker of {run} has a membership that is malformed or differs from its entries")
    if not any(name in entries for name in databases):
        raise Refused(f"the marker of {run} lists none of the requested databases")
    return entries


def download(client: Any, bucket: str, item: dict[str, Any], path: Path, label: str, clock: Callable[[], float], deadline: float) -> None:
    """Stream one dump to ``path`` in bounded chunks, hashing it. It stops as soon as the body is longer than the marker says."""
    try:
        body = client.get_object(Bucket=bucket, Key=item["key"])["Body"]
    except Exception as error:
        if type(error).__name__ == "ClientError":
            raise Refused(f"the dump of {label} cannot be read") from None
        raise
    digest, total = hashlib.sha256(), 0
    with path.open("wb") as target:
        while block := body.read(CHUNK):
            total += len(block)
            if total > item["bytes"]:
                raise Refused(f"the dump of {label} is longer than its marker says")
            if clock() > deadline:
                raise Refused(f"the deadline of {FETCH_SECONDS} seconds passed")
            digest.update(block)
            target.write(block)
    if total != item["bytes"] or digest.hexdigest() != item["sha256"]:
        raise Refused(f"the dump of {label} differs from its marker")


def read_set(client: Any, bucket: str, key: str, databases: list[str], target: Path, clock: Callable[[], float], deadline: float) -> str:
    """Write the requested dumps of one set into a ``.partial`` folder and return its run ID; raises Refused when the set is not whole."""
    run = key.rsplit("/", 1)[-1].removesuffix(".json")
    entries = read_marker(client, bucket, key, run, databases)
    staging = target / f"{run}.partial"
    staging.mkdir(parents=True, exist_ok=True)
    try:
        for name in [name for name in databases if name in entries]:
            download(client, bucket, entries[name], staging / f"{name}.dump", f"{name} in {run}", clock, deadline)
    except Refused:
        for leftover in staging.glob("*"):
            leftover.unlink(missing_ok=True)
        raise
    return run


def run(bucket: str, databases: list[str], target: Path, client: Any, clock: Callable[[], float] = time.monotonic) -> int:
    """Fetch the newest usable set into ``target``; returns 0 (also when no backup exists) or 1 with the reason on standard error."""
    deadline = clock() + FETCH_SECONDS
    target.mkdir(parents=True, exist_ok=True)
    for old in [*target.glob("*.dump"), target / "RUN", *target.glob("*.partial/*")]:
        old.unlink(missing_ok=True)
    try:
        keys = marker_keys(client, bucket)
    except Exception as error:
        sys.stderr.write(f"pg_restore_fetch failed: the backup list cannot be read ({type(error).__name__})\n")
        return 1
    if not keys:
        sys.stdout.write("pg_restore_fetch: no completed backup; nothing to restore\n")
        return 0
    reasons = []
    for key in keys[:FALLBACKS]:
        try:
            found = read_set(client, bucket, key, databases, target, clock, deadline)
        except Refused as refusal:
            reasons.append(str(refusal))
            continue
        except Exception as error:
            sys.stderr.write(f"pg_restore_fetch failed: {type(error).__name__} while reading {key.rsplit('/', 1)[-1]}\n")
            return 1
        staging = target / f"{found}.partial"
        for path in staging.glob("*.dump"):
            path.replace(target / path.name)
        staging.rmdir()
        (target / "RUN").write_text(found + "\n", encoding="utf-8")
        sys.stdout.write(f"pg_restore_fetch: completed backup {found}; skipped {len(reasons)} newer set(s)\n")
        return 0
    sys.stderr.write("pg_restore_fetch failed: no completed backup set is usable: " + "; ".join(reasons) + "\n")
    return 1


def main() -> int:
    import boto3
    from botocore.config import Config

    bucket = os.environ.get("DATA_BUCKET_NAME", "")
    names = [item.split(":")[0] for item in os.environ.get("PG_DATABASES", "").split()]
    if not BUCKET.fullmatch(bucket) or not names or not all(DATABASE.fullmatch(name) for name in names):
        sys.stderr.write("pg_restore_fetch refused: DATA_BUCKET_NAME or PG_DATABASES is not set or has another form\n")
        return 1
    target = Path(os.environ.get("RESTORE_DIR", "/restore"))
    client = boto3.client("s3", config=Config(connect_timeout=10, read_timeout=120, retries={"max_attempts": 3, "mode": "standard"}))
    return run(bucket, names, target, client)


if __name__ == "__main__":
    sys.exit(main())
