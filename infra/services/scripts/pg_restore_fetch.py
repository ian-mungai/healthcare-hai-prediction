"""Fetch step of the Postgres restore: write the dumps of the newest completed backup set to RESTORE_DIR, checked against its marker.

It is the command of the ``postgres_restore_fetch`` container of the postgres task (training image, ``python -I -c``). It reads the markers under
``pg_backups/complete/`` and takes the newest set whose every requested dump exists with the size and SHA-256 its marker records. A set that fails the
check is skipped and the next older one is tried (at most FALLBACKS sets). A dump with no marker is never read, because the dump step writes the marker last.
When no marker exists it writes nothing and exits 0: there is nothing to restore. When markers exist and none passes, it exits 1 with the reason, and the
databases step then creates and starts nothing. It writes each dump as ``<database>.dump`` and the run ID as ``RUN``; only the databases named in
PG_DATABASES are read. The whole fetch has a deadline and every S3 call has timeouts.
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


def read_set(client: Any, bucket: str, key: str, databases: list[str], target: Path, clock: Callable[[], float], deadline: float) -> str:
    """Write the requested dumps of one set into a ``.partial`` folder and return its run ID; raises Refused when the set is not whole."""
    run = key.rsplit("/", 1)[-1].removesuffix(".json")
    try:
        marker = json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
        entries = marker["databases"]
    except (ValueError, KeyError, TypeError):
        raise Refused(f"the marker of {run} is not readable") from None
    staging = target / f"{run}.partial"
    staging.mkdir(parents=True, exist_ok=True)
    for name in databases:
        if name not in entries:
            continue
        if clock() > deadline:
            raise Refused(f"the deadline of {FETCH_SECONDS} seconds passed")
        entry = entries[name]
        if entry.get("key") != f"{PREFIX}/{name}/{run}.dump" or not 0 < int(entry.get("bytes", 0)) <= MAX_BYTES:
            raise Refused(f"the marker of {run} names a wrong key or size for {name}")
        try:
            data = client.get_object(Bucket=bucket, Key=entry["key"])["Body"].read()
        except Exception as error:
            if type(error).__name__ == "ClientError":
                raise Refused(f"the dump of {name} in {run} cannot be read") from None
            raise
        if len(data) != entry["bytes"] or hashlib.sha256(data).hexdigest() != entry["sha256"]:
            raise Refused(f"the dump of {name} in {run} differs from its marker")
        (staging / f"{name}.dump").write_bytes(data)
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
