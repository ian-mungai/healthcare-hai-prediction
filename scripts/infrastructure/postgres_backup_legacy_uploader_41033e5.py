# Frozen copy of infra/services/scripts/pg_backup_upload.py at commit 41033e5, the uploader before markers recorded their members.
# The PostgreSQL backup E2E builds legacy markers with it, so the test needs no Git history. Never edit it: change the live uploader instead.
"""Upload step of the Postgres backup service: send each finished dump set to S3 once, then write the completion marker last.

It is the command of the ``postgres_backup_upload`` container (training image, ``python -I -c``). The dump step (``pg_backup_dump.sh``) leaves
``<run>.ready`` folders in BACKUP_DIR, each with one ``<database>.dump`` and a ``manifest.tsv`` of ``database<TAB>tables<TAB>bytes``. This step checks
every dump again (the custom-format header, the size in the manifest and at least one table), uploads it to ``pg_backups/<database>/<run>.dump`` with
``IfNoneMatch="*"`` so a key is created once and never replaced, and only then writes ``pg_backups/complete/<run>.json`` the same way. A restore reads
only sets that have a marker, so a set that stopped half way is never used. The role may not read, so a key that already exists is accepted only when
the set's own ``uploaded.tsv`` records that an earlier try wrote it with the same hash, which makes a rerun after an interruption safe.
A set that fails three times is set aside as ``<run>.failed``. A dump that is empty or truncated is set aside at once as ``<run>.refused`` and
nothing of it is uploaded. Every set has a deadline and every S3 call has timeouts.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PREFIX = "pg_backups"
RUN = re.compile(r"[0-9]{8}T[0-9]{6}Z")
DATABASE = re.compile(r"[a-z][a-z0-9_]{0,62}")
BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
MAGIC = b"PGDMP"
MAX_BYTES = 4 * 1024**3
RUN_SECONDS = 1800
POLL_SECONDS = 60
ATTEMPTS = 3
EXISTS = ("PreconditionFailed", "412")


class Refused(Exception):
    """The set can never be uploaded: a dump is empty, truncated or not a dump."""


class Failed(Exception):
    """The upload failed and a later try may succeed."""


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1 << 20):
            digest.update(block)
    return digest.hexdigest()


def read_manifest(folder: Path) -> dict[str, tuple[int, int]]:
    """The tables and bytes of each database of a set, from ``manifest.tsv``."""
    try:
        lines = (folder / "manifest.tsv").read_text(encoding="utf-8").splitlines()
        found = {}
        for line in lines:
            name, tables, size = line.split("\t")
            if not DATABASE.fullmatch(name):
                raise ValueError(name)
            found[name] = (int(tables), int(size))
    except (OSError, ValueError):
        raise Refused("the manifest is missing or unreadable") from None
    if not found:
        raise Refused("the manifest lists no database")
    return found


def check_dump(folder: Path, name: str, tables: int, size: int) -> None:
    """Refuse a dump that is empty, truncated or not a custom-format dump."""
    path = folder / f"{name}.dump"
    try:
        actual = path.stat().st_size
        with path.open("rb") as source:
            header = source.read(len(MAGIC))
    except OSError:
        raise Refused(f"database {name}: the dump file is missing") from None
    if header != MAGIC:
        raise Refused(f"database {name}: the file is not a dump")
    if tables < 1:
        raise Refused(f"database {name}: the dump is empty (no table)")
    if actual != size or actual < len(MAGIC) + 2:
        raise Refused(f"database {name}: the dump is truncated ({actual} bytes, manifest {size})")
    if actual > MAX_BYTES:
        raise Refused(f"database {name}: the dump is larger than {MAX_BYTES} bytes")


def put_once(client: Any, bucket: str, key: str, path: Path, digest: str, record: Path) -> None:
    """Create ``key`` from ``path`` once. The role may write and may not read, so a key that exists is accepted only when this folder's own record
    (``uploaded.tsv``, written after a confirmed put) says an earlier try of this same set wrote the same hash; otherwise the set is refused."""
    from botocore.exceptions import BotoCoreError, ClientError

    line = f"{key}\t{digest}"
    done = record.read_text(encoding="utf-8").splitlines() if record.exists() else []
    try:
        with path.open("rb") as body:
            client.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*", ChecksumAlgorithm="SHA256", Metadata={"sha256": digest})
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "error")
        if code not in EXISTS:
            raise Failed(f"upload of {key} failed ({code})") from None
        if line not in done:
            raise Refused(f"the key {key} already exists and this set did not write it") from None
        return
    except (OSError, BotoCoreError) as error:
        raise Failed(f"upload of {key} failed ({type(error).__name__})") from None
    if line not in done:
        with record.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def upload_set(folder: Path, bucket: str, client: Any, clock: Callable[[], float]) -> str:
    """Check and upload one ready set, then write its marker; returns the run ID."""
    run = folder.name.removesuffix(".ready")
    deadline = clock() + RUN_SECONDS
    manifest = read_manifest(folder)
    for name, (tables, size) in manifest.items():
        check_dump(folder, name, tables, size)
    entries = {}
    for name, (tables, size) in manifest.items():
        if clock() > deadline:
            raise Failed(f"the deadline of {RUN_SECONDS} seconds passed before database {name}")
        key = f"{PREFIX}/{name}/{run}.dump"
        digest = sha256_of(folder / f"{name}.dump")
        put_once(client, bucket, key, folder / f"{name}.dump", digest, folder / "uploaded.tsv")
        entries[name] = {"key": key, "bytes": size, "sha256": digest, "tables": tables}
    if clock() > deadline:
        raise Failed(f"the deadline of {RUN_SECONDS} seconds passed before the marker")
    marker = folder / "marker.json"
    marker.write_text(
        json.dumps({"schema": 1, "run": run, "uploaded_utc": datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"), "databases": entries}, sort_keys=True),
        encoding="utf-8",
    )
    put_once(client, bucket, f"{PREFIX}/complete/{run}.json", marker, sha256_of(marker), folder / "uploaded.tsv")
    return run


def note(backups: Path, result: str, run: str, reason: str = "") -> None:
    text = json.dumps({"result": result, "run": run, "reason": reason, "at_utc": datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")}, sort_keys=True)
    (backups / "upload_result.json").write_text(text + "\n", encoding="utf-8")
    sys.stdout.write(f"pg_backup_upload: {result} {run} {reason}".rstrip() + "\n")


def tried(folder: Path) -> int:
    path = folder / "attempts"
    count = int(path.read_text(encoding="utf-8").strip() or 0) + 1 if path.exists() else 1
    path.write_text(f"{count}\n", encoding="utf-8")
    return count


def run(backups: Path, bucket: str, client: Any, clock: Callable[[], float] = time.monotonic) -> int:
    """Upload every ready set, oldest first, once; returns 1 when any set failed or was refused."""
    status = 0
    for folder in sorted(item for item in backups.glob("*.ready") if item.is_dir() and RUN.fullmatch(item.name.removesuffix(".ready"))):
        name = folder.name.removesuffix(".ready")
        try:
            note(backups, "uploaded", upload_set(folder, bucket, client, clock))
            shutil.rmtree(folder)
        except Refused as refusal:
            status = 1
            folder.rename(backups / f"{name}.refused")
            note(backups, "refused", name, str(refusal))
        except Failed as failure:
            status = 1
            if tried(folder) >= ATTEMPTS:
                folder.rename(backups / f"{name}.failed")
            note(backups, "failed", name, str(failure))
    return status


def main() -> int:
    """Upload ready sets in a loop, or once when PG_BACKUP_ONCE is 1."""
    import boto3
    from botocore.config import Config

    bucket = os.environ.get("DATA_BUCKET_NAME", "")
    if not BUCKET.fullmatch(bucket):
        sys.stderr.write("pg_backup_upload refused: DATA_BUCKET_NAME is not set or has another form\n")
        return 1
    backups = Path(os.environ.get("BACKUP_DIR", "/backup"))
    client = boto3.client("s3", config=Config(connect_timeout=10, read_timeout=120, retries={"max_attempts": 3, "mode": "standard"}))
    while True:
        code = run(backups, bucket, client)
        if os.environ.get("PG_BACKUP_ONCE") == "1":
            return code
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    sys.exit(main())
