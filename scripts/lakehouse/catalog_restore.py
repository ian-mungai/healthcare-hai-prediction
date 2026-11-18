"""Re-register the published tables in the AWS Polaris catalog after a restore: the logic of the ``restore`` step of ``aws_catalog_setup``.

``host_session start`` launches the step after a start that restored a database, and ``host_session catalog-setup restore [--dry-run]`` runs it by hand. It runs
as the catalog setup one-shot on the host, under the task role, against Polaris on the host's loopback, so it uses no tunnel and no project profile.

Each publish writes ``catalog_backup_<run>.json`` under ``lakehouse/catalog_backups/`` (gold), ``lakehouse/silver/catalog_backups/`` and
``lakehouse/features/catalog_backups/``. It holds, per table, the newest Iceberg metadata file. A restored Polaris database holds the catalog as of its dump, so
a table published after the dump is absent or points at an older metadata file. This module reads the newest backup of each folder and, per table, through the
root credential of the setup task:

- registers the table when the catalog has none (``unchanged`` is the answer when it already points at that file);
- advances a table whose catalog file is older: the metadata file names carry a sequence number (``00012-<uuid>.metadata.json``), and a backup with a higher
  number than the catalog's replaces the catalog entry (drop without purge, then register), registering the old file again when the new one is refused;
- leaves a table whose catalog file is newer or has the same number but another name, and reports it (``catalog ahead``, ``conflict``).

A namespace that the catalog lacks stops the run: namespaces are created only by ``aws_catalog_setup principal``. Nothing is written under ``--dry-run``.
Every request runs once within the run's deadline. The reply names tables, outcomes and sequence numbers and never a credential. This module imports nothing
from ``aws_catalog_setup``, which is run as ``__main__`` and would otherwise load a second copy of itself.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

CATALOG = "hai_lakehouse"

BACKUP_PREFIXES = ("lakehouse/catalog_backups/", "lakehouse/silver/catalog_backups/", "lakehouse/features/catalog_backups/")
BACKUP_NAME = re.compile(r"catalog_backup_([0-9]{8}T[0-9]{6}Z-[a-f0-9]{8})\.json")
IDENTIFIER = re.compile(r"[a-z][a-z0-9_]{0,62}")
SEQUENCE = re.compile(r"/([0-9]{5,})-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.metadata\.json$")
SCHEMA = 1
MAX_BACKUP_BYTES = 8 * 1024 * 1024

Request = Callable[..., tuple[int, Any]]


class RestoreError(RuntimeError):
    """A restore failed; ``code`` names the part and the message never holds a credential. ``aws_catalog_setup.RestoreError`` is a subclass, so a request that
    passes its deadline inside a restore is handled like any other failure of it."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def check_deadline(deadline: float, clock: Callable[[], float] = time.monotonic) -> float:
    """The seconds left before the absolute deadline; none left stops the run."""
    left = deadline - clock()
    if left <= 0:
        raise RestoreError("deadline", "the catalog restore passed its deadline")
    return left


@dataclass(frozen=True)
class Item:
    """One table of the newest backup of a folder."""

    namespace: str
    table: str
    location: str
    sequence: int
    run: str


class Store(Protocol):
    """The backup folders of the data bucket."""

    def names(self, prefix: str) -> list[str]: ...

    def read(self, key: str) -> bytes: ...


def sequence_of(location: str) -> int | None:
    found = SEQUENCE.search(location)
    return int(found.group(1)) if found else None


def read_items(store: Store, bucket: str) -> list[Item]:
    """The tables of the newest backup of each folder. A malformed backup stops the run; a folder with no backup contributes nothing."""
    items: list[Item] = []
    seen: set[tuple[str, str]] = set()
    for prefix in BACKUP_PREFIXES:
        runs = sorted((found.group(1), name) for name in store.names(prefix) if (found := BACKUP_NAME.fullmatch(name.rsplit("/", 1)[-1])) is not None)
        if not runs:
            continue
        run, key = runs[-1][0], runs[-1][1]
        try:
            document = json.loads(store.read(key))
        except ValueError:
            raise RestoreError("backup", f"the backup {run} of {prefix} is not JSON") from None
        namespace, tables = document.get("namespace"), document.get("tables")
        if (
            document.get("schema") != SCHEMA
            or document.get("run_id") != run
            or not isinstance(namespace, str)
            or not IDENTIFIER.fullmatch(namespace)
            or not isinstance(tables, dict)
        ):
            raise RestoreError("backup", f"the backup {run} of {prefix} has an unexpected form")
        for table, entry in sorted(tables.items()):
            location = entry.get("metadata_file") if isinstance(entry, dict) else None
            if location is None:
                continue
            number = sequence_of(str(location))
            if not IDENTIFIER.fullmatch(table) or number is None or not str(location).startswith(f"s3://{bucket}/lakehouse/"):
                raise RestoreError("backup", f"the backup {run} of {prefix} names table {table} with an unexpected metadata file")
            if (namespace, table) in seen:
                raise RestoreError("backup", f"table {namespace}.{table} is in two backup folders")
            seen.add((namespace, table))
            items.append(Item(namespace, table, str(location), number, run))
    return items


def table_path(namespace: str, table: str = "") -> str:
    return f"/api/catalog/v1/{CATALOG}/namespaces/{namespace}" + (f"/tables/{table}" if table else "")


def restore_table(request: Request, access: str, item: Item, *, apply: bool) -> str:
    """Bring one table to the backup's metadata file; returns the outcome word."""
    status, found = request("GET", table_path(item.namespace, item.table), access)
    if status == 404:
        if apply:
            register(request, access, item)
        return "registered" if apply else "would register"
    if status != 200 or not isinstance(found, dict):
        raise RestoreError("catalog", f"reading {item.namespace}.{item.table} failed (HTTP {status})")
    current = str(found.get("metadata-location", ""))
    if current == item.location:
        return "unchanged"
    held = sequence_of(current)
    if held is None:
        raise RestoreError("catalog", f"{item.namespace}.{item.table} has a metadata file of an unexpected form")
    if held > item.sequence:
        return f"catalog ahead ({held} after {item.sequence})"
    if held == item.sequence:
        return f"conflict (sequence {held} names another file)"
    if not apply:
        return f"would advance ({held} to {item.sequence})"
    status, _ = request("DELETE", table_path(item.namespace, item.table), access)
    if status not in (200, 204):
        raise RestoreError("catalog", f"dropping the older entry of {item.namespace}.{item.table} failed (HTTP {status})")
    try:
        register(request, access, item)
    except RestoreError as error:
        status, _ = request("POST", f"{table_path(item.namespace)}/register", access, body={"name": item.table, "metadata-location": current})
        again = "the older entry was registered again" if status in (200, 201) else "THE OLDER ENTRY COULD NOT BE REGISTERED AGAIN: register it by hand"
        raise RestoreError("catalog", f"{error}; {again}") from None
    return f"advanced ({held} to {item.sequence})"


def register(request: Request, access: str, item: Item) -> None:
    status, _ = request("POST", f"{table_path(item.namespace)}/register", access, body={"name": item.table, "metadata-location": item.location})
    if status not in (200, 201):
        raise RestoreError("catalog", f"registering {item.namespace}.{item.table} failed (HTTP {status})")


def restore_items(
    request: Request, access: str, items: list[Item], *, apply: bool, deadline: float, clock: Callable[[], float] = time.monotonic
) -> list[dict[str, str]]:
    """Check each namespace, then restore each table in order; the first failure stops the run (the outcomes so far stay true)."""
    for namespace in sorted({item.namespace for item in items}):
        status, found = request("GET", table_path(namespace), access)
        if status != 200 or not isinstance(found, dict) or found.get("namespace") != [namespace]:
            raise RestoreError("namespace", f"namespace {namespace} is not in the catalog (HTTP {status}); create it with aws_catalog_setup principal first")
    results = []
    for item in items:
        check_deadline(deadline, clock)
        results.append({"table": f"{item.namespace}.{item.table}", "backup": item.run, "outcome": restore_table(request, access, item, apply=apply)})
    return results


def restore_catalog(
    store: Store, bucket: str, request: Request, access: str, *, apply: bool, deadline: float, clock: Callable[[], float] = time.monotonic
) -> dict[str, Any]:
    """Read the newest backup of each folder and bring the catalog to it: the reply of the ``restore`` step (every table with its outcome, and the counts)."""
    items = read_items(store, bucket)
    results = restore_items(request, access, items, apply=apply, deadline=deadline, clock=clock)
    counts: dict[str, int] = {}
    for row in results:
        word = row["outcome"].split(" (")[0]
        counts[word] = counts.get(word, 0) + 1
    return {"step": "restore", "applied": apply, "tables": results, "counts": counts}


class LiveStore:
    """The backup folders of the data bucket through an S3 client of the task role."""

    def __init__(self, client: Any, bucket: str) -> None:
        self.client, self.bucket = client, bucket

    def names(self, prefix: str) -> list[str]:
        keys: list[str] = []
        token = None
        while True:
            page = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, **({"ContinuationToken": token} if token else {}))
            keys += [item["Key"] for item in page.get("Contents", [])]
            if not page.get("IsTruncated"):
                return keys
            token = page.get("NextContinuationToken")

    def read(self, key: str) -> bytes:
        body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read(MAX_BACKUP_BYTES + 1)
        if len(body) > MAX_BACKUP_BYTES:
            raise RestoreError("backup", "a catalog backup is larger than the limit")
        return bytes(body)
