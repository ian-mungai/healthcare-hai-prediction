"""Re-register the published tables in the AWS Polaris catalog after a restore: the logic of the ``restore`` step of ``aws_catalog_setup``.

``host_session start`` launches the step after a start that restored a database, and ``host_session catalog-setup restore [--dry-run]`` runs it by hand. It runs
as the catalog setup one-shot on the host, under the task role, against Polaris on the host's loopback, so it uses no tunnel and no project profile.

A publish writes ``catalog_backup_<run>.json`` BEFORE it commits (the tables' metadata files then hold the candidate on a branch, ``main`` is still the base
and ``publish_runs`` has no row of the run), so that backup is never used here: registering it would present the state before the last commit as recovered.
The source is the post-commit recovery snapshot ``catalog_committed_<run>.json`` that a commit writes under the same folders (``lakehouse/catalog_backups/``,
``lakehouse/silver/catalog_backups/`` and ``lakehouse/features/catalog_backups/``) after ``main`` of every table is at the committed snapshot and the run's
``publish_runs`` row exists. It holds, per table, the metadata file whose ``main`` is the committed snapshot, that snapshot ID and the snapshot set of the
run. A restored Polaris database holds the catalog as of its dump, so a table committed after the dump is absent or points at an older file. For the newest
snapshot of each folder, through the root credential of the setup task, per table:

- an absent table is registered; one at that file is ``unchanged``;
- a table whose catalog file has a lower sequence number (``00012-<uuid>.metadata.json``) is advanced: drop without purge, then register, and the older
  entry is registered again when the new one is refused or does not resolve;
- a table whose catalog file is newer, or has the same number and another name, is left and reported (``catalog ahead``, ``conflict``).

After each registration the table is loaded again and its ``main`` snapshot must be the committed snapshot of the set; a table that does not resolve to it is
put back and the run fails. Folders with no snapshot contribute nothing. A namespace that the catalog lacks stops the run (``aws_catalog_setup principal``
creates namespaces). Nothing is written under ``--dry-run``. Every request runs once within the run's deadline. The reply names tables, outcomes and
sequence numbers and never a credential. This module imports nothing from ``aws_catalog_setup``, which is run as ``__main__`` and
would otherwise load a second copy of itself.
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
# The run ID form of orchestration/training_contract.py (RUN_ID). The setup task mounts scripts only, so this module holds a copy that
# tests/test_run_id_patterns.py keeps equal.
RUN_ID_PATTERN = (
    r"(?:(?:ingestion|promotion|features|metadata|research|serving|bronze|silver|gold)_[a-f0-9]{8}(?:_(?:[2-9]|[1-9][0-9]))?"
    r"|(?:features|research|training|reload|drift)_(?:gradient_boosting|fixed_effects|mixed_effects|elastic_net|spatial|ols)_[a-f0-9]{8}(?:_(?:[2-9]|[1-9][0-9]))?"
    r"|(?:canary_deploy|stable_deploy|promote|release|canary|notice)_(?:ssi_hysterectomy|ssi_colon|clabsi|cauti|cdiff|mrsa)_[a-f0-9]{8}(?:_(?:[2-9]|[1-9][0-9]))?"
    r"|ingestion_retry_[a-f0-9]{8}(?:_(?:[2-9]|[1-9][0-9]))?_[2-3])"
)
BACKUP_NAME = re.compile(rf"catalog_committed_({RUN_ID_PATTERN})\.json")
IDENTIFIER = re.compile(r"[a-z][a-z0-9_]{0,62}")
SEQUENCE = re.compile(r"/([0-9]{5,})-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.metadata\.json$")
SCHEMA = 2
MAX_BACKUP_BYTES = 8 * 1024 * 1024

Request = Callable[..., tuple[int, Any]]


class RestoreError(RuntimeError):
    """A restore failed; ``code`` names the part and the message never holds a credential. ``aws_catalog_setup.SetupError`` is a subclass, so a request that
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
    main: int


class Store(Protocol):
    """The backup folders of the data bucket."""

    def names(self, prefix: str) -> list[str]:
        """The keys under the prefix in the order they were written, the newest last (a run ID holds no time)."""
        ...

    def read(self, key: str) -> bytes: ...


def sequence_of(location: str) -> int | None:
    found = SEQUENCE.search(location)
    return int(found.group(1)) if found else None


def read_items(store: Store, bucket: str, *, deadline: float | None = None, clock: Callable[[], float] = time.monotonic) -> list[Item]:
    """The tables of the newest committed snapshot of each folder. A malformed snapshot stops the run; a folder with no snapshot contributes nothing.
    With a ``deadline`` the time is checked before each folder and after the last one, so a run that used its whole budget on reading the folders never
    answers success, empty folders included."""
    items: list[Item] = []
    seen: set[tuple[str, str]] = set()
    for prefix in BACKUP_PREFIXES:
        if deadline is not None:
            check_deadline(deadline, clock)
        runs = [(found.group(1), name) for name in store.names(prefix) if (found := BACKUP_NAME.fullmatch(name.rsplit("/", 1)[-1])) is not None]
        if not runs:
            continue
        run, key = runs[-1]
        try:
            document = json.loads(store.read(key))
        except ValueError:
            raise RestoreError("backup", f"the committed snapshot {run} of {prefix} is not JSON") from None
        namespace, tables, wanted = document.get("namespace"), document.get("tables"), document.get("snapshot_set")
        if (
            document.get("schema") != SCHEMA
            or document.get("kind") != "committed"
            or document.get("run_id") != run
            or not isinstance(namespace, str)
            or not IDENTIFIER.fullmatch(namespace)
            or not isinstance(tables, dict)
            or not isinstance(wanted, dict)
        ):
            raise RestoreError("backup", f"the committed snapshot {run} of {prefix} has an unexpected form")
        for table, entry in sorted(tables.items()):
            location = entry.get("metadata_file") if isinstance(entry, dict) else None
            if location is None:
                continue
            number, main = sequence_of(str(location)), entry.get("main_snapshot_id")
            if not IDENTIFIER.fullmatch(table) or number is None or type(main) is not int or not str(location).startswith(f"s3://{bucket}/lakehouse/"):
                raise RestoreError("backup", f"the committed snapshot {run} of {prefix} names table {table} with an unexpected metadata file or snapshot")
            if table in wanted and wanted[table] != main:
                raise RestoreError("backup", f"the committed snapshot {run} of {prefix} names two main snapshots for table {table}")
            if (namespace, table) in seen:
                raise RestoreError("backup", f"table {namespace}.{table} is in two snapshot folders")
            seen.add((namespace, table))
            items.append(Item(namespace, table, str(location), number, run, main))
    if deadline is not None:
        check_deadline(deadline, clock)
    return items


def table_path(namespace: str, table: str = "") -> str:
    return f"/api/catalog/v1/{CATALOG}/namespaces/{namespace}" + (f"/tables/{table}" if table else "")


def main_of(found: Any) -> int | None:
    """The ``main`` snapshot ID of a loaded table, from the Iceberg REST ``metadata`` (``refs.main``, else ``current-snapshot-id``)."""
    metadata = found.get("metadata") if isinstance(found, dict) else None
    if not isinstance(metadata, dict):
        return None
    refs = metadata.get("refs")
    main = refs.get("main") if isinstance(refs, dict) else None
    value = main.get("snapshot-id") if isinstance(main, dict) else metadata.get("current-snapshot-id")
    return value if type(value) is int else None


def verify_main(request: Request, access: str, item: Item) -> None:
    """Load the table again and require its main to be the committed snapshot of the set."""
    status, found = request("GET", table_path(item.namespace, item.table), access)
    seen = main_of(found) if status == 200 else None
    if seen != item.main:
        raise RestoreError("catalog", f"{item.namespace}.{item.table} resolves to main snapshot {seen}, the committed set names {item.main}")


def restore_table(request: Request, access: str, item: Item, *, apply: bool) -> str:
    """Bring one table to the committed metadata file; returns the outcome word."""
    status, found = request("GET", table_path(item.namespace, item.table), access)
    if status == 404:
        if not apply:
            return "would register"
        register(request, access, item)
        try:
            verify_main(request, access, item)
        except RestoreError:
            request("DELETE", table_path(item.namespace, item.table), access)
            raise
        return "registered"
    if status != 200 or not isinstance(found, dict):
        raise RestoreError("catalog", f"reading {item.namespace}.{item.table} failed (HTTP {status})")
    current = str(found.get("metadata-location", ""))
    if current == item.location:
        if main_of(found) != item.main:
            raise RestoreError(
                "catalog", f"{item.namespace}.{item.table} is at the committed file but resolves to main snapshot {main_of(found)}, not {item.main}"
            )
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
        verify_main(request, access, item)
    except RestoreError as error:
        request("DELETE", table_path(item.namespace, item.table), access)
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
    """Read the newest committed snapshot of each folder and bring the catalog to it: the reply of the ``restore`` step (outcomes and counts)."""
    items = read_items(store, bucket, deadline=deadline, clock=clock)
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
        """The keys under the prefix ordered by the time S3 stored them, the newest last. A snapshot is created once and holds no time itself."""
        found: list[tuple[Any, str]] = []
        token = None
        while True:
            page = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, **({"ContinuationToken": token} if token else {}))
            found += [(item["LastModified"], item["Key"]) for item in page.get("Contents", [])]
            if not page.get("IsTruncated"):
                return [key for _, key in sorted(found)]
            token = page.get("NextContinuationToken")

    def read(self, key: str) -> bytes:
        body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read(MAX_BACKUP_BYTES + 1)
        if len(body) > MAX_BACKUP_BYTES:
            raise RestoreError("backup", "a catalog backup is larger than the limit")
        return bytes(body)
