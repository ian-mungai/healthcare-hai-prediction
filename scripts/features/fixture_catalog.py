"""Register the committed features publication of a fixture run in the SQL catalog that the Feast reader opens (code review 1, finding 2).

A fixture publish writes Iceberg tables with the Hadoop catalog into the local warehouse: ``<warehouse>/<namespace>/<table>/metadata/v<N>.metadata.json`` and
``version-hint.text``. The Feast reader uses a PyIceberg SQL catalog (a SQLite file in the run folder) and finds nothing there until this step registers the
metadata. ``register_publication`` does it in this order, and every later read goes through the ordinary reader path (``publication_reads``):

1. Register ``publish_runs`` at its current metadata (``current_version``: the hint, advanced past while a higher version file exists, or the highest file when
   the hint is missing) and read the head row R from the catalog.
2. For each table R names, find the metadata file of the warehouse whose ``main`` reference is exactly R's snapshot (the highest version that has it, with
   the snapshot present in the file) and register that file. A table whose warehouse holds no such file is refused by name: it is not the committed set.
3. A table already registered at the same file is left alone; one registered at another file (an earlier publication of the same run folder) is dropped from the
   catalog (the files stay) and registered again.

The step reads the warehouse and writes only the SQL catalog file in the run folder. It prints and raises names, counts and snapshot IDs, never row values.
PyIceberg is not imported here; the catalog object is passed in.
"""

from __future__ import annotations

import json
import re
import urllib.parse
from pathlib import Path
from typing import Any, Protocol

from scripts.features import publication_reads as reads
from scripts.features.publication_reads import Publication, ReadError

VERSION_FILE = re.compile(r"v([0-9]+)\.metadata\.json")


class RegisteringCatalog(Protocol):
    """The part of a PyIceberg catalog that registration uses."""

    def load_table(self, identifier: str) -> Any: ...

    def table_exists(self, identifier: str) -> bool: ...

    def register_table(self, identifier: str, metadata_location: str) -> Any: ...

    def drop_table(self, identifier: str) -> None: ...

    def create_namespace_if_not_exists(self, namespace: str) -> None: ...


def warehouse_folder(uri: str) -> Path:
    """The local folder of a ``file:`` warehouse URI; any other scheme is refused."""
    parts = urllib.parse.urlsplit(uri)
    if parts.scheme != "file" or not parts.path.startswith("/"):
        raise ReadError("the fixture warehouse is not a file: URI")
    return Path(parts.path)


def table_folder(warehouse: Path, namespace: str, table: str) -> Path:
    """The Hadoop-catalog folder of a table; the names are plain identifiers, so nothing can climb out of the warehouse."""
    if not reads.IDENT.fullmatch(namespace) or not reads.IDENT.fullmatch(table):
        raise ReadError("a namespace or table name is not a plain identifier")
    return warehouse / namespace / table / "metadata"


def versions(metadata: Path) -> list[tuple[int, Path]]:
    """The ``v<N>.metadata.json`` files of one table, oldest first."""
    found = [(int(match.group(1)), path) for path in metadata.glob("v*.metadata.json") if (match := VERSION_FILE.fullmatch(path.name))]
    return sorted(found)


def main_snapshot(path: Path) -> int | None:
    """The snapshot ID of ``main`` in one metadata file, or None when it has no ``main`` or the snapshot is not listed in the file."""
    try:
        document = json.loads(path.read_text())
        main = int(document["refs"]["main"]["snapshot-id"])
        listed = {int(item["snapshot-id"]) for item in document.get("snapshots", [])}
    except (OSError, ValueError, KeyError, TypeError):
        return None
    return main if main in listed else None


def current_version(folder: Path) -> int:
    """The current version of a Hadoop table, found the way Iceberg's ``HadoopTableOperations`` finds it.

    A commit renames the new metadata file first and writes ``version-hint.text`` afterwards, best effort, so the hint can be stale or missing.
    Start from the hint and advance while the next ``v<N>.metadata.json`` exists; when the hint is missing, unreadable or names a missing file, take the highest
    version found.
    """
    found = {number for number, _ in versions(folder)}
    try:
        version = int((folder / "version-hint.text").read_text().strip())
    except (OSError, ValueError):
        version = 0
    if version not in found:
        version = max(found, default=0)
    if version == 0:
        raise ReadError(f"{folder.parent.parent.name}.{folder.parent.name} has no metadata file in the local warehouse")
    while version + 1 in found:
        version += 1
    return version


def current_metadata(warehouse: Path, namespace: str, table: str) -> Path:
    """The table's current metadata file (``current_version``): what the Hadoop catalog treats as its current state."""
    folder = table_folder(warehouse, namespace, table)
    return folder / f"v{current_version(folder)}.metadata.json"


def metadata_for(warehouse: Path, namespace: str, table: str, snapshot_id: int) -> Path:
    """The highest-version metadata file of the table whose ``main`` is ``snapshot_id``; refused by table name when none exists."""
    for _, path in reversed(versions(table_folder(warehouse, namespace, table))):
        if main_snapshot(path) == snapshot_id:
            return path
    raise ReadError(f"{namespace}.{table}: no metadata file of the local warehouse has main at snapshot {snapshot_id}")


def register(catalog: RegisteringCatalog, namespace: str, table: str, path: Path) -> bool:
    """Make the catalog's entry for the table point at ``path``; True when it changed. The files of a replaced entry are not touched."""
    location = path.resolve().as_uri()
    identifier = f"{namespace}.{table}"
    catalog.create_namespace_if_not_exists(namespace)
    if catalog.table_exists(identifier):
        if str(catalog.load_table(identifier).metadata_location) == location:
            return False
        catalog.drop_table(identifier)
    catalog.register_table(identifier, location)
    return True


def register_publication(catalog: RegisteringCatalog, warehouse: Path, namespace: str, run_id: str | None = None) -> Publication:
    """Register ``publish_runs`` and then every table of the selected publication (the head row, or ``run_id``); returns the publication."""
    register(catalog, namespace, reads.RUNS_TABLE, current_metadata(warehouse, namespace, reads.RUNS_TABLE))
    publication = reads.read_publication(catalog, namespace, run_id)
    for table, snapshot_id in sorted(publication.snapshot_set.items()):
        register(catalog, namespace, table, metadata_for(warehouse, namespace, table, snapshot_id))
    return publication
