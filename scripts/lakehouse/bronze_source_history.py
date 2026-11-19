"""The version history of a bronze table's own source collection, as evidence that the table has, or never had, a source object.

A step that leaves a declared bronze table out (the Glue mirror ``register``, the AWS catalog registration and the load's own mirror hook) never rests
on a run record alone: it lists the table's source collection with its own credentials and leaves the table out only when ``source_evidence`` finds no
source object and no observation history. This module holds that evidence and nothing that reads run records, so the Spark image can import it:
``bronze_unsourced`` reads run records through ``orchestration.raw_lineage``, whose import chain the image does not install, and re-exports these
names."""

from __future__ import annotations

import re
from collections.abc import Collection
from typing import Any

from scripts.lakehouse import bronze

HISTORY_REASON = "the source collection holds a source object or observation history"


class UnsourcedError(ValueError):
    """A bronze run record cannot be read as evidence, so no table may be left out on its word."""


READ_ROOTS = ("", "raw/")
MANIFEST = "/manifest.json"


class HistoryStorage:
    """The loader's ``Storage`` protocol over the version history of a bucket, for the omission evidence only.

    The loader lists current versions, so a manifest whose current version is a delete marker vanishes from its discovery while every earlier version stays
    readable by its exact ID. This adapter lists every readable version of every manifest key (``list_keys``) and the newest readable version of every key
    (``list_current``), so discovery reads such a manifest and counts what it names. Reads and downloads go to the underlying reader unchanged.
    """

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.listings: dict[tuple[str, str], list[tuple[str, str, bool]]] = {}

    def history(self, bucket: str, prefix: str) -> list[tuple[str, str, bool]]:
        if (bucket, prefix) not in self.listings:
            self.listings[(bucket, prefix)] = list(self.inner.list_history(bucket, prefix))
        return self.listings[(bucket, prefix)]

    def list_keys(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        return sorted((key, version) for key, version, marker in self.history(bucket, prefix) if not marker and key.endswith(MANIFEST))

    def list_current(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        newest: dict[str, str] = {}
        for key, version, marker in self.history(bucket, prefix):
            if not marker:
                newest.setdefault(key, version)
        return sorted(newest.items())

    def get(self, bucket: str, key: str, version_id: str) -> bytes:
        return bytes(self.inner.get(bucket, key, version_id))

    def download(self, bucket: str, key: str, version_id: str, path: Any) -> None:
        self.inner.download(bucket, key, version_id, path)


def payload_matches(table: dict[str, Any], key: str) -> bool:
    """Whether a stored key could be a source file of the table by its own selector: a dataset folder for a dataset-ID table, the file name for a member
    pattern. The snapshot pattern is ignored, so this over-counts rather than misses."""
    if "dataset_ids" in table:
        return any(f"/datasets/{dataset}/" in key for dataset in table["dataset_ids"])
    return re.search(table["member_pattern"], key.rsplit("/", 1)[-1]) is not None


def source_evidence(storage: Any, bucket: str, config: dict[str, Any], tables: Collection[str]) -> frozenset[str]:
    """The tables, of ``tables``, that have a source object or observation history in the table map's own collection.

    The reader must list the bucket's version history (``list_history``, as ``bronze.S3Storage`` does): the omission may rest on nothing but versions, so a
    reader that lists current objects only cannot prove absence and raises. Two reads of the history count as evidence:

    - the loader's own discovery (collection, manifests, member pattern or dataset IDs, retirement) over both read roots and over every readable manifest
      version, so a loadable object, a rejected capture, a retired object, every matching superseded copy and a manifest whose current version is a delete
      marker all count (superseded copies whether or not an earlier generation published them: ``count_superseded``);
    - every stored key of the collection, any version and any delete marker, that the table's own selector matches, so a payload with a delete marker and a
      payload that never had a manifest count too.

    Only a collection with no version of any matching manifest or object supports absence. ``config`` must be a table map as ``bronze.load_table_map`` returns
    it (it adds the roles and encodings that discovery reads): a map without the named table or its defaults raises ``UnsourcedError``, so a malformed map
    cannot pass as a refusal. A listing or read that fails raises: nothing is guessed.
    """
    names = list(tables)
    if not names:
        return frozenset()
    by_name = {entry.get("table"): entry for entry in config.get("tables", []) if isinstance(entry, dict)}
    for name in names:
        entry = by_name.get(name)
        if (
            entry is None
            or not isinstance(entry.get("roles"), list)
            or not isinstance(entry.get("encodings"), list)
            or not isinstance(entry.get("collection"), str)
            or ("dataset_ids" in entry) == ("member_pattern" in entry)
        ):
            raise UnsourcedError(
                f"{name}: the table map is malformed for discovery (it lacks the table, its collection or selector, or the loader's roles and encodings)"
            )
    if not callable(getattr(storage, "list_history", None)):
        raise TypeError("the source reader does not list version history, so absence cannot be proved")
    reader = HistoryStorage(storage)
    found, excluded = bronze.discover(reader, bucket, config, names, bronze.RetiredObjects(), roots=READ_ROOTS, count_superseded=True)
    evidence = {item["table"] for item in found} | {entry["copy"]["table"] for entry in excluded if "copy" in entry}
    for name in names:
        if name in evidence:
            continue
        for root in READ_ROOTS:
            if any(
                MANIFEST not in key and payload_matches(by_name[name], key) for key, _, _ in reader.history(bucket, f"{root}{by_name[name]['collection']}/")
            ):
                evidence.add(name)
    return frozenset(evidence)
