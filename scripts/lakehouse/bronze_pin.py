"""Pin the source inventory of a bronze run, and replay exactly that inventory on every attempt.

The owner builds the inventory before the run (``bronze_run inventory``). It lists the manifest versions the run reads, the object versions they name and the
SHA-256 of each manifest. The request of the run names the inventory's digest. The pin phase checks the digest, and discovery, the dry run and the loader
read through ``PinnedStorage``, which answers only from the inventory. A capture that arrives later is not part of the run. A different capture needs a new run.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from scripts.lakehouse.bronze import BronzeError, RetiredObjects, Storage, discover

VERSION = 1
SHA256_HEX = 64


def inventory_bytes(inventory: dict[str, Any]) -> bytes:
    return (json.dumps(inventory, sort_keys=True, separators=(",", ":")) + "\n").encode()


def read_inventory(raw: bytes) -> dict[str, Any]:
    """Parse a staged inventory and refuse any other shape."""
    try:
        inventory = json.loads(raw)
    except ValueError:
        raise BronzeError("the source inventory is not JSON") from None
    shapes = {"manifests": 3, "objects": 5, "documents": 2}
    if not isinstance(inventory, dict) or inventory.get("version") != VERSION or set(inventory) != {"version", "tables", *shapes}:
        raise BronzeError("the source inventory needs exactly version, tables, manifests, objects and documents")
    if not isinstance(inventory["tables"], list) or not all(isinstance(name, str) for name in inventory["tables"]):
        raise BronzeError("the source inventory tables are not a list of names")
    for name, width in shapes.items():
        rows = inventory[name]
        if not isinstance(rows, list) or any(not isinstance(row, list) or len(row) != width for row in rows):
            raise BronzeError(f"the source inventory {name} have the wrong shape")
    if any(
        not isinstance(row[0], str) or not isinstance(row[1], str) or not row[1]
        for row in [*inventory["manifests"], *inventory["objects"], *inventory["documents"]]
    ):
        raise BronzeError("every inventory entry needs a key and an object version")
    return inventory


class Recorder:
    """Wraps a storage during discovery and keeps what it served."""

    def __init__(self, inner: Storage) -> None:
        self.inner = inner
        self.manifests: dict[tuple[str, str], bytes] = {}
        self.listed: list[tuple[str, str]] = []

    def list_keys(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        return self.inner.list_keys(bucket, prefix)

    def list_current(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        found = self.inner.list_current(bucket, prefix)
        self.listed.extend(found)
        return found

    def get(self, bucket: str, key: str, version_id: str) -> bytes:
        data = self.inner.get(bucket, key, version_id)
        self.manifests[(key, version_id)] = data
        return data

    def download(self, bucket: str, key: str, version_id: str, path: Path) -> None:
        self.inner.download(bucket, key, version_id, path)


def build_inventory(storage: Storage, bucket: str, config: dict[str, Any], names: list[str], retired: RetiredObjects | None = None) -> dict[str, Any]:
    """Discover the current sources of the tables once and list every manifest and object version the run may read."""
    recorder = Recorder(storage)
    inputs, _ = discover(recorder, bucket, config, names, retired or RetiredObjects())
    documents = []
    for raw in recorder.manifests.values():
        manifest = json.loads(raw)
        if "objects" not in manifest and "sha256" in manifest:
            documents += [[key, version] for key, version in recorder.listed if f"/{manifest['sha256']}/" in key and "/manifests/" not in key]
    objects = [[item["table"], item["key"], item["version_id"], item["sha256"], item["byte_count"]] for item in inputs]
    manifests = [[key, version, hashlib.sha256(raw).hexdigest()] for (key, version), raw in recorder.manifests.items()]
    return {"version": VERSION, "tables": sorted(names), "manifests": sorted(manifests), "objects": sorted(objects), "documents": sorted(documents)}


class PinnedStorage:
    """Answers listings and reads from the pinned inventory only."""

    def __init__(self, inner: Storage, inventory: dict[str, Any]) -> None:
        self.inner = inner
        self.manifests = {(row[0], row[1]): row[2] for row in inventory["manifests"]}
        self.objects = {(row[1], row[2]) for row in inventory["objects"]} | {(row[0], row[1]) for row in inventory["documents"]}

    def list_keys(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        return sorted(identity for identity in self.manifests if identity[0].startswith(prefix))

    def list_current(self, bucket: str, prefix: str) -> list[tuple[str, str]]:
        return sorted(identity for identity in self.objects if identity[0].startswith(prefix))

    def get(self, bucket: str, key: str, version_id: str) -> bytes:
        if (key, version_id) not in self.manifests:
            raise BronzeError(f"{key}: not a manifest version of the pinned inventory")
        data = self.inner.get(bucket, key, version_id)
        if hashlib.sha256(data).hexdigest() != self.manifests[(key, version_id)]:
            raise BronzeError(f"{key}: the manifest bytes differ from the pinned inventory")
        return data

    def download(self, bucket: str, key: str, version_id: str, path: Path) -> None:
        if (key, version_id) not in self.objects:
            raise BronzeError(f"{key}: not an object version of the pinned inventory")
        self.inner.download(bucket, key, version_id, path)


def verify_replay(inventory: dict[str, Any], inputs: list[dict[str, Any]]) -> None:
    """The replayed discovery must name exactly the pinned objects."""
    found = sorted([item["table"], item["key"], item["version_id"], item["sha256"], item["byte_count"]] for item in inputs)
    if found != sorted(inventory["objects"]):
        raise BronzeError("the replayed discovery differs from the pinned source inventory")
