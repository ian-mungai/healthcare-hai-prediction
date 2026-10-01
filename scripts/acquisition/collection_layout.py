"""Explicit publisher/collection routes for every source, including access-held sources."""

from __future__ import annotations

from pathlib import Path

from scripts.acquisition.dataset_layout import dataset_id
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, read_json
from scripts.acquisition.transport import CaptureError

LAYOUT_PATH = REPO_ROOT / "config/acquisition/collection_layout.json"


def collection_routes(registry: dict, layout: dict) -> dict[str, dict]:
    """Return one explicit publisher/collection route per registered source, preserving held sources."""
    if set(layout) != {"layout_version", "groups"} or layout["layout_version"] != "4.0.0":
        raise CaptureError("An explicit version 4 publisher/collection layout is required.")
    routes: dict[str, dict] = {}
    for group in layout["groups"]:
        if set(group) != {"publisher", "collection", "source_ids"} or not group["source_ids"]:
            raise CaptureError("Each collection requires only its publisher, collection name and source IDs.")
        publisher, collection = dataset_id(group["publisher"]), dataset_id(group["collection"])
        for source in group["source_ids"]:
            if source in routes:
                raise CaptureError("A source cannot route to multiple collections.")
            routes[source] = {"publisher": publisher, "collection": collection, "layout_sha256": canonical_hash(layout)}
    if set(routes) != {source["source_id"] for source in registry["sources"]}:
        raise CaptureError("Collection layout must map the entire source registry exactly, without dropping holds.")
    return routes


def load_routes(registry: dict, path: Path = LAYOUT_PATH) -> dict[str, dict]:
    """Read the collection layout and return its validated source routes."""
    return collection_routes(registry, read_json(path))


def object_prefix(route: dict, folder: str, role: str, release: str, run: str, evidence_only: bool) -> str:
    """Return the collection prefix for the selected role, release and evidence status."""
    root = f"{dataset_id(route['publisher'])}/{dataset_id(route['collection'])}"
    partition = f"release_date={release}" if release != run else f"capture_id={run}"
    if evidence_only or role in {"transport_audit", "archive_inventory", "source_metadata_conflict"}:
        return f"{root}/audit/{run}"
    if role in {"data", "api_page"}:
        return f"{root}/datasets/{dataset_id(folder)}/{partition}"
    if role == "capture_receipt":
        return f"{root}/manifests/{partition}/{run}"
    return f"{root}/references/{partition}"
