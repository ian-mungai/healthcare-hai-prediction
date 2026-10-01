from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scripts.acquisition import collection_layout as layout
from tests.support import check


def test_entire_pool_has_routes_without_clearing_holds(tmp_path: Path) -> None:
    registry = {
        "sources": [
            {"source_id": "example_table", "preferred_route": "file_download"},
            {"source_id": "example_reference", "preferred_route": "reference_only"},
            {"source_id": "example_held", "preferred_route": "access_hold"},
        ]
    }
    path = tmp_path / "layout.json"
    path.write_text(
        json.dumps(
            {
                "layout_version": "4.0.0",
                "groups": [
                    {"publisher": "example", "collection": "tables", "source_ids": ["example_table", "example_reference", "example_held"]},
                ],
            }
        ),
        encoding="utf-8",
    )
    original = copy.deepcopy(registry)
    routes = layout.load_routes(registry, path)
    check(set(routes) == {source["source_id"] for source in registry["sources"]}, 'set(routes) == {source["source_id"] for source in registry["sources"]}')
    check(len(routes) == 3 and registry == original, "len(routes) == 3 and registry == original")
    check(
        sum(source["preferred_route"] == "access_hold" for source in registry["sources"]) == 1,
        'sum(source["preferred_route"] == "access_hold" for source in registry["sources"]) == 1',
    )
    check(all(route["collection"] == "tables" for route in routes.values()), 'all(route["collection"] == "tables" for route in routes.values())')


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "unsafe", "version", "extra_field", "empty"])
def test_invalid_routes_cannot_silently_fall_back(mutation: str) -> None:
    registry = {"sources": [{"source_id": "example_source"}]}
    config: dict = {"layout_version": "4.0.0", "groups": [{"publisher": "example", "collection": "collection", "source_ids": ["example_source"]}]}
    if mutation == "missing":
        config["groups"] = []
    elif mutation == "duplicate":
        config["groups"] *= 2
    elif mutation == "unsafe":
        config["groups"][0]["publisher"] = "../outside"
    elif mutation == "version":
        config["layout_version"] = "3.0.0"
    elif mutation == "extra_field":
        config["groups"][0]["extra"] = True
    else:
        config["groups"][0]["source_ids"] = []
    with pytest.raises(layout.CaptureError):
        layout.collection_routes(registry, config)


def test_unknown_release_uses_capture_id_not_an_invented_date() -> None:
    route = {"publisher": "example", "collection": "collection"}
    prefix = layout.object_prefix(route, "table", "dictionary", "capture_1", "capture_1", False)
    check(prefix == "example/collection/references/capture_id=capture_1", 'prefix == "example/collection/references/capture_id=capture_1"')
