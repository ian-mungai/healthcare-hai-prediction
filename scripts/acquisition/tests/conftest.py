from __future__ import annotations

import pytest

from scripts.acquisition import s3_store


@pytest.fixture(autouse=True)
def synthetic_collection_routes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        s3_store,
        "load_routes",
        lambda registry: {
            source["source_id"]: {"publisher": "example_publisher", "collection": "example_collection", "layout_sha256": "a" * 64}
            for source in registry["sources"]
        },
    )
