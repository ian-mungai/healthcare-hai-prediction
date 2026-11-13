"""The completion record schema: digests that decide skip and the refusals a reader depends on."""

from __future__ import annotations

import copy
from typing import Any

import pytest

from scripts.acquisition import completion_record as record
from tests.support import check

DIGEST = "a" * 64
ROOT = "raw/"


def stored(key: str, **extra: Any) -> dict:
    return {"key": key, "version_id": "v1", "sha256": DIGEST, "byte_count": 3} | extra


def body(**changes: Any) -> dict:
    data = [stored("raw/p/c/datasets/d/x/a.csv", role="data", stored_file_name="a.csv")]
    other = [stored("raw/p/c/references/x/page.json", role="api_page")]
    fields = {
        "key": DIGEST,
        "source_id": "S",
        "collection": "p/c",
        "mode": "file",
        "run_id": "run01",
        "code_sha256": DIGEST,
        "bundle_sha256": DIGEST,
        "retrieved_at_utc": "2026-11-12T00:00:00Z",
        "receipt": stored("raw/p/c/manifests/x/receipt.json"),
        "manifests": [stored("raw/p/c/manifests/x/manifest.json")],
        "data_objects": data,
        "other_objects": other,
    }
    return record.build(**(fields | changes))


def key_of(item: dict) -> str:
    return record.record_key(ROOT, item["unit_key"], item["payload_sha256"])


def test_the_payload_digest_counts_data_files_only_and_ignores_order_and_other_objects() -> None:
    first = body()
    second = body(other_objects=[stored("raw/p/c/references/y/other.json", role="api_page", sha256="b" * 64)])
    check(first["payload_sha256"] == second["payload_sha256"], "an API page or reference that differs leaves the payload digest alone")
    pair = [("a.csv", DIGEST), ("b.csv", "b" * 64)]
    check(record.payload_sha256(pair) == record.payload_sha256(reversed(pair)), "the digest does not depend on order")
    check(record.payload_sha256(pair) != record.payload_sha256(pair[:1]), "another data file set is another payload")
    renamed = body(data_objects=[stored("raw/p/c/datasets/d/x/a.csv", role="data", stored_file_name="renamed.csv")])
    check(renamed["payload_sha256"] != first["payload_sha256"], "the stored file name is part of the payload")


def test_a_stored_record_parses_back_to_the_same_record() -> None:
    item = body()
    check(record.parse(record.encode(item), key=key_of(item), root=ROOT) == item, "the round trip keeps every field")
    check(
        {found["key"] for found in record.objects_named(item)} >= {"raw/p/c/manifests/x/receipt.json", "raw/p/c/references/x/page.json"},
        "every object is named",
    )


def forge(change: str) -> tuple[bytes, str]:
    item = body()
    key = key_of(item)
    if change == "data digest":
        item["data_objects"][0]["sha256"] = "f" * 64
    elif change == "extra field":
        item["note"] = "x"
    elif change == "missing manifest":
        item["manifests"] = []
    elif change == "data role":
        item["data_objects"][0]["role"] = "dictionary"
    elif change == "other is data":
        item["other_objects"][0]["role"] = "data"
    elif change == "version":
        item["receipt"]["version_id"] = "null"
    elif change == "collection":
        item["collection"] = "no_slash"
    elif change == "schema version":
        item["schema_version"] = 2
    elif change == "other key":
        key = record.record_key(ROOT, "e" * 64, item["payload_sha256"])
    elif change == "other payload":
        key = record.record_key(ROOT, item["unit_key"], "e" * 64)
    elif change == "other root":
        key = record.record_key("ingestion_test/t1/", item["unit_key"], item["payload_sha256"])
    return record.encode(item) if change != "not json" else b"{", key


@pytest.mark.parametrize(
    "change",
    [
        "data digest",
        "extra field",
        "missing manifest",
        "data role",
        "other is data",
        "version",
        "collection",
        "schema version",
        "other key",
        "other payload",
        "other root",
        "not json",
    ],
)
def test_a_record_that_is_not_exactly_the_schema_or_does_not_name_its_own_key_is_refused(change: str) -> None:
    payload, key = forge(change)
    with pytest.raises(record.RecordError):
        record.parse(payload, key=key, root=ROOT)


def test_the_unit_key_is_the_digest_of_the_canonical_identity() -> None:
    identity = {"source_id": "S", "mode": "file", "request": {"method": "GET", "url": "https://example.org/a"}}
    reordered = copy.deepcopy(dict(reversed(list(identity.items()))))
    check(record.unit_key(identity) == record.unit_key(reordered), "key order does not change the unit key")
    check(record.unit_key(identity) != record.unit_key(identity | {"mode": "api"}), "another handler mode is another unit")
