"""Plan inputs that stay in S3 are pinned by key, version and digest taken from the stored index, and only when the plan's digest agrees."""

from __future__ import annotations

import json

import pytest

from scripts.acquisition import build_remote_pins as pins
from tests.support import check

SHA = "a" * 64
INDEX = {"data/x/a.csv": {"bucket": "example-bucket", "byte_count": 7, "key": "cms/a.csv", "sha256": SHA, "version_id": "v1"}}


def test_an_indexed_input_with_an_agreeing_digest_is_pinned_without_the_bucket() -> None:
    document = json.loads(pins.build({"data/x/a.csv": SHA, "data/x/t.txt": "b" * 64}, INDEX, ["data/x/a.csv"], {}))
    check(document["remote"] == [{"path": "data/x/a.csv", "sha256": SHA, "bytes": 7, "s3_key": "cms/a.csv", "s3_version_id": "v1"}], "pin")
    check("example-bucket" not in json.dumps(document), "no bucket name")


def test_a_digest_that_disagrees_with_the_plan_is_refused() -> None:
    with pytest.raises(ValueError, match="disagrees"):
        pins.build({"data/x/a.csv": "c" * 64}, INDEX, ["data/x/a.csv"], {})


def test_an_input_missing_from_the_index_becomes_a_pending_note_with_its_reason() -> None:
    document = json.loads(pins.build({"data/x/a.csv": SHA, "data/x/m.csv": "d" * 64}, INDEX, ["data/x/a.csv", "data/x/m.csv"], {"data/x/m.csv": "not stored"}))
    check(document["pending"] == [{"path": "data/x/m.csv", "reason": "not stored"}], "pending")


def test_expected_inputs_list_every_plan_input_sorted() -> None:
    document = json.loads(pins.build({"b": "1" * 64, "a": SHA}, {}, [], {"a": "x", "b": "y"}))
    check(document["expected_plan_inputs"] == ["a", "b"], "sorted")
