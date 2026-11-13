"""The capture specification keeps only what a fresh capture needs from a stored receipt, job and reconciliation, with no bucket, key or local path."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.acquisition import build_capture_specs as specs
from tests.support import check

BUCKET = "example-bucket-name"


def write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def mirror(root: Path) -> list[dict]:
    """Two stored captures: a file unit with a job and a collector unit with an archive member map."""
    first = "data/datasets/snapshots/S1/S1__1/receipt.json"
    second = "data/datasets/snapshots/S2/S2__1/receipt.json"
    write(root / first, {"artifacts": [{"role": "data", "stored_file_name": "a.csv", "sha256": "0" * 64}]})
    write(
        root / Path(first).parent.parent / "job.json",
        {"plan": {"expected_format": "csv"}, "candidate": {"reviewed_unsigned_redirect": "https://example.invalid/r/"}},
    )
    write(root / second, {"artifacts": [{"role": "data", "stored_file_name": "b.zip"}, {"role": "reference", "stored_file_name": "c.txt"}]})
    objects = [
        {"archive_member": '["inner.zip", "m1.csv"]', "role": "data", "dataset_id": "D1", "storage_path": "x", "object": {"key": f"{BUCKET}/k1"}},
        {"archive_member": '["inner.zip", "m1.csv"]', "role": "reference", "dataset_id": "D1", "storage_path": "x", "object": {"key": f"{BUCKET}/k2"}},
        {"archive_member": "m2.txt", "role": "reference", "dataset_id": "D2", "storage_path": "y", "object": {"key": f"{BUCKET}/k3"}},
        {"role": "data", "dataset_id": "D1", "storage_path": "receipt.json", "object": {"key": f"{BUCKET}/k4"}},
    ]
    write(root / Path(second).parent / "s3_collections_reconciliation.json", {"objects": objects, "manifests": [{"object": {"key": f"{BUCKET}/m"}}]})
    return [
        {"snapshot_id": "S1__1", "mode": "file", "receipt": first, "disposition": "direct_download"},
        {"snapshot_id": "S2__1", "mode": "collector", "receipt": second, "disposition": "api"},
    ]


def test_each_unit_keeps_data_file_names_format_redirect_and_member_map(tmp_path: Path) -> None:
    units = mirror(tmp_path)
    result = json.loads(specs.derive(units, tmp_path, "f" * 64))
    check(result["units"] == 2 and result["queue_sha256"] == "f" * 64, "header")
    one, two = result["specs"]["S1__1"], result["specs"]["S2__1"]
    check(one["stored_data_files"] == ["a.csv"] and one["expected_format"] == "csv", "file unit format")
    check(one["reviewed_unsigned_redirect"] == "https://example.invalid/r/", "redirect")
    check(two["stored_data_files"] == ["b.zip"] and "expected_format" not in two, "collector unit")
    members = two["archive_members"]
    check({tuple(m["member_chain"]): m["role"] for m in members} == {("inner.zip", "m1.csv"): "data", ("m2.txt",): "reference"}, "data wins over reference")


def test_output_has_no_bucket_key_or_local_path(tmp_path: Path) -> None:
    text = specs.derive(mirror(tmp_path), tmp_path, "f" * 64).decode()
    check(BUCKET not in text and "s3://" not in text and str(tmp_path) not in text and "/Users/" not in text, "no environment values")


def test_derivation_is_deterministic(tmp_path: Path) -> None:
    units = mirror(tmp_path)
    check(specs.derive(units, tmp_path, "f" * 64) == specs.derive(list(reversed(units)), tmp_path, "f" * 64), "same bytes in any unit order")


def test_a_missing_receipt_is_refused_with_its_unit(tmp_path: Path) -> None:
    units = mirror(tmp_path)
    units.append({"snapshot_id": "S3__1", "mode": "file", "receipt": "data/datasets/snapshots/S3/S3__1/receipt.json", "disposition": "api"})
    with pytest.raises(ValueError, match="S3__1"):
        specs.derive(units, tmp_path, "f" * 64)
