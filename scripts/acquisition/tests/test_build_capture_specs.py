"""The capture specification keeps only what a fresh capture needs from a stored receipt, job and reconciliation, with no bucket, key or local path."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.acquisition import build_capture_specs as specs
from scripts.acquisition.rewrite_registry_provenance import POINTER_FROM
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


CONDITIONS = [{"measure_id": "AMI", "condition_label": "Acute Myocardial Infarction", "domain_label": "Primary chronic conditions"}]
TEMPLATE = {
    "source_id": "S",
    "mode": "file",
    "publisher": "Example Publisher",
    "scope": "an example scope",
    "release": {"observed_history": "none"},
    "measurement_periods": [{"label": "p"}],
    "governance": {"access_class": "public", "credentials_required": False},
    "role": "data",
    "export_selections": {},
}


def stored(root: Path, name: str, receipt: dict, *, job: dict | None = None, objects: list[dict] | None = None) -> str:
    """One stored capture under the mirror: its receipt, an optional job file above it and an optional reconciliation file beside it."""
    path = f"data/datasets/snapshots/{name}/{name}__1/receipt.json"
    write(root / path, {"artifacts": [{"role": "data", "stored_file_name": f"{name}.csv"}], **receipt})
    if job is not None:
        write(root / Path(path).parent.parent / "job.json", job)
    if objects is not None:
        write(root / Path(path).parent / "s3_collections_reconciliation.json", {"objects": objects, "manifests": []})
    return path


def collector_receipt(**lineage: object) -> dict:
    return {"lineage": {"extraction_or_query": json.dumps(lineage)}}


def collector_units(root: Path) -> list[dict]:
    """Two BLS units that share one URL, an MMD API unit, a flagged browser export, a file unit with a job plan, one without, and manual units."""
    url = "https://api.example.invalid/timeseries"
    units = [
        {"snapshot_id": "B1", "mode": "bls_api_v2", "receipt": stored(root, "B1", collector_receipt(batch_id="b1")), "url": url},
        {"snapshot_id": "B2", "mode": "bls_api_v2", "receipt": stored(root, "B2", collector_receipt(batch_id="b2")), "url": url},
        {"snapshot_id": "M1", "mode": "mmd_api", "receipt": stored(root, "M1", collector_receipt(measure_id="AMI", year=2020))},
        {
            "snapshot_id": "M2",
            "mode": "file",
            "flags": ["browser_export_moved_to_api:parity_verified"],
            "receipt": stored(
                root,
                "M2",
                {
                    "acquisition": {
                        "export_selections": {
                            "condition": CONDITIONS[0]["condition_label"],
                            "domain": CONDITIONS[0]["domain_label"],
                            "geography": "County",
                            "year": "2019",
                        }
                    }
                },
            ),
        },
        {
            "snapshot_id": "F1",
            "mode": "file",
            "receipt": stored(root, "F1", {}, job={"plan": {**TEMPLATE, "route_id": "r", "file_name": "f.csv", "expected_format": "csv"}}),
        },
        {"snapshot_id": "F2", "mode": "file", "receipt": stored(root, "F2", {})},
        {"snapshot_id": "O1", "mode": "cms_owners_org", "receipt": stored(root, "O1", collector_receipt(release_id="r1"))},
    ]
    objects = [
        {
            "role": "data",
            "storage_path": "raw/h.xlsx",
            "object": {"bucket": BUCKET, "key": "hud/x/h.xlsx", "version_id": "v1", "sha256": "1" * 64, "byte_count": 7},
        },
        {
            "role": "export_receipt",
            "storage_path": "evidence/p.json",
            "object": {"bucket": BUCKET, "key": "hud/x/p.json", "version_id": "v2", "sha256": "2" * 64, "byte_count": 3},
        },
    ]
    held = {"snapshot_id": "H1", "mode": "hud_xlsx", "disposition": "approved_manual"}
    units.append(held | {"receipt": stored(root, "H1", {"acquisition": {"retrieved_at_utc": "2026-09-28T21:36:18Z"}}, objects=objects)})
    units.append({"snapshot_id": "H2", "mode": "file", "disposition": "approved_manual", "receipt": stored(root, "H2", {}, objects=objects)})
    units.append(held | {"snapshot_id": "H4", "receipt": stored(root, "H4", {})})
    units.append(
        held | {"snapshot_id": "H3", "receipt": stored(root, "H3", {}, objects=[{**objects[0], "object": {**objects[0]["object"], "key": "raw/h3.xlsx"}}])}
    )
    return units


def derived(tmp_path: Path) -> dict:
    return json.loads(specs.derive(collector_units(tmp_path), tmp_path, "f" * 64, CONDITIONS))["specs"]


def test_two_units_with_one_url_get_distinct_planned_item_ids(tmp_path: Path) -> None:
    result = derived(tmp_path)
    check(result["B1"]["plan_item"] == "b1" and result["B2"]["plan_item"] == "b2", "batch IDs")
    check(result["O1"]["plan_item"] == "r1", "release ID")


def test_a_collector_unit_whose_receipt_has_no_planned_item_id_is_refused(tmp_path: Path) -> None:
    units = collector_units(tmp_path)
    units[0]["receipt"] = stored(tmp_path, "B9", collector_receipt(other="x"))
    units[0]["snapshot_id"] = "B9"
    with pytest.raises(ValueError, match="B9.*planned item"):
        specs.derive(units, tmp_path, "f" * 64, CONDITIONS)


def test_an_mmd_unit_names_its_measure_and_year_from_its_lineage_or_its_export_selections(tmp_path: Path) -> None:
    result = derived(tmp_path)
    check(result["M1"]["mmd"] == {"measure_id": "AMI", "year": 2020} and "plan_item" not in result["M1"], "api unit")
    check(result["M2"]["mmd"] == {"measure_id": "AMI", "year": 2019}, "browser export moved to the API")
    with pytest.raises(ValueError, match="M2.*exactly one"):
        specs.derive(collector_units(tmp_path), tmp_path, "f" * 64, [])


def test_a_file_unit_keeps_the_plan_fields_of_its_job_and_one_without_a_job_has_none(tmp_path: Path) -> None:
    result = derived(tmp_path)
    check(result["F1"]["receipt_template"] == TEMPLATE, f"template: {result['F1']}")
    check(result["F1"]["expected_format"] == "csv" and "route_id" not in result["F1"]["receipt_template"], "format stays beside, route and file name out")
    check("receipt_template" not in result["F2"] and "receipt_template" not in result["M2"], "no job, or an MMD unit")


def test_a_manual_unit_names_its_newest_stored_capture_and_one_without_records_has_no_field(tmp_path: Path) -> None:
    result = derived(tmp_path)
    expected = {
        "read_root": "",
        "artifacts": [
            {"role": "data", "key": "hud/x/h.xlsx", "version_id": "v1", "sha256": "1" * 64, "byte_count": 7, "retrieved_at_utc": "2026-09-28T21:36:18Z"},
            {
                "role": "export_receipt",
                "key": "hud/x/p.json",
                "version_id": "v2",
                "sha256": "2" * 64,
                "byte_count": 3,
                "retrieved_at_utc": "2026-09-28T21:36:18Z",
            },
        ],
    }
    check(result["H1"]["last_capture"] == expected, f"last capture: {result['H1'].get('last_capture')}")
    check("last_capture" not in result["H4"], "no reconciliation, no field")
    check(
        "last_capture" not in result["H2"] and "receipt_template" not in result["H2"], "an ACS file unit moves to the Census API: no last capture, no template"
    )
    check(result["H3"]["last_capture"]["read_root"] == "raw/", "a capture under raw/")
    check(BUCKET not in json.dumps(result), "no bucket name")


def test_a_template_loses_agent_home_pointers_and_phone_examples_and_the_header_counts_them(tmp_path: Path) -> None:
    phone = f"({'1' * 3}) {'2' * 3}-{'3' * 4}"
    history = f'[{{"file": "{POINTER_FROM}a/b.csv", "other": "{POINTER_FROM}c.csv", "examples": ["{phone}", "keep"]}}]'
    template = {**TEMPLATE, "release": {"observed_history": history}}
    stored(tmp_path, "F1", {}, job={"plan": {**template, "route_id": "r", "file_name": "f.csv"}})
    unit = {"snapshot_id": "F1", "mode": "file", "receipt": "data/datasets/snapshots/F1/F1__1/receipt.json"}
    document = json.loads(specs.derive([unit], tmp_path, "f" * 64))
    cleaned = document["specs"]["F1"]["receipt_template"]["release"]["observed_history"]
    check(cleaned == '[{"file": "/workspace/provenance/a/b.csv", "other": "/workspace/provenance/c.csv", "examples": ["(redacted)", "keep"]}]', cleaned)
    check(document["template_rewrites"] == {"pointers": 2, "phone_examples": 1}, f"{document['template_rewrites']}")
