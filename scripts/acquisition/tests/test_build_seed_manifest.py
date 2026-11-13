"""The seed manifest lists the plan inputs outside the bundle and the stored files of the manual units, with a digest only where a tracked source holds one."""

from __future__ import annotations

import hashlib
import json

import pytest

from scripts.acquisition import build_seed_manifest as build
from tests.support import check

QUEUE = {
    "queue": [
        {"snapshot_id": "H1", "disposition": "approved_manual", "mode": "hud_xlsx", "source_id": "HUD"},
        {"snapshot_id": "A1", "disposition": "approved_manual", "mode": "file", "source_id": "ACS"},
        {"snapshot_id": "W1", "disposition": "approved_manual", "mode": "wonder_export", "source_id": "WONDER"},
        {"snapshot_id": "D1", "disposition": "direct_download", "source_id": "ADJ"},
    ]
}
SPECS = {
    "specs": {
        "H1": {"stored_data_files": ["ZIP-COUNTY_032010.xlsx", "crosswalk.csv"]},
        "A1": {"stored_data_files": ["acs.zip"]},
        "W1": {"stored_data_files": ["county_year.csv", "wonder_D76_1999_2005.txt"]},
        "D1": {"stored_data_files": ["d.txt"]},
    }
}
INPUTS = [
    "data/datasets/historical_acquisition/mmd_browser_history/x/captures/MMD/M1/raw/mmd_data.csv",
    "data/e2e/mmd_api_comparison/20260925/references/urban.tsv",
    "data/datasets/historical_acquisition/structure_history/jobs/J/captures/ADJ/A/raw/county_adjacency2010.txt",
]
DIGESTS = {"mmd_data.csv": "a" * 64, "urban.tsv": "b" * 64}
ORIGINALS = {"ZIP-COUNTY_032010.xlsx": "c" * 64, "wonder_D76_1999_2005.txt": "d" * 64}


def manifest() -> dict:
    queue = json.dumps(QUEUE).encode()
    return json.loads(build.build(queue, SPECS, INPUTS, DIGESTS, ORIGINALS))


def by_path(document: dict) -> dict:
    return {item["path"]: item for item in document["files"]}


def test_the_manifest_binds_the_queue_and_lists_every_file_sorted() -> None:
    document = manifest()
    check(document["kind"] == "bootstrap_seed_manifest" and document["queue_sha256"] == hashlib.sha256(json.dumps(QUEUE).encode()).hexdigest(), "header")
    paths = [item["path"] for item in document["files"]]
    check(paths == sorted(paths) and len(paths) == 6, f"six files in order: {paths}")


def test_plan_inputs_get_a_folder_by_kind_a_digest_from_the_source_and_their_hold() -> None:
    files = by_path(manifest())
    csv = files["mmd_browser_history/mmd_data.csv"]
    check(csv == {"path": "mmd_browser_history/mmd_data.csv", "sha256": "a" * 64, "plan_input": INPUTS[0]}, f"csv: {csv}")
    reference = files["mmd_references/urban.tsv"]
    check(reference == {"path": "mmd_references/urban.tsv", "sha256": "b" * 64, "plan_input": INPUTS[1]}, f"reference: {reference}")
    adjacency = files["structure_history/county_adjacency2010.txt"]
    check(adjacency["sha256"] is None and adjacency["hold"].startswith("pending"), f"adjacency: {adjacency}")


def test_a_manual_hud_unit_lists_its_stored_files_and_only_the_workbook_has_a_digest() -> None:
    files = by_path(manifest())
    check(files["manual_units/H1/ZIP-COUNTY_032010.xlsx"] == {"path": "manual_units/H1/ZIP-COUNTY_032010.xlsx", "sha256": "c" * 64, "unit": "H1"}, "workbook")
    check(files["manual_units/H1/crosswalk.csv"]["sha256"] is None, "a derived file has no source")
    check(not any(path.startswith(("manual_units/D1", "manual_units/A1")) for path in files), "a direct download and an ACS unit are not seeded")


def test_a_wonder_unit_lists_only_its_original_with_the_plan_digest_and_not_the_file_rebuilt_from_it() -> None:
    files = by_path(manifest())
    original = files["manual_units/W1/wonder_D76_1999_2005.txt"]
    check(original == {"path": "manual_units/W1/wonder_D76_1999_2005.txt", "sha256": "d" * 64, "unit": "W1"}, f"original: {original}")
    check("manual_units/W1/county_year.csv" not in files, "the derived county_year.csv is rebuilt on the host, not seeded")


def test_an_input_of_an_unknown_kind_is_refused() -> None:
    with pytest.raises(ValueError, match="no seed folder"):
        build.build(json.dumps(QUEUE).encode(), SPECS, ["data/other/x.bin"], {}, {})


def test_the_build_is_deterministic() -> None:
    queue = json.dumps(QUEUE).encode()
    check(
        build.build(queue, SPECS, INPUTS, DIGESTS, ORIGINALS) == build.build(queue, SPECS, INPUTS[::-1], DIGESTS, ORIGINALS), "same bytes for any input order"
    )
