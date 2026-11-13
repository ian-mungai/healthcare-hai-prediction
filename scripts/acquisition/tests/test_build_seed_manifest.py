"""The seed manifest lists the plan inputs outside the bundle and the stored files of the manual units, with a digest only where a tracked source holds one."""

from __future__ import annotations

import hashlib
import json

import pytest

from scripts.acquisition import build_seed_manifest as build
from scripts.acquisition import check_baseline_run as check_run
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
DIGESTS = {"mmd_data.csv": "a" * 64, "urban.tsv": "b" * 64, "county_adjacency2010.txt": "e" * 64}
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
    check(paths == sorted(paths) and len(paths) == 5, f"five files in order: {paths}")


def test_plan_inputs_get_a_folder_by_kind_and_a_digest_from_the_source() -> None:
    files = by_path(manifest())
    csv = files["mmd_browser_history/mmd_data.csv"]
    check(csv == {"path": "mmd_browser_history/mmd_data.csv", "sha256": "a" * 64, "plan_input": INPUTS[0]}, f"csv: {csv}")
    reference = files["mmd_references/urban.tsv"]
    check(reference == {"path": "mmd_references/urban.tsv", "sha256": "b" * 64, "plan_input": INPUTS[1]}, f"reference: {reference}")
    adjacency = files["structure_history/county_adjacency2010.txt"]
    check(adjacency == {"path": "structure_history/county_adjacency2010.txt", "sha256": "e" * 64, "plan_input": INPUTS[2]}, f"adjacency: {adjacency}")


def test_a_manual_hud_unit_lists_only_its_workbook_with_the_plan_digest_and_not_the_crosswalk_rebuilt_from_it() -> None:
    files = by_path(manifest())
    check(files["manual_units/H1/ZIP-COUNTY_032010.xlsx"] == {"path": "manual_units/H1/ZIP-COUNTY_032010.xlsx", "sha256": "c" * 64, "unit": "H1"}, "workbook")
    check("manual_units/H1/crosswalk.csv" not in files, "the derived crosswalk.csv is rebuilt on the host, not seeded")
    check(not any(path.startswith(("manual_units/D1", "manual_units/A1")) for path in files), "a direct download and an ACS unit are not seeded")


def test_a_wonder_unit_lists_only_its_original_with_the_plan_digest_and_not_the_file_rebuilt_from_it() -> None:
    files = by_path(manifest())
    original = files["manual_units/W1/wonder_D76_1999_2005.txt"]
    check(original == {"path": "manual_units/W1/wonder_D76_1999_2005.txt", "sha256": "d" * 64, "unit": "W1"}, f"original: {original}")
    check("manual_units/W1/county_year.csv" not in files, "the derived county_year.csv is rebuilt on the host, not seeded")


BROWSER = "data/datasets/historical_acquisition/mmd_browser_history/P/captures/MMD/MMD__S/raw/mmd_data.csv"
STRUCTURE = "data/datasets/historical_acquisition/structure_history/jobs/J/captures/ADJ/ADJ__S/raw/county_adjacency2010.txt"
REFERENCES = "data/e2e/mmd_api_comparison/20260925/references/urban.tsv"


@pytest.mark.parametrize(
    ("path", "folder"),
    [(BROWSER, "mmd_browser_history"), (STRUCTURE, "structure_history"), (REFERENCES, "mmd_references")],
    ids=["browser_history_capture", "structure_history_capture", "mmd_comparison_reference"],
)
def test_the_seed_folder_follows_from_the_fixed_layout_of_each_kind_of_plan_input(path: str, folder: str) -> None:
    check(build.input_folder(path) == folder, f"{path} takes {build.input_folder(path)}, not {folder}")


@pytest.mark.parametrize(
    "path",
    [
        "data/other/mmd_browser_history/x/captures/MMD/M/raw/f.csv",
        "data/datasets/other/jobs/structure_history/J/captures/ADJ/S/raw/f.txt",
        "data/datasets/historical_acquisition/other/structure_history/jobs/J/captures/ADJ/S/raw/f.txt",
        "data/datasets/historical_acquisition/structure_history/other/mmd_browser_history/captures/ADJ/S/raw/f.txt",
        "data/e2e/other/mmd_api_comparison/20260925/references/f.tsv",
        "data/e2e/mmd_api_comparison/20260925/other/mmd_browser_history/f.tsv",
        "x/structure_history/f.txt",
        "/mmd_api_comparison/f.tsv",
    ],
    ids=[
        "marker_below_other_data_folder",
        "marker_in_a_job_folder",
        "marker_below_an_unrelated_collection",
        "second_marker_in_a_structure_path",
        "marker_below_an_unrelated_e2e_folder",
        "second_marker_in_a_comparison_path",
        "marker_in_a_relative_path",
        "marker_only",
    ],
)
def test_a_marker_text_in_an_unrelated_folder_does_not_choose_a_seed_folder(path: str) -> None:
    with pytest.raises(ValueError, match="no seed folder"):
        build.input_folder(path)


def test_the_refusal_names_the_path_and_the_layouts_it_knows() -> None:
    with pytest.raises(ValueError, match=r"data/other/x\.bin.*mmd_browser_history.*mmd_references.*structure_history"):
        build.input_folder("data/other/x.bin")


def test_an_input_of_an_unknown_kind_is_refused() -> None:
    with pytest.raises(ValueError, match="no seed folder"):
        build.build(json.dumps(QUEUE).encode(), SPECS, ["data/other/x.bin"], {}, {})


def test_the_build_is_deterministic() -> None:
    queue = json.dumps(QUEUE).encode()
    check(
        build.build(queue, SPECS, INPUTS, DIGESTS, ORIGINALS) == build.build(queue, SPECS, INPUTS[::-1], DIGESTS, ORIGINALS), "same bytes for any input order"
    )


def test_a_period_row_gives_the_digest_of_the_input_whose_capture_folder_is_its_release_and_whose_name_is_its_file() -> None:
    rows = [
        {"member_sha256": "1" * 64, "release_id": "ADJ__A", "file_name": "county_adjacency2010.txt"},
        {"member_sha256": "2" * 64, "release_id": "ADJ__B", "file_name": "county_adjacency2010.txt"},
        {"member_sha256": "3" * 64, "release_id": "ADJ__A", "file_name": "other.txt"},
    ]
    found = build.period_digests(["x/captures/ADJ/ADJ__A/raw/county_adjacency2010.txt", "x/captures/MMD/M1/raw/mmd_data.csv"], rows)
    check(found == {"county_adjacency2010.txt": "1" * 64}, f"found {found}")


def test_a_release_named_by_another_folder_of_the_path_than_the_capture_folder_selects_no_digest() -> None:
    rows = [{"member_sha256": "1" * 64, "release_id": "ADJ__A", "file_name": "county_adjacency2010.txt"}]
    other_capture = "x/jobs/ADJ__A/captures/ADJ/ADJ__B/raw/county_adjacency2010.txt"
    check(build.period_digests([other_capture], rows) == {}, "the job folder names release A but the capture folder is release B")
    check(
        build.period_digests(["ADJ__A/captures/ADJ/ADJ__B/raw/county_adjacency2010.txt"], rows) == {}, "the first folder of the path is not the capture folder"
    )


@pytest.mark.parametrize(
    "path",
    ["x/captures/ADJ/ADJ__A/county_adjacency2010.txt", "x/jobs/ADJ__A/raw/county_adjacency2010.txt", "x/ADJ/ADJ__A/raw/county_adjacency2010.txt"],
    ids=["no_raw_folder", "no_captures_folder", "capture_folder_not_below_captures"],
)
def test_a_path_that_is_not_laid_out_as_captures_source_release_raw_file_selects_no_digest(path: str) -> None:
    rows = [{"member_sha256": "1" * 64, "release_id": "ADJ__A", "file_name": "county_adjacency2010.txt"}]
    check(build.period_digests([path], rows) == {}, f"{path} is not a capture path")


def test_two_period_rows_with_different_digests_for_one_input_are_refused() -> None:
    rows = [{"member_sha256": digest, "release_id": "ADJ__A", "file_name": "county_adjacency2010.txt"} for digest in ("1" * 64, "2" * 64)]
    with pytest.raises(ValueError, match="more than one digest"):
        build.period_digests(["x/captures/ADJ/ADJ__A/raw/county_adjacency2010.txt"], rows)


def test_the_committed_manifest_is_byte_for_byte_what_the_builder_writes_from_the_committed_bundle() -> None:
    committed = (check_run.SEED_ROOT / check_run.SEED_MANIFEST).read_bytes()
    check(build.manifest_body(check_run.ROOT) == committed, "rebuild the manifest with python -m scripts.acquisition.build_seed_manifest")
