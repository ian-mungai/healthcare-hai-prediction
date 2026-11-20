"""The seeded copy of the 2018 AHRQ workbook: only the workstation path element is removed, every other member stays, and the plan, spec and checks agree."""

from __future__ import annotations

import hashlib
import io
import json
import re
import shutil
import zipfile
from pathlib import Path

import pytest

from scripts.acquisition import ahrq_compendium_contract as ahrq_contract
from scripts.acquisition import ahrq_seed_copy as seed_copy
from scripts.acquisition import check_baseline_run as check_run
from scripts.acquisition.source_registry import canonical_hash
from tests.support import check

PATH_VALUE = "C:\\" + "Users\\synthetic_person\\Documents\\book.xlsx"
BLOCK = (
    '<mc:AlternateContent xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006"><mc:Choice Requires="x15">'
    f'<x15ac:absPath url="{PATH_VALUE}" xmlns:x15ac="http://schemas.microsoft.com/office/spreadsheetml/2010/11/ac"/></mc:Choice></mc:AlternateContent>'
)
HEAD = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\r\n<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
TAIL = '<sheets><sheet name="Hospitals" sheetId="1"/></sheets></workbook>'
MEMBERS = {
    "[Content_Types].xml": b"<Types/>",
    "_rels/.rels": b"<Relationships/>",
    "xl/workbook.xml": (HEAD + '<fileVersion appName="xl"/>' + BLOCK + "<workbookPr/>" + TAIL).encode(),
    "xl/worksheets/sheet1.xml": b"<worksheet><row>hospital names only</row></worksheet>",
}


def package(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, body in members.items():
            info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, body)
    return buffer.getvalue()


ORIGINAL = package(MEMBERS)
EXPECTED = (hashlib.sha256(ORIGINAL).hexdigest(), len(ORIGINAL))


def members_of(body: bytes) -> dict[str, bytes]:
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        return {info.filename: archive.read(info) for info in archive.infolist()}


def test_only_the_workstation_path_block_is_removed_and_every_other_member_stays_in_order() -> None:
    cleaned = seed_copy.clean(ORIGINAL, EXPECTED)
    before, after = members_of(ORIGINAL), members_of(cleaned)
    check(list(after) == list(before), "the same member names in the same order")
    check([n for n in before if before[n] != after[n]] == ["xl/workbook.xml"], "only xl/workbook.xml differs")
    check(after["xl/workbook.xml"] == before["xl/workbook.xml"].replace(BLOCK.encode(), b""), "exactly the AlternateContent block is gone")
    check(b"absPath" not in cleaned and b"synthetic_person" not in cleaned and b"synthetic_person" not in after["xl/workbook.xml"], "the path is gone")
    check(not [n for n in zipfile.ZipFile(io.BytesIO(cleaned)).namelist() if n.endswith("/")], "no directory entry was added")
    check(zipfile.ZipFile(io.BytesIO(cleaned)).testzip() is None, "the package is sound")


def test_the_copy_is_reproducible_and_the_result_is_well_formed_xml() -> None:
    check(seed_copy.clean(ORIGINAL, EXPECTED) == seed_copy.clean(ORIGINAL, EXPECTED), "two runs give the same bytes")
    from defusedxml import ElementTree

    ElementTree.fromstring(members_of(seed_copy.clean(ORIGINAL, EXPECTED))["xl/workbook.xml"])


@pytest.mark.parametrize(
    ("members", "message"),
    [
        ({**MEMBERS, "xl/workbook.xml": (HEAD + "<workbookPr/>" + TAIL).encode()}, "no workstation path"),
        ({**MEMBERS, "xl/workbook.xml": (HEAD + BLOCK + BLOCK + TAIL).encode()}, "more than one"),
        ({**MEMBERS, "xl/workbook.xml": (HEAD + f'<x15ac:absPath url="{PATH_VALUE}"/>' + TAIL).encode()}, "outside an AlternateContent block"),
        (
            {**MEMBERS, "xl/workbook.xml": (HEAD + BLOCK.replace("</mc:Choice>", "<mc:Fallback><other/></mc:Fallback></mc:Choice>") + TAIL).encode()},
            "holds more than the path element",
        ),
        ({**MEMBERS, "xl/other.xml": f'<x15ac:absPath url="{PATH_VALUE}"/>'.encode()}, "another member"),
    ],
    ids=["no_path_element", "two_blocks", "element_outside_a_block", "block_with_other_content", "path_in_another_member"],
)
def test_the_step_refuses_anything_but_exactly_one_path_block(members: dict[str, bytes], message: str) -> None:
    body = package(members)
    with pytest.raises(ValueError, match=message) as error:
        seed_copy.clean(body, (hashlib.sha256(body).hexdigest(), len(body)))
    check("synthetic_person" not in str(error.value), "the refusal never quotes the path")


def test_the_step_refuses_an_input_other_than_the_recorded_original_and_a_non_package() -> None:
    with pytest.raises(ValueError, match="not the recorded original"):
        seed_copy.clean(ORIGINAL, ("0" * 64, len(ORIGINAL)))
    with pytest.raises(ValueError, match="not the recorded original"):
        seed_copy.clean(ORIGINAL, (EXPECTED[0], len(ORIGINAL) + 1))
    with pytest.raises(ValueError, match="not a workbook package"):
        seed_copy.clean(b"not a zip", (hashlib.sha256(b"not a zip").hexdigest(), 9))


def plan_with(original: bytes) -> dict:
    entry = {"file_name": seed_copy.NAME, "sha256": hashlib.sha256(original).hexdigest(), "bytes": len(original), "role": "data"}
    entry["id"] = ahrq_contract.file_id(entry)
    return {"files": [entry]}


def test_the_plan_entry_takes_the_digest_size_and_id_of_the_copy_and_a_second_update_changes_nothing() -> None:
    plan = plan_with(ORIGINAL)
    copy = seed_copy.clean(ORIGINAL, EXPECTED)
    updated = seed_copy.update_plan(plan, copy)
    [entry] = updated["files"]
    check((entry["sha256"], entry["bytes"]) == (hashlib.sha256(copy).hexdigest(), len(copy)), "the entry binds the copy")
    check(entry["id"] == ahrq_contract.file_id(entry) and entry["id"] != plan["files"][0]["id"], "the file id follows the new binding")
    check(seed_copy.update_plan(updated, copy) == updated, "a second update changes nothing")
    check(canonical_hash(updated) != canonical_hash(plan), "the plan hash moves, so its lock must follow")


def test_the_committed_plan_binds_the_copy_and_the_spec_records_the_original_and_the_change() -> None:
    plan = {item["file_name"]: item for item in json.loads(ahrq_contract.PLAN_PATH.read_text(encoding="utf-8"))["files"]}
    entry = plan[seed_copy.NAME]
    recorded = seed_copy.COPIES[seed_copy.NAME]
    check((entry["sha256"], entry["bytes"]) != (recorded["original_sha256"], recorded["original_bytes"]), "the plan no longer binds the original")
    root = Path("config/acquisition/baseline_run")
    queue = json.loads((root / "queue.json").read_bytes())["queue"]
    specs = json.loads((root / "capture_specs.json").read_text(encoding="utf-8"))["specs"]
    [unit] = [u for u in queue if u["mode"] == "ahrq_compendium" and u["url"] == entry["origin_url"]]
    spec = specs[unit["snapshot_id"]]
    check(spec["seeded_copy"] == recorded and recorded["change"] == "workstation path element removed from xl/workbook.xml", f"{spec['seeded_copy']}")
    check(unit["bytes"] == entry["bytes"] and unit["snapshot_id"].endswith("__" + entry["id"][:32]), "the unit binds the copy")
    others = [
        s
        for n, s in ((specs[u["snapshot_id"]]["stored_data_files"][0], specs[u["snapshot_id"]]) for u in queue if u["mode"] == "ahrq_compendium")
        if n != seed_copy.NAME
    ]
    check(all("seeded_copy" not in s for s in others), "no other file is a copy")


def test_the_committed_seed_object_is_the_copy_and_holds_no_workstation_path_element() -> None:
    plan = {item["file_name"]: item for item in json.loads(ahrq_contract.PLAN_PATH.read_text(encoding="utf-8"))["files"]}
    entry = plan[seed_copy.NAME]
    folders = [p for p in Path("config/acquisition/bootstrap_seed/manual_units").glob(f"*__{entry['id'][:32]}")]
    check(len(folders) == 1, f"one seed folder for the copy: {folders}")
    path = folders[0] / seed_copy.NAME
    body = path.read_bytes()
    if body.startswith(b"version https://git-lfs"):
        pytest.skip("the seed object is not fetched in this checkout")
    check((hashlib.sha256(body).hexdigest(), len(body)) == (entry["sha256"], entry["bytes"]), "the seeded file is the copy the plan binds")
    check(not re.search(rb"absPath", zipfile.ZipFile(path).read("xl/workbook.xml")), "the element is not in xl/workbook.xml")
    old = Path("config/acquisition/bootstrap_seed/manual_units").glob("*__38e56a1156f76e77c86bf985077de29c")
    check(list(old) == [], "the folder of the original is gone")


def provenance_root(tmp_path: Path, spec_change: dict | None, plan_sha: str | None = None) -> tuple[Path, Path]:
    root, seed = tmp_path / "bundle", tmp_path / "seed"
    root.mkdir(parents=True)
    seed.mkdir()
    unit = {
        "disposition": "approved_manual",
        "mode": "ahrq_compendium",
        "snapshot_id": "A1",
        "source_id": "AHRQ_SYSTEM",
        "url": "https://www.ahrq.gov/x/" + seed_copy.NAME,
    }
    spec = {"mode": "ahrq_compendium", "stored_data_files": [seed_copy.NAME], "seeded_copy": seed_copy.COPIES[seed_copy.NAME]} | (spec_change or {})
    (root / "queue.json").write_text(json.dumps({"queue": [unit]}))
    (root / "capture_specs.json").write_text(json.dumps({"specs": {"A1": spec}}))
    return root, seed


def test_the_baseline_check_accepts_exactly_the_recorded_copy_provenance(tmp_path: Path) -> None:
    root, _ = provenance_root(tmp_path, None)
    plan = {"files": [{"file_name": seed_copy.NAME, "sha256": "1" * 64, "bytes": 5}]}
    check(check_run.copy_problems(root, plan) == [], "the recorded provenance passes")
    root2, _ = provenance_root(tmp_path / "two", {"seeded_copy": {**seed_copy.COPIES[seed_copy.NAME], "original_bytes": 1}})
    check(any("seeded_copy" in p for p in check_run.copy_problems(root2, plan)), "another original is refused")
    root3, _ = provenance_root(tmp_path / "three", {"seeded_copy": None})
    check(any("seeded_copy" in p for p in check_run.copy_problems(root3, plan)), "a missing provenance is refused")
    same = {
        "files": [
            {
                "file_name": seed_copy.NAME,
                "sha256": seed_copy.COPIES[seed_copy.NAME]["original_sha256"],
                "bytes": seed_copy.COPIES[seed_copy.NAME]["original_bytes"],
            }
        ]
    }
    root4, _ = provenance_root(tmp_path / "four", None)
    check(any("original" in p for p in check_run.copy_problems(root4, same)), "a plan that still binds the original is refused")
    root5, _ = provenance_root(tmp_path / "five", {"stored_data_files": ["other.xlsx"]})
    problems = check_run.copy_problems(root5, plan)
    check(
        len(problems) == 1 and "records a seeded_copy for other.xlsx, which is no seeded copy" in problems[0],
        f"a provenance on a file that is no copy is refused: {problems}",
    )


def test_the_whole_baseline_check_refuses_a_committed_bundle_whose_copy_record_was_changed(tmp_path: Path) -> None:
    root = tmp_path / "bundle"
    shutil.copytree(check_run.ROOT, root)
    check([p for p in check_run.problems(root, check_run.SEED_ROOT) if "seeded_copy" in p] == [], "the committed bundle records its copy exactly")
    path = root / "capture_specs.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    [spec] = [item for item in document["specs"].values() if "seeded_copy" in item]
    spec["seeded_copy"]["change"] = "another change"
    path.write_text(json.dumps(document, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    (root / "inputs.lock.json").write_bytes(check_run.build_lock(root))
    found = [p for p in check_run.problems(root, check_run.SEED_ROOT) if "seeded_copy" in p]
    check(len(found) == 1 and "must record the original and the change" in found[0], f"{found}")
