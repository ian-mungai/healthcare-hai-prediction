"""The AHRQ Compendium files as manual units: seed-only originals bound to the locked plan, no request, no derived file, no host on the approved list."""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from pathlib import Path

import pytest

from scripts.acquisition import add_ahrq_units as add_units
from scripts.acquisition import ahrq_compendium_contract as ahrq_contract
from scripts.acquisition import build_seed_manifest as build
from scripts.acquisition import check_baseline_run as check_run
from scripts.acquisition import manual_content, redownload_route
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.transport import hosts_digest
from tests.support import check

MODE = "ahrq_compendium"
BASE = "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/"
XLSX_NAME, XLS_NAME, PDF_NAME = "chsp-hospital-linkage-2018.xlsx", "compendium_system_hospital_linkage_file.xls", "2018-hospital-linkage-techdoc-appa.pdf"
BASELINE = Path("config/acquisition/baseline_run")
SEED = Path("config/acquisition/bootstrap_seed")


def make_unit(source: str, mode: str, url: str, snapshot: str) -> dict:
    return {"source_id": source, "mode": mode, "url": url, "snapshot_id": snapshot, "disposition": "approved_manual", "route": "manual_browser_download"}


def workbook(tag: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("[Content_Types].xml", "<Types/>")
        archive.writestr("xl/workbook.xml", f"<workbook><!-- {tag} --></workbook>")
    return buffer.getvalue()


BODIES = {
    XLSX_NAME: workbook("2018"),
    XLS_NAME: b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1 synthetic 2016 workbook",
    PDF_NAME: b"%PDF-1.4\n% synthetic technical document\n%%EOF\n",
}


def plan_of(bodies: dict[str, bytes]) -> dict:
    return {
        "files": [
            {
                "file_name": name,
                "sha256": hashlib.sha256(body).hexdigest(),
                "bytes": len(body),
                "origin_url": BASE + name,
                "vintage": "2018",
                "role": "data" if name.endswith(("x", "s")) else "methodology",
            }
            for name, body in bodies.items()
        ]
    }


@pytest.mark.parametrize(
    ("names", "refusal"),
    [([XLSX_NAME, "crosswalk.csv"], "exactly one"), (["data.csv"], "Unsupported|unsupported|format"), ([], "stored file names")],
    ids=["a_derived_file_is_named", "a_csv_is_not_an_ahrq_file", "no_file"],
)
def test_an_ahrq_unit_needs_exactly_one_stored_file_of_a_supported_format(names: list[str], refusal: str) -> None:
    unit = make_unit("AHRQ_SYSTEM", MODE, BASE + "chsp-hospital-linkage-2018.xlsx", "AHRQ_SYSTEM__0")
    with pytest.raises(manual_content.ManualUnitError, match=refusal):
        manual_content.plan_unit(unit, {"mode": MODE, "stored_data_files": names})


@pytest.mark.parametrize(("name", "expected"), [(XLSX_NAME, "xlsx"), (XLS_NAME, "xls"), (PDF_NAME, "pdf")])
def test_the_format_of_an_ahrq_file_comes_from_its_name(name: str, expected: str) -> None:
    unit = make_unit("AHRQ_SYSTEM", MODE, BASE + name, "AHRQ_SYSTEM__0")
    check(manual_content.plan_unit(unit, {"mode": MODE, "stored_data_files": [name]}) == (MODE, name), "the unit is accepted")
    check(manual_content.expected_format(MODE, name) == expected, f"{name} is {expected}")


def queue_of(*units: dict) -> dict:
    return {"queue": [{"disposition": "approved_manual", "bytes": 1, "pages": 1, "flags": [], **unit} for unit in units]}


def test_the_hosts_of_the_approved_list_skip_a_seed_only_unit_but_not_another_unit_of_the_same_host() -> None:
    ahrq = {"mode": MODE, "url": BASE + XLSX_NAME, "snapshot_id": "A1"}
    check(redownload_route.queue_hosts(queue_of(ahrq)) == set(), "an AHRQ unit adds no host")
    other = {"mode": "file", "url": "https://www.ahrq.gov/other/file.xlsx", "snapshot_id": "O1"}
    check(redownload_route.queue_hosts(queue_of(ahrq, other)) == {"www.ahrq.gov"}, "a unit of another mode at the same host still counts")
    hud = {"mode": "hud_xlsx", "url": "https://www.huduser.gov/apps/public/uspscrosswalk/home", "snapshot_id": "H1"}
    check(redownload_route.queue_hosts(queue_of(ahrq, hud)) == {"www.huduser.gov"}, "the hosts of the other manual units stay")
    check(MODE in redownload_route.SEED_ONLY_MODES and MODE in manual_content.SEED_ONLY_MODES, "the mode is declared seed-only once")


def write_bundle(root: Path, name: str = XLSX_NAME) -> tuple[Path, Path, dict]:
    """A bundle with one AHRQ unit, a seed folder beside it and the manifest built from the unit and the plan."""
    root.mkdir(parents=True)
    unit = {"disposition": "approved_manual", "mode": MODE, "snapshot_id": "AHRQ_SYSTEM__0", "source_id": "AHRQ_SYSTEM", "url": BASE + name}
    queue = (json.dumps({"queue": [unit]}, indent=1, sort_keys=True) + "\n").encode()
    specs = {"specs": {"AHRQ_SYSTEM__0": {"mode": MODE, "stored_data_files": [name]}}}
    (root / "queue.json").write_bytes(queue)
    (root / "capture_specs.json").write_text(json.dumps(specs))
    seed = root.parent / "seed"
    seed.mkdir()
    manifest = json.loads(build.build(queue, specs, [], {}, {name: hashlib.sha256(BODIES[name]).hexdigest()}))
    return root, seed, manifest


def test_the_manifest_lists_the_seed_only_original_with_the_digest_of_the_plan(tmp_path: Path) -> None:
    _, _, manifest = write_bundle(tmp_path / "bundle")
    [item] = manifest["files"]
    check(
        item == {"path": f"manual_units/AHRQ_SYSTEM__0/{XLSX_NAME}", "sha256": hashlib.sha256(BODIES[XLSX_NAME]).hexdigest(), "unit": "AHRQ_SYSTEM__0"},
        f"{item}",
    )


def test_the_seed_check_passes_without_the_ahrq_file_unless_files_are_required_and_refuses_a_changed_or_unlisted_one(tmp_path: Path) -> None:
    root, seed, manifest = write_bundle(tmp_path / "bundle")
    check(check_run.seed_problems(root, seed, manifest) == [], "an absent original is pending, as for the other manual originals")
    check(any("is missing from the seed folder" in p for p in check_run.seed_problems(root, seed, manifest, require_files=True)), "--require-files demands it")
    folder = seed / "manual_units" / "AHRQ_SYSTEM__0"
    folder.mkdir(parents=True)
    (folder / XLSX_NAME).write_bytes(BODIES[XLSX_NAME])
    check(check_run.seed_problems(root, seed, manifest, require_files=True) == [], "the exact original passes")
    (folder / XLSX_NAME).write_bytes(BODIES[XLSX_NAME] + b"x")
    check(any("differs from the manifest digest" in p for p in check_run.seed_problems(root, seed, manifest)), "a changed original is refused")
    (folder / XLSX_NAME).write_bytes(BODIES[XLSX_NAME])
    (folder / "extra.xlsx").write_bytes(b"x")
    check(any("extra.xlsx is not in the manifest" in p for p in check_run.seed_problems(root, seed, manifest)), "a file the manifest does not list is refused")
    (folder / "extra.xlsx").unlink()
    check("manual_units/AHRQ_SYSTEM__0" in " ".join(check_run.seed_problems(root, seed, {**manifest, "files": []})), "a manifest without the file is refused")


def test_a_seed_only_original_that_the_plan_does_not_bind_is_named(tmp_path: Path) -> None:
    root, seed, _ = write_bundle(tmp_path / "bundle")
    scopes = manual_content.Scopes(ahrq=plan_of({XLSX_NAME: BODIES[XLSX_NAME]}))
    folder = seed / "manual_units" / "AHRQ_SYSTEM__0"
    check(check_run.seed_only_problems(root, seed, scopes) == [], "an absent original is pending")
    folder.mkdir(parents=True)
    (folder / XLSX_NAME).write_bytes(BODIES[XLSX_NAME])
    check(check_run.seed_only_problems(root, seed, scopes) == [], "the planned original passes")
    (folder / XLSX_NAME).write_bytes(BODIES[XLSX_NAME][:-1] + b"X")
    [problem] = check_run.seed_only_problems(root, seed, scopes)
    check("AHRQ_SYSTEM__0" in problem and "plan" in problem, f"the unit is named: {problem}")


def committed() -> tuple[dict, dict, dict, dict]:
    queue = json.loads((BASELINE / "queue.json").read_bytes())
    specs = json.loads((BASELINE / "capture_specs.json").read_text(encoding="utf-8"))
    manifest = json.loads((SEED / "manifest.json").read_text(encoding="utf-8"))
    plan = json.loads(ahrq_contract.PLAN_PATH.read_text(encoding="utf-8"))
    return queue, specs, manifest, plan


def test_the_committed_queue_holds_the_thirteen_ahrq_files_as_seed_only_manual_units_bound_to_the_plan() -> None:
    queue, specs, manifest, plan = committed()
    units = [unit for unit in queue["queue"] if unit["mode"] == MODE]
    planned = {item["file_name"]: item for item in plan["files"]}
    check(len(units) == 13 and len(planned) == 13, f"{len(units)} units for {len(planned)} planned files")
    for unit in units:
        name = specs["specs"][unit["snapshot_id"]]["stored_data_files"]
        check(
            unit["disposition"] == "approved_manual" and unit["route"] == "manual_browser_download" and unit["source_id"] == "AHRQ_SYSTEM",
            f"{unit['snapshot_id']} shape",
        )
        check(len(name) == 1 and name[0] in planned, f"{unit['snapshot_id']} stores one planned file")
        item = planned[name[0]]
        check(unit["url"] == item["origin_url"] and unit["bytes"] == item["bytes"], f"{name[0]} url and bytes equal the plan")
        check(unit["snapshot_id"].endswith("__" + item["id"][:32]), f"{name[0]} snapshot ID carries the plan file id")
        listed = [f for f in manifest["files"] if f.get("unit") == unit["snapshot_id"]]
        check(
            listed == [{"path": f"manual_units/{unit['snapshot_id']}/{name[0]}", "sha256": item["sha256"], "unit": unit["snapshot_id"]}],
            f"{name[0]} manifest entry",
        )
    check(check_run.queue_problems(BASELINE) == [], "the queue, its lock and its budgets agree")


def test_the_committed_ahrq_units_record_when_and_from_where_the_owner_saved_each_file() -> None:
    queue, specs, _, plan = committed()
    units = [u for u in queue["queue"] if u["mode"] == MODE]
    check(len(units) == 13, f"{len(units)} AHRQ units")
    for unit in units:
        saved = specs["specs"][unit["snapshot_id"]]["saved"]
        check(saved["origin_url"] == unit["url"] and saved["saved_by"] == "owner, browser", f"{unit['snapshot_id']} origin and saver")
        check(
            saved["birth_time_utc"].startswith("2026-11-08T20:4") or saved["birth_time_utc"].startswith("2026-11-08T20:5"),
            f"{unit['snapshot_id']} saved on Nov 8 2026",
        )
        check(
            unit["snapshot_id"].split("__")[1] == saved["birth_time_utc"][:19].replace("-", "").replace(":", "") + "Z", "the snapshot ID carries the saved time"
        )
    check(len(plan["files"]) == 13, "plan size")


def test_www_ahrq_gov_stays_off_the_approved_host_list_while_the_hosts_file_binds_the_queue() -> None:
    queue_body = (BASELINE / "queue.json").read_bytes()
    hosts = json.loads((BASELINE / "publisher_hosts_scope.json").read_text(encoding="utf-8"))
    check("www.ahrq.gov" not in hosts["hosts"] and "www.ahrq.gov" not in redownload_route.queue_hosts(json.loads(queue_body)), "AHRQ is not a host of the run")
    check(
        redownload_route.host_problems(queue_body, BASELINE / "publisher_hosts_scope.json", BASELINE / "hosts_approval.json") == [],
        "the approval still governs the queue",
    )
    check(encoded_json(hosts) == (BASELINE / "publisher_hosts_scope.json").read_bytes(), "the hosts file keeps its form")


def test_a_seed_only_unit_names_its_source_and_a_source_with_its_own_record_is_checked_against_that_record() -> None:
    spec = {"mode": MODE, "stored_data_files": [XLSX_NAME], "release_binding": "access_release", "source_id": "AHRQ_SYSTEM"}
    check(check_run.release_problems({"A1": spec}) == [], "the tracked AHRQ record releases AHRQ_SYSTEM")
    [problem] = check_run.release_problems({"A1": spec | {"source_id": "NOT_A_SOURCE"}})
    check("NOT_A_SOURCE" in problem and "A1" in problem, f"another source is not released by it: {problem}")
    check(check_run.release_problems({"A1": {k: v for k, v in spec.items() if k != "release_binding"}}) == [], "no binding, nothing to check")


def small_bundle() -> tuple[bytes, bytes]:
    unit = {
        "bytes": 5,
        "disposition": "direct_download",
        "flags": [],
        "mode": "file",
        "pages": 1,
        "route": "publisher_url",
        "snapshot_id": "D1",
        "source_id": "ADJ",
        "url": "https://example.gov/d",
    }
    queue = {
        "budgets": {"acquisition_units": 1, "max_primary_attempts": 2, "pages_recorded": 1, "recorded_bytes": 5, "units_by_route": {"publisher_url": 1}},
        "derived_from": {"rule": "x"},
        "dispositions": {"direct_download": 1},
        "entries": 1,
        "kind": "bootstrap_capture_queue",
        "queue": [unit],
    }
    specs = {"kind": "capture_specs", "queue_sha256": "0" * 64, "specs": {"D1": {"mode": "file"}}, "template_rewrites": {}, "units": 1}
    return add_units.encode(queue), add_units.encode(specs)


def metadata_for(plan: dict) -> dict:
    return {
        "files": [
            {
                "file_name": item["file_name"],
                "size": item["bytes"],
                "sha256": item["sha256"],
                "birth_time_utc": f"2026-11-08T20:4{number % 10}:00.123456+00:00",
                "attributes": {
                    "com.apple.metadata:kMDItemDownloadedDate": [f"2026-11-08 20:4{number % 10}:00.234567"],
                    "com.apple.metadata:kMDItemWhereFroms": [item["origin_url"]],
                },
            }
            for number, item in enumerate(plan["files"])
        ]
    }


def test_the_unit_builder_appends_one_seed_only_unit_and_spec_per_planned_file_and_rebinds_the_bundle() -> None:
    queue_bytes, specs_bytes = small_bundle()
    plan = plan_of(BODIES) | {"files": [item | {"id": hashlib.sha256(item["file_name"].encode()).hexdigest()} for item in plan_of(BODIES)["files"]]}
    new_queue, new_specs = add_units.add(queue_bytes, specs_bytes, plan, metadata_for(plan))
    queue, specs = json.loads(new_queue), json.loads(new_specs)
    added = [unit for unit in queue["queue"] if unit["mode"] == MODE]
    check(len(added) == 3 and queue["queue"][0]["snapshot_id"] == "D1", "three units were appended after the existing one")
    check(all(unit["route"] == "manual_browser_download" and unit["disposition"] == "approved_manual" and unit["pages"] == 1 for unit in added), "unit shape")
    check(all("receipt" not in unit for unit in added), "no earlier receipt is claimed")
    budgets = queue["budgets"]
    check(budgets["acquisition_units"] == 4 and queue["entries"] == 4 and budgets["units_by_route"]["manual_browser_download"] == 3, f"{budgets}")
    check(budgets["recorded_bytes"] == 5 + sum(len(body) for body in BODIES.values()) and budgets["max_primary_attempts"] == 8, f"{budgets}")
    check(queue["dispositions"] == {"approved_manual": 3, "direct_download": 1}, f"{queue['dispositions']}")
    check(queue["derived_from"]["ahrq_units"]["added"] == 3 and queue["derived_from"]["rule"] == "x", "the addition is recorded beside the earlier record")
    check(
        specs["queue_sha256"] == hashlib.sha256(new_queue).hexdigest() and specs["units"] == 4 and list(specs["specs"]) == sorted(specs["specs"]),
        "specs follow the queue",
    )
    unit = added[0]
    saved = specs["specs"][unit["snapshot_id"]]
    check(
        saved["mode"] == MODE and saved["stored_data_files"] == [next(i["file_name"] for i in plan["files"] if i["origin_url"] == unit["url"])],
        "one stored file",
    )
    check(saved["release_binding"] == "access_release" and saved["source_id"] == "AHRQ_SYSTEM", "the held source is released by the tracked record")
    check(saved["saved"]["saved_by"] == "owner, browser" and saved["saved"]["origin_url"] == unit["url"], "saved by and origin are recorded")
    check(saved["saved"]["birth_time_utc"].endswith("+00:00") and saved["saved"]["downloaded_at_utc"].startswith("2026-11-08T20:4"), "both times are recorded")
    check(
        unit["snapshot_id"].startswith("AHRQ_SYSTEM__20261108T2040") and unit["snapshot_id"].endswith("__" + plan["files"][0]["id"][:32]),
        f"{unit['snapshot_id']}",
    )


def test_the_unit_builder_refuses_a_second_run_a_metadata_record_that_differs_from_the_plan_and_a_file_it_lacks() -> None:
    queue_bytes, specs_bytes = small_bundle()
    plan = plan_of(BODIES) | {"files": [item | {"id": hashlib.sha256(item["file_name"].encode()).hexdigest()} for item in plan_of(BODIES)["files"]]}
    metadata = metadata_for(plan)
    new_queue, new_specs = add_units.add(queue_bytes, specs_bytes, plan, metadata)
    with pytest.raises(ValueError, match="already"):
        add_units.add(new_queue, new_specs, plan, metadata)
    other = json.loads(json.dumps(metadata))
    other["files"][0]["sha256"] = "0" * 64
    with pytest.raises(ValueError, match="differs from the plan"):
        add_units.add(queue_bytes, specs_bytes, plan, other)
    with pytest.raises(ValueError, match="no metadata"):
        add_units.add(queue_bytes, specs_bytes, plan, {"files": metadata["files"][:-1]})
    wrong_origin = json.loads(json.dumps(metadata))
    wrong_origin["files"][0]["attributes"]["com.apple.metadata:kMDItemWhereFroms"] = ["https://example.org/x.xlsx"]
    with pytest.raises(ValueError, match="origin"):
        add_units.add(queue_bytes, specs_bytes, plan, wrong_origin)


def test_the_unit_builder_command_rebinds_the_queue_lock_and_the_hosts_file_and_keeps_the_host_list(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    queue_bytes, specs_bytes = small_bundle()
    root = tmp_path / "bundle"
    root.mkdir()
    (root / "queue.json").write_bytes(queue_bytes)
    (root / "capture_specs.json").write_bytes(specs_bytes)
    hosts = ["example.gov"]
    (root / "publisher_hosts_scope.json").write_bytes(encoded_json({"hosts": hosts, "hosts_sha256": hosts_digest(hosts), "queue_sha256": "0" * 64}))
    plan = {"files": [item | {"id": hashlib.sha256(item["file_name"].encode()).hexdigest()} for item in plan_of(BODIES)["files"]]}
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan))
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(json.dumps(metadata_for(plan)))
    monkeypatch.setattr(add_units, "PLAN", plan_path)
    monkeypatch.setattr("sys.argv", ["add_ahrq_units", "--metadata", str(metadata_path), "--root", str(root)])
    add_units.main()
    body = (root / "queue.json").read_bytes()
    check(json.loads((root / "queue.lock.json").read_text())["queue_sha256"] == hashlib.sha256(body).hexdigest(), "the queue lock follows the queue")
    document = json.loads((root / "publisher_hosts_scope.json").read_text())
    check(document["hosts"] == hosts and document["queue_sha256"] == hashlib.sha256(body).hexdigest(), "the host list is unchanged and bound to the new queue")
    check(json.loads((root / "capture_specs.json").read_text())["queue_sha256"] == hashlib.sha256(body).hexdigest(), "the specification follows the queue")


def test_the_committed_ahrq_plan_loads_against_the_registry_and_the_release_record_and_keeps_the_terms_record() -> None:
    plan = ahrq_contract.load_plan()
    check(len(plan["files"]) == 13 and plan["model_eligible"] is False, "the plan holds the 13 files and no model eligibility")
    check(plan["access_release"] == ahrq_contract.release_record(), "the plan binds the tracked release record by SHA-256")
    check(ahrq_contract.TERMS_RECORD["terms_status"] == "no_published_terms_found" and ahrq_contract.TERMS_RECORD["reuse_license"] == "none inferred", "terms")
    check("settle terms before any publication" in ahrq_contract.TERMS_RECORD["open_item"], "the open item stays recorded")
    check(any("no model approval" in text for text in ahrq_contract.USE_RESTRICTIONS), "the use restrictions keep the model hold")
    check(ahrq_contract.source_of({"sources": [{"source_id": "AHRQ_SYSTEM"}]}) == {"source_id": "AHRQ_SYSTEM"}, "the source is found")
