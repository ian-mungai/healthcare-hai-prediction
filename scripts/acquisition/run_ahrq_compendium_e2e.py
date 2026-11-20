"""Repeatable synthetic AHRQ Compendium hospital linkage capture, from owner-saved files to versioned S3, without live downloads or credentials."""

import argparse
import base64
import copy
import hashlib
import io
import json
import os
import re
import shutil
import sys
import tempfile
import zipfile
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

from scripts.acquisition import ahrq_compendium_contract as contract
from scripts.acquisition import history_routes, reference_download_contract, s3_store
from scripts.acquisition import store_ahrq_compendium as store
from scripts.acquisition.code_closure import closure_hashes
from scripts.acquisition.collection_layout import capture_root, load_routes, object_prefix
from scripts.acquisition.dataset_layout import source_folder
from scripts.acquisition.run_bls_api_e2e import inventory
from scripts.acquisition.run_hud_xlsx_e2e import download_metadata, set_origin
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, read_json, require
from scripts.acquisition.store_ahrq_compendium import execute, run_all, verify_local
from scripts.acquisition.tests.test_s3_store import OUTPUTS, SETTINGS, FakeS3

BASE = "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/"
XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
REFERRER = "https://www.example.org/compendium-page"
# What the owner saved on Nov 8 2026: the six data files and seven technical documents, by the README's SHA-256 and size.
OWNER_FILES = {
    "compendium_system_hospital_linkage_file.xls": ("8e3e61b0d1e8200cf15ab1d077e9d2c75eb9b3c0f77fa38a0317359762735660", 1798656, "data", "2016"),
    "chsp-hospital-linkage-2018.xlsx": ("0c4a56df0c25168a23cef0e62af2eeec335176d808f4f235ff10733073c89880", 654428, "data", "2018"),
    "chsp-hospital-linkage-2020-rev.xlsx": ("4a8a19e50edabdddb3b7948d9bbf0d1f997a3563e484ff5dd971f22ea56846fb", 966049, "data", "2020"),
    "chsp-hospital-linkage-2021-rev.xlsx": ("e63cc5ce0783745761dcce75723f91e400e0160ade4829cab1df8297a2a61856", 970270, "data", "2021"),
    "chsp-hospital-linkage-2022-rev.xlsx": ("ff270a157cbef0463039b34206dbd83415db46a8f2bce00706fe8ab255dad801", 972775, "data", "2022"),
    "chsp-hospital-linkage-2023.xlsx": ("33346db251947e26d3ef842c546cd1a740e376d8aa6437725f2c1fd5d68be4d4", 1086704, "data", "2023"),
    "chsp_linkage_file_tech_doc.pdf": ("6ad73489b3186a4737e1088dab3522677a0de13789a9bde1a1732f2d917c9274", 358194, "methodology", "2016"),
    "2018-hospital-linkage-techdoc-appa.pdf": ("6f8f61ed3344abdffb2361b1af13e4ede87d919467f650fc5c89e6c5fabe7cc7", 199446, "methodology", "2018"),
    "2018-hospital-linkage-techdoc-cx.pdf": ("52b37832f4bbd0155c37054bc7cc1a21547119f34c7f42f588157959179810a4", 839143, "methodology", "2018"),
    "2020-hospital-linkage-techdoc.pdf": ("b0c7d2b53488b91d7d51c0cdd7545c0a024209a4bc2b22b65f52b9995344532e", 549758, "methodology", "2020"),
    "2021-hospital-linkage-techdoc-rev.pdf": ("2e0c4bdebefab0fc1aabe8471cb2df87c0980850eb0bad89e5a25d838f8fe02c", 473959, "methodology", "2021"),
    "2022-hospital-linkage-techdoc-021224.pdf": ("7efe618e4d651ceae2c96c19e358095172adf6ea92b1f2d6330ab74af4cc0f69", 495854, "methodology", "2022"),
    "2023-hospital-linkage-techdoc.pdf": ("e0b72c98b0cad5dcf6683ce11ed7e81bdf0446633f1fee85c07489d675459f88", 599075, "methodology", "2023"),
}
# The owner's terms decision of 9 Nov 2026: AHRQ publishes no terms text for these files, and what was checked is recorded in every receipt.
TERMS = {
    "terms_status": "no_published_terms_found",
    "internet_citation": "Compendium of U.S. Health Systems, 2023. Agency for Healthcare Research and Quality, Rockville, MD. "
    "https://www.ahrq.gov/chsp/data-resources/compendium-2023.html",
    "checked_on": "2026-11-09",
    "checked": [
        "Compendium 2023 page, https://www.ahrq.gov/chsp/data-resources/compendium-2023.html: Internet citation only",
        "https://www.ahrq.gov/policy/electronic/disclaimers/index.html: no reuse text",
        "the seven technical documents and six data files of the plan: no copyright, public domain or permission text",
    ],
    "reuse_license": "none inferred",
    "open_item": "settle terms before any publication of AHRQ-derived data",
}
CONTENT = re.compile(r"(?:raw/|ingestion_test/ingestion_[a-f0-9]{8}/)?bronze_(?:objects|evidence)/sha256/[0-9a-f]{64}")
ENABLED = {"enabled": True, "objects": {}, "evidence_objects": {}, "bootstrap_sha256": "0" * 64}
# The two earlier release records stay byte-identical: receipts bind them by SHA-256.
EARLIER_RECORDS = {
    "config/acquisition/access_releases_20260929.json": "d15851bb7c686cb360de0bc8549aaaf8d96803a67d91c566771247366dd54b4d",
    "config/acquisition/access_releases_20261002.json": "d86fe056f24f59a0e5c9c11df216cdd9d6894179659f0ca1066960517289b90e",
}
# The shared release records of the testing branch, oldest first; the AHRQ record belongs to none of them and the shared binding never moves to it.
SHARED_RECORDS = (*EARLIER_RECORDS, "config/acquisition/access_releases_20261117.json")


def synthetic_workbook(tag: str) -> bytes:
    """A small valid .xlsx package: only its package structure matters to the capture."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", '<?xml version="1.0"?><Types/>')
        archive.writestr("xl/workbook.xml", f"<workbook><!-- synthetic {tag} --></workbook>")
    return buffer.getvalue()


def request(name: str, role: str, vintage: str, media_type: str) -> dict:
    return {
        "source_id": "AHRQ_SYSTEM",
        "role": role,
        "title": f"Synthetic compendium file {name}",
        "publisher": "Agency for Healthcare Research and Quality",
        "origin_url": BASE + name,
        "file_name": name,
        "media_type": media_type,
        "vintage": vintage,
        "release_binding": "access_release",
    }


REQUESTS = [
    request("synthetic_linkage_2016.xls", "data", "2016", "application/vnd.ms-excel"),
    request("synthetic-linkage-2018.xlsx", "data", "2018", XLSX),
    request("synthetic-linkage-2023.xlsx", "data", "2023", XLSX),
    request("synthetic-techdoc-2018.pdf", "methodology", "2018", "application/pdf"),
    request("synthetic-techdoc-2023.pdf", "methodology", "2023", "application/pdf"),
]
BODIES = {
    "synthetic_linkage_2016.xls": b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1 synthetic 2016 workbook",
    "synthetic-linkage-2018.xlsx": synthetic_workbook("2018"),
    "synthetic-linkage-2023.xlsx": synthetic_workbook("2023"),
    "synthetic-techdoc-2018.pdf": b"%PDF-1.4\n% synthetic 2018 technical document\n%%EOF\n",
    "synthetic-techdoc-2023.pdf": b"%PDF-1.4\n% synthetic 2023 technical document\n%%EOF\n",
}


def downloads_for(target: Path, requests: list[dict] = REQUESTS) -> None:
    """Write each synthetic file with the origin a browser records: the file URL, then the referring page."""
    target.mkdir(parents=True)
    for item in requests:
        path = target / item["file_name"]
        path.write_bytes(BODIES[item["file_name"]])
        set_origin(path, [item["origin_url"], REFERRER])


def refusing(action: Any, folder: Path) -> Any:
    """Run an action that must be refused; a refusal also proves nothing was stored, so a missing refusal never passes."""

    def run() -> None:
        try:
            action()
        except (ValueError, OSError, KeyError, TypeError):
            require(not any(p.is_file() for p in (folder / "files").rglob("*")), "A refused run wrote files")
            raise

    return run


def exercise(root: Path) -> list[dict]:
    """Use production orchestration with synthetic owner-saved files and versioned fake storage."""
    downloads = root / "downloads"
    downloads_for(downloads)
    plan_path, versions = root / "plan.json", root / "versions.json"
    write_once(versions, encoded_json({"versions": [{"code_sha256": closure_hashes("scripts.acquisition.store_ahrq_compendium")}]}))
    scenarios: list[dict] = []

    def scenario(name: str, action: Any, error: str | None = None) -> None:
        try:
            action()
            scenarios.append({"name": name, "passed": error is None, "observed": "success", "expected": error or "success"})
        except (ValueError, OSError, KeyError, TypeError, StopIteration, IndexError, AssertionError, NotImplementedError) as failure:
            # The temporary folder's path never counts toward a match: a path can contain the expected words.
            observed = str(failure).replace(str(root), "<root>")
            scenarios.append({"name": name, "passed": error is not None and error in observed, "observed": observed[:200], "expected": error or "success"})

    with patch.object(contract, "PLAN_PATH", plan_path), patch.object(contract, "VERSIONS_PATH", versions):
        plan = contract.build_plan(REQUESTS, downloads)
        write_once(plan_path, encoded_json(plan))
        write_once(plan_path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(plan)}))
        entries = {e["file_name"]: e for e in plan["files"]}
        data_entry, doc_entry = entries["synthetic-linkage-2018.xlsx"], entries["synthetic-techdoc-2018.pdf"]
        client = FakeS3()
        scenario("locked_scope", contract.load_plan)
        scenario(
            "plan_binds_hash_and_size",
            lambda: require(fingerprint(downloads / data_entry["file_name"]) == (data_entry["sha256"], data_entry["bytes"]), "Binding differs"),
        )
        committed_scenarios(scenario)
        planning_scenarios(scenario, downloads)
        run = root / "run"
        scenario("capture_without_storage", lambda: execute(plan, data_entry, run, downloads, False, client, OUTPUTS))
        receipt_path = run / "files" / data_entry["id"] / "capture/receipt.json"
        scenario("offline_receipt_replay", lambda: verify_local(receipt_path))

        def provenance() -> None:
            receipt = verify_local(receipt_path)
            acquisition, lineage = receipt["acquisition"], json.loads(receipt["lineage"]["extraction_or_query"])
            require(acquisition["http_status"] is None and acquisition["request_method"] == "manual_download", "Browser download given an HTTP status")
            require(acquisition["requested_url"] == data_entry["origin_url"] and acquisition["preferred_route"] == "file_download", "Route differs")
            proof = read_json(receipt_path.parent / "evidence/download_proof.json")
            require(proof["origin_url"] == data_entry["origin_url"] and proof["created_at_utc"] == acquisition["retrieved_at_utc"], "Proof differs")
            require(all(REFERRER not in p.read_text(errors="ignore") for p in run.rglob("*") if p.is_file()), "Referring page persisted")
            require({a["role"] for a in receipt["artifacts"]} == {"data", "export_receipt"}, "Roles differ")
            require((receipt_path.parent / "raw" / data_entry["file_name"]).read_bytes() == BODIES[data_entry["file_name"]], "Original changed")
            require(lineage["mode"] == "ahrq_compendium" and lineage["model_eligible"] is False, "Lineage differs")
            require(lineage["access_release_sha256"] == plan["access_release"]["sha256"], "Hold release not bound")
            require(receipt["governance"]["access_class"] == "public" and receipt["governance"]["contains_pii"] is False, "Governance differs")
            require(lineage.get("terms_record") == TERMS, f"Terms record differs: {lineage.get('terms_record')}")
            restrictions = " ".join(receipt["governance"]["use_restrictions"]).lower()
            require("not recorded yet" not in restrictions and "no reuse license" in restrictions, "Use restrictions still carry the placeholder")
            require(not any("not recorded yet" in w for w in receipt["quality_profile"]["warnings"]), "Warning still carries the placeholder")
            require(receipt["release"]["release_date"] is None and "2018" in receipt["release"]["publisher_release_label"], "Release label differs")

        scenario("no_http_status_planned_origin_only_original_kept_release_bound", provenance)
        scenario("capture_and_version_verified_storage", lambda: execute(plan, data_entry, run, downloads, True, client, OUTPUTS))

        def data_destination() -> None:
            keys = [k for k in client.objects if "/manifests/" not in k]
            data = [k for k in keys if "/datasets/" in k]
            prefix = "ahrq/health_system_compendium/datasets/ahrq_system/"
            require(len(data) == 1 and data[0].startswith(prefix) and data[0].endswith("/synthetic-linkage-2018.xlsx"), f"Data destination differs: {data}")
            require(all("/references/" in k for k in keys if k not in data), f"Proof stored outside references: {keys}")

        scenario("data_file_stored_under_the_ahrq_collection", data_destination)
        before, objects = inventory(run), copy.deepcopy(client.objects)

        def repeat() -> None:
            execute(plan, data_entry, run, downloads, True, client, OUTPUTS)
            require(before == inventory(run) and objects == client.objects, "Repeated effects")

        scenario("second_run_writes_nothing_new", repeat)
        scenario("third_run_writes_nothing_new", repeat)

        def downloads_not_needed() -> None:
            moved = root / "moved"
            moved.mkdir()
            execute(plan, data_entry, run, moved, True, client, OUTPUTS)
            require(before == inventory(run) and objects == client.objects, "Replay needed the Downloads copy")

        scenario("replay_without_downloads_copy", downloads_not_needed)

        def document_destination() -> None:
            s3 = FakeS3()
            execute(plan, doc_entry, root / "doc", downloads, True, s3, OUTPUTS)
            keys = [k for k in s3.objects if "/manifests/" not in k]
            require(bool(keys) and all("/references/" in k and "/datasets/" not in k for k in keys), f"Document stored as data: {keys}")

        scenario("technical_document_stored_as_reference_only", document_destination)
        relocked_scenarios(scenario, plan, plan_path, root)
        hold_scenarios(scenario, plan, data_entry, root, downloads)
        staging_scenarios(scenario, plan, data_entry, root, client)
        corrupt_scenarios(scenario, plan, receipt_path)
        storage_scenarios(scenario, plan, data_entry, root, run, downloads)
        binding_scenarios(scenario, plan, root)
        default_run_scenarios(scenario, plan, data_entry, doc_entry, root, downloads)
        single_copy_scenarios(scenario, plan, data_entry, root, downloads)
        adapted_scenarios(scenario, plan, data_entry, doc_entry, root, downloads, receipt_path)
        round2_scenarios(scenario, plan, data_entry, doc_entry, root, downloads)
        round3_scenarios(scenario, plan, data_entry, root, downloads)

        def tampered_plan() -> None:
            original = plan_path.read_bytes()
            try:
                plan_path.write_bytes(encoded_json(plan | {"model_eligible": True}))
                contract.load_plan()
            finally:
                plan_path.write_bytes(original)

        scenario("plan_tampering_rejected", tampered_plan, "plan lock differs")

        def unreviewed_current() -> None:
            with patch.object(contract, "closure_hashes", return_value={"changed": "0" * 64}):
                execute(plan, doc_entry, root / "unreviewed", downloads, True, client, OUTPUTS)

        scenario("unreviewed_current_code_before_any_effect", refusing(unreviewed_current, root / "unreviewed"), "Unreviewed AHRQ")

        def independent() -> None:
            partial = root / "partial_downloads"
            downloads_for(partial)
            (partial / "synthetic-linkage-2023.xlsx").unlink()
            s3 = FakeS3()
            results, held = run_all(plan, root / "all", partial, True, s3, OUTPUTS)
            require(
                [h["file_name"] for h in held] == ["synthetic-linkage-2023.xlsx"] and "missing or not a regular file" in held[0]["reason"],
                f"Held set differs: {held}",
            )
            require(len(results) == 4 and all(r["status"] == "stored" for r in results), "Other files did not complete")
            again, held_again = run_all(plan, root / "all", partial, True, s3, OUTPUTS)
            require(again == results and len(held_again) == 1, "Rerun differs")

        scenario("missing_file_held_others_stored", independent)
    return scenarios


def committed_scenarios(scenario: Any) -> None:
    """The committed plan binds the owner's thirteen files by origin, hash and size, and the hold lift is one append-only record."""
    plan_path = REPO_ROOT / "config/acquisition/ahrq_compendium_plan.json"

    def real_plan() -> None:
        with patch.object(contract, "PLAN_PATH", plan_path):
            real = contract.load_plan()
        found = {f["file_name"]: (f["sha256"], f["bytes"], f["role"], f["vintage"]) for f in real["files"]}
        require(found == OWNER_FILES, "Committed plan differs from the owner's saved files")
        require(all(f["origin_url"].startswith("https://www.ahrq.gov/sites/default/files/wysiwyg/") for f in real["files"]), "Origin differs")
        require(real["registry_sha256"] == canonical_hash(load_registry()), "Registry differs")

    scenario("committed_plan_binds_the_owner_files", real_plan)

    def release_record() -> None:
        record = read_json(REPO_ROOT / contract.RELEASE_PATH)
        require(record["previous_record"] == "config/acquisition/access_releases_20261002.json", "Chain differs")
        require(
            len(record["releases"]) == 1 and record["releases"][0]["source_id"] == "AHRQ_SYSTEM" and record["releases"][0]["released"] is True,
            "Release differs",
        )
        require(
            getattr(s3_store, "SOURCE_RELEASE_PATHS", None) == {"AHRQ_SYSTEM": REPO_ROOT / contract.RELEASE_PATH},
            "The AHRQ record is not selected by its source",
        )
        require(tuple(REPO_ROOT / p for p in SHARED_RECORDS) == s3_store.ACCESS_RELEASE_PATHS, "The shared record list changed")
        require(REPO_ROOT / SHARED_RECORDS[-1] == s3_store.ACCESS_RELEASE_PATH, "The shared binding is not the newest shared record")
        source = next(s for s in load_registry()["sources"] if s["source_id"] == "AHRQ_SYSTEM")
        require(source["preferred_route"] == "access_hold", "Base registry changed")

    scenario("hold_lift_is_one_new_record_for_ahrq_system_only", release_record)

    def earlier_unchanged() -> None:
        for name, digest in EARLIER_RECORDS.items():
            require(contract.digest((REPO_ROOT / name).read_bytes()) == digest, f"{name} changed")

    scenario("earlier_release_records_unchanged", earlier_unchanged)

    def bronze_entry() -> None:
        tables = {t["table"]: t for t in read_json(REPO_ROOT / "config/lakehouse/bronze_tables.json")["tables"]}
        table = tables["ahrq_hospital_linkage"]
        require(
            table["collection"] == "ahrq/health_system_compendium" and table["format"] == "sheet_rows" and "dataset_ids" not in table, "Bronze entry differs"
        )
        route = load_routes(load_registry())["AHRQ_SYSTEM"]
        require(f"{route['publisher']}/{route['collection']}" == table["collection"], "Bronze collection differs from the layout")
        picked = {name: re.search(table["member_pattern"], name) is not None for name in OWNER_FILES}
        require(picked == {name: name.endswith((".xls", ".xlsx")) for name in OWNER_FILES}, f"The table selects the wrong owner files: {picked}")
        require(table.get("roles", ["data"]) == ["data"], "The table reads other roles than data")

    scenario("bronze_table_entry_matches_the_collection_layout", bronze_entry)


def planning_scenarios(scenario: Any, downloads: Path) -> None:
    """Plans refuse other sources, roles, hosts and formats, and files that are missing, edited or not what they claim to be."""
    data, doc = REQUESTS[1], REQUESTS[3]
    cases = [
        ("unknown_source", data | {"source_id": "NOT_A_SOURCE"}, "entry invalid"),
        ("other_held_source", data | {"source_id": "AHA"}, "entry invalid"),
        ("dictionary_role", doc | {"role": "dictionary"}, "entry invalid"),
        (
            "plain_http_origin",
            data | {"origin_url": "http://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/synthetic-linkage-2018.xlsx"},
            "entry invalid",
        ),
        (
            "origin_on_other_host",
            data | {"origin_url": "https://example.org/sites/default/files/wysiwyg/chsp/compendium/synthetic-linkage-2018.xlsx"},
            "entry invalid",
        ),
        ("origin_name_differs_from_file", data | {"origin_url": BASE + "other.xlsx"}, "entry invalid"),
        ("data_role_on_a_pdf", doc | {"role": "data"}, "entry invalid"),
        ("document_role_on_a_workbook", data | {"role": "methodology"}, "entry invalid"),
        ("media_type_differs", data | {"media_type": "text/csv"}, "entry invalid"),
        ("unsafe_file_name", data | {"file_name": "../synthetic-linkage-2018.xlsx"}, "entry invalid"),
        ("bad_vintage", data | {"vintage": "latest"}, "entry invalid"),
        ("missing_binding", {k: v for k, v in data.items() if k != "release_binding"}, "entry invalid"),
        ("unknown_field", data | {"http_status": 200}, "entry invalid"),
    ]
    for name, item, error in cases:
        scenario(f"{name}_rejected", lambda item=item: contract.build_plan([item], downloads), error)
    scenario("duplicate_file_rejected", lambda: contract.build_plan([data, data], downloads), "entry invalid")

    def prepared(name: str, body: bytes | None, origins: list[str] | None, item: dict) -> Path:
        folder = downloads.parent / f"plan_{name}"
        folder.mkdir()
        if body is not None:
            (folder / item["file_name"]).write_bytes(body)
            if origins is not None:
                set_origin(folder / item["file_name"], origins)
        return folder

    def symlinked() -> None:
        folder = downloads.parent / "plan_link"
        folder.mkdir()
        (folder / data["file_name"]).symlink_to(downloads / data["file_name"])
        contract.build_plan([data], folder)

    scenario("symlinked_download_rejected", symlinked, "not a regular file")
    scenario("file_missing_at_planning", lambda: contract.build_plan([data], prepared("nofile", None, None, data)), "missing or not a regular file")
    scenario("origin_missing_at_planning", lambda: contract.build_plan([data], prepared("no_origin", BODIES[data["file_name"]], None, data)), "origin missing")
    scenario(
        "other_origin_at_planning",
        lambda: contract.build_plan([data], prepared("other_origin", BODIES[data["file_name"]], ["https://mirror.example.org/x.xlsx"], data)),
        "origin differs",
    )
    scenario(
        "workbook_that_is_not_a_package_rejected",
        lambda: contract.build_plan([data], prepared("fake_xlsx", b"just text", [data["origin_url"]], data)),
        "not a valid xlsx",
    )
    scenario(
        "pdf_that_is_html_rejected",
        lambda: contract.build_plan([doc], prepared("fake_pdf", b"<html>error</html>", [doc["origin_url"]], doc)),
        "not a valid pdf",
    )
    scenario("empty_file_rejected", lambda: contract.build_plan([doc], prepared("empty", b"", [doc["origin_url"]], doc)), "not a valid pdf")


def relocked_scenarios(scenario: Any, plan: dict, plan_path: Path, root: Path) -> None:
    """A plan that was edited and re-locked is still refused by the plan's own checks."""

    def relocked(edit: Any, record: Path | None = None) -> Any:
        def action() -> None:
            original, lock = plan_path.read_bytes(), plan_path.with_suffix(".lock.json").read_bytes()
            changed = copy.deepcopy(plan)
            edit(changed)
            try:
                plan_path.write_bytes(encoded_json(changed))
                plan_path.with_suffix(".lock.json").write_bytes(encoded_json({"plan_sha256": canonical_hash(changed)}))
                if record is None:
                    contract.load_plan()
                else:
                    with patch.object(contract, "RELEASE_PATH", str(record)):
                        contract.load_plan()
            finally:
                plan_path.write_bytes(original)
                plan_path.with_suffix(".lock.json").write_bytes(lock)

        return action

    scenario("relocked_other_release_record_refused", relocked(lambda c: c["access_release"].__setitem__("sha256", "0" * 64)), "release record differs")
    two = root / "two_sources_released.json"
    two.write_text(json.dumps({"releases": [{"source_id": "AHRQ_SYSTEM", "released": True}, {"source_id": "AHA", "released": True}]}))
    record = {"path": str(two), "sha256": contract.digest(two.read_bytes())}
    scenario("relocked_record_releasing_another_source_refused", relocked(lambda c: c.__setitem__("access_release", record), two), "release record differs")
    scenario("relocked_model_eligible_refused", relocked(lambda c: c.__setitem__("model_eligible", True)), "hold differs")
    scenario("relocked_unknown_vintage_refused", relocked(lambda c: c["files"][0].__setitem__("vintage", "latest")), "entry invalid")
    scenario("relocked_bytes_without_new_id_refused", relocked(lambda c: c["files"][0].__setitem__("bytes", 1)), "entry invalid")


def hold_scenarios(scenario: Any, plan: dict, entry: dict, root: Path, downloads: Path) -> None:
    """The hold stays in force for storage unless the bound release record is listed; nothing reaches S3 otherwise."""

    def hold_enforced() -> None:
        s3 = FakeS3()
        with (
            patch.object(s3_store, "ACCESS_RELEASE_PATHS", tuple(REPO_ROOT / p for p in EARLIER_RECORDS)),
            patch.object(s3_store, "SOURCE_RELEASE_PATHS", {}, create=True),
        ):
            execute(plan, entry, root / "held", downloads, True, s3, OUTPUTS)
        require(not s3.objects, "Held source reached S3")

    scenario("hold_refuses_storage_when_the_release_record_is_not_listed", hold_enforced, "on access hold")


def staging_scenarios(scenario: Any, plan: dict, entry: dict, root: Path, client: FakeS3) -> None:
    """A Downloads copy with no origin, another origin, other bytes or no file is refused before any write."""

    def prepared(name: str, body: bytes | None, origins: list[str] | None) -> Path:
        folder = root / name
        folder.mkdir()
        if body is not None:
            path = folder / entry["file_name"]
            path.write_bytes(body)
            if origins is not None:
                set_origin(path, origins)
        return folder

    body = BODIES[entry["file_name"]]
    cases = (
        ("file_missing_refused", prepared("no_file", None, None), "missing or not a regular file"),
        ("origin_missing_refused", prepared("no_origin", body, None), "origin missing"),
        ("origin_differs_refused", prepared("other_origin", body, ["https://mirror.example.org/x.xlsx"]), "origin differs"),
        ("changed_bytes_refused", prepared("changed", body + b"edited", [entry["origin_url"]]), "differs from the plan"),
    )
    for name, folder, error in cases:
        target = root / f"run_{name}"
        scenario(name, refusing(lambda f=folder, t=target: execute(plan, entry, t, f, True, client, OUTPUTS), target), error)
    scenario(
        "entry_outside_plan_rejected",
        lambda: execute(plan, entry | {"vintage": "2099"}, root / "outside", root / "downloads", False, client, OUTPUTS),
        "plan or file",
    )


def corrupt_scenarios(scenario: Any, plan: dict, receipt_path: Path) -> None:
    """Replay refuses tampered files, lineage and governance, even when hashes are resealed."""
    collection = receipt_path.parents[3]

    def corrupt(kind: str) -> None:
        target_root = collection.parent / f"corrupt_{kind}"
        shutil.copytree(collection, target_root)
        target = target_root / receipt_path.relative_to(collection).parent
        receipt = read_json(target / "receipt.json")
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        raw = next((target / "raw").iterdir())
        if kind == "resealed_file_changed":
            raw.chmod(0o644)
            raw.write_bytes(raw.read_bytes() + b"x")
            receipt["artifacts"][0]["sha256"], receipt["artifacts"][0]["byte_count"] = fingerprint(raw)
        elif kind == "model_promotion":
            lineage["model_eligible"] = True
        elif kind == "unreviewed_code":
            lineage["code_sha256"] = {"bad": "0" * 64}
        elif kind == "wrong_plan":
            lineage["plan_sha256"] = "0" * 64
        elif kind == "release_binding_removed":
            lineage.pop("access_release_sha256")
        elif kind == "release_binding_other_record":
            lineage["access_release_sha256"] = contract.digest((REPO_ROOT / "config/acquisition/access_releases_20261002.json").read_bytes())
        elif kind == "terms_record_removed":
            lineage.pop("terms_record", None)
        elif kind == "terms_record_unknown_key":
            lineage["terms_record"] = {**lineage.get("terms_record", {}), "licence_granted": True}
        elif kind == "terms_record_changed":
            lineage["terms_record"] = {**lineage.get("terms_record", {}), "reuse_license": "public domain"}
        elif kind == "artifact_role_changed":
            receipt["artifacts"][0]["role"] = "methodology"
        elif kind == "pii_governance":
            receipt["governance"]["contains_pii"] = True
        elif kind == "other_origin":
            receipt["acquisition"]["requested_url"] = "https://example.org/x.xlsx"
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
        (target / "receipt.json").chmod(0o644)
        (target / "receipt.json").write_bytes(encoded_json(receipt))
        verify_local(target / "receipt.json")

    for kind, error in [
        ("resealed_file_changed", "differ"),
        ("model_promotion", "hold differs"),
        ("unreviewed_code", "Unreviewed AHRQ"),
        ("wrong_plan", "plan differs"),
        ("release_binding_removed", "release binding differs"),
        ("release_binding_other_record", "release binding differs"),
        ("pii_governance", "governance differs"),
        ("terms_record_removed", "terms record differs"),
        ("terms_record_unknown_key", "terms record differs"),
        ("terms_record_changed", "terms record differs"),
        ("artifact_role_changed", "artifact set differs"),
        ("other_origin", "route differs"),
    ]:
        scenario(kind, lambda kind=kind: corrupt(kind), error)


def storage_scenarios(scenario: Any, plan: dict, entry: dict, root: Path, run: Path, downloads: Path) -> None:
    """Interrupted uploads resume without duplicates; resealed storage evidence is refused."""

    def interrupted() -> None:
        partial, s3 = root / "interrupted", FakeS3()
        s3.corrupt = True
        with suppress(ValueError):
            execute(plan, entry, partial, downloads, True, s3, OUTPUTS)
        require(len(s3.objects) == 1, "No interrupted upload")
        s3.corrupt = False
        execute(plan, entry, partial, downloads, True, s3, OUTPUTS)
        first = copy.deepcopy(s3.objects)
        execute(plan, entry, partial, downloads, True, s3, OUTPUTS)
        require(first == s3.objects, "Resume duplicated effects")

    scenario("resume_interrupted_storage", interrupted)

    def invalid_version() -> None:
        target = root / "bad_storage"
        shutil.copytree(run, target)
        branch = target / "files" / entry["id"]
        path = branch / "capture/s3_collections_reconciliation.json"
        record = read_json(path)
        record["objects"][0]["object"]["version_id"] = ""
        path.chmod(0o644)
        path.write_bytes(encoded_json(record))
        done = read_json(branch / "completed.json")
        done["reconciliation_sha256"] = fingerprint(path)[0]
        (branch / "completed.json").chmod(0o644)
        (branch / "completed.json").write_bytes(encoded_json(done))
        execute(plan, entry, target, downloads, True, FakeS3(), OUTPUTS)

    scenario("resealed_missing_storage_version", invalid_version, "version missing")


def sha_of(relative: str) -> str:
    return contract.digest((REPO_ROOT / relative).read_bytes())


def binding_scenarios(scenario: Any, plan: dict, root: Path) -> None:
    """Each held source binds its own release record: a shared-record source keeps the newest shared record, AHRQ binds its own, neither the other's."""
    october, newest, ahrq = sha_of(SHARED_RECORDS[1]), sha_of(SHARED_RECORDS[-1]), sha_of(contract.RELEASE_PATH)
    hcai = {"source_id": "HCAI_UTIL", "preferred_route": "access_hold"}
    held_ahrq = {"source_id": "AHRQ_SYSTEM", "preferred_route": "access_hold"}
    request = {"release_binding": "access_release"}

    def check(source: dict, expected: str) -> Any:
        return lambda: require(history_routes.lineage_bindings(source, request) == {"access_release_sha256": expected}, "Binding differs")

    scenario("hcai_reference_binds_the_newest_shared_record", check(hcai, newest))
    scenario("ahrq_capture_binds_the_ahrq_record", check(held_ahrq, ahrq))
    scenario("ahrq_plan_binds_the_record_its_source_selects", lambda: require(plan["access_release"]["sha256"] == ahrq, "Plan record differs"))

    def reference_contract() -> None:
        bound = reference_download_contract.bindings_for(request, hcai)
        require(bound == {"access_release_sha256": newest}, f"Reference receipts would meet {bound}")

    scenario("existing_reference_receipt_binding_still_expected", reference_contract)

    def releases(source_id: str, digest: str) -> bool:
        return s3_store.released_by_record({"source_id": source_id}, {"access_release_sha256": digest})

    scenario("october_record_still_releases_hcai", lambda: require(releases("HCAI_UTIL", october), "October record no longer releases HCAI"))
    scenario("ahrq_record_releases_ahrq", lambda: require(releases("AHRQ_SYSTEM", ahrq), "AHRQ record does not release AHRQ"))
    scenario("non_ahrq_source_cannot_bind_the_ahrq_record", lambda: require(not releases("HCAI_UTIL", ahrq), "HCAI bound the AHRQ record"))
    scenario("another_held_source_cannot_bind_the_ahrq_record", lambda: require(not releases("HUD", ahrq), "HUD bound the AHRQ record"))
    scenario("ahrq_cannot_bind_the_october_record", lambda: require(not releases("AHRQ_SYSTEM", october), "AHRQ bound the October record"))

    def record_for(name: str, sources: list[str]) -> tuple[Path, str]:
        path = root / f"{name}.json"
        path.write_text(json.dumps({"releases": [{"source_id": source, "released": True} for source in sources]}))
        return path, contract.digest(path.read_bytes())

    def shared_record_naming_ahrq() -> None:
        path, digest = record_for("shared_names_ahrq", ["HCAI_UTIL", "AHRQ_SYSTEM"])
        with patch.object(s3_store, "ACCESS_RELEASE_PATHS", (path,)):
            require(releases("HCAI_UTIL", digest) and not releases("AHRQ_SYSTEM", digest), "AHRQ reached a shared record")

    scenario("ahrq_uses_only_its_own_record_even_when_a_shared_record_names_it", shared_record_naming_ahrq)

    def own_record_naming_hcai() -> None:
        path, digest = record_for("own_names_hcai", ["HCAI_UTIL", "AHRQ_SYSTEM"])
        with patch.object(s3_store, "SOURCE_RELEASE_PATHS", {"AHRQ_SYSTEM": path}, create=True):
            require(releases("AHRQ_SYSTEM", digest) and not releases("HCAI_UTIL", digest), "HCAI reached the AHRQ source record")

    scenario("another_source_cannot_use_the_ahrq_record_even_when_it_names_that_source", own_record_naming_hcai)

    def record_naming_another_source() -> None:
        path, digest = record_for("names_hud_only", ["HUD"])
        with patch.object(s3_store, "ACCESS_RELEASE_PATHS", (path,)):
            require(not releases("HCAI_UTIL", digest) and releases("HUD", digest), "A record released a source it does not name")

    scenario("record_naming_another_source_does_not_release_this_one", record_naming_another_source)


def tree(folder: Path) -> dict:
    """Every path under a folder, lock files included, with a file fingerprint or a folder marker."""
    return {str(p.relative_to(folder)): fingerprint(p) if p.is_file() else "folder" for p in sorted(folder.rglob("*"))} if folder.exists() else {}


class LogRecorder:
    """Stand in for the store's logger: keep every line, and with a probe note what had been sent to storage when the line was written."""

    def __init__(self, probe: Any = None) -> None:
        self.lines: list[tuple[str, Any]] = []
        self.errors: list[str] = []
        self.probe = probe

    def info(self, message: str, *args: Any) -> None:
        self.lines.append((message, self.probe() if self.probe else None))

    def error(self, message: str, *args: Any) -> None:
        self.errors.append(message)

    def text(self) -> str:
        return "\n".join([m for m, _ in self.lines] + self.errors)


def command(argv: list[str], target: Path, client: FakeS3 | None = None, env: dict[str, str] | None = None, log: LogRecorder | None = None) -> int:
    """Run the real command line against a target folder; only its exit code is returned."""
    with (
        patch.object(sys, "argv", ["store_ahrq_compendium", *argv]),
        patch.object(store, "ROOT", target),
        patch.object(store, "load_configuration", lambda path: (SETTINGS.copy(), None)),
        patch.object(store, "load_policy", lambda: FakeS3().storage_policy, create=True),
        patch.object(store, "runtime", lambda: (client, OUTPUTS)),
        patch.object(store, "LOGGER", log or store.LOGGER),
        patch.dict(os.environ, env or {}),
    ):
        try:
            store.main()
        except SystemExit as stop:
            return int(stop.code or 0)
    return 0


def default_run_scenarios(scenario: Any, plan: dict, data_entry: dict, doc_entry: dict, root: Path, downloads: Path) -> None:
    """The command line without --execute only validates: no lock, staged original, proof, receipt, capture or folder is written."""

    def fresh() -> None:
        target, before = root / "cli_fresh", tree(downloads)
        code = command(["--downloads", str(downloads)], target)
        require(not target.exists(), f"Default run created {sorted(tree(target))}")
        require(code == 0 and tree(downloads) == before, f"Default run differs: exit {code}")

    scenario("default_command_with_valid_files_writes_nothing", fresh)

    def mixed() -> None:
        built = root / "cli_mixed_build"
        execute(plan, data_entry, built, downloads, True, FakeS3(), OUTPUTS)
        execute(plan, doc_entry, built, downloads, False, None, OUTPUTS)
        target = root / "cli_mixed"
        shutil.copytree(built, target, ignore=shutil.ignore_patterns(".collection.lock"))
        before = tree(target)
        code = command(["--downloads", str(downloads)], target)
        require(tree(target) == before, f"Default run wrote {sorted(set(tree(target)) ^ set(before))}")
        require(code == 0, f"Default run held a file: exit {code}")

    scenario("default_command_over_stored_captured_and_new_files_writes_nothing", mixed)

    def bad_download() -> None:
        bad, target = root / "cli_bad_downloads", root / "cli_bad"
        downloads_for(bad)
        path = bad / data_entry["file_name"]
        path.write_bytes(path.read_bytes() + b"edited")
        code = command(["--downloads", str(bad)], target)
        require(code == 1 and not target.exists(), f"Default run did not hold the edited file untouched: exit {code}, {sorted(tree(target))}")

    scenario("default_command_holds_an_edited_file_and_writes_nothing", bad_download)

    def execute_flag() -> None:
        target, s3 = root / "cli_execute", FakeS3()
        code = command(["--downloads", str(downloads), "--execute"], target, s3)
        stored = sorted((target / "files").glob("*/completed.json"))
        require(code == 0 and len(stored) == len(plan["files"]) and bool(s3.objects), f"--execute stored {len(stored)} of {len(plan['files'])}: exit {code}")

    scenario("execute_flag_captures_and_stores", execute_flag)


def single_copy_client(objects: dict | None = None) -> FakeS3:
    client = FakeS3()
    client.storage_policy = {**ENABLED, "objects": objects or {}}
    return client


def reseal_reconciliation(target: Path, entry: dict, edit: Any) -> None:
    """Edit the stored reconciliation and re-seal the completion record, so only the storage check can refuse the change."""
    branch = target / "files" / entry["id"]
    path = branch / "capture/s3_collections_reconciliation.json"
    reconciliation = read_json(path)
    edit(reconciliation)
    path.chmod(0o644)
    path.write_bytes(encoded_json(reconciliation))
    done = read_json(branch / "completed.json")
    done["reconciliation_sha256"] = fingerprint(path)[0]
    (branch / "completed.json").chmod(0o644)
    (branch / "completed.json").write_bytes(encoded_json(done))


def single_copy_scenarios(scenario: Any, plan: dict, entry: dict, root: Path, downloads: Path) -> None:
    """Storage verification follows the policy the writer selected: collection keys when off, content keys or bootstrap keys when on."""

    def done(target: Path) -> bool:
        return (target / "files" / entry["id"] / "completed.json").is_file()

    def content_keys() -> None:
        target, s3 = root / "single_copy", single_copy_client()
        result = execute(plan, entry, target, downloads, True, s3, OUTPUTS)
        require(result["status"] == "stored" and done(target), "No completion record")
        keys = sorted(s3.objects)
        require(
            any(k.startswith("bronze_objects/sha256/") for k in keys) and any(k.startswith("bronze_evidence/sha256/") for k in keys), f"Keys differ: {keys}"
        )
        before, objects = inventory(target), copy.deepcopy(s3.objects)
        execute(plan, entry, target, root / "moved", True, s3, OUTPUTS)
        require(before == inventory(target) and objects == s3.objects, "Replay wrote")

    scenario("single_copy_on_stores_content_keys_and_completes", content_keys)

    def bootstrap_setup() -> tuple[FakeS3, str, str]:
        body = BODIES[entry["file_name"]]
        sha = hashlib.sha256(body).hexdigest()
        key = f"ahrq/health_system_compendium/datasets/ahrq_system/release_date=2018-01-01/{sha}/{entry['file_name']}"
        record = {"bucket": SETTINGS["data_bucket_name"], "key": key, "version_id": "version_boot", "sha256": sha, "byte_count": len(body)}
        s3 = single_copy_client({sha: record})
        checksum = base64.b64encode(hashlib.sha256(body).digest()).decode()
        s3.objects[key] = {"VersionId": "version_boot", "ContentLength": len(body), "ChecksumSHA256": checksum, "ServerSideEncryption": "AES256", "body": body}
        return s3, sha, key

    def bootstrap_reuse() -> None:
        target, (s3, sha, key) = root / "bootstrap_reuse", bootstrap_setup()
        result = execute(plan, entry, target, downloads, True, s3, OUTPUTS)
        reconciliation = read_json(target / "files" / entry["id"] / "capture/s3_collections_reconciliation.json")
        keys = [e["object"]["key"] for e in reconciliation["objects"] if e["object"]["sha256"] == sha]
        require(result["status"] == "stored" and keys == [key], f"Bootstrap object not reused: {keys}")
        require(f"bronze_objects/sha256/{sha}" not in s3.objects, "Reused content written again")
        execute(plan, entry, target, root / "moved", True, s3, OUTPUTS)

    scenario("single_copy_on_bootstrap_reuse_completes", bootstrap_reuse)

    def bootstrap_other_version() -> None:
        target, (s3, sha, _) = root / "bootstrap_other_version", bootstrap_setup()
        execute(plan, entry, target, downloads, True, s3, OUTPUTS)

        def edit(reconciliation: dict) -> None:
            next(e["object"] for e in reconciliation["objects"] if e["object"]["sha256"] == sha)["version_id"] = "version_other"

        reseal_reconciliation(target, entry, edit)
        execute(plan, entry, target, root / "moved", True, s3, OUTPUTS)

    scenario("resealed_bootstrap_object_with_other_version_refused", bootstrap_other_version, "storage key differs")

    def retry_after_partial_upload() -> None:
        target, s3 = root / "single_copy_partial", single_copy_client()
        s3.corrupt = True
        with suppress(ValueError):
            execute(plan, entry, target, downloads, True, s3, OUTPUTS)
        require(len(s3.objects) == 1 and not done(target), "No interrupted upload")
        s3.corrupt = False
        execute(plan, entry, target, downloads, True, s3, OUTPUTS)
        first = copy.deepcopy(s3.objects)
        execute(plan, entry, target, downloads, True, s3, OUTPUTS)
        require(first == s3.objects and done(target), "Resume duplicated effects or did not complete")

    scenario("single_copy_on_retry_after_partial_upload_completes", retry_after_partial_upload)

    def policy_changed() -> None:
        target = root / "single_copy_policy_changed"
        execute(plan, entry, target, downloads, True, single_copy_client(), OUTPUTS)
        execute(plan, entry, target, root / "moved", True, FakeS3(), OUTPUTS)

    scenario("completion_stored_under_single_copy_refused_when_policy_is_off", policy_changed, "storage key differs")

    def collection_key() -> None:
        target, s3 = root / "single_copy_forged", single_copy_client()
        execute(plan, entry, target, downloads, True, s3, OUTPUTS)

        def edit(reconciliation: dict) -> None:
            record = next(e["object"] for e in reconciliation["objects"] if e["role"] == "data")
            route = load_routes(load_registry())["AHRQ_SYSTEM"]
            prefix = object_prefix(route, source_folder("AHRQ_SYSTEM"), "data", reconciliation["snapshot_id"], reconciliation["snapshot_id"], False)
            record["key"] = f"{prefix}/{record['sha256']}/{entry['file_name']}"

        reseal_reconciliation(target, entry, edit)
        execute(plan, entry, target, root / "moved", True, s3, OUTPUTS)

    scenario("resealed_collection_key_refused_under_single_copy", collection_key, "storage key differs")

    def evidence_key_for_data() -> None:
        target, s3 = root / "single_copy_foreign", single_copy_client()
        execute(plan, entry, target, downloads, True, s3, OUTPUTS)

        def edit(reconciliation: dict) -> None:
            record = next(e["object"] for e in reconciliation["objects"] if e["role"] == "data")
            record["key"] = f"bronze_evidence/sha256/{record['sha256']}"

        reseal_reconciliation(target, entry, edit)
        execute(plan, entry, target, root / "moved", True, s3, OUTPUTS)

    scenario("resealed_evidence_key_for_data_refused_under_single_copy", evidence_key_for_data, "storage key differs")


# Header rows of the six owner-saved workbooks, read on Nov 19 2026 (header rows only; no data row was read or is kept here).
BASE_HEADERS = [
    "compendium_hospital_id",
    "ccn",
    "hospital_name",
    "hospital_street",
    "hospital_city",
    "hospital_state",
    "hospital_zip",
    "acutehosp_flag",
    "health_sys_id",
    "health_sys_name",
    "health_sys_city",
    "health_sys_state",
]
HOSPITAL_FIELDS = [
    "hos_beds",
    "hos_dsch",
    "hos_res",
    "hos_children",
    "hos_majteach",
    "hos_vmajteach",
    "hos_teachint",
    "hos_highdpp",
    "hos_ucburden",
    "hos_highuc",
    "hos_ownership",
    "hos_net_revenue",
    "hos_total_revenue",
]
WORKBOOK_HEADERS = {
    "2016": BASE_HEADERS,
    "2018": BASE_HEADERS,
    "2020": BASE_HEADERS + HOSPITAL_FIELDS,
    "2021": BASE_HEADERS + HOSPITAL_FIELDS,
    "2022": BASE_HEADERS + HOSPITAL_FIELDS,
    "2023": BASE_HEADERS + ["corp_parent_id", "corp_parent_name", "corp_parent_type"] + HOSPITAL_FIELDS,
}
# Words of a column that names a person or a way to reach one; a header with such a word needs a person policy before a bronze load.
PERSON_WORDS = re.compile(r"first_?name|last_?name|surname|person|physician|administrator|executive|ceo|contact|phone|fax|e_?mail|ssn|birth", re.IGNORECASE)


def adapted_scenarios(scenario: Any, plan: dict, data_entry: dict, doc_entry: dict, root: Path, downloads: Path, receipt_path: Path) -> None:
    """What the testing branch changed: per-source records with reviewed cover, capture roots, a prefix guard, and the person-data review of the headers."""
    record = REPO_ROOT / contract.RELEASE_PATH

    def record_is_separate_and_covered() -> None:
        require(record not in s3_store.ACCESS_RELEASE_PATHS and record.name not in s3_store.RELEASE_COVERS, "The AHRQ record joined the shared lists")
        require(
            s3_store.release_paths("AHRQ_SYSTEM") == (record,) and s3_store.release_paths("HCAI_UTIL") == s3_store.ACCESS_RELEASE_PATHS, "Selection differs"
        )
        require({record.name: frozenset({"reference_documents", "whole_workbooks"})} == s3_store.SOURCE_RELEASE_COVERS, "Reviewed cover differs")
        scope = next(r for r in read_json(record)["releases"] if r["source_id"] == "AHRQ_SYSTEM")["scope"].lower()
        require("hospital linkage workbooks" in scope and "technical documents" in scope, "The record scope text does not name both kinds of file")

    scenario("ahrq_record_is_outside_the_shared_lists_and_its_cover_is_reviewed", record_is_separate_and_covered)

    def refused_without_cover(kinds: frozenset, entry: dict) -> Any:
        def run() -> None:
            target = root / f"cover_{entry['role']}_{len(kinds)}"
            execute(plan, entry, target, downloads, False, None, {})
            with patch.object(contract, "SOURCE_RELEASE_COVERS", {record.name: kinds}):
                verify_local(target / "files" / entry["id"] / "capture/receipt.json")

        return run

    scenario(
        "workbook_receipt_refused_when_the_record_does_not_cover_whole_workbooks",
        refused_without_cover(frozenset({"reference_documents"}), data_entry),
        "does not cover",
    )
    scenario(
        "document_receipt_refused_when_the_record_does_not_cover_reference_documents",
        refused_without_cover(frozenset({"whole_workbooks"}), doc_entry),
        "does not cover",
    )
    scenario("receipt_valid_with_the_reviewed_cover", lambda: verify_local(receipt_path))

    def full_run(target: Path, s3: FakeS3, source: Path) -> tuple[list[dict], list[dict]]:
        return run_all(plan, target, source, True, s3, OUTPUTS)

    def same_receipts_second_run() -> None:
        target, s3 = root / "all_twice", FakeS3()
        first, held = full_run(target, s3, downloads)
        require(len(first) == 5 and not held and all(r["status"] == "stored" for r in first), f"First run differs: {held}")
        before, objects = inventory(target), copy.deepcopy(s3.objects)
        second, held_again = full_run(target, s3, root / "moved")
        require(second == first and not held_again, "A second run reported other receipts")
        require(before == inventory(target) and objects == s3.objects and len(s3.objects) == len(objects), "A second run stored something new")

    scenario("second_full_run_stores_nothing_new_and_reports_the_same_receipts", same_receipts_second_run)

    def unlocked_hash() -> None:
        changed = root / "unlocked_downloads"
        downloads_for(changed)
        name = "synthetic-linkage-2023.xlsx"
        (changed / name).write_bytes(synthetic_workbook("not the locked bytes"))
        set_origin(changed / name, [BASE + name, REFERRER])
        s3 = FakeS3()
        results, held = full_run(root / "unlocked", s3, changed)
        require([h["file_name"] for h in held] == [name], f"Held set differs: {held}")
        require(held[0]["reason"] == "AHRQ download differs from the plan", f"Reason differs: {held[0]['reason']}")
        require(len(results) == 4 and not any(name in key for key in s3.objects), "The file with an unlocked hash reached storage")

    scenario("file_whose_hash_is_not_in_the_lock_is_held_by_name_and_never_stored", unlocked_hash)

    def puts(s3: FakeS3) -> list[str]:
        return [args[args.index("--key") + 1] for operation, args in s3.calls if operation == "put-object"]

    def every_put_inside_prefix() -> None:
        s3 = FakeS3()
        full_run(root / "prefix_all", s3, downloads)
        keys = puts(s3)
        require(bool(keys) and all(key.startswith(f"{capture_root()}{contract.PUBLISHER_PREFIX}") for key in keys), f"A key left the collection prefix: {keys}")
        single = single_copy_client()
        full_run(root / "prefix_single", single, downloads)
        content = puts(single)
        inside = f"{capture_root()}{contract.PUBLISHER_PREFIX}"
        require(
            any(CONTENT.fullmatch(key) for key in content) and all(CONTENT.fullmatch(key) or key.startswith(inside) for key in content),
            f"Keys differ: {content}",
        )

    scenario("every_object_put_stays_inside_the_collection_prefix", every_put_inside_prefix)

    def guard_refuses() -> None:
        s3 = FakeS3()
        guard = store.PrefixGuard(s3)
        body = root / "guard_body"
        body.write_bytes(b"x")
        base = ["--if-none-match", "*", "--body", str(body), "--checksum-sha256", base64.b64encode(hashlib.sha256(b"x").digest()).decode()]
        own = f"{contract.PUBLISHER_PREFIX}datasets/ahrq_system/capture_id=x/{'0' * 64}/file.xlsx"
        guard.call("s3api", "put-object", ["--key", own, *base])
        require(list(s3.objects) == [own], "A key under the collection prefix was refused")
        for operation, key in (
            ("put-object", "cms/hospitals/datasets/x/y"),
            ("put-object", "../ahrq/health_system_compendium/x"),
            ("delete-object", own),
            ("copy-object", own),
        ):
            try:
                guard.call("s3api", operation, ["--key", key, *base])
            except ValueError as failure:
                require("outside its collection prefix" in str(failure), f"Reason differs: {failure}")
            else:
                raise AssertionError(f"{operation} {key} was not refused")
        require(list(s3.objects) == [own], "A refused write reached storage")

    scenario("guard_refuses_a_write_outside_the_prefix_and_any_delete_before_it_is_sent", guard_refuses)

    def bounded_guard() -> None:
        class Recorder:
            storage_policy = {"enabled": False}

            def __init__(self) -> None:
                self.sent: list[str] = []

            def call_bounded(self, service: str, operation: str, arguments: list[str] | None = None, *, timeout: float) -> dict:
                self.sent.append(operation)
                return {}

        recorder = Recorder()
        guard = store.PrefixGuard(cast(Any, recorder))
        for operation in ("delete-object", "delete-objects", "put-bucket-policy"):
            try:
                guard.call_bounded("s3api", operation, ["--key", f"{contract.PUBLISHER_PREFIX}x"], timeout=5.0)
            except ValueError as failure:
                require("outside its collection prefix" in str(failure), f"Reason differs: {failure}")
            else:
                raise AssertionError(f"{operation} was not refused")
        guard.call_bounded("s3api", "head-object", ["--key", "any/key"], timeout=5.0)
        require(recorder.sent == ["head-object"], f"The bounded call sent {recorder.sent}")

    scenario("guard_covers_the_bounded_call_path_as_well", bounded_guard)

    elsewhere = FakeS3()

    def layout_pointing_elsewhere() -> None:
        with patch.object(s3_store, "object_prefix", lambda *args: "cms/hospitals/datasets/hospital_general_information/capture_id=x"):
            execute(plan, data_entry, root / "wrong_prefix", downloads, True, elsewhere, OUTPUTS)

    scenario("a_layout_that_points_at_another_collection_is_refused", layout_pointing_elsewhere, "outside its collection prefix")
    scenario("a_layout_that_points_at_another_collection_uploads_nothing", lambda: require(not elsewhere.objects, f"Uploaded: {list(elsewhere.objects)}"))

    def root_stored(capture: str, s3: FakeS3, name: str) -> None:
        with patch.dict(os.environ, {"HAI_CAPTURE_ROOT": capture}):
            target = root / name
            result = execute(plan, data_entry, target, downloads, True, s3, OUTPUTS)
            require(result["status"] == "stored", "Not stored")
            keys = puts(s3)
            require(bool(keys) and all(key.startswith(capture) for key in keys), f"A key left the capture root {capture!r}: {keys}")
            before, objects = inventory(target), copy.deepcopy(s3.objects)
            execute(plan, data_entry, target, root / "moved", True, s3, OUTPUTS)
            require(before == inventory(target) and objects == s3.objects, "Replay under the root wrote")

    test_root = "ingestion_test/ingestion_0a1b2c3d/"
    scenario("capture_under_the_raw_root_stays_under_raw_and_replays", lambda: root_stored("raw/", FakeS3(), "root_raw"))
    scenario("capture_under_a_test_root_stays_under_it_and_replays", lambda: root_stored(test_root, FakeS3(), "root_test"))
    scenario("single_copy_capture_under_the_raw_root_uses_rooted_content_keys", lambda: root_stored("raw/", single_copy_client(), "root_single"))

    def invalid_root() -> None:
        with patch.dict(os.environ, {"HAI_CAPTURE_ROOT": "elsewhere/"}):
            execute(plan, data_entry, root / "root_bad", downloads, True, FakeS3(), OUTPUTS)

    scenario("capture_root_outside_the_two_allowed_forms_is_refused", refusing(invalid_root, root / "root_bad"), "HAI_CAPTURE_ROOT")

    def headers_have_no_person_column() -> None:
        found = {vintage: [h for h in headers if PERSON_WORDS.search(h)] for vintage, headers in WORKBOOK_HEADERS.items()}
        require(not any(found.values()), f"A header names a person or a contact detail: {found}")
        require(all(PERSON_WORDS.search(name) for name in ("administrator_name", "contact_phone")), "The person-word check misses a contact column")
        require(len({tuple(h) for h in WORKBOOK_HEADERS.values()}) == 3 and len(WORKBOOK_HEADERS["2023"]) == 28, "The reviewed header sets changed")

    scenario("reviewed_workbook_headers_hold_no_person_or_contact_column", headers_have_no_person_column)

    def no_person_policy_entry() -> None:
        tables = read_json(REPO_ROOT / "config/lakehouse/person_policies.json")["tables"]
        require("ahrq_hospital_linkage" not in tables, "A person policy exists for the AHRQ table; its reviewed headers hold no person column")

    scenario("ahrq_table_has_no_person_policy_because_its_headers_hold_no_person_column", no_person_policy_entry)


class ReadingFake(FakeS3):
    """A fake store whose bounded call path is answered by the same fake, so no case can reach the real AWS command."""

    def __init__(self) -> None:
        super().__init__()
        self.timeouts: list[float] = []
        self.bounded_failure: str | None = None
        self.damaged: str | None = None
        self.tick: Any = None

    def call_bounded(self, service: str, operation: str, arguments: list[str] | None = None, *, timeout: float) -> dict:
        self.timeouts.append(timeout)
        if self.tick:
            self.tick()
        if self.bounded_failure:
            raise s3_store.StorageError(operation, self.bounded_failure)
        return self.call(service, operation, arguments)


class StepClock:
    """A clock that moves a fixed step at every read, so a deadline case needs no sleeping."""

    def __init__(self, step: float) -> None:
        self.now, self.step = 0.0, step

    def __call__(self) -> float:
        self.now += self.step
        return self.now


def operations(s3: FakeS3) -> set[str]:
    return {operation for operation, _ in s3.calls}


def round2_scenarios(scenario: Any, plan: dict, data_entry: dict, doc_entry: dict, root: Path, downloads: Path) -> None:
    """S3 proof before any local copy goes, an explicit capture root, a confined local folder and the retrospective release basis."""
    verify_s3_scenarios(scenario, plan, root, downloads)
    root_choice_scenarios(scenario, plan, root, downloads)
    confinement_scenarios(scenario, plan, data_entry, root, downloads)
    release_basis_scenarios(scenario, plan, data_entry, root, downloads)


def stored_all(root: Path, name: str, downloads: Path, plan: dict) -> tuple[Path, ReadingFake]:
    """Store every planned synthetic file into a fake store; return the local folder and the store."""
    target, s3 = root / name, ReadingFake()
    results, held = run_all(plan, target, downloads, True, s3, OUTPUTS)
    require(len(results) == len(plan["files"]) and not held, f"Setup did not store every file: {held}")
    return target, s3


def verify_s3_scenarios(scenario: Any, plan: dict, root: Path, downloads: Path) -> None:
    """--verify-s3 reads every recorded version back, fails closed, and only then says the local copies may go."""
    count = len(plan["files"])

    def verified(target: Path, s3: ReadingFake, argv: list[str] | None = None) -> tuple[int, LogRecorder]:
        log = LogRecorder()
        return command(argv or ["--verify-s3"], target, s3, log=log), log

    def after_clearing() -> None:
        target, s3 = stored_all(root, "v_cleared", downloads, plan)
        before_calls = len(s3.calls)
        replay = run_all(plan, target, downloads, True, s3, OUTPUTS)
        require(len(replay[0]) == count and len(s3.calls) == before_calls, "The local replay changed")
        s3.objects.clear()
        code, log = verified(target, s3)
        require(code != 0 and '"status": "verified_on_s3"' not in log.text(), f"Cleared objects were reported as proven: exit {code}: {log.text()[:300]}")
        require("NOT PROVEN" in log.text() and "Do not remove" in log.text(), f"The summary does not warn: {log.text()[-300:]}")
        for file in plan["files"]:
            require(file["file_name"] in log.text(), f"{file['file_name']} is not named")

    scenario("verify_s3_fails_when_every_remote_object_is_gone_though_the_local_record_says_stored", after_clearing)

    def passing() -> None:
        target, s3 = stored_all(root, "v_pass", downloads, plan)
        before = inventory(target)
        s3.calls.clear()
        code, log = verified(target, s3)
        require(code == 0, f"Exit {code}: {log.text()[:400]}")
        lines = [json.loads(m) for m, _ in log.lines if m.startswith("{")]
        proven = [line for line in lines if line.get("status") == "verified_on_s3"]
        require(len(proven) == count and {line["file_name"] for line in proven} == {f["file_name"] for f in plan["files"]}, f"Lines differ: {lines}")
        require(all(line["bucket"] == SETTINGS["data_bucket_name"] and line["objects_verified"] == 4 for line in proven), f"Line fields differ: {proven[0]}")
        summary = log.lines[-1][0]
        require(f"All {count} files are proven on S3" in summary and "may now be removed" in summary, f"Summary differs: {summary}")
        require(operations(s3) <= {"head-object", "get-object"}, f"A non-read call was made: {operations(s3)}")
        require(before == inventory(target), "The verify mode wrote local files")
        require(max(s3.timeouts) <= store.VERIFY_OBJECT_SECONDS and bool(s3.timeouts), "A read was not bounded")

    scenario("verify_s3_proves_every_file_by_reading_the_recorded_versions_and_writes_nothing", passing)

    def damaged(change: Any, error: str, no_download: bool = False) -> Any:
        def run() -> None:
            target, s3 = stored_all(root, f"v_{error[:12].replace(' ', '_')}_{id(change)}", downloads, plan)
            change(s3, target)
            s3.calls.clear()
            results, failed = store.verify_remote(plan, target, s3)
            require(len(failed) >= 1 and all(error in f["reason"] for f in failed), f"Reasons differ: {failed}")
            require(not any(r["file_name"] in {f["file_name"] for f in failed} for r in results), "A failed file was also reported proven")
            if no_download:
                fetched = [args[args.index("--key") + 1] for operation, args in s3.calls if operation == "get-object"]
                require(s3.damaged is not None and s3.damaged not in fetched, "The object was downloaded before the size was checked")

        return run

    def other_version(s3: ReadingFake, target: Path) -> None:
        key = next(k for k in s3.objects if k.endswith(".xlsx") or k.endswith(".xls"))
        s3.objects[key]["VersionId"] = "version_other"

    def other_bytes(s3: ReadingFake, target: Path) -> None:
        key = next(k for k in s3.objects if k.endswith(".xlsx") or k.endswith(".xls"))
        s3.objects[key]["body"] = s3.objects[key]["body"][:-1] + b"X"

    def other_size(s3: ReadingFake, target: Path) -> None:
        key = next(k for k in s3.objects if k.endswith(".xlsx") or k.endswith(".xls"))
        s3.objects[key]["ContentLength"] += 4096
        s3.damaged = key

    def no_receipt(s3: ReadingFake, target: Path) -> None:
        next(target.glob("files/*/capture/receipt.json")).unlink()

    def not_stored(s3: ReadingFake, target: Path) -> None:
        done = next(target.glob("files/*/completed.json"))
        done.unlink()

    scenario("verify_s3_refuses_a_wrong_version", damaged(other_version, "AHRQ S3 version differs"))
    scenario("verify_s3_refuses_changed_bytes", damaged(other_bytes, "Downloaded S3 version differs"))
    scenario("verify_s3_refuses_a_larger_object_before_downloading_it", damaged(other_size, "AHRQ S3 object size differs", no_download=True))
    scenario("verify_s3_refuses_a_missing_receipt", damaged(no_receipt, "AHRQ receipt missing"))
    scenario("verify_s3_refuses_a_file_with_no_completion_record", damaged(not_stored, "AHRQ completion missing"))

    def timeout() -> None:
        target, s3 = stored_all(root, "v_timeout", downloads, plan)
        s3.bounded_failure = "request_unconfirmed"
        results, failed = store.verify_remote(plan, target, s3)
        require(not results and len(failed) == count and all("request_unconfirmed" in f["reason"] for f in failed), f"Timeouts differ: {failed}")
        code, log = verified(target, s3)
        require(code != 0 and "NOT PROVEN" in log.text(), f"A timeout passed: {code}")

    scenario("verify_s3_fails_closed_on_a_read_timeout", timeout)

    def deadline() -> None:
        target, s3 = stored_all(root, "v_deadline", downloads, plan)
        results, failed = store.verify_remote(plan, target, s3, clock=StepClock(store.VERIFY_TOTAL_SECONDS / 6))
        require(len(failed) >= 1 and any("deadline" in f["reason"] for f in failed), f"The whole-run deadline did not stop the run: {failed}")
        require(all(t <= store.VERIFY_OBJECT_SECONDS for t in s3.timeouts), "A read was given more than the per-object limit")

    scenario("verify_s3_stops_at_its_whole_run_deadline", deadline)

    def wrong_partial() -> None:
        target, s3 = stored_all(root, "v_partial", downloads, plan)
        other_bytes(s3, target)
        code, log = verified(target, s3)
        require(code != 0 and "NOT PROVEN" in log.text(), f"One bad file passed: {code}")
        require(any("verified_on_s3" in m for m, _ in log.lines), "The good files were not reported")
        require("may now be removed" not in log.text(), "The summary allows removal with one file unproven")

    scenario("verify_s3_exits_nonzero_when_any_file_is_not_proven_and_never_allows_removal", wrong_partial)

    def exclusive() -> None:
        target, s3 = root / "v_both", ReadingFake()
        code = command(["--verify-s3", "--execute"], target, s3)
        require(code != 0 and not s3.objects and not target.exists(), f"Both modes ran: {code}")

    scenario("verify_s3_and_execute_cannot_be_combined", exclusive)

    def local_wording() -> None:
        target, s3 = stored_all(root, "v_wording", downloads, plan)
        log = LogRecorder()
        require(command(["--downloads", str(downloads), "--execute"], target, s3, log=log) == 0, "The local replay failed")
        text = log.text()
        require("local_record_of_upload" in text and '"stored"' not in text, f"The local result reads as proof: {text[:300]}")
        require("not a check of S3" in log.lines[-1][0] and "--verify-s3" in log.lines[-1][0], f"The summary does not point to --verify-s3: {log.lines[-1][0]}")

    scenario("local_result_is_called_a_local_record_and_points_to_verify_s3", local_wording)

    def cleared_repro() -> None:
        target, s3 = root / "v_repro", ReadingFake()
        first = run_all(plan, target, downloads, True, s3, OUTPUTS)
        s3.objects.clear()
        calls = len(s3.calls)
        again = run_all(plan, target, downloads, True, s3, OUTPUTS)
        require(again == first and len(s3.calls) == calls, "The local path changed")
        require(bool(store.verify_remote(plan, target, s3)[1]), "The remote check accepted cleared objects")

    scenario("reviewer_reproduction_cleared_objects_replay_stays_local_and_verify_catches_it", cleared_repro)


class SetClock:
    """A clock the cases move by hand."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def round3_scenarios(scenario: Any, plan: dict, data_entry: dict, root: Path, downloads: Path) -> None:
    """Every mode excludes every other, a verification needs the full object count, one deadline covers an object, and the root and lock are checked."""
    count = len(plan["files"])
    receipt = root / "never_used_receipt.json"
    modes = {
        "execute": ["--execute"],
        "verify_s3": ["--verify-s3"],
        "validate_receipt": ["--validate-receipt", str(receipt)],
        "build_plan": ["--build-plan"],
    }

    def pair(first: str, second: str) -> Any:
        def run() -> None:
            target, s3 = root / f"m_{first}_{second}", ReadingFake()
            code = command(["--downloads", str(downloads), *modes[first], *modes[second]], target, s3)
            require(code == 2 and not s3.calls and not target.exists(), f"The two modes were not refused as a mode conflict: exit {code}")

        return run

    names = list(modes)
    for i, first in enumerate(names):
        for second in names[i + 1 :]:
            scenario(f"modes_{first}_and_{second}_cannot_be_combined", pair(first, second))

    def verify_with_receipt_on_empty_store() -> None:
        target, s3 = stored_all(root, "m_empty", downloads, plan)
        path = next(target.glob("files/*/capture/receipt.json"))
        empty = ReadingFake()
        code = command(["--verify-s3", "--validate-receipt", str(path)], target, empty)
        require(code != 0 and not empty.calls, f"Verify plus validate returned {code} with an empty store")

    scenario("verify_s3_with_validate_receipt_on_an_empty_store_is_refused", verify_with_receipt_on_empty_store)

    def count_check() -> None:
        target, s3 = stored_all(root, "m_count", downloads, plan)
        results, failed = store.verify_remote(plan, target, s3)
        require(len(results) == count and store.all_proven(plan, results, failed), "The full set was not proven")
        require("NOT PROVEN" in store.verification_summary(plan, results[:-1], []), "A skipped file still read as proven")
        short = [dict(r) for r in results]
        short[0]["objects_verified"] = 3
        require("NOT PROVEN" in store.verification_summary(plan, short, []) and not store.all_proven(plan, short, []), "A short object count read as proven")
        log = LogRecorder()
        with patch.object(store, "verify_remote", lambda *a, **k: (results[:-1], [])):
            code = command(["--verify-s3"], target, s3, log=log)
        require(code != 0 and "may now be removed" not in log.text(), f"A skipped file passed: exit {code}")

    scenario("verify_s3_never_succeeds_when_a_file_or_object_was_not_read_back", count_check)

    def shared_budget() -> None:
        target, s3 = stored_all(root, "d_shared", downloads, plan)
        clock = SetClock()
        s3.tick = lambda: setattr(clock, "now", clock.now + 90)
        results, failed = store.verify_remote(plan, target, s3, clock=clock)
        require(
            not results and bool(failed) and all("deadline" in f["reason"] for f in failed), f"A 90 s HEAD plus a 90 s read passed: {results[:1]} {failed[:1]}"
        )
        require(max(s3.timeouts) <= store.VERIFY_OBJECT_SECONDS and min(s3.timeouts) <= 30, f"The second call got a fresh budget: {s3.timeouts[:4]}")

    scenario("one_object_deadline_covers_the_head_and_the_download", shared_budget)

    def expiry_in_hash() -> None:
        target, s3 = stored_all(root, "d_hash", downloads, plan)
        clock, original, seen = SetClock(), store.verify_version, []
        last = count * store.OBJECTS_PER_FILE

        def slow(client: Any, key: str, version: Any, digest: str, size: int) -> None:
            original(client, key, version, digest, size)
            seen.append(key)
            if len(seen) == last:
                clock.now = store.VERIFY_TOTAL_SECONDS + 1

        with patch.object(store, "verify_version", slow):
            results, failed = store.verify_remote(plan, target, s3, clock=clock)
        require(len(seen) == last, f"The expiry was not placed in the last hash: {len(seen)} of {last} objects read")
        require(
            len(results) == count - 1 and len(failed) == 1 and "deadline" in failed[0]["reason"],
            f"Expiry during the last hash passed: {len(results)} proven, {failed}",
        )
        require(not store.all_proven(plan, results, failed), "An expired run read as proven")

    scenario("deadline_that_passes_while_hashing_stops_the_file_from_counting", expiry_in_hash)

    def run_budget_from_the_start() -> None:
        target, s3 = stored_all(root, "d_start", downloads, plan)
        clock = SetClock()
        clock.now = store.VERIFY_TOTAL_SECONDS + 5
        results, failed = store.verify_remote(plan, target, s3, clock=clock, started=0.0)
        require(not results and len(failed) == count, "Time spent before the first read did not count against the run")

    scenario("run_deadline_counts_from_the_start_of_the_command", run_budget_from_the_start)

    def linked_root() -> None:
        target, outside = root / "k_root", root / "k_root_outside"
        outside.mkdir(parents=True)
        target.symlink_to(outside, target_is_directory=True)
        run_all(plan, target, downloads, True, ReadingFake(), OUTPUTS)

    scenario("owned_root_that_is_a_symlink_is_refused", linked_root, "outside the tool's own folder")
    scenario(
        "owned_root_symlink_leaves_the_target_untouched",
        lambda: require(not any((root / "k_root_outside").rglob("*")), "Files were written through the root link"),
    )

    def linked_lock() -> None:
        target, elsewhere = root / "k_lock", root / "k_lock_elsewhere.lock"
        target.mkdir()
        (target / ".collection.lock").symlink_to(elsewhere)
        run_all(plan, target, downloads, True, ReadingFake(), OUTPUTS)

    scenario("collection_lock_that_is_a_symlink_is_refused", linked_lock, "outside the tool's own folder")
    scenario(
        "collection_lock_symlink_creates_nothing_outside",
        lambda: require(not (root / "k_lock_elsewhere.lock").exists(), "The lock was created through the link"),
    )


def root_choice_scenarios(scenario: Any, plan: dict, root: Path, downloads: Path) -> None:
    """An inherited HAI_CAPTURE_ROOT never picks the destination: the command line chooses it, and the full destination is printed before any upload."""
    test_root = "ingestion_test/ingestion_0a1b2c3d/"
    base = ["--downloads", str(downloads), "--execute"]

    def refused(name: str, argv: list[str], env: dict[str, str]) -> Any:
        def run() -> None:
            target, s3, log = root / name, ReadingFake(), LogRecorder()
            code = command(argv, target, s3, env=env, log=log)
            require(code != 0 and not s3.objects and not any(op == "put-object" for op, _ in s3.calls), f"Uploaded under an unchosen root: exit {code}")
            require(not target.exists(), "A local effect happened before the refusal")
            require("HAI_CAPTURE_ROOT" in log.text() and test_root in log.text(), f"The refusal does not name the variable and root: {log.text()[:300]}")

        return run

    scenario("inherited_test_root_without_a_command_line_choice_is_refused_before_any_effect", refused("r_inherit", base, {"HAI_CAPTURE_ROOT": test_root}))
    scenario(
        "inherited_root_different_from_the_command_line_choice_is_refused",
        refused("r_differ", [*base, "--capture-root", "raw/"], {"HAI_CAPTURE_ROOT": test_root}),
    )
    scenario("inherited_root_is_refused_even_for_the_read_only_run", refused("r_dry", ["--downloads", str(downloads)], {"HAI_CAPTURE_ROOT": test_root}))

    def puts_of(s3: FakeS3) -> list[str]:
        return [args[args.index("--key") + 1] for operation, args in s3.calls if operation == "put-object"]

    def chosen(env: dict[str, str]) -> Any:
        def run() -> None:
            target, s3 = root / f"r_chosen_{len(env)}", ReadingFake()
            log = LogRecorder(probe=lambda: len(puts_of(s3)))
            code = command([*base, "--capture-root", test_root], target, s3, env=env, log=log)
            require(code == 0, f"The explicit choice was refused: {code}: {log.text()[:300]}")
            keys = puts_of(s3)
            require(bool(keys) and all(k.startswith(test_root) for k in keys), f"A key left the chosen root: {keys[:3]}")
            printed = [json.loads(m)["destination"] for m, _ in log.lines if m.startswith('{"destination"')]
            require(len(printed) == 1, "The destination line was not printed exactly once")
            destination = printed[0]
            require(
                destination["bucket"] == SETTINGS["data_bucket_name"] and destination["key_prefix"] == f"{test_root}ahrq/health_system_compendium/",
                f"Destination differs: {destination}",
            )
            first = next(i for i, (m, _) in enumerate(log.lines) if m.startswith('{"destination"'))
            require(log.lines[first][1] == 0, "The destination was printed after an upload had started")

        return run

    scenario("explicit_test_root_stores_under_it_and_prints_the_destination_before_the_first_upload", chosen({}))
    scenario("explicit_root_that_equals_the_environment_is_accepted", chosen({"HAI_CAPTURE_ROOT": test_root}))

    def default_destination() -> None:
        target, s3 = root / "r_default", ReadingFake()
        log = LogRecorder()
        require(command(base, target, s3, log=log) == 0, f"The default root was refused: {log.text()[:300]}")
        destination = next(json.loads(m)["destination"] for m, _ in log.lines if m.startswith('{"destination"'))
        require(
            destination["key_prefix"] == "ahrq/health_system_compendium/" and destination["capture_root"].startswith("(none"),
            f"Destination differs: {destination}",
        )
        require(all(k.startswith("ahrq/health_system_compendium/") for k in puts_of(s3)), "A default-root key left the prefix")

    scenario("default_root_needs_no_choice_and_the_destination_names_the_bucket_and_prefix", default_destination)


def confinement_scenarios(scenario: Any, plan: dict, data_entry: dict, root: Path, downloads: Path) -> None:
    """A symbolic link inside the tool's own folder never lets a write land outside it."""

    def escape(name: str) -> Any:
        def run() -> None:
            target, outside, s3 = root / f"c_{name}", root / f"c_{name}_outside", ReadingFake()
            outside.mkdir(parents=True)
            branch = target / "files" / data_entry["id"]
            location = {"files": target / "files", "id": branch, "source": branch / "source", "capture": branch / "capture"}[name]
            location.parent.mkdir(parents=True, exist_ok=True)
            location.symlink_to(outside, target_is_directory=True)
            execute(plan, data_entry, target, downloads, True, s3, OUTPUTS)

        return run

    def nothing_outside(name: str) -> Any:
        def run() -> None:
            outside = root / f"c_{name}_outside"
            require(not any(outside.rglob("*")), f"Files were written outside the tool's folder: {sorted(p.name for p in outside.rglob('*'))}")

        return run

    for name in ("files", "id", "source", "capture"):
        scenario(f"symlinked_{name}_folder_is_refused", escape(name), "outside the tool's own folder")
        scenario(f"symlinked_{name}_folder_leaves_the_target_untouched", nothing_outside(name))

    def symlinked_completion() -> None:
        target, s3 = root / "c_done_link", ReadingFake()
        execute(plan, data_entry, target, downloads, False, s3, OUTPUTS)
        done = target / "files" / data_entry["id"] / "completed.json"
        done.symlink_to(root / "c_done_link_elsewhere.json")
        execute(plan, data_entry, target, downloads, True, s3, OUTPUTS)

    scenario("symlinked_completion_record_is_refused", symlinked_completion, "outside the tool's own folder")


def release_basis_scenarios(scenario: Any, plan: dict, data_entry: dict, root: Path, downloads: Path) -> None:
    """A capture that precedes the release record is stored on an explicit, checked basis; one that follows it says so."""
    early = "2026-11-08T20:45:00+00:00"

    def retrospective() -> None:
        target = root / "b_early"
        with patch.object(store.download_metadata, "download_created_at", lambda path: early):
            execute(plan, data_entry, target, downloads, False, None, OUTPUTS)
        receipt = verify_local(target / "files" / data_entry["id"] / "capture/receipt.json")
        basis = json.loads(receipt["lineage"]["extraction_or_query"])["release_basis"]
        recorded = read_json(REPO_ROOT / contract.RELEASE_PATH)["recorded_at_utc"]
        require(basis["relation"] == "capture_precedes_release_record", f"Relation differs: {basis}")
        require(basis["capture_time_utc"] == early and basis["release_record_time_utc"] == recorded, f"Times differ: {basis}")
        require(
            "after the fact" in basis["authorization"] and "exact" in basis["authorization"], f"The basis does not say it authorizes after the fact: {basis}"
        )
        require(any("precedes the release record" in w for w in receipt["quality_profile"]["warnings"]), "The receipt warnings do not state the order")

    scenario("capture_before_the_release_record_states_the_retrospective_basis_with_both_times", retrospective)

    def contemporaneous() -> None:
        target = root / "b_late"
        with patch.object(store.download_metadata, "download_created_at", lambda path: "2026-11-19T12:00:00+00:00"):
            execute(plan, data_entry, target, downloads, False, None, OUTPUTS)
        receipt = verify_local(target / "files" / data_entry["id"] / "capture/receipt.json")
        basis = json.loads(receipt["lineage"]["extraction_or_query"])["release_basis"]
        require(basis["relation"] == "capture_follows_release_record", f"Relation differs: {basis}")
        require(
            not any("precedes the release record" in w for w in receipt["quality_profile"]["warnings"]), "A later capture carries the retrospective warning"
        )

    scenario("capture_after_the_release_record_says_it_follows_it", contemporaneous)

    def tampered(change: Any) -> Any:
        def run() -> None:
            target = root / f"b_tamper_{id(change)}"
            with patch.object(store.download_metadata, "download_created_at", lambda path: early):
                execute(plan, data_entry, target, downloads, False, None, OUTPUTS)
            path = target / "files" / data_entry["id"] / "capture/receipt.json"
            receipt = read_json(path)
            lineage = json.loads(receipt["lineage"]["extraction_or_query"])
            change(lineage)
            receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
            path.chmod(0o644)
            path.write_bytes(encoded_json(receipt))
            verify_local(path)

        return run

    scenario("receipt_without_the_release_basis_is_refused", tampered(lambda lineage: lineage.pop("release_basis")), "release basis differs")
    scenario(
        "receipt_that_calls_an_earlier_capture_later_is_refused",
        tampered(lambda lineage: lineage["release_basis"].update(relation="capture_follows_release_record")),
        "release basis differs",
    )
    scenario(
        "receipt_with_another_record_time_is_refused",
        tampered(lambda lineage: lineage["release_basis"].update(release_record_time_utc="2026-11-01T00:00:00+00:00")),
        "release basis differs",
    )


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="ahrq_compendium_e2e_") as directory, download_metadata():
        scenarios = exercise(Path(directory))
    passed = all(s["passed"] for s in scenarios)
    report = {
        "status": "passed" if passed else "failed",
        "feature": "AHRQ Compendium linkage files saved by the owner: bound by plan, held source released by record, versioned S3 storage",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "code_sha256": closure_hashes("scripts.acquisition.run_ahrq_compendium_e2e"),
        "dependency_sha256": hashlib.sha256(Path("uv.lock").read_bytes()).hexdigest(),
        "reproduce": f".venv/bin/python -m scripts.acquisition.run_ahrq_compendium_e2e --output {args.output}",
        "boundary": "Synthetic files with real macOS origins; real registry and release records; fake versioned S3; no AHRQ or AWS access; no real AHRQ file.",
        "cleanup": "Temporary folder removed on exit.",
        "scenarios": scenarios,
    }
    write_once(args.output, encoded_json(report))
    sys.stdout.write(
        json.dumps({"status": report["status"], "passed": sum(s["passed"] for s in scenarios), "total": len(scenarios), "artifact": str(args.output)}) + "\n"
    )
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
