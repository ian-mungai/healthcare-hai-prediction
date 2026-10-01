from __future__ import annotations

import copy
import sys
import zipfile
from pathlib import Path

import pytest

from scripts.acquisition import historical_inventory
from scripts.acquisition import plan_remaining as planning
from scripts.acquisition import s3_store as storage
from scripts.acquisition.archive_review import expand_snapshot
from scripts.acquisition.capture import capture
from scripts.acquisition.source_registry import canonical_hash, read_json
from scripts.acquisition.tests.test_capture import Opener, Response
from scripts.acquisition.tests.test_capture import setup_capture as setup_capture
from scripts.acquisition.tests.test_s3_store import OUTPUTS
from scripts.acquisition.tests.test_s3_store import bundle as bundle
from scripts.acquisition.transport import inspect_payload
from tests.support import check


def inputs(setup: tuple) -> tuple:
    source, _, validator = setup
    source.update(
        planning={"wave": 1},
        required_checks=["Preserve original checks"],
        measurement_history={"advertised": "Some releases", "observed": "A sample", "unresolved": "Full history"},
        validation={"reviewed_access_terms": "Public aggregates", "source_specific_gate": {"status": "not_run"}},
        api_fallback={"needed": False, "reason": "Files available"},
    )
    source["file_routes"][0].update(format="CSV", scope="Example population", remaining_checks=["Preserve strings"], vintage_or_release="FY2022")
    registry = {"sources": [source]}
    rules = {
        "rules_version": 1,
        "privacy_review_sources": [],
        "large_file_review_sources": [],
        "reference_route_ids": [],
        "privacy_review_reason": "Review personal fields",
        "large_file_review_reason": "Review complete size",
        "source_actions": [{"source_ids": [source["source_id"]], "history_action": "Enumerate releases", "extraction_action": "Keep bytes"}],
    }
    routes = {source["source_id"]: {"publisher": "example", "collection": "tables", "layout_sha256": "a" * 64}}
    return registry, historical_inventory.build_inventory(registry), rules, routes, validator


def test_full_pool_rules_cover_all_sources_and_routes(setup_capture: tuple) -> None:
    registry, _, rules, _, _ = inputs(setup_capture)
    check(
        set(planning.strategies(registry, rules)) == {source["source_id"] for source in registry["sources"]},
        'set(planning.strategies(registry, rules)) == {source["source_id"] for source in registry["sources"]}',
    )


def test_planning_preserves_history_and_does_not_execute(setup_capture: tuple) -> None:
    registry, inventory, rules, routes, validator = inputs(setup_capture)
    inventory["sources"][0].pop("documentation")
    old = copy.deepcopy(inventory)
    refreshed, register, batch = planning.build_plans(registry, inventory, rules, routes, [], validator)
    check(inventory == old, "inventory == old")
    check(
        register["counts"]["batch_jobs"] == 1 and register["execution_authorized_by_plan"] is False,
        'register["counts"]["batch_jobs"] == 1 and register["execution_authorized_by_plan"] is False',
    )
    check(batch["jobs"][0]["plan"]["release"]["release_date"] is None, 'batch["jobs"][0]["plan"]["release"]["release_date"] is None')
    check(batch["jobs"][0]["plan"]["measurement_periods"][0]["start_date"] is None, 'batch["jobs"][0]["plan"]["measurement_periods"][0]["start_date"] is None')
    check(
        refreshed["inventory_version"] == 2 and refreshed["counts"]["stored_snapshots"] == 0,
        'refreshed["inventory_version"] == 2 and refreshed["counts"]["stored_snapshots"] == 0',
    )
    check(not refreshed["sources"][0]["history_complete"], 'not refreshed["sources"][0]["history_complete"]')
    check(
        refreshed["sources"][0]["documentation"]["status"] == "release_and_section_mapping_pending",
        'refreshed["sources"][0]["documentation"]["status"] == "release_and_section_mapping_pending"',
    )
    check(
        register["sources"][0]["required_checks"] == registry["sources"][0]["required_checks"],
        'register["sources"][0]["required_checks"] == registry["sources"][0]["required_checks"]',
    )
    check("example_source" in planning.checklist(register), '"example_source" in planning.checklist(register)')


@pytest.mark.parametrize("change", ["version", "missing_source", "duplicate_source", "unknown_source", "unknown_reference"])
def test_invalid_rules_fail_closed(setup_capture: tuple, change: str) -> None:
    registry, _, rules, _, _ = inputs(setup_capture)
    if change == "version":
        rules["rules_version"] = 3
    elif change == "missing_source":
        rules["source_actions"] = []
    elif change == "duplicate_source":
        rules["source_actions"].append(rules["source_actions"][0])
    elif change == "unknown_source":
        rules["privacy_review_sources"] = ["absent"]
    else:
        rules["reference_route_ids"] = ["absent"]
    with pytest.raises(ValueError):
        planning.strategies(registry, rules)


@pytest.mark.parametrize("change", ["hash", "sources", "routes", "layout"])
def test_incomplete_or_stale_inventory_is_rejected(setup_capture: tuple, change: str) -> None:
    registry, inventory, rules, routes, validator = inputs(setup_capture)
    if change == "hash":
        inventory["registry_sha256"] = "0" * 64
    elif change == "sources":
        inventory["sources"] = []
    elif change == "routes":
        inventory["known_routes"] = []
    else:
        routes = {}
    with pytest.raises(ValueError):
        planning.build_plans(registry, inventory, rules, routes, [], validator)


@pytest.mark.parametrize(
    "reason",
    [
        "source_access_hold",
        "publisher_access_unresolved",
        "privacy_and_terms_review_before_capture",
        "complete_size_and_large_file_strategy_required",
        "discovery_index_not_an_artifact",
        "interactive_export_selections_and_artifact_required",
        "html_requires_document_or_report_specific_capture",
        "unimplemented_or_ambiguous_format",
    ],
)
def test_no_executable_capture_for_unresolved_route(setup_capture: tuple, reason: str) -> None:
    registry, _, rules, _, _ = inputs(setup_capture)
    source = registry["sources"][0]
    route = source["file_routes"][0]
    if reason == "source_access_hold":
        source["preferred_route"] = "access_hold"
    elif reason == "publisher_access_unresolved":
        route["status"] = "access_failed_or_empty_response"
    elif reason == "privacy_and_terms_review_before_capture":
        rules["privacy_review_sources"] = [source["source_id"]]
    elif reason == "complete_size_and_large_file_strategy_required":
        rules["large_file_review_sources"] = [source["source_id"]]
    elif reason == "discovery_index_not_an_artifact":
        route["route_type"] = "file_index"
    elif reason == "interactive_export_selections_and_artifact_required":
        route["route_type"] = "permitted_export"
    else:
        route.update(url="https://example.org/report", format="HTML" if "html" in reason else "unknown")
    result = planning.route_plan(source, route, rules, [])
    check(reason in result["blockers"] and result["capture_plan"] is None, 'reason in result["blockers"] and result["capture_plan"] is None')


def test_zip_is_capture_only_not_an_upload_batch_job(setup_capture: tuple) -> None:
    registry, _, rules, routes, validator = inputs(setup_capture)
    registry["sources"][0]["file_routes"][0].update(url="https://example.org/data.zip", format="ZIP")
    _, register, batch = planning.build_plans(registry, historical_inventory.build_inventory(registry), rules, routes, [], validator)
    check(not batch["jobs"], 'not batch["jobs"]')
    check(register["routes"][0]["capture_plan"], 'register["routes"][0]["capture_plan"]')
    check(register["routes"][0]["status"] == "capture_only_pending_member_map", 'register["routes"][0]["status"] == "capture_only_pending_member_map"')
    check(register["routes"][0]["extraction"]["upload_zip"] is False, 'register["routes"][0]["extraction"]["upload_zip"] is False')


@pytest.mark.parametrize("same_source", [True, False])
def test_stored_archive_is_not_redownloaded_or_approved_for_alias(setup_capture: tuple, same_source: bool) -> None:
    registry, _, rules, _, _ = inputs(setup_capture)
    source, route = registry["sources"][0], registry["sources"][0]["file_routes"][0]
    stored = [
        {
            "source_id": source["source_id"] if same_source else "another_source",
            "requested_url": route["url"],
            "snapshot_id": "example_snapshot",
            "release": {},
            "status": "stored_unvalidated",
        }
    ]
    result = planning.route_plan(source, route, rules, stored)
    check(result["capture_plan"] is None and len(result["stored_matches"]) == 1, 'result["capture_plan"] is None and len(result["stored_matches"]) == 1')
    check(
        any("already_stored" in reason if same_source else "source_scope" in reason for reason in result["blockers"]),
        'any("already_stored" in reason if same_source else "source_scope" in reason for reason in result["blockers"])',
    )


@pytest.mark.parametrize(
    "url,format_name", [("https://example.org/file.dat", "txt"), ("https://example.org/download", "zip"), ("https://example.org/report.docx", "docx")]
)
def test_format_selection_preserves_container_or_fixed_width(url: str, format_name: str) -> None:
    check(planning.format_for({"url": url, "format": "ZIP/CSV"}) == format_name, 'planning.format_for({"url": url, "format": "ZIP/CSV"}) == format_name')


@pytest.mark.parametrize(
    "url,key",
    [
        ("https://data.chhs.ca.gov/datastore/dump/example?bom=True", "resource_id"),
        ("https://data.cdc.gov/api/views/abcd-1234/rows.csv?accessType=DOWNLOAD", "dataset_id"),
        ("https://healthcarereportcard.illinois.gov/hospital/123456/exportAll", "hospital_id"),
    ],
)
def test_export_selections_are_exact_and_unfiltered(url: str, key: str) -> None:
    check(
        key in planning.export_selection({"url": url, "route_type": "permitted_export"}),
        'key in planning.export_selection({"url": url, "route_type": "permitted_export"})',
    )


def test_shared_url_prefers_complete_evidence_and_is_not_counted_twice(setup_capture: tuple) -> None:
    registry, _, rules, _, _ = inputs(setup_capture)
    source, route = registry["sources"][0], registry["sources"][0]["file_routes"][0]
    first = planning.route_plan(source, {**route, "status": "partial_response_not_parseable"}, rules, [])
    second = planning.route_plan(source, {**route, "route_id": "better_route", "status": "content_inspected"}, rules, [])
    planning.coordinate_shared_routes([first, second])
    check(
        first["capture_plan"] is None and first["shared_primary_route_id"] == "better_route",
        'first["capture_plan"] is None and first["shared_primary_route_id"] == "better_route"',
    )
    check(second["capture_plan"], 'second["capture_plan"]')


def test_dictionary_roles_and_release_dates_are_not_guessed(setup_capture: tuple) -> None:
    registry, _, rules, _, _ = inputs(setup_capture)
    source, route = registry["sources"][0], registry["sources"][0]["file_routes"][0]
    check(
        planning.role_for(source, {**route, "url": "https://example.org/dictionary.pdf"}, rules) == "dictionary",
        'planning.role_for(source, {**route, "url": "https://example.org/dictionary.pdf"}, rules) == "dictionary"',
    )
    rules["reference_route_ids"] = [route["route_id"]]
    check(planning.role_for(source, route, rules) == "methodology", 'planning.role_for(source, route, rules) == "methodology"')
    plan = planning.capture_plan(source, {**route, "vintage_or_release": {"catalog_modified": "2026-01-01"}}, rules)
    check(plan["release"]["release_date"] is None, 'plan["release"]["release_date"] is None')
    route.update(
        url="https://data.cms.gov/provider-data/sites/default/files/dataset-archives/theme/hospitals/file.zip", vintage_or_release="2020-01-04", format="ZIP"
    )
    check(
        planning.capture_plan(source, route, rules)["release"]["release_date"] == "2020-01-04",
        'planning.capture_plan(source, route, rules)["release"]["release_date"] == "2020-01-04"',
    )


def test_illinois_directory_plan_does_not_replace_numeric_csv(setup_capture: tuple) -> None:
    registry, _, rules, _, _ = inputs(setup_capture)
    source = registry["sources"][0]
    source.update(source_id="IL", preferred_route="file_plus_api_fallback", api_fallback={"needed": True, "reason": "Identifiers absent from CSV"})
    result = planning.api_plan(source, rules)
    check(
        result["execution_ready"] and result["capture_plan"]["mode"] == "il_directory",
        'result["execution_ready"] and result["capture_plan"]["mode"] == "il_directory"',
    )
    check("numeric histories remain file-first" in result["capture_plan"]["scope"], '"numeric histories remain file-first" in result["capture_plan"]["scope"]')
    check("route_id" not in result["capture_plan"], '"route_id" not in result["capture_plan"]')


def test_docx_capture_retains_office_container(setup_capture: tuple, tmp_path: Path) -> None:
    source, plan, validator = setup_capture
    path = tmp_path / "report.docx"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("[Content_Types].xml", "<Types/>")
        archive.writestr("word/document.xml", "<document/>")
    check(inspect_payload(path, "docx", "data") is None, 'inspect_payload(path, "docx", "data") is None')
    source["file_routes"][0]["url"] = "https://example.org/report.docx"
    plan.update(expected_format="docx", file_name="report.docx")
    receipt = capture(plan, tmp_path / "captures", {"sources": [source]}, validator, opener=Opener(Response(path.read_bytes())))
    check(read_json(receipt)["artifacts"][0]["compression"] == "zip", 'read_json(receipt)["artifacts"][0]["compression"] == "zip"')
    check(expand_snapshot(receipt, validator) == [], "expand_snapshot(receipt, validator) == []")
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("unrelated.xml", "<test/>")
    check(inspect_payload(path, "docx", "data") == "invalid_document_structure", 'inspect_payload(path, "docx", "data") == "invalid_document_structure"')


def saved_checkpoint(bundle: tuple, tmp_path: Path) -> tuple:
    path, references, client, registry, validator = bundle
    storage.upload_snapshot(path, references, client, OUTPUTS, registry, validator)
    record = read_json(path.with_name("s3_collections_reconciliation.json"))
    keys = {entry["object"]["key"] for entry in record["objects"] + record["manifests"]}
    verification = tmp_path / "verification.json"
    verification.write_bytes(
        storage.encoded_json(
            {
                "status": "collection_inventory_verified",
                "dataset_objects": len(keys),
                "dataset_count": len(record["manifests"]),
                "collection_root": record["publisher"] + "/" + record["collection"],
                "checked_at_utc": "2026-01-01T00:00:00+00:00",
                "held_members": [],
            }
        )
    )
    return path, verification, registry, validator


def test_checkpoint_uses_current_layout_and_preserves_refs(bundle: tuple, tmp_path: Path) -> None:
    path, verification, registry, validator = saved_checkpoint(bundle, tmp_path)
    result = planning.current_snapshot(path, verification, registry, validator)
    check(
        result["live_s3_rechecked"] is False and result["local_files_rechecked"] > 0,
        'result["live_s3_rechecked"] is False and result["local_files_rechecked"] > 0',
    )
    check(
        result["active_reconciliation"]["path"].endswith("s3_collections_reconciliation.json"),
        'result["active_reconciliation"]["path"].endswith("s3_collections_reconciliation.json")',
    )
    check(result["shared_references"], 'result["shared_references"]')


@pytest.mark.parametrize("mutation", ["snapshot", "count", "local_file"])
def test_changed_checkpoint_evidence_is_rejected(bundle: tuple, tmp_path: Path, mutation: str) -> None:
    path, verification, registry, validator = saved_checkpoint(bundle, tmp_path)
    reconciliation = path.with_name("s3_collections_reconciliation.json")
    record = read_json(reconciliation)
    if mutation == "snapshot":
        record["snapshot_id"] = "different"
        reconciliation.write_bytes(storage.encoded_json(record))
    elif mutation == "count":
        changed = read_json(verification)
        changed["dataset_objects"] = 0
        verification.write_bytes(storage.encoded_json(changed))
    else:
        reference = next(entry for entry in record["objects"] if entry.get("role") == "dictionary")
        (path.parent / reference["storage_path"]).write_bytes(b"changed")
    with pytest.raises(ValueError):
        planning.current_snapshot(path, verification, registry, validator)


def test_cli_generates_immutable_outputs_outside_repo_checklist(setup_capture: tuple, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    registry, inventory, rules, routes, validator = inputs(setup_capture)
    inventory_path, rules_path = tmp_path / "inventory.json", tmp_path / "rules.json"
    inventory_path.write_bytes(storage.encoded_json(inventory))
    rules_path.write_bytes(storage.encoded_json(rules))
    monkeypatch.setattr(planning, "RULES_PATH", rules_path)
    monkeypatch.setattr(planning, "load_registry", lambda: registry)
    monkeypatch.setattr(planning, "load_routes", lambda _: routes)
    monkeypatch.setattr(planning, "receipt_validator", lambda: validator)
    monkeypatch.setattr(planning, "current_snapshot", lambda *args: {"collection_root": "other", "shared_references": [], "requested_url": "other"})
    argv = [
        "plans",
        "--inventory",
        str(inventory_path),
        "--receipt",
        "example",
        "--verification",
        "example",
        "--output-directory",
        str(tmp_path / "output"),
        "--checklist",
        str(tmp_path / "review.md"),
    ]
    monkeypatch.setattr(sys, "argv", argv)
    planning.main()
    planning.main()
    check(len(list((tmp_path / "output").glob("*.json"))) == 3, 'len(list((tmp_path / "output").glob("*.json"))) == 3')
    result = read_json(tmp_path / "output/acquisition_plans_v1.json")
    check(result["registry_sha256"] == canonical_hash(registry), 'result["registry_sha256"] == canonical_hash(registry)')
    check((tmp_path / "review.md").exists(), '(tmp_path / "review.md").exists()')
    argv[-1] = str(planning.REPO_ROOT / "review.md")
    with pytest.raises(SystemExit):
        planning.main()
