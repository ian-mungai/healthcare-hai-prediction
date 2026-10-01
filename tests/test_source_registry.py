"""Existing synthetic safeguards for immutable source registry approvals."""

from __future__ import annotations

import copy
import hashlib
import json
import sys
from pathlib import Path

import pytest

from scripts.acquisition import build_source_registry as builder
from scripts.acquisition import source_registry as registry
from scripts.acquisition.cli_tools import run_argv


def save_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


@pytest.fixture
def approved_inputs() -> tuple[dict, dict]:
    sources: list[dict] = []
    approvals, plans, validations, controls, measures = [], [], [], [], []
    shared_url = "https://example.org/archive.zip"
    for index, (route, (decision, disposition)) in enumerate(registry.ROUTE_RULES.items()):
        source_id, measure_id = f"source_{index}", f"M{index:03}"
        checks = ["Verify period and scope.", "Keep all existing access and modeling restrictions."]
        file_route = {
            "route_id": f"{source_id}:file:001",
            "url": shared_url,
            "format": "ZIP",
            "scope": f"member_{index}.csv",
            "vintage_or_release": "2020-01-01",
            "measurement_period": [["2018-01-01", "2018-12-31"]],
            "remaining_checks": ["Verify member and checksum."],
            "inspected_evidence": [{"local_path": "/Users/<user>/cache/archive.zip"}],
        }
        source = {
            "source_id": source_id,
            "title": f"Example source {index}",
            "preferred_route": route,
            "route_status": "Bounded research only.",
            "official_landing_url": "https://example.org/data",
            "file_routes": [file_route],
            "api_fallback": {"needed": route == "api_fallback"},
            "measurement_history": {"advertised": "2010 onward", "observed": [2018], "unresolved": "Intervening years not acquired."},
            "required_checks": checks,
            "evidence_urls": [shared_url],
            "linked_measure_ids": [measure_id],
            "original_source_family_ids": ["F01"],
        }
        approval = {
            "source_id": source_id,
            "preferred_route": route,
            "transport_decision": decision,
            "numeric_route": disposition == "conditional_route",
            "approval_scope": "transport_strategy_only",
            "modeling_eligibility": "not_decided_by_transport_approval",
            "bulk_acquisition_executed": False,
            "linked_measure_ids": [measure_id],
            "required_checks": checks,
            "remaining_checks": checks,
            "file_route_count": 1,
        }
        plan = {"source_id": source_id, "disposition": disposition, "acquisition_executed": False, "wave": 1}
        validation = {
            "source_id": source_id,
            "required_checks_verbatim": checks,
            "gates": [{"gate_id": "G01", "status": "not_run"}],
            "source_reference": builder.reference(f"/source_records/{index}", source),
            "reviewed_access_terms": "Verify permission before capture.",
        }
        measure = {"id": measure_id, "hold_actions": ["Keep pending definition check."], "file_first_acquisition": {"source_ids": [source_id]}}
        control = {
            "id": measure_id,
            "parent_id": None,
            "source_ids": [source_id],
            "gate_status": "not_run",
            "collection": "original_entries",
            "canonical_record": builder.reference(f"/original_entries/{index}", measure),
            "preserved_controls": measure,
        }
        sources.append(source)
        approvals.append(approval)
        plans.append(plan)
        validations.append(validation)
        measures.append(measure)
        controls.append(control)
    counts = registry.registry_counts([{**source, "approval": approval} for source, approval in zip(sources, approvals, strict=True)])
    manifest = {
        "source_records": sources,
        "original_entries": measures,
        "child_entries": [],
        "supplemental_closed_field_variants": [],
        "acquisition_policy": "Files first, approved API fallback only.",
        "limitations": ["Incomplete history."],
        "original_source_pool": [{"id": "F01"}],
    }
    approval_register = {"records": approvals, "counts": counts, "global_conditions": ["No gate clearance."], "input_manifest": {}}
    waves = {
        "source_records": plans,
        "inputs": {},
        "history_rule": "Keep long history.",
        "comparison_rule": "Compare matching hospital-years.",
        "sequencing_rule": "Retain blocked records without downloading them.",
        "shared_file_routes": [{"url": shared_url, "source_ids": sorted(source["source_id"] for source in sources), "reuse_rule": "Verify equal bytes."}],
    }
    gates = {"source_records": validations, "measure_controls": controls, "inputs": {}, "gate_catalog": [], "status_policy": {}, "threshold_policy": {}}
    documents = dict(zip(builder.INPUT_NAMES, (manifest, approval_register, waves, gates), strict=True))
    return documents, bind_inputs(documents)


def bind_inputs(documents: dict) -> dict:
    hashes: dict[str, str] = {}
    for name in builder.INPUT_NAMES:
        document = documents[name]
        if name == builder.INPUT_NAMES[1]:
            document["input_manifest"]["sha256"] = hashes[builder.INPUT_NAMES[0]]
        elif name in builder.INPUT_NAMES[2:]:
            document["inputs"] = {key: {"sha256": hashes[key]} for key in builder.INPUT_NAMES[:2]}
        hashes[name] = hashlib.sha256(json.dumps(document).encode("utf-8")).hexdigest()
    return hashes


def test_full_pool_build_preserves_controls_and_separates_research(approved_inputs: tuple[dict, dict]) -> None:
    documents, hashes = approved_inputs
    result, lock = builder.build_registry(documents, hashes)
    registry.validate_registry(result, lock)
    if not (result["counts"]["source_records"] == 6):
        raise AssertionError("Registry expectation failed")
    if not (result["counts"]["numeric_routes_approved_with_conditions"] == 4):
        raise AssertionError("Registry expectation failed")
    if not (result["counts"]["reference_only"] == result["counts"]["access_holds_retained"] == 1):
        raise AssertionError("Registry expectation failed")
    if not (result["measure_controls"] == documents[builder.INPUT_NAMES[3]]["measure_controls"]):
        raise AssertionError("Registry expectation failed")
    if not (result["sources"][0]["measurement_history"]["observed"] == [2018]):
        raise AssertionError("Registry expectation failed")
    if not (result["sources"][0]["file_routes"][0]["measurement_period"] == [["2018-01-01", "2018-12-31"]]):
        raise AssertionError("Registry expectation failed")
    if not ("inspected_evidence" not in result["sources"][0]["file_routes"][0]):
        raise AssertionError("Registry expectation failed")
    if not (len(result["shared_file_routes"]) == 1):
        raise AssertionError("Registry expectation failed")
    if not (documents[builder.INPUT_NAMES[0]]["source_records"][0]["file_routes"][0]["inspected_evidence"]):
        raise AssertionError("Registry expectation failed")
    if not (builder.build_registry(documents, hashes) == (result, lock)):
        raise AssertionError("Registry expectation failed")


@pytest.mark.parametrize("collection", ["sources", "measure_controls", "shared_file_routes"])
@pytest.mark.parametrize("mutation", ["remove", "duplicate"])
def test_missing_and_duplicate_records_fail(approved_inputs: tuple[dict, dict], collection: str, mutation: str) -> None:
    result, lock = builder.build_registry(*approved_inputs)
    records = result[collection]
    records.pop() if mutation == "remove" else records.append(copy.deepcopy(records[0]))
    with pytest.raises(registry.RegistryError):
        registry.validate_registry(result, lock)


@pytest.mark.parametrize(
    "path,value",
    [
        (("source_id",), "replacement_source"),
        (("preferred_route",), "unknown"),
        (("approval", "source_id"), "wrong_source"),
        (("approval", "preferred_route"), "access_hold"),
        (("approval", "transport_decision"), "approved"),
        (("approval", "numeric_route"), False),
        (("approval", "approval_scope"), "all_use"),
        (("approval", "modeling_eligibility"), "approved"),
        (("approval", "bulk_acquisition_executed"), True),
        (("planning", "disposition"), "access_hold"),
        (("planning", "acquisition_executed"), True),
        (("linked_measure_ids",), []),
        (("required_checks",), []),
        (("approval", "file_route_count"), 0),
        (("validation", "source_id"), "wrong_source"),
        (("validation", "required_checks_verbatim"), []),
        (("validation", "gates", 0, "status"), "pass"),
        (("file_routes", 0, "url"), "file:///tmp/local.csv"),
        (("file_routes", 0, "remaining_checks"), []),
        (("measurement_history", "unresolved"), "Complete"),
        (("api_fallback", "needed"), True),
    ],
)
def test_source_restriction_drift_fails(approved_inputs: tuple[dict, dict], path: tuple, value: object) -> None:
    result, lock = builder.build_registry(*approved_inputs)
    target = result["sources"][0]
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(registry.RegistryError):
        registry.validate_registry(result, lock)


def test_clearing_hold_with_adjusted_counts_still_fails(approved_inputs: tuple[dict, dict]) -> None:
    result, lock = builder.build_registry(*approved_inputs)
    source = next(item for item in result["sources"] if item["preferred_route"] == "access_hold")
    source["preferred_route"] = source["approval"]["preferred_route"] = "file_download"
    source["approval"]["transport_decision"] = registry.ROUTE_RULES["file_download"][0]
    source["approval"]["numeric_route"] = True
    source["planning"]["disposition"] = "conditional_route"
    result["counts"] = registry.registry_counts(result["sources"])
    with pytest.raises(registry.RegistryError, match="counts differ"):
        registry.validate_registry(result, lock)


@pytest.mark.parametrize("field,value", [("gate_status", "pass"), ("source_ids", ["unknown"]), ("parent_id", "unknown")])
def test_measure_gate_changes_fail(approved_inputs: tuple[dict, dict], field: str, value: object) -> None:
    result, lock = builder.build_registry(*approved_inputs)
    result["measure_controls"][0][field] = value
    with pytest.raises(registry.RegistryError):
        registry.validate_registry(result, lock)


def test_duplicate_route_ids_fail(approved_inputs: tuple[dict, dict]) -> None:
    result, lock = builder.build_registry(*approved_inputs)
    result["sources"][1]["file_routes"][0]["route_id"] = result["sources"][0]["file_routes"][0]["route_id"]
    with pytest.raises(registry.RegistryError, match="Duplicate file route ID"):
        registry.validate_registry(result, lock)


@pytest.mark.parametrize("input_index,key", [(0, "source_records"), (1, "records"), (2, "source_records"), (3, "source_records"), (3, "measure_controls")])
def test_import_rejects_incomplete_input_sets(approved_inputs: tuple[dict, dict], input_index: int, key: str) -> None:
    documents, _ = approved_inputs
    documents[builder.INPUT_NAMES[input_index]][key].pop()
    with pytest.raises(registry.RegistryError):
        builder.build_registry(documents, bind_inputs(documents))


@pytest.mark.parametrize("index", [1, 2, 3])
def test_import_rejects_stale_input_hashes(approved_inputs: tuple[dict, dict], index: int) -> None:
    documents, hashes = approved_inputs
    if index == 1:
        documents[builder.INPUT_NAMES[index]]["input_manifest"]["sha256"] = "0" * 64
    else:
        documents[builder.INPUT_NAMES[index]]["inputs"][builder.INPUT_NAMES[0]]["sha256"] = "0" * 64
    with pytest.raises(registry.RegistryError, match="hash|binding"):
        builder.build_registry(documents, hashes)


@pytest.mark.parametrize("collection,reference_key", [("source_records", "source_reference"), ("measure_controls", "canonical_record")])
@pytest.mark.parametrize("field,value", [("record_sha256", "0" * 64), ("json_pointer", "/wrong/0")])
def test_import_rejects_stale_record_references(approved_inputs: tuple[dict, dict], collection: str, reference_key: str, field: str, value: str) -> None:
    documents, hashes = approved_inputs
    documents[builder.INPUT_NAMES[3]][collection][0][reference_key][field] = value
    with pytest.raises(registry.RegistryError, match="Stale|Wrong"):
        builder.build_registry(documents, hashes)


def test_portable_evidence_does_not_disclose_workstation_username() -> None:
    value = {"path": "/Users/<user>/<private>/file.json", "nested": ["[review](/home/<user>/<notes>.md)", None, 7]}
    expected = {"path": "external://home/<private>/file.json", "nested": ["[review](external://home/<notes>.md)", None, 7]}
    if not (builder.portable(value) == expected):
        raise AssertionError("Registry expectation failed")


@pytest.mark.parametrize("content", ['{"id":1,"id":2}', '{"value":NaN}', "[]", '{"broken"'])
def test_invalid_json_fails(tmp_path: Path, content: str) -> None:
    path = tmp_path / "invalid.json"
    path.write_text(content, encoding="utf-8")
    with pytest.raises(registry.RegistryError):
        registry.read_json(path)


def test_missing_json_fails(tmp_path: Path) -> None:
    with pytest.raises(registry.RegistryError):
        registry.read_json(tmp_path / "missing.json")


@pytest.mark.parametrize("records", [None, [None], [{}], [{"id": ""}], [{"id": "same"}, {"id": "same"}]])
def test_invalid_records_fail(records: object) -> None:
    with pytest.raises(registry.RegistryError):
        registry.index_records(records, "id", "example")


def test_import_check_and_no_overwrite(approved_inputs: tuple[dict, dict], tmp_path: Path) -> None:
    documents, _ = approved_inputs
    for name, document in documents.items():
        save_json(tmp_path / name, document)
    output = tmp_path / "output"
    builder.import_registry(tmp_path, output)
    builder.import_registry(tmp_path, output, check=True)
    if not (registry.load_registry(output / "source_registry.json", output / "source_registry_lock.json")["counts"]["source_records"] == 6):
        raise AssertionError("Registry expectation failed")
    with pytest.raises(registry.RegistryError, match="Refusing to replace"):
        builder.import_registry(tmp_path, output)
    save_json(output / "source_registry.json", {})
    with pytest.raises(registry.RegistryError, match="stale"):
        builder.import_registry(tmp_path, output, check=True)


def test_import_preflights_both_outputs(approved_inputs: tuple[dict, dict], tmp_path: Path) -> None:
    documents, _ = approved_inputs
    for name, document in documents.items():
        save_json(tmp_path / name, document)
    output = tmp_path / "output"
    output.mkdir()
    save_json(output / "source_registry_lock.json", {})
    with pytest.raises(registry.RegistryError, match="Refusing to replace"):
        builder.import_registry(tmp_path, output)
    if (output / "source_registry.json").exists():
        raise AssertionError("Registry expectation failed")


def test_cli_validates_without_credentials_from_another_directory(approved_inputs: tuple[dict, dict], tmp_path: Path) -> None:
    sample, lock = builder.build_registry(*approved_inputs)
    registry_path, lock_path = tmp_path / "registry.json", tmp_path / "lock.json"
    save_json(registry_path, sample)
    save_json(lock_path, lock)
    result = run_argv(
        [sys.executable, str(Path(registry.__file__).resolve()), "--registry", str(registry_path), "--lock", str(lock_path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
        env={"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)},
    )
    if not (result.returncode == 0):
        raise AssertionError("Registry expectation failed")
    if "6 sources" not in result.stdout:
        raise AssertionError("Registry expectation failed")
    if "1 access holds retained" not in result.stdout:
        raise AssertionError("Registry expectation failed")
    if "No data acquired" not in result.stdout:
        raise AssertionError("Registry expectation failed")


def test_cli_fails_on_bad_registry(tmp_path: Path) -> None:
    result = run_argv(
        [sys.executable, str(Path(registry.__file__).resolve()), "--registry", str(tmp_path / "missing.json"), "--lock", str(tmp_path / "missing_lock.json")],
        capture_output=True,
        text=True,
        timeout=30,
        env={"PATH": "/usr/bin:/bin", "HOME": str(tmp_path)},
    )
    if not (result.returncode != 0):
        raise AssertionError("Registry expectation failed")
    if "Cannot read" not in result.stderr:
        raise AssertionError("Registry expectation failed")


def test_saved_registry_retains_entire_sample_pool(approved_inputs: tuple[dict, dict], tmp_path: Path) -> None:
    sample, lock = builder.build_registry(*approved_inputs)
    registry_path, lock_path = tmp_path / "registry.json", tmp_path / "lock.json"
    save_json(registry_path, sample)
    save_json(lock_path, lock)
    result = registry.load_registry(registry_path, lock_path)
    if not (result["counts"]["source_records"] == 6):
        raise AssertionError("Registry expectation failed")
    if not (result["counts"]["numeric_routes_approved_with_conditions"] == 4):
        raise AssertionError("Registry expectation failed")
    if not (result["counts"]["reference_only"] == 1):
        raise AssertionError("Registry expectation failed")
    if not (result["counts"]["access_holds_retained"] == 1):
        raise AssertionError("Registry expectation failed")
    if not (len(result["source_families"]) == 1):
        raise AssertionError("Registry expectation failed")
    if not (len(result["measure_controls"]) == 6):
        raise AssertionError("Registry expectation failed")
    if not (len(result["shared_file_routes"]) == 1):
        raise AssertionError("Registry expectation failed")
    content = registry_path.read_text(encoding="utf-8").replace("external://home/", "")
    if not ("/Users/" not in content and "/home/" not in content):
        raise AssertionError("Registry expectation failed")


def test_validator_main(approved_inputs: tuple[dict, dict], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    result, lock = builder.build_registry(*approved_inputs)
    registry_path, lock_path = tmp_path / "registry.json", tmp_path / "lock.json"
    save_json(registry_path, result)
    save_json(lock_path, lock)
    monkeypatch.setattr(sys, "argv", ["source_registry", "--registry", str(registry_path), "--lock", str(lock_path)])
    registry.main()
    if "6 sources" not in capsys.readouterr().out:
        raise AssertionError("Registry expectation failed")
    result["sources"][0]["approval"] = None
    save_json(registry_path, result)
    with pytest.raises(SystemExit) as error:
        registry.main()
    if not (error.value.code == 2):
        raise AssertionError("Registry expectation failed")
    if "Malformed registry" not in capsys.readouterr().err:
        raise AssertionError("Registry expectation failed")


def test_builder_main(approved_inputs: tuple[dict, dict], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    documents, _ = approved_inputs
    for name, document in documents.items():
        save_json(tmp_path / name, document)
    output = tmp_path / "output"
    arguments = ["build_source_registry", "--audit-directory", str(tmp_path), "--output-directory", str(output)]
    monkeypatch.setattr(sys, "argv", arguments)
    builder.main()
    if "generated" not in capsys.readouterr().out:
        raise AssertionError("Registry expectation failed")
    monkeypatch.setattr(sys, "argv", [*arguments, "--check"])
    builder.main()
    if "matches approved inputs" not in capsys.readouterr().out:
        raise AssertionError("Registry expectation failed")
    monkeypatch.setattr(sys, "argv", arguments)
    with pytest.raises(SystemExit) as error:
        builder.main()
    if not (error.value.code == 2):
        raise AssertionError("Registry expectation failed")
    if "Refusing to replace" not in capsys.readouterr().err:
        raise AssertionError("Registry expectation failed")


@pytest.mark.parametrize("collection", ["original_entries", "child_entries", "supplemental_closed_field_variants"])
def test_duplicate_measure_identity_cannot_be_imported(approved_inputs: tuple[dict, dict], collection: str) -> None:
    documents, hashes = approved_inputs
    manifest = documents[builder.INPUT_NAMES[0]]
    manifest[collection].append(copy.deepcopy(manifest["original_entries"][0]))
    with pytest.raises(registry.RegistryError, match="Duplicate"):
        builder.build_registry(documents, hashes)


def test_unrelated_source_metadata_change_is_detected(approved_inputs: tuple[dict, dict]) -> None:
    result, lock = builder.build_registry(*approved_inputs)
    result["sources"][0]["title"] = "Changed without approval"
    with pytest.raises(registry.RegistryError, match="content differs"):
        registry.validate_registry(result, lock)
