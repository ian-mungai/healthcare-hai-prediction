"""E2E check of the data source verification log: the committed file matches a fresh build and each planted defect fails the check."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from typing import Any

import pytest

from scripts.acquisition import source_verification_log as log_module
from tests.support import check


@pytest.fixture(scope="module")
def inputs() -> dict[str, Any]:
    return log_module.load_inputs()


@pytest.fixture()
def built(inputs: dict[str, Any]) -> dict[str, Any]:
    return log_module.build_log(inputs)


def rows_by_id(log: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {row["dataset_id"]: row for row in log["rows"]}


def test_committed_log_equals_a_fresh_build_and_passes_the_check(inputs: dict[str, Any]) -> None:
    committed = json.loads(log_module.LOG_PATH.read_text(encoding="utf-8"))
    check(committed == log_module.build_log(inputs), "committed == log_module.build_log(inputs)")
    check(
        log_module.LOG_PATH.read_text(encoding="utf-8") == log_module.render(log_module.build_log(inputs)),
        "log_module.LOG_PATH.read_text(encoding='utf-8') == log_module.render(l",
    )
    check(log_module.check_log(committed, inputs) == [], "log_module.check_log(committed, inputs) == []")


def test_build_is_deterministic(inputs: dict[str, Any]) -> None:
    check(
        log_module.render(log_module.build_log(inputs)) == log_module.render(log_module.build_log(copy.deepcopy(inputs))),
        "log_module.render(log_module.build_log(inputs)) == log_module.render(l",
    )


def test_rows_cover_bronze_sources_validation_only_and_rejected(built: dict[str, Any], inputs: dict[str, Any]) -> None:
    rows = rows_by_id(built)
    for source_id in log_module.bronze_source_ids(inputs):
        check(source_id in rows, "source_id in rows")
    check(rows["care-hac-cms"]["status"] == "validation-only", "rows['care-hac-cms']['status'] == 'validation-only'")
    for rejected in ("AHRF", "NHSN"):
        check(rows[rejected]["status"] == "rejected", "rows[rejected]['status'] == 'rejected'")
        check(rows[rejected]["status_reason"] != "not recorded", "rows[rejected]['status_reason'] != 'not recorded'")
    check({row["status"] for row in built["rows"]} <= set(log_module.STATUSES), "{row['status'] for row in built['rows']} <= set(log_module.STATUSES)")


def test_confidential_sources_are_classed_and_carry_no_personal_field(built: dict[str, Any]) -> None:
    rows = rows_by_id(built)
    for source_id in ("CMS_OWNERS", "CMS_CHOW", "HCAI_FINANCE", "HCAI_UTIL"):
        check(rows[source_id]["data_class"] == "Confidential", "rows[source_id]['data_class'] == 'Confidential'")
    for row in built["rows"]:
        check(set(row) <= set(log_module.ROW_FIELDS), "set(row) <= set(log_module.ROW_FIELDS)")


def test_missing_bronze_row_fails_the_check(built: dict[str, Any], inputs: dict[str, Any]) -> None:
    broken = copy.deepcopy(built)
    broken["rows"] = [row for row in broken["rows"] if row["dataset_id"] != "SVI"]
    problems = log_module.check_log(broken, inputs)
    check(any("missing row" in problem and "SVI" in problem for problem in problems), "any('missing row' in problem and 'SVI' in problem for problem in probl")


def test_stale_evidence_hash_fails_the_check(built: dict[str, Any], inputs: dict[str, Any]) -> None:
    broken = copy.deepcopy(built)
    rows_by_id(broken)["main-hai-pdc"]["evidence"]["registry_record_sha256"] = "0" * 64
    problems = log_module.check_log(broken, inputs)
    check(
        any("stale evidence" in problem and "main-hai-pdc" in problem for problem in problems),
        "any('stale evidence' in problem and 'main-hai-pdc' in problem for prob",
    )


def test_personal_data_field_fails_the_check(built: dict[str, Any], inputs: dict[str, Any]) -> None:
    named_field = copy.deepcopy(built)
    rows_by_id(named_field)["CMS_OWNERS"]["contact_name"] = "Example Person"
    check(
        any("unexpected field" in problem and "contact_name" in problem for problem in log_module.check_log(named_field, inputs)),
        "any('unexpected field' in problem and 'contact_name' in problem for pr",
    )
    mailbox_value = copy.deepcopy(built)
    rows_by_id(mailbox_value)["CMS_OWNERS"]["access_and_terms"]["text"] = "Ask owner@example.org"
    check(
        any("personal data" in problem and "CMS_OWNERS" in problem for problem in log_module.check_log(mailbox_value, inputs)),
        "any('personal data' in problem and 'CMS_OWNERS' in problem for problem",
    )


def test_not_recorded_fact_without_a_question_fails_the_check(built: dict[str, Any], inputs: dict[str, Any]) -> None:
    broken = copy.deepcopy(built)
    broken["questions"] = [question for question in broken["questions"] if question["field"] != "evidence.receipt_sha256"]
    check(
        any("no question" in problem and "evidence.receipt_sha256" in problem for problem in log_module.check_log(broken, inputs)),
        "any('no question' in problem and 'evidence.receipt_sha256' in problem ",
    )


def test_hand_edit_that_differs_from_a_fresh_build_fails_the_check(built: dict[str, Any], inputs: dict[str, Any]) -> None:
    broken = copy.deepcopy(built)
    rows_by_id(broken)["SVI"]["publisher"] = "Somebody else"
    check(
        any("differs from a fresh build" in problem for problem in log_module.check_log(broken, inputs)),
        "any('differs from a fresh build' in problem for problem in log_module.",
    )


def test_person_name_in_a_free_text_field_fails_the_check(built: dict[str, Any], inputs: dict[str, Any]) -> None:
    titled = copy.deepcopy(built)
    rows_by_id(titled)["CMS_OWNERS"]["title"] = "Jane Placeholder"
    check(
        any("title" in problem and "allowlist" in problem and "CMS_OWNERS" in problem for problem in log_module.check_log(titled, inputs)),
        "any('title' in problem and 'allowlist' in problem and 'CMS_OWNERS' in ",
    )
    termed = copy.deepcopy(built)
    rows_by_id(termed)["CMS_OWNERS"]["access_and_terms"]["text"] = "Public source; ask Jane Placeholder for access."
    check(
        any("terms" in problem and "allowlist" in problem and "CMS_OWNERS" in problem for problem in log_module.check_log(termed, inputs)),
        "any('terms' in problem and 'allowlist' in problem and 'CMS_OWNERS' in ",
    )


def test_person_name_in_the_registry_stops_the_build(inputs: dict[str, Any]) -> None:
    for field in ("title", "reviewed_access_terms"):
        planted = copy.deepcopy(inputs)
        record = next(source for source in planted["registry"]["sources"] if source["source_id"] == "CMS_OWNERS")
        (record if field == "title" else record["validation"])[field] = "Jane Placeholder"
        with pytest.raises(log_module.RegistryError, match="allowlist"):
            log_module.build_log(planted)


# Digits of a fictional number, joined by the layout under test so no complete number appears in the source.
PHONE_LAYOUTS = ["{a}{b}{c}", "+1{a}{b}{c}", "+1 {a} {b} {c}", "1-{a}-{b}-{c}", "{a}.{b}.{c}", "({a}) {b}-{c}", "+1.{a}.{b}.{c}"]


@pytest.mark.parametrize("layout", PHONE_LAYOUTS)
def test_phone_number_variants_fail_the_check(layout: str, built: dict[str, Any], inputs: dict[str, Any]) -> None:
    broken = copy.deepcopy(built)
    rows_by_id(broken)["CMS_OWNERS"]["status_reason"] = "Call " + layout.format(a="555", b="010", c="0100") + " for access"
    check(
        any("personal data" in problem and "CMS_OWNERS" in problem for problem in log_module.check_log(broken, inputs)),
        "any('personal data' in problem and 'CMS_OWNERS' in problem for pro",
    )


def drop_collection_from_layout(inputs: dict[str, Any], source_id: str) -> tuple[dict[str, Any], str]:
    planted = copy.deepcopy(inputs)
    _, collection = log_module.collections_by_source(planted["layout"])[source_id]
    planted["layout"]["groups"] = [group for group in planted["layout"]["groups"] if source_id not in group["source_ids"]]
    return planted, collection


def test_bronze_collection_missing_from_the_layout_fails_the_check(built: dict[str, Any], inputs: dict[str, Any]) -> None:
    planted, collection = drop_collection_from_layout(inputs, "SVI")
    check(collection in log_module.bronze_groups(planted["bronze"]), "collection in log_module.bronze_groups(planted['bronze'])")
    problems = log_module.check_log(built, planted)
    check(
        any("bronze collection" in problem and collection in problem and "layout" in problem for problem in problems),
        "any('bronze collection' in problem and collection in problem and 'l",
    )
    with pytest.raises(log_module.RegistryError, match="layout"):
        log_module.build_log(planted)


def test_layout_collection_without_a_registry_source_fails_the_check(built: dict[str, Any], inputs: dict[str, Any]) -> None:
    planted = copy.deepcopy(inputs)
    gone = next(source for source in planted["registry"]["sources"] if source["source_id"] == "SVI")
    planted["registry"]["sources"].remove(gone)
    problems = log_module.check_log(built, planted)
    check(
        any("bronze collection" in problem and "registry" in problem for problem in problems),
        "any('bronze collection' in problem and 'registry' in problem for pro",
    )


def run_check_on(text: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> tuple[int, str]:
    path = tmp_path / "source_verification_log.json"
    path.write_text(text, encoding="utf-8")
    monkeypatch.setattr(log_module, "LOG_PATH", path)
    monkeypatch.setattr(sys, "argv", ["source_verification_log", "--check"])
    try:
        log_module.main()
    except SystemExit as stop:
        return int(stop.code or 0), capsys.readouterr().err
    return 0, capsys.readouterr().out


def test_check_accepts_the_canonical_file(built: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    code, output = run_check_on(log_module.render(built), tmp_path, monkeypatch, capsys)
    check(code == 0 and "valid" in output, "code == 0 and 'valid' in output")


def test_duplicate_key_with_an_email_before_the_real_value_fails_the_check(
    built: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    text = log_module.render(built)
    first_title = json.dumps(built["rows"][0]["title"], ensure_ascii=False)
    hidden = text.replace(f'"title": {first_title}', f'"title": "owner@example.org",\n      "title": {first_title}', 1)
    check(hidden != text and json.loads(hidden) == built, "hidden != text and json.loads(hidden) == built")
    code, output = run_check_on(hidden, tmp_path, monkeypatch, capsys)
    check(code == 1 and "Duplicate JSON key: title" in output, "code == 1 and 'Duplicate JSON key: title' in output")


def test_file_text_that_is_not_the_canonical_serialization_fails_the_check(
    built: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    compact = json.dumps(built, ensure_ascii=False)
    code, output = run_check_on(compact, tmp_path, monkeypatch, capsys)
    check(code == 1 and "canonical" in output, "code == 1 and 'canonical' in output")
