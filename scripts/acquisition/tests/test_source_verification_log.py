"""E2E check of the data source verification log: the committed file matches a fresh build and each planted defect fails the check."""

from __future__ import annotations

import copy
import json
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
