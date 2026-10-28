"""Real-process checks for approved registry additions; no network or AWS calls."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.acquisition.cli_tools import run_argv
from scripts.acquisition.registry_additions_boundary import BOUNDARY_RULES, PROBE_REPLACEMENTS, SENSITIVITY_RULES
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import canonical_hash, load_registry, read_json
from scripts.process import CompletedProcess, SubprocessError

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG = REPO_ROOT / "config" / "acquisition"
LEGACY_ARCHIVE = REPO_ROOT / "data/acquisition_planning/registry_legacy_20260929"
FILES = ("source_registry.json", "source_registry_lock.json", "registry_additions.json", "registry_additions_lock.json")
PINNED_BATCH = CONFIG / "planning_v1" / "bounded_batch_v1.json"


def sanitize(text: str) -> str:
    """Replace workstation paths so shareable evidence never names the local user."""
    return text.replace(str(REPO_ROOT), "<repo>").replace(str(Path.home()), "<home>")


def cli(case: Path, write_lock: bool = False, bronze_lineage: Path | None = None) -> CompletedProcess[str]:
    """Run the additions CLI against one case directory's copied configuration."""
    argv = [sys.executable, "-m", "scripts.acquisition.registry_additions"]
    argv += ["--registry", str(case / FILES[0]), "--lock", str(case / FILES[1]), "--additions", str(case / FILES[2]), "--additions-lock", str(case / FILES[3])]
    argv += ["--write-lock"] if write_lock else []
    argv += ["--bronze-lineage", str(bronze_lineage)] if bronze_lineage is not None else []
    return run_argv(argv, cwd=REPO_ROOT, capture_output=True, text=True, timeout=120)


def prepare(root: Path, name: str) -> Path:
    """Copy the real registry, lock, additions and additions lock into a new case directory."""
    case = root / "cases" / name
    case.mkdir(parents=True)
    for file_name in FILES:
        write_once(case / file_name, (CONFIG / file_name).read_bytes())
    return case


def edit_additions(case: Path, change: Callable[[dict[str, Any]], None], drop_lock: bool) -> None:
    """Apply one mutation to the copied additions file and optionally remove its copied lock."""
    path = case / FILES[2]
    additions = read_json(path)
    change(additions)
    path.write_bytes(encoded_json(additions))
    if drop_lock:
        (case / FILES[3]).unlink()


def control(additions: dict[str, Any], identifier: str) -> dict[str, Any]:
    """Return the added control with the given ID from a mutable additions document."""
    return next(item for item in additions["measure_controls"] if item["id"] == identifier)


def entry(additions: dict[str, Any], identifier: str) -> dict[str, Any]:
    """Return the source metadata entry for a source ID from a mutable additions document."""
    return next(item for item in additions["source_metadata"] if item["source_id"] == identifier)


def two_sources(additions: dict[str, Any]) -> None:
    """Give one dataset_id to a second source."""
    other = entry(additions, "CMS_HCRIS_PUF")
    other["dataset_ids"].append("cms_gv")
    other["dataset_ids_evidence"].append({**entry(additions, "CMS_GV")["dataset_ids_evidence"][0]})


def repeated_in_list(additions: dict[str, Any]) -> None:
    """List one dataset_id twice under its source."""
    target = entry(additions, "CMS_GV")
    target["dataset_ids"].append("cms_gv")
    target["dataset_ids_evidence"].append({**target["dataset_ids_evidence"][0]})


def invalid_dataset_id(additions: dict[str, Any]) -> None:
    """Replace a dataset_id and its evidence with an identifier that is not lowercase."""
    target = entry(additions, "CMS_GV")
    target["dataset_ids"] = ["Bad-ID"]
    target["dataset_ids_evidence"][0]["dataset_id"] = "Bad-ID"


def drop_evidence_record(additions: dict[str, Any]) -> None:
    """Remove the evidence record of a source's only dataset_id."""
    entry(additions, "CMS_GV")["dataset_ids_evidence"] = []


def remove_entry(additions: dict[str, Any]) -> None:
    """Remove one entry while the lock still lists it."""
    additions["source_metadata"] = [item for item in additions["source_metadata"] if item["source_id"] != "CMS_GV"]


def counts_match(stdout: str, additions: dict[str, Any]) -> bool:
    """Return whether the CLI's source metadata line equals the entry and dataset_id counts of the additions file."""
    match = re.search(r"Source metadata: ([0-9]+) sources with a publisher; ([0-9]+) dataset_ids placed", stdout)
    entries = additions.get("source_metadata", [])
    return match is not None and (int(match.group(1)), int(match.group(2))) == (len(entries), sum(len(item["dataset_ids"]) for item in entries))


def rejection(result: CompletedProcess[str], case: Path, expect_no_lock: bool) -> list[dict[str, Any]]:
    """Assert a handled CLI rejection: exit 2, an argparse error message and no traceback or new lock."""
    checks = [
        {"name": "exit_code_2", "passed": result.returncode == 2},
        {"name": "handled_error_message", "passed": "error:" in result.stderr and "Traceback" not in result.stderr},
    ]
    if expect_no_lock:
        checks.append({"name": "no_lock_written", "passed": not (case / FILES[3]).exists()})
    return checks


MUTATIONS: dict[str, tuple[Callable[[dict[str, Any]], None], bool]] = {
    "id_collision": (lambda a: control(a, "C258.81").update(id="C258.79"), True),
    "duplicate_ids": (lambda a: control(a, "C258.81").update(id="C258.80"), True),
    "unknown_parent": (lambda a: control(a, "C258.80").update(parent_id="C999"), True),
    "unknown_source": (lambda a: control(a, "C258.80").update(source_ids=["NOT_A_SOURCE"]), True),
    "empty_source": (lambda a: control(a, "C258.80").update(source_ids=[]), True),
    "cleared_gate": (lambda a: control(a, "C258.80").update(gate_status="passed"), True),
    "non_hold_decision": (lambda a: control(a, "C258.80")["preserved_controls"].update(current_review_decision="retain_conditional"), True),
    "missing_approval_date": (lambda a: control(a, "C258.80")["approval"].pop("decided_on"), True),
    "non_iso_approval_date": (lambda a: control(a, "C258.80")["approval"].update(decided_on="25/09/2026"), True),
    "approver_not_user": (lambda a: control(a, "C258.80")["approval"].update(decided_by="agent"), True),
    "tampered_additions": (lambda a: control(a, "C258.80")["preserved_controls"]["current_remaining_checks"].append("Unreviewed edit."), False),
    # entries that are unregistered, doubled or malformed, or approvals that are not the user's.
    "sm_unknown_source": (lambda a: entry(a, "CMS_GV").update(source_id="NOT_A_SOURCE"), True),
    "sm_duplicate_entry": (lambda a: entry(a, "CMS_GV").update(source_id="CMS_HCRIS_PUF"), True),
    "sm_extra_key": (lambda a: entry(a, "CMS_GV").update(note="not allowed"), True),
    "sm_missing_key": (lambda a: entry(a, "CMS_GV").pop("approval"), True),
    "sm_approver_not_user": (lambda a: entry(a, "CMS_GV")["approval"].update(decided_by="agent"), True),
    "sm_non_iso_approval_date": (lambda a: entry(a, "CMS_GV")["approval"].update(decided_on="12/10/2026"), True),
    "sm_approval_without_decision": (lambda a: entry(a, "CMS_GV")["approval"].update(decision=" "), True),
    # dataset_id ownership and evidence.
    "sm_dataset_id_two_sources": (two_sources, True),
    "sm_dataset_id_repeated": (repeated_in_list, True),
    "sm_dataset_id_invalid": (invalid_dataset_id, True),
    "sm_evidence_count_mismatch": (drop_evidence_record, True),
    # publisher and its evidence.
    "sm_empty_publisher": (lambda a: entry(a, "CMS_GV").update(publisher="  "), True),
    "sm_publisher_not_text": (lambda a: entry(a, "CMS_GV").update(publisher=5), True),
    "sm_landing_url_differs": (lambda a: entry(a, "CMS_GV")["publisher_evidence"].update(official_landing_url="https://example.org/other"), True),
    "sm_missing_page_hash": (lambda a: entry(a, "CMS_GV")["publisher_evidence"].pop("page_sha256"), True),
    "sm_bad_page_hash": (lambda a: entry(a, "CMS_GV")["publisher_evidence"].update(page_sha256="XYZ"), True),
    "sm_bad_retrieval_time": (lambda a: entry(a, "CMS_GV")["publisher_evidence"].update(retrieved_utc="2026-10-12 16:58:34"), True),
    # the section cannot carry a base field, gate or hold.
    "sm_base_field_gate_status": (lambda a: entry(a, "CMS_GV").update(gate_status="passed"), True),
    "sm_base_field_route": (lambda a: entry(a, "CMS_GV").update(preferred_route="api_fallback"), True),
    # the section changes after the lock (no relock) or the entry list differs from the lock.
    "sm_tampered_publisher": (lambda a: entry(a, "CMS_GV").update(publisher="Unreviewed Agency"), False),
    "sm_entry_removed_with_stale_lock": (remove_entry, False),
}


def remove_placement_sections(additions: dict[str, Any]) -> None:
    """Remove both sections to exercise the tracked-input requirement."""
    additions.pop("unplaced_dataset_ids")
    additions.pop("dataset_id_placements")


def empty_publisher_field(field: str) -> Callable[[dict[str, Any]], None]:
    """Build a typed mutation for one publisher evidence field."""

    def change(additions: dict[str, Any]) -> None:
        entry(additions, "CMS_GV")["publisher_evidence"][field] = " "

    return change


# One mutation per declared evidence condition; exact messages prevent accidental rejection passes.
EVIDENCE_MUTATIONS: dict[str, tuple[Callable[[dict[str, Any]], None], str]] = {
    "sm_impossible_approval_date": (lambda a: entry(a, "CMS_GV")["approval"].update(decided_on="2026-02-30"), "impossible approval date"),
    "control_impossible_approval_date": (lambda a: control(a, "C258.80")["approval"].update(decided_on="2026-02-30"), "impossible approval date"),
    "sm_evidence_out_of_order": (lambda a: entry(a, "ACS")["dataset_ids_evidence"].reverse(), "evidence is out of order"),
    "sm_evidence_empty_rule": (lambda a: entry(a, "CMS_GV")["dataset_ids_evidence"][0].update(rule=" "), "needs a rule and a detail"),
    "sm_evidence_empty_detail": (lambda a: entry(a, "CMS_GV")["dataset_ids_evidence"][0].update(detail=" "), "needs a rule and a detail"),
    "sm_evidence_malformed_record": (lambda a: entry(a, "CMS_GV")["dataset_ids_evidence"][0].update(extra=True), "malformed dataset_id evidence"),
    "sm_evidence_lists_required": (lambda a: entry(a, "CMS_GV").update(dataset_ids_evidence={}), "needs dataset_ids and evidence lists"),
    "sm_publisher_evidence_not_object": (lambda a: entry(a, "CMS_GV").update(publisher_evidence=[]), "needs publisher evidence"),
    "sm_publisher_evidence_extra_key": (lambda a: entry(a, "CMS_GV")["publisher_evidence"].update(extra=True), "malformed publisher evidence"),
    "sm_empty_evidence_note": (lambda a: entry(a, "CMS_GV")["publisher_evidence"].update(evidence_note=" "), "empty evidence note"),
    "sm_approval_incomplete": (lambda a: entry(a, "CMS_GV")["approval"].pop("recorded_in"), "complete approval block"),
    "sm_approval_empty_location": (lambda a: entry(a, "CMS_GV")["approval"].update(recorded_in=" "), "must cite its decision"),
    "sm_impossible_retrieval_date": (
        lambda a: entry(a, "CMS_GV")["publisher_evidence"].update(retrieved_utc="2026-02-30T12:00:00+00:00"),
        "impossible retrieval time",
    ),
    "sm_impossible_retrieval_time": (
        lambda a: entry(a, "CMS_GV")["publisher_evidence"].update(retrieved_utc="2026-10-12T99:00:00+00:00"),
        "impossible retrieval time",
    ),
    "sm_zero_year_retrieval": (
        lambda a: entry(a, "CMS_GV")["publisher_evidence"].update(retrieved_utc="0000-00-00T00:00:00+00:00"),
        "impossible retrieval time",
    ),
    "sm_non_utc_retrieval": (lambda a: entry(a, "CMS_GV")["publisher_evidence"].update(retrieved_utc="2026-10-12T12:00:00+01:00"), "UTC retrieval time"),
    "placement_changed_owner": (lambda a: a["dataset_id_placements"][0].update(source_id="CMS_GV"), "Stored placements differ"),
    "placement_changed_rule": (lambda a: a["dataset_id_placements"][0].update(rule="manual"), "Stored placements differ"),
    "placement_changed_detail": (lambda a: a["dataset_id_placements"][0].update(detail="manual"), "Stored placements differ"),
    "placement_duplicate": (lambda a: a["dataset_id_placements"].append(dict(a["dataset_id_placements"][0])), "Duplicate placement ID"),
    "placement_missing_section": (lambda a: a.pop("unplaced_dataset_ids"), "sections must exist together"),
    "placement_missing_both_sections": (remove_placement_sections, "needs tracked placement sections"),
    "unplaced_changed_evidence": (lambda a: a["unplaced_dataset_ids"][0].update(detail="manual"), "Unplaced evidence differs"),
    "unplaced_with_rule": (lambda a: a["unplaced_dataset_ids"].append({"dataset_id": "cms_gv", "detail": "unknown"}), "both placed and unplaced"),
    "source_missing_placement": (lambda a: entry(a, "CMS_GV").update(dataset_ids=[], dataset_ids_evidence=[]), "Source dataset_ids differ"),
    "source_changed_evidence": (lambda a: entry(a, "CMS_GV")["dataset_ids_evidence"][0].update(detail="manual"), "Source dataset evidence differs"),
}
for field in ("page_title", "naming_field", "naming_text"):
    EVIDENCE_MUTATIONS[f"sm_empty_{field}"] = (empty_publisher_field(field), f"evidence needs {field}")
MUTATIONS.update({name: (change, True) for name, (change, _) in EVIDENCE_MUTATIONS.items()})


# The handled message each source metadata case must produce, so a case cannot pass for an unrelated reason.
EXPECTED_MESSAGE = {
    "sm_unknown_source": "unregistered source",
    "sm_duplicate_entry": "Duplicate source metadata ID",
    "sm_extra_key": "must hold exactly the keys",
    "sm_missing_key": "must hold exactly the keys",
    "sm_approver_not_user": "needs the user's approval",
    "sm_non_iso_approval_date": "needs an ISO approval date",
    "sm_approval_without_decision": "must cite its decision",
    "sm_dataset_id_two_sources": "belongs to more than one source",
    "sm_dataset_id_repeated": "belongs to more than one source",
    "sm_dataset_id_invalid": "invalid dataset_id",
    "sm_evidence_count_mismatch": "one evidence record per dataset_id",
    "sm_empty_publisher": "needs a publisher",
    "sm_publisher_not_text": "needs a publisher",
    "sm_landing_url_differs": "differs from the registry landing URL",
    "sm_missing_page_hash": "malformed publisher evidence",
    "sm_bad_page_hash": "needs a page SHA-256",
    "sm_bad_retrieval_time": "needs a UTC retrieval time",
    "sm_base_field_gate_status": "must hold exactly the keys",
    "sm_base_field_route": "must hold exactly the keys",
    "sm_tampered_publisher": "differ from their lock",
    "sm_entry_removed_with_stale_lock": "Source metadata IDs differ",
}

EXPECTED_MESSAGE.update({name: message for name, (_, message) in EVIDENCE_MUTATIONS.items()})


def run_coverage_cases(root: Path) -> list[dict[str, Any]]:
    """Check completeness, explicit unplaced coverage and evidence through public offline CLIs."""
    from scripts.acquisition.source_metadata import derive

    fixture = REPO_ROOT / "tests/fixtures/registry_additions_bronze_lineage.csv"
    cases = []
    for name, message in (
        ("missing_id", "Bronze dataset_ids are neither placed nor listed as unplaced"),
        ("unplaced_id", None),
        ("placement_without_evidence", "needs one evidence record per dataset_id"),
    ):
        case = prepare(root, "coverage_" + name)
        document = read_json(case / FILES[2])
        ids = ["cms_gv"] + (["synthetic_coverage_unknown"] if name == "unplaced_id" else [])
        placements, unresolved = derive(ids, read_json(case / FILES[0]))
        metadata = entry(document, "CMS_GV")
        metadata["dataset_ids"] = ["cms_gv"]
        metadata["dataset_ids_evidence"] = [{key: value for key, value in placements[0].items() if key != "source_id"}]
        metadata["dataset_ids_evidence"][0]["acquired_via"] = "main-hai-pdc"
        document.update(source_metadata=[metadata], dataset_id_placements=placements, unplaced_dataset_ids=unresolved)
        additions = case / FILES[2]
        additions.write_bytes(encoded_json(document))
        lock = case / FILES[3]
        lock.unlink()
        valid_control = cli(case, write_lock=True)
        if name == "placement_without_evidence":
            metadata["dataset_ids_evidence"] = []
            additions.write_bytes(encoded_json(document))
        lineage = case / "bronze_lineage.csv"
        text = fixture.read_text(encoding="utf-8")
        if name == "placement_without_evidence":
            text = "\n".join(line for line in text.splitlines() if "synthetic_coverage_unknown" not in line) + "\n"
        lineage.write_text(text, encoding="utf-8")
        checked = cli(case, bronze_lineage=lineage)
        lock.unlink()
        written = cli(case, write_lock=True, bronze_lineage=lineage)
        source_checked = run_argv(
            [sys.executable, "-m", "scripts.acquisition.source_metadata", "--check", "--additions", str(additions), "--bronze-lineage", str(lineage)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=120,
        )
        checks = [{"name": "valid_control", "passed": valid_control.returncode == 0}]
        if message:
            checks += [
                {"name": "writer_refuses_intended_cause", "passed": written.returncode == 2 and message in written.stderr},
                {"name": "loader_refuses_intended_cause", "passed": checked.returncode == 2 and message in checked.stderr},
                {"name": "source_refuses_intended_cause", "passed": source_checked.returncode == 2 and message in source_checked.stderr},
                {"name": "no_lock_created", "passed": not lock.exists()},
            ]
        else:
            before = fingerprint(lock) if lock.exists() else None
            repeated = cli(case, write_lock=True, bronze_lineage=lineage)
            checks += [
                {"name": "explicit_unplaced_accepted", "passed": written.returncode == checked.returncode == source_checked.returncode == 0},
                {"name": "repeat_preserves_lock", "passed": before is not None and repeated.returncode == 0 and before == fingerprint(lock)},
            ]
        cases.append(
            {
                "case": "coverage_" + name,
                "passed": all(check["passed"] for check in checks),
                "checks": checks,
                "valid_control": process_evidence(valid_control),
                "writer": process_evidence(written),
                "readback": process_evidence(checked),
                "source_check": process_evidence(source_checked),
                "fixture_sha256": fingerprint(fixture)[0],
            }
        )
    case = prepare(root, "coverage_absent_additions")
    valid_control = cli(case)
    for filename in FILES[2:]:
        (case / filename).unlink()
    lineage = case / "bronze_lineage.csv"
    lineage.write_bytes(fixture.read_bytes())
    checked = cli(case, bronze_lineage=lineage)
    ordinary = cli(case)
    message = "Bronze dataset_ids are neither placed nor listed as unplaced"
    checks = [
        {"name": "valid_control", "passed": valid_control.returncode == 0},
        {"name": "explicit_gate_refuses_empty_inventory", "passed": checked.returncode == 2 and message in checked.stderr},
        {"name": "ordinary_optional_additions_accepted", "passed": ordinary.returncode == 0 and "Bronze lineage gate not run" in ordinary.stdout},
        {"name": "no_additions_pair_created", "passed": all(not (case / filename).exists() for filename in FILES[2:])},
    ]
    cases.append(
        {
            "case": "coverage_absent_additions",
            "passed": all(check["passed"] for check in checks),
            "checks": checks,
            "valid_control": process_evidence(valid_control),
            "readback": process_evidence(checked),
            "ordinary_optional": process_evidence(ordinary),
            "fixture_sha256": fingerprint(fixture)[0],
        }
    )
    return cases


def run_lineage_cases(root: Path) -> list[dict[str, Any]]:
    """Exercise synthetic bronze provenance through both public CLIs and the lock writer."""
    from scripts.acquisition.registry_additions import additions_lock
    from scripts.acquisition.source_metadata import derive

    fixture = REPO_ROOT / "tests/fixtures/registry_additions_bronze_lineage.csv"
    cases = []
    scenarios: list[tuple[str, str, Any, str | None, str | None]] = [
        ("missing_route", "cms_gv", None, None, "does not account for bronze sources"),
        ("observed_route", "cms_gv", "main-hai-pdc", None, None),
        ("unreviewed_route", "cms_gv", "ACS", "unreviewed", "acquired_via is not a reviewed route"),
        ("redundant_route", "main_hai_pdc", "main-hai-pdc", "redundant", "acquired_via duplicates its catalog source"),
        ("absent_dataset", "cms_9n3s_kdb3", None, None, "is absent from bronze lineage"),
        ("invented_route", "cms_gv", "main-hai-pdc", "direct", "acquired_via is not shown by bronze"),
        ("empty_route", "cms_gv", "", None, "needs a non-empty acquired_via"),
        ("nontext_route", "cms_gv", ["main-hai-pdc"], None, "needs a non-empty acquired_via"),
        ("unaccounted_source", "cms_gv", "main-hai-pdc", "extra", "does not account for bronze sources"),
        ("direct_source", "cms_gv", None, "direct", None),
        ("bad_header", "cms_gv", "main-hai-pdc", "header", "needs exactly the gate CSV columns"),
        ("empty_inventory", "cms_gv", "main-hai-pdc", "empty", "must contain at least one row"),
        ("invalid_count", "cms_gv", "main-hai-pdc", "count", "has an invalid bronze row"),
        ("invalid_dataset", "cms_gv", "main-hai-pdc", "dataset", "has an invalid bronze row"),
        ("missing_cell", "cms_gv", "main-hai-pdc", "cell", "has an invalid bronze row"),
        ("missing_file", "cms_gv", "main-hai-pdc", "missing", "No such file"),
        ("invalid_encoding", "cms_gv", "main-hai-pdc", "encoding", "Cannot read bronze lineage"),
    ]
    for name, dataset_id, via, variant, message in scenarios:
        case = prepare(root, "lineage_" + name)
        document = read_json(case / FILES[2])
        source_id = {"cms_gv": "CMS_GV", "main_hai_pdc": "main-hai-pdc"}.get(dataset_id, "care-hrrp-cms")
        metadata = entry(document, source_id)
        placements, unresolved = derive([dataset_id], read_json(case / FILES[0]))
        metadata["dataset_ids"] = [dataset_id]
        metadata["dataset_ids_evidence"] = [{key: value for key, value in placements[0].items() if key != "source_id"}]
        if via is not None:
            metadata["dataset_ids_evidence"][0]["acquired_via"] = via
        document.update(source_metadata=[metadata], dataset_id_placements=placements, unplaced_dataset_ids=unresolved)
        (case / FILES[2]).write_bytes(encoded_json(document))
        (case / FILES[3]).unlink()
        lineage = case / "bronze_lineage.csv"
        text = "\n".join(line for line in fixture.read_text(encoding="utf-8").splitlines() if "synthetic_coverage_unknown" not in line) + "\n"
        if variant == "unreviewed":
            text = text.replace("main-hai-pdc", "ACS")
        elif variant == "redundant":
            text = text.splitlines()[0] + "\nsynthetic_direct,main-hai-pdc,main_hai_pdc,7\n"
        elif variant == "extra":
            text += "synthetic_third,WONDER,cms_gv,3\n"
        elif variant == "direct":
            text = text.splitlines()[0] + "\nsynthetic_direct,CMS_GV,cms_gv,7\n"
        elif variant == "header":
            text = text.replace("_dataset_id", "dataset_id")
        elif variant == "empty":
            text = text.splitlines()[0] + "\n"
        elif variant == "count":
            text = text.replace(",7", ",-1")
        elif variant == "dataset":
            text = text.replace("cms_gv", "Bad-ID")
        elif variant == "cell":
            text = text.replace("CMS_GV,cms_gv,7", "CMS_GV,cms_gv")
        if variant != "missing":
            lineage.write_bytes(b"\xff" if variant == "encoding" else text.encode("utf-8"))
        written = cli(case, write_lock=True, bronze_lineage=lineage)
        checked = cli(case, bronze_lineage=lineage)
        source_checked = run_argv(
            [sys.executable, "-m", "scripts.acquisition.source_metadata", "--check", "--additions", str(case / FILES[2]), "--bronze-lineage", str(lineage)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if message:
            read_case = prepare(root, "lineage_readback_" + name)
            valid_document = read_json(read_case / FILES[2])
            valid_metadata = entry(valid_document, source_id)
            valid_id = {"CMS_GV": "cms_gv", "main-hai-pdc": "main_hai_pdc"}.get(source_id, "cms_legacy_9n3s_kdb3")
            valid_placements, valid_unresolved = derive([valid_id], read_json(read_case / FILES[0]))
            valid_metadata["dataset_ids"] = [valid_id]
            valid_metadata["dataset_ids_evidence"] = [{key: value for key, value in valid_placements[0].items() if key != "source_id"}]
            valid_document.update(source_metadata=[valid_metadata], dataset_id_placements=valid_placements, unplaced_dataset_ids=valid_unresolved)
            (read_case / FILES[2]).write_bytes(encoded_json(valid_document))
            (read_case / FILES[3]).unlink()
            direct_lineage = read_case / "direct.csv"
            direct_lineage.write_text(f"bronze_table,_source_id,_dataset_id,n\nsynthetic_direct,{source_id},{valid_id},7\n", encoding="utf-8")
            valid_control = cli(read_case, write_lock=True, bronze_lineage=direct_lineage)
            (read_case / FILES[2]).write_bytes(encoded_json(document))
            checked = cli(read_case, bronze_lineage=lineage)
            checks = [
                {"name": "readback_valid_control", "passed": valid_control.returncode == 0},
                {"name": "loader_refuses_intended_cause", "passed": checked.returncode == 2 and message in checked.stderr},
                {"name": "writer_refuses_intended_cause", "passed": written.returncode == 2 and message in written.stderr},
                {"name": "refused_write_creates_no_lock", "passed": not (case / FILES[3]).exists()},
                {"name": "source_check_refuses_same_cause", "passed": source_checked.returncode == 2 and message in source_checked.stderr},
            ]
        else:
            before = fingerprint(case / FILES[3]) if (case / FILES[3]).exists() else None
            repeated = cli(case, write_lock=True, bronze_lineage=lineage)
            checks = [
                {"name": "writer_and_readback_accept", "passed": written.returncode == checked.returncode == source_checked.returncode == 0},
                {
                    "name": "canonical_lock_written",
                    "passed": before is not None and read_json(case / FILES[3]) == additions_lock(document, read_json(case / FILES[0])),
                },
                {"name": "repeat_preserves_lock", "passed": repeated.returncode == 0 and before == fingerprint(case / FILES[3])},
            ]
        cases.append(
            {
                "case": "lineage_" + name,
                "passed": all(check["passed"] for check in checks),
                "checks": checks,
                "writer": process_evidence(written),
                "readback": process_evidence(checked),
                "source_check": process_evidence(source_checked),
                "fixture_sha256": fingerprint(fixture)[0],
            }
        )
    for name, route, message in (
        ("observed_derivation", "main-hai-pdc", None),
        ("unreviewed_derivation", "ACS", "acquired_via is not a reviewed route"),
    ):
        case = prepare(root, "lineage_" + name)
        document = read_json(case / FILES[2])
        metadata = entry(document, "CMS_GV")
        placements, unresolved = derive(["cms_gv"], read_json(case / FILES[0]))
        metadata["dataset_ids"] = ["cms_gv"]
        metadata["dataset_ids_evidence"] = [{key: value for key, value in placements[0].items() if key != "source_id"}]
        document.update(source_metadata=[metadata], dataset_id_placements=placements, unplaced_dataset_ids=unresolved)
        additions = case / FILES[2]
        additions.write_bytes(encoded_json(document))
        before = fingerprint(additions)
        lineage = case / "bronze_lineage.csv"
        text = "\n".join(line for line in fixture.read_text(encoding="utf-8").splitlines() if "synthetic_coverage_unknown" not in line) + "\n"
        lineage.write_text(text.replace("main-hai-pdc", route), encoding="utf-8")
        inventory = case / "inventory.csv"
        inventory.write_text("dataset_id\ncms_gv\n", encoding="utf-8")
        argv = [
            sys.executable,
            "-m",
            "scripts.acquisition.source_metadata",
            "--dataset-ids",
            str(inventory),
            "--additions",
            str(additions),
            "--bronze-lineage",
            str(lineage),
        ]
        result = run_argv(argv, cwd=REPO_ROOT, capture_output=True, text=True, timeout=120)
        if message:
            checks = [
                {"name": "derivation_refuses_intended_cause", "passed": result.returncode == 2 and message in result.stderr},
                {"name": "refused_derivation_preserves_additions", "passed": before == fingerprint(additions)},
            ]
        else:
            after = fingerprint(additions)
            repeated = run_argv(argv, cwd=REPO_ROOT, capture_output=True, text=True, timeout=120)
            records = read_json(additions)["source_metadata"][0]["dataset_ids_evidence"]
            checks = [
                {"name": "approved_route_derived", "passed": result.returncode == 0 and records[0].get("acquired_via") == route},
                {"name": "repeat_preserves_additions", "passed": repeated.returncode == 0 and after == fingerprint(additions)},
            ]
        cases.append(
            {
                "case": "lineage_" + name,
                "passed": all(check["passed"] for check in checks),
                "checks": checks,
                "process": process_evidence(result),
                "fixture_sha256": fingerprint(fixture)[0],
            }
        )
    return cases


def run_cases(root: Path) -> list[dict[str, Any]]:
    """Execute every scenario from the failure-mode list and return per-case assertions."""
    cases: list[dict[str, Any]] = []
    code_paths = [
        Path(__file__),
        Path(__file__).with_name("registry_additions.py"),
        Path(__file__).with_name("source_metadata.py"),
        Path(__file__).with_name("registry_additions_boundary.py"),
        REPO_ROOT / "tests/fixtures/registry_additions_bronze_lineage.csv",
    ]
    code_before = {str(path): fingerprint(path)[0] for path in code_paths}

    def record(name: str, result: CompletedProcess[str] | None, checks: list[dict[str, Any]]) -> None:
        """Store one scenario's sanitized process output and pass state."""
        output = {"stdout": sanitize(result.stdout), "stderr": sanitize(result.stderr), "exit_code": result.returncode} if result else None
        cases.append({"case": name, "passed": all(check["passed"] for check in checks), "checks": checks, "process": output})

    base_before = (fingerprint(CONFIG / FILES[0]), fingerprint(CONFIG / FILES[1]))
    registry = read_json(CONFIG / FILES[0])
    pinned = read_json(PINNED_BATCH)["registry_sha256"]
    base_hash = canonical_hash(registry)
    base_checks = [{"name": "matches_base_lock", "passed": base_hash == read_json(CONFIG / FILES[1])["registry_sha256"]}]
    # The legacy archive is private by design and never in Git, so a clean checkout (CI) cannot resolve the
    # pinned legacy registry; that one check is recorded as skipped there and always runs where the archive exists.
    if LEGACY_ARCHIVE.is_dir():
        base_checks.insert(0, {"name": "pinned_batch_version_resolves_exactly", "passed": canonical_hash(load_registry(expected_sha256=pinned)) == pinned})
    else:
        base_checks.insert(0, {"name": "pinned_batch_version_resolves_exactly", "passed": True, "skipped": "private legacy registry archive absent"})
    record("registry_versions_preserved", None, base_checks)
    case = prepare(root, "baseline_valid")
    result = cli(case)
    record(
        "baseline_valid",
        result,
        [
            {"name": "exit_code_0", "passed": result.returncode == 0},
            {"name": "lists_both_ids", "passed": "C258.80" in result.stdout and "C258.81" in result.stdout},
            {"name": "reports_source_metadata_counts", "passed": counts_match(result.stdout, read_json(CONFIG / FILES[2]))},
        ],
    )
    case = prepare(root, "idempotent_relock")
    before = fingerprint(case / FILES[3])
    first, second = cli(case, write_lock=True), cli(case, write_lock=True)
    checks = [
        {"name": "both_runs_exit_0", "passed": first.returncode == second.returncode == 0},
        {"name": "lock_bytes_unchanged", "passed": fingerprint(case / FILES[3]) == before},
    ]
    record("idempotent_relock", second, checks)
    for name, (change, drop_lock) in MUTATIONS.items():
        case = prepare(root, name)
        edit_additions(case, change, drop_lock)
        result = cli(case, write_lock=drop_lock)
        checks = rejection(result, case, expect_no_lock=drop_lock)
        if name in EXPECTED_MESSAGE:
            checks.append({"name": "names_the_failure", "passed": EXPECTED_MESSAGE[name] in result.stderr})
        record(name, result, checks)
    case = prepare(root, "stale_base_binding")
    for file_name in FILES[2:]:
        document = read_json(case / file_name)
        document["base_registry_sha256"] = "0" * 64
        (case / file_name).write_bytes(encoded_json(document))
    result = cli(case)
    record("stale_base_binding", result, rejection(result, case, expect_no_lock=False))
    case = prepare(root, "stale_lock_refused")
    before = fingerprint(case / FILES[3])
    edit_additions(case, MUTATIONS["tampered_additions"][0], drop_lock=False)
    result = cli(case, write_lock=True)
    record(
        "stale_lock_refused",
        result,
        [*rejection(result, case, expect_no_lock=False), {"name": "lock_unchanged", "passed": fingerprint(case / FILES[3]) == before}],
    )
    case = prepare(root, "sm_lock_without_section_ids")
    lock = read_json(case / FILES[3])
    lock.pop("source_metadata_source_ids")
    (case / FILES[3]).write_bytes(encoded_json(lock))
    result = cli(case)
    record(
        "sm_lock_without_section_ids",
        result,
        [*rejection(result, case, expect_no_lock=False), {"name": "names_source_metadata", "passed": "Source metadata IDs" in result.stderr}],
    )
    case = prepare(root, "sm_section_removed_lock_lists_it")
    edit_additions(case, lambda a: a.pop("source_metadata"), drop_lock=False)
    result = cli(case)
    record(
        "sm_section_removed_lock_lists_it",
        result,
        [*rejection(result, case, expect_no_lock=False), {"name": "names_source_metadata", "passed": "lists source metadata" in result.stderr}],
    )
    case = prepare(root, "sm_stale_lock_refused")
    before = fingerprint(case / FILES[3])
    edit_additions(case, MUTATIONS["sm_tampered_publisher"][0], drop_lock=False)
    result = cli(case, write_lock=True)
    record(
        "sm_stale_lock_refused",
        result,
        [*rejection(result, case, expect_no_lock=False), {"name": "lock_unchanged", "passed": fingerprint(case / FILES[3]) == before}],
    )
    case = prepare(root, "sm_section_absent_valid")
    edit_additions(case, lambda a: a.pop("source_metadata"), drop_lock=True)
    written = cli(case, write_lock=True)
    checked = cli(case)
    record(
        "sm_section_absent_valid",
        checked,
        [
            {"name": "write_and_check_exit_0", "passed": written.returncode == checked.returncode == 0},
            {"name": "lock_has_no_section_ids", "passed": "source_metadata_source_ids" not in read_json(case / FILES[3])},
            {"name": "reports_zero_sources", "passed": "Source metadata: 0 sources" in checked.stdout},
        ],
    )
    case = prepare(root, "lock_missing_only")
    (case / FILES[3]).unlink()
    result = cli(case)
    record("lock_missing_only", result, rejection(result, case, expect_no_lock=True))
    case = prepare(root, "both_missing")
    (case / FILES[2]).unlink()
    (case / FILES[3]).unlink()
    result = cli(case)
    record(
        "both_missing",
        result,
        [{"name": "exit_code_0", "passed": result.returncode == 0}, {"name": "reports_zero_additions", "passed": "0 user-added" in result.stdout}],
    )
    case = prepare(root, "malformed_json")
    (case / FILES[2]).write_text('{"additions_version": 1, "additions_version": 1}\n', encoding="utf-8")
    result = cli(case)
    record("malformed_json", result, rejection(result, case, expect_no_lock=False))
    # no case may touch the real base registry or its lock (cases work on copies).
    record(
        "base_registry_untouched",
        None,
        [
            {"name": "registry_bytes_unchanged", "passed": fingerprint(CONFIG / FILES[0]) == base_before[0]},
            {"name": "registry_lock_bytes_unchanged", "passed": fingerprint(CONFIG / FILES[1]) == base_before[1]},
        ],
    )
    from scripts.acquisition.source_metadata import html_attribution

    sample = (
        '<html><head><title>Nested &amp; page</title><script type="application/ld+json">'
        '{"@graph":[{"@type":["Organization"],"name":"Synthetic Federal Agency"}]}</script>'
        "<style>Hidden Agency</style></head><body><footer>Centers for Medicare "
        "<span>&amp; Medicaid <b>Services</b></span></footer><h1>Nested <em>heading</em></h1></body></html>"
    )
    pairs = html_attribution(sample)
    record(
        "publisher_nested_attribution",
        None,
        [
            {"name": "nested_footer_preserved", "passed": ("footer", "Centers for Medicare & Medicaid Services") in pairs},
            {"name": "nested_heading_preserved", "passed": ("heading", "Nested heading") in pairs},
            {"name": "structured_organization_preserved", "passed": ("jsonld:Organization", "Synthetic Federal Agency") in pairs},
            {"name": "title_entities_preserved", "passed": ("title", "Nested & page") in pairs},
            {"name": "style_not_visible", "passed": not any("Hidden Agency" in value for _, value in pairs)},
        ],
    )
    record(
        "publisher_invalid_jsonld",
        None,
        [
            {
                "name": "invalid_structure_disclosed",
                "passed": ("unsupported_jsonld", "Invalid JSON-LD") in html_attribution('<script type="application/ld+json">invalid</script>'),
            }
        ],
    )
    cases.extend(run_sensitivity_cases(root))
    case = prepare(root, "placement_cli_check")
    argv = [sys.executable, "-m", "scripts.acquisition.source_metadata", "--check", "--additions", str(case / FILES[2])]
    first = run_argv(argv, cwd=REPO_ROOT, capture_output=True, text=True, timeout=120)
    second = run_argv(argv, cwd=REPO_ROOT, capture_output=True, text=True, timeout=120)
    record(
        "placement_cli_check",
        second,
        [
            {"name": "repeat_check_identical", "passed": first.returncode == second.returncode == 0 and first.stdout == second.stdout},
        ],
    )
    for name in ("placement_changed_owner", "placement_changed_rule", "unplaced_changed_evidence"):
        case = prepare(root, "check_" + name)
        edit_additions(case, MUTATIONS[name][0], drop_lock=False)
        result = run_argv(
            [sys.executable, "-m", "scripts.acquisition.source_metadata", "--check", "--additions", str(case / FILES[2])],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=120,
        )
        record(
            "check_" + name,
            result,
            [
                {"name": "rejects_intended_placement_drift", "passed": result.returncode == 2 and EXPECTED_MESSAGE[name] in result.stderr},
            ],
        )
    case = prepare(root, "placement_cli_derive")
    document = read_json(case / FILES[2])
    inventory = case / "dataset_ids.csv"
    inventory.write_text(
        "dataset_id\n" + "\n".join(sorted(item["dataset_id"] for key in ("dataset_id_placements", "unplaced_dataset_ids") for item in document[key])) + "\n",
        encoding="utf-8",
    )
    argv = [sys.executable, "-m", "scripts.acquisition.source_metadata", "--dataset-ids", str(inventory), "--additions", str(case / FILES[2])]
    first = run_argv(argv, cwd=REPO_ROOT, capture_output=True, text=True, timeout=120)
    before = fingerprint(case / FILES[2])
    second = run_argv(argv, cwd=REPO_ROOT, capture_output=True, text=True, timeout=120)
    record(
        "placement_cli_derive",
        second,
        [
            {"name": "repeated_derivation_identical", "passed": first.returncode == second.returncode == 0 and fingerprint(case / FILES[2]) == before},
            {"name": "all_stored_placement_values_reproduced", "passed": read_json(case / FILES[2]) == document},
        ],
    )
    for label, values, message in [
        ("duplicate", "cms_gv\ncms_gv\n", "Duplicate placement dataset_id"),
        ("invalid", "Bad-ID\n", "Invalid placement dataset_id"),
    ]:
        inventory.write_text("dataset_id\n" + values, encoding="utf-8")
        before = fingerprint(case / FILES[2])
        result = run_argv(argv, cwd=REPO_ROOT, capture_output=True, text=True, timeout=120)
        record(
            "placement_csv_" + label,
            result,
            [
                {"name": "intended_input_refusal", "passed": result.returncode == 2 and message in result.stderr},
                {"name": "additions_not_rewritten", "passed": fingerprint(case / FILES[2]) == before},
            ],
        )
    cases.extend(run_lineage_cases(root))
    cases.extend(run_coverage_cases(root))
    record(
        "code_snapshot_preserved",
        None,
        [{"name": "implementation_and_fixture_hashes_unchanged", "passed": code_before == {str(path): fingerprint(path)[0] for path in code_paths}}],
    )
    return cases


# Single removals can leave another logical defense active. These outcomes are diagnostic
# evidence only; each affected invariant also has a grouped CLI or focused boundary probe.
SURVIVING_DEFENSES = {
    "sm_evidence_out_of_order": "Source dataset evidence differs",
    "sm_evidence_empty_rule": "Source dataset evidence differs",
    "sm_evidence_empty_detail": "Source dataset evidence differs",
    "sm_evidence_malformed_record": "Source dataset evidence differs",
    "sm_approval_incomplete": "Malformed registry additions",  # Caught exception mapped to a handled error; no invariant credit.
    "sm_evidence_lists_required": "one evidence record per dataset_id",
    "sm_publisher_evidence_not_object": "malformed publisher evidence",
}


def altered_cli(case: Path, mutations: list[str], boundary: str | None = None, write_lock: bool = True) -> CompletedProcess[str]:
    """Run fixed named substitutions through the CLI or one validation boundary.

    A focused probe stops after its guard accepts, without claiming a lock or
    whole-document acceptance. The child receives no source or callable expression.
    """
    call = BOUNDARY_RULES[boundary] if boundary is not None else ("write_lock" if write_lock else "check_lock")
    argv = [sys.executable, "-B", "-m", "scripts.acquisition.registry_additions_boundary", "--case", str(case), "--call", call]
    if boundary is not None:
        argv.extend(("--boundary", boundary))
    for name in mutations:
        argv.extend(("--mutation", name))
    return run_argv(argv, cwd=REPO_ROOT, capture_output=True, text=True, timeout=120)


def process_evidence(result: CompletedProcess[str]) -> dict[str, Any]:
    """Retain sanitized outputs and the exit code for each control and probe."""
    return {"exit_code": result.returncode, "stdout": sanitize(result.stdout), "stderr": sanitize(result.stderr)}


def probe_outcome(case: Path, mutations: list[str], boundary: str | None = None) -> dict[str, Any]:
    """Classify acceptance, surviving rejection and execution failure without inversion."""
    from scripts.acquisition.registry_additions import additions_lock, canonical_json

    inputs = {name: fingerprint(case / name)[0] for name in FILES[:3]}
    result = altered_cli(case, mutations, boundary)
    lock = case / FILES[3]
    evidence: dict[str, Any] = {"process": process_evidence(result), "inputs": inputs, "lock_created": lock.exists()}
    status = "execution_failure"
    if result.returncode == 2 and "error:" in result.stderr and "Traceback" not in result.stderr and not lock.exists():
        status = "surviving_defense"
    elif result.returncode == 0 and not result.stderr:
        if boundary is not None:
            if result.stdout == "boundary_accepted:" + boundary + "\n" and not lock.exists():
                status = "accepted"
        elif lock.exists():
            document = read_json(case / FILES[2])
            expected = canonical_json(additions_lock(document, read_json(case / FILES[0])))
            evidence["lock_is_canonical_and_bound"] = lock.read_text(encoding="utf-8") == expected
            evidence["lock_sha256"] = fingerprint(lock)[0]
            checked = altered_cli(case, mutations, write_lock=False)
            evidence["readback"] = process_evidence(checked)
            if evidence["lock_is_canonical_and_bound"] and checked.returncode == 0 and not checked.stderr and counts_match(checked.stdout, document):
                status = "accepted"
    evidence["status"] = status
    evidence["scope"] = "focused_guard_boundary" if boundary else "whole_document_cli"
    return evidence


def invariant_exposed(valid: dict[str, Any], invalid: dict[str, Any]) -> bool:
    """Require successful valid and malformed controls at the same declared boundary."""
    return valid["status"] == invalid["status"] == "accepted" and valid["scope"] == invalid["scope"]


def probe_pair(root: Path, label: str, name: str, mutations: list[str], boundary: str | None = None) -> dict[str, Any]:
    """Run every altered validator on fresh valid and malformed case copies."""
    good = prepare(root, label + "_valid")
    (good / FILES[3]).unlink()
    bad = prepare(root, label + "_invalid")
    edit_additions(bad, MUTATIONS[name][0], drop_lock=True)
    valid = probe_outcome(good, mutations, boundary)
    invalid = probe_outcome(bad, mutations, boundary)
    return {
        "mutation": name,
        "replacements": [{"needle": needle, "replacement": replacement} for needle, replacement in (PROBE_REPLACEMENTS[name] for name in mutations)],
        "boundary": boundary,
        "valid_control": valid,
        "invalid_control": invalid,
        "invariant_exposed": invariant_exposed(valid, invalid),
    }


def sensitivity_record(label: str, pair: dict[str, Any], checks: list[dict[str, Any]], purpose: str) -> dict[str, Any]:
    """Separate diagnostic observations from affirmative invariant coverage in artifacts."""
    return {"case": label, "passed": all(check["passed"] for check in checks), "checks": checks, "purpose": purpose, "sensitivity": pair}


def run_sensitivity_cases(root: Path) -> list[dict[str, Any]]:
    """Prove invariant sensitivity with positive controls and explicit overlapping-defense limits."""
    cases: list[dict[str, Any]] = []
    for name, _, _ in SENSITIVITY_RULES:
        pair = probe_pair(root, "removed_rule_" + name, name, [name])
        invalid = pair["invalid_control"]
        checks = [{"name": "valid_control_accepts_locks_and_reads_back", "passed": pair["valid_control"]["status"] == "accepted"}]
        if name in SURVIVING_DEFENSES:
            checks.append(
                {
                    "name": (
                        "caught exception mapped to a handled error"
                        if name == "sm_approval_incomplete"
                        else "replacement_defense_survives_without_invariant_claim"
                    ),
                    "passed": invalid["status"] == "surviving_defense"
                    and SURVIVING_DEFENSES[name] in invalid["process"]["stderr"]
                    and not pair["invariant_exposed"],
                }
            )
        elif name == "sm_evidence_count_mismatch":
            checks.append(
                {
                    "name": "strict_zip_execution_failure_is_not_sensitivity",
                    "passed": invalid["status"] == "execution_failure"
                    and "ValueError: zip()" in invalid["process"]["stderr"]
                    and not pair["invariant_exposed"],
                }
            )
        else:
            checks.append({"name": "malformed_input_accepts_locks_and_reads_back", "passed": pair["invariant_exposed"]})
        purpose = "single-removal diagnostic; surviving defenses and execution failures are not invariant proof"
        cases.append(sensitivity_record("rule_removal_" + name, pair, checks, purpose))
        if name in BOUNDARY_RULES:
            original = probe_pair(root, "boundary_original_" + name, name, [], boundary=name)
            focused = probe_pair(root, "boundary_removed_" + name, name, [name], boundary=name)
            focused["original_controls"] = original
            checks = [
                {"name": "original_valid_reaches_boundary", "passed": original["valid_control"]["status"] == "accepted"},
                {
                    "name": "original_invalid_rejected_for_intended_cause",
                    "passed": original["invalid_control"]["status"] == "surviving_defense"
                    and EXPECTED_MESSAGE[name] in original["invalid_control"]["process"]["stderr"],
                },
                {"name": "altered_valid_and_invalid_cross_guard", "passed": focused["invariant_exposed"]},
            ]
            cases.append(
                sensitivity_record(
                    "boundary_rule_removal_" + name,
                    focused,
                    checks,
                    "focused named require boundary only; downstream schema defenses remain active; no whole-document acceptance claim",
                )
            )
        elif name in SURVIVING_DEFENSES or name == "sm_evidence_count_mismatch":
            mutations = [name, "placement_rule"]
            if name == "sm_evidence_count_mismatch":
                mutations.append("relaxed_zip")
            grouped = probe_pair(root, "group_removed_" + name, name, mutations)
            checks = [
                {"name": "group_valid_accepts_locks_and_reads_back", "passed": grouped["valid_control"]["status"] == "accepted"},
                {"name": "group_invalid_accepts_locks_and_reads_back", "passed": grouped["invariant_exposed"]},
            ]
            cases.append(
                sensitivity_record(
                    "group_rule_removal_" + name,
                    grouped,
                    checks,
                    "logical source-evidence rule group: local guard plus tracked derivation equality; count group also removes strict zip",
                )
            )
    for label in ("both_controls", "invalid_only"):
        pair = probe_pair(root, "broken_validator_" + label, "sm_evidence_empty_rule", ["broken_" + label])
        checks = [
            {"name": "invalid_execution_failure_not_counted_as_sensitivity", "passed": pair["invalid_control"]["status"] == "execution_failure"},
            {"name": "no_invariant_claim", "passed": not pair["invariant_exposed"]},
            {
                "name": "valid_control_exposes_expected_breakage",
                "passed": pair["valid_control"]["status"] == ("execution_failure" if label == "both_controls" else "accepted"),
            },
        ]
        cases.append(sensitivity_record("broken_validator_" + label, pair, checks, "deliberate execution-error control proving false-positive prevention"))
    return cases


def main() -> None:
    """Run all scenarios into a new directory and write a hash-indexed artifact."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    status, cases, error = "blocked", [], None
    try:
        cases = run_cases(root)
        status = "passed" if all(case["passed"] for case in cases) else "failed"
    except (OSError, KeyError, ValueError, StopIteration, SubprocessError) as failure:
        error = sanitize(f"{type(failure).__name__}: {failure}")
    finally:
        implementation = Path(__file__).with_name("registry_additions.py")
        artifact = {
            "kind": "registry_additions_process_e2e",
            "status": status,
            "error": error,
            "completed_at_utc": datetime.now(UTC).isoformat(),
            "python_version": sys.version,
            "platform": sys.platform,
            "requirements_sha256": fingerprint(REPO_ROOT / "requirements.txt")[0],
            "project_configuration_sha256": fingerprint(REPO_ROOT / "pyproject.toml")[0],
            "runner_sha256": fingerprint(Path(__file__))[0],
            "boundary_implementation_sha256": fingerprint(Path(__file__).with_name("registry_additions_boundary.py"))[0],
            "placement_implementation_sha256": fingerprint(Path(__file__).with_name("source_metadata.py"))[0],
            "implementation_sha256": fingerprint(implementation)[0] if implementation.exists() else None,
            "inputs": {name: fingerprint(CONFIG / name)[0] if (CONFIG / name).exists() else None for name in FILES},
            "code_revision_note": "File hashes above identify the tested revision.",
            "cases": cases,
            "files": [{"path": str(p.relative_to(root)), "sha256": fingerprint(p)[0]} for p in sorted(root.rglob("*")) if p.is_file()],
            "reproduce": [sys.executable, "-m", "scripts.acquisition.run_registry_additions_e2e", "--output", "<new_directory>"],
            "prerequisites": ["Run from the repository root with the project .venv; config/acquisition must hold the four registry files."],
            "reset": "Use a new output directory; case directories hold copies, so real configuration is never modified.",
            "tested_boundary": "Copies of the real registry, lock, additions and additions lock through the actual additions CLI subprocess.",
            "untested_boundaries": [
                "Consumers other than the additions CLI",
                "Schema review use of added controls",
                "A deliberate edit of both additions and lock",
            ],
            "network_calls": 0,
            "aws_calls": 0,
            "cleanup": "Evidence and case copies retained locally under the output directory; no cloud resources touched.",
        }
        write_once(root / "artifact.json", encoded_json(artifact))
        sys.stdout.write(
            json.dumps({"status": status, "cases": len(cases), "artifact": sanitize(str(root / "artifact.json")), "sha256": canonical_hash(artifact)}) + "\n"
        )
    if status != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
