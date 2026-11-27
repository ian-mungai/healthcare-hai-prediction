"""Release readiness (design v4 section 5.1, cases 6 and 10): the report names every open item and the step it waits for.

Step S4 is a minimal step: no file closes an item. The evidence rules wait for the steps that produce real records.
Case 10 runs the command on the committed files.
"""

from __future__ import annotations

import io
import re
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any

from scripts.ingestion import registry, registry_readiness
from scripts.ingestion import registry_records as records
from scripts.ingestion.registry_model import Tree, default_scope
from tests.registry_records_support import DECISIONS, TABLES, digest, synthetic
from tests.registry_support import scope_report
from tests.support import check


def assess(built: Tree, report: dict[str, Any]) -> tuple[registry_readiness.Assessment, list[str]]:
    """The readiness report of a synthetic tree, as the check builds it: the obligations and the shape lines."""
    scope_tables = default_scope(report)
    assessment = registry_readiness.obligations(built.units, built.tables, scope_tables, [], {})
    return assessment, registry_readiness.observations(built, scope_tables, records.Schemas(registry.ROOT))


def test_hrsa_with_the_right_role_and_no_evidence_passes_check_and_fails_release_by_table(tmp_path: Path) -> None:
    """Case 6: both tables select their file (check passes the route predicates), no completion and no object evidence exist, so release fails by table name."""
    built, report = synthetic(tmp_path)
    outputs = registry.unit_outputs(built)
    routing, _ = registry.route_problems(built, outputs, registry.selections(built, outputs), set())
    check(not routing, f"The synthetic tables do not select their files: {routing}")
    assessment, _ = assess(built, report)
    found = sorted(problem.subject for problem in assessment.problems if problem.predicate == "R1")
    check(found == sorted(TABLES), f"Release did not fail by table name: {assessment.problems}")
    check(all("waiting for" in problem.message for problem in assessment.problems if problem.predicate == "R1"), "An R1 problem names no step")
    check(any(all(name in item for name in TABLES) for item in assessment.items), f"The READINESS items do not name the tables: {assessment.items}")


def test_release_on_the_committed_files_exits_1_and_lists_the_open_items(tmp_path: Path) -> None:
    """Case 10: check passes on the committed files with no byte changed; check --release exits 1 and lists the open items by name."""
    report_file = tmp_path / "scope_report.json"
    report_file.write_bytes(registry.scope.report_bytes(scope_report()))
    before = {name: digest((registry.ROOT / name).read_bytes()) for name in registry.bound_paths(registry.ROOT, registry.classified_plans(registry.ROOT).names)}
    plain, release = io.StringIO(), io.StringIO()
    with redirect_stdout(plain):
        plain_code = registry.main(["check", "--scope-report", str(report_file)])
    with redirect_stdout(release):
        release_code = registry.main(["check", "--release", "--scope-report", str(report_file)])
    after = {name: digest((registry.ROOT / name).read_bytes()) for name in registry.bound_paths(registry.ROOT, registry.classified_plans(registry.ROOT).names)}
    check(before == after, "The check changed a bound file")
    check(
        plain_code == 0 and "READINESS" in plain.getvalue() and "pass" in plain.getvalue(),
        f"check failed on the committed files: {plain_code} {plain.getvalue()[-600:]}",
    )
    check(re.search(r"\bready\b", plain.getvalue(), re.IGNORECASE) is None, "check printed the word ready")
    text = release.getvalue()
    check(release_code == 1 and "fail (101 problems)" in text, f"check --release did not exit 1 with 101 problems: {release_code}")
    for name in ("hrsa_hpsa_detail", "hrsa_mua_detail", "hcai_finance_csv", "illinois_report_card_csv", "D3", "pending_owner", "R4", "layout", "I13"):
        check(name in text, f"The open item {name} is not listed:\n{text[-1500:]}")


def test_a_recorded_decision_closes_nothing_and_an_undated_one_is_only_noted(tmp_path: Path) -> None:
    """R3 stays open whatever the file records: a dated decision closes nothing, and a value with no record date appears in a shape line only."""
    built, report = synthetic(tmp_path)
    dated, dated_notes = assess(built, report)
    check([p for p in dated.problems if p.predicate == "R3" and p.subject == "D1"], "A dated decision closed R3 for D1")
    check(not [note for note in dated_notes if "D1 has" in note], f"A dated decision was noted as unrecorded: {dated_notes}")
    built.collections["decisions"]["D1"] = {**DECISIONS["D1"], "recorded": None}
    undated, undated_notes = assess(built, report)
    check(list(map(str, undated.problems)) == list(map(str, dated.problems)), "An undated decision changed the readiness problems")
    found = [note for note in undated_notes if "D1 has a value and no record date" in note]
    check(len(found) == 1 and found[0].startswith("shape:"), f"The undated decision was not noted in one shape line: {undated_notes}")
