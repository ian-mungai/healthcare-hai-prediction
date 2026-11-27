"""Step S4 is a minimal step: no file of any kind closes or reduces a readiness item, and each open item names the step it waits for.

The central case supplies perfect files of every kind, binds them with the lock command and runs the release check on the tree. The open items and the problem
count must equal the run on the committed files. A relabelled record, shaped placeholders and unrelated manual records change no count either.
"""

from __future__ import annotations

import io
import json
import re
from collections.abc import Callable
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any

from scripts.ingestion import registry
from tests.registry_perfect_support import EVIDENCE, FILES, IAM, RETENTION, add_evidence, add_manual, add_r4, bind, put, record_for
from tests.registry_support import edit_json, fresh_copy, replace, scope_report
from tests.support import check

NO_CLOSURES = frozenset({"I6"})


def release(root: Path) -> registry.Result:
    return registry.check(root, release=True, scope_report=scope_report(), skip=NO_CLOSURES)


def signature(result: registry.Result) -> tuple[list[str], list[str]]:
    """The problems and the open items: everything a closed item would change. Notes about supplied files are reported apart and are not part of it."""
    return sorted(str(problem) for problem in result.problems), list(result.readiness)


def test_perfect_files_of_every_kind_close_nothing(tmp_path: Path) -> None:
    """Central case: a perfect, bound completion record, inventory record, live check, manual outcome record, retention input and IAM plan evidence."""
    baseline = signature(release(fresh_copy(tmp_path / "committed")))
    check(len(baseline[0]) == 101, f"The committed files no longer give 101 problems: {len(baseline[0])}")
    root = fresh_copy(tmp_path / "perfect")
    add_evidence(root, "cms_hai_hospital")
    add_evidence(root, "illinois_report_card_csv")
    add_manual(root, collection="hrsa/shortage_areas")
    add_r4(root)
    bind(root)
    check(signature(release(root)) == baseline, "A perfect supplied file closed or changed a readiness item")
    report_file = tmp_path / "scope_report.json"
    report_file.write_bytes(registry.scope.report_bytes(scope_report()))
    output = io.StringIO()
    with redirect_stdout(output):
        code = registry.main(["check", "--release", "--root", str(root), "--scope-report", str(report_file)])
    check(code == 1 and "fail (101 problems)" in output.getvalue(), f"The command did not stay at exit 1 and 101 problems: {code}")


def test_files_that_only_look_right_change_no_count(tmp_path: Path) -> None:
    """The relabelled record, the shaped retention and IAM placeholders and 50 manual outcomes marked completed with unrelated records change no count."""
    baseline = signature(release(fresh_copy(tmp_path / "committed")))
    relabelled = fresh_copy(tmp_path / "relabelled")
    add_evidence(relabelled, "cms_hai_hospital", listed_name="Some_Other_Table_File.csv")
    bind(relabelled)
    check(signature(release(relabelled)) == baseline, "A record relabelled for another table closed an item")
    placeholders = fresh_copy(tmp_path / "placeholders")
    add_r4(placeholders, zeros=True)
    bind(placeholders)
    check(signature(release(placeholders)) == baseline, "Shaped zero-digest retention and IAM placeholders closed R4")
    manual = fresh_copy(tmp_path / "manual")
    add_manual(manual)
    bind(manual)
    check(signature(release(manual)) == baseline, "50 manual outcomes marked completed with unrelated records closed the manual item")


def test_every_open_item_names_the_step_it_waits_for(tmp_path: Path) -> None:
    """Each open item of the committed report says what it waits for: a step that produces the records, the lead or the owner."""
    lines = release(fresh_copy(tmp_path / "committed")).readiness
    for item in lines:
        check("waiting" in item, f"An open item does not say what it waits for: {item}")
    text = "\n".join(lines)
    for step in ("S7", "L5", "S11", "S9", "L3"):
        check(re.search(rf"\b{step}\b", text) is not None, f"No open item waits for {step}")
    check(RETENTION in text and IAM in text, "The two R4 inputs are not named")


SENTINEL = "SENTINEL_INPUT_VALUE_7f3a"


def faulty_record(table: str, change: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
    record, _, _ = record_for(table)
    change(record)
    return record


def supply_faulty(root: Path, kind: str) -> None:
    """Supply one malformed file of a kind, in the place a perfect one would go."""
    if kind == "completion record, a null stored name":
        record, _, _ = record_for("cms_hai_hospital")
        record["data_objects"].append({**record["data_objects"][0], "key": "cms/hospitals/datasets/x/b.csv", "stored_file_name": None})
        add_evidence(root, "cms_hai_hospital", record=record)
    elif kind == "completion record, schema_version true":
        add_evidence(root, "cms_hai_hospital", record=faulty_record("cms_hai_hospital", lambda record: record.update(schema_version=True)))
    elif kind == "completion record, an unknown property":
        add_evidence(root, "cms_hai_hospital", record=faulty_record("cms_hai_hospital", lambda record: record.update({SENTINEL: 1})))
    elif kind == "inventory record":
        add_evidence(root, "illinois_report_card_csv", record=faulty_record("illinois_report_card_csv", lambda record: record.update(root=SENTINEL)))
    elif kind == "live check, a text result":
        add_evidence(root, "cms_hai_hospital", live={"kind": "live_object_check", "schema_version": 1, "passed": SENTINEL, "objects": []})
    elif kind == "evidence entry, an unknown property":
        add_evidence(root, "cms_hai_hospital")
        edit_json(root, EVIDENCE, lambda document: document["tables"][0].update({SENTINEL: 1}))
    elif kind == "evidence pointer to a directory":
        add_evidence(root, "cms_hai_hospital")
        edit_json(root, EVIDENCE, lambda document: document["tables"][0]["record"].update(path="config/ingestion"))
    elif kind == "evidence record that is not JSON":
        add_evidence(root, "cms_hai_hospital")
        replace(root, f"{FILES}/cms_hai_hospital_record.json", SENTINEL.encode())
    elif kind == "manual outcome record":
        add_manual(root)
        replace(root, f"{FILES}/manual_00.json", json.dumps({"kind": "manual_outcome_record", SENTINEL: 1}).encode())
    elif kind == "retention input":
        add_r4(root)
        put(root, RETENTION, {"kind": "retention_input", SENTINEL: 1})
    else:
        add_r4(root)
        put(root, IAM, {"kind": "iam_plan_evidence", "status": SENTINEL})


KINDS = (
    "completion record, a null stored name",
    "completion record, schema_version true",
    "completion record, an unknown property",
    "inventory record",
    "live check, a text result",
    "evidence entry, an unknown property",
    "evidence pointer to a directory",
    "evidence record that is not JSON",
    "manual outcome record",
    "retention input",
    "IAM plan evidence",
)


def test_a_malformed_file_of_each_kind_is_a_named_shape_refusal_that_changes_nothing(tmp_path: Path) -> None:
    """No crash, no count change and no input value in any line: the refusal names positions only, and the report says shape refused."""
    baseline = signature(release(fresh_copy(tmp_path / "committed")))
    for number, kind in enumerate(KINDS):
        root = fresh_copy(tmp_path / f"case_{number}")
        supply_faulty(root, kind)
        bind(root)
        result = release(root)
        check(signature(result) == baseline, f"A malformed {kind} changed a readiness item")
        notes = "\n".join(result.notes)
        check("refused" in notes, f"A malformed {kind} was not reported as a shape refusal: {result.notes}")
        shown = notes + "\n" + registry.render(result, True)
        check(SENTINEL not in shown, f"A malformed {kind} leaked an input value into the report")
