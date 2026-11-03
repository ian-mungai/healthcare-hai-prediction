"""Synthetic broker reports exercise transform mutation acceptance and owned scratch cleanup."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from scripts.orchestration import run_transform_failing_first as reader
from tests.report_diagnostics import parse_cause_present
from tests.support import check

CASE = "requested_transform_case"
INTENDED = "the transform guard refuses the input"
LATER = "later_transform_failure_must_survive"
SOURCE = "transform_guard_fixture.txt"
ORIGINAL = "transform_guard = True\n"
CHANGED = "transform_guard = False\n"
CONTROL_FAULTS = (
    "wrong_name",
    "extra_failed_row",
    "empty_checks",
    "false_check",
    "integer_check",
    "string_check",
    "row_error",
    "row_passed_false",
    "failing_status",
    "summary_failed",
    "summary_total",
    "summary_error",
    "command_exit",
)
MUTANT_FAULTS = ("wrong_name", "extra_failed_row", "integer_check", "passing_status", "setup_error", "summary_error", "command_exit")
PARSE_FAULTS = ("invalid_json", "missing_case", "missing_status", "missing_checks", "missing_error", "missing_seconds")


def producer_report(*, mutant: bool = False) -> dict[str, Any]:
    return {
        "feature": "broker run-control rows and the shipped fixture rows, fake Docker, test registries",
        "command": ".venv/bin/python -m scripts.orchestration.run_transform_broker_e2e",
        "started_utc": "20261102T000000Z",
        "head": "a" * 40,
        "python": "3.12.0",
        "cases": [
            {
                "case": CASE,
                "passed": not mutant,
                "checks": {INTENDED: not mutant, reader.LEFTOVER: True},
                "error": "",
                "leftovers": [],
                "seconds": 0.1,
                "status": "fail" if mutant else "pass",
            }
        ],
        "passed": int(not mutant),
        "total": 1,
        "limits": ["Synthetic transport in the actual broker producer schema"],
    }


def control_input(fault: str) -> tuple[dict[str, Any], int]:
    document = producer_report()
    row = document["cases"][0]
    code = 0
    if fault == "wrong_name":
        row["case"] = "another_transform_case"
    elif fault == "extra_failed_row":
        document["cases"].append({**row, "case": "later_case", "passed": False, "status": "fail", "checks": {LATER: False}})
        document["total"] = 2
    elif fault == "empty_checks":
        row["checks"] = {}
    elif fault in ("false_check", "integer_check", "string_check"):
        row["checks"][INTENDED] = {"false_check": False, "integer_check": 1, "string_check": "true"}[fault]
    elif fault == "row_error":
        row["error"] = "synthetic transform control setup error"
    elif fault == "row_passed_false":
        row["passed"] = False
    elif fault == "failing_status":
        row["status"] = "fail"
    elif fault == "summary_failed":
        document["passed"] = 0
    elif fault == "summary_total":
        document["total"] = 2
    elif fault == "summary_error":
        document["error"] = "synthetic transform control summary error"
    elif fault == "command_exit":
        code = 7
    else:
        raise ValueError(fault)
    return document, code


def mutant_input(fault: str) -> tuple[dict[str, Any], int]:
    document = producer_report(mutant=True)
    row = document["cases"][0]
    code = 1
    if fault == "wrong_name":
        row["case"] = "another_transform_case"
    elif fault == "extra_failed_row":
        document["cases"].append({**row, "case": "later_case", "error": "synthetic later setup error", "checks": {LATER: False}})
        document["total"] = 2
    elif fault == "integer_check":
        row["checks"][INTENDED] = 0
    elif fault == "passing_status":
        row.update(status="pass", passed=True)
        document["passed"] = 1
    elif fault == "setup_error":
        row["error"] = "synthetic transform mutant setup error"
    elif fault == "summary_error":
        document["error"] = "synthetic transform mutant summary error"
    elif fault == "command_exit":
        code = 7
    else:
        raise ValueError(fault)
    return document, code


def read_report(monkeypatch: pytest.MonkeyPatch, tree: Path, document: dict[str, Any], code: int) -> dict[str, Any]:
    calls: list[Path] = []

    def command(executable: str, arguments: list[str], *, cwd: Path, **options: Any) -> SimpleNamespace:
        check(executable == sys.executable, "the transform reader uses its interpreter")
        check(arguments == ["-m", "scripts.orchestration.run_transform_broker_e2e", "--case", CASE], "the actual broker producer gets one named case")
        check(cwd == tree, "the report belongs to the requested fixture tree")
        calls.append(cwd)
        report = cwd / "data" / "e2e" / reader.REPORTS / "synthetic" / "report.json"
        report.parent.mkdir(parents=True)
        report.write_text(json.dumps(document), encoding="utf-8")
        return SimpleNamespace(returncode=code, stdout="", stderr="synthetic transform command failed" if code else "")

    monkeypatch.setattr(reader, "run_command", command)
    result = reader.run_case(tree, CASE)
    check(calls == [tree], "the real command transport was reached once")
    return result


def test_named_broker_producer_control_and_deliberate_mutant(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    control = read_report(monkeypatch, tmp_path / "control", producer_report(), 0)
    mutant = read_report(monkeypatch, tmp_path / "mutant", producer_report(mutant=True), 1)
    check(control["passed"] is True and control["error"] == "", "the actual broker producer's complete shape is a usable control")
    check(control["checks"] == {INTENDED: True, reader.LEFTOVER: True}, "the reader retains passing checks")
    check(mutant["passed"] is False and mutant["error"] == "", "the expected exit 1 remains deliberate assertion failure")
    check(reader.verdict(control, mutant, INTENDED)[0] is True, "the real verdict verifies the deliberately failed mutant")


@pytest.mark.parametrize("fault", CONTROL_FAULTS)
def test_control_refuses_inconsistent_report(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fault: str) -> None:
    document, code = control_input(fault)
    result = read_report(monkeypatch, tmp_path / "control", document, code)
    check(result["passed"] is False, f"control {fault} must remain a failed result")
    mutant = read_report(monkeypatch, tmp_path / "mutant", producer_report(mutant=True), 1)
    check(reader.verdict(result, mutant, INTENDED)[0] is False, f"control {fault} cannot verify a mutant")
    if fault == "extra_failed_row":
        check(LATER in json.dumps(result), "the later transform failure remains in the refusal evidence")
    if fault == "row_error":
        check("synthetic transform control setup error" in json.dumps(result), "the control's error is retained")
    if fault == "command_exit":
        check("7" in json.dumps(result), "the nonzero control exit remains in the refusal evidence")
    if fault == "summary_error":
        check("synthetic transform control summary error" in json.dumps(result), "the summary's error remains in the refusal evidence")


@pytest.mark.parametrize("fault", MUTANT_FAULTS)
def test_mutant_refuses_setup_or_inconsistent_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fault: str) -> None:
    control = read_report(monkeypatch, tmp_path / "control", producer_report(), 0)
    document, code = mutant_input(fault)
    mutant = read_report(monkeypatch, tmp_path / "mutant", document, code)
    check(reader.verdict(control, mutant, INTENDED)[0] is False, f"mutant {fault} is not deliberate assertion evidence")


def prepare_main(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fault: str = "", phase: str = "") -> dict[str, Any]:
    root = tmp_path / "fixture_root"
    root.mkdir()
    source = root / SOURCE
    source.write_text(ORIGINAL, encoding="utf-8")
    outside = tmp_path / "unowned_keep.txt"
    outside.write_text("retain unowned transform fixture\n", encoding="utf-8")
    out, logs = root / "data" / "transform_mutation_reports", root / "logs"
    mutant = reader.Mutant("an inert transform fixture edit", (reader.Edit(SOURCE, ORIGINAL, CHANGED),))
    monkeypatch.setattr(reader, "MUTANTS", {"synthetic_guard": mutant})
    monkeypatch.setattr(reader, "GUARDS", (reader.Guard(CASE, "synthetic_guard", INTENDED),))
    for name, value in {"ROOT": root, "OUT": out, "LOGS": logs, "SOURCE": (SOURCE,)}.items():
        monkeypatch.setattr(reader, name, value)
    monkeypatch.setattr(sys, "argv", ["run_transform_failing_first", "--case", CASE])
    calls: list[tuple[Path, str]] = []

    def command(executable: str, arguments: list[str], *, cwd: Path, **options: Any) -> SimpleNamespace:
        check(executable == sys.executable, "the source copy requests the existing interpreter")
        check(arguments == ["-m", "scripts.orchestration.run_transform_broker_e2e", "--case", CASE], "the main path selects the broker fixture producer")
        check(cwd.resolve().is_relative_to(out.resolve()), "source generation stays beneath the fixture output")
        text = (cwd / SOURCE).read_text(encoding="utf-8")
        check(text in (ORIGINAL, CHANGED), "the real copier and mutation create the exact inert fixture bytes")
        mode = "control" if text == ORIGINAL else "mutant"
        calls.append((cwd, mode))
        document = producer_report(mutant=mode == "mutant")
        payload = json.dumps(document)
        if mode == phase:
            if fault == "invalid_json":
                payload = "{invalid synthetic transform JSON"
            elif fault.startswith("missing_"):
                del document["cases"][0][fault.removeprefix("missing_")]
                payload = json.dumps(document)
            else:
                raise ValueError(fault)
        report = cwd / "data" / "e2e" / reader.REPORTS / "synthetic" / "report.json"
        report.parent.mkdir(parents=True)
        report.write_text(payload, encoding="utf-8")
        return SimpleNamespace(returncode=int(mode == "mutant"), stdout="", stderr="")

    monkeypatch.setattr(reader, "run_command", command)
    return {"root": root, "source": source, "outside": outside, "out": out, "logs": logs, "calls": calls}


def check_owned_cleanup(state: dict[str, Any]) -> None:
    check(not list(state["out"].glob("*/runs")), "owned transform scratch is removed even after parse failures")
    check(all(not tree.exists() for tree, _ in state["calls"]), "every generated transform source copy is removed")
    check(state["outside"].read_text(encoding="utf-8") == "retain unowned transform fixture\n", "cleanup preserves unowned paths")
    check(hashlib.sha256(state["source"].read_bytes()).digest() == hashlib.sha256(ORIGINAL.encode()).digest(), "fixture source bytes remain unchanged")


def test_outer_main_verifies_named_case_and_cleans_owned_scratch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    state = prepare_main(monkeypatch, tmp_path)
    check(reader.main() == 0, "the outer main verifies the healthy control and deliberate mutant")
    check([mode for _, mode in state["calls"]] == ["control", "mutant"], "both real reader paths run")
    check_owned_cleanup(state)
    reports = list(state["out"].glob("*/report.json"))
    check(len(reports) == 1, "the outer main persists one transform report")
    document = json.loads(reports[0].read_text())
    check(len(document["guards"]) == 1, "one selected transform guard is retained")
    check(document["guards"][0]["verified"] is True and document["guards"][0]["case"] == CASE, "the named guard is verified")
    log = (state["logs"] / f"{CASE}__synthetic_guard.log").read_text()
    check(INTENDED in log and CHANGED.strip() in log, "the real mutation and intended check reach the case log")


@pytest.mark.parametrize("phase", ("control", "mutant"))
@pytest.mark.parametrize("fault", PARSE_FAULTS)
def test_outer_parse_failure_retains_failure_and_cleans_scratch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, phase: str, fault: str) -> None:
    state = prepare_main(monkeypatch, tmp_path, fault, phase)
    exit_code = None
    parse_error = None
    try:
        exit_code = reader.main()
    except (json.JSONDecodeError, KeyError, TypeError, IndexError) as error:
        parse_error = error
    check(exit_code != 0, f"a parser failure cannot return success: {parse_error}")
    check_owned_cleanup(state)
    check(any(mode == phase for _, mode in state["calls"]), "the malformed report reaches the requested phase")
    reports = list(state["out"].glob("*/report.json"))
    check(len(reports) == 1, "a parse failure still persists a failed transform report")
    document = json.loads(reports[0].read_text())
    check(document["guards"] and document["guards"][0]["case"] == CASE, "the failed guard remains present")
    check(all(row["verified"] is False for row in document["guards"]), "no parse failure can verify a guard")
    check(exit_code == 1 or parse_error is not None, "a failed result or the original parse exception remains visible")
    logs = [path.read_text() for path in state["logs"].rglob("*.log")]
    check(parse_cause_present(document, logs, fault), "one persisted diagnostic identifies the exact parser failure")


def test_source_edit_count_guard_preserves_fixture_hash(tmp_path: Path) -> None:
    source = tmp_path / SOURCE
    source.write_text(ORIGINAL, encoding="utf-8")
    before = hashlib.sha256(source.read_bytes()).digest()
    mutant = reader.Mutant("an invalid inert edit", (reader.Edit(SOURCE, "absent text", CHANGED),))
    with pytest.raises(SystemExit, match="mutant edit does not match"):
        reader.apply(tmp_path, mutant)
    check(hashlib.sha256(source.read_bytes()).digest() == before, "a mismatched mutation cannot alter source bytes")


def test_command_exception_keeps_its_identity(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    failure = OSError("synthetic transform command transport failure")

    def command(*arguments: Any, **options: Any) -> Any:
        raise failure

    monkeypatch.setattr(reader, "run_command", command)
    with pytest.raises(OSError) as caught:
        reader.run_case(tmp_path, CASE)
    check(caught.value is failure, "the original transport exception is not replaced with intentional mutant failure")
