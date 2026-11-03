"""Synthetic child reports exercise the orchestration mutation reader without native workers."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from scripts.orchestration import guard_mutants
from scripts.orchestration import run_failing_first as reader
from tests.report_diagnostics import parse_cause_present
from tests.support import check

CASE = "requested_case"
INTENDED = "the guarded input is refused"
LATER = "later_failed_check_must_survive"
SOURCE = "guard_fixture.txt"
ORIGINAL = "guard_enabled = True\n"
CHANGED = "guard_enabled = False\n"
HEAD = "a" * 40
RUNNERS = ("broker", "supervisor", "stack")
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


def producer_report(runner: str, *, mutant: bool = False) -> dict[str, Any]:
    row = {
        "case": CASE,
        "passed": not mutant,
        "checks": {INTENDED: not mutant, reader.LEFTOVER: True},
        "error": "",
        "leftovers": [],
        "seconds": 0.1,
        "status": "fail" if mutant else "pass",
    }
    document = {
        "feature": f"Synthetic {runner} transport with the producer's case schema",
        "command": f".venv/bin/python -m scripts.orchestration.run_{runner}_e2e",
        "started_utc": "20261102T000000Z",
        "head": HEAD,
        "python": "3.12.0",
        "cases": [row],
        "passed": int(not mutant),
        "total": 1,
        "limits": ["Synthetic report transport only"],
    }
    if runner == "stack":
        document["launcher_sha256"] = "b" * 64
    return document


def control_input(runner: str, fault: str) -> tuple[dict[str, Any], int]:
    document = producer_report(runner)
    row = document["cases"][0]
    code = 0
    if fault == "wrong_name":
        row["case"] = "another_case"
    elif fault == "extra_failed_row":
        document["cases"].append({**row, "case": "later_case", "passed": False, "status": "fail", "checks": {LATER: False}})
        document["total"] = 2
    elif fault == "empty_checks":
        row["checks"] = {}
    elif fault in ("false_check", "integer_check", "string_check"):
        row["checks"][INTENDED] = {"false_check": False, "integer_check": 1, "string_check": "true"}[fault]
    elif fault == "row_error":
        row["error"] = "synthetic control setup error"
    elif fault == "row_passed_false":
        row["passed"] = False
    elif fault == "failing_status":
        row["status"] = "fail"
    elif fault == "summary_failed":
        document["passed"] = 0
    elif fault == "summary_total":
        document["total"] = 2
    elif fault == "summary_error":
        document["error"] = "synthetic control summary error"
    elif fault == "command_exit":
        code = 7
    else:
        raise ValueError(fault)
    return document, code


def mutant_input(runner: str, fault: str) -> tuple[dict[str, Any], int]:
    document = producer_report(runner, mutant=True)
    row = document["cases"][0]
    code = 1
    if fault == "wrong_name":
        row["case"] = "another_case"
    elif fault == "extra_failed_row":
        document["cases"].append({**row, "case": "later_case", "error": "synthetic later setup error", "checks": {LATER: False}})
        document["total"] = 2
    elif fault == "integer_check":
        row["checks"][INTENDED] = 0
    elif fault == "passing_status":
        row.update(status="pass", passed=True)
        document["passed"] = 1
    elif fault == "setup_error":
        row["error"] = "synthetic mutant setup error"
    elif fault == "summary_error":
        document["error"] = "synthetic mutant summary error"
    elif fault == "command_exit":
        code = 7
    else:
        raise ValueError(fault)
    return document, code


def read_report(monkeypatch: pytest.MonkeyPatch, tree: Path, runner: str, document: dict[str, Any], code: int) -> dict[str, Any]:
    calls: list[Path] = []

    def command(executable: str, arguments: list[str], *, cwd: Path, **options: Any) -> SimpleNamespace:
        check(executable == sys.executable, "the reader uses its Python interpreter")
        check(arguments == ["-m", f"scripts.orchestration.run_{runner}_e2e", "--case", CASE], "the real reader requests one named case")
        check(cwd == tree, "the report belongs to the requested fixture tree")
        calls.append(cwd)
        report = cwd / "data" / "e2e" / reader.RUNNERS[runner] / "synthetic" / "report.json"
        report.parent.mkdir(parents=True)
        report.write_text(json.dumps(document), encoding="utf-8")
        return SimpleNamespace(returncode=code, stdout="", stderr="synthetic command failed" if code else "")

    monkeypatch.setattr(reader, "run_command", command)
    result = reader.run_case(runner, CASE, tree)
    check(calls == [tree], "the real command transport was reached once")
    return result


@pytest.mark.parametrize("runner", RUNNERS)
def test_named_producer_control_and_deliberate_mutant(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, runner: str) -> None:
    control = read_report(monkeypatch, tmp_path / "control", runner, producer_report(runner), 0)
    mutant = read_report(monkeypatch, tmp_path / "mutant", runner, producer_report(runner, mutant=True), 1)
    check(control["passed"] is True and control["error"] == "", "the complete genuine producer shape is a usable control")
    check(control["checks"] == {INTENDED: True, reader.LEFTOVER: True}, "the reader retains passing checks")
    check(mutant["passed"] is False and mutant["error"] == "", "the expected exit 1 is an intentional assertion failure")
    check(reader.verdict(control, mutant, INTENDED)[0] is True, "the real verdict verifies the deliberate mutant")


@pytest.mark.parametrize("runner", RUNNERS)
@pytest.mark.parametrize("fault", CONTROL_FAULTS)
def test_control_refuses_inconsistent_report(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, runner: str, fault: str) -> None:
    document, code = control_input(runner, fault)
    result = read_report(monkeypatch, tmp_path / "control", runner, document, code)
    check(result["passed"] is False, f"control {fault} must remain a failed result")
    mutant = read_report(monkeypatch, tmp_path / "mutant", runner, producer_report(runner, mutant=True), 1)
    check(reader.verdict(result, mutant, INTENDED)[0] is False, f"control {fault} cannot verify a mutant")
    if fault == "extra_failed_row":
        check(LATER in json.dumps(result), "the later failed check remains in the refusal evidence")
    if fault == "row_error":
        check("synthetic control setup error" in json.dumps(result), "the command report's error is retained")
    if fault == "command_exit":
        check("7" in json.dumps(result), "the nonzero control exit remains in the refusal evidence")
    if fault == "summary_error":
        check("synthetic control summary error" in json.dumps(result), "the summary's error remains in the refusal evidence")


@pytest.mark.parametrize("runner", RUNNERS)
@pytest.mark.parametrize("fault", MUTANT_FAULTS)
def test_mutant_refuses_setup_or_inconsistent_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, runner: str, fault: str) -> None:
    control = read_report(monkeypatch, tmp_path / "control", runner, producer_report(runner), 0)
    document, code = mutant_input(runner, fault)
    mutant = read_report(monkeypatch, tmp_path / "mutant", runner, document, code)
    check(reader.verdict(control, mutant, INTENDED)[0] is False, f"mutant {fault} is not deliberate assertion evidence")


def prepare_main(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, runner: str, fault: str = "", phase: str = "") -> dict[str, Any]:
    root = tmp_path / "fixture_root"
    root.mkdir()
    source = root / SOURCE
    source.write_text(ORIGINAL, encoding="utf-8")
    outside = tmp_path / "unowned_keep.txt"
    outside.write_text("retain unowned fixture\n", encoding="utf-8")
    out, logs = root / "data" / "mutation_reports", root / "logs"
    mutant = guard_mutants.Mutant("synthetic_guard", "an inert fixture edit", (guard_mutants.Edit(SOURCE, ORIGINAL, CHANGED),))
    monkeypatch.setattr(guard_mutants, "MUTANTS", {mutant.name: mutant})
    monkeypatch.setattr(guard_mutants, "GUARDS", {runner: {CASE: (guard_mutants.Guard(mutant.name, INTENDED),)}})
    for name, value in {"ROOT": root, "OUT": out, "LOGS": logs, "SOURCE": (SOURCE,)}.items():
        monkeypatch.setattr(reader, name, value)
    monkeypatch.setattr(sys, "argv", ["run_failing_first", "--runner", runner, "--case", CASE, "--logs", str(logs)])
    calls: list[tuple[Path, str]] = []

    def command(executable: str, arguments: list[str], *, cwd: Path, **options: Any) -> SimpleNamespace:
        if executable == "git":
            check(arguments == ["rev-parse", "HEAD"] and cwd == root, "only the fixture revision command is synthesized")
            return SimpleNamespace(returncode=0, stdout=HEAD + "\n", stderr="")
        check(executable == sys.executable, "the source copy requests the existing interpreter")
        expected = ["-m", f"scripts.orchestration.run_{runner}_e2e", "--case", CASE]
        if runner == "offline":
            expected = ["-m", "scripts.orchestration.offline_checks", *[item for section in reader.OFFLINE_SECTIONS for item in ("--only", section)]]
        check(arguments == expected, "the outer path preserves the selected case or offline sections")
        check(cwd.resolve().is_relative_to(out.resolve()), "source generation stays beneath the fixture output")
        text = (cwd / SOURCE).read_text(encoding="utf-8")
        check(text in (ORIGINAL, CHANGED), "the real source copier and mutant produce the exact inert fixture bytes")
        mode = "control" if text == ORIGINAL else "mutant"
        calls.append((cwd, mode))
        document = offline_report(mutant=mode == "mutant") if runner == "offline" else producer_report(runner, mutant=mode == "mutant")
        payload = json.dumps(document)
        if mode == phase:
            if fault == "invalid_json":
                payload = "{invalid synthetic JSON"
            elif runner == "offline" and fault == "missing_ok":
                del document["results"][INTENDED]["ok"]
                payload = json.dumps(document)
            elif fault.startswith("missing_"):
                del document["cases"][0][fault.removeprefix("missing_")]
                payload = json.dumps(document)
            else:
                raise ValueError(fault)
        folder = "orchestration_offline" if runner == "offline" else reader.RUNNERS[runner]
        report = cwd / "data" / "e2e" / folder / "synthetic" / "report.json"
        report.parent.mkdir(parents=True)
        report.write_text(payload, encoding="utf-8")
        return SimpleNamespace(returncode=int(mode == "mutant"), stdout="", stderr="")

    monkeypatch.setattr(reader, "run_command", command)
    return {"root": root, "source": source, "outside": outside, "out": out, "logs": logs, "calls": calls}


def check_owned_cleanup(state: dict[str, Any]) -> None:
    check(not list(state["out"].glob("*/runs")), "owned scratch is removed even when report parsing fails")
    check(all(not tree.exists() for tree, _ in state["calls"]), "every generated source copy is removed")
    check(state["outside"].read_text(encoding="utf-8") == "retain unowned fixture\n", "cleanup preserves unowned paths")
    check(hashlib.sha256(state["source"].read_bytes()).digest() == hashlib.sha256(ORIGINAL.encode()).digest(), "fixture source bytes remain unchanged")


def offline_report(*, mutant: bool) -> dict[str, Any]:
    return {
        "feature": "orchestration offline checks",
        "started_utc": "2026-11-02T00:00:00+00:00",
        "status": "fail" if mutant else "pass",
        "commit": HEAD,
        "command": ".venv/bin/python -m scripts.orchestration.offline_checks --only mutations --only graph --only distribution",
        "platform_sha256": "b" * 64,
        "sections": list(reader.OFFLINE_SECTIONS),
        "results": {INTENDED: {"ok": not mutant, "problems": ["synthetic intended failure"] if mutant else []}},
        "pending_typechecks": {},
        "limits": ["Synthetic one-guard fixture group for cleanup coverage only"],
    }


@pytest.mark.parametrize("runner", RUNNERS)
def test_outer_main_verifies_named_case_and_cleans_owned_scratch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, runner: str) -> None:
    state = prepare_main(monkeypatch, tmp_path, runner)
    check(reader.main() == 0, "the outer runner accepts a healthy control and deliberate failed mutant")
    check([mode for _, mode in state["calls"]] == ["control", "mutant"], "both real reader paths run")
    check_owned_cleanup(state)
    reports = list(state["out"].glob("*/report.json"))
    check(len(reports) == 1, "the outer main persists one report")
    document = json.loads(reports[0].read_text())
    check(document["total"] == document["verified"] == 1, "the selected case is verified once")
    check(document["cases"][0]["case"] == CASE, "the selected name survives the outer summary")
    check(json.loads((state["logs"] / "summary.json").read_text()) == document, "both summary destinations retain the same result")
    log = (state["logs"] / runner / f"{CASE}.log").read_text()
    check(INTENDED in log and CHANGED.strip() in log, "the real mutation and intended check reach the case log")


@pytest.mark.parametrize("runner", RUNNERS)
@pytest.mark.parametrize("phase", ("control", "mutant"))
@pytest.mark.parametrize("fault", PARSE_FAULTS)
def test_outer_parse_failure_retains_failure_and_cleans_scratch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, runner: str, phase: str, fault: str) -> None:
    state = prepare_main(monkeypatch, tmp_path, runner, fault, phase)
    exit_code = None
    parse_error = None
    try:
        exit_code = reader.main()
    except (json.JSONDecodeError, KeyError, TypeError, IndexError) as error:
        parse_error = error
    check(exit_code != 0, f"a parser failure cannot return success: {parse_error}")
    check_owned_cleanup(state)
    check(any(mode == phase for _, mode in state["calls"]), "the malformed transport reaches the requested phase")
    reports = list(state["out"].glob("*/report.json"))
    check(len(reports) == 1, "a parse failure still persists a final failed report")
    document = json.loads(reports[0].read_text())
    check(document["verified"] == 0 and document["total"] == 1, "parse failures do not disappear from the selected case count")
    check(document["cases"] and document["cases"][0]["case"] == CASE, "the failed case is retained")
    check(exit_code == 1 or parse_error is not None, "a failed result or the original parse exception remains visible")
    logs = [path.read_text() for path in state["logs"].rglob("*.log")]
    check(parse_cause_present(document, logs, fault), "one persisted diagnostic identifies the exact parser failure")
    check(json.loads((state["logs"] / "summary.json").read_text()) == document, "parse failures reach the second summary destination")


def test_offline_outer_cleanup_positive_control(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    test_outer_main_verifies_named_case_and_cleans_owned_scratch(monkeypatch, tmp_path, "offline")


@pytest.mark.parametrize("phase", ("control", "mutant"))
@pytest.mark.parametrize("fault", ("invalid_json", "missing_ok"))
def test_offline_outer_parse_cleanup(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, phase: str, fault: str) -> None:
    test_outer_parse_failure_retains_failure_and_cleans_scratch(monkeypatch, tmp_path, "offline", phase, fault)


def test_source_edit_count_guard_preserves_fixture_hash(tmp_path: Path) -> None:
    source = tmp_path / SOURCE
    source.write_text(ORIGINAL, encoding="utf-8")
    before = hashlib.sha256(source.read_bytes()).digest()
    mutant = guard_mutants.Mutant("mismatch", "an invalid inert edit", (guard_mutants.Edit(SOURCE, "absent text", CHANGED),))
    with pytest.raises(guard_mutants.MutantError, match="expected 1 occurrence"):
        guard_mutants.apply(tmp_path, mutant)
    check(hashlib.sha256(source.read_bytes()).digest() == before, "a mismatched mutation cannot alter source bytes")


@pytest.mark.parametrize("runner", RUNNERS)
def test_command_exception_keeps_its_identity(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, runner: str) -> None:
    failure = OSError("synthetic command transport failure")

    def command(*arguments: Any, **options: Any) -> Any:
        raise failure

    monkeypatch.setattr(reader, "run_command", command)
    with pytest.raises(OSError) as caught:
        reader.run_case(runner, CASE, tmp_path)
    check(caught.value is failure, "the original transport exception is not replaced with intentional mutant failure")


@pytest.mark.parametrize("field", ["case", "error", "ok"])
def test_parse_cause_control_rejects_wrapper_words(field: str) -> None:
    document = {"case": "requested_case", "error": "required unrelated dependency missing"}
    logs = ["case broker/requested_case", "  error: required unrelated dependency missing", "  ok: successful peer"]
    check(not parse_cause_present(document, logs, f"missing_{field}"), "wrapper words must not name the missing field")
    check(parse_cause_present({"error": f"KeyError: '{field}'"}, [], f"missing_{field}"), "a persisted exact KeyError names its missing field")
    check(parse_cause_present({}, [f"  error: missing required field {field}"], f"missing_{field}"), "an error-line body can name the missing field")


def test_parse_cause_control_rejects_json_filenames() -> None:
    check(
        not parse_cause_present({"case": "report.json", "error": "unrelated refusal"}, ["case report.json"], "invalid_json"), "a filename is not a JSON error"
    )
    check(parse_cause_present({"error": "JSONDecodeError: Expecting value"}, [], "invalid_json"), "the original JSON parse cause remains valid")


@pytest.mark.parametrize("field", ["case", "status", "checks", "error", "seconds", "ok"])
@pytest.mark.parametrize("channel", ["structured", "log"])
def test_parse_cause_bracket_control(field: str, channel: str) -> None:
    def matches(body: str) -> bool:
        return parse_cause_present(
            {"reason": body} if channel == "structured" else {}, [] if channel == "structured" else [f"  error: {body}"], f"missing_{field}"
        )

    check(matches(f"required key: [{field}]"), "a retained single-key bracket diagnostic identifies its exact field")
    for name in (f"{field}-id", f"{field}.json", f"{field}_other", f"{field}, other", f"[{field}]"):
        check(not matches(f"required key: [{name}]"), "longer, multiple and nested bracket names remain distinct")
    check(not matches(f"KeyError: '[{field}]'"), "a quoted literal bracket key is not the bare field")
    check(not matches(f"required unrelated dependency missing while checking field '[{field}]'"), "an unrelated missing dependency is not a field diagnostic")
