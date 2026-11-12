"""The startup check runs in a child process and prints only what it validates: every fault in the child stays out of the job output.

The child is the real isolated Python process. Its sockets, files and faults are synthetic. A marker stands for any configured value or secret.
"""

from __future__ import annotations

import ast
import json
import logging
import os
import sys
import time
from pathlib import Path

import pytest

from scripts.infrastructure import startup_check
from scripts.process import SESSION_VARIABLE
from tests.startup_check_faults import MARKER
from tests.startup_check_support import host_environment
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
FAULT_CHILD = "tests.startup_check_child_faults"
GOOD = [
    {"k": "f", "c": "credentials", "o": True, "d": [["p", "native task provider"]]},
    {"k": "s", "j": "silver", "l": "environment", "n": 1, "x": 0, "t": 0.25},
    {"k": "e"},
]


@pytest.fixture
def isolated(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run the real child process, with the fault entry in its place and the fault name passed through the explicit environment."""
    host_environment(monkeypatch)
    monkeypatch.setattr(startup_check, "ISOLATED", True)
    monkeypatch.setattr(startup_check, "CHILD_MODULE", FAULT_CHILD)
    monkeypatch.setattr(startup_check, "CHILD_ENV_EXTRA", ("STARTUP_CHECK_TEST_FAULT", "STARTUP_CHECK_TEST_PROBE"))


def run_fault(monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture, fault: str) -> tuple[str, str]:
    """Run the environment check with one fault in the child. Return everything the parent showed and the text of the error it raised."""
    monkeypatch.setenv("STARTUP_CHECK_TEST_FAULT", fault)
    caplog.set_level(logging.DEBUG)
    raised = ""
    try:
        startup_check.check_environment("silver", polaris=True)
    except startup_check.StartupCheckError as error:
        raised = str(error)
    captured = capfd.readouterr()
    return captured.out + captured.err + caplog.text, raised


@pytest.mark.parametrize("fault", ["log", "spill", "faulthandler", "excepthook", "exit_write", "file_log"])
def test_a_fault_in_the_child_never_reaches_the_job_output(
    isolated: None, monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture, tmp_path: Path, fault: str
) -> None:
    monkeypatch.chdir(tmp_path)
    shown, raised = run_fault(monkeypatch, capfd, caplog, fault)
    check(MARKER not in shown + raised, f"{fault}: the marker reached the parent: {(shown + raised)[:800]}")
    check("startup_check: silver environment:" in shown, f"{fault}: the validated result is shown: {shown[:600]}")
    check(not list(tmp_path.glob("*.log")), "no dependency file lands in the job folder")


def test_an_exception_whose_text_conversion_writes_is_never_converted(
    isolated: None, monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    shown, raised = run_fault(monkeypatch, capfd, caplog, "exception_str")
    check(MARKER not in shown + raised, f"the marker reached the parent: {(shown + raised)[:800]}")
    check("credentials task_credential_source_refused" in shown, f"the fixed code of the refusal is shown: {shown[:600]}")


@pytest.mark.parametrize("fault", ["crash", "garbage_line", "free_text", "unknown_code", "no_end"])
def test_a_child_that_crashes_or_reports_anything_else_ends_as_the_internal_error(
    isolated: None, monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture, fault: str
) -> None:
    shown, raised = run_fault(monkeypatch, capfd, caplog, fault)
    check(MARKER not in shown + raised, f"{fault}: the marker reached the parent: {(shown + raised)[:800]}")
    check(
        "startup_check_internal_error" in raised and "startup_check: FAILED startup_check_internal_error" in shown,
        f"{fault}: the fixed code is shown: {shown[:400]}",
    )


def test_a_child_that_hangs_is_stopped_at_its_bound(
    isolated: None, monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(startup_check, "CHILD_SECONDS", 3.0)
    started = time.monotonic()
    shown, raised = run_fault(monkeypatch, capfd, caplog, "hang")
    check(time.monotonic() - started < 30, "the wait is bounded")
    check("startup_check_internal_error" in raised, f"the timeout ends as the internal error: {raised} {shown[:300]}")


def test_the_child_gets_a_minimal_environment_in_a_private_folder(
    isolated: None, monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture, tmp_path: Path
) -> None:
    probe = tmp_path / "probe.json"
    monkeypatch.setenv("STARTUP_CHECK_TEST_PROBE", str(probe))
    monkeypatch.setenv("SYNTHETIC_PARENT_ONLY", MARKER)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "")
    monkeypatch.chdir(tmp_path)
    run_fault(monkeypatch, capfd, caplog, "environment_probe")
    seen = json.loads(probe.read_text())
    check("SYNTHETIC_PARENT_ONLY" not in seen["names"], f"a variable outside the list reached the child: {seen['names']}")
    check(
        set(seen["names"])
        <= set(startup_check.child_environment_names()) | {"PATH", "HOME", "TMPDIR", "PYTHONPATH", SESSION_VARIABLE, "__CF_USER_TEXT_ENCODING", "LC_CTYPE"},
        f"{seen['names']}",
    )
    check(
        seen["safe_path"] == 1 and seen["no_user_site"] == 1 and seen["dont_write_bytecode"] == 1,
        f"the child runs without the current folder, user site and bytecode: {seen}",
    )
    check(Path(seen["cwd"]).resolve() != tmp_path.resolve() and seen["home"] == seen["tmp"], f"the child works in its own private folder: {seen}")
    check(not Path(seen["cwd"]).exists(), "the private folder is removed afterwards")


def test_the_check_outside_a_task_starts_no_child(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("AWS_EXECUTION_ENV", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "ECS_CONTAINER_METADATA_URI_V4", "ECS_CONTAINER_METADATA_URI"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(startup_check, "ISOLATED", True)
    monkeypatch.setattr(startup_check, "CHILD_MODULE", "tests.no_such_child")
    startup_check.check_environment("silver", polaris=True)
    startup_check.check_files("publisher", [])


def lines_of(records: list[dict]) -> list[str]:
    return [json.dumps(record) for record in records]


def test_a_well_formed_transcript_parses() -> None:
    parsed = startup_check.parse_records(lines_of(GOOD))
    check([item["k"] for item in parsed] == ["f", "s", "e"], f"{parsed}")


def altered(index: int, **changes: object) -> list[str]:
    records = [dict(item) for item in GOOD]
    records[index].update(changes)
    return lines_of(records)


HOSTILE = {
    "not json": ["{not json " + MARKER],
    "not an object": ["[1]", json.dumps(GOOD[2])],
    "an unknown code": altered(0, c=MARKER),
    "an extra key": altered(0, extra=MARKER),
    "a missing key": lines_of([{"k": "f", "c": "credentials", "o": True}, *GOOD[1:]]),
    "a flag that is not a boolean": altered(0, o=1),
    "free text in a phrase": altered(0, d=[["p", "native provider " + MARKER]]),
    "an unknown part": altered(0, d=[["z", MARKER]]),
    "a part that is not a list": altered(0, d=["native task provider"]),
    "a detail that is not a list": altered(0, d="native task provider"),
    "a variable off the list": altered(0, d=[["v", MARKER]]),
    "a label off its list": altered(0, d=[["l", "exec", MARKER]]),
    "a list that does not exist": altered(0, d=[["l", MARKER, "x"]]),
    "a mode off the list": altered(0, d=[["m", MARKER]]),
    "a file kind off the list": altered(0, d=[["f", MARKER, "0123456789ab"]]),
    "a file digest of the wrong shape": altered(0, d=[["f", "plan", MARKER]]),
    "a host that is not an address": altered(0, d=[["h", "ip", MARKER, 443]]),
    "a host with a scope": altered(0, d=[["h", "ip", "fe80::1%" + MARKER, 443]]),
    "an endpoint off the list": altered(0, d=[["h", "aws", "evil." + MARKER + ".example.com", 443]]),
    "a digest of the wrong shape": altered(0, d=[["h", "digest", MARKER, 443]]),
    "a port out of range": altered(0, d=[["h", "ip", "192.0.2.10", 99999]]),
    "a port that is text": altered(0, d=[["h", "ip", "192.0.2.10", MARKER]]),
    "a count that is text": altered(0, d=[["n", MARKER]]),
    "a count that is a boolean": altered(0, d=[["n", True]]),
    "a count out of range": altered(0, d=[["n", 10**12]]),
    "seconds that are not a number": altered(0, d=[["t", MARKER]]),
    "an error name off the list": altered(0, d=[["r", MARKER]]),
    "a summary job off the list": altered(1, j=MARKER),
    "a summary label off the list": altered(1, l=MARKER),
    "a summary count that is text": altered(1, n=MARKER),
    "no end": lines_of(GOOD[:2]),
    "records after the end": lines_of([*GOOD, GOOD[0]]),
    "two ends": lines_of([*GOOD, GOOD[2]]),
    "two summaries": lines_of([GOOD[0], GOOD[1], GOOD[1], GOOD[2]]),
    "no summary": lines_of([GOOD[0], GOOD[2]]),
    "an empty transcript": [],
    "a line that is too long": [json.dumps({"k": "f", "c": "credentials", "o": True, "d": [["p", "x" * 10000]]}), *lines_of(GOOD[1:])],
    "a flood of records": lines_of([GOOD[0]] * 100000 + GOOD[1:]),
    "a non finite number": [json.dumps(GOOD[0]), '{"k": "s", "j": "silver", "l": "environment", "n": 1, "x": 0, "t": NaN}', json.dumps(GOOD[2])],
}


@pytest.mark.parametrize("name", list(HOSTILE))
def test_the_parent_rejects_anything_but_a_strictly_valid_transcript(name: str) -> None:
    with pytest.raises(startup_check.InvalidRecords) as caught:
        startup_check.parse_records(HOSTILE[name])
    check(MARKER not in str(caught.value), f"the rejection does not repeat the input: {caught.value}")


def test_every_fixed_phrase_the_checks_use_is_on_the_list() -> None:
    """The child may only choose a phrase from the parent's list, so a phrase that is used but not listed would end its check as an internal error."""
    tree = ast.parse(Path(startup_check.__file__).read_text())
    used = {
        node.args[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "phrase" and isinstance(node.args[0], ast.Constant)
    }
    check(used <= startup_check.PHRASES, f"phrases used but not listed: {sorted(used - startup_check.PHRASES)}")
    check(used | {startup_check.SPILL_CODE} >= startup_check.PHRASES, f"phrases listed but never used: {sorted(startup_check.PHRASES - used)}")


def test_the_known_modes_cover_every_mode_the_harness_lists() -> None:
    from scripts.acquisition import redownload, stored_mirror

    listed = (
        set(redownload.PLANNED) | set(stored_mirror.PLAN_MODES) | set(redownload.PLAIN_TASK_MODES) | set(redownload.SECRET_MODES) | set(redownload.MANUAL_MODES)
    )
    check(listed <= startup_check.KNOWN_MODES, f"modes the check would show as a digest: {sorted(listed - startup_check.KNOWN_MODES)}")


def test_the_parent_keeps_its_own_streams_and_never_prints_what_the_child_wrote(
    isolated: None, monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("STARTUP_CHECK_TEST_FAULT", "spill")
    startup_check.check_environment("silver", polaris=True)
    captured = capfd.readouterr()
    check(MARKER not in captured.out + captured.err and sys.stdout is not None and captured.out == "", f"{captured.out[:200]} {captured.err[:300]}")
    check(os.environ.get("STARTUP_CHECK_TEST_FAULT") == "spill", "the parent environment is untouched")


def test_the_production_child_script_runs_end_to_end_without_a_network(
    monkeypatch: pytest.MonkeyPatch, capfd: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    """The real child module as the production command starts it. File checks and a features build need no socket; a missing file is named by digest."""
    host_environment(monkeypatch)
    monkeypatch.setattr(startup_check, "ISOLATED", True)
    monkeypatch.chdir(tmp_path)
    present = tmp_path / "present.json"
    present.write_text("{}")
    needs = [startup_check.Need("hcai_util_workbook", "code", "present.json"), startup_check.Need("hcai_util_workbook", "code", "absent.json", kind="plan")]
    with pytest.raises(startup_check.StartupCheckError, match="input_file_missing"):
        startup_check.check_files("publisher", needs)
    shown = capfd.readouterr().err
    check(f"ok input_file hcai_util_workbook code root: input {startup_check.file_digest('present.json')}" in shown, f"the present file is shown: {shown}")
    check(
        f"FAILED input_file_missing hcai_util_workbook code root: plan {startup_check.file_digest('absent.json')}" in shown,
        f"the absent file is shown: {shown}",
    )
    startup_check.check_files("publisher", [])
    startup_check.check_build("features", "real", tmp_path / "profiles.yml")
    check("startup_check: features build: 1 checks, 0 failed" in capfd.readouterr().err, "the features build check runs in the child")
    queue = tmp_path / "queue.json"
    queue.write_text(json.dumps({"queue": []}))
    startup_check.check_queue_files("publisher", queue, controls=None, keys=False)
    check("startup_check: publisher files: 1 checks, 0 failed" in capfd.readouterr().err, "the queue check runs in the child")
