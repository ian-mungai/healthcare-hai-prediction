"""The failure record a host job writes and the launcher validates: closed, with a checked phase, codes of a fixed vocabulary and numeric facts only.

No stderr text, exception message or log line is ever part of it. The job side (``scripts/phase_failure.py``) and the launcher side
(``orchestration/ecs_adapter.py``, which the Airflow image mounts and which cannot import the job side) are separate modules, so these cases keep the two
equal."""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from orchestration import ecs_adapter
from scripts import phase_failure
from scripts.infrastructure import startup_check
from scripts.lakehouse import task_capacity
from tests.support import check

# Values of the shapes that must never be kept are built from parts, so no scanner reads them as real secrets in this file.
ACCESS_KEY = "AK" + "IA" + "ABCDEFGH" + "IJKLMNOP"
ASSIGNMENT = "pass" + "word=" + "hunter2"
BEARER = "Author" + "ization: Bearer synthetic.jwt.value"
BUCKET = "s3://synthetic-private-bucket/object"
ROW_VALUE = "SYNTHETIC_ROW_VALUE_42"
HOSTILE = [
    ACCESS_KEY,
    ACCESS_KEY[:8] + "\n" + ACCESS_KEY[8:],
    "".join(f"%{ord(letter):02x}" for letter in ACCESS_KEY),
    ASSIGNMENT,
    BEARER,
    BUCKET,
    "Conversion Error: could not convert string " + ROW_VALUE + " to DOUBLE",
    "https://synthetic-private-bucket.s3.amazonaws.com/object?X-Amz-Signature=synthetic.signature.value",
    "%31" * 12,
    "111111\n111111",
    "arn:aws:ecs:us-west-2:" + "1" * 12 + ":task/example",
]
FACTS = {"memory": 1 << 30, "available": -(1 << 28), "engine_floor": 2 << 30}
GOOD: dict[str, Any] = {"v": 1, "phase": "build", "codes": ["capacity_engine_floor"], "facts": FACTS}


def marker(record: object) -> str:
    return phase_failure.MARKER + json.dumps(record, sort_keys=True, separators=(",", ":"))


def test_a_record_round_trips_from_the_writer_to_the_reader() -> None:
    written = phase_failure.line(phase_failure.record("build", ["capacity_engine_floor"], FACTS))
    check(written.startswith(phase_failure.MARKER) and "\n" not in written, f"one line: {written!r}")
    check(
        ecs_adapter.parse_failure_line(written) == {"v": 1, "phase": "build", "codes": ["capacity_engine_floor"], "facts": GOOD["facts"]},
        "the reader returns the record",
    )
    check(ecs_adapter.failure_from_tail("noise\n" + written + "\nlater noise\n") is not None, "the reader finds it in a tail")


def test_the_last_valid_record_of_a_tail_wins_and_an_invalid_one_never_does() -> None:
    first = phase_failure.line(phase_failure.record("pin", ["polaris_unreachable"]))
    second = phase_failure.line(phase_failure.record("build", ["dbt_extension_missing"]))
    check((ecs_adapter.failure_from_tail("\n".join([first, "x", second])) or {})["phase"] == "build", "the last valid record wins")
    forged = marker({**GOOD, "codes": ["not_a_code"]})
    check((ecs_adapter.failure_from_tail("\n".join([first, forged])) or {})["phase"] == "pin", "an invalid last record is skipped, never repaired")


@pytest.mark.parametrize("text", HOSTILE)
def test_text_in_any_field_of_a_marker_is_rejected_and_never_persisted(text: str) -> None:
    for record in (
        {**GOOD, "phase": text},
        {**GOOD, "codes": [text]},
        {**GOOD, "facts": {"memory": text}},
        {**GOOD, "facts": {text: 1}},
        {**GOOD, "reason": text},
        {**GOOD, "container": text},
    ):
        check(ecs_adapter.parse_failure_line(marker(record)) is None, f"a record that carries text is rejected: {record}")
    for lines in (text, "\n".join([text, text]), "error: " + text, text.replace("\n", "\r\n")):
        check(ecs_adapter.failure_from_tail(lines) is None, f"a plain log line is never a record: {lines!r}")


@pytest.mark.parametrize(
    "record",
    [
        {**GOOD, "v": 2},
        {**GOOD, "v": True},
        {"v": 1, "phase": "build", "codes": ["capacity_engine_floor"]},
        {**GOOD, "codes": []},
        {**GOOD, "codes": ["capacity_engine_floor"] * 2},
        {**GOOD, "codes": ["phase_failed"] * 9},
        {**GOOD, "codes": "capacity_engine_floor"},
        {**GOOD, "facts": {"memory": 1.5}},
        {**GOOD, "facts": {"memory": True}},
        {**GOOD, "facts": {"memory": 1 << 70}},
        {**GOOD, "facts": {"not_a_fact": 1}},
        {**GOOD, "facts": []},
        {**GOOD, "phase": "BUILD"},
    ],
)
def test_a_record_off_the_schema_is_rejected(record: object) -> None:
    check(ecs_adapter.parse_failure_line(marker(record)) is None, f"{record} is not a closed record")


def test_a_line_that_is_not_one_json_object_is_rejected() -> None:
    for text in (phase_failure.MARKER + "{not json", phase_failure.MARKER + "[1]", phase_failure.MARKER + json.dumps(GOOD) + " trailing", phase_failure.MARKER):
        check(ecs_adapter.parse_failure_line(text) is None, f"{text!r}")


def test_the_writer_keeps_only_the_closed_parts_of_an_error() -> None:
    class Tagged(RuntimeError):
        failure_codes = ("capacity_engine_floor",)
        failure_facts = {"memory": 5, "message": ASSIGNMENT, "available": "text", "extra": 1}

    found = phase_failure.record_for("build", Tagged(ASSIGNMENT))
    check(found["codes"] == ["capacity_engine_floor"] and found["facts"] == {"memory": 5}, f"only allowed numeric facts stay: {found}")
    check(ASSIGNMENT not in json.dumps(found), "the message of the error is never read")
    check(phase_failure.record_for("not a phase", KeyError(ASSIGNMENT))["phase"] == "unknown", "a phase that is not on the list is unknown")
    check(phase_failure.record_for("build", KeyError(ASSIGNMENT))["codes"] == ["key_error"], "a known class gives its category")

    class Odd(Exception):
        pass

    check(phase_failure.record_for("build", Odd(BUCKET))["codes"] == ["unexpected_error"], "any other class is one safe category")
    check(phase_failure.record_for("build", Odd(), exit_code=3)["facts"] == {"exit_code": 3}, "the exit code is a fact")


def test_a_failure_that_names_no_error_is_a_generic_phase_failure_with_its_exit_code() -> None:
    check(
        phase_failure.record_for("build", None, exit_code=1) == {"v": 1, "phase": "build", "codes": ["phase_failed"], "facts": {"exit_code": 1}},
        "generic record",
    )


def test_the_two_sides_share_one_vocabulary() -> None:
    check(phase_failure.MARKER == ecs_adapter.FAILURE_MARKER, "the marker")
    check(tuple(phase_failure.PHASES) == tuple(ecs_adapter.FAILURE_PHASES), "the phase names")
    check(tuple(phase_failure.CODES) == tuple(ecs_adapter.FAILURE_CODES), "the codes")
    check(frozenset(phase_failure.FACTS) == frozenset(ecs_adapter.FAILURE_FACTS), "the numeric facts")
    check(set(ecs_adapter.FAILURE_TEMPLATES) == set(ecs_adapter.FAILURE_CODES), "every code has a template")


def test_the_startup_check_codes_that_can_fail_are_all_in_the_vocabulary() -> None:
    failing = (startup_check.CODES - startup_check.PASSING) | startup_check.EITHER
    check(failing <= set(phase_failure.CODES), f"startup codes missing from the vocabulary: {sorted(failing - set(phase_failure.CODES))}")
    check(set(phase_failure.STARTUP_CODES) == failing, "the startup part of the vocabulary is exactly the failing codes")


def test_the_capacity_codes_are_the_ones_the_capacity_module_raises() -> None:
    raised = set(task_capacity.CAPACITY_CODES)
    check(raised <= set(phase_failure.CODES), "capacity codes missing from the vocabulary")
    check(set(phase_failure.CAPACITY_CODES) == raised, "the capacity part of the vocabulary")


def test_every_container_of_a_host_job_is_a_known_phase_name() -> None:
    root = Path(__file__).resolve().parents[1]
    names = {
        name
        for table in ("ecs_broker_jobs", "host_jobs")
        for job in json.loads((root / f"config/orchestration/{table}.json").read_text())["jobs"].values()
        for name in job.get("containers", {})
    }
    check(names <= set(phase_failure.PHASES), f"containers that are not on the list: {sorted(names - set(phase_failure.PHASES))}")


def test_the_startup_check_error_and_the_capacity_error_carry_their_codes_and_facts() -> None:
    error = task_capacity.CapacityError("text that must not travel", "capacity_engine_floor", {"memory": 7})
    check(error.failure_codes == ("capacity_engine_floor",) and error.failure_facts == {"memory": 7}, "capacity error")
    derived = None
    try:
        task_capacity.derive(task_capacity.Capacity(GIB := 1 << 30, "case", 4, "case", 1, "case", 100 * GIB))
    except task_capacity.CapacityError as caught:
        derived = caught
    check(derived is not None and derived.failure_codes == ("capacity_engine_floor",), "the floor refusal is tagged")
    check(
        derived is not None and set(derived.failure_facts) == {"memory", "headroom", "siblings", "sibling_reserve", "available", "engine_floor"},
        "with its numeric facts",
    )
    check(startup_check.StartupCheckError("x").failure_codes == ("startup_check_internal_error",), "an error built without codes names the internal error")
    check(startup_check.StartupCheckError("x", ("dbt_extension_missing",)).failure_codes == ("dbt_extension_missing",), "and one built with codes keeps them")


# The recorder every job entry point runs through writes a generic record for a failed job that wrote none, and never copies text.


def recorded(main: Callable[[], object], tmp_path: Path, capsys: pytest.CaptureFixture[str], argv: list[str] | None = None) -> tuple[list[str], int]:
    from scripts import resource_peaks

    previous = sys.argv
    sys.argv = argv or ["layer_run.py", "build"]
    try:
        with pytest.raises(SystemExit) as stop:
            resource_peaks.recorded(main, scratch=(tmp_path,), interval=0.02)
    finally:
        sys.argv = previous
    return capsys.readouterr().err.splitlines(), int(stop.value.code or 0)


def records_of(lines: list[str]) -> list[dict[str, object]]:
    return [found for found in (ecs_adapter.parse_failure_line(line) for line in lines) if found is not None]


@pytest.mark.parametrize("text", HOSTILE)
def test_a_failed_job_leaves_a_generic_record_and_no_text_of_its_stderr(tmp_path: Path, capsys: pytest.CaptureFixture[str], text: str) -> None:
    def main() -> int:
        sys.stderr.write("bronze_run: step load failed: " + text + "\n")
        return 1

    lines, code = recorded(main, tmp_path, capsys)
    found = records_of(lines)
    check(code == 1 and found == [{"v": 1, "phase": "build", "codes": ["phase_failed"], "facts": {"exit_code": 1}}], f"one generic record: {found}")
    written = [line for line in lines if line.startswith(phase_failure.MARKER)]
    check(len(written) == 1 and not any(part in written[0] for part in (ROW_VALUE, "synthetic", ACCESS_KEY[:8] + "I")), f"no text of the stderr: {written}")


def test_an_exception_leaves_only_its_category(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from scripts import resource_peaks

    def main() -> int:
        raise KeyError(ASSIGNMENT + BUCKET)

    previous = sys.argv
    sys.argv = ["aws_run.py", "extract"]
    try:
        with pytest.raises(KeyError):
            resource_peaks.recorded(main, scratch=(tmp_path,), interval=0.02)
    finally:
        sys.argv = previous
    err = capsys.readouterr().err
    found = records_of(err.splitlines())
    check(found == [{"v": 1, "phase": "extract", "codes": ["key_error"], "facts": {"exit_code": 1}}], f"the class, not the message: {found}")
    check("hunter2" not in "".join(line for line in err.splitlines() if line.startswith(phase_failure.MARKER)), "no value of the exception")


def test_a_job_that_wrote_its_own_record_gets_no_second_one_and_a_success_none(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def failing() -> int:
        phase_failure.emit(phase_failure.record("build", ["dbt_extension_missing"]))
        sys.stderr.write("later noise\n")
        return 1

    lines, _ = recorded(failing, tmp_path, capsys)
    check(len(records_of(lines)) == 1 and records_of(lines)[0]["codes"] == ["dbt_extension_missing"], f"the job's own record stands alone: {lines}")
    lines, code = recorded(lambda: 0, tmp_path, capsys)
    check(code == 0 and not records_of(lines), "a success writes no record")


def test_the_stderr_tail_keeps_no_text(tmp_path: Path) -> None:
    import io

    tail = phase_failure.StderrTail(io.StringIO())
    tail.write("some text " + ASSIGNMENT + "\n" + phase_failure.line(phase_failure.record("build", ["phase_failed"])) + "\n")
    tail.finish()
    check(tail.marked and not hasattr(tail, "last") and ASSIGNMENT not in repr(vars(tail)), f"only whether a record was written is kept: {vars(tail)}")
