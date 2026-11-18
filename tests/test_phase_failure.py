"""The phase failure line that a host job writes and the launcher reads: bounded, one line and free of secrets, account numbers and ARNs."""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from orchestration import ecs_adapter
from scripts import phase_failure
from tests.support import check

ARN = "arn:aws:ecs:us-west-2:111111111111:task/cluster/0123456789abcdef0123456789abcdef"
# Values of the shapes the checks must remove are built from parts, so no scanner reads them as real secrets in this file.
ACCESS_KEY = "AK" + "IA" + "ABCDEFGHIJKLMNOP"
SESSION_KEY = "AS" + "IA" + "ABCDEFGHIJKLMNOP"
ASSIGNMENT = "pass" + "word=" + "hunter2"
TOKEN_LINE = "to" + "ken: abc.def"
URL_CREDENTIAL = "https://" + "user:" + "pass@example.invalid/path"


def test_a_failure_line_round_trips_through_the_reader() -> None:
    text = phase_failure.line("build", "only 0.50 GiB is left for the engines, below the floor of 2 GiB")
    check(text.startswith(phase_failure.MARKER) and "\n" not in text, f"one line with the marker: {text!r}")
    check(
        ecs_adapter.failure_summary("noise\n" + text + "\n") == "phase build: only 0.50 GiB is left for the engines, below the floor of 2 GiB",
        "the reader returns it",
    )


def test_the_reader_takes_the_last_failure_line() -> None:
    tail = "\n".join([phase_failure.line("pin", "first"), "other output", phase_failure.line("build", "second"), "trailing line"])
    check(ecs_adapter.failure_summary(tail) == "phase build: second", f"the last marker wins: {ecs_adapter.failure_summary(tail)!r}")


@pytest.mark.parametrize(
    "secret",
    [
        ARN,
        "111111111111",
        ACCESS_KEY,
        SESSION_KEY,
        ASSIGNMENT,
        TOKEN_LINE,
        URL_CREDENTIAL,
        "a" * 40,
    ],
)
def test_the_summary_never_carries_a_secret_an_account_number_or_an_arn(secret: str) -> None:
    for text in (phase_failure.line("build", f"failed with {secret} in the text"), f"plain last line {secret} here"):
        found = ecs_adapter.failure_summary(text)
        check(secret not in found and secret.split("=")[-1] not in found.replace("[redacted]", ""), f"{secret!r} leaked: {found!r}")


def test_the_summary_is_bounded_and_one_line() -> None:
    found = ecs_adapter.failure_summary(phase_failure.line("build", "x\ny " * 1000))
    check(len(found) <= ecs_adapter.FAILURE_LIMIT + 40 and "\n" not in found and "\r" not in found, f"bounded: {len(found)}")
    check(len(phase_failure.line("build", "y" * 10_000)) < 1000, "the written line is bounded as well")


def test_a_phase_name_that_is_not_a_name_is_dropped() -> None:
    record = json.loads(phase_failure.line("build; rm -rf /", "r")[len(phase_failure.MARKER) :])
    check(record["phase"] == "unknown" and record["reason"] == "r", f"{record}")


def test_a_log_without_a_marker_gives_its_last_nonempty_line_and_an_empty_log_gives_nothing() -> None:
    check(
        ecs_adapter.failure_summary("a\nValueError: bad value\n\n") == "last log line: ValueError: bad value",
        ecs_adapter.failure_summary("a\nValueError: bad value\n"),
    )
    check(ecs_adapter.failure_summary("") == "" and ecs_adapter.failure_summary("\n \n") == "", "nothing to say")


def test_a_marker_that_is_not_json_is_ignored() -> None:
    check(ecs_adapter.failure_summary(phase_failure.MARKER + "{not json\nlast") == "last log line: last", "a broken marker falls back to the last line")


# The recorder every job entry point runs through keeps the last stderr line of a failed main, so every job leaves a failure line for the launcher.


def recorded(main: Callable[[], object], tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> tuple[list[str], int]:
    """Run ``main`` through the recorder, as an entry point's ``__main__`` does, and return its stderr lines and exit code."""
    from scripts import resource_peaks

    with pytest.raises(SystemExit) as stop:
        resource_peaks.recorded(main, scratch=(tmp_path,), interval=0.02)
    return capsys.readouterr().err.splitlines(), int(stop.value.code or 0)


def test_a_failed_job_leaves_its_last_stderr_line_as_the_phase_failure(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def main() -> int:
        sys.stderr.write("some progress\n")
        sys.stderr.write("bronze_run: step load (run bronze_0a1b2c3d) failed: the request names 330 tables\n")
        return 1

    lines, code = recorded(main, tmp_path, capsys)
    marker = [line for line in lines if line.startswith(phase_failure.MARKER)]
    check(code == 1 and len(marker) == 1, f"one failure line and the job's own exit code: {code} {lines}")
    check("the request names 330 tables" in ecs_adapter.failure_summary("\n".join(lines)), f"it carries the last line: {marker}")


def test_a_job_that_wrote_its_own_failure_line_gets_no_second_one(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def main() -> int:
        phase_failure.emit("build", "its own reason")
        sys.stderr.write("later noise\n")
        return 1

    lines, _ = recorded(main, tmp_path, capsys)
    check(len([line for line in lines if line.startswith(phase_failure.MARKER)]) == 1, f"the job's own line stands alone: {lines}")


def test_an_exception_leaves_its_type_and_still_passes_on(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from scripts import resource_peaks

    def main() -> int:
        raise KeyError(ASSIGNMENT)

    with pytest.raises(KeyError):
        resource_peaks.recorded(main, scratch=(tmp_path,), interval=0.02)
    err = capsys.readouterr().err
    check("phase_failure:" in err and "KeyError" in err and "hunter2" not in ecs_adapter.failure_summary(err), f"only the exception type is written: {err}")


def test_a_successful_job_writes_no_failure_line(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def main() -> int:
        sys.stderr.write("fine\n")
        return 0

    lines, code = recorded(main, tmp_path, capsys)
    check(code == 0 and not any(line.startswith(phase_failure.MARKER) for line in lines), f"no failure line: {lines}")


def test_the_writer_and_the_reader_agree_on_the_line_and_the_cleaning() -> None:
    """The two sides are separate modules (the Airflow image cannot import the job side), so this keeps their contract one."""
    check(phase_failure.MARKER == ecs_adapter.FAILURE_MARKER and phase_failure.LIMIT == ecs_adapter.FAILURE_LIMIT, "the marker and the bound are the same")
    check(phase_failure.PHASE_NAME.pattern == ecs_adapter.PHASE_NAME.pattern, "the phase names have one form")
    corpus = [
        ARN,
        "111111111111",
        ACCESS_KEY,
        ASSIGNMENT,
        "to" + "ken: abc",
        "https://" + "u:p@example.invalid/x",
        "a" * 50,
        "x\ny\tz",
        "y" * 900,
        "plain",
    ]
    for text in corpus:
        check(phase_failure.clean(text) == ecs_adapter.clean_failure(text), f"both sides clean {text[:20]!r} the same way")
