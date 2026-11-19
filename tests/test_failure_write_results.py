"""A failure record is best effort: writing it never changes the result of a job, whatever the stream does, through every entry point that writes it directly.

Each case runs a real job main down its failure path with a standard error that raises only for the failure line, both bare and inside the recorder that every
job entry point runs through, and requires the result that the job would have had without the fault."""

from __future__ import annotations

import contextlib
import io
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from monitoring import aws_run as drift_run
from scripts import phase_failure, resource_peaks
from scripts.lakehouse import aws_bronze, bronze_run, bronze_writer, layer_run
from scripts.serving import deploy_job
from scripts.training import aws_run as training_run
from tests.support import check

RUN = "silver_0a1b2c3d"
BRONZE_RUN = "bronze_0a1b2c3d"


class Odd(BaseException):
    pass


def faulty(fault: type[BaseException]) -> type[io.StringIO]:
    class Faulty(io.StringIO):
        def write(self, text: str) -> int:
            if text.startswith(phase_failure.MARKER):
                raise fault("synthetic fault of the diagnostic stream")
            return super().write(text)

    return Faulty


def direct_bronze(work: Path, monkeypatch: pytest.MonkeyPatch) -> int:
    job = next(iter(bronze_writer.MUTATIONS))
    return int(aws_bronze.main(["--binding", str(work / "missing.json"), "--job", job, "--authority-epoch", "1"]))


def bronze(work: Path, monkeypatch: pytest.MonkeyPatch) -> int:
    monkeypatch.delenv("DATA_BUCKET_NAME", raising=False)
    return int(bronze_run.main(["load", "--run", BRONZE_RUN, "--work", str(work), "--tables", "x", "--target", "qualify"]))


def training(work: Path, monkeypatch: pytest.MonkeyPatch) -> int:
    def refused(*_arguments: object) -> list[dict[str, object]]:
        raise training_run.RunRefused("synthetic refusal")

    monkeypatch.setattr(training_run, "extract", refused)
    return int(training_run.main(["extract", "--run", RUN, "--work", str(work)]))


def drift(work: Path, monkeypatch: pytest.MonkeyPatch) -> int:
    monkeypatch.setattr(drift_run, "prepare_environment", lambda _bucket: None)
    return int(drift_run.main(["baseline", "--run", "not a run id", "--work", str(work)]))


def serving(work: Path, monkeypatch: pytest.MonkeyPatch) -> int:
    monkeypatch.setattr(sys, "argv", ["deploy_job", "--run", RUN])
    monkeypatch.delenv("SERVING_REQUEST", raising=False)
    return int(deploy_job.main())


def layer(work: Path, monkeypatch: pytest.MonkeyPatch) -> int:
    return int(layer_run.main(["pin", "--layer", "silver", "--run", RUN, "--code-run-id", "not a code run", "--work", str(work)]))


MAINS: dict[str, Callable[[Path, pytest.MonkeyPatch], int]] = {
    "direct_bronze": direct_bronze,
    "bronze": bronze,
    "training": training,
    "drift": drift,
    "serving": serving,
    "layer": layer,
}
FAULTS = [KeyboardInterrupt, Odd, OSError, ValueError]


def result_without_fault(name: str, work: Path, monkeypatch: pytest.MonkeyPatch) -> int:
    with contextlib.redirect_stderr(io.StringIO()):
        return MAINS[name](work, monkeypatch)


@pytest.mark.parametrize("fault", FAULTS, ids=lambda fault: fault.__name__)
@pytest.mark.parametrize("name", sorted(MAINS))
def test_a_fault_in_the_failure_line_does_not_change_the_result_of_the_main(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str, fault: type[BaseException]
) -> None:
    expected = result_without_fault(name, tmp_path / "plain", monkeypatch)
    check(expected != 0, f"{name} fails on this path: {expected}")
    with contextlib.redirect_stderr(faulty(fault)()):
        try:
            found: object = MAINS[name](tmp_path / "faulty", monkeypatch)
        except BaseException as error:
            found = type(error).__name__
    check(found == expected, f"{name}: a {fault.__name__} on the failure line changed the result from {expected} to {found}")


@pytest.mark.parametrize("fault", FAULTS, ids=lambda fault: fault.__name__)
@pytest.mark.parametrize("name", sorted(MAINS))
def test_a_fault_in_the_failure_line_does_not_change_the_exit_inside_the_recorder(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str, fault: type[BaseException]
) -> None:
    expected = result_without_fault(name, tmp_path / "plain", monkeypatch)
    with contextlib.redirect_stderr(faulty(fault)()):
        try:
            resource_peaks.recorded(lambda: MAINS[name](tmp_path / "faulty", monkeypatch), scratch=(tmp_path,), interval=0.01)
        except SystemExit as stop:
            found: object = stop.code
        except BaseException as error:
            found = type(error).__name__
    check(found == expected, f"{name}: inside the recorder, a {fault.__name__} on the failure line changed the exit from {expected} to {found}")


@pytest.mark.parametrize("fault", FAULTS, ids=lambda fault: fault.__name__)
def test_the_shared_emit_functions_never_raise(fault: type[BaseException]) -> None:
    stream = faulty(fault)()
    phase_failure.emit(phase_failure.record("build", ["phase_failed"]), stream)
    phase_failure.emit_error("build", KeyError("x"), stream)
    phase_failure.emit_error("build", None, stream)
    check(stream.getvalue() == "", "nothing was written")
