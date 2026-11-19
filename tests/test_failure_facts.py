"""The numeric facts of a failure record: typed capacity figures from the capacity module only, in fixed ranges, consistent with each other.

A number is data too. A job could carry an account-shaped value in a count, so a fact is accepted only for the code it belongs to, from the measurement of the
capacity module, in the unit and range of its name and consistent with the other facts of the refusal (the floors and the headroom are fixed by the capacity
module, so a refusal is two numbers: the memory and the count of other containers). An exit code is never the job's: the launcher takes it from ECS."""

from __future__ import annotations

import contextlib
import io
import itertools
import json
import sys
from pathlib import Path
from typing import Any

import pytest

from orchestration import ecs_adapter
from scripts import phase_failure, resource_peaks
from scripts.lakehouse import task_capacity
from tests.failure_support import GIB, engine_facts, refusal, spill_facts
from tests.support import check
from tests.test_launch_failure_summary import ended, stored

MIB = 1 << 20
ENGINE = ["capacity_engine_floor"]
SPILL = ["capacity_spill_floor"]
ACCOUNT_SHAPED = 123456789012


def engine(memory: int, siblings: int, **changes: int) -> dict[str, int]:
    """The facts of an engine floor refusal for a memory and a sibling count, as the capacity module computes them, with ``changes`` applied."""
    headroom = max(GIB, memory * 6 // 100)
    reserve = siblings * (256 * MIB)
    return {
        "memory": memory,
        "headroom": headroom,
        "siblings": siblings,
        "sibling_reserve": reserve,
        "available": memory - headroom - reserve,
        "engine_floor": 2 * GIB,
        **changes,
    }


# One contract: every shared constant of the two modules is equal, and both validate every record the same way.


def test_every_shared_constant_of_the_two_modules_is_equal() -> None:
    pairs = {
        "MARKER": (phase_failure.MARKER, ecs_adapter.FAILURE_MARKER),
        "VERSION": (phase_failure.VERSION, ecs_adapter.FAILURE_VERSION),
        "MAX_CODES": (phase_failure.MAX_CODES, ecs_adapter.FAILURE_MAX_CODES),
        "RANGES": (phase_failure.RANGES, ecs_adapter.FAILURE_RANGES),
        "ENGINE_FLOOR": (phase_failure.ENGINE_FLOOR, ecs_adapter.FAILURE_ENGINE_FLOOR),
        "SPILL_FLOOR": (phase_failure.SPILL_FLOOR, ecs_adapter.FAILURE_SPILL_FLOOR),
        "SIBLING_RESERVE": (phase_failure.SIBLING_RESERVE, ecs_adapter.FAILURE_SIBLING_RESERVE),
        "HEADROOM_FLOOR": (phase_failure.HEADROOM_FLOOR, ecs_adapter.FAILURE_HEADROOM_FLOOR),
        "HEADROOM_PERCENT": (phase_failure.HEADROOM_PERCENT, ecs_adapter.FAILURE_HEADROOM_PERCENT),
        "SPILL_SHARE_PERCENT": (phase_failure.SPILL_SHARE_PERCENT, ecs_adapter.FAILURE_SPILL_SHARE_PERCENT),
        "LINE_FACTS": (tuple(phase_failure.LINE_FACTS), tuple(ecs_adapter.FAILURE_LINE_FACTS)),
        "FACTS": (tuple(phase_failure.FACTS), tuple(ecs_adapter.FAILURE_FACTS)),
        "PHASES": (tuple(phase_failure.PHASES), tuple(ecs_adapter.FAILURE_PHASES)),
        "CODES": (tuple(phase_failure.CODES), tuple(ecs_adapter.FAILURE_CODES)),
    }
    for name, (writer, reader) in pairs.items():
        check(writer == reader, f"{name} differs between the writer and the reader: {writer!r} != {reader!r}")
    check(ecs_adapter.FAILURE_MAX_LINE >= phase_failure.MAX_LINE, "the reader reads every line the writer may write")


def test_the_floors_are_the_ones_the_capacity_module_uses() -> None:
    check(phase_failure.ENGINE_FLOOR == task_capacity.ENGINE_FLOOR and phase_failure.SPILL_FLOOR == task_capacity.SPILL_FLOOR, "the floors")
    check(phase_failure.SIBLING_RESERVE == task_capacity.SIBLING_RESERVE, "the sibling reserve")
    check(phase_failure.HEADROOM_FLOOR == task_capacity.HEADROOM_FLOOR and phase_failure.HEADROOM_PERCENT == task_capacity.HEADROOM_PERCENT, "the headroom")
    check(phase_failure.SPILL_SHARE_PERCENT == task_capacity.SPILL_SHARE_PERCENT, "the spill share")


def candidates() -> list[tuple[list[str], dict[str, Any], bool]]:
    """Records that straddle every boundary: engine and spill facts, and invalid neighbours of each, with the same wish for both modules."""
    found: list[tuple[list[str], dict[str, Any], bool]] = []
    for memory, siblings in itertools.product([0, 1, MIB - 1, MIB, GIB, 3 * GIB - 1, 3 * GIB, 1 << 44, (1 << 44) + 1], [-1, 0, 1, 10, 11]):
        found.append((ENGINE, engine(memory, siblings), True))
    for name, value in itertools.product(phase_failure.RANGES, [-(1 << 45), -1, 0, 1, 255, 256, 1 << 50, (1 << 50) + 1, True, 1.5, "1", None]):
        found.append((ENGINE, {**engine(GIB, 1), name: value}, True))
        found.append((SPILL, {"spill_free": 1 << 29, "spill_floor": GIB, name: value}, True))
        found.append((["phase_failed"], {name: value}, True))
    for free in [-1, 0, GIB // 2, 1_789_569_706, 1_789_569_707, 1 << 50, (1 << 50) + 1]:
        found.append((SPILL, {"spill_free": free, "spill_floor": GIB}, True))
        found.append((SPILL, {"spill_free": free, "spill_floor": GIB - 1}, True))
    found.append((ENGINE, {}, True))
    found.append((SPILL, {}, True))
    found.append((["phase_failed"], {}, True))
    found.append((ENGINE + SPILL, {**engine(GIB, 1), "spill_free": 1 << 29, "spill_floor": GIB}, True))
    found.append((ENGINE, {**engine(GIB, 1), "spill_free": 1 << 29, "spill_floor": GIB}, True))
    found.append((["phase_failed"], engine(GIB, 1), True))
    found.append((["dbt_extension_missing", "capacity_engine_floor"], engine(GIB, 1), True))
    return found


def test_both_modules_judge_every_boundary_record_the_same_way() -> None:
    disagreements = []
    for codes, facts, _ in candidates():
        for stored_form in (False, True):
            writer = phase_failure.facts_fit(codes, facts, stored=stored_form)
            reader = ecs_adapter.facts_fit(codes, facts, stored=stored_form)
            if writer != reader:
                disagreements.append((codes, facts, stored_form, writer, reader))
    check(not disagreements, f"the writer and the reader disagree on {len(disagreements)} records, first: {disagreements[:1]}")


def test_the_real_refusals_fit_and_their_boundary_neighbours_do_not() -> None:
    for memory, siblings in [(MIB, 0), (GIB, 1), (3 * GIB - 1, 0), (2 * GIB, 9), (2 * GIB, 10)]:
        facts = engine(memory, siblings)
        check(phase_failure.facts_fit(ENGINE, facts, stored=False) == (facts["available"] < 2 * GIB), f"engine {memory} {siblings}")
    check(phase_failure.facts_fit(ENGINE, engine(3 * GIB - 1, 0), stored=False), "the largest memory that is refused")
    check(not phase_failure.facts_fit(ENGINE, engine(3 * GIB, 0), stored=False), "memory that is not refused is not a refusal")
    check(not phase_failure.facts_fit(ENGINE, engine(GIB, 11), stored=False), "more than ten other containers")
    check(not phase_failure.facts_fit(ENGINE, engine(GIB, 1, headroom=GIB + 1), stored=False), "a headroom the capacity module would not compute")
    check(not phase_failure.facts_fit(ENGINE, engine(GIB, 1, engine_floor=GIB), stored=False), "a floor that is not the capacity module's")
    check(not phase_failure.facts_fit(SPILL, {"spill_free": 1_789_569_707, "spill_floor": GIB}, stored=False), "a spill that is not refused")
    check(phase_failure.facts_fit(SPILL, {"spill_free": 1_789_569_706, "spill_floor": GIB}, stored=False), "the largest spill that is refused")
    check(
        not phase_failure.facts_fit(ENGINE, {}, stored=False) and not phase_failure.facts_fit(["phase_failed"], engine(GIB, 1), stored=False),
        "code and facts go together",
    )


def test_the_exit_code_is_a_fact_of_a_stored_record_only_and_in_the_range_of_an_exit_code() -> None:
    for value, fits in [(1, True), (255, True), (0, False), (256, False), (-1, False), (True, False), (1.0, False)]:
        check(ecs_adapter.facts_fit(["phase_failed"], {"exit_code": value}, stored=True) is fits, f"stored exit code {value!r}")
        check(not phase_failure.facts_fit(["phase_failed"], {"exit_code": value}, stored=False), f"a job's line holds no exit code: {value!r}")


@pytest.mark.parametrize("name", sorted(phase_failure.RANGES))
def test_a_fact_outside_its_range_makes_the_whole_line_unreadable(name: str) -> None:
    low, high = phase_failure.RANGES[name]
    base = {"v": 1, "phase": "build", "codes": ["capacity_engine_floor"], "facts": engine_facts()}
    for value in (low - 1, high + 1):
        facts = {**base["facts"], name: value} if name in base["facts"] else {name: value}
        line = phase_failure.MARKER + json.dumps({**base, "facts": facts})
        check(ecs_adapter.parse_failure_line(line) is None, f"{name}={value} is outside {low}..{high}")


def test_a_job_line_with_an_exit_code_is_unreadable() -> None:
    line = phase_failure.MARKER + json.dumps({"v": 1, "phase": "build", "codes": ["phase_failed"], "facts": {"exit_code": ACCOUNT_SHAPED}})
    check(ecs_adapter.parse_failure_line(line) is None, "the exit code of a job's own line is never read")


# A number that a job puts in its own exception never reaches a record, from the recorder to the end record and its message.


def test_the_numbers_of_a_job_defined_exception_are_dropped_end_to_end(tmp_path: Path) -> None:
    class JobDefinedError(Exception):
        failure_codes = ("capacity_engine_floor",)
        failure_facts = {
            "memory": GIB,
            "headroom": GIB,
            "available": -(1 << 28),
            "engine_floor": 2 * GIB,
            "siblings": ACCOUNT_SHAPED,
            "sibling_reserve": ACCOUNT_SHAPED,
        }

    def job() -> int:
        raise JobDefinedError("SYNTHETIC_MESSAGE")

    previous = sys.argv
    sys.argv = ["layer_run.py", "build"]
    try:
        with contextlib.redirect_stderr(io.StringIO()) as err, pytest.raises(JobDefinedError):
            resource_peaks.recorded(job, scratch=(tmp_path,), interval=0.01)
    finally:
        sys.argv = previous
    lines = [line for line in err.getvalue().splitlines() if line.startswith(phase_failure.MARKER)]
    check(len(lines) == 1 and str(ACCOUNT_SHAPED) not in lines[0], f"the recorder writes no job number: {lines}")
    rig, launcher, launch = ended({"build": 1, "publish": None}, {"build": lines})
    outcome = launcher.conclude(launch)
    record = stored(rig)["failure"]
    check(str(ACCOUNT_SHAPED) not in json.dumps(record) + outcome.failure, f"no job number in the end record or its message: {record} {outcome.failure}")
    check(record["facts"] == {"exit_code": 1}, f"the only fact is the exit code that ECS reported: {record}")


def test_a_forged_line_with_a_real_looking_refusal_for_other_numbers_is_refused() -> None:
    forged = {"v": 1, "phase": "build", "codes": ENGINE, "facts": engine(GIB, ACCOUNT_SHAPED)}
    check(ecs_adapter.parse_failure_line(phase_failure.MARKER + json.dumps(forged)) is None, "a count above the ten containers of a task")
    forged = {"v": 1, "phase": "build", "codes": ENGINE, "facts": {**engine(GIB, 1), "memory": GIB + 7}}
    check(ecs_adapter.parse_failure_line(phase_failure.MARKER + json.dumps(forged)) is None, "figures that do not follow from one another")


def test_the_launcher_takes_the_exit_code_from_ecs_and_binds_the_count_to_the_task() -> None:
    from tests.test_launch_failure_summary import diagnose

    real = phase_failure.line(phase_failure.record("build", ENGINE, engine_facts(siblings=1)))
    found = diagnose({"build": 7, "publish": None}, {"build": [real]})
    check(found.record["facts"]["exit_code"] == 7 and found.record["codes"] == ENGINE, f"the exit code of ECS: {found.record}")
    crowded = phase_failure.line(phase_failure.record("build", ENGINE, engine_facts(siblings=9)))
    found = diagnose({"build": 1, "publish": None}, {"build": [crowded]})
    check(found.record["codes"] == ["phase_failed"], f"nine siblings cannot be in a two container task: {found.record}")


def test_the_measured_facts_of_the_capacity_module_are_what_the_writer_keeps() -> None:
    error = refusal(GIB, 1)
    found = phase_failure.record_for("build", error)
    check(found["codes"] == ENGINE and found["facts"] == dict(error.failure_facts) and found["facts"]["memory"] == GIB, f"{found}")
    again = phase_failure.record_for("build", refusal(16 * GIB, 1, spill_free=GIB // 2))
    check(again["codes"] == SPILL and set(again["facts"]) == {"spill_free", "spill_floor"}, f"{again}")
    check(dict(spill_facts()) == again["facts"], "the helper")


def test_records_at_the_limits_round_trip_and_their_neighbours_do_not() -> None:
    """The count of codes and the range of every fact, at the allowed boundary and one past it, through the writer and the reader."""
    one = phase_failure.record("build", ["phase_failed"])
    eight = phase_failure.record("build", list(phase_failure.CODES[: phase_failure.MAX_CODES]))
    check(len(eight["codes"]) == phase_failure.MAX_CODES, f"the writer keeps eight distinct codes: {eight}")
    for found in (one, eight):
        line = phase_failure.line(found)
        check(len(line) <= phase_failure.MAX_LINE and ecs_adapter.parse_failure_line(line) == found, f"round trip of {len(found['codes'])} codes")
    nine = {**eight, "codes": list(phase_failure.CODES[: phase_failure.MAX_CODES + 1])}
    check(ecs_adapter.parse_failure_line(phase_failure.line(nine)) is None, "nine codes are one past the limit")
    check(len(phase_failure.record("build", list(phase_failure.CODES[:12]))["codes"]) == phase_failure.MAX_CODES, "the writer keeps no more than eight")
    for memory, siblings in [
        (phase_failure.RANGES["memory"][0], 0),
        (3 * GIB - 1, 0),
        (phase_failure.RANGES["memory"][0], phase_failure.RANGES["siblings"][1]),
    ]:
        edge = engine(memory, siblings)
        written = phase_failure.record("build", ENGINE, phase_failure.measured(edge))
        check(written["facts"] == edge and ecs_adapter.parse_failure_line(phase_failure.line(written)) == written, f"engine boundary {memory} {siblings}")
    for memory, siblings in [(phase_failure.RANGES["memory"][0] - 1, 0), (3 * GIB, 0), (GIB, phase_failure.RANGES["siblings"][1] + 1)]:
        edge = engine(memory, siblings)
        check(phase_failure.record("build", ENGINE, phase_failure.measured(edge))["codes"] == ["unexpected_error"], f"engine neighbour {memory} {siblings}")
        forged = {"v": 1, "phase": "build", "codes": ENGINE, "facts": edge}
        check(ecs_adapter.parse_failure_line(phase_failure.MARKER + json.dumps(forged)) is None, f"engine neighbour read {memory} {siblings}")
    for free, fits in [(0, True), (1_789_569_706, True), (1_789_569_707, False), (phase_failure.RANGES["spill_free"][1], False)]:
        spill = {"spill_free": free, "spill_floor": GIB}
        written = phase_failure.record("build", SPILL, phase_failure.measured(spill))
        check((written["codes"] == SPILL) is fits, f"spill {free}")
        check((ecs_adapter.parse_failure_line(phase_failure.line(written)) == written) and (written["facts"] == spill) is fits, f"spill round trip {free}")
