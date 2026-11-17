"""Cases for the launch outcome and refusal counts that the launcher sends to the StatsD exporter, against the same fake ECS rig as the launch core.

Each launch that ends is counted once by its job family and its outcome (``hai.launch.outcome.<family>.<outcome>``), and each refusal of a launch call is
counted by its family and one of six closed kinds (``hai.launch.refusal.<family>.<kind>``). A launch that only reports an earlier attempt's recorded outcome
counts nothing, and neither does a family the launcher does not know. A run ID, a task ARN and the text of an error never leave the process. A loopback UDP
socket stands in for the exporter. Counting never changes what the launcher returns or raises.
"""

from __future__ import annotations

import os
import socket
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import partial

from orchestration.ecs_launch import FamilyBusy, LaunchRefused, LaunchUnavailable, StopFailed, count_outcome, count_refusal_kind
from scripts.orchestration.ecs_launch_cases import BOUND, OTHER_RUN, RUN, THROTTLE, Rig, die_while_running, refuses, require, spec

VARIABLES = ("AIRFLOW__METRICS__STATSD_HOST", "AIRFLOW__METRICS__STATSD_PORT")


@contextmanager
def exporter() -> Iterator[Callable[[], list[str]]]:
    """A loopback UDP socket that the launcher's counts reach: the environment names it while the case runs, and ``taken()`` returns what it received."""
    found = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    found.bind(("127.0.0.1", 0))
    saved = {name: os.environ.get(name) for name in VARIABLES}
    os.environ[VARIABLES[0]], os.environ[VARIABLES[1]] = "127.0.0.1", str(found.getsockname()[1])

    def taken() -> list[str]:
        got: list[str] = []
        found.settimeout(0.1)
        try:
            while True:
                got.append(found.recv(512).decode())
        except OSError:
            return got

    try:
        yield taken
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        found.close()


def outcome_line(family: str, outcome: str) -> str:
    return f"hai.launch.outcome.{family}.{outcome}:1|c"


def refusal_line(family: str, kind: str) -> str:
    return f"hai.launch.refusal.{family}.{kind}:1|c"


def a_success_is_counted_once_and_a_repeat_of_it_is_not() -> None:
    with exporter() as taken:
        rig = Rig()
        require(rig.launcher().run(spec()).ok, "the launch succeeds")
        require(taken() == [outcome_line("gold", "succeeded")], "one succeeded count for the gold family")
        require(rig.launcher().run(spec()).ok, "the repeat reports the recorded outcome")
        require(taken() == [], "a repeat of an ended attempt counts nothing")


def a_failed_attempt_is_counted_and_the_retry_that_adopts_it_is_not() -> None:
    with exporter() as taken:
        rig = Rig()
        rig.ecs.plan = {"gold": 3}
        require(rig.launcher().run(spec()).outcome == "failed", "the first attempt fails")
        require(taken() == [outcome_line("gold", "failed")], "one failed count")
        require(rig.launcher().run(spec(2)).adopted, "the retry reports the earlier failure")
        require(taken() == [], "the retry counts nothing, because it launched nothing")


def a_timeout_a_closed_launch_and_a_cancel_are_counted_by_their_outcome() -> None:
    with exporter() as taken:
        rig = Rig()
        rig.ecs.hold = True
        require(rig.launcher().run(spec(deadline_seconds=100)).outcome == "timeout", "timeout")
        require(taken() == [outcome_line("gold", "timeout")], "one timeout count")
        rig = Rig()
        rig.scratch_free = 10
        require(rig.launcher().run(spec()).outcome == "failed_closed", "failed closed")
        require(taken() == [outcome_line("gold", "failed_closed")], "one failed_closed count")
        rig = Rig()
        rig.ecs.hold = True
        die_while_running(rig, spec())
        require(rig.launcher().cancel(RUN, "gold", 1).outcome == "cancelled", "cancelled")
        require(taken() == [outcome_line("gold", "cancelled")], "one cancelled count")


def a_refused_call_is_counted_by_the_kind_of_its_refusal() -> None:
    with exporter() as taken:
        rig = Rig()
        refuses(LaunchRefused, partial(rig.launcher().run, spec(attempt=0)))
        require(taken() == [refusal_line("gold", "refused")], "a malformed request is a refused count")
        refuses(LaunchRefused, partial(rig.launcher().run, spec(job="train")))
        require(taken() == [], "a family the launcher does not know counts nothing")
        rig = Rig()
        rig.ecs.hold = True
        die_while_running(rig, spec())
        taken()
        refuses(FamilyBusy, lambda: rig.launcher().run(spec(1, run_id=OTHER_RUN)))
        require(taken() == [refusal_line("gold", "family_busy")], "a busy family is its own kind")
        rig = Rig()
        rig.ecs.hold = True
        rig.ecs.stop_lag = 10**6
        refuses(StopFailed, lambda: rig.launcher().run(spec(deadline_seconds=10)))
        require(taken() == [refusal_line("gold", "stop_failed")], "an unconfirmed stop is its own kind")
        rig = Rig()
        rig.ecs.hold = True
        die_while_running(rig, spec())
        taken()
        refuses(LaunchRefused, lambda: rig.launcher().run(spec(2, bound={**BOUND, "code_digest": "c" * 64})))
        require(taken() == [refusal_line("gold", "refused")], "changed inputs are a refused count")
        rig = Rig()
        rig.ecs.hold = True
        die_while_running(rig, spec())
        taken()
        rig.ecs.faults["describe_tasks"] = [THROTTLE]
        refuses(LaunchUnavailable, lambda: rig.launcher().run(spec(2)))
        require(taken() == [refusal_line("gold", "unavailable")], "a temporary failure is its own kind")


def counting_changes_nothing_when_the_exporter_is_not_set() -> None:
    saved = {name: os.environ.pop(name, None) for name in VARIABLES}
    try:
        rig = Rig()
        require(rig.launcher().run(spec()).ok, "a launch works with no exporter set")
        refuses(LaunchRefused, partial(rig.launcher().run, spec(attempt=0)))
    finally:
        for name, value in saved.items():
            if value is not None:
                os.environ[name] = value


def the_counting_functions_send_only_closed_names() -> None:
    with exporter() as taken:
        count_outcome("gold", "succeeded")
        count_outcome("gold", "strange")
        count_outcome("Gold Run", "failed")
        count_outcome(f"gold.{RUN}", "failed")
        count_refusal_kind("gold", "unavailable")
        count_refusal_kind("gold", "free text")
        count_refusal_kind("", "refused")
        require(
            taken() == [outcome_line("gold", "succeeded"), refusal_line("gold", "unavailable")],
            "only a closed outcome, a closed kind and a plain family are sent",
        )
    count_outcome("gold", "succeeded", {})
    count_outcome("gold", "succeeded", {VARIABLES[0]: "127.0.0.1", VARIABLES[1]: "no"})


CASES: dict[str, Callable[[], None]] = {
    "counts_a_success_is_counted_once_and_a_repeat_of_it_is_not": a_success_is_counted_once_and_a_repeat_of_it_is_not,
    "counts_a_failed_attempt_is_counted_and_the_retry_that_adopts_it_is_not": a_failed_attempt_is_counted_and_the_retry_that_adopts_it_is_not,
    "counts_a_timeout_a_closed_launch_and_a_cancel_are_counted_by_their_outcome": a_timeout_a_closed_launch_and_a_cancel_are_counted_by_their_outcome,
    "counts_a_refused_call_is_counted_by_the_kind_of_its_refusal": a_refused_call_is_counted_by_the_kind_of_its_refusal,
    "counts_counting_changes_nothing_when_the_exporter_is_not_set": counting_changes_nothing_when_the_exporter_is_not_set,
    "counts_the_counting_functions_send_only_closed_names": the_counting_functions_send_only_closed_names,
}
