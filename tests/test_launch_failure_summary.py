"""The failed phase and its reason reach the durable launch record (the end record) that the evidence and gate tasks read.

The cases run the real launcher over the repository's fake ECS, log service and write-once bucket. The task has the shape of a layer task: the log container
of the job is the last one (``publish``) and never runs when ``build`` fails, so the failing container's own log is where the cause is."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, cast

import pytest

from orchestration.ecs_launch import Launch, Launcher, LaunchRefused
from orchestration.ecs_launch_operator import JobFailed
from scripts import phase_failure
from scripts.orchestration.ecs_launch_cases import RUN, Rig, spec
from tests.support import check

LAYER = {"pin": {"RUN_ID": RUN}, "build": {"RUN_ID": RUN}, "publish": {"RUN_ID": RUN}}
CAUSE = "only 0.50 GiB is left for the engines, below the floor of 2 GiB"
# Values of the shapes the checks must remove are built from parts, so no scanner reads them as real secrets in this file.
ACCESS_KEY = "AK" + "IA" + "ABCDEFGHIJKLMNOP"
ASSIGNMENT = "pass" + "word=" + "hunter2"
ARN = "arn:aws:ecs:us-west-2:111111111111:task/cluster/0123456789abcdef0123456789abcdef"


def layer_spec(attempt: int = 1) -> Any:
    return spec(attempt, environment=LAYER, log_container="publish", log_prefix="gold", acceptance=("publish",))


def stream(rig: Rig, arn: str, container: str) -> str:
    return f"gold/{container}/{arn.rsplit('/', 1)[-1]}"


def ended_without_a_record(rig: Rig, lines: list[str], container: str = "build") -> tuple[Launcher, Launch]:
    """A layer task whose ``container`` exited 1 with ``lines`` in its log, shown STOPPED, with no end record written yet."""
    launcher = rig.launcher()
    launch = launcher.attach(layer_spec())
    launcher.start(launch)
    check(launch.arn is not None, "the task started")
    rig.logs.streams[stream(rig, launch.arn or "", container)] = lines
    rig.ecs.finish(launch.arn or "", {container: 1, "publish": None})
    launcher.wait(launch)
    return launcher, launch


def test_the_failure_of_a_phase_is_kept_in_the_end_record() -> None:
    rig = Rig()
    launcher, launch = ended_without_a_record(rig, ["noise", phase_failure.line("build", CAUSE)])
    outcome = launcher.conclude(launch)
    check(outcome.outcome == "failed" and outcome.exit_code == 1, f"the launch still fails with the exit code: {outcome}")
    check(outcome.failure == f"phase build: {CAUSE}", f"the outcome carries the failure: {outcome.failure!r}")
    check(rig.record(1, "end").get("failure") == f"phase build: {CAUSE}", f"the durable end record holds it: {rig.record(1, 'end')}")


def test_a_log_with_no_marker_gives_its_last_line() -> None:
    rig = Rig()
    launcher, launch = ended_without_a_record(rig, ["Traceback (most recent call last):", "ValueError: the profile cannot be read"])
    check(launcher.conclude(launch).failure == "last log line: ValueError: the profile cannot be read", "the last line stands for a crash")


def test_the_summary_in_the_record_has_no_account_number_arn_or_secret() -> None:
    rig = Rig()
    launcher, launch = ended_without_a_record(rig, [phase_failure.line("build", f"denied for {ARN} in 111111111111 with {ASSIGNMENT} {ACCESS_KEY}")])
    launcher.conclude(launch)
    stored = json.dumps(rig.record(1, "end"))
    check("[account]" in stored and "[arn]" in stored and "[redacted]" in stored, f"the failure is recorded, cleaned: {stored}")
    for leak in ("111111111111", "arn:aws", "hunter2", ACCESS_KEY):
        check(leak not in stored.replace(launch.arn or "", ""), f"{leak} leaked into the durable record: {stored}")


def test_a_success_and_an_unreadable_log_add_no_failure_field() -> None:
    rig = Rig()
    launcher = rig.launcher()
    launch = launcher.attach(layer_spec())
    launcher.start(launch)
    rig.ecs.finish(launch.arn or "", {})
    launcher.wait(launch)
    check(launcher.conclude(launch).outcome == "succeeded" and "failure" not in rig.record(1, "end"), "a success records no failure")
    other = Rig()
    launcher, launch = ended_without_a_record(other, [])
    other.logs.streams.clear()
    check(launcher.conclude(launch).failure == "" and "failure" not in other.record(1, "end"), "a missing log leaves the record as it was")


def test_a_later_attempt_adopts_the_recorded_failure_and_keeps_it_when_it_copies_the_record() -> None:
    rig = Rig()
    launcher, launch = ended_without_a_record(rig, [phase_failure.line("build", CAUSE)])
    launcher.conclude(launch)
    again = rig.launcher().run(layer_spec(2))
    check(again.adopted and again.failure == f"phase build: {CAUSE}", f"the adopted outcome reports the failure: {again}")
    cancelled = rig.launcher().cancel(RUN, "gold", 3)
    check(cancelled.failure == f"phase build: {CAUSE}", f"a cancel of an ended launch reports it: {cancelled}")
    check(rig.record(3, "end").get("failure") == f"phase build: {CAUSE}", "and the copy of the record keeps it")


def test_a_terminal_outcome_adopted_from_the_task_state_derives_the_failure_from_the_log() -> None:
    """Attempt 1 started the task and went away. Attempt 2 finds the stopped task and writes the end record, so it reads the failing container's log."""
    rig = Rig()
    ended_without_a_record(rig, [phase_failure.line("build", CAUSE)])
    outcome = rig.launcher().attach(layer_spec(2)).outcome
    check(outcome is not None and outcome.adopted and outcome.failure == f"phase build: {CAUSE}", f"the adopted failure is derived: {outcome}")
    check(rig.record(1, "end").get("failure") == f"phase build: {CAUSE}", "and stored in the end record of the launch")


def evidence(rig: Rig) -> Any:
    from scripts.orchestration.raw_lineage_cases import run_evidence

    return run_evidence(rig)


def test_the_evidence_task_shows_the_failure_and_still_refuses() -> None:
    rig = Rig()
    launcher = rig.launcher()
    job = spec(environment={"gold": {"RUN_ID": RUN}, "verify": {"RUN_ID": RUN}}, log_container="verify", log_prefix="gold")
    launch = launcher.attach(job)
    launcher.start(launch)
    rig.logs.streams[f"gold/gold/{(launch.arn or '').rsplit('/', 1)[-1]}"] = [phase_failure.line("build", CAUSE)]
    rig.ecs.finish(launch.arn or "", {"gold": 3})
    launcher.wait(launch)
    launcher.conclude(launch)
    with pytest.raises(LaunchRefused) as caught:
        evidence(rig)
    check("failed" in str(caught.value) and f"phase build: {CAUSE}" in str(caught.value), f"the refusal shows the recorded failure: {caught.value}")


def test_the_operator_error_and_the_gate_input_check_name_the_failure() -> None:
    from scripts.orchestration.ecs_launch_operator_cases import context, operator

    rig = Rig()
    launcher = rig.launcher()
    job = spec(environment={"gold": {"RUN_ID": RUN}, "verify": {"RUN_ID": RUN}}, log_container="verify", log_prefix="gold")
    launch = launcher.attach(job)
    launcher.start(launch)
    rig.logs.streams[f"gold/gold/{(launch.arn or '').rsplit('/', 1)[-1]}"] = [phase_failure.line("build", CAUSE)]
    rig.ecs.finish(launch.arn or "", {"gold": 3})
    launcher.wait(launch)
    launcher.conclude(launch)
    with pytest.raises(JobFailed) as caught:
        operator(rig, 2, log_container="verify").execute(context(2))
    check(f"phase build: {CAUSE}" in str(caught.value), f"the failed job's error names the phase failure: {caught.value}")


def test_the_job_input_gate_names_the_failure_of_the_job_it_needs() -> None:
    from orchestration.ecs_preflight import Preflight

    rig = Rig()
    launcher, launch = ended_without_a_record(rig, [phase_failure.line("build", CAUSE)])
    launcher.conclude(launch)
    gate = cast(Any, SimpleNamespace(store=launcher.store))
    with pytest.raises(LaunchRefused) as caught:
        Preflight.require_passed(gate, "publish", "gold", RUN)
    check(f"phase build: {CAUSE}" in str(caught.value), f"the gate refusal shows why the earlier job failed: {caught.value}")
