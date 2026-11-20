"""The failed phase reaches the durable launch record (the end record) as a closed record, and the cause is the right container.

The cases run the real launcher over the repository's fake ECS, log service and write-once bucket. A layer task logs each phase in its own container and the
job's log container is the last one (``publish``), so the cause is resolved once from the task's containers and used for the durable record and the tail."""

from __future__ import annotations

import itertools
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from orchestration.ecs_adapter import TaskReport, container_reason_code
from orchestration.ecs_launch import Launch, Launcher, LaunchRefused
from orchestration.ecs_launch_operator import JobFailed
from orchestration.ecs_preflight import Preflight
from scripts import phase_failure
from scripts.orchestration import ecs_launch_cases
from scripts.orchestration.ecs_launch_cases import RUN, Rig, spec
from tests.failure_support import engine_facts
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
LAYER = ["fetch", "pin", "build", "validate", "export", "publish", "upload", "maintain"]
GUARD_TIMEOUT = "guard: the task passed its time limit, so it stops"
GUARD_PHASE = "guard: phase build failed, so the task stops"
# Values of the shapes that must never be kept are built from parts, so no scanner reads them as real secrets in this file.
ACCESS_KEY = "AK" + "IA" + "ABCDEFGH" + "IJKLMNOP"
ASSIGNMENT = "pass" + "word=" + "hunter2"
HOSTILE = [
    "Author" + "ization: Bearer synthetic.jwt.value",
    ACCESS_KEY[:8] + "\n" + ACCESS_KEY[8:],
    "".join(f"%{ord(letter):02x}" for letter in ACCESS_KEY),
    "s3://synthetic-private-bucket/object",
    "Conversion Error: could not convert string SYNTHETIC_ROW_VALUE_42 to DOUBLE",
    "https://synthetic-private-bucket.s3.amazonaws.com/object?X-Amz-Signature=synthetic.signature.value",
    "%31" * 12,
    "111111\n111111",
    ASSIGNMENT,
]
FLOOR = phase_failure.record("build", ["capacity_engine_floor"], engine_facts())
# What the messages of the end record say for that refusal: every figure comes from the record itself, and the wording from fixed templates.
FLOOR_TEXT = "only -0.25 GiB is left for the engines, below the floor of 2.00 GiB (memory 1.00 GiB, headroom 1.00 GiB, 1 other containers)"


def gold_spec(attempt: int = 1, containers: list[str] | None = None) -> Any:
    names = containers or LAYER
    return spec(attempt, environment={name: {"RUN_ID": RUN} for name in names}, log_container="publish", log_prefix="gold", acceptance=("publish",))


def stream(arn: str, container: str) -> str:
    return f"gold/{container}/{arn.rsplit('/', 1)[-1]}"


def ended(
    codes: dict[str, int | None], logs: dict[str, list[str]], reasons: dict[str, str] | None = None, attempt_logs: bool = True
) -> tuple[Rig, Launcher, Launch]:
    """A gold task whose containers exited with ``codes`` (a container not named exits 0) and logged ``logs``, shown STOPPED, with no end record yet."""
    rig = Rig()
    launcher = rig.launcher()
    launch = launcher.attach(gold_spec())
    launcher.start(launch)
    arn = launch.arn or ""
    if attempt_logs:
        for name, lines in logs.items():
            rig.logs.streams[stream(arn, name)] = lines
    rig.ecs.finish(arn, codes)
    for container in rig.ecs.tasks[arn]["containers"]:
        if container["name"] in (reasons or {}):
            container["reason"] = (reasons or {})[container["name"]]
    launcher.wait(launch)
    return rig, launcher, launch


def stored(rig: Rig, attempt: int = 1) -> dict[str, Any]:
    return rig.record(attempt, "end")


def test_the_closed_record_of_the_failed_phase_is_kept_in_the_end_record_and_rendered_from_templates() -> None:
    rig, launcher, launch = ended({"build": 1, "publish": None}, {"build": ["noise", phase_failure.line(FLOOR)]})
    outcome = launcher.conclude(launch)
    expected = {
        "v": 1,
        "phase": "build",
        "codes": ["capacity_engine_floor"],
        "facts": {**FLOOR["facts"], "exit_code": 1},
        "container": "build",
        "source": "job",
    }
    check(outcome.outcome == "failed" and outcome.exit_code == 1, f"the launch still fails with its exit code: {outcome}")
    check(stored(rig).get("failure") == expected, f"the durable end record holds the closed record: {stored(rig)}")
    check(outcome.failure == f"phase build failed in container build: {FLOOR_TEXT}", f"the message is rendered from the template: {outcome.failure!r}")


def test_the_plain_reason_and_exit_code_of_the_end_record_agree_with_its_failure_record() -> None:
    """A phase that fails stops the containers before it with SIGTERM (143) and ECS words the stop "Task failed to start": the end record names the cause."""
    rig = Rig()
    launcher = rig.launcher()
    launch = launcher.attach(gold_spec())
    launcher.start(launch)
    arn = launch.arn or ""
    rig.logs.streams[stream(arn, "build")] = [phase_failure.line(FLOOR)]
    rig.ecs.finish(arn, {"fetch": 143, "build": 1, "publish": None})
    rig.ecs.tasks[arn]["stoppedReason"] = "Task failed to start"
    launcher.wait(launch)
    outcome = launcher.conclude(launch)
    end = stored(rig)
    check(end["failure"]["container"] == "build" and end["failure"]["facts"]["exit_code"] == 1, f"the structured block names the build container: {end}")
    check(
        end["exit_code"] == 1 and outcome.exit_code == 1,
        f"the exit code is the build container's, not a stopped sibling's: {end['exit_code']} and {outcome.exit_code}",
    )
    check(end["reason"] == outcome.failure and "failed to start" not in end["reason"], f"the reason is the rendered failure: {end['reason']!r}")
    again = rig.launcher().run(gold_spec(2))
    check(again.adopted and again.exit_code == 1 and again.reason == end["reason"], f"a later attempt reports the same pair: {again}")


@pytest.mark.parametrize("text", HOSTILE)
def test_no_job_text_reaches_the_record_in_any_form(text: str) -> None:
    forged = [
        phase_failure.MARKER + json.dumps({"v": 1, "phase": "build", "codes": ["phase_failed"], "facts": {}, "reason": text}),
        phase_failure.MARKER + json.dumps({"v": 1, "phase": text, "codes": ["phase_failed"], "facts": {}}),
        phase_failure.MARKER + json.dumps({"v": 1, "phase": "build", "codes": [text], "facts": {}}),
        phase_failure.MARKER + json.dumps({"v": 1, "phase": "build", "codes": ["phase_failed"], "facts": {"memory": text}}),
        "error: " + text,
        *text.splitlines(),
        text.replace("\n", "\r\n"),
    ]
    rig, launcher, launch = ended({"build": 1, "publish": None}, {"build": forged})
    outcome = launcher.conclude(launch)
    keep = json.dumps({**stored(rig), "task_arn": ""}, sort_keys=True) + outcome.failure
    for part in [text, *text.splitlines(), "synthetic", "SYNTHETIC", "hunter2", "Bearer"]:
        if part:
            check(part not in keep, f"{part!r} reached the durable record: {keep}")
    check(
        stored(rig)["failure"]["codes"] == ["phase_failed"] and stored(rig)["failure"]["facts"] == {"exit_code": 1},
        f"the record is the one derived from the task state: {stored(rig)}",
    )


def test_a_failed_container_with_no_usable_log_still_leaves_a_safe_record() -> None:
    rig, launcher, launch = ended({"build": 1, "publish": None}, {}, attempt_logs=False)
    launcher.conclude(launch)
    check(
        stored(rig)["failure"] == {"v": 1, "phase": "build", "codes": ["phase_failed"], "facts": {"exit_code": 1}, "container": "build", "source": "task"},
        f"{stored(rig)}",
    )


def test_a_success_adds_no_failure_field_and_keeps_its_handling() -> None:
    rig, launcher, launch = ended({}, {"publish": ["publish finished successfully"]})
    outcome = launcher.conclude(launch)
    check(outcome.outcome == "succeeded" and "failure" not in stored(rig) and outcome.failure == "" and outcome.logs == "", f"{outcome} {stored(rig)}")


def test_a_later_attempt_adopts_and_copies_the_closed_record() -> None:
    rig, launcher, launch = ended({"build": 1, "publish": None}, {"build": [phase_failure.line(FLOOR)]})
    first = launcher.conclude(launch)
    again = rig.launcher().run(gold_spec(2))
    check(again.adopted and again.failure == first.failure and first.failure != "", f"the adopted outcome reports it: {again}")
    cancelled = rig.launcher().cancel(RUN, "gold", 3)
    check(cancelled.failure == first.failure and stored(rig, 3)["failure"] == stored(rig)["failure"], "a cancel of an ended launch copies it")


def test_a_terminal_outcome_adopted_from_the_task_state_derives_the_record_from_the_log() -> None:
    rig, _, _ = ended({"build": 1, "publish": None}, {"build": [phase_failure.line(FLOOR)]})
    outcome = rig.launcher().attach(gold_spec(2)).outcome
    check(outcome is not None and outcome.adopted and outcome.failure != "", f"the adopted failure is derived: {outcome}")
    check(stored(rig)["failure"]["codes"] == ["capacity_engine_floor"], "and stored in the end record of the launch")


def test_the_launcher_refuses_to_read_an_end_record_with_an_invalid_failure() -> None:
    from orchestration.ecs_launch import LaunchError

    rig, launcher, launch = ended({"build": 1, "publish": None}, {"build": [phase_failure.line(FLOOR)]})
    launcher.conclude(launch)
    key = rig.key(1, "end")
    record = json.loads(rig.bucket.objects[key])
    for broken in (
        {**record["failure"], "codes": ["free text here"]},
        {**record["failure"], "extra": 1},
        "free text",
        {**record["failure"], "container": "x y"},
    ):
        rig.bucket.objects[key] = json.dumps({**record, "failure": broken}).encode()
        with pytest.raises(LaunchError):
            rig.launcher().attach(gold_spec(2))


# The cause is resolved once: a primary phase failure beats shutdown siblings, a failure without an exit code keeps its status code, and the guard is the cause
# only when it reports its own timeout.


def diagnose(codes: dict[str, int | None], logs: dict[str, list[str]], reasons: dict[str, str] | None = None, task_order: list[str] | None = None) -> Any:
    rig = Rig()
    arn = "arn:aws:ecs:us-west-2:111111111111:task/cluster/0123456789abcdef0123456789abcdef"
    for name, lines in logs.items():
        rig.logs.streams[stream(arn, name)] = lines
    containers = tuple(codes.items())
    coded = tuple((name, container_reason_code(text)) for name, text in (reasons or {}).items())
    state = TaskReport("failed", next((c for _, c in containers if c not in (0, None)), 125), "Essential container in task exited", containers, coded)
    return rig.launcher().diagnose(gold_spec(containers=task_order or [name for name, _ in containers]), arn, state)


@pytest.mark.parametrize("order", list(itertools.permutations(["fetch", "build", "publish"])))
def test_a_primary_failure_beats_a_shutdown_sibling_in_every_response_order(order: tuple[str, ...]) -> None:
    exits = {"fetch": 143, "build": 1, "publish": None}
    found = diagnose({name: exits[name] for name in order}, {"fetch": ["fetch interrupted"], "build": [phase_failure.line(FLOOR)]})
    check(found.record is not None and found.record["container"] == "build" and found.record["codes"] == ["capacity_engine_floor"], f"{order}: {found.record}")
    check("floor" in found.logs.lower() or phase_failure.MARKER in found.logs, f"and the tail is the cause's own log: {found.logs!r}")


def test_the_first_primary_failure_in_task_order_wins_whatever_the_response_order() -> None:
    for order in itertools.permutations(["validate", "build", "export"]):
        exits = {"build": 1, "validate": 2, "export": 3}
        found = diagnose({name: exits[name] for name in order}, {}, task_order=["build", "validate", "export"])
        check(found.record["container"] == "build", f"{order}: task order decides, not the reply order: {found.record}")


@pytest.mark.parametrize("failing", range(len(LAYER)))
def test_a_failure_after_publish_shows_its_own_log_and_not_the_log_of_a_successful_phase(failing: int) -> None:
    name = LAYER[failing]
    exits: dict[str, int | None] = {phase: (0 if index < failing else None) for index, phase in enumerate(LAYER)}
    exits[name] = 1
    exits["guard"] = 1
    logs = {phase: [f"{phase} finished successfully"] for phase in LAYER[:failing]}
    logs[name] = ["last words", phase_failure.line(phase_failure.record(name, ["phase_failed"]))]
    found = diagnose(exits, logs)
    check(found.record["container"] == name and found.record["phase"] == name, f"{name}: {found.record}")
    check(
        "finished successfully" not in found.logs and "last words" in found.logs, f"{name}: the immediate tail is not a successful phase's log: {found.logs!r}"
    )


def test_every_base_job_resolves_its_failed_phase_at_every_position() -> None:
    table = json.loads((ROOT / "config/orchestration/ecs_broker_jobs.json").read_text())["jobs"]
    positions = 0
    for job, row in table.items():
        names = list(row["containers"])
        for index, name in enumerate(names):
            exits: dict[str, int | None] = {phase: (0 if place < index else None) for place, phase in enumerate(names)} | {name: 1}
            rig = Rig()
            arn = "arn:aws:ecs:us-west-2:111111111111:task/cluster/0123456789abcdef0123456789abcdef"
            rig.logs.streams[f"{job}/{name}/{arn.rsplit('/', 1)[-1]}"] = [phase_failure.line(phase_failure.record(name, ["phase_failed"]))]
            job_spec = spec(1, environment={phase: {} for phase in names}, log_container=row["log_container"], log_prefix=job, acceptance=())
            state = TaskReport("failed", 1, "Essential container in task exited", tuple(exits.items()))
            found = rig.launcher().diagnose(job_spec, arn, state)
            check(found.record is not None and found.record["container"] == name, f"{job} {name}: {found.record}")
            positions += 1
    check(positions >= 40, f"positions checked: {positions}")


def test_a_failure_with_no_exit_code_keeps_a_bounded_status_code_and_the_guard_does_not_hide_it() -> None:
    found = diagnose(
        {"build": None, "guard": 1, "publish": None},
        {"guard": [GUARD_PHASE], "build": ["secret detail " + ASSIGNMENT]},
        {"build": "CannotStartContainerError: " + ASSIGNMENT + " synthetic initialization failed"},
    )
    expected = {"v": 1, "phase": "build", "codes": ["container_start_error"], "facts": {}, "container": "build", "source": "task"}
    check(found.record == expected, f"{found.record}")
    check("hunter2" not in json.dumps(found.record), "no reason text is stored")


def test_the_adapter_turns_the_reason_of_a_container_into_a_code_and_the_record_keeps_only_that() -> None:
    """Through the real adapter and launcher: the reason text of ECS never reaches the report or the end record."""
    rig, launcher, launch = ended(
        {"build": None, "guard": 1, "publish": None}, {"guard": [GUARD_PHASE]}, {"build": "CannotStartContainerError: " + ASSIGNMENT + " failed"}
    )
    check(launch.report is not None and launch.report.reasons == (("build", "container_start_error"),), f"the report holds the code only: {launch.report}")
    outcome = launcher.conclude(launch)
    expected = {"v": 1, "phase": "build", "codes": ["container_start_error"], "facts": {}, "container": "build", "source": "task"}
    check(stored(rig)["failure"] == expected and "hunter2" not in json.dumps(stored(rig)) + outcome.failure, f"{stored(rig)}")


def test_an_unknown_container_reason_is_one_safe_code() -> None:
    found = diagnose({"build": None, "guard": 1}, {}, {"build": "SomethingNew: " + ASSIGNMENT})
    check(found.record["codes"] == ["container_reason_other"], f"{found.record}")


def test_the_guard_is_the_cause_only_when_it_reports_its_own_timeout() -> None:
    found = diagnose({"fetch": 143, "build": None, "guard": 1}, {"guard": ["guard: watching", GUARD_TIMEOUT]})
    check(
        found.record == {"v": 1, "phase": "guard", "codes": ["guard_timeout"], "facts": {"exit_code": 1}, "container": "guard", "source": "guard"},
        f"{found.record}",
    )
    again = diagnose({"build": 1, "guard": 1}, {"guard": [GUARD_PHASE], "build": [phase_failure.line(FLOOR)]})
    check(
        again.record["container"] == "build" and again.record["codes"] == ["capacity_engine_floor"],
        f"a guard that only stops a failed task is not the cause: {again.record}",
    )
    both = diagnose({"build": 1, "guard": 1}, {"guard": [GUARD_TIMEOUT], "build": [phase_failure.line(FLOOR)]})
    check(both.record["container"] == "build", "a primary phase failure beats a guard timeout")


def test_only_shutdown_siblings_give_a_stopped_record_and_nothing_gives_no_record() -> None:
    stopped = diagnose({"fetch": 143, "build": None, "guard": 1}, {"guard": [GUARD_PHASE]})
    check(stopped.record is not None and stopped.record["codes"] == ["phase_stopped"] and stopped.record["container"] == "fetch", f"{stopped.record}")
    none = diagnose({"build": None, "publish": None}, {})
    check(none.record is None and none.logs == "", "no cause, no record")


def test_a_killed_container_with_a_memory_reason_is_a_primary_failure() -> None:
    found = diagnose({"fetch": 143, "build": 137, "publish": None}, {}, {"build": "OutOfMemoryError: Container killed due to memory usage"})
    check(found.record["container"] == "build" and found.record["facts"] == {"exit_code": 137}, f"{found.record}")


# The messages that show the record keep failing the job.


def finished_gold(rig: Rig, line: str | None = None) -> Launcher:
    launcher = rig.launcher()
    job = spec(environment={"build": {"RUN_ID": RUN}, "publish": {"RUN_ID": RUN}}, log_container="publish", log_prefix="gold")
    launch = launcher.attach(job)
    launcher.start(launch)
    if line:
        rig.logs.streams[f"gold/build/{(launch.arn or '').rsplit('/', 1)[-1]}"] = [line]
    rig.ecs.finish(launch.arn or "", {"build": 3})
    launcher.wait(launch)
    launcher.conclude(launch)
    return launcher


def test_the_evidence_task_shows_the_rendered_record_and_still_refuses() -> None:
    from scripts.orchestration.raw_lineage_cases import run_evidence

    rig = Rig()
    finished_gold(rig, phase_failure.line(phase_failure.record("build", ["capacity_engine_floor"], engine_facts())))
    with pytest.raises(LaunchRefused) as caught:
        run_evidence(rig)
    check(
        f"phase build failed in container build: {FLOOR_TEXT}" in str(caught.value), f"the refusal shows the rendered message, not the record: {caught.value}"
    )
    check("capacity_engine_floor" not in str(caught.value) and "{" not in str(caught.value), f"and no dictionary: {caught.value}")


def test_the_operator_error_and_the_gate_input_check_name_the_rendered_record() -> None:
    from scripts.orchestration.ecs_launch_operator_cases import context, operator

    rig = Rig()
    finished_gold(rig, phase_failure.line(phase_failure.record("build", ["capacity_engine_floor"], engine_facts())))
    with pytest.raises(JobFailed) as caught:
        operator(rig, 2, log_container="publish").execute(context(2))
    check(f"phase build failed in container build: {FLOOR_TEXT}" in str(caught.value), f"the failed job's error names the record: {caught.value}")
    check(str(caught.value).count(FLOOR_TEXT) == 1, f"and names it once, not as the reason and again as the failure: {caught.value}")
    gate = cast(Any, SimpleNamespace(store=rig.launcher().store))
    with pytest.raises(LaunchRefused) as refused:
        Preflight.require_passed(gate, "publish", "gold", RUN)
    check(f"phase build failed in container build: {FLOOR_TEXT}" in str(refused.value), f"the gate refusal shows the rendered message: {refused.value}")
    check("capacity_engine_floor" not in str(refused.value) and "{" not in str(refused.value), f"and no dictionary: {refused.value}")


def test_the_guard_lines_that_the_launcher_compares_are_the_ones_the_guard_writes() -> None:
    """The guard's command is inlined into the task definition, so the launcher reads its fixed lines and the guard is not changed."""
    from orchestration.ecs_launch import GUARD_TIMEOUT_LINE

    source = (ROOT / "scripts/orchestration/task_guard.py").read_text()
    check(GUARD_TIMEOUT_LINE == GUARD_TIMEOUT and f'say("{GUARD_TIMEOUT_LINE}")' in source, "the time limit line is the guard's own")
    check(GUARD_PHASE.replace("build", "{found}") in source, "and the phase line the launcher must not treat as a guard cause is too")


# The tail that a timeout or a cancel shows comes from the phase that was active, not from a log container that never ran.


def stopped_task(containers: list[str], logs: dict[str, list[str]], cancel: bool = False, deadline: bool = False) -> tuple[Rig, Any]:
    """A layer-shaped gold task stopped while ``build`` was active: by a cancel request, or by the deadline of the real virtual clock."""
    from dataclasses import replace

    rig = Rig()
    launcher = rig.launcher()
    launch = launcher.attach(replace(gold_spec(containers=containers), deadline_seconds=1 if deadline else 3600))
    launcher.start(launch)
    for name, lines in logs.items():
        rig.logs.streams[stream(launch.arn or "", name)] = lines
    if deadline:
        rig.time.now += 2
    else:
        rig.ecs.finish(launch.arn or "", {"fetch": 0, "pin": 0, "build": 143, "validate": None, "export": None, "publish": None})
    if cancel:
        launcher.store.put_once(
            {"schema_version": 1, "event": "cancel", "run_id": RUN, "job": "gold", "attempt": 1, "at": "2026-11-19T00:00:00Z", "reason": "cancel"}
        )
    launcher.wait(launch)
    return rig, (launcher, launch)


ACTIVE = ["fetch", "pin", "build", "validate", "export", "publish"]
PROGRESS = {"build": ["build progress line 1", "build progress line 2"]}
FINISHED = {**PROGRESS, "pin": ["pin finished successfully"]}


def test_a_cancel_shows_the_tail_of_the_active_phase_when_the_log_container_never_ran() -> None:
    rig, (launcher, launch) = stopped_task(ACTIVE, FINISHED, cancel=True)
    outcome = launcher.conclude(launch)
    check(outcome.outcome == "cancelled", f"{outcome}")
    check(
        "build progress line 2" in outcome.logs and "pin finished successfully" not in outcome.logs, f"the cancel tail is the active phase's: {outcome.logs!r}"
    )
    check("failure" not in stored(rig), "a cancel keeps no failure record")


def test_a_timeout_on_the_real_clock_shows_the_tail_of_the_active_phase() -> None:
    rig, (launcher, launch) = stopped_task(ACTIVE, PROGRESS, deadline=True)
    check(launch.timed_out and launch.report is not None, f"the stopped report of the timeout is kept: {launch.report}")
    outcome = launcher.conclude(launch)
    check(
        outcome.outcome == "timeout" and "build progress line 2" in outcome.logs, f"the timeout tail is the active phase's: {outcome.outcome} {outcome.logs!r}"
    )
    check("failure" not in stored(rig), "a timeout keeps no failure record")


def test_a_stop_while_a_later_phase_runs_shows_that_phase_and_not_the_successful_log_container() -> None:
    names = ["fetch", "pin", "build", "validate", "export", "publish", "upload", "maintain"]
    rig = Rig()
    launcher = rig.launcher()
    launch = launcher.attach(gold_spec(containers=names))
    launcher.start(launch)
    rig.logs.streams[stream(launch.arn or "", "publish")] = ["publish finished successfully"]
    rig.logs.streams[stream(launch.arn or "", "maintain")] = ["maintain progress"]
    rig.ecs.finish(launch.arn or "", {"publish": 0, "upload": 0, "maintain": 143, **{name: 0 for name in names[:5]}})
    launcher.store.put_once(
        {"schema_version": 1, "event": "cancel", "run_id": RUN, "job": "gold", "attempt": 1, "at": "2026-11-19T00:00:00Z", "reason": "cancel"}
    )
    launcher.wait(launch)
    outcome = launcher.conclude(launch)
    check(outcome.logs == "maintain progress", f"{outcome.logs!r}")


def test_a_timeout_or_cancel_with_no_stopped_phase_keeps_the_configured_tail() -> None:
    rig = Rig()
    launcher = rig.launcher()
    job = spec(environment={"build": {"RUN_ID": RUN}, "publish": {"RUN_ID": RUN}}, log_container="publish", log_prefix="gold")
    launch = launcher.attach(job)
    launcher.start(launch)
    rig.logs.streams[stream(launch.arn or "", "publish")] = ["publish tail"]
    rig.ecs.finish(launch.arn or "", {})
    launcher.store.put_once(
        {"schema_version": 1, "event": "cancel", "run_id": RUN, "job": "gold", "attempt": 1, "at": "2026-11-19T00:00:00Z", "reason": "cancel"}
    )
    launcher.wait(launch)
    check(launcher.conclude(launch).logs == "publish tail", "no stopped phase, so the configured container's tail")


# A training run whose last action found no fitted type: the train container exits 1 with the named failure, the launcher records the launch as failed and the
# promotion chain and the research readiness do not count it as a passed training run.


def train_rig() -> Rig:
    from dataclasses import replace

    rig = Rig()
    rig.ecs.register("example_dev_job_train_host", ecs_launch_cases.NEED.memory_mib, None)
    rig.adapter = ecs_launch_cases.EcsAdapter(rig.ecs, rig.logs, replace(ecs_launch_cases.SETTINGS, families=frozenset({"gold", "drift", "train"})))
    return rig


def train_spec() -> Any:
    names = ["prepare", "extract", "train"]
    revision = ecs_launch_cases.REVISIONS["gold"].replace("_gold_", "_train_")
    return spec(
        job="train",
        environment={name: {"RUN_ID": RUN} for name in names},
        log_container="train",
        log_prefix="train",
        acceptance=("train",),
        task_definition=revision,
    )


def train_ended(codes: dict[str, int | None], logs: list[str]) -> tuple[Rig, Launcher, Launch]:
    rig = train_rig()
    launcher = rig.launcher()
    launch = launcher.attach(train_spec())
    launcher.start(launch)
    arn = launch.arn or ""
    rig.logs.streams[f"train/train/{arn.rsplit('/', 1)[-1]}"] = logs
    rig.ecs.finish(arn, codes)
    launcher.wait(launch)
    return rig, launcher, launch


def nothing_fitted_logs() -> list[str]:
    from scripts.training import aws_run

    detail = "model m6 was fitted for none of HAI_1 (HAI_1: not_estimable: panel has no complete repeated training hospitals)"
    failure = aws_run.StepFailed("log_run", 1, "nothing_fitted", detail)
    failure.failure_codes = phase_failure.nothing_fitted_codes({"HAI_1": "not_estimable"})
    named = json.dumps({"phase": "train", "step": "log_run", "failure": "nothing_fitted", "detail": detail}, sort_keys=True)
    return [
        '{"code": 0, "resumed": false, "step": "version_extract"}',
        '{"code": 0, "resumed": false, "step": "fit"}',
        named,
        phase_failure.line(phase_failure.record_for("train", failure)),
    ]


def test_a_training_run_that_fitted_nothing_is_recorded_as_a_failed_launch_with_its_named_reason() -> None:
    from orchestration import promotion_chain

    rig, launcher, launch = train_ended({"prepare": 0, "extract": 0, "train": 1}, nothing_fitted_logs())
    outcome = launcher.conclude(launch)
    stored_end = json.loads(rig.bucket.objects[rig.key(1, "end", "train")])
    check(outcome.outcome == "failed" and outcome.exit_code == 1, f"the launch of a run that fitted nothing: {outcome}")
    check(
        stored_end["outcome"] == "failed" and stored_end["failure"]["container"] == "train" and stored_end["failure"]["phase"] == "train",
        f"the end record: {stored_end}",
    )
    check(stored_end["failure"]["codes"] == ["nothing_fitted", "hai_1_not_estimable"], f"the closed codes of the run: {stored_end['failure']}")
    check(stored_end["failure"]["facts"] == {"exit_code": 1}, f"no number but the exit code: {stored_end['failure']}")
    expected = "phase train failed in container train: no requested type was fitted; HAI_1 is not estimable"
    check(outcome.failure == expected, f"the message names the reason and the status of the type: {outcome.failure!r}")
    check(not promotion_chain.trained(launcher.store, RUN), "the promotion chain counts the failed launch as a passed training run")


def test_a_training_run_that_fitted_a_type_is_recorded_as_a_passed_launch() -> None:
    from orchestration import promotion_chain

    rig, launcher, launch = train_ended(
        {"prepare": 0, "extract": 0, "train": 0}, ['{"outcome": "partly_fitted", "unfitted": {"HAI_2": {"status": "not_estimable"}}}']
    )
    outcome = launcher.conclude(launch)
    check(outcome.outcome == "succeeded" and promotion_chain.trained(launcher.store, RUN), f"a run with a fitted type: {outcome}")
