"""Cases for the Spark progress pushes of the bronze job: a fake status tracker stands in for Spark's, and a loopback Pushgateway receives the pushes.

The real tracker is checked in the lakehouse image by the evidence run in data/local_keep (a local session, one failing job, the same reporter). These cases
cover the counting, the closed labels and the rule that a read or push failure never reaches the job. No AWS service is contacted and every thread ends.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

from scripts import metrics_push
from scripts.infrastructure.metrics_push_cases import DripGateway, StandIn, quiet
from scripts.lakehouse import spark_progress

RUNNING = ("bronze", "spark", "running")
SUCCEEDED = ("bronze", "spark", "succeeded")
FAILED = ("bronze", "spark", "failed")
NAMES = (
    "hai_spark_stages_active",
    "hai_spark_tasks_running",
    "hai_spark_tasks_completed",
    "hai_spark_tasks_failed",
    "hai_spark_jobs_running",
    "hai_spark_jobs_completed",
    "hai_spark_jobs_failed",
    "hai_spark_poll_timestamp_seconds",
)


@dataclass
class Job:
    status: str
    stageIds: list[int]


@dataclass
class Stage:
    numTasks: int
    numActiveTasks: int = 0
    numCompletedTasks: int = 0
    numFailedTasks: int = 0


@dataclass
class FakeTracker:
    jobs: dict[int, Job] = field(default_factory=dict)
    stages: dict[int, Stage] = field(default_factory=dict)
    fail: bool = False
    stage_reads: list[int] = field(default_factory=list)

    def getJobIdsForGroup(self, jobGroup: str | None = None) -> list[int]:
        if self.fail:
            raise RuntimeError("py4j gone")
        return sorted(self.jobs)

    def getJobInfo(self, jobId: int) -> Job | None:
        return self.jobs.get(jobId)

    def getStageInfo(self, stageId: int) -> Stage | None:
        self.stage_reads.append(stageId)
        return self.stages.get(stageId)

    def getActiveStageIds(self) -> list[int]:
        return [stage for job in self.jobs.values() if job.status == "RUNNING" for stage in job.stageIds if self.stages.get(stage, Stage(0)).numActiveTasks > 0]

    def getActiveJobsIds(self) -> list[int]:
        return [job for job, info in self.jobs.items() if info.status == "RUNNING"]


def scenario() -> FakeTracker:
    return FakeTracker(
        jobs={0: Job("SUCCEEDED", [0]), 1: Job("FAILED", [1]), 2: Job("RUNNING", [2, 3])},
        stages={0: Stage(4, 0, 4, 0), 1: Stage(2, 0, 1, 1), 2: Stage(8, 3, 4, 0), 3: Stage(8)},
    )


def values(samples: list[metrics_push.Sample]) -> dict[str, float]:
    return {sample.name: sample.value for sample in samples}


def counts_come_from_the_status_tracker(root: Path) -> dict[str, bool]:
    found = values(spark_progress.Progress(scenario()).poll())
    return {
        "one stage is active": found["hai_spark_stages_active"] == 1,
        "the running tasks are those of the active stages": found["hai_spark_tasks_running"] == 3,
        "completed tasks add up over every stage of every job": found["hai_spark_tasks_completed"] == 9,
        "failed tasks add up over every stage of every job": found["hai_spark_tasks_failed"] == 1,
        "one job runs, one succeeded and one failed": (found["hai_spark_jobs_running"], found["hai_spark_jobs_completed"], found["hai_spark_jobs_failed"])
        == (1, 1, 1),
        "the poll time is exposed so a stale group shows": found["hai_spark_poll_timestamp_seconds"] > 1_000_000_000,
        "the metrics are exactly the catalog's": set(found) == set(NAMES),
    }


def counts_never_fall_when_spark_forgets_a_job(root: Path) -> dict[str, bool]:
    tracker = scenario()
    progress = spark_progress.Progress(tracker)
    progress.poll()
    tracker.jobs[2] = Job("SUCCEEDED", [2, 3])
    tracker.stages[2] = Stage(8, 0, 8, 0)
    tracker.stages[3] = Stage(8, 0, 8, 0)
    second = values(progress.poll())
    tracker.jobs.clear()
    tracker.stages.clear()
    third = values(progress.poll())
    return {
        "a finished job moves from running to completed": second["hai_spark_jobs_completed"] == 2 and second["hai_spark_jobs_running"] == 0,
        "tasks of a stage that finished between two polls are counted": second["hai_spark_tasks_completed"] == 4 + 1 + 8 + 8,
        "forgotten jobs and stages do not lower the totals": third["hai_spark_jobs_completed"] == 2
        and third["hai_spark_tasks_completed"] == 21
        and third["hai_spark_tasks_failed"] == 1,
        "nothing runs once the tracker is empty": third["hai_spark_stages_active"] == 0 and third["hai_spark_tasks_running"] == 0,
    }


def settled_jobs_are_not_read_again(root: Path) -> dict[str, bool]:
    tracker = scenario()
    progress = spark_progress.Progress(tracker)
    progress.poll()
    tracker.stage_reads.clear()
    progress.poll()
    return {"only the unfinished job's stages are read on the next poll": sorted(set(tracker.stage_reads)) == [2, 3]}


def a_read_failure_raises_only_to_the_reporter(root: Path) -> dict[str, bool]:
    tracker = scenario()
    tracker.fail = True
    try:
        spark_progress.Progress(tracker).poll()
        raised = False
    except RuntimeError:
        raised = True
    return {"the poll raises so the reporter can log it": raised}


def reporter_pushes_the_running_group_then_the_final_one(root: Path) -> dict[str, bool]:
    tracker = scenario()
    with StandIn() as stand_in:
        reporter = spark_progress.Reporter(metrics_push.Pusher(stand_in.url, "bronze", "spark"), spark_progress.Progress(tracker), interval=0.05)
        reporter.start()
        deadline = time.monotonic() + 5
        while RUNNING not in stand_in.groups and time.monotonic() < deadline:
            time.sleep(0.02)
        running = set(stand_in.names(RUNNING))
        quiet(reporter.stop)
        again, _ = quiet(reporter.stop)
    return {
        "the running group carries the progress metrics": running == set(NAMES),
        "the final group replaces it": set(stand_in.groups) == {FAILED},
        "the final outcome is failed when a Spark job failed": stand_in.value(FAILED, "hai_spark_jobs_failed") == 1.0
        and stand_in.value(FAILED, "hai_spark_tasks_completed") == 9.0,
        "stop is safe to call twice": again is None,
        "the thread has ended": not any(thread.name == "spark_progress" for thread in threading.enumerate()),
    }


def a_run_without_failures_ends_succeeded(root: Path) -> dict[str, bool]:
    tracker = FakeTracker(jobs={0: Job("SUCCEEDED", [0])}, stages={0: Stage(4, 0, 4, 0)})
    with StandIn() as stand_in:
        reporter = spark_progress.Reporter(metrics_push.Pusher(stand_in.url, "bronze", "spark"), spark_progress.Progress(tracker), interval=0.05)
        reporter.start()
        time.sleep(0.2)
        quiet(reporter.stop)
    return {"the final group is succeeded": set(stand_in.groups) == {SUCCEEDED}}


def grouping_keys_are_job_phase_and_outcome_only(root: Path) -> dict[str, bool]:
    tracker = scenario()
    with StandIn() as stand_in:
        reporter = spark_progress.Reporter(metrics_push.Pusher(stand_in.url, "bronze", "spark"), spark_progress.Progress(tracker), interval=0.05)
        reporter.start()
        time.sleep(0.2)
        quiet(reporter.stop)
    paths = {path for _, path in stand_in.requests}
    return {
        "every request path is the three closed keys": paths
        <= {f"/metrics/job/bronze/phase/spark/outcome/{outcome}" for outcome in ("running", "succeeded", "failed")},
        "no label or key carries a run ID": all("2026" not in body for body in stand_in.groups.values()),
    }


def an_unreachable_gateway_never_fails_the_job(root: Path) -> dict[str, bool]:
    with StandIn() as stand_in:
        url = stand_in.url
    reporter = spark_progress.Reporter(
        metrics_push.Pusher(url, "bronze", "spark", request_timeout=0.5, total_timeout=1.0), spark_progress.Progress(scenario()), interval=0.05
    )
    started = time.monotonic()

    def run_reporter() -> None:
        reporter.start()
        time.sleep(0.3)
        reporter.stop()

    _, text = quiet(run_reporter)
    return {
        "stop returns quickly": time.monotonic() - started < 4.0,
        "the failure is named, and not once per heartbeat": 1 <= text.count("event=metrics_push_failed") <= 4,
    }


def a_failing_tracker_is_logged_once_and_the_reporter_goes_on(root: Path) -> dict[str, bool]:
    tracker = scenario()
    tracker.fail = True
    with StandIn() as stand_in:
        reporter = spark_progress.Reporter(metrics_push.Pusher(stand_in.url, "bronze", "spark"), spark_progress.Progress(tracker), interval=0.05)

        def run_reporter() -> None:
            reporter.start()
            time.sleep(0.4)

        stream_text = quiet(run_reporter)[1]
        tracker.fail = False
        deadline = time.monotonic() + 5
        while RUNNING not in stand_in.groups and time.monotonic() < deadline:
            time.sleep(0.02)
        recovered = RUNNING in stand_in.groups
        quiet(reporter.stop)
    return {
        "a failing read is named once": stream_text.count("event=spark_progress_failed") == 1,
        "nothing stale is pushed while the read fails, and pushing resumes after": recovered,
    }


def start_only_for_bronze_with_an_address(root: Path) -> dict[str, bool]:
    spark = SimpleNamespace(sparkContext=SimpleNamespace(statusTracker=scenario))
    no_tracker_spark = SimpleNamespace(sparkContext=SimpleNamespace())

    with StandIn() as stand_in:
        good = {"PUSHGATEWAY_URL": stand_in.url, "HAI_JOB": "bronze"}
        started = spark_progress.maybe_start(spark, good, interval=60)
        other_job = spark_progress.maybe_start(spark, {**good, "HAI_JOB": "gold"}, interval=60)
        no_address = spark_progress.maybe_start(spark, {"HAI_JOB": "bronze"}, interval=60)
        no_tracker, text = quiet(lambda: spark_progress.maybe_start(no_tracker_spark, good, interval=60))
        if started is not None:
            quiet(started.stop)
    return {
        "bronze with an address starts a reporter": started is not None,
        "another job starts none, because only bronze is in scope": other_job is None,
        "no address starts none": no_address is None,
        "a session without a status tracker starts none and says so": no_tracker is None and "event=spark_progress_skipped" in text,
    }


def the_final_push_of_a_session_ends_within_the_budget(root: Path) -> dict[str, bool]:
    tracker = scenario()
    with DripGateway() as gateway:
        reporter = spark_progress.Reporter(
            metrics_push.Pusher(gateway.url, "bronze", "spark", request_timeout=1.0, total_timeout=1.5), spark_progress.Progress(tracker), interval=0.05
        )
        reporter.start()
        time.sleep(0.3)
        started = time.monotonic()
        quiet(reporter.stop)
        elapsed = time.monotonic() - started
    return {
        "stop ends within the total budget of the pusher, plus a margin, although every request is answered one byte at a time": elapsed < 2.3,
        "the thread has ended": not any(thread.name == "spark_progress" for thread in threading.enumerate()),
    }


def catalog_and_phase_table_know_the_spark_group(root: Path) -> dict[str, bool]:
    return {
        "every metric is in the push catalog with no label": all(name in metrics_push.CATALOG and metrics_push.CATALOG[name][1] == () for name in NAMES),
        "spark is a phase of the bronze family": "spark" in metrics_push.JOB_PHASES["bronze"],
    }


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "spark_counts_come_from_the_status_tracker": counts_come_from_the_status_tracker,
    "spark_counts_never_fall_when_spark_forgets_a_job": counts_never_fall_when_spark_forgets_a_job,
    "spark_settled_jobs_are_not_read_again": settled_jobs_are_not_read_again,
    "spark_a_read_failure_raises_only_to_the_reporter": a_read_failure_raises_only_to_the_reporter,
    "spark_reporter_pushes_the_running_group_then_the_final_one": reporter_pushes_the_running_group_then_the_final_one,
    "spark_a_run_without_failures_ends_succeeded": a_run_without_failures_ends_succeeded,
    "spark_grouping_keys_are_job_phase_and_outcome_only": grouping_keys_are_job_phase_and_outcome_only,
    "spark_an_unreachable_gateway_never_fails_the_job": an_unreachable_gateway_never_fails_the_job,
    "spark_a_failing_tracker_is_logged_once_and_the_reporter_goes_on": a_failing_tracker_is_logged_once_and_the_reporter_goes_on,
    "spark_start_only_for_bronze_with_an_address": start_only_for_bronze_with_an_address,
    "spark_the_final_push_of_a_session_ends_within_the_budget": the_final_push_of_a_session_ends_within_the_budget,
    "spark_catalog_and_phase_table_know_the_spark_group": catalog_and_phase_table_know_the_spark_group,
}
