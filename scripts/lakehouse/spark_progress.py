"""Push the progress of the bronze job's Spark session to the Pushgateway from the process that runs Spark, and never let a read or a push fail the job.

``scripts.lakehouse.bronze`` runs as a child process of the bronze task phase, so the heartbeat of that phase cannot see its Spark session. This module
starts one more thread inside the child, right after ``scripts.lakehouse.session.spark_session`` builds the session. Every ``HEARTBEAT_SECONDS`` it reads
Spark's status tracker (public methods of ``SparkContext.statusTracker()``, which work with ``spark.ui.enabled`` off) and pushes the group ``{job=bronze,
phase=spark, outcome=running}``; at exit it pushes the totals as ``succeeded`` (no Spark job failed) or ``failed`` and deletes the running group.

The tracker gives the active stages, the tasks of each stage and the status of each job. Totals add up the largest task counts seen per stage, and a job that
reached a final status is read once, so a total never falls when Spark forgets old jobs. The tracker has no heap reading: the driver heap needs the private
``_jvm`` bridge, so it is not pushed. The container's own memory is already pushed by the task phase heartbeat.
"""

from __future__ import annotations

import atexit
import os
import threading
import time
from collections.abc import Mapping
from typing import Any

from scripts import metrics_push

JOB = "bronze"
PHASE = "spark"
JOIN_SHARE = 0.3
FINAL_STATUSES = ("SUCCEEDED", "FAILED")


class Progress:
    """The totals of one Spark session, read from its status tracker."""

    def __init__(self, tracker: Any) -> None:
        self.tracker = tracker
        self.outcomes: dict[int, str] = {}
        self.tasks: dict[int, tuple[int, int]] = {}

    def fold_stages(self, stage_ids: list[int]) -> None:
        for stage_id in stage_ids:
            stage = self.tracker.getStageInfo(stage_id)
            if stage is not None:
                done, failed = self.tasks.get(stage_id, (0, 0))
                self.tasks[stage_id] = (max(done, int(stage.numCompletedTasks)), max(failed, int(stage.numFailedTasks)))

    def poll(self) -> list[metrics_push.Sample]:
        """One reading; an error of the tracker propagates to the reporter."""
        for job_id in self.tracker.getJobIdsForGroup():
            if job_id in self.outcomes:
                continue
            info = self.tracker.getJobInfo(job_id)
            if info is None:
                continue
            self.fold_stages(list(info.stageIds))
            if str(info.status) in FINAL_STATUSES:
                self.outcomes[job_id] = str(info.status)
        active = list(self.tracker.getActiveStageIds())
        running = 0
        for stage_id in active:
            stage = self.tracker.getStageInfo(stage_id)
            running += int(stage.numActiveTasks) if stage is not None else 0
        values = {
            "hai_spark_stages_active": len(active),
            "hai_spark_tasks_running": running,
            "hai_spark_tasks_completed": sum(done for done, _ in self.tasks.values()),
            "hai_spark_tasks_failed": sum(failed for _, failed in self.tasks.values()),
            "hai_spark_jobs_running": len(list(self.tracker.getActiveJobsIds())),
            "hai_spark_jobs_completed": sum(1 for status in self.outcomes.values() if status == "SUCCEEDED"),
            "hai_spark_jobs_failed": sum(1 for status in self.outcomes.values() if status == "FAILED"),
            "hai_spark_poll_timestamp_seconds": time.time(),
        }
        return [metrics_push.Sample(name, value) for name, value in values.items()]


class Reporter(threading.Thread):
    """Poll and push every ``interval`` seconds until stopped, then push the final group."""

    def __init__(self, pusher: metrics_push.Pusher, progress: Progress, interval: float | None = None) -> None:
        super().__init__(name="spark_progress", daemon=True)
        self.pusher, self.progress = pusher, progress
        self.interval = metrics_push.HEARTBEAT_SECONDS if interval is None else interval
        self.stopped = threading.Event()
        self.finished = False
        self.read_failed = False
        self.last: list[metrics_push.Sample] = []

    def step(self) -> list[metrics_push.Sample] | None:
        """One poll and one push of the running group. A failed read is named once and pushes nothing, so no stale value looks current."""
        try:
            samples = self.progress.poll()
        except Exception as error:
            if not self.read_failed:
                metrics_push.line("spark_progress_failed", reason=f"{type(error).__name__}: {error}")
            self.read_failed = True
            return None
        self.read_failed = False
        self.last = samples
        self.pusher.push_samples("running", samples)
        return samples

    def run(self) -> None:
        while not self.stopped.is_set():
            self.step()
            self.stopped.wait(self.interval)

    def stop(self) -> None:
        """End the thread, push the totals under the final outcome and delete the running group. Safe to call again and never raises."""
        if self.finished:
            return
        self.finished = True
        ends = time.monotonic() + self.pusher.budget()
        try:
            self.stopped.set()
            if self.is_alive():
                self.join(timeout=self.pusher.budget() * JOIN_SHARE)
            try:
                samples = self.progress.poll()
            except Exception:
                samples = self.last
            if samples:
                failed = next((sample.value for sample in samples if sample.name == "hai_spark_jobs_failed"), 0)
                self.pusher.push_samples("failed" if failed else "succeeded", samples, within=ends - time.monotonic())
                self.pusher.delete_group("running", within=ends - time.monotonic())
        except Exception as error:
            metrics_push.line("spark_progress_failed", reason=f"{type(error).__name__}: {error}")


def maybe_start(spark: Any, environ: Mapping[str, str] | None = None, interval: float | None = None) -> Reporter | None:
    """Start the reporter for a bronze session when the push address is set; any other job, a missing address or a session without a tracker starts none."""
    environ = os.environ if environ is None else environ
    if environ.get(metrics_push.URL_VARIABLE, "") == "" or environ.get(metrics_push.JOB_VARIABLE, "") != JOB:
        return None
    try:
        pusher = metrics_push.Pusher(environ[metrics_push.URL_VARIABLE], JOB, PHASE)
        tracker = spark.sparkContext.statusTracker()
    except Exception as error:
        metrics_push.line("spark_progress_skipped", reason=f"{type(error).__name__}")
        return None
    reporter = Reporter(pusher, Progress(tracker), interval)
    reporter.start()
    atexit.register(reporter.stop)
    return reporter
