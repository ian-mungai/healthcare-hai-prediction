"""The launch guarantees as a mixin for the Amazon provider's ``EcsRunTaskOperator``.

``BrokerlessEcsRunTaskOperator`` in ``orchestration/ecs_launch_provider.py`` puts this mixin before the provider operator. The provider's ``execute`` keeps
its flow (reattach, start, wait, check) and the mixin replaces the four hooks it calls, written against apache-airflow-providers-amazon 9.36.0
(``operators/ecs.py`` lines 519-590 for ``execute`` and 805-815 for ``on_kill``). The provider's own hook bodies are not used: they send no client token, list
tasks with ``startedBy`` and ``desiredStatus`` together, wait up to 1,000,000 attempts, count the guard's exit code and stop a killed task
without confirming it.
This module imports nothing from Airflow, so the offline cases can run it over a stand-in.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from orchestration.ecs_launch import JobSpec, Launch, Launcher


class JobFailed(Exception):
    """The job ended without success."""


class LaunchGuarantees:
    """Place before the provider operator in the bases. ``launcher`` builds the launcher on first use, so no cloud client exists while a DAG file is parsed.

    ``stop_on_kill`` is False by default: Airflow calls ``on_kill`` when the scheduler process ends too, and a stop would end a long job that the next attempt
    can reattach to. The job's deadline and ``Launcher.cancel`` stop it otherwise.
    """

    arn: str | None  # the provider operator's fields and logger, set by its constructor and by Airflow
    log: Any

    def __init__(self, *, job: JobSpec, launcher: Callable[[], Launcher], stop_on_kill: bool = False, **kwargs: Any) -> None:
        if kwargs.get("deferrable"):
            raise ValueError("a launch operator cannot defer: the deadline, stop and records need this process")
        if kwargs.get("wait_for_completion", True) is False:
            raise ValueError("a launch operator always waits for the job to end")
        kwargs["reattach"] = True
        super().__init__(**kwargs)
        self.job = job
        self.stop_on_kill = stop_on_kill
        self._make_launcher = launcher
        self._launcher: Launcher | None = None
        self._launch: Launch | None = None

    def launcher(self) -> Launcher:
        if self._launcher is None:
            self._launcher = self._make_launcher()
        return self._launcher

    def sync(self) -> Launch:
        if self._launch is None:
            raise RuntimeError("the launch hooks ran out of order")
        self.arn = self._launch.arn  # the provider's field: xcom, on_kill and the log fetcher read it
        return self._launch

    def _try_reattach_task(self, started_by: str) -> None:
        self._launch = self.launcher().attach(self.job)
        self.sync()

    def _start_task(self) -> None:
        launch = self.sync()
        self.launcher().start(launch)
        self.sync()

    def _wait_for_task_ended(self) -> None:
        self.launcher().wait(self.sync())

    def _check_success_task(self) -> None:
        outcome = self.launcher().conclude(self.sync())
        if outcome.logs:
            self.log.info("last log lines of the failed job:\n%s", outcome.logs)
        if not outcome.ok:
            earlier = " in an earlier attempt, nothing was launched, start a new run to try again" if outcome.adopted else ""
            raise JobFailed(f"{self.job.job} attempt {self.job.attempt} ended {outcome.outcome} (exit {outcome.exit_code}){earlier}: {outcome.reason}")

    def on_kill(self) -> None:
        if not self.stop_on_kill:
            self.log.warning("kill requested: the task keeps running so that the next attempt can reattach; it ends at its deadline")
            return
        if self._launch is not None and self._launch.outcome is None:
            self.launcher().cancel(self.job.run_id, self.job.job, self.job.attempt, "killed")
