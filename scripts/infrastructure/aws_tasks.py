"""List and stop the project's job tasks before the host is stopped: ``list`` and ``stop``.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.aws_tasks list``. The profile and region come from ``.env`` through
``load_configuration``, the cluster from the platform Terraform output. Only tasks of the families ``<project>_<environment>_job_*`` are listed or stopped;
service tasks, one-shot tasks and tasks of other projects are never touched. The family comes from the task definition, and a task whose ECS group and task
definition disagree is refused. A writer task (family ``..._job_write_*``) is stopped only when the owner names its ID suffix with
``--confirm-writer-stop``. A task that is already stopping (desired status STOPPED, not yet STOPPED) counts as running. Every call and wait ends at one
deadline and fails visibly. Output shows only the family and the last 8 characters of the task ID, never an ARN, an account ID or a provider reason.
"""

from __future__ import annotations

import argparse
import contextlib
import re
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from scripts.acquisition.s3_store import AwsCli
from scripts.infrastructure.host_session import Clock, HostSessionError, Session, TerminationRequested, install_termination_handler, load_names
from scripts.infrastructure.render_project_config import REPO_ROOT, ConfigurationError, load_configuration

CLEANUP_SECONDS = 120
STOP_GRACE_SECONDS = 90
STOP_POLL_SECONDS = 5
DESCRIBE_LIMIT = 100
MAX_PAGES = 50
SUFFIX_LENGTH = 8
GROUP_PREFIX = "family:"
WRITER_MARK = "write_"
STOP_REASON = "aws_tasks stop"
WRITER_STOP_REASON = "aws_tasks stop: owner confirmed writer stop"
MISSING = "MISSING"
STOPPED = "STOPPED"
DESIRED_STATUSES = ("RUNNING", "STOPPED")
SUFFIX_FORM = re.compile(r"[0-9a-f]{8}")
FAMILY_FORM = re.compile(r"[A-Za-z0-9_-]{1,255}")
UNNAMED = "unnamed"


@dataclass(frozen=True)
class JobTask:
    """One running job task: its ARN stays inside the process, the output shows the family and the ID suffix."""

    arn: str
    family: str
    writer: bool
    mismatch: bool = False

    @property
    def suffix(self) -> str:
        return self.arn.rsplit("/", 1)[-1][-SUFFIX_LENGTH:]


class TaskCleanup:
    """The job tasks of one project and environment in the project cluster."""

    def __init__(self, session: Session, prefix: str, started: float | None = None) -> None:
        """``started`` is the clock time at which the command began, before its setup lookups, and the cleanup deadline counts from it.
        When absent, the deadline counts from the start of each cleanup command."""
        self.session, self.prefix, self.started = session, prefix, started

    @contextlib.contextmanager
    def bounded(self) -> Iterator[None]:
        """Run one command under the cleanup deadline, which counts from the start of the command and includes its setup lookups.
        Each call gets the time that remains and no call starts after it."""
        outer = self.session.deadline
        begin = self.session.clock.now() if self.started is None else self.started
        self.session.deadline = begin + CLEANUP_SECONDS
        try:
            yield
        finally:
            self.session.deadline = outer

    def grace(self) -> int:
        """The seconds to wait for STOPPED: the stop grace, cut to the time left under the cleanup deadline so that no wait outlives it."""
        deadline = self.session.deadline
        left = STOP_GRACE_SECONDS if deadline is None else min(STOP_GRACE_SECONDS, int(deadline - self.session.clock.now()))
        return max(left, 1)

    def page_arns(self) -> list[str]:
        """Every task ARN of the cluster with either desired status, each without duplicates: the pages are followed by their tokens, a repeated token or
        too many pages fails. A task that was told to stop but still runs only shows under desired status STOPPED."""
        arns: dict[str, None] = {}
        for desired in DESIRED_STATUSES:
            arns |= dict.fromkeys(self.page_status(desired))
        return list(arns)

    def page_status(self, desired: str) -> list[str]:
        """The task ARNs with one desired status."""
        arns: list[str] = []
        seen: set[str] = set()
        token: str | None = None
        for _ in range(MAX_PAGES):
            arguments = ["--cluster", self.session.names.cluster, "--desired-status", desired, "--no-paginate"]
            page = self.session.call("ecs", "list-tasks", [*arguments, "--next-token", token] if token else arguments)
            arns += page.get("taskArns", [])
            token = page.get("nextToken")
            if not token:
                return arns
            if token in seen:
                raise HostSessionError("list-tasks repeated a page token")
            seen.add(token)
        raise HostSessionError(f"list-tasks has more than {MAX_PAGES} pages")

    def describe(self, arns: list[str]) -> list[dict[str, Any]]:
        """The described tasks, in chunks; a task that is already gone (MISSING) is dropped and any other failure raises."""
        found: list[dict[str, Any]] = []
        for start in range(0, len(arns), DESCRIBE_LIMIT):
            described = self.session.call("ecs", "describe-tasks", ["--cluster", self.session.names.cluster, "--tasks", *arns[start : start + DESCRIBE_LIMIT]])
            if any(failure.get("reason") != MISSING for failure in described.get("failures", [])):
                raise HostSessionError("describe-tasks reported failures")
            found += described.get("tasks", [])
        return found

    def discover(self) -> list[JobTask]:
        """The tasks of the project's job families that are not STOPPED. The family comes from the task definition; a task whose group names another
        family, or whose definition is absent, is marked as a mismatch and is never stopped."""
        tasks: list[JobTask] = []
        for item in self.describe(self.page_arns()):
            group, definition = item.get("group"), item.get("taskDefinitionArn")
            grouped = group.removeprefix(GROUP_PREFIX) if isinstance(group, str) and group.startswith(GROUP_PREFIX) else None
            defined = definition.rsplit("/", 1)[-1].rsplit(":", 1)[0] if isinstance(definition, str) and "/" in definition else None
            ours = [family for family in (grouped, defined) if family is not None and family.startswith(self.prefix)]
            if not ours or item.get("lastStatus") == STOPPED:
                continue
            family = ours[-1]
            shown = family if FAMILY_FORM.fullmatch(family) else UNNAMED
            tasks.append(JobTask(str(item["taskArn"]), shown, family.removeprefix(self.prefix).startswith(WRITER_MARK), grouped != defined))
        return tasks

    def not_stopped(self, tasks: list[JobTask]) -> list[JobTask]:
        """The tasks that ECS does not describe as STOPPED; a task it no longer finds is not confirmed."""
        states = {str(item.get("taskArn")): item.get("lastStatus") for item in self.describe([task.arn for task in tasks])}
        return [task for task in tasks if states.get(task.arn) != STOPPED]

    def list_tasks(self) -> list[JobTask]:
        """Report every running job task, read only."""
        with self.bounded():
            tasks = self.discover()
        for task in tasks:
            self.session.step(
                "task", f"{task.family} {task.suffix}" + (" (writer)" if task.writer else "") + (" (group and task definition differ)" if task.mismatch else "")
            )
        self.session.step("tasks", f"{len(tasks)} job task(s) running" if tasks else "no job task is running")
        return tasks

    def stop(self, confirmed: frozenset[str]) -> None:
        """Stop every running job task and wait for STOPPED; a writer needs its suffix in ``confirmed``. Any task left running fails the command."""
        problems: list[str] = []
        with self.bounded():
            tasks = self.discover()
            if not tasks:
                self.session.step("tasks", "no job task is running")
            sent: list[JobTask] = []
            failed: dict[JobTask, str] = {}
            for task in tasks:
                if task.mismatch:
                    problems.append(f"{task.family} {task.suffix} not stopped: its group and its task definition differ")
                    continue
                if task.writer and task.suffix not in confirmed:
                    problems.append(
                        f"writer {task.family} {task.suffix} not stopped: its intent may be open; to stop it add --confirm-writer-stop {task.suffix}"
                    )
                    continue
                reason = WRITER_STOP_REASON if task.writer else STOP_REASON
                try:
                    self.session.call("ecs", "stop-task", ["--cluster", self.session.names.cluster, "--task", task.arn, "--reason", reason])
                except HostSessionError as error:
                    failed[task] = str(error)
                sent.append(task)
            problems += [
                f"confirmation {value} matches no running writer"
                for value in sorted(confirmed - {task.suffix for task in tasks if task.writer and not task.mismatch})
            ]
            if sent:
                try:
                    self.session.wait("the job tasks to stop", lambda: not self.not_stopped(sent), self.grace(), STOP_POLL_SECONDS)
                    left: list[JobTask] = []
                except HostSessionError as error:
                    problems.append(str(error))
                    left = sent
                    with contextlib.suppress(HostSessionError):
                        left = self.not_stopped(sent)
                problems += [
                    f"{task.family} {task.suffix} " + (f"stop call failed ({failed[task]}) and " if task in failed else "") + "not STOPPED" for task in left
                ]
                for task in sent:
                    if task not in left:
                        self.session.step("stopped", f"{task.family} {task.suffix}")
                        if task.writer:
                            self.session.step(
                                "writer_stopped",
                                f"{task.family} {task.suffix}; the layer stays in recovery hold until complete runs from the terminal observation",
                            )
        if problems:
            raise HostSessionError("stop incomplete: " + "; ".join(problems))


def suffix_argument(value: str) -> str:
    """A task ID suffix: 8 lowercase hexadecimal characters."""
    if not SUFFIX_FORM.fullmatch(value):
        raise argparse.ArgumentTypeError("a task ID suffix is 8 lowercase hexadecimal characters")
    return value


def build_parser() -> argparse.ArgumentParser:
    """The command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="show the running job tasks, read only")
    stop = commands.add_parser("stop", help="stop the running job tasks and wait until they are STOPPED")
    stop.add_argument(
        "--confirm-writer-stop",
        action="append",
        default=[],
        type=suffix_argument,
        metavar="SUFFIX",
        help="owner confirmation to stop the writer task with this ID suffix (repeatable)",
    )
    return parser


def main(argv: list[str] | None = None, clock: Clock | None = None) -> int:
    """Run one subcommand and return 0 on success, 1 on a failed, refused, interrupted or terminated step, 2 on a configuration error.

    The 120-second deadline starts here, before the setup lookups. A setup that used all of it leaves no time for a task call, so none is sent.
    """
    arguments = build_parser().parse_args(argv)
    clock = clock or Clock()
    started = clock.now()
    try:
        settings, _ = load_configuration(REPO_ROOT / ".env")
        session = Session(AwsCli(settings), load_names(settings), clock)
        cleanup = TaskCleanup(session, f"{settings['project_name']}_{settings['environment']}_job_", started)
    except ConfigurationError as error:
        sys.stderr.write(f"configuration: {error}\n")
        return 2
    restore = install_termination_handler()
    try:
        if arguments.command == "list":
            cleanup.list_tasks()
        else:
            cleanup.stop(frozenset(arguments.confirm_writer_stop))
    except (HostSessionError, TerminationRequested, KeyboardInterrupt) as error:
        sys.stderr.write(f"{arguments.command} failed: {type(error).__name__} {error}\n")
        return 1
    finally:
        restore()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
