"""The host session's settle of launches (owner decision A, Nov 24 2026; ``orchestration/ecs_launch.py`` rule 11).

When the host is shown stopped, every claimed launch of the job families with no end record is ended as ``cancelled``: the family gate alone keeps such a
launch unresolved for good when ECS no longer describes its saved task. ``HostLaunchSettle.run`` reads the proof (host group at desired capacity 0 with no
instance, then no ACTIVE container instance in the cluster, then no task of a job host definition that ECS does not show STOPPED) and gives it to
``HostStopSettler``, which also needs the claim to be older than the RunTask send window plus a margin. Each launch is reported as a step line by run, job and
attempt, with the last 8 characters of a saved task ID, never an ARN. The proof is read again immediately before each end write, a task that ECS lists and
does not describe completely holds every family, and the whole settle has one deadline (``SETTLE_SECONDS``). A failure is reported and never raised: the
host-off stays complete and reported, and the next stop or start repeats the settle.

The jobs are those of the job catalog (``load_tables``), the set the family gate and ``Launcher.validate`` use, so every family whose gate can be blocked is
visited, with one list of its family index. The launch records are read and written through the session's AWS CLI (``CliS3Client``), the same way the session
keeps its code run record: no new process kind. The Mac user's S3 policy already allows this: ``ManageLakehouseTables`` (get and put under ``lakehouse/``) and
``InspectApprovedAcquisitionBucket`` (``ListBucket``, so an absent record answers NoSuchKey) in ``infra/iam/policies/healthcare_hai_prediction_s3_policy.json``.
"""

from __future__ import annotations

import io
import re
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError, EndpointConnectionError

from orchestration.ecs_adapter import HOST_SUFFIX
from orchestration.ecs_launch import Clock, HostStopProof, HostStopSettler, LaunchError, S3LaunchStore, SettleResult, launch_record_prefix
from scripts.infrastructure.host_session import ARN_TEXT, AwsCallFailed, HostSessionError, Session, TerminationRequested
from scripts.infrastructure.render_project_config import REPO_ROOT

UNCONFIRMED_CODES = frozenset({"request_unconfirmed", "invalid_response"})
TASK_ID_LENGTH = 8
SHOWN_LIMIT = 300
SETTLE_SECONDS = 900  # one deadline for the whole settle: every proof read, record read and write counts against it
TASK_COMPLETE = ("lastStatus", "taskDefinitionArn")


class CliS3Client:
    """The three S3 calls of ``S3LaunchStore`` (``get_object``, ``put_object``, ``list_objects_v2``) over the session's AWS CLI calls on its data bucket. An
    S3 answer with an error code becomes a ``ClientError`` with that code; a call with no confirmed answer becomes a ``BotoCoreError``, as the store expects."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def call(self, operation: str, arguments: list[str]) -> dict[str, Any]:
        try:
            return self.session.call("s3api", operation, arguments)
        except AwsCallFailed as error:
            if error.code in UNCONFIRMED_CODES:
                raise EndpointConnectionError(endpoint_url="aws-cli") from None
            raise ClientError({"Error": {"Code": error.code}}, operation) from None

    def get_object(self, **arguments: Any) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix="launch_record_") as folder:
            target = Path(folder) / "record"
            reply = self.call("get-object", ["--key", arguments["Key"], str(target)])
            return {"Body": io.BytesIO(target.read_bytes()), "ETag": reply.get("ETag", "")}

    def put_object(self, **arguments: Any) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix="launch_record_") as folder:
            body = Path(folder) / "record"
            body.write_bytes(arguments["Body"])
            options = ["--key", arguments["Key"], "--body", str(body), "--content-type", "application/json"]
            if "IfNoneMatch" in arguments:
                options += ["--if-none-match", arguments["IfNoneMatch"]]
            return self.call("put-object", options)

    def list_objects_v2(self, **arguments: Any) -> dict[str, Any]:
        options = ["--prefix", arguments["Prefix"], "--no-paginate"]
        if "ContinuationToken" in arguments:
            options += ["--continuation-token", arguments["ContinuationToken"]]
        return self.call("list-objects-v2", options)


def chained_signal(error: BaseException) -> BaseException | None:
    """The SIGINT or SIGTERM that an error is chained from, through its causes and contexts, or None. An error that carries a signal (the AWS runner raises
    ``LaunchesNotStopped`` from an interrupt) is that signal: the settle never hides it."""
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        if isinstance(current, KeyboardInterrupt | TerminationRequested):
            return current
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return None


def jobs_of_catalog() -> list[str]:
    """The jobs of the job catalog: the families the launch gate knows."""
    from orchestration.ecs_preflight import load_tables

    return sorted(load_tables(REPO_ROOT)[0]["jobs"])


class HostLaunchSettle:
    """Reads the proof that the host is stopped and settles the launches of every job family on it, reporting through the session's steps."""

    def __init__(self, session: Session, jobs: Callable[[], Sequence[str]] = jobs_of_catalog) -> None:
        self.session = session
        self.jobs = jobs

    def read_proof(self, jobs: Sequence[str]) -> HostStopProof | None:
        """The proof, or None when the host group shows capacity (the host is not stopped). A read that fails raises ``HostSessionError``: no proof is built."""
        session = self.session
        group = session.group_state()
        group_read_at = session.clock.wall()
        desired, instances = group.get("DesiredCapacity"), group.get("Instances") or []
        if not isinstance(desired, int) or isinstance(desired, bool):
            raise HostSessionError("the host group does not show its desired capacity")
        if desired != 0 or instances:
            return None
        listed = session.call("ecs", "list-container-instances", ["--cluster", session.names.cluster, "--status", "ACTIVE"]).get("containerInstanceArns")
        if not isinstance(listed, list):
            raise HostSessionError("ECS answered the container instance listing without a list of instances")
        arns = listed
        instances_read_at = session.clock.wall()
        busy = self.busy_jobs(jobs)
        return HostStopProof(0, 0, group_read_at, len(arns), instances_read_at, session.clock.wall(), busy)

    def busy_jobs(self, jobs: Sequence[str]) -> frozenset[str]:
        """The jobs with a task that ECS does not show STOPPED. The discovery is strict: a task that ECS lists (with either desired status) and does not
        describe completely, such as a PROVISIONING task that DescribeTasks reports MISSING, has no known family, so every job is busy. This differs from
        the cleanup helper of ``aws_tasks``, which drops a MISSING task because its callers stop tasks and a task that is gone needs no stop. A task of
        another family (a service task) does not hold a job."""
        from scripts.infrastructure.aws_tasks import DESCRIBE_LIMIT, GROUP_PREFIX

        session = self.session
        if session.job_prefix is None:
            raise HostSessionError("the session has no job task family prefix")
        definitions = {f"{session.job_prefix}{job}{HOST_SUFFIX}": job for job in jobs}
        arns = self.listed_tasks()
        busy: set[str] = set()
        for start in range(0, len(arns), DESCRIBE_LIMIT):
            chunk = arns[start : start + DESCRIBE_LIMIT]
            described = session.call("ecs", "describe-tasks", ["--cluster", session.names.cluster, "--tasks", *chunk]).get("tasks")
            if not isinstance(described, list):
                raise HostSessionError("ECS answered the task description without a list of tasks")
            tasks = {str(item.get("taskArn")): item for item in described if isinstance(item, dict)}
            for arn in chunk:
                task = tasks.get(arn)
                if task is None or not all(isinstance(task.get(name), str) and task[name] for name in TASK_COMPLETE) or "/" not in task["taskDefinitionArn"]:
                    return frozenset(jobs)
                if task["lastStatus"] == "STOPPED":
                    continue
                family = task["taskDefinitionArn"].rsplit("/", 1)[-1].rsplit(":", 1)[0]
                group = task.get("group")
                grouped = group.removeprefix(GROUP_PREFIX) if isinstance(group, str) and group.startswith(GROUP_PREFIX) else family
                if not family:
                    return frozenset(jobs)
                if not (family.startswith(session.job_prefix) or grouped.startswith(session.job_prefix)):
                    continue
                if family != grouped or family not in definitions:
                    return frozenset(jobs)
                busy.add(definitions[family])
        return frozenset(busy)

    def listed_tasks(self) -> list[str]:
        """Every task ARN of the cluster with either desired status. A reply that is not a list of ARNs, a repeated page token and too many pages each raise:
        a listing that is not read to its end is no evidence that no task runs."""
        from scripts.infrastructure.aws_tasks import DESIRED_STATUSES, MAX_PAGES

        session = self.session
        arns: dict[str, None] = {}
        for desired in DESIRED_STATUSES:
            token: str | None = None
            seen: set[str] = set()
            for _ in range(MAX_PAGES):
                options = ["--cluster", session.names.cluster, "--desired-status", desired, "--no-paginate"]
                page = session.call("ecs", "list-tasks", [*options, "--next-token", token] if token else options)
                listed = page.get("taskArns")
                if not isinstance(listed, list) or not all(isinstance(arn, str) and arn for arn in listed):
                    raise HostSessionError("ECS answered the task listing without a list of tasks")
                arns |= dict.fromkeys(listed)
                token = page.get("nextToken")
                if not token:
                    break
                if token in seen:
                    raise HostSessionError("ECS repeated a page token of the task listing")
                seen.add(str(token))
            else:
                raise HostSessionError(f"the task listing has more than {MAX_PAGES} pages")
        return list(arns)

    def run(self, point: str) -> None:
        """Settle at ``point`` (``stop`` after a verified host-off, ``start`` before the capacity is raised). Reports every result, and any failure, as a step,
        and raises nothing but a signal: the settle is optional, so the host-off or the start goes on whatever the AWS runner or the stores raise. The whole
        settle has one deadline (``SETTLE_SECONDS``) that every AWS call counts against."""
        session = self.session
        outer, begin = session.deadline, session.clock.now()
        session.deadline = begin + SETTLE_SECONDS if outer is None else min(begin + SETTLE_SECONDS, outer)
        try:
            self.settle(point)
        except (KeyboardInterrupt, TerminationRequested):
            raise
        except Exception as error:
            signal_error = chained_signal(error)
            if signal_error is not None:
                raise signal_error from None
            session.step("launch_settle_failed", f"{type(error).__name__}: {self.shown(error)}; the next stop or start repeats the settle")
        finally:
            session.deadline = outer

    def settle(self, point: str) -> None:
        session = self.session
        bucket = session.names.code_record_bucket
        if not bucket:
            session.step("launch_settle_skipped", "the session names no data bucket, so the launch records cannot be read")
            return
        jobs = list(self.jobs())
        proof = self.read_proof(jobs)
        if proof is None:
            if point != "start":
                session.step("launch_settle_skipped", "the host group shows capacity, so the host is not stopped")
            return
        store = S3LaunchStore(CliS3Client(session), bucket, launch_record_prefix())
        settler = HostStopSettler(store, Clock(wall=session.clock.wall), jobs, lambda: self.read_proof(jobs))
        settled = unresolved = failed = 0
        for job in jobs:
            try:
                for result in settler.settle_family(job, proof):
                    settled, unresolved = settled + (result.status == "settled"), unresolved + (result.status == "unresolved")
                    self.report(result)
            except (LaunchError, ClientError, EndpointConnectionError, HostSessionError) as error:
                failed += 1
                session.step("launch_settle_failed", f"{job}: {self.shown(error)}; the next stop or start repeats the settle")
        done = f"{settled} launch(es) cancelled, {unresolved} still unresolved"
        if failed:
            session.step("launch_settle_incomplete", f"{done}; {failed} job family(ies) not read to the end, so a launch may still be unresolved")
        else:
            session.step("launch_settle", done)

    def report(self, result: SettleResult) -> None:
        who = f"{result.run_id} {result.job} attempt {result.attempt}"
        task = f", saved task {result.saved_task[-TASK_ID_LENGTH:]}" if result.saved_task else ""
        name = {"settled": "launch_settled", "unresolved": "launch_unresolved"}.get(result.status, "launch_ended")
        detail = "cancelled: the host is stopped" if result.status == "settled" else self.shown(result.reason)
        self.session.step(name, f"{who}{task}: {detail}")

    @staticmethod
    def shown(text: object) -> str:
        """A failure text for the report: no ARN, and a limit on its length."""
        return ARN_TEXT.sub("<arn>", re.sub(r"\s+", " ", str(text)))[:SHOWN_LIMIT]
