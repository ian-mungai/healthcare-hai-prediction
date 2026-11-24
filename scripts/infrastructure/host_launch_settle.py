"""The host session's settle of launches (owner decision A, Nov 24 2026; ``orchestration/ecs_launch.py`` rule 11).

When the host is shown stopped, every claimed launch of the job families with no end record is ended as ``cancelled``: the family gate alone keeps such a
launch unresolved for good when ECS no longer describes its saved task. ``HostLaunchSettle.run`` reads the proof (host group at desired capacity 0 with no
instance, then no ACTIVE container instance in the cluster, then no task of a job host definition that ECS does not show STOPPED) and gives it to
``HostStopSettler``, which also needs the claim to be older than the RunTask send window plus a margin. Each launch is reported as a step line by run, job and
attempt, with the last 8 characters of a saved task ID, never an ARN. A failure is reported and never raised: the host-off stays complete and reported, and the
next stop or start repeats the settle.

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
from scripts.infrastructure.host_session import ARN_TEXT, AwsCallFailed, HostSessionError, Session
from scripts.infrastructure.render_project_config import REPO_ROOT

UNCONFIRMED_CODES = frozenset({"request_unconfirmed", "invalid_response"})
TASK_ID_LENGTH = 8
SHOWN_LIMIT = 300


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
            if "IfMatch" in arguments:
                options += ["--if-match", arguments["IfMatch"]]
            return self.call("put-object", options)

    def list_objects_v2(self, **arguments: Any) -> dict[str, Any]:
        options = ["--prefix", arguments["Prefix"], "--no-paginate"]
        if "ContinuationToken" in arguments:
            options += ["--continuation-token", arguments["ContinuationToken"]]
        return self.call("list-objects-v2", options)


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
        arns = session.call("ecs", "list-container-instances", ["--cluster", session.names.cluster, "--status", "ACTIVE"]).get("containerInstanceArns", [])
        instances_read_at = session.clock.wall()
        busy = self.busy_jobs(jobs)
        return HostStopProof(0, 0, group_read_at, len(arns), instances_read_at, session.clock.wall(), busy)

    def busy_jobs(self, jobs: Sequence[str]) -> frozenset[str]:
        """The jobs with a task that ECS does not show STOPPED: every job when a task is not the host definition of one job (it cannot be tied to a family)."""
        session = self.session
        if session.job_prefix is None:
            raise HostSessionError("the session has no job task family prefix")
        definitions = {f"{session.job_prefix}{job}{HOST_SUFFIX}": job for job in jobs}
        busy: set[str] = set()
        for task in session.running_job_tasks():
            if task.mismatch or task.family not in definitions:
                return frozenset(jobs)
            busy.add(definitions[task.family])
        return frozenset(busy)

    def run(self, point: str) -> None:
        """Settle at ``point`` (``stop`` after a verified host-off, ``start`` before the capacity is raised). Reports every result, and any failure, as a step;
        never raises, so the host-off or the start goes on."""
        try:
            self.settle(point)
        except (HostSessionError, LaunchError, ClientError, EndpointConnectionError, OSError, KeyError, ValueError) as error:
            self.session.step("launch_settle_failed", f"{self.shown(error)}; the next stop or start repeats the settle")

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
        settler = HostStopSettler(store, Clock(wall=session.clock.wall), jobs)
        settled = unresolved = failed = 0
        for job in jobs:
            try:
                for result in settler.settle_family(job, proof):
                    settled, unresolved = settled + (result.status == "settled"), unresolved + (result.status == "unresolved")
                    self.report(result)
            except (LaunchError, ClientError, EndpointConnectionError, HostSessionError) as error:
                failed += 1
                session.step("launch_settle_failed", f"{job}: {self.shown(error)}; the next stop or start repeats the settle")
        session.step("launch_settle", f"{settled} launch(es) cancelled, {unresolved} still unresolved, {failed} job family(ies) not read")

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
