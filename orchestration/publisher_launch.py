"""Build the direct ECS launch request of the Run 3 publisher job from the run's staged ``publisher_request.json``.

The owner's Mac stages ``publisher_inputs.tar.gz`` and ``publisher_request.json`` under ``lakehouse/run_stage/<run>/inputs/``
(``scripts.acquisition.stage_publisher``). ``read_request`` reads and checks the request and returns the values of the publisher's ``run`` container.
``publisher_spec`` turns them into the ``JobSpec`` that ``orchestration.ecs_launch.Launcher`` runs on the host: every attempt of a run is bound to the digest of
the whole request file and to the run's code digest, so a request changed after the first launch needs a new run. A request that is not staged yet is retryable
(``LaunchUnavailable``), a request that fails a check is refused (``LaunchRefused``) before any launch record or cloud call. The checks are those of the route's
broker request reader, unchanged; this module replaces that reader.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any, Protocol

from botocore.exceptions import BotoCoreError, ClientError

from orchestration.ecs_launch import JobSpec, LaunchRefused, LaunchUnavailable, Resources

JOB = "publisher"
# The job's own deadline: one day, the publisher downloads every Run 3 source once.
DEADLINE_SECONDS = 86_400
FETCH_CONTAINER = "fetch"
RUN_CONTAINER = "run"
REQUEST_BYTES = 4096
REQUEST_KEYS = frozenset({"inputs_sha256", "queue_sha256", "resume_sources", "prior_state"})
PRIOR_KEYS = frozenset({"key", "version_id", "sha256"})
DIGEST = re.compile(r"[a-f0-9]{64}")
SOURCE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
CODE_RUN = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")


class S3Reader(Protocol):
    """The one S3 call the reader makes."""

    def get_object(self, **arguments: Any) -> dict[str, Any]: ...


def request_key(run_id: str) -> str:
    """Where the owner stages the run's publisher request."""
    return f"lakehouse/run_stage/{run_id}/inputs/publisher_request.json"


def read_request(s3: S3Reader, bucket: str, run_id: str) -> dict[str, str]:
    """Read and check the staged request and return the ``run`` container's publisher variables, with ``PUBLISHER_REQUEST_SHA256`` the digest of the file.

    The file holds exactly ``inputs_sha256`` (the digest of the staged inputs archive), ``queue_sha256`` (the queue whose state the task restores),
    ``resume_sources`` (the paused sources the owner resumes) and ``prior_state`` (the key, version and digest of the state the owner approved, or null).
    """
    try:
        body = s3.get_object(Bucket=bucket, Key=request_key(run_id))["Body"].read(REQUEST_BYTES + 1)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") == "NoSuchKey":
            raise LaunchUnavailable("publisher_request_not_staged") from None
        raise LaunchUnavailable("publisher_request_unreadable") from None
    except BotoCoreError:
        raise LaunchUnavailable("publisher_request_unreadable") from None
    if len(body) > REQUEST_BYTES:
        raise LaunchRefused("publisher_request_too_large")
    try:
        document = json.loads(body)
    except ValueError:
        raise LaunchRefused("publisher_request_malformed") from None
    if not isinstance(document, dict) or set(document) != REQUEST_KEYS:
        raise LaunchRefused("publisher_request_needs_exactly_inputs_queue_resume_sources_and_prior_state")
    inputs, queue, sources, prior = document["inputs_sha256"], document["queue_sha256"], document["resume_sources"], document["prior_state"]
    if prior is not None and (
        not isinstance(prior, dict)
        or set(prior) != PRIOR_KEYS
        or not all(isinstance(value, str) and value for value in prior.values())
        or not DIGEST.fullmatch(prior["sha256"])
        or not prior["key"].startswith("raw/run3_state/")
    ):
        raise LaunchRefused("publisher_request_has_wrong_prior_state")
    if (
        not isinstance(inputs, str)
        or not DIGEST.fullmatch(inputs)
        or not isinstance(queue, str)
        or not DIGEST.fullmatch(queue)
        or not isinstance(sources, list)
        or not all(isinstance(name, str) and SOURCE.fullmatch(name) for name in sources)
    ):
        raise LaunchRefused("publisher_request_has_wrong_values")
    return {
        "PUBLISHER_INPUTS_SHA256": inputs,
        "PUBLISHER_QUEUE_SHA256": queue,
        "PUBLISHER_RESUME_SOURCES": ",".join(sources),
        "PUBLISHER_PRIOR_STATE": "" if prior is None else json.dumps(prior, sort_keys=True, separators=(",", ":")),
        "PUBLISHER_REQUEST_SHA256": hashlib.sha256(body).hexdigest(),
    }


def publisher_spec(
    s3: S3Reader,
    bucket: str,
    run_id: str,
    attempt: int,
    code: Mapping[str, str],
    need: Resources,
    now: float,
) -> JobSpec:
    """The launch request of one publisher attempt. ``code`` holds the run's code archive ``run_id`` and ``sha256``. ``need`` is the host room the job
    takes; it comes from the publisher's host task definition. ``now`` (epoch seconds) dates ``HAI_DEADLINE_UTC``, which only a new task reads: a reattached
    attempt keeps the recorded deadline of the launch it joins."""
    code_run, code_digest = code.get("run_id", ""), code.get("sha256", "")
    if not CODE_RUN.fullmatch(code_run) or not DIGEST.fullmatch(code_digest):
        raise LaunchRefused("publisher_code_has_wrong_values")
    values = read_request(s3, bucket, run_id)
    deadline = str(int(now) + DEADLINE_SECONDS)
    return JobSpec(
        run_id=run_id,
        job=JOB,
        attempt=attempt,
        environment={
            FETCH_CONTAINER: {"RUN_ID": run_id, "CODE_RUN_ID": code_run, "CODE_SHA256": code_digest, "HAI_DEADLINE_UTC": deadline},
            RUN_CONTAINER: {"RUN_ID": run_id, **values, "HAI_DEADLINE_UTC": deadline},
        },
        # The publisher reads no earlier run's output, so its input run is its own run; the request digest binds what it reads.
        bound={"code_digest": code_digest, "request_hash": values["PUBLISHER_REQUEST_SHA256"], "input_run": run_id},
        deadline_seconds=DEADLINE_SECONDS,
        need=need,
        acceptance=(RUN_CONTAINER,),
        log_container=RUN_CONTAINER,
        log_prefix=RUN_CONTAINER,
    )
