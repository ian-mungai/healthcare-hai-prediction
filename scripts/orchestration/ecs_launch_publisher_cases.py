"""The Run 3 publisher job on the direct ECS launch: the request staged by the owner becomes a host launch, against the fake ECS API and in-memory buckets.

Every case drives the real ``orchestration.publisher_launch`` reader and the real ``Launcher`` and ``S3LaunchStore``. The staged request lives in its own
in-memory bucket that answers ``NoSuchKey`` like S3. Each case builds its own rig, so cases run in any order.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import replace
from typing import Any

from botocore.exceptions import EndpointConnectionError

from orchestration.ecs_adapter import EcsAdapter, EcsSettings
from orchestration.ecs_launch import EcsFitProbe, JobSpec, Launcher, LaunchRefused, LaunchUnavailable, Resources, S3LaunchStore, launch_record_prefix
from orchestration.publisher_launch import DEADLINE_SECONDS, publisher_spec, request_key
from scripts.orchestration.ecs_broker_fake import ACCOUNT, CLUSTER, FakeLogs, client_error
from scripts.orchestration.ecs_launch_fake import FakeClock, FakeLaunchBucket, RevisionEcs

RUN = "20261110T100000Z-abcdef01"
CODE = {"run_id": "20261110T090000Z-12345678", "sha256": "c" * 64}
BUCKET = "example-private-bucket"
PREFIX = launch_record_prefix()
HOST_PROVIDER = "stand-in_host"
SETTINGS = EcsSettings(
    cluster=CLUSTER,
    family_prefix="example_dev_job_",
    subnets=(),
    networks={},
    project_tag="example",
    log_group="example_jobs_dev",
    families=frozenset({"publisher"}),
    host_capacity_provider=HOST_PROVIDER,
)
# The revision of the publisher host definition that the preflight checked (registered as revision 1 by every rig).
REVISION = f"arn:aws:ecs:stand-in-region:{ACCOUNT}:task-definition/example_dev_job_publisher_host:1"
NEED = Resources(cpu_units=2048, memory_mib=8192, scratch_bytes=20 * 2**30)
REQUEST = {"inputs_sha256": "a" * 64, "queue_sha256": "b" * 64, "resume_sources": ["bls_api", "hud_api"], "prior_state": None}
PRIOR = {"key": "raw/run3_state/" + "b" * 64 + "/state.json", "version_id": "v1", "sha256": "d" * 64}


class StageBucket:
    """The run stage prefix of the data bucket: ``get_object`` returns the staged body or raises ``NoSuchKey``; ``fault`` makes the next read fail."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.fault: Exception | None = None
        self.reads = 0

    def stage(self, body: bytes) -> str:
        self.objects[request_key(RUN)] = body
        return hashlib.sha256(body).hexdigest()

    def get_object(self, **arguments: Any) -> dict[str, Any]:
        self.reads += 1
        if self.fault is not None:
            fault, self.fault = self.fault, None
            raise fault
        if arguments.get("Bucket") != BUCKET or arguments.get("Key") not in self.objects:
            raise client_error("NoSuchKey", "GetObject")
        body = self.objects[arguments["Key"]]

        class Body:
            def read(self, limit: int = -1) -> bytes:
                return body if limit < 0 else body[:limit]

        return {"Body": Body()}


class Rig:
    """One fake cloud, launch bucket, stage bucket and clock."""

    def __init__(self) -> None:
        self.ecs, self.logs = RevisionEcs(), FakeLogs()
        self.ecs.register("example_dev_job_publisher_host", NEED.memory_mib, None)
        self.records = FakeLaunchBucket(BUCKET, f"{PREFIX}/")
        self.stage = StageBucket()
        self.time = FakeClock()
        self.ecs.clock = lambda: self.time.now
        self.adapter = EcsAdapter(self.ecs, self.logs, SETTINGS)

    def launcher(self) -> Launcher:
        probe = EcsFitProbe(self.ecs, CLUSTER, lambda: 2 * NEED.scratch_bytes)
        return Launcher(self.adapter, S3LaunchStore(self.records, BUCKET, PREFIX), probe, self.time.clock())

    def spec(self, attempt: int = 1) -> JobSpec:
        return replace(publisher_spec(self.stage, BUCKET, RUN, attempt, CODE, NEED, self.time.now), task_definition=REVISION)

    def record(self, attempt: int, event: str) -> dict[str, Any]:
        return dict(json.loads(self.records.objects[f"{PREFIX}/{RUN}/publisher/{attempt:02d}/{event}.json"]))


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def refuses(error: type[BaseException], action: Callable[[], Any]) -> BaseException:
    try:
        action()
    except error as caught:
        return caught
    raise AssertionError(f"expected {error.__name__}")


def encoded(document: object) -> bytes:
    return json.dumps(document, sort_keys=True).encode()


def publisher_runs_on_the_host_bound_to_its_request() -> None:
    rig = Rig()
    digest = rig.stage.stage(encoded(REQUEST))
    job = rig.spec()
    require(job.job == "publisher" and job.deadline_seconds == DEADLINE_SECONDS == 86_400, f"the publisher job with its one-day deadline: {job}")
    require(job.bound == {"code_digest": CODE["sha256"], "request_hash": digest, "input_run": RUN}, f"bound to the code and the request file: {job.bound}")
    require(job.acceptance == ("run",) and job.log_container == "run", "acceptance and logs come from the run container")
    run = job.environment["run"]
    require(
        run["PUBLISHER_INPUTS_SHA256"] == "a" * 64
        and run["PUBLISHER_QUEUE_SHA256"] == "b" * 64
        and run["PUBLISHER_RESUME_SOURCES"] == "bls_api,hud_api"
        and run["PUBLISHER_PRIOR_STATE"] == ""
        and run["PUBLISHER_REQUEST_SHA256"] == digest
        and run["RUN_ID"] == RUN,
        f"the run container gets the checked request values: {run}",
    )
    fetch = job.environment["fetch"]
    require(fetch["CODE_RUN_ID"] == CODE["run_id"] and fetch["CODE_SHA256"] == CODE["sha256"], f"the fetch container gets the code archive: {fetch}")
    require(int(run["HAI_DEADLINE_UTC"]) == int(rig.time.now) + 86_400 == int(fetch["HAI_DEADLINE_UTC"]), "both containers carry the job deadline")
    done = rig.launcher().run(job)
    require(done.ok and done.outcome == "succeeded", f"the publisher attempt succeeds: {done}")
    sent = [arguments for name, arguments in rig.ecs.calls if name == "run_task"]
    require(len(sent) == 1, f"one RunTask: {len(sent)}")
    require(rig.ecs.sent == [REVISION], f"RunTask named the checked host revision: {rig.ecs.sent}")
    require(
        sent[0]["capacityProviderStrategy"] == [{"capacityProvider": HOST_PROVIDER, "weight": 1}] and "networkConfiguration" not in sent[0],
        f"host placement with no Fargate network: {sent[0]}",
    )
    intent = rig.record(1, "intent")
    require(intent["deadline_seconds"] == 86_400 and intent["bound"]["request_hash"] == digest, f"the intent records the deadline and request: {intent}")


def publisher_prior_state_reaches_the_task() -> None:
    rig = Rig()
    rig.stage.stage(encoded(REQUEST | {"prior_state": PRIOR}))
    run = rig.spec().environment["run"]
    require(json.loads(run["PUBLISHER_PRIOR_STATE"]) == PRIOR, f"the approved prior state is passed as one JSON value: {run['PUBLISHER_PRIOR_STATE']}")


def publisher_request_not_staged_is_retryable() -> None:
    rig = Rig()
    error = refuses(LaunchUnavailable, rig.spec)
    require(str(error) == "publisher_request_not_staged", f"a request not staged yet is retryable: {error}")
    rig.stage.fault = EndpointConnectionError(endpoint_url="https://s3.invalid")
    rig.stage.stage(encoded(REQUEST))
    error = refuses(LaunchUnavailable, rig.spec)
    require(str(error) == "publisher_request_unreadable", f"a read that fails in transit is retryable: {error}")
    rig.stage.fault = client_error("AccessDenied", "GetObject")
    error = refuses(LaunchUnavailable, rig.spec)
    require(str(error) == "publisher_request_unreadable", f"a refused read is reported: {error}")
    require(rig.ecs.count("run_task") == 0 and not rig.records.objects, "nothing is recorded or launched")


def publisher_request_refusals() -> None:
    bad: dict[str, tuple[bytes, str]] = {
        "too large": (encoded(REQUEST | {"resume_sources": ["a" * 60] * 80}), "publisher_request_too_large"),
        "not JSON": (b"{not json", "publisher_request_malformed"),
        "a list": (b"[]", "publisher_request_needs_exactly_inputs_queue_resume_sources_and_prior_state"),
        "a missing key": (
            encoded({k: v for k, v in REQUEST.items() if k != "prior_state"}),
            "publisher_request_needs_exactly_inputs_queue_resume_sources_and_prior_state",
        ),
        "an extra key": (encoded(REQUEST | {"hosts": "all"}), "publisher_request_needs_exactly_inputs_queue_resume_sources_and_prior_state"),
        "a short digest": (encoded(REQUEST | {"inputs_sha256": "a" * 63}), "publisher_request_has_wrong_values"),
        "an upper-case digest": (encoded(REQUEST | {"queue_sha256": "B" * 64}), "publisher_request_has_wrong_values"),
        "a source with a slash": (encoded(REQUEST | {"resume_sources": ["../bls"]}), "publisher_request_has_wrong_values"),
        "sources as text": (encoded(REQUEST | {"resume_sources": "bls_api"}), "publisher_request_has_wrong_values"),
        "a prior state outside the state prefix": (
            encoded(REQUEST | {"prior_state": PRIOR | {"key": "raw/other/state.json"}}),
            "publisher_request_has_wrong_prior_state",
        ),
        "a prior state with a bad digest": (encoded(REQUEST | {"prior_state": PRIOR | {"sha256": "x"}}), "publisher_request_has_wrong_prior_state"),
        "a prior state with an extra key": (encoded(REQUEST | {"prior_state": PRIOR | {"etag": "e"}}), "publisher_request_has_wrong_prior_state"),
    }
    for label, (body, reason) in bad.items():
        rig = Rig()
        rig.stage.stage(body)
        error = refuses(LaunchRefused, rig.spec)
        require(str(error) == reason, f"{label}: expected {reason}, got {error}")
        require(rig.ecs.count("run_task") == 0 and not rig.records.objects, f"{label}: nothing is recorded or launched")
    rig = Rig()
    rig.stage.stage(encoded(REQUEST))
    error = refuses(LaunchRefused, lambda: publisher_spec(rig.stage, BUCKET, RUN, 1, {"run_id": "latest", "sha256": "c" * 64}, NEED, 0.0))
    require(str(error) == "publisher_code_has_wrong_values", f"a code archive without a run and digest is refused: {error}")


def publisher_request_changed_after_launch_is_refused() -> None:
    rig = Rig()
    rig.stage.stage(encoded(REQUEST))
    require(rig.launcher().run(rig.spec()).ok, "the first attempt succeeds")
    records = dict(rig.records.objects)
    rig.stage.stage(encoded(REQUEST | {"resume_sources": ["bls_api"]}))
    error = refuses(LaunchRefused, lambda: rig.launcher().run(rig.spec(2)))
    require("request_hash" in str(error), f"a request changed after the first launch needs a new run: {error}")
    require(rig.ecs.count("run_task") == 1 and rig.records.objects == records, "the changed request records and launches nothing")


CASES: dict[str, Callable[[], None]] = {
    "publisher_runs_on_the_host_bound_to_its_request": publisher_runs_on_the_host_bound_to_its_request,
    "publisher_prior_state_reaches_the_task": publisher_prior_state_reaches_the_task,
    "publisher_request_not_staged_is_retryable": publisher_request_not_staged_is_retryable,
    "publisher_request_refusals": publisher_request_refusals,
    "publisher_request_changed_after_launch_is_refused": publisher_request_changed_after_launch_is_refused,
}
