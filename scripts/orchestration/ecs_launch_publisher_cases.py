"""The Run 3 publisher job on the direct ECS launch: the run kind and capture root of a launch become a host launch, against the fake ECS API.

Every case drives the real ``orchestration.publisher_launch`` checks and the real ``Launcher`` and ``S3LaunchStore``. Nothing is staged for the run
but its code, so no bucket holds a request. Each case builds its own rig, so cases run in any order.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import replace
from functools import partial
from typing import Any

from orchestration.ecs_adapter import EcsAdapter, EcsSettings
from orchestration.ecs_launch import EcsFitProbe, JobSpec, Launcher, LaunchRefused, Resources, S3LaunchStore, launch_record_prefix
from orchestration.publisher_launch import DEADLINE_SECONDS, publisher_spec
from scripts.orchestration.ecs_fake import ACCOUNT, CLUSTER, FakeLogs
from scripts.orchestration.ecs_launch_fake import FakeClock, FakeLaunchBucket, RevisionEcs

RUN = "20261110T100000Z-abcdef01"
CODE = {"run_id": "20261110T090000Z-12345678", "sha256": "c" * 64}
BUCKET = "example-private-bucket"
PREFIX = launch_record_prefix()
HOST_PROVIDER = "stand-in_host"
SETTINGS = EcsSettings(
    cluster=CLUSTER,
    family_prefix="example_dev_job_",
    project_tag="example",
    log_group="example_jobs_dev",
    families=frozenset({"publisher"}),
    host_capacity_provider=HOST_PROVIDER,
)
# The revision of the publisher host definition that the preflight checked (registered as revision 1 by every rig).
REVISION = f"arn:aws:ecs:stand-in-region:{ACCOUNT}:task-definition/example_dev_job_publisher_host:1"
NEED = Resources(cpu_units=2048, memory_mib=8192, scratch_bytes=20 * 2**30)
REQUEST = {"kind": "bootstrap", "capture_root": "raw/"}
TEST_ROOT = "ingestion_test/20261113T100000Z-0a1b2c3d/"


class Rig:
    """One fake cloud, launch bucket, stage bucket and clock."""

    def __init__(self) -> None:
        self.ecs, self.logs = RevisionEcs(), FakeLogs()
        self.ecs.register("example_dev_job_publisher_host", NEED.memory_mib, None)
        self.records = FakeLaunchBucket(BUCKET, f"{PREFIX}/")
        self.time = FakeClock()
        self.ecs.clock = lambda: self.time.now
        self.adapter = EcsAdapter(self.ecs, self.logs, SETTINGS)

    def launcher(self) -> Launcher:
        probe = EcsFitProbe(self.ecs, CLUSTER, lambda: 2 * NEED.scratch_bytes)
        return Launcher(self.adapter, S3LaunchStore(self.records, BUCKET, PREFIX), probe, self.time.clock())

    def spec(self, attempt: int = 1, request: object = REQUEST) -> JobSpec:
        return replace(publisher_spec(request, RUN, attempt, CODE, NEED, self.time.now), task_definition=REVISION)

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


def publisher_runs_on_the_host_bound_to_its_request() -> None:
    rig = Rig()
    job = rig.spec()
    require(job.job == "publisher" and job.deadline_seconds == DEADLINE_SECONDS == 86_400, f"the publisher job with its one-day deadline: {job}")
    require(
        job.bound["code_digest"] == CODE["sha256"] and job.bound["input_run"] == RUN and len(job.bound["request_hash"]) == 64,
        f"bound to the code and the digest of the request: {job.bound}",
    )
    require(job.acceptance == ("run",) and job.log_container == "run", "acceptance and logs come from the run container")
    run = job.environment["run"]
    require(
        run
        == {
            "RUN_ID": RUN,
            "CODE_SHA256": CODE["sha256"],
            "PUBLISHER_RUN_KIND": "bootstrap",
            "HAI_CAPTURE_ROOT": "raw/",
            "HAI_DEADLINE_UTC": run["HAI_DEADLINE_UTC"],
        },
        f"the run container gets its code digest, the run kind and the capture root and nothing else: {run}",
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
    require(
        intent["deadline_seconds"] == 86_400 and intent["bound"]["request_hash"] == job.bound["request_hash"],
        f"the intent records the deadline and request: {intent}",
    )


def publisher_launch_names_each_run_kind_and_capture_root() -> None:
    rig = Rig()
    for kind in ("bootstrap", "monthly"):
        for root in ("raw/", TEST_ROOT):
            run = rig.spec(request={"kind": kind, "capture_root": root}).environment["run"]
            require(run["PUBLISHER_RUN_KIND"] == kind and run["HAI_CAPTURE_ROOT"] == root, f"{kind} under {root} reached the task as {run}")
    first = rig.spec(request=REQUEST).bound["request_hash"]
    require(first == rig.spec(request=dict(reversed(REQUEST.items()))).bound["request_hash"], "the digest of the request does not depend on key order")
    require(first != rig.spec(request=REQUEST | {"kind": "monthly"}).bound["request_hash"], "another kind has another digest")
    require(first != rig.spec(request=REQUEST | {"capture_root": TEST_ROOT}).bound["request_hash"], "another root has another digest")


def publisher_capture_root_is_validated_and_reaches_the_task() -> None:
    """The request names the capture root. Only raw/ or one run-ID test folder is accepted, and it reaches the run container as the capture root variable. Every
    other form is refused by name before anything is recorded or launched. (No root is refused too: the DAG form supplies raw/ as its default.)"""
    for given in ("raw/", TEST_ROOT):
        run = Rig().spec(request=REQUEST | {"capture_root": given}).environment["run"]
        require(run["HAI_CAPTURE_ROOT"] == given, f"root {given!r} reaches the run container as {run['HAI_CAPTURE_ROOT']!r}")
    bad_roots = (
        "",
        "ingestion_test/",
        "ingestion_test/x/",
        TEST_ROOT.rstrip("/"),
        TEST_ROOT + "raw/",
        TEST_ROOT.upper(),
        "raw",
        "../raw/",
        "lakehouse/",
        None,
        3,
    )
    for root in bad_roots:
        rig = Rig()
        error = refuses(LaunchRefused, partial(rig.spec, request=REQUEST | {"capture_root": root}))
        require(str(error) == "publisher_request_has_wrong_capture_root", f"root {root!r} is refused: {error}")
        require(rig.ecs.count("run_task") == 0 and not rig.records.objects, f"root {root!r}: nothing is recorded or launched")


def publisher_request_refusals() -> None:
    inputs_or_state = "publisher_request_takes_no_inputs_or_prior_state"
    exact = "publisher_request_needs_exactly_kind_and_capture_root"
    bad: dict[str, tuple[object, str]] = {
        "no request": (None, exact),
        "a list": ([], exact),
        "text": ("bootstrap", exact),
        "a missing root": ({"kind": "bootstrap"}, exact),
        "a missing kind": ({"capture_root": "raw/"}, exact),
        "an extra key": (REQUEST | {"hosts": "all"}, exact),
        "an inputs archive": (REQUEST | {"inputs_sha256": "a" * 64}, inputs_or_state),
        "a queue digest": (REQUEST | {"queue_sha256": "b" * 64}, inputs_or_state),
        "a prior state": (REQUEST | {"prior_state": None}, inputs_or_state),
        "resumed sources": (REQUEST | {"resume_sources": ["bls_api"]}, inputs_or_state),
        "an inputs archive and a prior state with no kind": ({"inputs_sha256": "a" * 64, "prior_state": None, "capture_root": "raw/"}, inputs_or_state),
        "an unknown kind": (REQUEST | {"kind": "retry"}, "publisher_request_has_wrong_kind"),
        "a kind in upper case": (REQUEST | {"kind": "Monthly"}, "publisher_request_has_wrong_kind"),
        "a kind that is not text": (REQUEST | {"kind": ["monthly"]}, "publisher_request_has_wrong_kind"),
        "no slash at the end of the root": (REQUEST | {"capture_root": "raw"}, "publisher_request_has_wrong_capture_root"),
        "the empty root": (REQUEST | {"capture_root": ""}, "publisher_request_has_wrong_capture_root"),
        "another folder": (REQUEST | {"capture_root": "lakehouse/"}, "publisher_request_has_wrong_capture_root"),
        "a test folder without a run id": (REQUEST | {"capture_root": "ingestion_test/t1/"}, "publisher_request_has_wrong_capture_root"),
        "a test folder inside a folder": (REQUEST | {"capture_root": f"raw/{TEST_ROOT}"}, "publisher_request_has_wrong_capture_root"),
        "a test folder with a longer path": (REQUEST | {"capture_root": f"{TEST_ROOT}more/"}, "publisher_request_has_wrong_capture_root"),
        "a root that leaves its folder": (REQUEST | {"capture_root": "ingestion_test/../raw/"}, "publisher_request_has_wrong_capture_root"),
        "a root with a line break": (REQUEST | {"capture_root": "raw/\n"}, "publisher_request_has_wrong_capture_root"),
        "a root that is not text": (REQUEST | {"capture_root": None}, "publisher_request_has_wrong_capture_root"),
    }
    for label, (request, reason) in bad.items():
        rig = Rig()
        error = refuses(LaunchRefused, partial(rig.spec, request=request))
        require(str(error) == reason, f"{label}: expected {reason}, got {error}")
        require(rig.ecs.count("run_task") == 0 and not rig.records.objects, f"{label}: nothing is recorded or launched")
    for label, run_id in (("a run id that is not a run id", "run_a"), ("a run id with a line break", RUN + "\n"), ("no run id", "")):
        rig = Rig()
        error = refuses(LaunchRefused, partial(publisher_spec, REQUEST, run_id, 1, CODE, NEED, 0.0))
        require(str(error) == "publisher_request_has_wrong_run_id", f"{label}: {error}")
        require(rig.ecs.count("run_task") == 0 and not rig.records.objects, f"{label}: nothing is recorded or launched")
    error = refuses(LaunchRefused, lambda: publisher_spec(REQUEST, RUN, 1, {"run_id": "latest", "sha256": "c" * 64}, NEED, 0.0))
    require(str(error) == "publisher_code_has_wrong_values", f"a code archive without a run and digest is refused: {error}")


def publisher_request_changed_after_launch_is_refused() -> None:
    rig = Rig()
    require(rig.launcher().run(rig.spec()).ok, "the first attempt succeeds")
    records = dict(rig.records.objects)
    error = refuses(LaunchRefused, lambda: rig.launcher().run(rig.spec(2, REQUEST | {"kind": "monthly"})))
    require("request_hash" in str(error), f"a request changed after the first launch needs a new run: {error}")
    error = refuses(LaunchRefused, lambda: rig.launcher().run(rig.spec(2, REQUEST | {"capture_root": TEST_ROOT})))
    require("request_hash" in str(error), f"another root after the first launch needs a new run: {error}")
    require(rig.ecs.count("run_task") == 1 and rig.records.objects == records, "the changed request records and launches nothing")


def publisher_task_lists_the_kinds_and_roots_the_launch_lists() -> None:
    """The host job checks the run kind and the capture root again on its own, and its two lists equal those of the launch."""
    from orchestration import ecs_preflight
    from scripts.acquisition import publisher_run, raw_root

    require(publisher_run.KINDS == ecs_preflight.KINDS, f"the kinds differ: {publisher_run.KINDS}, {ecs_preflight.KINDS}")
    for root in ("raw/", TEST_ROOT, "", "raw", "ingestion_test/x/", TEST_ROOT + "raw/", TEST_ROOT.upper(), "lakehouse/"):
        launch = ecs_preflight.CAPTURE_ROOT.fullmatch(root) is not None
        try:
            publisher_run.state_root({"HAI_CAPTURE_ROOT": root})
            task = True
        except publisher_run.PublisherError:
            task = False
        require(launch == task == (root == "raw/" or raw_root.is_test_root(root)), f"root {root!r}: the launch says {launch} and the task says {task}")


CASES: dict[str, Callable[[], None]] = {
    "publisher_runs_on_the_host_bound_to_its_request": publisher_runs_on_the_host_bound_to_its_request,
    "publisher_launch_names_each_run_kind_and_capture_root": publisher_launch_names_each_run_kind_and_capture_root,
    "publisher_capture_root_is_validated_and_reaches_the_task": publisher_capture_root_is_validated_and_reaches_the_task,
    "publisher_request_refusals": publisher_request_refusals,
    "publisher_request_changed_after_launch_is_refused": publisher_request_changed_after_launch_is_refused,
    "publisher_task_lists_the_kinds_and_roots_the_launch_lists": publisher_task_lists_the_kinds_and_roots_the_launch_lists,
}
