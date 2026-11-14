"""Build the direct ECS launch request of the Run 3 publisher job from the run kind and the capture root of the launch.

No file is staged for the run but its code archive, which carries the tracked inputs. The launch names the run kind and the capture root only:
``{"kind": "bootstrap" or "monthly", "capture_root": "raw/" or "ingestion_test/<run id>/"}``, and a retry also names its ``original_run`` and its ``day``
and runs under the run id of that day. It names no inputs archive, no queue digest and no prior state, and ``publisher_values`` refuses a request that
does, before any launch record or cloud call. ``publisher_spec`` turns the checked request into the ``JobSpec`` that ``orchestration.ecs_launch.Launcher``
runs on the host: every attempt of a run is bound to the digest of the canonical request and to the run's code digest, so another kind or root for a run
that launched needs a new run. ``orchestration.ecs_preflight`` holds the check, which the DAG's launch runs too: the Airflow containers mount that module
and not this one.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from orchestration.ecs_launch import JobSpec, LaunchRefused, Resources
from orchestration.ecs_preflight import RETRY_VARIABLES, RUN_ID, publisher_values

JOB = "publisher"
# The job's own deadline: one day, the publisher downloads every Run 3 source once.
DEADLINE_SECONDS = 86_400
FETCH_CONTAINER = "fetch"
RUN_CONTAINER = "run"
DIGEST = re.compile(r"[a-f0-9]{64}")
CODE_RUN = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")


def publisher_spec(request: Any, run_id: str, attempt: int, code: Mapping[str, str], need: Resources, now: float) -> JobSpec:
    """The launch request of one publisher attempt. ``request`` is the run kind and the capture root. ``code`` holds the run's code archive ``run_id`` and
    ``sha256``. ``need`` is the host room the job takes; it comes from the publisher's host task definition. ``now`` (epoch seconds) dates
    ``HAI_DEADLINE_UTC``, which only a new task reads: a reattached attempt keeps the recorded deadline of the launch it joins."""
    if not RUN_ID.fullmatch(run_id):
        raise LaunchRefused("publisher_request_has_wrong_run_id")
    code_run, code_digest = code.get("run_id", ""), code.get("sha256", "")
    if not CODE_RUN.fullmatch(code_run) or not DIGEST.fullmatch(code_digest):
        raise LaunchRefused("publisher_code_has_wrong_values")
    values = publisher_values(request, run_id)
    deadline = str(int(now) + DEADLINE_SECONDS)
    return JobSpec(
        run_id=run_id,
        job=JOB,
        attempt=attempt,
        environment={
            FETCH_CONTAINER: {"RUN_ID": run_id, "CODE_RUN_ID": code_run, "CODE_SHA256": code_digest, "HAI_DEADLINE_UTC": deadline},
            RUN_CONTAINER: {
                "RUN_ID": run_id,
                "CODE_SHA256": code_digest,
                "PUBLISHER_RUN_KIND": values["PUBLISHER_RUN_KIND"],
                "HAI_CAPTURE_ROOT": values["HAI_CAPTURE_ROOT"],
                **{name: values[name] for name in RETRY_VARIABLES if name in values},
                "HAI_DEADLINE_UTC": deadline,
            },
        },
        # The publisher reads no earlier run's output, so its input run is its own run; the digest of the request binds what it is asked to do.
        bound={"code_digest": code_digest, "request_hash": values["PUBLISHER_LAUNCH_SHA256"], "input_run": run_id},
        deadline_seconds=DEADLINE_SECONDS,
        need=need,
        acceptance=(RUN_CONTAINER,),
        log_container=RUN_CONTAINER,
        log_prefix=RUN_CONTAINER,
    )
