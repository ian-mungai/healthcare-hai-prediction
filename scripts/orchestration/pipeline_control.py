"""Run fixed pipeline lifecycle actions and accept trusted producer completions."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from orchestration import launcher
from orchestration.interface import SubmissionRequest
from orchestration.pipeline_protocol import PipelineError, seal_copy
from scripts.orchestration.broker import Broker, LauncherUnavailable
from scripts.orchestration.run_control import ControlFailed, ControlRefused

ACTIONS = (
    "catalog_up",
    "catalog_release",
    "snapshot_project",
    "snapshot_training",
    "reserve_silver",
    "seal_silver",
    "seal_gold",
    "fixture_pin",
    "fixture_verify",
)
WAIT_SECONDS = 600.0


def wait_for(label: str, read: Callable[[float], dict[str, Any]], ready: Callable[[dict[str, Any]], bool], *, seconds: float = 600.0) -> dict[str, Any]:
    """Poll one launcher operation with an absolute ten-minute deadline.

    ``read`` receives the seconds left and bounds its own request by them. A reply that arrives after the deadline is refused, even when it is ready,
    and a launcher request that the deadline ended is reported as this wait's failure, naming ``label`` and carrying the request's own error. Cleaning
    up an intermediate launcher process that outlived the deadline can end the wait up to 1.0 second after it.
    """
    budget = min(WAIT_SECONDS, seconds)
    deadline = time.monotonic() + budget
    last: dict[str, Any] = {}
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            last = read(remaining)
        except LauncherUnavailable as error:
            if time.monotonic() < deadline:
                raise
            raise ControlFailed(f"{label} did not finish within {budget:g} seconds. Its launcher request ended at the deadline ({error})") from error
        if time.monotonic() >= deadline:
            raise ControlFailed(f"{label} did not finish before its {budget:g} second deadline. The last reply, phase {phase_of(last)}, came after it")
        if ready(last):
            return last
        if last.get("phase") == "failed" or last.get("error"):
            raise ControlFailed(f"{label} failed before readiness")
        time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))
    raise ControlFailed(f"{label} did not finish within {budget:g} seconds. Last phase {phase_of(last)}")


def phase_of(view: dict[str, Any]) -> Any:
    """The phase or state a launcher reply names."""
    return view.get("phase", view.get("state"))


def validation(broker: Broker, request: SubmissionRequest, name: str) -> dict[str, Any]:
    """Require the latest attempt of the named validation to have succeeded on a recorded build."""
    found = [lines for lines in broker.ledger.submissions(request.run_id).values() if lines[0]["template"] == name]
    latest = max(found, key=lambda lines: int(lines[0]["attempt"])) if found else []
    if not latest or latest[-1].get("event") != "finished" or latest[-1].get("exit_code") != 0 or latest[-1].get("cancelled"):
        raise ControlRefused(f"{name} has no latest successful completion")
    result = latest[-1].get("build_file")
    if not isinstance(result, dict) or result.get("run_id") != request.run_id:
        raise ControlRefused(f"{name} has no validated build identity")
    return dict(result)


def perform(broker: Broker, request: SubmissionRequest, action: str, params: dict[str, Any]) -> dict[str, Any]:
    """Execute a closed lifecycle action using only this submission's run principal."""
    if action == "catalog_release" and broker.token_absent(request.run_id):
        return {"registered": False}
    principal = broker.principal(request.run_id)
    folder = broker.runs_root / request.run_id
    try:
        if action == "catalog_up":
            catalog = broker.call("pipeline_catalog_up", {}, principal)
            if catalog.get("local"):
                return catalog
            wait_for(
                "catalog startup",
                lambda left: broker.call("status", {"what": "launcher"}, principal, timeout=left)["catalog"],
                lambda view: view.get("phase") == "up",
            )
            return wait_for(
                "network startup", lambda left: broker.call("pipeline_network_up", {}, principal, timeout=left), lambda view: view.get("phase") == "ready"
            )
        if action == "catalog_release":
            if request.training is not None:
                wait_for(
                    "local MLflow release",
                    lambda left: broker.call("training_mlflow_release", {}, principal, timeout=left),
                    lambda view: view.get("phase") == "down",
                    seconds=120.0,
                )
                if request.mode == "fixture" or request.dag_id == "model_promotion_dag":
                    return {"registered": False, "mlflow": "released"}
            wait_for(
                "network release", lambda left: broker.call("pipeline_network_down", {}, principal, timeout=left), lambda view: view.get("phase") == "down"
            )
            result = broker.call("pipeline_catalog_release", {}, principal)
            if result.get("phase") == "tearing_down":
                return wait_for(
                    "catalog release",
                    lambda left: broker.call("status", {"what": "launcher"}, principal, timeout=left)["catalog"],
                    lambda view: view.get("phase") == "down",
                )
            return result
        if action == "snapshot_training":
            return wait_for(
                "training snapshot",
                lambda left: broker.call("snapshot_training", {"checkout": str(broker.repo_root)}, principal, timeout=left),
                lambda view: view.get("state") == "complete",
                seconds=120.0,
            )
        if action == "snapshot_project":
            payload = {"checkout": str(broker.repo_root), "packages_host": str(broker.repo_root / "data/analytics/dbt/dbt_packages")}
            return wait_for(
                "project snapshot", lambda left: broker.call("snapshot", payload, principal, timeout=left), lambda view: view.get("state") == "complete"
            )
        if action in ("fixture_pin", "fixture_verify"):
            return broker.call(f"pipeline_{action}", {}, principal)
        if action == "reserve_silver":
            return broker.call("pipeline_reserve_silver", {}, principal)
        if action in ("seal_silver", "seal_gold", "fixture_pin", "fixture_verify"):
            layer = action.removeprefix("seal_")
            result = validation(broker, request, f"{layer}_validate")
            seal_copy(folder, layer, str(result["sha256"]))
            payload = {"layer": layer, "attempt_id": result["attempt_id"]}
            if layer == "gold":
                cross = validation(broker, request, "cross_layer_checks")
                if cross["sha256"] != result["sha256"]:
                    raise ControlRefused("cross-layer checks and gold validation read different build bytes")
                payload["cross_attempt_id"] = cross["attempt_id"]
            return broker.call("pipeline_accept_seal", payload, principal)
    except launcher.ClientError as error:
        raise ControlRefused(f"{action} refused by the launcher [{error.code}]") from None
    except PipelineError as error:
        raise ControlFailed(str(error)) from error
    raise ControlRefused("the pipeline control action is unknown")


def completed(broker: Broker, request: SubmissionRequest, attempt: dict[str, Any]) -> dict[str, Any]:
    """Accept exported or pinned identities through the launcher's trusted completion methods."""
    methods = {
        "silver_export": ("pipeline_accept_export", {"layer": "silver"}),
        "gold_export": ("pipeline_accept_export", {"layer": "gold"}),
        "pin_silver": ("pipeline_accept_pin", {}),
        "read_silver": ("pipeline_accept_input", {}),
    }
    if request.template not in methods:
        return {}
    method, values = methods[request.template]
    try:
        result = broker.call(method, {**values, "attempt_id": str(attempt["id"])}, broker.principal(request.run_id))
    except launcher.ClientError as error:
        raise ControlFailed(f"{request.template} result was refused by the launcher [{error.code}]") from None
    return {"producer_acceptance": result}
