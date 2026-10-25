"""Bind catalog authority and owner evidence inside the launcher's registry transaction."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from orchestration.catalog_authority import AuthorityError, check_writer, read_authority, writer_namespaces
from orchestration.ecs_target import PlacementError, validate_execution, writer_retry_blocked

CLASSES = ("offline", "seal", "lakehouse_read", "lakehouse_write")


def _open(record: dict[str, Any] | None) -> dict[str, Any]:
    if record is None or record["state"] != "aws_writer" or record["target"] != "aws" or record["frozen"]:
        raise PlacementError("catalog admission is closed")
    return record


def _writer(record: Mapping[str, Any], admission: Mapping[str, Any], authorization: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if authorization is None or set(authorization) != {"qualification", "layer_activation", "publish_approval"}:
        raise PlacementError("writer authorization is missing or malformed")
    if not isinstance(authorization["qualification"], str) or not authorization["qualification"].strip():
        raise PlacementError("writer qualification reference is required")
    check_writer(
        record,
        epoch=record["epoch"],
        layer=admission["layer"],
        mode=admission["mode"],
        target="ecs",
        run_id=admission["run_id"],
        namespaces=admission["namespaces"],
        approval=authorization["publish_approval"],
    )
    if admission["mode"] == "real":
        if not isinstance(authorization["layer_activation"], str) or not authorization["layer_activation"].strip():
            raise PlacementError("production layer activation reference is required")
        approval = authorization["publish_approval"]
        if not isinstance(approval, Mapping) or not isinstance(approval.get("reference"), str) or not approval["reference"].strip():
            raise PlacementError("per-publish approval reference is required")
    elif authorization["layer_activation"] is not None or authorization["publish_approval"] is not None:
        raise PlacementError("qualification cannot inherit production authorization")
    return authorization


def bind(
    state: Mapping[str, Any],
    attempt: dict[str, Any],
    *,
    job_class: str,
    registry_kind: str,
    run: Mapping[str, Any],
    layer: str | None = None,
    authorization: Mapping[str, Any] | None = None,
) -> None:
    """Persist the current catalog and exact owner evidence before reserving a placement.

    ``authorization`` comes from the authenticated owner adapter and its immutable
    evidence records, never from untrusted job parameters. This function does not
    issue approval, activate a layer or change the authority record.
    """
    if job_class not in CLASSES:
        raise PlacementError("unknown job class")
    execution = attempt["execution"]
    if execution["placements"] or "admission" in execution:
        raise PlacementError("admission binding is already frozen")
    admission: dict[str, Any] = {
        "job_class": job_class,
        "registry_kind": registry_kind,
        "run_id": run.get("id"),
        "mode": run.get("mode"),
        "layer": layer,
        "namespaces": [],
    }
    try:
        record = read_authority(state, registry_kind=registry_kind) if job_class.startswith("lakehouse_") else None
        if job_class.startswith("lakehouse_"):
            record = _open(record)
            if execution["catalog_epoch"] is not None and execution["catalog_epoch"] != record["epoch"]:
                raise PlacementError("stale catalog epoch")
        if job_class == "lakehouse_write":
            if execution["requested_target"] == "docker":
                raise PlacementError("writers require ECS on demand")
            record = _open(record)
            if not isinstance(layer, str) or not isinstance(admission["mode"], str):
                raise PlacementError("writer layer and mode are required")
            admission["namespaces"] = writer_namespaces(layer, admission["mode"])
            authorization = _writer(record, admission, authorization)
            for prior in state.get("attempts", {}).values():
                previous = prior.get("execution", {})
                same_operation = (
                    prior.get("id") != attempt["id"]
                    and previous.get("catalog_epoch") == record["epoch"]
                    and previous.get("admission") == admission
                    and previous["writer_authorization"]["publish_approval"] == authorization["publish_approval"]
                )
                if same_operation:
                    validate_execution(previous)
                    if writer_retry_blocked(previous):
                        raise PlacementError("writer operation requires reconciliation before another attempt")
        elif authorization is not None:
            raise PlacementError("non-write admission cannot carry writer authorization")
    except (AuthorityError, TypeError, KeyError) as error:
        raise PlacementError(str(error) if isinstance(error, AuthorityError) else "malformed catalog admission") from None
    execution["admission"] = copy.deepcopy(admission)
    execution["catalog_authority"] = copy.deepcopy(record)
    execution["writer_authorization"] = copy.deepcopy(authorization)
    execution["catalog_epoch"] = _open(record)["epoch"] if job_class.startswith("lakehouse_") else None


def current(state: Mapping[str, Any], attempt: Mapping[str, Any], *, registry_kind: str) -> None:
    """Recheck changing authority at reservation and again immediately before dispatch."""
    execution = attempt["execution"]
    admission = execution.get("admission")
    if not isinstance(admission, Mapping):
        if execution["catalog_epoch"] is None:
            return
        raise PlacementError("catalog admission binding is missing")
    if admission["registry_kind"] != registry_kind:
        raise PlacementError("catalog registry kind changed")
    if not admission["job_class"].startswith("lakehouse_"):
        return
    try:
        record = read_authority(state, registry_kind=registry_kind)
        record = _open(record)
        saved = execution.get("catalog_authority")
        if not isinstance(saved, Mapping) or any(record[key] != saved[key] for key in ("epoch", "state", "target", "frozen", "cutover_id")):
            raise PlacementError("stale catalog epoch")
        if execution["catalog_epoch"] != record["epoch"]:
            raise PlacementError("stale catalog epoch")
        if admission["job_class"] == "lakehouse_write":
            _writer(record, admission, execution.get("writer_authorization"))
    except (AuthorityError, KeyError, TypeError) as error:
        raise PlacementError(str(error) if isinstance(error, AuthorityError) else "malformed catalog admission") from None


def matches(state: Mapping[str, Any], attempt: Mapping[str, Any]) -> bool:
    """Return the launcher's fail-closed authority predicate without performing a service call."""
    admission = attempt["execution"].get("admission", {})
    try:
        current(state, attempt, registry_kind=admission.get("registry_kind", "real"))
    except PlacementError:
        return False
    return True
