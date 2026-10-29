"""Read owner-only approval records for training, forward promotion and rollback."""

from __future__ import annotations

import json
import os
import re
import stat
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from orchestration.interface import SubmissionRequest
from orchestration.training_contract import parameters_for

APPROVAL_SCOPES = {"training_dag": "real_training", "model_promotion_dag": ("promote", "rollback")}
APPROVAL_KEYS = frozenset({"run_id", "dag_id", "scope", "approved_on"})
PROMOTION_KEYS = APPROVAL_KEYS | {"hai_type", "model_version"}
RELEASE_ACTIONS = frozenset({"catalog_release", "gate_accepted", "gate_rejected", "release_accepted", "release_rejected"})


def read_approval(state: Path, request: SubmissionRequest, *, today: date | None = None) -> dict[str, Any]:
    """Require a closed, owner-only approval for the exact run, operation, type and model version."""
    if request.training is None:
        raise ValueError("training approval has no request parameters")
    parameters = parameters_for(request.dag_id, request.training)
    promotion = request.dag_id == "model_promotion_dag"
    expected_scope = parameters["promotion_mode"] if promotion else "real_training"
    descriptors = []
    try:
        descriptors.append(os.open(state, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW))
        for part in ("approvals", request.dag_id):
            descriptors.append(os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptors[-1]))
        descriptors.append(os.open(f"{request.run_id}.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptors[-1]))
        info = os.fstat(descriptors[-1])
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_nlink != 1:
            raise ValueError("approval must be a regular owner-only file of this user")
        raw = os.read(descriptors[-1], 4097)
    except OSError:
        raise ValueError("approval is missing or cannot be read without links") from None
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
    if len(raw) > 4096:
        raise ValueError("approval exceeds its byte limit")
    try:
        body = json.loads(raw)
    except ValueError:
        raise ValueError("approval is not valid JSON") from None
    if not isinstance(body, dict) or set(body) != (PROMOTION_KEYS if promotion else APPROVAL_KEYS):
        raise ValueError("approval fields do not match the closed scope")
    if body["run_id"] != request.run_id or body["dag_id"] != request.dag_id or body["scope"] != expected_scope:
        raise ValueError("approval names another run, DAG or scope")
    if promotion and any(body[name] != parameters[name] for name in ("hai_type", "model_version")):
        raise ValueError("approval names another HAI type or model version")
    approved = body["approved_on"]
    try:
        day = date.fromisoformat(approved) if isinstance(approved, str) and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", approved) else None
    except ValueError:
        day = None
    if day is None or day > (today if today is not None else max(date.today(), datetime.now(UTC).date())):
        raise ValueError("approval date is invalid or in the future")
    return body
