"""Version 2 caller identity and fixed launch selection for the host broker.

Requests identify their DAG run and task without supplying datasets or overriding a launch specification.
A silver pin request may also name the committed producer run whose silver it pins.
Repeating a request returns the same handle. Reusing its key with different caller identity is refused.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

MODES = ("fixture", "publish_fixture", "real")


class SubmissionRefused(Exception):
    """The request is malformed, names an unknown template or a mode the template does not support."""


class State(StrEnum):
    """A submission's state, read from its ledger lines."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    NO_OP = "no_op"
    FAILED_CLOSED = "failed_closed"


@dataclass(frozen=True)
class SubmissionRequest:
    """What a caller may send: nothing that changes the launch specification."""

    template: str
    run_id: str
    mode: str
    attempt: int
    dag_id: str
    task_id: str
    dag_run_id: str
    input_run_id: str | None = None

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> SubmissionRequest:
        """Recover a v2 caller request from its validated persisted fields."""
        if record.get("request_version") != 2:
            raise SubmissionRefused("request version 2 with caller identity is required")
        try:
            return cls(
                record["template"],
                record["run_id"],
                record["mode"],
                record["attempt"],
                record["dag_id"],
                record["task_id"],
                record["dag_run_id"],
                record.get("input_run_id"),
            )
        except KeyError:
            raise SubmissionRefused("request version 2 is missing caller identity") from None

    def identity(self) -> dict[str, str]:
        """Return the three caller identifiers carried by this request."""
        return {"dag_id": self.dag_id, "task_id": self.task_id, "dag_run_id": self.dag_run_id}

    @property
    def submission_id(self) -> str:
        """The idempotency key: one launch per template, run, mode and attempt."""
        key = f"{self.run_id}:{self.template}:{self.mode}:{self.attempt}"
        return f"{key}:{self.input_run_id}" if self.input_run_id is not None else key

    @property
    def container_name(self) -> str:
        """The deterministic container name, so a second create of one submission fails instead of duplicating it."""
        return f"hai-{self.run_id}-{self.template}-{self.attempt}"

    def as_dict(self) -> dict[str, Any]:
        """The request as the submission request schema reads it."""
        body: dict[str, Any] = {
            "request_version": 2,
            "template": self.template,
            "run_id": self.run_id,
            "mode": self.mode,
            "attempt": self.attempt,
            **self.identity(),
        }
        if self.input_run_id is not None:
            body["input_run_id"] = self.input_run_id
        return body


@dataclass(frozen=True)
class Handle:
    """What submit returns; the same request always returns an equal handle."""

    submission_id: str
    container_name: str


@dataclass(frozen=True)
class Status:
    """A submission's state with its exit code and record, once finished."""

    handle: Handle
    state: State
    exit_code: int | None
    record_path: str | None


class Submissions(Protocol):
    """The five operations a task may call."""

    def submit(self, request: SubmissionRequest) -> Handle:
        """Write the intent, then launch once; a repeated request returns the existing handle."""
        ...

    def status(self, handle: Handle) -> Status:
        """The submission's current state from the ledger."""
        ...

    def cancel(self, handle: Handle) -> Status:
        """Stop the submission's container with its template's signal and grace period; a finished one is unchanged."""
        ...

    def logs(self, handle: Handle, tail: int = 200) -> str:
        """The last ``tail`` lines of the container's output, with secrets redacted."""
        ...

    def record(self, handle: Handle) -> dict[str, Any]:
        """The submission's finished ledger line; refused while it has not finished."""
        ...
