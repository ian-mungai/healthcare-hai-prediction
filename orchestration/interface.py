"""The submission interface between Airflow tasks and the host job broker.

DAG code imports only this module (failure mode 771): it names what a task may ask for and what it gets back, never how
the broker launches a container. The broker (scripts/orchestration/broker.py) and the fake in orchestration/fake.py
implement it. A request carries only the template, run ID, mode and attempt (data_contracts/orchestration/
submission_request.schema.json); everything else comes from the template's launch specification. Repeating a request
returns the same handle instead of a second launch (Airflow plan section 7).
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

    PENDING = "pending"  # intent written; no container yet
    RUNNING = "running"  # started or adopted
    SUCCEEDED = "succeeded"  # finished with exit code 0
    FAILED = "failed"  # finished with a non-zero exit code
    CANCELLED = "cancelled"  # stopped on request; finished with the signal's exit code
    NO_OP = "no_op"  # the mode makes the template an intentional no-op; finished without a container
    FAILED_CLOSED = "failed_closed"  # reconciliation found an ambiguous container state; stops for the owner


@dataclass(frozen=True)
class SubmissionRequest:
    """What a caller may send: nothing that changes the launch specification."""

    template: str
    run_id: str
    mode: str
    attempt: int

    @property
    def submission_id(self) -> str:
        """The idempotency key: one launch per template, run, mode and attempt."""
        return f"{self.run_id}:{self.template}:{self.mode}:{self.attempt}"

    @property
    def container_name(self) -> str:
        """The deterministic container name, so a second create of one submission fails instead of duplicating it."""
        return f"hai-{self.run_id}-{self.template}-{self.attempt}"

    def as_dict(self) -> dict[str, Any]:
        """The request as the submission request schema reads it."""
        return {"template": self.template, "run_id": self.run_id, "mode": self.mode, "attempt": self.attempt}


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
