"""An in-memory implementation of orchestration.interface.Submissions for package tests.

It checks requests against the submission request schema and the launch specifications, writes the same ledger lines
the broker writes (validated against data_contracts/orchestration/ledger_line.schema.json) and finishes each launch at
once with the exit code the test sets for its template, except the templates the test marks as running, which stay
running until cancel. It starts no container and contacts no service. A test checks
its DAG or launcher against this fake; the broker's own behavior is verified by its E2E (Airflow plan section 11).
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from orchestration.interface import Handle, State, Status, SubmissionRefused, SubmissionRequest

CONTRACTS = Path(__file__).resolve().parents[1] / "data_contracts" / "orchestration"


def validator(name: str) -> Draft202012Validator:
    """A Draft 2020-12 validator for one contract schema."""
    schema = json.loads((CONTRACTS / name).read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


class FakeSubmissions:
    """Submissions that finish at once with scripted exit codes; ``ledger`` holds every line written."""

    def __init__(self, templates: dict[str, Any], exit_codes: dict[str, int] | None = None, running: set[str] | None = None) -> None:
        """``templates`` is a launch specification document; ``exit_codes`` maps a template to its exit code (default 0);
        ``running`` names templates that stay running until cancel, for cancellation tests."""
        validator("launch_spec.schema.json").validate(templates)
        self.templates: dict[str, Any] = templates["templates"]
        self.exit_codes = exit_codes or {}
        self.ledger: list[dict[str, Any]] = []
        self._requests = validator("submission_request.schema.json")
        self._lines = validator("ledger_line.schema.json")
        self._handles: dict[str, Handle] = {}
        self._running = running or set()
        self._cancelled: set[str] = set()

    def _write(self, request: SubmissionRequest, event: str, **fields: Any) -> None:
        """Append one ledger line after checking it against the ledger schema."""
        line = {"schema_version": 1, "event": event, "at": datetime.now(UTC).isoformat(), "submission_id": request.submission_id, **request.as_dict(), **fields}
        self._lines.validate(line)
        self.ledger.append(line)

    def submit(self, request: SubmissionRequest) -> Handle:
        """Refuse a malformed request, return the existing handle for a repeat, otherwise run the launch to its end."""
        errors = sorted(self._requests.iter_errors(request.as_dict()), key=str)
        if errors:
            raise SubmissionRefused(f"request refused: {errors[0].message}")
        spec = self.templates.get(request.template)
        if spec is None:
            raise SubmissionRefused(f"unknown template {request.template}")
        if request.mode not in spec["modes"]:
            raise SubmissionRefused(f"template {request.template} does not support mode {request.mode}")
        if request.submission_id in self._handles:
            return self._handles[request.submission_id]
        handle = Handle(request.submission_id, request.container_name)
        self._handles[request.submission_id] = handle
        spec_sha256 = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
        self._write(
            request,
            "intent",
            spec_sha256=spec_sha256,
            code_sha256="0" * 64,
            inputs={},
            reservation_id=None,
            reserved_memory_bytes=None,
            container_name=request.container_name,
        )
        if spec["modes"][request.mode]["no_op"]:
            self._write(request, "finished", exit_code=0, record_path=None, observed_peak_memory_bytes=None, no_op=True)
            return handle
        container_id = hashlib.sha256(request.submission_id.encode()).hexdigest()
        self._write(request, "started", container_id=container_id, container_state="created")
        if request.template in self._running:
            return handle
        code = self.exit_codes.get(request.template, 0)
        self._write(request, "finished", exit_code=code, record_path=None, observed_peak_memory_bytes=None, no_op=False)
        return handle

    def _lines_of(self, handle: Handle) -> list[dict[str, Any]]:
        """The ledger lines of one submission, or a refusal for an unknown handle."""
        lines = [line for line in self.ledger if line["submission_id"] == handle.submission_id]
        if not lines:
            raise SubmissionRefused(f"unknown submission {handle.submission_id}")
        return lines

    def status(self, handle: Handle) -> Status:
        """The state the last ledger line implies."""
        last = self._lines_of(handle)[-1]
        if last["event"] == "finished":
            if handle.submission_id in self._cancelled:
                return Status(handle, State.CANCELLED, last["exit_code"], last["record_path"])
            state = State.NO_OP if last["no_op"] else State.SUCCEEDED if last["exit_code"] == 0 else State.FAILED
            return Status(handle, state, last["exit_code"], last["record_path"])
        if last["event"] == "failed_closed":
            return Status(handle, State.FAILED_CLOSED, None, None)
        return Status(handle, State.PENDING if last["event"] == "intent" else State.RUNNING, None, None)

    def cancel(self, handle: Handle) -> Status:
        """Stop a running submission (exit code 143, as after SIGTERM); a finished one is unchanged."""
        last = self._lines_of(handle)[-1]
        if last["event"] in ("started", "adopted"):
            request = SubmissionRequest(last["template"], last["run_id"], last["mode"], last["attempt"])
            self._cancelled.add(handle.submission_id)
            self._write(request, "finished", exit_code=143, record_path=None, observed_peak_memory_bytes=None, no_op=False)
        return self.status(handle)

    def logs(self, handle: Handle, tail: int = 200) -> str:
        """Synthetic output naming the submission; no secret exists here to redact."""
        return "\n".join(f"{line['event']} {line['submission_id']}" for line in self._lines_of(handle)[-tail:])

    def record(self, handle: Handle) -> dict[str, Any]:
        """The finished ledger line."""
        last = self._lines_of(handle)[-1]
        if last["event"] != "finished":
            raise SubmissionRefused(f"submission {handle.submission_id} has not finished")
        return dict(last)
