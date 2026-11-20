"""Check the submission request contract without launching containers."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any, TypedDict

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_ROOT = ROOT
EVIDENCE: Path | None = None
RUN = "silver_1234abcd"


class Identity(TypedDict):
    dag_id: str
    task_id: str
    dag_run_id: str


IDENTITY: Identity = {"dag_id": "silver_transform_dag", "task_id": "job", "dag_run_id": "manual__2026-10-17T01:00:00+00:00"}
ENVELOPE = {
    "version": 1,
    "inputs": [{"namespace": "file", "name": "fixture/bronze.duckdb"}],
    "outputs": [{"namespace": "file", "name": "fixture/silver.duckdb"}],
}


def read(relative: str) -> dict[str, Any]:
    return json.loads((SCHEMA_ROOT / relative).read_text())


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def old_request() -> None:
    schema = Draft202012Validator(read("data_contracts/orchestration/submission_request.schema.json"))
    payload = {"template": "silver_build", "run_id": RUN, "mode": "fixture", "attempt": 1}
    errors = list(schema.iter_errors(payload))
    check(any(e.validator == "required" for e in errors), "v1 request without caller identity was accepted")


def identity_bounds() -> None:
    schema = Draft202012Validator(read("data_contracts/orchestration/submission_request.schema.json"))
    payload = {"request_version": 2, "template": "silver_build", "run_id": RUN, "mode": "fixture", "attempt": 1, **IDENTITY}
    schema.validate(payload)
    for field in IDENTITY:
        missing = {key: value for key, value in payload.items() if key != field}
        check(any(e.validator == "required" and field in e.message for e in schema.iter_errors(missing)), f"missing {field} accepted")
        for value in ("", "x" * 251, "unsafe\nidentity", "unsafe/identity", "unsafe\n"):
            mutated = {**payload, field: value}
            errors = list(schema.iter_errors(mutated))
            check(any(list(e.path) == [field] for e in errors), f"{field} bound accepted")
    check(bool(list(schema.iter_errors({**payload, "request_version": 1}))), "old explicit request version accepted")
    check(bool(list(schema.iter_errors({**payload, "lineage": ENVELOPE}))), "caller-supplied lineage accepted")


CASES: dict[str, Callable[[], None]] = {
    "old_request": old_request,
    "identity_bounds": identity_bounds,
}


def main() -> int:
    """Run selected light cases and retain results under the ignored E2E folder."""
    global SCHEMA_ROOT, EVIDENCE
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=sorted(CASES))
    parser.add_argument("--schema-root", type=Path)
    args = parser.parse_args()
    SCHEMA_ROOT = args.schema_root or ROOT
    output = ROOT / "data/e2e/lineage" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    output.mkdir(parents=True)
    EVIDENCE = output
    result: dict[str, Any] = {
        "boundary": "light contracts, no launcher or scheduler",
        "mode": "light, not commit evidence",
        "environment": {"python": sys.version.split()[0], "platform": sys.platform, "jsonschema": version("jsonschema"), "sqlite": sqlite3.sqlite_version},
        "command": [sys.executable, "-m", "scripts.orchestration.run_lineage_e2e", *sys.argv[1:]],
        "revision": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, timeout=10).stdout.strip(),
        "cases": {},
        "cleanup": "no launcher or container started",
        "input_sha256": {
            name: hashlib.sha256((SCHEMA_ROOT / name).read_bytes()).hexdigest()
            for name in ("data_contracts/orchestration/submission_request.schema.json",)
            if (SCHEMA_ROOT / name).is_file()
        },
    }
    for name in args.case or CASES:
        started = time.monotonic()
        try:
            CASES[name]()
        except (AssertionError, OSError, ValueError, TypeError, ImportError, ValidationError) as error:
            result["cases"][name] = {"pass": False, "error": str(error), "seconds": time.monotonic() - started}
        else:
            result["cases"][name] = {"pass": True, "seconds": time.monotonic() - started}
    (output / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    (output / "retention.json").write_text(
        json.dumps(
            {"class": 4, "purpose": "lineage verification summary", "created": datetime.now(UTC).isoformat(), "end_condition": "no record cites this run"}
        )
        + "\n"
    )
    sys.stdout.write(json.dumps({"report": str(output / "report.json"), "cases": result["cases"]}, indent=2) + "\n")
    return 0 if all(item["pass"] for item in result["cases"].values()) else 1


if __name__ == "__main__":
    sys.exit(main())
