"""Check the submission request contract and the lineage service specification without launching containers."""

from __future__ import annotations

import argparse
import hashlib
import importlib
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
RUN = "20261017T010000Z-1234abcd"


class Identity(TypedDict):
    dag_id: str
    task_id: str
    dag_run_id: str


IDENTITY: Identity = {"dag_id": "silver_transform_dag", "task_id": "silver_build", "dag_run_id": "manual__2026-10-17T01:00:00+00:00"}
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


def service_spec() -> None:
    path = SCHEMA_ROOT / "services/lineage/docker-compose.lineage.yaml"
    check(path.is_file(), "lineage Compose proposal is missing")
    model = importlib.import_module("yaml").safe_load(path.read_text())
    check(model["name"] == "hai-lineage", "lineage project is not independent")
    services = model["services"]
    check(set(services) == {"postgres", "api", "web"}, "unexpected lineage services")
    for name, service in services.items():
        check("@sha256:" in service["image"] and len(service["image"].split("@sha256:")[1]) == 64, f"{name} image is not pinned")
        check(service["mem_limit"].startswith("${HAI_LINEAGE_MEM_"), f"{name} memory is not computed")
        expected_networks = {"postgres": ["backend"], "api": ["frontend", "backend"], "web": ["frontend"]}[name]
        check(service["networks"] == expected_networks and service.get("network_mode") is None, f"{name} network escaped its fixed role")
        check(service["healthcheck"]["timeout"] in ("2s", "3s"), f"{name} health wait is unbounded")
        for mount in service.get("volumes", []):
            check(mount.startswith("${HAI_LINEAGE_STATE}/"), f"{name} mounts outside lineage state")
            check(not any(value in mount for value in (".aws", "lakehouse", "/session")), f"{name} mounts foreign authority")
    check(not services["postgres"].get("ports"), "PostgreSQL is published")
    check(services["api"]["ports"] == ["127.0.0.1:${HAI_LINEAGE_API_PORT}:5000"], "API publishes beyond loopback or publishes admin")
    check(services["web"]["ports"] == ["127.0.0.1:${HAI_LINEAGE_WEB_PORT}:3000"], "UI publishes beyond loopback")
    check(model["networks"]["backend"]["internal"] is True, "PostgreSQL backend has external routing")
    check(model["networks"]["frontend"]["internal"] is False, "frontend prevents host ingress")
    check(services["api"]["environment"]["MARQUEZ_CONFIG"] == "/run/hai/config/marquez.yaml", "API config is not privately generated")


CASES: dict[str, Callable[[], None]] = {
    "service_spec": service_spec,
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
        "boundary": "light contracts, no launcher, scheduler or Marquez",
        "mode": "light, not commit evidence",
        "environment": {"python": sys.version.split()[0], "platform": sys.platform, "jsonschema": version("jsonschema"), "sqlite": sqlite3.sqlite_version},
        "command": [sys.executable, "-m", "scripts.orchestration.run_lineage_e2e", *sys.argv[1:]],
        "revision": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT, timeout=10).stdout.strip(),
        "cases": {},
        "cleanup": "no launcher or container started",
        "input_sha256": {
            name: hashlib.sha256((SCHEMA_ROOT / name).read_bytes()).hexdigest()
            for name in ("data_contracts/orchestration/submission_request.schema.json", "services/lineage/docker-compose.lineage.yaml")
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
