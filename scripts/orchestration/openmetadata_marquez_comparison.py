"""Compare what OpenMetadata and Marquez hold for the first silver, gold and feature runs, the test before Marquez is switched off.

Run on the host (or through a Session Manager port forward) as:
``python -m scripts.orchestration.openmetadata_marquez_comparison --outbox PATH --marquez URL --openmetadata URL --report PATH``
with the comparison bot credential in ``OM_COMPARISON_CREDENTIAL``. Both addresses must be loopback. The outbox is opened read-only.
The comparison passes only when all of these hold for every compared run, and a silver, a gold and a feature run exist:

1. Every event of the run was answered by OpenMetadata with no unresolved dataset.
2. The jobs, the datasets and the input-to-output edges that OpenMetadata holds equal those that Marquez holds.
3. The run identity and the snapshot status can be read from OpenMetadata, or else from the spool record kept beside it (named in the report).

Exit codes: 0 passed, 1 a check failed or a server was unreachable, 2 the arguments were refused before any call.
The report names datasets, jobs and counts and never the credential.
The routes are the ones of the Marquez and OpenMetadata REST APIs as the author knew them and are unproven against the real servers.
This script never switches Marquez off: that stays a separate owner action after the restore test.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import sqlite3
import sys
import urllib.parse
from collections.abc import Mapping
from pathlib import Path
from typing import Any

STAGES = ("silver", "gold", "feature")
LOOPBACK = frozenset({"127.0.0.1", "localhost", "::1"})
CREDENTIAL_VARIABLE = "OM_COMPARISON_CREDENTIAL"
REQUEST_SECONDS = 10


class Refused(ValueError):
    """The arguments or the environment cannot start a comparison."""


class Unreachable(Exception):
    """A server did not answer; the message names the system and the class of the error."""


def loopback_address(value: str, label: str) -> tuple[str, int]:
    parts = urllib.parse.urlsplit(value)
    if parts.scheme != "http" or parts.hostname not in LOOPBACK or parts.port is None or parts.path not in ("", "/"):
        raise Refused(f"{label} must be an http address on loopback with a port, for example http://127.0.0.1:5000")
    return parts.hostname or "", parts.port


class Client:
    """A bounded JSON GET client for one loopback server."""

    def __init__(self, label: str, address: tuple[str, int], credential: str | None = None) -> None:
        self.label, self.address, self.credential = label, address, credential

    def get(self, path: str) -> tuple[int, Any]:
        headers = {"Accept": "application/json"}
        if self.credential:
            headers["Authorization"] = f"Bearer {self.credential}"
        connection = http.client.HTTPConnection(self.address[0], self.address[1], timeout=REQUEST_SECONDS)
        try:
            connection.request("GET", path, headers=headers)
            reply = connection.getresponse()
            raw = reply.read(4 * 1024 * 1024)
            try:
                return reply.status, json.loads(raw) if raw else {}
            except ValueError:
                return reply.status, {}
        except (OSError, http.client.HTTPException) as error:
            raise Unreachable(f"{self.label}: {type(error).__name__}") from None
        finally:
            connection.close()


def load_runs(outbox: Path) -> list[dict[str, Any]]:
    """The events of the outbox as dictionaries, read without changing the file."""
    connection = sqlite3.connect(f"file:{outbox}?mode=ro", uri=True, timeout=5)
    try:
        rows = connection.execute("SELECT id,payload,om_state,om_result FROM events ORDER BY rowid").fetchall()
    finally:
        connection.close()
    return [{"id": row[0], "event": json.loads(row[1]), "om_state": row[2], "om_result": json.loads(row[3]) if row[3] else None} for row in rows]


def group_runs(rows: list[dict[str, Any]], wanted: set[str]) -> dict[str, list[dict[str, Any]]]:
    runs: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        identity = row["event"].get("run", {}).get("facets", {}).get("haiIdentity")
        if isinstance(identity, dict) and (not wanted or identity.get("runId") in wanted):
            runs.setdefault(str(identity["runId"]), []).append(row)
    return runs


def stage_of(jobs: set[str]) -> str:
    for stage in STAGES:
        if any(stage in job for job in jobs):
            return stage
    return "other"


def dataset_names(event: dict[str, Any]) -> tuple[list[str], list[str]]:
    return [str(item["name"]) for item in event.get("inputs", [])], [str(item["name"]) for item in event.get("outputs", [])]


def name_from_fqn(template: str, fqn: str) -> str:
    """The dataset name for a table FQN, by removing the fixed text around {name} in the template; an FQN that does not fit stays as it is."""
    prefix, _, suffix = template.partition("{name}")
    if fqn.startswith(prefix) and fqn.endswith(suffix):
        return fqn[len(prefix) : len(fqn) - len(suffix) if suffix else len(fqn)]
    return fqn


def marquez_view(marquez: Client, run_ids: list[str]) -> dict[str, Any]:
    jobs: set[str] = set()
    datasets: set[str] = set()
    edges: set[tuple[str, str, str]] = set()
    missing = 0
    for run_id in run_ids:
        status, body = marquez.get(f"/api/v1/jobs/runs/{urllib.parse.quote(run_id, safe='')}")
        if status != 200 or not isinstance(body, dict) or not body:
            missing += 1
            continue
        job = str((body.get("jobVersion") or {}).get("name", ""))
        inputs = [str(item["datasetVersionId"]["name"]) for item in body.get("inputDatasetVersions", [])]
        outputs = [str(item["datasetVersionId"]["name"]) for item in body.get("outputDatasetVersions", [])]
        jobs.add(job)
        datasets.update(inputs, outputs)
        edges.update((i, o, job) for i in inputs for o in outputs)
    return {"jobs": jobs, "datasets": datasets, "edges": edges, "missing": missing}


def openmetadata_view(om: Client, names: set[str], jobs: set[str], template: str, pipeline_template: str) -> dict[str, Any]:
    """Jobs, datasets and edges OpenMetadata holds for the named datasets.

    The shape is OpenMetadata 2.0.7 ``type/entityLineage.json``. ``entity`` is the primary table and is not repeated in ``nodes``. An edge names its
    two tables by UUID string in ``fromEntity`` and ``toEntity``. The pipeline of an edge is an entity reference under ``lineageDetails.pipeline``.
    """
    found: set[str] = set()
    edges: set[tuple[str, str, str]] = set()
    for name in sorted(names):
        status, body = om.get(f"/api/v1/lineage/table/name/{urllib.parse.quote(template.format(name=name), safe='')}?upstreamDepth=1&downstreamDepth=1")
        if status != 200 or not isinstance(body, dict):
            continue
        found.add(name)
        by_id = {
            str(node.get("id")): name_from_fqn(template, str(node.get("fullyQualifiedName", "")))
            for node in [body.get("entity") or {}, *body.get("nodes", [])]
            if isinstance(node, dict)
        }
        for edge in [*body.get("upstreamEdges", []), *body.get("downstreamEdges", [])]:
            pipeline = (edge.get("lineageDetails") or {}).get("pipeline") or {}
            job = str(pipeline.get("name") or name_from_fqn(pipeline_template.replace("{job}", "{name}"), str(pipeline.get("fullyQualifiedName", ""))))
            source, target = by_id.get(str(edge.get("fromEntity"))), by_id.get(str(edge.get("toEntity")))
            if job in jobs and source and target:
                edges.add((source, target, job))
    return {"jobs": {edge[2] for edge in edges}, "datasets": found | {end for edge in edges for end in edge[:2]}, "edges": edges}


def identity_source(om: Client, jobs: set[str], run_id: str, complete: list[dict[str, Any]], pipeline_template: str) -> str | None:
    """Where the run identity and the snapshot status can be read: OpenMetadata when a pipeline record names both, else the spool record."""
    for job in sorted(jobs):
        status, body = om.get(f"/api/v1/pipelines/name/{urllib.parse.quote(pipeline_template.format(job=job), safe='')}")
        text = json.dumps(body).lower() if status == 200 else ""
        if run_id.lower() in text and "snapshot" in text:
            return "openmetadata"
    for row in complete:
        facets = row["event"].get("run", {}).get("facets", {})
        identity, snapshot = facets.get("haiIdentity"), facets.get("haiSnapshotStatus")
        if not (
            isinstance(identity, dict) and identity.get("runId") == run_id and identity.get("attempt") and isinstance(snapshot, dict) and snapshot.get("state")
        ):
            return None
    return "spool" if complete else None


def compare_run(run_id: str, rows: list[dict[str, Any]], marquez: Client, om: Client, templates: tuple[str, str]) -> dict[str, Any]:
    complete = [row for row in rows if row["event"].get("eventType") == "COMPLETE"]
    jobs = {str(row["event"]["job"]["name"]) for row in complete}
    declared = {name for row in complete for part in dataset_names(row["event"]) for name in part}
    native = sorted({str(row["event"]["run"]["runId"]) for row in complete})
    expected = marquez_view(marquez, native)
    seen = openmetadata_view(om, declared | expected["datasets"], jobs | expected["jobs"], templates[0], templates[1])
    unresolved = sorted({name for row in rows if row["om_state"] == 2 for name in (row["om_result"] or {}).get("unresolved", [])})
    source = identity_source(om, jobs, run_id, complete, templates[1])
    row: dict[str, Any] = {
        "run_id": run_id,
        "stage": stage_of(jobs),
        "events": len(rows),
        "events_not_delivered": sum(1 for item in rows if item["om_state"] == 0),
        "unresolved_events": sum(1 for item in rows if item["om_state"] == 2),
        "unresolved_free": all(item["om_state"] == 1 for item in rows),
        "unresolved_datasets": unresolved,
        "marquez_runs_missing": expected["missing"],
        "identity_source": source,
        "identity_retrievable": source is not None,
        "edges": len(seen["edges"]),
    }
    for kind in ("jobs", "datasets", "edges"):
        only_marquez, only_om = sorted(expected[kind] - seen[kind]), sorted(seen[kind] - expected[kind])
        row[f"{kind}_equal"] = not only_marquez and not only_om and not expected["missing"]
        row[f"{kind}_only_in_marquez"] = [list(item) if isinstance(item, tuple) else item for item in only_marquez]
        row[f"{kind}_only_in_openmetadata"] = [list(item) if isinstance(item, tuple) else item for item in only_om]
    row["pass"] = bool(row["unresolved_free"] and row["jobs_equal"] and row["datasets_equal"] and row["edges_equal"] and row["identity_retrievable"])
    return row


def main(argv: list[str], env: Mapping[str, str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--outbox", type=Path, required=True)
    parser.add_argument("--marquez", required=True)
    parser.add_argument("--openmetadata", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--dataset-fqn", default="polaris.{name}", help="FQN of the OpenMetadata table for a dataset name; confirm it on the first real run")
    parser.add_argument("--pipeline-fqn", default="hai_events.{job}", help="FQN of the OpenMetadata pipeline for a job name; confirm it on the first real run")
    parser.add_argument("--run-id", action="append", default=[], help="compare only this pipeline run (repeatable); default every run in the outbox")
    options = parser.parse_args(argv)
    environment = os.environ if env is None else env
    try:
        credential = environment.get(CREDENTIAL_VARIABLE, "")
        if not credential:
            raise Refused(f"{CREDENTIAL_VARIABLE} must hold the comparison bot credential")
        if "{name}" not in options.dataset_fqn or "{job}" not in options.pipeline_fqn:
            raise Refused("--dataset-fqn needs {name} and --pipeline-fqn needs {job}")
        marquez = Client("marquez", loopback_address(options.marquez, "--marquez"))
        om = Client("openmetadata", loopback_address(options.openmetadata, "--openmetadata"), credential)
        if not options.outbox.is_file():
            raise Refused("--outbox must be an existing outbox file")
    except Refused as error:
        sys.stderr.write(f"openmetadata_marquez_comparison: {error}\n")
        return 2
    errors: list[str] = []
    rows: list[dict[str, Any]] = []
    try:
        for run_id, events in sorted(group_runs(load_runs(options.outbox), set(options.run_id)).items()):
            rows.append(compare_run(run_id, events, marquez, om, (options.dataset_fqn, options.pipeline_fqn)))
    except Unreachable as error:
        errors.append(str(error))
    except sqlite3.Error as error:
        errors.append(f"outbox: {type(error).__name__}")
    missing = [stage for stage in STAGES if not any(row["stage"] == stage for row in rows)]
    report = {
        "pass": bool(rows) and not errors and not missing and all(row["pass"] for row in rows),
        "runs": rows,
        "missing_stages": missing,
        "errors": errors,
        "events_not_delivered": sum(row["events_not_delivered"] for row in rows),
        "unresolved_events": sum(row["unresolved_events"] for row in rows),
        "unresolved_datasets": sorted({name for row in rows for name in row["unresolved_datasets"]}),
        "marquez_switched_off": False,
    }
    options.report.parent.mkdir(parents=True, exist_ok=True)
    options.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    sys.stdout.write(f"openmetadata_marquez_comparison: {'passed' if report['pass'] else 'FAILED'} ({len(rows)} runs, report written)\n")
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
