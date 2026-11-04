"""Check the Marquez retirement comparison with synthetic fixtures: a real outbox, loopback stand-ins of Marquez and OpenMetadata and the real comparison code.

The comparison passes only when, for the first silver, gold and feature runs:
every delivered event had zero unresolved datasets in OpenMetadata,
each run's jobs, datasets and input-to-output edges in OpenMetadata equal those in Marquez,
and each run's identity and snapshot status are retrievable from OpenMetadata or from the spool record kept beside it.
The stand-ins model the routes as the author knew them. The real runs and the real servers are not touched, and Marquez is not switched off.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import sys
import tempfile
import threading
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from orchestration import lineage
from scripts.orchestration import openmetadata_marquez_comparison as comparison

ROOT = Path(__file__).resolve().parents[2]
CREDENTIAL = "synthetic-comparison-bot-value"
NAMESPACE = "hai-prediction-aws"
OBSERVED: dict[str, Any] = {}
RUNS: dict[str, dict[str, Any]] = {
    "silver": {
        "id": "20261103T100000Z-aaaaaaa1",
        "job": "silver_transform_dag.silver_build",
        "inputs": ["hai.bronze.claims"],
        "outputs": ["hai.silver.claims"],
    },
    "gold": {
        "id": "20261103T110000Z-bbbbbbb2",
        "job": "gold_dag.gold_build",
        "inputs": ["hai.silver.claims"],
        "outputs": ["hai.gold.hac_scores", "hai.gold.hac_flags"],
    },
    "feature": {
        "id": "20261103T120000Z-ccccccc3",
        "job": "feature_dag.feature_build",
        "inputs": ["hai.gold.hac_scores"],
        "outputs": ["hai.features.hac_window"],
    },
}


def require(value: Any, message: str) -> None:
    if not value:
        raise AssertionError(message)


def native_run(stage: str) -> str:
    digest = hashlib.sha256(stage.encode()).hexdigest()
    return f"{int(digest[:8], 16) % 10**8:08d}-0000-4000-8000-{digest[8:20]}"


def events_for(stage: str, spec: dict[str, Any]) -> list[dict[str, Any]]:
    """START and COMPLETE of one run, with the identity and snapshot-status facets the host builds."""
    facets = {
        "haiIdentity": {"eventId": "", "runId": spec["id"], "attempt": 1},
        "haiSnapshotStatus": {"state": "complete", "missingOutputs": []},
    }
    rows = []
    for kind, hour in (("START", "00"), ("COMPLETE", "01")):
        identity = {**facets["haiIdentity"], "eventId": hashlib.sha256(f"{stage}-{kind}".encode()).hexdigest()}
        rows.append(
            {
                "eventType": kind,
                "eventTime": f"2026-11-03T{spec['id'][9:11]}:{hour}:00+00:00",
                "run": {"runId": native_run(stage), "facets": {**facets, "haiIdentity": identity}},
                "job": {"namespace": NAMESPACE, "name": spec["job"]},
                "inputs": [{"namespace": "iceberg://polaris", "name": name} for name in spec["inputs"]] if kind == "COMPLETE" else [],
                "outputs": [{"namespace": "iceberg://polaris", "name": name} for name in spec["outputs"]] if kind == "COMPLETE" else [],
            }
        )
    return rows


class World:
    """What the two stand-ins hold: Marquez runs, OpenMetadata tables and pipeline edges, and the pipeline extensions."""

    def __init__(self) -> None:
        self.marquez: dict[str, dict[str, Any]] = {}
        self.tables: set[str] = set()
        self.edges: list[tuple[str, str, str]] = []
        self.pipelines: dict[str, dict[str, Any]] = {}

    def fill(self) -> None:
        for stage, spec in RUNS.items():
            self.marquez[native_run(stage)] = {
                "id": native_run(stage),
                "jobVersion": {"namespace": NAMESPACE, "name": spec["job"]},
                "inputDatasetVersions": [{"datasetVersionId": {"namespace": "iceberg://polaris", "name": name}} for name in spec["inputs"]],
                "outputDatasetVersions": [{"datasetVersionId": {"namespace": "iceberg://polaris", "name": name}} for name in spec["outputs"]],
            }
            for name in [*spec["inputs"], *spec["outputs"]]:
                self.tables.add(f"polaris.{name}")
            self.edges.extend((f"polaris.{i}", f"polaris.{o}", spec["job"]) for i in spec["inputs"] for o in spec["outputs"])
            self.pipelines[f"hai_events.{spec['job']}"] = {"name": spec["job"], "extension": {"runId": spec["id"], "snapshotState": "complete"}}


def entity_id(fqn: str) -> str:
    """The UUID string OpenMetadata gives an entity; the stand-in derives it from the fully qualified name."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, fqn))


def reference(fqn: str, kind: str, name: str | None = None) -> dict[str, Any]:
    """An entity reference as OpenMetadata 2.0.7 answers it."""
    return {"id": entity_id(fqn), "type": kind, "name": name or fqn.rsplit(".", 1)[-1], "fullyQualifiedName": fqn}


@contextlib.contextmanager
def servers(world: World) -> Iterator[tuple[str, str]]:
    class Marquez(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *args: Any) -> None:
            """Keep synthetic requests out of the console."""

        def do_GET(self) -> None:
            run = world.marquez.get(self.path.rsplit("/", 1)[-1]) if self.path.startswith("/api/v1/jobs/runs/") else None
            data = json.dumps(run or {}).encode()
            self.send_response(200 if run else 404)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    class OpenMetadata(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *args: Any) -> None:
            """Keep synthetic requests out of the console."""

        def do_GET(self) -> None:
            if self.headers.get("Authorization") != f"Bearer {CREDENTIAL}":
                return self.send(401, {})
            path = self.path.split("?", 1)[0]
            if path.startswith("/api/v1/lineage/table/name/"):
                fqn = path.rsplit("/", 1)[1]
                if fqn not in world.tables:
                    return self.send(404, {})
                mine = [edge for edge in world.edges if fqn in edge[:2]]
                names = sorted({end for edge in mine for end in edge[:2]} - {fqn})
                # The shape of OpenMetadata 2.0.7 type/entityLineage.json: the primary entity is `entity` and absent from `nodes`; an edge names its two
                # entities by UUID string; the pipeline of an edge is an entity reference.
                nodes = [reference(name, "table") for name in names]

                def edge_of(a: str, b: str, job: str) -> dict[str, Any]:
                    return {
                        "fromEntity": entity_id(a),
                        "toEntity": entity_id(b),
                        "lineageDetails": {"source": "OpenLineage", "pipeline": reference(f"hai_events.{job}", "pipeline", job)},
                    }

                upstream = [edge_of(*edge) for edge in mine if edge[1] == fqn]
                downstream = [edge_of(*edge) for edge in mine if edge[0] == fqn]
                return self.send(200, {"entity": reference(fqn, "table"), "nodes": nodes, "upstreamEdges": upstream, "downstreamEdges": downstream})
            if path.startswith("/api/v1/pipelines/name/"):
                pipeline = world.pipelines.get(path.rsplit("/", 1)[1])
                return self.send(200, pipeline) if pipeline else self.send(404, {})
            return self.send(404, {})

        def send(self, status: int, body: dict[str, Any]) -> None:
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    started = [ThreadingHTTPServer(("127.0.0.1", 0), Marquez), ThreadingHTTPServer(("127.0.0.1", 0), OpenMetadata)]
    threads = [threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, name="comparison_standin") for server in started]
    for thread in threads:
        thread.start()
    try:
        yield f"http://127.0.0.1:{started[0].server_port}", f"http://127.0.0.1:{started[1].server_port}"
    finally:
        for server in started:
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join(timeout=5)
            require(not thread.is_alive(), "a stand-in did not stop")


def outbox_with(folder: str, *, states: dict[str, int] | None = None, stages: tuple[str, ...] = tuple(RUNS), strip_facets: bool = False) -> Path:
    """An outbox holding the events of the chosen runs, each marked delivered with no unresolved dataset unless ``states`` says otherwise."""
    database = Path(folder).resolve() / "private" / "outbox.sqlite"
    with lineage.Outbox(database) as box:
        for stage in stages:
            events = events_for(stage, RUNS[stage])
            if strip_facets:
                for event in events:
                    event["run"]["facets"].pop("haiSnapshotStatus")
            box.enqueue(events)
        for event_id, payload in box.connection.execute("SELECT id,payload FROM events").fetchall():
            state = (states or {}).get(json.loads(payload)["run"]["facets"]["haiIdentity"]["runId"], 1)
            result = {"http": 200, "status": "SUCCESS", "edges_created": 1, "unresolved": ["x"] if state == 2 else []}
            with box.connection:
                box.connection.execute("UPDATE events SET om_state=?, om_result=? WHERE id=?", (state, lineage.canonical(result), event_id))
    return database


def run_comparison(database: Path, marquez: str, om: str, *extra: str) -> tuple[int, dict[str, Any], str]:
    out, err = io.StringIO(), io.StringIO()
    report = database.parent / "report.json"
    argv = [
        "--outbox",
        str(database),
        "--marquez",
        marquez,
        "--openmetadata",
        om,
        "--report",
        str(report),
        "--dataset-fqn",
        "polaris.{name}",
        "--pipeline-fqn",
        "hai_events.{job}",
        *extra,
    ]
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = comparison.main(argv, env={"OM_COMPARISON_CREDENTIAL": CREDENTIAL})
    return code, (json.loads(report.read_text()) if report.exists() else {}), out.getvalue() + err.getvalue()


def checks(report: dict[str, Any], stage: str) -> dict[str, Any]:
    return next(row for row in report["runs"] if row["stage"] == stage)


def all_three_runs_match() -> None:
    world = World()
    world.fill()
    with tempfile.TemporaryDirectory() as folder, servers(world) as (marquez, om):
        code, report, _ = run_comparison(outbox_with(folder), marquez, om)
        require(code == 0 and report["pass"], f"matching systems must pass: {report}")
        require(sorted(row["stage"] for row in report["runs"]) == ["feature", "gold", "silver"], "silver, gold and feature runs must all be compared")
        require(
            all(row["jobs_equal"] and row["datasets_equal"] and row["edges_equal"] and row["identity_retrievable"] for row in report["runs"]),
            "a run check failed",
        )
        require(report["unresolved_datasets"] == [] and report["events_not_delivered"] == 0, "unresolved or undelivered events were reported")
        require(
            checks(report, "gold")["edges"] == 2 and checks(report, "gold")["identity_source"] == "openmetadata", "gold edge count or identity source is wrong"
        )
        OBSERVED["all_three_runs_match"] = {row["stage"]: {"edges": row["edges"], "identity_source": row["identity_source"]} for row in report["runs"]}


def unresolved_dataset_fails() -> None:
    world = World()
    world.fill()
    with tempfile.TemporaryDirectory() as folder, servers(world) as (marquez, om):
        code, report, _ = run_comparison(outbox_with(folder, states={RUNS["gold"]["id"]: 2}), marquez, om)
        require(code == 1 and not report["pass"], "an unresolved dataset must fail the comparison")
        require(report["unresolved_events"] >= 1 and not checks(report, "gold")["unresolved_free"], "the gold run must be the one with unresolved datasets")
        require(checks(report, "silver")["unresolved_free"], "an unrelated run was blamed")
        OBSERVED["unresolved"] = {"unresolved_events": report["unresolved_events"]}


def undelivered_event_fails() -> None:
    world = World()
    world.fill()
    with tempfile.TemporaryDirectory() as folder, servers(world) as (marquez, om):
        code, report, _ = run_comparison(outbox_with(folder, states={RUNS["silver"]["id"]: 0}), marquez, om)
        require(code == 1 and report["events_not_delivered"] >= 1, "an event OpenMetadata never answered must fail the comparison")
        OBSERVED["undelivered"] = {"events_not_delivered": report["events_not_delivered"]}


def missing_edge_in_openmetadata_fails() -> None:
    world = World()
    world.fill()
    world.edges.remove(("polaris.hai.silver.claims", "polaris.hai.gold.hac_flags", "gold_dag.gold_build"))
    with tempfile.TemporaryDirectory() as folder, servers(world) as (marquez, om):
        code, report, _ = run_comparison(outbox_with(folder), marquez, om)
        gold = checks(report, "gold")
        require(code == 1 and not gold["edges_equal"], "a missing edge must fail the run")
        require(
            gold["edges_only_in_marquez"] == [["hai.silver.claims", "hai.gold.hac_flags", "gold_dag.gold_build"]] and gold["edges_only_in_openmetadata"] == [],
            "the difference must be named exactly",
        )
        require(checks(report, "silver")["edges_equal"] and checks(report, "feature")["edges_equal"], "unrelated runs were blamed")
        OBSERVED["missing_edge"] = {"only_in_marquez": gold["edges_only_in_marquez"]}


def extra_dataset_in_marquez_fails() -> None:
    world = World()
    world.fill()
    world.marquez[native_run("silver")]["outputDatasetVersions"].append({"datasetVersionId": {"namespace": "iceberg://polaris", "name": "hai.silver.extra"}})
    with tempfile.TemporaryDirectory() as folder, servers(world) as (marquez, om):
        code, report, _ = run_comparison(outbox_with(folder), marquez, om)
        silver = checks(report, "silver")
        require(
            code == 1 and not silver["datasets_equal"] and silver["datasets_only_in_marquez"] == ["hai.silver.extra"],
            "an extra Marquez dataset must be named and fail",
        )
        OBSERVED["extra_dataset"] = {"only_in_marquez": silver["datasets_only_in_marquez"]}


def run_missing_in_marquez_fails() -> None:
    world = World()
    world.fill()
    del world.marquez[native_run("feature")]
    with tempfile.TemporaryDirectory() as folder, servers(world) as (marquez, om):
        code, report, _ = run_comparison(outbox_with(folder), marquez, om)
        require(code == 1 and checks(report, "feature")["marquez_runs_missing"] == 1, "a run Marquez does not know must fail")
        OBSERVED["marquez_missing"] = {"missing": 1}


def identity_falls_back_to_the_spool_record() -> None:
    world = World()
    world.fill()
    world.pipelines.clear()
    with tempfile.TemporaryDirectory() as folder, servers(world) as (marquez, om):
        code, report, _ = run_comparison(outbox_with(folder), marquez, om)
        require(
            code == 0 and all(row["identity_source"] == "spool" for row in report["runs"]), "the spool record must serve when OpenMetadata lacks the identity"
        )
        OBSERVED["identity_fallback"] = {"source": "spool"}


def identity_missing_everywhere_fails() -> None:
    world = World()
    world.fill()
    world.pipelines.clear()
    with tempfile.TemporaryDirectory() as folder, servers(world) as (marquez, om):
        code, report, _ = run_comparison(outbox_with(folder, strip_facets=True), marquez, om)
        require(code == 1 and all(not row["identity_retrievable"] for row in report["runs"]), "a run with no snapshot status anywhere must fail")
        OBSERVED["identity_missing"] = {"failed_runs": len(report["runs"])}


def missing_stage_or_no_runs_fails() -> None:
    world = World()
    world.fill()
    with tempfile.TemporaryDirectory() as folder, servers(world) as (marquez, om):
        code, report, _ = run_comparison(outbox_with(folder, stages=("silver", "gold")), marquez, om)
        require(code == 1 and report["missing_stages"] == ["feature"], "the comparison needs a silver, a gold and a feature run")
    with tempfile.TemporaryDirectory() as folder, servers(world) as (marquez, om):
        code, report, _ = run_comparison(outbox_with(folder, stages=()), marquez, om)
        require(code == 1 and sorted(report["missing_stages"]) == ["feature", "gold", "silver"], "an empty outbox must not pass")
    OBSERVED["stages"] = {"missing": "reported"}


def credential_and_non_loopback_are_refused() -> None:
    world = World()
    world.fill()
    with tempfile.TemporaryDirectory() as folder, servers(world) as (marquez, om):
        database = outbox_with(folder)
        code, report, text = run_comparison(database, marquez, om)
        require(
            CREDENTIAL not in text and CREDENTIAL not in json.dumps(report) and CREDENTIAL not in (database.parent / "report.json").read_text(),
            "the credential reached an output",
        )
        out = io.StringIO()
        with contextlib.redirect_stderr(out):
            refused = comparison.main(
                ["--outbox", str(database), "--marquez", "http://203.0.113.9:5000", "--openmetadata", om, "--report", str(database.parent / "r2.json")],
                env={"OM_COMPARISON_CREDENTIAL": CREDENTIAL},
            )
            absent = comparison.main(
                ["--outbox", str(database), "--marquez", marquez, "--openmetadata", om, "--report", str(database.parent / "r3.json")], env={}
            )
        require(refused == 2 and absent == 2, "a non-loopback address or a missing credential must be refused before any call")
        require(not (database.parent / "r2.json").exists() and not (database.parent / "r3.json").exists(), "a refused run must write no report")
        OBSERVED["refusals"] = {"non_loopback": 2, "no_credential": 2}


def openmetadata_unreachable_fails_visibly() -> None:
    world = World()
    world.fill()
    with tempfile.TemporaryDirectory() as folder, servers(world) as (marquez, _om):
        code, report, _ = run_comparison(outbox_with(folder), marquez, "http://127.0.0.1:9")
        require(code == 1 and not report["pass"] and report["errors"], "an unreachable OpenMetadata must fail with a named error and never pass")
        OBSERVED["unreachable"] = {"errors": len(report["errors"])}


CASES: dict[str, Callable[[], None]] = {
    "all_three_runs_match": all_three_runs_match,
    "unresolved_dataset_fails": unresolved_dataset_fails,
    "undelivered_event_fails": undelivered_event_fails,
    "missing_edge_in_openmetadata_fails": missing_edge_in_openmetadata_fails,
    "extra_dataset_in_marquez_fails": extra_dataset_in_marquez_fails,
    "run_missing_in_marquez_fails": run_missing_in_marquez_fails,
    "identity_falls_back_to_the_spool_record": identity_falls_back_to_the_spool_record,
    "identity_missing_everywhere_fails": identity_missing_everywhere_fails,
    "missing_stage_or_no_runs_fails": missing_stage_or_no_runs_fails,
    "credential_and_non_loopback_are_refused": credential_and_non_loopback_are_refused,
    "openmetadata_unreachable_fails_visibly": openmetadata_unreachable_fails_visibly,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=sorted(CASES))
    parser.add_argument("--output-root", type=Path, default=ROOT / "data/e2e/openmetadata_comparison")
    args = parser.parse_args()
    output = args.output_root / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    output.mkdir(parents=True)
    sources = [
        ROOT / path
        for path in (
            "scripts/orchestration/openmetadata_marquez_comparison.py",
            "scripts/orchestration/run_openmetadata_comparison_e2e.py",
            "orchestration/lineage.py",
        )
    ]
    results = []
    for name in args.case or CASES:
        try:
            CASES[name]()
        except (AssertionError, OSError, ValueError, KeyError, TypeError, AttributeError, RuntimeError, StopIteration) as error:
            results.append({"case": name, "pass": False, "error": f"{type(error).__name__}: {error}"})
        else:
            results.append({"case": name, "pass": True})
    report = {
        "created_at": datetime.now(UTC).isoformat(),
        "command": [sys.executable, "-m", "scripts.orchestration.run_openmetadata_comparison_e2e", *sys.argv[1:]],
        "environment": {"python": sys.version.split()[0], "platform": sys.platform},
        "input_fixtures": "three synthetic runs (RUNS) and a synthetic credential in this file",
        "boundary": (
            "real outbox and comparison code, loopback stand-ins of the Marquez and OpenMetadata routes as the author knew them; "
            "no real run, Marquez, OpenMetadata or AWS; Marquez is not switched off"
        ),
        "observed": OBSERVED,
        "cases": results,
        "pass": all(row["pass"] for row in results),
        "source_sha256": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources},
        "cleanup": "no services started; stand-in servers and threads joined",
    }
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    sys.stdout.write(json.dumps({"pass": report["pass"], "report": str(output / "report.json"), "cases": results}) + "\n")
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
