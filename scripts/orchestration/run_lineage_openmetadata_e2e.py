"""Check that the host delivery sends each spooled event to Marquez and to OpenMetadata and records what OpenMetadata answered.

Real SQLite outbox, real loopback HTTP and the real CompletionWorker; the two receivers are stand-ins that model the documented answers of Marquez
(HTTP 200, empty body) and of OpenMetadata's ``/api/v1/openlineage/lineage`` (``status``, ``lineageEdgesCreated``, ``unresolvedDatasets``; HTTP 400 when
no dataset resolved). No real OpenMetadata or Marquez runs. Each case is a failing-first contract of ``Outbox.deliver_openmetadata`` and the worker.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import socket
import sqlite3
import sys
import tempfile
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from orchestration import lineage

ROOT = Path(__file__).resolve().parents[2]
BOT_VALUE = "synthetic-lineage-bot-token-not-real"
ROUTE = "/api/v1/openlineage/lineage"
OBSERVED: dict[str, Any] = {}
JOB = {"namespace": "hai-prediction-local", "name": "silver_transform_dag.silver_build"}


def require(value: Any, message: str) -> None:
    if not value:
        raise AssertionError(message)


def event(event_id: str, kind: str, when: str, run: str, inputs: list[str], outputs: list[str]) -> dict[str, Any]:
    """A minimal event with the identity facet the outbox keys on."""
    return {
        "eventType": kind,
        "eventTime": when,
        "run": {"runId": run, "facets": {"haiIdentity": {"eventId": event_id}}},
        "job": dict(JOB),
        "inputs": [{"namespace": "iceberg", "name": name} for name in inputs],
        "outputs": [{"namespace": "iceberg", "name": name} for name in outputs],
    }


RUN_A, RUN_B = "13d9e9ef-93b2-4efc-90d5-066b1becf67d", "91ea107d-e153-4016-a12e-4c09778e8110"


def fixture_events() -> list[dict[str, Any]]:
    """Run A reads known tables; run B reads a table OpenMetadata has not ingested yet."""
    return [
        event("a-start", "START", "2026-11-03T10:00:00+00:00", RUN_A, [], []),
        event("a-done", "COMPLETE", "2026-11-03T10:01:00+00:00", RUN_A, ["hai.bronze.claims"], ["hai.silver.claims"]),
        event("b-done", "COMPLETE", "2026-11-03T10:02:00+00:00", RUN_B, ["hai.silver.claims"], ["hai.gold.hac_scores"]),
    ]


@contextmanager
def receivers(known: set[str]) -> Iterator[dict[str, Any]]:
    """Marquez and OpenMetadata stand-ins on loopback. ``state`` lists every request and lets a case switch each side on or off."""
    state: dict[str, Any] = {"marquez": True, "om": True, "known": known, "marquez_bodies": [], "om_requests": [], "om_status_override": None}

    class Marquez(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *args: Any) -> None:
            """Keep synthetic requests out of the console."""

        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers["Content-Length"]))
            if state["marquez"]:
                state["marquez_bodies"].append(body)
            self.send_response(200 if state["marquez"] else 503)
            self.send_header("Content-Length", "0")
            self.end_headers()

    class OpenMetadata(BaseHTTPRequestHandler):
        def log_message(self, _format: str, *args: Any) -> None:
            """Keep synthetic requests out of the console."""

        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers["Content-Length"]))
            state["om_requests"].append({"path": self.path, "authorization": self.headers.get("Authorization"), "body": body})
            if self.path != ROUTE:
                status, reply = 404, {}
            elif self.headers.get("Authorization") != f"Bearer {BOT_VALUE}":
                status, reply = 401, {}
            elif not state["om"]:
                status, reply = 503, {"status": "FAILURE", "message": "OpenLineage API is disabled", "lineageEdgesCreated": 0}
            else:
                parsed = json.loads(body)
                names = [dataset["name"] for key in ("inputs", "outputs") for dataset in parsed[key]]
                unresolved = [name for name in names if name not in state["known"]]
                edges = len(parsed["inputs"]) * len(parsed["outputs"]) if not unresolved else 0
                if not names:
                    reply = {"status": "SUCCESS", "message": "ok", "lineageEdgesCreated": 0}
                elif unresolved and len(unresolved) == len(names):
                    reply = {"status": "FAILURE", "message": "no dataset resolved", "lineageEdgesCreated": 0, "unresolvedDatasets": unresolved}
                elif unresolved:
                    reply = {"status": "partial_success", "message": "some", "lineageEdgesCreated": 0, "unresolvedDatasets": unresolved}
                else:
                    reply = {"status": "SUCCESS", "message": "ok", "lineageEdgesCreated": edges}
                status = 400 if reply["status"] == "FAILURE" else 200
            if state["om_status_override"] is not None:
                status = state["om_status_override"]
            data = json.dumps(reply).encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    servers = {"marquez": ThreadingHTTPServer(("127.0.0.1", 0), Marquez), "om": ThreadingHTTPServer(("127.0.0.1", 0), OpenMetadata)}
    threads = [threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, name=f"standin_{name}") for name, server in servers.items()]
    for thread in threads:
        thread.start()
    state["marquez_port"], state["om_port"] = servers["marquez"].server_port, servers["om"].server_port
    try:
        yield state
    finally:
        for server in servers.values():
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join(timeout=5)
            require(not thread.is_alive(), "a stand-in receiver did not stop")


def worker_for(database: Path, state: dict[str, Any], *, om: bool = True, marquez: bool = True, credential: str | None = BOT_VALUE) -> lineage.CompletionWorker:
    root = database.parent
    (root / "records").mkdir(exist_ok=True)
    (root / "spool").mkdir(exist_ok=True)
    return lineage.CompletionWorker(
        root / "records",
        root / "spool",
        database,
        port=state["marquez_port"] if marquez else None,
        openmetadata_port=state["om_port"] if om else None,
        openmetadata_token=credential,
    )


def seeded(folder: str) -> Path:
    database = Path(folder).resolve() / "private" / "outbox.sqlite"
    with lineage.Outbox(database) as box:
        box.enqueue(fixture_events())
    return database


def dual_delivery() -> None:
    with tempfile.TemporaryDirectory() as folder, receivers({"hai.bronze.claims", "hai.silver.claims"}) as state:
        database = seeded(folder)
        outcome = worker_for(database, state).run_once()
        require(outcome["delivered"] == 3 and outcome["pending"] == 0, "Marquez did not receive every event")
        require(outcome["om_delivered"] == 3 and outcome["om_pending"] == 0, "OpenMetadata did not receive every event")
        require(len(state["marquez_bodies"]) == 3 and len(state["om_requests"]) == 3, "each event must be sent once to each receiver")
        require([row["body"] for row in state["om_requests"]] == state["marquez_bodies"], "OpenMetadata and Marquez must receive the same event bytes")
        require(all(row["authorization"] == f"Bearer {BOT_VALUE}" and row["path"] == ROUTE for row in state["om_requests"]), "wrong route or credential")
        with lineage.Outbox(database) as box:
            report = box.openmetadata_report()
        require(report["edges_created"] == 1, "the created edge count of the resolved run was not recorded")
        require(report["unresolved_events"] == 1 and report["unresolved_datasets"] == ["hai.gold.hac_scores"], "the unresolved gold output was not recorded")
        require(report["delivered"] == 3 and report["pending"] == 0, "the report disagrees with the outcome")
        OBSERVED["dual_delivery"] = report


def marquez_outage_does_not_block_openmetadata() -> None:
    with tempfile.TemporaryDirectory() as folder, receivers({"hai.bronze.claims", "hai.silver.claims"}) as state:
        database = seeded(folder)
        state["marquez"] = False
        outcome = worker_for(database, state).run_once()
        require(outcome["delivered"] == 0 and outcome["pending"] == 3, "a Marquez outage acknowledged an event")
        require(outcome["om_delivered"] == 3, "a Marquez outage stopped OpenMetadata delivery")
        state["marquez"] = True
        require(worker_for(database, state).run_once()["pending"] == 0, "Marquez did not drain after recovery")
        require(len(state["om_requests"]) == 3, "recovery of Marquez sent an event to OpenMetadata a second time")
        OBSERVED["marquez_outage"] = {"om_requests": len(state["om_requests"]), "marquez_bodies": len(state["marquez_bodies"])}


def openmetadata_outage_does_not_block_marquez() -> None:
    with tempfile.TemporaryDirectory() as folder, receivers({"hai.bronze.claims", "hai.silver.claims"}) as state:
        database = seeded(folder)
        state["om"] = False
        outcome = worker_for(database, state).run_once()
        require(outcome["delivered"] == 3 and outcome["pending"] == 0, "an OpenMetadata outage stopped Marquez delivery")
        require(outcome["om_delivered"] == 0 and outcome["om_pending"] == 3 and outcome.get("om_http") == 503, "the outage was not reported")
        require("error" not in outcome, "an OpenMetadata outage must not fail the pass")
        state["om"] = True
        recovered = worker_for(database, state).run_once()
        require(recovered["om_delivered"] == 3 and recovered["om_pending"] == 0, "pending events did not reach OpenMetadata after recovery")
        require(len(state["marquez_bodies"]) == 3, "recovery of OpenMetadata sent a Marquez event a second time")
        OBSERVED["openmetadata_outage"] = {"first_pass_om_http": 503, "recovered_om_delivered": recovered["om_delivered"]}


def connection_refused_is_tolerated() -> None:
    with tempfile.TemporaryDirectory() as folder, receivers(set()) as state:
        database = seeded(folder)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            state["om_port"] = probe.getsockname()[1]
        outcome = worker_for(database, state).run_once()
        require(outcome["delivered"] == 3 and outcome["om_pending"] == 3 and outcome.get("om_error") == "ConnectionRefusedError", "refusal not tolerated")
        OBSERVED["connection_refused"] = {"om_error": outcome["om_error"]}


def authentication_failure_keeps_events_pending() -> None:
    with tempfile.TemporaryDirectory() as folder, receivers({"hai.bronze.claims"}) as state:
        database = seeded(folder)
        outcome = worker_for(database, state, credential=f"{BOT_VALUE}-wrong").run_once()
        require(outcome["om_delivered"] == 0 and outcome["om_pending"] == 3 and outcome.get("om_http") == 401, "a refused credential counted as delivery")
        require(len(state["om_requests"]) == 1, "delivery must stop at the first refused event, not hammer the server")
        OBSERVED["authentication_failure"] = {"om_http": 401, "requests": 1}


def unresolved_events_replay_after_ingestion() -> None:
    with tempfile.TemporaryDirectory() as folder, receivers(set()) as state:
        database = seeded(folder)
        first = worker_for(database, state).run_once()
        with lineage.Outbox(database) as box:
            before = box.openmetadata_report()
        require(before["unresolved_events"] == 1 or before["unresolved_events"] == 2, "events of unknown tables must be recorded as unresolved")
        require(before["edges_created"] == 0 and first["om_delivered"] == 3, "no edge can exist before the tables are ingested")
        state["known"].update({"hai.bronze.claims", "hai.silver.claims", "hai.gold.hac_scores"})
        idle = worker_for(database, state).run_once()
        require(idle["om_delivered"] == 0 and len(state["om_requests"]) == 3, "an unresolved event must not be resent without a replay request")
        with lineage.Outbox(database) as box:
            requeued = box.replay_unresolved()
        require(requeued == before["unresolved_events"], "replay must requeue exactly the unresolved events")
        worker_for(database, state).run_once()
        with lineage.Outbox(database) as box:
            after = box.openmetadata_report()
        require(after["unresolved_events"] == 0 and after["unresolved_datasets"] == [], "replay after ingestion left unresolved datasets")
        require(after["edges_created"] == 2, "replay did not create the input-to-output edges")
        require(len(state["marquez_bodies"]) == 3, "replay to OpenMetadata resent events to Marquez")
        OBSERVED["replay"] = {"before": before, "after": after}


def credential_is_never_stored() -> None:
    with tempfile.TemporaryDirectory() as folder, receivers({"hai.bronze.claims", "hai.silver.claims"}) as state:
        database = seeded(folder)
        outcome = worker_for(database, state).run_once()
        require(BOT_VALUE not in json.dumps(outcome), "the credential appeared in the pass outcome")
        require(BOT_VALUE.encode() not in database.read_bytes(), "the credential reached the outbox file")
        require(BOT_VALUE not in json.dumps(OBSERVED), "the credential reached the evidence")
        try:
            lineage.CompletionWorker(database.parent / "records", database.parent / "spool", database, port=None, openmetadata_port=state["om_port"])
        except lineage.LineageError:
            pass
        else:
            raise AssertionError("an OpenMetadata port without a credential must be refused at construction")
        OBSERVED["credential"] = {"stored": False}


def older_outbox_is_migrated() -> None:
    with tempfile.TemporaryDirectory() as folder:
        database = Path(folder).resolve() / "private" / "outbox.sqlite"
        database.parent.mkdir(mode=0o700)
        old = sqlite3.connect(database)
        old.execute("CREATE TABLE events (id TEXT PRIMARY KEY, payload TEXT NOT NULL, sequence INTEGER NOT NULL, acked INTEGER NOT NULL DEFAULT 0)")
        old.execute("INSERT INTO events(id,payload,sequence,acked) VALUES ('old','{}',0,1)")
        old.commit()
        old.close()
        with lineage.Outbox(database) as box:
            row = box.connection.execute("SELECT acked,om_state FROM events WHERE id='old'").fetchone()
            require(row == (1, 0), "the existing row must keep its Marquez acknowledgment and start with OpenMetadata pending")
            box.enqueue(fixture_events())
            require(box.openmetadata_report()["pending"] == 4, "an acknowledged old event must still be offered to OpenMetadata once")
        OBSERVED["migration"] = {"old_row": "kept"}


def retried_pass_does_not_duplicate_effects() -> None:
    with tempfile.TemporaryDirectory() as folder, receivers({"hai.bronze.claims", "hai.silver.claims"}) as state:
        database = seeded(folder)
        for _ in range(3):
            worker_for(database, state).run_once()
        require(len(state["om_requests"]) == 3 and len(state["marquez_bodies"]) == 3, "a repeated pass resent events")
        with lineage.Outbox(database) as box:
            require(box.openmetadata_report()["edges_created"] == 1, "a repeated pass changed the edge count")
        OBSERVED["idempotent"] = {"om_requests": 3}


CASES = {
    "dual_delivery": dual_delivery,
    "marquez_outage_does_not_block_openmetadata": marquez_outage_does_not_block_openmetadata,
    "openmetadata_outage_does_not_block_marquez": openmetadata_outage_does_not_block_marquez,
    "connection_refused_is_tolerated": connection_refused_is_tolerated,
    "authentication_failure_keeps_events_pending": authentication_failure_keeps_events_pending,
    "unresolved_events_replay_after_ingestion": unresolved_events_replay_after_ingestion,
    "credential_is_never_stored": credential_is_never_stored,
    "older_outbox_is_migrated": older_outbox_is_migrated,
    "retried_pass_does_not_duplicate_effects": retried_pass_does_not_duplicate_effects,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=sorted(CASES))
    parser.add_argument("--output-root", type=Path, default=ROOT / "data/e2e/lineage_openmetadata")
    args = parser.parse_args()
    output = args.output_root / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    output.mkdir(parents=True)
    sources = [ROOT / path for path in ("orchestration/lineage.py", "scripts/orchestration/run_lineage_openmetadata_e2e.py", "services/lineage/delivery.json")]
    before = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    results = []
    for name in args.case or CASES:
        try:
            CASES[name]()
        except (AssertionError, OSError, ValueError, RuntimeError, AttributeError, TypeError, KeyError, sqlite3.Error) as error:
            results.append({"case": name, "pass": False, "error": f"{type(error).__name__}: {error}"})
        else:
            results.append({"case": name, "pass": True})
    report = {
        "created_at": datetime.now(UTC).isoformat(),
        "command": [sys.executable, "-m", "scripts.orchestration.run_lineage_openmetadata_e2e", *sys.argv[1:]],
        "environment": {"python": sys.version.split()[0], "platform": sys.platform},
        "input_fixtures": "three synthetic events (fixture_events) in this file; the credential is a synthetic string",
        "boundary": "real SQLite outbox, CompletionWorker and loopback HTTP; modeled Marquez and OpenMetadata answers; no real OpenMetadata, Marquez or AWS",
        "observed": OBSERVED,
        "cases": results,
        "pass": all(row["pass"] for row in results),
        "source_sha256": before,
        "cleanup": "no services started; stand-in servers and threads joined",
    }
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    sys.stdout.write(json.dumps({"pass": report["pass"], "report": str(output / "report.json"), "cases": results}) + "\n")
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
