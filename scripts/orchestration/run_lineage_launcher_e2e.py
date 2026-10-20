"""Exercise declared lineage and snapshot records through real test-launcher IPC with a fake engine."""

from __future__ import annotations

import argparse
import copy
import json
import shutil
import sys
import threading
import uuid
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from unittest import mock

from orchestration import dbt_lineage, launcher
from orchestration.interface import State, SubmissionRequest
from orchestration.lineage import Outbox, build_events, canonical, sweep
from scripts.orchestration.broker import Broker, BrokerPaths, load_templates
from scripts.orchestration.run_broker_e2e import RUN, BrokerEnv, run_record_file
from scripts.orchestration.run_lineage_provider_e2e import test_launcher, wait

ROOT = Path(__file__).resolve().parents[2]
MODE = "publish_fixture"


def require(value: Any, message: str) -> None:
    if not value:
        raise AssertionError(message)


def release_finished_run(broker: Broker, run_id: str, record: str) -> None:
    """Require the broker's approved release interface to confirm that the test reservation is released."""
    broker.finalize_run(run_id)
    require(broker.release_run(run_id, record).get("state") == "released", "the final test reservation was not released")


class SyntheticProducer(Broker):
    """Test producer: emit the closed metadata at the existing completion-validation boundary."""

    produced: dict[int, dict[str, Any]]

    def validated_build(self, request: SubmissionRequest, attempt: dict[str, Any]) -> dict[str, Any]:
        return {"snapshot_record": copy.deepcopy(self.produced[request.attempt])} if request.attempt in self.produced else {}


def transport(events: list[dict[str, Any]], database: Path) -> dict[str, Any]:
    received: list[bytes] = []

    class Receiver(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            """Keep synthetic request logs out of the console."""

        def do_POST(self) -> None:
            received.append(self.rfile.read(int(self.headers["Content-Length"])))
            if len(received) > 1:
                self.send_response(201)
                self.send_header("Content-Length", "0")
                self.end_headers()

    server = HTTPServer(("127.0.0.1", 0), Receiver)
    server.timeout = 1

    def receive() -> None:
        for _ in range(len(events) + 1):
            server.handle_request()

    thread = threading.Thread(target=receive, name="lineage_launcher_receiver")
    thread.start()
    try:
        with Outbox(database) as box:
            pending = box.pending(100)
            lost = box.deliver("127.0.0.1", server.server_port, timeout=0.5, limit=100, deadline_seconds=10)
            require(lost["delivered"] == 0 and box.pending(100) == pending, "lost reply altered or acknowledged queued bytes")
            resent = box.deliver("127.0.0.1", server.server_port, timeout=0.5, limit=100, deadline_seconds=10)
            require(resent == {"delivered": len(events), "pending": 0}, "resend did not acknowledge all persisted events")
            require(box.deliver("127.0.0.1", server.server_port) == {"delivered": 0, "pending": 0}, "acknowledged events were resent")
    finally:
        thread.join(timeout=12)
        server.server_close()
    require(not thread.is_alive(), "receiver did not stop")
    require(len(received) == len(events) + 1 and received[0] == received[1], "lost reply changed event bytes")
    return {"requests": len(received), "events": len(events), "same_bytes_on_retry": received[0] == received[1]}


CASE_MOUNT = "/workspace/out/case"
DATASET = {"namespace": "file", "name": "fixture/staging.duckdb"}
CAPTURE_FAILED = dbt_lineage.CAPTURE_FAILED
CLOSED_CONTRACT = "dbt artifact record violates its closed contract"


def dbt_stand_in(container: dict[str, Any], honors_flag: bool, seen: list[dict[str, str]]) -> tuple[float, int]:
    """Play dbt in the fake daemon: write native artifacts to the case's ``target`` folder the way dbt does when partial parsing finds a saved manifest.

    A saved manifest is reused with its recorded environment unless ``DBT_PARTIAL_PARSE=false`` is set; ``run_results.json`` always carries the
    environment of the invocation. Probe containers and jobs without the attempt variable write nothing."""
    if "hai.probe" in container["Config"]["Labels"]:
        return 0.2, 0
    entries = dict(item.partition("=")[::2] for item in container["Config"]["Env"])
    seen.append(entries)
    if dbt_lineage.ATTEMPT_ENV not in entries:
        return 0.2, 0
    host = next(Path(mount["Source"]) for mount in container["Mounts"] if mount["Destination"] == CASE_MOUNT)
    target = host / "target"
    target.mkdir(exist_ok=True)
    attempt = entries[dbt_lineage.ATTEMPT_ENV]
    saved = target / "manifest.json"
    reuse = saved.exists() and not (honors_flag and entries.get("DBT_PARTIAL_PARSE") == "false")
    manifest_env = json.loads(saved.read_text())["metadata"]["env"] if reuse else {"HAI_ATTEMPT_ID": attempt}
    invocation = str(uuid.uuid4())
    command = container["Config"]["Cmd"]
    model = f"model.tiny.{command[command.index('--select') + 1]}"
    nodes = {name: {"resource_type": "model", "config": {"materialized": "table"}, "depends_on": {"nodes": []}} for name in dbt_nodes()}
    saved.write_text(json.dumps({"metadata": {"invocation_id": invocation, "env": manifest_env}, "nodes": nodes, "sources": {}}))
    (target / "run_results.json").write_text(
        json.dumps({"metadata": {"invocation_id": invocation, "env": {"HAI_ATTEMPT_ID": attempt}}, "results": [{"unique_id": model, "status": "success"}]})
    )
    return 0.2, 0


def dbt_nodes() -> dict[str, dict[str, str]]:
    """Every model of the tiny project bound to the step's one output, as the transform fixtures bind them."""
    return {"model.tiny.silver_one": dict(DATASET), "model.tiny.gold_one": dict(DATASET)}


def launcher_reports(broker: Broker, error: Any) -> Any:
    """The broker's view of an attempt whose capture record is replaced by the launcher's recorded ``dbt_artifact_error``."""
    original = broker.attempt_of

    def attempt_of(request: SubmissionRequest, forward: dict[str, Any]) -> dict[str, Any]:
        attempt = original(request, forward)
        attempt.pop("dbt_artifacts", None)
        attempt["dbt_artifact_error"] = error
        return attempt

    return attempt_of


def partial_parse(output: Path) -> int:
    """Run two captured dbt attempts of one run on one shared ``target`` folder through the real launcher and broker; the second must still be captured.

    Also checks that a job without capture gets neither attempt variable and that a dbt which ignores the partial-parse variable still fails closed."""
    result: dict[str, Any] = {
        "status": "stop",
        "boundary": "real launcher process, IPC and broker; fake Docker whose script plays dbt (including its partial-parse manifest reuse); no real dbt",
    }
    env = BrokerEnv("partial_parse", output)
    seen: list[dict[str, str]] = []
    honors = [True]
    try:
        document = env.inventory()
        for name in ("silver_build", "gold_build"):
            row = env.fixture_row(["run", "--select", f"{name.partition('_')[0]}_one"], timeout_seconds=10)
            row["launcher"]["lineage"] = {"version": 1, "inputs": [], "outputs": [dict(DATASET)]}
            row["launcher"]["dbt_nodes"] = dbt_nodes()
            document["templates"][name]["modes"]["fixture"] = row
        document["templates"]["gold_validate"]["modes"]["fixture"] = env.fixture_row(["test"])
        env.write_inventory(document)
        env.daemon.script = lambda container: dbt_stand_in(container, honors[0], seen)
        broker = env.new_broker()
        broker.start()

        def submit(template: str, attempt: int, wanted: State) -> Any:
            request = SubmissionRequest(template, RUN, "fixture", attempt, "transform_dag", template, "manual__partial_parse")
            handle = broker.submit(request)
            ended = env.wait_state(broker, handle, (State.SUCCEEDED, State.FAILED, State.FAILED_CLOSED, State.CANCELLED))
            last = broker.lines_of(request)[-1]
            require(ended.state == wanted, f"{template} attempt {attempt} ended {ended.state.value}, not {wanted.value}: {last.get('reason')}")
            return request, handle

        _, silver_handle = submit("silver_build", 1, State.SUCCEEDED)
        _, gold_handle = submit("gold_build", 1, State.SUCCEEDED)
        _, validate_handle = submit("gold_validate", 1, State.SUCCEEDED)
        for handle, model in ((silver_handle, "model.tiny.silver_one"), (gold_handle, "model.tiny.gold_one")):
            recorded = json.loads(Path(broker.record(handle)["record_path"]).read_text())
            models = [edge["model"] for edge in recorded.get("dbt_lineage", {}).get("models", [])]
            require(models == [model], f"the captured attempt of {model} was recorded with the model edges {models}; the record has {sorted(recorded)}")
        require("dbt_lineage" not in json.loads(Path(broker.record(validate_handle)["record_path"]).read_text()), "an uncaptured job recorded dbt lineage")
        require(len(seen) == 3, f"the daemon started {len(seen)} jobs, not 3")
        for step, entries in zip(("silver_build", "gold_build"), seen, strict=False):
            require(entries.get("DBT_PARTIAL_PARSE") == "false", f"{step}: a captured attempt does not disable partial parsing")
        require(
            not any(key in seen[2] for key in (dbt_lineage.ATTEMPT_ENV, "DBT_PARTIAL_PARSE")),
            "a job without capture received a capture variable",
        )
        honors[0] = False
        stale_request, _ = submit("gold_build", 2, State.FAILED_CLOSED)
        require(
            not (broker.paths.records / RUN / f"{stale_request.container_name}.json").exists(),
            "a manifest from another attempt was accepted: the attempt binding was weakened",
        )
        stale_reason = broker.lines_of(stale_request)[-1]["reason"]
        require(stale_reason == CAPTURE_FAILED, f"a stale manifest closed with {stale_reason!r}, not the launcher's capture failure {CAPTURE_FAILED!r}")
        honors[0] = True
        known_value = "synthetic-run-value-0123456789"
        broker.remember_secret(known_value)
        hostile = (
            f"hostile-note\n\x1b[31m {known_value} in /srv/example/data/lakehouse/secrets/aws.json with AWS_SECRET_ACCESS_KEY=hunter2value " + "z" * 10000,
            'hostile-note: TOKEN="quoted hunter2value"',
            "hostile-note: AWS_SECRET_ACCESS_KEY = hunter2value",
            "hostile-note: Authorization: Bearer hunter2value",
            "hostile-note in /srv/example/lakehouse/secrets/my hunter2value.json",
        )
        recorded_errors: tuple[object, ...] = (*hostile, 7)
        for attempt, recorded in enumerate(recorded_errors, start=3):
            with mock.patch.object(broker, "attempt_of", launcher_reports(broker, recorded)):
                request, _ = submit("gold_build", attempt, State.FAILED_CLOSED)
            lines = broker.lines_of(request)
            reason = lines[-1]["reason"]
            if not isinstance(recorded, str):
                require(reason == CLOSED_CONTRACT, f"a malformed capture field closed with {reason!r}, not the contract text")
                continue
            require(reason == dbt_lineage.CAPTURE_WITHHELD, f"an unknown recorded capture failure closed with {reason!r}, not the fixed message")
            ledger = json.dumps(lines)
            leaks = [part for part in (known_value, "/srv/example", "lakehouse/secrets", "hunter2value", "hostile-note") if part in ledger]
            require(not leaks, f"the ledger of an unknown capture failure holds {leaks}")
        release_finished_run(broker, RUN, run_record_file(env, RUN))
        result.update(status="pass", captured_attempts=2, uncaptured_attempts=1, stale_manifest_refused=True, capture_failure_reported=True)
    except Exception as error:
        result["error"] = f"{type(error).__name__}: {error}"
    finally:
        result["leftovers"] = env.close()
        if result["leftovers"]:
            result["status"] = "stop"
    (output / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    sys.stdout.write(json.dumps({"status": result["status"], "report": str(output / "report.json"), "error": result.get("error")}) + "\n")
    return 0 if result["status"] == "pass" else 1


def native_artifacts(output: Path) -> int:
    """Probe native dbt metadata and capture with one tiny offline fixture on a real test launcher."""
    result: dict[str, Any] = {"status": "stop", "launchers": [], "boundary": "real launcher and offline tiny fixture; no real catalog, cloud or image pull"}
    try:
        with test_launcher({}, result["launchers"]) as client:
            root = client.config.registry.path.parent.resolve()
            case = root / "native_case"
            shutil.copytree(ROOT / "tests/orchestration/fixtures/tiny", case / "project")
            packages = case / "packages"
            packages.mkdir()
            dataset = {"namespace": "file", "name": "fixture/staging.duckdb"}
            document = load_templates()
            document["templates"]["silver_build"]["modes"]["fixture"] = {
                "class": "offline",
                "no_op": False,
                "launcher": {
                    "template": "fixture_dbt",
                    "args": ["run", "--select", "silver_one"],
                    "params": {"case_host": str(case), "case_container": "/workspace/out/case", "packages_host": str(packages)},
                    "timeout_seconds": 300,
                    "lineage": {"version": 1, "inputs": [], "outputs": [dataset]},
                    "dbt_nodes": {"model.tiny.silver_one": dataset},
                },
            }
            broker = Broker(BrokerPaths(client.config.registry, root / "broker"), document, client)
            acquired = False
            handle = None
            try:
                broker.start()
                request = SubmissionRequest("silver_build", RUN, "fixture", 1, "native_lineage_dag", "silver_build", "manual__native")
                broker.ensure_run(request)
                acquired = True
                handle = broker.submit(request)

                def completed() -> Any:
                    status = broker.status(handle)
                    if status.state in (State.FAILED, State.FAILED_CLOSED, State.CANCELLED):
                        raise RuntimeError("native dbt probe ended " + status.state.value)
                    return status if status.state == State.SUCCEEDED else None

                wait(completed, 330, "native dbt lineage completion")
                record = json.loads(Path(broker.record(handle)["record_path"]).read_text())
                models = record.get("dbt_lineage", {}).get("models")
                require(models and models[0]["model"] == "model.tiny.silver_one", "native model edge was not persisted")
                events = build_events(record)
                require(len(events) == 4, "native model record did not emit a task and model run")
                (output / "events.json").write_text(json.dumps(events, indent=2) + "\n")
                result.update(
                    status="pass", models=len(models), events=len(events), artifact_hashes=record["dbt_lineage"]["files"], attempt_id=record["launcher"]["id"]
                )
            finally:
                if acquired:
                    if handle is not None and broker.status(handle).state in (State.PENDING, State.RUNNING):
                        broker.cancel(handle)
                        wait(lambda: broker.status(handle).state not in (State.PENDING, State.RUNNING), 90, "native probe cancellation")
                    broker.finalize_run(RUN)
                    release_record = root / "native_run.json"
                    release_record.write_text(json.dumps({"run_id": RUN}))
                    broker.release_run(RUN, str(release_record))
                broker.close()
    except Exception as error:
        result.update(status="stop", error=f"{type(error).__name__}: {error}")
    (output / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    sys.stdout.write(json.dumps({"status": result["status"], "report": str(output / "report.json"), "error": result.get("error")}) + "\n")
    return 0 if result["status"] == "pass" else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--real-artifacts", action="store_true", help="one offline tiny fixture on a real test launcher; requires the heavy slot")
    parser.add_argument("--partial-parse", action="store_true", help="two captured dbt attempts on one shared target folder, fake engine")
    parser.add_argument("--mutation", choices=("final_release",), help="omit the final release to check its independent outcome assertion")
    args = parser.parse_args()
    if (args.real_artifacts or args.partial_parse) and args.mutation:
        parser.error("the final-release mutation uses only the fake-engine case")
    if args.real_artifacts and args.partial_parse:
        parser.error("choose one of --real-artifacts and --partial-parse")
    output = ROOT / "data/e2e/lineage_launcher" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    output.mkdir(parents=True)
    if args.real_artifacts:
        return native_artifacts(output)
    if args.partial_parse:
        return partial_parse(output)
    result: dict[str, Any] = {
        "status": "stop",
        "boundary": (
            "real launcher process and IPC; fake Docker; synthetic producer and HTTP receiver; "
            "test-only publish_fixture rows on the fixture_dbt template, no real publishing template, catalog or publish input provenance"
        ),
    }
    result["mutation"] = args.mutation
    env = BrokerEnv("declared_lineage", output)
    final_run: tuple[Broker, str] | None = None
    try:
        document = env.inventory()
        shared = {"namespace": "iceberg://polaris:8181/hai_lakehouse", "name": "silver.synthetic_measure"}
        bronze = {"namespace": "file", "name": "fixture/bronze.duckdb"}
        gold = {"namespace": "iceberg://polaris:8181/hai_lakehouse", "name": "gold.synthetic_measure"}
        document["templates"]["silver_build"]["modes"][MODE] = env.fixture_row(["build"])
        document["templates"]["gold_build"]["modes"][MODE] = env.fixture_row(["run"], timeout_seconds=10)
        silver_row = document["templates"]["silver_build"]["modes"][MODE]["launcher"]
        gold_row = document["templates"]["gold_build"]["modes"][MODE]["launcher"]
        silver_row["lineage"] = {"version": 1, "inputs": [bronze], "outputs": [shared]}
        gold_row["lineage"] = {"version": 1, "inputs": [shared], "outputs": [gold]}
        env.write_inventory(document)
        broker = SyntheticProducer(env.paths, load_templates(env.inventory_path), env.client_for_broker())
        broker.produced = {}
        env.brokers.append(broker)
        broker.start()
        records: list[dict[str, Any]] = []

        def submit(request: SubmissionRequest, wanted: State) -> dict[str, Any] | None:
            handle = broker.submit(request)
            env.wait_state(broker, handle, (wanted,))
            forward = broker.read_forward(request)
            require(forward is not None, "broker did not persist a launcher submission")
            attempt = broker.attempt_of(request, forward or {})
            require(attempt["phase"] == "ended" and attempt["caller_identity"] == request.identity(), "launcher lost caller identity")
            require(attempt["template"] == "fixture_dbt", "the publish fixture row did not select the fixture_dbt template")
            lines = broker.lines_of(request)
            require("input_pins" not in lines[0], "a publish fixture intent froze fixture input pins")
            require(
                lines[0]["lineage"] == document["templates"][request.template]["modes"][request.mode]["launcher"]["lineage"], "intent lost declared lineage"
            )
            if lines[-1]["event"] == "failed_closed":
                require(not (broker.paths.records / request.run_id / f"{request.container_name}.json").exists(), "rejected snapshot wrote a finished record")
                return None
            record = json.loads(Path(broker.record(handle)["record_path"]).read_text())
            records.append(record)
            return record

        silver = submit(SubmissionRequest("silver_build", RUN, MODE, 1, "silver_transform_dag", "silver_build", "manual__silver"), State.SUCCEEDED)
        env.daemon.script = lambda container: (0.2, 0 if "hai.probe" in container["Config"]["Labels"] else 1)
        failed = submit(SubmissionRequest("silver_build", RUN, MODE, 2, "silver_transform_dag", "silver_build", "manual__silver"), State.FAILED)
        require(
            build_events(failed or {})[-1]["eventType"] == "FAIL" and not build_events(failed or {})[-1]["outputs"],
            "failed launcher task asserted a produced output",
        )
        release_finished_run(broker, RUN, run_record_file(env, RUN))
        env.daemon.script = lambda container: (0.2, 0)
        gold_request = SubmissionRequest("gold_build", "20261017T020000Z-abcdef12", MODE, 1, "gold_transform_dag", "gold_build", "manual__gold")
        final_run = (broker, gold_request.run_id)
        gold_record = submit(gold_request, State.SUCCEEDED)
        require(silver is not None and gold_record is not None, "successful DAG record missing")
        silver_events, gold_events = build_events(silver or {}), build_events(gold_record or {})
        require(silver_events[-1]["outputs"] == gold_events[-1]["inputs"], "two DAGs did not share the declared dataset")
        require(silver_events[0]["run"]["runId"] != gold_events[0]["run"]["runId"], "two DAGs shared a run identity")
        snapshot_id = "4096000000000001"
        for attempt_number, mutation in enumerate((None, "undeclared", "malformed", "attempt"), start=2):
            request = SubmissionRequest("gold_build", gold_request.run_id, MODE, attempt_number, "gold_transform_dag", "gold_build", "manual__gold")
            snapshot: dict[str, Any] = {
                "version": 1,
                "invocation": request.as_dict(),
                "outputs": [{**gold, "snapshot": {"kind": "iceberg_snapshot", "id": snapshot_id}}],
            }
            if mutation == "undeclared":
                snapshot["outputs"][0]["name"] = "gold.undeclared"
            elif mutation == "malformed":
                snapshot["outputs"][0]["snapshot"]["id"] = "bad_hash"
            elif mutation == "attempt":
                snapshot["invocation"]["attempt"] = 99
            broker.produced[attempt_number] = snapshot
            record = submit(request, State.SUCCEEDED if mutation is None else State.FAILED_CLOSED)
            if mutation is None:
                require(build_events(record or {})[-1]["outputs"][0]["facets"]["version"]["datasetVersion"] == snapshot_id, "snapshot version facet missing")
        events = [event for record in records for event in build_events(record)]
        database = output / "outbox.sqlite"
        first, second = sweep(broker.paths.records, database), sweep(broker.paths.records, database)
        require(first["queued"] == len(events) and second["queued"] == 0, "sweep duplicated or lost launcher records")
        with Outbox(database) as box:
            before = box.pending(100)
            outage = box.deliver("127.0.0.1", 9, timeout=0.05, limit=100)
            require(outage["delivered"] == 0 and box.pending(100) == before, "outage altered persisted events")
        with Outbox(database) as box:
            require(box.pending(100) == before, "reopened outbox changed event bytes")
        result["transport"] = transport(events, database)
        sentinel_values = [*env.secret_values.values(), *broker._secrets]
        require(not any(value and value in canonical(events) for value in sentinel_values), "synthetic credentials leaked into lineage")
        (output / "events.json").write_text(json.dumps(events, indent=2) + "\n")
        if args.mutation != "final_release":
            release_finished_run(broker, gold_request.run_id, run_record_file(env, gold_request.run_id))
        client = env.client(start=False)
        final_state = client.call("status", {"what": "run", "id": gold_request.run_id}, client.principal_run(gold_request.run_id))["state"]
        require(final_state == "released", "the final test reservation remains held")
        result["final_reservation"] = final_state
        result.update(status="pass", completed_records=len(records), rejected_snapshots=3, events=len(events), two_dag_shared_dataset=True)
    except Exception as error:
        result["error"] = f"{type(error).__name__}: {error}"
    finally:
        if final_run is not None:
            try:
                final_broker, final_id = final_run
                client = env.client(start=False)
                final_state = client.call("status", {"what": "run", "id": final_id}, client.principal_run(final_id))["state"]
                if final_state != "released":
                    release_finished_run(final_broker, final_id, run_record_file(env, final_id))
                result["known_final_run_cleanup"] = "released"
            except (OSError, ValueError, RuntimeError, launcher.ClientError, AssertionError) as error:
                result["known_final_run_cleanup"] = f"incomplete: {type(error).__name__}"
                result["status"] = "stop"
        result["leftovers"] = env.close()
        if result["leftovers"]:
            result["status"] = "stop"
    (output / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    sys.stdout.write(json.dumps({"status": result["status"], "report": str(output / "report.json"), "error": result.get("error")}) + "\n")
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
