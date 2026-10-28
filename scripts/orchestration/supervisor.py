"""The host supervisor of the orchestration session: a client of the launcher that starts the stack, supplies the broker and owns teardown
(Airflow plan sections 2 and 8; D1 section 5 item 6; failure modes 925, 933 and 934).

Run from the main checkout with the launcher's Docker running:

    .venv/bin/python -m scripts.orchestration.supervisor up       # one foreground session; Ctrl-C tears it down
    .venv/bin/python -m scripts.orchestration.supervisor down     # teardown after a session that did not end cleanly
    .venv/bin/python -m scripts.orchestration.supervisor status   # the stack's record from the launcher; never a secret

``up`` pulls PostgreSQL by its digest through the launcher when it is absent and requires the Airflow image that
``services/orchestration/Dockerfile`` builds. It writes this session's broker token (32 random bytes, owner-only, never an environment variable)
under the ignored ``data/orchestration/airflow/`` and stores the metadata secrets in AWS Parameter Store (the database password and the Fernet key once,
the JSON Web Token secret and the API secret key anew each session), starts the broker and its loopback listener, then asks the launcher for
``orchestration_up``: the launcher sizes every service from the memory free at that moment, starts PostgreSQL, runs the migration as a finite
service under the deadline entrypoint and starts the four Airflow services, each after the one before is healthy. The supervisor then waits until
the API reports every component healthy. Any failure on the way asks the launcher for ``orchestration_down`` and removes the session token. The
supervisor makes no Docker call and never stops the catalog or restarts Docker. ``down`` and the teardown at the end of ``up`` return 3 until the
launcher confirms that the stack is down: a failed request or wait, or any other phase, is listed as an error. Tests pass
``--test-registry <folder>/registry`` with their own ports; the session E2Es are ``run_supervisor_e2e`` (fake daemon) and ``run_real_e2e`` (real Docker).
"""

from __future__ import annotations

import argparse
import http.client
import json
import secrets
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any

from orchestration import launcher
from orchestration.interface import SubmissionRefused, SubmissionRequest
from orchestration.lineage import CompletionWorker
from scripts.infrastructure import secret_store
from scripts.lakehouse import run_lock
from scripts.orchestration import run_control
from scripts.orchestration.airflow_api import AirflowApi, AirflowApiError
from scripts.orchestration.broker import (
    PLATFORM_FILE,
    TEMPLATES_FILE,
    Broker,
    BrokerError,
    BrokerPaths,
    LauncherUnavailable,
    Listener,
    load_templates,
    resolve_paths,
    write_private,
)

ROOT = Path(__file__).resolve().parents[2]
COMPONENTS = ("metadatabase", "scheduler", "triggerer", "dag_processor")
PULL_SECONDS = 3600.0
TRANSFORM_DAG = "transform_dag"
PIPELINE_DAGS = ("silver_transform_dag", "gold_transform_dag", "gold_maintenance_dag", "training_dag", "model_promotion_dag")
RECONCILE_SECONDS = 60.0
RECONCILE_ATTEMPT = 99
LIVE_RUN_STATES = ("queued", "running")
GATE_TASK = "final_gate"
ENDED_TASK_STATES = ("failed", "upstream_failed", "skipped", "removed")
SNAPSHOT_SECONDS = 20.0
SNAPSHOT_TOTAL_SECONDS = 60.0


class SupervisorError(Exception):
    """The session cannot start or stop safely; the message names the step."""


class Supervisor:
    """One orchestration session: the broker and its listener, and the launcher's orchestration stack."""

    def __init__(
        self,
        paths: BrokerPaths,
        platform: dict[str, Any],
        client: launcher.LauncherClient,
        *,
        api_port: int | None = None,
        broker_port: int | None = None,
        readiness_seconds: int = 420,
        fail_at: str | None = None,
        templates_file: Path = TEMPLATES_FILE,
        repo_root: Path = ROOT,
        lineage_api_port: int | None = None,
    ) -> None:
        """Other ports or a failure point only with a test registry."""
        if paths.registry.kind != "test" and any(value is not None for value in (api_port, broker_port, fail_at)):
            raise SupervisorError("other ports and failure points are test-only")
        self.paths = paths
        self.platform = platform
        self.client = client
        self.api_port = api_port or int(platform["listeners"]["airflow_api"]["port"])
        self.broker_host = str(platform["listeners"]["broker"]["host"])
        self.broker_port = broker_port if broker_port is not None else int(platform["listeners"]["broker"]["port"])
        self.readiness_seconds = readiness_seconds
        self.fail_at = fail_at
        self.templates_file = templates_file
        self.repo_root = repo_root
        self.lineage_api_port = lineage_api_port
        self.transform_dag = TRANSFORM_DAG
        self.state = paths.state / "airflow"
        self.jwt_secret = ""
        self.broker: Broker | None = None
        self.listener: Listener | None = None
        self.token = ""
        self.evidence: dict[str, Any] = {}
        self.emitter: CompletionWorker | None = None

    def call(self, method: str, params: dict[str, Any] | None = None, timeout: float = 120.0) -> Any:
        """One launcher request; a refusal or an outage is a ``SupervisorError`` naming the request."""
        try:
            return self.client.call(method, params, timeout=timeout)
        except launcher.ClientError as error:
            raise SupervisorError(f"the launcher refused {method}: {error} [{error.code}]") from None
        except (OSError, TimeoutError) as error:
            raise SupervisorError(f"the launcher did not answer {method}: {type(error).__name__}") from None

    def stack(self) -> dict[str, Any]:
        """The launcher's record of the stack."""
        record: dict[str, Any] = self.call("status", {"what": "orchestration"})
        return record

    def wait_for_stack(self, phases: tuple[str, ...], seconds: float) -> dict[str, Any]:
        """Wait until the stack record reaches one of the phases or the time ends; a launcher refusal or outage is a ``SupervisorError``, as in ``call``."""
        try:
            return self.client.wait_for("orchestration", "", "phase", phases, {"kind": "any"}, seconds)
        except launcher.ClientError as error:
            raise SupervisorError(f"the launcher refused the wait for the stack: {error} [{error.code}]") from None
        except (OSError, TimeoutError) as error:
            raise SupervisorError(f"the launcher did not answer the wait for the stack: {type(error).__name__}") from None

    def check_images(self) -> dict[str, Any]:
        """PostgreSQL by digest (pulled through the launcher when absent) and the locally built Airflow image."""
        pulled = self.call("pull", {"reference": self.platform["metadata_database"]["image"]}, timeout=PULL_SECONDS)
        try:
            airflow = self.client.call("image", {"service": "airflow-check", "compose": "orchestration", "checkout": str(self.repo_root)})
        except launcher.ClientError as error:
            raise SupervisorError(
                f"the Airflow image {self.platform['airflow']['image']} is not on this machine: build it once with the command in "
                f"{self.platform['airflow']['dockerfile']} ({error})"
            ) from None
        labels = (airflow["inspect"].get("Config") or {}).get("Labels") or {}
        if any(labels.get(name) != value for name, value in self.platform["airflow"]["labels"].items()):
            raise SupervisorError("the Airflow image does not carry the deadline entrypoint labels")
        return {"postgres": pulled, "airflow": {"image": self.platform["airflow"]["image"], "id": airflow["id"]}}

    def store_metadata_secrets(self) -> None:
        """Keep the database password and the Fernet key in Parameter Store (generated there once) and store this session's rotated secrets.

        A database that already holds data keeps working only with the password it was created with, so a missing secret next to an initialized database
        is refused instead of generated."""
        try:
            store = self.client.config.secret_store()
            initialized = (self.state / "postgres" / "PG_VERSION").exists()
            for name, make in ((secret_store.AIRFLOW_DB_PASSWORD, secret_store.new_password), (secret_store.AIRFLOW_FERNET_KEY, secret_store.new_fernet_key)):
                if initialized and store.find(name) is None:
                    raise SupervisorError(f"the Airflow database exists but secret {name} is missing; run the one-time secrets move first")
                value, _ = store.ensure(name, make)
                if name == secret_store.AIRFLOW_FERNET_KEY and not secret_store.is_fernet_key(value):
                    raise SupervisorError(f"secret {name} is not a Fernet key (the URL-safe base64 text of 32 bytes); Airflow could not decrypt with it")
            self.jwt_secret = secrets.token_urlsafe(48)
            store.put(secret_store.AIRFLOW_JWT_SECRET, self.jwt_secret)
            store.put(secret_store.AIRFLOW_API_SECRET_KEY, secrets.token_urlsafe(48))
        except (launcher.LauncherError, secret_store.SecretStoreError) as error:
            raise SupervisorError(str(error)) from None

    def prepare_state(self, token: str, broker_port: int) -> None:
        """Folders, the metadata secrets in Parameter Store and this session's token and broker URL."""
        for folder in ("postgres", "logs", "session", "lineage_provider"):
            (self.state / folder).mkdir(parents=True, exist_ok=True, mode=0o700)
        self.store_metadata_secrets()
        write_private(self.state / "session" / "broker_token", token + "\n")
        write_private(self.state / "session" / "client.json", json.dumps({"url": f"http://host.docker.internal:{broker_port}"}) + "\n")

    def start(self) -> dict[str, Any]:
        """Start the whole session or leave nothing of it running."""
        current = self.stack()
        if current.get("phase") != "down":
            raise SupervisorError(f"the orchestration stack is {current.get('phase')}; run down first")
        self.evidence["images"] = self.check_images()
        self.token = secrets.token_bytes(32).hex()
        try:
            self.broker = Broker(self.paths, load_templates(self.templates_file), self.client, repo_root=self.repo_root, dag_snapshot=self.dag_snapshot)
            self.evidence["reconcile"] = self.broker.start()
            self.listener = Listener(self.broker, self.broker_host, self.broker_port, self.token)
            self.listener.start()
            self.prepare_state(self.token, self.listener.port)
            self.call("orchestration_up", {"state": str(self.state), "api_port": self.api_port, "checkout": str(self.repo_root)})
            started = time.monotonic()
            record = self.wait_for_stack(("up", "down"), self.readiness_seconds)
            self.evidence["start"] = {"seconds": round(time.monotonic() - started, 1), "phase": record.get("phase")}
            if record.get("phase") != "up":
                raise SupervisorError(
                    f"the launcher could not start the stack: {record.get('error') or 'no result within ' + str(self.readiness_seconds) + ' s'}"
                )
            self.evidence["settings"] = {name: value for name, value in record.get("settings", {}).items() if name.startswith(("HAI_ORCH_MEM", "HAI_ORCH_PAR"))}
            if self.fail_at == "after_airflow_start":
                raise SupervisorError("test failure point after_airflow_start")
            self.evidence["health"] = self.ready()
            self.start_lineage()
        except BaseException:
            outcome = self.teardown()
            if outcome.get("errors"):
                sys.stderr.write("supervisor: the cleanup after the failed start did not finish:\n" + "".join(f"  {item}\n" for item in outcome["errors"]))
            raise
        return self.evidence

    def start_lineage(self) -> None:
        """Schedule independent durable collection; lineage failures leave the orchestration session available."""
        try:
            settings = json.loads((self.repo_root / "services/lineage/delivery.json").read_text())
            if set(settings) != {"api_port", "interval_seconds", "pass_seconds", "batch_limit"}:
                raise ValueError("lineage delivery settings contain unknown or missing fields")
            port = self.lineage_api_port
            if port is None and self.paths.registry.kind != "test":
                port = settings["api_port"]
            worker = CompletionWorker(
                self.paths.records,
                self.state / "lineage_provider",
                self.paths.state / "lineage" / "outbox.sqlite",
                port=port,
                interval_seconds=settings["interval_seconds"],
                pass_seconds=settings["pass_seconds"],
                limit=settings["batch_limit"],
            )
            worker.start()
            self.emitter = worker
            self.evidence["lineage"] = {"scheduled": True, "delivery": port is not None}
        except (OSError, ValueError, TypeError, KeyError, RuntimeError) as error:
            self.evidence["lineage"] = {"scheduled": False, "error": type(error).__name__}
            sys.stderr.write(f"supervisor: lineage collection was not scheduled ({type(error).__name__})\n")

    def ready(self) -> dict[str, Any]:
        """Poll the API's health endpoint until every component reports healthy."""
        deadline = time.monotonic() + self.readiness_seconds
        last: dict[str, Any] = {}
        while time.monotonic() < deadline:
            connection = http.client.HTTPConnection("127.0.0.1", self.api_port, timeout=10)
            try:
                connection.request("GET", "/api/v2/monitor/health")
                response = connection.getresponse()
                last = json.loads(response.read()) if response.status == 200 else {"status": response.status}
            except (OSError, http.client.HTTPException, ValueError) as exc:
                last = {"error": type(exc).__name__}
            finally:
                connection.close()
            if all((last.get(component) or {}).get("status") == "healthy" for component in COMPONENTS):
                return {component: last[component]["status"] for component in COMPONENTS}
            time.sleep(3)
        raise SupervisorError(f"Airflow not healthy after {self.readiness_seconds} s: {json.dumps(last)[:300]}")

    def dag_snapshot(self, run_id: str, mode: str) -> dict[str, Any]:
        """One read of the transform DAG run that names ``run_id`` and ``mode`` in its configuration: its ID, its state and the state of each of its tasks.
        ``{"unavailable": reason}`` when no DAG run or more than one names them or the API cannot answer, so no single DAG run is read. The whole read ends
        within ``SNAPSHOT_TOTAL_SECONDS``, because the broker calls it while it holds the run's ledger lock."""
        deadline = time.monotonic() + SNAPSHOT_TOTAL_SECONDS
        try:
            if not self.jwt_secret:
                return {"unavailable": "this session has not stored its JSON Web Token secret yet"}
            api = AirflowApi(self.api_port, self.jwt_secret, timeout=SNAPSHOT_SECONDS, deadline=deadline)
            names = (
                PIPELINE_DAGS if self.transform_dag == TRANSFORM_DAG and (self.repo_root / "dags/silver_transform_dag.py").is_file() else (self.transform_dag,)
            )
            runs = [
                (name, item)
                for name in names
                for item in api.runs(name)
                if (item.get("conf") or {}).get("run_id") == run_id
                and (
                    (item.get("conf") or {}).get("mode") == mode
                    or name == "model_promotion_dag"
                    and mode == "real"
                    and (item.get("conf") or {}).get("mode") in ("promote", "rollback")
                )
            ]
            if len(runs) != 1:
                return {"unavailable": f"{len(runs)} pipeline DAG runs name run {run_id} in mode {mode}, not one"}
            dag_id, selected = runs[0]
            dag_run_id = str(selected["dag_run_id"])
            tasks = api.task_states(dag_id, dag_run_id)
            if time.monotonic() >= deadline:
                raise AirflowApiError("the deadline of the read passed")
        except (AirflowApiError, OSError, KeyError, ValueError) as error:
            return {"unavailable": f"the DAG run cannot be read: {type(error).__name__}: {str(error)[:200]}"}
        return {"dag_id": dag_id, "dag_run_id": dag_run_id, "dag_run_state": str(selected["state"]), "tasks": tasks}

    def reconcile_runs(self) -> dict[str, Any]:
        """Move the transform run to ``failed_retained``, through the broker's ``release_rejected`` control row, when its DAG run has ended without the gate
        having accepted it.

        The finalization tasks do not run when a DAG run is marked failed or cancelled by hand, so the lock and any open attempt would outlive the run.
        A run is reconciled only when it is held or finalizing under this broker, exactly one DAG run of the transform DAG names
        it and its mode and that DAG run is not queued or running. A finalizing run is an accepted finalization, which the release task resumes, only when
        its record holds an accepted gate result and the gate task of that DAG run succeeded: it is then left alone. A record that cannot be read, a gate
        task whose state is not settled and a DAG run that cannot be read also leave the run alone. The lock is never released here: only the owner's
        release frees a retained run."""
        outcome: dict[str, Any] = {"reconciled": [], "left": {}}
        if self.broker is None:
            raise SupervisorError("the session has no broker")
        try:
            held = self.call("status", {"what": "registry"})["domains"].get("transform")
        except KeyError:
            outcome["left"]["registry"] = "the registry answer has no domains"
            return outcome
        if not held or held["state"] not in ("held", "finalizing") or held["holder"] != "broker":
            return outcome
        run_id, mode = str(held["run_id"]), str(held["mode"])
        snapshot = self.dag_snapshot(run_id, mode)
        if "unavailable" in snapshot:
            outcome["left"][run_id] = snapshot["unavailable"]
        elif snapshot["dag_run_state"] in LIVE_RUN_STATES:
            outcome["left"][run_id] = "its DAG run is still live"
        elif held["state"] == "finalizing" and self.release_pending(run_id, mode, snapshot["tasks"], outcome):
            pass
        else:
            dag_id = str(snapshot["dag_id"])
            training = None
            if dag_id in ("training_dag", "model_promotion_dag"):
                admitted = [
                    SubmissionRequest.from_record(lines[0])
                    for lines in self.broker.ledger.submissions(run_id).values()
                    if lines[0].get("dag_id") == dag_id and lines[0].get("training") is not None
                ]
                if not admitted or any(item.training != admitted[0].training for item in admitted):
                    outcome["left"][run_id] = "training caller parameters cannot be reconciled from the admitted ledger"
                    return outcome
                training = admitted[0].training
            handle = self.broker.submit(
                SubmissionRequest(
                    "release_rejected", run_id, mode, RECONCILE_ATTEMPT, dag_id, "supervisor_reconcile", snapshot["dag_run_id"], training=training
                )
            )
            outcome["reconciled"].append({"run_id": run_id, "submission_id": handle.submission_id, "dag_run_states": [snapshot["dag_run_state"]]})
        return outcome

    def release_pending(self, run_id: str, mode: str, tasks: dict[str, str | None], outcome: dict[str, Any]) -> bool:
        """True, with the reason noted in ``outcome``, when the run's finalization is left for its release task: the record holds an accepted gate result and
        the gate task succeeded, the record cannot be read or the gate task has not settled."""
        if self.broker is None:
            raise SupervisorError("the session has no broker")
        try:
            result = run_control.recorded_result(self.broker, run_id, mode)
        except (OSError, run_control.ControlRefused) as error:
            outcome["left"][run_id] = f"its run record cannot be read: {type(error).__name__}"
            return True
        if result != "accepted":
            return False
        gate_state = tasks.get(GATE_TASK)
        if gate_state == "success":
            outcome["left"][run_id] = "its finalization was accepted and its gate task succeeded; the release task resumes it"
            return True
        if gate_state in ENDED_TASK_STATES:
            return False
        outcome["left"][run_id] = f"its record is accepted and its gate task is {gate_state}: the gate's outcome is not settled"
        return True

    def failure(self, step: str, error: BaseException) -> str:
        """One teardown failure as a line: the step, the exception type and its text without this session's token."""
        text = f"{type(error).__name__}: {error}"
        return f"{step}: {(text.replace(self.token, '[redacted]') if self.token else text)[:300]}"

    def teardown(self) -> dict[str, Any]:
        """Stop the listener and the broker, delete the session files and ask the launcher to take the stack down.

        Every step runs even when an earlier one failed, so a failure never leaves the stack up or the token on disk. The failures are listed under
        ``errors``; a listener or broker that failed to stop is kept, so a second teardown tries it again. The stack is one of the steps: a request or a
        wait that fails and any phase other than ``down`` are errors too, so the result is clean only when the launcher confirms the stack is down."""
        result: dict[str, Any] = {}
        errors: list[str] = []
        if self.emitter is not None:
            try:
                self.emitter.stop()
                result["lineage"] = {"cleanup": "done", "last_pass": self.emitter.outcome, "failures": self.emitter.failures}
                self.emitter = None
            except TimeoutError as error:
                errors.append(self.failure("lineage", error))
        if self.listener is not None:
            try:
                self.listener.stop()
                self.listener = None
            except Exception as error:
                errors.append(self.failure("listener", error))
        if self.broker is not None:
            try:
                self.broker.close()
                self.broker = None
            except Exception as error:
                errors.append(self.failure("broker", error))
        session = self.state / "session"
        for name in ("broker_token", "client.json"):
            try:
                (session / name).unlink(missing_ok=True)
            except OSError as error:
                errors.append(self.failure(f"session file {name}", error))
        try:
            self.call("orchestration_down")
            record = self.wait_for_stack(("down",), 600)
            result["stack"] = {"phase": record.get("phase"), "error": record.get("error", "")}
            if record.get("phase") != "down":
                errors.append(f"stack: the launcher reports phase {record.get('phase')}, not down; run down again")
        except SupervisorError as error:
            result["stack"] = {"phase": "unknown", "error": str(error)}
            errors.append(self.failure("stack", error))
        if errors:
            result["errors"] = errors
        return result


def build(test_registry: str | None, lineage_api_port: int | None = None) -> Supervisor:
    """A supervisor for the real registry or a test one."""
    paths = resolve_paths(test_registry)
    platform: dict[str, Any] = json.loads(PLATFORM_FILE.read_text())
    return Supervisor(paths, platform, launcher.load_client(test_registry), lineage_api_port=lineage_api_port)


def reconcile(supervisor: Supervisor) -> None:
    """One reconciliation pass of the running session; a failure is reported on standard error and the next pass tries again."""
    try:
        outcome = supervisor.reconcile_runs()
    except (SupervisorError, SubmissionRefused, LauncherUnavailable) as error:
        sys.stderr.write(f"supervisor: the run reconciliation did not finish: {error}\n")
        return
    if outcome["reconciled"]:
        sys.stderr.write(f"supervisor: retained {len(outcome['reconciled'])} run(s) whose DAG run ended: {json.dumps(outcome['reconciled'])}\n")


def main() -> int:
    """Run one command; ``up`` holds the session in the foreground until SIGINT or SIGTERM."""
    cli = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    cli.add_argument("--test-registry", help="a test registry folder named registry; its parent holds the test state")
    cli.add_argument("--lineage-api-port", type=int, help="the existing loopback Marquez API port; test sessions otherwise collect without HTTP")
    commands = cli.add_subparsers(dest="command", required=True)
    commands.add_parser("up", help="start the session and hold it until Ctrl-C")
    commands.add_parser("down", help="tear down the stack and delete the session token")
    commands.add_parser("status", help="print the launcher's record of the stack")
    args = cli.parse_args()
    try:
        supervisor = build(args.test_registry, args.lineage_api_port)
        if args.command == "status":
            result: Any = supervisor.stack()
        elif args.command == "down":
            result = supervisor.teardown()
        else:
            stop = threading.Event()
            for signum in (signal.SIGINT, signal.SIGTERM):
                signal.signal(signum, lambda *_: stop.set())
            evidence = supervisor.start()
            sys.stdout.write(json.dumps({"ready": True, "api": f"http://127.0.0.1:{supervisor.api_port}", **evidence}, indent=2, default=str) + "\n")
            sys.stdout.flush()
            try:
                while not stop.wait(RECONCILE_SECONDS):
                    reconcile(supervisor)
            finally:
                result = supervisor.teardown()
    except (SupervisorError, BrokerError, LauncherUnavailable, run_lock.RegistryError) as exc:
        sys.stderr.write(f"supervisor: {exc}\n")
        return 3
    sys.stdout.write(json.dumps(result, indent=2, default=str) + "\n")
    if isinstance(result, dict) and result.get("errors"):
        sys.stderr.write("supervisor: teardown finished with errors; run down again\n" + "".join(f"  {item}\n" for item in result["errors"]))
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
