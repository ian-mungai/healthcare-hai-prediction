"""The Airflow REST API of one pipeline run, in memory, and a loopback server that serves it to the DAG runtime's own signed requests.

``Cluster`` stands in for the four Airflow 3 routes that ``orchestration/dag_runtime.py`` calls (create a DAG run, read a DAG run, read a DAG, list the task
instances of a run). It keeps each run's state, moves it from queued to running to its end over fake clock ticks, never starts a run of a paused DAG and, when
a run succeeds, creates the runs that the shipped DAGs create after it (the features base to the six model features runs, a model features run to its drift
run, a drift run to its training run when its gate trains, a promotion model's training run to its research run). ``ApiServer`` serves one ``Cluster`` on the
loopback port that the runtime reads from ``AIRFLOW_API_PORT`` and refuses a request whose bearer token is not an HS512 token signed with the session's JWT
secret for the Airflow audience, so the runtime's real request code runs. The offline cases (``pipeline_cases.py``) and the real-image DAG runs
(``real_image_dags.py``) both use it. The real API server, the scheduler and the host pool are not run.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from orchestration import training_contract as contract

AUDIENCE = "apache-airflow"
RUNAWAY_SLEEPS = 20000  # the longest wait of the first DAG is 30 hours of 60-second polls (1800): a wait past this has no end
MODEL_OF_DAG = dict(contract.FEATURE_DAGS.items())
DAG_OF_MODEL = {model: dag for dag, model in MODEL_OF_DAG.items()}


class Cluster:
    """The in-memory Airflow API and the DAG runs behind it, on a fake clock. ``ticks`` is how many polls a run takes from its creation to its end."""

    def __init__(self, ticks: int = 2) -> None:
        self.runs: dict[tuple[str, str], dict[str, Any]] = {}
        self.paused: set[str] = set()
        self.unregistered: set[str] = set()
        self.failing: set[tuple[str, str]] = set()
        self.slow: dict[tuple[str, str], int] = {}
        self.no_training: set[str] = set()
        self.silent: set[tuple[str, str, str]] = set()  # task instances that the listing of a run leaves out
        self.task_states: dict[tuple[str, str, str], list[str]] = {}
        self.unmapped: set[tuple[str, str, str]] = set()  # trigger tasks that mapped no request: Airflow keeps one skipped instance with map index -1
        self.log: list[tuple[Any, ...]] = []
        self.calls: list[tuple[str, str]] = []
        self.ticks, self.now, self.sleeps = ticks, 0.0, 0
        self.on_create: Callable[[str, str], None] | None = None  # told of every run the moment it is created, by the API or by a DAG

    # The clock the DAG runtime reads, and the wait it asks for.
    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        if self.sleeps >= RUNAWAY_SLEEPS:
            raise RuntimeError(f"a wait of {RUNAWAY_SLEEPS} polls: the code under test does not end its wait")
        self.sleeps += 1
        self.now += seconds
        for key in sorted(self.runs):
            self.advance(key)

    def advance(self, key: tuple[str, str]) -> None:
        run = self.runs[key]
        if run["state"] in ("success", "failed") or key[0] in self.paused:
            return
        run["state"] = "running"
        run["left"] -= 1
        if run["left"] > 0:
            return
        run["state"] = "failed" if key in self.failing else "success"
        self.log.append(("end", key[0], key[1], run["state"], self.sleeps))
        if run["state"] == "success":
            self.spawn(*key)

    def create(self, dag: str, run: str, by: str, conf: dict[str, Any] | None = None) -> bool:
        if (dag, run) in self.runs:
            return False
        self.runs[(dag, run)] = {"state": "queued", "left": self.slow.get((dag, run), self.ticks), "conf": conf}
        self.log.append(("create", dag, run, by, self.sleeps))
        if self.on_create is not None:
            self.on_create(dag, run)
        return True

    def spawn(self, dag: str, run: str) -> None:
        """The runs that the shipped DAGs create once a run succeeded, with the settings the DAG before gives them (``training_contract.route_run_conf``)."""
        if dag == "features_base_dag":
            for model in contract.MODELS:
                conf, _ = contract.route_run_conf("features", run, model)
                self.create(DAG_OF_MODEL[model], conf["run_id"], "dag", conf)
        elif dag in MODEL_OF_DAG:
            conf, _ = contract.route_run_conf("drift", contract.base_run_of(run), MODEL_OF_DAG[dag])
            self.create("drift_monitoring_dag", conf["run_id"], "dag", conf)
        elif dag == "drift_monitoring_dag":
            parts = contract.run_id_parts(run)
            name = next(item for item, value in contract.MODEL_NAMES.items() if value == parts.qualifier)
            if name in self.no_training:
                self.skip_unmapped(dag, run, "trigger_training")
                return
            conf, _ = contract.route_run_conf("training", contract.compose_run_id("features", parts.hex, parts.attempt), name)
            self.trigger(dag, run, "trigger_training", "training_dag", conf["run_id"], {**conf, "hai_types": list(contract.HAI_TYPES)})
        elif dag == "training_dag":
            parts = contract.run_id_parts(run)
            name = next(item for item, value in contract.MODEL_NAMES.items() if value == parts.qualifier)
            if name in contract.PROMOTION_MODELS:
                base = contract.compose_run_id("features", parts.hex, parts.attempt)
                (request,) = contract.research_requests(name, run, base, "primary", contract.HAI_TYPES, "aws")
                self.trigger(dag, run, "trigger_research", "research_dag", request["trigger_run_id"], request["conf"])
            else:
                self.skip_unmapped(dag, run, "trigger_research")

    def trigger(self, dag: str, run: str, task: str, child_dag: str, child_run: str, conf: dict[str, Any]) -> None:
        """A mapped ``TriggerDagRunOperator`` (``skip_when_already_exists``) with one request: the instance succeeds when it created the child run and ends
        skipped when the child run exists already, as the Airflow task runner ends it for ``DAGRUN_ALREADY_EXISTS``."""
        created = self.create(child_dag, child_run, "dag", conf)
        self.task_states[(dag, run, task)] = ["success" if created else "skipped"]

    def skip_unmapped(self, dag: str, run: str, task: str) -> None:
        """A trigger task whose request list is empty: one skipped instance with map index -1, and no child run."""
        self.task_states[(dag, run, task)] = ["skipped"]
        self.unmapped.add((dag, run, task))

    # The four routes.
    def __call__(self, method: str, path: str, body: dict[str, Any] | None = None) -> tuple[int, Any]:
        self.calls.append((method, path))
        route = re.fullmatch(r"/api/v2/dags/([A-Za-z0-9_]+)(?:/dagRuns(?:/([A-Za-z0-9_]+)(?:/(taskInstances))?)?)?(?:\?.*)?", path)
        if route is None:
            return 404, {}
        dag, run, listing = route.groups()
        if dag in self.unregistered:
            return 404, {}
        if run is None and "/dagRuns" not in path:
            return (200, {"dag_id": dag, "is_paused": dag in self.paused}) if method == "GET" else (405, {})
        if run is None:
            if method != "POST" or body is None:
                return 405, {}
            created = self.create(dag, str(body["dag_run_id"]), "api", body["conf"])
            return (200, {"dag_run_id": body["dag_run_id"], "state": "queued"}) if created else (409, {})
        if (dag, run) not in self.runs:
            return 404, {}
        if listing:
            rows = [
                {"task_id": task, "state": state, "map_index": -1 if (d, r, task) in self.unmapped else index - 1}
                for (d, r, task), states in self.task_states.items()
                if (d, r) == (dag, run) and (d, r, task) not in self.silent
                for index, state in enumerate(states, 1)
            ]
            return 200, {"task_instances": rows, "total_entries": len(rows)}
        return 200, {"dag_id": dag, "dag_run_id": run, "state": self.runs[(dag, run)]["state"], "conf": self.runs[(dag, run)]["conf"]}

    def created_by(self, by: str) -> list[tuple[str, str]]:
        return [(item[1], item[2]) for item in self.log if item[0] == "create" and item[3] == by]


def token_problem(header: str, secret: str, now: float) -> str | None:
    """Why a ``Authorization`` header is not a bearer token signed with ``secret`` for the Airflow audience and valid at ``now``, or None."""
    scheme, _, token = header.partition(" ")
    parts = token.split(".")
    if scheme != "Bearer" or len(parts) != 3:
        return "no bearer token"
    head, claims, signature = parts
    expected = base64.urlsafe_b64encode(hmac.new(secret.encode(), f"{head}.{claims}".encode(), hashlib.sha512).digest()).rstrip(b"=").decode()
    if not hmac.compare_digest(signature, expected):
        return "the token signature is not the session secret's"

    def decode(part: str) -> dict[str, Any]:
        return dict(json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))))

    try:
        algorithm, body = decode(head).get("alg"), decode(claims)
    except ValueError:
        return "the token is not JSON"
    if algorithm != "HS512" or body.get("aud") != AUDIENCE:
        return "the token is not an HS512 token for the Airflow audience"
    if not isinstance(body.get("exp"), int) or not isinstance(body.get("nbf"), int) or not body["nbf"] <= now <= body["exp"]:
        return "the token is not valid now"
    return None


class ApiServer:
    """A loopback server (one thread) that answers the requests of the DAG runtime from a ``Cluster``. A request with a bad token gets 401 and is not served.
    ``requests`` holds each request's method, path and status, in order."""

    def __init__(self, cluster: Cluster, secret: str) -> None:
        self.cluster, self.secret = cluster, secret
        self.requests: list[tuple[str, str, int]] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def handle_request(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length)
                problem = token_problem(self.headers.get("Authorization", ""), outer.secret, time.time())
                if problem is not None:
                    status, payload = 401, {"detail": problem}
                else:
                    status, payload = outer.cluster(self.command, self.path, json.loads(raw) if raw else None)
                outer.requests.append((self.command, self.path, status))
                data = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_POST = handle_request

            def log_message(self, *_: Any) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> ApiServer:
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    @property
    def port(self) -> int:
        return int(self.server.server_address[1])
