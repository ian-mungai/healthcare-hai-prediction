"""Cases for the bronze-triggered chain DAG: derived run IDs, trigger order and the guard against starting a retried bronze snapshot again.

The airflow packages are not installed here. Small stand-ins record the DAG, its tasks and each trigger, and Jinja renders the templated trigger fields
with the DAG's own macros, so the run IDs and settings a trigger would send are the ones the cases compare. Nothing leaves the machine.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import re
import runpy
import socket
import sys
import threading
import time
from collections.abc import Callable
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

import jinja2

from scripts.orchestration.ecs_adapter_cases import require
from scripts.orchestration.ecs_broker import CHAIN_LAYERS, derived_run_id, load_jobs

ROOT = Path(__file__).resolve().parents[2]
BRONZE = "20261030T090000Z-b0b0b0b0"
OTHER_BRONZE = "20261031T090000Z-b1b1b1b1"
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")


class Operator:
    """A task node that records its upstream and downstream links and the settings it was built with."""

    def __init__(self, task_id: str, **settings: Any) -> None:
        self.task_id, self.settings = task_id, settings
        self.upstream: list[Operator] = []
        self.function: Callable[..., Any] | None = None

    def __rshift__(self, other: Operator) -> Operator:
        other.upstream.append(self)
        return other


class Dag:
    def __init__(self, **settings: Any) -> None:
        self.settings = settings

    def __enter__(self) -> Dag:
        return self

    def __exit__(self, *_: object) -> None:
        return None


def load_dag() -> dict[str, Any]:
    """Run ``dags/bronze_chain_dag.py`` against the stand-ins and return its globals and the operators it built."""
    built: list[Operator] = []

    def task(**settings: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
            def call() -> SimpleNamespace:
                node = Operator(**settings)
                node.function = function
                built.append(node)
                return SimpleNamespace(operator=node)

            return call

        return decorate

    def trigger(**settings: Any) -> Operator:
        node = Operator(**settings)
        built.append(node)
        return node

    sdk = SimpleNamespace(
        Asset=str,
        TriggerRule=SimpleNamespace(ALL_SUCCESS="all_success", ALL_DONE="all_done"),
        DAG=Dag,
        Param=lambda *a, **k: (a, k),
        task=task,
        get_current_context=dict,
    )
    modules: dict[str, Any] = {
        "airflow": SimpleNamespace(),
        "airflow.sdk": sdk,
        "airflow.providers": SimpleNamespace(),
        "airflow.providers.standard": SimpleNamespace(),
        "airflow.providers.standard.operators": SimpleNamespace(),
        "airflow.providers.standard.operators.trigger_dagrun": SimpleNamespace(TriggerDagRunOperator=trigger),
    }
    previous = sys.modules.pop("orchestration.dag_runtime", None)
    try:
        with mock.patch.dict(sys.modules, modules):
            loaded = runpy.run_path(str(ROOT / "dags/bronze_chain_dag.py"))
    finally:
        sys.modules.pop("orchestration.dag_runtime", None)
        if previous is not None:
            sys.modules["orchestration.dag_runtime"] = previous
    return {**loaded, "operators": built}


def render(loaded: dict[str, Any], template: str, bronze: str) -> str:
    """A templated field as Airflow renders it: the DAG's macros and its ``params``."""
    environment = jinja2.Environment(undefined=jinja2.StrictUndefined, autoescape=True)
    return environment.from_string(template).render(params=SimpleNamespace(run_id=bronze, mode="real"), **loaded["dag"].settings["user_defined_macros"])


def derived_ids_are_valid_and_stable() -> None:
    loaded = load_dag()
    ids = [loaded["chain_run"](layer, BRONZE) for layer, _, _ in loaded["CHAIN"]]
    require(all(RUN_ID.fullmatch(run) for run in ids), f"a derived run ID has the wrong shape: {ids}")
    require(all(run.startswith(BRONZE[:16]) for run in ids), "a derived run ID lost the bronze time stamp")
    require(len(set(ids)) == 4 and BRONZE not in ids, "two layers share a run ID")
    require(ids == [loaded["chain_run"](layer, BRONZE) for layer, _, _ in loaded["CHAIN"]], "the run IDs are not stable")
    require(not set(ids) & {loaded["chain_run"](layer, OTHER_BRONZE) for layer, _, _ in loaded["CHAIN"]}, "two bronze runs share a layer run ID")
    require(loaded["chain_run"]("gold", BRONZE) == loaded["derived_run_id"](ids[0], "gold"), "gold does not derive from silver")


def broker_and_dag_derive_the_same_run_ids() -> None:
    loaded = load_dag()
    require(tuple(layer for layer, _, _ in loaded["CHAIN"]) == CHAIN_LAYERS, "the broker and the DAG list different layers")
    for bronze in (BRONZE, OTHER_BRONZE):
        run = bronze
        for layer in CHAIN_LAYERS:
            run = derived_run_id(run, layer)
            require(run == loaded["chain_run"](layer, bronze), f"the broker derives another {layer} run ID than the DAG")


def chain_runs_in_order_after_the_gate() -> None:
    loaded = load_dag()
    nodes = {node.task_id: node for node in loaded["operators"]}
    require(list(nodes) == ["chain_gate", "run_silver", "run_gold", "run_features", "run_drift", "start_metadata"], f"wrong tasks: {list(nodes)}")
    order = ["chain_gate", "run_silver", "run_gold", "run_features", "run_drift"]
    for before, after in zip(order, order[1:], strict=False):
        require(nodes[after].upstream == [nodes[before]], f"{after} does not follow only {before}")
    require(loaded["dag"].settings["schedule"] is None and loaded["dag"].settings["catchup"] is False, "the chain has a calendar schedule")
    require(
        [nodes[name].settings["trigger_dag_id"] for name in order[1:]]
        == ["silver_transform_dag", "gold_transform_dag", "features_transform_dag", "drift_monitoring_dag"],
        "wrong DAGs",
    )
    for name in order[1:]:
        settings = nodes[name].settings
        require(
            settings["wait_for_completion"] is True and settings["allowed_states"] == ["success"] and "failed" in settings["failed_states"],
            f"{name} does not wait for success",
        )
        require(settings["skip_when_already_exists"] is True and settings["reset_dag_run"] is False, f"{name} may restart an existing run")


def triggers_send_derived_ids() -> None:
    loaded = load_dag()
    nodes = {node.task_id: node.settings for node in loaded["operators"]}

    def run(layer: str) -> str:
        return str(loaded["chain_run"](layer, BRONZE))

    rendered = {name: render(loaded, nodes[name]["trigger_run_id"], BRONZE) for name in ("run_silver", "run_gold", "run_features", "run_drift")}
    require(rendered["run_gold"] == f"chain__gold_transform_dag__{run('gold')}", f"wrong DAG run ID for gold: {rendered['run_gold']}")
    conf = {name: {key: render(loaded, value, BRONZE) for key, value in nodes[name]["conf"].items()} for name in rendered}
    require(conf["run_silver"] == {"run_id": run("silver"), "mode": "real"}, f"silver settings: {conf['run_silver']}")
    require(conf["run_gold"] == {"run_id": run("gold"), "mode": "real", "silver_snapshot_run": run("silver")}, f"gold settings: {conf['run_gold']}")
    require(conf["run_features"] == {"run_id": run("features"), "mode": "real", "gold_snapshot_run": run("gold")}, f"features settings: {conf['run_features']}")
    require(conf["run_drift"] == {"run_id": run("drift"), "mode": "real", "mlflow": "local"}, f"drift settings: {conf['run_drift']}")


def a_retried_bronze_run_names_the_same_runs() -> None:
    loaded = load_dag()
    nodes = {node.task_id: node.settings for node in loaded["operators"]}
    first = {name: render(loaded, nodes[name]["trigger_run_id"], BRONZE) for name in nodes if name.startswith("run_")}
    again = {name: render(loaded, nodes[name]["trigger_run_id"], BRONZE) for name in nodes if name.startswith("run_")}
    other = {name: render(loaded, nodes[name]["trigger_run_id"], OTHER_BRONZE) for name in nodes if name.startswith("run_")}
    require(first == again, "a second trigger for one bronze run names new DAG runs")
    require(not set(first.values()) & set(other.values()), "a new bronze run reuses an old DAG run ID")


def waits_have_explicit_deadlines() -> None:
    loaded = load_dag()
    triggers = [node.settings for node in loaded["operators"] if node.task_id.startswith("run_")]
    limits = [settings.get("execution_timeout") for settings in triggers]
    require(len(triggers) == 4 and all(isinstance(limit, timedelta) and limit > timedelta(0) for limit in limits), f"a layer wait has no deadline: {limits}")
    run_limit = loaded["dag"].settings.get("dagrun_timeout")
    require(isinstance(run_limit, timedelta) and run_limit >= sum(limits, timedelta(0)), f"the chain run has no deadline that covers its layers: {run_limit}")
    require(loaded["dag"].settings.get("max_active_runs") == 1, "the chain may run twice at once")


def named_child_dags_exist_and_accept_the_settings() -> None:
    loaded = load_dag()
    nodes = {node.task_id: node.settings for node in loaded["operators"]}
    problems = []
    for layer, dag_id, _ in loaded["CHAIN"]:
        path = ROOT / "dags" / f"{dag_id}.py"
        if not path.exists():
            problems.append(f"{dag_id} is not in the tree")
            continue
        text = path.read_text()
        if f'dag_id="{dag_id}"' not in text:
            problems.append(f"{path.name} does not declare {dag_id}")
        for key in nodes[f"run_{layer}"]["conf"]:
            if not re.search(rf'"{key}"\s*:\s*Param\(', text):
                problems.append(f"{dag_id} declares no parameter {key}")
    require(not problems, "; ".join(problems))


def metadata_start_follows_drift_and_never_blocks_the_chain() -> None:
    loaded = load_dag()
    nodes = {node.task_id: node for node in loaded["operators"]}
    node = nodes.get("start_metadata")
    if node is None or node.function is None:
        raise AssertionError("the chain has no start_metadata task")
    settings = node.settings
    require(node.upstream == [nodes["run_drift"]], "the catalog start does not follow only the drift layer")
    require(not any(node in other.upstream for other in loaded["operators"]), "a task follows the catalog start, so its failure could hold up the chain")
    require(
        settings.get("trigger_rule") in (None, "all_success"),
        "an upstream failure must still fail the chain, so the catalog start keeps the default trigger rule",
    )
    require(settings.get("retries") == 0, "a catalog start retry may start a second paid task")
    run = str(loaded["metadata_run_id"](BRONZE))
    require(RUN_ID.fullmatch(run) is not None, f"the catalog run id is not a valid run id: {run}")
    layer_ids = {str(loaded["chain_run"](layer, BRONZE)) for layer, _, _ in loaded["CHAIN"]}
    require(run not in layer_ids, "the catalog run id repeats a layer run id")
    require(
        run == str(loaded["metadata_run_id"](BRONZE)) and run != str(loaded["metadata_run_id"](OTHER_BRONZE)), "the catalog run id is not stable per bronze run"
    )


class Airflow:
    """A loopback stand-in for the Airflow 3 REST API that records each request and answers with a configured status."""

    def __init__(self, status: int = 200, delay: float = 0.0) -> None:
        self.requests: list[dict[str, Any]] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}")
                outer.requests.append({"path": self.path, "authorization": self.headers.get("Authorization", ""), "body": body})
                time.sleep(delay)
                data = b"{}"
                try:
                    self.send_response(status)
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                except OSError:
                    return

            def log_message(self, *_: Any) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> Airflow:
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    @property
    def port(self) -> int:
        return int(self.server.server_address[1])


SIGNING = "synthetic-signing-value"


def start_metadata(port: int, limit: float | None = None) -> tuple[str, list[str], float]:
    """Run the catalog start task function against the stand-in API and return its result, the error log lines it wrote and the seconds it took."""
    loaded = load_dag()
    node = {item.task_id: item for item in loaded["operators"]}["start_metadata"]
    lines: list[str] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            lines.append(record.getMessage())

    handler = Capture(level=logging.ERROR)
    logging.getLogger().addHandler(handler)
    patched: dict[str, Any] = {"get_current_context": lambda: {"params": {"run_id": BRONZE, "mode": "real"}}}
    if limit is not None:
        patched["TRIGGER_TIMEOUT_SECONDS"] = limit
    environment = {"AIRFLOW_API_PORT": str(port), "AIRFLOW__API_AUTH__JWT_SECRET": SIGNING}
    started = time.monotonic()
    try:
        with mock.patch.dict(node.function.__globals__, patched), mock.patch.dict(os.environ, environment):
            result = node.function()
    finally:
        logging.getLogger().removeHandler(handler)
    return result, lines, time.monotonic() - started


def metadata_start_posts_the_dag_run_through_the_api_with_a_signed_token() -> None:
    run = str(load_dag()["metadata_run_id"](BRONZE))
    with Airflow() as api:
        result, lines, _ = start_metadata(api.port)
    require(len(api.requests) == 1, f"the start must make one trigger request, made {len(api.requests)}")
    request = api.requests[0]
    require(request["path"] == "/api/v2/dags/metadata_ingest_dag/dagRuns", f"wrong trigger path: {request['path']}")
    require(
        request["body"].get("dag_run_id") == f"chain__metadata_ingest_dag__{run}" and request["body"].get("conf") == {"run_id": run, "mode": "real"},
        f"wrong trigger body: {request['body']}",
    )
    header, claims, signature = request["authorization"].removeprefix("Bearer ").split(".")
    expected = base64.urlsafe_b64encode(hmac.new(SIGNING.encode(), f"{header}.{claims}".encode(), hashlib.sha512).digest()).rstrip(b"=").decode()
    require(signature == expected, "the token is not signed with the Airflow JWT secret")
    payload = json.loads(base64.urlsafe_b64decode(claims + "=" * (-len(claims) % 4)))
    require(payload.get("aud") == "apache-airflow" and payload.get("exp", 0) > time.time(), f"wrong token claims: {payload}")
    require("requested" in result and run in result and not lines, f"a trigger that worked must say so and log no error: {result} {lines}")


def metadata_start_treats_an_existing_run_as_done() -> None:
    with Airflow(status=409) as api:
        result, lines, _ = start_metadata(api.port)
    require("already" in result and "failed" not in result and not lines, f"a retried bronze run names the DAG run that exists: {result} {lines}")


def metadata_start_records_a_failed_trigger_in_the_log_and_the_result() -> None:
    for label, status, delay, limit in (("refused", 500, 0.0, None), ("missing", 404, 0.0, None), ("hung", 200, 4.0, 1.0)):
        with Airflow(status=status, delay=delay) as api:
            result, lines, took = start_metadata(api.port, limit)
        require(result.startswith("catalog start failed"), f"{label}: the result must say the start failed: {result}")
        require(any("catalog start" in line for line in lines), f"{label}: the failure must reach the task log: {lines}")
        require(took < 3.5, f"{label}: the trigger call must end within its bound, took {took:.1f} seconds")
    unused = socket.socket()
    unused.bind(("127.0.0.1", 0))
    port = unused.getsockname()[1]
    unused.close()
    result, lines, _ = start_metadata(port)
    require(result.startswith("catalog start failed") and bool(lines), f"an unreachable API must be recorded the same way: {result} {lines}")


def broker_table_runs_the_catalog_step_as_a_bounded_chain_job() -> None:
    document = json.loads((ROOT / "config/orchestration/ecs_broker_jobs.json").read_text())
    job = document["jobs"].get("metadata")
    if job is None:
        raise AssertionError("the broker table has no metadata job")
    require(document["steps"].get("metadata_run") == "metadata", "the step metadata_run does not name the metadata job")
    require(job.get("chain") is True, "the catalog job may start before the first publishes are signed")
    require(job["containers"] == {"ingest_polaris": ["RUN_ID", "POLARIS_URI"]}, f"wrong containers: {job['containers']}")
    terraform = (ROOT / "infra/jobs/openmetadata.tf").read_text()
    default = re.search(r'variable "openmetadata_lifetime_seconds".*?default\s*=\s*([0-9]+)', terraform, re.S)
    require(default is not None and job["deadline_seconds"] == int(default.group(1)) + 900, "the broker deadline is not the task lifetime plus 900 seconds")
    require('run_step("metadata_run")' in (ROOT / "dags/metadata_ingest_dag.py").read_text(), "the catalog DAG does not submit metadata_run")
    require(load_jobs(ROOT)["jobs"]["metadata"] == job, "the broker does not load the metadata job")


CASES: dict[str, Callable[[], None]] = {
    "derived_ids_are_valid_and_stable": derived_ids_are_valid_and_stable,
    "broker_and_dag_derive_the_same_run_ids": broker_and_dag_derive_the_same_run_ids,
    "chain_runs_in_order_after_the_gate": chain_runs_in_order_after_the_gate,
    "triggers_send_derived_ids": triggers_send_derived_ids,
    "a_retried_bronze_run_names_the_same_runs": a_retried_bronze_run_names_the_same_runs,
    "waits_have_explicit_deadlines": waits_have_explicit_deadlines,
    "named_child_dags_exist_and_accept_the_settings": named_child_dags_exist_and_accept_the_settings,
    "metadata_start_follows_drift_and_never_blocks_the_chain": metadata_start_follows_drift_and_never_blocks_the_chain,
    "metadata_start_posts_the_dag_run_through_the_api_with_a_signed_token": metadata_start_posts_the_dag_run_through_the_api_with_a_signed_token,
    "metadata_start_treats_an_existing_run_as_done": metadata_start_treats_an_existing_run_as_done,
    "metadata_start_records_a_failed_trigger_in_the_log_and_the_result": metadata_start_records_a_failed_trigger_in_the_log_and_the_result,
    "broker_table_runs_the_catalog_step_as_a_bounded_chain_job": broker_table_runs_the_catalog_step_as_a_bounded_chain_job,
}
