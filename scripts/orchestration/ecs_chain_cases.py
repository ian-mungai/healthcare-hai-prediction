"""Cases for the first DAG (the pipeline), the plain bronze DAG, the catalog start of the training DAG and the job table of the catalog.

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

from orchestration import training_contract as contract
from orchestration.ecs_preflight import load_tables
from orchestration.training_contract import RUN_ID as RUN_ID_PATTERN
from scripts.orchestration.ecs_adapter_cases import require

ROOT = Path(__file__).resolve().parents[2]
BRONZE = "bronze_b0b0b0b0"
OTHER_BRONZE = "bronze_b1b1b1b1"
BASE_RUN = "features_b0b0b0b0"
OTHER_BASE_RUN = "features_b1b1b1b1"
RUN_ID = re.compile(RUN_ID_PATTERN)


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


class Skipped(Exception):
    """The stand-in of ``AirflowSkipException``."""


class Trigger(Operator):
    """The stand-in of ``TriggerDagRunOperator``: built with its settings, or partial with the requests mapped over a task's output (``expand_kwargs``)."""

    built: list[Operator] = []

    def __init__(self, **settings: Any) -> None:
        super().__init__(**settings)
        self.built.append(self)

    @classmethod
    def partial(cls, **settings: Any) -> SimpleNamespace:
        def expand_kwargs(source: Any) -> Operator:
            node = cls(**settings)
            source.operator >> node
            return node

        return SimpleNamespace(expand_kwargs=expand_kwargs)


class Branch(Trigger):
    """The stand-in of ``BranchPythonOperator``: built with its settings, among them the callable that names the tasks to go on with."""


def load_dag(file: str) -> dict[str, Any]:
    """Run ``dags/<file>`` against the stand-ins and return its globals, the operators it built and the stand-in modules it ran with."""
    built: list[Operator] = []
    Trigger.built = built

    def task(**settings: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
            def call() -> SimpleNamespace:
                node = Operator(**settings)
                node.function = function
                built.append(node)
                return SimpleNamespace(operator=node)

            return call

        return decorate

    sdk = SimpleNamespace(
        Asset=str,
        TriggerRule=SimpleNamespace(ALL_SUCCESS="all_success", ALL_DONE="all_done", NONE_FAILED="none_failed"),
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
        "airflow.providers.standard.operators.trigger_dagrun": SimpleNamespace(TriggerDagRunOperator=Trigger),
        "airflow.providers.standard.operators.python": SimpleNamespace(BranchPythonOperator=Branch),
        "airflow.sdk.exceptions": SimpleNamespace(AirflowSkipException=Skipped),
    }
    previous = sys.modules.pop("orchestration.dag_runtime", None)
    try:
        with mock.patch.dict(sys.modules, modules):
            loaded = runpy.run_path(str(ROOT / "dags" / file))
    finally:
        sys.modules.pop("orchestration.dag_runtime", None)
        if previous is not None:
            sys.modules["orchestration.dag_runtime"] = previous
    return {**loaded, "operators": built, "modules": modules}


def recording(seen: list[str]) -> Callable[[str], str]:
    """A stand-in of ``run_step`` that records the template it is asked to run and returns it."""

    def run_step(template: str) -> str:
        seen.append(template)
        return template

    return run_step


def bronze_dag_runs_only_the_bronze_jobs_in_order() -> None:
    loaded = load_dag("bronze_dag.py")
    operators = loaded["operators"]
    nodes = {node.task_id: node for node in operators}
    require(list(nodes) == ["bronze_stage", "bronze_load"], f"wrong tasks: {list(nodes)}")
    require(not any(isinstance(node, Trigger) for node in operators), "the bronze DAG starts another DAG")
    require(nodes["bronze_stage"].upstream == [], "the stage job has an upstream task")
    require(nodes["bronze_load"].upstream == [nodes["bronze_stage"]], "the bronze job does not follow only the stage job")
    settings = loaded["dag"].settings
    require(settings["dag_id"] == "bronze_dag" and settings["schedule"] is None and settings["catchup"] is False, f"the bronze DAG settings: {settings}")
    require(settings["max_active_runs"] == 1, "the bronze DAG may run twice at once")
    (run_args, run_settings), (mode_args, mode_settings) = settings["params"]["run_id"], settings["params"]["mode"]
    require(run_args == () and run_settings["pattern"] == f"^{contract.run_id_pattern('bronze')}$", f"the run ID form: {run_settings}")
    require(mode_args == ("real",) and mode_settings["enum"] == ["real"], f"the mode form: {mode_settings}")
    require(all(node.settings.get("retries") == 0 for node in operators), "a task retries")


def bronze_dag_tasks_run_their_own_templates() -> None:
    loaded = load_dag("bronze_dag.py")
    seen: list[str] = []
    for node in loaded["operators"]:
        function = node.function
        if function is None:
            raise AssertionError(f"{node.task_id} has no function")
        with mock.patch.dict(function.__globals__, {"run_step": recording(seen)}):
            require(function() == node.task_id, f"{node.task_id} ran another template")
    require(seen == ["bronze_stage", "bronze_load"], f"the templates run were {seen}")
    text = (ROOT / "dags/bronze_dag.py").read_text()
    for name in ("TriggerDagRunOperator", "create_dag_run", "silver_transform_dag", "metadata_ingest_dag", "chain_layer_run"):
        require(name not in text, f"the bronze DAG file names {name}: only the pipeline DAG runs the later layers")


def bronze_dag_waits_have_explicit_deadlines() -> None:
    loaded = load_dag("bronze_dag.py")
    nodes = {node.task_id: node.settings for node in loaded["operators"]}
    stage, load = nodes["bronze_stage"].get("execution_timeout"), nodes["bronze_load"].get("execution_timeout")
    require(isinstance(stage, timedelta) and timedelta(0) < stage <= timedelta(hours=2), f"the bronze stage wait has no short deadline: {stage}")
    require(isinstance(load, timedelta) and load >= timedelta(hours=1), f"the bronze job wait has no deadline: {load}")
    run_limit = loaded["dag"].settings.get("dagrun_timeout")
    require(isinstance(run_limit, timedelta) and run_limit >= stage + load, f"the bronze run deadline does not cover the stage and bronze jobs: {run_limit}")


def the_gate_templates_launch_nothing_and_the_old_names_are_gone() -> None:
    steps = json.loads((ROOT / "config/orchestration/ecs_broker_jobs.json").read_text())["steps"]
    require(steps.get("bind_code") == "@gate", f"the binding step is {steps.get('bind_code')}")
    require("bronze_gate" not in steps, "the bronze DAG has a gate step: its jobs bind their own run")
    dags = json.loads((ROOT / "config/orchestration/host_jobs.json").read_text())["dags"]
    require(dags.get(contract.PIPELINE_DAG) == "chain" and dags.get("bronze_dag") == "chain", "the first DAG and the bronze DAG have no row in the host table")


def the_automatic_training_route_carries_the_base_run() -> None:
    """The request that the drift DAG builds for a model's training run names the base run of the model's set, so the training run starts the catalog (once
    for the base) and, for a promotion model, the research. Only a hand start names no base run."""
    build = load_dag("drift_monitoring_dag.py")["training_requests"]
    base = BASE_RUN
    for model in contract.MODELS:
        features = contract.model_features_run_id(base, model)
        drift = contract.model_drift_run_id(features)
        request = build(drift, "aws", model, features, ["HAI_1", "HAI_2"])
        require(len(request) == 1, f"{model}: the gate trains two types and the request is {request}")
        conf = request[0]["conf"]
        require(
            conf["base_run"] == base and request[0]["trigger_run_id"] == contract.model_training_run_id(drift, model), f"{model}: the request is {request[0]}"
        )
        require(conf["trigger"] == f"drift:{drift}" and conf["features_run"] == features, f"{model}: the request lost its trigger or its set: {conf}")
        researched = contract.research_requests(model, conf["run_id"], conf["base_run"], conf["population"], conf["hai_types"], conf["mlflow"])
        require(bool(researched) == (model in contract.PROMOTION_MODELS), f"{model}: research for the automatic training run is {researched}")
    with Airflow() as api:
        result, lines, _ = start_metadata(api.port, base_run=conf["base_run"])
    require(api.requests[0]["body"]["dag_run_id"] == contract.metadata_run_for_base(base) and not lines, f"the catalog was not started: {result} {lines}")
    require(build(contract.model_drift_run_id(features), "aws", "m6", features, []) == [], "a gate that trains no type still made a request")


def training_dag_ends_with_a_catalog_start_behind_its_release_that_never_blocks() -> None:
    loaded = load_dag("training_dag.py")
    nodes = {node.task_id: node for node in loaded["operators"]}
    node = nodes.get("start_metadata")
    if node is None or node.function is None:
        raise AssertionError("the training DAG has no start_metadata task")
    settings = node.settings
    require(node.upstream == [nodes["release_run"]], "the catalog start does not follow only the release of the training run")
    require(not any(node in other.upstream for other in loaded["operators"]), "a task follows the catalog start, so its failure could hold up the run")
    require(
        settings.get("trigger_rule") in (None, "all_success"),
        "a training run that failed its release must not start the catalog, so the start keeps the default rule",
    )
    require(settings.get("retries") == 0, "a catalog start retry may start a second paid task")
    require(isinstance(settings.get("execution_timeout"), timedelta), "the catalog start has no deadline")
    run = contract.metadata_run_for_base(BASE_RUN)
    require(RUN_ID.fullmatch(run) is not None and run == "metadata_b0b0b0b0", f"the catalog run id is not a valid run id of the base run: {run}")
    require(run != contract.metadata_run_for_base(OTHER_BASE_RUN), "the catalog run id is not stable per base run")
    require(
        run not in {contract.chain_layer_run(layer, BRONZE) for layer in contract.PIPELINE_LAYERS if layer != "metadata"},
        "the catalog run id repeats a layer run id",
    )


class Airflow:
    """A loopback stand-in for the Airflow 3 REST API that records each request and answers with a configured status."""

    def __init__(self, status: int = 200, delay: float = 0.0, record: dict[str, Any] | None = None) -> None:
        self.requests: list[dict[str, Any]] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                outer.requests.append({"path": self.path, "authorization": self.headers.get("Authorization", ""), "body": None})
                data = json.dumps(record if record is not None else {}).encode()
                self.send_response(200 if record is not None else 404)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

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


def start_metadata(port: int, limit: float | None = None, base_run: str = BASE_RUN) -> tuple[str, list[str], float]:
    """Run the catalog start task function of the training DAG against the stand-in API and return its result, the error log lines it wrote and the seconds."""
    loaded = load_dag("training_dag.py")
    node = {item.task_id: item for item in loaded["operators"]}["start_metadata"]
    lines: list[str] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            lines.append(record.getMessage())

    handler = Capture(level=logging.ERROR)
    logging.getLogger().addHandler(handler)
    runtime: dict[str, Any] = node.function.__globals__  # the call, its limit and the context live in the DAG runtime module
    patched: dict[str, Any] = {"get_current_context": lambda: {"params": {"base_run": base_run, "mode": "real"}}}
    patched |= {} if limit is None else {"TRIGGER_TIMEOUT_SECONDS": limit}
    environment = {"AIRFLOW_API_PORT": str(port), "AIRFLOW__API_AUTH__JWT_SECRET": SIGNING}
    started = time.monotonic()
    try:
        with mock.patch.dict(sys.modules, loaded["modules"]), mock.patch.dict(runtime, patched), mock.patch.dict(os.environ, environment):
            result = node.function()
    finally:
        logging.getLogger().removeHandler(handler)
    return result, lines, time.monotonic() - started


def metadata_start_posts_the_dag_run_through_the_api_with_a_signed_token() -> None:
    run = contract.metadata_run_for_base(BASE_RUN)
    with Airflow() as api:
        result, lines, _ = start_metadata(api.port)
    require(len(api.requests) == 1, f"the start must make one trigger request, made {len(api.requests)}")
    request = api.requests[0]
    require(request["path"] == "/api/v2/dags/metadata_ingest_dag/dagRuns", f"wrong trigger path: {request['path']}")
    require(
        request["body"].get("dag_run_id") == run and request["body"].get("conf") == {"run_id": run, "features_run": BASE_RUN, "mode": "real"},
        f"wrong trigger body: {request['body']}",
    )
    header, claims, signature = request["authorization"].removeprefix("Bearer ").split(".")
    expected = base64.urlsafe_b64encode(hmac.new(SIGNING.encode(), f"{header}.{claims}".encode(), hashlib.sha512).digest()).rstrip(b"=").decode()
    require(signature == expected, "the token is not signed with the Airflow JWT secret")
    payload = json.loads(base64.urlsafe_b64decode(claims + "=" * (-len(claims) % 4)))
    require(payload.get("aud") == "apache-airflow" and payload.get("exp", 0) > time.time(), f"wrong token claims: {payload}")
    require("requested" in result and run in result and not lines, f"a trigger that worked must say so and log no error: {result} {lines}")


def metadata_start_treats_an_existing_run_as_done() -> None:
    run = contract.metadata_run_for_base(BASE_RUN)
    record = {"dag_id": "metadata_ingest_dag", "dag_run_id": run, "state": "success", "conf": {"run_id": run, "features_run": BASE_RUN}}  # mode defaulted
    with Airflow(status=409, record=record) as api:
        result, lines, _ = start_metadata(api.port)
    require(
        "already" in result and "failed" not in result and not lines, f"a second training run of the base run names the DAG run that exists: {result} {lines}"
    )


def metadata_start_refuses_a_run_of_another_base_without_failing_training() -> None:
    """The catalog run that exists must be the run of this base run: one started with another features run is not reused. The start returns the refusal and
    logs it at error level, and never raises, as every other failure of the catalog start (the training result does not depend on the catalog)."""
    run = contract.metadata_run_for_base(BASE_RUN)
    for label, conf in (
        ("another features run", {"run_id": run, "features_run": OTHER_BASE_RUN, "mode": "real"}),
        ("another run", {"run_id": contract.metadata_run_for_base(OTHER_BASE_RUN), "features_run": BASE_RUN, "mode": "real"}),
        ("no settings", None),
    ):
        record = {"dag_id": "metadata_ingest_dag", "dag_run_id": run, "state": "success", "conf": conf}
        with Airflow(status=409, record=record) as api:
            result, lines, _ = start_metadata(api.port)
        require(result.startswith("catalog start refused") and run in result and "expected" in result and "found" in result, f"{label}: {result}")
        require(any("other settings" in line for line in lines), f"{label}: the refusal did not reach the task log: {lines}")


def a_training_run_with_no_base_run_starts_no_catalog() -> None:
    with Airflow() as api:
        try:
            start_metadata(api.port, base_run="")
        except Skipped:
            pass
        else:
            raise AssertionError("a hand-started training run with no base run started the catalog")
    require(not api.requests, f"a skipped catalog start still called the API: {api.requests}")


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


def job_table_runs_the_catalog_step_as_a_bounded_chain_job() -> None:
    document = json.loads((ROOT / "config/orchestration/ecs_broker_jobs.json").read_text())
    job = document["jobs"].get("metadata")
    if job is None:
        raise AssertionError("the job table has no metadata job")
    require(document["steps"].get("metadata_run") == "metadata", "the step metadata_run does not name the metadata job")
    require(job.get("chain") is True, "the catalog job belongs to the chain")
    require(job["containers"] == {"ingest": ["RUN_ID", "POLARIS_URI", "FEATURES_RUN", "MLFLOW_TRACKING_URI"]}, f"wrong containers: {job['containers']}")
    require(0 < job["deadline_seconds"] <= 14400 + 900, "the metadata job deadline is not within the DAG step limit plus 900 seconds")
    require('run_step("metadata_run")' in (ROOT / "dags/metadata_ingest_dag.py").read_text(), "the catalog DAG does not submit metadata_run")
    require(load_tables(ROOT)[0]["jobs"]["metadata"] == job, "the launch preflight does not load the metadata job")


CASES: dict[str, Callable[[], None]] = {
    "bronze_dag_runs_only_the_bronze_jobs_in_order": bronze_dag_runs_only_the_bronze_jobs_in_order,
    "bronze_dag_tasks_run_their_own_templates": bronze_dag_tasks_run_their_own_templates,
    "bronze_dag_waits_have_explicit_deadlines": bronze_dag_waits_have_explicit_deadlines,
    "the_gate_templates_launch_nothing_and_the_old_names_are_gone": the_gate_templates_launch_nothing_and_the_old_names_are_gone,
    "the_automatic_training_route_carries_the_base_run": the_automatic_training_route_carries_the_base_run,
    "training_dag_ends_with_a_catalog_start_behind_its_release_that_never_blocks": training_dag_ends_with_a_catalog_start_behind_its_release_that_never_blocks,
    "metadata_start_posts_the_dag_run_through_the_api_with_a_signed_token": metadata_start_posts_the_dag_run_through_the_api_with_a_signed_token,
    "metadata_start_treats_an_existing_run_as_done": metadata_start_treats_an_existing_run_as_done,
    "metadata_start_refuses_a_run_of_another_base_without_failing_training": metadata_start_refuses_a_run_of_another_base_without_failing_training,
    "a_training_run_with_no_base_run_starts_no_catalog": a_training_run_with_no_base_run_starts_no_catalog,
    "metadata_start_records_a_failed_trigger_in_the_log_and_the_result": metadata_start_records_a_failed_trigger_in_the_log_and_the_result,
    "job_table_runs_the_catalog_step_as_a_bounded_chain_job": job_table_runs_the_catalog_step_as_a_bounded_chain_job,
}
