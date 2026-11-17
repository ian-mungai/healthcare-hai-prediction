"""E2E cases for ``dags/drift_monitoring_dag.py`` on synthetic drift records, with a declared stand-in for the Airflow SDK.

The real DAG file runs against stand-ins for ``airflow.sdk``, the standard provider's ``TriggerDagRunOperator`` and ``AirflowSkipException``, as the other
DAG cases do. The stand-ins record the graph the file declares and keep each task's function, so the cases run the real task bodies with a chosen DAG run
context. The launch library is stood in by a captured ``launch_step``. What this proves: the declared graph, the trigger arguments, the decision from records,
the training configuration against the real validators, and the skip for nothing to monitor. What it does not prove: that Airflow accepts the graph, maps the
trigger over the list, or starts ``training_dag``. That needs ``airflow dags test`` in the orchestration image.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import logging
import os
import re
import sys
from collections.abc import Callable
from contextlib import contextmanager
from enum import StrEnum
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, ClassVar
from unittest import mock

from monitoring import drift_gate
from monitoring.aws_cases import MemoryS3
from orchestration.training_contract import HAI_TYPES, LAUNCH_LANES, RUN_ID, SET_RUN, model_training_run_id, parameters_for, run_id_pattern

ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = ROOT / "dags/drift_monitoring_dag.py"
DRIFT_RUN = "drift_elastic_net_0a1b2c3d"
FEATURES_RUN = "features_elastic_net_feed5eed"
BUCKET = "bucket-for-tests"
Problems = list[str]
CONTEXT: dict[str, Any] = {}


class Rule(StrEnum):
    ALL_SUCCESS = "all_success"
    ALL_DONE = "all_done"


class SkipSignal(Exception):
    """The stand-in for AirflowSkipException."""


class Node:
    """A declared task: its settings, its function and its upstream task ids."""

    def __init__(self, task_id: str, settings: dict[str, Any], function: Callable[..., Any] | None = None) -> None:
        self.task_id, self.settings, self.function = task_id, settings, function
        self.upstream_task_ids: set[str] = set()

    def __rshift__(self, after: Node) -> Node:
        after.upstream_task_ids.add(self.task_id)
        return after


class Dag:
    current: ClassVar[Dag | None] = None

    def __init__(self, **settings: Any) -> None:
        self.settings = settings
        self.task_dict: dict[str, Node] = {}

    def __enter__(self) -> Dag:
        Dag.current = self
        return self

    def __exit__(self, *arguments: Any) -> None:
        Dag.current = None


class Param:
    def __init__(self, *arguments: Any, **settings: Any) -> None:
        self.default = arguments[0] if arguments else None
        self.settings = settings


def task(**settings: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
        def call(*arguments: Any, **keywords: Any) -> Any:
            if Dag.current is None:
                raise ValueError("a task was created outside its DAG")
            node = Node(settings["task_id"], settings, function)
            Dag.current.task_dict[node.task_id] = node
            return SimpleNamespace(operator=node)

        return call

    return decorate


class TriggerDagRunOperator:
    """Record ``partial(...).expand_kwargs(source)`` as one mapped task fed by the source task."""

    @staticmethod
    def partial(**settings: Any) -> SimpleNamespace:
        def expand_kwargs(source: Any) -> Node:
            if Dag.current is None:
                raise ValueError("an operator was created outside its DAG")
            node = Node(settings["task_id"], settings)
            node.upstream_task_ids.add(source.operator.task_id)
            Dag.current.task_dict[node.task_id] = node
            return node

        return SimpleNamespace(expand_kwargs=expand_kwargs)


def modules() -> dict[str, ModuleType]:
    sdk = ModuleType("airflow.sdk")
    for name, value in (
        ("DAG", Dag),
        ("Param", Param),
        ("TriggerRule", Rule),
        ("Asset", str),
        ("task", task),
        ("get_current_context", lambda: CONTEXT["context"]),
    ):
        setattr(sdk, name, value)
    exceptions, trigger = ModuleType("airflow.sdk.exceptions"), ModuleType("airflow.providers.standard.operators.trigger_dagrun")
    for module, name, value in ((exceptions, "AirflowSkipException", SkipSignal), (trigger, "TriggerDagRunOperator", TriggerDagRunOperator)):
        setattr(module, name, value)
    names = ("airflow", "airflow.providers", "airflow.providers.standard", "airflow.providers.standard.operators")
    return {
        **{name: ModuleType(name) for name in names},
        "airflow.sdk": sdk,
        "airflow.sdk.exceptions": exceptions,
        "airflow.providers.standard.operators.trigger_dagrun": trigger,
    }


@contextmanager
def loaded_dag() -> Any:
    """Load the real DAG file against the stand-ins and give back its module, restoring the real ``orchestration.dag_runtime`` afterwards."""
    previous = sys.modules.pop("orchestration.dag_runtime", None)
    try:
        with mock.patch.dict(sys.modules, modules()):
            specification = importlib.util.spec_from_file_location("drift_dag_under_test", DAG_FILE)
            if specification is None or specification.loader is None:
                raise ValueError("the DAG file has no import specification")
            module = importlib.util.module_from_spec(specification)
            specification.loader.exec_module(module)
            yield module
    finally:
        sys.modules.pop("orchestration.dag_runtime", None)
        if previous is not None:
            sys.modules["orchestration.dag_runtime"] = previous


def record(hai_type: str, *, run: str = DRIFT_RUN, status: str = "checked", triggered: bool = False, population: str = "primary") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "drift_record",
        "run_id": run,
        "hai_type": hai_type,
        "population": population,
        "status": status,
        "skip_reason": "no_new_window" if status == "skipped" else None,
        "triggered": triggered,
    }


def bucket_with(
    records: list[dict[str, Any]],
    *,
    marker: bool = True,
    marker_run: str = DRIFT_RUN,
    raw: dict[str, bytes] | None = None,
    unmonitored: dict[str, str] | None = None,
    model: str = "m2",
    gated: bool = True,
) -> MemoryS3:
    """An in-memory bucket as the task's publish phase leaves it: the records, then the marker that names them and, for a run of one model, the gate's
    decision of that model (``monitoring.drift_gate``)."""
    s3 = MemoryS3()
    bodies = {f"drift_{item['hai_type']}.json": (json.dumps(item) + "\n").encode() for item in records}
    sums = {name: hashlib.sha256(body).hexdigest() for name, body in bodies.items()}
    bodies.update(raw or {})
    for name, body in bodies.items():
        s3.put_object(Bucket=BUCKET, Key=f"drift/{DRIFT_RUN}/{name}", Body=body)
        sums.setdefault(name, hashlib.sha256(body).hexdigest())
    if marker:
        document: dict[str, Any] = {
            "run_id": marker_run,
            "monitored": [item["hai_type"] for item in records],
            "unmonitored": dict(unmonitored or {}),
            "files": sums,
        }
        if gated:
            baseline = {"monitored": document["monitored"], "unmonitored": document["unmonitored"]}
            document |= {"model": model, "gate": drift_gate.decide(model, baseline, {item["hai_type"]: item for item in records})}
        s3.put_object(Bucket=BUCKET, Key=f"drift/{DRIFT_RUN}/_complete.json", Body=json.dumps(document).encode())
    return s3


def decide(module: Any, s3: MemoryS3, *, mlflow: str = "aws", model: str = "m2") -> list[dict[str, Any]]:
    """Run the real ``decide`` task body for the DRIFT_RUN of one model run's set against the in-memory bucket."""
    CONTEXT["context"] = {"params": {"run_id": DRIFT_RUN, "mode": "real", "mlflow": mlflow, "model": model, "features_run": FEATURES_RUN}}
    with mock.patch.dict(os.environ, {"HAI_LAUNCH_BUCKET": BUCKET}), mock.patch.object(module, "s3_client", return_value=s3):
        os.environ.pop("DATA_BUCKET_NAME", None)
        result: list[dict[str, Any]] = module.dag.task_dict["decide"].function()
    return result


def decide_reads_the_launch_bucket(folder: Path) -> Problems:
    """The services environment names the data bucket in HAI_LAUNCH_BUCKET (infra/services/airflow.tf) and sets no DATA_BUCKET_NAME, so decide reads it."""
    with loaded_dag() as module:
        CONTEXT["context"] = {"params": {"run_id": DRIFT_RUN, "mode": "real", "mlflow": "aws", "model": "m2", "features_run": FEATURES_RUN}}
        s3 = bucket_with([record("HAI_1")])
        with mock.patch.dict(os.environ, {"DATA_BUCKET_NAME": BUCKET}), mock.patch.object(module, "s3_client", return_value=s3):
            os.environ.pop("HAI_LAUNCH_BUCKET", None)
            try:
                module.dag.task_dict["decide"].function()
            except RuntimeError as error:
                if "HAI_LAUNCH_BUCKET" not in str(error):
                    return [f"the refusal does not name HAI_LAUNCH_BUCKET: {error}"]
            else:
                return ["decide read a bucket without HAI_LAUNCH_BUCKET"]
    text = (ROOT / "infra/services/airflow.tf").read_text()
    if "HAI_LAUNCH_BUCKET" not in text:
        return ["the services environment no longer sets HAI_LAUNCH_BUCKET"]
    return []


def graph_is_declared(folder: Path) -> Problems:
    with loaded_dag() as module:
        dag = module.dag
        nodes = dag.task_dict
        expected = {"drift_run": set(), "decide": {"drift_run"}, "trigger_training": {"decide"}}
        problems = []
        if {name: node.upstream_task_ids for name, node in nodes.items()} != expected:
            problems.append(f"the graph is { ({name: sorted(node.upstream_task_ids) for name, node in nodes.items()}) }")
        wanted = {"schedule": None, "catchup": False, "max_active_runs": 1, "default_args": {"retries": 0}, "dag_id": "drift_monitoring_dag"}
        problems += [f"DAG setting {key} is {dag.settings.get(key)!r}, expected {value!r}" for key, value in wanted.items() if dag.settings.get(key) != value]
        trigger = nodes["trigger_training"].settings
        wanted_trigger = {"trigger_dag_id": "training_dag", "wait_for_completion": False, "reset_dag_run": False, "skip_when_already_exists": True}
        problems += [f"trigger setting {key} is {trigger.get(key)!r}, expected {value!r}" for key, value in wanted_trigger.items() if trigger.get(key) != value]
        if trigger.get("fail_when_dag_is_paused") is not True:
            problems.append("a trigger of a paused training_dag would pass silently")
        params = dag.settings["params"]
        if params["run_id"].settings.get("pattern") != f"^{run_id_pattern('drift', qualified=True)}$" or params["mode"].settings.get("enum") != ["real"]:
            problems.append("the run_id pattern differs from the training DAG's or the mode is not real only")
        if params["features_run"].default is not None or params["features_run"].settings.get("pattern") != f"^{SET_RUN}$":
            problems.append("the features_run is not a required run ID: the drift check reads one published set, never the latest")
        return problems


def one_drifted_type_starts_one_training_run(folder: Path) -> Problems:
    with loaded_dag() as module:
        requests = decide(module, bucket_with([record("HAI_1", triggered=True), record("HAI_2"), record("HAI_3", status="skipped")]))
        if len(requests) != 1:
            return [f"expected one trigger request, got {len(requests)}"]
        request = requests[0]
        training = {
            "features_run": FEATURES_RUN,
            "population": "primary",
            "hai_types": ["HAI_1"],
            "trigger": f"drift:{DRIFT_RUN}",
            "mlflow": "aws",
            "model": "m2",
        }
        problems = []
        if request["trigger_run_id"] != request["conf"]["run_id"]:
            problems.append(f"the trigger run id is {request['trigger_run_id']}")
        if request["conf"] != {"run_id": request["conf"]["run_id"], "mode": "real", **training}:
            problems.append(f"the training conf is not the run, the real mode and the training parameters: {request['conf']}")
        if not re.fullmatch(RUN_ID, request["conf"]["run_id"]) or request["conf"]["run_id"] == DRIFT_RUN:
            problems.append("the training run id is malformed or equals the drift run id")
        if request["conf"]["run_id"] != model_training_run_id(DRIFT_RUN, "m2"):
            problems.append("the training run id is not the model's own, derived from the drift run and the model (the chain gate binds that run)")
        return problems


def aws_decision_reaches_a_training_request(folder: Path) -> Problems:
    """Round 2 review H4: with the aws lane (the only one a launched DAG step accepts) a drifted type becomes a training request whose parameters the
    training DAG's launch step accepts, instead of an error before the request is built."""
    with loaded_dag() as module:
        try:
            requests = decide(module, bucket_with([record("HAI_2", triggered=True)]), mlflow="aws")
        except Exception as error:
            return [f"decide raised before the training request with mlflow=aws: {type(error).__name__}: {error}"]
        if len(requests) != 1 or requests[0]["conf"].get("mlflow") != "aws":
            return [f"expected one training request on the aws lane, got {requests}"]
        conf = requests[0]["conf"]
        try:
            parameters_for(
                "training_dag", {name: conf[name] for name in ("features_run", "population", "hai_types", "trigger", "mlflow", "model")}, LAUNCH_LANES
            )
        except ValueError as error:
            return [f"the training DAG's launch step would refuse the request: {error}"]
        return []


def local_lane_is_refused_by_name(folder: Path) -> Problems:
    """A drift run that names mlflow=local is refused by name: a launched training step would refuse it anyway, so no request carries it."""
    with loaded_dag() as module:
        try:
            requests = decide(module, bucket_with([record("HAI_1", triggered=True)]), mlflow="local")
        except ValueError as error:
            return [] if "mlflow=aws" in str(error) else [f"the refusal does not name the aws lane: {error}"]
        return [f"a drift run on the local lane built training requests: {requests}"]


def several_drifted_types_share_one_run(folder: Path) -> Problems:
    with loaded_dag() as module:
        requests = decide(module, bucket_with([record("HAI_4", triggered=True), record("HAI_1", triggered=True), record("HAI_2")]))
        types = requests[0]["conf"]["hai_types"] if requests else None
        return [] if len(requests) == 1 and types == ["HAI_1", "HAI_4"] else [f"expected one run for HAI_1 and HAI_4, got {types} in {len(requests)} requests"]


def quiet_records_start_nothing(folder: Path) -> Problems:
    with loaded_dag() as module:
        requests = decide(module, bucket_with([record("HAI_1"), record("HAI_2", status="skipped"), record("HAI_3")]))
        return [] if requests == [] else [f"quiet records started {requests}"]


def a_run_with_nothing_to_monitor_starts_nothing(folder: Path) -> Problems:
    """Every type unmonitored: the marker names no record. The run ends well and the trigger is skipped."""
    with loaded_dag() as module:
        requests = decide(module, bucket_with([]))
        return [] if requests == [] else [f"an empty marker started {requests}"]


def a_model_without_a_production_model_trains_every_type(folder: Path) -> Problems:
    """Q11: a model with no Production model yet trains. Nothing is monitored (the marker names no record) and the gate's decision starts the training."""
    with loaded_dag() as module:
        requests = decide(module, bucket_with([], unmonitored=dict.fromkeys(HAI_TYPES, "no_production_model")))
        types = requests[0]["conf"]["hai_types"] if requests else None
        return [] if len(requests) == 1 and types == list(HAI_TYPES) else [f"expected one run for every type, got {types} in {len(requests)} requests"]


def a_changed_schema_trains_beside_a_drifted_type(folder: Path) -> Problems:
    """Q11: a type whose feature schema changed trains though its model is not monitored, and shares the run with a drifted type; a quiet type does not."""
    with loaded_dag() as module:
        records = [record("HAI_1", triggered=True), record("HAI_3")]
        requests = decide(module, bucket_with(records, unmonitored={"HAI_2": "feature_schema_changed"}))
        types = requests[0]["conf"]["hai_types"] if requests else None
        return [] if len(requests) == 1 and types == ["HAI_1", "HAI_2"] else [f"expected one run for HAI_1 and HAI_2, got {types} in {len(requests)} requests"]


def a_skip_names_its_reason(folder: Path) -> Problems:
    """A model that does not train starts nothing and says why for each type, in the task log."""
    lines: list[str] = []

    class Capture(logging.Handler):
        def emit(self, entry: logging.LogRecord) -> None:
            lines.append(entry.getMessage())

    with loaded_dag() as module:
        logger = logging.getLogger(module.__name__)
        handler = Capture(level=logging.INFO)
        previous = logger.level
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        try:
            requests = decide(module, bucket_with([record("HAI_1"), record("HAI_2", status="skipped")]))
        finally:
            logger.removeHandler(handler)
            logger.setLevel(previous)
    text = " ".join(lines)
    problems = [] if requests == [] else [f"quiet records started {requests}"]
    return problems + [f"the log does not name the skip reasons not_drifted and no_new_window: {lines}"] * (
        not ("not_drifted" in text and "no_new_window" in text)
    )


def with_gate(s3: MemoryS3, **changes: Any) -> MemoryS3:
    """The bucket with the marker's gate decision changed, as a marker that was edited or written by another version would hold."""
    key = (BUCKET, f"drift/{DRIFT_RUN}/_complete.json")
    document = json.loads(s3.objects[key])
    document["gate"] = {**document["gate"], **changes}
    s3.objects[key] = json.dumps(document).encode()
    return s3


def with_marker(s3: MemoryS3, **changes: Any) -> MemoryS3:
    """The bucket with top-level fields of the marker changed, as a marker written by another version would hold."""
    key = (BUCKET, f"drift/{DRIFT_RUN}/_complete.json")
    s3.objects[key] = json.dumps({**json.loads(s3.objects[key]), **changes}).encode()
    return s3


def no_production_model_changed_to_an_unsupported_reason() -> MemoryS3:
    """A valid no-Production marker whose reason, decision and gate all say ``artifact_missing``, a reason the gate never accepts."""
    s3 = bucket_with([], unmonitored={"HAI_1": "no_production_model"})
    with_marker(s3, unmonitored={"HAI_1": "artifact_missing"})
    return with_gate(s3, decisions={"HAI_1": {"train": False, "reason": "artifact_missing"}}, train=False)


def valid_marker() -> MemoryS3:
    """A model with no Production model for HAI_1 and a quiet record for HAI_2."""
    return bucket_with([record("HAI_2")], unmonitored={"HAI_1": "no_production_model"})


def decision_repeats(folder: Path) -> Problems:
    with loaded_dag() as module:
        s3 = bucket_with([record("HAI_1", triggered=True)])
        return [] if decide(module, s3) == decide(module, s3) else ["two decisions on the same records differ, so a retry could start a second run"]


def refuses(detail: str, build: Callable[[], MemoryS3]) -> Callable[[Path], Problems]:
    def case(folder: Path) -> Problems:
        with loaded_dag() as module:
            try:
                requests = decide(module, build())
            except RuntimeError as error:
                return [] if detail in str(error) else [f"the error does not say {detail!r}: {error}"]
            return [f"records that must be refused started {requests}"]

    return case


def a_changed_record() -> MemoryS3:
    s3 = bucket_with([record("HAI_1", triggered=True)])
    s3.objects[(BUCKET, f"drift/{DRIFT_RUN}/drift_HAI_1.json")] = (
        json.dumps(record("HAI_1", triggered=True, population="primary") | {"triggered": True, "extra": 1}) + "\n"
    ).encode()
    return s3


def launch_step(folder: Path) -> Problems:
    """The one step reaches the launch library as the template drift_run for this DAG, in real mode, naming the model whose set it checks."""
    sent: list[Any] = []

    def launched(request: Any, context: Any, selections: Any) -> str:
        sent.append(request)
        return "succeeded"

    with loaded_dag() as module:
        instance = SimpleNamespace(try_number=1, dag_id="drift_monitoring_dag", task_id="drift_run", run_id="manual")
        CONTEXT["context"] = {"params": {"run_id": DRIFT_RUN, "mode": "real", "mlflow": "aws", "model": "m2", "features_run": FEATURES_RUN}, "ti": instance}
        with mock.patch.object(module.dag_runtime, "launch_module", return_value=SimpleNamespace(launch_step=launched)):
            module.dag.task_dict["drift_run"].function()
    shape = [(request.template, request.dag_id, request.mode, request.training) for request in sent]
    return [] if shape == [("drift_run", "drift_monitoring_dag", "real", {"model": "m2"})] else [f"the launch library received {shape}"]


def cases() -> list[tuple[str, Callable[[Path], Problems]]]:
    stale = [record("HAI_1", run="drift_elastic_net_ffffffff", triggered=True)]
    return [
        ("dag_declares_the_graph", graph_is_declared),
        ("dag_one_drifted_type_starts_one_training_run", one_drifted_type_starts_one_training_run),
        ("dag_drifted_types_share_one_training_run", several_drifted_types_share_one_run),
        ("dag_quiet_records_start_nothing", quiet_records_start_nothing),
        ("dag_nothing_to_monitor_starts_nothing", a_run_with_nothing_to_monitor_starts_nothing),
        ("dag_decision_repeats", decision_repeats),
        ("dag_a_model_without_a_production_model_trains_every_type", a_model_without_a_production_model_trains_every_type),
        ("dag_a_changed_schema_trains_beside_a_drifted_type", a_changed_schema_trains_beside_a_drifted_type),
        ("dag_a_skip_names_its_reason", a_skip_names_its_reason),
        ("dag_refuses_a_marker_without_the_gate_decision", refuses("no gate decision", lambda: bucket_with([record("HAI_1", triggered=True)], gated=False))),
        (
            "dag_refuses_the_gate_decision_of_another_model",
            refuses("another model", lambda: bucket_with([record("HAI_1", triggered=True)], model="m4")),
        ),
        (
            "dag_refuses_a_gate_that_skips_a_drifted_type",
            refuses("inconsistent", lambda: with_gate(bucket_with([record("HAI_1", triggered=True)]), train_types=[], train=False)),
        ),
        (
            "dag_refuses_a_gate_that_trains_a_quiet_type",
            refuses("inconsistent", lambda: with_gate(bucket_with([record("HAI_1")]), train_types=["HAI_1"], train=True)),
        ),
        ("dag_refuses_an_unsupported_unmonitored_reason", refuses("artifact_missing", no_production_model_changed_to_an_unsupported_reason)),
        (
            "dag_refuses_a_gate_without_its_decisions",
            refuses("inconsistent", lambda: with_gate(bucket_with([], unmonitored={"HAI_1": "no_production_model"}), decisions={}, train=False)),
        ),
        (
            "dag_refuses_a_marker_that_monitors_a_type_without_a_record",
            refuses("monitored", lambda: with_marker(bucket_with([record("HAI_1")]), monitored=["HAI_1", "HAI_2"])),
        ),
        (
            "dag_refuses_a_record_of_a_type_the_marker_does_not_monitor",
            refuses("monitored", lambda: with_marker(bucket_with([record("HAI_1"), record("HAI_2")]), monitored=["HAI_1"])),
        ),
        (
            "dag_refuses_a_type_named_monitored_and_unmonitored",
            refuses("names a type twice", lambda: with_marker(bucket_with([record("HAI_1")]), unmonitored={"HAI_1": "no_production_model"})),
        ),
        (
            "dag_refuses_an_unknown_unmonitored_type",
            refuses("unknown type", lambda: with_marker(bucket_with([record("HAI_1")]), unmonitored={"HAI_9": "no_production_model"})),
        ),
        ("dag_refuses_a_train_flag_that_contradicts_the_types", refuses("inconsistent", lambda: with_gate(valid_marker(), train=False))),
        ("dag_refuses_a_train_flag_on_a_gate_that_trains_nothing", refuses("inconsistent", lambda: with_gate(bucket_with([record("HAI_1")]), train=True))),
        (
            "dag_refuses_a_decision_that_trains_a_quiet_type",
            refuses(
                "inconsistent",
                lambda: with_gate(
                    valid_marker(), decisions={"HAI_1": {"train": True, "reason": "no_production_model"}, "HAI_2": {"train": True, "reason": "drifted"}}
                ),
            ),
        ),
        (
            "dag_refuses_a_decision_with_another_reason",
            refuses(
                "inconsistent",
                lambda: with_gate(
                    valid_marker(), decisions={"HAI_1": {"train": True, "reason": "drifted"}, "HAI_2": {"train": False, "reason": "not_drifted"}}
                ),
            ),
        ),
        (
            "dag_refuses_skip_reasons_that_differ_from_the_decisions",
            refuses("inconsistent", lambda: with_gate(bucket_with([record("HAI_1")]), skip_reasons={})),
        ),
        ("dag_refuses_a_gate_of_another_schema", refuses("inconsistent", lambda: with_gate(bucket_with([record("HAI_1")]), schema=2))),
        (
            "dag_refuses_a_gate_that_trains_a_type_no_record_covers",
            refuses("inconsistent", lambda: with_gate(valid_marker(), train_types=["HAI_1", "HAI_3"])),
        ),
        ("dag_refuses_a_record_of_another_run", refuses("another run", lambda: bucket_with(stale))),
        ("dag_refuses_a_run_without_its_marker", refuses("not complete", lambda: bucket_with([record("HAI_1")], marker=False))),
        ("dag_refuses_a_marker_of_another_run", refuses("another run", lambda: bucket_with([record("HAI_1")], marker_run="drift_elastic_net_ffffffff"))),
        ("dag_refuses_a_changed_record", refuses("changed", a_changed_record)),
        ("dag_refuses_an_unreadable_record", refuses("unreadable", lambda: bucket_with([record("HAI_1")], raw={"drift_HAI_2.json": b"{not json"}))),
        ("dag_refuses_a_skipped_record_that_triggers", refuses("inconsistent", lambda: bucket_with([record("HAI_1", status="skipped", triggered=True)]))),
        (
            "dag_refuses_a_sensitivity_record_that_triggers",
            refuses("population", lambda: bucket_with([record("HAI_1", triggered=True, population="sensitivity")])),
        ),
        ("dag_step_reaches_the_launch_library", launch_step),
        ("dag_aws_decision_reaches_a_training_request", aws_decision_reaches_a_training_request),
        ("dag_local_lane_is_refused_by_name", local_lane_is_refused_by_name),
        ("dag_decide_reads_the_launch_bucket", decide_reads_the_launch_bucket),
    ]
