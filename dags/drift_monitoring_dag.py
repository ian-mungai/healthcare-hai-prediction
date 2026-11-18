"""Check one model set's features against the model's Production baseline and start ``training_dag`` for that model when its gate says so.

Each ``features_m<n>_dag`` starts this DAG once its set is published, with the model and the run of that set (``features_run``). One step launches the drift
task on AWS through the direct ECS launch library. The task covers the six HAI types of that model and publishes its records and the gate's decision to
``s3://<data bucket>/drift/<run>/``, with the marker ``_complete.json`` last. ``decide`` reads the marker and the records it names, refuses a missing marker,
a record of another run, a changed or inconsistent record, a marker without the gate's decision, a decision of another model, a marker whose monitored types
are not the types of its records and a decision that differs in any field from the one the gate rule (``decide_drift_gate``) gives for the marker's types and
records. It builds at most one request for ``training_dag``: the model's own training run for the types the gate trains (the model drifted, its feature schema
changed or it has no Production model for the type), the primary population, the model's run as ``features_run`` and
``trigger=drift:<this run>``. A type the gate skips is logged with its reason, and a type whose check left features out of the comparison (no value in any
reference row) is logged with their names. The trigger is mapped over that list, so a model that does not train starts nothing. The training run id and the
trigger run id come from the drift run id and the model, so a retry never starts a second training run. Promotion of a retrained model is automatic through the
promotion rule and the canary. A run whose check failed does not start again: its launch is terminal, so
``trigger_dag --drift-attempt`` starts the same model run's check as a run of its own that runs the code staged for it."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import Any

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param, get_current_context, task

from orchestration import dag_runtime
from orchestration.training_contract import (
    HAI_TYPES,
    LAUNCH_LANES,
    SET_RUN,
    GateRefused,
    decide_drift_gate,
    model_training_run_id,
    parameters_for,
    run_id_pattern,
)

LOG = logging.getLogger(__name__)
MARKER = "_complete.json"


def s3_client() -> Any:
    """The S3 client of Airflow's own Amazon hook, made on first use. It signs with the credentials of the service's task role."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    return S3Hook().get_conn()


def fetch(s3: Any, bucket: str, key: str) -> bytes | None:
    """Return the object's bytes, or None when it cannot be read."""
    try:
        body: bytes = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception:
        return None
    return body


def read_marker(s3: Any, bucket: str, drift_run: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Read this run's marker and the drift records it names. Refuse a stale, changed, unreadable or inconsistent one, so that it never triggers training."""
    prefix = f"drift/{drift_run}/"
    raw = fetch(s3, bucket, prefix + MARKER)
    try:
        marker = json.loads(raw) if raw is not None else None
    except ValueError:
        marker = None
    if not isinstance(marker, dict) or not isinstance(marker.get("files"), dict):
        raise RuntimeError(f"the drift run {drift_run} is not complete: there is no readable marker at s3://{bucket}/{prefix}{MARKER}")
    if marker.get("run_id") != drift_run:
        raise RuntimeError(f"the marker at {prefix}{MARKER} belongs to another run, {marker.get('run_id')}")
    records = []
    for name, digest in sorted(marker["files"].items()):
        body = fetch(s3, bucket, prefix + name)
        if body is None or hashlib.sha256(body).hexdigest() != digest:
            raise RuntimeError(f"the drift record {name} is missing or changed after it was published")
        try:
            record = json.loads(body)
        except ValueError:
            raise RuntimeError(f"the drift record {name} is unreadable") from None
        if record.get("kind") != "drift_record" or name != f"drift_{record.get('hai_type')}.json" or record.get("hai_type") not in HAI_TYPES:
            raise RuntimeError(f"the drift record {name} is not a drift record of its HAI type")
        if not isinstance(record.get("triggered"), bool):
            raise RuntimeError(f"the drift record {name} has no triggered flag")
        if record.get("run_id") != drift_run:
            raise RuntimeError(f"the drift record {name} belongs to another run, {record.get('run_id')}")
        if record["triggered"] and (record.get("status") != "checked" or record.get("population") != "primary"):
            raise RuntimeError(f"the drift record {name} is inconsistent: only a checked record of the primary population may trigger")
        excluded_names(record)
        records.append(record)
    return marker, sorted(records, key=lambda item: HAI_TYPES.index(item["hai_type"]))


def excluded_names(record: dict[str, Any]) -> list[str]:
    """The names of the features the drift check left out of its comparison. A record whose list is missing or not a list of a name and the reason
    ``all_missing_in_reference`` is refused: it was not written by the drift check of this repository."""
    name = f"drift_{record['hai_type']}.json"
    excluded = record.get("excluded_features")
    if not isinstance(excluded, list) or any(
        not isinstance(item, dict) or set(item) != {"name", "reason"} or not isinstance(item["name"], str) or item["reason"] != "all_missing_in_reference"
        for item in excluded
    ):
        raise RuntimeError(f"the drift record {name} has no well-formed list of excluded features (a name and the reason all_missing_in_reference each)")
    return [item["name"] for item in excluded]


def gate_types(marker: dict[str, Any], model: str, records: list[dict[str, Any]]) -> tuple[list[str], dict[str, str]]:
    """Return the types the marker's gate decision trains for ``model`` and the reason of each type it skips. The reader recomputes the decision with the rule
    of the drift task (``decide_drift_gate``) from the marker's monitored types, the reasons of its unmonitored types and the records, and refuses a marker
    whose monitored types are not the types of its records, whose inputs the rule refuses (an unknown or unsupported reason, a type named twice, a missing
    record) or whose decision differs from the recomputed one in any field."""
    gate = marker.get("gate")
    if not isinstance(gate, dict) or gate.get("kind") != "drift_gate":
        raise RuntimeError(f"the marker holds no gate decision of {model}: the drift task of this run did not gate a model")
    if marker.get("model") != model or gate.get("model") != model:
        raise RuntimeError(f"the gate decision belongs to another model, {marker.get('model')}, not {model}")
    monitored, unmonitored = marker.get("monitored"), marker.get("unmonitored")
    if (
        not isinstance(monitored, list)
        or not isinstance(unmonitored, dict)
        or not all(isinstance(item, str) for item in (*monitored, *unmonitored, *unmonitored.values()))
    ):
        raise RuntimeError("the marker names its monitored types as a list and its unmonitored types as a dictionary of reasons")
    held = {record["hai_type"]: record for record in records}
    if sorted(monitored) != sorted(held):
        raise RuntimeError(f"the marker monitors {sorted(monitored)} and its drift records are of {sorted(held)}: it must hold one record per monitored type")
    try:
        expected = decide_drift_gate(model, {"monitored": monitored, "unmonitored": unmonitored}, held)
    except GateRefused as error:
        raise RuntimeError(f"the marker's monitored and unmonitored types do not support a gate decision: {error}") from None
    if gate != expected:
        raise RuntimeError(f"the gate decision is inconsistent with the drift records: it holds {gate}, the records give {expected}")
    decisions: dict[str, dict[str, Any]] = expected["decisions"]
    return expected["train_types"], {hai_type: str(item["reason"]) for hai_type, item in sorted(decisions.items()) if not item["train"]}


def training_requests(drift_run: str, mlflow: str, model: str, features_run: str, types: list[str]) -> list[dict[str, Any]]:
    """Return one request for ``training_dag`` that trains ``model`` on its own set for the given types, or none when the gate trains no type."""
    if not types:
        return []
    training = {"features_run": features_run, "population": "primary", "hai_types": types, "trigger": f"drift:{drift_run}", "mlflow": mlflow, "model": model}
    return [
        {
            "trigger_run_id": model_training_run_id(drift_run, model),
            "conf": {"run_id": model_training_run_id(drift_run, model), "mode": "real", **parameters_for("training_dag", training, LAUNCH_LANES)},
        }
    ]


with DAG(
    dag_id="drift_monitoring_dag",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["monitoring"],
    params={
        "run_id": Param(type="string", pattern=f"^{run_id_pattern('drift', qualified=True)}$"),
        "mode": Param("real", type="string", enum=["real"]),
        "mlflow": Param("aws", type="string", enum=["local", "aws"]),
        # The model whose drift this run checks and whose training it starts (orchestration.training_contract.MODELS).
        "model": Param(type="string", enum=["m1", "m2", "m3", "m4", "m5", "m6"]),
        # The run of the model's published set: the drift job reads that set (its features job must have passed) and the training run it starts fits on it.
        "features_run": Param(type="string", pattern=f"^{SET_RUN}$"),
        # The attempt of this model run's drift check: 1 is the run every model run has. The launch of a run is terminal once it ended, so a check that
        # failed starts again as attempt 2 or later, with the run ID that the attempt derives (training_contract.model_drift_run_id) and its own training run.
        "drift_attempt": Param(1, type="integer", minimum=1, maximum=99),
    },
    default_args={"retries": 0},
) as dag:

    @task(task_id="drift_run", retries=0, execution_timeout=dag_runtime.STEP_TIMEOUT, **dag_runtime.pool_settings("drift"))
    def drift_run() -> str:
        return dag_runtime.run_step("drift_run")

    @task(task_id="decide", retries=0)
    def decide() -> list[dict[str, Any]]:
        params = get_current_context()["params"]
        bucket = os.environ.get("HAI_LAUNCH_BUCKET", "")
        if not bucket:
            raise RuntimeError("HAI_LAUNCH_BUCKET (the data bucket of the launched jobs) is not set in the Airflow service environment")
        marker, records = read_marker(s3_client(), bucket, params["run_id"])
        types, skipped = gate_types(marker, params["model"], records)
        for record in records:
            if names := excluded_names(record):
                LOG.info(
                    "drift check of %s for %s did not compare %d features (all_missing_in_reference): %s",
                    params["model"],
                    record["hai_type"],
                    len(names),
                    ", ".join(names),
                )
        for hai_type, reason in skipped.items():
            LOG.info("no training of %s for %s: %s", params["model"], hai_type, reason)
        return training_requests(params["run_id"], params["mlflow"], params["model"], params["features_run"], types)

    ran = drift_run()
    requests = decide()
    ran.operator >> requests.operator
    TriggerDagRunOperator.partial(
        task_id="trigger_training",
        trigger_dag_id="training_dag",
        wait_for_completion=False,
        reset_dag_run=False,
        skip_when_already_exists=True,
        fail_when_dag_is_paused=True,
    ).expand_kwargs(requests)
