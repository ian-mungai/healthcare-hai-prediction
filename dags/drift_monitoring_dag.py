"""Compare the Production models' features with their training baselines and start ``training_dag`` when enough of them drift (project decision 21).

One broker step starts the drift task on AWS. The task covers the six HAI types and publishes its records to ``s3://<data bucket>/drift/<run>/``, with the
marker ``_complete.json`` last. ``decide`` reads the marker and the records it names, refuses a missing marker, a record of another run, a changed or
inconsistent record, and builds at most one request for ``training_dag``: the types whose record says ``triggered``, the primary population, the latest
publication and ``trigger=drift:<this run>``. A run where no type has a Production model has a marker with no records and starts nothing. The trigger is mapped
over that list, so a quiet run starts nothing. The training run id and the trigger run id come from the drift run id, so a retry never starts a second training
run. The training DAG keeps its own approval gate for real runs, and promotion of a retrained model stays with the owner.
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param, get_current_context, task

from orchestration import dag_runtime
from orchestration.training_contract import HAI_TYPES, RUN_ID, parameters_for, training_run_id

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


def read_records(s3: Any, bucket: str, drift_run: str) -> list[dict[str, Any]]:
    """Read the drift records the marker of this run names and refuse a stale, changed, unreadable or inconsistent one, so that it never triggers training."""
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
        records.append(record)
    return sorted(records, key=lambda item: HAI_TYPES.index(item["hai_type"]))


def training_requests(drift_run: str, mlflow: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return one request for ``training_dag`` naming every drifted type, or none when no record triggered."""
    drifted = [record["hai_type"] for record in records if record["triggered"]]
    if not drifted:
        return []
    training = {"features_run": "latest", "population": "primary", "hai_types": drifted, "trigger": f"drift:{drift_run}", "mlflow": mlflow}
    return [
        {"trigger_run_id": f"drift__{drift_run}", "conf": {"run_id": training_run_id(drift_run), "mode": "real", **parameters_for("training_dag", training)}}
    ]


with DAG(
    dag_id="drift_monitoring_dag",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["monitoring"],
    params={
        "run_id": Param(type="string", pattern=f"^{RUN_ID}$"),
        "mode": Param("real", type="string", enum=["real"]),
        "mlflow": Param("local", type="string", enum=["local", "aws"]),
    },
    default_args={"retries": 0},
) as dag:

    @task(task_id="drift_run", retries=0, execution_timeout=dag_runtime.STEP_TIMEOUT)
    def drift_run() -> str:
        return dag_runtime.run_step("drift_run")

    @task(task_id="decide", retries=0)
    def decide() -> list[dict[str, Any]]:
        params = get_current_context()["params"]
        bucket = os.environ.get("DATA_BUCKET_NAME", "")
        if not bucket:
            raise RuntimeError("DATA_BUCKET_NAME is not set in the Airflow service environment")
        return training_requests(params["run_id"], params["mlflow"], read_records(s3_client(), bucket, params["run_id"]))

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
