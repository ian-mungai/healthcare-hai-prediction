"""Compare the Production models' features with their training baselines and start ``training_dag`` when enough of them drift (project decision 21).

Three broker steps fetch the baselines, read the current windows and run the drift check for the six HAI types. ``decide`` then reads the drift records of this
run from ``RECORDS`` and builds at most one request for ``training_dag``: the types whose record says ``triggered``, the primary population, the latest
publication and ``trigger=drift:<this run>``. The trigger is mapped over that list, so a quiet run starts nothing. The training run id and the trigger run id
come from the drift run id, so a retry never starts a second training run. A step that exits with 4 found nothing to monitor and skips the rest of the run.
The training DAG keeps its own approval gate for real runs, and promotion of a retrained model stays with the owner.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from orchestration import dag_runtime
from orchestration.training_contract import HAI_TYPES, RUN_ID, parameters_for

STEPS = ("fetch_baseline", "fetch_current", "check_drift")
NOTHING_TO_MONITOR = 4
RECORDS = Path("/opt/hai/data/drift")


def training_run_id(drift_run: str) -> str:
    """The training run's id, derived from the drift run's so that a retried decision names the same run."""
    return f"{drift_run[:16]}-{hashlib.sha256(f'training:{drift_run}'.encode()).hexdigest()[:8]}"


def read_records(folder: Path, drift_run: str) -> list[dict[str, Any]]:
    """Read the drift records of this run and refuse a stale, unreadable or inconsistent one, so that it never triggers training."""
    records = []
    for hai_type in HAI_TYPES:
        path = folder / f"drift_{hai_type}.json"
        if not path.is_file():
            continue
        try:
            record = json.loads(path.read_bytes())
        except ValueError:
            raise RuntimeError(f"the drift record {path.name} is unreadable") from None
        if record.get("kind") != "drift_record" or record.get("hai_type") != hai_type or not isinstance(record.get("triggered"), bool):
            raise RuntimeError(f"the drift record {path.name} is not a drift record of {hai_type}")
        if record.get("run_id") != drift_run:
            raise RuntimeError(f"the drift record {path.name} belongs to another run, {record.get('run_id')}")
        if record["triggered"] and (record.get("status") != "checked" or record.get("population") != "primary"):
            raise RuntimeError(f"the drift record {path.name} is inconsistent: only a checked record of the primary population may trigger")
        records.append(record)
    if not records:
        raise RuntimeError(f"no drift record of run {drift_run} in {folder}")
    return records


def training_requests(drift_run: str, mode: str, mlflow: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return one request for ``training_dag`` naming every drifted type, or none when no record triggered."""
    drifted = [record["hai_type"] for record in records if record["triggered"]]
    if not drifted:
        return []
    training = {"features_run": "latest", "population": "primary", "hai_types": drifted, "trigger": f"drift:{drift_run}", "mlflow": mlflow}
    return [{"trigger_run_id": f"drift__{drift_run}", "conf": {"run_id": training_run_id(drift_run), "mode": mode, **parameters_for("training_dag", training)}}]


def run_drift_step(name: str) -> str:
    """Submit the step to the broker. An exit code of 4 means nothing to monitor and skips the run instead of failing it."""
    try:
        return dag_runtime.run_step(name)
    except RuntimeError as error:
        if f"exit code {NOTHING_TO_MONITOR}" in str(error):
            raise AirflowSkipException(str(error)) from None
        raise


def step(name: str) -> Any:
    @task(task_id=name, retries=0, execution_timeout=dag_runtime.STEP_TIMEOUT)
    def run() -> str:
        return run_drift_step(name)

    return run().operator


with DAG(
    dag_id="drift_monitoring_dag",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["monitoring"],
    params={
        "run_id": Param(type="string", pattern=f"^{RUN_ID}$"),
        "mode": Param("fixture", type="string", enum=["fixture", "real"]),
        "mlflow": Param("local", type="string", enum=["local", "aws"]),
    },
    default_args={"retries": 0},
) as dag:
    work = [step(name) for name in STEPS]
    for before, after in zip(work, work[1:], strict=False):
        before >> after

    @task(task_id="decide", retries=0)
    def decide() -> list[dict[str, Any]]:
        params = get_current_context()["params"]
        return training_requests(params["run_id"], params["mode"], params["mlflow"], read_records(RECORDS / params["run_id"], params["run_id"]))

    requests = decide()
    work[-1] >> requests.operator
    TriggerDagRunOperator.partial(
        task_id="trigger_training",
        trigger_dag_id="training_dag",
        wait_for_completion=False,
        reset_dag_run=False,
        skip_when_already_exists=True,
        fail_when_dag_is_paused=True,
    ).expand_kwargs(requests)
