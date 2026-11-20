"""Fit and evaluate one model of a training run through the training job of the direct ECS launch library.

After its release, a training run of m1 (OLS baseline), m2 (Elastic Net) or m4 (Gradient boosting) that names its base run (``base_run``) starts
``research_dag`` for that base; the DAG skips until all three models have run on it. Another model, a hand start with no base and a run that did not
pass its release start nothing. The same release also starts the catalog (``metadata_ingest_dag``, a ``start_metadata`` task, under the catalog run of the
base run) once for every base run, whichever model finishes first.
"""

from __future__ import annotations

from typing import Any

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param, get_current_context, task

from orchestration.dag_runtime import build_metadata_start, build_tasks
from orchestration.training_contract import (
    BASE_RUN,
    DRIFT_RUN,
    HAI_TYPES,
    PROMOTION_MODELS,
    SET_RUN,
    TRAINING_STEPS,
    model_choices,
    model_param_schema,
    research_requests,
    run_id_pattern,
)

with DAG(
    dag_id="training_dag",
    doc_md=f"{__doc__}\n\nModels: {model_choices()}. Research starts for {model_choices(PROMOTION_MODELS)}.",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["training"],
    params={
        "run_id": Param(type="string", pattern=f"^{run_id_pattern('training', qualified=True)}$"),
        "mode": Param("real", type="string", enum=["real"]),
        "features_run": Param("latest", type="string", pattern=f"^(latest|{SET_RUN})$"),
        "population": Param("primary", type="string", enum=["primary", "sensitivity"]),
        "hai_types": Param(list(HAI_TYPES), type="array", minItems=1, maxItems=6, uniqueItems=True, items={"enum": list(HAI_TYPES)}),
        "trigger": Param("manual", type="string", pattern=f"^(manual|drift:{DRIFT_RUN})$"),
        "mlflow": Param("aws", type="string", enum=["local", "aws"]),
        # The one model this run fits; no default, so a form trigger must choose it (orchestration.training_contract.MODELS).
        "model": Param(type="string", **model_param_schema()),
        # The base run of the model set this run trains on: research compares the models of one base run. Empty for a hand start, which starts no research.
        "base_run": Param("", type="string", pattern=f"^(|{BASE_RUN})$"),
    },
    default_args={"retries": 0},
) as dag:
    released = build_tasks(TRAINING_STEPS, "training")

    @task(task_id="research_request", retries=0)
    def research_request() -> list[dict[str, Any]]:
        params = get_current_context()["params"]
        return research_requests(params["model"], params["run_id"], params["base_run"], params["population"], params["hai_types"], params["mlflow"])

    request = research_request()
    released >> request.operator
    released >> build_metadata_start()
    TriggerDagRunOperator.partial(
        task_id="trigger_research",
        trigger_dag_id="research_dag",
        wait_for_completion=False,
        reset_dag_run=False,
        skip_when_already_exists=True,
        fail_when_dag_is_paused=True,
    ).expand_kwargs(request)
