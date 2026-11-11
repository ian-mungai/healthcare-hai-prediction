"""Compare the models of one base run and promote what passes: research, then the canary check, the promotion and the owner notice of each type's challenger.

``training_dag`` starts this DAG after each training run of m1, m2 or m4 on a base run, with the run ID derived from the base run. The DAG ends skipped
until all three models have passed training runs on that base. Then one research task launches in the promote family and the chain task runs, for each
infection type with a challenger, the canary check, the promotion, the release of the model set's held online load and the owner notice
(``orchestration/promotion_chain.py``). No step needs the owner's approval. A second run for the same base joins the launches of the first and starts
nothing again.
"""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_research_tasks
from orchestration.training_contract import HAI_TYPES

with DAG(
    dag_id="research_dag",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["training", "promotion"],
    params={
        # The research run ID, derived from the base run (orchestration.training_contract.research_run_id); a hand start names its own.
        "run_id": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
        "mode": Param("real", type="string", enum=["real"]),
        # The base run whose models research compares: no default, so a form trigger must name it.
        "base_run": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
        "population": Param("primary", type="string", enum=["primary", "sensitivity"]),
        "hai_types": Param(list(HAI_TYPES), type="array", minItems=1, maxItems=6, uniqueItems=True, items={"enum": list(HAI_TYPES)}),
        "mlflow": Param("aws", type="string", enum=["local", "aws"]),
    },
    default_args={"retries": 0},
) as dag:
    build_research_tasks()
