"""Deploy a challenger beside the production model as a canary, or the production version alone (stable), through the direct ECS launch library.

The promotion chain of ``research_dag`` starts both deploys itself. The form is the same closed request: the infection type, the mode and, for a canary, the
research run whose record names the challenger. It names no version and no share: the launch preflight derives the version and the job table fixes the share.
"""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_job_chain
from orchestration.training_contract import HAI_TYPES, RESEARCH_RUN, SERVE_MODES, run_id_pattern

with DAG(
    dag_id="serving_deploy_dag",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["serving"],
    params={
        "run_id": Param(type="string", pattern=f"^{run_id_pattern('serving')}$"),
        "mode": Param("real", type="string", enum=["real"]),
        "hai_type": Param(type="string", enum=list(HAI_TYPES)),
        "deploy_mode": Param(type="string", enum=list(SERVE_MODES)),
        # The research run whose record names the canary's candidate; a stable deploy needs none.
        "research_run": Param("latest", type="string", pattern=f"^(latest|{RESEARCH_RUN})$"),
    },
    default_args={"retries": 0},
) as dag:
    build_job_chain("serve_deploy", family="serve")
