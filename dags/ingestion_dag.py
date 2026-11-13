"""Run the Run 3 publisher once on the host: download every non-personal unit of the tracked queue and store each capture under its capture root.

One job, one chain: preflight (the run kind, the capture root and the code digest are checked, nothing is launched), job (the publisher_host task on the
host through the direct ECS launch library), evidence (its launch record says the run container passed) and gate. Nothing is staged for the run but its code
archive, which carries the tracked inputs. The DAG run names the kind (``bootstrap``, which skips every unit that has a completion record, or ``monthly``)
and the capture root (``raw/``, or one ``ingestion_test/<run id>/`` folder for a test) as the launch parameters, and the owner triggers the DAG by hand with
the run's ``run_id``. The monthly production schedule, the plan rebuilds and holds, the daily retries of failed files and the start of a bronze DAG are
later slices.
"""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_job_chain

with DAG(
    dag_id="ingestion_dag",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["orchestration", "ingestion"],
    params={
        "run_id": Param(type="string", pattern=r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"),
        "mode": Param("real", type="string", enum=["real"]),
        "run_kind": Param("bootstrap", type="string", enum=["bootstrap", "monthly"]),
        "capture_root": Param("raw/", type="string", pattern=r"^(raw/|ingestion_test/[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}/)$(?![\s\S])"),
    },
    default_args={"retries": 0},
) as dag:
    build_job_chain("publisher_run", family="publisher")
