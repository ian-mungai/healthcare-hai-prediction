"""Settle an unfinished launch of the direct ECS launch library on the owner's evidence, with the scheduler's own launch services and role.

The owner triggers it over the Session Manager port forward when a launch stays without its end record (its scheduler stopped after the task started):
after ``aws_tasks stop`` and an empty ``aws_tasks list``, and once the send window of the launch's claim is over. ``Launcher.owner_settle`` first cancels,
which records the end of a launch whose stopped task it finds; without a task it needs the window over and every task of the family stopped. The statement
says what the owner checked. A run of the family is refused (FamilyBusy) until the launch has its end record.
"""

from __future__ import annotations

from airflow.sdk import DAG, Param

from orchestration.dag_runtime import build_settle_task

RUN_ID = r"^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$"

with DAG(
    dag_id="launch_settle_dag",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["owner", "launch"],
    params={
        "run_id": Param(type="string", pattern=RUN_ID),
        "mode": Param("real", type="string", enum=["real"]),
        "launch_run": Param(type="string", pattern=RUN_ID),
        "job": Param(type="string", pattern=r"^[a-z]{1,40}$"),
        "statement": Param(type="string", minLength=20, maxLength=500),
    },
    default_args={"retries": 0},
) as dag:
    build_settle_task()
