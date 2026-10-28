"""A manual DAG that proves a LocalExecutor task reaches the host job broker with this session's token.

The task calls only the broker's health operation through
``orchestration.client``; it submits nothing and holds no credential. It has no schedule and starts paused, so it runs
only when the owner triggers it.
"""

from __future__ import annotations

from airflow.sdk import DAG, task

from orchestration.client import BrokerClient


@task
def broker_health() -> str:
    """Return the broker's answer; raise when it is unreachable or refuses the token."""
    return str(BrokerClient.from_session().health()["broker"])


with DAG(dag_id="broker_probe", schedule=None, catchup=False, tags=["orchestration"], description="Broker reachability probe (O1)"):
    broker_health()
