"""The launch operator on the real Amazon provider, apache-airflow-providers-amazon 9.36.0 (the version pinned in the Airflow image constraints).

Only the Airflow image can import this module. The guarantees are in ``orchestration/ecs_launch_operator.py``, which the offline cases run over a stand-in
of the provider operator. After any provider upgrade, re-read ``operators/ecs.py`` (``execute``, the four hooks and ``on_kill``) and rerun the operator cases.
"""

from __future__ import annotations

from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

from orchestration.ecs_launch_operator import LaunchGuarantees


class BrokerlessEcsRunTaskOperator(LaunchGuarantees, EcsRunTaskOperator):
    """``EcsRunTaskOperator`` with launch records, adoption, reattach, overlap refusal, a recorded deadline, acceptance checks and a confirmed stop."""
