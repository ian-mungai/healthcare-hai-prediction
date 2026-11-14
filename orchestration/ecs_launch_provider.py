"""The launch operator on the real Amazon provider, apache-airflow-providers-amazon 9.39.0 (the version pinned in the Airflow image constraints).

Only the Airflow image can import this module. The guarantees are in ``orchestration/ecs_launch_operator.py``, which the offline cases run over a stand-in
of the provider operator. After any provider upgrade, re-read ``operators/ecs.py`` (``execute``, the four hooks and ``on_kill``) and rerun the operator cases.
The lineage is turned into the OpenLineage provider's ``OperatorLineage`` here, written against apache-airflow-providers-openlineage 2.20.1 and
openlineage-python 1.53.0 (the constraints pins). Those imports run only when the provider asks for lineage, so a missing provider never stops a launch.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

from orchestration.ecs_launch_operator import LaunchGuarantees


def operator_lineage(found: Mapping[str, Any]) -> Any:
    """The mapping of ``LineageSource.facets`` as an ``OperatorLineage``: datasets with their version facets and the ``haiLaunch`` run facet."""
    import attr
    from airflow.providers.openlineage.extractors import OperatorLineage
    from openlineage.client.event_v2 import Dataset
    from openlineage.client.facet_v2 import RunFacet, dataset_version_dataset

    from orchestration.ecs_lineage import FACET_SCHEMA

    @attr.define
    class HaiLaunchRunFacet(RunFacet):
        runId: str
        job: str
        attempt: int
        template: str
        boundInputRun: str
        snapshotState: str
        missingOutputs: list[dict[str, str]]
        refusal: str | None = attr.field(default=None)

        @staticmethod
        def _get_schema() -> str:
            return FACET_SCHEMA

    def datasets(items: list[Mapping[str, Any]]) -> list[Any]:
        built = []
        for item in items:
            version = item.get("facets", {}).get("version")
            facets = {"version": dataset_version_dataset.DatasetVersionDatasetFacet(datasetVersion=version["datasetVersion"])} if version else {}
            built.append(Dataset(namespace=item["namespace"], name=item["name"], facets=facets))
        return built

    launch = {key: value for key, value in found["run_facets"]["haiLaunch"].items() if not key.startswith("_")}
    return OperatorLineage(inputs=datasets(found["inputs"]), outputs=datasets(found["outputs"]), run_facets={"haiLaunch": HaiLaunchRunFacet(**launch)})


class BrokerlessEcsRunTaskOperator(LaunchGuarantees, EcsRunTaskOperator):
    """``EcsRunTaskOperator`` with launch records, adoption, reattach, overlap refusal, a recorded deadline, acceptance checks, a confirmed stop and the
    job's lineage."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)  # BaseOperatorMeta wraps __init__ only when it differs from the first superclass's, and the guard line needs the wrapper

    def openlineage_result(self, found: Mapping[str, Any]) -> Any:
        return operator_lineage(found)
