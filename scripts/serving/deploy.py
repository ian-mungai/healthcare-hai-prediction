"""The serving deploy action: write the active deployment of one HAI type from the production alias and an optional candidate.

It reads the registry and never moves an alias. Promotion is the owner's, through the promotion DAG. A canary deploy names a candidate version and a
share. A rollback names none and writes a deployment with the stable version only. The deployment id is the run id, so a retry with the same
parameters is a no-op and a retry with other parameters is refused. Every refusal happens before any write.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, cast

from scripts.serving.canary import HAI_TYPES, RUN_ID, Deployment, DeploymentInvalid
from scripts.serving.manifests import ManifestConflict, ManifestStore
from scripts.training import registry as model_registry

CONTRACT = Path(__file__).resolve().parents[2] / "data_contracts/training/model_contract.json"


class DeployRefused(ValueError):
    """The deployment cannot be written; nothing was written."""


class ReadOnlyRegistry(Protocol):
    def search_model_versions(self, filter_string: str = "") -> Any: ...

    def get_model_version_by_alias(self, name: str, alias: str) -> Any: ...


@dataclass(frozen=True)
class Outcome:
    status: Literal["written", "unchanged"]
    deployment: Deployment


def run(
    *,
    registry: ReadOnlyRegistry,
    manifests: ManifestStore,
    hai_type: str,
    candidate_version: str | None,
    percent: int,
    mode: str,
    deployment_id: str,
) -> Outcome:
    if mode not in ("canary", "rollback"):
        raise DeployRefused("the mode must be canary or rollback")
    if hai_type not in HAI_TYPES or not RUN_ID.fullmatch(deployment_id):
        raise DeployRefused("the HAI type or the deployment id is malformed")
    contract = json.loads(CONTRACT.read_text())
    name = contract["registered_models"][hai_type]
    production = _production(registry, name)
    if mode == "rollback":
        if candidate_version is not None or percent != 0:
            raise DeployRefused("a rollback names no candidate and no percent")
    else:
        if candidate_version is None or not re.fullmatch(r"[1-9][0-9]{0,18}", candidate_version):
            raise DeployRefused("a canary names a candidate version")
        if candidate_version == production:
            raise DeployRefused("the candidate is the production version")
        found = [version for version in _versions(registry, name) if str(version.version) == candidate_version]
        if not found:
            raise DeployRefused(f"the registered model has no version {candidate_version}")
        if not found[0].tags.get(contract["completion_tag"]):
            raise DeployRefused(f"version {candidate_version} has no completed bundle")
    try:
        deployment = Deployment(hai_type, deployment_id, production, candidate_version, percent)
    except DeploymentInvalid as error:
        raise DeployRefused(str(error)) from None
    current = manifests.read_active(hai_type)
    if current is not None:
        try:
            existing = Deployment.parse(current.body)
        except DeploymentInvalid:
            existing = None
        if existing is not None and existing.deployment_id == deployment_id:
            if (existing.candidate_version, existing.percent) == (candidate_version, percent):
                return Outcome("unchanged", existing)
            raise DeployRefused("this deployment id already holds other parameters")
    body = deployment.dump()
    try:
        manifests.put_history(hai_type, deployment_id, body)
        manifests.replace_active(hai_type, body, None if current is None else current.token)
    except ManifestConflict as error:
        raise DeployRefused(str(error)) from None
    return Outcome("written", deployment)


def _production(store: ReadOnlyRegistry, name: str) -> str:
    version = model_registry.champion(cast(model_registry.RegistryStore, store), name)
    if version is None:
        raise DeployRefused("the registered model has no production version")
    return str(version.version)


def _versions(store: ReadOnlyRegistry, name: str) -> list[Any]:
    return model_registry.versions_of(cast(model_registry.RegistryStore, store), name)
