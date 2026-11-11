"""The serving deploy action: write the active deployment of one HAI type from the production alias and an optional candidate.

It reads the registry and never moves an alias. The promotion chain moves it through the promote job. A canary deploy names a candidate version and a share.
A withdraw ends the canary of one named canary deployment and keeps the stable version of the active deployment, never the alias. A stable deploy names none
and writes a deployment with the production version only: it ends a promotion's rollout. The deployment id is the run id, and each deployment carries a
sequence number one above the one it replaces. A retry with the same parameters is a no-op while that deployment is active and equals the recorded one. It
finishes the deployment if it stopped before the active write, and it is refused once a later deployment replaced it. A retry with other parameters is
refused. Every refusal happens before any write. After a write the deploy reads the active deployment back and succeeds only when it is the one written.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, cast

from scripts.serving.canary import HAI_TYPES, RUN_ID, Deployment, DeploymentInvalid
from scripts.serving.manifests import ManifestConflict, ManifestStore, Stored
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
    deployment: Deployment | None


def run(
    *,
    registry: ReadOnlyRegistry,
    manifests: ManifestStore,
    hai_type: str,
    candidate_version: str | None,
    percent: int,
    mode: str,
    deployment_id: str,
    canary_deployment_id: str | None = None,
) -> Outcome:
    if mode not in ("canary", "stable", "withdraw"):
        raise DeployRefused("the mode must be canary, stable or withdraw")
    if hai_type not in HAI_TYPES or not RUN_ID.fullmatch(deployment_id):
        raise DeployRefused("the HAI type or the deployment id is malformed")
    if mode == "withdraw":
        return _withdraw(manifests, hai_type, candidate_version, percent, deployment_id, canary_deployment_id)
    contract = json.loads(CONTRACT.read_text())
    name = contract["registered_models"][hai_type]
    production = _production(registry, name)
    if mode == "stable":
        if candidate_version is not None or percent != 0:
            raise DeployRefused("a stable deploy names no candidate and no percent")
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
    active = _parse(current.body) if current is not None else None
    recorded = manifests.read_history(hai_type, deployment_id)
    try:
        if recorded is not None:
            return _retry(manifests, hai_type, current, active, _parse(recorded), candidate_version, percent)
        deployment = Deployment(hai_type, deployment_id, production, candidate_version, percent, 1 if active is None else active.sequence + 1)
        body = deployment.dump()
        manifests.put_history(hai_type, deployment_id, body)
        manifests.replace_active(hai_type, body, None if current is None else current.token)
    except DeploymentInvalid as error:
        raise DeployRefused(str(error)) from None
    except ManifestConflict as error:
        raise DeployRefused(str(error)) from None
    _confirm(manifests, hai_type, deployment)
    return Outcome("written", deployment)


def _withdraw(manifests: ManifestStore, hai_type: str, candidate: str | None, percent: int, deployment_id: str, canary_id: str | None) -> Outcome:
    """End the canary a chain installed and nothing else. The stable version is the one the active deployment holds, which is what the API serves: the
    registry and its production alias are never read, because the alias can be ahead of the deployment. The active deployment must be the chain's own canary
    (its deployment id and its candidate version), or nothing is written."""
    if percent != 0 or candidate is None or not re.fullmatch(r"[1-9][0-9]{0,18}", candidate):
        raise DeployRefused("a withdraw names the candidate to remove and no percent")
    if canary_id is None or not RUN_ID.fullmatch(canary_id):
        raise DeployRefused("a withdraw names the canary deployment to remove")
    current = manifests.read_active(hai_type)
    active = _parse(current.body) if current is not None else None
    recorded = manifests.read_history(hai_type, deployment_id)
    try:
        if recorded is not None:
            return _retry(manifests, hai_type, current, active, _parse(recorded), None, 0)
        if active is None or active.deployment_id != canary_id or active.candidate_version != candidate:
            return Outcome("unchanged", active)
        deployment = Deployment(hai_type, deployment_id, active.stable_version, None, 0, active.sequence + 1)
        body = deployment.dump()
        manifests.put_history(hai_type, deployment_id, body)
        manifests.replace_active(hai_type, body, None if current is None else current.token)
    except DeploymentInvalid as error:
        raise DeployRefused(str(error)) from None
    except ManifestConflict as error:
        raise DeployRefused(str(error)) from None
    _confirm(manifests, hai_type, deployment)
    return Outcome("written", deployment)


def _confirm(manifests: ManifestStore, hai_type: str, wanted: Deployment) -> None:
    """Read the active deployment back and refuse unless it is the one just written, field for field: type, id, versions, percent and sequence."""
    stored = manifests.read_active(hai_type)
    found = _parse(stored.body) if stored is not None else None
    if found != wanted:
        raise DeployRefused(
            f"the active deployment is not the one written for {wanted.deployment_id}: it reads back as {None if found is None else found.deployment_id}"
        )


def _parse(raw: bytes) -> Deployment | None:
    try:
        return Deployment.parse(raw)
    except DeploymentInvalid:
        return None


def _retry(
    manifests: ManifestStore, hai_type: str, current: Stored | None, active: Deployment | None, prior: Deployment | None, candidate: str | None, percent: int
) -> Outcome:
    """A deployment id seen before: finish it if it stopped before the active write, leave it if it is active, refuse it if a later deployment replaced it."""
    if prior is None or (prior.candidate_version, prior.percent) != (candidate, percent):
        raise DeployRefused("this deployment id already holds other parameters")
    if active is not None and active.deployment_id == prior.deployment_id:
        if active != prior:
            raise DeployRefused(f"the active deployment is not the one recorded for {prior.deployment_id}: it differs in a field the retry cannot repair")
        return Outcome("unchanged", active)
    if active is not None and active.sequence >= prior.sequence:
        raise DeployRefused(f"a later deployment replaced this one (active sequence {active.sequence}, this sequence {prior.sequence})")
    if prior.sequence != (0 if active is None else active.sequence) + 1:
        raise DeployRefused("the active deployment changed since this one was recorded, so it cannot finish")
    try:
        manifests.replace_active(hai_type, prior.dump(), None if current is None else current.token)
    except ManifestConflict as error:
        raise DeployRefused(str(error)) from None
    _confirm(manifests, hai_type, prior)
    return Outcome("written", prior)


def _production(store: ReadOnlyRegistry, name: str) -> str:
    version = model_registry.champion(cast(model_registry.RegistryStore, store), name)
    if version is None:
        raise DeployRefused("the registered model has no production version")
    return str(version.version)


def _versions(store: ReadOnlyRegistry, name: str) -> list[Any]:
    return model_registry.versions_of(cast(model_registry.RegistryStore, store), name)
