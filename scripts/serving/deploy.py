"""The serving deploy action: write the active deployment of one HAI type from the production alias and an optional candidate.

It reads the registry and never moves an alias. The promotion chain moves it through the promote job. A canary deploy names a candidate version, a share, a
lifetime in hours and the research run the candidate comes from, and records its write time and its expiry: the API serves the canary only until the expiry.
The first deployment of one research canary (research run, HAI type and candidate version) stores its write time and expiry in a binding that is written once.
Every later deployment of that canary, under any deployment id, takes the stored times and never a new lifetime, and is refused once the expiry has passed.
A retry never extends the lifetime, and a retry names its research canary: the saved deployment must belong to that canary's binding and hold its times. A
deployment written before the binding existed belongs to the research canary of its deterministic chain id, and no other lifetime of the candidate is
guessed: when a lifetime exists that no research canary can be shown to own, the deploy is refused. A stable deploy
names none and has no expiry: it writes a deployment with the production version only and ends a promotion's rollout. The deployment id is the run id, and each
deployment carries a sequence number one above the one it replaces. A retry with the same parameters is a no-op while that deployment is active and equals the
recorded one. It finishes the deployment if it stopped before the active write, and it is refused once a later deployment replaced it. A retry with other
parameters is refused. Every refusal happens before any write. After a write the deploy reads the active deployment back and succeeds only when it is the one
written.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, cast

from orchestration.training_contract import chain_run_id
from scripts.serving.canary import HAI_TYPES, MAX_TTL_HOURS, MAX_TTL_SECONDS, RUN_ID, Deployment, DeploymentInvalid
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
    canary_ttl_hours: int | None = None,
    now: int | None = None,
    research_run: str | None = None,
) -> Outcome:
    if mode not in ("canary", "stable"):
        raise DeployRefused("the mode must be canary or stable")
    if hai_type not in HAI_TYPES or not RUN_ID.fullmatch(deployment_id):
        raise DeployRefused("the HAI type or the deployment id is malformed")
    contract = json.loads(CONTRACT.read_text())
    name = contract["registered_models"][hai_type]
    production = _production(registry, name)
    written_at, expires_at = None, None
    if mode == "stable":
        if candidate_version is not None or percent != 0:
            raise DeployRefused("a stable deploy names no candidate and no percent")
        if canary_ttl_hours is not None or research_run is not None:
            raise DeployRefused("a stable deploy has no expiry and no research run")
    else:
        if research_run is None or not RUN_ID.fullmatch(research_run):
            raise DeployRefused("a canary names the research run its candidate comes from")
        if type(canary_ttl_hours) is not int or not 1 <= canary_ttl_hours <= MAX_TTL_HOURS:
            raise DeployRefused(f"a canary has a lifetime of a whole number of hours from 1 to {MAX_TTL_HOURS}")
        if type(now) is not int or now < 1:
            raise DeployRefused("a canary is written at a time given as whole seconds since the epoch")
        written_at, expires_at = now, now + canary_ttl_hours * 3600
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
        deployment = Deployment(hai_type, deployment_id, production, candidate_version, percent, written_at=written_at, expires_at=expires_at)
    except DeploymentInvalid as error:
        raise DeployRefused(str(error)) from None
    current = manifests.read_active(hai_type)
    active = _parse(current.body) if current is not None else None
    recorded = manifests.read_history(hai_type, deployment_id)
    try:
        if recorded is not None:
            prior = _parse(recorded)
            if research_run is not None:
                _belongs(manifests, hai_type, research_run, candidate_version, prior)
            return _retry(manifests, hai_type, current, active, prior, candidate_version, percent)
        if research_run is not None:
            written_at, expires_at = _first_lifetime(manifests, deployment, research_run, now, active)
        deployment = Deployment(
            hai_type, deployment_id, production, candidate_version, percent, 1 if active is None else active.sequence + 1, written_at, expires_at
        )
        body = deployment.dump()
        manifests.put_history(hai_type, deployment_id, body)
        manifests.replace_active(hai_type, body, None if current is None else current.token)
    except DeploymentInvalid as error:
        raise DeployRefused(str(error)) from None
    except ManifestConflict as error:
        raise DeployRefused(str(error)) from None
    _confirm(manifests, hai_type, deployment)
    return Outcome("written", deployment)


def _first_lifetime(manifests: ManifestStore, wanted: Deployment, research_run: str, now: Any, active: Deployment | None) -> tuple[int, int]:
    """The write time and expiry of a canary deployment: the ones the first deployment of its research canary stored, which this deployment stores itself when
    it is the first. A deployment under another id is refused once the stored expiry has passed, so no run opens a new traffic window. With no binding, the
    lifetime of a deployment written before the binding existed is carried into it, and none is made up while one exists that cannot be attributed."""
    seed = (wanted.deployment_id, wanted.written_at, wanted.expires_at)
    if manifests.read_canary(wanted.hai_type, research_run, str(wanted.candidate_version)) is None:
        seed = _unbound_lifetime(manifests, wanted, research_run, active) or seed
        if seed[0] != wanted.deployment_id and now >= seed[2]:
            raise DeployRefused(f"the lifetime of this research canary ended at {seed[2]}, so no deployment opens a new traffic window")
    document = {
        "version": 1,
        "hai_type": wanted.hai_type,
        "research_run": research_run,
        "candidate_version": wanted.candidate_version,
        "deployment_id": seed[0],
        "written_at": seed[1],
        "expires_at": seed[2],
    }
    try:
        first: Any = json.loads(manifests.bind_canary(wanted.hai_type, research_run, str(wanted.candidate_version), _dump(document)))
    except (ValueError, ManifestConflict):
        raise DeployRefused("the binding of this research canary cannot be read") from None
    written, expires = _check_binding(first, document)
    if first["deployment_id"] != wanted.deployment_id and now >= expires:
        raise DeployRefused(f"the lifetime of this research canary ended at {expires}, so no deployment opens a new traffic window")
    return written, expires


def _check_binding(first: Any, document: dict[str, Any]) -> tuple[int, int]:
    """The write time and expiry of a binding, refused unless it names this canary and holds a lifetime within the bound."""
    written, expires = (first.get("written_at"), first.get("expires_at")) if isinstance(first, dict) else (None, None)
    if (
        not isinstance(first, dict)
        or set(first) != set(document)
        or any(first[name] != document[name] for name in ("version", "hai_type", "research_run", "candidate_version"))
        or not isinstance(first["deployment_id"], str)
        or type(written) is not int
        or type(expires) is not int
        or not 0 < expires - written <= MAX_TTL_SECONDS
    ):
        raise DeployRefused("the binding of this research canary is malformed")
    return written, expires


def _holds_lifetime(deployment: Deployment) -> bool:
    return deployment.written_at is not None or deployment.expires_at is not None


def _unbound_lifetime(manifests: ManifestStore, wanted: Deployment, research_run: str, active: Deployment | None) -> tuple[str, int, int] | None:
    """The first deployment id, write time and expiry of a research canary that has no binding because its deployment predates it, or None when no lifetime
    of the candidate exists. The deterministic chain id of the research run names the first deployment. A lifetime held by the active deployment of the
    candidate under another id cannot be attributed to this research canary, so it refuses the deploy."""
    chain_id = chain_run_id(research_run, wanted.hai_type, "canary_deploy")
    recorded = manifests.read_history(wanted.hai_type, chain_id)
    if recorded is not None:
        first = _parse(recorded)
        if first is None:
            raise DeployRefused("the first deployment of this research canary cannot be read, so its lifetime cannot be established")
        if first.candidate_version == wanted.candidate_version and _holds_lifetime(first):
            return chain_id, *_lifetime_of(first)
    if active is not None and active.candidate_version == wanted.candidate_version and _holds_lifetime(active):
        raise DeployRefused(
            f"the active deployment {active.deployment_id} holds a lifetime of candidate {wanted.candidate_version} that no research canary is shown to own"
        )
    return None


def _lifetime_of(deployment: Deployment) -> tuple[int, int]:
    written, expires = deployment.written_at, deployment.expires_at
    if type(written) is not int or type(expires) is not int or not 0 < expires - written <= MAX_TTL_SECONDS:
        raise DeployRefused(f"the lifetime of deployment {deployment.deployment_id} is malformed, so it cannot be established")
    return written, expires


def _belongs(manifests: ManifestStore, hai_type: str, research_run: str, candidate: str | None, prior: Deployment | None) -> None:
    """Refuse a retry unless the saved deployment belongs to the named research canary and holds the times of its lifetime: the times of its binding, or, with
    no binding, its own when it is the first deployment of the research canary by its chain id. A deployment that holds no lifetime serves no canary traffic."""
    if prior is None or candidate is None or not _holds_lifetime(prior):
        return
    raw = manifests.read_canary(hai_type, research_run, candidate)
    if raw is not None:
        try:
            first: Any = json.loads(raw)
        except ValueError:
            raise DeployRefused("the binding of this research canary cannot be read") from None
        written, expires = _check_binding(
            first,
            {
                "version": 1,
                "hai_type": hai_type,
                "research_run": research_run,
                "candidate_version": candidate,
                "deployment_id": "",
                "written_at": 0,
                "expires_at": 0,
            },
        )
    elif prior.deployment_id == chain_run_id(research_run, hai_type, "canary_deploy"):
        written, expires = _lifetime_of(prior)
    else:
        raise DeployRefused(f"deployment {prior.deployment_id} holds a lifetime that research canary {research_run} is not shown to own")
    if (prior.written_at, prior.expires_at) != (written, expires):
        raise DeployRefused(f"deployment {prior.deployment_id} does not hold the lifetime of research canary {research_run}: it ended at {expires}")


def _dump(document: dict[str, Any]) -> bytes:
    return (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()


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
