"""The model API's core: authenticate, limit, validate, route, read features under the serving contract, predict and record.

It knows no web framework. The FastAPI adapter (``api.py``) passes the key header, the request id header and the parsed body here and sends the reply
back. One process holds, for each HAI type, the stable model and at most one canary model. Each version has its own state, so a version that fails to
load or keeps failing affects only itself. A canary that fails three requests in a row is ejected until the next deployment. Its requests fall back to
the stable model, and the row records both versions.
"""

from __future__ import annotations

import logging
import re
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any, Literal, Protocol

from scripts.features import serving_contract_client as contract
from scripts.serving import canary
from scripts.serving.keys import KeyRing
from scripts.serving.limiter import RateLimiter
from scripts.serving.manifests import ManifestStore
from scripts.serving.predictions import PredictionBuffer

LOG = logging.getLogger(__name__)
SAFE_REQUEST_ID = re.compile(r"[A-Za-z0-9_.:-]{1,64}")
FIELDS = frozenset(("hai_type", "ccn", "window_year"))
Model = Callable[[Mapping[str, Any]], float]


@dataclass(frozen=True)
class LoadedModel:
    version: str
    references: tuple[str, ...]
    schema_hash: str
    predict: Model


class ModelLoader(Protocol):
    def load(self, hai_type: str, version: str) -> LoadedModel: ...

    def release(self, model: LoadedModel) -> None: ...


@dataclass(frozen=True)
class Reply:
    status: int
    body: dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class Slot:
    version: str
    model: LoadedModel | None
    state: Literal["loaded", "failed", "ejected"]
    failures: int = 0


@dataclass(frozen=True)
class Active:
    deployment: canary.Deployment
    token: str
    stable: Slot
    candidate: Slot | None


def refusal(status: int, error: str, **extra: Any) -> Reply:
    return Reply(status, {"error": error, **extra})


class ModelService:
    def __init__(
        self,
        *,
        keys: KeyRing,
        limiter: RateLimiter,
        read_marker: contract.MarkerReader,
        read_features: contract.FeatureReader,
        manifests: ManifestStore,
        loader: ModelLoader,
        buffer: PredictionBuffer,
        clock: Callable[[], float],
        new_id: Callable[[], str],
        hai_types: Sequence[str],
        refresh_seconds: float,
        failure_limit: int,
    ) -> None:
        self.keys, self.limiter, self.buffer = keys, limiter, buffer
        self._read_marker, self._read_features = read_marker, read_features
        self._manifests, self._loader, self._clock, self._new_id = manifests, loader, clock, new_id
        self._hai_types, self._refresh_seconds, self._failure_limit = tuple(hai_types), refresh_seconds, failure_limit
        self._active: dict[str, Active] = {}
        self._lock = threading.Lock()
        self._next_refresh = 0.0
        self.refresh()

    def deployment(self, hai_type: str) -> canary.Deployment:
        return self._active[hai_type].deployment

    def refresh(self) -> None:
        """Read each active deployment and load or release models to match. A record that cannot be read or parsed leaves the last good deployment serving."""
        with self._lock:
            self._next_refresh = self._clock() + self._refresh_seconds
            for hai_type in self._hai_types:
                try:
                    self._refresh_one(hai_type)
                except Exception as error:
                    LOG.warning("deployment refresh failed for %s (%s); the last good deployment keeps serving", hai_type, type(error).__name__)

    def _refresh_one(self, hai_type: str) -> None:
        stored = self._manifests.read_active(hai_type)
        before = self._active.get(hai_type)
        if stored is None or (before is not None and before.token == stored.token):
            return
        deployment = canary.Deployment.parse(stored.body)
        if deployment.hai_type != hai_type:
            raise canary.DeploymentInvalid("the record belongs to another HAI type")
        held = {} if before is None else {slot.version: slot for slot in (before.stable, before.candidate) if slot is not None}

        def slot_for(version: str) -> Slot:
            if version in held and held[version].state != "ejected":
                return held[version]
            try:
                return Slot(version, self._loader.load(hai_type, version), "loaded")
            except Exception as error:
                LOG.warning("model version %s of %s did not load (%s)", version, hai_type, type(error).__name__)
                return Slot(version, None, "failed")

        stable = slot_for(deployment.stable_version)
        candidate = None if deployment.candidate_version is None else slot_for(deployment.candidate_version)
        self._active[hai_type] = Active(deployment, stored.token, stable, candidate)
        kept = {slot.version for slot in (stable, candidate) if slot is not None}
        for version, slot in held.items():
            if version not in kept and slot.model is not None:
                self._loader.release(slot.model)

    def health(self) -> Reply:
        body: dict[str, Any] = {}
        for hai_type in self._hai_types:
            active = self._active.get(hai_type)
            if active is None:
                body[hai_type] = {"deployment_id": None, "stable": None, "candidate": None}
                continue
            body[hai_type] = {
                "deployment_id": active.deployment.deployment_id,
                "stable": active.stable.version if active.stable.state == "loaded" else active.stable.state,
                "candidate": None if active.candidate is None else (active.candidate.version if active.candidate.state == "loaded" else active.candidate.state),
            }
        ready = all(self._active.get(h) is not None and self._active[h].stable.state == "loaded" for h in self._hai_types)
        return Reply(200 if ready else 503, body)

    def predict(self, api_key: str | None, request_id: str | None, payload: Any) -> Reply:
        key_id = self.keys.identify(api_key)
        if key_id is None:
            return refusal(401, "unauthorized")
        wait = self.limiter.check(key_id)
        if wait is not None:
            return Reply(429, {"error": "rate_limited"}, {"Retry-After": str(int(wait))})
        parsed = self._parse(payload)
        if isinstance(parsed, Reply):
            return parsed
        hai_type, ccn, window_year = parsed
        rid = request_id if request_id is not None and SAFE_REQUEST_ID.fullmatch(request_id) else self._new_id()
        if self._clock() >= self._next_refresh:
            self.refresh()
        active = self._active.get(hai_type)
        if active is None:
            return refusal(503, "no_deployment", request_id=rid)
        routed = canary.route(active.deployment, ccn, window_year)
        slot, fallback = active.stable, False
        if routed == "canary":
            if active.candidate is not None and active.candidate.state == "loaded":
                slot = active.candidate
            else:
                fallback = True
        try:
            answered = self._answer(slot, ccn, window_year)
            if answered is None and routed == "canary" and slot is not active.stable:
                slot, fallback = active.stable, True
                answered = self._answer(slot, ccn, window_year)
        except contract.NotFound:
            return refusal(404, "hospital_window_not_found", request_id=rid)
        except contract.ServingError:
            return refusal(503, "features_unavailable", request_id=rid)
        if answered is None:
            return refusal(503, "model_unavailable", request_id=rid)
        value, served = answered
        candidate_version = active.candidate.version if active.candidate is not None else None
        routed_version = candidate_version if routed == "canary" and candidate_version is not None else active.stable.version
        served_by = "canary" if slot is active.candidate else "stable"
        self.buffer.add(
            {
                "request_id": rid,
                "predicted_at": datetime.now(UTC),
                "hai_type": hai_type,
                "ccn": ccn,
                "window_year": window_year,
                "prediction": value,
                "model_version": slot.version,
                "routed_version": routed_version,
                "served_by": served_by,
                "fallback": fallback,
                "deployment_id": active.deployment.deployment_id,
                "api_key_id": key_id,
                "feature_schema_sha256": served.feature_schema_hash,
                "features_as_of": date.fromisoformat(served.features_as_of),
            }
        )
        LOG.info("request %s answered by %s version %s for key %s", rid, served_by, slot.version, key_id)
        body = {
            "request_id": rid,
            "hai_type": hai_type,
            "ccn": ccn,
            "window_year": window_year,
            "prediction": value,
            "model_version": slot.version,
            "served_by": served_by,
            "fallback": fallback,
            "features_as_of": served.features_as_of,
            "feature_schema_sha256": served.feature_schema_hash,
        }
        return Reply(200, body, {"X-Request-ID": rid})

    def _answer(self, slot: Slot, ccn: str, window_year: int) -> tuple[float, contract.Served] | None:
        """Read the features for the slot's model and predict. None when the slot cannot predict. Feature store errors propagate as serving errors."""
        if slot.model is None or slot.state != "loaded":
            return None
        served = contract.serve(
            self._read_marker, self._read_features, references=slot.model.references, schema_hash=slot.model.schema_hash, ccn=ccn, window_year=window_year
        )
        try:
            value = float(slot.model.predict(served.values))
        except Exception as error:
            self._failed(slot, error)
            return None
        with self._lock:
            slot.failures = 0
        return value, served

    def _failed(self, slot: Slot, error: Exception) -> None:
        with self._lock:
            slot.failures += 1
            LOG.warning("model version %s failed a prediction (%s), %d in a row", slot.version, type(error).__name__, slot.failures)
            if slot.failures >= self._failure_limit and slot is not self._stable_slot(slot):
                slot.state = "ejected"

    def _stable_slot(self, slot: Slot) -> Slot | None:
        for active in self._active.values():
            if slot is active.stable:
                return slot
        return None

    def _parse(self, payload: Any) -> tuple[str, str, int] | Reply:
        if not isinstance(payload, dict) or set(payload) != FIELDS:
            return refusal(422, "invalid_request", detail="the body must hold hai_type, ccn and window_year only")
        hai_type, ccn, year = payload["hai_type"], payload["ccn"], payload["window_year"]
        if hai_type not in canary.HAI_TYPES:
            return refusal(422, "invalid_request", detail="unsupported hai_type")
        try:
            contract.spine_key(str(ccn) if isinstance(ccn, str) else "", year)
        except ValueError:
            return refusal(422, "invalid_request", detail="ccn or window_year is not valid")
        return hai_type, ccn, year
