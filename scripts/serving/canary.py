"""The deployment record of one HAI type and the deterministic split between its stable and canary model versions.

A deployment names the stable version (the production alias when it was written), at most one candidate and the share of hospital-windows that go to
the candidate. The split hashes the deployment id, the HAI type and the hospital-window, so one hospital-window always meets the same version of one
deployment, in every process, and a new deployment reshuffles the groups. The share is in whole percent over 10,000 buckets.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Literal

from orchestration.serving_contract import MAX_PERCENT
from orchestration.training_contract import HAI_TYPES

RUN_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}")
SAFE_ID = re.compile(r"[A-Za-z0-9_-]{1,64}")
VERSION = re.compile(r"[1-9][0-9]{0,18}")
BUCKETS = 10_000
Route = Literal["stable", "canary"]


class DeploymentInvalid(ValueError):
    """A deployment record that does not follow the format."""


@dataclass(frozen=True)
class Deployment:
    hai_type: str
    deployment_id: str
    stable_version: str
    candidate_version: str | None
    percent: int
    sequence: int = 1

    def __post_init__(self) -> None:
        if self.hai_type not in HAI_TYPES:
            raise DeploymentInvalid("the HAI type is not supported")
        if not SAFE_ID.fullmatch(self.deployment_id):
            raise DeploymentInvalid("the deployment id is not safe text")
        if not VERSION.fullmatch(self.stable_version) or (self.candidate_version is not None and not VERSION.fullmatch(self.candidate_version)):
            raise DeploymentInvalid("a model version is not a positive integer text")
        if self.candidate_version is None and self.percent != 0:
            raise DeploymentInvalid("a deployment without a candidate has no percent")
        if self.candidate_version is not None and (isinstance(self.percent, bool) or not isinstance(self.percent, int) or not 1 <= self.percent <= MAX_PERCENT):
            raise DeploymentInvalid(f"the canary percent must be a whole number from 1 to {MAX_PERCENT}")
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) or self.sequence < 1:
            raise DeploymentInvalid("the sequence must be a positive whole number")
        if self.candidate_version == self.stable_version:
            raise DeploymentInvalid("the candidate is the stable version")

    def dump(self) -> bytes:
        document = {
            "version": 1,
            "hai_type": self.hai_type,
            "deployment_id": self.deployment_id,
            "stable_version": self.stable_version,
            "candidate_version": self.candidate_version,
            "percent": self.percent,
            "sequence": self.sequence,
        }
        return (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()

    @classmethod
    def parse(cls, raw: bytes) -> Deployment:
        try:
            document: Any = json.loads(raw)
        except ValueError:
            raise DeploymentInvalid("the deployment record is not JSON") from None
        expected = {"version", "hai_type", "deployment_id", "stable_version", "candidate_version", "percent", "sequence"}
        if not isinstance(document, dict) or set(document) != expected or document["version"] != 1:
            raise DeploymentInvalid("the deployment record has other fields than the format")
        return cls(
            document["hai_type"],
            document["deployment_id"],
            document["stable_version"],
            document["candidate_version"],
            document["percent"],
            document["sequence"],
        )


def bucket(deployment_id: str, hai_type: str, ccn: str, window_year: int) -> int:
    """The bucket from 0 to 9,999 of one hospital-window under one deployment."""
    digest = hashlib.sha256(f"{deployment_id}|{hai_type}|{ccn}|{window_year}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % BUCKETS


def route(deployment: Deployment, ccn: str, window_year: int) -> Route:
    """``canary`` when the hospital-window falls in the candidate's share, else ``stable``."""
    if deployment.candidate_version is None:
        return "stable"
    return "canary" if bucket(deployment.deployment_id, deployment.hai_type, ccn, window_year) < deployment.percent * (BUCKETS // 100) else "stable"
