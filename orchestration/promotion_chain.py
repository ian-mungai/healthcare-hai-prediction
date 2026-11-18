"""The research DAG's readiness check and the promotion chain it runs for each infection type that has a challenger.

Readiness (``missing_models``): research compares m1, m2 and m4 on one base run, so the DAG waits, skipped and not failed, until each of them has a training
run on that base whose launch ended with every acceptance check passed. The training run of a model on a base run is the chain of its features, drift and
training run IDs (``training_contract.trained_run_id``), which the launch store holds a record of.

The chain (``run_types``) reads the research record of each type from the data bucket. A type with no challenger has no chain. A type with a challenger runs
its steps, each launched once under a run of its own (``training_contract.chain_run_id``), so a retry joins what ran:

1. ``canary_deploy`` asks the serve family to deploy the challenger beside the Production model. The request is closed: the infection type and the mode canary.
   The serve preflight derives the version from the type's research record and the share and the lifetime from the job table. The deployment record
   carries the canary's expiry, and the model API serves the canary only before it.
2. ``canary_check`` judges the canary replay stored for the research run and stores its result. The chain reads that result and goes on only for ``promote``.
3. ``promote`` moves the Production alias to the challenger's version. The promote action refuses a version without a passed canary result, unless the
   registry has no Production version.
4. ``release_online`` launches the release phase of the features family for the promoted model's set and the features run the model trained on. The phase
   (``scripts.features.online_release``) reads its own hold record (``online_hold.json``) and ends ``released`` (it loaded the held online data, once),
   ``already_released`` or ``not_held`` (the run loaded itself online, so there is nothing to release), all of which pass, or it refuses by name and the launch
   fails. It runs only after the promotion, and only when the research record names a Production version or none of the key (see the first champion below).
   The chain reads no hold record: the Airflow launch role reads only ``research/*`` and may not list, and the features writer role may read the hold.
5. ``stable_deploy`` asks the serve family for a stable-only deployment (the mode stable): the serve job writes the Production version alone and reads the
   active deployment back, so the API refreshes to the promoted version and the canary share ends. It runs only after a promotion and its release. A hold, a
   failed check or a failed canary deploy deploys nothing more: the canary stops getting traffic at its expiry, with no later step.
6. ``owner_notice`` tells the owner what happened: ``promoted`` only when the promotion and the rollout both ended, ``promoted_not_rolled_out`` when the
   promotion stands but the release or the rollout failed, and ``held`` after a hold, a failed canary deploy or check, or a canary result that does not name
   this candidate.

First champion (owner decision A, Nov 15 2026): when the research record names no Production version (``promotion.production_version`` is None), the type
has nothing to run a canary beside. Steps 1 and 2 are skipped, so the chain promotes a challenger whose verdict is ``pass`` or ``pass_baseline`` (research
chooses only such a challenger) and goes on with the release check, the rollout and the notice. This is a one-time exception to the canary rule for the first
champion only. A record without the key, or one that names a version, keeps the canary.

The first champion also skips the release (owner decision B, Nov 15 2026): a first deploy has no hold record, because ``feature_sets.online_action``
returns ``load`` without a Production model, so there is nothing to release and the chain launches no release for a first champion. With a Production
version the release is launched (owner decision A, Nov 15 2026: a ``release`` phase of the features family, not a read of the hold in Airflow). A record
without the key keeps the release.

The features run of the challenger is the one its training run read, which the research record names (``features_runs``). It is not derived from the base
run, because the training runs read the run IDs the features DAGs chose. The release launch runs under the chain run of its own and names that features run
in its closed parameters (``training_contract.release_parameters_for``), so a retry joins it.

One type failing never stops another type's chain. A failed promotion is not announced, because the Production alias may have moved: the failure is the signal,
and the owner resolves it with the promotion tooling. A failed release or rollout leaves the promotion standing, is announced as such and is reported as a
failure; a release the phase refuses fails by name and stops the rollout. ``run_types`` returns every failure; the caller fails the DAG task with them.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from orchestration.training_contract import PROMOTION_MODELS, STORED_RUN, chain_run_id, stored_record_key, trained_run_id

TRAIN_JOB = "train"
Launch = Callable[[str, str, dict[str, Any]], None]
Read = Callable[[str], bytes | None]


class StepFailed(RuntimeError):
    """A launching step of the chain did not pass; the message names the cause."""


class RecordRefused(ValueError):
    """A stored record cannot be used for the chain; the message names the record and why."""


class RunStore(Protocol):
    """The launch records the readiness check reads: the claim of a run's job and the records of its attempt."""

    def run_record(self, run_id: str, job: str, event: str) -> dict[str, Any] | None: ...

    def records(self, run_id: str, job: str, attempts: Sequence[int]) -> Sequence[dict[str, Any]]: ...


@dataclass(frozen=True)
class Challenger:
    """The challenger of one infection type: the model and registered version research chose, the features run its training run read and whether the
    registry has no Production version for the type (the first champion, which has no canary)."""

    hai_type: str
    model: str
    version: str
    features_run: str
    first_champion: bool = False


def trained(store: RunStore, run_id: str) -> bool:
    """True when the training job of the run ended with every acceptance check passed."""
    claim = store.run_record(run_id, TRAIN_JOB, "claim")
    if claim is None:
        return False
    end = {record["event"]: record for record in store.records(run_id, TRAIN_JOB, [claim["attempt"]])}.get("end")
    return end is not None and end.get("outcome") == "succeeded"


def missing_models(store: RunStore, base_run: str) -> list[str]:
    """The promotion-eligible models without a passed training run on the base run, in model order."""
    return [model for model in PROMOTION_MODELS if not trained(store, trained_run_id(base_run, model))]


def parse(read: Read, key: str) -> dict[str, Any]:
    """Read a stored record that must be a JSON object."""
    body = read(key)
    if body is None:
        raise RecordRefused(f"{key} is not stored")
    try:
        document = json.loads(body)
    except ValueError:
        raise RecordRefused(f"{key} is not JSON") from None
    if not isinstance(document, dict):
        raise RecordRefused(f"{key} is not a JSON object")
    return document


def research_document(read: Read, research_run: str, hai_type: str) -> tuple[str, dict[str, Any]]:
    """The key and the document of the type's research record, which must be a record of that type."""
    key = stored_record_key("research", research_run, hai_type)
    document = parse(read, key)
    if document.get("hai_type") != hai_type:
        raise RecordRefused(f"{key} is a record of {document.get('hai_type')}")
    return key, document


def named_in(key: str, document: Mapping[str, Any]) -> tuple[str, str] | None:
    """The model and version of the challenger in a research record, or None for a type without one. A record that does not name a promotable candidate is
    refused."""
    promotion = document.get("promotion")
    challenger = promotion.get("challenger") if isinstance(promotion, dict) else None
    if challenger is None:
        return None
    model, version = (challenger.get("model"), challenger.get("version")) if isinstance(challenger, dict) else (None, None)
    if model not in PROMOTION_MODELS or not isinstance(version, str) or not version:
        raise RecordRefused(f"{key} names a challenger that is not a promotable model and version")
    return str(model), version


def challenger_named(read: Read, research_run: str, hai_type: str) -> tuple[str, str] | None:
    """The model and version of the challenger in the type's research record, or None for a type without one. A record that does not name a promotable
    candidate is refused."""
    key, document = research_document(read, research_run, hai_type)
    return named_in(key, document)


def challenger_of(read: Read, research_run: str, hai_type: str) -> Challenger | None:
    """The challenger in the type's research record with its features run and whether it is the first champion, or None for a type without a challenger.

    The features run is the one the record names for the model. A record that names none, or names a malformed one, is refused, because the hold check needs
    the real run. The challenger is the first champion only when the record carries ``production_version`` and it is None: a record without the key is of
    unknown state and keeps the canary.
    """
    key, document = research_document(read, research_run, hai_type)
    named = named_in(key, document)
    if named is None:
        return None
    runs = document.get("features_runs")
    features_run = runs.get(named[0]) if isinstance(runs, dict) else None
    if not isinstance(features_run, str) or not re.fullmatch(STORED_RUN, features_run):
        raise RecordRefused(f"{key} names no features run for {named[0]}")
    promotion = document["promotion"]
    if (
        "production_version" in promotion
        and promotion["production_version"] is not None
        and not (isinstance(promotion["production_version"], str) and promotion["production_version"])
    ):
        raise RecordRefused(f"{key} names a Production version that is not a version number or None")
    return Challenger(hai_type, named[0], named[1], features_run, "production_version" in promotion and promotion["production_version"] is None)


def canary_decision(read: Read, research_run: str, challenger: Challenger) -> str:
    """The stored decision of the type's canary check, which must name this research run, type, model and version."""
    return stored_decision(read, research_run, challenger.hai_type, challenger.model, challenger.version)


def stored_decision(read: Read, research_run: str, hai_type: str, model: str, version: str) -> str:
    """The stored decision, promote or hold, of the canary check of one research run, type, model and version."""
    key = stored_record_key("canary", research_run, hai_type)
    document = parse(read, key)
    expected = {"research_run": research_run, "hai_type": hai_type, "model": model, "version": version}
    other = {name: document.get(name) for name in expected if document.get(name) != expected[name]}
    if other:
        raise RecordRefused(f"{key} is of another candidate: {other}")
    if document.get("decision") not in ("promote", "hold"):
        raise RecordRefused(f"{key} holds the decision {document.get('decision')!r}, not promote or hold")
    return str(document["decision"])


@dataclass(frozen=True)
class Steps:
    """What the chain calls: a launch of one step under a run and a read of a stored record."""

    launch: Launch
    read: Read


def type_chain(challenger: Challenger, research_run: str, steps: Steps, failures: list[str]) -> None:
    """Run one type's chain and append what failed to ``failures``."""
    hai_type = challenger.hai_type

    def step(name: str, action: Callable[[], None]) -> bool:
        try:
            action()
        except (StepFailed, RecordRefused) as error:
            failures.append(f"{hai_type} {name}: {error}")
            return False
        return True

    def run_of(name: str) -> str:
        return chain_run_id(research_run, hai_type, name)

    def serve(mode: str, name: str) -> Callable[[], None]:
        return lambda: steps.launch("serve_deploy", run_of(name), {"hai_type": hai_type, "deploy_mode": mode})

    outcome = "held"
    decision: list[str] = []

    def canary() -> None:
        steps.launch("canary_check", run_of("canary"), {"research_run": research_run, "hai_type": hai_type, "mlflow": "aws"})
        decision.append(canary_decision(steps.read, research_run, challenger))

    if challenger.first_champion:
        decision.append("promote")
    elif step("canary deploy", serve("canary", "canary_deploy")):
        step("canary check", canary)

    def release() -> None:
        if challenger.first_champion:
            return
        steps.launch("release_online", run_of("release"), {"hai_type": hai_type, "model": challenger.model, "features_run": challenger.features_run})

    if decision == ["promote"]:
        promotion = {"hai_type": hai_type, "model_version": challenger.version, "promotion_mode": "promote", "mlflow": "aws"}
        if not step("promotion", lambda: steps.launch("promote", run_of("promote"), promotion)):
            return
        released = step("release", release)
        outcome = "promoted" if released and step("rollout", serve("stable", "stable_deploy")) else "promoted_not_rolled_out"
    notice = {"research_run": research_run, "hai_type": hai_type, "outcome": outcome, "mlflow": "aws"}
    step("owner notice", lambda: steps.launch("owner_notice", run_of("notice"), notice))


def run_types(research_run: str, hai_types: Sequence[str], *, launch: Launch, read: Read) -> list[str]:
    """Run the chain of each listed type that has a challenger and return every failure, in order. The types run one after the other, each isolated."""
    failures: list[str] = []
    steps = Steps(launch, read)
    for hai_type in hai_types:
        try:
            challenger = challenger_of(read, research_run, hai_type)
        except RecordRefused as error:
            failures.append(f"{hai_type} research record: {error}")
            continue
        if challenger is not None:
            type_chain(challenger, research_run, steps, failures)
    return failures
