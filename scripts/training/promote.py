"""The promote action: move the ``production`` alias of one registered model to the requested version, or back to an earlier one.

The approval of the owner is checked before the job starts (the broker reads the staged record), so this action changes the registry only for the one version
the request names. A forward promotion needs a version whose promotion check passed. A rollback needs a version that was in production before, which the version
tag ``promoted_run`` shows. Setting the alias to the version it already names changes nothing, so a repeat in a new task is safe.

One run at a time moves a model. The action takes the lease of the model (``promote_lease``) before it stores the intent, keeps it through the alias move and
any recovery, and ``aws_run.publish_receipt`` closes it once the receipt is durable. A second run for the same model is refused with ``promotion_busy`` and
never touches the registry, whatever its timing. Only the holder observes production, so the version it records as ``previous_version`` is the true one.

Before the alias moves, the action stores the intent of the run in the data bucket (``promote_receipt``). A retry in a new task finds its own lease, reads that
intent and so keeps the version production held before the first attempt. A retry that finds the receipt of the run returns it and changes nothing, whatever
production names now. Otherwise the action moves the alias only when production still names the version before the promotion, finishes without a move when
it already names the promoted version and refuses any other state with the lease still held. A request that differs from the stored intent is refused before
any registry write. A run the owner abandoned (``promote_lease resolve``) is refused.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import Any

from orchestration import training_contract
from scripts.training import job_actions, job_inputs, promote_lease, promote_receipt, registry, run_tracking
from scripts.training.job_failure import JobFailure

PROMOTED_TAG = "promoted_run"
VERDICT_TAG = "promotion_verdict"
RECORD = "promote.json"


def parameters_of(path: Any) -> dict[str, Any]:
    """The closed parameters of a promotion run with the AWS MLflow lane."""
    try:
        raw = json.loads(path.read_bytes())
        return training_contract.parameters_for("model_promotion_dag", raw, ("aws",))
    except (OSError, ValueError) as error:
        raise JobFailure("parameters_refused", f"the promotion parameters are unusable: {error}") from None


def check_version(version: Any, mode: str, number: str) -> None:
    """Refuse a forward promotion of an unpassed version and a rollback to a version that never ran in production."""
    tags = dict(getattr(version, "tags", {}) or {})
    if mode == "promote" and tags.get(VERDICT_TAG) != "pass":
        raise JobFailure("version_not_promotable", f"version {number} has the verdict {tags.get(VERDICT_TAG, 'none')}, not pass")
    if mode == "rollback" and PROMOTED_TAG not in tags:
        raise JobFailure("rollback_target_never_promoted", f"version {number} was never in production")


def storage() -> tuple[Any, str]:
    """The S3 client of the task and the data bucket that holds the promotion records."""
    bucket = os.environ.get("DATA_BUCKET_NAME", "")
    if not bucket:
        raise JobFailure("receipt_bucket_missing", "DATA_BUCKET_NAME is not set")
    return promote_receipt.s3_client(), bucket


def completed_receipt(client: Any, bucket: str, request: Mapping[str, Any]) -> bytes | None:
    """The stored receipt when the run has completed, checked against the request and the intent, else None. Nothing is written.

    The listing of the run's records decides absence. A run the owner abandoned, a receipt without its intent and a receipt that differs from the one the
    intent and the request produce are refused.
    """
    run = str(request["run_id"])
    try:
        names = promote_receipt.records_of(client, bucket, run)
        if promote_receipt.ABANDONED in names:
            raise JobFailure("promotion_abandoned", f"run {run} was abandoned by the owner and never starts again. Production was not changed")
        if promote_receipt.RECORD not in names:
            return None
        if promote_receipt.INTENT not in names:
            raise JobFailure("promotion_conflict", f"{promote_receipt.key_of(run)} exists without an intent. Production was not changed")
        intent = promote_receipt.stored_intent(client, bucket, run)
        stored = promote_receipt.fetch(client, bucket, promote_receipt.key_of(run))
    except promote_receipt.ReceiptError as error:
        raise JobFailure("promotion_state_unknown", f"cannot tell whether run {run} already completed: {error}. Production was not changed") from None
    if any(intent[name] != request[name] for name in promote_receipt.PROMOTION_FIELDS) or stored != promote_receipt.canonical(
        {**request, "previous_version": intent["previous_version"]}
    ):
        raise JobFailure("promotion_conflict", f"{promote_receipt.key_of(run)} already holds another promotion. Production was not changed")
    return stored


def hold(client: Any, bucket: str, request: Mapping[str, Any]) -> promote_lease.Lease:
    """Take the lease of the model for the run, or resume the lease the run holds. Another holder refuses the run before it reads the registry."""
    try:
        return promote_lease.acquire(client, bucket, str(request["model"]), str(request["run_id"]))
    except promote_lease.Busy as busy:
        raise JobFailure(
            "promotion_busy",
            f"{busy}. Production was not changed. Retry run {busy.holder} to finish it, or the owner resolves it with promote_lease",
        ) from None
    except promote_receipt.ReceiptError as error:
        raise JobFailure("promotion_lease_failed", f"{error}. Production was not changed") from None


def settle(client: Any, bucket: str, request: Mapping[str, Any], observed: str | None) -> dict[str, Any]:
    """Store the intent of the run before any registry write and return it, the stored one when the run was here before."""
    try:
        return promote_receipt.open_intent(client, bucket, str(request["run_id"]), request, observed)[0]
    except promote_receipt.RequestConflict as error:
        raise JobFailure("promotion_conflict", f"{error}. Production was not changed") from None
    except promote_receipt.ReceiptError as error:
        raise JobFailure("promotion_intent_failed", f"{error}. Production was not changed") from None


def inspect(store: Any, request: Mapping[str, Any]) -> str | None:
    """Refuse a request the registry cannot serve and return the version production names, or None."""
    name, number = request["model"], request["version"]
    target = next((item for item in registry.versions_of(store, name) if str(item.version) == number), None)
    if target is None:
        raise JobFailure("version_missing", f"{name} has no version {number}")
    current = registry.champion(store, name)
    check_version(target, request["mode"], number)
    return None if current is None else str(current.version)


def reconcile(store: Any, request: Mapping[str, Any], intent: Mapping[str, Any], observed: str | None) -> bytes:
    """Bring the registry to the promotion the request names, from the intent and the version production names, and return the receipt bytes."""
    name, number, run = request["model"], request["version"], str(request["run_id"])
    previous = intent["previous_version"]
    if observed not in (previous, number):
        raise JobFailure("promotion_conflict", f"production names {observed or 'no version'}, not {previous or 'no version'} (before) or {number}")
    if previous != number:
        if observed != number:
            store.set_registered_model_alias(name, registry.CHAMPION_ALIAS, number)
        store.set_model_version_tag(name, number, PROMOTED_TAG, run)
    return promote_receipt.canonical({**request, "previous_version": previous})


def run(context: Any, record: Mapping[str, Any]) -> None:
    """Move the alias for the version the parameters name and record what changed."""
    with job_actions.guarded("promote"):
        parameters = parameters_of(context.parameters)
        contract = job_inputs.read_json(job_inputs.ROOT / "data_contracts/training/model_contract.json", "model_contract")
        name = contract["registered_models"][parameters["hai_type"]]
        number, mode = parameters["model_version"], parameters["promotion_mode"]

        def plan() -> dict[str, bytes]:
            request = {"schema": 1, "run_id": context.run, "model": name, "version": number, "mode": mode}
            client, bucket = storage()
            receipt = completed_receipt(client, bucket, request)
            if receipt is not None:
                return {RECORD: receipt}
            uri = os.environ.get("MLFLOW_TRACKING_URI", "")
            if not uri:
                raise JobFailure("mlflow_uri_missing", "MLFLOW_TRACKING_URI is not set")
            try:
                store: Any = run_tracking.open_store(uri)
                inspect(store, request)
                hold(client, bucket, request)
                observed = inspect(store, request)
                receipt = reconcile(store, request, settle(client, bucket, request, observed), observed)
            except (JobFailure, ImportError):
                raise
            except run_tracking.TrackingError as error:
                raise JobFailure(error.name, error.detail) from None
            except Exception as error:
                raise JobFailure("mlflow_failed", f"{type(error).__name__}: {error}") from None
            return {RECORD: receipt}

        job_actions.run_step(context, record, "promote", {RECORD}.__contains__, plan)
