"""The promote action: move the ``production`` alias of one registered model to the requested version, or back to an earlier one.

The approval of the owner is checked before the job starts (the broker reads the staged record), so this action changes the registry only for the one version
the request names. A forward promotion needs a version whose promotion check passed. A rollback needs a version that was in production before, which the version
tag ``promoted_run`` shows. Setting the alias to the version it already names changes nothing, so a repeat in a new task is safe.

Before the alias moves, the action stores the intent of the run in the data bucket (``promote_receipt``). A retry in a new task reads that intent, so the
receipt keeps the version production held before the first attempt. A retry that finds the receipt of the run returns it and changes nothing. Otherwise the
action moves the alias only when production still names the version before the promotion, finishes without a move when it already names the promoted version
and refuses any other state. A request that differs from the stored intent is refused before any registry write.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import Any

from orchestration import training_contract
from scripts.training import job_actions, job_inputs, promote_receipt, registry, run_tracking
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


def settle(request: Mapping[str, Any], observed: str | None) -> tuple[dict[str, Any], bytes | None]:
    """Store the intent of the run before any registry write and return it with the stored receipt, when the run has completed.

    Only a retry reads the receipt. A first attempt just created the intent, so no receipt can exist and no absent read is needed.
    """
    run, bucket = str(request["run_id"]), os.environ.get("DATA_BUCKET_NAME", "")
    if not bucket:
        raise JobFailure("receipt_bucket_missing", "DATA_BUCKET_NAME is not set")
    client = promote_receipt.s3_client()
    try:
        intent, created = promote_receipt.open_intent(client, bucket, run, request, observed)
    except promote_receipt.RequestConflict as error:
        raise JobFailure("promotion_conflict", f"{error}. Production was not changed") from None
    except promote_receipt.ReceiptError as error:
        raise JobFailure("promotion_intent_failed", f"{error}. Production was not changed") from None
    if created:
        return intent, None
    try:
        return intent, promote_receipt.completed(client, bucket, run)
    except promote_receipt.ReceiptError as error:
        raise JobFailure("promotion_state_unknown", f"cannot tell whether run {run} already completed: {error}. Production was not changed") from None


def reconcile(store: Any, request: Mapping[str, Any], observed: str | None) -> bytes:
    """Bring the registry and the records of the run to the promotion the request names and return the receipt bytes."""
    name, number, run = request["model"], request["version"], str(request["run_id"])
    intent, stored = settle(request, observed)
    previous = intent["previous_version"]
    receipt = promote_receipt.canonical({**request, "previous_version": previous})
    if stored is not None:
        if stored != receipt:
            raise JobFailure("promotion_conflict", f"{promote_receipt.key_of(run)} already holds another receipt")
        return stored
    if observed not in (previous, number):
        raise JobFailure("promotion_conflict", f"production names {observed or 'no version'}, not {previous or 'no version'} (before) or {number}")
    if previous != number:
        if observed != number:
            store.set_registered_model_alias(name, registry.CHAMPION_ALIAS, number)
        store.set_model_version_tag(name, number, PROMOTED_TAG, run)
    return receipt


def run(context: Any, record: Mapping[str, Any]) -> None:
    """Move the alias for the version the parameters name and record what changed."""
    with job_actions.guarded("promote"):
        parameters = parameters_of(context.parameters)
        contract = job_inputs.read_json(job_inputs.ROOT / "data_contracts/training/model_contract.json", "model_contract")
        name = contract["registered_models"][parameters["hai_type"]]
        number, mode = parameters["model_version"], parameters["promotion_mode"]

        def plan() -> dict[str, bytes]:
            uri = os.environ.get("MLFLOW_TRACKING_URI", "")
            if not uri:
                raise JobFailure("mlflow_uri_missing", "MLFLOW_TRACKING_URI is not set")
            try:
                store: Any = run_tracking.open_store(uri)
                target = next((item for item in registry.versions_of(store, name) if str(item.version) == number), None)
                if target is None:
                    raise JobFailure("version_missing", f"{name} has no version {number}")
                current = registry.champion(store, name)
                observed = None if current is None else str(current.version)
                check_version(target, mode, number)
                request = {"schema": 1, "run_id": context.run, "model": name, "version": number, "mode": mode}
                receipt = reconcile(store, request, observed)
            except (JobFailure, ImportError):
                raise
            except run_tracking.TrackingError as error:
                raise JobFailure(error.name, error.detail) from None
            except Exception as error:
                raise JobFailure("mlflow_failed", f"{type(error).__name__}: {error}") from None
            return {RECORD: receipt}

        job_actions.run_step(context, record, "promote", {RECORD}.__contains__, plan)
