"""The promote action: move the ``production`` alias of one registered model to the requested version, or back to an earlier one.

No approval record is read: the rule and the canary checks decide (``auto_promotion``). This action changes the registry only for the one version the
request names. A forward promotion needs a version whose promotion check passed. A rollback needs a version that was in production before, which the version
tag ``promoted_run`` shows. Setting the alias to the version it already names changes nothing, so a repeat in a new task is safe.

One run at a time moves a model. The action takes the lease of the model (``promote_lease``) before it stores the intent, keeps it through the alias move and
any recovery, and ``aws_run.publish_receipt`` closes it once the receipt is durable. A second run for the same model is refused with ``promotion_busy`` and
never touches the registry, whatever its timing. Only the holder observes production, so the version it records as ``previous_version`` is the true one.

Before the alias moves, the action stores the intent of the run in the data bucket (``promote_receipt``). A retry in a new task finds its own lease, reads that
intent and so keeps the version production held before the first attempt. A retry that finds the receipt of the run returns it and changes nothing, whatever
production names now. Otherwise, when production already names the promoted version, the action finishes without a move. A request that differs from the
stored intent is refused before any registry write. A run the owner abandoned (``promote_owner resolve``) is refused.

The alias request is sent at most once per run (``start_attempt``). The MLflow server can commit a request after the task that sent it ended, so a request that
was sent and not seen to commit may still move production later, after the lease closed and another run promoted. The create-only attempt record is written
before the request, and only the call that created it sends. A retry that finds the record while production still names the version before the promotion cannot
tell a request that never arrived from a pending one. It sends nothing, keeps the lease and fails with ``promotion_needs_resolution`` until the owner resolves
the run, which restarts the MLflow service first. Automatic promotions of that model stay blocked until then.

Once production names the version, and before the receipt, the action writes the Production record of the infection type (``production_record``): the
version's feature set and schema hash, the model, the version, the run, its lease generation and the time, create-only under ``promote/production/``. The
features job reads it to decide whether an online set loads or holds, without MLflow access. A retry keeps the record the run wrote first.

Every mode first checks, as data only, that the model API can load the version (``check_model_data``): its ``bundle/provenance.json`` names the closure of the
model code (``model_code_closure_sha256``) that this job's own code root also has, and its three bundle files pass the data rules that the loader applies.
``scripts/model_code.py`` holds those rules for both and also fetches the three files, one deadline each. They are one UTF-8 JSON document with no repeated key
and no NaN or Infinity, the allowed keys and bounds of the feature list and the provenance, and the shape of the self-check. The check loads no model and
predicts nothing. A version that fails it is refused by name before the lease, the intent, the alias and the Production record.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from orchestration import training_contract
from scripts import model_code
from scripts.training import job_actions, job_inputs, production_record, promote_lease, promote_receipt, registry, run_tracking
from scripts.training.job_failure import JobFailure

PROMOTED_TAG = "promoted_run"
VERDICT_TAG = "promotion_verdict"
RECORD = "promote.json"
CANARY_TAGS = ("canary_verdict", "canary_version", "canary_run")
# The reason of a broken data rule, for a rule that is not its own reason (the rules of the self-check parts are: ``self_check_keys`` and the rest).
RULE_REASONS = {"feature_list": "self_check_feature_list", "provenance": "model_provenance_unreadable", "self_check_not_object": "self_check_not_json"}


def parameters_of(path: Any) -> dict[str, Any]:
    """The closed parameters of a promotion run with the AWS MLflow lane."""
    try:
        raw = json.loads(path.read_bytes())
        return training_contract.parameters_for("model_promotion_dag", raw, ("aws",))
    except (OSError, ValueError) as error:
        raise JobFailure("parameters_refused", f"the promotion parameters are unusable: {error}") from None


def canary_tags(number: str, model_run: str) -> dict[str, str]:
    """The tags a passed canary leaves on the version it judged: its verdict, the version and the MLflow run of the model it judged."""
    return dict(zip(CANARY_TAGS, ("promote", number, model_run), strict=True))


def check_canary(version: Any, number: str) -> None:
    """Refuse a promotion whose version has no passed canary result, or one for another version or another run of the model."""
    tags = dict(getattr(version, "tags", {}) or {})
    if not any(name in tags for name in CANARY_TAGS):
        raise JobFailure("canary_result_missing", f"version {number} has no canary result, so production does not move to it")
    if {name: tags.get(name) for name in CANARY_TAGS} != canary_tags(number, str(getattr(version, "run_id", ""))):
        raise JobFailure("canary_result_mismatch", f"the canary result on version {number} is not a pass for this version and its model run")


def check_version(version: Any, mode: str, number: str, current: str | None) -> None:
    """Refuse a forward promotion of an unpassed version and a rollback to a version that never ran in production.

    A version that passed against the intercept + CMI baseline (``pass_baseline``) can become only the first champion: once production names another
    version, a new version must pass against that champion. A forward promotion also needs the passed canary result that ``auto_promotion record`` tags on
    the version, bound to the version number and its model run. The one exception is the first champion (owner decision A, Nov 15 2026): while production
    names no version, a version with the verdict ``pass`` or ``pass_baseline`` moves production with no canary result, because no canary can run beside a
    Production model that does not exist. Production naming this version already (a retry after the alias moved) is accepted without a canary result, since
    the retry moves no alias.
    """
    tags = dict(getattr(version, "tags", {}) or {})
    verdict = tags.get(VERDICT_TAG, "none")
    if mode == "promote" and verdict == "pass_baseline" and current not in (None, number):
        raise JobFailure(
            "version_not_promotable",
            f"version {number} passed against the baseline only, and production already names version {current}; it must pass against that champion",
        )
    if mode == "promote" and verdict not in ("pass", "pass_baseline"):
        raise JobFailure("version_not_promotable", f"version {number} has the verdict {verdict}, not pass")
    if mode == "promote" and current not in (None, number):
        check_canary(version, number)
    if mode == "rollback" and PROMOTED_TAG not in tags:
        raise JobFailure("rollback_target_never_promoted", f"version {number} was never in production")


# The bounded single-file fetch of a run's file (a name here so that a case can replace the transport).
fetch_file = model_code.mlflow_fetch


def bundle_document(run_id: str, name: str, scratch: Path, unreadable: str, oversize: str = "", not_json: str = "") -> Any:
    """Fetch ``bundle/<name>`` of a run through the bounded fetch of ``scripts/model_code.py`` (one file, at most 1 MiB, one deadline, never a folder) and
    parse it with the data rules of the model API (one UTF-8 JSON document, no repeated key, no NaN or Infinity), or refuse with ``unreadable``
    (``oversize`` for a file over 1 MiB, ``not_json`` for text that is not such a document; each defaults to ``unreadable``). A fetch that reaches its
    deadline, or that finds the one worker slot of the shared fetch taken by an earlier cancelled call, is ``bundle_fetch_deadline``.

    The fetch, a missing or unreadable file and text that is not JSON each refuse. No detail holds a value of the file, only the class of the error.
    """
    try:
        local = model_code.fetch_regular_file(fetch_file, run_id, f"bundle/{name}", str(scratch / name), model_code.JSON_CAP)
        return model_code.read_json_file(local, f"bundle/{name}")
    except JobFailure:
        raise
    except model_code.DataRuleError as error:
        reasons = {
            "too_large": oversize or unreadable,
            "not_json": not_json or unreadable,
            "deadline": "bundle_fetch_deadline",
            "busy": "bundle_fetch_deadline",
        }
        raise JobFailure(reasons.get(error.rule, unreadable), str(error)) from None
    except Exception as error:
        raise JobFailure(unreadable, f"bundle/{name} could not be downloaded or read: {type(error).__name__}") from None


def check_closure(provenance: Any, root: Path) -> None:
    """Refuse a provenance that breaks the shape rules of the model API, one that has no closure value, one that differs from the closure of this job's own
    code root, or a root that cannot give its own value. The model API refuses the same versions at load (``ModelCodeMissing`` and ``ModelCodeMismatch``)."""
    try:
        model_code.check_provenance(provenance)
    except model_code.DataRuleError as error:
        raise JobFailure("model_provenance_unreadable", str(error)) from None
    if model_code.CLOSURE_KEY not in provenance:
        raise JobFailure(
            "model_code_closure_missing",
            f"bundle/provenance.json has no {model_code.CLOSURE_KEY}: the version was trained before the model code closure was recorded",
        )
    try:
        own = model_code.closure_digest(root)
    except (model_code.RecordError, OSError) as error:
        raise JobFailure("model_code_closure_unreadable", f"this job's model code closure cannot be computed: {type(error).__name__}: {error}") from None
    found = provenance[model_code.CLOSURE_KEY]
    if found != own:
        shown = found if isinstance(found, str) and model_code.DIGEST.fullmatch(found) else "not a digest"
        raise JobFailure("model_code_closure_mismatch", f"the version was trained with model code {shown}, and this job's model code is {own}")


def feature_references(document: Any) -> list[str]:
    """The ``references`` of a feature list that passes the model API's rules for it (the six keys, ``references`` and ``columns`` as equal lists of text of
    at most 256 characters and at most 2000 each), or refuse with ``self_check_feature_list``."""
    try:
        return list(model_code.check_feature_list(document)[0])
    except model_code.DataRuleError as error:
        raise JobFailure("self_check_feature_list", str(error)) from None


def check_self_check(document: Any, references: list[str]) -> None:
    """Refuse a self-check that breaks the model API's shape rules (``model_code.check_self_check``), by the reason of the rule. The file is data only: no
    model runs. A detail names the place, never a value."""
    try:
        model_code.check_self_check(document, references)
    except model_code.DataRuleError as error:
        raise JobFailure(RULE_REASONS.get(error.rule, error.rule), str(error)) from None


def check_model_data(version: Any, root: Path) -> None:
    """Refuse, by name, a version the model API would refuse at load because of its closure value or its self-check file, reading the files as data only.

    ``root`` is the code root of this job. Nothing is loaded, unpickled or predicted, and nothing is written.
    """
    run_id = str(getattr(version, "run_id", ""))
    with tempfile.TemporaryDirectory(prefix="promote_data_") as folder:
        scratch = Path(folder)
        check_closure(bundle_document(run_id, "provenance.json", scratch, "model_provenance_unreadable"), root)
        references = feature_references(bundle_document(run_id, "feature_list.json", scratch, "self_check_feature_list"))
        check_self_check(bundle_document(run_id, "self_check.json", scratch, "self_check_missing", "self_check_too_large", "self_check_not_json"), references)


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
            f"{busy}. Production was not changed. Retry run {busy.holder} to finish it. If that retry reports promotion_needs_resolution, "
            "the owner resolves it with promote_owner",
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


def target_of(store: Any, request: Mapping[str, Any]) -> Any:
    """The registered version the request names, or refuse with ``version_missing``."""
    name, number = request["model"], request["version"]
    target = next((item for item in registry.versions_of(store, name) if str(item.version) == number), None)
    if target is None:
        raise JobFailure("version_missing", f"{name} has no version {number}")
    return target


def inspect(store: Any, request: Mapping[str, Any]) -> str | None:
    """Refuse a request the registry cannot serve and return the version production names, or None."""
    name, number = request["model"], request["version"]
    target = target_of(store, request)
    current = registry.champion(store, name)
    observed = None if current is None else str(current.version)
    check_version(target, request["mode"], number, observed)
    return observed


def start_attempt(client: Any, bucket: str, request: Mapping[str, Any], observed: str | None) -> None:
    """Record that the alias request is about to be sent, or fail closed when an earlier attempt of the run may already have sent it."""
    run = str(request["run_id"])
    try:
        first = promote_receipt.open_attempt(client, bucket, run, request)
    except promote_receipt.ReceiptError as error:
        raise JobFailure("promotion_attempt_failed", f"{error}. Production was not changed") from None
    if not first:
        raise JobFailure(
            "promotion_needs_resolution",
            f"run {run} sent its alias request in an earlier attempt and production still names {observed or 'no version'}, so that request may still commit. "
            f"This task sent nothing and the lease of {request['model']} stays held. The owner resolves it: python -m scripts.training.promote_owner resolve "
            f"--model {request['model']} --run {run} --owner <name> --reason <text>",
        )


def reconcile(client: Any, bucket: str, store: Any, request: Mapping[str, Any], intent: Mapping[str, Any], observed: str | None) -> bytes:
    """Bring the registry to the promotion the request names, from the intent and the version production names, and return the receipt bytes."""
    name, number, run = request["model"], request["version"], str(request["run_id"])
    previous = intent["previous_version"]
    if observed not in (previous, number):
        raise JobFailure("promotion_conflict", f"production names {observed or 'no version'}, not {previous or 'no version'} (before) or {number}")
    if previous != number:
        if observed != number:
            start_attempt(client, bucket, request, observed)
            store.set_registered_model_alias(name, registry.CHAMPION_ALIAS, number)
        store.set_model_version_tag(name, number, PROMOTED_TAG, run)
    return promote_receipt.canonical({**request, "previous_version": previous})


def record_production(client: Any, bucket: str, store: Any, hai_type: str, request: Mapping[str, Any], lease: promote_lease.Lease) -> None:
    """Write the Production record of the infection type for the version production now names, or keep the one this run wrote before."""
    version = next((item for item in registry.versions_of(store, request["model"]) if str(item.version) == request["version"]), None)
    tags = dict(getattr(version, "tags", {}) or {})
    document = production_record.build(hai_type, request, lease.generation, tags, datetime.now(UTC).isoformat(timespec="seconds"))
    try:
        production_record.store(client, bucket, document)
    except promote_receipt.ReceiptError as error:
        raise JobFailure("production_record_failed", f"{error}. Production names version {request['version']}; retry the run to write its record") from None


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
                check_model_data(target_of(store, request), job_inputs.ROOT)
                lease = hold(client, bucket, request)
                observed = inspect(store, request)
                receipt = reconcile(client, bucket, store, request, settle(client, bucket, request, observed), observed)
                record_production(client, bucket, store, parameters["hai_type"], request, lease)
            except (JobFailure, ImportError):
                raise
            except run_tracking.TrackingError as error:
                raise JobFailure(error.name, error.detail) from None
            except Exception as error:
                raise JobFailure("mlflow_failed", f"{type(error).__name__}: {error}") from None
            return {RECORD: receipt}

        job_actions.run_step(context, record, "promote", {RECORD}.__contains__, plan)
