"""One promotion owner per model: a create-only lease in the data bucket, under ``promote/models/<model>/``.

Two runs of the promote phase that move the same model at once would overwrite each other's alias and publish a false ``previous_version``, and a run that
died after moving the alias could move it again after another run restored its predecessor. The MLflow alias setter has no compare-and-swap, so the
exclusion is a lease that every run takes before its intent and keeps until its receipt is durable.

Records (all create-only, none is ever replaced or deleted):

- ``lease-<generation>.json`` names the run that holds generation N of the model: schema, model, generation, run_id.
- ``lease-<generation>.done.json`` closes generation N: outcome ``completed`` (written by the holder after its receipt is durable) or ``abandoned`` (written by
  the owner). The next run takes generation N+1. The model is held while its highest generation has no ``done`` record.

A run takes the lease by listing the model prefix and creating the next generation with a conditional write. Exactly one concurrent creator wins. The loser
lists again: it refuses while the lease is held, and takes the next generation when the winner has already closed it. The same run ID finding its own held
lease resumes it, so a retry in a fresh task continues from its intent. A different run is refused (``Busy``) and never reads or writes the registry.

No timer proves a run dead, so a lease is never taken over by age. The lease of a run that will never return is closed by the owner with ``resolve``, which
restarts the MLflow service first and records the production alias (``resolve`` here takes the restart and the alias read as calls; the owner command is
``promote_owner``, whose docstring has the steps). A run that sent its alias request and did not see it commit cannot be retried: its retry fails with
``promotion_needs_resolution`` and keeps the lease, because the MLflow server can still commit the earlier request (see ``promote``).

The library calls no process and starts no thread. Every S3 call has the client's connect and read limits.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from scripts.training import promote_receipt
from scripts.training.promote_receipt import ReceiptError

PREFIX = f"{promote_receipt.PREFIX}/models"
MODEL = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,80}")
KEY = re.compile(r"lease-([0-9]{10})(\.done)?\.json")
ATTEMPTS = 5


class Busy(ReceiptError):
    """Another run holds the lease of the model."""

    def __init__(self, model: str, holder: str, key: str) -> None:
        super().__init__(f"model {model} is held by run {holder} (lease {key})")
        self.model, self.holder, self.key = model, holder, key


@dataclass(frozen=True)
class Lease:
    """The lease one run holds on one model."""

    model: str
    generation: int
    run_id: str

    @property
    def key(self) -> str:
        return key_of(self.model, self.generation)


def prefix_of(model: str) -> str:
    """The key prefix of the model's lease records. Raises ReceiptError for a name that is not a plain model name."""
    if not MODEL.fullmatch(model):
        raise ReceiptError(f"{model!r} is not a model name")
    return f"{PREFIX}/{model}/"


def key_of(model: str, generation: int, done: bool = False) -> str:
    """The object key of one lease record."""
    return f"{prefix_of(model)}lease-{generation:010d}{'.done' if done else ''}.json"


def generations(client: Any, bucket: str, model: str) -> dict[int, bool]:
    """Map each generation of the model to whether it is closed. A key the library does not write raises ReceiptError."""
    prefix, found = prefix_of(model), dict[int, bool]()
    for key in promote_receipt.list_keys(client, bucket, prefix):
        match = KEY.fullmatch(key.removeprefix(prefix))
        if match is None:
            raise ReceiptError(f"{key} is not a lease record, so the lease of {model} cannot be read")
        generation = int(match[1])
        found[generation] = found.get(generation, False) or match[2] is not None
    return found


def holder_of(client: Any, bucket: str, model: str, generation: int) -> dict[str, Any]:
    """The stored record of a held generation, checked against its key."""
    key = key_of(model, generation)
    try:
        record = json.loads(promote_receipt.fetch(client, bucket, key))
    except ValueError:
        record = None
    wanted = {"schema": 1, "model": model, "generation": generation}
    if not isinstance(record, dict) or set(record) != {*wanted, "run_id"} or any(record[name] != value for name, value in wanted.items()):
        raise ReceiptError(f"{key} is not a lease of {model}")
    if not isinstance(record["run_id"], str) or not record["run_id"]:
        raise ReceiptError(f"{key} names no run")
    return dict(record)


def acquire(client: Any, bucket: str, model: str, run: str) -> Lease:
    """Take the lease of the model for the run, or resume the one the run already holds.

    Raises Busy while another run holds it, and ReceiptError when the records cannot be listed or read, or the creation stays contested. Nothing but the
    lease record is written, so a refusal leaves the registry and every other record alone.
    """
    for _ in range(ATTEMPTS):
        found = generations(client, bucket, model)
        latest = max(found, default=0)
        if latest and not found[latest]:
            holder = holder_of(client, bucket, model, latest)["run_id"]
            if holder == run:
                return Lease(model, latest, run)
            raise Busy(model, holder, key_of(model, latest))
        lease = Lease(model, latest + 1, run)
        body = promote_receipt.canonical({"schema": 1, "model": model, "generation": lease.generation, "run_id": run})
        try:
            if promote_receipt.put_once(client, bucket, lease.key, body):
                return lease
        except promote_receipt.Uncertain:
            pass
    raise ReceiptError(f"the lease of {model} was contested {ATTEMPTS} times. Nothing was changed")


def close(client: Any, bucket: str, lease: Lease, outcome: str, **details: str) -> None:
    """Close the generation once. A generation that is already closed stays as it is."""
    body = promote_receipt.canonical({"schema": 1, "model": lease.model, "generation": lease.generation, "run_id": lease.run_id, "outcome": outcome, **details})
    try:
        promote_receipt.put_once(client, bucket, key_of(lease.model, lease.generation, done=True), body)
    except promote_receipt.Uncertain as error:
        raise ReceiptError(f"the close of {lease.key} may not be stored: {error}") from None


def release(client: Any, bucket: str, model: str, run: str) -> bool:
    """Close the generation the run holds, after its receipt is durable. Return True when this call held a generation, False when the run holds none."""
    found = generations(client, bucket, model)
    latest = max(found, default=0)
    if not latest or found[latest] or holder_of(client, bucket, model, latest)["run_id"] != run:
        return False
    close(client, bucket, Lease(model, latest, run), "completed")
    return True


def held(client: Any, bucket: str, model: str) -> Lease | None:
    """The lease that is held now, or None when the model is free."""
    found = generations(client, bucket, model)
    latest = max(found, default=0)
    if not latest or found[latest]:
        return None
    return Lease(model, latest, holder_of(client, bucket, model, latest)["run_id"])


def resolve(
    client: Any, bucket: str, model: str, run: str, owner: str, reason: str, at: str, restart: Callable[[], None], alias: Callable[[], str | None]
) -> Lease:
    """Close the held lease of a run the owner has stopped for good, and fence the run so it never starts again.

    ``restart`` restarts the MLflow service and returns only when every task that ran before is stopped and the new deployment is complete. It runs before
    anything is recorded, so no request the old server accepted can commit after the close. ``alias`` then reads the production alias, which the fence records.
    Either raises ReceiptError, and the lease stays held. Raises ReceiptError too when the model is free, another run holds it, or the run already stored its
    receipt (a retry of the run closes its own lease).
    """
    lease = held(client, bucket, model)
    if lease is None:
        raise ReceiptError(f"the lease of {model} is not held, so there is nothing to resolve")
    if lease.run_id != run:
        raise ReceiptError(f"the lease of {model} is held by run {lease.run_id}, not {run}")
    if promote_receipt.RECORD in promote_receipt.records_of(client, bucket, run):
        raise ReceiptError(f"run {run} stored its receipt. Retry it and it closes its own lease")
    restart()
    production = alias()
    details = {"owner": owner, "reason": reason, "resolved_at": at, "production_alias": production or "none"}
    fence = promote_receipt.canonical({"schema": 1, "run_id": run, "model": model, "generation": lease.generation, "outcome": "abandoned", **details})
    promote_receipt.put_once(client, bucket, promote_receipt.key_of(run, promote_receipt.ABANDONED), fence)
    close(client, bucket, lease, "abandoned", **details)
    return lease


def report(client: Any, bucket: str, model: str) -> dict[str, Any]:
    """What the owner needs to decide: the generations, the lease that is held and the records of its run."""
    found = generations(client, bucket, model)
    lease = held(client, bucket, model)
    shown: dict[str, Any] = {"model": model, "generations": len(found), "held": None}
    if lease is not None:
        names = sorted(promote_receipt.records_of(client, bucket, lease.run_id))
        intent = promote_receipt.stored_intent(client, bucket, lease.run_id) if promote_receipt.INTENT in names else None
        shown["held"] = {"generation": lease.generation, "run_id": lease.run_id, "lease": lease.key, "records": names, "intent": intent}
    return shown
