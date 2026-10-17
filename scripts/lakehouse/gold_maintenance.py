"""Gold maintenance: snapshot expiry and orphan-file removal that keep every protected snapshot set.

A reader that wants a published set takes a reservation first, then reads the newest ``publish_runs`` row, then pins the set (a registration). Maintenance
never expires a snapshot of a set that a reservation or a registration names.

The launcher registry owns the protection and the fence around it. At the admission of a maintenance job it writes the protected file this module reads
and, in the same registry transaction, takes the maintenance fence; the fence refuses every new reservation and registration until the attempt of the job
has ended. The file therefore stays true for the whole run, up to and including the expiry call:

    {"schema": 1,
     "fence": {"run_id": "<this maintenance run>", "attempt_id": "<the attempt that holds the fence>", "taken_at": "<UTC time>"},
     "items": [
        {"kind": "registration", "id": "<reader>", "run_id": "<a committed run ID>"},
        {"kind": "pending", "id": "<reader>", "since": "<UTC time the reservation was taken>"}
     ]}

The module accepts no other shape: a missing or unknown field, a fence of another run, an absent file or a file that changes while the job runs stops it
before any expiry. A registration names a committed run. A pending reservation does not name a set yet, so it protects the set that was the newest at the
time it was taken (less a margin for clock difference) and every set committed after.

Beyond the protected sets maintenance always keeps: the newest ``retain_sets`` committed sets (the newest is the published head), every branch and tag head of
a table (a candidate in flight is a branch) and every snapshot newer than the expiry cutoff. Orphan removal never uses a cutoff newer than one day (the lock
domain keeps a publish and maintenance apart, but a file of a running write is unreferenced until it commits).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from scripts.lakehouse.gold_publish import MODES, Context, Plan, ident, load_plan, published_runs, table_exists, table_refs, write_record
from scripts.lakehouse.gold_publish_record import READER_NAME, RUN_ID, PublishError

SCHEMA = 1
TOP_KEYS = frozenset({"schema", "fence", "items"})
FENCE_KEYS = frozenset({"run_id", "attempt_id", "taken_at"})
ITEM_KEYS = {"registration": frozenset({"kind", "id", "run_id"}), "pending": frozenset({"kind", "id", "since"})}
ATTEMPT_ID = re.compile(r"[A-Za-z0-9_-]{1,64}")
MARGIN_SECONDS = 60
MIN_ORPHAN_AGE = timedelta(days=1)
LIST_SNAPSHOTS = "SELECT snapshot_id, committed_at FROM {table}.snapshots ORDER BY committed_at"
EXPIRE = "CALL {catalog}.system.expire_snapshots(table => '{namespace}.{name}', older_than => TIMESTAMP '1970-01-01 00:00:00', snapshot_ids => ARRAY({ids}))"
ORPHANS = "CALL {catalog}.system.remove_orphan_files(table => '{namespace}.{name}', older_than => TIMESTAMP '{older_than}')"


@dataclass(frozen=True)
class Item:
    """One reader reservation or registration from the launcher registry."""

    kind: str
    id: str
    run_id: str | None = None
    since: datetime | None = None


@dataclass(frozen=True)
class Protected:
    """The registry's protected file: the items and the fence (the maintenance run and the attempt that holds it) they were written under."""

    items: tuple[Item, ...] = ()
    fence_run: str | None = None
    fence_attempt: str | None = None


@dataclass
class Hooks:
    """A case's way to act between the plan and the expiry, as a second process would."""

    after_plan: Callable[[], None] | None = None
    log: list[str] = field(default_factory=list)


def parse_time(text: str) -> datetime:
    value = datetime.fromisoformat(text)
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def timestamp(value: Any, what: str) -> datetime:
    """A UTC time from a protected-file field, or a refusal."""
    if not isinstance(value, str):
        raise PublishError(f"the protected file's {what} is not a time")
    try:
        return parse_time(value)
    except ValueError as error:
        raise PublishError(f"the protected file's {what} is not a time") from error


def load_protected(path: Path | None) -> Protected:
    """The registry's protected file. No file, an unreadable file and any shape but the one in the module docstring refuse the run."""
    if path is None:
        raise PublishError("no protected file is named: maintenance runs only under the registry's fence")
    try:
        document = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        raise PublishError(f"the protected file {path.name} cannot be read") from error
    if not isinstance(document, dict) or set(document) != TOP_KEYS:
        raise PublishError(f"the protected file {path.name} does not have exactly the fields {sorted(TOP_KEYS)}")
    if document["schema"] != SCHEMA:
        raise PublishError(f"the protected file {path.name} has another schema")
    fence = document["fence"]
    if not isinstance(fence, dict) or set(fence) != FENCE_KEYS:
        raise PublishError(f"the protected file {path.name} has no complete fence")
    if not isinstance(fence["run_id"], str) or not RUN_ID.fullmatch(fence["run_id"]) or not ATTEMPT_ID.fullmatch(str(fence["attempt_id"])):
        raise PublishError(f"the protected file {path.name} has a fence with a bad run or attempt")
    timestamp(fence["taken_at"], "fence time")
    if not isinstance(document["items"], list):
        raise PublishError(f"the protected file {path.name} holds no list of items")
    items = []
    for raw in document["items"]:
        keys = ITEM_KEYS.get(str(raw.get("kind"))) if isinstance(raw, dict) else None
        if keys is None or set(raw) != keys or not isinstance(raw["id"], str) or not READER_NAME.fullmatch(raw["id"]):
            raise PublishError(
                "the protected file holds an item that is neither a registration of a run nor a pending reservation, or has a field it does not define"
            )
        if raw["kind"] == "registration":
            if not isinstance(raw["run_id"], str) or not RUN_ID.fullmatch(raw["run_id"]):
                raise PublishError("the protected file holds a registration that names no run")
            items.append(Item("registration", raw["id"], run_id=raw["run_id"]))
        else:
            items.append(Item("pending", raw["id"], since=timestamp(raw["since"], "reservation time")))
    return Protected(tuple(items), str(fence["run_id"]), str(fence["attempt_id"]))


def kept_runs(runs: Sequence[Mapping[str, Any]], protected: Protected, retain_sets: int) -> set[str]:
    """The run IDs whose snapshot sets must stay: the newest sets, every registered set and the sets a pending reservation may read."""
    known = [str(run["run_id"]) for run in runs]
    keep = set(known[-retain_sets:]) if retain_sets > 0 else set(known[-1:])
    for item in protected.items:
        if item.kind == "registration":
            if item.run_id not in known:
                raise PublishError(f"registration {item.id} names run {item.run_id}, which is not a committed set: maintenance stops")
            keep.add(str(item.run_id))
        elif item.since is not None:
            floor = item.since - timedelta(seconds=MARGIN_SECONDS)
            at_or_before = [index for index, run in enumerate(runs) if parse_time(str(run["committed_at"])) <= floor]
            start = at_or_before[-1] if at_or_before else 0
            keep |= set(known[start:])
    return keep


def protected_snapshots(plan: Plan, runs: Sequence[Mapping[str, Any]], protected: Protected, refs: Mapping[str, Mapping[str, int]]) -> dict[str, set[int]]:
    """For each table the snapshot IDs maintenance must not expire."""
    keep = kept_runs(runs, protected, plan.retain_sets)
    result: dict[str, set[int]] = {name: set(heads.values()) for name, heads in refs.items()}
    for run in runs:
        if str(run["run_id"]) in keep:
            for table, snapshot in dict(run["snapshot_set"]).items():
                result.setdefault(str(table), set()).add(int(snapshot))
    return result


def gather_refs(ctx: Context) -> dict[str, dict[str, int]]:
    return {name: table_refs(ctx, name) for name in [*ctx.plan.tables, ctx.plan.runs_table] if table_exists(ctx, name)}


def candidates_to_expire(ctx: Context, name: str, cutoff: datetime, keep: set[int]) -> list[int]:
    rows = ctx.sql(LIST_SNAPSHOTS, table=ctx.fq(name)).collect()
    return [int(row[0]) for row in rows if row[1].astimezone(UTC) < cutoff and int(row[0]) not in keep]


def maintain(
    ctx: Context,
    source: Callable[[], Protected],
    now: datetime | None = None,
    hooks: Hooks | None = None,
    orphan_now: datetime | None = None,
) -> dict[str, Any]:
    """Expire unprotected snapshots older than the plan's cutoff, then remove orphan files older than the orphan cutoff.

    ``source`` returns the registry's protected file; it must be bound to this maintenance run. The file is read before the plan and again before each table's
    expiry: the fence promises it does not change, so a difference stops the run before that table's expiry. Returns counts per table. ``now`` and
    ``orphan_now`` are the clocks of the two cutoffs; only the end-to-end cases set them apart (a case moves the expiry clock forward to make fresh snapshots
    old, and the orphan clock stays real because Iceberg refuses a cutoff newer than 24 hours).
    """
    moment = now or datetime.now(UTC)
    hooks = hooks or Hooks()
    cutoff = moment - timedelta(days=ctx.plan.expire_after_days)
    orphan_cutoff = (orphan_now or moment) - timedelta(days=ctx.plan.orphan_after_days)
    if orphan_cutoff > datetime.now(UTC) - MIN_ORPHAN_AGE:
        raise PublishError("the orphan-file cutoff is newer than one day")
    protected = source()
    if protected.fence_run != ctx.run_id:
        raise PublishError("the protected file is not bound to this maintenance run: maintenance stops")
    runs = published_runs(ctx)
    planned: dict[str, list[int]] = {}
    refs = gather_refs(ctx)
    first = protected_snapshots(ctx.plan, runs, protected, refs)
    for name in refs:
        planned[name] = candidates_to_expire(ctx, name, cutoff, first.get(name, set()))
    if hooks.after_plan is not None:
        hooks.log.append("after_plan")
        hooks.after_plan()
    expired: dict[str, int] = {}
    for name in refs:
        if source() != protected:
            raise PublishError("the protected file changed while maintenance runs: the fence was not held, maintenance stops before this expiry")
        keep = protected_snapshots(ctx.plan, published_runs(ctx), protected, gather_refs(ctx)).get(name, set())
        doomed = sorted(set(planned[name]) - keep)
        if doomed:
            ctx.sql(EXPIRE, name=ident(name), ids=", ".join(str(int(snapshot)) for snapshot in doomed))
        expired[name] = len(doomed)
    orphans: dict[str, int] = {}
    for name in refs:
        removed = ctx.sql(ORPHANS, name=ident(name), older_than=orphan_cutoff.strftime("%Y-%m-%d %H:%M:%S")).collect()
        orphans[name] = len(removed)
    return {
        "step": "maintenance",
        "run_id": ctx.run_id,
        "fence_attempt": protected.fence_attempt,
        "expired": expired,
        "orphans_removed": orphans,
        "kept_runs": sorted(kept_runs(published_runs(ctx), protected, ctx.plan.retain_sets)),
    }


def main(argv: Sequence[str] | None = None) -> int:
    """The maintenance job: ``--run`` names this maintenance run, ``--protected`` the registry's fenced file (required)."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--run", required=True)
    parser.add_argument("--mode", required=True, choices=MODES)
    parser.add_argument("--protected", required=True)
    parser.add_argument("--publish-dir", default="/workspace/out/publish")
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    plan = load_plan()
    protected_path = Path(args.protected)
    try:
        from scripts.lakehouse.session import spark_session

        spark = spark_session("gold_maintenance")
        try:
            ctx = Context(spark, plan, args.run, args.mode, Path(args.publish_dir), "", needs_sealed=False)
            result = maintain(ctx, lambda: load_protected(protected_path))
            write_record(ctx.path(f"maintenance_{args.run}.json"), result)
            sys.stdout.write(json.dumps(result) + "\n")
        finally:
            spark.stop()
    except PublishError as error:
        sys.stderr.write(f"gold maintenance: {error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
