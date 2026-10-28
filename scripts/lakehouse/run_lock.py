"""One run lock and registry per machine for every run that builds, publishes or reads the lakehouse.

Run from any checkout or worktree of this repository:

    .venv/bin/python -m scripts.lakehouse.run_lock where       # the registry folder every worktree resolves
    .venv/bin/python -m scripts.lakehouse.run_lock status      # holders, runs and readers; never a token
    .venv/bin/python -m scripts.lakehouse.run_lock acquire --domain transform --run <id> --mode fixture
    .venv/bin/python -m scripts.lakehouse.run_lock release --run <id> --owner

The registry is ``data/orchestration/registry/`` in the main checkout, found through Git's common directory, so every
worktree shares it; there is no environment override. Tests pass ``--test-registry <folder>``, which can never hold a
``real`` run. The registry is the launcher's (orchestration/launcher.py): every command is a request to the launcher, which
is the registry's only writer and is started on demand. ``acquire`` stores the run's token in the token store (a ``0700`` folder under
``Library/Application Support/hai-prediction/run_tokens/<registry_id>/``) and prints the run ID and the token file path, never the token;
the registry keeps only its SHA-256, and every later command reads the token from the store. A malformed registry stops every command and
is never reset. Nothing releases a lock automatically: a failed or stale run stays ``failed_retained`` until the owner releases it, after
its labelled containers are gone. A registry written before the launcher (schema 1) is migrated by the launcher on its first start, after
its checks (D1 section 2 item 7); this module reads schema 1 and keeps its state functions for the launcher. The launcher's state
functions here (``new_run``, ``pin_in``, ``register_in``) hold every lock predicate.
Exit codes: 0 done, 2 usage, 3 refused, 4 registry error. Failure modes 757, 768, 776, 777 and 880 to 885 are in
plans/wave0_20261010/failure_modes.md; the design is in plans/airflow_20261010/plan.md sections 4, 9.1 and 9.4 and in D1.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import secrets
import sys
import time
import uuid
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import run_command

DOMAINS = ("acquisition", "transform", "training", "serving")
MODES = ("fixture", "publish_fixture", "real")
REGISTRATION_KINDS = ("catalog_user",)
# Schema 1 is the registry the command-line tools wrote before the launcher; schema 2 is the launcher's (D1 section 2 item 7). A schema 1 registry
# is migrated only by the launcher, after its migration gate; the tools of this module refuse a schema 2 registry and ask for the launcher.
LEGACY_SCHEMA = 1
LAUNCHER_SCHEMA = 2
# Locks and readers (this module); memory reservations and service limits (memory_budget.admit) share the transactions.
SECTIONS = ("domains", "runs", "readers", "reservations", "services")
# What the launcher adds: allocations, attempts, operations, guards, registrations, request records, snapshot copies and the containers it created.
LAUNCHER_SECTIONS = ("allocations", "attempts", "operations", "guards", "registrations", "requests", "copies", "containers")
TOKEN_FOLDER = ("Library", "Application Support", "hai-prediction", "run_tokens")
LOCK_WAIT_SECONDS = 30
CONTAINER_LABEL = "hai.run_id"
REFUSED, REGISTRY_ERROR = 3, 4
PLAIN_SEGMENT = re.compile(r"[A-Za-z0-9_=,@.-]+")


class Refused(Exception):
    """The request breaks a lock rule; nothing was changed."""


class RegistryError(Exception):
    """The registry cannot be trusted (malformed, wrong identity or wrong kind); nothing was changed."""


@dataclass(frozen=True)
class Registry:
    """A registry folder and whether it is the real one or a test one."""

    path: Path
    kind: str

    @property
    def state_file(self) -> Path:
        """The registry's state, replaced atomically."""
        return self.path / "registry.json"

    @property
    def identity_file(self) -> Path:
        """The registry's identity, written once at creation [880]."""
        return self.path / "registry_id"


def main_checkout(cwd: Path) -> Path:
    """The main checkout of the repository containing ``cwd``: the parent of Git's absolute common directory [880]."""
    result = run_command("git", ["rev-parse", "--path-format=absolute", "--git-common-dir"], cwd=cwd, timeout=30)
    if result.returncode != 0:
        raise RegistryError(f"not inside a Git checkout: {result.stderr.strip()}")
    return Path(result.stdout.strip()).resolve().parent


def resolve(test_registry: str | None, cwd: Path, *, registry_root: Path | None = None) -> Registry:
    """The real registry of this machine's main checkout, or the named test registry.

    An explicit controller root preserves the real or test identity and bypasses main-checkout discovery.
    """
    if registry_root is not None:
        if not registry_root.is_absolute():
            raise RegistryError("controller registry root must be absolute")
        if test_registry is not None and Path(test_registry).resolve() != registry_root.resolve():
            raise RegistryError("controller root differs from the named test registry")
        return Registry(registry_root.resolve(), "test" if test_registry is not None else "real")
    if test_registry is not None:
        return Registry(Path(test_registry).resolve(), "test")
    return Registry((main_checkout(cwd) / "data" / "orchestration" / "registry").resolve(), "real")


def now() -> str:
    """Current UTC time in ISO format."""
    return datetime.now(UTC).isoformat()


def digest(token: str) -> str:
    """The stored form of a token [882]."""
    return hashlib.sha256(token.encode()).hexdigest()


def write_atomic(path: Path, text: str) -> None:
    """Replace ``path`` with ``text``: temporary file, fsync, rename, directory fsync [881]."""
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    folder = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(folder)
    finally:
        os.close(folder)


def read_identity(registry: Registry) -> dict[str, str]:
    """The registry's identity file, checked against the kind the caller resolved [883]."""
    try:
        identity = json.loads(registry.identity_file.read_text())
        registry_id, kind = str(identity["registry_id"]), str(identity["kind"])
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise RegistryError(f"{registry.identity_file} is not valid: {exc}") from exc
    if kind != registry.kind:
        if registry.kind == "real":
            raise RegistryError(f"{registry.path} carries a test registry marker; the real registry refuses it")
        raise RegistryError(f"{registry.path} is not a test registry")
    return {"registry_id": registry_id, "kind": kind}


def empty_state(registry_id: str, schema: int) -> dict[str, Any]:
    """A registry with no runs, in the legacy schema or the launcher's."""
    sections = SECTIONS + (LAUNCHER_SECTIONS if schema == LAUNCHER_SCHEMA else ())
    state: dict[str, Any] = {"schema_version": schema, "registry_id": registry_id, **{key: {} for key in sections}, "reservation_history": []}
    if schema == LAUNCHER_SCHEMA:
        state["launcher"] = {"stream_epoch": 0, "status": "stopped", "docker_state": "unknown", "barrier": None, "restart": None, "migrated_at": now()}
    return state


def legacy_only(state: dict[str, Any]) -> None:
    """The command-line tools of this module work on a schema 1 registry only; a launcher registry has one writer, the launcher."""
    if state["schema_version"] != LEGACY_SCHEMA:
        raise Refused("this registry belongs to the launcher; use orchestration/launcher.py")


def parse_state(data: bytes, registry_id: str, filename: str = "registry.json") -> dict[str, Any]:
    """Parse supplied registry bytes and return their state, or raise ``RegistryError`` for an invalid schema, shape or registry identity."""
    try:
        state = json.loads(data.decode("utf-8"))
        if not isinstance(state, dict) or state.get("schema_version") not in (LEGACY_SCHEMA, LAUNCHER_SCHEMA):
            raise ValueError("unknown schema")
        for key in SECTIONS + (LAUNCHER_SECTIONS if state["schema_version"] == LAUNCHER_SCHEMA else ()):
            if not isinstance(state.get(key), dict):
                raise ValueError(f"missing {key}")
        if state["schema_version"] == LAUNCHER_SCHEMA and not isinstance(state.get("launcher"), dict):
            raise ValueError("missing launcher")
        if not isinstance(state.get("reservation_history"), list):
            raise ValueError("missing reservation_history")
    except ValueError as error:
        raise RegistryError(f"{filename} is not valid: {error}") from error
    if state.get("registry_id") != registry_id:
        raise RegistryError(f"registry_id differs from {filename}: the registry was replaced")
    return state


@contextmanager
def transaction(registry: Registry, write: bool = True, create_schema: int = LEGACY_SCHEMA) -> Iterator[dict[str, Any]]:
    """Hold the registry's exclusive lock, yield its state and, when ``write`` and no error, replace it atomically.

    ``create_schema`` names the schema of a registry that does not exist yet; only the launcher creates schema 2.
    """
    registry.path.mkdir(parents=True, exist_ok=True)
    with (registry.path / "registry.lock").open("a") as lock:
        deadline = time.monotonic() + LOCK_WAIT_SECONDS
        while True:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() > deadline:
                    raise RegistryError(f"registry lock busy for {LOCK_WAIT_SECONDS} s") from None
                time.sleep(0.05)
        try:
            if not registry.identity_file.exists() and not registry.state_file.exists():
                identity = {"registry_id": str(uuid.uuid4()), "kind": registry.kind}
                write_atomic(registry.identity_file, json.dumps(identity) + "\n")
                write_atomic(registry.state_file, json.dumps(empty_state(identity["registry_id"], create_schema), indent=2) + "\n")
            identity = read_identity(registry)
            try:
                data = registry.state_file.read_bytes()
            except OSError as exc:
                raise RegistryError(f"{registry.state_file.name} is not valid: {exc}") from exc
            state = parse_state(data, identity["registry_id"], registry.state_file.name)
            original = json.dumps(state, indent=2, sort_keys=True) + "\n"
            state["_path"] = str(registry.path)
            yield state
            state.pop("_path", None)
            if write:
                after = json.dumps(state, indent=2, sort_keys=True) + "\n"
                # A transaction that changed nothing writes nothing, so the launcher's periodic passes cost no disk writes.
                if after != original:
                    write_atomic(registry.state_file, after)
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def process_start(pid: int) -> str | None:
    """The start time of ``pid`` as ``ps`` prints it, or None when the process is gone."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return None
    except PermissionError:
        pass
    result = run_command("ps", ["-o", "lstart=", "-p", str(pid)], timeout=30)
    return (result.stdout.strip() or None) if result.returncode == 0 else None


def is_stale(entry: Mapping[str, Any]) -> bool:
    """A holder or reader whose process ended or whose PID now names another process [884]."""
    started = process_start(int(entry["pid"]))
    return started is None or started != entry["pid_start"]


def run_entry(state: dict[str, Any], run_id: str) -> dict[str, Any]:
    """The run's entry, or a refusal."""
    if run_id not in state["runs"]:
        raise Refused(f"no run {run_id} in the registry")
    return dict(state["runs"][run_id])


def owned(state: dict[str, Any], run_id: str) -> dict[str, Any]:
    """The run's entry after checking the caller's token and the registry identity it was acquired in [880] [882]."""
    entry = run_entry(state, run_id)
    token = os.environ.get("RUN_LOCK_TOKEN", "")
    if not token or not secrets.compare_digest(digest(token), entry["token_sha256"]):
        raise Refused(f"token does not match run {run_id}")
    if entry["registry_path"] != state["_path"] or entry["registry_id"] != state["registry_id"]:
        raise RegistryError(f"run {run_id} was acquired in another registry (registry_id differs)")
    return entry


def prefix_segments(prefix: object) -> tuple[str, ...] | None:
    """The segments of a storage prefix, or None when it is not one: nonempty plain segments split by ``/`` with one optional final slash."""
    if not isinstance(prefix, str):
        return None
    parts = prefix.removesuffix("/").split("/")
    return tuple(parts) if all(PLAIN_SEGMENT.fullmatch(part) and part not in (".", "..") for part in parts) else None


def covers(parent: object, child: object) -> bool:
    """Whether ``child`` is ``parent`` or lies below it, compared by whole segments; False when either is not a plain prefix."""
    above, below = prefix_segments(parent), prefix_segments(child)
    return above is not None and below is not None and below[: len(above)] == above


def check_prefixes(prefixes: Iterable[object], what: str) -> None:
    """Refuse a declaration that holds a value that is not a plain prefix, naming the values."""
    bad = [repr(prefix) for prefix in prefixes if prefix_segments(prefix) is None]
    if bad:
        raise Refused(f"{what} {', '.join(bad)}: not a plain path (each segment is nonempty, uses letters, digits and _ = , @ . - and is not . or ..)")


def overlaps(left: str, right: str) -> bool:
    """Two storage prefixes overlap when one contains the other, compared by whole segments; a value that is not a plain prefix overlaps everything."""
    if prefix_segments(left) is None or prefix_segments(right) is None:
        return True
    return covers(left, right) or covers(right, left)


def active(state: dict[str, Any], domain: str) -> dict[str, Any] | None:
    """The domain's holder entry, if any run holds it."""
    run_id = state["domains"].get(domain)
    return None if run_id is None else dict(state["runs"][run_id])


def check_cross_domain(state: dict[str, Any], domain: str, write_prefixes: list[str]) -> None:
    """Acquisition may not write a prefix a transform run pinned [768]."""
    if domain != "acquisition":
        return
    holder = active(state, "transform")
    if holder is None:
        return
    for prefix in write_prefixes:
        for pinned in holder["pinned_prefixes"]:
            if overlaps(prefix, pinned):
                raise Refused(f"write prefix {prefix} overlaps {pinned}, pinned by transform run {holder['run_id']}")


def new_run(
    state: dict[str, Any],
    domain: str,
    run_id: str,
    mode: str,
    holder: str,
    pid: int,
    pid_start: str,
    write_prefixes: list[str],
    checkout: str,
    *,
    token: str | None = None,
) -> str:
    """Add a held run to ``state`` and return its token; every lock predicate is checked here [757] [768]."""
    current = active(state, domain)
    if current is not None:
        raise Refused(f"domain {domain} is held by {current['run_id']} (state {current['state']})")
    if run_id in state["runs"]:
        raise Refused(f"run ID {run_id} was used before; a retry with new inputs needs a new run ID")
    check_prefixes(write_prefixes, "declared write prefix")
    check_cross_domain(state, domain, write_prefixes)
    token = secrets.token_urlsafe(32) if token is None else token
    state["runs"][run_id] = {
        "run_id": run_id,
        "domain": domain,
        "mode": mode,
        "holder": holder,
        "token_sha256": digest(token),
        "checkout": checkout,
        "pid": pid,
        "pid_start": pid_start,
        "registry_path": state["_path"],
        "registry_id": state["registry_id"],
        "state": "held",
        "write_prefixes": sorted(write_prefixes),
        "pinned_prefixes": [],
        "registrations": [],
        "history": [{"state": "held", "at": now()}],
    }
    if state["schema_version"] == LAUNCHER_SCHEMA:
        state["runs"][run_id].update({"generation": 1, "snapshot": None, "token_file": None})
    state["domains"][domain] = run_id
    return token


def acquire(registry: Registry, domain: str, run_id: str, mode: str, holder: str, pid: int, write_prefixes: list[str]) -> dict[str, str]:
    """Take the domain's lock for a new run in one transaction and return the run ID and its token [757] [768]."""
    if registry.kind == "test" and mode == "real":
        raise Refused("real runs are refused in a test registry")
    pid_start = process_start(pid)
    if pid_start is None:
        raise Refused(f"holder process {pid} is not running")
    with transaction(registry) as state:
        legacy_only(state)
        token = new_run(state, domain, run_id, mode, holder, pid, pid_start, write_prefixes, str(Path.cwd()))
    return {"run_id": run_id, "token": token}


def enter(registry: Registry, run_id: str) -> dict[str, str]:
    """Let a nested launcher with the run's token join it [757]."""
    with transaction(registry, write=False) as state:
        entry = owned(state, run_id)
        if entry["state"] != "held":
            raise Refused(f"run {run_id} is in state {entry['state']}; only a held run accepts launches")
    return {"run_id": run_id, "domain": entry["domain"], "mode": entry["mode"]}


def pin_in(state: dict[str, Any], run_id: str, entry: dict[str, Any], prefixes: list[str]) -> None:
    """Record the bronze prefixes a held transform run reads; refused over an acquisition run's writes [768]."""
    if entry["domain"] != "transform" or entry["state"] != "held":
        raise Refused(f"only a held transform run pins prefixes; {run_id} is {entry['domain']} in state {entry['state']}")
    check_prefixes(prefixes, "pinned prefix")
    writer = active(state, "acquisition")
    for prefix in prefixes:
        for written in writer["write_prefixes"] if writer is not None else []:
            if writer is not None and overlaps(prefix, written):
                raise Refused(f"prefix {prefix} overlaps {written}, written by acquisition run {writer['run_id']}")
    state["runs"][run_id]["pinned_prefixes"] = sorted(set(entry["pinned_prefixes"]) | set(prefixes))


def pin(registry: Registry, run_id: str, prefixes: list[str]) -> None:
    """Record the bronze prefixes a transform run reads; refused over an acquisition run's writes [768]."""
    with transaction(registry) as state:
        legacy_only(state)
        pin_in(state, run_id, owned(state, run_id), prefixes)


def set_state(state: dict[str, Any], run_id: str, new: str) -> None:
    """Move a run to ``new`` and record the change."""
    state["runs"][run_id]["state"] = new
    state["runs"][run_id]["history"].append({"state": new, "at": now()})
    if new == "released" and state["domains"].get(state["runs"][run_id]["domain"]) == run_id:
        state["domains"].pop(state["runs"][run_id]["domain"])


def mark(registry: Registry, run_id: str, new: str) -> None:
    """Move a held run to ``finalizing`` or ``failed_retained``; a finalizing run may still fail [776]."""
    allowed = {"finalizing": ("held",), "failed_retained": ("held", "finalizing")}
    with transaction(registry) as state:
        legacy_only(state)
        entry = owned(state, run_id)
        if entry["state"] == new:
            return
        if entry["state"] not in allowed[new]:
            raise Refused(f"run {run_id} is in state {entry['state']}; it cannot move to {new}")
        set_state(state, run_id, new)


def check_record(run_id: str, record: str | None) -> None:
    """The durable run record must exist and name the run [885]."""
    if record is None or not Path(record).is_file():
        raise Refused(f"run record missing for {run_id}")
    try:
        named = json.loads(Path(record).read_text()).get("run_id")
    except (OSError, ValueError, AttributeError) as exc:
        raise Refused(f"run record {record} is not valid JSON: {exc}") from exc
    if named != run_id:
        raise Refused(f"run record names {named}, not {run_id}")


def release(registry: Registry, run_id: str, record: str | None, owner: bool) -> None:
    """Release a finalizing run when every predicate holds, or a failed one by the owner [776] [885].

    This is the schema 1 path. It cannot read Docker; the launcher's migration gate refuses to migrate while any container carries the
    run label, so a run released here with a container left is found there (D1 section 2 item 7).
    """
    with transaction(registry) as state:
        legacy_only(state)
        entry = owned(state, run_id)
        if entry["state"] == "released":
            return
        if entry["state"] == "failed_retained":
            if not owner:
                raise Refused(f"run {run_id} is in state failed_retained; failed_retained needs --owner")
        elif entry["state"] == "finalizing":
            check_record(run_id, record)
            if entry["registrations"]:
                raise Refused(f"run {run_id}: {entry['registrations'][0]} registration remains")
        else:
            raise Refused(f"run {run_id} is in state {entry['state']}; release needs finalizing, or failed_retained with --owner")
        set_state(state, run_id, "released")


def recover(registry: Registry, run_id: str) -> None:
    """Owner recovery of a stale or interrupted schema 1 run: keep it as failed_retained [884]; containers are the migration gate's concern."""
    with transaction(registry) as state:
        legacy_only(state)
        entry = owned(state, run_id)
        if entry["state"] in ("failed_retained", "released"):
            return
        set_state(state, run_id, "failed_retained")


def register_in(state: dict[str, Any], run_id: str, entry: dict[str, Any], kind: str, add: bool) -> None:
    """Add or remove a run's registration as a shared-service user [777]."""
    kinds = set(entry["registrations"])
    if add and entry["state"] != "held":
        raise Refused(f"run {run_id} is in state {entry['state']}; only a held run registers")
    state["runs"][run_id]["registrations"] = sorted(kinds | {kind} if add else kinds - {kind})


def register(registry: Registry, run_id: str, kind: str, add: bool) -> None:
    """Add or remove a run's registration as a shared-service user [777]."""
    with transaction(registry) as state:
        legacy_only(state)
        register_in(state, run_id, owned(state, run_id), kind, add)


def reserve(registry: Registry, reader: str, domain: str, publish_run: str, pid: int) -> None:
    """A reader announces the publish run it is about to read, before it pins the snapshot set [777]."""
    pid_start = process_start(pid)
    if pid_start is None:
        raise Refused(f"reader process {pid} is not running")
    with transaction(registry) as state:
        legacy_only(state)
        if reader in state["readers"]:
            raise Refused(f"reader {reader} already holds a reservation or registration")
        entry = {"domain": domain, "publish_run": publish_run, "snapshot_set": None, "state": "reserved", "pid": pid, "pid_start": pid_start}
        state["readers"][reader] = {**entry, "at": now()}


def register_set(registry: Registry, reader: str, snapshot_set: str) -> None:
    """Turn a reservation into a registration of the pinned snapshot set [777]."""
    with transaction(registry) as state:
        legacy_only(state)
        if reader not in state["readers"]:
            raise Refused(f"reader {reader} has no reservation")
        state["readers"][reader].update({"snapshot_set": snapshot_set, "state": "registered", "at": now()})


def deregister_reader(registry: Registry, reader: str, owner_confirmed: bool) -> None:
    """Remove a reader; a stale one only with the owner's confirmation [777]."""
    with transaction(registry) as state:
        legacy_only(state)
        if reader not in state["readers"]:
            return
        if is_stale(state["readers"][reader]) and not owner_confirmed:
            raise Refused(f"reader {reader} is stale; needs --owner-confirmed")
        state["readers"].pop(reader)


def protected_sets(registry: Registry) -> dict[str, list[str]]:
    """Every publish run named by a reservation and every snapshot set named by a registration [777]."""
    with transaction(registry, write=False) as state:
        readers = list(state["readers"].values())
    return {
        "publish_runs": sorted({reader["publish_run"] for reader in readers if reader["state"] == "reserved"}),
        "snapshot_sets": sorted({reader["snapshot_set"] for reader in readers if reader["state"] == "registered"}),
    }


def status(registry: Registry) -> dict[str, Any]:
    """Holders, runs and readers with staleness; no token or token hash [882] [884]."""
    with transaction(registry, write=False) as state:
        runs = {run_id: {key: value for key, value in entry.items() if key != "token_sha256"} for run_id, entry in state["runs"].items()}
        readers = {reader: {**entry, "stale": is_stale(entry)} for reader, entry in state["readers"].items()}
        domains: dict[str, dict[str, Any] | None] = dict.fromkeys(DOMAINS)
        for domain, run_id in state["domains"].items():
            entry = runs[run_id]
            domains[domain] = {"run_id": run_id, "state": entry["state"], "mode": entry["mode"], "holder": entry["holder"], "stale": is_stale(entry)}
        return {"registry": state["_path"], "registry_id": state["registry_id"], "domains": domains, "runs": runs, "readers": readers}


def parser() -> argparse.ArgumentParser:
    """The command-line interface."""
    cli = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    cli.add_argument("--test-registry", help="a test registry folder; it can never hold a real run")
    commands = cli.add_subparsers(dest="command", required=True)
    commands.add_parser("where", help="print the registry folder")
    commands.add_parser("status", help="print holders, runs and readers")
    commands.add_parser("protected-sets", help="print the publish runs and snapshot sets readers hold")
    command = commands.add_parser("acquire", help="take a domain's lock for a new run; prints the token once")
    command.add_argument("--domain", choices=DOMAINS, required=True)
    command.add_argument("--run", required=True)
    command.add_argument("--mode", choices=MODES, required=True)
    command.add_argument("--holder", choices=("broker", "manual"), default="manual")
    command.add_argument("--pid", type=int, default=os.getppid(), help="the long-running holder process (default: the caller)")
    command.add_argument("--write-prefix", action="append", default=[], help="an S3 prefix an acquisition run writes")
    for name in ("enter", "recover"):
        commands.add_parser(name, help=f"{name} a run (token from RUN_LOCK_TOKEN)").add_argument("--run", required=True)
    command = commands.add_parser("pin", help="record bronze prefixes a transform run reads")
    command.add_argument("--run", required=True)
    command.add_argument("--prefix", action="append", required=True)
    command = commands.add_parser("mark", help="move a run to finalizing or failed_retained")
    command.add_argument("--run", required=True)
    command.add_argument("--state", choices=("finalizing", "failed_retained"), required=True)
    command = commands.add_parser("release", help="release a finalizing run, or a failed one with --owner")
    command.add_argument("--run", required=True)
    command.add_argument("--record", help="the run's durable record (required after success)")
    command.add_argument("--owner", action="store_true", help="the owner releases a failed_retained run after fencing")
    for name in ("register", "deregister"):
        command = commands.add_parser(name, help=f"{name} a run as a shared-service user")
        command.add_argument("--run", required=True)
        command.add_argument("--kind", choices=REGISTRATION_KINDS, required=True)
    command = commands.add_parser("reserve", help="reserve the publish run a reader is about to read")
    command.add_argument("--reader", required=True)
    command.add_argument("--domain", choices=DOMAINS, required=True)
    command.add_argument("--publish-run", required=True)
    command.add_argument("--pid", type=int, default=os.getppid())
    command = commands.add_parser("register-set", help="register the snapshot set a reader pinned")
    command.add_argument("--reader", required=True)
    command.add_argument("--snapshot-set", required=True)
    command = commands.add_parser("deregister-reader", help="remove a reader")
    command.add_argument("--reader", required=True)
    command.add_argument("--owner-confirmed", action="store_true", help="the owner confirms a stale reader is gone")
    return cli


def registry_schema(registry: Registry) -> int:
    """The schema of an existing registry, or the launcher's for one that does not exist yet (only the launcher creates registries)."""
    if not registry.state_file.exists():
        return LAUNCHER_SCHEMA
    with transaction(registry, write=False) as state:
        return int(state["schema_version"])


def launcher_dispatch(args: argparse.Namespace, registry: Registry, *, client: Any = None) -> Any:
    """The same commands on a launcher registry: each is a request to the launcher, with the run's token from the token store [D1 section 3]."""
    from orchestration import launcher

    client = client if client is not None else launcher.LauncherClient(launcher.resolve_config(args.test_registry))

    def run_principal(owner: bool = False) -> dict[str, Any]:
        return client.principal_run(args.run, owner=owner)

    command = args.command
    if command == "status":
        return client.call("status", {"what": "registry"})
    if command == "protected-sets":
        return client.call("status", {"what": "protected_sets"})
    if command == "acquire":
        params = {
            "domain": args.domain,
            "run_id": args.run,
            "mode": args.mode,
            "holder": args.holder,
            "pid": args.pid,
            "write_prefixes": args.write_prefix,
            "checkout": str(Path.cwd()),
        }
        reply = client.call("acquire", params)
        return {"run_id": reply["run_id"], "token_file": reply["token_file"]}
    if command == "enter":
        return client.call("enter", {}, run_principal())
    if command == "recover":
        return client.call("recover", {"owner": True}, run_principal(owner=True))
    if command == "pin":
        return client.call("pin", {"prefixes": args.prefix}, run_principal())
    if command == "mark":
        return client.call("mark", {"state": args.state}, run_principal())
    if command == "release":
        return client.call("release", {"record": args.record, "owner": args.owner}, run_principal(owner=args.owner), timeout=300)
    if command in ("register", "deregister"):
        return client.call("register", {"kind": args.kind, "add": command == "register"}, run_principal())
    if command == "reserve":
        return client.call("reserve", {"reader": args.reader, "domain": args.domain, "publish_run": args.publish_run, "pid": args.pid})
    if command == "register-set":
        return client.call("register_set", {"reader": args.reader, "snapshot_set": args.snapshot_set})
    return client.call("deregister_set", {"reader": args.reader, "owner_confirmed": args.owner_confirmed})


def dispatch(args: argparse.Namespace, registry: Registry) -> Any:
    """Run one command and return what it prints."""
    if registry_schema(registry) == LAUNCHER_SCHEMA:
        return launcher_dispatch(args, registry)
    simple = {
        "status": lambda: status(registry),
        "protected-sets": lambda: protected_sets(registry),
        "acquire": lambda: acquire(registry, args.domain, args.run, args.mode, args.holder, args.pid, args.write_prefix),
        "enter": lambda: enter(registry, args.run),
        "recover": lambda: recover(registry, args.run),
        "pin": lambda: pin(registry, args.run, args.prefix),
        "mark": lambda: mark(registry, args.run, args.state),
        "release": lambda: release(registry, args.run, args.record, args.owner),
        "register": lambda: register(registry, args.run, args.kind, add=True),
        "deregister": lambda: register(registry, args.run, args.kind, add=False),
        "reserve": lambda: reserve(registry, args.reader, args.domain, args.publish_run, args.pid),
        "register-set": lambda: register_set(registry, args.reader, args.snapshot_set),
        "deregister-reader": lambda: deregister_reader(registry, args.reader, args.owner_confirmed),
    }
    return simple[args.command]()


def main() -> int:
    """Run the command; refusals and registry errors print their reason and exit 3 or 4."""
    args = parser().parse_args()
    try:
        registry = resolve(args.test_registry, Path.cwd())
        if args.command == "where":
            result: Any = {"path": str(registry.path), "kind": registry.kind}
        else:
            result = dispatch(args, registry)
    except Refused as exc:
        sys.stderr.write(f"refused: {exc}\n")
        return REFUSED
    except RegistryError as exc:
        sys.stderr.write(f"registry error: {exc}\n")
        return REGISTRY_ERROR
    except Exception as exc:
        # A launcher refusal arrives as the launcher client's error class; it carries the same exit code as a refusal here.
        if getattr(exc, "code", None) is None:
            raise
        sys.stderr.write(f"refused: {exc}\n")
        return REFUSED
    sys.stdout.write(json.dumps(result if result is not None else {"ok": True}, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
