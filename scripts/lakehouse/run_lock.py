"""One run lock and registry per machine for every run that builds, publishes or reads the lakehouse.

Run from any checkout or worktree of this repository:

    .venv/bin/python -m scripts.lakehouse.run_lock where       # the registry folder every worktree resolves
    .venv/bin/python -m scripts.lakehouse.run_lock status      # holders, runs and readers; never a token
    .venv/bin/python -m scripts.lakehouse.run_lock acquire --domain transform --run <id> --mode fixture
    RUN_LOCK_TOKEN=<token> .venv/bin/python -m scripts.lakehouse.run_lock release --run <id> --owner

The registry is ``data/orchestration/registry/`` in the main checkout, found through Git's common directory, so every
worktree shares it; there is no environment override. Tests pass ``--test-registry <folder>``, which can never hold a
``real`` run. Each change holds an exclusive ``flock`` and replaces ``registry.json`` atomically; a malformed registry
stops every command and is never reset. ``acquire`` prints the run's token once; the registry keeps only its SHA-256 and
every later command reads it from ``RUN_LOCK_TOKEN``. Nothing releases a lock automatically: a failed or stale run
stays ``failed_retained`` until the owner releases it with the token, after its labelled containers are stopped.
Exit codes: 0 done, 2 usage, 3 refused, 4 registry error. Failure modes 757, 768, 776, 777 and 880 to 885 are in
plans/wave0_20261010/failure_modes.md; the design is in plans/airflow_20261010/plan.md sections 4, 9.1 and 9.4.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import secrets
import sys
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import run_command

DOMAINS = ("acquisition", "transform", "training", "serving")
MODES = ("fixture", "publish_fixture", "real")
REGISTRATION_KINDS = ("catalog_user",)
SCHEMA_VERSION = 1
# Locks and readers (this module); memory reservations and service limits (memory_budget.admit) share the transactions.
SECTIONS = ("domains", "runs", "readers", "reservations", "services")
LOCK_WAIT_SECONDS = 30
CONTAINER_LABEL = "hai.run_id"
REFUSED, REGISTRY_ERROR = 3, 4


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


def resolve(test_registry: str | None, cwd: Path) -> Registry:
    """The real registry of this machine's main checkout, or the named test registry."""
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


@contextmanager
def transaction(registry: Registry, write: bool = True) -> Iterator[dict[str, Any]]:
    """Hold the registry's exclusive lock, yield its state and, when ``write`` and no error, replace it atomically."""
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
                empty = {"schema_version": SCHEMA_VERSION, "registry_id": identity["registry_id"], **{key: {} for key in SECTIONS}, "reservation_history": []}
                write_atomic(registry.state_file, json.dumps(empty, indent=2) + "\n")
            identity = read_identity(registry)
            try:
                state = json.loads(registry.state_file.read_text())
                if not isinstance(state, dict) or state.get("schema_version") != SCHEMA_VERSION:
                    raise ValueError("unknown schema")
                for key in SECTIONS:
                    if not isinstance(state.get(key), dict):
                        raise ValueError(f"missing {key}")
                if not isinstance(state.get("reservation_history"), list):
                    raise ValueError("missing reservation_history")
            except (OSError, ValueError) as exc:
                raise RegistryError(f"{registry.state_file.name} is not valid: {exc}") from exc
            if state.get("registry_id") != identity["registry_id"]:
                raise RegistryError(f"registry_id differs from {registry.state_file.name}: the registry was replaced")
            state["_path"] = str(registry.path)
            yield state
            state.pop("_path", None)
            if write:
                write_atomic(registry.state_file, json.dumps(state, indent=2, sort_keys=True) + "\n")
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


def is_stale(entry: dict[str, Any]) -> bool:
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


def overlaps(left: str, right: str) -> bool:
    """Two S3 key prefixes overlap when one contains the other."""
    return left.startswith(right) or right.startswith(left)


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


def acquire(registry: Registry, domain: str, run_id: str, mode: str, holder: str, pid: int, write_prefixes: list[str]) -> dict[str, str]:
    """Take the domain's lock for a new run in one transaction and return the run ID and its token [757] [768]."""
    if registry.kind == "test" and mode == "real":
        raise Refused("real runs are refused in a test registry")
    pid_start = process_start(pid)
    if pid_start is None:
        raise Refused(f"holder process {pid} is not running")
    with transaction(registry) as state:
        current = active(state, domain)
        if current is not None:
            raise Refused(f"domain {domain} is held by {current['run_id']} (state {current['state']})")
        if run_id in state["runs"]:
            raise Refused(f"run ID {run_id} was used before; a retry with new inputs needs a new run ID")
        check_cross_domain(state, domain, write_prefixes)
        token = secrets.token_urlsafe(32)
        state["runs"][run_id] = {
            "run_id": run_id,
            "domain": domain,
            "mode": mode,
            "holder": holder,
            "token_sha256": digest(token),
            "checkout": str(Path.cwd()),
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
        state["domains"][domain] = run_id
    return {"run_id": run_id, "token": token}


def enter(registry: Registry, run_id: str) -> dict[str, str]:
    """Let a nested launcher with the run's token join it [757]."""
    with transaction(registry, write=False) as state:
        entry = owned(state, run_id)
        if entry["state"] != "held":
            raise Refused(f"run {run_id} is in state {entry['state']}; only a held run accepts launches")
    return {"run_id": run_id, "domain": entry["domain"], "mode": entry["mode"]}


def pin(registry: Registry, run_id: str, prefixes: list[str]) -> None:
    """Record the bronze prefixes a transform run reads; refused over an acquisition run's writes [768]."""
    with transaction(registry) as state:
        entry = owned(state, run_id)
        if entry["domain"] != "transform" or entry["state"] != "held":
            raise Refused(f"only a held transform run pins prefixes; {run_id} is {entry['domain']} in state {entry['state']}")
        writer = active(state, "acquisition")
        for prefix in prefixes:
            for written in writer["write_prefixes"] if writer is not None else []:
                if writer is not None and overlaps(prefix, written):
                    raise Refused(f"prefix {prefix} overlaps {written}, written by acquisition run {writer['run_id']}")
        state["runs"][run_id]["pinned_prefixes"] = sorted(set(entry["pinned_prefixes"]) | set(prefixes))


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
        entry = owned(state, run_id)
        if entry["state"] == new:
            return
        if entry["state"] not in allowed[new]:
            raise Refused(f"run {run_id} is in state {entry['state']}; it cannot move to {new}")
        set_state(state, run_id, new)


def containers(run_id: str, running_only: bool) -> list[str]:
    """IDs of containers labelled with the run, all or running only."""
    flags = "-q" if running_only else "-aq"
    result = run_command("docker", ["ps", flags, "--filter", f"label={CONTAINER_LABEL}={run_id}"], timeout=60)
    if result.returncode != 0:
        raise Refused(f"cannot list containers of run {run_id}: {result.stderr.strip()}")
    return result.stdout.split()


def fence(run_id: str) -> None:
    """Stop every running container labelled with the run and confirm none still runs [776] [885]."""
    running = containers(run_id, running_only=True)
    if running:
        result = run_command("docker", ["stop", *running], timeout=300)
        if result.returncode != 0:
            raise Refused(f"cannot stop containers of run {run_id}: {result.stderr.strip()}")
    left = containers(run_id, running_only=True)
    if left:
        raise Refused(f"containers {', '.join(left)} of run {run_id} still run after the stop")


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
    """Release a finalizing run when every predicate holds, or a failed one by the owner after fencing [776] [885]."""
    with transaction(registry, write=False) as state:
        entry = owned(state, run_id)
    if entry["state"] == "released":
        return
    if entry["state"] == "failed_retained":
        if not owner:
            raise Refused(f"run {run_id} is in state failed_retained; failed_retained needs --owner")
        fence(run_id)
    elif entry["state"] == "finalizing":
        check_record(run_id, record)
        if entry["registrations"]:
            raise Refused(f"run {run_id}: {entry['registrations'][0]} registration remains")
        remaining = containers(run_id, running_only=False)
        if remaining:
            raise Refused(f"run {run_id}: container {remaining[0]} remains")
    else:
        raise Refused(f"run {run_id} is in state {entry['state']}; release needs finalizing, or failed_retained with --owner")
    with transaction(registry) as state:
        current = owned(state, run_id)
        if current["state"] != entry["state"] or current["registrations"]:
            raise Refused(f"run {run_id} changed during the release checks; retry")
        set_state(state, run_id, "released")


def recover(registry: Registry, run_id: str) -> None:
    """Owner recovery of a stale or interrupted run: fence its containers, then keep it as failed_retained [884]."""
    with transaction(registry, write=False) as state:
        entry = owned(state, run_id)
    if entry["state"] in ("failed_retained", "released"):
        return
    fence(run_id)
    with transaction(registry) as state:
        owned(state, run_id)
        set_state(state, run_id, "failed_retained")


def register(registry: Registry, run_id: str, kind: str, add: bool) -> None:
    """Add or remove a run's registration as a shared-service user [777]."""
    with transaction(registry) as state:
        entry = owned(state, run_id)
        kinds = set(entry["registrations"])
        if add and entry["state"] != "held":
            raise Refused(f"run {run_id} is in state {entry['state']}; only a held run registers")
        state["runs"][run_id]["registrations"] = sorted(kinds | {kind} if add else kinds - {kind})


def reserve(registry: Registry, reader: str, domain: str, publish_run: str, pid: int) -> None:
    """A reader announces the publish run it is about to read, before it pins the snapshot set [777]."""
    pid_start = process_start(pid)
    if pid_start is None:
        raise Refused(f"reader process {pid} is not running")
    with transaction(registry) as state:
        if reader in state["readers"]:
            raise Refused(f"reader {reader} already holds a reservation or registration")
        entry = {"domain": domain, "publish_run": publish_run, "snapshot_set": None, "state": "reserved", "pid": pid, "pid_start": pid_start}
        state["readers"][reader] = {**entry, "at": now()}


def register_set(registry: Registry, reader: str, snapshot_set: str) -> None:
    """Turn a reservation into a registration of the pinned snapshot set [777]."""
    with transaction(registry) as state:
        if reader not in state["readers"]:
            raise Refused(f"reader {reader} has no reservation")
        state["readers"][reader].update({"snapshot_set": snapshot_set, "state": "registered", "at": now()})


def deregister_reader(registry: Registry, reader: str, owner_confirmed: bool) -> None:
    """Remove a reader; a stale one only with the owner's confirmation [777]."""
    with transaction(registry) as state:
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


def dispatch(args: argparse.Namespace, registry: Registry) -> Any:
    """Run one command and return what it prints."""
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
    sys.stdout.write(json.dumps(result if result is not None else {"ok": True}, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
