"""Check the run lock and registry through the real CLI in throwaway registries and scratch Git repositories.

Run ``.venv/bin/python -m scripts.lakehouse.run_lock_e2e``. Reports stay in data/e2e/run_lock/. Every scenario uses a
``--test-registry`` folder inside the report folder or a scratch repository's own registry, never this repository's.
Container fencing runs against a synthetic ``docker`` on ``PATH`` that keeps its containers in a state file; live
Docker fencing is checked by the launcher wiring (wave 1). Failure modes 757, 768, 776, 777 and 880 to 885 are in
plans/wave0_20261010/failure_modes.md.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import sys
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import CompletedProcess, clear_git_environment, run_command

ROOT = Path(__file__).resolve().parents[2]
MODULE = ["-m", "scripts.lakehouse.run_lock"]
REFUSED, REGISTRY_ERROR = 3, 4
SYNTHETIC_DOCKER = """#!/bin/sh
set -eu
state="$DOCKER_STATE"
printf '%s\\n' "$*" >> "$state.calls"
case "$1" in
  ps)
    all=0; run=""
    for arg in "$@"; do
      case "$arg" in
        -aq) all=1 ;;
        label=hai.run_id=*) run="${arg#label=hai.run_id=}" ;;
      esac
    done
    awk -v r="$run" -v all="$all" '$2 == r && (all == 1 || $3 == "running") {print $1}' "$state" ;;
  stop)
    shift
    for id in "$@"; do
      awk -v i="$id" '{ if ($1 == i) $3 = "exited"; print }' "$state" > "$state.tmp" && mv "$state.tmp" "$state"
      printf '%s\\n' "$id"
    done ;;
  info)
    case "$*" in
      *MemTotal*) cat "$state.total" ;;
      *NCPU*) echo 8 ;;
    esac ;;
  stats)
    case "$*" in
      *Name*) awk '{print $1 " " $2 " / 32GiB"}' "$state.stats" ;;
      *) awk '{print $2 " / 32GiB"}' "$state.stats" ;;
    esac ;;
  *) echo "synthetic docker rejects: $*" >&2; exit 93 ;;
esac
"""
# The admission scenarios read memory from these instead of the Mac; ps answers the Docker VM query only.
SYNTHETIC_TOOLS = {
    "sysctl": "#!/bin/sh\necho 8\n",
    "vm_stat": "#!/bin/sh\nprintf 'Mach Virtual Memory Statistics: (page size of 16384 bytes)\\n'\n"
    + "printf 'Pages free: 4194304.\\nFile-backed pages: 0.\\nPages purgeable: 0.\\n'\n",
    "ps": '#!/bin/sh\nif [ "$1" = "-axo" ]; then echo "2097152 com.apple.Virtualization.VirtualMachine"; else exec /bin/ps "$@"; fi\n',
}
GIB = 1024**3


@dataclass
class Context:
    """One scenario's folder, synthetic Docker and every captured output, for the token scan."""

    folder: Path
    env: dict[str, str]
    outputs: list[str] = field(default_factory=list)
    tokens: list[str] = field(default_factory=list)
    commands: list[str] = field(default_factory=list)

    @property
    def registry(self) -> Path:
        """The scenario's test registry folder."""
        return self.folder / "registry"

    def containers(self, rows: str) -> None:
        """Replace the synthetic Docker's containers: one ``<id> <run_id> <running|exited>`` per line."""
        (self.folder / "docker_state").write_text(rows)

    def cli(self, *args: str, token: str | None = None, cwd: Path | None = None, test: bool = True, via_shell: bool = False) -> CompletedProcess[str]:
        """Run the CLI; ``via_shell`` starts it from a short-lived shell, so the recorded holder process ends at once."""
        argv = [*MODULE, *(["--test-registry", str(self.registry)] if test else []), *args]
        env = {**self.env, **({"RUN_LOCK_TOKEN": token} if token else {})}
        self.commands.append(" ".join(["run_lock", *argv[2:]]))
        if via_shell:
            line = " ".join(f"'{part}'" for part in [sys.executable, *argv])
            result = run_command("sh", ["-c", f"{line}; true"], cwd=cwd or ROOT, env=env, timeout=60)
        else:
            result = run_command(sys.executable, argv, cwd=cwd or ROOT, env=env, timeout=60)
        self.outputs.append(result.stdout + result.stderr)
        return result

    def acquire(self, run: str, domain: str = "transform", *extra: str, via_shell: bool = False, cwd: Path | None = None, test: bool = True) -> str:
        """Acquire and return the token, recording it for the leak scan."""
        result = self.cli("acquire", "--domain", domain, "--run", run, "--mode", "fixture", *extra, via_shell=via_shell, cwd=cwd, test=test)
        if result.returncode != 0:
            raise AssertionError(f"acquire {run} failed: {result.stderr.strip()}")
        token = json.loads(result.stdout)["token"]
        self.outputs.pop()  # the acquire result is the one output allowed to hold the token
        self.tokens.append(token)
        return token

    def status(self, test: bool = True, cwd: Path | None = None) -> dict[str, Any]:
        """Return the parsed status."""
        result = self.cli("status", test=test, cwd=cwd)
        if result.returncode != 0:
            raise AssertionError(f"status failed: {result.stderr.strip()}")
        return dict(json.loads(result.stdout))


def refused(result: CompletedProcess[str], reason: str, code: int = REFUSED) -> bool:
    """A rejection counts only with its exit code and its intended reason."""
    return result.returncode == code and reason in result.stderr


def nested_and_competing(ctx: Context) -> dict[str, bool]:
    """757: the token joins the run; a wrong token or a second run of the domain is refused."""
    token = ctx.acquire("run_a")
    joined = ctx.cli("enter", "--run", "run_a", token=token)
    return {
        "nested launch with the token joins the run": joined.returncode == 0 and json.loads(joined.stdout)["domain"] == "transform",
        "wrong token refused": refused(ctx.cli("enter", "--run", "run_a", token=secrets.token_urlsafe(32)), "token does not match"),
        "second run of the domain refused": refused(ctx.cli("acquire", "--domain", "transform", "--run", "run_b", "--mode", "fixture"), "held by run_a"),
        "other domain allowed": ctx.cli("acquire", "--domain", "training", "--run", "train_a", "--mode", "fixture").returncode == 0,
        "status shows the holder": ctx.status()["domains"]["transform"]["run_id"] == "run_a",
    }


def cross_domain(ctx: Context) -> dict[str, bool]:
    """768: acquisition writes and transform pins may not overlap, in either order."""
    token = ctx.acquire("run_a")
    pinned = ctx.cli("pin", "--run", "run_a", "--prefix", "lakehouse/bronze/hai/", token=token)
    overlap = ctx.cli("acquire", "--domain", "acquisition", "--run", "acq_a", "--mode", "fixture", "--write-prefix", "lakehouse/bronze/hai/new/")
    disjoint = ctx.cli("acquire", "--domain", "acquisition", "--run", "acq_b", "--mode", "fixture", "--write-prefix", "raw/cms/")
    second = Context(ctx.folder / "reverse", ctx.env)
    second.folder.mkdir()
    second.acquire("acq_c", "acquisition", "--write-prefix", "lakehouse/bronze/")
    transform = second.acquire("run_c")
    late_pin = second.cli("pin", "--run", "run_c", "--prefix", "lakehouse/bronze/hai/", token=transform)
    ctx.outputs += second.outputs
    ctx.tokens += second.tokens
    ctx.commands += second.commands
    return {
        "transform pins its bronze prefix": pinned.returncode == 0,
        "acquisition writing a pinned prefix refused": refused(overlap, "pinned by transform run run_a"),
        "acquisition writing elsewhere allowed": disjoint.returncode == 0,
        "pin over an acquisition write refused": refused(late_pin, "written by acquisition run acq_c"),
    }


def failed_run(ctx: Context) -> dict[str, bool]:
    """776 and 885: a failed run keeps the lock until the owner releases it with the token, after fencing."""
    token = ctx.acquire("run_a")
    ctx.containers("c1 run_a running\nc2 other running\n")
    retained = ctx.cli("mark", "--run", "run_a", "--state", "failed_retained", token=token)
    retained_state = ctx.status()["runs"]["run_a"]["state"]
    blocked = ctx.cli("acquire", "--domain", "transform", "--run", "run_b", "--mode", "fixture")
    plain = ctx.cli("release", "--run", "run_a", token=token)
    no_token = ctx.cli("release", "--run", "run_a", "--owner")
    owner = ctx.cli("release", "--run", "run_a", "--owner", token=token)
    calls = (ctx.folder / "docker_state.calls").read_text() if (ctx.folder / "docker_state.calls").exists() else ""
    state = (ctx.folder / "docker_state").read_text()
    return {
        "failed run retained": retained.returncode == 0 and retained_state == "failed_retained",
        "next run refused while retained": refused(blocked, "held by run_a"),
        "release without --owner refused": refused(plain, "failed_retained needs --owner"),
        "release without the token refused": refused(no_token, "token does not match"),
        "owner release fences only this run's containers": owner.returncode == 0 and "stop c1" in calls and "c2 other running" in state,
        "next run allowed after owner release": ctx.cli("acquire", "--domain", "transform", "--run", "run_b", "--mode", "fixture").returncode == 0,
    }


def success_release(ctx: Context) -> dict[str, bool]:
    """885: release after success needs the record, no registration and no container of the run."""
    token = ctx.acquire("run_a")
    registered = ctx.cli("register", "--run", "run_a", "--kind", "catalog_user", token=token)
    held = ctx.cli("release", "--run", "run_a", token=token)
    ctx.cli("mark", "--run", "run_a", "--state", "finalizing", token=token)
    record = ctx.folder / "run.json"
    missing = ctx.cli("release", "--run", "run_a", "--record", str(record), token=token)
    record.write_text(json.dumps({"run_id": "run_a"}))
    wrong = ctx.folder / "other.json"
    wrong.write_text(json.dumps({"run_id": "run_z"}))
    mismatch = ctx.cli("release", "--run", "run_a", "--record", str(wrong), token=token)
    registration = ctx.cli("release", "--run", "run_a", "--record", str(record), token=token)
    ctx.cli("deregister", "--run", "run_a", "--kind", "catalog_user", token=token)
    ctx.containers("c9 run_a exited\n")
    container = ctx.cli("release", "--run", "run_a", "--record", str(record), token=token)
    ctx.containers("")
    released = ctx.cli("release", "--run", "run_a", "--record", str(record), token=token)
    again = ctx.cli("release", "--run", "run_a", "--record", str(record), token=token)
    return {
        "registration recorded": registered.returncode == 0,
        "release while held refused": refused(held, "state held"),
        "missing record refused": refused(missing, "run record missing"),
        "record of another run refused": refused(mismatch, "run record names run_z"),
        "remaining registration refused": refused(registration, "catalog_user registration remains"),
        "remaining container refused": refused(container, "container c9 remains"),
        "release succeeds when every predicate holds": released.returncode == 0 and ctx.status()["runs"]["run_a"]["state"] == "released",
        "repeated release is a no-op": again.returncode == 0,
    }


def race(ctx: Context) -> dict[str, bool]:
    """881: 16 processes race for one domain; exactly one wins and the registry stays valid."""
    ctx.cli("status")  # create the registry first, so the race is over the domain, not the folder
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda n: ctx.cli("acquire", "--domain", "transform", "--run", f"race_{n:02d}", "--mode", "fixture"), range(16)))
    winners = [result for result in results if result.returncode == 0]
    ctx.tokens += [json.loads(result.stdout)["token"] for result in winners]
    for result in winners:
        ctx.outputs.remove(result.stdout + result.stderr)
    status = ctx.status()
    return {
        "exactly one winner": len(winners) == 1,
        "every other process refused for the holder": all(refused(result, "held by race_") for result in results if result.returncode != 0),
        "registry names the winner": len(winners) == 1 and status["domains"]["transform"]["run_id"] == json.loads(winners[0].stdout)["run_id"],
    }


def stale_holder(ctx: Context) -> dict[str, bool]:
    """884: a holder whose process ended is shown stale, never freed; the owner recovers it with the token."""
    token = ctx.acquire("run_a", via_shell=True)
    status = ctx.status()
    blocked = ctx.cli("acquire", "--domain", "transform", "--run", "run_b", "--mode", "fixture")
    ctx.containers("c1 run_a running\n")
    recovered = ctx.cli("recover", "--run", "run_a", token=token)
    after = ctx.status()["runs"]["run_a"]["state"]
    released = ctx.cli("release", "--run", "run_a", "--owner", token=token)
    return {
        "dead holder shown stale": status["domains"]["transform"]["stale"] is True,
        "stale holder still blocks the domain": refused(blocked, "held by run_a"),
        "recover fences and retains": recovered.returncode == 0
        and after == "failed_retained"
        and "c1 run_a exited" in (ctx.folder / "docker_state").read_text(),
        "owner release after recovery": released.returncode == 0,
    }


def malformed(ctx: Context) -> dict[str, bool]:
    """881: a malformed registry stops every command and is never reset."""
    ctx.cli("status")
    (ctx.registry / "registry.json").write_text("{not json")
    stopped = ctx.cli("status")
    acquire = ctx.cli("acquire", "--domain", "transform", "--run", "run_a", "--mode", "fixture")
    return {
        "status stops on a malformed registry": refused(stopped, "registry.json is not valid", REGISTRY_ERROR),
        "acquire stops on a malformed registry": refused(acquire, "registry.json is not valid", REGISTRY_ERROR),
        "file left as found": (ctx.registry / "registry.json").read_text() == "{not json",
    }


def scratch_repository(folder: Path) -> tuple[Path, Path]:
    """A Git repository with one commit and a linked worktree; returns both checkouts."""
    main = folder / "main"
    main.mkdir(parents=True)
    git = ["-c", "user.name=E2E", "-c", "user.email=e2e@example.invalid"]
    run_command("git", ["init", "-q", "-b", "main"], cwd=main, check=True)
    (main / "README.md").write_text("scratch\n")
    run_command("git", ["add", "README.md"], cwd=main, check=True)
    run_command("git", [*git, "commit", "-qm", "chore: scratch"], cwd=main, check=True)
    worktree = folder / "linked"
    run_command("git", ["worktree", "add", "-q", "-b", "feat/wp-x", str(worktree)], cwd=main, check=True)
    return main, worktree


def worktrees(ctx: Context) -> dict[str, bool]:
    """880 and 883: every worktree resolves the main checkout's registry; markers and registry IDs are enforced."""
    main, linked = scratch_repository(ctx.folder / "repo")
    env = {**ctx.env, "PYTHONPATH": str(ROOT)}
    scratch = Context(ctx.folder, env)
    from_main = scratch.cli("where", test=False, cwd=main)
    from_linked = scratch.cli("where", test=False, cwd=linked)
    expected = str((main / "data" / "orchestration" / "registry").resolve())
    token = scratch.acquire("run_a", test=False, cwd=main)
    competing = scratch.cli("acquire", "--domain", "transform", "--run", "run_b", "--mode", "fixture", test=False, cwd=linked)
    joined = scratch.cli("enter", "--run", "run_a", token=token, test=False, cwd=linked)
    identity = main / "data" / "orchestration" / "registry" / "registry_id"
    original = identity.read_text()
    identity.write_text(json.dumps({"registry_id": "replaced", "kind": "real"}))
    changed = scratch.cli("enter", "--run", "run_a", token=token, test=False, cwd=main)
    identity.write_text(json.dumps({**json.loads(original), "kind": "test"}))
    marked = scratch.cli("status", test=False, cwd=main)
    identity.write_text(original)
    real_in_test = ctx.cli("acquire", "--domain", "transform", "--run", "run_r", "--mode", "real")
    real_registry = ctx.cli("--test-registry", expected, "status", test=False)
    run_command("git", ["worktree", "remove", "--force", str(linked)], cwd=main, check=True)
    ctx.outputs += scratch.outputs
    ctx.tokens += scratch.tokens
    ctx.commands += scratch.commands
    return {
        "main checkout resolves its own registry": from_main.returncode == 0 and json.loads(from_main.stdout)["path"] == expected,
        "linked worktree resolves the same registry": from_linked.returncode == 0 and json.loads(from_linked.stdout)["path"] == expected,
        "competing run from the worktree refused": refused(competing, "held by run_a"),
        "worktree joins the run with the token": joined.returncode == 0,
        "changed registry_id refused": refused(changed, "registry_id differs", REGISTRY_ERROR),
        "test marker in the real registry refused": refused(marked, "test registry", REGISTRY_ERROR),
        "real run refused in a test registry": refused(real_in_test, "real runs are refused in a test registry"),
        "a real registry cannot be used as a test registry": refused(real_registry, "not a test registry", REGISTRY_ERROR),
    }


def readers(ctx: Context) -> dict[str, bool]:
    """777: reservations and registrations protect snapshot sets; a stale reader is removed only when confirmed."""
    reserved = ctx.cli("reserve", "--reader", "train_1", "--domain", "training", "--publish-run", "pub_7", via_shell=True)
    protected = json.loads(ctx.cli("protected-sets").stdout)
    registered = ctx.cli("register-set", "--reader", "train_1", "--snapshot-set", "set_7")
    after = json.loads(ctx.cli("protected-sets").stdout)
    stale = ctx.status()["readers"]["train_1"]["stale"]
    unconfirmed = ctx.cli("deregister-reader", "--reader", "train_1")
    confirmed = ctx.cli("deregister-reader", "--reader", "train_1", "--owner-confirmed")
    final = json.loads(ctx.cli("protected-sets").stdout)
    return {
        "reservation recorded": reserved.returncode == 0,
        "reservation protects its publish run": protected["publish_runs"] == ["pub_7"],
        "registration protects its snapshot set": registered.returncode == 0 and after["snapshot_sets"] == ["set_7"],
        "reader whose process ended is stale": stale is True,
        "stale reader kept without confirmation": refused(unconfirmed, "stale; needs --owner-confirmed"),
        "confirmed removal frees the set": confirmed.returncode == 0 and final == {"publish_runs": [], "snapshot_sets": []},
    }


def budget(ctx: Context, *args: str, reservation: str | None = None, via_shell: bool = False) -> CompletedProcess[str]:
    """Run the memory budget CLI against the scenario's test registry and synthetic readings."""
    argv = ["-m", "scripts.lakehouse.memory_budget", "--test-registry", str(ctx.registry), *args]
    env = {**ctx.env, **({"HAI_RESERVATION": reservation} if reservation else {})}
    ctx.commands.append(" ".join(["memory_budget", *argv[2:]]))
    if via_shell:
        line = " ".join(f"'{part}'" for part in [sys.executable, *argv])
        result = run_command("sh", ["-c", f"{line}; true"], cwd=ROOT, env=env, timeout=60)
    else:
        result = run_command(sys.executable, argv, cwd=ROOT, env=env, timeout=60)
    ctx.outputs.append(result.stdout + result.stderr)
    return result


def readings(ctx: Context, stats: str, total: int = 32 * GIB) -> None:
    """Set the synthetic Docker total and the running containers' use: one ``<name> <usage>`` per line."""
    (ctx.folder / "docker_state.total").write_text(f"{total}\n")
    (ctx.folder / "docker_state.stats").write_text(stats)


def heavy_jobs(ctx: Context) -> dict[str, bool]:
    """759: one reservation at a time across entry points, with the reading it was admitted on."""
    readings(ctx, "other_project 3GiB\n")
    first = budget(ctx, "--admit", "job")
    second = budget(ctx, "--admit", "job")
    plan = json.loads(first.stdout) if first.returncode == 0 else {}
    released = budget(ctx, "--release", plan.get("reservation_id", "none"))
    third = budget(ctx, "--admit", "job")
    return {
        "first job admitted with Docker's free memory": first.returncode == 0 and plan["environment"]["JOB_MEMORY_LIMIT"] == str(21 * GIB),
        "reading recorded with the reservation": first.returncode == 0 and plan["record"]["budget"]["used_by_containers"] == 3 * GIB,
        "second job refused while the first holds": refused(second, f"reservation {plan.get('reservation_id')} holds the machine"),
        "release frees the machine": released.returncode == 0 and third.returncode == 0,
    }


def pool_reuse(ctx: Context) -> dict[str, bool]:
    """886: a pool takes one reservation; a worker carrying its ID reuses it without a new reading."""
    readings(ctx, "other_project 3GiB\n")
    pool = json.loads(budget(ctx, "--admit", "pool").stdout)
    readings(ctx, "other_project 20GiB\n")  # a new reading would now be refused
    worker = budget(ctx, "--admit", "job", reservation=pool["reservation_id"])
    stranger = budget(ctx, "--admit", "job", reservation="r-unknown")
    reused = json.loads(worker.stdout) if worker.returncode == 0 else {}
    return {
        "pool admitted as one reservation": pool["kind"] == "pool" and len(json.loads(budget(ctx, "--reservations").stdout)) == 1,
        "worker reuses the pool's reservation and plan": reused.get("reservation_id") == pool["reservation_id"] and reused.get("reused") is True,
        "worker plan equals the pool's": reused.get("environment") == pool["environment"],
        "unknown reservation ID refused": refused(stranger, "no reservation r-unknown"),
    }


def growth_floor_stale(ctx: Context) -> dict[str, bool]:
    """887: a service's unused limit is reserved; a reading below the floor writes nothing; a dead holder is cleared."""
    readings(ctx, "other_project 3GiB\nhai-airflow-scheduler 2GiB\n")
    registered = budget(ctx, "--register-service", "hai-airflow-scheduler", "--limit", str(6 * GIB))
    grown = budget(ctx, "--admit", "job")
    plan = json.loads(grown.stdout) if grown.returncode == 0 else {}
    budget(ctx, "--release", plan.get("reservation_id", "none"))
    readings(ctx, "other_project 21GiB\n")
    low = budget(ctx, "--admit", "job")
    empty = json.loads(budget(ctx, "--reservations").stdout)
    readings(ctx, "other_project 3GiB\n")
    orphan = budget(ctx, "--admit", "job", via_shell=True)
    after = budget(ctx, "--admit", "job")
    history = json.loads((ctx.registry / "registry.json").read_text())["reservation_history"]
    return {
        "service registered": registered.returncode == 0,
        "unused service limit subtracted": grown.returncode == 0 and plan["environment"]["JOB_MEMORY_LIMIT"] == str(15 * GIB),
        "reading below the floor refused": low.returncode == 1 and "only 3.0 GiB free" in low.stderr,
        "refusal writes no reservation": empty == {},
        "dead holder's reservation cleared at the next admission": orphan.returncode == 0 and after.returncode == 0,
        "clearing recorded": any(event["event"] == "cleared_stale" for event in history),
    }


SCENARIOS: list[tuple[str, Callable[[Context], dict[str, bool]]]] = [
    ("nested and competing launches", nested_and_competing),
    ("cross-domain prefixes", cross_domain),
    ("failed run and owner release", failed_run),
    ("release predicates after success", success_release),
    ("16-process race", race),
    ("stale holder recovery", stale_holder),
    ("malformed registry", malformed),
    ("worktrees, markers and registry IDs", worktrees),
    ("readers and protected snapshot sets", readers),
    ("admission: one heavy job at a time", heavy_jobs),
    ("admission: pool reservation reuse", pool_reuse),
    ("admission: growth reserve, floor and stale holders", growth_floor_stale),
]


def token_scan(ctx: Context) -> bool:
    """882: no token appears in any output other than its acquire result, nor in the registry files."""
    stored = "".join(path.read_text(errors="replace") for path in ctx.folder.rglob("*") if path.is_file() and path.suffix in ("", ".json"))
    text = "\n".join(ctx.outputs) + stored
    return not any(token in text for token in ctx.tokens)


def revision() -> dict[str, Any]:
    """The code under test: commit and the hashes of the two files, so edits during a run are visible."""
    files = {
        name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
        for name in ("scripts/lakehouse/run_lock.py", "scripts/lakehouse/memory_budget.py", "scripts/lakehouse/run_lock_e2e.py")
    }
    head = run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip()
    dirty = run_command("git", ["status", "--porcelain", "--", *files], cwd=ROOT).stdout.splitlines()
    return {"commit": head, "uncommitted": dirty, "sha256": files}


def main() -> int:
    """Run every scenario, write the report and return 1 when any assertion fails."""
    clear_git_environment()
    started = datetime.now(UTC)
    report_dir = ROOT / "data" / "e2e" / "run_lock" / started.strftime("%Y%m%dT%H%M%S%fZ")
    report_dir.mkdir(parents=True)
    stub = report_dir / "bin"
    stub.mkdir()
    (stub / "docker").write_text(SYNTHETIC_DOCKER)
    (stub / "docker").chmod(0o700)
    for name, body in SYNTHETIC_TOOLS.items():
        (stub / name).write_text(body)
        (stub / name).chmod(0o700)
    before = revision()
    results: list[dict[str, Any]] = []
    for index, (name, scenario) in enumerate(SCENARIOS):
        folder = report_dir / f"{index:02d}"
        folder.mkdir()
        (folder / "docker_state").write_text("")
        env = {**os.environ, "PATH": f"{stub}{os.pathsep}{os.environ['PATH']}", "DOCKER_STATE": str(folder / "docker_state")}
        env.pop("RUN_LOCK_TOKEN", None)
        ctx = Context(folder, env)
        try:
            checks = scenario(ctx)
            if ctx.tokens:  # scenarios that hold no token have nothing to leak
                checks["no token in outputs or registry files"] = token_scan(ctx)
            error = ""
        except (AssertionError, KeyError, ValueError, OSError) as exc:
            checks, error = {}, f"{type(exc).__name__}: {exc}"
        status = "pass" if checks and all(checks.values()) and not error else "fail"
        results.append({"scenario": name, "status": status, "checks": checks, "error": error, "commands": ctx.commands})
        sys.stdout.write(f"{'ok' if status == 'pass' else 'FAIL':<5} {name}\n")
        for check, ok in checks.items():
            if not ok:
                sys.stdout.write(f"      failed: {check}\n")
        if error:
            sys.stdout.write(f"      error: {error}\n")
    after = revision()
    report = {
        "feature": "run lock, registry and resource admission (wave 0 items 1 and 2)",
        "started_utc": started.isoformat(),
        "finished_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "platform": sys.platform,
        "code": before,
        "code_changed_during_run": before != after,
        "command": ".venv/bin/python -m scripts.lakehouse.run_lock_e2e",
        "scenarios": results,
        "limits": [
            "Container fencing uses a synthetic docker on PATH; live Docker fencing is checked when the launchers are wired (wave 1).",
            "flock is checked on the local disk only; process start times use macOS ps -o lstart=.",
        ],
        "cleanup": "Scratch repositories and test registries stay in this report folder; the linked worktree is removed.",
    }
    passed = all(result["status"] == "pass" for result in results) and before == after
    report["status"] = "pass" if passed else "fail"
    (report_dir / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    sys.stdout.write(f"{report['status']}: {report_dir.relative_to(ROOT)}/report.json\n")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
