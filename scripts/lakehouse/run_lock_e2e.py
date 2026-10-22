"""Check the run lock and registry through the real CLI, the launcher and a fake Docker, in throwaway registries and scratch Git repositories.

Run ``.venv/bin/python -m scripts.lakehouse.run_lock_e2e``. Reports stay in data/e2e/run_lock/. Every scenario starts its own launcher on a
``--test-registry`` folder with its own fake Docker daemon (scripts/lakehouse/fake_docker.py); the scratch-repository scenario reads the
real-registry resolution rules without starting a launcher. Container checks use the fake daemon's containers; live Docker is checked by the
launcher's fixture runs. Failure modes 757, 768, 776, 777 and 880 to 885 are in plans/wave0_20261010/failure_modes.md; the admission modes
759, 886 and 887 are covered by scripts/lakehouse/run_launcher_e2e.py (allocations).
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from orchestration import launcher
from scripts.lakehouse import run_lock
from scripts.lakehouse.run_launcher_e2e import Env
from scripts.process import CompletedProcess, clear_git_environment, run_command

ROOT = Path(__file__).resolve().parents[2]
MODULE = ["-m", "scripts.lakehouse.run_lock"]
REFUSED, REGISTRY_ERROR = 3, 4


class Scenario:
    """One scenario's environment, its CLI and every captured output, for the token scan."""

    def __init__(self, env: Env) -> None:
        self.env = env
        self.outputs: list[str] = []
        self.commands: list[str] = []

    def cli(self, *args: str, via_shell: bool = False, cwd: Path | None = None, test: bool = True, env: dict[str, str] | None = None) -> CompletedProcess[str]:
        """Run the CLI; ``via_shell`` starts it from a short-lived shell, so the recorded holder process ends at once."""
        argv = [*MODULE, *(["--test-registry", str(self.env.registry)] if test else []), *args]
        self.commands.append(" ".join(["run_lock", *argv[2:]]))
        if via_shell:
            line = " ".join(f"'{part}'" for part in [sys.executable, *argv])
            result = run_command("sh", ["-c", f"{line}; true"], cwd=cwd or ROOT, env=env or os.environ, timeout=90)
        else:
            result = run_command(sys.executable, argv, cwd=cwd or ROOT, env=env or os.environ, timeout=90)
        self.outputs.append(result.stdout + result.stderr)
        return result

    def acquire(self, run: str, domain: str = "transform", *extra: str, via_shell: bool = False, cwd: Path | None = None, test: bool = True) -> None:
        """Acquire a run; the token goes to the token store."""
        result = self.cli("acquire", "--domain", domain, "--run", run, "--mode", "fixture", *extra, via_shell=via_shell, cwd=cwd, test=test)
        if result.returncode != 0:
            raise AssertionError(f"acquire {run} failed: {result.stderr.strip()}")

    def status(self) -> dict[str, Any]:
        """Return the parsed status."""
        result = self.cli("status")
        if result.returncode != 0:
            raise AssertionError(f"status failed: {result.stderr.strip()}")
        return dict(json.loads(result.stdout))

    def token(self, run: str) -> str:
        """The run's token, read from the token store for the leak scan."""
        return launcher.read_token_file(Path(self.env.settings["token_root"]), run)

    def containers(self, rows: list[tuple[str, str, str]]) -> None:
        """Put containers into the fake daemon: ``(name, run_id, running|exited)`` with the run label and no registry label."""
        self.env.daemon.containers.clear()
        for name, run_id, state in rows:
            self.env.daemon.create_rogue(name, {"hai.run_id": run_id}, running=state == "running")


def refused(result: CompletedProcess[str], reason: str, code: int = REFUSED) -> bool:
    """A rejection counts only with its exit code and its intended reason."""
    return result.returncode == code and reason in result.stderr


def record(scenario: Scenario, run: str) -> Path:
    """A durable run record."""
    path = scenario.env.work / f"{run}.json"
    path.write_text(json.dumps({"run_id": run}))
    return path


def nested_and_competing(sc: Scenario) -> dict[str, bool]:
    """757: the token store lets a nested launcher join the run; a second run of the domain is refused."""
    sc.acquire("run_a")
    joined = sc.cli("enter", "--run", "run_a")
    return {
        "nested launch with the stored token joins the run": joined.returncode == 0 and json.loads(joined.stdout)["domain"] == "transform",
        "a run without a token file is refused": sc.cli("enter", "--run", "run_missing").returncode != 0,
        "second run of the domain refused": refused(sc.cli("acquire", "--domain", "transform", "--run", "run_b", "--mode", "fixture"), "held by run_a"),
        "other domain allowed": sc.cli("acquire", "--domain", "training", "--run", "train_a", "--mode", "fixture").returncode == 0,
        "status shows the holder": sc.status()["domains"]["transform"]["run_id"] == "run_a",
    }


def cross_domain(sc: Scenario) -> dict[str, bool]:
    """768: acquisition writes and transform pins may not overlap, in either order."""
    sc.acquire("run_a")
    pinned = sc.cli("pin", "--run", "run_a", "--prefix", "lakehouse/bronze/hai/")
    overlap = sc.cli("acquire", "--domain", "acquisition", "--run", "acq_a", "--mode", "fixture", "--write-prefix", "lakehouse/bronze/hai/new/")
    disjoint = sc.cli("acquire", "--domain", "acquisition", "--run", "acq_b", "--mode", "fixture", "--write-prefix", "raw/cms/")
    sc.cli("release", "--run", "acq_b", "--owner")
    return {
        "transform pins its bronze prefix": pinned.returncode == 0,
        "acquisition writing a pinned prefix refused": refused(overlap, "pinned by transform run run_a"),
        "acquisition writing elsewhere allowed": disjoint.returncode == 0,
    }


def cross_domain_reverse(sc: Scenario) -> dict[str, bool]:
    """768: a pin over an acquisition run's write prefix is refused."""
    sc.acquire("acq_c", "acquisition", "--write-prefix", "lakehouse/bronze/")
    sc.acquire("run_c")
    late_pin = sc.cli("pin", "--run", "run_c", "--prefix", "lakehouse/bronze/hai/")
    return {"pin over an acquisition write refused": refused(late_pin, "written by acquisition run acq_c")}


def cross_domain_segments(sc: Scenario) -> dict[str, bool]:
    """768: prefixes are compared by whole path segments, and a declaration that is not a plain path is refused."""
    sc.acquire("run_a")
    pinned = sc.cli("pin", "--run", "run_a", "--prefix", "lakehouse/bronze/hai/")
    empty = sc.cli("acquire", "--domain", "acquisition", "--run", "acq_e", "--mode", "fixture", "--write-prefix", "")
    doubled = sc.cli("acquire", "--domain", "acquisition", "--run", "acq_d", "--mode", "fixture", "--write-prefix", "lakehouse//bronze/")
    text_only = sc.cli("acquire", "--domain", "acquisition", "--run", "acq_t", "--mode", "fixture", "--write-prefix", "lakehouse/bronze/ha")
    return {
        "transform pins its bronze prefix": pinned.returncode == 0,
        "acquisition writing an empty prefix refused": refused(empty, "not a plain path"),
        "acquisition writing a doubled slash prefix refused": refused(doubled, "not a plain path"),
        "acquisition writing the text start of a pinned folder allowed": text_only.returncode == 0,
    }


def failed_run(sc: Scenario) -> dict[str, bool]:
    """776 and 885: a failed run keeps the lock until the owner releases it, after its containers are gone."""
    sc.acquire("run_a")
    retained = sc.cli("mark", "--run", "run_a", "--state", "failed_retained")
    retained_state = sc.status()["runs"]["run_a"]["state"]
    blocked = sc.cli("acquire", "--domain", "transform", "--run", "run_b", "--mode", "fixture")
    plain = sc.cli("release", "--run", "run_a")
    sc.containers([("c1", "run_a", "exited"), ("c2", "other", "running")])
    with_container = sc.cli("release", "--run", "run_a", "--owner")
    sc.containers([("c2", "other", "running")])
    owner = sc.cli("release", "--run", "run_a", "--owner")
    sc.containers([])
    return {
        "failed run retained": retained.returncode == 0 and retained_state == "failed_retained",
        "next run refused while retained": refused(blocked, "held by run_a"),
        "release without --owner refused": refused(plain, "failed_retained needs --owner"),
        "owner release refused while a container of the run remains": refused(with_container, "container c1 remains"),
        "owner release succeeds when only another run's container exists": owner.returncode == 0,
        "next run allowed after owner release": sc.cli("acquire", "--domain", "transform", "--run", "run_b", "--mode", "fixture").returncode == 0,
    }


def success_release(sc: Scenario) -> dict[str, bool]:
    """885: release after success needs the record, no registration and no container of the run."""
    sc.acquire("run_a")
    registered = sc.cli("register", "--run", "run_a", "--kind", "catalog_user")
    held = sc.cli("release", "--run", "run_a")
    sc.cli("mark", "--run", "run_a", "--state", "finalizing")
    missing = sc.cli("release", "--run", "run_a", "--record", str(sc.env.work / "none.json"))
    wrong = sc.env.work / "other.json"
    wrong.write_text(json.dumps({"run_id": "run_z"}))
    mismatch = sc.cli("release", "--run", "run_a", "--record", str(wrong))
    good = record(sc, "run_a")
    registration = sc.cli("release", "--run", "run_a", "--record", str(good))
    sc.cli("deregister", "--run", "run_a", "--kind", "catalog_user")
    sc.containers([("c9", "run_a", "exited")])
    container = sc.cli("release", "--run", "run_a", "--record", str(good))
    sc.containers([])
    released = sc.cli("release", "--run", "run_a", "--record", str(good))
    again = sc.cli("release", "--run", "run_a", "--record", str(good))
    return {
        "registration recorded": registered.returncode == 0,
        "release while held refused": refused(held, "state held"),
        "missing record refused": refused(missing, "run record missing"),
        "record of another run refused": refused(mismatch, "run record names run_z"),
        "remaining registration refused": refused(registration, "registration catalog_user is open"),
        "remaining container refused": refused(container, "container c9 remains"),
        "release succeeds when every predicate holds": released.returncode == 0 and sc.status()["runs"]["run_a"]["state"] == "released",
        "repeated release is a no-op": again.returncode == 0,
    }


def race(sc: Scenario) -> dict[str, bool]:
    """881: 16 processes race for one domain (and for the launcher's start); exactly one wins and the registry stays valid."""
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda n: sc.cli("acquire", "--domain", "transform", "--run", f"race_{n:02d}", "--mode", "fixture"), range(16)))
    winners = [result for result in results if result.returncode == 0]
    status = sc.status()
    return {
        "exactly one winner": len(winners) == 1,
        "every other process refused for the holder": all(refused(result, "held by race_") for result in results if result.returncode != 0),
        "registry names the winner": len(winners) == 1 and status["domains"]["transform"]["run_id"] == json.loads(winners[0].stdout)["run_id"],
        "one launcher serves the registry": sc.env.ping().get("status") == "serving",
    }


def stale_holder(sc: Scenario) -> dict[str, bool]:
    """884: a holder whose process ended is shown stale, never freed; the owner recovers it."""
    sc.acquire("run_a", via_shell=True)
    status = sc.status()
    blocked = sc.cli("acquire", "--domain", "transform", "--run", "run_b", "--mode", "fixture")
    recovered = sc.cli("recover", "--run", "run_a")
    after = sc.env.wait(lambda: sc.status()["runs"]["run_a"]["state"] == "failed_retained" and "failed_retained", 20, "recovery ends")
    released = sc.cli("release", "--run", "run_a", "--owner")
    return {
        "dead holder shown stale": status["domains"]["transform"]["stale"] is True,
        "stale holder still blocks the domain": refused(blocked, "held by run_a"),
        "recover retains the run": recovered.returncode == 0 and after == "failed_retained",
        "owner release after recovery": released.returncode == 0,
    }


def malformed(sc: Scenario) -> dict[str, bool]:
    """881: a malformed registry stops every command and is never reset."""
    sc.cli("status")
    sc.env.stop_launcher()
    (sc.env.registry / "registry.json").write_text("{not json")
    stopped = sc.cli("status")
    acquire = sc.cli("acquire", "--domain", "transform", "--run", "run_a", "--mode", "fixture")
    return {
        "status stops on a malformed registry": refused(stopped, "registry.json is not valid", REGISTRY_ERROR),
        "acquire stops on a malformed registry": refused(acquire, "registry.json is not valid", REGISTRY_ERROR),
        "file left as found": (sc.env.registry / "registry.json").read_text() == "{not json",
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


def worktrees(sc: Scenario) -> dict[str, bool]:
    """880 and 883: every worktree resolves the main checkout's registry; markers and registry IDs are enforced (no launcher starts)."""
    main, linked = scratch_repository(sc.env.temp / "repo")
    from_main = sc.cli("where", test=False, cwd=main, env={**os.environ, "PYTHONPATH": str(ROOT)})
    from_linked = sc.cli("where", test=False, cwd=linked, env={**os.environ, "PYTHONPATH": str(ROOT)})
    expected = str((main / "data" / "orchestration" / "registry").resolve())
    registry = main / "data" / "orchestration" / "registry"
    registry.mkdir(parents=True)
    (registry / "registry_id").write_text(json.dumps({"registry_id": "replaced", "kind": "test"}))
    (registry / "registry.json").write_text(json.dumps(run_lock.empty_state("other", run_lock.LAUNCHER_SCHEMA)))
    marked = sc.cli("status", test=False, cwd=main, env={**os.environ, "PYTHONPATH": str(ROOT)})
    (registry / "registry_id").write_text(json.dumps({"registry_id": "replaced", "kind": "real"}))
    changed = sc.cli("status", test=False, cwd=linked, env={**os.environ, "PYTHONPATH": str(ROOT)})
    real_in_test = sc.cli("acquire", "--domain", "transform", "--run", "run_r", "--mode", "real")
    real_registry = sc.cli("--test-registry", expected, "status", test=False, env={**os.environ, "PYTHONPATH": str(ROOT)})
    run_command("git", ["worktree", "remove", "--force", str(linked)], cwd=main, check=True)
    return {
        "main checkout resolves its own registry": from_main.returncode == 0 and json.loads(from_main.stdout)["path"] == expected,
        "linked worktree resolves the same registry": from_linked.returncode == 0 and json.loads(from_linked.stdout)["path"] == expected,
        "changed registry_id refused": refused(changed, "registry_id differs", REGISTRY_ERROR),
        "test marker in the real registry refused": refused(marked, "test registry", REGISTRY_ERROR),
        "real run refused in a test registry": refused(real_in_test, "real runs are refused in a test registry"),
        "a real registry cannot be used as a test registry": refused(real_registry, "not a test registry", REGISTRY_ERROR),
    }


def readers(sc: Scenario) -> dict[str, bool]:
    """777: reservations and registrations protect snapshot sets; a stale reader is removed only when confirmed."""
    reserved = sc.cli("reserve", "--reader", "train_1", "--domain", "training", "--publish-run", "pub_7", via_shell=True)
    protected = json.loads(sc.cli("protected-sets").stdout)
    registered = sc.cli("register-set", "--reader", "train_1", "--snapshot-set", "set_7")
    after = json.loads(sc.cli("protected-sets").stdout)
    stale = sc.status()["readers"]["train_1"]["stale"]
    unconfirmed = sc.cli("deregister-reader", "--reader", "train_1")
    confirmed = sc.cli("deregister-reader", "--reader", "train_1", "--owner-confirmed")
    final = json.loads(sc.cli("protected-sets").stdout)
    return {
        "reservation recorded": reserved.returncode == 0,
        "reservation protects its publish run": protected["publish_runs"] == ["pub_7"],
        "registration protects its snapshot set": registered.returncode == 0 and after["snapshot_sets"] == ["set_7"],
        "reader whose process ended is stale": stale is True,
        "stale reader kept without confirmation": refused(unconfirmed, "stale; needs --owner-confirmed"),
        "confirmed removal frees the set": confirmed.returncode == 0 and final == {"publish_runs": [], "snapshot_sets": []},
    }


SCENARIOS: list[tuple[str, Callable[[Scenario], dict[str, bool]]]] = [
    ("nested and competing launches", nested_and_competing),
    ("cross-domain prefixes", cross_domain),
    ("cross-domain prefixes, reverse order", cross_domain_reverse),
    ("cross-domain prefixes, whole segments", cross_domain_segments),
    ("failed run and owner release", failed_run),
    ("release predicates after success", success_release),
    ("16-process race", race),
    ("stale holder recovery", stale_holder),
    ("malformed registry", malformed),
    ("worktrees, markers and registry IDs", worktrees),
    ("readers and protected snapshot sets", readers),
]


def token_scan(sc: Scenario) -> bool:
    """882: no token appears in any output or in the registry files; the token store holds them."""
    tokens = []
    for path in Path(sc.env.settings["token_root"]).glob("*.token") if Path(sc.env.settings["token_root"]).is_dir() else []:
        tokens.append(path.read_text())
    stored = "".join(path.read_text(errors="replace") for path in sc.env.registry.glob("*.json"))
    text = "\n".join(sc.outputs) + stored
    return not any(token in text for token in tokens)


def revision() -> dict[str, Any]:
    """The code under test: commit and the hashes of the files, so edits during a run are visible."""
    files = {
        name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
        for name in ("scripts/lakehouse/run_lock.py", "orchestration/launcher.py", "scripts/lakehouse/run_lock_e2e.py", "scripts/lakehouse/fake_docker.py")
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
    before = revision()
    results: list[dict[str, Any]] = []
    for index, (name, scenario) in enumerate(SCENARIOS):
        env = Env(f"{index:02d}", report_dir)
        sc = Scenario(env)
        leftovers: list[str] = []
        try:
            checks = scenario(sc)
            if Path(env.settings["token_root"]).is_dir():
                checks["no token in outputs or registry files"] = token_scan(sc)
            error = ""
        except (AssertionError, KeyError, ValueError, OSError, launcher.ClientError) as exc:
            checks, error = {}, f"{type(exc).__name__}: {exc}"
        leftovers = env.close()
        checks["nothing of the scenario is left over"] = not leftovers
        status = "pass" if checks and all(checks.values()) and not error else "fail"
        results.append({"scenario": name, "status": status, "checks": checks, "error": error, "commands": sc.commands})
        sys.stdout.write(f"{'ok' if status == 'pass' else 'FAIL':<5} {name}\n")
        for check, ok in checks.items():
            if not ok:
                sys.stdout.write(f"      failed: {check}\n")
        if error:
            sys.stdout.write(f"      error: {error}\n")
    after = revision()
    report = {
        "feature": "run lock, registry and the launcher's request protocol (wave 0 items 1 and 2, launcher package)",
        "started_utc": started.isoformat(),
        "finished_utc": datetime.now(UTC).isoformat(),
        "python": sys.version.split()[0],
        "platform": sys.platform,
        "code": before,
        "code_changed_during_run": before != after,
        "command": ".venv/bin/python -m scripts.lakehouse.run_lock_e2e",
        "scenarios": results,
        "limits": [
            "Container checks use a fake Docker daemon; live Docker is checked by the launcher's fixture runs.",
            "flock is checked on the local disk only; process start times use macOS ps -o lstart=.",
            "The worktree scenario starts no launcher, so a launcher on a real-kind scratch registry (which would write the real token root) never runs.",
        ],
        "cleanup": "Scratch repositories and test registries stay in the scenario folders; each scenario stops its launcher and its fake daemon.",
    }
    passed = all(result["status"] == "pass" for result in results) and before == after
    report["status"] = "pass" if passed else "fail"
    (report_dir / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    sys.stdout.write(f"{report['status']}: {report_dir.relative_to(ROOT)}/report.json\n")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
