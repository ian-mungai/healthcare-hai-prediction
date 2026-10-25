"""Check the package fixture kit end to end: a credential-free prepare in a fresh worktree and every check outcome.

Run ``.venv/bin/python -m scripts.lakehouse.run_package_fixture_e2e [--test-registry <folder>]`` with Docker
running and the analytics image built. The kit is a launcher client: the run holds one allocation for all its kit calls (the machine
admits one heavy job at a time) and every container is an attempt of a launcher template. Reports stay in data/e2e/package_fixture/.
It uses synthetic selectors for a test package (W0 to W2) on the beab859 fixture; real package selectors are frozen in
plans/parallel_work_20261010/selectors/. It needs the beab859 fixture base
build and baseline in the main checkout (data/analytics/dbt/e2e/base/, data/wave0/baselines/fixture_beab859/). It takes
about 20 minutes and is not part of the local CI script. Failure modes 737, 888 and 889 are in
plans/wave0_20261010/failure_modes.md.
"""

from __future__ import annotations

import argparse
import contextlib
import difflib
import fcntl
import hashlib
import json
import os
import shutil
import signal
import sys
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from orchestration import launcher
from scripts.lakehouse import launcher_cleanup
from scripts.process import CompletedProcess, run_command

ROOT = Path(__file__).resolve().parents[2]
MODEL = "dbt/models/silver/intermediate/int_occmix_survey_rows.sql"
TEST = "dbt/tests/assert_occmix_values_cast.sql"
BASELINE = "data/wave0/baselines/fixture_beab859/baseline.json"


def patch(folder: Path, name: str, path: str, change: Callable[[str], str]) -> Path:
    """Write a unified diff of one file of dbt/ with paths a/<path> and b/<path>."""
    before = (ROOT / path).read_text()
    lines = difflib.unified_diff(before.splitlines(keepends=True), change(before).splitlines(keepends=True), f"a/{path}", f"b/{path}")
    target = folder / f"{name}.patch"
    target.write_text("".join(lines))
    return target


def git_format(path: Path) -> Path:
    """The same patch with the headers git diff writes, which git apply once skipped silently inside the repository."""
    text = path.read_text()
    first = text.splitlines()[0].removeprefix("--- a/")
    target = path.with_name(path.stem + "_git.patch")
    target.write_text(f"diff --git a/{first} b/{first}\nindex 0000000..1111111 100644\n" + text)
    return target


# A model wrapped so it returns its rows plus one copy of its first row under a new key; {body} is the model's own SQL.
ADDED_ROW_MODEL = (
    "{{{{ config(materialized='table') }}}}\nselect * from (\n{body}\n) as kept\nunion all\n"
    "select * from (select * replace ('added-fixture-row' as survey_row_key) from (\n{body}\n) as copied limit 1)\n"
)


def add_row(text: str) -> str:
    """The model plus one copy of its first row under a new survey_row_key, as a fixture with an added row gives."""
    return ADDED_ROW_MODEL.format(body="\n".join(line for line in text.splitlines() if "config(" not in line))


def selector(folder: Path, package: str, tests: list[str], exclude: list[str] | None = None, added: dict[str, Any] | None = None) -> Path:
    """A synthetic selector contract in the selectors' format, owning the occupational-mix survey model.

    The silver refactor dropped four formula columns from that model (``changes`` in silver_columns.json), so it is compared
    with the beab859 baseline on its unchanged columns as a documented change, not as an unchanged model.
    """
    document = {
        "package": package,
        "owned_models": ["int_occmix_survey_rows"],
        "owned_seeds": [],
        "required_tests": tests,
        "required_test_count": len(tests),
        "retired_tests": [],
        "exclude_tests": exclude or [],
        "documented_changes": ["int_occmix_survey_rows"],
        "compare_unchanged_with_baseline": [],
        "baseline": BASELINE,
        "rule": "test selector for the kit's E2E",
        "note": "synthetic",
        **({"fixture_rows_added": added} if added else {}),
    }
    path = folder / f"{package.lower()}.yml"
    path.write_text(json.dumps(document, indent=2))  # JSON is valid YAML
    return path


CALLS: list[dict[str, Any]] = []
# The allocation this run holds for all its kit calls, so a package's kit job cannot take the machine between them.
HELD: dict[str, str] = {}
ADMISSION_WAIT_SECONDS = 3600
OPTIONS: dict[str, str | None] = {"registry": None}


def hold_machine() -> str:
    """Take one allocation for the whole run, waiting while another job holds the machine [759]."""
    client = launcher.load_client(OPTIONS["registry"])
    deadline = time.monotonic() + ADMISSION_WAIT_SECONDS
    while True:
        try:
            return client.allocate()
        except launcher.ClientError as refusal:
            if "holds the machine" not in str(refusal) or time.monotonic() > deadline:
                raise
            time.sleep(30)


def launcher_lock_free() -> bool:
    """True when no process holds the test registry's launcher lock: a live launcher holds it for its whole life, whatever its socket does."""
    lock = Path(str(OPTIONS["registry"])) / "launcher.lock"
    if not lock.exists():
        return True
    with lock.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        fcntl.flock(handle, fcntl.LOCK_UN)
    return True


def process_start(pid: int) -> str | None:
    """The process's start time as ``ps`` prints it, or None when no such process exists; with the PID it identifies one process."""
    result = run_command("ps", ["-o", "lstart=", "-p", str(pid)], timeout=10)
    return result.stdout.strip() or None if result.returncode == 0 else None


def lock_holders() -> list[int]:
    """The PIDs holding the test registry's launcher lock (``lsof``), to identify the launcher when its socket does not answer."""
    result = run_command("lsof", ["-t", str(Path(str(OPTIONS["registry"])) / "launcher.lock")], timeout=10)
    return sorted(int(line) for line in result.stdout.split() if line.isdigit())


def ping() -> dict[str, Any]:
    """The test-registry launcher's ping, or an empty dict when none answers (never starts one)."""
    try:
        return dict(launcher.load_client(OPTIONS["registry"], start=False).call("ping", timeout=10))
    except (launcher.ClientError, OSError):
        return {}


def start_own_launcher(nonce: str) -> dict[str, Any] | None:
    """Start the test-registry launcher with a one-time nonce and return its identity (PID, start time) only if the launcher that then
    serves reports that nonce: a launcher another client started in the meantime is never taken as this run's own (failure mode 919)."""
    client = launcher.load_client(OPTIONS["registry"], start=False)
    client.launch_nonce = nonce
    client.start_launcher()
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        reply = ping()
        if reply.get("launch_nonce") == nonce:
            started = process_start(int(reply["pid"]))
            return {"pid": int(reply["pid"]), "started": started, "nonce": nonce} if started else None
        if reply:
            return None  # another launcher serves: this run's own start lost the lock and exited
        time.sleep(0.2)
    return None


def stop_owned_launcher(owned: dict[str, Any] | None, nonce: str) -> None:
    """Stop the test-registry launcher this run started and prove it is gone (failure mode 919): its recorded process no longer exists and no
    process holds the launcher lock. Ownership is the nonce this run started it with; a launcher this run did not start is never touched."""
    if owned is None:
        reply = ping()  # identified after an early failure, but only by this run's nonce
        if reply.get("launch_nonce") == nonce and (started := process_start(int(reply["pid"]))):
            owned = {"pid": int(reply["pid"]), "started": started, "nonce": nonce}
    if owned is None:
        if lock_holders() and not ping():
            raise RuntimeError("cleanup unresolved: a launcher holds the test-registry lock and does not answer; this run cannot tell whether it is its own")
        sys.stderr.write("cleanup: no launcher of this run serves the test registry (another client's launcher, if any, is left running)\n")
        return
    if ping().get("pid") == owned["pid"]:
        # The request names this run's nonce, and the launcher that receives it refuses a nonce not its own: a replacement that took the
        # registry after this ping keeps serving.
        with contextlib.suppress(launcher.ClientError, OSError):
            launcher.load_client(OPTIONS["registry"], start=False).call("shutdown", {"owner": True, "launch_nonce": nonce}, {"kind": "any", "owner": True})
    other: dict[str, Any] = {}

    def gone() -> bool:
        if owned is not None and process_start(owned["pid"]) == owned["started"]:
            return False
        if launcher_lock_free():
            return True
        reply = ping()  # the lock is held: only a launcher that answers with another nonce is provably not this run's
        if reply and reply.get("launch_nonce") != nonce and owned is not None and int(reply["pid"]) != owned["pid"]:
            other.update(reply)
            return True
        return False

    for seconds, signal_name in ((30, None), (15, "TERM")):
        if signal_name and owned is not None and process_start(owned["pid"]) == owned["started"]:
            os.kill(owned["pid"], signal.SIGTERM)  # the same process (PID and start time), so no other process can receive it
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if gone():
                how = "after TERM" if signal_name else "on request"
                sys.stderr.write(f"cleanup: the test-registry launcher this run started (pid {owned and owned['pid']}) has stopped {how}\n")
                if other:
                    sys.stderr.write(f"cleanup: another client's launcher (pid {other['pid']}) serves the test registry and is left running\n")
                return
            time.sleep(0.5)
    raise RuntimeError(f"cleanup unresolved: the test-registry launcher this run started (pid {owned and owned['pid']}) or its lock remains")


def kit(*args: str, cwd: Path = ROOT) -> CompletedProcess[str]:
    """Run the kit's CLI under the run's allocation and keep its exit code and the tail of its error output for the report."""
    environment = {**os.environ, "HAI_ALLOCATION": HELD["allocation"]}
    registry = ["--test-registry", OPTIONS["registry"]] if OPTIONS["registry"] else []
    result = run_command(sys.executable, ["-m", "scripts.lakehouse.package_fixture", *registry, *args], cwd=cwd, env=environment, timeout=3600)
    CALLS.append({"args": [arg.replace(str(ROOT), ".") for arg in args], "exit": result.returncode, "stderr": result.stderr[-600:]})
    return result


def main() -> int:
    """Hold the machine for the whole run, run every scenario and free the allocation however the run ends."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--test-registry", help="a test registry folder; it never touches the real registry")
    args = parser.parse_args()
    launcher_cleanup.exit_on_termination()
    OPTIONS.update({"registry": args.test_registry})
    HELD.clear()
    # Only a launcher this run starts on a test registry is stopped at the end; the real registry's launcher is never stopped here. A free
    # launcher lock, not an unanswered ping, shows that no launcher runs before this run starts one.
    nonce = uuid.uuid4().hex
    started_own = False
    owned: dict[str, Any] | None = None
    try:
        if OPTIONS["registry"] is not None and launcher_lock_free():
            started_own = True
            owned = start_own_launcher(nonce)
        HELD["allocation"] = hold_machine()
        return scenarios()
    finally:
        with launcher_cleanup.defer_termination() as interrupted:
            try:
                if "allocation" in HELD:
                    # Never start a launcher to release: a gone launcher makes the release fail visibly instead.
                    launcher.load_client(OPTIONS["registry"], start=False).release_allocation(HELD["allocation"])
            finally:
                if started_own:
                    stop_owned_launcher(owned, nonce)
        if interrupted:
            raise SystemExit(128 + interrupted[0])


def scenarios() -> int:
    """Run every scenario, write the report and return 1 when any check fails."""
    started = datetime.now(UTC)
    folder = ROOT / "data" / "e2e" / "package_fixture" / started.strftime("%Y%m%dT%H%M%SZ")
    folder.mkdir(parents=True)
    checks: dict[str, bool] = {}
    evidence: dict[str, Any] = {}

    # 888: prepare in a fresh linked worktree that has no data/ at all (no secrets, no packages, no builds).
    worktree = folder / "worktree"
    run_command("git", ["worktree", "add", "--detach", "-q", str(worktree), "HEAD"], cwd=ROOT, check=True)
    for name in ("scripts/lakehouse", "orchestration", "services/hai"):
        shutil.copytree(ROOT / name, worktree / name, dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__"))  # the kit under test may be uncommitted
    for name in ("docker-compose.yaml",):
        shutil.copy2(ROOT / name, worktree / name)
    prepared = kit("prepare", cwd=worktree)
    record = worktree / "data/package_fixture/prepare.json"
    evidence["prepare"] = json.loads(record.read_text()) if record.exists() else prepared.stderr[-800:]
    checks["prepare builds in a worktree with no secrets"] = (
        prepared.returncode == 0 and not (worktree / "data/lakehouse/secrets").exists() and json.loads(record.read_text())["failed_nodes"] == []
    )
    run_command("git", ["worktree", "remove", "--force", str(worktree)], cwd=ROOT, check=True)

    # 889 and 737: record, verify, tampering, a missing required test and a changed kept model.
    w0 = selector(folder, "W0", ["assert_occmix_values_cast"])
    artifact = ROOT / "data/package_fixture/records/w0/effective_selection.json"
    artifact.unlink(missing_ok=True)
    recorded = kit("check", "--package", "W0", "--selector", str(w0), "--record")
    approved = folder / "approved_w0.json"
    if artifact.exists():
        shutil.copy2(artifact, approved)
    evidence["recorded"] = json.loads(artifact.read_text()) if artifact.exists() else recorded.stderr[-800:]
    checks["record writes the effective selection with both hashes"] = recorded.returncode == 0 and approved.exists()
    again = kit("check", "--package", "W0", "--selector", str(w0), "--record")
    checks["a second record is refused"] = again.returncode == 1 and "is recorded once" in again.stderr
    verified = kit("check", "--package", "W0", "--selector", str(w0), "--approved", str(approved))
    checks["check passes against the approved selection"] = verified.returncode == 0
    evidence["verified"] = verified.stderr[-800:]
    check_file = ROOT / "data/package_fixture/records/w0/check.json"
    report = json.loads(check_file.read_text()) if check_file.exists() else {"comparisons": {}}
    checks["the model equals the beab859 baseline on its unchanged columns"] = (
        report["comparisons"].get("int_occmix_survey_rows") == "equal on unchanged columns"
    )
    tampered = folder / "tampered_w0.json"
    content = json.loads(approved.read_text()) if approved.exists() else {"nodes": []}
    content["nodes"] = content["nodes"][1:]
    tampered.write_text(json.dumps(content))
    refused = kit("check", "--package", "W0", "--selector", str(w0), "--approved", str(tampered))
    checks["a selection that differs from the approved one fails"] = refused.returncode == 1 and "nodes differs from the approved selection" in refused.stderr
    w1 = selector(folder, "W1", ["assert_occmix_values_cast", "assert_test_that_does_not_exist"])
    (ROOT / "data/package_fixture/records/w1/effective_selection.json").unlink(missing_ok=True)
    missing = kit("check", "--package", "W1", "--selector", str(w1), "--record")
    checks["a missing required test fails"] = missing.returncode == 1 and "required test missing: assert_test_that_does_not_exist" in missing.stderr
    emptied = patch(folder, "empty_model", MODEL, lambda text: text.rstrip("\n") + "\nwhere false\n")
    changed = kit("check", "--package", "W0", "--selector", str(w0), "--patch", str(emptied), "--approved", str(approved))
    checks["a changed kept model fails the baseline comparison"] = changed.returncode == 1 and "int_occmix_survey_rows differs" in changed.stderr
    in_git_format = kit("check", "--package", "W0", "--selector", str(w0), "--patch", str(git_format(emptied)), "--approved", str(approved))
    checks["a patch in git diff format is applied, not skipped"] = in_git_format.returncode == 1 and "int_occmix_survey_rows differs" in in_git_format.stderr
    w2 = selector(folder, "W2", ["assert_occmix_values_cast"], exclude=["assert_occmix_layouts_known"])  # the silver refactor removed the hold test this used
    (ROOT / "data/package_fixture/records/w2/effective_selection.json").unlink(missing_ok=True)
    excluded = kit("check", "--package", "W2", "--selector", str(w2), "--record")
    w2_nodes = json.loads((ROOT / "data/package_fixture/records/w2/effective_selection.json").read_text())["nodes"] if excluded.returncode == 0 else []
    w0_nodes = json.loads(approved.read_text())["nodes"] if approved.exists() else []
    checks["an excluded test leaves the selection"] = (
        excluded.returncode == 0
        and any(n.endswith(".assert_occmix_layouts_known") for n in w0_nodes)
        and not any(n.endswith(".assert_occmix_layouts_known") for n in w2_nodes)
    )

    # 737: a documented added fixture row is left out of the comparison by its grain key; without the key it fails.
    extra = patch(folder, "added_row", MODEL, add_row)
    plain = kit("check", "--package", "W0", "--selector", str(w0), "--patch", str(extra), "--approved", str(approved))
    checks["an added row without a documented key fails the comparison"] = plain.returncode == 1 and "int_occmix_survey_rows differs" in plain.stderr
    documented = {"int_occmix_survey_rows": {"key": "survey_row_key", "rows": ["added-fixture-row"]}}
    w3 = selector(folder, "W3", ["assert_occmix_values_cast"], added=documented)
    (ROOT / "data/package_fixture/records/w3/effective_selection.json").unlink(missing_ok=True)
    keyed = kit("check", "--package", "W3", "--selector", str(w3), "--patch", str(extra), "--record")
    keyed_report = ROOT / "data/package_fixture/records/w3/check.json"
    verdict = json.loads(keyed_report.read_text())["comparisons"].get("int_occmix_survey_rows", "") if keyed_report.exists() else ""
    checks["a documented added row is compared on the baseline's keys"] = (
        keyed.returncode == 0 and verdict == "equal on unchanged columns (1 documented fixture rows added)"
    )
    # 737: a documented added row that the build lacks fails; so does a selector that names only the key column.
    w3_record = ROOT / "data/package_fixture/records/w3/effective_selection.json"
    dropped = kit("check", "--package", "W3", "--selector", str(w3), "--approved", str(w3_record))
    checks["a documented added row that is missing fails the comparison"] = (
        dropped.returncode == 1 and "added rows [] against documented ['added-fixture-row']" in dropped.stderr
    )
    w4 = selector(folder, "W4", ["assert_occmix_values_cast"], added={"int_occmix_survey_rows": "survey_row_key"})
    key_only = kit("check", "--package", "W4", "--selector", str(w4), "--patch", str(extra), "--approved", str(approved))
    checks["an added-rows entry without its keys is refused before a build"] = key_only.returncode == 1 and "rows: [<each added key>]" in key_only.stderr

    # 889: an expected failure passes only when exactly its named test fails.
    broken = patch(folder, "break_cast_test", TEST, lambda _text: "select 1 as failure\n")
    exact = kit(
        "check", "--package", "W0", "--selector", str(w0), "--approved", str(approved), "--expected-failure", f"cast:{broken}:assert_occmix_values_cast"
    )
    checks["an expected failure of its named test passes"] = exact.returncode == 0
    wrong = kit(
        "check",
        "--package",
        "W0",
        "--selector",
        str(w0),
        "--approved",
        str(approved),
        "--expected-failure",
        f"cast:{broken}:assert_occmix_holds_exclude_values",
    )
    checks["an expected failure of another test fails"] = wrong.returncode == 1 and "wanted only assert_occmix_holds_exclude_values to fail" in wrong.stderr

    passed = all(checks.values())
    code = {
        name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
        for name in ("scripts/lakehouse/package_fixture.py", "scripts/lakehouse/run_package_fixture_e2e.py")
    }
    result = {
        "feature": "package fixture kit (wave 0 item 9)",
        "started_utc": started.isoformat(),
        "finished_utc": datetime.now(UTC).isoformat(),
        "status": "pass" if passed else "fail",
        "commit": run_command("git", ["rev-parse", "HEAD"], cwd=ROOT).stdout.strip(),
        "code_sha256": code,
        "command": ".venv/bin/python -m scripts.lakehouse.run_package_fixture_e2e",
        "checks": checks,
        "evidence": evidence,
        "calls": CALLS,
        "limits": ["One synthetic package selector on the occupational-mix survey model; real selectors are checked when packages hand back work."],
    }
    (folder / "report.json").write_text(json.dumps(result, indent=2) + "\n")
    for name, ok in checks.items():
        sys.stdout.write(f"{'ok' if ok else 'FAIL':<5} {name}\n")
    sys.stdout.write(f"{result['status']}: {folder.relative_to(ROOT)}/report.json\n")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
