"""The push gate: the checks before every push to any branch, over the pushed range (the merge base of the pushed head with ``origin/main`` to the head).

Hosted CI runs every check group in full on the pushed commit (.github/workflows/ci.yml), and main moves to a commit only after that run is green. The gate
therefore runs only what hosted CI cannot do for a commit that is already public, in about two minutes. The commit stage holds the fast hooks
(.pre-commit-config.yaml).

The changed-file checks run first (scripts/quality/ci_changed_files.py: Ruff, mypy, SQLFluff, the Docker-call guard and the guard of the check group files).
A failed check stops the run. Then these steps run at once, except the OSV-Scanner hook, which runs through pre-commit (it takes the worktree's index lock):

- ``commit_messages`` and ``removed_names``: the commit-message and removed-name checks of the range (scripts/quality/repo_checks.py);
- ``secret_scan`` and ``privacy_scan``: the Gitleaks scan (scripts/quality/scan_secrets.py) and the privacy scan (scripts/quality/repo_checks.py privacy-scan)
  of every file Git does not ignore;
- ``secret_scan_pushed`` and ``privacy_scan_pushed``: the same scans over the commits the push newly sends, ``git rev-list <tip> --not --remotes=<remote>`` for
  every updated reference (the privacy scan reads the lines each commit adds), so a value that one commit adds and a later commit removes is found. They need
  no base, so they also cover a new branch and a force push. A pushed annotated tag has its message scanned by both scans (``secret_scan_tags`` and the
  privacy step), also when its commit is already on the remote. The scans stay at the push, so a commit made with its hooks bypassed, or in another clone, never
  reaches GitHub unchecked;
- ``osv_scanner``: ``pre-commit run osv-scanner --hook-stage pre-push`` over the range, or over all files when there is no range. The hook skips itself
  unless the range changes uv.lock, a requirements file or the allowlist.

Without a usable range (no ``origin/main`` or no merge base) the changed-file, commit-message and removed-name checks have nothing to read and are left out.
Hosted CI still runs every group.
Every step has the deadline ``pre_push.timeout_seconds`` (the changed-file checks have twice ``pre_push.changed_file_seconds``) and fails visibly with its
name when it expires. The run and every step start without the variables that change what pre-commit or the scans run (``EXECUTION_CONTROLS``, ``SKIP``
included) and without the Git repository variables of the caller.

The checks read the working tree, so ``run`` refuses a head that is not the checked-out commit and tracked files that differ from it (staged or unstaged).
The hook form, ``run --hook``, is the gate of every push. ``install-shim`` registers it as a Git config hook (Git 2.54 or newer; Git starts it before the
hooks-directory hook, for every push, with the whole input on standard input), so the gate does not depend on the reference pre-commit hands its hooks. It
judges each updated reference: a deletion is allowed; a reference that names a ``temp/`` branch (as its remote name, its local name or ``HEAD`` on
such a branch) is refused whatever its commit, because temporary code never reaches GitHub; a reference whose commit is already in ``origin/main`` is
allowed with nothing to check and is recorded in ``report.json``; a reference at the checked-out commit is checked; any other reference refuses the push.
The ``local_ci_pushed`` pre-commit hook runs
``check-shim``, which refuses a push when the Git config hook is missing, disabled or changed.

    .venv/bin/python -m scripts.quality.pre_push install-shim
    .venv/bin/python -m scripts.quality.pre_push plan [--root DIR] [--base SHA] [--head SHA]
    .venv/bin/python -m scripts.quality.pre_push run [same options] [--hook]
    .venv/bin/python -m scripts.quality.pre_push check-shim

The head is ``--head``, else ``HEAD``.
E2E: ``.venv/bin/python -m scripts.quality.run_ci_groups_e2e --selection-only``.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from typing import Any

from scripts.process import CompletedProcess, LaunchesNotStopped, LaunchPool, ProcessGroupSurvived, TimeoutExpired, run_command

CONFIG = Path("config/quality/ci_groups.json")
GIT_SECONDS = 120
SHIM_NAME = "hai-push-input"
SHIM_COMMAND = ".venv/bin/python -m scripts.quality.pre_push run --hook"
MINIMUM_GIT = (2, 54)
INSTALL_HINT = ".venv/bin/python -m scripts.quality.pre_push install-shim"
SHA = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
TEMP_PREFIX = "refs/heads/temp/"
SERIAL_KINDS = frozenset({"pre_commit"})
EXECUTION_CONTROLS = (
    "CI_SCOPE",
    "CI_TIMING_FILE",
    "CI_PART",
    "CI_UNITS",
    "CI_LIST_ONLY",
    "CI_ADMISSION_ONLY",
    "CI_WORKER_LIMIT_SECONDS",
    "CI_WORKER_GRACE_SECONDS",
    "CI_CORE_BUDGET",
    "PROCESS_LAUNCH_SESSION",
    "RANGE_START",
    "RANGE_END",
    "LAUNCHER_SHARD",
    "CI_LAUNCHER_LIMIT_SECONDS",
    "FEATURE_SHARD",
    "ACQUISITION_PART",
    "ACQUISITION_SHARD",
    "ACQUISITION_SHARD_DIR",
    "ACQUISITION_SHARD_COUNT",
    "GITHUB_ACTIONS",
    "PRE_COMMIT_ORIGIN",
    "PRE_COMMIT_SOURCE",
    "PRE_COMMIT_FROM_REF",
    "PRE_COMMIT_TO_REF",
    "PRE_COMMIT_REMOTE_BRANCH",
    "PRE_COMMIT_LOCAL_BRANCH",
    "PRE_COMMIT_REMOTE_NAME",
    "PRE_COMMIT_REMOTE_URL",
    "PRE_COMMIT_PRE_REBASE_UPSTREAM",
    "PRE_COMMIT_PRE_REBASE_BRANCH",
    "PRE_COMMIT_COMMIT_MSG_SOURCE",
    "PRE_COMMIT_COMMIT_OBJECT_NAME",
    "PRE_COMMIT_CHECKOUT_TYPE",
    "PRE_COMMIT_IS_SQUASH_MERGE",
    "PRE_COMMIT_REWRITE_COMMAND",
    "SKIP",
    "PRE_COMMIT_ALLOW_NO_CONFIG",
    "PRE_COMMIT_RUNNING_LEGACY",
    "_PRE_COMMIT_SKIP_POST_CHECKOUT",
)


def git(root: Path, *args: str) -> tuple[int, str]:
    result = run_command("git", ["-C", str(root), *args], timeout=GIT_SECONDS)
    return result.returncode, result.stdout.strip()


def resolve_commit(root: Path, name: str) -> str:
    """The commit a name resolves to, or an empty string."""
    if not name:
        return ""
    code, sha = git(root, "rev-parse", "--verify", f"{name}^{{commit}}")
    return sha if code == 0 else ""


def resolve_head(root: Path, head: str) -> str:
    return resolve_commit(root, head or "HEAD")


def resolve_base(root: Path, base: str, head: str) -> tuple[str, str]:
    """The start of the range and the reason it cannot be used (empty when it can)."""
    if not head:
        return "", "the pushed head is not a commit"
    if base:
        sha = resolve_commit(root, base)
        return (sha, "") if sha else ("", f"base {base} is not a commit")
    code, sha = git(root, "merge-base", head, "origin/main")
    return (sha, "") if code == 0 and sha else ("", "no merge base with origin/main (origin/main is missing or unrelated)")


def command(root: Path, step: dict[str, Any]) -> list[str]:
    """The command line of a step, for the plan and for ``launch``."""
    python = str(root / ".venv/bin/python")
    modules = {
        "repo_checks": "scripts.quality.repo_checks",
        "secret_scan": "scripts.quality.scan_secrets",
        "pre_commit": "pre_commit",
        "changed_files": "scripts.quality.ci_changed_files",
    }
    return [python, "-m", modules[step["kind"]], *step["arguments"]]


def launch(root: Path, step: dict[str, Any], environment: dict[str, str]) -> CompletedProcess[str]:
    """Start the step's command (the one ``command`` gives, written out with literal modules for the Docker-call guard)."""
    arguments, timeout = step["arguments"], step["timeout_seconds"]
    python = str(root / ".venv/bin/python")
    if step["kind"] == "repo_checks":
        return run_command(python, ["-m", "scripts.quality.repo_checks", *arguments], cwd=root, env=environment, timeout=timeout)
    if step["kind"] == "secret_scan":
        return run_command(python, ["-m", "scripts.quality.scan_secrets", *arguments], cwd=root, env=environment, timeout=timeout)
    if step["kind"] == "pre_commit":
        return run_command(python, ["-m", "pre_commit", *arguments], cwd=root, env=environment, timeout=timeout)
    return run_command(python, ["-m", "scripts.quality.ci_changed_files", *arguments], cwd=root, env=environment, timeout=timeout)


def output_directory(root: Path) -> Path:
    return root / "data/e2e/pre_push" / f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{os.getpid()}"


def remote_name(root: Path, given: str) -> str:
    """The remote whose commits a push does not send again: the one Git names (``origin`` without one), or every remote for a URL."""
    name = given or "origin"
    return name if name == "origin" or name in git(root, "remote")[1].split() else ""


def plan(
    root: Path, base: str = "", head: str = "", remote: str = "", tips: list[str] | None = None, tags: list[str] | None = None, checkout: bool = True
) -> dict[str, Any]:
    """The steps of the gate: ``tips`` are the commits it sends (the head by default), ``tags`` its tag objects, ``checkout`` whether it sends the head."""
    config = json.loads((root / CONFIG).read_text(encoding="utf-8"))
    end = resolve_head(root, head) if checkout else ""
    start, unusable = resolve_base(root, base, end) if checkout else ("", "")
    seconds = int(config["pre_push"]["timeout_seconds"])
    remote = remote_name(root, remote)
    tips = [end] if tips is None and end else list(tips or [])
    tags = list(tags or [])
    steps: list[dict[str, Any]] = []
    if start and end:
        steps += [
            {"name": "commit_messages", "kind": "repo_checks", "arguments": ["commit-range", start, end], "env": {}},
            {"name": "removed_names", "kind": "repo_checks", "arguments": ["removed-names"], "env": {"PRE_COMMIT_FROM_REF": start, "PRE_COMMIT_TO_REF": end}},
        ]
    if checkout:
        steps += [
            {"name": "secret_scan", "kind": "secret_scan", "arguments": [], "env": {}},
            {"name": "privacy_scan", "kind": "repo_checks", "arguments": ["privacy-scan"], "env": {}},
        ]
    sent = [part for tip in tips for part in ("--tip", tip)] + ["--remote", remote] * bool(tips)
    named = [part for tag in tags for part in ("--tag-object", tag)]
    if tips:
        steps.append({"name": "secret_scan_pushed", "kind": "secret_scan", "arguments": sent, "env": {}})
    if tags:
        steps.append({"name": "secret_scan_tags", "kind": "secret_scan", "arguments": named, "env": {}})
    if tips or tags:
        steps.append({"name": "privacy_scan_pushed", "kind": "repo_checks", "arguments": ["privacy-scan", *sent, *named], "env": {}})
    if checkout:
        scan_files = ["--from-ref", start, "--to-ref", end] if start and end else ["--all-files"]
        steps.append({"name": "osv_scanner", "kind": "pre_commit", "arguments": ["run", "osv-scanner", "--hook-stage", "pre-push", *scan_files], "env": {}})
    first = (
        {"name": "changed_file_checks", "kind": "changed_files", "arguments": ["--root", str(root), "--from", start, "--to", end], "env": {}}
        if start and end
        else None
    )
    for step in [*steps, *([first] if first else [])]:
        step["timeout_seconds"] = int(config["pre_push"]["changed_file_seconds"]) * 2 if step is first else seconds
        step["argv"] = command(root, step)
    if not checkout:
        reason = "the push sends no checked-out commit: only its tag messages are scanned"
    elif start and end:
        reason = f"range {start}..{end}"
    else:
        reason = f"{unusable}: the checks that read a range are left out and hosted CI runs every group"
    return {"reason": reason, "start": start, "end": end, "output": str(output_directory(root)), "first": first, "steps": steps}


def tag_object(root: Path, sha: str) -> bool:
    """Whether ``sha`` names a tag object (an annotated tag), whose message is text that reaches GitHub."""
    return sha.strip("0") != "" and git(root, "cat-file", "-t", sha) == (0, "tag")


@cache
def git_repository_variables() -> frozenset[str]:
    code, listing = git(Path.cwd(), "rev-parse", "--local-env-vars")
    return frozenset(listing.split()) if code == 0 else frozenset({"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"})


def step_environment(step: dict[str, Any]) -> dict[str, str]:
    """The environment of a step: the caller's without the execution controls and the Git repository variables, plus the step's own."""
    cleared = set(EXECUTION_CONTROLS) | git_repository_variables()
    return {**{key: value for key, value in os.environ.items() if key not in cleared}, **step["env"]}


def run_step(root: Path, output: Path, step: dict[str, Any]) -> dict[str, Any]:
    environment = step_environment(step)
    sys.stdout.write(f"pre-push: start {step['name']} (deadline {step['timeout_seconds']} s)\n")
    sys.stdout.flush()
    started = time.monotonic()
    error = ""
    try:
        result = launch(root, step, environment)
        code, text = result.returncode, result.stdout + result.stderr
    except ProcessGroupSurvived as survived:
        code, text, error = 1, "", f"step {step['name']} exceeded its deadline of {step['timeout_seconds']} seconds and its stop is not confirmed: {survived}"
    except TimeoutExpired:
        code, text, error = 1, "", f"step {step['name']} exceeded its deadline of {step['timeout_seconds']} seconds and was stopped"
    except FileNotFoundError as missing:
        code, text, error = 1, "", f"step {step['name']} could not start: {missing}"
    (output / f"{step['name']}.log").write_text(text + error + "\n", encoding="utf-8")
    return {"name": step["name"], "exit": code, "seconds": round(time.monotonic() - started, 1), "output": text, "error": error}


def run_steps(root: Path, output: Path, steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Run the steps at once, with the pre-commit steps serialized in one worker (pre-commit takes the worktree's index lock)."""
    serial = [step for step in steps if step["kind"] in SERIAL_KINDS]
    jobs = [[step] for step in steps if step["kind"] not in SERIAL_KINDS] + ([serial] if serial else [])
    finished: list[list[dict[str, Any]]] | None = None
    unstopped = ""
    if jobs:
        try:
            with LaunchPool(max_workers=len(jobs)) as pool:
                finished = list(pool.map(lambda job: [run_step(root, output, step) for step in job], jobs))
        except LaunchesNotStopped as failure:
            # The pool is left with a stop that was not confirmed. Without results (an interrupt) the failure goes to the caller; with them it is a failing row.
            if finished is None:
                raise
            unstopped = str(failure)
    rows: dict[str, dict[str, Any]] = {row["name"]: row for job in finished or [] for row in job}
    results = [rows[step["name"]] for step in steps]
    if unstopped:
        results.append({"name": "stop_confirmation", "exit": 1, "seconds": 0.0, "output": "", "error": unstopped})
    return results


def parse_updates(text: str) -> list[tuple[str, str, str]]:
    """The (remote reference, local object, local reference) triples of a pre-push input; a deletion has an all-zero object."""
    updates: list[tuple[str, str, str]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        fields = line.split()
        if len(fields) != 4 or not SHA.fullmatch(fields[1]):
            raise ValueError(f"unreadable line in the push input: {line!r}")
        updates.append((fields[2], fields[1], fields[0]))
    return updates


def judge(root: Path, sha: str, current: str) -> tuple[str, str]:
    """The decision for one updated reference and its reason: ``deleted``, ``known``, ``checked`` or ``refused``."""
    if not sha.strip("0"):
        return "deleted", "deletion"
    commit = resolve_commit(root, sha)
    if not commit:
        return "refused", "is not a commit in this repository"
    if git(root, "merge-base", "--is-ancestor", commit, "origin/main")[0] == 0:
        return "known", "its commit is already in origin/main"
    if commit == current:
        return "checked", "its commit is the checked-out commit"
    return "refused", f"is neither in origin/main nor the checked-out commit {current[:12]}"


def temporary(root: Path, remote_ref: str, local_ref: str) -> str:
    """The ``temp/`` branch a pushed reference names, as its remote name, its local name or the checked-out branch behind ``HEAD``; empty when none."""
    if local_ref == "HEAD":
        code, branch = git(root, "symbolic-ref", "-q", "HEAD")
        local_ref = branch if code == 0 else local_ref
    for name in (remote_ref, local_ref):
        if name.startswith(TEMP_PREFIX):
            return name
    return ""


def decide(root: Path, updates: list[tuple[str, str, str]]) -> list[dict[str, str]]:
    current = resolve_commit(root, "HEAD")
    rows = []
    for ref, sha, local in updates:
        decision, reason = judge(root, sha, current)
        named = temporary(root, ref, local) if decision != "deleted" else ""
        if named:
            decision, reason = "refused", f"names the temp/ branch {named}; temporary code stays on a local branch and is never pushed"
        rows.append({"ref": ref, "sha": sha, "decision": decision, "reason": reason})
    return rows


def refusals(root: Path, end: str) -> list[str]:
    """Reasons the checks of the working tree would not describe the push; empty when they do."""
    problems: list[str] = []
    current = resolve_commit(root, "HEAD")
    if end != current:
        problems.append(f"the pushed head {end[:12] or 'is not a commit'} is not the checked-out commit {current[:12]}")
    status, dirty = git(root, "status", "--porcelain", "--untracked-files=no")
    if status != 0:
        problems.append("the state of the working tree cannot be read (git status failed)")
    if dirty:
        problems.append("tracked files differ from HEAD (staged or unstaged): " + ", ".join(line.split(None, 1)[-1] for line in dirty.splitlines()[:10]))
    return problems


def refuse(problems: list[str]) -> int:
    sys.stdout.write("pre-push: refused. No check ran\n")
    sys.stdout.write("".join(f"pre-push:   {problem}\n" for problem in problems))
    sys.stdout.write("pre-push: push only references of the checked-out commit or of commits in origin/main. Commit or stash tracked changes\n")
    return 1


def record(output: Path, plan_made: dict[str, Any] | None, results: list[dict[str, Any]], decisions: list[dict[str, str]]) -> None:
    report = {"plan": plan_made, "results": results, "refs": decisions}
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")


def run(root: Path, base: str, head: str, push_input: str | None = None, remote: str = "") -> int:
    decisions: list[dict[str, str]] = []
    tips: list[str] | None = None
    tags: list[str] = []
    checkout = True
    if push_input is not None:
        try:
            decisions = decide(root, parse_updates(push_input))
        except ValueError as error:
            return refuse([str(error)])
        problems = [f"{row['ref']} ({row['sha'][:12]}) {row['reason']}" for row in decisions if row["decision"] == "refused"]
        if problems:
            return refuse(problems)
        for row in decisions:
            sys.stdout.write(f"pre-push: {row['ref']}: {row['decision']} ({row['reason']})\n")
        sent = [row for row in decisions if row["decision"] != "deleted"]
        tips = sorted({resolve_commit(root, row["sha"]) for row in sent})
        tags = sorted({row["sha"] for row in sent if tag_object(root, row["sha"])})
        checkout = any(row["decision"] == "checked" for row in decisions)
        if not checkout and not tags:
            sys.stdout.write("pre-push: nothing to check\n")
            if decisions:
                output = output_directory(root)
                output.mkdir(parents=True, exist_ok=True)
                record(output, None, [], decisions)
            return 0
        if not checkout:
            tips = []
    chosen = plan(root, base, head, remote, tips, tags, checkout)
    problems = refusals(root, chosen["end"]) if checkout else []
    if problems:
        return refuse(problems)
    output = Path(chosen["output"])
    output.mkdir(parents=True, exist_ok=True)
    sys.stdout.write(f"pre-push: {chosen['reason']}\n")
    first = chosen["first"]
    if first:
        first_result = run_step(root, output, first)
        sys.stdout.write(f"pre-push: {'PASS' if first_result['exit'] == 0 else 'FAIL'} {first['name']} ({first_result['seconds']} s)\n")
        if first_result["exit"] != 0:
            sys.stdout.write("\n".join(first_result["output"].splitlines()[-80:]) + "\n" + first_result["error"] + "\n")
            record(output, chosen, [{k: v for k, v in first_result.items() if k != "output"}], decisions)
            sys.stdout.write(f"pre-push: failed steps: {first['name']}. The other steps did not start. Logs are in {output}\n")
            return 1
    results = run_steps(root, output, chosen["steps"])
    for result in results:
        sys.stdout.write(f"pre-push: {'PASS' if result['exit'] == 0 else 'FAIL'} {result['name']} ({result['seconds']} s)\n")
        if result["exit"] != 0:
            sys.stdout.write("\n".join(result["output"].splitlines()[-80:]) + "\n" + result["error"] + "\n")
    failed = [result["name"] for result in results if result["exit"] != 0]
    record(output, chosen, [{k: v for k, v in result.items() if k != "output"} for result in results], decisions)
    if failed:
        sys.stdout.write(f"pre-push: failed steps: {', '.join(failed)}. Logs are in {output}\n")
        return 1
    sys.stdout.write(f"pre-push: all steps passed. Logs are in {output}\n")
    return 0


def check_shim(root: Path) -> int:
    """The check of the ``local_ci_pushed`` hook: the Git config hook that runs the gate is listed as active and holds the expected command."""
    listed_code, listing = git(root, "hook", "list", "pre-push")
    command_code, commands = git(root, "config", "--local", "--get-all", f"hook.{SHIM_NAME}.command")
    active = listed_code == 0 and SHIM_NAME in (line.strip() for line in listing.splitlines())
    if active and command_code == 0 and commands.splitlines() == [SHIM_COMMAND]:
        sys.stdout.write(f"pre-push: the Git config hook {SHIM_NAME} runs the gate\n")
        return 0
    sys.stdout.write(f"pre-push: refused. The Git config hook {SHIM_NAME} that runs the gate is missing, disabled or changed. Run `{INSTALL_HINT}` once\n")
    return 1


def install_shim(root: Path) -> int:
    code, banner = git(root, "--version")
    found = re.search(r"(\d+)\.(\d+)", banner)
    if code != 0 or not found or (int(found.group(1)), int(found.group(2))) < MINIMUM_GIT:
        sys.stdout.write(f"pre-push: the push-input hook needs Git {'.'.join(map(str, MINIMUM_GIT))} or newer, found {banner or 'no Git'}\n")
        return 1
    settings = ((f"hook.{SHIM_NAME}.event", "pre-push"), (f"hook.{SHIM_NAME}.command", SHIM_COMMAND), (f"hook.{SHIM_NAME}.enabled", "true"))
    for key, value in settings:
        if git(root, "config", "--local", "--replace-all", key, value)[0] != 0:
            sys.stdout.write(f"pre-push: could not set {key}\n")
            return 1
    sys.stdout.write(f"pre-push: the Git config hook {SHIM_NAME} runs the gate for every push\n")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("plan", "run", "check-shim", "install-shim"))
    parser.add_argument("remote", nargs="*", help="remote name and URL that Git passes to the hook")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--base", default="")
    parser.add_argument("--head", default="")
    parser.add_argument("--hook", action="store_true", help="read the push input from standard input and judge every updated reference")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    if args.command == "plan":
        sys.stdout.write(json.dumps(plan(root, args.base, args.head, args.remote[0] if args.remote else ""), indent=2) + "\n")
        return 0
    if args.command == "check-shim":
        return check_shim(root)
    if args.command == "install-shim":
        return install_shim(root)
    for name in EXECUTION_CONTROLS:
        os.environ.pop(name, None)
    return run(root, args.base, args.head, sys.stdin.read() if args.hook else None, args.remote[0] if args.remote else "")


if __name__ == "__main__":
    raise SystemExit(main())
