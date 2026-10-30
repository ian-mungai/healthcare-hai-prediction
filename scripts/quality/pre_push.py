"""The pre-push run: the check groups of the pushed range, once, the way GitHub CI selects them (config/quality/ci_groups.json).

The commit stage holds only fast hooks (.pre-commit-config.yaml). Before a push this run covers the rest:

- ``range`` scope (the default): ``scripts/run_ci.sh`` with ``CI_SCOPE=range`` from the merge base of the pushed head with ``origin/main`` to the head, the
  commit-message and removed-name checks of the hosted hooks job, the three hosted hook partitions (scripts/quality/ci_hooks.py) and the acquisition
  gate (the ``acquisition-checks`` hook with its own path filter, as scripts/acquisition/run_checks_ci.sh runs it), all over the same range.
- ``full`` scope: ``scripts/run_ci.sh`` with ``CI_SCOPE=full`` (it holds the hook partitions and the acquisition suite) when the range changes a path in
  ``pre_push.full_paths`` (the selector, its configuration, the CI scripts and group bodies, the acquisition CI entry scripts, the workflow and the
  pre-commit configuration), when ``origin/main`` or the merge base cannot be found, or with ``--full``.
- Every scope also runs the ``osv-scanner`` hook (``pre-commit run osv-scanner --hook-stage pre-push``) over the range, or over all files when there is no
  range. Hosted CI does not run it, so a small change runs it too. The hook skips itself unless the range changes a pinned requirements file or its allowlist.
- A ``range`` of a small change (``pre_push.small_change``: at most ``max_files`` files and ``max_changed_lines`` changed lines, none on the ``not_paths``
  list, none a CI file) leaves out the acquisition hook and the three hook partitions. Hosted CI runs them.

Before any of these steps the changed-file checks run (scripts/quality/ci_changed_files.py: Ruff, mypy, SQLFluff, the Docker-call guard and the guard of the
check group files). A failed check stops the run before the long groups.

The steps run at once, except the acquisition hook, the OSV-Scanner hook and the hook partitions: pre-commit takes the worktree's index lock,
so they run one after the other in one worker.
Every step has a deadline from ``pre_push.timeout_seconds`` and fails visibly with its name when it expires. The run and every step start without the variables
that change what the CI scripts or pre-commit run (``EXECUTION_CONTROLS``, ``SKIP`` included) and without the Git repository variables of the caller.

The checks read the working tree, so ``run`` refuses a head that is not the checked-out commit and tracked files that differ from it (staged or unstaged).
The hook form, ``run --hook``, is the gate of every push. ``install-shim`` registers it as a Git config hook (Git 2.54 or newer; Git starts it before the
hooks-directory hook, for every push, with the whole input on standard input), so the gate does not depend on the reference pre-commit hands its hooks. It
judges each updated reference: a deletion is allowed; a reference whose commit is already in ``origin/main`` is allowed with nothing to check and is recorded
in ``report.json``; a reference at the checked-out commit is checked; any other reference refuses the push. The ``local_ci_pushed`` pre-commit hook runs
``check-shim``, which refuses a push when the Git config hook is missing, disabled or changed.

    .venv/bin/python -m scripts.quality.pre_push install-shim
    .venv/bin/python -m scripts.quality.pre_push plan [--root DIR] [--base SHA] [--head SHA] [--full]
    .venv/bin/python -m scripts.quality.pre_push run [same options] [--hook]
    .venv/bin/python -m scripts.quality.pre_push check-shim

The head is ``--head``, else ``HEAD``.
After the steps, in a ``finally`` path (also after a failed step or Ctrl-C), the run asks the launcher to quit Docker Desktop
(``python -m orchestration.launcher docker-release --owner --since <start of the run>``, one ``RELEASE_SECONDS`` deadline). The launcher skips it unless
a container was created since the start and refuses it while any other container or registry work runs. A skipped, refused or failed release never changes the
exit code: the run prints one line and records ``docker_release`` in ``report.json`` (after Ctrl-C it prints the line but writes no report).
``--release-registry DIR`` (for tests of this script only) passes ``--test-registry`` to the launcher.
E2E: ``.venv/bin/python -m scripts.quality.run_ci_groups_e2e --selection-only``.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from typing import Any

from scripts.process import CompletedProcess, TimeoutExpired, run_command

CONFIG = Path("config/quality/ci_groups.json")
GIT_SECONDS = 120
RELEASE_SECONDS = 600
SHIM_NAME = "hai-push-input"
SHIM_COMMAND = ".venv/bin/python -m scripts.quality.pre_push run --hook"
MINIMUM_GIT = (2, 54)
INSTALL_HINT = ".venv/bin/python -m scripts.quality.pre_push install-shim"
SHA = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
SERIAL_KINDS = frozenset({"pre_commit", "ci_hooks"})
# The gate releases Docker once after all its steps, so the run_ci.sh it starts skips its own release (failure mode R22).
OUTER_RELEASE = {"HAI_DOCKER_RELEASE": "outer"}
# The gate never reuses a local pass of a check group (docs/quality_checks.md; scripts/quality/ci_main_passes.py).
NO_REUSE = {"HAI_CI_REUSE": "off"}
EXECUTION_CONTROLS = (
    "CI_SCOPE",
    "CI_TIMING_FILE",
    "CI_PART",
    "CI_UNITS",
    "CI_LIST_ONLY",
    "RANGE_START",
    "RANGE_END",
    "LAUNCHER_SHARD",
    "CI_LAUNCHER_LIMIT_SECONDS",
    "FEATURE_SHARD",
    "ACQUISITION_PART",
    "ACQUISITION_SHARD",
    "ACQUISITION_SHARD_DIR",
    "ACQUISITION_SHARD_COUNT",
    "ACQUISITION_EXPECT",
    "ACQUISITION_RAN_MARKER",
    "GITHUB_ACTIONS",
    "HAI_DOCKER_RELEASE",
    "HAI_CI_REUSE",
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


def escalating_path(root: Path, config: dict[str, Any], start: str, end: str) -> str:
    code, listing = git(root, "diff", "--name-only", "--no-renames", start, end)
    if code != 0:
        return "<git diff failed>"
    patterns = [re.compile(pattern) for pattern in config["pre_push"]["full_paths"]]
    return next((path for path in listing.splitlines() if any(pattern.search(path) for pattern in patterns)), "")


def command(root: Path, step: dict[str, Any]) -> list[str]:
    """The command line of a step, for the plan and for ``launch``."""
    python = str(root / ".venv/bin/python")
    modules = {
        "repo_checks": "scripts.quality.repo_checks",
        "ci_hooks": "scripts.quality.ci_hooks",
        "pre_commit": "pre_commit",
        "changed_files": "scripts.quality.ci_changed_files",
    }
    return ["bash", "scripts/run_ci.sh"] if step["kind"] == "run_ci" else [python, "-m", modules[step["kind"]], *step["arguments"]]


def launch(root: Path, step: dict[str, Any], environment: dict[str, str]) -> CompletedProcess[str]:
    """Start the step's command (the one ``command`` gives, written out with literal programs and modules for the Docker-call guard)."""
    arguments, timeout = step["arguments"], step["timeout_seconds"]
    python = str(root / ".venv/bin/python")
    if step["kind"] == "repo_checks":
        return run_command(python, ["-m", "scripts.quality.repo_checks", *arguments], cwd=root, env=environment, timeout=timeout)
    if step["kind"] == "ci_hooks":
        return run_command(python, ["-m", "scripts.quality.ci_hooks", *arguments], cwd=root, env=environment, timeout=timeout)
    if step["kind"] == "pre_commit":
        return run_command(python, ["-m", "pre_commit", *arguments], cwd=root, env=environment, timeout=timeout)
    if step["kind"] == "changed_files":
        return run_command(python, ["-m", "scripts.quality.ci_changed_files", *arguments], cwd=root, env=environment, timeout=timeout)
    return run_command("bash", ["scripts/run_ci.sh"], cwd=root, env=environment, timeout=timeout)


def range_paths(root: Path, start: str, end: str) -> list[str]:
    code, listing = git(root, "diff", "--name-only", "--no-renames", start, end)
    return listing.splitlines() if code == 0 else []


def matching(patterns: list[str], paths: list[str]) -> list[str]:
    compiled = [re.compile(pattern) for pattern in patterns]
    return [path for path in paths if any(pattern.search(path) for pattern in compiled)]


def changed_lines(root: Path, start: str, end: str) -> int:
    """Added and removed lines of the range; a binary file counts as unbounded."""
    code, listing = git(root, "diff", "--numstat", "--no-renames", start, end)
    if code != 0:
        return 10**9
    total = 0
    for line in listing.splitlines():
        added, removed = line.split("\t")[:2]
        total += 10**9 if "-" in (added, removed) else int(added) + int(removed)
    return total


def small_change(root: Path, config: dict[str, Any], start: str, end: str, paths: list[str]) -> bool:
    """Whether the range is small: few files and lines, no infrastructure and no CI file (thresholds in ``pre_push.small_change``)."""
    small = config["pre_push"]["small_change"]
    if not paths or len(paths) > small["max_files"]:
        return False
    if matching([*small["not_paths"], *config["pre_push"]["full_paths"]], paths):
        return False
    return changed_lines(root, start, end) <= small["max_changed_lines"]


def output_directory(root: Path) -> Path:
    return root / "data/e2e/pre_push" / f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{os.getpid()}"


def plan(root: Path, base: str = "", head: str = "", full: bool = False) -> dict[str, Any]:
    config = json.loads((root / CONFIG).read_text(encoding="utf-8"))
    end = resolve_head(root, head)
    start, unusable = resolve_base(root, base, end)
    if full:
        scope, reason = "full", "requested with --full"
    elif unusable:
        scope, reason = "full", f"{unusable}: every group runs"
    else:
        trigger = escalating_path(root, config, start, end)
        scope, reason = ("full", f"{trigger} changes the group selector or the CI scripts: every group runs") if trigger else ("range", f"range {start}..{end}")
    output = output_directory(root)
    seconds = int(config["pre_push"]["timeout_seconds"][scope])
    paths = range_paths(root, start, end) if start and end else []
    small = scope == "range" and small_change(root, config, start, end, paths)
    steps: list[dict[str, Any]] = []
    if start and end:
        steps += [
            {"name": "commit_messages", "kind": "repo_checks", "arguments": ["commit-range", start, end], "env": {}},
            {"name": "removed_names", "kind": "repo_checks", "arguments": ["removed-names"], "env": {"PRE_COMMIT_FROM_REF": start, "PRE_COMMIT_TO_REF": end}},
        ]
    if scope == "range":
        steps.append(
            {
                "name": "local_ci",
                "kind": "run_ci",
                "arguments": [],
                "env": {"CI_SCOPE": "range", "RANGE_START": start, "RANGE_END": end, **OUTER_RELEASE, **NO_REUSE},
            }
        )
        if not small:
            steps.append(
                {
                    "name": "acquisition",
                    "kind": "pre_commit",
                    "arguments": ["run", "acquisition-checks", "--hook-stage", "pre-push", "--from-ref", start, "--to-ref", end],
                    "env": {},
                }
            )
            steps += [
                {
                    "name": f"hooks_{group}",
                    "kind": "ci_hooks",
                    "arguments": ["--group", group, "--from", start, "--to", end, "--output-directory", str(output / "hooks" / group)],
                    "env": {},
                }
                for group in config["hook_groups"]
            ]
    else:
        steps.append({"name": "local_ci", "kind": "run_ci", "arguments": [], "env": {"CI_SCOPE": "full", **OUTER_RELEASE, **NO_REUSE}})
    scan_files = ["--from-ref", start, "--to-ref", end] if start and end else ["--all-files"]
    steps.append({"name": "osv_scanner", "kind": "pre_commit", "arguments": ["run", "osv-scanner", "--hook-stage", "pre-push", *scan_files], "env": {}})
    first = (
        {"name": "changed_file_checks", "kind": "changed_files", "arguments": ["--root", str(root), "--from", start, "--to", end], "env": {}}
        if start and end
        else None
    )
    for step in [*steps, *([first] if first else [])]:
        step["timeout_seconds"] = seconds if step is not first else int(config["pre_push"]["changed_file_seconds"]) * 2
        step["argv"] = command(root, step)
    if small:
        reason += "; small change: the hook partitions and the acquisition hook stay with hosted CI"
    return {"scope": scope, "reason": reason, "start": start, "end": end, "output": str(output), "first": first, "steps": steps}


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
    except TimeoutExpired:
        code, text, error = 1, "", f"step {step['name']} exceeded its deadline of {step['timeout_seconds']} seconds and was stopped"
    except FileNotFoundError as missing:
        code, text, error = 1, "", f"step {step['name']} could not start: {missing}"
    (output / f"{step['name']}.log").write_text(text + error + "\n", encoding="utf-8")
    return {"name": step["name"], "exit": code, "seconds": round(time.monotonic() - started, 1), "output": text, "error": error}


def run_steps(root: Path, output: Path, steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Run the steps at once, except the pre-commit steps (the acquisition hook and the hook partitions).

    Pre-commit takes the worktree's index lock, so one worker runs those steps one after the other.
    """
    serial = [step for step in steps if step["kind"] in SERIAL_KINDS]
    jobs = [[step] for step in steps if step["kind"] not in SERIAL_KINDS] + ([serial] if serial else [])
    with ThreadPoolExecutor(max_workers=len(jobs)) as pool:
        finished = list(pool.map(lambda job: [run_step(root, output, step) for step in job], jobs))
    rows: dict[str, dict[str, Any]] = {row["name"]: row for job in finished for row in job}
    return [rows[step["name"]] for step in steps]


def release(root: Path, started: float, registry: str = "") -> dict[str, Any]:
    """Ask the launcher to quit Docker Desktop after a run that began at ``started``; never raises (owner answer 1, failure modes P3 to P5).

    The result holds ``outcome`` (``quit``, ``skipped``, ``refused`` or ``failed``), ``detail`` and ``seconds``.
    """
    python = str(root / ".venv/bin/python")
    registry_arguments = ["--test-registry", registry] if registry else []
    begun = time.monotonic()
    try:
        result = run_command(
            python,
            ["-m", "orchestration.launcher", *registry_arguments, "docker-release", "--owner", "--since", f"{started:.3f}"],
            cwd=root,
            env=step_environment({"env": {}}),
            timeout=RELEASE_SECONDS,
        )
    except TimeoutExpired:
        return {"outcome": "failed", "detail": f"exceeded {RELEASE_SECONDS} s and was stopped", "seconds": round(time.monotonic() - begun, 1)}
    except OSError as error:
        return {"outcome": "failed", "detail": f"could not start: {error}", "seconds": round(time.monotonic() - begun, 1)}
    seconds = round(time.monotonic() - begun, 1)
    message = (result.stderr.strip() or result.stdout.strip()).splitlines()
    if result.returncode == 3:
        return {"outcome": "refused", "detail": message[0] if message else "refused", "seconds": seconds}
    if result.returncode != 0:
        return {"outcome": "failed", "detail": f"exit {result.returncode}: {message[-1] if message else 'no output'}", "seconds": seconds}
    try:
        answer = json.loads(result.stdout)
        outcome, detail = str(answer["outcome"]), str(answer.get("reason") or answer.get("seconds") or "")
    except (ValueError, KeyError, TypeError):
        return {"outcome": "failed", "detail": f"unreadable answer: {result.stdout.strip()[-200:]}", "seconds": seconds}
    return {"outcome": outcome, "detail": detail, "seconds": seconds}


def release_line(released: dict[str, Any]) -> str:
    """The one line the gate prints for the release."""
    outcome, detail = released["outcome"], released["detail"]
    if outcome == "quit":
        return f"pre-push: Docker release: Docker Desktop quit in {released['seconds']} s (VM memory freed)\n"
    if outcome == "skipped":
        return f"pre-push: Docker release: skipped ({detail})\n"
    if outcome == "refused":
        return f"pre-push: Docker release: refused ({detail}); Docker left running\n"
    return (
        f"pre-push: Docker release: failed ({detail}); the gate result stands; "
        "run .venv/bin/python -m orchestration.launcher docker-start --owner before the next heavy run\n"
    )


def parse_updates(text: str) -> list[tuple[str, str]]:
    """The (remote reference, local object) pairs of a pre-push input; a deletion has an all-zero object."""
    updates: list[tuple[str, str]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        fields = line.split()
        if len(fields) != 4 or not SHA.fullmatch(fields[1]):
            raise ValueError(f"unreadable line in the push input: {line!r}")
        updates.append((fields[2], fields[1]))
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


def decide(root: Path, updates: list[tuple[str, str]]) -> list[dict[str, str]]:
    current = resolve_commit(root, "HEAD")
    rows = []
    for ref, sha in updates:
        decision, reason = judge(root, sha, current)
        rows.append({"ref": ref, "sha": sha, "decision": decision, "reason": reason})
    return rows


def refusals(root: Path, end: str) -> list[str]:
    """Reasons the checks of the working tree would not describe the push; empty when they do."""
    problems: list[str] = []
    current = resolve_commit(root, "HEAD")
    if end != current:
        problems.append(f"the pushed head {end[:12] or 'is not a commit'} is not the checked-out commit {current[:12]}")
    dirty = git(root, "status", "--porcelain", "--untracked-files=no")[1]
    if dirty:
        problems.append("tracked files differ from HEAD (staged or unstaged): " + ", ".join(line.split(None, 1)[-1] for line in dirty.splitlines()[:10]))
    return problems


def refuse(problems: list[str]) -> int:
    sys.stdout.write("pre-push: refused; no check ran\n")
    sys.stdout.write("".join(f"pre-push:   {problem}\n" for problem in problems))
    sys.stdout.write("pre-push: push only references of the checked-out commit or of commits in origin/main, and commit or stash tracked changes\n")
    return 1


def record(
    output: Path, plan_made: dict[str, Any] | None, results: list[dict[str, Any]], decisions: list[dict[str, str]], released: dict[str, Any] | None = None
) -> None:
    report = {"plan": plan_made, "results": results, "refs": decisions, "docker_release": released}
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")


def run(root: Path, base: str, head: str, full: bool, push_input: str | None = None, release_registry: str = "") -> int:
    decisions: list[dict[str, str]] = []
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
        if not any(row["decision"] == "checked" for row in decisions):
            sys.stdout.write("pre-push: nothing to check\n")
            if decisions:
                output = output_directory(root)
                output.mkdir(parents=True, exist_ok=True)
                record(output, None, [], decisions)
            return 0
    chosen = plan(root, base, head, full)
    problems = refusals(root, chosen["end"])
    if problems:
        return refuse(problems)
    output = Path(chosen["output"])
    output.mkdir(parents=True, exist_ok=True)
    sys.stdout.write(f"pre-push: scope {chosen['scope']}: {chosen['reason']}\n")
    first = chosen["first"]
    if first:
        first_result = run_step(root, output, first)
        sys.stdout.write(f"pre-push: {'PASS' if first_result['exit'] == 0 else 'FAIL'} {first['name']} ({first_result['seconds']} s)\n")
        if first_result["exit"] != 0:
            sys.stdout.write("\n".join(first_result["output"].splitlines()[-80:]) + "\n" + first_result["error"] + "\n")
            record(output, chosen, [{k: v for k, v in first_result.items() if k != "output"}], decisions)
            sys.stdout.write(f"pre-push: failed steps: {first['name']}; the long groups did not start; logs in {output}\n")
            return 1
    started = time.time()
    try:
        results = run_steps(root, output, chosen["steps"])
    finally:
        released = release(root, started, release_registry)
        sys.stdout.write(release_line(released))
        sys.stdout.flush()
    for result in results:
        sys.stdout.write(f"pre-push: {'PASS' if result['exit'] == 0 else 'FAIL'} {result['name']} ({result['seconds']} s)\n")
        if result["exit"] != 0:
            sys.stdout.write("\n".join(result["output"].splitlines()[-80:]) + "\n" + result["error"] + "\n")
    failed = [result["name"] for result in results if result["exit"] != 0]
    record(output, chosen, [{k: v for k, v in result.items() if k != "output"} for result in results], decisions, released)
    if failed:
        sys.stdout.write(f"pre-push: failed steps: {', '.join(failed)}; logs in {output}\n")
        return 1
    sys.stdout.write(f"pre-push: all steps passed; logs in {output}\n")
    return 0


def check_shim(root: Path) -> int:
    """The check of the ``local_ci_pushed`` hook: the Git config hook that runs the gate is listed as active and holds the expected command."""
    listed_code, listing = git(root, "hook", "list", "pre-push")
    command_code, commands = git(root, "config", "--local", "--get-all", f"hook.{SHIM_NAME}.command")
    active = listed_code == 0 and SHIM_NAME in (line.strip() for line in listing.splitlines())
    if active and command_code == 0 and commands.splitlines() == [SHIM_COMMAND]:
        sys.stdout.write(f"pre-push: the Git config hook {SHIM_NAME} runs the gate\n")
        return 0
    sys.stdout.write(f"pre-push: refused; the Git config hook {SHIM_NAME} that runs the gate is missing, disabled or changed; run `{INSTALL_HINT}` once\n")
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
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--hook", action="store_true", help="read the push input from standard input and judge every updated reference")
    parser.add_argument("--release-registry", default="", help="a launcher test registry folder for the Docker release (for tests of this script only)")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    if args.command == "plan":
        sys.stdout.write(json.dumps(plan(root, args.base, args.head, args.full), indent=2) + "\n")
        return 0
    if args.command == "check-shim":
        return check_shim(root)
    if args.command == "install-shim":
        return install_shim(root)
    for name in EXECUTION_CONTROLS:
        os.environ.pop(name, None)
    return run(root, args.base, args.head, args.full, sys.stdin.read() if args.hook else None, args.release_registry)


if __name__ == "__main__":
    raise SystemExit(main())
