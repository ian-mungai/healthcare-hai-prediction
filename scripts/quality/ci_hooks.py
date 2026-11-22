"""Run one complete partition of the repository's existing manual pre-commit stage."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import sys
import threading
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import Any

from scripts.process import LaunchesNotStopped, LaunchPool, ProcessGroupSurvived, TimeoutExpired, run_command
from scripts.quality import case_shards, ci_groups, core_commands
from scripts.quality.repo_checks import load_yaml

# A hook group in ``hook_shards`` of config/quality/ci_groups.json runs as that many shards. Each shard's hook entry reads its shard and its folder from these
# variables (pre-commit passes the environment through), and the suite names the cases that the shards split.
SHARD_VARIABLE = "CHECKS_SHARD"
SHARD_DIRECTORY_VARIABLE = "CHECKS_SHARD_DIRECTORY"
SHARD_SUITES = {"samples": "run_checks_e2e"}
# What pre-commit prints for a hook whose file filter matches no file of the checked range.
SKIPPED_MARKER = "(no files to check)Skipped"


def groups(root: Path) -> dict[str, list[str]]:
    """Return disjoint hook groups only when they cover the complete manual stage except its local CI aggregate."""
    config = json.loads((root / "config/quality/ci_groups.json").read_text())
    document = load_yaml((root / ".pre-commit-config.yaml").read_text())
    if not isinstance(document, dict):
        raise ValueError(".pre-commit-config.yaml must contain a mapping")
    expected = [
        hook["id"]
        for repo in document["repos"]
        for hook in repo["hooks"]
        if "manual" in hook.get("stages", document.get("default_stages", ["pre-commit", "manual"]))
    ]
    partitions: dict[str, list[str]] = config["hook_groups"]
    # A hook that a check group runs as the identical complete check is in no partition (``delegation_findings`` proves the group runs it).
    expected = [hook for hook in expected if hook not in config.get("hooks_run_by_groups", {})]
    actual = [hook for hooks in partitions.values() for hook in hooks]
    if Counter(actual) != Counter(expected):
        missing = sorted((Counter(expected) - Counter(actual)).elements())
        extra = sorted((Counter(actual) - Counter(expected)).elements())
        raise ValueError(f"manual hook partition is incomplete: missing={missing}, repeated or unknown={extra}")
    return partitions


def delegation_findings(root: Path, config: dict[str, Any] | None = None, document: Any = None, groups_directory: Path | None = None) -> list[str]:
    """Why a hook in ``hooks_run_by_groups`` is not the same check as its group's command, or is skipped by a path that selects the hook.

    The hook must take no file names and no arguments, the group's body file must run the identical command, and every tracked path that the hook's ``files``
    pattern matches must select the group (so the group runs wherever the hook would). A hook that fails these stays in a partition.
    """
    config = config or ci_groups.load()
    if document is None:
        document = load_yaml((root / ".pre-commit-config.yaml").read_text())
    folder = groups_directory or root / "scripts/ci/groups"
    hooks = {hook["id"]: hook for repo in document["repos"] for hook in repo["hooks"]}
    tracked = run_command("git", ["ls-files", "-z"], cwd=root, timeout=60, check=True).stdout.split("\0")
    problems = []
    for hook_id, group in config.get("hooks_run_by_groups", {}).items():
        hook = hooks.get(hook_id)
        if hook is None or "manual" not in hook.get("stages", document.get("default_stages", ["pre-commit", "manual"])):
            problems.append(f"{hook_id}: no such manual hook")
            continue
        if hook.get("pass_filenames", True) or hook.get("args"):
            problems.append(f"{hook_id}: the hook takes file names or arguments, so the complete run of group {group} is not the same check")
        command = f'"$python_bin" {hook["entry"].removeprefix(".venv/bin/python ").strip()}'
        if command not in core_commands.commands_of(folder / f"{group}.sh"):
            problems.append(f"{hook_id}: group {group} does not run the identical command {command}")
        pattern = re.compile(hook.get("files", ""))
        missed = [path for path in tracked if path and pattern.search(path) and group not in ci_groups.plan("paths", "all", paths=[path], config=config)["run"]]
        if missed:
            problems.append(f"{hook_id}: group {group} is not selected by {len(missed)} paths that select the hook, for example {missed[0]}")
    return problems


def usable(root: Path, value: str) -> bool:
    return bool(value and set(value) != {"0"} and run_command("git", ["cat-file", "-e", f"{value}^{{commit}}"], cwd=root, timeout=60).returncode == 0)


def group_deadline_seconds(root: Path, group: str) -> int:
    """Return the remaining hook budget from job start, reserving two minutes for evidence retention."""
    workflow = load_yaml((root / ".github/workflows/ci.yml").read_text())
    if not isinstance(workflow, dict):
        raise ValueError(".github/workflows/ci.yml must contain a mapping")
    # A sharded group runs as the job ``hook_<group>``; every other group is a row of the matrix of the job ``hooks``.
    sharded = workflow["jobs"].get(f"hook_{group}") if group in SHARD_SUITES else None
    if sharded is not None:
        seconds = int(sharded["timeout-minutes"]) * 60 - 120
    else:
        rows = workflow["jobs"]["hooks"]["strategy"]["matrix"]["include"]
        seconds = next(int(row["timeout"]) * 60 - 120 for row in rows if row["group"] == group)
    if seconds <= 0:
        raise ValueError(f"hook job {group} must allow more than the two-minute evidence margin")
    now = int(time.time())
    started = int(os.environ.get("JOB_STARTED", str(now)))
    return max(0, seconds - (now - started))


def run(root: Path, group: str, hooks: list[str], output: Path, start: str, end: str, shard: str = "") -> int:
    """Run unchanged hook entries within the workflow's group deadline and retain failures, including partial timeout output.

    With ``shard`` (K/N of a sharded group) the hook entry runs only that shard of its cases and writes its manifest to ``output``.
    """
    output.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    for name in (SHARD_VARIABLE, SHARD_DIRECTORY_VARIABLE):
        environment.pop(name, None)
    if shard:
        environment[SHARD_VARIABLE] = shard
        environment[SHARD_DIRECTORY_VARIABLE] = str(output)
    ranged = usable(root, start) and usable(root, end)
    selection = ["--from-ref", start, "--to-ref", end] if ranged else ["--all-files"]
    budget = group_deadline_seconds(root, group)
    deadline = time.monotonic() + budget
    results: list[dict[str, Any]] = []
    for hook in hooks:
        remaining = deadline - time.monotonic()
        command = [hook, *selection, "--hook-stage", "manual", "--show-diff-on-failure"]
        sys.stdout.write(f"Hook group {group}{f' shard {shard}' if shard else ''}: {hook}\n")
        sys.stdout.flush()
        started = time.monotonic()
        try:
            if remaining <= 0:
                raise TimeoutExpired(["pre-commit", "run", *command], budget)
            # A shard runs beside the other shards: its own session lets a stop reach its descendants, nested in another launch or not.
            result = run_command(str(root / ".venv/bin/pre-commit"), ["run", *command], cwd=root, env=environment, timeout=remaining, own_session=bool(shard))
        except TimeoutExpired as error:
            stdout = error.stdout.decode(errors="replace") if isinstance(error.stdout, bytes) else error.stdout or ""
            stderr = error.stderr.decode(errors="replace") if isinstance(error.stderr, bytes) else error.stderr or ""
            message = (
                f"no time left for hook {hook}: the {group} group budget is exhausted\n"
                if remaining <= 0
                else f"hook {hook} exceeded the remaining {remaining:.3f} second budget for group {group}\n"
            )
            if isinstance(error, ProcessGroupSurvived):
                message += f"the stop of hook {hook} is not confirmed: {error}\n"
            (output / f"{hook}.log").write_text(stdout + stderr + message)
            sys.stdout.write(stdout)
            sys.stderr.write(stderr + message)
            results.append({"hook": hook, "exit": 1, "seconds": round(time.monotonic() - started, 3), "command": command, "error": message.strip()})
            break
        (output / f"{hook}.log").write_text(result.stdout + result.stderr)
        sys.stdout.write(result.stdout)
        sys.stderr.write(result.stderr)
        row = {"hook": hook, "exit": result.returncode, "seconds": round(time.monotonic() - started, 3), "command": command}
        if shard and result.returncode == 0 and not (output / "manifest.json").exists() and SKIPPED_MARKER in result.stdout:
            row["skipped"] = True
        results.append(row)
    failed = len(results) != len(hooks) or any(row["exit"] != 0 for row in results)
    report = {
        "group": group,
        **({"shard": shard} if shard else {}),
        "selection": selection,
        "deadline_seconds": budget,
        "hooks": results,
        "status": "fail" if failed else "pass",
        "configuration_sha256": {
            path: hashlib.sha256((root / path).read_bytes()).hexdigest()
            for path in (".pre-commit-config.yaml", "config/quality/ci_groups.json", ".github/workflows/ci.yml")
        },
    }
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return int(failed)


def shard_folders(directory: Path, count: int) -> list[Path]:
    return [directory / f"shard_{index}" for index in range(1, count + 1)]


def shard_skipped(folder: Path) -> bool:
    """Whether the shard's report says that pre-commit skipped its hook because no file of the checked range matches the hook's filter."""
    try:
        report = json.loads((folder / "report.json").read_text(encoding="utf-8"))
        return (
            not (folder / "manifest.json").exists()
            and bool(report["hooks"])
            and all(row.get("skipped") is True and row["exit"] == 0 for row in report["hooks"])
        )
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return False


def combine(group: str, count: int, directory: Path) -> int:
    """Require every shard of a sharded group to have run exactly its cases, once each in all, or every shard to have skipped. Writes combined.json."""
    from scripts.quality import run_checks_e2e  # the case list of the suite that the shards split

    folders = shard_folders(directory, count)
    skipped = [shard_skipped(folder) for folder in folders]
    directory.mkdir(parents=True, exist_ok=True)
    if all(skipped):
        result: dict[str, Any] = {"group": group, "shards": count, "skipped": True, "problems": [], "status": "pass"}
        (directory / "combined.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        sys.stdout.write(f"Hook group {group}: every one of the {count} shards skipped, because no file of the checked range matches the hook's filter.\n")
        return 0
    if any(skipped):
        problems = [
            f"shard {index}/{count} skipped its hook but another shard ran: the shards must all run or all skip"
            for index, flag in enumerate(skipped, 1)
            if flag
        ]
        result = {"group": group, "shards": count, "problems": problems, "status": "fail"}
        (directory / "combined.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    else:
        suite = SHARD_SUITES[group]
        result = case_shards.combine(suite, run_checks_e2e.inventory(), count, directory, case_shards.saved_costs(suite))
    for problem in result["problems"]:
        sys.stderr.write(f"Hook group {group}: {problem}\n")
    if result["status"] == "pass":
        sys.stdout.write(f"Hook group {group}: all {count} shards ran every case once.\n")
    return int(result["status"] != "pass")


def run_shards(root: Path, group: str, hooks: list[str], output: Path, start: str, end: str, count: int) -> int:
    """Run every shard of a sharded group at once, each in its own folder under ``output``, and then combine them.

    The pool is a ``LaunchPool``: Ctrl-C stops every shard's session with its descendants before the interrupt goes on. TERM and HUP take the same path (the
    pool handles only SIGINT) and share the pool's one interrupt count: the first signal of any kind raises the interrupt and every later one is held until the
    stops are done. The CLI then exits 128 plus the number of that first signal (143 for TERM, 129 for HUP; an interrupt is raised on).
    """
    received: list[int] = []
    pool = LaunchPool(max_workers=count)

    def terminated(number: int, frame: FrameType | None) -> None:
        if pool.interrupts == 0:
            received.append(number)
        pool.interrupted(number, frame)

    previous = {}
    if threading.current_thread() is threading.main_thread():
        previous = {number: signal.signal(number, terminated) for number in (signal.SIGTERM, signal.SIGHUP)}
    try:
        with pool:
            futures = [
                pool.submit(run, root, group, hooks, folder, start, end, f"{index}/{count}") for index, folder in enumerate(shard_folders(output, count), 1)
            ]
            codes = [future.result() for future in futures]
    except KeyboardInterrupt:
        if not received:
            raise
        sys.stderr.write(f"Hook group {group}: stopped every shard after signal {signal.Signals(received[0]).name}.\n")
        return 128 + received[0]
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)
    return int(any(codes)) | combine(group, count, output)


def cli() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--group", required=True)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--from", dest="start", default="")
    parser.add_argument("--to", dest="end", default="")
    parser.add_argument("--output-directory", type=Path)
    parser.add_argument(
        "--shard", help="run only shard K/N of a sharded group (hook_shards in config/quality/ci_groups.json); its folder is shard_K under the output directory"
    )
    parser.add_argument("--combine", action="store_true", help="check the shard folders under the output directory of a sharded group instead of running it")
    parser.add_argument("--count", type=int, help="with --combine: the number of shards that the workflow ran")
    args = parser.parse_args()
    try:
        partitions = groups(args.root)
        if args.group not in partitions:
            raise ValueError(f"unknown hook group {args.group!r}, choose one of {', '.join(partitions)}")
        configured = json.loads((args.root / "config/quality/ci_groups.json").read_text()).get("hook_shards", {}).get(args.group, 0)
    except (ValueError, KeyError) as error:
        parser.error(str(error))
    if args.list:
        sys.stdout.write(json.dumps(partitions[args.group]) + "\n")
        return 0
    if (args.shard or args.combine or args.count is not None) and not configured:
        parser.error(f"hook group {args.group} is not sharded: add it to hook_shards in config/quality/ci_groups.json first")
    if args.combine and (args.shard or args.count is None):
        parser.error("--combine requires --count and takes no --shard")
    if args.count is not None and not args.combine:
        parser.error("--count belongs to --combine")
    if args.combine and args.count != configured:
        parser.error(f"the shard count {args.count} differs from hook_shards {configured} of group {args.group}")
    if args.shard:
        found = re.fullmatch(r"([1-9][0-9]*)/([1-9][0-9]*)", args.shard)
        if not found or int(found[2]) != configured or int(found[1]) > configured:
            parser.error(f"--shard must be K/{configured} with 1 <= K <= {configured} for group {args.group}, not {args.shard!r}")
    if configured:
        base = args.output_directory or args.root / "data/e2e/hooks" / args.group
        if args.combine:
            return combine(args.group, configured, base)
        if args.shard:
            return run(args.root, args.group, partitions[args.group], base / f"shard_{args.shard.split('/')[0]}", args.start, args.end, args.shard)
        return run_shards(args.root, args.group, partitions[args.group], base, args.start, args.end, configured)
    output = args.output_directory or args.root / "data/e2e/hooks" / args.group / f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{os.getpid()}"
    return run(args.root, args.group, partitions[args.group], output, args.start, args.end)


def main() -> int:
    """Run the CLI; an interrupt that left a shard's processes alive fails with their names instead of a traceback."""
    try:
        return cli()
    except LaunchesNotStopped as failure:
        sys.stderr.write(f"hook group: the interrupt did not stop every shard process: {failure}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
