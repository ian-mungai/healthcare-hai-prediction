"""Scan staged changes, outgoing source files, or what a push sends (each commit message and patch, each tag message) with project-pinned Gitleaks."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from scripts.process import CompletedProcess, TimeoutExpired, run_command

ROOT = Path(__file__).resolve().parents[2]
LOGGER = logging.getLogger(__name__)
HISTORY_OPTIONS = ("--root", "--diff-merges=first-parent")


def execute(command: list[str], cwd: Path, environment: dict[str, str]) -> CompletedProcess[str]:
    """Execute fixed argument arrays without echoing scanner output or source values."""
    return run_command(command[0], command[1:], cwd=cwd, env=environment, timeout=180)


def git_output(git: str, repository: Path, environment: dict[str, str], *arguments: str) -> str:
    """The output of one Git command of the scan; a failed command stops the scan and is named, so a scan never passes over what it could not read."""
    result = execute([git, "-C", str(repository), *arguments], repository, environment)
    if result.returncode:
        raise ValueError(f"git {arguments[0]} failed (exit {result.returncode}), so the scan cannot read what the push sends")
    return result.stdout


def run_gitleaks(scratch: Path, mode: str, target: list[str], environment: dict[str, str], repository: Path, label: str) -> tuple[int, list[dict[str, Any]]]:
    """Run Gitleaks in one mode (``git``, ``dir``) and return its exit status and findings; a run that did not complete raises."""
    config = scratch / "gitleaks.toml"
    config.write_text("[extend]\nuseDefault = true\n", encoding="utf-8")
    report = scratch / f"findings_{label}.json"
    command = [
        str(ROOT / ".tools/bin/gitleaks"),
        mode,
        "--config",
        str(config),
        "--redact=100",
        "--no-banner",
        "--no-color",
        "--ignore-gitleaks-allow",
        "--gitleaks-ignore-path",
        str(scratch),
        "--report-format",
        "json",
        "--report-path",
        str(report),
        *target,
    ]
    result = execute(command, repository, environment)
    findings = json.loads(report.read_text(encoding="utf-8")) if report.exists() else None
    if result.returncode not in {0, 1} or not isinstance(findings, list):
        raise ValueError("Scanner did not complete")
    return result.returncode, findings


def scan_push(repository: Path, tips: tuple[str, ...], excluded: tuple[str, ...], tags: tuple[str, ...]) -> int:
    """Scan every commit a push newly sends (its message and its patch against its first parent) and the message of every tag object.

    The commits are those reachable from ``tips`` and not from ``excluded``, the objects the destination already holds. The log names commits, never a value.
    """
    git = shutil.which("git")
    if not git:
        raise ValueError("Git is required")
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("GITLEAKS_", "AWS_"))}
    scope = [*tips, *(["--not", *excluded] if excluded else [])]
    with tempfile.TemporaryDirectory(prefix="push_scan_") as directory:
        scratch = Path(directory)
        texts = scratch / "texts"
        texts.mkdir()
        commits = git_output(git, repository, environment, "rev-list", *scope, "--").split() if tips else []
        if commits:
            for record in git_output(git, repository, environment, "log", "-z", "--format=%H%x02%B", *scope, "--").split("\0"):
                identifier, _, message = record.strip("\n").partition("\x02")
                if identifier:
                    (texts / f"commit_{identifier}.txt").write_text(message, encoding="utf-8", errors="replace")
        for tag in tags:
            (texts / f"tag_{tag}.txt").write_text(
                git_output(git, repository, environment, "cat-file", "tag", tag).partition("\n\n")[2], encoding="utf-8", errors="replace"
            )
        findings: list[dict[str, Any]] = []
        status = 0
        if commits:
            # Gitleaks can report a clean scan after a Git that failed, so the same command runs here first, checked, and its output is dropped.
            git_output(git, repository, environment, "log", "-p", "-U0", *HISTORY_OPTIONS, *scope, "--")
            options = " ".join([*scope, *HISTORY_OPTIONS])
            code, found = run_gitleaks(scratch, "git", ["--log-opts", options, str(repository)], environment, repository, "history")
            status |= code
            findings += found
        if any(texts.iterdir()):
            code, found = run_gitleaks(scratch, "dir", [str(texts)], environment, repository, "messages")
            status |= code
            findings += [{"Commit": Path(str(finding.get("File", ""))).stem.split("_", 1)[-1]} for finding in found]
        named = sorted({str(finding.get("Commit", ""))[:12] for finding in findings if finding.get("Commit")})
        LOGGER.info("event=secret_scan status=%s findings=%s commits=%s", "passed" if status == 0 else "failed", len(findings), ",".join(named))
        return status


def scan(repository: Path, staged: bool) -> int:
    """Scan the real index or a temporary tree of nonignored repository source files."""
    git = shutil.which("git")
    if not git:
        raise ValueError("Git is required")
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("GITLEAKS_", "AWS_"))}
    with tempfile.TemporaryDirectory(prefix="source_scan_") as directory:
        scratch = Path(directory)
        if staged:
            status, findings = run_gitleaks(scratch, "git", ["--pre-commit", "--staged", str(repository)], environment, repository, "staged")
        else:
            listing = execute([git, "ls-files", "-z", "--cached", "--others", "--exclude-standard"], repository, environment)
            if listing.returncode:
                raise ValueError("Could not enumerate source files")
            tree = scratch / "source"
            tree.mkdir()
            for name in sorted(set(listing.stdout.split("\0")) - {""}):
                relative = Path(name)
                source = repository / relative
                if relative.is_absolute() or ".." in relative.parts or source.is_symlink():
                    raise ValueError("Unsafe source path")
                if source.is_file():
                    target = tree / relative
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(source, target)
            status, findings = run_gitleaks(scratch, "dir", [str(tree)], environment, repository, "tree")
        # Deliberately omit paths, lines and source fragments from logs.
        LOGGER.info("event=secret_scan status=%s findings=%s", "passed" if status == 0 else "failed", len(findings))
        return status


def main() -> None:
    """Fail closed on missing tools and scanner errors; never expose matching text."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument("--staged", action="store_true")
    parser.add_argument("--tip", action="append", default=[], metavar="SHA", help="scan the commits a push sends, up to this tip")
    parser.add_argument(
        "--exclude-file", type=Path, metavar="FILE", help="with --tip: the object IDs the destination holds, one per line; their history is not sent again"
    )
    parser.add_argument("--tag-object", action="append", default=[], metavar="SHA", help="scan the message of this pushed tag object")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        if args.tip or args.tag_object:
            excluded = tuple(args.exclude_file.read_text(encoding="utf-8").split()) if args.exclude_file else ()
            status = scan_push(args.repository.resolve(), tuple(args.tip), excluded, tuple(args.tag_object))
        else:
            status = scan(args.repository.resolve(), args.staged)
    except (OSError, ValueError, TimeoutExpired) as error:
        LOGGER.error("event=secret_scan status=blocked error_type=%s detail=%s", type(error).__name__, error if isinstance(error, ValueError) else "")
        raise SystemExit(2) from None
    raise SystemExit(status)


if __name__ == "__main__":
    main()
