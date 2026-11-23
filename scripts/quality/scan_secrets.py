"""Scan staged changes, outgoing source files, or what a push sends (each commit message and patch, each tag message) with project-pinned Gitleaks."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any

from scripts.process import CompletedProcess, TimeoutExpired, run_command

ROOT = Path(__file__).resolve().parents[2]
LOGGER = logging.getLogger(__name__)
GIT_SETTINGS = (
    ("color.ui", "never"),
    ("color.diff", "never"),
    ("diff.renames", "false"),
    ("log.showRoot", "true"),
    ("core.quotePath", "false"),
    ("diff.noprefix", "false"),
    ("diff.external", ""),
    ("core.bigFileThreshold", "2047m"),
)
GIT_VARIABLES = frozenset({"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_EXTERNAL_DIFF"})
GITLEAKS_CONFIG = "[extend]\nuseDefault = true\n"
LEAKS_EXIT = 2
SCANNED = re.compile(r"scanned ~(\d+) bytes")
LOG_ERROR = re.compile(r"\b(?:ERR|FTL)\b")


def execute(command: list[str], cwd: Path, environment: dict[str, str]) -> CompletedProcess[str]:
    """Execute fixed argument arrays without echoing scanner output or source values."""
    return run_command(command[0], command[1:], cwd=cwd, env=environment, timeout=180)


def git_output(git: str, repository: Path, environment: dict[str, str], *arguments: str) -> str:
    """The output of one Git command of the scan; a failed command stops the scan and is named, so a scan never passes over what it could not read."""
    result = run_command(git, ["-C", str(repository), *arguments], cwd=repository, env=environment, timeout=180, errors="surrogateescape", keep_newlines=True)
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


def scan_environment(environment: dict[str, str]) -> dict[str, str]:
    """The environment of every process a push scan starts, Gitleaks' own Git included: no user or system Git setting can change what is read."""
    kept = {key: value for key, value in environment.items() if not key.startswith(("GITLEAKS_", "AWS_", "GIT_CONFIG")) and key not in GIT_VARIABLES}
    kept |= {"GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_COUNT": str(len(GIT_SETTINGS)), "GITLEAKS_CONFIG_TOML": GITLEAKS_CONFIG}
    for index, (key, value) in enumerate(GIT_SETTINGS):
        kept |= {f"GIT_CONFIG_KEY_{index}": key, f"GIT_CONFIG_VALUE_{index}": value}
    return kept


def added_bytes(patch: str) -> int:
    """The bytes of the added lines of a patch as Gitleaks counts them in its Git mode: the raw bytes of each line and its newline, but no newline that
    the marker ``\\ No newline at end of file`` says is absent."""
    total, in_header, added = 0, False, False
    for line in patch.split("\n"):
        if line.startswith("diff --git "):
            in_header, added = True, False
        elif line.startswith("@@"):
            in_header, added = False, False
        elif line.startswith("\\") and added:
            total, added = total - 1, False
        elif line.startswith("+") and not in_header:
            total, added = total + len(line[1:].encode("utf-8", "surrogateescape")) + 1, True
        else:
            added = False
    return total


def gitleaks_in_memory(
    mode: str, target: list[str], environment: dict[str, str], repository: Path, expected: int, text: str | None = None
) -> tuple[int, list[dict[str, Any]]]:
    """Run Gitleaks with the report on standard output and its configuration and input in memory, and check what it did.

    Gitleaks reports a clean scan after a Git that failed or wrote to standard error, so its own figure of scanned bytes must reach ``expected``, the bytes
    this run read for it. A log line of an error, an exit status that is neither clean nor the leaks status, or a report that is not a list fails the scan.
    """
    arguments = [
        mode,
        "--ignore-gitleaks-allow",
        "--gitleaks-ignore-path",
        "/dev/null",
        "--exit-code",
        str(LEAKS_EXIT),
        "--report-format",
        "json",
        "--report-path",
        "-",
    ]
    arguments += ["--no-banner", "--no-color", "--redact=100", *target]
    result = run_command(
        str(ROOT / ".tools/bin/gitleaks"),
        arguments,
        cwd=repository,
        env=environment,
        timeout=180,
        input_text=text,
        errors="surrogateescape",
        keep_newlines=True,
    )
    scanned = SCANNED.search(result.stderr)
    try:
        findings = json.loads(result.stdout)
    except ValueError:
        findings = None
    if result.returncode not in {0, LEAKS_EXIT} or not isinstance(findings, list) or LOG_ERROR.search(result.stderr) or scanned is None:
        raise ValueError("Scanner did not complete")
    if int(scanned.group(1)) < expected:
        raise ValueError("Scanner read less than Git gave")
    return int(result.returncode == LEAKS_EXIT), findings


def scan_push(repository: Path, tips: tuple[str, ...], excluded: tuple[str, ...], tags: tuple[str, ...]) -> int:
    """Scan every commit a push newly sends (its message and its patch against its first parent) and the message of every tag object.

    The commits are those reachable from ``tips`` and not from ``excluded``, the objects the destination already holds. Nothing is written to disk: the
    configuration, the messages and the reports pass through the environment, standard input and standard output. The log names commits, never a value.
    """
    git = shutil.which("git")
    if not git:
        raise ValueError("Git is required")
    environment = scan_environment(dict(os.environ))
    scope = [*tips, *(["--not", *excluded] if excluded else [])]
    options = ["--no-textconv", "--no-ext-diff", *scope, "--root", "--diff-merges=first-parent", "--"]
    commits = git_output(git, repository, environment, "rev-list", *scope, "--").split() if tips else []
    findings: list[dict[str, Any]] = []
    status = 0
    if commits:
        # Gitleaks runs this very command and can call the scan clean after it failed: it runs here first, checked, and sets the figure to meet.
        patch = git_output(git, repository, environment, "log", "-p", "-U0", *options)
        code, found = gitleaks_in_memory("git", ["--log-opts", " ".join(options), str(repository)], environment, repository, added_bytes(patch))
        status |= code
        findings += found
    lines: list[str] = []
    owners: dict[int, str] = {}
    records = git_output(git, repository, environment, "log", "-z", "--format=%H%x02%B", *options).split("\0") if commits else []
    messages = [(identifier, "commit", message) for identifier, _, message in (record.strip("\n").partition("\x02") for record in records) if identifier]
    messages += [(tag, "tag", git_output(git, repository, environment, "cat-file", "tag", tag).partition("\n\n")[2]) for tag in tags]
    for identifier, kind, message in messages:
        lines += [""] if lines else []
        owners[len(lines) + 1] = identifier
        lines += [f"== {kind} {identifier} ==", *message.split("\n")]
    if lines:
        text = "\n".join(lines) + "\n"
        code, found = gitleaks_in_memory("stdin", [], environment, repository, len(text.encode("utf-8", "surrogateescape")), text)
        status |= code
        findings += [{"Commit": owners[max(number for number in owners if number <= int(finding.get("StartLine", 1)))]} for finding in found]
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
