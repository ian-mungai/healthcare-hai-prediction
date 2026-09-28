"""Repository checks run by pre-commit and CI.

Run from the repository root: ``.venv/bin/python -m scripts.quality.repo_checks <check> [paths ...]``. Each finding
names the file and line, the rule, how to fix it and what to do when the rule seems wrong; any finding exits 1. A check
that cannot run raises and exits non-zero, so it fails closed. There are no bypasses. Markers the checks look for are
assembled from fragments, so this file does not flag itself. The rules are listed in README.md under Quality Checks.
"""

from __future__ import annotations

import argparse
import fnmatch
import os
import re
import sys
import tomllib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from scripts.process import run_command

POLICY = "README.md, Quality Checks"
ASK = "if the rule seems wrong here, stop and ask the repository owner; there are no bypasses (no SKIP= or --no-verify)"

CREDENTIAL_NAMES = ("*.tfstate", "*.tfstate.backup", "*.tfplan", "*.pem", "*.key", "*.p12", "*.pfx", "id_rsa", "id_ecdsa", "id_ed25519", "credentials")
DATA_EXTENSIONS = {".csv", ".tsv", ".parquet", ".xlsx", ".xls", ".jsonl", ".ndjson", ".avro", ".db", ".sqlite"}
DATA_FOLDERS = ("tests/fixtures/",)  # The only folder declared for data files: synthetic test fixtures.
MAX_BYTES = 5 * 1024 * 1024

NOQA = re.compile(r"#\s*" + "no" + r"qa\b(?::\s*(?P<codes>[A-Z]+\d+(?:\s*,\s*[A-Z]+\d+)*))?(?P<rest>.*)", re.IGNORECASE)
OTHER_SUPPRESSIONS = re.compile("|".join([r"#\s*type:\s*" + "ignore", "eslint" + "-disable", "@ts-" + "ignore", "@ts-" + "expect-error"]))
APPROVED_SUPPRESSIONS = {"S603": "scripts/process.py"}  # Rule code: the one file allowed to carry it.
REASON = re.compile(r"^\s+-\s+\S")
LAUNCHER = "scripts/process.py"
SUBPROCESS_IMPORT = re.compile(r"^\s*(?:import\s+" + "sub" + r"process\b|from\s+" + "sub" + r"process\s+import\b)", re.MULTILINE)

RUFF_SELECT = {"E", "F", "I", "B", "UP", "S", "SIM", "T20"}
RUFF_LINE_LENGTH = 160
RUFF_LOOSENING_KEYS = {"ignore", "extend-ignore", "per-file-ignores", "extend-per-file-ignores", "exclude", "extend-exclude"}
MYPY_FLAGS = ("--check-untyped-defs", "--disallow-untyped-defs")
MYPY_LOOSENING_KEYS = {"ignore_errors", "ignore_missing_imports", "follow_imports", "disable_error_code", "allow_untyped_defs", "allow_incomplete_defs"}
CI_SCRIPT = "scripts/run_ci.sh"

ENV_READ = re.compile(r"""(?:os\.environ(?:\.get)?\s*[\[(]\s*|os\.getenv\s*\(\s*)["']([A-Z][A-Z0-9_]*)["']""")
ENV_EXAMPLE_NAME = re.compile(r"^\s*#?\s*(?:export\s+)?([A-Z][A-Z0-9_]*)\s*=", re.MULTILINE)
SCRIPT_SUFFIXES = {".py", ".sh"}
FLAG = re.compile(r"""add_argument\(\s*["'](--[\w-]+)["']""")

COMMIT_TYPES = ("feat", "fix", "docs", "chore", "refactor", "test", "build", "ci", "perf", "style", "revert")
CONVENTIONAL_SUBJECT = re.compile(rf"(?:{'|'.join(COMMIT_TYPES)})(?:\([a-z0-9._/-]+\))?!?: \S.*")

AI_AGENTS = ("claude", "codex", "grok", "antigravity", "chatgpt", "copilot", "gemini", "cursor", "devin", "aider", "windsurf")
AI_NAMES = re.compile(rf"\b(?:{'|'.join((*AI_AGENTS, 'anthropic', 'openai', 'xai'))})\b", re.IGNORECASE)
AI_CREDIT = re.compile(r"^\s*(?:co-authored-by|co-developed-by|assisted-by|generated-by)\s*:\s*(.*)$", re.IGNORECASE)
AI_GENERATED = re.compile(r"^\s*(?:[🤖✨]\s*)?(?:generated (?:with|by)|written by)\b(.*)$", re.IGNORECASE)
AI_SESSION = re.compile(rf"^\s*(?:{'|'.join(AI_AGENTS)})-session\s*:", re.IGNORECASE)


@dataclass
class Finding:
    """One problem: where it is, what it is, the rule it breaks and how to fix it."""

    location: str
    problem: str
    rule: str
    fix: str


def git(*args: str) -> str:
    """Run Git in the current repository and return its output."""
    return run_command("git", args, check=True).stdout


def report(findings: list[Finding]) -> int:
    """Write each finding in the fixed four-part format and return the exit code."""
    for finding in findings:
        sys.stderr.write(f"{finding.location}: {finding.problem}\n  rule: {finding.rule}\n  fix: {finding.fix}\n  {ASK}\n")
    return 1 if findings else 0


def docs() -> list[Path]:
    """Return tracked Markdown files that describe current behavior."""
    return [Path(p) for p in git("ls-files", "*.md").splitlines()]


def mentions(name: str, files: list[Path]) -> list[str]:
    """Return ``file:line`` for every line in ``files`` that contains ``name``."""
    return [f"{file}:{number}" for file in files if file.exists() for number, line in enumerate(file.read_text().splitlines(), 1) if name in line]


def credential_files(paths: list[str]) -> list[Finding]:
    """Block environment files other than .env.example, Terraform state and saved plans, keys and cloud credentials."""
    findings = []
    for path in paths:
        name = Path(path).name
        is_env = (name == ".env" or name.startswith(".env.")) and name != ".env.example"
        if is_env or any(fnmatch.fnmatch(name, pattern) for pattern in CREDENTIAL_NAMES):
            fix = "remove it from the commit (git rm --cached) and ignore it; keep secrets in AWS Secrets Manager"
            findings.append(Finding(path, "credential, state or saved-plan file staged", f"{POLICY}: secrets and credential files", fix))
    return findings


def data_files(paths: list[str]) -> list[Finding]:
    """Block data files outside the declared folder and any file over 5 MB unless Git LFS stores it."""
    findings = []
    for path in paths:
        if Path(path).suffix.lower() in DATA_EXTENSIONS and not path.startswith(DATA_FOLDERS):
            fix = f"keep data in S3; put synthetic samples in {', '.join(DATA_FOLDERS)}; never commit real personal data"
            findings.append(Finding(path, "data file outside the declared folder", f"{POLICY}: data files", fix))
        if os.path.getsize(path) > MAX_BYTES and "filter: lfs" not in git("check-attr", "filter", "--", path):
            fix = "keep it out of Git (S3, a release asset) or track it with Git LFS"
            findings.append(Finding(path, f"{os.path.getsize(path):,} bytes, over the 5 MB limit", f"{POLICY}: data files", fix))
    return findings


def suppressions(paths: list[str]) -> list[Finding]:
    """Block suppression comments except the approved rule codes in their one approved file."""
    findings = []
    for path in paths:
        for number, line in enumerate(Path(path).read_text().splitlines(), 1):
            match = NOQA.search(line)
            codes = set(re.split(r"\s*,\s*", match.group("codes"))) if match and match.group("codes") else set()
            approved = (
                bool(codes) and all(APPROVED_SUPPRESSIONS.get(code) == path for code in codes) and bool(REASON.match(match.group("rest") if match else ""))
            )
            if (match and not approved) or OTHER_SUPPRESSIONS.search(line):
                fix = f"fix the code so the check passes; the only approved exception is S603 in {LAUNCHER}"
                findings.append(Finding(f"{path}:{number}", "suppression comment not in the approved list", f"{POLICY}: lint settings", fix))
    return findings


def subprocess_imports(paths: list[str]) -> list[Finding]:
    """Only the process launcher may import the standard library's process module."""
    findings = []
    for path in paths:
        if path == LAUNCHER:
            continue
        text = Path(path).read_text()
        for match in SUBPROCESS_IMPORT.finditer(text):
            number = text.count("\n", 0, match.start()) + 1
            fix = f"start programs with run_command from {LAUNCHER}"
            findings.append(Finding(f"{path}:{number}", "process module imported outside the launcher", f"{POLICY}: lint settings", fix))
    return findings


def lint_settings() -> list[Finding]:
    """Ruff keeps the standard rule sets and line length with no ignores; MyPy keeps its flags and has no loosening keys."""
    findings = []
    tool = tomllib.loads(Path("pyproject.toml").read_text()).get("tool", {})
    ruff = tool.get("ruff", {})
    lint = ruff.get("lint", {})
    missing = RUFF_SELECT - set(lint.get("select", ruff.get("select", [])))
    if missing:
        findings.append(
            Finding("pyproject.toml", f"Ruff rule sets missing from select: {', '.join(sorted(missing))}", f"{POLICY}: lint settings", "restore them")
        )
    if ruff.get("line-length") != RUFF_LINE_LENGTH:
        findings.append(
            Finding("pyproject.toml", f"Ruff line-length is {ruff.get('line-length')}, not {RUFF_LINE_LENGTH}", f"{POLICY}: lint settings", "restore 160")
        )
    loosening = sorted(RUFF_LOOSENING_KEYS & (set(ruff) | set(lint)))
    if loosening:
        findings.append(
            Finding("pyproject.toml", f"Ruff loosening keys present: {', '.join(loosening)}", f"{POLICY}: lint settings", "remove them and fix the code")
        )
    mypy_loosening = sorted(MYPY_LOOSENING_KEYS & set(tool.get("mypy", {})))
    if mypy_loosening:
        findings.append(Finding("pyproject.toml", f"MyPy loosening keys present: {', '.join(mypy_loosening)}", f"{POLICY}: lint settings", "remove them"))
    ci = Path(CI_SCRIPT).read_text()
    if not any(re.search(r"\bmypy\b", line) and all(flag in line for flag in MYPY_FLAGS) for line in ci.splitlines()):
        findings.append(Finding(CI_SCRIPT, f"MyPy no longer runs with {' and '.join(MYPY_FLAGS)}", f"{POLICY}: lint settings", "restore both flags"))
    return findings


def env_example() -> list[Finding]:
    """Every environment variable the code reads is listed in .env.example."""
    example = Path(".env.example")
    documented = set(ENV_EXAMPLE_NAME.findall(example.read_text())) if example.exists() else set()
    findings = []
    for path in git("ls-files", "*.py").splitlines():
        for number, line in enumerate(Path(path).read_text().splitlines(), 1):
            for name in ENV_READ.findall(line):
                if name not in documented:
                    fix = f"add {name}= with a comment to .env.example"
                    findings.append(Finding(f"{path}:{number}", f"{name} is read but not in .env.example", f"{POLICY}: docs match the code", fix))
    return findings


def removed_names() -> list[Finding]:
    """A removed script, CLI flag or environment variable no longer appears in the docs."""
    base = ["diff", os.environ["PRE_COMMIT_FROM_REF"], os.environ["PRE_COMMIT_TO_REF"]] if "PRE_COMMIT_FROM_REF" in os.environ else ["diff", "--cached"]
    removed = [Path(p).name for p in git(*base, "--name-only", "--diff-filter=D").splitlines() if Path(p).suffix in SCRIPT_SUFFIXES]
    tracked_code = "\n".join(Path(p).read_text() for p in git("ls-files", "*.py").splitlines() if Path(p).exists())
    lines = [line[1:] for line in git(*base, "-U0", "--", "*.py").splitlines() if line.startswith("-") and not line.startswith("---")]
    removed += [flag for line in lines for flag in FLAG.findall(line) if flag not in set(FLAG.findall(tracked_code))]
    removed += [name for line in lines for name in ENV_READ.findall(line) if name not in set(ENV_READ.findall(tracked_code))]
    findings = []
    for name in dict.fromkeys(removed):
        for location in mentions(name, [*docs(), Path(".env.example")]):
            findings.append(Finding(location, f"{name} was removed but is still documented", f"{POLICY}: docs match the code", "update or remove the text"))
    return findings


def is_conventional(subject: str) -> bool:
    """Return whether a commit subject follows Conventional Commits.

    Accepts ``feat(infra): add a tag``, ``fix: handle an empty file`` and ``feat(infra)!: drop a stack``; rejects
    ``Update README``, ``fix:handle empty input``, ``update(infra): add a tag`` and ``docs(readme): ``. Git's default
    "Merge ..." and "Revert ..." subjects fail: ``main`` keeps a linear history, and a revert is written ``revert: <subject>``.
    """
    return CONVENTIONAL_SUBJECT.fullmatch(subject) is not None


def commit_subjects(subjects: list[tuple[str, str]]) -> list[Finding]:
    """Check (location, subject) pairs and return a finding for each non-conforming subject."""
    fix = "reword as <type>(<scope>): <description>, for example docs(readme): add setup steps"
    return [
        Finding(where, f"not a Conventional Commit: {subject!r}", f"{POLICY}: commit messages", fix)
        for where, subject in subjects
        if not is_conventional(subject)
    ]


def attribution_findings(location: str, message: str) -> list[Finding]:
    """Check the whole commit message for AI credit, preserving human co-author trailers and ordinary mentions."""
    findings = []
    for number, line in enumerate(message.splitlines(), 1):
        credit = AI_CREDIT.match(line) or AI_GENERATED.match(line)
        if AI_SESSION.match(line) or (credit and AI_NAMES.search(credit.group(1))):
            findings.append(
                Finding(
                    f"{location}:{number}",
                    "AI attribution in commit message",
                    f"{POLICY}: commit messages",
                    "remove the AI credit or session line; keep human co-authors and factual descriptions of the change",
                )
            )
    return findings


def message_findings(location: str, message: str, *, comments: bool = False) -> list[Finding]:
    """Apply the subject and attribution policies without printing message bodies into check logs."""
    subject = next((line for line in message.splitlines() if not (comments and line.startswith("#"))), "")
    return commit_subjects([(location, subject)]) + attribution_findings(location, message)


def commit_message(path: str) -> list[Finding]:
    """Check the proposed message, excluding Git's edited verbose patch but preserving non-edited message bodies."""
    message = Path(path).read_text()
    if os.environ.get("GIT_EDITOR") != ":":
        comment = run_command("git", ["config", "--get", "core.commentString"])
        if comment.returncode == 1:
            comment = run_command("git", ["config", "--get", "core.commentChar"])
        if comment.returncode not in (0, 1):
            raise RuntimeError("cannot read Git's comment configuration")
        prefix = comment.stdout.strip() or "#"
        prefixes = list("#;@!$%^&|:") if prefix == "auto" else [prefix]
        for marker in (f"{item} ------------------------ >8 ------------------------" for item in prefixes):
            before, separator, after = message.partition(f"\n{marker}\n")
            if separator and any(line.startswith("diff --git ") for line in after.splitlines()):
                message = before
                break
    return message_findings(path, message, comments=True)


def commit_range(start: str, end: str) -> list[Finding]:
    """CI: check every complete commit message in ``start..end``; unreadable revisions fail closed."""
    findings = []
    for commit in git("rev-list", f"{start}..{end}").splitlines():
        message = git("show", "-s", "--format=%B", commit)
        findings.extend(message_findings(f"commit {commit}", message))
    return findings


def main() -> int:
    """Run the named check and report its findings."""
    checks = ["credential-files", "data-files", "suppressions", "subprocess-imports", "lint-settings", "env-example", "removed-names"]
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("check", choices=[*checks, "commit-msg", "commit-range"])
    parser.add_argument("paths", nargs="*")
    args = parser.parse_args()
    runners: dict[str, Callable[[], list[Finding]]] = {
        "credential-files": lambda: credential_files(args.paths),
        "data-files": lambda: data_files(args.paths),
        "suppressions": lambda: suppressions(args.paths),
        "subprocess-imports": lambda: subprocess_imports(args.paths),
        "lint-settings": lint_settings,
        "env-example": env_example,
        "removed-names": removed_names,
        "commit-msg": lambda: commit_message(args.paths[0]),
        "commit-range": lambda: commit_range(args.paths[0], args.paths[1]),
    }
    return report(runners[args.check]())


if __name__ == "__main__":
    sys.exit(main())
