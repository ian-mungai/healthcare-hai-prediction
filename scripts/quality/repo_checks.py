"""Repository checks run by pre-commit and CI.

Run from the repository root: ``.venv/bin/python -m scripts.quality.repo_checks <check> [paths ...]``. Each finding
names the file and line, the rule, how to fix it and what to do when the rule seems wrong; any finding exits 1. A check
that cannot run raises and exits non-zero, so it fails closed. There are no bypasses. Markers the checks look for are
assembled from fragments, so this file does not flag itself. The rules are listed in README.md under Quality Checks.
"""

from __future__ import annotations

import argparse
import datetime
import fnmatch
import importlib
import json
import os
import re
import sys
import tempfile
import tomllib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from scripts.process import run_command

POLICY = "README.md, Quality Checks"
ASK = "if the rule seems wrong here, stop and ask the repository owner; there are no bypasses (no SKIP= or --no-verify)"

CREDENTIAL_NAMES = ("*.tfstate", "*.tfstate.backup", "*.tfplan", "*.pem", "*.key", "*.p12", "*.pfx", "id_rsa", "id_ecdsa", "id_ed25519", "credentials")
DATA_EXTENSIONS = {".csv", ".tsv", ".parquet", ".xlsx", ".xls", ".jsonl", ".ndjson", ".avro", ".db", ".sqlite"}
# Folders declared for data files: synthetic test fixtures and the reviewed dbt seeds, which hold file labels and
# checksums from the S3 manifests, never source data values (owner approval of the dbt staging commit, Oct 4 2026).
DATA_FOLDERS = ("tests/fixtures/", "dbt/seeds/")
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
SCRIPT_SUFFIXES = {".py", ".sh", ".js", ".mjs", ".ts"}
FLAG = re.compile(r"""add_argument\(\s*["'](--[\w-]+)["']""")
CLEANUP_ALLOWLIST = ".cleanup_allowlist"
CLEANUP_RULE = f"{POLICY}: removed and unused code"
REMOVED_KINDS = ["script", "flag", "env", "python", "config", "terraform", "dbt", "airflow", "dependency"]
UNUSED_KINDS = ["unused-code", "unused-dependencies", "orphan", "terraform-unused"]
CODE_SUFFIXES = {".py", ".sh", ".js", ".mjs", ".ts", ".sql", ".tf", ".yaml", ".yml", ".toml", ".json", ".ini", ".cfg", ".j2", ".jinja"}
CODE_NAMES = {"Dockerfile", "Makefile", ".env.example"}
CONFIG_SUFFIXES = {".yaml", ".yml", ".toml", ".json", ".ini", ".cfg"}
PY_DEFINITION = re.compile(r"^[ \t]*(?:async[ \t]+def|def|class)[ \t]+([A-Za-z_]\w*)", re.MULTILINE)
PY_TOP_LEVEL = re.compile(r"^(?:async[ \t]+def|def|class)[ \t]+([A-Za-z_]\w*)", re.MULTILINE)
PY_ASSIGNED = re.compile(r"^[ \t]*([A-Za-z_]\w*)[ \t]*(?::[^=\n]*)?=(?!=)", re.MULTILINE)
# Environment variables a shell script, workflow or rendered template still uses: $NAME, ${NAME}, vars.NAME, secrets.NAME, env.NAME.
ENV_USE = re.compile(r"\$\{?([A-Z][A-Z0-9_]*)\b|\b(?:vars|secrets|env)\.([A-Z][A-Z0-9_]*)\b")
PYTHON_REFERENCE_SUFFIXES = {".py", ".yaml", ".yml", ".toml", ".cfg", ".ini"}
CONFIG_KEY = re.compile(r"""^\s*(?:-\s+)?["']?([A-Za-z_][\w-]*)["']?\s*[:=](?!:)""")
TF_BLOCK = re.compile(r'^\s*(variable|output|module|resource|data)\s+"([\w-]+)"(?:\s+"([\w-]+)")?', re.MULTILINE)
TASK_ID = re.compile(r"""task_id\s*=\s*["']([\w.-]+)["']""")
REQUIREMENT = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*(?:\[[^\]]*\])?\s*(?:[=<>!~;@]|$)")
# Distribution names whose import name differs; any other package imports as its name with dashes as underscores.
MODULE_NAMES = {
    "pyyaml": "yaml",
    "beautifulsoup4": "bs4",
    "scikit-learn": "sklearn",
    "pillow": "PIL",
    "python-dateutil": "dateutil",
    "psycopg2-binary": "psycopg2",
}
VULTURE_CONFIDENCE = 60  # Unused functions, classes and variables; lower levels flag dynamic uses vulture cannot see.
DEPTRY_CODES = {"DEP001", "DEP002", "DEP003"}  # Missing, unused and transitive-only dependencies.
DEPTRY_EXCLUDED = ("e2e", "data", r"\.review_dependencies", r"\.tools")  # Local records and installed tools, not project code.
TOOL_COMMENT = re.compile(r"#\s*(?:tool|dynamic):\s*\S")  # A requirement used as a command or imported dynamically, with why.
# Files every repository may hold without another file naming them.
WELL_KNOWN_FILES = {
    "README.md",
    "LICENSE",
    "LICENSE.md",
    "CHANGELOG.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "CODE_OF_CONDUCT.md",
    "AGENTS.md",
    "CLAUDE.md",
    "GEMINI.md",
    "VERSION",
    "requirements.txt",
    "pyproject.toml",
    "package.json",
    "package-lock.json",
    "ruff.toml",
    "setup.cfg",
    "Makefile",
    "Dockerfile",
    "__init__.py",
    "conftest.py",
}


COMMIT_TYPES = ("feat", "fix", "docs", "chore", "refactor", "test", "build", "ci", "perf", "style", "revert")
CONVENTIONAL_SUBJECT = re.compile(rf"(?:{'|'.join(COMMIT_TYPES)})(?:\([a-z0-9._/-]+\))?!?: \S.*")

AI_AGENTS = ("claude", "codex", "grok", "antigravity", "chatgpt", "copilot", "gemini", "cursor", "devin", "aider", "windsurf")
AI_NAMES = re.compile(rf"\b(?:{'|'.join((*AI_AGENTS, 'anthropic', 'openai', 'xai'))})\b", re.IGNORECASE)
AI_CREDIT = re.compile(r"^\s*(?:co-authored-by|co-developed-by|assisted-by|generated-by)\s*:\s*(.*)$", re.IGNORECASE)
AI_GENERATED = re.compile(r"^\s*(?:[🤖✨]\s*)?(?:generated (?:with|by)|written by)\b(.*)$", re.IGNORECASE)
AI_SESSION = re.compile(rf"^\s*(?:{'|'.join(AI_AGENTS)})-session\s*:", re.IGNORECASE)

PRIVACY_RULE = f"{POLICY}: privacy scan"
PRIVACY_FIX = (
    "replace the value with a placeholder (~, $TMPDIR, <name>, example.invalid) or read it from configuration;"
    " if it is meant to be public, add a reasoned entry to .privacy_allowlist"
)
ALLOWLIST = ".privacy_allowlist"
PRIVACY_PATTERNS = {
    "home-path": ("home-directory path with a user name", re.compile(r"/(?:" + "Us" + r"ers|home)/(?![<$])(?!Shared/)[A-Za-z0-9._-]+")),
    "temp-path": ("machine temporary path", re.compile(r"/var/" + r"folders/[A-Za-z0-9_+-]+/[A-Za-z0-9_+-]+")),
    "email": ("email address", re.compile(r"\b[A-Za-z0-9._%+-]+@((?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,})\b")),
    "phone": (
        "phone number",
        re.compile(
            r"(?<![\w.])(?:\+\d{1,3}[ .-]?)?(?:\(\d{3}\)[ .-]?|\d{3}[ .-])\d{3}[ .-]\d{4}(?![\w.])"
            r"|(?<![\w.])\+\d{1,3}[ -]\d{2,4}[ -]\d{3}[ -]\d{3,4}(?![\w.])"
        ),
    ),
    "aws-account": ("AWS account ID", re.compile(r"arn:aws[a-z-]*:[a-z0-9-]*:[a-z0-9-]*:\d{12}:|(?i:account[_ -]?id)\W{1,4}\d{12}\b")),
}
RESERVED_DOMAINS = (".invalid", ".test", ".localhost", ".example", "example.com", "example.org", "example.net")
ENV_VALUE_KEYS = re.compile(r"PROFILE|BUCKET|ACCOUNT|ARN|ENDPOINT|HOST|EMAIL|USER")
WRITING_RULE = f"{POLICY}: writing check (Oxford comma, date format, time-bound words; em dashes in declared article files)"
WRITING_ALLOWLIST = ".writing_allowlist"
# Checked in prose only: code spans, fenced blocks, front matter, URLs and link targets are masked first.
WRITING_PATTERNS = {
    "comma-and": (
        "comma before a final 'and', 'or' or 'nor'",
        re.compile(r",\s+(?:and|or|nor)\b"),
        "drop the comma, or split the sentence in two",
    ),
    "iso-date": ("ISO date in prose", re.compile(r"\b\d{4}-\d{2}-\d{2}\b"), "write the date as Sep 30 2026, or put a machine value in `code`"),
    # Only the always time-bound words of documents.md 6.10; "now", "new", "latest", "yet" and "old" have timeless
    # procedural uses and are judged in the documentation review.
    "time-word": (
        "time-bound word in prose",
        re.compile(r"(?i)\b(?:currently|recently|eventually|(?<!as )soon(?! as)|as of this writing|at present|in the future|for now)\b"),
        "state the fact without the time word, or tie it to a version, commit or issue",
    ),
    "em-dash": ("em dash in article text", re.compile("\u2014"), "use a colon, comma, parentheses or a new sentence"),
}
ARTICLE_ONLY = {"em-dash"}
SKIPPED_DIRS = {".git", ".venv", ".tools", "node_modules", "__pycache__", ".mypy_cache", ".ruff_cache", ".pytest_cache", ".terraform"}


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
    if example.exists():
        others = "\n".join("\n".join(lines) for path, lines in reference_files().items() if path != ".env.example")
        for number, line in enumerate(example.read_text().splitlines(), 1):
            match = ENV_EXAMPLE_NAME.match(line)
            if match and not re.search(rf"\b{match.group(1)}\b", others):
                fix = f"remove {match.group(1)} from .env.example, or restore the code that reads it"
                findings.append(Finding(f".env.example:{number}", f"{match.group(1)} is documented but nothing reads it", CLEANUP_RULE, fix))
    return findings


def changed_lines_and_deletions() -> tuple[dict[str, list[str]], list[str]]:
    """Return removed lines by file and deleted paths, from the staged diff or, in CI, the pushed range."""
    refs = [os.environ["PRE_COMMIT_FROM_REF"], os.environ["PRE_COMMIT_TO_REF"]] if "PRE_COMMIT_FROM_REF" in os.environ else ["--cached"]
    deleted = git("diff", *refs, "--name-only", "--diff-filter=D").splitlines()
    removed: dict[str, list[str]] = {}
    current, in_header = "", False
    for line in git("diff", *refs, "-U0", "--no-color").splitlines():
        if line.startswith("diff --git "):
            current, in_header = "", True
        elif in_header and line.startswith("--- "):
            current = line[6:] if line.startswith("--- a/") else ""
        elif line.startswith("@@"):
            in_header = False
        elif not in_header and line.startswith("-") and current:
            removed.setdefault(current, []).append(line[1:])
    return removed, deleted


def reference_files() -> dict[str, list[str]]:
    """Return the lines of every tracked text file that describes current behavior; history and the allowlist are exempt."""
    files: dict[str, list[str]] = {}
    for path in git("ls-files").splitlines():
        if path == "CHANGELOG.md" or path.startswith("e2e/") or path == CLEANUP_ALLOWLIST or not Path(path).is_file():
            continue
        try:
            files[path] = Path(path).read_text().splitlines()
        except UnicodeDecodeError:
            continue
    return files


def search_view(kind: str, path: str, lines: list[str]) -> list[str] | None:
    """Return the text of each line to search for a removed name of ``kind``, or None to skip the file.

    Scripts, flags and dependencies: every line. Environment variables: Markdown and .env.example, where people read
    them. Code names: code and config files, plus Markdown code spans and fences, so "the main branch" in prose is not a
    reference. Python names skip quoted strings and attribute access (``parser.parse_args()``) in Python files and are
    not searched in Terraform, SQL, JSON or shell files. Terraform skips ``from =`` lines: moved and removed blocks name
    old addresses on purpose.
    """
    suffix, name = Path(path).suffix, Path(path).name
    if kind in ("script", "flag", "dependency"):
        return lines
    if kind == "env":
        return lines if suffix == ".md" or name == ".env.example" else None
    if suffix == ".md":
        view, fence = [], False
        for line in lines:
            if re.match(r"^\s*(```|~~~)", line):
                fence = not fence
                view.append("")
            else:
                view.append(line if fence else " ".join(re.findall(r"`([^`\n]*)`", line)))
        return view
    if suffix not in CODE_SUFFIXES and name not in CODE_NAMES:
        return None
    if kind == "python":
        if suffix not in PYTHON_REFERENCE_SUFFIXES:
            return None
        if suffix == ".py":
            return python_code(lines)
    if kind == "terraform" and suffix == ".tf":
        return ["" if re.match(r"^\s*from\s*=", line) else line for line in lines]
    return lines


def python_code(lines: list[str]) -> list[str]:
    """Return Python lines with docstrings, comments, strings and attribute access blanked, keeping line numbers."""
    view, quote = [], ""
    for line in lines:
        if quote:
            view.append("")
            quote = "" if quote in line else quote
            continue
        start = re.match(r"^\s*[rRbBuUfF]?(\"\"\"|\'\'\')", line)
        if start:
            view.append("")
            quote = "" if line.count(start.group(1)) >= 2 else start.group(1)
            continue
        code = re.sub(r"""(["'])(?:\\.|(?!\1).)*\1""", '""', line).split("#")[0]
        view.append(re.sub(r"\.\s*[A-Za-z_]\w*", "", code))
    return view


def python_reference(name: str) -> re.Pattern[str]:
    """Return the pattern for a removed Python name outside Python files: a call, import or dotted path, or the bare
    name when it looks like an identifier (``_`` or a capital), so a removed ``docs`` does not match the commit type."""
    word = re.escape(name)
    contexts = [rf"(?<![\w./-]){word}\((?![^)\s]*\)!?:\s)", rf"\bimport\b[^\n]*\b{word}\b", rf"[.:]{word}\b"]
    if re.search(r"_|[A-Z]", name):
        contexts.append(rf"(?<![\w./-]){word}(?![\w/-]|\.(?:md|py|txt|json|ya?ml|toml|sh|sql|tf)\b)")
    return re.compile("|".join(contexts))


def config_key(line: str) -> str | None:
    """Return the key a YAML, TOML, JSON or INI line defines, or None for comments and other lines."""
    if line.lstrip().startswith(("#", ";", "//")):
        return None
    match = CONFIG_KEY.match(line)
    return match.group(1) if match else None


def requirement_names(lines_by_file: dict[str, list[str]]) -> set[str]:
    """Return the lower-case package names in requirements files."""
    names = set()
    for path, lines in lines_by_file.items():
        if re.search(r"(^|/)requirements[^/]*\.(txt|in)$", path):
            names |= {match.group(1).lower() for line in lines if (match := REQUIREMENT.match(line))}
    return names


def removed_definitions(removed: dict[str, list[str]], deleted: list[str], files: dict[str, list[str]]) -> dict[str, dict[str, re.Pattern[str]]]:
    """Find the names the change removed, by kind, each with the pattern that marks a remaining reference."""

    def gone(found: set[str], still: set[str]) -> set[str]:
        return {name for name in found - still if len(name) >= 3 and not (name.startswith("__") and name.endswith("__"))}

    def joined(lines_by_file: dict[str, list[str]], suffixes: set[str]) -> str:
        return "\n".join("\n".join(lines) for path, lines in lines_by_file.items() if Path(path).suffix in suffixes)

    kinds: dict[str, dict[str, re.Pattern[str]]] = {kind: {} for kind in REMOVED_KINDS}
    for path in deleted:
        if Path(path).suffix in SCRIPT_SUFFIXES:
            kinds["script"][Path(path).name] = re.compile(re.escape(Path(path).name))
        if path.endswith(".sql") and re.search(r"(^|/)(models|snapshots)/", path):
            stem = re.escape(Path(path).stem)
            kinds["dbt"][Path(path).stem] = re.compile(rf"""ref\(\s*["']{stem}["']\s*\)|name:\s*{stem}\b|(?<![\w.]){stem}(?![\w.])""")
    code_now, code_removed = joined(files, {".py"}), joined(removed, {".py"})
    for flag in gone(set(FLAG.findall(code_removed)), set(FLAG.findall(code_now))):
        kinds["flag"][flag] = re.compile(re.escape(flag) + r"(?![\w-])")
    env_now = set(ENV_READ.findall(code_now)) | {a or b for a, b in ENV_USE.findall(joined(files, {".sh", ".yml", ".yaml", ".json", ".tf", ".tpl", ".j2", ""}))}
    for name in gone(set(ENV_READ.findall(code_removed)), env_now):
        kinds["env"][name] = re.compile(rf"\b{name}\b")
    python_now = set(PY_DEFINITION.findall(code_now)) | set(PY_ASSIGNED.findall(code_now))
    for name in gone(set(PY_TOP_LEVEL.findall(code_removed)), python_now):
        kinds["python"][name] = re.compile(rf"(?<![\w./-]){re.escape(name)}(?![\w/-]|\.(?:md|py|txt|json|ya?ml|toml|sh|sql|tf)\b)")
    for name in gone(set(TASK_ID.findall(code_removed)), set(TASK_ID.findall(code_now))):
        kinds["airflow"][name] = re.compile(rf"""["']{re.escape(name)}["']|^{re.escape(name)}$""")
    config_now = {key for line in joined(files, CONFIG_SUFFIXES).splitlines() if (key := config_key(line))}
    # Keys of a deleted or untracked config file are often data fields the code still writes; only edited files count.
    # Single-word keys (run, env, path) are schema vocabulary, not project settings.
    edited = {path: lines for path, lines in removed.items() if path not in deleted}
    config_removed = {key for line in joined(edited, CONFIG_SUFFIXES).splitlines() if (key := config_key(line)) and re.search(r"[_-]|[a-z][A-Z]", key)}
    for name in gone(config_removed, config_now):
        kinds["config"][name] = re.compile(rf"(?<![\w-]){re.escape(name)}(?![\w-])")
    for kind, first, second in set(TF_BLOCK.findall(joined(removed, {".tf"}))) - set(TF_BLOCK.findall(joined(files, {".tf"}))):
        one, two = re.escape(first), re.escape(second)
        reference = {
            "variable": rf"\bvar\.{one}\b",
            "module": rf"\bmodule\.{one}\b",
            "output": rf"\b(?:output(?:\s+-[\w-]+)*\s+|outputs?\.){one}\b",
            "resource": rf"(?<![\w.]){one}\.{two}\b",
            "data": rf"\bdata\.{one}\.{two}\b",
        }[kind]
        kinds["terraform"][first if kind in ("variable", "module", "output") else f"{first}.{second}"] = re.compile(reference)
    for package in requirement_names(removed) - requirement_names(files):
        module = re.escape(MODULE_NAMES.get(package, package.replace("-", "_")))
        pip = rf"\bpip install\b.*(?<![\w-]){re.escape(package)}(?![\w-])"
        kinds["dependency"][package] = re.compile(rf"^\s*(?:import|from)\s+{module}\b|{pip}", re.IGNORECASE)
    return kinds


def allowed_paths(kind: str) -> tuple[list[str], list[Finding]]:
    """Return the path globs .cleanup_allowlist exempts for ``kind`` and any malformed-entry findings."""
    entries, findings = read_allowlist(CLEANUP_ALLOWLIST, REMOVED_KINDS + UNUSED_KINDS, CLEANUP_RULE, "why it stays")
    return [glob for entry_kind, glob in entries if entry_kind == kind], findings


def unused_code() -> list[Finding]:
    """No Python function, class, method or variable that nothing uses (vulture, all tracked Python at once)."""
    globs, findings = allowed_paths("unused-code")
    paths = [p for p in git("ls-files", "*.py").splitlines() if not p.startswith("e2e/") and Path(p).is_file()]
    if not paths:
        return findings
    result = run_command(str(Path(".venv/bin/vulture").resolve()), [*paths, "--min-confidence", str(VULTURE_CONFIDENCE)], timeout=300)
    if result.returncode not in (0, 3):  # 3: unused code found
        raise SystemExit(f"vulture failed ({result.returncode}): {result.stderr[-2000:]}")
    for line in result.stdout.splitlines():
        match = re.match(r"^(.+?):(\d+): (.+?) \((\d+)% confidence\)", line)
        if match and not any(fnmatch.fnmatch(match.group(1), glob) for glob in globs):
            fix = f"delete it, or add 'unused-code {match.group(1)} -- <who uses it>' to {CLEANUP_ALLOWLIST}"
            findings.append(Finding(f"{match.group(1)}:{match.group(2)}", match.group(3), CLEANUP_RULE, fix))
    return findings


def unused_dependencies() -> list[Finding]:
    """Every declared dependency is imported and every imported package is declared (deptry).

    A requirement used as a command or imported dynamically carries ``# tool: <use>`` or ``# dynamic: <where>``.
    """
    globs, findings = allowed_paths("unused-dependencies")
    requirements = [p for p in git("ls-files").splitlines() if re.search(r"(^|/)requirements[^/]*\.txt$", p) and Path(p).is_file()]
    if not requirements:
        return findings
    exempt = {
        m.group(1).lower() for p in requirements for line in Path(p).read_text().splitlines() if TOOL_COMMENT.search(line) and (m := REQUIREMENT.match(line))
    }
    first_party = sorted(
        {Path(p).stem for p in git("ls-files", "*.py").splitlines()} | {Path(p).parent.name for p in git("ls-files", "*/__init__.py").splitlines()}
    )
    with tempfile.TemporaryDirectory(prefix="deptry_") as scratch:
        report_path = Path(scratch) / "deptry.json"
        args = [".", "--requirements-files", ",".join(requirements), "--json-output", str(report_path), "--no-ansi"]
        args += [flag for folder in DEPTRY_EXCLUDED for flag in ("--extend-exclude", folder)]
        args += [flag for name in first_party for flag in ("--known-first-party", name)]
        args += ["--per-rule-ignores", "DEP002=" + "|".join(sorted(exempt))] if exempt else []
        result = run_command(str(Path(".venv/bin/deptry").resolve()), args, timeout=300)
        if not report_path.exists():
            raise SystemExit(f"deptry failed ({result.returncode}): {result.stderr[-2000:]}")
        issues = json.loads(report_path.read_text())
    for issue in issues:
        code, module, location = issue["error"]["code"], issue["module"], issue["location"]
        if code not in DEPTRY_CODES or any(fnmatch.fnmatch(location["file"], glob) for glob in globs):
            continue
        if code == "DEP002":
            fix = "remove it, or mark the line '# tool: <use>' or '# dynamic: <where>' when it is not imported directly"
            findings.append(Finding(location["file"], f"{module} is declared but nothing imports it", CLEANUP_RULE, fix))
        else:
            fix = f"pin it in {requirements[0]}, or replace the import"
            findings.append(
                Finding(f"{location['file']}:{location['line']}", f"{module} is imported but not declared", f"{POLICY}: declared dependencies", fix)
            )
    return findings


def orphan_files() -> list[Finding]:
    """Every tracked file is named by another tracked file (path, file name or Python import), or is well known."""
    globs, findings = allowed_paths("orphan")
    tracked = git("ls-files").splitlines()
    texts = {}
    for path in tracked:
        try:
            texts[path] = Path(path).read_text()
        except (UnicodeDecodeError, FileNotFoundError, IsADirectoryError):
            continue
    for path in tracked:
        name = Path(path).name
        if path.startswith(("e2e/", ".github/")) or name.startswith(".") or name in WELL_KNOWN_FILES or any(fnmatch.fnmatch(path, glob) for glob in globs):
            continue
        if re.fullmatch(r"test_.*\.py|.*_test\.py|.*\.tf|.*\.tfvars", name):  # pytest discovers tests; Terraform loads a whole folder
            continue
        patterns = [re.escape(path), rf"(?<![\w.-]){re.escape(name)}(?![\w-])"]
        parts = Path(path).parent.parts
        if len(parts) >= 2:  # A file a script loads by folder, such as policies/*.json, is used when the folder is named
            patterns.append(rf"(?<![\w-]){re.escape('/'.join(parts[-2:]))}(?![\w-])")
        if path.endswith(".py"):
            patterns.append(rf"\b(?:import|from|-m)\s+(?:[\w.]+\.)?{re.escape(Path(path).stem)}\b")
        pattern = re.compile("|".join(patterns))
        if not any(other != path and pattern.search(text) for other, text in texts.items()):
            fix = f"delete it, link it from the file that uses it, or add 'orphan {path} -- <why it stays>' to {CLEANUP_ALLOWLIST}"
            findings.append(Finding(path, "no other tracked file references it", CLEANUP_RULE, fix))
    return findings


def terraform_unused() -> list[Finding]:
    """No Terraform variable, local, data source or module output that nothing uses (tflint, every module)."""
    globs, findings = allowed_paths("terraform-unused")
    if not git("ls-files", "*.tf").strip():
        return findings
    args = ["--recursive", "--only", "terraform_unused_declarations", "--format", "json", "--force"]
    result = run_command(str(Path(".tools/bin/tflint").resolve()), args, timeout=300)
    report = json.loads(result.stdout or "{}")
    if result.returncode or report.get("errors"):
        raise SystemExit(f"tflint failed ({result.returncode}): {report.get('errors') or result.stderr[-2000:]}")
    root = Path.cwd().resolve()
    for issue in report.get("issues", []):
        where = issue["range"]
        path = (root / where["filename"]).resolve().relative_to(root).as_posix()
        if not any(fnmatch.fnmatch(path, glob) for glob in globs):
            fix = f"delete the declaration, or add 'terraform-unused {path} -- <who uses it>' to {CLEANUP_ALLOWLIST}"
            findings.append(Finding(f"{path}:{where['start']['line']}", issue["message"], CLEANUP_RULE, fix))
    return findings


def removed_names() -> list[Finding]:
    """A name the change removed is no longer referenced in any file that describes current behavior.

    Kinds: scripts, command-line flags, environment variables, Python functions and classes, config keys, Terraform
    declarations, dbt models, Airflow task IDs and dependencies. Code names match code and config files and only the
    code spans and fences of Markdown, so a removed ``main`` does not flag "the main branch" in prose.
    """
    allowed, findings = read_allowlist(CLEANUP_ALLOWLIST, REMOVED_KINDS + UNUSED_KINDS, CLEANUP_RULE, "why the reference stays")
    removed, deleted = changed_lines_and_deletions()
    files = reference_files()
    for kind, names in removed_definitions(removed, deleted, files).items():
        for name, pattern in sorted(names.items()):
            for path, lines in files.items():
                view = search_view(kind, path, lines)
                if view is None or any(entry_kind == kind and fnmatch.fnmatch(path, glob) for entry_kind, glob in allowed):
                    continue
                matcher = python_reference(name) if kind == "python" and not path.endswith(".py") else pattern
                for number, line in enumerate(view, 1):
                    if matcher.search(line):
                        fix = f"remove or update the reference, or add '{kind} {path} -- <why the reference stays>' to {CLEANUP_ALLOWLIST}"
                        findings.append(Finding(f"{path}:{number}", f"{name} was removed but is still referenced", CLEANUP_RULE, fix))
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


def privacy_files(everything: bool) -> list[str]:
    """Tracked and untracked non-ignored files; with ``everything``, ignored and hidden files too (not tool folders)."""
    if not everything:
        return sorted(set(git("ls-files", "--cached", "--others", "--exclude-standard").splitlines()))
    found = []
    for folder, subfolders, names in os.walk("."):
        subfolders[:] = sorted(name for name in subfolders if name not in SKIPPED_DIRS and not os.path.islink(os.path.join(folder, name)))
        found += [os.path.normpath(os.path.join(folder, name)) for name in names if name not in SKIPPED_DIRS]  # A worktree's .git is a file.
    return sorted(found)


def read_allowlist(name: str, kinds: list[str], rule: str, reason: str) -> tuple[list[tuple[str, str]], list[Finding]]:
    """Read ``type glob -- reason`` entries; an entry without a reason or with an unknown type is itself a finding."""
    entries: list[tuple[str, str]] = []
    findings: list[Finding] = []
    if not Path(name).exists():
        return entries, findings
    for number, line in enumerate(Path(name).read_text().splitlines(), 1):
        text = "" if line.lstrip().startswith("#") else line.strip()
        if not text:
            continue
        match = re.fullmatch(r"(\S+)\s+(\S+)(?:\s+--\s*(.*))?", text)
        location = f"{name}:{number}"
        if not match or match.group(1) not in kinds:
            findings.append(Finding(location, "allowlist entry with an unknown type", rule, f"use one of: {', '.join(kinds)}"))
        elif not (match.group(3) or "").strip():
            findings.append(Finding(location, "allowlist entry without a reason", rule, f"add ' -- <{reason}>'"))
        else:
            entries.append((match.group(1), match.group(2)))
    return entries, findings


def privacy_allowlist() -> tuple[list[tuple[str, str]], list[Finding]]:
    """Privacy allowlist entries by finding type and path glob."""
    return read_allowlist(ALLOWLIST, [*PRIVACY_PATTERNS, "env-value"], PRIVACY_RULE, "why this value is intentionally public")


def project_name_prefixes(lines: list[str]) -> set[str]:
    """The lower-case name prefixes of .env's PROJECT_NAME, dashes and underscores treated alike.

    A project whose AWS profile follows its naming standard names that profile after the project prefix, so the value
    is a public name, not an environment-specific secret. Longer values, such as a bucket named after the project,
    stay checked.
    """
    segments: list[str] = []
    for line in lines:
        key, _, value = line.partition("=")
        if key.strip().upper() == "PROJECT_NAME" and not line.lstrip().startswith("#"):
            segments = value.strip().strip("\"'").lower().replace("_", "-").split("-")
    return {"-".join(segments[:count]) for count in range(1, len(segments) + 1)}


def env_values() -> list[str]:
    """Values of identifying keys in the project's .env (profile, bucket, account, host), never printed."""
    if not Path(".env").is_file():
        return []
    lines = Path(".env").read_text().splitlines()
    public = project_name_prefixes(lines)
    values = []
    for line in lines:
        key, _, value = line.partition("=")
        value = value.strip().strip("\"'")
        named = value.lower().replace("_", "-")
        if ENV_VALUE_KEYS.search(key.strip().upper()) and len(value) >= 4 and not line.lstrip().startswith("#") and named not in public:
            values.append(value)
    return values


def line_privacy(line: str, values: list[str]) -> list[str]:
    """Return the privacy finding types on one line."""
    kinds = []
    for kind, (_, pattern) in PRIVACY_PATTERNS.items():
        for match in pattern.finditer(line):
            if kind == "email" and match.group(1).lower().endswith(RESERVED_DOMAINS):
                continue
            kinds.append(kind)
            break
    if any(value in line for value in values):
        kinds.append("env-value")
    return kinds


def privacy_scan(everything: bool) -> tuple[list[Finding], list[str]]:
    """Scan whole files, not the diff, for personal data and environment-specific values; never report the value."""
    allowed, findings = privacy_allowlist()
    values = env_values()
    unreviewed = []
    descriptions = {kind: description for kind, (description, _) in PRIVACY_PATTERNS.items()} | {"env-value": "value declared in .env"}
    for path in privacy_files(everything):
        if path == ".env" or os.path.islink(path) or not os.path.isfile(path):
            continue
        data = Path(path).read_bytes()
        try:
            if b"\0" in data[:8192]:
                raise UnicodeDecodeError("utf-8", data[:1], 0, 1, "binary")
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            unreviewed.append(f"{path}: unreviewed: cannot be read as text; inspect it before publishing")
            continue
        for number, line in enumerate(text.splitlines(), 1):
            for kind in line_privacy(line, values):
                if not any(kind == entry_kind and fnmatch.fnmatch(path, glob) for entry_kind, glob in allowed):
                    findings.append(Finding(f"{path}:{number}", descriptions[kind], PRIVACY_RULE, PRIVACY_FIX))
    return findings, unreviewed


def mask_prose(line: str) -> str:
    """Replace code spans, link targets and URLs with a neutral word, so only prose is checked.

    A neutral word, not blanks: blanking would turn "`a`, `b` and `c`" into a comma followed by spaces and "and".
    """
    line = re.sub(r"`[^`\n]*`", "code", line)
    line = re.sub(r"\]\([^)\s]*\)", "]", line)
    return re.sub(r"https?://\S+", "url", line)


def prose_lines(text: str) -> list[tuple[int, str]]:
    """Masked prose lines of a Markdown file, outside front matter and fenced code blocks."""
    lines: list[tuple[int, str]] = []
    fenced = front = False
    for number, line in enumerate(text.splitlines(), 1):
        if number == 1 and line.strip() == "---":
            front = True
            continue
        if front:
            front = line.strip() != "---"
            continue
        if line.lstrip().startswith(("```", "~~~")):
            fenced = not fenced
            continue
        if not fenced:
            lines.append((number, mask_prose(line)))
    return lines


def writing_check(articles: list[str]) -> list[Finding]:
    """Prose in Markdown follows the writing preferences; em dashes are checked in declared article files only.

    The declared data folders hold synthetic fixtures and are skipped; ignored files (data/, local evidence) never reach it.
    """
    allowed, findings = read_allowlist(WRITING_ALLOWLIST, list(WRITING_PATTERNS), WRITING_RULE, "why this text is kept as is")
    for path in sorted(set(git("ls-files", "--cached", "--others", "--exclude-standard").splitlines())):
        article = any(fnmatch.fnmatch(path, glob) for glob in articles)
        if not (path.endswith(".md") or article) or path.startswith(DATA_FOLDERS) or not os.path.isfile(path):
            continue
        for number, line in prose_lines(Path(path).read_text(errors="replace")):
            for kind, (description, pattern, fix) in WRITING_PATTERNS.items():
                if kind in ARTICLE_ONLY and not article:
                    continue
                if pattern.search(line) and not any(kind == entry_kind and fnmatch.fnmatch(path, glob) for entry_kind, glob in allowed):
                    findings.append(Finding(f"{path}:{number}", description, WRITING_RULE, fix))
    return findings


FRONT_RULE = "bundle standards/documents.md#3-metadata (sections 3.1, 3.2): document front matter"
# Well-known files and .github templates need no front matter; READMEs and E2E records are out of scope.
WELL_KNOWN = {"AGENTS.md", "CLAUDE.md", "CONTRIBUTING.md", "CHANGELOG.md", "SECURITY.md", "CODE_OF_CONDUCT.md", "SUPPORT.md", "LICENSE.md"}
DESCRIPTION_LIMIT = 120
ISO_DAY = re.compile(r"\d{4}-\d{2}-\d{2}")


def load_yaml(text: str) -> object:
    """Parse YAML with PyYAML's safe loader; PyYAML ships no type hints, so it is imported by name."""
    yaml = importlib.import_module("yaml")
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as error:
        raise ValueError(str(error).splitlines()[0]) from error


def exempt_from_front_matter(path: str) -> bool:
    """Return whether a Markdown file needs no front matter: a README, a well-known file or a .github template."""
    name = os.path.basename(path)
    return name in WELL_KNOWN or name.startswith("README") or path.startswith(".github/")


def first_h1(text: str) -> str | None:
    """The first level-one heading outside fenced code blocks."""
    fenced = False
    for line in text.splitlines():
        if line.lstrip().startswith(("```", "~~~")):
            fenced = not fenced
        elif not fenced and line.startswith("# "):
            return line[2:].strip()
    return None


def front_matter_problems(text: str) -> list[str]:
    """Problems with one document's front matter: valid YAML with title (equal to the H1), description and last_updated."""
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return ["no front matter: start the file with --- title, description and last_updated ---"]
    if "---" not in (line.strip() for line in lines[1:]):
        return ["front matter is not closed with a --- line"]
    end = next(number for number, line in enumerate(lines[1:], 1) if line.strip() == "---")
    try:
        data = load_yaml("\n".join(lines[1:end]))
    except ValueError as error:
        return [f"front matter is not valid YAML ({error}); quote values that contain ': '"]
    if not isinstance(data, dict):
        return ["front matter is not valid YAML: expected key: value lines"]
    problems = []
    title, description, updated = data.get("title"), data.get("description"), data.get("last_updated")
    if not isinstance(title, str) or title != first_h1(text):
        problems.append("title does not match the H1")
    if not isinstance(description, str) or not description.strip():
        problems.append("description is missing")
    elif len(description) > DESCRIPTION_LIMIT:
        problems.append(f"description over {DESCRIPTION_LIMIT} characters ({len(description)})")
    if not (isinstance(updated, datetime.date) or (isinstance(updated, str) and ISO_DAY.fullmatch(updated))):
        problems.append("last_updated is not a YYYY-MM-DD date")
    return problems


def front_matter() -> list[Finding]:
    """Every in-scope Markdown document has valid front matter with title, description and last_updated."""
    findings = []
    for path in sorted(git("ls-files", "--cached", "--others", "--exclude-standard").splitlines()):
        if not path.endswith(".md") or path.startswith(DATA_FOLDERS) or exempt_from_front_matter(path) or not os.path.isfile(path):
            continue
        for problem in front_matter_problems(Path(path).read_text(errors="replace")):
            findings.append(
                Finding(
                    f"{path}:1",
                    problem,
                    FRONT_RULE,
                    "add or fix the front matter: title equal to the H1, description of at most 120 characters, last_updated YYYY-MM-DD",
                )
            )
    return findings


def warn(findings: list[Finding]) -> int:
    """Report findings without failing: the warn period before a check blocks."""
    for finding in findings:
        sys.stderr.write(f"WARN {finding.location}: {finding.problem}\n  rule: {finding.rule}\n  fix: {finding.fix}\n")
    return 0


def main() -> int:
    """Run the named check and report its findings."""
    checks = ["credential-files", "data-files", "suppressions", "subprocess-imports", "lint-settings", "env-example", "removed-names"]
    checks += ["unused-code", "unused-dependencies", "orphan-files", "terraform-unused"]
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("check", choices=[*checks, "commit-msg", "commit-range", "privacy-scan", "writing-check", "front-matter"])
    parser.add_argument("paths", nargs="*")
    parser.add_argument("--all", action="store_true", help="privacy-scan: include ignored and hidden files (before publishing)")
    parser.add_argument("--warn", action="store_true", help="report findings without failing: a check's warn period before it blocks")
    parser.add_argument("--articles", action="append", default=[], metavar="GLOB", help="writing-check: files that hold article text (em dashes)")
    args = parser.parse_args()
    if args.check == "writing-check":
        findings = writing_check(args.articles)
        return warn(findings) if args.warn else report(findings)
    if args.check == "privacy-scan":
        findings, unreviewed = privacy_scan(args.all)
        for line in unreviewed:
            sys.stderr.write(f"{line}\n")
        return warn(findings) if args.warn else report(findings)
    runners: dict[str, Callable[[], list[Finding]]] = {
        "credential-files": lambda: credential_files(args.paths),
        "data-files": lambda: data_files(args.paths),
        "suppressions": lambda: suppressions(args.paths),
        "subprocess-imports": lambda: subprocess_imports(args.paths),
        "lint-settings": lint_settings,
        "env-example": env_example,
        "removed-names": removed_names,
        "unused-code": unused_code,
        "unused-dependencies": unused_dependencies,
        "orphan-files": orphan_files,
        "terraform-unused": terraform_unused,
        "front-matter": front_matter,
        "commit-msg": lambda: commit_message(args.paths[0]),
        "commit-range": lambda: commit_range(args.paths[0], args.paths[1]),
    }
    findings = runners[args.check]()
    return warn(findings) if args.warn else report(findings)


if __name__ == "__main__":
    sys.exit(main())
