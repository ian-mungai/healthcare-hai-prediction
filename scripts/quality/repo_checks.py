"""Repository checks run by pre-commit and CI.

Run from the repository root: ``.venv/bin/python -m scripts.quality.repo_checks <check> [paths ...]``. Each finding
names the file and line, the rule, how to fix it and what to do when the rule seems wrong; any finding exits 1. A check
that cannot run raises and exits non-zero, so it fails closed. There are no bypasses. Markers the checks look for are
assembled from fragments, so this file does not flag itself. The rules are the check functions below.
"""

from __future__ import annotations

import argparse
import ast
import configparser
import datetime
import fnmatch
import functools
import importlib
import io
import json
import ntpath
import os
import posixpath
import re
import shlex
import sys
import tempfile
import tokenize
import tomllib
import unicodedata
import urllib.parse
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

from scripts.process import run_command
from scripts.quality.markdown_links import check_links

POLICY = "scripts/quality/repo_checks.py"
ASK = "if the rule seems wrong here, stop and ask the repository owner. There are no bypasses (no SKIP= or --no-verify)"

CREDENTIAL_NAMES = (
    "*.tfstate",
    "*.tfstate.backup",
    "*.tfstate.*.backup",
    "*.tfplan",
    "*.tfplan.json",
    "*.tfvars",
    "*.tfvars.json",
    "crash.log",
    "crash.*.log",
    "*.pem",
    "*.key",
    "*.p12",
    "*.pfx",
    "id_rsa",
    "id_ecdsa",
    "id_ed25519",
    "credentials",
)
DATA_EXTENSIONS = {".csv", ".tsv", ".parquet", ".xlsx", ".xls", ".jsonl", ".ndjson", ".avro", ".db", ".sqlite"}
# Folders declared for data files: synthetic test fixtures and the reviewed dbt seeds (which include the formula recipes of the feature
# phase), which hold file labels and checksums from the S3 manifests and rule definitions, never source data values.
DATA_FOLDERS = ("tests/fixtures/", "dbt/seeds/")
# The one bootstrap seed folder (owner, Nov 12 2026): public publisher captures that no host can fetch, stored through Git LFS for a run in an
# empty environment. Only the data-files rule admits data there; the Markdown checks still read its documents.
SEED_FOLDER = "config/acquisition/bootstrap_seed/"
DATA_FILES = ("config/lakehouse/identity_classification.csv",)
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
TEMPORARY_LIST = "config/quality/temporary_code.json"
TEMPORARY_RULE = "the bundle's git_and_ci rules: main never imports, calls or reads temporary code"
CI_SCRIPT = "scripts/run_ci.sh"
CI_GROUP_FILES = "scripts/ci/groups"

ENV_READ = re.compile(r"""(?:os\.environ(?:\.get)?\s*[\[(]\s*|os\.getenv\s*\(\s*)["']([A-Z][A-Z0-9_]*)["']""")
ENV_EXAMPLE_NAME = re.compile(r"^\s*#?\s*(?:export\s+)?([A-Z][A-Z0-9_]*)\s*=", re.MULTILINE)
ENV_LIST = "config/quality/env_names.json"
ENV_CATEGORIES = ("test", "container", "shell", "tool")
# Test code: a tests folder, test_*.py, *_cases.py, run_*e2e.py runners, and stand-in, fake, fixture, fault and plant modules. The word patterns match whole
# parts of a file or folder name (split at / _ . -), so understanding.py is production code. A module outside this rule that only tests use goes under
# "test_modules" in ENV_LIST.
TEST_CODE = re.compile(
    r"(?:^|/)(?:tests?/|test_[^/]*$|conftest\.py$)"
    r"|_cases\.py$"
    r"|(?:^|/)run_(?:[^/]*_)?e2e\.py$"
    r"|(?:^|[/_.-])(?:stand_?ins?|fakes?|planted)(?:$|[/_.-])"
    r"|(?:^|[/_])fixtures?\.py$|_fixtures?(?:$|[/_.-])"
    r"|(?:^|[/_])plants?\.py$|_faults\.py$"
)
# The job configs of the launcher (the paths of orchestration/ecs_preflight.py): the only JSON files whose lists declare the variables of a container.
JOB_CONFIGS = ("config/orchestration/ecs_broker_jobs.json", "config/orchestration/direct_jobs.json", "config/orchestration/host_jobs.json")
# Where Terraform, the job templates and the launcher configs set the variables of a container.
CONTAINER_SETTERS = re.compile(r"(?:infra/|config/orchestration/|data_contracts/orchestration/|services/|docker-compose[^/]*$)")
SCRIPT_SUFFIXES = {".py", ".sh", ".js", ".mjs", ".ts"}
FLAG = re.compile(r"""add_argument\(\s*["'](--[\w-]+)["']""")
CLEANUP_ALLOWLIST = ".cleanup_allowlist"
CLEANUP_RULE = f"{POLICY}: removed and unused code"
# The collectors' code-version records list the code hashes of stored captures; the file path of each fingerprint is history, like the changelog.
CODE_VERSION_RECORDS = re.compile(r"config/acquisition/[^/]*_code_versions\.json")
REMOVED_KINDS = ["script", "flag", "env", "python", "config", "terraform", "dbt", "dependency"]
UNUSED_KINDS = ["unused-code", "unused-dependencies", "orphan", "terraform-unused", "docs-path"]
CODE_SUFFIXES = {".py", ".sh", ".js", ".mjs", ".ts", ".sql", ".tf", ".yaml", ".yml", ".toml", ".json", ".ini", ".cfg", ".j2", ".jinja"}
CODE_NAMES = {"Dockerfile", "Makefile", ".env.example"}
CONFIG_SUFFIXES = {".yaml", ".yml", ".toml", ".json", ".ini", ".cfg"}
TERRAFORM_SUFFIXES = {".tf", ".hcl", ".tfvars"}
PY_DEFINITION = re.compile(r"^[ \t]*(?:async[ \t]+def|def|class)[ \t]+([A-Za-z_]\w*)", re.MULTILINE)
PY_TOP_LEVEL = re.compile(r"^(?:async[ \t]+def|def|class)[ \t]+([A-Za-z_]\w*)", re.MULTILINE)
PY_ASSIGNED = re.compile(r"^[ \t]*([A-Za-z_]\w*)[ \t]*(?::[^=\n]*)?=(?!=)", re.MULTILINE)
# Environment variables a shell script, workflow or rendered template still uses: $NAME, ${NAME}, vars.NAME, secrets.NAME, env.NAME.
ENV_USE = re.compile(r"\$\{?([A-Z][A-Z0-9_]*)\b|\b(?:vars|secrets|env)\.([A-Z][A-Z0-9_]*)\b")
CONFIG_KEY = re.compile(r"""^\s*(?:-\s+)?["']?([A-Za-z_][\w-]*)["']?\s*[:=](?!:)""")
TF_NESTED_BLOCK = re.compile(r"^\s*([A-Za-z_][\w-]*)\s*\{")
SCHEMA_ID = re.compile(r'"\$id"\s*:\s*"([^"]+)"')
TASK_IDS_RULE = f"{POLICY}: a task ID beside a shipped DAG names a task the DAG has"
TASK_KEY = re.compile(r'"task_id"\s*:\s*"')
COMMAND_STRING = re.compile(r'"([^"]*)"|\'([^\']*)\'')
COMMAND_MODULES_RULE = f"{POLICY}: a python -m command names a module of this repository that exists"
TF_BLOCK = re.compile(r'^\s*(variable|output|module|resource|data)\s+"([\w-]+)"(?:\s+"([\w-]+)")?', re.MULTILINE)
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
RUNTIME_MAP = "config/quality/dependency_runtimes.json"  # Each runtime's dependency groups, the code it runs and what it provides.
PYPROJECT_REQUIREMENT = re.compile(
    r"""^\s*"([A-Za-z0-9][A-Za-z0-9._-]*)\s*(?:\[[^\]]*\])?\s*[=<>!~;@]"""
)  # A quoted requirement line of a pyproject.toml group.
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
    "uv.lock",
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

# Internal plan references: they point at plan and review records that live outside the repository. A finding names the
# form and never echoes the matched text, so the check log holds no message text. Codes need a keyword or brackets, so a
# bare "B1" (hex fragment, CSS class) passes and "S3" is never a code (the storage service). The registry's public source
# families S13 to S16 are not codes either; a bracketed list counts when any of its codes is not public. Hash and snapshot forms
# need their provenance words on the same line, so a downloaded file's hash and a data snapshot ID pass.
PLAN_CODE = r"(?!S3\b)[A-Z]\d{1,2}"
PUBLIC_CODE = r"S(?:3|13|14|15|16)\b"
BRACKET_LIST = rf"[(\[](?=[^)\]]*\b(?!{PUBLIC_CODE})[A-Z]\d{{1,2}}\b)[A-Z]\d{{1,2}}(?:(?:, | to | and )[A-Z]\d{{1,2}})*[)\]]"
ISSUE_ID = r"(?!SHA|MD|RC)[A-Z]{1,4}\d+-\d+"
NUMBER_WORD = r"(?:one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)"
REVIEW_LINE = r"(?=.*\b(?:re-?check|review(?:s|ed|er|ers|ing)?)\b)"
PATCH_HASH = r"(?:hash|sha-?256|checksum)\W+[0-9a-f]{12,64}\b"
PLAN_REFERENCES = (
    (
        "unit or lane code",
        re.compile(rf"\b(?:[Ll]ane [A-Z]\b|(?i:units?|lanes?|failure modes?|tasks?|slices?|steps?) {PLAN_CODE}\b)"),
    ),
    ("bracketed unit code", re.compile(BRACKET_LIST)),
    ("issue ID", re.compile(rf"\b(?i:refs?|issues?|tickets?|closes|fixes|resolves)\b[ :]+{ISSUE_ID}\b|[(\[]{ISSUE_ID}[)\]]")),
    (
        "review-round name",
        re.compile(
            r"\b(?:re-?check\s+#?\d+|fix\s+round\s+#?\d+|review\s+(?:round\s+\d+|#\d+)|round\s+\d+\s+of\s+(?:the|its|their)\s+review)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "review-round name",
        re.compile(
            rf"^{REVIEW_LINE}.*\b(?:{NUMBER_WORD}\s+(?:review\s+|fix\s+)?rounds?\b(?!\s+(?:trips?|robin|numbers?|values?|figures?|digits?|decimals?|places?))"
            rf"|rounds?\s+{NUMBER_WORD}\b(?!\s+(?:decimals?|digits?|places?|numbers?|values?)))",
            re.IGNORECASE,
        ),
    ),
    (
        "accepted-patch hash",
        re.compile(rf"^(?=.*\b(?:patch|diff)\b)(?=.*\b(?:accepted|reviewed|staged)\b)(?=.*\b{PATCH_HASH}).*$", re.IGNORECASE),
    ),
    (
        "worktree snapshot ID",
        re.compile(r"^(?=.*\bworktrees?\b).*\bsnapshot\s+(?:id\s+)?[0-9a-f]{7,40}\b", re.IGNORECASE),
    ),
    ("review status note", re.compile(r"\bnot\s+independently\s+reviewed\b", re.IGNORECASE)),
)

PRIVACY_RULE = f"{POLICY}: privacy scan"
PRIVACY_FIX = (
    "replace the value with a placeholder (~, $TMPDIR, <name>, example.invalid, 111111111111 for a synthetic AWS account) or read it from configuration."
    " If it is meant to be public, add a reasoned entry to .privacy_allowlist"
)
ALLOWLIST = ".privacy_allowlist"
# A home path is /home/<name>, /Users/<name> (the macOS Shared folder excepted), C:\Users\<name> or ~<name>/. A name starts with any character that is not a
# delimiter, so a name in any script, an emoji or a no-break space is a name. ASCII white space, path separators, quotes, brackets and the placeholder openers
# < $ { * % are delimiters, so /home/<user>, /home/$USER, /home/{user} and /home/* are placeholders.
HOME_DELIMITERS = " \t\r\n\f\v/\\<>$(){}[]\"'`*%,;:|&?=!#@~"
HOME_NAME_CHAR = "[^" + re.escape(HOME_DELIMITERS) + "]"
HOME_QUOTES = "\"'`"
HOME_SPACES = " \t\r\n\f\v"
SHELL_BREAKS = HOME_SPACES + "()|&;"  # Outside quotes these end a shell word.
SLASH_SEPARATOR = re.compile("/")
ANY_SEPARATOR = re.compile(r"[/\\]")
# The roots of a home. The macOS and Windows file systems ignore case, so their roots match in any case. Another case of the macOS root must start a path (or be
# the path of a local file address), so a route such as /v1/users/login is no home. The Linux root is exact.
HOME_ROOT = re.compile(r"(?P<linux>/home/)|(?P<mac>(?:/Users|(?<![\w/.})\]-])/(?i:users))/)|(?P<windows>(?<!\w)[A-Za-z]:(?:/|\\+)(?i:Users)(?:/|\\+))")
FILE_URL_ROOT = re.compile(r"(?P<mac>/(?i:users)/)")
FILE_URL = re.compile(r"(?<![\w+.-])file:\S*", re.IGNORECASE)
HOME_HINT = re.compile(r"home|users|~|file:", re.IGNORECASE)
TILDE_NAME = re.compile(r"(?<![\w~/])~[^\W\d]" + HOME_NAME_CHAR + r"*/")  # A named tilde. A bare ~/ and a tilde inside an address name no user.
FOLDER_KEPT = 16  # The longest folder whose own text still matters to the home path rules.
ENCODED_FOLDER = re.compile(r"[^/\\\s\"'`<>]+")
ENCODED_PART = re.compile(r"%[0-9A-Fa-f]{2}|\\(?:x[0-9A-Fa-f]{2}|u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8}|[0-3][0-7]{2})")
PLACEHOLDER_PART = re.compile(r"%[A-Za-z_]\w*%|%\(\w+\)s")
ESCAPE = re.compile(r"\\(?:x([0-9A-Fa-f]{2})|u([0-9A-Fa-f]{4})|U([0-9A-Fa-f]{8})|([0-3][0-7]{2})|([/\"'\\]))")
PRIVACY_PATTERNS = {
    "home-path": ("home-directory path with a user name", HOME_ROOT),
    "temp-path": ("machine temporary path", re.compile(r"/var/" + r"folders/[A-Za-z0-9_+-]+/[A-Za-z0-9_+-]+")),
    "email": ("email address", re.compile(r"\b[A-Za-z0-9._%+-]+@((?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,})\b")),
    "phone": (
        "phone number",
        re.compile(
            r"(?<![\w.])(?:\+\d{1,3}[ .-]?)?(?:\(\d{3}\)[ .-]?|\d{3}[ .-])\d{3}[ .-]\d{4}(?![\w.])"
            r"|(?<![\w.])\+\d{1,3}[ -]\d{2,4}[ -]\d{3}[ -]\d{3,4}(?![\w.])"
        ),
    ),
    "aws-account": ("AWS account ID", re.compile(r"arn:aws[a-z-]*:[a-z0-9-]*:[a-z0-9-]*:(\d{12}):|(?i:account[_ -]?id)\W{1,4}(\d{12})\b")),
}
RESERVED_TOP_LEVEL_DOMAINS = (".invalid", ".test", ".localhost", ".example")  # Any name below them is reserved.
RESERVED_DOMAINS = ("example.com", "example.org", "example.net")  # Reserved by equality or as a dot-delimited subdomain, never as a longer name.
# The one synthetic account ID that code and tests may use. Any other 12-digit ID in an ARN or after an account id label is flagged, and so is
# any other 12-digit number on a line that holds an account match (a conditional that ends in a real account).
SYNTHETIC_ACCOUNT_ID = "111111111111"
TWELVE_DIGITS = re.compile(r"(?<![\w.])\d{12}(?!\w)")
UUID = re.compile(r"\b[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\b", re.IGNORECASE)
ENV_VALUE_KEYS = re.compile(r"PROFILE|BUCKET|ACCOUNT|ARN|ENDPOINT|HOST|EMAIL|USER")
WRITING_RULE = (
    f"{POLICY}: writing check. It checks the Oxford comma, date format, time-bound words, dashes, semicolons, slashes, internal references, "
    "history notes, the phrase no longer and hard-wrapped lines. In declared article files it also checks em dashes"
)
WRITING_ALLOWLIST = ".writing_allowlist"
MONTH_ANY = "(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)"
# Provenance and past-state forms only. A requirement such as "must be confirmed by the user" or "replaces the old file" is a current rule.
HISTORY_FORMS = (
    r"(?i)\(\s*(?:explicit )?user decisions?\b"
    r"|\buser decisions?,? (?:on |of |in |from )?(?:MONTH[a-z]* \d|v\d|\[?#\d)"
    r"|\b(?:was|were|been) \w+ by (?:explicit )?user decision\b"
    r"|\b(?:was|were|been) confirmed by the user\b|(?:^|[.!?]\s+)confirmed by the user\b"
    r"|\(v\d+\.\d+\.\d+[;,)]|\b(?:until|since|as of) v\d+\.\d+\.\d+\b"
    r"|\b(?:added|removed|introduced|changed|deprecated|fixed) in v\d+\.\d+\.\d+\b"
    r"|\bas of (?:MONTH[a-z]*\.? (?:\d{1,2},? )?\d{4}|\d{4}-\d{2}-\d{2})\b"
    r"|\b(?:was|were) (?:replaced|superseded)\b|\b(?:it|this|which|that) (?:replaced|superseded) (?:the|an?)\b"
    r"|\b(?:replaced|superseded) (?:the |an |a )?(?:earlier|old|older|previous|former)\b"
    r"|\b(?:in|from) (?:earlier|previous|former|older) (?:versions?|releases?)\b(?=.*\b(?:was|were|used to|did|had)\b)"
    r"|\bsince MONTH[a-z]* \d{1,2},? \d{4}\b"
    r"|\[#\d+\]"
    r"|\bwhen this (?:section|file|document|standard|rule|page|guide|text) was (?:written|added|created|drafted)\b"
    r"|\bon the mistaken (?:reading|assumption|belief|premise|view)\b|\b(?:was|were) (?:capped|limited) at\b"
    r"|(?<!\bis )(?<!\bare )(?<!\bwas )(?<!\bwere )(?<!\bbe )(?<!\bbeen )\bused to "
    r"(?:be|have|accept|allow|require|need|include|use|call|create|support|read|write|run|return|default|work|reject)\b"
    r"|\bpreviously (?:was|were|used|did|had|ran|allowed|required|accepted|defaulted)\b|\b(?:was|were) previously\b|(?:^|[.!?]\s+)previously,"
    r"|(?<!\bis )(?<!\bare )(?<!\bbe )\bchanged from\b"
)
# Checked in prose only: code spans, URLs and link targets are masked, and front matter, fenced blocks and indented code blocks are skipped.
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
    "dash": ("em or en dash in prose", re.compile("[\u2013\u2014]"), "end the sentence or use a comma"),
    "semicolon": ("semicolon in prose", re.compile(r";(?!\w)"), "split the text into two sentences"),
    "slash": ("slash between words in prose", re.compile(r"(?<![\w./~-])[a-z]+/[a-z]+(?![\w/.-])"), 'write "a, b or both" instead of "a/b" or "and/or"'),
    "history": (
        "history note in a current document",
        re.compile(HISTORY_FORMS.replace("MONTH", MONTH_ANY)),
        "state the current rule only. Git history, the CHANGELOG and issues hold when and why it changed",
    ),
    "no-longer": (
        "possible history note: 'no longer' describes a change",
        re.compile(r"(?i)\bno longer\b"),
        "if the sentence records a past state, state the current rule only. A current rule such as 'remove the files it no longer reads' stays",
    ),
}
# A line that follows a prose line and starts no structure of its own continues that line.
WRAP = ("hard-wrapped prose line", "join the line to the one before it. This repository writes each paragraph and list item on one line")
WRAP_START = re.compile(r"\s*(?:[-*+] |\d+[.)] |#|\||>|<|\[[^\]]*\]:|$)")
LIST_ITEM = re.compile(r"\s*(?:[-*+] |\d+[.)] )")
SENTENCE_END = re.compile(r"[.!?:][\"')\]*_]*$")
SETEXT_UNDERLINE = re.compile(r"\s*(?:=+|-+)\s*$")
HISTORY_EXEMPT = {"CHANGELOG.md"}
ARTICLE_ONLY = {"em-dash"}
WARN_FIRST = {"dash", "semicolon", "slash", "internal-reference", "history", "no-longer", "wrap"}
# A person reviews these kinds: a current rule can use the phrase, so no option blocks them.
REVIEWED_BY_HAND = {"no-longer"}


MONTH_NAME = (
    r"(?-i:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\b"
)
INTERNAL_REFERENCE_PATTERNS = {
    "owner-record": re.compile(
        r"\b(?:(?:owner|user)[ -]+(?:decision|approval|approved)|approval|approved)\b[^\n.!?]{0,32}?"
        r"(?:\b\d{4}-\d{2}-\d{2}\b|\b" + MONTH_NAME + r"[ \t]+\d{1,2}[ \t]+\d{4}\b)|"
        r"\b(?:owner|user)[ -]+(?:decision|approval|approved)[ \t]+\d+\b(?!\.\d)|"
        r"\b(?:owner|user)[ -]+(?:decision|approval|approved)\b[ \t,:]+(?-i:[A-Z]\d*)-\d+\b|"
        r"\b(?:owner|user)[ -]+(?:decision|approval|approved)\b[^\n.!?]{0,32}?"
        r"(?:\b\d{1,2}[ \t]+" + MONTH_NAME + r"|"
        r"\b" + MONTH_NAME + r"[ \t]+\d{1,2}\b)|"
        r"\b(?:owner|user)[ -]+(?:decision|approval|approved)\b[^\n.!?]{0,32}?(?<![\w.])(?:D\d+|#\d+)\b(?!\.\d)|"
        r"\b(?:approval|approved)[ \t]+(?:D\d+|#\d+)\b(?!\.\d)|"
        r"\bD[1-9]\d*(?=[ \t]+(?:section|item)\b|[ \t]*§)|\b(?:per|under|from|follows)[ \t]+D[1-9]\d*\b",
        re.IGNORECASE,
    ),
    "amendment": re.compile(
        r"(?i:\bamendment[ \t]+(?:no\.?[ \t]+|#)?A?\d+\b)|"
        r"(?<![\w+/=-])A(?:[235-9]|[1-9]\d+)(?![\w-])|"
        r"(?<![\w+/=-])A[14](?=[ \t]+(?i:amendment)\b)"
    ),
    "work-package": re.compile(r"(?<![\w+/=])(?:P[1-9]\d*[a-z]|(?i:WV\d+-\d+))(?!\w|\.\w)"),
    "wave": re.compile(r"\bwave[ _-]*\d+\b", re.IGNORECASE),
    "review-round": re.compile(
        r"\b(?:review[ _-]+(?:round[ _-]*(?:r[ _-]*)?|r[ _-]*)\d+\b(?![.-]\d)|R\d+[ _-]+review|round[ _-]+\d+[ _-]+review)\b",
        re.IGNORECASE,
    ),
    "issue-provenance": re.compile(r"\bissue[ \t]+#?[ \t]*\d+\b|\b(?:per|see|from|via|follows|under)[ \t]+#\d+\b|\(\s*#\d+\s*\)", re.IGNORECASE),
    "plans-path": re.compile(r"(?<![\w-])plans/"),
}
INTERNAL_REFERENCE_FIX = (
    "state the current behavior and keep provenance in local records. For a required public example, add a reasoned "
    "internal-reference entry to .writing_allowlist. Ask the owner if the rule seems wrong"
)
JSON_STRING = re.compile(r'(?P<value>"(?:\\.|[^"\\])*")(?P<key>\s*:)?')
QUOTED_CODE = r"\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|`[^`]*`"
HASH_COMMENT_SUFFIXES = {".sh", ".bash", ".zsh", ".tf", ".j2", ".jinja"}
C_COMMENT_SUFFIXES = {".js", ".mjs", ".cjs", ".jsx", ".ts", ".tsx", ".go", ".c", ".h", ".cpp", ".hpp", ".java", ".css", ".tf", ".sql"}


@dataclass
class Finding:
    """One problem: where it is, what it is, the rule it breaks and how to fix it."""

    location: str
    problem: str
    rule: str
    fix: str


@dataclass
class Runtime:
    """One place code runs: its dependency groups, the tracked Python files it runs and the modules it gets outside pip."""

    name: str
    groups: list[str]
    files: list[str]
    provided: dict[str, str]


BINARY_DIFF = re.compile(r"Binary files (?:/dev/null|a/(?P<old>.+?)) and (?:/dev/null|b/(?P<new>.+)) differ")
BINARY_NOTICE = ": skipped: binary data (NUL bytes), not checked for removed names"
SKIPPED: dict[str, str] = {}  # Changed file -> the notice for it, for each file the removed-names check skipped as binary in this run.


def skip_binary(path: str) -> None:
    """Record that the removed-names check skips ``path`` as binary data, once."""
    SKIPPED.setdefault(path, f"{path}{BINARY_NOTICE}")


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
            fix = "remove it from the commit (git rm --cached) and ignore it. Keep secrets in the secret store (Parameter Store or Secrets Manager)"
            findings.append(Finding(path, "credential, state or saved-plan file staged", f"{POLICY}: secrets and credential files", fix))
    return findings


def data_files(paths: list[str]) -> list[Finding]:
    """Block data files outside the declared folders and files and any file over 5 MB unless Git LFS stores it."""
    findings = []
    for path in paths:
        if Path(path).suffix.lower() in DATA_EXTENSIONS and not (path.startswith((*DATA_FOLDERS, SEED_FOLDER)) or path in DATA_FILES):
            fix = f"keep data in S3. Put synthetic samples in {', '.join(DATA_FOLDERS)}. Never commit real personal data"
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
                fix = f"fix the code so the check passes. The only approved exception is S603 in {LAUNCHER}"
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
    findings.extend(mypy_profiles())
    findings.extend(mypy_commands())
    ci = "\n".join(path.read_text() for path in (Path(CI_SCRIPT), *sorted(Path(CI_GROUP_FILES).glob("*.sh"))))
    if not any(re.search(r"\b(?:mypy|scripts\.quality\.typecheck)\b", line) and all(flag in line for flag in MYPY_FLAGS) for line in ci.splitlines()):
        findings.append(
            Finding(
                f"{CI_SCRIPT} or {CI_GROUP_FILES}/python_lint.sh",
                f"MyPy no longer runs with {' and '.join(MYPY_FLAGS)}",
                f"{POLICY}: lint settings",
                "restore both flags",
            )
        )
    return findings


def mypy_profiles() -> list[Finding]:
    """Inspect all committed or nonignored TOML and INI MyPy profiles, including module overrides."""
    findings = []
    for path in git("ls-files", "--cached", "--others", "--exclude-standard", "*.toml", "*.ini", "*.cfg").splitlines():
        content = Path(path).read_text()
        if not re.search(r"\[(?:tool\.mypy\b|mypy(?:\]|-))", content):
            continue
        if path.endswith(".toml"):
            settings = tomllib.loads(content).get("tool", {}).get("mypy", {})
            profiles = [("tool.mypy", settings), *(("tool.mypy.overrides", item) for item in settings.get("overrides", []))]
        else:
            config = configparser.ConfigParser(interpolation=None)
            config.read_string(content)
            profiles = [(name, dict(config[name])) for name in config.sections() if name == "mypy" or name.startswith("mypy-")]
        for name, settings in profiles:
            if loosened := sorted(MYPY_LOOSENING_KEYS & set(settings)):
                findings.append(
                    Finding(path, f"MyPy loosening keys in {name}: {', '.join(loosened)}", f"{POLICY}: lint settings", "remove them and fix the code")
                )
    return findings


def mypy_command(path: str, line: int, arguments: Sequence[str | None], files: set[str]) -> list[Finding]:
    findings = []
    banned = {"--" + key.replace("_", "-") for key in MYPY_LOOSENING_KEYS}
    if any(argument and argument.split("=", 1)[0] in banned for argument in arguments):
        findings.append(Finding(f"{path}:{line}", "MyPy loosening command flags present", f"{POLICY}: lint settings", "remove them and fix the code"))
    for index, argument in enumerate(arguments):
        if argument == "--config-file" or (argument and argument.startswith("--config-file=")):
            config = arguments[index + 1] if argument == "--config-file" and index + 1 < len(arguments) else argument.removeprefix("--config-file=")
            if config not in files:
                findings.append(
                    Finding(
                        f"{path}:{line}",
                        "MyPy configuration must name a committed profile instead of a generated or dynamic file",
                        f"{POLICY}: lint settings",
                        "use pyproject.toml or a committed MyPy profile so every profile is checked",
                    )
                )
    return findings


def mypy_commands() -> list[Finding]:
    """Check every literal MyPy command and require explicit configuration files to be in the inspected inventory."""
    files = set(git("ls-files", "--cached", "--others", "--exclude-standard").splitlines())
    findings = []
    for path in sorted(files):
        if path.endswith(".sh"):
            for number, line in enumerate(Path(path).read_text().splitlines(), 1):
                if re.search(r"\b(?:mypy|scripts\.quality\.typecheck)\b", line) and not line.lstrip().startswith("#"):
                    findings.extend(mypy_command(path, number, shlex.split(line.rstrip("\\"), comments=True), files))
        elif path.endswith(".py"):
            for node in ast.walk(ast.parse(Path(path).read_text())):
                if isinstance(node, (ast.List, ast.Tuple)):
                    arguments = [item.value if isinstance(item, ast.Constant) and isinstance(item.value, str) else None for item in node.elts]
                    if "mypy" in arguments or "scripts.quality.typecheck" in arguments:
                        findings.extend(mypy_command(path, node.lineno, arguments, files))
    return findings


def env_list() -> tuple[dict[str, Any], list[str]]:
    """Return ``ENV_LIST`` and the keys that one of its objects holds twice.

    The list has ``env_file`` (the settings of .env, in order), ``test_modules`` (path to reason) and ``names`` (name to entry).
    """
    path = Path(ENV_LIST)
    twice: list[str] = []

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        """Build one JSON object and note each key that it holds twice."""
        keys = [key for key, _ in items]
        twice.extend(sorted({key for key in keys if keys.count(key) > 1}))
        return dict(items)

    return (json.loads(path.read_text(), object_pairs_hook=pairs) if path.exists() else {}), twice


def env_without_comments(text: str, kind: str) -> str:
    """Return ``text`` of a ``kind`` file (hcl, yaml, shell, docker or python) with its comments cut out.

    HCL loses ``#`` and ``//`` to the end of the line and ``/* */`` blocks. YAML, shell and Dockerfiles lose ``#`` at the start of a word. Python loses its
    comment tokens, and a file that Python cannot tokenize is cut like a shell file. A quote opens a string that ends with the same quote or the line, so
    ``"s3://bucket"`` keeps its slashes.
    """
    if kind == "python":
        try:
            return tokenize.untokenize(token for token in tokenize.generate_tokens(io.StringIO(text).readline) if token.type != tokenize.COMMENT)
        except (tokenize.TokenError, SyntaxError, IndentationError):
            kind = "shell"
    hcl = kind == "hcl"
    out: list[str] = []
    quote, i = "", 0
    while i < len(text):
        char = text[i]
        if quote:
            out.append(char)
            if char == "\\" and i + 1 < len(text):
                out.append(text[i + 1])
                i += 1
            elif char in (quote, "\n"):
                quote = ""
        elif char in ('"' if hcl else "\"'"):
            quote = char
            out.append(char)
        elif hcl and text.startswith("/*", i):
            end = text.find("*/", i + 2)
            out.append(" ")
            i = len(text) if end < 0 else end + 1
        elif (char == "#" or (hcl and text.startswith("//", i))) and (hcl or i == 0 or text[i - 1].isspace()):
            while i + 1 < len(text) and text[i + 1] != "\n":
                i += 1
        else:
            out.append(char)
        i += 1
    return "".join(out)


def env_setter_kind(path: str) -> str | None:
    """Return how a file sets environment variables (hcl, json, yaml, shell, docker or python); None if it sets none, as a JSON schema only describes them."""
    name = Path(path).name
    if name.endswith(".schema.json"):
        return None
    if name == "Dockerfile":
        return "docker"
    if name.endswith((".sh", ".sh.tftpl", ".bash")):
        return "shell"
    return {".tf": "hcl", ".hcl": "hcl", ".json": "json", ".yaml": "yaml", ".yml": "yaml", ".py": "python"}.get(Path(path).suffix)


def job_config_variables(text: str) -> set[str]:
    """Return the variables that a job config declares: the strings of ``jobs.<job>.containers.<container>``, which is a list of names.

    Only that exact path counts, so a ``containers`` key anywhere else, notes or metadata never declares a variable. A file with a key that one of its objects
    holds twice, or that is not JSON, declares none.
    """

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        """Build one JSON object, refusing a key that it holds twice."""
        if len({key for key, _ in items}) != len(items):
            raise ValueError("a key is held twice")
        return dict(items)

    try:
        data = json.loads(text, object_pairs_hook=pairs)
    except ValueError:
        return set()
    found: set[str] = set()
    jobs = data.get("jobs") if isinstance(data, dict) else None
    for job in jobs.values() if isinstance(jobs, dict) else ():
        containers = job.get("containers") if isinstance(job, dict) else None
        for variables in containers.values() if isinstance(containers, dict) else ():
            if isinstance(variables, list):
                found.update(item for item in variables if isinstance(item, str))
    return found


def env_setters(path: str, text: str, names: set[str]) -> set[str]:
    """Return the ``names`` that the file ``path`` sets in an assignment form.

    The forms are ``name = "X"`` (an environment entry) and ``"X=value"`` in HCL, YAML and Python, a mapping key (``X = ...`` in HCL, ``X: ...`` and
    ``- X=...`` in YAML, ``"X": ...`` in Python), ``X=...`` or ``export X=...`` in shell, ``ENV X`` in a Dockerfile and ``env["X"] = ...`` or
    ``setdefault("X", ...)`` in Python. A JSON file sets only what ``job_config_variables`` finds in one of the ``JOB_CONFIGS``: no other JSON file sets a
    variable, whatever it holds. Comments and reads never count.
    """
    kind = env_setter_kind(path)
    if kind is None or not names:
        return set()
    if kind == "json":
        return names & job_config_variables(text) if path in JOB_CONFIGS else set()
    alt = "|".join(map(re.escape, sorted(names)))
    forms = {
        "name": rf"""["']?\bname["']?\s*[:=]\s*["']?({alt})["']?(?!\w)""",
        "string": rf"""["']({alt})=""",
        "hcl": rf"""["']({alt})["']\s*=(?!=)|^\s*({alt})\s*=(?!=)""",
        "yaml": rf"""["']({alt})["']\s*:|^\s*-?\s*({alt})\s*:|^\s*-\s*({alt})=""",
        "shell": rf"""^\s*(?:export\s+)?({alt})=""",
        "docker": rf"""^\s*ENV\s+({alt})(?:=|\s)""",
        "python": rf"""\[\s*["']({alt})["']\s*\]\s*=(?!=)|\bsetdefault\(\s*["']({alt})["']|["']({alt})["']\s*:""",
    }
    wanted = {"hcl": ("name", "string", "hcl"), "yaml": ("name", "string", "yaml"), "python": ("name", "string", "python")}
    pattern = re.compile("|".join(forms[form] for form in wanted.get(kind, (kind,))), re.M)
    return {next(group for group in match.groups() if group) for match in pattern.finditer(env_without_comments(text, kind))}


def env_example() -> list[Finding]:
    """Every environment variable that tracked Python code reads is in .env.example or in ``ENV_LIST``, never in both.

    .env.example is a direct example of the real .env: one header comment line at most, then the names of ``env_file`` and nothing else, one placeholder line
    each. Every other name is in ``ENV_LIST`` with a category: ``test`` (only test code uses it, see ``TEST_CODE``), ``container`` (Terraform, ECS, Airflow,
    a job template or the launcher sets it inside a container), ``shell`` (an optional override that a person may export for one command) or ``tool``
    (the OS or a tool sets it). A container name must be set in an assignment form (see ``env_setters``) in a ``CONTAINER_SETTERS`` file, in the file named by
    ``set_in`` or by the platform (``set_by`` is ``ecs``). A JSON file counts only as one of the ``JOB_CONFIGS``, at the exact path of ``job_config_variables``.
    ``set_by`` ``alias`` with ``set_in`` is the one exception that is not verified: project code sets the name through a constant that the check does not
    follow, and ``set_in``, a Python file, only has to use the name. The check shows where a name is set, not that the value is right. Aliases, indirect
    and dynamic reads, and reads in shell or template files only are not followed.
    """
    data, twice = env_list()
    env_file: list[str] = data.get("env_file", [])
    names: dict[str, Any] = data.get("names", {})
    extra_modules: dict[str, str] = data.get("test_modules", {})
    documented: list[tuple[int, str]] = []
    findings = []
    for key in sorted({*twice, *(name for name in env_file if env_file.count(name) > 1)}):
        findings.append(Finding(ENV_LIST, f"{key} is listed twice in {ENV_LIST}", f"{POLICY}: docs match the code", f"keep one entry of {key}"))
    example = Path(".env.example")
    for number, line in enumerate(example.read_text().splitlines() if example.exists() else [], 1):
        match = ENV_EXAMPLE_NAME.match(line)
        if match and line.lstrip().startswith("#"):
            fix = f"remove {match.group(1)} from .env.example: it shows only the settings a person puts in .env"
            findings.append(Finding(f".env.example:{number}", f"{match.group(1)} is commented out in .env.example", f"{POLICY}: docs match the code", fix))
        elif match:
            documented.append((number, match.group(1)))
        elif number > 1 or not line.startswith("#"):
            fix = "keep one header comment line and the settings of .env, one per line"
            findings.append(
                Finding(
                    f".env.example:{number}",
                    f".env.example line {number} is not a setting: only one header comment line is allowed",
                    f"{POLICY}: docs match the code",
                    fix,
                )
            )
    shown = {name for _, name in documented}
    tracked = set(git("ls-files").splitlines())

    def is_test(path: str) -> bool:
        """Return whether ``path`` is test code: it matches ``TEST_CODE`` or is a listed test module."""
        return bool(TEST_CODE.search(path)) or path in extra_modules

    for module in sorted(extra_modules):
        if module not in tracked:
            findings.append(
                Finding(ENV_LIST, f"{module} is listed as a test module but is not a tracked file", CLEANUP_RULE, f"remove {module} from {ENV_LIST}")
            )
    for path in git("ls-files", "*.py").splitlines():
        for number, line in enumerate(Path(path).read_text().splitlines(), 1):
            for name in ENV_READ.findall(line):
                if name not in shown and name not in names:
                    fix = f"add {name} to .env.example when a person sets it in .env, otherwise to {ENV_LIST} with the category test, container, shell or tool"
                    findings.append(
                        Finding(f"{path}:{number}", f"{name} is read but is in neither .env.example nor {ENV_LIST}", f"{POLICY}: docs match the code", fix)
                    )
    # A name is a whole word of a file exactly when it is one of its \w+ runs: one pass over the files, not one search of all their text per name.
    # Test code and production code are told apart per file; Markdown only mentions a name, so it never counts as use.
    interest = shown | set(names) | set(env_file)
    users: dict[str, tuple[set[str], set[str]]] = {name: (set(), set()) for name in interest}
    setters: dict[str, set[str]] = {name: set() for name in names if isinstance(names[name], dict) and names[name].get("category") == "container"}
    for path, lines in reference_files().items():
        if path in (".env.example", ENV_LIST) or path.endswith(".md"):
            continue
        for word in {word for line in lines for word in re.findall(r"\w+", line)} & interest:
            users[word][0 if is_test(path) else 1].add(path)
        if setters and CONTAINER_SETTERS.match(path) and not is_test(path):
            for name in env_setters(path, "\n".join(lines), set(setters)):
                setters[name].add(path)
    for number, name in documented:
        if name in names:
            fix = f"keep {name} only in {ENV_LIST}, or only in .env.example when a person sets it in .env"
            findings.append(Finding(f".env.example:{number}", f"{name} is in both .env.example and {ENV_LIST}", f"{POLICY}: docs match the code", fix))
        elif name not in env_file:
            fix = f"move {name} to {ENV_LIST} with the category test, container, shell or tool"
            findings.append(
                Finding(f".env.example:{number}", f"{name} is in .env.example but not in env_file of {ENV_LIST}", f"{POLICY}: docs match the code", fix)
            )
        elif not users[name][1]:
            fix = f"remove {name} from .env.example and env_file, or restore the production code that uses it"
            findings.append(Finding(f".env.example:{number}", f"{name} is in .env.example but production code does not use it", CLEANUP_RULE, fix))
    for name in env_file:
        if name not in shown:
            findings.append(
                Finding(
                    ENV_LIST,
                    f"{name} is in env_file but not in .env.example",
                    f"{POLICY}: docs match the code",
                    f"add {name}= with a placeholder to .env.example",
                )
            )
    if shown <= set(env_file) and set(env_file) <= shown and [name for _, name in documented] != env_file:
        fix = f"list the names in .env.example in the order of env_file in {ENV_LIST}"
        findings.append(Finding(".env.example", ".env.example lists its names in a different order than env_file", f"{POLICY}: docs match the code", fix))
    for name in sorted(names):
        entry, (tests, production) = names[name], users[name]
        category = entry.get("category") if isinstance(entry, dict) else None
        if category not in ENV_CATEGORIES:
            fix = f"give {name} one of the categories test, container, shell or tool in {ENV_LIST}"
            findings.append(Finding(ENV_LIST, f"{name} has category {category}: use test, container, shell or tool", f"{POLICY}: docs match the code", fix))
        elif category == "test":
            if production:
                fix = f"change the category of {name} to container, shell or tool, or stop production code from using it"
                findings.append(Finding(ENV_LIST, f"{name} is listed as test but production code also uses it", f"{POLICY}: docs match the code", fix))
            elif not tests:
                findings.append(Finding(ENV_LIST, f"{name} is listed as test but no test code uses it", CLEANUP_RULE, f"remove {name} from {ENV_LIST}"))
        elif not production:
            findings.append(Finding(ENV_LIST, f"{name} is listed as {category} but no production code uses it", CLEANUP_RULE, f"remove {name} from {ENV_LIST}"))
        elif category == "container" and not setters[name] and entry.get("set_by") != "ecs":
            home = entry.get("set_in")
            if home is None:
                fix = f"set {name} in infra/ or a job or launcher config, or name its file in set_in, or set set_by to ecs in {ENV_LIST}"
                findings.append(
                    Finding(
                        ENV_LIST,
                        f"{name} is listed as container but nothing in infra/ or the job and launcher configs sets it",
                        f"{POLICY}: docs match the code",
                        fix,
                    )
                )
            elif entry.get("set_by") == "alias":
                if not (home in tracked and home.endswith(".py") and not is_test(home) and re.search(rf"\b{re.escape(name)}\b", Path(home).read_text())):
                    fix = f"name a tracked production Python file that uses {name} in set_in, or remove it"
                    message = f"{name} is listed with set_by alias and set_in {home}, which is not a tracked production Python file that uses it"
                    findings.append(Finding(ENV_LIST, message, f"{POLICY}: docs match the code", fix))
            elif not (home in tracked and not is_test(home) and name in env_setters(home, Path(home).read_text(), {name})):
                fix = f"name a tracked production file that sets {name} in set_in, or remove it"
                message = f"{name} is listed with set_in {home}, which is not a tracked production file that sets it"
                findings.append(Finding(ENV_LIST, message, f"{POLICY}: docs match the code", fix))
    return findings


def is_workflow_file(path: str) -> bool:
    """Return whether ``path`` is a workflow or composite action file: its input names belong to the action or workflow that takes them."""
    return Path(path).name in ("action.yml", "action.yaml") or bool(re.fullmatch(r"\.github/workflows/[^/]+\.ya?ml", path))


def input_line_numbers(path: str, text: str) -> set[int]:
    """Return the numbers (from 1) of the lines of a workflow or composite action file that only name inputs.

    An input name is a key of a mapping that is the value of a ``with:`` key, or of the ``inputs:`` of an action (action.yml, action.yaml) or of a
    reusable workflow (``on.workflow_call.inputs``). It is a name of the action or workflow that takes it, never a setting or a program of this
    project. The file is read as YAML, so flow mappings, quoted keys and anchors count. A mapping inside an input (its metadata, such as
    ``{type: string}``) is part of the input and never vetoes it. An anchored mapping that an alias also uses outside an input position
    (``env: *params``), and a line that holds any other key, never count. A file that is not valid YAML counts nothing.
    """
    action = Path(path).name in ("action.yml", "action.yaml")
    if not is_workflow_file(path):
        return set()
    yaml = importlib.import_module("yaml")
    inputs = ("inputs",) if action else ("on", "workflow_call", "inputs")
    uses: dict[int, tuple[Any, set[str]]] = {}

    def visit(node: Any, role: str, trail: tuple[str, ...], active: frozenset[int]) -> None:
        """Walk one use of ``node``; ``role`` is ``input`` (keys name inputs), ``inside`` (part of an input) or ``other``."""
        if isinstance(node, yaml.SequenceNode):
            for item in node.value:
                visit(item, "inside" if role == "inside" else "other", trail, active)
        elif isinstance(node, yaml.MappingNode):
            uses.setdefault(id(node), (node, set()))[1].add(role)
            if id(node) in active:
                return
            for key, value in node.value:
                step = (*trail, key.value if isinstance(key, yaml.ScalarNode) else "")
                inner = "inside" if role in ("input", "inside") else ("input" if step[-1] == "with" or step == inputs else "other")
                visit(value, inner, step, active | {id(node)})

    try:
        for document in yaml.compose_all(text):
            if document is not None:
                visit(document, "other", (), frozenset())
    except yaml.YAMLError:
        return set()
    names: set[int] = set()
    others: set[int] = set()
    for node, roles in uses.values():
        if roles != {"inside"}:
            (names if roles == {"input"} else others).update(key.start_mark.line + 1 for key, _ in node.value)
    return names - others


def text_codec(data: bytes) -> str | None:
    """Return the codec for one file version: None for binary data (NUL byte), UTF-8 when the whole file decodes, otherwise Latin-1, which reads every byte."""
    if b"\0" in data:
        return None
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        return "latin-1"
    return "utf-8"


def git_text(*args: str) -> str:
    """Run Git and return its output, keeping bytes that are not UTF-8 as lone surrogates so a caller can read them with ``text_codec`` instead of raising."""
    return run_command("git", args, check=True, errors="surrogateescape").stdout


def git_blob(spec: str) -> bytes:
    """Return the bytes of the Git object ``spec`` (``<ref>:<path>``, or ``:<path>`` for the staged file)."""
    return git_text("show", spec).encode("utf-8", "surrogateescape")


def changed_lines_and_deletions() -> tuple[dict[str, list[str]], list[str]]:
    """Return removed lines by file and deleted paths, from the staged diff or, in CI, the pushed range.

    A removed line that only named an input of a workflow or action (see ``input_line_numbers``) is left out: the input is gone, the name stays free.
    Each file version is read with one codec (see ``text_codec``), also for its removed lines. A changed file whose old or new version holds NUL bytes is
    binary: it is skipped and named in ``SKIPPED``, which the command prints.
    """
    SKIPPED.clear()
    refs = [os.environ["PRE_COMMIT_FROM_REF"], os.environ["PRE_COMMIT_TO_REF"]] if "PRE_COMMIT_FROM_REF" in os.environ else ["--cached"]
    old_ref, new_ref = (refs[0], refs[1]) if len(refs) == 2 else ("HEAD", "")
    deleted = git("diff", *refs, "--name-only", "--diff-filter=D").splitlines()
    removed: dict[str, list[str]] = {}
    inputs: dict[str, set[int]] = {}
    codecs: dict[str, str] = {}
    current, new_path, in_header, old_number = "", "", False, 0
    for line in git_text("diff", *refs, "-U0", "--inter-hunk-context=0", "--no-color").splitlines():
        binary = BINARY_DIFF.fullmatch(line) if in_header else None
        if line.startswith("diff --git "):
            current, new_path, in_header = "", "", True
        elif binary:
            skip_binary(binary.group("new") or binary.group("old") or "")
        elif in_header and line.startswith("--- "):
            current = line[6:] if line.startswith("--- a/") else ""
        elif in_header and line.startswith("+++ "):
            new_path = line[6:] if line.startswith("+++ b/") else ""
        elif line.startswith("@@"):
            in_header = False
            old_number = int(line.split()[1].lstrip("-").split(",")[0])
        elif not in_header and line.startswith("-") and current:
            if current not in inputs:
                before = git_blob(f"{old_ref}:{current}")
                now = git_blob(f"{new_ref}:{new_path}") if new_path else b""
                codec = text_codec(before)
                if codec is None or text_codec(now) is None:
                    skip_binary(current)
                    inputs[current] = set()
                else:
                    codecs[current] = codec
                    inputs[current] = input_line_numbers(current, before.decode(codec))
            if current in codecs and old_number not in inputs[current]:
                removed.setdefault(current, []).append(line[1:].encode("utf-8", "surrogateescape").decode(codecs[current]))
            old_number += 1
    return removed, deleted


def without_fingerprint_files(text: str) -> str:
    """Return a code-version record with the ``file`` value of each fingerprint emptied; any text of another shape comes back unchanged."""
    try:
        record = json.loads(text)
    except ValueError:
        return text
    versions = record.get("versions") if isinstance(record, dict) else None
    if not isinstance(versions, list):
        return text
    for version in versions:
        fingerprints = version.get("code_sha256") if isinstance(version, dict) else None
        for fingerprint in fingerprints if isinstance(fingerprints, list) else []:
            if isinstance(fingerprint, dict) and isinstance(fingerprint.get("file"), str):
                fingerprint["file"] = ""
    return json.dumps(record, indent=2, ensure_ascii=False) + "\n"


def reference_files(record_skips: bool = False) -> dict[str, list[str]]:
    """Return the lines of every tracked text file that describes current behavior; history and the allowlist are exempt.

    Each file is read with one codec (see ``text_codec``). A file with NUL bytes is binary and left out; ``record_skips`` names it in ``SKIPPED``.
    """
    files: dict[str, list[str]] = {}
    for path in git("ls-files").splitlines():
        if path == "CHANGELOG.md" or path.startswith("e2e/") or path == CLEANUP_ALLOWLIST or not Path(path).is_file():
            continue
        data = Path(path).read_bytes()
        codec = text_codec(data)
        if codec is not None:
            text = data.decode(codec)
            files[path] = (without_fingerprint_files(text) if CODE_VERSION_RECORDS.fullmatch(path) else text).splitlines()
        elif record_skips:
            skip_binary(path)
    return files


def search_view(kind: str, path: str, lines: list[str]) -> list[str] | None:
    """Return the text of each line to search for a removed name of ``kind``, or None to skip the file.

    Scripts, flags and dependencies: every line. Environment variables: Markdown and .env.example, where people read
    them. Code names: code and config files, plus Markdown code spans and fences, so "the main branch" in prose is not a
    reference. Python names are searched only in the Python files MyPy does not cover (see ``removed_names``), skipping
    comments, docstrings, quoted strings and attribute access (``parser.parse_args()``). Terraform skips ``from =``
    lines: moved and removed blocks name old addresses on purpose.
    """
    suffix, name = Path(path).suffix, Path(path).name
    if kind == "script" and suffix == ".json":
        return [decoded_json_line(line) if "\\" in line else line for line in lines]
    if kind in ("script", "flag", "dependency"):
        return lines
    if kind == "env":
        return lines if suffix == ".md" or name == ".env.example" else None
    if kind == "python":
        return python_code(lines)
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


def config_key(line: str) -> str | None:
    """Return the key a YAML, TOML, JSON or INI line defines, or None for comments and other lines."""
    if line.lstrip().startswith(("#", ";", "//")):
        return None
    match = CONFIG_KEY.match(line)
    return match.group(1) if match else None


def requirement_names(lines_by_file: dict[str, list[str]]) -> set[str]:
    """Return the canonical package names in requirements files and in the quoted requirement lines of pyproject.toml."""
    names = set()
    for path, lines in lines_by_file.items():
        if re.search(r"(^|/)requirements[^/]*\.(txt|in)$", path):
            names |= {re.sub(r"[-_.]+", "-", match.group(1).lower()) for line in lines if (match := REQUIREMENT.match(line))}
        elif Path(path).name == "pyproject.toml":
            names |= {re.sub(r"[-_.]+", "-", match.group(1).lower()) for line in lines if (match := PYPROJECT_REQUIREMENT.match(line))}
    return names


DOTTED_WORD = re.compile(r"[A-Za-z_]\w*(?:\.\w+)*")
IMPORT_CALLS = {"__import__", "import_module", "find_spec"}
# The program of a Python command: python, python3, python3.12 (any folder) or a shell variable named like it.
PYTHON_PROGRAM = re.compile(r"python[\d.]*|\$\{?\w*PYTHON\w*\}?", re.IGNORECASE)


def dead_prefixes(path: str, names: list[str], roots: list[str]) -> set[str]:
    """Return the dotted names a deleted Python file takes away: its module path below each import root and below none, or, for an ``__init__.py``, each
    top-level name in its package (the package itself stays importable as a namespace)."""
    from scripts.quality.typecheck_importers import module_of

    modules = {module_of(path, roots), module_of(path, [])} - {"__init__"}
    if Path(path).name == "__init__.py":
        return {f"{module}.{name}" for module in modules for name in names if len(name) >= 3 and not (name.startswith("__") and name.endswith("__"))}
    return modules


def refers_to(dotted: str, prefixes: set[str]) -> bool:
    return any(dotted == prefix or dotted.startswith(f"{prefix}.") for prefix in prefixes)


def command_modules(text: str) -> list[str]:
    """Return the modules a command written in one string runs with ``-m``, reading the interpreter options as CPython does.

    A Python program is ``python``, ``python3.12`` or a path to one, behind a launcher (``uv run python``) or not, or a shell variable named like Python.
    After it come options: ``-W`` and ``-X`` take a value (the rest of the word, or the next word), as does ``--check-hash-based-pycs``; other letters
    are flags that may share a word (``-uB``, ``-OO``); ``-m`` takes the module (attached or the next word). A word that is not an option, ``-``,
    ``--`` and ``-c`` end the options, so a later ``-m`` belongs to the script or the program, not to Python. The string is split like a shell line,
    or at spaces when its quotes do not pair.
    """
    if "-m" not in text:
        return []
    try:
        words = shlex.split(text)
    except ValueError:
        words = text.split()
    words = [word.strip("`'\"()") for word in words]
    modules = []
    for program, word in enumerate(words):
        if not PYTHON_PROGRAM.fullmatch(Path(word).name):
            continue
        index = program + 1
        while index < len(words) and words[index].startswith("-") and words[index] not in ("-", "--"):
            option = words[index]
            if option == "--check-hash-based-pycs":
                index += 2
            elif option.startswith("--"):
                index += 1
            else:
                letters = option[1:]
                position = next((place for place, letter in enumerate(letters) if letter in "WXcm"), None)
                if position is None:
                    index += 1
                    continue
                letter, attached = letters[position], letters[position + 1 :]
                if letter == "c":
                    break
                if letter == "m":
                    value = attached or (words[index + 1] if index + 1 < len(words) else "")
                    if DOTTED_WORD.fullmatch(value.rstrip(".,;:")):
                        modules.append(value.rstrip(".,;:"))
                    break
                index += 1 if attached else 2
            continue
    return modules


def python_module_uses(tree: ast.Module, package: str, dead: dict[str, set[str]]) -> set[tuple[int, str]]:
    """Return the (line, deleted module) pairs a parsed Python file refers to.

    An import statement counts by the complete statement (continuations, parentheses, aliases; a relative import is resolved against ``package``). A name
    an import binds stands for the module it names, so an attribute read through it counts. A string that is a dotted module path counts, and so does the
    first argument of ``__import__``, ``import_module`` and ``find_spec``, the word after ``-m`` in a command list (a root module has no dot) and the
    word after ``-m`` in a command written as one string (``command_modules``). A name that no import bound stands for nothing.

    Known limits, each judged not to hide a reader in this repository: a name stays bound for the whole file, so an attribute read through a name that
    another import rebinds is read as the last import says; a bare ``import util`` beside ``util.py`` (run as a script) names no package, so it is not
    matched to ``pkg.util``; a deleted ``__init__.py`` takes away only the functions and classes it defines, not the names it imports; ``from pkg import *``
    binds nothing; a path without ``.py`` (``app/util``) in a configuration file is not read as a module; a runtime alias of a package
    (config/quality/dependency_runtimes.json) is not mapped back to its folder; a docstring that is only a dotted module path counts as a string.
    """
    hits: set[tuple[int, str]] = set()
    bound: dict[str, str] = {}

    def check(line: int, dotted: str) -> None:
        hits.update((line, module) for module, prefixes in dead.items() if refers_to(dotted, prefixes))

    def text(node: ast.expr) -> str | None:
        return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None

    parts = package.split(".") if package else []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                top = alias.name.partition(".")[0]
                bound[alias.asname or top] = alias.name if alias.asname else top
                check(node.lineno, alias.name)
        elif isinstance(node, ast.ImportFrom) and node.level <= len(parts):  # A relative import above the top package is no import of a module.
            anchor = parts[: len(parts) - (node.level - 1)] if node.level else []
            base = ".".join([*anchor, *([node.module] if node.module else [])])
            check(node.lineno, base)
            for alias in (alias for alias in node.names if alias.name != "*"):
                bound[alias.asname or alias.name] = f"{base}.{alias.name}" if base else alias.name
                check(node.lineno, bound[alias.asname or alias.name])
        elif isinstance(node, ast.Attribute):
            chain: list[str] = []
            receiver: ast.expr = node
            while isinstance(receiver, ast.Attribute):
                chain.append(receiver.attr)
                receiver = receiver.value
            if isinstance(receiver, ast.Name) and receiver.id in bound:
                check(node.lineno, ".".join([bound[receiver.id], *reversed(chain)]))
        elif isinstance(node, ast.Call) and node.args and text(node.args[0]) is not None:
            called = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else ""
            if called in IMPORT_CALLS:
                check(node.lineno, str(text(node.args[0])))
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            word = node.value.partition(":")[0]
            if "." in word and DOTTED_WORD.fullmatch(word):
                check(node.lineno, word)
            for module in command_modules(node.value):
                check(node.lineno, module)
        elif isinstance(node, ast.List | ast.Tuple):
            for flag, item in zip(node.elts, node.elts[1:], strict=False):
                if text(flag) == "-m" and text(item) is not None:
                    check(item.lineno, str(text(item)))
    return hits


def module_text(prefixes: set[str]) -> re.Pattern[str]:
    """Return the pattern for a line of text (not Python) that names a deleted module: its full dotted path, or ``-m`` and a root module's name."""
    dotted = "|".join(re.escape(prefix) for prefix in sorted(prefixes) if "." in prefix) or "(?!)"
    command = "|".join(re.escape(prefix) for prefix in sorted(prefixes)) or "(?!)"
    return re.compile(rf"(?<![\w.])(?:{dotted})(?!\w)|(?<![\w-])-m\s+(?:{command})(?![\w.-])")


def deleted_module_findings(dead: dict[str, set[str]], files: dict[str, list[str]], roots: list[str], allowed: list[tuple[str, str]]) -> list[Finding]:
    """Find the remaining references to the deleted Python modules in ``dead`` (display name to dotted prefixes).

    A Python file is read as a syntax tree (``python_module_uses``) and a text file, or a Python file that does not parse, by line (``module_text``).
    """
    from scripts.quality.typecheck_importers import module_of

    patterns = {module: module_text(prefixes) for module, prefixes in dead.items()}
    stems = {prefix.rpartition(".")[2] for prefixes in dead.values() for prefix in prefixes}
    findings: list[Finding] = []
    for path, lines in files.items():
        if any(kind == "script" and fnmatch.fnmatch(path, glob) for kind, glob in allowed):
            continue
        uses: set[tuple[int, str]] | None = None
        if path.endswith(".py"):
            source = "\n".join(lines)
            if not any(stem in source for stem in stems):
                continue
            try:
                module = module_of(path, roots)
                uses = python_module_uses(ast.parse(source, filename=path), module if path.endswith("__init__.py") else module.rpartition(".")[0], dead)
            except SyntaxError:
                uses = None
        if uses is None:
            uses = {(number, module) for number, line in enumerate(lines, 1) for module, pattern in patterns.items() if pattern.search(line)}
        for number, module in sorted(uses):
            fix = f"remove or update the reference, or add 'script {path} -- <why the reference stays> -- <expiry>' to {CLEANUP_ALLOWLIST}"
            findings.append(Finding(f"{path}:{number}", f"{module} was removed but is still referenced", CLEANUP_RULE, fix))
    return findings


HEREDOC_START = re.compile(r"<<-?\s*([A-Za-z_]\w*)\s*$")


def hcl_code(lines: list[str]) -> list[str]:
    """Return Terraform lines with the comments (``#``, ``//`` and ``/* */``) and the bodies of heredocs blanked, so only real HCL syntax is left.

    A comment sign inside a quoted string, also inside an interpolation, starts no comment. The line count stays the same.
    """
    code: list[str] = []
    heredoc, in_block = "", False
    for line in lines:
        if heredoc:
            heredoc = "" if line.strip() == heredoc else heredoc
            code.append("")
            continue
        text, index, depth, quoted = "", 0, 0, False
        while index < len(line):
            char, pair = line[index], line[index : index + 2]
            step = 1
            if in_block:
                in_block = pair != "*/"
                step = 1 if in_block else 2
            elif quoted and char == "\\":
                text += pair
                step = 2
            elif quoted:
                if pair == "${":
                    depth += 1
                    text += pair
                    step = 2
                else:
                    depth -= 1 if char == "}" and depth else 0
                    quoted = not (char == '"' and not depth)
                    text += char
            elif pair == "/*":
                in_block, step = True, 2
            elif char == "#" or pair == "//":
                break
            else:
                quoted = char == '"'
                text += char
            index += step
        start = HEREDOC_START.search(text)
        heredoc = start.group(1) if start else ""
        code.append(text)
    return code


STRUCTURED_SUFFIXES = {".json", ".yaml", ".yml", ".toml"}
PATH_WORD = re.compile(r"[\w.-]*/[\w./-]*|[\w-]+\.(?:md|py|txt|json|ya?ml|toml|sh|sql|tf)\b")
LITERAL_TOKENS = {tokenize.STRING, tokenize.FSTRING_MIDDLE}


def python_literals(lines: list[str], rewrite: Callable[[str], str], comments: bool = False, drop_unread: bool = False) -> list[str]:
    """Return the Python lines with the text of each string literal (and each comment, with ``comments``) replaced by ``rewrite`` of it.

    ``rewrite`` returns text of the same length. Tokens read before a tokenizer error count, so a file that does not parse is still read as far as it goes.
    With ``drop_unread`` the text after the last token read is blanked, because a string in it could not be told from code.
    """
    rows = [list(line) for line in lines]
    kinds = LITERAL_TOKENS | ({tokenize.COMMENT} if comments else set())
    read_to = (1, 0)
    try:
        for token in tokenize.generate_tokens(io.StringIO("\n".join(lines) + "\n").readline):
            read_to = token.end
            if token.type not in kinds:
                continue
            (first, start), (last, end) = token.start, token.end
            for row in range(first, last + 1):
                begin, stop = start if row == first else 0, end if row == last else len(rows[row - 1])
                rows[row - 1][begin:stop] = list(rewrite("".join(rows[row - 1][begin:stop])))
    except (tokenize.TokenError, SyntaxError):
        if drop_unread:
            for number in range(read_to[0], len(rows) + 1):
                begin = read_to[1] if number == read_to[0] else 0
                rows[number - 1][begin:] = list(blanked("".join(rows[number - 1][begin:])))
    return ["".join(row) for row in rows]


def blanked(text: str) -> str:
    return re.sub(r"[^\n]", " ", text)


def without_paths(text: str) -> str:
    return PATH_WORD.sub(lambda match: " " * len(match.group()), text)


def python_declarations(lines: list[str]) -> set[str]:
    """Return the names Python code declares: a function, class, parameter, assignment or annotated target, or attribute store.

    Reading a name (an attribute access, a string key, a call keyword or an import) declares nothing. A file that does not parse is read by line for
    definitions and assignments, with its strings and comments blanked and the text after a tokenizer error left out.
    """
    try:
        tree = ast.parse("\n".join(lines))
    except SyntaxError:
        view = "\n".join(python_literals(lines, blanked, comments=True, drop_unread=True))
        return set(PY_DEFINITION.findall(view)) | set(PY_ASSIGNED.findall(view))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            names.add(node.name)
        elif isinstance(node, ast.arg):
            names.add(node.arg)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            names.add(node.id)
        elif isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Store):
            names.add(node.attr)
    return names


def python_top_level(lines: list[str]) -> set[str]:
    """Return the functions and classes a Python file defines at module level, read without its strings, its comments and the text after a tokenizer error."""
    return set(PY_TOP_LEVEL.findall("\n".join(python_literals(lines, blanked, comments=True, drop_unread=True))))


def hcl_declarations(lines: list[str]) -> set[str]:
    """Return the names Terraform code declares: an argument or attribute, a nested block and the labels of a declared block.

    Comments and heredocs declare none.
    """
    code = hcl_code(lines)
    names = {key for line in code if (key := config_key(line))}
    names |= {block.group(1) for line in code if (block := TF_NESTED_BLOCK.match(line))}
    return names | {label for match in TF_BLOCK.findall("\n".join(code)) for label in match[1:] if label}


def hcl_blocks(lines: list[str]) -> set[tuple[str, str, str]]:
    """Return the (kind, name, second name) of each block Terraform code declares, not counting a comment or a heredoc."""
    return set(TF_BLOCK.findall("\n".join(hcl_code(lines))))


def structured_declarations(path: str, text: str) -> tuple[set[str], set[str]] | None:
    """Return the keys and the ``$id`` values of a JSON, YAML or TOML document at any depth, decoded, whatever its indentation or line breaks.

    None means text that does not parse. A YAML block scalar or a JSON string that holds a key-like text declares no key.
    """
    suffix = Path(path).suffix
    yaml = importlib.import_module("yaml")
    try:
        if suffix == ".json":
            documents = [json.loads(text)]
        elif suffix in (".yaml", ".yml"):
            documents = list(yaml.safe_load_all(text))
        else:
            documents = [tomllib.loads(text)]
    except (ValueError, yaml.YAMLError):
        return None
    keys: set[str] = set()
    ids: set[str] = set()
    pending = list(documents)
    while pending:
        node = pending.pop()
        if isinstance(node, dict):
            keys |= {str(key) for key in node}
            ids |= {value for key, value in node.items() if key == "$id" and isinstance(value, str)}
            pending.extend(node.values())
        elif isinstance(node, list):
            pending.extend(node)
    return keys, ids


def declared_names(path: str, lines: list[str]) -> set[str]:
    """Return every name the file declares as its own, read by the syntax of its kind.

    The one model of this check: a name is *removed* when a changed or deleted file declared it before and no version of that file after the change
    does. It is *live* when any remaining tracked file declares it as its own (this function). Every other mention in a file that describes current
    behavior is a *reference* to the removed thing. A declaration is a key or ``$id`` of a parsed JSON, YAML or TOML document
    (``structured_declarations``), an argument, nested block or block label of Terraform code without its comments and heredocs (``hcl_declarations``),
    or a function, class, parameter, assignment or field of Python code without its strings and comments (``python_declarations``). A document that
    does not parse is read by line. A written value (a dict literal key, a call keyword, a string) and a quoted example declare nothing, and a path or
    a file name that holds the word is no reference to a key (``without_paths``).
    """
    suffix = Path(path).suffix
    if suffix in STRUCTURED_SUFFIXES:
        parsed = structured_declarations(path, "\n".join(lines))
        if parsed is not None:
            return parsed[0] | parsed[1]
        ids = {decoded_json_string(raw) for raw in SCHEMA_ID.findall("\n".join(lines))} if suffix == ".json" else set()
        return {key for line in lines if (key := config_key(line))} | ids
    if suffix in TERRAFORM_SUFFIXES:
        return hcl_declarations(lines)
    if suffix == ".py":
        return python_declarations(lines)
    return {key for line in lines if (key := config_key(line))} if suffix in CONFIG_SUFFIXES else set()


def live_names(files: dict[str, list[str]], names: set[str]) -> set[str]:
    """Return the ``names`` that some remaining file declares as its own (``declared_names``).

    Only a file that holds a name is read. A structured file that holds a backslash is read too: an escape can spell a name that its text does not hold.
    """
    live: set[str] = set()
    for path, lines in files.items():
        text = "\n".join(lines)
        if not any(name in text for name in names - live) and not ("\\" in text and Path(path).suffix in STRUCTURED_SUFFIXES):
            continue
        live |= declared_names(path, lines) & names
    return live


def decoded_json_string(raw: str) -> str:
    """Return the value of the JSON string whose characters between the quotes are ``raw``, or ``raw`` itself when it is no valid JSON string."""
    try:
        decoded = json.loads(f'"{raw}"')
    except ValueError:
        return raw
    return decoded if isinstance(decoded, str) else raw


def changed_paths() -> list[str]:
    """Return the paths the staged diff or, in CI, the pushed range modified or deleted, also when it only added lines to them."""
    refs = [os.environ["PRE_COMMIT_FROM_REF"], os.environ["PRE_COMMIT_TO_REF"]] if "PRE_COMMIT_FROM_REF" in os.environ else ["--cached"]
    return git("diff", *refs, "--name-only", "--diff-filter=MD").splitlines()


def before_texts(paths: list[str]) -> dict[str, list[str]]:
    """Return the lines before the change of each changed or deleted file whose syntax this check reads (structured, Terraform and Python files)."""
    old_ref = os.environ.get("PRE_COMMIT_FROM_REF", "HEAD")
    before: dict[str, list[str]] = {}
    for path in paths:
        if Path(path).suffix in STRUCTURED_SUFFIXES | {".tf", ".py"}:
            data = git_blob(f"{old_ref}:{path}")
            if (codec := text_codec(data)) is not None:
                before[path] = data.decode(codec).splitlines()
    return before


def removed_declarations(before: dict[str, list[str]], files: dict[str, list[str]], deleted: list[str]) -> tuple[set[str], set[str], set[str]]:
    """Return the keys, the ``$id`` values and the paths read, for what the change took away: declared in a file before, not by the same file after.

    The keys of a deleted file are left out: they are often data fields that code still writes. A workflow or action file is not read here, because the
    names of its inputs belong to the action that takes them (``input_line_numbers``). Nor is a file whose old or new text does not parse.
    """
    keys: set[str] = set()
    ids: set[str] = set()
    read: set[str] = set()
    for path, lines in before.items():
        if Path(path).suffix not in STRUCTURED_SUFFIXES or is_workflow_file(path):
            continue
        old = structured_declarations(path, "\n".join(lines))
        now = structured_declarations(path, "\n".join(files[path])) if path in files else (set(), set())
        if old is None or now is None:
            continue
        read.add(path)
        ids |= old[1] - now[1]
        keys |= old[0] - now[0] if path not in deleted else set()
    return keys, ids, read


def decoded_json_line(line: str) -> str:
    """Return a JSON line with each escaped string written out in plain characters, so a reference spelled with an escape is found."""

    def plain(match: re.Match[str]) -> str:
        value = match.group("value")
        return value if "\\" not in value else json.dumps(decoded_json_string(value[1:-1])).replace("\\n", " ")

    return JSON_STRING.sub(lambda match: plain(match) + (match.group("key") or ""), line)


def file_name_pattern(name: str) -> re.Pattern[str]:
    """Return the pattern for a whole file name or id: a longer name that begins or ends the same way, or carries a dotted suffix, is another one."""
    return re.compile(rf"(?<![\w.-]){re.escape(name)}(?![\w-]|\.\w)")


def lost_flags(before: dict[str, list[str]], files: dict[str, list[str]]) -> dict[str, set[str]]:
    """Return each flag that an edited script lost, with the scripts that lost it: defined in the file before and in no definition of the same file now.

    A deleted script loses none: its file name and its module already mark every command that runs it (``deleted_module_findings``).
    """
    lost: dict[str, set[str]] = {}
    for path, lines in before.items():
        kept = files.get(path, [])
        if path.endswith(".py") and path in files:
            for flag in set(FLAG.findall("\n".join(lines))) - set(FLAG.findall("\n".join(kept))):
                lost.setdefault(flag, set()).add(path)
    return lost


def flag_pattern(flag: str, scripts: set[str]) -> re.Pattern[str]:
    """Return the pattern of a line that holds ``flag`` and names one of ``scripts`` by file name, repository path or dotted module."""
    names = {re.escape(name) for path in scripts for name in (Path(path).name, path, path.removesuffix(".py").replace("/", "."))}
    return re.compile(rf"^(?=.*{re.escape(flag)}(?![\w-]))(?=.*(?<![\w./-])(?:{'|'.join(sorted(names))})(?![\w-]))")


def removed_definitions(
    removed: dict[str, list[str]], deleted: list[str], files: dict[str, list[str]], before: dict[str, list[str]]
) -> dict[str, dict[str, re.Pattern[str]]]:
    """Find the names the change removed, by kind, each with the pattern that marks a remaining reference.

    The model is ``declared_names``: a name counts as removed only when a changed or deleted file declared it before and no remaining tracked file declares
    it as its own, whatever the format (another config key or schema property, a Terraform argument or block, a Python function, class, parameter,
    assignment or field). The top-level names of a deleted Python module are not searched as plain words: a word it shared with unrelated code (a local
    variable, a YAML column) is no leftover. The module is checked by ``deleted_module_findings``. A deleted schema is checked by its file name, and every
    ``$id`` that a deleted or changed JSON file no longer declares is a name too, not the property names.

    A flag counts as removed only for an edited script that defined it before and no longer does (``lost_flags``), and only on a line that holds the flag and
    names that script (``flag_pattern``). A command split over several lines is not seen: the flag and the script name must share one line.
    """

    def gone(found: set[str], still: set[str]) -> set[str]:
        return {name for name in found - still if len(name) >= 3 and not (name.startswith("__") and name.endswith("__"))}

    def joined(lines_by_file: dict[str, list[str]], suffixes: set[str]) -> str:
        return "\n".join("\n".join(lines) for path, lines in lines_by_file.items() if Path(path).suffix in suffixes)

    kinds: dict[str, dict[str, re.Pattern[str]]] = {kind: {} for kind in REMOVED_KINDS}
    for path in deleted:
        if Path(path).suffix in SCRIPT_SUFFIXES:
            kinds["script"][Path(path).name] = file_name_pattern(Path(path).name)
        if path.endswith(".schema.json"):
            kinds["script"][Path(path).name] = file_name_pattern(Path(path).name)
        if path.endswith(".sql") and re.search(r"(^|/)(models|snapshots)/", path):
            stem = re.escape(Path(path).stem)
            kinds["dbt"][Path(path).stem] = re.compile(rf"""ref\(\s*["']{stem}["']\s*\)|name:\s*{stem}\b|(?<![\w.]){stem}(?![\w.])""")
    code_now, code_removed = joined(files, {".py"}), joined(removed, {".py"})
    for flag, scripts in lost_flags(before, files).items():
        kinds["flag"][flag] = flag_pattern(flag, scripts)
    env_now = set(ENV_READ.findall(code_now)) | {a or b for a, b in ENV_USE.findall(joined(files, {".sh", ".yml", ".yaml", ".json", ".tf", ".tpl", ".j2", ""}))}
    for name in gone(set(ENV_READ.findall(code_removed)), env_now):
        kinds["env"][name] = re.compile(rf"\b{name}\b")
    edited = {path: lines for path, lines in removed.items() if path not in deleted}
    python_left = gone(
        {name for path, lines in before.items() if path in files and path.endswith(".py") for name in python_top_level(lines) - python_top_level(files[path])},
        set(),
    )
    removed_keys, removed_ids, parsed = removed_declarations(before, files, deleted)
    # A changed file that does not parse on both sides is read by its removed lines.
    unparsed = {path: lines for path, lines in edited.items() if path not in parsed}
    config_removed = {key for key in removed_keys if re.search(r"[_-]|[a-z][A-Z]", key)}
    config_removed |= {key for line in joined(unparsed, CONFIG_SUFFIXES).splitlines() if (key := config_key(line)) and re.search(r"[_-]|[a-z][A-Z]", key)}
    config_left = gone(config_removed, set())
    live = live_names(files, python_left | config_left | removed_ids)
    kinds["script"] |= {schema_id: file_name_pattern(schema_id) for schema_id in removed_ids - live}
    for name in python_left - live:
        kinds["python"][name] = re.compile(rf"(?<![\w./-]){re.escape(name)}(?![\w/-]|\.(?:md|py|txt|json|ya?ml|toml|sh|sql|tf)\b)")
    for name in config_left - live:
        kinds["config"][name] = re.compile(rf"(?<![\w-]){re.escape(name)}(?![\w-])")
    old_blocks = {block for path, lines in before.items() if path.endswith(".tf") for block in hcl_blocks(lines)}
    new_blocks = {block for path, lines in files.items() if path.endswith(".tf") for block in hcl_blocks(lines)}
    for kind, first, second in old_blocks - new_blocks:
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
            fix = f"delete it, or add 'unused-code {match.group(1)} -- <who uses it> -- <expiry>' to {CLEANUP_ALLOWLIST}"
            findings.append(Finding(f"{match.group(1)}:{match.group(2)}", match.group(3), CLEANUP_RULE, fix))
    return findings


def load_runtimes(python_files: list[str], groups: list[str]) -> tuple[list[Runtime], list[Finding], dict[str, str], set[str]]:
    """Read the runtime map: each runtime's dependency groups, the code it runs and the packages it gets outside pip.

    The host runtime takes every tracked Python file no other runtime claims. Without a map, one host runtime reads every group, each as declared
    directly. The map also names the ``tools`` (requirements used as commands or imported dynamically, with why) and the ``direct_groups`` whose
    dependencies are declared directly in pyproject.toml; every other group is a lock, so its transitive packages count as declared and only its
    missing imports are reported. A malformed map, or a group pyproject.toml does not declare, stops the check.
    """
    from scripts.quality import lock_env

    if not Path(RUNTIME_MAP).is_file():
        return [Runtime("host", groups, python_files, {})], [], {}, set(groups)
    config = json.loads(Path(RUNTIME_MAP).read_text())
    if (
        set(config) - {"host", "runtimes", "tools", "direct_groups"}
        or not isinstance(config.get("runtimes", {}), dict)
        or "groups" not in config.get("host", {})
    ):
        raise SystemExit(f"{RUNTIME_MAP}: expected 'host' with 'groups', an optional 'runtimes' object, 'tools' and 'direct_groups'")
    runtimes, findings, claimed = [], [], set()
    for name, entry in config.get("runtimes", {}).items():
        if set(entry) - {"groups", "code", "provided", "reason"} or not entry.get("groups") or "code" not in entry:
            raise SystemExit(f"{RUNTIME_MAP}: runtime {name} needs 'groups' and 'code' and allows only 'provided' and 'reason'")
        files = []
        for pattern in entry["code"]:
            matched = [path for path in python_files if fnmatch.fnmatch(path, pattern)]
            if not matched:
                fix = f"correct or remove the pattern in {RUNTIME_MAP}"
                findings.append(Finding(RUNTIME_MAP, f"runtime {name}: {pattern} matches no tracked Python file", CLEANUP_RULE, fix))
            files += [path for path in matched if path not in files]
        claimed |= set(files)
        runtimes.append(Runtime(name, entry["groups"], files, entry.get("provided", {})))
    host = config["host"]
    if set(host) - {"groups", "provided"}:
        raise SystemExit(f"{RUNTIME_MAP}: host allows only 'groups' and 'provided'")
    runtimes.insert(0, Runtime("host", host["groups"], [path for path in python_files if path not in claimed], host.get("provided", {})))
    for runtime in runtimes:
        for group in runtime.groups:
            if group not in groups:
                raise SystemExit(f"{RUNTIME_MAP}: runtime {runtime.name} names group {group}, which pyproject.toml does not declare")
    return runtimes, findings, {lock_env.normalize(name): why for name, why in config.get("tools", {}).items()}, set(config.get("direct_groups", []))


def direct_names(group: str) -> list[str]:
    """The package names a group of pyproject.toml declares itself, with no transitive package."""
    from scripts.quality import lock_env

    specs = tomllib.loads(Path("pyproject.toml").read_text())["dependency-groups"][group]
    return sorted({lock_env.normalize(match.group(0)) for spec in specs if isinstance(spec, str) and (match := re.match(r"[A-Za-z0-9][A-Za-z0-9._-]*", spec))})


def group_names(group: str, direct: set[str]) -> list[str]:
    """The names deptry reads for one group: its own declarations when it is direct, else every package the lock pins for it."""
    from scripts.quality import lock_env

    return direct_names(group) if group in direct else sorted(lock_env.versions(lock_env.export(group, Path.cwd())))


def write_listings(folder: Path, groups: list[str], direct: set[str]) -> dict[str, str]:
    """Write one package-name file per group for deptry; return each file's path with the source it reports (pyproject.toml or uv.lock)."""
    written: dict[str, str] = {}
    for group in groups:
        listing = folder / f"requirements-{group}.txt"
        listing.write_text("\n".join(group_names(group, direct)) + "\n")
        written[listing.resolve().as_posix()] = "pyproject.toml" if group in direct else "uv.lock"
    return written


def run_deptry(runtime: Runtime, first_party: list[str], exempt: set[str], direct: set[str]) -> list[dict[str, Any]]:
    """Run deptry on one runtime's files only, copied into a scratch folder, against the packages of that runtime's groups."""
    with tempfile.TemporaryDirectory(prefix="deptry_") as scratch:
        root = Path(scratch) / "code"
        for path in runtime.files:
            (root / path).parent.mkdir(parents=True, exist_ok=True)
            (root / path).write_bytes(Path(path).read_bytes())
        report_path = Path(scratch) / "deptry.json"
        names = write_listings(Path(scratch), runtime.groups, direct)
        args = [str(root), "--requirements-files", ",".join(names), "--json-output", str(report_path), "--no-ansi"]
        args += [flag for name in first_party for flag in ("--known-first-party", name)]
        ignores = [f"DEP002={'|'.join(sorted(exempt))}"] if exempt else []
        if runtime.provided:
            provided = "|".join(sorted(runtime.provided))
            ignores += [f"DEP001={provided}", f"DEP003={provided}"]
        args += ["--per-rule-ignores", ",".join(ignores)] if ignores else []
        result = run_command(str(Path(".venv/bin/deptry").resolve()), args, cwd=root, timeout=300)
        if not report_path.exists():
            raise SystemExit(f"deptry failed for runtime {runtime.name} ({result.returncode}): {result.stderr[-2000:]}")
        issues: list[dict[str, Any]] = json.loads(report_path.read_text())
        for issue in issues:  # Report repository paths: the dependency lists by their source, code relative to the scratch copy.
            location = Path(issue["location"]["file"])
            absolute = location if location.is_absolute() else root / location
            if str(absolute.resolve()) in names:
                issue["location"]["file"] = names[str(absolute.resolve())]
            elif absolute.resolve().is_relative_to(root.resolve()):
                issue["location"]["file"] = absolute.resolve().relative_to(root.resolve()).as_posix()
    return issues


def unused_dependencies() -> list[Finding]:
    """Every declared dependency is imported and every imported package is declared by the runtime that runs it (deptry).

    Each runtime in the runtime map is checked on its own code with the dependency groups it names. A requirement used as
    a command or imported dynamically is listed under ``tools`` in the map. A lock group pins transitive packages too, so only its
    missing imports are reported. A direct group two runtimes share reports a package as unused only when every one of them leaves it unused.
    """
    from scripts.quality import lock_env

    globs, findings = allowed_paths("unused-dependencies")
    python_files = [p for p in git("ls-files", "*.py").splitlines() if not re.match(r"(e2e|data)/", p) and Path(p).is_file()]
    groups = lock_env.groups(Path.cwd()) if Path("pyproject.toml").is_file() else []
    if not groups:
        return findings
    runtimes, map_findings, tools, direct = load_runtimes(python_files, groups)
    findings += map_findings
    exempt = set(tools)
    # Module names, package folders and top-level folders (scripts is a namespace package with no __init__.py).
    first_party = sorted({Path(p).stem for p in python_files} | {part for p in python_files for part in Path(p).parts[:-1]})
    unused: dict[tuple[str, str], int] = {}
    readers = sum(1 for runtime in runtimes if runtime.files and direct & set(runtime.groups))
    for runtime in runtimes:
        if not runtime.files:
            continue  # A tool environment with no project code, such as the SQLFluff tools.
        for issue in run_deptry(runtime, first_party, exempt, direct):
            code, module, location = issue["error"]["code"], issue["module"], issue["location"]
            if code not in DEPTRY_CODES or any(fnmatch.fnmatch(location["file"], glob) for glob in globs):
                continue
            if code == "DEP002":
                if location["file"] == "pyproject.toml":
                    unused[(location["file"], module)] = unused.get((location["file"], module), 0) + 1
                continue
            group = runtime.groups[-1]
            fix = f"declare it in the {group} group of pyproject.toml and lock it, list it under provided in {RUNTIME_MAP}, or replace the import"
            problem = f"{module} is imported but runtime {runtime.name} does not declare it"
            findings.append(Finding(f"{location['file']}:{location['line']}", problem, f"{POLICY}: declared dependencies", fix))
    for (path, module), count in sorted(unused.items()):
        if count == readers:
            fix = f"remove it, or list it under tools in {RUNTIME_MAP} when it is not imported directly"
            findings.append(Finding(path, f"{module} is declared but nothing imports it", CLEANUP_RULE, fix))
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
            patterns.append(rf"(?<![\w-])(?:import|from|-m)\s+(?:[\w.]+\.)?{re.escape(Path(path).stem)}\b")  # \b cannot precede -m
        pattern = re.compile("|".join(patterns))
        # Every alternative of the pattern holds one of these strings as written (the path, the name, the last two folders, the module name), so a text
        # that holds none of them cannot match and is not searched.
        literals = [path, name, *(["/".join(parts[-2:])] if len(parts) >= 2 else []), *([Path(path).stem] if path.endswith(".py") else [])]
        if not any(other != path and any(literal in text for literal in literals) and pattern.search(text) for other, text in texts.items()):
            fix = f"delete it, link it from the file that uses it, or add 'orphan {path} -- <why it stays> -- <expiry>' to {CLEANUP_ALLOWLIST}"
            findings.append(Finding(path, "no other tracked file references it", CLEANUP_RULE, fix))
    return findings


DOCS_RULE = f"{POLICY}: a document reference names a tracked file"
# A docs/ path in tracked text, also written ./docs/: not preceded by a path character, so a URL or an absolute path is not read, and not followed by a glob
# or an f-string brace.
DOCS_PATH = re.compile(r"(?<![\w./~$-])(?:\./)?docs/([\w.-]+(?:/[\w.-]+)*)")


def docs_references() -> list[Finding]:
    """No tracked text file names a docs/ path that is not tracked (a deleted or never committed document)."""
    globs, findings = allowed_paths("docs-path")
    tracked = git("ls-files").splitlines()
    known = {*tracked, *(parent.as_posix() for path in tracked for parent in Path(path).parents)}
    for path in tracked:
        if any(fnmatch.fnmatch(path, glob) for glob in globs):
            continue
        try:
            text = Path(path).read_text()
        except (UnicodeDecodeError, FileNotFoundError, IsADirectoryError):
            continue
        for number, line in enumerate(text.splitlines(), 1):
            for match in DOCS_PATH.finditer(line):
                # Only a sentence's closing full stop is dropped; any other trailing character is part of the name it checks.
                target = "docs/" + match.group(1).rstrip(".")
                if target not in known:
                    fix = f"point it at a tracked file, remove the reference, or add 'docs-path {path} -- <why it stays>' to {CLEANUP_ALLOWLIST}"
                    findings.append(Finding(f"{path}:{number}", f"{target} is not a tracked file", DOCS_RULE, fix))
    return findings


def temporary_list() -> tuple[list[str], list[Finding]]:
    """The paths in the list of temporary files; an unreadable or malformed list is a finding."""
    try:
        entries = json.loads(Path(TEMPORARY_LIST).read_text(encoding="utf-8"))["files"]
        return [entry["path"] for entry in entries], []
    except (OSError, ValueError, KeyError, TypeError):
        fix = 'restore it: {"files": [{"path": "<file>", "why": "<reason>"}]}'
        return [], [Finding(TEMPORARY_LIST, "the list of temporary files cannot be read", TEMPORARY_RULE, fix)]


def imported_modules(path: str, tree: ast.AST) -> list[tuple[int, str]]:
    """The (line, dotted module) pairs a Python file imports, relative imports resolved from the file's package."""
    package = path.removesuffix(".py").split("/")[:-1]
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found += [(node.lineno, alias.name) for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            base = package[: len(package) - node.level + 1] if node.level else []
            module = ".".join([*base, *([node.module] if node.module else [])])
            found += [(node.lineno, module)] + [(node.lineno, f"{module}.{alias.name}") for alias in node.names]
    return found


def string_values(tree: ast.AST) -> list[tuple[int, str]]:
    """The (line, text) pairs of every string in a Python file except the docstrings, which only describe."""
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
    }
    return [
        (node.lineno, node.value) for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings
    ]


def temporary_uses(path: str, text: str, listed: list[str]) -> list[tuple[int, str]]:
    """The (line, listed file) pairs a permanent Python or shell file uses: an import, or a string or shell line that names the file."""
    modules = {item.removesuffix(".py").replace("/", "."): item for item in listed if item.endswith(".py")}
    named = {item: re.compile(rf"(?<![\w./-]){re.escape(item)}(?![\w-])|(?<![\w./-]){re.escape(Path(item).name)}(?![\w-])") for item in listed}
    dotted = {item: re.compile(rf"(?<![\w.]){re.escape(module)}(?![\w])") for module, item in modules.items()}
    found: list[tuple[int, str]] = []
    if path.endswith(".py"):
        try:
            tree = ast.parse(text)
        except SyntaxError:
            return []
        found += [(line, modules[module]) for line, module in imported_modules(path, tree) if module in modules]
        for line, value in string_values(tree):
            found += [(line, item) for item, pattern in named.items() if pattern.search(value)]
            found += [(line, item) for item, pattern in dotted.items() if pattern.search(value)]
    else:
        for number, text_line in enumerate(text.splitlines(), 1):
            found += [(number, item) for item, pattern in named.items() if pattern.search(text_line)]
    return sorted(set(found))


def temporary_code() -> list[Finding]:
    """Permanent Python and shell files never import, call or read a file in the list of temporary files, and every listed file is tracked."""
    listed, findings = temporary_list()
    tracked = git("ls-files").splitlines()
    for item in listed:
        if item not in tracked:
            findings.append(Finding(TEMPORARY_LIST, f"temporary file {item} is listed but not tracked", TEMPORARY_RULE, f"remove {item} from the list"))
    for path in tracked:
        if path in listed or not path.endswith((".py", ".sh")):
            continue
        try:
            text = Path(path).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for line, item in temporary_uses(path, text, listed):
            fix = f"move what {path} needs from {item} into a permanent module that {item} imports (temporary code may use permanent code, not the reverse)"
            findings.append(Finding(f"{path}:{line}", f"permanent code uses the temporary file {item}", TEMPORARY_RULE, fix))
    return findings


def terraform_unused() -> list[Finding]:
    """No Terraform variable, local, data source or module output that nothing uses (tflint, every module)."""
    globs, findings = allowed_paths("terraform-unused")
    # One run per folder that holds a tracked .tf file: --recursive would walk every ignored folder under data/.
    folders = sorted({Path(path).parent.as_posix() for path in git("ls-files", "*.tf").splitlines()})
    root = Path.cwd().resolve()
    for folder in folders:
        args = ["--chdir", folder, "--only", "terraform_unused_declarations", "--format", "json", "--force"]
        result = run_command(str(Path(".tools/bin/tflint").resolve()), args, timeout=120)
        report = json.loads(result.stdout or "{}")
        if result.returncode or report.get("errors"):
            raise SystemExit(f"tflint failed in {folder} ({result.returncode}): {report.get('errors') or result.stderr[-2000:]}")
        for issue in report.get("issues", []):
            where = issue["range"]
            # tflint reports paths from the starting folder, not from the --chdir folder.
            path = (root / where["filename"]).resolve().relative_to(root).as_posix()
            if not any(fnmatch.fnmatch(path, glob) for glob in globs):
                fix = f"delete the declaration, or add 'terraform-unused {path} -- <who uses it> -- <expiry>' to {CLEANUP_ALLOWLIST}"
                findings.append(Finding(f"{path}:{where['start']['line']}", issue["message"], CLEANUP_RULE, fix))
    return findings


def mypy_uncovered_files() -> set[str]:
    """Return the Python files MyPy does not cover: the ``pending`` files of config/quality/mypy_runtimes.json and the files that import them."""
    from scripts.quality import typecheck  # Loaded only here: the other runners that copy this check do not copy MyPy's selector.

    def python_source(path: str) -> bytes | None:
        # Bytes, not text: ast.parse then reads the file as the interpreter does (UTF-8 or the codec the file declares).
        data = (typecheck.ROOT / path).read_bytes()
        return None if text_codec(data) is None else data

    config = Path("config/quality/mypy_runtimes.json")
    pending = json.loads(config.read_text())["pending"] if config.is_file() else {}
    try:
        return set(typecheck.pending_by_import(typecheck.source_files(), pending, python_source))
    except SyntaxError as error:
        raise SystemExit(f"removed-names: cannot read the import graph of the pending files: {error}") from error


def deleted_modules_left(removed: dict[str, list[str]], deleted: list[str], files: dict[str, list[str]], allowed: list[tuple[str, str]]) -> list[Finding]:
    """Find what still refers to a Python module the change deleted (see ``deleted_module_findings``), below the import roots pyproject.toml declares."""
    from scripts.quality import typecheck_importers

    gone = [path for path in deleted if path.endswith(".py")]
    if not gone:
        return []
    try:
        roots = typecheck_importers.declared_roots(Path.cwd(), os.environ.get("PRE_COMMIT_FROM_REF", "HEAD"))
    except ValueError as error:
        raise SystemExit(f"removed-names: cannot read the import roots, so a deleted module cannot be matched by its module path: {error}") from error
    dead = {typecheck_importers.module_of(path, roots): dead_prefixes(path, PY_TOP_LEVEL.findall("\n".join(removed.get(path, []))), roots) for path in gone}
    return deleted_module_findings({module: prefixes for module, prefixes in dead.items() if prefixes}, files, roots, allowed)


DEFERRAL_FILE = "dbt/dbt_project.yml"
DEFERRAL_KEY = re.compile(r"^(\s*)deferred_models:")


def without_deferral_record(lines: list[str]) -> list[str]:
    """The project file's lines with the deferral record (the ``deferred_models`` key and everything indented under it) blanked, so the record can name the
    deferred models while a reference anywhere else in the file still counts. Line numbers stay."""
    kept, indent = [], None
    for line in lines:
        found = DEFERRAL_KEY.match(line)
        if indent is None and found:
            indent = len(found.group(1))
            kept.append("")
        elif indent is not None and (not line.strip() or len(line) - len(line.lstrip()) > indent):
            kept.append("")
        else:
            indent = None
            kept.append(line)
    return kept


def removed_names() -> list[Finding]:
    """A name the change removed is no longer referenced in any file that describes current behavior.

    Kinds: scripts, command-line flags, environment variables, config keys, Terraform declarations, dbt models and
    dependencies. Code names match code and config files and only the code spans and fences of Markdown, so
    a removed ``main`` does not flag "the main branch" in prose. Python functions, classes and modules
    are not read as text: MyPy checks every file that imports a changed module (scripts/quality/typecheck_importers.py).
    Only the Python files MyPy does not cover keep a text check for them, until they join MyPy. The names of a deleted
    module are not part of it: only a reference to the module counts (see ``deleted_module_findings``), in any file. The known limits of that
    reading are listed in ``python_module_uses``. Unreadable import-root declarations stop the check when a Python file is deleted.
    """
    allowed, findings = read_allowlist(CLEANUP_ALLOWLIST, REMOVED_KINDS + UNUSED_KINDS, CLEANUP_RULE, "why the reference stays")
    removed, deleted = changed_lines_and_deletions()
    files = reference_files(record_skips=True)
    before = before_texts(changed_paths())
    kinds = removed_definitions(removed, deleted, files, before)
    owners = lost_flags(before, files)
    unchecked = mypy_uncovered_files() if kinds["python"] else set()
    findings += deleted_modules_left(removed, deleted, files, allowed)
    for kind, names in kinds.items():
        # The text searched in a file depends on the kind and the file, not on the name: read it once for all the names of the kind.
        views: dict[str, list[str] | None] = {}
        blobs: dict[str, str] = {}
        for name, pattern in sorted(names.items()):
            for path, lines in files.items():
                if kind == "flag" and path in owners[name]:
                    continue
                if path not in views:
                    view = search_view(kind, path, lines) if kind != "python" or path in unchecked else None
                    if view is not None and kind == "config":
                        view = python_literals(view, without_paths) if path.endswith(".py") else [without_paths(line) for line in view]
                    if view is not None and kind in ("dbt", "config") and path == DEFERRAL_FILE:
                        view = without_deferral_record(view)
                    if view is not None and any(entry_kind == kind and fnmatch.fnmatch(path, glob) for entry_kind, glob in allowed):
                        view = None
                    views[path] = view
                    blobs[path] = "\n".join(view) if view is not None else ""
                view = views[path]
                # Every pattern of a kind but dependency holds the removed name as written, so a file whose text lacks the name has no reference to it.
                if view is None or (kind != "dependency" and name not in blobs[path]):
                    continue
                for number, line in enumerate(view, 1):
                    if pattern.search(line):
                        fix = f"remove or update the reference, or add '{kind} {path} -- <why the reference stays> -- <expiry>' to {CLEANUP_ALLOWLIST}"
                        findings.append(Finding(f"{path}:{number}", f"{name} was removed but is still referenced", CLEANUP_RULE, fix))
    return findings


def json_task_ids(node: Any) -> list[tuple[str | None, str]]:
    """Every object at any depth with a string ``task_id``, in document order, as its ``dag_id`` (None when it has none or not a string) and task."""
    if isinstance(node, dict):
        own = [(node["dag_id"] if isinstance(node.get("dag_id"), str) else None, node["task_id"])] if isinstance(node.get("task_id"), str) else []
        return own + [pair for value in node.values() for pair in json_task_ids(value)]
    return [pair for value in node for pair in json_task_ids(value)] if isinstance(node, list) else []


def python_task_ids(tree: ast.AST, dag_at: int, task_at: int) -> list[tuple[int, str | None, str]]:
    """The line, DAG and task of every dict, keyword call and positional ``SubmissionRequest`` that holds both IDs as string literals."""

    def text(node: ast.AST) -> str | None:
        return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None

    found: list[tuple[int, str | None, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            named = {text(key): text(value) for key, value in zip(node.keys, node.values, strict=True) if key is not None}
        elif isinstance(node, ast.Call):
            named = {keyword.arg: text(keyword.value) for keyword in node.keywords}
            callee = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else ""
            if callee == "SubmissionRequest" and len(node.args) > max(dag_at, task_at):
                named = {"dag_id": text(node.args[dag_at]), "task_id": text(node.args[task_at])}
        else:
            continue
        if named.get("dag_id") and named.get("task_id"):
            found.append((node.lineno, str(named["dag_id"]), str(named["task_id"])))
    return found


def task_ids() -> list[Finding]:
    """A task ID written beside the ID of a shipped DAG names a task that DAG has.

    Reads the current tree: every tracked JSON object and every Python dict, call with keyword arguments or ``SubmissionRequest`` call (by its field order) that
    holds a ``dag_id`` and a ``task_id`` as string literals. A DAG that ``orchestration.graph`` does not ship is a made-up fixture DAG and is not judged, nor is
    a value that is not a literal. A file that does not parse is skipped: Ruff and the JSON check already block it, and a fixture may hold bad JSON on purpose.
    """
    from orchestration import graph, interface

    names = [field.name for field in fields(interface.SubmissionRequest)]
    dag_at, task_at = names.index("dag_id"), names.index("task_id")
    tasks = {dag: sorted(graph.reference_graph(dag)) for dag in graph.GRAPH_DAGS}
    findings = []
    for path in sorted(git("ls-files", "*.json", "*.py").splitlines()):
        try:
            text = Path(path).read_text(encoding="utf-8")
            if path.endswith(".json"):
                pairs = json_task_ids(json.loads(text))
                keyed = [number for number, line in enumerate(text.splitlines(), 1) if TASK_KEY.search(line)]
                found = [(keyed[index] if len(keyed) == len(pairs) else 1, dag, task) for index, (dag, task) in enumerate(pairs)]
            else:
                found = python_task_ids(ast.parse(text), dag_at, task_at)
        except (OSError, SyntaxError, ValueError):
            continue
        for line, dag, task in found:
            if dag in tasks and task not in tasks[dag]:
                fix = "name a task the DAG has, or use a DAG name that is not shipped if the fixture is not about a real DAG"
                findings.append(Finding(f"{path}:{line}", f"task '{task}' is not a task of {dag}. Its tasks are {', '.join(tasks[dag])}", TASK_IDS_RULE, fix))
    return findings


def missing_command_modules() -> list[Finding]:
    """A ``python -m`` command of a module of this repository names a module that exists: ``<path>.py`` or ``<path>/__main__.py``.

    Reads the current tree: the files that describe current behavior (``reference_files``) outside data/. A module is this repository's when its first name part
    is a top-level name of tracked Python code, below the project root or an import root that pyproject.toml declares. The module of another package is not
    judged, neither is a flag of the command nor a name that ends in an ellipsis (a prefix in prose). A command is read from the line and, outside Python
    files, from each quoted string of it (a Terraform or JSON value).
    """
    from scripts.quality import typecheck_importers

    try:
        roots = typecheck_importers.declared_roots(Path.cwd(), "HEAD")
    except ValueError as error:
        raise SystemExit(f"command-modules: cannot read the import roots, so a module cannot be matched by its path: {error}") from error
    tracked = set(git("ls-files").splitlines())
    first_party = {typecheck_importers.module_of(path, roots).split(".")[0] for path in tracked if path.endswith(".py")}
    folders = ["", *[f"{folder}/" for folder in roots]]
    findings = []
    for path, lines in sorted(reference_files().items()):
        if path.startswith("data/"):
            continue
        for number, line in enumerate(lines, 1):
            quoted = [] if path.endswith(".py") else [first or second for first, second in COMMAND_STRING.findall(line)]
            for module in dict.fromkeys(m for text in (line, *quoted) for m in command_modules(text)):
                base = module.replace(".", "/")
                if (
                    module.split(".")[0] in first_party
                    and f"{module}..." not in line
                    and not any(f"{folder}{base}{tail}" in tracked for folder in folders for tail in (".py", "/__main__.py"))
                ):
                    problem = f"python -m {module} names a module that does not exist: there is no {base}.py or {base}/__main__.py"
                    findings.append(Finding(f"{path}:{number}", problem, COMMAND_MODULES_RULE, "name a module that exists, or update the command"))
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
                    "remove the AI credit or session line. Keep human co-authors and factual descriptions of the change",
                )
            )
    return findings


def plan_reference_findings(location: str, message: str, *, comments: bool = False) -> list[Finding]:
    """Check each message line for internal plan references; Git's comment lines are not part of the message."""
    findings = []
    for number, line in enumerate(message.splitlines(), 1):
        if comments and line.startswith("#"):
            continue
        for form, pattern in PLAN_REFERENCES:
            if pattern.search(line):
                findings.append(
                    Finding(
                        f"{location}:{number}",
                        f"internal plan reference in commit message: {form}",
                        f"{POLICY}: commit messages",
                        "say what changed in plain words. Keep unit and lane codes, review rounds, patch hashes and snapshot IDs in the plan records",
                    )
                )
    return findings


def message_findings(location: str, message: str, *, comments: bool = False) -> list[Finding]:
    """Apply the subject, attribution and plan-reference policies without printing message bodies into check logs."""
    subject = next((line for line in message.splitlines() if not (comments and line.startswith("#"))), "")
    return commit_subjects([(location, subject)]) + attribution_findings(location, message) + plan_reference_findings(location, message, comments=comments)


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


def privacy_files() -> list[str]:
    """Every file Git does not ignore, tracked and untracked, hidden files included: the files that can reach GitHub."""
    return sorted(set(git("ls-files", "--cached", "--others", "--exclude-standard").splitlines()))


# An entry reads "<type> <key> -- <reason> -- <expiry>". The expiry field matches this exactly, with nothing before or after it.
EXPIRY_FIELD = re.compile(r"permanent|expires [0-9]{4}-[0-9]{2}-[0-9]{2}|expires-when path-absent:\S+")
PRINTABLE_ASCII = re.compile(r"[ -~]*")  # Space (0x20) through tilde (0x7E): a tab, a carriage return and any non-ASCII character are outside it.
ENTRY_SHAPE = "allowlist entry must read '<type> <key> -- <reason> -- <expiry>'"
EXPIRY_FIX = "end the entry with ' -- permanent', ' -- expires YYYY-MM-DD' or ' -- expires-when path-absent:<glob>', after the reason"
CLOCK: list[datetime.date] = []  # Holds the date --today sets; empty means the real UTC date.
PERMANENT: dict[str, dict[str, int]] = {}  # Allowlist file -> entry type -> permanent entries read in this run.


def set_today(day: str) -> None:
    """Fix the date that allowlist expiry is judged against (--today), so a test can pin it."""
    CLOCK[:] = [datetime.date.fromisoformat(day)]


def allowlist_today() -> datetime.date:
    """The UTC date an allowlist entry's expiry is judged against."""
    return CLOCK[0] if CLOCK else datetime.datetime.now(datetime.UTC).date()


@functools.cache
def tracked_paths() -> list[str]:
    """The files in the Git index; an expiry event reads this, so a deleted file is gone as soon as the deletion is staged."""
    return [path for path in git("ls-files", "-z").split("\0") if path]


def names_expiry(reason: str) -> bool:
    """Whether the reason names an expiry, in any case or width: NFKC and case folding bring lookalike forms of the words together."""
    folded = unicodedata.normalize("NFKC", unicodedata.normalize("NFKC", reason).casefold())
    return "expires" in folded or "permanent" in folded


def expiry_problems(field: str, today: datetime.date, tracked: Callable[[], list[str]]) -> list[str]:
    """Return what is wrong with an entry's expiry field: malformed, an impossible date, or already expired. The field is not trimmed."""
    if not EXPIRY_FIELD.fullmatch(field):
        return ["allowlist entry with a malformed expiry field"]
    if field.startswith("expires-when "):
        glob = field.partition(":")[2]
        return [] if any(fnmatch.fnmatchcase(path, glob) for path in tracked()) else [f"allowlist entry expired: no tracked file matches {glob}"]
    if field.startswith("expires "):
        try:
            day = datetime.date.fromisoformat(field.removeprefix("expires "))
        except ValueError:
            return ["allowlist entry with an invalid expiry date"]
        return [f"allowlist entry expired on {day.isoformat()}"] if day < today else []
    return []


def read_allowlist(name: str, kinds: list[str], rule: str, reason: str) -> tuple[list[tuple[str, str]], list[Finding]]:
    """Read ``type key -- reason -- expiry`` entries; an entry that breaks that form is itself a finding.

    Every line holds only printable ASCII (space through tilde); a line with any other character, such as a tab, a carriage
    return, a byte order mark or an invisible character, is a finding and is read no further. The line is split on exactly two
    `` -- `` separators, with no trimming and no bracket handling. The expiry field is exactly
    ``permanent`` (counted and reported by the check that read the list, when the entry has no finding), ``expires YYYY-MM-DD``
    (valid through that UTC date) or ``expires-when path-absent:<glob>`` (expired once no tracked file matches). The reason may not
    name an expiry. An entry with a finding stays in the result, so its own finding is the only one it raises.
    The expiry field is the only expiry the check reads; the reason is free text and is not interpreted. The ban on the literal words is a cheap
    guard, not a reading of the reason; a reason worded to mislead is left to review.
    """
    entries: list[tuple[str, str]] = []
    findings: list[Finding] = []
    PERMANENT.pop(name, None)
    if not Path(name).exists():
        return entries, findings
    today = allowlist_today()
    # Bytes, not text mode: text mode would turn a carriage return into a line end before the check sees it.
    for number, line in enumerate(Path(name).read_bytes().decode("utf-8", errors="replace").split("\n"), 1):
        location = f"{name}:{number}"
        if not PRINTABLE_ASCII.fullmatch(line):
            fix = "use only space through tilde; a tab, a carriage return (Windows line ends), a byte order mark and invisible characters are refused"
            findings.append(Finding(location, "allowlist lines may contain only printable ASCII", rule, fix))
            continue
        if not line.strip() or line.startswith("#"):
            continue
        parts = line.split(" -- ")
        head = re.fullmatch(r"(\S+) (\S+)", parts[0])
        if not head:
            findings.append(Finding(location, ENTRY_SHAPE, rule, "write the type and the key with one space between them"))
            continue
        if head.group(1) not in kinds:
            findings.append(Finding(location, "allowlist entry with an unknown type", rule, f"use one of: {', '.join(kinds)}"))
            continue
        if len(parts) == 1 or not parts[1].strip():
            findings.append(Finding(location, "allowlist entry without a reason", rule, f"add ' -- <{reason}> -- <expiry>'"))
            continue
        entries.append((head.group(1), head.group(2)))
        problems = []
        if names_expiry(parts[1]):
            problems.append("allowlist entry: the reason may not name an expiry; put it in the expiry field")
        if len(parts) == 2:
            problems.append("allowlist entry without an expiry")
        elif len(parts) > 3:
            problems.append("allowlist entry with more than two ' -- ' separators")
        else:
            problems.extend(expiry_problems(parts[2], today, tracked_paths))
        if len(parts) == 3 and parts[2] == "permanent" and not problems:
            counts = PERMANENT.setdefault(name, {})
            counts[head.group(1)] = counts.get(head.group(1), 0) + 1
        findings.extend(Finding(location, problem, rule, EXPIRY_FIX) for problem in problems)
    return entries, findings


def permanent_summary() -> str:
    """One line naming each allowlist read in this run that holds permanent entries, with the count per entry type."""
    parts = [
        f"{name} {sum(counts.values())} ({', '.join(f'{kind} {count}' for kind, count in sorted(counts.items()))})"
        for name, counts in sorted(PERMANENT.items())
    ]
    return f"permanent allowlist entries: {'; '.join(parts)}\n" if parts else ""


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
            if "ACCOUNT" in key.strip().upper() and value == SYNTHETIC_ACCOUNT_ID:
                continue
            values.append(value)
    return values


def reserved_domain(domain: str) -> bool:
    """Whether an email domain is reserved: below a reserved top-level domain, or equal to a reserved name or a subdomain of one."""
    return domain.endswith(RESERVED_TOP_LEVEL_DOMAINS) or any(domain == name or domain.endswith("." + name) for name in RESERVED_DOMAINS)


def exempt_match(kind: str, match: re.Match[str]) -> bool:
    """Whether a privacy match is a reserved email domain or the one synthetic account ID. No home path is exempt."""
    if kind == "email":
        return reserved_domain(match.group(1).lower())
    if kind == "aws-account":
        return (match.group(1) or match.group(2)) == SYNTHETIC_ACCOUNT_ID
    return False


def other_twelve_digits(line: str) -> bool:
    """Whether a line holds a 12-digit number other than the synthetic account ID, which an account match must not hide.

    The last group of a UUID can be 12 digits (a resource ID in an ARN), so UUIDs are not counted.
    """
    return any(number != SYNTHETIC_ACCOUNT_ID for number in TWELVE_DIGITS.findall(UUID.sub("", line)))


def unescape(text: str) -> str:
    """Decode the backslash forms of a character (hex, Unicode, octal), an escaped slash or quote, and a pair of backslashes."""

    def one(match: re.Match[str]) -> str:
        if match.group(5):
            return match.group(5)
        code = int(match.group(1) or match.group(2) or match.group(3) or match.group(4), 8 if match.group(4) else 16)
        return chr(code) if code <= sys.maxunicode else match.group(0)

    return ESCAPE.sub(one, text)


def decoded_views(line: str) -> set[str]:
    """The line and what it reads as after percent and backslash decoding, in every order up to three rounds."""
    views = {line}
    pending = [line]
    for _ in range(3):
        pending = [view for text in pending for view in (urllib.parse.unquote(text), unescape(text)) if view not in views]
        views.update(pending)
    return views


def lex_words(text: str) -> list[tuple[str, list[int], str]]:
    """Split text into shell words: the value of each, with quotes and escapes resolved, the lexical segment of each character and the character before it.

    A quote (double, single or backtick) holds any character. A backslash escapes a quote, a space or a backslash outside quotes, and a quote or a backslash
    inside double quotes. Any other backslash stays, so a Windows path keeps its separators. A character that a quote or an escape puts right below a
    home root has another segment than the root. The character before a word stays with it, since a route such as (api)/users/login starts no path.
    """
    words = []
    chars: list[str] = []
    segments: list[int] = []
    lead = ""
    segment = 0
    quote = ""
    started = False
    index = 0
    while index < len(text):
        char = text[index]
        following = text[index + 1 : index + 2]
        if quote:
            if char == quote:
                quote = ""
                segment += 1
            else:
                if char == "\\" and quote == '"' and following and following in '"\\`$':
                    index += 1
                    char = following
                lead = text[index - 1 : index] if not chars else lead
                chars.append(char)
                segments.append(segment)
        elif char in SHELL_BREAKS:
            if started:
                words.append(("".join(chars), segments, lead))
                chars, segments, started = [], [], False
            segment += 1
        else:
            started = True
            if char in HOME_QUOTES:
                quote = char
                segment += 1
            elif char == "\\" and following and following in "\"'` \\":
                index += 1
                segment += 1
                lead = text[index - 1 : index] if not chars else lead
                chars.append(following)
                segments.append(segment)
                segment += 1
            else:
                lead = text[index - 1 : index] if not chars else lead
                chars.append(char)
                segments.append(segment)
        index += 1
    if started:
        words.append(("".join(chars), segments, lead))
    return words


def name_found(rest: str, root_segment: int, segments: list[int] | None, base: int) -> bool:
    """Whether the folder at the start of ``rest``, the path below a home root, is a concrete name and not a placeholder.

    Quotes that stay in the text are literal characters of a quoted reading (an escaped quote), so they are skipped. A folder that starts with an escape is
    a name unless the whole folder is a placeholder. A space is a name character only when a quote or an escape put it right below the root.
    """
    start = 0
    while start < len(rest) and rest[start] in HOME_QUOTES:
        start += 1
    if start == len(rest):
        return False
    first = rest[start]
    if first in HOME_SPACES:
        stop = start
        while stop < len(rest) and rest[stop] in " \t":
            stop += 1
        return segments is not None and stop < len(rest) and rest[stop] not in HOME_DELIMITERS and segments[base + start] != root_segment
    if first == "%":
        folder = ENCODED_FOLDER.match(rest, start)
        return bool(folder and ENCODED_PART.search(folder.group()) and not PLACEHOLDER_PART.fullmatch(folder.group()))
    return first not in HOME_DELIMITERS


def home_root_found(kind: str, root: str, rest: str, segments: list[int] | None, base: int, tail: Callable[[int], str], climbs: bool) -> bool:
    """Apply the one rule to the path below a home root.

    ``rest`` starts the path below the root, and its first folder is what counts. ``tail(offset)`` is the path from that offset in the whole token, with the
    folders that cancel each other removed, and ``climbs`` says whether the token climbs out of a folder at all. The path is a finding when its first
    folder is a concrete name, as written or after the path is normalized. The macOS Shared folder is no home, but only while the normalized path stays in it.
    """
    folder = rest.split("/", 1)[0]
    if kind == "mac" and folder.casefold() == "shared":
        resolved = posixpath.normpath("/Users/Shared/" + tail(len(folder) + 1)).casefold()
        return resolved != "/users/shared" and not resolved.startswith("/users/shared/")
    if name_found(rest, segments[base - 1] if segments else 0, segments, base):
        return True
    if not climbs:
        return False
    prefix = {"linux": "/home/", "mac": "/Users/", "windows": root[0] + ":\\Users\\"}[kind]
    normal = (ntpath if kind == "windows" else posixpath).normpath(prefix + tail(0))
    return normal.casefold().startswith(prefix.casefold()) and name_found(normal[len(prefix) :], 0, None, 0)


def folder_stand_in(folder: str) -> str:
    """A folder that the path rules read as the folder is read: a short one as written, a long one as a bounded name or a bounded non-name.

    The rules compare a folder only with the names root, Users, home and Shared, and read the first character, the quotes before it and the percent
    escapes of the rest as name_found does, so a long folder cannot equal a name and one answer from name_found stands for all of its text.
    """
    if len(folder) <= FOLDER_KEPT:
        return folder
    return ("A" if name_found(folder, 0, None, 0) else "<") * (FOLDER_KEPT + 1)


def folded_tails(text: str, starts: set[int], separators: re.Pattern[str], joiner: str) -> dict[int, str]:
    """For each start, a short path that normalizes like ``text[start:]``: the folders that climb out of the start and the first two folders that stay.

    Each start is the position after a separator. One pass from the right reads each folder once and keeps a bounded stand-in for it, so work and
    storage stay linear however many home roots the text holds and however long a folder that survives them is.
    """
    tails: dict[int, str] = {}
    if not starts:
        return tails
    first = min(starts)
    climbs = 0
    names: deque[str] = deque(maxlen=2)
    stop = len(text)
    begins = [match.end() for match in separators.finditer(text, first)]
    for begin in [*reversed(begins), first]:
        size = stop - begin
        if size == 2 and text.startswith("..", begin):
            climbs += 1
        elif size > 1 or (size == 1 and text[begin] != "."):
            if climbs:
                climbs -= 1
            else:
                names.appendleft(folder_stand_in(text[begin:stop]))
        if begin in starts:
            tails[begin] = joiner.join([".."] * min(climbs, 3) + list(names))
        stop = begin - 1
    return tails


def path_from(path: str, offset: int) -> str:
    """The path from an offset."""
    return path[offset:]


def folded_from(tails: dict[int, str], begin: int, offset: int) -> str:
    """The folded path from an offset after ``begin``."""
    return tails.get(begin + offset, "")


def file_url_found(text: str, position: int = 0) -> bool:
    """Whether a local file address from ``position``, with any host (localhost, an IP address or another name), has a home path with a user name."""
    for url in FILE_URL.finditer(text, position):
        try:
            path = urllib.parse.urlsplit(url.group()).path
        except ValueError:
            continue
        root = FILE_URL_ROOT.match(path)
        if root:
            rest = path[root.end() :]
            if home_root_found("mac", root.group(), rest, None, 0, functools.partial(path_from, rest), ".." in rest):
                return True
    return False


def word_has_home(value: str, segments: list[int], lead: str = "") -> bool:
    """Whether one shell word holds a home path with a user name. ``lead`` is the character before the word, which a path start must not follow."""
    probe = lead + value
    shift = len(lead)
    found = []
    posix_starts: set[int] = set()
    windows_starts: set[int] = set()
    for match in HOME_ROOT.finditer(probe, shift):
        kind = str(match.lastgroup)
        begin = match.end() - shift
        if kind == "windows":
            cut = ANY_SEPARATOR.search(value, begin)
            windows_starts.add(begin)
        else:
            cut = SLASH_SEPARATOR.search(value, begin)
            posix_starts.add(begin)
        stop = cut.start() if cut else len(value)
        if kind == "mac" and value[begin:stop].casefold() == "shared":
            posix_starts.add(stop + 1)
        found.append((kind, match.group(), begin, stop))
    if found and ".." in value:
        posix_tails = folded_tails(value, posix_starts, SLASH_SEPARATOR, "/")
        windows_tails = folded_tails(value, windows_starts, ANY_SEPARATOR, "\\")
    else:
        posix_tails = windows_tails = {}
    climbed = value.rfind("..")
    for kind, root, begin, stop in found:
        tails = windows_tails if kind == "windows" else posix_tails
        if home_root_found(kind, root, value[begin:stop], segments, begin, functools.partial(folded_from, tails, begin), climbed >= begin):
            return True
    return TILDE_NAME.search(probe, shift) is not None or file_url_found(probe, shift)


def home_path_found(line: str) -> bool:
    """Whether a line holds a home path with a user name, in its text or in any decoded reading of it."""
    views = decoded_views(line) if "%" in line or "\\" in line else {line}
    return any(word_has_home(value, segments, lead) for view in views if HOME_HINT.search(view) for value, segments, lead in lex_words(view))


def line_privacy(line: str, values: list[str]) -> list[str]:
    """Return the privacy finding types on one line."""
    kinds = []
    for kind, (_, pattern) in PRIVACY_PATTERNS.items():
        if kind == "home-path":
            if home_path_found(line):
                kinds.append(kind)
            continue
        matches = list(pattern.finditer(line))
        if any(not exempt_match(kind, match) for match in matches) or (kind == "aws-account" and matches and other_twelve_digits(line)):
            kinds.append(kind)
    if any(value in line for value in values):
        kinds.append("env-value")
    return kinds


def privacy_scan() -> tuple[list[Finding], list[str]]:
    """Scan whole files, not the diff, for personal data and environment-specific values; never report the value."""
    allowed, findings = privacy_allowlist()
    values = env_values()
    unreviewed = []
    descriptions = {kind: description for kind, (description, _) in PRIVACY_PATTERNS.items()} | {"env-value": "value declared in .env"}
    for path in privacy_files():
        if path == ".env" or os.path.islink(path) or not os.path.isfile(path):
            continue
        data = Path(path).read_bytes()
        try:
            if b"\0" in data[:8192]:
                raise UnicodeDecodeError("utf-8", data[:1], 0, 1, "binary")
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            unreviewed.append(f"{path}: unreviewed: cannot be read as text. Inspect it before publishing")
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
    """Masked prose lines of a Markdown file, outside front matter, fenced code blocks and indented code blocks.

    An indented block is code when a blank line precedes it and no list item is open. Inside a list item, an indented paragraph is prose.
    """
    lines: list[tuple[int, str]] = []
    fenced = front = code = listed = False
    previous_blank = True
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
        if fenced:
            continue
        if not line.strip():
            lines.append((number, ""))
            previous_blank = True
            continue
        expanded = line.expandtabs(4)
        indent = len(expanded) - len(expanded.lstrip())
        if LIST_ITEM.match(line):
            listed = True
        elif indent == 0 and previous_blank:
            listed = False
        code = indent >= 4 and (code or (previous_blank and not listed))
        previous_blank = False
        if not code:
            lines.append((number, mask_prose(line)))
    return lines


def python_reference_lines(text: str) -> list[tuple[int, str]]:
    lines: list[tuple[int, str]] = []
    try:
        for token in tokenize.generate_tokens(io.StringIO(text).readline):
            if token.type == tokenize.COMMENT:
                lines.append((token.start[0], token.string))
    except (tokenize.TokenError, IndentationError):
        pass
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return lines
    source = text.splitlines()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) or not node.body:
            continue
        first = node.body[0]
        if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
            lines.extend((number + 1, source[number]) for number in range(first.lineno - 1, first.end_lineno or first.lineno))
    return sorted(lines)


def internal_reference_lines(path: str, text: str) -> list[tuple[int, str]]:
    suffix = Path(path).suffix
    if suffix in {".py", ".pyi", ".pyw"}:
        return python_reference_lines(text)
    if suffix == ".json":
        lines = []
        number, offset = 1, 0
        for match in JSON_STRING.finditer(text):
            number += text.count("\n", offset, match.start())
            offset = match.start()
            if not match.group("key"):
                try:
                    value = json.loads(match.group("value"))
                except json.JSONDecodeError:
                    value = match.group("value")[1:-1]
                lines.append((number, value))
        return lines
    comments = []
    if suffix in HASH_COMMENT_SUFFIXES or Path(path).name in {"Dockerfile", "Makefile", ".env.example"}:
        comments.append(r"\#[^\n]*")
    if suffix in C_COMMENT_SUFFIXES:
        comments.append(r"/\*.*?\*/|//[^\n]*")
    if suffix == ".sql":
        comments.append(r"--[^\n]*")
    if not comments:
        return list(enumerate(text.splitlines(), 1))
    pattern = rf"(?:{QUOTED_CODE})|(?P<comment>{'|'.join(comments)})"
    lines = []
    number, offset = 1, 0
    for match in re.finditer(pattern, text, re.DOTALL):
        number += text.count("\n", offset, match.start())
        offset = match.start()
        if match.group("comment"):
            lines.extend((number + index, line) for index, line in enumerate(match.group("comment").splitlines()))
    return lines


def internal_reference_findings(allowed: list[tuple[str, str]]) -> list[Finding]:
    findings = []
    for path in sorted(set(git("ls-files", "-z").split("\0")) - {"", WRITING_ALLOWLIST}):
        if not Path(path).is_file() or Path(path).is_symlink():
            continue
        if any(kind == "internal-reference" and fnmatch.fnmatch(path, glob) for kind, glob in allowed):
            continue
        data = Path(path).read_bytes()
        if b"\0" in data:
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            continue
        seen: set[tuple[int, str]] = set()
        for number, line in internal_reference_lines(path, text):
            line = re.sub(r"\b[a-z][a-z0-9+.-]*://[^\s<>\"'`]+", "url", line, flags=re.IGNORECASE)
            for pattern, expression in INTERNAL_REFERENCE_PATTERNS.items():
                if expression.search(line) and (number, pattern) not in seen:
                    seen.add((number, pattern))
                    findings.append(Finding(f"{path}:{number}", f"internal reference ({pattern})", WRITING_RULE, INTERNAL_REFERENCE_FIX))
    return findings


def line_matches(kind: str, path: str, article: bool, line: str, pattern: re.Pattern[str]) -> bool:
    """Whether a prose line holds the kind: article-only and changelog exemptions apply, and entities do not join words in the warn-first kinds."""
    if (kind in ARTICLE_ONLY and not article) or (kind == "history" and path in HISTORY_EXEMPT):
        return False
    text = re.sub(r"&#?\w+;", " ", line) if kind in WARN_FIRST else line
    if kind == "dash" and article:
        text = text.replace("\u2014", "")
    return bool(pattern.search(text))


def paragraph_prose(line: str) -> bool:
    """Whether the line is plain paragraph text: no heading, list marker, quote, HTML, table row, setext underline or link definition."""
    return not WRAP_START.match(line) and "|" not in line and not SETEXT_UNDERLINE.match(line)


def wrapped(previous: str, line: str) -> bool:
    """Whether the line continues the paragraph or list item line before it: that line stops mid-sentence and has no hard break."""
    if previous.endswith(("  ", "\\")) or SENTENCE_END.search(previous.rstrip()):
        return False
    return paragraph_prose(line) and (paragraph_prose(previous) or (bool(LIST_ITEM.match(previous)) and "|" not in previous))


def writing_check(articles: list[str], block: list[str]) -> tuple[list[Finding], list[Finding]]:
    allowed, findings = read_allowlist(WRITING_ALLOWLIST, [*WRITING_PATTERNS, "internal-reference", "wrap"], WRITING_RULE, "why this text is kept as is")
    warnings: list[Finding] = []
    for path in sorted(set(git("ls-files", "--cached", "--others", "--exclude-standard").splitlines())):
        article = any(fnmatch.fnmatch(path, glob) for glob in articles)
        if not (path.endswith(".md") or article) or path.startswith(DATA_FOLDERS) or not os.path.isfile(path):
            continue
        lines = prose_lines(Path(path).read_text(errors="replace"))
        for index, (number, line) in enumerate(lines):
            hits = [
                (kind, description, fix) for kind, (description, pattern, fix) in WRITING_PATTERNS.items() if line_matches(kind, path, article, line, pattern)
            ]
            if index and lines[index - 1][0] == number - 1 and wrapped(lines[index - 1][1], line):
                hits.append(("wrap", *WRAP))
            for kind, description, fix in hits:
                if not any(kind == entry_kind and fnmatch.fnmatch(path, glob) for entry_kind, glob in allowed):
                    target = warnings if kind in WARN_FIRST and kind not in block else findings
                    target.append(Finding(f"{path}:{number}", description, WRITING_RULE, fix))
    target = findings if "internal-reference" in block else warnings
    target.extend(internal_reference_findings(allowed))
    return findings, warnings


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
        return [f"front matter is not valid YAML ({error}). Quote values that contain ': '"]
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


LINK_RULE = "bundle standards/conventional_docs.md section 8.9: relative repository links with no broken targets or anchors"
LINK_FIX = "link a tracked file or an existing heading, or remove the link; ignored, untracked and deleted files count as missing"


def link_findings() -> list[Finding]:
    """Every relative link of a tracked Markdown file reaches a tracked file or folder, and its #anchor a heading or HTML anchor there."""
    return [Finding(location, problem, LINK_RULE, LINK_FIX) for location, problem in check_links(git("ls-files", "-s", "-z", "--cached"), DATA_FOLDERS)]


def warn(findings: list[Finding]) -> int:
    """Report findings without failing: the warn period before a check blocks."""
    for finding in findings:
        sys.stderr.write(f"WARN {finding.location}: {finding.problem}\n  rule: {finding.rule}\n  fix: {finding.fix}\n")
    return 0


def main() -> int:
    """Run the named check and report its findings, then count the permanent allowlist entries it read."""
    checks = [
        "credential-files",
        "data-files",
        "suppressions",
        "subprocess-imports",
        "lint-settings",
        "env-example",
        "removed-names",
        "task-ids",
        "command-modules",
    ]
    checks += ["unused-code", "unused-dependencies", "orphan-files", "terraform-unused", "temporary-code"]
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "check", choices=[*checks, "commit-msg", "commit-range", "privacy-scan", "writing-check", "front-matter", "docs-references", "markdown-links"]
    )
    parser.add_argument("paths", nargs="*")
    parser.add_argument("--all", action="store_true", help="privacy-scan: every file Git does not ignore (the default scope). Ignored files are never scanned")
    parser.add_argument("--warn", action="store_true", help="report findings without failing: a check's warn period before it blocks")
    parser.add_argument("--articles", action="append", default=[], metavar="GLOB", help="writing-check: files that hold article text (em dashes)")
    parser.add_argument(
        "--block", action="append", default=[], choices=sorted(WARN_FIRST - REVIEWED_BY_HAND), help="writing-check: make a warn-first kind block"
    )
    parser.add_argument("--today", metavar="YYYY-MM-DD", help="judge allowlist expiry against this UTC date instead of today's")
    args = parser.parse_args()
    if args.today:
        try:
            set_today(args.today)
        except ValueError:
            parser.error("--today must be a valid YYYY-MM-DD date")
    code = run(args)
    for notice in SKIPPED.values():
        sys.stderr.write(f"{notice}\n")
    sys.stdout.write(permanent_summary())
    return code


def run(args: argparse.Namespace) -> int:
    """Run the check ``args`` names."""
    if args.check == "writing-check":
        findings, warnings = writing_check(args.articles, args.block)
        if args.warn:
            return warn(findings + warnings)
        warn(warnings)
        return report(findings)
    if args.check == "privacy-scan":
        findings, unreviewed = privacy_scan()
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
        "task-ids": task_ids,
        "command-modules": missing_command_modules,
        "unused-code": unused_code,
        "unused-dependencies": unused_dependencies,
        "orphan-files": orphan_files,
        "terraform-unused": terraform_unused,
        "temporary-code": temporary_code,
        "front-matter": front_matter,
        "docs-references": docs_references,
        "markdown-links": link_findings,
        "commit-msg": lambda: commit_message(args.paths[0]),
        "commit-range": lambda: commit_range(args.paths[0], args.paths[1]),
    }
    findings = runners[args.check]()
    return warn(findings) if args.warn else report(findings)


if __name__ == "__main__":
    sys.exit(main())
