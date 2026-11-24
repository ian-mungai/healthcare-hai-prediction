"""E2E test of the pre-commit checks: each hook rejects a bad sample and passes a good one.

Run from the repository root after setup (``.venv`` and ``.tools`` present):
``.venv/bin/python -m scripts.quality.run_checks_e2e``. Each case builds a scratch Git repository with this repository's
hook configuration and scripts, links in ``.venv`` and ``.tools``, stages the sample and runs the hook through
``pre-commit`` itself, so a hook that is not wired in cannot pass. Samples that look like secrets, suppressions or
personal data are assembled at run time, so this file does not trip the checks it tests. The CI log is the run's artifact.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import shutil
import struct
import sys
import tempfile
import time
import warnings
import zipfile
import zlib
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any

from scripts.process import clear_git_environment, run_command
from scripts.quality import case_shards, scratch_marker
from scripts.quality.hook_stage import hook_stage
from scripts.quality.repo_checks import load_yaml

ROOT = Path(__file__).resolve().parents[2]
PRE_COMMIT = ROOT / ".venv" / "bin" / "pre-commit"
COPIED = [
    ".github/workflows/ci.yml",
    "config/quality/ci_groups.json",
    "config/quality/launcher_inputs.json",
    ".pre-commit-config.yaml",
    "pyproject.toml",
    ".env.example",
    ".privacy_allowlist",
    ".writing_allowlist",
    ".markdownlint-cli2.jsonc",
    ".sqlfluff",
    ".yamllint",
    "config/quality_tools.json",
    "dbt/dbt_project.yml",
    "dbt/profiles.yml",
    "scripts/run_ci.sh",
    "scripts/ci/groups/python_lint.sh",
]
NOQA = "no" + "qa"
TYPE_IGNORE = "type" + ": ignore"
FAKE_PAT = "gh" + "p_" + "Zx7Qm2Lp9Rt4Wv8Ks3Nd6Hy1Bc5Fj0Ga2TeQ"
ENVIRON = "os." + "environ"
ADD_ARGUMENT = "add_" + "argument"
CREDIT = "Co-Authored" + "-By"
MADE_WITH = "Generated" + " with"
SESSION = "Claude" + "-Session"
SCISSORS = "# ------------------------ >8 ------------------------"
PROCESS_MODULE = "sub" + "process"
LAUNCHER_SOURCE = (ROOT / "scripts" / "process.py").read_text()
CI_SOURCE = (ROOT / "scripts" / "ci" / "groups" / "python_lint.sh").read_text()
ALLOWLIST = ".privacy_allowlist"
# The scratch repository holds none of the documents that an entry's path-absent expiry waits on, so the copy makes those entries permanent.
ALLOWLIST_SOURCE = re.sub(r" -- expires-when path-absent:\S+$", " -- permanent", (ROOT / ALLOWLIST).read_text(), flags=re.MULTILINE)
VENDOR_ADDRESS = "noreply" + "@" + "anthropic.com"  # The attribution check matches this vendor address.
HOME_PATH = "/Us" + "ers/jdoe/projects/app/run.log"
TEMP_PATH = "/var/" + "folders/zq/k3j9x0000gn/T/run_1"
PERSONAL_EMAIL = "jane.doe" + "@" + "gmail.com"
PHONE = "(206) 555" + "-0100"
ACCOUNT = "1234" + "56789012"
AWS_ARN = f"arn:aws:iam::{ACCOUNT}:role/deploy"
# The one synthetic account ID the privacy scan exempts, and other fake IDs it must still flag. All are built at run time, so this file
# holds no account ID literal.
EXEMPT_ACCOUNT = "1" * 12
OTHER_ACCOUNTS = {"all-zero": "0" * 12, "all-two": "2" * 12, "sequential": ACCOUNT}
ENV_VALUE = "acme" + "-admin-profile"
# The AWS CLI profile name of this project, a local label that .privacy_allowlist names for its one tracked file.
PROFILE_LABEL = "healthcare_hai" + "_prediction_dev"
PROFILE_FILE = "scripts/deploy.sh"
# The scan exempts no home path, not even the one the analytics image user once had. The paths are built here, so this file holds none the scan would
# flag. The old OpenMetadata admin address and the look-alike reserved domains are built too: each is flagged like any other real-domain address.
LINUX_HOME = "/ho" + "me/"
OLD_ANALYTICS_HOME = LINUX_HOME + "analytics"
HOME_WORD = "ho" + "me"
WINDOWS_USERS = "C:" + "\\Us" + "ers\\"
OLD_OPENMETADATA_LOGIN = "admin" + "@" + "open-metadata.org"
PROJECT_NAME = "acme" + "-analytics"
# No privacy finding may print the value it found.
# The user name of the synthetic document paths; a document finding names a member by its index, so the name of a member never reaches the output.
DOCUMENT_USER = "sample" + "_person"
PRIVACY_VALUES = [HOME_PATH, TEMP_PATH, DOCUMENT_USER, PERSONAL_EMAIL, PHONE, ACCOUNT, *OTHER_ACCOUNTS.values(), ENV_VALUE, PROFILE_LABEL]
WRITING_ALLOWLIST = ".writing_allowlist"
ISO_DATE = "2026" + "-09-30"
EM_DASH = "\u2014"
EN_DASH = "\u2013"
CLEAN_WRITING = (
    f"---\nlast_updated: {ISO_DATE}\n---\n\n# Notes\n\n"
    "Keep secrets out of logs or commits and run a privacy scan. Lists read red, green and blue.\n\n"
    f"Dates read Sep 30 2026. Code keeps `{ISO_DATE}`, `a`, `b` and [the log](https://example.invalid/{ISO_DATE}).\n\n"
    f"An em dash {EM_DASH} allowed outside articles.\n\n```text\nrun {ISO_DATE}, and done\n```\n"
)
CLEAN_WRITING_HOOK = CLEAN_WRITING.replace(f"An em dash {EM_DASH} allowed outside articles.\n\n", "")
CLAUSE_COMMA = "Secrets never go into logs or commits" + ", and a privacy scan runs on every commit.\n"
SERIAL_COMMA = "The check reads Markdown, articles" + ", and commit messages.\n"
NOR_COMMA = "It is not fast" + ", nor is it cheap.\n"
# Removed-name samples are assembled so this file never names them literally: the check searches code too.
OLD_SCRIPT = "old" + "_tool.py"
OLD_FLAG = "--legacy" + "-mode"
OLD_FUNCTION = "helper" + "_total"
OLD_CLASS = "Legacy" + "Loader"
OLD_KEY = "retention" + "_days"
OLD_VARIABLE = "bucket" + "_label"
OLD_MODEL = "stg" + "_orders"
SHARED_ARGUMENT = "zone" + "_slots"
SHARED_FIELD = "window" + "_stamp"
DATA_FILE_WORD = "epoch" + "_marker"
RETIRED_KEY = "retired" + "_magic_key"
PRIOR_ID = "urn:review:" + "prior-shape"
RENAMED_ID = "urn:review:" + "new-shape"
ESCAPED_PRIOR_ID = "urn:review:prior" + "\\u002d" + "shape"
SCHEMA_STEM = "ledger" + "_row"
SCHEMA_ID = "urn:hai:" + "ledger-row"
# Not pinned by any dependency group that the scratch repository copies.
OLD_PACKAGE = "hum" + "anize"
OLD_MODULE = OLD_PACKAGE
STALE_HELPER = "stale" + "_helper"
DYNAMIC_CLASS = "Dyn" + "Plugin"
OLD_NOTES = "old" + "_notes.md"
SIGN_OFF = "--sign" + "-off"
TRIMMED_KEY = "trimmed" + "_columns"
RETRY_POLICY = "retry" + "_policy"
CLOCK_ALIAS = "utc" + "_clock"
INPUT_NAME = "warm" + "-cache"
RETIRED_ENV = "retired" + "-key"
PENDING_LIST = "config/quality/mypy_runtimes.json"
PENDING_FIXTURE = '{"pending": {"dags/pending_dag.py": {"runtime": "fixture", "reason": "synthetic missing dependency"}}}\n'
PENDING_IMPORT_ONLY = "import hai_missing_dependency\n"
LEFTOVER = "was removed but is still referenced"
IMPORTERS_HOOK = "typecheck-importers"
LAUNCHER_HOOK = "launcher-list"
LAUNCHER_LIST = "config/quality/launcher_inputs.json"
LAUNCHER_BODY = "scripts/ci/groups/launcher.sh"
LAUNCHER_START = "launcher_e2e() {\n  python -m app.entry\n}\n"
# No file under scripts/ may pin this name: the scratch repository copies those files and would then count the sample import as declared.
UNUSED_PACKAGE = "quill" + "pad"
UNUSED_SETTING = "OLD" + "_SETTING"
TEST_SETTING = "SAMPLE_TEST" + "_SETTING"
CONTAINER_SETTING = "SAMPLE_CONTAINER" + "_SETTING"
TOOL_SETTING = "SAMPLE_TOOL" + "_SETTING"
ENV_LIST = "config/quality/env_names.json"
# The settings a person puts in .env, in the order of .env.example. The env-example cases start from a repository that holds only these.
ENV_FILE_NAMES = ["AWS_REGION", "AWS_PROFILE", "AWS_ACCOUNT_ID", "PROJECT_NAME", "S3_BUCKET", "ENVIRONMENT", "ALERT_EMAIL"]
CONTAINER_SETTER = "infra/jobs/sample.tf"
ECS_JOBS = "config/orchestration/ecs_broker_jobs.json"
DIRECT_JOBS = "config/orchestration/direct_jobs.json"
HOST_JOBS = "config/orchestration/host_jobs.json"
CONTAINER_UNSET = "is listed as container but nothing in infra/ or the job and launcher configs sets it"
CONTAINER_SETTER_TEXT = f'environment = [{{ name = "{CONTAINER_SETTING}", value = "x" }}]\n'
SHELL_SETTING = "SAMPLE_SHELL" + "_SETTING"
CONTAINER_SETTER_NAME = "SAMPLE_COMMENTED" + "_SETTING"
UNUSED_TF_VARIABLE = "unused" + "_region"
DASH_M_MODULE = "nightly" + "_report"
USED_TF = f'variable "{UNUSED_TF_VARIABLE}" {{}}\n\nlocals {{\n  region = var.{UNUSED_TF_VARIABLE}\n}}\n\n' + 'output "region" {\n  value = local.region\n}\n'
# The scratch repository holds this repository's scripts; the cleanup samples sit outside them.
COPIED_ALLOWED = (
    "orphan scripts/* -- check scripts copied into the scratch repository -- permanent\n"
    "orphan dbt/* -- dbt settings copied into the scratch repository -- permanent\n"
    "unused-code scripts/* -- check scripts copied into the scratch repository -- permanent\n"
    "unused-dependencies scripts/* -- check scripts copied into the scratch repository -- permanent\n"
)
SIX = "si" + "x"


def lock_files(groups: dict[str, tuple[dict[str, str], dict[str, str]]]) -> dict[str, str]:
    """A pyproject.toml and a hand-written uv.lock for groups of ``(declared pins, transitive pins)``: uv export reads them with no network."""
    names = sorted(groups)
    declared = "".join(f"{group} = [{', '.join(f'"{name}=={version}"' for name, version in groups[group][0].items())}]\n" for group in names)
    conflicts = ", ".join(f'{{ group = "{group}" }}' for group in names)
    pyproject = f'[project]\nname = "hai-prediction"\nversion = "0.0.0"\nrequires-python = "==3.12.*"\ndependencies = []\n\n[dependency-groups]\n{declared}'
    pyproject += f"\n[tool.uv]\npackage = false\nconflicts = [[{conflicts}]]\n"
    lock = 'version = 1\nrevision = 3\nrequires-python = "==3.12.*"\nconflicts = [[\n'
    lock += "".join(f'    {{ package = "hai-prediction", group = "{group}" }},\n' for group in names)
    lock += ']]\n\n[[package]]\nname = "hai-prediction"\nversion = "0.0.0"\nsource = { virtual = "." }\n\n[package.dev-dependencies]\n'
    for group in names:
        lock += f"{group} = [\n" + "".join(f'    {{ name = "{name}" }},\n' for name in groups[group][0]) + "]\n"
    lock += "\n[package.metadata]\n\n[package.metadata.requires-dev]\n"
    for group in names:
        lock += f"{group} = [" + ", ".join(f'{{ name = "{name}", specifier = "=={version}" }}' for name, version in groups[group][0].items()) + "]\n"
    pins = {name: version for group in names for name, version in {**groups[group][1], **groups[group][0]}.items()}
    for name in sorted(pins):
        links = [other for group in names if name in groups[group][0] for other in groups[group][1]]
        lock += f'\n[[package]]\nname = "{name}"\nversion = "{pins[name]}"\nsource = {{ registry = "https://pypi.org/simple" }}\n'
        lock += "dependencies = [\n" + "".join(f'    {{ name = "{other}" }},\n' for other in sorted(set(links))) + "]\n" if links else ""
        lock += (
            f'wheels = [\n    {{ url = "https://files.pythonhosted.org/packages/aa/{name}-{pins[name]}-py3-none-any.whl", hash = "sha256:{"a" * 64}" }},\n]\n'
        )
    return {"pyproject.toml": pyproject, "uv.lock": lock}


# A runtime map with an image whose lock group pins a package and a transitive pin, and a package the image provides. The host provides
# jsonschema as the real map does, because the scratch repository holds the launcher, which imports it. The map names the dependency
# groups, the one direct group and the tools; a tool is used as a command, so nothing imports it.
# The scratch repository holds this repository's scripts, whose fixtures name docs/ paths of their own.
DOCS_ALLOWED = COPIED_ALLOWED + "docs-path scripts/* -- check scripts copied into the scratch repository -- permanent\n"
# A runtime map with an image whose lock file pins a package and a transitive pin, and a package the image provides. The host provides
# jsonschema as the real map does, because the scratch repository holds the launcher, which imports it.
RUNTIME_MAP = "config/quality/dependency_runtimes.json"
PROVIDED_MODULE = "spark" + "lib"
RUNTIME_MAP_SOURCE = (
    '{"tools": {"ruff": "tool: formatter run by pre-commit"}, "direct_groups": ["host"], '
    '"host": {"groups": ["host"], "provided": {"jsonschema": "pinned for the acquisition collectors"}}, '
    '"runtimes": {"image": {"groups": ["image"], '
    f'"code": ["image/*.py"], "provided": {{"{PROVIDED_MODULE}": "the image\'s own distribution"}}}}}}}}\n'
)
HOST_MAP = '{"direct_groups": ["host"], "host": {"groups": ["host"], "provided": {"jsonschema": "pinned for the acquisition collectors"}}}\n'
RUNTIME_SAMPLE = {
    RUNTIME_MAP: RUNTIME_MAP_SOURCE,
    **lock_files({"host": ({"ruff": "0.16.3"}, {}), "image": ({UNUSED_PACKAGE: "0.9.0"}, {SIX: "1.17.0"})}),
    ".cleanup_allowlist": COPIED_ALLOWED,
}
# Cleanup checks: a sample that must block must also print its finding, so a failure for another reason cannot pass.
CLEANUP_HOOKS = {"unused-code", "unused-dependencies", "orphan-files", "terraform-unused"}
WHITELIST = "scripts/quality/vulture_whitelist.py"


TEMPORARY_LIST = "config/quality/temporary_code.json"
# The sample names are assembled so this file, which the scratch repositories copy, never names a listed file itself.
TEMPORARY_MODULE = "tmp" + "_one"
TEMPORARY_FILE = f"scripts/lakehouse/{TEMPORARY_MODULE}.py"
TEMPORARY_USE = f"permanent code uses the temporary file {TEMPORARY_FILE}"
TEMPORARY_LIST_SOURCE = json.dumps({"note": "sample list", "files": [{"path": TEMPORARY_FILE, "why": "sample temporary file"}]})
TEMPORARY_BASE = {TEMPORARY_LIST: TEMPORARY_LIST_SOURCE, TEMPORARY_FILE: "VALUE = 1\n"}


LINK_TARGET_PAGE = "# Setup\n\n## Install steps\n\n## Run `a/b` (fast), then C\n\n## Repeat\n\n## Repeat\n"


def link_page(*links: str) -> str:
    """A guide whose first link sits on line 3, one link per line."""
    return "# Guide\n\n" + "".join(f"See [the target]({link}).\n" for link in links)


@dataclass
class Case:
    """One hook run: files to stage (or delete) in a scratch repository and whether the hook must pass."""

    name: str
    hook: str
    expect_pass: bool
    files: dict[str, str | bytes] = field(default_factory=dict)
    committed: dict[str, str | bytes] = field(default_factory=dict)
    delete: list[str] = field(default_factory=list)
    message: str | None = None
    reason: str = ""  # A blocked case must print this text, so it fails for its intended cause.
    symlinks: dict[str, str] = field(default_factory=dict)  # Link path to link target, staged with the files.
    untracked: dict[str, str] = field(default_factory=dict)  # Written after staging, so Git does not track them.
    git_config: dict[str, str] = field(default_factory=dict)  # Repository settings applied before the committed files.


def read_sample(name: str) -> str:
    """A Python file that reads the environment variable ``name``."""
    return "import os\n\nVALUE = " + ENVIRON + f'.get("{name}", "")\n'


def env_entry(category: str, **extra: str) -> dict[str, str]:
    """One entry of the name list: a category, a reason and optional ``set_in`` or ``set_by``."""
    return {"category": category, "reason": "sample", **extra}


def env_names(names: dict[str, dict[str, str]] | None = None, modules: list[str] | None = None, env_file: list[str] | None = None) -> str:
    """The name list of a sample repository: the settings of .env, the extra test modules ``modules`` and the entries ``names``."""
    data = {"env_file": ENV_FILE_NAMES if env_file is None else env_file, "test_modules": {path: "sample test module" for path in modules or []}}
    return json.dumps({**data, "names": names or {}}) + "\n"


def job_config(*names: str) -> str:
    """A job config with one job whose container declares the variables ``names``."""
    return json.dumps({"version": 1, "jobs": {"run": {"containers": {"run": ["RUN_ID", *names]}}}}) + "\n"


def env_example_text(*names: str) -> str:
    """The sample .env.example: a header line and one placeholder line per name."""
    return "# Settings of a person.\n" + "".join(f"{name}=placeholder\n" for name in names)


ENV_BASELINE = {
    ".env.example": env_example_text(*ENV_FILE_NAMES),
    ENV_LIST: env_names(),
    "app/settings.py": "import os\n\n" + "".join(f'{name} = {ENVIRON}.get("{name}", "")\n' for name in ENV_FILE_NAMES),
}


def guide_page(*body: str) -> str:
    """A guide whose body starts on line 3."""
    return "# Guide\n\n" + "\n".join(body) + "\n"


def link_case(name: str, expect_pass: bool, files: dict[str, str | bytes], reason: str = "", **extra: Any) -> Case:
    """A markdown-links case; a blocked one must print ``reason``."""
    return Case(name, "markdown-links", expect_pass, files, reason=reason, **extra)


def unresolved(line: int, target: str, source: str = "docs/guide.md") -> str:
    return f"{source}:{line}: link target '{target}' is not a tracked file"


def no_heading(line: int, fragment: str, target: str, source: str = "docs/guide.md") -> str:
    return f"{source}:{line}: link anchor '#{fragment}' matches no heading in {target}"


SETUP = "docs/setup.md"
PICTURE = {"assets/pic.png": b"\x89PNG"}
UNICODE_MARK = "Café"  # e plus a combining acute accent: the mark stays in GitHub's anchor
UNICODE_LINK = "setup.md#cafe%CC%81-menu"
LINK_CASES = [
    # Destination syntax: parentheses, escapes, angle brackets, line breaks, titles, percent encoding and queries.
    link_case(
        "destinations with parentheses, escapes, angle brackets, titles and queries pass",
        True,
        {
            "docs/guide.md": guide_page(
                "See [a](guide(one).md), [b](guide\\(one\\).md), [c](<my notes (v2).md>), [d](my%20notes.md),",
                "[e](setup.md \"Title (with) parens\"), [f](setup.md 'Other'), [g](<setup.md> (Third)), [h](setup.md?plain=1#install-steps)",
                'and [i](\n  setup.md\n  "Title").',
            ),
            "docs/guide(one).md": "# One\n",
            "docs/my notes (v2).md": "# Notes\n",
            "docs/my notes.md": "# Notes\n",
            SETUP: LINK_TARGET_PAGE,
        },
    ),
    link_case(
        "truncated destination does not hide a missing file",
        False,
        {"docs/guide.md": guide_page("See [a](guide(one).md)."), "docs/guide(one": "x\n"},
        unresolved(3, "guide(one).md"),
    ),
    link_case("destination on the next line blocked", False, {"docs/guide.md": "# Guide\n\nSee [a](\nabsent.md).\n"}, unresolved(3, "absent.md")),
    link_case("missing target with a title blocked", False, {"docs/guide.md": guide_page('See [a](absent.md "Title").')}, unresolved(3, "absent.md")),
    link_case("missing percent-encoded target blocked", False, {"docs/guide.md": guide_page("See [a](my%20missing.md).")}, unresolved(3, "my missing.md")),
    link_case(
        "query string before a missing fragment blocked",
        False,
        {"docs/guide.md": guide_page("See [a](setup.md?plain=1#nope)."), SETUP: LINK_TARGET_PAGE},
        no_heading(3, "nope", SETUP),
    ),
    # Code is never read as links or headings.
    link_case(
        "links in indented code, double-backtick spans, comments and list fences pass",
        True,
        {
            "docs/guide.md": '# Guide\n\n    [a](absent.md)\n\nA ``span with ` and [b](absent.md)`` and `<a href="absent.md">`.\n\n'
            '<!-- [c](absent.md) -->\n\n<!--\n<a href="absent.md">d</a>\n-->\n\n- ```text\n  [e](absent.md)\n  ```\n\n'
            "- a\n  - b\n\n    ```text\n    [f](absent.md)\n    ```\n\n~~~text\n[g](absent.md)\n~~~\n",
        },
    ),
    link_case(
        "heading in a list fence is no anchor",
        False,
        {"docs/guide.md": guide_page("See [a](setup.md#ghost)."), SETUP: "# Setup\n\n- ```markdown\n  # Ghost\n  ```\n"},
        no_heading(3, "ghost", SETUP),
    ),
    link_case(
        "front matter is neither links nor headings",
        True,
        {"docs/guide.md": "---\ntitle: Guide\ndescription: See [a](absent.md)\n# note: x\nlast_updated: 2026-10-02\n---\n\n# Guide\n"},
    ),
    link_case(
        "comment line in front matter is no anchor",
        False,
        {"docs/guide.md": "---\ntitle: Guide\n# notes line\n---\n\n# Guide\n\nSee [a](#notes-line).\n"},
        no_heading(8, "notes-line", "docs/guide.md"),
    ),
    # GitHub's heading anchors.
    link_case(
        "setext headings are anchors",
        True,
        {"docs/guide.md": guide_page("See [a](setup.md#install-steps) and [b](setup.md#run-it)."), SETUP: "Install steps\n=============\n\nRun it\n------\n"},
    ),
    link_case(
        "combining marks and Unicode letters stay in an anchor",
        True,
        {
            "docs/guide.md": guide_page(f"See [a]({UNICODE_LINK}), [b](setup.md#z%C3%BCrich-%E6%97%A5%E6%9C%AC), [c](setup.md#zürich-日本)."),
            SETUP: f"# Setup\n\n## {UNICODE_MARK} menu\n\n## Zürich 日本\n",
        },
    ),
    link_case(
        "underscores, entities, emphasis, links, punctuation, hyphens and emoji follow GitHub's slug",
        True,
        {
            "docs/guide.md": guide_page(
                "See [a](other.md#_name_-option), [b](other.md#a--b), [c](other.md#bold-and-soft-guide), [d](other.md#whats-new-v20-api-keys),",
                "[e](other.md#x---y--z), [f](other.md#-launch), [g](other.md#snake_case_name), [h](other.md#12-overview), [i](other.md#quoted-heading)",
                "and [j](other.md#Bold-And-Soft-Guide).",
            ),
            "docs/other.md": "# Other\n\n## `_name_` option\n\n## A &amp; B\n\n## **Bold** and *soft* [guide](guide.md) ##\n\n"
            "## What's New? (v2.0), API: Keys!\n\n## x - y  z\n\n## \U0001f680 Launch\n\n## snake_case_name\n\n## 1.2 Overview\n\n> ## Quoted heading\n",
        },
    ),
    link_case(
        "repeated headings skip generated names",
        True,
        {"docs/guide.md": guide_page("See [a](setup.md#repeat-2) and [b](setup.md#repeat-1)."), SETUP: "# Repeat\n\n## Repeat-1\n\n## Repeat\n"},
    ),
    link_case(
        "repeated heading beyond the last number blocked",
        False,
        {"docs/guide.md": guide_page("See [a](setup.md#repeat-3)."), SETUP: "# Repeat\n\n## Repeat-1\n\n## Repeat\n"},
        no_heading(3, "repeat-3", SETUP),
    ),
    # HTML: attributes in any spelling, anchors on any element, case kept for ids.
    link_case("spaced uppercase HTML attribute blocked", False, {"docs/guide.md": '# Guide\n\n<A HREF = "absent.md">a</A>\n'}, unresolved(3, "absent.md")),
    link_case("single-quoted HTML attribute blocked", False, {"docs/guide.md": "# Guide\n\n<a href='absent.md'>a</a>\n"}, unresolved(3, "absent.md")),
    link_case("unquoted HTML attribute blocked", False, {"docs/guide.md": "# Guide\n\n<a href=absent.md>a</a>\n"}, unresolved(3, "absent.md")),
    link_case("HTML image source blocked", False, {"docs/guide.md": '# Guide\n\n<img src="assets/absent.png" alt="x">\n'}, unresolved(3, "assets/absent.png")),
    link_case("inline HTML link in a paragraph blocked", False, {"docs/guide.md": 'Text\n\nmore <a href="absent.md">a</a> text\n'}, unresolved(3, "absent.md")),
    link_case(
        "HTML link on a later line of a block reports that line",
        False,
        {"docs/guide.md": '# Guide\n\n<div>\n<a href="setup.md">ok</a>\n<a href="absent.md">bad</a>\n</div>\n', SETUP: LINK_TARGET_PAGE},
        unresolved(5, "absent.md"),
    ),
    link_case(
        "Markdown inside an HTML block after a blank line is checked",
        False,
        {"docs/guide.md": "# Guide\n\n<div>\n\n[a](absent.md)\n\n</div>\n"},
        unresolved(5, "absent.md"),
    ),
    link_case(
        "valid HTML links, entities and external addresses pass",
        True,
        {
            "docs/guide.md": '# Guide\n\n<a href="setup.md#install-steps">a</a> <img src="../assets/pic.png"> <a href="my&#32;notes.md">b</a>\n\n'
            '<a href="https://example.invalid/absent.md">c</a>\n',
            SETUP: LINK_TARGET_PAGE,
            "docs/my notes.md": "# Notes\n",
            **PICTURE,
        },
    ),
    link_case(
        "id on any element and name on an anchor are anchors",
        True,
        {
            "docs/guide.md": guide_page("See [a](setup.md#custom), [b](setup.md#plain) and [c](setup.md#old-spot)."),
            SETUP: '# Setup\n\n<div id="custom">x</div>\n\n<span id=plain>y</span>\n\n<a name="old-spot"></a>\n',
        },
    ),
    link_case(
        "HTML id keeps its case: lower-case link blocked",
        False,
        {"docs/guide.md": guide_page("See [a](setup.md#custom)."), SETUP: '# Setup\n\n<a id="CUSTOM"></a>\n'},
        no_heading(3, "custom", SETUP),
    ),
    link_case(
        "HTML id keeps its case: exact link passes",
        True,
        {"docs/guide.md": guide_page("See [a](setup.md#CUSTOM)."), SETUP: '# Setup\n\n<a id="CUSTOM"></a>\n'},
    ),
    link_case(
        "name on a non-anchor element is no anchor",
        False,
        {"docs/guide.md": guide_page("See [a](setup.md#old-spot)."), SETUP: '# Setup\n\n<div name="old-spot">x</div>\n'},
        no_heading(3, "old-spot", SETUP),
    ),
    # Symlinks resolve inside the tracked tree.
    link_case(
        "symlink to a tracked file passes with that file's headings",
        True,
        {"docs/guide.md": guide_page("See [a](alias.md#install-steps)."), SETUP: LINK_TARGET_PAGE},
        symlinks={"docs/alias.md": "setup.md"},
    ),
    link_case(
        "symlink to a tracked file with a missing heading blocked",
        False,
        {"docs/guide.md": guide_page("See [a](alias.md#nope)."), SETUP: LINK_TARGET_PAGE},
        no_heading(3, "nope", SETUP),
        symlinks={"docs/alias.md": "setup.md"},
    ),
    link_case(
        "symlink to an ignored file blocked",
        False,
        {".gitignore": "docs/hidden.md\n", "docs/hidden.md": "# Hidden\n", "docs/guide.md": guide_page("See [a](alias.md#hidden).")},
        "docs/guide.md:3: link target 'alias.md' is a symlink that does not resolve to a tracked file",
        symlinks={"docs/alias.md": "hidden.md"},
    ),
    link_case(
        "symlink above the repository root blocked",
        False,
        {"docs/guide.md": guide_page("See [a](alias.md).")},
        "docs/guide.md:3: link target 'alias.md' is a symlink that does not resolve to a tracked file",
        symlinks={"docs/alias.md": "../../outside.md"},
    ),
    link_case(
        "symlink loop blocked",
        False,
        {"docs/guide.md": guide_page("See [a](one.md).")},
        "docs/guide.md:3: link target 'one.md' is a symlink that does not resolve to a tracked file",
        symlinks={"docs/one.md": "two.md", "docs/two.md": "one.md"},
    ),
    link_case(
        "symlinked folder passes",
        True,
        {
            "docs/guide.md": guide_page("See [a](linked/page.md#top-page), [b](linked/) and [c](linked/#top-page)."),
            "folder/page.md": "# Top page\n",
            "folder/README.md": "# Top page\n",
        },
        symlinks={"docs/linked": "../folder"},
    ),
    link_case(
        "symlinked Markdown file is not read as its own source",
        True,
        {"docs/real/page.md": "# Page\n\nSee [a](other.md).\n", "docs/real/other.md": "# Other\n"},
        symlinks={"docs/alias.md": "real/page.md"},
    ),
    # Markdown file extensions in any case.
    link_case(
        "uppercase Markdown extension is read as a source", False, {"README.MD": "# Project\n\nSee [a](absent.md).\n"}, unresolved(3, "absent.md", "README.MD")
    ),
    link_case(
        "markdown extension is read as a source",
        False,
        {"docs/notes.markdown": "# Notes\n\nSee [a](absent.md).\n"},
        unresolved(3, "absent.md", "docs/notes.markdown"),
    ),
    link_case(
        "anchor into an uppercase Markdown extension blocked",
        False,
        {"docs/guide.md": guide_page("See [a](Target.MD#nope)."), "docs/Target.MD": LINK_TARGET_PAGE},
        no_heading(3, "nope", "docs/Target.MD"),
    ),
    link_case(
        "anchor into an uppercase Markdown extension passes",
        True,
        {"docs/guide.md": guide_page("See [a](Target.MD#install-steps)."), "docs/Target.MD": LINK_TARGET_PAGE},
    ),
    # A folder link with a fragment points into the folder's README.
    link_case(
        "folder fragment with a missing heading blocked",
        False,
        {"docs/guide.md": guide_page("See [a](../folder/#missing)."), "folder/README.md": "# Valid\n"},
        no_heading(3, "missing", "folder/README.md"),
    ),
    link_case(
        "folder fragments pass for README and readme files and the root",
        True,
        {
            "docs/guide.md": guide_page("See [a](../folder/#valid), [b](../small/#little) and [c](../#project)."),
            "folder/README.md": "# Valid\n",
            "small/readme.md": "# Little\n",
            "README.md": "# Project\n",
        },
    ),
    link_case(
        "folder fragment without a README blocked",
        False,
        {"docs/guide.md": guide_page("See [a](../folder/#valid)."), "folder/app.py": "VALUE = 1\n"},
        "docs/guide.md:3: link anchor '#valid' points into folder 'folder', which has no tracked README",
    ),
    # Reference-style links and definitions.
    link_case(
        "reference links, definitions, labels and images pass",
        True,
        {
            "docs/guide.md": guide_page(
                "See [full][s], [collapsed][], [shortcut], ![img][pic] and [^1].",
                "",
                "[s]: setup.md#install-steps",
                '[collapsed]: <setup.md> "Title"',
                "[shortcut]: ./setup.md",
                "[Pic]: ../assets/pic.png",
                "[^1]: Note.",
            ),
            SETUP: LINK_TARGET_PAGE,
            **PICTURE,
        },
    ),
    link_case("unused broken definition blocked", False, {"docs/guide.md": guide_page("[ref]: absent.md")}, unresolved(3, "absent.md")),
    link_case(
        "definition with a missing heading blocked",
        False,
        {"docs/guide.md": guide_page("See [a][s].", "", "[s]: setup.md#nope"), SETUP: LINK_TARGET_PAGE},
        no_heading(5, "nope", SETUP),
    ),
    link_case(
        "reference-style image with a missing file blocked",
        False,
        {"docs/guide.md": guide_page("![diagram][pic]", "", "[pic]: assets/absent.png")},
        unresolved(5, "assets/absent.png"),
    ),
    link_case(
        "undefined labels and definitions in code pass",
        True,
        {"docs/guide.md": guide_page("See [a][nope] and [also nope].", "", "```markdown", "[ref]: absent.md", "```")},
    ),
    # Out of scope: autolinks and external addresses.
    link_case(
        "autolinks, bare addresses and external images pass",
        True,
        {
            "docs/guide.md": guide_page(
                "<https://example.invalid/absent.md> <mailto:someone@example.invalid> https://example.invalid/absent.md#x",
                "![badge](https://example.invalid/badge.svg)",
            )
        },
    ),
    # Targets: untracked files and folders.
    link_case(
        "untracked file blocked",
        False,
        {"docs/guide.md": guide_page("See [a](notes.md).")},
        unresolved(3, "notes.md"),
        untracked={"docs/notes.md": "# Notes\n"},
    ),
    link_case(
        "folder holding only untracked files blocked",
        False,
        {"docs/guide.md": guide_page("See [a](../drafts/).")},
        unresolved(3, "../drafts/"),
        untracked={"drafts/a.md": "# Draft\n"},
    ),
    # Where links sit: tables, lists, quotes, headings, nested brackets and images in links.
    link_case(
        "link on a later line of a paragraph reports that line",
        False,
        {"docs/guide.md": "# Guide\n\nFirst line.\nSecond with [x](setup.md)\nthird [y](absent.md).\n", SETUP: LINK_TARGET_PAGE},
        unresolved(5, "absent.md"),
    ),
    link_case(
        "link in a table cell blocked",
        False,
        {"docs/guide.md": "# Guide\n\n| a | b |\n|---|---|\n| [x](absent.md) | y |\n"},
        unresolved(5, "absent.md"),
    ),
    link_case("link in a quoted list item blocked", False, {"docs/guide.md": "# Guide\n\n- item\n  > quoted [x](absent.md)\n"}, unresolved(4, "absent.md")),
    link_case("link in a heading blocked", False, {"docs/guide.md": "# Guide\n\n## See [x](absent.md)\n"}, unresolved(3, "absent.md")),
    link_case(
        "nested brackets, image in a link and empty links pass",
        True,
        {
            "docs/guide.md": guide_page("See [a [b] c](setup.md), [![alt](../assets/pic.png)](setup.md), [empty](), [top](#top) and [hash](#)."),
            SETUP: LINK_TARGET_PAGE,
            **PICTURE,
        },
    ),
    # Review fixes: folders held by symlinks only, whitespace around HTML URLs, and the line of a link after a multiline construct.
    link_case(
        "folder holding only a symlink passes",
        True,
        {"docs/guide.md": guide_page("See [a](../folder/)."), "real.txt": "hi\n"},
        symlinks={"folder/alias.txt": "../real.txt"},
    ),
    link_case(
        "folder holding only a symlink to an ignored file passes as a folder",
        True,
        {".gitignore": "hidden.txt\n", "hidden.txt": "hi\n", "docs/guide.md": guide_page("See [a](../folder/).")},
        symlinks={"folder/alias.txt": "../hidden.txt"},
    ),
    link_case(
        "padded HTML URLs are trimmed before they are classified",
        True,
        {
            "docs/guide.md": '# Guide\n\n<a href=" https://example.invalid/page ">a</a> <a href="\n setup.md#install-steps\t">b</a>\n\n'
            '<img src=" ../assets/pic.png ">\n',
            SETUP: LINK_TARGET_PAGE,
            **PICTURE,
        },
    ),
    link_case(
        "padded relative HTML URL to a missing file blocked",
        False,
        {"docs/guide.md": '# Guide\n\n<a href=" absent.md ">a</a>\n'},
        unresolved(3, "absent.md"),
    ),
    link_case(
        "percent-encoded space in an HTML URL is kept",
        False,
        {"docs/guide.md": '# Guide\n\n<a href="%20setup.md">a</a>\n', SETUP: LINK_TARGET_PAGE},
        unresolved(3, " setup.md"),
    ),
    link_case(
        "padded HTML id and name keep their spaces as anchors",
        True,
        {
            "docs/guide.md": guide_page("See [a](setup.md#%20custom%20) and [b](setup.md#%20old-spot%20)."),
            SETUP: '# Setup\n\n<div id=" custom ">x</div>\n\n<a name=" old-spot "></a>\n',
        },
    ),
    link_case(
        "anchor without the spaces of a padded HTML id blocked",
        False,
        {
            "docs/guide.md": guide_page("See [a](setup.md#custom)."),
            SETUP: '# Setup\n\n<div id=" custom ">x</div>\n',
        },
        no_heading(3, "custom", SETUP),
    ),
    link_case(
        "link after a multiline code span reports its own line",
        False,
        {"docs/guide.md": "# Guide\n\n`a\nb` [x](absent.md)\n"},
        unresolved(4, "absent.md"),
    ),
    link_case(
        "link after a multiline link reports its own line",
        False,
        {"docs/guide.md": "# Guide\n\n[ok](\nsetup.md\n) [x](absent.md)\n", SETUP: LINK_TARGET_PAGE},
        unresolved(5, "absent.md"),
    ),
    link_case(
        "multiline link text reports the line where the link starts",
        False,
        {"docs/guide.md": "# Guide\n\n[a\nb](absent.md)\n"},
        unresolved(3, "absent.md"),
    ),
    link_case(
        "image and inline HTML after a multiline code span report their own line",
        False,
        {"docs/guide.md": '# Guide\n\n`a\nb` ![i](assets/absent.png) <a href="absent.md">x</a>\n'},
        unresolved(4, "assets/absent.png"),
    ),
    link_case(
        "link after a multiline code span in a quote reports its own line",
        False,
        {"docs/guide.md": "# Guide\n\n> `a\n> b` [x](absent.md)\n"},
        unresolved(4, "absent.md"),
    ),
    link_case(
        "link after a multiline code span in a list item reports its own line",
        False,
        {"docs/guide.md": "# Guide\n\n- item `c\n  d` [y](gone.md)\n"},
        unresolved(4, "gone.md"),
    ),
]


def step_workflow(*step_lines: str) -> str:
    """A workflow with one step that runs a local action; ``step_lines`` follow ``uses`` inside the step."""
    head = "name: ci\non: push\njobs:\n  core:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: ./.github/actions/setup\n"
    return head + "".join(f"        {line}\n" for line in step_lines)


DEFERRED_PROJECT = (
    "name: sample\nversion: '1.0.0'\nvars:\n  deferred_models:\n"
    f"    - models: [{OLD_MODEL}]\n      reason: gone\n      decision: owner\n      trigger: source\n      restore_from: git show abc\n"
)
INPUT_WORD_USE = {"scripts/warm.sh": f"{INPUT_NAME} --now\n"}  # The same word, as a program, still used in a script.
ACTION_INPUTS = (
    f"name: setup\ndescription: setup\ninputs:\n  {INPUT_NAME}:\n    description: warm\n    default: 'false'\n  python-version:\n    description: version\n"
)
CALLED_WORKFLOW = f"name: called\non:\n  workflow_call:\n    inputs:\n      {INPUT_NAME}:\n        type: boolean\n      python-version:\n        type: string\n"
STEPS_HEAD = "name: ci\non: push\njobs:\n  core:\n    runs-on: ubuntu-latest\n    steps:\n"
SETUP_STEP = "      - uses: ./.github/actions/setup\n"
INPUT_LINE = f'          {INPUT_NAME}: "true"\n'
ANCHORED_INPUTS = STEPS_HEAD + SETUP_STEP + f'        with: &params\n{INPUT_LINE}          python-version: "3.12"\n'
FLOW_ALIGNED = STEPS_HEAD + SETUP_STEP + f'        with: {{\n        {INPUT_NAME}: "true",\n        python-version: "3.12"\n        }}\n'
FLOW_STEP = STEPS_HEAD + f'      - {{uses: ./.github/actions/setup, with: {{\n          {INPUT_NAME}: "true",\n          python-version: "3.12"\n        }}}}\n'
ESCAPED_PARENT = STEPS_HEAD + SETUP_STEP + f'        "wi\\u0074h":\n{INPUT_LINE}          python-version: "3.12"\n'
MERGED_HUNK = (
    STEPS_HEAD
    + SETUP_STEP
    + f'        with:\n{INPUT_LINE}          python-version: "3.12"\n        env:\n          {RETIRED_ENV}: "old"\n          region: west\n'
)
INLINE_ACTION = (
    f"name: setup\ndescription: setup\ninputs:\n  {INPUT_NAME}: {{description: Cache, default: 'false'}}\n  python-version: {{description: Version}}\n"
)
INLINE_CALLED = (
    f"name: called\non:\n  workflow_call:\n    inputs:\n      {INPUT_NAME}: {{type: boolean, default: false}}\n      python-version: {{type: string}}\n"
)
SHARED_LINE = STEPS_HEAD + f'      - {{uses: ./.github/actions/setup, with: {{\n          {INPUT_NAME}: "true"}}, env: {{region: west}}}}\n'
MANUAL_WORKFLOW = CALLED_WORKFLOW.replace("workflow_call", "workflow_dispatch")
INPUT_CASES = [
    Case(
        "step input removed, the same word is still a program in a script",
        "removed-names",
        True,
        committed={".github/workflows/ci.yml": step_workflow("with:", f'  {INPUT_NAME}: "true"', '  python-version: "3.12"'), **INPUT_WORD_USE},
        files={".github/workflows/ci.yml": step_workflow("with:", '  python-version: "3.12"')},
    ),
    Case(
        "action input removed, the same word is still a program in a script",
        "removed-names",
        True,
        committed={".github/actions/setup/action.yml": ACTION_INPUTS, **INPUT_WORD_USE},
        files={".github/actions/setup/action.yml": ACTION_INPUTS.replace(f"  {INPUT_NAME}:\n    description: warm\n    default: 'false'\n", "")},
    ),
    Case(
        "reusable workflow input removed, the same word is still a program in a script",
        "removed-names",
        True,
        committed={".github/workflows/called.yml": CALLED_WORKFLOW, **INPUT_WORD_USE},
        files={".github/workflows/called.yml": CALLED_WORKFLOW.replace(f"      {INPUT_NAME}:\n        type: boolean\n", "")},
    ),
    Case(
        "anchored step input removed, the mapping only feeds inputs, the same word is still used",
        "removed-names",
        True,
        committed={".github/workflows/ci.yml": ANCHORED_INPUTS + SETUP_STEP + "        with: *params\n", **INPUT_WORD_USE},
        files={".github/workflows/ci.yml": (ANCHORED_INPUTS + SETUP_STEP + "        with: *params\n").replace(INPUT_LINE, "")},
    ),
    Case(
        "anchored step input removed, an alias also uses the mapping as environment, the same word is still used",
        "removed-names",
        False,
        committed={".github/workflows/ci.yml": ANCHORED_INPUTS + "      - run: echo done\n        env: *params\n", **INPUT_WORD_USE},
        files={".github/workflows/ci.yml": (ANCHORED_INPUTS + "      - run: echo done\n        env: *params\n").replace(INPUT_LINE, "")},
        reason=f"scripts/warm.sh:1: {INPUT_NAME} {LEFTOVER}",
    ),
    Case(
        "input and environment key removed in one merged hunk, the environment word is still used",
        "removed-names",
        False,
        committed={".github/workflows/ci.yml": MERGED_HUNK, "scripts/retired.sh": f"{RETIRED_ENV} --now\n"},
        files={".github/workflows/ci.yml": MERGED_HUNK.replace(INPUT_LINE, "").replace(f'          {RETIRED_ENV}: "old"\n', "")},
        reason=f"scripts/retired.sh:1: {RETIRED_ENV} {LEFTOVER}",
        git_config={"diff.interHunkContext": "5"},
    ),
    *(
        Case(
            f"{name} removed, the same word is still a program in a script",
            "removed-names",
            True,
            committed={".github/workflows/ci.yml": workflow, **INPUT_WORD_USE},
            files={".github/workflows/ci.yml": workflow.replace(line, "")},
        )
        for name, workflow, line in (
            ("aligned flow mapping step input", FLOW_ALIGNED, f'        {INPUT_NAME}: "true",\n'),
            ("inline flow step input", FLOW_STEP, f'          {INPUT_NAME}: "true",\n'),
            ("step input under an escaped parent key", ESCAPED_PARENT, INPUT_LINE),
        )
    ),
    Case(
        "action input with inline metadata removed, the same word is still a program in a script",
        "removed-names",
        True,
        committed={".github/actions/setup/action.yml": INLINE_ACTION, **INPUT_WORD_USE},
        files={".github/actions/setup/action.yml": INLINE_ACTION.replace(f"  {INPUT_NAME}: {{description: Cache, default: 'false'}}\n", "")},
    ),
    Case(
        "reusable workflow input with inline metadata removed, the same word is still a program in a script",
        "removed-names",
        True,
        committed={".github/workflows/called.yml": INLINE_CALLED, **INPUT_WORD_USE},
        files={".github/workflows/called.yml": INLINE_CALLED.replace(f"      {INPUT_NAME}: {{type: boolean, default: false}}\n", "")},
    ),
    Case(
        "step input shares its line with an ordinary key, the same word is still used",
        "removed-names",
        False,
        committed={".github/workflows/ci.yml": SHARED_LINE, **INPUT_WORD_USE},
        files={".github/workflows/ci.yml": STEPS_HEAD + "      - {uses: ./.github/actions/setup, with: {}, env: {region: west}}\n"},
        reason=f"scripts/warm.sh:1: {INPUT_NAME} {LEFTOVER}",
    ),
    Case(
        "step environment key removed, the same word is still used",
        "removed-names",
        False,
        committed={".github/workflows/ci.yml": step_workflow("env:", f'  {INPUT_NAME}: "true"', "  region: west"), **INPUT_WORD_USE},
        files={".github/workflows/ci.yml": step_workflow("env:", "  region: west")},
        reason=f"scripts/warm.sh:1: {INPUT_NAME} {LEFTOVER}",
    ),
    Case(
        "with key removed from an ordinary config file, the same word is still used",
        "removed-names",
        False,
        committed={"config/plan.yaml": f"with:\n  {INPUT_NAME}: 1\n  region: west\n", **INPUT_WORD_USE},
        files={"config/plan.yaml": "with:\n  region: west\n"},
        reason=f"scripts/warm.sh:1: {INPUT_NAME} {LEFTOVER}",
    ),
    Case(
        "manual dispatch input removed, the same word is still used",
        "removed-names",
        False,
        committed={
            ".github/workflows/manual.yml": MANUAL_WORKFLOW,
            **INPUT_WORD_USE,
        },
        files={".github/workflows/manual.yml": MANUAL_WORKFLOW.replace(f"      {INPUT_NAME}:\n        type: boolean\n", "")},
        reason=f"scripts/warm.sh:1: {INPUT_NAME} {LEFTOVER}",
    ),
]

# Text that is not UTF-8 (Latin-1) must not crash the check, and data with NUL bytes is binary: skipped with a notice.
UTIL_FUNCTION = f"def {OLD_FUNCTION}() -> int:\n    return 1\n"
UTIL_IMPORT = f"from app.util import {OLD_FUNCTION}\n\nVALUE: int = {OLD_FUNCTION}()\n"
ARCHIVE_FLAG = "--archive" + "-copy"
LATIN1_HEAD = "# -*- coding: latin-1 -*-\nimport argparse\n\nP = argparse.ArgumentParser()\n"
UMLAUT_HELP = 'help="Unterschrift f\u00fcr den Bericht"'
UTF8_HEAD = LATIN1_HEAD.replace("latin-1", "utf-8")
SIGN_OFF_LINE = f'P.{ADD_ARGUMENT}("{SIGN_OFF}", {UMLAUT_HELP})\n'
ARCHIVE_LINE = f'P.{ADD_ARGUMENT}("{ARCHIVE_FLAG}", {UMLAUT_HELP})\n'
SIGN_OFF_CALLER = f"cd /work\npython tools/digest.py {SIGN_OFF} /work/signoff.json\n"
PAD_LINES = "PAD = 1\n" * 1200  # Git reads only the first 8000 bytes to decide whether a file is binary.
BINARY_NOTICE = ": skipped: binary data (NUL bytes), not checked for removed names"
CAFE_FLAG = "--caf\u00c3\u00aa"  # The two bytes of UTF-8 \u00ea, read as Latin-1 text.
MIXED_PRIOR = (f'# Pr\u00fcfung\nimport argparse\n\nP = argparse.ArgumentParser()\nP.{ADD_ARGUMENT}("--caf').encode("latin-1") + b'\xc3\xaa")\n'
MIXED_NOW = "# Pr\u00fcfung\nimport argparse\n\nP = argparse.ArgumentParser()\n".encode("latin-1")
LATE_NUL_FILE = UTF8_HEAD + PAD_LINES + "BLOB = '\0'\n"
LATIN1_NAMES = {
    "binary": "Latin-1 case: a changed binary file is skipped with a notice",
    "late_nul": "Latin-1 case: a text file with a NUL byte after the first 8000 bytes is skipped with a notice",
    "gains_nul": "Latin-1 case: a text file that gains a NUL byte after the first 8000 bytes while it loses a flag is skipped with a notice",
    "new_nul": "Latin-1 case: a new text file with a NUL byte after the first 8000 bytes is skipped with a notice",
    "unchanged_nul": "Latin-1 case: an unchanged tracked binary file is skipped with a notice",
    "graph_nul": "Latin-1 case: a function removed while an unchanged tracked Python file holds a NUL byte is skipped with a notice",
}
GRAPH_LATIN1 = "# coding: latin-1\n# Pr\u00fcfung\n"
GRAPH_PENDING = {PENDING_LIST: PENDING_FIXTURE, "dags/pending_dag.py": "VALUE = 1\n"}
LATIN1_CASES = [
    Case(
        "Latin-1 file edited, a removed flag is still used in a script",
        "removed-names",
        False,
        committed={"tools/digest.py": (LATIN1_HEAD + SIGN_OFF_LINE).encode("latin-1"), "tools/run_report.sh": SIGN_OFF_CALLER},
        files={"tools/digest.py": LATIN1_HEAD.encode("latin-1")},
        reason=f"tools/run_report.sh:2: {SIGN_OFF} {LEFTOVER}",
    ),
    Case(
        "Latin-1 file edited, the removed flag is not used anywhere",
        "removed-names",
        True,
        committed={"tools/digest.py": (LATIN1_HEAD + SIGN_OFF_LINE).encode("latin-1"), "tools/run_report.sh": "python tools/digest.py\n"},
        files={"tools/digest.py": LATIN1_HEAD.encode("latin-1")},
    ),
    Case(
        "Latin-1 text file staged as new",
        "removed-names",
        True,
        committed={"tools/run_report.sh": "python tools/digest.py\n"},
        files={"data/adjacency_sample.txt": "county\tneighbor\nM\u00fcnchen\tFreising\n(redacted)\t(redacted)\n".encode("latin-1")},
    ),
    Case(
        "UTF-8 file with a non-ASCII line edited, a removed flag is still used in a script",
        "removed-names",
        False,
        committed={"tools/digest.py": UTF8_HEAD + SIGN_OFF_LINE, "tools/run_report.sh": SIGN_OFF_CALLER},
        files={"tools/digest.py": UTF8_HEAD},
        reason=f"tools/run_report.sh:2: {SIGN_OFF} {LEFTOVER}",
    ),
    Case(
        "Latin-1 and UTF-8 files edited together, the UTF-8 file's removed flag is still used",
        "removed-names",
        False,
        committed={
            "tools/digest.py": (LATIN1_HEAD + SIGN_OFF_LINE).encode("latin-1"),
            "tools/archive.py": UTF8_HEAD + ARCHIVE_LINE,
            "tools/run_report.sh": f"python tools/archive.py {ARCHIVE_FLAG} /work/copy\n",
        },
        files={"tools/digest.py": LATIN1_HEAD.encode("latin-1"), "tools/archive.py": UTF8_HEAD},
        reason=f"tools/run_report.sh:1: {ARCHIVE_FLAG} {LEFTOVER}",
    ),
    Case(
        "Latin-1 script still uses a removed flag",
        "removed-names",
        False,
        committed={
            "tools/digest.py": UTF8_HEAD + SIGN_OFF_LINE,
            "tools/run_report.sh": ("# Pr\u00fcfung des Berichts\n" + SIGN_OFF_CALLER).encode("latin-1"),
        },
        files={"tools/digest.py": UTF8_HEAD},
        reason=f"tools/run_report.sh:3: {SIGN_OFF} {LEFTOVER}",
    ),
    Case(
        "Latin-1 file with one valid UTF-8 byte pair on a removed line, the same bytes in a caller",
        "removed-names",
        False,
        committed={"tools/digest.py": MIXED_PRIOR, "tools/run_report.sh": b"# Pr\xfcfung\npython tools/digest.py " + CAFE_FLAG.encode("latin-1") + b"\n"},
        files={"tools/digest.py": MIXED_NOW},
        reason=f"tools/run_report.sh:2: {CAFE_FLAG} {LEFTOVER}",
    ),
    Case(
        LATIN1_NAMES["gains_nul"],
        "removed-names",
        True,
        committed={"tools/digest.py": UTF8_HEAD + PAD_LINES + SIGN_OFF_LINE, "tools/run_report.sh": SIGN_OFF_CALLER},
        files={"tools/digest.py": LATE_NUL_FILE},
    ),
    Case(
        LATIN1_NAMES["new_nul"],
        "removed-names",
        True,
        committed={"tools/run_report.sh": "python tools/digest.py\n"},
        files={"tools/blob.py": LATE_NUL_FILE},
    ),
    Case(
        LATIN1_NAMES["unchanged_nul"],
        "removed-names",
        True,
        committed={"data/blob.bin": b"\x00\x01\x02 first", "tools/run_report.sh": "python tools/digest.py\n"},
        files={"tools/run_report.sh": "python tools/digest.py --verbose\n"},
    ),
    Case(
        LATIN1_NAMES["binary"],
        "removed-names",
        True,
        committed={"data/blob.bin": b"\x00\x01\x02 first"},
        files={"data/blob.bin": b"\x00\x01\x02 second"},
    ),
    Case(
        LATIN1_NAMES["late_nul"],
        "removed-names",
        True,
        committed={"tools/digest.py": UTF8_HEAD + PAD_LINES + "BLOB = '\0'\n" + SIGN_OFF_LINE, "tools/run_report.sh": SIGN_OFF_CALLER},
        files={"tools/digest.py": UTF8_HEAD + PAD_LINES + "BLOB = '\0'\n"},
    ),
    Case(
        "Latin-1 Python file with a declared codec loses a function nothing else uses",
        "removed-names",
        True,
        committed={**GRAPH_PENDING, "tools/helpers.py": (GRAPH_LATIN1 + UTIL_FUNCTION).encode("latin-1")},
        files={"tools/helpers.py": GRAPH_LATIN1.encode("latin-1")},
    ),
    Case(
        "function removed from a UTF-8 file while an unchanged Latin-1 Python file exists",
        "removed-names",
        True,
        committed={**GRAPH_PENDING, "app/util.py": UTIL_FUNCTION, "tools/other.py": (GRAPH_LATIN1 + "VALUE = 1\n").encode("latin-1")},
        files={"app/util.py": "X = 1\n"},
    ),
    Case(
        "function removed while a Latin-1 Python file that imports a pending file still names it",
        "removed-names",
        False,
        committed={
            **GRAPH_PENDING,
            "app/util.py": UTIL_FUNCTION,
            "tools/second.py": (GRAPH_LATIN1 + "from dags import pending_dag\n" + UTIL_IMPORT).encode("latin-1"),
        },
        files={"app/util.py": "X = 1\n"},
        reason=f"tools/second.py:6: {OLD_FUNCTION} {LEFTOVER}",
    ),
    Case(
        "function removed while an unchanged Python file holds a Latin-1 string literal without a codec declaration",
        "removed-names",
        False,
        committed={**GRAPH_PENDING, "app/util.py": UTIL_FUNCTION, "tools/legacy.py": "VALUE = '\u00fc'\n".encode("latin-1")},
        files={"app/util.py": "X = 1\n"},
        reason="removed-names: cannot read the import graph of the pending files: (unicode error)",
    ),
    Case(
        LATIN1_NAMES["graph_nul"],
        "removed-names",
        True,
        committed={**GRAPH_PENDING, "app/util.py": UTIL_FUNCTION, "tools/blob.py": "BLOB = '\0'\n"},
        files={"app/util.py": "X = 1\n"},
    ),
]


POLICY_DOCUMENT = "scripts/quality/repo_checks.py"
RUFF_STANDARD = '[tool.ruff]\nline-length = 160\ntarget-version = "py312"\n\n[tool.ruff.lint]\nselect = ["E", "F", "I", "B", "UP", "S", "SIM", "T20"]\n'
NO_ATTRIBUTE = f'has no attribute "{OLD_FUNCTION}"'
# A package named like the removed function still exists, so its documented and imported name stays valid.
SURVIVING_PACKAGE = {
    "app/__init__.py": "",
    "app/rig.py": UTIL_FUNCTION,
    f"app/{OLD_FUNCTION}/__init__.py": "KEEP: int = 7\n",
    "app/read.py": f"from app import {OLD_FUNCTION} as {OLD_FUNCTION}\n\nRESULT: int = {OLD_FUNCTION}.KEEP\n",
    "README.md": f"Use `{OLD_FUNCTION}`.\n",
}


# A deleted module's top-level names count only where the module is referenced. The module name is assembled so the scripts copied into the scratch
# repository never name it, and a pending file is searched as text, so it shows what the check reads.
DEAD_STEM = "ledger" + "_totals"
DEAD_FILE = f"app/{DEAD_STEM}.py"
DEAD_PATH = f"app.{DEAD_STEM}"
DEAD_MODULE = {DEAD_FILE: UTIL_FUNCTION, PENDING_LIST: PENDING_FIXTURE}
UNRELATED_PARAMETER = f"def report({OLD_FUNCTION}: int) -> int:\n    return {OLD_FUNCTION} + 1\n"
UNRELATED_KEYWORD = f"RESULT = run({OLD_CLASS}=True)\n"
# Inputs of the independent review of the first version: each form below must be read from the whole import statement, with the module identity kept.
OTHER_PACKAGE = {"other/__init__.py": "", f"other/{DEAD_STEM}.py": UTIL_FUNCTION}
READ_TOTAL = f"VALUE: int = totals.{OLD_FUNCTION}()\n"
READ_STEM = f"VALUE: int = {DEAD_STEM}.{OLD_FUNCTION}()\n"
DEAD_ROOT_FILE = f"{DEAD_STEM}.py"
DELETED_MODULE_ROUND_TWO = [
    Case(
        "deleted module, from-import continued over two lines remains",
        "removed-names",
        False,
        committed={**DEAD_MODULE, "dags/pending_dag.py": f"from app \\\n    .{DEAD_STEM} import {OLD_FUNCTION}\nVALUE: int = {OLD_FUNCTION}()\n"},
        delete=[DEAD_FILE],
        reason=f"dags/pending_dag.py:1: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, parenthesized package import with an alias remains",
        "removed-names",
        False,
        committed={**DEAD_MODULE, "dags/pending_dag.py": f"from app import (\n    {DEAD_STEM} as totals,\n)\n{READ_TOTAL}"},
        delete=[DEAD_FILE],
        reason=f"dags/pending_dag.py:1: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, parenthesized relative import with an alias remains",
        "removed-names",
        False,
        committed={**DEAD_MODULE, "app/__init__.py": "", "app/read.py": f"from . import (\n    {DEAD_STEM} as totals,\n)\n{READ_TOTAL}"},
        delete=[DEAD_FILE],
        reason=f"app/read.py:1: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, relative import of the parent package with an alias remains",
        "removed-names",
        False,
        committed={
            **DEAD_MODULE,
            "app/__init__.py": "",
            "app/sub/__init__.py": "",
            "app/sub/read.py": f"from .. import (\n    {DEAD_STEM} as totals,\n)\n{READ_TOTAL}",
        },
        delete=[DEAD_FILE],
        reason=f"app/sub/read.py:1: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, aliased dotted import and its attribute remain",
        "removed-names",
        False,
        committed={**DEAD_MODULE, "dags/pending_dag.py": f"import {DEAD_PATH} as totals\n{READ_TOTAL}"},
        delete=[DEAD_FILE],
        reason=f"dags/pending_dag.py:2: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, dynamic import with a from-list remains",
        "removed-names",
        False,
        committed={**DEAD_MODULE, "dags/pending_dag.py": f"VALUE: int = __import__('{DEAD_PATH}', fromlist=['{OLD_FUNCTION}']).{OLD_FUNCTION}()\n"},
        delete=[DEAD_FILE],
        reason=f"dags/pending_dag.py:1: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, re-export over several lines and its reader remain",
        "removed-names",
        False,
        committed={
            **DEAD_MODULE,
            "app/__init__.py": f"from .{DEAD_STEM} import (\n    {OLD_FUNCTION},\n)\n",
            "dags/pending_dag.py": f"from app import {OLD_FUNCTION}\n\nVALUE: int = {OLD_FUNCTION}()\n",
        },
        delete=[DEAD_FILE],
        reason=f"app/__init__.py:1: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, a local object of the same name is no module",
        "removed-names",
        True,
        committed={
            DEAD_FILE: UTIL_FUNCTION,
            "app/read.py": f"from types import SimpleNamespace\n\n{DEAD_STEM} = SimpleNamespace({OLD_FUNCTION}=lambda: 7)\n{READ_STEM}",
        },
        delete=[DEAD_FILE],
    ),
    Case(
        "deleted module, same module name in another package is no reference",
        "removed-names",
        True,
        committed={
            **OTHER_PACKAGE,
            DEAD_FILE: UTIL_FUNCTION,
            "dags/pending_dag.py": f"from other import {DEAD_STEM}\nVALUE: int = {DEAD_STEM}.{OLD_FUNCTION}()\n",
        },
        delete=[DEAD_FILE],
    ),
    Case(
        "deleted module, same last package name is no reference",
        "removed-names",
        True,
        committed={
            f"services/audit/{DEAD_STEM}.py": UTIL_FUNCTION,
            f"clients/audit/{DEAD_STEM}.py": UTIL_FUNCTION,
            "dags/pending_dag.py": f"from clients.audit.{DEAD_STEM} import {OLD_FUNCTION}\nVALUE: int = {OLD_FUNCTION}()\n",
        },
        delete=[f"services/audit/{DEAD_STEM}.py"],
    ),
    Case(
        "deleted module, relative import inside another package is no reference",
        "removed-names",
        True,
        committed={**OTHER_PACKAGE, DEAD_FILE: UTIL_FUNCTION, "other/read.py": f"from . import {DEAD_STEM}\nVALUE: int = {DEAD_STEM}.{OLD_FUNCTION}()\n"},
        delete=[DEAD_FILE],
    ),
    Case(
        "deleted module, its dotted name in a sentence is no leftover to a Python reader",
        "removed-names",
        True,
        committed={DEAD_FILE: UTIL_FUNCTION, "app/read.py": f'NOTE = "the {DEAD_PATH} term is a label"\n'},
        delete=[DEAD_FILE],
    ),
    Case(
        "deleted module, leading underscores in its name, from-import remains",
        "removed-names",
        False,
        committed={
            "app/__worker.py": UTIL_FUNCTION,
            PENDING_LIST: PENDING_FIXTURE,
            "dags/pending_dag.py": f"from app.__worker import {OLD_FUNCTION}\nVALUE: int = {OLD_FUNCTION}()\n",
        },
        delete=["app/__worker.py"],
        reason="dags/pending_dag.py:1: app.__worker " + LEFTOVER,
    ),
    Case(
        "deleted package init, an exported name is still imported",
        "removed-names",
        False,
        committed={
            "app/__init__.py": UTIL_FUNCTION,
            PENDING_LIST: PENDING_FIXTURE,
            "dags/pending_dag.py": f"from app import {OLD_FUNCTION}\nVALUE: int = {OLD_FUNCTION}()\n",
        },
        delete=["app/__init__.py"],
        reason=f"dags/pending_dag.py:1: app {LEFTOVER}",
    ),
    Case(
        "deleted root module, dynamic import remains",
        "removed-names",
        False,
        committed={DEAD_ROOT_FILE: UTIL_FUNCTION, "dags/pending_dag.py": f"VALUE: int = __import__('{DEAD_STEM}').{OLD_FUNCTION}()\n"},
        delete=[DEAD_ROOT_FILE],
        reason=f"dags/pending_dag.py:1: {DEAD_STEM} {LEFTOVER}",
    ),
    Case(
        "deleted root module, python -m command remains",
        "removed-names",
        False,
        committed={DEAD_ROOT_FILE: UTIL_FUNCTION, "tools/run.sh": f"python -m {DEAD_STEM}\n"},
        delete=[DEAD_ROOT_FILE],
        reason=f"tools/run.sh:1: {DEAD_STEM} {LEFTOVER}",
    ),
    Case(
        "deleted root module, the word in prose and a string is no reference",
        "removed-names",
        True,
        committed={DEAD_ROOT_FILE: UTIL_FUNCTION, "README.md": f"The {DEAD_STEM} word is a label.\n", "app/read.py": f'LABEL = "{DEAD_STEM}"\n'},
        delete=[DEAD_ROOT_FILE],
    ),
    Case(
        "deleted module, file name with a dotted suffix is another file",
        "removed-names",
        True,
        committed={DEAD_FILE: UTIL_FUNCTION, "README.md": f"Run tools/{DEAD_STEM}.py.template now.\n"},
        delete=[DEAD_FILE],
    ),
    Case(
        "deleted module, file name at a sentence end still counts",
        "removed-names",
        False,
        committed={DEAD_FILE: UTIL_FUNCTION, "README.md": f"Run tools.v2/{DEAD_STEM}.py.\n"},
        delete=[DEAD_FILE],
        reason=f"README.md:1: {DEAD_STEM}.py {LEFTOVER}",
    ),
]
# Inputs of the second review: a command in one Python string, a relative import above the top package and unreadable import roots.
COMMAND_FILE = "app/read.py"
DELETED_MODULE_ROUND_THREE = [
    Case(
        f"deleted module, command string {label} remains",
        "removed-names",
        False,
        committed={**DEAD_MODULE, COMMAND_FILE: f'COMMAND = "{command}"\n'},
        delete=[DEAD_FILE],
        reason=f"{COMMAND_FILE}:1: {module} {LEFTOVER}",
    )
    for label, command, module in [
        ("with python", f"python -m {DEAD_PATH} --check", DEAD_PATH),
        ("with a path to python and a flag", f".venv/bin/python -u -m {DEAD_PATH} --check", DEAD_PATH),
        ("with uv run python", f"uv run python -m {DEAD_PATH}", DEAD_PATH),
        ("with a grouped interpreter option", f"python -Bm {DEAD_PATH} --check", DEAD_PATH),
        ("with a versioned python at the end of a sentence", f"run /usr/bin/python3.12 -m {DEAD_PATH}.", DEAD_PATH),
    ]
]
DELETED_MODULE_ROUND_THREE += [
    Case(
        "deleted module, command string with a shell variable as the program remains",
        "removed-names",
        False,
        committed={**DEAD_MODULE, COMMAND_FILE: f'COMMAND = "exec \\"$HAI_PYTHON\\" -m {DEAD_PATH} --check"\n'},
        delete=[DEAD_FILE],
        reason=f"{COMMAND_FILE}:1: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, command in an alert record remains",
        "removed-names",
        False,
        committed={**DEAD_MODULE, "orchestration/example_alert.py": f'ALERT = {{"kind": "unproven", "command": "python -m {DEAD_PATH} workers"}}\n'},
        delete=[DEAD_FILE],
        reason=f"orchestration/example_alert.py:1: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, command in an f-string remains",
        "removed-names",
        False,
        committed={**DEAD_MODULE, COMMAND_FILE: f'NAME = "x"\nCOMMAND = f".venv/bin/python -m {DEAD_PATH} --output {{NAME}}"\n'},
        delete=[DEAD_FILE],
        reason=f"{COMMAND_FILE}:2: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, command string for a root module remains",
        "removed-names",
        False,
        committed={DEAD_ROOT_FILE: UTIL_FUNCTION, COMMAND_FILE: f'COMMAND = "python3 -m {DEAD_STEM}"\n'},
        delete=[DEAD_ROOT_FILE],
        reason=f"{COMMAND_FILE}:1: {DEAD_STEM} {LEFTOVER}",
    ),
    Case(
        "deleted module, command strings for other modules and prose with a flag are no reference",
        "removed-names",
        True,
        committed={
            **DEAD_MODULE,
            COMMAND_FILE: f'OTHER = "python -m other.{DEAD_STEM} --check"\nLONGER = "python -m {DEAD_PATH}_v2"\n'
            f'PROSE = "use -m to name a module such as {DEAD_PATH} in a sentence"\nBARE = "see {DEAD_STEM} -m"\n',
        },
        delete=[DEAD_FILE],
    ),
    Case(
        "deleted module, relative import above the top package is unresolved",
        "removed-names",
        True,
        committed={**DEAD_MODULE, "other/__init__.py": "", "other/read.py": f"from ..app import (\n    {DEAD_STEM} as totals,\n)\n{READ_TOTAL}"},
        delete=[DEAD_FILE],
    ),
    Case(
        "deleted module, unreadable import roots stop the check",
        "removed-names",
        False,
        committed={
            "pyproject.toml": '[tool.mypy]\nmypy_path = ["src"]\n',
            "mypy.ini": "[mypy]\n",
            f"src/{DEAD_FILE}": UTIL_FUNCTION,
            "dags/pending_dag.py": f"from {DEAD_PATH} import {OLD_FUNCTION}\nVALUE: int = {OLD_FUNCTION}()\n",
        },
        delete=[f"src/{DEAD_FILE}"],
        reason="cannot read the import roots",
    ),
]
# Inputs of the third review: Python options that take a value (-W, -X), attached forms, flag clusters and where option parsing ends (-c, a script, --).
DELETED_MODULE_ROUND_FOUR = [
    Case(
        f"deleted module, command string with interpreter options {label} remains",
        "removed-names",
        False,
        committed={**DEAD_MODULE, COMMAND_FILE: f'COMMAND = "{prefix} -m {DEAD_PATH} --check"\n'},
        delete=[DEAD_FILE],
        reason=f"{COMMAND_FILE}:1: {DEAD_PATH} {LEFTOVER}",
    )
    for label, prefix in [
        ("-u -W ignore -X dev", "python3.12 -u -W ignore -X dev"),
        ("-W ignore", "python -W ignore"),
        ("-X dev", ".venv/bin/python -X dev"),
        ("attached -Wignore -Xdev", "python -Wignore -Xdev"),
        ("clustered flags", "python -bBdEIqsSvx -OO -uB"),
        ("ending a cluster with -W", "python -uW ignore"),
        ("--check-hash-based-pycs with its value", "python --check-hash-based-pycs always"),
    ]
]
DELETED_MODULE_ROUND_FOUR += [
    Case(
        "deleted module, command string with the module attached to -m remains",
        "removed-names",
        False,
        committed={**DEAD_MODULE, COMMAND_FILE: f'COMMAND = "python -W ignore -m{DEAD_PATH}"\n'},
        delete=[DEAD_FILE],
        reason=f"{COMMAND_FILE}:1: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, -m after the end of interpreter options is no reference",
        "removed-names",
        True,
        committed={
            **DEAD_MODULE,
            COMMAND_FILE: f'AFTER_DASHES = "python -- -m {DEAD_PATH}"\nAFTER_PROGRAM = "python -c \'pass\' -m {DEAD_PATH}"\n'
            f'ATTACHED_PROGRAM = "python -cpass -m {DEAD_PATH}"\nAFTER_SCRIPT = "python -W ignore tool.py -m {DEAD_PATH}"\n'
            f'AFTER_STDIN = "python - -m {DEAD_PATH}"\nOTHER_MODULE = "python -W {DEAD_PATH} -m other.{DEAD_STEM}"\n',
        },
        delete=[DEAD_FILE],
    ),
]
DELETED_MODULE_CASES = [
    Case(
        "deleted module, its function name is an unrelated local variable",
        "removed-names",
        True,
        committed={**DEAD_MODULE, "dags/pending_dag.py": UNRELATED_PARAMETER},
        delete=[DEAD_FILE],
    ),
    Case(
        "deleted module, its class name is an unrelated word in another test",
        "removed-names",
        True,
        committed={DEAD_FILE: f"class {OLD_CLASS}:\n    pass\n", PENDING_LIST: PENDING_FIXTURE, "dags/pending_dag.py": UNRELATED_KEYWORD},
        delete=[DEAD_FILE],
    ),
    Case(
        "deleted module, its function name is a YAML column",
        "removed-names",
        True,
        committed={DEAD_FILE: UTIL_FUNCTION, "dbt/models/schema.yml": f"models:\n  - name: totals\n    columns:\n      - name: {OLD_FUNCTION}\n"},
        delete=[DEAD_FILE],
    ),
    Case(
        "deleted module, from-import remains",
        "removed-names",
        False,
        committed={**DEAD_MODULE, "dags/pending_dag.py": f"from {DEAD_PATH} import {OLD_FUNCTION}\n\nVALUE: int = {OLD_FUNCTION}()\n"},
        delete=[DEAD_FILE],
        reason=f"dags/pending_dag.py:1: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, package import of it remains",
        "removed-names",
        False,
        committed={**DEAD_MODULE, "dags/pending_dag.py": f"from app import {DEAD_STEM}\n\nVALUE: int = 1\n"},
        delete=[DEAD_FILE],
        reason=f"dags/pending_dag.py:1: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, relative import of it remains",
        "removed-names",
        False,
        committed={**DEAD_MODULE, "app/__init__.py": "", "app/main.py": f"from . import {DEAD_STEM}\n\nVALUE: int = 1\n"},
        delete=[DEAD_FILE],
        reason=f"app/main.py:1: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, attribute through it remains",
        "removed-names",
        False,
        committed={**DEAD_MODULE, "dags/pending_dag.py": f"from app import (\n    {DEAD_STEM},\n)\n\nVALUE: int = {DEAD_STEM}.{OLD_FUNCTION}()\n"},
        delete=[DEAD_FILE],
        reason=f"dags/pending_dag.py:5: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, dynamic import of its path remains",
        "removed-names",
        False,
        committed={**DEAD_MODULE, "app/loader.py": f'import importlib\n\nMODULE = importlib.import_module("{DEAD_PATH}")\n'},
        delete=[DEAD_FILE],
        reason=f"app/loader.py:3: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, its path stays in a config file",
        "removed-names",
        False,
        committed={DEAD_FILE: UTIL_FUNCTION, "config/jobs.yaml": f"job:\n  module: {DEAD_PATH}\n"},
        delete=[DEAD_FILE],
        reason=f"config/jobs.yaml:2: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, its file path stays in a document",
        "removed-names",
        False,
        committed={DEAD_FILE: UTIL_FUNCTION, "README.md": f"Call `{DEAD_FILE}`.\n"},
        delete=[DEAD_FILE],
        reason=f"README.md:1: {DEAD_STEM}.py {LEFTOVER}",
    ),
    Case(
        "deleted module, longer file and module names that contain its own are no reference",
        "removed-names",
        True,
        committed={
            DEAD_FILE: UTIL_FUNCTION,
            "README.md": f"Run `tools/dbt_{DEAD_STEM}.py`, `tools/my-{DEAD_STEM}.py` and `tools/{DEAD_STEM}.pyi`.\n",
            "config/jobs.yaml": f"job:\n  module: other.{DEAD_STEM}\n  also: app.{DEAD_STEM}_v2\n  and: app.dbt_{DEAD_STEM}\n",
        },
        delete=[DEAD_FILE],
    ),
    Case(
        "deleted module, its file name stays in a shell command",
        "removed-names",
        False,
        committed={DEAD_FILE: UTIL_FUNCTION, "tools/run.sh": f"python {DEAD_STEM}.py --now\n"},
        delete=[DEAD_FILE],
        reason=f"tools/run.sh:1: {DEAD_STEM}.py {LEFTOVER}",
    ),
    Case(
        "deleted module, its file name stays in a quoted path",
        "removed-names",
        False,
        committed={DEAD_FILE: UTIL_FUNCTION, "app/paths.py": f'PATH = "{DEAD_STEM}.py"\n'},
        delete=[DEAD_FILE],
        reason=f"app/paths.py:1: {DEAD_STEM}.py {LEFTOVER}",
    ),
    *DELETED_MODULE_ROUND_TWO,
    *DELETED_MODULE_ROUND_THREE,
    *DELETED_MODULE_ROUND_FOUR,
    Case(
        "function removed from a file that stays, an unrelated parameter elsewhere is its own name",
        "removed-names",
        True,
        committed={"app/util.py": UTIL_FUNCTION, PENDING_LIST: PENDING_FIXTURE, "dags/pending_dag.py": UNRELATED_PARAMETER},
        files={"app/util.py": "X = 1\n"},
    ),
]


MYPY_SRC = '[tool.mypy]\npython_version = "3.12"\nexplicit_package_bases = true\nmypy_path = ["src"]\n'
SRC_PACKAGE = {
    "src/acme/__init__.py": "",
    "src/acme/rig.py": UTIL_FUNCTION,
    "src/acme/read.py": f"from acme.rig import {OLD_FUNCTION}\n\nRESULT: int = {OLD_FUNCTION}()\n",
}
# Each way a project declares that ``src`` holds importable packages: MyPy's path, pytest's path and Ruff's source roots.
# The last two keep MyPy's path too, so that the type check resolves the imports.
SOURCE_ROOT_SETTINGS = {
    "mypy_path": MYPY_SRC,
    "pytest pythonpath": MYPY_SRC + '\n[tool.pytest.ini_options]\npythonpath = ["src"]\n',
    "ruff src": MYPY_SRC + '\n[tool.ruff]\nsrc = ["src"]\n',
}


def launcher_list(files: list[str], folders: list[str] | None = None) -> str:
    """A committed launcher input list for the fixture suite whose one entry point is app/entry.py; ``folders`` are read whole (extra inputs)."""
    folders = folders or []
    document = {
        "note": "fixture",
        "entry_points": ["app/entry.py"],
        "extra_inputs": folders,
        "ignored_reads": [],
        "inventory_reads": [],
        "dynamic_sites": {},
        "inventory_sites": {},
        "folders": folders,
        "files": sorted(files),
    }
    return json.dumps(document, indent=2) + "\n"


def both_hooks(name: str, expect_pass: bool, **extra: Any) -> tuple[Case, Case]:
    """The same change under the removed-names check and the MyPy importer check."""
    return Case(f"{name} (removed names)", "removed-names", expect_pass, **extra), Case(f"{name} (importer types)", IMPORTERS_HOOK, expect_pass, **extra)


def account_arn(account: str) -> str:
    """An IAM role ARN in ``account``."""
    return f"arn:aws:iam::{account}:role/deploy"


def account_field(account: str) -> str:
    """A configuration line that names ``account`` after an account id label."""
    return f'CONFIG = {{"account_id": "{account}"}}\n'


def account_conditional(first: str, second: str) -> str:
    """An assignment that evaluates to ``second``, whatever the first account literal says."""
    return f'account_id = "{first}" if False else "{second}"\n'


HOME_REASON = "notes.md:1: home-directory path with a user name"
EMAIL_REASON = "notes.md:1: email address"


def home_path_cases() -> list[Case]:
    """Cases for home paths: no home is exempt, so the old analytics home is flagged in every position it once passed in."""
    home = OLD_ANALYTICS_HOME
    mac_users = HOME_PATH.split("jdoe")[0]
    mixed_users = mac_users.swapcase()
    flagged = {
        "old analytics home at the end of a line": f"The home is {home}",
        "old analytics home before a sentence-final period": f"The home is {home}.",
        "old analytics home in parentheses": f"The home ({home}) is private.",
        "old analytics home in a mount": f"./data/ui:{home}/.duckdb/ui:ro",
        "old analytics home in quotes": f"'{home}/extensions.csv'",
        "old analytics home in capitals": home.replace("analytics", "ANALYTICS") + "/private.txt",
        "old analytics home below another folder": f"/srv{home}/private.txt",
        "old analytics home in a Dockerfile command": f"RUN cat {home}/extensions.csv",
        "analytics-like home name": f"{home}2/run.log",
        "another home name": f"{LINUX_HOME}analyst/run.log",
        "analytics name under the user folder": f"{HOME_PATH.split('jdoe')[0]}analytics/run.log",
        "Linux user named Shared": f"{LINUX_HOME}Shared/private.txt",
        "Linux home with an accented first letter": f"{LINUX_HOME}\u00e9lise/private.txt",
        "Linux home with a non-Latin first letter": f"{LINUX_HOME}\u7528\u6237/private.txt",
        "Linux home with a symbol first character": f"{LINUX_HOME}\U0001f600/private.txt",
        "Linux home with a no-break space first character": f"{LINUX_HOME}\u00a0jdoe/private.txt",
        "shared folder climbing into a home": f"{mac_users}Shared/../jdoe/private.txt",
        "shared folder climbing out through a subfolder": f"{mac_users}Shared/folder/../../jdoe/private.txt",
        "shared folder climbing out through an encoded parent": f"{mac_users}Shared/%2e%2e/jdoe/private.txt",
        "percent-encoded home name": f"{LINUX_HOME}%6adoe/private.txt",
        "percent-encoded home name under the user folder": f"{mac_users}%6adoe/private.txt",
        "hex-escaped home name": f'path = "{LINUX_HOME}\\x6adoe/private.txt"',
        "unicode-escaped home name": f'path = "{LINUX_HOME}\\u006adoe/private.txt"',
        "percent-encoded home root": f"%2F{HOME_WORD}%2Fjdoe%2Fprivate.txt",
        "percent-encoded slash after the home folder": f"/{HOME_WORD}%2Fjdoe/private.txt",
        "hex-escaped home root": f"\\x2f{HOME_WORD}\\x2fjdoe/private.txt",
        "unicode-escaped home root": f"\\u002f{HOME_WORD}\\u002fjdoe/private.txt",
        "octal-escaped home root": f"\\057{HOME_WORD}\\057jdoe/private.txt",
        "JSON-escaped slashes in a home path": f"\\/{HOME_WORD}\\/jdoe\\/private.txt",
        "Windows user path with backslashes": f"{WINDOWS_USERS}jdoe\\private.txt",
        "Windows user path with forward slashes": f"{WINDOWS_USERS.replace(chr(92), '/')}jdoe/private.txt",
        "Windows user path with escaped backslashes": f"{WINDOWS_USERS.replace(chr(92), chr(92) * 2)}jdoe\\\\private.txt",
        "Windows user path in lower case": f"{WINDOWS_USERS.lower()}jdoe\\private.txt",
        "named tilde": "~" + "jdoe/private.txt",
        "named tilde in a command": "cd ~" + "jdoe/private",
        "unicode-escaped named tilde": "\\u007e" + "jdoe/private.txt",
        "quoted home name": f'{LINUX_HOME}"jdoe"/private.txt',
        "quoted user folder name": f"{mac_users}'jdoe'/private.txt",
        "home root closed before the name": f'"{LINUX_HOME}"jdoe',
        "user folder closed before a quoted name": f"'{mac_users}'" + '"jdoe"',
        "quoted home name with a leading space": f'{LINUX_HOME}" jdoe"/private.txt',
        "shared folder with a double-quoted component climbing out": f'{mac_users}Shared/"folder"/../../jdoe/private.txt',
        "shared folder with a single-quoted component climbing out": f"{mac_users}Shared/'folder'/../../jdoe/private.txt",
        "shared folder with a quoted parent component": f'{mac_users}Shared/folder/".."/".."/jdoe/private.txt',
        "shared folder with quoted and encoded components climbing out": f"{mac_users}Shared/%22folder%22/../../jdoe/private.txt",
        "lower-case user folder with a shared folder climbing out": f"{mac_users.lower()}Shared/../jdoe/private.txt",
        "mixed-case user folder with a shared folder climbing out": f"{mac_users.swapcase()[:3]}{mac_users[3:].upper()}Shared/../jdoe/private.txt",
        "lower-case user folder": f"{mac_users.lower()}jdoe/private.txt",
        "upper-case user folder": f"{mac_users.upper()}jdoe/private.txt",
        "lower-case shared folder climbing out in lower case": f"{mac_users.lower()}shared/../jdoe/private.txt",
        "JSON string with an escaped-quote home component": f'{{"command": "cat {LINUX_HOME}\\"jdoe\\"/private.txt"}}',
        "YAML scalar with an escaped-quote home component": f'command: "cat {LINUX_HOME}\\"jdoe\\"/private.txt"',
        "JSON string with an escaped-quote closed home root": f'"cat \\"{LINUX_HOME}\\"jdoe/private.txt"',
        "doubled backslash before an encoded home name": f"{LINUX_HOME}\\\\x6adoe/private.txt",
        "local file address with an empty host and a mixed-case user folder": f"file://{mixed_users}jdoe/private.txt",
        "local file address with a lower-case user folder": f"file://{mac_users.lower()}jdoe/private.txt",
        "local file address with a named local host": f"file://localhost{mixed_users}jdoe/private.txt",
        "local file address with an upper-case local host and user folder": f"file://LOCALHOST{mac_users.upper()}jdoe/private.txt",
        "local file address with one slash": f"file:{mac_users.lower()}jdoe/private.txt",
        "local file address with a shared folder climbing out": f"file://localhost{mac_users.lower()}Shared/../jdoe/private.txt",
        "local file address with an upper-case shared folder climbing out": f"file://{mac_users.upper()}SHARED/../jdoe/private.txt",
        "percent-encoded local file address": "file%3A%2F%2F%2F" + "uSeRs%2Fjdoe%2Fprivate.txt",
        "shared folder with a bracket in a quoted component": f'{mac_users}Shared/"team[1]"/../../jdoe/private.txt',
        "shared folder with a parenthesis in a quoted component": f'{mac_users}Shared/"team(1)"/../../jdoe/private.txt',
        "shared folder with a semicolon in a quoted component": f'{mac_users}Shared/"team;one"/../../jdoe/private.txt',
        "shared folder with a bracket in a single-quoted component": f"{mac_users}Shared/'team[1]'/../../jdoe/private.txt",
        "shared folder with nested brackets in a quoted component": f'{mac_users}Shared/"a[b[c]]"/../../jdoe/private.txt',
        "shared folder with a space in a quoted component": f'{mac_users}Shared/"team one"/../../jdoe/private.txt',
        "shared folder with a pipe and a comma in a single-quoted component": f"{mac_users}Shared/'a|b,c'/../../jdoe/private.txt",
        "shared folder with a brace in a quoted component": f'{mac_users}Shared/"team{{1}}"/../../jdoe/private.txt',
        "shared folder with quoted brackets below a lower-case user folder": f'{mac_users.lower()}Shared/"team[1]"/../../jdoe/private.txt',
        "shared folder with brackets in a JSON string": f'{{"command": "cat {mac_users}Shared/\\"team[1]\\"/../../jdoe/private.txt"}}',
        "shared folder with brackets in a percent-encoded quoted component": f"{mac_users}Shared/%22team[1]%22/../../jdoe/private.txt",
        "shared folder with an unquoted bracket component": f"{mac_users}Shared/team[1]/../../jdoe/private.txt",
        "shared folder with an unquoted brace component": f"{mac_users}Shared/{{a,b}}/../../jdoe/private.txt",
        "local file address with a loopback address": f"file://127.0.0.1{mixed_users}jdoe/private.txt",
        "local file address with a loopback address and a shared folder climbing out": f"file://127.0.0.1{mac_users.lower()}Shared/../jdoe/private.txt",
        "local file address with an IPv6 loopback address": f"file://[::1]{mixed_users}jdoe/private.txt",
        "local file address with a loopback address and an encoded name": f"file://127.0.0.1{mixed_users}%6adoe/private.txt",
        "local file address with a remote host": "file://example.invalid" + mixed_users + "jdoe/private.txt",
        "local file address with an upper-case scheme": f"FILE://127.0.0.1{mac_users.upper()}jdoe/private.txt",
        "local file address with a loopback address and a port": f"file://127.0.0.1:80{mixed_users}jdoe/private.txt",
        "shared folder with an escaped quote in a quoted component": f'cat {mac_users}Shared/"one\\" two"/../../jdoe/private.txt',
        "shared folder with a literal quote in a single-quoted path": f"cat '{mac_users}Shared/one\"two/../../jdoe/private.txt'",
        "shared folder with a literal quote in a lower-case single-quoted path": f"cat '{mac_users.lower()}Shared/one\"two/../../jdoe/private.txt'",
        "shared folder with a literal quote in a JSON string": f'{{"command": "cat \'{mac_users}Shared/one\\"two/../../jdoe/private.txt\'"}}',
        "shared folder with an escaped quote in a percent-encoded path": f"cat {mac_users}Shared/%22one%5C%22%20two%22/../../jdoe/private.txt",
        "shared folder with a backtick in a quoted component": f'cat {mac_users}Shared/"one`two"/../../jdoe/private.txt',
        "shared folder with an escaped space": f"cat {mac_users}Shared/one\\ two/../../jdoe/private.txt",
        "home placeholder climbing to a name": f"{LINUX_HOME}<user>/../jdoe/private.txt",
        "Windows placeholder climbing to a name": f"{WINDOWS_USERS}<user>\\..\\jdoe\\private.txt",
        "shared folder holding a home root with a variable climbing out": f"cat '{mac_users}Shared/{HOME_WORD}/$USER/../../../jdoe/private.txt'",
        "shared folder holding a home root with a brace placeholder climbing out": f"cat '{mac_users}Shared/{HOME_WORD}/{{user}}/../../../jdoe/private.txt'",
        "shared folder holding a home root with a percent placeholder climbing out": f"cat '{mac_users}Shared/{HOME_WORD}/%USER%/../../../jdoe/private.txt'",
        "home folder holding a home root with a variable climbing out": f"cat {LINUX_HOME}$USER/a/{HOME_WORD}/$USER/../../../../jdoe/private.txt",
        # An escaped shell separator is a character of its word, so it ends no token before a parent folder.
        "shared folder with an escaped semicolon before a parent": f"cat {mac_users}Shared/team\\;one/../../jdoe/private.txt",
        "shared folder with an escaped pipe before a parent": f"cat {mac_users}Shared/team\\|one/../../jdoe/private.txt",
        "shared folder with an escaped ampersand before a parent": f"cat {mac_users}Shared/team\\&one/../../jdoe/private.txt",
        "shared folder with an escaped opening parenthesis before a parent": f"cat {mac_users}Shared/team\\(one/../../jdoe/private.txt",
        "shared folder with an escaped closing parenthesis before a parent": f"cat {mac_users}Shared/team\\)one/../../jdoe/private.txt",
        "shared folder with an escaped tab before a parent": f"cat {mac_users}Shared/team\\\tone/../../jdoe/private.txt",
        "shared folder with an escaped semicolon right before a parent": f"cat {mac_users}Shared/team\\;/../../jdoe/private.txt",
        # A local file address is one token up to its first white space, whatever the shell would do with a separator inside it.
        "local file address with a semicolon in its path before a parent": f"file://{mac_users}Shared;one/../jdoe/private.txt",
        "local file address with a parenthesis in its path before a parent": f"file://{mac_users}Shared(one)/../jdoe/private.txt",
        "local file address with an ampersand in its path before a parent": f"file://{mac_users}Shared&one/../jdoe/private.txt",
        "local file address with a semicolon after the shared folder": f"file://{mac_users}Shared/team;one/../../jdoe/private.txt",
        "quoted local file address with a semicolon in its path before a parent": f"open 'file://{mac_users}Shared;one/../jdoe/private.txt'",
        # Decoding a JSON or YAML string reads its own text, not the quotes around it.
        "JSON string with a literal apostrophe and a double quote around a parent name": (
            f'{{"command": "cat {mac_users}Shared/\\"\'\\"/../../jdoe/private.txt"}}'
        ),
    }
    cases = [Case(name, "privacy-scan", False, {"notes.md": f"Log at {text}\n"}, reason=HOME_REASON) for name, text in flagged.items()]
    # The macOS shared folder is no user's home. Only the macOS root has this folder, so the Linux twin above stays flagged.
    # Placeholders and the home shortcuts name no user. A path through the shared folder passes only while it stays inside the folder.
    passing = {
        "macOS shared folder is no home": f"{mac_users}Shared/run.log",
        "macOS shared folder with a path that stays inside": f"{mac_users}Shared/a/../b/run.log",
        "macOS shared folder with a quoted path that stays inside": f'{mac_users}Shared/"a"/../b/run.log',
        "macOS shared folder with a quoted file name": f'"{mac_users}Shared/run.log"',
        "lower-case macOS shared folder": f"{mac_users.lower()}shared/run.log",
        "mixed-case macOS shared folder that stays inside": f"{mac_users.upper()}SHARED/a/../b/run.log",
        "JSON string with an escaped-quote placeholder": f'{{"command": "cat {LINUX_HOME}\\"<user>\\"/private.txt"}}',
        "JSON string with an escaped-quote variable": f'{{"command": "cat \\"{LINUX_HOME}\\"$USER/private.txt"}}',
        "lower-case user folder placeholder": f"{mac_users.lower()}<name>/run.log",
        "user folder route in an address path": "GET https://example.invalid/api/v1/users/login",
        "user folder route that follows another folder": "/api/v1/users/login",
        "local file address with a placeholder": f"file://{mixed_users}<name>/run.log",
        "local file address to a shared folder file": f"file://{mixed_users}Shared/run.log",
        "local file address to a shared folder path that stays inside": f"file://localhost{mac_users.lower()}shared/a/../b/run.log",
        "file address with a host named like the user folder": "file://" + mixed_users[1:] + "login",
        "web address on a host named like a local host": "https://localhost" + mixed_users.lower() + "login",
        "shared folder with a quoted bracket component that stays inside": f'{mac_users}Shared/"team[1]"/run.log',
        "shared folder with a quoted space component that stays inside": f"{mac_users}Shared/'team one'/a/../b/run.log",
        "shared folder with a bracket component that stays inside": f"{mac_users}Shared/team[1]/run.log",
        "two quoted shared folder arguments": f'cat "{mac_users}Shared/a.log" "/tmp/b"',
        "shared folder with apostrophes around a parent name": f"""cat {mac_users}Shared/"'..'"/private.txt""",
        "shared folder with double quotes around a parent name": f"""cat {mac_users}Shared/'".."'/private.txt""",
        "shared folder with an escaped quote that stays inside": f'cat {mac_users}Shared/"one\\" two"/run.log',
        "shared folder with a literal quote that stays inside": f"cat '{mac_users}Shared/one\"two/run.log'",
        "shared folder with a quoted parent that stays inside": f'cat {mac_users}Shared/"a"/"b"/"../c"/run.log',
        "home placeholder climbing to another placeholder": f"{LINUX_HOME}<user>/../<other>/private.txt",
        "local file address with a loopback address and a placeholder": f"file://127.0.0.1{mixed_users}<name>/run.log",
        "local file address with a loopback address to a shared file": f"file://127.0.0.1{mixed_users}Shared/run.log",
        "Linux home placeholder in angle brackets": f"{LINUX_HOME}<user>/run.log",
        "Linux home placeholder variable": f"{LINUX_HOME}$USER/run.log",
        "Linux home placeholder in braces": f"{LINUX_HOME}{{user}}/run.log",
        "Linux home placeholder in percent signs": f"{LINUX_HOME}%USER%/run.log",
        "Linux home placeholder format": f"{LINUX_HOME}%s/run.log",
        "user folder placeholder": f"{mac_users}<name>/run.log",
        "Windows user placeholder in angle brackets": f"{WINDOWS_USERS}<user>\\run.log",
        "Windows user placeholder in percent signs": f"{WINDOWS_USERS}%USERNAME%\\run.log",
        "bare tilde and home variables": "~/projects, $HOME/app and ${HOME}/app",
        "tilde placeholder": "~<user>/projects",
        "quoted home root followed by a variable": f'"{LINUX_HOME}"$USER/run.log',
        "quoted home root joined by a plus sign": f'"{LINUX_HOME}" + user',
        "quoted home roots in a list": f'("{LINUX_HOME}", "{mac_users}")',
        "address with a tilde folder": "https://example.invalid/~" + "jdoe/page",
        "shared folder holding a home root with a variable that stays inside": f"cat '{mac_users}Shared/{HOME_WORD}/$USER/a/../run.log'",
        "shared folder holding a home root with a brace placeholder": f"cat '{mac_users}Shared/{HOME_WORD}/{{user}}/run.log'",
        "nested web route below a parenthesized folder": "https://example.invalid/api(foo)/users/login",
        "parenthesized route folder before a user folder route": "(api)/users/login",
        "shared folder with an escaped semicolon that stays inside": f"cat {mac_users}Shared/team\\;one/../run.log",
        "local file address with a semicolon that stays inside the shared folder": f"file://{mac_users}Shared/team;one/../run.log",
        "JSON string with apostrophes around a parent name": f"""{{"command": "cat {mac_users}Shared/\\"'..'\\"/private.txt"}}""",
        "YAML double-quoted string with apostrophes around a parent name": f"""command: "cat {mac_users}Shared/\\"'..'\\"/private.txt\"""",
        "YAML list item with apostrophes around a parent name": f"""- "cat {mac_users}Shared/\\"'..'\\"/private.txt\"""",
        "single-quoted local file address to the shared folder": f"open 'file://{mac_users}Shared'",
        "double-quoted local file address to the shared folder": f'open "file://{mac_users}Shared"',
        "parenthesized local file address to the shared folder": f"open (file://{mac_users}Shared)",
    }
    cases += [Case(name, "privacy-scan", True, {"notes.md": f"Log at {text}\n"}) for name, text in passing.items()]
    return cases


def email_domain_cases() -> list[Case]:
    """Cases for reserved email domains: a reserved domain passes by equality or as a dot-delimited subdomain, never as a longer name that ends in it."""
    passing = [
        "someone" + "@" + "example.org",
        "someone" + "@" + "example.com",
        "someone" + "@" + "example.net",
        "someone" + "@" + "mail.example.org",
        "someone" + "@" + "a.b.example.net",
        "someone" + "@" + "host.invalid",
        "someone" + "@" + "notexample.invalid",
        "someone" + "@" + "host.test",
    ]
    flagged = [
        "someone" + "@" + "notexample.org",
        "someone" + "@" + "notexample.com",
        "someone" + "@" + "notexample.net",
        "someone" + "@" + "my-example.org",
        "someone" + "@" + "example.org.evil.io",
    ]
    cases = [Case(f"reserved domain address passes: {address.split('@')[1]}", "privacy-scan", True, {"notes.md": f"Ask {address}\n"}) for address in passing]
    cases += [
        Case(f"look-alike domain address is flagged: {address.split('@')[1]}", "privacy-scan", False, {"notes.md": f"Ask {address}\n"}, reason=EMAIL_REASON)
        for address in flagged
    ]
    return cases


HOME_AND_EMAIL_CASES = [*home_path_cases(), *email_domain_cases()]


def office_package(members: dict[str, str]) -> bytes:
    """A small OOXML package: member names with their text."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, text in members.items():
            archive.writestr(name, text)
    return buffer.getvalue()


def office_records(members: list[tuple[str, str | bytes]], method: int = zipfile.ZIP_DEFLATED) -> bytes:
    """An OOXML package written record by record, so a member name can repeat; a member name is whatever the publisher's archive says it is."""
    buffer = io.BytesIO()
    with warnings.catch_warnings(), zipfile.ZipFile(buffer, "w", method) as archive:
        warnings.simplefilter("ignore", UserWarning)
        for name, body in members:
            archive.writestr(name, body)
    return buffer.getvalue()


def damaged_package(kind: str) -> bytes:
    """A package of one member named with a user path that a ZIP reader cannot read: a damaged LZMA stream, a too new extract version or a bad UTF-8 name."""
    name = "/" + f"Users/{DOCUMENT_USER}/Documents/named.xml"
    data = bytearray(office_records([(name, "<a>" + "x" * 40 + "</a>")], zipfile.ZIP_LZMA if kind == "lzma" else zipfile.ZIP_STORED))
    local, central = data.index(b"PK\x03\x04"), data.index(b"PK\x01\x02")
    if kind == "lzma":
        data[local + 30 + sum(struct.unpack_from("<HH", data, local + 26)) + 4] = 0xFF
    elif kind == "version":
        struct.pack_into("<H", data, local + 4, 70)
        struct.pack_into("<H", data, central + 6, 70)
    else:
        struct.pack_into("<H", data, local + 6, 0x800)
        struct.pack_into("<H", data, central + 8, 0x800)
        data[central + 46] = 0xFF
    return bytes(data)


def pdf_document(content: str) -> bytes:
    """A small PDF whose one content stream holds ``content`` compressed, as a publisher's PDF does, so only a read of the decoded stream sees it."""
    body = zlib.compress(content.encode())
    return b"%PDF-1.4\n1 0 obj\n<< /Length " + str(len(body)).encode() + b" >>\nstream\n" + body + b"\nendstream\nendobj\n%%EOF\n"


def document_path_cases() -> list[Case]:
    """A user directory path inside a workbook, a legacy workbook or a PDF text stream is blocked; a clean file and a placeholder pass."""
    path = "C:\\" + f"Users\\{DOCUMENT_USER}\\Documents\\book.xlsx"
    block = f'<mc:AlternateContent><mc:Choice Requires="x15"><x15ac:absPath url="{path}"/></mc:Choice></mc:AlternateContent>'
    reason = "config/book.xlsx!member 2 (xml): home-directory path with a user name"
    named = "/" + f"Users/{DOCUMENT_USER}/Documents/named.xml"
    damaged = bytearray(office_records([("[Content_Types].xml", "<Types/>"), (named, "<a>" + "x" * 40 + "</a>")], zipfile.ZIP_STORED))
    damaged[damaged.index(b"x" * 40)] ^= 0xFF
    clean = {"[Content_Types].xml": "<Types/>", "xl/workbook.xml": "<workbook/>"}
    return [
        Case(
            "workbook holding a user directory path is blocked by member",
            "privacy-scan",
            False,
            {"config/book.xlsx": office_package(clean | {"xl/workbook.xml": f"<workbook>{block}</workbook>"})},
            reason=reason,
        ),
        Case(
            "workbook member named with a user directory path is blocked without printing the name",
            "privacy-scan",
            False,
            {"config/book.xlsx": office_records([("[Content_Types].xml", "<Types/>"), (named, f"<a>{path}</a>")])},
            reason="config/book.xlsx!member 2 (xml): home-directory path with a user name",
        ),
        Case(
            "workbook with an unreadable member named with a user directory path is blocked without printing the name",
            "privacy-scan",
            False,
            {"config/book.xlsx": bytes(damaged)},
            reason="config/book.xlsx: unreadable document: member 2 cannot be read",
        ),
        Case(
            "workbook with a damaged LZMA member is blocked without a traceback",
            "privacy-scan",
            False,
            {"config/book.xlsx": damaged_package("lzma")},
            reason="config/book.xlsx: unreadable document: member 1 cannot be read",
        ),
        Case(
            "workbook that needs a newer ZIP version is blocked without a traceback",
            "privacy-scan",
            False,
            {"config/book.xlsx": damaged_package("version")},
            reason="config/book.xlsx: unreadable document: an Office package cannot be read as a ZIP archive",
        ),
        Case(
            "workbook with a malformed UTF-8 member name is blocked without a traceback",
            "privacy-scan",
            False,
            {"config/book.xlsx": damaged_package("utf8")},
            reason="config/book.xlsx: unreadable document: an Office package cannot be read as a ZIP archive",
        ),
        Case(
            "workbook with a path in the first of two members of one name is blocked",
            "privacy-scan",
            False,
            {"config/book.xlsx": office_records([("xl/workbook.xml", f"<a>{path}</a>"), ("xl/workbook.xml", "<workbook/>")])},
            reason="config/book.xlsx: unreadable document: an Office package holds two members with the same name",
        ),
        Case(
            "workbook with two clean members of one name is blocked",
            "privacy-scan",
            False,
            {"config/book.xlsx": office_records([("xl/workbook.xml", "<workbook/>"), ("xl/workbook.xml", "<other/>")])},
            reason="config/book.xlsx: unreadable document: an Office package holds two members with the same name",
        ),
        Case(
            "legacy workbook holding a UTF-16 user directory path is blocked",
            "privacy-scan",
            False,
            {"config/book.xls": b"\xd0\xcf\x11\xe0 " + path.encode("utf-16-le")},
            reason="config/book.xls!(file): home-directory path with a user name",
        ),
        Case(
            "PDF holding a user directory path in a text stream is blocked",
            "privacy-scan",
            False,
            {"config/doc.pdf": pdf_document(f"BT ({path.replace(chr(92), chr(92) * 2)}) Tj ET")},
            reason="config/doc.pdf!(stream): home-directory path with a user name",
        ),
        Case("clean PDF passes", "privacy-scan", True, {"config/doc.pdf": pdf_document("BT (hospital linkage) Tj ET")}),
        Case(
            "PDF with a placeholder path passes", "privacy-scan", True, {"config/doc.pdf": pdf_document("BT (C:\\\\" + "Users\\\\<user>\\\\book.xlsx) Tj ET")}
        ),
        Case("clean workbook passes", "privacy-scan", True, {"config/book.xlsx": office_package(clean)}),
        Case(
            "workbook with a placeholder path passes",
            "privacy-scan",
            True,
            {"config/book.xlsx": office_package(clean | {"xl/workbook.xml": "<a>C:\\" + "Users\\<user>\\book.xlsx</a>"})},
        ),
    ]


def account_cases() -> list[Case]:
    """The exemption cases: the one synthetic ID passes in both contexts and every other 12-digit ID is flagged in both."""
    cases = [
        Case("synthetic account ARN is exempt", "privacy-scan", True, {"infra/policy.json": f'{{"Resource": "{account_arn(EXEMPT_ACCOUNT)}"}}\n'}),
        Case("synthetic account ID field is exempt", "privacy-scan", True, {"tests/test_config.py": account_field(EXEMPT_ACCOUNT)}),
        Case(
            "synthetic account beside another account",
            "privacy-scan",
            False,
            {"infra/policy.json": f'{{"Resource": ["{account_arn(EXEMPT_ACCOUNT)}", "{account_arn(OTHER_ACCOUNTS["all-two"])}"]}}\n'},
        ),
        Case(
            "exempt account first in a conditional assignment",
            "privacy-scan",
            False,
            {"tests/test_config.py": account_conditional(EXEMPT_ACCOUNT, OTHER_ACCOUNTS["all-two"])},
        ),
        Case(
            "exempt account ARN beside a bare other account",
            "privacy-scan",
            False,
            {"infra/policy.json": f'{{"Resource": "{account_arn(EXEMPT_ACCOUNT)}", "Other": "{OTHER_ACCOUNTS["all-two"]}"}}\n'},
        ),
        Case(
            "exempt account twice on one line",
            "privacy-scan",
            True,
            {"tests/test_config.py": account_conditional(EXEMPT_ACCOUNT, EXEMPT_ACCOUNT)},
        ),
        Case(
            "exempt account with a decimal number beside it",
            "privacy-scan",
            True,
            {"tests/test_config.py": f'ACCOUNT, RATIO = "{EXEMPT_ACCOUNT}", 0.{OTHER_ACCOUNTS["sequential"]}\n'},
        ),
        Case(
            "exempt account ARN with a UUID resource",
            "privacy-scan",
            True,
            {"infra/policy.json": f'{{"Resource": "arn:aws:autoscaling:eu-west-1:{EXEMPT_ACCOUNT}:group:00000000-0000-0000-0000-{"0" * 12}:name"}}\n'},
        ),
        Case(
            "synthetic account declared in .env is exempt",
            "privacy-scan",
            True,
            {".env": f"AWS_ACCOUNT_ID={EXEMPT_ACCOUNT}\n", "tests/test_config.py": account_field(EXEMPT_ACCOUNT)},
        ),
        Case(
            "real account declared in .env",
            "privacy-scan",
            False,
            {".env": f"AWS_ACCOUNT_ID={OTHER_ACCOUNTS['all-two']}\n", "scripts/deploy.sh": f"echo {OTHER_ACCOUNTS['all-two']}\n"},
        ),
    ]
    for label, account in OTHER_ACCOUNTS.items():
        if label != "sequential":  # The sequential ID has the plain cases "AWS account ARN" and "AWS account ID field".
            cases.append(Case(f"{label} account ARN", "privacy-scan", False, {"infra/policy.json": f'{{"Resource": "{account_arn(account)}"}}\n'}))
            cases.append(Case(f"{label} account ID field", "privacy-scan", False, {"tests/test_config.py": account_field(account)}))
    return cases


# A code-version record lists the code hashes of a stored capture: each fingerprint's file path is history, so a removed script stays named there.
VERSIONS_RECORD = "config/acquisition/sample_code_versions.json"
STILL_LIVE = "scripts/still_live.py"


def code_version_record(fingerprint_file: str, reason: str = "First reviewed version", entry_note: str | None = None) -> str:
    """One record in the shape of the collectors' files: versions[].code_sha256[] with a file and a sha256 each."""
    entry = {"file": fingerprint_file, "sha256": "0" * 64} | ({} if entry_note is None else {"note": entry_note})
    return json.dumps({"versions": [{"version": 1, "code_sha256": [entry], "reason": reason}]}, indent=2) + "\n"


def schema_file(*properties: str, schema_id: str = "") -> str:
    """A JSON schema that defines each of ``properties`` and, with ``schema_id``, an ``$id``."""
    document = {"type": "object", "properties": {name: {"type": "integer"} for name in properties}}
    return json.dumps(({"$id": schema_id} if schema_id else {}) | document, indent=2) + "\n"


def argument_file(name: str) -> str:
    return f'resource "aws_db_subnet_group" "main" {{\n  {name} = ["a"]\n}}\n'


# A name counts as removed only when no remaining file defines or uses it as its own; a mention that refers to the deleted thing still blocks.
FIRST_SCHEMA = "config/fir" + "st_row.schema.json"
SECOND_SCHEMA = "config/second_row.schema.json"
SCHEMA_FILE = f"config/{SCHEMA_STEM}.schema.json"
IN_USE_CASES = [
    Case(
        "schema property removed and Terraform file deleted, the same word is a Terraform argument elsewhere",
        "removed-names",
        True,
        committed={
            "config/jobs.schema.json": schema_file(SHARED_ARGUMENT, "region"),
            "infra/probe/variables.tf": f'variable "network" {{\n  type = object({{\n    {SHARED_ARGUMENT} = list(string)\n  }})\n}}\n',
            "infra/iam/network.tf": argument_file(SHARED_ARGUMENT),
        },
        files={"config/jobs.schema.json": schema_file("region")},
        delete=["infra/probe/variables.tf"],
    ),
    Case(
        "Terraform variable file deleted, the same word is a Terraform argument elsewhere",
        "removed-names",
        True,
        committed={"infra/variables.tf": f'variable "{SHARED_ARGUMENT}" {{}}\n', "infra/network.tf": argument_file(SHARED_ARGUMENT)},
        delete=["infra/variables.tf"],
    ),
    Case(
        "schema property removed, the same word is a Terraform block label elsewhere",
        "removed-names",
        True,
        committed={
            "config/jobs.schema.json": schema_file(SHARED_ARGUMENT, "region"),
            "infra/network.tf": f'resource "aws_db_subnet_group" "main" {{\n  {SHARED_ARGUMENT} {{\n    size = 1\n  }}\n}}\n',
        },
        files={"config/jobs.schema.json": schema_file("region")},
    ),
    Case(
        "schema deleted, another schema still defines its property",
        "removed-names",
        True,
        committed={FIRST_SCHEMA: schema_file(SHARED_FIELD), SECOND_SCHEMA: schema_file(SHARED_FIELD)},
        delete=[FIRST_SCHEMA],
    ),
    Case(
        "schema property removed, another schema still defines it",
        "removed-names",
        True,
        committed={FIRST_SCHEMA: schema_file(SHARED_FIELD, "region"), SECOND_SCHEMA: schema_file(SHARED_FIELD)},
        files={FIRST_SCHEMA: schema_file("region")},
    ),
    Case(
        "schema property removed, a Python field of the same name is still declared",
        "removed-names",
        True,
        committed={
            FIRST_SCHEMA: schema_file(SHARED_FIELD, "region"),
            "app/row.py": f"from dataclasses import dataclass\n\n\n@dataclass\nclass Row:\n    {SHARED_FIELD}: int\n",
        },
        files={FIRST_SCHEMA: schema_file("region")},
    ),
    Case(
        "schema property removed, a Python parameter of the same name is still declared",
        "removed-names",
        True,
        committed={FIRST_SCHEMA: schema_file(SHARED_FIELD, "region"), "app/start.py": f"def start(role: str, {SHARED_FIELD}: int) -> str:\n    return role\n"},
        files={FIRST_SCHEMA: schema_file("region")},
    ),
    Case(
        "schema property removed, the same word only names a data file",
        "removed-names",
        True,
        committed={FIRST_SCHEMA: schema_file(DATA_FILE_WORD, "region"), "app/paths.py": f'STORE = "authority/{DATA_FILE_WORD}.json"\n'},
        files={FIRST_SCHEMA: schema_file("region")},
    ),
    Case(
        "schema property removed, code still reads it as a string key",
        "removed-names",
        False,
        committed={FIRST_SCHEMA: schema_file(SHARED_FIELD, "region"), "app/job.py": f'ROW = {{}}\nDEADLINE = ROW["{SHARED_FIELD}"]\n'},
        files={FIRST_SCHEMA: schema_file("region")},
        reason=f"app/job.py:2: {SHARED_FIELD} {LEFTOVER}",
    ),
    Case(
        "deleted module, its function name is still defined in another module",
        "removed-names",
        True,
        committed={**DEAD_MODULE, "app/other.py": UTIL_FUNCTION},
        delete=[DEAD_FILE],
    ),
    Case(
        "deleted module, an import of it remains and its function name is also defined elsewhere",
        "removed-names",
        False,
        committed={**DEAD_MODULE, "app/other.py": UTIL_FUNCTION, "app/use.py": f"from {DEAD_PATH} import {OLD_FUNCTION}\n\nVALUE: int = {OLD_FUNCTION}()\n"},
        delete=[DEAD_FILE],
        reason=f"app/use.py:1: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "deleted module, a document line names its path",
        "removed-names",
        False,
        committed={**DEAD_MODULE, "docs/guide.md": f"The totals live in `{DEAD_FILE}`.\n"},
        delete=[DEAD_FILE],
        reason=f"docs/guide.md:1: {DEAD_STEM}.py {LEFTOVER}",
    ),
    Case(
        "deleted module, a document line names its dotted path",
        "removed-names",
        False,
        committed={**DEAD_MODULE, "docs/guide.md": f"Run `python -m {DEAD_PATH}` for the totals.\n"},
        delete=[DEAD_FILE],
        reason=f"docs/guide.md:1: {DEAD_PATH} {LEFTOVER}",
    ),
    Case(
        "schema deleted, nothing names its file or its id",
        "removed-names",
        True,
        committed={SCHEMA_FILE: schema_file("region", schema_id=SCHEMA_ID)},
        delete=[SCHEMA_FILE],
    ),
    Case(
        "schema deleted, a path to its file remains",
        "removed-names",
        False,
        committed={SCHEMA_FILE: schema_file("region", schema_id=SCHEMA_ID), "app/load.py": f'SCHEMA = "{SCHEMA_FILE}"\n'},
        delete=[SCHEMA_FILE],
        reason=f"app/load.py:1: {SCHEMA_STEM}.schema.json {LEFTOVER}",
    ),
    Case(
        "schema deleted, a reference to its id remains",
        "removed-names",
        False,
        committed={SCHEMA_FILE: schema_file("region", schema_id=SCHEMA_ID), SECOND_SCHEMA: json.dumps({"$ref": SCHEMA_ID}, indent=2) + "\n"},
        delete=[SCHEMA_FILE],
        reason=f"{SECOND_SCHEMA}:2: {SCHEMA_ID} {LEFTOVER}",
    ),
    Case(
        "schema deleted, a document line names its file",
        "removed-names",
        False,
        committed={SCHEMA_FILE: schema_file("region", schema_id=SCHEMA_ID), "docs/guide.md": f"The record shape is `{SCHEMA_STEM}.schema.json`.\n"},
        delete=[SCHEMA_FILE],
        reason=f"docs/guide.md:1: {SCHEMA_STEM}.schema.json {LEFTOVER}",
    ),
    *(
        Case(
            f"schema property removed, {label}, code still reads it",
            "removed-names",
            False,
            committed={FIRST_SCHEMA: schema_file(RETIRED_KEY, "region"), "app/reader.py": reader, **extra},
            files={FIRST_SCHEMA: schema_file("region")},
            reason=f"app/reader.py:1: {RETIRED_KEY} {LEFTOVER}",
        )
        for label, reader, extra in (
            (
                "a Terraform heredoc only quotes the same word",
                f'VALUE = RECORD["{RETIRED_KEY}"]\n',
                {"infra/main.tf": f'locals {{\n  example = <<-EOT\n{RETIRED_KEY} = old_record["{RETIRED_KEY}"]\nEOT\n}}\n'},
            ),
            ("a Terraform block comment only quotes the same word", f'VALUE = RECORD["{RETIRED_KEY}"]\n', {"infra/main.tf": f"/*\n{RETIRED_KEY} {{\n}}\n*/\n"}),
            (
                "Terraform line comments only quote the same word",
                f'VALUE = RECORD["{RETIRED_KEY}"]\n',
                {"infra/main.tf": f"# {RETIRED_KEY} = 1\n// {RETIRED_KEY} {{\n"},
            ),
            ("a Python attribute read is followed by a method", f"VALUE = response.{RETIRED_KEY}.json()\n", {}),
            (
                "a YAML block scalar only quotes the same word",
                f'VALUE = RECORD["{RETIRED_KEY}"]\n',
                {"config/example.yaml": f"example: |\n  {RETIRED_KEY}: old record field\n"},
            ),
            (
                "a Python docstring that does not parse only quotes the same word",
                f'VALUE = RECORD["{RETIRED_KEY}"]\n',
                {"app/broken.py": f'"""Example:\n{RETIRED_KEY} = 1\n"""\ndef unfinished(\n'},
            ),
        )
    ),
    *(
        Case(
            f"schema property removed, {label}",
            "removed-names",
            True,
            committed={FIRST_SCHEMA: schema_file(RETIRED_KEY, "region"), **declaration},
            files={FIRST_SCHEMA: schema_file("region")},
        )
        for label, declaration in (
            ("a Terraform argument follows a comment", {"infra/main.tf": f'# note\nresource "aws_db_subnet_group" "main" {{\n  {RETIRED_KEY} = 1\n}}\n'}),
            ("another schema defines it on one line", {SECOND_SCHEMA: json.dumps({"properties": {RETIRED_KEY: {"type": "integer"}}})}),
            ("another file defines it inside an array of objects", {"config/rows.json": json.dumps({"rows": [{RETIRED_KEY: 1}]})}),
            ("a YAML flow mapping defines it", {"config/flow.yaml": f"settings: {{{RETIRED_KEY}: 4}}\n"}),
            ("a TOML inline table defines it", {"config/inline.toml": f"[tool]\nvalues = {{{RETIRED_KEY} = 4}}\n"}),
        )
    ),
    Case(
        "schema id renamed in a schema that stays, a reference to the old id remains",
        "removed-names",
        False,
        committed={FIRST_SCHEMA: schema_file("region", schema_id=PRIOR_ID), "config/use.json": json.dumps({"$ref": PRIOR_ID}, indent=2)},
        files={FIRST_SCHEMA: schema_file("region", schema_id=RENAMED_ID)},
        reason=f"config/use.json:2: {PRIOR_ID} {LEFTOVER}",
    ),
    Case(
        "schema id kept while another property is edited, a reference to the id remains",
        "removed-names",
        True,
        committed={FIRST_SCHEMA: schema_file("region", "zone", schema_id=PRIOR_ID), "config/use.json": json.dumps({"$ref": PRIOR_ID}, indent=2)},
        files={FIRST_SCHEMA: schema_file("region", schema_id=PRIOR_ID)},
    ),
    Case(
        "deleted schema stores its id with a JSON escape, a reference to the plain id remains",
        "removed-names",
        False,
        committed={FIRST_SCHEMA: f'{{"$id": "{ESCAPED_PRIOR_ID}", "type": "object"}}\n', "config/use.json": json.dumps({"$ref": PRIOR_ID}, indent=2)},
        delete=[FIRST_SCHEMA],
        reason=f"config/use.json:2: {PRIOR_ID} {LEFTOVER}",
    ),
    Case(
        "deleted schema stores its plain id, a reference spells the id with a JSON escape",
        "removed-names",
        False,
        committed={FIRST_SCHEMA: schema_file("region", schema_id=PRIOR_ID), "config/use.json": f'{{\n  "$ref": "{ESCAPED_PRIOR_ID}"\n}}\n'},
        delete=[FIRST_SCHEMA],
        reason=f"config/use.json:2: {PRIOR_ID} {LEFTOVER}",
    ),
    *(
        Case(
            f"schema property removed, {label}",
            "removed-names",
            expect_pass,
            committed={FIRST_SCHEMA: schema_file(RETIRED_KEY, "region"), **extra},
            files={FIRST_SCHEMA: schema_file("region")},
            reason="" if expect_pass else f"{RETIRED_KEY} {LEFTOVER}",
        )
        for label, expect_pass, extra in (
            (
                "an unparsable Python file holds the word in an assigned multiline string, code still reads it",
                False,
                {"app/example.py": f'EXAMPLE = """\n{RETIRED_KEY} = 4\n"""\ndef broken(\n', "app/reader.py": f'VALUE = RECORD["{RETIRED_KEY}"]\n'},
            ),
            ("an attribute method call with a space follows the word", False, {"app/reader.py": f"VALUE = response.{RETIRED_KEY}.json ()\n"}),
            ("an attribute member named like a file suffix follows the word", False, {"app/reader.py": f"VALUE = response.{RETIRED_KEY}.json\n"}),
            (
                "a Terraform comment opens inside a line and hides the word",
                False,
                {"infra/main.tf": f"locals {{\n  x = 4 /*\n  {RETIRED_KEY} = 4\n  */\n}}\n", "app/reader.py": f'VALUE = RECORD["{RETIRED_KEY}"]\n'},
            ),
            ("a Python dict literal writes it", False, {"app/writer.py": f'RECORD = {{"{RETIRED_KEY}": 4}}\n'}),
            (
                "a Python constructor keyword writes it",
                False,
                {"app/writer.py": f"from types import SimpleNamespace\nRECORD = SimpleNamespace({RETIRED_KEY}=4)\n"},
            ),
            (
                "an independent JSON key is spelled with an escape",
                True,
                {"config/live.json": '{"retired_magic\\u005fkey": 4}\n', "app/reader.py": f'VALUE = RECORD["{RETIRED_KEY}"]\n'},
            ),
            (
                "an independent YAML key is spelled with an escape",
                True,
                {"config/live.yaml": '"retired_magic\\u005fkey": 4\n', "app/reader.py": f'VALUE = RECORD["{RETIRED_KEY}"]\n'},
            ),
            (
                "an independent TOML key is spelled with an escape",
                True,
                {"config/live.toml": '"retired_magic\\u005fkey" = 4\n', "app/reader.py": f'VALUE = RECORD["{RETIRED_KEY}"]\n'},
            ),
            (
                "a Terraform argument follows a quoted hash",
                True,
                {"infra/main.tf": f'locals {{\n  url = "x#y"\n  {RETIRED_KEY} = 4\n}}\n', "app/reader.py": f'VALUE = RECORD["{RETIRED_KEY}"]\n'},
            ),
            (
                "a Terraform argument follows a quoted slash pair",
                True,
                {
                    "infra/main.tf": f'locals {{\n  url = "https://example.invalid"\n  {RETIRED_KEY} = 4\n}}\n',
                    "app/reader.py": f'VALUE = RECORD["{RETIRED_KEY}"]\n',
                },
            ),
        )
    ),
    Case(
        "compact retained schema loses a property, code still reads it",
        "removed-names",
        False,
        committed={
            FIRST_SCHEMA: json.dumps({"properties": {"region": {"type": "string"}, RETIRED_KEY: {"type": "integer"}}}),
            "app/reader.py": f'VALUE = RECORD["{RETIRED_KEY}"]\n',
        },
        files={FIRST_SCHEMA: json.dumps({"properties": {"region": {"type": "string"}}})},
        reason=f"{RETIRED_KEY} {LEFTOVER}",
    ),
    Case(
        "compact configuration loses its second key, code still reads it",
        "removed-names",
        False,
        committed={"config/settings.json": f'{{"region": "west", "{RETIRED_KEY}": 4}}\n', "app/reader.py": f'VALUE = RECORD["{RETIRED_KEY}"]\n'},
        files={"config/settings.json": '{"region": "west"}\n'},
        reason=f"{RETIRED_KEY} {LEFTOVER}",
    ),
    *(
        Case(
            f"Terraform variable replaced by {label}, another file still reads it",
            "removed-names",
            False,
            committed={"infra/variables.tf": f'variable "{RETIRED_KEY}" {{}}\n', "infra/reader.tf": f"locals {{\n  x = var.{RETIRED_KEY}\n}}\n"},
            files={"infra/variables.tf": text},
            reason=f"{RETIRED_KEY} {LEFTOVER}",
        )
        for label, text in (("a line comment", f'# variable "{RETIRED_KEY}" {{}}\n'), ("a block comment", f'/*\nvariable "{RETIRED_KEY}" {{}}\n*/\n'))
    ),
    *(
        Case(
            f"removed Python function, {label} cannot declare",
            "removed-names",
            False,
            committed={
                "app/retired.py": f"def {RETIRED_KEY}():\n    return 1\n",
                "app/example.py": text,
                PENDING_LIST: PENDING_FIXTURE,
                "dags/pending_dag.py": f"from app.retired import {RETIRED_KEY}\nVALUE = {RETIRED_KEY}()\n",
            },
            files={"app/retired.py": "VALUE = 1\n"},
            reason=f"{RETIRED_KEY} {LEFTOVER}",
        )
        for label, text in (
            ("a docstring", f'"""Example:\ndef {RETIRED_KEY}():\n    pass\n"""\n'),
            ("an assigned multiline string", f'EXAMPLE = """\ndef {RETIRED_KEY}():\n    pass\n"""\n'),
        )
    ),
    *(
        Case(
            f"removed Python function, an independent {label} declaration keeps it live",
            "removed-names",
            True,
            committed={
                "app/retired.py": f"def {RETIRED_KEY}():\n    return 1\n",
                path: text,
                PENDING_LIST: PENDING_FIXTURE,
                "dags/pending_dag.py": f"VALUE = {RETIRED_KEY}\n",
            },
            files={"app/retired.py": "VALUE = 1\n"},
        )
        for label, path, text in (
            ("YAML", "config/live.yaml", f"{RETIRED_KEY}: 4\n"),
            ("Terraform", "infra/live.tf", f"locals {{\n  {RETIRED_KEY} = 4\n}}\n"),
        )
    ),
    Case(
        "schema id moved to another schema that stays, a reference to it remains",
        "removed-names",
        True,
        committed={FIRST_SCHEMA: schema_file("region", schema_id=PRIOR_ID), "config/use.json": json.dumps({"$ref": PRIOR_ID}, indent=2)},
        files={FIRST_SCHEMA: schema_file("region", schema_id=RENAMED_ID), SECOND_SCHEMA: schema_file("region", schema_id=PRIOR_ID)},
    ),
    Case(
        "deleted schema id is declared by another schema, a reference to it remains",
        "removed-names",
        True,
        committed={
            FIRST_SCHEMA: schema_file("region", schema_id=PRIOR_ID),
            SECOND_SCHEMA: schema_file("zone", schema_id=PRIOR_ID),
            "config/use.json": json.dumps({"$ref": PRIOR_ID}, indent=2),
        },
        delete=[FIRST_SCHEMA],
    ),
    Case(
        "removing a Python string example removes no definition",
        "removed-names",
        True,
        committed={
            "app/example.py": f'EXAMPLE = """\ndef {RETIRED_KEY}():\n    pass\n"""\n',
            "app/other_example.py": f'EXAMPLE = """\ndef {RETIRED_KEY}():\n    pass\n"""\n',
            PENDING_LIST: PENDING_FIXTURE,
            "dags/pending_dag.py": f"from external_library import {RETIRED_KEY}\nVALUE = {RETIRED_KEY}()\n",
        },
        files={"app/example.py": 'EXAMPLE = ""\n'},
    ),
    Case(
        "schema property removed, a Python string after a tokenizer error cannot declare it, code still reads it",
        "removed-names",
        False,
        committed={
            FIRST_SCHEMA: schema_file(RETIRED_KEY, "region"),
            "app/example.py": f'if True:\n    x = 1\n  y = 2\nEXAMPLE = """\n{RETIRED_KEY} = 4\n"""\n',
            "app/reader.py": f'VALUE = RECORD["{RETIRED_KEY}"]\n',
        },
        files={FIRST_SCHEMA: schema_file("region")},
        reason=f"{RETIRED_KEY} {LEFTOVER}",
    ),
]
CODE_VERSION_BODY = code_version_record(f"scripts/{OLD_SCRIPT}")
CODE_VERSION_CASES = [
    Case(
        "script removed, named only in a code-version record",
        "removed-names",
        True,
        committed={f"scripts/{OLD_SCRIPT}": "X = 1\n", VERSIONS_RECORD: CODE_VERSION_BODY},
        delete=[f"scripts/{OLD_SCRIPT}"],
    ),
    Case(
        "script removed, named in another file of the same folder",
        "removed-names",
        False,
        committed={f"scripts/{OLD_SCRIPT}": "X = 1\n", "config/acquisition/sample_notes.json": CODE_VERSION_BODY},
        delete=[f"scripts/{OLD_SCRIPT}"],
        reason=LEFTOVER,
    ),
    Case(
        "script removed, named in a code-version record outside the collectors' folder",
        "removed-names",
        False,
        committed={f"scripts/{OLD_SCRIPT}": "X = 1\n", "config/sample_code_versions.json": CODE_VERSION_BODY},
        delete=[f"scripts/{OLD_SCRIPT}"],
        reason=LEFTOVER,
    ),
    Case(
        "script removed, named in a code-version record and in the docs",
        "removed-names",
        False,
        committed={f"scripts/{OLD_SCRIPT}": "X = 1\n", VERSIONS_RECORD: CODE_VERSION_BODY, "README.md": f"Run `scripts/{OLD_SCRIPT}`.\n"},
        delete=[f"scripts/{OLD_SCRIPT}"],
        reason=LEFTOVER,
    ),
    Case(
        "script removed, named in the reason of a code-version record",
        "removed-names",
        False,
        committed={f"scripts/{OLD_SCRIPT}": "X = 1\n", VERSIONS_RECORD: code_version_record(STILL_LIVE, reason=f"Run scripts/{OLD_SCRIPT}")},
        delete=[f"scripts/{OLD_SCRIPT}"],
        reason=LEFTOVER,
    ),
    Case(
        "script removed, named in another field of a fingerprint entry",
        "removed-names",
        False,
        committed={f"scripts/{OLD_SCRIPT}": "X = 1\n", VERSIONS_RECORD: code_version_record(STILL_LIVE, entry_note=f"scripts/{OLD_SCRIPT}")},
        delete=[f"scripts/{OLD_SCRIPT}"],
        reason=LEFTOVER,
    ),
    Case(
        "script removed, named in a file with a record's name that holds other JSON",
        "removed-names",
        False,
        committed={f"scripts/{OLD_SCRIPT}": "X = 1\n", VERSIONS_RECORD: json.dumps({"command": f"python scripts/{OLD_SCRIPT}"}) + "\n"},
        delete=[f"scripts/{OLD_SCRIPT}"],
        reason=LEFTOVER,
    ),
]


SHIPPED_DAG = "silver" + "_transform_dag"
MADE_UP_DAG = "fixture" + "_dag"
STALE_TASK = "silver" + "_build"
REAL_TASK = "job"
TRIGGER_DAG = "features" + "_base_dag"
TRIGGER_TASK = "trigger" + "_m1"
TASK_IDS_FILES = ("orchestration/__init__.py", "orchestration/graph.py", "orchestration/training_contract.py", "orchestration/interface.py")
STALE_JSON = f'{{\n  "dag_id": "{SHIPPED_DAG}",\n  "task_id": "{STALE_TASK}"\n}}\n'

NESTED_JSON = "\n".join(
    ["{", '  "rows": [', '    {"run": 1},', "    {", f'      "dag_id": "{SHIPPED_DAG}",', f'      "task_id": "{STALE_TASK}"', "    }", "  ]", "}", ""]
)

OUTER_JSON = "\n".join(
    ["{", f'  "inner": {{"dag_id": "{SHIPPED_DAG}", "task_id": "{REAL_TASK}"}},', f'  "dag_id": "{SHIPPED_DAG}",', f'  "task_id": "{STALE_TASK}"', "}", ""]
)


def stale_reason(path: str, line: int, dag: str = SHIPPED_DAG, task: str = STALE_TASK) -> str:
    """The start of the finding the task ID check prints for a pair of ``dag`` and ``task`` at ``path:line``."""
    return f"{path}:{line}: task '{task}' is not a task of {dag}"


def task_case(name: str, files: dict[str, str | bytes], reason: str = "") -> Case:
    """A task-ids case: it passes when ``reason`` is empty and is blocked for that finding otherwise."""
    return Case(f"task ID, {name}", "task-ids", not reason, files, reason=reason)


TASK_ID_CASES = [
    task_case("stale pair in JSON", {"config/jobs.json": STALE_JSON}, stale_reason("config/jobs.json", 3)),
    task_case(
        "stale pair in a nested JSON object",
        {"data_contracts/ledger.json": NESTED_JSON},
        stale_reason("data_contracts/ledger.json", 6),
    ),
    task_case(
        "stale pair in a Python dict",
        {"app/request.py": f'REQUEST = {{"dag_id": "{SHIPPED_DAG}", "task_id": "{STALE_TASK}"}}\n'},
        stale_reason("app/request.py", 1),
    ),
    task_case(
        "stale pair in keyword arguments", {"app/launch.py": f'launch(dag_id="{SHIPPED_DAG}", task_id="{STALE_TASK}")\n'}, stale_reason("app/launch.py", 1)
    ),
    task_case(
        "stale pair in a positional SubmissionRequest",
        {"app/submit.py": f'SubmissionRequest("t", "r", "fixture", 1, "{SHIPPED_DAG}", "{STALE_TASK}", "manual__r")\n'},
        stale_reason("app/submit.py", 1),
    ),
    task_case("pair of a DAG that is not shipped", {"config/jobs.json": STALE_JSON.replace(SHIPPED_DAG, MADE_UP_DAG)}),
    task_case("real task of a shipped DAG in JSON", {"config/jobs.json": STALE_JSON.replace(STALE_TASK, REAL_TASK)}),
    task_case(
        "real task of a shipped DAG in a positional SubmissionRequest",
        {"app/submit.py": f'SubmissionRequest("t", "r", "fixture", 1, "{SHIPPED_DAG}", "{REAL_TASK}", "manual__r")\n'},
    ),
    task_case("trigger task of a DAG that has triggers", {"app/launch.py": f'launch(dag_id="{TRIGGER_DAG}", task_id="{TRIGGER_TASK}")\n'}),
    task_case(
        "a trigger task of another DAG is stale",
        {"app/launch.py": f'launch(dag_id="{SHIPPED_DAG}", task_id="{TRIGGER_TASK}")\n'},
        stale_reason("app/launch.py", 1, task=TRIGGER_TASK),
    ),
    task_case(
        "values that are not literals",
        {"app/launch.py": f'launch(dag_id="{SHIPPED_DAG}", task_id=name)\nREQUEST = {{"dag_id": dag, "task_id": "{STALE_TASK}"}}\n'},
    ),
    task_case(
        "stale pair in an object under a key",
        {"config/holder.json": f'{{\n  "holder": {{"dag_id": "{SHIPPED_DAG}", "task_id": "{STALE_TASK}"}}\n}}\n'},
        stale_reason("config/holder.json", 2),
    ),
    task_case(
        "empty task ID in a Python dict",
        {"app/request.py": f'REQUEST = {{"dag_id": "{SHIPPED_DAG}", "task_id": ""}}\n'},
        stale_reason("app/request.py", 1, task=""),
    ),
    task_case(
        "empty task ID in keyword arguments", {"app/launch.py": f'launch(dag_id="{SHIPPED_DAG}", task_id="")\n'}, stale_reason("app/launch.py", 1, task="")
    ),
    task_case("empty task ID in JSON", {"config/jobs.json": STALE_JSON.replace(STALE_TASK, "")}, stale_reason("config/jobs.json", 3, task="")),
    task_case(
        "Python source in a declared Latin-1 encoding",
        {"app/record.py": f'# coding: latin-1\n# caf\xe9\nrecord = {{"dag_id": "{SHIPPED_DAG}", "task_id": "{STALE_TASK}"}}\n'.encode("latin-1")},
        stale_reason("app/record.py", 3),
    ),
    task_case(
        "Python file with a non-ASCII path",
        {"app/caf\u00e9.py": f'record = {{"dag_id": "{SHIPPED_DAG}", "task_id": "{STALE_TASK}"}}\n'},
        stale_reason("app/caf\u00e9.py", 1),
    ),
    task_case(
        "two compact JSON objects on one line",
        {"config/compact.json": f'\n\n[{{"task_id":"neutral","dag_id":"{MADE_UP_DAG}"}}, {{"dag_id":"{SHIPPED_DAG}","task_id":"{STALE_TASK}"}}]\n'},
        stale_reason("config/compact.json", 3),
    ),
    task_case(
        "an outer object whose task ID follows a nested object",
        {
            "config/outer.json": OUTER_JSON,
        },
        stale_reason("config/outer.json", 4),
    ),
    task_case(
        "a JSON object whose line cannot be known is named by its path",
        {"config/twice.json": f'{{"rows": [{{"run": 1}}, {{"dag_id": "{SHIPPED_DAG}", "task_id": "{REAL_TASK}", "task_id": "{STALE_TASK}"}}]}}\n'},
        f"config/twice.json (rows[1]): task '{STALE_TASK}' is not a task of {SHIPPED_DAG}",
    ),
    task_case("Python file that does not parse", {"app/broken.py": f'launch(dag_id="{SHIPPED_DAG}", task_id="{STALE_TASK}"\n'}),
    task_case("JSON file that does not parse", {"config/broken.json": STALE_JSON.replace("\n}", ",\n")}),
]


DIGEST_SOURCE = f'import argparse\n\nP = argparse.ArgumentParser()\nP.{ADD_ARGUMENT}("{SIGN_OFF}")\n'
DIGEST_EDITED = "import argparse\n\nP = argparse.ArgumentParser()\n"
REPORT = "tools/run_report.sh"
RETIRED = "retired" + "_digest.py"


def flag_case(name: str, caller: str, reason: str = "", files: dict[str, str | bytes] | None = None, committed: dict[str, str | bytes] | None = None) -> Case:
    """A removed-names case: ``tools/digest.py`` loses its flag and ``tools/run_report.sh`` holds ``caller``; ``reason`` blocks it."""
    return Case(
        f"flag removed, {name}",
        "removed-names",
        not reason,
        committed={"tools/digest.py": DIGEST_SOURCE, REPORT: caller, **(committed or {})},
        files={"tools/digest.py": DIGEST_EDITED} if files is None else files,
        reason=reason,
    )


def flag_cases() -> list[Case]:
    """The flag rule: a flag that an edited script lost counts only on a line that also names that script, and a deleted script gives no flag."""
    other: dict[str, str | bytes] = {"tools/other.py": DIGEST_EDITED}
    moved: dict[str, str | bytes] = {"tools/digest.py": DIGEST_EDITED, "tools/other.py": DIGEST_SOURCE}
    return [
        flag_case(
            "a command that names the script by its path still passes it",
            f"python tools/digest.py {SIGN_OFF} /work/a.json\n",
            f"{REPORT}:1: {SIGN_OFF} {LEFTOVER}",
        ),
        flag_case(
            "a command that names the script by its file name still passes it", f"cd tools && digest.py {SIGN_OFF}\n", f"{REPORT}:1: {SIGN_OFF} {LEFTOVER}"
        ),
        flag_case(
            "a command that names the script by its module still passes it", f"python -m tools.digest {SIGN_OFF}\n", f"{REPORT}:1: {SIGN_OFF} {LEFTOVER}"
        ),
        flag_case("the same text in another program's command", f"report_tool {SIGN_OFF} /work/a.json\n"),
        flag_case(
            "a command that names the script by a relative path still passes it", f"python ./tools/digest.py {SIGN_OFF}\n", f"{REPORT}:1: {SIGN_OFF} {LEFTOVER}"
        ),
        flag_case(
            "a command that names the script by an absolute path still passes it",
            f"python /opt/work/tools/digest.py {SIGN_OFF}\n",
            f"{REPORT}:1: {SIGN_OFF} {LEFTOVER}",
        ),
        flag_case(
            "a command that names the script by its file name with a dot slash still passes it",
            f"cd tools && ./digest.py {SIGN_OFF}\n",
            f"{REPORT}:1: {SIGN_OFF} {LEFTOVER}",
        ),
        flag_case("a command that gives the flag a value still passes it", f"python tools/digest.py {SIGN_OFF}=1\n", f"{REPORT}:1: {SIGN_OFF} {LEFTOVER}"),
        flag_case("a script name followed by a suffix is another file", f"python tools/digest.py.extra {SIGN_OFF}\ncat tools/digest.pyc {SIGN_OFF}\n"),
        flag_case("a script name followed by a folder is another path", f"python tools/digest.py/child {SIGN_OFF}\n"),
        flag_case("a script name behind more name is another script", f"python tools/predigest.py {SIGN_OFF}\n"),
        flag_case("an absolute path with a space in a folder name is not seen (accepted limit)", f'python "/opt/My Projects/tools/digest.py" {SIGN_OFF}\n'),
        flag_case("a script of the same name in another folder is another script", f"python tools/sub/digest.py {SIGN_OFF}\n"),
        flag_case("a module followed by more of a module is another module", f"python -m tools.digest.extra {SIGN_OFF}\n"),
        flag_case("a flag behind a third dash is another flag", f"python tools/digest.py -{SIGN_OFF}\n"),
        flag_case("a flag behind more text is another flag", f"python tools/digest.py --other=abc{SIGN_OFF}\n"),
        flag_case("a longer flag is another flag", f"python tools/digest.py {SIGN_OFF}-all\n"),
        flag_case("a script of a longer name is another script", f"python tools/old_digest.py {SIGN_OFF}\n"),
        flag_case("a command split over several lines is not seen", f"python tools/digest.py \\\n    {SIGN_OFF} /work/a.json\n"),
        flag_case(
            "the owning script still mentions it",
            "echo done\n",
            files={"tools/digest.py": DIGEST_EDITED + f"# python tools/digest.py {SIGN_OFF} was a flag\n"},
        ),
        flag_case(
            "the old script's command after the flag moved to another script",
            f"python tools/digest.py {SIGN_OFF}\n",
            f"{REPORT}:1: {SIGN_OFF} {LEFTOVER}",
            files=moved,
            committed=other,
        ),
        flag_case("the new script's command after the flag moved to it", f"python tools/other.py {SIGN_OFF}\n", files=moved, committed=other),
        Case(
            "flag removed, another program's command after the script was deleted",
            "removed-names",
            True,
            committed={f"tools/{RETIRED}": DIGEST_SOURCE, REPORT: f"report_tool {SIGN_OFF}\n"},
            delete=[f"tools/{RETIRED}"],
        ),
        Case(
            "flag removed, the command of a deleted script is reported by the script name",
            "removed-names",
            False,
            committed={f"tools/{RETIRED}": DIGEST_SOURCE, REPORT: f"python tools/{RETIRED} {SIGN_OFF}\n"},
            delete=[f"tools/{RETIRED}"],
            reason=f"{REPORT}:1: {RETIRED} {LEFTOVER}",
        ),
    ]


ABSENT_MODULE = "samplepkg." + "absent_job"
PRESENT_MODULE = "samplepkg.present_job"
TOOLS_BASE = {"samplepkg/present_job.py": "VALUE = 1\n", "samplepkg/pkg_main/__main__.py": "VALUE = 1\n", "samplepkg/pkg_plain/__init__.py": ""}


def module_case(name: str, files: dict[str, str | bytes], reason: str = "") -> Case:
    """A command-modules case on the sample code of ``tools/``: it passes when ``reason`` is empty and is blocked for that finding otherwise."""
    return Case(f"python -m command, {name}", "command-modules", not reason, {**TOOLS_BASE, **files}, reason=reason)


def missing_reason(path: str, line: int, module: str = ABSENT_MODULE) -> str:
    """The start of the finding that the command-modules check prints for a missing ``module`` at ``path:line``."""
    return f"{path}:{line}: python -m {module} names a module that does not exist"


MODULE_CASES = [
    module_case("missing module in Markdown", {"docs/run.md": f"Run:\n\n```sh\npython -m {ABSENT_MODULE}\n```\n"}, missing_reason("docs/run.md", 4)),
    module_case(
        "missing module in a workflow",
        {".github/workflows/job.yml": f"jobs:\n  run:\n    steps:\n      - run: python -m {ABSENT_MODULE}\n"},
        missing_reason(".github/workflows/job.yml", 4),
    ),
    module_case(
        "missing module in a shell script",
        {"samplepkg/run.sh": f"cd /work\n.venv/bin/python -m {ABSENT_MODULE} --check\n"},
        missing_reason("samplepkg/run.sh", 2),
    ),
    module_case("missing module in a Terraform string", {"infra/job.tf": f'command = "python -m {ABSENT_MODULE}"\n'}, missing_reason("infra/job.tf", 1)),
    module_case("existing module", {"docs/run.md": f"Run `python -m {PRESENT_MODULE} --flag`.\n"}),
    module_case("package with a __main__ file", {"docs/run.md": "Run:\n\n```sh\npython -m samplepkg.pkg_main\n```\n"}),
    module_case(
        "package without a __main__ file",
        {"docs/run.md": "Run:\n\n```sh\npython -m samplepkg.pkg_plain\n```\n"},
        missing_reason("docs/run.md", 4, "samplepkg.pkg_plain"),
    ),
    module_case("module of another package", {"docs/run.md": "Run `python -m pytest tests` or `python -m pip install tool`.\n"}),
    module_case(
        "python -c and a -m after the script name",
        {"docs/run.md": f"Run `python -c 'print(1)' -m {ABSENT_MODULE}` or `python samplepkg/present_job.py -m {ABSENT_MODULE}`.\n"},
    ),
    module_case("command in a Python string", {"samplepkg/entry.py": f'ENTRY = "python -m {PRESENT_MODULE}\\n"\n'}),
    module_case("module prefix written with an ellipsis", {"docs/run.md": f"Use python -m {ABSENT_MODULE}... for any job.\n"}),
    module_case(
        "missing module behind a grouped interpreter option",
        {"samplepkg/run.sh": f"cd /work\npython -Bm {ABSENT_MODULE}\n"},
        missing_reason("samplepkg/run.sh", 2),
    ),
    module_case(
        "missing module behind several grouped interpreter options",
        {"samplepkg/run.sh": f"cd /work\npython -uBm {ABSENT_MODULE} --check\n"},
        missing_reason("samplepkg/run.sh", 2),
    ),
    module_case(
        "missing module beside the same name with an ellipsis",
        {"samplepkg/run.sh": f"python -m {ABSENT_MODULE}; echo {ABSENT_MODULE}...\n"},
        missing_reason("samplepkg/run.sh", 1),
    ),
    module_case(
        "missing module in a command after an ellipsis command",
        {"samplepkg/run.sh": f"python -m {ABSENT_MODULE}...; python -m {ABSENT_MODULE}_two\n"},
        missing_reason("samplepkg/run.sh", 1, f"{ABSENT_MODULE}_two"),
    ),
    module_case(
        "missing module in a second command of the same name after an ellipsis command",
        {"samplepkg/run.sh": f"python -m {ABSENT_MODULE}...; python -m {ABSENT_MODULE}\n"},
        missing_reason("samplepkg/run.sh", 1),
    ),
    module_case("module prefix in double quotes with an ellipsis", {"samplepkg/run.sh": f'python -m "{ABSENT_MODULE}..."\n'}),
    module_case("module prefix in single quotes behind a grouped option with an ellipsis", {"samplepkg/run.sh": f"python -Bm '{ABSENT_MODULE}...'\n"}),
    module_case("module prefix in backticks with an ellipsis", {"samplepkg/run.sh": f"python -m `{ABSENT_MODULE}...`\n"}),
    module_case("module prefix in double quotes inside a Markdown sentence", {"docs/run.md": f'Start with `python -m "{ABSENT_MODULE}..."` as the prefix.\n'}),
    module_case(
        "missing module after a quoted prefix with an ellipsis",
        {"samplepkg/run.sh": f'python -m "{ABSENT_MODULE}..."; python -m {ABSENT_MODULE}_two\n'},
        missing_reason("samplepkg/run.sh", 1, f"{ABSENT_MODULE}_two"),
    ),
    module_case(
        "missing module of the same name beside a quoted prefix with an ellipsis",
        {"samplepkg/run.sh": f'python -m {ABSENT_MODULE}; echo "see -m {ABSENT_MODULE}..."\n'},
        missing_reason("samplepkg/run.sh", 1),
    ),
    module_case(
        "missing module after a double-quoted echo that holds a prefix with an ellipsis",
        {"samplepkg/run.sh": f'echo "Example: python -m {ABSENT_MODULE}..."; python -m "{ABSENT_MODULE}_two"; echo done\n'},
        missing_reason("samplepkg/run.sh", 1, f"{ABSENT_MODULE}_two"),
    ),
    module_case(
        "missing module after a single-quoted echo that holds a prefix with an ellipsis",
        {"samplepkg/run.sh": f"echo 'Example: python -m {ABSENT_MODULE}...'; python -m '{ABSENT_MODULE}_two'; echo done\n"},
        missing_reason("samplepkg/run.sh", 1, f"{ABSENT_MODULE}_two"),
    ),
    module_case(
        "missing module behind a quoted warning option after a double-quoted echo that holds a prefix with an ellipsis",
        {"samplepkg/run.sh": f'echo "Example: python -m {ABSENT_MODULE}..."; python -W "ignore:example message" -m {ABSENT_MODULE}_two\n'},
        missing_reason("samplepkg/run.sh", 1, f"{ABSENT_MODULE}_two"),
    ),
    module_case("flag of a command", {"docs/run.md": f"Run `python -m {PRESENT_MODULE} --no-such-flag`.\n"}),
    module_case(
        "files that are history", {"CHANGELOG.md": f"Ran:\n\n```sh\npython -m {ABSENT_MODULE}\n```\n", "e2e/run.md": f"```sh\npython -m {ABSENT_MODULE}\n```\n"}
    ),
]


FLAG_CASES = flag_cases()


CASES = [
    Case("clean file", "gitleaks", True, {"notes.md": "Nothing secret here.\n"}),
    Case("GitHub token", "gitleaks", False, {"config.py": f'GITHUB = "{FAKE_PAT}"\n'}),
    Case(".env.example", "credential-files", True, {".env.example": "AWS_REGION=your-aws-region\n"}),
    Case(".env", "credential-files", False, {".env": "AWS_REGION=us-west-2\n"}),
    Case("Terraform state", "credential-files", False, {"infra/terraform.tfstate": "{}\n"}),
    Case("saved Terraform plan", "credential-files", False, {"infra/reviewed.tfplan": "placeholder\n"}),
    Case(
        "saved Terraform plan exported as JSON",
        "credential-files",
        False,
        {"infra/reviewed.tfplan.json": "{}\n"},
        reason="credential, state or saved-plan file staged",
    ),
    Case(
        "numbered Terraform state backup",
        "credential-files",
        False,
        {"infra/terraform.tfstate.1700000000.backup": "{}\n"},
        reason="credential, state or saved-plan file staged",
    ),
    Case("source file with tfstate in its name", "credential-files", True, {"scripts/restored.tfstate.py": "VALUE = 1\n"}),
    Case("private key file", "credential-files", False, {"keys/deploy.pem": "placeholder\n"}),
    # The scratch repository has no ignore rules, so each file below is staged as a force-added file would be.
    Case("Terraform variables file", "credential-files", False, {"infra/prod.tfvars": 'db_password = "placeholder"\n'}),
    Case("Terraform variables JSON file", "credential-files", False, {"infra/prod.tfvars.json": '{"db_password": "placeholder"}\n'}),
    Case("Terraform crash log", "credential-files", False, {"infra/crash.log": "placeholder\n"}),
    Case("Terraform crash log of a provider", "credential-files", False, {"infra/crash.1234.log": "placeholder\n"}),
    Case("committed example variables file", "credential-files", True, {"infra/example.tfvars.example": 'db_password = "your-password"\n'}),
    Case(
        "source files named like the blocked ones",
        "credential-files",
        True,
        {"infra/variables.tf": 'variable "db_password" {}\n', "scripts/crash_report.py": "X = 1\n"},
    ),
    Case("CSV in the declared folder", "data-files", True, {"tests/fixtures/result.csv": "a,b\n1,2\n"}),
    Case("CSV dbt seed in the declared seeds folder", "data-files", True, {"dbt/seeds/labels.csv": "a,b\n1,2\n"}),
    Case("CSV beside the dbt seeds folder", "data-files", False, {"dbt/models/labels.csv": "a,b\n1,2\n"}),
    Case("CSV in the declared bootstrap seed folder", "data-files", True, {"config/acquisition/bootstrap_seed/mmd_browser_history/a.csv": "a,b\n1,2\n"}),
    Case("workbook in the declared bootstrap seed folder", "data-files", True, {"config/acquisition/bootstrap_seed/manual_units/u/a.xlsx": "placeholder\n"}),
    Case("CSV beside the bootstrap seed folder", "data-files", False, {"config/acquisition/bootstrap_seed_copy/a.csv": "a,b\n1,2\n"}),
    Case("CSV in the baseline run bundle", "data-files", False, {"config/acquisition/baseline_run/a.csv": "a,b\n1,2\n"}),
    Case(
        "writing check still reads Markdown in the bootstrap seed folder",
        "writing-check",
        False,
        {"config/acquisition/bootstrap_seed/notes.md": SERIAL_COMMA},
        reason="config/acquisition/bootstrap_seed/notes.md:1: comma before a final 'and', 'or' or 'nor'",
    ),
    Case(
        "front matter check still reads Markdown in the bootstrap seed folder",
        "front-matter",
        False,
        {"config/acquisition/bootstrap_seed/guide.md": "# Guide\n"},
        reason="config/acquisition/bootstrap_seed/guide.md:1: no front matter",
    ),
    Case(
        "link check still reads Markdown in the bootstrap seed folder",
        "markdown-links",
        False,
        {"config/acquisition/bootstrap_seed/guide.md": link_page("absent.md")},
        reason=unresolved(3, "absent.md", "config/acquisition/bootstrap_seed/guide.md"),
    ),
    Case("CSV outside the declared folder", "data-files", False, {"data/hospitals.csv": "a,b\n1,2\n"}),
    Case("CSV approved classification list", "data-files", True, {"config/lakehouse/identity_classification.csv": "a,b\n1,2\n"}),
    Case("CSV named like the approved list", "data-files", False, {"config/lakehouse/identity_classification.csv_unreviewed.csv": "a,b\n1,2\n"}),
    Case("CSV beside the approved list", "data-files", False, {"config/lakehouse/other_list.csv": "a,b\n1,2\n"}),
    Case("file over 5 MB", "data-files", False, {"docs/big.bin": b"\0" * (5 * 1024 * 1024 + 1)}),
    Case("file at 5 MB", "data-files", True, {"docs/ok.bin": b"\0" * (5 * 1024 * 1024)}),
    Case("approved S603 in the launcher", "suppressions", True, {"scripts/process.py": LAUNCHER_SOURCE + f"x = 1  # {NOQA}: S603 - reason\n"}),
    Case("S603 outside the launcher", "suppressions", False, {"scripts/tool.py": f"x = 1  # {NOQA}: S603 - reason\n"}),
    Case("blanket noqa", "suppressions", False, {"scripts/tool.py": f"x = 1  # {NOQA}\n"}),
    Case("type ignore", "suppressions", False, {"scripts/tool.py": f"x: int = 'a'  # {TYPE_IGNORE}\n"}),
    Case("launcher import", "subprocess-imports", True, {"scripts/tool.py": "from scripts.process import run_command\n"}),
    Case("process module import", "subprocess-imports", False, {"scripts/tool.py": f"import {PROCESS_MODULE}\n"}),
    Case("standard Ruff settings", "lint-settings", True, {"pyproject.toml": RUFF_STANDARD}),
    Case("Ruff rule set removed", "lint-settings", False, {"pyproject.toml": RUFF_STANDARD.replace(', "T20"', "")}),
    Case("Ruff ignore added", "lint-settings", False, {"pyproject.toml": RUFF_STANDARD + 'ignore = ["E501"]\n'}),
    Case(
        "Ruff per-file ignore added",
        "lint-settings",
        False,
        {"pyproject.toml": RUFF_STANDARD + '\n[tool.ruff.lint.per-file-ignores]\n"tests/*.py" = ["S101"]\n'},
    ),
    Case("Ruff line length raised", "lint-settings", False, {"pyproject.toml": RUFF_STANDARD.replace("160", "200")}),
    Case(
        "MyPy flag removed",
        "lint-settings",
        False,
        {"pyproject.toml": RUFF_STANDARD, "scripts/ci/groups/python_lint.sh": CI_SOURCE.replace(" --disallow-untyped-defs", "")},
    ),
    Case("MyPy loosened in settings", "lint-settings", False, {"pyproject.toml": RUFF_STANDARD + "\n[tool.mypy]\nignore_errors = true\n"}),
    Case("MyPy alternate profile", "lint-settings", True, {"config/quality/mypy.ini": "[mypy]\npython_version = 3.12\n"}),
    Case("MyPy alternate profile loosened", "lint-settings", False, {"config/quality/mypy.ini": "[mypy]\nignore_missing_imports = True\n"}),
    Case(
        "MyPy per-module profile loosened",
        "lint-settings",
        False,
        {"config/quality/mypy.ini": "[mypy]\npython_version = 3.12\n[mypy-example.*]\nfollow_imports = silent\n"},
    ),
    Case(
        "MyPy TOML override loosened",
        "lint-settings",
        False,
        {"pyproject.toml": RUFF_STANDARD + '\n[[tool.mypy.overrides]]\nmodule = "example.*"\nignore_errors = true\n'},
    ),
    Case(
        "MyPy generated profile",
        "lint-settings",
        False,
        {
            "scripts/typed.py": 'from pathlib import Path\nconfig = Path("generated.ini")\n'
            'COMMAND = ["python", "-m", "mypy", "--config-file", str(config), "--check-untyped-defs", "--disallow-untyped-defs"]\n'
        },
    ),
    Case("documented variable", "env-example", True, {"app/tool.py": read_sample("AWS_REGION")}),
    Case(
        "undocumented variable",
        "env-example",
        False,
        {"app/tool.py": read_sample("NEW_SETTING")},
        reason=f"NEW_SETTING is read but is in neither .env.example nor {ENV_LIST}",
    ),
    Case(
        "documented variable nothing reads",
        "env-example",
        False,
        {".env.example": env_example_text(*ENV_FILE_NAMES, UNUSED_SETTING), ENV_LIST: env_names(env_file=[*ENV_FILE_NAMES, UNUSED_SETTING])},
        reason=f"{UNUSED_SETTING} is in .env.example but production code does not use it",
    ),
    Case(
        "variable that only test code reads, in the list",
        "env-example",
        True,
        {"app/tool_cases.py": read_sample(TEST_SETTING), ENV_LIST: env_names({TEST_SETTING: env_entry("test")})},
    ),
    Case(
        "variable that only test code reads, not in the list",
        "env-example",
        False,
        {"app/tool_cases.py": read_sample(TEST_SETTING)},
        reason=f"{TEST_SETTING} is read but is in neither .env.example nor {ENV_LIST}",
    ),
    Case(
        "test variable in .env.example",
        "env-example",
        False,
        {"app/tool_cases.py": read_sample(TEST_SETTING), ".env.example": env_example_text(*ENV_FILE_NAMES, TEST_SETTING)},
        reason=f"{TEST_SETTING} is in .env.example but not in env_file of {ENV_LIST}",
    ),
    Case(
        "setting of .env that only test code reads",
        "env-example",
        False,
        {
            "app/tool_cases.py": read_sample(TEST_SETTING),
            ".env.example": env_example_text(*ENV_FILE_NAMES, TEST_SETTING),
            ENV_LIST: env_names(env_file=[*ENV_FILE_NAMES, TEST_SETTING]),
        },
        reason=f"{TEST_SETTING} is in .env.example but production code does not use it",
    ),
    Case(
        "test variable that production code reads",
        "env-example",
        False,
        {"app/tool.py": read_sample(TEST_SETTING), "app/tool_cases.py": read_sample(TEST_SETTING), ENV_LIST: env_names({TEST_SETTING: env_entry("test")})},
        reason=f"{TEST_SETTING} is listed as test but production code also uses it",
    ),
    Case(
        "listed test variable no test code reads",
        "env-example",
        False,
        {ENV_LIST: env_names({TEST_SETTING: env_entry("test")})},
        reason=f"{TEST_SETTING} is listed as test but no test code uses it",
    ),
    Case(
        "container variable only in .env.example",
        "env-example",
        False,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            CONTAINER_SETTER: CONTAINER_SETTER_TEXT,
            ".env.example": env_example_text(*ENV_FILE_NAMES, CONTAINER_SETTING),
        },
        reason=f"{CONTAINER_SETTING} is in .env.example but not in env_file of {ENV_LIST}",
    ),
    Case(
        "variable in .env.example and in the list",
        "env-example",
        False,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            CONTAINER_SETTER: CONTAINER_SETTER_TEXT,
            ".env.example": env_example_text(*ENV_FILE_NAMES, CONTAINER_SETTING),
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container")}),
        },
        reason=f"{CONTAINER_SETTING} is in both .env.example and {ENV_LIST}",
    ),
    Case(
        "container variable set in infra",
        "env-example",
        True,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            CONTAINER_SETTER: CONTAINER_SETTER_TEXT,
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container")}),
        },
    ),
    Case(
        "container variable nothing sets",
        "env-example",
        False,
        {"app/tool.py": read_sample(CONTAINER_SETTING), ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container")})},
        reason=f"{CONTAINER_SETTING} is listed as container but nothing in infra/ or the job and launcher configs sets it",
    ),
    Case(
        "container variable set in a named file",
        "env-example",
        True,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            "app/launch.py": f'ENVIRONMENT_OVERRIDE = {{"{CONTAINER_SETTING}": "x"}}\n',
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container", set_in="app/launch.py")}),
        },
    ),
    Case(
        "container variable named file does not use it",
        "env-example",
        False,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            "app/launch.py": "X = 1\n",
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container", set_in="app/launch.py")}),
        },
        reason=f"{CONTAINER_SETTING} is listed with set_in app/launch.py, which is not a tracked production file that sets it",
    ),
    Case(
        "container variable set by the platform",
        "env-example",
        True,
        {"app/tool.py": read_sample(CONTAINER_SETTING), ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container", set_by="ecs")})},
    ),
    Case(
        "container variable no production code uses",
        "env-example",
        False,
        {ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container")})},
        reason=f"{CONTAINER_SETTING} is listed as container but no production code uses it",
    ),
    Case(
        "tool variable that production code reads",
        "env-example",
        True,
        {"app/tool.py": read_sample(TOOL_SETTING), ENV_LIST: env_names({TOOL_SETTING: env_entry("tool")})},
    ),
    Case(
        "listed variable with an unknown category",
        "env-example",
        False,
        {"app/tool.py": read_sample(TOOL_SETTING), ENV_LIST: env_names({TOOL_SETTING: env_entry("person")})},
        reason=f"{TOOL_SETTING} has category person: use test, container, shell or tool",
    ),
    Case(
        "setting of .env missing from .env.example",
        "env-example",
        False,
        {".env.example": env_example_text(*ENV_FILE_NAMES[:-1])},
        reason="ALERT_EMAIL is in env_file but not in .env.example",
    ),
    Case(
        "settings of .env in another order",
        "env-example",
        False,
        {".env.example": env_example_text(*ENV_FILE_NAMES[::-1])},
        reason=".env.example lists its names in a different order than env_file",
    ),
    Case(
        "name commented out in .env.example",
        "env-example",
        False,
        {".env.example": env_example_text(*ENV_FILE_NAMES) + f"# {TEST_SETTING}=\n"},
        reason=f"{TEST_SETTING} is commented out in .env.example",
    ),
    Case(
        "container variable read in services",
        "env-example",
        False,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            "services/app/reader.py": read_sample(CONTAINER_SETTING),
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container")}),
        },
        reason=f"{CONTAINER_SETTING} is listed as container but nothing in infra/ or the job and launcher configs sets it",
    ),
    Case(
        "container variable set only in a Terraform comment",
        "env-example",
        False,
        {
            "app/tool.py": read_sample(CONTAINER_SETTER_NAME),
            CONTAINER_SETTER: f'# environment = [{{ name = "{CONTAINER_SETTER_NAME}", value = "x" }}]\n'
            f'// environment = [{{ name = "{CONTAINER_SETTER_NAME}", value = "x" }}]\n'
            f'/* environment = [{{ name = "{CONTAINER_SETTER_NAME}", value = "x" }}] */\n',
            ENV_LIST: env_names({CONTAINER_SETTER_NAME: env_entry("container")}),
        },
        reason=f"{CONTAINER_SETTER_NAME} is listed as container but nothing in infra/ or the job and launcher configs sets it",
    ),
    Case(
        "container variable set only in a YAML comment",
        "env-example",
        False,
        {
            "app/tool.py": read_sample(CONTAINER_SETTER_NAME),
            "docker-compose.yaml": f"services:\n  app:\n    environment:\n      # - '{CONTAINER_SETTER_NAME}=x'\n      - OTHER=1  # {CONTAINER_SETTER_NAME}\n",
            ENV_LIST: env_names({CONTAINER_SETTER_NAME: env_entry("container")}),
        },
        reason=f"{CONTAINER_SETTER_NAME} is listed as container but nothing in infra/ or the job and launcher configs sets it",
    ),
    Case(
        "container variable set_in names the reader",
        "env-example",
        False,
        {"app/tool.py": read_sample(CONTAINER_SETTING), ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container", set_in="app/tool.py")})},
        reason=f"{CONTAINER_SETTING} is listed with set_in app/tool.py, which is not a tracked production file that sets it",
    ),
    Case(
        "container variable set by a shell export",
        "env-example",
        True,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            "infra/jobs/sample.sh": f"#!/bin/bash\nexport {CONTAINER_SETTING}=placeholder\n",
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container")}),
        },
    ),
    Case(
        "container variable set by a plain shell assignment",
        "env-example",
        True,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            "infra/jobs/sample.sh": f"#!/bin/bash\n{CONTAINER_SETTING}=placeholder\n",
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container")}),
        },
    ),
    Case(
        "container variable set only in a shell comment",
        "env-example",
        False,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            "infra/jobs/sample.sh": f'#!/bin/bash\n# export {CONTAINER_SETTING}=placeholder\necho "# {CONTAINER_SETTING}=x"\n',
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container")}),
        },
        reason=f"{CONTAINER_SETTING} is listed as container but nothing in infra/ or the job and launcher configs sets it",
    ),
    Case(
        "production file whose name holds a test word",
        "env-example",
        False,
        {
            "app/understanding.py": read_sample(TEST_SETTING),
            "app/fakery.py": read_sample(TEST_SETTING),
            "app/defaultplants.py": read_sample(TEST_SETTING),
            ENV_LIST: env_names({TEST_SETTING: env_entry("test")}),
        },
        reason=f"{TEST_SETTING} is listed as test but production code also uses it",
    ),
    Case(
        "variable read by stand-in, fake and fixture modules",
        "env-example",
        True,
        {
            "app/stand_in_server.py": read_sample(TEST_SETTING),
            "app/push_fake_cloud.py": read_sample(TEST_SETTING),
            "app/package_fixture.py": read_sample(TEST_SETTING),
            "app/session_plants.py": read_sample(TEST_SETTING),
            ENV_LIST: env_names({TEST_SETTING: env_entry("test")}),
        },
    ),
    Case(
        "shell variable that production code reads",
        "env-example",
        True,
        {"app/tool.py": read_sample(SHELL_SETTING), ENV_LIST: env_names({SHELL_SETTING: env_entry("shell")})},
    ),
    Case(
        "listed shell variable no production code uses",
        "env-example",
        False,
        {ENV_LIST: env_names({SHELL_SETTING: env_entry("shell")})},
        reason=f"{SHELL_SETTING} is listed as shell but no production code uses it",
    ),
    Case(
        "variable listed twice",
        "env-example",
        False,
        {
            "app/tool.py": read_sample(SHELL_SETTING),
            ENV_LIST: '{"env_file": '
            + json.dumps(ENV_FILE_NAMES)
            + ', "test_modules": {}, "names": {"'
            + SHELL_SETTING
            + '": {"category": "shell", "reason": "a"}, "'
            + SHELL_SETTING
            + '": {"category": "shell", "reason": "b"}}}\n',
        },
        reason=f"{SHELL_SETTING} is listed twice in {ENV_LIST}",
    ),
    Case(
        "second header comment in .env.example",
        "env-example",
        False,
        {".env.example": "# Settings of a person.\n# A second line.\n" + "".join(f"{name}=placeholder\n" for name in ENV_FILE_NAMES)},
        reason=".env.example line 2 is not a setting: only one header comment line is allowed",
    ),
    Case(
        "blank line in .env.example",
        "env-example",
        False,
        {
            ".env.example": "# Settings of a person.\n"
            + "".join(f"{name}=placeholder\n\n" for name in ENV_FILE_NAMES[:1])
            + "".join(f"{name}=placeholder\n" for name in ENV_FILE_NAMES[1:])
        },
        reason=".env.example line 3 is not a setting: only one header comment line is allowed",
    ),
    Case(
        "container variable declared in a job config",
        "env-example",
        True,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            ECS_JOBS: job_config(CONTAINER_SETTING),
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container")}),
        },
    ),
    Case(
        "container variable declared in the direct and host job configs",
        "env-example",
        True,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING) + read_sample(CONTAINER_SETTER_NAME),
            DIRECT_JOBS: job_config(CONTAINER_SETTING),
            HOST_JOBS: job_config(CONTAINER_SETTER_NAME),
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container"), CONTAINER_SETTER_NAME: env_entry("container")}),
        },
    ),
    Case(
        "container variable set_in names a job config",
        "env-example",
        True,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            ECS_JOBS: job_config(CONTAINER_SETTING),
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container", set_in=ECS_JOBS)}),
        },
    ),
    Case(
        "container variable in notes under containers",
        "env-example",
        False,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            "config/orchestration/notes.json": json.dumps({"documentation": {"containers": {"unused": [CONTAINER_SETTING]}}}) + "\n",
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container")}),
        },
        reason=CONTAINER_UNSET,
    ),
    Case(
        "container variable nested in metadata of a job config",
        "env-example",
        False,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            ECS_JOBS: json.dumps(
                {"jobs": {"run": {"notes": {"containers": {"unused": [CONTAINER_SETTING]}}, "containers": {"run": {"meta": [CONTAINER_SETTING]}}}}}
            )
            + "\n",
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container")}),
        },
        reason=CONTAINER_UNSET,
    ),
    Case(
        "container variable in a job config outside a job",
        "env-example",
        False,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            ECS_JOBS: json.dumps({"containers": {"run": [CONTAINER_SETTING]}, "steps": {"jobs": {"containers": {"run": [CONTAINER_SETTING]}}}}) + "\n",
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container")}),
        },
        reason=CONTAINER_UNSET,
    ),
    Case(
        "container variable in a job config list under a key twice",
        "env-example",
        False,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            ECS_JOBS: '{"jobs": {"run": {"containers": {"run": ["'
            + CONTAINER_SETTING
            + '"]}}}, "jobs": {"run": {"containers": {"run": ["'
            + CONTAINER_SETTING
            + '"]}}}}\n',
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container")}),
        },
        reason=CONTAINER_UNSET,
    ),
    Case(
        "container variable as a key of another JSON file",
        "env-example",
        False,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            "config/orchestration/notes.json": json.dumps({"environment": {CONTAINER_SETTING: "x"}, "name": CONTAINER_SETTING}) + "\n",
            "data_contracts/orchestration/examples/notes.json": json.dumps([{"name": CONTAINER_SETTING, "value": "x"}]) + "\n",
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container")}),
        },
        reason=CONTAINER_UNSET,
    ),
    Case(
        "container variable set_in names an unrelated JSON file",
        "env-example",
        False,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            "config/orchestration/notes.json": json.dumps({"containers": {"unused": [CONTAINER_SETTING]}, CONTAINER_SETTING: "x"}) + "\n",
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container", set_in="config/orchestration/notes.json")}),
        },
        reason=f"{CONTAINER_SETTING} is listed with set_in config/orchestration/notes.json, which is not a tracked production file that sets it",
    ),
    Case(
        "container variable set_in names a job config that does not declare it",
        "env-example",
        False,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            ECS_JOBS: job_config("OTHER_SETTING"),
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container", set_in=ECS_JOBS)}),
        },
        reason=f"{CONTAINER_SETTING} is listed with set_in {ECS_JOBS}, which is not a tracked production file that sets it",
    ),
    Case(
        "container variable set through an alias",
        "env-example",
        True,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            "app/launch.py": f'NAME = "{CONTAINER_SETTING}"\nENVIRONMENT_OVERRIDE = {{NAME: "x"}}\n',
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container", set_by="alias", set_in="app/launch.py")}),
        },
    ),
    Case(
        "container variable alias file does not use it",
        "env-example",
        False,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            "app/launch.py": "X = 1\n",
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container", set_by="alias", set_in="app/launch.py")}),
        },
        reason=f"{CONTAINER_SETTING} is listed with set_by alias and set_in app/launch.py, which is not a tracked production Python file that uses it",
    ),
    Case(
        "container variable alias set_in names a JSON file",
        "env-example",
        False,
        {
            "app/tool.py": read_sample(CONTAINER_SETTING),
            "config/orchestration/notes.json": json.dumps({"notes": [CONTAINER_SETTING]}) + "\n",
            ENV_LIST: env_names({CONTAINER_SETTING: env_entry("container", set_by="alias", set_in="config/orchestration/notes.json")}),
        },
        reason="set_by alias and set_in config/orchestration/notes.json, which is not a tracked production Python file that uses it",
    ),
    Case(
        "variable read by a listed test module",
        "env-example",
        True,
        {"app/tool_model.py": read_sample(TEST_SETTING), ENV_LIST: env_names({TEST_SETTING: env_entry("test")}, ["app/tool_model.py"])},
    ),
    Case(
        "listed test module is gone",
        "env-example",
        False,
        {ENV_LIST: env_names(modules=["app/tool_model.py"])},
        reason="app/tool_model.py is listed as a test module but is not a tracked file",
    ),
    Case("script removed with its docs", "removed-names", True, committed={f"scripts/{OLD_SCRIPT}": "X = 1\n"}, delete=[f"scripts/{OLD_SCRIPT}"]),
    Case(
        "script removed, docs still name it",
        "removed-names",
        False,
        committed={f"scripts/{OLD_SCRIPT}": "X = 1\n", "README.md": f"Run `scripts/{OLD_SCRIPT}`.\n"},
        delete=[f"scripts/{OLD_SCRIPT}"],
    ),
    Case(
        "flag removed, docs still name it",
        "removed-names",
        False,
        committed={
            "scripts/tool.py": "import argparse\n\nP = argparse.ArgumentParser()\nP." + ADD_ARGUMENT + f'("{OLD_FLAG}")\n',
            "README.md": f"Run `python scripts/tool.py {OLD_FLAG}`.\n",
        },
        files={"scripts/tool.py": "import argparse\n\nP = argparse.ArgumentParser()\n"},
    ),
    *both_hooks(
        "surviving package named like a removed function",
        True,
        committed=SURVIVING_PACKAGE,
        files={"app/rig.py": "KEEP = 1\n"},
    ),
    Case(
        "removed function named in docs and a YAML value, nothing imports it",
        "removed-names",
        True,
        committed={"app/util.py": UTIL_FUNCTION, "README.md": f"Call `{OLD_FUNCTION}`.\n", "config/policy.yaml": f"rule: {OLD_FUNCTION}\n"},
        files={"app/util.py": "X = 1\n"},
    ),
    Case(
        "removed function still named in a pending file",
        "removed-names",
        False,
        committed={"app/util.py": UTIL_FUNCTION, PENDING_LIST: PENDING_FIXTURE, "dags/pending_dag.py": UTIL_IMPORT},
        files={"app/util.py": "X = 1\n"},
        reason=f"dags/pending_dag.py:3: {OLD_FUNCTION} {LEFTOVER}",
    ),
    Case(
        "removed function named in a file that imports a pending file",
        "removed-names",
        False,
        committed={
            "app/util.py": UTIL_FUNCTION,
            PENDING_LIST: PENDING_FIXTURE,
            "dags/pending_dag.py": "VALUE = 1\n",
            "dags/second.py": "from dags import pending_dag\n" + UTIL_IMPORT,
        },
        files={"app/util.py": "X = 1\n"},
        reason=f"dags/second.py:4: {OLD_FUNCTION} {LEFTOVER}",
    ),
    Case(
        "removed function named in a typed file, left to MyPy",
        "removed-names",
        True,
        committed={"app/util.py": UTIL_FUNCTION, PENDING_LIST: PENDING_FIXTURE, "dags/pending_dag.py": "VALUE = 1\n", "app/main.py": UTIL_IMPORT},
        files={"app/util.py": "X = 1\n"},
    ),
    Case(
        "removed class named in Markdown, left to MyPy",
        "removed-names",
        True,
        committed={"app/models.py": f"class {OLD_CLASS}:\n    pass\n", "README.md": f"Use `{OLD_CLASS}`.\n"},
        files={"app/models.py": "X = 1\n"},
    ),
    Case(
        "allowlisted reference",
        "removed-names",
        True,
        committed={"app/tool.py": f'P.{ADD_ARGUMENT}("{OLD_FLAG}")\n', "docs/history.md": f"The `app/tool.py {OLD_FLAG}` era.\n"},
        files={"app/tool.py": "X = 1\n", ".cleanup_allowlist": "flag docs/history.md -- migration notes keep the old flag -- permanent\n"},
    ),
    *FLAG_CASES,
    Case(
        "config key removed, a docstring example sits beside the reader",
        "removed-names",
        False,
        committed={
            "config/plan.yaml": f"{TRIMMED_KEY}: [a]\nregion: west\n",
            "app/plan.py": f'"""Configuration example: {{"{TRIMMED_KEY}": ["a"]}}."""\nRESULT = PLAN["{TRIMMED_KEY}"]\n',
        },
        files={"config/plan.yaml": "region: west\n"},
        reason=f"app/plan.py:2: {TRIMMED_KEY} {LEFTOVER}",
    ),
    Case(
        "function removed, a YAML key of the same spelling remains",
        "removed-names",
        True,
        committed={"app/util.py": f"def {RETRY_POLICY}() -> int:\n    return 1\n", "config/policy.yaml": f"{RETRY_POLICY}: strict\n"},
        files={"app/util.py": "X = 1\n"},
    ),
    *INPUT_CASES,
    *LATIN1_CASES,
    *DELETED_MODULE_CASES,
    *IN_USE_CASES,
    *CODE_VERSION_CASES,
    Case(
        "function removed with its callers",
        IMPORTERS_HOOK,
        True,
        committed={"app/util.py": UTIL_FUNCTION, "app/main.py": UTIL_IMPORT},
        files={"app/util.py": "X = 1\n", "app/main.py": "X = 1\n"},
    ),
    Case(
        "function moved to another file, importer updated",
        IMPORTERS_HOOK,
        True,
        committed={"app/util.py": UTIL_FUNCTION, "app/main.py": UTIL_IMPORT},
        files={"app/util.py": "X = 1\n", "app/other.py": UTIL_FUNCTION, "app/main.py": UTIL_IMPORT.replace("app.util", "app.other")},
    ),
    Case(
        "function removed, sibling script imports it by package path",
        IMPORTERS_HOOK,
        False,
        committed={"app/util.py": UTIL_FUNCTION, "app/main.py": UTIL_IMPORT},
        files={"app/util.py": "X = 1\n"},
        reason=NO_ATTRIBUTE,
    ),
    Case(
        "function removed, sibling in the same package imports it by relative path",
        IMPORTERS_HOOK,
        False,
        committed={"app/__init__.py": "", "app/util.py": UTIL_FUNCTION, "app/main.py": UTIL_IMPORT.replace("from app.util", "from .util")},
        files={"app/util.py": "X = 1\n"},
        reason=NO_ATTRIBUTE,
    ),
    Case(
        "function removed, importer in another top-level folder",
        IMPORTERS_HOOK,
        False,
        committed={"app/util.py": UTIL_FUNCTION, "dags/user.py": UTIL_IMPORT},
        files={"app/util.py": "X = 1\n"},
        reason=NO_ATTRIBUTE,
    ),
    Case(
        "function removed, same-name re-export in the package",
        IMPORTERS_HOOK,
        False,
        committed={
            "app/__init__.py": f"from app.rig import {OLD_FUNCTION} as {OLD_FUNCTION}\n",
            "app/rig.py": UTIL_FUNCTION,
            "app/read.py": f"from app import {OLD_FUNCTION}\n\nRESULT: int = {OLD_FUNCTION}()\n",
        },
        files={"app/rig.py": "KEEP = 1\n"},
        reason=NO_ATTRIBUTE,
    ),
    Case(
        "module deleted, importer remains",
        IMPORTERS_HOOK,
        False,
        committed={"app/util.py": UTIL_FUNCTION, "app/main.py": UTIL_IMPORT},
        delete=["app/util.py"],
        reason='"app.util"',
    ),
    Case(
        "function parameter removed, caller still passes it",
        IMPORTERS_HOOK,
        False,
        committed={"app/util.py": "def total(count: int) -> int:\n    return count\n", "app/main.py": "from app.util import total\n\nVALUE: int = total(2)\n"},
        files={"app/util.py": "def total() -> int:\n    return 1\n"},
        reason="Too many arguments",
    ),
    Case(
        "function removed, import alias takes its place",
        IMPORTERS_HOOK,
        True,
        committed={
            "app/clock.py": f"def {CLOCK_ALIAS}() -> int:\n    return 1\n",
            "app/use.py": f"from app.clock import {CLOCK_ALIAS}\n\nNOW = {CLOCK_ALIAS}.now()\n",
        },
        files={"app/clock.py": f"from datetime import datetime as {CLOCK_ALIAS}\n"},
    ),
    Case(
        "function removed, importer is a pending file",
        IMPORTERS_HOOK,
        False,
        committed={"app/util.py": UTIL_FUNCTION, PENDING_LIST: PENDING_FIXTURE, "dags/pending_dag.py": "import hai_missing_dependency\n" + UTIL_IMPORT},
        files={"app/util.py": "X = 1\n"},
        reason=NO_ATTRIBUTE,
    ),
    Case(
        "staged pending file has a type error",
        IMPORTERS_HOOK,
        False,
        committed={PENDING_LIST: PENDING_FIXTURE, "dags/pending_dag.py": PENDING_IMPORT_ONLY},
        files={"dags/pending_dag.py": PENDING_IMPORT_ONLY + "\nVALUE: int = 'text'\n"},
        reason="Incompatible types in assignment",
    ),
    Case(
        "staged pending file has only a missing import",
        IMPORTERS_HOOK,
        True,
        committed={PENDING_LIST: PENDING_FIXTURE, "dags/pending_dag.py": "VALUE = 1\n"},
        files={"dags/pending_dag.py": PENDING_IMPORT_ONLY + "\nVALUE: int = 1\n"},
    ),
    Case(
        "staged importer of a pending file has a type error",
        IMPORTERS_HOOK,
        False,
        committed={
            PENDING_LIST: PENDING_FIXTURE,
            "dags/pending_dag.py": PENDING_IMPORT_ONLY,
            "app/main.py": "from dags import pending_dag\n\nVALUE: int = 1\n",
        },
        files={"app/main.py": "from dags import pending_dag\n\nVALUE: int = 'text'\n"},
        reason="Incompatible types in assignment",
    ),
    Case(
        "staged importer of a pending file has only the pending file's missing import",
        IMPORTERS_HOOK,
        True,
        committed={
            PENDING_LIST: PENDING_FIXTURE,
            "dags/pending_dag.py": PENDING_IMPORT_ONLY,
            "app/main.py": "from dags import pending_dag\n\nVALUE: int = 1\n",
        },
        files={"app/main.py": "from dags import pending_dag\n\nVALUE: int = 2\n"},
    ),
    *[
        Case(
            f"function removed from a source-root package ({label}), importer remains",
            IMPORTERS_HOOK,
            False,
            committed={**SRC_PACKAGE, "pyproject.toml": settings},
            files={"src/acme/rig.py": "KEEP = 1\n"},
            reason=NO_ATTRIBUTE,
        )
        for label, settings in SOURCE_ROOT_SETTINGS.items()
    ],
    Case(
        "module deleted from a source-root package, importer remains",
        IMPORTERS_HOOK,
        False,
        committed={**SRC_PACKAGE, "pyproject.toml": MYPY_SRC},
        delete=["src/acme/rig.py"],
        reason='"acme.rig"',
    ),
    Case(
        "function removed from a source-root package with its importer",
        IMPORTERS_HOOK,
        True,
        committed={**SRC_PACKAGE, "pyproject.toml": MYPY_SRC},
        files={"src/acme/rig.py": "KEEP = 1\n", "src/acme/read.py": "VALUE = 1\n"},
    ),
    Case(
        "function removed, source roots unreadable so every file is checked",
        IMPORTERS_HOOK,
        False,
        committed={**SRC_PACKAGE, "pyproject.toml": MYPY_SRC + '\n[tool.ruff]\nsrc = "src"\n'},
        files={"src/acme/rig.py": "KEEP = 1\n"},
        reason=NO_ATTRIBUTE,
    ),
    Case(
        "changed file has a type error",
        IMPORTERS_HOOK,
        False,
        committed={"app/util.py": UTIL_FUNCTION},
        files={"app/util.py": "VALUE: int = 'text'\n"},
        reason="Incompatible types in assignment",
    ),
    Case("documentation change only", IMPORTERS_HOOK, True, committed={"app/util.py": UTIL_FUNCTION}, files={"README.md": "A note.\n"}),
    Case(
        "launcher list lacks a file that the suite now imports",
        LAUNCHER_HOOK,
        False,
        committed={LAUNCHER_BODY: LAUNCHER_START, LAUNCHER_LIST: launcher_list(["app/entry.py"]), "app/entry.py": "VALUE = 1\n"},
        files={"app/entry.py": "from app import helper\n", "app/helper.py": "X = 1\n"},
        reason="app/helper.py: the launcher suite needs it",
    ),
    Case(
        "launcher list names the file that the suite now imports",
        LAUNCHER_HOOK,
        True,
        committed={LAUNCHER_BODY: LAUNCHER_START, LAUNCHER_LIST: launcher_list(["app/entry.py"]), "app/entry.py": "VALUE = 1\n"},
        files={"app/entry.py": "from app import helper\n", "app/helper.py": "X = 1\n", LAUNCHER_LIST: launcher_list(["app/entry.py", "app/helper.py"])},
    ),
    Case(
        "launcher list lacks a new file under a folder the suite reads whole",
        LAUNCHER_HOOK,
        False,
        committed={
            LAUNCHER_BODY: LAUNCHER_START,
            LAUNCHER_LIST: launcher_list(["app/entry.py", "app/data/a.txt"], ["app/data"]),
            "app/entry.py": "VALUE = 1\n",
            "app/data/a.txt": "a\n",
        },
        files={"app/data/b.txt": "b\n"},
        reason="app/data/b.txt",
    ),
    Case(
        "launcher list names a new file under a folder the suite reads whole",
        LAUNCHER_HOOK,
        True,
        committed={
            LAUNCHER_BODY: LAUNCHER_START,
            LAUNCHER_LIST: launcher_list(["app/entry.py", "app/data/a.txt"], ["app/data"]),
            "app/entry.py": "VALUE = 1\n",
            "app/data/a.txt": "a\n",
        },
        files={"app/data/b.txt": "b\n", LAUNCHER_LIST: launcher_list(["app/entry.py", "app/data/a.txt", "app/data/b.txt"], ["app/data"])},
    ),
    Case(
        "launcher list, a change outside what the suite reads",
        LAUNCHER_HOOK,
        True,
        committed={LAUNCHER_BODY: LAUNCHER_START, LAUNCHER_LIST: launcher_list(["app/entry.py"]), "app/entry.py": "VALUE = 1\n"},
        files={"README.md": "A note.\n"},
    ),
    Case(
        "config key removed, code still reads it",
        "removed-names",
        False,
        committed={"config/settings.yaml": f"{OLD_KEY}: 30\nregion: west\n", "app/job.py": f'SETTINGS = {{}}\nDAYS = SETTINGS["{OLD_KEY}"]\n'},
        files={"config/settings.yaml": "region: west\n"},
    ),
    Case(
        "Terraform variable removed, still used",
        "removed-names",
        False,
        committed={"infra/variables.tf": f'variable "{OLD_VARIABLE}" {{}}\n', "infra/main.tf": f"locals {{\n  name = var.{OLD_VARIABLE}\n}}\n"},
        delete=["infra/variables.tf"],
    ),
    Case(
        "dbt model deleted, ref remains",
        "removed-names",
        False,
        committed={
            f"dbt/models/staging/{OLD_MODEL}.sql": "select 1\n",
            "dbt/models/silver/orders.sql": "select id from {{ ref('MODEL') }}\n".replace("MODEL", OLD_MODEL),
        },
        delete=[f"dbt/models/staging/{OLD_MODEL}.sql"],
    ),
    *(
        Case(
            name,
            "removed-names",
            expect,
            committed={f"dbt/models/staging/{OLD_MODEL}.sql": "select 1\n", "dbt/dbt_project.yml": DEFERRED_PROJECT},
            files={"dbt/dbt_project.yml": project},
            delete=[f"dbt/models/staging/{OLD_MODEL}.sql"],
            reason=reason,
        )
        for name, expect, project, reason in (
            ("dbt model deleted and named only in the deferral record of the project file", True, DEFERRED_PROJECT, ""),
            (
                "dbt model deleted, named in the project file outside the deferral record",
                False,
                DEFERRED_PROJECT + f"  other_models: [{OLD_MODEL}]\n",
                f"dbt/dbt_project.yml:10: {OLD_MODEL} {LEFTOVER}",
            ),
            (
                "dbt model deleted, named in the project file before the deferral record",
                False,
                DEFERRED_PROJECT.replace("vars:\n", f"vars:\n  earlier_models: [{OLD_MODEL}]\n", 1),
                f"dbt/dbt_project.yml:4: {OLD_MODEL} {LEFTOVER}",
            ),
        )
    ),
    Case(
        "dependency removed, import remains",
        "removed-names",
        False,
        committed={"pyproject.toml": f'[dependency-groups]\nhost = [\n  "{OLD_PACKAGE}==2.9.0",\n]\n', "app/dates.py": f"import {OLD_MODULE}\n"},
        files={"pyproject.toml": "[dependency-groups]\nhost = []\n"},
    ),
    Case(
        "cleanup allowlist entry without a reason",
        "removed-names",
        False,
        committed={"app/models.py": f"class {OLD_CLASS}:\n    pass\n"},
        files={"app/models.py": "X = 1\n", ".cleanup_allowlist": "python docs/history.md\n"},
    ),
    Case(
        "function used by another file",
        "unused-code",
        True,
        {
            "app/util.py": f"def {STALE_HELPER}():\n    return 1\n",
            "app/main.py": f"from util import {STALE_HELPER}\n\nprint({STALE_HELPER}())\n",
            ".cleanup_allowlist": COPIED_ALLOWED,
        },
    ),
    Case(
        "function named in the vulture whitelist",
        "unused-code",
        True,
        {
            "app/util.py": f"def {STALE_HELPER}():\n    return 1\n",
            WHITELIST: f"from util import {STALE_HELPER}\n\n_ = {STALE_HELPER}\n",
            ".cleanup_allowlist": COPIED_ALLOWED,
        },
    ),
    Case(
        "class named through an attribute of the vulture whitelist",
        "unused-code",
        True,
        {
            "app/plugin.py": f"class {DYNAMIC_CLASS}:\n    pass\n",
            WHITELIST: f"def uses(module):\n    return (module.{DYNAMIC_CLASS},)\n",
            ".cleanup_allowlist": COPIED_ALLOWED,
        },
    ),
    Case(
        "whitelisted dynamic class does not hide an unused function of its file",
        "unused-code",
        False,
        {
            "app/plugin.py": f"class {DYNAMIC_CLASS}:\n    pass\n\n\ndef {STALE_HELPER}():\n    return 1\n",
            WHITELIST: f"def uses(module):\n    return (module.{DYNAMIC_CLASS},)\n",
            ".cleanup_allowlist": COPIED_ALLOWED,
        },
        reason=f"app/plugin.py:5: unused function '{STALE_HELPER}'",
    ),
    Case(
        "function nothing calls",
        "unused-code",
        False,
        {"app/util.py": f"X = 1\n\n\ndef {STALE_HELPER}():\n    return 1\n", ".cleanup_allowlist": COPIED_ALLOWED},
    ),
    Case(
        "declared dependency is imported",
        "unused-dependencies",
        True,
        {
            **lock_files({"host": ({UNUSED_PACKAGE: "0.9.0"}, {})}),
            RUNTIME_MAP: HOST_MAP,
            "app/table.py": f"import {UNUSED_PACKAGE}\n",
            ".cleanup_allowlist": COPIED_ALLOWED,
        },
    ),
    Case(
        "dependency nothing imports",
        "unused-dependencies",
        False,
        {**lock_files({"host": ({UNUSED_PACKAGE: "0.9.0"}, {})}), RUNTIME_MAP: HOST_MAP, ".cleanup_allowlist": COPIED_ALLOWED},
    ),
    Case(
        "import missing from requirements",
        "unused-dependencies",
        False,
        {**lock_files({"host": ({}, {})}), RUNTIME_MAP: HOST_MAP, "app/table.py": f"import {UNUSED_PACKAGE}\n", ".cleanup_allowlist": COPIED_ALLOWED},
    ),
    Case(
        "image package imported by image code",
        "unused-dependencies",
        True,
        {**RUNTIME_SAMPLE, "image/job.py": f"import {UNUSED_PACKAGE}\nimport {PROVIDED_MODULE}\n"},
    ),
    Case(
        "image package imported by host code",
        "unused-dependencies",
        False,
        {**RUNTIME_SAMPLE, "image/job.py": f"import {UNUSED_PACKAGE}\n", "app/table.py": f"import {UNUSED_PACKAGE}\n"},
    ),
    Case(
        "runtime pattern matches no file",
        "unused-dependencies",
        False,
        {**RUNTIME_SAMPLE, "image/job.py": f"import {UNUSED_PACKAGE}\n", RUNTIME_MAP: RUNTIME_MAP_SOURCE.replace("image/*.py", "worker/*.py")},
    ),
    Case(
        "every file referenced",
        "orphan-files",
        True,
        {"README.md": "Read [the guide](docs/guide.md).\n", "docs/guide.md": "# Guide\n", ".cleanup_allowlist": COPIED_ALLOWED},
    ),
    Case("file nothing references", "orphan-files", False, {f"docs/{OLD_NOTES}": "# Old Notes\n", ".cleanup_allowlist": COPIED_ALLOWED}),
    Case(
        "module run with python -m",
        "orphan-files",
        True,
        {"README.md": f"Run `python -m tools.{DASH_M_MODULE}`.\n", f"tools/{DASH_M_MODULE}.py": "print(1)\n", ".cleanup_allowlist": COPIED_ALLOWED},
    ),
    Case(
        "docs reference to a tracked file",
        "docs-references",
        True,
        {"README.md": "Read [the guide](docs/guide.md).\n", "docs/guide.md": "# Guide\n", ".cleanup_allowlist": DOCS_ALLOWED},
    ),
    Case(
        "docs reference to a missing file",
        "docs-references",
        False,
        {"README.md": "Read [the guide](docs/missing_guide.md).\n", ".cleanup_allowlist": DOCS_ALLOWED},
    ),
    Case(
        "docs reference kept by an allowlist entry",
        "docs-references",
        True,
        {
            "README.md": "Read [the guide](docs/missing_guide.md).\n",
            ".cleanup_allowlist": DOCS_ALLOWED + "docs-path README.md -- quotes a deleted guide -- permanent\n",
        },
    ),
    Case(
        "docs reference with a ./ prefix to a missing file",
        "docs-references",
        False,
        {"README.md": "Read [the guide](./docs/missing_guide.md).\n", ".cleanup_allowlist": DOCS_ALLOWED},
    ),
    Case(
        "docs reference that extends a tracked file name",
        "docs-references",
        False,
        {"README.md": "Read docs/guide.md- for the old steps.\n", "docs/guide.md": "# Guide\n", ".cleanup_allowlist": DOCS_ALLOWED},
    ),
    Case(
        "docs words that are not references",
        "docs-references",
        True,
        {
            "README.md": "Edit docs/*.md or the docs/ folder; see https://example.invalid/geo/docs/reference/page.html and ../docs.\n",
            ".cleanup_allowlist": DOCS_ALLOWED,
        },
    ),
    *TASK_ID_CASES,
    *MODULE_CASES,
    Case("permanent file with no temporary code", "temporary-code", True, {**TEMPORARY_BASE, "scripts/lakehouse/perm.py": "VALUE = 2\n"}),
    Case(
        "temporary file imports permanent code",
        "temporary-code",
        True,
        {TEMPORARY_LIST: TEMPORARY_LIST_SOURCE, TEMPORARY_FILE: "from scripts.lakehouse import perm\n", "scripts/lakehouse/perm.py": "VALUE = 2\n"},
    ),
    Case(
        "permanent docstring names a temporary file",
        "temporary-code",
        True,
        {**TEMPORARY_BASE, "scripts/lakehouse/perm.py": f'"""Prose may name {TEMPORARY_FILE} without using it."""\n'},
    ),
    Case(
        "import of a temporary module from its package",
        "temporary-code",
        False,
        {**TEMPORARY_BASE, "scripts/lakehouse/perm.py": f"from scripts.lakehouse import {TEMPORARY_MODULE}\n"},
    ),
    Case(
        "import of a name from a temporary module",
        "temporary-code",
        False,
        {**TEMPORARY_BASE, "scripts/lakehouse/perm.py": f"from scripts.lakehouse.{TEMPORARY_MODULE} import VALUE\n"},
    ),
    Case(
        "plain import of a temporary module",
        "temporary-code",
        False,
        {**TEMPORARY_BASE, "scripts/lakehouse/perm.py": f"import scripts.lakehouse.{TEMPORARY_MODULE}\n"},
    ),
    Case(
        "relative import of a temporary module", "temporary-code", False, {**TEMPORARY_BASE, "scripts/lakehouse/perm.py": f"from . import {TEMPORARY_MODULE}\n"}
    ),
    Case(
        "permanent file reads a temporary file by name",
        "temporary-code",
        False,
        {**TEMPORARY_BASE, "scripts/lakehouse/perm.py": f'from pathlib import Path\nTEXT = Path(__file__).with_name("{TEMPORARY_MODULE}.py").read_text()\n'},
    ),
    Case(
        "shell script pipes a temporary file",
        "temporary-code",
        False,
        {**TEMPORARY_BASE, "scripts/lakehouse/run_probe.sh": f"#!/usr/bin/env bash\ndocker run -i image < {TEMPORARY_FILE}\n"},
    ),
    Case("list names a file that is not tracked", "temporary-code", False, {TEMPORARY_LIST: TEMPORARY_LIST_SOURCE}),
    Case("list is missing", "temporary-code", False, {"scripts/lakehouse/perm.py": "VALUE = 2\n"}),
    Case("used Terraform variable", "terraform-unused", True, {"infra/main.tf": USED_TF}),
    Case("unused Terraform variable", "terraform-unused", False, {"infra/main.tf": f'variable "{UNUSED_TF_VARIABLE}" {{}}\n'}),
    # Only folders that hold a tracked .tf file are checked; an ignored folder is out of scope, as in CI.
    Case(
        "ignored Terraform folder",
        "terraform-unused",
        True,
        {".gitignore": "archive/\n", "archive/main.tf": f'variable "{UNUSED_TF_VARIABLE}" {{}}\n', "infra/main.tf": USED_TF},
    ),
    # A nested tracked folder is checked on its own and its findings keep their path from the repository root.
    Case(
        "unused variable in a nested folder",
        "terraform-unused",
        False,
        {"infra/main.tf": USED_TF, "infra/iam/main.tf": f'variable "{UNUSED_TF_VARIABLE}" {{}}\n'},
    ),
    Case(
        "clean privacy sample",
        "privacy-scan",
        True,
        {"notes.md": "Logs live under ~/projects and $HOME/app; see /Users/<name>/app. Contact test@example.invalid. 51,539,607,552 bytes.\n"},
    ),
    Case("home path", "privacy-scan", False, {"notes.md": f"Log at {HOME_PATH}\n"}),
    Case("temp path", "privacy-scan", False, {"notes.md": f"Scratch at {TEMP_PATH}\n"}),
    Case("personal email", "privacy-scan", False, {"notes.md": f"Ask {PERSONAL_EMAIL}\n"}),
    Case("phone number", "privacy-scan", False, {"notes.md": f"Call {PHONE}\n"}),
    *HOME_AND_EMAIL_CASES,
    *document_path_cases(),
    Case("former OpenMetadata admin address", "privacy-scan", False, {"notes.md": f"Ask {OLD_OPENMETADATA_LOGIN}\n"}),
    Case("AWS account ARN", "privacy-scan", False, {"infra/policy.json": f'{{"Resource": "{AWS_ARN}"}}\n'}),
    Case("AWS account ID field", "privacy-scan", False, {"tests/test_config.py": f'CONFIG = {{"account_id": "{ACCOUNT}"}}\n'}),
    *account_cases(),
    Case("value declared in .env", "privacy-scan", False, {".env": f"AWS_PROFILE={ENV_VALUE}\n", "scripts/deploy.sh": f"aws s3 ls --profile {ENV_VALUE}\n"}),
    Case(
        "alert email declared in .env on a reserved domain",
        "privacy-scan",
        False,
        {".env": "ALERT_EMAIL=owner@example.org\n", "scripts/notify.sh": "mail owner@example.org\n"},
    ),
    Case(
        "empty alert email declared in .env",
        "privacy-scan",
        True,
        {".env": "ALERT_EMAIL=\n", "scripts/notify.sh": "mail --help\n"},
    ),
    Case(
        "allowlisted profile label in its tracked file",
        "privacy-scan",
        True,
        {
            ".env": f"AWS_PROFILE={PROFILE_LABEL}\n",
            PROFILE_FILE: f"aws s3 ls --profile {PROFILE_LABEL}\n",
            ALLOWLIST: ALLOWLIST_SOURCE + f"env-value {PROFILE_FILE} -- a profile name is a local label in this scratch case -- permanent\n",
        },
    ),
    Case(
        "profile label in another file",
        "privacy-scan",
        False,
        {".env": f"AWS_PROFILE={PROFILE_LABEL}\n", "scripts/deploy.sh": f"aws s3 ls --profile {PROFILE_LABEL}\n"},
    ),
    Case(
        "profile named after the project",
        "privacy-scan",
        True,
        {".env": f"PROJECT_NAME={PROJECT_NAME}\nAWS_PROFILE={PROJECT_NAME.replace('-', '_')}\n", "scripts/deploy.sh": f"aws s3 ls --profile {PROJECT_NAME}\n"},
    ),
    Case(
        "profile equal to the project name",
        "privacy-scan",
        True,
        {".env": f"PROJECT_NAME={PROJECT_NAME}\nAWS_PROFILE={PROJECT_NAME}\n", "scripts/deploy.sh": f"aws s3 ls --profile {PROJECT_NAME}\n"},
    ),
    Case(
        "bucket named after the project",
        "privacy-scan",
        False,
        {".env": f"PROJECT_NAME={PROJECT_NAME}\nAWS_BUCKET={PROJECT_NAME}-raw\n", "scripts/deploy.sh": f"aws s3 ls s3://{PROJECT_NAME}-raw\n"},
    ),
    Case(
        "allowlisted email",
        "privacy-scan",
        True,
        {"notes.md": f"Ask {PERSONAL_EMAIL}\n", ALLOWLIST: ALLOWLIST_SOURCE + "email notes.md -- synthetic contact used in a demo -- permanent\n"},
    ),
    Case(
        "allowlist is by type",
        "privacy-scan",
        False,
        {"notes.md": f"Call {PHONE}\n", ALLOWLIST: ALLOWLIST_SOURCE + "email notes.md -- synthetic contact used in a demo -- permanent\n"},
    ),
    Case(
        "allowlist is by path",
        "privacy-scan",
        False,
        {"docs/notes.md": f"Ask {PERSONAL_EMAIL}\n", ALLOWLIST: ALLOWLIST_SOURCE + "email notes.md -- synthetic contact used in a demo -- permanent\n"},
    ),
    Case("allowlist entry without reason", "privacy-scan", False, {"notes.md": "Nothing here.\n", ALLOWLIST: ALLOWLIST_SOURCE + "email notes.md\n"}),
    Case("allowlist entry with unknown type", "privacy-scan", False, {ALLOWLIST: ALLOWLIST_SOURCE + "address notes.md -- demo\n"}),
    Case("unreadable file listed", "privacy-scan", True, {"docs/diagram.png": b"\x89PNG\r\n\x1a\n\x00\x00"}),
    Case("clean writing through the hook", "writing-check", True, {"notes.md": CLEAN_WRITING_HOOK}),
    Case("dash outside an article blocked by the hook", "writing-check", False, {"notes.md": f"An em dash {EM_DASH} reads as a break.\n"}),
    Case("slash between words blocked by the hook", "writing-check", False, {"notes.md": "Read and/or write the file.\n"}),
    Case("writing finding blocked by the hook", "writing-check", False, {"notes.md": SERIAL_COMMA}),
    Case("time-bound word blocked by the hook", "writing-check", False, {"notes.md": "The loader currently reads S3.\n"}),
    Case("procedural soon passes the hook", "writing-check", True, {"notes.md": "Run it as soon as the load ends.\n"}),
    Case("semicolon blocked by the hook", "writing-check", False, {"notes.md": "Rules help; checks enforce them.\n"}),
    Case(
        "history, no longer and wrapped line warn through the hook",
        "writing-check",
        True,
        {"notes.md": "This replaced the earlier rule.\n\nThe hook no longer runs on push.\n\nThis sentence is wrapped\nonto a second line.\n"},
    ),
    Case("internal references warn through the hook", "writing-check", True, {"notes.md": "The work belongs to wave 3.\n"}),
    Case(
        "valid front matter passes",
        "front-matter",
        True,
        {
            "docs/guide.md": "---\ntitle: Guide\ndescription: What the guide covers.\nlast_updated: 2026-10-02\n---\n\n# Guide\n",
            "README.md": "# Project\n",
        },
    ),
    Case("missing front matter blocked", "front-matter", False, {"docs/guide.md": "# Guide\n"}),
    Case(
        "title different from the H1 blocked",
        "front-matter",
        False,
        {"docs/guide.md": "---\ntitle: Guide\ndescription: What the guide covers.\nlast_updated: 2026-10-02\n---\n\n# Guide\n".replace("# Guide", "# Other")},
    ),
    Case(
        "description over 120 characters blocked",
        "front-matter",
        False,
        {
            "docs/guide.md": "---\ntitle: Guide\ndescription: What the guide covers.\nlast_updated: 2026-10-02\n---\n\n# Guide\n".replace(
                "What the guide covers.", "x" * 121
            )
        },
    ),
    Case(
        "invalid YAML front matter blocked",
        "front-matter",
        False,
        {
            "docs/guide.md": "---\ntitle: Guide\ndescription: What the guide covers.\nlast_updated: 2026-10-02\n---\n\n# Guide\n".replace(
                "title: Guide", "title: Guide: part one"
            )
        },
    ),
    Case(
        "non-ISO last_updated blocked",
        "front-matter",
        False,
        {
            "docs/guide.md": "---\ntitle: Guide\ndescription: What the guide covers.\nlast_updated: 2026-10-02\n---\n\n# Guide\n".replace(
                "2026-10-02", "Oct 2 2026"
            )
        },
    ),
    Case(
        "valid links and anchors pass",
        "markdown-links",
        True,
        {
            "docs/guide.md": link_page(
                "setup.md", "setup.md#install-steps", "setup.md#run-ab-fast-then-c", "setup.md#repeat-1", "#guide", "../README.md", "/docs/setup.md", "../docs/"
            ),
            "docs/setup.md": LINK_TARGET_PAGE,
            "README.md": "# Project\n",
        },
    ),
    Case(
        "external and non-Markdown links pass",
        "markdown-links",
        True,
        {
            "docs/guide.md": link_page("https://example.invalid/absent.md#nope", "mailto:someone@example.invalid", "//example.invalid/absent.md", "app.py#L10"),
            "docs/app.py": "VALUE = 1\n",
        },
    ),
    Case(
        "links inside code pass",
        "markdown-links",
        True,
        {"docs/guide.md": "# Guide\n\nA span `[x](absent.md)` stays.\n\n````markdown\n```text\n```\n[x](absent.md)\n````\n"},
    ),
    Case("missing file blocked", "markdown-links", False, {"docs/guide.md": link_page("absent.md")}),
    Case("missing anchor blocked", "markdown-links", False, {"docs/guide.md": link_page("setup.md#no-such-heading"), "docs/setup.md": LINK_TARGET_PAGE}),
    Case("missing anchor in the same file blocked", "markdown-links", False, {"docs/guide.md": link_page("#nowhere")}),
    Case(
        "ignored target blocked",
        "markdown-links",
        False,
        {".gitignore": "docs/local_notes.md\n", "docs/local_notes.md": "# Notes\n", "docs/guide.md": link_page("local_notes.md")},
    ),
    Case(
        "deleted target blocked",
        "markdown-links",
        False,
        committed={"docs/guide.md": link_page("setup.md"), "docs/setup.md": LINK_TARGET_PAGE},
        delete=["docs/setup.md"],
    ),
    Case("link above the repository root blocked", "markdown-links", False, {"docs/guide.md": link_page("../../outside.md")}),
    Case("missing image blocked", "markdown-links", False, {"docs/guide.md": "# Guide\n\n![diagram](assets/absent.png)\n"}),
    Case("missing reference definition target blocked", "markdown-links", False, {"docs/guide.md": "# Guide\n\nSee [the target][ref].\n\n[ref]: absent.md\n"}),
    Case("missing HTML link target blocked", "markdown-links", False, {"docs/guide.md": '# Guide\n\n<a href="absent.md">the target</a>\n'}),
    *LINK_CASES,
    Case("clean Markdown through markdownlint", "markdownlint", True, {"notes.md": "# Notes\n\n- One item\n\n```bash\nls\n```\n"}),
    Case("star bullet blocked by markdownlint", "markdownlint", False, {"notes.md": "# Notes\n\n* One item\n"}),
    Case("fence without a language blocked by markdownlint", "markdownlint", False, {"notes.md": "# Notes\n\n```\nls\n```\n"}),
    Case("E2E records skipped by markdownlint", "markdownlint", True, {"data/e2e/run/report.md": "* raw record\n"}),
    Case("clean dbt model through sqlfluff", "sqlfluff", True, {"dbt/models/sample.sql": "select\n    1 as id,\n    'a' as code\n"}),
    Case("uppercase keyword blocked by sqlfluff", "sqlfluff", False, {"dbt/models/sample.sql": "SELECT\n    1 AS id\n"}),
    Case("leading comma blocked by sqlfluff", "sqlfluff", False, {"dbt/models/sample.sql": "select\n    1 as id\n    , 2 as code\n"}),
    Case("typed subject with scope", "commit-msg", True, message="feat(infra): tag the data bucket\n\nBody.\n"),
    Case("typed subject without scope", "commit-msg", True, message="fix: handle an empty file\n"),
    Case("breaking change marker", "commit-msg", True, message="feat(infra)!: drop a stack\n"),
    Case("untyped subject", "commit-msg", False, message="Update README\n"),
    Case("missing space after colon", "commit-msg", False, message="fix:handle empty input\n"),
    Case("unknown type", "commit-msg", False, message="update(infra): tag the data bucket\n"),
    Case("empty description", "commit-msg", False, message="docs(readme): \n"),
    Case("human co-author", "commit-msg", True, message=f"fix(api): handle retries\n\n{CREDIT}: Jane Doe <jane@example.invalid>\n"),
    Case("agent named without credit", "commit-msg", True, message="docs(loaders): add the Codex loader\n\nClaude Code and Codex read the same files.\n"),
    Case("files generated with a script", "commit-msg", True, message="docs(diagram): refresh the PNG\n\nGenerated with the render script.\n"),
    Case(
        "git comments and verbose diff",
        "commit-msg",
        True,
        message=f"fix: tidy\n# {CREDIT}: Claude <x@example.invalid>\n{SCISSORS}\n+{CREDIT}: Claude <x@example.invalid>\n",
    ),
    Case(
        "ordinary hex, CSS and commit hash text",
        "commit-msg",
        True,
        message="fix(ui): keep the B1 class\n\nThe colour #B1B1B1 and byte 0xB1 stay. Fixed on 54551af and bd71c0d (B1 is a column).\n",
    ),
    Case(
        "review words without a round number",
        "commit-msg",
        True,
        message="fix(api): recheck the lane markers\n\nThe check reviewed the patch and the diff after round trips.\n",
    ),
    Case(
        "decimal snapshot ID and long hash without patch",
        "commit-msg",
        True,
        message="fix(lakehouse): read snapshot 6891234567890123456\n\nThe file SHA-256 7b1aeea20f2c01b0 matches.\n",
    ),
    Case("lane A in the subject", "commit-msg", False, message="feat(training): add lane A static part\n"),
    Case("unit code after a keyword", "commit-msg", False, message="fix(api): handle retries\n\nUnit U4 covers the retry path.\n"),
    Case("failure mode code", "commit-msg", False, message="fix(api): handle retries\n\nCovers failure mode T9.\n"),
    Case("bracketed unit codes", "commit-msg", False, message="fix(api): handle retries\n\nThe pair is reset (P1, P3 to P5).\n"),
    Case("recheck round", "commit-msg", False, message="fix(api): handle retries\n\nAccepted at Recheck 2 of the review.\n"),
    Case("fix round", "commit-msg", False, message="fix(api): handle retries\n\nDone in fix round 2.\n"),
    Case("review number", "commit-msg", False, message="fix(api): handle retries\n\nFound in review #3.\n"),
    Case("accepted patch hash", "commit-msg", False, message="fix(api): handle retries\n\nFrom the accepted patch (SHA-256 399813a5f1ca75a0).\n"),
    Case("staged diff hash", "commit-msg", False, message="fix(api): handle retries\n\nThe staged diff hash 7b1aeea20f2c01b0 was accepted.\n"),
    Case("worktree snapshot ID", "commit-msg", False, message="fix(api): handle retries\n\nEqual to the worktree snapshot ffa86ab.\n"),
    Case("not independently reviewed", "commit-msg", False, message="fix(api): handle retries\n\nNot  independently reviewed as its own change.\n"),
    Case(
        "capital letter codes without a keyword, S3 and issue-like names",
        "commit-msg",
        True,
        message="fix(storage): keep the S3 upload\n\nThe bucket (S3) and step S3 stay. Refs CVE-2024-1234 and SHA3-256 are names; B1 is no code.\n",
    ),
    Case("registry source families in brackets", "commit-msg", True, message="fix(api): add the measures\n\nOne row per control (S13 to S16).\n"),
    Case("a bracketed S code that is not a public family", "commit-msg", False, message="fix(api): add the measures\n\nThe pins follow the plan (S2).\n"),
    Case("a public family beside a plan code in brackets", "commit-msg", False, message="fix(api): add the measures\n\nThe pins follow the plan (P7, S13).\n"),
    Case("a mixed bracketed list with a plan code", "commit-msg", False, message="fix(api): add the measures\n\nThe pins follow the plan (P7, S2).\n"),
    Case(
        "round numbers in a review sentence",
        "commit-msg",
        True,
        message="fix(api): keep the sums\n\nThe reviewer requested two round numbers and five round figures in the totals.\n",
    ),
    Case(
        "rounding and round trips in a review sentence",
        "commit-msg",
        True,
        message="fix(api): keep the sums\n\nThe reviewer asked to round two decimals and checked a round trip and a round robin.\n",
    ),
    Case(
        "number of rounds outside review context", "commit-msg", True, message="test(api): repeat the load run\n\nThe run takes five rounds of ten requests.\n"
    ),
    Case(
        "downloaded patch hash",
        "commit-msg",
        True,
        message="fix(deps): apply the vendor fix\n\nThe downloaded patch SHA-256 399813a5f1ca75a0 matches the vendor page.\n",
    ),
    Case(
        "snapshot ID of data",
        "commit-msg",
        True,
        message="fix(lakehouse): read the table at the snapshot\n\nThe table snapshot ID a0b1c2d3 and snapshot 1234567 are table versions.\n",
    ),
    Case("unit code G4", "commit-msg", False, message="fix(api): handle retries\n\nCovers unit G4.\n"),
    Case("task code A54", "commit-msg", False, message="fix(api): handle retries\n\nDone in task A54.\n"),
    Case("lane codes D1 and L13", "commit-msg", False, message="fix(api): handle retries\n\nSee lanes D1 and L13.\n"),
    Case("step code R14", "commit-msg", False, message="fix(api): handle retries\n\nAfter step R14 the queue drains.\n"),
    Case("slice code K1", "commit-msg", False, message="fix(api): handle retries\n\nThe slice K1 retries once.\n"),
    Case("failure mode code S2", "commit-msg", False, message="fix(api): handle retries\n\nCovers failure mode S2.\n"),
    Case("bracketed code G4", "commit-msg", False, message="fix(api): handle retries\n\nThe pair is reset (G4).\n"),
    Case(
        "issue ID after Refs, ee93061",
        "commit-msg",
        False,
        message="fix(quality): run the hook partitions one after the other (#42)\n\nThe partitions run in one worker.\n\nRefs ABC1-033\n",
    ),
    Case("bracketed issue ID", "commit-msg", False, message="fix(api): handle retries\n\nThe pair is reset (ABC1-033).\n"),
    Case(
        "review rounds in number words, 4c524c7",
        "commit-msg",
        False,
        message="docs: add the acquisition design document\n\nIndependent review over five rounds; the last passed with one minor finding, fixed here.\n",
    ),
    Case("round two in a review", "commit-msg", False, message="fix(api): handle retries\n\nAccepted in round two of the review.\n"),
    Case(
        "accepted patch hash, any word order", "commit-msg", False, message="fix(api): handle retries\n\nThe SHA-256 399813a5f1ca75a0 of the accepted patch.\n"
    ),
    Case("worktree snapshot ID, letters only", "commit-msg", False, message="fix(api): handle retries\n\nEqual to the worktree snapshot abcdefa.\n"),
    Case("worktree snapshot ID, digits only", "commit-msg", False, message="fix(api): handle retries\n\nEqual to the worktree snapshot ID 1234567.\n"),
    Case("agent credit trailer", "commit-msg", False, message=f"docs(readme): update setup\n\nBody.\n\n{CREDIT}: Claude Opus 5.5 <{VENDOR_ADDRESS}>\n"),
    Case("agent credit, lower case", "commit-msg", False, message=f"fix: tidy\n\n{CREDIT.lower()}:codex <codex@example.invalid>\n"),
    Case("vendor address only", "commit-msg", False, message=f"fix: tidy\n\n{CREDIT}: Assistant <{VENDOR_ADDRESS}>\n"),
    Case("generated-with line", "commit-msg", False, message=f"fix: tidy\n\n{MADE_WITH} [Claude Code](https://claude.com/claude-code)\n"),
    Case("agent session trailer", "commit-msg", False, message=f"fix: tidy\n\n{SESSION}: https://example.invalid/session\n"),
    Case("assisted-by credit", "commit-msg", False, message="fix: tidy\n\nAssisted-By: GitHub Copilot\n"),
    Case("emoji generated credit", "commit-msg", False, message="fix: tidy\n\n🤖 Generated by ChatGPT\n"),
    Case("scissors cannot hide actual credit", "commit-msg", False, message=f"fix: tidy\n\n{SCISSORS}\n{CREDIT}: Claude\n"),
    Case("agent substring in human name", "commit-msg", True, message=f"fix: tidy\n\n{CREDIT}: Claudette Example <human@example.invalid>\n"),
]
# Privacy cases must be blocked for their intended cause, shown by file, line and type.
BLOCK_REASONS = {
    "import of a temporary module from its package": f"scripts/lakehouse/perm.py:1: {TEMPORARY_USE}",
    "import of a name from a temporary module": f"scripts/lakehouse/perm.py:1: {TEMPORARY_USE}",
    "plain import of a temporary module": f"scripts/lakehouse/perm.py:1: {TEMPORARY_USE}",
    "relative import of a temporary module": f"scripts/lakehouse/perm.py:1: {TEMPORARY_USE}",
    "permanent file reads a temporary file by name": f"scripts/lakehouse/perm.py:2: {TEMPORARY_USE}",
    "shell script pipes a temporary file": f"scripts/lakehouse/run_probe.sh:2: {TEMPORARY_USE}",
    "list names a file that is not tracked": f"config/quality/temporary_code.json: temporary file {TEMPORARY_FILE} is listed but not tracked",
    "list is missing": "config/quality/temporary_code.json: the list of temporary files cannot be read",
    "function nothing calls": f"app/util.py:4: unused function '{STALE_HELPER}'",
    "dependency nothing imports": f"pyproject.toml: {UNUSED_PACKAGE} is declared but nothing imports it",
    "import missing from requirements": f"app/table.py:1: {UNUSED_PACKAGE} is imported but runtime host does not declare it",
    "file nothing references": f"docs/{OLD_NOTES}: no other tracked file references it",
    "docs reference to a missing file": "README.md:1: docs/missing_guide.md is not a tracked file",
    "unused Terraform variable": f'infra/main.tf:1: variable "{UNUSED_TF_VARIABLE}" is declared but not used',
    "unused variable in a nested folder": f'infra/iam/main.tf:1: variable "{UNUSED_TF_VARIABLE}" is declared but not used',
    "image package imported by host code": f"app/table.py:1: {UNUSED_PACKAGE} is imported but runtime host does not declare it",
    "runtime pattern matches no file": "runtime image: worker/*.py matches no tracked Python file",
    "home path": "notes.md:1: home-directory path with a user name",
    "analytics-like home name": "notes.md:1: home-directory path with a user name",
    "another home name": "notes.md:1: home-directory path with a user name",
    "analytics name under the user folder": "notes.md:1: home-directory path with a user name",
    "former OpenMetadata admin address": "notes.md:1: email address",
    "temp path": "notes.md:1: machine temporary path",
    "personal email": "notes.md:1: email address",
    "phone number": "notes.md:1: phone number",
    "AWS account ARN": "infra/policy.json:1: AWS account ID",
    "AWS account ID field": "tests/test_config.py:1: AWS account ID",
    "synthetic account beside another account": "infra/policy.json:1: AWS account ID",
    "all-zero account ARN": "infra/policy.json:1: AWS account ID",
    "all-zero account ID field": "tests/test_config.py:1: AWS account ID",
    "all-two account ARN": "infra/policy.json:1: AWS account ID",
    "all-two account ID field": "tests/test_config.py:1: AWS account ID",
    "exempt account first in a conditional assignment": "tests/test_config.py:1: AWS account ID",
    "exempt account ARN beside a bare other account": "infra/policy.json:1: AWS account ID",
    "real account declared in .env": "scripts/deploy.sh:1: value declared in .env",
    "value declared in .env": "scripts/deploy.sh:1: value declared in .env",
    "alert email declared in .env on a reserved domain": "scripts/notify.sh:1: value declared in .env",
    "profile label in another file": "scripts/deploy.sh:1: value declared in .env",
    "bucket named after the project": "scripts/deploy.sh:1: value declared in .env",
    "allowlist is by type": "notes.md:1: phone number",
    "allowlist is by path": "docs/notes.md:1: email address",
    "allowlist entry without reason": ".privacy_allowlist:{line}: allowlist entry without a reason",
    "allowlist entry with unknown type": ".privacy_allowlist:{line}: allowlist entry with an unknown type",
    "writing finding blocked by the hook": "notes.md:1: comma before a final 'and', 'or' or 'nor'",
    "time-bound word blocked by the hook": "notes.md:1: time-bound word in prose",
    "semicolon blocked by the hook": "notes.md:1: semicolon in prose",
    "internal references warn through the hook": "WARN notes.md:1: internal reference (wave)",
    "missing file blocked": "docs/guide.md:3: link target 'absent.md' is not a tracked file",
    "missing anchor blocked": "docs/guide.md:3: link anchor '#no-such-heading' matches no heading in docs/setup.md",
    "missing anchor in the same file blocked": "docs/guide.md:3: link anchor '#nowhere' matches no heading in docs/guide.md",
    "ignored target blocked": "docs/guide.md:3: link target 'local_notes.md' is not a tracked file",
    "deleted target blocked": "docs/guide.md:3: link target 'setup.md' is not a tracked file",
    "link above the repository root blocked": "docs/guide.md:3: link target '../../outside.md' is not a tracked file",
    "missing image blocked": "docs/guide.md:3: link target 'assets/absent.png' is not a tracked file",
    "missing reference definition target blocked": "docs/guide.md:5: link target 'absent.md' is not a tracked file",
    "missing HTML link target blocked": "docs/guide.md:3: link target 'absent.md' is not a tracked file",
    "star bullet blocked by markdownlint": "MD004",
    "missing front matter blocked": "docs/guide.md:1: no front matter",
    "title different from the H1 blocked": "title does not match the H1",
    "description over 120 characters blocked": "description over 120 characters",
    "invalid YAML front matter blocked": "front matter is not valid YAML",
    "non-ISO last_updated blocked": "last_updated is not a YYYY-MM-DD date",
    "fence without a language blocked by markdownlint": "MD040",
    "uppercase keyword blocked by sqlfluff": "CP01",
    "leading comma blocked by sqlfluff": "LT04",
}
BLOCK_REASONS.update({case.name: case.reason for case in [*HOME_AND_EMAIL_CASES, *document_path_cases()] if case.reason})
# Good cases that must also print a marker, so a skipped check cannot pass silently.
WARNING_HEADERS = {
    LATIN1_NAMES["binary"]: (f"data/blob.bin{BINARY_NOTICE}",),
    LATIN1_NAMES["late_nul"]: (f"tools/digest.py{BINARY_NOTICE}",),
    LATIN1_NAMES["gains_nul"]: (f"tools/digest.py{BINARY_NOTICE}",),
    LATIN1_NAMES["new_nul"]: (f"tools/blob.py{BINARY_NOTICE}",),
    LATIN1_NAMES["unchanged_nul"]: (f"data/blob.bin{BINARY_NOTICE}",),
    LATIN1_NAMES["graph_nul"]: (f"tools/blob.py{BINARY_NOTICE}",),
    "history, no longer and wrapped line warn through the hook": (
        "WARN notes.md:1: history note in a current document",
        "WARN notes.md:3: possible history note: 'no longer' describes a change",
        "WARN notes.md:6: hard-wrapped prose line",
    ),
}
PASS_MARKERS = {
    "unreadable file listed": "docs/diagram.png: unreviewed: cannot be read as text",
    "internal references warn through the hook": "WARN notes.md:1: internal reference (wave)",
}


def git(repo: Path, *args: str) -> str:
    """Run Git in the scratch repository and return its output."""
    return run_command("git", args, cwd=repo, check=True).stdout


def reset_hard(repo: Path, target: str) -> None:
    """Reset a scratch repository that this runner made to ``target``; any other folder is refused and left as it is."""
    scratch_marker.require(repo, "reset")
    git(repo, "reset", "-q", "--hard", target)


def write(repo: Path, files: dict[str, str | bytes] | dict[str, str]) -> None:
    """Write files into the scratch repository, creating folders as needed."""
    for relative, content in files.items():
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            path.write_bytes(content)
        else:
            path.write_text(content)


def scratch_repo(root: Path, case: Case) -> Path:
    """Create a repository holding this repository's hook setup and scripts plus the case's committed files."""
    repo = root / "repo"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    scratch_marker.mark(repo, "scripts.quality.run_checks_e2e")
    git(repo, "config", "user.name", "check test")
    git(repo, "config", "user.email", "test@example.invalid")
    # No background garbage collection or maintenance: a detached run can still be writing into .git when the scratch
    # folder is removed, which failed CI run 37736374173 with "Directory not empty".
    git(repo, "config", "gc.auto", "0")
    git(repo, "config", "maintenance.auto", "false")
    for key, value in case.git_config.items():
        git(repo, "config", key, value)
    # Copy only files Git tracks or would track, so ignored local code never enters the scratch repository. The env-example cases copy the scripts
    # that run the hook but keep them out of Git (see below): they start from a baseline of their own, the settings of .env and a file that reads them.
    listed = run_command(
        "git", ["ls-files", "--cached", "--others", "--exclude-standard", "scripts", "orchestration/launcher.py"], cwd=ROOT, check=True
    ).stdout.splitlines()
    if case.hook == "env-example":
        listed = [path for path in listed if path.startswith("scripts/")]
    if case.hook == "task-ids":
        listed += TASK_IDS_FILES
    for relative in [*COPIED, *listed]:
        if (ROOT / relative).is_file():
            (repo / relative).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(ROOT / relative, repo / relative)
    (repo / ALLOWLIST).write_text(ALLOWLIST_SOURCE)
    for tool in (".venv", ".tools"):
        (repo / tool).symlink_to(ROOT / tool)
    (repo / ".git" / "info" / "exclude").write_text(".venv\n.tools\n__pycache__/\n" + ("scripts/\n" if case.hook == "env-example" else ""))
    write(repo, ENV_BASELINE if case.hook == "env-example" else {})
    write(repo, case.committed)
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "chore: scratch setup")
    return repo


def run_case(case: Case) -> tuple[bool, str]:
    """Run one case and return whether the hook behaved as expected, with its output."""
    with tempfile.TemporaryDirectory(prefix="hai_check_test_") as scratch:
        repo = scratch_repo(Path(scratch), case)
        write(repo, case.files)
        for relative, target in case.symlinks.items():
            (repo / relative).parent.mkdir(parents=True, exist_ok=True)
            (repo / relative).symlink_to(target)
        for relative in case.delete:
            git(repo, "rm", "-q", relative)
        git(repo, "add", "-A")
        write(repo, case.untracked)
        verbose = case.name in PASS_MARKERS or case.name in WARNING_HEADERS
        args = [
            "run",
            case.hook,
            "--hook-stage",
            hook_stage(repo / ".pre-commit-config.yaml", case.hook),
            *(["--verbose"] if verbose else []),
        ]  # pre-commit hides passing output
        if case.message is not None:
            message = repo / ".git" / "COMMIT_EDITMSG"
            message.write_text(case.message)
            args += ["--hook-stage", "commit-msg", "--commit-msg-filename", str(message)]
        environment = {**os.environ, "PRE_COMMIT_HOME": str(ROOT / ".tools" / "pre_commit_cache"), "GIT_EDITOR": ":"}
        result = run_command(str(PRE_COMMIT), args, cwd=repo, env=environment, timeout=180)
        passed = result.returncode == 0
        output = result.stdout + result.stderr
        correct = passed == case.expect_pass and "Traceback (most recent call last)" not in output
        if not case.expect_pass and case.hook == "commit-msg":
            correct = correct and any(
                marker in output for marker in ("not a Conventional Commit:", "AI attribution in commit message", "internal plan reference in commit message:")
            )
        if case.hook == "privacy-scan":
            added = len(ALLOWLIST_SOURCE.splitlines()) + 1
            reason = BLOCK_REASONS.get(case.name, "").format(line=added)
            marker = PASS_MARKERS.get(case.name, "")
            correct = correct and reason in output and marker in output and not any(value in output for value in PRIVACY_VALUES)
        if case.name in WARNING_HEADERS:
            correct = correct and all(header in output for header in WARNING_HEADERS[case.name])
        if not case.expect_pass and case.hook in ("writing-check", "privacy-scan"):
            correct = correct and POLICY_DOCUMENT in output
        if case.hook in (
            "writing-check",
            "markdownlint",
            "front-matter",
            "markdown-links",
            "sqlfluff",
            "docs-references",
            "temporary-code",
            "removed-names",
            "task-ids",
            "command-modules",
            "env-example",
            IMPORTERS_HOOK,
            LAUNCHER_HOOK,
            *CLEANUP_HOOKS,
        ):
            correct = correct and (case.reason or BLOCK_REASONS.get(case.name, "")) in output
        if case.hook == "lint-settings" and not case.expect_pass and case.name.startswith("MyPy"):
            reason = "configuration must name a committed profile" if case.name == "MyPy generated profile" else "MyPy"
            correct = correct and reason in output
        return correct, output


# The removed-names check and the importer check run as plain commands: planted regressions need the check's own source, not the hook wrapper.
REMOVED_NAMES_COMMAND = ["-m", "scripts.quality.repo_checks", "removed-names"]
TASK_IDS_COMMAND = ["-m", "scripts.quality.repo_checks", "task-ids"]
LOCATION_MOVES = -1  # The planted exit code of a plant that keeps the finding and changes only the location it names.
MODULES_COMMAND = ["-m", "scripts.quality.repo_checks", "command-modules"]
IMPORTERS_COMMAND = ["-m", "scripts.quality.typecheck_importers"]
LAUNCHER_COMMAND = ["-m", "scripts.quality.repo_checks", "launcher-list"]
# Each planted regression reverts one rule in the scratch copy of a check: what the rule frees must then be blocked, and what it blocks must then pass.
# Fields: rule, the cases that show it, the command, the source file, the marker text, its replacement and the exit code the damaged check must give.
PLANTED_REGRESSIONS = [
    (
        "code-version history rule",
        CODE_VERSION_CASES[:1],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "CODE_VERSION_RECORDS.fullmatch(path)",
        "False",
        1,
    ),
    (
        "text check only in pending files",
        [case for case in CASES if case.hook == "removed-names" and case.expect_pass and ("left to MyPy" in case.name or "nothing imports it" in case.name)],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        'if kind != "python" or path in unchecked else None',
        "if True else None",
        1,
    ),
    (
        "pending files keep the text check",
        [case for case in CASES if case.hook == "removed-names" and not case.expect_pass and "pending file" in case.name],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        'if kind != "python" or path in unchecked else None',
        'if kind != "python" else None',
        0,
    ),
    (
        "Terraform names stay live",
        [case for case in IN_USE_CASES if case.expect_pass and "schema property removed" in case.name and "Terraform" in case.name],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "        return hcl_declarations(lines)\n",
        "        return set()\n",
        1,
    ),
    (
        "Python declarations stay live",
        [case for case in IN_USE_CASES if case.expect_pass and "schema property removed" in case.name and "Python" in case.name],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "        return python_declarations(lines)\n",
        "        return set()\n",
        1,
    ),
    (
        "a file name is not a config key",
        [case for case in IN_USE_CASES if case.expect_pass and "names a data file" in case.name],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "python_literals(view, without_paths)",
        "view",
        1,
    ),
    (
        "a deleted schema keeps its file name and id checked",
        [case for case in IN_USE_CASES if not case.expect_pass and case.name.startswith("schema deleted") and "its id" not in case.name],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        '        if path.endswith(".schema.json"):\n',
        "        if False:\n",
        0,
    ),
    (
        "a deleted schema keeps its id checked",
        [case for case in IN_USE_CASES if not case.expect_pass and case.name == "schema deleted, a reference to its id remains"],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "for schema_id in removed_ids - live}",
        "for schema_id in set()}",
        0,
    ),
    (
        "Terraform comments and heredocs declare nothing",
        [case for case in IN_USE_CASES if not case.expect_pass and ("Terraform heredoc" in case.name or "Terraform block comment" in case.name)],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "code = hcl_code(lines)",
        "code = lines",
        0,
    ),
    (
        "structured keys are read from the parsed document",
        [
            case
            for case in IN_USE_CASES
            if case.expect_pass and any(label in case.name for label in ("on one line", "array of objects", "flow mapping", "inline table"))
        ],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        'parsed = structured_declarations(path, "\\n".join(lines))',
        "parsed = None",
        1,
    ),
    (
        "a YAML block scalar declares no key",
        [case for case in IN_USE_CASES if not case.expect_pass and "YAML block scalar" in case.name],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        'parsed = structured_declarations(path, "\\n".join(lines))',
        "parsed = None",
        0,
    ),
    (
        "a method after an attribute is no file suffix",
        [case for case in IN_USE_CASES if not case.expect_pass and "attribute" in case.name],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "python_literals(view, without_paths)",
        "[without_paths(line) for line in view]",
        0,
    ),
    (
        "a parameter keeps a removed Python name live",
        [case for case in CASES if case.hook == "removed-names" and case.expect_pass and "unrelated parameter elsewhere is its own name" in case.name],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "        return python_declarations(lines)\n",
        "        return set()\n",
        1,
    ),
    (
        "an edited schema keeps its ids checked",
        [case for case in IN_USE_CASES if not case.expect_pass and "schema id renamed" in case.name],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "for schema_id in removed_ids - live}",
        "for schema_id in set()}",
        0,
    ),
    (
        "an escaped reference to a deleted id is read decoded",
        [case for case in IN_USE_CASES if not case.expect_pass and "spells the id with a JSON escape" in case.name],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "return [decoded_json_line(line) if",
        "return lines or [decoded_json_line(line) if",
        0,
    ),
    (
        "Python strings and comments declare nothing",
        [case for case in IN_USE_CASES if not case.expect_pass and "unparsable Python file holds the word" in case.name],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        '        view = "\\n".join(python_literals(lines, blanked, comments=True, drop_unread=True))',
        '        view = "\\n".join(lines)',
        0,
    ),
    (
        "a changed document is read before and after",
        [case for case in IN_USE_CASES if not case.expect_pass and "compact" in case.name],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "config_removed = {key for key in removed_keys if",
        "config_removed = {key for key in set() if",
        0,
    ),
    (
        "a block comment declares no Terraform block",
        [case for case in IN_USE_CASES if not case.expect_pass and "Terraform variable replaced by a block comment" in case.name],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        'new_blocks = {block for path, lines in files.items() if path.endswith(".tf") for block in hcl_blocks(lines)}',
        'new_blocks = {block for path, lines in files.items() if path.endswith(".tf") for block in TF_BLOCK.findall("\\n".join(lines))}',
        0,
    ),
    (
        "a schema id declared elsewhere stays live",
        [case for case in IN_USE_CASES if case.expect_pass and ("schema id moved" in case.name or "id is declared by another" in case.name)],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "live = live_names(files, python_left | config_left | removed_ids)",
        "live = live_names(files, python_left | config_left)",
        1,
    ),
    (
        "an escaped key is read before the file is skipped",
        [case for case in IN_USE_CASES if case.expect_pass and "is spelled with an escape" in case.name],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        'and not ("\\\\" in text and Path(path).suffix in STRUCTURED_SUFFIXES)',
        "and True",
        1,
    ),
    (
        "a removed Python name stays live in every format",
        [case for case in IN_USE_CASES if case.expect_pass and "removed Python function, an independent" in case.name],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "live = live_names(files, python_left | config_left | removed_ids)",
        "live = live_names(files, config_left | removed_ids)",
        1,
    ),
    (
        "a removed Python name is read from the old and new code",
        [case for case in IN_USE_CASES if case.expect_pass and "removing a Python string example" in case.name],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "for name in python_top_level(lines) - python_top_level(files[path])",
        'for name in set(PY_TOP_LEVEL.findall("\\n".join(lines))) - python_top_level(files[path])',
        1,
    ),
    (
        "text after a tokenizer error declares nothing",
        [case for case in IN_USE_CASES if not case.expect_pass and "after a tokenizer error" in case.name],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        '        view = "\\n".join(python_literals(lines, blanked, comments=True, drop_unread=True))',
        '        view = "\\n".join(python_literals(lines, blanked, comments=True))',
        0,
    ),
    (
        "task ID pairs of JSON files are read",
        [
            case
            for case in TASK_ID_CASES
            if case.name
            in (
                "task ID, stale pair in JSON",
                "task ID, stale pair in a nested JSON object",
            )
        ],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        "pairs = json_task_ids(json.loads(text))",
        "pairs = []",
        0,
    ),
    (
        "task ID pairs are read in JSON arrays",
        [case for case in TASK_ID_CASES if case.name in ("task ID, stale pair in a nested JSON object",)],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        'found += json_task_ids(value, f"{where}[{index}]")',
        "pass",
        0,
    ),
    (
        "task ID pairs are read in JSON objects under a key",
        [case for case in TASK_ID_CASES if case.name in ("task ID, stale pair in an object under a key",)],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        'found += json_task_ids(value, f"{where}.{key}")',
        "pass",
        0,
    ),
    (
        "an empty task ID is a task ID",
        [
            case
            for case in TASK_ID_CASES
            if case.name
            in (
                "task ID, empty task ID in a Python dict",
                "task ID, empty task ID in keyword arguments",
            )
        ],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        "if dag is not None and task is not None:",
        "if dag and task:",
        0,
    ),
    (
        "Python source is read with its declared encoding",
        [case for case in TASK_ID_CASES if case.name in ("task ID, Python source in a declared Latin-1 encoding",)],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        "ast.parse(data)",
        'ast.parse(data.decode("utf-8"))',
        0,
    ),
    (
        "tracked paths come from a NUL-separated list",
        [case for case in TASK_ID_CASES if case.name in ("task ID, Python file with a non-ASCII path",)],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        'git("ls-files", "-z", "*.json", "*.py").split("\\0")',
        'git("ls-files", "*.json", "*.py").split("\\n")',
        0,
    ),
    (
        "a JSON finding names the line of its own key",
        [case for case in TASK_ID_CASES if case.name in ("task ID, two compact JSON objects on one line",)],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        'lines = [text.count("\\n", 0, match.start()) + 1 for match in TASK_KEY.finditer(text)]',
        "lines = [number for number, line in enumerate(text.splitlines(), 1) if TASK_KEY.search(line)]",
        LOCATION_MOVES,
    ),
    (
        "JSON pairs follow the key order of the text",
        [case for case in TASK_ID_CASES if case.name in ("task ID, an outer object whose task ID follows a nested object",)],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        "for key, value in node.items():",
        'for key, value in sorted(node.items(), key=lambda item: item[0] != "task_id"):',
        LOCATION_MOVES,
    ),
    (
        "a JSON object with an unknown line is named by its path",
        [case for case in TASK_ID_CASES if case.name in ("task ID, a JSON object whose line cannot be known is named by its path",)],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        "if len(lines) == len(pairs) else",
        "if True else",
        LOCATION_MOVES,
    ),
    (
        "task ID pairs of a Python dict are read",
        [case for case in TASK_ID_CASES if case.name in ("task ID, stale pair in a Python dict",)],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        "if isinstance(node, ast.Dict):",
        "if False:",
        0,
    ),
    (
        "task ID pairs of keyword arguments are read",
        [case for case in TASK_ID_CASES if case.name in ("task ID, stale pair in keyword arguments",)],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        "named = {keyword.arg: text(keyword.value) for keyword in node.keywords}",
        "named = {}",
        0,
    ),
    (
        "task ID pairs of a positional SubmissionRequest are read",
        [case for case in TASK_ID_CASES if case.name in ("task ID, stale pair in a positional SubmissionRequest",)],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        'if callee == "SubmissionRequest" and len(node.args) > max(dag_at, task_at):',
        "if False:",
        0,
    ),
    (
        "the positions of the IDs come from the request fields",
        [case for case in TASK_ID_CASES if case.name in ("task ID, stale pair in a positional SubmissionRequest",)],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        'dag_at, task_at = names.index("dag_id"), names.index("task_id")',
        "dag_at, task_at = 3, 4",
        0,
    ),
    (
        "a DAG that is not shipped is not judged",
        [case for case in TASK_ID_CASES if case.name in ("task ID, pair of a DAG that is not shipped",)],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        "if dag in tasks and task not in tasks[dag]:",
        "if task not in tasks.get(dag, []):",
        1,
    ),
    (
        "every task of a shipped DAG is accepted",
        [
            case
            for case in TASK_ID_CASES
            if case.name
            in (
                "task ID, real task of a shipped DAG in JSON",
                "task ID, real task of a shipped DAG in a positional SubmissionRequest",
                "task ID, trigger task of a DAG that has triggers",
            )
        ],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        "task not in tasks[dag]:",
        "task not in tasks[dag][:1]:",
        1,
    ),
    (
        "a trigger task belongs to its own DAG only",
        [case for case in TASK_ID_CASES if case.name in ("task ID, a trigger task of another DAG is stale",)],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        "task not in tasks[dag]:",
        "task not in sorted({name for names in tasks.values() for name in names}):",
        0,
    ),
    (
        "a value that is not a literal is not judged",
        [case for case in TASK_ID_CASES if case.name in ("task ID, values that are not literals",)],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        "def text(node: ast.AST) -> str | None:\n        return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None",
        "def text(node: ast.AST) -> str | None:\n        return node.value if isinstance(node, ast.Constant) else getattr(node, 'id', None)",
        1,
    ),
    (
        "a file that does not parse is skipped",
        [
            case
            for case in TASK_ID_CASES
            if case.name
            in (
                "task ID, Python file that does not parse",
                "task ID, JSON file that does not parse",
            )
        ],
        TASK_IDS_COMMAND,
        "scripts/quality/repo_checks.py",
        "except (OSError, SyntaxError, ValueError):",
        "except OSError:",
        1,
    ),
    (
        "a flag counts only on a line that names its script",
        [
            case
            for case in FLAG_CASES
            if case.name
            in (
                "flag removed, the same text in another program's command",
                "flag removed, a script of a longer name is another script",
                "flag removed, the new script's command after the flag moved to it",
            )
        ],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "(?=.*(?<![\\w./-]){script}(?![\\w/-]|\\.\\w))",
        "",
        1,
    ),
    (
        "the file name of a script marks its command",
        [case for case in FLAG_CASES if case.name in ("flag removed, a command that names the script by its file name still passes it",)],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "|(?:\\./)?(?:{'|'.join(sorted(names))})",
        "",
        0,
    ),
    (
        "a dot slash may precede a file name",
        [case for case in FLAG_CASES if case.name in ("flag removed, a command that names the script by its file name with a dot slash still passes it",)],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "|(?:\\./)?(?:{'|'.join(sorted(names))})",
        "|(?:{'|'.join(sorted(names))})",
        0,
    ),
    (
        "the path of a script marks its command",
        [case for case in FLAG_CASES if case.name in ("flag removed, a command that names the script by its path still passes it",)],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "(?:(?:\\./|/(?:[\\w.~-]+/)*)?(?:{'|'.join(sorted(paths))})|",
        "(?:",
        0,
    ),
    (
        "a dot slash may precede a path",
        [case for case in FLAG_CASES if case.name in ("flag removed, a command that names the script by a relative path still passes it",)],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "(?:\\./|/(?:[\\w.~-]+/)*)?(?:{'|'.join(sorted(paths))})",
        "(?:/(?:[\\w.~-]+/)*)?(?:{'|'.join(sorted(paths))})",
        0,
    ),
    (
        "an absolute folder path may precede a path",
        [case for case in FLAG_CASES if case.name in ("flag removed, a command that names the script by an absolute path still passes it",)],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "(?:\\./|/(?:[\\w.~-]+/)*)?(?:{'|'.join(sorted(paths))})",
        "(?:\\./)?(?:{'|'.join(sorted(paths))})",
        0,
    ),
    (
        "the module of a script marks its command",
        [case for case in FLAG_CASES if case.name in ("flag removed, a command that names the script by its module still passes it",)],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "|{'|'.join(sorted(modules))})",
        ")",
        0,
    ),
    (
        "a name is whole at its end",
        [
            case
            for case in FLAG_CASES
            if case.name
            in (
                "flag removed, a script name followed by a suffix is another file",
                "flag removed, a script name followed by a folder is another path",
                "flag removed, a module followed by more of a module is another module",
            )
        ],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "(?![\\w/-]|\\.\\w))",
        "(?![\\w-]))",
        1,
    ),
    (
        "a name is whole at its start",
        [
            case
            for case in FLAG_CASES
            if case.name
            in (
                "flag removed, a script name behind more name is another script",
                "flag removed, a script of the same name in another folder is another script",
            )
        ],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "(?<![\\w./-]){script}",
        "{script}",
        1,
    ),
    (
        "a flag is whole at its start",
        [
            case
            for case in FLAG_CASES
            if case.name
            in (
                "flag removed, a flag behind a third dash is another flag",
                "flag removed, a flag behind more text is another flag",
            )
        ],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "(?<![\\w-]){re.escape(flag)}",
        "{re.escape(flag)}",
        1,
    ),
    (
        "a flag is whole at its end",
        [case for case in FLAG_CASES if case.name in ("flag removed, a longer flag is another flag",)],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "{re.escape(flag)}(?![\\w-])",
        "{re.escape(flag)}",
        1,
    ),
    (
        "an absolute folder path may hold a space",
        [case for case in FLAG_CASES if case.name in ("flag removed, an absolute path with a space in a folder name is not seen (accepted limit)",)],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "/(?:[\\w.~-]+/)*",
        "/(?:[\\w. ~-]+/)*",
        1,
    ),
    (
        "the owning script is not searched for its own flag",
        [case for case in FLAG_CASES if case.name in ("flag removed, the owning script still mentions it",)],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        'if kind == "flag" and path == declarers.get((name, pattern.pattern)):',
        "if False:",
        1,
    ),
    (
        "a __main__ file runs its package",
        [case for case in MODULE_CASES if case.name in ("python -m command, package with a __main__ file",)],
        MODULES_COMMAND,
        "scripts/quality/repo_checks.py",
        '(".py", "/__main__.py")',
        '(".py",)',
        1,
    ),
    (
        "a module file runs its module",
        [case for case in MODULE_CASES if case.name in ("python -m command, existing module",)],
        MODULES_COMMAND,
        "scripts/quality/repo_checks.py",
        '(".py", "/__main__.py")',
        '("/__main__.py",)',
        1,
    ),
    (
        "a package without a __main__ file does not run",
        [case for case in MODULE_CASES if case.name in ("python -m command, package without a __main__ file",)],
        MODULES_COMMAND,
        "scripts/quality/repo_checks.py",
        '(".py", "/__main__.py")',
        '(".py", "/__main__.py", "/__init__.py")',
        0,
    ),
    (
        "only modules of this repository are judged",
        [case for case in MODULE_CASES if case.name in ("python -m command, module of another package",)],
        MODULES_COMMAND,
        "scripts/quality/repo_checks.py",
        'module.split(".")[0] in first_party',
        "True",
        1,
    ),
    (
        "history files are not read",
        [case for case in MODULE_CASES if case.name in ("python -m command, files that are history",)],
        MODULES_COMMAND,
        "scripts/quality/repo_checks.py",
        'if path == "CHANGELOG.md" or path.startswith("e2e/") or path == CLEANUP_ALLOWLIST or not Path(path).is_file():',
        "if path == CLEANUP_ALLOWLIST or not Path(path).is_file():",
        1,
    ),
    (
        "quoted strings are read outside Python files",
        [case for case in MODULE_CASES if case.name in ("python -m command, missing module in a Terraform string",)],
        MODULES_COMMAND,
        "scripts/quality/repo_checks.py",
        'quoted = [] if path.endswith(".py") else [',
        "quoted = [] if True else [",
        0,
    ),
    (
        "a Python string is read by its line only",
        [case for case in MODULE_CASES if case.name in ("python -m command, command in a Python string",)],
        MODULES_COMMAND,
        "scripts/quality/repo_checks.py",
        'quoted = [] if path.endswith(".py") else [',
        "quoted = [] if False else [",
        1,
    ),
    (
        "a module prefix written with an ellipsis is no command",
        [case for case in MODULE_CASES if case.name in ("python -m command, module prefix written with an ellipsis",)],
        MODULES_COMMAND,
        "scripts/quality/repo_checks.py",
        'command_modules(ELLIPSIS_MODULE.sub(r"\\1-", text))',
        "command_modules(text)",
        1,
    ),
    (
        "the ellipsis belongs to its own module token",
        [
            case
            for case in MODULE_CASES
            if case.name
            in (
                "python -m command, missing module beside the same name with an ellipsis",
                "python -m command, missing module in a second command of the same name after an ellipsis command",
            )
        ],
        MODULES_COMMAND,
        "scripts/quality/repo_checks.py",
        'if module.split(".")[0] in first_party and not any(',
        'if module.split(".")[0] in first_party and f"{module}..." not in line and not any(',
        0,
    ),
    (
        "a grouped interpreter option may carry the module option",
        [
            case
            for case in MODULE_CASES
            if case.name
            in (
                "python -m command, missing module behind a grouped interpreter option",
                "python -m command, missing module behind several grouped interpreter options",
            )
        ],
        MODULES_COMMAND,
        "scripts/quality/repo_checks.py",
        "if not GROUPED_M.search(text):",
        'if "-m" not in text:',
        0,
    ),
    (
        "a grouped interpreter option carries the module option of a deleted module",
        [case for case in CASES if case.name == "deleted module, command string with a grouped interpreter option remains"],
        REMOVED_NAMES_COMMAND,
        "scripts/quality/repo_checks.py",
        "if not GROUPED_M.search(text):",
        'if "-m" not in text:',
        0,
    ),
    (
        "a quoted module prefix with an ellipsis is no command",
        [
            case
            for case in MODULE_CASES
            if case.name
            in (
                "python -m command, module prefix in double quotes with an ellipsis",
                "python -m command, module prefix in single quotes behind a grouped option with an ellipsis",
                "python -m command, module prefix in backticks with an ellipsis",
                "python -m command, module prefix in double quotes inside a Markdown sentence",
            )
        ],
        MODULES_COMMAND,
        "scripts/quality/repo_checks.py",
        "(?:([\\\"'`])[\\w.]+\\.\\.\\.\\2|[\\w.]+\\.\\.\\.)",
        "[\\w.]+\\.\\.\\.",
        1,
    ),
    (
        "a quote beside an ellipsis module is consumed only as the pair of the module's own",
        [
            case
            for case in MODULE_CASES
            if case.name
            in (
                "python -m command, missing module after a double-quoted echo that holds a prefix with an ellipsis",
                "python -m command, missing module after a single-quoted echo that holds a prefix with an ellipsis",
                "python -m command, missing module behind a quoted warning option after a double-quoted echo that holds a prefix with an ellipsis",
            )
        ],
        MODULES_COMMAND,
        "scripts/quality/repo_checks.py",
        "(?:([\\\"'`])[\\w.]+\\.\\.\\.\\2|[\\w.]+\\.\\.\\.)",
        "[\\\"'`]?[\\w.]+\\.\\.\\.[\\\"'`]?",
        0,
    ),
    (
        "declared import roots",
        [case for case in CASES if case.hook == IMPORTERS_HOOK and not case.expect_pass and "source-root package" in case.name],
        IMPORTERS_COMMAND,
        "scripts/quality/typecheck_importers.py",
        "roots = declared_roots(root, base)",
        "roots: list[str] = []",
        0,
    ),
    (
        "unreadable import roots check every file",
        [case for case in CASES if case.hook == IMPORTERS_HOOK and "source roots unreadable" in case.name],
        IMPORTERS_COMMAND,
        "scripts/quality/typecheck_importers.py",
        "return sorted(typecheck.source_files())",
        "return []",
        0,
    ),
    (
        "importers of a changed module",
        [
            case
            for case in CASES
            if case.hook == IMPORTERS_HOOK
            and not case.expect_pass
            and case.name not in ("changed file has a type error", "function removed, same-name re-export in the package")
        ],
        IMPORTERS_COMMAND,
        "scripts/quality/typecheck_importers.py",
        "importing_files(root, base, [*present, *gone])",
        "importing_files(root, base, [])",
        0,
    ),
    (
        "importers of a deleted module",
        [case for case in CASES if case.name == "module deleted, importer remains"],
        IMPORTERS_COMMAND,
        "scripts/quality/typecheck_importers.py",
        "importing_files(root, base, [*present, *gone])",
        "importing_files(root, base, present)",
        0,
    ),
    (
        "the launcher list is checked when its group is selected",
        [case for case in CASES if case.hook == LAUNCHER_HOOK and not case.expect_pass],
        LAUNCHER_COMMAND,
        "scripts/quality/committed_lists.py",
        'if "launcher" not in ci_groups.plan("staged", "launcher")["run"]:',
        "if True:",
        0,
    ),
    (
        "the launcher list is compared with the computed list",
        [case for case in CASES if case.hook == LAUNCHER_HOOK and case.name == "launcher list lacks a file that the suite now imports"],
        LAUNCHER_COMMAND,
        "scripts/quality/committed_lists.py",
        'found = [("launcher_list_equals_computed", line) for line in launcher_inputs.problems(ROOT, tracked, committed)]',
        "found = []",
        0,
    ),
]


PLANTED_UNIT_CASES = 10  # A unit of the sharded run: at most this many samples of one rule (about 9 s each), so no unit is a large part of a shard.


def planted_regression_units() -> list[tuple[str, int, int, int]]:
    """The (label, rule index, first sample, end sample) of each unit of the planted regression run, in rule order."""
    units = []
    for index, (rule, cases, *_rest) in enumerate(PLANTED_REGRESSIONS):
        for first in range(0, len(cases), PLANTED_UNIT_CASES):
            stop = min(first + PLANTED_UNIT_CASES, len(cases))
            units.append((f"planted regressions of {rule}: samples {first + 1} to {stop}", index, first, stop))
    return units


def run_planted_regression_case(rule_index: int, first: int, stop: int) -> tuple[bool, str]:
    """Every sample passes or blocks as the real check says and has the planted exit code once the rule it shows is reverted in the scratch copy of the check.

    A plant of ``LOCATION_MOVES`` keeps the finding of the sample and changes only the location it names: the real run prints the sample's reason, and the
    planted run exits 1 with the finding but not the reason. An unrelated failure, an exception or another finding, is never caught.
    """
    python = str(ROOT / ".venv/bin/python")
    lines, ok = [], True
    position = -1
    for rule, cases, command, source, marker, replacement, planted_exit in PLANTED_REGRESSIONS:
        position += 1
        if position != rule_index:
            continue
        for case in cases[first:stop]:
            with tempfile.TemporaryDirectory(prefix="hai_planted_regression_") as scratch:
                repo = scratch_repo(Path(scratch), case)
                write(repo, case.files)
                for relative in case.delete:
                    git(repo, "rm", "-q", relative)
                git(repo, "add", "-A")
                real = run_command(python, command, cwd=repo, timeout=180)
                path = repo / source
                text = path.read_text()
                if text.count(marker) != 1:
                    return False, f"cannot plant the regression of the {rule}: {marker!r} is not in {source} once\n"
                path.write_text(text.replace(marker, replacement, 1))
                planted = run_command(python, command, cwd=repo, timeout=180)
                seen = planted.stdout + planted.stderr
                moves = planted_exit == LOCATION_MOVES
                finding = case.reason.partition(": ")[2]
                caught = (
                    planted.returncode == 1 and bool(finding) and finding in seen and case.reason not in seen and "Traceback" not in seen
                    if moves
                    else planted.returncode == planted_exit
                )
                shown = not moves or case.reason in real.stdout + real.stderr
                ok = ok and real.returncode == (0 if case.expect_pass else 1) and shown and caught
                lines.append(f"{rule}, {case.name}: real check exit={real.returncode}; planted regression exit={planted.returncode}, caught={caught}\n")
    return ok, "".join(lines)


def run_importer_range_case() -> tuple[bool, str]:
    """The importer check and the pre-push MyPy step read a pushed range: a removal in a later commit fails where an earlier file imports the name."""
    python = str(ROOT / ".venv/bin/python")
    with tempfile.TemporaryDirectory(prefix="hai_importer_range_") as scratch:
        repo = scratch_repo(
            Path(scratch),
            Case(
                "importer range setup",
                IMPORTERS_HOOK,
                True,
                committed={"app/util.py": UTIL_FUNCTION, "app/main.py": UTIL_IMPORT, PENDING_LIST: '{"pending": {}}\n'},
            ),
        )
        base = git(repo, "rev-parse", "HEAD").strip()
        outcomes = []
        for label, files, deleted, want in (
            ("function removed", {"app/util.py": "X = 1\n"}, [], 1),
            ("module deleted", {}, ["app/util.py"], 1),
            ("function removed with its caller", {"app/util.py": "X = 1\n", "app/main.py": "Y = 1\n"}, [], 0),
        ):
            reset_hard(repo, base)
            write(repo, files)
            for relative in deleted:
                git(repo, "rm", "-q", relative)
            git(repo, "add", "-A")
            git(repo, "commit", "-q", "-m", "refactor: change the helper")
            head = git(repo, "rev-parse", "HEAD").strip()
            hook = run_command(python, IMPORTERS_COMMAND, cwd=repo, env={**os.environ, "PRE_COMMIT_FROM_REF": base, "PRE_COMMIT_TO_REF": head}, timeout=180)
            step = run_command(python, ["-m", "scripts.quality.ci_changed_files", "--from", base, "--to", head, "--only", "mypy"], cwd=repo, timeout=180)
            blocked = want == 1
            good = hook.returncode == want and step.returncode == want and (not blocked or ("app/main.py" in hook.stdout and "FAIL mypy" in step.stdout))
            outcomes.append(
                (
                    good,
                    f"{'ok' if good else 'WRONG'} {label}: hook exit {hook.returncode}, pre-push step exit {step.returncode}, expected {want}\n"
                    + hook.stdout
                    + step.stdout,
                )
            )
    return all(good for good, _ in outcomes), "".join(text for _, text in outcomes)


def run_range_case() -> tuple[bool, str]:
    """Exercise CI's entry point on full stored messages, including credit after a scissors line."""
    with tempfile.TemporaryDirectory(prefix="hai_commit_range_") as scratch:
        repo = scratch_repo(Path(scratch), Case("range setup", "commit-msg", True))
        base = git(repo, "rev-parse", "HEAD").strip()
        git(repo, "commit", "--allow-empty", "-qm", f"fix: human credit\n\n{CREDIT}: Jane Doe <jane@example.invalid>")
        good = run_command(str(ROOT / ".venv/bin/python"), ["-m", "scripts.quality.repo_checks", "commit-range", base, "HEAD"], cwd=repo)
        git(repo, "commit", "--allow-empty", "-qm", f"fix: bad credit\n\n{SCISSORS}\n{CREDIT}: Claude <{VENDOR_ADDRESS}>")
        bad = run_command(str(ROOT / ".venv/bin/python"), ["-m", "scripts.quality.repo_checks", "commit-range", base, "HEAD"], cwd=repo)
        invalid = run_command(str(ROOT / ".venv/bin/python"), ["-m", "scripts.quality.repo_checks", "commit-range", "missing-ref", "HEAD"], cwd=repo)
        reset_hard(repo, "HEAD~1")
        git(repo, "commit", "--allow-empty", "-qm", "fix: handle retries\n\nAccepted at recheck 2 of lane A.")
        plan = run_command(str(ROOT / ".venv/bin/python"), ["-m", "scripts.quality.repo_checks", "commit-range", base, "HEAD"], cwd=repo)
        ok = good.returncode == 0 and bad.returncode == 1 and "AI attribution in commit message" in bad.stderr and invalid.returncode != 0
        ok = ok and plan.returncode == 1 and "internal plan reference in commit message: review-round name" in plan.stderr
        return (
            ok,
            f"clean range exit={good.returncode}; attributed range exit={bad.returncode}; "
            f"missing revision exit={invalid.returncode}; plan range exit={plan.returncode}\n" + bad.stderr + plan.stderr,
        )


def workflow_value(value: str, context: dict[str, str]) -> str:
    """Resolve the workflow's range expressions from a synthetic event and the real plan step's output."""
    if not value.startswith("${{"):
        return value
    names = value.removeprefix("${{").removesuffix("}}").strip().split("||")
    return next((context[name.strip()] for name in names if context[name.strip()]), "")


FIND_STEP = "Find the merge base of the pushed range"
RANGE_STEP = "Check commit messages and removed names of the pushed range"


def partial_failing_git(folder: Path, trigger: str, printed: str) -> Path:
    """A folder with a ``git`` stand-in that prints ``printed`` and exits 128 for a call with the argument ``trigger``, and runs the real Git otherwise."""
    folder.mkdir(parents=True)
    real = run_command("which", ["git"], timeout=30).stdout.strip()
    program = folder / "git"
    program.write_text(
        f'#!/bin/bash\nfor argument in "$@"; do\n  if [[ "$argument" == "{trigger}" ]]; then echo "{printed}"; exit 128; fi\ndone\nexec {real} "$@"\n'
    )
    program.chmod(0o755)
    return folder


def workflow_range_case(
    repo: Path, label: str, event: str, head: str, want_exit: int, want_marker: str = "", path_first: Path | None = None
) -> dict[str, object]:
    """Run the two hosted steps that check commit messages and removed names: every run, whatever its event, checks ``merge-base origin/main..sha``."""
    workflow = load_yaml((repo / ".github/workflows/ci.yml").read_text())
    if not isinstance(workflow, dict):
        raise ValueError(".github/workflows/ci.yml is not a mapping")
    steps = {item["name"]: item for item in workflow["jobs"]["hooks"]["steps"]}
    if FIND_STEP not in steps or RANGE_STEP not in steps:
        return {"case": label, "passed": False, "checks": {"the_steps_exist": False}}
    context = {"github.event_name": event, "github.sha": head}
    output = repo / ".git" / "range_output"
    output.write_text("")
    log, code = "", 0
    for name in (FIND_STEP, RANGE_STEP):
        step = steps[name]
        env = {key: workflow_value(value, context) for key, value in step.get("env", {}).items()}
        script = repo / ".git" / "range_check.sh"
        script.write_text("set -eo pipefail\n" + step["run"])
        environment = {**os.environ, "RUNNER_TEMP": str(repo / ".git"), "GITHUB_OUTPUT": str(output), **env}
        if path_first:
            environment["PATH"] = f"{path_first}:{environment['PATH']}"
        result = run_command("bash", [str(script)], cwd=repo, env=environment, timeout=120)
        log += result.stdout + result.stderr
        code = result.returncode
        if code != 0:
            break
        context.update({f"steps.range.outputs.{line.split('=', 1)[0]}": line.split("=", 1)[1] for line in output.read_text().splitlines() if "=" in line})
    conditions = " ".join(str(steps[name].get("if", "")) for name in (FIND_STEP, RANGE_STEP))
    checks = {
        "no_other_trigger": sorted(workflow.get(True) or workflow.get("on") or {}) == ["push", "workflow_dispatch"],
        "runs_for_every_event": "github.event_name" not in conditions and "github.event.before" not in conditions,
        "check_has_the_expected_result": code == want_exit and want_marker in log,
    }
    return {"case": label, "passed": all(checks.values()), "checks": checks, "check_exit": code, "log": log}


def run_workflow_range_case() -> tuple[bool, str]:
    """Run the actual workflow shell on a first push, a manual run, a push after a push, a removed name, a clean branch and a branch with no merge base."""
    with tempfile.TemporaryDirectory(prefix="hai_workflow_range_") as scratch:
        removed = "--branch" + "-removed-value"
        api = "scripts/acquisition/range_sample.py"
        original = f'import argparse\n\nP = argparse.ArgumentParser()\nP.{ADD_ARGUMENT}("{removed}")\n'
        repo = scratch_repo(
            Path(scratch), Case("workflow range setup", "commit-msg", True, committed={api: original, "caller.sh": f"python {api} {removed}\n"})
        )
        git(repo, "update-ref", "refs/remotes/origin/main", "main")
        git(repo, "checkout", "-qb", "topic")
        git(repo, "commit", "--allow-empty", "-qm", "docs: describe the branch")
        clean = git(repo, "rev-parse", "HEAD").strip()
        git(repo, "commit", "--allow-empty", "-qm", "Invalid subject")
        push_a = git(repo, "rev-parse", "HEAD").strip()
        git(repo, "commit", "--allow-empty", "-qm", "docs: a later valid commit")
        push_b = git(repo, "rev-parse", "HEAD").strip()
        mark = "not a Conventional Commit"
        results = [
            workflow_range_case(repo, "first_push_checks_the_whole_branch", "push", push_a, 1, mark),
            workflow_range_case(repo, "manual_run_checks_the_whole_branch", "workflow_dispatch", push_a, 1, mark),
            workflow_range_case(repo, "push_b_after_push_a_still_checks_push_a", "push", push_b, 1, mark),
            workflow_range_case(repo, "clean_branch_passes", "push", clean, 0),
        ]
        git(repo, "checkout", "-q", "--detach", clean)
        write(repo, {api: "VALUE = 1\n"})
        git(repo, "add", "--", api)
        git(repo, "commit", "-qm", "refactor: remove a branch flag")
        deleted = git(repo, "rev-parse", "HEAD").strip()
        results.append(workflow_range_case(repo, "real_branch_removal_is_rejected", "push", deleted, 1, f"{removed} was removed"))
        git(repo, "checkout", "-q", "--orphan", "unrelated")
        git(repo, "commit", "--allow-empty", "-qm", "docs: an unrelated history")
        unrelated = git(repo, "rev-parse", "HEAD").strip()
        results.append(workflow_range_case(repo, "no_merge_base_fails_with_a_clear_message", "push", unrelated, 1, "merge base"))
        git(repo, "checkout", "-q", "--force", "topic")
        git(repo, "tag", "origin/main", push_a)
        results.append(workflow_range_case(repo, "a_tag_named_origin_main_does_not_change_the_range", "push", push_a, 1, mark))
        git(repo, "tag", "-d", "origin/main")
        git(repo, "checkout", "-q", "--detach", clean)
        second = "scripts/acquisition/range_sample_two.py"
        flag = "--added" + "-then-removed"
        write(repo, {second: f'import argparse\n\nP = argparse.ArgumentParser()\nP.{ADD_ARGUMENT}("{flag}")\n', "caller_two.sh": f"python {second} {flag}\n"})
        git(repo, "add", "--", second, "caller_two.sh")
        git(repo, "commit", "-qm", "feat: add a flag and its caller")
        write(repo, {second: "VALUE = 1\n"})
        git(repo, "add", "--", second)
        git(repo, "commit", "-qm", "refactor: drop the flag and keep its caller")
        dropped = git(repo, "rev-parse", "HEAD").strip()
        results.append(workflow_range_case(repo, "a_name_added_and_removed_on_the_branch_is_caught", "push", dropped, 1, f"{flag} was removed"))
        shim = partial_failing_git(Path(scratch) / "failing_merge_base", "merge-base", dropped)
        results.append(workflow_range_case(repo, "a_failing_merge_base_fails_the_step_although_it_printed_a_commit", "push", dropped, 1, "merge base", shim))
        local = {**os.environ, "PRE_COMMIT_FROM_REF": clean, "PRE_COMMIT_TO_REF": dropped}
        net = run_command(".venv/bin/python", ["-m", "scripts.quality.repo_checks", "removed-names"], cwd=repo, env=local, timeout=120)
        per_commit = run_command(".venv/bin/python", ["-m", "scripts.quality.repo_checks", "removed-names", "--per-commit"], cwd=repo, env=local, timeout=120)
        checks = {"the_local_form_reads_the_net_diff": net.returncode == 0, "the_hosted_form_reads_each_commit": per_commit.returncode == 1}
        results.append({"case": "the_local_gate_form_reads_the_net_diff_and_the_hosted_form_each_commit", "passed": all(checks.values()), "checks": checks})
        dup = "--dup" + "-flag"
        first, second_script = "scripts/acquisition/range_dup_a.py", "scripts/acquisition/range_dup_b.py"
        definition = f'import argparse\n\nP = argparse.ArgumentParser()\nP.{ADD_ARGUMENT}("{dup}")\n'
        write(repo, {first: definition, second_script: definition})
        git(repo, "add", "--", first, second_script)
        git(repo, "commit", "-qm", "feat: two scripts with one flag name")
        both = git(repo, "rev-parse", "HEAD").strip()
        write(repo, {first: "VALUE = 1\n", second_script: f"VALUE = 2\n# python {first} {dup}\n"})
        git(repo, "add", "--", first, second_script)
        git(repo, "commit", "-qm", "refactor: drop the flag from both scripts")
        dropped_both = git(repo, "rev-parse", "HEAD").strip()
        scoped = run_command(
            ".venv/bin/python",
            ["-m", "scripts.quality.repo_checks", "removed-names"],
            cwd=repo,
            env={**os.environ, "PRE_COMMIT_FROM_REF": both, "PRE_COMMIT_TO_REF": dropped_both},
            timeout=120,
        )
        results.append(
            {
                "case": "one_script_losing_a_flag_does_not_excuse_a_stale_call_to_another_scripts_flag",
                "passed": scoped.returncode == 1 and f"{dup} was removed" in scoped.stdout + scoped.stderr,
                "exit": scoped.returncode,
            }
        )
        return all(result["passed"] for result in results), json.dumps(results, indent=2) + "\n"


def run_verbose_case() -> tuple[bool, str]:
    """Use an installed commit-msg hook to distinguish verbose patch context from actual message text."""
    with tempfile.TemporaryDirectory(prefix="hai_verbose_commit_") as scratch:
        sample = f"before\n{CREDIT}: Claude <example@example.invalid>\nafter\n"
        repo = scratch_repo(Path(scratch), Case("verbose setup", "commit-msg", True, committed={"sample.txt": sample}))
        write(repo, {"sample.txt": sample.replace("after", "changed")})
        git(repo, "add", "--", "sample.txt")
        run_command(str(PRE_COMMIT), ["install", "--hook-type", "commit-msg"], cwd=repo, check=True)
        editor = repo / ".git" / "sample_editor.sh"
        editor.write_text('#!/bin/sh\n{ printf "fix: verbose edit\\n\\n"; cat "$1"; } > "$1.tmp"\nmv "$1.tmp" "$1"\n')
        editor.chmod(0o700)
        verbose = run_command("env", [f"GIT_EDITOR={editor}", "git", "commit", "-v"], cwd=repo)
        stored = git(repo, "log", "-1", "--format=%B")
        direct = run_command("git", ["commit", "--allow-empty", "-qm", f"fix: bad credit\n\n{SCISSORS}\n{CREDIT}: Claude"], cwd=repo)
        ok = verbose.returncode == 0 and CREDIT not in stored and direct.returncode == 1 and "AI attribution in commit message" in direct.stdout + direct.stderr
        return ok, f"verbose commit exit={verbose.returncode}; direct attributed commit exit={direct.returncode}\n" + verbose.stdout + verbose.stderr


def run_privacy_scope_case() -> tuple[bool, str]:
    """Ignored files are never scanned, with or without --all; a hidden untracked file is; --warn exits 0; values never print."""
    with tempfile.TemporaryDirectory(prefix="hai_privacy_scope_") as scratch:
        repo = scratch_repo(Path(scratch), Case("privacy setup", "privacy-scan", True))
        (repo / ".git" / "info" / "exclude").write_text(".venv\n.tools\n__pycache__/\nlocal_notes.md\n")
        write(repo, {"local_notes.md": f"Log at {HOME_PATH}\n"})
        command = ["-m", "scripts.quality.repo_checks", "privacy-scan"]
        default = run_command(str(ROOT / ".venv/bin/python"), command, cwd=repo)
        ignored = run_command(str(ROOT / ".venv/bin/python"), [*command, "--all"], cwd=repo)
        write(repo, {".scratch/notes.md": f"Log at {HOME_PATH}\n"})
        full = run_command(str(ROOT / ".venv/bin/python"), [*command, "--all"], cwd=repo)
        warned = run_command(str(ROOT / ".venv/bin/python"), [*command, "--all", "--warn"], cwd=repo)
        skipped = "local_notes.md"
        marker = ".scratch/notes.md:1: home-directory path with a user name"
        outputs = [result.stdout + result.stderr for result in (default, ignored, full, warned)]
        leaked = any(HOME_PATH in output for output in outputs)
        ok = (
            default.returncode == 0
            and ignored.returncode == 0
            and full.returncode == 1
            and marker in full.stderr
            and not any(skipped in output for output in outputs)
            and warned.returncode == 0
            and f"WARN {marker}" in warned.stderr
            and not leaked
        )
        codes = f"default exit={default.returncode}; --all on ignored exit={ignored.returncode}; --all exit={full.returncode}; --warn exit={warned.returncode}"
        return ok, f"{codes}; value leaked={leaked}\n" + full.stderr


WORK_BOUND_ROOTS = 256
WORK_BOUND_LENGTH = 1024 * 1024
WORK_BOUND_FACTOR = 40  # Peak traced bytes per byte of token. The scan itself uses about 18, and one copy of the long folder per root would use over 250.
WORK_BOUND_PROGRAM = """
import json, sys, time, tracemalloc
from scripts.quality.repo_checks import home_path_found

roots, length, outer, nested = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], sys.argv[4]
results = {}
for name, climbs in (("climbs out", 2 * roots + 1), ("stays inside", 2 * roots)):
    token = outer + nested * roots + "../" * climbs + "j" + "!" * (length - 1)
    tracemalloc.start()
    started = time.perf_counter()
    found = home_path_found(token)
    seconds = time.perf_counter() - started
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    results[name] = {"found": found, "peak": peak, "size": len(token), "seconds": round(seconds, 2)}
print(json.dumps(results))
"""


def run_privacy_work_case() -> tuple[bool, str]:
    """A token with many nested home roots and one long surviving folder is judged with memory in proportion to its size, not to roots times length."""
    outer = f"{HOME_PATH.split('jdoe')[0]}Shared/"
    nested = f"{HOME_WORD}/$USER/"
    result = run_command(
        str(ROOT / ".venv/bin/python"),
        ["-B", "-c", WORK_BOUND_PROGRAM, str(WORK_BOUND_ROOTS), str(WORK_BOUND_LENGTH), outer, nested],
        cwd=ROOT,
        timeout=900,
    )
    try:
        measured = json.loads(result.stdout)
    except ValueError:
        return False, f"no measurement: exit {result.returncode}\n{result.stdout}{result.stderr}"
    expected = {"climbs out": True, "stays inside": False}
    lines = []
    ok = result.returncode == 0
    for name, found in expected.items():
        row = measured[name]
        limit = WORK_BOUND_FACTOR * row["size"]
        good = row["found"] is found and row["peak"] <= limit
        ok = ok and good
        verdict = f"found={row['found']} (expected {found}), peak {row['peak']} bytes for {row['size']} (limit {limit}), {row['seconds']} s"
        lines.append(f"{'ok' if good else 'WRONG'} {name}: {verdict}")
    return ok, "\n".join(lines)


PRIVACY_WORK_RUNNER = ("privacy work bound", run_privacy_work_case)


def account_probe(repo: Path) -> tuple[set[str], str]:
    """Run the privacy scan over probe files in ``repo``; return the probe files it flagged and its output."""
    probes = {"exempt_arn": account_arn(EXEMPT_ACCOUNT), "exempt_field": account_field(EXEMPT_ACCOUNT).strip()}
    probes |= {f"{label}_{kind}": text(account) for label, account in OTHER_ACCOUNTS.items() for kind, text in (("arn", account_arn), ("field", account_field))}
    probes["mixed"] = f"{account_arn(EXEMPT_ACCOUNT)} {account_arn(OTHER_ACCOUNTS['all-two'])}"
    probes["conditional"] = account_conditional(EXEMPT_ACCOUNT, OTHER_ACCOUNTS["all-two"]).strip()
    write(repo, {f"probe/{name}.txt": text + "\n" for name, text in probes.items()})
    result = run_command(str(ROOT / ".venv/bin/python"), ["-m", "scripts.quality.repo_checks", "privacy-scan", "--all"], cwd=repo)
    output = result.stdout + result.stderr
    return {name for name in probes if f"probe/{name}.txt:1: AWS account ID" in output}, output


def run_account_plant_case() -> tuple[bool, str]:
    """The scan flags every probe but the exempt ones; each planted regression is caught by the same probe.

    The first plant exempts any 12 digits, in the match check and in the second-literal check. The second plant keeps the match check but
    stops the second-literal check from counting any number, so only the conditional probe slips through.
    """
    expected = {"all-zero_arn", "all-zero_field", "all-two_arn", "all-two_field", "sequential_arn", "sequential_field", "mixed", "conditional"}
    exempt_any = ("== SYNTHETIC_ACCOUNT_ID", ".isdigit()")
    count_none = ("number != SYNTHETIC_ACCOUNT_ID", "number.isdigit() is False")
    plants = {"any 12 digits exempt": [exempt_any, count_none], "second literal ignored": [count_none]}
    with tempfile.TemporaryDirectory(prefix="hai_account_plant_") as scratch:
        repo = scratch_repo(Path(scratch), Case("account plant", "privacy-scan", True))
        real, real_output = account_probe(repo)
        source = repo / "scripts" / "quality" / "repo_checks.py"
        text = source.read_text()
        leaked = any(account in real_output for account in OTHER_ACCOUNTS.values())  # The fix hint names the exempt ID on purpose.
        ok = real == expected and not leaked
        detail = f"flagged by the real scan: {sorted(real)}; value leaked={leaked}\n"
        for label, replacements in plants.items():
            planted = text
            for old, new in replacements:
                planted = planted.replace(old, new)
            if any(old not in text for old, _ in replacements):
                return False, f"the plant '{label}' did not apply: the comparison is not where this case expects it\n"
            source.write_text(planted)
            caught, _ = account_probe(repo)
            ok = ok and caught != expected
            detail += f"flagged with the plant '{label}': {sorted(caught)}\n"
        return ok, detail


# Each sample runs the check directly, without --warn: (name, files, articles, expected exit, marker in the output).
WRITING_SAMPLES = [
    ("clean writing sample", {"notes.md": CLEAN_WRITING}, [], 0, ""),
    ("comma before a final clause", {"notes.md": CLAUSE_COMMA}, [], 1, "notes.md:1: comma before a final 'and', 'or' or 'nor'"),
    ("Oxford comma in a list", {"notes.md": SERIAL_COMMA}, [], 1, "notes.md:1: comma before a final 'and', 'or' or 'nor'"),
    ("comma before nor", {"notes.md": NOR_COMMA}, [], 1, "notes.md:1: comma before a final 'and', 'or' or 'nor'"),
    ("ISO date in prose", {"notes.md": f"Released on {ISO_DATE} after review.\n"}, [], 1, "notes.md:1: ISO date in prose"),
    ("declared data folder skipped", {"tests/fixtures/report.md": SERIAL_COMMA}, [], 0, ""),
    (
        "allowlisted ISO date",
        {"CHANGELOG.md": f"## v1.0.0 ({ISO_DATE})\n", WRITING_ALLOWLIST: "iso-date CHANGELOG.md -- release headings are machine-readable -- permanent\n"},
        [],
        0,
        "",
    ),
    ("writing allowlist entry without reason", {WRITING_ALLOWLIST: "iso-date notes.md\n"}, [], 1, ".writing_allowlist:1: allowlist entry without a reason"),
    ("em dash outside articles", {"notes.md": f"Rules help {EM_DASH} when they are checked.\n"}, [], 0, ""),
    (
        "em dash in a declared article",
        {"articles/agents.md": f"Rules help {EM_DASH} when they are checked.\n"},
        ["articles/*.md"],
        1,
        "articles/agents.md:1: em dash in article text",
    ),
]


def run_writing_case() -> tuple[bool, str]:
    """Each writing sample exits as expected for its intended cause; --warn reports the same finding but exits 0."""
    results = []
    for name, files, articles, expected, marker in WRITING_SAMPLES:
        with tempfile.TemporaryDirectory(prefix="hai_writing_") as scratch:
            repo = scratch_repo(Path(scratch), Case("writing setup", "writing-check", True))
            write(repo, files)
            command = ["-m", "scripts.quality.repo_checks", "writing-check", *(f"--articles={glob}" for glob in articles)]
            result = run_command(str(ROOT / ".venv/bin/python"), command, cwd=repo)
            warned = run_command(str(ROOT / ".venv/bin/python"), [*command, "--warn"], cwd=repo)
            ok = result.returncode == expected and marker in result.stderr and warned.returncode == 0 and (not marker or f"WARN {marker}" in warned.stderr)
            results.append((ok, f"{'ok' if ok else 'WRONG'} {name}: exit={result.returncode} (expected {expected}); --warn exit={warned.returncode}"))
            if not ok:
                results.append((False, result.stderr + warned.stderr))
    return all(ok for ok, _ in results), "\n".join(line for _, line in results) + "\n"


# Allowlist expiry. An entry reads ``<type> <key> -- <reason> -- <expiry>``: exactly two " -- " separators, and the expiry is exactly
# ``permanent``, ``expires YYYY-MM-DD`` or ``expires-when path-absent:GLOB``. The samples use .writing_allowlist, whose reader is the shared
# allowlist parser, beside a file whose one finding the entry silences.
EXPIRY_NOTES = {"notes.md": f"Released on {ISO_DATE} after review.\n"}
REASON = "release headings are machine-readable"
NBSP = " "


def entry(field: str | None, reason: str = REASON) -> str:
    """One writing-allowlist line in the new format; ``field`` None leaves the expiry field and its separator out."""
    return f"iso-date notes.md -- {reason}" + ("" if field is None else f" -- {field}")


MISSING_EXPIRY = "allowlist entry without an expiry"
MISSING_REASON = "allowlist entry without a reason"
MALFORMED_FIELD = "allowlist entry with a malformed expiry field"
INVALID_DATE = "allowlist entry with an invalid expiry date"
NAMES_EXPIRY = "allowlist entry: the reason may not name an expiry; put it in the expiry field"
TOO_MANY = "allowlist entry with more than two ' -- ' separators"
NON_ASCII = "allowlist lines may contain only printable ASCII"
BAD_SHAPE = "allowlist entry must read '<type> <key> -- <reason> -- <expiry>'"
UNKNOWN_TYPE = "allowlist entry with an unknown type"
# Many lines in one file, each judged on its own line number, with today fixed: (name, line, problem that line must raise or None).
EXPIRY_BATCH = [
    # Accepted.
    ("future date", entry("expires 2999-12-31"), None),
    ("permanent", entry("permanent"), None),
    ("path still tracked", entry("expires-when path-absent:notes.md"), None),
    ("path glob still tracked", entry("expires-when path-absent:*.md"), None),
    ("bracket glob still tracked", entry("expires-when path-absent:[n]otes.md"), None),
    ("glob with a leading closing bracket still tracked", entry("expires-when path-absent:[]n]otes.md"), None),
    ("bracketed reason", entry("permanent", "reviewed [RFC]"), None),
    ("bracketed reasons", entry("expires 2999-12-31", "reviewed [RFC] and [draft]"), None),
    ("bracketed reason with a class", entry("permanent", "reviewed [n]o brackets here"), None),
    # Expired.
    ("past date", entry("expires 2020-01-01"), "allowlist entry expired on 2020-01-01"),
    ("path gone", entry("expires-when path-absent:gone.md"), "allowlist entry expired: no tracked file matches gone.md"),
    ("bracket glob gone", entry("expires-when path-absent:gone[n].md"), "allowlist entry expired: no tracked file matches gone[n].md"),
    (
        "glob that opens a bracket matches nothing",
        entry("expires-when path-absent:[[]notes.md"),
        "allowlist entry expired: no tracked file matches [[]notes.md",
    ),
    # The expiry field must match exactly.
    ("impossible date", entry("expires 2026-13-40"), INVALID_DATE),
    ("date without zero padding", entry("expires 2026-1-5"), MALFORMED_FIELD),
    ("unknown marker", entry("expires-never"), MALFORMED_FIELD),
    ("unknown event kind", entry("expires-when file-exists:notes.md"), MALFORMED_FIELD),
    ("event without an argument", entry("expires-when path-absent:"), MALFORMED_FIELD),
    ("event with trailing text", entry("expires-when path-absent:notes.md extra"), MALFORMED_FIELD),
    ("event with a double space", entry("expires-when  path-absent:notes.md"), MALFORMED_FIELD),
    ("old bracket form of permanent", entry("[permanent]"), MALFORMED_FIELD),
    ("old bracket form of a date", entry("[expires 2999-12-31]"), MALFORMED_FIELD),
    ("permanent with a date", entry("permanent expires 2999-12-31"), MALFORMED_FIELD),
    ("two dates", entry("expires 2999-12-31 expires 2998-12-31"), MALFORMED_FIELD),
    ("capitalized permanent", entry("Permanent"), MALFORMED_FIELD),
    ("upper case permanent", entry("PERMANENT"), MALFORMED_FIELD),
    ("capitalized expires", entry("Expires 2999-12-31"), MALFORMED_FIELD),
    ("double space in a date field", entry("expires  2999-12-31"), MALFORMED_FIELD),
    ("trailing space", entry("permanent "), MALFORMED_FIELD),
    ("leading space", entry(" permanent"), MALFORMED_FIELD),
    ("trailing tab", entry("permanent\t"), MALFORMED_FIELD),
    ("leading tab", entry("\tpermanent"), MALFORMED_FIELD),
    ("trailing nonbreaking space", entry(f"permanent{NBSP}"), MALFORMED_FIELD),
    ("nonbreaking space in a date field", entry(f"expires{NBSP}2999-12-31"), MALFORMED_FIELD),
    ("full width permanent", entry("ｐｅｒｍａｎｅｎｔ"), MALFORMED_FIELD),
    ("Cyrillic a in permanent", entry("permаnent"), MALFORMED_FIELD),
    ("full width brackets", entry("［permanent］"), MALFORMED_FIELD),
    ("zero width space after permanent", entry("permanent​"), MALFORMED_FIELD),
    ("trailing text", entry("permanent forever"), MALFORMED_FIELD),
    ("trailing separator", entry("permanent --"), MALFORMED_FIELD),
    ("note after a date", entry("expires 2999-12-31 (renewed)"), MALFORMED_FIELD),
    ("Arabic-Indic digits in a date", entry("expires ٢٠٩٩-١٢-٣١"), MALFORMED_FIELD),
    # The reason may not name an expiry.
    ("reason with a bracketed date", entry("permanent", "reviewed [expires 2020-01-01]"), NAMES_EXPIRY),
    ("reason with a space after the bracket", entry("permanent", "reviewed [ expires 2020-01-01]"), NAMES_EXPIRY),
    ("reason with a tab after the bracket", entry("permanent", "reviewed [\texpires 2020-01-01]"), NAMES_EXPIRY),
    ("reason with a nonbreaking space after the bracket", entry("permanent", f"reviewed [{NBSP}expires 2020-01-01]"), NAMES_EXPIRY),
    ("reason with an upper case marker", entry("permanent", "reviewed [EXPIRES 2020-01-01]"), NAMES_EXPIRY),
    ("reason with a capitalized permanent", entry("permanent", "reviewed [Permanent]"), NAMES_EXPIRY),
    ("reason with an unclosed permanent", entry("expires 2999-12-31", "reviewed [permanent"), NAMES_EXPIRY),
    ("reason with an event", entry("permanent", "reviewed [expires-when path-absent:gone.md]"), NAMES_EXPIRY),
    ("reason with full width brackets", entry("permanent", "reviewed ［expires 2020-01-01］"), NAMES_EXPIRY),
    ("reason with a full width word", entry("permanent", "ｅｘｐｉｒｅｓ soon"), NAMES_EXPIRY),
    ("reason with a full width upper case word", entry("permanent", "ＰＥＲＭＡＮＥＮＴ exception"), NAMES_EXPIRY),
    ("reason with the plain word", entry("permanent", "permanent exception"), NAMES_EXPIRY),
    # The owner's accepted limit: the expiry field is the only expiry the check reads, and the reason is free text. A reason that spells the word
    # apart passes and the entry counts as permanent. This case pins that behavior so it cannot change without a decision.
    ("reason with a spaced word is accepted", entry("permanent", "reviewed [exp ires 2020-01-01]"), None),
    # Invisible characters inside a word: the reviewer's twelve variants (six characters, two words) must not hide an expiry.
    *(
        (f"reason with U+{ord(char):04X} inside {word}", entry("permanent", f"reviewed [{word[:3]}{char}{word[3:]} 2020-01-01]"), NON_ASCII)
        for char in ("\u200b", "\u200c", "\u200d", "\ufeff", "\u2060", "\u00ad")
        for word in ("expires", "permanent")
    ),
    # Any character outside space to tilde, anywhere on the line.
    ("tab in the reason", entry("permanent", "reviewed\tby the owner"), NON_ASCII),
    ("nonbreaking space in the reason", entry("permanent", f"reviewed{NBSP}by the owner"), NON_ASCII),
    ("accented letter in the reason", entry("permanent", "caf\u00e9 stand-in"), NON_ASCII),
    ("Cyrillic letter in the reason", entry("permanent", "r\u0435viewed"), NON_ASCII),
    ("full width letters in the reason", entry("permanent", "\uff52\uff45\uff56\uff49\uff45\uff57\uff45\uff44"), NON_ASCII),
    ("Cyrillic letter in the key", "iso-date n\u043etes.md -- reviewed -- permanent", NON_ASCII),
    ("line separator in the reason", entry("permanent", "reviewed\u2028by the owner"), NON_ASCII),
    ("next line character in the reason", entry("permanent", "reviewed\u0085by the owner"), NON_ASCII),
    ("vertical tab in the reason", entry("permanent", "reviewed\x0bby the owner"), NON_ASCII),
    ("form feed in the reason", entry("permanent", "reviewed\x0cby the owner"), NON_ASCII),
    ("carriage return at the end", entry("permanent") + "\r", NON_ASCII),
    ("carriage return before the expiry", entry("permanent", "reviewed\r"), NON_ASCII),
    ("control character in the reason", entry("permanent", "reviewed\x1bby the owner"), NON_ASCII),
    ("delete character in the reason", entry("permanent", "reviewed\x7fby the owner"), NON_ASCII),
    ("non-ASCII line that is a comment", "# caf\u00e9", NON_ASCII),
    ("tab-only line", "\t", NON_ASCII),
    # The line shape.
    ("no separator", "iso-date notes.md", MISSING_REASON),
    ("one separator", entry(None), MISSING_EXPIRY),
    ("one separator and an old trailer", entry(None, "reviewed [permanent]"), MISSING_EXPIRY),
    ("three separators", entry("permanent -- extra"), TOO_MANY),
    ("three separators with a date", entry("expires 2999-12-31 -- extra"), TOO_MANY),
    ("empty reason", "iso-date notes.md --  -- permanent", MISSING_REASON),
    ("tab separators", "iso-date notes.md\t--\treviewed\t--\tpermanent", BAD_SHAPE),
    ("nonbreaking space separators", f"iso-date notes.md{NBSP}--{NBSP}reviewed{NBSP}--{NBSP}permanent", BAD_SHAPE),
    ("two spaces in the head", "iso-date  notes.md -- reviewed -- permanent", BAD_SHAPE),
    ("leading space", " iso-date notes.md -- reviewed -- permanent", BAD_SHAPE),
    ("unknown type", "address notes.md -- reviewed -- permanent", UNKNOWN_TYPE),
]
# A line with a character outside space to tilde has one finding, whatever else is wrong with it.
EXPIRY_BATCH = [
    (name, line, NON_ASCII if not line.isascii() or not all(" " <= char <= "~" for char in line) else problem) for name, line, problem in EXPIRY_BATCH
]
# One entry per file, for the rules about suppression, counting and the date boundary: (name, line, extra arguments, exit, stderr marker, stdout marker).
EXPIRY_SINGLE = [
    ("last valid day", entry("expires 2026-11-04"), ["--today", "2026-11-04"], 0, "", ""),
    ("day after the last valid day", entry("expires 2026-11-04"), ["--today", "2026-11-05"], 1, "allowlist entry expired on 2026-11-04", ""),
    ("expired entry still silences its match", entry("expires 2020-01-01"), [], 1, "allowlist entry expired on 2020-01-01", ""),
    ("malformed field still silences its match", entry("permanent forever"), [], 1, MALFORMED_FIELD, ""),
    ("missing expiry still silences its match", entry(None), [], 1, MISSING_EXPIRY, ""),
    ("extra separator still silences its match", entry("permanent -- extra"), [], 1, TOO_MANY, ""),
    ("reason naming an expiry still silences its match", entry("permanent", "reviewed [expires 2020-01-01]"), [], 1, NAMES_EXPIRY, ""),
    ("permanent counted per file and type", entry("permanent"), [], 0, "", "permanent allowlist entries: .writing_allowlist 1 (iso-date 1)"),
]
# Whole files, for what a line-by-line batch cannot hold: (name, file content, line that must raise the ASCII finding, permanent entries counted).
EXPIRY_FILES = [
    ("byte order mark at the start of the file", "\ufeff" + entry("permanent") + "\n", 1, 0),
    ("file with Windows line ends", entry("permanent") + "\r\n" + entry("expires 2999-12-31") + "\r\n", 1, 0),
    ("byte order mark in a comment line", "\ufeff# header\n" + entry("permanent") + "\n", 1, 1),
    ("non-ASCII line after valid lines", entry("permanent") + "\n" + entry("expires 2999-12-31") + "\n" + entry("permanent", "caf\u00e9") + "\n", 3, 1),
]
# The real lists, read from the repository: every entry passes and the parser counts the same entries and permanent entries as a plain text count of the files.
REAL_LIST_FILES = [".linter_allowlist", ".privacy_allowlist", ".cleanup_allowlist", ".writing_allowlist"]
REAL_LISTS = """
import json
import sys
sys.path.insert(0, ".")
from scripts.quality import lint_tools, repo_checks
repo_checks.set_today("2026-11-04")  # The real lists carry dates, so the check pins its own day.
lists = {
    ".linter_allowlist": (list(lint_tools.CHECKS), lint_tools.RULE),
    ".privacy_allowlist": ([*repo_checks.PRIVACY_PATTERNS, "env-value"], repo_checks.PRIVACY_RULE),
    ".cleanup_allowlist": (repo_checks.REMOVED_KINDS + repo_checks.UNUSED_KINDS, repo_checks.CLEANUP_RULE),
    ".writing_allowlist": ([*repo_checks.WRITING_PATTERNS, "internal-reference"], repo_checks.WRITING_RULE),
}
entries = findings = permanent = 0
for name, (kinds, rule) in lists.items():
    found, problems = repo_checks.read_allowlist(name, kinds, rule, "why")
    entries += len(found)
    findings += len(problems)
    permanent += sum(repo_checks.PERMANENT.get(name, {}).values())
print(json.dumps([entries, findings, permanent]))
"""
# An entry that raises its own finding still silences the finding it excuses, so one defect never hides behind another.
SUPPRESSED_FINDING = "notes.md:1: ISO date in prose"
# The other readers share the parser: the privacy and cleanup lists get an expired and a permanent sample each.
EXPIRY_OTHER_LISTS = [
    (
        "privacy list, expired",
        "privacy-scan",
        ".privacy_allowlist",
        "email notes.md -- synthetic contact used in a demo -- expires 2020-01-01",
        1,
        "allowlist entry expired on 2020-01-01",
    ),
    ("privacy list, permanent", "privacy-scan", ".privacy_allowlist", "email notes.md -- synthetic contact used in a demo -- permanent", 0, ""),
    (
        "cleanup list, expired",
        "removed-names",
        ".cleanup_allowlist",
        "python notes.md -- history keeps the old name -- expires 2020-01-01",
        1,
        "allowlist entry expired on 2020-01-01",
    ),
    ("cleanup list, permanent", "removed-names", ".cleanup_allowlist", "python notes.md -- history keeps the old name -- permanent", 0, ""),
]


def run_allowlist_expiry_case() -> tuple[bool, str]:
    """Each line of the new format is accepted or refused for its own cause; the permanent entries are counted in the output."""
    results = []
    python = str(ROOT / ".venv/bin/python")
    with tempfile.TemporaryDirectory(prefix="hai_expiry_") as scratch:
        repo = scratch_repo(Path(scratch), Case("expiry setup", "writing-check", True))
        write(repo, {**EXPIRY_NOTES, WRITING_ALLOWLIST: "\n".join(line for _, line, _ in EXPIRY_BATCH) + "\n"})
        git(repo, "add", "-A")
        batch = run_command(python, ["-m", "scripts.quality.repo_checks", "writing-check", "--today", "2026-11-04"], cwd=repo)
        for number, (name, _, problem) in enumerate(EXPIRY_BATCH, 1):
            where = f"{WRITING_ALLOWLIST}:{number}:"
            ok = f"{where} {problem}" in batch.stderr if problem else where not in batch.stderr
            results.append((ok, f"{'ok' if ok else 'WRONG'} {name}"))
        permanent = sum(1 for _, line, problem in EXPIRY_BATCH if problem is None and line.endswith(" -- permanent"))
        batch_counted = f"permanent allowlist entries: {WRITING_ALLOWLIST} {permanent} (iso-date {permanent})" in batch.stdout
        results.append(
            (batch_counted and "Traceback" not in batch.stderr, f"{'ok' if batch_counted else 'WRONG'} the batch counts {permanent} permanent entries")
        )
    for name, content, line_number, permanent_count in EXPIRY_FILES:
        with tempfile.TemporaryDirectory(prefix="hai_expiry_") as scratch:
            repo = scratch_repo(Path(scratch), Case("expiry setup", "writing-check", True))
            (repo / "notes.md").write_text(EXPIRY_NOTES["notes.md"])
            (repo / WRITING_ALLOWLIST).write_bytes(content.encode())
            git(repo, "add", "-A")
            result = run_command(python, ["-m", "scripts.quality.repo_checks", "writing-check"], cwd=repo)
            ok = result.returncode == 1 and f"{WRITING_ALLOWLIST}:{line_number}: {NON_ASCII}" in result.stderr and "Traceback" not in result.stderr
            file_counted = (
                f"{WRITING_ALLOWLIST} {permanent_count} (" in result.stdout if permanent_count else "permanent allowlist entries" not in result.stdout
            )
            ok = ok and file_counted  # A refused line is never counted as permanent.
            results.append((ok, f"{'ok' if ok else 'WRONG'} {name}: exit={result.returncode}"))
            if not ok:
                results.append((False, result.stdout + result.stderr))
    real = run_command(python, ["-I", "-c", REAL_LISTS], cwd=ROOT)
    counts = real.stdout.strip()
    lines = [line for name in REAL_LIST_FILES for line in (ROOT / name).read_text().splitlines()]
    entry_lines = [line for line in lines if line.strip() and not line.startswith("#")]
    want = [len(entry_lines), 0, sum(1 for line in entry_lines if line.endswith(" -- permanent"))]
    ok = real.returncode == 0 and counts == json.dumps(want)
    got = counts or real.stderr[-200:]
    results.append((ok, f"{'ok' if ok else 'WRONG'} the real lists read {want[0]} entries with 0 findings and {want[2]} permanent (got {got})"))
    for name, line, extra, expected, marker, counted in EXPIRY_SINGLE:
        with tempfile.TemporaryDirectory(prefix="hai_expiry_") as scratch:
            repo = scratch_repo(Path(scratch), Case("expiry setup", "writing-check", True))
            write(repo, {**EXPIRY_NOTES, WRITING_ALLOWLIST: line + "\n"})
            git(repo, "add", "-A")
            result = run_command(python, ["-m", "scripts.quality.repo_checks", "writing-check", *extra], cwd=repo)
            ok = result.returncode == expected and marker in result.stderr and counted in result.stdout
            ok = ok and SUPPRESSED_FINDING not in result.stderr and "Traceback" not in result.stderr
            if name == "reason naming an expiry still silences its match":
                ok = ok and "permanent allowlist entries" not in result.stdout  # An entry with a finding is never counted as permanent.
            results.append((ok, f"{'ok' if ok else 'WRONG'} {name}: exit={result.returncode} (expected {expected})"))
            if not ok:
                results.append((False, result.stdout + result.stderr))
    for name, hook, path, line, expected, marker in EXPIRY_OTHER_LISTS:
        with tempfile.TemporaryDirectory(prefix="hai_expiry_") as scratch:
            repo = scratch_repo(Path(scratch), Case("expiry setup", hook, True))
            base = ALLOWLIST_SOURCE if path == ALLOWLIST else ""
            write(repo, {"notes.md": "Nothing here.\n", path: base + line + "\n"})
            git(repo, "add", "-A")
            result = run_command(python, ["-m", "scripts.quality.repo_checks", hook], cwd=repo)
            last = len(base.splitlines()) + 1
            ok = result.returncode == expected and (not marker or f"{path}:{last}: {marker}" in result.stderr) and "Traceback" not in result.stderr
            results.append((ok, f"{'ok' if ok else 'WRONG'} {name}: exit={result.returncode} (expected {expected})"))
            if not ok:
                results.append((False, result.stdout + result.stderr))
    return all(ok for ok, _ in results), "\n".join(line for _, line in results) + "\n"


DASH_FINDING = "em or en dash in prose"
SEMICOLON_FINDING = "semicolon in prose"
SLASH_FINDING = "slash between words in prose"
HISTORY_FINDING = "history note in a current document"
NO_LONGER_FINDING = "possible history note: 'no longer' describes a change"
WRAP_FINDING = "hard-wrapped prose line"
DASH_LINE = f"Rules help {EM_DASH} when they are checked.\n"
EN_DASH_LINE = f"Rules help {EN_DASH} when they are checked.\n"
SEMICOLON_LINE = "Rules help; checks enforce them.\n"
SLASH_LINE = "Read and/or write the file.\n"
ALL_KINDS = f"{DASH_LINE}\n{SEMICOLON_LINE}\n{SLASH_LINE}"
PUNCTUATION_PROSE = (
    "Rules help when they are checked. Read `a/b;c`, see [the log](https://example.invalid/a;b-c) &amp; write it.\n"
    "Open scripts/quality/timing.sh or scripts/run_ci.sh, then ./a/b, ~/a/b and a/b/c. Hyphens stay in well-known words.\n"
)
CLEAN_PUNCTUATION = f"---\ntitle: Notes\nnote: a {EM_DASH} b; c/d\n---\n\n# Notes\n\n{PUNCTUATION_PROSE}\n```text\n{ALL_KINDS}```\n"
BLOCK_ALL = ("--block", "dash", "--block", "semicolon", "--block", "slash")
# Each history sample sits in its own paragraph, so a finding names its own line and no sample is also a wrapped line.
HISTORY_SENTENCES = (
    "The scan was added by user decision.",
    "Until v8.35.0 every module needed a docstring.",
    "The suppression was approved in [#16](https://example.invalid/issues/16).",
    "This replaced the earlier rule.",
    "Other methods are allowed since Oct 10 2026.",
    "The loader reads the skill folder (v8.27.0).",
    "Confirmed by the user in a review.",
    "Diagrams use the archify skill (it replaced the architecture-diagram skill).",
    "No public repository existed when this section was written, so do not link one.",
    "The payload was capped at 28,672 bytes on the mistaken reading that the files share one budget.",
    "The flag used to accept two values.",
    "Previously, the loader read both folders.",
    "The default changed from 4 to 8.",
    "The table reflects the state as of Oct 2026.",
    "The old loader was replaced by the broker.",
    "The rule was confirmed by the user.",
)
# Statements of a current rule or requirement that share words with the history forms.
CURRENT_RULE_PROSE = (
    "\n\n".join(
        (
            "Each release must be confirmed by the user before deployment.",
            "Exceptions are granted by user decision.",
            "This command replaces the old config file with the new one.",
            "Use the flag in v2.1.0 or later.",
            "Packages from earlier versions must be rebuilt.",
            "Step #3 selects the option.",
            "Previously saved files are reused.",
            "The value is used to run the scan.",
            "The setting can be changed from the command line.",
            "The loader supersedes nothing and keeps every file.",
        )
    )
    + "\n"
)

HISTORY_PROSE = "\n\n".join(HISTORY_SENTENCES) + "\n"
HISTORY_HEADERS = [f"WARN notes.md:{1 + 2 * index}: {HISTORY_FINDING}" for index in range(len(HISTORY_SENTENCES))]
NO_LONGER_PROSE = (
    "The Claude Code loader no longer includes the index.\n\n- The hook no longer runs on push.\n\nThe copy of the loader no longer reads this file.\n"
)
NO_LONGER_HEADERS = [f"WARN notes.md:{line}: {NO_LONGER_FINDING}" for line in (1, 3, 5)]
WRAPPED_PROSE = "This sentence is wrapped\nonto a second line.\n\n1. A list item that\n   continues on the next line.\n"
WRAP_HEADERS = [f"WARN notes.md:{line}: {WRAP_FINDING}" for line in (2, 5)]
# A line is wrapped only when the line before it ends mid-sentence and both lines are plain paragraph prose.
SENTENCE_PER_LINE_PROSE = (
    "Read the file.\nCheck the result.\n\n"
    "Is it done?\nYes!\nGood.\n\n"
    'He said "done."\nNext.\n\n'
    "Run these:\nthe next item\n\n"
    "First line with a break  \nsecond line.\n\n"
    "First line with a break\\\nsecond line.\n\n"
    "a | b\n--- | ---\n1 | 2\n\n"
    "Title\n=====\n\nSubtitle\n--------\n\n"
    '[ref]: https://example.invalid/a\n    "Title of the link"\n'
)
MID_SENTENCE_PROSE = "Ends with a comma,\ncontinues here\nand again here.\n\n- A list item that\n  continues here.\n"
MID_SENTENCE_HEADERS = [f"WARN notes.md:{line}: {WRAP_FINDING}" for line in (2, 3, 6)]
# Indented code is not prose, but an indented paragraph inside a list item is.
INDENTED_CODE_PROSE = (
    "Intro paragraph.\n\n    The hook no longer runs.\n    This replaced the earlier rule; code/with slash \u2014 and dash.\n    Wrapped\n    line.\n\n"
    "\tTab indented: the hook no longer runs.\n\nBack to prose.\n"
)
LIST_CONTINUATION_PROSE = "- Item.\n\n    The hook no longer runs.\n"

# Structure that starts a line of its own is never a continuation: headings, lists, tables, quotes, HTML, link definitions, code and front matter.
UNWRAPPED_PROSE = (
    "---\ntitle: Notes\ndescription: front matter lines\n---\n\n# Title\n\nOne paragraph line.\n\n- item one\n- item two\n1. first\n2. second\n\n"
    "| a | b |\n| - | - |\n\n> quoted\n> quoted again\n\n<!-- note -->\n[ref]: https://example.invalid/a\n\n```text\nfenced\nfenced two\n```\n\nLast line.\n"
)
CODE_AND_FENCE_PROSE = (
    "Run `no longer` in code and keep `This replaced the earlier rule.` as an example.\n\n"
    "```text\nThis replaced the earlier rule.\nThe hook no longer runs.\n```\n"
)
KIND_FOLDERS = ("tests/fixtures/", "dbt/seeds/")
INTERNAL_REFERENCE_SAMPLES = [
    ("numbered owner decision", "Owner decision 12 permits the reader.", "owner-record"),
    ("distant marked user approval", "User approval for the reader is #12.", "owner-record"),
    ("owner decision with a lettered label", "The owner decision W0-1 permits the reader.", "owner-record"),
    ("owner decision with a short label", "Owner decision U-4 sets the domain.", "owner-record"),
    ("owner decision with a day-month date", "Kept by owner decision, 23 October 2026.", "owner-record"),
    ("owner decision with a month-day date", "Kept by owner decision, Oct 23.", "owner-record"),
    ("owner decision with a prefixed number", "Owner decision D12 permits the reader.", "owner-record"),
    ("decision section used as provenance", "The contract follows D1 section 2 item 4.", "owner-record"),
    ("compact decision used as provenance", "The contract follows D12.", "owner-record"),
    ("dated approval record", "Approved Oct 25 2026.", "owner-record"),
    ("direct marked approval record", "Approved #12 permits the reader.", "owner-record"),
    ("direct prefixed approval record", "Approval D12 permits the reader.", "owner-record"),
    ("dated owner decision", "The owner decision (Oct 25 2026) permits the reader.", "owner-record"),
    ("dated owner approval", "Owner-approved on 2026-10-25.", "owner-record"),
    ("numbered user approval", "User approval #12 permits the reader.", "owner-record"),
    ("numbered amendment", "Amendment 14 defines the contract.", "amendment"),
    ("compact amendment label", "The rule follows A273.", "amendment"),
    ("explicit amendment with a cell label", "Amendment A1 defines the contract.", "amendment"),
    ("explicit amendment with a paper label", "Amendment A4 defines the contract.", "amendment"),
    ("cell label before amendment wording", "The A1 amendment defines the contract.", "amendment"),
    ("paper label before amendment wording", "The A4 amendment defines the contract.", "amendment"),
    ("longer compact amendment starting with one", "The rule follows A14.", "amendment"),
    ("longer compact amendment starting with four", "The rule follows A40.", "amendment"),
    ("work package with a lowercase suffix", "The work belongs to P7d.", "work-package"),
    ("another work package", "The work belongs to P3c.", "work-package"),
    ("wave work package", "The work belongs to WV2-001.", "work-package"),
    ("numbered wave", "The work belongs to wave 3.", "wave"),
    ("hyphenated wave", "The work belongs to Wave-3.", "wave"),
    ("numbered review round", "The review round 2 accepted the change.", "review-round"),
    ("compact review round label", "The R2 review accepted the change.", "review-round"),
    ("hyphenated review round", "The review-r3 accepted the change.", "review-round"),
    ("review round before its label", "Round 2 review accepted the change.", "review-round"),
    ("issue number as provenance", "The rule follows issue 76.", "issue-provenance"),
    ("hash issue number as provenance", "The rule follows #76.", "issue-provenance"),
    ("local plan path", "Read plans/design.md.", "plans-path"),
    ("relative local plan path", "Read ../plans/design.md.", "plans-path"),
    ("inline local plan path", "Read `plans/design.md`.", "plans-path"),
    ("local plan link destination", "Read [the design](plans/design.md).", "plans-path"),
]
INTERNAL_NEAR_MISSES = (
    "Owner decisions describe scope. Approval is required. Amendments describe a rule. The wave breaks. Review rounds have names.\n\n"
    "The approved code hashes of 9 collectors. Approval is required for 2 users. Approved: 5 of 10 rows.\n\n"
    "Approved 5 rows were loaded. Approval 2 weeks later. User approval takes 2 steps. Encoded text +P7d/ and =P3c stays.\n\n"
    "Owner decisions in October are logged. User approval for re-entry stays.\n\n"
    "User approved utf-8 output. Owner approved sha-256 checksums. User approved top-10 results.\n\n"
    "User approval retains 3 decimal places. Owner approval needs 2 marked rows. User approval uses 4 augmented rows and 2 decoders.\n\n"
    "Approved Decimal 3 2026 values stay. Owner approval may 2 rows.\n\n"
    "Observed on Oct 25 2026. Versions v9.10.0 and 1.2.3 stay. RQ4, P3.12, P7docs and WPA stay.\n\n"
    "See https://example.invalid/plans/P7d/wave-3/issue/76#76 or [the source](https://example.invalid/plans/A273).\n"
)
WRITING_KIND_SAMPLES = [
    ("em dash warns", {"notes.md": DASH_LINE}, (), 0, [f"WARN notes.md:1: {DASH_FINDING}"]),
    ("en dash warns", {"notes.md": EN_DASH_LINE}, (), 0, [f"WARN notes.md:1: {DASH_FINDING}"]),
    ("semicolon warns", {"notes.md": SEMICOLON_LINE}, (), 0, [f"WARN notes.md:1: {SEMICOLON_FINDING}"]),
    ("slash warns", {"notes.md": SLASH_LINE}, (), 0, [f"WARN notes.md:1: {SLASH_FINDING}"]),
    (
        "three kinds on three lines warn",
        {"notes.md": ALL_KINDS},
        (),
        0,
        [f"WARN notes.md:1: {DASH_FINDING}", f"WARN notes.md:3: {SEMICOLON_FINDING}", f"WARN notes.md:5: {SLASH_FINDING}"],
    ),
    ("code, URL, path, entity, fence and front matter are not reported", {"notes.md": CLEAN_PUNCTUATION}, (), 0, []),
    (
        "an entity keeps the word boundary of a time word",
        {"notes.md": "The loader currently&nbsp;reads input.\n"},
        (),
        1,
        ["notes.md:1: time-bound word in prose"],
    ),
    ("an entity keeps the word boundary of a date", {"notes.md": "Released on&nbsp;2026-10-14&nbsp;after review.\n"}, (), 1, ["notes.md:1: ISO date in prose"]),
    ("--block dash blocks an em dash", {"notes.md": DASH_LINE}, ("--block", "dash"), 1, [f"notes.md:1: {DASH_FINDING}"]),
    ("--block dash blocks an en dash", {"notes.md": EN_DASH_LINE}, ("--block", "dash"), 1, [f"notes.md:1: {DASH_FINDING}"]),
    ("--block semicolon blocks", {"notes.md": SEMICOLON_LINE}, ("--block", "semicolon"), 1, [f"notes.md:1: {SEMICOLON_FINDING}"]),
    ("--block slash blocks", {"notes.md": SLASH_LINE}, ("--block", "slash"), 1, [f"notes.md:1: {SLASH_FINDING}"]),
    (
        "--block of every kind blocks all three",
        {"notes.md": ALL_KINDS},
        BLOCK_ALL,
        1,
        [f"notes.md:1: {DASH_FINDING}", f"notes.md:3: {SEMICOLON_FINDING}", f"notes.md:5: {SLASH_FINDING}"],
    ),
    ("--block of one kind leaves the others warning", {"notes.md": DASH_LINE}, ("--block", "semicolon"), 0, [f"WARN notes.md:1: {DASH_FINDING}"]),
    ("--block of every kind passes clean prose", {"notes.md": CLEAN_PUNCTUATION}, BLOCK_ALL, 0, []),
    ("--warn keeps a blocked kind a warning", {"notes.md": DASH_LINE}, ("--block", "dash", "--warn"), 0, [f"WARN notes.md:1: {DASH_FINDING}"]),
    (
        "allowlist entry without reason",
        {"notes.md": DASH_LINE, WRITING_ALLOWLIST: "dash notes.md\n"},
        (),
        1,
        [f"WARN notes.md:1: {DASH_FINDING}", f"{WRITING_ALLOWLIST}:1: allowlist entry without a reason"],
    ),
    (
        "allowlist entry with a reason silences only its kind",
        {"notes.md": f"{SEMICOLON_LINE}\n{DASH_LINE}", WRITING_ALLOWLIST: "semicolon notes.md -- a quoted sentence keeps its semicolon -- permanent\n"},
        (),
        0,
        [f"WARN notes.md:3: {DASH_FINDING}"],
    ),
    (
        "allowlist entry silences a blocked kind",
        {"notes.md": SEMICOLON_LINE, WRITING_ALLOWLIST: "semicolon notes.md -- a quoted sentence keeps its semicolon -- permanent\n"},
        ("--block", "semicolon"),
        0,
        [],
    ),
    (
        "allowlist entry is by path",
        {"notes.md": SLASH_LINE, WRITING_ALLOWLIST: "slash docs/*.md -- another folder -- permanent\n"},
        (),
        0,
        [f"WARN notes.md:1: {SLASH_FINDING}"],
    ),
    (
        "an em dash in a declared article is one finding",
        {"articles/a.md": DASH_LINE},
        ("--articles=articles/*.md",),
        1,
        ["articles/a.md:1: em dash in article text"],
    ),
    ("an en dash in a declared article warns", {"articles/a.md": EN_DASH_LINE}, ("--articles=articles/*.md",), 0, [f"WARN articles/a.md:1: {DASH_FINDING}"]),
    *[(f"{folder} is not scanned", {f"{folder}notes.md": ALL_KINDS}, BLOCK_ALL, 0, []) for folder in KIND_FOLDERS],
    ("history notes warn", {"notes.md": HISTORY_PROSE}, (), 0, HISTORY_HEADERS),
    ("--block history blocks", {"notes.md": "This replaced the earlier rule.\n"}, ("--block", "history"), 1, [f"notes.md:1: {HISTORY_FINDING}"]),
    (
        "--warn keeps blocked history a warning",
        {"notes.md": "This replaced the earlier rule.\n"},
        ("--block", "history", "--warn"),
        0,
        [f"WARN notes.md:1: {HISTORY_FINDING}"],
    ),
    ("history is exempt in the changelog", {"CHANGELOG.md": "This replaced the earlier rule.\n"}, (), 0, []),
    (
        "history allowlist entry silences its kind",
        {"notes.md": "This replaced the earlier rule.\n", WRITING_ALLOWLIST: "history notes.md -- the text quotes a past rule -- permanent\n"},
        ("--block", "history"),
        0,
        [],
    ),
    ("no longer warns", {"notes.md": NO_LONGER_PROSE}, (), 0, NO_LONGER_HEADERS),
    ("no longer warns in the changelog", {"CHANGELOG.md": "The hook no longer runs on push.\n"}, (), 0, [f"WARN CHANGELOG.md:1: {NO_LONGER_FINDING}"]),
    (
        "no longer allowlist entry silences its kind",
        {"notes.md": "The hook no longer runs on push.\n", WRITING_ALLOWLIST: "no-longer notes.md -- a current rule keeps the phrase -- permanent\n"},
        (),
        0,
        [],
    ),
    ("hard-wrapped lines warn", {"notes.md": WRAPPED_PROSE}, (), 0, WRAP_HEADERS),
    ("--block wrap blocks", {"notes.md": "Wrapped\nline.\n"}, ("--block", "wrap"), 1, [f"notes.md:2: {WRAP_FINDING}"]),
    (
        "wrap allowlist entry silences its kind",
        {"notes.md": "Wrapped\nline.\n", WRITING_ALLOWLIST: "wrap notes.md -- quoted text -- permanent\n"},
        ("--block", "wrap"),
        0,
        [],
    ),
    ("structure that starts a line is not a wrapped line", {"notes.md": UNWRAPPED_PROSE}, ("--block", "wrap"), 0, []),
    ("one sentence per line, breaks, tables and headings are not wrapped lines", {"notes.md": SENTENCE_PER_LINE_PROSE}, ("--block", "wrap"), 0, []),
    ("a line that ends mid-sentence is a wrapped line", {"notes.md": MID_SENTENCE_PROSE}, (), 0, MID_SENTENCE_HEADERS),
    (
        "indented code holds no history note, no longer, wrapped line or punctuation kind",
        {"notes.md": INDENTED_CODE_PROSE},
        (*BLOCK_ALL, "--block", "history", "--block", "wrap"),
        0,
        [],
    ),
    ("a paragraph indented inside a list item is prose", {"notes.md": LIST_CONTINUATION_PROSE}, (), 0, [f"WARN notes.md:3: {NO_LONGER_FINDING}"]),
    ("current rules and requirements are not history notes", {"notes.md": CURRENT_RULE_PROSE}, ("--block", "history"), 0, []),
    ("code spans and fences hold no history note or no longer", {"notes.md": CODE_AND_FENCE_PROSE}, ("--block", "history"), 0, []),
    *[
        (name, {"notes.txt": sample + "\n"}, (), 0, [f"WARN notes.txt:1: internal reference ({pattern})"])
        for name, sample, pattern in INTERNAL_REFERENCE_SAMPLES
    ],
    ("internal reference near misses", {"notes.md": INTERNAL_NEAR_MISSES}, ("--block", "internal-reference"), 0, []),
    (
        "review dates and versions are not review rounds",
        {"notes.txt": "Live review 2026-09-18. Review 2.3.1 describes a version.\n"},
        ("--block", "internal-reference"),
        0,
        [],
    ),
    (
        "statistical metrics and row labels are not review rounds",
        {"notes.json": '{"metric": "R2", "rows": ["r1", "r2"]}\n'},
        ("--block", "internal-reference"),
        0,
        [],
    ),
    (
        "UUID fragments are not amendments",
        {"notes.txt": "Resource 78622c04-a158-4c95-8ea3-7660725e9526 or 78622C04-A158-4C95-8EA3-7660725E9526.\n"},
        ("--block", "internal-reference"),
        0,
        [],
    ),
    (
        "lowercase technical labels are not amendment numbers",
        {"notes.json": '{"columns": ["a1", "a12"]}\n'},
        ("--block", "internal-reference"),
        0,
        [],
    ),
    (
        "quality guide approval prose remains ordinary",
        {
            "notes.txt": "The whitelist also keeps one dead WONDER constant until the next WONDER code version, because its file is in the "
            "approved code hashes of 9 collectors.\n"
        },
        ("--block", "internal-reference"),
        0,
        [],
    ),
    (
        "base64 fragments are not amendment labels",
        {"notes.html": "<style>src:url(data:font/woff2;base64,rNUE+A5/rgt/A6=abc=A7/xyz)</style>\n"},
        ("--block", "internal-reference"),
        0,
        [],
    ),
    (
        "technical names are not work packages",
        {"notes.txt": "Use p4d or p4d.24xlarge for compute. P2P links and P4d.24xlarge stay technical.\n"},
        ("--block", "internal-reference"),
        0,
        [],
    ),
    (
        "public paper and cell labels are not amendments",
        {"notes.txt": "Print on A4 paper. Use cell A1. A1C describes a metric.\n"},
        ("--block", "internal-reference"),
        0,
        [],
    ),
    (
        "plan ignore exception stays within its path",
        {
            ".gitignore": "/plans/\n",
            "notes.txt": "Read plans/design.md.\n",
            WRITING_ALLOWLIST: "internal-reference .gitignore -- local planning records must stay untracked -- permanent\n",
        },
        ("--block", "internal-reference"),
        1,
        ["notes.txt:1: internal reference (plans-path)"],
    ),
    (
        "tracked public seed CSVs remain in scope",
        {"dbt/seeds/labels.csv": "label\nwave 3\n"},
        (),
        0,
        ["WARN dbt/seeds/labels.csv:2: internal reference (wave)"],
    ),
    ("Markdown inline plan path", {"notes.md": "Read `plans/design.md`.\n"}, (), 0, ["WARN notes.md:1: internal reference (plans-path)"]),
    ("Markdown local link target", {"notes.md": "Read [the design](plans/design.md).\n"}, (), 0, ["WARN notes.md:1: internal reference (plans-path)"]),
    ("Markdown fenced local path", {"notes.md": "```text\nplans/design.md\n```\n"}, (), 0, ["WARN notes.md:2: internal reference (plans-path)"]),
    ("internal references block on request", {"notes.txt": "wave 3\n"}, ("--block", "internal-reference"), 1, ["notes.txt:1: internal reference (wave)"]),
    (
        "another blocked kind leaves internal references warning",
        {"notes.txt": "wave 3\n"},
        ("--block", "dash"),
        0,
        ["WARN notes.txt:1: internal reference (wave)"],
    ),
    (
        "global warn keeps internal references nonblocking",
        {"notes.txt": "wave 3\n"},
        ("--block", "internal-reference", "--warn"),
        0,
        ["WARN notes.txt:1: internal reference (wave)"],
    ),
    (
        "reasoned internal reference allowlist",
        {"notes.txt": "wave 3\n", WRITING_ALLOWLIST: "internal-reference *.txt -- synthetic example -- permanent\n"},
        ("--block", "internal-reference"),
        0,
        [],
    ),
    (
        "internal reference allowlist needs a reason",
        {"notes.txt": "wave 3\n", WRITING_ALLOWLIST: "internal-reference *.txt\n"},
        (),
        1,
        ["WARN notes.txt:1: internal reference (wave)", f"{WRITING_ALLOWLIST}:1: allowlist entry without a reason"],
    ),
    (
        "internal reference allowlist checks the kind",
        {"notes.txt": "wave 3\n", WRITING_ALLOWLIST: "semicolon *.txt -- another kind -- permanent\n"},
        (),
        0,
        ["WARN notes.txt:1: internal reference (wave)"],
    ),
    (
        "internal reference allowlist checks the path",
        {"notes.txt": "wave 3\n", WRITING_ALLOWLIST: "internal-reference docs/* -- another folder -- permanent\n"},
        (),
        0,
        ["WARN notes.txt:1: internal reference (wave)"],
    ),
    (
        "Python comment source line",
        {"sample.py": 'LABEL = "wave 3"\n# wave 3\n'},
        (),
        0,
        ["WARN sample.py:2: internal reference (wave)"],
    ),
    (
        "Python docstring source line",
        {"sample.py": 'def sample() -> None:\n    """The rule follows A273."""\n'},
        (),
        0,
        ["WARN sample.py:2: internal reference (amendment)"],
    ),
    ("shell comment", {"sample.sh": 'value="wave 3" # wave 3\n'}, (), 0, ["WARN sample.sh:1: internal reference (wave)"]),
    ("SQL comment", {"sample.sql": "select '-- wave 3'; -- wave 3\n"}, (), 0, ["WARN sample.sql:1: internal reference (wave)"]),
    ("C style block comment", {"sample.go": 'var value = "wave 3"\n/* rules\nwave 3\n*/\n'}, (), 0, ["WARN sample.go:3: internal reference (wave)"]),
    ("C style line comment", {"sample.js": 'const value = "wave 3"; // wave 3\n'}, (), 0, ["WARN sample.js:1: internal reference (wave)"]),
    ("JSON nested string value", {"sample.json": '{\n  "wave 3": ["wave 3"]\n}\n'}, (), 0, ["WARN sample.json:2: internal reference (wave)"]),
    ("JSON escaped string value", {"sample.json": '{"note": "wa\\u0076e 3"}\n'}, (), 0, ["WARN sample.json:1: internal reference (wave)"]),
    ("JSON keys are not prose", {"sample.json": '{"wave 3": "ordinary value"}\n'}, ("--block", "internal-reference"), 0, []),
    (
        "code literals are not comments",
        {"sample.py": 'LABEL = "# wave 3"\n', "sample.sh": 'value="# wave 3"\n', "sample.sql": "select '-- wave 3';\n"},
        ("--block", "internal-reference"),
        0,
        [],
    ),
    (
        "tracked fixtures remain in internal reference scope",
        {"tests/fixtures/notes.txt": "wave 3\n"},
        (),
        0,
        ["WARN tests/fixtures/notes.txt:1: internal reference (wave)"],
    ),
    (
        "all internal patterns on one line",
        {"notes.txt": "Owner decision 12 follows amendment 14 for P7d in wave 3 after review round 2 per issue 76 at plans/design.md.\n"},
        (),
        0,
        [
            f"WARN notes.txt:1: internal reference ({pattern})"
            for pattern in ("owner-record", "amendment", "work-package", "wave", "review-round", "issue-provenance", "plans-path")
        ],
    ),
]
WRITING_KIND_ERRORS = [
    ("--block with an unknown kind", ("--block", "colon"), "invalid choice: 'colon'"),
    ("--block of a kind that always blocks", ("--block", "comma-and"), "invalid choice: 'comma-and'"),
    ("--block of a kind that a person reviews", ("--block", "no-longer"), "invalid choice: 'no-longer'"),
]


def finding_headers(stderr: str, paths: list[str]) -> list[str]:
    """Return finding headers for the supplied fixture paths in CLI output."""
    return [line for line in stderr.splitlines() if line.removeprefix("WARN ").startswith(tuple(f"{path}:" for path in paths))]


def writing_kinds_repo(scratch: str) -> Path:
    """Create a writing fixture repository with only case inputs in its tracked inventory."""
    repo = scratch_repo(Path(scratch), Case("writing setup", "writing-check", True))
    for relative in git(repo, "ls-files", "*.md").splitlines():
        (repo / relative).unlink()
    git(repo, "rm", "-r", "--cached", "--", ".")
    return repo


def run_writing_kinds_case() -> tuple[bool, str]:
    """Run writing fixtures through the CLI and return their combined outcome and log."""
    results = []
    for name, files, arguments, expected, headers in WRITING_KIND_SAMPLES:
        with tempfile.TemporaryDirectory(prefix="hai_writing_kinds_") as scratch:
            repo = writing_kinds_repo(scratch)
            write(repo, files)
            git(repo, "add", "--", *files)
            result = run_command(str(ROOT / ".venv/bin/python"), ["-m", "scripts.quality.repo_checks", "writing-check", *arguments], cwd=repo)
            found = finding_headers(result.stderr, list(files))
            ok = result.returncode == expected and found == headers and "Traceback" not in result.stderr
            results.append(
                (ok, f"{'ok' if ok else 'WRONG'} {name}: exit={result.returncode} (expected {expected}); findings={len(found)} (expected {len(headers)})")
            )
            if not ok:
                results.append((False, result.stderr))
    for name, arguments, message in WRITING_KIND_ERRORS:
        with tempfile.TemporaryDirectory(prefix="hai_writing_kinds_") as scratch:
            repo = writing_kinds_repo(scratch)
            write(repo, {"notes.md": ALL_KINDS})
            result = run_command(str(ROOT / ".venv/bin/python"), ["-m", "scripts.quality.repo_checks", "writing-check", *arguments], cwd=repo)
            named = all(kind in result.stderr for kind in ("dash", "semicolon", "slash"))
            ok = result.returncode == 2 and message in result.stderr and named and not finding_headers(result.stderr, ["notes.md"])
            results.append((ok, f"{'ok' if ok else 'WRONG'} {name}: exit={result.returncode} (expected 2)"))
            if not ok:
                results.append((False, result.stderr))
    with tempfile.TemporaryDirectory(prefix="hai_writing_scope_") as scratch:
        repo = writing_kinds_repo(scratch)
        scope_files: dict[str, str | bytes] = {"tracked.txt": "wave 3\n", "untracked.txt": "wave 3\n", "binary.bin": b"\x00wave 3\xff"}
        write(repo, scope_files)
        (repo / "linked.txt").symlink_to(repo / "untracked.txt")
        git(repo, "add", "--", "tracked.txt", "binary.bin", "linked.txt")
        result = run_command(str(ROOT / ".venv/bin/python"), ["-m", "scripts.quality.repo_checks", "writing-check"], cwd=repo)
        found = finding_headers(result.stderr, ["tracked.txt", "untracked.txt", "binary.bin", "linked.txt"])
        ok = result.returncode == 0 and found == ["WARN tracked.txt:1: internal reference (wave)"]
        results.append((ok, f"{'ok' if ok else 'WRONG'} tracked text only, without symlinks or binary: {found}"))
    return all(ok for ok, _ in results), "\n".join(line for _, line in results) + "\n"


def typecheck_fixture(repo: Path) -> None:
    repo.mkdir()
    git(repo, "init", "-q")
    for relative in ("scripts/process.py", "scripts/quality/typecheck.py", "pyproject.toml"):
        write(repo, {relative: (ROOT / relative).read_text()})
    write(repo, {"scripts/pending_leaf.py": "import hai_typecheck_missing_dependency\n", "scripts/clean.py": "VALUE = 1\n"})


def fixture_typecheck(repo: Path, pending: dict[str, dict[str, str]]) -> tuple[int, str]:
    write(repo, {"config/quality/mypy_runtimes.json": json.dumps({"pending": pending}) + "\n"})
    result = run_command(
        str(ROOT / ".venv/bin/python"),
        ["-m", "scripts.quality.typecheck", "--check-untyped-defs", "--disallow-untyped-defs"],
        cwd=repo,
        env={**os.environ, "MYPY_CACHE_DIR": str(repo.parent / "cache")},
        timeout=180,
    )
    return result.returncode, result.stdout + result.stderr


def run_typecheck_import_case() -> tuple[bool, str]:
    with tempfile.TemporaryDirectory(prefix="hai_typecheck_imports_") as scratch:
        repo = Path(scratch) / "repo"
        typecheck_fixture(repo)
        importers = {
            "scripts/acquisition/direct.py": "from scripts import pending_leaf\n",
            "scripts/acquisition/relative.py": "from .. import pending_leaf\n",
            "scripts/orchestration/transitive.py": "from scripts.acquisition import relative\n",
            "scripts/package/__init__.py": "from scripts import pending_leaf\n",
            "scripts/package/child.py": "VALUE = 1\n",
            "scripts/quality/package_user.py": "import scripts.package.child\n",
            "scripts/acquisition/cycle_a.py": "from scripts.acquisition import cycle_b\n",
            "scripts/acquisition/cycle_b.py": "from scripts.acquisition import cycle_a\nfrom scripts import pending_leaf\n",
        }
        dynamic = {
            "scripts/acquisition/dynamic.py": 'import importlib\n\nimportlib.import_module("scripts.pending_leaf")\n',
            "scripts/acquisition/builtin_dynamic.py": '__import__("scripts.pending_leaf")\n',
        }
        write(repo, {**importers, **dynamic})
        code, output = fixture_typecheck(repo, {"scripts/pending_leaf.py": {"runtime": "fixture", "reason": "synthetic missing dependency"}})
        checks = {
            "strict selection passes": code == 0 and "Success: no issues found" in output,
            "all importers name the cause": all(f"pending {path}: imports a pending module" in output for path in importers),
            "dynamic imports stay checked": all(f"pending {path}:" not in output for path in dynamic),
            "clean source remains checked": "pending scripts/clean.py:" not in output and "MyPy runtime host: 5 source files," in output,
        }
        return all(checks.values()), json.dumps(checks) + "\n" + output


def run_typecheck_manifest_case() -> tuple[bool, str]:
    with tempfile.TemporaryDirectory(prefix="hai_typecheck_manifest_") as scratch:
        repo = Path(scratch) / "repo"
        typecheck_fixture(repo)
        entry = {"runtime": "fixture", "reason": "synthetic missing dependency"}
        pending = {"scripts/pending_leaf.py": entry}
        cases: tuple[tuple[str, dict[str, dict[str, str]], str, int, str], ...] = (
            ("matching leaves", pending, "import hai_typecheck_missing_dependency\n", 0, "pending manifest matches 1 import-diagnostic files"),
            (
                "clean extra leaf",
                {**pending, "scripts/clean.py": entry},
                "import hai_typecheck_missing_dependency\n",
                1,
                "stale manifest leaf: scripts/clean.py",
            ),
            ("missing leaf", {}, "import hai_typecheck_missing_dependency\n", 1, "missing manifest leaf: scripts/pending_leaf.py"),
            ("resolved import", pending, "import math\n", 1, "stale manifest leaf: scripts/pending_leaf.py"),
            ("source error in pending leaf", pending, "import hai_typecheck_missing_dependency\nVALUE: int = 'bad'\n", 1, "non-import MyPy diagnostic"),
        )
        results = []
        for name, manifest, source, expected, marker in cases:
            write(repo, {"scripts/pending_leaf.py": source})
            code, output = fixture_typecheck(repo, manifest)
            ok = code == expected and marker in output and "Traceback" not in output
            results.append((ok, f"{'ok' if ok else 'WRONG'} {name}: exit {code}, expected {expected}\n{output}"))
        return all(ok for ok, _ in results), "\n".join(output for _, output in results)


def run_typecheck_cold_case() -> tuple[bool, str]:
    results = []
    for runtime in ("host", "acquisition", "orchestration"):
        with tempfile.TemporaryDirectory(prefix=f"hai_mypy_{runtime}_") as cache:
            result = run_command(
                str(ROOT / ".venv/bin/python"),
                ["-m", "scripts.quality.typecheck", "--runtime", runtime, "--check-untyped-defs", "--disallow-untyped-defs"],
                cwd=ROOT,
                env={**os.environ, "MYPY_CACHE_DIR": cache},
                timeout=900,
            )
            output = result.stdout + result.stderr
            ok = result.returncode == 0 and f"MyPy runtime {runtime}:" in output and "Success: no issues found" in output
            results.append((ok, f"{'ok' if ok else 'WRONG'} cold-cache {runtime}: exit {result.returncode}\n{output}"))
    return all(ok for ok, _ in results), "\n".join(output for _, output in results)


def run_lint_coverage_case() -> tuple[bool, str]:
    result = run_command(str(ROOT / ".venv/bin/python"), ["-m", "scripts.quality.run_lint_coverage_e2e"], cwd=ROOT, timeout=1200)
    return result.returncode == 0, result.stdout + result.stderr


def run_worktree_checks_case() -> tuple[bool, str]:
    result = run_command(str(ROOT / ".venv/bin/python"), ["-m", "scripts.quality.run_worktree_checks_e2e"], cwd=ROOT, timeout=1200)
    return result.returncode == 0, result.stdout + result.stderr


SUITE = "run_checks_e2e"
RUNNERS: tuple[tuple[str, Callable[[], tuple[bool, str]]], ...] = (
    ("commit-range", run_range_case),
    ("workflow ranges", run_workflow_range_case),
    ("verbose commit", run_verbose_case),
    ("privacy scope", run_privacy_scope_case),
    PRIVACY_WORK_RUNNER,
    ("account exemption plant", run_account_plant_case),
    ("writing samples", run_writing_case),
    ("writing kinds", run_writing_kinds_case),
    ("allowlist expiry", run_allowlist_expiry_case),
    *((label, partial(run_planted_regression_case, index, first, stop)) for label, index, first, stop in planted_regression_units()),
    ("importer check on a pushed range", run_importer_range_case),
    ("typecheck import closure", run_typecheck_import_case),
    ("typecheck pending manifest", run_typecheck_manifest_case),
    ("typecheck cold caches", run_typecheck_cold_case),
    ("source lint coverage", run_lint_coverage_case),
    ("head and pinned-tools checks", run_worktree_checks_case),
)


def inventory() -> list[str]:
    """The name of every case of a complete run, in run order: each hook sample, then each extra runner. The shards of a sharded run split this list."""
    names = [case.name for case in CASES] + [label for label, _ in RUNNERS]
    if len(names) != len(set(names)):
        raise ValueError("the case and runner names must be unique")
    return names


def main() -> int:
    """Run every case and report the ones whose hook did not behave as expected."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--hook-probe", action="store_true", help="run only the clean sample, for the commit-hook E2E")
    parser.add_argument("--hook", help="iteration run: only the cases of this hook, with no extra runners (not commit evidence)")
    parser.add_argument("--case", action="append", default=[], help="with --hook: only the cases whose name contains this text (repeatable)")
    parser.add_argument(
        "--list-cases", action="store_true", help="print the JSON list of the case names of a complete run (of one shard with --shard) and stop"
    )
    parser.add_argument(
        "--shard", help="run only shard K/N of the complete run (also CHECKS_SHARD): the cases of case_shards, packed by the saved costs in shard_costs.json"
    )
    parser.add_argument("--shard-directory", type=Path, help="with --shard: the folder for the shard's manifest.json (also CHECKS_SHARD_DIRECTORY)")
    args = parser.parse_args()
    # The hook entry reads the shard from the environment (pre-commit passes the environment through). The cases run pre-commit again, so they never inherit it.
    shard = args.shard or os.environ.pop("CHECKS_SHARD", "") or None
    directory = args.shard_directory or (Path(os.environ["CHECKS_SHARD_DIRECTORY"]) if os.environ.get("CHECKS_SHARD_DIRECTORY") else None)
    os.environ.pop("CHECKS_SHARD", None)
    os.environ.pop("CHECKS_SHARD_DIRECTORY", None)
    if shard and (args.hook_probe or args.hook or args.case):
        parser.error("--shard runs part of the complete run: it cannot combine with --hook-probe, --hook or --case")
    names = inventory()
    selected: list[str] | None = None
    if shard:
        try:
            selected = case_shards.select(names, shard, case_shards.saved_costs(SUITE))
        except ValueError as error:
            parser.error(str(error))
        if not args.list_cases and directory is None:
            parser.error("--shard requires --shard-directory (or CHECKS_SHARD_DIRECTORY)")
    if args.list_cases:
        sys.stdout.write(json.dumps(selected if selected is not None else names) + "\n")
        return 0
    clear_git_environment()
    chosen = set(selected) if selected is not None else None
    cases = (
        CASES[:1]
        if args.hook_probe
        else [
            case
            for case in CASES
            if (chosen is None or case.name in chosen) and args.hook in (None, case.hook) and (not args.case or any(text in case.name for text in args.case))
        ]
    )
    if not PRE_COMMIT.exists() or not (ROOT / ".tools" / "bin" / "gitleaks").exists():
        sys.stderr.write("setup missing: create .venv from the host group of uv.lock and run scripts/quality/install_tools.py (README, Install)\n")
        return 1
    failures = 0
    rows: list[dict[str, Any]] = []
    for case in cases:
        started = time.monotonic()
        ok, output = run_case(case)
        rows.append({"case": case.name, "ok": ok, "seconds": round(time.monotonic() - started, 3)})
        expected = "pass" if case.expect_pass else "block"
        sys.stdout.write(f"{'ok' if ok else 'WRONG':<6} {case.hook:<19} {case.name} (expected {expected})\n")
        if not ok:
            failures += 1
            sys.stdout.write("".join(f"       {line}\n" for line in output.strip().splitlines()[-12:]))
    extra = 0
    runners: tuple[tuple[str, Callable[[], tuple[bool, str]]], ...] = ()
    if not args.hook_probe and args.hook is None:
        runners = tuple(runner for runner in RUNNERS if chosen is None or runner[0] in chosen)
    elif args.hook == "privacy-scan" and not args.case:
        runners = (PRIVACY_WORK_RUNNER,)
    for label, runner in runners:
        extra += 1
        started = time.monotonic()
        ok, output = runner()
        rows.append({"case": label, "ok": ok, "seconds": round(time.monotonic() - started, 3)})
        sys.stdout.write(f"{'ok' if ok else 'WRONG':<6} {label}\n")
        if not ok:
            failures += 1
            sys.stdout.write(output)
    sys.stdout.write(f"{len(cases) + extra - failures} of {len(cases) + extra} cases behaved as expected\n")
    if selected is not None and directory is not None:
        manifest = case_shards.write_manifest(directory, SUITE, shard or "", names, selected, rows)
        sys.stdout.write(f"shard {shard}: {manifest['status']}; manifest {directory / 'manifest.json'}\n")
        failures += int(manifest["status"] == "fail" and not failures)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
