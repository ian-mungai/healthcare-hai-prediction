"""E2E test of the pre-commit checks: each hook rejects a bad sample and passes a good one.

Run from the repository root after setup (``.venv`` and ``.tools`` present):
``.venv/bin/python -m scripts.quality.run_checks_e2e``. Each case builds a scratch Git repository with this repository's
hook configuration and scripts, links in ``.venv`` and ``.tools``, stages the sample and runs the hook through
``pre-commit`` itself, so a hook that is not wired in cannot pass. Samples that look like secrets, suppressions or
personal data are assembled at run time, so this file does not trip the checks it tests. The CI log is the run's artifact.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scripts.process import clear_git_environment, run_command
from scripts.quality import scratch_marker
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
ALLOWLIST_SOURCE = (ROOT / ALLOWLIST).read_text()
ENV_EXAMPLE_SOURCE = (ROOT / ".env.example").read_text()
VENDOR_ADDRESS = "noreply" + "@" + "anthropic.com"  # The attribution check matches this vendor address.
HOME_PATH = "/Us" + "ers/jdoe/projects/app/run.log"
TEMP_PATH = "/var/" + "folders/zq/k3j9x0000gn/T/run_1"
PERSONAL_EMAIL = "jane.doe" + "@" + "gmail.com"
PHONE = "(206) 555" + "-0100"
ACCOUNT = "1234" + "56789012"
AWS_ARN = f"arn:aws:iam::{ACCOUNT}:role/deploy"
ENV_VALUE = "acme" + "-admin-profile"
# The AWS CLI profile name of this project, a local label that .privacy_allowlist names for its one tracked file.
PROFILE_LABEL = "healthcare_hai" + "_prediction_dev"
PROFILE_FILE = "scripts/infrastructure/iam_preflight_policies.py"
PROJECT_NAME = "acme" + "-analytics"
# No privacy finding may print the value it found.
PRIVACY_VALUES = [HOME_PATH, TEMP_PATH, PERSONAL_EMAIL, PHONE, ACCOUNT, ENV_VALUE, PROFILE_LABEL]
WRITING_ALLOWLIST = ".writing_allowlist"
ISO_DATE = "2026" + "-09-30"
EM_DASH = "\u2014"
EN_DASH = "\u2013"
CLEAN_WRITING = (
    f"---\nlast_updated: {ISO_DATE}\n---\n\n# Notes\n\n"
    "Keep secrets out of logs or commits and run a privacy scan. Lists read red, green and blue.\n"
    f"Dates read Sep 30 2026; code keeps `{ISO_DATE}`, `a`, `b` and [the log](https://example.invalid/{ISO_DATE}).\n"
    f"An em dash {EM_DASH} allowed outside articles.\n\n```text\nrun {ISO_DATE}, and done\n```\n"
)
CLEAN_WRITING_HOOK = CLEAN_WRITING.replace(f"An em dash {EM_DASH} allowed outside articles.\n", "")
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
# Not pinned by any dependency group that the scratch repository copies.
OLD_PACKAGE = "hum" + "anize"
OLD_MODULE = OLD_PACKAGE
STALE_HELPER = "stale" + "_helper"
FOLDER_NAME = "run" + "books"
OLD_NOTES = "old" + "_notes.md"
# No file under scripts/ may pin this name: the scratch repository copies those files and would then count the sample import as declared.
UNUSED_PACKAGE = "quill" + "pad"
UNUSED_SETTING = "OLD" + "_SETTING"
UNUSED_TF_VARIABLE = "unused" + "_region"
DASH_M_MODULE = "nightly" + "_report"
USED_TF = f'variable "{UNUSED_TF_VARIABLE}" {{}}\n\nlocals {{\n  region = var.{UNUSED_TF_VARIABLE}\n}}\n\n' + 'output "region" {\n  value = local.region\n}\n'
# The scratch repository holds this repository's scripts; the cleanup samples sit outside them.
COPIED_ALLOWED = (
    "orphan scripts/* -- check scripts copied into the scratch repository\n"
    "orphan dbt/* -- dbt settings copied into the scratch repository\n"
    "unused-code scripts/* -- check scripts copied into the scratch repository\n"
    "unused-dependencies scripts/* -- check scripts copied into the scratch repository\n"
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
DOCS_ALLOWED = (
    COPIED_ALLOWED
    + "docs-path scripts/* -- check scripts copied into the scratch repository\n"
    + "docs-path config/quality/launcher_inputs.json -- the copied input list names the analysis plan, which the scratch repository lacks\n"
)
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
    committed: dict[str, str] = field(default_factory=dict)
    delete: list[str] = field(default_factory=list)
    message: str | None = None
    reason: str = ""  # A blocked case must print this text, so it fails for its intended cause.
    symlinks: dict[str, str] = field(default_factory=dict)  # Link path to link target, staged with the files.
    untracked: dict[str, str] = field(default_factory=dict)  # Written after staging, so Git does not track them.


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
        "padded HTML id and name are anchors",
        True,
        {
            "docs/guide.md": guide_page("See [a](setup.md#custom) and [b](setup.md#old-spot)."),
            SETUP: '# Setup\n\n<div id=" custom ">x</div>\n\n<a name="\told-spot\n"></a>\n',
        },
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


POLICY_DOCUMENT = "scripts/quality/repo_checks.py"
RUFF_STANDARD = '[tool.ruff]\nline-length = 160\ntarget-version = "py312"\n\n[tool.ruff.lint]\nselect = ["E", "F", "I", "B", "UP", "S", "SIM", "T20"]\n'

CASES = [
    Case("clean file", "gitleaks", True, {"notes.md": "Nothing secret here.\n"}),
    Case("GitHub token", "gitleaks", False, {"config.py": f'GITHUB = "{FAKE_PAT}"\n'}),
    Case(".env.example", "credential-files", True, {".env.example": "AWS_REGION=your-aws-region\n"}),
    Case(".env", "credential-files", False, {".env": "AWS_REGION=us-west-2\n"}),
    Case("Terraform state", "credential-files", False, {"infra/terraform.tfstate": "{}\n"}),
    Case("saved Terraform plan", "credential-files", False, {"infra/reviewed.tfplan": "placeholder\n"}),
    Case("private key file", "credential-files", False, {"keys/deploy.pem": "placeholder\n"}),
    Case("CSV in the declared folder", "data-files", True, {"tests/fixtures/result.csv": "a,b\n1,2\n"}),
    Case("CSV dbt seed in the declared seeds folder", "data-files", True, {"dbt/seeds/labels.csv": "a,b\n1,2\n"}),
    Case("CSV beside the dbt seeds folder", "data-files", False, {"dbt/models/labels.csv": "a,b\n1,2\n"}),
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
    Case("documented variable", "env-example", True, {"scripts/tool.py": "import os\n\nNAME = " + ENVIRON + '.get("AWS_REGION", "")\n'}),
    Case("undocumented variable", "env-example", False, {"scripts/tool.py": "import os\n\nNAME = " + ENVIRON + '["NEW_SETTING"]\n'}),
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
            "README.md": f"Use `{OLD_FLAG}`.\n",
        },
        files={"scripts/tool.py": "import argparse\n\nP = argparse.ArgumentParser()\n"},
    ),
    Case(
        "function removed with its callers",
        "removed-names",
        True,
        committed={"app/util.py": f"def {OLD_FUNCTION}():\n    return 1\n", "app/main.py": f"from util import {OLD_FUNCTION}\n\nX = {OLD_FUNCTION}()\n"},
        files={"app/util.py": "X = 1\n", "app/main.py": "X = 1\n"},
    ),
    Case(
        "function moved to another file",
        "removed-names",
        True,
        committed={"app/util.py": f"def {OLD_FUNCTION}():\n    return 1\n", "README.md": f"Call `{OLD_FUNCTION}`.\n"},
        files={"app/util.py": "X = 1\n", "app/other.py": f"def {OLD_FUNCTION}():\n    return 1\n"},
    ),
    Case(
        "removed function named only in history",
        "removed-names",
        True,
        committed={"app/util.py": f"def {OLD_FUNCTION}():\n    return 1\n", "CHANGELOG.md": f"- Removed `{OLD_FUNCTION}`.\n"},
        files={"app/util.py": "X = 1\n"},
    ),
    Case(
        "common word in prose after its function is removed",
        "removed-names",
        True,
        committed={"app/main.py": "def render():\n    return 0\n", "README.md": "Render the report.\n"},
        files={"app/main.py": "X = 1\n"},
    ),
    Case(
        "allowlisted reference",
        "removed-names",
        True,
        committed={"app/models.py": f"class {OLD_CLASS}:\n    pass\n", "docs/history.md": f"The `{OLD_CLASS}` era.\n"},
        files={"app/models.py": "X = 1\n", ".cleanup_allowlist": "python docs/history.md -- migration notes keep the old name\n"},
    ),
    Case(
        "function named like a folder is removed",
        "removed-names",
        True,
        committed={"app/util.py": f"def {FOLDER_NAME}():\n    return 1\n", "README.md": f"See `{FOLDER_NAME}/deploy.md`.\n"},
        files={"app/util.py": "X = 1\n"},
    ),
    Case(
        "function removed, caller remains",
        "removed-names",
        False,
        committed={"app/util.py": f"def {OLD_FUNCTION}():\n    return 1\n", "app/main.py": f"from util import {OLD_FUNCTION}\n\nX = {OLD_FUNCTION}()\n"},
        files={"app/util.py": "X = 1\n"},
    ),
    Case(
        "class removed, Markdown code names it",
        "removed-names",
        False,
        committed={"app/models.py": f"class {OLD_CLASS}:\n    pass\n", "README.md": f"Use `{OLD_CLASS}`.\n"},
        files={"app/models.py": "X = 1\n"},
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
    Case("documented variable nothing reads", "env-example", False, {".env.example": ENV_EXAMPLE_SOURCE + f"# Retired setting.\n{UNUSED_SETTING}=\n"}),
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
        {"README.md": "Read [the guide](docs/missing_guide.md).\n", ".cleanup_allowlist": DOCS_ALLOWED + "docs-path README.md -- quotes a deleted guide\n"},
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
    Case("AWS account ARN", "privacy-scan", False, {"infra/policy.json": f'{{"Resource": "{AWS_ARN}"}}\n'}),
    Case("AWS account ID field", "privacy-scan", False, {"tests/test_config.py": f'CONFIG = {{"account_id": "{ACCOUNT}"}}\n'}),
    Case("value declared in .env", "privacy-scan", False, {".env": f"AWS_PROFILE={ENV_VALUE}\n", "scripts/deploy.sh": f"aws s3 ls --profile {ENV_VALUE}\n"}),
    Case(
        "allowlisted profile label in its tracked file",
        "privacy-scan",
        True,
        {".env": f"AWS_PROFILE={PROFILE_LABEL}\n", PROFILE_FILE: f'OFFLINE_VALUES = {{"AWS_PROFILE": "{PROFILE_LABEL}"}}\n'},
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
        {"notes.md": f"Ask {PERSONAL_EMAIL}\n", ALLOWLIST: ALLOWLIST_SOURCE + "email notes.md -- synthetic contact used in a demo\n"},
    ),
    Case(
        "allowlist is by type",
        "privacy-scan",
        False,
        {"notes.md": f"Call {PHONE}\n", ALLOWLIST: ALLOWLIST_SOURCE + "email notes.md -- synthetic contact used in a demo\n"},
    ),
    Case(
        "allowlist is by path",
        "privacy-scan",
        False,
        {"docs/notes.md": f"Ask {PERSONAL_EMAIL}\n", ALLOWLIST: ALLOWLIST_SOURCE + "email notes.md -- synthetic contact used in a demo\n"},
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
    Case("warn-first kinds warn through the hook", "writing-check", True, {"notes.md": "Rules help; checks enforce them.\n"}),
    Case("internal references warn through the hook", "writing-check", True, {"notes.md": "The work belongs to wave 3.\n"}),
    Case(
        "valid front matter passes",
        "front-matter",
        True,
        {
            "docs/guide.md": "---\ntitle: Guide\ndescription: What the guide covers.\nlast_updated: 2026-10-02\n---\n\n# Guide\n",
            "README.md": "# Project\n",
            ".github/pull_request_template.md": "# Pull Request\n",
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
    "temp path": "notes.md:1: machine temporary path",
    "personal email": "notes.md:1: email address",
    "phone number": "notes.md:1: phone number",
    "AWS account ARN": "infra/policy.json:1: AWS account ID",
    "AWS account ID field": "tests/test_config.py:1: AWS account ID",
    "value declared in .env": "scripts/deploy.sh:1: value declared in .env",
    "profile label in another file": "scripts/deploy.sh:1: value declared in .env",
    "bucket named after the project": "scripts/deploy.sh:1: value declared in .env",
    "allowlist is by type": "notes.md:1: phone number",
    "allowlist is by path": "docs/notes.md:1: email address",
    "allowlist entry without reason": ".privacy_allowlist:{line}: allowlist entry without a reason",
    "allowlist entry with unknown type": ".privacy_allowlist:{line}: allowlist entry with an unknown type",
    "writing finding blocked by the hook": "notes.md:1: comma before a final 'and', 'or' or 'nor'",
    "time-bound word blocked by the hook": "notes.md:1: time-bound word in prose",
    "warn-first kinds warn through the hook": "WARN notes.md:1: semicolon in prose",
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
# Good cases that must also print a marker, so a skipped check cannot pass silently.
PASS_MARKERS = {
    "unreadable file listed": "docs/diagram.png: unreviewed: cannot be read as text",
    "warn-first kinds warn through the hook": "WARN notes.md:1: semicolon in prose",
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
    # Copy only files Git tracks or would track, so ignored local code never enters the scratch repository. The launcher reads a
    # variable .env.example documents (DOCKER_HOST), so the env-example cases need it too.
    listed = run_command(
        "git", ["ls-files", "--cached", "--others", "--exclude-standard", "scripts", "orchestration/launcher.py"], cwd=ROOT, check=True
    ).stdout.splitlines()
    if case.hook == "env-example":
        listed.append("infra/jobs/identity_probe/probe.py")
    for relative in [*COPIED, *listed]:
        if (ROOT / relative).is_file():
            (repo / relative).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(ROOT / relative, repo / relative)
    for tool in (".venv", ".tools"):
        (repo / tool).symlink_to(ROOT / tool)
    (repo / ".git" / "info" / "exclude").write_text(".venv\n.tools\n__pycache__/\n")
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
        verbose = case.name in PASS_MARKERS
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
        if not case.expect_pass and case.hook in ("writing-check", "privacy-scan"):
            correct = correct and POLICY_DOCUMENT in output
        if case.hook in ("writing-check", "markdownlint", "front-matter", "markdown-links", "sqlfluff", "docs-references", "temporary-code", *CLEANUP_HOOKS):
            correct = correct and (case.reason or BLOCK_REASONS.get(case.name, "")) in output
        if case.hook == "lint-settings" and not case.expect_pass and case.name.startswith("MyPy"):
            reason = "configuration must name a committed profile" if case.name == "MyPy generated profile" else "MyPy"
            correct = correct and reason in output
        return correct, output


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


def workflow_range_case(repo: Path, label: str, event: str, base: str, head: str, want_start: str, want_exit: int, want_marker: str = "") -> dict[str, object]:
    workflow = load_yaml((repo / ".github/workflows/ci.yml").read_text())
    if not isinstance(workflow, dict):
        raise ValueError(".github/workflows/ci.yml is not a mapping")
    context = {
        "github.event_name": event,
        "github.event.pull_request.base.sha": base if event == "pull_request" else "",
        "github.event.pull_request.head.sha": head if event == "pull_request" else "",
        "github.event.before": base if event == "push" else "",
        "github.sha": head,
    }
    output = repo / ".git" / f"{label}_output"
    environment = {**os.environ, "GITHUB_OUTPUT": str(output), "RUNNER_TEMP": str(repo / ".git")}
    steps = workflow["jobs"]["plan"]["steps"]
    resolution = next((step for step in steps if step.get("id") == "range"), None)
    plan_exit = 0
    log = ""
    if resolution:
        env = {name: workflow_value(value, context) for name, value in resolution.get("env", {}).items()}
        script = repo / ".git" / "range_plan.sh"
        script.write_text("set -eo pipefail\n" + resolution["run"])
        result = run_command("bash", [str(script)], cwd=repo, env={**environment, **env}, timeout=120)
        plan_exit, log = result.returncode, result.stdout + result.stderr
        if output.exists():
            context.update({f"steps.range.outputs.{key}": value for line in output.read_text().splitlines() for key, value in [line.split("=", 1)]})
    if plan_exit != 0:
        return {"case": label, "passed": False, "checks": {"plan_passes": False}, "plan_exit": plan_exit, "log": log}
    for name, value in workflow["jobs"]["plan"].get("outputs", {}).items():
        if "steps.range.outputs." in value:
            context[f"needs.plan.outputs.{name}"] = workflow_value(value, context)
    # The scope step and the check range step of the plan job run for real: the scope is range for a pull request and full for every other event, and the
    # file based checks get an empty range outside a pull request.
    scope_exits = {}
    for step_id in ("scope", "checkrange"):
        plan_step = next((step for step in steps if step.get("id") == step_id), None)
        scope_exits[step_id] = 1
        if plan_step:
            step_env = {name: workflow_value(value, context) for name, value in plan_step.get("env", {}).items()}
            step_output = repo / ".git" / f"{label}_{step_id}_output"
            script = repo / ".git" / "plan_step.sh"
            script.write_text("set -eo pipefail\n" + plan_step["run"])
            result = run_command("bash", [str(script)], cwd=repo, env={**environment, "GITHUB_OUTPUT": str(step_output), **step_env}, timeout=120)
            scope_exits[step_id] = result.returncode
            if step_output.exists():
                context.update(
                    {f"steps.{step_id}.outputs.{key}": value for line in step_output.read_text().splitlines() for key, value in [line.split("=", 1)]}
                )
    for name, value in workflow["jobs"]["plan"].get("outputs", {}).items():
        if "steps.scope.outputs." in value or "steps.checkrange.outputs." in value:
            context[f"needs.plan.outputs.{name}"] = workflow_value(value, context)
    file_based = {
        f"{job}.{index}"
        for job, definition in workflow["jobs"].items()
        if job != "plan"
        for index, step in enumerate(definition.get("steps", []))
        if "check_start" in str(step.get("env", {}).get("RANGE_START", ""))
    }
    scopes = {
        f"{job}.{index}": workflow_value(str(step["env"]["CI_SCOPE"]), context)
        for job, definition in workflow["jobs"].items()
        if job != "plan"
        for index, step in enumerate(definition.get("steps", []))
        if "CI_SCOPE" in step.get("env", {})
    }
    wanted_scope = "range" if event == "pull_request" else "full"
    consumers = {
        f"{job}.{index}": {name: workflow_value(step["env"][name], context) for name in ("RANGE_START", "RANGE_END")}
        for job, definition in workflow["jobs"].items()
        if job != "plan"
        for index, step in enumerate(definition.get("steps", []))
        if "RANGE_START" in step.get("env", {})
    }
    message_step = next(step for step in workflow["jobs"]["hooks"]["steps"] if step["name"] == "Check commit messages")
    check_env = {name: workflow_value(value, context) for name, value in message_step["env"].items()}
    check_exit = 0
    if event != "workflow_dispatch":
        script = repo / ".git" / "range_check.sh"
        script.write_text("set -eo pipefail\n" + message_step["run"])
        result = run_command("bash", [str(script)], cwd=repo, env={**environment, **check_env}, timeout=120)
        check_exit = result.returncode
        log += result.stdout + result.stderr
    result = run_command(
        str(ROOT / ".venv/bin/python"),
        ["-m", "scripts.quality.ci_groups", "plan", "--scope", "range", "--part", "core", "--from", check_env["RANGE_START"], "--to", head],
        cwd=repo,
        timeout=120,
    )
    selected = json.loads(result.stdout) if result.returncode == 0 else {}
    checks = {
        "plan_passes": plan_exit == 0,
        "scope_and_check_range_steps_pass": all(code == 0 for code in scope_exits.values()),
        "commit_checks_use_the_real_range": bool(consumers)
        and all(env == {"RANGE_START": want_start, "RANGE_END": head} for name, env in consumers.items() if name not in file_based),
        # The hook partitions and the acquisition tests read the files of the range in a pull request and every file in a full run.
        "file_based_checks_use_the_check_range": len(file_based) == 3
        and all(
            consumers[name] == ({"RANGE_START": want_start, "RANGE_END": head} if event == "pull_request" else {"RANGE_START": "", "RANGE_END": ""})
            for name in file_based
        ),
        # Pull requests are path selected; a push to main and a manual run are full runs, and no run is scheduled.
        "every_group_job_runs_the_scope_of_the_event": len(scopes) >= 8 and all(value == wanted_scope for value in scopes.values()),
        "no_scheduled_trigger": "schedule" not in (workflow.get(True) or workflow.get("on") or {}),
        "check_has_the_expected_result": check_exit == want_exit and want_marker in log,
        "selector_passes": result.returncode == 0,
    }
    if label == "branch_behind_main":
        checks["main_only_paths_do_not_select_groups"] = selected.get("changed") == 1 and selected.get("run") == [
            "repository_code_checks",
            "ci_tooling_checks",
            "sql_style",
        ]
        checkout = next(step for step in steps if step.get("uses", "").startswith("actions/checkout@"))
        checks["plan_fetches_full_history"] = checkout.get("with", {}).get("fetch-depth") == 0
    if not want_start or want_start == "missing-ref":
        checks["unusable_range_selects_all_groups"] = selected.get("changed") is None and selected.get("run") == selected.get("groups")
    return {
        "case": label,
        "passed": all(checks.values()),
        "checks": checks,
        "check_exit": check_exit,
        "consumers": consumers,
        "selection": selected,
        "log": log,
    }


def run_workflow_range_case() -> tuple[bool, str]:
    """Run the actual workflow shell and selector on divergent branches, pushes and missing history."""
    with tempfile.TemporaryDirectory(prefix="hai_workflow_range_") as scratch:
        function = "upstream" + "_added_value"
        removed = "branch" + "_removed_value"
        api = "scripts/acquisition/range_sample.py"
        original = f"def {removed}():\n    return 1\n"
        repo = scratch_repo(Path(scratch), Case("workflow range setup", "commit-msg", True, committed={api: original, "caller.py": f"{removed}()\n"}))
        ancestor = git(repo, "rev-parse", "HEAD").strip()
        git(repo, "checkout", "-qb", "topic")
        write(repo, {"README.md": f"# Example\n\n`{function}()` is named in this branch.\n"})
        git(repo, "add", "--", "README.md")
        git(repo, "commit", "-qm", "docs: describe the branch")
        head = git(repo, "rev-parse", "HEAD").strip()
        git(repo, "checkout", "-q", "main")
        write(repo, {api: original + f"\ndef {function}():\n    return 2\n"})
        git(repo, "add", "--", api)
        git(repo, "commit", "-qm", "feat: add a name on main")
        base = git(repo, "rev-parse", "HEAD").strip()
        git(repo, "checkout", "-q", "topic")
        results = [
            workflow_range_case(repo, "branch_behind_main", "pull_request", base, head, ancestor, 0),
            workflow_range_case(repo, "push_keeps_divergent_before", "push", base, head, base, 1, f"{function} was removed"),
            workflow_range_case(repo, "manual_run_keeps_full_selection", "workflow_dispatch", "", head, "", 0),
            workflow_range_case(repo, "missing_start_keeps_history_fallback", "pull_request", "missing-ref", head, "missing-ref", 0),
        ]
        write(repo, {api: "VALUE = 1\n"})
        git(repo, "add", "--", api)
        git(repo, "commit", "-qm", "refactor: remove a branch function")
        deleted = git(repo, "rev-parse", "HEAD").strip()
        results.append(workflow_range_case(repo, "real_branch_removal_is_rejected", "pull_request", base, deleted, ancestor, 1, f"{removed} was removed"))
        reset_hard(repo, head)
        git(repo, "commit", "--allow-empty", "-qm", "Invalid subject")
        invalid = git(repo, "rev-parse", "HEAD").strip()
        results.append(workflow_range_case(repo, "branch_commit_message_is_rejected", "pull_request", base, invalid, ancestor, 1, "not a Conventional Commit"))
        git(repo, "checkout", "-q", "--detach", ancestor)
        git(repo, "commit", "--amend", "-qm", "Invalid root subject")
        root = git(repo, "rev-parse", "HEAD").strip()
        results.append(
            workflow_range_case(repo, "missing_start_checks_the_root_message", "push", "missing-ref", root, "missing-ref", 1, "not a Conventional Commit")
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
        {"CHANGELOG.md": f"## v1.0.0 ({ISO_DATE})\n", WRITING_ALLOWLIST: "iso-date CHANGELOG.md -- release headings are machine-readable\n"},
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


DASH_FINDING = "em or en dash in prose"
SEMICOLON_FINDING = "semicolon in prose"
SLASH_FINDING = "slash between words in prose"
DASH_LINE = f"Rules help {EM_DASH} when they are checked.\n"
EN_DASH_LINE = f"Rules help {EN_DASH} when they are checked.\n"
SEMICOLON_LINE = "Rules help; checks enforce them.\n"
SLASH_LINE = "Read and/or write the file.\n"
ALL_KINDS = DASH_LINE + SEMICOLON_LINE + SLASH_LINE
PUNCTUATION_PROSE = (
    "Rules help when they are checked. Read `a/b;c`, see [the log](https://example.invalid/a;b-c) &amp; write it.\n"
    "Open scripts/quality/timing.sh or scripts/run_ci.sh, then ./a/b, ~/a/b and a/b/c. Hyphens stay in well-known words.\n"
)
CLEAN_PUNCTUATION = f"---\ntitle: Notes\nnote: a {EM_DASH} b; c/d\n---\n\n# Notes\n\n{PUNCTUATION_PROSE}\n```text\n{ALL_KINDS}```\n"
BLOCK_ALL = ("--block", "dash", "--block", "semicolon", "--block", "slash")
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
    "Owner decisions describe scope. Approval is required. Amendments describe a rule. The wave breaks. Review rounds have names.\n"
    "The approved code hashes of 9 collectors. Approval is required for 2 users. Approved: 5 of 10 rows.\n"
    "Approved 5 rows were loaded. Approval 2 weeks later. User approval takes 2 steps. Encoded text +P7d/ and =P3c stays.\n"
    "Owner decisions in October are logged. User approval for re-entry stays.\n"
    "User approved utf-8 output. Owner approved sha-256 checksums. User approved top-10 results.\n"
    "User approval retains 3 decimal places. Owner approval needs 2 marked rows. User approval uses 4 augmented rows and 2 decoders.\n"
    "Approved Decimal 3 2026 values stay. Owner approval may 2 rows.\n"
    "Observed on Oct 25 2026. Versions v9.10.0 and 1.2.3 stay. RQ4, P3.12, P7docs and WPA stay.\n"
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
        [f"WARN notes.md:1: {DASH_FINDING}", f"WARN notes.md:2: {SEMICOLON_FINDING}", f"WARN notes.md:3: {SLASH_FINDING}"],
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
        [f"notes.md:1: {DASH_FINDING}", f"notes.md:2: {SEMICOLON_FINDING}", f"notes.md:3: {SLASH_FINDING}"],
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
        {"notes.md": SEMICOLON_LINE + DASH_LINE, WRITING_ALLOWLIST: "semicolon notes.md -- a quoted sentence keeps its semicolon\n"},
        (),
        0,
        [f"WARN notes.md:2: {DASH_FINDING}"],
    ),
    (
        "allowlist entry silences a blocked kind",
        {"notes.md": SEMICOLON_LINE, WRITING_ALLOWLIST: "semicolon notes.md -- a quoted sentence keeps its semicolon\n"},
        ("--block", "semicolon"),
        0,
        [],
    ),
    (
        "allowlist entry is by path",
        {"notes.md": SLASH_LINE, WRITING_ALLOWLIST: "slash docs/*.md -- another folder\n"},
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
            WRITING_ALLOWLIST: "internal-reference .gitignore -- local planning records must stay untracked\n",
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
        {"notes.txt": "wave 3\n", WRITING_ALLOWLIST: "internal-reference *.txt -- synthetic example\n"},
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
        {"notes.txt": "wave 3\n", WRITING_ALLOWLIST: "semicolon *.txt -- another kind\n"},
        (),
        0,
        ["WARN notes.txt:1: internal reference (wave)"],
    ),
    (
        "internal reference allowlist checks the path",
        {"notes.txt": "wave 3\n", WRITING_ALLOWLIST: "internal-reference docs/* -- another folder\n"},
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


def main() -> int:
    """Run every case and report the ones whose hook did not behave as expected."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--hook-probe", action="store_true", help="run only the clean sample, for the commit-hook E2E")
    parser.add_argument("--hook", help="iteration run: only the cases of this hook, with no extra runners (not commit evidence)")
    parser.add_argument("--case", action="append", default=[], help="with --hook: only the cases whose name contains this text (repeatable)")
    args = parser.parse_args()
    clear_git_environment()
    cases = (
        CASES[:1]
        if args.hook_probe
        else [case for case in CASES if args.hook in (None, case.hook) and (not args.case or any(text in case.name for text in args.case))]
    )
    if not PRE_COMMIT.exists() or not (ROOT / ".tools" / "bin" / "gitleaks").exists():
        sys.stderr.write("setup missing: create .venv from the host group of uv.lock and run scripts/quality/install_tools.py (README, Install)\n")
        return 1
    failures = 0
    for case in cases:
        ok, output = run_case(case)
        expected = "pass" if case.expect_pass else "block"
        sys.stdout.write(f"{'ok' if ok else 'WRONG':<6} {case.hook:<19} {case.name} (expected {expected})\n")
        if not ok:
            failures += 1
            sys.stdout.write("".join(f"       {line}\n" for line in output.strip().splitlines()[-12:]))
    extra = 0
    if not args.hook_probe and args.hook is None:
        for label, runner in (
            ("commit-range", run_range_case),
            ("workflow ranges", run_workflow_range_case),
            ("verbose commit", run_verbose_case),
            ("privacy scope", run_privacy_scope_case),
            ("writing samples", run_writing_case),
            ("writing kinds", run_writing_kinds_case),
            ("typecheck import closure", run_typecheck_import_case),
            ("typecheck pending manifest", run_typecheck_manifest_case),
            ("typecheck cold caches", run_typecheck_cold_case),
            ("source lint coverage", run_lint_coverage_case),
            ("head and pinned-tools checks", run_worktree_checks_case),
        ):
            extra += 1
            ok, output = runner()
            sys.stdout.write(f"{'ok' if ok else 'WRONG':<6} {label}\n")
            if not ok:
                failures += 1
                sys.stdout.write(output)
    sys.stdout.write(f"{len(cases) + extra - failures} of {len(cases) + extra} cases behaved as expected\n")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
