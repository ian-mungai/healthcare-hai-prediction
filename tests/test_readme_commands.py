"""Every command the README shows is one Bash script invocation of a committed script, with no placeholder to fill by hand.

The README shows commands only as Bash script invocations. Each line of every fenced code block in ``README.md`` is ``bash scripts/<name>.sh`` with plain
arguments, and that script exists, whatever language the fence names (``sh``, ``bash``, ``shell``, ``console``, none or any other). Only a fence marked as plain
text (``text``) may hold other lines. The README must show the install, the full local checks and the single end-to-end check. The wrappers are tested in
``tests/test_script_entry_points.py`` and the web interface commands in ``tests/test_readme_web_interfaces.py``.
"""

from __future__ import annotations

import re
from pathlib import Path

from markdown_it import MarkdownIt

from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
PLAIN_TEXT = frozenset({"text", "txt", "plain", "plaintext"})
COMMAND = re.compile(r"^bash (scripts/[\w./-]+\.sh)((?: [\w./=-]+)*)$")
PLACEHOLDER = re.compile(r"<[A-Za-z_]+>|\$\{?[A-Za-z_]")
REQUIRED = (
    "bash scripts/install.sh",
    "bash scripts/run_ci.sh",
    "bash scripts/run_checks_e2e.sh",
    "bash scripts/host.sh start",
    "bash scripts/open_ui.sh --list",
)


def fenced_blocks(readme: str) -> tuple[list[tuple[str, list[str]]], list[str]]:
    """Every fenced code block as (language, lines), and a message for a fence never closed.

    The blocks are the ``fence`` tokens of markdown-it (the parser the repository's link check uses), so a fence inside a list item or a blockquote, in any
    nesting, is found as GitHub finds it.
    """
    lines = readme.splitlines()
    blocks: list[tuple[str, list[str]]] = []
    unclosed: list[str] = []
    for token in MarkdownIt("commonmark").parse(readme):
        if token.type != "fence" or token.map is None:
            continue
        blocks.append((token.info.strip().split(" ")[0].lower(), token.content.splitlines()))
        last = lines[token.map[1] - 1].lstrip(" >\t") if token.map[1] - 1 > token.map[0] else ""
        if not re.fullmatch(re.escape(token.markup[0]) + "{" + str(len(token.markup)) + r",}\s*", last):
            unclosed.append(f"the code fence opened at line {token.map[0] + 1} is never closed")
    return blocks, unclosed


def commands(readme: str) -> list[str]:
    """Every non-blank line of every fenced block that is not plain text, trimmed."""
    return [line.strip() for language, body in fenced_blocks(readme)[0] if language not in PLAIN_TEXT for line in body if line.strip()]


def problems(readme: str) -> list[str]:
    found: list[str] = list(fenced_blocks(readme)[1])
    shown = commands(readme)
    for line in shown:
        match = COMMAND.fullmatch(line)
        if match is None:
            found.append(f"the command {line!r} must be one 'bash scripts/<name>.sh' invocation with plain arguments")
            continue
        if not (ROOT / match.group(1)).is_file():
            found.append(f"the command {line!r} names {match.group(1)}, which does not exist")
        if PLACEHOLDER.search(line):
            found.append(f"the command {line!r} holds a placeholder or a variable")
    found += [f"the README must show {line!r}" for line in REQUIRED if line not in shown]
    if re.search(r"^\s*(\.venv/bin/python|python3?|aws|terraform|cp|env) ", readme, re.MULTILINE):
        found.append("a README line starts with a program other than bash")
    if PLACEHOLDER.search("\n".join(shown)):
        found.append("a command block holds a placeholder")
    return found


def test_every_readme_command_is_a_bash_script_that_exists() -> None:
    differences = problems((ROOT / "README.md").read_text())
    check(not differences, "README.md differs from the rule:\n" + "\n".join(differences))


def test_a_python_command_a_missing_script_a_placeholder_and_a_missing_command_are_caught() -> None:
    readme = (ROOT / "README.md").read_text()
    cases = {
        "a python module command": (readme.replace("bash scripts/run_checks_e2e.sh", ".venv/bin/python -m scripts.quality.run_checks_e2e", 1), "must be one"),
        "a missing script": (readme.replace("bash scripts/run_checks_e2e.sh", "bash scripts/no_such_script.sh", 1), "does not exist"),
        "a placeholder argument": (readme.replace("bash scripts/host.sh start", "bash scripts/host.sh <ACTION>", 1), "must be one"),
        "an environment assignment": (
            readme.replace("bash scripts/run_ci.sh", 'env PYTHON_BIN="$PWD/.venv/bin/python" bash scripts/run_ci.sh', 1),
            "must be one",
        ),
        "a missing install command": (readme.replace("bash scripts/install.sh", "", 1), "must show"),
        "a missing check command": (readme.replace("bash scripts/run_ci.sh", "", 1), "must show"),
        "an unclosed fence": (readme + "\n```sh\nbash scripts/install.sh\n", "never closed"),
    }
    for name, (text, expected) in cases.items():
        found = problems(text)
        check(any(expected in message for message in found), f"{name} was not reported as {expected!r}: {found}")


def test_every_fence_language_is_checked_and_only_plain_text_is_exempt() -> None:
    readme = (ROOT / "README.md").read_text()
    for language in ("sh", "bash", "shell", "zsh", "console", "SH", "python", "json", ""):
        for marker in ("```", "~~~", "````"):
            found = problems(readme + f"\n{marker}{language}\necho nonconforming\n{marker}\n")
            check(any("must be one" in message for message in found), f"a {marker}{language} fence with a bad command was not reported: {found}")
    indented = problems(readme + "\n   ```bash\n   echo nonconforming\n   ```\n")
    check(any("must be one" in message for message in indented), f"an indented bash fence with a bad command was not reported: {indented}")
    plain = problems(readme + "\n```text\necho only an example of output\n```\n")
    check(plain == [], f"a plain text fence must not be checked: {plain}")
    good = problems(readme + "\n```bash\nbash scripts/install.sh\n```\n")
    check(good == [], f"a conforming command in a bash fence must pass: {good}")


def test_a_fence_inside_a_blockquote_or_a_list_is_checked() -> None:
    readme = (ROOT / "README.md").read_text()
    samples = {
        "a blockquoted bash fence": "> ```bash\n> echo nonconforming\n> ```\n",
        "a blockquoted fence without a language": "> ```\n> echo nonconforming\n> ```\n",
        "a blockquoted tilde fence": "> ~~~sh\n> echo nonconforming\n> ~~~\n",
        "a nested blockquote": "> > ```shell\n> > echo nonconforming\n> > ```\n",
        "a blockquote without a space after the marker": ">```bash\n>echo nonconforming\n>```\n",
        "a fence opened on the list item line": "- ```bash\n  echo nonconforming\n  ```\n",
        "a numbered list fence": "1. Step:\n\n   ```console\n   echo nonconforming\n   ```\n",
        "a blockquote inside a list": "- item\n\n  > ```zsh\n  > echo nonconforming\n  > ```\n",
    }
    for name, sample in samples.items():
        found = problems(readme + "\n" + sample)
        check(any("must be one" in message for message in found), f"{name} was not reported: {found}")
    good = problems(readme + "\n> ```bash\n> bash scripts/install.sh\n> ```\n")
    check(good == [], f"a conforming command in a blockquoted fence must pass: {good}")
    plain = problems(readme + "\n> ```text\n> echo only an example of output\n> ```\n")
    check(plain == [], f"a blockquoted plain text fence must not be checked: {plain}")
    unclosed = problems(readme + "\n> ```bash\n> bash scripts/install.sh\n")
    check(any("never closed" in message for message in unclosed), f"an unclosed blockquoted fence was not reported: {unclosed}")
