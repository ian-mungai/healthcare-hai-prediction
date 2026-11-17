"""Every command the README shows is one Bash script invocation of a committed script, with no placeholder to fill by hand.

The README shows commands only as Bash script invocations. Each line of a fenced ``sh`` block in ``README.md`` is
``bash scripts/<name>.sh`` with plain arguments, and that script exists. The README must show the install, the full local checks and the single
end-to-end check. The wrappers are tested in ``tests/test_script_entry_points.py`` and the web interface commands in ``tests/test_readme_web_interfaces.py``.
"""

from __future__ import annotations

import re
from pathlib import Path

from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
BLOCK = re.compile(r"^(\s*)```sh\n(.*?)^\1```$", re.DOTALL | re.MULTILINE)
COMMAND = re.compile(r"^bash (scripts/[\w./-]+\.sh)((?: [\w./=-]+)*)$")
PLACEHOLDER = re.compile(r"<[A-Za-z_]+>|\$\{?[A-Za-z_]")
REQUIRED = (
    "bash scripts/install.sh",
    "bash scripts/run_ci.sh",
    "bash scripts/run_checks_e2e.sh",
    "bash scripts/host.sh start",
    "bash scripts/open_ui.sh --list",
)


def commands(readme: str) -> list[str]:
    """Every non-blank line of every fenced ``sh`` block, trimmed."""
    return [line.strip() for _, body in BLOCK.findall(readme) for line in body.splitlines() if line.strip()]


def problems(readme: str) -> list[str]:
    found: list[str] = []
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
    }
    for name, (text, expected) in cases.items():
        found = problems(text)
        check(any(expected in message for message in found), f"{name} was not reported as {expected!r}: {found}")
