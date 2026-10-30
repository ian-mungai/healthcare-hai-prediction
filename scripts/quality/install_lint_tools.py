"""Install the hash-pinned Python lint tools (import-linter, check-jsonschema) for their pre-commit hooks.

Run from the repository root:

    .venv/bin/python -m scripts.quality.install_lint_tools [--tool import-linter] [--tool check-jsonschema]

``config/quality_tools.json`` names each tool, its version and its requirements file; the requirements pin every package with hashes.
Each tool gets its own virtual environment in ``.tools/<tool>``, so neither can disturb ``.venv``. pip verifies every hash and installs
wheels only. A rerun with the same requirements changes nothing; a failed install removes the copied requirements so the next run retries.
"""

from __future__ import annotations

import argparse
import filecmp
import json
import shutil
import sys
from pathlib import Path

from scripts.process import run_command
from scripts.quality.install_sqlfluff import base_python

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "config/quality_tools.json"
TOOLS = ("import-linter", "check-jsonschema")


def pinned(name: str) -> dict[str, str]:
    """Return the manifest entry of a Python lint tool."""
    entry = json.loads(MANIFEST.read_text(encoding="utf-8"))["tools"][name]
    if entry.get("installer") != "scripts/quality/install_lint_tools.py":
        raise ValueError(f"config/quality_tools.json:1: {name} is not installed by this script")
    return entry


def current(name: str, entry: dict[str, str]) -> bool:
    """Return whether the installed environment matches the committed requirements and runs the pinned version."""
    destination = ROOT / ".tools" / name
    executable, source, installed = ROOT / entry["executable"], ROOT / entry["requirements"], destination / "requirements.txt"
    if not (executable.exists() and installed.exists() and filecmp.cmp(source, installed, shallow=False)):
        return False
    result = run_command(str(executable), ["--version"], timeout=60)
    return result.returncode == 0 and result.stdout.strip().endswith(entry["version"])


def install(name: str) -> str:
    """Create the environment and install the hash-pinned requirements; return what happened."""
    entry = pinned(name)
    if current(name, entry):
        return f"unchanged  {name} {entry['version']}"
    destination, source = ROOT / ".tools" / name, ROOT / entry["requirements"]
    (destination / "requirements.txt").unlink(missing_ok=True)
    if destination.exists():
        shutil.rmtree(destination)
    run_command(base_python(), ["-m", "venv", str(destination)], timeout=300, check=True)
    pip = [str(destination / "bin/python"), "-m", "pip", "install", "--quiet", "--require-hashes", "--only-binary=:all:", "-r", str(source)]
    result = run_command(pip[0], pip[1:], timeout=900)
    if result.returncode:
        raise SystemExit(f"pip install failed for {name} {entry['version']}; nothing usable was installed:\n{result.stderr[-2000:]}")
    shutil.copyfile(source, destination / "requirements.txt")
    return f"installed  {name} {entry['version']} (hashes verified) in {destination.relative_to(ROOT)}"


def main() -> int:
    """Install the selected Python lint tools (all by default) and report the result."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tool", action="append", choices=TOOLS, help="install only this tool (repeat to select more)")
    parser.add_argument("--check", action="store_true", help="install nothing; exit 1 when a selected tool is missing or differs from its pin")
    args = parser.parse_args()
    stale = False
    for name in args.tool or TOOLS:
        if args.check:
            entry = pinned(name)
            ok = current(name, entry)
            stale |= not ok
            sys.stdout.write(f"{'current  ' if ok else 'not current'}  {name} {entry['version']}\n")
        else:
            sys.stdout.write(install(name) + "\n")
    return int(stale)


if __name__ == "__main__":
    raise SystemExit(main())
