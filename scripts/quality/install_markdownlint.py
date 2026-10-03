"""Install the pinned markdownlint-cli2 for the markdownlint pre-commit hook.

Run from the repository root:

    .venv/bin/python -m scripts.quality.install_markdownlint

The version is pinned by ``scripts/quality/markdownlint/package-lock.json``. ``npm ci`` verifies every package's
integrity hash and runs no install scripts (``--ignore-scripts``). It installs into ``.tools/markdownlint-cli2`` and
needs Node.js 22 or later. A rerun with the same lockfile changes nothing; a failed install removes the copied
manifest so the next run retries instead of looking current.
"""

from __future__ import annotations

import filecmp
import json
import shutil
import sys
from pathlib import Path

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "scripts/quality/markdownlint"
DESTINATION = ROOT / ".tools/markdownlint-cli2"
FILES = ("package.json", "package-lock.json")
NODE_MAJOR_MINIMUM = 22


def pinned_version() -> str:
    """Return the markdownlint-cli2 version the committed manifest pins."""
    return str(json.loads((SOURCE / "package.json").read_text())["dependencies"]["markdownlint-cli2"])


def current() -> bool:
    """Return whether the installed copy matches the committed manifest, lockfile and pinned version."""
    installed = DESTINATION / "node_modules/markdownlint-cli2/package.json"
    if not installed.exists():
        return False
    if not all((DESTINATION / name).exists() and filecmp.cmp(SOURCE / name, DESTINATION / name, shallow=False) for name in FILES):
        return False
    return bool(json.loads(installed.read_text())["version"] == pinned_version())


def install() -> str:
    """Install from the committed lockfile with npm ci and return what happened."""
    pinned = pinned_version()
    if current():
        return f"unchanged  markdownlint-cli2 {pinned}"
    node = run_command("node", ["--version"], timeout=30, check=True).stdout.strip()
    if int(node.lstrip("v").split(".")[0]) < NODE_MAJOR_MINIMUM:
        raise SystemExit(f"markdownlint-cli2 needs Node.js {NODE_MAJOR_MINIMUM} or later; found {node}; nothing was installed")
    DESTINATION.mkdir(parents=True, exist_ok=True)
    for name in FILES:
        shutil.copyfile(SOURCE / name, DESTINATION / name)
    result = run_command("npm", ["ci", "--ignore-scripts", "--no-audit", "--no-fund"], cwd=DESTINATION, timeout=600)
    if result.returncode:
        for name in FILES:  # A failed install must not look current on the next run.
            (DESTINATION / name).unlink(missing_ok=True)
        raise SystemExit(f"npm ci failed for markdownlint-cli2 {pinned}; nothing usable was installed:\n{result.stderr[-2000:]}")
    return f"installed  markdownlint-cli2 {pinned} (npm ci, lockfile integrity verified, Node.js {node})"


def main() -> int:
    """Install markdownlint-cli2 and report the result."""
    sys.stdout.write(install() + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
