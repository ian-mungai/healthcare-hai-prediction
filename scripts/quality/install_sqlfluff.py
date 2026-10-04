"""Install the pinned SQLFluff and dbt templater for the sqlfluff pre-commit hook and local CI.

Run from the repository root:

    .venv/bin/python -m scripts.quality.install_sqlfluff

The versions are pinned with hashes by ``scripts/quality/sqlfluff/requirements.txt``. They go into their own virtual
environment, ``.tools/sqlfluff``, because dbt-core caps pathspec below the version MyPy needs in ``.venv``. pip
verifies every hash and installs wheels only. A rerun with the same requirements changes nothing; a failed install
removes the copied requirements so the next run retries instead of looking current.
"""

from __future__ import annotations

import filecmp
import shutil
import sys
from pathlib import Path

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "scripts/quality/sqlfluff/requirements.txt"
DESTINATION = ROOT / ".tools/sqlfluff"
INSTALLED = DESTINATION / "requirements.txt"
SQLFLUFF = DESTINATION / "bin/sqlfluff"


def pinned_version() -> str:
    """Return the SQLFluff version the committed requirements pin."""
    return next(line.split("==", 1)[1].split()[0] for line in SOURCE.read_text().splitlines() if line.startswith("sqlfluff=="))


def current() -> bool:
    """Return whether the installed environment matches the committed requirements and runs the pinned version."""
    if not (SQLFLUFF.exists() and INSTALLED.exists() and filecmp.cmp(SOURCE, INSTALLED, shallow=False)):
        return False
    result = run_command(str(SQLFLUFF), ["--version"], timeout=60)
    return result.returncode == 0 and result.stdout.strip().endswith(pinned_version())


def install() -> str:
    """Create the environment and install the hash-pinned requirements; return what happened."""
    pinned = pinned_version()
    if current():
        return f"unchanged  sqlfluff {pinned}"
    INSTALLED.unlink(missing_ok=True)
    if DESTINATION.exists():
        shutil.rmtree(DESTINATION)
    run_command(sys.executable, ["-m", "venv", str(DESTINATION)], timeout=300, check=True)
    pip = [str(DESTINATION / "bin/python"), "-m", "pip", "install", "--quiet", "--require-hashes", "--only-binary=:all:", "-r", str(SOURCE)]
    result = run_command(pip[0], pip[1:], timeout=900)
    if result.returncode:
        raise SystemExit(f"pip install failed for sqlfluff {pinned}; nothing usable was installed:\n{result.stderr[-2000:]}")
    shutil.copyfile(SOURCE, INSTALLED)
    return f"installed  sqlfluff {pinned} with the dbt templater (hashes verified) in {DESTINATION.relative_to(ROOT)}"


def main() -> int:
    """Install SQLFluff and report the result."""
    sys.stdout.write(install() + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
