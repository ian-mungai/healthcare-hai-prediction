"""Install the pinned SQLFluff and dbt templater for the sqlfluff pre-commit hook and local CI.

Run from the repository root:

    .venv/bin/python -m scripts.quality.install_sqlfluff

The versions are pinned with hashes by ``scripts/quality/sqlfluff/requirements.txt``. They go into their own virtual
environment, ``.tools/sqlfluff``, because dbt-core caps pathspec below the version MyPy needs in ``.venv``. pip
verifies every hash and installs wheels only. A rerun with the same requirements changes nothing; a failed install
removes the copied requirements so the next run retries instead of looking current. It then installs the dbt packages
from ``dbt/package-lock.yml`` into the ignored ``data/analytics/dbt/dbt_packages``, which the dbt templater needs to
compile the project; a rerun reinstalls the same locked versions.
"""

from __future__ import annotations

import filecmp
import os
import shutil
import sys
from pathlib import Path

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "scripts/quality/sqlfluff/requirements.txt"
DESTINATION = ROOT / ".tools/sqlfluff"
INSTALLED = DESTINATION / "requirements.txt"
SQLFLUFF = DESTINATION / "bin/sqlfluff"
DBT_PROJECT = ROOT / "dbt"
DBT_WORK = ROOT / "data/analytics/dbt/deps"


def pinned_version() -> str:
    """Return the SQLFluff version the committed requirements pin."""
    return next(line.split("==", 1)[1].split()[0] for line in SOURCE.read_text().splitlines() if line.startswith("sqlfluff=="))


def current() -> bool:
    """Return whether the installed environment matches the committed requirements and runs the pinned version."""
    if not (SQLFLUFF.exists() and INSTALLED.exists() and filecmp.cmp(SOURCE, INSTALLED, shallow=False)):
        return False
    result = run_command(str(SQLFLUFF), ["--version"], timeout=60)
    return result.returncode == 0 and result.stdout.strip().endswith(pinned_version())


def base_python() -> str:
    """Return the interpreter the running virtual environment was made from, by the path its pyvenv.cfg records.

    A virtual environment made by another one's Python records the versioned Homebrew Cellar folder, which a patch upgrade
    removes, so the hook's interpreter link breaks; the recorded path (/opt/homebrew/opt/python@3.12/bin) survives it.
    """
    config = Path(sys.prefix) / "pyvenv.cfg"
    if sys.prefix == sys.base_prefix or not config.exists():
        return sys.executable
    home = next((line.split("=", 1)[1].strip() for line in config.read_text().splitlines() if line.split("=", 1)[0].strip() == "home"), "")
    candidate = Path(home) / f"python{sys.version_info.major}.{sys.version_info.minor}"
    return str(candidate) if candidate.exists() else sys.executable


def install() -> str:
    """Create the environment and install the hash-pinned requirements; return what happened."""
    pinned = pinned_version()
    if current():
        return f"unchanged  sqlfluff {pinned}"
    INSTALLED.unlink(missing_ok=True)
    if DESTINATION.exists():
        shutil.rmtree(DESTINATION)
    run_command(base_python(), ["-m", "venv", str(DESTINATION)], timeout=300, check=True)
    pip = [str(DESTINATION / "bin/python"), "-m", "pip", "install", "--quiet", "--require-hashes", "--only-binary=:all:", "-r", str(SOURCE)]
    result = run_command(pip[0], pip[1:], timeout=900)
    if result.returncode:
        raise SystemExit(f"pip install failed for sqlfluff {pinned}; nothing usable was installed:\n{result.stderr[-2000:]}")
    shutil.copyfile(SOURCE, INSTALLED)
    return f"installed  sqlfluff {pinned} with the dbt templater (hashes verified) in {DESTINATION.relative_to(ROOT)}"


def install_packages() -> str:
    """Install the dbt packages from the committed lock file; return what happened."""
    lock = DBT_PROJECT / "package-lock.yml"
    before = lock.read_bytes()
    env = {**os.environ, "DBT_PROFILES_DIR": str(DBT_PROJECT), "DBT_LOG_PATH": str(DBT_WORK / "logs"), "DBT_TARGET_PATH": str(DBT_WORK / "target")}
    result = run_command(str(DESTINATION / "bin/dbt"), ["deps", "--project-dir", str(DBT_PROJECT)], cwd=ROOT, env=env, timeout=600)
    if result.returncode:
        raise SystemExit(f"dbt deps failed:\n{(result.stdout + result.stderr)[-2000:]}")
    if lock.read_bytes() != before:
        raise SystemExit("dbt deps changed dbt/package-lock.yml; restore it and pin the packages there")
    return "installed  dbt packages from dbt/package-lock.yml"


def main() -> int:
    """Install SQLFluff and the dbt packages and report the result."""
    sys.stdout.write(install() + "\n")
    sys.stdout.write(install_packages() + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
