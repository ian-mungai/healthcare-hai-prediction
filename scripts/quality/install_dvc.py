"""Install the pinned DVC and its S3 remote for the training baseline (project decision 20).

Run from the repository root:

    .venv/bin/python -m scripts.quality.install_dvc

The versions are pinned with hashes by the ``dvc`` group of ``uv.lock`` (declared in ``pyproject.toml``; ``scripts/quality/lock_env.py`` exports it).
They go into their own virtual environment, ``.tools/dvc``, so the training image, CI and every developer machine run the same DVC. pip verifies every hash.
All packages are wheels except one, antlr4-python3-runtime (a dependency of dvc through hydra-core), which is published only as a source archive (the project
owner accepted this one source build as the only exception to wheels only): the export's build requirements install its hashed build tool first, and the second
step builds that one archive with ``--no-build-isolation`` so that nothing unpinned is downloaded. A rerun with the same lock changes nothing; a failed install
removes the copied requirements so the next run retries instead of looking current. A Homebrew patch upgrade that removes the interpreter this environment links
to makes ``dvc --version`` fail, so the next run rebuilds it.
"""

from __future__ import annotations

import filecmp
import shutil
import sys
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from scripts.process import run_command
from scripts.quality import lock_env

ROOT = Path(__file__).resolve().parents[2]
DESTINATION = ROOT / ".tools/dvc"
INSTALLED = DESTINATION / "requirements.txt"
SOURCE_ONLY = "antlr4-python3-runtime"
VENV_DEADLINE = 300
PIP_DEADLINE = 900


@contextmanager
def exported() -> Iterator[tuple[Path, Path]]:
    """Export the lock's dvc group into a scratch folder and give its requirements file and its build requirements file."""
    with tempfile.TemporaryDirectory(prefix="hai_dvc_lock_") as folder:
        lock_env.write_files("dvc", Path(folder))
        yield Path(folder) / "requirements.txt", Path(folder) / "build_requirements.txt"


def pinned_version(package: str, source: Path | None = None) -> str:
    """Return the version the lock pins for ``package`` in the dvc group (or in ``source``, an exported requirements file)."""
    text = source.read_text() if source else lock_env.export_files("dvc")["requirements.txt"]
    return next(line.split("==", 1)[1].split()[0] for line in text.splitlines() if line.startswith(f"{package}=="))


def current(source: Path | None = None, destination: Path = DESTINATION) -> bool:
    """Return whether the installed environment matches the lock's dvc group (or ``source``) and runs the pinned version."""
    if source is None:
        with exported() as (requirements, _):
            return current(requirements, destination)
    installed = destination / "requirements.txt"
    dvc = destination / "bin/dvc"
    if not (dvc.exists() and installed.exists() and filecmp.cmp(source, installed, shallow=False)):
        return False
    try:
        result = run_command(str(dvc), ["--version"], timeout=60)
    except OSError:
        return False
    return result.returncode == 0 and result.stdout.strip().endswith(pinned_version("dvc", source))


def install(source: Path | None = None, build_source: Path | None = None, destination: Path = DESTINATION) -> str:
    """Create the environment and install the hash-pinned requirements of the lock's dvc group; return what happened."""
    if source is None or build_source is None:
        with exported() as (requirements, build):
            return install(requirements, build, destination)
    pinned = pinned_version("dvc", source)
    if current(source, destination):
        return f"unchanged  dvc {pinned}"
    (destination / "requirements.txt").unlink(missing_ok=True)
    if destination.exists():
        shutil.rmtree(destination)
    run_command(sys.executable, ["-m", "venv", str(destination)], timeout=VENV_DEADLINE, check=True)
    python = str(destination / "bin/python")
    pip = ["-m", "pip", "install", "--quiet", "--require-hashes", "--only-binary=:all:", f"--no-binary={SOURCE_ONLY}", "--no-build-isolation"]
    for requirements in (build_source, source):
        result = run_command(python, [*pip, "-r", str(requirements)], timeout=PIP_DEADLINE)
        if result.returncode:
            shutil.rmtree(destination, ignore_errors=True)
            raise SystemExit(f"pip install failed for dvc {pinned} from {requirements.name}. Nothing usable was installed:\n{result.stderr[-2000:]}")
    shutil.copyfile(source, destination / "requirements.txt")
    return f"installed  dvc {pinned} with dvc-s3 {pinned_version('dvc-s3', source)} (hashes verified) in {destination}"


def main() -> int:
    """Install DVC and report the result."""
    sys.stdout.write(install() + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
