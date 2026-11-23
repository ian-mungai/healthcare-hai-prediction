"""Read the single dependency lock: export one group of ``uv.lock`` as hash-pinned requirements and check the lock.

Run from the repository root:

    .venv/bin/python -m scripts.quality.lock_env export <group> --output <folder>
    .venv/bin/python -m scripts.quality.lock_env images [--check]
    .venv/bin/python -m scripts.quality.lock_env check

``pyproject.toml`` declares every dependency in one group per image or tool environment, and ``uv.lock`` pins every group for the three
target platforms (macOS arm64, Linux arm64 and Linux x86_64) with the hashes of every release file. The groups never install together, so
the lock holds one independent resolution per group. ``export`` writes ``requirements.txt`` (and ``build_requirements.txt`` for a group
that names build tools) through the pinned ``uv`` of ``config/quality_tools.json``; pip installs each file with ``--require-hashes``.
``images`` writes the requirement files the Dockerfiles copy into each image from the group of that image, and the Airflow pin list the in-image
check mounts; they are Git-ignored generated inputs, so run it before every image build (``--check`` fails when a file is missing or differs from the
lock). ``check`` fails when the lock is stale, when a group other than the named exceptions needs a source build, or when the model API group and the
training group disagree on a model library.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from pathlib import Path

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
UV = ".tools/bin/uv"  # A literal path, run with the repository as the working folder, so the Docker-call guard can prove the program.
BUILD_PACKAGES = {
    "dvc": ("setuptools",)
}  # Installed first from their own hashed file: the source build of antlr4-python3-runtime needs them (no build isolation).
SOURCE_ONLY = {
    "dvc": ("antlr4-python3-runtime",)
}  # The one accepted exception to wheels only: hydra-core pins this package, which is published as a source archive.
UNINSTALLED_GROUPS = ("orchestration",)  # Pins only: the official Airflow image installs nothing, so its packages are never installed from this lock.
SHARED_MODEL_LIBRARIES = ("numpy", "pandas", "scipy", "scikit-learn", "statsmodels", "xgboost", "skops")  # A model saved by one release must load in the other.
SHARED_PAIR = ("training", "model_api")
IMAGE_FILES = {  # Group to the build-context files its Dockerfile copies (Git-ignored: generated from the lock before each image build).
    "analytics": {"requirements.txt": "services/analytics/requirements.txt"},
    "drift": {"requirements.txt": "services/drift/requirements.txt"},
    "dvc": {"requirements.txt": "services/training/dvc_requirements.txt", "build_requirements.txt": "services/training/dvc_build_requirements.txt"},
    "feature_store": {"requirements.txt": "services/feature_store/requirements.txt"},
    "mlflow": {"requirements.txt": "services/mlflow/requirements.txt"},
    "model_api": {"requirements.txt": "services/model_api/requirements.txt"},
    "quality": {"requirements.txt": "services/quality/requirements.txt"},
    "spark": {"requirements.txt": "services/lakehouse/spark/requirements.txt"},
    "streamlit": {"requirements.txt": "services/streamlit/requirements.txt"},
    "training": {"requirements.txt": "services/training/requirements.txt"},
}
PIN_FILES = {
    "orchestration": "services/orchestration/constraints-3.12.txt"
}  # Group to its generated list of name==version pins (no hashes), which the in-image check mounts.
PLATFORM_FILE = "config/orchestration/platform.json"
EXPORT_DEADLINE = 120
LOCK_DEADLINE = 300
REQUIREMENT = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s;]+)")


class LockError(ValueError):
    """The lock cannot be read or a check found a problem."""


def normalize(name: str) -> str:
    """The canonical package name: lower case with runs of ``-``, ``_`` and ``.`` as one ``-``."""
    return re.sub(r"[-_.]+", "-", name).lower()


def verify_uv(root: Path = ROOT) -> str:
    """Check that the pinned uv of ``.tools/bin`` exists and runs at exactly the pinned version; return its path."""
    pinned = json.loads((root / "config/quality_tools.json").read_text(encoding="utf-8"))["tools"]["uv"]["version"]
    if not (root / UV).is_file():
        raise LockError(f"config/quality_tools.json:1: uv {pinned} is not installed at {root / UV}. Run scripts/quality/install_tools.py --tool uv.")
    version = run_command(UV, ["--version"], cwd=root, timeout=30)
    if version.returncode or not re.search(rf"(?<![\d.]){re.escape(pinned)}(?![\d.])", version.stdout + version.stderr):
        raise LockError(f"config/quality_tools.json:1: uv must be version {pinned}, found {(version.stdout + version.stderr).strip()[:80]!r}.")
    return str(root / UV)


def groups(root: Path = ROOT) -> list[str]:
    """The dependency groups pyproject.toml declares, in file order."""
    return list(tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8")).get("dependency-groups", {}))


def blocks(text: str) -> list[tuple[str, str]]:
    """Split exported requirements into ``(package, block)`` pairs; a block is the requirement line with its hash lines."""
    found: list[tuple[str, list[str]]] = []
    for line in text.splitlines():
        if line and not line.startswith((" ", "#")):
            match = REQUIREMENT.match(line)
            if not match:
                raise LockError(f"pyproject.toml:1: exported line is not a pinned requirement: {line[:80]!r}")
            found.append((normalize(match.group(1)), [line]))
        elif line.startswith(" ") and found:
            found[-1][1].append(line)
    return [(name, "\n".join(lines) + "\n") for name, lines in found]


def export(group: str, root: Path = ROOT) -> str:
    """Return the hash-pinned requirements of one group, exactly as the lock records them (no network, no resolution)."""
    if normalize(group) not in {normalize(name) for name in groups(root)}:
        raise LockError(f"pyproject.toml:1: no dependency group named {group}; the groups are {', '.join(groups(root))}")
    arguments = ["export", "--only-group", group, "--frozen", "--no-emit-project", "--no-header", "--no-annotate", "--format", "requirements-txt", "--quiet"]
    verify_uv(root)
    result = run_command(UV, [*arguments, "--project", str(root)], cwd=root, timeout=EXPORT_DEADLINE)
    if result.returncode:
        raise LockError(f"uv.lock:1: uv export failed for group {group} ({result.returncode}): {result.stderr[-1500:]}")
    return result.stdout


def export_files(group: str, root: Path = ROOT) -> dict[str, str]:
    """The files an install of one group needs: ``requirements.txt`` and, for a group with build tools, ``build_requirements.txt`` first."""
    text = export(group, root)
    build = BUILD_PACKAGES.get(group, ())
    files = {"requirements.txt": "".join(block for name, block in blocks(text) if name not in build)}
    if build:
        files["build_requirements.txt"] = "".join(block for name, block in blocks(text) if name in build)
    return files


def write_files(group: str, folder: Path, root: Path = ROOT) -> list[Path]:
    """Write a group's requirement files into ``folder``; a file whose bytes already match is left untouched."""
    folder.mkdir(parents=True, exist_ok=True)
    written = []
    for name, text in export_files(group, root).items():
        path = folder / name
        if not path.exists() or path.read_text(encoding="utf-8") != text:
            path.write_text(text, encoding="utf-8")
        written.append(path)
    return written


def pin_text(group: str, root: Path = ROOT) -> str:
    """The group's pins as sorted ``name==version`` lines with canonical names, without the Airflow release packages that platform.json pins apart."""
    release = {normalize(name) for name in json.loads((root / PLATFORM_FILE).read_text(encoding="utf-8"))["airflow"]["release_packages"]}
    pinned = versions(export(group, root))
    many = sorted(name for name, found in pinned.items() if len(found) > 1)
    if many:
        raise LockError(f"uv.lock:1: group {group} pins more than one version of {', '.join(many)}; its pin list needs one")
    return "".join(f"{name}=={next(iter(found))}\n" for name, found in sorted(pinned.items()) if name not in release)


def image_files(root: Path = ROOT) -> dict[str, str]:
    """Repository path to expected text of every generated file: the image requirement files and the pin lists."""
    expected = {}
    for group, files in IMAGE_FILES.items():
        exported = export_files(group, root)
        expected.update({path: exported[name] for name, path in files.items()})
    expected.update({path: pin_text(group, root) for group, path in PIN_FILES.items()})
    return expected


def write_images(root: Path = ROOT) -> list[str]:
    """Write every image requirement file; a file whose bytes already match is left untouched. Return the paths written."""
    written = []
    for path, text in image_files(root).items():
        target = root / path
        if not target.exists() or target.read_text(encoding="utf-8") != text:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
            written.append(path)
    return written


def write_pins(root: Path = ROOT) -> list[str]:
    """Write only the pin lists (the offline orchestration checks need them); a file whose bytes already match is left untouched."""
    written = []
    for group, path in PIN_FILES.items():
        text, target = pin_text(group, root), root / path
        if not target.exists() or target.read_text(encoding="utf-8") != text:
            target.write_text(text, encoding="utf-8")
            written.append(path)
    return written


def image_problems(root: Path = ROOT) -> list[str]:
    """Each generated image requirement file that is missing or differs from the lock."""
    problems = []
    for path, text in image_files(root).items():
        target = root / path
        if not target.is_file():
            problems.append(f"{path}:1: missing; run `.venv/bin/python -m scripts.quality.lock_env images` before the image build or the offline checks")
        elif target.read_text(encoding="utf-8") != text:
            problems.append(
                f"{path}:1: differs from the lock; run `.venv/bin/python -m scripts.quality.lock_env images` before the image build or the offline checks"
            )
    return problems


def versions(text: str) -> dict[str, set[str]]:
    """Package name to the versions an export pins (more than one only when platforms differ)."""
    found: dict[str, set[str]] = {}
    for name, block in blocks(text):
        found.setdefault(name, set()).add(block.splitlines()[0].split("==", 1)[1].split()[0])
    return found


def stale_problems(root: Path = ROOT) -> list[str]:
    """``uv lock --check`` fails when pyproject.toml and uv.lock disagree; with a current lock it needs no network."""
    verify_uv(root)
    result = run_command(UV, ["lock", "--check", "--project", str(root)], cwd=root, timeout=LOCK_DEADLINE)
    if result.returncode:
        return [f"uv.lock:1: the lock does not match pyproject.toml; run `.tools/bin/uv lock` ({(result.stderr or result.stdout).strip()[-300:]})"]
    return []


def source_build_problems(root: Path = ROOT) -> list[str]:
    """Every group installs wheels only, except the named source-only package, and a group with no pins in the lock cannot be empty."""
    lock = tomllib.loads((root / "uv.lock").read_text(encoding="utf-8"))
    without_wheels = {normalize(package["name"]) for package in lock["package"] if not package.get("wheels") and package["name"] != "hai-prediction"}
    problems = []
    for group in groups(root):
        if group in UNINSTALLED_GROUPS:
            continue
        pinned = versions(export(group, root))
        if not pinned:
            problems.append(f"pyproject.toml:1: group {group} exports no package")
        for name in sorted(without_wheels & set(pinned) - set(SOURCE_ONLY.get(group, ()))):
            problems.append(f"uv.lock:1: group {group} pins {name}, which is published only as a source archive; image and tool installs take wheels only")
        for name in SOURCE_ONLY.get(group, ()):
            if name not in pinned:
                problems.append(f"uv.lock:1: group {group} no longer pins {name}; remove the exception in scripts/quality/lock_env.py")
    return problems


def model_library_problems(training: str, serving: str) -> list[str]:
    """The training and the model API exports pin the same release of every shared model library."""
    left, right = versions(training), versions(serving)
    return [
        f"uv.lock:1: {name} is {sorted(left.get(name, set()))} in group {SHARED_PAIR[0]} and {sorted(right.get(name, set()))} in group {SHARED_PAIR[1]}; "
        "a model saved with one release is not safe to load with another"
        for name in SHARED_MODEL_LIBRARIES
        if left.get(name) != right.get(name) or not left.get(name)
    ]


def check(root: Path = ROOT) -> list[str]:
    """All lock problems, each as ``path:line: problem``."""
    problems = stale_problems(root)
    if problems:
        return problems
    problems += source_build_problems(root)
    problems += model_library_problems(export(SHARED_PAIR[0], root), export(SHARED_PAIR[1], root))
    return problems


def main(argv: list[str] | None = None) -> int:
    """Export one group's requirement files, or check the lock."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    export_parser = commands.add_parser("export", help="write the hash-pinned requirement files of one group")
    export_parser.add_argument("group")
    export_parser.add_argument("--output", type=Path, required=True)
    images_parser = commands.add_parser("images", help="write the requirement files the Dockerfiles copy, from each image's group")
    images_parser.add_argument("--check", action="store_true", help="write nothing; fail when a file is missing or differs from the lock")
    commands.add_parser("check", help="fail when the lock is stale or breaks a lock rule")
    args = parser.parse_args(argv)
    try:
        if args.command == "export":
            for path in write_files(args.group, args.output):
                sys.stdout.write(f"{path}\n")
            return 0
        if args.command == "images":
            problems = image_problems() if args.check else []
            written = [] if args.check else write_images()
            sys.stdout.write("".join(f"{path}\n" for path in written) or "image requirement files are current\n")
            for problem in problems:
                sys.stderr.write(f"{problem}\n")
            return 1 if problems else 0
        problems = check()
    except (LockError, OSError) as error:
        sys.stderr.write(f"{error}\n")
        return 1
    for problem in problems:
        sys.stderr.write(f"{problem}\n")
    if not problems:
        sys.stdout.write(f"lock: {len(groups())} groups current, wheels only apart from {SOURCE_ONLY['dvc'][0]}, model libraries equal\n")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
