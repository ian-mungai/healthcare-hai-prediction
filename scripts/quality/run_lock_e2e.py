"""E2E for the single dependency lock: the exports of every group, the lock checks on the real lock and on planted defects, and hash-checked installs.

Run from the repository root after ``python scripts/quality/install_tools.py --tool uv``: ``.venv/bin/python -m scripts.quality.run_lock_e2e``.
Every group of pyproject.toml is exported from uv.lock: each requirement must be an exact pin with hashes, an export must repeat byte for byte, and
the Feast release must be the upgraded one in every group that pins it. The lock check must pass on the real lock. Planted defects use a scratch copy
of pyproject.toml and uv.lock: a declared dependency the lock does not hold, a package with no wheel in an image group and a model library that differs
between the training and the model API groups. The image requirement files are generated into a scratch tree, then a stale one is reported.
The import-linter group is then installed into a temporary environment with ``pip --require-hashes``: the good export installs at the pinned version, an
export with one wrong hash fails, and an export with no hash fails.
Results go to ``data/e2e/lock/<UTC>/report.json``.
"""

from __future__ import annotations

import json
import re
import shutil
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.process import SubprocessError, TimeoutExpired, run_command
from scripts.quality import lock_env

ROOT = Path(__file__).resolve().parents[2]
EXPECTED_GROUPS = {
    "analytics",
    "check_jsonschema",
    "drift",
    "dvc",
    "feature_store",
    "host",
    "import_linter",
    "mlflow",
    "model_api",
    "orchestration",
    "quality",
    "review",
    "spark",
    "sqlfluff",
    "streamlit",
    "training",
}
PIP_DEADLINE = 900
Problems = list[str]


def report_case(name: str, check: Callable[[], Problems]) -> dict[str, Any]:
    """Run one case; any exception is a failed case with its text, never a crash of the runner."""
    try:
        problems = check()
    except (SubprocessError, OSError, ValueError, SystemExit, TimeoutExpired) as error:
        problems = [f"{type(error).__name__}: {error}"]
    return {"case": name, "passed": not problems, "problems": problems}


def declared_groups() -> Problems:
    """pyproject.toml declares exactly the sixteen image and tool groups."""
    found = set(lock_env.groups())
    return [] if found == EXPECTED_GROUPS else [f"groups differ: missing {sorted(EXPECTED_GROUPS - found)}, unexpected {sorted(found - EXPECTED_GROUPS)}"]


def exports_are_pinned() -> Problems:
    """Every requirement of every group is an exact pin with at least one hash, in the shape pip --require-hashes reads."""
    problems: Problems = []
    for group in lock_env.groups():
        text = lock_env.export(group)
        entries = lock_env.blocks(text)
        if not entries:
            problems.append(f"{group}: the export is empty")
        for name, block in entries:
            if not re.search(r"^\s+--hash=sha256:[0-9a-f]{64}", block, re.MULTILINE):
                problems.append(f"{group}: {name} has no hash")
        if any(line.startswith("-e ") or " @ " in line for line in text.splitlines()):
            problems.append(f"{group}: the export holds a URL or editable requirement")
    return problems


def exports_repeat() -> Problems:
    """An export gives the same bytes twice and a rewrite leaves an unchanged file untouched (the dvc group also splits its build tools)."""
    problems: Problems = []
    with tempfile.TemporaryDirectory(prefix="lock_e2e_") as folder:
        for group in ("import_linter", "dvc"):
            first = {path.name: path.read_bytes() for path in lock_env.write_files(group, Path(folder) / group)}
            stamps = {path.name: path.stat().st_mtime_ns for path in (Path(folder) / group).iterdir()}
            second = {path.name: path.read_bytes() for path in lock_env.write_files(group, Path(folder) / group)}
            if first != second:
                problems.append(f"{group}: two exports differ")
            if stamps != {path.name: path.stat().st_mtime_ns for path in (Path(folder) / group).iterdir()}:
                problems.append(f"{group}: an unchanged export was rewritten")
        dvc = Path(folder) / "dvc"
        if sorted(path.name for path in dvc.iterdir()) != ["build_requirements.txt", "requirements.txt"]:
            problems.append("dvc: expected a build requirements file and a requirements file")
        elif [name for name, _ in lock_env.blocks((dvc / "build_requirements.txt").read_text())] != ["setuptools"]:
            problems.append("dvc: the build requirements file must hold setuptools only")
        elif "setuptools" in {name for name, _ in lock_env.blocks((dvc / "requirements.txt").read_text())}:
            problems.append("dvc: setuptools must not repeat in the requirements file")
    return problems


def feast_is_upgraded() -> Problems:
    """Feast 0.67.0 or later (GHSA-gg2p-37pv-v7fq) in every group that pins it, and no 0.66.0 anywhere in the lock."""
    problems: Problems = []
    for group in ("feature_store", "model_api"):
        found = lock_env.versions(lock_env.export(group)).get("feast", set())
        if not found or any(tuple(int(part) for part in version.split(".")) < (0, 67, 0) for version in found):
            problems.append(f"{group}: feast is {sorted(found)}; 0.67.0 or later is required")
    if re.search(r'name = "feast"\nversion = "0\.66\.0"', (ROOT / "uv.lock").read_text(encoding="utf-8")):
        problems.append("uv.lock still holds feast 0.66.0")
    return problems


def orchestration_pins_airflow() -> Problems:
    """The orchestration group pins the Airflow release and its two release packages exactly as config/orchestration/platform.json records them."""
    platform = json.loads((ROOT / "config/orchestration/platform.json").read_text(encoding="utf-8"))["airflow"]
    found = lock_env.versions(lock_env.export("orchestration"))
    wanted = {"apache-airflow": platform["version"], **{lock_env.normalize(name): version for name, version in platform["release_packages"].items()}}
    return [
        f"orchestration: {name} is {sorted(found.get(name, set()))}, expected {version}" for name, version in wanted.items() if found.get(name) != {version}
    ]


def real_lock_passes() -> Problems:
    """The lock checks pass on the committed lock (current, wheels only apart from one package, equal model libraries)."""
    return lock_env.check()


def scratch_copy(top: Path) -> Path:
    """A scratch repository root with the lock files, the tool pins and a link to the real pinned uv."""
    root = top / "scratch"
    (root / "config/orchestration").mkdir(parents=True)
    for name in ("pyproject.toml", "uv.lock", "config/quality_tools.json", "config/orchestration/platform.json"):
        shutil.copyfile(ROOT / name, root / name)
    (root / ".tools").symlink_to(ROOT / ".tools")
    return root


def planted_defects() -> Problems:
    """A stale lock, an image group with a source-only package and a model library mismatch are each reported by name."""
    problems: Problems = []
    with tempfile.TemporaryDirectory(prefix="lock_e2e_") as folder:
        root = scratch_copy(Path(folder))
        text = (root / "pyproject.toml").read_text(encoding="utf-8")
        (root / "pyproject.toml").write_text(text.replace("import_linter = [\n", 'import_linter = [\n  "idna==3.20",\n', 1), encoding="utf-8")
        stale = lock_env.check(root)
        if not stale or "does not match pyproject.toml" not in stale[0]:
            problems.append(f"a dependency missing from the lock was not reported: {stale}")
        (root / "pyproject.toml").write_text(text, encoding="utf-8")
        lock = (root / "uv.lock").read_text(encoding="utf-8")
        planted, count = re.subn(
            r'(name = "click"\nversion = "[^"]+"\n(?:(?!\[\[package\]\]).)*?)wheels = \[\n.*?\n\]\n', r"\1", lock, count=1, flags=re.DOTALL
        )
        (root / "uv.lock").write_text(planted, encoding="utf-8")
        reported = lock_env.source_build_problems(root)
        if count != 1 or not any("group import_linter pins click" in line for line in reported):
            problems.append(f"a package with no wheel in an image group was not reported ({count} edits): {reported[:2]}")
        training = lock_env.export("training")
        serving = lock_env.export("model_api")
        changed = re.sub(r"^scikit-learn==[^ ]+", "scikit-learn==0.0.1", serving, count=1, flags=re.MULTILINE)
        mismatch = lock_env.model_library_problems(training, changed)
        if len(mismatch) != 1 or "scikit-learn" not in mismatch[0]:
            problems.append(f"a model library mismatch was not reported once: {mismatch}")
        if lock_env.model_library_problems(training, serving):
            problems.append("the real training and model API exports were reported as different")
        pins = json.loads((root / "config/quality_tools.json").read_text(encoding="utf-8"))
        pins["tools"]["uv"]["version"] = "0.0.1"
        (root / "config/quality_tools.json").write_text(json.dumps(pins), encoding="utf-8")
        try:
            lock_env.export("import_linter", root)
            problems.append("a uv of another version than the pin was accepted")
        except lock_env.LockError as error:
            if "must be version 0.0.1" not in str(error):
                problems.append(f"the wrong-version message does not name the pin: {error}")
    return problems


def image_files_written() -> Problems:
    """``images`` writes each image's requirement files from its group, a second run writes nothing, and a stale or missing file is reported."""
    problems: Problems = []
    with tempfile.TemporaryDirectory(prefix="lock_e2e_") as folder:
        root = scratch_copy(Path(folder))
        before = lock_env.image_problems(root)
        if len(before) != sum(len(files) for files in lock_env.IMAGE_FILES.values()) + len(lock_env.PIN_FILES):
            problems.append(f"an empty tree must report every image file as missing: {len(before)} problems")
        written = lock_env.write_images(root)
        if len(written) != len(before) or lock_env.image_problems(root) or lock_env.write_images(root):
            problems.append("the first run must write every file, and a second run none")
        for group, files in lock_env.IMAGE_FILES.items():
            for name, path in files.items():
                if (root / path).read_text(encoding="utf-8") != lock_env.export_files(group)[name]:
                    problems.append(f"{path} does not equal the {group} export")
        pins = (root / lock_env.PIN_FILES["orchestration"]).read_text(encoding="utf-8").splitlines()
        if not pins or any(not re.fullmatch(r"[a-z0-9][a-z0-9-]*==\S+", line) for line in pins) or any(name.startswith("apache-airflow==") for name in pins):
            problems.append("the Airflow pin list must hold canonical name==version lines and no release package")
        stale = root / "services/mlflow/requirements.txt"
        stale.write_text(stale.read_text(encoding="utf-8").replace("--hash=sha256:", "--hash=sha256:0", 1), encoding="utf-8")
        found = lock_env.image_problems(root)
        if len(found) != 1 or "services/mlflow/requirements.txt:1: differs from the lock" not in found[0]:
            problems.append(f"a stale image file was not reported once: {found}")
    return problems


def pip_install(environment: Path, requirements: Path) -> tuple[int, str]:
    """Install a requirements file into a virtual environment with the running interpreter's pip, which checks every hash."""
    arguments = [
        "-m",
        "pip",
        "--python",
        str(environment / "bin/python"),
        "install",
        "--quiet",
        "--require-hashes",
        "--only-binary=:all:",
        "-r",
        str(requirements),
    ]
    result = run_command(sys.executable, arguments, timeout=PIP_DEADLINE)
    return result.returncode, result.stderr[-300:]


def hashed_install() -> Problems:
    """The import-linter group installs under --require-hashes and runs; one wrong hash and no hash each fail the install."""
    problems: Problems = []
    with tempfile.TemporaryDirectory(prefix="lock_e2e_") as folder:
        top = Path(folder)
        text = lock_env.export_files("import_linter")["requirements.txt"]
        lines = text.splitlines()
        wrong_lines = [line.replace("--hash=sha256:", "--hash=sha256:0") if index < 3 else line for index, line in enumerate(lines)]
        wrong = "\n".join(wrong_lines) + "\n"
        bare = "".join(line.rstrip(" \\") + "\n" for line in text.splitlines() if not line.startswith("    --hash="))
        for label, content in (("wrong_hash", wrong), ("no_hash", bare)):
            run_command(sys.executable, ["-m", "venv", str(top / label)], timeout=300, check=True)
            (top / f"{label}.txt").write_text(content, encoding="utf-8")
            code, _ = pip_install(top / label, top / f"{label}.txt")
            if code == 0:
                problems.append(f"{label}: the install did not fail")
        run_command(sys.executable, ["-m", "venv", str(top / "good")], timeout=300, check=True)
        (top / "good.txt").write_text(text, encoding="utf-8")
        code, error = pip_install(top / "good", top / "good.txt")
        if code:
            return [*problems, f"good export did not install: {error}"]
        shown = run_command(sys.executable, ["-m", "pip", "--python", str(top / "good/bin/python"), "show", "import-linter"], timeout=60)
        pinned = json.loads((ROOT / "config/quality_tools.json").read_text(encoding="utf-8"))["tools"]["import-linter"]["version"]
        if shown.returncode or f"Version: {pinned}" not in shown.stdout:
            problems.append(f"the installed import-linter is not {pinned}: {shown.stdout.strip()[:100]}")
    return problems


CASES: tuple[tuple[str, Callable[[], Problems]], ...] = (
    ("declared groups", declared_groups),
    ("every export is pinned with hashes", exports_are_pinned),
    ("exports repeat and split the dvc build tools", exports_repeat),
    ("feast is upgraded", feast_is_upgraded),
    ("orchestration pins the Airflow release", orchestration_pins_airflow),
    ("the real lock passes its checks", real_lock_passes),
    ("planted defects are reported", planted_defects),
    ("image requirement files", image_files_written),
    ("hash-checked install", hashed_install),
)


def main() -> int:
    """Run every case, write the report and return 1 when any case failed."""
    results = [report_case(name, check) for name, check in CASES]
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    folder = ROOT / "data/e2e/lock" / stamp
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "report.json").write_text(json.dumps({"cases": results, "uv": lock_env.verify_uv(), "generated": stamp}, indent=2) + "\n", encoding="utf-8")
    for result in results:
        sys.stdout.write(f"{'ok  ' if result['passed'] else 'FAIL'} {result['case']}\n")
        for problem in result["problems"]:
            sys.stdout.write(f"     {problem}\n")
    sys.stdout.write(f"report: {folder / 'report.json'}\n")
    return 0 if all(result["passed"] for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
