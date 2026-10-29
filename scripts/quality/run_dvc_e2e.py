"""E2E for the DVC setup of project decision 20: the pinned environment, the tracked repository files and the rendered remote.

Run from the repository root after ``python -m scripts.quality.install_dvc``: ``python -m scripts.quality.run_dvc_e2e``. The real environment
(``.tools/dvc``) is checked for its pinned versions and an unchanged rerun. Planted defects use a temporary destination, never ``.tools/dvc``: a
requirements file with one wrong hash, one with a package's hashes removed and then the good file, which must install after the failed attempts. The Linux
or macOS wheels (whichever this host is not) are downloaded into a temporary folder with ``--require-hashes`` to prove that the hashes cover both
platforms. The tracked ``.dvc`` files are read for the owner's settings and for the absence of any account value. The remote renderer runs on a synthetic
``.env`` in a temporary folder (the project ``.env`` is never opened): its output must hold the bucket only in the ignored file, the rerun must give the same
bytes, a missing bucket must fail without writing, and DVC itself must read the rendered remote.
Results go to ``data/e2e/dvc_setup/<UTC>/report.json``.
"""

from __future__ import annotations

import configparser
import json
import os
import platform
import shutil
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.infrastructure.render_project_config import ConfigurationError, build_configuration, dvc_local_config
from scripts.process import SubprocessError, TimeoutExpired, run_command
from scripts.quality import install_dvc

ROOT = Path(__file__).resolve().parents[2]
DVC_HOME = ROOT / ".tools/dvc"
BUCKET = "example-project-dvc-e2e-bucket"
SYNTHETIC_ENV = "\n".join(
    (
        "AWS_ACCOUNT_ID=" + "1" * 12,
        "AWS_PROFILE=example_project_dev",
        "AWS_REGION=eu-west-1",
        "ENVIRONMENT=dev",
        "PROJECT_NAME=example_project",
        f"S3_BUCKET={BUCKET}",
        "",
    )
)
RENDERER = "scripts.infrastructure.render_project_config"
DOWNLOAD_DEADLINE = 900
HOST_PLATFORMS = {
    "Darwin": ("Linux x86_64", ["manylinux2014_x86_64", "manylinux_2_17_x86_64", "manylinux_2_28_x86_64", "manylinux_2_34_x86_64", "linux_x86_64"]),
    "Linux": (
        "macOS arm64",
        ["macosx_11_0_arm64", "macosx_12_0_arm64", "macosx_13_0_arm64", "macosx_14_0_arm64", "macosx_15_0_arm64", "macosx_11_0_universal2"],
    ),
}
Problems = list[str]


def report_case(name: str, check: Callable[[], Problems]) -> dict[str, Any]:
    """Run one case; any exception is a failed case with its text, never a crash of the runner."""
    try:
        problems = check()
    except (SubprocessError, OSError, ValueError, SystemExit, ConfigurationError, TimeoutExpired) as error:
        problems = [f"{type(error).__name__}: {error}"]
    return {"case": name, "passed": not problems, "problems": problems}


def tracked_config() -> configparser.ConfigParser:
    parser = configparser.ConfigParser()
    parser.read(ROOT / ".dvc/config", encoding="utf-8")
    return parser


def render(env_file: Path, target: Path, *extra: str) -> tuple[int, str]:
    result = run_command(
        sys.executable, ["-m", RENDERER, "--env-file", str(env_file), "--stack", "dvc", "--dvc-config", str(target), *extra], cwd=ROOT, timeout=120
    )
    return result.returncode, result.stdout + result.stderr


def pinned_versions() -> Problems:
    """The real environment runs the pinned dvc and dvc-s3 and a rerun of the installer changes nothing."""
    problems: Problems = []
    if not install_dvc.current():
        return [".tools/dvc does not match scripts/quality/dvc/requirements.txt; run python -m scripts.quality.install_dvc"]
    dvc = run_command(str(DVC_HOME / "bin/dvc"), ["--version"], timeout=60).stdout.strip()
    s3 = run_command(str(DVC_HOME / "bin/python"), ["-m", "pip", "show", "dvc-s3"], timeout=60).stdout
    if dvc != install_dvc.pinned_version("dvc") or f"Version: {install_dvc.pinned_version('dvc-s3')}" not in s3:
        problems.append(f"installed versions (dvc {dvc!r}) differ from the pins")
    before = install_dvc.INSTALLED.stat().st_mtime_ns
    message = install_dvc.install()
    if not message.startswith("unchanged") or install_dvc.INSTALLED.stat().st_mtime_ns != before:
        problems.append(f"a rerun changed the environment: {message}")
    return problems


def mutated_source(base: Path, name: str, change: Callable[[str], str]) -> Path:
    path = base / name
    path.write_text(change(install_dvc.SOURCE.read_text()))
    return path


def flip_first_hash(text: str) -> str:
    marker = "--hash=sha256:"
    start = text.index(marker) + len(marker)
    return text[:start] + ("0" if text[start] != "0" else "1") + text[start + 1 :]


def drop_first_hashes(text: str) -> str:
    """Remove the hash lines of the first package."""
    lines = text.splitlines()
    first = next(index for index, line in enumerate(lines) if line and not line.startswith(("#", " ")))
    lines[first] = lines[first].rstrip(" \\")
    return "\n".join(line for index, line in enumerate(lines) if index <= first or not line.startswith("    --hash=")) + "\n"


def failed_install_then_retry(top: Path) -> Problems:
    """A wrong hash and a missing hash both abort with no marker left; the good file then installs into the same destination."""
    problems: Problems = []
    destination = top / "env"
    for label, change in (("wrong_hash", flip_first_hash), ("missing_hash", drop_first_hashes)):
        source = mutated_source(top, f"{label}.txt", change)
        try:
            install_dvc.install(source=source, destination=destination)
            problems.append(f"{label}: the install did not fail")
        except SystemExit as error:
            if "pip install failed" not in str(error):
                problems.append(f"{label}: the failure does not name pip: {str(error)[:200]}")
        if (destination / "requirements.txt").exists():
            problems.append(f"{label}: a marker was left, so the next run would look current")
    message = install_dvc.install(destination=destination)
    if not message.startswith("installed"):
        problems.append(f"the retry did not install: {message}")
    if not install_dvc.current(destination=destination):
        problems.append("the retried environment is not current")
    return problems


def other_platform_hashes(top: Path) -> Problems:
    """The hashes cover the wheels of the platform this host does not run: pip downloads them under --require-hashes."""
    label, tags = HOST_PLATFORMS.get(platform.system(), ("", []))
    if not tags:
        return [f"no cross-platform check is defined for {platform.system()}"]
    kept: list[str] = []
    skip = False
    for line in install_dvc.SOURCE.read_text().splitlines():
        skip = line.startswith("antlr4-python3-runtime==") or (skip and line.startswith("    --hash="))
        if not skip:
            kept.append(line)
    requirements = top / "wheels_only.txt"
    requirements.write_text("\n".join(kept) + "\n")
    args = ["-m", "pip", "download", "--quiet", "--no-deps", "--only-binary=:all:", "--require-hashes", "--python-version", "3.12", "--implementation", "cp"]
    args += ["--abi", "cp312", "--abi", "abi3", "--abi", "none", "-d", str(top / "wheels"), "-r", str(requirements)]
    for tag in tags:
        args += ["--platform", tag]
    result = run_command(str(DVC_HOME / "bin/python"), args, timeout=DOWNLOAD_DEADLINE)
    if result.returncode:
        return [f"{label}: pip could not download every pinned wheel with its hash: {result.stderr[-600:]}"]
    count = len(list((top / "wheels").glob("*.whl")))
    return [] if count >= 100 else [f"{label}: only {count} wheels downloaded"]


def tracked_files() -> Problems:
    """The tracked configuration holds the owner's three settings and no remote, URL or account value."""
    problems: Problems = []
    parser = tracked_config()
    if parser.get("core", "autostage", fallback="") != "false":
        problems.append(".dvc/config must set core.autostage to false")
    if parser.get("core", "analytics", fallback="") != "false":
        problems.append(".dvc/config must set core.analytics to false")
    if parser.get("core", "no_scm", fallback="") != "true":
        problems.append(".dvc/config must set core.no_scm to true: the matrix and its metafile live under git-ignored data/")
    text = (ROOT / ".dvc/config").read_text()
    for forbidden in ("url", "s3://", "remote", "profile", "region", "bucket"):
        if forbidden in text.lower():
            problems.append(f".dvc/config must not hold {forbidden!r}")
    if not (ROOT / ".dvcignore").is_file():
        problems.append(".dvcignore is missing")
    return problems


def ignored_paths() -> Problems:
    """Git ignores the local config, the cache and the tmp folder of DVC; it tracks the config and .dvcignore."""
    problems: Problems = []
    for path in (".dvc/config.local", ".dvc/cache", ".dvc/tmp"):
        if run_command("git", ["check-ignore", "--quiet", path], cwd=ROOT, timeout=60).returncode:
            problems.append(f"{path} is not ignored by Git")
    for path in (".dvc/config", ".dvcignore"):
        if run_command("git", ["check-ignore", "--quiet", path], cwd=ROOT, timeout=60).returncode == 0:
            problems.append(f"{path} is ignored; it must be tracked")
    return problems


def rendered_remote(top: Path) -> Problems:
    """The synthetic .env gives the remote in the ignored file only; a rerun gives the same bytes; a check passes; nothing prints the bucket."""
    problems: Problems = []
    env = top / "synthetic.env"
    env.write_text(SYNTHETIC_ENV)
    target = top / "dvc" / "config.local"
    code, output = render(env, target)
    if code or BUCKET in output:
        problems.append(f"render exit {code} or the output names the bucket: {output[-300:]}")
    first = target.read_bytes()
    if f"url = s3://{BUCKET}/dvc/" not in first.decode():
        problems.append("the rendered file does not hold the dvc/ prefix URL")
    if target.stat().st_mode & 0o077:
        problems.append("the rendered file is readable by others")
    code, output = render(env, target)
    if code or target.read_bytes() != first:
        problems.append("a rerun changed the rendered bytes")
    code, output = render(env, target, "--check")
    if code:
        problems.append(f"--check failed on fresh output: {output[-300:]}")
    target.write_text("[core]\n")
    code, output = render(env, target, "--check")
    if code == 0:
        problems.append("--check accepted a stale file")
    return problems


def rendering_refuses_a_missing_bucket(top: Path) -> Problems:
    env = top / "missing.env"
    env.write_text(SYNTHETIC_ENV.replace(f"S3_BUCKET={BUCKET}\n", ""))
    target = top / "none" / "config.local"
    code, output = render(env, target)
    problems: Problems = []
    if code == 0:
        problems.append("rendering succeeded without S3_BUCKET")
    if target.exists():
        problems.append("a file was written although the configuration is invalid")
    if "S3_BUCKET" not in output:
        problems.append("the failure does not name the setting")
    return problems


def dvc_reads_the_rendered_remote(top: Path) -> Problems:
    """DVC accepts the tracked config and the rendered local config together and resolves the default remote."""
    project = top / "project"
    project.mkdir()
    dvc = str(DVC_HOME / "bin/dvc")
    if run_command(dvc, ["init", "--no-scm", "--quiet"], cwd=project, timeout=120).returncode:
        return ["dvc init failed in the temporary project"]
    shutil.copyfile(ROOT / ".dvc/config", project / ".dvc/config")
    storage, _ = build_configuration({line.split("=")[0]: line.split("=")[1] for line in SYNTHETIC_ENV.splitlines()})
    (project / ".dvc/config.local").write_text(dvc_local_config(storage))
    listing = run_command(dvc, ["remote", "list"], cwd=project, timeout=120)
    if listing.returncode or f"s3://{BUCKET}/dvc/" not in listing.stdout:
        return [f"dvc does not list the rendered remote: {(listing.stdout + listing.stderr)[-300:]}"]
    default = run_command(dvc, ["remote", "default"], cwd=project, timeout=120)
    return [] if default.stdout.strip() == "data" else [f"the default remote is {default.stdout.strip()!r}, not 'data'"]


def main() -> int:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = ROOT / "data" / "e2e" / "dvc_setup" / stamp
    out.mkdir(parents=True)
    top = Path(tempfile.mkdtemp(prefix="dvc_e2e_"))
    results: list[dict[str, Any]] = []
    for sub in ("render", "missing", "read", "platform", "install"):
        (top / sub).mkdir()
    try:
        for name, check in (
            ("pinned_versions_and_idempotent_rerun", pinned_versions),
            ("tracked_files_hold_only_the_settings", tracked_files),
            ("local_config_cache_and_tmp_are_ignored", ignored_paths),
            ("renderer_writes_the_remote_once_and_quietly", lambda: rendered_remote(top / "render")),
            ("renderer_refuses_a_missing_bucket", lambda: rendering_refuses_a_missing_bucket(top / "missing")),
            ("dvc_reads_the_rendered_remote", lambda: dvc_reads_the_rendered_remote(top / "read")),
            ("hashes_cover_the_other_platform", lambda: other_platform_hashes(top / "platform")),
            ("failed_installs_leave_no_marker_and_the_retry_installs", lambda: failed_install_then_retry(top / "install")),
        ):
            results.append(report_case(name, check))
    finally:
        shutil.rmtree(top, ignore_errors=True)
    passed = sum(result["passed"] for result in results)
    report = out / "report.json"
    report.write_text(json.dumps({"pass": passed == len(results), "cases": results}, indent=2) + "\n")
    for result in results:
        sys.stdout.write(f"{'PASS' if result['passed'] else 'FAIL'} {result['case']}\n")
        for problem in result["problems"]:
            sys.stdout.write(f"  {problem}\n")
    sys.stdout.write(f"{passed}/{len(results)} passed; report {report}\n")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    os.umask(0o022)
    raise SystemExit(main())
