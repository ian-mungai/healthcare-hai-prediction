"""E2E of the code archive that the Stage code workflow builds from a CI checkout (real Git and Git LFS, no network, no AWS).

    .venv/bin/python -m scripts.lakehouse.run_code_archive_e2e [--report data/e2e/code_archive/<stamp>/report.json]

A throwaway repository holds what the archive must carry once the tracked ingestion inputs land: ``config/acquisition/baseline_run/`` with a registry that
Git LFS stores. The runner builds the archive with the real builder (``scripts.lakehouse.code_archive``) from that repository and from a clone of it that
is checked out the way the workflow does it (``actions/checkout`` with ``lfs: true`` leaves the LFS objects in the clone; without it the clone holds
pointer files). The pinned dbt package archive comes from this repository's own pin, copied where ``dbt_package_archive fetch`` would put it.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import shutil
import sys
import tarfile
import tempfile
import traceback
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

from scripts.lakehouse import code_archive
from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
BASELINE = "config/acquisition/baseline_run"
REGISTRY = f"{BASELINE}/plan_inputs/data/acquisition_planning/registry_legacy_20260929/source_registry.json"
PLAIN_FILES = (f"{BASELINE}/inputs.lock.json", f"{BASELINE}/capture_specs.json", f"{BASELINE}/hosts_approval.json")
GIT_DATE = "2026-11-12T12:00:00+0000"
POINTER_HEAD = b"version https://git-lfs.github.com/spec/v1\n"
GIT_TIMEOUT = 120


def require(condition: bool, evidence: object = "") -> None:
    if not condition:
        raise AssertionError(evidence)


def git_environment(work: Path) -> dict[str, str]:
    """An environment that reads no global or system Git configuration, so the same cases run on any machine."""
    config = work / "gitconfig"
    config.write_text("")
    identity = {"NAME": "Fixture", "EMAIL": "fixture@example.com", "DATE": GIT_DATE}
    return {
        **os.environ,
        "GIT_CONFIG_GLOBAL": str(config),
        "GIT_CONFIG_NOSYSTEM": "1",
        **{f"GIT_{role}_{key}": value for role in ("AUTHOR", "COMMITTER") for key, value in identity.items()},
    }


@contextlib.contextmanager
def process_environment(environment: Mapping[str, str]) -> Iterator[None]:
    """The builder starts Git with the process environment, so the case sets it for the call."""
    with mock.patch.dict(os.environ, environment, clear=True):
        yield


def git(arguments: Sequence[str], cwd: Path, environment: Mapping[str, str]) -> str:
    done = run_command("git", list(arguments), cwd=cwd, env=environment, timeout=GIT_TIMEOUT)
    require(done.returncode == 0, f"git {' '.join(arguments)}: {done.stderr.strip()[-300:]}")
    return done.stdout


def registry_text() -> str:
    """A registry of more than one kilobyte, so no pointer file can be mistaken for it."""
    return json.dumps({"rows": [{"source": f"source_{index:04d}", "note": "x" * 40} for index in range(400)]}, indent=1) + "\n"


def pin_archive(destination: Path) -> None:
    """Put the pinned dbt package archive where ``dbt_package_archive fetch`` writes it (the archive is not tracked and is rebuilt from the recorded URLs)."""
    relative = json.loads((ROOT / code_archive.RECORD).read_text())["archive"]["path"]
    source = ROOT / relative
    require(source.is_file(), f"{relative} is missing: run .venv/bin/python -m scripts.lakehouse.dbt_package_archive fetch")
    (destination / relative).parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination / relative)


def make_origin(work: Path, environment: Mapping[str, str]) -> Path:
    """The repository that a local build reads: dbt project and lock, the baseline run folder, and an LFS-stored registry."""
    origin = work / "origin"
    origin.mkdir()
    git(["init", "-q", "-b", "main"], origin, environment)
    git(["lfs", "install", "--local", "--skip-repo"], origin, environment)
    (origin / ".gitattributes").write_text(f"{REGISTRY} filter=lfs diff=lfs merge=lfs -text\n")
    for name in (code_archive.LOCK, code_archive.RECORD):
        (origin / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, origin / name)
    (origin / "dbt/dbt_project.yml").write_text("name: fixture\nversion: '1.0'\n")
    for name in PLAIN_FILES:
        (origin / name).parent.mkdir(parents=True, exist_ok=True)
        (origin / name).write_text(json.dumps({"file": Path(name).name}) + "\n")
    (origin / REGISTRY).parent.mkdir(parents=True, exist_ok=True)
    (origin / REGISTRY).write_text(registry_text())
    git(["add", "-A"], origin, environment)
    git(["commit", "-q", "-m", "fixture"], origin, environment)
    stored = git(["cat-file", "-p", f"HEAD:{REGISTRY}"], origin, environment)
    require(stored.encode().startswith(POINTER_HEAD), "the fixture registry is not stored through Git LFS")
    pin_archive(origin)
    return origin


def ci_checkout(origin: Path, destination: Path, environment: Mapping[str, str], lfs: bool) -> Path:
    """A fresh clone the way the workflow checks out. With ``lfs`` the LFS objects are in the clone, as ``actions/checkout`` with ``lfs: true`` leaves them.
    Without it nothing downloads an object, so the clone holds pointer files and Git LFS passes them through."""
    git(["clone", "-q", "--no-local", str(origin), str(destination)], origin, environment)
    git(["lfs", "install", "--local", "--skip-repo", *([] if lfs else ["--skip-smudge"])], destination, environment)
    if lfs:
        shutil.copytree(origin / ".git/lfs/objects", destination / ".git/lfs/objects")
    pin_archive(destination)
    return destination


def build(root: Path, output: Path, environment: Mapping[str, str]) -> dict[str, str]:
    with process_environment(environment):
        return code_archive.build(output, "HEAD", root=root)


def members(archive: Path) -> dict[str, bytes]:
    result: dict[str, bytes] = {}
    with tarfile.open(archive) as tar:
        for member in tar.getmembers():
            if member.isfile():
                extracted = tar.extractfile(member)
                require(extracted is not None, member.name)
                if extracted is not None:
                    result[member.name] = extracted.read()
    return result


def case_a_clean_ci_checkout_builds_the_bytes_of_the_local_build(work: Path, environment: Mapping[str, str]) -> None:
    origin = make_origin(work, environment)
    local = build(origin, work / "local", environment)
    checkout = ci_checkout(origin, work / "ci", environment, lfs=True)
    ci = build(checkout, work / "ci_out", environment)
    again = build(ci_checkout(origin, work / "ci_again", environment, lfs=True), work / "ci_again_out", environment)
    require((work / "local/code.tar.gz").read_bytes() == (work / "ci_out/code.tar.gz").read_bytes(), "the CI archive differs from the local archive")
    require((work / "local/code.sha256").read_bytes() == (work / "ci_out/code.sha256").read_bytes(), "the digest files differ")
    require(local["sha256"] == ci["sha256"] == again["sha256"], (local, ci, again))
    require(hashlib.sha256((work / "ci_out/code.tar.gz").read_bytes()).hexdigest() == ci["sha256"], "the digest is not the archive's")


def case_the_archive_carries_the_baseline_run_folder_with_the_real_registry(work: Path, environment: Mapping[str, str]) -> None:
    origin = make_origin(work, environment)
    build(ci_checkout(origin, work / "ci", environment, lfs=True), work / "out", environment)
    held = members(work / "out/code.tar.gz")
    for name in PLAIN_FILES:
        require(name in held, f"{name} is not in the archive")
    require(held[REGISTRY] == registry_text().encode(), "the registry in the archive is not the real file")
    require(not held[REGISTRY].startswith(POINTER_HEAD), "the registry in the archive is a pointer file")


def case_a_checkout_without_lfs_objects_is_refused_before_anything_is_written(work: Path, environment: Mapping[str, str]) -> None:
    origin = make_origin(work, environment)
    checkout = ci_checkout(origin, work / "ci", environment, lfs=False)
    try:
        build(checkout, work / "out", environment)
    except code_archive.ArchiveError as error:
        require("LFS pointer" in str(error) and REGISTRY in str(error), str(error))
    else:
        raise AssertionError("an archive that holds a Git LFS pointer file was built")
    require(not (work / "out").exists(), "a refused build left its output folder")


def case_the_check_refuses_an_archive_that_holds_a_pointer_file(work: Path, environment: Mapping[str, str]) -> None:
    origin = make_origin(work, environment)
    build(ci_checkout(origin, work / "ci", environment, lfs=True), work / "out", environment)
    pointer = POINTER_HEAD + b"oid sha256:" + b"0" * 64 + b"\nsize 4001\n"
    swapped = work / "swapped.tar"
    with tarfile.open(work / "out/code.tar.gz") as source, tarfile.open(swapped, "w", format=tarfile.PAX_FORMAT, pax_headers=source.pax_headers) as target:
        for member in source.getmembers():
            if member.name == REGISTRY:
                member.size = len(pointer)
                target.addfile(member, io.BytesIO(pointer))
            else:
                target.addfile(member, source.extractfile(member) if member.isfile() else None)
    digest = hashlib.sha256(swapped.read_bytes()).hexdigest()
    try:
        code_archive.check(swapped, expected=digest)
    except code_archive.ArchiveError as error:
        require("LFS pointer" in str(error) and REGISTRY in str(error), str(error))
    else:
        raise AssertionError("the check accepted an archive that holds a Git LFS pointer file")


CASES: dict[str, Callable[[Path, Mapping[str, str]], None]] = {
    "a clean CI checkout builds the bytes of the local build": case_a_clean_ci_checkout_builds_the_bytes_of_the_local_build,
    "the archive carries the baseline run folder with the real registry": case_the_archive_carries_the_baseline_run_folder_with_the_real_registry,
    "a checkout without LFS objects is refused before anything is written": case_a_checkout_without_lfs_objects_is_refused_before_anything_is_written,
    "the check refuses an archive that holds a pointer file": case_the_check_refuses_an_archive_that_holds_a_pointer_file,
}


def run_all(only: Sequence[str]) -> list[dict[str, str]]:
    results = []
    for name, case in CASES.items():
        if only and not any(text in name for text in only):
            continue
        with tempfile.TemporaryDirectory(prefix="code_archive_e2e_") as scratch:
            work = Path(scratch).resolve()  # the package installer refuses a path that holds a link, and macOS temporary folders sit behind one
            try:
                case(work, git_environment(work))
                outcome, detail = "PASS", ""
            except (AssertionError, ValueError, OSError):
                outcome, detail = "FAIL", traceback.format_exc(limit=3)[-600:]
        sys.stdout.write(f"{outcome}: {name}\n")
        if detail:
            sys.stderr.write(detail + "\n")
        results.append({"case": name, "outcome": outcome, "detail": detail})
    return results


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--report", type=Path)
    parser.add_argument("--only", action="append", default=[], help="run the cases whose name contains this text")
    args = parser.parse_args(argv)
    results = run_all(args.only)
    failed = [item for item in results if item["outcome"] != "PASS"]
    sys.stdout.write(f"{len(results) - len(failed)} of {len(results)} cases passed\n")
    if not results:
        sys.stderr.write(f"no case name contains {' or '.join(args.only)}\n")
        return 1
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps({"at": datetime.now(UTC).isoformat(), "results": results}, indent=2) + "\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
