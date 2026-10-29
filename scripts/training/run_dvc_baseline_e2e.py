"""E2E for the DVC training baseline (decision 20): record a training matrix and load the exact file back from a run.

Run from the repository root after ``python -m scripts.quality.install_dvc``: ``python -m scripts.training.run_dvc_baseline_e2e``. Every case uses the real
pinned DVC in a throwaway Git checkout in a temporary folder (the tracked ``.dvc/config`` and ``.dvcignore`` copied in, a ``.gitignore`` that ignores
``data/`` and the matrix under ``data/``, as in the repository), a local folder as the DVC remote and a local stand-in for the MLflow run store that keeps the
MLflow file store layout (``<run>/artifacts/<path>`` and ``<run>/tags/<key>``) and the call shapes the helper uses. The matrix is a real Parquet file written
by DuckDB. Planted defects must fail visibly: a missing matrix, empty snapshot IDs, a wrong hash tag, a missing metafile, a missing baseline, an unwritable
remote, a deadline, a replaced remote object and a rerun that must change nothing.
Results go to ``data/e2e/dvc_baseline/<UTC>/report.json``.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import tempfile
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import duckdb

from scripts.process import SubprocessError, run_command
from scripts.training import dvc_baseline
from scripts.training.dvc_baseline import BaselineError, Deadlines, Run

ROOT = Path(__file__).resolve().parents[2]
DVC = str(ROOT / ".tools/dvc/bin/dvc")
SNAPSHOTS = {"gold.hai_features": "4021883701233", "silver.claims": "88120045511"}
Problems = list[str]


class LocalRunStore:
    """A stand-in for the MLflow client on a file store: the same four calls the helper makes, the same folder layout."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def run_folder(self, run_id: str) -> Path:
        return self.root / "0" / run_id

    def log_artifact(self, run_id: str, local_path: str, artifact_path: str | None = None) -> None:
        target = self.run_folder(run_id) / "artifacts" / (artifact_path or "")
        target.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, target / Path(local_path).name)

    def set_tag(self, run_id: str, key: str, value: str) -> None:
        tags = self.run_folder(run_id) / "tags"
        tags.mkdir(parents=True, exist_ok=True)
        (tags / key).write_text(value)

    def get_tags(self, run_id: str) -> Mapping[str, str]:
        tags = self.run_folder(run_id) / "tags"
        return {path.name: path.read_text() for path in tags.iterdir()} if tags.is_dir() else {}

    def get_run(self, run_id: str) -> SimpleNamespace:
        return SimpleNamespace(data=SimpleNamespace(tags=self.get_tags(run_id)))

    def download_artifacts(self, run_id: str, path: str, dst_path: str | None = None) -> str:
        source = self.run_folder(run_id) / "artifacts" / path
        if not source.is_file():
            raise FileNotFoundError(f"no artifact {path} in run {run_id}")
        target = Path(dst_path or tempfile.mkdtemp()) / Path(path).name
        shutil.copyfile(source, target)
        return str(target)


class Workspace:
    """A DVC project with a local remote, a run store and one Parquet matrix."""

    def __init__(self, top: Path) -> None:
        self.project = top / "project"
        self.remote = top / "remote"
        self.project.mkdir()
        self.remote.mkdir()
        self.store = LocalRunStore(top / "mlruns")
        self.run = Run(self.store, "run1")
        (self.project / ".dvc").mkdir()
        for name in (".dvc/config", ".dvc/.gitignore", ".dvcignore"):
            shutil.copyfile(ROOT / name, self.project / name)
        (self.project / ".gitignore").write_text("data/\n")
        for command, args in (("git", ["init", "--quiet"]), (DVC, ["remote", "add", "--local", "--default", "store", str(self.remote)])):
            if run_command(command, args, cwd=self.project, timeout=120).returncode:
                raise SubprocessError(f"{Path(command).name} {args[0]} failed in the temporary project")
        self.folder = self.project / "data" / "training" / "run1"
        self.folder.mkdir(parents=True)
        self.matrix = self.folder / "matrix.parquet"
        duckdb.sql("SELECT range AS id, range % 2 AS label FROM range(500)").write_parquet(str(self.matrix))
        self.original = hashlib.sha256(self.matrix.read_bytes()).hexdigest()

    def record(self, **options: Any) -> None:
        dvc_baseline.record(self.matrix, self.run, SNAPSHOTS, project=self.project, **options)

    def wipe_local_copies(self) -> None:
        """Remove the workspace file, its metafile and the DVC cache so that only the remote and the run keep the data."""
        self.matrix.unlink(missing_ok=True)
        (self.folder / "matrix.parquet.dvc").unlink(missing_ok=True)
        shutil.rmtree(self.project / ".dvc" / "cache", ignore_errors=True)

    def load(self) -> Path:
        return dvc_baseline.load(self.run, project=self.project)


def expect_error(action: Callable[[], object], needle: str) -> Problems:
    try:
        action()
    except BaselineError as error:
        return [] if needle in str(error) else [f"the error does not say {needle!r}: {str(error)[:300]}"]
    return [f"no BaselineError; expected one naming {needle!r}"]


def round_trip(top: Path) -> Problems:
    work = Workspace(top)
    problems: Problems = []
    for ignored in (work.matrix, work.folder / "matrix.parquet.dvc"):
        if run_command("git", ["check-ignore", "--quiet", str(ignored)], cwd=work.project, timeout=60).returncode:
            problems.append(f"{ignored.name} is not ignored by Git in the test checkout, so the case does not cover the repository layout")
    work.record()
    tags = work.store.get_tags("run1")
    for key, value in SNAPSHOTS.items():
        if tags.get(f"iceberg_snapshot.{key}") != value:
            problems.append(f"snapshot tag for {key} missing or wrong")
    metafile = work.folder / "matrix.parquet.dvc"
    if tags.get("dvc_metafile_sha256") != hashlib.sha256(metafile.read_bytes()).hexdigest():
        problems.append("the metafile hash tag does not match the metafile")
    if not (work.store.run_folder("run1") / "artifacts" / "dvc" / "matrix.parquet.dvc").is_file():
        problems.append("the metafile is not an artifact of the run")
    work.wipe_local_copies()
    loaded = work.load()
    if hashlib.sha256(loaded.read_bytes()).hexdigest() != work.original:
        problems.append("the loaded file differs from the recorded matrix")
    return problems


def idempotent_record(top: Path) -> Problems:
    work = Workspace(top)
    work.record()
    first = (dict(work.store.get_tags("run1")), (work.folder / "matrix.parquet.dvc").read_bytes())
    work.record()
    second = (dict(work.store.get_tags("run1")), (work.folder / "matrix.parquet.dvc").read_bytes())
    return [] if first == second else ["a second record changed the tags or the metafile"]


def wrong_hash_tag(top: Path) -> Problems:
    work = Workspace(top)
    work.record()
    work.store.set_tag("run1", "dvc_metafile_sha256", "0" * 64)
    work.wipe_local_copies()
    problems = expect_error(work.load, "hash")
    if work.matrix.exists():
        problems.append("a file was fetched although the hash tag did not match")
    return problems


def missing_metafile(top: Path) -> Problems:
    work = Workspace(top)
    work.record()
    shutil.rmtree(work.store.run_folder("run1") / "artifacts")
    work.wipe_local_copies()
    return expect_error(work.load, "metafile")


def missing_baseline(top: Path) -> Problems:
    work = Workspace(top)
    return expect_error(work.load, "no recorded baseline")


def missing_matrix(top: Path) -> Problems:
    work = Workspace(top)
    work.matrix.unlink()
    problems = expect_error(work.record, "matrix")
    if work.store.get_tags("run1") or (work.folder / "matrix.parquet.dvc").exists():
        problems.append("a failed record left tags or a metafile behind")
    return problems


def empty_snapshots(top: Path) -> Problems:
    work = Workspace(top)
    problems = expect_error(lambda: dvc_baseline.record(work.matrix, work.run, {}, project=work.project), "snapshot")
    if (work.folder / "matrix.parquet.dvc").exists():
        problems.append("dvc add ran although no snapshot IDs were given")
    return problems


def push_failure(top: Path) -> Problems:
    work = Workspace(top)
    work.remote.chmod(0o500)
    try:
        problems = expect_error(work.record, "dvc push")
    finally:
        work.remote.chmod(0o755)
    if work.store.get_tags("run1"):
        problems.append("tags were set although the push failed")
    return problems


def deadline(top: Path) -> Problems:
    work = Workspace(top)
    problems = expect_error(lambda: work.record(deadlines=Deadlines(add=0.05)), "deadline")
    if work.store.get_tags("run1"):
        problems.append("tags were set although the add ran out of time")
    return problems


def replaced_remote_object(top: Path) -> Problems:
    work = Workspace(top)
    work.record()
    for stored in work.remote.rglob("*"):
        if stored.is_file():
            stored.chmod(0o644)
            stored.write_bytes(b"not the matrix")
    work.wipe_local_copies()
    problems = expect_error(work.load, "matrix")
    if work.matrix.exists():
        problems.append("a file that does not match the recorded hash was left at the matrix path")
    return problems


def main() -> int:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = ROOT / "data" / "e2e" / "dvc_baseline" / stamp
    out.mkdir(parents=True)
    top = Path(tempfile.mkdtemp(prefix="dvc_baseline_e2e_"))
    results: list[dict[str, Any]] = []
    cases = (
        round_trip,
        idempotent_record,
        wrong_hash_tag,
        missing_metafile,
        missing_baseline,
        missing_matrix,
        empty_snapshots,
        push_failure,
        deadline,
        replaced_remote_object,
    )
    try:
        for check in cases:
            folder = top / check.__name__
            folder.mkdir()
            try:
                problems = check(folder)
            except (SubprocessError, OSError, ValueError, BaselineError) as error:
                problems = [f"{type(error).__name__}: {error}"]
            results.append({"case": check.__name__, "passed": not problems, "problems": problems})
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
