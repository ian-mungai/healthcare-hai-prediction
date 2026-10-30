"""Record a training run's exact matrix with DVC and load it back from the run (project decision 20).

``record`` runs ``dvc add`` and ``dvc push`` for the matrix file, logs the ``.dvc`` metafile as an MLflow artifact of the run and sets the metafile's
SHA-256 as a run tag beside the Iceberg snapshot IDs of the source feature tables. The tags are set last, so a run that carries the hash tag always
carries the metafile. ``load`` reads the metafile from the run, checks its hash against the tag, fetches the file with ``dvc pull`` and refuses a file whose
MD5 differs from the one in the metafile. The drift loop loads its baseline this way from the Production run.

DVC runs in explicit no-SCM mode (``core.no_scm`` in the tracked ``.dvc/config``): the matrix and its metafile live under the git-ignored ``data/`` and the
metafile is kept in the MLflow run, not in Git, so DVC must not ask Git about either file. The run store is any object with the MLflow client calls used here
(``MlflowClient`` has them). Every DVC process starts through ``run_command`` with a deadline; a failure, a deadline or a mismatch raises ``BaselineError`` that
names the step, never a silent partial result. The matrix must lie inside the DVC
project. Run scripts that import this module from the repository root as modules.
"""

from __future__ import annotations

import hashlib
import re
import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from scripts.process import SubprocessError, TimeoutExpired, run_command

ROOT = Path(__file__).resolve().parents[2]
DVC = ROOT / ".tools/dvc/bin/dvc"
ARTIFACT_FOLDER = "dvc"
HASH_TAG = "dvc_metafile_sha256"
PATH_TAG = "dvc_matrix_path"
SNAPSHOT_TAG = "iceberg_snapshot."
MD5_LINE = re.compile(r"^- md5: ([0-9a-f]{32})$", re.MULTILINE)


class BaselineError(RuntimeError):
    """A baseline step failed; the message names the step and the cause."""


class RunData(Protocol):
    @property
    def tags(self) -> Mapping[str, str]: ...


class RunRecord(Protocol):
    @property
    def data(self) -> RunData: ...


class RunStore(Protocol):
    """The MLflow client calls this module makes."""

    def log_artifact(self, run_id: str, local_path: str, artifact_path: str | None = None) -> None: ...

    def set_tag(self, run_id: str, key: str, value: str) -> None: ...

    def get_run(self, run_id: str) -> RunRecord: ...

    def download_artifacts(self, run_id: str, path: str, dst_path: str | None = None) -> str: ...


@dataclass(frozen=True)
class Run:
    """An MLflow run: the store that holds it and its ID."""

    store: RunStore
    run_id: str


@dataclass(frozen=True)
class Deadlines:
    """Seconds each DVC step may take before it is stopped."""

    add: float = 600
    push: float = 1800
    pull: float = 1800


def run_dvc(arguments: list[str], project: Path, deadline: float) -> str:
    """Run one DVC command in the project and return its output; a failure or a deadline is a BaselineError naming the step."""
    step = f"dvc {arguments[0]}"
    try:
        result = run_command(str(DVC), arguments, cwd=project, timeout=deadline)
    except TimeoutExpired:
        raise BaselineError(f"{step} passed its deadline of {deadline} seconds") from None
    except (OSError, SubprocessError) as error:
        raise BaselineError(f"{step} could not start: {error}") from error
    if result.returncode:
        raise BaselineError(f"{step} failed with exit {result.returncode}: {(result.stdout + result.stderr)[-1500:]}")
    return result.stdout


def file_digest(path: Path, algorithm: str) -> str:
    digest = hashlib.new(algorithm)
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def metafile_md5(metafile: Path) -> str:
    """The MD5 of the tracked file, from a DVC metafile that tracks exactly one file."""
    found = MD5_LINE.findall(metafile.read_text())
    if len(found) != 1:
        raise BaselineError(f"the metafile {metafile.name} must track exactly one file with an MD5; it holds {len(found)}")
    return str(found[0])


def add_and_push(matrix_path: Path, *, project: Path = ROOT, deadlines: Deadlines | None = None) -> tuple[Path, Path]:
    """Version the matrix with ``dvc add`` and push it; return its path inside the project and the path of its metafile."""
    limits = deadlines or Deadlines()
    if not matrix_path.is_file():
        raise BaselineError(f"the matrix file does not exist: {matrix_path}")
    try:
        relative = matrix_path.resolve().relative_to(project.resolve())
    except ValueError:
        raise BaselineError(f"the matrix {matrix_path.name} must lie inside the DVC project") from None
    run_dvc(["add", str(relative)], project, limits.add)
    metafile = project / f"{relative}.dvc"
    run_dvc(["push", str(relative) + ".dvc"], project, limits.push)
    return relative, metafile


def record(matrix_path: Path, run: Run, snapshots: Mapping[str, str], *, project: Path = ROOT, deadlines: Deadlines | None = None) -> str:
    """Version the matrix with DVC, push it and record the metafile and its hash in the run; return the metafile hash."""
    if not matrix_path.is_file():
        raise BaselineError(f"the matrix file does not exist: {matrix_path}")
    if not snapshots or not all(snapshots.values()):
        raise BaselineError(
            "record needs the Iceberg snapshot ID of every source feature table; a baseline without them does not say which data trained the model"
        )
    relative, metafile = add_and_push(matrix_path, project=project, deadlines=deadlines)
    return log_baseline(metafile, relative, run, snapshots)


def log_baseline(metafile: Path, relative: Path, run: Run, snapshots: Mapping[str, str]) -> str:
    """Log a recorded metafile as an artifact of the run and set the snapshot, path and hash tags; return the metafile hash.

    The tags are set last, in the order a reader depends on: a run that carries the hash tag always carries the metafile. A repeat with the same inputs
    logs the same artifact and sets the same tags.
    """
    digest = file_digest(metafile, "sha256")
    run.store.log_artifact(run.run_id, str(metafile), ARTIFACT_FOLDER)
    for table, snapshot in sorted(snapshots.items()):
        run.store.set_tag(run.run_id, SNAPSHOT_TAG + table, snapshot)
    run.store.set_tag(run.run_id, PATH_TAG, relative.as_posix())
    run.store.set_tag(run.run_id, HASH_TAG, digest)
    return digest


def load(run: Run, *, project: Path = ROOT, deadlines: Deadlines | None = None) -> Path:
    """Fetch the exact matrix a run recorded and return its path; any mismatch raises and leaves no unverified file at the matrix path."""
    limits = deadlines or Deadlines()
    tags = run.store.get_run(run.run_id).data.tags
    if HASH_TAG not in tags or PATH_TAG not in tags:
        raise BaselineError(f"run {run.run_id} has no recorded baseline: the tags {HASH_TAG} and {PATH_TAG} are not both set")
    relative = Path(tags[PATH_TAG])
    if relative.is_absolute() or ".." in relative.parts:
        raise BaselineError(f"the recorded matrix path {relative} is not a path inside the DVC project")
    scratch = Path(tempfile.mkdtemp(prefix="dvc_baseline_"))
    try:
        try:
            downloaded = Path(run.store.download_artifacts(run.run_id, f"{ARTIFACT_FOLDER}/{relative.name}.dvc", str(scratch)))
        except (OSError, ValueError, KeyError) as error:
            raise BaselineError(f"run {run.run_id} has no metafile artifact {ARTIFACT_FOLDER}/{relative.name}.dvc: {error}") from error
        if file_digest(downloaded, "sha256") != tags[HASH_TAG]:
            raise BaselineError(f"the metafile of run {run.run_id} does not match the recorded hash tag {HASH_TAG}; nothing was fetched")
        expected = metafile_md5(downloaded)
        metafile = project / f"{relative}.dvc"
        metafile.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(downloaded, metafile)
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    matrix = project / relative
    try:
        run_dvc(["pull", "--force", f"{relative}.dvc"], project, limits.pull)
    except BaselineError as error:
        raise BaselineError(f"dvc pull could not fetch the matrix of run {run.run_id}: {error}") from error
    if not matrix.is_file() or file_digest(matrix, "md5") != expected:
        matrix.unlink(missing_ok=True)
        raise BaselineError(f"the fetched matrix of run {run.run_id} does not match the MD5 in its metafile; it was removed")
    return matrix
