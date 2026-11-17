"""File-level operations on an Evidently UI workspace folder, with the standard library only.

The live check (drift image) and the restore (training image) both change the folder that the UI reads, and neither image holds the other's packages, so the
operations live here. A project is a folder ``<project id>/`` with ``metadata.json`` and ``snapshots/<snapshot id>.json``. The UI loader parses every file of a
snapshots folder, so each file is written whole through a temporary file outside that folder and a rename, and a file that is not complete JSON is set aside.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from pathlib import Path

INCOMPLETE = ".incomplete"
IDENTIFIER = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def write_bytes(target: Path, body: bytes, temp_folder: Path | None = None) -> None:
    """Write ``body`` to ``target`` through a temporary file (in ``temp_folder``, by default the target's folder) and a rename, so no reader sees half of it."""
    target.parent.mkdir(parents=True, exist_ok=True)
    handle, name = tempfile.mkstemp(dir=temp_folder or target.parent, prefix=".live_", suffix=".tmp")
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, target)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def set_aside_incomplete_snapshots(workspace: Path) -> None:
    """Move a snapshot file that is not complete JSON, as an interrupted write leaves it, out of the folder the loader reads (it parses every file there)."""
    for path in sorted(workspace.glob("*/snapshots/*.json")):
        try:
            json.loads(path.read_bytes())
        except ValueError:
            target = path.parent.parent / INCOMPLETE
            target.mkdir(exist_ok=True)
            os.replace(path, target / path.name)
            sys.stderr.write(f"live drift check: set aside the incomplete snapshot file {path.name} of {path.parent.parent.name} under {INCOMPLETE}\n")


def project_folders(workspace: Path) -> dict[str, Path]:
    """The project folders of the workspace by project name, read from the files the UI reads."""
    found: dict[str, Path] = {}
    for metadata in sorted(workspace.glob("*/metadata.json")):
        try:
            name = json.loads(metadata.read_bytes()).get("name")
        except ValueError:
            continue
        if isinstance(name, str):
            found.setdefault(name, metadata.parent)
    return found


def snapshot_of_run(folder: Path, run_id: str) -> tuple[bool, str | None]:
    """Whether the project folder holds a snapshot of ``run_id`` and the inputs identity that snapshot carries."""
    for path in sorted((folder / "snapshots").glob("*.json")):
        try:
            found = json.loads(path.read_bytes()).get("metadata", {})
        except ValueError:
            continue
        if found.get("run_id") == run_id:
            return True, found.get("inputs_sha256") or None
    return False, None


def snapshot_identity(workspace: Path, name: str, run_id: str) -> tuple[bool, str | None]:
    """Whether the project ``name`` holds a snapshot of ``run_id`` and the inputs identity that snapshot carries."""
    folder = project_folders(workspace).get(name)
    return (False, None) if folder is None else snapshot_of_run(folder, run_id)


def export_run_files(workspace: Path, name: str, run_id: str) -> tuple[bytes, bytes]:
    """The project metadata and the snapshot file of ``run_id`` as the UI reads them, which S3 keeps as the durable copy of the workspace."""
    folder = project_folders(workspace).get(name)
    if folder is not None:
        for path in sorted((folder / "snapshots").glob("*.json")):
            body = path.read_bytes()
            if json.loads(body).get("metadata", {}).get("run_id") == run_id:
                return (folder / "metadata.json").read_bytes(), body
    raise RuntimeError(f"the workspace holds no snapshot of run {run_id} in the project {name!r}")


def place_snapshot(workspace: Path, project: bytes, snapshot: bytes) -> str:
    """Put one run's snapshot into the workspace and return ``restored``, or ``present`` when a snapshot of that run is already there (the same file, or a
    snapshot of the same run under another id). The project is the workspace's own folder of that name, or a new one with the recorded id and metadata. The
    metadata is written first and the snapshot last, so a stopped restore leaves no snapshot without its project. Raises ``ValueError`` for a file that is not
    what it claims to be."""
    meta, snap = json.loads(project), json.loads(snapshot)
    name, project_id, snapshot_id = meta.get("name"), meta.get("id"), snap.get("id")
    run_id = snap.get("metadata", {}).get("run_id")
    if (
        not isinstance(name, str)
        or not isinstance(run_id, str)
        or not all(isinstance(item, str) and IDENTIFIER.fullmatch(item) for item in (project_id, snapshot_id))
    ):
        raise ValueError("a project file holds a name and a project id and a snapshot file holds a snapshot id and the run it belongs to")
    folder = project_folders(workspace).get(name) or workspace / project_id
    target = folder / "snapshots" / f"{snapshot_id}.json"
    if (target.is_file() and target.read_bytes() == snapshot) or snapshot_of_run(folder, run_id)[0]:
        return "present"
    if not (folder / "metadata.json").is_file():
        write_bytes(folder / "metadata.json", project)
    write_bytes(target, snapshot, temp_folder=folder)
    return "restored"
