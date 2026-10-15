"""Provenance of a real dbt build and the sealed build file (D1 section 10, failure modes 920 to 924).

Host side, used by the launcher through ``provenance_vars(run)`` and ``seal(run)``: it computes the provenance values of a run from the
launcher's registry records (the run entry, its snapshot copy record and its recorded pins), reading and writing run files only through held
directory descriptors, each component opened with ``O_NOFOLLOW``. Every value is recorded or the call raises ``ProvenanceError``; there is no
``unrecorded`` fallback. ``seal_build`` makes the sealed copy of the full build that the launcher names as the attempt that wrote the build file and
returns the ``completion`` record. The copy is made by this process from the held build file into the held sealed folder: no process names a path.

Container side, ``python -m scripts.lakehouse.provenance consume``: the fixed consumer entrypoint. It hashes the mounted sealed file before any
consumer opens it, reports the digest it computed and exits non-zero when that digest is not the one in ``completion``. Every reader of a sealed copy
runs behind it in the same container, after it exits 0.

Only the standard library is imported at module level, so the container side needs no other package.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import hashlib
import importlib
import io
import json
import os
import re
import stat
import sys
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

VARIABLE_RUN_ID = "HAI_BUILD_RUN_ID"
VARIABLE_TREE = "HAI_BUILD_DBT_TREE_SHA256"
VARIABLE_PINS_SHA = "HAI_BUILD_PINS_SHA256"
VARIABLE_PINS_JSON = "HAI_BUILD_PINS_JSON"
VARIABLE_REVISION = "HAI_BUILD_GIT_REVISION"
VARIABLE_REQUIRED = "HAI_PROVENANCE_REQUIRED"
VARIABLE_SEALED_SHA = "HAI_SEALED_SHA256"
VARIABLE_SEALED_EXPECTED = "HAI_SEALED_EXPECTED"
SEALED_RUN_PARAM = "sealed_run"
RESULT_PREFIX = "HAI_SEALED_RESULT "

SHA256 = re.compile(r"[0-9a-f]{64}")
REVISION = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
BUILD_FILE = "staging.duckdb"
SEALED_FOLDER = "sealed"
ARTIFACTS = ("run_results.json", "manifest.json")
MARKER_COLUMNS = ("dbt_invocation_id", "git_revision", "run_id", "dbt_tree_sha256", "bronze_pins_sha256", "pins_json")
MARKER_QUERY = "select dbt_invocation_id, git_revision, run_id, dbt_tree_sha256, bronze_pins_sha256, pins_json from int_silver_build_marker"
# The dbt commands by what they do to the build file (D1 section 10 items 4 and 5). The hooks drop the marker for exactly the first group, whether or not
# dbt runs them for a command (tested for every command and form in ``hook_command_coverage``); the commands of the other groups run no model SQL and
# change no relation. ``dbt/macros/build_marker_hooks.sql`` (hai_marker_untouched_commands) lists CONNECTING and INERT: an unselected ``compile`` runs the
# hooks, which then leave the marker alone.
HOOKED = ("build", "run", "seed", "snapshot", "test")
OPERATION = ("run-operation",)  # changes the build without a hook; the launcher admits no macro for it (RUN_OPERATION_ALLOWLIST is empty)
CONNECTING = ("compile", "debug")  # may open the build file, so they wait for a writer, but they change nothing
INERT = ("ls", "parse")  # never open the build file
SELECTION_OPTIONS = ("--select", "-s", "--exclude", "--selector")
PASSING = ("success", "pass")
READ_LIMIT = 1 << 20
RESULTS_LIMIT = 1 << 30
COPY_CHUNK = 1 << 20
DIRECTORY_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY


class ProvenanceError(Exception):
    """A provenance value is missing, malformed or not the recorded one, or a build cannot be sealed. The launcher reports it with the code ``provenance``."""


def registry_folder(run: Mapping[str, Any]) -> Path:
    """The registry folder the run entry names; the run entry is the launcher's record, not a client's."""
    value = run.get("registry_path")
    if not isinstance(value, str) or not value or not Path(value).is_absolute():
        raise ProvenanceError("the run entry names no registry folder")
    return Path(value)


def runs_root(run: Mapping[str, Any]) -> Path:
    """The folder that holds ``<run_id>/``: a test registry's ``runs_root`` setting (default ``test_state/runs``) or the real ``data/analytics/dbt/runs``."""
    folder = registry_folder(run)
    try:
        identity = json.loads((folder / "registry_id").read_text())
        kind, registry_id = str(identity["kind"]), str(identity["registry_id"])
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise ProvenanceError(f"the registry identity of run {run.get('run_id')} cannot be read: {type(error).__name__}") from error
    if run.get("registry_id") not in (None, registry_id):
        raise ProvenanceError("the run entry belongs to another registry")
    if kind == "test":
        settings: dict[str, Any] = {}
        settings_file = folder / "launcher_test.json"
        if settings_file.is_file():
            try:
                settings = json.loads(settings_file.read_text())
            except (OSError, ValueError) as error:
                raise ProvenanceError("the test registry settings cannot be read") from error
        return Path(str(settings.get("runs_root") or folder / "test_state" / "runs"))
    return folder.parent.parent / "analytics" / "dbt" / "runs"


def open_directory(root: Path, parts: Sequence[str]) -> int:
    """A descriptor of ``<root>/<parts...>``: ``root`` is opened by name, every part relative to the descriptor of the part before it with ``O_NOFOLLOW``.

    A link in any part, also one put there after an earlier check, is refused (D1 section 10 item 2). The caller closes the descriptor.
    """
    if any(part in ("", ".", "..") or "/" in part for part in parts):
        raise ProvenanceError("not a plain folder name")
    try:
        descriptor = os.open(root, DIRECTORY_FLAGS)
    except OSError as error:
        raise ProvenanceError(f"{root} cannot be opened without following links: {error.strerror or type(error).__name__}") from error
    try:
        for component in parts:
            following = os.open(component, DIRECTORY_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = following
    except OSError as error:
        os.close(descriptor)
        raise ProvenanceError(f"{'/'.join(parts)} cannot be opened without following links: {error.strerror or type(error).__name__}") from error
    return descriptor


@contextlib.contextmanager
def held(root: Path, parts: Sequence[str]) -> Iterator[int]:
    """``open_directory`` for the length of a block."""
    descriptor = open_directory(root, parts)
    try:
        yield descriptor
    finally:
        os.close(descriptor)


def open_file(directory: int, name: str, what: str) -> int:
    """A read-only descriptor of the regular file ``name`` in a held directory, opened with ``O_NOFOLLOW``; ``what`` names it in a refusal."""
    try:
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
    except OSError as error:
        raise ProvenanceError(f"{what} cannot be opened without following links: {error.strerror or type(error).__name__}") from error
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise ProvenanceError(f"{what} is not a regular file")
    return descriptor


def exists_below(directory: int, name: str) -> bool:
    """True when ``name`` exists in a held directory, a link included (it is not followed)."""
    try:
        os.stat(name, dir_fd=directory, follow_symlinks=False)
    except FileNotFoundError:
        return False
    except OSError as error:
        raise ProvenanceError(f"{name} cannot be inspected: {error.strerror or type(error).__name__}") from error
    return True


def read_below(directory: int, parts: Sequence[str], limit: int = READ_LIMIT) -> bytes:
    """Read the regular file ``parts`` below a held directory, every component opened relative to the one before with ``O_NOFOLLOW``."""
    if not parts:
        raise ProvenanceError("not a plain run file")
    label = "/".join(parts)
    with held_below(directory, parts[:-1]) as parent:
        descriptor = open_file(parent, parts[-1], label)
    try:
        data = os.read(descriptor, limit + 1)
    finally:
        os.close(descriptor)
    if len(data) > limit:
        raise ProvenanceError(f"{label} is larger than {limit} bytes")
    return data


@contextlib.contextmanager
def held_below(directory: int, parts: Sequence[str]) -> Iterator[int]:
    """A descriptor of ``parts`` below a held directory (the held directory itself stays open)."""
    if any(part in ("", ".", "..") or "/" in part for part in parts):
        raise ProvenanceError("not a plain folder name")
    descriptor = os.dup(directory)
    try:
        for component in parts:
            try:
                following = os.open(component, DIRECTORY_FLAGS, dir_fd=descriptor)
            except OSError as error:
                raise ProvenanceError(f"{component} cannot be opened without following links: {error.strerror or type(error).__name__}") from error
            os.close(descriptor)
            descriptor = following
        yield descriptor
    finally:
        os.close(descriptor)


def read_run_file(root: Path, run_id: str, parts: Sequence[str], limit: int = READ_LIMIT) -> bytes:
    """Read ``<root>/<run_id>/<parts...>`` opening each component with ``O_NOFOLLOW`` (D1 section 10 item 2); a link or a non-file is refused."""
    if not RUN_ID.fullmatch(run_id) or not parts:
        raise ProvenanceError("not a plain run file")
    with held(root, (run_id,)) as folder:
        try:
            return read_below(folder, parts, limit)
        except ProvenanceError as error:
            raise ProvenanceError(f"{error} (run {run_id})") from error


def file_digest(descriptor: int) -> tuple[str, int]:
    """SHA-256 and size of the open file, read from its start."""
    digest = hashlib.sha256()
    size = 0
    offset = 0
    while chunk := os.pread(descriptor, 1 << 20, offset):
        digest.update(chunk)
        size += len(chunk)
        offset += len(chunk)
    return digest.hexdigest(), size


def tree_entries(directory: int, prefix: tuple[str, ...]) -> Iterator[tuple[tuple[str, ...], bytes]]:
    """The files below a held directory as (path parts, SHA-256 of the content): each folder and file is opened relative to a held descriptor."""
    with os.scandir(directory) as scanned:
        listed = [(item.name, item.is_symlink(), item.is_dir(follow_symlinks=False)) for item in scanned]
    for name, is_link, is_folder in listed:
        shown = "/".join((*prefix, name))
        if is_link:
            raise ProvenanceError(f"the snapshot holds a link: {shown}")
        if is_folder:
            try:
                child = os.open(name, DIRECTORY_FLAGS, dir_fd=directory)
            except OSError as error:
                raise ProvenanceError(f"the snapshot folder {shown} cannot be opened without following links") from error
            try:
                yield from tree_entries(child, (*prefix, name))
            finally:
                os.close(child)
            continue
        descriptor = open_file(directory, name, f"the snapshot file {shown}")
        try:
            yield (*prefix, name), bytes.fromhex(file_digest(descriptor)[0])
        finally:
            os.close(descriptor)


def tree_digest_at(directory: int) -> str:
    """The launcher's ``dbt_tree_sha256`` of the snapshot copy a held descriptor names, computed again from the files: the same walk and bytes as the copy
    step, every folder and file opened relative to a held descriptor, links refused."""
    digest = hashlib.sha256()
    for label in ("project", "packages"):
        try:
            top = os.open(label, DIRECTORY_FLAGS, dir_fd=directory)
        except OSError as error:
            raise ProvenanceError(f"the snapshot has no plain {label} folder") from error
        try:
            entries = sorted(tree_entries(top, (label,)))
        finally:
            os.close(top)
        for parts, content in entries:
            digest.update("/".join(parts).encode() + b"\0" + content)
    return digest.hexdigest()


def tree_digest(folder: Path) -> str:
    """``tree_digest_at`` for a snapshot folder named by path (its last component is not followed if it is a link)."""
    with held(folder, ()) as directory:
        return tree_digest_at(directory)


def command_of(args: Sequence[str]) -> str:
    """The dbt command of a launch's arguments."""
    return str(args[0]) if args else ""


def is_mutating(args: Sequence[str]) -> bool:
    """True for a dbt launch that changes the build file: a task whose hooks drop the marker. A sealed run refuses it and it clears the completion."""
    return command_of(args) in (*HOOKED, *OPERATION)


def is_serialized(args: Sequence[str]) -> bool:
    """True for a dbt launch that may open the build file, so it goes through the run's write queue: every command but ``ls`` and ``parse``."""
    return bool(args) and command_of(args) not in INERT


def is_full_build(args: Sequence[str]) -> bool:
    """True for a ``build`` without a selection: the only launch a seal may certify."""
    return command_of(args) == "build" and not any(str(token).split("=", 1)[0] in SELECTION_OPTIONS for token in args[1:])


@dataclass(frozen=True)
class Recorded:
    """The provenance values of a run, each read from a launcher record and checked again against the files."""

    run_id: str
    dbt_tree_sha256: str
    bronze_pins_sha256: str
    pins_json: str
    git_revision: str


def copy_record(run: Mapping[str, Any]) -> Mapping[str, Any]:
    """The run's complete snapshot copy record, from the registry file (replaced atomically, so an unlocked read sees a committed state)."""
    pointer = run.get("snapshot")
    if not isinstance(pointer, str) or not pointer:
        raise ProvenanceError(f"run {run.get('run_id')} has no complete snapshot")
    try:
        state = json.loads((registry_folder(run) / "registry.json").read_text())
        copy = state["copies"][pointer]
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise ProvenanceError(f"the snapshot copy {pointer} has no record: {type(error).__name__}") from error
    if copy.get("run") != run.get("run_id") or copy.get("phase") != "complete" or not SHA256.fullmatch(str(copy.get("dbt_tree_sha256", ""))):
        raise ProvenanceError(f"the snapshot copy {pointer} is not a complete copy of run {run.get('run_id')} with a recorded hash")
    return dict(copy)


def git_revision(checkout: str) -> str:
    """The commit a checkout's ``HEAD`` names, read from the Git files without starting a process (a worktree's ``.git`` file and a packed ref included)."""
    root = Path(checkout)
    git = root / ".git"
    try:
        if git.is_file():
            gitdir = Path(git.read_text().split("gitdir:", 1)[1].strip())
            gitdir = gitdir if gitdir.is_absolute() else root / gitdir
        else:
            gitdir = git
        head = (gitdir / "HEAD").read_text().strip()
        common = gitdir
        if (gitdir / "commondir").is_file():
            common = gitdir / (gitdir / "commondir").read_text().strip()
        revision = head
        if head.startswith("ref:"):
            ref = head[4:].strip()
            revision = ""
            for base in (gitdir, common):
                if (base / ref).is_file():
                    revision = (base / ref).read_text().strip()
                    break
            if not revision:
                for line in (common / "packed-refs").read_text().splitlines():
                    if line.endswith(f" {ref}") and not line.startswith(("#", "^")):
                        revision = line.split(" ", 1)[0]
    except (OSError, IndexError, ValueError) as error:
        raise ProvenanceError(f"the Git revision of {checkout} cannot be read: {type(error).__name__}") from error
    if not REVISION.fullmatch(revision):
        raise ProvenanceError(f"the Git revision of {checkout} cannot be read")
    return revision


def recorded(run: Mapping[str, Any]) -> Recorded:
    """Every provenance value of a real transform run, or ``ProvenanceError`` (D1 section 10 items 1 to 3 and 8).

    The tree hash is computed again from the snapshot files and must equal the copy record's; the pins file is read through a safe read and its
    digest must equal the recorded one. Any missing or unrecorded value refuses; nothing is replaced by a default.
    """
    run_id = str(run.get("run_id") or "")
    if not RUN_ID.fullmatch(run_id):
        raise ProvenanceError("the run entry has no plain run ID")
    if run.get("mode") != "real" or run.get("domain") != "transform":
        raise ProvenanceError(f"run {run_id} is not a real transform run")
    root = runs_root(run)
    copy = copy_record(run)
    with held(root, (run_id, "snapshots", str(run["snapshot"]))) as snapshot:
        tree = tree_digest_at(snapshot)
    if tree != copy["dbt_tree_sha256"]:
        raise ProvenanceError(f"the snapshot of run {run_id} changed after it was copied")
    pins_sha = run.get("pins_sha256")
    if not isinstance(pins_sha, str) or not SHA256.fullmatch(pins_sha):
        raise ProvenanceError(f"run {run_id} has no recorded pins")
    data = read_run_file(root, run_id, ("pins.json",))
    if hashlib.sha256(data).hexdigest() != pins_sha:
        raise ProvenanceError(f"pins.json of run {run_id} differs from the recorded digest")
    try:
        text = data.decode("utf-8")
        document = json.loads(text)
    except ValueError as error:
        raise ProvenanceError(f"pins.json of run {run_id} is not valid JSON") from error
    if not isinstance(document, dict) or not document or not all(isinstance(k, str) and isinstance(v, str) and v.isdigit() for k, v in document.items()):
        raise ProvenanceError(f"pins.json of run {run_id} does not map tables to snapshot IDs")
    return Recorded(run_id, tree, pins_sha, text, git_revision(str(run.get("checkout") or "")))


def variables(run: Mapping[str, Any]) -> dict[str, str]:
    """The typed environment of a ``real_dbt`` launch (D1 section 7 item 2); the launcher adds nothing else and a caller cannot set any of it."""
    values = recorded(run)
    return {
        VARIABLE_RUN_ID: values.run_id,
        VARIABLE_TREE: values.dbt_tree_sha256,
        VARIABLE_PINS_SHA: values.bronze_pins_sha256,
        VARIABLE_PINS_JSON: values.pins_json,
        VARIABLE_REVISION: values.git_revision,
        VARIABLE_REQUIRED: "1",
    }


def dispatch_blocker(run: Mapping[str, Any], attempt: Mapping[str, Any], attempts: Iterable[Mapping[str, Any]]) -> str | None:
    """None when this queued serialized ``real_dbt`` attempt may be created now; ``"sealed"`` when the run is sealed and the attempt is mutating (it ends);
    else why it waits (a seal in progress may still fail, which reopens the run).

    One serialized launch of a run is open at a time, and queued launches go in the order they were submitted.
    """
    if run.get("build_state") == "sealed" and is_mutating(attempt["args"]):
        return "sealed"
    if run.get("build_state") == "sealing":
        return "a seal of the run is in progress"
    for other in attempts:
        if other["id"] == attempt["id"] or other["owner"] != attempt["owner"] or other["template"] != "real_dbt" or other["phase"] == "ended":
            continue
        if not is_serialized(other["args"]):
            continue
        if other["phase"] != "queued":
            return "a launch of the run that may write the build file is open"
        if (other["created_at"], other["id"]) < (attempt["created_at"], attempt["id"]):
            return "an earlier launch of the run that may write the build file is queued"
    return None


def binding(attempt: Mapping[str, Any]) -> dict[str, Any]:
    """What the launcher records of the attempt whose end starts a seal: the attempt, the run generation it ran in and its arguments."""
    return {"attempt": str(attempt["id"]), "generation": int(attempt["generation"]), "args": [str(token) for token in attempt["args"]]}


def check_binding(run: Mapping[str, Any]) -> Mapping[str, Any]:
    """The binding of the attempt that wrote the build, or ``ProvenanceError`` unless it is a full build of the run's current generation."""
    run_id = run.get("run_id")
    found = run.get("build_binding")
    if not isinstance(found, Mapping) or not found.get("attempt") or found.get("attempt") != run.get("build_attempt"):
        raise ProvenanceError(f"run {run_id} names no build attempt to seal")
    args = found.get("args")
    if not isinstance(args, list) or not is_full_build(args):
        what = "a build with a selection" if isinstance(args, list) and command_of(args) == "build" else f"`{command_of(args or [])}`, not a build"
        raise ProvenanceError(f"the last launch of run {run_id} (attempt {found['attempt']}) was {what} and cannot be sealed")
    if found.get("generation") != int(run.get("generation", 1)):
        raise ProvenanceError(f"the build of run {run_id} ran in generation {found.get('generation')}, the run is in generation {run.get('generation', 1)}")
    return found


def clear_build_artifacts(run: Mapping[str, Any]) -> None:
    """Remove ``run_results.json`` and ``manifest.json`` of the run before a mutating launch starts.

    A seal reads these two files, so after this call only what the launch writes can be sealed; an older build's files cannot be certified again.
    """
    run_id = str(run.get("run_id") or "")
    if not RUN_ID.fullmatch(run_id):
        raise ProvenanceError("the run entry has no plain run ID")
    with held(runs_root(run), (run_id,)) as folder:
        try:
            target = os.open("target", DIRECTORY_FLAGS, dir_fd=folder)
        except FileNotFoundError:
            return
        except OSError as error:
            raise ProvenanceError(f"the target folder of run {run_id} cannot be opened without following links") from error
        try:
            for name in ARTIFACTS:
                try:
                    os.unlink(name, dir_fd=target)
                except FileNotFoundError:
                    continue
                except OSError as error:
                    raise ProvenanceError(f"{name} of run {run_id} cannot be removed: {error.strerror or type(error).__name__}") from error
        finally:
            os.close(target)


def gold_eligible_invocation(folder: int, run_id: str) -> str:
    """The invocation ID of a build that may be sealed: a ``build`` without a selection whose results are all ``success`` or ``pass``.

    ``run_results.json`` and ``manifest.json`` (read below the held run folder) must name the same invocation (L1 review round 1, finding R5).
    """
    try:
        results = json.loads(read_below(folder, ("target", "run_results.json"), RESULTS_LIMIT))
        manifest = json.loads(read_below(folder, ("target", "manifest.json"), RESULTS_LIMIT))
    except ValueError as error:
        raise ProvenanceError(f"the dbt artifacts of run {run_id} are not valid JSON") from error
    args = results.get("args") or {}
    if args.get("which") != "build":
        raise ProvenanceError(f"the last launch of run {run_id} was not a build")
    for option in ("select", "exclude", "selector", "resource_types", "exclude_resource_types"):
        if args.get(option):
            raise ProvenanceError(f"the last build of run {run_id} had a selection ({option}) and cannot be sealed")
    if args.get("empty"):
        raise ProvenanceError(f"the last build of run {run_id} used --empty and cannot be sealed")
    entries = results.get("results")
    if not isinstance(entries, list) or not entries:
        raise ProvenanceError(f"the last build of run {run_id} has no results")
    bad = {str(entry.get("status")) for entry in entries if str(entry.get("status")) not in PASSING}
    if bad:
        raise ProvenanceError(f"the last build of run {run_id} has results that are not success or pass: {', '.join(sorted(bad))}")
    invocation = str((results.get("metadata") or {}).get("invocation_id") or "")
    if not invocation or invocation != str((manifest.get("metadata") or {}).get("invocation_id") or ""):
        raise ProvenanceError(f"the results and the manifest of run {run_id} name different invocations")
    return invocation


def copy_into(source: int, sealed: int, name: str) -> int:
    """Copy the held build file into a new file ``name`` of the held sealed folder; returns a read-write descriptor of the copy.

    Both ends are descriptors this process holds, and ``name`` is a single component opened relative to the sealed folder with ``O_EXCL`` and
    ``O_NOFOLLOW``: no path is resolved after the descriptors are held, so a folder replaced by a link cannot redirect the read or the write (review
    delta 1, finding 4). The copy is a plain byte copy with its own inode; a failed copy is removed.
    """
    descriptor = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=sealed)
    try:
        offset = 0
        while chunk := os.pread(source, COPY_CHUNK, offset):
            view = memoryview(chunk)
            written = 0
            while written < len(view):
                written += os.pwrite(descriptor, view[written:], offset + written)
            offset += len(chunk)
        os.fsync(descriptor)
    except BaseException:
        os.close(descriptor)
        with contextlib.suppress(OSError):
            os.unlink(name, dir_fd=sealed)
        raise
    return descriptor


def sealed_directory(folder: int, run_id: str) -> int:
    """A descriptor of ``sealed/`` in the held run folder, made if absent and opened for writing (mode 700) without following a link."""
    try:
        os.mkdir(SEALED_FOLDER, 0o700, dir_fd=folder)
    except FileExistsError:
        pass
    except OSError as error:
        raise ProvenanceError(f"the sealed folder of run {run_id} cannot be made: {error.strerror or type(error).__name__}") from error
    try:
        descriptor = os.open(SEALED_FOLDER, DIRECTORY_FLAGS, dir_fd=folder)
    except OSError as error:
        raise ProvenanceError(f"the sealed folder of run {run_id} is not a plain folder") from error
    os.fchmod(descriptor, 0o700)
    return descriptor


def drop_interrupted_copies(sealed: int) -> None:
    """Remove the temporary copies an interrupted seal left in the held sealed folder (names ``.<hex>.clone``); a seal runs alone for its run."""
    with os.scandir(sealed) as scanned:
        names = [item.name for item in scanned if item.name.startswith(".") and item.name.endswith(".clone")]
    for name in names:
        os.unlink(name, dir_fd=sealed)


def seal_build(run: Mapping[str, Any]) -> dict[str, Any]:
    """Make the sealed copy of the full build that wrote the build file of ``run`` and return its ``completion`` record (D1 section 10 item 5).

    Only the attempt the launcher bound to the run (``build_binding``) is sealed: a full ``build`` of the run's current generation, after
    ``clear_build_artifacts`` removed every older result. The copy is ``runs/<run_id>/sealed/<sha256>.duckdb``, a byte copy of the build file with its
    own inode made through descriptors (``copy_into``); its bytes must equal the build file the seal checked through its own descriptor. The file and its
    folder are made read-only. Sealing the same bytes again returns the same record. Any other launch, a build file with a write-ahead log, a run whose inputs
    changed after the build or a link anywhere on the path refuses. Every file is reached through descriptors held from the runs root.
    """
    values = recorded(run)
    bound = check_binding(run)
    root = runs_root(run)
    run_id = values.run_id
    with held(root, (run_id,)) as folder:
        invocation = gold_eligible_invocation(folder, run_id)
        source = open_file(folder, BUILD_FILE, f"the build file of run {run_id}")
        try:
            if exists_below(folder, f"{BUILD_FILE}.wal"):
                raise ProvenanceError(f"the build file of run {run_id} has a write-ahead log; a clone would miss it")
            sealed = sealed_directory(folder, run_id)
            try:
                drop_interrupted_copies(sealed)
                final, sha256, size = clone_into(source, sealed, run_id)
                info = os.fstat(final)
                os.close(final)
                os.fsync(sealed)
            finally:
                with contextlib.suppress(OSError):
                    os.fchmod(sealed, 0o555)
                os.close(sealed)
        finally:
            os.close(source)
    return {
        "sha256": sha256,
        "size": size,
        "inode": info.st_ino,
        "device": info.st_dev,
        "file": f"{SEALED_FOLDER}/{sha256}.duckdb",
        "invocation_id": invocation,
        "build_attempt": bound["attempt"],
        "build_generation": bound["generation"],
        "run_id": run_id,
        "dbt_tree_sha256": values.dbt_tree_sha256,
        "bronze_pins_sha256": values.bronze_pins_sha256,
        "git_revision": values.git_revision,
        "copy_method": "descriptor copy",
        "sealed_utc": datetime.now(UTC).isoformat(),
    }


def clone_into(source: int, sealed: int, run_id: str) -> tuple[int, str, int]:
    """Copy the build file into the held sealed folder; returns a descriptor of the read-only sealed copy, its SHA-256 and its size.

    The copy is made by ``copy_into`` from the held build file into the held folder, and accepted only when it holds the bytes of the build file as it
    is when the copy ends.
    """
    temporary = f".{uuid.uuid4().hex}.clone"
    clone = -1
    try:
        clone = copy_into(source, sealed, temporary)
        sha256, size = file_digest(clone)
        if (sha256, size) != file_digest(source):
            raise ProvenanceError(f"the copy of run {run_id} does not hold the bytes of the build file that was checked")
        final = f"{sha256}.duckdb"
        if exists_below(sealed, final):
            existing = open_file(sealed, final, f"the sealed copy {final} of run {run_id}")
            try:
                if file_digest(existing)[0] != sha256:
                    raise ProvenanceError(f"the sealed copy {final} of run {run_id} does not hold the bytes its name says")
            finally:
                os.close(existing)
            os.unlink(temporary, dir_fd=sealed)
        else:
            os.fchmod(clone, 0o444)
            os.rename(temporary, final, src_dir_fd=sealed, dst_dir_fd=sealed)
        result = open_file(sealed, final, f"the sealed copy {final} of run {run_id}")
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temporary, dir_fd=sealed)
        raise
    finally:
        if clone >= 0:
            os.close(clone)
    os.fchmod(result, 0o444)
    return result, sha256, size


def check_completion(run: Mapping[str, Any]) -> Mapping[str, Any]:
    """The ``completion`` of a sealed run, only if it is the one of the run's live build: the attempt and the generation the run's binding names, and the
    run's current generation (a recovery that moved the run on leaves an older completion that no reader may use; review delta 1, finding 8)."""
    run_id = run.get("run_id")
    completion = run.get("completion")
    if run.get("build_state") != "sealed" or not isinstance(completion, Mapping):
        raise ProvenanceError(f"run {run_id} is not sealed")
    found = run.get("build_binding")
    generation = int(run.get("generation", 1))
    if (
        not isinstance(found, Mapping)
        or completion.get("run_id") != run_id
        or completion.get("build_attempt") != found.get("attempt")
        or completion.get("build_attempt") != run.get("build_attempt")
        or completion.get("build_generation") != found.get("generation")
        or completion.get("build_generation") != generation
    ):
        raise ProvenanceError(
            f"the completion of run {run_id} (attempt {completion.get('build_attempt')}, generation {completion.get('build_generation')}) "
            f"is not the one of the live build (generation {generation})"
        )
    return completion


def check_sealed_file(run: Mapping[str, Any]) -> None:
    """A sealed run's copy exists as a regular file of the recorded size, and its completion is the live build's (no bytes are read: every reader hashes the
    object it attaches)."""
    completion = check_completion(run)
    run_id = str(run["run_id"])
    with held(runs_root(run), (run_id, SEALED_FOLDER)) as folder:
        descriptor = open_file(folder, f"{completion['sha256']}.duckdb", f"the sealed copy of run {run_id}")
        try:
            size = os.fstat(descriptor).st_size
        finally:
            os.close(descriptor)
    if size != completion["size"]:
        raise ProvenanceError(f"the sealed copy of run {run_id} has {size} bytes, the completion records {completion['size']}")


def consumer_variables(run: Mapping[str, Any]) -> dict[str, str]:
    """The environment of a consumer container: the sealed digest and what the marker row must say.

    The Git revision is not compared: it is read from the checkout, which may have moved on during a long build; the tree hash is what binds the code.
    The consumer still requires a real revision in the marker, never ``unrecorded``.
    """
    check_sealed_file(run)
    completion = run["completion"]
    expected = {
        "run_id": completion["run_id"],
        "dbt_invocation_id": completion["invocation_id"],
        "dbt_tree_sha256": completion["dbt_tree_sha256"],
        "bronze_pins_sha256": completion["bronze_pins_sha256"],
    }
    return {VARIABLE_SEALED_SHA: str(completion["sha256"]), VARIABLE_SEALED_EXPECTED: json.dumps(expected, sort_keys=True, separators=(",", ":"))}


def accepts_result(result: Mapping[str, Any], completion: Mapping[str, Any]) -> bool:
    """True when a consumer's result carries the digest that ``completion`` records, was computed inside the container and found the marker right."""
    marker = result.get("marker") or {}
    return (
        result.get("matched") is True
        and result.get("sha256") == completion.get("sha256")
        and result.get("expected_sha256") == completion.get("sha256")
        and result.get("size") == completion.get("size")
        and marker.get("matched") is True
    )


def request_snapshot(client: Any, principal: Mapping[str, Any], run_id: str, checkout: Path, packages_host: Path, timeout: float = 600.0) -> str:
    """Ask the launcher for the run's snapshot and wait until the copy is complete; returns the copy ID. The request is idempotent, so it is repeated."""
    deadline = time.monotonic() + timeout
    while True:
        reply = client.call("snapshot", {"checkout": str(checkout), "packages_host": str(packages_host)}, principal)
        if reply.get("state") == "complete":
            return str(reply["copy_id"])
        if time.monotonic() > deadline:
            raise ProvenanceError(f"the snapshot of run {run_id} was not complete after {timeout:.0f} s")
        time.sleep(1.0)


def settled_run(client: Any, principal: Mapping[str, Any], run_id: str, timeout: float = 600.0, interval: float = 1.0) -> dict[str, Any]:
    """The run entry once no seal is in progress (a build that just ended is ``sealing`` until the launcher has decided)."""
    deadline = time.monotonic() + timeout
    while True:
        entry = dict(client.call("status", {"what": "run", "id": run_id}, principal))
        if entry.get("build_state") != "sealing":
            return entry
        if time.monotonic() > deadline:
            raise ProvenanceError(f"run {run_id} was still sealing after {timeout:.0f} s")
        time.sleep(interval)


def sealed_completion(client: Any, principal: Mapping[str, Any], run_id: str) -> dict[str, Any]:
    """The ``completion`` record of the run once it is sealed; the caller never receives a path of the sealed copy, because a reader must attach it itself
    behind the consumer entrypoint (``sealed_duckdb`` and the ``sealed_run`` parameter of the reader templates)."""
    entry = settled_run(client, principal, run_id)
    if entry.get("build_state") != "sealed" or not isinstance(entry.get("completion"), Mapping):
        raise ProvenanceError(f"run {run_id} is not sealed; its last build was not a full success ({entry.get('seal_error') or 'no seal recorded'})")
    return dict(check_completion(entry))


def reader_result(stderr: str) -> dict[str, Any]:
    """The check the consumer entrypoint printed as the first line of standard error: what the reader's own container computed from its own object."""
    first = stderr.split("\n", 1)[0]
    if not first.startswith(RESULT_PREFIX):
        raise ProvenanceError("the reader printed no check of its object before it ran")
    try:
        found = json.loads(first[len(RESULT_PREFIX) :])
    except ValueError as error:
        raise ProvenanceError("the check the reader printed is not valid JSON") from error
    if not isinstance(found, dict):
        raise ProvenanceError("the check the reader printed is not an object")
    return found


def accepted_check(outcome: Any, completion: Mapping[str, Any]) -> dict[str, Any]:
    """The check a reader attempt printed, only if it accepts the object that reader attached; otherwise the caller stops here.

    ``outcome`` is the attempt result (``stdout``, ``stderr``, ``exit_code``, ``ok``). The accepted check carries the digest the reader's own
    container computed, never the one it was told.
    """
    result = reader_result(str(outcome.stderr))
    if not accepts_result(result, completion):
        raise ProvenanceError(
            f"the object the reader attached hashes to {result.get('sha256')}, not {completion.get('sha256')}, or its marker differs; its output is not used"
        )
    return result


def accepted_output(outcome: Any, completion: Mapping[str, Any]) -> str:
    """The standard output of a reader attempt that ran on an accepted object and exited 0 (``accepted_check`` first)."""
    accepted_check(outcome, completion)
    if not outcome.ok:
        raise ProvenanceError(f"the reader failed after its object was accepted (exit {outcome.exit_code}): {str(outcome.stderr)[-300:]}")
    return str(outcome.stdout)


def sealed_duckdb(
    session: Any, run_id: str, completion: Mapping[str, Any], sql: str, mounts: Sequence[Mapping[str, str]] = (), flags: Sequence[str] = ("-csv", "-noheader")
) -> list[list[str]]:
    """Run SQL on the sealed copy of a run in a ``duckdb_helper`` container whose entrypoint checks the copy it attached before it runs ``duckdb``.

    ``session`` has ``run(template, launch, args, params)`` (the kit's session). The rows are returned only for an accepted check.
    """
    params: dict[str, Any] = {SEALED_RUN_PARAM: run_id, "sql": sql, "flags": list(flags)}
    if mounts:
        params["mounts"] = [dict(mount) for mount in mounts]
    outcome = session.run("duckdb_helper", f"sealed-{uuid.uuid4().hex[:10]}", [], params)
    return [row for row in csv.reader(io.StringIO(accepted_output(outcome, completion))) if row]


def run_consumer(client: Any, principal: Mapping[str, Any], run_id: str, launch_id: str, checkout: Path) -> bool:
    """Run the ``sealed_consumer`` template on the run's sealed copy and accept its result only by ``accepts_result`` (D1 section 10 item 6)."""
    entry = settled_run(client, principal, run_id)
    completion = entry.get("completion")
    if entry.get("build_state") != "sealed" or not isinstance(completion, Mapping):
        raise ProvenanceError(f"run {run_id} is not sealed")
    check_completion(entry)
    outcome = client.run_attempt(dict(principal), "sealed_consumer", launch_id, ["verify"], {}, checkout=checkout)
    if outcome.exit_code != 0:
        return False
    name = f"{launch_id}-a{outcome.record['n']}.json"
    result = json.loads(read_run_file(runs_root(entry), run_id, ("consumer", name)))
    return accepts_result(result, completion)


def read_marker_row(path: str) -> dict[str, str]:
    """The marker row of the sealed copy, read with the DuckDB of the container image (not available on the host)."""
    duckdb = importlib.import_module("duckdb")
    try:
        connection = duckdb.connect(path, read_only=True)
        try:
            rows = connection.execute(MARKER_QUERY).fetchall()
        finally:
            connection.close()
    except duckdb.Error as error:
        raise ProvenanceError(f"the marker cannot be read: {type(error).__name__}") from error
    if len(rows) != 1:
        raise ProvenanceError(f"the marker holds {len(rows)} rows, not one")
    return {name: str(value) for name, value in zip(MARKER_COLUMNS, rows[0], strict=True)}


def marker_verdict(found: Mapping[str, str], expected: Mapping[str, str]) -> dict[str, Any]:
    """Compare the marker row with the expected values; ``pins_json`` must hash to the pins digest."""
    wrong = sorted(name for name, value in expected.items() if found.get(name) != value)
    if not REVISION.fullmatch(found.get("git_revision", "")):
        wrong.append("git_revision")
    pins_ok = hashlib.sha256(found.get("pins_json", "").encode()).hexdigest() == expected.get("bronze_pins_sha256")
    if not pins_ok:
        wrong.append("pins_json")
    return {"checked": True, "matched": not wrong, "different": wrong}


def check_object(
    descriptor: int, expected_sha256: str, expected_marker: Mapping[str, str] | None, marker_reader: Callable[[], Mapping[str, str]] | None
) -> dict[str, Any]:
    """The consumer's check of the object it was given: the digest of the open file, then the marker row.

    ``descriptor`` is the mounted file as the container sees it. The result names the digest computed here, never the one it was told.
    """
    sha256, size = file_digest(descriptor)
    info = os.fstat(descriptor)
    result: dict[str, Any] = {
        "sha256": sha256,
        "expected_sha256": expected_sha256,
        "matched": sha256 == expected_sha256,
        "size": size,
        "inode": info.st_ino,
        "device": info.st_dev,
        "marker": {"checked": False, "matched": None, "different": []},
        "checked_utc": datetime.now(UTC).isoformat(),
    }
    if result["matched"] and expected_marker is not None and marker_reader is not None:
        try:
            result["marker"] = marker_verdict(marker_reader(), expected_marker)
        except (ProvenanceError, OSError, ImportError, ValueError) as error:
            result["marker"] = {"checked": True, "matched": False, "different": [f"unreadable: {type(error).__name__}"]}
    elif result["matched"]:
        result["marker"] = {"checked": False, "matched": None, "different": []}
    return result


def write_result(path: Path, result: Mapping[str, Any]) -> None:
    """Write the consumer's result once (an existing file is an error)."""
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def consume(argv: Sequence[str]) -> int:
    """``consume --sealed P --expected-sha256 H (--result P | --print-result) [--expected-marker JSON]``: 0 ok, 3 digest differs, 4 marker differs.

    A reader container runs this first and its own command only after exit 0 (``sh -c 'consume ... && exec "$@"'``), on the same mounted object.
    ``--print-result`` writes the check as the first line of standard error, ``--result`` into a new file.
    """
    parser = argparse.ArgumentParser(prog="consume")
    parser.add_argument("--sealed", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--result")
    parser.add_argument("--print-result", action="store_true")
    parser.add_argument("--expected-marker")
    args = parser.parse_args(list(argv))
    if not SHA256.fullmatch(args.expected_sha256):
        sys.stderr.write("the expected digest is not a SHA-256\n")
        return 2
    if not args.result and not args.print_result:
        sys.stderr.write("consume needs --result or --print-result\n")
        return 2
    expected_marker = json.loads(args.expected_marker) if args.expected_marker else None
    descriptor = os.open(args.sealed, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        result = check_object(descriptor, args.expected_sha256, expected_marker, lambda: read_marker_row(args.sealed))
    finally:
        os.close(descriptor)
    if args.result:
        write_result(Path(args.result), result)
    if args.print_result:
        sys.stderr.write(RESULT_PREFIX + json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
        sys.stderr.flush()
    if not result["matched"]:
        sys.stderr.write(f"the mounted file hashes to {result['sha256']}, not {args.expected_sha256}\n")
        return 3
    if expected_marker is not None and result["marker"]["matched"] is not True:
        sys.stderr.write(f"the marker row differs: {', '.join(result['marker']['different'])}\n")
        return 4
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Command line: ``consume`` only."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments[:1] != ["consume"]:
        sys.stderr.write("usage: python -m scripts.lakehouse.provenance consume --sealed PATH --expected-sha256 HEX (--result PATH | --print-result)\n")
        return 2
    return consume(arguments[1:])


if __name__ == "__main__":
    sys.exit(main())
