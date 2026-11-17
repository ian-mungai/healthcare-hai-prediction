"""Set up the Git-ignored inputs of a new worktree from the main checkout, so no check or run starts from a hand-made worktree.

Run from any checkout, with the worktree under ``<main checkout>/.worktrees/``:

    .venv/bin/python -m scripts.quality.worktree_setup .worktrees/<name>

It makes or verifies, in this order, each item that Git ignores:

1. ``.venv`` and ``.tools`` as links to the folders of the main checkout. These two are the only links the command makes at the worktree's top, and a
   link inside a copied folder is made only for a link of the main checkout's folder that points into a copied folder, and then to the worktree's own copy.
2. The dbt ``dbt_packages`` and ``package_archive`` folders and the ``.terraform`` folder of the stacks ``infra``, ``infra/iam``, ``infra/services`` and
   ``infra/platform``, copied from the main checkout (no state file is ever copied).
3. ``.env`` and ``infra/deployment.auto.tfvars.json``, copied only when absent, in mode 0600. They hold the account, bucket and profile names and no credential.
4. The ``.terraform`` folder of ``infra/jobs``, made by ``terraform init -backend=false`` from the copied providers (the main checkout may hold none to copy).
   An existing folder is accepted only when the providers of the lock file are all there and ``terraform modules -json`` succeeds, which it does only when
   Terraform itself finds every module that the configuration declares installed from its declared source; otherwise init runs again, and a modules command
   that cannot run, outlives its deadline or fails never ends as ready. Terraform runs only when the lock file and everything in an existing folder are
   plain files and folders with one name; the one link allowed is the one init makes to the copied providers.
5. The generated requirement and pin files of ``lock_env images``, which include ``services/orchestration/constraints-3.12.txt``, the file that the
   ``airflow-check`` service bind-mounts.

Safety design, in the order the checks run for each destination:

- **Tracked paths.** The command reads the whole Git index (``git ls-files -s -z``, so a path that is tracked but absent from the disk counts) and refuses a
  destination when any index path is the same entry as the destination, lies below it or holds a folder of its path. "The same entry" is decided by the
  filesystem, not by comparing spellings: the command makes a probe file in its private folder and looks every candidate name up there, so a case or
  Unicode normalisation alias (``.TOOLS``, ``.toolſ`` on APFS) is found where the volume treats it as the same name.
- **Folder handles.** Every write goes through folder handles opened one component at a time with ``O_NOFOLLOW`` from the worktree's top, so a link or a
  file in a component of the path is refused and a component swapped for a link after the check cannot redirect a write. A destination that is a link,
  dangling or not, is refused too.
- **Exclusive publication.** A file is written to a staging name in a private folder of the worktree's Git directory (``worktree_setup/stage``), flushed,
  read back and compared with its source by SHA-256, and only then published with ``link``, which fails when the destination exists. A destination is never
  replaced, and a run that stops leaves no partial file at a destination.
- **Completion marker.** ``.env`` and the tfvars file get an entry in ``worktree_setup/published.json`` after their publication. A file without an entry
  that is empty or a cut-short prefix of its source is an earlier run that stopped (the old command wrote in place): it is reported, not called ready, and
  not overwritten. A file edited since its entry is the owner's and is kept.

A second run changes nothing. A file that exists and differs from its source is never overwritten: the run names it and exits 1. ``.env`` and the tfvars
file are the exception, because a worktree may keep its own values: a differing one is kept and reported. A source the main checkout lacks is named, and the
other items are still done. The exit code is 0 when every item is made or verified, 1 when an item is missing or refused and 2 when the path is not a worktree
under ``.worktrees/`` of its repository.

Limits: the command is for one run at a time (a second run at the same moment is not supported). Between a check and its write another process of the same
user can still change the index (a path that becomes tracked after the check), rename a folder that a handle already holds or change what the main
checkout's source files hold. A folder swapped for a link after its handle was opened is not followed, and no destination is ever replaced. ``terraform init``
resolves its own paths, so its folder and lock file are checked just before the call and not during it. A volume that is case-insensitive per folder is probed
in the private folder only.
"""

from __future__ import annotations

import argparse
import contextlib
import errno
import hashlib
import json
import os
import re
import secrets
import stat
import sys
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

from scripts.process import SubprocessError, run_command
from scripts.quality import lock_env

LINKS = (".venv", ".tools")
STACKS = ("infra", "infra/iam", "infra/services", "infra/platform")
TREES = ("data/analytics/dbt/dbt_packages", "data/analytics/dbt/package_archive", *(f"{stack}/.terraform" for stack in STACKS))
COPIED_FILES = (".env", "infra/deployment.auto.tfvars.json")
JOBS = "infra/jobs"
JOBS_LOCK = f"{JOBS}/.terraform.lock.hcl"
PROVIDERS = "infra/.terraform/providers"
SKIPPED_NAMES = (".DS_Store",)
STATE_MARKER = ".tfstate"
SCRATCH = "worktree_setup"
MARKER_FILE = "published.json"
GIT_DEADLINE = 60
TERRAFORM_DEADLINE = 600
CHUNK = 1 << 20
MAX_JUDGED = 1 << 20  # A copied small file larger than this is the owner's own, whatever its start.
DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
NEW_FILE = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
EXISTING_FILE = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC  # Non-blocking, so a pipe swapped in after the check cannot hold the run.
COLOUR = re.compile(r"\x1b\[[0-9;]*m")
LOCK_PROVIDER = re.compile(r'provider\s+"([^"]+)"\s*\{\s*version\s*=\s*"([^"]+)"')


class SetupRefused(ValueError):
    """The path is not a worktree under ``.worktrees/`` of its repository."""


class Refusal(Exception):
    """One destination is refused; the text says why, and nothing was written there. ``prefix`` names the folder of a tree that holds a link or a file."""

    def __init__(self, text: str, prefix: tuple[str, ...] | None = None) -> None:
        super().__init__(text)
        self.prefix = prefix


@dataclass
class Report:
    """What one run did, in order, and what stopped it."""

    lines: list[str] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)

    def note(self, text: str) -> None:
        self.lines.append(text)

    def problem(self, item: str, text: str) -> None:
        self.problems.append(f"{item}: {text}")


@dataclass
class Node:
    """One name of the index trie; ``path`` is the tracked path to the node and ``tracked`` is true when an index entry ends here."""

    path: bytes
    tracked: bool = False
    children: dict[bytes, Node] = field(default_factory=dict)


@dataclass
class Entry:
    """One file or link of a source folder, with its path relative to the folder."""

    parts: tuple[str, ...]
    path: Path
    link: str | None = None


def terraform_environment() -> dict[str, str]:
    """The caller's environment with Terraform's Checkpoint update check turned off, so setup never reaches the network."""
    return {**os.environ, "CHECKPOINT_DISABLE": "1"}


def generated_files(root: Path) -> dict[str, str]:
    """Repository path to expected text of every file ``lock_env images`` writes; a seam for the cases, which have no ``uv``."""
    return lock_env.image_files(root)


def git(folder: Path, *arguments: str) -> tuple[int, str]:
    result = run_command("git", ["-C", str(folder), *arguments], timeout=GIT_DEADLINE)
    return result.returncode, result.stdout.strip()


def locate(worktree: Path) -> tuple[Path, Path]:
    """The resolved worktree and the main checkout, after checking that the worktree is a folder directly under ``<main>/.worktrees/``."""
    path = worktree.resolve()
    if not path.is_dir():
        raise SetupRefused(f"{worktree} is not a folder")
    code, top = git(path, "rev-parse", "--show-toplevel")
    if code or Path(top).resolve() != path:
        raise SetupRefused(f"{worktree} is not the top folder of a Git worktree")
    code, common = git(path, "rev-parse", "--path-format=absolute", "--git-common-dir")
    if code:
        raise SetupRefused(f"{worktree}: Git cannot name its repository")
    main = Path(common).resolve().parent
    if path.parent != main / ".worktrees":
        raise SetupRefused(f"{worktree} is not a worktree directly under {main / '.worktrees'} (a worktree is a folder of .worktrees/ in the main checkout)")
    return path, main


def read_index(worktree: Path) -> Node:
    """The trie of every path of the Git index, in every stage and including paths that are absent from the disk."""
    result = run_command("git", ["-C", str(worktree), "ls-files", "-s", "-z"], timeout=GIT_DEADLINE, errors="surrogateescape")
    if result.returncode:
        raise Refusal(f"git ls-files failed ({result.returncode}): {result.stderr.strip()[-300:]}")
    root = Node(b"")
    for record in result.stdout.split("\0"):
        if not record:
            continue
        _, _, path = record.partition("\t")
        node = root
        for part in os.fsencode(path).split(b"/"):
            node = node.children.setdefault(part, Node(f"{node.path.decode(errors='replace')}/{part.decode(errors='replace')}".strip("/").encode()))
        node.tracked = True
    return root


class Scratch:
    """The command's private folder in the worktree's Git directory: staging files, name probes and the completion marker."""

    def __init__(self, git_directory: Path, top_device: int) -> None:
        self.fds: list[int] = []
        self.names: dict[tuple[bytes, bytes], bool] = {}
        try:
            base = os.open(git_directory, DIRECTORY)
            self.fds.append(base)
            self.fd = self.sub(base, SCRATCH)
            self.stage = self.sub(self.fd, "stage")
            self.probe = self.sub(self.fd, "probe")
            if os.fstat(self.fd).st_dev != top_device:
                raise Refusal(f"the Git directory {git_directory} is on another filesystem than the worktree, so no file can be published from it")
            for folder in (self.stage, self.probe):
                for name in os.listdir(folder):  # An entry here was left by a run that was killed; only one run is supported at a time.
                    with contextlib.suppress(FileNotFoundError):
                        os.unlink(name, dir_fd=folder)
            for name in os.listdir(self.fd):
                if name.startswith(f"{MARKER_FILE}."):
                    with contextlib.suppress(FileNotFoundError):
                        os.unlink(name, dir_fd=self.fd)
        except BaseException:
            self.close()
            raise

    def sub(self, parent: int, name: str) -> int:
        with contextlib.suppress(FileExistsError):
            os.mkdir(name, 0o700, dir_fd=parent)
        descriptor = os.open(name, DIRECTORY, dir_fd=parent)
        self.fds.append(descriptor)
        return descriptor

    def close(self) -> None:
        for descriptor in reversed(self.fds):
            with contextlib.suppress(OSError):
                os.close(descriptor)

    def aliases(self, part: bytes, names: Sequence[bytes]) -> list[bytes]:
        """The names of ``names`` that this filesystem gives to the same entry as ``part``: make a probe file named ``part`` and look each name up beside it."""
        unknown = [name for name in names if name != part and (part, name) not in self.names]
        if unknown:
            try:
                descriptor = os.open(part, NEW_FILE, 0o600, dir_fd=self.probe)
            except OSError as error:
                raise Refusal(f"the filesystem cannot make a probe named {part.decode(errors='replace')}: {error.strerror}") from error
            try:
                os.close(descriptor)
                own = os.stat(part, dir_fd=self.probe, follow_symlinks=False)
                for name in unknown:
                    try:
                        other = os.stat(name, dir_fd=self.probe, follow_symlinks=False)
                        self.names[part, name] = (own.st_dev, own.st_ino) == (other.st_dev, other.st_ino)
                    except (FileNotFoundError, NotADirectoryError):
                        self.names[part, name] = False
            finally:
                os.unlink(part, dir_fd=self.probe)
        return [name for name in names if name == part or self.names[part, name]]

    def read_marker(self) -> dict[str, object]:
        try:
            descriptor = os.open(MARKER_FILE, EXISTING_FILE, dir_fd=self.fd)
        except FileNotFoundError:
            return {}
        with os.fdopen(descriptor, "rb") as handle:
            try:
                found = json.loads(handle.read())
            except ValueError:
                return {}
        files = found.get("files") if isinstance(found, dict) else None
        return files if isinstance(files, dict) else {}


def validated(parts: Sequence[str]) -> None:
    for part in parts:
        if part in ("", ".", "..") or "/" in part or "\0" in part:
            raise Refusal(f"{part!r} is not a plain file name")


def open_directory(parent: int, name: str, create: bool) -> int:
    """A handle to the folder ``name`` of the open folder ``parent``, made when absent and ``create`` is set; a link or a file there is refused."""
    for _ in range(3):
        try:
            return os.open(name, DIRECTORY, dir_fd=parent)
        except FileNotFoundError:
            if not create:
                raise
            with contextlib.suppress(FileExistsError):
                os.mkdir(name, 0o755, dir_fd=parent)
        except OSError as error:
            if error.errno in (errno.ELOOP, errno.ENOTDIR):
                raise Refusal(f"{name} is a link or is not a folder, so nothing is written below it") from error
            raise
    raise Refusal(f"{name} changed while it was opened")


@contextlib.contextmanager
def directory(root: int, parts: Sequence[str], create: bool) -> Iterator[int]:
    """The handle of the folder at ``parts`` below ``root``, opened one component at a time without following any link."""
    validated(parts)
    handles: list[int] = []
    current = root
    try:
        for part in parts:
            current = open_directory(current, part, create)
            handles.append(current)
        yield current
    finally:
        for descriptor in reversed(handles):
            os.close(descriptor)


def lstat_leaf(parent: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(name, dir_fd=parent, follow_symlinks=False)
    except FileNotFoundError:
        return None


def digest_of(parent: int, name: str) -> tuple[str, int]:
    """SHA-256 and size of the regular file ``name`` in the open folder, read without following a link."""
    with open_existing(parent, name) as handle:
        return digest_stream(handle)


@contextlib.contextmanager
def open_existing(parent: int, name: str) -> Iterator[BinaryIO]:
    """The regular file ``name`` of the open folder, opened for reading without following a link; anything else is refused."""
    descriptor = os.open(name, EXISTING_FILE, dir_fd=parent)
    with os.fdopen(descriptor, "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise Refusal(f"{name} is not a regular file")
        yield handle


def digest_stream(handle: BinaryIO) -> tuple[str, int]:
    digest, size = hashlib.sha256(), 0
    while chunk := handle.read(CHUNK):
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def source_digest(path: Path) -> tuple[str, int]:
    with path.open("rb") as handle:
        return digest_stream(handle)


class Setup:
    """The open handles and the index of one worktree; every write of the command goes through this."""

    def __init__(self, worktree: Path, main: Path, report: Report, root: int, scratch: Scratch, index: Node) -> None:
        self.worktree, self.main, self.report = worktree, main, report
        self.root, self.scratch, self.index = root, scratch, index

    @classmethod
    def start(cls, worktree: Path, main: Path, report: Report) -> Setup | None:
        """Open the worktree's top, the private folder and the index; None after naming why the worktree cannot be set up."""
        code, git_directory = git(worktree, "rev-parse", "--absolute-git-dir")
        if code:
            report.problem("worktree", "Git cannot name its Git directory")
            return None
        root = os.open(worktree, DIRECTORY)
        try:
            scratch = Scratch(Path(git_directory), os.fstat(root).st_dev)
            try:
                return cls(worktree, main, report, root, scratch, read_index(worktree))
            except BaseException:
                scratch.close()
                raise
        except (Refusal, OSError) as error:
            os.close(root)
            report.problem("worktree", f"cannot start: {error}")
            return None
        except BaseException:
            os.close(root)
            raise

    def close(self) -> None:
        self.scratch.close()
        os.close(self.root)

    def tracked_conflict(self, relative: str) -> str | None:
        """Why ``relative`` cannot be written: a tracked path is its entry, lies below it or holds a folder of its path. None when no index path touches it."""
        nodes = [self.index]
        parts = os.fsencode(relative).split(b"/")
        for position, part in enumerate(parts):
            following: list[Node] = []
            for node in nodes:
                try:
                    found = self.scratch.aliases(part, list(node.children))
                except Refusal as error:
                    return f"cannot be compared with the tracked paths ({error})"
                following.extend(node.children[name] for name in found)
            if not following:
                return None
            held = next((child for child in following if child.tracked), None)
            if held is not None and position < len(parts) - 1:
                return f"is inside the tracked file {held.path.decode(errors='replace')}"
            nodes = following
        node = nodes[0]
        return f"{'is the tracked path' if node.tracked else 'holds tracked paths below'} {node.path.decode(errors='replace')}"

    def ignore_gap(self, relative: str, folder: bool) -> str | None:
        """Why Git does not ignore the path (None when it does); a folder is tested through a name inside it, as Git matches ``name/`` only inside."""
        probe = f"{relative}/.setup_probe" if folder else relative
        code, _ = git(self.worktree, "check-ignore", "-q", "--", probe)
        if code == 0:
            return None
        return "is not ignored by Git" if code == 1 else "cannot be checked by Git (a link or a file holds a folder of its path)"

    def refuse(self, relative: str, folder: bool, only_new: bool = False) -> bool:
        """Name the reason and return True when the destination must not be written: a tracked path touches it, or Git would show it.

        ``only_new`` asks for the ignore test only when nothing is at the path yet; the tracked test always runs.
        """
        conflict = self.tracked_conflict(relative)
        if conflict:
            self.report.problem(relative, f"{conflict}, so nothing is written there")
            return True
        gap = None if only_new and os.path.lexists(self.worktree / relative) else self.ignore_gap(relative, folder)
        if gap is None:
            return False
        self.report.problem(relative, f"{gap}, so nothing is written there")
        return True

    def publish(self, parent: int, name: str, source: Path | bytes, mode: int, mtime: int | None = None) -> str:
        """Write a file to a staging name, verify it against its source, then publish it under ``name`` without replacing anything. Returns its SHA-256."""
        staging = f"{secrets.token_hex(8)}.part"
        descriptor = os.open(staging, NEW_FILE, 0o600, dir_fd=self.scratch.stage)
        try:
            digest = hashlib.sha256()
            with os.fdopen(descriptor, "wb") as handle:
                if isinstance(source, bytes):
                    digest.update(source)
                    handle.write(source)
                else:
                    with source.open("rb") as reader:
                        while chunk := reader.read(CHUNK):
                            digest.update(chunk)
                            handle.write(chunk)
                handle.flush()
                os.fchmod(handle.fileno(), mode)
                if mtime is not None:
                    os.utime(handle.fileno(), ns=(mtime, mtime))
                os.fsync(handle.fileno())
            written, _ = digest_of(self.scratch.stage, staging)
            if written != digest.hexdigest():
                raise Refusal(f"the staged copy of {name} does not match its source")
            try:
                os.link(staging, name, src_dir_fd=self.scratch.stage, dst_dir_fd=parent, follow_symlinks=False)
            except FileExistsError as error:
                raise Refusal(f"{name} appeared before it was published; it was not replaced") from error
            except OSError as error:
                if error.errno == errno.EXDEV:
                    raise Refusal("the Git directory is on another filesystem than the worktree") from error
                raise
            return written
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(staging, dir_fd=self.scratch.stage)

    def link(self, parent: int, name: str, target: str) -> None:
        try:
            os.symlink(target, name, dir_fd=parent)
        except FileExistsError as error:
            raise Refusal(f"{name} appeared before the link was made; it was not replaced") from error


class Folders:
    """Handles to the folders of one copied tree, opened one component at a time without following a link and kept until the tree is done."""

    def __init__(self, base: int) -> None:
        self.base = base
        self.open: dict[tuple[str, ...], int] = {}

    def get(self, parts: tuple[str, ...]) -> int:
        current = self.base
        for depth in range(1, len(parts) + 1):
            prefix = parts[:depth]
            if prefix not in self.open:
                validated(prefix)
                try:
                    self.open[prefix] = open_directory(current, prefix[-1], create=True)
                except Refusal as error:
                    raise Refusal(str(error), prefix) from error
            current = self.open[prefix]
        return current

    def close(self) -> None:
        for descriptor in self.open.values():
            os.close(descriptor)


def write_marker(setup: Setup, relative: str, digest: str, size: int) -> None:
    """Record that ``relative`` was published completely; written after the publication, by staging and replacing the marker file."""
    files = setup.scratch.read_marker()
    files[relative] = {"sha256": digest, "size": size}
    staging = f"{MARKER_FILE}.{secrets.token_hex(4)}"
    descriptor = os.open(staging, NEW_FILE, 0o600, dir_fd=setup.scratch.fd)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(json.dumps({"version": 1, "files": files}, indent=1, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(staging, MARKER_FILE, src_dir_fd=setup.scratch.fd, dst_dir_fd=setup.scratch.fd)


def link_item(setup: Setup, name: str) -> None:
    report, source = setup.report, setup.main / name
    if not source.exists():
        report.problem(name, f"the main checkout has no {name} to link to")
        return
    if setup.refuse(name, folder=False, only_new=True):
        return
    found = lstat_leaf(setup.root, name)
    if found is None:
        try:
            setup.link(setup.root, name, str(source))
        except Refusal as error:
            report.problem(name, str(error))
        else:
            report.note(f"created {name} -> {source}")
    elif stat.S_ISLNK(found.st_mode):
        try:
            same = os.stat(name, dir_fd=setup.root)
        except OSError:
            report.problem(name, f"is a dangling link to {os.readlink(name, dir_fd=setup.root)}; it was left as it is")
            return
        wanted = source.stat()
        if (same.st_dev, same.st_ino) == (wanted.st_dev, wanted.st_ino):
            report.note(f"verified {name} (link to the main checkout)")
        else:
            report.problem(name, f"is a link to {os.readlink(name, dir_fd=setup.root)}, not to {source}; it was left as it is")
    else:
        report.problem(name, "exists and is not a link to the main checkout; it was left as it is")


def skipped(name: str) -> bool:
    return name in SKIPPED_NAMES or STATE_MARKER in name


def tree_entries(folder: Path) -> list[Entry]:
    """Every file and link below the folder that a copy carries; a link is an entry and is never followed, and a state file is never one."""
    found: list[Entry] = []
    for current, folders, files in os.walk(folder, followlinks=False):
        links = [name for name in folders if (Path(current) / name).is_symlink()]
        folders[:] = sorted(name for name in folders if name not in links)
        for name in files + links:
            path = Path(current) / name
            if skipped(name):
                continue
            if path.is_symlink():
                found.append(Entry(path.relative_to(folder).parts, path, os.readlink(path)))
            elif stat.S_ISREG(path.lstat().st_mode):
                found.append(Entry(path.relative_to(folder).parts, path))
    return sorted(found, key=lambda entry: entry.parts)


def mapped_link(setup: Setup, entry: Entry, link: str) -> str:
    """The text of the link that stands for a source link in the worktree: a link of the main checkout into a copied folder points to the worktree's copy."""
    absolute = os.path.normpath(link if os.path.isabs(link) else os.path.join(entry.path.parent, link))
    try:
        relative = Path(absolute).relative_to(setup.main).as_posix()
    except ValueError:
        raise Refusal(f"is a link to {link}, outside the main checkout, so it was not copied") from None
    if not any(relative == tree or relative.startswith(f"{tree}/") for tree in TREES):
        raise Refusal(f"is a link to {link}, which is not inside a copied folder, so it was not copied")
    if not os.path.exists(setup.worktree / relative):
        raise Refusal(f"is a link to {link}, whose copy {relative} is not in the worktree, so it was not copied")
    return str(setup.worktree / relative) if os.path.isabs(link) else link


def reaches_target(parent: int, name: str) -> bool:
    """True when the link ``name`` of the open folder points at something that exists."""
    try:
        os.stat(name, dir_fd=parent)
    except OSError:
        return False
    return True


def copy_entry(setup: Setup, folders: Folders, entry: Entry) -> str:
    """Copy one entry of a tree. Returns ``copied`` or ``verified``; raises ``Refusal`` for a differing, linked or redirected destination."""
    parent, name = folders.get(entry.parts[:-1]), entry.parts[-1]
    found = lstat_leaf(parent, name)
    if entry.link is not None:
        text = mapped_link(setup, entry, entry.link)
        if found is None:
            setup.link(parent, name, text)
            return "copied"
        if stat.S_ISLNK(found.st_mode) and os.readlink(name, dir_fd=parent) == text and reaches_target(parent, name):
            return "verified"
        raise Refusal("differs from the main checkout's link or is a dangling link; it was not replaced")
    stats = entry.path.stat()
    if found is None:
        setup.publish(parent, name, entry.path, stat.S_IMODE(stats.st_mode), stats.st_mtime_ns)
        return "copied"
    if not stat.S_ISREG(found.st_mode):
        raise Refusal("is a link or not a file; it was left as it is")
    if found.st_size == stats.st_size and digest_of(parent, name) == source_digest(entry.path):
        return "verified"
    raise Refusal("differs from the main checkout's file; it was not overwritten")


def copy_tree(setup: Setup, relative: str) -> None:
    """Copy every file of the main checkout's folder that the worktree's folder lacks; a file that exists and differs is named and left."""
    report = setup.report
    entries = tree_entries(setup.main / relative) if (setup.main / relative).is_dir() else []
    if not entries:
        report.problem(relative, "the main checkout has no files there to copy")
        return
    if setup.refuse(relative, folder=True):
        return
    copied = verified = 0
    blocked: set[tuple[str, ...]] = set()
    try:
        with directory(setup.root, relative.split("/"), create=True) as base:
            folders = Folders(base)
            try:
                for entry in entries:
                    if any(entry.parts[:depth] in blocked for depth in range(1, len(entry.parts))):
                        continue
                    try:
                        outcome = copy_entry(setup, folders, entry)
                    except Refusal as error:
                        if error.prefix is None:
                            report.problem(f"{relative}/{'/'.join(entry.parts)}", str(error))
                        else:
                            blocked.add(error.prefix)
                            report.problem(f"{relative}/{'/'.join(error.prefix)}", str(error))
                        continue
                    copied += outcome == "copied"
                    verified += outcome == "verified"
            finally:
                folders.close()
    except Refusal as error:
        report.problem(relative, str(error))
        return
    report.note(f"copied {relative} ({copied} files added, {verified} already there)" if copied else f"verified {relative} ({verified} files)")


def copy_file(setup: Setup, relative: str) -> None:
    """Copy a file in mode 0600 when the worktree has none; a file that is there is kept, and a different one is reported."""
    report, source = setup.report, setup.main / relative
    if not source.is_file():
        report.problem(relative, "the main checkout has no such file to copy")
        return
    if setup.refuse(relative, folder=False, only_new=True):
        return
    data = source.read_bytes()
    wanted = hashlib.sha256(data).hexdigest()
    *folders, name = relative.split("/")
    try:
        with directory(setup.root, folders, create=True) as parent:
            validated([name])
            found = lstat_leaf(parent, name)
            if found is None:
                setup.publish(parent, name, data, 0o600)
                write_marker(setup, relative, wanted, len(data))
                report.note(f"created {relative} (mode 0600)")
            elif not stat.S_ISREG(found.st_mode):
                report.problem(relative, "is a link or not a file; it was left as it is")
            else:
                judge_existing(setup, relative, parent, name, data)
    except Refusal as error:
        report.problem(relative, str(error))


def judge_existing(setup: Setup, relative: str, parent: int, name: str, data: bytes) -> None:
    """Judge a copied file that is already there: complete, edited since the setup, the owner's own, or left cut short by a run that stopped."""
    with open_existing(parent, name) as handle:
        existing = handle.read(MAX_JUDGED + 1)
    found, wanted = hashlib.sha256(existing).hexdigest(), hashlib.sha256(data).hexdigest()
    recorded = setup.scratch.read_marker().get(relative)
    if not isinstance(recorded, dict):
        recorded = None
    if existing == data:
        if not recorded or recorded.get("sha256") != wanted:
            write_marker(setup, relative, wanted, len(data))
            setup.report.note(f"verified {relative} (complete; an earlier run stopped before it wrote its marker, so the marker is written now)")
        else:
            setup.report.note(f"verified {relative}")
    elif recorded:
        reason = "edited here after the setup" if recorded.get("sha256") != found else "the main checkout's file changed since the setup"
        setup.report.note(f"kept {relative} ({reason}; it is not replaced)")
    elif not existing or (len(existing) < len(data) and data.startswith(existing) and not existing.endswith(b"\n")):
        setup.report.problem(
            relative,
            "is incomplete: it has no completion marker and is empty or a cut-short start of the main checkout's file, so an earlier run stopped; "
            "delete it and run again (it was not replaced)",
        )
    else:
        setup.report.note(f"kept {relative} (it differs from the main checkout's file, which is not copied over it)")


def modules_gap(setup: Setup) -> str | None:
    """Why terraform does not take the modules of the configuration as installed (None when it does): ``terraform modules -json`` decides.

    The command fails when a module the configuration declares is not installed or its source has changed, and it lists every installed module, nested ones
    included. A command that cannot run, outlives its deadline or fails counts as incomplete, so the caller runs init and never reports ready.
    """
    arguments = [f"-chdir={setup.worktree / JOBS}", "modules", "-json"]
    try:
        result = run_command("terraform", arguments, cwd=setup.worktree, timeout=TERRAFORM_DEADLINE, env=terraform_environment())
    except (OSError, SubprocessError) as error:
        return f"terraform modules could not run ({type(error).__name__})"
    if result.returncode:
        said = " ".join(COLOUR.sub("", result.stderr or result.stdout).split())
        return f"terraform modules failed ({result.returncode}): {said[-400:]}"
    try:
        listed = json.loads(result.stdout)["modules"]
        if not isinstance(listed, list) or not all(isinstance(item, dict) and isinstance(item.get("key"), str) for item in listed):
            raise TypeError
    except (ValueError, KeyError, TypeError):
        return "terraform modules printed no list of modules"
    return None


def jobs_cache_gap(setup: Setup) -> str | None:
    """Why the jobs stack's ``.terraform`` is not a complete cache: None when every lock file provider is there and terraform finds every module installed."""
    cache = setup.worktree / JOBS / ".terraform"
    try:
        lock = (setup.worktree / JOBS_LOCK).read_text(encoding="utf-8")
    except OSError:
        lock = ""
    for provider, version in LOCK_PROVIDER.findall(lock):
        host, namespace, name = provider.split("/")
        folder = cache / "providers" / host / namespace / name / version
        prefix = f"terraform-provider-{name}_v{version}"
        files: list[Path] = []
        with contextlib.suppress(OSError):
            files = [folder / platform / entry for platform in os.listdir(folder) for entry in os.listdir(folder / platform) if entry.startswith(prefix)]
        if not any(os.path.isfile(path) and os.path.getsize(path) > 0 for path in files):
            return f"the provider {provider} {version} is missing or empty"
        for path in files:
            copied = setup.worktree / PROVIDERS / host / namespace / name / version / path.parent.name / path.name
            if os.path.isfile(copied) and not os.path.samefile(copied, path) and source_digest(copied) != source_digest(path):
                return f"the provider file {path.name} differs from the copied providers"
    return modules_gap(setup)


def native_provider_link(setup: Setup, cache: Path, path: Path) -> bool:
    """True for the link terraform init makes to a platform folder of the copied providers: ``providers/<host>/<namespace>/<name>/<version>/<platform>``."""
    parts = path.relative_to(cache).parts
    if len(parts) != 6 or parts[0] != "providers":
        return False
    wanted = setup.worktree / PROVIDERS / Path(*parts[1:])
    return os.path.exists(wanted) and os.path.realpath(path) == os.path.realpath(wanted)


def init_hazard(setup: Setup, cache: Path) -> str | None:
    """Why terraform init must not run: it writes by path, so a link or a file with another name in its cache or lock file would take its writes elsewhere.

    The lock file and everything below an existing cache must be plain folders and files with one name. The one link that is allowed is the one terraform
    init itself makes from the cache to the copied providers of the worktree.
    """
    lock = setup.worktree / JOBS_LOCK
    with contextlib.suppress(FileNotFoundError):
        held = lock.lstat()
        if stat.S_ISLNK(held.st_mode) or (stat.S_ISREG(held.st_mode) and held.st_nlink > 1):
            return f"{JOBS_LOCK} is a link or has other names, and terraform init writes it"
    if not cache.is_dir():
        return None
    for current, folders, files in os.walk(cache, followlinks=False):
        for name in folders + files:
            path = Path(current) / name
            held = path.lstat()
            shown = path.relative_to(setup.worktree)
            if stat.S_ISLNK(held.st_mode):
                if not native_provider_link(setup, cache, path):
                    return f"{shown} is a link, and terraform init would write through it"
            elif stat.S_ISREG(held.st_mode):
                if held.st_nlink > 1:
                    return f"{shown} has {held.st_nlink} hard links, and terraform init would write through them"
            elif not stat.S_ISDIR(held.st_mode):
                return f"{shown} is not a plain file or folder"
    return None


def initialise_jobs(setup: Setup) -> None:
    """Make the jobs stack's ``.terraform`` from the copied providers, with no backend and no network; accept an existing one only when it is complete."""
    report = setup.report
    relative = f"{JOBS}/.terraform"
    conflict = setup.tracked_conflict(relative)
    if conflict:
        report.problem(relative, f"{conflict}, so nothing is written there")
        return
    try:
        with directory(setup.root, JOBS.split("/"), create=False) as jobs:
            found = lstat_leaf(jobs, ".terraform")
    except (Refusal, FileNotFoundError) as error:
        report.problem(JOBS, f"cannot be initialised: {error}")
        return
    if found is not None and not stat.S_ISDIR(found.st_mode):
        report.problem(relative, "is a link or not a folder; it was left as it is")
        return
    hazard = init_hazard(setup, setup.worktree / relative)
    if hazard:
        report.problem(JOBS, f"terraform was not run: {hazard}")
        return
    gap = jobs_cache_gap(setup) if found is not None else "it does not exist"
    if gap is None:
        report.note(f"verified {relative}")
        return
    if not (setup.worktree / PROVIDERS).is_dir():
        report.problem(JOBS, f"cannot be initialised: {PROVIDERS} is missing")
        return
    if setup.refuse(relative, folder=True):
        return
    arguments = [f"-chdir={setup.worktree / JOBS}", "init", "-backend=false", "-input=false", f"-plugin-dir={setup.worktree / PROVIDERS}"]
    try:
        result = run_command("terraform", arguments, cwd=setup.worktree, timeout=TERRAFORM_DEADLINE, env=terraform_environment())
    except (OSError, SubprocessError) as error:
        report.problem(JOBS, f"terraform init could not run: {type(error).__name__}")
        return
    if result.returncode:
        report.problem(JOBS, f"terraform init -backend=false failed ({result.returncode}): {(result.stderr or result.stdout).strip()[-600:]}")
        return
    after = jobs_cache_gap(setup)
    if after:
        report.problem(JOBS, f"terraform init finished but the cache is incomplete: {after}")
    elif found is None:
        report.note(f"created {relative} (terraform init -backend=false)")
    else:
        report.note(f"completed {relative} (terraform init -backend=false ran again because {gap})")


def generate(setup: Setup) -> None:
    """Write each generated file the worktree lacks from the dependency lock; a file that exists and differs from the lock is named and left."""
    report = setup.report
    try:
        expected = generated_files(setup.worktree)
    except (lock_env.LockError, OSError) as error:
        report.problem("generated files", f"cannot be made from the lock: {error}")
        return
    written = 0
    for relative, text in expected.items():
        data = text.encode("utf-8")
        if setup.refuse(relative, folder=False, only_new=True):
            continue
        *folders, name = relative.split("/")
        try:
            with directory(setup.root, folders, create=True) as parent:
                validated([name])
                found = lstat_leaf(parent, name)
                if found is None:
                    setup.publish(parent, name, data, 0o644)
                    written += 1
                elif not stat.S_ISREG(found.st_mode):
                    report.problem(relative, "is a link or not a file; it was left as it is")
                elif digest_of(parent, name)[0] != hashlib.sha256(data).hexdigest():
                    report.problem(
                        relative, "differs from the lock; it was not overwritten (run `.venv/bin/python -m scripts.quality.lock_env images` to replace it)"
                    )
        except Refusal as error:
            report.problem(relative, str(error))
    pins = lock_env.PIN_FILES["orchestration"]
    report.note(
        f"created {written} of the {len(expected)} generated files of lock_env images, including {pins}"
        if written
        else f"verified the generated files of lock_env images, including {pins}"
    )


def attempt(setup_state: Setup, item: str, action: Callable[[Setup, str], None]) -> None:
    """Run one item; a file error (a full disk, a permission) is named as that item's problem, and the other items still run."""
    try:
        action(setup_state, item)
    except OSError as error:
        setup_state.report.problem(item, f"stopped by {type(error).__name__}: {error.strerror or error}; no partial file was published")


def setup(worktree: Path, main: Path) -> Report:
    """Make or verify every item for the worktree, collecting every problem instead of stopping at the first."""
    report = Report()
    state = Setup.start(worktree, main, report)
    if state is None:
        return report
    try:
        for name in LINKS:
            attempt(state, name, link_item)
        for relative in TREES:
            attempt(state, relative, copy_tree)
        for relative in COPIED_FILES:
            attempt(state, relative, copy_file)
        attempt(state, JOBS, lambda current, _: initialise_jobs(current))
        attempt(state, "generated files", lambda current, _: generate(current))
    finally:
        state.close()
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("worktree", type=Path, help="a worktree folder directly under .worktrees/ of the main checkout")
    arguments = parser.parse_args(argv)
    try:
        worktree, main_checkout = locate(arguments.worktree)
    except (SetupRefused, OSError, SubprocessError) as error:
        sys.stderr.write(f"worktree_setup: {error}\n")
        return 2
    report = setup(worktree, main_checkout)
    for line in report.lines:
        sys.stdout.write(f"worktree_setup: {line}\n")
    for problem in report.problems:
        sys.stderr.write(f"worktree_setup: {problem}\n")
    sys.stdout.write(f"worktree_setup: {worktree}: {'ready' if not report.problems else f'{len(report.problems)} problem(s) above'}\n")
    return 1 if report.problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
