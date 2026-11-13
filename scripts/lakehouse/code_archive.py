"""Build and check the code archive that a layer task fetches: one deterministic tarball of a Git revision plus the verified dbt packages.

    .venv/bin/python -m scripts.lakehouse.code_archive build --output <new folder> [--revision HEAD]
    .venv/bin/python -m scripts.lakehouse.code_archive check <code.tar.gz> <code.sha256>

``git archive`` alone cannot pass the first layer pin: the dbt packages are installed, not tracked, and the task has no network to install them. The builder
exports the revision, installs the packages of that revision's lock from the pinned local archive (``dbt_package_archive``, no network) and writes both
into one tar under ``dbt_packages/``. Names, order, owners, times and the gzip header are fixed, so the same revision and the same pinned packages always give
the same bytes. The digest file holds the SHA-256 of the tarball, which is the value the launch passes to the task as the code digest.
``check`` applies the layout that the layer driver's pin phase needs, before anything is staged or claimed.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import re
import shutil
import sys
import tarfile
import tempfile
from pathlib import Path

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
REVISION = re.compile(r"[0-9a-f]{40}")
SHA256 = re.compile(r"[0-9a-f]{64}")
PROJECT = "dbt"
PACKAGES = "dbt_packages"
LOCK = "dbt/package-lock.yml"
RECORD = "config/lakehouse/dbt_package_archive.json"
ARCHIVE_FOLDER = "data/analytics/dbt/package_archive"
LIMIT = 512 * 1024 * 1024
POINTER_LIMIT = 1024
POINTER_KEYS = ("version", "oid", "size")
POINTER_VERSIONS = frozenset({"https://git-lfs.github.com/spec/v1", "https://hawser.github.com/spec/v1", "http://git-media.io/v/2"})
POINTER_OID = re.compile(r"sha256:[0-9a-f]{64}")
POINTER_SIZE = re.compile(r"[+-]?[0-9]+")
POINTER_EXTENSION = re.compile(r"ext-([0-9])-([A-Za-z0-9_][^\n ]*)")
POINTER_SPACE = "\t\n\v\f\r \x85\xa0\u1680\u2028\u2029\u202f\u205f\u3000" + "".join(chr(code) for code in range(0x2000, 0x200B))


class ArchiveError(ValueError):
    """The code archive cannot be built, or it lacks what the layer pin needs."""


def git(arguments: list[str], root: Path) -> str:
    done = run_command("git", arguments, cwd=root, timeout=300)
    if done.returncode:
        raise ArchiveError(f"git {arguments[0]} failed: {done.stderr.strip()[-300:]}")
    return done.stdout.strip()


def pointer_value_is_valid(key: str, value: str) -> bool:
    if key == "version":
        return value in POINTER_VERSIONS
    if key == "oid":
        return POINTER_OID.fullmatch(value) is not None
    return POINTER_SIZE.fullmatch(value) is not None and 0 <= int(value) < 2**63


def is_pointer(head: bytes) -> bool:
    """Whether Git LFS takes a file that starts with ``head`` (its first ``POINTER_LIMIT`` bytes) as a pointer, as ``git lfs pointer --check`` decides it.
    The rule is Git LFS's own, so a form that is not canonical still counts: surrounding whitespace, blank lines, CRLF, a missing final newline, the older
    version URLs, a signed size and extension lines before the size, whose names start with a letter, digit or underscore and then hold any character but a
    space, tabs and Unicode spaces included. A file with no pointer lines, an empty one included, is not a pointer."""
    seen = 0
    extensions: dict[str, str] = {}
    for line in head.decode("utf-8", "surrogateescape").strip(POINTER_SPACE).split("\n"):
        line = line.removesuffix("\r")
        if not line:
            continue
        key, space, value = line.partition(" ")
        extension = POINTER_EXTENSION.fullmatch(key)
        if not space or seen == len(POINTER_KEYS):
            return False
        if key == POINTER_KEYS[seen]:
            if not pointer_value_is_valid(key, value):
                return False
            seen += 1
        elif extension is None or POINTER_OID.fullmatch(value) is None or extensions.setdefault(extension[1], extension[2]) != extension[2]:
            return False
    return seen == len(POINTER_KEYS)


def refuse_pointers(tar: tarfile.TarFile, members: list[tarfile.TarInfo]) -> None:
    """Refuse a member that is a Git LFS pointer file: a checkout without its LFS objects makes ``git archive`` export the pointer in place of the file."""
    for member in members:
        if member.isfile():
            data = tar.extractfile(member)
            if data is not None and is_pointer(data.read(POINTER_LIMIT)):
                raise ArchiveError(f"{member.name} is a Git LFS pointer file, not the file: check out with the Git LFS objects (actions/checkout lfs: true)")


def compose(git_tar: Path, packages: Path, output: Path) -> str:
    """Write the deterministic ``output`` tarball from a ``git archive`` tar and an installed package folder; return the commit ID it carries."""
    with tarfile.open(git_tar) as source:
        revision = str(source.pax_headers.get("comment", ""))
        if not REVISION.fullmatch(revision):
            raise ArchiveError("the Git tar carries no commit ID (it must come from git archive)")
        members = source.getmembers()
        if any(Path(member.name).parts[:1] == (PACKAGES,) for member in members):
            raise ArchiveError(f"the revision tracks a {PACKAGES} folder: installed packages must not be committed")
        mtime = max((member.mtime for member in members), default=0)
        with (
            output.open("wb") as raw,
            gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0, compresslevel=9) as compressed,
            tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT, pax_headers={"comment": revision}) as target,
        ):
            for member in members:
                target.addfile(member, source.extractfile(member) if member.isfile() else None)
            for path in sorted(packages.rglob("*")):
                info = tarfile.TarInfo(f"{PACKAGES}/{path.relative_to(packages).as_posix()}")
                info.mtime, info.uid, info.gid, info.uname, info.gname = mtime, 0, 0, "", ""
                if path.is_symlink():
                    raise ArchiveError(f"the package folder holds a link: {path.name}")
                if path.is_dir():
                    info.type, info.mode = tarfile.DIRTYPE, 0o755
                    target.addfile(info)
                else:
                    data = path.read_bytes()
                    info.size, info.mode = len(data), 0o755 if path.stat().st_mode & 0o100 else 0o644
                    target.addfile(info, io.BytesIO(data))
    return revision


def build(output: Path, revision: str = "HEAD", root: Path = ROOT) -> dict[str, str]:
    """Export ``revision``, install its locked dbt packages from the pinned local archive and write ``code.tar.gz`` and ``code.sha256`` to a new folder."""
    from scripts.lakehouse import dbt_package_archive  # needs PyYAML, which the task images do not carry, so only the host build imports it

    if output.exists():
        raise ArchiveError(f"{output} exists: pass a new folder")
    commit = git(["rev-parse", "--verify", f"{revision}^{{commit}}"], root)
    with tempfile.TemporaryDirectory(prefix="code_archive_") as scratch:
        folder = Path(scratch)
        git_tar = folder / "git.tar"
        git(["archive", "--format=tar", f"--output={git_tar}", commit], root)
        with tarfile.open(git_tar) as source:
            for name in (LOCK, RECORD):
                member = source.extractfile(name)
                if member is None:
                    raise ArchiveError(f"the revision has no {name}")
                (folder / Path(name).name).write_bytes(member.read())
        record = folder / Path(RECORD).name
        packages = folder / PACKAGES
        archive = root / json.loads(record.read_text())["archive"]["path"]
        dbt_package_archive.install(root, folder / Path(LOCK).name, record, archive, packages, hashlib.sha256(record.read_bytes()).hexdigest())
        built = folder / "code.tar.gz"
        revision_id = compose(git_tar, packages, built)
        digest = hashlib.sha256(built.read_bytes()).hexdigest()
        (folder / "code.sha256").write_text(digest + "\n")
        check(built, folder / "code.sha256")
        output.mkdir(parents=True)
        shutil.copyfile(built, output / "code.tar.gz")
        shutil.copyfile(folder / "code.sha256", output / "code.sha256")
    return {"revision": revision_id, "sha256": digest, "folder": str(output)}


def check(archive: Path, digest_file: Path | None = None, expected: str = "") -> str:
    """Refuse an archive the layer pin cannot use; return its SHA-256. The tarball must match the digest, carry the commit ID, hold the dbt project and hold
    every package of its own lock under ``dbt_packages/``."""
    data = archive.read_bytes()
    if len(data) > LIMIT:
        raise ArchiveError("the archive is larger than the bound")
    digest = hashlib.sha256(data).hexdigest()
    wanted = expected or (digest_file.read_text().strip() if digest_file else digest)
    if not SHA256.fullmatch(wanted) or wanted != digest:
        raise ArchiveError("the archive does not match its SHA-256")
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        if not REVISION.fullmatch(str(tar.pax_headers.get("comment", ""))):
            raise ArchiveError("the archive carries no commit ID (it must come from git archive)")
        members = tar.getmembers()
        refuse_pointers(tar, members)
        names = {member.name.rstrip("/") for member in members}
        if f"{PROJECT}/dbt_project.yml" not in names:
            raise ArchiveError(f"the archive has no {PROJECT}/dbt_project.yml")
        lock = tar.extractfile(LOCK)
        if lock is None:
            raise ArchiveError(f"the archive has no {LOCK}")
        wanted_packages = re.findall(r"^  - name: ([A-Za-z0-9_]+)$", lock.read().decode(), re.MULTILINE)
    if not wanted_packages:
        raise ArchiveError(f"{LOCK} names no package")
    missing = [name for name in wanted_packages if f"{PACKAGES}/{name}/dbt_project.yml" not in names]
    if missing:
        raise ArchiveError(f"the archive lacks the locked dbt packages {missing}: build it with scripts.lakehouse.code_archive, not git archive alone")
    return digest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    built = commands.add_parser("build", help="write code.tar.gz and code.sha256 for a revision into a new folder")
    built.add_argument("--output", type=Path, required=True)
    built.add_argument("--revision", default="HEAD")
    checked = commands.add_parser("check", help="refuse an archive the layer pin cannot use")
    checked.add_argument("archive", type=Path)
    checked.add_argument("digest", type=Path)
    args = parser.parse_args()
    try:
        result = build(args.output, args.revision) if args.command == "build" else {"sha256": check(args.archive, args.digest)}
    except (ArchiveError, OSError, KeyError, ValueError, tarfile.TarError) as error:
        sys.stderr.write(f"code archive refused: {error}\n")
        return 1
    sys.stdout.write(json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
