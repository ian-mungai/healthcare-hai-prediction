"""Refresh, verify and install the lock's dbt packages from a pinned local archive; runtime installation has no network.

Refresh and fetch are explicit host operations: refresh pins new tarballs and rewrites the record; fetch rebuilds the
recorded archive on a fresh clone (or in CI) from the recorded URLs and refuses any byte that differs.
Verify and install read only local files and refuse a changed lock, record or archive.
The upstream tarballs remain immutable members of the local archive. Installation validates every member before copying files.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import http.client
import importlib
import io
import json
import os
import re
import shutil
import sys
import tarfile
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

yaml = importlib.import_module("yaml")

ROOT = Path(__file__).resolve().parents[2]
RECORD = Path("config/lakehouse/dbt_package_archive.json")
LOCK = Path("dbt/package-lock.yml")
ARCHIVES = Path("data/analytics/dbt/package_archive")
LIMIT = 64 * 1024 * 1024


class ArchiveError(ValueError):
    """A package archive or its source lock does not match its pinned record."""


def sha(data: bytes) -> str:
    """SHA-256 of exact bytes."""
    return hashlib.sha256(data).hexdigest()


def locked_packages(lock: Path) -> list[dict[str, str]]:
    """The complete resolved package set, without resolving new versions or dependencies."""
    document = yaml.safe_load(lock.read_bytes())
    packages = document.get("packages") if isinstance(document, dict) else None
    if not isinstance(packages, list) or not packages:
        raise ArchiveError("lock has no resolved packages")
    result = []
    for package in packages:
        row = {key: str(package.get(key, "")) for key in ("name", "package", "version")}
        if not re.fullmatch(r"[A-Za-z0-9_]+", row["name"]) or not re.fullmatch(r"[A-Za-z0-9_-]+/[A-Za-z0-9_-]+", row["package"]):
            raise ArchiveError("lock package must have a plain name and hub namespace")
        if not re.fullmatch(r"\d+\.\d+\.\d+(?:[-.A-Za-z0-9]+)?", row["version"]):
            raise ArchiveError("lock package version must be exact")
        result.append(row)
    if len({row["name"] for row in result}) != len(result):
        raise ArchiveError("lock has duplicate package names")
    return sorted(result, key=lambda row: row["name"])


def archive_members(data: bytes, compressed: bool = False) -> dict[str, bytes]:
    """Read only safe regular files or folders; links, duplicate names and unsupported members are refused."""
    output = {}
    seen = set()
    total = 0
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz" if compressed else "r:") as archive:
        for member in archive:
            name = PurePosixPath(member.name)
            if name.is_absolute() or ".." in name.parts or not name.parts or member.name.rstrip("/") != str(name) or str(name) in seen or "\\" in member.name:
                raise ArchiveError("archive has an unsafe or duplicate member")
            seen.add(str(name))
            if member.isdir():
                continue
            if not member.isfile() or member.size < 0 or total + member.size > LIMIT:
                raise ArchiveError("archive has a link, unsupported member or exceeds the byte limit")
            total += member.size
            stream = archive.extractfile(member)
            if stream is None:
                raise ArchiveError("archive member is unreadable")
            with stream:
                content = stream.read(member.size + 1)
            if len(content) != member.size:
                raise ArchiveError("archive member size differs")
            output[member.name] = content
    return output


def package_tree(data: bytes, name: str) -> dict[str, bytes]:
    """Validate a hub tarball's sole project root and flatten that root for the installed package folder."""
    members = archive_members(data, True)
    roots = {PurePosixPath(member).parts[0] for member in members}
    if len(roots) != 1 or any(len(PurePosixPath(member).parts) < 2 for member in members):
        raise ArchiveError("package archive must have one project root")
    files = {str(PurePosixPath(member).relative_to(next(iter(roots)))): content for member, content in members.items()}
    project = yaml.safe_load(files.get("dbt_project.yml", b""))
    if not isinstance(project, dict) or project.get("name") != name:
        raise ArchiveError(f"package archive project name differs for {name}")
    return files


def verified_content(
    root: Path, *, lock: Path | None = None, record: Path | None = None, archive: Path | None = None, record_sha256: str | None = None
) -> tuple[Path, dict[str, Any], dict[str, bytes]]:
    """Verify exact lock bytes, the complete resolved set and all aggregate and upstream archive hashes."""
    lock, record = lock or root / LOCK, record or root / RECORD
    raw = record.read_bytes()
    if record_sha256 is not None and sha(raw) != record_sha256:
        raise ArchiveError("archive record hash differs from the admitted record")
    document = json.loads(raw)
    if document.get("schema_version") != 1 or document.get("lock_sha256") != sha(lock.read_bytes()):
        raise ArchiveError("lock changed; refresh the package archive explicitly")
    rows = document["packages"]
    if [{key: row[key] for key in ("name", "package", "version")} for row in rows] != locked_packages(lock):
        raise ArchiveError("archive package set differs from the lock")
    relative = Path(document["archive"]["path"])
    if relative.is_absolute() or ".." in relative.parts or not relative.is_relative_to(ARCHIVES):
        raise ArchiveError("archive path must be in the ignored package archive folder")
    archive = archive or root / relative
    if archive.is_symlink() or archive.resolve() != archive.absolute():
        raise ArchiveError("archive path may not contain a link")
    data = archive.read_bytes()
    if len(data) > LIMIT or len(data) != document["archive"]["size"] or sha(data) != document["archive"]["sha256"]:
        raise ArchiveError("archive size or hash differs from the pinned record")
    members = archive_members(data)
    if set(members) != {row["filename"] for row in rows}:
        raise ArchiveError("archive members differ from the pinned package set")
    for row in rows:
        content = members[row["filename"]]
        if len(content) != row["size"] or sha(content) != row["sha256"]:
            raise ArchiveError(f"package archive hash differs for {row['name']}")
        package_tree(content, row["name"])
    return archive, document, members


def verify(root: Path, **kwargs: Any) -> Path:
    """Verify the local archive and return its path; installation consumes the bytes from that same verified read."""
    return verified_content(root, **kwargs)[0]


def download(url: str) -> bytes:
    """Download an exact HTTPS package resource to memory with a bounded response; never execute it."""
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise ArchiveError("package source URL must be HTTPS without credentials")
    for _ in range(4):
        connection = http.client.HTTPSConnection(parsed.netloc, timeout=60)
        try:
            connection.request("GET", parsed.path + (f"?{parsed.query}" if parsed.query else ""), headers={"User-Agent": "hai-dbt-package-archive"})
            response = connection.getresponse()
            if response.status in (301, 302, 303, 307, 308):
                parsed = urlparse(response.getheader("Location") or "")
                if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
                    raise ArchiveError("package source redirects to a URL that is not HTTPS without credentials")
                continue
            if response.status != 200:
                raise ArchiveError(f"package download failed with HTTP {response.status}")
            result = bytearray()
            while block := response.read(65536):
                result.extend(block)
                if len(result) > LIMIT:
                    raise ArchiveError("package download exceeds the byte limit")
            return bytes(result)
        finally:
            connection.close()
    raise ArchiveError("package source redirects more than three times")


def pack(contents: dict[str, bytes]) -> bytes:
    """The deterministic aggregate archive: sorted members, fixed mode and no timestamps or owners."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for filename, content in sorted(contents.items()):
            member = tarfile.TarInfo(filename)
            member.size = len(content)
            member.mode = 0o444
            archive.addfile(member, io.BytesIO(content))
    return buffer.getvalue()


NOFOLLOW = os.O_NOFOLLOW | os.O_CLOEXEC


def write_all(fd: int, data: bytes) -> None:
    """Write every byte to an open file."""
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view) :]


def published(folder: int, name: str, data: bytes) -> bool:
    """Whether the folder already holds the archive; a link refuses, and so do different bytes."""
    try:
        fd = os.open(name, os.O_RDONLY | NOFOLLOW, dir_fd=folder)
    except FileNotFoundError:
        return False
    except OSError as error:
        raise ArchiveError("archive path may not contain a link") from error
    with os.fdopen(fd, "rb") as handle:
        if handle.read(LIMIT + 1) != data:
            raise ArchiveError("immutable archive path has different bytes")
    return True


def open_child(folder: int, part: str, create: bool = False) -> int:
    """Open one folder relative to its parent without following a link, making it first when asked; closes the parent."""
    try:
        if create:
            with contextlib.suppress(FileExistsError):
                os.mkdir(part, 0o755, dir_fd=folder)
        return os.open(part, os.O_RDONLY | os.O_DIRECTORY | NOFOLLOW, dir_fd=folder)
    except OSError as error:
        raise ArchiveError("archive path may not contain a link") from error
    finally:
        os.close(folder)


def write_immutable(root: Path, relative: Path, data: bytes) -> None:
    """Publish an archive once, read-only, under root without following a link; an existing archive must hold the same bytes.

    Every folder from / down opens relative to its parent with O_NOFOLLOW. The bytes go to an exclusive staging file in the
    same folder, which a hard link publishes: a link never replaces an existing file, and an interrupted write leaves no
    archive. The published file must be the staged inode, or the call refuses and deletes nothing. A folder that another
    process moves after it was opened, or an entry it changes during the fetch, is outside this check (recorded in the issue register).
    """
    if relative.is_absolute() or ".." in relative.parts or not relative.is_relative_to(ARCHIVES):
        raise ArchiveError("archive path must be in the ignored package archive folder")
    if root.absolute() != root.resolve():
        raise ArchiveError("archive path may not contain a link")
    folder = os.open("/", os.O_RDONLY | os.O_DIRECTORY | NOFOLLOW)
    for part in root.absolute().parts[1:]:
        folder = open_child(folder, part)
    for part in relative.parts[:-1]:
        folder = open_child(folder, part, create=True)
    try:
        if published(folder, relative.name, data):
            return
        staging = f".{relative.name}.{os.getpid()}.{os.urandom(8).hex()}.partial"
        fd = os.open(staging, os.O_WRONLY | os.O_CREAT | os.O_EXCL | NOFOLLOW, 0o600, dir_fd=folder)
        try:
            write_all(fd, data)
            os.fchmod(fd, 0o444)
            os.fsync(fd)
            staged = os.fstat(fd)
            try:
                os.link(staging, relative.name, src_dir_fd=folder, dst_dir_fd=folder, follow_symlinks=False)
            except FileExistsError:
                published(folder, relative.name, data)
                return
            final = os.stat(relative.name, dir_fd=folder, follow_symlinks=False)
            if (final.st_dev, final.st_ino) != (staged.st_dev, staged.st_ino):
                # Another process changed the staging file or the final name: this call cannot prove which entry it
                # created, so it deletes nothing and refuses; verify and install still check every byte at use.
                raise ArchiveError(f"another process changed {relative} during the fetch; check it and remove it by hand before a retry")
        finally:
            os.close(fd)
            with contextlib.suppress(FileNotFoundError):
                os.unlink(staging, dir_fd=folder)
    finally:
        os.close(folder)


def fetch(root: Path) -> str:
    """Rebuild the recorded archive from the recorded tarball URLs in an explicit host step; any byte that differs from the record refuses."""
    document = json.loads((root / RECORD).read_bytes())
    if document.get("schema_version") != 1 or document.get("lock_sha256") != sha((root / LOCK).read_bytes()):
        raise ArchiveError("lock changed; refresh the package archive explicitly")
    relative = Path(document["archive"]["path"])
    if relative.is_absolute() or ".." in relative.parts or not relative.is_relative_to(ARCHIVES):
        raise ArchiveError("archive path must be in the ignored package archive folder")
    contents = {}
    for row in document["packages"]:
        content = download(row["url"])
        if len(content) != row["size"] or sha(content) != row["sha256"]:
            raise ArchiveError(f"downloaded package differs from the pinned record for {row['name']}")
        contents[row["filename"]] = content
    data = pack(contents)
    if len(data) != document["archive"]["size"] or sha(data) != document["archive"]["sha256"]:
        raise ArchiveError("rebuilt archive differs from the pinned record")
    write_immutable(root, relative, data)
    return str(verify(root))


def refresh(root: Path) -> dict[str, Any]:
    """Resolve only the lock's named hub releases and download their recorded tarballs in an explicit host step."""
    lock = root / LOCK
    lock_bytes = lock.read_bytes()
    rows = []
    contents = {}
    for package in locked_packages(lock):
        metadata_url = f"https://hub.getdbt.com/api/v1/{package['package']}.json"
        metadata_bytes = download(metadata_url)
        metadata = json.loads(metadata_bytes)["versions"][package["version"]]
        url = str(metadata["downloads"]["tarball"])
        content = download(url)
        package_tree(content, package["name"])
        filename = f"{package['name']}-{package['version']}.tar.gz"
        contents[filename] = content
        rows.append(
            {
                **package,
                "filename": filename,
                "url": url,
                "size": len(content),
                "sha256": sha(content),
                "hub_metadata_url": metadata_url,
                "hub_metadata_sha256": sha(metadata_bytes),
            }
        )
    if lock.read_bytes() != lock_bytes:
        raise ArchiveError("lock changed during refresh; no record published")
    data = pack(contents)
    relative = ARCHIVES / sha(lock_bytes) / f"packages_{sha(data)}.tar"
    write_immutable(root, relative, data)
    document = {
        "schema_version": 1,
        "lock_sha256": sha(lock_bytes),
        "packages": rows,
        "archive": {"path": str(relative), "size": len(data), "sha256": sha(data)},
    }
    record = root / RECORD
    record.parent.mkdir(parents=True, exist_ok=True)
    pending = record.with_suffix(".pending")
    pending.write_text(json.dumps(document, indent=2) + "\n")
    verify(root, record=pending)
    os.replace(pending, record)
    return document


def install(root: Path, lock: Path, record: Path, archive: Path, destination: Path, expected_record: str) -> None:
    """Verify before reading or copying package content; install an exact replacement set from local bytes only."""
    _verified, document, members = verified_content(root, lock=lock, record=record, archive=archive, record_sha256=expected_record)
    destination.mkdir(parents=True, exist_ok=True)
    # Only the record's own package folders and this tool's stale staging folders may be replaced; anything else stops the install first.
    names = {str(row["name"]) for row in document["packages"]}
    foreign = sorted(path.name for path in destination.iterdir() if path.name not in names and not path.name.startswith(".package_install_"))
    if foreign:
        raise ArchiveError(f"the package folder {destination} holds entries that are not locked packages: {', '.join(foreign)}; nothing was removed")
    with tempfile.TemporaryDirectory(prefix=".package_install_", dir=destination) as temporary:
        staging = Path(temporary)
        for row in document["packages"]:
            for name, content in package_tree(members[row["filename"]], row["name"]).items():
                path = staging / row["name"] / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
        # The folder is read again: an entry that appeared while staging is never removed; it stops the install instead.
        entries = [path for path in destination.iterdir() if path != staging]
        late = sorted(path.name for path in entries if path.name not in names and not path.name.startswith(".package_install_"))
        if late:
            raise ArchiveError(f"the package folder {destination} gained entries that are not locked packages: {', '.join(late)}; nothing was removed")
        for path in entries:
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
            else:
                path.unlink()
        for path in staging.iterdir():
            os.replace(path, destination / path.name)


def main() -> int:
    """Run an explicit refresh or fetch, or an entirely local verify/install command."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=("refresh", "fetch", "verify", "install"))
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--lock", type=Path)
    parser.add_argument("--record", type=Path)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--destination", type=Path)
    parser.add_argument("--record-sha256")
    args = parser.parse_args()
    try:
        if args.command == "refresh":
            result: Any = refresh(args.root)
        elif args.command == "fetch":
            result = fetch(args.root)
        elif args.command == "verify":
            result = str(verify(args.root, lock=args.lock, record=args.record, archive=args.archive, record_sha256=args.record_sha256))
        else:
            if not all((args.lock, args.record, args.archive, args.destination, args.record_sha256)):
                parser.error("install requires lock, record, archive, destination and record-sha256")
            install(args.root, args.lock, args.record, args.archive, args.destination, args.record_sha256)
            result = "packages installed from the verified local archive without network"
        sys.stdout.write(json.dumps(result, indent=2) + "\n")
        return 0
    except (ArchiveError, OSError, ValueError, KeyError, tarfile.TarError, http.client.HTTPException) as error:
        sys.stderr.write(f"package archive refused: {error}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
