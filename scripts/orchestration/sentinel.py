"""Exact-match scan of a set of folders for planted values (Airflow plan section 10; failure modes 755 and 932b).

The scan reads every regular file under the roots, binary-safe, and looks for each value as written and as the hexadecimal text of its bytes (a
database column of bytes is printed that way). An unreadable file, a file that vanishes while it is read and a special file (socket, FIFO, device) are
failures, as is a root that does not exist or a scan that read no file at all: a result that cannot say what it read must not pass. A symbolic link is
never followed and its path text is no content: the link's text is searched for the values, but the link adds no file, byte or per-root count. It is
covered only when it resolves to a regular file or a folder inside one of the scan's own roots, which the scan reads itself; a link to anything else (a
target outside the roots, a missing target, a loop, a special file) is listed as skipped, so the scan is incomplete. The result records what was read.
"""

from __future__ import annotations

import os
import stat
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class ScanResult:
    """What a scan found and what it covered."""

    hits: list[str] = field(default_factory=list)
    files: int = 0
    bytes_read: int = 0
    links: int = 0
    per_root: dict[str, int] = field(default_factory=dict)
    unreadable: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    missing_roots: list[str] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        """True when every input was read: none unreadable, skipped or missing, and at least one file read."""
        return not (self.unreadable or self.skipped or self.missing_roots) and self.files > 0

    @property
    def ok(self) -> bool:
        """True when the scan was complete and found nothing."""
        return self.complete and not self.hits

    def record(self) -> dict[str, Any]:
        """The coverage as JSON-ready evidence (no scanned value is in it)."""
        return {
            "ok": self.ok,
            "complete": self.complete,
            "hits": self.hits[:10],
            "files": self.files,
            "bytes": self.bytes_read,
            "links": self.links,
            "per_root": dict(self.per_root),
            "unreadable": self.unreadable[:10],
            "skipped": self.skipped[:10],
            "missing_roots": list(self.missing_roots),
        }


def entries(root: Path, result: ScanResult) -> Iterator[Path]:
    """Every file, link and special file under a folder (or the file itself), without following a link; an unreadable folder is recorded."""
    if root.is_symlink() or not root.exists():
        result.missing_roots.append(root.name)
        return
    if not root.is_dir():
        yield root
        return

    def unreadable_folder(error: OSError) -> None:
        result.unreadable.append(f"{root.name}/{os.path.relpath(str(error.filename), root)}")

    for folder, folders, files in os.walk(root, onerror=unreadable_folder, followlinks=False):
        folders.sort()
        for name in sorted([*(n for n in folders if (Path(folder) / n).is_symlink()), *files]):
            yield Path(folder) / name


def link_is_covered(path: Path, scanned: Sequence[Path]) -> bool:
    """True when a link resolves to a regular file or a folder inside one of the scanned roots (given by their real paths), where the scan reads it."""
    try:
        target = Path(os.path.realpath(path, strict=True))
        mode = os.lstat(target).st_mode
    except (OSError, RuntimeError):
        return False
    return (stat.S_ISREG(mode) or stat.S_ISDIR(mode)) and any(target.is_relative_to(root) for root in scanned)


def scan(roots: Sequence[Path], values: Sequence[str]) -> ScanResult:
    """Scan every file under the roots for any of the values (exact match, binary-safe, also as the hexadecimal text of the value's bytes)."""
    needles = [text for value in values if value for text in (value.encode(), value.encode().hex().encode())]
    scanned = [Path(os.path.realpath(root)) for root in roots if root.exists() and not root.is_symlink()]
    result = ScanResult()
    for root in roots:
        for path in entries(root, result):
            label = root.name if path == root else f"{root.name}/{path.relative_to(root)}"
            try:
                mode = os.lstat(path).st_mode
                if stat.S_ISLNK(mode):
                    result.links += 1
                    if any(needle in os.readlink(path).encode() for needle in needles):
                        result.hits.append(label)
                    if not link_is_covered(path, scanned):
                        result.skipped.append(label)
                    continue
                if not stat.S_ISREG(mode):
                    result.skipped.append(label)
                    continue
                data = path.read_bytes()
            except OSError:
                result.unreadable.append(label)
                continue
            result.files += 1
            result.bytes_read += len(data)
            result.per_root[root.name] = result.per_root.get(root.name, 0) + 1
            if any(needle in data for needle in needles):
                result.hits.append(label)
    return result
