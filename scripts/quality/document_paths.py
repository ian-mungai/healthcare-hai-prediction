"""Find a user directory path inside an Office or PDF file without ever reporting its value.

The privacy scan reads text files line by line and skips a file with NUL bytes as unreviewed. A workbook or a PDF holds text inside a package or a compressed
stream, where a workstation path can hide (an absolute path element of a workbook, a title in a PDF information dictionary). This module looks at the text
members of an OOXML package, at the bytes of a legacy Office file in both common encodings, and at a PDF's bytes and its decompressed streams. A hit is
reported by member name or by ``(file)`` and ``(stream)`` only. The caller passes the line test of the privacy scan, so the two scans agree on what a user
directory path is: a Windows drive path under ``Users``, ``/Users/<name>`` and ``/home/<name>``, with placeholders and the macOS shared folder exempt.

Limits: a PDF text layer in a custom font encoding or an image of a path is not read, and an encrypted PDF stream is read as bytes only.
"""

from __future__ import annotations

import io
import re
import zipfile
import zlib
from collections.abc import Callable

OFFICE_SUFFIXES = frozenset({".xlsx", ".xlsm", ".docx", ".pptx"})
LEGACY_SUFFIXES = frozenset({".xls", ".doc", ".ppt"})
DOCUMENT_SUFFIXES = OFFICE_SUFFIXES | LEGACY_SUFFIXES | {".pdf"}
# The kinds the privacy scan enforces: every document kind, PDFs included (owner, Nov 20 2026: three AHRQ PDFs that held a user directory path were left out).
ENFORCED_SUFFIXES = DOCUMENT_SUFFIXES
MAX_MEMBER_BYTES = 256 * 1024**2
MAX_STREAM_BYTES = 64 * 1024**2
# The part of a line around a possible path that is handed to the line test, so a one-line XML member costs only its matches.
WINDOW = (200, 600)
HINT = re.compile(rb"users|home|~|file:", re.IGNORECASE)
STREAM = re.compile(rb"stream\r?\n(.*?)\r?\nendstream", re.DOTALL)


def views(body: bytes) -> list[str]:
    """The readings of a member that a person's path can take: UTF-8 or Latin-1 text, and UTF-16 either way round."""
    texts = [body.decode("utf-8", errors="replace") if b"\0" not in body else body.decode("latin-1")]
    if b"\0" in body or body[:2] in (b"\xff\xfe", b"\xfe\xff"):
        # Dropping the NUL bytes reads UTF-16 text of any alignment as ASCII: a legacy Office string starts at an odd offset as often as at an even one.
        texts += [body.decode("utf-16-le", errors="ignore"), body.decode("utf-16-be", errors="ignore"), body.replace(b"\0", b"").decode("latin-1")]
    if body[:2] in (b"\xff\xfe", b"\xfe\xff"):
        texts.append(body.decode("utf-16", errors="ignore"))
    return texts


def has_path(body: bytes, found: Callable[[str], bool]) -> bool:
    """Whether any reading of ``body`` holds a user directory path, judged by the privacy scan's own line test on the text around each hint."""
    if not HINT.search(body) and b"\0" not in body:
        return False
    for text in views(body):
        for match in re.finditer(r"users|home|~|file:", text, re.IGNORECASE):
            line = text[max(0, match.start() - WINDOW[0]) : match.end() + WINDOW[1]]
            if found(line):
                return True
    return False


def office_members(data: bytes) -> dict[str, bytes]:
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        raise ValueError("an Office package cannot be read as a ZIP archive") from None
    found: dict[str, bytes] = {}
    with archive:
        for info in archive.infolist():
            if info.is_dir():
                continue
            if info.file_size > MAX_MEMBER_BYTES:
                raise ValueError(f"member {info.filename} is too large to scan")
            found[info.filename] = archive.read(info)
    return found


def is_text(body: bytes) -> bool:
    """Whether almost every byte is printable ASCII or white space, which is how a plain content stream or a metadata stream reads."""
    return not body or sum(32 <= byte < 127 or byte in (9, 10, 13) for byte in body) >= 0.9 * len(body)


def pdf_streams(data: bytes) -> list[bytes]:
    """Every stream of a PDF, decompressed when it is zlib data. A stream that does not read as text is left out."""
    streams = []
    total = 0
    for match in STREAM.finditer(data):
        raw = match.group(1)
        try:
            body = zlib.decompressobj().decompress(raw, MAX_STREAM_BYTES)
        except zlib.error:
            body = raw
        # Image and font bytes spell a named tilde path by chance; only a stream that reads as text can hold a path.
        if not is_text(body):
            continue
        total += len(body)
        if total > 4 * MAX_STREAM_BYTES:
            raise ValueError("the PDF streams are too large to scan")
        streams.append(body)
    return streams


def members_with_user_path(data: bytes, suffix: str, found: Callable[[str], bool]) -> list[str]:
    """The members of an Office or PDF file that hold a user directory path: member names for a package, ``(file)`` and ``(stream)`` otherwise.

    Raises ValueError for a package or a stream set that cannot be read, so that an unreadable file is never taken as clean.
    """
    suffix = suffix.lower()
    if suffix in OFFICE_SUFFIXES:
        return [name for name, body in office_members(data).items() if has_path(body, found)]
    hits = []
    # The syntax of a PDF is read without its stream bodies: compressed bytes spell a named tilde path by chance, and the streams are read decoded below.
    if has_path(STREAM.sub(b"stream\nendstream", data) if suffix == ".pdf" else data, found):
        hits.append("(file)")
    if suffix == ".pdf" and any(has_path(stream, found) for stream in pdf_streams(data)):
        hits.append("(stream)")
    return sorted(hits)
